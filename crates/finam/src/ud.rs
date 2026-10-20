//! Finam user data connector: [`UserDataConnector`]. One connector = one account.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use brokapi_core::{
    Backpressure, Balance, ClientOrderId, Envelope, Error, Execution, ExecutionSource, OrderAck,
    OrderExecutor, OrderSnapshot, PlaceIdempotency, PlaceOrderRequest, PlaceStopOrderRequest,
    Portfolio, PortfolioSource, Position, ReconcileReport, ReplaceOrderRequest, ShutdownMode,
    ShutdownReport, StopOrderExecutor, StopOrderRef, StopOrderSnapshot, StopOrderState,
    SubscribeOpts, SubscriptionId, TimeRange, UserDataCaps,
};
use brokapi_finam_protocol::pb::google::r#type::Interval;
use brokapi_finam_protocol::pb::grpc::tradeapi::v1::accounts as pa;
use brokapi_finam_protocol::pb::grpc::tradeapi::v1::orders as po;
use brokapi_finam_protocol::{FinamApi, FinamError};
use brokapi_runtime::{
    BackpressureManager, BrokerLimiter, CommandGate, InstrumentLocks, PriceGrid,
    ShutdownController, reconcile_ledger,
};
use brokapi_sansio::{BrokerCore, CoreEvent, EventSink};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::warn;

use crate::cid::FinamCidCodec;
use crate::config::UserDataConfig;
use crate::core::ud::UdSubKind;
use crate::core::{StreamEvent, SubStatus, UdCore, UdEvent, UdFrame};
use crate::error::{map_command_err, map_protocol_err, unsupported};
use crate::index::OrderIndex;
use crate::map::order::{build_place_order, is_active, rejection_of};
use crate::map::portfolio::{map_account, map_trade};
use crate::map::stop::{self as map_stop, StopWire};
use crate::pump::{PumpIo, SharedBackpressure, run_pump};
use crate::raw::FinamRaw;
use crate::rpc::Rpc;
use crate::stream::{OpenFn, StreamHandle, StreamParams, spawn_stream};
use crate::subs::SubIdGen;
use crate::util::{no_instrument, timestamps, to_timestamp};
use brokapi_sansio::OrderSignal;

type UdWrap<M> = Arc<dyn Fn(StreamEvent<M>) -> UdFrame + Send + Sync>;

struct UdShared {
    rpc: Rpc,
    account_id: String,
    core: Arc<Mutex<UdCore>>,
    index: Arc<OrderIndex>,
    /// Commands on one instrument go one at a time, in arrival order.
    locks: InstrumentLocks,
    price_grid: PriceGrid,
    /// Cids of stop orders already sent: the library never sends the same one twice.
    stops_sent: Mutex<std::collections::HashSet<ClientOrderId>>,
    backpressure: SharedBackpressure,
    streams: Arc<Mutex<HashMap<SubscriptionId, StreamHandle>>>,
    frames: mpsc::Sender<UdFrame>,
    sub_ids: SubIdGen,
    shutdown: Arc<Mutex<ShutdownController>>,
    order_state_buffer: usize,
    executions_limit: i32,
    ack_timeout: Duration,
}

/// Finam user data connector for ONE account: orders, stop orders, own trades and the portfolio.
///
/// It implements [`OrderExecutor`], [`StopOrderExecutor`], [`ExecutionSource`] and
/// [`PortfolioSource`]. To trade two accounts, start two connectors (they may share one token
/// manager and must share one rate limiter).
///
/// Rules that protect your money:
///
/// - **Call `reconcile` first.** `place`, `cancel`, `place_stop` and `cancel_stop` return
///   `Error::NotReconciled` until `reconcile` has succeeded once. Pass the cids you know from before the
///   restart (an empty list at a clean start).
/// - **A place command is sent once.** If it gets no answer in time, the result is `Error::Unknown`
///   and the library does not repeat it, because a repeat could create a second order. Ask
///   `order_state` or `active_orders` to see what happened. Sending the same cid again is refused
///   locally with `Error::Rejected` and code `duplicate_place`.
/// - **A refusal is final.** `Error::Rejected` means the broker took nothing. Use a NEW cid for the
///   next try.
/// - **The cid travels in the order `comment`** (`bc1:` and 32 hex digits), so it survives a restart.
///   See [`FinamCidCodec`]. That the broker echoes the comment unchanged is not verified against the
///   live API.
/// - **No replace.** Finam has no amend call. `replace` returns `Error::Unsupported`. Cancel plus a new
///   order is not offered, because it is not atomic.
/// - **Quantity is in pieces**, price in the instrument currency.
/// - **Finam has no sandbox.** Test against the mock, then with a tiny order far from the market.
///
/// # Examples
///
/// Place, look at the order and cancel it. Then a refusal and a stop order. The broker is the
/// in-process imitation, which refuses a limit order at `REJECT_PRICE`:
///
/// ```
/// use brokapi_conformance::REJECT_PRICE;
/// use brokapi_core::{
///     ClientOrderId, Error, InstrumentId, InstrumentKey, NativeId, OrderExecutor, OrderState, OrderType,
///     PlaceOrderRequest, PlaceStopOrderRequest, Price, Qty, ShutdownMode, Side, StopExpiry,
///     StopOrderExecutor, StopOrderKind, StopOrderRef,
/// };
/// use brokapi_finam::limits::default_limits;
/// use brokapi_finam::{UserDataConfig, UserDataConnector};
/// use brokapi_runtime::BrokerLimiter;
/// use tokio::sync::mpsc;
/// # use brokapi_finam::FinamApiConfig;
/// # use brokapi_mocks::finam::{MockServer, SECRET};
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let server = MockServer::start().await;
/// # let api = FinamApiConfig::new(SECRET).with_endpoint(server.url.clone());
/// # let account = "ACC1";
/// // `api`: your FinamApiConfig. `account`: the account id, as Finam writes it.
/// let (events_tx, _events) = mpsc::channel(256);
/// let limiter = BrokerLimiter::new(&default_limits());
/// let ud = UserDataConnector::start(UserDataConfig::new(api, account, "finam-ud"), limiter, events_tx).await?;
///
/// // Before reconcile every command is refused and nothing is sent.
/// let sber = InstrumentId::new(
///     InstrumentKey::new("SBER", "MISX"),
///     NativeId::SymbolMic { symbol: "SBER".into(), mic: "MISX".into() },
/// );
/// let limit_buy = |price: Price| PlaceOrderRequest {
///     cid: ClientOrderId::new_v4(),
///     instrument: sber.clone(),
///     side: Side::Buy,
///     order_type: OrderType::Limit,
///     qty: Qty::from(1),
///     price: Some(price),
/// };
/// assert!(matches!(ud.place(limit_buy(Price::from(1))).await, Err(Error::NotReconciled)));
///
/// ud.reconcile(&[]).await?;
///
/// // Place, read back, cancel.
/// let req = limit_buy(Price::from(1));
/// let cid = req.cid;
/// let ack = ud.place(req).await?;
/// assert_eq!(ack.cid, cid);
/// let snapshot = ud.order_state(cid).await?;
/// assert!(!matches!(snapshot.state, OrderState::Rejected { .. }));
/// ud.cancel(cid).await?;
///
/// // The broker says no: a clean refusal. Nothing is open at the broker. Use a new cid to try again.
/// match ud.place(limit_buy(REJECT_PRICE)).await {
///     Err(Error::Rejected { code, .. }) => assert_eq!(code, "invalid_argument"),
///     other => panic!("expected a refusal, got {other:?}"),
/// }
///
/// // A stop-loss: a SELL that triggers when the price falls to 100.
/// let stop = PlaceStopOrderRequest {
///     cid: ClientOrderId::new_v4(),
///     instrument: sber,
///     side: Side::Sell,
///     kind: StopOrderKind::StopLoss,
///     qty: Qty::from(5),
///     stop_price: Price::from(100),
///     limit_price: None,
///     expiry: StopExpiry::GoodTillCancel,
/// };
/// let stop_cid = stop.cid;
/// ud.place_stop(stop).await?;
/// assert_eq!(ud.active_stop_orders().await?.len(), 1);
/// ud.cancel_stop(StopOrderRef::Cid(stop_cid)).await?;
/// assert!(ud.active_stop_orders().await?.is_empty());
///
/// ud.shutdown(ShutdownMode::Immediate).await;
/// # Ok(())
/// # }
/// ```
///
/// Own orders as events. The connector keeps a lossless buffer for this subscription, so no order
/// event is lost. When the buffer overflows, `Event::Overflow` arrives and trading is refused until
/// `reconcile`:
///
/// ```
/// use std::time::Duration;
/// use brokapi_core::{
///     ClientOrderId, Event, InstrumentId, InstrumentKey, NativeId, OrderExecutor, OrderType,
///     PlaceOrderRequest, Price, Qty, ShutdownMode, Side,
/// };
/// use brokapi_finam::limits::default_limits;
/// use brokapi_finam::{UserDataConfig, UserDataConnector};
/// use brokapi_runtime::BrokerLimiter;
/// use tokio::sync::mpsc;
/// # use brokapi_finam::FinamApiConfig;
/// # use brokapi_mocks::finam::{MockServer, SECRET};
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let server = MockServer::start().await;
/// # let api = FinamApiConfig::new(SECRET).with_endpoint(server.url.clone());
/// let (events_tx, mut events) = mpsc::channel(256);
/// let ud = UserDataConnector::start(
///     UserDataConfig::new(api, "ACC1", "finam-ud"),
///     BrokerLimiter::new(&default_limits()),
///     events_tx,
/// )
/// .await?;
/// ud.reconcile(&[]).await?;
/// let sub = ud.subscribe_order_state().await?;
///
/// let cid = ClientOrderId::new_v4();
/// ud.place(PlaceOrderRequest {
///     cid,
///     instrument: InstrumentId::new(
///         InstrumentKey::new("SBER", "MISX"),
///         NativeId::SymbolMic { symbol: "SBER".into(), mic: "MISX".into() },
///     ),
///     side: Side::Buy,
///     order_type: OrderType::Limit,
///     qty: Qty::from(1),
///     price: Some(Price::from(1)),
/// })
/// .await?;
///
/// // Wait for the event of OUR order, with a timeout, never a sleep.
/// let ours = tokio::time::timeout(Duration::from_secs(5), async {
///     while let Some(envelope) = events.recv().await {
///         if let Event::Order(order) = envelope.payload {
///             if order.cid == cid {
///                 return Some(envelope.sub);
///             }
///         }
///     }
///     None
/// })
/// .await?;
/// assert_eq!(ours, Some(Some(sub)));
///
/// ud.cancel(cid).await?;
/// ud.shutdown(ShutdownMode::Immediate).await;
/// # Ok(())
/// # }
/// ```
pub struct UserDataConnector {
    inner: Arc<UdShared>,
    pump: JoinHandle<()>,
}

impl UserDataConnector {
    /// Checks the config and the cid codec, logs in, and starts the event pump. Streams are opened later,
    /// by the `subscribe_*` methods.
    ///
    /// The connector starts NOT reconciled: commands are refused until you call
    /// [`reconcile`](OrderExecutor::reconcile). `limiter` is the rate limiter of your Finam user,
    /// shared by every connector. Events go to `out`.
    ///
    /// When the token lists its accounts and `cfg.account_id` is not one of them, start fails.
    ///
    /// # Errors
    ///
    /// - [`Error::Config`]: empty account id, empty secret, invalid endpoint URL, the cid codec does
    ///   not fit the broker limits, or the account is not available to this secret.
    /// - [`Error::Rejected`]: the broker refused the secret (code `unauthenticated`).
    /// - [`Error::RateLimited`]: the broker answered `RESOURCE_EXHAUSTED` to the login.
    /// - [`Error::Transport`]: the broker could not be reached.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::{Error, ShutdownMode};
    /// use brokapi_finam::limits::default_limits;
    /// use brokapi_finam::{FinamApiConfig, UserDataConfig, UserDataConnector};
    /// use brokapi_runtime::BrokerLimiter;
    /// use tokio::sync::mpsc;
    /// # use brokapi_mocks::finam::{MockServer, SECRET};
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = MockServer::start().await;
    /// # let api = FinamApiConfig::new(SECRET).with_endpoint(server.url.clone());
    /// let limiter = BrokerLimiter::new(&default_limits());
    /// let (events, _rx) = mpsc::channel(256);
    ///
    /// let ud = UserDataConnector::start(UserDataConfig::new(api.clone(), "ACC1", "finam-ud"), limiter.clone(), events.clone()).await?;
    /// assert_eq!(ud.account_id(), "ACC1");
    /// ud.shutdown(ShutdownMode::Immediate).await;
    ///
    /// // An account that the secret cannot reach is a configuration error.
    /// let err = UserDataConnector::start(UserDataConfig::new(api, "NOT-MINE", "finam-ud"), limiter, events).await.err().unwrap();
    /// assert!(matches!(err, Error::Config(_)));
    /// # Ok(())
    /// # }
    /// ```
    pub async fn start(
        cfg: UserDataConfig,
        limiter: Arc<BrokerLimiter>,
        out: mpsc::Sender<Envelope>,
    ) -> Result<Self, Error> {
        cfg.check()?;
        // A lossy cid format is a configuration error at startup, not a runtime degradation.
        FinamCidCodec.validate()?;

        let tokens = cfg.api.resolve_tokens().await?;
        let known = tokens.account_ids();
        if !known.is_empty() && !known.iter().any(|a| a == &cfg.account_id) {
            return Err(Error::Config(brokapi_core::ConfigError::Message(format!(
                "account {} is not available to this secret (token lists {} account(s))",
                cfg.account_id,
                known.len()
            ))));
        }
        let api = FinamApi::connect(&cfg.api.endpoint, tokens, cfg.api.bearer)
            .map_err(|e| map_protocol_err(&e))?;

        let (frames, rx) = mpsc::channel::<UdFrame>(1024);
        let index = Arc::new(OrderIndex::new());
        let core = Arc::new(Mutex::new(UdCore::new(Arc::clone(&index))));
        let backpressure: SharedBackpressure = Arc::new(Mutex::new(BackpressureManager::new()));
        let shutdown = Arc::new(Mutex::new(ShutdownController::new(CommandGate::new())));
        let streams: Arc<Mutex<HashMap<SubscriptionId, StreamHandle>>> = Arc::default();

        let handler = {
            let core = Arc::clone(&core);
            move |frame: UdFrame, now: Instant| -> Vec<CoreEvent> {
                let mut sink = EventSink::new();
                core.lock()
                    .expect("core poisoned")
                    .on_frame(frame, now, &mut sink);
                sink.drain()
            }
        };
        // Spec 6: a Lossless overflow degrades the connector: trading commands are refused until
        // `reconcile` succeeds.
        let on_overflow = {
            let shutdown = Arc::clone(&shutdown);
            Arc::new(move |_sub: SubscriptionId| {
                shutdown
                    .lock()
                    .expect("shutdown poisoned")
                    .gate_mut()
                    .mark_degraded();
            })
        };
        let cancel = shutdown.lock().expect("shutdown poisoned").child_token();
        let pump = tokio::spawn(run_pump(
            PumpIo {
                rx,
                out,
                source: Arc::clone(&cfg.source),
                backpressure: Arc::clone(&backpressure),
                cancel,
            },
            handler,
            on_overflow,
        ));

        let inner = Arc::new(UdShared {
            rpc: Rpc {
                api,
                limiter,
                timeout: cfg.api.request_timeout,
            },
            account_id: cfg.account_id,
            core,
            index,
            locks: InstrumentLocks::new(),
            price_grid: cfg.price_grid.clone(),
            stops_sent: Mutex::default(),
            backpressure,
            streams,
            frames,
            sub_ids: SubIdGen::new(1000),
            shutdown,
            order_state_buffer: cfg.order_state_buffer,
            executions_limit: cfg.executions_limit,
            ack_timeout: cfg.subscribe_ack_timeout,
        });
        Ok(Self { inner, pump })
    }

    /// Account this connector trades and reads.
    ///
    /// # Examples
    ///
    /// See [`start`](Self::start).
    pub fn account_id(&self) -> &str {
        &self.inner.account_id
    }

    /// Any other method of the Finam API with this connector's token, endpoint and rate limiter.
    /// `account_id()` of the result is this connector's account. See [`FinamRaw`].
    ///
    /// Orders placed through `raw()` are not tracked by this connector: no events, no `reconcile`.
    /// Use the shared traits for orders.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::ShutdownMode;
    /// use brokapi_finam::limits::default_limits;
    /// use brokapi_finam::{FinamApiConfig, UserDataConfig, UserDataConnector};
    /// use brokapi_runtime::BrokerLimiter;
    /// use tokio::sync::mpsc;
    /// # use brokapi_mocks::finam::{MockServer, SECRET};
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = MockServer::start().await;
    /// # let api = FinamApiConfig::new(SECRET).with_endpoint(server.url.clone());
    /// # let (events, _rx) = mpsc::channel(16);
    /// let ud = UserDataConnector::start(UserDataConfig::new(api, "ACC1", "finam-ud"), BrokerLimiter::new(&default_limits()), events).await?;
    /// assert_eq!(ud.raw().account_id(), Some("ACC1"));
    /// # ud.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    pub fn raw(&self) -> FinamRaw {
        FinamRaw::new(self.inner.rpc.clone(), Some(self.inner.account_id.clone()))
    }

    /// The price steps this connector checks order prices against. Fill it from
    /// [`TradingParams`](brokapi_core::TradingParams) (`grid.learn(&params)`) to have a price off the
    /// step refused before anything is sent. See [`PriceGrid`].
    pub fn price_grid(&self) -> &PriceGrid {
        &self.inner.price_grid
    }

    /// Capabilities.
    ///
    /// - `place_idempotency` is `LabelOnly` until a live experiment proves otherwise. The `client_order_id` is a label for us, nothing says Finam rejects or merges a
    ///   repeat of it.
    /// - No replace call exists in the Finam API.
    /// - `supports_stop_orders` is true: stop-loss and stop-limit go through `PlaceOrder` (STOP,
    ///   STOP_LIMIT), take-profit through `PlaceSLTPOrder`, see [`StopOrderExecutor`]. Not verified
    ///   against the live API.
    /// - Executions stream (`SubscribeTrades`) exists. Portfolio also streams (`SubscribeAccount`).
    /// - Finam Trade API has no sandbox.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::{PlaceIdempotency, ShutdownMode};
    /// use brokapi_finam::limits::default_limits;
    /// use brokapi_finam::{FinamApiConfig, UserDataConfig, UserDataConnector};
    /// use brokapi_runtime::BrokerLimiter;
    /// use tokio::sync::mpsc;
    /// # use brokapi_mocks::finam::{MockServer, SECRET};
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = MockServer::start().await;
    /// # let api = FinamApiConfig::new(SECRET).with_endpoint(server.url.clone());
    /// # let (events, _rx) = mpsc::channel(16);
    /// # let ud = UserDataConnector::start(UserDataConfig::new(api, "ACC1", "finam-ud"), BrokerLimiter::new(&default_limits()), events).await?;
    /// let caps = ud.caps();
    /// assert!(!caps.supports_replace);
    /// assert!(caps.supports_stop_orders);
    /// assert_eq!(caps.place_idempotency, PlaceIdempotency::LabelOnly);
    /// # ud.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    pub fn caps(&self) -> UserDataCaps {
        UserDataCaps {
            place_idempotency: PlaceIdempotency::LabelOnly,
            ws_rpc: false,
            ordered_command_channel: false,
            // Finam has no amend call. Cancel + new order is not an emulation we offer: it is not
            // atomic (the old order can fill between the two, or the new one be refused after the cancel).
            supports_replace: false,
            supports_stop_orders: true,
            streams_executions: true,
            sandbox: false,
        }
    }

    /// State of a subscription's stream (connecting, active, reconnecting, failed).
    ///
    /// `None` for an id this connector does not know. Use it after `subscribe_*` returned and the
    /// stream may still be opening. A stream that the broker refuses (no permission) shows as
    /// [`SubStatus::Failed`] and is retried slowly.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use brokapi_core::{OrderExecutor, ShutdownMode};
    /// use brokapi_finam::limits::default_limits;
    /// use brokapi_finam::{FinamApiConfig, SubStatus, UserDataConfig, UserDataConnector};
    /// use brokapi_runtime::BrokerLimiter;
    /// use tokio::sync::mpsc;
    /// # use brokapi_mocks::finam::{MockServer, SECRET};
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = MockServer::start().await;
    /// # let api = FinamApiConfig::new(SECRET).with_endpoint(server.url.clone());
    /// # let (events, _rx) = mpsc::channel(16);
    /// # let ud = UserDataConnector::start(UserDataConfig::new(api, "ACC1", "finam-ud"), BrokerLimiter::new(&default_limits()), events).await?;
    /// let sub = ud.subscribe_order_state().await?;
    /// // Registered at once. Active when the stream is open.
    /// let status = tokio::time::timeout(Duration::from_secs(5), async {
    ///     loop {
    ///         match ud.sub_status(sub) {
    ///             Some(SubStatus::Active) => return SubStatus::Active,
    ///             _ => tokio::time::sleep(Duration::from_millis(20)).await,
    ///         }
    ///     }
    /// })
    /// .await?;
    /// assert_eq!(status, SubStatus::Active);
    /// # ud.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    pub fn sub_status(&self, sub: SubscriptionId) -> Option<SubStatus> {
        self.inner.core.lock().expect("core poisoned").status(sub)
    }

    /// Stops the connector in the order that protects your orders.
    ///
    /// 1. New commands are refused (`Error::ShuttingDown`).
    /// 2. Every stream is cancelled (the gRPC way of unsubscribing). `Graceful` waits for each up to
    ///    `deadline`, `Immediate` aborts them.
    /// 3. Commands still in flight are abandoned and listed in the report (`inflight_abandoned`).
    ///    Their orders may exist at the broker. Read them with `active_orders` after you start again.
    /// 4. The event pump stops.
    ///
    /// Orders already on the exchange stay there. Shutdown does NOT cancel them.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use brokapi_core::{Error, OrderExecutor, ShutdownMode};
    /// use brokapi_finam::limits::default_limits;
    /// use brokapi_finam::{FinamApiConfig, UserDataConfig, UserDataConnector};
    /// use brokapi_runtime::BrokerLimiter;
    /// use tokio::sync::mpsc;
    /// # use brokapi_mocks::finam::{MockServer, SECRET};
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = MockServer::start().await;
    /// # let api = FinamApiConfig::new(SECRET).with_endpoint(server.url.clone());
    /// # let (events, _rx) = mpsc::channel(16);
    /// let ud = UserDataConnector::start(UserDataConfig::new(api, "ACC1", "finam-ud"), BrokerLimiter::new(&default_limits()), events).await?;
    /// ud.reconcile(&[]).await?;
    /// ud.subscribe_order_state().await?;
    ///
    /// let report = ud.shutdown(ShutdownMode::Graceful { deadline: Duration::from_secs(2) }).await;
    /// assert!(report.inflight_abandoned.is_empty());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn shutdown(self, mode: ShutdownMode) -> ShutdownReport {
        // Step 1: no new commands.
        self.inner
            .shutdown
            .lock()
            .expect("shutdown poisoned")
            .gate_mut()
            .begin_shutdown();
        // Steps 2-3: unsubscribe = cancel the gRPC streams.
        let handles: Vec<StreamHandle> = self
            .inner
            .streams
            .lock()
            .expect("streams poisoned")
            .drain()
            .map(|(_, h)| h)
            .collect();
        let (mut ok, mut failed) = (0usize, 0usize);
        match mode {
            ShutdownMode::Graceful { deadline } => {
                let started = brokapi_runtime::now();
                for h in handles {
                    let left = deadline.saturating_sub(started.elapsed());
                    if h.stop(left).await {
                        ok += 1;
                    } else {
                        failed += 1;
                    }
                }
            }
            ShutdownMode::Immediate => handles.into_iter().for_each(StreamHandle::abort),
        }
        // Step 4-6: abandon in-flight commands (exchange orders stay), cancel the root token.
        let report = {
            let mut shutdown = self.inner.shutdown.lock().expect("shutdown poisoned");
            let mut core = self.inner.core.lock().expect("core poisoned");
            shutdown.shutdown_local(mode, brokapi_runtime::now(), core.orders(), || (ok, failed))
        };
        let _ = self.pump.await;
        report
    }

    fn check_command(&self) -> Result<(), Error> {
        self.inner
            .shutdown
            .lock()
            .expect("shutdown poisoned")
            .gate()
            .check_command()
    }

    fn account(&self) -> String {
        self.inner.account_id.clone()
    }

    /// Orders of the account (all statuses Finam returns), cids resolved, local lifecycle updated.
    async fn fetch_orders(&self) -> Result<Vec<(po::OrderState, OrderSnapshot)>, Error> {
        let account = self.account();
        let resp = self
            .inner
            .rpc
            .read("GetOrders", |api| {
                let req = po::OrdersRequest {
                    account_id: account.clone(),
                };
                async move { api.orders().get_orders(req).await }
            })
            .await?;
        let now = brokapi_runtime::now();
        let mut core = self.inner.core.lock().expect("core poisoned");
        Ok(resp
            .orders
            .into_iter()
            .map(|os| {
                let snap = core.ingest_order(&os, now);
                (os, snap)
            })
            .collect())
    }

    /// Finam order id of a cid: from the index, else from the account's order list.
    async fn order_id_of(&self, cid: ClientOrderId) -> Result<String, Error> {
        if let Some(id) = self.inner.index.order_id(cid) {
            return Ok(id);
        }
        self.inner.index.know(cid);
        self.fetch_orders().await?;
        self.inner
            .index
            .order_id(cid)
            .ok_or_else(|| Error::Rejected {
                code: "not_found".into(),
                message: format!("order {cid} not found at the broker"),
            })
    }

    async fn register_sub<M: Send + 'static>(
        &self,
        kind: UdSubKind,
        opts: SubscribeOpts,
        method: &'static str,
        build: impl FnOnce(SubscriptionId) -> (OpenFn<M>, UdWrap<M>),
    ) -> Result<SubscriptionId, Error> {
        let max = self.inner.rpc.limiter.conn_quota().max_subs_per_conn;
        if self.inner.streams.lock().expect("streams poisoned").len() >= max {
            return Err(Error::Rejected {
                code: "subscription_quota".into(),
                message: format!("at most {max} streams per connection"),
            });
        }
        let id = self.inner.sub_ids.next();
        let (open, wrap) = build(id);
        self.inner
            .backpressure
            .lock()
            .expect("backpressure poisoned")
            .register(id, no_instrument(), opts.backpressure);
        self.inner
            .core
            .lock()
            .expect("core poisoned")
            .register(id, kind);

        let (first_tx, first_rx) = oneshot::channel();
        let cancel = self
            .inner
            .shutdown
            .lock()
            .expect("shutdown poisoned")
            .child_token();
        let handle = spawn_stream(
            StreamParams {
                sub: id,
                method,
                limiter: Arc::clone(&self.inner.rpc.limiter),
                tokens: Arc::clone(self.inner.rpc.api.tokens()),
                open,
                open_timeout: self.inner.rpc.timeout,
                wrap,
                tx: self.inner.frames.clone(),
                first: Some(first_tx),
            },
            cancel,
        );
        self.inner
            .streams
            .lock()
            .expect("streams poisoned")
            .insert(id, handle);

        match tokio::time::timeout(self.inner.ack_timeout, first_rx).await {
            Ok(Ok(Err(e))) => {
                let h = self
                    .inner
                    .streams
                    .lock()
                    .expect("streams poisoned")
                    .remove(&id);
                if let Some(h) = h {
                    h.stop(Duration::from_secs(2)).await;
                }
                self.inner
                    .core
                    .lock()
                    .expect("core poisoned")
                    .unregister(id);
                self.inner
                    .backpressure
                    .lock()
                    .expect("backpressure poisoned")
                    .unregister(id);
                Err(map_protocol_err(&e))
            }
            _ => Ok(id),
        }
    }

    fn wrap<M: Send + 'static>(sub: SubscriptionId, data: fn(M) -> UdEvent) -> UdWrap<M> {
        Arc::new(move |ev| {
            let event = match ev {
                StreamEvent::Msg(m) => data(m),
                StreamEvent::Connected => UdEvent::Connected,
                StreamEvent::Disconnected(r) => UdEvent::Disconnected(r),
                StreamEvent::Failed(r) => UdEvent::Failed(r),
            };
            UdFrame { sub, event }
        })
    }

    fn lossless(&self) -> SubscribeOpts {
        SubscribeOpts {
            backpressure: Backpressure::Lossless {
                cap: self.inner.order_state_buffer,
            },
            depth: None,
        }
    }

    fn signal(&self, cid: ClientOrderId, signal: OrderSignal) {
        self.inner
            .core
            .lock()
            .expect("core poisoned")
            .orders()
            .apply_signal(cid, signal, brokapi_runtime::now());
    }
}

#[async_trait]
impl OrderExecutor for UserDataConnector {
    /// Sends a `PlaceOrder`. The request is checked locally first, then the quota is taken, then the
    /// command goes out once.
    ///
    /// The cid goes into the order `comment` (lossless) and into a 20 character `client_order_id`
    /// tag. Quantity is in pieces. A market order ignores `price`, a limit order needs it.
    ///
    /// Commands on one instrument go one at a time, in arrival order. A refusal does not block the
    /// instrument.
    ///
    /// # Errors
    ///
    /// - [`Error::NotReconciled`], [`Error::Degraded`], [`Error::ShuttingDown`]: the command gate is
    ///   closed. Nothing was sent. Call `reconcile`.
    /// - [`Error::Rejected`]: a local check failed (code `invalid_order`, `missing_price`), the same cid was
    ///   sent before (code `duplicate_place`), or the broker refused (a gRPC status such as
    ///   `invalid_argument`, or an order status such as `rejected`). The broker holds no order.
    /// - [`Error::RateLimited`]: the local quota is spent or the broker said `RESOURCE_EXHAUSTED`.
    ///   Nothing was sent. The cid is still unused: send the same request later.
    /// - [`Error::Unknown`]: timeout, broken connection or server fault, or the answer carried no
    ///   order id. The order MAY exist. It is never repeated by the library. Look it up with
    ///   `order_state` or `active_orders`.
    ///
    /// See the [type docs](UserDataConnector) for an example with a refusal.
    async fn place(&self, req: PlaceOrderRequest) -> Result<OrderAck, Error> {
        self.check_command()?;
        let body = build_place_order(&self.inner.account_id, &req)?;
        // Before a quota is spent or the cid is marked.
        if let Some(p) = req.price {
            self.inner.price_grid.check(&req.instrument.key, p)?;
        }
        // Quota first: a refusal here means nothing was registered and the cid stays usable.
        self.inner.rpc.acquire("PlaceOrder")?;
        // Waits for earlier commands on this instrument, released on every exit path.
        let _instrument = self.inner.locks.lock(&req.instrument).await;
        self.inner.locks.bind(req.cid, &req.instrument);
        {
            let mut core = self.inner.core.lock().expect("core poisoned");
            // Spec 7.2: the library never sends the same place twice.
            if !core.orders().try_place_once(req.cid) {
                return Err(Error::Rejected {
                    code: "duplicate_place".into(),
                    message: "auto-retry of place is forbidden".into(),
                });
            }
            core.orders()
                .register_place(req.cid, brokapi_runtime::now());
        }
        self.inner.index.know(req.cid);

        let sent = self
            .inner
            .rpc
            .send_command(|api| {
                let body = body.clone();
                async move { api.orders().place_order(body).await }
            })
            .await;
        match sent {
            Ok(os) => {
                if let Some(rej) = rejection_of(&os) {
                    self.signal(
                        req.cid,
                        OrderSignal::Rejected {
                            reason: rej.to_string(),
                        },
                    );
                    return Err(rej);
                }
                if os.order_id.is_empty() {
                    self.signal(req.cid, OrderSignal::Timeout);
                    return Err(Error::Unknown);
                }
                self.inner.index.bind(req.cid, &os.order_id);
                let broker_id = brokapi_core::BrokerOrderId::new(os.order_id.clone());
                self.signal(
                    req.cid,
                    OrderSignal::Ack {
                        broker_id: broker_id.clone(),
                    },
                );
                Ok(OrderAck {
                    cid: req.cid,
                    broker_id,
                    ts: timestamps(
                        os.accept_at.as_ref().or(os.transact_at.as_ref()),
                        brokapi_runtime::now(),
                    ),
                })
            }
            Err(e) => Err(self.command_failed(req.cid, &e)),
        }
    }

    /// Sends a `CancelOrder` for the order of `cid`.
    ///
    /// Finam cancels by its own order id. The connector knows the id of orders it placed or has seen
    /// (stream, `reconcile`). For any other cid it reads the order list once (`GetOrders`, one more
    /// request against the quota) and looks the cid up in the comments.
    ///
    /// A cancel is a request, not a result: the order may fill before the cancel reaches the
    /// exchange. Watch the order state.
    ///
    /// # Errors
    ///
    /// - [`Error::NotReconciled`], [`Error::Degraded`], [`Error::ShuttingDown`]: the gate is closed.
    /// - [`Error::RateLimited`]: the local quota is spent. Nothing was sent.
    /// - [`Error::Rejected`]: the broker refused, or no order with this cid exists at the broker
    ///   (code `not_found`).
    /// - [`Error::Unknown`]: no answer. The cancel may or may not have been applied. Read the order state.
    async fn cancel(&self, cid: ClientOrderId) -> Result<(), Error> {
        self.check_command()?;
        self.inner.rpc.acquire("CancelOrder")?;
        // The instrument comes from the orders this process placed; an older order is not serialized.
        let _instrument = self.inner.locks.lock_cid(cid).await;
        let order_id = self.order_id_of(cid).await?;
        let account = self.account();
        let sent = self
            .inner
            .rpc
            .send_command(|api| {
                let req = po::CancelOrderRequest {
                    account_id: account.clone(),
                    order_id: order_id.clone(),
                };
                async move { api.orders().cancel_order(req).await }
            })
            .await;
        match sent {
            Ok(_) => Ok(()),
            // The cancel may or may not have been applied: Unknown, resolved by order_state.
            Err(e) => Err(map_command_err(&e)),
        }
    }

    /// Finam has no replace/amend call, so this is `Unsupported` (and `caps().supports_replace` is false).
    /// Cancel + place is not emulated here: it is not atomic. The client decides whether to risk it.
    ///
    /// # Errors
    ///
    /// Always [`Error::Unsupported`] (capability `replace_order`). Nothing is sent.
    async fn replace(&self, _req: ReplaceOrderRequest) -> Result<OrderAck, Error> {
        Err(unsupported("replace_order"))
    }

    /// Reads one order (`GetOrder`) and reports its state under `cid`. This is the way to resolve an
    /// [`Error::Unknown`] from `place` or `cancel`.
    ///
    /// The order id is found as in `cancel`: a cid that was never seen costs one `GetOrders`.
    ///
    /// # Errors
    ///
    /// - [`Error::Rejected`] with code `not_found`: no order with this cid at the broker. After an
    ///   `Unknown` place this means the order did not arrive (or is not visible yet, not verified).
    /// - [`Error::RateLimited`], [`Error::Transport`]: as for any read.
    async fn order_state(&self, cid: ClientOrderId) -> Result<OrderSnapshot, Error> {
        let order_id = self.order_id_of(cid).await?;
        let account = self.account();
        let os = self
            .inner
            .rpc
            .read("GetOrder", |api| {
                let req = po::GetOrderRequest {
                    account_id: account.clone(),
                    order_id: order_id.clone(),
                };
                async move { api.orders().get_order(req).await }
            })
            .await?;
        let mut core = self.inner.core.lock().expect("core poisoned");
        Ok(core.ingest_order(&os, brokapi_runtime::now()))
    }

    /// Orders of the account that are still working (`GetOrders`, filtered). Stop orders are not in
    /// this list, they have [`active_stop_orders`](StopOrderExecutor::active_stop_orders).
    ///
    /// An order placed by someone else (the web terminal, another program) is listed under a made-up
    /// cid. A status the library does not know counts as working, so an order is never hidden.
    ///
    /// # Errors
    ///
    /// [`Error::RateLimited`], [`Error::Rejected`] or [`Error::Transport`], as for any read.
    async fn active_orders(&self) -> Result<Vec<OrderSnapshot>, Error> {
        // Stop orders have their own list (`active_stop_orders`).
        Ok(self
            .fetch_orders()
            .await?
            .into_iter()
            .filter(|(os, _)| is_active(os.status) && !map_stop::is_stop(os))
            .map(|(_, snap)| snap)
            .collect())
    }

    /// Opens a `SubscribeOrders` stream for the account. Envelopes carry `Event::Order`.
    ///
    /// The buffer is lossless (size `order_state_buffer`): when it overflows the connector turns
    /// degraded and refuses commands until `reconcile`. Errors as in `subscribe_*` of the market data
    /// connector: [`Error::Rejected`] for a refusal or for too many streams (code `subscription_quota`).
    async fn subscribe_order_state(&self) -> Result<SubscriptionId, Error> {
        let account = self.account();
        let opts = self.lossless();
        self.register_sub(UdSubKind::Orders, opts, "SubscribeOrders", |sub| {
            let api = self.inner.rpc.api.clone();
            let open: OpenFn<po::SubscribeOrdersResponse> = Arc::new(move || {
                let api = api.clone();
                let req = po::SubscribeOrdersRequest {
                    account_id: account.clone(),
                };
                Box::pin(async move {
                    api.orders()
                        .subscribe_orders(req)
                        .await
                        .map(|r| r.into_inner())
                })
            });
            (open, Self::wrap(sub, UdEvent::Orders))
        })
        .await
    }

    /// Ledger cids are matched against the account's order list. A ledger cid the broker does
    /// not list is reported in `closed_offline`. An order the broker lists that the
    /// ledger does not know is `foreign` (only orders still working are looked at for this).
    /// Success opens the command gate and clears `Degraded`.
    ///
    /// Stop orders are not part of it: restore them with `active_stop_orders`, and keep their cids out
    /// of `ledger`.
    ///
    /// Call it once after `start` (an empty ledger at a clean start), and again after
    /// `Event::Overflow` or after an `Unknown` outcome you could not resolve. It fills the cid index,
    /// so later cancels of old orders find their broker ids.
    ///
    /// # Errors
    ///
    /// [`Error::RateLimited`], [`Error::Rejected`] or [`Error::Transport`] from the order list
    /// request. On error the gate stays as it was.
    async fn reconcile(&self, ledger: &[ClientOrderId]) -> Result<ReconcileReport, Error> {
        for cid in ledger {
            self.inner.index.know(*cid);
        }
        // Reconcile is about plain orders: stop orders are restored with `active_stop_orders`, and
        // their cids do not belong in this ledger.
        let all: Vec<_> = self
            .fetch_orders()
            .await?
            .into_iter()
            .filter(|(os, _)| !map_stop::is_stop(os))
            .collect();
        let wanted: std::collections::HashSet<_> = ledger.iter().copied().collect();
        let ledger_snaps: Vec<OrderSnapshot> = all
            .iter()
            .filter(|(_, s)| wanted.contains(&s.cid))
            .map(|(_, s)| s.clone())
            .collect();
        let active: Vec<OrderSnapshot> = all
            .iter()
            .filter(|(os, _)| is_active(os.status))
            .map(|(_, s)| s.clone())
            .collect();
        let mut report = reconcile_ledger(&ledger_snaps, &active);
        // Cids the broker does not list at all.
        let listed: std::collections::HashSet<_> = all.iter().map(|(_, s)| s.cid).collect();
        report
            .closed_offline
            .extend(ledger.iter().filter(|c| !listed.contains(c)).copied());
        report.closed_offline.sort();
        report.closed_offline.dedup();

        {
            let mut shutdown = self.inner.shutdown.lock().expect("shutdown poisoned");
            shutdown.gate_mut().mark_reconciled();
            shutdown.gate_mut().clear_degraded();
        }
        self.inner
            .backpressure
            .lock()
            .expect("backpressure poisoned")
            .clear_degraded();
        Ok(report)
    }
}

impl UserDataConnector {
    /// Folds a failed command into the public error and the local order lifecycle.
    fn command_failed(&self, cid: ClientOrderId, e: &FinamError) -> Error {
        let err = map_command_err(e);
        match &err {
            Error::Unknown => self.signal(cid, OrderSignal::Timeout),
            other => self.signal(
                cid,
                OrderSignal::Rejected {
                    reason: other.to_string(),
                },
            ),
        }
        if matches!(err, Error::Unknown) {
            warn!(?cid, error = %e, "place outcome unknown");
        }
        err
    }
}

#[async_trait]
impl ExecutionSource for UserDataConnector {
    /// Own trades in `range` (`Trades`), at most `executions_limit` of them.
    ///
    /// The commission is in the account currency, its currency code is not part of `Execution`. Only
    /// historical trades carry a commission.
    ///
    /// # Errors
    ///
    /// [`Error::RateLimited`], [`Error::Rejected`] or [`Error::Transport`], as for any read.
    async fn executions(&self, range: TimeRange) -> Result<Vec<Execution>, Error> {
        let account = self.account();
        let limit = self.inner.executions_limit;
        let resp = self
            .inner
            .rpc
            .read("Trades", |api| {
                let req = pa::TradesRequest {
                    account_id: account.clone(),
                    limit,
                    interval: Some(Interval {
                        start_time: Some(to_timestamp(range.from)),
                        end_time: Some(to_timestamp(range.to)),
                    }),
                };
                async move { api.accounts().trades(req).await }
            })
            .await?;
        let now = brokapi_runtime::now();
        let index = &self.inner.index;
        Ok(resp
            .trades
            .iter()
            .map(|t| map_trade(t, index.lookup(&t.order_id, &t.comment), now))
            .collect())
    }

    /// Opens a `SubscribeTrades` stream for the account. Envelopes carry `Event::Execution`, with the cid
    /// when the order is ours. Lossless buffer, errors as in `subscribe_order_state`.
    async fn subscribe_executions(&self) -> Result<SubscriptionId, Error> {
        let account = self.account();
        let opts = self.lossless();
        self.register_sub(UdSubKind::Executions, opts, "SubscribeTrades", |sub| {
            let api = self.inner.rpc.api.clone();
            let open: OpenFn<po::SubscribeTradesResponse> = Arc::new(move || {
                let api = api.clone();
                let req = po::SubscribeTradesRequest {
                    account_id: account.clone(),
                };
                Box::pin(async move {
                    api.orders()
                        .subscribe_trades(req)
                        .await
                        .map(|r| r.into_inner())
                })
            });
            (open, Self::wrap(sub, UdEvent::Trades))
        })
        .await
    }
}

#[async_trait]
impl PortfolioSource for UserDataConnector {
    /// The account state (`GetAccount`): one balance per currency and one position per holding.
    ///
    /// Quantity is signed (short is negative). Finam reports no blocked amount per currency, so
    /// `blocked` is 0. The average price is missing for futures positions.
    ///
    /// # Errors
    ///
    /// [`Error::Rejected`] (unknown account), [`Error::RateLimited`], [`Error::Transport`].
    async fn portfolio(&self) -> Result<Portfolio, Error> {
        let account = self.account();
        let resp = self
            .inner
            .rpc
            .read("GetAccount", |api| {
                let req = pa::GetAccountRequest {
                    account_id: account.clone(),
                };
                async move { api.accounts().get_account(req).await }
            })
            .await?;
        Ok(map_account(&resp, brokapi_runtime::now()))
    }

    /// The positions of [`portfolio`](PortfolioSource::portfolio). One `GetAccount` request.
    async fn positions(&self) -> Result<Vec<Position>, Error> {
        Ok(self.portfolio().await?.positions)
    }

    /// The balances of [`portfolio`](PortfolioSource::portfolio). One `GetAccount` request.
    async fn balances(&self) -> Result<Vec<Balance>, Error> {
        Ok(self.portfolio().await?.balances)
    }

    /// Streams `SubscribeAccount`: every message is a full account state (`Event::Portfolio`).
    ///
    /// Each event replaces the previous one. `opts` is your choice of buffer policy: `Conflate` keeps
    /// only the latest state, which is what you usually want here.
    async fn subscribe_portfolio(&self, opts: SubscribeOpts) -> Result<SubscriptionId, Error> {
        let account = self.account();
        self.register_sub(UdSubKind::Portfolio, opts, "SubscribeAccount", |sub| {
            let api = self.inner.rpc.api.clone();
            let open: OpenFn<pa::GetAccountResponse> = Arc::new(move || {
                let api = api.clone();
                let req = pa::GetAccountRequest {
                    account_id: account.clone(),
                };
                Box::pin(async move {
                    api.accounts()
                        .subscribe_account(req)
                        .await
                        .map(|r| r.into_inner())
                })
            });
            (open, Self::wrap(sub, UdEvent::Account))
        })
        .await
    }
}

#[async_trait]
impl StopOrderExecutor for UserDataConnector {
    /// Stop-loss and stop-limit: `PlaceOrder` (STOP / STOP_LIMIT). Take-profit: `PlaceSLTPOrder`
    /// with the take-profit leg only. The cid travels in `comment` (lossless) and in the
    /// `client_order_id` tag, like for plain orders.
    ///
    /// The trigger direction follows the side and the kind: a sell stop-loss triggers when the price
    /// falls, a buy stop-limit when it rises. Checks that need no network run first. The cid is sent
    /// once: after a definite refusal it is free again, after `Unknown` it stays used.
    ///
    /// # Errors
    ///
    /// - [`Error::NotReconciled`], [`Error::Degraded`], [`Error::ShuttingDown`]: the gate is closed.
    /// - [`Error::Rejected`]: a local check failed (`invalid_price`, `invalid_order`,
    ///   `missing_limit_price` for a stop-limit without a limit price, `unsupported_expiry` for a
    ///   date on a stop-loss, `unsupported_limit_price` for a take-profit with a limit price), the cid
    ///   was sent before (`duplicate_place`), or the broker refused.
    /// - [`Error::RateLimited`]: the local quota is spent or the broker said `RESOURCE_EXHAUSTED`.
    /// - [`Error::Unknown`]: no answer. Look with `active_stop_orders`.
    ///
    /// That the live broker accepts these orders the way the mock does is not verified against the
    /// live API.
    ///
    /// See the [type docs](UserDataConnector) for an example.
    async fn place_stop(&self, req: PlaceStopOrderRequest) -> Result<OrderAck, Error> {
        self.check_command()?;
        let wire = map_stop::build(&self.inner.account_id, &req)?;
        self.inner
            .price_grid
            .check(&req.instrument.key, req.stop_price)?;
        if let Some(p) = req.limit_price {
            self.inner.price_grid.check(&req.instrument.key, p)?;
        }
        self.inner.rpc.acquire(match &wire {
            StopWire::Order(_) => "PlaceOrder",
            StopWire::TakeProfit(_) => "PlaceSLTPOrder",
        })?;
        let _instrument = self.inner.locks.lock(&req.instrument).await;
        self.inner.locks.bind(req.cid, &req.instrument);
        if !self
            .inner
            .stops_sent
            .lock()
            .expect("stops poisoned")
            .insert(req.cid)
        {
            return Err(Error::Rejected {
                code: "duplicate_place".into(),
                message: "auto-retry of place_stop is forbidden".into(),
            });
        }
        self.inner.index.know(req.cid);
        let sent = match wire {
            StopWire::Order(body) => {
                self.inner
                    .rpc
                    .send_command(|api| {
                        let body = body.clone();
                        async move { api.orders().place_order(body).await }
                    })
                    .await
            }
            StopWire::TakeProfit(body) => {
                self.inner
                    .rpc
                    .send_command(|api| {
                        let body = body.clone();
                        async move { api.orders().place_sltp_order(body).await }
                    })
                    .await
            }
        };
        match sent {
            Ok(os) => {
                if let Some(rej) = rejection_of(&os) {
                    self.inner
                        .stops_sent
                        .lock()
                        .expect("stops poisoned")
                        .remove(&req.cid);
                    return Err(rej);
                }
                if os.order_id.is_empty() {
                    return Err(Error::Unknown);
                }
                self.inner.index.bind(req.cid, &os.order_id);
                Ok(OrderAck {
                    cid: req.cid,
                    broker_id: brokapi_core::BrokerOrderId::new(os.order_id.clone()),
                    ts: timestamps(
                        os.accept_at.as_ref().or(os.transact_at.as_ref()),
                        brokapi_runtime::now(),
                    ),
                })
            }
            Err(e) => {
                let err = map_command_err(&e);
                if !matches!(err, Error::Unknown) {
                    // A definite refusal: the broker holds nothing, the cid is clean again.
                    self.inner
                        .stops_sent
                        .lock()
                        .expect("stops poisoned")
                        .remove(&req.cid);
                }
                Err(err)
            }
        }
    }

    /// `CancelOrder` with the stop order's id. That Finam cancels stop orders through the same call
    /// is an assumption from the API layout, not verified against the live API.
    ///
    /// `StopOrderRef::Broker` uses the id as given. `StopOrderRef::Cid` looks the id up like
    /// `cancel` does (one extra `GetOrders` for a cid not seen yet).
    ///
    /// # Errors
    ///
    /// As for `cancel`: [`Error::NotReconciled`] and its siblings, [`Error::RateLimited`],
    /// [`Error::Rejected`] (including code `not_found`), [`Error::Unknown`].
    async fn cancel_stop(&self, stop: StopOrderRef) -> Result<(), Error> {
        self.check_command()?;
        self.inner.rpc.acquire("CancelOrder")?;
        let order_id = match stop {
            StopOrderRef::Broker(id) => id.as_str().to_string(),
            StopOrderRef::Cid(cid) => self.order_id_of(cid).await?,
        };
        let account = self.account();
        let sent = self
            .inner
            .rpc
            .send_command(|api| {
                let req = po::CancelOrderRequest {
                    account_id: account.clone(),
                    order_id: order_id.clone(),
                };
                async move { api.orders().cancel_order(req).await }
            })
            .await;
        match sent {
            Ok(_) => Ok(()),
            Err(e) => Err(map_command_err(&e)),
        }
    }

    /// Stop orders that are waiting for their trigger (`GetOrders`, filtered to stop orders in the
    /// active state).
    ///
    /// `cid` is `Some` only for orders this library placed. A stop order set by hand has none, and can
    /// still be cancelled by its broker id.
    ///
    /// # Errors
    ///
    /// [`Error::RateLimited`], [`Error::Rejected`] or [`Error::Transport`], as for any read.
    async fn active_stop_orders(&self) -> Result<Vec<StopOrderSnapshot>, Error> {
        let account = self.account();
        let resp = self
            .inner
            .rpc
            .read("GetOrders", |api| {
                let req = po::OrdersRequest {
                    account_id: account.clone(),
                };
                async move { api.orders().get_orders(req).await }
            })
            .await?;
        let now = brokapi_runtime::now();
        let mut out = Vec::new();
        for os in resp.orders.iter().filter(|os| map_stop::is_stop(os)) {
            // Keep the id <-> cid index current, then show the cid only when it is really ours.
            let (client_order_id, comment) = map_stop::echo_keys(os);
            let cid = self
                .inner
                .index
                .resolve(&os.order_id, &client_order_id, &comment);
            let ours = (cid != crate::index::foreign_cid(&os.order_id)).then_some(cid);
            let snap = map_stop::snapshot(os, ours, now);
            if snap.state == StopOrderState::Active {
                out.push(snap);
            }
        }
        Ok(out)
    }

    /// No separate stream: `SubscribeOrders` delivers orders, and whether stop orders are among them
    /// is not verified against the live API. Use `subscribe_order_state`.
    ///
    /// # Errors
    ///
    /// Always [`Error::Unsupported`] (capability `stop_order_stream`).
    async fn subscribe_stop_orders(&self) -> Result<SubscriptionId, Error> {
        Err(unsupported("stop_order_stream"))
    }
}
