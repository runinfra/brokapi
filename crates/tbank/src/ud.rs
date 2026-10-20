//! T-Bank user data connector, one account.
//!
//! [`UserDataConnector`] places and cancels orders, reports their state, and reads the portfolio,
//! the own trades and the stop orders of ONE account.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use brokapi_core::{
    Backpressure, Balance, BrokerOrderId, ClientOrderId, Envelope, Error, Execution,
    ExecutionSource, OrderAck, OrderExecutor, OrderSnapshot, OrderType, PlaceIdempotency,
    PlaceOrderRequest, PlaceStopOrderRequest, Portfolio, PortfolioSource, Position, Qty,
    ReconcileReport, ReplaceOrderRequest, RoundMode, ShutdownMode, ShutdownReport,
    StopOrderExecutor, StopOrderRef, StopOrderSnapshot, SubscribeOpts, SubscriptionId, TimeRange,
    UserDataCaps,
};
use brokapi_runtime::{
    BrokerLimiter, CommandGate, InstrumentLocks, PriceGrid, ShutdownController, reconcile_ledger,
};
use brokapi_sansio::{BrokerCore, EventSink};
use brokapi_tbank_protocol::TbankClient;
use brokapi_tbank_protocol::v1 as pb;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::config::UserDataConfig;
use crate::core::ud::{StreamKind, UdCore, UdFrame, UdSubKind};
use crate::emit::Emitter;
use crate::error::{config, is_not_found, map_status, transport};
use crate::ids::{instrument_ref, nanos_to_proto, price_to_quotation, require_whole_lots, stamps};
use crate::map::{execution, order as map_order, portfolio as map_portfolio, stop as map_stop};
use crate::raw::TbankRaw;
use crate::stream::{LastError, supervise};
use crate::util::{CommandOutcome, SubIdGen, command, query};

/// Pages of operation history read at most per `executions` call (1000 operations each).
const MAX_HISTORY_PAGES: usize = 20;

/// Stop orders this connector placed: the broker's `StopOrder` does not carry our idempotency key,
/// so the cid <-> broker id link lives only here (lost on restart, see the T-Bank page of the book).
#[derive(Default)]
struct StopBook {
    /// `None` = sent, the broker's id is not known (yet).
    by_cid: HashMap<ClientOrderId, Option<String>>,
    by_id: HashMap<String, ClientOrderId>,
}

impl StopBook {
    /// `false` when this cid was already sent: the library never sends the same stop order twice.
    fn begin(&mut self, cid: ClientOrderId) -> bool {
        self.by_cid.insert(cid, None).is_none()
    }

    fn bind(&mut self, cid: ClientOrderId, id: &str) {
        self.by_cid.insert(cid, Some(id.to_string()));
        self.by_id.insert(id.to_string(), cid);
    }

    /// The broker refused for sure: the cid is clean again.
    fn forget_cid(&mut self, cid: ClientOrderId) {
        self.by_cid.remove(&cid);
    }

    fn forget_id(&mut self, id: &str) {
        if let Some(cid) = self.by_id.remove(id) {
            self.by_cid.remove(&cid);
        }
    }

    fn id_of(&self, cid: ClientOrderId) -> Option<String> {
        self.by_cid.get(&cid).cloned().flatten()
    }

    fn cid_of(&self, id: &str) -> Option<ClientOrderId> {
        self.by_id.get(id).copied()
    }
}

struct Shared {
    client: TbankClient,
    limiter: Arc<BrokerLimiter>,
    emitter: Arc<Emitter>,
    account: String,
    core: Mutex<UdCore>,
    /// Commands on one instrument go one at a time, in arrival order.
    locks: InstrumentLocks,
    price_grid: PriceGrid,
    stops: Mutex<StopBook>,
    ctl: Mutex<ShutdownController>,
    waiters: Mutex<HashMap<SubscriptionId, oneshot::Sender<Result<(), String>>>>,
    sub_ids: SubIdGen,
    streams: Mutex<HashMap<StreamKind, (CancellationToken, JoinHandle<()>)>>,
    frames: mpsc::Sender<UdFrame>,
    last_error: Arc<LastError>,
    request_timeout: Duration,
    ping: Duration,
    order_state_buffer: usize,
    sandbox: bool,
    cancel: CancellationToken,
}

impl Shared {
    fn with_core<R>(&self, f: impl FnOnce(&mut UdCore, &mut EventSink) -> R) -> R {
        let mut sink = EventSink::new();
        let (r, reconnected) = {
            let mut core = self.core.lock().expect("ud core poisoned");
            let r = f(&mut core, &mut sink);
            for u in core.take_updates() {
                if let Some(tx) = self
                    .waiters
                    .lock()
                    .expect("waiters poisoned")
                    .remove(&u.sub)
                {
                    let _ = tx.send(u.result);
                }
            }
            (r, core.take_reconnected())
        };
        self.emitter.emit(sink.drain());
        for kind in reconnected {
            if kind == StreamKind::OrderState {
                self.refresh_orders();
            }
        }
        r
    }

    /// After the order stream reopened: orders may have changed meanwhile, so re-read the live ones
    /// and let the core emit what differs. Orders that ended during the gap are not in `GetOrders`:
    /// the client finds them with `order_state` / `executions` (documented on the T-Bank page of the book).
    fn refresh_orders(&self) {
        let (client, limiter, tx) = (
            self.client.clone(),
            Arc::clone(&self.limiter),
            self.frames.clone(),
        );
        let (account, timeout, cancel) = (
            self.account.clone(),
            self.request_timeout,
            self.cancel.child_token(),
        );
        tokio::spawn(async move {
            if limiter
                .acquire("GetOrders", brokapi_runtime::now())
                .is_err()
            {
                return;
            }
            let mut c = client.orders();
            let req = pb::GetOrdersRequest {
                account_id: account,
                advanced_filters: None,
            };
            let res = tokio::select! {
                _ = cancel.cancelled() => return,
                r = query(timeout, c.get_orders(req)) => r,
            };
            match res {
                Ok(r) => {
                    let _ = tx.send(UdFrame::ActiveOrders(r)).await;
                }
                Err(e) => warn!(error = %e, "order refresh after reconnect failed"),
            }
        });
    }

    async fn pump(self: Arc<Self>, mut rx: mpsc::Receiver<UdFrame>) {
        loop {
            let backlog = self.emitter.has_backlog();
            tokio::select! {
                _ = self.cancel.cancelled() => return,
                frame = rx.recv() => {
                    let Some(frame) = frame else { return };
                    self.with_core(|c, s| c.on_frame(frame, brokapi_runtime::now(), s));
                }
                _ = tokio::time::sleep(Duration::from_millis(20)), if backlog => {
                    self.emitter.flush();
                }
            }
        }
    }

    fn start_stream(&self, kind: StreamKind) {
        let mut streams = self.streams.lock().expect("streams poisoned");
        if streams.contains_key(&kind) {
            return;
        }
        let token = self.cancel.child_token();
        let (client, tx, last_error) = (
            self.client.clone(),
            self.frames.clone(),
            Arc::clone(&self.last_error),
        );
        let accounts = vec![self.account.clone()];
        let ms = self.ping.as_millis().clamp(5_000, 120_000) as i32;
        let stale = self.ping * 3;
        let t = token.clone();
        let handle = match kind {
            StreamKind::OrderState => {
                let open = move || {
                    let mut c = client.orders_stream();
                    let req = pb::OrderStateStreamRequest {
                        accounts: accounts.clone(),
                        ping_delay_millis: Some(ms),
                    };
                    async move { c.order_state_stream(req).await.map(|r| r.into_inner()) }
                };
                tokio::spawn(supervise(
                    "order_state",
                    open,
                    move || UdFrame::Connected(kind),
                    UdFrame::OrderState,
                    tx,
                    stale,
                    last_error,
                    t,
                ))
            }
            StreamKind::Trades => {
                let open = move || {
                    let mut c = client.orders_stream();
                    let req = pb::TradesStreamRequest {
                        accounts: accounts.clone(),
                        ping_delay_ms: Some(ms),
                    };
                    async move { c.trades_stream(req).await.map(|r| r.into_inner()) }
                };
                tokio::spawn(supervise(
                    "trades",
                    open,
                    move || UdFrame::Connected(kind),
                    UdFrame::Trades,
                    tx,
                    stale,
                    last_error,
                    t,
                ))
            }
            StreamKind::Portfolio => {
                let open = move || {
                    let mut c = client.operations_stream();
                    let req = pb::PortfolioStreamRequest {
                        accounts: accounts.clone(),
                        ping_settings: Some(pb::PingDelaySettings {
                            ping_delay_ms: Some(ms),
                        }),
                    };
                    async move { c.portfolio_stream(req).await.map(|r| r.into_inner()) }
                };
                tokio::spawn(supervise(
                    "portfolio",
                    open,
                    move || UdFrame::Connected(kind),
                    UdFrame::Portfolio,
                    tx,
                    stale,
                    last_error,
                    t,
                ))
            }
            StreamKind::Positions => {
                let open = move || {
                    let mut c = client.operations_stream();
                    let req = pb::PositionsStreamRequest {
                        accounts: accounts.clone(),
                        with_initial_positions: true,
                        ping_settings: Some(pb::PingDelaySettings {
                            ping_delay_ms: Some(ms),
                        }),
                    };
                    async move { c.positions_stream(req).await.map(|r| r.into_inner()) }
                };
                tokio::spawn(supervise(
                    "positions",
                    open,
                    move || UdFrame::Connected(kind),
                    UdFrame::Positions,
                    tx,
                    stale,
                    last_error,
                    t,
                ))
            }
        };
        streams.insert(kind, (token, handle));
    }

    fn stop_stream(&self, kind: StreamKind) {
        if let Some((token, handle)) = self.streams.lock().expect("streams poisoned").remove(&kind)
        {
            token.cancel();
            handle.abort();
        }
    }
}

/// T-Bank user data connector: orders, own trades, portfolio and stop orders of ONE account.
///
/// It implements [`OrderExecutor`], [`ExecutionSource`], [`PortfolioSource`] and
/// [`StopOrderExecutor`]. Commands return their answer directly. Order states, own trades and
/// portfolio changes arrive as [`Envelope`]s in the channel given to [`start`](Self::start), after
/// you subscribe.
///
/// # Rules that protect your money
///
/// - **The gate.** Trading commands return [`Error::NotReconciled`] until [`OrderExecutor::reconcile`]
///   has succeeded once. Call it right after `start` with the cids you have on record (an empty
///   slice if none).
/// - **A place is never repeated.** If a `place` gets no answer (timeout, broken connection, server
///   fault) the result is [`Error::Unknown`] and the library does NOT send it again, because a retry
///   could create a second order. A `cid` is used for one `place` at most: sending it again is
///   refused locally as `duplicate_place`. Look at the order with
///   [`order_state`](OrderExecutor::order_state), and use a NEW `cid` for a new attempt.
/// - **One account.** Messages for another account are dropped. For several accounts of one token
///   start one connector per account and share ONE rate limiter between them.
/// - **Lots.** Order quantities are whole lots. T-Bank prices are per one instrument.
///
/// # Examples
///
/// Place an order, cancel it, and see a refusal. The mock refuses a limit order at one marker price
/// (13.37), where a real broker refuses a price outside its band.
///
/// ```
/// use brokapi_core::{
///     ClientOrderId, Error, InstrumentId, InstrumentKey, NativeId, OrderExecutor, OrderType, PlaceOrderRequest,
///     Price, Qty, ShutdownMode, Side,
/// };
/// use brokapi_runtime::BrokerLimiter;
/// use brokapi_tbank::limits::default_limits;
/// use brokapi_tbank::{UserDataConfig, UserDataConnector};
/// use tokio::sync::mpsc;
/// # use brokapi_mocks::tbank::{MockServer, ACCOUNT_A, TOKEN};
/// # use brokapi_tbank_protocol::TbankConfig;
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let server = MockServer::start(Default::default()).await;
/// # let cfg = TbankConfig::for_mock(TOKEN, &server.endpoint).with_account(ACCOUNT_A);
/// # let refused_price = brokapi_conformance::REJECT_PRICE;
/// let (events_tx, _events) = mpsc::channel(256);
/// let limiter = BrokerLimiter::new(&default_limits());
/// let ud = UserDataConnector::start(UserDataConfig::new(cfg, "tbank-ud-main"), limiter, events_tx).await?;
///
/// let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::Uid("uid-sber".into()));
/// let buy = |cid, price| PlaceOrderRequest {
///     cid,
///     instrument: sber.clone(),
///     side: Side::Buy,
///     order_type: OrderType::Limit,
///     qty: Qty::from(1), // one lot
///     price: Some(price),
/// };
///
/// // 1. The gate is closed until the first reconcile. Nothing was sent.
/// let early = ud.place(buy(ClientOrderId::new_v4(), Price::from(250))).await;
/// assert!(matches!(early, Err(Error::NotReconciled)));
///
/// // 2. Reconcile with the cids you know. A fresh start knows none.
/// let report = ud.reconcile(&[]).await?;
/// assert!(report.confirmed.is_empty());
///
/// // 3. Place, look, cancel.
/// let cid = ClientOrderId::new_v4();
/// let ack = ud.place(buy(cid, Price::from(250))).await?;
/// assert_eq!(ack.cid, cid);
/// assert_eq!(ud.active_orders().await?.len(), 1);
/// ud.cancel(cid).await?;
/// assert!(ud.active_orders().await?.is_empty());
///
/// // 4. A refusal is `Rejected`. The broker did nothing, and the next order is not blocked.
/// let refused = ud.place(buy(ClientOrderId::new_v4(), refused_price)).await;
/// assert!(matches!(refused, Err(Error::Rejected { .. })));
/// assert!(ud.place(buy(ClientOrderId::new_v4(), Price::from(251))).await.is_ok());
///
/// ud.shutdown(ShutdownMode::Immediate).await;
/// # Ok(())
/// # }
/// ```
///
/// # Several accounts
///
/// One token can see several accounts. Start one connector per account. Give each its own `source`
/// and share ONE limiter, because T-Bank counts its limits per user.
///
/// ```
/// use std::sync::Arc;
/// use brokapi_core::{OrderExecutor, ShutdownMode};
/// use brokapi_runtime::BrokerLimiter;
/// use brokapi_tbank::limits::default_limits;
/// use brokapi_tbank::{UserDataConfig, UserDataConnector};
/// use brokapi_tbank_protocol::TbankClient;
/// use tokio::sync::mpsc;
/// # use brokapi_mocks::tbank::{MockServer, TOKEN};
/// # use brokapi_tbank_protocol::TbankConfig;
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let server = MockServer::start(Default::default()).await;
/// # let token_config = TbankConfig::for_mock(TOKEN, &server.endpoint);
/// // `token_config` is your sandbox or production config without an account.
/// let accounts = TbankClient::new(&token_config)?.list_accounts().await?;
///
/// let limiter = BrokerLimiter::new(&default_limits()); // ONE limiter for all accounts
/// let mut connectors = Vec::new();
/// for account in &accounts {
///     let (events_tx, _events) = mpsc::channel(256);
///     let cfg = UserDataConfig::new(
///         token_config.clone().with_account(account.id.clone()),
///         format!("tbank-ud-{}", account.id),
///     );
///     let ud = UserDataConnector::start(cfg, Arc::clone(&limiter), events_tx).await?;
///     ud.reconcile(&[]).await?;
///     connectors.push(ud);
/// }
/// assert_eq!(connectors.len(), accounts.len());
/// assert_ne!(connectors[0].account_id(), connectors[1].account_id());
/// for ud in connectors {
///     ud.shutdown(ShutdownMode::Immediate).await;
/// }
/// # Ok(())
/// # }
/// ```
pub struct UserDataConnector {
    inner: Arc<Shared>,
    pump: JoinHandle<()>,
}

impl UserDataConnector {
    /// Resolves the account (`cfg.tbank.account_id`, or the token's only open account) and starts.
    /// Commands are refused with `NotReconciled` until [`OrderExecutor::reconcile`] succeeds.
    ///
    /// With an `account_id` in the config nothing is sent at start. Without one the connector calls
    /// `GetAccounts` once, and takes the only open account. Streams are opened later, when you
    /// subscribe. `limiter` should be the one limiter of the whole user (see [`crate::limits`]).
    ///
    /// # Errors
    ///
    /// - [`Error::Config`] when the token or endpoint is invalid, when no `account_id` is set and the
    ///   token has no open account, when it has several (set `tbank.account_id`), or when the
    ///   broker does not accept the token.
    /// - [`Error::Transport`] when the `GetAccounts` call fails on the network (only without an
    ///   `account_id`), and [`Error::RateLimited`] when it is refused for rate.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::{Error, ShutdownMode};
    /// use brokapi_runtime::BrokerLimiter;
    /// use brokapi_tbank::limits::default_limits;
    /// use brokapi_tbank::{UserDataConfig, UserDataConnector};
    /// use tokio::sync::mpsc;
    /// # use brokapi_mocks::tbank::{MockServer, ACCOUNT_A, TOKEN};
    /// # use brokapi_tbank_protocol::TbankConfig;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = MockServer::start(Default::default()).await;
    /// # let token_config = TbankConfig::for_mock(TOKEN, &server.endpoint);
    /// let limiter = BrokerLimiter::new(&default_limits());
    /// let (events_tx, _events) = mpsc::channel(256);
    ///
    /// // This token sees two accounts, so the connector will not guess: it refuses to start.
    /// let cfg = UserDataConfig::new(token_config.clone(), "tbank-ud");
    /// let result = UserDataConnector::start(cfg, limiter.clone(), events_tx.clone()).await;
    /// assert!(matches!(result, Err(Error::Config(_))));
    ///
    /// // Name the account and it starts.
    /// let cfg = UserDataConfig::new(token_config.with_account(ACCOUNT_A), "tbank-ud");
    /// let ud = UserDataConnector::start(cfg, limiter, events_tx).await?;
    /// assert_eq!(ud.account_id(), ACCOUNT_A);
    /// ud.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn start(
        cfg: UserDataConfig,
        limiter: Arc<BrokerLimiter>,
        out: mpsc::Sender<Envelope>,
    ) -> Result<Self, Error> {
        let client = TbankClient::new(&cfg.tbank).map_err(|e| config(e.to_string()))?;
        let account = match cfg.tbank.account_id.clone() {
            Some(a) => a,
            None => pick_account(&client, cfg.tbank.request_timeout).await?,
        };
        let (frames, rx) = mpsc::channel(1024);
        let ctl = ShutdownController::new(CommandGate::new());
        let cancel = ctl.child_token();
        let inner = Arc::new(Shared {
            client,
            limiter,
            emitter: Emitter::new(cfg.source, out),
            core: Mutex::new(UdCore::new(account.clone())),
            locks: InstrumentLocks::new(),
            price_grid: cfg.price_grid.clone(),
            stops: Mutex::new(StopBook::default()),
            account,
            ctl: Mutex::new(ctl),
            waiters: Mutex::new(HashMap::new()),
            sub_ids: SubIdGen::new(1000),
            streams: Mutex::new(HashMap::new()),
            frames,
            last_error: Arc::new(LastError::default()),
            request_timeout: cfg.tbank.request_timeout,
            ping: cfg.ping_interval,
            order_state_buffer: cfg.order_state_buffer,
            sandbox: cfg.tbank.sandbox,
            cancel,
        });
        let pump = tokio::spawn(Arc::clone(&inner).pump(rx));
        Ok(Self { inner, pump })
    }

    /// The brokerage account this connector works on: the configured one, or the token's only open
    /// account.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::ShutdownMode;
    /// use brokapi_runtime::BrokerLimiter;
    /// use brokapi_tbank::limits::default_limits;
    /// use brokapi_tbank::{UserDataConfig, UserDataConnector};
    /// use tokio::sync::mpsc;
    /// # use brokapi_mocks::tbank::{MockServer, ACCOUNT_B, TOKEN};
    /// # use brokapi_tbank_protocol::TbankConfig;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = MockServer::start(Default::default()).await;
    /// # let cfg = TbankConfig::for_mock(TOKEN, &server.endpoint).with_account(ACCOUNT_B);
    /// let (events_tx, _events) = mpsc::channel(16);
    /// let ud = UserDataConnector::start(UserDataConfig::new(cfg, "tbank-ud"), BrokerLimiter::new(&default_limits()), events_tx).await?;
    /// assert_eq!(ud.account_id(), ACCOUNT_B);
    /// # ud.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    pub fn account_id(&self) -> &str {
        &self.inner.account
    }

    /// Any other method of the T-Bank API with this connector's token, endpoint and rate limiter.
    /// `account_id()` of the result is this connector's account. See [`TbankRaw`].
    ///
    /// The raw layer does not know the connector's order registry: orders placed through it are not
    /// tracked (no events, no `reconcile`). Use the trait methods for orders. Use `call_command`
    /// for anything that changes the account, so a lost answer is [`Error::Unknown`].
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_runtime::BrokerLimiter;
    /// use brokapi_tbank::limits::default_limits;
    /// use brokapi_tbank::{UserDataConfig, UserDataConnector};
    /// use brokapi_core::ShutdownMode;
    /// use brokapi_tbank_protocol::v1 as pb;
    /// use tokio::sync::mpsc;
    /// # use brokapi_mocks::tbank::{MockServer, ACCOUNT_A, TOKEN};
    /// # use brokapi_tbank_protocol::TbankConfig;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = MockServer::start(Default::default()).await;
    /// # let cfg = TbankConfig::for_mock(TOKEN, &server.endpoint).with_account(ACCOUNT_A);
    /// let (events_tx, _events) = mpsc::channel(16);
    /// let ud = UserDataConnector::start(UserDataConfig::new(cfg, "tbank-ud"), BrokerLimiter::new(&default_limits()), events_tx).await?;
    /// let raw = ud.raw();
    /// assert_eq!(raw.account_id(), Some(ACCOUNT_A));
    /// let account = ud.account_id().to_string();
    /// // A read of the portfolio, spelled out through the raw layer.
    /// let portfolio = raw
    ///     .call("GetPortfolio", |c| async move {
    ///         c.operations().get_portfolio(pb::PortfolioRequest { account_id: account, currency: None }).await
    ///     })
    ///     .await?;
    /// assert!(!portfolio.positions.is_empty());
    /// # ud.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    pub fn raw(&self) -> TbankRaw {
        TbankRaw::new(
            self.inner.client.clone(),
            Arc::clone(&self.inner.limiter),
            self.inner.request_timeout,
            Some(self.inner.account.clone()),
        )
    }

    /// The price steps this connector checks order prices against. Fill it from
    /// [`TradingParams`](brokapi_core::TradingParams) (`grid.learn(&params)`) to have a price off the
    /// step refused before anything is sent. See [`PriceGrid`].
    pub fn price_grid(&self) -> &PriceGrid {
        &self.inner.price_grid
    }

    /// What this connector can do.
    ///
    /// `place_idempotency` is `LabelOnly` on purpose: `order_id` is documented as an idempotency key
    /// ("для целей идемпотентности"; error 30057 mentions a duplicate with a found report), but
    /// nobody has measured the broker's behavior on a repeated key, and the library requires `LabelOnly`
    /// until it is confirmed on the sandbox (a live run). The library never repeats a
    /// `place` itself regardless.
    ///
    /// Replace and stop orders are supported, and own trades are streamed. There is no websocket RPC
    /// and no ordered command channel: commands are plain gRPC calls, serialized per instrument by
    /// the connector. `sandbox` is the `sandbox` flag of the config.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::{PlaceIdempotency, ShutdownMode};
    /// use brokapi_runtime::BrokerLimiter;
    /// use brokapi_tbank::limits::default_limits;
    /// use brokapi_tbank::{UserDataConfig, UserDataConnector};
    /// use tokio::sync::mpsc;
    /// # use brokapi_mocks::tbank::{MockServer, ACCOUNT_A, TOKEN};
    /// # use brokapi_tbank_protocol::TbankConfig;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = MockServer::start(Default::default()).await;
    /// # let cfg = TbankConfig::for_mock(TOKEN, &server.endpoint).with_account(ACCOUNT_A);
    /// let (events_tx, _events) = mpsc::channel(16);
    /// let ud = UserDataConnector::start(UserDataConfig::new(cfg, "tbank-ud"), BrokerLimiter::new(&default_limits()), events_tx).await?;
    /// let caps = ud.caps();
    /// assert_eq!(caps.place_idempotency, PlaceIdempotency::LabelOnly);
    /// assert!(caps.supports_replace && caps.supports_stop_orders && caps.streams_executions);
    /// assert!(caps.sandbox, "a mock config is a sandbox config");
    /// # ud.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    pub fn caps(&self) -> UserDataCaps {
        UserDataCaps {
            place_idempotency: PlaceIdempotency::LabelOnly,
            ws_rpc: false,
            ordered_command_channel: false,
            supports_replace: true,
            // StopOrdersService, through the `StopOrderExecutor` trait.
            supports_stop_orders: true,
            streams_executions: true,
            sandbox: self.inner.sandbox,
        }
    }

    /// Stops the connector. Orders already at the exchange are NOT touched.
    ///
    /// The command gate closes first. Placements still in flight are abandoned locally: each becomes
    /// `Unknown` with one event, and its `cid` is listed in `inflight_abandoned`. Look at those orders
    /// with a new connector (`reconcile`, `order_state`) before you place anything again.
    ///
    /// With [`ShutdownMode::Graceful`] the open server streams are closed (the broker has no
    /// unsubscribe message, ending the call is the way) and `unsubscribed` counts these streams (a
    /// portfolio subscription uses two). The event backlog gets until the deadline to drain into your
    /// channel. With [`ShutdownMode::Immediate`] nothing is waited for and `unsubscribed` is 0.
    /// `unsubscribe_failed` is always 0 here. The method consumes the connector.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use brokapi_core::{ExecutionSource, OrderExecutor, ShutdownMode};
    /// use brokapi_runtime::BrokerLimiter;
    /// use brokapi_tbank::limits::default_limits;
    /// use brokapi_tbank::{UserDataConfig, UserDataConnector};
    /// use tokio::sync::mpsc;
    /// # use brokapi_mocks::tbank::{MockServer, ACCOUNT_A, TOKEN};
    /// # use brokapi_tbank_protocol::TbankConfig;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = MockServer::start(Default::default()).await;
    /// # let cfg = TbankConfig::for_mock(TOKEN, &server.endpoint).with_account(ACCOUNT_A);
    /// let (events_tx, _events) = mpsc::channel(256);
    /// let ud = UserDataConnector::start(UserDataConfig::new(cfg, "tbank-ud"), BrokerLimiter::new(&default_limits()), events_tx).await?;
    /// ud.subscribe_order_state().await?;
    /// ud.subscribe_executions().await?;
    ///
    /// let report = ud.shutdown(ShutdownMode::Graceful { deadline: Duration::from_secs(2) }).await;
    /// assert_eq!(report.unsubscribed, 2, "two streams were open");
    /// assert!(report.inflight_abandoned.is_empty());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn shutdown(self, mode: ShutdownMode) -> ShutdownReport {
        let inner = &self.inner;
        let now = brokapi_runtime::now();
        inner
            .ctl
            .lock()
            .expect("ctl poisoned")
            .gate_mut()
            .begin_shutdown();

        // In-flight placements are abandoned locally: Unknown, one event each. Exchange orders stay.
        let mut sink = EventSink::new();
        let abandoned = inner
            .core
            .lock()
            .expect("ud core poisoned")
            .abandon_inflight(now, &mut sink);
        inner.emitter.emit(sink.drain());

        // Server-side streams have no unsubscribe message: ending the call is the protocol's way.
        let open: Vec<StreamKind> = inner
            .streams
            .lock()
            .expect("streams poisoned")
            .keys()
            .copied()
            .collect();
        let mut unsubscribed = 0;
        if let ShutdownMode::Graceful { deadline } = mode {
            for kind in &open {
                inner.stop_stream(*kind);
                unsubscribed += 1;
            }
            let end = brokapi_runtime::now() + deadline;
            while inner.emitter.has_backlog() && brokapi_runtime::now() < end {
                inner.emitter.flush();
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }
        {
            let mut ctl = inner.ctl.lock().expect("ctl poisoned");
            let mut core = inner.core.lock().expect("ud core poisoned");
            ctl.shutdown_local(mode, now, core.orders_mut(), || (0, 0));
        }
        inner.cancel.cancel();
        let _ = self.pump.await;
        ShutdownReport {
            unsubscribed,
            unsubscribe_failed: 0,
            inflight_abandoned: abandoned,
        }
    }

    fn check_command(&self) -> Result<(), Error> {
        let mut ctl = self.inner.ctl.lock().expect("ctl poisoned");
        if self.inner.emitter.is_degraded() {
            ctl.gate_mut().mark_degraded();
        }
        ctl.gate().check_command()
    }

    fn acquire(&self, method: &str) -> Result<(), Error> {
        self.inner.limiter.acquire(method, brokapi_runtime::now())
    }

    fn account(&self) -> String {
        self.inner.account.clone()
    }

    async fn subscribe(
        &self,
        kind: UdSubKind,
        opts: SubscribeOpts,
    ) -> Result<SubscriptionId, Error> {
        let inner = &self.inner;
        if let Some(id) = inner.core.lock().expect("ud core poisoned").sub(kind) {
            return Ok(id);
        }
        let id = inner.sub_ids.next();
        inner.emitter.register(id, None, opts.backpressure);
        let (tx, rx) = oneshot::channel();
        inner
            .waiters
            .lock()
            .expect("waiters poisoned")
            .insert(id, tx);
        inner
            .core
            .lock()
            .expect("ud core poisoned")
            .add_sub(id, kind);
        let streams: &[StreamKind] = match kind {
            UdSubKind::OrderState => &[StreamKind::OrderState],
            UdSubKind::Executions => &[StreamKind::Trades],
            UdSubKind::Portfolio => &[StreamKind::Portfolio, StreamKind::Positions],
        };
        for s in streams {
            inner.start_stream(*s);
        }
        let outcome = tokio::time::timeout(inner.request_timeout, rx).await;
        let failure = match outcome {
            Ok(Ok(Ok(()))) => return Ok(id),
            Ok(Ok(Err(reason))) => Error::Rejected {
                code: reason,
                message: "subscription refused".into(),
            },
            _ => transport(format!(
                "subscription not confirmed within {:?} ({})",
                inner.request_timeout,
                inner
                    .last_error
                    .get()
                    .unwrap_or_else(|| "no answer from the stream".into())
            )),
        };
        inner.waiters.lock().expect("waiters poisoned").remove(&id);
        inner
            .core
            .lock()
            .expect("ud core poisoned")
            .remove_sub(kind);
        inner.emitter.unregister(id);
        for s in streams {
            inner.stop_stream(*s);
        }
        Err(failure)
    }

    /// Runs `f` on the core and delivers the events it produced.
    fn core<R>(&self, f: impl FnOnce(&mut UdCore, &mut EventSink) -> R) -> R {
        self.inner.with_core(f)
    }

    async fn get_state(&self, order_id: String, by_request: bool) -> Result<pb::OrderState, Error> {
        self.acquire("GetOrderState")?;
        let req = pb::GetOrderStateRequest {
            account_id: self.account(),
            order_id,
            price_type: 0,
            order_id_type: Some(if by_request {
                pb::OrderIdType::Request as i32
            } else {
                pb::OrderIdType::Exchange as i32
            }),
        };
        let mut c = self.inner.client.orders();
        match tokio::time::timeout(self.inner.request_timeout, c.get_order_state(req)).await {
            Ok(Ok(r)) => Ok(r.into_inner()),
            Ok(Err(s)) if is_not_found(&s) => Err(Error::Rejected {
                code: "NOT_FOUND".into(),
                message: "the broker does not know this order".into(),
            }),
            Ok(Err(s)) => Err(map_status(&s)),
            Err(_) => Err(transport("request timed out")),
        }
    }

    async fn get_active(&self) -> Result<Vec<OrderSnapshot>, Error> {
        self.acquire("GetOrders")?;
        let req = pb::GetOrdersRequest {
            account_id: self.account(),
            advanced_filters: None,
        };
        let mut c = self.inner.client.orders();
        let r = query(self.inner.request_timeout, c.get_orders(req)).await?;
        let now = brokapi_runtime::now();
        let mut core = self.inner.core.lock().expect("ud core poisoned");
        Ok(r.orders
            .iter()
            .map(|o| {
                let parts = map_order::parts_from_state(o);
                core.note_order(&parts);
                map_order::snapshot(&parts, now)
            })
            .collect())
    }
}

/// The token's only open account; several (or none) is a configuration error.
async fn pick_account(client: &TbankClient, timeout: Duration) -> Result<String, Error> {
    let mut c = client.users();
    let req = pb::GetAccountsRequest {
        status: Some(pb::AccountStatus::Open as i32),
    };
    let r = query(timeout, c.get_accounts(req)).await?;
    match r.accounts.as_slice() {
        [] => Err(config(
            "the token has no open account (a sandbox token needs an account opened with SandboxService/OpenSandboxAccount)",
        )),
        [one] => Ok(one.id.clone()),
        many => Err(config(format!(
            "the token sees {} accounts: set tbank.account_id (TbankClient::list_accounts shows the ids)",
            many.len()
        ))),
    }
}

#[async_trait]
impl OrderExecutor for UserDataConnector {
    /// Places an order (`PostOrder`). `req.cid` goes to the broker as `order_id`, its idempotency key.
    ///
    /// Units: `qty` is a whole number of LOTS (at least 1), `price` is per one instrument. The price is
    /// converted to units and nano. It is not rounded to the price step, the broker checks that.
    /// A limit order needs a positive price. A market order ignores the price. Commands on one
    /// instrument go one at a time, in the order they arrive.
    ///
    /// **Never repeated.** If the call gets no clear answer, the result is [`Error::Unknown`] and the
    /// library does not send it again, because a retry could create a second order. The `cid` is used up:
    /// placing it again is refused locally as `duplicate_place`. First ask
    /// [`order_state`](Self::order_state) or [`active_orders`](Self::active_orders), and place a
    /// new order under a NEW `cid` only when the broker has none.
    ///
    /// # Errors
    ///
    /// - [`Error::NotReconciled`] before the first successful [`reconcile`](Self::reconcile),
    ///   [`Error::Degraded`] after an event buffer overflow, [`Error::ShuttingDown`] during
    ///   shutdown. Nothing was sent.
    /// - [`Error::Rejected`] with code `invalid_qty` (not a whole number of lots), `missing_price`
    ///   (limit order without a positive price) or `duplicate_place` (this `cid` was placed before).
    ///   Nothing was sent.
    /// - [`Error::Rejected`] when the broker refuses. The code is the gRPC status name (for example
    ///   `InvalidArgument`) and the message is the broker's error code. The same code `REJECTED` is
    ///   used when the broker accepted the call but reports the order as rejected. The broker did
    ///   nothing, and the `cid` stays used.
    /// - [`Error::RateLimited`] from the local limiter: refused before the `cid` is registered, so the
    ///   same `cid` can be used again. If the broker itself says `ResourceExhausted`, the `cid` is used up.
    /// - [`Error::Config`] when the broker does not accept the token (`Unauthenticated`).
    /// - [`Error::Unknown`] on a timeout (`request_timeout`), a broken connection or a server fault.
    ///   The order may or may not exist.
    ///
    /// # Examples
    ///
    /// ```
    /// # use brokapi_core::{Decimal, dec};
    /// use brokapi_core::{
    ///     ClientOrderId, Error, InstrumentId, InstrumentKey, NativeId, OrderExecutor, OrderType, PlaceOrderRequest,
    ///     Price, Qty, ShutdownMode, Side,
    /// };
    /// use brokapi_runtime::BrokerLimiter;
    /// use brokapi_tbank::limits::default_limits;
    /// use brokapi_tbank::{UserDataConfig, UserDataConnector};
    /// use tokio::sync::mpsc;
    /// # use brokapi_mocks::tbank::{MockServer, ACCOUNT_A, TOKEN};
    /// # use brokapi_tbank_protocol::TbankConfig;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = MockServer::start(Default::default()).await;
    /// # let cfg = TbankConfig::for_mock(TOKEN, &server.endpoint).with_account(ACCOUNT_A);
    /// # let refused_price = brokapi_conformance::REJECT_PRICE;
    /// let (events_tx, _events) = mpsc::channel(256);
    /// let ud = UserDataConnector::start(UserDataConfig::new(cfg, "tbank-ud"), BrokerLimiter::new(&default_limits()), events_tx).await?;
    /// ud.reconcile(&[]).await?;
    /// let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::Uid("uid-sber".into()));
    /// let order = |cid, qty, price| PlaceOrderRequest {
    ///     cid,
    ///     instrument: sber.clone(),
    ///     side: Side::Buy,
    ///     order_type: OrderType::Limit,
    ///     qty,
    ///     price: Some(price),
    /// };
    ///
    /// // The normal path.
    /// let cid = ClientOrderId::new_v4();
    /// let ack = ud.place(order(cid, Qty::from(1), Price::from(250))).await?;
    /// assert_eq!(ack.cid, cid);
    ///
    /// // The same cid again is refused locally, nothing is sent.
    /// match ud.place(order(cid, Qty::from(1), Price::from(250))).await {
    ///     Err(Error::Rejected { code, .. }) => assert_eq!(code, "duplicate_place"),
    ///     other => panic!("expected duplicate_place, got {other:?}"),
    /// }
    ///
    /// // Half a lot is refused before anything is sent.
    /// let half = ud.place(order(ClientOrderId::new_v4(), Qty::new(dec!(0.5)), Price::from(250))).await;
    /// assert!(matches!(half, Err(Error::Rejected { .. })));
    ///
    /// // The broker refuses a price (in the mock: 13.37). That is `Rejected`: it did nothing.
    /// let refused = ud.place(order(ClientOrderId::new_v4(), Qty::from(1), refused_price)).await;
    /// assert!(matches!(refused, Err(Error::Rejected { .. })));
    /// # ud.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// When the answer is lost, the result is `Unknown`. Here the mock answers 1.5 s late and the
    /// connector gives up after 200 ms. The order still reaches the exchange, so the right move is to
    /// look, not to place again.
    ///
    /// ```
    /// use std::sync::atomic::Ordering;
    /// use std::sync::Arc;
    /// use std::time::Duration;
    /// use brokapi_core::{
    ///     ClientOrderId, Error, InstrumentId, InstrumentKey, NativeId, OrderExecutor, OrderType, PlaceOrderRequest,
    ///     Price, Qty, ShutdownMode, Side,
    /// };
    /// use brokapi_runtime::BrokerLimiter;
    /// use brokapi_tbank::limits::default_limits;
    /// use brokapi_tbank::{UserDataConfig, UserDataConnector};
    /// use tokio::sync::mpsc;
    /// # use brokapi_mocks::tbank::{MockOptions, MockServer, ACCOUNT_A, TOKEN};
    /// # use brokapi_tbank_protocol::TbankConfig;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let opts = Arc::new(MockOptions::default());
    /// # opts.post_delay_ms.store(1500, Ordering::SeqCst);
    /// # let server = MockServer::start(opts).await;
    /// # let cfg = TbankConfig::for_mock(TOKEN, &server.endpoint)
    /// #     .with_account(ACCOUNT_A)
    /// #     .with_request_timeout(Duration::from_millis(200));
    /// let (events_tx, _events) = mpsc::channel(256);
    /// let ud = UserDataConnector::start(UserDataConfig::new(cfg, "tbank-ud"), BrokerLimiter::new(&default_limits()), events_tx).await?;
    /// ud.reconcile(&[]).await?;
    /// let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::Uid("uid-sber".into()));
    ///
    /// let cid = ClientOrderId::new_v4();
    /// let req = PlaceOrderRequest {
    ///     cid,
    ///     instrument: sber,
    ///     side: Side::Buy,
    ///     order_type: OrderType::Limit,
    ///     qty: Qty::from(1),
    ///     price: Some(Price::from(250)),
    /// };
    /// assert!(matches!(ud.place(req.clone()).await, Err(Error::Unknown)));
    ///
    /// // Do NOT place again. Ask the broker, with a bound on the waiting.
    /// let found = tokio::time::timeout(Duration::from_secs(10), async {
    ///     loop {
    ///         if let Ok(order) = ud.order_state(cid).await {
    ///             break order;
    ///         }
    ///         tokio::time::sleep(Duration::from_millis(100)).await;
    ///     }
    /// })
    /// .await?;
    /// assert_eq!(found.cid, cid);
    /// # ud.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    async fn place(&self, req: PlaceOrderRequest) -> Result<OrderAck, Error> {
        self.check_command()?;
        let lots = require_whole_lots(req.qty)?;
        let price = match (req.order_type, req.price) {
            (OrderType::Limit, Some(p)) if p.is_positive() => {
                // Before a quota is spent or the cid is marked.
                self.inner.price_grid.check(&req.instrument.key, p)?;
                Some(price_to_quotation(p)?)
            }
            (OrderType::Limit, _) => {
                return Err(Error::Rejected {
                    code: "missing_price".into(),
                    message: "a limit order needs a positive price".into(),
                });
            }
            (OrderType::Market, _) => None,
        };
        self.acquire("PostOrder")?;
        // Waits for earlier commands on this instrument, released on every exit path.
        let _instrument = self.inner.locks.lock(&req.instrument).await;

        let now = brokapi_runtime::now();
        self.core(|c, _| c.begin_place(&req, lots, now))?;

        let wire = pb::PostOrderRequest {
            quantity: lots,
            price,
            direction: map_order::direction(req.side) as i32,
            account_id: self.account(),
            order_type: map_order::wire_order_type(req.order_type) as i32,
            order_id: req.cid.hyphenated().to_string(),
            instrument_id: instrument_ref(&req.instrument),
            ..Default::default()
        };
        let mut c = self.inner.client.orders();
        let outcome = command(self.inner.request_timeout, c.post_order(wire)).await;
        let cid = req.cid;
        match outcome {
            CommandOutcome::Done(resp) => {
                self.core(|c, s| c.place_response(cid, &resp, now, s));
                if resp.execution_report_status()
                    == pb::OrderExecutionReportStatus::ExecutionReportStatusRejected
                {
                    return Err(Error::Rejected {
                        code: "REJECTED".into(),
                        message: resp.message,
                    });
                }
                Ok(OrderAck {
                    cid,
                    broker_id: BrokerOrderId::new(resp.order_id),
                    ts: stamps(now, None),
                })
            }
            CommandOutcome::Refused(status, err) => {
                let reason = format!("{:?}: {}", status.code(), status.message());
                self.core(|c, s| c.place_rejected(cid, reason, now, s));
                Err(err)
            }
            CommandOutcome::Unknown => {
                self.core(|c, s| c.place_unknown(cid, now, s));
                Err(Error::Unknown)
            }
        }
    }

    /// Cancels an order by the `cid` you placed it with (`CancelOrder` by request id).
    ///
    /// `Ok` means the broker accepted the cancel. The final state arrives on the order stream, or ask
    /// [`order_state`](Self::order_state). The exchange order id is not needed. Commands on the same
    /// instrument go one at a time. The library does not send a cancel twice on its own.
    ///
    /// # Errors
    ///
    /// - [`Error::NotReconciled`], [`Error::Degraded`], [`Error::ShuttingDown`]: the gate is
    ///   closed, nothing was sent.
    /// - [`Error::Rejected`] when the broker refuses. The usual reason is code `NotFound`: the broker
    ///   has no such order (never placed, or already done).
    /// - [`Error::RateLimited`] when a quota is used up.
    /// - [`Error::Unknown`] on a timeout or a broken connection: the order may be cancelled or still
    ///   working. Check [`order_state`](Self::order_state).
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::{ClientOrderId, Error, InstrumentId, InstrumentKey, NativeId, OrderExecutor, OrderType, PlaceOrderRequest, Price, Qty, ShutdownMode, Side};
    /// use brokapi_runtime::BrokerLimiter;
    /// use brokapi_tbank::limits::default_limits;
    /// use brokapi_tbank::{UserDataConfig, UserDataConnector};
    /// use tokio::sync::mpsc;
    /// # use brokapi_mocks::tbank::{MockServer, ACCOUNT_A, TOKEN};
    /// # use brokapi_tbank_protocol::TbankConfig;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = MockServer::start(Default::default()).await;
    /// # let cfg = TbankConfig::for_mock(TOKEN, &server.endpoint).with_account(ACCOUNT_A);
    /// let (events_tx, mut events) = mpsc::channel(256);
    /// let ud = UserDataConnector::start(UserDataConfig::new(cfg, "tbank-ud"), BrokerLimiter::new(&default_limits()), events_tx).await?;
    /// ud.reconcile(&[]).await?;
    /// let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::Uid("uid-sber".into()));
    /// let buy = |cid, price| PlaceOrderRequest {
    ///     cid,
    ///     instrument: sber.clone(),
    ///     side: Side::Buy,
    ///     order_type: OrderType::Limit,
    ///     qty: Qty::from(1),
    ///     price: Some(price),
    /// };
    ///
    /// let cid = ClientOrderId::new_v4();
    /// ud.place(buy(cid, Price::from(250))).await?;
    /// ud.cancel(cid).await?;
    ///
    /// // A cid the broker has never seen is a refusal.
    /// assert!(matches!(ud.cancel(ClientOrderId::new_v4()).await, Err(Error::Rejected { .. })));
    /// # ud.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    async fn cancel(&self, cid: ClientOrderId) -> Result<(), Error> {
        self.check_command()?;
        self.acquire("CancelOrder")?;
        // The instrument comes from the connector's order registry; an unknown order is not serialized.
        let instrument = self.core(|c, _| c.instrument_of(cid));
        let _instrument = match &instrument {
            Some(i) => Some(self.inner.locks.lock(i).await),
            None => None,
        };
        let wire = pb::CancelOrderRequest {
            account_id: self.account(),
            order_id: cid.hyphenated().to_string(),
            order_id_type: Some(pb::OrderIdType::Request as i32),
        };
        let mut c = self.inner.client.orders();
        match command(self.inner.request_timeout, c.cancel_order(wire)).await {
            CommandOutcome::Done(_) => {
                let now = brokapi_runtime::now();
                self.core(|c, s| c.cancel_ok(cid, now, s));
                Ok(())
            }
            CommandOutcome::Refused(_, err) => Err(err),
            CommandOutcome::Unknown => Err(Error::Unknown),
        }
    }

    /// `ReplaceOrder` needs the exchange order id and the quantity: both come from what the connector
    /// already knows about the original, else from one `GetOrderState`. `req.cid` is the new
    /// idempotency key, it replaces the old one.
    ///
    /// `req.qty` of `None` keeps the original lots and `req.price` of `None` keeps the original
    /// price. A new quantity is a whole number of lots. The old order is replaced by a new one,
    /// which gets `req.cid` and a new exchange id. Like `place`, the call is never repeated: a lost
    /// answer is [`Error::Unknown`], and `req.cid` is used up. Look at both orders before you try
    /// again.
    ///
    /// # Errors
    ///
    /// - [`Error::NotReconciled`], [`Error::Degraded`], [`Error::ShuttingDown`]: nothing was sent.
    /// - [`Error::Rejected`] with code `invalid_qty` or `duplicate_place` (the new `cid` was used
    ///   before), both before the replace is sent. [`Error::Rejected`] with code `NotFound` when the
    ///   broker does not know the original order. Any other refusal of the broker, with the gRPC
    ///   status name as code.
    /// - [`Error::RateLimited`], [`Error::Transport`] and [`Error::Config`] while the connector
    ///   reads the original order (one `GetOrderState`, only when it does not know the order).
    /// - [`Error::Unknown`] on a timeout or a broken connection during the replace itself.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::ReplaceOrderRequest;
    /// use brokapi_core::{ClientOrderId, Error, InstrumentId, InstrumentKey, NativeId, OrderExecutor, OrderType, PlaceOrderRequest, Price, Qty, ShutdownMode, Side};
    /// use brokapi_runtime::BrokerLimiter;
    /// use brokapi_tbank::limits::default_limits;
    /// use brokapi_tbank::{UserDataConfig, UserDataConnector};
    /// use tokio::sync::mpsc;
    /// # use brokapi_mocks::tbank::{MockServer, ACCOUNT_A, TOKEN};
    /// # use brokapi_tbank_protocol::TbankConfig;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = MockServer::start(Default::default()).await;
    /// # let cfg = TbankConfig::for_mock(TOKEN, &server.endpoint).with_account(ACCOUNT_A);
    /// let (events_tx, mut events) = mpsc::channel(256);
    /// let ud = UserDataConnector::start(UserDataConfig::new(cfg, "tbank-ud"), BrokerLimiter::new(&default_limits()), events_tx).await?;
    /// ud.reconcile(&[]).await?;
    /// let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::Uid("uid-sber".into()));
    /// let buy = |cid, price| PlaceOrderRequest {
    ///     cid,
    ///     instrument: sber.clone(),
    ///     side: Side::Buy,
    ///     order_type: OrderType::Limit,
    ///     qty: Qty::from(1),
    ///     price: Some(price),
    /// };
    ///
    /// let first = ClientOrderId::new_v4();
    /// ud.place(buy(first, Price::from(250))).await?;
    ///
    /// // Same lots, new price. The new order has a new cid.
    /// let second = ClientOrderId::new_v4();
    /// let ack = ud
    ///     .replace(ReplaceOrderRequest { cid: second, original_cid: first, qty: None, price: Some(Price::from(251)) })
    ///     .await?;
    /// assert_eq!(ack.cid, second);
    ///
    /// // An order the broker has never heard of cannot be replaced.
    /// let ghost = ReplaceOrderRequest { cid: ClientOrderId::new_v4(), original_cid: ClientOrderId::new_v4(), qty: None, price: None };
    /// assert!(matches!(ud.replace(ghost).await, Err(Error::Rejected { .. })));
    /// # ud.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    async fn replace(&self, req: ReplaceOrderRequest) -> Result<OrderAck, Error> {
        self.check_command()?;
        let original = req.original_cid;
        // Input is checked before anything is sent or any quota is spent.
        let requested_lots = req.qty.map(require_whole_lots).transpose()?;
        // The request has no instrument: it is the original order's. Not in the registry - one
        // GetOrderState teaches it (before the lock: the exchange id is read only under the lock,
        // a replace queued before this one changes it).
        let mut instrument = self.core(|c, _| c.instrument_of(original));
        if instrument.is_none() {
            let st = self
                .get_state(original.hyphenated().to_string(), true)
                .await?;
            self.core(|c, _| c.note_order(&map_order::parts_from_state(&st)));
            instrument = self.core(|c, _| c.instrument_of(original));
        }
        if let (Some(i), Some(p)) = (&instrument, req.price) {
            self.inner.price_grid.check(&i.key, p)?;
        }
        self.acquire("ReplaceOrder")?;
        let _instrument = match &instrument {
            Some(i) => Some(self.inner.locks.lock(i).await),
            None => None,
        };
        let known_id = self.core(|c, _| c.exchange_id(original).map(str::to_string));
        let known_basis = self.core(|c, _| c.order_basis(original));
        let (exchange_id, base_qty, base_price) = match (known_id, known_basis) {
            (Some(id), Some((q, p))) => (id, q, p),
            _ => {
                let st = self
                    .get_state(original.hyphenated().to_string(), true)
                    .await?;
                let parts = map_order::parts_from_state(&st);
                self.core(|c, _| c.note_order(&parts));
                (st.order_id, Qty::from(st.lots_requested), parts.unit_price)
            }
        };
        let lots = requested_lots.unwrap_or_else(|| {
            base_qty
                .round_to_step(Qty::from(1), RoundMode::Nearest)
                .ok()
                .and_then(Qty::to_whole)
                .unwrap_or(0)
        });
        let price = req
            .price
            .or(base_price)
            .map(price_to_quotation)
            .transpose()?;

        let now = brokapi_runtime::now();
        self.core(|c, _| c.begin_replace(req.cid, original, now))?;
        let wire = pb::ReplaceOrderRequest {
            account_id: self.account(),
            order_id: exchange_id,
            idempotency_key: req.cid.hyphenated().to_string(),
            quantity: lots,
            price,
            price_type: None,
            confirm_margin_trade: false,
        };
        let mut c = self.inner.client.orders();
        let cid = req.cid;
        match command(self.inner.request_timeout, c.replace_order(wire)).await {
            CommandOutcome::Done(resp) => {
                self.core(|c, s| c.replace_response(cid, &resp, now, s));
                Ok(OrderAck {
                    cid,
                    broker_id: BrokerOrderId::new(resp.order_id),
                    ts: stamps(now, None),
                })
            }
            CommandOutcome::Refused(status, err) => {
                let reason = format!("{:?}: {}", status.code(), status.message());
                self.core(|c, s| c.replace_failed(cid, false, reason, now, s));
                Err(err)
            }
            CommandOutcome::Unknown => {
                self.core(|c, s| c.replace_failed(cid, true, String::new(), now, s));
                Err(Error::Unknown)
            }
        }
    }

    /// The current state of one order by its `cid` (`GetOrderState` by request id).
    ///
    /// It also works for an order that is already filled or cancelled, so it is the way to learn the
    /// final state. `qty` and `filled` are in lots. The gate is not required: you may call it before
    /// the first reconcile.
    ///
    /// # Errors
    ///
    /// - [`Error::Rejected`] with code `NOT_FOUND` when the broker does not know the order.
    /// - [`Error::RateLimited`] when a quota is used up. Repeat after `retry_after`.
    /// - [`Error::Transport`] on a timeout or a network failure. A read, so repeat it later.
    /// - [`Error::Config`] when the broker does not accept the token.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::OrderState;
    /// use brokapi_core::{ClientOrderId, Error, InstrumentId, InstrumentKey, NativeId, OrderExecutor, OrderType, PlaceOrderRequest, Price, Qty, ShutdownMode, Side};
    /// use brokapi_runtime::BrokerLimiter;
    /// use brokapi_tbank::limits::default_limits;
    /// use brokapi_tbank::{UserDataConfig, UserDataConnector};
    /// use tokio::sync::mpsc;
    /// # use brokapi_mocks::tbank::{MockServer, ACCOUNT_A, TOKEN};
    /// # use brokapi_tbank_protocol::TbankConfig;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = MockServer::start(Default::default()).await;
    /// # let cfg = TbankConfig::for_mock(TOKEN, &server.endpoint).with_account(ACCOUNT_A);
    /// let (events_tx, mut events) = mpsc::channel(256);
    /// let ud = UserDataConnector::start(UserDataConfig::new(cfg, "tbank-ud"), BrokerLimiter::new(&default_limits()), events_tx).await?;
    /// ud.reconcile(&[]).await?;
    /// let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::Uid("uid-sber".into()));
    /// let buy = |cid, price| PlaceOrderRequest {
    ///     cid,
    ///     instrument: sber.clone(),
    ///     side: Side::Buy,
    ///     order_type: OrderType::Limit,
    ///     qty: Qty::from(1),
    ///     price: Some(price),
    /// };
    ///
    /// let cid = ClientOrderId::new_v4();
    /// ud.place(buy(cid, Price::from(250))).await?;
    /// let snapshot = ud.order_state(cid).await?;
    /// assert_eq!(snapshot.cid, cid);
    /// assert!(matches!(snapshot.state, OrderState::Acked { .. }));
    ///
    /// assert!(matches!(ud.order_state(ClientOrderId::new_v4()).await, Err(Error::Rejected { .. })));
    /// # ud.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    async fn order_state(&self, cid: ClientOrderId) -> Result<OrderSnapshot, Error> {
        let st = self.get_state(cid.hyphenated().to_string(), true).await?;
        let parts = map_order::parts_from_state(&st);
        self.core(|c, _| c.note_order(&parts));
        Ok(map_order::snapshot(&parts, brokapi_runtime::now()))
    }

    /// Orders that are live at the broker now (`GetOrders`) for this account.
    ///
    /// Orders that ended (filled, cancelled, rejected) are not listed. An order placed elsewhere
    /// (phone, another program) is listed too, with a stand-in `cid` made from its exchange id.
    /// `qty` and `filled` are in lots.
    ///
    /// # Errors
    ///
    /// [`Error::RateLimited`], [`Error::Transport`], [`Error::Config`] and [`Error::Rejected`], as for
    /// [`order_state`](Self::order_state). A read, so repeat it later.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::{ClientOrderId, Error, InstrumentId, InstrumentKey, NativeId, OrderExecutor, OrderType, PlaceOrderRequest, Price, Qty, ShutdownMode, Side};
    /// use brokapi_runtime::BrokerLimiter;
    /// use brokapi_tbank::limits::default_limits;
    /// use brokapi_tbank::{UserDataConfig, UserDataConnector};
    /// use tokio::sync::mpsc;
    /// # use brokapi_mocks::tbank::{MockServer, ACCOUNT_A, TOKEN};
    /// # use brokapi_tbank_protocol::TbankConfig;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = MockServer::start(Default::default()).await;
    /// # let cfg = TbankConfig::for_mock(TOKEN, &server.endpoint).with_account(ACCOUNT_A);
    /// let (events_tx, mut events) = mpsc::channel(256);
    /// let ud = UserDataConnector::start(UserDataConfig::new(cfg, "tbank-ud"), BrokerLimiter::new(&default_limits()), events_tx).await?;
    /// ud.reconcile(&[]).await?;
    /// let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::Uid("uid-sber".into()));
    /// let buy = |cid, price| PlaceOrderRequest {
    ///     cid,
    ///     instrument: sber.clone(),
    ///     side: Side::Buy,
    ///     order_type: OrderType::Limit,
    ///     qty: Qty::from(1),
    ///     price: Some(price),
    /// };
    ///
    /// assert!(ud.active_orders().await?.is_empty());
    /// let cid = ClientOrderId::new_v4();
    /// ud.place(buy(cid, Price::from(250))).await?;
    /// let live = ud.active_orders().await?;
    /// assert_eq!(live.iter().map(|o| o.cid).collect::<Vec<_>>(), vec![cid]);
    /// # ud.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    async fn active_orders(&self) -> Result<Vec<OrderSnapshot>, Error> {
        self.get_active().await
    }

    /// Streams the state changes of your orders (`OrderStateStream`), Lossless.
    ///
    /// The buffer holds `order_state_buffer` events. Own orders are never dropped: on overflow an
    /// overflow event is sent and the trading gate closes until the next successful
    /// [`reconcile`](Self::reconcile). Calling it again returns the same subscription id. The stream
    /// reconnects by itself. After a reconnect the connector reads `GetOrders` and emits what
    /// changed. An order that ended during the gap is not in `GetOrders`: find it with
    /// [`order_state`](Self::order_state).
    ///
    /// # Errors
    ///
    /// - [`Error::Rejected`] when the broker refuses the stream.
    /// - [`Error::Transport`] when the broker did not confirm the stream within `request_timeout`.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use brokapi_core::{Event, OrderState};
    /// use brokapi_core::{ClientOrderId, Error, InstrumentId, InstrumentKey, NativeId, OrderExecutor, OrderType, PlaceOrderRequest, Price, Qty, ShutdownMode, Side};
    /// use brokapi_runtime::BrokerLimiter;
    /// use brokapi_tbank::limits::default_limits;
    /// use brokapi_tbank::{UserDataConfig, UserDataConnector};
    /// use tokio::sync::mpsc;
    /// # use brokapi_mocks::tbank::{MockServer, ACCOUNT_A, TOKEN};
    /// # use brokapi_tbank_protocol::TbankConfig;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = MockServer::start(Default::default()).await;
    /// # let cfg = TbankConfig::for_mock(TOKEN, &server.endpoint).with_account(ACCOUNT_A);
    /// let (events_tx, mut events) = mpsc::channel(256);
    /// let ud = UserDataConnector::start(UserDataConfig::new(cfg, "tbank-ud"), BrokerLimiter::new(&default_limits()), events_tx).await?;
    /// ud.reconcile(&[]).await?;
    /// let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::Uid("uid-sber".into()));
    /// let buy = |cid, price| PlaceOrderRequest {
    ///     cid,
    ///     instrument: sber.clone(),
    ///     side: Side::Buy,
    ///     order_type: OrderType::Limit,
    ///     qty: Qty::from(1),
    ///     price: Some(price),
    /// };
    ///
    /// let sub = ud.subscribe_order_state().await?;
    /// assert_eq!(ud.subscribe_order_state().await?, sub, "the same id again");
    ///
    /// let cid = ClientOrderId::new_v4();
    /// ud.place(buy(cid, Price::from(250))).await?;
    /// let state = tokio::time::timeout(Duration::from_secs(5), async {
    ///     loop {
    ///         let env = events.recv().await.expect("the channel stays open");
    ///         if let Event::Order(snapshot) = env.payload {
    ///             if snapshot.cid == cid {
    ///                 break snapshot.state;
    ///             }
    ///         }
    ///     }
    /// })
    /// .await?;
    /// assert!(matches!(state, OrderState::Acked { .. } | OrderState::InFlight { .. }));
    /// # ud.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    async fn subscribe_order_state(&self) -> Result<SubscriptionId, Error> {
        // Own order states are never dropped: Lossless only.
        let opts = SubscribeOpts {
            backpressure: Backpressure::Lossless {
                cap: self.inner.order_state_buffer,
            },
            depth: None,
        };
        self.subscribe(UdSubKind::OrderState, opts).await
    }

    /// One `GetOrders` call: the live orders at the broker are compared with the ledger. A cid in
    /// the ledger that is not live is `closed_offline` (filled, cancelled, rejected, or never
    /// accepted - ask `order_state` for which). Live orders without our cid are `foreign`.
    /// A successful reconcile opens the command gate and ends `Degraded`.
    ///
    /// `ledger` is the list of cids you know to be yours (an empty slice on a fresh start). Until
    /// this call succeeds once, every trading command is refused with [`Error::NotReconciled`].
    ///
    /// # Errors
    ///
    /// [`Error::RateLimited`], [`Error::Transport`], [`Error::Config`] and [`Error::Rejected`], as for
    /// [`active_orders`](Self::active_orders). The gate stays closed when it fails. Repeat the call.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::{ClientOrderId, Error, InstrumentId, InstrumentKey, NativeId, OrderExecutor, OrderType, PlaceOrderRequest, Price, Qty, ShutdownMode, Side};
    /// use brokapi_runtime::BrokerLimiter;
    /// use brokapi_tbank::limits::default_limits;
    /// use brokapi_tbank::{UserDataConfig, UserDataConnector};
    /// use tokio::sync::mpsc;
    /// # use brokapi_mocks::tbank::{MockServer, ACCOUNT_A, TOKEN};
    /// # use brokapi_tbank_protocol::TbankConfig;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = MockServer::start(Default::default()).await;
    /// # let cfg = TbankConfig::for_mock(TOKEN, &server.endpoint).with_account(ACCOUNT_A);
    /// let (events_tx, mut events) = mpsc::channel(256);
    /// let ud = UserDataConnector::start(UserDataConfig::new(cfg, "tbank-ud"), BrokerLimiter::new(&default_limits()), events_tx).await?;
    /// let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::Uid("uid-sber".into()));
    /// let buy = |cid, price| PlaceOrderRequest {
    ///     cid,
    ///     instrument: sber.clone(),
    ///     side: Side::Buy,
    ///     order_type: OrderType::Limit,
    ///     qty: Qty::from(1),
    ///     price: Some(price),
    /// };
    ///
    /// // A run places an order, then the program restarts and knows only the cid from its own record.
    /// ud.reconcile(&[]).await?;
    /// let cid = ClientOrderId::new_v4();
    /// ud.place(buy(cid, Price::from(250))).await?;
    ///
    /// let lost = ClientOrderId::new_v4();
    /// let report = ud.reconcile(&[cid, lost]).await?;
    /// assert_eq!(report.confirmed.iter().map(|o| o.cid).collect::<Vec<_>>(), vec![cid]);
    /// // A cid that is not live at the broker: filled, cancelled or never accepted. Ask `order_state`.
    /// assert_eq!(report.closed_offline, vec![lost]);
    /// assert!(report.foreign.is_empty());
    /// # ud.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    async fn reconcile(&self, ledger: &[ClientOrderId]) -> Result<ReconcileReport, Error> {
        let active = self.get_active().await?;
        let ledger_snaps: Vec<OrderSnapshot> = active
            .iter()
            .filter(|o| ledger.contains(&o.cid))
            .cloned()
            .collect();
        let mut report = reconcile_ledger(&ledger_snaps, &active);
        for cid in ledger {
            if !active.iter().any(|o| o.cid == *cid) && !report.closed_offline.contains(cid) {
                report.closed_offline.push(*cid);
            }
        }
        self.inner.emitter.clear_degraded();
        let mut ctl = self.inner.ctl.lock().expect("ctl poisoned");
        ctl.gate_mut().clear_degraded();
        ctl.gate_mut().mark_reconciled();
        Ok(report)
    }
}

#[async_trait]
impl ExecutionSource for UserDataConnector {
    /// History from `GetOperationsByCursor` (executed BUY/SELL operations, one `Execution` per trade).
    /// Differences from the live stream are in `map::execution::from_operation`.
    ///
    /// Quantities here are in PIECES (the operation history is not in lots), unlike the live stream.
    /// `cid` is `None`: the history has no order id. The commission of an operation is spread over its
    /// trades in proportion to quantity. At most 20 pages of 1000 operations are read per call. A
    /// longer history is cut silently at that point, so ask for shorter ranges.
    ///
    /// # Errors
    ///
    /// [`Error::RateLimited`] (the operations quota is small), [`Error::Transport`], [`Error::Config`]
    /// and [`Error::Rejected`], as for [`order_state`](OrderExecutor::order_state). A read, so repeat
    /// it later.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::{ExecutionSource, TimeRange, UnixNanos};
    /// use brokapi_core::{ClientOrderId, Error, InstrumentId, InstrumentKey, NativeId, OrderExecutor, OrderType, PlaceOrderRequest, Price, Qty, ShutdownMode, Side};
    /// use brokapi_runtime::BrokerLimiter;
    /// use brokapi_tbank::limits::default_limits;
    /// use brokapi_tbank::{UserDataConfig, UserDataConnector};
    /// use tokio::sync::mpsc;
    /// # use brokapi_mocks::tbank::{MockServer, ACCOUNT_A, TOKEN};
    /// # use brokapi_tbank_protocol::TbankConfig;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = MockServer::start(Default::default()).await;
    /// # let cfg = TbankConfig::for_mock(TOKEN, &server.endpoint).with_account(ACCOUNT_A);
    /// let (events_tx, mut events) = mpsc::channel(256);
    /// let ud = UserDataConnector::start(UserDataConfig::new(cfg, "tbank-ud"), BrokerLimiter::new(&default_limits()), events_tx).await?;
    /// let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::Uid("uid-sber".into()));
    ///
    /// let range = TimeRange::new(UnixNanos::ZERO, UnixNanos::from_nanos(i64::MAX / 2));
    /// let history = ud.executions(range).await?;
    /// assert!(history.iter().all(|x| x.qty.is_positive()));
    /// # ud.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    async fn executions(&self, range: TimeRange) -> Result<Vec<Execution>, Error> {
        let mut out = Vec::new();
        let mut cursor: Option<String> = None;
        for _ in 0..MAX_HISTORY_PAGES {
            self.acquire("GetOperationsByCursor")?;
            let req = pb::GetOperationsByCursorRequest {
                account_id: self.account(),
                from: Some(nanos_to_proto(range.from)),
                to: Some(nanos_to_proto(range.to)),
                cursor: cursor.clone(),
                limit: Some(1000),
                operation_types: vec![
                    pb::OperationType::Buy as i32,
                    pb::OperationType::Sell as i32,
                ],
                state: Some(pb::OperationState::Executed as i32),
                without_trades: Some(false),
                ..Default::default()
            };
            let mut c = self.inner.client.operations();
            let r = query(self.inner.request_timeout, c.get_operations_by_cursor(req)).await?;
            let now = brokapi_runtime::now();
            for item in &r.items {
                out.extend(execution::from_operation(item, now));
            }
            if !r.has_next || r.next_cursor.is_empty() {
                return Ok(out);
            }
            cursor = Some(r.next_cursor);
        }
        Ok(out)
    }

    /// Own trades as they happen (`TradesStream`), Lossless.
    ///
    /// Same buffer and overflow rules as [`subscribe_order_state`](OrderExecutor::subscribe_order_state).
    /// Quantities are in lots and the stream carries no commission (0.0). Calling it again returns the
    /// same id.
    ///
    /// # Errors
    ///
    /// The same as [`subscribe_order_state`](OrderExecutor::subscribe_order_state).
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::ExecutionSource;
    /// use brokapi_core::{ClientOrderId, Error, InstrumentId, InstrumentKey, NativeId, OrderExecutor, OrderType, PlaceOrderRequest, Price, Qty, ShutdownMode, Side};
    /// use brokapi_runtime::BrokerLimiter;
    /// use brokapi_tbank::limits::default_limits;
    /// use brokapi_tbank::{UserDataConfig, UserDataConnector};
    /// use tokio::sync::mpsc;
    /// # use brokapi_mocks::tbank::{MockServer, ACCOUNT_A, TOKEN};
    /// # use brokapi_tbank_protocol::TbankConfig;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = MockServer::start(Default::default()).await;
    /// # let cfg = TbankConfig::for_mock(TOKEN, &server.endpoint).with_account(ACCOUNT_A);
    /// let (events_tx, mut events) = mpsc::channel(256);
    /// let ud = UserDataConnector::start(UserDataConfig::new(cfg, "tbank-ud"), BrokerLimiter::new(&default_limits()), events_tx).await?;
    /// let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::Uid("uid-sber".into()));
    ///
    /// let sub = ud.subscribe_executions().await?;
    /// assert_eq!(ud.subscribe_executions().await?, sub);
    /// # ud.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    async fn subscribe_executions(&self) -> Result<SubscriptionId, Error> {
        let opts = SubscribeOpts {
            backpressure: Backpressure::Lossless {
                cap: self.inner.order_state_buffer,
            },
            depth: None,
        };
        self.subscribe(UdSubKind::Executions, opts).await
    }
}

impl UserDataConnector {
    async fn fetch_positions(&self) -> Result<pb::PositionsResponse, Error> {
        self.acquire("GetPositions")?;
        let mut c = self.inner.client.operations();
        query(
            self.inner.request_timeout,
            c.get_positions(pb::PositionsRequest {
                account_id: self.account(),
            }),
        )
        .await
    }

    async fn fetch_portfolio(&self) -> Result<pb::PortfolioResponse, Error> {
        self.acquire("GetPortfolio")?;
        let mut c = self.inner.client.operations();
        query(
            self.inner.request_timeout,
            c.get_portfolio(pb::PortfolioRequest {
                account_id: self.account(),
                currency: None,
            }),
        )
        .await
    }
}

#[async_trait]
impl PortfolioSource for UserDataConnector {
    /// Cash from `GetPositions`, positions from `GetPortfolio` (two calls).
    ///
    /// The two calls are not one atomic snapshot: the account may change between them. Cash lines are
    /// per currency (upper case), with `available` and `blocked`. Currency holdings are not repeated
    /// in `positions`.
    ///
    /// # Errors
    ///
    /// [`Error::RateLimited`], [`Error::Transport`], [`Error::Config`] and [`Error::Rejected`], as for
    /// [`order_state`](OrderExecutor::order_state). A read, so repeat it later.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::PortfolioSource;
    /// use brokapi_core::{ClientOrderId, Error, InstrumentId, InstrumentKey, NativeId, OrderExecutor, OrderType, PlaceOrderRequest, Price, Qty, ShutdownMode, Side};
    /// use brokapi_runtime::BrokerLimiter;
    /// use brokapi_tbank::limits::default_limits;
    /// use brokapi_tbank::{UserDataConfig, UserDataConnector};
    /// use tokio::sync::mpsc;
    /// # use brokapi_mocks::tbank::{MockServer, ACCOUNT_A, TOKEN};
    /// # use brokapi_tbank_protocol::TbankConfig;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = MockServer::start(Default::default()).await;
    /// # let cfg = TbankConfig::for_mock(TOKEN, &server.endpoint).with_account(ACCOUNT_A);
    /// let (events_tx, mut events) = mpsc::channel(256);
    /// let ud = UserDataConnector::start(UserDataConfig::new(cfg, "tbank-ud"), BrokerLimiter::new(&default_limits()), events_tx).await?;
    /// let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::Uid("uid-sber".into()));
    ///
    /// let portfolio = ud.portfolio().await?;
    /// assert!(!portfolio.balances.is_empty());
    /// assert!(!portfolio.positions.is_empty());
    /// # ud.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    async fn portfolio(&self) -> Result<Portfolio, Error> {
        let positions = self.fetch_positions().await?;
        let portfolio = self.fetch_portfolio().await?;
        let now = brokapi_runtime::now();
        let cash = map_portfolio::balances_from_positions(&positions, now);
        Ok(map_portfolio::portfolio(&portfolio, cash, now))
    }

    /// Quantity in pieces (not lots), prices in the settlement currency.
    ///
    /// One `GetPortfolio` call. Currency holdings are left out (see
    /// [`balances`](PortfolioSource::balances)).
    ///
    /// # Errors
    ///
    /// The same as [`portfolio`](PortfolioSource::portfolio).
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::PortfolioSource;
    /// use brokapi_core::{ClientOrderId, Error, InstrumentId, InstrumentKey, NativeId, OrderExecutor, OrderType, PlaceOrderRequest, Price, Qty, ShutdownMode, Side};
    /// use brokapi_runtime::BrokerLimiter;
    /// use brokapi_tbank::limits::default_limits;
    /// use brokapi_tbank::{UserDataConfig, UserDataConnector};
    /// use tokio::sync::mpsc;
    /// # use brokapi_mocks::tbank::{MockServer, ACCOUNT_A, TOKEN};
    /// # use brokapi_tbank_protocol::TbankConfig;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = MockServer::start(Default::default()).await;
    /// # let cfg = TbankConfig::for_mock(TOKEN, &server.endpoint).with_account(ACCOUNT_A);
    /// let (events_tx, mut events) = mpsc::channel(256);
    /// let ud = UserDataConnector::start(UserDataConfig::new(cfg, "tbank-ud"), BrokerLimiter::new(&default_limits()), events_tx).await?;
    /// let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::Uid("uid-sber".into()));
    ///
    /// let positions = ud.positions().await?;
    /// assert!(positions.iter().all(|p| !p.qty.is_negative()));
    /// # ud.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    async fn positions(&self) -> Result<Vec<Position>, Error> {
        let p = self.fetch_portfolio().await?;
        Ok(map_portfolio::positions_from_portfolio(
            &p,
            brokapi_runtime::now(),
        ))
    }

    /// Cash by currency from `GetPositions`: `available` money and the `blocked` part.
    ///
    /// Currency codes are upper case. One call.
    ///
    /// # Errors
    ///
    /// The same as [`portfolio`](PortfolioSource::portfolio).
    ///
    /// # Examples
    ///
    /// ```
    /// # use brokapi_core::{Money};
    /// use brokapi_core::PortfolioSource;
    /// use brokapi_core::{ClientOrderId, Error, InstrumentId, InstrumentKey, NativeId, OrderExecutor, OrderType, PlaceOrderRequest, Price, Qty, ShutdownMode, Side};
    /// use brokapi_runtime::BrokerLimiter;
    /// use brokapi_tbank::limits::default_limits;
    /// use brokapi_tbank::{UserDataConfig, UserDataConnector};
    /// use tokio::sync::mpsc;
    /// # use brokapi_mocks::tbank::{MockServer, ACCOUNT_A, TOKEN};
    /// # use brokapi_tbank_protocol::TbankConfig;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = MockServer::start(Default::default()).await;
    /// # let cfg = TbankConfig::for_mock(TOKEN, &server.endpoint).with_account(ACCOUNT_A);
    /// let (events_tx, mut events) = mpsc::channel(256);
    /// let ud = UserDataConnector::start(UserDataConfig::new(cfg, "tbank-ud"), BrokerLimiter::new(&default_limits()), events_tx).await?;
    /// let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::Uid("uid-sber".into()));
    ///
    /// let cash = ud.balances().await?;
    /// assert!(cash.iter().all(|b| b.available >= Money::from(0)));
    /// # ud.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    async fn balances(&self) -> Result<Vec<Balance>, Error> {
        let p = self.fetch_positions().await?;
        Ok(map_portfolio::balances_from_positions(
            &p,
            brokapi_runtime::now(),
        ))
    }

    /// `PortfolioStream` (positions with prices) and `PositionsStream` (cash and balances) under one id.
    ///
    /// Two server streams feed one subscription. The events are [`Event::Portfolio`] (a full portfolio
    /// from `PortfolioStream`, with prices, and the latest known cash), [`Event::Balances`] (cash
    /// changed) and [`Event::Positions`] (positions changed, no prices). Calling it again returns the
    /// same id (the options of the first call stay).
    ///
    /// [`Event::Portfolio`]: brokapi_core::Event::Portfolio
    /// [`Event::Balances`]: brokapi_core::Event::Balances
    /// [`Event::Positions`]: brokapi_core::Event::Positions
    ///
    /// # Errors
    ///
    /// The same as [`subscribe_order_state`](OrderExecutor::subscribe_order_state).
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use brokapi_core::{Event, PortfolioSource, SubscribeOpts};
    /// use brokapi_core::{ClientOrderId, Error, InstrumentId, InstrumentKey, NativeId, OrderExecutor, OrderType, PlaceOrderRequest, Price, Qty, ShutdownMode, Side};
    /// use brokapi_runtime::BrokerLimiter;
    /// use brokapi_tbank::limits::default_limits;
    /// use brokapi_tbank::{UserDataConfig, UserDataConnector};
    /// use tokio::sync::mpsc;
    /// # use brokapi_mocks::tbank::{MockServer, ACCOUNT_A, TOKEN};
    /// # use brokapi_tbank_protocol::TbankConfig;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = MockServer::start(Default::default()).await;
    /// # let cfg = TbankConfig::for_mock(TOKEN, &server.endpoint).with_account(ACCOUNT_A);
    /// let (events_tx, mut events) = mpsc::channel(256);
    /// let ud = UserDataConnector::start(UserDataConfig::new(cfg, "tbank-ud"), BrokerLimiter::new(&default_limits()), events_tx).await?;
    /// let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::Uid("uid-sber".into()));
    ///
    /// let sub = ud.subscribe_portfolio(SubscribeOpts::conflate()).await?;
    /// let first = tokio::time::timeout(Duration::from_secs(5), async {
    ///     loop {
    ///         let env = events.recv().await.expect("the channel stays open");
    ///         if let Event::Portfolio(p) = env.payload {
    ///             break (env.sub, p);
    ///         }
    ///     }
    /// })
    /// .await?;
    /// assert_eq!(first.0, Some(sub));
    /// # ud.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    async fn subscribe_portfolio(&self, opts: SubscribeOpts) -> Result<SubscriptionId, Error> {
        self.subscribe(UdSubKind::Portfolio, opts).await
    }
}

#[async_trait]
impl StopOrderExecutor for UserDataConnector {
    /// `StopOrdersService/PostStopOrder`. `cid` goes to `order_id` (the idempotency key), as for
    /// plain orders. The broker's stop order does not carry it back, so the link cid <-> broker id is
    /// kept in memory: after a restart `active_stop_orders` shows `cid: None` and a stop order is
    /// cancelled by `StopOrderRef::Broker`.
    ///
    /// Units: `qty` is a whole number of LOTS (at least 1). Prices are positive. A stop-limit order
    /// needs a limit price. Expiry is `GoodTillCancel` or `GoodTillDate`: T-Bank stop orders have no
    /// end-of-day expiry. The checks run before any quota is spent. Like `place`, the call is never
    /// repeated: a lost answer is [`Error::Unknown`], and so is an answer with no stop order id
    /// (the broker may hold the order, nobody can tell). In both cases the `cid` is used up.
    ///
    /// # Errors
    ///
    /// - [`Error::NotReconciled`], [`Error::Degraded`], [`Error::ShuttingDown`]: nothing was sent.
    /// - [`Error::Rejected`] with code `invalid_qty`, `invalid_price`, `missing_limit_price` or
    ///   `unsupported_expiry` for a bad request, or `duplicate_place` for a `cid` used before.
    ///   Nothing was sent. A refusal by the broker is [`Error::Rejected`] with the gRPC status name
    ///   as code. After a refusal by the broker the `cid` may be used again.
    /// - [`Error::RateLimited`] when a quota is used up. Nothing was sent.
    /// - [`Error::Unknown`] on a timeout, a broken connection, a server fault or an answer without an
    ///   id. Check [`active_stop_orders`](Self::active_stop_orders) before you place again.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::{
    ///     OrderExecutor, PlaceStopOrderRequest, Price, Qty, StopExpiry, StopOrderExecutor, StopOrderKind, StopOrderRef,
    /// };
    /// use brokapi_core::{ClientOrderId, Error, InstrumentId, InstrumentKey, NativeId, ShutdownMode, Side};
    /// use brokapi_runtime::BrokerLimiter;
    /// use brokapi_tbank::limits::default_limits;
    /// use brokapi_tbank::{UserDataConfig, UserDataConnector};
    /// use tokio::sync::mpsc;
    /// # use brokapi_mocks::tbank::{MockServer, ACCOUNT_A, TOKEN};
    /// # use brokapi_tbank_protocol::TbankConfig;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = MockServer::start(Default::default()).await;
    /// # let cfg = TbankConfig::for_mock(TOKEN, &server.endpoint).with_account(ACCOUNT_A);
    /// let (events_tx, mut _events) = mpsc::channel(256);
    /// let ud = UserDataConnector::start(UserDataConfig::new(cfg, "tbank-ud"), BrokerLimiter::new(&default_limits()), events_tx).await?;
    /// ud.reconcile(&[]).await?;
    /// let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::Uid("uid-sber".into()));
    ///
    /// let cid = ClientOrderId::new_v4();
    /// let stop_loss = PlaceStopOrderRequest {
    ///     cid,
    ///     instrument: sber.clone(),
    ///     side: Side::Sell,
    ///     kind: StopOrderKind::StopLoss,
    ///     qty: Qty::from(1),
    ///     stop_price: Price::from(200),
    ///     limit_price: None, // market order at the trigger
    ///     expiry: StopExpiry::GoodTillCancel,
    /// };
    /// let ack = ud.place_stop(stop_loss).await?;
    /// assert_eq!(ud.active_stop_orders().await?[0].cid, Some(cid));
    ///
    /// // A stop-limit order without a limit price is refused before anything is sent.
    /// let bad = PlaceStopOrderRequest {
    ///     cid: ClientOrderId::new_v4(),
    ///     instrument: sber,
    ///     side: Side::Sell,
    ///     kind: StopOrderKind::StopLimit,
    ///     qty: Qty::from(1),
    ///     stop_price: Price::from(200),
    ///     limit_price: None,
    ///     expiry: StopExpiry::GoodTillCancel,
    /// };
    /// assert!(matches!(ud.place_stop(bad).await, Err(Error::Rejected { .. })));
    ///
    /// ud.cancel_stop(StopOrderRef::Broker(ack.broker_id)).await?;
    /// assert!(ud.active_stop_orders().await?.is_empty());
    /// # ud.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    async fn place_stop(&self, req: PlaceStopOrderRequest) -> Result<OrderAck, Error> {
        self.check_command()?;
        let wire = map_stop::build_post(&self.account(), &req)?;
        self.inner
            .price_grid
            .check(&req.instrument.key, req.stop_price)?;
        if let Some(p) = req.limit_price {
            self.inner.price_grid.check(&req.instrument.key, p)?;
        }
        self.acquire("PostStopOrder")?;
        let _instrument = self.inner.locks.lock(&req.instrument).await;
        if !self
            .inner
            .stops
            .lock()
            .expect("stops poisoned")
            .begin(req.cid)
        {
            return Err(Error::Rejected {
                code: "duplicate_place".into(),
                message: "auto-retry of place_stop is forbidden".into(),
            });
        }
        let now = brokapi_runtime::now();
        let mut c = self.inner.client.stop_orders();
        let cid = req.cid;
        match command(self.inner.request_timeout, c.post_stop_order(wire)).await {
            CommandOutcome::Done(resp) if !resp.stop_order_id.is_empty() => {
                self.inner
                    .stops
                    .lock()
                    .expect("stops poisoned")
                    .bind(cid, &resp.stop_order_id);
                Ok(OrderAck {
                    cid,
                    broker_id: BrokerOrderId::new(resp.stop_order_id),
                    ts: stamps(now, None),
                })
            }
            // An answer without an id: the broker may have the order, nobody can tell which.
            CommandOutcome::Done(_) | CommandOutcome::Unknown => Err(Error::Unknown),
            CommandOutcome::Refused(_, err) => {
                self.inner
                    .stops
                    .lock()
                    .expect("stops poisoned")
                    .forget_cid(cid);
                Err(err)
            }
        }
    }

    /// Cancels a stop order by its `cid` or by the broker's id (`CancelStopOrder`).
    ///
    /// A `cid` works only for a stop order placed by this connector object: the link lives in
    /// memory and is lost on restart. After a restart cancel by `StopOrderRef::Broker`, taking the
    /// id from [`active_stop_orders`](Self::active_stop_orders). Not repeated automatically.
    ///
    /// # Errors
    ///
    /// - [`Error::NotReconciled`], [`Error::Degraded`], [`Error::ShuttingDown`]: nothing was sent.
    /// - [`Error::Rejected`] with code `unknown_stop_order` when the `cid` is not known to this
    ///   connector. The broker's `NotFound` and other refusals come with the gRPC status name as code.
    /// - [`Error::RateLimited`] when a quota is used up.
    /// - [`Error::Unknown`] on a timeout or a broken connection: the stop order may or may not be
    ///   cancelled. Check [`active_stop_orders`](Self::active_stop_orders).
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::{OrderExecutor, StopOrderExecutor, StopOrderRef};
    /// use brokapi_core::{ClientOrderId, Error, InstrumentId, InstrumentKey, NativeId, ShutdownMode, Side};
    /// use brokapi_runtime::BrokerLimiter;
    /// use brokapi_tbank::limits::default_limits;
    /// use brokapi_tbank::{UserDataConfig, UserDataConnector};
    /// use tokio::sync::mpsc;
    /// # use brokapi_mocks::tbank::{MockServer, ACCOUNT_A, TOKEN};
    /// # use brokapi_tbank_protocol::TbankConfig;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = MockServer::start(Default::default()).await;
    /// # let cfg = TbankConfig::for_mock(TOKEN, &server.endpoint).with_account(ACCOUNT_A);
    /// let (events_tx, mut _events) = mpsc::channel(256);
    /// let ud = UserDataConnector::start(UserDataConfig::new(cfg, "tbank-ud"), BrokerLimiter::new(&default_limits()), events_tx).await?;
    /// ud.reconcile(&[]).await?;
    /// let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::Uid("uid-sber".into()));
    ///
    /// // A cid this connector never placed cannot be mapped to a broker id.
    /// let result = ud.cancel_stop(StopOrderRef::Cid(ClientOrderId::new_v4())).await;
    /// match result {
    ///     Err(Error::Rejected { code, .. }) => assert_eq!(code, "unknown_stop_order"),
    ///     other => panic!("expected unknown_stop_order, got {other:?}"),
    /// }
    /// # ud.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    async fn cancel_stop(&self, stop: StopOrderRef) -> Result<(), Error> {
        self.check_command()?;
        let id = match stop {
            StopOrderRef::Broker(id) => id.as_str().to_string(),
            StopOrderRef::Cid(cid) => self.inner.stops.lock().expect("stops poisoned").id_of(cid).ok_or_else(|| {
                Error::Rejected {
                    code: "unknown_stop_order".into(),
                    message: format!(
                        "stop order {cid} was not placed by this connector instance (T-Bank does not return the cid): cancel by the broker id"
                    ),
                }
            })?,
        };
        self.acquire("CancelStopOrder")?;
        let wire = pb::CancelStopOrderRequest {
            account_id: self.account(),
            stop_order_id: id.clone(),
        };
        let mut c = self.inner.client.stop_orders();
        match command(self.inner.request_timeout, c.cancel_stop_order(wire)).await {
            CommandOutcome::Done(_) => {
                self.inner
                    .stops
                    .lock()
                    .expect("stops poisoned")
                    .forget_id(&id);
                Ok(())
            }
            CommandOutcome::Refused(_, err) => Err(err),
            CommandOutcome::Unknown => Err(Error::Unknown),
        }
    }

    /// The stop orders that are active now (`GetStopOrders` with the active filter).
    ///
    /// `cid` is filled only for stop orders placed by this connector object. Others (placed earlier,
    /// by hand, by another program) have `cid: None`: use their `broker_id`.
    ///
    /// # Errors
    ///
    /// [`Error::RateLimited`], [`Error::Transport`], [`Error::Config`] and [`Error::Rejected`], as for
    /// [`order_state`](OrderExecutor::order_state). A read, so repeat it later.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::{OrderExecutor, StopOrderExecutor};
    /// use brokapi_core::{ClientOrderId, Error, InstrumentId, InstrumentKey, NativeId, ShutdownMode, Side};
    /// use brokapi_runtime::BrokerLimiter;
    /// use brokapi_tbank::limits::default_limits;
    /// use brokapi_tbank::{UserDataConfig, UserDataConnector};
    /// use tokio::sync::mpsc;
    /// # use brokapi_mocks::tbank::{MockServer, ACCOUNT_A, TOKEN};
    /// # use brokapi_tbank_protocol::TbankConfig;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = MockServer::start(Default::default()).await;
    /// # let cfg = TbankConfig::for_mock(TOKEN, &server.endpoint).with_account(ACCOUNT_A);
    /// let (events_tx, mut _events) = mpsc::channel(256);
    /// let ud = UserDataConnector::start(UserDataConfig::new(cfg, "tbank-ud"), BrokerLimiter::new(&default_limits()), events_tx).await?;
    /// ud.reconcile(&[]).await?;
    /// let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::Uid("uid-sber".into()));
    ///
    /// assert!(ud.active_stop_orders().await?.is_empty());
    /// # ud.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    async fn active_stop_orders(&self) -> Result<Vec<StopOrderSnapshot>, Error> {
        self.acquire("GetStopOrders")?;
        let req = pb::GetStopOrdersRequest {
            account_id: self.account(),
            status: pb::StopOrderStatusOption::StopOrderStatusActive as i32,
            from: None,
            to: None,
        };
        let mut c = self.inner.client.stop_orders();
        let r = query(self.inner.request_timeout, c.get_stop_orders(req)).await?;
        let now = brokapi_runtime::now();
        let stops = self.inner.stops.lock().expect("stops poisoned");
        Ok(r.stop_orders
            .iter()
            .map(|o| map_stop::snapshot(o, stops.cid_of(&o.stop_order_id), now))
            .collect())
    }

    /// T-Bank has no stream of stop order statuses (`StopOrdersService` is unary only).
    ///
    /// # Errors
    ///
    /// Always [`Error::Unsupported`] (`stop_order_stream`), without a network call. The library does
    /// not fake the stream by polling. Poll [`active_stop_orders`](Self::active_stop_orders) yourself.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::{OrderExecutor, StopOrderExecutor};
    /// use brokapi_core::{ClientOrderId, Error, InstrumentId, InstrumentKey, NativeId, ShutdownMode, Side};
    /// use brokapi_runtime::BrokerLimiter;
    /// use brokapi_tbank::limits::default_limits;
    /// use brokapi_tbank::{UserDataConfig, UserDataConnector};
    /// use tokio::sync::mpsc;
    /// # use brokapi_mocks::tbank::{MockServer, ACCOUNT_A, TOKEN};
    /// # use brokapi_tbank_protocol::TbankConfig;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = MockServer::start(Default::default()).await;
    /// # let cfg = TbankConfig::for_mock(TOKEN, &server.endpoint).with_account(ACCOUNT_A);
    /// let (events_tx, mut _events) = mpsc::channel(256);
    /// let ud = UserDataConnector::start(UserDataConfig::new(cfg, "tbank-ud"), BrokerLimiter::new(&default_limits()), events_tx).await?;
    /// ud.reconcile(&[]).await?;
    /// let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::Uid("uid-sber".into()));
    ///
    /// assert!(matches!(ud.subscribe_stop_orders().await, Err(Error::Unsupported { .. })));
    /// # ud.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    async fn subscribe_stop_orders(&self) -> Result<SubscriptionId, Error> {
        Err(Error::Unsupported {
            capability: "stop_order_stream",
        })
    }
}
