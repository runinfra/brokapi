//! The user data connector: orders, stop orders, portfolio and fills.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use brokapi_bcs_protocol::error::BcsApiError;
use brokapi_bcs_protocol::http::client::BcsHttpClient;
use brokapi_bcs_protocol::http::models::{
    CancelByIdRequest, CreateOrderRequest, EditOrderRequest, OrderRecord, OrderSearchRequest,
    OrderStateListResponse, TradeSearchRequest,
};
use brokapi_bcs_protocol::models::enums::OrderSide as BcsOrderSide;
use brokapi_bcs_protocol::ws::client::BcsWsClient;
use brokapi_bcs_protocol::ws::session::WsSession;
use brokapi_core::{
    Backpressure, Balance, ClientOrderId, Envelope, Error, Execution, ExecutionSource, OrderAck,
    OrderExecutor, OrderSnapshot, OrderType as CoreOrderType, PlaceIdempotency, PlaceOrderRequest,
    PlaceStopOrderRequest, Portfolio, PortfolioSource, Position, ReconcileReport,
    ReplaceOrderRequest, ShutdownMode, ShutdownReport, Side, StopOrderExecutor, StopOrderRef,
    StopOrderSnapshot, SubscribeOpts, SubscriptionId, TimeRange, UserDataCaps,
};
use brokapi_runtime::{
    BrokerLimiter, CommandGate, Emitter, InstrumentLocks, OrderRuntime, PriceGrid,
    ShutdownController,
};
use brokapi_sansio::{
    BrokerCore, DesiredSubscription, OrderSignal, SubscriptionKind, SubscriptionSet,
};
use chrono::{TimeZone, Utc};
use tokio::sync::{Mutex, RwLock, mpsc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::cid::foreign_cid;
use crate::config::UserDataConfig;
use crate::core::UdCore;
use crate::error::{
    map_command_err, map_config_err, map_err, map_order_cmd_err, map_order_read_err,
};
use crate::map::execution::map_trade_record;
use crate::map::order::{
    LIST_STATUS_ACTIVE, PLAIN_LIST_TYPES, STOP_LIST_TYPES, map_order_ack, map_order_record,
    map_state_list, map_stop_record,
};
use crate::map::portfolio::map_rest_portfolio;
use crate::map::stop as map_stop;
use crate::map::whole_pieces;
use crate::pump::{drain_backlog, run_ud_pump};
use crate::raw::BcsRaw;
use crate::subs::{SubIdGen, UdSubKind, UdSubscription};
use crate::util::{class_code, nanos_to_datetime, ticker};

struct UdShared {
    http: BcsHttpClient,
    limiter: Arc<BrokerLimiter>,
    core: Arc<Mutex<UdCore>>,
    emitter: Arc<Emitter>,
    orders: Mutex<OrderRuntime>,
    instrument_locks: InstrumentLocks,
    price_grid: PriceGrid,
    subs: RwLock<HashMap<SubscriptionId, UdSubscription>>,
    sub_ids: SubIdGen,
    shutdown: Mutex<ShutdownController>,
    cancel: CancellationToken,
    order_state_buffer: usize,
    /// How long a `subscribe_*` call waits for its WebSocket to be open.
    ack_timeout: Duration,
}

/// BCS user data connector: orders, stop orders, portfolio and the history of fills for one account.
///
/// It implements [`OrderExecutor`], [`StopOrderExecutor`], [`ExecutionSource`] and
/// [`PortfolioSource`]. The account is the one the refresh token in the config belongs to. One
/// connector per account. Start it with [`start`](Self::start). Commands return their answer
/// directly, and subscriptions deliver [`Envelope`]s into the channel given to `start`.
///
/// # Rules that protect the account
///
/// - **Write access.** The config must be built with `with_write(true)` (the environment switch
///   `BCS_IS_WRITE=true` does the same for `BcsConfig::from_env`). Without it the connector asks for
///   a read-only token. Whether the broker then refuses the orders with a definite error is not
///   verified against the live API. Never rely on it as a safety lock, use a read-only config
///   only for reading.
/// - **Reconcile gate.** Trading commands (`place`, `replace`, `cancel`, `place_stop`, `cancel_stop`)
///   are refused with [`Error::NotReconciled`] until [`reconcile`](OrderExecutor::reconcile) has
///   succeeded once. This forces the bot to compare its own ledger of orders with the broker before
///   it sends a new order, so a restart cannot double an order that was placed before the crash.
///   Reads (`order_state`, `active_orders`, `portfolio`, `executions`) are not gated.
/// - **Degraded.** If a lossless subscription overflows (the bot reads too slowly), the gate closes
///   and commands return [`Error::Degraded`] until the next successful `reconcile`.
/// - **No retry of `place`.** A cid that has been sent is spent. If `place` returns
///   [`Error::Unknown`] (no answer, a break, HTTP 5xx), the order may exist. Ask `order_state` or
///   run `reconcile`. Calling `place` again with the same cid returns [`Error::Rejected`] with code
///   `duplicate_place`, because a retry could create a second order.
/// - **The gate is checked twice.** Once at the start, and again when the command's turn in the queue of its
///   instrument comes: a gate that closed while it waited stops it, and nothing is sent.
/// - **A cut of the order events socket is told.** [`Event::Reconnected`](brokapi_core::Event::Reconnected) on the
///   order state and executions subscriptions, the gate closes until `reconcile`.
/// - **Quantity is in pieces**, a whole number, never rounded: `0.4` or `2.5` is `Rejected` with code `invalid_qty`.
/// - **Rate limit.** Quota is taken from the shared limiter before the request. A refusal is
///   [`Error::RateLimited`] and nothing is sent, so the same cid can be used again.
///
/// # Examples
///
/// The whole path against the BCS imitation: reconcile, place, cancel, and two kinds of refusal.
///
/// ```
/// use brokapi_bcs::limits::default_limits;
/// use brokapi_bcs::{UserDataConfig, UserDataConnector};
/// use brokapi_conformance::REJECT_PRICE;
/// use brokapi_core::{
///     ClientOrderId, Error, InstrumentId, InstrumentKey, NativeId, OrderExecutor, OrderState, OrderType,
///     PlaceOrderRequest, Price, Qty, ShutdownMode, Side,
/// };
/// use brokapi_mocks::bcs::REJECT_IN_BODY_PRICE;
/// use brokapi_runtime::BrokerLimiter;
/// use tokio::sync::mpsc;
/// # use brokapi_bcs_protocol::config::BcsConfig;
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
/// # let cfg = BcsConfig::for_mock("mock-refresh-token", &server.http_base).with_write(true);
/// let (events_tx, _events) = mpsc::channel(256);
/// let limiter = BrokerLimiter::new(&default_limits());
/// let ud = UserDataConnector::start(UserDataConfig::new(cfg, "bcs-ud"), limiter, events_tx).await?;
///
/// let sber = InstrumentId::new(
///     InstrumentKey::new("SBER", "TQBR"),
///     NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() },
/// );
/// let limit_buy = |price: Price| PlaceOrderRequest {
///     cid: ClientOrderId::new_v4(),
///     instrument: sber.clone(),
///     side: Side::Buy,
///     order_type: OrderType::Limit,
///     qty: Qty::from(1),
///     price: Some(price),
/// };
///
/// // Before the first reconcile the gate is closed.
/// assert!(matches!(ud.place(limit_buy(Price::from(100))).await, Err(Error::NotReconciled)));
/// // An empty ledger: the bot has no orders of its own to compare. Opens the gate.
/// ud.reconcile(&[]).await?;
///
/// // Place, then cancel.
/// let req = limit_buy(Price::from(100));
/// let cid = req.cid;
/// let ack = ud.place(req).await?;
/// println!("broker order id: {}", ack.broker_id.as_str());
/// ud.cancel(cid).await?;
/// assert!(matches!(ud.order_state(cid).await?.state, OrderState::Cancelled { .. }));
///
/// // The broker says no, way one: HTTP 400. Nothing was created.
/// match ud.place(limit_buy(REJECT_PRICE)).await {
///     Err(Error::Rejected { code, .. }) => assert_eq!(code, "validation"),
///     other => panic!("expected a refusal, got {other:?}"),
/// }
/// // Way two: HTTP 200, but the report in the answer has orderStatus = 8.
/// match ud.place(limit_buy(Price::new(REJECT_IN_BODY_PRICE))).await {
///     Err(Error::Rejected { code, message }) => {
///         assert_eq!(code, "rejected");
///         println!("reason from the broker: {message}");
///     }
///     other => panic!("expected a refusal, got {other:?}"),
/// }
///
/// ud.shutdown(ShutdownMode::Immediate).await;
/// # Ok(())
/// # }
/// ```
pub struct UserDataConnector {
    inner: Arc<UdShared>,
    pump: JoinHandle<()>,
    /// `orders/events/ws`: status changes and fills of orders in one channel.
    ws_events: WsSession,
    /// `portfolio/ws`: the positions of the account.
    ws_portfolio: WsSession,
}

impl UserDataConnector {
    /// Starts the connector: gets the first access token and opens the order events and portfolio
    /// WebSockets.
    ///
    /// The trading gate starts closed: call [`reconcile`](OrderExecutor::reconcile) before the first
    /// command, or `place` returns [`Error::NotReconciled`]. The token refresh does not spend rate
    /// limit quota. The two WebSocket sessions run in the background and reconnect on their own.
    /// Order status changes and fills arrive on one channel, `orders/events/ws`.
    ///
    /// `limiter` should be the one shared by every connector of this BCS account, because the limits
    /// belong to the token. `out` receives the envelopes of all subscriptions and must be read.
    ///
    /// # Errors
    ///
    /// - [`Error::Config`] when the HTTP or WebSocket client cannot be built from `cfg`.
    /// - [`Error::Transport`] when the token refresh fails: no connection, a timeout, or the auth
    ///   server refuses the refresh token. Exact auth server answers: not verified against the live
    ///   API.
    /// - [`Error::RateLimited`] when the auth server answers HTTP 429.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_bcs::limits::default_limits;
    /// use brokapi_bcs::{UserDataConfig, UserDataConnector};
    /// use brokapi_core::{OrderExecutor, ShutdownMode};
    /// use brokapi_runtime::BrokerLimiter;
    /// use tokio::sync::mpsc;
    /// # use brokapi_bcs_protocol::config::BcsConfig;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = BcsConfig::for_mock("mock-refresh-token", &server.http_base).with_write(true);
    /// let (events_tx, _events) = mpsc::channel(256);
    /// let limiter = BrokerLimiter::new(&default_limits());
    /// let ud = UserDataConnector::start(UserDataConfig::new(cfg, "bcs-ud"), limiter, events_tx).await?;
    /// // The gate is closed until the first reconcile.
    /// ud.reconcile(&[]).await?;
    /// # ud.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// A broker that cannot be reached is a [`Error::Transport`]:
    ///
    /// ```
    /// use brokapi_bcs::limits::default_limits;
    /// use brokapi_bcs::{UserDataConfig, UserDataConnector};
    /// use brokapi_core::Error;
    /// use brokapi_runtime::BrokerLimiter;
    /// use tokio::sync::mpsc;
    /// use brokapi_bcs_protocol::config::BcsConfig;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// // Port 1 of the local host: nothing listens there.
    /// let cfg = BcsConfig::for_mock("any-token", "http://127.0.0.1:1").with_write(true);
    /// let (events_tx, _events) = mpsc::channel(16);
    /// let limiter = BrokerLimiter::new(&default_limits());
    /// let started = UserDataConnector::start(UserDataConfig::new(cfg, "bcs-ud"), limiter, events_tx).await;
    /// assert!(matches!(started, Err(Error::Transport(_))));
    /// # }
    /// ```
    pub async fn start(
        cfg: UserDataConfig,
        limiter: Arc<BrokerLimiter>,
        out: mpsc::Sender<Envelope>,
    ) -> Result<Self, Error> {
        let ws_client = BcsWsClient::new(cfg.bcs.clone())
            .map_err(map_config_err)?
            .with_options(cfg.ws_options());
        ws_client
            .http_client()
            .authenticate()
            .await
            .map_err(map_err)?;

        let (event_tx, event_rx) = kanal::bounded_async(512);

        // One channel for order statuses AND fills: the current `orders/events/ws`. The deprecated
        // execution-status and transaction-status are not opened (the protocol crate still has them,
        // reachable through `raw().connect_ws`).
        let ws_events = ws_client.connect_order_events(event_tx.clone());
        // Only the order events session announces its connects (a lost connection there loses fills).
        let ws_client = ws_client.with_options(cfg.portfolio_ws_options());
        let ws_portfolio = ws_client.connect_portfolio(event_tx);

        let cancel = CancellationToken::new();
        let core = Arc::new(Mutex::new(UdCore::new()));
        let emitter = Emitter::new(Arc::clone(&cfg.source), out);
        let gate = CommandGate::new();

        let inner = Arc::new(UdShared {
            http: ws_client.http_client().clone(),
            limiter,
            core: Arc::clone(&core),
            emitter: Arc::clone(&emitter),
            orders: Mutex::new(OrderRuntime::new()),
            instrument_locks: InstrumentLocks::new(),
            price_grid: cfg.price_grid.clone(),
            subs: RwLock::new(HashMap::new()),
            sub_ids: SubIdGen::new(1000),
            shutdown: Mutex::new(ShutdownController::new(gate)),
            cancel: cancel.clone(),
            order_state_buffer: cfg.order_state_buffer,
            ack_timeout: cfg.subscribe_ack_timeout,
        });

        let pump = tokio::spawn(run_ud_pump(core, emitter, event_rx, cancel));

        Ok(Self {
            inner,
            pump,
            ws_events,
            ws_portfolio,
        })
    }

    /// Returns a handle for any other method of the BCS API, with this connector's token, endpoint
    /// and rate limiter.
    ///
    /// Orders sent through the handle bypass this connector's order state: there are no events for
    /// them, they are not in the ledger and `reconcile` does not know them. Use [`OrderExecutor`]
    /// for orders, and the handle for reads and for methods the traits do not wrap. Use
    /// [`call_command`](BcsRaw::call_command) for anything that changes the account. See [`BcsRaw`]
    /// for the rules of a call.
    ///
    /// # Examples
    ///
    /// A raw read, spending the quota of the name `GetQuotes`:
    ///
    /// ```
    /// use brokapi_bcs_protocol::http::models::{QuoteInstrument, QuotesRequest};
    /// use brokapi_bcs::limits::default_limits;
    /// use brokapi_bcs::{UserDataConfig, UserDataConnector};
    /// use brokapi_core::ShutdownMode;
    /// use brokapi_runtime::BrokerLimiter;
    /// use tokio::sync::mpsc;
    /// # use brokapi_bcs_protocol::config::BcsConfig;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = BcsConfig::for_mock("mock-refresh-token", &server.http_base).with_write(true);
    /// # let (events_tx, _events) = mpsc::channel(256);
    /// # let limiter = BrokerLimiter::new(&default_limits());
    /// let ud = UserDataConnector::start(UserDataConfig::new(cfg, "bcs-ud"), limiter, events_tx).await?;
    /// let quotes = ud
    ///     .raw()
    ///     .call("GetQuotes", |http| async move {
    ///         http.get_quotes(&QuotesRequest { instruments: vec![QuoteInstrument::new("SBER", "TQBR")] }).await
    ///     })
    ///     .await?;
    /// assert!(!quotes.records.is_empty());
    /// ud.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    pub fn raw(&self) -> BcsRaw {
        BcsRaw::new(self.inner.http.clone(), Arc::clone(&self.inner.limiter))
    }

    /// The price steps this connector checks order prices against. Fill it from
    /// [`TradingParams`](brokapi_core::TradingParams) (`grid.learn(&params)`) to have a price off the
    /// step refused before anything is sent. See [`PriceGrid`].
    pub fn price_grid(&self) -> &PriceGrid {
        &self.inner.price_grid
    }

    /// Describes what this connector can do, without any request.
    ///
    /// The answer is fixed for BCS: a place request is deduplicated by label only
    /// ([`PlaceIdempotency::LabelOnly`], so never repeat a place), replace and stop orders are
    /// supported, fills are streamed, there is no command channel over WebSocket and no sandbox.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_bcs::limits::default_limits;
    /// use brokapi_bcs::{UserDataConfig, UserDataConnector};
    /// use brokapi_core::ShutdownMode;
    /// use brokapi_runtime::BrokerLimiter;
    /// use tokio::sync::mpsc;
    /// # use brokapi_bcs_protocol::config::BcsConfig;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = BcsConfig::for_mock("mock-refresh-token", &server.http_base).with_write(true);
    /// let (events_tx, _events) = mpsc::channel(256);
    /// let limiter = BrokerLimiter::new(&default_limits());
    /// let ud = UserDataConnector::start(UserDataConfig::new(cfg, "bcs-ud"), limiter, events_tx).await?;
    /// let caps = ud.caps();
    /// assert!(caps.supports_replace && caps.supports_stop_orders && caps.streams_executions);
    /// assert!(!caps.sandbox);
    /// # ud.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    pub fn caps(&self) -> UserDataCaps {
        // Conditional orders (`orderType = 5`): see the `StopOrderExecutor` impl.
        UserDataCaps::new(
            PlaceIdempotency::LabelOnly,
            false,
            false,
            true,
            true,
            true,
            false,
        )
    }

    /// Stops the connector and reports what was left unfinished. Consumes the connector.
    ///
    /// The command gate closes first, so nothing new is sent. Orders that were placed but have no
    /// answer yet are listed in `inflight_abandoned` and marked unknown locally. The broker is not
    /// asked to cancel anything: orders that exist on the exchange stay alive, and a later
    /// `reconcile` finds them. With [`ShutdownMode::Graceful`] the events that wait in the
    /// subscription buffers first get time to reach the bot's channel, until `deadline`. The
    /// WebSocket sessions are not unsubscribed message by message, so `unsubscribed` is `0`.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use brokapi_bcs::limits::default_limits;
    /// use brokapi_bcs::{UserDataConfig, UserDataConnector};
    /// use brokapi_core::ShutdownMode;
    /// use brokapi_runtime::BrokerLimiter;
    /// use tokio::sync::mpsc;
    /// # use brokapi_bcs_protocol::config::BcsConfig;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = BcsConfig::for_mock("mock-refresh-token", &server.http_base).with_write(true);
    /// # let (events_tx, _events) = mpsc::channel(256);
    /// # let limiter = BrokerLimiter::new(&default_limits());
    /// let ud = UserDataConnector::start(UserDataConfig::new(cfg, "bcs-ud"), limiter, events_tx).await?;
    /// let report = ud.shutdown(ShutdownMode::Graceful { deadline: Duration::from_secs(1) }).await;
    /// assert!(report.inflight_abandoned.is_empty());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn shutdown(self, mode: ShutdownMode) -> ShutdownReport {
        let now = brokapi_runtime::now();
        if let ShutdownMode::Graceful { deadline } = mode {
            drain_backlog(&self.inner.emitter, deadline).await;
        }
        let mut orders = self.inner.orders.lock().await;
        let mut shutdown = self.inner.shutdown.lock().await;
        let report = shutdown.shutdown_local(mode, now, &mut orders, || (0, 0));
        self.inner.cancel.cancel();
        let _ = self.pump.await;
        report
    }

    /// The gate of trading commands. An `Overflow` of a lossless subscription (found by the emitter,
    /// which runs in the pump task) closes it as `Degraded` until `reconcile` succeeds.
    async fn check_command(&self) -> Result<(), Error> {
        let mut ctl = self.inner.shutdown.lock().await;
        if self.inner.emitter.is_degraded() {
            ctl.gate_mut().mark_degraded();
        }
        ctl.gate().check_command()
    }

    async fn acquire(&self, method: &str) -> Result<(), Error> {
        self.inner.limiter.acquire(method, brokapi_runtime::now())
    }

    /// Tells the core the whole set of subscriptions: applying one alone would drop the others.
    async fn apply_desired(&self) {
        let desired = SubscriptionSet::with_subscriptions(
            self.inner
                .subs
                .read()
                .await
                .values()
                .map(|s| DesiredSubscription {
                    id: s.id,
                    kind: match s.kind {
                        UdSubKind::OrderState => SubscriptionKind::OrderState,
                        UdSubKind::Portfolio => SubscriptionKind::Portfolio,
                        UdSubKind::Executions => SubscriptionKind::Executions,
                    },
                })
                .collect(),
        );
        self.inner.core.lock().await.apply(&desired);
    }

    async fn register_ud_sub(
        &self,
        kind: UdSubKind,
        opts: SubscribeOpts,
    ) -> Result<SubscriptionId, Error> {
        let id = self.inner.sub_ids.next();
        let instrument = brokapi_core::InstrumentId::new(
            brokapi_core::InstrumentKey::new("", ""),
            brokapi_core::NativeId::TickerClass {
                ticker: String::new(),
                class_code: String::new(),
            },
        );
        self.inner
            .emitter
            .register(id, Some(instrument), opts.backpressure);
        self.inner
            .subs
            .write()
            .await
            .insert(id, UdSubscription { id, kind, opts });

        self.apply_desired().await;

        // Returns when the WebSocket of the channel is open: the broker pushes order events and positions as soon
        // as it is, there is nothing to send. Whatever happens at the broker after that is delivered. Before it,
        // an order that is placed and filled would be missed.
        let socket = match kind {
            UdSubKind::Portfolio => &self.ws_portfolio,
            UdSubKind::OrderState | UdSubKind::Executions => &self.ws_events,
        };
        let limit = self.inner.ack_timeout;
        if tokio::time::timeout(limit, socket.wait_connected())
            .await
            .is_err()
        {
            // Not open in time: take the subscription back, nothing stays half open. The socket keeps
            // connecting in the background, and a later subscribe finds it open.
            self.inner.subs.write().await.remove(&id);
            self.apply_desired().await;
            self.inner.emitter.unregister(id);
            return Err(brokapi_runtime::not_confirmed(limit));
        }
        Ok(id)
    }
}

// ─── helpers ──────────────────────────────────────────────────────────────────

/// The list of orders shows orders from 26.01.2026 (documented limit), so the search starts there.
fn list_start() -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 1, 26, 0, 0, 0)
        .single()
        .expect("valid date")
}

/// The order list pages are read until `totalPages`, at most this many (100 per page).
const MAX_LIST_PAGES: i32 = 10;

/// The biggest page `POST /trades/search` gives (`size`, 100), and the most pages read in one call.
const TRADES_PAGE_SIZE: u32 = 100;
const MAX_TRADE_PAGES: u32 = 20;
/// The trades list holds at most this many of the most recent trades (docs of `POST /trades/search`).
const TRADES_LIST_CAP: u64 = 1000;

fn too_long() -> Error {
    Error::Rejected {
        code: "range_too_long".into(),
        message: format!(
            "the trade list holds {TRADES_LIST_CAP} trades at most and the range reaches that: read it in shorter ranges"
        ),
    }
}

/// How a broker order id is named in `orderId` / `orderIdType`: our cid is a UUID (`"1"`), anything
/// else is the exchange id such as `20261019-TQBR-79628540663` (`"2"`).
fn order_ref(id: &str) -> (&'static str, String) {
    if uuid::Uuid::parse_str(id).is_ok() {
        ("1", id.to_string())
    } else {
        ("2", id.to_string())
    }
}

impl UserDataConnector {
    /// A read that a replace needs before sending (the original order) failed. On `Transport` the cid is used up
    /// all the same: the book tells a bot to treat `Transport` on a command as "the order may stand", and the
    /// same cid again must be `duplicate_place`, never a second move of the order.
    async fn used_up<T>(&self, cid: ClientOrderId, read: Result<T, Error>) -> Result<T, Error> {
        if let Err(Error::Transport(_)) = &read {
            let _ = self.inner.orders.lock().await.try_place_once(cid);
        }
        read
    }

    /// Orders of the account from `POST /orders/search`, all pages.
    async fn list_orders(
        &self,
        statuses: &[i32],
        types: &[i32],
    ) -> Result<Vec<OrderRecord>, Error> {
        let mut out = Vec::new();
        for page in 0..MAX_LIST_PAGES {
            self.acquire("SearchOrders").await?;
            let req = OrderSearchRequest {
                start_date_time: list_start(),
                // The end is a day ahead of the local clock: the clock of this machine may lag behind the
                // broker, and an order of the last minutes must not fall out of the window.
                end_date_time: Utc::now() + chrono::Duration::days(1),
                side: None,
                order_status: Some(statuses.to_vec()),
                order_types: Some(types.to_vec()),
                tickers: None,
                class_codes: None,
            };
            let resp = self
                .inner
                .http
                .search_orders(req, Some(page), Some(100))
                .await
                .map_err(map_err)?;
            out.extend(resp.records);
            if page + 1 >= resp.total_pages {
                break;
            }
        }
        Ok(out)
    }

    /// Our cid of a broker order id: what this process has seen, else one `GET /orders` by the
    /// exchange id (the list carries no `clientOrderId`). `None` when the broker has no cid of ours
    /// for it (an order from the app, or the lookup failed).
    async fn cid_of_order_id(&self, order_id: &str) -> Option<ClientOrderId> {
        if let Some(cid) = self.inner.core.lock().await.cid_of_order_id(order_id) {
            return Some(cid);
        }
        self.acquire("GetOrder").await.ok()?;
        let resp = self.inner.http.get_order("2", order_id).await.ok()?;
        // The cid of an order is its own `clientOrderId`; `original...` names the order it replaced.
        let cid = uuid::Uuid::parse_str(&resp.client_order_id)
            .ok()
            .or_else(|| {
                resp.original_client_order_id
                    .as_deref()
                    .and_then(|s| uuid::Uuid::parse_str(s).ok())
            })?;
        self.inner.core.lock().await.learn_order_id(order_id, cid);
        Some(cid)
    }

    async fn learn(&self, order_id: &str, cid: ClientOrderId) {
        self.inner.core.lock().await.learn_order_id(order_id, cid);
    }

    /// The state of the order after a command that waits for the broker (`/sync` methods): the report
    /// the broker returned, or `Unknown` when it answered without one (the order may exist).
    fn state_of(
        &self,
        cid: ClientOrderId,
        resp: &OrderStateListResponse,
    ) -> Result<brokapi_core::OrderSnapshot, Error> {
        map_state_list(Some(cid), resp, brokapi_runtime::now()).ok_or(Error::Unknown)
    }

    /// A cancel that waited for the broker: done when the order is cancelled, refused when the broker
    /// says it is filled or rejected the request, `Unknown` when it is still alive in the answer
    /// (the outcome is not decided: ask `order_state`).
    fn cancel_outcome(
        &self,
        cid: ClientOrderId,
        resp: &OrderStateListResponse,
    ) -> Result<(), Error> {
        use brokapi_core::OrderState as S;
        let snap = self.state_of(cid, resp)?;
        match snap.state {
            S::Cancelled { .. } => Ok(()),
            S::Filled { .. } => Err(Error::Rejected {
                code: "already_filled".into(),
                message: "the order was filled before the cancel".into(),
            }),
            S::Rejected { reason, .. } => Err(Error::Rejected {
                code: "cancel_rejected".into(),
                message: reason,
            }),
            _ => Err(Error::Unknown),
        }
    }

    /// An order command that waited for the broker, as an ack: a refusal inside a 200 answer
    /// (`orderStatus = 8`) is `Rejected`.
    /// Moves the order out of `InFlight` when its command is answered, so a shutdown reports only commands
    /// that really have no answer.
    async fn settle(
        &self,
        cid: ClientOrderId,
        outcome: &Result<(OrderAck, Option<String>), Error>,
    ) {
        let signal = match outcome {
            Ok((ack, _)) => OrderSignal::Ack {
                broker_id: ack.broker_id.clone(),
            },
            Err(Error::Rejected { message, .. }) => OrderSignal::Rejected {
                reason: message.clone(),
            },
            Err(_) => OrderSignal::Timeout,
        };
        self.inner
            .orders
            .lock()
            .await
            .apply_signal(cid, signal, brokapi_runtime::now());
    }

    fn ack_of(
        &self,
        cid: ClientOrderId,
        resp: &OrderStateListResponse,
        now: Instant,
    ) -> Result<(OrderAck, Option<String>), Error> {
        let snap = self.state_of(cid, resp)?;
        if let brokapi_core::OrderState::Rejected { reason, .. } = &snap.state {
            return Err(Error::Rejected {
                code: "rejected".into(),
                message: reason.clone(),
            });
        }
        let order_id = snap.broker_id.as_ref().map(|b| b.as_str().to_string());
        let broker_id = order_id
            .clone()
            .unwrap_or_else(|| cid.hyphenated().to_string());
        Ok((
            map_order_ack(cid, brokapi_core::BrokerOrderId::new(broker_id), now),
            order_id,
        ))
    }
}

#[async_trait]
impl brokapi_core::Connector for UserDataConnector {
    type Caps = brokapi_core::UserDataCaps;

    fn caps(&self) -> brokapi_core::UserDataCaps {
        UserDataConnector::caps(self)
    }

    async fn shutdown(self, mode: brokapi_core::ShutdownMode) -> brokapi_core::ShutdownReport {
        UserDataConnector::shutdown(self, mode).await
    }
}

#[async_trait]
impl OrderExecutor for UserDataConnector {
    /// `POST /orders/sync`, not `POST /orders`. The synchronous method waits for the broker's decision
    /// and answers with the order's state, so (1) a refusal (`orderStatus = 8`, `rejectReason`) is
    /// known right here as `Rejected`, not a minute later from the stream, and (2) the ack carries the
    /// broker's real order id (`orderId`), the same one the order list and the stream use. The plain
    /// method only says "request accepted".
    async fn place(&self, req: PlaceOrderRequest) -> Result<OrderAck, Error> {
        self.check_command().await?;
        // Input first, then quota, then the cid: a refusal before the cid is marked (bad input,
        // `RateLimited`) sends nothing, so the same cid can be used again. The order is the one of
        // T-Bank and Finam.
        let side = match req.side {
            Side::Buy => BcsOrderSide::Buy,
            Side::Sell => BcsOrderSide::Sell,
        };
        // Whole pieces, a multiple of the lot when the lot is known: refused as it is, never rounded.
        whole_pieces(req.qty)?;
        self.inner
            .price_grid
            .check_qty(&req.instrument.key, req.qty)?;
        let http_req = match req.order_type {
            CoreOrderType::Market => CreateOrderRequest::market(
                req.cid,
                ticker(&req.instrument)?,
                class_code(&req.instrument)?,
                side,
                whole_pieces(req.qty)?,
            ),
            CoreOrderType::Limit => {
                let price = req.price.ok_or_else(|| Error::Rejected {
                    code: "missing_price".into(),
                    message: "limit order requires price".into(),
                })?;
                // The step is checked here, before a quota is spent or the cid is marked.
                self.inner.price_grid.check(&req.instrument.key, price)?;
                CreateOrderRequest::limit(
                    req.cid,
                    ticker(&req.instrument)?,
                    class_code(&req.instrument)?,
                    side,
                    whole_pieces(req.qty)?,
                    price.decimal(),
                )
            }
        };
        self.acquire("PlaceOrder").await?;

        // Waits for earlier commands on this instrument, released on every exit path.
        let _instrument = self.inner.instrument_locks.lock(&req.instrument).await;
        // The gate may have closed while this command stood in the queue (degraded, shutdown): ask again
        // before the cid is marked and anything is sent.
        self.check_command().await?;
        self.inner.instrument_locks.bind(req.cid, &req.instrument);

        let now = brokapi_runtime::now();
        {
            let mut orders = self.inner.orders.lock().await;
            if !orders.try_place_once(req.cid) {
                return Err(Error::Rejected {
                    code: "duplicate_place".into(),
                    message: "auto-retry of place is forbidden".into(),
                });
            }
            orders.register_place(req.cid, now);
        }

        let resp = match self.inner.http.create_order_sync(&http_req).await {
            Ok(resp) => resp,
            Err(e) => {
                let err = map_command_err(e);
                self.settle(req.cid, &Err(err.clone())).await;
                return Err(err);
            }
        };
        let outcome = self.ack_of(req.cid, &resp, now);
        self.settle(req.cid, &outcome).await;
        let (ack, order_id) = outcome?;
        if let Some(id) = order_id {
            self.learn(&id, req.cid).await;
        }
        Ok(ack)
    }

    /// `POST /orders/cancel/sync` (the order is named in the body). Synchronous for the same reason as
    /// `place`: "cancelled" means the broker says so, an order that was filled first is `Rejected`
    /// (`already_filled`), and an answer that leaves the order alive is `Unknown`, never "ok".
    async fn cancel(&self, cid: ClientOrderId) -> Result<(), Error> {
        self.check_command().await?;
        self.acquire("CancelOrder").await?;
        // The instrument comes from the orders this process placed; an older order is not serialized.
        let _instrument = self.inner.instrument_locks.lock_cid(cid).await;
        // The gate may have closed while this command stood in the queue.
        self.check_command().await?;
        let req = CancelByIdRequest::by_client_order_id(cid, uuid::Uuid::new_v4());
        let resp = self
            .inner
            .http
            .cancel_order_by_id_sync(&req)
            .await
            .map_err(map_order_cmd_err)?;
        self.cancel_outcome(cid, &resp)
    }

    /// `POST /orders/edit/sync`: the broker cancels the original and puts a new order under the new
    /// cid. The quantity and price the caller leaves out are the order's current ones (one
    /// `GET /orders`), because the request needs `orderQuantity` and a limit order needs its price. An
    /// order that is partly filled needs an explicit quantity: whether `orderQuantity` of an edit is the
    /// total or the rest is not documented (live question 7).
    async fn replace(&self, req: ReplaceOrderRequest) -> Result<OrderAck, Error> {
        self.check_command().await?;
        // The request has no instrument: it is the one of the original order, if this process
        // placed it. The new cid inherits it.
        let (qty, price) = match (req.qty, req.price) {
            (Some(q), Some(p)) => {
                if let Some(i) = self.inner.instrument_locks.instrument_of(req.original_cid) {
                    self.inner.price_grid.check(&i.key, p)?;
                }
                if let Some(i) = self.inner.instrument_locks.instrument_of(req.original_cid) {
                    self.inner.price_grid.check_qty(&i.key, q)?;
                }
                (whole_pieces(q)?, Some(p.decimal()))
            }
            (q, p) => {
                self.acquire("GetOrder").await?;
                let cur = self
                    .inner
                    .http
                    .get_order("1", &req.original_cid.hyphenated().to_string())
                    .await
                    .map_err(map_err);
                let cur = self.used_up(req.cid, cur).await?;
                let snap = map_state_list(Some(req.original_cid), &cur, brokapi_runtime::now())
                    .ok_or(Error::Unknown)?;
                if q.is_none() && snap.filled.is_positive() {
                    return Err(Error::Rejected {
                        code: "qty_required".into(),
                        message: "the order is partly filled: give the quantity of the new order"
                            .into(),
                    });
                }
                if let Some(p) = p {
                    self.inner.price_grid.check(&snap.instrument.key, p)?;
                }
                if let Some(q) = q {
                    self.inner.price_grid.check_qty(&snap.instrument.key, q)?;
                }
                let qty = q.unwrap_or(snap.qty);
                (
                    whole_pieces(qty)?,
                    p.or(snap.price).map(brokapi_core::Price::decimal),
                )
            }
        };
        self.acquire("ReplaceOrder").await?;
        let _instrument = self.inner.instrument_locks.lock_cid(req.original_cid).await;
        // The gate may have closed while this command stood in the queue.
        self.check_command().await?;
        // The new cid is used once, as for a place: an edit repeated after `Unknown` must not move the order a
        // second time.
        if !self.inner.orders.lock().await.try_place_once(req.cid) {
            return Err(Error::Rejected {
                code: "duplicate_place".into(),
                message: "auto-retry of replace is forbidden".into(),
            });
        }
        if let Some(i) = self.inner.instrument_locks.instrument_of(req.original_cid) {
            self.inner.instrument_locks.bind(req.cid, &i);
        }
        let http_req = EditOrderRequest {
            order_id_type: Some("1".into()),
            order_id: req.original_cid.hyphenated().to_string(),
            client_order_id: req.cid,
            order_type: None,
            price,
            order_quantity: qty,
            extras: Default::default(),
        };
        let resp = self
            .inner
            .http
            .edit_order_sync(&http_req)
            .await
            .map_err(map_order_cmd_err)?;
        let (ack, order_id) = self.ack_of(req.cid, &resp, brokapi_runtime::now())?;
        if let Some(id) = order_id {
            self.learn(&id, req.cid).await;
        }
        Ok(ack)
    }

    /// `GET /orders?orderIdType=1&orderId=<cid>`. The answer is about the order asked for, so its cid is
    /// the one asked.
    async fn order_state(&self, cid: ClientOrderId) -> Result<OrderSnapshot, Error> {
        self.acquire("GetOrder").await?;
        let resp = self
            .inner
            .http
            .get_order("1", &cid.hyphenated().to_string())
            .await
            .map_err(map_order_read_err)?;
        let snap = map_state_list(Some(cid), &resp, brokapi_runtime::now()).ok_or_else(|| {
            Error::Transport(brokapi_core::TransportError::Message(
                "order status answer has no report".into(),
            ))
        })?;
        if let Some(id) = &snap.broker_id {
            self.learn(id.as_str(), cid).await;
        }
        Ok(snap)
    }

    /// Active orders (`orderStatus = 3`, plain order types) from `POST /orders/search`. The list carries
    /// the broker's order id only: our cid comes from what this process has seen or from one lookup by
    /// the exchange id; an order we cannot name gets a stable foreign cid ([`foreign_cid`]).
    async fn active_orders(&self) -> Result<Vec<OrderSnapshot>, Error> {
        let records = self
            .list_orders(&[LIST_STATUS_ACTIVE], &PLAIN_LIST_TYPES)
            .await?;
        let now = brokapi_runtime::now();
        let mut out = Vec::new();
        for r in &records {
            let cid = match self.cid_of_order_id(&r.order_id).await {
                Some(c) => c,
                None => foreign_cid(&r.order_id),
            };
            if let Some(snap) = map_order_record(r, cid, now) {
                out.push(snap);
            }
        }
        Ok(out)
    }

    async fn subscribe_order_state(&self) -> Result<SubscriptionId, Error> {
        let opts = SubscribeOpts {
            backpressure: Backpressure::Lossless {
                cap: self.inner.order_state_buffer,
            },
            depth: None,
        };
        self.register_ud_sub(UdSubKind::OrderState, opts).await
    }

    /// Compares the ledger with the broker. The decision for each cid of the ledger is the answer of
    /// `GET /orders` for that order, not the list of orders (the list is cut at the 1000 most recent
    /// orders and is cut by a time window, so an order that is alive may be missing from it):
    ///
    /// - the order is alive (new, partly filled, being replaced, or being cancelled): `confirmed`, with
    ///   the snapshot of that answer;
    /// - the order is over (filled, cancelled, rejected), or the broker does not know the cid
    ///   (`not_found`): `closed_offline`;
    /// - the answer has no report, or any other error: the reconcile fails with that error and the gate
    ///   stays as it was. Nothing is guessed.
    ///
    /// `foreign` are the active orders of the list that are not in the ledger. Success opens the gate
    /// and ends `Degraded`.
    async fn reconcile(&self, ledger: &[ClientOrderId]) -> Result<ReconcileReport, Error> {
        use brokapi_core::OrderState as S;
        self.acquire("Reconcile").await?;
        let mut report = ReconcileReport::new(Vec::new(), Vec::new(), Vec::new());
        for cid in ledger {
            self.acquire("GetOrder").await?;
            match self
                .inner
                .http
                .get_order("1", &cid.hyphenated().to_string())
                .await
            {
                Ok(resp) => {
                    let snap = map_state_list(Some(*cid), &resp, brokapi_runtime::now())
                        .ok_or_else(|| {
                            Error::Transport(brokapi_core::TransportError::Message(format!(
                                "reconcile: the answer for order {cid} has no report"
                            )))
                        })?;
                    // Teaches the cid of this broker order, so the list below names it.
                    if let Some(id) = &snap.broker_id {
                        self.learn(id.as_str(), *cid).await;
                    }
                    match snap.state {
                        S::Filled { .. } | S::Cancelled { .. } | S::Rejected { .. } => {
                            report.closed_offline.push(*cid);
                        }
                        // Alive, or not clear (status 6 and the like): not closed, so the bot must not
                        // place a second order in its stead.
                        _ => report.confirmed.push(snap),
                    }
                }
                // The broker never saw this cid (or forgot it): closed while we were offline.
                Err(BcsApiError::NotFound { .. }) => report.closed_offline.push(*cid),
                // Anything else is not an answer about the order: reconcile is not done.
                Err(e) => return Err(map_err(e)),
            }
        }
        let known: std::collections::HashSet<ClientOrderId> = ledger.iter().copied().collect();
        report.foreign = self
            .active_orders()
            .await?
            .into_iter()
            .filter(|o| !o.cid.is_nil() && !known.contains(&o.cid))
            .collect();
        // A reconcile that got this far leaves `Degraded`: the answers above are the repair of what the
        // lost events may have cost.
        self.inner.emitter.clear_degraded();
        let mut ctl = self.inner.shutdown.lock().await;
        ctl.gate_mut().clear_degraded();
        ctl.gate_mut().mark_reconciled();
        Ok(report)
    }
}

#[async_trait]
impl ExecutionSource for UserDataConnector {
    /// Own trades in `range` (`POST /trades/search`), every page.
    ///
    /// The connector asks for the biggest page (`size = 100`) and reads page after page until
    /// `totalPages`. Every page spends quota of `SearchTrades`. BCS lists at most the 1000 most recent
    /// trades since 26.01.2026: when the answer says it holds that many (`totalRecords` of 1000 or more),
    /// older trades of the range may be missing, and the answer is [`Error::Rejected`] with the code
    /// `range_too_long` instead of a list that looks complete: read a shorter range.
    ///
    /// # Errors
    ///
    /// [`Error::RateLimited`], [`Error::Transport`] or [`Error::Rejected`] (including `range_too_long`),
    /// as for any read.
    async fn executions(&self, range: TimeRange) -> Result<Vec<Execution>, Error> {
        let req = TradeSearchRequest {
            start_date_time: nanos_to_datetime(range.from).to_rfc3339(),
            end_date_time: nanos_to_datetime(range.to).to_rfc3339(),
            side: None,
            tickers: None,
        };
        let mut out = Vec::new();
        for page in 0..MAX_TRADE_PAGES {
            self.acquire("SearchTrades").await?;
            let resp = self
                .inner
                .http
                .search_trades(&req, Some(TRADES_PAGE_SIZE), Some(page))
                .await
                .map_err(map_err)?;
            if resp.total_records >= TRADES_LIST_CAP {
                return Err(too_long());
            }
            let now = brokapi_runtime::now();
            out.extend(resp.records.iter().filter_map(|r| map_trade_record(r, now)));
            if u64::from(page) + 1 >= resp.total_pages {
                return Ok(out);
            }
        }
        Err(too_long())
    }

    /// Fills pushed by the `orders/events/ws` channel: messages with `lastQuantity > 0`, each
    /// `executionId` once. Lossless: a fill is never dropped (an overflow is a visible `Overflow`).
    async fn subscribe_executions(&self) -> Result<SubscriptionId, Error> {
        let opts = SubscribeOpts {
            backpressure: Backpressure::Lossless {
                cap: self.inner.order_state_buffer,
            },
            depth: None,
        };
        self.register_ud_sub(UdSubKind::Executions, opts).await
    }
}

#[async_trait]
impl PortfolioSource for UserDataConnector {
    async fn portfolio(&self) -> Result<Portfolio, Error> {
        self.acquire("GetPortfolio").await?;
        let resp = self.inner.http.get_portfolio().await.map_err(map_err)?;
        Ok(map_rest_portfolio(&resp, brokapi_runtime::now()))
    }

    async fn positions(&self) -> Result<Vec<Position>, Error> {
        Ok(self.portfolio().await?.positions)
    }

    async fn balances(&self) -> Result<Vec<Balance>, Error> {
        Ok(self.portfolio().await?.balances)
    }

    async fn subscribe_portfolio(&self, opts: SubscribeOpts) -> Result<SubscriptionId, Error> {
        self.register_ud_sub(UdSubKind::Portfolio, opts).await
    }
}

/// Stop orders are BCS conditional orders (`orderType = "5"`): `stopPrice` for a stop-loss and
/// stop-limit, `takePrice` for a take-profit, market or limit execution. How the request is built, and
/// every choice the docs leave open (lifetime, trigger direction, the meaning of `price`, TP+SL pairs),
/// is in [`crate::map::stop`]; each of them is a choice made from the docs. Both commands use the synchronous methods, for the reasons given at `place`.
///
/// There is no stream of stop order statuses: `orders/events/ws` documents only order types 1 and 2,
/// whether it carries conditional orders is a live question, so `subscribe_stop_orders` is `Unsupported`.
#[async_trait]
impl StopOrderExecutor for UserDataConnector {
    async fn place_stop(&self, req: PlaceStopOrderRequest) -> Result<OrderAck, Error> {
        self.check_command().await?;
        // Input is checked before anything is sent or any quota is spent.
        let wire = map_stop::build(&req)?;
        self.inner
            .price_grid
            .check_qty(&req.instrument.key, req.qty)?;
        self.inner
            .price_grid
            .check(&req.instrument.key, req.stop_price)?;
        if let Some(p) = req.limit_price {
            self.inner.price_grid.check(&req.instrument.key, p)?;
        }
        // Quota before the cid is marked: a `RateLimited` sent nothing, the cid stays usable.
        self.acquire("PlaceStopOrder").await?;
        let _instrument = self.inner.instrument_locks.lock(&req.instrument).await;
        // The gate may have closed while this command stood in the queue.
        self.check_command().await?;
        self.inner.instrument_locks.bind(req.cid, &req.instrument);
        let now = brokapi_runtime::now();
        {
            let mut orders = self.inner.orders.lock().await;
            if !orders.try_place_once(req.cid) {
                return Err(Error::Rejected {
                    code: "duplicate_place".into(),
                    message: "auto-retry of place_stop is forbidden".into(),
                });
            }
            orders.register_place(req.cid, now);
        }

        let resp = match self.inner.http.create_order_sync(&wire).await {
            Ok(resp) => resp,
            Err(e) => {
                let err = map_command_err(e);
                self.settle(req.cid, &Err(err.clone())).await;
                return Err(err);
            }
        };
        let outcome = self.ack_of(req.cid, &resp, now);
        self.settle(req.cid, &outcome).await;
        let (ack, order_id) = outcome?;
        if let Some(id) = order_id {
            self.learn(&id, req.cid).await;
        }
        Ok(ack)
    }

    async fn cancel_stop(&self, stop: StopOrderRef) -> Result<(), Error> {
        self.check_command().await?;
        self.acquire("CancelStopOrder").await?;
        let (known_cid, order_id_type, order_id) = match stop {
            StopOrderRef::Cid(cid) => (cid, "1", cid.hyphenated().to_string()),
            StopOrderRef::Broker(id) => {
                let (t, id) = order_ref(id.as_str());
                let cid = self
                    .inner
                    .core
                    .lock()
                    .await
                    .cid_of_order_id(&id)
                    .unwrap_or_else(|| foreign_cid(&id));
                (cid, t, id)
            }
        };
        let _instrument = self.inner.instrument_locks.lock_cid(known_cid).await;
        // The gate may have closed while this command stood in the queue.
        self.check_command().await?;
        let req = CancelByIdRequest {
            order_id_type: Some(order_id_type.into()),
            order_id,
            client_order_id: uuid::Uuid::new_v4(),
        };
        let resp = self
            .inner
            .http
            .cancel_order_by_id_sync(&req)
            .await
            .map_err(map_order_cmd_err)?;
        self.cancel_outcome(known_cid, &resp)
    }

    /// Conditional orders waiting for their trigger: `POST /orders/search`, `orderStatus = 3`, types
    /// 4..7 and 11. A TP+SL pair (type 7) is shown as its two legs with the same ids. `cid` is `None`
    /// when the order is not one this connector can name.
    async fn active_stop_orders(&self) -> Result<Vec<StopOrderSnapshot>, Error> {
        let records = self
            .list_orders(&[LIST_STATUS_ACTIVE], &STOP_LIST_TYPES)
            .await?;
        let now = brokapi_runtime::now();
        let mut out = Vec::new();
        for r in &records {
            let cid = self.cid_of_order_id(&r.order_id).await;
            out.extend(map_stop_record(r, cid, now));
        }
        Ok(out)
    }

    async fn subscribe_stop_orders(&self) -> Result<SubscriptionId, Error> {
        Err(Error::Unsupported {
            capability: "stop_order_stream".into(),
        })
    }
}
