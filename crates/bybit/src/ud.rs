//! The user data connector: orders, stop orders, fills and the portfolio of one Bybit account.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Instant;

use async_trait::async_trait;
use brokapi_bybit_protocol::http::client::BybitHttpClient;
use brokapi_bybit_protocol::http::models::{
    CancelOrderRequest, CreateOrderRequest, InstrumentInfo, OrderRecord,
};
use brokapi_bybit_protocol::ws::session::{WsSession, WsStream};
use brokapi_core::{
    Backpressure, Balance, ClientOrderId, Envelope, Error, Event, Execution, ExecutionSource,
    InstrumentId, OrderAck, OrderExecutor, OrderSnapshot, OrderState, OrderType as CoreOrderType,
    PlaceIdempotency, PlaceOrderRequest, PlaceStopOrderRequest, Portfolio, PortfolioSource,
    Position, Price, Qty, ReconcileReport, ReplaceOrderRequest, ShutdownMode, ShutdownReport,
    StopOrderExecutor, StopOrderRef, StopOrderSnapshot, SubscribeOpts, SubscriptionId, TimeRange,
    UserDataCaps,
};
use brokapi_runtime::{
    BrokerLimiter, CommandGate, Emitter, InstrumentLocks, OrderRuntime, PriceGrid,
    ShutdownController,
};
use brokapi_sansio::{CoreEvent, OrderSignal};
use tokio::sync::{Mutex, RwLock, mpsc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::cid::cid_from_link_id;
use crate::config::UserDataConfig;
use crate::core::UdCore;
use crate::core::ud::{WALLET_TOPIC, execution_topic, order_topic, position_topic};
use crate::error::{map_command_err, map_config_err, map_err};
use crate::map::execution::execution;
use crate::map::order::{is_final, is_untriggered, order_ack, order_snapshot};
use crate::map::portfolio::portfolio as map_portfolio;
use crate::map::{ms_to_nanos, side_word, stop as map_stop};
use crate::pump::{drain_backlog, run_pump};
use crate::raw::BybitRaw;
use crate::subs::{ReqIdGen, SubIdGen, UdSubKind, UdSubscription, wait_ack};
use crate::util::{nanos_to_ms, symbol_category};

/// How many orders a page of the lists holds at most (a limit of the exchange).
const ORDERS_PAGE: u32 = 50;
/// How many fills a page holds at most.
const EXECUTIONS_PAGE: u32 = 100;
/// How many positions a page holds at most.
const POSITIONS_PAGE: u32 = 200;
/// The pages read before a list is given up as too long.
const MAX_PAGES: u32 = 40;
/// The fills of a window: the exchange answers a range of 7 days at most.
const EXECUTION_WINDOW_MS: i64 = 7 * 24 * 3600 * 1000;
/// The exchange keeps the fills for two years.
const EXECUTION_HISTORY_MS: i64 = 2 * 365 * 24 * 3600 * 1000;

/// Where the connector keeps what it knows about an order of its own.
#[derive(Debug, Clone)]
struct Tracked {
    order_id: String,
    symbol: String,
}

/// The trading rules of a symbol, read once.
#[derive(Debug, Clone)]
struct Rules {
    qty_step: Qty,
    min_qty: Qty,
    tick: Price,
}

impl Rules {
    fn of(info: &InstrumentInfo) -> Self {
        Self {
            qty_step: Qty::new(info.lot_size_filter.qty_step),
            min_qty: Qty::new(info.lot_size_filter.min_order_qty),
            tick: Price::new(info.price_filter.tick_size),
        }
    }

    fn check_qty(&self, qty: Qty) -> Result<(), Error> {
        let bad = |why: String| Error::Rejected {
            code: "invalid_qty".into(),
            message: why,
        };
        if !qty.is_positive() {
            return Err(bad(format!("quantity must be positive, got {qty}")));
        }
        if self.qty_step.is_positive() && !qty.is_on_step(self.qty_step) {
            return Err(bad(format!(
                "quantity {qty} is not a whole number of steps of {}",
                self.qty_step
            )));
        }
        if qty < self.min_qty {
            return Err(bad(format!(
                "quantity {qty} is below the minimum {}",
                self.min_qty
            )));
        }
        Ok(())
    }

    fn check_price(&self, price: Price) -> Result<(), Error> {
        brokapi_core::check_price_step(price, self.tick)
    }
}

struct UdShared {
    http: BybitHttpClient,
    ws: WsSession,
    limiter: Arc<BrokerLimiter>,
    core: Arc<Mutex<UdCore>>,
    emitter: Arc<Emitter>,
    orders: Mutex<OrderRuntime>,
    instrument_locks: InstrumentLocks,
    price_grid: PriceGrid,
    subs: RwLock<HashMap<SubscriptionId, UdSubscription>>,
    sub_ids: SubIdGen,
    req_ids: ReqIdGen,
    shutdown: Mutex<ShutdownController>,
    cancel: CancellationToken,
    order_state_buffer: usize,
    category: String,
    settle_coin: String,
    demo: bool,
    ack_timeout: std::time::Duration,
    /// cid -> the number of the order, for the orders this process placed or has seen.
    tracked: StdMutex<HashMap<ClientOrderId, Tracked>>,
    /// The rules of the symbols, read from the directory.
    rules: StdMutex<HashMap<String, Rules>>,
}

impl UdShared {
    fn track(&self, cid: ClientOrderId, order_id: &str, symbol: &str) {
        if !order_id.is_empty()
            && let Ok(mut t) = self.tracked.lock()
        {
            t.insert(
                cid,
                Tracked {
                    order_id: order_id.to_string(),
                    symbol: symbol.to_string(),
                },
            );
        }
    }

    fn tracked_of(&self, cid: ClientOrderId) -> Option<Tracked> {
        self.tracked.lock().ok().and_then(|t| t.get(&cid).cloned())
    }
}

/// The user data connector of Bybit for one account: implements [`OrderExecutor`], [`StopOrderExecutor`],
/// [`ExecutionSource`] and [`PortfolioSource`].
///
/// Commands (place, cancel) go by HTTP, one request each, never repeated: a lost answer is [`Error::Unknown`]. The
/// streams (own orders, fills, positions, wallet) come over one private WebSocket session that logs in with the key,
/// reconnects on its own and restores its subscriptions. The account must be in one-way position mode (the default);
/// the unified trading account is the only kind V5 knows.
///
/// Quantities are in units of the base coin (`0.001` BTC). The connector reads the quantity step, the smallest order and
/// the price step of a symbol once, and refuses an order off the grid before anything is sent.
///
/// The `cid` goes to the exchange as the `orderLinkId` of the order, which is how it comes back in the order list, the
/// stream and the fills. A conditional order (a stop order) is an order with a trigger price: it is placed and cancelled
/// through the same methods, and listed apart in [`StopOrderExecutor::active_stop_orders`].
///
/// # Examples
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let server = brokapi_mocks::bybit::MockServer::start(Default::default()).await;
/// # let cfg = brokapi_bybit_protocol::config::BybitConfig::for_mock("mock-key", "mock-secret", &server.http_base);
/// use brokapi_bybit::limits::default_limits;
/// use brokapi_bybit::util::instrument_id;
/// use brokapi_bybit::{UserDataConfig, UserDataConnector};
/// use brokapi_core::{
///     ClientOrderId, Decimal, OrderExecutor, OrderType, PlaceOrderRequest, Price, Qty, ShutdownMode, Side, dec,
/// };
/// use brokapi_runtime::BrokerLimiter;
/// use tokio::sync::mpsc;
///
/// let (tx, _events) = mpsc::channel(256);
/// let ud = UserDataConnector::start(UserDataConfig::new(cfg, "bybit-ud"), BrokerLimiter::new(&default_limits()), tx).await?;
/// // Commands wait for the first reconcile.
/// ud.reconcile(&[]).await?;
/// let cid = ClientOrderId::new_v4();
/// let ack = ud
///     .place(PlaceOrderRequest {
///         cid,
///         instrument: instrument_id("MOCKUSDT", "linear"),
///         side: Side::Buy,
///         order_type: OrderType::Limit,
///         qty: Qty::from(10), // one step of the mock symbol
///         price: Some(Price::new(dec!(1.00))),
///     })
///     .await?;
/// assert_eq!(ack.cid, cid);
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
    /// Starts the connector: reads the clock of the exchange, checks the key pair with a read of the wallet, and opens
    /// the private WebSocket session in the background. Commands wait for the first
    /// [`reconcile`](OrderExecutor::reconcile).
    ///
    /// # Errors
    ///
    /// [`Error::Config`] when the client cannot be built or the exchange refuses the key pair (a key of the demo account
    /// used on the main host, a wrong secret, a missing right, an IP not on the list of the key);
    /// [`Error::Transport`] when the exchange cannot be reached.
    pub async fn start(
        cfg: UserDataConfig,
        limiter: Arc<BrokerLimiter>,
        out: mpsc::Sender<Envelope>,
    ) -> Result<Self, Error> {
        let http = BybitHttpClient::new(cfg.bybit.clone()).map_err(map_config_err)?;
        http.sync_time().await.map_err(map_err)?;
        http.get_wallet_balance().await.map_err(map_err)?;

        let (event_tx, event_rx) = kanal::bounded_async(512);
        let ws = WsSession::spawn(http.clone(), WsStream::Private, event_tx, cfg.ws_options());
        let cancel = CancellationToken::new();
        let category = cfg.bybit.category.as_str().to_string();
        let core = Arc::new(Mutex::new(UdCore::new(category.clone())));
        let emitter = Emitter::new(Arc::clone(&cfg.source), out);
        let gate = CommandGate::new();

        let inner = Arc::new(UdShared {
            http,
            ws,
            limiter,
            core: Arc::clone(&core),
            emitter: Arc::clone(&emitter),
            orders: Mutex::new(OrderRuntime::new()),
            instrument_locks: InstrumentLocks::new(),
            price_grid: cfg.price_grid.clone(),
            subs: RwLock::new(HashMap::new()),
            sub_ids: SubIdGen::new(1000),
            req_ids: ReqIdGen::default(),
            shutdown: Mutex::new(ShutdownController::new(gate)),
            cancel: cancel.clone(),
            order_state_buffer: cfg.order_state_buffer,
            category,
            settle_coin: cfg.bybit.settle_coin.clone(),
            demo: cfg.bybit.is_demo(),
            ack_timeout: cfg.subscribe_ack_timeout,
            tracked: StdMutex::new(HashMap::new()),
            rules: StdMutex::new(HashMap::new()),
        });
        let pump = tokio::spawn(run_pump(core, emitter, event_rx, cancel, || async {}));
        Ok(Self { inner, pump })
    }

    /// Direct access to any API method with this connector's key and rate limiter. See [`BybitRaw`].
    pub fn raw(&self) -> BybitRaw {
        BybitRaw::new(self.inner.http.clone(), Arc::clone(&self.inner.limiter))
    }

    /// The price steps and quantity steps the connector checks orders against, on top of the rules it reads from the
    /// exchange. Fill it with `learn` to be stricter.
    pub fn price_grid(&self) -> &PriceGrid {
        &self.inner.price_grid
    }

    /// What the connector can do.
    ///
    /// - `place_idempotency`: `LabelOnly`. The documentation says an `orderLinkId` is unique and a second order with the
    ///   same one is refused (code 110072), but how long the exchange remembers a closed order's id is not stated and
    ///   not checked on a live account. The library never repeats a place whatever the value.
    /// - `supports_replace`: no. An amendment of the exchange keeps the client id of the order, and the shared `replace`
    ///   promises a new order under a new cid. The amendment is available through [`raw`](Self::raw) and
    ///   `BybitHttpClient::amend_order`. `supports_stop_orders` and `streams_executions`: yes.
    /// - `ws_rpc` and `ordered_command_channel`: no (commands go by HTTP; the exchange has a WebSocket trade channel
    ///   that this connector does not use, and the demo contour has none). `sandbox`: yes on the demo contour (virtual
    ///   money), no on the main one.
    pub fn caps(&self) -> UserDataCaps {
        UserDataCaps::new(
            PlaceIdempotency::LabelOnly,
            false,
            false,
            false,
            true,
            true,
            self.inner.demo,
        )
    }

    /// Stops the connector. Unless the mode is `Immediate` it closes the trading gate first; `Graceful` also lets the
    /// bot's channel take what is still buffered. Orders at the exchange are not touched.
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

    async fn check_command(&self) -> Result<(), Error> {
        let mut ctl = self.inner.shutdown.lock().await;
        if self.inner.emitter.is_degraded() {
            ctl.gate_mut().mark_degraded();
        }
        ctl.gate().check_command()
    }

    fn acquire(&self, method: &str) -> Result<(), Error> {
        self.inner.limiter.acquire(method, brokapi_runtime::now())
    }

    fn symbol(&self, id: &InstrumentId) -> Result<String, Error> {
        symbol_category(id, &self.inner.category)
    }

    /// The rules of a symbol: from the cache, else one read of the directory.
    async fn rules_of(&self, symbol: &str) -> Result<Rules, Error> {
        if let Some(r) = self
            .inner
            .rules
            .lock()
            .ok()
            .and_then(|m| m.get(symbol).cloned())
        {
            return Ok(r);
        }
        self.acquire("GetInstruments")?;
        let info = self
            .inner
            .http
            .get_instrument(&self.inner.category, symbol)
            .await
            .map_err(map_err)?;
        let rules = Rules::of(&info);
        if let Ok(mut m) = self.inner.rules.lock() {
            m.insert(symbol.to_string(), rules.clone());
        }
        Ok(rules)
    }

    /// Takes the pages of a list until the cursor ends. The caller has taken the quota of the first page.
    async fn all_open_orders(&self) -> Result<Vec<OrderRecord>, Error> {
        let mut all = Vec::new();
        let mut cursor: Option<String> = None;
        for page in 0..MAX_PAGES {
            if page > 0 {
                self.acquire("GetOrders")?;
            }
            let batch = self
                .inner
                .http
                .get_open_orders(
                    &self.inner.category,
                    None,
                    Some(&self.inner.settle_coin),
                    None,
                    None,
                    ORDERS_PAGE,
                    cursor.as_deref(),
                )
                .await
                .map_err(map_err)?;
            all.extend(batch.list);
            if batch.next_page_cursor.is_empty() {
                return Ok(all);
            }
            cursor = Some(batch.next_page_cursor);
        }
        Err(Error::Transport(brokapi_core::TransportError::Message(
            format!(
                "the list of open orders is longer than {MAX_PAGES} pages: not returned in part"
            ),
        )))
    }

    /// The order of a cid as the exchange shows it now: working, or closed (the exchange keeps the closed ones of the
    /// last hours in the order list, and the history of the last days). `None` when it knows none.
    async fn find_record(&self, cid: ClientOrderId) -> Result<Option<OrderRecord>, Error> {
        let tracked = self.inner.tracked_of(cid);
        let (order_id, link_id) = match &tracked {
            Some(t) => (Some(t.order_id.clone()), None),
            None => (None, Some(cid.hyphenated().to_string())),
        };
        self.acquire("GetOrder")?;
        let open = self
            .inner
            .http
            .get_open_orders(
                &self.inner.category,
                None,
                Some(&self.inner.settle_coin),
                order_id.as_deref(),
                link_id.as_deref(),
                ORDERS_PAGE,
                None,
            )
            .await
            .map_err(map_err)?;
        if let Some(r) = open.list.into_iter().next() {
            return Ok(Some(r));
        }
        self.acquire("GetOrderHistory")?;
        let history = self
            .inner
            .http
            .get_order_history(
                &self.inner.category,
                None,
                order_id.as_deref(),
                link_id.as_deref(),
                ORDERS_PAGE,
                None,
            )
            .await
            .map_err(map_err)?;
        if let Some(r) = history.list.into_iter().next() {
            return Ok(Some(r));
        }
        if tracked.is_some() {
            return Ok(None);
        }
        // An order of another program carries a made-up cid: look for it among the working orders.
        let working = self.all_open_orders().await?;
        Ok(working
            .into_iter()
            .find(|r| crate::cid::foreign_cid(&r.order_id) == cid))
    }

    /// The order of a cid and its place in what this process tracks.
    async fn find_order(&self, cid: ClientOrderId) -> Result<Tracked, Error> {
        if let Some(t) = self.inner.tracked_of(cid) {
            return Ok(t);
        }
        match self.find_record(cid).await? {
            Some(r) => {
                self.inner.track(cid, &r.order_id, &r.symbol);
                Ok(Tracked {
                    order_id: r.order_id,
                    symbol: r.symbol,
                })
            }
            None => Err(Error::not_found(
                "the exchange has no order with this client order id",
            )),
        }
    }

    async fn settle(&self, cid: ClientOrderId, outcome: &Result<OrderAck, Error>) {
        let signal = match outcome {
            Ok(ack) => OrderSignal::Ack {
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

    /// The ack of a command from the answer of the exchange. An answer without the number of the order does not say what
    /// happened: unknown.
    fn ack_from(
        &self,
        cid: ClientOrderId,
        order_id: &str,
        symbol: &str,
    ) -> Result<OrderAck, Error> {
        if order_id.is_empty() {
            return Err(Error::Unknown);
        }
        self.inner.track(cid, order_id, symbol);
        Ok(order_ack(cid, order_id, brokapi_runtime::now()))
    }

    /// A read that a place needs before sending (the rules of the symbol) failed. On `Transport` the cid is used up
    /// all the same: the book tells a bot to treat `Transport` on a command as "the order may stand", and the
    /// same cid again must be `duplicate_place`, never a second order.
    async fn used_up<T>(&self, cid: ClientOrderId, read: Result<T, Error>) -> Result<T, Error> {
        if let Err(Error::Transport(_)) = &read {
            let _ = self.inner.orders.lock().await.try_place_once(cid);
        }
        read
    }

    /// Marks the start of a place command for `cid`: once, with the clock of the order runtime.
    async fn begin_place(&self, cid: ClientOrderId, what: &str) -> Result<(), Error> {
        let now = brokapi_runtime::now();
        let mut orders = self.inner.orders.lock().await;
        if !orders.try_place_once(cid) {
            return Err(Error::Rejected {
                code: "duplicate_place".into(),
                message: format!("auto-retry of {what} is forbidden"),
            });
        }
        orders.register_place(cid, now);
        Ok(())
    }

    fn snapshot_of(
        &self,
        cid: ClientOrderId,
        record: &OrderRecord,
    ) -> Result<OrderSnapshot, Error> {
        order_snapshot(record, cid, &self.inner.category, brokapi_runtime::now()).ok_or_else(|| {
            Error::Transport(brokapi_core::TransportError::Message(
                "the order answer has a side the connector cannot read".into(),
            ))
        })
    }

    /// Sends a request for an order and settles it. Shared by `place` and `place_stop`.
    async fn send_create(
        &self,
        cid: ClientOrderId,
        symbol: &str,
        body: CreateOrderRequest,
    ) -> Result<OrderAck, Error> {
        let result = self.inner.http.create_order(&body).await;
        let outcome = match result {
            Ok(r) => self.ack_from(cid, &r.order_id, symbol),
            Err(e) => Err(map_command_err(e)),
        };
        self.settle(cid, &outcome).await;
        let ack = outcome?;
        self.inner
            .core
            .lock()
            .await
            .learn_order_id(ack.broker_id.as_str(), cid);
        Ok(ack)
    }

    async fn register_ud_sub(
        &self,
        kind: UdSubKind,
        opts: SubscribeOpts,
        topics: Vec<String>,
    ) -> Result<SubscriptionId, Error> {
        let id = self.inner.sub_ids.next();
        self.inner.emitter.register(id, None, opts.backpressure);
        let reqs: Vec<(String, String)> = topics
            .iter()
            .map(|t| (self.inner.req_ids.next(), t.clone()))
            .collect();
        // The topics some other subscription already holds are not asked for again.
        let (fresh, notify) = {
            let mut core = self.inner.core.lock().await;
            let fresh: Vec<(String, String)> = reqs
                .iter()
                .filter(|(_, t)| !core.has_topic(t))
                .cloned()
                .collect();
            core.register(id, kind, &topics, &reqs);
            (fresh, core.notifier())
        };
        for (req, topic) in &fresh {
            if let Err(e) = self.inner.ws.subscribe(req, topic).await {
                self.drop_sub(id).await;
                return Err(map_err(e));
            }
        }
        // Returns when the exchange has confirmed the streams: what happens after that is delivered.
        let confirmed = wait_ack(
            &self.inner.core,
            notify,
            self.inner.ack_timeout,
            |c: &UdCore| c.sub_state(id),
        )
        .await;
        if let Err(e) = confirmed {
            self.drop_sub(id).await;
            return Err(e);
        }
        self.inner.subs.write().await.insert(
            id,
            UdSubscription {
                id,
                kind,
                opts,
                topics,
            },
        );
        Ok(id)
    }

    async fn drop_sub(&self, id: SubscriptionId) {
        let orphans = self.inner.core.lock().await.forget(id);
        self.inner.emitter.unregister(id);
        for topic in orphans {
            let _ = self.inner.ws.unsubscribe(&topic).await;
        }
    }

    /// The whole portfolio, read now.
    async fn read_portfolio(&self) -> Result<Portfolio, Error> {
        self.acquire("GetWallet")?;
        let wallets = self
            .inner
            .http
            .get_wallet_balance()
            .await
            .map_err(map_err)?;
        let mut records = Vec::new();
        let mut cursor: Option<String> = None;
        for page in 0..MAX_PAGES {
            self.acquire("GetPositions")?;
            let batch = self
                .inner
                .http
                .get_positions(
                    &self.inner.category,
                    &self.inner.settle_coin,
                    POSITIONS_PAGE,
                    cursor.as_deref(),
                )
                .await
                .map_err(map_err)?;
            records.extend(batch.list);
            if batch.next_page_cursor.is_empty() {
                break;
            }
            cursor = Some(batch.next_page_cursor);
            if page + 1 == MAX_PAGES {
                return Err(Error::Transport(brokapi_core::TransportError::Message(
                    "the list of positions is too long: not returned in part".into(),
                )));
            }
        }
        Ok(map_portfolio(
            &wallets,
            &records,
            &self.inner.category,
            brokapi_runtime::now(),
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
    async fn place(&self, req: PlaceOrderRequest) -> Result<OrderAck, Error> {
        self.check_command().await?;
        // Input first, then the rules (a read), then quota, then the cid: a refusal before the cid is marked (bad
        // input, `RateLimited`) sends nothing, so the same cid can be used again.
        let symbol = self.symbol(&req.instrument)?;
        self.inner
            .price_grid
            .check_qty(&req.instrument.key, req.qty)?;
        let price = match req.order_type {
            CoreOrderType::Market => None,
            CoreOrderType::Limit => {
                let price = req.price.ok_or_else(|| Error::Rejected {
                    code: "missing_price".into(),
                    message: "limit order requires price".into(),
                })?;
                // The step is checked here, before a quota is spent or the cid is marked.
                self.inner.price_grid.check(&req.instrument.key, price)?;
                Some(price)
            }
        };
        let rules = self.used_up(req.cid, self.rules_of(&symbol).await).await?;
        rules.check_qty(req.qty)?;
        if let Some(p) = price {
            rules.check_price(p)?;
        }
        self.acquire("PlaceOrder")?;

        // Waits for earlier commands on this instrument, released on every exit path.
        let _instrument = self.inner.instrument_locks.lock(&req.instrument).await;
        // The gate may have closed while this command stood in the queue (degraded, shutdown): ask again before the
        // cid is marked and anything is sent.
        self.check_command().await?;
        self.inner.instrument_locks.bind(req.cid, &req.instrument);
        self.begin_place(req.cid, "place").await?;

        let body = CreateOrderRequest {
            category: self.inner.category.clone(),
            symbol: symbol.clone(),
            side: side_word(req.side).to_string(),
            order_type: if price.is_some() { "Limit" } else { "Market" }.to_string(),
            qty: req.qty.decimal(),
            price: price.map(Price::decimal),
            // A market order is always immediate-or-cancel at the exchange.
            time_in_force: price.map(|_| "GTC".to_string()),
            order_link_id: Some(req.cid.hyphenated().to_string()),
            ..Default::default()
        };
        self.send_create(req.cid, &symbol, body).await
    }

    async fn cancel(&self, cid: ClientOrderId) -> Result<(), Error> {
        self.check_command().await?;
        self.acquire("CancelOrder")?;
        // The instrument comes from the orders this process placed; an older order is not serialized.
        let _instrument = self.inner.instrument_locks.lock_cid(cid).await;
        // The gate may have closed while this command stood in the queue.
        self.check_command().await?;
        let t = self.find_order(cid).await.map_err(|e| match e {
            e if e.is_not_found() => Error::Rejected {
                code: brokapi_core::NOT_FOUND.into(),
                message: "the exchange has no order with this client order id".into(),
            },
            e => e,
        })?;
        self.inner
            .http
            .cancel_order(&CancelOrderRequest {
                category: self.inner.category.clone(),
                symbol: t.symbol,
                order_id: Some(t.order_id),
                order_link_id: None,
            })
            .await
            .map_err(map_command_err)?;
        Ok(())
    }

    async fn replace(&self, _req: ReplaceOrderRequest) -> Result<OrderAck, Error> {
        // The amendment of the exchange keeps the order and its client id; this call promises a new order under a new
        // cid. Cancel and place, or amend through raw().
        Err(Error::Unsupported {
            capability: "replace".into(),
        })
    }

    async fn order_state(&self, cid: ClientOrderId) -> Result<OrderSnapshot, Error> {
        match self.find_record(cid).await? {
            Some(rec) => {
                self.inner.track(cid, &rec.order_id, &rec.symbol);
                self.snapshot_of(cid, &rec)
            }
            None => Err(Error::not_found(
                "the exchange has no order with this client order id",
            )),
        }
    }

    async fn active_orders(&self) -> Result<Vec<OrderSnapshot>, Error> {
        // all_open_orders takes the quota of the pages after the first: the first is this call's.
        self.acquire("GetOrders")?;
        let records = self.all_open_orders().await?;
        let now = brokapi_runtime::now();
        let mut out = Vec::new();
        let mut core = self.inner.core.lock().await;
        for r in &records {
            if is_untriggered(r) || is_final(r) {
                continue;
            }
            let cid = core.resolve_cid(&r.order_id, &r.order_link_id);
            // Ours or not, the cid is remembered for the number: an order of another program can be cancelled by the
            // made-up cid it was listed with.
            self.inner.track(cid, &r.order_id, &r.symbol);
            if let Some(snap) = order_snapshot(r, cid, &self.inner.category, now) {
                out.push(snap);
            }
        }
        Ok(out)
    }

    async fn subscribe_order_state(&self) -> Result<SubscriptionId, Error> {
        let opts = lossless(self.inner.order_state_buffer);
        self.register_ud_sub(
            UdSubKind::OrderState,
            opts,
            vec![order_topic(&self.inner.category)],
        )
        .await
    }

    async fn reconcile(&self, ledger: &[ClientOrderId]) -> Result<ReconcileReport, Error> {
        self.acquire("Reconcile")?;
        let open = self.all_open_orders().await?;
        let now = brokapi_runtime::now();
        let mut report = ReconcileReport::new(Vec::new(), Vec::new(), Vec::new());
        let known: HashSet<ClientOrderId> = ledger.iter().copied().collect();
        for cid in ledger {
            let tracked = self.inner.tracked_of(*cid);
            let working = open.iter().find(|r| {
                cid_from_link_id(Some(&r.order_link_id)) == Some(*cid)
                    || tracked.as_ref().is_some_and(|t| t.order_id == r.order_id)
            });
            if let Some(r) = working {
                self.inner.track(*cid, &r.order_id, &r.symbol);
                let snap = if is_untriggered(r) {
                    stop_as_order(r, *cid, &self.inner.category, now)
                } else {
                    order_snapshot(r, *cid, &self.inner.category, now)
                };
                match snap {
                    Some(s) => report.confirmed.push(s),
                    // Alive but unreadable: it is not closed.
                    None => {
                        return Err(Error::Transport(brokapi_core::TransportError::Message(
                            "an order of the ledger cannot be read".into(),
                        )));
                    }
                }
                continue;
            }
            // Not among the working orders: closed while we were offline, or never there. Ask for its end.
            match self.find_record(*cid).await? {
                Some(r) if !is_final(&r) => {
                    // Working after all (the list and the lookup disagree): not closed, so the bot must not place a
                    // second order in its stead.
                    self.inner.track(*cid, &r.order_id, &r.symbol);
                    if let Some(s) = order_snapshot(&r, *cid, &self.inner.category, now) {
                        report.confirmed.push(s);
                    }
                }
                _ => report.closed_offline.push(*cid),
            }
        }
        {
            let mut core = self.inner.core.lock().await;
            report.foreign = open
                .iter()
                .filter(|r| !is_untriggered(r) && !is_final(r))
                .filter_map(|r| {
                    let cid = core.resolve_cid(&r.order_id, &r.order_link_id);
                    (!known.contains(&cid))
                        .then(|| order_snapshot(r, cid, &self.inner.category, now))?
                })
                .collect();
        }
        // A reconcile that got this far leaves `Degraded`: the answers above are the repair of what the lost events may
        // have cost.
        self.inner.emitter.clear_degraded();
        let mut ctl = self.inner.shutdown.lock().await;
        ctl.gate_mut().clear_degraded();
        ctl.gate_mut().mark_reconciled();
        Ok(report)
    }
}

/// A conditional order that waits, as a snapshot of an order (for `reconcile`, where the ledger holds stop orders too).
fn stop_as_order(
    r: &OrderRecord,
    cid: ClientOrderId,
    category: &str,
    now: Instant,
) -> Option<OrderSnapshot> {
    let s = map_stop::snapshot(r, Some(cid), category, now)?;
    Some(
        OrderSnapshot::new(
            cid,
            s.instrument,
            OrderState::Acked {
                cid,
                broker_id: s.broker_id.clone(),
            },
            s.side,
            if s.limit_price.is_some() {
                CoreOrderType::Limit
            } else {
                CoreOrderType::Market
            },
            s.qty,
            s.ts,
        )
        .with_broker_id(Some(s.broker_id))
        .with_price(s.limit_price),
    )
}

fn lossless(cap: usize) -> SubscribeOpts {
    SubscribeOpts {
        backpressure: Backpressure::Lossless { cap },
        depth: None,
    }
}

#[async_trait]
impl ExecutionSource for UserDataConnector {
    async fn executions(&self, range: TimeRange) -> Result<Vec<Execution>, Error> {
        let now_ms = chrono::Utc::now().timestamp_millis();
        let end = nanos_to_ms(range.to).min(now_ms);
        // The exchange keeps two years and answers a range of 7 days at most: the range is read in windows, the oldest
        // first.
        let start = nanos_to_ms(range.from).max(end - EXECUTION_HISTORY_MS);
        let mut seen = HashSet::new();
        let mut out = Vec::new();
        let mut from = start;
        while from <= end {
            let to = (from + EXECUTION_WINDOW_MS).min(end);
            let mut cursor: Option<String> = None;
            for page in 0..MAX_PAGES {
                self.acquire("GetExecutions")?;
                let batch = self
                    .inner
                    .http
                    .get_executions(
                        &self.inner.category,
                        None,
                        Some(from),
                        Some(to),
                        EXECUTIONS_PAGE,
                        cursor.as_deref(),
                    )
                    .await
                    .map_err(map_err)?;
                let nanos = brokapi_runtime::now();
                let core = self.inner.core.lock().await;
                for r in &batch.list {
                    if !seen.insert(r.exec_id.clone()) {
                        continue;
                    }
                    let at = ms_to_nanos(r.exec_time);
                    if r.exec_time > 0 && (at < range.from || at >= range.to) {
                        continue;
                    }
                    let cid = cid_from_link_id(Some(&r.order_link_id))
                        .or_else(|| core.cid_of_order_id(&r.order_id));
                    if let Some(e) = execution(r, cid, &self.inner.category, nanos) {
                        out.push(e);
                    }
                }
                drop(core);
                if batch.next_page_cursor.is_empty() {
                    break;
                }
                cursor = Some(batch.next_page_cursor);
                if page + 1 == MAX_PAGES {
                    return Err(Error::Rejected {
                        code: "range_too_long".into(),
                        message: "a window of 7 days holds more fills than the connector reads: read a shorter range".into(),
                    });
                }
            }
            if to >= end {
                break;
            }
            from = to + 1;
        }
        out.sort_by_key(|e| e.ts.broker_ts.map(|t| t.as_nanos()).unwrap_or(0));
        Ok(out)
    }

    async fn subscribe_executions(&self) -> Result<SubscriptionId, Error> {
        let opts = lossless(self.inner.order_state_buffer);
        self.register_ud_sub(
            UdSubKind::Executions,
            opts,
            vec![execution_topic(&self.inner.category)],
        )
        .await
    }
}

#[async_trait]
impl PortfolioSource for UserDataConnector {
    async fn portfolio(&self) -> Result<Portfolio, Error> {
        self.read_portfolio().await
    }

    async fn positions(&self) -> Result<Vec<Position>, Error> {
        Ok(self.read_portfolio().await?.positions)
    }

    async fn balances(&self) -> Result<Vec<Balance>, Error> {
        Ok(self.read_portfolio().await?.balances)
    }

    async fn subscribe_portfolio(&self, opts: SubscribeOpts) -> Result<SubscriptionId, Error> {
        let topics = vec![
            position_topic(&self.inner.category),
            WALLET_TOPIC.to_string(),
        ];
        let id = self
            .register_ud_sub(UdSubKind::Portfolio, opts, topics)
            .await?;
        // The streams of the exchange send changes only, with no snapshot at the start. The first events are the
        // positions and balances now, read once, so the reader starts from a whole picture.
        match self.read_portfolio().await {
            Ok(p) => {
                let mut events = Vec::new();
                if !p.positions.is_empty() {
                    events.push(CoreEvent {
                        sub: Some(id),
                        payload: Event::Positions(p.positions),
                    });
                }
                if !p.balances.is_empty() {
                    events.push(CoreEvent {
                        sub: Some(id),
                        payload: Event::Balances(p.balances),
                    });
                }
                self.inner.emitter.emit(events);
                Ok(id)
            }
            Err(e) => {
                self.inner.subs.write().await.remove(&id);
                self.drop_sub(id).await;
                Err(e)
            }
        }
    }
}

#[async_trait]
impl StopOrderExecutor for UserDataConnector {
    async fn place_stop(&self, req: PlaceStopOrderRequest) -> Result<OrderAck, Error> {
        self.check_command().await?;
        // Input is checked before anything is sent or any quota is spent.
        let symbol = self.symbol(&req.instrument)?;
        self.inner
            .price_grid
            .check_qty(&req.instrument.key, req.qty)?;
        self.inner
            .price_grid
            .check(&req.instrument.key, req.stop_price)?;
        if let Some(p) = req.limit_price {
            self.inner.price_grid.check(&req.instrument.key, p)?;
        }
        let body = map_stop::build(&req, &symbol, &self.inner.category, req.cid)?;
        let rules = self.used_up(req.cid, self.rules_of(&symbol).await).await?;
        rules.check_qty(req.qty)?;
        rules.check_price(req.stop_price)?;
        if let Some(p) = req.limit_price {
            rules.check_price(p)?;
        }
        // Quota before the cid is marked: a `RateLimited` sent nothing, the cid stays usable.
        self.acquire("PlaceStopOrder")?;
        let _instrument = self.inner.instrument_locks.lock(&req.instrument).await;
        // The gate may have closed while this command stood in the queue.
        self.check_command().await?;
        self.inner.instrument_locks.bind(req.cid, &req.instrument);
        self.begin_place(req.cid, "place_stop").await?;
        self.send_create(req.cid, &symbol, body).await
    }

    async fn cancel_stop(&self, stop: StopOrderRef) -> Result<(), Error> {
        self.check_command().await?;
        self.acquire("CancelStopOrder")?;
        let (known_cid, order_id, symbol) = match stop {
            StopOrderRef::Cid(cid) => {
                let t = self.find_order(cid).await.map_err(|e| {
                    if e.is_not_found() {
                        Error::Rejected {
                            code: brokapi_core::NOT_FOUND.into(),
                            message: "the exchange has no stop order with this client order id"
                                .into(),
                        }
                    } else {
                        e
                    }
                })?;
                (cid, t.order_id, t.symbol)
            }
            StopOrderRef::Broker(id) => {
                // The cancel needs the symbol: ask for the order by its number.
                self.acquire("GetOrder")?;
                let page = self
                    .inner
                    .http
                    .get_open_orders(
                        &self.inner.category,
                        None,
                        Some(&self.inner.settle_coin),
                        Some(id.as_str()),
                        None,
                        ORDERS_PAGE,
                        None,
                    )
                    .await
                    .map_err(map_err)?;
                let rec = page
                    .list
                    .into_iter()
                    .next()
                    .ok_or_else(|| Error::Rejected {
                        code: brokapi_core::NOT_FOUND.into(),
                        message: "the exchange has no stop order with this number".into(),
                    })?;
                let cid = self
                    .inner
                    .core
                    .lock()
                    .await
                    .resolve_cid(&rec.order_id, &rec.order_link_id);
                (cid, rec.order_id, rec.symbol)
            }
        };
        let _instrument = self.inner.instrument_locks.lock_cid(known_cid).await;
        // The gate may have closed while this command stood in the queue.
        self.check_command().await?;
        self.inner
            .http
            .cancel_order(&CancelOrderRequest {
                category: self.inner.category.clone(),
                symbol,
                order_id: Some(order_id),
                order_link_id: None,
            })
            .await
            .map_err(map_command_err)?;
        Ok(())
    }

    async fn active_stop_orders(&self) -> Result<Vec<StopOrderSnapshot>, Error> {
        self.acquire("GetOrders")?;
        let records = self.all_open_orders().await?;
        let now = brokapi_runtime::now();
        let mut core = self.inner.core.lock().await;
        let mut out = Vec::new();
        for r in records.iter().filter(|r| is_untriggered(r)) {
            let cid = core.resolve_cid(&r.order_id, &r.order_link_id);
            self.inner.track(cid, &r.order_id, &r.symbol);
            if let Some(s) = map_stop::snapshot(r, Some(cid), &self.inner.category, now) {
                out.push(s);
            }
        }
        Ok(out)
    }

    async fn subscribe_stop_orders(&self) -> Result<SubscriptionId, Error> {
        // The shared events have no stop order event; the list is `active_stop_orders`.
        Err(Error::Unsupported {
            capability: "stop_order_stream".into(),
        })
    }
}
