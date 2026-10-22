//! The user data connector: orders, stop orders, fills and the portfolio of one ALOR portfolio.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Instant;

use async_trait::async_trait;
use brokapi_alor_protocol::http::client::AlorHttpClient;
use brokapi_alor_protocol::http::models::{
    InstrumentRef, LimitOrderRequest, MarketOrderRequest, OrderCommandResponse, OrderRecord,
    UserRef,
};
use brokapi_alor_protocol::ws::models::{WsIncoming, WsSubscribe};
use brokapi_alor_protocol::ws::session::WsSession;
use brokapi_core::{
    Backpressure, Balance, ClientOrderId, ConfigError, Decimal, Envelope, Error, Execution,
    ExecutionSource, InstrumentId, OrderAck, OrderExecutor, OrderSnapshot, OrderState,
    OrderType as CoreOrderType, PlaceIdempotency, PlaceOrderRequest, PlaceStopOrderRequest,
    Portfolio, PortfolioSource, Position, ReconcileReport, ReplaceOrderRequest, ShutdownMode,
    ShutdownReport, Side, StopOrderExecutor, StopOrderRef, StopOrderSnapshot, SubscribeOpts,
    SubscriptionId, TimeRange, UserDataCaps,
};
use brokapi_runtime::{
    BrokerLimiter, CommandGate, Emitter, InstrumentLocks, OrderRuntime, PriceGrid,
    ShutdownController,
};
use brokapi_sansio::OrderSignal;
use tokio::sync::{Mutex, RwLock, mpsc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::cid::cid_from_comment;
use crate::config::UserDataConfig;
use crate::core::UdCore;
use crate::core::ud::{BoardBook, board_key};
use crate::error::{map_command_err, map_config_err, map_err};
use crate::map::execution::execution;
use crate::map::instrument::board_of;
use crate::map::order::{order_ack, order_snapshot, state_of};
use crate::map::portfolio::portfolio as map_portfolio;
use crate::map::{parse_time, pieces_to_lots, stop as map_stop};
use crate::pump::{drain_backlog, run_pump};
use crate::raw::AlorRaw;
use crate::subs::{SubIdGen, UdSubKind, UdSubscription, new_guid, wait_ack};
use crate::util::{exchange_of_board, nanos_to_datetime, ticker_board};

/// How many trades the history answers at most in one page (a limit of the broker).
const TRADES_PAGE: usize = 1000;

/// Where the connector keeps what it knows about an order of its own.
#[derive(Debug, Clone)]
struct Tracked {
    order_id: String,
    stop: bool,
}

struct UdShared {
    http: AlorHttpClient,
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
    portfolio: String,
    exchange: String,
    ack_timeout: std::time::Duration,
    /// cid -> the number of the order (and whether it is a stop order), for the orders this process placed or
    /// has seen.
    tracked: StdMutex<HashMap<ClientOrderId, Tracked>>,
    /// Lot sizes read from the reference data, by (ticker, board).
    lots: StdMutex<HashMap<(String, String), Decimal>>,
    boards: BoardBook,
}

impl UdShared {
    /// Looks up what a frame needs before the core reads it: the board of an instrument of a position.
    async fn prefetch(&self, msg: &WsIncoming) {
        let WsIncoming::Data { guid, data } = msg else {
            return;
        };
        let is_portfolio = self
            .subs
            .read()
            .await
            .values()
            .any(|s| s.kind == UdSubKind::Portfolio && &s.guid == guid);
        let symbol = data.get("symbol").and_then(|s| s.as_str());
        let money = data.get("isCurrency").and_then(|b| b.as_bool()) == Some(true);
        if let (true, false, Some(symbol)) = (is_portfolio, money, symbol) {
            let _ = self.ensure_board(symbol).await;
        }
    }

    /// The main board of a ticker: from the book, else one read of the reference data.
    async fn ensure_board(&self, symbol: &str) -> Result<String, Error> {
        let key = board_key(&self.exchange, symbol);
        if let Some(b) = self.boards.lock().ok().and_then(|b| b.get(&key).cloned()) {
            return Ok(b);
        }
        self.limiter
            .acquire("GetSecurity", brokapi_runtime::now())?;
        let info = self
            .http
            .get_security(&self.exchange, symbol, None)
            .await
            .map_err(map_err)?;
        let board = info
            .primary_board
            .clone()
            .filter(|b| !b.is_empty())
            .unwrap_or_else(|| board_of(&info));
        if let Ok(mut b) = self.boards.lock() {
            b.insert(key, board.clone());
        }
        if let Some(lot) = info.lot_size {
            self.remember_lot(symbol, &board, lot);
        }
        Ok(board)
    }

    fn remember_lot(&self, ticker: &str, board: &str, lot: Decimal) {
        if let Ok(mut l) = self.lots.lock() {
            l.insert((ticker.to_string(), board.to_string()), lot);
        }
    }

    fn track(&self, cid: ClientOrderId, order_id: &str, stop: bool) {
        if !order_id.is_empty()
            && let Ok(mut t) = self.tracked.lock()
        {
            t.insert(
                cid,
                Tracked {
                    order_id: order_id.to_string(),
                    stop,
                },
            );
        }
    }

    fn tracked_of(&self, cid: ClientOrderId) -> Option<Tracked> {
        self.tracked.lock().ok().and_then(|t| t.get(&cid).cloned())
    }
}

/// The user data connector of ALOR for one portfolio: implements [`OrderExecutor`], [`StopOrderExecutor`],
/// [`ExecutionSource`] and [`PortfolioSource`].
///
/// Commands (place, cancel, replace) go by HTTP, one request each, never repeated: a lost answer is
/// [`Error::Unknown`]. The streams (own orders, fills, positions) come over one WebSocket session that
/// reconnects on its own. Quantities of the shared interface are pieces; the broker counts in lots, so the
/// connector reads the lot size of an instrument once and converts both ways.
///
/// The portfolio and the exchange come from the config (`ALOR_PORTFOLIO`, `ALOR_EXCHANGE`). An order for an
/// instrument of another exchange is refused with [`Error::Config`]: start a second connector for it.
///
/// The `cid` goes to the broker twice: as `X-REQID` of the command, and as the comment of the order. The comment
/// is how it comes back in the order list, the stream and the trades.
///
/// # Examples
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let server = brokapi_mocks::alor::MockServer::start(Default::default()).await;
/// # let cfg = brokapi_alor_protocol::config::AlorConfig::for_mock("mock-refresh-token", &server.http_base);
/// use brokapi_alor::limits::default_limits;
/// use brokapi_alor::util::instrument_id;
/// use brokapi_alor::{UserDataConfig, UserDataConnector};
/// use brokapi_core::{
///     ClientOrderId, Decimal, OrderExecutor, OrderType, PlaceOrderRequest, Price, Qty, ShutdownMode, Side, dec,
/// };
/// use brokapi_runtime::BrokerLimiter;
/// use tokio::sync::mpsc;
///
/// let (tx, _events) = mpsc::channel(256);
/// let ud = UserDataConnector::start(UserDataConfig::new(cfg, "alor-ud"), BrokerLimiter::new(&default_limits()), tx).await?;
/// // Commands wait for the first reconcile.
/// ud.reconcile(&[]).await?;
/// let cid = ClientOrderId::new_v4();
/// let ack = ud
///     .place(PlaceOrderRequest {
///         cid,
///         instrument: instrument_id("SBER", "TQBR"),
///         side: Side::Buy,
///         order_type: OrderType::Limit,
///         qty: Qty::from(10), // one lot of SBER: 10 shares
///         price: Some(Price::new(dec!(100.0))),
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
    _ws: WsSession,
}

impl UserDataConnector {
    /// Starts the connector: gets the first access token, finds the portfolio, and opens the WebSocket session in
    /// the background. Commands wait for the first [`reconcile`](OrderExecutor::reconcile).
    ///
    /// # Errors
    ///
    /// [`Error::Config`] when the client cannot be built, the broker refuses the token, or no single portfolio
    /// can be told (the config names none and the token names several or none); [`Error::Transport`] when the
    /// authorization server cannot be reached.
    pub async fn start(
        cfg: UserDataConfig,
        limiter: Arc<BrokerLimiter>,
        out: mpsc::Sender<Envelope>,
    ) -> Result<Self, Error> {
        let http = AlorHttpClient::new(cfg.alor.clone()).map_err(map_config_err)?;
        http.get_access_token().await.map_err(map_err)?;
        let portfolio = match cfg.alor.portfolio.clone() {
            Some(p) => p,
            None => {
                let mut named = http.token_portfolios().await.map_err(map_err)?;
                if named.len() != 1 {
                    return Err(Error::Config(ConfigError::Message(format!(
                        "name the portfolio (ALOR_PORTFOLIO): the access token names {} of them",
                        named.len()
                    ))));
                }
                named.remove(0)
            }
        };

        let (event_tx, event_rx) = kanal::bounded_async(512);
        let ws = WsSession::spawn_with(http.clone(), event_tx, cfg.ws_options());
        let cancel = CancellationToken::new();
        let boards = BoardBook::default();
        let core = Arc::new(Mutex::new(UdCore::new(
            cfg.alor.exchange.clone(),
            Arc::clone(&boards),
        )));
        let emitter = Emitter::new(Arc::clone(&cfg.source), out);
        let gate = CommandGate::new();

        let inner = Arc::new(UdShared {
            http,
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
            portfolio,
            exchange: cfg.alor.exchange.clone(),
            ack_timeout: cfg.subscribe_ack_timeout,
            tracked: StdMutex::new(HashMap::new()),
            lots: StdMutex::new(HashMap::new()),
            boards,
        });
        let hook = Arc::clone(&inner);
        let pump = tokio::spawn(run_pump(core, emitter, event_rx, cancel, move |m| {
            let hook = Arc::clone(&hook);
            async move {
                hook.prefetch(&m).await;
                m
            }
        }));
        Ok(Self {
            inner,
            pump,
            _ws: ws,
        })
    }

    /// Direct access to any API method with this connector's token and rate limiter. See [`AlorRaw`].
    pub fn raw(&self) -> AlorRaw {
        AlorRaw::new(self.inner.http.clone(), Arc::clone(&self.inner.limiter))
    }

    /// The portfolio this connector works with.
    pub fn portfolio_id(&self) -> &str {
        &self.inner.portfolio
    }

    /// The price steps and lot sizes the connector checks orders against. Fill it with `learn` before you trade.
    pub fn price_grid(&self) -> &PriceGrid {
        &self.inner.price_grid
    }

    /// What the connector can do.
    ///
    /// - `place_idempotency`: `LabelOnly`. The broker's documentation says a repeated `X-REQID` gets a copy of the
    ///   first answer instead of a second order, but that is not checked on a live account and the window is
    ///   unknown. The library never repeats a place whatever the value.
    /// - `supports_replace` and `supports_stop_orders`: yes. `streams_executions`: yes.
    /// - `ws_rpc` and `ordered_command_channel`: no (commands go by HTTP). `sandbox`: no (the test contour is a
    ///   contour of its own, chosen in the config).
    pub fn caps(&self) -> UserDataCaps {
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

    /// Stops the connector. Unless the mode is `Immediate` it closes the trading gate first; `Graceful` also lets
    /// the bot's channel take what is still buffered. Orders at the exchange are not touched.
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

    /// The exchange of an instrument, which must be the one of the connector.
    fn exchange_for(&self, id: &InstrumentId) -> Result<(String, String), Error> {
        let (ticker, board) = ticker_board(id)?;
        let exchange = exchange_of_board(&board);
        if exchange != self.inner.exchange {
            return Err(Error::Config(ConfigError::Message(format!(
                "{} trades on {exchange}, this connector works with {}: start another connector for it",
                id.key.display(),
                self.inner.exchange
            ))));
        }
        Ok((ticker, board))
    }

    /// The lot size of an instrument in pieces: from the grid of the user, else the cache, else one read of the
    /// reference data.
    async fn lot_of(&self, ticker: &str, board: &str, id: &InstrumentId) -> Result<Decimal, Error> {
        if let Some(lot) = self.inner.price_grid.lot(&id.key) {
            return Ok(lot.decimal());
        }
        if let Some(lot) = self
            .inner
            .lots
            .lock()
            .ok()
            .and_then(|l| l.get(&(ticker.to_string(), board.to_string())).copied())
        {
            return Ok(lot);
        }
        self.acquire("GetSecurity")?;
        let info = self
            .inner
            .http
            .get_security(&self.inner.exchange, ticker, Some(board))
            .await
            .map_err(map_err)?;
        let lot = info
            .lot_size
            .filter(|l| !l.is_zero())
            .unwrap_or(Decimal::ONE);
        self.inner.remember_lot(ticker, board, lot);
        Ok(lot)
    }

    async fn register_ud_sub(
        &self,
        kind: UdSubKind,
        opts: SubscribeOpts,
        ws: impl FnOnce(&str, &str, &str) -> WsSubscribe,
    ) -> Result<SubscriptionId, Error> {
        let id = self.inner.sub_ids.next();
        let guid = new_guid();
        let instrument = crate::util::instrument_id("", "");
        self.inner
            .emitter
            .register(id, Some(instrument), opts.backpressure);
        self.inner.core.lock().await.register(&guid, id, kind);
        let request = ws(&guid, &self.inner.exchange, &self.inner.portfolio);
        let notify = self.inner.core.lock().await.notifier();
        if let Err(e) = self._ws.subscribe(request).await {
            self.inner.core.lock().await.forget(&guid);
            self.inner.emitter.unregister(id);
            return Err(map_err(e));
        }
        // Returns when the broker has confirmed the subscription: what happens after that is delivered.
        let confirmed = wait_ack(
            &self.inner.core,
            notify,
            self.inner.ack_timeout,
            |c: &UdCore| {
                (
                    c.actual_state(id).unwrap_or(crate::core::SubState::Pending),
                    c.failure(id).map(str::to_string),
                )
            },
        )
        .await;
        if let Err(e) = confirmed {
            let _ = self._ws.unsubscribe(&guid).await;
            self.inner.core.lock().await.forget(&guid);
            self.inner.emitter.unregister(id);
            return Err(e);
        }
        self.inner.subs.write().await.insert(
            id,
            UdSubscription {
                id,
                kind,
                opts,
                guid,
            },
        );
        Ok(id)
    }

    async fn list_orders(&self) -> Result<Vec<OrderRecord>, Error> {
        self.acquire("GetOrders")?;
        self.inner
            .http
            .get_orders(&self.inner.exchange, &self.inner.portfolio)
            .await
            .map_err(map_err)
    }

    async fn list_stop_orders(
        &self,
    ) -> Result<Vec<brokapi_alor_protocol::http::models::StopOrderRecord>, Error> {
        self.acquire("GetStopOrders")?;
        self.inner
            .http
            .get_stop_orders(&self.inner.exchange, &self.inner.portfolio)
            .await
            .map_err(map_err)
    }

    /// The number of the order of a cid: from what this process knows, else from the lists of the broker (by the
    /// comment of the order).
    async fn find_order(&self, cid: ClientOrderId, stops: bool) -> Result<Tracked, Error> {
        if let Some(t) = self.inner.tracked_of(cid) {
            return Ok(t);
        }
        for r in self.list_orders().await? {
            if cid_from_comment(r.comment.as_deref()) == Some(cid)
                || crate::cid::foreign_cid(&r.id) == cid
            {
                self.inner.track(cid, &r.id, false);
                return Ok(Tracked {
                    order_id: r.id,
                    stop: false,
                });
            }
        }
        if stops {
            for r in self.list_stop_orders().await? {
                if map_stop::cid_of(&r) == Some(cid) {
                    self.inner.track(cid, &r.id, true);
                    return Ok(Tracked {
                        order_id: r.id,
                        stop: true,
                    });
                }
            }
        }
        Err(Error::not_found(
            "the broker has no order with this client order id",
        ))
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

    /// The ack of a command from the answer of the broker. An answer without the number of the order does not say
    /// what happened: unknown.
    fn ack_from(
        &self,
        cid: ClientOrderId,
        resp: &OrderCommandResponse,
        stop: bool,
    ) -> Result<OrderAck, Error> {
        let Some(number) = resp.order_number.as_deref().filter(|n| !n.is_empty()) else {
            return Err(Error::Unknown);
        };
        self.inner.track(cid, number, stop);
        Ok(order_ack(cid, number, brokapi_runtime::now()))
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

    async fn snapshot_of(
        &self,
        cid: ClientOrderId,
        record: &OrderRecord,
    ) -> Result<OrderSnapshot, Error> {
        order_snapshot(record, cid, brokapi_runtime::now()).ok_or_else(|| {
            Error::Transport(brokapi_core::TransportError::Message(
                "the order answer has no quantity in pieces".into(),
            ))
        })
    }
}

fn side_word(side: Side) -> String {
    match side {
        Side::Buy => "buy",
        Side::Sell => "sell",
    }
    .to_string()
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
        // Input first, then the lot (a read), then quota, then the cid: a refusal before the cid is marked (bad
        // input, `RateLimited`) sends nothing, so the same cid can be used again.
        let (ticker, board) = self.exchange_for(&req.instrument)?;
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
        let lot = self.lot_of(&ticker, &board, &req.instrument).await?;
        let lots = pieces_to_lots(req.qty, lot)?;
        self.acquire("PlaceOrder")?;

        // Waits for earlier commands on this instrument, released on every exit path.
        let _instrument = self.inner.instrument_locks.lock(&req.instrument).await;
        // The gate may have closed while this command stood in the queue (degraded, shutdown): ask again before
        // the cid is marked and anything is sent.
        self.check_command().await?;
        self.inner.instrument_locks.bind(req.cid, &req.instrument);
        self.begin_place(req.cid, "place").await?;

        let comment = Some(req.cid.hyphenated().to_string());
        let instrument = InstrumentRef {
            symbol: ticker,
            exchange: self.inner.exchange.clone(),
            instrument_group: Some(board),
        };
        let user = UserRef {
            portfolio: self.inner.portfolio.clone(),
        };
        let req_id = req.cid.hyphenated().to_string();
        let result = match price {
            Some(price) => {
                let body = LimitOrderRequest {
                    side: side_word(req.side),
                    quantity: lots,
                    price: price.decimal(),
                    instrument,
                    user,
                    comment,
                    time_in_force: Some("oneday".into()),
                    allow_margin: None,
                    iceberg_fixed: None,
                    iceberg_variance: None,
                };
                self.inner.http.create_limit_order(&req_id, &body).await
            }
            None => {
                let body = MarketOrderRequest {
                    side: side_word(req.side),
                    quantity: lots,
                    instrument,
                    user,
                    comment,
                    time_in_force: Some("oneday".into()),
                    allow_margin: None,
                };
                self.inner.http.create_market_order(&req_id, &body).await
            }
        };
        let outcome = match result {
            Ok(resp) => self.ack_from(req.cid, &resp, false),
            Err(e) => Err(map_command_err(e)),
        };
        self.settle(req.cid, &outcome).await;
        let ack = outcome?;
        self.inner
            .core
            .lock()
            .await
            .learn_order_id(ack.broker_id.as_str(), req.cid);
        Ok(ack)
    }

    async fn cancel(&self, cid: ClientOrderId) -> Result<(), Error> {
        self.check_command().await?;
        self.acquire("CancelOrder")?;
        // The instrument comes from the orders this process placed; an older order is not serialized.
        let _instrument = self.inner.instrument_locks.lock_cid(cid).await;
        // The gate may have closed while this command stood in the queue.
        self.check_command().await?;
        let t = self.find_order(cid, true).await.map_err(|e| match e {
            e if e.is_not_found() => Error::Rejected {
                code: brokapi_core::NOT_FOUND.into(),
                message: "the broker has no order with this client order id".into(),
            },
            e => e,
        })?;
        self.inner
            .http
            .cancel_order(
                &self.inner.exchange,
                &self.inner.portfolio,
                &t.order_id,
                t.stop,
            )
            .await
            .map_err(map_command_err)?;
        Ok(())
    }

    async fn replace(&self, req: ReplaceOrderRequest) -> Result<OrderAck, Error> {
        self.check_command().await?;
        // The broker changes an order by its number, with the whole new body: read the order first.
        let t = self.find_order(req.original_cid, false).await?;
        self.acquire("GetOrder")?;
        let cur = self
            .inner
            .http
            .get_order(&self.inner.exchange, &self.inner.portfolio, &t.order_id)
            .await
            .map_err(map_err)?;
        if cur.kind.as_deref() == Some("market") {
            return Err(Error::Rejected {
                code: "not_replaceable".into(),
                message: "a market order cannot be replaced".into(),
            });
        }
        let snap = self.snapshot_of(req.original_cid, &cur).await?;
        if req.qty.is_none() && snap.filled.is_positive() {
            return Err(Error::Rejected {
                code: "qty_required".into(),
                message: "the order is partly filled: give the quantity of the new order".into(),
            });
        }
        let (ticker, board) = self.exchange_for(&snap.instrument)?;
        let price = req.price.or(snap.price).ok_or_else(|| Error::Rejected {
            code: "missing_price".into(),
            message: "the order has no price to keep".into(),
        })?;
        self.inner.price_grid.check(&snap.instrument.key, price)?;
        let qty = req.qty.unwrap_or(snap.qty);
        self.inner.price_grid.check_qty(&snap.instrument.key, qty)?;
        let lot = self.lot_of(&ticker, &board, &snap.instrument).await?;
        let lots = pieces_to_lots(qty, lot)?;

        self.acquire("ReplaceOrder")?;
        let _instrument = self.inner.instrument_locks.lock_cid(req.original_cid).await;
        // The gate may have closed while this command stood in the queue.
        self.check_command().await?;
        self.inner.instrument_locks.bind(req.cid, &snap.instrument);
        let body = LimitOrderRequest {
            side: side_word(snap.side),
            quantity: lots,
            price: price.decimal(),
            instrument: InstrumentRef {
                symbol: ticker,
                exchange: self.inner.exchange.clone(),
                instrument_group: Some(board),
            },
            user: UserRef {
                portfolio: self.inner.portfolio.clone(),
            },
            comment: Some(req.cid.hyphenated().to_string()),
            time_in_force: cur
                .time_in_force
                .clone()
                .or_else(|| Some("oneday".to_string())),
            allow_margin: None,
            iceberg_fixed: None,
            iceberg_variance: None,
        };
        let resp = self
            .inner
            .http
            .update_limit_order(&req.cid.hyphenated().to_string(), &t.order_id, &body)
            .await
            .map_err(map_command_err)?;
        let ack = self.ack_from(req.cid, &resp, false)?;
        self.inner
            .core
            .lock()
            .await
            .learn_order_id(ack.broker_id.as_str(), req.cid);
        Ok(ack)
    }

    async fn order_state(&self, cid: ClientOrderId) -> Result<OrderSnapshot, Error> {
        let t = self.find_order(cid, false).await?;
        self.acquire("GetOrder")?;
        let rec = self
            .inner
            .http
            .get_order(&self.inner.exchange, &self.inner.portfolio, &t.order_id)
            .await
            .map_err(map_err)?;
        self.snapshot_of(cid, &rec).await
    }

    async fn active_orders(&self) -> Result<Vec<OrderSnapshot>, Error> {
        let records = self.list_orders().await?;
        let now = brokapi_runtime::now();
        let mut out = Vec::new();
        let mut core = self.inner.core.lock().await;
        for r in &records {
            if !is_working(r.status.as_deref()) {
                continue;
            }
            let cid = core.resolve_cid(&r.id, r.comment.as_deref());
            // Ours or not, the cid is remembered for the number: an order of another program can be cancelled
            // by the made-up cid it was listed with.
            self.inner.track(cid, &r.id, false);
            if let Some(snap) = order_snapshot(r, cid, now) {
                out.push(snap);
            }
        }
        Ok(out)
    }

    async fn subscribe_order_state(&self) -> Result<SubscriptionId, Error> {
        let opts = lossless(self.inner.order_state_buffer);
        self.register_ud_sub(UdSubKind::OrderState, opts, |guid, ex, pf| {
            WsSubscribe::orders(guid, ex, pf, true)
        })
        .await
    }

    async fn reconcile(&self, ledger: &[ClientOrderId]) -> Result<ReconcileReport, Error> {
        self.acquire("Reconcile")?;
        let orders = self.list_orders().await?;
        let stops = self.list_stop_orders().await?;
        let now = brokapi_runtime::now();
        let mut report = ReconcileReport::new(Vec::new(), Vec::new(), Vec::new());
        let known: HashSet<ClientOrderId> = ledger.iter().copied().collect();
        for cid in ledger {
            let found_order = orders.iter().find(|r| {
                cid_from_comment(r.comment.as_deref()) == Some(*cid)
                    || self
                        .inner
                        .tracked_of(*cid)
                        .is_some_and(|t| !t.stop && t.order_id == r.id)
            });
            if let Some(r) = found_order {
                self.inner.track(*cid, &r.id, false);
                let snap = self.snapshot_of(*cid, r).await?;
                match snap.state {
                    OrderState::Filled { .. }
                    | OrderState::Cancelled { .. }
                    | OrderState::Rejected { .. } => {
                        report.closed_offline.push(*cid);
                    }
                    // Alive, or not clear: not closed, so the bot must not place a second order in its stead.
                    _ => report.confirmed.push(snap),
                }
                continue;
            }
            let found_stop = stops.iter().find(|r| {
                map_stop::cid_of(r) == Some(*cid)
                    || self
                        .inner
                        .tracked_of(*cid)
                        .is_some_and(|t| t.stop && t.order_id == r.id)
            });
            match found_stop {
                Some(r)
                    if matches!(
                        map_stop::state_of(r.status.as_deref()),
                        brokapi_core::StopOrderState::Active
                    ) =>
                {
                    self.inner.track(*cid, &r.id, true);
                    if let Some(s) = stop_as_order(r, *cid, now) {
                        report.confirmed.push(s);
                    } else {
                        // Alive but unreadable: it is not closed.
                        return Err(Error::Transport(brokapi_core::TransportError::Message(
                            "a stop order of the ledger cannot be read".into(),
                        )));
                    }
                }
                // Triggered, cancelled, or never seen by the broker today: closed while we were offline.
                _ => report.closed_offline.push(*cid),
            }
        }
        report.foreign = self
            .active_orders()
            .await?
            .into_iter()
            .filter(|o| !o.cid.is_nil() && !known.contains(&o.cid))
            .collect();
        // A reconcile that got this far leaves `Degraded`: the answers above are the repair of what the lost
        // events may have cost.
        self.inner.emitter.clear_degraded();
        let mut ctl = self.inner.shutdown.lock().await;
        ctl.gate_mut().clear_degraded();
        ctl.gate_mut().mark_reconciled();
        Ok(report)
    }
}

/// A working stop order as a snapshot of an order (for `reconcile`, where the ledger holds stop orders too).
fn stop_as_order(
    r: &brokapi_alor_protocol::http::models::StopOrderRecord,
    cid: ClientOrderId,
    now: Instant,
) -> Option<OrderSnapshot> {
    let s = crate::map::stop::snapshot(r, Some(cid), now)?;
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

fn is_working(status: Option<&str>) -> bool {
    !matches!(
        state_of(
            status,
            uuid::Uuid::nil(),
            "",
            brokapi_core::Qty::ZERO,
            brokapi_core::Qty::ZERO
        ),
        OrderState::Filled { .. } | OrderState::Cancelled { .. } | OrderState::Rejected { .. }
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
        let now = brokapi_runtime::now();
        let mut seen = HashSet::new();
        let mut out = Vec::new();
        // The history of past days, from the date the range starts. One page of the broker holds 1000 trades: a
        // full page may hide more, and paging by trade number is not verified against the live API, so a range
        // that fills it is refused instead of cut.
        self.acquire("GetTrades")?;
        let from = nanos_to_datetime(range.from).format("%Y-%m-%d").to_string();
        let query = [
            ("dateFrom", from),
            ("limit", TRADES_PAGE.to_string()),
            ("orderByTradeDate", "true".to_string()),
        ];
        let history = self
            .inner
            .http
            .get_history_trades(&self.inner.exchange, &self.inner.portfolio, &query)
            .await
            .map_err(map_err)?;
        if history.len() >= TRADES_PAGE {
            return Err(Error::Rejected {
                code: "range_too_long".into(),
                message: format!(
                    "the history holds {TRADES_PAGE} trades at most per answer and the range reaches that: read it in shorter ranges"
                ),
            });
        }
        // The trades of today.
        self.acquire("GetTrades")?;
        let today = self
            .inner
            .http
            .get_trades(&self.inner.exchange, &self.inner.portfolio)
            .await
            .map_err(map_err)?;
        let core = self.inner.core.lock().await;
        for r in history.iter().chain(today.iter()) {
            if !seen.insert(r.id.clone()) {
                continue;
            }
            let at = parse_time(r.date.as_deref());
            if at.is_some_and(|t| t < range.from || t >= range.to) {
                continue;
            }
            let order_no = r.orderno.clone().unwrap_or_default();
            let cid =
                cid_from_comment(r.comment.as_deref()).or_else(|| core.cid_of_order_id(&order_no));
            if let Some(e) = execution(r, cid, now) {
                out.push(e);
            }
        }
        Ok(out)
    }

    async fn subscribe_executions(&self) -> Result<SubscriptionId, Error> {
        let opts = lossless(self.inner.order_state_buffer);
        self.register_ud_sub(UdSubKind::Executions, opts, |guid, ex, pf| {
            WsSubscribe::trades(guid, ex, pf, true)
        })
        .await
    }
}

#[async_trait]
impl PortfolioSource for UserDataConnector {
    async fn portfolio(&self) -> Result<Portfolio, Error> {
        self.acquire("GetPositions")?;
        let records = self
            .inner
            .http
            .get_positions(&self.inner.exchange, &self.inner.portfolio, false)
            .await
            .map_err(map_err)?;
        // The positions do not name the board of an instrument: look it up (once per ticker).
        for r in &records {
            if r.is_currency != Some(true) && r.qty_units.is_some_and(|q| !q.is_zero()) {
                self.inner.ensure_board(&r.symbol).await?;
            }
        }
        let boards = Arc::clone(&self.inner.boards);
        let exchange = self.inner.exchange.clone();
        Ok(map_portfolio(
            &records,
            |symbol| {
                boards
                    .lock()
                    .ok()
                    .and_then(|b| b.get(&board_key(&exchange, symbol)).cloned())
                    .unwrap_or_default()
            },
            brokapi_runtime::now(),
        ))
    }

    async fn positions(&self) -> Result<Vec<Position>, Error> {
        Ok(self.portfolio().await?.positions)
    }

    async fn balances(&self) -> Result<Vec<Balance>, Error> {
        Ok(self.portfolio().await?.balances)
    }

    async fn subscribe_portfolio(&self, opts: SubscribeOpts) -> Result<SubscriptionId, Error> {
        // The first messages are the current positions (the history of the subscription), then the changes.
        self.register_ud_sub(UdSubKind::Portfolio, opts, |guid, ex, pf| {
            WsSubscribe::positions(guid, ex, pf, false)
        })
        .await
    }
}

#[async_trait]
impl StopOrderExecutor for UserDataConnector {
    async fn place_stop(&self, req: PlaceStopOrderRequest) -> Result<OrderAck, Error> {
        self.check_command().await?;
        // Input is checked before anything is sent or any quota is spent.
        let (ticker, board) = self.exchange_for(&req.instrument)?;
        self.inner
            .price_grid
            .check_qty(&req.instrument.key, req.qty)?;
        self.inner
            .price_grid
            .check(&req.instrument.key, req.stop_price)?;
        if let Some(p) = req.limit_price {
            self.inner.price_grid.check(&req.instrument.key, p)?;
        }
        let lot = self.lot_of(&ticker, &board, &req.instrument).await?;
        let now_secs = chrono::Utc::now().timestamp();
        let (wire, limit) = map_stop::build(
            &req,
            lot,
            &self.inner.portfolio,
            &self.inner.exchange,
            now_secs,
            req.cid,
        )?;
        // Quota before the cid is marked: a `RateLimited` sent nothing, the cid stays usable.
        self.acquire("PlaceStopOrder")?;
        let _instrument = self.inner.instrument_locks.lock(&req.instrument).await;
        // The gate may have closed while this command stood in the queue.
        self.check_command().await?;
        self.inner.instrument_locks.bind(req.cid, &req.instrument);
        self.begin_place(req.cid, "place_stop").await?;
        let req_id = req.cid.hyphenated().to_string();
        let result = if limit {
            self.inner
                .http
                .create_stop_limit_order(&req_id, &wire)
                .await
        } else {
            self.inner.http.create_stop_order(&req_id, &wire).await
        };
        let outcome = match result {
            Ok(resp) => self.ack_from(req.cid, &resp, true),
            Err(e) => Err(map_command_err(e)),
        };
        self.settle(req.cid, &outcome).await;
        let ack = outcome?;
        self.inner
            .core
            .lock()
            .await
            .learn_order_id(ack.broker_id.as_str(), req.cid);
        Ok(ack)
    }

    async fn cancel_stop(&self, stop: StopOrderRef) -> Result<(), Error> {
        self.check_command().await?;
        self.acquire("CancelStopOrder")?;
        let (known_cid, order_id) = match stop {
            StopOrderRef::Cid(cid) => {
                let t = self.find_order(cid, true).await.map_err(|e| {
                    if e.is_not_found() {
                        Error::Rejected {
                            code: brokapi_core::NOT_FOUND.into(),
                            message: "the broker has no stop order with this client order id"
                                .into(),
                        }
                    } else {
                        e
                    }
                })?;
                (cid, t.order_id)
            }
            StopOrderRef::Broker(id) => {
                let cid = self
                    .inner
                    .core
                    .lock()
                    .await
                    .cid_of_order_id(id.as_str())
                    .unwrap_or_else(|| crate::cid::foreign_cid(id.as_str()));
                (cid, id.as_str().to_string())
            }
        };
        let _instrument = self.inner.instrument_locks.lock_cid(known_cid).await;
        // The gate may have closed while this command stood in the queue.
        self.check_command().await?;
        self.inner
            .http
            .cancel_order(&self.inner.exchange, &self.inner.portfolio, &order_id, true)
            .await
            .map_err(map_command_err)?;
        Ok(())
    }

    async fn active_stop_orders(&self) -> Result<Vec<StopOrderSnapshot>, Error> {
        let records = self.list_stop_orders().await?;
        let now = brokapi_runtime::now();
        let core = self.inner.core.lock().await;
        let mut out = Vec::new();
        for r in &records {
            let cid = map_stop::cid_of(r).or_else(|| core.cid_of_order_id(&r.id));
            if let Some(cid) = cid {
                self.inner.track(cid, &r.id, true);
            }
            if let Some(s) = map_stop::snapshot(r, cid, now)
                && matches!(s.state, brokapi_core::StopOrderState::Active)
            {
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
