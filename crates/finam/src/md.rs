//! Finam market data connector: [`MarketDataConnector`].

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use brokapi_core::{
    Candle, ConfigError, Envelope, Error, Instrument, InstrumentId, InstrumentQuery,
    InstrumentSource, MarketDataCaps, MarketDataSource, MarketTrade, OrderBook, OrderBookSource,
    Schedule, ShutdownMode, ShutdownReport, StreamingCaps, SubscribeOpts, SubscriptionId,
    TimeRange, Timeframe, TradingParams, TradingStatus,
};
use brokapi_finam_protocol::FinamApi;
use brokapi_finam_protocol::pb::google::r#type::Interval;
use brokapi_finam_protocol::pb::grpc::tradeapi::v1::assets as pa;
use brokapi_finam_protocol::pb::grpc::tradeapi::v1::marketdata as pm;
use brokapi_runtime::{BackpressureManager, BrokerLimiter};
use brokapi_sansio::{
    BrokerCore, CoreEvent, DesiredSubscription, EventSink, SubscriptionKind, SubscriptionSet,
};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::config::MarketDataConfig;
use crate::core::md::DEFAULT_DEPTH;
use crate::core::{MdCore, MdEvent, MdFrame, StreamEvent, SubStatus};
use crate::error::{map_protocol_err, transport};
use crate::map::instrument::{
    availability_at, kind_from_type, map_asset, map_get_asset, map_schedule, map_trading_params,
};
use crate::map::m10::{floor_m10, merge_m5};
use crate::map::market::{map_bar, map_rest_order_book, map_trade, wire_timeframe};
use crate::pump::{PumpIo, SharedBackpressure, run_pump};
use crate::raw::FinamRaw;
use crate::rpc::Rpc;
use crate::stream::{OpenFn, StreamHandle, StreamParams, spawn_stream};
use crate::subs::SubIdGen;
use crate::util::{now_nanos, symbol_of, to_timestamp};

/// Finam does not document a book depth limit; the server decides what it sends and we truncate
/// to the requested depth.
const MAX_DEPTH: u32 = DEFAULT_DEPTH;
/// Safety bound on pages when listing the whole directory.
const MAX_ASSET_PAGES: usize = 200;

type MdWrap<M> = Arc<dyn Fn(StreamEvent<M>) -> MdFrame + Send + Sync>;

struct MdShared {
    rpc: Rpc,
    core: Arc<Mutex<MdCore>>,
    backpressure: SharedBackpressure,
    streams: Arc<Mutex<HashMap<SubscriptionId, StreamHandle>>>,
    frames: mpsc::Sender<MdFrame>,
    sub_ids: SubIdGen,
    root: CancellationToken,
    ack_timeout: Duration,
}

/// Finam market data connector: order books, trades, quotes, candles and the instrument directory.
///
/// One connector is enough for the whole broker, it is not tied to an account. It implements
/// [`OrderBookSource`], [`MarketDataSource`] and [`InstrumentSource`]. Queries return their answer
/// directly. Subscriptions deliver [`Envelope`]s into the channel given to
/// [`start`](Self::start).
///
/// What is specific to Finam:
///
/// - **Instruments are `SYMBOL@MIC`.** Pass `NativeId::SymbolMic`. Any other native id gives
///   `Error::Config` without a network call.
/// - **Order books come as full snapshots.** Finam sends rows with ADD, UPDATE and REMOVE actions. The
///   core folds them into a local book, so you never see increments (`incremental_order_book` is
///   false). If an increment arrives with no snapshot under it, the stream is reopened and you get an
///   `Event::Resync`.
/// - **Depth.** Finam documents no depth limit. The book is cut to the depth you ask for, at most
///   [`caps().max_order_book_depth`](Self::caps).
/// - **Every call spends quota.** Opening a stream counts as a request too. A refusal is
///   `Error::RateLimited`, and nothing is sent.
/// - **Streams reconnect by themselves** with a growing pause (0.5 s up to 30 s). After a reconnect
///   of an order book stream you get `Event::Resync` and then a new snapshot. Trades, quotes and
///   candle streams resume without a notice (trades already seen are not repeated).
///   [`sub_status`](Self::sub_status) says what a stream is doing.
/// - **M10 candles** are built by the connector from M5 candles, because the wire has no M10.
/// - **Schedules are per instrument**: pass `SBER@MISX` as the `exchange` argument of `schedule`.
///
/// # Examples
///
/// Order book by request, then as a stream, with the instrument directory and an error path:
///
/// ```
/// use std::time::Duration;
/// use brokapi_core::{
///     Error, Event, InstrumentId, InstrumentKey, InstrumentQuery, InstrumentSource, NativeId, OrderBook,
///     OrderBookSource, ShutdownMode, SubscribeOpts,
/// };
/// use brokapi_finam::limits::default_limits;
/// use brokapi_finam::{MarketDataConfig, MarketDataConnector};
/// use brokapi_runtime::BrokerLimiter;
/// use tokio::sync::mpsc;
/// # use brokapi_finam::FinamApiConfig;
/// # use brokapi_mocks::finam::{MockServer, SECRET};
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let server = MockServer::start().await;
/// # let cfg = FinamApiConfig::new(SECRET).with_endpoint(server.url.clone());
/// // `cfg` is your `FinamApiConfig`: the secret, and the endpoint of the real broker by default.
/// let (events_tx, mut events) = mpsc::channel(256);
/// let limiter = BrokerLimiter::new(&default_limits());
/// let md = MarketDataConnector::start(MarketDataConfig::new(cfg, "finam-md"), limiter, events_tx).await?;
///
/// let sber = InstrumentId::new(
///     InstrumentKey::new("SBER", "MISX"),
///     NativeId::SymbolMic { symbol: "SBER".into(), mic: "MISX".into() },
/// );
///
/// // By request: a snapshot.
/// let OrderBook::Snapshot { bids, asks, .. } = md.order_book(&sber, 10).await? else {
///     panic!("a request answers a snapshot")
/// };
/// assert!(!bids.is_empty() && !asks.is_empty());
///
/// // As a stream: envelopes arrive in `events`.
/// let sub = md.subscribe_order_book(&sber, SubscribeOpts::conflate()).await?;
/// let envelope = tokio::time::timeout(Duration::from_secs(5), events.recv()).await?.expect("channel open");
/// assert_eq!(envelope.sub, Some(sub));
/// assert!(matches!(envelope.payload, Event::OrderBook(_) | Event::Resync { .. }));
/// md.unsubscribe(sub).await?;
///
/// // The directory: one `GetAsset` when the exchange is known.
/// let found = md
///     .find(InstrumentQuery::ByTicker { ticker: "SBER".into(), exchange: Some("MISX".into()) })
///     .await?;
/// assert_eq!(found[0].id.key.symbol, "SBER");
///
/// // An instrument that is not a Finam symbol fails before anything is sent.
/// let foreign = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::Figi("BBG004730N88".into()));
/// assert!(matches!(md.order_book(&foreign, 5).await, Err(Error::Config(_))));
///
/// md.shutdown(ShutdownMode::Immediate).await;
/// # Ok(())
/// # }
/// ```
pub struct MarketDataConnector {
    inner: Arc<MdShared>,
    pump: JoinHandle<()>,
}

impl MarketDataConnector {
    /// Logs in, opens a lazy channel and starts the event pump. Streams are opened later, by `subscribe_*`.
    ///
    /// `limiter` is the rate limiter of your Finam user: create one per user and share it with every
    /// connector. Events and subscription results go to `out`. Give it room, because a full channel makes
    /// a `Lossless` subscription overflow and a `Conflate` one merge events.
    ///
    /// If `cfg.api.tokens` is set, that token manager is used and no login happens here.
    ///
    /// # Errors
    ///
    /// - [`Error::Config`]: empty secret, or an invalid endpoint URL.
    /// - [`Error::Rejected`]: the broker refused the secret (code `unauthenticated`).
    /// - [`Error::RateLimited`]: the broker answered `RESOURCE_EXHAUSTED` to the login.
    /// - [`Error::Transport`]: the broker could not be reached.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::{Error, ShutdownMode};
    /// use brokapi_finam::limits::default_limits;
    /// use brokapi_finam::{FinamApiConfig, MarketDataConfig, MarketDataConnector};
    /// use brokapi_runtime::BrokerLimiter;
    /// use tokio::sync::mpsc;
    /// # use brokapi_mocks::finam::{MockServer, SECRET};
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = MockServer::start().await;
    /// # let cfg = FinamApiConfig::new(SECRET).with_endpoint(server.url.clone());
    /// let limiter = BrokerLimiter::new(&default_limits());
    /// let (events, _rx) = mpsc::channel(256);
    /// let md = MarketDataConnector::start(MarketDataConfig::new(cfg.clone(), "finam-md"), limiter.clone(), events.clone()).await?;
    /// md.shutdown(ShutdownMode::Immediate).await;
    ///
    /// // A wrong secret is refused at start.
    /// let wrong = FinamApiConfig { secret: "wrong".into(), ..cfg };
    /// let err = MarketDataConnector::start(MarketDataConfig::new(wrong, "finam-md"), limiter, events).await.err().unwrap();
    /// assert!(matches!(err, Error::Rejected { .. }));
    /// # Ok(())
    /// # }
    /// ```
    pub async fn start(
        cfg: MarketDataConfig,
        limiter: Arc<BrokerLimiter>,
        out: mpsc::Sender<Envelope>,
    ) -> Result<Self, Error> {
        let tokens = cfg.api.resolve_tokens().await?;
        let api = FinamApi::connect(&cfg.api.endpoint, tokens, cfg.api.bearer)
            .map_err(|e| map_protocol_err(&e))?;

        let (frames, rx) = mpsc::channel::<MdFrame>(1024);
        let core = Arc::new(Mutex::new(MdCore::new()));
        let backpressure: SharedBackpressure = Arc::new(Mutex::new(BackpressureManager::new()));
        let streams: Arc<Mutex<HashMap<SubscriptionId, StreamHandle>>> = Arc::default();
        let root = CancellationToken::new();

        let handler = {
            let core = Arc::clone(&core);
            let streams = Arc::clone(&streams);
            move |frame: MdFrame, now: Instant| -> Vec<CoreEvent> {
                let mut sink = EventSink::new();
                let restarts = {
                    let mut c = core.lock().expect("core poisoned");
                    c.on_frame(frame, now, &mut sink);
                    c.take_restarts()
                };
                if !restarts.is_empty() {
                    let map = streams.lock().expect("streams poisoned");
                    for sub in restarts {
                        if let Some(h) = map.get(&sub) {
                            h.restart();
                        }
                    }
                }
                sink.drain()
            }
        };
        // Market-data buffers never gate commands, so overflow needs no further reaction.
        let pump = tokio::spawn(run_pump(
            PumpIo {
                rx,
                out,
                source: Arc::clone(&cfg.source),
                backpressure: Arc::clone(&backpressure),
                cancel: root.child_token(),
            },
            handler,
            Arc::new(|_| {}),
        ));

        let inner = Arc::new(MdShared {
            rpc: Rpc {
                api,
                limiter,
                timeout: cfg.api.request_timeout,
            },
            core,
            backpressure,
            streams,
            frames,
            sub_ids: SubIdGen::new(0),
            root,
            ack_timeout: cfg.subscribe_ack_timeout,
        });
        Ok(Self { inner, pump })
    }

    /// Any other method of the Finam API with this connector's token, endpoint and rate limiter.
    /// See [`FinamRaw`].
    ///
    /// # Examples
    ///
    /// See the [`raw` module](crate::raw) for a complete call.
    pub fn raw(&self) -> FinamRaw {
        FinamRaw::new(self.inner.rpc.clone(), None)
    }

    /// What this connector can do. Order books are streamed as full snapshots: Finam sends rows
    /// with ADD/UPDATE/REMOVE actions, which the core folds into a local book, so the consumer
    /// never sees increments (and `Conflate` is safe).
    ///
    /// All four streams (order book, market trades, quotes, candles) are on. The maximum depth is
    /// the cut this connector applies, not a limit Finam documents.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_finam::limits::default_limits;
    /// use brokapi_finam::{FinamApiConfig, MarketDataConfig, MarketDataConnector};
    /// use brokapi_core::ShutdownMode;
    /// use brokapi_runtime::BrokerLimiter;
    /// use tokio::sync::mpsc;
    /// # use brokapi_mocks::finam::{MockServer, SECRET};
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = MockServer::start().await;
    /// # let cfg = FinamApiConfig::new(SECRET).with_endpoint(server.url.clone());
    /// # let (events, _rx) = mpsc::channel(16);
    /// # let md = MarketDataConnector::start(MarketDataConfig::new(cfg, "finam-md"), BrokerLimiter::new(&default_limits()), events).await?;
    /// let caps = md.caps();
    /// assert!(caps.streaming.order_book && caps.streaming.candles);
    /// assert!(!caps.incremental_order_book);
    /// # md.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    pub fn caps(&self) -> MarketDataCaps {
        MarketDataCaps {
            max_order_book_depth: MAX_DEPTH,
            incremental_order_book: false,
            streaming: StreamingCaps {
                order_book: true,
                market_trades: true,
                quotes: true,
                candles: true,
            },
        }
    }

    /// State of a subscription's stream (connecting, active, reconnecting, failed).
    ///
    /// `None` for an id this connector does not know (never issued, or already unsubscribed).
    /// Use it after `subscribe_*` returned and the stream may still be opening.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use brokapi_core::{InstrumentId, InstrumentKey, NativeId, OrderBookSource, ShutdownMode, SubscribeOpts};
    /// use brokapi_finam::limits::default_limits;
    /// use brokapi_finam::{FinamApiConfig, MarketDataConfig, MarketDataConnector, SubStatus};
    /// use brokapi_runtime::BrokerLimiter;
    /// use tokio::sync::mpsc;
    /// # use brokapi_mocks::finam::{MockServer, SECRET};
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = MockServer::start().await;
    /// # let cfg = FinamApiConfig::new(SECRET).with_endpoint(server.url.clone());
    /// # let (events, mut rx) = mpsc::channel(256);
    /// # let md = MarketDataConnector::start(MarketDataConfig::new(cfg, "finam-md"), BrokerLimiter::new(&default_limits()), events).await?;
    /// # let sber = InstrumentId::new(InstrumentKey::new("SBER", "MISX"), NativeId::SymbolMic { symbol: "SBER".into(), mic: "MISX".into() });
    /// let sub = md.subscribe_order_book(&sber, SubscribeOpts::conflate()).await?;
    /// // Wait for the first event: by then the stream is open.
    /// tokio::time::timeout(Duration::from_secs(5), rx.recv()).await?;
    /// assert_eq!(md.sub_status(sub), Some(SubStatus::Active));
    ///
    /// md.unsubscribe(sub).await?;
    /// assert_eq!(md.sub_status(sub), None);
    /// # md.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    pub fn sub_status(&self, sub: SubscriptionId) -> Option<SubStatus> {
        self.inner.core.lock().expect("core poisoned").status(sub)
    }

    /// Graceful: every stream is cancelled (the protocol-level unsubscribe of gRPC) and awaited
    /// within `deadline`; `Immediate` aborts them.
    ///
    /// Consumes the connector and stops the event pump. The pump makes one last pass and hands over
    /// what fits into your channel without waiting. Events that do not fit are dropped. There are no commands here, so nothing is abandoned in flight
    /// (`inflight_abandoned` is always empty).
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use brokapi_core::ShutdownMode;
    /// use brokapi_finam::limits::default_limits;
    /// use brokapi_finam::{FinamApiConfig, MarketDataConfig, MarketDataConnector};
    /// use brokapi_runtime::BrokerLimiter;
    /// use tokio::sync::mpsc;
    /// # use brokapi_mocks::finam::{MockServer, SECRET};
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = MockServer::start().await;
    /// # let cfg = FinamApiConfig::new(SECRET).with_endpoint(server.url.clone());
    /// # let (events, _rx) = mpsc::channel(16);
    /// let md = MarketDataConnector::start(MarketDataConfig::new(cfg, "finam-md"), BrokerLimiter::new(&default_limits()), events).await?;
    /// let report = md.shutdown(ShutdownMode::Graceful { deadline: Duration::from_secs(2) }).await;
    /// assert_eq!(report.unsubscribe_failed, 0);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn shutdown(self, mode: ShutdownMode) -> ShutdownReport {
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
        self.inner.root.cancel();
        let _ = self.pump.await;
        ShutdownReport {
            unsubscribed: ok,
            unsubscribe_failed: failed,
            inflight_abandoned: Vec::new(),
        }
    }

    fn quota_check(&self) -> Result<(), Error> {
        let max = self.inner.rpc.limiter.conn_quota().max_subs_per_conn;
        if self.inner.streams.lock().expect("streams poisoned").len() >= max {
            return Err(Error::Rejected {
                code: "subscription_quota".into(),
                message: format!("at most {max} streams per connection"),
            });
        }
        Ok(())
    }

    /// Registers the subscription in the core and buffers, opens the stream, and waits briefly
    /// for the first outcome: a refusal (bad symbol, no permission) is returned as an error,
    /// a slow or unreachable server leaves the subscription connecting.
    async fn register_sub<M: Send + 'static>(
        &self,
        instrument: InstrumentId,
        core_kind: SubscriptionKind,
        opts: SubscribeOpts,
        method: &'static str,
        build: impl FnOnce(SubscriptionId) -> (OpenFn<M>, MdWrap<M>),
    ) -> Result<SubscriptionId, Error> {
        self.quota_check()?;
        let id = self.inner.sub_ids.next();
        let (open, wrap) = build(id);
        self.inner
            .backpressure
            .lock()
            .expect("backpressure poisoned")
            .register(id, instrument, opts.backpressure);
        self.set_core_sub(id, Some(core_kind));

        let (first_tx, first_rx) = oneshot::channel();
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
            self.inner.root.child_token(),
        );
        self.inner
            .streams
            .lock()
            .expect("streams poisoned")
            .insert(id, handle);

        match tokio::time::timeout(self.inner.ack_timeout, first_rx).await {
            Ok(Ok(Err(e))) => {
                self.drop_sub(id).await;
                Err(map_protocol_err(&e))
            }
            _ => Ok(id),
        }
    }

    fn set_core_sub(&self, id: SubscriptionId, kind: Option<SubscriptionKind>) {
        let mut core = self.inner.core.lock().expect("core poisoned");
        let mut subs: Vec<DesiredSubscription> = core
            .desired()
            .iter()
            .filter(|s| s.id != id)
            .cloned()
            .collect();
        if let Some(kind) = kind {
            subs.push(DesiredSubscription { id, kind });
        }
        core.apply(&SubscriptionSet::with_subscriptions(subs));
    }

    async fn drop_sub(&self, id: SubscriptionId) -> bool {
        let handle = self
            .inner
            .streams
            .lock()
            .expect("streams poisoned")
            .remove(&id);
        let had = handle.is_some();
        if let Some(h) = handle {
            h.stop(Duration::from_secs(2)).await;
        }
        self.set_core_sub(id, None);
        self.inner
            .backpressure
            .lock()
            .expect("backpressure poisoned")
            .unregister(id);
        had
    }

    fn wrap<M: Send + 'static>(sub: SubscriptionId, data: fn(M) -> MdEvent) -> MdWrap<M> {
        Arc::new(move |ev| {
            let event = match ev {
                StreamEvent::Msg(m) => data(m),
                StreamEvent::Connected => MdEvent::Connected,
                StreamEvent::Disconnected(r) => MdEvent::Disconnected(r),
                StreamEvent::Failed(r) => MdEvent::Failed(r),
            };
            MdFrame { sub, event }
        })
    }
}

fn history_interval(range: TimeRange) -> Interval {
    Interval {
        start_time: Some(to_timestamp(range.from)),
        end_time: Some(to_timestamp(range.to)),
    }
}

impl MarketDataConnector {
    async fn all_assets(&self) -> Result<Vec<pa::Asset>, Error> {
        let mut out = Vec::new();
        let mut cursor = 0i64;
        for _ in 0..MAX_ASSET_PAGES {
            let resp = self
                .inner
                .rpc
                .read("Assets", |api| async move {
                    api.assets()
                        .all_assets(pa::AllAssetsRequest {
                            cursor,
                            only_active: true,
                            only_disabled: false,
                        })
                        .await
                })
                .await?;
            out.extend(resp.assets);
            if resp.next_cursor == 0 || resp.next_cursor == cursor {
                return Ok(out);
            }
            cursor = resp.next_cursor;
        }
        Err(transport("asset directory has too many pages"))
    }

    async fn get_asset(&self, symbol: &str) -> Result<pa::GetAssetResponse, Error> {
        let symbol = symbol.to_string();
        self.inner
            .rpc
            .read("GetAsset", |api| {
                let req = pa::GetAssetRequest {
                    symbol: symbol.clone(),
                    // The directory is account-independent here; params would need an account.
                    account_id: String::new(),
                };
                async move { api.assets().get_asset(req).await }
            })
            .await
    }

    async fn schedule_of(&self, symbol: &str) -> Result<pa::ScheduleResponse, Error> {
        let symbol = symbol.to_string();
        self.inner
            .rpc
            .read("Schedule", |api| {
                let req = pa::ScheduleRequest {
                    symbol: symbol.clone(),
                };
                async move { api.assets().schedule(req).await }
            })
            .await
    }
}

#[async_trait]
impl OrderBookSource for MarketDataConnector {
    /// One `OrderBook` request. Answers `OrderBook::Snapshot`, cut to `depth` levels per side
    /// (at most 50, the cut of this connector).
    ///
    /// # Errors
    ///
    /// - [`Error::Config`]: the instrument has no `NativeId::SymbolMic`. Nothing is sent.
    /// - [`Error::RateLimited`]: the local quota is spent, or the broker answered `RESOURCE_EXHAUSTED`.
    /// - [`Error::Rejected`]: the broker refused (unknown symbol, no permission).
    /// - [`Error::Transport`]: connection failure or no answer within the request timeout.
    ///
    /// See the [type docs](MarketDataConnector) for an example.
    async fn order_book(&self, id: &InstrumentId, depth: u32) -> Result<OrderBook, Error> {
        let symbol = symbol_of(id)?;
        let resp = self
            .inner
            .rpc
            .read("OrderBook", |api| {
                let req = pm::OrderBookRequest {
                    symbol: symbol.clone(),
                };
                async move { api.market_data().order_book(req).await }
            })
            .await?;
        Ok(map_rest_order_book(
            id,
            &resp,
            depth.min(MAX_DEPTH),
            brokapi_runtime::now(),
        ))
    }

    /// Opens an `SubscribeOrderBook` stream. Envelopes carry `Event::OrderBook(OrderBook::Snapshot)`.
    ///
    /// `opts.depth` is the number of levels per side (default and maximum 50). Waits up to
    /// `subscribe_ack_timeout` for the stream to open: a refusal inside that time is returned. After it
    /// the call returns and the stream may still be opening (see [`MarketDataConnector::sub_status`]).
    ///
    /// # Errors
    ///
    /// - [`Error::Config`]: the instrument has no `NativeId::SymbolMic`.
    /// - [`Error::Rejected`]: the broker refused the stream (bad symbol, no permission), or this
    ///   connector already has the maximum number of streams (code `subscription_quota`).
    ///
    /// See the [type docs](MarketDataConnector) for an example.
    async fn subscribe_order_book(
        &self,
        id: &InstrumentId,
        opts: SubscribeOpts,
    ) -> Result<SubscriptionId, Error> {
        let symbol = symbol_of(id)?;
        let depth = opts.depth.unwrap_or(MAX_DEPTH).min(MAX_DEPTH);
        self.register_sub(
            id.clone(),
            SubscriptionKind::OrderBook {
                instrument: id.clone(),
                depth,
            },
            opts,
            "SubscribeOrderBook",
            |sub| {
                let api = self.inner.rpc.api.clone();
                let open: OpenFn<pm::SubscribeOrderBookResponse> = Arc::new(move || {
                    let api = api.clone();
                    let req = pm::SubscribeOrderBookRequest {
                        symbol: symbol.clone(),
                    };
                    Box::pin(async move {
                        api.market_data()
                            .subscribe_order_book(req)
                            .await
                            .map(|r| r.into_inner())
                    })
                });
                (open, Self::wrap(sub, MdEvent::OrderBook))
            },
        )
        .await
    }

    /// Cancels the stream (the gRPC unsubscribe) and forgets the subscription. Waits up to 2 seconds
    /// for the stream task to end.
    ///
    /// # Errors
    ///
    /// [`Error::Rejected`] with code `unknown_subscription` when `sub` is not a live subscription of
    /// this connector (a second `unsubscribe` of the same id ends here).
    async fn unsubscribe(&self, sub: SubscriptionId) -> Result<(), Error> {
        if self.drop_sub(sub).await {
            Ok(())
        } else {
            Err(Error::Rejected {
                code: "unknown_subscription".into(),
                message: format!("subscription {} is not active", sub.value()),
            })
        }
    }
}

#[async_trait]
impl MarketDataSource for MarketDataConnector {
    /// One `LatestTrades` request. Returns the newest `limit` trades, oldest first.
    ///
    /// A trade that carries no side is dropped, because the core `Side` is mandatory.
    ///
    /// # Errors
    ///
    /// The same as [`order_book`](OrderBookSource::order_book).
    async fn last_trades(&self, id: &InstrumentId, limit: u32) -> Result<Vec<MarketTrade>, Error> {
        let symbol = symbol_of(id)?;
        let resp = self
            .inner
            .rpc
            .read("LatestTrades", |api| {
                let req = pm::LatestTradesRequest {
                    symbol: symbol.clone(),
                };
                async move { api.market_data().latest_trades(req).await }
            })
            .await?;
        let now = brokapi_runtime::now();
        let mut trades: Vec<MarketTrade> = resp
            .trades
            .iter()
            .filter_map(|t| map_trade(id, t, now))
            .collect();
        // Newest `limit`, oldest first.
        let skip = trades.len().saturating_sub(limit as usize);
        Ok(trades.split_off(skip))
    }

    /// Opens a `SubscribeLatestTrades` stream. Trades seen before are not repeated after a reconnect
    /// (the last 2048 ids are remembered). Errors as in `subscribe_order_book`.
    async fn subscribe_market_trades(
        &self,
        id: &InstrumentId,
        opts: SubscribeOpts,
    ) -> Result<SubscriptionId, Error> {
        let symbol = symbol_of(id)?;
        self.register_sub(
            id.clone(),
            SubscriptionKind::LastTrades {
                instrument: id.clone(),
            },
            opts,
            "SubscribeLatestTrades",
            |sub| {
                let api = self.inner.rpc.api.clone();
                let open: OpenFn<pm::SubscribeLatestTradesResponse> = Arc::new(move || {
                    let api = api.clone();
                    let req = pm::SubscribeLatestTradesRequest {
                        symbol: symbol.clone(),
                    };
                    Box::pin(async move {
                        api.market_data()
                            .subscribe_latest_trades(req)
                            .await
                            .map(|r| r.into_inner())
                    })
                });
                (open, Self::wrap(sub, MdEvent::Trades))
            },
        )
        .await
    }

    /// Opens a `SubscribeQuote` stream for one instrument. A side with price 0 is reported as missing.
    /// An in-band error message from the broker shows as [`SubStatus::Erroring`]. Errors as in
    /// `subscribe_order_book`.
    async fn subscribe_quotes(
        &self,
        id: &InstrumentId,
        opts: SubscribeOpts,
    ) -> Result<SubscriptionId, Error> {
        let symbol = symbol_of(id)?;
        self.register_sub(
            id.clone(),
            SubscriptionKind::Quotes {
                instrument: id.clone(),
            },
            opts,
            "SubscribeQuote",
            |sub| {
                let api = self.inner.rpc.api.clone();
                let open: OpenFn<pm::SubscribeQuoteResponse> = Arc::new(move || {
                    let api = api.clone();
                    let req = pm::SubscribeQuoteRequest {
                        symbols: vec![symbol.clone()],
                    };
                    Box::pin(async move {
                        api.market_data()
                            .subscribe_quote(req)
                            .await
                            .map(|r| r.into_inner())
                    })
                });
                (open, Self::wrap(sub, MdEvent::Quotes))
            },
        )
        .await
    }

    /// One `Bars` request.
    ///
    /// M10 is not on the wire: the connector asks for M5 and merges pairs. The start of the range moves
    /// down to the 10 minute boundary so the first candle keeps both halves. A lone M5 candle (one half
    /// had no trades) is a legitimate M10 candle and is kept.
    ///
    /// # Errors
    ///
    /// As [`order_book`](OrderBookSource::order_book), plus [`Error::Unsupported`] for a timeframe
    /// Finam has no bars for.
    async fn candles(
        &self,
        id: &InstrumentId,
        tf: Timeframe,
        range: TimeRange,
    ) -> Result<Vec<Candle>, Error> {
        let symbol = symbol_of(id)?;
        let timeframe = wire_timeframe(tf)? as i32;
        // M10 is built from M5: the range starts at the 10-minute boundary, so the first candle
        // does not lose its first half.
        let range = if tf == Timeframe::M10 {
            TimeRange::new(floor_m10(range.from), range.to)
        } else {
            range
        };
        let resp = self
            .inner
            .rpc
            .read("Bars", |api| {
                let req = pm::BarsRequest {
                    symbol: symbol.clone(),
                    timeframe,
                    interval: Some(history_interval(range)),
                };
                async move { api.market_data().bars(req).await }
            })
            .await?;
        let now = brokapi_runtime::now();
        if tf == Timeframe::M10 {
            let m5: Vec<Candle> = resp
                .bars
                .iter()
                .map(|b| map_bar(id, Timeframe::M5, b, now))
                .collect();
            return Ok(merge_m5(&m5));
        }
        Ok(resp.bars.iter().map(|b| map_bar(id, tf, b, now)).collect())
    }

    /// 500 bars of the timeframe. The Finam documentation of `Bars` states no limit, so this is a
    /// cautious guess that costs only extra requests when it is too small. Not checked against the
    /// live API.
    fn candles_max_span(&self, tf: Timeframe) -> Option<Duration> {
        Some(Duration::from_secs(500 * tf.seconds()))
    }

    /// Opens a `SubscribeBars` stream. Every message of a forming bar gives an updated candle, not only
    /// the closed one. For M10 the stream is M5 and the connector builds the 10 minute candle.
    /// Errors as in `candles` and `subscribe_order_book`.
    async fn subscribe_candles(
        &self,
        id: &InstrumentId,
        tf: Timeframe,
        opts: SubscribeOpts,
    ) -> Result<SubscriptionId, Error> {
        let symbol = symbol_of(id)?;
        // M10: the wire stream is M5, the core builds the 10-minute candle.
        let timeframe = wire_timeframe(tf)? as i32;
        self.register_sub(
            id.clone(),
            SubscriptionKind::LastCandle {
                instrument: id.clone(),
                timeframe: tf,
            },
            opts,
            "SubscribeBars",
            |sub| {
                let api = self.inner.rpc.api.clone();
                let open: OpenFn<pm::SubscribeBarsResponse> = Arc::new(move || {
                    let api = api.clone();
                    let req = pm::SubscribeBarsRequest {
                        symbol: symbol.clone(),
                        timeframe,
                    };
                    Box::pin(async move {
                        api.market_data()
                            .subscribe_bars(req)
                            .await
                            .map(|r| r.into_inner())
                    })
                });
                (open, Self::wrap(sub, MdEvent::Bars))
            },
        )
        .await
    }
}

#[async_trait]
impl InstrumentSource for MarketDataConnector {
    /// `ByTicker` with an exchange is one `GetAsset`; everything else scans the (paged) directory
    /// of active instruments, so prefer the exchange form. The library keeps no cache.
    ///
    /// The directory list carries no currency: `currency` is empty for the answers of a scan. Use `get`
    /// for it. `ByKind` matches the Finam `type` string by substring, which is not verified against
    /// the live API.
    ///
    /// # Errors
    ///
    /// [`Error::Rejected`] (a symbol the broker does not know is `not_found`), [`Error::RateLimited`],
    /// [`Error::Transport`] (also when the directory has more pages than the safety bound of 200).
    async fn find(&self, q: InstrumentQuery) -> Result<Vec<Instrument>, Error> {
        match q {
            InstrumentQuery::ByTicker {
                ticker,
                exchange: Some(mic),
            } => {
                let symbol = format!("{ticker}@{mic}");
                Ok(vec![map_get_asset(
                    &symbol,
                    &self.get_asset(&symbol).await?,
                )])
            }
            InstrumentQuery::ByTicker {
                ticker,
                exchange: None,
            } => Ok(self
                .all_assets()
                .await?
                .iter()
                .filter(|a| a.ticker.eq_ignore_ascii_case(&ticker))
                .map(map_asset)
                .collect()),
            InstrumentQuery::ByIsin(isin) => Ok(self
                .all_assets()
                .await?
                .iter()
                .filter(|a| a.isin.eq_ignore_ascii_case(&isin))
                .map(map_asset)
                .collect()),
            InstrumentQuery::ByKind(kind) => Ok(self
                .all_assets()
                .await?
                .iter()
                .filter(|a| kind_from_type(&a.r#type) == kind)
                .map(map_asset)
                .collect()),
        }
    }

    /// One `GetAsset`: name, ISIN, kind and currency of the instrument.
    ///
    /// # Errors
    ///
    /// [`Error::Config`] for a native id that is not `SymbolMic`. [`Error::Rejected`] when the broker
    /// does not know the symbol. [`Error::RateLimited`] and [`Error::Transport`] as for any read.
    async fn get(&self, id: &InstrumentId) -> Result<Instrument, Error> {
        let symbol = symbol_of(id)?;
        Ok(map_get_asset(&symbol, &self.get_asset(&symbol).await?))
    }

    /// Trading parameters from `GetAsset` (price step, lot size and the like). The library does not
    /// round your orders with them. Errors as in `get`.
    async fn trading_params(&self, id: &InstrumentId) -> Result<TradingParams, Error> {
        let symbol = symbol_of(id)?;
        Ok(map_trading_params(&symbol, &self.get_asset(&symbol).await?))
    }

    /// Derived from the instrument's schedule: Finam has no direct status call.
    ///
    /// Inside a session that is not of the closed type the status is open, otherwise closed. An empty
    /// schedule gives `Unknown`. Errors as in `get`.
    async fn trading_status(&self, id: &InstrumentId) -> Result<TradingStatus, Error> {
        let symbol = symbol_of(id)?;
        let sched = self.schedule_of(&symbol).await?;
        let now = now_nanos();
        let (availability, reason) = availability_at(&sched, now);
        Ok(TradingStatus {
            instrument: id.clone(),
            availability,
            reason,
            ts: Some(now),
        })
    }

    /// Finam schedules are per instrument, so `exchange` must be a symbol (`SBER@MISX`).
    ///
    /// Only sessions that overlap `range` are returned, closed sessions are left out.
    ///
    /// # Errors
    ///
    /// [`Error::Config`] when `exchange` has no `@` (nothing is sent). Otherwise as in `get`.
    async fn schedule(&self, exchange: &str, range: TimeRange) -> Result<Schedule, Error> {
        if !exchange.contains('@') {
            return Err(Error::Config(ConfigError::Message(
                "Finam schedule is per instrument: pass SYMBOL@MIC as `exchange`".into(),
            )));
        }
        let resp = self.schedule_of(exchange).await?;
        Ok(map_schedule(
            exchange,
            &resp,
            range.from,
            range.to,
            brokapi_runtime::now(),
        ))
    }
}
