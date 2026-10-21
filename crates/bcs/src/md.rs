//! The market data connector: order books, trades, quotes, candles and the instrument directory.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use brokapi_bcs_protocol::http::client::BcsHttpClient;
use brokapi_bcs_protocol::http::models::{
    CandlesRequest, DailyScheduleRequest, InstrumentsByIsinsRequest, InstrumentsByTickersRequest,
    InstrumentsByTypeRequest, LastTradesRequest, OrderBookRequest, TradingStatusRequest,
};
use brokapi_bcs_protocol::ws::client::BcsWsClient;
use brokapi_bcs_protocol::ws::models::MarketDataSubscribe;
use brokapi_bcs_protocol::ws::session::WsSession;
use brokapi_core::{
    Candle, Envelope, Error, Instrument, InstrumentId, InstrumentQuery, InstrumentSource,
    MarketDataCaps, MarketDataSource, MarketTrade, OrderBook, OrderBookSource, Schedule,
    ShutdownMode, ShutdownReport, StreamingCaps, SubscribeOpts, SubscriptionId, TimeRange,
    Timeframe, TradingParams, TradingStatus,
};
use brokapi_runtime::{BrokerLimiter, Emitter};
use brokapi_sansio::{BrokerCore, DesiredSubscription, SubscriptionKind, SubscriptionSet};
use tokio::sync::{Mutex, RwLock, mpsc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::config::MarketDataConfig;
use crate::core::MdCore;
use crate::error::{map_config_err, map_err};
use crate::map::instrument::{map_instrument_info, map_trading_params, map_trading_status};
use crate::map::market::{map_rest_candles, map_rest_last_trade, map_rest_order_book};
use crate::map::schedule::map_daily_schedule;
use crate::pump::{drain_backlog, run_md_pump};
use crate::raw::BcsRaw;
use crate::subs::{MdSubKind, MdSubscription, SubIdGen};
use crate::util::{class_code, nanos_to_datetime, require_ticker_class, ticker, timeframe_label};

const MAX_DEPTH: u32 = 20;

/// `size` of `GET /instruments/by-type` (the docs allow 1..100, default 50).
const KIND_PAGE_SIZE: u32 = 100;
/// Pages read for one kind query at most. More is an error, never a silent cut.
const MAX_KIND_PAGES: u32 = 200;

struct MdShared {
    http: BcsHttpClient,
    ws: WsSession,
    limiter: Arc<BrokerLimiter>,
    core: Arc<Mutex<MdCore>>,
    emitter: Arc<Emitter>,
    subs: RwLock<HashMap<SubscriptionId, MdSubscription>>,
    sub_ids: SubIdGen,
    cancel: CancellationToken,
}

/// BCS market data connector: order books, trades, quotes, candles and the instrument directory.
///
/// One connector per broker is enough: it is not tied to an account. It implements
/// [`OrderBookSource`], [`MarketDataSource`] and [`InstrumentSource`]. Commands return their
/// answer directly. Subscriptions deliver [`Envelope`]s into the channel given to
/// [`start`](Self::start).
///
/// # Examples
///
/// ```
/// use brokapi_bcs::limits::default_limits;
/// use brokapi_bcs::{MarketDataConfig, MarketDataConnector};
/// use brokapi_core::{InstrumentId, InstrumentKey, NativeId, OrderBook, OrderBookSource};
/// use brokapi_runtime::BrokerLimiter;
/// use tokio::sync::mpsc;
/// # use brokapi_bcs_protocol::config::BcsConfig;
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
/// # let cfg = BcsConfig::for_mock("mock-refresh-token", &server.http_base);
/// let (events_tx, _events) = mpsc::channel(256);
/// let limiter = BrokerLimiter::new(&default_limits());
/// let md = MarketDataConnector::start(MarketDataConfig::new(cfg, "bcs-md"), limiter, events_tx).await?;
///
/// let sber = InstrumentId::new(
///     InstrumentKey::new("SBER", "TQBR"),
///     NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() },
/// );
/// let book = md.order_book(&sber, 10).await?;
/// let OrderBook::Snapshot { bids, asks, .. } = book else { panic!("REST answers a snapshot") };
/// assert!(!bids.is_empty() && !asks.is_empty());
/// # Ok(())
/// # }
/// ```
pub struct MarketDataConnector {
    inner: Arc<MdShared>,
    pump: JoinHandle<()>,
}

impl MarketDataConnector {
    /// Starts the connector: gets the first access token and opens the market data WebSocket.
    ///
    /// The refresh token in `cfg` is exchanged for an access token before anything else, so a wrong
    /// token fails here and not on the first request. This step does not spend rate limit quota.
    /// The WebSocket session is started in the background. It reconnects on its own and restores the
    /// subscriptions after a reconnect. A reconnect voids the order books: the stream tells the bot
    /// with `Event::Resync` before the new snapshot.
    ///
    /// `limiter` is shared by value of `Arc`: give the same limiter to every connector of one BCS
    /// account, because the limits belong to the token. `out` receives the envelopes of all
    /// subscriptions. Its capacity is the bot's choice. The bot must read it, because a slow reader
    /// triggers the backpressure policy of each subscription.
    ///
    /// # Errors
    ///
    /// - [`Error::Config`] when the HTTP or WebSocket client cannot be built from `cfg`.
    /// - [`Error::Transport`] when the token refresh fails: no connection, a timeout, or the auth
    ///   server refuses the refresh token. A refused or expired refresh token is a permanent failure,
    ///   it will not heal on retry (exact auth server answers: not verified against the live API).
    /// - [`Error::RateLimited`] when the auth server answers HTTP 429.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_bcs::limits::default_limits;
    /// use brokapi_bcs::{MarketDataConfig, MarketDataConnector};
    /// use brokapi_core::ShutdownMode;
    /// use brokapi_runtime::BrokerLimiter;
    /// use tokio::sync::mpsc;
    /// # use brokapi_bcs_protocol::config::BcsConfig;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = BcsConfig::for_mock("mock-refresh-token", &server.http_base);
    /// let (events_tx, _events) = mpsc::channel(256);
    /// let limiter = BrokerLimiter::new(&default_limits());
    /// let md = MarketDataConnector::start(MarketDataConfig::new(cfg, "bcs-md"), limiter, events_tx).await?;
    /// assert_eq!(md.caps().max_order_book_depth, 20);
    /// md.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// A broker that cannot be reached is a [`Error::Transport`], and nothing is left running:
    ///
    /// ```
    /// use brokapi_bcs::limits::default_limits;
    /// use brokapi_bcs::{MarketDataConfig, MarketDataConnector};
    /// use brokapi_core::Error;
    /// use brokapi_runtime::BrokerLimiter;
    /// use tokio::sync::mpsc;
    /// use brokapi_bcs_protocol::config::BcsConfig;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// // Port 1 of the local host: nothing listens there.
    /// let cfg = BcsConfig::for_mock("any-token", "http://127.0.0.1:1");
    /// let (events_tx, _events) = mpsc::channel(16);
    /// let limiter = BrokerLimiter::new(&default_limits());
    /// let started = MarketDataConnector::start(MarketDataConfig::new(cfg, "bcs-md"), limiter, events_tx).await;
    /// assert!(matches!(started, Err(Error::Transport(_))));
    /// # }
    /// ```
    pub async fn start(
        cfg: MarketDataConfig,
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
        let ws = ws_client.connect_market_data(event_tx);
        let cancel = CancellationToken::new();
        let core = Arc::new(Mutex::new(MdCore::new()));
        let emitter = Emitter::new(Arc::clone(&cfg.source), out);

        let inner = Arc::new(MdShared {
            http: ws_client.http_client().clone(),
            ws,
            limiter,
            core: Arc::clone(&core),
            emitter: Arc::clone(&emitter),
            subs: RwLock::new(HashMap::new()),
            sub_ids: SubIdGen::new(0),
            cancel: cancel.clone(),
        });

        let pump = tokio::spawn(run_md_pump(core, emitter, event_rx, cancel));

        Ok(Self { inner, pump })
    }

    /// Returns a handle for any other method of the BCS API, with this connector's token, endpoint and
    /// rate limiter.
    ///
    /// The handle is cheap to clone and stays usable while the connector lives. Every call through it
    /// spends quota by the operation name you give. See [`BcsRaw`] for the rules of a call.
    ///
    /// # Examples
    ///
    /// A method the shared traits do not wrap, here the quotes snapshot:
    ///
    /// ```
    /// use brokapi_bcs_protocol::http::models::{QuoteInstrument, QuotesRequest};
    /// use brokapi_bcs::limits::default_limits;
    /// use brokapi_bcs::{MarketDataConfig, MarketDataConnector};
    /// use brokapi_core::ShutdownMode;
    /// use brokapi_runtime::BrokerLimiter;
    /// use tokio::sync::mpsc;
    /// # use brokapi_bcs_protocol::config::BcsConfig;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = BcsConfig::for_mock("mock-refresh-token", &server.http_base);
    /// # let (events_tx, _events) = mpsc::channel(256);
    /// # let limiter = BrokerLimiter::new(&default_limits());
    /// let md = MarketDataConnector::start(MarketDataConfig::new(cfg, "bcs-md"), limiter, events_tx).await?;
    ///
    /// let quotes = md
    ///     .raw()
    ///     .call("GetQuotes", |http| async move {
    ///         http.get_quotes(&QuotesRequest { instruments: vec![QuoteInstrument::new("SBER", "TQBR")] }).await
    ///     })
    ///     .await?;
    /// assert!(!quotes.records.is_empty());
    /// md.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    pub fn raw(&self) -> BcsRaw {
        BcsRaw::new(self.inner.http.clone(), Arc::clone(&self.inner.limiter))
    }

    /// Describes what this connector can do, without any request.
    ///
    /// The answer is fixed for BCS: order books up to 20 levels, always as snapshots
    /// (`incremental_order_book` is `false`), and streams for order books, market trades, quotes and
    /// candles. A deeper `depth` in a request is cut to the maximum, it is not an error.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_bcs::limits::default_limits;
    /// use brokapi_bcs::{MarketDataConfig, MarketDataConnector};
    /// use brokapi_core::ShutdownMode;
    /// use brokapi_runtime::BrokerLimiter;
    /// use tokio::sync::mpsc;
    /// # use brokapi_bcs_protocol::config::BcsConfig;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = BcsConfig::for_mock("mock-refresh-token", &server.http_base);
    /// let (events_tx, _events) = mpsc::channel(256);
    /// let limiter = BrokerLimiter::new(&default_limits());
    /// let md = MarketDataConnector::start(MarketDataConfig::new(cfg, "bcs-md"), limiter, events_tx).await?;
    /// let caps = md.caps();
    /// assert_eq!(caps.max_order_book_depth, 20);
    /// assert!(!caps.incremental_order_book);
    /// assert!(caps.streaming.order_book && caps.streaming.candles);
    /// # md.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    pub fn caps(&self) -> MarketDataCaps {
        MarketDataCaps::new(MAX_DEPTH, false, StreamingCaps::new(true, true, true, true))
    }

    /// Stops the connector and reports what was cleaned up. Consumes the connector.
    ///
    /// - [`ShutdownMode::Immediate`]: no unsubscribe messages are sent, the WebSocket just goes away.
    /// - [`ShutdownMode::Graceful`]: every subscription is unsubscribed on the WebSocket first. The
    ///   counts are in the report (`unsubscribed`, `unsubscribe_failed`). Then the events that still
    ///   wait in the subscription buffers get time to reach the bot's channel, until `deadline`.
    ///
    /// After the call the event pump has stopped, and no more envelopes arrive. Market data holds no
    /// orders, so `inflight_abandoned` is always empty here.
    ///
    /// # Examples
    ///
    /// A graceful shutdown with one live subscription:
    ///
    /// ```
    /// use std::time::Duration;
    /// use brokapi_bcs::limits::default_limits;
    /// use brokapi_bcs::{MarketDataConfig, MarketDataConnector};
    /// use brokapi_core::{Backpressure, InstrumentId, InstrumentKey, NativeId, OrderBookSource, ShutdownMode, SubscribeOpts};
    /// use brokapi_runtime::BrokerLimiter;
    /// use tokio::sync::mpsc;
    /// # use brokapi_bcs_protocol::config::BcsConfig;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = BcsConfig::for_mock("mock-refresh-token", &server.http_base);
    /// # let (events_tx, _events) = mpsc::channel(256);
    /// # let limiter = BrokerLimiter::new(&default_limits());
    /// let md = MarketDataConnector::start(MarketDataConfig::new(cfg, "bcs-md"), limiter, events_tx).await?;
    /// let sber = InstrumentId::new(
    ///     InstrumentKey::new("SBER", "TQBR"),
    ///     NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() },
    /// );
    /// md.subscribe_order_book(&sber, SubscribeOpts { backpressure: Backpressure::Conflate, depth: Some(10) }).await?;
    ///
    /// let report = md.shutdown(ShutdownMode::Graceful { deadline: Duration::from_secs(1) }).await;
    /// assert_eq!(report.unsubscribed + report.unsubscribe_failed, 1);
    /// assert!(report.inflight_abandoned.is_empty());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn shutdown(self, mode: ShutdownMode) -> ShutdownReport {
        let mut unsubscribed = 0usize;
        let mut unsubscribe_failed = 0usize;

        if !matches!(mode, ShutdownMode::Immediate) {
            let subs: Vec<_> = self.inner.subs.read().await.values().cloned().collect();
            for sub in subs {
                match self.inner.ws.unsubscribe(sub.ws.clone()).await {
                    Ok(()) => unsubscribed += 1,
                    Err(_) => unsubscribe_failed += 1,
                }
            }
        }

        if let ShutdownMode::Graceful { deadline } = mode {
            drain_backlog(&self.inner.emitter, deadline).await;
        }
        self.inner.cancel.cancel();
        let _ = self.pump.await;

        ShutdownReport::new(unsubscribed, unsubscribe_failed, Vec::new())
    }

    async fn acquire(&self, method: &str) -> Result<(), Error> {
        self.inner.limiter.acquire(method, brokapi_runtime::now())
    }

    async fn register_md_sub(
        &self,
        instrument: InstrumentId,
        kind: MdSubKind,
        core_kind: SubscriptionKind,
        opts: SubscribeOpts,
        ws: MarketDataSubscribe,
    ) -> Result<SubscriptionId, Error> {
        let id = self.inner.sub_ids.next();
        self.inner
            .emitter
            .register(id, Some(instrument.clone()), opts.backpressure);
        // The core attributes incoming data to this subscription (Envelope::sub, backpressure).
        // It is told before the WS subscribe, so the success ack cannot overtake it.
        self.set_core_sub(id, Some(core_kind)).await;
        if let Err(e) = self.inner.ws.subscribe(ws.clone()).await {
            self.set_core_sub(id, None).await;
            self.inner.emitter.unregister(id);
            return Err(map_err(e));
        }
        self.inner.subs.write().await.insert(
            id,
            MdSubscription {
                id,
                instrument,
                kind,
                opts,
                ws,
            },
        );
        Ok(id)
    }

    /// Adds (`Some`) or removes (`None`) a subscription in the core's desired set.
    /// The core's own subscribe frames are dropped: `WsSession` sends them and restores
    /// them after a reconnect.
    async fn set_core_sub(&self, id: SubscriptionId, kind: Option<SubscriptionKind>) {
        let mut core = self.inner.core.lock().await;
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
        while core.poll_transmit().is_some() {}
    }
}

#[async_trait]
impl brokapi_core::Connector for MarketDataConnector {
    type Caps = brokapi_core::MarketDataCaps;

    fn caps(&self) -> brokapi_core::MarketDataCaps {
        MarketDataConnector::caps(self)
    }

    async fn shutdown(self, mode: brokapi_core::ShutdownMode) -> brokapi_core::ShutdownReport {
        MarketDataConnector::shutdown(self, mode).await
    }
}

#[async_trait]
impl OrderBookSource for MarketDataConnector {
    async fn order_book(&self, id: &InstrumentId, depth: u32) -> Result<OrderBook, Error> {
        self.acquire("GetOrderBook").await?;
        let depth = depth.min(MAX_DEPTH);
        let req = OrderBookRequest {
            ticker: ticker(id)?,
            class_code: class_code(id)?,
            depth,
        };
        let resp = self
            .inner
            .http
            .get_order_book(&req)
            .await
            .map_err(map_err)?;
        match map_rest_order_book(&resp, brokapi_runtime::now()) {
            brokapi_core::Event::OrderBook(book) => Ok(book),
            _ => Err(Error::Transport(brokapi_core::TransportError::Message(
                "order book mapping failed".into(),
            ))),
        }
    }

    async fn subscribe_order_book(
        &self,
        id: &InstrumentId,
        opts: SubscribeOpts,
    ) -> Result<SubscriptionId, Error> {
        let depth = opts.depth.unwrap_or(MAX_DEPTH).min(MAX_DEPTH);
        let key = require_ticker_class(id)?;
        let ws = MarketDataSubscribe::order_book(vec![key], depth);
        self.register_md_sub(
            id.clone(),
            MdSubKind::OrderBook,
            SubscriptionKind::OrderBook {
                instrument: id.clone(),
                depth,
            },
            opts,
            ws,
        )
        .await
    }

    async fn unsubscribe(&self, sub: SubscriptionId) -> Result<(), Error> {
        let record = self
            .inner
            .subs
            .write()
            .await
            .remove(&sub)
            .ok_or_else(|| Error::Rejected {
                code: "unknown_subscription".into(),
                message: format!("subscription {} is not active", sub.value()),
            })?;
        self.set_core_sub(sub, None).await;
        self.inner.emitter.unregister(sub);
        self.inner.ws.unsubscribe(record.ws).await.map_err(map_err)
    }
}

#[async_trait]
impl MarketDataSource for MarketDataConnector {
    /// `POST /last-trades`: the tape of the current session. The API has no limit, so the newest
    /// `limit` trades are kept here (oldest first). A record without a known side is dropped.
    async fn last_trades(&self, id: &InstrumentId, limit: u32) -> Result<Vec<MarketTrade>, Error> {
        self.acquire("GetLastTrades").await?;
        let req = LastTradesRequest {
            ticker: ticker(id)?,
            class_code: class_code(id)?,
            ..Default::default()
        };
        let mut resp = self
            .inner
            .http
            .get_last_trades(&req)
            .await
            .map_err(map_err)?;
        // Oldest first; records without a time keep their place (stable sort on a None key).
        resp.sort_by_key(|t| crate::map::parse_broker_ts(t.date_time.as_deref()));
        let now = brokapi_runtime::now();
        let mut trades: Vec<MarketTrade> = resp
            .iter()
            .filter_map(|t| map_rest_last_trade(t, id, now))
            .collect();
        if trades.len() > limit as usize {
            trades.drain(..trades.len() - limit as usize);
        }
        Ok(trades)
    }

    async fn subscribe_market_trades(
        &self,
        id: &InstrumentId,
        opts: SubscribeOpts,
    ) -> Result<SubscriptionId, Error> {
        let key = require_ticker_class(id)?;
        let ws = MarketDataSubscribe::last_trades(vec![key]);
        self.register_md_sub(
            id.clone(),
            MdSubKind::MarketTrades,
            SubscriptionKind::LastTrades {
                instrument: id.clone(),
            },
            opts,
            ws,
        )
        .await
    }

    async fn subscribe_quotes(
        &self,
        id: &InstrumentId,
        opts: SubscribeOpts,
    ) -> Result<SubscriptionId, Error> {
        let key = require_ticker_class(id)?;
        let ws = MarketDataSubscribe::quotes(vec![key]);
        self.register_md_sub(
            id.clone(),
            MdSubKind::Quotes,
            SubscriptionKind::Quotes {
                instrument: id.clone(),
            },
            opts,
            ws,
        )
        .await
    }

    async fn candles(
        &self,
        id: &InstrumentId,
        tf: Timeframe,
        range: TimeRange,
    ) -> Result<Vec<Candle>, Error> {
        let label = timeframe_label(tf).ok_or(Error::Unsupported {
            capability: "candles_m10".into(),
        })?;
        self.acquire("GetCandles").await?;
        let req = CandlesRequest {
            ticker: ticker(id)?,
            class_code: class_code(id)?,
            start_date: nanos_to_datetime(range.from),
            end_date: nanos_to_datetime(range.to),
            time_frame: label.to_string(),
        };
        let resp = self.inner.http.get_candles(&req).await.map_err(map_err)?;
        Ok(map_rest_candles(&resp, tf, brokapi_runtime::now()))
    }

    /// One request returns at most 1440 bars (BCS documentation of `candles-chart`), so the span is
    /// 1440 bars of the timeframe. Not checked against the live API.
    fn candles_max_span(&self, tf: Timeframe) -> Option<Duration> {
        Some(Duration::from_secs(1440 * tf.seconds()))
    }

    async fn subscribe_candles(
        &self,
        id: &InstrumentId,
        tf: Timeframe,
        opts: SubscribeOpts,
    ) -> Result<SubscriptionId, Error> {
        let label = timeframe_label(tf).ok_or(Error::Unsupported {
            capability: "candles_m10".into(),
        })?;
        let key = require_ticker_class(id)?;
        let ws = MarketDataSubscribe::last_candle(vec![key], label);
        self.register_md_sub(
            id.clone(),
            MdSubKind::Candles,
            SubscriptionKind::LastCandle {
                instrument: id.clone(),
                timeframe: tf,
            },
            opts,
            ws,
        )
        .await
    }
}

#[async_trait]
impl InstrumentSource for MarketDataConnector {
    async fn find(&self, q: InstrumentQuery) -> Result<Vec<Instrument>, Error> {
        let route = match &q {
            InstrumentQuery::ByTicker { .. } => "GetInstrumentsByTickers",
            InstrumentQuery::ByIsin(_) => "GetInstrumentsByIsins",
            InstrumentQuery::ByKind(_) => "GetInstrumentsByType",
        };
        self.acquire(route).await?;
        let infos = match q {
            InstrumentQuery::ByTicker {
                ticker,
                exchange: _,
            } => self
                .inner
                .http
                .get_instruments_by_tickers(
                    &InstrumentsByTickersRequest {
                        tickers: vec![ticker.clone()],
                    },
                    None,
                    None,
                )
                .await
                .map_err(map_err)?,
            InstrumentQuery::ByIsin(isin) => self
                .inner
                .http
                .get_instruments_by_isins(
                    &InstrumentsByIsinsRequest { isins: vec![isin] },
                    None,
                    None,
                )
                .await
                .map_err(map_err)?,
            InstrumentQuery::ByKind(kind) => {
                // The docs: a page shorter than `size` ends the list, a full one means "ask for the
                // next". Every page spends quota like a request of its own.
                let ty = instrument_type(kind)?.to_string();
                let mut all = Vec::new();
                for page in 0..MAX_KIND_PAGES {
                    if page > 0 {
                        self.acquire(route).await?;
                    }
                    let batch = self
                        .inner
                        .http
                        .get_instruments_by_type(&InstrumentsByTypeRequest {
                            instrument_type: ty.clone(),
                            base_asset_ticker: None,
                            size: Some(KIND_PAGE_SIZE),
                            page: Some(page),
                        })
                        .await
                        .map_err(map_err)?;
                    let full = batch.len() >= KIND_PAGE_SIZE as usize;
                    all.extend(batch);
                    if !full {
                        break;
                    }
                    if page + 1 == MAX_KIND_PAGES {
                        return Err(Error::Transport(brokapi_core::TransportError::Message(
                            format!(
                                "the list of instruments of this kind is longer than {} pages: not returned in part",
                                MAX_KIND_PAGES
                            ),
                        )));
                    }
                }
                all
            }
        };
        Ok(infos
            .iter()
            .map(|info| {
                let class = if info.primary_board.is_empty() {
                    info.boards
                        .first()
                        .map(|b| b.class_code.clone())
                        .unwrap_or_default()
                } else {
                    info.primary_board.clone()
                };
                map_instrument_info(info, &class)
            })
            .collect())
    }

    async fn get(&self, id: &InstrumentId) -> Result<Instrument, Error> {
        let t = ticker(id)?;
        let list = self
            .find(InstrumentQuery::ByTicker {
                ticker: t,
                exchange: None,
            })
            .await?;
        list.into_iter().next().ok_or_else(|| {
            Error::Transport(brokapi_core::TransportError::Message(
                "instrument not found".into(),
            ))
        })
    }

    async fn trading_params(&self, id: &InstrumentId) -> Result<TradingParams, Error> {
        self.acquire("GetInstrumentsByTickers").await?;
        let t = ticker(id)?;
        let cc = class_code(id)?;
        let infos = self
            .inner
            .http
            .get_instruments_by_tickers(
                &InstrumentsByTickersRequest {
                    tickers: vec![t.clone()],
                },
                None,
                None,
            )
            .await
            .map_err(map_err)?;
        let info = infos.first().ok_or_else(|| {
            Error::Transport(brokapi_core::TransportError::Message(
                "instrument not found".into(),
            ))
        })?;
        let snap = info.trading_params_for_board(&cc).ok_or_else(|| {
            Error::Transport(brokapi_core::TransportError::Message(
                "trading params unavailable".into(),
            ))
        })?;
        Ok(map_trading_params(&snap))
    }

    async fn trading_status(&self, id: &InstrumentId) -> Result<TradingStatus, Error> {
        self.acquire("GetTradingStatus").await?;
        let req = TradingStatusRequest {
            class_code: class_code(id)?,
        };
        let resp = self
            .inner
            .http
            .get_trading_status(&req)
            .await
            .map_err(map_err)?;
        Ok(map_trading_status(&ticker(id)?, &class_code(id)?, &resp))
    }

    /// `GET /trading-schedule/daily-schedule` needs the instrument (`ticker` and `classCode` are both
    /// required), so BCS schedules are per instrument, like Finam's: `exchange` is `"TICKER@CLASSCODE"`
    /// (`"SBER@TQBR"`). Anything else is a `Config` error. The answer is for one day, today: the
    /// intervals are put on the day of `range.from` (an assumption, see `map/schedule.rs`).
    async fn schedule(&self, exchange: &str, range: TimeRange) -> Result<Schedule, Error> {
        let Some((t, c)) = exchange
            .split_once('@')
            .filter(|(t, c)| !t.is_empty() && !c.is_empty())
        else {
            return Err(Error::Config(brokapi_core::ConfigError::Message(
                "BCS schedule is per instrument: pass \"TICKER@CLASSCODE\"".into(),
            )));
        };
        self.acquire("GetDailySchedule").await?;
        let req = DailyScheduleRequest {
            class_code: c.to_string(),
            ticker: t.to_string(),
        };
        let resp = self
            .inner
            .http
            .get_daily_schedule(&req)
            .await
            .map_err(map_err)?;
        let day = nanos_to_datetime(range.from).date_naive();
        Ok(map_daily_schedule(
            exchange,
            day,
            &resp,
            brokapi_runtime::now(),
        ))
    }
}

/// `type` of `GET /instruments/by-type`: `STOCK`, `BONDS`, `FUTURES`, `ETF`, `MUTUAL_FUNDS`, `CURRENCY`
/// (docs enum, checked 2026-10-19). Options need `baseAssetTicker`, which a kind query cannot carry, and
/// `Other` has no BCS type.
fn instrument_type(kind: brokapi_core::InstrumentKind) -> Result<&'static str, Error> {
    use brokapi_core::InstrumentKind as K;
    match kind {
        K::Stock => Ok("STOCK"),
        K::Bond => Ok("BONDS"),
        K::Future => Ok("FUTURES"),
        K::Etf => Ok("ETF"),
        K::MutualFund => Ok("MUTUAL_FUNDS"),
        K::Currency => Ok("CURRENCY"),
        K::Option => Err(Error::Unsupported {
            capability: "find_options_by_kind".into(),
        }),
        K::Other => Err(Error::Unsupported {
            capability: "find_other_by_kind".into(),
        }),
    }
}
