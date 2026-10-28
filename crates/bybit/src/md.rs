//! The market data connector: order books, trades, quotes, candles and the directory of instruments.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use brokapi_bybit_protocol::http::client::BybitHttpClient;
use brokapi_bybit_protocol::ws::session::{WsSession, WsStream};
use brokapi_core::{
    Candle, Envelope, Error, Instrument, InstrumentId, InstrumentKind, InstrumentQuery,
    InstrumentSource, MarketDataCaps, MarketDataSource, MarketTrade, OrderBook, OrderBookSource,
    Schedule, ShutdownMode, ShutdownReport, StreamingCaps, SubscribeOpts, SubscriptionId,
    TimeRange, Timeframe, TradingParams, TradingStatus,
};
use brokapi_runtime::{BrokerLimiter, Emitter};
use brokapi_sansio::SubscriptionKind;
use tokio::sync::{Mutex, RwLock, mpsc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::config::MarketDataConfig;
use crate::core::MdCore;
use crate::error::{map_config_err, map_err};
use crate::map::instrument::{instrument, kind_of, trading_params, trading_status};
use crate::map::market::{candle, market_trade, order_book};
use crate::map::unix_now_nanos;
use crate::pump::{drain_backlog, run_pump};
use crate::raw::BybitRaw;
use crate::subs::{MdSubKind, MdSubscription, ReqIdGen, SubIdGen, wait_ack};
use crate::util::{interval_label, nanos_to_ms, symbol_category};

/// The deepest book the exchange gives for linear: 1000 levels a side.
const MAX_DEPTH: u32 = 1000;
/// The depths of the order book stream of linear.
const STREAM_DEPTHS: [u32; 4] = [1, 50, 200, 1000];
/// The default depth of a stream when the caller names none.
const DEFAULT_DEPTH: u32 = 50;
/// How many bars one request of the history answers at most.
const KLINE_LIMIT: u32 = 1000;
/// How many instruments a page of the directory holds at most.
const PAGE_SIZE: u32 = 1000;
const MAX_PAGES: u32 = 50;

struct MdShared {
    http: BybitHttpClient,
    ws: WsSession,
    limiter: Arc<BrokerLimiter>,
    core: Arc<Mutex<MdCore>>,
    emitter: Arc<Emitter>,
    subs: RwLock<HashMap<SubscriptionId, MdSubscription>>,
    sub_ids: SubIdGen,
    req_ids: ReqIdGen,
    cancel: CancellationToken,
    ack_timeout: Duration,
    category: String,
}

/// The market data connector of Bybit: implements [`OrderBookSource`], [`MarketDataSource`] and [`InstrumentSource`].
///
/// It is not tied to an account: the public data needs no key. Reads go by HTTP, streams come over one WebSocket
/// session that reconnects on its own and restores its subscriptions. The streams are the public ones of the category
/// (`wss://stream.bybit.com/v5/public/linear`): the demo contour has no public streams of its own, so a connector made
/// from a demo config reads the same market as one made from a main config.
///
/// Instruments are named by the symbol and the category: `BTCUSDT@linear`
/// ([`instrument_id`](crate::util::instrument_id)).
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
/// use brokapi_bybit::{MarketDataConfig, MarketDataConnector};
/// use brokapi_core::{OrderBook, OrderBookSource, ShutdownMode};
/// use brokapi_runtime::BrokerLimiter;
///
/// let (tx, _events) = tokio::sync::mpsc::channel(256);
/// let md = MarketDataConnector::start(MarketDataConfig::new(cfg, "bybit-md"), BrokerLimiter::new(&default_limits()), tx).await?;
/// let OrderBook::Snapshot { bids, asks, .. } = md.order_book(&instrument_id("MOCKUSDT", "linear"), 5).await? else {
///     panic!("a read answers a snapshot");
/// };
/// assert!(bids[0].price < asks[0].price);
/// md.shutdown(ShutdownMode::Immediate).await;
/// # Ok(())
/// # }
/// ```
pub struct MarketDataConnector {
    inner: Arc<MdShared>,
    pump: JoinHandle<()>,
}

impl MarketDataConnector {
    /// Starts the connector: reads the clock of the exchange and opens the WebSocket session in the background.
    ///
    /// # Errors
    ///
    /// [`Error::Config`] when the client cannot be built; [`Error::Transport`] when the exchange cannot be reached.
    pub async fn start(
        cfg: MarketDataConfig,
        limiter: Arc<BrokerLimiter>,
        out: mpsc::Sender<Envelope>,
    ) -> Result<Self, Error> {
        let http = BybitHttpClient::new(cfg.bybit.clone()).map_err(map_config_err)?;
        http.sync_time().await.map_err(map_err)?;

        let (event_tx, event_rx) = kanal::bounded_async(512);
        let ws = WsSession::spawn(http.clone(), WsStream::Public, event_tx, cfg.ws_options());
        let cancel = CancellationToken::new();
        let core = Arc::new(Mutex::new(MdCore::new()));
        let emitter = Emitter::new(Arc::clone(&cfg.source), out);

        let inner = Arc::new(MdShared {
            http,
            ws,
            limiter,
            core: Arc::clone(&core),
            emitter: Arc::clone(&emitter),
            subs: RwLock::new(HashMap::new()),
            sub_ids: SubIdGen::new(0),
            req_ids: ReqIdGen::default(),
            cancel: cancel.clone(),
            ack_timeout: cfg.subscribe_ack_timeout,
            category: cfg.bybit.category.as_str().to_string(),
        });
        let hook = Arc::clone(&inner);
        let pump = tokio::spawn(run_pump(core, emitter, event_rx, cancel, move || {
            let hook = Arc::clone(&hook);
            async move { hook.ask_for_lost_books().await }
        }));
        Ok(Self { inner, pump })
    }

    /// Direct access to any API method with this connector's key and rate limiter. See [`BybitRaw`].
    pub fn raw(&self) -> BybitRaw {
        BybitRaw::new(self.inner.http.clone(), Arc::clone(&self.inner.limiter))
    }

    /// What the connector can do: books up to 1000 levels (the stream sends the whole book after every change, not
    /// increments), and all four streams.
    pub fn caps(&self) -> MarketDataCaps {
        MarketDataCaps::new(MAX_DEPTH, false, StreamingCaps::new(true, true, true, true))
    }

    /// Stops the connector. Unless the mode is `Immediate` it cancels the streams first; `Graceful` also lets the bot's
    /// channel take what is still buffered.
    pub async fn shutdown(self, mode: ShutdownMode) -> ShutdownReport {
        let mut unsubscribed = 0usize;
        let mut unsubscribe_failed = 0usize;
        if !matches!(mode, ShutdownMode::Immediate) {
            let mut topics: Vec<String> = self
                .inner
                .subs
                .read()
                .await
                .values()
                .map(|s| s.topic.clone())
                .collect();
            topics.sort();
            topics.dedup();
            for topic in topics {
                match self.inner.ws.unsubscribe(&topic).await {
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

    fn acquire(&self, method: &str) -> Result<(), Error> {
        self.inner.limiter.acquire(method, brokapi_runtime::now())
    }

    fn symbol(&self, id: &InstrumentId) -> Result<String, Error> {
        symbol_category(id, &self.inner.category)
    }

    async fn register_md_sub(
        &self,
        instrument: InstrumentId,
        kind: MdSubKind,
        core_kind: SubscriptionKind,
        opts: SubscribeOpts,
        topic: String,
    ) -> Result<SubscriptionId, Error> {
        let id = self.inner.sub_ids.next();
        let req = self.inner.req_ids.next();
        self.inner
            .emitter
            .register(id, Some(instrument.clone()), opts.backpressure);
        // The core attributes incoming data to this subscription. It is told before the request goes out, so the
        // answer cannot overtake it.
        let (first_on_topic, notify) = {
            let mut core = self.inner.core.lock().await;
            let first = !core.has_topic(&topic);
            core.register(id, core_kind, &topic, Some(&req));
            (first, core.notifier())
        };
        let sent = if first_on_topic {
            self.inner.ws.subscribe(&req, &topic).await
        } else if kind == MdSubKind::OrderBook {
            // The topic is already open for someone else: this book has no snapshot yet, so the exchange is asked
            // for the topic again, which sends one to all the listeners.
            self.inner.ws.resubscribe(&req, &topic).await
        } else {
            Ok(())
        };
        if let Err(e) = sent {
            self.drop_sub(id).await;
            return Err(map_err(e));
        }
        // Returns when the exchange has confirmed the stream: what happens after that is delivered.
        let confirmed = wait_ack(
            &self.inner.core,
            notify,
            self.inner.ack_timeout,
            |c: &MdCore| c.sub_state(id),
        )
        .await;
        if let Err(e) = confirmed {
            self.drop_sub(id).await;
            return Err(e);
        }
        self.inner.subs.write().await.insert(
            id,
            MdSubscription {
                id,
                instrument,
                kind,
                opts,
                topic,
            },
        );
        Ok(id)
    }

    /// Takes a subscription back: the core forgets it, the exchange is told when nobody else listens to the topic.
    async fn drop_sub(&self, id: SubscriptionId) {
        let orphans = self.inner.core.lock().await.forget(id);
        self.inner.emitter.unregister(id);
        for topic in orphans {
            let _ = self.inner.ws.unsubscribe(&topic).await;
        }
    }
}

impl MdShared {
    /// Asks the exchange for a new snapshot of every order book topic that lost a delta.
    async fn ask_for_lost_books(&self) {
        let topics = self.core.lock().await.take_gaps();
        for topic in topics {
            let req = self.req_ids.next();
            self.core.lock().await.add_request(&req, &topic);
            let _ = self.ws.resubscribe(&req, &topic).await;
        }
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
        let symbol = self.symbol(id)?;
        self.acquire("GetOrderBook")?;
        let view = self
            .inner
            .http
            .get_order_book(
                &self.inner.category,
                &symbol,
                Some(depth.clamp(1, MAX_DEPTH)),
            )
            .await
            .map_err(map_err)?;
        Ok(order_book(
            &view.bids,
            &view.asks,
            view.ts_ms,
            id,
            brokapi_runtime::now(),
        ))
    }

    async fn subscribe_order_book(
        &self,
        id: &InstrumentId,
        opts: SubscribeOpts,
    ) -> Result<SubscriptionId, Error> {
        let symbol = self.symbol(id)?;
        let depth = opts.depth.unwrap_or(DEFAULT_DEPTH).clamp(1, MAX_DEPTH);
        // The stream has fixed depths: the smallest one that holds what was asked for, cut to `depth` by the core.
        let stream_depth = STREAM_DEPTHS
            .iter()
            .copied()
            .find(|d| *d >= depth)
            .unwrap_or(MAX_DEPTH);
        let topic = format!("orderbook.{stream_depth}.{symbol}");
        self.register_md_sub(
            id.clone(),
            MdSubKind::OrderBook,
            SubscriptionKind::OrderBook {
                instrument: id.clone(),
                depth,
            },
            opts,
            topic,
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
        let orphans = self.inner.core.lock().await.forget(sub);
        self.inner.emitter.unregister(sub);
        let mut result = Ok(());
        for topic in orphans {
            if let Err(e) = self.inner.ws.unsubscribe(&topic).await {
                result = Err(map_err(e));
            }
        }
        let _ = record;
        result
    }
}

#[async_trait]
impl MarketDataSource for MarketDataConnector {
    async fn last_trades(&self, id: &InstrumentId, limit: u32) -> Result<Vec<MarketTrade>, Error> {
        let symbol = self.symbol(id)?;
        self.acquire("GetLastTrades")?;
        let records = self
            .inner
            .http
            .get_recent_trades(&self.inner.category, &symbol, limit.clamp(1, 1000))
            .await
            .map_err(map_err)?;
        let now = brokapi_runtime::now();
        // Newest first from the exchange; the shared interface wants the oldest first.
        let mut trades: Vec<MarketTrade> = records
            .iter()
            .rev()
            .filter_map(|r| market_trade(r, id, now))
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
        let symbol = self.symbol(id)?;
        self.register_md_sub(
            id.clone(),
            MdSubKind::MarketTrades,
            SubscriptionKind::LastTrades {
                instrument: id.clone(),
            },
            opts,
            format!("publicTrade.{symbol}"),
        )
        .await
    }

    async fn subscribe_quotes(
        &self,
        id: &InstrumentId,
        opts: SubscribeOpts,
    ) -> Result<SubscriptionId, Error> {
        let symbol = self.symbol(id)?;
        // The best level of the book is the quote: the level 1 stream pushes it within 10 ms of a change.
        self.register_md_sub(
            id.clone(),
            MdSubKind::Quotes,
            SubscriptionKind::Quotes {
                instrument: id.clone(),
            },
            opts,
            format!("orderbook.1.{symbol}"),
        )
        .await
    }

    async fn candles(
        &self,
        id: &InstrumentId,
        tf: Timeframe,
        range: TimeRange,
    ) -> Result<Vec<Candle>, Error> {
        let symbol = self.symbol(id)?;
        let interval = interval_label(tf).ok_or_else(|| Error::Unsupported {
            capability: "candles_m10".into(),
        })?;
        let span = KLINE_LIMIT as u64 * tf.seconds();
        let asked = (range.to.as_nanos().saturating_sub(range.from.as_nanos()) / 1_000_000_000)
            .max(0) as u64;
        if asked > span {
            return Err(Error::Rejected {
                code: "range_too_long".into(),
                message: format!(
                    "one request covers {KLINE_LIMIT} bars ({span} seconds of this timeframe) and the range is {asked} seconds: read it in windows (brokapi_runtime::history does)"
                ),
            });
        }
        self.acquire("GetCandles")?;
        let rows = self
            .inner
            .http
            .get_klines(
                &self.inner.category,
                &symbol,
                interval,
                Some(nanos_to_ms(range.from)),
                // The exchange counts both ends: asked to `to` itself, a full window is one bar over the limit, and
                // the limit cuts the oldest one (the answer is newest first).
                Some(nanos_to_ms(range.to) - 1),
                KLINE_LIMIT,
            )
            .await
            .map_err(map_err)?;
        let now = brokapi_runtime::now();
        Ok(rows
            .iter()
            // The window is half open: a bar that starts at `to` belongs to the next one.
            .filter(|k| k.start_ms < nanos_to_ms(range.to))
            .map(|k| candle(k, id, tf, now))
            .collect())
    }

    fn candles_max_span(&self, tf: Timeframe) -> Option<Duration> {
        interval_label(tf)?;
        Some(Duration::from_secs(KLINE_LIMIT as u64 * tf.seconds()))
    }

    async fn subscribe_candles(
        &self,
        id: &InstrumentId,
        tf: Timeframe,
        opts: SubscribeOpts,
    ) -> Result<SubscriptionId, Error> {
        let symbol = self.symbol(id)?;
        let interval = interval_label(tf).ok_or_else(|| Error::Unsupported {
            capability: "candles_m10".into(),
        })?;
        self.register_md_sub(
            id.clone(),
            MdSubKind::Candles,
            SubscriptionKind::LastCandle {
                instrument: id.clone(),
                timeframe: tf,
            },
            opts,
            format!("kline.{interval}.{symbol}"),
        )
        .await
    }
}

#[async_trait]
impl InstrumentSource for MarketDataConnector {
    async fn find(&self, q: InstrumentQuery) -> Result<Vec<Instrument>, Error> {
        match q {
            InstrumentQuery::ByTicker { ticker, exchange } => {
                // The exchange is one: BYBIT, or the category written the way the key does.
                if exchange.as_deref().is_some_and(|e| {
                    !e.eq_ignore_ascii_case("BYBIT")
                        && !e.eq_ignore_ascii_case(&self.inner.category)
                }) {
                    return Ok(Vec::new());
                }
                self.acquire("GetInstruments")?;
                match self
                    .inner
                    .http
                    .get_instrument(&self.inner.category, &ticker.to_ascii_uppercase())
                    .await
                {
                    Ok(info) => Ok(vec![instrument(&info, &self.inner.category)]),
                    Err(e) => match map_err(e) {
                        e if e.is_not_found() => Ok(Vec::new()),
                        e => Err(e),
                    },
                }
            }
            InstrumentQuery::ByIsin(_) => Err(Error::Unsupported {
                capability: "find_by_isin".into(),
            }),
            InstrumentQuery::ByKind(kind) => self.find_by_kind(kind).await,
        }
    }

    async fn get(&self, id: &InstrumentId) -> Result<Instrument, Error> {
        let symbol = self.symbol(id)?;
        self.acquire("GetInstruments")?;
        let info = self
            .inner
            .http
            .get_instrument(&self.inner.category, &symbol)
            .await
            .map_err(map_err)?;
        Ok(instrument(&info, &self.inner.category))
    }

    async fn trading_params(&self, id: &InstrumentId) -> Result<TradingParams, Error> {
        let symbol = self.symbol(id)?;
        self.acquire("GetInstruments")?;
        let info = self
            .inner
            .http
            .get_instrument(&self.inner.category, &symbol)
            .await
            .map_err(map_err)?;
        Ok(trading_params(&info, &self.inner.category))
    }

    async fn trading_status(&self, id: &InstrumentId) -> Result<TradingStatus, Error> {
        let symbol = self.symbol(id)?;
        self.acquire("GetInstruments")?;
        let info = self
            .inner
            .http
            .get_instrument(&self.inner.category, &symbol)
            .await
            .map_err(map_err)?;
        Ok(trading_status(
            &info,
            &self.inner.category,
            Some(unix_now_nanos()),
        ))
    }

    async fn schedule(&self, _exchange: &str, _range: TimeRange) -> Result<Schedule, Error> {
        // A crypto exchange trades round the clock: there is no timetable of sessions.
        Err(Error::Unsupported {
            capability: "schedule".into(),
        })
    }
}

impl MarketDataConnector {
    async fn find_by_kind(&self, kind: InstrumentKind) -> Result<Vec<Instrument>, Error> {
        match kind {
            InstrumentKind::Perpetual | InstrumentKind::Future => {}
            InstrumentKind::Other => {
                return Err(Error::Unsupported {
                    capability: "find_other_by_kind".into(),
                });
            }
            // Category linear holds contracts only.
            _ => return Ok(Vec::new()),
        }
        let mut all = Vec::new();
        let mut cursor: Option<String> = None;
        for page in 0..MAX_PAGES {
            if page > 0 {
                self.acquire("GetInstruments")?;
            }
            let batch = self
                .inner
                .http
                .get_instruments_page(&self.inner.category, PAGE_SIZE, cursor.as_deref())
                .await
                .map_err(map_err)?;
            all.extend(
                batch
                    .list
                    .iter()
                    .filter(|i| kind_of(&i.contract_type) == kind)
                    .map(|i| instrument(i, &self.inner.category)),
            );
            if batch.next_page_cursor.is_empty() {
                return Ok(all);
            }
            cursor = Some(batch.next_page_cursor);
        }
        Err(Error::Transport(brokapi_core::TransportError::Message(
            format!(
                "the list of instruments is longer than {MAX_PAGES} pages: not returned in part"
            ),
        )))
    }
}
