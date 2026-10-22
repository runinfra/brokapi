//! The market data connector: order books, trades of the market, quotes, candles and the reference data of ALOR.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use brokapi_alor_protocol::http::client::AlorHttpClient;
use brokapi_alor_protocol::ws::models::WsSubscribe;
use brokapi_alor_protocol::ws::session::WsSession;
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
use crate::map::instrument::{instrument, trading_params, trading_status};
use crate::map::market::{candles, market_trade, order_book};
use crate::map::unix_nanos;
use crate::pump::{drain_backlog, run_pump};
use crate::raw::AlorRaw;
use crate::subs::{MdSubKind, MdSubscription, SubIdGen, new_guid, wait_ack};
use crate::util::{exchange_of_board, nanos_to_secs, ticker_board, timeframe_label};

/// The most levels of an order book the broker gives (50 on each side).
const MAX_DEPTH: u32 = 50;

/// How many instruments a page of the search asks for, and how many pages are read at most.
const KIND_PAGE_SIZE: u32 = 100;
const MAX_KIND_PAGES: u32 = 200;

struct MdShared {
    http: AlorHttpClient,
    ws: WsSession,
    limiter: Arc<BrokerLimiter>,
    core: Arc<Mutex<MdCore>>,
    emitter: Arc<Emitter>,
    subs: RwLock<HashMap<SubscriptionId, MdSubscription>>,
    sub_ids: SubIdGen,
    cancel: CancellationToken,
    ack_timeout: Duration,
}

/// The market data connector of ALOR: implements [`OrderBookSource`], [`MarketDataSource`] and
/// [`InstrumentSource`]. It is not tied to an account.
///
/// Start it with [`MarketDataConnector::start`]; commands return their answer directly and subscriptions deliver
/// [`Envelope`]s into the channel given at start. The data comes over one WebSocket session that reconnects on its
/// own and restores the subscriptions (an order book is then marked with `Event::Resync`).
///
/// An instrument is named by `NativeId::TickerClass { ticker, class_code }`, where the class code is the trading
/// mode (the board, for example `TQBR`). The exchange follows from it: the board `SPBX` is the exchange `SPBX`,
/// every other board is `MOEX`.
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
/// use brokapi_alor::{MarketDataConfig, MarketDataConnector};
/// use brokapi_core::{OrderBook, OrderBookSource, ShutdownMode};
/// use brokapi_runtime::BrokerLimiter;
/// use tokio::sync::mpsc;
///
/// let (tx, _events) = mpsc::channel(256);
/// let md = MarketDataConnector::start(
///     MarketDataConfig::new(cfg, "alor-md"),
///     BrokerLimiter::new(&default_limits()),
///     tx,
/// )
/// .await?;
/// let book = md.order_book(&instrument_id("SBER", "TQBR"), 5).await?;
/// let OrderBook::Snapshot { bids, .. } = book else { unreachable!() };
/// assert!(!bids.is_empty());
/// md.shutdown(ShutdownMode::Immediate).await;
/// # Ok(())
/// # }
/// ```
pub struct MarketDataConnector {
    inner: Arc<MdShared>,
    pump: JoinHandle<()>,
}

impl MarketDataConnector {
    /// Starts the connector: gets the first access token (so a refused token is an error here, not at the first
    /// call) and opens the WebSocket session in the background.
    ///
    /// # Errors
    ///
    /// [`Error::Config`] when the client cannot be built or the broker refuses the token; [`Error::Transport`]
    /// when the authorization server cannot be reached.
    pub async fn start(
        cfg: MarketDataConfig,
        limiter: Arc<BrokerLimiter>,
        out: mpsc::Sender<Envelope>,
    ) -> Result<Self, Error> {
        let http = AlorHttpClient::new(cfg.alor.clone()).map_err(map_config_err)?;
        http.get_access_token().await.map_err(map_err)?;

        let (event_tx, event_rx) = kanal::bounded_async(512);
        let ws = WsSession::spawn_with(http.clone(), event_tx, cfg.ws_options());
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
            cancel: cancel.clone(),
            ack_timeout: cfg.subscribe_ack_timeout,
        });
        let pump = tokio::spawn(run_pump(
            core,
            emitter,
            event_rx,
            cancel,
            |m| async move { m },
        ));
        Ok(Self { inner, pump })
    }

    /// Direct access to any API method with this connector's token and rate limiter. See [`AlorRaw`].
    pub fn raw(&self) -> AlorRaw {
        AlorRaw::new(self.inner.http.clone(), Arc::clone(&self.inner.limiter))
    }

    /// What the connector can do: books of up to 50 levels (always whole books), all four streams.
    pub fn caps(&self) -> MarketDataCaps {
        MarketDataCaps::new(MAX_DEPTH, false, StreamingCaps::new(true, true, true, true))
    }

    /// Stops the connector. Unless the mode is `Immediate`, it cancels the subscriptions at the broker first;
    /// `Graceful` also lets the bot's channel take what is still buffered.
    pub async fn shutdown(self, mode: ShutdownMode) -> ShutdownReport {
        let mut unsubscribed = 0usize;
        let mut unsubscribe_failed = 0usize;
        if !matches!(mode, ShutdownMode::Immediate) {
            let subs: Vec<_> = self.inner.subs.read().await.values().cloned().collect();
            for sub in subs {
                match self.inner.ws.unsubscribe(&sub.guid).await {
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

    async fn register_md_sub(
        &self,
        instrument: InstrumentId,
        kind: MdSubKind,
        core_kind: SubscriptionKind,
        opts: SubscribeOpts,
        guid: String,
        ws: WsSubscribe,
    ) -> Result<SubscriptionId, Error> {
        let id = self.inner.sub_ids.next();
        self.inner
            .emitter
            .register(id, Some(instrument.clone()), opts.backpressure);
        // The core attributes incoming data to this subscription. It is told before the request goes out, so the
        // confirmation cannot overtake it.
        self.inner.core.lock().await.register(&guid, id, core_kind);
        let notify = self.inner.core.lock().await.notifier();
        if let Err(e) = self.inner.ws.subscribe(ws).await {
            self.inner.core.lock().await.forget(&guid);
            self.inner.emitter.unregister(id);
            return Err(map_err(e));
        }
        // Returns when the broker has confirmed the subscription: what happens after that is delivered.
        let confirmed = wait_ack(
            &self.inner.core,
            notify,
            self.inner.ack_timeout,
            |c: &MdCore| {
                (
                    c.actual_state(id).unwrap_or(crate::core::SubState::Pending),
                    c.failure(id).map(str::to_string),
                )
            },
        )
        .await;
        if let Err(e) = confirmed {
            let _ = self.inner.ws.unsubscribe(&guid).await;
            self.inner.core.lock().await.forget(&guid);
            self.inner.emitter.unregister(id);
            return Err(e);
        }
        self.inner.subs.write().await.insert(
            id,
            MdSubscription {
                id,
                instrument,
                kind,
                opts,
                guid,
            },
        );
        Ok(id)
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
        self.acquire("GetOrderBook")?;
        let (ticker, board) = ticker_board(id)?;
        let wire = self
            .inner
            .http
            .get_order_book(
                exchange_of_board(&board),
                &ticker,
                Some(&board),
                Some(depth.clamp(1, MAX_DEPTH)),
            )
            .await
            .map_err(map_err)?;
        Ok(order_book(&wire, id, brokapi_runtime::now()))
    }

    async fn subscribe_order_book(
        &self,
        id: &InstrumentId,
        opts: SubscribeOpts,
    ) -> Result<SubscriptionId, Error> {
        let depth = opts.depth.unwrap_or(MAX_DEPTH).clamp(1, MAX_DEPTH);
        let (ticker, board) = ticker_board(id)?;
        let guid = new_guid();
        let ws = WsSubscribe::order_book(
            &guid,
            exchange_of_board(&board),
            &ticker,
            Some(&board),
            depth,
        );
        self.register_md_sub(
            id.clone(),
            MdSubKind::OrderBook,
            SubscriptionKind::OrderBook {
                instrument: id.clone(),
                depth,
            },
            opts,
            guid,
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
        self.inner.core.lock().await.forget(&record.guid);
        self.inner.emitter.unregister(sub);
        self.inner
            .ws
            .unsubscribe(&record.guid)
            .await
            .map_err(map_err)
    }
}

#[async_trait]
impl MarketDataSource for MarketDataConnector {
    async fn last_trades(&self, id: &InstrumentId, limit: u32) -> Result<Vec<MarketTrade>, Error> {
        self.acquire("GetLastTrades")?;
        let (ticker, board) = ticker_board(id)?;
        let query = [
            ("take", limit.to_string()),
            ("descending", "true".to_string()),
        ];
        let records = self
            .inner
            .http
            .get_all_trades(exchange_of_board(&board), &ticker, Some(&board), &query)
            .await
            .map_err(map_err)?;
        let now = brokapi_runtime::now();
        // Newest first from the broker; the shared interface wants the oldest first.
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
        let (ticker, board) = ticker_board(id)?;
        let guid = new_guid();
        let ws = WsSubscribe::all_trades(&guid, exchange_of_board(&board), &ticker, Some(&board));
        self.register_md_sub(
            id.clone(),
            MdSubKind::MarketTrades,
            SubscriptionKind::LastTrades {
                instrument: id.clone(),
            },
            opts,
            guid,
            ws,
        )
        .await
    }

    async fn subscribe_quotes(
        &self,
        id: &InstrumentId,
        opts: SubscribeOpts,
    ) -> Result<SubscriptionId, Error> {
        let (ticker, board) = ticker_board(id)?;
        let guid = new_guid();
        let ws = WsSubscribe::quotes(&guid, exchange_of_board(&board), &ticker, Some(&board));
        self.register_md_sub(
            id.clone(),
            MdSubKind::Quotes,
            SubscriptionKind::Quotes {
                instrument: id.clone(),
            },
            opts,
            guid,
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
        self.acquire("GetCandles")?;
        let (ticker, board) = ticker_board(id)?;
        let resp = self
            .inner
            .http
            .get_history(
                exchange_of_board(&board),
                &ticker,
                Some(&board),
                &timeframe_label(tf),
                nanos_to_secs(range.from),
                nanos_to_secs(range.to),
                None,
            )
            .await
            .map_err(map_err)?;
        Ok(candles(&resp, id, tf, brokapi_runtime::now()))
    }

    async fn subscribe_candles(
        &self,
        id: &InstrumentId,
        tf: Timeframe,
        opts: SubscribeOpts,
    ) -> Result<SubscriptionId, Error> {
        let (ticker, board) = ticker_board(id)?;
        let guid = new_guid();
        // Only the candle that is forming now and the ones after it: the history is read with `candles`.
        let from = nanos_to_secs(unix_nanos(chrono::Utc::now()));
        let ws = WsSubscribe::bars(
            &guid,
            exchange_of_board(&board),
            &ticker,
            Some(&board),
            &timeframe_label(tf),
            from,
            true,
        );
        self.register_md_sub(
            id.clone(),
            MdSubKind::Candles,
            SubscriptionKind::LastCandle {
                instrument: id.clone(),
                timeframe: tf,
            },
            opts,
            guid,
            ws,
        )
        .await
    }
}

#[async_trait]
impl InstrumentSource for MarketDataConnector {
    async fn find(&self, q: InstrumentQuery) -> Result<Vec<Instrument>, Error> {
        self.acquire("GetSecurities")?;
        match q {
            InstrumentQuery::ByTicker { ticker, exchange } => {
                let mut query = vec![("query", ticker.clone()), ("limit", "100".to_string())];
                if let Some(e) = exchange {
                    query.push(("exchange", e));
                }
                // The search matches a part of the ticker: keep the exact ticker, in every board it trades in.
                query.push(("includeNonBaseBoards", "true".to_string()));
                let list = self
                    .inner
                    .http
                    .get_securities(&query)
                    .await
                    .map_err(map_err)?;
                Ok(list
                    .iter()
                    .filter(|s| s.symbol.eq_ignore_ascii_case(&ticker))
                    .map(instrument)
                    .collect())
            }
            InstrumentQuery::ByIsin(_) => Err(Error::Unsupported {
                capability: "find_by_isin".into(),
            }),
            InstrumentQuery::ByKind(kind) => self.find_by_kind(kind).await,
        }
    }

    async fn get(&self, id: &InstrumentId) -> Result<Instrument, Error> {
        self.acquire("GetSecurity")?;
        let (ticker, board) = ticker_board(id)?;
        let info = self
            .inner
            .http
            .get_security(exchange_of_board(&board), &ticker, Some(&board))
            .await
            .map_err(map_err)?;
        Ok(instrument(&info))
    }

    async fn trading_params(&self, id: &InstrumentId) -> Result<TradingParams, Error> {
        self.acquire("GetSecurity")?;
        let (ticker, board) = ticker_board(id)?;
        let info = self
            .inner
            .http
            .get_security(exchange_of_board(&board), &ticker, Some(&board))
            .await
            .map_err(map_err)?;
        Ok(trading_params(&info))
    }

    async fn trading_status(&self, id: &InstrumentId) -> Result<TradingStatus, Error> {
        self.acquire("GetTradingStatus")?;
        let (ticker, board) = ticker_board(id)?;
        let info = self
            .inner
            .http
            .get_security(exchange_of_board(&board), &ticker, Some(&board))
            .await
            .map_err(map_err)?;
        let mut status = trading_status(&info, Some(unix_nanos(chrono::Utc::now())));
        // The status is about the instrument asked for, in the board asked for.
        status.instrument = id.clone();
        Ok(status)
    }

    async fn schedule(&self, _exchange: &str, _range: TimeRange) -> Result<Schedule, Error> {
        // ALOR gives the state of a market now (`/md/status/...`) and the status code of an instrument, but no
        // timetable of sessions.
        Err(Error::Unsupported {
            capability: "schedule".into(),
        })
    }
}

impl MarketDataConnector {
    /// Instruments of a kind: a search by market, filtered by the kind the connector reads from the type.
    async fn find_by_kind(&self, kind: InstrumentKind) -> Result<Vec<Instrument>, Error> {
        let sector = match kind {
            InstrumentKind::Stock
            | InstrumentKind::Bond
            | InstrumentKind::Etf
            | InstrumentKind::MutualFund => "FOND",
            InstrumentKind::Future | InstrumentKind::Option => "FORTS",
            InstrumentKind::Currency => "CURR",
            InstrumentKind::Other => {
                return Err(Error::Unsupported {
                    capability: "find_other_by_kind".into(),
                });
            }
        };
        let mut all = Vec::new();
        for page in 0..MAX_KIND_PAGES {
            if page > 0 {
                self.acquire("GetSecurities")?;
            }
            let query = [
                ("sector", sector.to_string()),
                ("limit", KIND_PAGE_SIZE.to_string()),
                ("offset", (page * KIND_PAGE_SIZE).to_string()),
            ];
            let batch = self
                .inner
                .http
                .get_securities(&query)
                .await
                .map_err(map_err)?;
            let full = batch.len() >= KIND_PAGE_SIZE as usize;
            all.extend(
                batch
                    .iter()
                    .filter(|s| crate::map::instrument::kind_of(s) == kind)
                    .map(instrument),
            );
            if !full {
                return Ok(all);
            }
        }
        Err(Error::Transport(brokapi_core::TransportError::Message(
            format!(
                "the list of instruments of this kind is longer than {MAX_KIND_PAGES} pages: not returned in part"
            ),
        )))
    }
}
