//! [`Client`]: one account at one broker.

use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use brokapi::core::{
    Candle, ConfigError, Envelope, Error, InstrumentId, InstrumentKey, InstrumentQuery,
    InstrumentSource, MarketDataCaps, NativeId, ShutdownMode, ShutdownReport, TimeRange, Timeframe,
    TradingParams, UserDataCaps,
};
use brokapi::runtime::BrokerLimiter;
use brokapi::runtime::history::{CandlePages, HistoryOpts};
use serde::Serialize;
use serde_json::Value;
use tokio::sync::{Mutex, RwLock, RwLockReadGuard, mpsc};

use crate::Account;
use crate::conn::{Conn, start};
use crate::error::{ClientError, config_error};
use crate::settings::Settings;

/// The brokers of the library.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Broker {
    /// BCS.
    Bcs,
    /// T-Bank.
    Tbank,
    /// Finam.
    Finam,
    /// ALOR.
    Alor,
    /// Bybit (USDT perpetuals and futures, API V5).
    Bybit,
}

impl Broker {
    /// All of them, in the order of the documentation.
    pub const ALL: [Broker; 5] = [
        Broker::Bcs,
        Broker::Tbank,
        Broker::Finam,
        Broker::Alor,
        Broker::Bybit,
    ];

    /// The lower case name used in settings, in logs and on the wire: `"bcs"`, `"tbank"`, `"finam"`, `"alor"`,
    /// `"bybit"`.
    pub const fn as_str(self) -> &'static str {
        match self {
            Broker::Bcs => "bcs",
            Broker::Tbank => "tbank",
            Broker::Finam => "finam",
            Broker::Alor => "alor",
            Broker::Bybit => "bybit",
        }
    }
}

impl std::fmt::Display for Broker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Broker {
    type Err = Error;

    fn from_str(name: &str) -> Result<Self, Error> {
        match name {
            "bcs" => Ok(Broker::Bcs),
            "tbank" => Ok(Broker::Tbank),
            "finam" => Ok(Broker::Finam),
            "alor" => Ok(Broker::Alor),
            "bybit" => Ok(Broker::Bybit),
            other => Err(Error::Config(ConfigError::Message(format!(
                "unknown broker {other:?}: use \"bcs\", \"tbank\", \"finam\" or \"alor\""
            )))),
        }
    }
}

/// How to connect to one broker: the configuration of its connectors.
#[derive(Debug, Clone)]
pub enum BrokerConfig {
    /// BCS.
    Bcs(brokapi::bcs::protocol::config::BcsConfig),
    /// T-Bank.
    Tbank(brokapi::tbank::protocol::TbankConfig),
    /// Finam: the API configuration and the account the connector trades.
    Finam {
        /// The API: the secret, the endpoint.
        api: brokapi::finam::FinamApiConfig,
        /// The account id.
        account: String,
    },
    /// ALOR.
    Alor(brokapi::alor::protocol::config::AlorConfig),
    /// Bybit.
    Bybit(brokapi::bybit::protocol::config::BybitConfig),
}

impl BrokerConfig {
    /// The broker this configuration is for.
    pub fn broker(&self) -> Broker {
        match self {
            BrokerConfig::Bcs(_) => Broker::Bcs,
            BrokerConfig::Tbank(_) => Broker::Tbank,
            BrokerConfig::Finam { .. } => Broker::Finam,
            BrokerConfig::Alor(_) => Broker::Alor,
            BrokerConfig::Bybit(_) => Broker::Bybit,
        }
    }

    /// Reads the configuration of `broker` from the settings, then from the environment variables of the library
    /// (`BCS_REFRESH_TOKEN`, `TBANK_TOKEN`, `FINAM_SECRET`, ...) for everything the settings do not say.
    pub fn from_settings(broker: Broker, settings: &Settings) -> Result<Self, Error> {
        Ok(match broker {
            Broker::Bcs => BrokerConfig::Bcs(settings.bcs()?),
            Broker::Tbank => BrokerConfig::Tbank(settings.tbank()?),
            Broker::Alor => BrokerConfig::Alor(settings.alor()?),
            Broker::Bybit => BrokerConfig::Bybit(settings.bybit()?),
            Broker::Finam => {
                let (api, account) = settings.finam()?;
                BrokerConfig::Finam { api, account }
            }
        })
    }
}

/// The rate limit counter of one broker area. A clone is the same counter: clients that must share the quota of one
/// broker user are given clones of one limiter.
#[derive(Clone)]
pub struct Limiter {
    broker: Broker,
    limiter: Arc<BrokerLimiter>,
}

impl Limiter {
    /// A counter with the default limits of the broker.
    pub fn new(broker: Broker) -> Self {
        let limiter = match broker {
            Broker::Bcs => BrokerLimiter::new(&brokapi::bcs::limits::default_limits()),
            Broker::Tbank => BrokerLimiter::new(&brokapi::tbank::limits::default_limits()),
            Broker::Alor => BrokerLimiter::new(&brokapi::alor::limits::default_limits()),
            Broker::Bybit => BrokerLimiter::new(&brokapi::bybit::limits::default_limits()),
            Broker::Finam => BrokerLimiter::new(&brokapi::finam::limits::default_limits()),
        };
        Self { broker, limiter }
    }

    /// A counter with limits of your own (the buckets and routes of the broker's limits file).
    pub fn with_limits(broker: Broker, limits: &brokapi::runtime::BrokerLimits) -> Self {
        Self {
            broker,
            limiter: BrokerLimiter::new(limits),
        }
    }

    /// The broker whose limits this counter keeps.
    pub fn broker(&self) -> Broker {
        self.broker
    }

    /// The counter of the runtime, for code that talks to a connector directly.
    pub fn shared(&self) -> &Arc<BrokerLimiter> {
        &self.limiter
    }
}

/// What a connection can do: the capabilities of its two connectors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Caps {
    /// Market data side.
    pub md: MarketDataCaps,
    /// Account side.
    pub ud: UserDataCaps,
}

/// The reports of the two connectors after [`Client::close`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CloseReport {
    /// Market data connector.
    pub md: ShutdownReport,
    /// Account connector.
    pub ud: ShutdownReport,
}

pub(crate) struct Inner {
    broker: Broker,
    name: String,
    /// `None` after `close`.
    pub(crate) conn: RwLock<Option<Conn>>,
    closed: AtomicBool,
    /// `None` once taken with [`Client::take_events`].
    events: Mutex<Option<mpsc::Receiver<Envelope>>>,
    limiter: Limiter,
}

/// One account at one broker: both connectors, the event channel, the rate limiter.
///
/// A `Client` is a cheap handle (a clone is the same connection). It implements the traits of `brokapi-core`
/// ([`OrderExecutor`](brokapi::core::OrderExecutor), [`PortfolioSource`](brokapi::core::PortfolioSource), [`OrderBookSource`](brokapi::core::OrderBookSource) and the rest), so code written against them runs on
/// any of the five brokers, and it can be called by method name with JSON ([`Client::call`]), which is how the Python
/// module and the gateway reach it.
///
/// After [`Client::close`] every typed call fails with [`Error::ShuttingDown`] and a call by name with
/// [`ClientError::Closed`].
///
/// # Examples
///
/// Connect to an imitation of a broker and read an order book. The settings of a real account are the same keys
/// (`token`, `refresh_token`, ...) or the environment variables of the library.
///
/// ```
/// use brokapi_client::{Client, Settings};
/// use brokapi::core::{OrderBook, OrderBookSource};
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
/// # let endpoint = server.http_base.clone();
/// let settings = Settings::parse(&format!(
///     r#"{{"refresh_token": "mock-refresh-token", "endpoint": "{endpoint}"}}"#
/// ))?;
/// let client = Client::connect("bcs", "main", &settings, None, 256).await?;
/// let sber = client.instrument("SBER", Some("TQBR")).await?;
/// assert!(matches!(client.order_book(&sber, 5).await?, OrderBook::Snapshot { .. }));
/// client.close(brokapi::core::ShutdownMode::Immediate).await;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct Client {
    pub(crate) inner: Arc<Inner>,
}

impl Client {
    /// Starts both connectors of the broker. `name` is the name of the connection: its events come from the sources
    /// `"{name}-md"` and `"{name}-ud"`. `limiter` is the shared counter of the broker user (a new one with the default
    /// limits when `None`, a limiter of another broker is a configuration error). `event_buffer` is the size of the
    /// event channel.
    pub async fn start(
        config: BrokerConfig,
        name: impl Into<String>,
        limiter: Option<Limiter>,
        event_buffer: usize,
    ) -> Result<Self, Error> {
        let broker = config.broker();
        let name = name.into();
        let limiter = match limiter {
            Some(l) if l.broker == broker => l,
            Some(l) => {
                return Err(config_error(format!(
                    "this limiter counts the limits of {}, not of {broker}: a limiter is shared only inside one broker",
                    l.broker
                )));
            }
            None => Limiter::new(broker),
        };
        let (tx, rx) = mpsc::channel::<Envelope>(event_buffer.max(1));
        let conn = start(config, &name, limiter.limiter.clone(), tx).await?;
        Ok(Self {
            inner: Arc::new(Inner {
                broker,
                name,
                conn: RwLock::new(Some(conn)),
                closed: AtomicBool::new(false),
                events: Mutex::new(Some(rx)),
                limiter,
            }),
        })
    }

    /// [`Client::start`] for a broker named by text and settings in the form of [`Settings`]. A `.env` file in the
    /// working directory (never a parent) is read first, like `from_env` of the library does.
    pub async fn connect(
        broker: &str,
        name: impl Into<String>,
        settings: &Settings,
        limiter: Option<Limiter>,
        event_buffer: usize,
    ) -> Result<Self, ClientError> {
        let broker: Broker = broker.parse()?;
        dotenvy::from_path(".env").ok();
        let config = BrokerConfig::from_settings(broker, settings)?;
        Ok(Self::start(config, name, limiter, event_buffer).await?)
    }

    /// The broker.
    pub fn broker(&self) -> Broker {
        self.inner.broker
    }

    /// The name given at start.
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// The rate limit counter of this connection (a clone of it shares the quota).
    pub fn limiter(&self) -> Limiter {
        self.inner.limiter.clone()
    }

    /// Whether [`Client::close`] has been called.
    pub fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::SeqCst)
    }

    /// What the two connectors can do, `None` after close.
    pub async fn caps(&self) -> Option<Caps> {
        self.inner.conn.read().await.as_ref().map(Conn::caps)
    }

    /// Tells the account connector the price step of an instrument: a price off the step is then refused before
    /// anything is sent. [`InstrumentSource::trading_params`] of the client does it by itself.
    pub async fn learn_grid(&self, params: &TradingParams) {
        if let Some(conn) = self.inner.conn.read().await.as_ref() {
            conn.learn_grid(params);
        }
    }

    /// The next event, or `None` when no connector can send any more (after [`Client::close`]) or the channel was
    /// taken with [`Client::take_events`].
    pub async fn next_event(&self) -> Option<Envelope> {
        let mut guard = self.inner.events.lock().await;
        guard.as_mut()?.recv().await
    }

    /// Up to `limit` events that are already in the channel, without waiting.
    pub fn drain_events(&self, limit: usize) -> Vec<Envelope> {
        let Ok(mut guard) = self.inner.events.try_lock() else {
            return Vec::new();
        };
        let mut out = Vec::new();
        if let Some(rx) = guard.as_mut() {
            while out.len() < limit {
                match rx.try_recv() {
                    Ok(env) => out.push(env),
                    Err(_) => break,
                }
            }
        }
        out
    }

    /// Hands the event channel to the caller (a task that forwards the events). `None` if it was taken already.
    pub async fn take_events(&self) -> Option<mpsc::Receiver<Envelope>> {
        self.inner.events.lock().await.take()
    }

    /// Stops both connectors: the account side first, then market data. `ShutdownMode::Graceful` waits up to the
    /// deadline. Nothing is cancelled at the exchange. `None` when the client was closed before.
    pub async fn close(&self, mode: ShutdownMode) -> Option<CloseReport> {
        let conn = {
            let mut guard = self.inner.conn.write().await;
            self.inner.closed.store(true, Ordering::SeqCst);
            guard.take()
        };
        let (md, ud) = conn?.shutdown(mode).await;
        Some(CloseReport { md, ud })
    }

    /// Any method of the broker's API by its name, arguments and reply as JSON. `command` says that the call changes
    /// something at the broker (a lost reply is then `Unknown`, and the call is not repeated). See the module `raw` for
    /// the names each broker takes.
    pub async fn raw(
        &self,
        method: &str,
        args: &Value,
        command: bool,
    ) -> Result<Value, ClientError> {
        let guard = self.inner.conn.read().await;
        let conn = guard.as_ref().ok_or(ClientError::Closed)?;
        conn.raw(method, args, command).await
    }

    /// The instrument of a ticker. BCS, Finam, ALOR and Bybit build it from the text, T-Bank looks it up in its directory.
    /// `board` is the trading board (`TQBR` by default, `MISX` for Finam, the category `linear` for Bybit); `"SBER@TQBR"` names both in one text.
    pub async fn instrument(
        &self,
        ticker: &str,
        board: Option<&str>,
    ) -> Result<InstrumentId, ClientError> {
        let (ticker, board) = match ticker.split_once('@') {
            Some((t, b)) if board.is_none() => (t, Some(b)),
            _ => (ticker, board),
        };
        if ticker.is_empty() {
            return Err(ClientError::Arg("the ticker is empty".into()));
        }
        match self.inner.broker {
            Broker::Bcs => {
                let class = board.unwrap_or("TQBR");
                Ok(InstrumentId::new(
                    InstrumentKey::new(ticker, class),
                    NativeId::TickerClass {
                        ticker: ticker.into(),
                        class_code: class.into(),
                    },
                ))
            }
            Broker::Alor => Ok(brokapi::alor::util::instrument_id(
                ticker,
                board.unwrap_or("TQBR"),
            )),
            Broker::Bybit => Ok(brokapi::bybit::util::instrument_id(
                ticker,
                board.unwrap_or("linear"),
            )),
            Broker::Finam => Ok(brokapi::finam::util::instrument_from_symbol(&format!(
                "{ticker}@{}",
                board.unwrap_or("MISX")
            ))),
            Broker::Tbank => {
                let found = self
                    .find(InstrumentQuery::ByTicker {
                        ticker: ticker.into(),
                        exchange: Some(board.unwrap_or("TQBR").into()),
                    })
                    .await?;
                found.into_iter().next().map(|i| i.id).ok_or_else(|| {
                    ClientError::Broker(Error::Rejected {
                        code: "not_found".into(),
                        message: format!("no instrument {ticker} on {}", board.unwrap_or("TQBR")),
                    })
                })
            }
        }
    }

    /// A reader of a long candle history, window by window, from `range.from` to `range.to`.
    pub fn candle_pages(
        &self,
        instrument: InstrumentId,
        timeframe: Timeframe,
        range: TimeRange,
        opts: HistoryOpts,
    ) -> CandlePager {
        CandlePager::new(self.clone(), instrument, timeframe, range, opts)
    }

    /// The connectors for one call, or [`Error::ShuttingDown`] after close.
    pub(crate) async fn conn(&self) -> Result<RwLockReadGuard<'_, Conn>, Error> {
        RwLockReadGuard::try_map(self.inner.conn.read().await, |c| c.as_ref())
            .map_err(|_| Error::ShuttingDown)
    }
}

/// A candle history read window by window ([`CandlePages`] of the runtime), from [`Client::candle_pages`] or
/// [`candle_pages_from_json`](crate::candle_pages_from_json). It reads through any [`Account`]: the default is a
/// [`Client`].
pub struct CandlePager<A: Account = Client> {
    account: A,
    instrument: InstrumentId,
    timeframe: Timeframe,
    next_from: brokapi::core::UnixNanos,
    to: brokapi::core::UnixNanos,
    last_open: Option<brokapi::core::UnixNanos>,
    opts: HistoryOpts,
}

/// One window of a candle history.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CandlePage {
    /// The bars of the window, oldest first, none twice.
    pub bars: Vec<Candle>,
    /// Where the next window starts.
    pub resume_from: brokapi::core::UnixNanos,
}

/// A window that failed.
#[derive(Debug)]
pub struct CandlePageError {
    /// What went wrong: an error of the broker, or [`ClientError::Closed`].
    pub error: ClientError,
    /// The start of the window that failed: read again from here. `None` when the client is closed.
    pub resume_from: Option<brokapi::core::UnixNanos>,
}

impl<A: Account> CandlePager<A> {
    /// A reader of `timeframe` bars of `instrument` from `range.from` to `range.to`.
    pub fn new(
        account: A,
        instrument: InstrumentId,
        timeframe: Timeframe,
        range: TimeRange,
        opts: HistoryOpts,
    ) -> Self {
        Self {
            account,
            instrument,
            timeframe,
            next_from: range.from,
            to: range.to,
            last_open: None,
            opts,
        }
    }

    /// The next window, or `None` when the range is read.
    pub async fn next_page(&mut self) -> Result<Option<CandlePage>, CandlePageError> {
        if self.account.is_closed() {
            return Err(CandlePageError {
                error: ClientError::Closed,
                resume_from: None,
            });
        }
        let range = TimeRange::new(self.next_from, self.to);
        let mut pages = CandlePages::new(
            &self.account,
            &self.instrument,
            self.timeframe,
            range,
            self.opts,
        );
        match pages.next_page().await {
            Ok(None) => Ok(None),
            Ok(Some(mut bars)) => {
                if let Some(last) = self.last_open {
                    bars.retain(|c| c.open_ts > last);
                }
                if let Some(c) = bars.last() {
                    self.last_open = Some(c.open_ts);
                }
                self.next_from = pages.resume_from();
                Ok(Some(CandlePage {
                    bars,
                    resume_from: self.next_from,
                }))
            }
            Err(e) if matches!(e.error, Error::ShuttingDown) && self.account.is_closed() => {
                Err(CandlePageError {
                    error: ClientError::Closed,
                    resume_from: None,
                })
            }
            Err(e) => Err(CandlePageError {
                error: ClientError::Broker(e.error),
                resume_from: Some(e.resume_from),
            }),
        }
    }
}
