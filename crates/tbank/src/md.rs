//! T-Bank market data connector
//!
//! [`MarketDataConnector`] is the market data side of the T-Bank connector. It reads order books,
//! trades, quotes, candles and the instrument directory. It is not tied to an account.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use brokapi_core::{
    Candle, Envelope, Error, Instrument, InstrumentId, InstrumentKind, InstrumentQuery,
    InstrumentSource, MarketDataCaps, MarketDataSource, MarketTrade, NativeId, OrderBook,
    OrderBookSource, Schedule, ShutdownMode, ShutdownReport, StreamingCaps, SubscribeOpts,
    SubscriptionId, TimeRange, Timeframe, TradingParams, TradingStatus, UnixNanos,
};
use brokapi_runtime::BrokerLimiter;
use brokapi_sansio::{
    BrokerCore, DesiredSubscription, EventSink, SubscriptionKind, SubscriptionSet,
};
use brokapi_tbank_protocol::TbankClient;
use brokapi_tbank_protocol::v1 as pb;
use tokio::sync::{Notify, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::Request;
use tracing::warn;

use crate::config::MarketDataConfig;
use crate::core::md::{MdCore, MdFrame};
use crate::emit::Emitter;
use crate::error::{config, transport};
use crate::ids::{instrument_ref, nanos_to_proto, stamps};
use crate::map::{instrument as map_instrument, market, schedule as map_schedule};
use crate::raw::TbankRaw;
use crate::stream::{LastError, backoff, sleep_or_cancel};
use crate::util::{SubIdGen, query};

const DEFAULT_DEPTH: u32 = 20;

struct Shared {
    client: TbankClient,
    limiter: Arc<BrokerLimiter>,
    emitter: Arc<Emitter>,
    core: Mutex<MdCore>,
    waiters: Mutex<HashMap<SubscriptionId, oneshot::Sender<Result<(), String>>>>,
    subs: Mutex<Vec<DesiredSubscription>>,
    sub_ids: SubIdGen,
    notify: Notify,
    last_error: Arc<LastError>,
    request_timeout: Duration,
    stale_after: Duration,
    closing: AtomicBool,
    cancel: CancellationToken,
}

impl Shared {
    /// Runs `f` on the core, then delivers what it produced (events, subscription results).
    fn with_core<R>(&self, f: impl FnOnce(&mut MdCore, &mut EventSink) -> R) -> R {
        let mut sink = EventSink::new();
        let r = {
            let mut core = self.core.lock().expect("md core poisoned");
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
            r
        };
        self.emitter.emit(sink.drain());
        r
    }

    fn forward(core: &mut MdCore, tx: &mpsc::UnboundedSender<pb::MarketDataRequest>) {
        while let Some(req) = core.poll_request() {
            let _ = tx.send(req);
        }
    }

    /// Owns the stream: open, send subscriptions, read, reconnect.
    async fn run(self: Arc<Self>) {
        let mut attempt = 0u32;
        loop {
            if self.cancel.is_cancelled() {
                return;
            }
            let (req_tx, req_rx) = mpsc::unbounded_channel::<pb::MarketDataRequest>();
            self.with_core(|c, s| {
                c.on_frame(MdFrame::Connected, Instant::now(), s);
                Self::forward(c, &req_tx);
            });
            let mut client = self.client.market_data_stream();
            let call =
                client.market_data_stream(Request::new(UnboundedReceiverStream::new(req_rx)));
            let opened = tokio::select! {
                _ = self.cancel.cancelled() => return,
                r = call => r,
            };
            let reason = match opened {
                Err(status) => format!("open failed: {}: {}", status.code(), status.message()),
                Ok(resp) => {
                    attempt = 0;
                    self.last_error.clear();
                    match self.read_loop(resp.into_inner(), &req_tx).await {
                        Some(reason) => reason,
                        None => return,
                    }
                }
            };
            warn!(%reason, "market data stream down, reconnecting");
            self.last_error.set(format!("market data stream: {reason}"));
            drop(req_tx);
            if !sleep_or_cancel(backoff(attempt), &self.cancel).await {
                return;
            }
            attempt = attempt.saturating_add(1);
        }
    }

    /// `None` = cancelled, `Some(reason)` = the stream ended.
    async fn read_loop(
        &self,
        mut stream: tonic::Streaming<pb::MarketDataResponse>,
        req_tx: &mpsc::UnboundedSender<pb::MarketDataRequest>,
    ) -> Option<String> {
        let mut last_rx = tokio::time::Instant::now();
        loop {
            let retry_in = {
                let core = self.core.lock().expect("md core poisoned");
                core.poll_timeout()
                    .map(|t| t.saturating_duration_since(Instant::now()))
            };
            let backlog = self.emitter.has_backlog();
            tokio::select! {
                _ = self.cancel.cancelled() => return None,
                _ = self.notify.notified() => {
                    self.with_core(|c, _| Self::forward(c, req_tx));
                }
                m = stream.message() => match m {
                    Ok(Some(msg)) => {
                        last_rx = tokio::time::Instant::now();
                        self.with_core(|c, s| {
                            c.on_frame(MdFrame::Response(msg), Instant::now(), s);
                            Self::forward(c, req_tx);
                        });
                    }
                    Ok(None) => return Some("closed by the server".into()),
                    Err(status) => return Some(format!("{}: {}", status.code(), status.message())),
                },
                _ = tokio::time::sleep_until(last_rx + self.stale_after) => {
                    return Some(format!("silent for {:?}", self.stale_after));
                }
                _ = tokio::time::sleep(retry_in.unwrap_or(Duration::from_secs(3600))), if retry_in.is_some() => {
                    self.with_core(|c, s| {
                        c.on_timeout(Instant::now(), s);
                        Self::forward(c, req_tx);
                    });
                }
                _ = tokio::time::sleep(Duration::from_millis(20)), if backlog => {
                    self.emitter.flush();
                }
            }
        }
    }
}

/// T-Bank market data connector: order books, trades, quotes, candles and the instrument directory.
///
/// One connector per token is enough, it is not tied to an account. It implements
/// [`OrderBookSource`], [`MarketDataSource`] and [`InstrumentSource`]. Requests return their answer
/// directly. Subscriptions deliver [`Envelope`]s into the channel given to [`start`](Self::start).
///
/// How T-Bank differs from the other brokers:
///
/// - All market data streams run over ONE bidirectional gRPC stream, which the connector keeps open
///   and reopens with a growing delay after a break. After a reopen the subscriptions are sent again
///   and an order book subscription gets an [`Event::Resync`](brokapi_core::Event::Resync).
/// - Every order book message is a full snapshot, there are no increments.
/// - Quotes are the best level of the order book. T-Bank has no separate quote stream.
/// - Quantities of the order book, trades and candles are in LOTS. Prices are per one instrument.
/// - Depths the broker accepts are 1, 10, 20, 30, 40 and 50. Other depths are rounded up on the wire
///   and the events are cut to the depth you asked for.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use brokapi_core::{
///     Event, InstrumentId, InstrumentKey, NativeId, OrderBook, OrderBookSource, ShutdownMode, SubscribeOpts,
/// };
/// use brokapi_runtime::BrokerLimiter;
/// use brokapi_tbank::limits::default_limits;
/// use brokapi_tbank::{MarketDataConfig, MarketDataConnector};
/// use tokio::sync::mpsc;
/// # use brokapi_mocks::tbank::{MockServer, TOKEN};
/// # use brokapi_tbank_protocol::TbankConfig;
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let server = MockServer::start(Default::default()).await;
/// # let cfg = TbankConfig::for_mock(TOKEN, &server.endpoint);
/// let (events_tx, mut events) = mpsc::channel(256);
/// let limiter = BrokerLimiter::new(&default_limits());
/// let md = MarketDataConnector::start(MarketDataConfig::new(cfg, "tbank-md"), limiter, events_tx).await?;
///
/// // The uid comes from the instrument directory, see `InstrumentSource::find`.
/// let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::Uid("uid-sber".into()));
///
/// // A snapshot by request.
/// let OrderBook::Snapshot { bids, asks, .. } = md.order_book(&sber, 10).await? else {
///     panic!("T-Bank answers a snapshot")
/// };
/// assert!(!bids.is_empty() && !asks.is_empty());
///
/// // A stream. The call returns once the broker has confirmed the subscription.
/// let sub = md.subscribe_order_book(&sber, SubscribeOpts::conflate()).await?;
/// let env = tokio::time::timeout(Duration::from_secs(5), async {
///     loop {
///         let env = events.recv().await.expect("the channel stays open");
///         if matches!(env.payload, Event::OrderBook(_)) {
///             break env;
///         }
///     }
/// })
/// .await?;
/// assert_eq!(env.sub, Some(sub));
///
/// md.unsubscribe(sub).await?;
/// md.shutdown(ShutdownMode::Immediate).await;
/// # Ok(())
/// # }
/// ```
pub struct MarketDataConnector {
    inner: Arc<Shared>,
    pump: JoinHandle<()>,
}

impl MarketDataConnector {
    /// Builds the channel and starts the stream task.
    ///
    /// The task opens the market data stream in the background at once and reopens it after a break
    /// (delay from 500 ms doubling to 30 s). `start` does not wait for it and does not fail when the
    /// broker cannot be reached: the first request or subscription reports that. A subscription
    /// waits for the broker to confirm it, for at most `request_timeout`.
    ///
    /// `limiter` should be the one limiter of the whole user, shared with every other connector
    /// (see [`crate::limits`]). Events go to `out`.
    ///
    /// # Errors
    ///
    /// [`Error::Config`] when the token or app name is not a valid header value, or the endpoint is
    /// not a valid URL. Nothing was started in that case.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::{Error, ShutdownMode};
    /// use brokapi_runtime::BrokerLimiter;
    /// use brokapi_tbank::limits::default_limits;
    /// use brokapi_tbank::{MarketDataConfig, MarketDataConnector};
    /// use tokio::sync::mpsc;
    /// # use brokapi_mocks::tbank::{MockServer, TOKEN};
    /// # use brokapi_tbank_protocol::TbankConfig;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = MockServer::start(Default::default()).await;
    /// # let cfg = TbankConfig::for_mock(TOKEN, &server.endpoint);
    /// let limiter = BrokerLimiter::new(&default_limits());
    /// let (events_tx, _events) = mpsc::channel(256);
    ///
    /// // A bad endpoint is a configuration error at start.
    /// let mut bad = cfg.clone();
    /// bad.endpoint = "not a url".into();
    /// let refused = MarketDataConnector::start(MarketDataConfig::new(bad, "tbank-md"), limiter.clone(), events_tx.clone()).await;
    /// assert!(matches!(refused, Err(Error::Config(_))));
    ///
    /// let md = MarketDataConnector::start(MarketDataConfig::new(cfg, "tbank-md"), limiter, events_tx).await?;
    /// md.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn start(
        cfg: MarketDataConfig,
        limiter: Arc<BrokerLimiter>,
        out: mpsc::Sender<Envelope>,
    ) -> Result<Self, Error> {
        let client = TbankClient::new(&cfg.tbank).map_err(|e| config(e.to_string()))?;
        let inner = Arc::new(Shared {
            client,
            limiter,
            emitter: Emitter::new(cfg.source, out),
            core: Mutex::new(MdCore::new(cfg.ping_interval)),
            waiters: Mutex::new(HashMap::new()),
            subs: Mutex::new(Vec::new()),
            sub_ids: SubIdGen::new(0),
            notify: Notify::new(),
            last_error: Arc::new(LastError::default()),
            request_timeout: cfg.tbank.request_timeout,
            stale_after: cfg.ping_interval * 3,
            closing: AtomicBool::new(false),
            cancel: CancellationToken::new(),
        });
        let pump = tokio::spawn(Arc::clone(&inner).run());
        Ok(Self { inner, pump })
    }

    /// Any other method of the T-Bank API with this connector's token, endpoint and rate limiter.
    /// See [`TbankRaw`].
    ///
    /// The raw layer is not blocked by the connector's shutdown gate and does not know the
    /// connector's subscriptions. Its calls spend quota from the same limiter as the connector's own.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_runtime::BrokerLimiter;
    /// use brokapi_tbank::limits::default_limits;
    /// use brokapi_tbank::{MarketDataConfig, MarketDataConnector};
    /// use brokapi_tbank_protocol::v1 as pb;
    /// use tokio::sync::mpsc;
    /// # use brokapi_core::ShutdownMode;
    /// # use brokapi_mocks::tbank::{MockServer, TOKEN};
    /// # use brokapi_tbank_protocol::TbankConfig;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = MockServer::start(Default::default()).await;
    /// # let cfg = TbankConfig::for_mock(TOKEN, &server.endpoint);
    /// let (events_tx, _events) = mpsc::channel(64);
    /// let md = MarketDataConnector::start(
    ///     MarketDataConfig::new(cfg, "tbank-md"),
    ///     BrokerLimiter::new(&default_limits()),
    ///     events_tx,
    /// )
    /// .await?;
    ///
    /// // Last prices of several instruments at once: no shared trait has this call.
    /// let prices = md
    ///     .raw()
    ///     .call("GetLastPrices", |c| async move {
    ///         c.market_data()
    ///             .get_last_prices(pb::GetLastPricesRequest {
    ///                 instrument_id: vec!["SBER_TQBR".into()],
    ///                 ..Default::default()
    ///             })
    ///             .await
    ///     })
    ///     .await?;
    /// assert_eq!(prices.last_prices.len(), 1);
    /// # md.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    pub fn raw(&self) -> TbankRaw {
        TbankRaw::new(
            self.inner.client.clone(),
            Arc::clone(&self.inner.limiter),
            self.inner.request_timeout,
            None,
        )
    }

    /// What this connector can do.
    ///
    /// The order book is at most 50 levels deep ([`MarketDataCaps::max_order_book_depth`]). Every stream
    /// message is a full snapshot (`incremental_order_book` is `false`). Quotes are the best level of
    /// the book, since T-Bank has no separate quote stream. Candles stream as they form.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_runtime::BrokerLimiter;
    /// use brokapi_tbank::limits::default_limits;
    /// use brokapi_tbank::{MarketDataConfig, MarketDataConnector};
    /// use tokio::sync::mpsc;
    /// # use brokapi_core::ShutdownMode;
    /// # use brokapi_mocks::tbank::{MockServer, TOKEN};
    /// # use brokapi_tbank_protocol::TbankConfig;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = MockServer::start(Default::default()).await;
    /// # let cfg = TbankConfig::for_mock(TOKEN, &server.endpoint);
    /// let (events_tx, _events) = mpsc::channel(64);
    /// let md = MarketDataConnector::start(
    ///     MarketDataConfig::new(cfg, "tbank-md"),
    ///     BrokerLimiter::new(&default_limits()),
    ///     events_tx,
    /// )
    /// .await?;
    /// let caps = md.caps();
    /// assert_eq!(caps.max_order_book_depth, 50);
    /// assert!(!caps.incremental_order_book);
    /// assert!(caps.streaming.quotes && caps.streaming.candles);
    /// # md.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    pub fn caps(&self) -> MarketDataCaps {
        MarketDataCaps {
            max_order_book_depth: market::MAX_DEPTH,
            incremental_order_book: false,
            streaming: StreamingCaps {
                order_book: true,
                market_trades: true,
                quotes: true,
                candles: true,
            },
        }
    }

    /// Stops the connector and its stream task.
    ///
    /// With [`ShutdownMode::Graceful`] the connector first unsubscribes every wire subscription and
    /// waits for the broker to confirm, until the deadline. Subscriptions not confirmed by then count
    /// as `unsubscribe_failed`. It also gives the event backlog the same deadline to drain into your
    /// channel. With [`ShutdownMode::Immediate`] it just stops, and the report counts nothing.
    ///
    /// Market data has no commands, so `inflight_abandoned` is always empty. The method consumes the
    /// connector, so no further call is possible. Closing the stream is the only cleanup the broker
    /// needs.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use brokapi_core::{InstrumentId, InstrumentKey, NativeId, OrderBookSource, ShutdownMode, SubscribeOpts};
    /// use brokapi_runtime::BrokerLimiter;
    /// use brokapi_tbank::limits::default_limits;
    /// use brokapi_tbank::{MarketDataConfig, MarketDataConnector};
    /// use tokio::sync::mpsc;
    /// # use brokapi_mocks::tbank::{MockServer, TOKEN};
    /// # use brokapi_tbank_protocol::TbankConfig;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = MockServer::start(Default::default()).await;
    /// # let cfg = TbankConfig::for_mock(TOKEN, &server.endpoint);
    /// let (events_tx, _events) = mpsc::channel(256);
    /// let md = MarketDataConnector::start(
    ///     MarketDataConfig::new(cfg, "tbank-md"),
    ///     BrokerLimiter::new(&default_limits()),
    ///     events_tx,
    /// )
    /// .await?;
    /// let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::Uid("uid-sber".into()));
    /// md.subscribe_order_book(&sber, SubscribeOpts::conflate()).await?;
    ///
    /// let report = md.shutdown(ShutdownMode::Graceful { deadline: Duration::from_secs(2) }).await;
    /// assert!(report.inflight_abandoned.is_empty());
    /// println!("unsubscribed {}, failed {}", report.unsubscribed, report.unsubscribe_failed);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn shutdown(self, mode: ShutdownMode) -> ShutdownReport {
        let inner = &self.inner;
        inner.closing.store(true, Ordering::SeqCst);
        let (mut ok, mut failed) = (0, 0);
        if let ShutdownMode::Graceful { deadline } = mode {
            // Protocol unsubscribe for every wire subscription, then wait for the confirmations.
            inner.subs.lock().expect("subs poisoned").clear();
            inner.with_core(|c, _| c.apply(&SubscriptionSet::new()));
            inner.notify.notify_one();
            let end = Instant::now() + deadline;
            loop {
                let pending = inner
                    .core
                    .lock()
                    .expect("md core poisoned")
                    .unsubscribe_progress()
                    .2;
                if pending == 0 || Instant::now() >= end {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            {
                let mut core = inner.core.lock().expect("md core poisoned");
                core.give_up_unsubscribing();
                let (o, f, _) = core.unsubscribe_progress();
                ok = o;
                failed = f;
            }
            let end = Instant::now() + deadline;
            while inner.emitter.has_backlog() && Instant::now() < end {
                inner.emitter.flush();
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }
        inner.cancel.cancel();
        let _ = self.pump.await;
        ShutdownReport {
            unsubscribed: ok,
            unsubscribe_failed: failed,
            inflight_abandoned: Vec::new(),
        }
    }

    fn acquire(&self, method: &str) -> Result<(), Error> {
        if self.inner.closing.load(Ordering::SeqCst) {
            return Err(Error::ShuttingDown);
        }
        self.inner.limiter.acquire(method, Instant::now())
    }

    /// Registers a logical subscription and waits until the broker confirms (or refuses) it.
    async fn subscribe(
        &self,
        instrument: &InstrumentId,
        kind: SubscriptionKind,
        opts: SubscribeOpts,
    ) -> Result<SubscriptionId, Error> {
        self.acquire("SubscribeMarketData")?;
        let inner = &self.inner;
        let id = inner.sub_ids.next();
        inner
            .emitter
            .register(id, Some(instrument.clone()), opts.backpressure);
        let (tx, rx) = oneshot::channel();
        inner
            .waiters
            .lock()
            .expect("waiters poisoned")
            .insert(id, tx);
        self.set_desired(|subs| subs.push(DesiredSubscription { id, kind }));

        let outcome = tokio::time::timeout(inner.request_timeout, rx).await;
        match outcome {
            Ok(Ok(Ok(()))) => Ok(id),
            Ok(Ok(Err(reason))) => {
                self.drop_sub(id);
                Err(Error::Rejected {
                    code: reason,
                    message: format!("subscription to {} refused", instrument.key.display()),
                })
            }
            _ => {
                self.drop_sub(id);
                let why = inner
                    .last_error
                    .get()
                    .unwrap_or_else(|| "no answer from the stream".into());
                Err(transport(format!(
                    "subscription to {} not confirmed within {:?} ({why})",
                    instrument.key.display(),
                    inner.request_timeout
                )))
            }
        }
    }

    fn set_desired(&self, f: impl FnOnce(&mut Vec<DesiredSubscription>)) {
        let set = {
            let mut subs = self.inner.subs.lock().expect("subs poisoned");
            f(&mut subs);
            SubscriptionSet::with_subscriptions(subs.clone())
        };
        self.inner.with_core(|c, _| c.apply(&set));
        self.inner.notify.notify_one();
    }

    fn drop_sub(&self, id: SubscriptionId) {
        self.inner
            .waiters
            .lock()
            .expect("waiters poisoned")
            .remove(&id);
        self.set_desired(|subs| subs.retain(|s| s.id != id));
        self.inner.emitter.unregister(id);
    }

    fn id_ref(id: &InstrumentId) -> String {
        instrument_ref(id)
    }
}

#[async_trait]
impl OrderBookSource for MarketDataConnector {
    /// One snapshot by request (`GetOrderBook`).
    ///
    /// `depth` is the number of levels you want. It is sent rounded up to a depth the broker accepts
    /// (1, 10, 20, 30, 40 or 50) and the answer is cut back to `depth` levels, best first. A depth of 0
    /// counts as 1 and a depth above 50 is capped at 50 on the wire. Quantities are in lots.
    ///
    /// # Errors
    ///
    /// - [`Error::ShuttingDown`] after shutdown started.
    /// - [`Error::RateLimited`] when the local limiter or the broker refuses for rate. Repeat later.
    /// - [`Error::Rejected`] when the broker does not know the instrument (code `NotFound`).
    /// - [`Error::Transport`] on a timeout or a network failure. A read, so repeat it later.
    /// - [`Error::Config`] when the token is not accepted.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::{Error, InstrumentId, InstrumentKey, NativeId, OrderBook, OrderBookSource, ShutdownMode};
    /// use brokapi_runtime::BrokerLimiter;
    /// use brokapi_tbank::limits::default_limits;
    /// use brokapi_tbank::{MarketDataConfig, MarketDataConnector};
    /// use tokio::sync::mpsc;
    /// # use brokapi_mocks::tbank::{MockServer, TOKEN};
    /// # use brokapi_tbank_protocol::TbankConfig;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = MockServer::start(Default::default()).await;
    /// # let cfg = TbankConfig::for_mock(TOKEN, &server.endpoint);
    /// let (events_tx, _events) = mpsc::channel(256);
    /// let md = MarketDataConnector::start(
    ///     MarketDataConfig::new(cfg, "tbank-md"),
    ///     BrokerLimiter::new(&default_limits()),
    ///     events_tx,
    /// )
    /// .await?;
    /// let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::Uid("uid-sber".into()));
    ///
    /// let OrderBook::Snapshot { bids, .. } = md.order_book(&sber, 3).await? else { panic!("a snapshot") };
    /// assert!(bids.len() <= 3);
    ///
    /// // An instrument the broker does not know is a refusal, not a transport error.
    /// let nobody = InstrumentId::new(InstrumentKey::new("NOPE", "TQBR"), NativeId::Uid("no-such-uid".into()));
    /// assert!(matches!(md.order_book(&nobody, 3).await, Err(Error::Rejected { .. })));
    /// # md.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    async fn order_book(&self, id: &InstrumentId, depth: u32) -> Result<OrderBook, Error> {
        self.acquire("GetOrderBook")?;
        let req = pb::GetOrderBookRequest {
            depth: market::wire_depth(depth) as i32,
            instrument_id: Some(Self::id_ref(id)),
            ..Default::default()
        };
        let mut c = self.inner.client.market_data();
        let r = query(self.inner.request_timeout, c.get_order_book(req)).await?;
        Ok(market::book_snapshot(
            id,
            &r.bids,
            &r.asks,
            depth.max(1) as usize,
            r.orderbook_ts.as_ref(),
            Instant::now(),
        ))
    }

    /// Streams the order book as full snapshots.
    ///
    /// `opts.depth` of `None` means 20 levels here (not the 50 maximum). Other values are clamped to
    /// 1..=50 and sent rounded up to a depth the broker accepts. Events are cut to the depth you asked
    /// for. Two subscriptions to one book share one wire subscription (the deeper one wins). The
    /// call returns after the broker confirmed the subscription. After a reconnect you get an
    /// [`Event::Resync`](brokapi_core::Event::Resync) and then fresh snapshots.
    ///
    /// # Errors
    ///
    /// - [`Error::ShuttingDown`] after shutdown started.
    /// - [`Error::RateLimited`] when the subscription quota (`SubscribeMarketData`) is used up.
    /// - [`Error::Rejected`] when the broker refuses the subscription (the code is the broker's
    ///   subscription status, for example `SUBSCRIPTION_STATUS_INSTRUMENT_NOT_FOUND`).
    /// - [`Error::Transport`] when the broker did not confirm within `request_timeout` (the message
    ///   says why the stream is down, if it is).
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::{Error, InstrumentId, InstrumentKey, NativeId, OrderBookSource, ShutdownMode, SubscribeOpts};
    /// use brokapi_runtime::BrokerLimiter;
    /// use brokapi_tbank::limits::default_limits;
    /// use brokapi_tbank::{MarketDataConfig, MarketDataConnector};
    /// use tokio::sync::mpsc;
    /// # use brokapi_mocks::tbank::{MockServer, TOKEN};
    /// # use brokapi_tbank_protocol::TbankConfig;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = MockServer::start(Default::default()).await;
    /// # let cfg = TbankConfig::for_mock(TOKEN, &server.endpoint);
    /// let (events_tx, _events) = mpsc::channel(256);
    /// let md = MarketDataConnector::start(
    ///     MarketDataConfig::new(cfg, "tbank-md"),
    ///     BrokerLimiter::new(&default_limits()),
    ///     events_tx,
    /// )
    /// .await?;
    /// let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::Uid("uid-sber".into()));
    ///
    /// let opts = SubscribeOpts { depth: Some(10), ..SubscribeOpts::conflate() };
    /// let sub = md.subscribe_order_book(&sber, opts).await?;
    /// md.unsubscribe(sub).await?;
    /// // A second unsubscribe is a refusal: the subscription is gone.
    /// assert!(matches!(md.unsubscribe(sub).await, Err(Error::Rejected { .. })));
    /// # md.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    async fn subscribe_order_book(
        &self,
        id: &InstrumentId,
        opts: SubscribeOpts,
    ) -> Result<SubscriptionId, Error> {
        let depth = opts
            .depth
            .unwrap_or(DEFAULT_DEPTH)
            .clamp(1, market::MAX_DEPTH);
        self.subscribe(
            id,
            SubscriptionKind::OrderBook {
                instrument: id.clone(),
                depth,
            },
            opts,
        )
        .await
    }

    /// Ends a subscription made by any `subscribe_*` method.
    ///
    /// The logical subscription is dropped at once and its buffer is closed. The wire subscription is
    /// dropped when no other subscription uses it. Events already queued for you may still arrive.
    ///
    /// # Errors
    ///
    /// [`Error::Rejected`] with code `unknown_subscription` when the id is not active (never made,
    /// or already ended).
    async fn unsubscribe(&self, sub: SubscriptionId) -> Result<(), Error> {
        let known = self
            .inner
            .subs
            .lock()
            .expect("subs poisoned")
            .iter()
            .any(|s| s.id == sub);
        if !known {
            return Err(Error::Rejected {
                code: "unknown_subscription".into(),
                message: format!("subscription {} is not active", sub.value()),
            });
        }
        self.drop_sub(sub);
        Ok(())
    }
}

#[async_trait]
impl MarketDataSource for MarketDataConnector {
    /// Anonymous market trades of the last hour, the newest `limit` of them (`GetLastTrades`).
    ///
    /// One hour is all that T-Bank guarantees, so older trades are not available here. Quantities
    /// are in lots. Trades without a direction are skipped, so you may get fewer than `limit`.
    ///
    /// # Errors
    ///
    /// The same as [`order_book`](OrderBookSource::order_book).
    async fn last_trades(&self, id: &InstrumentId, limit: u32) -> Result<Vec<MarketTrade>, Error> {
        self.acquire("GetLastTrades")?;
        let now = crate::ids::wall_nanos();
        let req = pb::GetLastTradesRequest {
            from: Some(nanos_to_proto(UnixNanos::from_nanos(
                now.as_nanos() - 3_600_000_000_000,
            ))),
            to: Some(nanos_to_proto(now)),
            instrument_id: Some(Self::id_ref(id)),
            ..Default::default()
        };
        let mut c = self.inner.client.market_data();
        let r = query(self.inner.request_timeout, c.get_last_trades(req)).await?;
        let rx = Instant::now();
        let mut trades: Vec<MarketTrade> = r
            .trades
            .iter()
            .filter_map(|t| market::trade(id, t, rx))
            .collect();
        let keep = limit as usize;
        if trades.len() > keep {
            trades.drain(..trades.len() - keep);
        }
        Ok(trades)
    }

    /// Streams anonymous market trades as they happen.
    ///
    /// Each event is one trade. Quantities are in lots. Trades without a direction are skipped.
    ///
    /// # Errors
    ///
    /// The same as [`subscribe_order_book`](OrderBookSource::subscribe_order_book).
    async fn subscribe_market_trades(
        &self,
        id: &InstrumentId,
        opts: SubscribeOpts,
    ) -> Result<SubscriptionId, Error> {
        self.subscribe(
            id,
            SubscriptionKind::LastTrades {
                instrument: id.clone(),
            },
            opts,
        )
        .await
    }

    /// Best bid and ask: the first level of the order book stream.
    ///
    /// T-Bank has no quote stream of its own, so this subscribes to a one-level order book. Every
    /// order book message produces one quote event.
    ///
    /// # Errors
    ///
    /// The same as [`subscribe_order_book`](OrderBookSource::subscribe_order_book).
    async fn subscribe_quotes(
        &self,
        id: &InstrumentId,
        opts: SubscribeOpts,
    ) -> Result<SubscriptionId, Error> {
        self.subscribe(
            id,
            SubscriptionKind::Quotes {
                instrument: id.clone(),
            },
            opts,
        )
        .await
    }

    /// Historic candles of `range` (`GetCandles`).
    ///
    /// `close_ts` of each candle is its open time plus the length of the timeframe (a month is taken
    /// as 30 days). Volumes are in lots. How many candles one call returns, and the maximum range per
    /// timeframe, are broker limits that this method does not split: not verified against the live
    /// API.
    ///
    /// # Errors
    ///
    /// The same as [`order_book`](OrderBookSource::order_book).
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::{
    ///     InstrumentId, InstrumentKey, MarketDataSource, NativeId, ShutdownMode, TimeRange, Timeframe, UnixNanos,
    /// };
    /// use brokapi_runtime::BrokerLimiter;
    /// use brokapi_tbank::limits::default_limits;
    /// use brokapi_tbank::{MarketDataConfig, MarketDataConnector};
    /// use tokio::sync::mpsc;
    /// # use brokapi_mocks::tbank::{MockServer, TOKEN};
    /// # use brokapi_tbank_protocol::TbankConfig;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = MockServer::start(Default::default()).await;
    /// # let cfg = TbankConfig::for_mock(TOKEN, &server.endpoint);
    /// let (events_tx, _events) = mpsc::channel(256);
    /// let md = MarketDataConnector::start(
    ///     MarketDataConfig::new(cfg, "tbank-md"),
    ///     BrokerLimiter::new(&default_limits()),
    ///     events_tx,
    /// )
    /// .await?;
    /// let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::Uid("uid-sber".into()));
    ///
    /// let range = TimeRange::new(UnixNanos::from_nanos(1_700_000_000_000_000_000), UnixNanos::from_nanos(1_700_003_600_000_000_000));
    /// let candles = md.candles(&sber, Timeframe::M5, range).await?;
    /// assert!(candles.iter().all(|c| c.high >= c.low));
    ///
    /// // Newest trades of the last hour, at most `limit` of them.
    /// let trades = md.last_trades(&sber, 2).await?;
    /// assert!(trades.len() <= 2);
    /// # md.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    async fn candles(
        &self,
        id: &InstrumentId,
        tf: Timeframe,
        range: TimeRange,
    ) -> Result<Vec<Candle>, Error> {
        self.acquire("GetCandles")?;
        let req = pb::GetCandlesRequest {
            from: Some(nanos_to_proto(range.from)),
            to: Some(nanos_to_proto(range.to)),
            interval: market::candle_interval(tf) as i32,
            instrument_id: Some(Self::id_ref(id)),
            ..Default::default()
        };
        let mut c = self.inner.client.market_data();
        let r = query(self.inner.request_timeout, c.get_candles(req)).await?;
        let rx = Instant::now();
        Ok(r.candles
            .iter()
            .map(|x| market::historic_candle(id, tf, x, rx))
            .collect())
    }

    /// The longest span one `GetCandles` request may cover, per timeframe, from the T-Invest
    /// documentation page "Исторические котировки" (1 minute: 1 day, 5 and 10 minutes: 1 week,
    /// 15 and 30 minutes: 3 weeks, 1 and 4 hours: 3 months, 1 day: 6 years, 1 week: 5 years,
    /// 1 month: 10 years). The broker calls the limits approximate. Not checked against the live API.
    fn candles_max_span(&self, tf: Timeframe) -> Option<Duration> {
        const DAY: u64 = 86_400;
        let days = match tf {
            Timeframe::M1 => 1,
            Timeframe::M5 | Timeframe::M10 => 7,
            Timeframe::M15 | Timeframe::M30 => 21,
            Timeframe::H1 | Timeframe::H4 => 90,
            Timeframe::D1 => 6 * 365,
            Timeframe::W1 => 5 * 365,
            Timeframe::MN => 10 * 365,
        };
        Some(Duration::from_secs(days * DAY))
    }

    /// Streams the candle that is forming, for timeframe `tf`.
    ///
    /// Each event is the current state of the last candle, not only closed ones. Volumes are in lots.
    ///
    /// # Errors
    ///
    /// The same as [`subscribe_order_book`](OrderBookSource::subscribe_order_book).
    async fn subscribe_candles(
        &self,
        id: &InstrumentId,
        tf: Timeframe,
        opts: SubscribeOpts,
    ) -> Result<SubscriptionId, Error> {
        self.subscribe(
            id,
            SubscriptionKind::LastCandle {
                instrument: id.clone(),
                timeframe: tf,
            },
            opts,
        )
        .await
    }
}

fn instrument_request(id: &InstrumentId) -> pb::InstrumentRequest {
    match &id.native {
        NativeId::Uid(u) => pb::InstrumentRequest {
            id_type: pb::InstrumentIdType::Uid as i32,
            id: u.clone(),
            class_code: None,
        },
        NativeId::Figi(f) => pb::InstrumentRequest {
            id_type: pb::InstrumentIdType::Figi as i32,
            id: f.clone(),
            class_code: None,
        },
        NativeId::TickerClass { ticker, class_code } => pb::InstrumentRequest {
            id_type: pb::InstrumentIdType::Ticker as i32,
            id: ticker.clone(),
            class_code: Some(class_code.clone()),
        },
        NativeId::SymbolMic { symbol, mic } => pb::InstrumentRequest {
            id_type: pb::InstrumentIdType::Ticker as i32,
            id: symbol.clone(),
            class_code: Some(mic.clone()),
        },
    }
}

impl MarketDataConnector {
    /// Whole-kind listing. Base status: what the API can trade (the default of the broker).
    async fn list_kind(&self, kind: InstrumentKind) -> Result<Vec<Instrument>, Error> {
        let method = match kind {
            InstrumentKind::Stock => "Shares",
            InstrumentKind::Bond => "Bonds",
            InstrumentKind::Etf => "Etfs",
            InstrumentKind::Future => "Futures",
            InstrumentKind::Option => "Options",
            InstrumentKind::Currency => "Currencies",
            InstrumentKind::MutualFund => {
                return Err(Error::Unsupported {
                    capability: "find_by_kind: T-Bank has no listing of mutual funds",
                });
            }
            InstrumentKind::Other => {
                return Err(Error::Unsupported {
                    capability: "find_by_kind: kind Other has no T-Bank listing (indicatives, assets are not requested)",
                });
            }
        };
        self.acquire(method)?;
        let req = pb::InstrumentsRequest {
            instrument_status: Some(pb::InstrumentStatus::Base as i32),
            instrument_exchange: None,
        };
        let t = self.inner.request_timeout;
        let mut c = self.inner.client.instruments();
        Ok(match kind {
            InstrumentKind::Stock => query(t, c.shares(req))
                .await?
                .instruments
                .iter()
                .map(map_instrument::from_share)
                .collect(),
            InstrumentKind::Bond => query(t, c.bonds(req))
                .await?
                .instruments
                .iter()
                .map(map_instrument::from_bond)
                .collect(),
            InstrumentKind::Etf => query(t, c.etfs(req))
                .await?
                .instruments
                .iter()
                .map(map_instrument::from_etf)
                .collect(),
            InstrumentKind::Future => query(t, c.futures(req))
                .await?
                .instruments
                .iter()
                .map(map_instrument::from_future)
                .collect(),
            InstrumentKind::Option => {
                // `Options` is marked deprecated in the proto, but its replacement `OptionsBy` demands a
                // base asset, which a whole-kind query does not have. If T-Bank removes it, this becomes
                // Unsupported (a live question, see the live runs page of the book).
                #[allow(deprecated)]
                let r = c.options(req);
                query(t, r)
                    .await?
                    .instruments
                    .iter()
                    .map(map_instrument::from_option)
                    .collect()
            }
            _ => query(t, c.currencies(req))
                .await?
                .instruments
                .iter()
                .map(map_instrument::from_currency)
                .collect(),
        })
    }

    async fn instrument_by(&self, id: &InstrumentId) -> Result<pb::Instrument, Error> {
        self.acquire("GetInstrumentBy")?;
        let mut c = self.inner.client.instruments();
        let r = query(
            self.inner.request_timeout,
            c.get_instrument_by(instrument_request(id)),
        )
        .await?;
        r.instrument.ok_or_else(|| Error::Rejected {
            code: "NotFound".into(),
            message: format!("instrument {} not found", id.key.display()),
        })
    }
}

#[async_trait]
impl InstrumentSource for MarketDataConnector {
    /// Searches the instrument directory by ticker, ISIN or kind.
    ///
    /// `ByTicker` / `ByIsin` use `FindInstrument` (short records: `currency` is empty, use `get`).
    /// `ByKind` calls the listing of that kind: `Shares`, `Bonds`, `Etfs`, `Futures`, `Options`,
    /// `Currencies` (base list, the instruments tradable through the API). They are heavy (a few thousand
    /// records) and limited to 15 requests a minute: the limiter has its own bucket `instruments_list`,
    /// nothing is cached here. `MutualFund` / `Other` are `Unsupported`: T-Bank has no listing for them.
    ///
    /// # Errors
    ///
    /// - [`Error::Unsupported`] for `MutualFund` and `Other`, without a network call.
    /// - [`Error::RateLimited`] when a quota is used up. The listing quota is small (15 a minute).
    /// - [`Error::Transport`], [`Error::Config`] and [`Error::Rejected`] as for
    ///   [`order_book`](OrderBookSource::order_book).
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::{
    ///     Error, InstrumentId, InstrumentKey, InstrumentKind, InstrumentQuery, InstrumentSource, NativeId,
    ///     ShutdownMode,
    /// };
    /// use brokapi_runtime::BrokerLimiter;
    /// use brokapi_tbank::limits::default_limits;
    /// use brokapi_tbank::{MarketDataConfig, MarketDataConnector};
    /// use tokio::sync::mpsc;
    /// # use brokapi_mocks::tbank::{MockServer, TOKEN};
    /// # use brokapi_tbank_protocol::TbankConfig;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = MockServer::start(Default::default()).await;
    /// # let cfg = TbankConfig::for_mock(TOKEN, &server.endpoint);
    /// let (events_tx, _events) = mpsc::channel(256);
    /// let md = MarketDataConnector::start(
    ///     MarketDataConfig::new(cfg, "tbank-md"),
    ///     BrokerLimiter::new(&default_limits()),
    ///     events_tx,
    /// )
    /// .await?;
    /// let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::Uid("uid-sber".into()));
    ///
    /// // Search by ticker. A short record has no currency: use `get` for the full one.
    /// let found = md
    ///     .find(InstrumentQuery::ByTicker { ticker: "SBER".into(), exchange: None })
    ///     .await?;
    /// assert_eq!(found[0].id.key.symbol, "SBER");
    /// let full = md.get(&found[0].id).await?;
    /// assert_eq!(full.currency, "rub");
    ///
    /// let params = md.trading_params(&sber).await?;
    /// assert_eq!(params.min_qty.to_string(), "1");
    ///
    /// // T-Bank has no listing of mutual funds.
    /// let funds = md.find(InstrumentQuery::ByKind(InstrumentKind::MutualFund)).await;
    /// assert!(matches!(funds, Err(Error::Unsupported { .. })));
    /// # md.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    async fn find(&self, q: InstrumentQuery) -> Result<Vec<Instrument>, Error> {
        let (text, ticker, class, isin) = match q {
            InstrumentQuery::ByTicker { ticker, exchange } => {
                (ticker.clone(), Some(ticker), exchange, None)
            }
            InstrumentQuery::ByIsin(isin) => (isin.clone(), None, None, Some(isin)),
            InstrumentQuery::ByKind(kind) => return self.list_kind(kind).await,
        };
        self.acquire("FindInstrument")?;
        let req = pb::FindInstrumentRequest {
            query: text,
            instrument_kind: None,
            api_trade_available_flag: None,
        };
        let mut c = self.inner.client.instruments();
        let r = query(self.inner.request_timeout, c.find_instrument(req)).await?;
        Ok(r.instruments
            .iter()
            .filter(|i| {
                ticker
                    .as_ref()
                    .is_none_or(|t| i.ticker.eq_ignore_ascii_case(t))
            })
            .filter(|i| {
                class
                    .as_ref()
                    .is_none_or(|c| i.class_code.eq_ignore_ascii_case(c))
            })
            .filter(|i| isin.as_ref().is_none_or(|x| i.isin.eq_ignore_ascii_case(x)))
            .map(map_instrument::instrument_from_short)
            .collect())
    }

    /// The full record of one instrument (`GetInstrumentBy`), including its currency.
    ///
    /// # Errors
    ///
    /// [`Error::Rejected`] (code `NotFound`) when the broker does not know the instrument, and the
    /// other errors of [`order_book`](OrderBookSource::order_book).
    async fn get(&self, id: &InstrumentId) -> Result<Instrument, Error> {
        Ok(map_instrument::instrument_from_full(
            &self.instrument_by(id).await?,
        ))
    }

    /// Lot size, price step and currency of an instrument.
    ///
    /// `lot_size` is the pieces in one lot. `min_qty` is always 1 lot, because orders are sized in
    /// lots. `price_step` and `price_step_value` are both the instrument's minimal price increment
    /// (for futures the step in money would need another call, which is not made).
    ///
    /// # Errors
    ///
    /// The same as [`get`](InstrumentSource::get).
    async fn trading_params(&self, id: &InstrumentId) -> Result<TradingParams, Error> {
        let i = self.instrument_by(id).await?;
        Ok(map_instrument::trading_params(id, &i))
    }

    /// Whether trading is going on now (`GetTradingStatus`).
    ///
    /// The broker's status is mapped to open, break, closed or unknown. Opening and closing periods
    /// and auctions are `Unknown`, because orders are possible there only with restrictions. The
    /// raw broker status name is in `reason`.
    ///
    /// # Errors
    ///
    /// The same as [`get`](InstrumentSource::get).
    async fn trading_status(&self, id: &InstrumentId) -> Result<TradingStatus, Error> {
        self.acquire("GetTradingStatus")?;
        let req = pb::GetTradingStatusRequest {
            instrument_id: Some(Self::id_ref(id)),
            ..Default::default()
        };
        let mut c = self.inner.client.market_data();
        let r = query(self.inner.request_timeout, c.get_trading_status(req)).await?;
        Ok(map_instrument::trading_status(id, r.trading_status()))
    }

    /// Trading sessions of an exchange in `range` (`TradingSchedules`).
    ///
    /// `exchange` is the broker's exchange name (for example `MOEX`). If the broker has no schedule
    /// with that name, the first one in the answer is used. An empty answer gives a schedule with no
    /// sessions.
    ///
    /// # Errors
    ///
    /// The same as [`order_book`](OrderBookSource::order_book).
    async fn schedule(&self, exchange: &str, range: TimeRange) -> Result<Schedule, Error> {
        self.acquire("TradingSchedules")?;
        let req = pb::TradingSchedulesRequest {
            exchange: (!exchange.is_empty()).then(|| exchange.to_string()),
            from: Some(nanos_to_proto(range.from)),
            to: Some(nanos_to_proto(range.to)),
        };
        let mut c = self.inner.client.instruments();
        let r = query(self.inner.request_timeout, c.trading_schedules(req)).await?;
        let found = r
            .exchanges
            .iter()
            .find(|s| s.exchange.eq_ignore_ascii_case(exchange))
            .or_else(|| r.exchanges.first());
        match found {
            Some(s) => Ok(map_schedule::schedule(exchange, s, Instant::now())),
            None => Ok(Schedule {
                exchange: exchange.to_string(),
                sessions: Vec::new(),
                ts: stamps(Instant::now(), None),
            }),
        }
    }
}
