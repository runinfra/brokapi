//! The capability traits: what a connector can do, split by what it needs.
//!
//! A market data connector implements [`OrderBookSource`], [`MarketDataSource`] and
//! [`InstrumentSource`]. A user data connector (orders, fills, portfolio) implements
//! [`OrderExecutor`], [`ExecutionSource`], [`PortfolioSource`] and [`StopOrderExecutor`]. Write your
//! code against the traits and it runs on every broker.
//!
//! Rules that hold for all of them:
//! - A call that reads or commands returns its answer directly. A subscription call returns a
//!   [`SubscriptionId`] and the events come later as [`Envelope`](crate::Envelope)s in the channel
//!   given to the connector at start.
//! - A capability the broker lacks answers [`Error::Unsupported`] at once, without a network call.
//!   Read the connector's caps ([`MarketDataCaps`](crate::MarketDataCaps),
//!   [`UserDataCaps`](crate::UserDataCaps)) to know in advance.
//! - A trading command is never repeated by the library. If the outcome is unknown the error is
//!   [`Error::Unknown`], see there what to do.
//! - Trading commands are refused with [`Error::NotReconciled`] until the first successful
//!   [`OrderExecutor::reconcile`], with [`Error::Degraded`] after a lost event, and with
//!   [`Error::ShuttingDown`] during shutdown. Reading calls are not gated by this.

use async_trait::async_trait;

use crate::error::Error;
use crate::types::{
    Balance, Candle, ClientOrderId, Execution, Instrument, InstrumentId, InstrumentQuery,
    MarketTrade, OrderAck, OrderBook, OrderSnapshot, PlaceOrderRequest, PlaceStopOrderRequest,
    Portfolio, Position, ReconcileReport, ReplaceOrderRequest, Schedule, StopOrderRef,
    StopOrderSnapshot, SubscribeOpts, SubscriptionId, TimeRange, Timeframe, TradingParams,
    TradingStatus,
};

/// Order book snapshot and stream.
///
/// [`order_book`](Self::order_book) reads the book once and answers an
/// [`OrderBook::Snapshot`]. [`subscribe_order_book`](Self::subscribe_order_book) starts a stream:
/// the books arrive as [`Event::OrderBook`](crate::Event::OrderBook) in the channel given to the
/// connector at start, tagged with the returned [`SubscriptionId`]. After a reconnect the stream
/// sends [`Event::Resync`](crate::Event::Resync) and a new snapshot. [`unsubscribe`](Self::unsubscribe)
/// stops a subscription of this connector by its id.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use brokapi_bcs::limits::default_limits;
/// use brokapi_bcs::{MarketDataConfig, MarketDataConnector};
/// use brokapi_core::{
///     Event, InstrumentId, InstrumentKey, NativeId, OrderBook, OrderBookSource, Price, ShutdownMode,
///     SubscribeOpts,
/// };
/// use brokapi_runtime::BrokerLimiter;
/// # use brokapi_bcs_protocol::config::BcsConfig;
///
/// /// Works with the connector of any broker.
/// async fn best_bid(src: &impl OrderBookSource, id: &InstrumentId) -> Option<Price> {
///     match src.order_book(id, 5).await.ok()? {
///         OrderBook::Snapshot { bids, .. } => bids.first().map(|l| l.price),
///         _ => None,
///     }
/// }
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
/// # let cfg = BcsConfig::for_mock("mock-refresh-token", &server.http_base);
/// let (events_tx, mut events) = tokio::sync::mpsc::channel(256);
/// let limiter = BrokerLimiter::new(&default_limits());
/// let md = MarketDataConnector::start(MarketDataConfig::new(cfg, "bcs-md"), limiter, events_tx).await?;
///
/// let sber = InstrumentId::new(
///     InstrumentKey::new("SBER", "TQBR"),
///     NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() },
/// );
/// assert!(best_bid(&md, &sber).await.is_some());
///
/// // A stream: the book arrives as an envelope with the id of the subscription.
/// let sub = md.subscribe_order_book(&sber, SubscribeOpts::conflate()).await?;
/// let env = tokio::time::timeout(Duration::from_secs(8), events.recv()).await?.expect("channel is open");
/// assert_eq!(env.sub, Some(sub));
/// assert!(matches!(env.payload, Event::OrderBook(_)));
///
/// md.unsubscribe(sub).await?;
/// md.shutdown(ShutdownMode::Immediate).await;
/// # Ok(())
/// # }
/// ```
#[async_trait]
pub trait OrderBookSource: Send + Sync {
    /// Reads the order book once, to `depth` levels per side.
    ///
    /// The answer is an [`OrderBook::Snapshot`]. Bids are listed best (highest) first and asks best
    /// (lowest) first. A `depth` above [`MarketDataCaps::max_order_book_depth`](crate::MarketDataCaps::max_order_book_depth)
    /// is cut to it (the BCS connector does so without telling).
    ///
    /// # Errors
    ///
    /// - [`Error::Config`] when `id.native` is of a kind this broker does not use.
    /// - [`Error::RateLimited`] when the local limiter or the broker refuses for now. Wait and repeat.
    /// - [`Error::Transport`] when the network failed or the answer was unreadable. Repeat later.
    /// - [`Error::Rejected`] when the broker refuses the request, for example an unknown instrument
    ///   (BCS: code `validation`).
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::{ErrorClass, OrderBook, OrderBookSource};
    /// # use brokapi_bcs::{MarketDataConfig, MarketDataConnector};
    /// # use brokapi_core::*;
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
    /// # let (events_tx, _events) = tokio::sync::mpsc::channel(256);
    /// # let limiter = brokapi_runtime::BrokerLimiter::new(&brokapi_bcs::limits::default_limits());
    /// # let md = MarketDataConnector::start(MarketDataConfig::new(cfg, "bcs-md"), limiter, events_tx).await?;
    /// # let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() });
    /// let OrderBook::Snapshot { bids, asks, .. } = md.order_book(&sber, 3).await? else {
    ///     panic!("a read always answers a snapshot");
    /// };
    /// assert!(bids.len() <= 3 && asks.len() <= 3);
    /// // The best prices are first on both sides, so the spread is the gap between them.
    /// assert!(bids[0].price < asks[0].price);
    ///
    /// // An instrument id of another broker's kind is a setup error: the call is not made.
    /// let wrong = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::Figi("BBG004730N88".into()));
    /// let err = md.order_book(&wrong, 3).await.unwrap_err();
    /// assert!(matches!(err, Error::Config(_)));
    /// assert_eq!(err.class(), ErrorClass::Fatal);
    /// # md.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    async fn order_book(&self, id: &InstrumentId, depth: u32) -> Result<OrderBook, Error>;

    /// Starts a stream of the order book of one instrument.
    ///
    /// Returns when the broker accepted the subscription. Books then arrive as
    /// [`Event::OrderBook`](crate::Event::OrderBook) with `Envelope::sub` equal to the returned id.
    /// The stream restores itself after a reconnect and announces it with
    /// [`Event::Resync`](crate::Event::Resync). `opts.depth` is the number of levels
    /// (`None`: the connector's default, BCS uses its maximum). For books use
    /// [`Backpressure::Conflate`](crate::Backpressure::Conflate): a slow reader then sees the latest
    /// book and not a queue of stale ones.
    ///
    /// # Errors
    ///
    /// - [`Error::Config`] when `id.native` is of a kind this broker does not use.
    /// - [`Error::Unsupported`] when the connector has no order book stream
    ///   ([`StreamingCaps::order_book`](crate::StreamingCaps::order_book) is `false`).
    /// - [`Error::RateLimited`], [`Error::Transport`] when the subscription could not be made. The
    ///   subscription does not exist, ask again later.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use brokapi_core::{Backpressure, Event, OrderBookSource, SubscribeOpts};
    /// # use brokapi_bcs::{MarketDataConfig, MarketDataConnector};
    /// # use brokapi_core::*;
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
    /// # let (events_tx, mut events) = tokio::sync::mpsc::channel(256);
    /// # let limiter = brokapi_runtime::BrokerLimiter::new(&brokapi_bcs::limits::default_limits());
    /// # let md = MarketDataConnector::start(MarketDataConfig::new(cfg, "bcs-md"), limiter, events_tx).await?;
    /// # let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() });
    /// // The 10 best levels, latest book wins if we read slowly.
    /// let opts = SubscribeOpts { depth: Some(10), ..SubscribeOpts::conflate() };
    /// assert_eq!(opts.backpressure, Backpressure::Conflate);
    /// let sub = md.subscribe_order_book(&sber, opts).await?;
    ///
    /// // All subscriptions of the connector write into the one channel, so route by `sub`.
    /// let env = tokio::time::timeout(Duration::from_secs(8), events.recv()).await?.expect("channel is open");
    /// assert_eq!(env.sub, Some(sub));
    /// let Event::OrderBook(book) = env.payload else { panic!("an order book subscription sends books") };
    /// # let _ = book;
    /// # md.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    async fn subscribe_order_book(
        &self,
        id: &InstrumentId,
        opts: SubscribeOpts,
    ) -> Result<SubscriptionId, Error>;

    /// Stops a subscription of this connector.
    ///
    /// After it returns, no new events of `sub` are produced. Envelopes already in your channel stay
    /// there, so keep reading until you have what you need. A subscription ends on its own when the
    /// connector shuts down.
    ///
    /// # Errors
    ///
    /// - [`Error::Transport`] when the broker could not be told, or (BCS, Finam) when `sub` is not an
    ///   active subscription of this connector.
    /// - [`Error::Rejected`] with code `unknown_subscription` for the same case on the T-Bank
    ///   connector.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::{OrderBookSource, SubscribeOpts};
    /// # use brokapi_bcs::{MarketDataConfig, MarketDataConnector};
    /// # use brokapi_core::*;
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
    /// # let (events_tx, _events) = tokio::sync::mpsc::channel(256);
    /// # let limiter = brokapi_runtime::BrokerLimiter::new(&brokapi_bcs::limits::default_limits());
    /// # let md = MarketDataConnector::start(MarketDataConfig::new(cfg, "bcs-md"), limiter, events_tx).await?;
    /// # let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() });
    /// let sub = md.subscribe_order_book(&sber, SubscribeOpts::conflate()).await?;
    /// md.unsubscribe(sub).await?;
    /// // The id is gone: a second unsubscribe is an error, not a silent success.
    /// assert!(md.unsubscribe(sub).await.is_err());
    /// # md.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    async fn unsubscribe(&self, sub: SubscriptionId) -> Result<(), Error>;
}

/// Public trades, quotes and candles: history reads and streams.
///
/// Reads ([`last_trades`](Self::last_trades), [`candles`](Self::candles)) answer directly. Streams
/// ([`subscribe_market_trades`](Self::subscribe_market_trades),
/// [`subscribe_quotes`](Self::subscribe_quotes), [`subscribe_candles`](Self::subscribe_candles))
/// return a [`SubscriptionId`] and send [`Event::MarketTrade`](crate::Event::MarketTrade),
/// [`Event::Quote`](crate::Event::Quote) and [`Event::Candle`](crate::Event::Candle) into the
/// channel given at start. Stop a stream with [`OrderBookSource::unsubscribe`], which takes any
/// subscription of the connector. Your own fills are not here, see [`ExecutionSource`].
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use brokapi_bcs::limits::default_limits;
/// use brokapi_bcs::{MarketDataConfig, MarketDataConnector};
/// use brokapi_core::{
///     Event, InstrumentId, InstrumentKey, MarketDataSource, NativeId, ShutdownMode, SubscribeOpts,
///     TimeRange, Timeframe, UnixNanos,
/// };
/// use brokapi_runtime::BrokerLimiter;
/// # use brokapi_bcs_protocol::config::BcsConfig;
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
/// # let cfg = BcsConfig::for_mock("mock-refresh-token", &server.http_base);
/// let (events_tx, mut events) = tokio::sync::mpsc::channel(256);
/// let limiter = BrokerLimiter::new(&default_limits());
/// let md = MarketDataConnector::start(MarketDataConfig::new(cfg, "bcs-md"), limiter, events_tx).await?;
///
/// let sber = InstrumentId::new(
///     InstrumentKey::new("SBER", "TQBR"),
///     NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() },
/// );
///
/// // The last 2 deals of the tape, oldest first (BCS).
/// let trades = md.last_trades(&sber, 2).await?;
/// assert!(!trades.is_empty() && trades.len() <= 2);
///
/// // One day of 1-minute candles. The range is in nanoseconds since the epoch.
/// let day = TimeRange::new(
///     UnixNanos::from_nanos(1_792_368_000_000_000_000), // 2026-10-19 00:00 UTC
///     UnixNanos::from_nanos(1_792_454_400_000_000_000), // 2026-10-20 00:00 UTC
/// );
/// let bars = md.candles(&sber, Timeframe::M1, day).await?;
/// assert!(bars.iter().all(|c| c.low <= c.high));
///
/// // A stream of quotes: the best bid and ask, the latest one wins if we read slowly.
/// let sub = md.subscribe_quotes(&sber, SubscribeOpts::conflate()).await?;
/// let quote = tokio::time::timeout(Duration::from_secs(8), async {
///     while let Some(env) = events.recv().await {
///         if let (Some(s), Event::Quote(q)) = (env.sub, env.payload) {
///             if s == sub {
///                 return Some(q);
///             }
///         }
///     }
///     None
/// })
/// .await?
/// .expect("a quote arrived");
/// assert!(quote.bid.is_some() || quote.ask.is_some());
///
/// md.shutdown(ShutdownMode::Immediate).await;
/// # Ok(())
/// # }
/// ```
#[async_trait]
pub trait MarketDataSource: Send + Sync {
    /// Reads the newest `limit` deals of the public tape.
    ///
    /// How far back the tape goes depends on the broker (BCS: the current session, T-Bank: the last
    /// hour). The BCS connector returns the deals oldest first and drops a record whose side is
    /// unknown. You may get fewer than `limit`.
    ///
    /// # Errors
    ///
    /// - [`Error::Config`] when `id.native` is of a kind this broker does not use.
    /// - [`Error::RateLimited`], [`Error::Transport`]: wait or repeat later.
    /// - [`Error::Rejected`] when the broker refuses the request.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::MarketDataSource;
    /// # use brokapi_bcs::{MarketDataConfig, MarketDataConnector};
    /// # use brokapi_core::*;
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
    /// # let (events_tx, _events) = tokio::sync::mpsc::channel(256);
    /// # let limiter = brokapi_runtime::BrokerLimiter::new(&brokapi_bcs::limits::default_limits());
    /// # let md = MarketDataConnector::start(MarketDataConfig::new(cfg, "bcs-md"), limiter, events_tx).await?;
    /// # let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() });
    /// let trades = md.last_trades(&sber, 5).await?;
    /// for t in &trades {
    ///     assert!(t.price.is_positive() && t.qty.is_positive());
    /// }
    /// # md.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    async fn last_trades(&self, id: &InstrumentId, limit: u32) -> Result<Vec<MarketTrade>, Error>;

    /// Starts a stream of the public deals of one instrument.
    ///
    /// Deals arrive as [`Event::MarketTrade`](crate::Event::MarketTrade) tagged with the returned
    /// id. Every deal is an event of its own, so [`Backpressure::Conflate`](crate::Backpressure::Conflate)
    /// would lose deals: use [`Backpressure::DropNewest`](crate::Backpressure::DropNewest) if losing
    /// some is acceptable, or [`Backpressure::Lossless`](crate::Backpressure::Lossless) if not (an
    /// overflow then sends [`Event::Overflow`](crate::Event::Overflow)).
    ///
    /// # Errors
    ///
    /// - [`Error::Config`] when `id.native` is of a kind this broker does not use.
    /// - [`Error::Unsupported`] when [`StreamingCaps::market_trades`](crate::StreamingCaps::market_trades)
    ///   is `false`.
    /// - [`Error::RateLimited`], [`Error::Transport`] when the subscription could not be made.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use brokapi_core::{Backpressure, Event, MarketDataSource, SubscribeOpts};
    /// # use brokapi_bcs::{MarketDataConfig, MarketDataConnector};
    /// # use brokapi_core::*;
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
    /// # let (events_tx, mut events) = tokio::sync::mpsc::channel(256);
    /// # let limiter = brokapi_runtime::BrokerLimiter::new(&brokapi_bcs::limits::default_limits());
    /// # let md = MarketDataConnector::start(MarketDataConfig::new(cfg, "bcs-md"), limiter, events_tx).await?;
    /// # let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() });
    /// // Keep up to 256 deals while we are slow, drop the newest ones beyond that.
    /// let opts = SubscribeOpts { backpressure: Backpressure::DropNewest { cap: 256 }, depth: None };
    /// let sub = md.subscribe_market_trades(&sber, opts).await?;
    /// let env = tokio::time::timeout(Duration::from_secs(8), events.recv()).await?.expect("channel is open");
    /// assert_eq!(env.sub, Some(sub));
    /// assert!(matches!(env.payload, Event::MarketTrade(_)));
    /// # md.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    async fn subscribe_market_trades(
        &self,
        id: &InstrumentId,
        opts: SubscribeOpts,
    ) -> Result<SubscriptionId, Error>;

    /// Starts a stream of the best bid and ask of one instrument.
    ///
    /// Quotes arrive as [`Event::Quote`](crate::Event::Quote) tagged with the returned id. Each
    /// quote replaces the previous one, so [`Backpressure::Conflate`](crate::Backpressure::Conflate)
    /// is the right policy.
    ///
    /// # Errors
    ///
    /// - [`Error::Config`] when `id.native` is of a kind this broker does not use.
    /// - [`Error::Unsupported`] when [`StreamingCaps::quotes`](crate::StreamingCaps::quotes) is `false`.
    /// - [`Error::RateLimited`], [`Error::Transport`] when the subscription could not be made.
    ///
    /// # Examples
    ///
    /// See the example of the trait, which receives a quote.
    async fn subscribe_quotes(
        &self,
        id: &InstrumentId,
        opts: SubscribeOpts,
    ) -> Result<SubscriptionId, Error>;

    /// Reads the bars of `tf` that fall in `range`.
    ///
    /// A bar has `open_ts` and `close_ts`, and a price is a [`Price`](crate::Price). Whether the ends
    /// of `range` are inclusive and how far back the broker serves is decided by the broker.
    ///
    /// # Errors
    ///
    /// - [`Error::Unsupported`] when the broker has no such timeframe (BCS has no
    ///   [`Timeframe::M10`], capability `candles_m10`). The call is not made.
    /// - [`Error::Config`] when `id.native` is of a kind this broker does not use.
    /// - [`Error::RateLimited`], [`Error::Transport`]: wait or repeat later.
    /// - [`Error::Rejected`] when the broker refuses the request.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::{Error, MarketDataSource, TimeRange, Timeframe, UnixNanos};
    /// # use brokapi_bcs::{MarketDataConfig, MarketDataConnector};
    /// # use brokapi_core::*;
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
    /// # let (events_tx, _events) = tokio::sync::mpsc::channel(256);
    /// # let limiter = brokapi_runtime::BrokerLimiter::new(&brokapi_bcs::limits::default_limits());
    /// # let md = MarketDataConnector::start(MarketDataConfig::new(cfg, "bcs-md"), limiter, events_tx).await?;
    /// # let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() });
    /// let day = TimeRange::new(
    ///     UnixNanos::from_nanos(1_792_368_000_000_000_000), // 2026-10-19 00:00 UTC
    ///     UnixNanos::from_nanos(1_792_454_400_000_000_000), // 2026-10-20 00:00 UTC
    /// );
    /// let bars = md.candles(&sber, Timeframe::M5, day).await?;
    /// assert!(bars.iter().all(|c| c.timeframe == Timeframe::M5));
    ///
    /// // BCS has no 10-minute bars. The answer comes at once, nothing is sent to the broker.
    /// match md.candles(&sber, Timeframe::M10, day).await {
    ///     Err(Error::Unsupported { capability }) => assert_eq!(capability, "candles_m10"),
    ///     other => panic!("expected Unsupported, got {other:?}"),
    /// }
    /// # md.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    async fn candles(
        &self,
        id: &InstrumentId,
        tf: Timeframe,
        range: TimeRange,
    ) -> Result<Vec<Candle>, Error>;

    /// Starts a stream of the latest bar of `tf` for one instrument.
    ///
    /// Bars arrive as [`Event::Candle`](crate::Event::Candle) tagged with the returned id. Use
    /// [`Backpressure::Conflate`](crate::Backpressure::Conflate): the latest bar replaces older ones.
    ///
    /// # Errors
    ///
    /// - [`Error::Unsupported`] when [`StreamingCaps::candles`](crate::StreamingCaps::candles) is
    ///   `false`, or the broker has no such timeframe (BCS has no [`Timeframe::M10`]).
    /// - [`Error::Config`] when `id.native` is of a kind this broker does not use.
    /// - [`Error::RateLimited`], [`Error::Transport`] when the subscription could not be made.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use brokapi_core::{Event, MarketDataSource, SubscribeOpts, Timeframe};
    /// # use brokapi_bcs::{MarketDataConfig, MarketDataConnector};
    /// # use brokapi_core::*;
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
    /// # let (events_tx, mut events) = tokio::sync::mpsc::channel(256);
    /// # let limiter = brokapi_runtime::BrokerLimiter::new(&brokapi_bcs::limits::default_limits());
    /// # let md = MarketDataConnector::start(MarketDataConfig::new(cfg, "bcs-md"), limiter, events_tx).await?;
    /// # let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() });
    /// let sub = md.subscribe_candles(&sber, Timeframe::M1, SubscribeOpts::conflate()).await?;
    /// let env = tokio::time::timeout(Duration::from_secs(8), events.recv()).await?.expect("channel is open");
    /// assert_eq!(env.sub, Some(sub));
    /// let Event::Candle(bar) = env.payload else { panic!("a candle subscription sends candles") };
    /// assert_eq!(bar.timeframe, Timeframe::M1);
    /// # md.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    async fn subscribe_candles(
        &self,
        id: &InstrumentId,
        tf: Timeframe,
        opts: SubscribeOpts,
    ) -> Result<SubscriptionId, Error>;
}

/// The instrument directory and the rules for trading an instrument.
///
/// Public data, so it lives in the market data connector. The library does NOT check your orders
/// against it: round quantity and price with [`TradingParams`] yourself before
/// [`OrderExecutor::place`].
///
/// # Examples
///
/// ```
/// # use brokapi_core::{Price, Qty};
/// use brokapi_bcs::limits::default_limits;
/// use brokapi_bcs::{MarketDataConfig, MarketDataConnector};
/// use brokapi_core::{
///     Error, InstrumentId, InstrumentKind, InstrumentQuery, InstrumentSource, TradingAvailability,
///     ShutdownMode,
/// };
/// use brokapi_runtime::BrokerLimiter;
/// # use brokapi_bcs_protocol::config::BcsConfig;
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
/// # let cfg = BcsConfig::for_mock("mock-refresh-token", &server.http_base);
/// let (events_tx, _events) = tokio::sync::mpsc::channel(256);
/// let limiter = BrokerLimiter::new(&default_limits());
/// let md = MarketDataConnector::start(MarketDataConfig::new(cfg, "bcs-md"), limiter, events_tx).await?;
///
/// // Find an instrument by ticker. The result carries ids ready to pass to every other method.
/// let found = md
///     .find(InstrumentQuery::ByTicker { ticker: "SBER".into(), exchange: None })
///     .await?;
/// let sber: InstrumentId = found.first().expect("SBER is in the directory").id.clone();
///
/// // How to size and price an order.
/// let params = md.trading_params(&sber).await?;
/// assert!(params.lot_size > Qty::from(0) && params.price_step > Price::from(0));
///
/// // Is it tradable now?
/// let status = md.trading_status(&sber).await?;
/// println!("{}: {:?}", sber.key.display(), status.availability);
///
/// // A search by a kind the broker cannot list is refused at once.
/// let by_kind = md.find(InstrumentQuery::ByKind(InstrumentKind::Option)).await;
/// assert!(matches!(by_kind, Err(Error::Unsupported { .. })));
///
/// md.shutdown(ShutdownMode::Immediate).await;
/// # Ok(())
/// # }
/// ```
#[async_trait]
pub trait InstrumentSource: Send + Sync {
    /// Looks instruments up in the directory.
    ///
    /// The result can be empty (nothing matches). A query by kind can be a long list, the connector
    /// may read it page by page, each page counts against the rate limit.
    ///
    /// # Errors
    ///
    /// - [`Error::Unsupported`] when the broker cannot list that kind (BCS: `Option` and `Other`).
    /// - [`Error::RateLimited`], [`Error::Transport`]: wait or repeat later. The BCS connector also
    ///   answers `Transport` when a kind has more than its page limit, rather than return part of
    ///   the list.
    /// - [`Error::Rejected`] when the broker refuses the request.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::{InstrumentQuery, InstrumentSource};
    /// # use brokapi_bcs::{MarketDataConfig, MarketDataConnector};
    /// # use brokapi_core::*;
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
    /// # let (events_tx, _events) = tokio::sync::mpsc::channel(256);
    /// # let limiter = brokapi_runtime::BrokerLimiter::new(&brokapi_bcs::limits::default_limits());
    /// # let md = MarketDataConnector::start(MarketDataConfig::new(cfg, "bcs-md"), limiter, events_tx).await?;
    /// let list = md
    ///     .find(InstrumentQuery::ByTicker { ticker: "SBER".into(), exchange: None })
    ///     .await?;
    /// assert!(list.iter().any(|i| i.id.key.symbol == "SBER"));
    /// # md.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    async fn find(&self, q: InstrumentQuery) -> Result<Vec<Instrument>, Error>;

    /// Reads one directory entry.
    ///
    /// # Errors
    ///
    /// - [`Error::Config`] when `id.native` is of a kind this broker does not use.
    /// - [`Error::RateLimited`], [`Error::Transport`]: wait or repeat later. An instrument the
    ///   directory does not have is reported as `Transport` by the BCS connector (message
    ///   `instrument not found`), there is no separate variant for "not found".
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::InstrumentSource;
    /// # use brokapi_bcs::{MarketDataConfig, MarketDataConnector};
    /// # use brokapi_core::*;
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
    /// # let (events_tx, _events) = tokio::sync::mpsc::channel(256);
    /// # let limiter = brokapi_runtime::BrokerLimiter::new(&brokapi_bcs::limits::default_limits());
    /// # let md = MarketDataConnector::start(MarketDataConfig::new(cfg, "bcs-md"), limiter, events_tx).await?;
    /// # let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() });
    /// let info = md.get(&sber).await?;
    /// assert_eq!(info.id.key.symbol, "SBER");
    /// # md.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    async fn get(&self, id: &InstrumentId) -> Result<Instrument, Error>;

    /// Reads lot size, price step and the other numbers needed to size and price an order.
    ///
    /// # Errors
    ///
    /// - [`Error::Config`] when `id.native` is of a kind this broker does not use.
    /// - [`Error::RateLimited`], [`Error::Transport`]: wait or repeat later. The BCS connector also
    ///   answers `Transport` when the directory has no trading parameters for the board.
    ///
    /// # Examples
    ///
    /// ```
    /// # use brokapi_core::{Decimal, dec};
    /// use brokapi_core::InstrumentSource;
    /// # use brokapi_bcs::{MarketDataConfig, MarketDataConnector};
    /// # use brokapi_core::*;
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
    /// # let (events_tx, _events) = tokio::sync::mpsc::channel(256);
    /// # let limiter = brokapi_runtime::BrokerLimiter::new(&brokapi_bcs::limits::default_limits());
    /// # let md = MarketDataConnector::start(MarketDataConfig::new(cfg, "bcs-md"), limiter, events_tx).await?;
    /// # let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() });
    /// let params = md.trading_params(&sber).await?;
    ///
    /// // The nearest price on the grid of the instrument at or below 250.037. The rounding is exact.
    /// let wanted = Price::new(dec!(250.037));
    /// let price = params.round_price(wanted, RoundMode::Down)?;
    /// assert!(price <= wanted);
    /// assert!(params.is_price_on_grid(price));
    /// # md.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    async fn trading_params(&self, id: &InstrumentId) -> Result<TradingParams, Error>;

    /// Reads whether the instrument can be traded now.
    ///
    /// # Errors
    ///
    /// - [`Error::Config`] when `id.native` is of a kind this broker does not use.
    /// - [`Error::RateLimited`], [`Error::Transport`]: wait or repeat later.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::{InstrumentSource, TradingAvailability};
    /// # use brokapi_bcs::{MarketDataConfig, MarketDataConnector};
    /// # use brokapi_core::*;
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
    /// # let (events_tx, _events) = tokio::sync::mpsc::channel(256);
    /// # let limiter = brokapi_runtime::BrokerLimiter::new(&brokapi_bcs::limits::default_limits());
    /// # let md = MarketDataConnector::start(MarketDataConfig::new(cfg, "bcs-md"), limiter, events_tx).await?;
    /// # let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() });
    /// let status = md.trading_status(&sber).await?;
    /// if status.availability == TradingAvailability::Open {
    ///     // safe to send orders
    /// }
    /// # md.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    async fn trading_status(&self, id: &InstrumentId) -> Result<TradingStatus, Error>;

    /// Reads the trading sessions of an exchange over `range`.
    ///
    /// What `exchange` means depends on the broker: T-Bank takes an exchange name (`MOEX`), Finam and
    /// BCS take `TICKER@MARKET` because their schedules are per instrument. The BCS answer is for
    /// one day, the day of `range.from`.
    ///
    /// # Errors
    ///
    /// - [`Error::Config`] when `exchange` is not in the form the broker needs (BCS: not
    ///   `TICKER@CLASSCODE`).
    /// - [`Error::RateLimited`], [`Error::Transport`]: wait or repeat later.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::{Error, InstrumentSource, TimeRange, UnixNanos};
    /// # use brokapi_bcs::{MarketDataConfig, MarketDataConnector};
    /// # use brokapi_core::*;
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
    /// # let (events_tx, _events) = tokio::sync::mpsc::channel(256);
    /// # let limiter = brokapi_runtime::BrokerLimiter::new(&brokapi_bcs::limits::default_limits());
    /// # let md = MarketDataConnector::start(MarketDataConfig::new(cfg, "bcs-md"), limiter, events_tx).await?;
    /// let day = TimeRange::new(
    ///     UnixNanos::from_nanos(1_792_368_000_000_000_000), // 2026-10-19 00:00 UTC
    ///     UnixNanos::from_nanos(1_792_454_400_000_000_000), // 2026-10-20 00:00 UTC
    /// );
    /// // BCS schedules are per instrument: "TICKER@CLASSCODE".
    /// let schedule = md.schedule("SBER@TQBR", day).await?;
    /// assert!(schedule.sessions.iter().all(|s| s.start < s.end));
    ///
    /// // A bare exchange name is not enough for BCS.
    /// assert!(matches!(md.schedule("MOEX", day).await, Err(Error::Config(_))));
    /// # md.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    async fn schedule(&self, exchange: &str, range: TimeRange) -> Result<Schedule, Error>;
}

/// Orders: put, cancel, replace, look at them, follow them, and reconcile with your ledger.
///
/// The rules that matter:
/// - YOU make the [`ClientOrderId`] of every order, a fresh UUID, and keep it in your own ledger.
/// - Trading commands ([`place`](Self::place), [`cancel`](Self::cancel), [`replace`](Self::replace))
///   are refused until the first successful [`reconcile`](Self::reconcile), so start by calling
///   `reconcile` (with an empty slice if you have no ledger).
/// - A command that got no answer is [`Error::Unknown`] and is NEVER repeated by the library,
///   because a repeated place could create a second order. Look at the order first.
/// - Shutdown does not cancel orders at the exchange.
///
/// # Examples
///
/// The whole path against the BCS connector on its mock: reconcile, follow the order stream, place,
/// read the state, cancel.
///
/// ```
/// use std::time::Duration;
/// use brokapi_bcs::limits::default_limits;
/// use brokapi_bcs::{UserDataConfig, UserDataConnector};
/// use brokapi_core::{
///     ClientOrderId, Event, InstrumentId, InstrumentKey, NativeId, OrderExecutor, OrderState,
///     OrderType, PlaceOrderRequest, Price, Qty, ShutdownMode, Side,
/// };
/// use brokapi_runtime::BrokerLimiter;
/// # use brokapi_bcs_protocol::config::BcsConfig;
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let mock_options = std::sync::Arc::new(brokapi_mocks::bcs::MockOptions::default());
/// # let server = brokapi_mocks::bcs::MockServer::start(mock_options.clone()).await;
/// # let cfg = BcsConfig::for_mock("mock-refresh-token", &server.http_base).with_write(true);
/// let (events_tx, mut events) = tokio::sync::mpsc::channel(256);
/// let limiter = BrokerLimiter::new(&default_limits());
/// let ud = UserDataConnector::start(UserDataConfig::new(cfg, "bcs-ud"), limiter, events_tx).await?;
///
/// // 1. Commands wait for this. We have no ledger yet, so we pass an empty one.
/// let report = ud.reconcile(&[]).await?;
/// assert!(report.confirmed.is_empty() && report.foreign.is_empty());
///
/// // 2. Subscribe BEFORE placing, so no event of our order is missed.
/// let _sub = ud.subscribe_order_state().await?;
/// # // The event socket connects in the background: wait for it, or the first event could be missed.
/// # tokio::time::timeout(Duration::from_secs(8), async {
/// #     while mock_options.event_sockets() == 0 {
/// #         tokio::time::sleep(Duration::from_millis(20)).await;
/// #     }
/// # })
/// # .await?;
///
/// // 3. Place a limit order far below the market, so it stays working.
/// let cid = ClientOrderId::new_v4();
/// let ack = ud
///     .place(PlaceOrderRequest {
///         cid,
///         instrument: InstrumentId::new(
///             InstrumentKey::new("SBER", "TQBR"),
///             NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() },
///         ),
///         side: Side::Buy,
///         order_type: OrderType::Limit,
///         qty: Qty::from(1),
///         price: Some(Price::from(100)),
///     })
///     .await?;
/// assert_eq!(ack.cid, cid);
///
/// // 4. The stream tells about it, and a read agrees.
/// let seen = tokio::time::timeout(Duration::from_secs(8), async {
///     while let Some(env) = events.recv().await {
///         if let Event::Order(o) = env.payload {
///             if o.cid == cid {
///                 return Some(o);
///             }
///         }
///     }
///     None
/// })
/// .await?
/// .expect("an event for our order");
/// assert!(!matches!(seen.state, OrderState::Rejected { .. }));
/// let now = ud.order_state(cid).await?;
/// assert!(matches!(now.state, OrderState::Acked { .. }));
///
/// // 5. Cancel it. Ok(()) means the broker says it is cancelled.
/// ud.cancel(cid).await?;
///
/// ud.shutdown(ShutdownMode::Immediate).await;
/// # Ok(())
/// # }
/// ```
#[async_trait]
pub trait OrderExecutor: Send + Sync {
    /// Puts a new order and waits for the broker's decision.
    ///
    /// `Ok` means the broker accepted the order (it is working, not necessarily filled). The
    /// [`OrderAck`] has the broker's id. Follow the order with [`order_state`](Self::order_state) or
    /// the stream of [`subscribe_order_state`](Self::subscribe_order_state).
    ///
    /// The `cid` in `req` must be new. A connector sends a cid at most once: a second place with the
    /// same cid is refused with [`Error::Rejected`] (BCS: code `duplicate_place`) without being sent.
    /// This is on purpose. After a timeout the first one may have reached the broker, and a retry
    /// could make a second order. Use a new cid for a new order, after you know what happened to the
    /// first. The library does not check quantity against lot size or price against the price step.
    ///
    /// # Errors
    ///
    /// - [`Error::NotReconciled`], [`Error::Degraded`], [`Error::ShuttingDown`]: the gate is closed,
    ///   nothing was sent. See [`reconcile`](Self::reconcile).
    /// - [`Error::Config`] when `req.instrument.native` is of a kind this broker does not use.
    /// - [`Error::RateLimited`] when the local limiter or the broker refuses for now. The command was
    ///   not executed. Wait `retry_after`. The safe rule for the retry is a new cid.
    /// - [`Error::Rejected`] when the broker (or the connector before sending) says no: a bad price
    ///   or quantity, a limit order without a price (BCS: `missing_price`), a repeated cid
    ///   (`duplicate_place`), a refusal inside an accepted request (BCS: `rejected`). The order does
    ///   not exist. Change the request.
    /// - [`Error::Unknown`] when the request may have reached the broker and there is no answer
    ///   (timeout, cut connection, HTTP 5xx). Do NOT place again. Ask the broker with
    ///   [`order_state`](Self::order_state) or [`active_orders`](Self::active_orders).
    /// - [`Error::Transport`] when the failure happened before the request left, for example
    ///   authentication.
    ///
    /// # Examples
    ///
    /// A limit order that the mock broker refuses, and what the caller sees:
    ///
    /// ```
    /// use brokapi_core::{
    ///     ClientOrderId, Error, ErrorClass, InstrumentId, InstrumentKey, NativeId, OrderExecutor,
    ///     OrderType, PlaceOrderRequest, Price, Qty, Side,
    /// };
    /// # use brokapi_bcs::{UserDataConfig, UserDataConnector};
    /// # use brokapi_core::*;
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base).with_write(true);
    /// # let (events_tx, _events) = tokio::sync::mpsc::channel(256);
    /// # let limiter = brokapi_runtime::BrokerLimiter::new(&brokapi_bcs::limits::default_limits());
    /// # let ud = UserDataConnector::start(UserDataConfig::new(cfg, "bcs-ud"), limiter, events_tx).await?;
    /// # ud.reconcile(&[]).await?;
    /// # let refusing_price = brokapi_conformance::REJECT_PRICE;
    /// let sber = InstrumentId::new(
    ///     InstrumentKey::new("SBER", "TQBR"),
    ///     NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() },
    /// );
    /// let req = PlaceOrderRequest {
    ///     cid: ClientOrderId::new_v4(),
    ///     instrument: sber,
    ///     side: Side::Buy,
    ///     order_type: OrderType::Limit,
    ///     qty: Qty::from(1),
    ///     price: Some(refusing_price), // the mock broker refuses this price
    /// };
    /// match ud.place(req.clone()).await {
    ///     Ok(ack) => println!("accepted as {}", ack.broker_id.as_str()),
    ///     // The broker said no. The order does not exist. Fix the request and use a new cid.
    ///     Err(e @ Error::Rejected { .. }) => assert_eq!(e.class(), ErrorClass::Fatal),
    ///     // Maybe it exists. Never place again: ask with order_state / active_orders.
    ///     Err(Error::Unknown) => panic!("look at the order before anything else"),
    ///     Err(other) => panic!("unexpected: {other}"),
    /// }
    ///
    /// // Sending the same cid again is refused locally: a cid is used once.
    /// let again = ud.place(req).await.unwrap_err();
    /// assert!(matches!(again, Error::Rejected { ref code, .. } if code == "duplicate_place"));
    /// # ud.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    async fn place(&self, req: PlaceOrderRequest) -> Result<OrderAck, Error>;

    /// Cancels a working order and waits for the broker to confirm.
    ///
    /// `Ok(())` means the broker says the order is cancelled. It cancels orders placed through
    /// [`place`](Self::place) and [`replace`](Self::replace). For a stop order use
    /// [`StopOrderExecutor::cancel_stop`]. A part that was filled before stays filled.
    ///
    /// # Errors
    ///
    /// - [`Error::NotReconciled`], [`Error::Degraded`], [`Error::ShuttingDown`]: the gate is closed,
    ///   nothing was sent.
    /// - [`Error::RateLimited`]: not executed, wait `retry_after` and repeat.
    /// - [`Error::Rejected`] when the broker refuses: the order is unknown to it, it was filled
    ///   first, or the request was refused. The code depends on how the broker answers (BCS codes
    ///   include `not_found`, `already_filled`, `cancel_rejected`, `validation`). Read the order with
    ///   [`order_state`](Self::order_state) to see where it stands.
    /// - [`Error::Unknown`] when the answer is missing, or the answer leaves the order alive. The
    ///   order may still be working. Ask [`order_state`](Self::order_state) and cancel again only if
    ///   it is. A repeated cancel is harmless for the order, but it uses rate limit.
    /// - [`Error::Transport`] when the failure happened before the request left.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::{ClientOrderId, Error, OrderExecutor, OrderType, PlaceOrderRequest, Price, Qty, Side};
    /// # use brokapi_bcs::{UserDataConfig, UserDataConnector};
    /// # use brokapi_core::*;
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base).with_write(true);
    /// # let (events_tx, _events) = tokio::sync::mpsc::channel(256);
    /// # let limiter = brokapi_runtime::BrokerLimiter::new(&brokapi_bcs::limits::default_limits());
    /// # let ud = UserDataConnector::start(UserDataConfig::new(cfg, "bcs-ud"), limiter, events_tx).await?;
    /// # ud.reconcile(&[]).await?;
    /// # let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() });
    /// # let order = |price: Price| PlaceOrderRequest { cid: ClientOrderId::new_v4(), instrument: sber.clone(), side: Side::Buy, order_type: OrderType::Limit, qty: Qty::from(2), price: Some(price) };
    /// let req = order(Price::from(100));
    /// let cid = req.cid;
    /// ud.place(req).await?;
    /// ud.cancel(cid).await?;
    ///
    /// // An order that was filled before the cancel cannot be cancelled. The broker refuses, and the
    /// // order is not working any more.
    /// let req = order(Price::from(100));
    /// let cid = req.cid;
    /// ud.place(req).await?;
    /// # server.fill(&cid.hyphenated().to_string(), brokapi_core::Qty::from(2), brokapi_core::Price::from(100)); // the mock executes the order
    /// match ud.cancel(cid).await {
    ///     Err(Error::Rejected { code, message }) => println!("refused: {code} - {message}"),
    ///     other => panic!("expected a refusal, got {other:?}"),
    /// }
    /// # ud.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    async fn cancel(&self, cid: ClientOrderId) -> Result<(), Error>;

    /// Changes the quantity or price of a working order.
    ///
    /// The broker cancels the original and puts a new order under `req.cid`, so the answer is a new
    /// order with a new broker id. The original is not active afterwards. Only where
    /// [`UserDataCaps::supports_replace`](crate::UserDataCaps::supports_replace) is `true`.
    ///
    /// # Errors
    ///
    /// - [`Error::Unsupported`] when the broker has no replace (Finam). Nothing was sent, the order
    ///   is untouched.
    /// - [`Error::NotReconciled`], [`Error::Degraded`], [`Error::ShuttingDown`]: the gate is closed.
    /// - [`Error::RateLimited`]: not executed, wait `retry_after`.
    /// - [`Error::Rejected`] when the broker refuses, for example the original is unknown or filled,
    ///   or (BCS) the original is partly filled and `qty` is `None` (`qty_required`).
    /// - [`Error::Unknown`] when the outcome is not known. The original may be replaced, still
    ///   working or gone. Ask [`order_state`](Self::order_state) for both cids. Do not replace again
    ///   until you know.
    /// - [`Error::Transport`] when the failure happened before the request left.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::{ClientOrderId, OrderExecutor, OrderType, PlaceOrderRequest, Price, Qty, ReplaceOrderRequest, Side};
    /// # use brokapi_bcs::{UserDataConfig, UserDataConnector};
    /// # use brokapi_core::*;
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base).with_write(true);
    /// # let (events_tx, _events) = tokio::sync::mpsc::channel(256);
    /// # let limiter = brokapi_runtime::BrokerLimiter::new(&brokapi_bcs::limits::default_limits());
    /// # let ud = UserDataConnector::start(UserDataConfig::new(cfg, "bcs-ud"), limiter, events_tx).await?;
    /// # ud.reconcile(&[]).await?;
    /// # let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() });
    /// assert!(ud.caps().supports_replace);
    /// let first = ClientOrderId::new_v4();
    /// ud.place(PlaceOrderRequest {
    ///     cid: first, instrument: sber, side: Side::Buy, order_type: OrderType::Limit,
    ///     qty: Qty::from(2), price: Some(Price::from(100)),
    /// }).await?;
    ///
    /// // Move the price. The new order has a NEW cid, the old cid is not active any more.
    /// let second = ClientOrderId::new_v4();
    /// let ack = ud.replace(ReplaceOrderRequest {
    ///     cid: second, original_cid: first, qty: None, price: Some(Price::from(101)),
    /// }).await?;
    /// assert_eq!(ack.cid, second);
    /// let active = ud.active_orders().await?;
    /// assert!(active.iter().any(|o| o.cid == second));
    /// assert!(!active.iter().any(|o| o.cid == first));
    /// ud.cancel(second).await?;
    /// # ud.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    async fn replace(&self, req: ReplaceOrderRequest) -> Result<OrderAck, Error>;

    /// Reads one order by its cid, as the broker shows it now.
    ///
    /// This is how you leave the doubt of [`Error::Unknown`]. A read is not behind the trading
    /// gate, it works before the first reconcile. It also finds orders that are no longer active
    /// (filled, cancelled), unlike [`active_orders`](Self::active_orders).
    ///
    /// # Errors
    ///
    /// - [`Error::RateLimited`], [`Error::Transport`]: wait or repeat later.
    /// - A cid the broker does not know is reported by the BCS connector as [`Error::Transport`]
    ///   (message `not found`), not as `Rejected`. After an [`Error::Unknown`] this can mean that the
    ///   order never reached the broker. There is no separate variant for it.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::{ClientOrderId, OrderExecutor, OrderState, OrderType, PlaceOrderRequest, Price, Qty, Side};
    /// # use brokapi_bcs::{UserDataConfig, UserDataConnector};
    /// # use brokapi_core::*;
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base).with_write(true);
    /// # let (events_tx, _events) = tokio::sync::mpsc::channel(256);
    /// # let limiter = brokapi_runtime::BrokerLimiter::new(&brokapi_bcs::limits::default_limits());
    /// # let ud = UserDataConnector::start(UserDataConfig::new(cfg, "bcs-ud"), limiter, events_tx).await?;
    /// # ud.reconcile(&[]).await?;
    /// # let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() });
    /// let cid = ClientOrderId::new_v4();
    /// ud.place(PlaceOrderRequest {
    ///     cid, instrument: sber, side: Side::Buy, order_type: OrderType::Limit,
    ///     qty: Qty::from(2), price: Some(Price::from(100)),
    /// }).await?;
    ///
    /// let snapshot = ud.order_state(cid).await?;
    /// match snapshot.state {
    ///     OrderState::Acked { .. } => println!("working, nothing filled"),
    ///     OrderState::PartiallyFilled { filled, left, .. } => println!("{} done, {} to go", filled.decimal(), left.decimal()),
    ///     OrderState::Filled { .. } | OrderState::Cancelled { .. } | OrderState::Rejected { .. } => println!("final"),
    ///     OrderState::InFlight { .. } | OrderState::Unknown { .. } => println!("ask again a bit later"),
    /// }
    /// assert_eq!(snapshot.qty, Qty::from(2));
    /// # ud.cancel(cid).await?;
    /// # ud.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    async fn order_state(&self, cid: ClientOrderId) -> Result<OrderSnapshot, Error>;

    /// Lists the working orders of the account.
    ///
    /// Plain orders only: stop orders are in [`StopOrderExecutor::active_stop_orders`]. The list
    /// includes orders that were not placed through this connector (by hand, by another program).
    /// Their cid is not yours: where the broker does not give a cid back, the BCS connector makes a
    /// stable one from the broker id, the same for the same order every time.
    ///
    /// # Errors
    ///
    /// - [`Error::RateLimited`], [`Error::Transport`]: wait or repeat later.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::{ClientOrderId, OrderExecutor, OrderType, PlaceOrderRequest, Price, Qty, Side};
    /// # use brokapi_bcs::{UserDataConfig, UserDataConnector};
    /// # use brokapi_core::*;
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base).with_write(true);
    /// # let (events_tx, _events) = tokio::sync::mpsc::channel(256);
    /// # let limiter = brokapi_runtime::BrokerLimiter::new(&brokapi_bcs::limits::default_limits());
    /// # let ud = UserDataConnector::start(UserDataConfig::new(cfg, "bcs-ud"), limiter, events_tx).await?;
    /// # ud.reconcile(&[]).await?;
    /// # let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() });
    /// let cid = ClientOrderId::new_v4();
    /// ud.place(PlaceOrderRequest {
    ///     cid, instrument: sber, side: Side::Buy, order_type: OrderType::Limit,
    ///     qty: Qty::from(3), price: Some(Price::from(100)),
    /// }).await?;
    ///
    /// let active = ud.active_orders().await?;
    /// let mine = active.iter().find(|o| o.cid == cid).expect("our order is working");
    /// assert_eq!(mine.price, Some(Price::from(100)));
    /// # ud.cancel(cid).await?;
    /// # ud.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    async fn active_orders(&self) -> Result<Vec<OrderSnapshot>, Error>;

    /// Starts the stream of changes of your orders.
    ///
    /// Every change (accepted, partly filled, filled, cancelled, refused) arrives as
    /// [`Event::Order`](crate::Event::Order), tagged with the returned id, and carries the cid.
    ///
    /// The stream is [`Backpressure::Lossless`](crate::Backpressure::Lossless) with a buffer size set
    /// in the connector config. If you read too slowly and the buffer fills, an
    /// [`Event::Overflow`](crate::Event::Overflow) is sent and trading is refused with
    /// [`Error::Degraded`] until [`reconcile`](Self::reconcile). Keep draining your channel.
    ///
    /// Subscribe BEFORE you place. The stream connects in the background, and changes that happened
    /// before it was open are not delivered. To catch up, read [`order_state`](Self::order_state) or
    /// [`active_orders`](Self::active_orders). There is no way to end this subscription except
    /// shutting the connector down.
    ///
    /// # Errors
    ///
    /// - [`Error::Unsupported`] when the connector has no order stream.
    /// - [`Error::Transport`] when the subscription could not be made.
    ///
    /// # Examples
    ///
    /// See the example of the trait, which follows an order on the stream.
    async fn subscribe_order_state(&self) -> Result<SubscriptionId, Error>;

    /// Compares your ledger with the broker and opens the trading gate.
    ///
    /// Call it once after the connector starts, and again after [`Error::Degraded`]. A successful
    /// call is what lifts [`Error::NotReconciled`] and [`Error::Degraded`].
    ///
    /// `ledger` is the list of cids of orders you believe are working. Pass `&[]` if you have none.
    /// The report sorts the world into three:
    /// - `confirmed`: in your ledger and active at the broker.
    /// - `closed_offline`: in your ledger and NOT active at the broker (filled, cancelled, refused,
    ///   or never received). The report does not say which. Ask [`order_state`](Self::order_state)
    ///   for each, and the executions for the fills.
    /// - `foreign`: active at the broker and not in your ledger. Leave them alone.
    ///
    /// The comparison is with [`active_orders`](Self::active_orders), which lists plain orders.
    /// Keep the cids of stop orders out of the ledger and check them with
    /// [`StopOrderExecutor::active_stop_orders`] (not verified against the live API for every
    /// connector).
    ///
    /// # Errors
    ///
    /// - [`Error::RateLimited`], [`Error::Transport`] when the broker could not be read. The gate
    ///   stays closed. Repeat later. A cid the broker answers "not found" for is not an error, it
    ///   goes to `closed_offline`.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::{ClientOrderId, Error, OrderExecutor, OrderType, PlaceOrderRequest, Price, Qty, Side};
    /// use brokapi_bcs::{UserDataConfig, UserDataConnector};
    /// # use brokapi_core::*;
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base).with_write(true);
    /// # let limiter = || brokapi_runtime::BrokerLimiter::new(&brokapi_bcs::limits::default_limits());
    /// # let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() });
    /// # let order = || PlaceOrderRequest { cid: ClientOrderId::new_v4(), instrument: sber.clone(), side: Side::Buy, order_type: OrderType::Limit, qty: Qty::from(1), price: Some(Price::from(100)) };
    /// # // The previous run of the program: it put an order and stopped. The order stays at the exchange.
    /// # let previous = UserDataConnector::start(UserDataConfig::new(cfg.clone(), "previous-run"), limiter(), tokio::sync::mpsc::channel(8).0).await?;
    /// # previous.reconcile(&[]).await?;
    /// # let left_over = order();
    /// # previous.place(left_over.clone()).await?;
    /// # previous.shutdown(ShutdownMode::Immediate).await;
    /// # let saved_ledger = [left_over.cid];
    /// let (events_tx, _events) = tokio::sync::mpsc::channel(256);
    /// let ud = UserDataConnector::start(UserDataConfig::new(cfg, "bcs-ud"), limiter(), events_tx).await?;
    ///
    /// // Before the first reconcile, trading commands are refused and nothing is sent.
    /// assert!(matches!(ud.place(order()).await, Err(Error::NotReconciled)));
    ///
    /// // Our ledger has the cid saved by the previous run, and one the broker never heard of.
    /// let lost = ClientOrderId::new_v4();
    /// let ledger = [saved_ledger[0], lost];
    /// let report = ud.reconcile(&ledger).await?;
    /// assert_eq!(report.confirmed.len(), 1); // still working at the broker
    /// assert_eq!(report.confirmed[0].cid, saved_ledger[0]);
    /// assert_eq!(report.closed_offline, vec![lost]); // not active at the broker
    ///
    /// // The gate is open now.
    /// let next = order();
    /// let cid = next.cid;
    /// ud.place(next).await?;
    /// ud.cancel(cid).await?;
    /// # ud.cancel(saved_ledger[0]).await?;
    /// # ud.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    async fn reconcile(&self, ledger: &[ClientOrderId]) -> Result<ReconcileReport, Error>;
}

/// Your own fills: the history and the live stream.
///
/// An [`Execution`] is one fill of one of your orders. Public deals of the market are in
/// [`MarketDataSource`]. The stream is
/// [`Backpressure::Lossless`](crate::Backpressure::Lossless): a fill must not be lost, so an
/// overflow is announced with [`Event::Overflow`](crate::Event::Overflow) and trading stops until
/// [`OrderExecutor::reconcile`].
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use brokapi_bcs::limits::default_limits;
/// use brokapi_bcs::{UserDataConfig, UserDataConnector};
/// use brokapi_core::{
///     ClientOrderId, Event, ExecutionSource, InstrumentId, InstrumentKey, NativeId, OrderExecutor,
///     OrderType, PlaceOrderRequest, Price, Qty, ShutdownMode, Side, TimeRange, UnixNanos,
/// };
/// use brokapi_runtime::BrokerLimiter;
/// # use brokapi_bcs_protocol::config::BcsConfig;
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let mock_options = std::sync::Arc::new(brokapi_mocks::bcs::MockOptions::default());
/// # let server = brokapi_mocks::bcs::MockServer::start(mock_options.clone()).await;
/// # let cfg = BcsConfig::for_mock("mock-refresh-token", &server.http_base).with_write(true);
/// let (events_tx, mut events) = tokio::sync::mpsc::channel(256);
/// let limiter = BrokerLimiter::new(&default_limits());
/// let ud = UserDataConnector::start(UserDataConfig::new(cfg, "bcs-ud"), limiter, events_tx).await?;
/// ud.reconcile(&[]).await?;
///
/// // Follow the fills as they happen.
/// let sub = ud.subscribe_executions().await?;
/// # tokio::time::timeout(Duration::from_secs(8), async {
/// #     while mock_options.event_sockets() == 0 {
/// #         tokio::time::sleep(Duration::from_millis(20)).await;
/// #     }
/// # })
/// # .await?;
///
/// let cid = ClientOrderId::new_v4();
/// ud.place(PlaceOrderRequest {
///     cid,
///     instrument: InstrumentId::new(
///         InstrumentKey::new("SBER", "TQBR"),
///         NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() },
///     ),
///     side: Side::Buy,
///     order_type: OrderType::Limit,
///     qty: Qty::from(2),
///     price: Some(Price::from(100)),
/// })
/// .await?;
/// # server.fill(&cid.hyphenated().to_string(), brokapi_core::Qty::from(2), brokapi_core::Price::from(100)); // the mock executes the order
///
/// let fill = tokio::time::timeout(Duration::from_secs(8), async {
///     while let Some(env) = events.recv().await {
///         if let Event::Execution(x) = env.payload {
///             assert_eq!(env.sub, Some(sub));
///             return Some(x);
///         }
///     }
///     None
/// })
/// .await?
/// .expect("a fill arrived");
/// assert_eq!(fill.cid, Some(cid));
/// assert!(fill.qty.is_positive());
///
/// // The history of the day, the same fill with its trade id.
/// let everything = TimeRange::new(UnixNanos::ZERO, UnixNanos::from_nanos(i64::MAX / 2));
/// let history = ud.executions(everything).await?;
/// assert!(history.iter().any(|x| x.trade_id.len() > 0));
///
/// ud.shutdown(ShutdownMode::Immediate).await;
/// # Ok(())
/// # }
/// ```
#[async_trait]
pub trait ExecutionSource: Send + Sync {
    /// Reads your fills in `range`.
    ///
    /// Use it to find what happened to an order after [`Error::Unknown`], or to catch up after a
    /// gap. `cid` of an [`Execution`] is `None` when the broker does not echo it (the BCS history
    /// does not), match by `broker_order_id` then. `commission` is `0.0` where the broker gives none
    /// (BCS history). How far back it goes is decided by the broker.
    ///
    /// # Errors
    ///
    /// - [`Error::RateLimited`], [`Error::Transport`]: wait or repeat later.
    /// - [`Error::Rejected`] when the broker refuses the request.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::{ExecutionSource, TimeRange, UnixNanos};
    /// # use brokapi_bcs::{UserDataConfig, UserDataConnector};
    /// # use brokapi_core::*;
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base).with_write(true);
    /// # let (events_tx, _events) = tokio::sync::mpsc::channel(256);
    /// # let limiter = brokapi_runtime::BrokerLimiter::new(&brokapi_bcs::limits::default_limits());
    /// # let ud = UserDataConnector::start(UserDataConfig::new(cfg, "bcs-ud"), limiter, events_tx).await?;
    /// let everything = TimeRange::new(UnixNanos::ZERO, UnixNanos::from_nanos(i64::MAX / 2));
    /// let history = ud.executions(everything).await?;
    /// // A fresh account has no fills. The call still works.
    /// assert!(history.is_empty());
    /// # ud.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    async fn executions(&self, range: TimeRange) -> Result<Vec<Execution>, Error>;

    /// Starts the stream of your fills.
    ///
    /// Each fill arrives once as [`Event::Execution`](crate::Event::Execution), tagged with the
    /// returned id. The stream is lossless and has a buffer size from the connector config: keep
    /// draining your channel. Subscribe before you place, because fills that happened before the
    /// stream was open are not delivered (read [`executions`](Self::executions) to catch up). There is
    /// no way to end this subscription except shutting the connector down.
    ///
    /// # Errors
    ///
    /// - [`Error::Unsupported`] when the broker has no fill stream
    ///   ([`UserDataCaps::streams_executions`](crate::UserDataCaps::streams_executions) is `false`).
    ///   Poll [`executions`](Self::executions) yourself then.
    /// - [`Error::Transport`] when the subscription could not be made.
    ///
    /// # Examples
    ///
    /// See the example of the trait, which receives a fill.
    async fn subscribe_executions(&self) -> Result<SubscriptionId, Error>;
}

/// The account: money and positions, as a snapshot and as a stream.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use brokapi_bcs::limits::default_limits;
/// use brokapi_bcs::{UserDataConfig, UserDataConnector};
/// use brokapi_core::{Event, PortfolioSource, ShutdownMode, SubscribeOpts};
/// use brokapi_runtime::BrokerLimiter;
/// # use brokapi_bcs_protocol::config::BcsConfig;
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
/// # let cfg = BcsConfig::for_mock("mock-refresh-token", &server.http_base);
/// let (events_tx, mut events) = tokio::sync::mpsc::channel(256);
/// let limiter = BrokerLimiter::new(&default_limits());
/// let ud = UserDataConnector::start(UserDataConfig::new(cfg, "bcs-ud"), limiter, events_tx).await?;
///
/// // A stream: each event is a full snapshot, so the latest one wins if we read slowly.
/// // Subscribe first: an update that arrives before the subscription belongs to no subscription.
/// let sub = ud.subscribe_portfolio(SubscribeOpts::conflate()).await?;
/// let update = tokio::time::timeout(Duration::from_secs(8), async {
///     while let Some(env) = events.recv().await {
///         // The channel is shared by all subscriptions of the connector: pick ours by `sub`.
///         if env.sub == Some(sub) {
///             return Some(env.payload);
///         }
///     }
///     None
/// })
/// .await?
/// .expect("an update arrived");
/// assert!(matches!(
///     update,
///     Event::Portfolio(_) | Event::Positions(_) | Event::Balances(_)
/// ));
///
/// // The whole account at once, or only one part of it.
/// let portfolio = ud.portfolio().await?;
/// let balances = ud.balances().await?;
/// let positions = ud.positions().await?;
/// println!("{} currencies, {} positions", balances.len(), positions.len());
/// assert_eq!(portfolio.positions.len(), positions.len());
///
/// ud.shutdown(ShutdownMode::Immediate).await;
/// # Ok(())
/// # }
/// ```
#[async_trait]
pub trait PortfolioSource: Send + Sync {
    /// Reads money and positions at one moment.
    ///
    /// # Errors
    ///
    /// - [`Error::RateLimited`], [`Error::Transport`]: wait or repeat later.
    /// - [`Error::Rejected`] when the broker refuses the request.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::PortfolioSource;
    /// # use brokapi_bcs::{UserDataConfig, UserDataConnector};
    /// # use brokapi_core::*;
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
    /// # let (events_tx, _events) = tokio::sync::mpsc::channel(256);
    /// # let limiter = brokapi_runtime::BrokerLimiter::new(&brokapi_bcs::limits::default_limits());
    /// # let ud = UserDataConnector::start(UserDataConfig::new(cfg, "bcs-ud"), limiter, events_tx).await?;
    /// let p = ud.portfolio().await?;
    /// for b in &p.balances {
    ///     assert!(!b.available.is_negative() && !b.blocked.is_negative());
    /// }
    /// # ud.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    async fn portfolio(&self) -> Result<Portfolio, Error>;

    /// Reads the positions only.
    ///
    /// The BCS connector reads the whole portfolio and returns this part, so it costs the same as
    /// [`portfolio`](Self::portfolio).
    ///
    /// # Errors
    ///
    /// The same as [`portfolio`](Self::portfolio).
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::PortfolioSource;
    /// # use brokapi_bcs::{UserDataConfig, UserDataConnector};
    /// # use brokapi_core::*;
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
    /// # let (events_tx, _events) = tokio::sync::mpsc::channel(256);
    /// # let limiter = brokapi_runtime::BrokerLimiter::new(&brokapi_bcs::limits::default_limits());
    /// # let ud = UserDataConnector::start(UserDataConfig::new(cfg, "bcs-ud"), limiter, events_tx).await?;
    /// for p in ud.positions().await? {
    ///     println!("{}: {}", p.instrument.key.display(), p.qty);
    /// }
    /// # ud.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    async fn positions(&self) -> Result<Vec<Position>, Error>;

    /// Reads the money balances only.
    ///
    /// The BCS connector reads the whole portfolio and returns this part, so it costs the same as
    /// [`portfolio`](Self::portfolio).
    ///
    /// # Errors
    ///
    /// The same as [`portfolio`](Self::portfolio).
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::PortfolioSource;
    /// # use brokapi_bcs::{UserDataConfig, UserDataConnector};
    /// # use brokapi_core::*;
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
    /// # let (events_tx, _events) = tokio::sync::mpsc::channel(256);
    /// # let limiter = brokapi_runtime::BrokerLimiter::new(&brokapi_bcs::limits::default_limits());
    /// # let ud = UserDataConnector::start(UserDataConfig::new(cfg, "bcs-ud"), limiter, events_tx).await?;
    /// let free_money: Money = ud.balances().await?.iter().map(|b| b.available).sum();
    /// assert!(!free_money.is_negative());
    /// # ud.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    async fn balances(&self) -> Result<Vec<Balance>, Error>;

    /// Starts a stream of portfolio updates.
    ///
    /// Updates arrive as [`Event::Portfolio`](crate::Event::Portfolio), or as
    /// [`Event::Positions`](crate::Event::Positions) and [`Event::Balances`](crate::Event::Balances)
    /// where the connector sends the parts separately (the T-Bank connector does). Each is a full
    /// snapshot of its part and replaces the previous one, so
    /// [`Backpressure::Conflate`](crate::Backpressure::Conflate) fits.
    ///
    /// The broker may push a first snapshot as soon as its portfolio channel connects, which can be
    /// before you subscribe. Such an update has no subscription (`Envelope::sub` is `None`). Subscribe
    /// right after the connector starts and then read [`portfolio`](Self::portfolio) once for a known
    /// starting point.
    ///
    /// # Errors
    ///
    /// - [`Error::Unsupported`] when the connector has no portfolio stream.
    /// - [`Error::Transport`] when the subscription could not be made.
    ///
    /// # Examples
    ///
    /// See the example of the trait, which receives an update.
    async fn subscribe_portfolio(&self, opts: SubscribeOpts) -> Result<SubscriptionId, Error>;
}

/// Stop orders, take-profit and stop-limit - an extra capability next to [`OrderExecutor`]
/// ([`UserDataCaps::supports_stop_orders`](crate::UserDataCaps::supports_stop_orders) tells whether the broker has it).
///
/// A connector without stop orders implements the trait and answers `Error::Unsupported` to every
/// method. The command gate of the connector (`NotReconciled`, `Degraded`, shutdown) applies to
/// `place_stop` and `cancel_stop` like to `place` and `cancel`, and a stop order that got no answer
/// is `Error::Unknown`, never repeated by the library. A stop order waits at the broker and is not
/// an order on the exchange until the price crosses the trigger, see [`StopOrderKind`](crate::StopOrderKind).
///
/// # Examples
///
/// ```
/// use brokapi_bcs::limits::default_limits;
/// use brokapi_bcs::{UserDataConfig, UserDataConnector};
/// use brokapi_core::{
///     ClientOrderId, Error, InstrumentId, InstrumentKey, NativeId, OrderExecutor,
///     PlaceStopOrderRequest, Price, Qty, ShutdownMode, Side, StopExpiry, StopOrderExecutor,
///     StopOrderKind, StopOrderRef, StopOrderState,
/// };
/// use brokapi_runtime::BrokerLimiter;
/// # use brokapi_bcs_protocol::config::BcsConfig;
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
/// # let cfg = BcsConfig::for_mock("mock-refresh-token", &server.http_base).with_write(true);
/// let (events_tx, _events) = tokio::sync::mpsc::channel(256);
/// let limiter = BrokerLimiter::new(&default_limits());
/// let ud = UserDataConnector::start(UserDataConfig::new(cfg, "bcs-ud"), limiter, events_tx).await?;
/// ud.reconcile(&[]).await?; // stop orders are behind the same gate as plain orders
/// assert!(ud.caps().supports_stop_orders);
///
/// let sber = InstrumentId::new(
///     InstrumentKey::new("SBER", "TQBR"),
///     NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() },
/// );
///
/// // A stop-loss: sell 1 if the price falls to 200, far below the market so it only waits.
/// let cid = ClientOrderId::new_v4();
/// let ack = ud
///     .place_stop(PlaceStopOrderRequest {
///         cid,
///         instrument: sber.clone(),
///         side: Side::Sell,
///         kind: StopOrderKind::StopLoss,
///         qty: Qty::from(1),
///         stop_price: Price::from(200),
///         limit_price: None, // market order at the trigger
///         expiry: StopExpiry::GoodTillCancel,
///     })
///     .await?;
///
/// // It is listed with its parameters while it waits.
/// let waiting = ud.active_stop_orders().await?;
/// let mine = waiting.iter().find(|s| s.broker_id == ack.broker_id).expect("listed");
/// assert_eq!(mine.state, StopOrderState::Active);
///
/// // Cancel it by the cid we gave. `StopOrderRef::Broker` names it by the broker's id instead.
/// ud.cancel_stop(StopOrderRef::Cid(cid)).await?;
///
/// // A stop-limit needs a limit price. Without it the request is refused before anything is sent.
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
/// ud.shutdown(ShutdownMode::Immediate).await;
/// # Ok(())
/// # }
/// ```
#[async_trait]
pub trait StopOrderExecutor: Send + Sync {
    /// Puts a stop order and waits for the broker's decision.
    ///
    /// The `cid` is the same kind of UUID as for plain orders and follows the same rule: it is sent
    /// at most once, a repeat is refused with [`Error::Rejected`] (BCS: `duplicate_place`). `Ok`
    /// means the broker accepted the stop order, it is waiting for its trigger. The library checks
    /// the shape of the request (a stop-limit needs `limit_price`) and nothing else: the trigger
    /// direction against the market price is the broker's to judge.
    ///
    /// # Errors
    ///
    /// - [`Error::Unsupported`] when the broker has no stop orders. Nothing was sent.
    /// - [`Error::NotReconciled`], [`Error::Degraded`], [`Error::ShuttingDown`]: the gate is closed,
    ///   nothing was sent.
    /// - [`Error::Rejected`] when the request is wrong or the broker says no: a stop-limit without a
    ///   limit price, a repeated cid, an expiry the broker does not have for this kind.
    /// - [`Error::RateLimited`]: not executed, wait `retry_after`. Use a new cid for the retry.
    /// - [`Error::Unknown`] when the request may have reached the broker and there is no answer. Do
    ///   NOT place again. Look in [`active_stop_orders`](Self::active_stop_orders) for the cid.
    /// - [`Error::Transport`] when the failure happened before the request left.
    ///
    /// # Examples
    ///
    /// See the example of the trait.
    async fn place_stop(&self, req: PlaceStopOrderRequest) -> Result<OrderAck, Error>;

    /// Cancels a stop order that has not triggered.
    ///
    /// Name it by the cid you gave ([`StopOrderRef::Cid`]) or by the broker's id
    /// ([`StopOrderRef::Broker`]). A cid is known from what this process has seen. After a restart a
    /// connector may not be able to find it, use the broker id then (from
    /// [`active_stop_orders`](Self::active_stop_orders)).
    ///
    /// # Errors
    ///
    /// - [`Error::Unsupported`] when the broker has no stop orders.
    /// - [`Error::NotReconciled`], [`Error::Degraded`], [`Error::ShuttingDown`]: the gate is closed.
    /// - [`Error::Rejected`] when the broker refuses: the stop order is unknown (BCS: `not_found`),
    ///   or it already triggered.
    /// - [`Error::RateLimited`]: not executed, wait `retry_after`.
    /// - [`Error::Unknown`] when the answer is missing or leaves the stop order alive. Look in
    ///   [`active_stop_orders`](Self::active_stop_orders) before you decide.
    /// - [`Error::Transport`] when the failure happened before the request left.
    ///
    /// # Examples
    ///
    /// See the example of the trait.
    async fn cancel_stop(&self, stop: StopOrderRef) -> Result<(), Error>;

    /// Lists the stop orders that wait for their trigger.
    ///
    /// `cid` of an entry is `None` when the connector cannot tie the broker's stop order to a cid you
    /// gave (placed by someone else, or placed before this process started and not echoed). Use
    /// `broker_id` then.
    ///
    /// # Errors
    ///
    /// - [`Error::Unsupported`] when the broker has no stop orders.
    /// - [`Error::RateLimited`], [`Error::Transport`]: wait or repeat later.
    ///
    /// # Examples
    ///
    /// See the example of the trait.
    async fn active_stop_orders(&self) -> Result<Vec<StopOrderSnapshot>, Error>;

    /// Stream of stop order statuses. `Unsupported` where the broker has no such stream.
    ///
    /// The BCS connector has none: its order events channel is documented for plain orders only.
    /// Poll [`active_stop_orders`](Self::active_stop_orders) instead. The library does not poll for you.
    ///
    /// # Errors
    ///
    /// - [`Error::Unsupported`] when the broker has no such stream. BCS: capability
    ///   `stop_order_stream`.
    /// - [`Error::Transport`] when the subscription could not be made.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::{Error, StopOrderExecutor};
    /// # use brokapi_bcs::{UserDataConfig, UserDataConnector};
    /// # use brokapi_core::*;
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    /// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base).with_write(true);
    /// # let (events_tx, _events) = tokio::sync::mpsc::channel(256);
    /// # let limiter = brokapi_runtime::BrokerLimiter::new(&brokapi_bcs::limits::default_limits());
    /// # let ud = UserDataConnector::start(UserDataConfig::new(cfg, "bcs-ud"), limiter, events_tx).await?;
    /// match ud.subscribe_stop_orders().await {
    ///     Err(Error::Unsupported { capability }) => assert_eq!(capability, "stop_order_stream"),
    ///     other => panic!("BCS has no stop order stream, got {other:?}"),
    /// }
    /// # ud.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    async fn subscribe_stop_orders(&self) -> Result<SubscriptionId, Error>;
}
