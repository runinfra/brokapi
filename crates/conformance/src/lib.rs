//! Shared behaviour scenarios that every broker adapter must pass against its own mock.
//!
//! This is the test layer of the connector library. Each adapter (`brokapi-bcs`, `brokapi-tbank`,
//! `brokapi-finam`, ...) implements the capability traits of [`brokapi_core`], and each runs the same
//! scenarios from this crate against its own in-process broker imitation (see `brokapi-mocks`).
//! So all brokers behave the same through the traits: the same refusal is the same
//! [`brokapi_core::Error`], the same stream gives the same envelope. The scenarios use only the
//! traits, never an adapter's own types.
//!
//! A scenario is an `async fn` that takes the connector (and sometimes the event receiver). It
//! returns `()` and **panics** when the adapter breaks the contract. The panic message starts with
//! `[conformance:<scenario name>]` and gives the reason, so the failing scenario and the cause are
//! visible in the test output. A scenario that passes leaves the connector usable, with its test
//! orders cancelled.
//!
//! What a scenario proves is that the adapter agrees with the mock. The mocks are written from the
//! broker documentation. They do not prove how the live broker behaves.
//!
//! The mock behind the adapter must:
//! - serve an order book (both sides non-empty) for the instrument passed in;
//! - stream at least one order book update after subscribe;
//! - acknowledge a limit order and cancel it;
//! - refuse a limit order whose price is [`REJECT_PRICE`] (for [`rejected_order_does_not_block_the_instrument`]);
//! - serve a portfolio snapshot and push at least one portfolio update after subscribe;
//! - for [`stop_order_far_from_market`]: accept a stop order, list it while it waits, cancel it;
//! - for [`market_data_history`]: serve the tape, candles for the range, trading status and a schedule;
//! - for [`market_data_streams`]: push a market trade, a quote and a candle after each subscribe;
//! - for [`order_state_events`]: push an order event when an order is placed and when it is cancelled;
//! - for [`active_orders_lifecycle`] / [`replace_order`]: keep placed orders in `active_orders`;
//! - for [`executions`]: execute an order on request (the adapter's closure) and keep the trade history.
//!
//! Trading scenarios need the command gate open, so call [`reconcile`] first on a user data
//! connector. Until the first reconcile every command fails with
//! [`brokapi_core::Error::NotReconciled`].
//!
//! The book is at <https://brokapi.ru/docs/>.
//!
//! # Examples
//!
//! The usual adapter test: start the mock, start the connector on it, run the scenarios, shut down.
//! Here the adapter is the BCS connector.
//!
//! ```
//! # use brokapi_core::Price;
//! use brokapi_bcs::limits::default_limits;
//! use brokapi_bcs::{UserDataConfig, UserDataConnector};
//! use brokapi_core::{InstrumentId, InstrumentKey, NativeId, ShutdownMode};
//! use brokapi_runtime::BrokerLimiter;
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
//! # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base).with_write(true);
//! # let (tx, _rx) = tokio::sync::mpsc::channel(256);
//! # let ud = UserDataConnector::start(UserDataConfig::new(cfg, "bcs-ud"), BrokerLimiter::new(&default_limits()), tx).await?;
//! # let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() });
//! // `ud` is a connected user data connector, `sber` the test instrument.
//! brokapi_conformance::reconcile(&ud).await;
//! brokapi_conformance::place_and_cancel(&ud, &sber, Price::from(1)).await;
//! # ud.shutdown(ShutdownMode::Immediate).await;
//! # Ok(())
//! # }
//! ```

#![warn(missing_docs)]

use std::future::Future;
use std::time::Duration;

use brokapi_core::{
    ClientOrderId, Decimal, Envelope, Error, Event, ExecutionSource, InstrumentId, InstrumentQuery,
    InstrumentSource, MarketDataSource, OrderBook, OrderBookSource, OrderExecutor, OrderState,
    OrderType, PlaceOrderRequest, PlaceStopOrderRequest, PortfolioSource, Price, Qty,
    ReplaceOrderRequest, Side, StopExpiry, StopOrderExecutor, StopOrderKind, StopOrderRef,
    StopOrderState, SubscribeOpts, SubscriptionId, TimeRange, Timeframe, TradingAvailability,
    UnixNanos,
};
use tokio::sync::mpsc;
use tokio::time::timeout;

/// A limit order at exactly this price is refused by every mock (the broker says no).
///
/// Scenarios use it to get a definite refusal on demand. The refusal must be a clear answer from
/// the broker side, not a timeout, so the library reports it as an error other than
/// [`brokapi_core::Error::Unknown`]. The value is a price the mocks agree on. It has no meaning at a live
/// broker, never send it there.
///
/// # Examples
///
/// A refused order is an error, and it leaves the connector able to take the next order:
///
/// ```
/// # use brokapi_bcs::limits::default_limits;
/// # use brokapi_bcs::{UserDataConfig, UserDataConnector};
/// # use brokapi_core::{
/// #     ClientOrderId, Error, InstrumentId, InstrumentKey, NativeId, OrderExecutor, OrderType,
/// #     PlaceOrderRequest, Price, Qty, ShutdownMode, Side,
/// # };
/// # use brokapi_runtime::BrokerLimiter;
/// use brokapi_conformance::REJECT_PRICE;
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
/// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base).with_write(true);
/// # let (tx, _rx) = tokio::sync::mpsc::channel(256);
/// # let ud = UserDataConnector::start(UserDataConfig::new(cfg, "bcs-ud"), BrokerLimiter::new(&default_limits()), tx).await?;
/// # let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() });
/// brokapi_conformance::reconcile(&ud).await; // opens the command gate
/// let refused = ud
///     .place(PlaceOrderRequest {
///         cid: ClientOrderId::new_v4(),
///         instrument: sber,
///         side: Side::Buy,
///         order_type: OrderType::Limit,
///         qty: lots(1),
///         price: Some(REJECT_PRICE),
///     })
///     .await;
/// assert!(refused.is_err());
/// assert!(!matches!(refused, Err(Error::Unknown)), "a refusal is a clear answer");
/// # ud.shutdown(ShutdownMode::Immediate).await;
/// # Ok(())
/// # }
/// ```
pub const REJECT_PRICE: Price = Price::new(Decimal::from_parts(1337, 0, 0, false, 2));

/// How long a scenario waits for a pushed event, or for one command, before it fails.
///
/// [`next_matching`] gives up after this time in total. Scenarios that must not hang (for example
/// [`rejected_order_does_not_block_the_instrument`]) wrap each step in the same limit. A mock that is
/// slower than this makes the scenario fail with a "no expected event" or "hung" message.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// assert_eq!(brokapi_conformance::EVENT_TIMEOUT, Duration::from_secs(8));
/// ```
pub const EVENT_TIMEOUT: Duration = Duration::from_secs(8);

fn fail(scenario: &str, why: impl std::fmt::Display) -> ! {
    panic!("[conformance:{scenario}] {why}")
}

/// One lot of the instruments the mocks serve, in units: 10. The scenarios order whole lots, because an
/// order of units that is not a whole number of lots is refused (`invalid_qty`) by the connectors that
/// know the lot. A connector that does not look the lot up itself (BCS, Finam) is told the lot of the
/// test instrument with `price_grid().set_lot(..)` in the test.
pub const LOT: i64 = 10;

/// `n` lots of the test instruments, in units.
pub fn lots(n: i64) -> Qty {
    Qty::from(n * LOT)
}

/// Reads envelopes from `rx` until `pred` accepts one, and returns that one.
///
/// Envelopes that do not match are read and thrown away, so they are not seen by later calls. Use a
/// precise `pred` (the event kind and, when several subscriptions run, the subscription id).
/// `scenario` is only the name shown in the failure message.
///
/// The whole wait is limited to [`EVENT_TIMEOUT`], not each envelope.
///
/// # Panics
///
/// With `[conformance:<scenario>] ...` when the channel is closed before a matching envelope arrives
/// (`event channel closed before the expected event`), and when none arrives within [`EVENT_TIMEOUT`]
/// (`no expected event within 8s`).
///
/// # Examples
///
/// ```
/// use brokapi_conformance::next_matching;
/// use brokapi_core::{Envelope, Event, Timestamps, UnixNanos};
/// use tokio::sync::mpsc;
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// let (tx, mut rx) = mpsc::channel(4);
/// let ts = Timestamps::new(UnixNanos::ZERO, std::time::Instant::now());
/// tx.send(Envelope::new("demo".into(), ts, Event::Balances(vec![]))).await.unwrap();
/// tx.send(Envelope::new("demo".into(), ts, Event::Positions(vec![]))).await.unwrap();
///
/// // The balances envelope is skipped, the positions envelope is returned.
/// let env = next_matching("my_scenario", &mut rx, |e| matches!(e.payload, Event::Positions(_))).await;
/// assert!(matches!(env.payload, Event::Positions(_)));
/// # }
/// ```
///
/// When nothing matching can arrive, the scenario fails with its name in the message:
///
/// ```should_panic
/// use brokapi_conformance::next_matching;
/// use brokapi_core::{Envelope, Event};
/// use tokio::sync::mpsc;
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// let (tx, mut rx) = mpsc::channel::<Envelope>(1);
/// drop(tx); // the connector went away
/// // panics: [conformance:my_scenario] event channel closed before the expected event
/// next_matching("my_scenario", &mut rx, |_| true).await;
/// # }
/// ```
pub async fn next_matching(
    scenario: &str,
    rx: &mut mpsc::Receiver<Envelope>,
    pred: impl Fn(&Envelope) -> bool,
) -> Envelope {
    let got = timeout(EVENT_TIMEOUT, async {
        while let Some(env) = rx.recv().await {
            if pred(&env) {
                return Some(env);
            }
        }
        None
    })
    .await;
    match got {
        Ok(Some(env)) => env,
        Ok(None) => fail(scenario, "event channel closed before the expected event"),
        Err(_) => fail(
            scenario,
            format!("no expected event within {EVENT_TIMEOUT:?}"),
        ),
    }
}

fn book_sides(
    book: &OrderBook,
) -> (
    &InstrumentId,
    &[brokapi_core::BookLevel],
    &[brokapi_core::BookLevel],
) {
    match book {
        OrderBook::Snapshot {
            instrument,
            bids,
            asks,
            ..
        }
        | OrderBook::Increment {
            instrument,
            bids,
            asks,
            ..
        } => (instrument, bids, asks),
    }
}

/// REST order book: the right instrument, the asked depth, sorted best-first, not crossed.
///
/// Checks on `md.order_book(id, depth)`:
/// - the call succeeds and returns [`brokapi_core::OrderBook::Snapshot`] (not an increment);
/// - the book is for the requested instrument (same [`brokapi_core::InstrumentKey`]);
/// - both sides have at least one level;
/// - neither side has more than `depth` levels;
/// - bids are sorted from the highest price, asks from the lowest (equal neighbours pass);
/// - the best bid is strictly below the best ask.
///
/// The mock must serve both sides for `id` and respect `depth`.
///
/// # Panics
///
/// With `[conformance:order_book_snapshot] <reason>` when a check fails or `order_book` returns an error.
///
/// # Examples
///
/// ```
/// # use brokapi_bcs::limits::default_limits;
/// # use brokapi_bcs::{MarketDataConfig, MarketDataConnector};
/// # use brokapi_core::{InstrumentId, InstrumentKey, NativeId, ShutdownMode};
/// # use brokapi_runtime::BrokerLimiter;
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
/// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
/// # let (tx, mut rx) = tokio::sync::mpsc::channel(256);
/// # let md = MarketDataConnector::start(MarketDataConfig::new(cfg, "bcs-md"), BrokerLimiter::new(&default_limits()), tx).await?;
/// # let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() });
/// brokapi_conformance::order_book_snapshot(&md, &sber, 20).await;
/// # md.shutdown(ShutdownMode::Immediate).await;
/// # Ok(())
/// # }
/// ```
///
/// A scenario that fails stops the test with its name and the reason. Depth 0 is something no book can
/// satisfy, so this one panics:
///
/// ```should_panic(expected = "[conformance:order_book_snapshot] depth 0 exceeded")
/// # use brokapi_bcs::limits::default_limits;
/// # use brokapi_bcs::{MarketDataConfig, MarketDataConnector};
/// # use brokapi_core::{InstrumentId, InstrumentKey, NativeId, ShutdownMode};
/// # use brokapi_runtime::BrokerLimiter;
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
/// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
/// # let (tx, mut rx) = tokio::sync::mpsc::channel(256);
/// # let md = MarketDataConnector::start(MarketDataConfig::new(cfg, "bcs-md"), BrokerLimiter::new(&default_limits()), tx).await?;
/// # let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() });
/// // panics with a message that starts with [conformance:order_book_snapshot]
/// brokapi_conformance::order_book_snapshot(&md, &sber, 0).await;
/// # md.shutdown(ShutdownMode::Immediate).await;
/// # Ok(())
/// # }
/// ```
pub async fn order_book_snapshot<M: OrderBookSource + ?Sized>(
    md: &M,
    id: &InstrumentId,
    depth: u32,
) {
    const S: &str = "order_book_snapshot";
    let book = md
        .order_book(id, depth)
        .await
        .unwrap_or_else(|e| fail(S, format!("order_book failed: {e:?}")));
    let OrderBook::Snapshot { .. } = &book else {
        fail(S, "REST order_book must return a Snapshot");
    };
    let (inst, bids, asks) = book_sides(&book);
    if inst.key != id.key {
        fail(
            S,
            format!(
                "instrument {} != requested {}",
                inst.key.display(),
                id.key.display()
            ),
        );
    }
    if bids.is_empty() || asks.is_empty() {
        fail(S, "mock must serve both sides");
    }
    if bids.len() > depth as usize || asks.len() > depth as usize {
        fail(
            S,
            format!(
                "depth {depth} exceeded: {} bids, {} asks",
                bids.len(),
                asks.len()
            ),
        );
    }
    if bids.windows(2).any(|w| w[0].price < w[1].price) {
        fail(S, "bids must be sorted from best (highest) price");
    }
    if asks.windows(2).any(|w| w[0].price > w[1].price) {
        fail(S, "asks must be sorted from best (lowest) price");
    }
    if bids[0].price >= asks[0].price {
        fail(S, "book is crossed: best bid >= best ask");
    }
}

/// Order book stream: subscribe, then an order book envelope arrives tagged with our source and subscription id.
///
/// Subscribes with [`brokapi_core::SubscribeOpts::conflate`] and waits (up to [`EVENT_TIMEOUT`]) for the
/// first [`brokapi_core::Event::OrderBook`] envelope on `rx`. Checks that:
/// - `env.source` equals `source` (the label the connector was configured with);
/// - `env.sub` is the id that `subscribe_order_book` returned;
/// - the book is for the requested instrument.
///
/// Returns the subscription id and leaves the subscription open, so the caller can pass it to
/// [`unsubscribe`]. `rx` must be the receiver of the connector under test. Envelopes read before the
/// match are discarded. If an older order book subscription is still running on the same connector, its
/// book may arrive first and fail the sub check, so use a connector with no other book streams.
///
/// The mock must push at least one order book update after subscribe.
///
/// # Panics
///
/// With `[conformance:order_book_stream] <reason>` when the subscribe fails, when no order book arrives
/// (see [`next_matching`]), or when the source, subscription id or instrument is wrong.
///
/// # Examples
///
/// ```
/// # use brokapi_bcs::limits::default_limits;
/// # use brokapi_bcs::{MarketDataConfig, MarketDataConnector};
/// # use brokapi_core::{InstrumentId, InstrumentKey, NativeId, ShutdownMode};
/// # use brokapi_runtime::BrokerLimiter;
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
/// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
/// # let (tx, mut rx) = tokio::sync::mpsc::channel(256);
/// # let md = MarketDataConnector::start(MarketDataConfig::new(cfg, "bcs-md"), BrokerLimiter::new(&default_limits()), tx).await?;
/// # let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() });
/// let sub = brokapi_conformance::order_book_stream(&md, &mut rx, "bcs-md", &sber).await;
/// brokapi_conformance::unsubscribe(&md, sub).await;
/// # md.shutdown(ShutdownMode::Immediate).await;
/// # Ok(())
/// # }
/// ```
pub async fn order_book_stream<M: OrderBookSource + ?Sized>(
    md: &M,
    rx: &mut mpsc::Receiver<Envelope>,
    source: &str,
    id: &InstrumentId,
) -> SubscriptionId {
    const S: &str = "order_book_stream";
    let sub = md
        .subscribe_order_book(id, SubscribeOpts::conflate())
        .await
        .unwrap_or_else(|e| fail(S, format!("subscribe_order_book failed: {e:?}")));
    let env = next_matching(S, rx, |e| matches!(e.payload, Event::OrderBook(_))).await;
    if env.source.as_ref() != source {
        fail(S, format!("source {:?} != {source:?}", env.source));
    }
    if env.sub != Some(sub) {
        fail(
            S,
            format!("envelope sub {:?} != subscription {sub:?}", env.sub),
        );
    }
    let Event::OrderBook(book) = &env.payload else {
        unreachable!()
    };
    if book_sides(book).0.key != id.key {
        fail(S, "order book for a different instrument");
    }
    sub
}

/// Unsubscribing an active subscription succeeds, and doing it again is refused the same way at every broker.
///
/// The first call must return `Ok`. The second call, for an id that is already closed, must return
/// [`Error::Rejected`] with code `unknown_subscription`: not `Ok`, not a transport error, not a panic.
/// `sub` is the id of a live subscription, for example the one [`order_book_stream`] returned.
///
/// # Panics
///
/// With `[conformance:unsubscribe] ...` when the first call returns an error, or the second one is
/// not the refusal described above.
///
/// # Examples
///
/// ```
/// # use brokapi_bcs::limits::default_limits;
/// # use brokapi_bcs::{MarketDataConfig, MarketDataConnector};
/// # use brokapi_core::{InstrumentId, InstrumentKey, NativeId, ShutdownMode};
/// # use brokapi_runtime::BrokerLimiter;
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
/// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
/// # let (tx, mut rx) = tokio::sync::mpsc::channel(256);
/// # let md = MarketDataConnector::start(MarketDataConfig::new(cfg, "bcs-md"), BrokerLimiter::new(&default_limits()), tx).await?;
/// # let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() });
/// let sub = brokapi_conformance::order_book_stream(&md, &mut rx, "bcs-md", &sber).await;
/// brokapi_conformance::unsubscribe(&md, sub).await;
/// # md.shutdown(ShutdownMode::Immediate).await;
/// # Ok(())
/// # }
/// ```
pub async fn unsubscribe<M: OrderBookSource + ?Sized>(md: &M, sub: SubscriptionId) {
    const S: &str = "unsubscribe";
    md.unsubscribe(sub)
        .await
        .unwrap_or_else(|e| fail(S, format!("unsubscribe failed: {e:?}")));
    expect_unknown_subscription(S, md.unsubscribe(sub).await);
}

/// Unsubscribing an id the connector never gave out is refused: [`Error::Rejected`] with code
/// `unknown_subscription`. The same answer at every broker, so a bot needs one branch for it.
///
/// # Panics
///
/// With `[conformance:unsubscribe_unknown] ...` when the answer is anything else.
///
/// # Examples
///
/// ```
/// # use brokapi_bcs::limits::default_limits;
/// # use brokapi_bcs::{MarketDataConfig, MarketDataConnector};
/// # use brokapi_core::ShutdownMode;
/// # use brokapi_runtime::BrokerLimiter;
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
/// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
/// # let (tx, _rx) = tokio::sync::mpsc::channel(256);
/// # let md = MarketDataConnector::start(MarketDataConfig::new(cfg, "bcs-md"), BrokerLimiter::new(&default_limits()), tx).await?;
/// brokapi_conformance::unsubscribe_unknown(&md).await;
/// # md.shutdown(ShutdownMode::Immediate).await;
/// # Ok(())
/// # }
/// ```
pub async fn unsubscribe_unknown<M: OrderBookSource + ?Sized>(md: &M) {
    expect_unknown_subscription(
        "unsubscribe_unknown",
        md.unsubscribe(SubscriptionId::new(u64::MAX - 1)).await,
    );
}

fn expect_unknown_subscription(scenario: &str, outcome: Result<(), Error>) {
    match outcome {
        Err(Error::Rejected { code, .. }) if code == "unknown_subscription" => {}
        other => fail(
            scenario,
            format!(
                "unsubscribe of an unknown id must be Rejected(unknown_subscription), got {other:?}"
            ),
        ),
    }
}

/// Instrument directory: a search finds the instrument, and its trading parameters are usable for order sizing.
///
/// Checks:
/// - `md.find(query)` succeeds and the result contains an instrument with the same
///   [`brokapi_core::InstrumentKey`] as `id`;
/// - `md.trading_params(id)` succeeds with `lot_size > 0` and `price_step > 0`. Both are used to round
///   order quantities and prices, so zero would make every order invalid.
///
/// `query` must be a search that finds `id`. The mock must list that instrument and serve its parameters.
///
/// # Panics
///
/// With `[conformance:instrument_lookup] <reason>` when a call fails, the instrument is not found, or a
/// parameter is not positive.
///
/// # Examples
///
/// ```
/// # use brokapi_bcs::limits::default_limits;
/// # use brokapi_bcs::{MarketDataConfig, MarketDataConnector};
/// # use brokapi_core::{InstrumentQuery, InstrumentId, InstrumentKey, NativeId, ShutdownMode};
/// # use brokapi_runtime::BrokerLimiter;
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
/// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
/// # let (tx, mut rx) = tokio::sync::mpsc::channel(256);
/// # let md = MarketDataConnector::start(MarketDataConfig::new(cfg, "bcs-md"), BrokerLimiter::new(&default_limits()), tx).await?;
/// # let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() });
/// let query = InstrumentQuery::ByTicker { ticker: "SBER".into(), exchange: None };
/// brokapi_conformance::instrument_lookup(&md, query, &sber).await;
/// # md.shutdown(ShutdownMode::Immediate).await;
/// # Ok(())
/// # }
/// ```
pub async fn instrument_lookup<M: InstrumentSource + ?Sized>(
    md: &M,
    query: InstrumentQuery,
    id: &InstrumentId,
) {
    const S: &str = "instrument_lookup";
    let found = md
        .find(query)
        .await
        .unwrap_or_else(|e| fail(S, format!("find failed: {e:?}")));
    if !found.iter().any(|i| i.id.key == id.key) {
        fail(S, format!("find did not return {}", id.key.display()));
    }
    let params = md
        .trading_params(id)
        .await
        .unwrap_or_else(|e| fail(S, format!("trading_params failed: {e:?}")));
    if !params.lot_size.is_positive() || !params.price_step.is_positive() {
        fail(
            S,
            format!(
                "lot_size {} / price_step {} must be > 0",
                params.lot_size, params.price_step
            ),
        );
    }
}

/// Places a far-from-market limit order, reads its state by cid, cancels it.
///
/// Steps and checks:
/// 1. `place` a buy limit order, one lot, at `price`, with a fresh cid. The ack must carry the same
///    cid and a non-empty broker order id.
/// 2. `order_state(cid)` must return a snapshot for that cid that is not `Rejected`.
/// 3. `cancel(cid)` must succeed.
///
/// `price` must be far below the market so the order rests and does not fill, and must be a price the
/// mock accepts (not [`REJECT_PRICE`]). There is no retry: a local rate-limit refusal is a failure
/// here. The command gate must be open, so call [`reconcile`] first.
///
/// The mock must acknowledge a limit order, report its state, and cancel it.
///
/// # Panics
///
/// With `[conformance:place_and_cancel] <reason>` when `place`, `order_state` or `cancel` fails, or the
/// ack or snapshot is wrong. A `NotReconciled` error here means [`reconcile`] was not run first.
///
/// # Examples
///
/// ```
/// # use brokapi_core::{Price};
/// # use brokapi_bcs::limits::default_limits;
/// # use brokapi_bcs::{UserDataConfig, UserDataConnector};
/// # use brokapi_core::{InstrumentId, InstrumentKey, NativeId, ShutdownMode};
/// # use brokapi_runtime::BrokerLimiter;
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let opts = std::sync::Arc::new(brokapi_mocks::bcs::MockOptions::default());
/// # let server = brokapi_mocks::bcs::MockServer::start(std::sync::Arc::clone(&opts)).await;
/// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base).with_write(true);
/// # let (tx, mut rx) = tokio::sync::mpsc::channel(256);
/// # let ud = UserDataConnector::start(UserDataConfig::new(cfg, "bcs-ud"), BrokerLimiter::new(&default_limits()), tx).await?;
/// # let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() });
/// brokapi_conformance::reconcile(&ud).await;
/// brokapi_conformance::place_and_cancel(&ud, &sber, Price::from(1)).await;
/// # ud.shutdown(ShutdownMode::Immediate).await;
/// # Ok(())
/// # }
/// ```
pub async fn place_and_cancel<U: OrderExecutor + ?Sized>(ud: &U, id: &InstrumentId, price: Price) {
    const S: &str = "place_and_cancel";
    let cid = ClientOrderId::new_v4();
    let ack = ud
        .place(PlaceOrderRequest {
            cid,
            instrument: id.clone(),
            side: Side::Buy,
            order_type: OrderType::Limit,
            qty: lots(1),
            price: Some(price),
        })
        .await
        .unwrap_or_else(|e| fail(S, format!("place failed: {e:?}")));
    if ack.cid != cid {
        fail(S, format!("ack cid {} != sent {cid}", ack.cid));
    }
    if ack.broker_id.as_str().is_empty() {
        fail(S, "ack without broker order id");
    }
    let snap = ud
        .order_state(cid)
        .await
        .unwrap_or_else(|e| fail(S, format!("order_state failed: {e:?}")));
    if snap.cid != cid {
        fail(S, format!("order_state cid {} != {cid}", snap.cid));
    }
    if matches!(snap.state, OrderState::Rejected { .. }) {
        fail(S, format!("order rejected: {:?}", snap.state));
    }
    ud.cancel(cid)
        .await
        .unwrap_or_else(|e| fail(S, format!("cancel failed: {e:?}")));
}

/// `order_state` of a `cid` the broker has never seen is a refusal: [`Error::Rejected`] with code
/// `not_found` ([`brokapi_core::NOT_FOUND`]), spelled the same at every broker.
///
/// The same answer at every broker, so a bot that recovers from [`Error::Unknown`] with `order_state`
/// needs one branch for "the broker does not know the order (yet)". Not `Transport`, not `Ok`.
///
/// # Panics
///
/// With `[conformance:order_state_unknown_cid] ...` when the answer is anything else.
///
/// # Examples
///
/// ```
/// # use brokapi_bcs::limits::default_limits;
/// # use brokapi_bcs::{UserDataConfig, UserDataConnector};
/// # use brokapi_core::ShutdownMode;
/// # use brokapi_runtime::BrokerLimiter;
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
/// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base).with_write(true);
/// # let (tx, _rx) = tokio::sync::mpsc::channel(256);
/// # let ud = UserDataConnector::start(UserDataConfig::new(cfg, "bcs-ud"), BrokerLimiter::new(&default_limits()), tx).await?;
/// brokapi_conformance::order_state_unknown_cid(&ud).await;
/// # ud.shutdown(ShutdownMode::Immediate).await;
/// # Ok(())
/// # }
/// ```
pub async fn order_state_unknown_cid<U: OrderExecutor + ?Sized>(ud: &U) {
    const S: &str = "order_state_unknown_cid";
    match ud.order_state(ClientOrderId::new_v4()).await {
        Err(e) if e.is_not_found() => {}
        other => fail(
            S,
            format!("order_state of an unknown cid must be Rejected(not_found), got {other:?}"),
        ),
    }
}

fn limit_buy(id: &InstrumentId, price: Price) -> PlaceOrderRequest {
    PlaceOrderRequest {
        cid: ClientOrderId::new_v4(),
        instrument: id.clone(),
        side: Side::Buy,
        order_type: OrderType::Limit,
        qty: lots(1),
        price: Some(price),
    }
}

/// Places a limit order; a local rate-limit refusal (nothing was sent) is waited out and retried
/// under a fresh cid. Returns the cid that was finally used.
async fn place_patient<U: OrderExecutor + ?Sized>(
    ud: &U,
    id: &InstrumentId,
    price: Price,
) -> (
    ClientOrderId,
    Result<brokapi_core::OrderAck, brokapi_core::Error>,
) {
    for _ in 0..20 {
        let req = limit_buy(id, price);
        let cid = req.cid;
        match ud.place(req).await {
            Err(brokapi_core::Error::RateLimited { retry_after }) => {
                tokio::time::sleep(retry_after).await
            }
            other => return (cid, other),
        }
    }
    fail("place_patient", "still rate limited after 20 waits")
}

/// An order the broker refuses must not block the instrument: the next order on the same instrument goes through.
///
/// This guards a bug BCS once had: a failed place left the per-instrument queue busy, and every later
/// order got "command queued" until restart. Two rounds:
/// 1. One after another: a limit order at [`REJECT_PRICE`] must fail with an error other than
///    [`brokapi_core::Error::Unknown`] (a refusal is a clear answer, not a lost one). Then an order at
///    `ok_price` must succeed and is cancelled.
/// 2. At the same time (`tokio::join!`): the refused order and the good order are in flight together.
///    The refused one must fail, the good one must succeed and is cancelled.
///
/// Every place waits at most [`EVENT_TIMEOUT`], so a stuck queue fails the scenario instead of hanging.
/// A local rate-limit refusal (nothing was sent) is waited out and the order is retried under a fresh
/// cid, up to 20 times. A cid is never sent twice, because a repeated place could create a second order.
///
/// `ok_price` is a far-from-market price the mock accepts. The mock must refuse [`REJECT_PRICE`] with a
/// definite refusal. The command gate must be open, so call [`reconcile`] first.
///
/// # Panics
///
/// With `[conformance:rejected_order_does_not_block_the_instrument] <reason>` when the refused order is
/// accepted, is reported as `Unknown`, when a later order fails or hangs, or when a cancel fails.
///
/// # Examples
///
/// ```
/// # use brokapi_core::{Price};
/// # use brokapi_bcs::limits::default_limits;
/// # use brokapi_bcs::{UserDataConfig, UserDataConnector};
/// # use brokapi_core::{InstrumentId, InstrumentKey, NativeId, ShutdownMode};
/// # use brokapi_runtime::BrokerLimiter;
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let opts = std::sync::Arc::new(brokapi_mocks::bcs::MockOptions::default());
/// # let server = brokapi_mocks::bcs::MockServer::start(std::sync::Arc::clone(&opts)).await;
/// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base).with_write(true);
/// # let (tx, mut rx) = tokio::sync::mpsc::channel(256);
/// # let ud = UserDataConnector::start(UserDataConfig::new(cfg, "bcs-ud"), BrokerLimiter::new(&default_limits()), tx).await?;
/// # let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() });
/// brokapi_conformance::reconcile(&ud).await;
/// brokapi_conformance::rejected_order_does_not_block_the_instrument(&ud, &sber, Price::from(1)).await;
/// # ud.shutdown(ShutdownMode::Immediate).await;
/// # Ok(())
/// # }
/// ```
pub async fn rejected_order_does_not_block_the_instrument<U: OrderExecutor + ?Sized>(
    ud: &U,
    id: &InstrumentId,
    ok_price: Price,
) {
    const S: &str = "rejected_order_does_not_block_the_instrument";

    // One after another: refused, then accepted.
    let (_, refused) = timeout(EVENT_TIMEOUT, place_patient(ud, id, REJECT_PRICE))
        .await
        .unwrap_or_else(|_| fail(S, "place of the refused order hung"));
    match refused {
        Ok(_) => fail(
            S,
            "the mock refuses price REJECT_PRICE, but place succeeded",
        ),
        Err(brokapi_core::Error::Unknown) => fail(S, "a refusal by the broker must not be Unknown"),
        Err(_) => {}
    }
    let (cid, accepted) = timeout(EVENT_TIMEOUT, place_patient(ud, id, ok_price))
        .await
        .unwrap_or_else(|_| {
            fail(
                S,
                "place after a refused order hung (instrument still locked?)",
            )
        });
    accepted.unwrap_or_else(|e| fail(S, format!("place after a refused order failed: {e:?}")));
    ud.cancel(cid)
        .await
        .unwrap_or_else(|e| fail(S, format!("cancel failed: {e:?}")));

    // At the same time: both are in flight together, the refused one must not take the other down.
    let both = async {
        tokio::join!(
            place_patient(ud, id, REJECT_PRICE),
            place_patient(ud, id, ok_price)
        )
    };
    let ((_, refused), (cid, accepted)) = timeout(EVENT_TIMEOUT, both)
        .await
        .unwrap_or_else(|_| fail(S, "concurrent places hung"));
    if refused.is_ok() {
        fail(S, "concurrent: the order at REJECT_PRICE was accepted");
    }
    accepted.unwrap_or_else(|e| {
        fail(
            S,
            format!("concurrent: the order after the refused one failed: {e:?}"),
        )
    });
    ud.cancel(cid)
        .await
        .unwrap_or_else(|e| fail(S, format!("cancel failed: {e:?}")));
}

/// Reconcile: an empty ledger touches nothing, and an unknown cid is reported as closed offline.
///
/// Checks:
/// - `reconcile(&[])` succeeds and reports no `confirmed` and no `closed_offline` orders;
/// - `reconcile(&[ghost])` with a random cid the broker has never seen succeeds and lists the cid in
///   `closed_offline`.
///
/// `foreign` orders are not checked. The account at the mock must have no orders of ours.
///
/// A successful reconcile also opens the command gate of the connector. That is why this scenario is
/// the first step of every scenario that sends trading commands: before it, those commands fail with
/// [`brokapi_core::Error::NotReconciled`].
///
/// # Panics
///
/// With `[conformance:reconcile] <reason>` when a call fails or the report is wrong.
///
/// # Examples
///
/// ```
/// # use brokapi_bcs::limits::default_limits;
/// # use brokapi_bcs::{UserDataConfig, UserDataConnector};
/// # use brokapi_core::{InstrumentId, InstrumentKey, NativeId, ShutdownMode};
/// # use brokapi_runtime::BrokerLimiter;
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let opts = std::sync::Arc::new(brokapi_mocks::bcs::MockOptions::default());
/// # let server = brokapi_mocks::bcs::MockServer::start(std::sync::Arc::clone(&opts)).await;
/// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base).with_write(true);
/// # let (tx, mut rx) = tokio::sync::mpsc::channel(256);
/// # let ud = UserDataConnector::start(UserDataConfig::new(cfg, "bcs-ud"), BrokerLimiter::new(&default_limits()), tx).await?;
/// # let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() });
/// brokapi_conformance::reconcile(&ud).await;
/// # ud.shutdown(ShutdownMode::Immediate).await;
/// # Ok(())
/// # }
/// ```
pub async fn reconcile<U: OrderExecutor + ?Sized>(ud: &U) {
    const S: &str = "reconcile";
    let report = ud
        .reconcile(&[])
        .await
        .unwrap_or_else(|e| fail(S, format!("reconcile([]) failed: {e:?}")));
    if !report.confirmed.is_empty() || !report.closed_offline.is_empty() {
        fail(
            S,
            "empty ledger must give no confirmed and no closed_offline",
        );
    }
    let ghost = ClientOrderId::new_v4();
    let report = ud
        .reconcile(&[ghost])
        .await
        .unwrap_or_else(|e| fail(S, format!("reconcile([ghost]) failed: {e:?}")));
    if !report.closed_offline.contains(&ghost) {
        fail(
            S,
            "cid unknown to the broker must be reported in closed_offline",
        );
    }
}

/// Portfolio: the snapshot works, and a portfolio update arrives after subscribe.
///
/// Calls `portfolio()`, then `subscribe_portfolio` with [`brokapi_core::SubscribeOpts::lossless`] (64),
/// then waits (up to [`EVENT_TIMEOUT`]) for the first `Portfolio`, `Positions` or `Balances` event on
/// `rx`. That envelope must have `source` as its source and the id returned by the subscribe as its `sub`.
///
/// The mock must serve a portfolio and push at least one update after subscribe. The subscription is
/// left open.
///
/// # Panics
///
/// With `[conformance:portfolio] <reason>` when a call fails, no update arrives, or source or sub is
/// wrong.
///
/// # Examples
///
/// ```
/// # use brokapi_bcs::limits::default_limits;
/// # use brokapi_bcs::{UserDataConfig, UserDataConnector};
/// # use brokapi_core::{InstrumentId, InstrumentKey, NativeId, ShutdownMode};
/// # use brokapi_runtime::BrokerLimiter;
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let opts = std::sync::Arc::new(brokapi_mocks::bcs::MockOptions::default());
/// # let server = brokapi_mocks::bcs::MockServer::start(std::sync::Arc::clone(&opts)).await;
/// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base).with_write(true);
/// # let (tx, mut rx) = tokio::sync::mpsc::channel(256);
/// # let ud = UserDataConnector::start(UserDataConfig::new(cfg, "bcs-ud"), BrokerLimiter::new(&default_limits()), tx).await?;
/// # let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() });
/// brokapi_conformance::portfolio(&ud, &mut rx, "bcs-ud").await;
/// # ud.shutdown(ShutdownMode::Immediate).await;
/// # Ok(())
/// # }
/// ```
pub async fn portfolio<U: PortfolioSource + ?Sized>(
    ud: &U,
    rx: &mut mpsc::Receiver<Envelope>,
    source: &str,
) {
    const S: &str = "portfolio";
    ud.portfolio()
        .await
        .unwrap_or_else(|e| fail(S, format!("portfolio failed: {e:?}")));
    let sub = ud
        .subscribe_portfolio(SubscribeOpts::lossless(64))
        .await
        .unwrap_or_else(|e| fail(S, format!("subscribe_portfolio failed: {e:?}")));
    let env = next_matching(S, rx, |e| {
        matches!(
            e.payload,
            Event::Portfolio(_) | Event::Positions(_) | Event::Balances(_)
        )
    })
    .await;
    if env.source.as_ref() != source {
        fail(S, format!("source {:?} != {source:?}", env.source));
    }
    if env.sub != Some(sub) {
        fail(
            S,
            format!("envelope sub {:?} != subscription {sub:?}", env.sub),
        );
    }
}

fn stop_request(
    id: &InstrumentId,
    kind: StopOrderKind,
    stop: Price,
    limit: Option<Price>,
) -> PlaceStopOrderRequest {
    PlaceStopOrderRequest {
        cid: ClientOrderId::new_v4(),
        instrument: id.clone(),
        side: Side::Sell,
        kind,
        qty: lots(1),
        stop_price: stop,
        limit_price: limit,
        expiry: StopExpiry::GoodTillCancel,
    }
}

/// Stop orders, the common path: place, list, cancel by cid, then a stop-limit cancelled by broker id.
///
/// Steps and checks:
/// 1. A stop-loss SELL, one lot, at `stop_price`. The ack carries the same cid and a non-empty broker
///    id. The order is then in `active_stop_orders()` with the same cid, kind `StopLoss`, side `Sell`,
///    the stop price and quantity we sent, no limit price, and state `Active`. `cancel_stop` by
///    [`brokapi_core::StopOrderRef::Cid`] succeeds and the order leaves the list.
/// 2. A stop-limit SELL with limit price `stop_price * 0.99`. It appears in the list with kind
///    `StopLimit` and a limit price. It is cancelled by [`brokapi_core::StopOrderRef::Broker`] and
///    leaves the list.
/// 3. A stop-limit without a limit price is refused with [`brokapi_core::Error::Rejected`] before
///    anything is sent, and the active list is empty afterwards (so the mock must start with no stop
///    orders).
///
/// `stop_price` must be far BELOW the market: the orders are SELL stops, they must not trigger. The
/// command gate must be open, so call [`reconcile`] first. There is no rate-limit retry in this scenario.
///
/// The mock must accept a stop order, list it while it waits, and cancel it.
///
/// # Panics
///
/// With `[conformance:stop_order_far_from_market] <reason>` when any check above fails.
///
/// # Examples
///
/// ```
/// # use brokapi_core::{Price};
/// # use brokapi_bcs::limits::default_limits;
/// # use brokapi_bcs::{UserDataConfig, UserDataConnector};
/// # use brokapi_core::{InstrumentId, InstrumentKey, NativeId, ShutdownMode};
/// # use brokapi_runtime::BrokerLimiter;
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let opts = std::sync::Arc::new(brokapi_mocks::bcs::MockOptions::default());
/// # let server = brokapi_mocks::bcs::MockServer::start(std::sync::Arc::clone(&opts)).await;
/// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base).with_write(true);
/// # let (tx, mut rx) = tokio::sync::mpsc::channel(256);
/// # let ud = UserDataConnector::start(UserDataConfig::new(cfg, "bcs-ud"), BrokerLimiter::new(&default_limits()), tx).await?;
/// # let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() });
/// brokapi_conformance::reconcile(&ud).await;
/// assert!(ud.caps().supports_stop_orders);
/// brokapi_conformance::stop_order_far_from_market(&ud, &sber, Price::from(200)).await;
/// # ud.shutdown(ShutdownMode::Immediate).await;
/// # Ok(())
/// # }
/// ```
pub async fn stop_order_far_from_market<U: StopOrderExecutor + ?Sized>(
    ud: &U,
    id: &InstrumentId,
    stop_price: Price,
) {
    const S: &str = "stop_order_far_from_market";

    // 1. Stop-loss: place, list, cancel by cid.
    let req = stop_request(id, StopOrderKind::StopLoss, stop_price, None);
    let cid = req.cid;
    let ack = ud
        .place_stop(req)
        .await
        .unwrap_or_else(|e| fail(S, format!("place_stop failed: {e:?}")));
    if ack.cid != cid {
        fail(S, format!("ack cid {} != sent {cid}", ack.cid));
    }
    if ack.broker_id.as_str().is_empty() {
        fail(S, "ack without broker stop order id");
    }
    let active = ud
        .active_stop_orders()
        .await
        .unwrap_or_else(|e| fail(S, format!("active_stop_orders failed: {e:?}")));
    let Some(snap) = active.iter().find(|s| s.broker_id == ack.broker_id) else {
        fail(
            S,
            format!(
                "the stop order {} is not in the active list: {active:?}",
                ack.broker_id.as_str()
            ),
        );
    };
    if snap.cid != Some(cid) {
        fail(
            S,
            format!("the list shows cid {:?}, we placed {cid}", snap.cid),
        );
    }
    if snap.kind != StopOrderKind::StopLoss || snap.side != Side::Sell {
        fail(
            S,
            format!("kind/side lost: {:?} {:?}", snap.kind, snap.side),
        );
    }
    if snap.stop_price != stop_price || snap.qty != lots(1) {
        fail(
            S,
            format!("price/qty lost: stop {} qty {}", snap.stop_price, snap.qty),
        );
    }
    if snap.limit_price.is_some() {
        fail(S, "a stop-loss without a limit price came back with one");
    }
    if snap.state != StopOrderState::Active {
        fail(
            S,
            format!("a waiting stop order must be Active, got {:?}", snap.state),
        );
    }
    ud.cancel_stop(StopOrderRef::Cid(cid))
        .await
        .unwrap_or_else(|e| fail(S, format!("cancel_stop by cid failed: {e:?}")));
    let after = ud
        .active_stop_orders()
        .await
        .unwrap_or_else(|e| fail(S, format!("list after cancel failed: {e:?}")));
    if after.iter().any(|s| s.broker_id == ack.broker_id) {
        fail(S, "the stop order is still active after cancel_stop");
    }

    // 2. Stop-limit with a limit price, cancelled by the broker id.
    let req = stop_request(
        id,
        StopOrderKind::StopLimit,
        stop_price,
        Some(stop_price * Decimal::new(99, 2)),
    );
    let ack2 = ud
        .place_stop(req)
        .await
        .unwrap_or_else(|e| fail(S, format!("place_stop (stop-limit) failed: {e:?}")));
    let active = ud
        .active_stop_orders()
        .await
        .unwrap_or_else(|e| fail(S, format!("list failed: {e:?}")));
    let Some(snap2) = active.iter().find(|s| s.broker_id == ack2.broker_id) else {
        fail(S, "the stop-limit order is not in the active list");
    };
    if snap2.kind != StopOrderKind::StopLimit || snap2.limit_price.is_none() {
        fail(
            S,
            format!("stop-limit lost its kind or limit price: {snap2:?}"),
        );
    }
    ud.cancel_stop(StopOrderRef::Broker(ack2.broker_id.clone()))
        .await
        .unwrap_or_else(|e| fail(S, format!("cancel_stop by broker id failed: {e:?}")));
    let after = ud
        .active_stop_orders()
        .await
        .unwrap_or_else(|e| fail(S, format!("list failed: {e:?}")));
    if after.iter().any(|s| s.broker_id == ack2.broker_id) {
        fail(S, "the stop-limit order is still active after cancel_stop");
    }

    // 3. A stop-limit needs a limit price: refused locally, nothing sent, nothing listed.
    let bad = stop_request(id, StopOrderKind::StopLimit, stop_price, None);
    match ud.place_stop(bad).await {
        Err(Error::Rejected { .. }) => {}
        other => fail(
            S,
            format!("stop-limit without a limit price must be Rejected, got {other:?}"),
        ),
    }
    let after = ud
        .active_stop_orders()
        .await
        .unwrap_or_else(|e| fail(S, format!("list failed: {e:?}")));
    if !after.is_empty() {
        fail(S, format!("nothing should be left, found {after:?}"));
    }
}

/// A connector without stop orders says so: every stop method answers [`brokapi_core::Error::Unsupported`].
///
/// Checks that `place_stop`, `cancel_stop`, `active_stop_orders` and `subscribe_stop_orders` each
/// return `Unsupported` (any other result, success or another error, fails). No adapter uses this
/// today, because all three map stop orders. It stays for a future adapter whose broker has none. Do
/// not run it for an adapter that supports stop orders.
///
/// # Panics
///
/// With `[conformance:stop_orders_unsupported] <method> must answer Unsupported, got ...` for the first
/// method that answers anything else.
///
/// # Examples
///
/// The BCS connector supports stop orders, so this scenario fails against it. That is the failure the
/// scenario exists to catch:
///
/// ```should_panic(expected = "[conformance:stop_orders_unsupported] place_stop must answer Unsupported")
/// # use brokapi_bcs::limits::default_limits;
/// # use brokapi_bcs::{UserDataConfig, UserDataConnector};
/// # use brokapi_core::{InstrumentId, InstrumentKey, NativeId, ShutdownMode};
/// # use brokapi_runtime::BrokerLimiter;
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let opts = std::sync::Arc::new(brokapi_mocks::bcs::MockOptions::default());
/// # let server = brokapi_mocks::bcs::MockServer::start(std::sync::Arc::clone(&opts)).await;
/// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base).with_write(true);
/// # let (tx, mut rx) = tokio::sync::mpsc::channel(256);
/// # let ud = UserDataConnector::start(UserDataConfig::new(cfg, "bcs-ud"), BrokerLimiter::new(&default_limits()), tx).await?;
/// # let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() });
/// // panics: [conformance:stop_orders_unsupported] ... must answer Unsupported, got ...
/// brokapi_conformance::stop_orders_unsupported(&ud, &sber).await;
/// # ud.shutdown(ShutdownMode::Immediate).await;
/// # Ok(())
/// # }
/// ```
pub async fn stop_orders_unsupported<U: StopOrderExecutor + ?Sized>(ud: &U, id: &InstrumentId) {
    const S: &str = "stop_orders_unsupported";
    let req = stop_request(id, StopOrderKind::StopLoss, Price::from(1), None);
    let cid = req.cid;
    let checks: [(&str, Result<(), Error>); 4] = [
        ("place_stop", ud.place_stop(req).await.map(|_| ())),
        ("cancel_stop", ud.cancel_stop(StopOrderRef::Cid(cid)).await),
        (
            "active_stop_orders",
            ud.active_stop_orders().await.map(|_| ()),
        ),
        (
            "subscribe_stop_orders",
            ud.subscribe_stop_orders().await.map(|_| ()),
        ),
    ];
    for (name, res) in checks {
        if !matches!(res, Err(Error::Unsupported { .. })) {
            fail(S, format!("{name} must answer Unsupported, got {res:?}"));
        }
    }
}

// ─── market data reads and streams ────────────────────────────────────────────

/// The tape, candles, trading status and schedule of one instrument.
///
/// - `last_trades(limit)` gives at least one trade and never more than `limit`. Prices and quantities
///   are positive and the trades are for the right instrument.
/// - `candles(tf, candle_range)` gives bars with `low <= open, close <= high`, closing after they open,
///   for the asked timeframe and instrument.
/// - `trading_status` names the instrument and is not `Unknown`.
/// - `schedule(schedule_key, schedule_range)` gives sessions that start before they end. `schedule_key`
///   is what the adapter calls an exchange (T-Bank `"MOEX"`, Finam and BCS `"TICKER@MARKET"`).
///
/// The scenario asks for `last_trades` with a limit of 2. The mock must serve the tape, candles for
/// `candle_range`, a known trading status and at least one session in `schedule_range`.
///
/// # Panics
///
/// With `[conformance:market_data_history] <reason>` when a call fails or a check above fails.
///
/// # Examples
///
/// ```
/// # use brokapi_bcs::limits::default_limits;
/// # use brokapi_bcs::{MarketDataConfig, MarketDataConnector};
/// # use brokapi_core::{TimeRange, Timeframe, UnixNanos, InstrumentId, InstrumentKey, NativeId, ShutdownMode};
/// # use brokapi_runtime::BrokerLimiter;
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
/// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
/// # let (tx, mut rx) = tokio::sync::mpsc::channel(256);
/// # let md = MarketDataConnector::start(MarketDataConfig::new(cfg, "bcs-md"), BrokerLimiter::new(&default_limits()), tx).await?;
/// # let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() });
/// // 2026-10-19 00:00 UTC .. 2026-10-20 00:00 UTC
/// let day = TimeRange::new(
///     UnixNanos::from_nanos(1_792_368_000_000_000_000),
///     UnixNanos::from_nanos(1_792_454_400_000_000_000),
/// );
/// // BCS schedules are per instrument: "TICKER@CLASSCODE".
/// brokapi_conformance::market_data_history(&md, &sber, Timeframe::M1, day, "SBER@TQBR", day).await;
/// # md.shutdown(ShutdownMode::Immediate).await;
/// # Ok(())
/// # }
/// ```
pub async fn market_data_history<M: MarketDataSource + InstrumentSource + ?Sized>(
    md: &M,
    id: &InstrumentId,
    tf: Timeframe,
    candle_range: TimeRange,
    schedule_key: &str,
    schedule_range: TimeRange,
) {
    const S: &str = "market_data_history";
    let trades = md
        .last_trades(id, 2)
        .await
        .unwrap_or_else(|e| fail(S, format!("last_trades failed: {e:?}")));
    if trades.is_empty() {
        fail(S, "the mock must serve at least one trade");
    }
    if trades.len() > 2 {
        fail(
            S,
            format!("last_trades(limit 2) returned {} trades", trades.len()),
        );
    }
    for t in &trades {
        if !(t.price.is_positive() && t.qty.is_positive()) {
            fail(S, format!("trade with price {} qty {}", t.price, t.qty));
        }
        if t.instrument.key != id.key {
            fail(
                S,
                format!(
                    "trade of {} for {}",
                    t.instrument.key.display(),
                    id.key.display()
                ),
            );
        }
    }

    let bars = md
        .candles(id, tf, candle_range)
        .await
        .unwrap_or_else(|e| fail(S, format!("candles failed: {e:?}")));
    if bars.is_empty() {
        fail(S, "the mock must serve candles");
    }
    for c in &bars {
        let (lo, hi) = (c.low, c.high);
        if !(lo <= c.open && c.open <= hi && lo <= c.close && c.close <= hi) {
            fail(S, format!("candle OHLC is inconsistent: {c:?}"));
        }
        if c.close_ts.as_nanos() < c.open_ts.as_nanos() {
            fail(S, "a candle closes before it opens");
        }
        if c.timeframe != tf || c.instrument.key != id.key {
            fail(
                S,
                format!(
                    "candle for {:?} {}, asked {tf:?} {}",
                    c.timeframe,
                    c.instrument.key.display(),
                    id.key.display()
                ),
            );
        }
    }

    let st = md
        .trading_status(id)
        .await
        .unwrap_or_else(|e| fail(S, format!("trading_status failed: {e:?}")));
    if st.instrument.key != id.key {
        fail(S, "trading_status is about another instrument");
    }
    if st.availability == TradingAvailability::Unknown {
        fail(S, "the mock status must be a known one");
    }

    let sched = md
        .schedule(schedule_key, schedule_range)
        .await
        .unwrap_or_else(|e| fail(S, format!("schedule({schedule_key:?}) failed: {e:?}")));
    if sched.sessions.is_empty() {
        fail(S, "the mock must serve at least one session");
    }
    if sched
        .sessions
        .iter()
        .any(|x| x.start.as_nanos() >= x.end.as_nanos())
    {
        fail(
            S,
            format!("a session starts after it ends: {:?}", sched.sessions),
        );
    }
}

/// Market trades, quotes and candles as streams, each for the right instrument and tagged with our source and subscription id.
///
/// For each of the three kinds the scenario subscribes, then waits (up to [`EVENT_TIMEOUT`]) for an event
/// of that kind on that subscription, and checks:
/// - trades: source, instrument, positive price and quantity;
/// - quotes: source, instrument, at least a bid or an ask, and not crossed (bid not above ask);
/// - candles: source, instrument, the asked `candle_tf`, `low` not above `high`.
///
/// Trades use [`brokapi_core::SubscribeOpts::lossless`] (64), quotes and candles use
/// [`brokapi_core::SubscribeOpts::conflate`]. The subscriptions are left open. The mock must push a trade, a quote and
/// a candle after each subscribe. `rx` must be the receiver of the connector under test.
///
/// # Panics
///
/// With `[conformance:market_data_streams] <reason>` when a subscribe fails, an event does not arrive in
/// time, or a check fails.
///
/// # Examples
///
/// ```
/// # use brokapi_bcs::limits::default_limits;
/// # use brokapi_bcs::{MarketDataConfig, MarketDataConnector};
/// # use brokapi_core::{Timeframe, InstrumentId, InstrumentKey, NativeId, ShutdownMode};
/// # use brokapi_runtime::BrokerLimiter;
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
/// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
/// # let (tx, mut rx) = tokio::sync::mpsc::channel(256);
/// # let md = MarketDataConnector::start(MarketDataConfig::new(cfg, "bcs-md"), BrokerLimiter::new(&default_limits()), tx).await?;
/// # let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() });
/// brokapi_conformance::market_data_streams(&md, &mut rx, "bcs-md", &sber, Timeframe::M1).await;
/// # md.shutdown(ShutdownMode::Immediate).await;
/// # Ok(())
/// # }
/// ```
pub async fn market_data_streams<M: MarketDataSource + ?Sized>(
    md: &M,
    rx: &mut mpsc::Receiver<Envelope>,
    source: &str,
    id: &InstrumentId,
    candle_tf: Timeframe,
) {
    const S: &str = "market_data_streams";
    let t = md
        .subscribe_market_trades(id, SubscribeOpts::lossless(64))
        .await
        .unwrap_or_else(|e| fail(S, format!("subscribe_market_trades failed: {e:?}")));
    let env = next_matching(S, rx, |e| {
        matches!(e.payload, Event::MarketTrade(_)) && e.sub == Some(t)
    })
    .await;
    let Event::MarketTrade(tr) = &env.payload else {
        unreachable!()
    };
    if env.source.as_ref() != source {
        fail(S, format!("trade source {:?} != {source:?}", env.source));
    }
    if tr.instrument.key != id.key || !(tr.price.is_positive() && tr.qty.is_positive()) {
        fail(S, format!("bad market trade: {tr:?}"));
    }

    let q = md
        .subscribe_quotes(id, SubscribeOpts::conflate())
        .await
        .unwrap_or_else(|e| fail(S, format!("subscribe_quotes failed: {e:?}")));
    let env = next_matching(S, rx, |e| {
        matches!(e.payload, Event::Quote(_)) && e.sub == Some(q)
    })
    .await;
    let Event::Quote(quote) = &env.payload else {
        unreachable!()
    };
    if env.source.as_ref() != source || quote.instrument.key != id.key {
        fail(S, format!("bad quote envelope: {env:?}"));
    }
    if quote.bid.is_none() && quote.ask.is_none() {
        fail(S, "a quote with neither bid nor ask");
    }
    if let (Some(b), Some(a)) = (&quote.bid, &quote.ask)
        && b.price > a.price
    {
        fail(S, "a quote is crossed: bid above ask");
    }

    let c = md
        .subscribe_candles(id, candle_tf, SubscribeOpts::conflate())
        .await
        .unwrap_or_else(|e| fail(S, format!("subscribe_candles failed: {e:?}")));
    let env = next_matching(S, rx, |e| {
        matches!(e.payload, Event::Candle(_)) && e.sub == Some(c)
    })
    .await;
    let Event::Candle(candle) = &env.payload else {
        unreachable!()
    };
    if env.source.as_ref() != source
        || candle.instrument.key != id.key
        || candle.timeframe != candle_tf
    {
        fail(S, format!("bad candle envelope: {env:?}"));
    }
    if candle.low > candle.high {
        fail(S, "a candle with low above high");
    }
}

// ─── own orders: events, active list, replace, executions ─────────────────────

/// Places a limit buy. A local rate-limit refusal (nothing was sent) is waited out and retried under a
/// fresh cid, because a connector never sends the same cid twice. Returns the cid that went through.
async fn place_limit<U: OrderExecutor + ?Sized>(
    ud: &U,
    scenario: &str,
    id: &InstrumentId,
    price: Price,
    qty: Qty,
) -> ClientOrderId {
    for _ in 0..20 {
        let req = PlaceOrderRequest {
            cid: ClientOrderId::new_v4(),
            instrument: id.clone(),
            side: Side::Buy,
            order_type: OrderType::Limit,
            qty,
            price: Some(price),
        };
        let cid = req.cid;
        match ud.place(req).await {
            Err(Error::RateLimited { retry_after }) => tokio::time::sleep(retry_after).await,
            Ok(_) => return cid,
            Err(e) => fail(scenario, format!("place failed: {e:?}")),
        }
    }
    fail(scenario, "place still rate limited after 20 waits")
}

/// Cancels with the same patience as [`place_with_cid`].
async fn cancel_patient<U: OrderExecutor + ?Sized>(ud: &U, scenario: &str, cid: ClientOrderId) {
    for _ in 0..20 {
        match ud.cancel(cid).await {
            Err(Error::RateLimited { retry_after }) => tokio::time::sleep(retry_after).await,
            Ok(()) => return,
            Err(e) => fail(scenario, format!("cancel failed: {e:?}")),
        }
    }
    fail(scenario, "cancel still rate limited after 20 waits");
}

/// The order stream: placing an order gives an event for our cid, cancelling it gives a `Cancelled` event.
///
/// Steps and checks:
/// 1. `subscribe_order_state`.
/// 2. Place a buy limit order, one lot, at `price`, under a fresh cid. A local rate-limit refusal is
///    waited out and retried under a fresh cid (a cid is never sent twice).
/// 3. An [`brokapi_core::Event::Order`] for our cid arrives. Its source is `source`, its `sub` is the
///    subscription from step 1, and its state is not `Rejected`.
/// 4. Cancel the order. An order event with state `Cancelled` for the same cid arrives on the same
///    subscription.
///
/// Each wait is limited to [`EVENT_TIMEOUT`]. `price` must be far from the market and accepted by the
/// mock. The command gate must be open, so call [`reconcile`] first. The mock must push an order event
/// when an order is placed and when it is cancelled. If the adapter opens its order event socket in the
/// background, wait until it is connected before running this scenario, otherwise the first event can
/// be missed.
///
/// # Panics
///
/// With `[conformance:order_state_events] <reason>` when a call fails, an event does not arrive, or a
/// check fails.
///
/// # Examples
///
/// ```
/// # use brokapi_core::{Price};
/// # use brokapi_bcs::limits::default_limits;
/// # use brokapi_bcs::{UserDataConfig, UserDataConnector};
/// # use brokapi_core::{InstrumentId, InstrumentKey, NativeId, ShutdownMode};
/// # use brokapi_runtime::BrokerLimiter;
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let opts = std::sync::Arc::new(brokapi_mocks::bcs::MockOptions::default());
/// # let server = brokapi_mocks::bcs::MockServer::start(std::sync::Arc::clone(&opts)).await;
/// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base).with_write(true);
/// # let (tx, mut rx) = tokio::sync::mpsc::channel(256);
/// # let ud = UserDataConnector::start(UserDataConfig::new(cfg, "bcs-ud"), BrokerLimiter::new(&default_limits()), tx).await?;
/// # let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() });
/// brokapi_conformance::reconcile(&ud).await;
/// # // The order-events socket connects in the background: events sent before that are not delivered.
/// # tokio::time::timeout(std::time::Duration::from_secs(8), async {
/// #     while opts.event_sockets() == 0 {
/// #         tokio::time::sleep(std::time::Duration::from_millis(20)).await;
/// #     }
/// # })
/// # .await?;
/// brokapi_conformance::order_state_events(&ud, &mut rx, "bcs-ud", &sber, Price::from(100)).await;
/// # ud.shutdown(ShutdownMode::Immediate).await;
/// # Ok(())
/// # }
/// ```
pub async fn order_state_events<U: OrderExecutor + ?Sized>(
    ud: &U,
    rx: &mut mpsc::Receiver<Envelope>,
    source: &str,
    id: &InstrumentId,
    price: Price,
) {
    const S: &str = "order_state_events";
    let sub = ud
        .subscribe_order_state()
        .await
        .unwrap_or_else(|e| fail(S, format!("subscribe_order_state failed: {e:?}")));
    let cid = place_limit(ud, S, id, price, lots(1)).await;
    let env = next_matching(
        S,
        rx,
        |e| matches!(&e.payload, Event::Order(o) if o.cid == cid),
    )
    .await;
    if env.source.as_ref() != source || env.sub != Some(sub) {
        fail(
            S,
            format!(
                "order event envelope: source {:?} sub {:?}, expected {source:?} / {sub:?}",
                env.source, env.sub
            ),
        );
    }
    let Event::Order(o) = &env.payload else {
        unreachable!()
    };
    if matches!(o.state, OrderState::Rejected { .. }) {
        fail(
            S,
            format!("a placed order came back rejected: {:?}", o.state),
        );
    }
    cancel_patient(ud, S, cid).await;
    let env = next_matching(S, rx, |e| {
        matches!(&e.payload, Event::Order(o) if o.cid == cid && matches!(o.state, OrderState::Cancelled { .. }))
    })
    .await;
    if env.sub != Some(sub) {
        fail(S, "the cancel event is not on the order subscription");
    }
}

/// `active_orders` follows the order: a placed order is listed with its details, a cancelled one is gone.
///
/// Steps and checks:
/// 1. Place a buy limit order, three lots, at `price` (rate-limit refusals are waited out and retried
///    under a fresh cid).
/// 2. `active_orders()` lists the order under its cid, with the same instrument, side `Buy`, three lots
///    and price `price`, and a state that is not `Cancelled`, `Filled` or `Rejected`.
/// 3. Cancel it. `active_orders()` no longer lists it.
///
/// `price` must be far from the market and accepted by the mock. The command gate must be open, so
/// call [`reconcile`] first. The mock must keep placed orders in its active list.
///
/// # Panics
///
/// With `[conformance:active_orders_lifecycle] <reason>` when a call fails or a check fails.
///
/// # Examples
///
/// ```
/// # use brokapi_core::{Price};
/// # use brokapi_bcs::limits::default_limits;
/// # use brokapi_bcs::{UserDataConfig, UserDataConnector};
/// # use brokapi_core::{InstrumentId, InstrumentKey, NativeId, ShutdownMode};
/// # use brokapi_runtime::BrokerLimiter;
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let opts = std::sync::Arc::new(brokapi_mocks::bcs::MockOptions::default());
/// # let server = brokapi_mocks::bcs::MockServer::start(std::sync::Arc::clone(&opts)).await;
/// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base).with_write(true);
/// # let (tx, mut rx) = tokio::sync::mpsc::channel(256);
/// # let ud = UserDataConnector::start(UserDataConfig::new(cfg, "bcs-ud"), BrokerLimiter::new(&default_limits()), tx).await?;
/// # let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() });
/// brokapi_conformance::reconcile(&ud).await;
/// brokapi_conformance::active_orders_lifecycle(&ud, &sber, Price::from(100)).await;
/// # ud.shutdown(ShutdownMode::Immediate).await;
/// # Ok(())
/// # }
/// ```
pub async fn active_orders_lifecycle<U: OrderExecutor + ?Sized>(
    ud: &U,
    id: &InstrumentId,
    price: Price,
) {
    const S: &str = "active_orders_lifecycle";
    let cid = place_limit(ud, S, id, price, lots(3)).await;
    let active = ud
        .active_orders()
        .await
        .unwrap_or_else(|e| fail(S, format!("active_orders failed: {e:?}")));
    let Some(o) = active.iter().find(|o| o.cid == cid) else {
        fail(
            S,
            format!("the placed order {cid} is not in the active list: {active:?}"),
        );
    };
    if o.instrument.key != id.key || o.side != Side::Buy {
        fail(
            S,
            format!(
                "instrument/side lost: {} {:?}",
                o.instrument.key.display(),
                o.side
            ),
        );
    }
    if o.qty != lots(3) {
        fail(S, format!("quantity lost: {}", o.qty));
    }
    if o.price != Some(price) {
        fail(S, format!("price lost: {:?}", o.price));
    }
    if matches!(
        o.state,
        OrderState::Cancelled { .. } | OrderState::Filled { .. } | OrderState::Rejected { .. }
    ) {
        fail(S, format!("an active order in state {:?}", o.state));
    }
    cancel_patient(ud, S, cid).await;
    let after = ud
        .active_orders()
        .await
        .unwrap_or_else(|e| fail(S, format!("active_orders after cancel failed: {e:?}")));
    if after.iter().any(|o| o.cid == cid) {
        fail(S, "the cancelled order is still in the active list");
    }
}

/// `replace`: where the broker has it the order moves to a new cid, where it has not, `replace` says `Unsupported`.
///
/// `supports_replace` is what the adapter reports in its capabilities. The scenario places a buy
/// limit order (two lots, at `price`) and then replaces it with a new cid, four lots and price `new_price`.
///
/// With `supports_replace == true`:
/// - the ack carries the new cid;
/// - `active_orders()` lists the new cid with four lots and price `new_price`, and no longer lists the
///   old one;
/// - `order_state(new cid)` is not `Rejected` or `Cancelled`.
///
/// With `supports_replace == false`:
/// - `replace` returns [`brokapi_core::Error::Unsupported`];
/// - the original order is still active. It is then cancelled.
///
/// Rate-limit refusals are waited out and retried. Both prices must be far from the market and accepted
/// by the mock. The command gate must be open, so call [`reconcile`] first.
///
/// # Panics
///
/// With `[conformance:replace_order] <reason>` when a check fails. Passing the wrong `supports_replace`
/// for the adapter fails the scenario: `true` for an adapter without replace gives a replace error,
/// `false` for one with replace gives "replace must be Unsupported where the broker has none".
///
/// # Examples
///
/// ```
/// # use brokapi_core::{Price};
/// # use brokapi_bcs::limits::default_limits;
/// # use brokapi_bcs::{UserDataConfig, UserDataConnector};
/// # use brokapi_core::{InstrumentId, InstrumentKey, NativeId, ShutdownMode};
/// # use brokapi_runtime::BrokerLimiter;
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let opts = std::sync::Arc::new(brokapi_mocks::bcs::MockOptions::default());
/// # let server = brokapi_mocks::bcs::MockServer::start(std::sync::Arc::clone(&opts)).await;
/// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base).with_write(true);
/// # let (tx, mut rx) = tokio::sync::mpsc::channel(256);
/// # let ud = UserDataConnector::start(UserDataConfig::new(cfg, "bcs-ud"), BrokerLimiter::new(&default_limits()), tx).await?;
/// # let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() });
/// brokapi_conformance::reconcile(&ud).await;
/// assert!(ud.caps().supports_replace);
/// brokapi_conformance::replace_order(&ud, true, &sber, Price::from(100), Price::from(101)).await;
/// # ud.shutdown(ShutdownMode::Immediate).await;
/// # Ok(())
/// # }
/// ```
///
/// Claiming that BCS has no replace fails the scenario:
///
/// ```should_panic(expected = "[conformance:replace_order] replace must be Unsupported where the broker has none")
/// # use brokapi_core::{Price};
/// # use brokapi_bcs::limits::default_limits;
/// # use brokapi_bcs::{UserDataConfig, UserDataConnector};
/// # use brokapi_core::{InstrumentId, InstrumentKey, NativeId, ShutdownMode};
/// # use brokapi_runtime::BrokerLimiter;
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let opts = std::sync::Arc::new(brokapi_mocks::bcs::MockOptions::default());
/// # let server = brokapi_mocks::bcs::MockServer::start(std::sync::Arc::clone(&opts)).await;
/// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base).with_write(true);
/// # let (tx, mut rx) = tokio::sync::mpsc::channel(256);
/// # let ud = UserDataConnector::start(UserDataConfig::new(cfg, "bcs-ud"), BrokerLimiter::new(&default_limits()), tx).await?;
/// # let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() });
/// brokapi_conformance::reconcile(&ud).await;
/// // panics: [conformance:replace_order] replace must be Unsupported where the broker has none, got ...
/// brokapi_conformance::replace_order(&ud, false, &sber, Price::from(100), Price::from(101)).await;
/// # ud.shutdown(ShutdownMode::Immediate).await;
/// # Ok(())
/// # }
/// ```
pub async fn replace_order<U: OrderExecutor + ?Sized>(
    ud: &U,
    supports_replace: bool,
    id: &InstrumentId,
    price: Price,
    new_price: Price,
) {
    const S: &str = "replace_order";
    let cid = place_limit(ud, S, id, price, lots(2)).await;
    let new = ClientOrderId::new_v4();
    let req = ReplaceOrderRequest {
        cid: new,
        original_cid: cid,
        qty: Some(lots(4)),
        price: Some(new_price),
    };
    if !supports_replace {
        match ud.replace(req).await {
            Err(Error::Unsupported { .. }) => {}
            other => fail(
                S,
                format!("replace must be Unsupported where the broker has none, got {other:?}"),
            ),
        }
        if !ud
            .active_orders()
            .await
            .unwrap_or_default()
            .iter()
            .any(|o| o.cid == cid)
        {
            fail(S, "an unsupported replace must leave the order alone");
        }
        cancel_patient(ud, S, cid).await;
        return;
    }
    let mut ack = None;
    for _ in 0..20 {
        match ud.replace(req.clone()).await {
            Err(Error::RateLimited { retry_after }) => tokio::time::sleep(retry_after).await,
            other => {
                ack = Some(other);
                break;
            }
        }
    }
    let ack = ack
        .unwrap_or_else(|| fail(S, "replace still rate limited"))
        .unwrap_or_else(|e| fail(S, format!("replace failed: {e:?}")));
    if ack.cid != new {
        fail(S, format!("ack cid {} != new cid {new}", ack.cid));
    }
    let active = ud
        .active_orders()
        .await
        .unwrap_or_else(|e| fail(S, format!("active_orders failed: {e:?}")));
    let Some(o) = active.iter().find(|o| o.cid == new) else {
        fail(S, format!("the new order {new} is not active: {active:?}"));
    };
    if o.qty != lots(4) || o.price != Some(new_price) {
        fail(
            S,
            format!("new quantity/price lost: {} {:?}", o.qty, o.price),
        );
    }
    if active.iter().any(|o| o.cid == cid) {
        fail(S, "the replaced order is still active");
    }
    let state = ud
        .order_state(new)
        .await
        .unwrap_or_else(|e| fail(S, format!("order_state of the new cid failed: {e:?}")));
    if matches!(
        state.state,
        OrderState::Rejected { .. } | OrderState::Cancelled { .. }
    ) {
        fail(S, format!("the new order is {:?}", state.state));
    }
    cancel_patient(ud, S, new).await;
}

/// Own executions: the history works, and a fill arrives as an `Execution` event and in the history.
///
/// Steps and checks:
/// 1. `executions(range)` over a very wide range succeeds on an empty account.
/// 2. `subscribe_executions`.
/// 3. Place a buy limit order, two lots, at `price` (rate-limit refusals are waited out and retried
///    under a fresh cid), then call `fill(cid)`.
/// 4. An [`brokapi_core::Event::Execution`] arrives. Its source is `source`, its `sub` is the executions
///    subscription, its cid is ours, price and quantity are positive, the trade id is not empty and the
///    instrument is `id`.
/// 5. `executions(range)` now lists at least one trade, each with a positive price, positive quantity
///    and a trade id.
///
/// `fill(cid)` makes the mock execute the order. Each mock has its own way, so the adapter provides it.
/// It may wait until the stream is open. It runs once. `price` must be far from the market and accepted
/// by the mock, and the fill itself is the mock's business. The command gate must be open, so call
/// [`reconcile`] first, and wait for the adapter's event socket if it connects in the background.
///
/// # Panics
///
/// With `[conformance:executions] <reason>` when a call fails, no execution arrives in time, or a check
/// fails.
///
/// # Examples
///
/// ```
/// # use brokapi_core::{Price};
/// # use brokapi_bcs::limits::default_limits;
/// # use brokapi_bcs::{UserDataConfig, UserDataConnector};
/// # use brokapi_core::{InstrumentId, InstrumentKey, NativeId, ShutdownMode};
/// # use brokapi_runtime::BrokerLimiter;
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let opts = std::sync::Arc::new(brokapi_mocks::bcs::MockOptions::default());
/// # let server = brokapi_mocks::bcs::MockServer::start(std::sync::Arc::clone(&opts)).await;
/// # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base).with_write(true);
/// # let (tx, mut rx) = tokio::sync::mpsc::channel(256);
/// # let ud = UserDataConnector::start(UserDataConfig::new(cfg, "bcs-ud"), BrokerLimiter::new(&default_limits()), tx).await?;
/// # let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() });
/// brokapi_conformance::reconcile(&ud).await;
/// # // The order-events socket connects in the background: events sent before that are not delivered.
/// # tokio::time::timeout(std::time::Duration::from_secs(8), async {
/// #     while opts.event_sockets() == 0 {
/// #         tokio::time::sleep(std::time::Duration::from_millis(20)).await;
/// #     }
/// # })
/// # .await?;
/// brokapi_conformance::executions(&ud, &mut rx, "bcs-ud", &sber, Price::from(100), |cid| {
///     // The BCS mock executes an order when told: cid, quantity, price.
///     let _ = server.fill(&cid.hyphenated().to_string(), brokapi_core::Qty::from(20), brokapi_core::Price::from(100));
///     async {}
/// })
/// .await;
/// # ud.shutdown(ShutdownMode::Immediate).await;
/// # Ok(())
/// # }
/// ```
/// # use brokapi_core::{Qty};
pub async fn executions<U, F, Fut>(
    ud: &U,
    rx: &mut mpsc::Receiver<Envelope>,
    source: &str,
    id: &InstrumentId,
    price: Price,
    fill: F,
) where
    U: OrderExecutor + ExecutionSource + ?Sized,
    F: FnOnce(ClientOrderId) -> Fut,
    Fut: Future<Output = ()>,
{
    const S: &str = "executions";
    let range = TimeRange::new(UnixNanos::ZERO, UnixNanos::from_nanos(i64::MAX / 2));
    ud.executions(range).await.unwrap_or_else(|e| {
        fail(
            S,
            format!("executions(range) failed on an empty account: {e:?}"),
        )
    });
    let sub = ud
        .subscribe_executions()
        .await
        .unwrap_or_else(|e| fail(S, format!("subscribe_executions failed: {e:?}")));
    let cid = place_limit(ud, S, id, price, lots(2)).await;
    fill(cid).await;
    let env = next_matching(S, rx, |e| matches!(e.payload, Event::Execution(_))).await;
    if env.source.as_ref() != source || env.sub != Some(sub) {
        fail(
            S,
            format!(
                "execution envelope: source {:?} sub {:?}, expected {source:?} / {sub:?}",
                env.source, env.sub
            ),
        );
    }
    let Event::Execution(x) = &env.payload else {
        unreachable!()
    };
    if x.cid != Some(cid) {
        fail(
            S,
            format!("the execution is for {:?}, we placed {cid}", x.cid),
        );
    }
    if !(x.price.is_positive() && x.qty.is_positive()) || x.trade_id.is_empty() {
        fail(S, format!("bad execution: {x:?}"));
    }
    if x.instrument.key != id.key {
        fail(S, "the execution is for another instrument");
    }
    let hist = ud
        .executions(range)
        .await
        .unwrap_or_else(|e| fail(S, format!("executions(range) after a fill failed: {e:?}")));
    if hist.is_empty() {
        fail(S, "the history is empty after a fill");
    }
    if hist
        .iter()
        .any(|h| !(h.price.is_positive() && h.qty.is_positive()) || h.trade_id.is_empty())
    {
        fail(
            S,
            format!("a history entry without price, quantity or trade id: {hist:?}"),
        );
    }
}

// ─── guarantees of the command path ───────────────────────────────────────────

fn order(id: &InstrumentId, price: Price, qty: Qty) -> PlaceOrderRequest {
    PlaceOrderRequest {
        cid: ClientOrderId::new_v4(),
        instrument: id.clone(),
        side: Side::Buy,
        order_type: OrderType::Limit,
        qty,
        price: Some(price),
    }
}

/// How many of the active orders are ours: the cid is among `cids`.
async fn active_among<U: OrderExecutor + ?Sized>(
    ud: &U,
    scenario: &str,
    cids: &[ClientOrderId],
) -> usize {
    ud.active_orders()
        .await
        .unwrap_or_else(|e| fail(scenario, format!("active_orders failed: {e:?}")))
        .iter()
        .filter(|o| cids.contains(&o.cid))
        .count()
}

/// A `cid` is used for one place at most: the second place with it is refused locally and creates nothing.
///
/// Steps and checks:
/// 1. `place` a buy limit order of one lot at `price` under a fresh cid. It succeeds.
/// 2. `place` with the same cid again: [`Error::Rejected`] with code `duplicate_place`, not `Ok` and not
///    `Unknown`. Nothing was sent, so the broker holds one order of that cid.
/// 3. Cancel the order.
///
/// The command gate must be open, so call [`reconcile`] first.
///
/// # Panics
///
/// With `[conformance:duplicate_place] <reason>`.
pub async fn duplicate_place<U: OrderExecutor + ?Sized>(ud: &U, id: &InstrumentId, price: Price) {
    const S: &str = "duplicate_place";
    let req = order(id, price, lots(1));
    let cid = req.cid;
    ud.place(req.clone())
        .await
        .unwrap_or_else(|e| fail(S, format!("the first place failed: {e:?}")));
    match ud.place(req).await {
        Err(Error::Rejected { code, .. }) if code == "duplicate_place" => {}
        other => fail(
            S,
            format!("the same cid twice must be Rejected(duplicate_place), got {other:?}"),
        ),
    }
    let live = active_among(ud, S, &[cid]).await;
    if live != 1 {
        fail(
            S,
            format!("the broker must hold one order of the cid, holds {live}"),
        );
    }
    cancel_patient(ud, S, cid).await;
}

/// A quantity the broker cannot take is refused before anything is sent, and the cid stays usable.
///
/// The refused quantities, one after another with the same cid: zero, a negative number, a fraction of
/// a unit (`0.5`), and a number of units that is not a whole number of lots (`LOT + 5`). Each must be
/// [`Error::Rejected`] with code `invalid_qty`: never rounded into an order, never `Unknown`. Then the same
/// cid with one lot goes through, which shows the refusals used no cid, and the order is cancelled.
///
/// The connector must know that the lot of `id` is [`LOT`]: the T-Bank connector looks it up itself, for
/// BCS and Finam the test tells it with `price_grid().set_lot(..)`.
///
/// # Panics
///
/// With `[conformance:invalid_qty] <reason>`.
pub async fn invalid_qty<U: OrderExecutor + ?Sized>(ud: &U, id: &InstrumentId, price: Price) {
    const S: &str = "invalid_qty";
    let mut req = order(id, price, lots(1));
    let cid = req.cid;
    let bad = [
        ("zero", Qty::ZERO),
        ("negative", Qty::from(-LOT)),
        ("a fraction", Qty::new(Decimal::new(5, 1))),
        ("not a whole number of lots", Qty::from(LOT + 5)),
    ];
    for (what, qty) in bad {
        req.qty = qty;
        match ud.place(req.clone()).await {
            Err(Error::Rejected { code, .. }) if code == "invalid_qty" => {}
            other => fail(
                S,
                format!("a quantity that is {what} must be Rejected(invalid_qty), got {other:?}"),
            ),
        }
    }
    req.qty = lots(1);
    ud.place(req).await.unwrap_or_else(|e| {
        fail(
            S,
            format!("the refusals must not use the cid, but place failed: {e:?}"),
        )
    });
    cancel_patient(ud, S, cid).await;
}

/// An instrument id of another broker's kind is a configuration error, and nothing is sent.
///
/// `foreign` is an [`InstrumentId`] whose `NativeId` is of a kind this broker does not use. A `place` with
/// it must be [`Error::Config`]: not `Rejected` (that would say the broker was asked), not `Unknown`.
///
/// # Panics
///
/// With `[conformance:foreign_instrument_id] <reason>`.
pub async fn foreign_instrument_id<U: OrderExecutor + ?Sized>(
    ud: &U,
    foreign: &InstrumentId,
    price: Price,
) {
    const S: &str = "foreign_instrument_id";
    match ud.place(order(foreign, price, lots(1))).await {
        Err(Error::Config(_)) => {}
        other => fail(
            S,
            format!("an id of another broker must be a Config error, got {other:?}"),
        ),
    }
}

/// A place the broker never answers is `Unknown`: not repeated, not `Rejected`, resolved by asking.
///
/// `drive` receives the future of the `place` and must run it to its end while making the connector's
/// request time out (the test holds the answer at the mock and moves the clock past the timeout of the
/// connector). Then:
/// 1. The outcome is [`Error::Unknown`].
/// 2. The same cid again is `Rejected(duplicate_place)`: the library never repeats a place.
/// 3. `release` lets the broker finish: the mock took the order after all. `order_state(cid)` then finds
///    it (retried until it does), and the order is cancelled.
///
/// # Panics
///
/// With `[conformance:place_without_answer_is_unknown] <reason>`.
pub async fn place_without_answer_is_unknown<'a, U, D, DFut, R, RFut>(
    ud: &'a U,
    id: &InstrumentId,
    price: Price,
    drive: D,
    release: R,
) where
    U: OrderExecutor + ?Sized,
    D: FnOnce(
        std::pin::Pin<Box<dyn Future<Output = Result<brokapi_core::OrderAck, Error>> + 'a>>,
    ) -> DFut,
    DFut: Future<Output = Result<brokapi_core::OrderAck, Error>> + 'a,
    R: FnOnce() -> RFut,
    RFut: Future<Output = ()>,
{
    const S: &str = "place_without_answer_is_unknown";
    let req = order(id, price, lots(1));
    let cid = req.cid;
    let call: std::pin::Pin<Box<dyn Future<Output = Result<brokapi_core::OrderAck, Error>> + 'a>> =
        Box::pin(ud.place(req.clone()));
    match drive(call).await {
        Err(Error::Unknown) => {}
        other => fail(
            S,
            format!("a place without an answer must be Unknown, got {other:?}"),
        ),
    }
    match ud.place(req).await {
        Err(Error::Rejected { code, .. }) if code == "duplicate_place" => {}
        other => fail(
            S,
            format!("a place is never repeated: expected Rejected(duplicate_place), got {other:?}"),
        ),
    }
    release().await;
    let mut found = false;
    for _ in 0..50 {
        match ud.order_state(cid).await {
            Ok(_) => {
                found = true;
                break;
            }
            Err(e) if e.is_not_found() => tokio::task::yield_now().await,
            Err(Error::RateLimited { retry_after }) => tokio::time::sleep(retry_after).await,
            Err(e) => fail(S, format!("order_state failed: {e:?}")),
        }
    }
    if !found {
        fail(S, "the order the broker took is not found by order_state");
    }
    cancel_patient(ud, S, cid).await;
}

/// The stream of own orders breaks: the bot is told, the gate closes, `reconcile` opens it.
///
/// `cut` breaks the order and fill streams from the broker side, and returns once the broker has done it
/// (the connector then reopens the streams by itself). `reopened` says when the streams are open again.
/// Steps and checks:
/// 1. `subscribe_order_state` and `subscribe_executions`; `reopened` is awaited once for the first open.
/// 2. `cut()`. An [`Event::Reconnected`] arrives for the order state subscription, and one for the
///    executions subscription.
/// 3. `place` is [`Error::Degraded`] and sends nothing (`active_orders` shows no order of ours).
/// 4. `reconcile(&[])` succeeds and opens the gate. A place goes through and is cancelled.
///
/// # Panics
///
/// With `[conformance:stream_cut_degrades_until_reconcile] <reason>`.
pub async fn stream_cut_degrades_until_reconcile<U, C, CFut>(
    ud: &U,
    rx: &mut mpsc::Receiver<Envelope>,
    id: &InstrumentId,
    price: Price,
    cut: C,
) where
    U: OrderExecutor + ExecutionSource + ?Sized,
    C: FnOnce() -> CFut,
    CFut: Future<Output = ()>,
{
    const S: &str = "stream_cut_degrades_until_reconcile";
    let orders_sub = ud
        .subscribe_order_state()
        .await
        .unwrap_or_else(|e| fail(S, format!("subscribe_order_state failed: {e:?}")));
    let fills_sub = ud
        .subscribe_executions()
        .await
        .unwrap_or_else(|e| fail(S, format!("subscribe_executions failed: {e:?}")));
    cut().await;
    // One notice for each of the two subscriptions, in whatever order they come.
    let (mut orders_told, mut fills_told) = (false, false);
    while !(orders_told && fills_told) {
        let env = next_matching(S, rx, |e| matches!(e.payload, Event::Reconnected { .. })).await;
        let Event::Reconnected { sub, .. } = env.payload else {
            unreachable!()
        };
        if env.sub != Some(sub) {
            fail(
                S,
                format!("the Reconnected of {sub:?} carries sub {:?}", env.sub),
            );
        }
        if sub == orders_sub {
            orders_told = true;
        } else if sub == fills_sub {
            fills_told = true;
        } else {
            fail(
                S,
                format!("Reconnected for a subscription nobody made: {sub:?}"),
            );
        }
    }
    let req = order(id, price, lots(1));
    let cid = req.cid;
    match ud.place(req).await {
        Err(Error::Degraded) => {}
        other => fail(
            S,
            format!("after the cut a place must be Degraded, got {other:?}"),
        ),
    }
    if active_among(ud, S, &[cid]).await != 0 {
        fail(S, "the refused place reached the broker");
    }
    ud.reconcile(&[])
        .await
        .unwrap_or_else(|e| fail(S, format!("reconcile after the cut failed: {e:?}")));
    let cid = place_limit(ud, S, id, price, lots(1)).await;
    cancel_patient(ud, S, cid).await;
}

/// A command that waits in the queue of its instrument is checked against the gate again when its turn
/// comes: a gate that closed meanwhile stops it, and nothing is sent.
///
/// Two places on one instrument go out together. The first is held at the broker (`in_flight` returns when
/// the broker has it in hand), the second waits in the queue of the instrument. Then `degrade` closes the
/// gate (it breaks the order streams) and the test waits for the `Reconnected` event, and `release` lets
/// the broker finish the first one. Checks:
/// - the first place is `Ok` or `Unknown` (it was sent before the gate closed);
/// - the second is [`Error::Degraded`]: it did not go out after the queue;
/// - after `reconcile(&[])`, of the two cids only the first is at the broker. Both are cancelled.
///
/// # Panics
///
/// With `[conformance:gate_checked_after_the_queue] <reason>`.
pub async fn gate_checked_after_the_queue<U, A, AFut, D, DFut, R, RFut>(
    ud: &U,
    rx: &mut mpsc::Receiver<Envelope>,
    id: &InstrumentId,
    price: Price,
    in_flight: A,
    degrade: D,
    release: R,
) where
    U: OrderExecutor + ?Sized,
    A: FnOnce() -> AFut,
    AFut: Future<Output = ()>,
    D: FnOnce() -> DFut,
    DFut: Future<Output = ()>,
    R: FnOnce() -> RFut,
    RFut: Future<Output = ()>,
{
    const S: &str = "gate_checked_after_the_queue";
    let orders_sub = ud
        .subscribe_order_state()
        .await
        .unwrap_or_else(|e| fail(S, format!("subscribe_order_state failed: {e:?}")));
    let (first, second) = (order(id, price, lots(1)), order(id, price, lots(1)));
    let (c1, c2) = (first.cid, second.cid);
    let driver = async {
        in_flight().await;
        degrade().await;
        next_matching(
            S,
            rx,
            |e| matches!(e.payload, Event::Reconnected { sub, .. } if sub == orders_sub),
        )
        .await;
        release().await;
    };
    let (r1, r2, ()) = tokio::join!(ud.place(first), ud.place(second), driver);
    if !matches!(r1, Ok(_) | Err(Error::Unknown)) {
        fail(
            S,
            format!("the first place was sent before the gate closed, got {r1:?}"),
        );
    }
    if !matches!(r2, Err(Error::Degraded)) {
        fail(S, format!("the queued place must be Degraded, got {r2:?}"));
    }
    ud.reconcile(&[c1])
        .await
        .unwrap_or_else(|e| fail(S, format!("reconcile failed: {e:?}")));
    let mut at_broker = 0;
    for _ in 0..50 {
        at_broker = active_among(ud, S, &[c1, c2]).await;
        if at_broker >= 1 {
            break;
        }
        tokio::task::yield_now().await;
    }
    if at_broker != 1 {
        fail(
            S,
            format!("only the first order may be at the broker, found {at_broker}"),
        );
    }
    cancel_patient(ud, S, c1).await;
}

/// The order book stream is void after a break: a [`Event::Resync`] follows, then a fresh snapshot.
///
/// `prepare` sets the mock so that it breaks the stream after the first snapshot (it runs before the
/// subscribe). Steps and checks, on one subscription:
/// 1. A [`Event::Resync`] for the subscription and the instrument arrives (books before it may or may not).
/// 2. A book arrives after the `Resync`: the stream came back.
///
/// # Panics
///
/// With `[conformance:order_book_resync] <reason>`.
pub async fn order_book_resync<M: OrderBookSource + ?Sized>(
    md: &M,
    rx: &mut mpsc::Receiver<Envelope>,
    id: &InstrumentId,
    prepare: impl FnOnce(),
) {
    const S: &str = "order_book_resync";
    prepare();
    let sub = md
        .subscribe_order_book(id, SubscribeOpts::lossless(64))
        .await
        .unwrap_or_else(|e| fail(S, format!("subscribe_order_book failed: {e:?}")));
    // The cut may come before or after the first book: wait for the Resync of the subscription.
    let env = next_matching(
        S,
        rx,
        |e| matches!(e.payload, Event::Resync { sub: s, .. } if s == sub),
    )
    .await;
    let Event::Resync { instrument, .. } = &env.payload else {
        unreachable!()
    };
    if instrument.key != id.key {
        fail(
            S,
            format!("Resync for {:?}, expected {:?}", instrument.key, id.key),
        );
    }
    // After it the stream is back: a book arrives.
    next_matching(S, rx, |e| {
        e.sub == Some(sub) && matches!(e.payload, Event::OrderBook(_))
    })
    .await;
}

/// A read of an instrument the broker does not know is `Rejected(not_found)`, the same at every broker.
///
/// Not `Transport` (that would invite a retry), not `Ok`, not a differently spelled code.
///
/// # Panics
///
/// With `[conformance:unknown_instrument] <reason>`.
pub async fn unknown_instrument<M: OrderBookSource + ?Sized>(md: &M, unknown: &InstrumentId) {
    const S: &str = "unknown_instrument";
    match md.order_book(unknown, 5).await {
        Err(e) if e.is_not_found() => {}
        other => fail(
            S,
            format!("an unknown instrument must be Rejected(not_found), got {other:?}"),
        ),
    }
}

/// The history of own trades is complete: `expected` trades come back, none is dropped or doubled.
///
/// The test has put `expected` trades in the mock (more than one page of the broker's list). Checks:
/// `executions(all time)` returns exactly `expected` entries, with `expected` different trade ids.
///
/// # Panics
///
/// With `[conformance:executions_history_is_complete] <reason>`.
pub async fn executions_history_is_complete<U: ExecutionSource + ?Sized>(ud: &U, expected: usize) {
    const S: &str = "executions_history_is_complete";
    let range = TimeRange::new(UnixNanos::ZERO, UnixNanos::from_nanos(i64::MAX / 2));
    let got = ud
        .executions(range)
        .await
        .unwrap_or_else(|e| fail(S, format!("executions failed: {e:?}")));
    let mut ids: Vec<&str> = got.iter().map(|x| x.trade_id.as_str()).collect();
    ids.sort_unstable();
    ids.dedup();
    if got.len() != expected || ids.len() != expected {
        fail(
            S,
            format!(
                "expected {expected} different trades, got {} entries with {} different ids",
                got.len(),
                ids.len()
            ),
        );
    }
}
