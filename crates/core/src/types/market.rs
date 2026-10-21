//! Market data types: order book, trades, quotes, candles, and the time range of a history request.

use serde::{Deserialize, Serialize};

use super::instrument::InstrumentId;
use super::number::{Price, Qty};
use super::primitives::Side;
use super::time::{Timestamps, UnixNanos};

/// One price level of an order book or a quote: a price and the total quantity waiting at it.
///
/// # Examples
///
/// ```
/// # use brokapi_core::{Decimal, dec};
/// use brokapi_core::{BookLevel, Price, Qty};
///
/// let level = BookLevel { price: Price::new(dec!(312.45)), qty: Qty::from(120) };
/// assert_eq!(level.price.to_string(), "312.45");
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BookLevel {
    /// Price of the level.
    pub price: Price,
    /// Total quantity at this price, see [`Qty`] for the unit.
    pub qty: Qty,
}

/// An order book: either a full snapshot or an incremental update.
///
/// [`OrderBookSource::order_book`](crate::OrderBookSource::order_book) always answers a `Snapshot`.
/// A stream may send snapshots, or (where [`MarketDataCaps::incremental_order_book`](crate::MarketDataCaps::incremental_order_book)
/// is `true`) a snapshot followed by `Increment`s with a growing `seq`. In both variants the bid
/// side is listed from the best (highest) price down and the ask side from the best (lowest) price
/// up. This is checked for the BCS mock in `brokapi-conformance`, not verified against the live API.
///
/// How an `Increment` is applied (whether a level with zero quantity removes it) is defined by the
/// connector that sends it, see its docs. The BCS, T-Bank and Finam connectors declare
/// `incremental_order_book: false` and send snapshots only.
/// When a gap in `seq` or a reconnect makes the book untrustworthy the stream sends
/// [`Event::Resync`](crate::Event::Resync) and a new snapshot.
///
/// # Examples
///
/// ```
/// use brokapi_core::{BookLevel, OrderBook};
///
/// /// The best bid of a book, whatever the variant.
/// fn best_bid(book: &OrderBook) -> Option<&BookLevel> {
///     match book {
///         OrderBook::Snapshot { bids, .. } | OrderBook::Increment { bids, .. } => bids.first(),
///     }
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum OrderBook {
    /// The whole book to the depth asked for. Replaces whatever you had.
    Snapshot {
        /// The instrument of the book.
        instrument: InstrumentId,
        /// Buy levels, best (highest) price first.
        bids: Vec<BookLevel>,
        /// Sell levels, best (lowest) price first.
        asks: Vec<BookLevel>,
        /// When received, and the broker time if given.
        ts: Timestamps,
    },
    /// A change to apply to the book you have.
    Increment {
        /// The instrument of the book.
        instrument: InstrumentId,
        /// Sequence number of this update. A jump means updates were lost: expect
        /// [`Event::Resync`](crate::Event::Resync).
        seq: u64,
        /// Changed buy levels.
        bids: Vec<BookLevel>,
        /// Changed sell levels.
        asks: Vec<BookLevel>,
        /// When received, and the broker time if given.
        ts: Timestamps,
    },
}

/// A trade on the exchange tape: someone else's or yours, a public record of one deal.
///
/// Your own fills are [`Execution`](crate::Execution), a different type with different delivery
/// guarantees.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarketTrade {
    /// What was traded.
    pub instrument: InstrumentId,
    /// Price of the deal.
    pub price: Price,
    /// Quantity of the deal, see [`Qty`] for the unit.
    pub qty: Qty,
    /// Side of the deal as the broker reports it. A history record whose side the broker does not
    /// give is dropped by the BCS connector.
    pub side: Side,
    /// The exchange's id of the deal, when the broker gives one.
    pub trade_id: Option<String>,
    /// When received, and the broker time if given.
    pub ts: Timestamps,
}

/// The best bid and best ask of an instrument: the top of the book without the rest.
///
/// Comes from [`MarketDataSource::subscribe_quotes`](crate::MarketDataSource::subscribe_quotes) as
/// [`Event::Quote`](crate::Event::Quote). There is no one-shot read of a quote in the traits: read
/// the top of [`OrderBookSource::order_book`](crate::OrderBookSource::order_book) with depth 1.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Quote {
    /// The instrument of the quote.
    pub instrument: InstrumentId,
    /// Best buy level. `None` when nobody is buying.
    pub bid: Option<BookLevel>,
    /// Best sell level. `None` when nobody is selling.
    pub ask: Option<BookLevel>,
    /// When received, and the broker time if given.
    pub ts: Timestamps,
}

/// The length of one candle.
///
/// Not every broker has every timeframe. The BCS connector has no `M10` and answers
/// [`Error::Unsupported`](crate::Error::Unsupported) for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Timeframe {
    /// 1 minute.
    M1,
    /// 5 minutes.
    M5,
    /// 10 minutes.
    M10,
    /// 15 minutes.
    M15,
    /// 30 minutes.
    M30,
    /// 1 hour.
    H1,
    /// 4 hours.
    H4,
    /// 1 day.
    D1,
    /// 1 week.
    W1,
    /// 1 month.
    MN,
}

impl Timeframe {
    /// The length of one bar in seconds. A month is counted as 30 days: it is a size for planning
    /// requests, not a calendar.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::Timeframe;
    ///
    /// assert_eq!(Timeframe::M5.seconds(), 300);
    /// assert_eq!(Timeframe::H1.seconds(), 3_600);
    /// assert_eq!(Timeframe::D1.seconds(), 86_400);
    /// ```
    pub fn seconds(self) -> u64 {
        match self {
            Self::M1 => 60,
            Self::M5 => 5 * 60,
            Self::M10 => 10 * 60,
            Self::M15 => 15 * 60,
            Self::M30 => 30 * 60,
            Self::H1 => 3_600,
            Self::H4 => 4 * 3_600,
            Self::D1 => 86_400,
            Self::W1 => 7 * 86_400,
            Self::MN => 30 * 86_400,
        }
    }
}

/// One OHLC bar of an instrument.
///
/// From [`MarketDataSource::candles`](crate::MarketDataSource::candles) (history) or as
/// [`Event::Candle`](crate::Event::Candle) from a candle subscription. A subscription sends the
/// latest bar, so the same bar can arrive several times while it forms (not verified against the
/// live API for every broker). Use `open_ts` to tell bars apart.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Candle {
    /// The instrument of the bar.
    pub instrument: InstrumentId,
    /// Length of the bar.
    pub timeframe: Timeframe,
    /// First price of the bar.
    pub open: Price,
    /// Highest price of the bar.
    pub high: Price,
    /// Lowest price of the bar.
    pub low: Price,
    /// Last price of the bar.
    pub close: Price,
    /// Traded quantity in the bar, see [`Qty`] for the unit.
    pub volume: Qty,
    /// Start of the bar.
    pub open_ts: UnixNanos,
    /// End of the bar.
    pub close_ts: UnixNanos,
    /// When received, and the broker time if given.
    pub ts: Timestamps,
}

/// A span of time for history requests: [`MarketDataSource::candles`](crate::MarketDataSource::candles),
/// [`InstrumentSource::schedule`](crate::InstrumentSource::schedule) and
/// [`ExecutionSource::executions`](crate::ExecutionSource::executions).
///
/// Whether the ends are inclusive, and how far back a broker serves, are decided by the broker. The
/// BCS schedule answer is for one day, the day of `from`.
///
/// # Examples
///
/// ```
/// use brokapi_core::{TimeRange, UnixNanos};
///
/// // Everything since the epoch: "all history".
/// let all = TimeRange::new(UnixNanos::ZERO, UnixNanos::from_nanos(i64::MAX / 2));
/// assert!(all.from < all.to);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TimeRange {
    /// Start of the span.
    pub from: UnixNanos,
    /// End of the span.
    pub to: UnixNanos,
}

impl TimeRange {
    /// A span from `from` to `to`. Nothing is checked, a reversed range is the caller's mistake.
    pub fn new(from: UnixNanos, to: UnixNanos) -> Self {
        Self { from, to }
    }
}

/// Why a stream tells you to drop what you have and start over: see [`Event::Resync`](crate::Event::Resync).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum ResyncReason {
    /// An incremental update was missed (the sequence number jumped). The book you hold is wrong.
    SequenceGap,
    /// The connection was re-established and the subscription restored. What you held before is
    /// void. The BCS connector sends this before the new snapshot after a reconnect, not on the
    /// first connect.
    SubscriptionReset,
    /// A resync was asked for by hand, not forced by a gap or a reconnect.
    Manual,
}
