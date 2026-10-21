//! Subscriptions: the id a subscribe call returns and the options that say what to do when the
//! consumer is slow.

use serde::{Deserialize, Serialize};

/// The handle of one subscription.
///
/// Returned by every `subscribe_*` method, and carried by each [`Envelope`](crate::Envelope) that
/// belongs to the subscription (`Envelope::sub`). It is unique only inside ONE connector. Two
/// connectors can use the same number, so tell them apart by [`Envelope::source`](crate::Envelope::source).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SubscriptionId(pub u64);

impl SubscriptionId {
    /// Wraps a number. Connectors make their own ids, this is for tests and tools.
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    /// The raw number.
    pub fn value(self) -> u64 {
        self.0
    }
}

/// What a subscription does when you read your channel slower than events arrive.
///
/// A connector never blocks its reading task on your channel. Each subscription has a buffer of its
/// own, and this policy says what happens when it is full:
///
/// | Policy | Meaning | Use for |
/// |---|---|---|
/// | `Conflate` | Keep only the latest value, replace older ones. | Order books, quotes, portfolio: full snapshots. |
/// | `DropNewest` | When full, throw away the new event and count it. | Streams you can lose, like a tape. |
/// | `Lossless` | Must not lose. When full: an [`Event::Overflow`](crate::Event::Overflow) is sent and the connector becomes degraded, trading commands then fail with [`Error::Degraded`](crate::Error::Degraded) until `reconcile`. | Own order states and own fills. |
///
/// `Lossless` is the only honest answer for own orders and fills: the source cannot be slowed down,
/// so a hole must be admitted loudly and trading stopped, instead of continuing with a wrong picture.
///
/// # Examples
///
/// ```
/// use brokapi_core::{Backpressure, SubscribeOpts};
///
/// let opts = SubscribeOpts::lossless(1024);
/// assert_eq!(opts.backpressure, Backpressure::Lossless { cap: 1024 });
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum Backpressure {
    /// Full snapshots: a new value replaces the previous one that was not read yet.
    Conflate,
    /// Must not drop. When the buffer reaches `cap` the overflow is terminal for trading, see the
    /// table above.
    Lossless {
        /// How many events the subscription buffer holds.
        cap: usize,
    },
    /// When the buffer reaches `cap`, the new event is dropped and counted.
    DropNewest {
        /// How many events the subscription buffer holds.
        cap: usize,
    },
}

/// Options of a `subscribe_*` call.
///
/// Some subscriptions take no options and fix the policy themselves: `subscribe_order_state` and
/// `subscribe_executions` of the BCS connector always use [`Backpressure::Lossless`], with a buffer
/// size from the connector config.
///
/// # Examples
///
/// ```
/// use brokapi_core::{Backpressure, SubscribeOpts};
///
/// // Latest top 10 levels of the book, older ones replaced when the reader is slow.
/// let opts = SubscribeOpts { depth: Some(10), ..SubscribeOpts::conflate() };
/// assert_eq!(opts.backpressure, Backpressure::Conflate);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscribeOpts {
    /// What to do when the consumer is slow.
    pub backpressure: Backpressure,
    /// How many price levels of an order book to follow. `None` takes the connector's default (the
    /// BCS connector uses its maximum, see
    /// [`MarketDataCaps::max_order_book_depth`](crate::MarketDataCaps::max_order_book_depth)).
    /// Ignored by subscriptions that are not order books.
    pub depth: Option<u32>,
}

impl SubscribeOpts {
    /// Options with the [`Backpressure::Conflate`] policy and the default depth.
    pub fn conflate() -> Self {
        Self {
            backpressure: Backpressure::Conflate,
            depth: None,
        }
    }

    /// Options with the [`Backpressure::Lossless`] policy, a buffer of `cap` events and the default depth.
    pub fn lossless(cap: usize) -> Self {
        Self {
            backpressure: Backpressure::Lossless { cap },
            depth: None,
        }
    }
}
