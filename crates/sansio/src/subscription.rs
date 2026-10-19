//! Desired subscription set passed into sans-io cores.

use brokapi_core::{InstrumentId, SubscriptionId, Timeframe};

/// Kind of streaming subscription the connector should maintain.
///
/// Market data kinds carry the instrument. Account kinds ([`OrderState`](Self::OrderState),
/// [`Portfolio`](Self::Portfolio), [`Executions`](Self::Executions)) are per account and carry
/// nothing.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SubscriptionKind {
    /// Order book updates.
    OrderBook {
        /// The instrument to follow.
        instrument: InstrumentId,
        /// Number of price levels per side.
        depth: u32,
    },
    /// Best bid and ask updates.
    Quotes {
        /// The instrument to follow.
        instrument: InstrumentId,
    },
    /// Trades printed on the market, by all participants.
    LastTrades {
        /// The instrument to follow.
        instrument: InstrumentId,
    },
    /// Updates of the current candle.
    LastCandle {
        /// The instrument to follow.
        instrument: InstrumentId,
        /// Candle length.
        timeframe: Timeframe,
    },
    /// State changes of the account's own orders.
    OrderState,
    /// Portfolio, positions and balances updates.
    Portfolio,
    /// Own fills, pushed as `Event::Execution`.
    Executions,
}

/// One wanted subscription: what to follow, under which id.
///
/// The id is given by the runtime and stays the same for the life of the subscription. Events
/// for it carry this id.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DesiredSubscription {
    /// The id events of this subscription are tagged with.
    pub id: SubscriptionId,
    /// What the subscription follows.
    pub kind: SubscriptionKind,
}

/// The full set of subscriptions the consumer wants: the input of
/// [`BrokerCore::apply`](crate::BrokerCore::apply).
///
/// It is a plain list in the order given. The set does not check for duplicates: two entries with
/// the same id both stay, and [`find`](Self::find) returns the first.
///
/// # Examples
///
/// ```
/// use brokapi_core::{InstrumentId, InstrumentKey, NativeId, SubscriptionId};
/// use brokapi_sansio::{DesiredSubscription, SubscriptionKind, SubscriptionSet};
///
/// let sber = InstrumentId::new(
///     InstrumentKey::new("SBER", "TQBR"),
///     NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() },
/// );
/// let quotes = DesiredSubscription {
///     id: SubscriptionId::new(1),
///     kind: SubscriptionKind::Quotes { instrument: sber.clone() },
/// };
/// let book = DesiredSubscription {
///     id: SubscriptionId::new(2),
///     kind: SubscriptionKind::OrderBook { instrument: sber, depth: 10 },
/// };
///
/// let set = SubscriptionSet::with_subscriptions(vec![quotes.clone(), book]);
/// assert_eq!(set.subs().len(), 2);
/// assert_eq!(set.find(SubscriptionId::new(1)), Some(&quotes));
/// assert!(set.find(SubscriptionId::new(9)).is_none());
/// assert!(SubscriptionSet::new().is_empty());
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SubscriptionSet {
    subs: Vec<DesiredSubscription>,
}

impl SubscriptionSet {
    /// Creates an empty set: no subscriptions wanted.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a set from a list, keeping its order.
    pub fn with_subscriptions(subs: Vec<DesiredSubscription>) -> Self {
        Self { subs }
    }

    /// The subscriptions, in the order given.
    pub fn subs(&self) -> &[DesiredSubscription] {
        &self.subs
    }

    /// Whether the set wants nothing.
    pub fn is_empty(&self) -> bool {
        self.subs.is_empty()
    }

    /// Iterates over the subscriptions, in the order given.
    pub fn iter(&self) -> impl Iterator<Item = &DesiredSubscription> {
        self.subs.iter()
    }

    /// The subscription with this id, or `None`.
    pub fn find(&self, id: SubscriptionId) -> Option<&DesiredSubscription> {
        self.subs.iter().find(|s| s.id == id)
    }
}
