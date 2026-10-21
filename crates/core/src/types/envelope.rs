//! What a connector writes into your channel: [`Envelope`] around an [`Event`].

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::execution::Execution;
use super::instrument::InstrumentId;
use super::market::{Candle, MarketTrade, OrderBook, Quote, ResyncReason};
use super::order::OrderSnapshot;
use super::portfolio::{Balance, Portfolio, Position};
use super::subscription::SubscriptionId;
use super::time::{Timestamps, UnixNanos};

/// The version of the [`Envelope`] format, now `1`.
///
/// It grows only forward, with additive changes. If you store or forward envelopes, refuse a
/// `schema` higher than the one you were built for, and ignore fields you do not know.
pub const ENVELOPE_SCHEMA: u16 = 1;

/// The payload of an [`Envelope`]: one thing that happened.
///
/// The same set for every broker. Which events you get depends on what you subscribed to. A
/// subscription's events carry its [`SubscriptionId`] in [`Envelope::sub`].
///
/// # Examples
///
/// ```
/// use brokapi_core::{Envelope, Event};
///
/// /// What a consumer loop does with each envelope.
/// fn handle(env: &Envelope) {
///     match &env.payload {
///         Event::OrderBook(_) | Event::Quote(_) | Event::MarketTrade(_) | Event::Candle(_) => { /* market data */ }
///         Event::Order(snapshot) => println!("order {} is {:?}", snapshot.cid, snapshot.state),
///         Event::Execution(fill) => println!("fill {} at {}", fill.trade_id, fill.price),
///         Event::Portfolio(_) | Event::Positions(_) | Event::Balances(_) => { /* account */ }
///         Event::Resync { .. } => { /* drop the book of that instrument, wait for a snapshot */ }
///         Event::Overflow { .. } => { /* events were lost: read the state, call reconcile */ }
///         Event::Reconnected { .. } => { /* the stream was cut and reopened: read the state, call reconcile */ }
///         // The enum is `#[non_exhaustive]`: a later version may add an event. Skip what you do not know.
///         _ => {}
///     }
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum Event {
    /// An order book from an order book subscription.
    OrderBook(OrderBook),
    /// A deal of the public tape from a market trades subscription.
    MarketTrade(MarketTrade),
    /// The best bid and ask from a quotes subscription.
    Quote(Quote),
    /// A bar from a candles subscription.
    Candle(Candle),
    /// A change of one of your orders, from the order state subscription. Carries the cid.
    Order(OrderSnapshot),
    /// A fill of one of your orders, from the executions subscription.
    Execution(Execution),
    /// The whole account, from a portfolio subscription. Replaces the previous one.
    Portfolio(Portfolio),
    /// Positions only, from a portfolio subscription of a connector that sends the parts
    /// separately (the T-Bank connector does).
    Positions(Vec<Position>),
    /// Balances only, from a portfolio subscription of a connector that sends the parts
    /// separately (the T-Bank connector does).
    Balances(Vec<Balance>),
    /// What you hold for this instrument on this subscription is void, a new snapshot follows. Sent
    /// after a sequence gap or a reconnect.
    Resync {
        /// The subscription that must start over.
        sub: SubscriptionId,
        /// The instrument whose data is void.
        instrument: InstrumentId,
        /// Why.
        reason: ResyncReason,
    },
    /// A [`Lossless`](crate::Backpressure::Lossless) subscription overflowed: events were lost and
    /// cannot be recovered from the stream.
    ///
    /// The connector is now degraded and refuses trading commands with
    /// [`Error::Degraded`](crate::Error::Degraded). Read the state from the broker
    /// ([`OrderExecutor::active_orders`](crate::OrderExecutor::active_orders),
    /// [`ExecutionSource::executions`](crate::ExecutionSource::executions)), then call
    /// [`OrderExecutor::reconcile`](crate::OrderExecutor::reconcile). The notice can arrive before
    /// older events of the same subscription that are still waiting in the buffer.
    Overflow {
        /// The subscription that lost events.
        sub: SubscriptionId,
        /// Wall clock when the loss was found. The connector sets it to "now" at the overflow, so it
        /// is an upper bound for where the hole starts, not the time of the first lost event.
        lost_from: UnixNanos,
    },
    /// A stream of your orders or fills was cut and opened again: whatever happened to your orders in
    /// the gap never reached you.
    ///
    /// Sent for the order state and the executions subscriptions (the user data streams that carry
    /// money), after the stream is open again. Market data streams have [`Event::Resync`] instead, and
    /// the portfolio stream needs nothing because every message of it is a whole state.
    ///
    /// The connector is now degraded, like after an [`Event::Overflow`]: it refuses trading commands
    /// with [`Error::Degraded`](crate::Error::Degraded) until you read the state from the broker
    /// ([`OrderExecutor::active_orders`](crate::OrderExecutor::active_orders),
    /// [`OrderExecutor::order_state`](crate::OrderExecutor::order_state),
    /// [`ExecutionSource::executions`](crate::ExecutionSource::executions)) and call
    /// [`OrderExecutor::reconcile`](crate::OrderExecutor::reconcile).
    Reconnected {
        /// The subscription whose stream was cut.
        sub: SubscriptionId,
        /// Wall clock when the connector saw the cut. The hole starts at or before it, so read the
        /// history from some time before this moment.
        gap_from: UnixNanos,
    },
}

/// One event with its address: which connector, which subscription, when, and what.
///
/// This is the item type of the `tokio::sync::mpsc` channel you give to a connector at start. All
/// subscriptions of the connector write into that one channel, so route by `sub`. Read the channel
/// steadily: a connector never blocks on it and applies the
/// [`Backpressure`](crate::Backpressure) policy of the subscription instead.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use std::time::Instant;
/// use brokapi_core::{Envelope, Event, SubscriptionId, Timestamps, UnixNanos, ENVELOPE_SCHEMA};
///
/// let ts = Timestamps::new(UnixNanos::from_nanos(1), Instant::now());
/// let env = Envelope::new(Arc::from("bcs-md"), ts, Event::Balances(vec![]))
///     .with_sub(SubscriptionId::new(7));
/// assert_eq!(env.schema, ENVELOPE_SCHEMA);
/// assert_eq!(&*env.source, "bcs-md");
/// assert_eq!(env.sub, Some(SubscriptionId::new(7)));
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Envelope {
    /// Format version, see [`ENVELOPE_SCHEMA`].
    pub schema: u16,
    /// The label you gave the connector in its config (for example `"bcs-md"`). The library does
    /// not interpret it. It lets you tell connectors apart when several write into one channel.
    pub source: Arc<str>,
    /// The subscription this event belongs to. `None` for an event that belongs to no subscription.
    pub sub: Option<SubscriptionId>,
    /// When the envelope was built, which is when it left the subscription buffer, not when the
    /// bytes arrived. For a slow reader that is later than the arrival. The data inside has its own
    /// timestamps.
    pub ts: Timestamps,
    /// What happened.
    pub payload: Event,
}

impl Envelope {
    /// An envelope of the current schema with no subscription. Add one with [`with_sub`](Self::with_sub).
    pub fn new(source: Arc<str>, ts: Timestamps, payload: Event) -> Self {
        Self {
            schema: ENVELOPE_SCHEMA,
            source,
            sub: None,
            ts,
            payload,
        }
    }

    /// Sets the subscription the envelope belongs to.
    pub fn with_sub(mut self, sub: SubscriptionId) -> Self {
        self.sub = Some(sub);
        self
    }

    /// Like [`with_sub`](Self::with_sub) for a subscription that may be absent: `None` leaves the envelope
    /// without one.
    #[must_use]
    pub fn with_sub_opt(mut self, sub: Option<SubscriptionId>) -> Self {
        self.sub = sub;
        self
    }
}
