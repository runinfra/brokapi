//! The subscription records a connector keeps, and the generator of subscription ids.
//!
//! These types are the bookkeeping of the connectors. A bot does not build them: it gets a
//! [`SubscriptionId`] from a `subscribe_*` call and passes it back to `unsubscribe`.

use std::sync::atomic::{AtomicU64, Ordering};

use brokapi_bcs_protocol::ws::models::MarketDataSubscribe;
use brokapi_core::{InstrumentId, SubscribeOpts, SubscriptionId};

/// What a market data subscription delivers.
#[derive(Debug, Clone)]
pub enum MdSubKind {
    /// Order book snapshots of one instrument.
    OrderBook,
    /// Trades of the market (the tape) of one instrument.
    MarketTrades,
    /// Best bid and ask of one instrument.
    Quotes,
    /// The latest candle of one instrument and timeframe.
    Candles,
}

/// One live market data subscription, as the connector remembers it.
#[derive(Debug, Clone)]
pub struct MdSubscription {
    /// The id the bot got from `subscribe_*`.
    pub id: SubscriptionId,
    /// The instrument the data is about.
    pub instrument: InstrumentId,
    /// What the subscription delivers.
    pub kind: MdSubKind,
    /// The options given at subscribe time (backpressure policy, depth).
    pub opts: SubscribeOpts,
    /// The WebSocket message that created it. It is replayed after a reconnect, and its mirror
    /// image is sent to unsubscribe.
    pub ws: MarketDataSubscribe,
}

/// What a user data subscription delivers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UdSubKind {
    /// Status changes of orders.
    OrderState,
    /// Portfolio updates.
    Portfolio,
    /// Fills of orders.
    Executions,
}

/// One live user data subscription, as the connector remembers it.
#[derive(Debug, Clone)]
pub struct UdSubscription {
    /// The id the bot got from `subscribe_*`.
    pub id: SubscriptionId,
    /// What the subscription delivers.
    pub kind: UdSubKind,
    /// The options given at subscribe time. Order state and executions use a lossless buffer.
    pub opts: SubscribeOpts,
}

/// Hands out subscription ids of one connector, in increasing order, from a thread-safe counter.
///
/// The market data connector counts from 1, the user data connector from 1001, so the ids of the
/// two connectors of one bot do not collide.
pub struct SubIdGen(AtomicU64);

impl SubIdGen {
    /// Creates a generator whose first id is `start + 1`.
    pub fn new(start: u64) -> Self {
        Self(AtomicU64::new(start))
    }

    /// Returns the next id. Safe to call from several tasks.
    pub fn next(&self) -> SubscriptionId {
        SubscriptionId::new(self.0.fetch_add(1, Ordering::Relaxed) + 1)
    }
}
