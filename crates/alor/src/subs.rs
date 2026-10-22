//! The subscription records a connector keeps, and the generator of subscription ids.
//!
//! These types are the bookkeeping of the connectors. A bot does not build them: it gets a
//! [`SubscriptionId`] from a `subscribe_*` call and passes it back to `unsubscribe`.

use std::sync::atomic::{AtomicU64, Ordering};

use brokapi_core::{InstrumentId, SubscribeOpts, SubscriptionId};

/// What a market data subscription delivers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    /// The `guid` of the WebSocket subscription: it names the data frames and the `unsubscribe`.
    pub guid: String,
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
    /// The `guid` of the WebSocket subscription.
    pub guid: String,
}

/// Hands out subscription ids of one connector, in increasing order, from a thread-safe counter.
///
/// The market data connector counts from 1, the user data connector from 1001, so the ids of the two
/// connectors of one bot do not collide.
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

/// A new `guid` for a WebSocket subscription.
pub fn new_guid() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// Waits for the broker to confirm a new subscription.
///
/// `state` reads the state of the subscription from the core. The wait ends with `Ok` when the subscription is
/// active (the broker confirmed it, or data came), with [`Error::Rejected`] (`subscription_refused`) when the
/// broker refused it, and with `Ok` again when `limit` passes with no answer: the subscription goes on opening
/// in the background, and a bot that needs to know reads the state of its stream.
///
/// # Errors
///
/// [`Error::Rejected`] with the text of the broker when the subscription was refused.
pub async fn wait_ack<C>(
    core: &tokio::sync::Mutex<C>,
    notify: std::sync::Arc<tokio::sync::Notify>,
    limit: std::time::Duration,
    state: impl Fn(&C) -> (crate::core::SubState, Option<String>),
) -> Result<(), brokapi_core::Error> {
    use crate::core::SubState;
    let wait = async {
        loop {
            let notified = notify.notified();
            tokio::pin!(notified);
            // Registered before the state is read, so a change in between is not missed.
            notified.as_mut().enable();
            match state(&*core.lock().await) {
                (SubState::Active, _) => return Ok(()),
                (SubState::Failed, reason) => {
                    return Err(brokapi_core::Error::Rejected {
                        code: "subscription_refused".into(),
                        message: reason
                            .unwrap_or_else(|| "the broker refused the subscription".into()),
                    });
                }
                (SubState::Pending, _) => {}
            }
            notified.await;
        }
    };
    match tokio::time::timeout(limit, wait).await {
        Ok(result) => result,
        Err(_) => Ok(()),
    }
}
