//! The subscription records a connector keeps, and the generators of subscription ids and request ids.
//!
//! These types are the bookkeeping of the connectors. A bot does not build them: it gets a
//! [`SubscriptionId`] from a `subscribe_*` call and passes it back to `unsubscribe`.

use std::sync::atomic::{AtomicU64, Ordering};

use brokapi_core::{InstrumentId, SubscribeOpts, SubscriptionId};

/// What a market data subscription is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MdSubKind {
    /// An order book stream.
    OrderBook,
    /// Public trades.
    MarketTrades,
    /// Best bid and ask.
    Quotes,
    /// Candles.
    Candles,
}

/// A market data subscription as the connector keeps it.
#[derive(Debug, Clone)]
pub struct MdSubscription {
    /// The id given to the caller.
    pub id: SubscriptionId,
    /// The instrument.
    pub instrument: InstrumentId,
    /// The purpose.
    pub kind: MdSubKind,
    /// The options the caller gave.
    pub opts: SubscribeOpts,
    /// The topic of the stream at the exchange.
    pub topic: String,
}

/// What a user data subscription is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UdSubKind {
    /// Own orders.
    OrderState,
    /// Positions and balances.
    Portfolio,
    /// Own fills.
    Executions,
}

/// A user data subscription as the connector keeps it.
#[derive(Debug, Clone)]
pub struct UdSubscription {
    /// The id given to the caller.
    pub id: SubscriptionId,
    /// The purpose.
    pub kind: UdSubKind,
    /// The options the caller gave.
    pub opts: SubscribeOpts,
    /// The topics of the streams at the exchange.
    pub topics: Vec<String>,
}

/// Hands out subscription ids, counting up from `start + 1`.
pub struct SubIdGen(AtomicU64);

impl SubIdGen {
    /// A generator whose first id is `start + 1`.
    pub fn new(start: u64) -> Self {
        Self(AtomicU64::new(start))
    }

    /// The next id.
    pub fn next(&self) -> SubscriptionId {
        SubscriptionId::new(self.0.fetch_add(1, Ordering::Relaxed) + 1)
    }
}

/// Hands out the `req_id` that ties an answer of the exchange to its request.
#[derive(Default)]
pub struct ReqIdGen(AtomicU64);

impl ReqIdGen {
    /// The next request id, a short text.
    pub fn next(&self) -> String {
        format!("r{}", self.0.fetch_add(1, Ordering::Relaxed) + 1)
    }
}

/// Waits until the exchange has answered a subscription: `Ok` when it is open, [`brokapi_core::Error::Rejected`]
/// (`subscription_refused`) when the exchange refused it, [`brokapi_core::Error::Transport`] when nothing came in
/// `limit`.
pub async fn wait_ack<C>(
    core: &tokio::sync::Mutex<C>,
    notify: std::sync::Arc<tokio::sync::Notify>,
    limit: std::time::Duration,
    state: impl Fn(&C) -> (crate::core::SubState, Option<String>),
) -> Result<(), brokapi_core::Error> {
    use crate::core::SubState;
    use brokapi_runtime::AckState;
    brokapi_runtime::wait_ack(core, notify, limit, |c: &C| match state(c) {
        (SubState::Active, _) => AckState::Confirmed,
        (SubState::Failed, reason) => AckState::Refused(reason.unwrap_or_default()),
        (SubState::Pending, _) => AckState::Pending,
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_count_up_from_the_start() {
        let g = SubIdGen::new(1000);
        assert_eq!(g.next().value(), 1001);
        assert_eq!(g.next().value(), 1002);
        let r = ReqIdGen::default();
        assert_eq!((r.next().as_str(), r.next().as_str()), ("r1", "r2"));
    }
}
