//! Connection desired/actual subscription tracking.

use std::collections::HashMap;
use std::time::Instant;

use brokapi_sansio::{DesiredSubscription, SubscriptionSet};

use crate::health::{ConnectionHealth, HealthClass};

/// What the connection knows about one subscription: the "actual" side of desired-versus-actual.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubState {
    /// The subscribe request was sent and no answer has arrived yet.
    Pending {
        /// When the request was sent. The runtime does not time out pending subscriptions by
        /// itself.
        since: Instant,
    },
    /// The broker confirmed the subscription.
    Active {
        /// Sequence number of the last message seen for it, as supplied by the owner.
        seq: u64,
    },
    /// The broker refused the subscription or it broke.
    Failed {
        /// Human-readable cause for logs.
        reason: String,
    },
}

/// The desired and actual subscriptions of one connection, and its health.
///
/// The owner sets what it wants ([`set_desired`](Self::set_desired)) and records what happens on
/// the wire ([`mark_pending`](Self::mark_pending), [`mark_active`](Self::mark_active),
/// [`mark_failed`](Self::mark_failed)). The difference between the two is what
/// [`needs_resubscribe`](Self::needs_resubscribe) and [`needs_unsubscribe`](Self::needs_unsubscribe)
/// report. The struct does no I/O.
///
/// # Examples
///
/// ```
/// use std::time::Instant;
/// use brokapi_core::{InstrumentId, InstrumentKey, NativeId, SubscriptionId};
/// use brokapi_runtime::ConnectionState;
/// use brokapi_sansio::{DesiredSubscription, SubscriptionKind, SubscriptionSet};
///
/// let sber = InstrumentId::new(
///     InstrumentKey::new("SBER", "TQBR"),
///     NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() },
/// );
/// let sub = DesiredSubscription {
///     id: SubscriptionId::new(1),
///     kind: SubscriptionKind::Quotes { instrument: sber },
/// };
///
/// let mut conn = ConnectionState::new(1, Instant::now());
/// conn.set_desired(SubscriptionSet::with_subscriptions(vec![sub.clone()]));
/// // Wanted but not yet on the wire.
/// assert_eq!(conn.needs_resubscribe().len(), 1);
///
/// conn.mark_active(sub.id, 1);
/// assert!(conn.needs_resubscribe().is_empty());
///
/// // The broker drops it: it must be sent again.
/// conn.mark_failed(sub.id, "denied");
/// assert_eq!(conn.needs_resubscribe().len(), 1);
/// ```
#[derive(Debug)]
pub struct ConnectionState {
    /// Connection id, unique inside a [`Supervisor`](crate::Supervisor).
    pub id: u64,
    /// The subscriptions the owner wants on this connection.
    pub desired: SubscriptionSet,
    /// What is known about each subscription on the wire.
    pub actual: HashMap<brokapi_core::SubscriptionId, SubState>,
    /// Receive-side health of the connection.
    pub health: ConnectionHealth,
}

impl ConnectionState {
    /// Creates a connection with nothing desired and nothing active, last heard from at `now`.
    pub fn new(id: u64, now: Instant) -> Self {
        Self {
            id,
            desired: SubscriptionSet::new(),
            actual: HashMap::new(),
            health: ConnectionHealth::new(now),
        }
    }

    /// Replaces the whole desired set. The `actual` map is left as it is.
    pub fn set_desired(&mut self, desired: SubscriptionSet) {
        self.desired = desired;
    }

    /// Records that the subscribe request for `sub` was sent at `now`.
    pub fn mark_pending(&mut self, sub: &DesiredSubscription, now: Instant) {
        self.actual.insert(sub.id, SubState::Pending { since: now });
    }

    /// Records that the broker confirmed `sub_id`, with the sequence number `seq`.
    pub fn mark_active(&mut self, sub_id: brokapi_core::SubscriptionId, seq: u64) {
        self.actual.insert(sub_id, SubState::Active { seq });
    }

    /// Records that `sub_id` was refused or broke, with a cause for logs.
    pub fn mark_failed(&mut self, sub_id: brokapi_core::SubscriptionId, reason: impl Into<String>) {
        self.actual.insert(
            sub_id,
            SubState::Failed {
                reason: reason.into(),
            },
        );
    }

    /// Forgets the actual state of `sub_id`. It does not touch the desired set.
    pub fn remove(&mut self, sub_id: brokapi_core::SubscriptionId) {
        self.actual.remove(&sub_id);
    }

    /// Classifies the connection health, see [`ConnectionHealth::classify`].
    pub fn classify(
        &self,
        now: Instant,
        dead_after: std::time::Duration,
        degraded_after: std::time::Duration,
    ) -> HealthClass {
        self.health.classify(now, dead_after, degraded_after)
    }

    /// Desired subscriptions that are not confirmed active, to be (re)sent without a full reconnect.
    ///
    /// A subscription is listed when it has no state, has failed, or is still pending. Pending ones
    /// are included whatever their age, so call this when you are ready to send again, not on every
    /// tick, or you will resend requests that are merely slow.
    pub fn needs_resubscribe(&self) -> Vec<DesiredSubscription> {
        self.desired
            .subs()
            .iter()
            .filter(|sub| match self.actual.get(&sub.id) {
                None | Some(SubState::Failed { .. }) | Some(SubState::Pending { .. }) => true,
                Some(SubState::Active { .. }) => false,
            })
            .cloned()
            .collect()
    }

    /// Subscriptions that were in `previous` but are no longer desired, to be cancelled.
    pub fn needs_unsubscribe(&self, previous: &SubscriptionSet) -> Vec<DesiredSubscription> {
        previous
            .subs()
            .iter()
            .filter(|sub| self.desired.find(sub.id).is_none())
            .cloned()
            .collect()
    }
}
