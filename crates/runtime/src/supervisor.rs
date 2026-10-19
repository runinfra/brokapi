//! Supervisor reconcile tick.

use std::time::{Duration, Instant};

use brokapi_sansio::SubscriptionSet;

use crate::connection::ConnectionState;
use crate::health::HealthClass;

/// What the owner should do for one connection after a [`Supervisor::reconcile`] pass.
///
/// The supervisor only names the action. The owner performs it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReconcileAction {
    /// The connection is healthy. Make the wire match the desired set: send what
    /// [`Supervisor::resubscribe_targets`] lists and cancel what was removed.
    ApplyDesired,
    /// The connection has been quiet for a while. Re-send the subscriptions that are missing,
    /// without a reconnect.
    ResubscribeMissing,
    /// The connection is dead. Close it and open a new one, then apply the desired set.
    ScheduleReconnect,
}

/// Connection supervisor without I/O: it compares desired and actual state and says what to do.
///
/// Call [`reconcile`](Self::reconcile) on a timer (every [`reconcile_interval`](Self::reconcile_interval)).
/// It returns one [`ReconcileAction`] per connection and changes nothing on the wire. The decision
/// is made by the connection's silence: see [`HealthClass`].
///
/// A new supervisor holds one connection with id `1`.
///
/// # Examples
///
/// ```
/// use std::time::{Duration, Instant};
/// use brokapi_core::{InstrumentId, InstrumentKey, NativeId, SubscriptionId};
/// use brokapi_runtime::{ReconcileAction, Supervisor};
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
/// let t0 = Instant::now();
/// let mut sup = Supervisor::new(t0);
/// sup.update_desired(1, SubscriptionSet::with_subscriptions(vec![sub.clone()]));
///
/// // Fresh connection: apply the desired set. One subscription is missing on the wire.
/// assert_eq!(sup.reconcile(t0), vec![(1, ReconcileAction::ApplyDesired)]);
/// assert_eq!(sup.resubscribe_targets(1), vec![sub]);
///
/// // Silent for longer than `dead_after` (60 s by default): replace the connection.
/// let later = t0 + Duration::from_secs(61);
/// assert_eq!(sup.reconcile(later), vec![(1, ReconcileAction::ScheduleReconnect)]);
/// ```
#[derive(Debug)]
pub struct Supervisor {
    /// How often the owner should call [`reconcile`](Self::reconcile). Default 1 second. The
    /// supervisor does not read it itself.
    pub reconcile_interval: Duration,
    /// Silence after which a connection is [`HealthClass::Dead`]. Default 60 seconds.
    pub dead_after: Duration,
    /// Silence after which a connection is [`HealthClass::Degraded`]. Default 15 seconds.
    pub degraded_after: Duration,
    /// The supervised connections.
    pub connections: Vec<ConnectionState>,
}

impl Supervisor {
    /// Creates a supervisor with one connection (id `1`) last heard from at `now`, and the default
    /// timings listed on the fields.
    pub fn new(now: Instant) -> Self {
        Self {
            reconcile_interval: Duration::from_secs(1),
            dead_after: Duration::from_secs(60),
            degraded_after: Duration::from_secs(15),
            connections: vec![ConnectionState::new(1, now)],
        }
    }

    /// The connection with this id, if there is one.
    pub fn connection_mut(&mut self, id: u64) -> Option<&mut ConnectionState> {
        self.connections.iter_mut().find(|c| c.id == id)
    }

    /// Decides what each connection needs at `now`. Pure: the connections are not changed.
    ///
    /// The silence of the connection picks the action: healthy gives
    /// [`ReconcileAction::ApplyDesired`], degraded gives [`ReconcileAction::ResubscribeMissing`], dead
    /// gives [`ReconcileAction::ScheduleReconnect`]. One entry per connection, in connection order.
    pub fn reconcile(&mut self, now: Instant) -> Vec<(u64, ReconcileAction)> {
        let mut actions = Vec::new();
        for conn in &mut self.connections {
            let health = conn.classify(now, self.dead_after, self.degraded_after);
            let action = match health {
                HealthClass::Healthy => ReconcileAction::ApplyDesired,
                HealthClass::Degraded => ReconcileAction::ResubscribeMissing,
                HealthClass::Dead => ReconcileAction::ScheduleReconnect,
            };
            actions.push((conn.id, action));
        }
        actions
    }

    /// Sets the desired subscriptions of a connection. An unknown `conn_id` is ignored.
    pub fn update_desired(&mut self, conn_id: u64, desired: SubscriptionSet) {
        if let Some(conn) = self.connection_mut(conn_id) {
            conn.set_desired(desired);
        }
    }

    /// The subscriptions of a connection that must be (re)sent, see
    /// [`ConnectionState::needs_resubscribe`]. Empty for an unknown `conn_id`.
    pub fn resubscribe_targets(&self, conn_id: u64) -> Vec<brokapi_sansio::DesiredSubscription> {
        self.connections
            .iter()
            .find(|c| c.id == conn_id)
            .map(ConnectionState::needs_resubscribe)
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use brokapi_core::{InstrumentId, InstrumentKey, NativeId, SubscriptionId};
    use brokapi_sansio::{DesiredSubscription, SubscriptionKind};

    fn sber() -> InstrumentId {
        InstrumentId::new(
            InstrumentKey::new("SBER", "TQBR"),
            NativeId::TickerClass {
                ticker: "SBER".into(),
                class_code: "TQBR".into(),
            },
        )
    }

    #[test]
    fn failed_subscription_resubscribed_without_reconnect() {
        let now = Instant::now();
        let mut sup = Supervisor::new(now);
        let sub = DesiredSubscription {
            id: SubscriptionId::new(1),
            kind: SubscriptionKind::Quotes { instrument: sber() },
        };
        sup.update_desired(1, SubscriptionSet::with_subscriptions(vec![sub.clone()]));
        sup.connection_mut(1).unwrap().mark_failed(sub.id, "denied");
        let targets = sup.resubscribe_targets(1);
        assert_eq!(targets.len(), 1);
        let actions = sup.reconcile(now);
        assert!(
            actions
                .iter()
                .any(|(_, a)| *a == ReconcileAction::ApplyDesired)
        );
    }
}
