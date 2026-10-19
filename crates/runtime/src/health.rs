//! Connection health classification.

use std::time::{Duration, Instant};

/// How healthy a connection looks, judged by how long it has been silent.
///
/// The [`Supervisor`](crate::Supervisor) maps each class to an action.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthClass {
    /// Data arrived recently. Nothing to do.
    Healthy,
    /// The connection has been quiet for longer than the degraded threshold but not yet long
    /// enough to be declared dead. Missing subscriptions should be retried.
    Degraded,
    /// Quiet for at least the dead threshold. The connection should be replaced.
    Dead,
}

/// Receive-side health signals of one connection.
///
/// # Examples
///
/// ```
/// use std::time::{Duration, Instant};
/// use brokapi_runtime::{ConnectionHealth, HealthClass};
///
/// let t0 = Instant::now();
/// let mut health = ConnectionHealth::new(t0);
/// let (degraded_after, dead_after) = (Duration::from_secs(15), Duration::from_secs(60));
///
/// assert_eq!(health.classify(t0 + Duration::from_secs(5), dead_after, degraded_after), HealthClass::Healthy);
/// assert_eq!(health.classify(t0 + Duration::from_secs(20), dead_after, degraded_after), HealthClass::Degraded);
/// assert_eq!(health.classify(t0 + Duration::from_secs(60), dead_after, degraded_after), HealthClass::Dead);
///
/// // A frame arrives: the silence timer restarts.
/// health.on_rx(t0 + Duration::from_secs(61));
/// assert_eq!(health.classify(t0 + Duration::from_secs(62), dead_after, degraded_after), HealthClass::Healthy);
/// ```
#[derive(Debug, Clone)]
pub struct ConnectionHealth {
    /// When the last frame arrived. Starts at the creation time.
    pub last_rx: Instant,
    /// Round-trip time of the last ping, if the owner measures it. The runtime does not fill it.
    pub rtt: Option<Duration>,
    /// Number of sequence gaps seen on this connection. The runtime does not count them by itself.
    pub seq_gaps: u64,
}

impl ConnectionHealth {
    /// Creates the health record of a connection that is considered just heard from at `now`.
    pub fn new(now: Instant) -> Self {
        Self {
            last_rx: now,
            rtt: None,
            seq_gaps: 0,
        }
    }

    /// Records that a frame arrived at `now`.
    pub fn on_rx(&mut self, now: Instant) {
        self.last_rx = now;
    }

    /// Classifies the connection by the time since the last frame.
    ///
    /// Silence of `dead_after` or more gives [`HealthClass::Dead`]. Silence of `degraded_after` or
    /// more gives [`HealthClass::Degraded`]. Otherwise [`HealthClass::Healthy`]. The caller should
    /// pass `degraded_after < dead_after`, otherwise a connection is never reported as degraded.
    /// A `now` earlier than `last_rx` counts as no silence.
    pub fn classify(
        &self,
        now: Instant,
        dead_after: Duration,
        degraded_after: Duration,
    ) -> HealthClass {
        let silent = now.duration_since(self.last_rx);
        if silent >= dead_after {
            HealthClass::Dead
        } else if silent >= degraded_after {
            HealthClass::Degraded
        } else {
            HealthClass::Healthy
        }
    }
}
