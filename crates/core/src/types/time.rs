//! Time types: [`UnixNanos`] for points in time and [`Timestamps`] for the three clocks of one event.

use std::time::Instant;

use serde::{Deserialize, Serialize};

/// A point in time: nanoseconds since the Unix epoch, UTC, wall clock.
///
/// Used for time ranges ([`TimeRange`](crate::TimeRange)), candle bounds, schedules and the
/// broker time of an event. It is a signed 64-bit count, so it covers years 1677 to 2262. A wall
/// clock can jump, so do not subtract two of them to measure how long something took: use
/// [`Timestamps::rx_mono`].
///
/// # Examples
///
/// ```
/// use brokapi_core::UnixNanos;
///
/// // 2026-10-19 00:00:00 UTC
/// let t = UnixNanos::from_nanos(1_792_368_000_000_000_000);
/// assert_eq!(t.as_nanos() / 1_000_000_000, 1_792_368_000);
/// assert!(t > UnixNanos::ZERO);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct UnixNanos(pub i64);

impl UnixNanos {
    /// The epoch itself, 1970-01-01 00:00:00 UTC. Handy as the start of "all history".
    pub const ZERO: Self = Self(0);

    /// Wraps a raw number of nanoseconds since the epoch. Nothing is checked.
    pub fn from_nanos(nanos: i64) -> Self {
        Self(nanos)
    }

    /// The raw number of nanoseconds since the epoch.
    pub fn as_nanos(self) -> i64 {
        self.0
    }
}

/// The clocks of one event: what the broker said and when this process got it.
///
/// Every domain object ([`OrderBook`](crate::OrderBook), [`OrderSnapshot`](crate::OrderSnapshot),
/// [`Execution`](crate::Execution) and so on) and every [`Envelope`](crate::Envelope) carries one.
/// Equality compares `broker_ts` and `rx_ts` only. The monotonic clock is not part of it and is not
/// serialized: after a round trip through serde it is the time of deserialization.
///
/// # Examples
///
/// ```
/// use std::time::Instant;
/// use brokapi_core::{Timestamps, UnixNanos};
///
/// let ts = Timestamps::new(UnixNanos::from_nanos(1_000), Instant::now())
///     .with_broker_ts(UnixNanos::from_nanos(900));
/// assert_eq!(ts.broker_ts, Some(UnixNanos::from_nanos(900)));
/// ```
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Timestamps {
    /// Time the broker put on the event, `None` when it gave none. May be wrong, may go backwards.
    /// Never use it to measure latency or timeouts.
    pub broker_ts: Option<UnixNanos>,
    /// Wall clock of this process when the event was received. For logs and for matching with other
    /// systems. It can jump (clock sync), so it is not for measuring durations.
    pub rx_ts: UnixNanos,
    /// Monotonic clock of this process when the event was received. The only valid source for
    /// latency and timeout arithmetic. Not serialized.
    #[serde(skip, default = "Instant::now")]
    pub rx_mono: Instant,
}

impl PartialEq for Timestamps {
    fn eq(&self, other: &Self) -> bool {
        self.broker_ts == other.broker_ts && self.rx_ts == other.rx_ts
    }
}

impl Timestamps {
    /// Timestamps of an event received now: no broker time yet, see [`with_broker_ts`](Self::with_broker_ts).
    pub fn new(rx_ts: UnixNanos, rx_mono: Instant) -> Self {
        Self {
            broker_ts: None,
            rx_ts,
            rx_mono,
        }
    }

    /// Sets the time the broker put on the event.
    pub fn with_broker_ts(mut self, broker_ts: UnixNanos) -> Self {
        self.broker_ts = Some(broker_ts);
        self
    }
}
