//! Mapping of the wire models of the exchange to the broker-independent types.
//!
//! Every function here is pure: no clock but the one passed in, no network.

pub mod execution;
pub mod instrument;
pub mod market;
pub mod order;
pub mod portfolio;
pub mod stop;

use std::time::Instant;

use brokapi_core::{Side, Timestamps, UnixNanos};

/// The three clocks of an event: `now` is the local monotonic one, the wall clock is read here, the time of the
/// exchange is given when the frame has it.
pub fn timestamps(now: Instant, broker_ts: Option<UnixNanos>) -> Timestamps {
    let rx_ts = UnixNanos::from_nanos(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0),
    );
    let mut ts = Timestamps::new(rx_ts, now);
    if let Some(b) = broker_ts {
        ts = ts.with_broker_ts(b);
    }
    ts
}

/// The wall clock now, as nanoseconds since the epoch.
pub fn unix_now_nanos() -> UnixNanos {
    UnixNanos::from_nanos(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0),
    )
}

/// Nanoseconds from milliseconds since the epoch.
pub fn ms_to_nanos(ms: i64) -> UnixNanos {
    UnixNanos::from_nanos(ms.saturating_mul(1_000_000))
}

/// The time of the exchange as nanoseconds, `None` when the exchange gave none (zero).
pub fn broker_ts(ms: i64) -> Option<UnixNanos> {
    (ms > 0).then(|| ms_to_nanos(ms))
}

/// The word of the API for a side.
pub fn side_word(side: Side) -> &'static str {
    match side {
        Side::Buy => "Buy",
        Side::Sell => "Sell",
    }
}

/// The side from the word of the API.
pub fn side_of(raw: &str) -> Option<Side> {
    match raw {
        "Buy" => Some(Side::Buy),
        "Sell" => Some(Side::Sell),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sides_and_times_round_trip() {
        assert_eq!(side_of(side_word(Side::Buy)), Some(Side::Buy));
        assert_eq!(side_of("Sell"), Some(Side::Sell));
        assert_eq!(side_of(""), None);
        assert_eq!(ms_to_nanos(2).as_nanos(), 2_000_000);
        assert_eq!(broker_ts(0), None);
    }
}
