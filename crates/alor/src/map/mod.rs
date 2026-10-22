//! Mapping of ALOR wire models to the domain types of `brokapi-core`.

pub mod execution;
pub mod instrument;
pub mod market;
pub mod order;
pub mod portfolio;
pub mod stop;

use std::time::Instant;

use brokapi_core::{Decimal, Error, Qty, Timestamps, UnixNanos};
use chrono::{DateTime, Utc};

/// The timestamps of an event: the time we received it and, if the broker gave one, the time of the broker.
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

/// A time of the broker (UTC text such as `2020-06-16T23:59:59.9990000Z`).
pub fn parse_time(raw: Option<&str>) -> Option<UnixNanos> {
    let s = raw?;
    DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| unix_nanos(dt.with_timezone(&Utc)))
}

/// A date as [`UnixNanos`].
pub fn unix_nanos(dt: DateTime<Utc>) -> UnixNanos {
    UnixNanos::from_nanos(dt.timestamp_nanos_opt().unwrap_or(0))
}

/// Unix seconds as [`UnixNanos`].
pub fn secs_to_nanos(secs: i64) -> UnixNanos {
    UnixNanos::from_nanos(secs.saturating_mul(1_000_000_000))
}

/// Unix milliseconds as [`UnixNanos`].
pub fn ms_to_nanos(ms: i64) -> UnixNanos {
    UnixNanos::from_nanos(ms.saturating_mul(1_000_000))
}

/// The number of lots in a quantity of pieces: a positive whole number of lots, else `invalid_qty` (the library
/// never rounds a quantity).
pub fn pieces_to_lots(qty: Qty, lot: Decimal) -> Result<i64, Error> {
    let bad = |why: String| Error::Rejected {
        code: "invalid_qty".into(),
        message: why,
    };
    if !lot.is_sign_positive() || lot.is_zero() {
        return Err(bad(format!("the lot size {lot} is not usable")));
    }
    let lots = qty.decimal() / lot;
    if lots.fract().is_zero() && lots >= Decimal::ONE {
        use rust_decimal::prelude::ToPrimitive;
        return lots
            .to_i64()
            .ok_or_else(|| bad(format!("quantity {qty} is too large")));
    }
    Err(bad(format!(
        "quantity {qty} is not a positive whole number of lots of {lot}"
    )))
}

/// Lots to pieces.
pub fn lots_to_pieces(lots: Decimal, lot: Decimal) -> Qty {
    Qty::new(lots * lot)
}

#[cfg(test)]
mod tests {
    use super::*;
    use brokapi_core::dec;

    #[test]
    fn pieces_become_whole_lots_or_are_refused() {
        assert_eq!(pieces_to_lots(Qty::from(30), dec!(10)).unwrap(), 3);
        assert_eq!(pieces_to_lots(Qty::from(1), dec!(1)).unwrap(), 1);
        for q in ["0", "5", "15", "0.5"] {
            assert!(
                matches!(pieces_to_lots(q.parse().unwrap(), dec!(10)), Err(Error::Rejected { ref code, .. }) if code == "invalid_qty"),
                "{q}"
            );
        }
        assert!(pieces_to_lots(Qty::from(10), Decimal::ZERO).is_err());
    }

    #[test]
    fn times_of_the_broker_are_read_with_seven_digits() {
        let t = parse_time(Some("2020-06-16T23:59:59.9990000Z")).unwrap();
        assert_eq!(t.as_nanos() % 1_000_000_000, 999_000_000);
        assert!(parse_time(Some("nonsense")).is_none());
        assert_eq!(secs_to_nanos(2).as_nanos(), 2_000_000_000);
        assert_eq!(ms_to_nanos(2).as_nanos(), 2_000_000);
    }
}
