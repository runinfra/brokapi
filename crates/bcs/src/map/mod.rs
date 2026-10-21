//! Mappers from the BCS wire models to the brokapi-core domain types.
//!
//! The functions are pure: they take a model that the protocol crate parsed and the current monotonic
//! time, and they return a domain value. They never fail loudly. A record that cannot be mapped
//! safely is dropped (`None` or an empty list), not guessed. Where the documentation of the API leaves
//! a choice open, the function says which choice it makes.
//!
//! The connectors call these functions. You need them only to read raw data from [`BcsRaw`](crate::BcsRaw)
//! in the same shape as the shared traits give it.

pub mod execution;
pub mod instrument;
pub mod market;
pub mod order;
pub mod portfolio;
pub mod schedule;
pub mod stop;

use std::time::Instant;

use brokapi_bcs_protocol::models::instrument::InstrumentKey as BcsInstrumentKey;
use brokapi_core::{InstrumentId, InstrumentKey, NativeId, Timestamps, UnixNanos};
use chrono::{DateTime, Utc};

/// The order quantity as the whole number of pieces the BCS order API takes.
///
/// BCS counts an order in pieces (`orderQuantity`, "Количество в заявке (шт.)", an integer of at
/// least 1), and so does [`Qty`](brokapi_core::Qty) everywhere in this library: the number is passed
/// on as it is. A quantity that is not a whole number of pieces is refused, never rounded: a rounded
/// order would buy something the caller did not ask for.
///
/// # Errors
///
/// [`brokapi_core::Error::Rejected`] with code `invalid_qty` for a fraction, zero, a negative number or
/// a number that does not fit in an `i64`.
///
/// # Examples
///
/// ```
/// use brokapi_bcs::map::whole_pieces;
/// use brokapi_core::Qty;
///
/// assert_eq!(whole_pieces(Qty::from(10))?, 10);
/// assert_eq!(whole_pieces("10.0".parse()?)?, 10);
/// // 2.5 is not rounded to 3: it is refused.
/// assert!(whole_pieces("2.5".parse()?).is_err());
/// assert!(whole_pieces("0.4".parse()?).is_err());
/// assert!(whole_pieces(Qty::ZERO).is_err());
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub fn whole_pieces(qty: brokapi_core::Qty) -> Result<i64, brokapi_core::Error> {
    match qty.to_whole() {
        Some(n) if n >= 1 => Ok(n),
        _ => Err(brokapi_core::Error::Rejected {
            code: "invalid_qty".into(),
            message: format!("quantity {qty} is not a positive whole number of pieces"),
        }),
    }
}

/// Builds an [`InstrumentId`] from a BCS instrument key. The native id is
/// [`NativeId::TickerClass`], the form every BCS call wants.
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::models::instrument::InstrumentKey;
/// use brokapi_bcs::map::instrument_id;
/// use brokapi_core::NativeId;
///
/// let id = instrument_id(&InstrumentKey::new("SBER", "TQBR"));
/// assert!(matches!(id.native, NativeId::TickerClass { .. }));
/// ```
pub fn instrument_id(key: &BcsInstrumentKey) -> InstrumentId {
    InstrumentId::new(
        InstrumentKey::new(&key.ticker, &key.class_code),
        NativeId::TickerClass {
            ticker: key.ticker.clone(),
            class_code: key.class_code.clone(),
        },
    )
}

/// Builds an [`InstrumentId`] from a ticker and a class code (board). Same as [`instrument_id`].
pub fn instrument_id_ticker(ticker: &str, class_code: &str) -> InstrumentId {
    instrument_id(&BcsInstrumentKey::new(ticker, class_code))
}

/// Builds the [`Timestamps`] of an event: the local receive time now (wall clock), the monotonic
/// `now` of the receive, and the broker's own time when it is known.
///
/// The broker time is optional because not every BCS message carries one.
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

/// Parses a time string of the BCS API into nanoseconds since the Unix epoch.
///
/// Reads RFC 3339 (`2026-10-19T10:00:00.123+03:00`) and the form `2026-10-19T10:00:00.123Z`. `None`
/// when the input is `None` or in neither form. A time with no zone is not accepted, because its zone
/// would be a guess.
///
/// # Examples
///
/// ```
/// use brokapi_bcs::map::parse_broker_ts;
///
/// let t = parse_broker_ts(Some("2026-10-19T10:00:00.500Z")).expect("a valid time");
/// assert_eq!(t.as_nanos() % 1_000_000_000, 500_000_000);
/// assert!(parse_broker_ts(Some("yesterday")).is_none());
/// assert!(parse_broker_ts(None).is_none());
/// ```
pub fn parse_broker_ts(raw: Option<&str>) -> Option<UnixNanos> {
    let s = raw?;
    DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| unix_nanos(dt.with_timezone(&Utc)))
        .or_else(|| {
            chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.3fZ")
                .ok()
                .map(|ndt| unix_nanos(DateTime::<Utc>::from_naive_utc_and_offset(ndt, Utc)))
        })
}

/// Converts a UTC date and time to nanoseconds since the Unix epoch. A date outside the range that
/// fits in 64 bits (years 1677 to 2262) gives `0`.
pub fn unix_nanos(dt: DateTime<Utc>) -> UnixNanos {
    UnixNanos::from_nanos(dt.timestamp_nanos_opt().unwrap_or(0))
}

/// Returns the BCS instrument key of an [`InstrumentId`], or `None` when its native id is not
/// [`NativeId::TickerClass`] (the id came from another broker).
///
/// # Examples
///
/// ```
/// use brokapi_bcs::map::{bcs_instrument_key, instrument_id_ticker};
///
/// let id = instrument_id_ticker("SBER", "TQBR");
/// assert_eq!(bcs_instrument_key(&id).unwrap().ticker, "SBER");
/// ```
pub fn bcs_instrument_key(id: &InstrumentId) -> Option<BcsInstrumentKey> {
    match &id.native {
        NativeId::TickerClass { ticker, class_code } => {
            Some(BcsInstrumentKey::new(ticker.clone(), class_code.clone()))
        }
        _ => None,
    }
}
