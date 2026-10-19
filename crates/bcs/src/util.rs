//! Small helpers shared by the two connectors: instrument ids, timeframes and timestamps.

use brokapi_bcs_protocol::models::instrument::InstrumentKey;
use brokapi_core::{InstrumentId, NativeId, Timeframe, UnixNanos};
use chrono::{DateTime, TimeZone, Utc};

use crate::map::bcs_instrument_key;

/// Returns the BCS instrument key (ticker and class code) of an instrument id.
///
/// BCS names an instrument by ticker plus trading board (class code), for example `SBER` on `TQBR`.
///
/// # Errors
///
/// [`brokapi_core::Error::Config`] when the native id of `id` is not [`NativeId::TickerClass`]. An id
/// that came from another broker cannot be used here.
///
/// # Examples
///
/// ```
/// use brokapi_bcs::util::require_ticker_class;
/// use brokapi_core::{InstrumentId, InstrumentKey, NativeId};
///
/// let sber = InstrumentId::new(
///     InstrumentKey::new("SBER", "TQBR"),
///     NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() },
/// );
/// assert_eq!(require_ticker_class(&sber).unwrap().class_code, "TQBR");
///
/// let foreign = InstrumentId::new(InstrumentKey::new("SBER", "MISX"), NativeId::Uid("uid".into()));
/// assert!(require_ticker_class(&foreign).is_err());
/// ```
pub fn require_ticker_class(id: &InstrumentId) -> Result<InstrumentKey, brokapi_core::Error> {
    bcs_instrument_key(id).ok_or_else(|| {
        brokapi_core::Error::Config(brokapi_core::ConfigError::Message(
            "instrument native id must be TickerClass for BCS".into(),
        ))
    })
}

/// The label of a timeframe in BCS requests: `M1 M5 M15 M30 H1 H4 D W MN` (docs, checked 2026-10-19).
/// `M10` is not offered by BCS: `None`, the caller answers `Unsupported`.
pub fn timeframe_label(tf: Timeframe) -> Option<&'static str> {
    Some(match tf {
        Timeframe::M1 => "M1",
        Timeframe::M5 => "M5",
        Timeframe::M10 => return None,
        Timeframe::M15 => "M15",
        Timeframe::M30 => "M30",
        Timeframe::H1 => "H1",
        Timeframe::H4 => "H4",
        Timeframe::D1 => "D",
        Timeframe::W1 => "W",
        Timeframe::MN => "MN",
    })
}

/// Converts a time in nanoseconds since the Unix epoch to a UTC date and time.
///
/// Sub-second precision is kept. A value that is out of the range of dates falls back to the current
/// time, so this function never fails (and never panics).
///
/// # Examples
///
/// ```
/// use brokapi_bcs::util::nanos_to_datetime;
/// use brokapi_core::UnixNanos;
///
/// let dt = nanos_to_datetime(UnixNanos::from_nanos(1_000_000_000));
/// assert_eq!(dt.timestamp(), 1);
/// ```
pub fn nanos_to_datetime(nanos: UnixNanos) -> DateTime<Utc> {
    let secs = nanos.as_nanos() / 1_000_000_000;
    let nsec = (nanos.as_nanos() % 1_000_000_000) as u32;
    match Utc.timestamp_opt(secs, nsec) {
        chrono::LocalResult::Single(dt) => dt,
        chrono::LocalResult::Ambiguous(dt, _) => dt,
        chrono::LocalResult::None => Utc::now(),
    }
}

/// Returns the class code (trading board, for example `TQBR`) of an instrument id.
///
/// # Errors
///
/// [`brokapi_core::Error::Config`] when the native id is not [`NativeId::TickerClass`], as in
/// [`require_ticker_class`].
///
/// # Examples
///
/// ```
/// use brokapi_bcs::util::class_code;
/// use brokapi_core::{InstrumentId, InstrumentKey, NativeId};
///
/// let sber = InstrumentId::new(
///     InstrumentKey::new("SBER", "TQBR"),
///     NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() },
/// );
/// assert_eq!(class_code(&sber).unwrap(), "TQBR");
/// ```
pub fn class_code(id: &InstrumentId) -> Result<String, brokapi_core::Error> {
    match &id.native {
        NativeId::TickerClass { class_code, .. } => Ok(class_code.clone()),
        _ => require_ticker_class(id).map(|k| k.class_code),
    }
}

/// Returns the ticker (for example `SBER`) of an instrument id.
///
/// # Errors
///
/// [`brokapi_core::Error::Config`] when the native id is not [`NativeId::TickerClass`], as in
/// [`require_ticker_class`].
///
/// # Examples
///
/// ```
/// use brokapi_bcs::util::ticker;
/// use brokapi_core::{InstrumentId, InstrumentKey, NativeId};
///
/// let sber = InstrumentId::new(
///     InstrumentKey::new("SBER", "TQBR"),
///     NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() },
/// );
/// assert_eq!(ticker(&sber).unwrap(), "SBER");
/// ```
pub fn ticker(id: &InstrumentId) -> Result<String, brokapi_core::Error> {
    match &id.native {
        NativeId::TickerClass { ticker, .. } => Ok(ticker.clone()),
        _ => require_ticker_class(id).map(|k| k.ticker),
    }
}
