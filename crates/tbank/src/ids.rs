//! Instrument identifiers and clocks. Conversions never touch the network.
//!
//! # Examples
//!
//! ```
//! use brokapi_core::{InstrumentId, InstrumentKey, NativeId};
//! use brokapi_tbank::ids::{id_from_parts, instrument_ref};
//!
//! // A broker message names an instrument by uid, figi or ticker and class code.
//! let id = id_from_parts("uid-sber", "BBG004730N88", "SBER", "TQBR");
//! assert_eq!(id.key, InstrumentKey::new("SBER", "TQBR"));
//! // The uid wins: it does not change when a ticker is renamed.
//! assert_eq!(id.native, NativeId::Uid("uid-sber".into()));
//! // The same id as the string a request carries in `instrument_id`.
//! assert_eq!(instrument_ref(&id), "uid-sber");
//! ```

use std::time::Instant;

use brokapi_core::{
    Error, InstrumentId, InstrumentKey, Money, NativeId, Price, Qty, Timestamps, UnixNanos,
};
use brokapi_tbank_protocol::money::{decimal_to_quotation, money_to_decimal, quotation_to_decimal};
use brokapi_tbank_protocol::v1::{self as pb, MoneyValue, Quotation};
use prost_types::Timestamp;

/// Value for the `instrument_id` request fields: `figi`, `instrument_uid` or `ticker_classcode`.
///
/// A `Uid` or `Figi` native id is passed as is. A `TickerClass` (or `SymbolMic`) becomes
/// `TICKER_CLASSCODE`, for example `SBER_TQBR`. No network call is made and the id is not checked
/// against the directory.
///
/// # Examples
///
/// ```
/// use brokapi_core::{InstrumentId, InstrumentKey, NativeId};
/// use brokapi_tbank::ids::instrument_ref;
///
/// let sber = InstrumentId::new(
///     InstrumentKey::new("SBER", "TQBR"),
///     NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() },
/// );
/// assert_eq!(instrument_ref(&sber), "SBER_TQBR");
/// ```
pub fn instrument_ref(id: &InstrumentId) -> String {
    match &id.native {
        NativeId::Uid(u) => u.clone(),
        NativeId::Figi(f) => f.clone(),
        NativeId::TickerClass { ticker, class_code } => format!("{ticker}_{class_code}"),
        NativeId::SymbolMic { symbol, mic } => format!("{symbol}_{mic}"),
        // A kind of id of a later version: the key still names ticker and class.
        _ => format!("{}_{}", id.key.symbol, id.key.mic),
    }
}

/// `true` when a broker message (identified by any of uid/figi/ticker+class) is about `id`.
///
/// A message matches when its ticker and class code equal the key of `id`, or when its uid (for a
/// `Uid` id) or figi (for a `Figi` id) equals the native id. An empty uid or figi in the message never matches.
///
/// # Examples
///
/// ```
/// use brokapi_core::{InstrumentId, InstrumentKey, NativeId};
/// use brokapi_tbank::ids::matches;
///
/// let id = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::Uid("uid-sber".into()));
/// assert!(matches(&id, "uid-sber", "", "", ""));
/// assert!(matches(&id, "", "", "SBER", "TQBR"));
/// assert!(!matches(&id, "uid-other", "", "GAZP", "TQBR"));
/// ```
pub fn matches(id: &InstrumentId, uid: &str, figi: &str, ticker: &str, class_code: &str) -> bool {
    if !ticker.is_empty() && id.key.symbol == ticker && id.key.mic == class_code {
        return true;
    }
    match &id.native {
        NativeId::Uid(u) => !uid.is_empty() && u == uid,
        NativeId::Figi(f) => !figi.is_empty() && f == figi,
        NativeId::TickerClass {
            ticker: t,
            class_code: c,
        } => t == ticker && c == class_code,
        NativeId::SymbolMic { symbol, mic } => symbol == ticker && mic == class_code,
        _ => false,
    }
}

/// Canonical id from the fields a broker message carries. Prefers the uid (stable across tickers).
///
/// The native id is the uid, else the figi, else ticker and class code. When the message has no
/// ticker, the key holds the uid or figi in place of the symbol, until a message with a ticker
/// arrives (the user data core learns the mapping).
///
/// # Examples
///
/// ```
/// use brokapi_core::NativeId;
/// use brokapi_tbank::ids::id_from_parts;
///
/// let id = id_from_parts("", "", "GAZP", "TQBR");
/// assert!(matches!(id.native, NativeId::TickerClass { .. }));
/// ```
pub fn id_from_parts(uid: &str, figi: &str, ticker: &str, class_code: &str) -> InstrumentId {
    let native = if !uid.is_empty() {
        NativeId::Uid(uid.to_string())
    } else if !figi.is_empty() {
        NativeId::Figi(figi.to_string())
    } else {
        NativeId::TickerClass {
            ticker: ticker.to_string(),
            class_code: class_code.to_string(),
        }
    };
    let key = if ticker.is_empty() {
        // Unknown ticker: keep the broker id readable in the key until a message with the ticker arrives.
        InstrumentKey::new(if uid.is_empty() { figi } else { uid }, class_code)
    } else {
        InstrumentKey::new(ticker, class_code)
    };
    InstrumentId::new(key, native)
}

/// Current wall-clock time in nanoseconds since the Unix epoch.
///
/// Returns zero if the system clock is before 1970. It is the local clock: nothing guarantees it
/// matches the broker's clock.
///
/// # Examples
///
/// ```
/// assert!(brokapi_tbank::ids::wall_nanos().as_nanos() > 0);
/// ```
pub fn wall_nanos() -> UnixNanos {
    UnixNanos::from_nanos(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0),
    )
}

/// Converts a protobuf `Timestamp` to nanoseconds since the Unix epoch. Overflow saturates.
///
/// # Examples
///
/// ```
/// use brokapi_tbank::ids::proto_to_nanos;
/// use prost_types::Timestamp;
///
/// let ts = Timestamp { seconds: 1, nanos: 500 };
/// assert_eq!(proto_to_nanos(&ts).as_nanos(), 1_000_000_500);
/// ```
pub fn proto_to_nanos(ts: &Timestamp) -> UnixNanos {
    UnixNanos::from_nanos(
        ts.seconds
            .saturating_mul(1_000_000_000)
            .saturating_add(i64::from(ts.nanos)),
    )
}

/// Converts nanoseconds since the Unix epoch to a protobuf `Timestamp`. Negative times keep a
/// non-negative `nanos` part, as protobuf requires.
///
/// # Examples
///
/// ```
/// use brokapi_core::UnixNanos;
/// use brokapi_tbank::ids::nanos_to_proto;
///
/// let ts = nanos_to_proto(UnixNanos::from_nanos(2_000_000_007));
/// assert_eq!((ts.seconds, ts.nanos), (2, 7));
/// ```
pub fn nanos_to_proto(n: UnixNanos) -> Timestamp {
    let n = n.as_nanos();
    Timestamp {
        seconds: n.div_euclid(1_000_000_000),
        nanos: n.rem_euclid(1_000_000_000) as i32,
    }
}

/// Reception timestamps with the broker time attached when the message has one.
///
/// `now` is the instant the message was received. A missing broker time, or one that is exactly
/// zero, is left out.
///
/// # Examples
///
/// ```
/// use std::time::Instant;
/// use brokapi_tbank::ids::stamps;
/// use prost_types::Timestamp;
///
/// assert!(stamps(Instant::now(), None).broker_ts.is_none());
/// let t = stamps(Instant::now(), Some(&Timestamp { seconds: 5, nanos: 0 }));
/// assert!(t.broker_ts.is_some());
/// ```
pub fn stamps(now: Instant, broker: Option<&Timestamp>) -> Timestamps {
    let t = Timestamps::new(wall_nanos(), now);
    match broker {
        Some(b) if b.seconds != 0 || b.nanos != 0 => t.with_broker_ts(proto_to_nanos(b)),
        _ => t,
    }
}

/// A [`Quotation`] as a [`Price`], exactly (units and nano, no `f64`).
///
/// # Examples
///
/// ```
/// use brokapi_core::Price;
/// use brokapi_tbank::ids::price_of_quotation;
/// use brokapi_tbank_protocol::money::quotation;
///
/// assert_eq!(price_of_quotation(&quotation("244.30")), "244.3".parse::<Price>()?);
/// # Ok::<(), brokapi_core::NumberError>(())
/// ```
pub fn price_of_quotation(q: &Quotation) -> Price {
    Price::new(quotation_to_decimal(q))
}

/// A [`MoneyValue`] as a [`Price`], exactly. The currency is ignored.
pub fn price_of_money(m: &MoneyValue) -> Price {
    Price::new(money_to_decimal(m))
}

/// A [`MoneyValue`] as [`Money`], exactly. The currency is ignored: read it from the message.
pub fn money_of(m: &MoneyValue) -> Money {
    Money::new(money_to_decimal(m))
}

/// A [`Quotation`] as a [`Qty`], exactly.
pub fn qty_of_quotation(q: &Quotation) -> Qty {
    Qty::new(quotation_to_decimal(q))
}

/// A [`Price`] as the [`Quotation`] a request carries, exactly.
///
/// # Errors
///
/// [`Error::Rejected`] with code `invalid_price` when the price has more than 9 decimal places (T-Bank
/// counts in billionths) or does not fit. The price is never rounded in silence.
///
/// # Examples
///
/// ```
/// use brokapi_core::Price;
/// use brokapi_tbank::ids::price_to_quotation;
///
/// let q = price_to_quotation("244.30".parse::<Price>()?)?;
/// assert_eq!((q.units, q.nano), (244, 300_000_000));
/// assert!(price_to_quotation("0.0000000001".parse::<Price>()?).is_err());
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub fn price_to_quotation(price: Price) -> Result<Quotation, Error> {
    decimal_to_quotation(price.decimal()).map_err(|e| Error::Rejected {
        code: "invalid_price".into(),
        message: format!("price {price} cannot be sent: {e}"),
    })
}

/// Refuses an [`InstrumentId`] whose [`NativeId`] is of another broker's kind (`SymbolMic` is the Finam
/// form), before anything is sent.
///
/// # Errors
///
/// [`Error::Config`]: T-Bank takes a uid, a figi, or a ticker with a class code.
///
/// # Examples
///
/// ```
/// use brokapi_core::{Error, InstrumentId, InstrumentKey, NativeId};
/// use brokapi_tbank::ids::require_native;
///
/// let finam = InstrumentId::new(
///     InstrumentKey::new("SBER", "MISX"),
///     NativeId::SymbolMic { symbol: "SBER".into(), mic: "MISX".into() },
/// );
/// assert!(matches!(require_native(&finam), Err(Error::Config(_))));
/// ```
pub fn require_native(id: &InstrumentId) -> Result<(), Error> {
    match id.native {
        NativeId::SymbolMic { .. } => {
            Err(Error::Config(brokapi_core::ConfigError::Message(format!(
                "instrument {} has an id of another broker (symbol and mic): T-Bank takes a uid, a figi, or a ticker with a class code",
                id.key.display()
            ))))
        }
        _ => Ok(()),
    }
}

/// The directory request for an [`InstrumentId`]: by uid, figi, or ticker with class code.
pub fn instrument_request(id: &InstrumentId) -> pb::InstrumentRequest {
    match &id.native {
        NativeId::Uid(u) => pb::InstrumentRequest {
            id_type: pb::InstrumentIdType::Uid as i32,
            id: u.clone(),
            class_code: None,
        },
        NativeId::Figi(f) => pb::InstrumentRequest {
            id_type: pb::InstrumentIdType::Figi as i32,
            id: f.clone(),
            class_code: None,
        },
        NativeId::TickerClass { ticker, class_code } => pb::InstrumentRequest {
            id_type: pb::InstrumentIdType::Ticker as i32,
            id: ticker.clone(),
            class_code: Some(class_code.clone()),
        },
        NativeId::SymbolMic { symbol, mic } => pb::InstrumentRequest {
            id_type: pb::InstrumentIdType::Ticker as i32,
            id: symbol.clone(),
            class_code: Some(mic.clone()),
        },
        _ => pb::InstrumentRequest {
            id_type: pb::InstrumentIdType::Ticker as i32,
            id: id.key.symbol.clone(),
            class_code: Some(id.key.mic.clone()),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sber_uid() -> InstrumentId {
        InstrumentId::new(
            InstrumentKey::new("SBER", "TQBR"),
            NativeId::Uid("e6123145".into()),
        )
    }

    #[test]
    fn ref_per_native_form() {
        assert_eq!(instrument_ref(&sber_uid()), "e6123145");
        let tc = InstrumentId::new(
            InstrumentKey::new("SBER", "TQBR"),
            NativeId::TickerClass {
                ticker: "SBER".into(),
                class_code: "TQBR".into(),
            },
        );
        assert_eq!(instrument_ref(&tc), "SBER_TQBR");
        let f = InstrumentId::new(
            InstrumentKey::new("SBER", "TQBR"),
            NativeId::Figi("BBG004730N88".into()),
        );
        assert_eq!(instrument_ref(&f), "BBG004730N88");
    }

    #[test]
    fn matching_by_uid_figi_or_ticker() {
        let id = sber_uid();
        assert!(matches(&id, "e6123145", "", "", ""));
        assert!(matches(&id, "", "", "SBER", "TQBR"));
        assert!(!matches(&id, "other", "BBG", "GAZP", "TQBR"));
    }

    #[test]
    fn an_id_of_another_broker_is_a_config_error_before_the_network() {
        assert!(require_native(&sber_uid()).is_ok());
        let finam = InstrumentId::new(
            InstrumentKey::new("SBER", "MISX"),
            NativeId::SymbolMic {
                symbol: "SBER".into(),
                mic: "MISX".into(),
            },
        );
        assert!(matches!(require_native(&finam), Err(Error::Config(_))));
    }

    #[test]
    fn instrument_requests_per_native_form() {
        assert_eq!(
            instrument_request(&sber_uid()).id_type,
            pb::InstrumentIdType::Uid as i32
        );
        let f = InstrumentId::new(InstrumentKey::new("S", "T"), NativeId::Figi("F".into()));
        assert_eq!(
            instrument_request(&f).id_type,
            pb::InstrumentIdType::Figi as i32
        );
        let tc = InstrumentId::new(
            InstrumentKey::new("SBER", "TQBR"),
            NativeId::TickerClass {
                ticker: "SBER".into(),
                class_code: "TQBR".into(),
            },
        );
        let r = instrument_request(&tc);
        assert_eq!(
            (r.id.as_str(), r.class_code.as_deref()),
            ("SBER", Some("TQBR"))
        );
        let sm = InstrumentId::new(
            InstrumentKey::new("SBER", "TQBR"),
            NativeId::SymbolMic {
                symbol: "SBER".into(),
                mic: "TQBR".into(),
            },
        );
        assert_eq!(
            instrument_request(&sm).id_type,
            pb::InstrumentIdType::Ticker as i32
        );
    }

    #[test]
    fn proto_time_roundtrip() {
        let n = UnixNanos::from_nanos(1_700_000_000_123_456_789);
        assert_eq!(proto_to_nanos(&nanos_to_proto(n)), n);
    }
}
