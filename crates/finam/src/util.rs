//! Shared helpers: instrument ids, timestamps, decimals.

use std::str::FromStr;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use brokapi_core::{
    ConfigError, Error, InstrumentId, InstrumentKey, NativeId, Timestamps, UnixNanos,
};
use brokapi_core::{Decimal as Dec, Money as BrokerMoney, Price, Qty};
use brokapi_finam_protocol::pb::google::r#type::{Decimal, Money};

/// `SBER@MISX` for the Finam API. Needs `NativeId::SymbolMic`, no network.
pub fn symbol_of(id: &InstrumentId) -> Result<String, Error> {
    match &id.native {
        NativeId::SymbolMic { symbol, mic } => Ok(format!("{symbol}@{mic}")),
        _ => Err(Error::Config(ConfigError::Message(
            "instrument native id must be SymbolMic for Finam".into(),
        ))),
    }
}

/// Instrument id from a Finam symbol (`SBER@MISX`). A symbol without `@` gets an empty MIC.
pub fn instrument_from_symbol(symbol: &str) -> InstrumentId {
    let (sym, mic) = symbol.rsplit_once('@').unwrap_or((symbol, ""));
    InstrumentId::new(
        InstrumentKey::new(sym, mic),
        NativeId::SymbolMic {
            symbol: sym.to_string(),
            mic: mic.to_string(),
        },
    )
}

/// Placeholder instrument for subscriptions that are not about one instrument (orders, account).
pub fn no_instrument() -> InstrumentId {
    instrument_from_symbol("")
}

/// Wall clock now, in nanoseconds since the Unix epoch. Gives 0 when the clock is before the epoch.
///
/// # Examples
///
/// ```
/// let now = brokapi_finam::util::now_nanos();
/// assert!(now.as_nanos() > 1_700_000_000_000_000_000);
/// ```
pub fn now_nanos() -> UnixNanos {
    UnixNanos::from_nanos(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0),
    )
}

/// Protobuf timestamp -> Unix nanoseconds. Saturates instead of overflowing on absurd values.
pub fn ts_nanos(ts: &prost_types::Timestamp) -> UnixNanos {
    UnixNanos::from_nanos(
        ts.seconds
            .saturating_mul(1_000_000_000)
            .saturating_add(ts.nanos as i64),
    )
}

/// Receive timestamps with the broker time attached when present.
pub fn timestamps(broker: Option<&prost_types::Timestamp>, rx_mono: Instant) -> Timestamps {
    let t = Timestamps::new(now_nanos(), rx_mono);
    match broker {
        Some(b) if b.seconds != 0 || b.nanos != 0 => t.with_broker_ts(ts_nanos(b)),
        _ => t,
    }
}

/// Unix nanoseconds -> protobuf timestamp. Negative values give `seconds < 0` and `nanos >= 0`, the
/// protobuf way.
///
/// # Examples
///
/// ```
/// use brokapi_core::UnixNanos;
/// use brokapi_finam::util::{to_timestamp, ts_nanos};
///
/// let t = to_timestamp(UnixNanos::from_nanos(1_700_000_000_123_456_789));
/// assert_eq!((t.seconds, t.nanos), (1_700_000_000, 123_456_789));
/// assert_eq!(ts_nanos(&t).as_nanos(), 1_700_000_000_123_456_789);
/// ```
pub fn to_timestamp(n: UnixNanos) -> prost_types::Timestamp {
    let n = n.as_nanos();
    prost_types::Timestamp {
        seconds: n.div_euclid(1_000_000_000),
        nanos: n.rem_euclid(1_000_000_000) as i32,
    }
}

/// `google.type.Decimal` -> [`Dec`], exactly: the text is parsed as it is, no `f64` on the way.
/// Empty, absent or unparseable -> `None`. The grammar of the message allows a sign, a leading or
/// trailing point and an exponent.
pub fn dec(d: &Option<Decimal>) -> Option<Dec> {
    let s = d.as_ref()?.value.trim();
    if s.is_empty() {
        return None;
    }
    let s = s.strip_prefix('+').unwrap_or(s);
    Dec::from_str(s).or_else(|_| Dec::from_scientific(s)).ok()
}

/// Like [`dec`] but 0 for absent.
pub fn dec0(d: &Option<Decimal>) -> Dec {
    dec(d).unwrap_or(Dec::ZERO)
}

/// [`dec`] as a [`Price`].
pub fn price(d: &Option<Decimal>) -> Option<Price> {
    dec(d).map(Price::new)
}

/// [`dec0`] as a [`Price`].
pub fn price0(d: &Option<Decimal>) -> Price {
    Price::new(dec0(d))
}

/// [`dec`] as a [`Qty`].
pub fn qty(d: &Option<Decimal>) -> Option<Qty> {
    dec(d).map(Qty::new)
}

/// [`dec0`] as a [`Qty`].
pub fn qty0(d: &Option<Decimal>) -> Qty {
    Qty::new(dec0(d))
}

/// A number -> `google.type.Decimal`, exactly. The text is plain (Rust never writes an exponent for
/// a decimal), the scale is kept: `244.30` is sent as `"244.30"`.
pub fn to_decimal(v: Dec) -> Decimal {
    Decimal {
        value: v.to_string(),
    }
}

/// `google.type.Money` -> (currency, amount), exactly. `units` and `nanos` carry the sign of the value.
pub fn money(m: &Money) -> (String, BrokerMoney) {
    (
        m.currency_code.clone(),
        BrokerMoney::new((Dec::from(m.units) + Dec::new(i64::from(m.nanos), 9)).normalize()),
    )
}

/// (currency, amount) -> `google.type.Money`, exactly. The reverse of [`money`].
///
/// `None` when the amount has more than 9 decimal places (`nanos` counts billionths) or its integer
/// part does not fit in an `i64`: the amount is never rounded in silence. `units` and `nanos` carry the
/// sign of the amount.
pub fn to_money(currency: &str, amount: BrokerMoney) -> Option<Money> {
    let v = amount.decimal().normalize();
    if v.scale() > 9 {
        return None;
    }
    let units = i64::try_from(v.trunc()).ok()?;
    let mut frac = v.fract();
    frac.rescale(9);
    let nanos = i32::try_from(frac.mantissa()).ok()?;
    Some(Money {
        currency_code: currency.to_string(),
        units,
        nanos,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        // broker -> type -> broker: the text of a Decimal comes back as it was.
        #[test]
        fn decimal_text_round_trips(m in -1_000_000_000_000_000_000_i64..1_000_000_000_000_000_000, scale in 0u32..=12) {
            let text = Dec::new(m, scale).to_string();
            let back = to_decimal(dec(&Some(Decimal { value: text.clone() })).unwrap());
            prop_assert_eq!(back.value, text);
        }

        // broker -> type -> broker for Money: units and nanos come back as they were.
        #[test]
        fn money_round_trips(units in -1_000_000_000_000_i64..1_000_000_000_000, nanos in 0i32..1_000_000_000, neg in any::<bool>()) {
            let (units, nanos) = if neg { (-units.abs(), -nanos) } else { (units.abs(), nanos) };
            let wire = Money { currency_code: "RUB".into(), units, nanos };
            let (ccy, amount) = money(&wire);
            prop_assert_eq!(to_money(&ccy, amount), Some(wire));
        }
    }

    #[test]
    fn money_with_a_tenth_of_a_nano_is_refused() {
        assert_eq!(to_money("RUB", BrokerMoney::new(Dec::new(1, 10))), None);
    }

    #[test]
    fn decimal_grammar() {
        let d = |s: &str| Some(Decimal { value: s.into() });
        let n = |s: &str| Some(s.parse::<Dec>().unwrap());
        assert_eq!(dec(&d("123.45")), n("123.45"));
        assert_eq!(dec(&d("+2.5")), n("2.5"));
        assert_eq!(dec(&d("-0.001")), n("-0.001"));
        assert_eq!(dec(&d(".5")), n("0.5"));
        assert_eq!(dec(&d("2.5e3")), n("2500"));
        assert_eq!(dec(&d("1E-2")), n("0.01"));
        assert_eq!(dec(&d("")), None);
        assert_eq!(dec(&d("abc")), None);
        assert_eq!(dec(&d("NaN")), None);
        assert_eq!(dec(&None), None);
    }

    #[test]
    fn decimal_keeps_the_written_form() {
        let d = |s: &str| Some(Decimal { value: s.into() });
        assert_eq!(dec(&d("244.30")).unwrap().to_string(), "244.30");
        assert_eq!(dec(&d("0.0001")).unwrap().to_string(), "0.0001");
        assert_eq!(to_decimal(dec(&d("244.30")).unwrap()).value, "244.30");
    }

    #[test]
    fn decimal_text_has_no_exponent() {
        for s in [
            "0",
            "1",
            "150.5",
            "0.0000001",
            "1000000000000000",
            "123456.789",
            "-4.2",
        ] {
            let v: Dec = s.parse().unwrap();
            let out = to_decimal(v).value;
            assert!(!out.contains('e') && !out.contains('E'), "{out}");
            assert_eq!(out, s);
        }
    }

    #[test]
    fn money_units_and_nanos() {
        let m = Money {
            currency_code: "RUB".into(),
            units: 1000,
            nanos: 500_000_000,
        };
        assert_eq!(money(&m), ("RUB".into(), "1000.5".parse().unwrap()));
        let neg = Money {
            currency_code: "RUB".into(),
            units: -3,
            nanos: -10_000_000,
        };
        assert_eq!(money(&neg).1, "-3.01".parse().unwrap());
    }

    #[test]
    fn symbol_roundtrip_without_network() {
        let id = instrument_from_symbol("SBER@MISX");
        assert_eq!(id.key.display(), "SBER@MISX");
        assert_eq!(symbol_of(&id).unwrap(), "SBER@MISX");
    }

    #[test]
    fn non_finam_native_id_is_a_config_error() {
        let id = InstrumentId::new(
            InstrumentKey::new("SBER", "TQBR"),
            NativeId::Figi("X".into()),
        );
        assert!(matches!(symbol_of(&id), Err(Error::Config(_))));
    }

    #[test]
    fn timestamp_conversions() {
        let n = UnixNanos::from_nanos(1_700_000_000_123_456_789);
        let t = to_timestamp(n);
        assert_eq!(ts_nanos(&t), n);
    }
}
