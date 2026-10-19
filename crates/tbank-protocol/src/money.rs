//! `Quotation` / `MoneyValue` (units + nano) <-> [`Decimal`], exactly.
//!
//! T-Bank prices are `units` (integer part) and `nano` (billionths); both carry the sign of the value.
//! A [`Decimal`] holds every such value without loss, so the conversion is exact in both directions.
//! There is no `f64` on the way. Going back needs at most 9 decimal places: a value with more is
//! refused with [`MoneyError::TooManyDecimals`] and never rounded in silence.
//!
//! The trailing zeros are dropped on the way in (`Quotation { units: 114, nano: 250_000_000 }` is
//! `114.25`). A [`Decimal`] compares by value, so the scale does not matter.
//!
//! # Examples
//!
//! ```
//! use rust_decimal::Decimal;
//! use brokapi_tbank_protocol::money::{
//!     decimal_to_money, decimal_to_parts, decimal_to_quotation, money_to_decimal, quotation_to_decimal,
//! };
//!
//! // 114.25 is 114 units and 250 000 000 nano.
//! let price = Decimal::new(11425, 2);
//! let q = decimal_to_quotation(price)?;
//! assert_eq!((q.units, q.nano), (114, 250_000_000));
//! assert_eq!(quotation_to_decimal(&q), price);
//!
//! // A negative value carries the sign on BOTH parts.
//! assert_eq!(decimal_to_parts(Decimal::new(-2002, 1))?, (-200, -200_000_000));
//!
//! // Money keeps the currency code next to the amount.
//! let m = decimal_to_money("rub", Decimal::new(15005, 1))?;
//! assert_eq!((m.currency.as_str(), m.units, m.nano), ("rub", 1500, 500_000_000));
//! assert_eq!(money_to_decimal(&m), Decimal::new(15005, 1));
//! # Ok::<(), brokapi_tbank_protocol::money::MoneyError>(())
//! ```

use std::str::FromStr;

use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use thiserror::Error;

use crate::v1::{MoneyValue, Quotation};

const NANO_SCALE: u32 = 9;
const NANO: i64 = 1_000_000_000;

/// Why a number could not be turned into `units` and `nano`.
///
/// # Examples
///
/// ```
/// use rust_decimal::Decimal;
/// use brokapi_tbank_protocol::money::{MoneyError, decimal_to_parts};
///
/// // A tenth of a nano does not exist.
/// assert_eq!(decimal_to_parts(Decimal::new(1, 10)), Err(MoneyError::TooManyDecimals));
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum MoneyError {
    /// The value has more than 9 decimal places (not counting trailing zeros).
    #[error("more than 9 decimal places: T-Bank counts in billionths")]
    TooManyDecimals,
    /// The integer part does not fit in an `i64`.
    #[error("the integer part does not fit in an i64")]
    OutOfRange,
    /// The text is not a decimal number.
    #[error("not a decimal number: {0:?}")]
    Parse(String),
}

/// Converts a [`Quotation`] (a price or a quantity in pieces) to a [`Decimal`], exactly.
///
/// Both parts are added with their own signs, so a negative value works as expected.
///
/// # Examples
///
/// ```
/// use rust_decimal::Decimal;
/// use brokapi_tbank_protocol::money::quotation_to_decimal;
/// use brokapi_tbank_protocol::v1::Quotation;
///
/// assert_eq!(quotation_to_decimal(&Quotation { units: 114, nano: 250_000_000 }), Decimal::new(11425, 2));
/// assert_eq!(quotation_to_decimal(&Quotation { units: 0, nano: -10_000_000 }), Decimal::new(-1, 2));
/// ```
pub fn quotation_to_decimal(q: &Quotation) -> Decimal {
    parts_to_decimal(q.units, q.nano)
}

/// Converts a [`MoneyValue`] to a [`Decimal`], ignoring its currency.
///
/// Read `m.currency` yourself before adding amounts: this function does not convert currencies.
///
/// # Examples
///
/// ```
/// use rust_decimal::Decimal;
/// use brokapi_tbank_protocol::money::{decimal_to_money, money_to_decimal};
///
/// let m = decimal_to_money("usd", Decimal::new(105, 1))?;
/// assert_eq!(money_to_decimal(&m), Decimal::new(105, 1));
/// # Ok::<(), brokapi_tbank_protocol::money::MoneyError>(())
/// ```
pub fn money_to_decimal(m: &MoneyValue) -> Decimal {
    parts_to_decimal(m.units, m.nano)
}

/// Converts raw `units` and `nano` to a [`Decimal`], exactly.
///
/// `nano` is in billionths and should carry the same sign as `units` (or be the only non-zero part).
/// The function does not check the range of `nano`: it is simply added.
///
/// # Examples
///
/// ```
/// use rust_decimal::Decimal;
/// use brokapi_tbank_protocol::money::parts_to_decimal;
///
/// assert_eq!(parts_to_decimal(1, 500_000_000), Decimal::new(15, 1));
/// assert_eq!(parts_to_decimal(-1, -500_000_000), Decimal::new(-15, 1));
/// ```
pub fn parts_to_decimal(units: i64, nano: i32) -> Decimal {
    // units < 2^63 and nano/1e9 with 9 places: well inside the 28 digits of Decimal.
    (Decimal::from(units) + Decimal::new(i64::from(nano), NANO_SCALE)).normalize()
}

/// Splits a number into `(units, nano)`. Both parts get the sign of the value.
///
/// # Errors
///
/// [`MoneyError::TooManyDecimals`] if the value does not fit into billionths without rounding,
/// [`MoneyError::OutOfRange`] if the integer part does not fit in an `i64`.
///
/// # Examples
///
/// ```
/// use rust_decimal::Decimal;
/// use brokapi_tbank_protocol::money::decimal_to_parts;
///
/// assert_eq!(decimal_to_parts(Decimal::new(11425, 2))?, (114, 250_000_000));
/// assert_eq!(decimal_to_parts(Decimal::new(-1, 2))?, (0, -10_000_000));
/// // Trailing zeros are not decimals: 0.5000000000 has one.
/// assert_eq!(decimal_to_parts(Decimal::new(5_000_000_000, 10))?, (0, 500_000_000));
/// # Ok::<(), brokapi_tbank_protocol::money::MoneyError>(())
/// ```
pub fn decimal_to_parts(value: Decimal) -> Result<(i64, i32), MoneyError> {
    let v = value.normalize();
    if v.scale() > NANO_SCALE {
        return Err(MoneyError::TooManyDecimals);
    }
    let units = v.trunc().to_i64().ok_or(MoneyError::OutOfRange)?;
    let mut frac = v.fract();
    frac.rescale(NANO_SCALE);
    // |frac| < 1 with 9 places: the mantissa is below 1e9, it fits in an i32.
    let nano = i32::try_from(frac.mantissa() as i64).map_err(|_| MoneyError::OutOfRange)?;
    debug_assert!(i64::from(nano).abs() < NANO);
    Ok((units, nano))
}

/// Builds a [`Quotation`] from a number.
///
/// Use it for the `price` of an order request. This function does not round to the instrument price
/// step: round with the library first (`Price::round_to_step`).
///
/// # Errors
///
/// As [`decimal_to_parts`].
///
/// # Examples
///
/// ```
/// use rust_decimal::Decimal;
/// use brokapi_tbank_protocol::money::decimal_to_quotation;
///
/// let q = decimal_to_quotation(Decimal::new(27345, 2))?;
/// assert_eq!((q.units, q.nano), (273, 450_000_000));
/// # Ok::<(), brokapi_tbank_protocol::money::MoneyError>(())
/// ```
pub fn decimal_to_quotation(value: Decimal) -> Result<Quotation, MoneyError> {
    let (units, nano) = decimal_to_parts(value)?;
    Ok(Quotation { units, nano })
}

/// Builds a [`MoneyValue`] with the given currency code from a number.
///
/// `currency` is stored as given. T-Bank messages carry lower-case codes such as `"rub"` (not
/// verified against the live API for every message).
///
/// # Errors
///
/// As [`decimal_to_parts`].
///
/// # Examples
///
/// ```
/// use rust_decimal::Decimal;
/// use brokapi_tbank_protocol::money::decimal_to_money;
///
/// let m = decimal_to_money("rub", Decimal::new(1, 2))?;
/// assert_eq!((m.units, m.nano), (0, 10_000_000));
/// assert_eq!(m.currency, "rub");
/// # Ok::<(), brokapi_tbank_protocol::money::MoneyError>(())
/// ```
pub fn decimal_to_money(currency: &str, value: Decimal) -> Result<MoneyValue, MoneyError> {
    let (units, nano) = decimal_to_parts(value)?;
    Ok(MoneyValue {
        currency: currency.to_string(),
        units,
        nano,
    })
}

/// Parses a decimal text into a [`Quotation`]: `"273.45"`.
///
/// # Errors
///
/// [`MoneyError::Parse`] for text that is not a number, and as [`decimal_to_parts`].
///
/// # Examples
///
/// ```
/// use brokapi_tbank_protocol::money::quotation_from_str;
///
/// let q = quotation_from_str("0.0001")?;
/// assert_eq!((q.units, q.nano), (0, 100_000));
/// # Ok::<(), brokapi_tbank_protocol::money::MoneyError>(())
/// ```
pub fn quotation_from_str(s: &str) -> Result<Quotation, MoneyError> {
    decimal_to_quotation(parse(s)?)
}

/// Parses a decimal text into a [`MoneyValue`] of the given currency.
///
/// # Errors
///
/// As [`quotation_from_str`].
///
/// # Examples
///
/// ```
/// use brokapi_tbank_protocol::money::money_from_str;
///
/// let m = money_from_str("rub", "1500.50")?;
/// assert_eq!((m.units, m.nano), (1500, 500_000_000));
/// # Ok::<(), brokapi_tbank_protocol::money::MoneyError>(())
/// ```
pub fn money_from_str(currency: &str, s: &str) -> Result<MoneyValue, MoneyError> {
    decimal_to_money(currency, parse(s)?)
}

/// [`quotation_from_str`] for a literal that is known to be right (tests, examples, imitations).
///
/// # Panics
///
/// If the text is not a number with at most 9 decimals.
///
/// # Examples
///
/// ```
/// let q = brokapi_tbank_protocol::money::quotation("114.25");
/// assert_eq!((q.units, q.nano), (114, 250_000_000));
/// ```
pub fn quotation(s: &str) -> Quotation {
    quotation_from_str(s).unwrap_or_else(|e| panic!("bad quotation literal {s:?}: {e}"))
}

/// [`money_from_str`] for a literal that is known to be right (tests, examples, imitations).
///
/// # Panics
///
/// If the text is not a number with at most 9 decimals.
///
/// # Examples
///
/// ```
/// let m = brokapi_tbank_protocol::money::money("rub", "-4");
/// assert_eq!((m.units, m.nano), (-4, 0));
/// ```
pub fn money(currency: &str, s: &str) -> MoneyValue {
    money_from_str(currency, s).unwrap_or_else(|e| panic!("bad money literal {s:?}: {e}"))
}

fn parse(s: &str) -> Result<Decimal, MoneyError> {
    let t = s.trim();
    Decimal::from_str(t)
        .or_else(|_| Decimal::from_scientific(t))
        .map_err(|_| MoneyError::Parse(s.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn quotation_positive() {
        let q = Quotation {
            units: 114,
            nano: 250_000_000,
        };
        assert_eq!(quotation_to_decimal(&q), Decimal::new(11425, 2));
    }

    #[test]
    fn quotation_negative_sign_on_both_parts() {
        let q = Quotation {
            units: -200,
            nano: -200_000_000,
        };
        assert_eq!(quotation_to_decimal(&q), Decimal::new(-2002, 1));
    }

    #[test]
    fn quotation_sub_unit() {
        let q = Quotation {
            units: 0,
            nano: -10_000_000,
        };
        assert_eq!(quotation_to_decimal(&q), Decimal::new(-1, 2));
    }

    #[test]
    fn split_keeps_equal_signs() {
        assert_eq!(
            decimal_to_parts(Decimal::new(-2002, 1)),
            Ok((-200, -200_000_000))
        );
        assert_eq!(decimal_to_parts(Decimal::new(-1, 2)), Ok((0, -10_000_000)));
        assert_eq!(
            decimal_to_parts(Decimal::new(11425, 2)),
            Ok((114, 250_000_000))
        );
    }

    #[test]
    fn exact_edges() {
        // Smallest unit and the biggest fraction.
        assert_eq!(decimal_to_parts(Decimal::new(1, 9)), Ok((0, 1)));
        assert_eq!(
            decimal_to_parts(Decimal::new(999_999_999, 9)),
            Ok((0, 999_999_999))
        );
        assert_eq!(
            decimal_to_parts(Decimal::new(-999_999_999, 9)),
            Ok((0, -999_999_999))
        );
        assert_eq!(
            decimal_to_parts(Decimal::new(1, 10)),
            Err(MoneyError::TooManyDecimals)
        );
        assert_eq!(
            decimal_to_parts(Decimal::MAX),
            Err(MoneyError::TooManyDecimals).or(Err(MoneyError::OutOfRange))
        );
        assert_eq!(decimal_to_parts(Decimal::from(i64::MAX)), Ok((i64::MAX, 0)));
        assert_eq!(
            decimal_to_parts(Decimal::from(i64::MAX) + Decimal::ONE),
            Err(MoneyError::OutOfRange)
        );
    }

    #[test]
    fn money_has_currency() {
        let m = money("rub", "1500.5");
        assert_eq!(
            (m.currency.as_str(), m.units, m.nano),
            ("rub", 1500, 500_000_000)
        );
        assert_eq!(money_to_decimal(&m), Decimal::new(15005, 1));
    }

    #[test]
    fn literals_and_parse_errors() {
        assert_eq!(
            quotation("0.0001"),
            Quotation {
                units: 0,
                nano: 100_000
            }
        );
        assert!(matches!(quotation_from_str("x"), Err(MoneyError::Parse(_))));
        assert_eq!(
            quotation_from_str("0.0000000001"),
            Err(MoneyError::TooManyDecimals)
        );
    }

    proptest! {
        // broker -> type -> broker: the units and nano come back as they were.
        #[test]
        fn parts_round_trip(units in -1_000_000_000_000_i64..1_000_000_000_000, nano in 0i32..1_000_000_000, neg in any::<bool>()) {
            let (units, nano) = if neg { (-units.abs(), -nano) } else { (units.abs(), nano) };
            let d = parts_to_decimal(units, nano);
            prop_assert_eq!(decimal_to_parts(d), Ok((units, nano)));
        }

        // type -> broker -> type: a number with up to 9 places comes back equal.
        #[test]
        fn decimal_round_trip(m in -1_000_000_000_000_000_000_i64..1_000_000_000_000_000_000, scale in 0u32..=9) {
            let d = Decimal::new(m, scale);
            let q = decimal_to_quotation(d).unwrap();
            prop_assert_eq!(quotation_to_decimal(&q), d);
            prop_assert!(i64::from(q.nano.signum()) * q.units.signum() >= 0);
            let mv = decimal_to_money("rub", d).unwrap();
            prop_assert_eq!(money_to_decimal(&mv), d);
        }
    }
}
