//! Exact numbers: [`Price`], [`Qty`], [`Money`], and the rounding to a step ([`RoundMode`]).
//!
//! All three wrap [`rust_decimal::Decimal`]: a decimal number with a 96-bit integer part and up to 28
//! decimal places. `0.1 + 0.2 == 0.3` holds, `244.30` is not `244.299999...`, and a price is a
//! multiple of its step or it is not. The decision and the comparison with the fixed-point integers of
//! NautilusTrader are on the numbers page of the book: <https://brokapi.ru/docs/numbers.html>.

use std::fmt;
use std::iter::Sum;
use std::ops::{Add, AddAssign, Mul, Neg, Sub, SubAssign};
use std::str::FromStr;

use rust_decimal::Decimal;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Why a number could not be made, or why a step calculation was refused.
///
/// # Examples
///
/// ```
/// use brokapi_core::{NumberError, Price};
///
/// assert!(matches!("12,5".parse::<Price>(), Err(NumberError::Parse(_))));
/// assert!(matches!(Price::from_f64_lossy(f64::NAN, 2), Err(NumberError::NotFinite)));
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum NumberError {
    /// The text is not a decimal number. Holds the text.
    #[error("not a decimal number: {0:?}")]
    Parse(String),
    /// The `f64` is NaN or infinite.
    #[error("the number is not finite")]
    NotFinite,
    /// The value does not fit into the 96-bit integer part, or a calculation overflowed.
    #[error("the number is out of range")]
    OutOfRange,
    /// The step of a rounding is zero or negative.
    #[error("the step must be above zero")]
    StepNotPositive,
}

/// Which way a value is rounded to a grid, see [`Price::round_to_step`].
///
/// "Down" and "up" are toward minus and plus infinity: for the positive prices of a stock exchange
/// they are toward zero and away from zero.
///
/// # Examples
///
/// ```
/// use brokapi_core::{Price, RoundMode};
///
/// let step: Price = "0.05".parse()?;
/// let p: Price = "101.03".parse()?;
/// assert_eq!(p.round_to_step(step, RoundMode::Down)?, "101.00".parse()?);
/// assert_eq!(p.round_to_step(step, RoundMode::Up)?, "101.05".parse()?);
/// assert_eq!(p.round_to_step(step, RoundMode::Nearest)?, "101.05".parse()?);
/// # Ok::<(), brokapi_core::NumberError>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RoundMode {
    /// To the nearest grid point at or below the value. A buy price rounded this way is never worse
    /// than computed.
    Down,
    /// To the nearest grid point at or above the value. A sell price rounded this way is never worse
    /// than computed.
    Up,
    /// To the nearest grid point. A value exactly in the middle goes away from zero.
    Nearest,
}

/// Rounds `value` to a multiple of `step`. The grid starts at zero.
fn round_to_step(value: Decimal, step: Decimal, mode: RoundMode) -> Result<Decimal, NumberError> {
    if step <= Decimal::ZERO {
        return Err(NumberError::StepNotPositive);
    }
    // The remainder is exact (integer arithmetic on the 96-bit mantissas), so a multiple of the step
    // is recognised as one, with no 0.9999999 noise. It has the sign of the value.
    let rem = value.checked_rem(step).ok_or(NumberError::OutOfRange)?;
    if rem.is_zero() {
        return Ok(value);
    }
    let floor = if rem > Decimal::ZERO {
        value.checked_sub(rem)
    } else {
        value.checked_sub(rem).and_then(|v| v.checked_sub(step))
    }
    .ok_or(NumberError::OutOfRange)?;
    let ceil = floor.checked_add(step).ok_or(NumberError::OutOfRange)?;
    Ok(match mode {
        RoundMode::Down => floor,
        RoundMode::Up => ceil,
        RoundMode::Nearest => {
            let down_dist = value - floor;
            let up_dist = ceil - value;
            if down_dist < up_dist || (down_dist == up_dist && value < Decimal::ZERO) {
                floor
            } else {
                ceil
            }
        }
    })
}

fn is_on_step(value: Decimal, step: Decimal) -> bool {
    step > Decimal::ZERO && value.checked_rem(step).is_some_and(|r| r.is_zero())
}

fn parse_decimal(s: &str) -> Result<Decimal, NumberError> {
    let t = s.trim();
    Decimal::from_str(t)
        .or_else(|_| Decimal::from_scientific(t))
        .map_err(|_| NumberError::Parse(s.to_string()))
}

fn decimal_from_f64(value: f64, decimals: u32) -> Result<Decimal, NumberError> {
    if !value.is_finite() {
        return Err(NumberError::NotFinite);
    }
    let d = Decimal::from_f64(value).ok_or(NumberError::OutOfRange)?;
    Ok(d.round_dp(decimals))
}

/// Methods and traits shared by [`Price`], [`Qty`] and [`Money`].
macro_rules! decimal_newtype {
    ($name:ident, $what:literal) => {
        impl $name {
            /// Zero.
            pub const ZERO: Self = Self(Decimal::ZERO);

            #[doc = concat!("Wraps a decimal number as ", $what, ". No range check is made.")]
            ///
            /// Make the [`Decimal`] with [`dec!`](crate::dec), `Decimal::new(24430, 2)` or by parsing.
            pub const fn new(value: Decimal) -> Self {
                Self(value)
            }

            /// Makes the value from a mantissa and a number of decimal places:
            /// `from_scaled(24430, 2)` is `244.30`. The trailing zero is kept.
            ///
            /// # Panics
            ///
            /// If `scale` is above 28.
            pub fn from_scaled(mantissa: i64, scale: u32) -> Self {
                Self(Decimal::new(mantissa, scale))
            }

            /// The exact number inside.
            pub const fn decimal(self) -> Decimal {
                self.0
            }

            /// Makes the value from an `f64`, rounded to `decimals` places. **This loses
            /// precision**: an `f64` already carries binary noise, and the rounding only hides it.
            /// Use it for numbers that come from outside as `f64`. Prefer parsing a string.
            ///
            /// `decimals` is the number of places the exchange knows (the scale of the price step).
            ///
            /// # Errors
            ///
            /// [`NumberError::NotFinite`] for NaN and infinities, [`NumberError::OutOfRange`] for a
            /// number that does not fit.
            pub fn from_f64_lossy(value: f64, decimals: u32) -> Result<Self, NumberError> {
                decimal_from_f64(value, decimals).map(Self)
            }

            /// The nearest `f64`. **This loses precision**: use it for a chart or a log line, never
            /// to compute a price that goes into an order.
            pub fn to_f64_lossy(self) -> f64 {
                self.0.to_f64().unwrap_or(f64::NAN)
            }

            /// Whether the value is zero (`0` and `0.00` are both zero).
            pub fn is_zero(self) -> bool {
                self.0.is_zero()
            }

            /// Whether the value is below zero.
            pub fn is_negative(self) -> bool {
                self.0.is_sign_negative() && !self.0.is_zero()
            }

            /// Whether the value is above zero.
            pub fn is_positive(self) -> bool {
                self.0.is_sign_positive() && !self.0.is_zero()
            }

            /// The value without its sign.
            pub fn abs(self) -> Self {
                Self(self.0.abs())
            }

            /// The number of decimal places the value is written with: `244.30` has 2.
            pub fn scale(self) -> u32 {
                self.0.scale()
            }

            /// The same number without trailing zeros: `244.30` becomes `244.3`. Equality and order
            /// do not depend on the scale, only the text does.
            pub fn normalize(self) -> Self {
                Self(self.0.normalize())
            }
        }

        impl fmt::Display for $name {
            /// Plain decimal text, with the trailing zeros kept: `244.30`.
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Display::fmt(&self.0, f)
            }
        }

        impl FromStr for $name {
            type Err = NumberError;

            /// Parses `250.15`, `-0.0001` or `1e-4`. Spaces around are dropped, a comma is not
            /// a decimal point.
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                parse_decimal(s).map(Self)
            }
        }

        impl From<Decimal> for $name {
            fn from(value: Decimal) -> Self {
                Self(value)
            }
        }

        impl From<$name> for Decimal {
            fn from(value: $name) -> Decimal {
                value.0
            }
        }

        impl From<i32> for $name {
            fn from(value: i32) -> Self {
                Self(Decimal::from(value))
            }
        }

        impl From<u32> for $name {
            fn from(value: u32) -> Self {
                Self(Decimal::from(value))
            }
        }

        impl From<i64> for $name {
            fn from(value: i64) -> Self {
                Self(Decimal::from(value))
            }
        }

        impl From<u64> for $name {
            fn from(value: u64) -> Self {
                Self(Decimal::from(value))
            }
        }

        impl Add for $name {
            type Output = Self;
            fn add(self, rhs: Self) -> Self {
                Self(self.0 + rhs.0)
            }
        }

        impl Sub for $name {
            type Output = Self;
            fn sub(self, rhs: Self) -> Self {
                Self(self.0 - rhs.0)
            }
        }

        impl Neg for $name {
            type Output = Self;
            fn neg(self) -> Self {
                Self(-self.0)
            }
        }

        impl Mul<Decimal> for $name {
            type Output = Self;
            fn mul(self, rhs: Decimal) -> Self {
                Self(self.0 * rhs)
            }
        }

        impl AddAssign for $name {
            fn add_assign(&mut self, rhs: Self) {
                self.0 += rhs.0;
            }
        }

        impl SubAssign for $name {
            fn sub_assign(&mut self, rhs: Self) {
                self.0 -= rhs.0;
            }
        }

        impl Sum for $name {
            fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
                Self(iter.map(|v| v.0).sum())
            }
        }

        impl<'a> Sum<&'a $name> for $name {
            fn sum<I: Iterator<Item = &'a $name>>(iter: I) -> Self {
                Self(iter.map(|v| v.0).sum())
            }
        }
    };
}

/// Rounding to a grid, for the types that sit on one (prices and quantities).
macro_rules! stepped {
    ($name:ident) => {
        impl $name {
            /// Rounds to a multiple of `step`. The grid starts at zero, as the exchange grids do.
            ///
            /// The calculation is exact: `244.30` with a step of `0.01` stays `244.30`, and
            /// `101.03` with a step of `0.05` becomes `101.00` down and `101.05` up or to the nearest, by
            /// the [`RoundMode`]. A value already on the grid is returned as it is.
            ///
            /// # Errors
            ///
            /// [`NumberError::StepNotPositive`] if `step` is zero or below,
            /// [`NumberError::OutOfRange`] on overflow.
            pub fn round_to_step(self, step: Self, mode: RoundMode) -> Result<Self, NumberError> {
                round_to_step(self.0, step.0, mode).map(Self)
            }

            /// Whether the value is a whole number of steps. `false` for a step that is zero or
            /// below.
            pub fn is_on_step(self, step: Self) -> bool {
                is_on_step(self.0, step.0)
            }
        }
    };
}

/// A quantity: order size, filled size, position size, trade size, candle volume.
///
/// An exact decimal number in a newtype, so a quantity cannot be passed where a price is expected.
/// Why decimal and not `f64`: see [`Price`].
///
/// **The unit is the same everywhere: units of the instrument, never lots.** A share is one share, a
/// bond is one bond (its price is a percent of the face value, see the book page on money and
/// quantities), a currency is one unit of it, a futures contract is one contract. The same unit in an
/// order, an order book level, a deal, a fill, a position and the volume of a candle, at every broker.
/// That is the rule of NautilusTrader and of the exchange. A broker that counts in lots (T-Bank) is
/// converted by its connector in both directions, so your code never sees a lot count.
///
/// - An order quantity ([`PlaceOrderRequest::qty`](crate::PlaceOrderRequest::qty)) is positive and a
///   whole number of lots. The size of a lot is
///   [`TradingParams::lot_size`](crate::TradingParams::lot_size), in units. A quantity that the broker
///   cannot take (a fraction where it takes whole units, a count that is not a multiple of the lot,
///   zero) is refused before anything is sent with [`Error::Rejected`](crate::Error::Rejected), code
///   `invalid_qty`, and the cid is not used. The library never rounds a quantity for you.
/// - A position, a deal, a fill and a volume are as the broker reports them, in units.
///
/// The library does not check the value against the account or the minimal size of an exchange board.
///
/// # Examples
///
/// ```
/// use brokapi_core::{Qty, dec};
/// use brokapi_core::Decimal;
///
/// let q = Qty::new(dec!(10));
/// assert_eq!(q, Qty::from(10));
/// assert_eq!(q.to_string(), "10");
/// assert_eq!("0.5".parse::<Qty>()? + "0.25".parse()?, "0.75".parse()?);
/// # Ok::<(), brokapi_core::NumberError>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Qty(Decimal);

decimal_newtype!(Qty, "a quantity");
stepped!(Qty);

impl Qty {
    /// The quantity as a whole number, or `None` if it has a fractional part or does not fit in an
    /// `i64`. `10.0` is whole.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::Qty;
    ///
    /// assert_eq!("10.0".parse::<Qty>()?.to_whole(), Some(10));
    /// assert_eq!("1.5".parse::<Qty>()?.to_whole(), None);
    /// # Ok::<(), brokapi_core::NumberError>(())
    /// ```
    pub fn to_whole(self) -> Option<i64> {
        if self.0.fract().is_zero() {
            self.0.to_i64()
        } else {
            None
        }
    }
}

/// A price of one unit of the instrument, in the currency of the instrument.
///
/// An exact decimal number ([`rust_decimal::Decimal`], 28 digits) in a newtype. Brokers send prices as
/// decimals (T-Bank as units and billionths, Finam as a decimal string, BCS as a JSON number), and
/// the exchange grid is decimal. In `f64` the price `244.3` is not on the step `0.01` (the remainder
/// `244.3 % 0.01` is `6.3e-15`, not zero) and `109.32 / 0.01` is `10931.999999999998` steps. Sums are off
/// too: `0.1 + 0.2` is `0.30000000000000004`. In `Price` all of these are exact.
///
/// How to make one:
/// - `Price::new(dec!(250.15))`, with [`dec!`](crate::dec) for a literal;
/// - `"250.15".parse::<Price>()`, for text from a file or a form;
/// - `Price::from(250)`, from an integer;
/// - `Price::from_scaled(25015, 2)`, from a mantissa and the places;
/// - [`Price::from_f64_lossy`], only for an `f64` that already exists. It loses precision and says so.
///
/// How to round to the grid of the instrument: [`Price::round_to_step`] with the
/// [`TradingParams::price_step`](crate::TradingParams::price_step), or
/// [`TradingParams::round_price`](crate::TradingParams::round_price). The library does not round for
/// you. The connectors refuse an order price off the grid when they know the step (give it to them
/// with `price_grid()` of the connector, see [`Error::Rejected`](crate::Error::Rejected), code
/// `price_off_step`).
///
/// Order and equality are numeric, `244.30 == 244.3`. Text and JSON keep the written scale:
/// `244.30` stays `"244.30"`. In JSON the number is a string, so no reader can round it. A JSON number is
/// read too, as exactly as the JSON reader hands it over (with `serde_json` and no `arbitrary_precision` an
/// `f64` comes in, so prefer the string).
///
/// # Examples
///
/// ```
/// use brokapi_core::{Price, RoundMode, dec};
/// use brokapi_core::Decimal;
///
/// let p = Price::new(dec!(244.30));
/// let step = Price::new(dec!(0.01));
/// assert!(p.is_on_step(step));
/// assert_eq!(p.to_string(), "244.30");
///
/// // In f64 these two are not equal, and 244.3 is not on the grid by the remainder.
/// assert_ne!(0.1_f64 + 0.2, 0.3);
/// assert_ne!(244.3_f64 % 0.01, 0.0);
/// assert_eq!(Price::new(dec!(0.1)) + Price::new(dec!(0.2)), Price::new(dec!(0.3)));
///
/// let off = Price::new(dec!(244.307));
/// assert_eq!(off.round_to_step(step, RoundMode::Down)?, p);
/// assert_eq!(serde_json::to_string(&p)?, "\"244.30\"");
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Price(Decimal);

decimal_newtype!(Price, "a price");
stepped!(Price);

/// An amount of money in some currency: a balance, a commission, the money value of a price step.
///
/// An exact decimal number in a newtype. The currency is not in the value: it sits next to it
/// (for example [`Balance::currency`](crate::Balance::currency)). Amounts of different currencies
/// are not meant to be added, and the type does not stop you, so check the currency first.
///
/// Amounts are in the currency itself (not in kopecks), as the broker reports them. Why decimal and
/// not `f64`: see [`Price`].
///
/// # Examples
///
/// ```
/// use brokapi_core::{Money, dec};
/// use brokapi_core::Decimal;
///
/// let fee = Money::new(dec!(-0.05));
/// let total: Money = [fee, fee, fee].into_iter().sum();
/// assert_eq!(total, Money::new(dec!(-0.15)));
/// assert!(total.is_negative());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Money(Decimal);

decimal_newtype!(Money, "an amount of money");

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use rust_decimal_macros::dec;

    fn p(s: &str) -> Price {
        s.parse().unwrap()
    }

    #[test]
    fn f64_grid_diverges_but_decimal_is_exact() {
        // On f64 the price 244.3 is not on the step 0.01 by the remainder (6.3e-15 is left), and
        // 109.32 is not a whole number of steps (10931.999999999998).
        assert_ne!(244.3_f64 % 0.01, 0.0);
        assert_ne!((109.32_f64 / 0.01).fract(), 0.0);
        assert!(p("244.3").is_on_step(p("0.01")));
        assert!(p("109.32").is_on_step(p("0.01")));
        // 101.05 on a 0.05 grid, which f64 gives as 2020.9999999999998 steps.
        assert_ne!((101.05_f64 / 0.05).fract(), 0.0);
        assert!(p("101.05").is_on_step(p("0.05")));
        assert_eq!(
            p("101.05")
                .round_to_step(p("0.05"), RoundMode::Down)
                .unwrap(),
            p("101.05")
        );
        assert_ne!(0.1_f64 + 0.2, 0.3);
        assert_eq!(p("0.1") + p("0.2"), p("0.3"));
    }

    #[test]
    fn rounding_modes() {
        let step = p("0.05");
        for (v, down, up, near) in [
            ("101.03", "101.00", "101.05", "101.05"),
            ("101.02", "101.00", "101.05", "101.00"),
            ("101.025", "101.00", "101.05", "101.05"),
            ("101.00", "101.00", "101.00", "101.00"),
            ("-101.03", "-101.05", "-101.00", "-101.05"),
            ("-101.02", "-101.05", "-101.00", "-101.00"),
            ("0.01", "0.00", "0.05", "0.00"),
        ] {
            let v = p(v);
            assert_eq!(
                v.round_to_step(step, RoundMode::Down).unwrap(),
                p(down),
                "{v} down"
            );
            assert_eq!(
                v.round_to_step(step, RoundMode::Up).unwrap(),
                p(up),
                "{v} up"
            );
            assert_eq!(
                v.round_to_step(step, RoundMode::Nearest).unwrap(),
                p(near),
                "{v} nearest"
            );
        }
    }

    #[test]
    fn bad_step() {
        assert_eq!(
            p("1").round_to_step(p("0"), RoundMode::Down),
            Err(NumberError::StepNotPositive)
        );
        assert_eq!(
            p("1").round_to_step(p("-0.1"), RoundMode::Down),
            Err(NumberError::StepNotPositive)
        );
        assert!(!p("1").is_on_step(p("0")));
    }

    #[test]
    fn big_step_and_whole_step() {
        assert_eq!(
            p("12345")
                .round_to_step(p("10"), RoundMode::Nearest)
                .unwrap(),
            p("12350")
        );
        assert_eq!(
            p("12344")
                .round_to_step(p("10"), RoundMode::Nearest)
                .unwrap(),
            p("12340")
        );
        assert_eq!(
            p("0.00012")
                .round_to_step(p("0.0001"), RoundMode::Up)
                .unwrap(),
            p("0.0002")
        );
    }

    #[test]
    fn scale_is_kept_in_text_and_json() {
        let x = p("244.30");
        assert_eq!(x.to_string(), "244.30");
        assert_eq!(serde_json::to_string(&x).unwrap(), "\"244.30\"");
        assert_eq!(
            serde_json::from_str::<Price>("\"0.0001\"")
                .unwrap()
                .to_string(),
            "0.0001"
        );
        assert_eq!(x, p("244.3"));
        assert_eq!(x.scale(), 2);
        assert_eq!(x.normalize().to_string(), "244.3");
    }

    #[test]
    fn parsing() {
        assert_eq!(p("  7 "), Price::from(7));
        assert_eq!(p("1e-4"), p("0.0001"));
        assert!("12,5".parse::<Price>().is_err());
        assert!("".parse::<Price>().is_err());
        assert!("abc".parse::<Qty>().is_err());
    }

    #[test]
    fn from_f64_is_explicit_and_rounded() {
        assert_eq!(Price::from_f64_lossy(244.3, 2).unwrap(), p("244.30"));
        assert_eq!(Price::from_f64_lossy(0.1 + 0.2, 2).unwrap(), p("0.30"));
        assert_eq!(
            Price::from_f64_lossy(f64::NAN, 2),
            Err(NumberError::NotFinite)
        );
        assert_eq!(
            Price::from_f64_lossy(f64::INFINITY, 2),
            Err(NumberError::NotFinite)
        );
        assert_eq!(p("244.3").to_f64_lossy(), 244.3);
    }

    #[test]
    fn whole_quantity() {
        assert_eq!(Qty::new(dec!(3)).to_whole(), Some(3));
        assert_eq!(Qty::new(dec!(3.0)).to_whole(), Some(3));
        assert_eq!(Qty::new(dec!(3.5)).to_whole(), None);
        assert_eq!(Qty::new(dec!(-3)).to_whole(), Some(-3));
    }

    #[test]
    fn money_sums() {
        let v = vec![Money::new(dec!(0.1)), Money::new(dec!(0.2))];
        assert_eq!(v.iter().sum::<Money>(), Money::new(dec!(0.3)));
        assert_eq!(v.into_iter().sum::<Money>(), Money::new(dec!(0.3)));
    }

    fn arb_price() -> impl Strategy<Value = Price> {
        (-1_000_000_000_i64..1_000_000_000_i64, 0u32..=8)
            .prop_map(|(m, s)| Price::from_scaled(m, s))
    }

    fn arb_step() -> impl Strategy<Value = Price> {
        (1_i64..100_000, 0u32..=6).prop_map(|(m, s)| Price::from_scaled(m, s))
    }

    proptest! {
        #[test]
        fn rounding_lands_on_the_grid_and_brackets_the_value(v in arb_price(), step in arb_step()) {
            let down = v.round_to_step(step, RoundMode::Down).unwrap();
            let up = v.round_to_step(step, RoundMode::Up).unwrap();
            let near = v.round_to_step(step, RoundMode::Nearest).unwrap();
            prop_assert!(down.is_on_step(step) && up.is_on_step(step) && near.is_on_step(step));
            prop_assert!(down <= v && v <= up);
            prop_assert!(up - down <= step);
            prop_assert!(near == down || near == up);
            // The nearest is not farther than the other bound.
            prop_assert!((near - v).abs() <= (down - v).abs().max((up - v).abs()));
            if v.is_on_step(step) {
                prop_assert_eq!(down, v);
                prop_assert_eq!(up, v);
            }
        }

        #[test]
        fn text_and_json_round_trip_keep_the_scale(v in arb_price()) {
            let s = v.to_string();
            let back: Price = s.parse().unwrap();
            prop_assert_eq!(back.to_string(), s.clone());
            let json = serde_json::to_string(&v).unwrap();
            let from_json: Price = serde_json::from_str(&json).unwrap();
            prop_assert_eq!(from_json.to_string(), s);
            prop_assert_eq!(from_json, v);
        }

        #[test]
        fn sums_are_exact(a in arb_price(), b in arb_price()) {
            prop_assert_eq!((a + b) - b, a);
        }
    }
}
