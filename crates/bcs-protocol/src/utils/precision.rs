//! Price and quantity arithmetic in whole steps, exactly.
//!
//! Exchange prices move in steps (for example 0.05) and an order price must sit on the step grid, or
//! the exchange refuses it. The numbers are [`Decimal`]s, so the step arithmetic has no noise:
//! `101.05 / 0.05` is exactly `2021` (in `f64` it is `2020.9999999999998`). The functions here count
//! the value in whole steps and give it back. The step comes from the instrument data, see
//! [`TradingParamsSnapshot::price_step`](crate::http::models::TradingParamsSnapshot::price_step).
//!
//! All functions are pure and return a [`PrecisionError`] for a step that cannot be used. The grid
//! starts at zero.
//!
//! # Examples
//!
//! ```
//! use brokapi_bcs_protocol::utils::precision::{precise_down, precise_up, precise_to_nearest};
//! use rust_decimal::Decimal;
//!
//! let price = Decimal::new(10103, 2); // 101.03
//! let step = Decimal::new(5, 2); // 0.05
//! // A buy price is rounded down, a sell price up, so you never pay or sell worse than computed.
//! assert_eq!(precise_down(price, step)?, Decimal::new(10100, 2));
//! assert_eq!(precise_up(price, step)?, Decimal::new(10105, 2));
//! assert_eq!(precise_to_nearest(price, step)?, Decimal::new(10105, 2));
//! # Ok::<(), brokapi_bcs_protocol::utils::precision::PrecisionError>(())
//! ```

use rust_decimal::Decimal;
use thiserror::Error;

/// Why a step calculation was refused.
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::utils::precision::{precise_down, PrecisionError};
/// use rust_decimal::Decimal;
///
/// assert!(matches!(
///     precise_down(Decimal::ONE_HUNDRED, Decimal::ZERO),
///     Err(PrecisionError::InvalidStep(_))
/// ));
/// ```
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum PrecisionError {
    /// The step is zero or negative.
    #[error("step must be above 0, got {0}")]
    InvalidStep(Decimal),

    /// The count of steps does not fit in an `i64`, or a number overflowed.
    #[error("steps overflow: value={value}, step={step}")]
    StepsOverflow {
        /// The value that was converted.
        value: Decimal,
        /// The step it was divided by.
        step: Decimal,
    },
}

fn validate_step(step: Decimal) -> Result<(), PrecisionError> {
    if step <= Decimal::ZERO {
        return Err(PrecisionError::InvalidStep(step));
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum Mode {
    Down,
    Up,
    Nearest,
}

fn steps(value: Decimal, step: Decimal, mode: Mode) -> Result<i64, PrecisionError> {
    validate_step(step)?;
    let overflow = || PrecisionError::StepsOverflow { value, step };
    // The remainder is exact, so a value on the grid is recognised as one.
    let rem = value.checked_rem(step).ok_or_else(overflow)?;
    let whole = value.checked_sub(rem).ok_or_else(overflow)?;
    // `whole` is a multiple of step with the sign of value; the count of steps is exact.
    let mut n = whole
        .checked_div(step)
        .and_then(|d| i64::try_from(d.trunc()).ok())
        .ok_or_else(overflow)?;
    if rem.is_zero() {
        return Ok(n);
    }
    // n is now toward zero. Move to the floor or the ceiling.
    let negative = rem < Decimal::ZERO;
    let floor = if negative { n - 1 } else { n };
    let ceil = floor + 1;
    n = match mode {
        Mode::Down => floor,
        Mode::Up => ceil,
        Mode::Nearest => {
            // Halves go away from zero.
            let twice = rem.abs() * Decimal::TWO;
            if twice >= step {
                if negative { floor } else { ceil }
            } else if negative {
                ceil
            } else {
                floor
            }
        }
    };
    Ok(n)
}

/// Converts a value to a whole number of steps, rounding to the **nearest** step (halves go away from
/// zero).
///
/// # Errors
///
/// [`PrecisionError::InvalidStep`] for a step that is zero or negative,
/// [`PrecisionError::StepsOverflow`] if the count does not fit in an `i64`.
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::utils::precision::value_to_steps_nearest;
/// use rust_decimal::Decimal;
///
/// assert_eq!(value_to_steps_nearest(Decimal::new(10105, 2), Decimal::new(5, 2))?, 2021);
/// # Ok::<(), brokapi_bcs_protocol::utils::precision::PrecisionError>(())
/// ```
pub fn value_to_steps_nearest(value: Decimal, step: Decimal) -> Result<i64, PrecisionError> {
    steps(value, step, Mode::Nearest)
}

/// Converts a value to a whole number of steps, rounding **down** (towards negative infinity).
///
/// # Errors
///
/// The same as [`value_to_steps_nearest`].
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::utils::precision::value_to_steps_down;
/// use rust_decimal::Decimal;
///
/// assert_eq!(value_to_steps_down(Decimal::new(10103, 2), Decimal::new(5, 2))?, 2020);
/// # Ok::<(), brokapi_bcs_protocol::utils::precision::PrecisionError>(())
/// ```
pub fn value_to_steps_down(value: Decimal, step: Decimal) -> Result<i64, PrecisionError> {
    steps(value, step, Mode::Down)
}

/// Converts a value to a whole number of steps, rounding **up** (towards positive infinity).
///
/// # Errors
///
/// The same as [`value_to_steps_nearest`].
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::utils::precision::value_to_steps_up;
/// use rust_decimal::Decimal;
///
/// assert_eq!(value_to_steps_up(Decimal::new(10103, 2), Decimal::new(5, 2))?, 2021);
/// # Ok::<(), brokapi_bcs_protocol::utils::precision::PrecisionError>(())
/// ```
pub fn value_to_steps_up(value: Decimal, step: Decimal) -> Result<i64, PrecisionError> {
    steps(value, step, Mode::Up)
}

/// Converts a whole number of steps back to a value, exactly: `2001` steps of `0.05` is `100.05`.
///
/// # Errors
///
/// [`PrecisionError::InvalidStep`] if the step is zero or negative, [`PrecisionError::StepsOverflow`]
/// if the product does not fit.
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::utils::precision::steps_to_value;
/// use rust_decimal::Decimal;
///
/// assert_eq!(steps_to_value(2001, Decimal::new(5, 2))?, Decimal::new(10005, 2));
/// assert_eq!(steps_to_value(3, Decimal::new(1, 3))?, Decimal::new(3, 3));
/// # Ok::<(), brokapi_bcs_protocol::utils::precision::PrecisionError>(())
/// ```
pub fn steps_to_value(steps: i64, step: Decimal) -> Result<Decimal, PrecisionError> {
    validate_step(step)?;
    Decimal::from(steps)
        .checked_mul(step)
        .ok_or(PrecisionError::StepsOverflow {
            value: Decimal::from(steps),
            step,
        })
}

/// Rounds a value to the **nearest** point of the step grid.
///
/// # Errors
///
/// The same as [`value_to_steps_nearest`].
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::utils::precision::precise_to_nearest;
/// use rust_decimal::Decimal;
///
/// let step = Decimal::new(5, 2);
/// assert_eq!(precise_to_nearest(Decimal::new(10102, 2), step)?, Decimal::new(10100, 2));
/// assert_eq!(precise_to_nearest(Decimal::new(10103, 2), step)?, Decimal::new(10105, 2));
/// # Ok::<(), brokapi_bcs_protocol::utils::precision::PrecisionError>(())
/// ```
pub fn precise_to_nearest(value: Decimal, step: Decimal) -> Result<Decimal, PrecisionError> {
    steps_to_value(value_to_steps_nearest(value, step)?, step)
}

/// Rounds a value **down** to the step grid.
///
/// Typical for a **buy** price: never pay more than the computed value.
///
/// # Errors
///
/// The same as [`value_to_steps_nearest`].
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::utils::precision::precise_down;
/// use rust_decimal::Decimal;
///
/// assert_eq!(precise_down(Decimal::new(10103, 2), Decimal::new(5, 2))?, Decimal::new(10100, 2));
/// // a whole-number step works too
/// assert_eq!(precise_down(Decimal::new(143, 1), Decimal::from(5))?, Decimal::from(10));
/// # Ok::<(), brokapi_bcs_protocol::utils::precision::PrecisionError>(())
/// ```
pub fn precise_down(value: Decimal, step: Decimal) -> Result<Decimal, PrecisionError> {
    steps_to_value(value_to_steps_down(value, step)?, step)
}

/// Rounds a value **up** to the step grid.
///
/// Typical for a **sell** price: never sell below the computed value.
///
/// # Errors
///
/// The same as [`value_to_steps_nearest`].
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::utils::precision::precise_up;
/// use rust_decimal::Decimal;
///
/// let step = Decimal::new(5, 2);
/// assert_eq!(precise_up(Decimal::new(10103, 2), step)?, Decimal::new(10105, 2));
/// // already on the grid: unchanged
/// assert_eq!(precise_up(Decimal::new(10105, 2), step)?, Decimal::new(10105, 2));
/// # Ok::<(), brokapi_bcs_protocol::utils::precision::PrecisionError>(())
/// ```
pub fn precise_up(value: Decimal, step: Decimal) -> Result<Decimal, PrecisionError> {
    steps_to_value(value_to_steps_up(value, step)?, step)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn d(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    #[test]
    fn test_quantize_nearest_basic() {
        assert_eq!(
            precise_to_nearest(d("101.03"), d("0.05")).unwrap(),
            d("101.05")
        );
        assert_eq!(
            precise_to_nearest(d("101.02"), d("0.05")).unwrap(),
            d("101.00")
        );
        assert_eq!(
            precise_to_nearest(d("101.025"), d("0.05")).unwrap(),
            d("101.05")
        );
        assert_eq!(
            precise_to_nearest(d("-101.025"), d("0.05")).unwrap(),
            d("-101.05")
        );
    }

    #[test]
    fn test_quantize_down_no_overpay() {
        assert_eq!(precise_down(d("101.03"), d("0.05")).unwrap(), d("101.00"));
        assert_eq!(precise_down(d("101.05"), d("0.05")).unwrap(), d("101.05"));
        assert_eq!(precise_down(d("-101.03"), d("0.05")).unwrap(), d("-101.05"));
    }

    #[test]
    fn test_quantize_up_no_undersell() {
        assert_eq!(precise_up(d("101.03"), d("0.05")).unwrap(), d("101.05"));
        assert_eq!(precise_up(d("101.05"), d("0.05")).unwrap(), d("101.05"));
        assert_eq!(precise_up(d("-101.03"), d("0.05")).unwrap(), d("-101.00"));
    }

    #[test]
    fn test_steps_to_value_is_exact() {
        assert_eq!(steps_to_value(2001, d("0.05")).unwrap(), d("100.05"));
        assert_eq!(steps_to_value(3, d("0.001")).unwrap(), d("0.003"));
        // The f64 ratios that were off by a hair are whole steps here.
        assert_eq!(value_to_steps_down(d("101.05"), d("0.05")).unwrap(), 2021);
        assert_eq!(value_to_steps_down(d("244.3"), d("0.01")).unwrap(), 24430);
    }

    #[test]
    fn test_invalid_step() {
        for bad in ["0", "-0.05"] {
            assert!(matches!(
                precise_to_nearest(d("100"), d(bad)),
                Err(PrecisionError::InvalidStep(_))
            ));
        }
    }

    #[test]
    fn test_integer_step() {
        assert_eq!(precise_down(d("14.3"), d("5")).unwrap(), d("10"));
        assert_eq!(precise_up(d("11"), d("5")).unwrap(), d("15"));
        assert_eq!(precise_to_nearest(d("12.5"), d("5")).unwrap(), d("15"));
    }

    proptest! {
        #[test]
        fn grid_brackets_the_value(m in -1_000_000_000_i64..1_000_000_000, s in 0u32..=6, sm in 1_i64..10_000, ss in 0u32..=6) {
            let v = Decimal::new(m, s);
            let step = Decimal::new(sm, ss);
            let down = precise_down(v, step).unwrap();
            let up = precise_up(v, step).unwrap();
            let near = precise_to_nearest(v, step).unwrap();
            prop_assert!(down <= v && v <= up && up - down <= step);
            prop_assert!(near == down || near == up);
            prop_assert!(down.checked_rem(step).unwrap().is_zero());
        }
    }
}
