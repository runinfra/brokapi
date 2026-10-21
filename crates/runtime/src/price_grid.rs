//! The price steps and lot sizes a connector knows, and the check of an order price and quantity against them.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use brokapi_core::{Error, InstrumentKey, Price, Qty, TradingParams, check_price_step};

/// The price step of each instrument the connector was told about.
///
/// A connector that sends orders refuses a price that is not a whole number of steps **before it
/// sends anything**: the error is [`Error::Rejected`] with the code `price_off_step`, no quota is spent
/// and the cid stays unused. An instrument whose step is not known passes unchecked: the broker
/// decides then. The grid starts empty. Fill it from [`TradingParams`], which you read once per
/// instrument with
/// [`InstrumentSource::trading_params`](brokapi_core::InstrumentSource::trading_params).
///
/// The grid also keeps the lot size of an instrument. Quantities are in units of the instrument at every
/// broker (see [`Qty`]), and an exchange trades whole lots, so a quantity that is not a whole number of
/// lots is refused the same way, before anything is sent: [`Error::Rejected`] with the code
/// `invalid_qty`. An instrument whose lot is not known passes unchecked (the T-Bank connector looks the
/// lot up itself, it does not need the grid for that).
///
/// The handle is cheap to clone, and all clones are one grid: share it between the connectors of one
/// broker.
///
/// # Examples
///
/// ```
/// use brokapi_core::{Decimal, InstrumentKey, Price, dec};
/// use brokapi_runtime::PriceGrid;
///
/// let grid = PriceGrid::default();
/// let sber = InstrumentKey::new("SBER", "TQBR");
///
/// // The step is not known yet: the check passes.
/// assert!(grid.check(&sber, Price::new(dec!(244.307))).is_ok());
///
/// grid.set(sber.clone(), Price::new(dec!(0.01)));
/// assert!(grid.check(&sber, Price::new(dec!(244.30))).is_ok());
/// assert!(grid.check(&sber, Price::new(dec!(244.307))).is_err());
/// ```
#[derive(Debug, Clone, Default)]
pub struct PriceGrid {
    steps: Arc<RwLock<HashMap<InstrumentKey, Price>>>,
    lots: Arc<RwLock<HashMap<InstrumentKey, Qty>>>,
}

impl PriceGrid {
    /// Remembers the price step of an instrument. A step of zero or below forgets it.
    pub fn set(&self, key: InstrumentKey, step: Price) {
        let mut steps = self.steps.write().unwrap_or_else(|e| e.into_inner());
        if step.is_positive() {
            steps.insert(key, step);
        } else {
            steps.remove(&key);
        }
    }

    /// Remembers the price step and the lot size from the trading parameters of an instrument.
    pub fn learn(&self, params: &TradingParams) {
        self.set(params.instrument.key.clone(), params.price_step);
        self.set_lot(params.instrument.key.clone(), params.lot_size);
    }

    /// Remembers the lot size (units of the instrument in one lot). A size of zero or below forgets it.
    pub fn set_lot(&self, key: InstrumentKey, lot: Qty) {
        let mut lots = self.lots.write().unwrap_or_else(|e| e.into_inner());
        if lot.is_positive() {
            lots.insert(key, lot);
        } else {
            lots.remove(&key);
        }
    }

    /// The lot size of an instrument, if it is known.
    pub fn lot(&self, key: &InstrumentKey) -> Option<Qty> {
        self.lots
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(key)
            .copied()
    }

    /// Checks an order quantity against the lot size of the instrument.
    ///
    /// # Errors
    ///
    /// [`Error::Rejected`] with the code `invalid_qty` when the quantity is not positive, or when the
    /// lot size is known and the quantity is not a whole number of lots.
    pub fn check_qty(&self, key: &InstrumentKey, qty: Qty) -> Result<(), Error> {
        if !qty.is_positive() {
            return Err(Error::Rejected {
                code: "invalid_qty".into(),
                message: format!("quantity must be positive, got {qty}"),
            });
        }
        match self.lot(key) {
            Some(lot) if !qty.is_on_step(lot) => Err(Error::Rejected {
                code: "invalid_qty".into(),
                message: format!("quantity {qty} is not a whole number of lots of {lot}"),
            }),
            _ => Ok(()),
        }
    }

    /// The step of an instrument, if it is known.
    pub fn step(&self, key: &InstrumentKey) -> Option<Price> {
        self.steps
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(key)
            .copied()
    }

    /// Checks a price against the step of the instrument.
    ///
    /// # Errors
    ///
    /// [`Error::Rejected`] with the code `price_off_step` when the step is known and the price is not a
    /// whole number of steps.
    pub fn check(&self, key: &InstrumentKey, price: Price) -> Result<(), Error> {
        match self.step(key) {
            Some(step) => check_price_step(price, step),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use brokapi_core::{Decimal, dec};

    #[test]
    fn unknown_step_passes_and_zero_forgets() {
        let grid = PriceGrid::default();
        let k = InstrumentKey::new("SBER", "TQBR");
        assert!(grid.check(&k, Price::new(dec!(1.234))).is_ok());
        grid.set(k.clone(), Price::new(dec!(0.05)));
        assert!(grid.check(&k, Price::new(dec!(1.23))).is_err());
        grid.set(k.clone(), Price::ZERO);
        assert!(grid.check(&k, Price::new(dec!(1.23))).is_ok());
    }

    #[test]
    fn quantity_is_checked_against_the_lot_when_it_is_known() {
        let grid = PriceGrid::default();
        let k = InstrumentKey::new("SBER", "TQBR");
        // No lot known: any positive quantity passes, a non-positive one never does.
        assert!(grid.check_qty(&k, Qty::from(3)).is_ok());
        assert!(grid.check_qty(&k, Qty::ZERO).is_err());
        assert!(grid.check_qty(&k, Qty::from(-1)).is_err());
        grid.set_lot(k.clone(), Qty::from(10));
        assert_eq!(grid.lot(&k), Some(Qty::from(10)));
        assert!(grid.check_qty(&k, Qty::from(30)).is_ok());
        let err = grid.check_qty(&k, Qty::from(25)).unwrap_err();
        assert!(matches!(err, Error::Rejected { ref code, .. } if code == "invalid_qty"));
        assert!(grid.check_qty(&k, Qty::new(dec!(0.5))).is_err());
        grid.set_lot(k.clone(), Qty::ZERO);
        assert_eq!(grid.lot(&k), None);
        assert!(grid.check_qty(&k, Qty::from(25)).is_ok());
    }

    #[test]
    fn learn_takes_step_and_lot_from_the_trading_params() {
        use brokapi_core::{InstrumentId, Money, NativeId, TradingParams};
        let k = InstrumentKey::new("SBER", "TQBR");
        let params = TradingParams {
            instrument: InstrumentId::new(
                k.clone(),
                NativeId::TickerClass {
                    ticker: "SBER".into(),
                    class_code: "TQBR".into(),
                },
            ),
            lot_size: Qty::from(10),
            min_qty: Qty::from(10),
            price_step: Price::new(dec!(0.01)),
            price_step_value: Money::new(dec!(0.01)),
            currency: "RUB".into(),
        };
        let grid = PriceGrid::default();
        grid.learn(&params);
        assert_eq!(grid.step(&k), Some(Price::new(dec!(0.01))));
        assert_eq!(grid.lot(&k), Some(Qty::from(10)));
    }

    #[test]
    fn clones_share_one_grid() {
        let a = PriceGrid::default();
        let b = a.clone();
        let k = InstrumentKey::new("GAZP", "TQBR");
        b.set(k.clone(), Price::new(dec!(0.01)));
        assert_eq!(a.step(&k), Some(Price::new(dec!(0.01))));
    }
}
