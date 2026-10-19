//! The price steps a connector knows, and the check of an order price against them.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use brokapi_core::{Error, InstrumentKey, Price, TradingParams, check_price_step};

/// The price step of each instrument the connector was told about.
///
/// A connector that sends orders refuses a price that is not a whole number of steps **before it
/// sends anything**: the error is [`Error::Rejected`] with the code `price_off_step`, no quota is spent
/// and the cid stays unused. An instrument whose step is not known passes unchecked: the broker
/// decides then. The grid starts empty. Fill it from [`TradingParams`], which you read once per
/// instrument with
/// [`InstrumentSource::trading_params`](brokapi_core::InstrumentSource::trading_params).
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

    /// Remembers the price step from the trading parameters of an instrument.
    pub fn learn(&self, params: &TradingParams) {
        self.set(params.instrument.key.clone(), params.price_step);
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
    fn clones_share_one_grid() {
        let a = PriceGrid::default();
        let b = a.clone();
        let k = InstrumentKey::new("GAZP", "TQBR");
        b.set(k.clone(), Price::new(dec!(0.01)));
        assert_eq!(a.step(&k), Some(Price::new(dec!(0.01))));
    }
}
