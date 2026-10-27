//! Limits of your own on the orders of a client: a read-only mode and caps on one order.
//!
//! The broker checks what it checks (money, the price band). These limits are the ones of the person who runs the
//! bot: a bot that a person lets look at a real account but not trade, a bot that must never send an order bigger
//! than it was meant to. They are checked in the client, **before** anything is sent: a refused order spends no
//! quota, and its `cid` is not used up.
//!
//! Each limit is a setting of the connection (`read_only`, `max_order_qty`, `max_order_value`) and a variable of the
//! environment (`BROKAPI_READ_ONLY`, `BROKAPI_MAX_ORDER_QTY`, `BROKAPI_MAX_ORDER_VALUE`). When both are given the
//! stricter one holds: the code of a bot can tighten a limit the person set in the environment, never loosen it.
//!
//! ```
//! use brokapi_client::{OrderGuard, Settings};
//!
//! let settings = Settings::parse(r#"{"max_order_value": "50000"}"#)?;
//! let guard = OrderGuard::from_settings(&settings)?;
//! assert!(!guard.read_only);
//! assert_eq!(guard.max_order_value.map(|v| v.to_string()), Some("50000".into()));
//! # Ok::<(), brokapi::core::Error>(())
//! ```

use std::str::FromStr;

use brokapi::core::{
    Decimal, Error, OrderType, PlaceOrderRequest, PlaceStopOrderRequest, Price, Qty,
};
use serde_json::Value;

use crate::error::config_error as config;
use crate::settings::Settings;

/// The code of the refusal of a command by the read-only mode.
pub const READ_ONLY: &str = "read_only";
/// The code of the refusal of an order bigger than `max_order_qty`.
pub const MAX_ORDER_QTY: &str = "max_order_qty";
/// The code of the refusal of an order worth more than `max_order_value`, or of a market order when the value is
/// capped (it has no price to count the value by).
pub const MAX_ORDER_VALUE: &str = "max_order_value";

/// The limits of your own on the orders of one client. The default has none.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OrderGuard {
    /// No command reaches the broker: `place`, `replace`, `cancel`, `place_stop`, `cancel_stop`, and `raw` other than
    /// a plain read. Reads and subscriptions work.
    pub read_only: bool,
    /// The largest quantity of one order, in units of the instrument (shares, contracts), as in the order.
    pub max_order_qty: Option<Decimal>,
    /// The largest value of one order: quantity times price, in the money of the price.
    pub max_order_value: Option<Decimal>,
}

impl OrderGuard {
    /// The limits from the settings of a connection and the environment, the stricter of the two for each.
    ///
    /// # Errors
    ///
    /// [`Error::Config`] for a limit that is not a positive number, or a `read_only` that is not a flag.
    pub fn from_settings(settings: &Settings) -> Result<Self, Error> {
        let read_only = match settings.0.get("read_only") {
            None | Some(Value::Null) => false,
            Some(Value::Bool(b)) => *b,
            Some(other) => {
                return Err(config(format!(
                    "read_only must be true or false, got {other}"
                )));
            }
        };
        let env_read_only = std::env::var("BROKAPI_READ_ONLY")
            .is_ok_and(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "true" | "1" | "yes"));
        Ok(Self {
            read_only: read_only || env_read_only,
            max_order_qty: stricter(
                cap(settings.0.get("max_order_qty"), "max_order_qty")?,
                cap_env("BROKAPI_MAX_ORDER_QTY")?,
            ),
            max_order_value: stricter(
                cap(settings.0.get("max_order_value"), "max_order_value")?,
                cap_env("BROKAPI_MAX_ORDER_VALUE")?,
            ),
        })
    }

    /// Whether any limit is set.
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }

    /// A command (`what` names it) in the read-only mode is refused.
    pub(crate) fn command(&self, what: &str) -> Result<(), Error> {
        if self.read_only {
            return Err(refuse(
                READ_ONLY,
                format!("{what} refused: the client is read-only (read_only), nothing was sent"),
            ));
        }
        Ok(())
    }

    /// A new order: the read-only mode, then the caps.
    pub(crate) fn place(&self, req: &PlaceOrderRequest) -> Result<(), Error> {
        self.command("place")?;
        let price = match req.order_type {
            OrderType::Market => None,
            _ => req.price,
        };
        self.caps(req.qty, price)
    }

    /// A new stop order: its value is counted by the limit price, else by the trigger price.
    pub(crate) fn place_stop(&self, req: &PlaceStopOrderRequest) -> Result<(), Error> {
        self.command("place_stop")?;
        self.caps(req.qty, Some(req.limit_price.unwrap_or(req.stop_price)))
    }

    /// The caps on an order of `qty` at `price` (`None`: a market order, no price to count by).
    pub(crate) fn caps(&self, qty: Qty, price: Option<Price>) -> Result<(), Error> {
        if let Some(max) = self.max_order_qty
            && qty.decimal() > max
        {
            return Err(refuse(
                MAX_ORDER_QTY,
                format!("the order of {qty} is above max_order_qty {max}, nothing was sent"),
            ));
        }
        if let Some(max) = self.max_order_value {
            let Some(price) = price else {
                return Err(refuse(
                    MAX_ORDER_VALUE,
                    format!(
                        "a market order has no price to count its value by, and max_order_value {max} is set: \
                         place a limit order, nothing was sent"
                    ),
                ));
            };
            let value = qty.decimal() * price.decimal();
            if value > max {
                return Err(refuse(
                    MAX_ORDER_VALUE,
                    format!(
                        "the order of {qty} at {price} is worth {value}, above max_order_value {max}, nothing was sent"
                    ),
                ));
            }
        }
        Ok(())
    }
}

fn refuse(code: &str, message: String) -> Error {
    Error::Rejected {
        code: code.into(),
        message,
    }
}

/// The smaller of two caps; one of them when the other is not set.
fn stricter(a: Option<Decimal>, b: Option<Decimal>) -> Option<Decimal> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    }
}

/// A cap of the settings: a positive number, as a JSON number or text.
fn cap(value: Option<&Value>, name: &str) -> Result<Option<Decimal>, Error> {
    let text = match value {
        None | Some(Value::Null) => return Ok(None),
        Some(Value::String(s)) => s.trim().to_owned(),
        Some(Value::Number(n)) => n.to_string(),
        Some(other) => {
            return Err(config(format!(
                "{name} must be a positive number, got {other}"
            )));
        }
    };
    positive(&text, name).map(Some)
}

fn cap_env(name: &str) -> Result<Option<Decimal>, Error> {
    match std::env::var(name) {
        Ok(v) if !v.trim().is_empty() => positive(v.trim(), name).map(Some),
        _ => Ok(None),
    }
}

fn positive(text: &str, name: &str) -> Result<Decimal, Error> {
    match Decimal::from_str(text) {
        Ok(d) if d > Decimal::ZERO => Ok(d.normalize()),
        _ => Err(config(format!(
            "{name} must be a positive number, got {text:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use brokapi::core::{
        ClientOrderId, InstrumentId, InstrumentKey, NativeId, Side, StopExpiry, StopOrderKind,
    };

    fn guard(json: &str) -> Result<OrderGuard, Error> {
        OrderGuard::from_settings(&Settings::parse(json).unwrap())
    }

    fn sber() -> InstrumentId {
        InstrumentId::new(
            InstrumentKey::new("SBER", "TQBR"),
            NativeId::TickerClass {
                ticker: "SBER".into(),
                class_code: "TQBR".into(),
            },
        )
    }

    fn order(order_type: OrderType, qty: i64, price: Option<i64>) -> PlaceOrderRequest {
        PlaceOrderRequest {
            cid: ClientOrderId::new_v4(),
            instrument: sber(),
            side: Side::Buy,
            order_type,
            qty: Qty::from(qty),
            price: price.map(Price::from),
        }
    }

    fn code(r: Result<(), Error>) -> Option<String> {
        match r {
            Err(Error::Rejected { code, .. }) => Some(code),
            Err(other) => panic!("not a refusal: {other}"),
            Ok(()) => None,
        }
    }

    #[test]
    fn without_limits_every_order_passes() {
        let g = guard("{}").unwrap();
        assert!(g.is_empty());
        assert_eq!(
            code(g.place(&order(OrderType::Market, 1_000_000, None))),
            None
        );
    }

    #[test]
    fn the_read_only_mode_refuses_every_command_by_name() {
        let g = guard(r#"{"read_only": true}"#).unwrap();
        let e = g.place(&order(OrderType::Limit, 1, Some(100))).unwrap_err();
        assert_eq!(
            e.to_string(),
            "rejected: read_only: place refused: the client is read-only (read_only), nothing was sent"
        );
        assert_eq!(code(g.command("cancel")), Some(READ_ONLY.into()));
    }

    #[test]
    fn the_caps_count_the_quantity_and_the_value_of_one_order() {
        let g = guard(r#"{"max_order_qty": 100, "max_order_value": "25000"}"#).unwrap();
        assert_eq!(
            code(g.place(&order(OrderType::Limit, 100, Some(250)))),
            None
        );
        assert_eq!(
            code(g.place(&order(OrderType::Limit, 101, Some(1)))),
            Some(MAX_ORDER_QTY.into())
        );
        assert_eq!(
            code(g.place(&order(OrderType::Limit, 100, Some(251)))),
            Some(MAX_ORDER_VALUE.into())
        );
        // A market order has no price: with the value capped it is refused, not let through.
        assert_eq!(
            code(g.place(&order(OrderType::Market, 1, None))),
            Some(MAX_ORDER_VALUE.into())
        );
    }

    #[test]
    fn a_stop_order_is_counted_by_its_limit_price_else_by_its_trigger() {
        let g = guard(r#"{"max_order_value": 1000}"#).unwrap();
        let stop = |limit: Option<i64>| PlaceStopOrderRequest {
            cid: ClientOrderId::new_v4(),
            instrument: sber(),
            side: Side::Sell,
            kind: StopOrderKind::StopLoss,
            qty: Qty::from(10),
            stop_price: Price::from(90),
            limit_price: limit.map(Price::from),
            expiry: StopExpiry::GoodTillCancel,
        };
        assert_eq!(code(g.place_stop(&stop(None))), None);
        assert_eq!(
            code(g.place_stop(&stop(Some(101)))),
            Some(MAX_ORDER_VALUE.into())
        );
    }

    #[test]
    fn a_limit_that_is_not_a_positive_number_is_a_config_error_with_its_name() {
        for bad in [
            r#"{"max_order_qty": 0}"#,
            r#"{"max_order_qty": "-1"}"#,
            r#"{"max_order_qty": "many"}"#,
            r#"{"max_order_qty": true}"#,
        ] {
            let e = guard(bad).unwrap_err();
            assert!(
                matches!(e, Error::Config(_)) && e.to_string().contains("max_order_qty"),
                "{bad}: {e}"
            );
        }
        assert!(
            guard(r#"{"read_only": "yes"}"#)
                .unwrap_err()
                .to_string()
                .contains("read_only")
        );
    }

    #[test]
    fn the_stricter_of_the_setting_and_the_environment_holds() {
        assert_eq!(
            stricter(Some(Decimal::from(5)), Some(Decimal::from(3))),
            Some(Decimal::from(3))
        );
        assert_eq!(
            stricter(None, Some(Decimal::from(3))),
            Some(Decimal::from(3))
        );
        assert_eq!(
            stricter(Some(Decimal::from(5)), None),
            Some(Decimal::from(5))
        );
    }
}
