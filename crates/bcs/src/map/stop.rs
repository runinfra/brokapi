//! Stop orders: `PlaceStopOrderRequest` -> BCS conditional order (`orderType = "5"`).
//!
//! The mapping follows the current docs (`POST /orders`, checked 2026-10-19). What the docs do NOT say
//! is settled here in the safest way:
//!
//! - **Trigger direction.** Not a field of the request. The broker decides it, from the side and the
//!   kind (stop-loss of a SELL = price falls to `stopPrice`, take-profit of a SELL = price rises to
//!   `takePrice`). The connector sends no direction and does not guess one.
//! - **Lifetime.** `timeInForce = 3` is "algorithmic orders only", and the docs give no lifetime for a
//!   conditional order. So only `StopExpiry::GoodTillCancel` is accepted and NO time-in-force field is
//!   sent: the order lives as long as the broker's default for conditional orders. If that is shorter
//!   than "until cancelled", the stop disappears at the broker's deadline: the live run must find
//!   out. `EndOfDay` and `GoodTillDate` are refused (`unsupported_expiry`) rather than sent into a
//!   field the docs reserve for another order type.
//! - **`price`.** With `executionStopLossType = 2` / `executionTakeProfitType = 2` (limit execution)
//!   `price` is taken as the limit price of the order put at the trigger. With market execution (`1`)
//!   `price` is not sent.
//! - **Pairs.** One request is one leg. The connector never sends `stopPrice` and `takePrice` together,
//!   so it never creates the TP+SL pair (list type 7) whose cancellation the docs do not describe.
//! - **Quantity.** `orderQuantity` of a conditional order is sent like for plain orders (whole units).

use brokapi_bcs_protocol::http::models::{
    CreateOrderRequest, OrderExtras, OrderType as BcsOrderType,
};
use brokapi_bcs_protocol::models::enums::OrderSide as BcsOrderSide;
use brokapi_core::{Error, PlaceStopOrderRequest, Price, Side, StopExpiry, StopOrderKind};

use crate::util::{class_code, ticker};

fn refused(code: &str, message: impl Into<String>) -> Error {
    Error::Rejected {
        code: code.into(),
        message: message.into(),
    }
}

/// Wire body of a stop order. Everything that can be checked locally is checked before anything is
/// sent or any quota is spent.
pub fn build(req: &PlaceStopOrderRequest) -> Result<CreateOrderRequest, Error> {
    match req.expiry {
        StopExpiry::GoodTillCancel => {}
        StopExpiry::EndOfDay | StopExpiry::GoodTillDate(_) => {
            return Err(refused(
                "unsupported_expiry",
                "BCS documents no lifetime for conditional orders: only GoodTillCancel (no time-in-force is sent)",
            ));
        }
    }
    let qty = match req.qty.to_whole() {
        Some(n) if n >= 1 => n,
        _ => {
            return Err(refused(
                "invalid_qty",
                format!(
                    "stop order quantity must be a whole number >= 1, got {}",
                    req.qty
                ),
            ));
        }
    };
    let stop = req.stop_price;
    if !stop.is_positive() {
        return Err(refused(
            "invalid_price",
            format!("stop price must be > 0, got {stop}"),
        ));
    }
    let limit = match req.limit_price {
        Some(p) if !p.is_positive() => {
            return Err(refused(
                "invalid_price",
                format!("limit price must be > 0, got {p}"),
            ));
        }
        other => other,
    };
    if req.kind == StopOrderKind::StopLimit && limit.is_none() {
        return Err(refused(
            "missing_limit_price",
            "a stop-limit order needs a limit price",
        ));
    }

    // 1 = market, 2 = limit execution at the trigger.
    let execution = if limit.is_some() { 2 } else { 1 };
    let mut extras = OrderExtras::default();
    match req.kind {
        StopOrderKind::StopLoss | StopOrderKind::StopLimit => {
            extras.stop_price = Some(stop.decimal());
            extras.execution_stop_loss_type = Some(execution);
        }
        StopOrderKind::TakeProfit => {
            extras.take_price = Some(stop.decimal());
            extras.execution_take_profit_type = Some(execution);
        }
    }
    let side = match req.side {
        Side::Buy => BcsOrderSide::Buy,
        Side::Sell => BcsOrderSide::Sell,
    };
    Ok(CreateOrderRequest {
        client_order_id: req.cid,
        side,
        order_type: BcsOrderType::Conditional,
        order_quantity: qty,
        ticker: ticker(&req.instrument)?,
        class_code: class_code(&req.instrument)?,
        price: limit.map(Price::decimal),
        extras,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use brokapi_core::{
        ClientOrderId, InstrumentId, InstrumentKey, NativeId, Price, Qty, UnixNanos,
    };
    use brokapi_core::{Decimal, dec};

    fn sber() -> InstrumentId {
        InstrumentId::new(
            InstrumentKey::new("SBER", "TQBR"),
            NativeId::TickerClass {
                ticker: "SBER".into(),
                class_code: "TQBR".into(),
            },
        )
    }

    fn req(
        kind: StopOrderKind,
        side: Side,
        limit: Option<Decimal>,
        expiry: StopExpiry,
    ) -> PlaceStopOrderRequest {
        PlaceStopOrderRequest {
            cid: ClientOrderId::from_bytes([5; 16]),
            instrument: sber(),
            side,
            kind,
            qty: Qty::from(3),
            stop_price: Price::from(250),
            limit_price: limit.map(Price::new),
            expiry,
        }
    }

    fn json(r: &CreateOrderRequest) -> serde_json::Value {
        serde_json::to_value(r).unwrap()
    }

    #[test]
    fn market_stop_loss_is_a_conditional_order_with_stop_price_only() {
        let w = json(
            &build(&req(
                StopOrderKind::StopLoss,
                Side::Sell,
                None,
                StopExpiry::GoodTillCancel,
            ))
            .unwrap(),
        );
        assert_eq!(w["orderType"], "5");
        assert_eq!(w["side"], "2");
        assert_eq!(w["stopPrice"], 250.0);
        assert_eq!(w["executionStopLossType"], 1);
        assert!(
            w.get("price").is_none(),
            "market execution sends no price: {w}"
        );
        assert!(w.get("takePrice").is_none());
        assert!(
            w.get("timeInForce").is_none() && w.get("expiryDate").is_none(),
            "no lifetime is sent: {w}"
        );
        assert_eq!(w["orderQuantity"], 3);
    }

    #[test]
    fn stop_limit_and_limit_stop_loss_send_the_limit_price() {
        for kind in [StopOrderKind::StopLimit, StopOrderKind::StopLoss] {
            let w = json(
                &build(&req(
                    kind,
                    Side::Sell,
                    Some(dec!(249.0)),
                    StopExpiry::GoodTillCancel,
                ))
                .unwrap(),
            );
            assert_eq!(w["executionStopLossType"], 2);
            assert_eq!(w["price"], 249.0);
            assert_eq!(w["stopPrice"], 250.0);
        }
    }

    #[test]
    fn take_profit_uses_take_price_not_stop_price() {
        let w = json(
            &build(&req(
                StopOrderKind::TakeProfit,
                Side::Sell,
                None,
                StopExpiry::GoodTillCancel,
            ))
            .unwrap(),
        );
        assert_eq!(w["takePrice"], 250.0);
        assert_eq!(w["executionTakeProfitType"], 1);
        assert!(
            w.get("stopPrice").is_none() && w.get("executionStopLossType").is_none(),
            "one leg only: {w}"
        );
        let w = json(
            &build(&req(
                StopOrderKind::TakeProfit,
                Side::Buy,
                Some(dec!(240.0)),
                StopExpiry::GoodTillCancel,
            ))
            .unwrap(),
        );
        assert_eq!(w["executionTakeProfitType"], 2);
        assert_eq!(w["price"], 240.0);
    }

    #[test]
    fn refused_before_anything_is_sent() {
        let t = UnixNanos::from_nanos(1);
        for (r, code) in [
            (
                req(
                    StopOrderKind::StopLimit,
                    Side::Sell,
                    None,
                    StopExpiry::GoodTillCancel,
                ),
                "missing_limit_price",
            ),
            (
                req(
                    StopOrderKind::StopLoss,
                    Side::Sell,
                    None,
                    StopExpiry::EndOfDay,
                ),
                "unsupported_expiry",
            ),
            (
                req(
                    StopOrderKind::StopLoss,
                    Side::Sell,
                    None,
                    StopExpiry::GoodTillDate(t),
                ),
                "unsupported_expiry",
            ),
            (
                req(
                    StopOrderKind::StopLoss,
                    Side::Sell,
                    Some(dec!(-1.0)),
                    StopExpiry::GoodTillCancel,
                ),
                "invalid_price",
            ),
        ] {
            match build(&r) {
                Err(Error::Rejected { code: got, .. }) => assert_eq!(got, code),
                other => panic!("{code}: {other:?}"),
            }
        }
        let mut frac = req(
            StopOrderKind::StopLoss,
            Side::Sell,
            None,
            StopExpiry::GoodTillCancel,
        );
        frac.qty = Qty::new(dec!(1.5));
        assert!(matches!(build(&frac), Err(Error::Rejected { code, .. }) if code == "invalid_qty"));
        let mut bad_stop = req(
            StopOrderKind::StopLoss,
            Side::Sell,
            None,
            StopExpiry::GoodTillCancel,
        );
        bad_stop.stop_price = Price::from(0);
        assert!(
            matches!(build(&bad_stop), Err(Error::Rejected { code, .. }) if code == "invalid_price")
        );
    }
}
