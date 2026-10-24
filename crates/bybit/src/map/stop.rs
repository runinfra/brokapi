//! Stop orders: conditional orders of the exchange.
//!
//! Bybit has no separate stop order method for category linear. A **conditional order** is an ordinary order with a
//! `triggerPrice`: it waits as `Untriggered` and becomes a working order when the price crosses the trigger. The shared
//! kinds map as follows.
//!
//! | Kind | Order | Trigger direction |
//! |---|---|---|
//! | `StopLoss` | market | a sell waits for a fall, a buy for a rise |
//! | `TakeProfit` | market | a sell waits for a rise, a buy for a fall |
//! | `StopLimit` | limit at `limit_price` | as `StopLoss` |
//!
//! The take profit and stop loss that the exchange keeps on a *position* (`/v5/position/trading-stop`) are conditional
//! orders too: they are listed here, with a made-up cid, but this connector does not create them (they need an open
//! position and cannot carry a client order id).

use std::time::Instant;

use brokapi_bybit_protocol::http::models::{CreateOrderRequest, OrderRecord};
use brokapi_core::{
    BrokerOrderId, ClientOrderId, Error, PlaceStopOrderRequest, Price, Qty, Side, StopExpiry,
    StopOrderKind, StopOrderSnapshot, StopOrderState,
};

use super::{broker_ts, side_of, side_word, timestamps};
use crate::util::instrument_id;

/// The trigger direction of the API: `1` when the order fires as the price rises to the trigger price, `2` when it
/// falls to it.
pub fn trigger_direction(kind: StopOrderKind, side: Side) -> i64 {
    if kind.triggers_on_rise(side) { 1 } else { 2 }
}

/// The request for a stop order.
///
/// # Errors
///
/// [`Error::Rejected`] with code `missing_price` for a stop-limit without `limit_price`, and with code
/// `unsupported_expiry` when the expiry is not good-till-cancel: the exchange keeps a conditional order until it fires or
/// is cancelled, and has no time limit to give it.
pub fn build(
    req: &PlaceStopOrderRequest,
    symbol: &str,
    category: &str,
    cid: ClientOrderId,
) -> Result<CreateOrderRequest, Error> {
    if req.expiry != StopExpiry::GoodTillCancel {
        return Err(Error::Rejected {
            code: "unsupported_expiry".into(),
            message: "Bybit keeps a conditional order until it fires or is cancelled: use StopExpiry::GoodTillCancel"
                .into(),
        });
    }
    let limit = match (req.kind, req.limit_price) {
        (StopOrderKind::StopLimit, None) => {
            return Err(Error::Rejected {
                code: "missing_price".into(),
                message: "a stop-limit order requires limit_price".into(),
            });
        }
        (StopOrderKind::StopLimit, limit) => limit,
        // A stop loss or a take profit is a market order when it fires: a limit price is not used.
        (_, _) => None,
    };
    Ok(CreateOrderRequest {
        category: category.to_string(),
        symbol: symbol.to_string(),
        side: side_word(req.side).to_string(),
        order_type: if limit.is_some() { "Limit" } else { "Market" }.to_string(),
        qty: req.qty.decimal(),
        price: limit.map(Price::decimal),
        time_in_force: Some("GTC".into()),
        order_link_id: Some(cid.hyphenated().to_string()),
        trigger_price: Some(req.stop_price.decimal()),
        trigger_direction: Some(trigger_direction(req.kind, req.side)),
        trigger_by: Some("LastPrice".into()),
        position_idx: Some(0),
        ..Default::default()
    })
}

/// The state of a conditional order by the status of the exchange.
pub fn state_of(status: &str) -> StopOrderState {
    match status {
        "Untriggered" => StopOrderState::Active,
        "Triggered" | "New" | "PartiallyFilled" | "Filled" => StopOrderState::Triggered,
        "Cancelled" | "Deactivated" | "PartiallyFilledCanceled" => StopOrderState::Cancelled,
        other => StopOrderState::Other(other.to_string()),
    }
}

/// The snapshot of a conditional order.
pub fn snapshot(
    r: &OrderRecord,
    cid: Option<ClientOrderId>,
    category: &str,
    now: Instant,
) -> Option<StopOrderSnapshot> {
    let side = side_of(&r.side)?;
    let stop_price = Price::new(r.trigger_price.filter(|p| !p.is_zero())?);
    let limit = r.order_type == "Limit";
    let rises = r.trigger_direction == 1;
    let kind = if limit {
        StopOrderKind::StopLimit
    } else if rises == (side == Side::Buy) {
        StopOrderKind::StopLoss
    } else {
        StopOrderKind::TakeProfit
    };
    let category = if r.category.is_empty() {
        category
    } else {
        &r.category
    };
    Some(StopOrderSnapshot {
        cid,
        broker_id: BrokerOrderId::new(r.order_id.clone()),
        instrument: instrument_id(&r.symbol, category),
        side,
        kind,
        qty: Qty::new(r.qty),
        stop_price,
        limit_price: if limit { r.price.map(Price::new) } else { None },
        expiry: StopExpiry::GoodTillCancel,
        state: state_of(&r.order_status),
        ts: timestamps(
            now,
            broker_ts(r.updated_time).or_else(|| broker_ts(r.created_time)),
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use brokapi_core::{Decimal, dec};
    use uuid::Uuid;

    fn request(kind: StopOrderKind, side: Side, limit: Option<Price>) -> PlaceStopOrderRequest {
        PlaceStopOrderRequest {
            cid: Uuid::new_v4(),
            instrument: instrument_id("BTCUSDT", "linear"),
            side,
            kind,
            qty: Qty::new(dec!(0.2)),
            stop_price: Price::new(dec!(80000.5)),
            limit_price: limit,
            expiry: StopExpiry::GoodTillCancel,
        }
    }

    #[test]
    fn a_sell_stop_loss_waits_for_a_fall_and_a_sell_take_profit_for_a_rise() {
        let b = |k, s, l| build(&request(k, s, l), "BTCUSDT", "linear", Uuid::new_v4()).unwrap();
        let w = b(StopOrderKind::StopLoss, Side::Sell, None);
        assert_eq!(
            (w.trigger_direction, w.order_type.as_str()),
            (Some(2), "Market")
        );
        let w = b(StopOrderKind::TakeProfit, Side::Sell, None);
        assert_eq!(w.trigger_direction, Some(1));
        let w = b(StopOrderKind::StopLoss, Side::Buy, None);
        assert_eq!(w.trigger_direction, Some(1));
        let w = b(
            StopOrderKind::StopLimit,
            Side::Sell,
            Some(Price::new(dec!(80000))),
        );
        assert_eq!(
            (w.order_type.as_str(), w.price),
            ("Limit", Some(dec!(80000)))
        );
        assert_eq!(w.trigger_price, Some(dec!(80000.5)));
        assert_eq!(w.order_link_id.as_ref().map(String::len), Some(36));
    }

    #[test]
    fn a_stop_limit_without_a_price_and_a_time_limit_are_refused() {
        let err = build(
            &request(StopOrderKind::StopLimit, Side::Sell, None),
            "BTCUSDT",
            "linear",
            Uuid::new_v4(),
        )
        .unwrap_err();
        assert!(matches!(err, Error::Rejected { ref code, .. } if code == "missing_price"));
        let mut req = request(StopOrderKind::StopLoss, Side::Sell, None);
        req.expiry = StopExpiry::EndOfDay;
        let err = build(&req, "BTCUSDT", "linear", Uuid::new_v4()).unwrap_err();
        assert!(matches!(err, Error::Rejected { ref code, .. } if code == "unsupported_expiry"));
    }

    #[test]
    fn a_listed_conditional_order_reads_back_with_its_kind() {
        let r: OrderRecord = serde_json::from_str(
            r#"{"orderId":"9","symbol":"BTCUSDT","side":"Sell","orderType":"Market","orderStatus":"Untriggered","qty":"0.2","triggerPrice":"80000.5","triggerDirection":2}"#,
        )
        .unwrap();
        let s = snapshot(&r, None, "linear", Instant::now()).unwrap();
        assert_eq!(s.kind, StopOrderKind::StopLoss);
        assert_eq!(s.stop_price.to_string(), "80000.5");
        assert_eq!(s.state, StopOrderState::Active);
        let r: OrderRecord = serde_json::from_str(
            r#"{"orderId":"9","symbol":"BTCUSDT","side":"Sell","orderType":"Limit","price":"79000","orderStatus":"Triggered","qty":"0.2","triggerPrice":"80000.5","triggerDirection":1}"#,
        )
        .unwrap();
        let s = snapshot(&r, None, "linear", Instant::now()).unwrap();
        assert_eq!(
            (s.kind, s.state),
            (StopOrderKind::StopLimit, StopOrderState::Triggered)
        );
        assert_eq!(s.limit_price.unwrap().to_string(), "79000");
    }
}
