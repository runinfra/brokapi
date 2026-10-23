//! Orders: the state words of the exchange and the snapshot of an order.

use std::time::Instant;

use brokapi_bybit_protocol::http::models::OrderRecord;
use brokapi_core::{
    BrokerOrderId, ClientOrderId, Decimal, OrderAck, OrderSnapshot, OrderState, OrderType, Price,
    Qty,
};

use super::{broker_ts, side_of, timestamps};
use crate::util::instrument_id;

/// The state of an order by the status of the exchange.
///
/// `New` and the transient `Triggered` are working orders. A conditional order that has not fired (`Untriggered`)
/// is working too, as far as this function goes: the connectors keep it in the list of stop orders (see
/// [`is_untriggered`]). `Cancelled` and `Deactivated` are final even when part of the order was filled before.
/// A word the connector does not know is a working order, so the bot does not place a second one in its stead.
pub fn state_of(
    status: &str,
    cid: ClientOrderId,
    broker_id: &str,
    qty: Qty,
    filled: Qty,
    reject_reason: &str,
) -> OrderState {
    match status {
        "Filled" => OrderState::Filled { cid },
        "Cancelled" | "PartiallyFilledCanceled" | "Deactivated" => OrderState::Cancelled { cid },
        "Rejected" => OrderState::Rejected {
            cid,
            reason: if reject_reason.is_empty() || reject_reason == "EC_NoError" {
                "rejected by the exchange".to_string()
            } else {
                reject_reason.to_string()
            },
        },
        _ if filled.is_positive() => OrderState::PartiallyFilled {
            cid,
            filled,
            left: Qty::new((qty.decimal() - filled.decimal()).max(Decimal::ZERO)),
        },
        _ => OrderState::Acked {
            cid,
            broker_id: BrokerOrderId::new(broker_id),
        },
    }
}

/// Whether the order is a conditional order that has not fired.
pub fn is_untriggered(r: &OrderRecord) -> bool {
    r.order_status == "Untriggered"
}

/// Whether the order is closed for good.
pub fn is_final(r: &OrderRecord) -> bool {
    matches!(
        r.order_status.as_str(),
        "Filled" | "Cancelled" | "PartiallyFilledCanceled" | "Deactivated" | "Rejected"
    )
}

/// The snapshot of an order. `category` names the instrument when the record does not.
pub fn order_snapshot(
    r: &OrderRecord,
    cid: ClientOrderId,
    category: &str,
    now: Instant,
) -> Option<OrderSnapshot> {
    let qty = Qty::new(r.qty);
    let filled = Qty::new(r.cum_exec_qty);
    let side = side_of(&r.side)?;
    let order_type = if r.order_type == "Market" {
        OrderType::Market
    } else {
        OrderType::Limit
    };
    let category = if r.category.is_empty() {
        category
    } else {
        &r.category
    };
    let broker_ts = broker_ts(r.updated_time).or_else(|| broker_ts(r.created_time));
    Some(
        OrderSnapshot::new(
            cid,
            instrument_id(&r.symbol, category),
            state_of(
                &r.order_status,
                cid,
                &r.order_id,
                qty,
                filled,
                &r.reject_reason,
            ),
            side,
            order_type,
            qty,
            timestamps(now, broker_ts),
        )
        .with_broker_id(Some(BrokerOrderId::new(r.order_id.clone())))
        .with_filled(filled)
        .with_price(r.price.filter(|p| !p.is_zero()).map(Price::new)),
    )
}

/// The ack of a command.
pub fn order_ack(cid: ClientOrderId, order_id: &str, now: Instant) -> OrderAck {
    OrderAck {
        cid,
        broker_id: BrokerOrderId::new(order_id),
        ts: timestamps(now, None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn rec(json: &str) -> OrderRecord {
        serde_json::from_str(json).unwrap()
    }

    #[test]
    fn working_orders_are_acked_or_partly_filled() {
        let cid = Uuid::new_v4();
        let r = rec(
            r#"{"orderId":"77","symbol":"BTCUSDT","orderType":"Limit","side":"Buy","orderStatus":"New","qty":"0.20","cumExecQty":"0.00","price":"100.5"}"#,
        );
        let s = order_snapshot(&r, cid, "linear", Instant::now()).unwrap();
        assert!(matches!(s.state, OrderState::Acked { .. }));
        assert_eq!(s.price.unwrap().to_string(), "100.5");
        assert_eq!(s.broker_id.unwrap().as_str(), "77");
        let r = rec(
            r#"{"orderId":"77","symbol":"BTCUSDT","orderType":"Limit","side":"Buy","orderStatus":"PartiallyFilled","qty":"0.20","cumExecQty":"0.05"}"#,
        );
        let s = order_snapshot(&r, cid, "linear", Instant::now()).unwrap();
        match s.state {
            OrderState::PartiallyFilled { filled, left, .. } => {
                assert_eq!(
                    (filled.to_string(), left.to_string()),
                    ("0.05".into(), "0.15".into())
                );
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn final_states_follow_the_status() {
        let cid = Uuid::new_v4();
        let st = |s: &str| state_of(s, cid, "1", Qty::from(1), Qty::ZERO, "");
        assert!(matches!(st("Filled"), OrderState::Filled { .. }));
        assert!(matches!(st("Cancelled"), OrderState::Cancelled { .. }));
        assert!(matches!(st("Deactivated"), OrderState::Cancelled { .. }));
        assert!(matches!(st("Rejected"), OrderState::Rejected { .. }));
        // A word the connector does not know is a working order.
        assert!(matches!(st("SomethingNew"), OrderState::Acked { .. }));
        // A cancelled order that was filled in part is still cancelled, and keeps the filled quantity.
        let r = rec(
            r#"{"orderId":"1","symbol":"BTCUSDT","side":"Buy","orderStatus":"Cancelled","qty":"1","cumExecQty":"0.4"}"#,
        );
        let s = order_snapshot(&r, cid, "linear", Instant::now()).unwrap();
        assert!(matches!(s.state, OrderState::Cancelled { .. }));
        assert_eq!(s.filled.to_string(), "0.4");
    }

    #[test]
    fn the_reject_reason_is_kept_unless_there_is_none() {
        let cid = Uuid::new_v4();
        let reason = |r: &str| match state_of("Rejected", cid, "1", Qty::from(1), Qty::ZERO, r) {
            OrderState::Rejected { reason, .. } => reason,
            other => panic!("{other:?}"),
        };
        assert_eq!(reason("EC_PerCancelRequest"), "EC_PerCancelRequest");
        assert_eq!(reason("EC_NoError"), "rejected by the exchange");
    }

    #[test]
    fn a_record_with_a_side_the_connector_cannot_read_is_skipped() {
        let r =
            rec(r#"{"orderId":"1","symbol":"BTCUSDT","side":"","orderStatus":"New","qty":"1"}"#);
        assert!(order_snapshot(&r, Uuid::new_v4(), "linear", Instant::now()).is_none());
    }
}
