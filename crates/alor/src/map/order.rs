//! Order mapping: ALOR orders to `brokapi-core` order states and snapshots.

use std::time::Instant;

use brokapi_alor_protocol::http::models::OrderRecord;
use brokapi_core::{
    BrokerOrderId, ClientOrderId, OrderAck, OrderSnapshot, OrderState, OrderType, Price, Qty,
};

use super::market::side_of;
use super::{parse_time, timestamps};
use crate::util::instrument_id;

/// The state of an order from the `status` of the broker and the quantities.
///
/// | `status` | State |
/// |---|---|
/// | `working`, nothing filled | `Acked` |
/// | `working`, partly filled | `PartiallyFilled` |
/// | `filled` | `Filled` |
/// | `canceled` | `Cancelled` |
/// | `rejected` | `Rejected` |
/// | anything else (new words of the broker) | `Acked`, a working order: the safe reading, the bot must not place a second one in its stead |
pub fn state_of(
    status: Option<&str>,
    cid: ClientOrderId,
    broker_id: &str,
    qty: Qty,
    filled: Qty,
) -> OrderState {
    match status.map(str::to_ascii_lowercase).as_deref() {
        Some("filled") => OrderState::Filled { cid },
        Some("canceled") | Some("cancelled") => OrderState::Cancelled { cid },
        Some("rejected") => OrderState::Rejected {
            cid,
            reason: "rejected by the broker or the exchange".to_string(),
        },
        _ if filled.is_positive() => OrderState::PartiallyFilled {
            cid,
            filled,
            left: Qty::new((qty.decimal() - filled.decimal()).max(brokapi_core::Decimal::ZERO)),
        },
        _ => OrderState::Acked {
            cid,
            broker_id: BrokerOrderId::new(broker_id),
        },
    }
}

/// A snapshot of an order of the portfolio. `None` if the record has no pieces quantity.
pub fn order_snapshot(r: &OrderRecord, cid: ClientOrderId, now: Instant) -> Option<OrderSnapshot> {
    let qty = Qty::new(r.qty_units?);
    let filled = Qty::new(r.filled_qty_units.unwrap_or_default());
    let side = side_of(r.side.as_deref())?;
    let order_type = match r.kind.as_deref() {
        Some("market") => OrderType::Market,
        _ => OrderType::Limit,
    };
    let instrument = instrument_id(&r.symbol, r.board.as_deref().unwrap_or_default());
    let broker_ts =
        parse_time(r.update_time.as_deref()).or_else(|| parse_time(r.trans_time.as_deref()));
    Some(
        OrderSnapshot::new(
            cid,
            instrument,
            state_of(r.status.as_deref(), cid, &r.id, qty, filled),
            side,
            order_type,
            qty,
            timestamps(now, broker_ts),
        )
        .with_broker_id(Some(BrokerOrderId::new(r.id.clone())))
        .with_filled(filled)
        .with_price(r.price.filter(|p| !p.is_zero()).map(Price::new)),
    )
}

/// The ack of a command: the cid and the number of the order the broker made.
pub fn order_ack(cid: ClientOrderId, order_number: &str, now: Instant) -> OrderAck {
    OrderAck {
        cid,
        broker_id: BrokerOrderId::new(order_number),
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
            r#"{"id":"77","symbol":"SBER","board":"TQBR","type":"limit","side":"buy","status":"working","qtyUnits":20,"filledQtyUnits":0,"price":100.5}"#,
        );
        let s = order_snapshot(&r, cid, Instant::now()).unwrap();
        assert!(matches!(s.state, OrderState::Acked { .. }));
        assert_eq!(s.price.unwrap().to_string(), "100.5");
        assert_eq!(s.broker_id.unwrap().as_str(), "77");

        let r = rec(
            r#"{"id":"77","symbol":"SBER","board":"TQBR","type":"limit","side":"buy","status":"working","qtyUnits":20,"filledQtyUnits":10}"#,
        );
        let s = order_snapshot(&r, cid, Instant::now()).unwrap();
        match s.state {
            OrderState::PartiallyFilled { filled, left, .. } => {
                assert_eq!((filled, left), (Qty::from(10), Qty::from(10)));
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn final_states_follow_the_status() {
        let cid = Uuid::new_v4();
        let st = |s: &str| state_of(Some(s), cid, "1", Qty::from(1), Qty::ZERO);
        assert!(matches!(st("filled"), OrderState::Filled { .. }));
        assert!(matches!(st("canceled"), OrderState::Cancelled { .. }));
        assert!(matches!(st("rejected"), OrderState::Rejected { .. }));
        // A word the connector does not know is a working order.
        assert!(matches!(st("something_new"), OrderState::Acked { .. }));
    }

    #[test]
    fn a_record_without_pieces_is_skipped() {
        let r = rec(r#"{"id":"1","symbol":"SBER","side":"buy","status":"working","qty":2}"#);
        assert!(order_snapshot(&r, Uuid::new_v4(), Instant::now()).is_none());
    }
}
