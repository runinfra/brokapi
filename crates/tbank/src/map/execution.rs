//! Own trades: `TradesStream` and operation history.

use std::time::Instant;

use brokapi_core::{
    BrokerOrderId, ClientOrderId, Decimal, Execution, InstrumentId, Money, Price, Qty,
};
use brokapi_tbank_protocol::v1 as pb;

use crate::ids::{id_from_parts, money_of, price_of_money, price_of_quotation, stamps};
use crate::map::order::side;

/// One `Execution` per trade in an `OrderTrades` message. The stream carries no commission (zero) and
/// no `order_request_id`: `cid` comes from the order ids the connector has seen.
pub fn from_order_trades(
    t: &pb::OrderTrades,
    instrument: InstrumentId,
    cid: Option<ClientOrderId>,
    now: Instant,
) -> Vec<Execution> {
    t.trades
        .iter()
        .map(|tr| Execution {
            cid,
            broker_order_id: BrokerOrderId::new(t.order_id.clone()),
            instrument: instrument.clone(),
            side: side(t.direction()),
            price: tr
                .price
                .as_ref()
                .map(price_of_quotation)
                .unwrap_or(Price::ZERO),
            qty: Qty::from(tr.quantity),
            commission: Money::ZERO,
            trade_id: tr.trade_id.clone(),
            ts: stamps(now, tr.date_time.as_ref().or(t.created_at.as_ref())),
        })
        .collect()
}

/// History from `GetOperationsByCursor`: BUY/SELL operations, one `Execution` per trade.
/// `broker_order_id` is the operation id (the history has no order id), `cid` is `None`, the
/// operation commission is spread over its trades in proportion to quantity. Quantities are in
/// pieces here, unlike the lots of the live stream.
pub fn from_operation(op: &pb::OperationItem, now: Instant) -> Vec<Execution> {
    use pb::OperationType as T;
    let side = match op.r#type() {
        T::Buy | T::BuyCard | T::BuyMargin => brokapi_core::Side::Buy,
        T::Sell | T::SellCard | T::SellMargin => brokapi_core::Side::Sell,
        _ => return Vec::new(),
    };
    let Some(info) = &op.trades_info else {
        return Vec::new();
    };
    let instrument = id_from_parts(&op.instrument_uid, &op.figi, &op.ticker, &op.class_code);
    let total_qty: i64 = info.trades.iter().map(|t| t.quantity).sum();
    let commission = op
        .commission
        .as_ref()
        .map(money_of)
        .unwrap_or(Money::ZERO)
        .abs();
    // The commission is split by quantity, in whole billionths (the broker's own precision). The
    // last trade takes the rest, so the parts add up to the commission exactly.
    let mut left = commission;
    let last = info.trades.len().saturating_sub(1);
    info.trades
        .iter()
        .enumerate()
        .map(|(i, tr)| {
            let share = if total_qty <= 0 {
                Money::ZERO
            } else if i == last {
                left
            } else {
                let part =
                    commission.decimal() * Decimal::from(tr.quantity) / Decimal::from(total_qty);
                Money::new(part.trunc_with_scale(9))
            };
            left -= share;
            (tr, share)
        })
        .map(|(tr, share)| Execution {
            cid: None,
            broker_order_id: BrokerOrderId::new(op.id.clone()),
            instrument: instrument.clone(),
            side,
            price: tr.price.as_ref().map(price_of_money).unwrap_or(Price::ZERO),
            qty: Qty::from(tr.quantity),
            commission: share,
            trade_id: tr.num.clone(),
            ts: stamps(now, tr.date.as_ref()),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use brokapi_core::dec;
    use brokapi_core::{InstrumentKey, NativeId, Side};
    use brokapi_tbank_protocol::money::{money, quotation};

    #[test]
    fn stream_trades_one_execution_each() {
        let t = pb::OrderTrades {
            order_id: "55".into(),
            direction: pb::OrderDirection::Sell as i32,
            trades: vec![
                pb::OrderTrade {
                    price: Some(quotation("100.5")),
                    quantity: 2,
                    trade_id: "t1".into(),
                    ..Default::default()
                },
                pb::OrderTrade {
                    price: Some(quotation("100.6")),
                    quantity: 3,
                    trade_id: "t2".into(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let inst = InstrumentId::new(
            InstrumentKey::new("SBER", "TQBR"),
            NativeId::Uid("u".into()),
        );
        let cid = ClientOrderId::from_bytes([7; 16]);
        let ex = from_order_trades(&t, inst, Some(cid), Instant::now());
        assert_eq!(ex.len(), 2);
        assert_eq!(
            (ex[0].side, ex[0].price.decimal(), ex[0].qty.decimal()),
            (Side::Sell, dec!(100.5), dec!(2.0))
        );
        assert_eq!(ex[1].trade_id, "t2");
        assert_eq!(ex[0].cid, Some(cid));
        assert_eq!(ex[0].broker_order_id.as_str(), "55");
    }

    #[test]
    fn operation_commission_is_spread_by_quantity() {
        let op = pb::OperationItem {
            id: "op1".into(),
            r#type: pb::OperationType::Buy as i32,
            ticker: "SBER".into(),
            class_code: "TQBR".into(),
            instrument_uid: "u".into(),
            commission: Some(money("rub", "-4.0")),
            trades_info: Some(pb::OperationItemTrades {
                trades: vec![
                    pb::OperationItemTrade {
                        num: "a".into(),
                        quantity: 1,
                        price: Some(money("rub", "10.0")),
                        ..Default::default()
                    },
                    pb::OperationItemTrade {
                        num: "b".into(),
                        quantity: 3,
                        price: Some(money("rub", "11.0")),
                        ..Default::default()
                    },
                ],
            }),
            ..Default::default()
        };
        let ex = from_operation(&op, Instant::now());
        assert_eq!(ex.len(), 2);
        assert_eq!(
            (ex[0].commission.decimal(), ex[1].commission.decimal()),
            (dec!(1.0), dec!(3.0))
        );
        // The parts add up to the commission exactly.
        assert_eq!(ex[0].commission + ex[1].commission, Money::new(dec!(4)));
        assert_eq!(ex[0].side, Side::Buy);
    }

    #[test]
    fn non_trade_operations_are_skipped() {
        let op = pb::OperationItem {
            r#type: pb::OperationType::Dividend as i32,
            ..Default::default()
        };
        assert!(from_operation(&op, Instant::now()).is_empty());
    }
}
