//! Execution history mapping.

use brokapi_core::Decimal;
use std::time::Instant;

use brokapi_bcs_protocol::http::models::TradeRecord;
use brokapi_bcs_protocol::models::enums::ExecutionType;
use brokapi_bcs_protocol::ws::models::OrderMessage;
use brokapi_core::{BrokerOrderId, Execution, Money, Price, Qty, Side};
use uuid::Uuid;

use super::{instrument_id_ticker, parse_broker_ts, timestamps, unix_nanos};

/// A trade of the history (`POST /trades/search`). The docs describe a flat record (`tradeNum`,
/// `orderNum`, `tradeQuantity`, `tradeDateTime`, ...) without our `clientOrderId` and without a
/// commission: `cid` is `None`, `commission` 0. The older `data` form is still read if it comes.
pub fn map_trade_record(record: &TradeRecord, now: Instant) -> Option<Execution> {
    let Some(data) = record.data.as_ref() else {
        return map_flat_trade(record, now);
    };
    let ticker = data.ticker.as_deref()?;
    let class_code = data.class_code.as_deref()?;
    let order_id = data.order_id.as_ref()?;
    let execution_id = data.execution_id.as_ref()?;
    let price = data.price?;
    let qty = data
        .last_quantity
        .or(data.executed_quantity)
        .unwrap_or(Decimal::ZERO);
    let side = match data.side.as_deref() {
        Some("1") | Some("Buy") | Some("BUY") => Side::Buy,
        Some("2") | Some("Sell") | Some("SELL") => Side::Sell,
        // Not guessed: a trade with the wrong side would corrupt the position.
        _ => return None,
    };
    let cid = record
        .client_order_id
        .as_deref()
        .and_then(|s| Uuid::parse_str(s).ok());
    let broker_ts = data
        .transaction_time
        .as_deref()
        .and_then(|s| parse_broker_ts(Some(s)));

    Some(Execution {
        cid,
        broker_order_id: BrokerOrderId::new(order_id.clone()),
        instrument: instrument_id_ticker(ticker, class_code),
        side,
        price: Price::new(price),
        qty: Qty::new(qty),
        commission: Money::new(data.commission.unwrap_or(Decimal::ZERO)),
        trade_id: execution_id.clone(),
        ts: timestamps(now, broker_ts),
    })
}

fn map_flat_trade(record: &TradeRecord, now: Instant) -> Option<Execution> {
    let ticker = record.ticker.as_deref()?;
    let class_code = record.class_code.as_deref()?;
    let trade_num = record.trade_num?;
    let price = record.price?;
    let qty = record.trade_quantity?;
    if qty <= Decimal::ZERO {
        return None;
    }
    // An unknown side is not guessed: a trade with the wrong side would corrupt the position.
    let side = match record.side.as_deref() {
        Some("1") => Side::Buy,
        Some("2") => Side::Sell,
        _ => return None,
    };
    let broker_ts = record
        .trade_date_time
        .as_deref()
        .and_then(|s| parse_broker_ts(Some(s)));
    Some(Execution {
        cid: None,
        broker_order_id: BrokerOrderId::new(
            record.order_num.map(|n| n.to_string()).unwrap_or_default(),
        ),
        instrument: instrument_id_ticker(ticker, class_code),
        side,
        price: Price::new(price),
        qty: Qty::new(qty),
        commission: Money::ZERO,
        trade_id: trade_num.to_string(),
        ts: timestamps(now, broker_ts),
    })
}

/// What the broker had executed in one order after a message: the quantity and the value (price times
/// quantity) of everything filled so far, summed over all fills of the order.
///
/// The `orders/events/ws` messages carry running totals (`executedQuantity`, `executionValue`,
/// `averagePrice`), not the numbers of one fill. The docs sample: `lastQuantity` 50, `executedQuantity`
/// 100, `averagePrice` 244.5, `executionValue` 24450. The price of one fill is the growth of the value
/// divided by the growth of the quantity between two messages of the same order, see [`map_ws_execution`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Cumulative {
    /// Quantity executed so far (`executedQuantity`), in pieces.
    pub qty: Decimal,
    /// Value executed so far, in the currency of the instrument: `executionValue`, else
    /// `averagePrice * executedQuantity`.
    pub value: Decimal,
}

/// The running totals of an order message, or `None` when the message does not carry enough to know
/// them (no `executedQuantity`, or neither `executionValue` nor `averagePrice`).
pub fn cumulative_of(
    data: &brokapi_bcs_protocol::ws::models::OrderExecutionData,
) -> Option<Cumulative> {
    let qty = data.executed_quantity?;
    let value = match (data.execution_value, data.average_price) {
        (Some(v), _) if v > Decimal::ZERO => v,
        (_, Some(avg)) if avg > Decimal::ZERO => avg * qty,
        _ => return None,
    };
    Some(Cumulative { qty, value })
}

/// A fill from the `orders/events/ws` WebSocket. Only messages that carry a fill: `lastQuantity > 0`
/// and an `executionId`, and not a corrected report (`executionType` 13, which restates an earlier
/// message). The order's cid is read as [`crate::map::order::message_cid`] says.
///
/// **The price of the fill.** `before` is the running total of the same order from the previous message
/// the connector saw ([`cumulative_of`]). With it the price is exact:
///
/// - the totals after this message minus `before` give the quantity and the value of this fill alone,
///   and the price is value divided by quantity. A first fill of an order (`before` is `None` and
///   `executedQuantity` equals `lastQuantity`) is the same with `before` of zero. Two fills at
///   different prices give two different prices, not their average.
/// - When the totals do not fit (a message was missed, so the growth of `executedQuantity` is not
///   `lastQuantity`, or the message has no totals), the price is the best the message says:
///   `averagePrice` of the order so far, else the price of the order. That is an average over earlier
///   fills too, so it can be off by the difference between the fills. The price of every trade is in the
///   history (`ExecutionSource::executions`), which is the way to settle it.
pub fn map_ws_execution(
    msg: &OrderMessage,
    now: Instant,
    before: Option<Cumulative>,
) -> Option<Execution> {
    let data = msg.data.as_ref()?;
    // A corrected report (`executionType` 13, "Исправлено") restates an earlier one. It is not a new fill:
    // counting it would count the fill twice. The history of trades settles a real correction.
    if data.execution_type == Some(ExecutionType::Corrected) {
        return None;
    }
    let qty = data.last_quantity;
    if qty <= Decimal::ZERO {
        return None;
    }
    let execution_id = data.execution_id.as_deref().filter(|s| !s.is_empty())?;
    let cid = super::order::message_cid(msg);
    let exact = cumulative_of(data).and_then(|after| {
        let base = match before {
            Some(b) => b,
            // The first message of the order: the totals are those of this fill alone.
            None if after.qty == qty => Cumulative {
                qty: Decimal::ZERO,
                value: Decimal::ZERO,
            },
            None => return None,
        };
        (after.qty - base.qty == qty && after.value > base.value)
            .then(|| (after.value - base.value) / qty)
    });
    let price = exact.unwrap_or_else(|| match data.average_price {
        Some(avg) if avg > Decimal::ZERO => avg,
        _ => data.price,
    });
    // An unknown side is not guessed: a fill with the wrong side would corrupt the position.
    let side = match data.side {
        brokapi_bcs_protocol::models::enums::OrderSide::Buy => Side::Buy,
        brokapi_bcs_protocol::models::enums::OrderSide::Sell => Side::Sell,
        brokapi_bcs_protocol::models::enums::OrderSide::Unknown => return None,
    };
    Some(Execution {
        cid,
        broker_order_id: BrokerOrderId::new(data.order_id.clone().unwrap_or_default()),
        instrument: instrument_id_ticker(&data.ticker, &data.class_code),
        side,
        price: Price::new(price),
        qty: Qty::new(qty),
        commission: Money::new(data.commission.unwrap_or(Decimal::ZERO)),
        trade_id: execution_id.to_string(),
        ts: timestamps(now, data.transaction_time.map(unix_nanos)),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(side: Option<&str>) -> TradeRecord {
        serde_json::from_value(serde_json::json!({
            "data": {
                "ticker": "SBER", "classCode": "TQBR", "orderId": "o1", "executionId": "e1",
                "price": 100.0, "lastQuantity": 2.0, "side": side
            }
        }))
        .unwrap()
    }

    #[test]
    fn history_trade_side_of_the_older_form_is_read_and_never_guessed() {
        let now = Instant::now();
        assert_eq!(
            map_trade_record(&record(Some("BUY")), now).unwrap().side,
            Side::Buy
        );
        assert_eq!(
            map_trade_record(&record(Some("2")), now).unwrap().side,
            Side::Sell
        );
        assert!(map_trade_record(&record(None), now).is_none());
        assert!(map_trade_record(&record(Some("???")), now).is_none());
    }
}
