//! Execution history mapping.

use brokapi_core::Decimal;
use std::time::Instant;

use brokapi_bcs_protocol::http::models::TradeRecord;
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

/// A fill from the `orders/events/ws` WebSocket. Only messages that carry a fill: `lastQuantity > 0`
/// and an `executionId`. The price of one fill is `executionValue / lastQuantity` when the broker
/// sends the value of the fill, else `averagePrice` (exact for a single fill), else the order price.
/// `clientOrderId` may be empty (the sample in the docs has it empty): then `originalClientOrderId`.
pub fn map_ws_execution(msg: &OrderMessage, now: Instant) -> Option<Execution> {
    let data = msg.data.as_ref()?;
    let qty = data.last_quantity;
    if qty <= Decimal::ZERO {
        return None;
    }
    let execution_id = data.execution_id.as_deref().filter(|s| !s.is_empty())?;
    let cid = Uuid::parse_str(&msg.client_order_id).ok().or_else(|| {
        msg.original_client_order_id
            .as_deref()
            .and_then(|s| Uuid::parse_str(s).ok())
    });
    let price = match (data.execution_value, data.average_price) {
        (Some(v), _) if v > Decimal::ZERO => v / qty,
        (_, Some(avg)) if avg > Decimal::ZERO => avg,
        _ => data.price,
    };
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
