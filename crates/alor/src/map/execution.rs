//! Fill mapping: ALOR trades of the portfolio to `brokapi-core` executions.

use std::time::Instant;

use brokapi_alor_protocol::http::models::TradeRecord;
use brokapi_core::{BrokerOrderId, ClientOrderId, Execution, Money, Price, Qty};

use super::market::side_of;
use super::{parse_time, timestamps};
use crate::util::instrument_id;

/// A fill. `cid` is the client order id the connector found for the order of the trade (`None` for an order that
/// is not ours). `None` if the record has no pieces quantity or no side.
pub fn execution(r: &TradeRecord, cid: Option<ClientOrderId>, now: Instant) -> Option<Execution> {
    let qty = Qty::new(r.qty_units?);
    Some(Execution {
        cid,
        broker_order_id: BrokerOrderId::new(r.orderno.clone().unwrap_or_default()),
        instrument: instrument_id(&r.symbol, r.board.as_deref().unwrap_or_default()),
        side: side_of(r.side.as_deref())?,
        price: Price::new(r.price),
        qty,
        commission: Money::new(r.commission.unwrap_or_default()),
        trade_id: r.id.clone(),
        ts: timestamps(now, parse_time(r.date.as_deref())),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_trade_becomes_an_execution_in_pieces() {
        let r: TradeRecord = serde_json::from_str(
            r#"{"id":"T1","orderno":"77","symbol":"SBER","board":"TQBR","qtyUnits":20,"qty":2,"price":100.50,"side":"buy","commission":0.05,"date":"2026-10-22T10:00:00Z"}"#,
        )
        .unwrap();
        let e = execution(&r, None, Instant::now()).unwrap();
        assert_eq!(e.qty, Qty::from(20));
        assert_eq!(e.price.to_string(), "100.50");
        assert_eq!(e.trade_id, "T1");
        assert_eq!(e.broker_order_id.as_str(), "77");
        assert_eq!(e.commission.to_string(), "0.05");
    }
}
