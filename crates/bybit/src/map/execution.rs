//! Fills.

use std::time::Instant;

use brokapi_bybit_protocol::http::models::ExecutionRecord;
use brokapi_core::{BrokerOrderId, ClientOrderId, Execution, Money, Price, Qty};

use super::{broker_ts, side_of, timestamps};
use crate::util::instrument_id;

/// Whether the record is a fill of an order. The funding payments the exchange lists among the executions
/// (`execType` `Funding`) and the settlement of a delivery are not fills of an order of the bot.
pub fn is_fill(r: &ExecutionRecord) -> bool {
    matches!(
        r.exec_type.as_str(),
        "Trade" | "AdlTrade" | "BustTrade" | ""
    )
}

/// The fill. The commission is the fee as the exchange states it: a rebate is negative. The currency of the fee is the
/// settle coin of the instrument.
pub fn execution(
    r: &ExecutionRecord,
    cid: Option<ClientOrderId>,
    category: &str,
    now: Instant,
) -> Option<Execution> {
    if !is_fill(r) {
        return None;
    }
    let category = if r.category.is_empty() {
        category
    } else {
        &r.category
    };
    Some(Execution {
        cid,
        broker_order_id: BrokerOrderId::new(r.order_id.clone()),
        instrument: instrument_id(&r.symbol, category),
        side: side_of(&r.side)?,
        price: Price::new(r.exec_price),
        qty: Qty::new(r.exec_qty),
        commission: Money::new(r.exec_fee),
        trade_id: r.exec_id.clone(),
        ts: timestamps(now, broker_ts(r.exec_time)),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_fill_becomes_an_execution() {
        let r: ExecutionRecord = serde_json::from_str(
            r#"{"execId":"e1","orderId":"77","symbol":"BTCUSDT","side":"Buy","execPrice":"100.50","execQty":"0.2","execFee":"0.011","execType":"Trade","execTime":"1672282722429"}"#,
        )
        .unwrap();
        let e = execution(&r, None, "linear", Instant::now()).unwrap();
        assert_eq!(e.qty.to_string(), "0.2");
        assert_eq!(e.price.to_string(), "100.50");
        assert_eq!(e.trade_id, "e1");
        assert_eq!(e.broker_order_id.as_str(), "77");
        assert_eq!(e.commission.to_string(), "0.011");
        assert_eq!(
            e.ts.broker_ts.unwrap().as_nanos(),
            1_672_282_722_429_000_000
        );
    }

    #[test]
    fn a_funding_payment_is_not_a_fill() {
        let r: ExecutionRecord = serde_json::from_str(
            r#"{"execId":"f1","symbol":"BTCUSDT","side":"Buy","execPrice":"1","execQty":"1","execType":"Funding"}"#,
        )
        .unwrap();
        assert!(execution(&r, None, "linear", Instant::now()).is_none());
    }
}
