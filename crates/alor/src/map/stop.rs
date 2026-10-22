//! Stop orders: the request of the shared interface to ALOR's stop and stop-limit orders, and back.
//!
//! ALOR has two kinds of stop orders, and both keep the trigger price and the condition (`more`, `less`, ...):
//! a stop order (a **market** order is made on trigger) and a stop-limit order (a **limit** order is made, with
//! its own price). The shared interface has three kinds:
//!
//! | Request | ALOR |
//! |---|---|
//! | `StopLoss`, `TakeProfit` without `limit_price` | a stop order. The condition follows the side and the kind (a sell stop-loss triggers when the price falls to the trigger price, a sell take-profit when it rises to it) |
//! | `StopLoss`, `TakeProfit` with `limit_price`, `StopLimit` | a stop-limit order (a `StopLimit` without `limit_price` is refused) |
//!
//! A stop order read back has the kind `StopLoss` or `TakeProfit` (from the side and the condition) and a
//! stop-limit order has the kind `StopLimit`: ALOR does not keep which of the two a bot meant.

use std::time::Instant;

use brokapi_alor_protocol::http::models::{
    InstrumentRef, StopOrderRecord, StopOrderRequest, UserRef,
};
use brokapi_core::{
    BrokerOrderId, ClientOrderId, Error, PlaceStopOrderRequest, Price, Qty, Side, StopExpiry,
    StopOrderKind, StopOrderSnapshot, StopOrderState,
};

use super::market::side_of;
use super::{parse_time, pieces_to_lots, timestamps};
use crate::cid::cid_from_comment;
use crate::util::{instrument_id, nanos_to_secs, ticker_board};

/// The stop orders of ALOR end at a given time; a stop order that is "good till cancel" gets this long.
pub const GTC_HORIZON_SECS: i64 = 365 * 24 * 3600;

/// The trigger condition for the side and the kind.
pub fn condition(kind: StopOrderKind, side: Side) -> &'static str {
    if kind.triggers_on_rise(side) {
        "moreorequal"
    } else {
        "lessorequal"
    }
}

/// The end of the stop order, Unix seconds, from the expiry. `now_secs` is the time now.
pub fn end_time(expiry: StopExpiry, now_secs: i64) -> i64 {
    match expiry {
        StopExpiry::GoodTillCancel => now_secs + GTC_HORIZON_SECS,
        // The end of the Moscow day (UTC+3) that now is in.
        StopExpiry::EndOfDay => {
            const MSK: i64 = 3 * 3600;
            ((now_secs + MSK).div_euclid(86_400) + 1) * 86_400 - MSK - 1
        }
        StopExpiry::GoodTillDate(ts) => nanos_to_secs(ts),
    }
}

/// The wire request and whether it is a stop-limit order (`true`) or a stop order (`false`).
///
/// # Errors
///
/// [`Error::Rejected`] with `missing_price` for a `StopLimit` without `limit_price`; `invalid_qty` for a quantity
/// that is not a whole number of lots; [`Error::Config`] for an instrument that is not a `TickerClass` one.
pub fn build(
    req: &PlaceStopOrderRequest,
    lot: brokapi_core::Decimal,
    portfolio: &str,
    exchange: &str,
    now_secs: i64,
    cid: ClientOrderId,
) -> Result<(StopOrderRequest, bool), Error> {
    let (ticker, board) = ticker_board(&req.instrument)?;
    let limit = match (req.kind, req.limit_price) {
        (StopOrderKind::StopLimit, None) => {
            return Err(Error::Rejected {
                code: "missing_price".into(),
                message: "a stop-limit order requires limit_price".into(),
            });
        }
        (_, limit) => limit,
    };
    let lots = pieces_to_lots(req.qty, lot)?;
    let wire = StopOrderRequest {
        side: match req.side {
            Side::Buy => "buy",
            Side::Sell => "sell",
        }
        .to_string(),
        condition: condition(req.kind, req.side).to_string(),
        trigger_price: req.stop_price.decimal(),
        stop_end_unix_time: Some(end_time(req.expiry, now_secs)),
        price: limit.map(Price::decimal),
        quantity: lots,
        instrument: InstrumentRef {
            symbol: ticker,
            exchange: exchange.to_string(),
            instrument_group: Some(board),
        },
        user: UserRef {
            portfolio: portfolio.to_string(),
        },
        comment: Some(cid.hyphenated().to_string()),
        time_in_force: limit.map(|_| "oneday".to_string()),
        allow_margin: None,
        activate: Some(true),
    };
    Ok((wire, limit.is_some()))
}

/// The state of a stop order from its `status`: a `working` one waits for the trigger.
pub fn state_of(status: Option<&str>) -> StopOrderState {
    match status.map(str::to_ascii_lowercase).as_deref() {
        Some("working") | None => StopOrderState::Active,
        Some("filled") => StopOrderState::Triggered,
        Some("canceled") | Some("cancelled") => StopOrderState::Cancelled,
        Some(other) => StopOrderState::Other(other.to_string()),
    }
}

/// A stop order of the portfolio as a snapshot. The cid is the one the connector found (`None` if the order is
/// not ours). `None` if the record lacks the side, the quantity or the trigger price.
pub fn snapshot(
    r: &StopOrderRecord,
    cid: Option<ClientOrderId>,
    now: Instant,
) -> Option<StopOrderSnapshot> {
    let side = side_of(r.side.as_deref())?;
    let stop_price = Price::new(r.stop_price?);
    let qty = Qty::new(r.qty_units?);
    let limit = r.kind.as_deref() == Some("stoplimit");
    let rises = matches!(
        r.condition
            .as_deref()
            .map(str::to_ascii_lowercase)
            .as_deref(),
        Some("more" | "moreorequal")
    );
    let kind = if limit {
        StopOrderKind::StopLimit
    } else if rises == (side == Side::Buy) {
        StopOrderKind::StopLoss
    } else {
        StopOrderKind::TakeProfit
    };
    let expiry = parse_time(r.end_time.as_deref())
        .map(StopExpiry::GoodTillDate)
        .unwrap_or(StopExpiry::GoodTillCancel);
    Some(StopOrderSnapshot {
        cid,
        broker_id: BrokerOrderId::new(r.id.clone()),
        instrument: instrument_id(&r.symbol, r.board.as_deref().unwrap_or_default()),
        side,
        kind,
        qty,
        stop_price,
        limit_price: if limit { r.price.map(Price::new) } else { None },
        expiry,
        state: state_of(r.status.as_deref()),
        ts: timestamps(now, parse_time(r.trans_time.as_deref())),
    })
}

/// The cid in the comment of a stop order record, if it is ours.
pub fn cid_of(r: &StopOrderRecord) -> Option<ClientOrderId> {
    cid_from_comment(r.comment.as_deref())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::instrument_id;
    use brokapi_core::{Decimal, dec};
    use uuid::Uuid;

    fn request(kind: StopOrderKind, side: Side, limit: Option<Price>) -> PlaceStopOrderRequest {
        PlaceStopOrderRequest {
            cid: Uuid::new_v4(),
            instrument: instrument_id("SBER", "TQBR"),
            side,
            kind,
            qty: Qty::from(20),
            stop_price: Price::new(dec!(200.5)),
            limit_price: limit,
            expiry: StopExpiry::GoodTillCancel,
        }
    }

    #[test]
    fn a_sell_stop_loss_waits_for_a_fall_and_a_sell_take_profit_for_a_rise() {
        let b = |k, s, l| {
            build(
                &request(k, s, l),
                dec!(10),
                "D1",
                "MOEX",
                1_000_000,
                Uuid::new_v4(),
            )
            .unwrap()
        };
        let (w, limit) = b(StopOrderKind::StopLoss, Side::Sell, None);
        assert_eq!(
            (w.condition.as_str(), limit, w.quantity),
            ("lessorequal", false, 2)
        );
        let (w, _) = b(StopOrderKind::TakeProfit, Side::Sell, None);
        assert_eq!(w.condition, "moreorequal");
        let (w, limit) = b(
            StopOrderKind::StopLoss,
            Side::Sell,
            Some(Price::new(dec!(200))),
        );
        assert!(limit);
        assert_eq!(w.price, Some(dec!(200)));
        assert_eq!(w.time_in_force.as_deref(), Some("oneday"));
    }

    #[test]
    fn a_stop_limit_without_a_price_is_refused_and_a_half_lot_too() {
        let err = build(
            &request(StopOrderKind::StopLimit, Side::Sell, None),
            dec!(10),
            "D1",
            "MOEX",
            0,
            Uuid::new_v4(),
        )
        .unwrap_err();
        assert!(matches!(err, Error::Rejected { ref code, .. } if code == "missing_price"));
        let mut r = request(StopOrderKind::StopLoss, Side::Sell, None);
        r.qty = Qty::from(15);
        assert!(matches!(
            build(&r, dec!(10), "D1", "MOEX", 0, Uuid::new_v4()),
            Err(Error::Rejected { ref code, .. }) if code == "invalid_qty"
        ));
    }

    #[test]
    fn expiries_become_unix_seconds() {
        // 2026-10-22 12:00:00 UTC is 15:00 in Moscow: the day ends at 2026-10-22 20:59:59 UTC.
        let noon = 1_792_670_400;
        assert_eq!(end_time(StopExpiry::EndOfDay, noon), noon + 9 * 3600 - 1);
        assert_eq!(
            end_time(StopExpiry::GoodTillCancel, noon),
            noon + GTC_HORIZON_SECS
        );
        let date = brokapi_core::UnixNanos::from_nanos(5_000_000_000);
        assert_eq!(end_time(StopExpiry::GoodTillDate(date), noon), 5);
    }

    #[test]
    fn a_record_is_read_back_with_a_kind() {
        let r: StopOrderRecord = serde_json::from_str(
            r#"{"id":"9","symbol":"SBER","board":"TQBR","type":"stop","side":"sell","condition":"lessorequal","status":"working","qtyUnits":20,"stopPrice":200.5}"#,
        )
        .unwrap();
        let s = snapshot(&r, None, Instant::now()).unwrap();
        assert_eq!(s.kind, StopOrderKind::StopLoss);
        assert_eq!(s.state, StopOrderState::Active);
        assert_eq!(s.stop_price.decimal(), dec!(200.5));
        assert_eq!(s.limit_price, None);
        let _ = Decimal::ZERO;
    }
}
