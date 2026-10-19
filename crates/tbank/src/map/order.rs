//! Orders: broker messages -> `OrderSnapshot`.

use std::time::Instant;

use brokapi_core::{
    BrokerOrderId, ClientOrderId, InstrumentId, OrderSnapshot, OrderState, OrderType, Price, Qty,
    Side,
};
use brokapi_tbank_protocol::v1 as pb;
use brokapi_tbank_protocol::v1::order_state_stream_response as stream;

use crate::cid::{parse_request_id, synthetic_cid};
use crate::ids::{id_from_parts, price_of_money, stamps};

/// What every order message (unary state, placement response, stream update) has in common.
#[derive(Debug, Clone)]
pub struct OrderParts {
    /// The exchange order id (`order_id`). Empty when the message has none.
    pub exchange_id: String,
    /// The `order_request_id`: the idempotency key we sent, as text. Empty for an order placed
    /// elsewhere.
    pub request_id: String,
    /// The order status as the broker reports it.
    pub status: pb::OrderExecutionReportStatus,
    /// Lots requested.
    pub lots_requested: i64,
    /// Lots executed so far.
    pub lots_executed: i64,
    /// Price of one instrument, if the message carries a non-zero one.
    pub unit_price: Option<Price>,
    /// Buy or sell, as the broker reports it.
    pub direction: pb::OrderDirection,
    /// Limit, market or best price, as the broker reports it.
    pub order_type: pb::OrderType,
    /// The instrument, built from the uid, figi or ticker the message carries.
    pub instrument: InstrumentId,
    /// The broker's text for a refusal, if any (the placement response `message`, or the cause of a
    /// stream status).
    pub reason: String,
    /// The broker's time of the message, if it gave one.
    pub broker_ts: Option<prost_types::Timestamp>,
}

impl OrderParts {
    /// Our cid, or a stable stand-in for an order placed elsewhere.
    pub fn cid(&self) -> ClientOrderId {
        parse_request_id(&self.request_id).unwrap_or_else(|| synthetic_cid(&self.exchange_id))
    }

    /// `true` when `request_id` is a cid of ours, so the order was placed by this library.
    pub fn is_ours(&self) -> bool {
        parse_request_id(&self.request_id).is_some()
    }
}

/// Broker direction to [`Side`]. Anything but sell reads as buy.
pub fn side(d: pb::OrderDirection) -> Side {
    match d {
        pb::OrderDirection::Sell => Side::Sell,
        _ => Side::Buy,
    }
}

/// [`Side`] to broker direction.
pub fn direction(s: Side) -> pb::OrderDirection {
    match s {
        Side::Buy => pb::OrderDirection::Buy,
        Side::Sell => pb::OrderDirection::Sell,
    }
}

/// `ORDER_TYPE_BESTPRICE` has no counterpart in the domain model and reads as market.
pub fn order_type(t: pb::OrderType) -> OrderType {
    match t {
        pb::OrderType::Limit => OrderType::Limit,
        _ => OrderType::Market,
    }
}

/// [`OrderType`] to broker order type (limit or market).
pub fn wire_order_type(t: OrderType) -> pb::OrderType {
    match t {
        OrderType::Limit => pb::OrderType::Limit,
        OrderType::Market => pb::OrderType::Market,
    }
}

/// The order state of a status report.
///
/// Fill, cancel, reject, new and partial fill map to their states. Partial fill carries `filled` and
/// `left` in lots. An unspecified status is `Unknown`, never a guess. A rejection without a reason
/// gets the text `REJECTED`.
pub fn state(p: &OrderParts, cid: ClientOrderId, now: Instant) -> OrderState {
    use pb::OrderExecutionReportStatus as S;
    let broker_id = BrokerOrderId::new(p.exchange_id.clone());
    match p.status {
        S::ExecutionReportStatusFill => OrderState::Filled { cid },
        S::ExecutionReportStatusCancelled => OrderState::Cancelled { cid },
        S::ExecutionReportStatusRejected => OrderState::Rejected {
            cid,
            reason: if p.reason.is_empty() {
                "REJECTED".into()
            } else {
                p.reason.clone()
            },
        },
        S::ExecutionReportStatusNew => OrderState::Acked { cid, broker_id },
        S::ExecutionReportStatusPartiallyfill => OrderState::PartiallyFilled {
            cid,
            filled: Qty::from(p.lots_executed),
            left: Qty::from((p.lots_requested - p.lots_executed).max(0)),
        },
        S::ExecutionReportStatusUnspecified => OrderState::Unknown { cid, since: now },
    }
}

/// An [`OrderSnapshot`] from the parts of any order message.
///
/// `qty` and `filled` are in lots, `price` is per one instrument. The cid is [`OrderParts::cid`].
pub fn snapshot(p: &OrderParts, now: Instant) -> OrderSnapshot {
    let cid = p.cid();
    OrderSnapshot {
        cid,
        broker_id: (!p.exchange_id.is_empty()).then(|| BrokerOrderId::new(p.exchange_id.clone())),
        instrument: p.instrument.clone(),
        state: state(p, cid, now),
        side: side(p.direction),
        order_type: order_type(p.order_type),
        qty: Qty::from(p.lots_requested),
        filled: Qty::from(p.lots_executed),
        price: p.unit_price,
        ts: stamps(now, p.broker_ts.as_ref()),
    }
}

fn price_of(m: &Option<pb::MoneyValue>) -> Option<Price> {
    m.as_ref().map(price_of_money).filter(|p| !p.is_zero())
}

/// `GetOrderState` / `GetOrders`.
pub fn parts_from_state(o: &pb::OrderState) -> OrderParts {
    OrderParts {
        exchange_id: o.order_id.clone(),
        request_id: o.order_request_id.clone(),
        status: o.execution_report_status(),
        lots_requested: o.lots_requested,
        lots_executed: o.lots_executed,
        unit_price: price_of(&o.initial_security_price),
        direction: o.direction(),
        order_type: o.order_type(),
        instrument: id_from_parts(&o.instrument_uid, &o.figi, &o.ticker, &o.class_code),
        reason: String::new(),
        broker_ts: o.order_date,
    }
}

/// `PostOrder` / `ReplaceOrder` response.
pub fn parts_from_post(r: &pb::PostOrderResponse) -> OrderParts {
    OrderParts {
        exchange_id: r.order_id.clone(),
        request_id: r.order_request_id.clone(),
        status: r.execution_report_status(),
        lots_requested: r.lots_requested,
        lots_executed: r.lots_executed,
        unit_price: price_of(&r.initial_security_price),
        direction: r.direction(),
        order_type: r.order_type(),
        instrument: id_from_parts(&r.instrument_uid, &r.figi, &r.ticker, &r.class_code),
        reason: r.message.clone(),
        broker_ts: None,
    }
}

/// `OrderStateStream` update. `order_price` is taken as the price of one instrument (the docs do not
/// say; for our own orders the price we sent wins, see `UdCore`).
pub fn parts_from_stream(o: &stream::OrderState) -> OrderParts {
    let cause = o
        .status_info
        .and_then(|c| stream::StatusCauseInfo::try_from(c).ok())
        .map(|c| c.as_str_name().to_string())
        .unwrap_or_default();
    OrderParts {
        exchange_id: o.order_id.clone(),
        request_id: o.order_request_id.clone().unwrap_or_default(),
        status: o.execution_report_status(),
        lots_requested: o.lots_requested,
        lots_executed: o.lots_executed,
        unit_price: price_of(&Some(o.order_price.clone().unwrap_or_default())),
        direction: o.direction(),
        order_type: o.order_type(),
        instrument: id_from_parts(&o.instrument_uid, "", &o.ticker, &o.class_code),
        reason: cause,
        broker_ts: o.created_at,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use brokapi_core::{Decimal, dec};
    use brokapi_tbank_protocol::money::money;

    fn base() -> pb::OrderState {
        pb::OrderState {
            order_id: "5001".into(),
            execution_report_status: pb::OrderExecutionReportStatus::ExecutionReportStatusNew
                as i32,
            lots_requested: 10,
            lots_executed: 0,
            initial_security_price: Some(money("rub", "250.5")),
            direction: pb::OrderDirection::Buy as i32,
            order_type: pb::OrderType::Limit as i32,
            order_request_id: "6f1c4f6e-0a54-4a43-8c46-0f6d3b2c8a11".into(),
            ticker: "SBER".into(),
            class_code: "TQBR".into(),
            instrument_uid: "uid-1".into(),
            ..Default::default()
        }
    }

    fn snap(o: &pb::OrderState) -> OrderSnapshot {
        snapshot(&parts_from_state(o), Instant::now())
    }

    #[test]
    fn new_order_is_acked_with_our_cid() {
        let s = snap(&base());
        assert_eq!(s.cid.to_string(), "6f1c4f6e-0a54-4a43-8c46-0f6d3b2c8a11");
        assert!(matches!(s.state, OrderState::Acked { .. }));
        assert_eq!(s.broker_id.as_ref().unwrap().as_str(), "5001");
        assert_eq!(
            (s.qty.decimal(), s.filled.decimal()),
            (dec!(10.0), dec!(0.0))
        );
        assert_eq!(s.price.unwrap().decimal(), dec!(250.5));
        assert_eq!((s.side, s.order_type), (Side::Buy, OrderType::Limit));
    }

    #[test]
    fn partial_fill_counts_left_lots() {
        let mut o = base();
        o.execution_report_status =
            pb::OrderExecutionReportStatus::ExecutionReportStatusPartiallyfill as i32;
        o.lots_executed = 4;
        let s = snap(&o);
        match s.state {
            OrderState::PartiallyFilled { filled, left, .. } => {
                assert_eq!((filled.decimal(), left.decimal()), (dec!(4.0), dec!(6.0)));
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn terminal_statuses() {
        use pb::OrderExecutionReportStatus as S;
        for (st, check) in [
            (S::ExecutionReportStatusFill, 0),
            (S::ExecutionReportStatusCancelled, 1),
            (S::ExecutionReportStatusRejected, 2),
        ] {
            let mut o = base();
            o.execution_report_status = st as i32;
            let s = snap(&o);
            match (check, &s.state) {
                (0, OrderState::Filled { .. }) | (1, OrderState::Cancelled { .. }) => {}
                (2, OrderState::Rejected { reason, .. }) => assert_eq!(reason, "REJECTED"),
                other => panic!("{other:?}"),
            }
        }
    }

    #[test]
    fn unspecified_status_is_unknown() {
        let mut o = base();
        o.execution_report_status = 0;
        assert!(matches!(snap(&o).state, OrderState::Unknown { .. }));
    }

    #[test]
    fn foreign_order_gets_synthetic_cid() {
        let mut o = base();
        o.order_request_id.clear();
        let p = parts_from_state(&o);
        assert!(!p.is_ours());
        assert!(!p.cid().is_nil());
        assert_eq!(p.cid(), parts_from_state(&o).cid());
    }

    #[test]
    fn stream_state_with_cause() {
        let o = stream::OrderState {
            order_id: "7".into(),
            order_request_id: Some("6f1c4f6e-0a54-4a43-8c46-0f6d3b2c8a11".into()),
            execution_report_status: pb::OrderExecutionReportStatus::ExecutionReportStatusRejected
                as i32,
            status_info: Some(stream::StatusCauseInfo::CauseRejectedByExchange as i32),
            lots_requested: 1,
            ticker: "SBER".into(),
            class_code: "TQBR".into(),
            ..Default::default()
        };
        let s = snapshot(&parts_from_stream(&o), Instant::now());
        match s.state {
            OrderState::Rejected { reason, .. } => assert_eq!(reason, "CAUSE_REJECTED_BY_EXCHANGE"),
            other => panic!("{other:?}"),
        }
    }
}
