//! Stop orders: core requests <-> Finam `Order` (STOP, STOP_LIMIT) and `SLTPOrder`.
//!
//! Finam keeps stop orders in `OrdersService`:
//! - stop-loss and stop-limit go through `PlaceOrder` with type `STOP` / `STOP_LIMIT`, a `stop_price`
//!   and an explicit `stop_condition` (LAST_UP: the trigger price is above the current price,
//!   LAST_DOWN: below);
//! - take-profit goes through `PlaceSLTPOrder` (the take-profit leg only).
//!
//! Quantity is in pieces, as everywhere in Finam. The cid is carried like for plain orders: the
//! lossless form in `comment`, a 20-hex tag in `client_order_id`.

use brokapi_core::{
    BrokerOrderId, CidCodec, ClientOrderId, ConfigError, Error, PlaceStopOrderRequest, Price, Qty,
    Side, StopExpiry, StopOrderKind, StopOrderSnapshot, StopOrderState,
};
use brokapi_finam_protocol::pb::grpc::tradeapi::v1::Side as PbSide;
use brokapi_finam_protocol::pb::grpc::tradeapi::v1::orders as pb;
use brokapi_finam_protocol::pb::grpc::tradeapi::v1::orders::{
    OrderStatus, OrderType as PbOrderType,
};
use std::time::Instant;

use crate::cid::FinamCidCodec;
use crate::map::order::side_from_pb;
use crate::util::{
    instrument_from_symbol, price, price0, qty, qty0, symbol_of, timestamps, to_decimal,
    to_timestamp, ts_nanos,
};

fn rejected(code: &str, message: impl Into<String>) -> Error {
    Error::Rejected {
        code: code.into(),
        message: message.into(),
    }
}

fn positive(what: &str, p: Price) -> Result<(), Error> {
    if p.is_positive() {
        Ok(())
    } else {
        Err(rejected(
            "invalid_price",
            format!("{what} must be a positive number, got {p}"),
        ))
    }
}

/// Which Finam call carries the request.
#[derive(Debug, Clone, PartialEq)]
pub enum StopWire {
    /// `PlaceOrder` with type STOP or STOP_LIMIT.
    Order(pb::Order),
    /// `PlaceSLTPOrder` with the take-profit leg only.
    TakeProfit(pb::SltpOrder),
}

/// Checks that need no network, then the wire request.
pub fn build(account_id: &str, req: &PlaceStopOrderRequest) -> Result<StopWire, Error> {
    let symbol = symbol_of(&req.instrument)?;
    if !req.qty.is_positive() {
        return Err(rejected("invalid_order", "quantity must be > 0"));
    }
    positive("stop_price", req.stop_price)?;
    if let Some(p) = req.limit_price {
        positive("limit_price", p)?;
    }
    if req.kind == StopOrderKind::StopLimit && req.limit_price.is_none() {
        return Err(rejected(
            "missing_limit_price",
            "a stop-limit order needs a limit price",
        ));
    }
    let quantity = to_decimal(req.qty.decimal());
    let stop_price = to_decimal(req.stop_price.decimal());
    let codec = FinamCidCodec;
    let comment = codec
        .encode(req.cid)
        .map_err(|e| Error::Config(ConfigError::Message(e.to_string())))?
        .0;
    let tag = codec.client_order_id(req.cid);
    let side = match req.side {
        Side::Buy => PbSide::Buy as i32,
        Side::Sell => PbSide::Sell as i32,
    };

    match req.kind {
        StopOrderKind::TakeProfit => {
            if req.limit_price.is_some() {
                return Err(rejected(
                    "unsupported_limit_price",
                    "Finam sets a take-profit limit through a guard spread, not a price: place the take-profit without a limit price",
                ));
            }
            let (valid_before, valid_expiry_time) = match req.expiry {
                StopExpiry::GoodTillCancel => (pb::ValidBefore::GoodTillCancel, None),
                StopExpiry::EndOfDay => (pb::ValidBefore::EndOfDay, None),
                StopExpiry::GoodTillDate(t) => {
                    (pb::ValidBefore::GoodTillDate, Some(to_timestamp(t)))
                }
            };
            Ok(StopWire::TakeProfit(pb::SltpOrder {
                account_id: account_id.to_string(),
                symbol,
                side,
                quantity_tp: Some(quantity),
                tp_price: Some(stop_price),
                tp_qty_measure: pb::SltpQtyMeasure::Value as i32,
                client_order_id: tag,
                valid_before: valid_before as i32,
                valid_expiry_time,
                comment,
                ..Default::default()
            }))
        }
        StopOrderKind::StopLoss | StopOrderKind::StopLimit => {
            let valid_before = match req.expiry {
                StopExpiry::GoodTillCancel => pb::ValidBefore::GoodTillCancel,
                StopExpiry::EndOfDay => pb::ValidBefore::EndOfDay,
                StopExpiry::GoodTillDate(_) => {
                    return Err(rejected(
                        "unsupported_expiry",
                        "Finam supports an expiry date only for SL/TP orders: use GoodTillCancel or EndOfDay, or a take-profit",
                    ));
                }
            };
            let condition = if req.kind.triggers_on_rise(req.side) {
                pb::StopCondition::LastUp
            } else {
                pb::StopCondition::LastDown
            };
            let (ty, limit_price) = match req.limit_price {
                Some(p) => (PbOrderType::StopLimit, Some(to_decimal(p.decimal()))),
                None => (PbOrderType::Stop, None),
            };
            Ok(StopWire::Order(pb::Order {
                account_id: account_id.to_string(),
                symbol,
                quantity: Some(quantity),
                side,
                r#type: ty as i32,
                limit_price,
                stop_price: Some(stop_price),
                stop_condition: condition as i32,
                client_order_id: tag,
                valid_before: valid_before as i32,
                comment,
                ..Default::default()
            }))
        }
    }
}

/// A Finam order that is a stop order: type STOP / STOP_LIMIT, or an SL/TP order.
pub fn is_stop(os: &pb::OrderState) -> bool {
    os.sltp_order.is_some()
        || os.order.as_ref().is_some_and(|o| {
            matches!(
                PbOrderType::try_from(o.r#type),
                Ok(PbOrderType::Stop | PbOrderType::StopLimit)
            )
        })
}

/// `(client_order_id, comment)` an order echo carries: from `order`, or for an SL/TP order from
/// `sltp_order`. This is where the cid is read back.
pub fn echo_keys(os: &pb::OrderState) -> (String, String) {
    if let Some(o) = os
        .order
        .as_ref()
        .filter(|o| !o.client_order_id.is_empty() || !o.comment.is_empty())
    {
        return (o.client_order_id.clone(), o.comment.clone());
    }
    match &os.sltp_order {
        Some(s) => (s.client_order_id.clone(), s.comment.clone()),
        None => (String::new(), String::new()),
    }
}

/// Lifecycle of a stop order from the Finam status. The names come from the proto; how Finam really
/// walks through them is not verified against the live API.
pub fn state_of(status: i32) -> StopOrderState {
    use OrderStatus as S;
    match S::try_from(status) {
        // Waiting for the trigger (guard time and TP correction come before the exchange order).
        Ok(
            S::New
            | S::PendingNew
            | S::Watching
            | S::Wait
            | S::LinkWait
            | S::Suspended
            | S::SlGuardTime
            | S::TpGuardTime
            | S::TpCorrection
            | S::TpCorrGuardTime,
        ) => StopOrderState::Active,
        // Triggered: the exchange order is on its way or has been executed.
        Ok(
            S::Forwarding
            | S::SlForwarding
            | S::TpForwarding
            | S::PartiallyFilled
            | S::Filled
            | S::Executed
            | S::SlExecuted
            | S::TpExecuted
            | S::DoneForDay,
        ) => StopOrderState::Triggered,
        Ok(S::Canceled | S::Replaced | S::Disabled | S::PendingCancel) => StopOrderState::Cancelled,
        Ok(S::Expired) => StopOrderState::Expired,
        Ok(other) => StopOrderState::Other(format!("{other:?}")),
        Err(_) => StopOrderState::Other(format!("status {status}")),
    }
}

/// Core snapshot of a Finam stop order. `cid` is resolved by the caller (`OrderIndex::lookup`).
pub fn snapshot(
    os: &pb::OrderState,
    cid: Option<ClientOrderId>,
    now: Instant,
) -> StopOrderSnapshot {
    let ts = timestamps(os.accept_at.as_ref().or(os.transact_at.as_ref()), now);
    let state = state_of(os.status);
    if let Some(s) = &os.sltp_order {
        // SL/TP order. Only the take-profit leg is ours; a foreign pair is shown by its SL leg.
        let has_sl = price(&s.sl_price).is_some_and(|p| p.is_positive());
        let side = side_from_pb(s.side);
        let (kind, qty, stop, limit) = if has_sl {
            let limit = price(&s.limit_price).filter(|p| p.is_positive());
            (
                if limit.is_some() {
                    StopOrderKind::StopLimit
                } else {
                    StopOrderKind::StopLoss
                },
                qty0(&s.quantity_sl),
                price0(&s.sl_price),
                limit,
            )
        } else {
            (
                StopOrderKind::TakeProfit,
                qty0(&s.quantity_tp),
                price0(&s.tp_price),
                None,
            )
        };
        let expiry = match pb::ValidBefore::try_from(s.valid_before) {
            Ok(pb::ValidBefore::EndOfDay) => StopExpiry::EndOfDay,
            Ok(pb::ValidBefore::GoodTillDate) => s
                .valid_expiry_time
                .as_ref()
                .map(|t| StopExpiry::GoodTillDate(ts_nanos(t)))
                .unwrap_or(StopExpiry::GoodTillCancel),
            _ => StopExpiry::GoodTillCancel,
        };
        return StopOrderSnapshot {
            cid,
            broker_id: BrokerOrderId::new(os.order_id.clone()),
            instrument: instrument_from_symbol(&s.symbol),
            side,
            kind,
            qty,
            stop_price: stop,
            limit_price: limit,
            expiry,
            state,
            ts,
        };
    }
    let o = os.order.clone().unwrap_or_default();
    let side = side_from_pb(o.side);
    let condition =
        pb::StopCondition::try_from(o.stop_condition).unwrap_or(pb::StopCondition::Unspecified);
    let kind = if o.r#type == PbOrderType::StopLimit as i32 {
        StopOrderKind::StopLimit
    } else {
        // A stop market order that triggers against the position's direction is a take-profit.
        let rises = condition == pb::StopCondition::LastUp;
        if rises == StopOrderKind::StopLoss.triggers_on_rise(side) {
            StopOrderKind::StopLoss
        } else {
            StopOrderKind::TakeProfit
        }
    };
    let expiry = match pb::ValidBefore::try_from(o.valid_before) {
        Ok(pb::ValidBefore::EndOfDay) => StopExpiry::EndOfDay,
        _ => StopExpiry::GoodTillCancel,
    };
    let qty = qty(&os.initial_quantity)
        .filter(|q| q.is_positive())
        .or_else(|| self::qty(&o.quantity))
        .unwrap_or(Qty::ZERO);
    StopOrderSnapshot {
        cid,
        broker_id: BrokerOrderId::new(os.order_id.clone()),
        instrument: instrument_from_symbol(&o.symbol),
        side,
        kind,
        qty,
        stop_price: price0(&o.stop_price),
        limit_price: price(&o.limit_price).filter(|p| p.is_positive()),
        expiry,
        state,
        ts,
    }
}

#[cfg(test)]
mod tests {
    use brokapi_core::{Decimal, dec};
    use brokapi_core::{InstrumentId, InstrumentKey, NativeId, UnixNanos};

    use super::*;

    fn req(
        side: Side,
        kind: StopOrderKind,
        limit: Option<Decimal>,
        expiry: StopExpiry,
    ) -> PlaceStopOrderRequest {
        PlaceStopOrderRequest {
            cid: ClientOrderId::from_u128(0xabcdef),
            instrument: InstrumentId::new(
                InstrumentKey::new("SBER", "MISX"),
                NativeId::SymbolMic {
                    symbol: "SBER".into(),
                    mic: "MISX".into(),
                },
            ),
            side,
            kind,
            qty: Qty::from(10),
            stop_price: Price::new(dec!(250.5)),
            limit_price: limit.map(Price::new),
            expiry,
        }
    }

    fn order(w: StopWire) -> pb::Order {
        match w {
            StopWire::Order(o) => o,
            other => panic!("expected PlaceOrder, got {other:?}"),
        }
    }

    #[test]
    fn stop_loss_sell_is_a_stop_that_triggers_down() {
        let o = order(
            build(
                "ACC1",
                &req(
                    Side::Sell,
                    StopOrderKind::StopLoss,
                    None,
                    StopExpiry::GoodTillCancel,
                ),
            )
            .unwrap(),
        );
        assert_eq!(o.r#type, PbOrderType::Stop as i32);
        assert_eq!(o.stop_condition, pb::StopCondition::LastDown as i32);
        assert_eq!(o.stop_price.unwrap().value, "250.5");
        assert!(o.limit_price.is_none());
        assert_eq!(o.symbol, "SBER@MISX");
        assert_eq!(o.quantity.unwrap().value, "10");
        assert_eq!(o.valid_before, pb::ValidBefore::GoodTillCancel as i32);
        assert_eq!(o.account_id, "ACC1");
        // The cid travels like for plain orders.
        assert!(o.comment.starts_with("bc1:"));
        assert_eq!(o.client_order_id.len(), 20);
    }

    #[test]
    fn stop_loss_buy_triggers_up_and_a_limit_price_makes_it_stop_limit() {
        let o = order(
            build(
                "A",
                &req(
                    Side::Buy,
                    StopOrderKind::StopLoss,
                    None,
                    StopExpiry::EndOfDay,
                ),
            )
            .unwrap(),
        );
        assert_eq!(o.stop_condition, pb::StopCondition::LastUp as i32);
        assert_eq!(o.valid_before, pb::ValidBefore::EndOfDay as i32);
        let o = order(
            build(
                "A",
                &req(
                    Side::Sell,
                    StopOrderKind::StopLimit,
                    Some(dec!(249.0)),
                    StopExpiry::GoodTillCancel,
                ),
            )
            .unwrap(),
        );
        assert_eq!(o.r#type, PbOrderType::StopLimit as i32);
        assert_eq!(o.limit_price.unwrap().value, "249.0");
        assert_eq!(o.stop_condition, pb::StopCondition::LastDown as i32);
    }

    #[test]
    fn take_profit_goes_through_the_sltp_call() {
        let t = UnixNanos::from_nanos(1_900_000_000_000_000_000);
        let w = build(
            "A",
            &req(
                Side::Sell,
                StopOrderKind::TakeProfit,
                None,
                StopExpiry::GoodTillDate(t),
            ),
        )
        .unwrap();
        let StopWire::TakeProfit(s) = w else {
            panic!("expected SLTP")
        };
        assert_eq!(s.symbol, "SBER@MISX");
        assert_eq!(s.quantity_tp.unwrap().value, "10");
        assert_eq!(s.tp_price.unwrap().value, "250.5");
        assert!(
            s.quantity_sl.is_none() && s.sl_price.is_none(),
            "the take-profit leg only"
        );
        assert_eq!(s.valid_before, pb::ValidBefore::GoodTillDate as i32);
        assert_eq!(s.valid_expiry_time.unwrap().seconds, 1_900_000_000);
        assert!(s.comment.starts_with("bc1:"));
    }

    #[test]
    fn refusals_need_no_network() {
        let cases = [
            (
                req(
                    Side::Sell,
                    StopOrderKind::StopLimit,
                    None,
                    StopExpiry::GoodTillCancel,
                ),
                "missing_limit_price",
            ),
            (
                req(
                    Side::Sell,
                    StopOrderKind::TakeProfit,
                    Some(dec!(1.0)),
                    StopExpiry::GoodTillCancel,
                ),
                "unsupported_limit_price",
            ),
            (
                req(
                    Side::Sell,
                    StopOrderKind::StopLoss,
                    None,
                    StopExpiry::GoodTillDate(UnixNanos::from_nanos(1)),
                ),
                "unsupported_expiry",
            ),
            (
                req(
                    Side::Sell,
                    StopOrderKind::StopLoss,
                    Some(dec!(-3.0)),
                    StopExpiry::GoodTillCancel,
                ),
                "invalid_price",
            ),
        ];
        for (r, want) in cases {
            match build("A", &r) {
                Err(Error::Rejected { code, .. }) => assert_eq!(code, want),
                other => panic!("{want}: {other:?}"),
            }
        }
        let mut r = req(
            Side::Sell,
            StopOrderKind::StopLoss,
            None,
            StopExpiry::GoodTillCancel,
        );
        r.qty = Qty::from(0);
        assert!(build("A", &r).is_err());
    }

    #[test]
    fn states_group_the_finam_statuses() {
        assert_eq!(
            state_of(OrderStatus::Watching as i32),
            StopOrderState::Active
        );
        assert_eq!(
            state_of(OrderStatus::SlGuardTime as i32),
            StopOrderState::Active
        );
        assert_eq!(
            state_of(OrderStatus::TpForwarding as i32),
            StopOrderState::Triggered
        );
        assert_eq!(
            state_of(OrderStatus::SlExecuted as i32),
            StopOrderState::Triggered
        );
        assert_eq!(
            state_of(OrderStatus::Canceled as i32),
            StopOrderState::Cancelled
        );
        assert_eq!(
            state_of(OrderStatus::Expired as i32),
            StopOrderState::Expired
        );
        assert!(matches!(
            state_of(OrderStatus::Rejected as i32),
            StopOrderState::Other(_)
        ));
        assert!(matches!(state_of(999), StopOrderState::Other(_)));
    }

    #[test]
    fn the_snapshot_round_trips_what_was_sent() {
        let cid = ClientOrderId::from_u128(0xabcdef);
        let o = order(
            build(
                "A",
                &req(
                    Side::Sell,
                    StopOrderKind::StopLimit,
                    Some(dec!(249.0)),
                    StopExpiry::GoodTillCancel,
                ),
            )
            .unwrap(),
        );
        let os = pb::OrderState {
            order_id: "ORD7".into(),
            status: OrderStatus::Watching as i32,
            order: Some(o),
            initial_quantity: Some(crate::util::to_decimal(dec!(10.0))),
            ..Default::default()
        };
        assert!(is_stop(&os));
        let s = snapshot(&os, Some(cid), Instant::now());
        assert_eq!(s.cid, Some(cid));
        assert_eq!(s.broker_id.as_str(), "ORD7");
        assert_eq!(s.kind, StopOrderKind::StopLimit);
        assert_eq!(s.side, Side::Sell);
        assert_eq!(s.qty.decimal(), dec!(10.0));
        assert_eq!(s.stop_price.decimal(), dec!(250.5));
        assert_eq!(s.limit_price.map(Price::decimal), Some(dec!(249.0)));
        assert_eq!(s.state, StopOrderState::Active);
        assert_eq!(s.instrument.key.symbol, "SBER");
    }

    #[test]
    fn a_plain_order_is_not_a_stop() {
        let os = pb::OrderState {
            order: Some(pb::Order {
                r#type: PbOrderType::Limit as i32,
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(!is_stop(&os));
    }

    #[test]
    fn a_stop_that_triggers_against_the_position_reads_as_take_profit() {
        let os = pb::OrderState {
            order_id: "1".into(),
            status: OrderStatus::Watching as i32,
            order: Some(pb::Order {
                side: PbSide::Sell as i32,
                r#type: PbOrderType::Stop as i32,
                stop_condition: pb::StopCondition::LastUp as i32,
                stop_price: Some(crate::util::to_decimal(dec!(300.0))),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(
            snapshot(&os, None, Instant::now()).kind,
            StopOrderKind::TakeProfit
        );
    }
}
