//! Stop orders: core requests <-> `StopOrdersService` messages.
//!
//! Units as for plain orders: quantity in LOTS, prices per one instrument.

use std::time::Instant;

use brokapi_core::{
    BrokerOrderId, ClientOrderId, Error, PlaceStopOrderRequest, Price, Qty, Side, StopExpiry,
    StopOrderKind, StopOrderSnapshot, StopOrderState,
};
use brokapi_tbank_protocol::v1 as pb;

use crate::ids::{
    id_from_parts, instrument_ref, nanos_to_proto, price_of_money, price_to_quotation,
    proto_to_nanos, require_whole_lots, stamps,
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

/// Checks that need no network. Run before any quota is spent or anything is registered.
pub fn validate(req: &PlaceStopOrderRequest) -> Result<(), Error> {
    require_whole_lots(req.qty)?;
    positive("stop_price", req.stop_price)?;
    match (req.kind, req.limit_price) {
        (StopOrderKind::StopLimit, None) => {
            return Err(rejected(
                "missing_limit_price",
                "a stop-limit order needs a limit price",
            ));
        }
        (_, Some(p)) => positive("limit_price", p)?,
        _ => {}
    }
    if matches!(req.expiry, StopExpiry::EndOfDay) {
        return Err(rejected(
            "unsupported_expiry",
            "T-Bank stop orders live until cancelled or until a date: use GoodTillCancel or GoodTillDate",
        ));
    }
    Ok(())
}

/// Builds the `PostStopOrder` request for `account` from a domain request.
///
/// The `cid` goes to `order_id`, the idempotency key. The request is checked with [`validate`]
/// first, so nothing invalid is built.
///
/// # Errors
///
/// [`Error::Rejected`] with code `invalid_qty`, `invalid_price`, `missing_limit_price` or
/// `unsupported_expiry`, as [`validate`] lists.
pub fn build_post(
    account: &str,
    req: &PlaceStopOrderRequest,
) -> Result<pb::PostStopOrderRequest, Error> {
    validate(req)?;
    let lots = require_whole_lots(req.qty)?;
    let (expiration_type, expire_date) = match req.expiry {
        StopExpiry::GoodTillCancel => (pb::StopOrderExpirationType::GoodTillCancel, None),
        StopExpiry::GoodTillDate(t) => (
            pb::StopOrderExpirationType::GoodTillDate,
            Some(nanos_to_proto(t)),
        ),
        StopExpiry::EndOfDay => unreachable!("validate() refuses it"),
    };
    let stop_order_type = match req.kind {
        StopOrderKind::StopLoss => pb::StopOrderType::StopLoss,
        StopOrderKind::TakeProfit => pb::StopOrderType::TakeProfit,
        StopOrderKind::StopLimit => pb::StopOrderType::StopLimit,
    };
    // The child exchange order: a limit order when there is a limit price, else a market one.
    let exchange_order_type = if req.limit_price.is_some() {
        pb::ExchangeOrderType::Limit
    } else {
        pb::ExchangeOrderType::Market
    };
    Ok(pb::PostStopOrderRequest {
        quantity: lots,
        price: req.limit_price.map(price_to_quotation).transpose()?,
        stop_price: Some(price_to_quotation(req.stop_price)?),
        direction: match req.side {
            Side::Buy => pb::StopOrderDirection::Buy,
            Side::Sell => pb::StopOrderDirection::Sell,
        } as i32,
        account_id: account.to_string(),
        expiration_type: expiration_type as i32,
        stop_order_type: stop_order_type as i32,
        expire_date,
        instrument_id: instrument_ref(&req.instrument),
        exchange_order_type: exchange_order_type as i32,
        take_profit_type: if req.kind == StopOrderKind::TakeProfit {
            pb::TakeProfitType::Regular as i32
        } else {
            pb::TakeProfitType::Unspecified as i32
        },
        order_id: req.cid.hyphenated().to_string(),
        ..Default::default()
    })
}

fn kind_of(t: pb::StopOrderType) -> StopOrderKind {
    match t {
        pb::StopOrderType::TakeProfit => StopOrderKind::TakeProfit,
        pb::StopOrderType::StopLimit => StopOrderKind::StopLimit,
        // Unspecified is shown as the plainest kind rather than hiding the order.
        pb::StopOrderType::StopLoss | pb::StopOrderType::Unspecified => StopOrderKind::StopLoss,
    }
}

fn state_of(s: pb::StopOrderStatusOption) -> StopOrderState {
    use pb::StopOrderStatusOption as S;
    match s {
        S::StopOrderStatusActive => StopOrderState::Active,
        S::StopOrderStatusExecuted => StopOrderState::Triggered,
        S::StopOrderStatusCanceled => StopOrderState::Cancelled,
        S::StopOrderStatusExpired => StopOrderState::Expired,
        other => StopOrderState::Other(format!("{other:?}")),
    }
}

/// Core snapshot of a broker stop order. `cid` comes from what this connector registered at
/// `place_stop` (the broker's `StopOrder` does not carry the idempotency key).
pub fn snapshot(o: &pb::StopOrder, cid: Option<ClientOrderId>, now: Instant) -> StopOrderSnapshot {
    let kind = kind_of(o.order_type());
    let limit = o.price.as_ref().map(price_of_money).filter(|p| {
        p.is_positive()
            && (kind == StopOrderKind::StopLimit
                || o.exchange_order_type() == pb::ExchangeOrderType::Limit)
    });
    let expiry = match o.expiration_time.as_ref().filter(|t| t.seconds != 0) {
        Some(t) => StopExpiry::GoodTillDate(proto_to_nanos(t)),
        None => StopExpiry::GoodTillCancel,
    };
    StopOrderSnapshot {
        cid,
        broker_id: BrokerOrderId::new(o.stop_order_id.clone()),
        instrument: id_from_parts(&o.instrument_uid, &o.figi, &o.ticker, &o.class_code),
        side: match o.direction() {
            pb::StopOrderDirection::Sell => Side::Sell,
            _ => Side::Buy,
        },
        kind,
        qty: Qty::from(o.lots_requested),
        stop_price: o
            .stop_price
            .as_ref()
            .map(price_of_money)
            .unwrap_or(Price::ZERO),
        limit_price: limit,
        expiry,
        state: state_of(o.status()),
        ts: stamps(now, o.create_date.as_ref()),
    }
}

#[cfg(test)]
mod tests {
    use brokapi_core::{Decimal, dec};
    use brokapi_core::{InstrumentId, InstrumentKey, NativeId, UnixNanos};

    use super::*;

    fn req(kind: StopOrderKind, limit: Option<Price>, expiry: StopExpiry) -> PlaceStopOrderRequest {
        PlaceStopOrderRequest {
            cid: ClientOrderId::from_u128(0x1234),
            instrument: InstrumentId::new(
                InstrumentKey::new("SBER", "TQBR"),
                NativeId::Uid("uid-sber".into()),
            ),
            side: Side::Sell,
            kind,
            qty: Qty::from(3),
            stop_price: Price::new(dec!(250.5)),
            limit_price: limit,
            expiry,
        }
    }

    #[test]
    fn stop_loss_is_a_market_child_order() {
        let w = build_post(
            "acc",
            &req(StopOrderKind::StopLoss, None, StopExpiry::GoodTillCancel),
        )
        .unwrap();
        assert_eq!(w.quantity, 3);
        assert_eq!(w.stop_order_type(), pb::StopOrderType::StopLoss);
        assert_eq!(w.exchange_order_type(), pb::ExchangeOrderType::Market);
        assert_eq!(w.direction(), pb::StopOrderDirection::Sell);
        assert_eq!(
            w.expiration_type(),
            pb::StopOrderExpirationType::GoodTillCancel
        );
        assert!(w.price.is_none() && w.expire_date.is_none());
        assert_eq!(
            w.order_id,
            ClientOrderId::from_u128(0x1234).hyphenated().to_string()
        );
        assert_eq!(w.instrument_id, "uid-sber");
        assert_eq!(w.account_id, "acc");
        let stop = w.stop_price.unwrap();
        assert_eq!((stop.units, stop.nano), (250, 500_000_000));
    }

    #[test]
    fn take_profit_with_limit_and_date() {
        let t = UnixNanos::from_nanos(1_800_000_000_000_000_000);
        let w = build_post(
            "acc",
            &req(
                StopOrderKind::TakeProfit,
                Some(Price::from(251)),
                StopExpiry::GoodTillDate(t),
            ),
        )
        .unwrap();
        assert_eq!(w.stop_order_type(), pb::StopOrderType::TakeProfit);
        assert_eq!(w.take_profit_type(), pb::TakeProfitType::Regular);
        assert_eq!(w.exchange_order_type(), pb::ExchangeOrderType::Limit);
        assert_eq!(
            w.expiration_type(),
            pb::StopOrderExpirationType::GoodTillDate
        );
        assert_eq!(w.expire_date.unwrap().seconds, 1_800_000_000);
        assert_eq!(w.price.unwrap().units, 251);
    }

    #[test]
    fn refusals_need_no_network() {
        let cases = [
            (
                req(StopOrderKind::StopLimit, None, StopExpiry::GoodTillCancel),
                "missing_limit_price",
            ),
            (
                req(StopOrderKind::StopLoss, None, StopExpiry::EndOfDay),
                "unsupported_expiry",
            ),
            (
                req(
                    StopOrderKind::StopLoss,
                    Some(Price::new(dec!(-1.0))),
                    StopExpiry::GoodTillCancel,
                ),
                "invalid_price",
            ),
        ];
        for (r, want) in cases {
            match validate(&r) {
                Err(Error::Rejected { code, .. }) => assert_eq!(code, want),
                other => panic!("{want}: {other:?}"),
            }
        }
        let mut r = req(StopOrderKind::StopLoss, None, StopExpiry::GoodTillCancel);
        r.qty = Qty::new(dec!(1.5));
        assert!(validate(&r).is_err(), "half a lot");
        r.qty = Qty::from(1);
        r.stop_price = Price::from(0);
        assert!(validate(&r).is_err(), "zero stop price");
    }

    #[test]
    fn snapshot_reads_the_broker_message() {
        let o = pb::StopOrder {
            stop_order_id: "SO1".into(),
            lots_requested: 2,
            direction: pb::StopOrderDirection::Sell as i32,
            order_type: pb::StopOrderType::StopLimit as i32,
            price: Some(brokapi_tbank_protocol::money::money("rub", "249.0")),
            stop_price: Some(brokapi_tbank_protocol::money::money("rub", "250.0")),
            instrument_uid: "uid-sber".into(),
            ticker: "SBER".into(),
            class_code: "TQBR".into(),
            status: pb::StopOrderStatusOption::StopOrderStatusActive as i32,
            ..Default::default()
        };
        let cid = ClientOrderId::from_u128(7);
        let s = snapshot(&o, Some(cid), Instant::now());
        assert_eq!(s.cid, Some(cid));
        assert_eq!(s.broker_id.as_str(), "SO1");
        assert_eq!(s.kind, StopOrderKind::StopLimit);
        assert_eq!(s.side, Side::Sell);
        assert_eq!(s.qty.decimal(), dec!(2.0));
        assert_eq!(s.stop_price.decimal(), dec!(250.0));
        assert_eq!(s.limit_price.map(Price::decimal), Some(dec!(249.0)));
        assert_eq!(s.state, StopOrderState::Active);
        assert_eq!(s.expiry, StopExpiry::GoodTillCancel);
        assert_eq!(s.instrument.key.symbol, "SBER");
    }

    #[test]
    fn a_zero_price_is_not_a_limit_price() {
        let o = pb::StopOrder {
            order_type: pb::StopOrderType::StopLoss as i32,
            price: Some(brokapi_tbank_protocol::money::money("rub", "0.0")),
            stop_price: Some(brokapi_tbank_protocol::money::money("rub", "10.0")),
            ..Default::default()
        };
        assert!(snapshot(&o, None, Instant::now()).limit_price.is_none());
    }
}
