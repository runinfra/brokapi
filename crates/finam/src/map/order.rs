//! Orders: Finam `OrderState` <-> core snapshots, requests.

use std::time::Instant;

use brokapi_core::CidCodec;
use brokapi_core::{
    BrokerOrderId, ClientOrderId, Error, InstrumentId, NativeCid, OrderSnapshot, OrderState,
    OrderType, PlaceOrderRequest, Qty, Side,
};
use brokapi_finam_protocol::pb::grpc::tradeapi::v1::Side as PbSide;
use brokapi_finam_protocol::pb::grpc::tradeapi::v1::orders as pb;
use brokapi_finam_protocol::pb::grpc::tradeapi::v1::orders::{
    OrderStatus, OrderType as PbOrderType,
};

use crate::cid::FinamCidCodec;
use crate::util::{instrument_from_symbol, price, qty, qty0, symbol_of, timestamps, to_decimal};

/// Order is still working at the broker (unknown statuses count as working, never hidden).
pub fn is_active(status: i32) -> bool {
    !matches!(
        OrderStatus::try_from(status),
        Ok(OrderStatus::Filled
            | OrderStatus::Canceled
            | OrderStatus::Replaced
            | OrderStatus::Rejected
            | OrderStatus::Expired
            | OrderStatus::Failed
            | OrderStatus::DeniedByBroker
            | OrderStatus::RejectedByExchange
            | OrderStatus::Executed
            | OrderStatus::Disabled
            | OrderStatus::SlExecuted
            | OrderStatus::TpExecuted)
    )
}

/// Finam status -> core state. `filled`/`left` come from executed/remaining quantity.
pub fn map_status(
    status: i32,
    cid: ClientOrderId,
    broker_id: &str,
    filled: Qty,
    left: Qty,
    description: &str,
) -> OrderState {
    use OrderStatus as S;
    let st = S::try_from(status).unwrap_or(S::Unspecified);
    match st {
        S::Filled | S::Executed | S::SlExecuted | S::TpExecuted => OrderState::Filled { cid },
        S::Canceled | S::Replaced | S::Expired | S::Disabled => OrderState::Cancelled { cid },
        S::Rejected | S::Failed | S::DeniedByBroker | S::RejectedByExchange => {
            OrderState::Rejected {
                cid,
                reason: if description.is_empty() {
                    format!("{st:?}")
                } else {
                    format!("{st:?}: {description}")
                },
            }
        }
        S::PartiallyFilled => OrderState::PartiallyFilled { cid, filled, left },
        // Everything else is a working order; a partial execution shows through the quantities.
        _ if filled.is_positive() && left.is_positive() => {
            OrderState::PartiallyFilled { cid, filled, left }
        }
        _ => OrderState::Acked {
            cid,
            broker_id: BrokerOrderId::new(broker_id),
        },
    }
}

/// Finam side -> core side. An unspecified side reads as `Buy`: check the source of a value before
/// you trust it on an order that is not yours.
pub fn side_from_pb(s: i32) -> Side {
    match PbSide::try_from(s) {
        Ok(PbSide::Sell) => Side::Sell,
        _ => Side::Buy,
    }
}

/// Stop and multi-leg orders have no core equivalent: shown as the nearest plain type.
pub fn order_type_from_pb(t: i32) -> OrderType {
    match PbOrderType::try_from(t) {
        Ok(PbOrderType::Market | PbOrderType::Stop) => OrderType::Market,
        _ => OrderType::Limit,
    }
}

/// Core snapshot of a Finam order. `cid` is resolved by the caller (see `OrderIndex`).
pub fn map_order_state(os: &pb::OrderState, cid: ClientOrderId, now: Instant) -> OrderSnapshot {
    let order = os.order.clone().unwrap_or_default();
    let filled = qty0(&os.executed_quantity);
    let left = qty0(&os.remaining_quantity);
    let total = qty(&os.initial_quantity)
        .filter(|q| q.is_positive())
        .or_else(|| qty(&order.quantity))
        .unwrap_or(filled + left);
    let description = os.status_description.clone().unwrap_or_default();
    let limit = price(&order.limit_price).filter(|p| p.is_positive());
    OrderSnapshot::new(
        cid,
        instrument_from_symbol(&order.symbol),
        map_status(os.status, cid, &os.order_id, filled, left, &description),
        side_from_pb(order.side),
        order_type_from_pb(order.r#type),
        total,
        timestamps(os.accept_at.as_ref().or(os.transact_at.as_ref()), now),
    )
    .with_broker_id((!os.order_id.is_empty()).then(|| BrokerOrderId::new(os.order_id.clone())))
    .with_filled(filled)
    .with_price(limit)
}

/// Cid carried by an order echo: the comment (lossless) wins.
pub fn cid_from_comment(comment: &str) -> Option<ClientOrderId> {
    FinamCidCodec.decode(&NativeCid::new(comment))
}

/// Builds the `PlaceOrder` request. Quantity is in pieces (шт.), as in the Finam API.
pub fn build_place_order(account_id: &str, req: &PlaceOrderRequest) -> Result<pb::Order, Error> {
    let codec = FinamCidCodec;
    let symbol = symbol_of(&req.instrument)?;
    if !req.qty.is_positive() {
        return Err(Error::Rejected {
            code: "invalid_qty".into(),
            message: format!("quantity must be positive, got {}", req.qty),
        });
    }
    let quantity = to_decimal(req.qty.decimal());
    let (ty, limit_price) = match req.order_type {
        OrderType::Market => (PbOrderType::Market, None),
        OrderType::Limit => {
            let p = req.price.ok_or_else(|| Error::Rejected {
                code: "missing_price".into(),
                message: "limit order requires price".into(),
            })?;
            (PbOrderType::Limit, Some(to_decimal(p.decimal())))
        }
    };
    let comment = codec
        .encode(req.cid)
        .map_err(|e| Error::Config(brokapi_core::ConfigError::Message(e.to_string())))?;
    Ok(pb::Order {
        account_id: account_id.to_string(),
        symbol,
        quantity: Some(quantity),
        side: match req.side {
            Side::Buy => PbSide::Buy as i32,
            Side::Sell => PbSide::Sell as i32,
        },
        r#type: ty as i32,
        time_in_force: pb::TimeInForce::Day as i32,
        limit_price,
        client_order_id: codec.client_order_id(req.cid),
        comment: comment.0,
        ..Default::default()
    })
}

/// Rejection coming back as an order status in the `PlaceOrder` response.
pub fn rejection_of(os: &pb::OrderState) -> Option<Error> {
    match OrderStatus::try_from(os.status) {
        Ok(
            s @ (OrderStatus::Rejected
            | OrderStatus::Failed
            | OrderStatus::DeniedByBroker
            | OrderStatus::RejectedByExchange),
        ) => Some(Error::Rejected {
            code: format!("{s:?}").to_lowercase(),
            message: os.status_description.clone().unwrap_or_default(),
        }),
        _ => None,
    }
}

/// Instrument of an order echo.
pub fn order_instrument(os: &pb::OrderState) -> InstrumentId {
    instrument_from_symbol(os.order.as_ref().map(|o| o.symbol.as_str()).unwrap_or(""))
}
