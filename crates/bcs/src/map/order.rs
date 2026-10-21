//! Order and execution mapping.

use brokapi_core::Decimal;
use std::time::Instant;

use brokapi_bcs_protocol::http::models::{
    OrderExecutionData as RestOrderExecutionData, OrderOperationResponse, OrderRecord,
    OrderStateListResponse, OrderStatusResponse,
};
use brokapi_bcs_protocol::models::enums::{OrderSide, OrderStatus, OrderType as BcsOrderType};
use brokapi_bcs_protocol::ws::models::{OrderExecutionData, OrderMessage};
use brokapi_core::{
    BrokerOrderId, ClientOrderId, Event, OrderAck, OrderSnapshot, OrderState, OrderType, Price,
    Qty, Side, StopExpiry, StopOrderKind, StopOrderSnapshot, StopOrderState,
};
use uuid::Uuid;

use super::{instrument_id_ticker, timestamps};

/// Maps the plain answer of a place request (`status: OK`) to an `Event::Order` that says "acknowledged".
///
/// The answer only says that the request was accepted, so the snapshot is a thin one: no instrument,
/// quantity or price (empty and zero), and the broker order id is the client order id as text. Use
/// [`map_state_list`] on the answer of a synchronous call to get the full report.
pub fn map_place_response(resp: &OrderOperationResponse, now: Instant) -> Event {
    Event::Order(map_operation_ack(
        resp,
        now,
        OrderState::Acked {
            cid: resp.client_order_id,
            broker_id: BrokerOrderId::new(resp.client_order_id.hyphenated().to_string()),
        },
    ))
}

/// Maps an order status answer to an `Event::Order`. `None` when the client order id is not a UUID
/// or the answer has no report.
pub fn map_order_status(resp: &OrderStatusResponse, now: Instant) -> Option<Event> {
    let cid = parse_cid(&resp.client_order_id)?;
    let data = resp.data.as_ref()?;
    Some(Event::Order(map_execution_data(cid, data, now)))
}

/// The cid of the order an `orders/events/ws` message is about.
///
/// The two ids of the message mean: `clientOrderId` is the id of the request that caused the message,
/// `originalClientOrderId` is the order that was changed (docs: "идентификатор измененной заявки"). For a
/// cancel the request id is a fresh id of the cancel, for a replace the new order's id. So:
///
/// - only one of them is a UUID: that one (the docs sample of a fill has `clientOrderId` empty);
/// - both are, and the message is a cancel, a replace or a cancel in progress (`orderStatus` 4, 5, 6):
///   `originalClientOrderId`, the order the news is about, not the id of the request;
/// - both are, any other status: `clientOrderId`, the id of the order that was placed or changed.
///
/// Not verified against the live API: which id the broker puts in which field for each kind of message.
/// `None` when neither is a UUID.
pub fn message_cid(msg: &OrderMessage) -> Option<ClientOrderId> {
    use brokapi_bcs_protocol::models::enums::OrderStatus as S;
    let client = parse_cid(&msg.client_order_id);
    let original = msg.original_client_order_id.as_deref().and_then(parse_cid);
    match (client, original) {
        (Some(c), Some(o)) => {
            let about_the_original = msg.data.as_ref().is_some_and(|d| {
                matches!(
                    d.order_status,
                    S::Cancelled | S::Replaced | S::PendingCancel
                )
            });
            Some(if about_the_original { o } else { c })
        }
        (c, o) => c.or(o),
    }
}

/// Maps a message of the order events WebSocket to an `Event::Order`.
///
/// The client order id is chosen by [`message_cid`]. `None` when neither id is a UUID or the message has no
/// report. Fills are mapped separately by [`crate::map::execution::map_ws_execution`].
pub fn map_ws_order_message(msg: &OrderMessage, now: Instant) -> Option<Event> {
    let cid = message_cid(msg)?;
    let data = msg.data.as_ref()?;
    let original = msg
        .original_client_order_id
        .as_deref()
        .unwrap_or(&msg.client_order_id);
    Some(Event::Order(map_ws_execution_data(
        cid, data, now, original,
    )))
}

/// Order types of the order LIST (`POST /orders/search`) that are plain orders: 1 market, 2 limit,
/// 3 iceberg, 10 limit for 30 days. Types 4..7 and 11 are conditional (stop) orders.
pub const PLAIN_LIST_TYPES: [i32; 4] = [1, 2, 3, 10];
/// Order types of the list that are conditional orders: 4 stop-limit, 5 and 11 take-profit,
/// 6 stop-loss, 7 take-profit and stop-loss.
pub const STOP_LIST_TYPES: [i32; 5] = [4, 5, 6, 7, 11];
/// Order status of the list: 3 = active.
pub const LIST_STATUS_ACTIVE: i32 = 3;

/// A plain order of the list. `cid` is what the caller knows about this order (the list does not carry
/// our `clientOrderId`).
pub fn map_order_record(
    record: &OrderRecord,
    cid: ClientOrderId,
    now: Instant,
) -> Option<OrderSnapshot> {
    let ticker = record.ticker.as_deref()?;
    let class_code = record.class_code.as_deref()?;
    let instrument = instrument_id_ticker(ticker, class_code);
    let side = map_side_int(record.side)?;
    let order_type = map_order_type_int(record.order_type)?;
    let qty = record.order_quantity.unwrap_or(Decimal::ZERO);
    let filled = record.executed_quantity.unwrap_or(Decimal::ZERO);
    let state = list_state(record, cid);
    Some(
        OrderSnapshot::new(
            cid,
            instrument,
            state,
            side,
            order_type,
            Qty::new(qty),
            timestamps(now, None),
        )
        .with_broker_id(Some(BrokerOrderId::new(record.order_id.clone())))
        .with_filled(Qty::new(filled))
        .with_price(record.price.filter(|p| *p > Decimal::ZERO).map(Price::new)),
    )
}

/// Status of the list (`orderStatus`): 1 cancelled, 2 executed, 3 active. An active order with some
/// quantity executed is partially filled.
fn list_state(record: &OrderRecord, cid: ClientOrderId) -> OrderState {
    let broker_id = BrokerOrderId::new(record.order_id.clone());
    let executed = record.executed_quantity.unwrap_or(Decimal::ZERO);
    match record.order_status {
        Some(1) => OrderState::Cancelled { cid },
        Some(2) => OrderState::Filled { cid },
        Some(3) if executed > Decimal::ZERO => OrderState::PartiallyFilled {
            cid,
            filled: Qty::new(executed),
            left: Qty::new(record.remained_quantity.unwrap_or_else(|| {
                (record.order_quantity.unwrap_or(Decimal::ZERO) - executed).max(Decimal::ZERO)
            })),
        },
        Some(3) => OrderState::Acked { cid, broker_id },
        _ => OrderState::Unknown {
            cid,
            since: brokapi_runtime::now(),
        },
    }
}

/// A conditional order of the list as one or two stop snapshots (two for type 7, a take-profit and
/// stop-loss pair: the pair is one order at the broker, shown as its two legs with the same ids).
/// `None` for plain orders.
pub fn map_stop_record(
    record: &OrderRecord,
    cid: Option<ClientOrderId>,
    now: Instant,
) -> Vec<StopOrderSnapshot> {
    let (Some(ticker), Some(class_code)) = (record.ticker.as_deref(), record.class_code.as_deref())
    else {
        return Vec::new();
    };
    let Some(side) = map_side_int(record.side) else {
        return Vec::new();
    };
    let state = match record.order_status {
        Some(3) => StopOrderState::Active,
        // "Executed" for a conditional order means it triggered and produced its order.
        Some(2) => StopOrderState::Triggered,
        Some(1) => StopOrderState::Cancelled,
        other => StopOrderState::Other(format!("orderStatus={other:?}")),
    };
    let limit = |market_flag: Option<i32>| {
        (market_flag == Some(2))
            .then_some(record.price)
            .flatten()
            .filter(|p| *p > Decimal::ZERO)
            .map(Price::new)
    };
    let mut legs: Vec<(StopOrderKind, Decimal, Option<Price>)> = Vec::new();
    match record.order_type {
        // Stop-limit: always a limit order at `price`.
        Some(4) => legs.push((
            StopOrderKind::StopLimit,
            record.stop_price.unwrap_or(Decimal::ZERO),
            record.price.filter(|p| *p > Decimal::ZERO).map(Price::new),
        )),
        Some(6) => legs.push((
            StopOrderKind::StopLoss,
            record.stop_price.unwrap_or(Decimal::ZERO),
            limit(record.market_stop_loss),
        )),
        Some(5) | Some(11) => legs.push((
            StopOrderKind::TakeProfit,
            record.take_price.unwrap_or(Decimal::ZERO),
            limit(record.market_take_profit),
        )),
        Some(7) => {
            legs.push((
                StopOrderKind::StopLoss,
                record.stop_price.unwrap_or(Decimal::ZERO),
                limit(record.market_stop_loss),
            ));
            legs.push((
                StopOrderKind::TakeProfit,
                record.take_price.unwrap_or(Decimal::ZERO),
                limit(record.market_take_profit),
            ));
        }
        _ => return Vec::new(),
    }
    legs.into_iter()
        .map(|(kind, stop_price, limit_price)| StopOrderSnapshot {
            cid,
            broker_id: BrokerOrderId::new(record.order_id.clone()),
            instrument: instrument_id_ticker(ticker, class_code),
            side,
            kind,
            qty: Qty::new(record.order_quantity.unwrap_or(Decimal::ZERO)),
            stop_price: Price::new(stop_price),
            limit_price,
            // The list does not report the lifetime of a conditional order (live question 12).
            expiry: StopExpiry::GoodTillCancel,
            state: state.clone(),
            ts: timestamps(now, None),
        })
        .collect()
}

/// The latest report of an answer with a `data` list (`GET /orders`, `/orders*/sync`) as a snapshot.
/// `known_cid` is the cid the caller asked about (a status answer is about that order); without it the
/// ids of the answer are used.
pub fn map_state_list(
    known_cid: Option<ClientOrderId>,
    resp: &OrderStateListResponse,
    now: Instant,
) -> Option<OrderSnapshot> {
    // The newest report wins (equal or missing times: the later one in the list).
    let data = resp.data.iter().max_by_key(|d| d.transaction_time)?;
    let cid = known_cid
        .or_else(|| parse_cid(&resp.client_order_id))
        .or_else(|| resp.original_client_order_id.as_deref().and_then(parse_cid))?;
    Some(map_execution_data(cid, data, now))
}

/// `true` when the report says the broker refused the order (`orderStatus` 8), with its reason.
pub fn rejection_reason(snap: &OrderSnapshot) -> Option<&str> {
    match &snap.state {
        OrderState::Rejected { reason, .. } => Some(reason.as_str()),
        _ => None,
    }
}

fn map_operation_ack(
    resp: &OrderOperationResponse,
    now: Instant,
    state: OrderState,
) -> OrderSnapshot {
    OrderSnapshot::new(
        resp.client_order_id,
        instrument_id_ticker("", ""),
        state,
        Side::Buy,
        OrderType::Limit,
        Qty::ZERO,
        timestamps(now, None),
    )
    .with_broker_id(Some(BrokerOrderId::new(
        resp.client_order_id.hyphenated().to_string(),
    )))
}

fn map_ws_execution_data(
    cid: ClientOrderId,
    data: &OrderExecutionData,
    now: Instant,
    original_cid: &str,
) -> OrderSnapshot {
    let instrument = instrument_id_ticker(&data.ticker, &data.class_code);
    let state = map_bcs_status(data, cid);
    let side = map_bcs_side(data.side);
    let order_type = data
        .order_type
        .map(map_bcs_order_type)
        .unwrap_or(OrderType::Limit);
    let qty = Qty::new(
        data.order_quantity
            .unwrap_or(data.last_quantity + data.remained_quantity),
    );
    let filled = Qty::new(data.executed_quantity.unwrap_or(data.last_quantity));
    let broker_ts = data.transaction_time.map(super::unix_nanos);
    let _original = parse_cid(original_cid);

    OrderSnapshot::new(
        cid,
        instrument,
        state,
        side,
        order_type,
        qty,
        timestamps(now, broker_ts),
    )
    .with_broker_id(
        data.order_id
            .as_ref()
            .map(|id| BrokerOrderId::new(id.clone())),
    )
    .with_filled(filled)
    .with_price(Some(Price::new(data.price)))
}

fn map_execution_data(
    cid: ClientOrderId,
    data: &RestOrderExecutionData,
    now: Instant,
) -> OrderSnapshot {
    let ticker = data.ticker.as_deref().unwrap_or("");
    let class_code = data.class_code.as_deref().unwrap_or("");
    let instrument = instrument_id_ticker(ticker, class_code);
    let state = map_rest_status(data, cid);
    let side = data.side.as_deref().map(map_side_str).unwrap_or(Side::Buy);
    let order_type = data
        .order_type
        .as_deref()
        .map(map_order_type_str)
        .unwrap_or(OrderType::Limit);
    let qty = Qty::new(data.order_quantity.unwrap_or(Decimal::ZERO));
    let filled = Qty::new(data.executed_quantity.unwrap_or(Decimal::ZERO));

    OrderSnapshot::new(
        cid,
        instrument,
        state,
        side,
        order_type,
        qty,
        timestamps(now, data.transaction_time.map(super::unix_nanos)),
    )
    .with_broker_id(
        data.order_id
            .as_ref()
            .filter(|id| !id.is_empty())
            .map(|id| BrokerOrderId::new(id.clone())),
    )
    .with_filled(filled)
    .with_price(data.price.filter(|p| *p > Decimal::ZERO).map(Price::new))
}

fn parse_cid(raw: &str) -> Option<ClientOrderId> {
    Uuid::parse_str(raw).ok()
}

fn map_bcs_status(data: &OrderExecutionData, cid: ClientOrderId) -> OrderState {
    let broker_id = BrokerOrderId::new(
        data.order_id
            .clone()
            .filter(|id| !id.is_empty())
            .unwrap_or_else(|| cid.hyphenated().to_string()),
    );
    match data.order_status {
        OrderStatus::New
        | OrderStatus::PendingConfirmation
        | OrderStatus::Replaced
        | OrderStatus::PendingReplace => OrderState::Acked { cid, broker_id },
        OrderStatus::PartiallyFilled => {
            let filled = data.executed_quantity.unwrap_or(data.last_quantity);
            OrderState::PartiallyFilled {
                cid,
                filled: Qty::new(filled),
                left: Qty::new(data.remained_quantity),
            }
        }
        OrderStatus::Filled => OrderState::Filled { cid },
        OrderStatus::Cancelled => OrderState::Cancelled { cid },
        // "Cancelling" (6) is not a result: the order is still in the book and may be filled before the
        // cancel is done. It is reported as `Unknown` (the final state comes with status 4 or a fill).
        OrderStatus::PendingCancel => OrderState::Unknown {
            cid,
            since: brokapi_runtime::now(),
        },
        OrderStatus::Rejected => OrderState::Rejected {
            cid,
            reason: "rejected".into(),
        },
        OrderStatus::Unknown => OrderState::Unknown {
            cid,
            since: brokapi_runtime::now(),
        },
    }
}

/// `orderStatus` of a REST report (the same codes as the WebSocket): 0 new, 1 partially filled,
/// 2 filled, 4 cancelled, 5 replaced, 6 cancelling, 8 rejected, 9 replacing, 10 waiting for the new
/// order's confirmation. The orders replaced or still being changed are alive for the caller. 6 is
/// `Unknown`, not `Cancelled`: the order is still alive and may be filled before the cancel is done.
fn map_rest_status(data: &RestOrderExecutionData, cid: ClientOrderId) -> OrderState {
    let broker_id = BrokerOrderId::new(
        data.order_id
            .clone()
            .filter(|id| !id.is_empty())
            .unwrap_or_else(|| cid.hyphenated().to_string()),
    );
    match data.order_status.as_deref() {
        Some("0") | Some("NEW") | Some("5") | Some("9") | Some("10") => {
            OrderState::Acked { cid, broker_id }
        }
        Some("1") => {
            let filled = data.executed_quantity.unwrap_or(Decimal::ZERO);
            OrderState::PartiallyFilled {
                cid,
                filled: Qty::new(filled),
                left: Qty::new(data.remained_quantity.unwrap_or_else(|| {
                    (data.order_quantity.unwrap_or(Decimal::ZERO) - filled).max(Decimal::ZERO)
                })),
            }
        }
        Some("2") => OrderState::Filled { cid },
        Some("4") => OrderState::Cancelled { cid },
        Some("6") => OrderState::Unknown {
            cid,
            since: brokapi_runtime::now(),
        },
        Some("8") => OrderState::Rejected {
            cid,
            reason: data
                .reject_reason
                .clone()
                .filter(|r| !r.is_empty())
                .unwrap_or_else(|| "rejected".into()),
        },
        _ => OrderState::Unknown {
            cid,
            since: brokapi_runtime::now(),
        },
    }
}

fn map_bcs_side(side: OrderSide) -> Side {
    match side {
        OrderSide::Buy => Side::Buy,
        OrderSide::Sell => Side::Sell,
        OrderSide::Unknown => Side::Buy,
    }
}

fn map_bcs_order_type(t: BcsOrderType) -> OrderType {
    match t {
        BcsOrderType::Market => OrderType::Market,
        BcsOrderType::Limit => OrderType::Limit,
        BcsOrderType::Unknown => OrderType::Limit,
    }
}

fn map_side_int(v: Option<i32>) -> Option<Side> {
    match v {
        Some(1) => Some(Side::Buy),
        Some(2) => Some(Side::Sell),
        _ => None,
    }
}

/// Plain order types of the list: market, limit, iceberg and limit-for-30-days (the last two are
/// limit orders). Conditional types are not plain orders.
fn map_order_type_int(v: Option<i32>) -> Option<OrderType> {
    match v {
        Some(1) => Some(OrderType::Market),
        Some(2) | Some(3) | Some(10) => Some(OrderType::Limit),
        _ => None,
    }
}

fn map_side_str(raw: &str) -> Side {
    match raw {
        "1" | "Buy" | "BUY" => Side::Buy,
        _ => Side::Sell,
    }
}

fn map_order_type_str(raw: &str) -> OrderType {
    match raw {
        "1" | "Market" => OrderType::Market,
        _ => OrderType::Limit,
    }
}

/// Builds an [`OrderAck`] for a client order id and the broker's order id. The ack carries the local
/// receive time only, the broker gives no time with it.
pub fn map_order_ack(cid: ClientOrderId, broker_id: BrokerOrderId, now: Instant) -> OrderAck {
    OrderAck {
        cid,
        broker_id,
        ts: timestamps(now, None),
    }
}
