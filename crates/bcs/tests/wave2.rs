//! Wave 2 of the code review of 2026-10-21 for BCS: the limits of the broker's table, the cid of a message
//! of the order channel, a corrected report, a subscription error that fails only its own subscription,
//! tokens that do not show in `Debug`, the memory window of the cid maps. Sans-io, no network.

use std::time::{Duration, Instant};

use brokapi_bcs::core::md::{MdCore, SubState};
use brokapi_bcs::core::ud::UdCore;
use brokapi_bcs::limits::{DEFAULT_LIMITS_TOML, default_limits, limits_from_toml_str};
use brokapi_bcs::map::execution::map_ws_execution;
use brokapi_bcs::map::order::{map_ws_order_message, message_cid};
use brokapi_bcs::{MarketDataConfig, UserDataConfig};
use brokapi_bcs_protocol::config::BcsConfig;
use brokapi_bcs_protocol::models::messages::IncomingMessage;
use brokapi_bcs_protocol::ws::models::OrderMessage;
use brokapi_core::{
    ClientOrderId, Error, Event, InstrumentId, InstrumentKey, NativeId, OrderState, SubscriptionId,
};
use brokapi_runtime::BrokerLimiter;
use brokapi_sansio::{
    BrokerCore, DesiredSubscription, EventSink, SubscriptionKind, SubscriptionSet,
};

fn instrument(symbol: &str) -> InstrumentId {
    InstrumentId::new(
        InstrumentKey::new(symbol, "TQBR"),
        NativeId::TickerClass {
            ticker: symbol.into(),
            class_code: "TQBR".into(),
        },
    )
}

// ----- the limits of the table of the broker -----

#[test]
fn the_limits_follow_the_table_of_the_broker() {
    let limits = default_limits();
    // Every service has its own bucket of 10 requests a second.
    for name in [
        "portfolio",
        "limits",
        "reference",
        "orders",
        "order_status",
        "trades",
        "discounts",
        "market_data",
    ] {
        let b = &limits.buckets[name];
        assert_eq!((b.rate, b.per), (10, Duration::from_secs(1)), "{name}");
    }
    // The operation history is 3 a second.
    let b = &limits.buckets["non_trade"];
    assert_eq!((b.rate, b.per), (3, Duration::from_secs(1)));
    // The orders service has a cap of 200000 a day.
    let daily = &limits.buckets["orders_daily"];
    assert_eq!(
        (daily.rate, daily.burst, daily.per),
        (200_000, 200_000, Duration::from_secs(86_400))
    );
    // A command spends from the second-bucket and the daily one.
    for method in ["CancelOrder", "CancelStopOrder", "SearchOrders"] {
        assert_eq!(
            limits.buckets_for(method),
            vec!["orders", "orders_daily"],
            "{method}"
        );
    }
    // Placing, replacing and stop orders pass a soft gate as well (not a limit of the broker).
    for method in ["PlaceOrder", "ReplaceOrder", "PlaceStopOrder"] {
        assert_eq!(
            limits.buckets_for(method),
            vec!["orders", "orders_daily", "place"],
            "{method}"
        );
    }
    assert_eq!(
        (limits.buckets["place"].rate, limits.buckets["place"].burst),
        (8, 4)
    );
    assert_eq!(limits.buckets_for("GetOperationHistory"), vec!["non_trade"]);
    // A method nobody named (a raw() call) is not free.
    assert_eq!(limits.buckets_for("SomethingNew"), vec!["reference"]);
    assert_eq!(limits.conns.max_conns, 4);
    assert_eq!(limits_from_toml_str(DEFAULT_LIMITS_TOML).unwrap(), limits);
}

#[test]
fn a_burst_of_orders_is_stopped_before_the_broker_would_stop_it() {
    let limiter = BrokerLimiter::new(&default_limits());
    let t0 = Instant::now();
    let mut passed = 0;
    while limiter.acquire("PlaceOrder", t0).is_ok() {
        passed += 1;
        assert!(passed <= 10, "more than the 10 a second of the table");
    }
    // A placement passes the soft gate of 4 first, so cancels keep room in the 10 of the service.
    assert_eq!(passed, 4);
    let mut cancels = 0;
    while limiter.acquire("CancelOrder", t0).is_ok() {
        cancels += 1;
    }
    assert_eq!(cancels, 6, "the orders service has 10 in all, 4 are spent");
    // A second later the buckets have refilled.
    assert!(
        limiter
            .acquire("PlaceOrder", t0 + Duration::from_secs(1))
            .is_ok()
    );
    // The other services are untouched by it.
    assert!(limiter.acquire("GetPortfolio", t0).is_ok());
    assert!(limiter.acquire("GetOrder", t0).is_ok());
}

// ----- the cid of a message of the order channel -----

fn message(client: &str, original: Option<&str>, status: &str, last_qty: u32) -> OrderMessage {
    let original = original
        .map(|o| format!(r#""originalClientOrderId":"{o}","#))
        .unwrap_or_default();
    serde_json::from_str(&format!(
        r#"{{{original}"clientOrderId":"{client}","data":{{"orderStatus":"{status}","executionType":"{status}",
            "ticker":"SBER","classCode":"TQBR","side":"1","orderType":"2","orderQuantity":10,
            "executedQuantity":{last_qty},"lastQuantity":{last_qty},"remainedQuantity":0,"price":250.0,
            "averagePrice":250.0,"executionValue":{},"orderId":"O-1","executionId":"E-1"}}}}"#,
        250 * last_qty
    ))
    .unwrap()
}

const ORDER: &str = "11111111-1111-4111-8111-111111111111";
const REQUEST: &str = "22222222-2222-4222-8222-222222222222";

#[test]
fn the_cid_is_the_order_not_the_request_that_caused_the_message() {
    let order: ClientOrderId = ORDER.parse().unwrap();
    // Only one of the two ids is a UUID: that one.
    assert_eq!(message_cid(&message(ORDER, None, "0", 0)), Some(order));
    assert_eq!(message_cid(&message("", Some(ORDER), "2", 1)), Some(order));
    assert_eq!(message_cid(&message("", None, "0", 0)), None);
    // Both, the same: that one.
    assert_eq!(
        message_cid(&message(ORDER, Some(ORDER), "0", 0)),
        Some(order)
    );
    // A cancel (4), a replace (5) or a cancel in progress (6): the request has its own id, the order is the
    // original one.
    for status in ["4", "5", "6"] {
        assert_eq!(
            message_cid(&message(REQUEST, Some(ORDER), status, 0)),
            Some(order),
            "status {status}"
        );
    }
    // Any other message with both ids: the id of the order that was placed or changed.
    for status in ["0", "1", "2", "8"] {
        assert_eq!(
            message_cid(&message(ORDER, Some(REQUEST), status, 0)),
            Some(order),
            "status {status}"
        );
    }
    // The event carries that cid.
    let Some(Event::Order(snapshot)) =
        map_ws_order_message(&message(REQUEST, Some(ORDER), "4", 0), Instant::now())
    else {
        panic!()
    };
    assert_eq!(snapshot.cid, order);
    assert!(matches!(snapshot.state, OrderState::Cancelled { .. }));
}

#[test]
fn a_corrected_report_is_not_a_new_fill() {
    let now = Instant::now();
    // A fill of 2 pieces (executionType 2): an execution.
    let fill = message(ORDER, None, "2", 2);
    assert!(map_ws_execution(&fill, now, None).is_some());
    // The same message told as a correction (executionType 13): not a new execution.
    let text = r#"{"clientOrderId":"11111111-1111-4111-8111-111111111111","data":{"orderStatus":"2","executionType":"13",
        "ticker":"SBER","classCode":"TQBR","side":"1","orderType":"2","orderQuantity":10,"executedQuantity":2,
        "lastQuantity":2,"remainedQuantity":0,"price":250.0,"averagePrice":250.0,"executionValue":500,
        "orderId":"O-1","executionId":"E-2"}}"#;
    let corrected: OrderMessage = serde_json::from_str(text).unwrap();
    assert!(map_ws_execution(&corrected, now, None).is_none());
    // The order itself is still reported: a correction updates the state.
    assert!(map_ws_order_message(&corrected, now).is_some());
}

#[test]
fn a_corrected_report_does_not_reach_the_stream_of_executions() {
    let mut core = UdCore::new();
    core.apply(&SubscriptionSet::with_subscriptions(vec![
        DesiredSubscription {
            id: SubscriptionId::new(7),
            kind: SubscriptionKind::Executions,
        },
    ]));
    let now = Instant::now();
    let mut out = EventSink::new();
    let send = |core: &mut UdCore, out: &mut EventSink, exec_type: &str, id: &str| {
        let text = format!(
            r#"{{"clientOrderId":"{ORDER}","data":{{"orderStatus":"2","executionType":"{exec_type}","ticker":"SBER",
            "classCode":"TQBR","side":"1","orderType":"2","orderQuantity":10,"executedQuantity":2,"lastQuantity":2,
            "remainedQuantity":0,"price":250.0,"averagePrice":250.0,"executionValue":500,"orderId":"O-1","executionId":"{id}"}}}}"#
        );
        let msg: OrderMessage = serde_json::from_str(&text).unwrap();
        core.on_incoming(IncomingMessage::OrderExecution(msg), now, out);
    };
    send(&mut core, &mut out, "2", "E-1");
    send(&mut core, &mut out, "13", "E-1-corrected");
    let fills = out
        .drain()
        .into_iter()
        .filter(|e| matches!(e.payload, Event::Execution(_)))
        .count();
    assert_eq!(fills, 1, "the correction is not counted as a second fill");
}

// ----- a subscription error fails only its own subscription -----

fn md_with(symbols: &[&str]) -> (MdCore, Vec<SubscriptionId>) {
    let mut core = MdCore::new();
    let ids: Vec<_> = (1..=symbols.len() as u64)
        .map(SubscriptionId::new)
        .collect();
    core.apply(&SubscriptionSet::with_subscriptions(
        symbols
            .iter()
            .zip(&ids)
            .map(|(s, id)| DesiredSubscription {
                id: *id,
                kind: SubscriptionKind::Quotes {
                    instrument: instrument(s),
                },
            })
            .collect(),
    ));
    (core, ids)
}

fn error_frame(text: &str) -> String {
    format!(
        r#"{{"responseType":"ErrorResponse","errors":[{{"code":"NOT_FOUND","message":"{text}"}}]}}"#
    )
}

#[test]
fn an_error_that_names_a_ticker_fails_that_subscription_only() {
    let (mut core, ids) = md_with(&["SBER", "GAZP", "LKOH"]);
    let mut out = EventSink::new();
    core.feed_raw(
        &error_frame("instrument GAZP not found"),
        Instant::now(),
        &mut out,
    );
    assert_eq!(core.actual_state(ids[0]), Some(SubState::Pending));
    assert_eq!(core.actual_state(ids[1]), Some(SubState::Failed));
    assert_eq!(core.actual_state(ids[2]), Some(SubState::Pending));
}

#[test]
fn with_one_subscription_pending_an_error_is_about_it() {
    let (mut core, ids) = md_with(&["SBER"]);
    let mut out = EventSink::new();
    core.feed_raw(
        &error_frame("something went wrong"),
        Instant::now(),
        &mut out,
    );
    assert_eq!(core.actual_state(ids[0]), Some(SubState::Failed));
}

#[test]
fn an_error_that_cannot_be_placed_fails_every_pending_subscription() {
    let (mut core, ids) = md_with(&["SBER", "GAZP"]);
    let mut out = EventSink::new();
    core.feed_raw(&error_frame("bad request"), Instant::now(), &mut out);
    assert!(
        ids.iter()
            .all(|id| core.actual_state(*id) == Some(SubState::Failed))
    );
}

#[test]
fn a_subscription_that_is_active_already_is_not_failed_by_a_later_error() {
    let (mut core, ids) = md_with(&["SBER", "GAZP"]);
    let mut out = EventSink::new();
    // SBER is confirmed.
    core.on_frame(
        parse(r#"{"responseType":"QuotesSuccess","instruments":[{"ticker":"SBER","classCode":"TQBR"}]}"#),
        Instant::now(),
        &mut out,
    );
    assert_eq!(core.actual_state(ids[0]), Some(SubState::Active));
    core.feed_raw(&error_frame("GAZP not found"), Instant::now(), &mut out);
    assert_eq!(core.actual_state(ids[0]), Some(SubState::Active));
    assert_eq!(core.actual_state(ids[1]), Some(SubState::Failed));
}

fn parse(text: &str) -> IncomingMessage {
    brokapi_bcs_protocol::ws::parse_incoming(
        text,
        brokapi_bcs_protocol::ws::client::BcsWsChannel::MarketData,
    )
}

// ----- tokens do not show in Debug -----

#[test]
fn the_configs_of_the_connectors_do_not_print_the_token() {
    let bcs = BcsConfig::new("refresh-secret-123".to_string());
    let md = MarketDataConfig::new(bcs.clone(), "bcs-md");
    let ud = UserDataConfig::new(bcs, "bcs-ud");
    for shown in [format!("{md:?}"), format!("{ud:?}")] {
        assert!(!shown.contains("refresh-secret-123"), "{shown}");
        assert!(shown.contains("<redacted>"), "{shown}");
    }
}

// ----- the memory window of the maps -----

#[test]
fn the_ids_of_orders_that_finished_long_ago_are_forgotten() {
    let mut core = UdCore::new();
    let t0 = Instant::now();
    let mut out = EventSink::new();
    let order = |client: &str, order_id: &str| -> OrderMessage {
        serde_json::from_str(&format!(
            r#"{{"clientOrderId":"{client}","data":{{"orderStatus":"2","executionType":"2","ticker":"SBER",
            "classCode":"TQBR","side":"1","orderType":"2","orderQuantity":1,"executedQuantity":1,"lastQuantity":0,
            "remainedQuantity":0,"price":250.0,"orderId":"{order_id}"}}}}"#
        ))
        .unwrap()
    };
    core.orders_mut().register_place(ORDER.parse().unwrap(), t0);
    core.on_incoming(
        IncomingMessage::OrderExecution(order(ORDER, "O-1")),
        t0,
        &mut out,
    );
    assert_eq!(core.cid_of_order_id("O-1"), Some(ORDER.parse().unwrap()));

    // A day and a second later another order finishes: the first one is past the window.
    let later = t0 + brokapi_runtime::RETENTION + Duration::from_secs(1);
    core.on_incoming(
        IncomingMessage::OrderExecution(order(REQUEST, "O-2")),
        later,
        &mut out,
    );
    assert_eq!(core.cid_of_order_id("O-1"), None);
    assert_eq!(core.cid_of_order_id("O-2"), Some(REQUEST.parse().unwrap()));
    assert!(core.orders().get(ORDER.parse().unwrap()).is_none());
}

#[test]
fn the_error_type_of_a_command_that_is_not_to_be_repeated() {
    // Transport on a command is not retried; a rate limit is.
    let cut = Error::Transport(brokapi_core::TransportError::Message("cut".into()));
    assert!(!cut.retryable_for_command());
    assert!(
        Error::RateLimited {
            retry_after: Duration::from_secs(1)
        }
        .retryable_for_command()
    );
}

// ----- the statuses of the order channel, of the status call and of the list -----

fn ws_with(status: &str, extra: &str) -> OrderMessage {
    serde_json::from_str(&format!(
        r#"{{"clientOrderId":"{ORDER}","data":{{"orderStatus":"{status}","ticker":"SBER","classCode":"TQBR",
            "orderQuantity":10,"executedQuantity":4,"lastQuantity":0,"remainedQuantity":6,"price":250.0,
            "orderId":"O-1"{extra}}}}}"#
    ))
    .unwrap()
}

fn state_of(message: &OrderMessage) -> OrderState {
    match map_ws_order_message(message, Instant::now()) {
        Some(Event::Order(o)) => o.state,
        other => panic!("{other:?}"),
    }
}

#[test]
fn every_status_of_the_order_channel_has_its_state() {
    use OrderState as S;
    for (status, want) in [
        ("0", "Acked"),
        ("10", "Acked"),
        ("5", "Acked"),
        ("9", "Acked"),
        ("1", "PartiallyFilled"),
        ("2", "Filled"),
        ("4", "Cancelled"),
        ("6", "Unknown"),
        ("8", "Rejected"),
        ("77", "Unknown"),
    ] {
        let state = state_of(&ws_with(status, r#","side":"1","orderType":"2""#));
        assert_eq!(state.name(), want, "status {status}: {state:?}");
    }
    // A partly filled order carries what is filled and what is left.
    let S::PartiallyFilled { filled, left, .. } = state_of(&ws_with("1", "")) else {
        panic!()
    };
    assert_eq!(
        (filled, left),
        (brokapi_core::Qty::from(4), brokapi_core::Qty::from(6))
    );
}

#[test]
fn side_type_and_prices_of_a_message_are_read_or_defaulted() {
    use brokapi_core::{OrderType, Side};
    let snapshot = |extra: &str| match map_ws_order_message(&ws_with("0", extra), Instant::now()) {
        Some(Event::Order(o)) => o,
        other => panic!("{other:?}"),
    };
    let sell_market = snapshot(r#","side":"2","orderType":"1""#);
    assert_eq!(
        (sell_market.side, sell_market.order_type),
        (Side::Sell, OrderType::Market)
    );
    // An unknown side is a buy and no type is a limit order, in the event (a fill is dropped instead).
    let plain = snapshot("");
    assert_eq!(
        (plain.side, plain.order_type),
        (Side::Buy, OrderType::Limit)
    );
    assert_eq!(plain.broker_id.as_ref().unwrap().as_str(), "O-1");
    assert_eq!(plain.qty, brokapi_core::Qty::from(10));
}

fn rest_state(status: &str, extra: &str) -> OrderState {
    use brokapi_bcs_protocol::http::models::OrderStateListResponse;
    let resp: OrderStateListResponse = serde_json::from_str(&format!(
        r#"{{"clientOrderId":"request","originalClientOrderId":"{ORDER}","data":[{{"orderStatus":"{status}",
            "ticker":"SBER","classCode":"TQBR","orderQuantity":10,"executedQuantity":4,"price":250.0,
            "orderId":"O-1"{extra}}}]}}"#
    ))
    .unwrap();
    brokapi_bcs::map::order::map_state_list(None, &resp, Instant::now())
        .unwrap()
        .state
}

#[test]
fn every_status_of_the_status_call_has_its_state() {
    for (status, want) in [
        ("0", "Acked"),
        ("NEW", "Acked"),
        ("5", "Acked"),
        ("9", "Acked"),
        ("10", "Acked"),
        ("1", "PartiallyFilled"),
        ("2", "Filled"),
        ("4", "Cancelled"),
        ("6", "Unknown"),
        ("8", "Rejected"),
        ("?", "Unknown"),
    ] {
        assert_eq!(rest_state(status, "").name(), want, "status {status}");
    }
    // A refusal carries the reason of the broker, or a word when there is none.
    let OrderState::Rejected { reason, .. } =
        rest_state("8", r#","rejectReason":"price out of band""#)
    else {
        panic!()
    };
    assert_eq!(reason, "price out of band");
    let OrderState::Rejected { reason, .. } = rest_state("8", r#","rejectReason":"""#) else {
        panic!()
    };
    assert_eq!(reason, "rejected");
    // What is left of a partly filled order is computed when the broker does not say.
    let OrderState::PartiallyFilled { left, .. } = rest_state("1", "") else {
        panic!()
    };
    assert_eq!(left, brokapi_core::Qty::from(6));
    let OrderState::PartiallyFilled { left, .. } = rest_state("1", r#","remainedQuantity":5"#)
    else {
        panic!()
    };
    assert_eq!(left, brokapi_core::Qty::from(5));
}

fn record(status: i32, extra: &str) -> brokapi_bcs_protocol::http::models::OrderRecord {
    serde_json::from_str(&format!(
        r#"{{"orderId":"O-9","ticker":"SBER","classCode":"TQBR","side":1,"orderType":2,"orderStatus":{status},
            "orderQuantity":10,"executedQuantity":0,"price":250.0{extra}}}"#
    ))
    .unwrap()
}

#[test]
fn the_list_of_orders_gives_states_and_leaves_out_what_it_cannot_read() {
    use brokapi_bcs::map::order::map_order_record;
    let cid: ClientOrderId = ORDER.parse().unwrap();
    let now = Instant::now();
    let state = |r: &_| map_order_record(r, cid, now).unwrap().state.name();
    assert_eq!(state(&record(1, "")), "Cancelled");
    assert_eq!(state(&record(2, "")), "Filled");
    assert_eq!(state(&record(3, "")), "Acked");
    assert_eq!(state(&record(9, "")), "Unknown");
    // Active and partly filled: the rest is computed, or told.
    let part = map_order_record(&record(3, "").tap_executed(4), cid, now).unwrap();
    let OrderState::PartiallyFilled { left, .. } = part.state else {
        panic!()
    };
    assert_eq!(left, brokapi_core::Qty::from(6));
    // An iceberg and a limit order for 30 days are limit orders. A conditional type, or an unknown side, is not a plain order.
    for (order_type, want) in [
        (1, Some("Market")),
        (2, Some("Limit")),
        (3, Some("Limit")),
        (10, Some("Limit")),
        (4, None),
    ] {
        let mut r = record(3, "");
        r.order_type = Some(order_type);
        let mapped = map_order_record(&r, cid, now).map(|o| format!("{:?}", o.order_type));
        assert_eq!(mapped.as_deref(), want, "type {order_type}");
    }
    let mut r = record(3, "");
    r.side = Some(7);
    assert!(map_order_record(&r, cid, now).is_none());
    let mut r = record(3, "");
    r.ticker = None;
    assert!(map_order_record(&r, cid, now).is_none());
}

trait TapExecuted {
    fn tap_executed(self, n: i64) -> Self;
}

impl TapExecuted for brokapi_bcs_protocol::http::models::OrderRecord {
    fn tap_executed(mut self, n: i64) -> Self {
        self.executed_quantity = Some(n.into());
        self
    }
}
