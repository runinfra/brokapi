//! Level-1 sans-io tests - no network.

use brokapi_core::Money;
use brokapi_core::{Decimal, dec};
use std::time::Instant;

use brokapi_bcs::core::md::MdCore;
use brokapi_bcs::core::ud::{UdCore, UdFrame, UdRestFrame};
use brokapi_bcs::map::market::{map_rest_last_trade, map_rest_order_book, parse_subscribe_error};
use brokapi_bcs_protocol::http::models::{
    InstrumentInfo, LastTradesResponse, OrderBookResponse, OrderOperationResponse,
    OrderStatusResponse,
};
use brokapi_bcs_protocol::models::messages::IncomingMessage;
use brokapi_bcs_protocol::ws::client::BcsWsChannel;
use brokapi_bcs_protocol::ws::models::MarketDataMessage;
use brokapi_bcs_protocol::ws::parse_incoming;
use brokapi_core::{Event, InstrumentId, InstrumentKey, NativeId, OrderState, SubscriptionId};
use brokapi_sansio::{
    BrokerCore, BufferPush, DesiredSubscription, Harness, JsonFrameBuffer, OrderFsm, OrderRegistry,
    OrderSignal, SubscriptionKind, SubscriptionSet,
};

fn fixture(name: &str) -> String {
    let path = format!(
        "{}/../bcs-protocol/tests/fixtures/{}",
        env!("CARGO_MANIFEST_DIR"),
        name
    );
    std::fs::read_to_string(path).expect("fixture file")
}

fn sber() -> InstrumentId {
    InstrumentId::new(
        InstrumentKey::new("SBER", "TQBR"),
        NativeId::TickerClass {
            ticker: "SBER".into(),
            class_code: "TQBR".into(),
        },
    )
}

fn now() -> Instant {
    Instant::now()
}

// ─── REST parsing via mappers ────────────────────────────────────────────────

#[test]
fn parse_rest_order_book_fixture() {
    let book: OrderBookResponse = serde_json::from_str(&fixture("rest/order_book.json")).unwrap();
    let event = map_rest_order_book(&book, now());
    match event {
        Event::OrderBook(brokapi_core::OrderBook::Snapshot { bids, asks, .. }) => {
            assert_eq!(bids.len(), 2);
            assert_eq!(asks.len(), 2);
        }
        other => panic!("expected snapshot, got {other:?}"),
    }
}

#[test]
fn parse_rest_last_trades_fixture() {
    let trades: LastTradesResponse =
        serde_json::from_str(&fixture("rest/last_trades.json")).unwrap();
    assert_eq!(trades.len(), 2);
    let trade = map_rest_last_trade(&trades[0], &sber(), now()).expect("a known side");
    assert!(matches!(trade.side, brokapi_core::Side::Buy));
}

/// The tape as the docs describe it: `{"records": [...]}`, side `1` / `2`, no instrument in a record.
#[test]
fn parse_rest_last_trades_documented_shape() {
    let v: serde_json::Value =
        serde_json::from_str(&fixture("rest/last_trades_records.json")).unwrap();
    let records: LastTradesResponse = serde_json::from_value(v["records"].clone()).unwrap();
    assert_eq!(records.len(), 2);
    assert!(records[0].ticker.is_none());
    let sell = map_rest_last_trade(&records[0], &sber(), now()).unwrap();
    assert!(matches!(sell.side, brokapi_core::Side::Sell));
    assert_eq!(sell.instrument.key.display(), "SBER@TQBR");
    // a side that is neither 1 nor 2 is dropped, not guessed
    let mut odd = records[0].clone();
    odd.side = Some("?".into());
    assert!(map_rest_last_trade(&odd, &sber(), now()).is_none());
}

#[test]
fn parse_rest_instrument_info_fixture() {
    let info: InstrumentInfo = serde_json::from_str(&fixture("rest/instrument_info.json")).unwrap();
    let params = info.trading_params_for_board("TQBR").expect("params");
    assert_eq!(params.ticker, "SBER");
}

#[test]
fn parse_rest_place_order_fixture() {
    let resp: OrderOperationResponse =
        serde_json::from_str(&fixture("rest/place_order.json")).unwrap();
    assert_eq!(resp.status, "OK");
}

// ─── WS parsing ──────────────────────────────────────────────────────────────

#[test]
fn parse_ws_order_book_fixture() {
    let msg: MarketDataMessage = serde_json::from_str(&fixture("ws/order_book.json")).unwrap();
    assert!(matches!(msg, MarketDataMessage::OrderBook(_)));
}

#[test]
fn parse_ws_quotes_fixture() {
    let msg: MarketDataMessage = serde_json::from_str(&fixture("ws/quotes.json")).unwrap();
    assert!(matches!(msg, MarketDataMessage::Quotes(_)));
}

#[test]
fn parse_ws_order_book_error_fixture() {
    let err = parse_subscribe_error(&fixture("ws/order_book_error.json")).expect("error");
    assert!(err.contains("instruments"));
}

#[test]
fn parse_ws_order_execution_fixture() {
    let parsed = parse_incoming(
        &fixture("ws/order_execution.json"),
        BcsWsChannel::OrderExecution,
    );
    assert!(matches!(parsed, IncomingMessage::OrderExecution(_)));
}

#[test]
fn parse_ws_portfolio_fixture() {
    let parsed = parse_incoming(&fixture("ws/portfolio.json"), BcsWsChannel::Portfolio);
    assert!(matches!(parsed, IncomingMessage::Portfolio(_)));
}

// ─── JSON mid-message break ──────────────────────────────────────────────────

#[test]
fn json_buffer_handles_split_frames() {
    let raw = fixture("ws/order_book.json");
    let split = raw.len() / 2;
    let mut buf = JsonFrameBuffer::new();
    assert_eq!(buf.push(&raw[..split]), BufferPush::Incomplete);
    match buf.push(&raw[split..]) {
        BufferPush::Complete(frames) => {
            assert_eq!(frames.len(), 1);
            let msg: MarketDataMessage = serde_json::from_str(&frames[0]).unwrap();
            assert!(matches!(msg, MarketDataMessage::OrderBook(_)));
        }
        other => panic!("expected complete frame, got {other:?}"),
    }
}

// ─── MD core harness + fixtures ──────────────────────────────────────────────

#[test]
fn md_core_replays_order_book_fixture() {
    let start = now();
    let mut h = Harness::new(MdCore::new(), start);
    let sub_id = SubscriptionId::new(1);
    h.apply(&SubscriptionSet::with_subscriptions(vec![
        DesiredSubscription {
            id: sub_id,
            kind: SubscriptionKind::OrderBook {
                instrument: sber(),
                depth: 20,
            },
        },
    ]));
    assert!(!h.transmitted().is_empty());

    let success: MarketDataMessage =
        serde_json::from_str(&fixture("ws/order_book_success.json")).unwrap();
    h.feed_frame(IncomingMessage::MarketData(success));

    let book: MarketDataMessage = serde_json::from_str(&fixture("ws/order_book.json")).unwrap();
    h.feed_frame(IncomingMessage::MarketData(book));

    let events = h.events();
    assert!(
        events
            .iter()
            .any(|e| matches!(e.payload, Event::OrderBook(_))),
        "expected order book event"
    );
}

#[test]
fn md_core_subscription_error_marks_failed() {
    let mut core = MdCore::new();
    let sub_id = SubscriptionId::new(7);
    core.apply(&SubscriptionSet::with_subscriptions(vec![
        DesiredSubscription {
            id: sub_id,
            kind: SubscriptionKind::Quotes { instrument: sber() },
        },
    ]));

    let mut sink = brokapi_sansio::EventSink::new();
    core.feed_raw(&fixture("ws/order_book_error.json"), now(), &mut sink);
    assert_eq!(
        core.actual_state(sub_id),
        Some(brokapi_bcs::core::md::SubState::Failed)
    );
    assert!(sink.is_empty());
}

/// What the connector really feeds the core: the session parses the frame, and the refusal of a subscribe
/// (a list `errors` where the data should be) is a shape the parser does not know, so it arrives as text. The
/// subscription must still fail, with the reason of the broker.
#[test]
fn md_core_refusal_that_arrives_as_text_marks_failed_with_its_reason() {
    use brokapi_bcs_protocol::ws::parse_incoming;

    let mut core = MdCore::new();
    let sub_id = SubscriptionId::new(7);
    core.apply(&SubscriptionSet::with_subscriptions(vec![
        DesiredSubscription {
            id: sub_id,
            kind: SubscriptionKind::Quotes { instrument: sber() },
        },
    ]));

    let frame = parse_incoming(
        &fixture("ws/order_book_error.json"),
        BcsWsChannel::MarketData,
    );
    assert!(
        matches!(frame, IncomingMessage::Unknown(_)),
        "the shape the parser does not know"
    );
    let mut sink = brokapi_sansio::EventSink::new();
    core.on_frame(frame, now(), &mut sink);
    assert_eq!(
        core.actual_state(sub_id),
        Some(brokapi_bcs::core::md::SubState::Failed)
    );
    assert!(
        core.failure(sub_id)
            .is_some_and(|r| r.contains("instruments"))
    );
    assert!(sink.is_empty());
}

// ─── UD core ─────────────────────────────────────────────────────────────────

#[test]
fn ud_core_rest_place_and_ws_execution() {
    let start = now();
    let mut h = Harness::new(UdCore::new(), start);
    let cid = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
    h.core_mut().register_place(cid, start);

    let place: OrderOperationResponse =
        serde_json::from_str(&fixture("rest/place_order.json")).unwrap();
    h.feed_frame(UdFrame::Rest(UdRestFrame::Place(place)));

    h.feed_frame(UdFrame::Ws {
        channel: BcsWsChannel::OrderExecution,
        text: fixture("ws/order_execution.json"),
    });

    assert!(
        h.events()
            .iter()
            .any(|e| matches!(e.payload, Event::Order(_)))
    );
    let fsm = h.core().orders().get(cid).expect("order registered");
    assert!(matches!(fsm.state(), OrderState::Acked { .. }));
}

fn executions_sub(id: u64) -> SubscriptionSet {
    SubscriptionSet::with_subscriptions(vec![DesiredSubscription {
        id: SubscriptionId::new(id),
        kind: SubscriptionKind::Executions,
    }])
}

fn execution_events(h: &Harness<UdCore>) -> Vec<brokapi_core::Execution> {
    h.events()
        .iter()
        .filter_map(|e| match &e.payload {
            Event::Execution(x) => Some(x.clone()),
            _ => None,
        })
        .collect()
}

#[test]
fn ud_core_fill_becomes_one_execution_and_duplicates_are_dropped() {
    let mut h = Harness::new(UdCore::new(), now());
    h.apply(&executions_sub(7));
    let fill = fixture("ws/order_execution_fill.json");
    for _ in 0..3 {
        h.feed_frame(UdFrame::Ws {
            channel: BcsWsChannel::OrderExecution,
            text: fill.clone(),
        });
    }
    let execs = execution_events(&h);
    assert_eq!(execs.len(), 1, "one executionId is one Execution");
    let x = &execs[0];
    assert_eq!(x.trade_id, "TQBR-Z3fE7c-S-1-1-N");
    assert_eq!(x.qty.decimal(), dec!(50.0));
    // executionValue / lastQuantity = 12225 / 50, not the order price 245
    assert_eq!(x.price.decimal(), dec!(244.5));
    assert_eq!(x.commission, Money::new(dec!(6.1)));
    assert_eq!(x.side, brokapi_core::Side::Buy);
    assert_eq!(x.broker_order_id.as_str(), "20241030-TQBR-12345678910");
    // clientOrderId is empty in the message: the original one is taken
    assert_eq!(
        x.cid,
        Some(uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap())
    );
    let sub = h
        .events()
        .iter()
        .find(|e| matches!(e.payload, Event::Execution(_)))
        .unwrap()
        .sub;
    assert_eq!(sub, Some(SubscriptionId::new(7)));
}

#[test]
fn ud_core_message_without_a_fill_is_no_execution() {
    let mut h = Harness::new(UdCore::new(), now());
    h.apply(&executions_sub(7));
    // lastQuantity = 0: an order status, not a fill
    h.feed_frame(UdFrame::Ws {
        channel: BcsWsChannel::OrderExecution,
        text: fixture("ws/order_execution.json"),
    });
    // a fill without executionId is not a fill we can de-duplicate
    let no_id = fixture("ws/order_execution_fill.json")
        .replace("\"executionId\": \"TQBR-Z3fE7c-S-1-1-N\",", "");
    h.feed_frame(UdFrame::Ws {
        channel: BcsWsChannel::OrderExecution,
        text: no_id,
    });
    assert!(execution_events(&h).is_empty());
    assert!(
        h.events()
            .iter()
            .any(|e| matches!(e.payload, Event::Order(_)))
    );
}

#[test]
fn ud_core_without_executions_subscription_emits_no_execution() {
    let mut h = Harness::new(UdCore::new(), now());
    h.feed_frame(UdFrame::Ws {
        channel: BcsWsChannel::OrderExecution,
        text: fixture("ws/order_execution_fill.json"),
    });
    assert!(execution_events(&h).is_empty());
}

#[test]
fn ud_core_fills_only_from_the_execution_channel() {
    let mut h = Harness::new(UdCore::new(), now());
    h.apply(&executions_sub(7));
    h.feed_frame(UdFrame::Ws {
        channel: BcsWsChannel::OrderTransaction,
        text: fixture("ws/order_execution_fill.json"),
    });
    assert!(execution_events(&h).is_empty());
}

#[test]
fn ud_core_portfolio_ws_fixture() {
    let mut h = Harness::new(UdCore::new(), now());
    h.apply(&SubscriptionSet::with_subscriptions(vec![
        DesiredSubscription {
            id: SubscriptionId::new(2),
            kind: SubscriptionKind::Portfolio,
        },
    ]));
    h.feed_frame(UdFrame::Ws {
        channel: BcsWsChannel::Portfolio,
        text: fixture("ws/portfolio.json"),
    });
    assert!(
        h.events()
            .iter()
            .any(|e| matches!(e.payload, Event::Portfolio(_)))
    );
}

// ─── Order FSM (all transitions) ─────────────────────────────────────────────

#[test]
fn order_fsm_all_main_transitions() {
    let cid = uuid::Uuid::from_bytes([1; 16]);
    let t = now();
    let mut fsm = OrderFsm::new(cid, t);

    fsm.apply(
        OrderSignal::Ack {
            broker_id: brokapi_core::BrokerOrderId::new("1"),
        },
        t,
    )
    .unwrap();
    fsm.apply(
        OrderSignal::PartialFill {
            filled: brokapi_core::Qty::from(1),
            left: brokapi_core::Qty::from(9),
        },
        t,
    )
    .unwrap();
    fsm.apply(OrderSignal::Filled, t).unwrap();
    assert!(matches!(fsm.state(), OrderState::Filled { .. }));
}

#[test]
fn order_fsm_timeout_from_in_flight() {
    let cid = uuid::Uuid::from_bytes([2; 16]);
    let mut fsm = OrderFsm::new(cid, now());
    fsm.apply(OrderSignal::Timeout, now()).unwrap();
    assert!(matches!(fsm.state(), OrderState::Unknown { .. }));
}

#[test]
fn order_registry_command_timeout() {
    let cid = uuid::Uuid::from_bytes([3; 16]);
    let t0 = now();
    let mut reg = OrderRegistry::new();
    reg.register_place(cid, t0);
    let changed = reg.on_global_timeout(
        t0 + std::time::Duration::from_secs(31),
        std::time::Duration::from_secs(30),
    );
    assert_eq!(changed.len(), 1);
}

#[test]
fn order_fsm_reject_from_in_flight() {
    let cid = uuid::Uuid::from_bytes([4; 16]);
    let mut fsm = OrderFsm::new(cid, now());
    fsm.apply(
        OrderSignal::Rejected {
            reason: "bad price".into(),
        },
        now(),
    )
    .unwrap();
    assert!(matches!(fsm.state(), OrderState::Rejected { .. }));
}

#[test]
fn order_fsm_cancel_from_acked() {
    let cid = uuid::Uuid::from_bytes([5; 16]);
    let t = now();
    let mut fsm = OrderFsm::new(cid, t);
    fsm.apply(
        OrderSignal::Ack {
            broker_id: brokapi_core::BrokerOrderId::new("1"),
        },
        t,
    )
    .unwrap();
    fsm.apply(OrderSignal::Cancelled, t).unwrap();
    assert!(matches!(fsm.state(), OrderState::Cancelled { .. }));
}

// ─── REST order state ────────────────────────────────────────────────────────

#[test]
fn parse_rest_order_status_response() {
    let resp = OrderStatusResponse {
        client_order_id: "550e8400-e29b-41d4-a716-446655440000".into(),
        original_client_order_id: "550e8400-e29b-41d4-a716-446655440000".into(),
        data: Some(brokapi_bcs_protocol::http::models::OrderExecutionData {
            message_type: None,
            order_status: Some("0".into()),
            execution_type: None,
            order_quantity: Some(dec!(1.0)),
            executed_quantity: Some(dec!(0.0)),
            last_quantity: None,
            remained_quantity: None,
            ticker: Some("SBER".into()),
            class_code: Some("TQBR".into()),
            side: Some("1".into()),
            order_type: Some("2".into()),
            average_price: None,
            order_id: Some("99".into()),
            execution_id: None,
            price: Some(dec!(100.0)),
            currency: None,
            client_code: None,
            transaction_time: None,
            order_number: None,
            accrued_coupon: None,
            execution_value: None,
            commission: None,
            security_exchange: None,
            ..Default::default()
        }),
    };
    let event = brokapi_bcs::map::order::map_order_status(&resp, now()).expect("event");
    assert!(matches!(event, Event::Order(_)));
}

// ─── orders/events/ws (current status channel) ──────────────────────────────

/// A pure status message of the events channel has no fill fields and an empty `clientOrderId`
/// (the docs sample for cancel does the same): it is still an order event, under the original cid.
#[test]
fn events_channel_status_message_without_fill_fields_is_an_order_event() {
    let mut h = Harness::new(UdCore::new(), now());
    let msg = r#"{"originalClientOrderId":"550e8400-e29b-41d4-a716-446655440000","clientOrderId":"",
        "data":{"orderStatus":"4","executionType":"4","ticker":"SBER","classCode":"TQBR","side":"1",
        "orderType":"2","orderId":"20261019-TQBR-79000001"}}"#;
    h.feed_frame(UdFrame::Ws {
        channel: BcsWsChannel::OrderEvents,
        text: msg.into(),
    });
    let order = h
        .events()
        .iter()
        .find_map(|e| match &e.payload {
            Event::Order(o) => Some(o.clone()),
            _ => None,
        })
        .expect("order event");
    assert_eq!(
        order.cid.to_string(),
        "550e8400-e29b-41d4-a716-446655440000"
    );
    assert!(matches!(order.state, OrderState::Cancelled { .. }));
    assert_eq!(
        order.broker_id.as_ref().map(|b| b.as_str()),
        Some("20261019-TQBR-79000001")
    );
    // The core now knows which cid the broker's order id is.
    assert_eq!(
        h.core()
            .cid_of_order_id("20261019-TQBR-79000001")
            .map(|c| c.to_string()),
        Some("550e8400-e29b-41d4-a716-446655440000".to_string())
    );
}

#[test]
fn events_channel_partial_fill_carries_filled_and_left() {
    let mut h = Harness::new(UdCore::new(), now());
    h.feed_frame(UdFrame::Ws {
        channel: BcsWsChannel::OrderEvents,
        text: fixture("ws/order_execution_fill.json"),
    });
    let order = h
        .events()
        .iter()
        .find_map(|e| match &e.payload {
            Event::Order(o) => Some(o.clone()),
            _ => None,
        })
        .expect("order event");
    match order.state {
        OrderState::PartiallyFilled { filled, left, .. } => {
            assert_eq!((filled.decimal(), left.decimal()), (dec!(50.0), dec!(50.0)))
        }
        other => panic!("{other:?}"),
    }
}
