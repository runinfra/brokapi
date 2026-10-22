//! Edges of the sans-io cores of BCS: answers of REST calls given by hand, the answer timer, the refusal frame of the
//! market data channel and the defaults.

use std::time::{Duration, Instant};

use brokapi_bcs::core::md::{MdCore, SubState};
use brokapi_bcs::core::ud::{UdCore, UdFrame, UdRestFrame};
use brokapi_bcs_protocol::http::models::{OrderOperationResponse, OrderStatusResponse};
use brokapi_bcs_protocol::ws::client::BcsWsChannel;
use brokapi_bcs_protocol::ws::parse_incoming;
use brokapi_core::{
    Event, InstrumentId, InstrumentKey, NativeId, OrderState, Side, SubscriptionId,
};
use brokapi_sansio::{
    BrokerCore, DesiredSubscription, EventSink, Harness, SubscriptionKind, SubscriptionSet,
};
use uuid::Uuid;

fn sber() -> InstrumentId {
    InstrumentId::new(
        InstrumentKey::new("SBER", "TQBR"),
        NativeId::TickerClass {
            ticker: "SBER".into(),
            class_code: "TQBR".into(),
        },
    )
}

fn operation(cid: Uuid) -> OrderOperationResponse {
    serde_json::from_value(serde_json::json!({"clientOrderId": cid, "status": "OK"})).unwrap()
}

#[test]
fn the_cores_start_empty_with_default() {
    let ud = UdCore::default();
    assert!(ud.poll_timeout().is_none());
    assert!(ud.orders().get(Uuid::new_v4()).is_none());
    let md = MdCore::default();
    assert!(md.poll_timeout().is_none());
}

#[test]
fn a_cancel_and_a_replace_answer_move_the_order_without_an_event() {
    let start = Instant::now();
    let mut h = Harness::new(UdCore::new(), start);
    let (first, second) = (Uuid::new_v4(), Uuid::new_v4());
    h.core_mut().register_place(first, start);
    h.feed_frame(UdFrame::Rest(UdRestFrame::Place(operation(first))));
    let events = h.events().len();
    h.feed_frame(UdFrame::Rest(UdRestFrame::Cancel(operation(first))));
    assert!(matches!(
        h.core().orders().get(first).unwrap().state(),
        OrderState::Cancelled { .. }
    ));
    h.core_mut().register_place(second, start);
    h.feed_frame(UdFrame::Rest(UdRestFrame::Replace(operation(second))));
    assert!(matches!(
        h.core().orders().get(second).unwrap().state(),
        OrderState::Acked { .. }
    ));
    assert_eq!(
        h.events().len(),
        events,
        "no event for a cancel or a replace"
    );
}

#[test]
fn a_status_read_gives_an_order_event_and_a_list_of_active_orders_is_ignored() {
    let start = Instant::now();
    let mut h = Harness::new(UdCore::new(), start);
    let cid = Uuid::new_v4();
    let status: OrderStatusResponse = serde_json::from_value(serde_json::json!({
        "clientOrderId": cid.to_string(),
        "originalClientOrderId": cid.to_string(),
        "data": {"orderStatus": "1", "executionType": "1", "orderQuantity": 10, "executedQuantity": 4,
                 "remainedQuantity": 6, "ticker": "SBER", "classCode": "TQBR", "side": "1",
                 "orderType": "2", "price": 300.0}
    }))
    .unwrap();
    h.feed_frame(UdFrame::Rest(UdRestFrame::OrderState(status)));
    let order = h
        .events()
        .iter()
        .find_map(|e| match &e.payload {
            Event::Order(o) => Some(o.clone()),
            _ => None,
        })
        .expect("an order event");
    assert_eq!(order.cid, cid);
    assert!(matches!(order.state, OrderState::PartiallyFilled { .. }));
    assert_eq!(order.side, Side::Buy);
    let search = serde_json::from_value(
        serde_json::json!({"records": [], "totalRecords": 0, "totalPages": 0}),
    )
    .unwrap();
    let before = h.events().len();
    h.feed_frame(UdFrame::Rest(UdRestFrame::ActiveOrders(search)));
    assert_eq!(h.events().len(), before);
}

#[test]
fn a_place_that_nobody_answers_times_out_into_an_unknown_order() {
    let start = Instant::now();
    let mut core = UdCore::new();
    let cid = Uuid::new_v4();
    core.register_place(cid, start);
    let due = core.poll_timeout().expect("the answer timer runs");
    assert_eq!(due, start + Duration::from_secs(30));
    let mut out = EventSink::new();
    core.on_timeout(due + Duration::from_secs(1), &mut out);
    assert!(core.poll_timeout().is_none());
    let events = out.drain();
    assert_eq!(events.len(), 1, "{events:?}");
    let Event::Order(o) = &events[0].payload else {
        panic!("an order event")
    };
    assert_eq!(o.cid, cid);
    assert!(
        matches!(o.state, OrderState::Unknown { .. }),
        "{:?}",
        o.state
    );
}

#[test]
fn a_portfolio_message_reaches_the_portfolio_subscription_and_other_messages_are_skipped() {
    let mut h = Harness::new(UdCore::new(), Instant::now());
    h.apply(&SubscriptionSet::with_subscriptions(vec![
        DesiredSubscription {
            id: SubscriptionId::new(9),
            kind: SubscriptionKind::Portfolio,
        },
    ]));
    let text = std::fs::read_to_string(format!(
        "{}/../bcs-protocol/tests/fixtures/ws/portfolio.json",
        env!("CARGO_MANIFEST_DIR")
    ))
    .unwrap();
    h.feed_frame(UdFrame::Ws {
        channel: BcsWsChannel::Portfolio,
        text,
    });
    assert!(
        h.events()
            .iter()
            .any(|e| e.sub == Some(SubscriptionId::new(9)))
    );
    let before = h.events().len();
    h.feed_frame(UdFrame::Ws {
        channel: BcsWsChannel::OrderExecution,
        text: "not json".into(),
    });
    assert_eq!(h.events().len(), before);
}

#[test]
fn an_error_frame_of_the_market_data_channel_fails_the_pending_subscription_with_its_text() {
    let mut core = MdCore::new();
    let id = SubscriptionId::new(1);
    core.apply(&SubscriptionSet::with_subscriptions(vec![
        DesiredSubscription {
            id,
            kind: SubscriptionKind::Quotes { instrument: sber() },
        },
    ]));
    for (text, reason) in [
        (
            r#"{"responseType":"ErrorResponse","errors":[{"message":"no such ticker","code":"X"}]}"#,
            "no such ticker",
        ),
        (
            r#"{"responseType":"ErrorResponse","error":{"message":"just one","code":"Y"}}"#,
            "just one",
        ),
        (r#"{"responseType":"ErrorResponse"}"#, "subscription error"),
    ] {
        let mut core = MdCore::new();
        core.apply(&SubscriptionSet::with_subscriptions(vec![
            DesiredSubscription {
                id,
                kind: SubscriptionKind::Quotes { instrument: sber() },
            },
        ]));
        let frame = parse_incoming(text, BcsWsChannel::MarketData);
        core.on_frame(frame, Instant::now(), &mut EventSink::new());
        assert_eq!(core.actual_state(id), Some(SubState::Failed), "{text}");
        assert_eq!(core.failure(id), Some(reason), "{text}");
    }
}

// --- small functions of the mapping ---

mod mapping {
    use brokapi_bcs::map::instrument::{instrument_from_native, map_trading_status};
    use brokapi_bcs::map::order::{map_stop_record, rejection_reason};
    use brokapi_bcs::map::parse_broker_ts;
    use brokapi_bcs::util::{class_code, nanos_to_datetime, timeframe_label};
    use brokapi_bcs_protocol::http::models::{OrderRecord, TradingStatusResponse};
    use brokapi_core::{
        OrderSnapshot, OrderState, OrderType, Qty, StopOrderKind, StopOrderState, Timeframe,
        Timestamps, UnixNanos,
    };

    use super::*;

    #[test]
    fn every_timeframe_has_its_label_but_ten_minutes() {
        let labels: Vec<_> = [
            Timeframe::M1,
            Timeframe::M5,
            Timeframe::M10,
            Timeframe::M15,
            Timeframe::M30,
            Timeframe::H1,
            Timeframe::H4,
            Timeframe::D1,
            Timeframe::W1,
            Timeframe::MN,
        ]
        .into_iter()
        .map(timeframe_label)
        .collect();
        assert_eq!(
            labels,
            [
                Some("M1"),
                Some("M5"),
                None,
                Some("M15"),
                Some("M30"),
                Some("H1"),
                Some("H4"),
                Some("D"),
                Some("W"),
                Some("MN")
            ]
        );
    }

    #[test]
    fn times_are_read_with_or_without_the_offset() {
        assert!(parse_broker_ts(Some("2026-10-20T10:00:00Z")).is_some());
        assert_eq!(
            parse_broker_ts(Some("2026-10-20T10:00:00.250Z")).map(|t| t.as_nanos() % 1_000_000_000),
            Some(250_000_000)
        );
        assert_eq!(
            nanos_to_datetime(UnixNanos::from_nanos(1_500_000_000)).timestamp(),
            1
        );
    }

    #[test]
    fn the_board_of_an_instrument_comes_from_its_native_id() {
        assert_eq!(class_code(&sber()).unwrap(), "TQBR");
        let other = InstrumentId::new(
            InstrumentKey::new("SBER", "TQBR"),
            NativeId::SymbolMic {
                symbol: "SBER".into(),
                mic: "TQBR".into(),
            },
        );
        assert!(class_code(&other).is_err());
        assert_eq!(instrument_from_native(&sber()), sber().key);
    }

    #[test]
    fn a_session_is_open_closed_or_not_known() {
        let status = |s: &str| -> TradingStatusResponse {
            serde_json::from_value(serde_json::json!({
                "tradingSessionStatus": s, "tradingSessionType": "Main", "tradingSessionTypeId": 1, "nextSessionDate": "2026-10-23T07:00:00Z"
            }))
            .unwrap()
        };
        use brokapi_core::TradingAvailability as A;
        assert_eq!(
            map_trading_status("SBER", "TQBR", &status("open")).availability,
            A::Open
        );
        assert_eq!(
            map_trading_status("SBER", "TQBR", &status("CLOSE")).availability,
            A::Closed
        );
        assert_eq!(
            map_trading_status("SBER", "TQBR", &status("break")).availability,
            A::Unknown
        );
    }

    fn record(extra: serde_json::Value) -> OrderRecord {
        let mut base = serde_json::json!({
            "orderId": "77", "ticker": "SBER", "classCode": "TQBR", "side": 2,
            "orderQuantity": 10, "orderStatus": 3
        });
        base.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        serde_json::from_value(base).unwrap()
    }

    #[test]
    fn conditional_orders_show_their_legs_and_their_state() {
        let now = Instant::now();
        // The pair of a stop-loss and a take-profit is one order at the broker and two legs here.
        let pair = map_stop_record(
            &record(serde_json::json!({"orderType": 7, "stopPrice": 90, "takePrice": 120})),
            None,
            now,
        );
        let kinds: Vec<_> = pair.iter().map(|s| s.kind).collect();
        assert_eq!(kinds, [StopOrderKind::StopLoss, StopOrderKind::TakeProfit]);
        // The state follows the status of the list.
        for (status, want) in [
            (2, StopOrderState::Triggered),
            (1, StopOrderState::Cancelled),
            (3, StopOrderState::Active),
        ] {
            let one = map_stop_record(
                &record(
                    serde_json::json!({"orderType": 6, "stopPrice": 90, "orderStatus": status}),
                ),
                None,
                now,
            );
            assert_eq!(one[0].state, want);
        }
        let other = map_stop_record(
            &record(serde_json::json!({"orderType": 6, "stopPrice": 90, "orderStatus": 9})),
            None,
            now,
        );
        assert!(matches!(other[0].state, StopOrderState::Other(_)));
        // A plain order, an order without a side and one without an instrument are not stop orders.
        assert!(
            map_stop_record(&record(serde_json::json!({"orderType": 2})), None, now).is_empty()
        );
        assert!(
            map_stop_record(
                &record(serde_json::json!({"orderType": 6, "side": 9})),
                None,
                now
            )
            .is_empty()
        );
        assert!(
            map_stop_record(
                &record(serde_json::json!({"orderType": 6, "ticker": null})),
                None,
                now
            )
            .is_empty()
        );
    }

    #[test]
    fn the_reason_of_a_refusal_is_only_there_for_a_refused_order() {
        let ts = Timestamps::new(UnixNanos::ZERO, Instant::now());
        let snap = |state| {
            OrderSnapshot::new(
                Uuid::new_v4(),
                sber(),
                state,
                Side::Buy,
                OrderType::Limit,
                Qty::ZERO,
                ts,
            )
        };
        let rejected = snap(OrderState::Rejected {
            cid: Uuid::new_v4(),
            reason: "no money".into(),
        });
        assert_eq!(rejection_reason(&rejected), Some("no money"));
        let acked = snap(OrderState::Cancelled {
            cid: Uuid::new_v4(),
        });
        assert_eq!(rejection_reason(&acked), None);
    }
}
