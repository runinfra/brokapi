//! The sans-io user data core of T-Bank fed with the messages of its streams: accounts, subscriptions,
//! reconnects, the replace path. No network.

use std::time::Instant;

use brokapi_core::{
    ClientOrderId, Event, InstrumentId, InstrumentKey, NativeId, OrderState, OrderType,
    PlaceOrderRequest, Price, Qty, Side, SubscriptionId,
};
use brokapi_sansio::{BrokerCore, EventSink};
use brokapi_tbank::core::ud::{StreamKind, UdSubKind};
use brokapi_tbank::core::{UdCore, UdFrame};
use brokapi_tbank_protocol::money::money;
use brokapi_tbank_protocol::v1 as pb;

const ACC: &str = "acc-main";

fn core() -> UdCore {
    let mut core = UdCore::new(ACC);
    core.lots().put("uid-sber", "", "SBER", "TQBR", 10);
    for (n, kind) in [
        (1, UdSubKind::OrderState),
        (2, UdSubKind::Executions),
        (3, UdSubKind::Portfolio),
    ] {
        core.add_sub(SubscriptionId::new(n), kind);
    }
    core
}

fn sber() -> InstrumentId {
    InstrumentId::new(
        InstrumentKey::new("SBER", "TQBR"),
        NativeId::Uid("uid-sber".into()),
    )
}

fn request() -> PlaceOrderRequest {
    PlaceOrderRequest {
        cid: ClientOrderId::new_v4(),
        instrument: sber(),
        side: Side::Buy,
        order_type: OrderType::Limit,
        qty: Qty::from(10),
        price: Some(Price::from(250)),
    }
}

fn feed(core: &mut UdCore, frame: UdFrame) -> Vec<Event> {
    let mut out = EventSink::new();
    core.on_frame(frame, Instant::now(), &mut out);
    out.drain().into_iter().map(|e| e.payload).collect()
}

fn updates(core: &mut UdCore) -> Vec<(SubscriptionId, Result<(), String>)> {
    core.take_updates()
        .into_iter()
        .map(|u| (u.sub, u.result))
        .collect()
}

// ----- subscription answers -----

fn order_ack(status: pb::ResultSubscriptionStatus, error: bool) -> UdFrame {
    UdFrame::OrderState(pb::OrderStateStreamResponse {
        payload: Some(pb::order_state_stream_response::Payload::Subscription(
            pb::SubscriptionResponse {
                status: status as i32,
                accounts: vec![ACC.into()],
                error: error.then(|| pb::ErrorDetail {
                    code: "40003".into(),
                    message: "no right".into(),
                }),
                ..Default::default()
            },
        )),
    })
}

#[test]
fn the_answer_to_a_subscription_confirms_or_refuses_it() {
    let mut core = core();
    feed(
        &mut core,
        order_ack(pb::ResultSubscriptionStatus::Ok, false),
    );
    assert_eq!(updates(&mut core), vec![(SubscriptionId::new(1), Ok(()))]);

    feed(
        &mut core,
        order_ack(pb::ResultSubscriptionStatus::Error, true),
    );
    let got = updates(&mut core);
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].1.as_ref().unwrap_err(), "40003: no right");

    // Without an error text the reason is a plain phrase.
    feed(
        &mut core,
        order_ack(pb::ResultSubscriptionStatus::Error, false),
    );
    assert_eq!(
        updates(&mut core)[0].1.as_ref().unwrap_err(),
        "subscription refused"
    );

    // The same for the stream of trades.
    let trades = |status: pb::ResultSubscriptionStatus, error: bool| {
        UdFrame::Trades(pb::TradesStreamResponse {
            payload: Some(pb::trades_stream_response::Payload::Subscription(
                pb::SubscriptionResponse {
                    status: status as i32,
                    error: error.then(|| pb::ErrorDetail {
                        code: "1".into(),
                        message: "x".into(),
                    }),
                    ..Default::default()
                },
            )),
        })
    };
    feed(&mut core, trades(pb::ResultSubscriptionStatus::Ok, false));
    feed(&mut core, trades(pb::ResultSubscriptionStatus::Error, true));
    let got = updates(&mut core);
    assert_eq!(got[0], (SubscriptionId::new(2), Ok(())));
    assert!(got[1].1.is_err());
}

fn portfolio_ack(ok: bool, account: &str) -> UdFrame {
    UdFrame::Portfolio(pb::PortfolioStreamResponse {
        payload: Some(pb::portfolio_stream_response::Payload::Subscriptions(
            pb::PortfolioSubscriptionResult {
                accounts: vec![pb::AccountSubscriptionStatus {
                    account_id: account.into(),
                    subscription_status: if ok {
                        pb::PortfolioSubscriptionStatus::Success
                    } else {
                        pb::PortfolioSubscriptionStatus::InternalError
                    } as i32,
                }],
                ..Default::default()
            },
        )),
    })
}

fn positions_ack(ok: bool, account: &str) -> UdFrame {
    UdFrame::Positions(pb::PositionsStreamResponse {
        payload: Some(pb::positions_stream_response::Payload::Subscriptions(
            pb::PositionsSubscriptionResult {
                accounts: vec![pb::PositionsSubscriptionStatus {
                    account_id: account.into(),
                    subscription_status: if ok {
                        pb::PositionsAccountSubscriptionStatus::PositionsSubscriptionStatusSuccess
                    } else {
                        pb::PositionsAccountSubscriptionStatus::PositionsSubscriptionStatusInternalError
                    } as i32,
                }],
                ..Default::default()
            },
        )),
    })
}

#[test]
fn the_portfolio_is_confirmed_when_both_of_its_streams_accept_the_account() {
    let mut core = core();
    feed(&mut core, portfolio_ack(true, ACC));
    assert!(updates(&mut core).is_empty(), "one of two is not enough");
    feed(&mut core, positions_ack(true, ACC));
    assert_eq!(updates(&mut core), vec![(SubscriptionId::new(3), Ok(()))]);

    // A stream that does not list our account refuses the subscription.
    let mut core = self::core();
    feed(&mut core, portfolio_ack(true, "another-account"));
    let got = updates(&mut core);
    assert!(got[0].1.as_ref().unwrap_err().contains("portfolio stream"));
    feed(&mut core, positions_ack(true, "another-account"));
    assert!(
        updates(&mut core)[0]
            .1
            .as_ref()
            .unwrap_err()
            .contains("positions stream")
    );

    // A stream that lists the account with an error does not confirm it.
    let mut core = self::core();
    feed(&mut core, portfolio_ack(false, ACC));
    feed(&mut core, positions_ack(true, ACC));
    assert!(updates(&mut core).is_empty());
}

// ----- the streams of the portfolio -----

fn position_data(account: &str, money_rub: Option<&str>, security: bool) -> UdFrame {
    UdFrame::Positions(pb::PositionsStreamResponse {
        payload: Some(pb::positions_stream_response::Payload::Position(
            pb::PositionData {
                account_id: account.into(),
                money: money_rub
                    .map(|v| {
                        vec![pb::PositionsMoney {
                            available_value: Some(money("rub", v)),
                            blocked_value: Some(money("rub", "0")),
                        }]
                    })
                    .unwrap_or_default(),
                securities: if security {
                    vec![pb::PositionsSecurities {
                        instrument_uid: "uid-sber".into(),
                        ticker: "SBER".into(),
                        class_code: "TQBR".into(),
                        balance: 5,
                        blocked: 1,
                        ..Default::default()
                    }]
                } else {
                    vec![]
                },
                ..Default::default()
            },
        )),
    })
}

#[test]
fn a_change_of_positions_gives_balances_and_positions_and_another_account_is_dropped() {
    let mut core = core();
    let events = feed(&mut core, position_data(ACC, Some("1000"), true));
    assert!(
        matches!(&events[0], Event::Balances(b) if b.len() == 1 && b[0].currency == "RUB"),
        "{events:?}"
    );
    assert!(
        matches!(&events[1], Event::Positions(p) if p.len() == 1),
        "{events:?}"
    );
    // Securities only: positions, and the cash is not touched.
    let events = feed(&mut core, position_data(ACC, None, true));
    assert_eq!(events.len(), 1);
    // Cash only.
    let events = feed(&mut core, position_data(ACC, Some("900"), false));
    assert_eq!(events.len(), 1);
    // Another account: nothing.
    assert!(feed(&mut core, position_data("other", Some("1"), true)).is_empty());

    // The initial positions carry the cash.
    let initial = |account: &str| {
        UdFrame::Positions(pb::PositionsStreamResponse {
            payload: Some(pb::positions_stream_response::Payload::InitialPositions(
                pb::PositionsResponse {
                    account_id: account.into(),
                    money: vec![money("rub", "500")],
                    blocked: vec![money("rub", "50")],
                    ..Default::default()
                },
            )),
        })
    };
    let events = feed(&mut core, initial(ACC));
    assert!(matches!(&events[0], Event::Balances(_)));
    assert!(feed(&mut core, initial("other")).is_empty());
    // A ping is nothing.
    assert!(
        feed(
            &mut core,
            UdFrame::Positions(pb::PositionsStreamResponse {
                payload: Some(pb::positions_stream_response::Payload::Ping(
                    pb::Ping::default()
                )),
            })
        )
        .is_empty()
    );
}

#[test]
fn a_portfolio_message_is_a_portfolio_and_another_account_is_dropped() {
    let mut core = core();
    let message = |account: &str| {
        UdFrame::Portfolio(pb::PortfolioStreamResponse {
            payload: Some(pb::portfolio_stream_response::Payload::Portfolio(
                pb::PortfolioResponse {
                    account_id: account.into(),
                    positions: vec![pb::PortfolioPosition {
                        instrument_uid: "uid-sber".into(),
                        ticker: "SBER".into(),
                        class_code: "TQBR".into(),
                        quantity: Some(brokapi_tbank_protocol::money::quotation("5")),
                        average_position_price: Some(money("rub", "250")),
                        current_price: Some(money("rub", "251")),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            )),
        })
    };
    let events = feed(&mut core, message(ACC));
    assert!(
        matches!(&events[0], Event::Portfolio(p) if p.positions.len() == 1),
        "{events:?}"
    );
    assert!(feed(&mut core, message("other")).is_empty());
}

// ----- reconnects, the order stream, replace -----

#[test]
fn a_stream_that_opens_again_is_reported_once_per_break() {
    let mut core = core();
    feed(&mut core, UdFrame::Connected(StreamKind::OrderState));
    assert!(
        core.take_reconnected().is_empty(),
        "the first open is no break"
    );
    feed(&mut core, UdFrame::Connected(StreamKind::OrderState));
    assert_eq!(core.take_reconnected(), vec![StreamKind::OrderState]);
    assert!(core.take_reconnected().is_empty());
}

fn stream_order(
    account: &str,
    order_id: &str,
    cid: &ClientOrderId,
    status: pb::OrderExecutionReportStatus,
) -> UdFrame {
    UdFrame::OrderState(pb::OrderStateStreamResponse {
        payload: Some(pb::order_state_stream_response::Payload::OrderState(
            pb::order_state_stream_response::OrderState {
                order_id: order_id.into(),
                order_request_id: Some(cid.hyphenated().to_string()),
                account_id: account.into(),
                execution_report_status: status as i32,
                ticker: "SBER".into(),
                class_code: "TQBR".into(),
                instrument_uid: "uid-sber".into(),
                lot_size: 10,
                lots_requested: 1,
                lots_executed: 0,
                lots_left: 1,
                direction: pb::OrderDirection::Buy as i32,
                order_type: pb::OrderType::Limit as i32,
                ..Default::default()
            },
        )),
    })
}

#[test]
fn the_order_stream_gives_the_state_and_drops_another_account() {
    let mut core = core();
    let req = request();
    core.begin_place(&req, Instant::now()).unwrap();
    let events = feed(
        &mut core,
        stream_order(
            ACC,
            "X1",
            &req.cid,
            pb::OrderExecutionReportStatus::ExecutionReportStatusNew,
        ),
    );
    assert!(
        matches!(&events[0], Event::Order(o) if o.cid == req.cid && matches!(o.state, OrderState::Acked { .. })),
        "{events:?}"
    );
    assert_eq!(core.exchange_id(req.cid), Some("X1"));
    assert!(core.instrument_of(req.cid).is_some());
    assert_eq!(core.order_basis(req.cid).unwrap().0, Qty::from(10));
    assert!(
        feed(
            &mut core,
            stream_order(
                "other",
                "X2",
                &ClientOrderId::new_v4(),
                pb::OrderExecutionReportStatus::ExecutionReportStatusNew
            )
        )
        .is_empty()
    );
    assert_eq!(
        core.sub(UdSubKind::OrderState),
        Some(SubscriptionId::new(1))
    );
    core.remove_sub(UdSubKind::OrderState);
    assert_eq!(core.sub(UdSubKind::OrderState), None);
    assert_eq!(core.account_id(), ACC);
    assert!(core.orders_mut().in_flight_count() == 0);
}

#[test]
fn a_refresh_after_a_reconnect_repeats_the_state_of_the_live_orders() {
    let mut core = core();
    let req = request();
    core.begin_place(&req, Instant::now()).unwrap();
    let live = pb::GetOrdersResponse {
        orders: vec![pb::OrderState {
            order_id: "X1".into(),
            order_request_id: req.cid.hyphenated().to_string(),
            execution_report_status:
                pb::OrderExecutionReportStatus::ExecutionReportStatusPartiallyfill as i32,
            lots_requested: 1,
            lots_executed: 0,
            instrument_uid: "uid-sber".into(),
            ticker: "SBER".into(),
            class_code: "TQBR".into(),
            direction: pb::OrderDirection::Buy as i32,
            ..Default::default()
        }],
    };
    let events = feed(&mut core, UdFrame::ActiveOrders(live));
    assert!(
        matches!(&events[0], Event::Order(o) if o.cid == req.cid),
        "{events:?}"
    );
}

#[test]
fn a_replace_registers_the_new_cid_once_and_follows_its_answer() {
    let now = Instant::now();
    let mut core = core();
    let original = request();
    core.begin_place(&original, now).unwrap();
    let mut out = EventSink::new();
    core.cancel_ok(ClientOrderId::new_v4(), now, &mut out); // an unknown cid: nothing happens
    assert!(out.drain().is_empty());

    // The replace of an order we placed inherits its instrument and side.
    let new_cid = ClientOrderId::new_v4();
    core.begin_replace(new_cid, original.cid, now).unwrap();
    let again = core.begin_replace(new_cid, original.cid, now);
    assert!(
        matches!(again, Err(brokapi_core::Error::Rejected { ref code, .. }) if code == "duplicate_place")
    );
    let resp = pb::PostOrderResponse {
        order_id: "X2".into(),
        order_request_id: new_cid.hyphenated().to_string(),
        execution_report_status: pb::OrderExecutionReportStatus::ExecutionReportStatusNew as i32,
        lots_requested: 2,
        instrument_uid: "uid-sber".into(),
        direction: pb::OrderDirection::Buy as i32,
        initial_security_price: Some(money("rub", "251")),
        ..Default::default()
    };
    core.replace_response(new_cid, &resp, now, &mut out);
    let events: Vec<_> = out.drain().into_iter().map(|e| e.payload).collect();
    assert!(
        matches!(&events[0], Event::Order(o) if o.cid == new_cid && o.qty == Qty::from(20)),
        "{events:?}"
    );

    // A replace of an order we never saw is registered from the answer alone.
    let stranger = ClientOrderId::new_v4();
    core.begin_replace(stranger, ClientOrderId::new_v4(), now)
        .unwrap();
    core.replace_response(stranger, &resp, now, &mut out);
    assert!(!out.drain().is_empty());

    // A replace that fails: refused for sure, or with no answer.
    for (unknown, want) in [(false, "Rejected"), (true, "Unknown")] {
        let cid = ClientOrderId::new_v4();
        core.begin_replace(cid, original.cid, now).unwrap();
        core.replace_failed(cid, unknown, "no".into(), now, &mut out);
        let events: Vec<_> = out.drain().into_iter().map(|e| e.payload).collect();
        assert!(
            matches!(&events[0], Event::Order(o) if o.state.name() == want),
            "{events:?}"
        );
    }
}

#[test]
fn a_cancel_that_the_broker_accepted_moves_the_order_on() {
    let now = Instant::now();
    let mut core = core();
    let req = request();
    core.begin_place(&req, now).unwrap();
    let mut out = EventSink::new();
    core.place_response(
        req.cid,
        &pb::PostOrderResponse {
            order_id: "X1".into(),
            order_request_id: req.cid.hyphenated().to_string(),
            execution_report_status: pb::OrderExecutionReportStatus::ExecutionReportStatusNew
                as i32,
            lots_requested: 1,
            instrument_uid: "uid-sber".into(),
            direction: pb::OrderDirection::Buy as i32,
            ..Default::default()
        },
        now,
        &mut out,
    );
    out.drain();
    core.cancel_ok(req.cid, now, &mut out);
    let events: Vec<_> = out.drain().into_iter().map(|e| e.payload).collect();
    assert!(
        matches!(&events[0], Event::Order(o) if matches!(o.state, OrderState::Cancelled { .. })),
        "{events:?}"
    );
    assert_eq!(
        core.order_state(req.cid).map(|s| s.name()),
        Some("Cancelled")
    );
}
