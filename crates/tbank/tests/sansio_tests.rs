//! Sans-io cores on recorded frames and virtual time (no network, no sleeps).

use brokapi_core::{Decimal, dec};
use std::time::{Duration, Instant};

use brokapi_core::{
    ClientOrderId, Event, InstrumentId, InstrumentKey, NativeId, OrderState, OrderType,
    PlaceOrderRequest, Price, Qty, ResyncReason, Side, SubscriptionId, Timeframe,
};
use brokapi_sansio::{BrokerCore, DesiredSubscription, Harness, SubscriptionKind, SubscriptionSet};
use brokapi_tbank::core::md::{LState, RETRY_AFTER, SubUpdate};
use brokapi_tbank::core::ud::{StreamKind, UdSubKind};
use brokapi_tbank::{MdCore, MdFrame, UdCore, UdFrame};
use brokapi_tbank_protocol::money::{money, quotation};
use brokapi_tbank_protocol::v1 as pb;
use brokapi_tbank_protocol::v1::market_data_request::Payload as Req;
use brokapi_tbank_protocol::v1::market_data_response::Payload as Resp;

fn sber() -> InstrumentId {
    InstrumentId::new(
        InstrumentKey::new("SBER", "TQBR"),
        NativeId::Uid("uid-sber".into()),
    )
}

fn sub(id: u64, kind: SubscriptionKind) -> DesiredSubscription {
    DesiredSubscription {
        id: SubscriptionId::new(id),
        kind,
    }
}

fn set(subs: Vec<DesiredSubscription>) -> SubscriptionSet {
    SubscriptionSet::with_subscriptions(subs)
}

fn book(depth: u32) -> SubscriptionKind {
    SubscriptionKind::OrderBook {
        instrument: sber(),
        depth,
    }
}

fn quotes() -> SubscriptionKind {
    SubscriptionKind::Quotes { instrument: sber() }
}

// ---------- market data ----------

fn drain(core: &mut MdCore) -> Vec<pb::MarketDataRequest> {
    std::iter::from_fn(|| core.poll_request()).collect()
}

/// (action, instrument_id, depth) of the order book requests in the batch.
fn book_reqs(reqs: &[pb::MarketDataRequest]) -> Vec<(i32, String, i32)> {
    reqs.iter()
        .filter_map(|r| match &r.payload {
            Some(Req::SubscribeOrderBookRequest(b)) => Some(
                b.instruments
                    .iter()
                    .map(|i| (b.subscription_action, i.instrument_id.clone(), i.depth))
                    .collect::<Vec<_>>(),
            ),
            _ => None,
        })
        .flatten()
        .collect()
}

const SUBSCRIBE: i32 = pb::SubscriptionAction::Subscribe as i32;
const UNSUBSCRIBE: i32 = pb::SubscriptionAction::Unsubscribe as i32;

fn book_ack(status: pb::SubscriptionStatus, action: i32, depth: i32) -> MdFrame {
    let s = pb::OrderBookSubscription {
        depth,
        subscription_status: status as i32,
        instrument_uid: "uid-sber".into(),
        ticker: "SBER".into(),
        class_code: "TQBR".into(),
        subscription_action: action,
        ..Default::default()
    };
    MdFrame::Response(pb::MarketDataResponse {
        payload: Some(Resp::SubscribeOrderBookResponse(
            pb::SubscribeOrderBookResponse {
                order_book_subscriptions: vec![s],
                ..Default::default()
            },
        )),
    })
}

fn ok_ack(depth: i32) -> MdFrame {
    book_ack(pb::SubscriptionStatus::Success, SUBSCRIBE, depth)
}

fn book_data(consistent: bool) -> MdFrame {
    let o = |p: &str, q: i64| pb::Order {
        price: Some(quotation(p)),
        quantity: q,
    };
    MdFrame::Response(pb::MarketDataResponse {
        payload: Some(Resp::Orderbook(pb::OrderBook {
            depth: 20,
            is_consistent: consistent,
            bids: vec![o("273.8", 5), o("273.9", 10), o("273.7", 1)],
            asks: vec![o("274.0", 4), o("274.2", 2), o("274.1", 9)],
            instrument_uid: "uid-sber".into(),
            ticker: "SBER".into(),
            class_code: "TQBR".into(),
            ..Default::default()
        })),
    })
}

fn md() -> Harness<MdCore> {
    Harness::new(MdCore::new(Duration::from_secs(30)), Instant::now())
}

#[test]
fn connect_sends_ping_settings_then_subscriptions() {
    let mut h = md();
    h.apply(&set(vec![sub(1, book(20))]));
    h.feed_frame(MdFrame::Connected);
    let reqs = drain(h.core_mut());
    assert!(matches!(
        reqs[0].payload,
        Some(Req::PingSettings(pb::PingDelaySettings {
            ping_delay_ms: Some(30_000)
        }))
    ));
    assert_eq!(
        book_reqs(&reqs),
        vec![(SUBSCRIBE, "uid-sber".to_string(), 20)]
    );
}

#[test]
fn ack_activates_and_data_carries_the_subscription_id() {
    let mut h = md();
    h.apply(&set(vec![sub(1, book(2))]));
    h.feed_frame(MdFrame::Connected);
    drain(h.core_mut());
    assert_eq!(
        h.core().state(SubscriptionId::new(1)),
        Some(LState::Pending)
    );

    h.feed_frame(ok_ack(10));
    assert_eq!(h.core().state(SubscriptionId::new(1)), Some(LState::Active));
    assert_eq!(
        h.core_mut().take_updates(),
        vec![SubUpdate {
            sub: SubscriptionId::new(1),
            result: Ok(())
        }]
    );

    h.feed_frame(book_data(true));
    let ev = h.take_events();
    assert_eq!(ev.len(), 1);
    assert_eq!(ev[0].sub, Some(SubscriptionId::new(1)));
    let Event::OrderBook(brokapi_core::OrderBook::Snapshot { bids, asks, .. }) = &ev[0].payload
    else {
        panic!("order book snapshot expected, got {:?}", ev[0].payload);
    };
    // sorted best first and cut to the subscription's depth (2)
    assert_eq!(
        bids.iter().map(|l| l.price.decimal()).collect::<Vec<_>>(),
        vec![dec!(273.9), dec!(273.8)]
    );
    assert_eq!(
        asks.iter().map(|l| l.price.decimal()).collect::<Vec<_>>(),
        vec![dec!(274.0), dec!(274.1)]
    );
}

#[test]
fn data_before_ack_activates_the_subscription() {
    let mut h = md();
    h.apply(&set(vec![sub(1, book(20))]));
    h.feed_frame(MdFrame::Connected);
    h.feed_frame(book_data(true));
    assert_eq!(h.core().state(SubscriptionId::new(1)), Some(LState::Active));
    assert_eq!(h.events().len(), 1);
}

#[test]
fn quotes_and_book_share_one_wire_subscription_of_the_larger_depth() {
    let mut h = md();
    h.feed_frame(MdFrame::Connected);
    drain(h.core_mut());
    h.apply(&set(vec![sub(1, quotes())]));
    assert_eq!(
        book_reqs(&drain(h.core_mut())),
        vec![(SUBSCRIBE, "uid-sber".to_string(), 1)]
    );
    h.feed_frame(ok_ack(1));

    // a book of depth 20 on the same instrument: depth 1 is dropped and depth 20 taken
    h.apply(&set(vec![sub(1, quotes()), sub(2, book(20))]));
    assert_eq!(
        book_reqs(&drain(h.core_mut())),
        vec![
            (UNSUBSCRIBE, "uid-sber".to_string(), 1),
            (SUBSCRIBE, "uid-sber".to_string(), 20)
        ]
    );
    assert_eq!(h.core().wire_count(), 1);
    h.feed_frame(ok_ack(20));

    // one book message feeds both: a quote for 1 and a book for 2
    h.take_events();
    h.feed_frame(book_data(true));
    let ev = h.take_events();
    assert_eq!(ev.len(), 2);
    assert!(matches!(ev[0].payload, Event::Quote(_)) && ev[0].sub == Some(SubscriptionId::new(1)));
    assert!(
        matches!(ev[1].payload, Event::OrderBook(_)) && ev[1].sub == Some(SubscriptionId::new(2))
    );
}

#[test]
fn second_subscription_to_a_live_wire_is_confirmed_at_once_and_unsubscribe_waits_for_the_last() {
    let mut h = md();
    h.feed_frame(MdFrame::Connected);
    h.apply(&set(vec![sub(1, book(20))]));
    drain(h.core_mut());
    h.feed_frame(ok_ack(20));
    h.core_mut().take_updates();

    h.apply(&set(vec![sub(1, book(20)), sub(2, book(20))]));
    assert!(
        book_reqs(&drain(h.core_mut())).is_empty(),
        "no new wire request"
    );
    assert_eq!(
        h.core_mut().take_updates(),
        vec![SubUpdate {
            sub: SubscriptionId::new(2),
            result: Ok(())
        }]
    );

    h.apply(&set(vec![sub(2, book(20))]));
    assert!(
        book_reqs(&drain(h.core_mut())).is_empty(),
        "one subscriber is left"
    );
    h.apply(&set(vec![]));
    assert_eq!(
        book_reqs(&drain(h.core_mut())),
        vec![(UNSUBSCRIBE, "uid-sber".to_string(), 20)]
    );
    assert_eq!(h.core().wire_count(), 0);
}

#[test]
fn refused_subscription_is_reported_with_the_broker_status() {
    let mut h = md();
    h.feed_frame(MdFrame::Connected);
    h.apply(&set(vec![sub(1, book(20))]));
    h.feed_frame(book_ack(
        pb::SubscriptionStatus::InstrumentNotFound,
        SUBSCRIBE,
        20,
    ));
    assert_eq!(h.core().state(SubscriptionId::new(1)), Some(LState::Failed));
    assert_eq!(
        h.core_mut().take_updates(),
        vec![SubUpdate {
            sub: SubscriptionId::new(1),
            result: Err("SUBSCRIPTION_STATUS_INSTRUMENT_NOT_FOUND".into())
        }]
    );
    // a hard refusal is not retried
    assert_eq!(h.core().poll_timeout(), None);
}

#[test]
fn transient_refusal_is_retried_without_reconnect() {
    let mut h = md();
    h.feed_frame(MdFrame::Connected);
    h.apply(&set(vec![sub(1, book(20))]));
    drain(h.core_mut());
    h.feed_frame(book_ack(
        pb::SubscriptionStatus::InternalError,
        SUBSCRIBE,
        20,
    ));
    assert_eq!(h.core().state(SubscriptionId::new(1)), Some(LState::Failed));
    assert!(h.core().poll_timeout().is_some());
    assert!(book_reqs(&drain(h.core_mut())).is_empty());

    h.advance_by(RETRY_AFTER + Duration::from_millis(1));
    assert_eq!(
        book_reqs(&drain(h.core_mut())),
        vec![(SUBSCRIBE, "uid-sber".to_string(), 20)]
    );
    assert_eq!(
        h.core().state(SubscriptionId::new(1)),
        Some(LState::Pending)
    );
    h.feed_frame(ok_ack(20));
    assert_eq!(h.core().state(SubscriptionId::new(1)), Some(LState::Active));
}

#[test]
fn reconnect_resubscribes_everything_and_tells_book_consumers() {
    let mut h = md();
    h.feed_frame(MdFrame::Connected);
    h.apply(&set(vec![
        sub(1, book(20)),
        sub(2, SubscriptionKind::LastTrades { instrument: sber() }),
    ]));
    drain(h.core_mut());
    h.feed_frame(ok_ack(20));
    assert_eq!(h.take_events().len(), 0, "first connect is not a reset");

    h.feed_frame(MdFrame::Connected);
    let ev = h.take_events();
    assert_eq!(ev.len(), 1);
    assert!(matches!(
        ev[0].payload,
        Event::Resync {
            reason: ResyncReason::SubscriptionReset,
            ..
        }
    ));
    assert_eq!(ev[0].sub, Some(SubscriptionId::new(1)));
    let reqs = drain(h.core_mut());
    assert_eq!(
        book_reqs(&reqs),
        vec![(SUBSCRIBE, "uid-sber".to_string(), 20)]
    );
    assert!(
        reqs.iter()
            .any(|r| matches!(r.payload, Some(Req::SubscribeTradesRequest(_))))
    );
    assert_eq!(
        h.core().state(SubscriptionId::new(1)),
        Some(LState::Pending)
    );
}

#[test]
fn inconsistent_book_is_not_published_and_asks_for_resync() {
    let mut h = md();
    h.feed_frame(MdFrame::Connected);
    h.apply(&set(vec![sub(1, book(20))]));
    h.feed_frame(ok_ack(20));
    h.feed_frame(book_data(false));
    let ev = h.take_events();
    assert_eq!(ev.len(), 1);
    assert!(matches!(
        ev[0].payload,
        Event::Resync {
            reason: ResyncReason::SequenceGap,
            ..
        }
    ));
}

#[test]
fn trades_and_candles_route_by_subscription() {
    let mut h = md();
    h.feed_frame(MdFrame::Connected);
    h.apply(&set(vec![
        sub(1, SubscriptionKind::LastTrades { instrument: sber() }),
        sub(
            2,
            SubscriptionKind::LastCandle {
                instrument: sber(),
                timeframe: Timeframe::M5,
            },
        ),
        sub(
            3,
            SubscriptionKind::LastCandle {
                instrument: sber(),
                timeframe: Timeframe::H1,
            },
        ),
    ]));
    let reqs = drain(h.core_mut());
    assert_eq!(
        reqs.iter()
            .filter(|r| matches!(r.payload, Some(Req::SubscribeCandlesRequest(_))))
            .count(),
        2
    );

    let trade = pb::Trade {
        direction: pb::TradeDirection::Sell as i32,
        price: Some(quotation("274.0")),
        quantity: 3,
        instrument_uid: "uid-sber".into(),
        ticker: "SBER".into(),
        class_code: "TQBR".into(),
        ..Default::default()
    };
    h.feed_frame(MdFrame::Response(pb::MarketDataResponse {
        payload: Some(Resp::Trade(trade)),
    }));
    let candle = pb::Candle {
        interval: pb::SubscriptionInterval::FiveMinutes as i32,
        close: Some(quotation("274.5")),
        volume: 9,
        instrument_uid: "uid-sber".into(),
        ticker: "SBER".into(),
        class_code: "TQBR".into(),
        ..Default::default()
    };
    h.feed_frame(MdFrame::Response(pb::MarketDataResponse {
        payload: Some(Resp::Candle(candle)),
    }));
    let ev = h.take_events();
    assert_eq!(ev.len(), 2);
    assert!(
        matches!(&ev[0].payload, Event::MarketTrade(t) if t.side == Side::Sell && t.qty.decimal() == dec!(3.0))
    );
    // the 5-minute candle reaches subscription 2 only, not the 1-hour one
    assert_eq!(ev[1].sub, Some(SubscriptionId::new(2)));
    assert!(
        matches!(&ev[1].payload, Event::Candle(c) if c.timeframe == Timeframe::M5 && c.close.decimal() == dec!(274.5))
    );
}

#[test]
fn unsubscribe_confirmations_are_counted() {
    let mut h = md();
    h.feed_frame(MdFrame::Connected);
    h.apply(&set(vec![sub(1, book(20))]));
    h.feed_frame(ok_ack(20));
    h.apply(&set(vec![]));
    assert_eq!(h.core().unsubscribe_progress(), (0, 0, 1));
    h.feed_frame(book_ack(pb::SubscriptionStatus::Success, UNSUBSCRIBE, 20));
    assert_eq!(h.core().unsubscribe_progress(), (1, 0, 0));
}

// ---------- user data ----------

const ACC: &str = "acc-a";

fn ud() -> Harness<UdCore> {
    let mut core = UdCore::new(ACC);
    core.add_sub(SubscriptionId::new(1001), UdSubKind::OrderState);
    core.add_sub(SubscriptionId::new(1002), UdSubKind::Executions);
    core.add_sub(SubscriptionId::new(1003), UdSubKind::Portfolio);
    Harness::new(core, Instant::now())
}

fn place_req(cid: ClientOrderId) -> PlaceOrderRequest {
    PlaceOrderRequest {
        cid,
        instrument: sber(),
        side: Side::Buy,
        order_type: OrderType::Limit,
        qty: Qty::from(3),
        price: Some(Price::from(250)),
    }
}

fn stream_state(
    cid: ClientOrderId,
    status: pb::OrderExecutionReportStatus,
    executed: i64,
    account: &str,
) -> UdFrame {
    UdFrame::OrderState(pb::OrderStateStreamResponse {
        payload: Some(pb::order_state_stream_response::Payload::OrderState(
            pb::order_state_stream_response::OrderState {
                order_id: "EX1".into(),
                order_request_id: Some(cid.hyphenated().to_string()),
                execution_report_status: status as i32,
                ticker: "SBER".into(),
                class_code: "TQBR".into(),
                direction: pb::OrderDirection::Buy as i32,
                order_type: pb::OrderType::Limit as i32,
                account_id: account.into(),
                lots_requested: 3,
                lots_executed: executed,
                instrument_uid: "uid-sber".into(),
                ..Default::default()
            },
        )),
    })
}

fn order_events(h: &mut Harness<UdCore>) -> Vec<(SubscriptionId, brokapi_core::OrderSnapshot)> {
    h.take_events()
        .into_iter()
        .filter_map(|e| match e.payload {
            Event::Order(s) => Some((e.sub.expect("order events are tagged"), s)),
            _ => None,
        })
        .collect()
}

fn cid(n: u8) -> ClientOrderId {
    ClientOrderId::from_bytes([n; 16])
}

#[test]
fn a_cid_is_placed_once_never_retried() {
    let mut h = ud();
    let now = Instant::now();
    h.core_mut()
        .begin_place(&place_req(cid(1)), 3, now)
        .unwrap();
    let again = h.core_mut().begin_place(&place_req(cid(1)), 3, now);
    assert!(
        matches!(again, Err(brokapi_core::Error::Rejected { code, .. }) if code == "duplicate_place")
    );
    // the same holds as a replace key
    assert!(h.core_mut().begin_replace(cid(1), cid(2), now).is_err());
}

#[test]
fn timeout_on_place_is_unknown_and_the_stream_resolves_it() {
    let mut h = ud();
    let now = Instant::now();
    h.core_mut()
        .begin_place(&place_req(cid(1)), 3, now)
        .unwrap();
    let mut sink = brokapi_sansio::EventSink::new();
    h.core_mut().place_unknown(cid(1), now, &mut sink);
    let ev = sink.drain();
    assert_eq!(ev.len(), 1);
    assert!(
        matches!(&ev[0].payload, Event::Order(s) if matches!(s.state, OrderState::Unknown { .. }) && s.cid == cid(1))
    );
    assert_eq!(ev[0].sub, Some(SubscriptionId::new(1001)));

    // the broker did get it: the order-state stream shows it
    h.feed_frame(stream_state(
        cid(1),
        pb::OrderExecutionReportStatus::ExecutionReportStatusNew,
        0,
        ACC,
    ));
    let ev = order_events(&mut h);
    assert_eq!(ev.len(), 1);
    assert!(matches!(ev[0].1.state, OrderState::Acked { .. }));
}

#[test]
fn rejected_only_when_the_broker_said_so() {
    let mut h = ud();
    let now = Instant::now();
    h.core_mut()
        .begin_place(&place_req(cid(1)), 3, now)
        .unwrap();
    let mut sink = brokapi_sansio::EventSink::new();
    h.core_mut()
        .place_rejected(cid(1), "INVALID_ARGUMENT: 30016".into(), now, &mut sink);
    let ev = sink.drain();
    assert!(
        matches!(&ev[0].payload, Event::Order(s) if matches!(&s.state, OrderState::Rejected { reason, .. } if reason.contains("30016")))
    );
}

#[test]
fn lifecycle_from_the_stream_every_event_has_the_cid() {
    use pb::OrderExecutionReportStatus as S;
    let mut h = ud();
    let now = Instant::now();
    h.core_mut()
        .begin_place(&place_req(cid(7)), 3, now)
        .unwrap();
    for (st, executed) in [
        (S::ExecutionReportStatusNew, 0),
        (S::ExecutionReportStatusNew, 0), // repeat: no new event
        (S::ExecutionReportStatusPartiallyfill, 1),
        (S::ExecutionReportStatusPartiallyfill, 2),
        (S::ExecutionReportStatusFill, 3),
    ] {
        h.feed_frame(stream_state(cid(7), st, executed, ACC));
    }
    let ev = order_events(&mut h);
    let states: Vec<&OrderState> = ev.iter().map(|(_, s)| &s.state).collect();
    assert_eq!(states.len(), 4);
    assert!(matches!(states[0], OrderState::Acked { .. }));
    assert!(
        matches!(states[1], OrderState::PartiallyFilled { filled, left, .. } if filled.decimal() == dec!(1.0) && left.decimal() == dec!(2.0))
    );
    assert!(
        matches!(states[2], OrderState::PartiallyFilled { filled, left, .. } if filled.decimal() == dec!(2.0) && left.decimal() == dec!(1.0))
    );
    assert!(matches!(states[3], OrderState::Filled { .. }));
    assert!(
        ev.iter()
            .all(|(sub, s)| *sub == SubscriptionId::new(1001) && s.cid == cid(7))
    );
    // price and instrument come from our own request
    assert_eq!(ev[0].1.price.unwrap().decimal(), dec!(250.0));
    assert_eq!(ev[0].1.broker_id.as_ref().unwrap().as_str(), "EX1");
}

#[test]
fn another_account_is_ignored_even_on_our_stream() {
    let mut h = ud();
    h.feed_frame(stream_state(
        cid(9),
        pb::OrderExecutionReportStatus::ExecutionReportStatusNew,
        0,
        "acc-b",
    ));
    assert!(h.take_events().is_empty());

    let foreign_trades = pb::TradesStreamResponse {
        payload: Some(pb::trades_stream_response::Payload::OrderTrades(
            pb::OrderTrades {
                order_id: "EX9".into(),
                account_id: "acc-b".into(),
                trades: vec![pb::OrderTrade {
                    quantity: 1,
                    ..Default::default()
                }],
                ..Default::default()
            },
        )),
    };
    h.feed_frame(UdFrame::Trades(foreign_trades));
    let portfolio = pb::PortfolioStreamResponse {
        payload: Some(pb::portfolio_stream_response::Payload::Portfolio(
            pb::PortfolioResponse {
                account_id: "acc-b".into(),
                ..Default::default()
            },
        )),
    };
    h.feed_frame(UdFrame::Portfolio(portfolio));
    assert!(h.take_events().is_empty());
}

#[test]
fn order_placed_elsewhere_is_seen_with_a_stand_in_cid() {
    let mut h = ud();
    let mut frame = stream_state(
        cid(1),
        pb::OrderExecutionReportStatus::ExecutionReportStatusNew,
        0,
        ACC,
    );
    if let UdFrame::OrderState(r) = &mut frame
        && let Some(pb::order_state_stream_response::Payload::OrderState(o)) = &mut r.payload
    {
        o.order_request_id = None;
        o.order_id = "EX77".into();
    }
    h.feed_frame(frame);
    let ev = order_events(&mut h);
    assert_eq!(ev.len(), 1);
    assert!(!ev[0].1.cid.is_nil());
    assert_eq!(ev[0].1.cid, brokapi_tbank::cid::synthetic_cid("EX77"));
}

#[test]
fn trades_are_linked_to_our_cid_through_the_exchange_order_id() {
    let mut h = ud();
    let now = Instant::now();
    h.core_mut()
        .begin_place(&place_req(cid(5)), 3, now)
        .unwrap();
    h.feed_frame(stream_state(
        cid(5),
        pb::OrderExecutionReportStatus::ExecutionReportStatusNew,
        0,
        ACC,
    ));
    h.take_events();
    let trades = pb::TradesStreamResponse {
        payload: Some(pb::trades_stream_response::Payload::OrderTrades(
            pb::OrderTrades {
                order_id: "EX1".into(),
                direction: pb::OrderDirection::Buy as i32,
                account_id: ACC.into(),
                instrument_uid: "uid-sber".into(),
                trades: vec![
                    pb::OrderTrade {
                        price: Some(quotation("249.5")),
                        quantity: 2,
                        trade_id: "t1".into(),
                        ..Default::default()
                    },
                    pb::OrderTrade {
                        price: Some(quotation("249.6")),
                        quantity: 1,
                        trade_id: "t2".into(),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            },
        )),
    };
    h.feed_frame(UdFrame::Trades(trades));
    let ev = h.take_events();
    assert_eq!(ev.len(), 2);
    for e in &ev {
        assert_eq!(e.sub, Some(SubscriptionId::new(1002)));
        let Event::Execution(x) = &e.payload else {
            panic!("execution expected")
        };
        assert_eq!(x.cid, Some(cid(5)));
        // the ticker was learnt from the order, the trade message has only the uid
        assert_eq!(x.instrument.key, InstrumentKey::new("SBER", "TQBR"));
    }
}

#[test]
fn in_flight_orders_are_abandoned_as_unknown_at_shutdown() {
    let mut h = ud();
    let now = Instant::now();
    h.core_mut()
        .begin_place(&place_req(cid(1)), 3, now)
        .unwrap();
    h.core_mut()
        .begin_place(&place_req(cid(2)), 3, now)
        .unwrap();
    let mut sink = brokapi_sansio::EventSink::new();
    let mut abandoned = h.core_mut().abandon_inflight(now, &mut sink);
    abandoned.sort();
    assert_eq!(abandoned, vec![cid(1), cid(2)]);
    assert_eq!(sink.len(), 2);
    assert!(matches!(
        h.core().order_state(cid(1)),
        Some(OrderState::Unknown { .. })
    ));
}

#[test]
fn portfolio_subscription_needs_both_streams_and_emits_cash_and_positions() {
    let mut h = ud();
    h.feed_frame(UdFrame::Connected(StreamKind::Portfolio));
    h.feed_frame(UdFrame::Connected(StreamKind::Positions));
    h.feed_frame(UdFrame::Portfolio(pb::PortfolioStreamResponse {
        payload: Some(pb::portfolio_stream_response::Payload::Subscriptions(
            pb::PortfolioSubscriptionResult {
                accounts: vec![pb::AccountSubscriptionStatus {
                    account_id: ACC.into(),
                    subscription_status: pb::PortfolioSubscriptionStatus::Success as i32,
                }],
                ..Default::default()
            },
        )),
    }));
    assert!(
        h.core_mut().take_updates().is_empty(),
        "one stream is not enough"
    );
    h.feed_frame(UdFrame::Positions(pb::PositionsStreamResponse {
        payload: Some(pb::positions_stream_response::Payload::Subscriptions(
            pb::PositionsSubscriptionResult {
                accounts: vec![pb::PositionsSubscriptionStatus {
                    account_id: ACC.into(),
                    subscription_status:
                        pb::PositionsAccountSubscriptionStatus::PositionsSubscriptionStatusSuccess
                            as i32,
                }],
                ..Default::default()
            },
        )),
    }));
    assert_eq!(
        h.core_mut().take_updates(),
        vec![SubUpdate {
            sub: SubscriptionId::new(1003),
            result: Ok(())
        }]
    );

    h.feed_frame(UdFrame::Positions(pb::PositionsStreamResponse {
        payload: Some(pb::positions_stream_response::Payload::InitialPositions(
            pb::PositionsResponse {
                money: vec![money("rub", "1000.0")],
                account_id: ACC.into(),
                ..Default::default()
            },
        )),
    }));
    h.feed_frame(UdFrame::Portfolio(pb::PortfolioStreamResponse {
        payload: Some(pb::portfolio_stream_response::Payload::Portfolio(
            pb::PortfolioResponse {
                account_id: ACC.into(),
                positions: vec![pb::PortfolioPosition {
                    instrument_type: "share".into(),
                    ticker: "SBER".into(),
                    class_code: "TQBR".into(),
                    instrument_uid: "uid-sber".into(),
                    quantity: Some(quotation("30.0")),
                    ..Default::default()
                }],
                ..Default::default()
            },
        )),
    }));
    let ev = h.take_events();
    assert!(
        matches!(&ev[0].payload, Event::Balances(b) if b[0].currency == "RUB" && b[0].available.decimal() == dec!(1000.0))
    );
    let Event::Portfolio(p) = &ev[1].payload else {
        panic!("portfolio expected")
    };
    assert_eq!(p.positions[0].qty.decimal(), dec!(30.0));
    // the portfolio carries the cash learnt from the positions stream
    assert_eq!(p.balances[0].available.decimal(), dec!(1000.0));
    assert!(ev.iter().all(|e| e.sub == Some(SubscriptionId::new(1003))));
}

#[test]
fn a_reopened_order_stream_asks_for_a_refresh() {
    let mut h = ud();
    h.feed_frame(UdFrame::Connected(StreamKind::OrderState));
    assert!(h.core_mut().take_reconnected().is_empty());
    h.feed_frame(UdFrame::Connected(StreamKind::OrderState));
    assert_eq!(
        h.core_mut().take_reconnected(),
        vec![StreamKind::OrderState]
    );
}

#[test]
fn refresh_after_reconnect_emits_only_what_changed() {
    let mut h = ud();
    let now = Instant::now();
    h.core_mut()
        .begin_place(&place_req(cid(1)), 3, now)
        .unwrap();
    h.feed_frame(stream_state(
        cid(1),
        pb::OrderExecutionReportStatus::ExecutionReportStatusNew,
        0,
        ACC,
    ));
    h.take_events();
    let state = |executed: i64, status: pb::OrderExecutionReportStatus| pb::OrderState {
        order_id: "EX1".into(),
        execution_report_status: status as i32,
        lots_requested: 3,
        lots_executed: executed,
        order_request_id: cid(1).hyphenated().to_string(),
        ticker: "SBER".into(),
        class_code: "TQBR".into(),
        instrument_uid: "uid-sber".into(),
        ..Default::default()
    };
    h.feed_frame(UdFrame::ActiveOrders(pb::GetOrdersResponse {
        orders: vec![state(
            0,
            pb::OrderExecutionReportStatus::ExecutionReportStatusNew,
        )],
    }));
    assert!(h.take_events().is_empty(), "unchanged order, no event");
    h.feed_frame(UdFrame::ActiveOrders(pb::GetOrdersResponse {
        orders: vec![state(
            2,
            pb::OrderExecutionReportStatus::ExecutionReportStatusPartiallyfill,
        )],
    }));
    assert_eq!(order_events(&mut h).len(), 1);
}
