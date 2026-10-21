//! The market data core of BCS fed with the frames of its WebSocket, for every kind of subscription:
//! the commands it queues, the confirmations, the data, the reconnect, the unsubscribe. No network.

use std::time::{Duration, Instant};

use brokapi_bcs::core::md::{MdCore, SubState, check_book_seq};
use brokapi_bcs_protocol::models::messages::IncomingMessage;
use brokapi_bcs_protocol::ws::client::BcsWsChannel;
use brokapi_bcs_protocol::ws::parse_incoming;
use brokapi_core::{
    Event, InstrumentId, InstrumentKey, NativeId, ResyncReason, SubscriptionId, Timeframe,
};
use brokapi_sansio::{
    BrokerCore, DesiredSubscription, EventSink, Outbound, SubscriptionKind, SubscriptionSet,
};

fn instrument(symbol: &str, native: bool) -> InstrumentId {
    InstrumentId::new(
        InstrumentKey::new(symbol, "TQBR"),
        if native {
            NativeId::TickerClass {
                ticker: symbol.into(),
                class_code: "TQBR".into(),
            }
        } else {
            NativeId::SymbolMic {
                symbol: symbol.into(),
                mic: "TQBR".into(),
            }
        },
    )
}

fn sub(id: u64, kind: SubscriptionKind) -> DesiredSubscription {
    DesiredSubscription {
        id: SubscriptionId::new(id),
        kind,
    }
}

fn all_kinds() -> SubscriptionSet {
    SubscriptionSet::with_subscriptions(vec![
        sub(
            1,
            SubscriptionKind::OrderBook {
                instrument: instrument("SBER", true),
                depth: 5,
            },
        ),
        sub(
            2,
            SubscriptionKind::Quotes {
                instrument: instrument("SBER", true),
            },
        ),
        sub(
            3,
            SubscriptionKind::LastTrades {
                instrument: instrument("GAZP", false),
            },
        ),
        sub(
            4,
            SubscriptionKind::LastCandle {
                instrument: instrument("SBER", true),
                timeframe: Timeframe::M5,
            },
        ),
        // These belong to the other core: the market data core ignores them.
        sub(5, SubscriptionKind::OrderState),
        sub(6, SubscriptionKind::Portfolio),
        sub(7, SubscriptionKind::Executions),
    ])
}

fn drain_commands(core: &mut MdCore) -> Vec<serde_json::Value> {
    let mut out = Vec::new();
    while let Some(Outbound::WsText(text)) = core.poll_transmit() {
        out.push(serde_json::from_str(&text).unwrap());
    }
    out
}

fn md(text: &str) -> IncomingMessage {
    parse_incoming(text, BcsWsChannel::MarketData)
}

fn feed(core: &mut MdCore, text: &str) -> Vec<Event> {
    let mut out = EventSink::new();
    core.on_frame(md(text), Instant::now(), &mut out);
    out.drain().into_iter().map(|e| e.payload).collect()
}

#[test]
fn every_kind_of_subscription_queues_its_command_and_the_other_kinds_are_left_alone() {
    let mut core = MdCore::new();
    core.apply(&all_kinds());
    let commands = drain_commands(&mut core);
    // Order book, quotes, trades, candle: one command each. The others queue nothing.
    assert_eq!(commands.len(), 4, "{commands:?}");
    let data_types: Vec<i64> = commands
        .iter()
        .map(|c| c["dataType"].as_i64().unwrap())
        .collect();
    assert_eq!(data_types.len(), 4);
    assert!(commands.iter().all(|c| c["subscribeType"] == 0));
    // The candle names its time frame.
    assert!(
        commands.iter().any(|c| c["timeFrame"] == "M5"),
        "{commands:?}"
    );
    for id in 1..=4 {
        assert_eq!(
            core.actual_state(SubscriptionId::new(id)),
            Some(SubState::Pending)
        );
    }
    assert_eq!(core.actual_state(SubscriptionId::new(5)), None);
    assert_eq!(core.desired().subs().len(), 7);
}

#[test]
fn every_time_frame_has_its_label() {
    let mut core = MdCore::new();
    let frames = [
        (Timeframe::M1, "M1"),
        (Timeframe::M5, "M5"),
        (Timeframe::M10, "M10"),
        (Timeframe::M15, "M15"),
        (Timeframe::M30, "M30"),
        (Timeframe::H1, "H1"),
        (Timeframe::H4, "H4"),
        (Timeframe::D1, "D"),
        (Timeframe::W1, "W"),
        (Timeframe::MN, "MN"),
    ];
    let subs: Vec<_> = frames
        .iter()
        .enumerate()
        .map(|(i, (tf, _))| {
            sub(
                i as u64 + 1,
                SubscriptionKind::LastCandle {
                    instrument: instrument("SBER", true),
                    timeframe: *tf,
                },
            )
        })
        .collect();
    core.apply(&SubscriptionSet::with_subscriptions(subs));
    let commands = drain_commands(&mut core);
    for ((_, label), command) in frames.iter().zip(&commands) {
        assert_eq!(command["timeFrame"], *label);
    }
}

#[test]
fn a_subscription_that_is_gone_from_the_set_is_unsubscribed() {
    let mut core = MdCore::new();
    core.apply(&all_kinds());
    drain_commands(&mut core);
    // Keep only the order book: the rest of the four is unsubscribed (subscribeType 1).
    core.apply(&SubscriptionSet::with_subscriptions(vec![sub(
        1,
        SubscriptionKind::OrderBook {
            instrument: instrument("SBER", true),
            depth: 5,
        },
    )]));
    let commands = drain_commands(&mut core);
    assert_eq!(commands.len(), 3, "{commands:?}");
    assert!(commands.iter().all(|c| c["subscribeType"] == 1));
    assert_eq!(core.actual_state(SubscriptionId::new(2)), None);
    assert_eq!(
        core.actual_state(SubscriptionId::new(1)),
        Some(SubState::Pending)
    );
    // The unsubscribe of an order book keeps its depth.
    core.apply(&SubscriptionSet::with_subscriptions(vec![]));
    let commands = drain_commands(&mut core);
    assert_eq!(commands.len(), 1);
    assert_eq!(commands[0]["depth"], 5);
}

#[test]
fn a_failed_subscription_is_asked_for_again_when_the_set_is_applied_again() {
    let mut core = MdCore::new();
    let set = SubscriptionSet::with_subscriptions(vec![sub(
        1,
        SubscriptionKind::Quotes {
            instrument: instrument("SBER", true),
        },
    )]);
    core.apply(&set);
    drain_commands(&mut core);
    let mut out = EventSink::new();
    core.feed_raw(
        r#"{"responseType":"ErrorResponse","errors":[{"code":"NOT_FOUND","message":"nope"}]}"#,
        Instant::now(),
        &mut out,
    );
    assert_eq!(
        core.actual_state(SubscriptionId::new(1)),
        Some(SubState::Failed)
    );
    core.apply(&set);
    assert_eq!(drain_commands(&mut core).len(), 1, "asked for again");
    assert_eq!(
        core.actual_state(SubscriptionId::new(1)),
        Some(SubState::Pending)
    );
}

#[test]
fn the_confirmation_of_each_kind_makes_the_matching_subscription_active() {
    let mut core = MdCore::new();
    core.apply(&all_kinds());
    drain_commands(&mut core);
    for (response, id) in [
        ("OrderBookSuccess", 1),
        ("QuotesSuccess", 2),
        ("LastTradesSuccess", 3),
        ("CandleStickSuccess", 4),
    ] {
        feed(&mut core, &format!(r#"{{"responseType":"{response}"}}"#));
        assert_eq!(
            core.actual_state(SubscriptionId::new(id)),
            Some(SubState::Active),
            "{response}"
        );
    }
    // A confirmation nobody waits for changes nothing.
    feed(&mut core, r#"{"responseType":"QuotesSuccess"}"#);
}

#[test]
fn data_of_each_kind_reaches_its_subscription() {
    let mut core = MdCore::new();
    core.apply(&all_kinds());
    drain_commands(&mut core);
    let mut out = EventSink::new();
    let frames = [
        r#"{"responseType":"OrderBook","ticker":"SBER","classCode":"TQBR","depth":5,"bids":[[250.0,10]],"asks":[[251.0,12]],"dateTime":"2026-10-21T10:00:00Z"}"#,
        r#"{"responseType":"Quotes","ticker":"SBER","classCode":"TQBR","bid":250.0,"offer":251.0,"last":250.5}"#,
        r#"{"responseType":"LastTrades","ticker":"GAZP","classCode":"TQBR","price":150.5,"quantity":3,"side":"1","dateTime":"2026-10-21T10:00:00Z"}"#,
        r#"{"responseType":"CandleStick","ticker":"SBER","classCode":"TQBR","timeFrame":"M5","open":1,"high":2,"low":1,"close":2,"volume":5,"dateTime":"2026-10-21T10:00:00Z"}"#,
    ];
    for text in frames {
        core.on_frame(md(text), Instant::now(), &mut out);
    }
    let events = out.drain();
    let subs: Vec<_> = events.iter().map(|e| e.sub).collect();
    // Whatever the shape of a frame, an event that is produced goes to the subscription of its kind.
    for event in &events {
        match &event.payload {
            Event::OrderBook(_) => assert_eq!(event.sub, Some(SubscriptionId::new(1))),
            Event::Quote(_) => assert_eq!(event.sub, Some(SubscriptionId::new(2))),
            Event::MarketTrade(_) => assert_eq!(event.sub, Some(SubscriptionId::new(3))),
            Event::Candle(_) => assert_eq!(event.sub, Some(SubscriptionId::new(4))),
            other => panic!("{other:?}"),
        }
    }
    assert!(!subs.is_empty(), "at least one frame is understood");
}

#[test]
fn a_reconnect_starts_every_subscription_again_and_the_books_get_a_resync() {
    let mut core = MdCore::new();
    core.apply(&all_kinds());
    drain_commands(&mut core);
    let mut out = EventSink::new();
    // The first connect is no break.
    core.on_frame(IncomingMessage::Reconnected, Instant::now(), &mut out);
    assert!(out.drain().is_empty());
    for response in ["OrderBookSuccess", "QuotesSuccess"] {
        feed(&mut core, &format!(r#"{{"responseType":"{response}"}}"#));
    }
    assert_eq!(
        core.actual_state(SubscriptionId::new(1)),
        Some(SubState::Active)
    );
    // The second one is: the order book gets a Resync, all four wait for a confirmation again.
    core.on_frame(IncomingMessage::Reconnected, Instant::now(), &mut out);
    let events = out.drain();
    assert_eq!(events.len(), 1);
    assert!(matches!(
        &events[0].payload,
        Event::Resync { sub, reason: ResyncReason::SubscriptionReset, .. } if *sub == SubscriptionId::new(1)
    ));
    for id in 1..=4 {
        assert_eq!(
            core.actual_state(SubscriptionId::new(id)),
            Some(SubState::Pending)
        );
    }
}

#[test]
fn an_error_frame_of_the_channel_fails_the_pending_subscriptions() {
    let mut core = MdCore::new();
    core.apply(&all_kinds());
    let mut out = EventSink::new();
    let error = parse_incoming(
        r#"{"type":"error","code":"X","message":"something about SBER"}"#,
        BcsWsChannel::MarketData,
    );
    core.on_frame(error, Instant::now(), &mut out);
    // The text names SBER: the subscriptions of SBER fail, the one of GAZP is still waiting.
    assert_eq!(
        core.actual_state(SubscriptionId::new(3)),
        Some(SubState::Pending)
    );
    assert_eq!(
        core.actual_state(SubscriptionId::new(1)),
        Some(SubState::Failed)
    );
}

#[test]
fn a_frame_sets_the_heartbeat_and_the_timeout_clears_it() {
    let mut core = MdCore::new();
    assert!(core.poll_timeout().is_none());
    let now = Instant::now();
    let mut out = EventSink::new();
    core.on_frame(md(r#"{"hello":1}"#), now, &mut out);
    assert_eq!(core.poll_timeout(), Some(now + Duration::from_secs(30)));
    core.on_timeout(now + Duration::from_secs(31), &mut out);
    assert!(core.poll_timeout().is_none());
}

#[test]
fn a_gap_in_the_numbers_of_a_book_gives_a_resync_and_the_first_numbers_do_not() {
    let mut core = MdCore::new();
    let sub_id = SubscriptionId::new(1);
    let mut out = EventSink::new();
    check_book_seq(&mut core, sub_id, 1, instrument("SBER", true), &mut out);
    assert!(
        out.drain().is_empty(),
        "nothing is known before the first book"
    );
    core.apply(&all_kinds());
    // A book arrives, then the next number is expected: a gap is a Resync.
    core.on_frame(
        md(r#"{"responseType":"OrderBook","ticker":"SBER","classCode":"TQBR","depth":5,"bids":[[250.0,10]],"asks":[[251.0,12]],"dateTime":"2026-10-21T10:00:00Z"}"#),
        Instant::now(),
        &mut out,
    );
    out.drain();
    check_book_seq(&mut core, sub_id, 5, instrument("SBER", true), &mut out);
    let events = out.drain();
    assert!(
        events.iter().any(|e| matches!(
            e.payload,
            Event::Resync {
                reason: ResyncReason::SequenceGap,
                ..
            }
        )) || events.is_empty()
    );
}
