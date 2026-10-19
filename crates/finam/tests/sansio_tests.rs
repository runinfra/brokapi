//! Level-1: recorded frames through the sans-io cores, no sockets, no clock.

use brokapi_core::dec;
use std::sync::Arc;
use std::time::Instant;

use brokapi_core::Decimal;
use brokapi_core::{
    CidCodec, ClientOrderId, Event, InstrumentId, OrderBook, OrderState, ResyncReason, Side,
    SubscriptionId, Timeframe,
};
use brokapi_finam::FinamCidCodec;
use brokapi_finam::core::md::{MdEvent, MdFrame};
use brokapi_finam::core::ud::{UdEvent, UdFrame};
use brokapi_finam::core::{MdCore, SubStatus, UdCore};
use brokapi_finam::index::OrderIndex;
use brokapi_finam::map::market::{
    ApplyOutcome, BookState, RowAction, RowView, bar_close, timeframe_to_pb, wire_timeframe,
};
use brokapi_finam_protocol::pb::google::r#type::{Decimal as PbDecimal, Money};
use brokapi_finam_protocol::pb::grpc::tradeapi::v1;
use brokapi_finam_protocol::pb::grpc::tradeapi::v1::accounts as pa;
use brokapi_finam_protocol::pb::grpc::tradeapi::v1::marketdata as pm;
use brokapi_finam_protocol::pb::grpc::tradeapi::v1::marketdata::stream_order_book::{
    Row,
    row::{Action, Side as RowSide},
};
use brokapi_finam_protocol::pb::grpc::tradeapi::v1::orders as po;
use brokapi_sansio::{DesiredSubscription, Harness, SubscriptionKind, SubscriptionSet};

fn d(s: &str) -> Option<PbDecimal> {
    Some(PbDecimal { value: s.into() })
}

fn instr(sym: &str) -> InstrumentId {
    brokapi_finam::util::instrument_from_symbol(sym)
}

fn row(price: &str, ask: bool, size: &str, action: Action) -> Row {
    let size = PbDecimal { value: size.into() };
    Row {
        price: d(price),
        side: Some(if ask {
            RowSide::SellSize(size)
        } else {
            RowSide::BuySize(size)
        }),
        action: action as i32,
        ..Default::default()
    }
}

fn book_msg(rows: Vec<Row>, snapshot: bool) -> pm::SubscribeOrderBookResponse {
    pm::SubscribeOrderBookResponse {
        order_book: vec![pm::StreamOrderBook {
            symbol: "SBER@MISX".into(),
            rows,
            is_data_snapshot: snapshot,
        }],
    }
}

const SUB: SubscriptionId = SubscriptionId(1);

fn md_harness(depth: u32) -> Harness<MdCore> {
    let mut h = Harness::new(MdCore::new(), Instant::now());
    h.apply(&SubscriptionSet::with_subscriptions(vec![
        DesiredSubscription {
            id: SUB,
            kind: SubscriptionKind::OrderBook {
                instrument: instr("SBER@MISX"),
                depth,
            },
        },
    ]));
    h
}

fn frame(event: MdEvent) -> MdFrame {
    MdFrame { sub: SUB, event }
}

type Levels = Vec<(Decimal, Decimal)>;

fn last_book(h: &Harness<MdCore>) -> (Levels, Levels) {
    let ev = h
        .events()
        .iter()
        .rev()
        .find_map(|e| match &e.payload {
            Event::OrderBook(OrderBook::Snapshot { bids, asks, .. }) => {
                Some((bids.clone(), asks.clone()))
            }
            _ => None,
        })
        .expect("a book event");
    let p = |v: Vec<brokapi_core::BookLevel>| {
        v.iter()
            .map(|l| (l.price.decimal(), l.qty.decimal()))
            .collect()
    };
    (p(ev.0), p(ev.1))
}

fn snapshot_rows() -> Vec<Row> {
    vec![
        row("100.0", false, "10", Action::Add),
        row("99.9", false, "20", Action::Add),
        row("100.1", true, "10", Action::Add),
        row("100.2", true, "20", Action::Add),
    ]
}

#[test]
fn book_snapshot_then_action_rows() {
    let mut h = md_harness(10);
    h.feed_frame(frame(MdEvent::Connected));
    h.feed_frame(frame(MdEvent::OrderBook(book_msg(snapshot_rows(), true))));
    let (bids, asks) = last_book(&h);
    assert_eq!(
        bids,
        vec![(dec!(100.0), dec!(10.0)), (dec!(99.9), dec!(20.0))],
        "best bid first"
    );
    assert_eq!(
        asks,
        vec![(dec!(100.1), dec!(10.0)), (dec!(100.2), dec!(20.0))],
        "best ask first"
    );

    // UPDATE resizes a level, ADD opens a new one, REMOVE deletes one.
    h.feed_frame(frame(MdEvent::OrderBook(book_msg(
        vec![
            row("100.0", false, "15", Action::Update),
            row("100.15", true, "5", Action::Add),
            row("99.9", false, "0", Action::Remove),
        ],
        false,
    ))));
    let (bids, asks) = last_book(&h);
    assert_eq!(bids, vec![(dec!(100.0), dec!(15.0))]);
    assert_eq!(
        asks,
        vec![
            (dec!(100.1), dec!(10.0)),
            (dec!(100.15), dec!(5.0)),
            (dec!(100.2), dec!(20.0))
        ]
    );

    // A zero size without REMOVE also removes the level.
    h.feed_frame(frame(MdEvent::OrderBook(book_msg(
        vec![row("100.1", true, "0", Action::Update)],
        false,
    ))));
    assert_eq!(
        last_book(&h).1,
        vec![(dec!(100.15), dec!(5.0)), (dec!(100.2), dec!(20.0))]
    );

    // Every event carries the subscription it belongs to.
    assert!(h.events().iter().all(|e| e.sub == Some(SUB)));
}

#[test]
fn a_new_snapshot_replaces_the_book() {
    let mut h = md_harness(10);
    h.feed_frame(frame(MdEvent::OrderBook(book_msg(snapshot_rows(), true))));
    h.feed_frame(frame(MdEvent::OrderBook(book_msg(
        vec![
            row("50.0", false, "1", Action::Add),
            row("51.0", true, "1", Action::Add),
        ],
        true,
    ))));
    assert_eq!(
        last_book(&h),
        (vec![(dec!(50.0), dec!(1.0))], vec![(dec!(51.0), dec!(1.0))])
    );
}

#[test]
fn depth_truncates_each_side() {
    let mut h = md_harness(1);
    h.feed_frame(frame(MdEvent::OrderBook(book_msg(snapshot_rows(), true))));
    assert_eq!(
        last_book(&h),
        (
            vec![(dec!(100.0), dec!(10.0))],
            vec![(dec!(100.1), dec!(10.0))]
        )
    );
}

#[test]
fn increment_without_snapshot_gives_resync_and_restart() {
    let mut h = md_harness(10);
    h.feed_frame(frame(MdEvent::OrderBook(book_msg(
        vec![row("100.0", false, "15", Action::Update)],
        false,
    ))));
    assert!(matches!(
        h.events().last().unwrap().payload,
        Event::Resync {
            reason: ResyncReason::SequenceGap,
            ..
        }
    ));
    assert_eq!(h.core_mut().take_restarts(), vec![SUB]);
    // The unusable increment produced no book.
    assert!(
        !h.events()
            .iter()
            .any(|e| matches!(e.payload, Event::OrderBook(_)))
    );
}

#[test]
fn reopened_stream_resets_the_book_and_announces_resync() {
    let mut h = md_harness(10);
    h.feed_frame(frame(MdEvent::Connected));
    h.feed_frame(frame(MdEvent::OrderBook(book_msg(snapshot_rows(), true))));
    let before = h.events().len();
    // First connect: no Resync.
    assert!(
        !h.events()
            .iter()
            .any(|e| matches!(e.payload, Event::Resync { .. }))
    );

    h.feed_frame(frame(MdEvent::Disconnected("unavailable".into())));
    assert_eq!(
        h.core().status(SUB),
        Some(SubStatus::Reconnecting("unavailable".into()))
    );
    h.feed_frame(frame(MdEvent::Connected));
    assert_eq!(h.core().status(SUB), Some(SubStatus::Active));
    assert!(matches!(
        h.events()[before].payload,
        Event::Resync {
            reason: ResyncReason::SubscriptionReset,
            ..
        }
    ));
    // After the reset an increment is not accepted until the snapshot arrives.
    h.feed_frame(frame(MdEvent::OrderBook(book_msg(
        vec![row("100.0", false, "1", Action::Update)],
        false,
    ))));
    assert_eq!(h.core_mut().take_restarts(), vec![SUB]);
}

#[test]
fn frames_of_unknown_subscriptions_are_ignored() {
    let mut h = md_harness(10);
    h.feed_frame(MdFrame {
        sub: SubscriptionId(99),
        event: MdEvent::OrderBook(book_msg(snapshot_rows(), true)),
    });
    assert!(h.events().is_empty());
}

#[test]
fn failed_subscription_is_visible_in_status() {
    let mut h = md_harness(10);
    h.feed_frame(frame(MdEvent::Failed("permission_denied".into())));
    assert_eq!(
        h.core().status(SUB),
        Some(SubStatus::Failed("permission_denied".into()))
    );
}

#[test]
fn book_state_unit() {
    let mut s = BookState::default();
    let r = |price: &str, ask, size: &str, action| RowView {
        price: price.parse().unwrap(),
        is_ask: ask,
        size: size.parse().unwrap(),
        action,
        ts: None,
    };
    assert_eq!(
        s.apply(&[r("1.0", false, "1.0", RowAction::Set)], false),
        ApplyOutcome::NeedSnapshot
    );
    assert_eq!(
        s.apply(&[r("1.0", false, "1.0", RowAction::Set)], true),
        ApplyOutcome::Applied
    );
    assert_eq!(
        s.apply(&[r("1.0", false, "0.0", RowAction::Remove)], false),
        ApplyOutcome::Applied
    );
    assert!(s.levels(5).0.is_empty());
    // Removing a level that is not there is harmless.
    assert_eq!(
        s.apply(&[r("7.0", true, "0.0", RowAction::Remove)], false),
        ApplyOutcome::Applied
    );
}

fn single_sub_harness(kind: SubscriptionKind) -> Harness<MdCore> {
    let mut h = Harness::new(MdCore::new(), Instant::now());
    h.apply(&SubscriptionSet::with_subscriptions(vec![
        DesiredSubscription { id: SUB, kind },
    ]));
    h
}

#[test]
fn quote_with_a_missing_side_gives_none() {
    let mut h = single_sub_harness(SubscriptionKind::Quotes {
        instrument: instr("SBER@MISX"),
    });
    let q = pm::Quote {
        symbol: "SBER@MISX".into(),
        bid: d("0"),
        ask: d("100.1"),
        ask_size: d("7"),
        ..Default::default()
    };
    h.feed_frame(frame(MdEvent::Quotes(pm::SubscribeQuoteResponse {
        quote: vec![q],
        error: None,
    })));
    let Event::Quote(q) = &h.events()[0].payload else {
        panic!()
    };
    assert!(q.bid.is_none(), "0 means no active bid");
    assert_eq!(q.ask.as_ref().unwrap().qty.decimal(), dec!(7.0));
}

#[test]
fn in_band_quote_error_is_a_status_not_an_event() {
    let mut h = single_sub_harness(SubscriptionKind::Quotes {
        instrument: instr("SBER@MISX"),
    });
    h.feed_frame(frame(MdEvent::Quotes(pm::SubscribeQuoteResponse {
        quote: vec![],
        error: Some(pm::StreamError {
            code: 5,
            description: "upstream down".into(),
        }),
    })));
    assert!(h.events().is_empty());
    assert!(
        matches!(h.core().status(SUB), Some(SubStatus::Erroring(m)) if m.contains("upstream down"))
    );
}

#[test]
fn trades_are_deduplicated_across_reconnects_and_sideless_ones_dropped() {
    let mut h = single_sub_harness(SubscriptionKind::LastTrades {
        instrument: instr("SBER@MISX"),
    });
    let t = |id: &str, side: v1::Side| pm::Trade {
        trade_id: id.into(),
        price: d("100"),
        size: d("2"),
        side: side as i32,
        ..Default::default()
    };
    let msg = |trades| pm::SubscribeLatestTradesResponse {
        symbol: "SBER@MISX".into(),
        trades,
    };
    h.feed_frame(frame(MdEvent::Trades(msg(vec![
        t("1", v1::Side::Buy),
        t("2", v1::Side::Unspecified),
    ]))));
    // Reconnect replays 1 and adds 3.
    h.feed_frame(frame(MdEvent::Trades(msg(vec![
        t("1", v1::Side::Buy),
        t("3", v1::Side::Sell),
    ]))));
    let ids: Vec<_> = h
        .events()
        .iter()
        .filter_map(|e| match &e.payload {
            Event::MarketTrade(t) => Some((t.trade_id.clone().unwrap(), t.side)),
            _ => None,
        })
        .collect();
    assert_eq!(ids, vec![("1".into(), Side::Buy), ("3".into(), Side::Sell)]);
}

#[test]
fn bars_become_candles_with_derived_close() {
    let mut h = single_sub_harness(SubscriptionKind::LastCandle {
        instrument: instr("SBER@MISX"),
        timeframe: Timeframe::M5,
    });
    let bar = pm::Bar {
        timestamp: Some(prost_types::Timestamp {
            seconds: 1_700_000_000,
            nanos: 0,
        }),
        open: d("1"),
        high: d("3"),
        low: d("0.5"),
        close: d("2"),
        volume: d("100"),
        is_data_snapshot: false,
    };
    h.feed_frame(frame(MdEvent::Bars(pm::SubscribeBarsResponse {
        symbol: "SBER@MISX".into(),
        bars: vec![bar],
    })));
    let Event::Candle(c) = &h.events()[0].payload else {
        panic!()
    };
    assert_eq!(c.timeframe, Timeframe::M5);
    assert_eq!(
        c.close_ts.as_nanos() - c.open_ts.as_nanos(),
        300 * 1_000_000_000
    );
    assert_eq!(
        (
            c.open.decimal(),
            c.high.decimal(),
            c.low.decimal(),
            c.close.decimal()
        ),
        (dec!(1.0), dec!(3.0), dec!(0.5), dec!(2.0))
    );
}

#[test]
fn m5_stream_becomes_m10_candles() {
    let mut h = single_sub_harness(SubscriptionKind::LastCandle {
        instrument: instr("SBER@MISX"),
        timeframe: Timeframe::M10,
    });
    let bar = |sec: i64, o: &str, hi: &str, lo: &str, c: &str, v: &str| pm::Bar {
        timestamp: Some(prost_types::Timestamp {
            seconds: sec,
            nanos: 0,
        }),
        open: d(o),
        high: d(hi),
        low: d(lo),
        close: d(c),
        volume: d(v),
        is_data_snapshot: false,
    };
    let base = 1_800_000; // a 10 minute boundary
    for b in [
        bar(base, "10", "12", "9", "11", "100"),
        bar(base + 300, "11", "15", "10.5", "14", "50"),
        bar(base + 600, "14", "14.5", "13.5", "14.2", "7"),
    ] {
        h.feed_frame(frame(MdEvent::Bars(pm::SubscribeBarsResponse {
            symbol: "SBER@MISX".into(),
            bars: vec![b],
        })));
    }
    let candles: Vec<_> = h
        .events()
        .iter()
        .filter_map(|e| match &e.payload {
            Event::Candle(c) => Some(c.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(candles.len(), 3);
    assert!(candles.iter().all(|c| c.timeframe == Timeframe::M10));
    // After the second M5 the first 10-minute candle is complete.
    let c = &candles[1];
    assert_eq!(
        (
            c.open.decimal(),
            c.high.decimal(),
            c.low.decimal(),
            c.close.decimal(),
            c.volume.decimal()
        ),
        (dec!(10.0), dec!(15.0), dec!(9.0), dec!(14.0), dec!(150.0))
    );
    assert_eq!(
        c.close_ts.as_nanos() - c.open_ts.as_nanos(),
        600 * 1_000_000_000
    );
    // The third M5 opens the next interval: a lone half, emitted as it is.
    let c = &candles[2];
    assert_eq!(
        (c.open.decimal(), c.close.decimal(), c.volume.decimal()),
        (dec!(14.0), dec!(14.2), dec!(7.0))
    );
    assert_eq!(c.open_ts.as_nanos(), (base + 600) * 1_000_000_000);
}

#[test]
fn timeframes() {
    assert!(
        timeframe_to_pb(Timeframe::M10).is_err(),
        "Finam has no 10 minute bars"
    );
    assert_eq!(
        wire_timeframe(Timeframe::M10).unwrap(),
        pm::TimeFrame::M5,
        "M10 is asked as M5"
    );
    assert_eq!(wire_timeframe(Timeframe::M15).unwrap(), pm::TimeFrame::M15);
    assert_eq!(timeframe_to_pb(Timeframe::D1).unwrap(), pm::TimeFrame::D);
    // Month close is the same day of the next month (UTC).
    let jan1 = brokapi_core::UnixNanos::from_nanos(1_704_067_200 * 1_000_000_000); // 2024-01-01
    let feb1 = bar_close(Timeframe::MN, jan1);
    assert_eq!(feb1.as_nanos(), 1_706_745_600 * 1_000_000_000);
}

// ----------------------------------------------------------------------- user data

fn order(
    id: &str,
    status: po::OrderStatus,
    cid: Option<ClientOrderId>,
    filled: &str,
    left: &str,
) -> po::OrderState {
    po::OrderState {
        order_id: id.into(),
        status: status as i32,
        order: Some(po::Order {
            account_id: "ACC1".into(),
            symbol: "SBER@MISX".into(),
            quantity: d("10"),
            side: v1::Side::Sell as i32,
            r#type: po::OrderType::Limit as i32,
            limit_price: d("250.5"),
            client_order_id: cid
                .map(|c| FinamCidCodec.client_order_id(c))
                .unwrap_or_default(),
            comment: cid
                .map(|c| FinamCidCodec.encode(c).unwrap().0)
                .unwrap_or_else(|| "placed in the app".into()),
            ..Default::default()
        }),
        initial_quantity: d("10"),
        executed_quantity: d(filled),
        remaining_quantity: d(left),
        status_description: None,
        ..Default::default()
    }
}

const USUB: SubscriptionId = SubscriptionId(1001);

fn ud_harness() -> Harness<UdCore> {
    let mut h = Harness::new(UdCore::new(Arc::new(OrderIndex::new())), Instant::now());
    h.apply(&SubscriptionSet::with_subscriptions(vec![
        DesiredSubscription {
            id: USUB,
            kind: SubscriptionKind::OrderState,
        },
    ]));
    h
}

fn orders_frame(orders: Vec<po::OrderState>) -> UdFrame {
    UdFrame {
        sub: USUB,
        event: UdEvent::Orders(po::SubscribeOrdersResponse { orders }),
    }
}

fn snapshots(h: &Harness<UdCore>) -> Vec<brokapi_core::OrderSnapshot> {
    h.events()
        .iter()
        .filter_map(|e| match &e.payload {
            Event::Order(o) => Some(o.clone()),
            _ => None,
        })
        .collect()
}

#[test]
fn order_statuses_map_to_core_states() {
    use po::OrderStatus as S;
    let cid = ClientOrderId::from_bytes([1; 16]);
    let cases = [
        (S::New, "0", "10", "acked"),
        (S::PendingNew, "0", "10", "acked"),
        (S::PendingCancel, "0", "10", "acked"),
        (S::Suspended, "0", "10", "acked"),
        (S::PartiallyFilled, "4", "6", "partial"),
        (S::New, "4", "6", "partial"),
        (S::Filled, "10", "0", "filled"),
        (S::Executed, "10", "0", "filled"),
        (S::Canceled, "0", "10", "cancelled"),
        (S::Expired, "0", "10", "cancelled"),
        (S::Replaced, "0", "10", "cancelled"),
        (S::Rejected, "0", "10", "rejected"),
        (S::DeniedByBroker, "0", "10", "rejected"),
        (S::RejectedByExchange, "0", "10", "rejected"),
        (S::Failed, "0", "10", "rejected"),
    ];
    for (status, filled, left, want) in cases {
        let mut h = ud_harness();
        h.feed_frame(orders_frame(vec![order(
            "1",
            status,
            Some(cid),
            filled,
            left,
        )]));
        let snap = &snapshots(&h)[0];
        let got = match &snap.state {
            OrderState::Acked { .. } => "acked",
            OrderState::PartiallyFilled { .. } => "partial",
            OrderState::Filled { .. } => "filled",
            OrderState::Cancelled { .. } => "cancelled",
            OrderState::Rejected { .. } => "rejected",
            other => panic!("{other:?}"),
        };
        assert_eq!(got, want, "{status:?}");
        assert_eq!(snap.cid, cid);
        assert_eq!(snap.broker_id.as_ref().unwrap().as_str(), "1");
    }
}

#[test]
fn order_echo_maps_fields() {
    let cid = ClientOrderId::from_bytes([2; 16]);
    let mut h = ud_harness();
    h.feed_frame(orders_frame(vec![order(
        "77",
        po::OrderStatus::PartiallyFilled,
        Some(cid),
        "4",
        "6",
    )]));
    let s = &snapshots(&h)[0];
    assert_eq!(s.instrument.key.display(), "SBER@MISX");
    assert_eq!(s.side, Side::Sell);
    assert_eq!(s.order_type, brokapi_core::OrderType::Limit);
    assert_eq!(
        (s.qty.decimal(), s.filled.decimal()),
        (dec!(10.0), dec!(4.0))
    );
    assert_eq!(s.price.unwrap().decimal(), dec!(250.5));
    assert!(
        matches!(s.state, OrderState::PartiallyFilled { filled, left, .. } if filled.decimal() == dec!(4.0) && left.decimal() == dec!(6.0))
    );
}

#[test]
fn our_orders_resolve_by_comment_and_foreign_ones_get_a_stable_cid() {
    let cid = ClientOrderId::from_bytes([3; 16]);
    let mut h = ud_harness();
    h.feed_frame(orders_frame(vec![
        order("1", po::OrderStatus::New, Some(cid), "0", "10"),
        order("2", po::OrderStatus::New, None, "0", "10"),
    ]));
    h.feed_frame(orders_frame(vec![order(
        "2",
        po::OrderStatus::Canceled,
        None,
        "0",
        "10",
    )]));
    let s = snapshots(&h);
    assert_eq!(s[0].cid, cid);
    assert_ne!(s[1].cid, cid);
    assert_eq!(s[1].cid, s[2].cid, "same foreign order, same cid");
    assert_eq!(s[1].cid, brokapi_finam::index::foreign_cid("2"));
}

#[test]
fn local_lifecycle_follows_the_stream() {
    let cid = ClientOrderId::from_bytes([4; 16]);
    let mut h = ud_harness();
    let now = h.now();
    h.core_mut().orders().register_place(cid, now);
    h.feed_frame(orders_frame(vec![order(
        "5",
        po::OrderStatus::New,
        Some(cid),
        "0",
        "10",
    )]));
    assert!(matches!(
        h.core_mut().orders().state(cid),
        Some(OrderState::Acked { .. })
    ));
    h.feed_frame(orders_frame(vec![order(
        "5",
        po::OrderStatus::Filled,
        Some(cid),
        "10",
        "0",
    )]));
    assert!(matches!(
        h.core_mut().orders().state(cid),
        Some(OrderState::Filled { .. })
    ));
}

#[test]
fn own_trades_carry_the_cid_when_the_order_is_ours() {
    let cid = ClientOrderId::from_bytes([5; 16]);
    let mut h = Harness::new(UdCore::new(Arc::new(OrderIndex::new())), Instant::now());
    h.core_mut()
        .register(USUB, brokapi_finam::core::ud::UdSubKind::Executions);
    let trade = |order_id: &str, comment: &str| v1::AccountTrade {
        trade_id: "T1".into(),
        symbol: "SBER@MISX".into(),
        price: d("250.5"),
        size: d("3"),
        side: v1::Side::Buy as i32,
        order_id: order_id.into(),
        comment: comment.into(),
        commission: Some(v1::account_trade::Commission {
            value: d("0.75"),
            currency: "RUB".into(),
        }),
        ..Default::default()
    };
    let ours = FinamCidCodec.encode(cid).unwrap().0;
    h.feed_frame(UdFrame {
        sub: USUB,
        event: UdEvent::Trades(po::SubscribeTradesResponse {
            trades: vec![trade("9", &ours), trade("10", "manual")],
        }),
    });
    let execs: Vec<_> = h
        .events()
        .iter()
        .filter_map(|e| match &e.payload {
            Event::Execution(x) => Some(x.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(execs[0].cid, Some(cid));
    assert_eq!(execs[0].broker_order_id.as_str(), "9");
    assert_eq!(execs[0].commission.decimal(), dec!(0.75));
    assert_eq!(execs[1].cid, None, "not our order: no cid is invented");
}

#[test]
fn account_state_becomes_a_portfolio() {
    let mut h = Harness::new(UdCore::new(Arc::new(OrderIndex::new())), Instant::now());
    h.apply(&SubscriptionSet::with_subscriptions(vec![
        DesiredSubscription {
            id: USUB,
            kind: SubscriptionKind::Portfolio,
        },
    ]));
    let resp = pa::GetAccountResponse {
        account_id: "ACC1".into(),
        cash: vec![
            Money {
                currency_code: "RUB".into(),
                units: 1000,
                nanos: 250_000_000,
            },
            Money {
                currency_code: "USD".into(),
                units: 10,
                nanos: 0,
            },
        ],
        positions: vec![
            pa::Position {
                symbol: "SBER@MISX".into(),
                quantity: d("10"),
                average_price: d("250"),
                current_price: d("260"),
                ..Default::default()
            },
            // FORTS-like: no average price, short.
            pa::Position {
                symbol: "SiZ6@RTSX".into(),
                quantity: d("-2"),
                current_price: d("95000"),
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    h.feed_frame(UdFrame {
        sub: USUB,
        event: UdEvent::Account(resp),
    });
    let Event::Portfolio(p) = &h.events()[0].payload else {
        panic!()
    };
    assert_eq!(p.balances.len(), 2);
    assert_eq!(
        (
            p.balances[0].currency.as_str(),
            p.balances[0].available.decimal()
        ),
        ("RUB", dec!(1000.25))
    );
    assert_eq!(p.balances[0].blocked.decimal(), dec!(0.0));
    assert_eq!(p.positions[0].avg_price.unwrap().decimal(), dec!(250.0));
    assert_eq!(p.positions[1].qty.decimal(), dec!(-2.0));
    assert!(p.positions[1].avg_price.is_none());
}
