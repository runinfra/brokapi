//! The fixes of the code review of 2026-10-21 for T-Bank: quantities in units (not lots), the gate after
//! the queue, the cut of the order streams, the errors. Against the gRPC mock, no network.

mod mock_server;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use brokapi_core::{
    ClientOrderId, Envelope, Error, Event, ExecutionSource, InstrumentId, InstrumentKey,
    InstrumentSource, MarketDataSource, NativeId, OrderBook, OrderBookSource, OrderExecutor,
    OrderState, OrderType, PlaceOrderRequest, PlaceStopOrderRequest, Price, Qty, ShutdownMode,
    Side, StopExpiry, StopOrderExecutor, StopOrderKind, SubscribeOpts, TimeRange, Timeframe,
    UnixNanos,
};
use brokapi_runtime::BrokerLimiter;
use brokapi_tbank::limits::default_limits;
use brokapi_tbank::{MarketDataConfig, MarketDataConnector, UserDataConfig, UserDataConnector};
use brokapi_tbank_protocol::TbankConfig;
use mock_server::{ACCOUNT_A, MockOptions, MockServer, TOKEN};
use tokio::sync::mpsc;
use tokio::time::timeout;

/// SBER has a lot of 10 shares at the mock.
const LOT: i64 = 10;

fn sber() -> InstrumentId {
    InstrumentId::new(
        InstrumentKey::new("SBER", "TQBR"),
        NativeId::Uid("uid-sber".into()),
    )
}

fn limiter() -> Arc<BrokerLimiter> {
    BrokerLimiter::new(&default_limits())
}

fn cfg(server: &MockServer) -> TbankConfig {
    TbankConfig::for_mock(TOKEN, &server.endpoint).with_request_timeout(Duration::from_secs(5))
}

async fn ud(server: &MockServer) -> (UserDataConnector, mpsc::Receiver<Envelope>) {
    let (tx, rx) = mpsc::channel(512);
    let ud = UserDataConnector::start(
        UserDataConfig::new(cfg(server).with_account(ACCOUNT_A), "tbank-ud"),
        limiter(),
        tx,
    )
    .await
    .expect("ud");
    (ud, rx)
}

async fn md(server: &MockServer) -> (MarketDataConnector, mpsc::Receiver<Envelope>) {
    let (tx, rx) = mpsc::channel(512);
    let md = MarketDataConnector::start(
        MarketDataConfig::new(cfg(server), "tbank-md"),
        limiter(),
        tx,
    )
    .await
    .expect("md");
    (md, rx)
}

fn buy(qty: i64) -> PlaceOrderRequest {
    PlaceOrderRequest {
        cid: ClientOrderId::new_v4(),
        instrument: sber(),
        side: Side::Buy,
        order_type: OrderType::Limit,
        qty: Qty::from(qty),
        price: Some(Price::from(250)),
    }
}

async fn next_event(rx: &mut mpsc::Receiver<Envelope>, pred: impl Fn(&Event) -> bool) -> Envelope {
    timeout(Duration::from_secs(5), async {
        while let Some(env) = rx.recv().await {
            if pred(&env.payload) {
                return env;
            }
        }
        panic!("channel closed");
    })
    .await
    .expect("no matching event")
}

// ─── orders count in units (К1) ──────────────────────────────────────────────

#[tokio::test]
async fn an_order_in_units_is_sent_in_lots_and_comes_back_in_units() {
    let opts = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&opts)).await;
    let (ud, mut rx) = ud(&server).await;
    ud.reconcile(&[]).await.unwrap();
    ud.subscribe_order_state().await.unwrap();

    let req = buy(30);
    let cid = req.cid;
    ud.place(req).await.unwrap();
    // The place answer, the stream, the state by cid and the live list all say 30 units.
    let acked = next_event(&mut rx, |e| matches!(e, Event::Order(_))).await;
    let Event::Order(o) = acked.payload else {
        unreachable!()
    };
    assert_eq!(o.qty, Qty::from(30));
    assert_eq!(o.filled, Qty::ZERO);
    assert_eq!(ud.order_state(cid).await.unwrap().qty, Qty::from(30));
    let live = ud.active_orders().await.unwrap();
    assert_eq!(live.len(), 1);
    assert_eq!(live[0].qty, Qty::from(30));
    // One directory read for the lot, not one per call.
    assert_eq!(opts.instrument_by_calls.load(Ordering::SeqCst), 1);
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn a_fill_is_in_units_in_the_stream_and_in_the_history() {
    let server = MockServer::start(Arc::default()).await;
    let (ud, mut rx) = ud(&server).await;
    ud.reconcile(&[]).await.unwrap();
    ud.subscribe_order_state().await.unwrap();
    ud.subscribe_executions().await.unwrap();
    let req = buy(30);
    let cid = req.cid;
    ud.place(req).await.unwrap();
    server.fill(ACCOUNT_A, &cid.hyphenated().to_string());

    let filled = next_event(
        &mut rx,
        |e| matches!(e, Event::Order(o) if matches!(o.state, OrderState::Filled { .. })),
    )
    .await;
    let Event::Order(o) = filled.payload else {
        unreachable!()
    };
    assert_eq!((o.qty, o.filled), (Qty::from(30), Qty::from(30)));
    // The trade of the stream is in pieces: 3 lots of 10.
    let fill = next_event(&mut rx, |e| matches!(e, Event::Execution(_))).await;
    let Event::Execution(x) = fill.payload else {
        unreachable!()
    };
    assert_eq!(x.qty, Qty::from(30));
    assert_eq!(x.cid, Some(cid));
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn a_quantity_that_is_not_whole_lots_never_reaches_the_broker() {
    let opts = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&opts)).await;
    let (ud, _rx) = ud(&server).await;
    ud.reconcile(&[]).await.unwrap();
    for bad in ["5", "15", "0", "-10", "10.5"] {
        let mut req = buy(10);
        req.qty = bad.parse().unwrap();
        let err = ud.place(req).await.unwrap_err();
        assert!(
            matches!(&err, Error::Rejected { code, .. } if code == "invalid_qty"),
            "{bad}: {err:?}"
        );
    }
    assert_eq!(opts.post_calls.load(Ordering::SeqCst), 0);
    // A stop order is sized the same way.
    let stop = |qty: i64| PlaceStopOrderRequest {
        cid: ClientOrderId::new_v4(),
        instrument: sber(),
        side: Side::Sell,
        kind: StopOrderKind::StopLoss,
        qty: Qty::from(qty),
        stop_price: Price::from(100),
        limit_price: None,
        expiry: StopExpiry::GoodTillCancel,
    };
    let err = ud.place_stop(stop(15)).await.unwrap_err();
    assert!(
        matches!(&err, Error::Rejected { code, .. } if code == "invalid_qty"),
        "{err:?}"
    );
    assert_eq!(opts.stop_post_calls.load(Ordering::SeqCst), 0);
    let ack = ud.place_stop(stop(20)).await.unwrap();
    let active = ud.active_stop_orders().await.unwrap();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].broker_id, ack.broker_id);
    assert_eq!(active[0].qty, Qty::from(20), "2 lots of 10");
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn a_replace_takes_units_and_keeps_the_quantity_by_default() {
    let server = MockServer::start(Arc::default()).await;
    let (ud, _rx) = ud(&server).await;
    ud.reconcile(&[]).await.unwrap();
    let first = buy(30);
    ud.place(first.clone()).await.unwrap();
    let (second, third) = (ClientOrderId::new_v4(), ClientOrderId::new_v4());
    // No quantity: the 30 units stay.
    ud.replace(brokapi_core::ReplaceOrderRequest {
        cid: second,
        original_cid: first.cid,
        qty: None,
        price: Some(Price::from(251)),
    })
    .await
    .unwrap();
    assert_eq!(ud.order_state(second).await.unwrap().qty, Qty::from(30));
    // A new quantity in units, in whole lots.
    let bad = ud
        .replace(brokapi_core::ReplaceOrderRequest {
            cid: third,
            original_cid: second,
            qty: Some(Qty::from(15)),
            price: None,
        })
        .await
        .unwrap_err();
    assert!(
        matches!(&bad, Error::Rejected { code, .. } if code == "invalid_qty"),
        "{bad:?}"
    );
    ud.replace(brokapi_core::ReplaceOrderRequest {
        cid: third,
        original_cid: second,
        qty: Some(Qty::from(50)),
        price: None,
    })
    .await
    .unwrap();
    assert_eq!(ud.order_state(third).await.unwrap().qty, Qty::from(50));
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn a_replace_of_an_order_this_connector_never_saw_reads_its_lot() {
    let server = MockServer::start(Arc::default()).await;
    let (first, _rx1) = ud(&server).await;
    first.reconcile(&[]).await.unwrap();
    let req = buy(30);
    first.place(req.clone()).await.unwrap();
    // A second connector (a restart) knows nothing of the order.
    let (second, _rx2) = ud(&server).await;
    second.reconcile(&[req.cid]).await.unwrap();
    let new = ClientOrderId::new_v4();
    second
        .replace(brokapi_core::ReplaceOrderRequest {
            cid: new,
            original_cid: req.cid,
            qty: None,
            price: Some(Price::from(252)),
        })
        .await
        .unwrap();
    assert_eq!(second.order_state(new).await.unwrap().qty, Qty::from(30));
    first.shutdown(ShutdownMode::Immediate).await;
    second.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn an_unknown_instrument_is_not_found_for_orders_and_reads() {
    let server = MockServer::start(Arc::default()).await;
    let (ud, _rx) = ud(&server).await;
    ud.reconcile(&[]).await.unwrap();
    let nope = InstrumentId::new(
        InstrumentKey::new("NOPE", "TQBR"),
        NativeId::Uid("no-such-uid".into()),
    );
    let mut req = buy(10);
    req.instrument = nope.clone();
    assert!(ud.place(req).await.unwrap_err().is_not_found());
    let (md, _rx) = md(&server).await;
    assert!(md.get(&nope).await.unwrap_err().is_not_found());
    assert!(md.order_book(&nope, 5).await.unwrap_err().is_not_found());
    // Other failures of the directory stay what they were: a read that may be repeated.
    server.opts.instrument_by_fail.store(true, Ordering::SeqCst);
    let mut req = buy(10);
    req.instrument = InstrumentId::new(
        InstrumentKey::new("GAZP", "TQBR"),
        NativeId::Uid("uid-gazp".into()),
    );
    assert!(matches!(
        ud.place(req).await.unwrap_err(),
        Error::Transport(_)
    ));
    ud.shutdown(ShutdownMode::Immediate).await;
}

// ─── market data counts in units (К1) ────────────────────────────────────────

#[tokio::test]
async fn the_book_trades_and_candles_are_in_units() {
    let opts = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&opts)).await;
    let (md, mut rx) = md(&server).await;

    // The mock's best bid holds 100 lots, the best ask 80: 1000 and 800 shares.
    let OrderBook::Snapshot { bids, asks, .. } = md.order_book(&sber(), 5).await.unwrap() else {
        panic!("a snapshot")
    };
    assert_eq!(bids[0].qty, Qty::from(100 * LOT));
    assert_eq!(asks[0].qty, Qty::from(80 * LOT));

    // The streams say the same. The four subscriptions are made first, then the events are read: the mock
    // pushes a burst for each one, and a late subscription would find nothing to read.
    md.subscribe_order_book(&sber(), SubscribeOpts::lossless(64))
        .await
        .unwrap();
    md.subscribe_quotes(&sber(), SubscribeOpts::lossless(64))
        .await
        .unwrap();
    md.subscribe_market_trades(&sber(), SubscribeOpts::lossless(64))
        .await
        .unwrap();
    md.subscribe_candles(&sber(), Timeframe::M5, SubscribeOpts::lossless(64))
        .await
        .unwrap();
    let (mut book, mut quote, mut trade, mut candle) = (false, false, false, false);
    timeout(Duration::from_secs(5), async {
        while !(book && quote && trade && candle) {
            let env = rx.recv().await.expect("the channel stays open");
            match env.payload {
                Event::OrderBook(OrderBook::Snapshot { bids, .. }) => {
                    assert_eq!(bids[0].qty, Qty::from(100 * LOT));
                    book = true;
                }
                Event::Quote(q) => {
                    assert_eq!(q.bid.unwrap().qty.decimal() % ten(), zero());
                    quote = true;
                }
                Event::MarketTrade(t) => {
                    assert_eq!(t.qty.decimal() % ten(), zero(), "whole lots, in units");
                    trade = true;
                }
                Event::Candle(c) => {
                    assert_eq!(c.volume.decimal() % ten(), zero());
                    candle = true;
                }
                _ => {}
            }
        }
    })
    .await
    .expect("all four kinds of events");

    let trades = md.last_trades(&sber(), 10).await.unwrap();
    assert!(!trades.is_empty());
    assert!(trades.iter().all(|t| t.qty.decimal() % ten() == zero()));
    let range = TimeRange::new(
        UnixNanos::from_nanos(1_700_000_000_000_000_000),
        UnixNanos::from_nanos(1_700_001_000_000_000_000),
    );
    let bars = md.candles(&sber(), Timeframe::M5, range).await.unwrap();
    assert!(!bars.is_empty());
    assert!(bars.iter().all(|c| c.volume.decimal() % ten() == zero()));
    // One directory read for all of it.
    assert_eq!(opts.instrument_by_calls.load(Ordering::SeqCst), 1);
    md.shutdown(ShutdownMode::Immediate).await;
}

fn ten() -> brokapi_core::Decimal {
    brokapi_core::Decimal::from(LOT)
}
fn zero() -> brokapi_core::Decimal {
    brokapi_core::Decimal::ZERO
}

#[tokio::test]
async fn the_trading_params_are_a_lot_in_units() {
    let server = MockServer::start(Arc::default()).await;
    let (md, _rx) = md(&server).await;
    let p = md.trading_params(&sber()).await.unwrap();
    assert_eq!(p.lot_size, Qty::from(LOT));
    assert_eq!(p.min_qty, Qty::from(LOT));
    md.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn a_foreign_id_is_a_config_error_for_reads_and_subscriptions_too() {
    let server = MockServer::start(Arc::default()).await;
    let (md, _rx) = md(&server).await;
    let foreign = InstrumentId::new(
        InstrumentKey::new("SBER", "MISX"),
        NativeId::SymbolMic {
            symbol: "SBER".into(),
            mic: "MISX".into(),
        },
    );
    assert!(matches!(
        md.order_book(&foreign, 5).await,
        Err(Error::Config(_))
    ));
    assert!(matches!(
        md.subscribe_order_book(&foreign, SubscribeOpts::conflate())
            .await,
        Err(Error::Config(_))
    ));
    assert!(matches!(md.get(&foreign).await, Err(Error::Config(_))));
    assert!(matches!(
        md.trading_status(&foreign).await,
        Err(Error::Config(_))
    ));
    md.shutdown(ShutdownMode::Immediate).await;
}

// ─── when the lot size cannot be read ────────────────────────────────────────

#[tokio::test]
async fn an_order_event_whose_lot_is_unknown_is_a_loss_the_bot_is_told_about() {
    let opts = Arc::new(MockOptions::default());
    opts.omit_lot_size.store(true, Ordering::SeqCst);
    let server = MockServer::start(Arc::clone(&opts)).await;
    let (watcher, mut rx) = ud(&server).await;
    watcher.reconcile(&[]).await.unwrap();
    let sub = watcher.subscribe_order_state().await.unwrap();

    // Someone else places an order. The stream message has no lot size and the directory is down: the
    // connector cannot turn the lots into units, so the event is lost, and the bot is told as for an overflow.
    opts.instrument_by_fail.store(true, Ordering::SeqCst);
    server.place_foreign(
        ACCOUNT_A,
        "6f1c4f6e-0a54-4a43-8c46-0f6d3b2c8a11",
        "uid-sber",
        3,
    );
    let env = next_event(&mut rx, |e| matches!(e, Event::Overflow { .. })).await;
    assert!(matches!(env.payload, Event::Overflow { sub: s, .. } if s == sub));
    let err = watcher.place(buy(10)).await.unwrap_err();
    assert!(matches!(err, Error::Degraded), "{err:?}");

    // The directory is back: the reconcile reads the order (and its lot) and opens the gate.
    opts.instrument_by_fail.store(false, Ordering::SeqCst);
    let report = watcher.reconcile(&[]).await.unwrap();
    assert_eq!(report.foreign.len(), 1);
    assert_eq!(report.foreign[0].qty, Qty::from(3 * LOT));
    watcher.place(buy(10)).await.unwrap();

    // With the lot known now, a later foreign order comes through the stream in units.
    server.place_foreign(
        ACCOUNT_A,
        "7f1c4f6e-0a54-4a43-8c46-0f6d3b2c8a12",
        "uid-sber",
        2,
    );
    let env = next_event(
        &mut rx,
        |e| matches!(e, Event::Order(o) if o.qty == Qty::from(2 * LOT)),
    )
    .await;
    assert!(matches!(env.payload, Event::Order(_)));
    watcher.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn orders_read_again_after_a_cut_arrive_in_units_even_when_their_lot_was_never_read() {
    let opts = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&opts)).await;
    // Someone else's order is already at the broker when this connector starts.
    server.place_foreign(
        ACCOUNT_A,
        "6f1c4f6e-0a54-4a43-8c46-0f6d3b2c8a11",
        "uid-sber",
        3,
    );
    let (ud, mut rx) = ud(&server).await;
    ud.subscribe_order_state().await.unwrap();
    let before = opts.user_stream_opens.load(Ordering::SeqCst);
    server.cut_user_streams();
    // The connector reopens the stream and reads the live orders again: the foreign order comes with its lot
    // read from the directory, and in units.
    brokapi_mocks::time::jump_until("the stream is back", Duration::from_millis(600), || {
        opts.user_stream_opens.load(Ordering::SeqCst) > before
    })
    .await;
    let env = next_event(&mut rx, |e| matches!(e, Event::Order(_))).await;
    let Event::Order(o) = env.payload else {
        unreachable!()
    };
    assert_eq!(o.qty, Qty::from(3 * LOT));
    assert_eq!(opts.instrument_by_calls.load(Ordering::SeqCst), 1);
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn orders_read_again_after_a_cut_are_a_loss_when_their_lot_cannot_be_read() {
    let opts = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&opts)).await;
    server.place_foreign(
        ACCOUNT_A,
        "6f1c4f6e-0a54-4a43-8c46-0f6d3b2c8a11",
        "uid-sber",
        3,
    );
    let (ud, mut rx) = ud(&server).await;
    let sub = ud.subscribe_order_state().await.unwrap();
    opts.instrument_by_fail.store(true, Ordering::SeqCst);
    let before = opts.user_stream_opens.load(Ordering::SeqCst);
    server.cut_user_streams();
    brokapi_mocks::time::jump_until("the stream is back", Duration::from_millis(600), || {
        opts.user_stream_opens.load(Ordering::SeqCst) > before
    })
    .await;
    // The refresh cannot turn lots into units: the bot is told that events were lost.
    let env = next_event(&mut rx, |e| matches!(e, Event::Overflow { .. })).await;
    assert!(matches!(env.payload, Event::Overflow { sub: s, .. } if s == sub));
    opts.instrument_by_fail.store(false, Ordering::SeqCst);
    ud.reconcile(&[]).await.unwrap();
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn a_directory_answer_with_no_instrument_is_not_found() {
    let opts = Arc::new(MockOptions::default());
    opts.instrument_by_empty.store(true, Ordering::SeqCst);
    let server = MockServer::start(Arc::clone(&opts)).await;
    let (ud, _rx) = ud(&server).await;
    ud.reconcile(&[]).await.unwrap();
    assert!(ud.place(buy(10)).await.unwrap_err().is_not_found());
    let (md, _rx) = md(&server).await;
    assert!(md.get(&sber()).await.unwrap_err().is_not_found());
    assert!(md.order_book(&sber(), 5).await.unwrap_err().is_not_found());
    ud.shutdown(ShutdownMode::Immediate).await;
    md.shutdown(ShutdownMode::Immediate).await;
}
