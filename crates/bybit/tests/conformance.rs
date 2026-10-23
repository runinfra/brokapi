//! Shared scenarios from `brokapi-conformance` against the Bybit mock (no external network).

mod mock_server;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use brokapi_bybit::limits::default_limits;
use brokapi_bybit::util::instrument_id;
use brokapi_bybit::{MarketDataConfig, MarketDataConnector, UserDataConfig, UserDataConnector};
use brokapi_bybit_protocol::config::BybitConfig;
use brokapi_conformance as conf;
use brokapi_core::{
    Decimal, Envelope, Error, InstrumentId, InstrumentKey, InstrumentQuery, InstrumentSource,
    NativeId, Price, Qty, ShutdownMode, TimeRange, Timeframe, UnixNanos, dec,
};
use brokapi_runtime::BrokerLimiter;
use mock_server::{DEFAULT_KEY, MockOptions, MockServer, SECRET};
use tokio::sync::mpsc;

fn mock() -> InstrumentId {
    instrument_id("MOCKUSDT", "linear")
}

fn limiter() -> Arc<BrokerLimiter> {
    // The scenarios run a dozen calls in a fraction of a second against an imitation. They test the connector, not the
    // budget (the budget has tests of its own), so the buckets get room for a burst.
    let mut limits = default_limits();
    for bucket in limits.buckets.values_mut() {
        bucket.burst = bucket.burst.max(1000);
    }
    BrokerLimiter::new(&limits)
}

fn config(server: &MockServer) -> BybitConfig {
    BybitConfig::for_mock(DEFAULT_KEY, SECRET, &server.http_base)
}

async fn md(server: &MockServer) -> (MarketDataConnector, mpsc::Receiver<Envelope>) {
    let (tx, rx) = mpsc::channel(256);
    let md = MarketDataConnector::start(
        MarketDataConfig::new(config(server), "bybit-md"),
        limiter(),
        tx,
    )
    .await
    .expect("md start");
    (md, rx)
}

async fn ud(server: &MockServer) -> (UserDataConnector, mpsc::Receiver<Envelope>) {
    let (tx, rx) = mpsc::channel(256);
    let ud = UserDataConnector::start(
        UserDataConfig::new(config(server), "bybit-ud"),
        limiter(),
        tx,
    )
    .await
    .expect("ud start");
    (ud, rx)
}

fn fill(
    server: &MockServer,
    cid: brokapi_core::ClientOrderId,
) -> impl std::future::Future<Output = ()> + use<> {
    let exec = server.fill(
        &cid.hyphenated().to_string(),
        Qty::from(20),
        Price::from(100),
    );
    async move {
        let _ = exec;
    }
}

#[tokio::test]
async fn market_data() {
    let server = MockServer::start(Arc::new(MockOptions::default())).await;
    let (md, mut rx) = md(&server).await;
    conf::order_book_snapshot(&md, &mock(), 20).await;
    let sub = conf::order_book_stream(&md, &mut rx, "bybit-md", &mock()).await;
    conf::unsubscribe(&md, sub).await;
    conf::unsubscribe_unknown(&md).await;
    md.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn instruments() {
    let server = MockServer::start(Arc::new(MockOptions::default())).await;
    let (md, _rx) = md(&server).await;
    let query = InstrumentQuery::ByTicker {
        ticker: "MOCKUSDT".into(),
        exchange: None,
    };
    conf::instrument_lookup(&md, query, &mock()).await;
    md.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn orders() {
    let server = MockServer::start(Arc::new(MockOptions::default())).await;
    let (ud, _rx) = ud(&server).await;
    conf::reconcile(&ud).await;
    conf::place_and_cancel(&ud, &mock(), Price::new(dec!(1.0))).await;
    conf::order_state_unknown_cid(&ud).await;
    conf::rejected_order_does_not_block_the_instrument(&ud, &mock(), Price::new(dec!(1.0))).await;
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn stop_orders() {
    let server = MockServer::start(Arc::new(MockOptions::default())).await;
    let (ud, _rx) = ud(&server).await;
    assert!(ud.caps().supports_stop_orders);
    // Commands wait for the first reconcile (the command gate).
    conf::reconcile(&ud).await;
    conf::stop_order_far_from_market(&ud, &mock(), Price::new(dec!(200.0))).await;
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn a_stop_order_without_an_answer_keeps_its_cid() {
    let opts = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&opts)).await;
    let (ud, _rx) = ud(&server).await;
    conf::reconcile(&ud).await;
    conf::stop_without_answer_keeps_its_cid(&ud, &mock(), Price::new(dec!(200.0)), |lose| {
        opts.stop_answer_lost.store(lose, Ordering::SeqCst)
    })
    .await;
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn portfolio() {
    let server = MockServer::start(Arc::new(MockOptions::default())).await;
    let (ud, mut rx) = ud(&server).await;
    conf::portfolio(&ud, &mut rx, "bybit-ud").await;
    ud.shutdown(ShutdownMode::Immediate).await;
}

fn day() -> TimeRange {
    // 2026-10-19 00:00 UTC .. 2026-10-20 00:00 UTC
    TimeRange::new(
        UnixNanos::from_nanos(1_792_368_000_000_000_000),
        UnixNanos::from_nanos(1_792_454_400_000_000_000),
    )
}

#[tokio::test]
async fn market_data_history_and_streams() {
    let server = MockServer::start(Arc::new(MockOptions::default())).await;
    let (md, mut rx) = md(&server).await;
    // A crypto exchange has no timetable of sessions: `schedule` is Unsupported (checked below), the rest is the
    // scenario. A day of 5-minute bars is 288, under the 1000 of one request.
    conf::market_data_history_without_schedule(&md, &mock(), Timeframe::M5, day()).await;
    match md.schedule("BYBIT", day()).await {
        Err(Error::Unsupported { capability }) => assert_eq!(capability, "schedule"),
        other => panic!("{other:?}"),
    }
    conf::market_data_streams(&md, &mut rx, "bybit-md", &mock(), Timeframe::M1).await;
    md.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn own_orders_events_active_and_executions() {
    let opts = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&opts)).await;
    let (ud, mut rx) = ud(&server).await;
    conf::reconcile(&ud).await;
    conf::order_state_events(&ud, &mut rx, "bybit-ud", &mock(), Price::new(dec!(100.0))).await;
    conf::active_orders_lifecycle(&ud, &mock(), Price::new(dec!(100.0))).await;
    // The amendment of the exchange keeps the order and its cid, the shared replace promises a new order: it is not
    // offered, and the scenario checks that the refusal leaves the order alone.
    assert!(!ud.caps().supports_replace);
    conf::replace_order(&ud, false, &mock(), Price::from(100), Price::from(101)).await;
    conf::executions(
        &ud,
        &mut rx,
        "bybit-ud",
        &mock(),
        Price::new(dec!(100.0)),
        |cid| fill(&server, cid),
    )
    .await;
    ud.shutdown(ShutdownMode::Immediate).await;
}

/// The exchange is slow to confirm a subscription: `subscribe_*` waits for it, and a subscription that is not
/// confirmed is an error with nothing left behind.
#[tokio::test]
async fn subscribe_returns_when_the_exchange_has_confirmed() {
    let opts = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&opts)).await;
    let (ud, mut rx) = ud(&server).await;
    conf::reconcile(&ud).await;
    opts.stream_gate.hold();
    conf::subscribe_waits_for_the_stream(
        &ud,
        &mut rx,
        "bybit-ud",
        &mock(),
        Price::new(dec!(100.0)),
        |call| {
            brokapi_mocks::time::elapse_when(
                "the subscribe reached the exchange",
                || opts.stream_gate.waiting() > 0,
                Duration::from_secs(10),
                call,
            )
        },
        || async { opts.stream_gate.release() },
        |cid| fill(&server, cid),
    )
    .await;
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn market_subscribe_returns_when_the_exchange_has_confirmed() {
    let opts = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&opts)).await;
    let (md, mut rx) = md(&server).await;
    opts.stream_gate.hold();
    let id = mock();
    conf::market_subscribe_waits_for_the_stream(
        &md,
        &mut rx,
        &id,
        |call| {
            brokapi_mocks::time::elapse_when(
                "the subscribe reached the exchange",
                || opts.stream_gate.waiting() > 0,
                Duration::from_secs(10),
                call,
            )
        },
        || async { opts.stream_gate.release() },
    )
    .await;
    md.shutdown(ShutdownMode::Immediate).await;
}

// ─── guarantees of the command path ───────────────────────────────────────────

/// A connector on a mock with the given options.
async fn ud_with(
    opts: &Arc<MockOptions>,
    request_timeout: Option<Duration>,
) -> (MockServer, UserDataConnector, mpsc::Receiver<Envelope>) {
    let server = MockServer::start(Arc::clone(opts)).await;
    let (tx, rx) = mpsc::channel(256);
    let mut cfg = config(&server);
    if let Some(t) = request_timeout {
        cfg.request_timeout = t;
    }
    let ud = UserDataConnector::start(UserDataConfig::new(cfg, "bybit-ud"), limiter(), tx)
        .await
        .expect("ud start");
    (server, ud, rx)
}

#[tokio::test]
async fn a_cid_is_placed_once() {
    let (_server, ud, _rx) = ud_with(&Arc::default(), None).await;
    conf::reconcile(&ud).await;
    conf::duplicate_place(&ud, &mock(), Price::new(dec!(100.0))).await;
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn a_quantity_the_exchange_cannot_take_is_refused_not_rounded() {
    let opts = Arc::new(MockOptions::default());
    let (_server, ud, _rx) = ud_with(&opts, None).await;
    conf::reconcile(&ud).await;
    conf::invalid_qty(&ud, &mock(), Price::new(dec!(100.0))).await;
    // Only the one good order reached the exchange (its place and its cancel): the refusals sent nothing.
    assert_eq!(opts.order_commands.load(Ordering::SeqCst), 2);
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn the_steps_of_the_symbol_are_read_from_the_exchange_and_checked_before_anything_is_sent() {
    let opts = Arc::new(MockOptions::default());
    let (_server, ud, _rx) = ud_with(&opts, None).await;
    conf::reconcile(&ud).await;
    use brokapi_core::{ClientOrderId, OrderExecutor, OrderType, PlaceOrderRequest, Side};
    let mut req = PlaceOrderRequest {
        cid: ClientOrderId::new_v4(),
        instrument: mock(),
        side: Side::Buy,
        order_type: OrderType::Limit,
        qty: Qty::from(10),
        price: Some(Price::new(dec!(100.005))),
    };
    // 100.005 is not a multiple of the tick 0.01.
    assert!(
        matches!(ud.place(req.clone()).await, Err(Error::Rejected { ref code, .. }) if code == "price_off_step")
    );
    // 5 is below the smallest order (10) and off the step.
    req.price = Some(Price::new(dec!(100.00)));
    req.qty = Qty::from(5);
    assert!(
        matches!(ud.place(req.clone()).await, Err(Error::Rejected { ref code, .. }) if code == "invalid_qty")
    );
    // A limit order with no price.
    req.qty = Qty::from(10);
    req.price = None;
    assert!(
        matches!(ud.place(req.clone()).await, Err(Error::Rejected { ref code, .. }) if code == "missing_price")
    );
    assert_eq!(
        opts.order_commands.load(Ordering::SeqCst),
        0,
        "nothing reached the exchange"
    );
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn an_id_of_another_broker_or_category_is_a_config_error() {
    let (_server, ud, _rx) = ud_with(&Arc::default(), None).await;
    conf::reconcile(&ud).await;
    let foreign = InstrumentId::new(
        InstrumentKey::new("SBER", "TQBR"),
        NativeId::TickerClass {
            ticker: "SBER".into(),
            class_code: "TQBR".into(),
        },
    );
    conf::foreign_instrument_id(&ud, &foreign, Price::new(dec!(100.0))).await;
    let spot = instrument_id("MOCKUSDT", "spot");
    conf::foreign_instrument_id(&ud, &spot, Price::new(dec!(100.0))).await;
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn an_unknown_instrument_is_not_found() {
    let server = MockServer::start(Arc::new(MockOptions::default())).await;
    let (md, _rx) = md(&server).await;
    conf::unknown_instrument(
        &md,
        &instrument_id(brokapi_mocks::bybit::UNKNOWN_SYMBOL, "linear"),
    )
    .await;
    md.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn a_place_without_an_answer_is_unknown() {
    let opts = Arc::new(MockOptions::default());
    let (_server, ud, _rx) = ud_with(&opts, Some(Duration::from_millis(150))).await;
    conf::reconcile(&ud).await;
    opts.order_delay_ms.store(600, Ordering::SeqCst);
    conf::place_without_answer_is_unknown(
        &ud,
        &mock(),
        Price::new(dec!(100.0)),
        |call| {
            brokapi_mocks::time::elapse_when(
                "the order reached the mock",
                || opts.order_commands.load(Ordering::SeqCst) == 1,
                Duration::from_millis(200),
                call,
            )
        },
        || async {
            // The delay of the mock ends: it took the order after all.
            brokapi_mocks::time::jump(Duration::from_millis(500)).await;
            opts.order_delay_ms.store(0, Ordering::SeqCst);
        },
    )
    .await;
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn a_cut_of_the_order_events_degrades_until_reconcile() {
    let opts = Arc::new(MockOptions::default());
    let (server, ud, mut rx) = ud_with(&opts, None).await;
    conf::reconcile(&ud).await;
    let before = opts.ws_connections();
    conf::stream_cut_degrades_until_reconcile(
        &ud,
        &mut rx,
        &mock(),
        Price::new(dec!(100.0)),
        || async {
            server.cut_event_sockets();
            // The connector pauses before it reopens the socket: move the clock until it has.
            brokapi_mocks::time::jump_until("the socket is back", Duration::from_secs(1), || {
                opts.ws_connections() > before
            })
            .await;
        },
    )
    .await;
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn the_gate_is_checked_again_after_the_queue() {
    let opts = Arc::new(MockOptions::default());
    let (server, ud, mut rx) = ud_with(&opts, Some(Duration::from_secs(120))).await;
    conf::reconcile(&ud).await;
    // The first order is held for 20 s of the test clock: longer than the jumps that reopen the socket.
    opts.order_delay_ms.store(20_000, Ordering::SeqCst);
    let before = opts.ws_connections();
    conf::gate_checked_after_the_queue(
        &ud,
        &mut rx,
        &mock(),
        Price::new(dec!(100.0)),
        || async {
            brokapi_mocks::time::wait_until("the first order is at the mock", || {
                opts.order_commands.load(Ordering::SeqCst) == 1
            })
            .await;
        },
        || async {
            server.cut_event_sockets();
            brokapi_mocks::time::jump_until(
                "the socket is back",
                Duration::from_millis(600),
                || opts.ws_connections() > before,
            )
            .await;
        },
        || async {
            opts.order_delay_ms.store(0, Ordering::SeqCst);
            brokapi_mocks::time::jump(Duration::from_secs(21)).await;
        },
    )
    .await;
    // Only the first order reached the mock: its place and its cancel. The queued one never went out.
    assert_eq!(opts.order_commands.load(Ordering::SeqCst), 2);
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn the_order_book_is_resynced_after_a_cut() {
    let opts = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&opts)).await;
    let (md, mut rx) = md(&server).await;
    conf::order_book_resync(&md, &mut rx, &mock(), || {
        opts.disconnect_after_data_frames.store(1, Ordering::SeqCst);
    })
    .await;
    md.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn the_fills_of_a_long_range_are_read_in_windows_of_seven_days() {
    let opts = Arc::new(MockOptions::default());
    let (server, ud, _rx) = ud_with(&opts, None).await;
    // 30 fills a month ago, 250 fills now (more than the 100 of a page).
    server.add_history_trades(30, Price::new(dec!(100.0)), Qty::from(10), 30);
    server.add_history_trades(250, Price::new(dec!(100.0)), Qty::from(10), 0);
    conf::executions_history_is_complete(&ud, 280).await;
    let asked = opts.execution_requests.lock().unwrap().clone();
    assert!(
        asked.len() > 100,
        "the two years of the range are read in windows: {}",
        asked.len()
    );
    assert!(
        asked
            .iter()
            .all(|(s, e)| e.unwrap() - s.unwrap() <= 7 * 86_400_000),
        "no window is longer than 7 days"
    );
    ud.shutdown(ShutdownMode::Immediate).await;
}
