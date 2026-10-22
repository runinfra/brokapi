//! Shared scenarios from `brokapi-conformance` against the ALOR mock (no external network).

mod mock_server;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use brokapi_alor::limits::default_limits;
use brokapi_alor::util::instrument_id;
use brokapi_alor::{MarketDataConfig, MarketDataConnector, UserDataConfig, UserDataConnector};
use brokapi_alor_protocol::config::AlorConfig;
use brokapi_conformance as conf;
use brokapi_core::{
    Decimal, Envelope, Error, InstrumentId, InstrumentKey, InstrumentQuery, InstrumentSource,
    NativeId, Price, ShutdownMode, TimeRange, Timeframe, UnixNanos, dec,
};
use brokapi_runtime::BrokerLimiter;
use mock_server::{MockOptions, MockServer};
use tokio::sync::mpsc;

fn sber() -> InstrumentId {
    instrument_id("SBER", "TQBR")
}

fn limiter() -> Arc<BrokerLimiter> {
    // The scenarios run a dozen calls in a fraction of a second against an imitation. They test the connector,
    // not the budget (the budget has tests of its own), so the buckets get room for a burst.
    let mut limits = default_limits();
    for bucket in limits.buckets.values_mut() {
        bucket.burst = bucket.burst.max(1000);
    }
    BrokerLimiter::new(&limits)
}

async fn md(server: &MockServer) -> (MarketDataConnector, mpsc::Receiver<Envelope>) {
    let (tx, rx) = mpsc::channel(256);
    let cfg = AlorConfig::for_mock("mock-refresh-token", &server.http_base);
    let md = MarketDataConnector::start(MarketDataConfig::new(cfg, "alor-md"), limiter(), tx)
        .await
        .expect("md start");
    (md, rx)
}

async fn ud(server: &MockServer) -> (UserDataConnector, mpsc::Receiver<Envelope>) {
    let (tx, rx) = mpsc::channel(256);
    let cfg = AlorConfig::for_mock("mock-refresh-token", &server.http_base);
    let ud = UserDataConnector::start(UserDataConfig::new(cfg, "alor-ud"), limiter(), tx)
        .await
        .expect("ud start");
    (ud, rx)
}

#[tokio::test]
async fn market_data() {
    let server = MockServer::start(Arc::new(MockOptions::default())).await;
    let (md, mut rx) = md(&server).await;
    conf::order_book_snapshot(&md, &sber(), 20).await;
    let sub = conf::order_book_stream(&md, &mut rx, "alor-md", &sber()).await;
    conf::unsubscribe(&md, sub).await;
    conf::unsubscribe_unknown(&md).await;
    md.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn instruments() {
    let server = MockServer::start(Arc::new(MockOptions::default())).await;
    let (md, _rx) = md(&server).await;
    let query = InstrumentQuery::ByTicker {
        ticker: "SBER".into(),
        exchange: None,
    };
    conf::instrument_lookup(&md, query, &sber()).await;
    md.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn orders() {
    let server = MockServer::start(Arc::new(MockOptions::default())).await;
    let (ud, _rx) = ud(&server).await;
    conf::reconcile(&ud).await;
    conf::place_and_cancel(&ud, &sber(), Price::new(dec!(1.0))).await;
    conf::order_state_unknown_cid(&ud).await;
    conf::rejected_order_does_not_block_the_instrument(&ud, &sber(), Price::new(dec!(1.0))).await;
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn stop_orders() {
    let server = MockServer::start(Arc::new(MockOptions::default())).await;
    let (ud, _rx) = ud(&server).await;
    assert!(ud.caps().supports_stop_orders);
    // Commands wait for the first reconcile (the command gate).
    conf::reconcile(&ud).await;
    conf::stop_order_far_from_market(&ud, &sber(), Price::new(dec!(200.0))).await;
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn portfolio() {
    let server = MockServer::start(Arc::new(MockOptions::default())).await;
    let (ud, mut rx) = ud(&server).await;
    conf::portfolio(&ud, &mut rx, "alor-ud").await;
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
    // ALOR has no timetable of sessions: `schedule` is Unsupported (checked below), the rest is the scenario.
    conf::market_data_history_without_schedule(&md, &sber(), Timeframe::M1, day()).await;
    match md.schedule("MOEX", day()).await {
        Err(Error::Unsupported { capability }) => assert_eq!(capability, "schedule"),
        other => panic!("{other:?}"),
    }
    conf::market_data_streams(&md, &mut rx, "alor-md", &sber(), Timeframe::M1).await;
    md.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn own_orders_events_active_replace_and_executions() {
    let opts = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&opts)).await;
    let (ud, mut rx) = ud(&server).await;
    conf::reconcile(&ud).await;
    conf::order_state_events(&ud, &mut rx, "alor-ud", &sber(), Price::new(dec!(100.0))).await;
    conf::active_orders_lifecycle(&ud, &sber(), Price::new(dec!(100.0))).await;
    assert!(ud.caps().supports_replace);
    conf::replace_order(&ud, true, &sber(), Price::from(100), Price::from(101)).await;
    conf::executions(
        &ud,
        &mut rx,
        "alor-ud",
        &sber(),
        Price::new(dec!(100.0)),
        |cid| {
            let fill = server.fill(
                &cid.hyphenated().to_string(),
                brokapi_core::Qty::from(20),
                Price::from(100),
            );
            async move {
                let _ = fill;
            }
        },
    )
    .await;
    ud.shutdown(ShutdownMode::Immediate).await;
}

/// The broker is slow to confirm a subscription: `subscribe_*` waits for it, and a subscription that is not
/// confirmed is an error with nothing left behind.
#[tokio::test]
async fn subscribe_returns_when_the_broker_has_confirmed() {
    let opts = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&opts)).await;
    let (ud, mut rx) = ud(&server).await;
    conf::reconcile(&ud).await;
    opts.stream_gate.hold();
    conf::subscribe_waits_for_the_stream(
        &ud,
        &mut rx,
        "alor-ud",
        &sber(),
        Price::new(dec!(100.0)),
        |call| {
            brokapi_mocks::time::elapse_when(
                "the subscribe reached the broker",
                || opts.stream_gate.waiting() > 0,
                Duration::from_secs(10),
                call,
            )
        },
        || async { opts.stream_gate.release() },
        |cid| {
            let fill = server.fill(
                &cid.hyphenated().to_string(),
                brokapi_core::Qty::from(20),
                Price::from(100),
            );
            async move {
                let _ = fill;
            }
        },
    )
    .await;
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn market_subscribe_returns_when_the_broker_has_confirmed() {
    let opts = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&opts)).await;
    let (md, mut rx) = md(&server).await;
    opts.stream_gate.hold();
    let id = sber();
    conf::market_subscribe_waits_for_the_stream(
        &md,
        &mut rx,
        &id,
        |call| {
            brokapi_mocks::time::elapse_when(
                "the subscribe reached the broker",
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

/// A connector whose lot is known to the grid, on a mock with the given options.
async fn ud_with(
    opts: &Arc<MockOptions>,
    request_timeout: Option<Duration>,
) -> (MockServer, UserDataConnector, mpsc::Receiver<Envelope>) {
    let server = MockServer::start(Arc::clone(opts)).await;
    let (tx, rx) = mpsc::channel(256);
    let mut alor = AlorConfig::for_mock("mock-refresh-token", &server.http_base);
    if let Some(t) = request_timeout {
        alor.request_timeout = t;
    }
    let ud = UserDataConnector::start(UserDataConfig::new(alor, "alor-ud"), limiter(), tx)
        .await
        .expect("ud start");
    ud.price_grid()
        .set_lot(sber().key, brokapi_core::Qty::from(conf::LOT));
    (server, ud, rx)
}

#[tokio::test]
async fn a_cid_is_placed_once() {
    let (_server, ud, _rx) = ud_with(&Arc::default(), None).await;
    conf::reconcile(&ud).await;
    conf::duplicate_place(&ud, &sber(), Price::new(dec!(100.0))).await;
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn a_quantity_the_broker_cannot_take_is_refused_not_rounded() {
    let opts = Arc::new(MockOptions::default());
    let (_server, ud, _rx) = ud_with(&opts, None).await;
    conf::reconcile(&ud).await;
    conf::invalid_qty(&ud, &sber(), Price::new(dec!(100.0))).await;
    // Only the one good order reached the broker (its place and its cancel): the refusals sent nothing.
    assert_eq!(opts.order_commands.load(Ordering::SeqCst), 2);
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn a_fraction_is_refused_even_when_the_lot_is_not_known() {
    let (_server, ud, _rx) = ud_with(&Arc::default(), None).await;
    ud.price_grid().set_lot(sber().key, brokapi_core::Qty::ZERO);
    conf::reconcile(&ud).await;
    let mut req = brokapi_core::PlaceOrderRequest {
        cid: brokapi_core::ClientOrderId::new_v4(),
        instrument: sber(),
        side: brokapi_core::Side::Buy,
        order_type: brokapi_core::OrderType::Limit,
        qty: "0.4".parse().unwrap(),
        price: Some(Price::new(dec!(100.0))),
    };
    use brokapi_core::OrderExecutor;
    // 0.4 and 2.5 pieces are not a whole lot (the lot is read from the broker): refused, never rounded.
    for q in ["0.4", "2.5"] {
        req.qty = q.parse().unwrap();
        assert!(
            matches!(ud.place(req.clone()).await, Err(Error::Rejected { ref code, .. }) if code == "invalid_qty"),
            "{q}"
        );
    }
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn an_id_of_another_broker_is_a_config_error() {
    let (_server, ud, _rx) = ud_with(&Arc::default(), None).await;
    conf::reconcile(&ud).await;
    let foreign = InstrumentId::new(
        InstrumentKey::new("SBER", "MISX"),
        NativeId::SymbolMic {
            symbol: "SBER".into(),
            mic: "MISX".into(),
        },
    );
    conf::foreign_instrument_id(&ud, &foreign, Price::new(dec!(100.0))).await;
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn an_unknown_instrument_is_not_found() {
    let server = MockServer::start(Arc::new(MockOptions::default())).await;
    let (md, _rx) = md(&server).await;
    conf::unknown_instrument(
        &md,
        &instrument_id(brokapi_mocks::alor::UNKNOWN_TICKER, "TQBR"),
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
        &sber(),
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
        &sber(),
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
        &sber(),
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
    conf::order_book_resync(&md, &mut rx, &sber(), || {
        opts.disconnect_after_data_frames.store(1, Ordering::SeqCst);
    })
    .await;
    md.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn the_history_of_trades_is_read_to_the_last_page() {
    let opts = Arc::new(MockOptions::default());
    let (server, ud, _rx) = ud_with(&opts, None).await;
    server.add_history_trades(250, Price::new(dec!(100.0)), brokapi_core::Qty::from(10));
    conf::executions_history_is_complete(&ud, 250).await;
    // One answer holds the whole history: 250 is under the 1000 of a page, so there is one question.
    let asked = opts.history_asked.lock().unwrap().clone();
    assert_eq!(asked.len(), 1);
    assert_eq!(asked[0].1.as_deref(), Some("1000"));
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn a_history_that_fills_a_page_is_an_error_not_a_silent_cut() {
    let (server, ud, _rx) = ud_with(&Arc::default(), None).await;
    server.add_history_trades(1200, Price::new(dec!(100.0)), brokapi_core::Qty::from(10));
    use brokapi_core::ExecutionSource;
    let range = TimeRange::new(UnixNanos::ZERO, UnixNanos::from_nanos(i64::MAX / 2));
    match ud.executions(range).await {
        Err(Error::Rejected { code, .. }) => assert_eq!(code, "range_too_long"),
        other => panic!("{other:?}"),
    }
    ud.shutdown(ShutdownMode::Immediate).await;
}
