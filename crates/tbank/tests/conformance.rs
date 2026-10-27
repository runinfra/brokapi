//! Shared scenarios from `brokapi-conformance` against the gRPC mock (no external network).

mod mock_server;

use brokapi_core::Price;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use brokapi_conformance as conf;
use brokapi_core::{
    Envelope, InstrumentId, InstrumentKey, InstrumentQuery, NativeId, ShutdownMode, TimeRange,
    Timeframe, UnixNanos,
};
use brokapi_runtime::BrokerLimiter;
use brokapi_tbank::limits::default_limits;
use brokapi_tbank::{MarketDataConfig, MarketDataConnector, UserDataConfig, UserDataConnector};
use brokapi_tbank_protocol::TbankConfig;
use mock_server::{ACCOUNT_A, MockOptions, MockServer, TOKEN};
use tokio::sync::mpsc;

fn sber() -> InstrumentId {
    InstrumentId::new(
        InstrumentKey::new("SBER", "TQBR"),
        NativeId::Uid("uid-sber".into()),
    )
}

fn limiter() -> Arc<BrokerLimiter> {
    BrokerLimiter::new(&default_limits())
}

async fn md(server: &MockServer) -> (MarketDataConnector, mpsc::Receiver<Envelope>) {
    let (tx, rx) = mpsc::channel(256);
    let cfg = TbankConfig::for_mock(TOKEN, &server.endpoint);
    let md = MarketDataConnector::start(MarketDataConfig::new(cfg, "tbank-md"), limiter(), tx)
        .await
        .expect("md start");
    (md, rx)
}

async fn ud(server: &MockServer) -> (UserDataConnector, mpsc::Receiver<Envelope>) {
    let (tx, rx) = mpsc::channel(256);
    let cfg = TbankConfig::for_mock(TOKEN, &server.endpoint).with_account(ACCOUNT_A);
    let ud = UserDataConnector::start(UserDataConfig::new(cfg, "tbank-ud"), limiter(), tx)
        .await
        .expect("ud start");
    (ud, rx)
}

#[tokio::test]
async fn market_data() {
    let server = MockServer::start(Arc::new(MockOptions::default())).await;
    let (md, mut rx) = md(&server).await;
    conf::order_book_snapshot(&md, &sber(), 10).await;
    let sub = conf::order_book_stream(&md, &mut rx, "tbank-md", &sber()).await;
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
    conf::place_and_cancel(&ud, &sber(), Price::from(1)).await;
    conf::order_state_unknown_cid(&ud).await;
    conf::rejected_order_does_not_block_the_instrument(&ud, &sber(), Price::from(1)).await;
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn stop_orders() {
    let server = MockServer::start(Arc::new(MockOptions::default())).await;
    let (ud, _rx) = ud(&server).await;
    conf::reconcile(&ud).await;
    assert!(ud.caps().supports_stop_orders);
    conf::stop_order_far_from_market(&ud, &sber(), Price::from(1)).await;
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn a_stop_order_without_an_answer_keeps_its_cid() {
    let opts = Arc::new(MockOptions::default());
    // The stop order gets the time of the real clock: the connector finds it by when it was created.
    opts.stop_create_now.store(true, Ordering::SeqCst);
    let server = MockServer::start(Arc::clone(&opts)).await;
    let (ud, _rx) = ud(&server).await;
    conf::reconcile(&ud).await;
    conf::stop_without_answer_keeps_its_cid(&ud, &sber(), Price::from(1), |lose| {
        opts.stop_answer_lost.store(lose, Ordering::SeqCst)
    })
    .await;
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn a_place_that_ends_in_transport_uses_up_its_cid() {
    let opts = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&opts)).await;
    let (ud, _rx) = ud(&server).await;
    conf::reconcile(&ud).await;
    // The first order on SBER reads its lot size: that read fails.
    conf::transport_uses_up_the_cid(&ud, &sber(), Price::from(100), |broken| {
        opts.instrument_by_fail.store(broken, Ordering::SeqCst)
    })
    .await;
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn a_stop_order_that_ends_in_transport_uses_up_its_cid() {
    let opts = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&opts)).await;
    let (ud, _rx) = ud(&server).await;
    conf::reconcile(&ud).await;
    conf::stop_transport_uses_up_the_cid(&ud, &sber(), Price::from(1), |broken| {
        opts.instrument_by_fail.store(broken, Ordering::SeqCst)
    })
    .await;
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn portfolio() {
    let server = MockServer::start(Arc::new(MockOptions::default())).await;
    let (ud, mut rx) = ud(&server).await;
    conf::portfolio(&ud, &mut rx, "tbank-ud").await;
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn market_data_history_and_streams() {
    let server = MockServer::start(Arc::new(MockOptions::default())).await;
    let (md, mut rx) = md(&server).await;
    let candles = TimeRange::new(
        UnixNanos::from_nanos(1_700_000_000_000_000_000),
        UnixNanos::from_nanos(1_700_001_000_000_000_000),
    );
    let wide = TimeRange::new(UnixNanos::ZERO, UnixNanos::from_nanos(i64::MAX / 2));
    conf::market_data_history(&md, &sber(), Timeframe::M5, candles, "MOEX", wide).await;
    conf::market_data_streams(&md, &mut rx, "tbank-md", &sber(), Timeframe::M5).await;
    md.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn own_orders_events_active_replace_and_executions() {
    let server = MockServer::start(Arc::new(MockOptions::default())).await;
    let (ud, mut rx) = ud(&server).await;
    conf::reconcile(&ud).await;
    conf::order_state_events(&ud, &mut rx, "tbank-ud", &sber(), Price::from(1)).await;
    conf::active_orders_lifecycle(&ud, &sber(), Price::from(1)).await;
    assert!(ud.caps().supports_replace);
    conf::replace_order(
        &ud,
        true,
        &sber(),
        brokapi_core::Price::from(1),
        brokapi_core::Price::from(2),
    )
    .await;
    conf::executions(&ud, &mut rx, "tbank-ud", &sber(), Price::from(1), |cid| {
        server.fill(ACCOUNT_A, &cid.hyphenated().to_string());
        async {}
    })
    .await;
    ud.shutdown(ShutdownMode::Immediate).await;
}

/// The broker is slow to open a stream: `subscribe_*` waits for it, and a stream that does not open is an error
/// with nothing left behind.
#[tokio::test]
async fn subscribe_returns_when_the_stream_is_open() {
    let opts = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&opts)).await;
    let (ud, mut rx) = ud(&server).await;
    conf::reconcile(&ud).await;
    opts.stream_gate.hold();
    conf::subscribe_waits_for_the_stream(
        &ud,
        &mut rx,
        "tbank-ud",
        &sber(),
        Price::from(1),
        |call| {
            brokapi_mocks::time::elapse_when(
                "the stream request reached the broker",
                || opts.stream_gate.waiting() > 0,
                Duration::from_secs(120),
                call,
            )
        },
        || async { opts.stream_gate.release() },
        |cid| {
            server.fill(ACCOUNT_A, &cid.hyphenated().to_string());
            async {}
        },
    )
    .await;
    ud.shutdown(ShutdownMode::Immediate).await;
}

/// The market data stream of T-Bank answers each subscribe with a status: without it the call is an error.
#[tokio::test]
async fn market_subscribe_returns_when_the_broker_has_confirmed() {
    let opts = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&opts)).await;
    let (md, mut rx) = md(&server).await;
    opts.md_silent.store(true, Ordering::SeqCst);
    let id = sber();
    conf::market_subscribe_waits_for_the_stream(
        &md,
        &mut rx,
        &id,
        |call| {
            brokapi_mocks::time::elapse_when(
                "the subscribe reached the broker",
                || !opts.md_requests.lock().unwrap().is_empty(),
                Duration::from_secs(120),
                call,
            )
        },
        || async { opts.md_silent.store(false, Ordering::SeqCst) },
    )
    .await;
    md.shutdown(ShutdownMode::Immediate).await;
}

// ─── guarantees of the command path ───────────────────────────────────────────

async fn ud_timeout(
    server: &MockServer,
    request_timeout: Duration,
) -> (UserDataConnector, mpsc::Receiver<Envelope>) {
    let (tx, rx) = mpsc::channel(256);
    let cfg = TbankConfig::for_mock(TOKEN, &server.endpoint)
        .with_account(ACCOUNT_A)
        .with_request_timeout(request_timeout);
    let ud = UserDataConnector::start(UserDataConfig::new(cfg, "tbank-ud"), limiter(), tx)
        .await
        .expect("ud start");
    (ud, rx)
}

#[tokio::test]
async fn a_cid_is_placed_once() {
    let server = MockServer::start(Arc::new(MockOptions::default())).await;
    let (ud, _rx) = ud(&server).await;
    conf::reconcile(&ud).await;
    conf::duplicate_place(&ud, &sber(), Price::from(100)).await;
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn a_quantity_that_is_not_whole_lots_is_refused_not_rounded() {
    let opts = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&opts)).await;
    let (ud, _rx) = ud(&server).await;
    conf::reconcile(&ud).await;
    conf::invalid_qty(&ud, &sber(), Price::from(100)).await;
    // The four refusals sent nothing: one order reached the broker.
    assert_eq!(opts.post_calls.load(Ordering::SeqCst), 1);
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn an_id_of_another_broker_is_a_config_error_and_nothing_is_sent() {
    let opts = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&opts)).await;
    let (ud, _rx) = ud(&server).await;
    conf::reconcile(&ud).await;
    let foreign = InstrumentId::new(
        InstrumentKey::new("SBER", "MISX"),
        NativeId::SymbolMic {
            symbol: "SBER".into(),
            mic: "MISX".into(),
        },
    );
    conf::foreign_instrument_id(&ud, &foreign, Price::from(100)).await;
    assert_eq!(opts.post_calls.load(Ordering::SeqCst), 0);
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn an_unknown_instrument_is_not_found() {
    let server = MockServer::start(Arc::new(MockOptions::default())).await;
    let (md, _rx) = md(&server).await;
    let nope = InstrumentId::new(
        InstrumentKey::new("NOPE", "TQBR"),
        NativeId::Uid("no-such-uid".into()),
    );
    conf::unknown_instrument(&md, &nope).await;
    md.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn a_place_without_an_answer_is_unknown() {
    let opts = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&opts)).await;
    let (ud, _rx) = ud_timeout(&server, Duration::from_millis(150)).await;
    conf::reconcile(&ud).await;
    opts.post_delay_ms.store(600, Ordering::SeqCst);
    conf::place_without_answer_is_unknown(
        &ud,
        &sber(),
        Price::from(100),
        |call| {
            brokapi_mocks::time::elapse_when(
                "the order reached the mock",
                || opts.post_calls.load(Ordering::SeqCst) == 1,
                Duration::from_millis(200),
                call,
            )
        },
        || async {
            brokapi_mocks::time::jump(Duration::from_millis(500)).await;
        },
    )
    .await;
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn a_cut_of_the_order_streams_degrades_until_reconcile() {
    let opts = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&opts)).await;
    let (ud, mut rx) = ud(&server).await;
    conf::reconcile(&ud).await;
    conf::stream_cut_degrades_until_reconcile(&ud, &mut rx, &sber(), Price::from(100), || async {
        // Both streams are open (the subscribes waited for their confirmations): cut them.
        server.cut_user_streams();
        // The connector pauses before it reopens: move the clock until both are open again.
        brokapi_mocks::time::jump_until("the streams are back", Duration::from_secs(1), || {
            opts.user_stream_opens.load(Ordering::SeqCst) >= 4
        })
        .await;
    })
    .await;
    // The streams were opened again after the cut: two at the start, two more after it.
    assert_eq!(opts.user_stream_opens.load(Ordering::SeqCst), 4);
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn the_gate_is_checked_again_after_the_queue() {
    let opts = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&opts)).await;
    let (ud, mut rx) = ud_timeout(&server, Duration::from_secs(120)).await;
    conf::reconcile(&ud).await;
    // The first order is held for 20 s of the test clock: longer than the jumps that reopen the streams.
    opts.post_delay_ms.store(20_000, Ordering::SeqCst);
    conf::gate_checked_after_the_queue(
        &ud,
        &mut rx,
        &sber(),
        Price::from(100),
        || async {
            brokapi_mocks::time::wait_until("the first order is at the mock", || {
                opts.post_calls.load(Ordering::SeqCst) == 1
            })
            .await;
        },
        || async {
            let before = opts.user_stream_opens.load(Ordering::SeqCst);
            server.cut_user_streams();
            brokapi_mocks::time::jump_until(
                "the streams are back",
                Duration::from_millis(600),
                || opts.user_stream_opens.load(Ordering::SeqCst) > before,
            )
            .await;
        },
        || async {
            opts.post_delay_ms.store(0, Ordering::SeqCst);
            brokapi_mocks::time::jump(Duration::from_secs(21)).await;
        },
    )
    .await;
    // The queued place never went out.
    assert_eq!(opts.post_calls.load(Ordering::SeqCst), 1);
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn the_order_book_is_resynced_after_a_cut() {
    let opts = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&opts)).await;
    let (md, mut rx) = md(&server).await;
    conf::order_book_resync(&md, &mut rx, &sber(), || {
        opts.md_cut_after.store(1, Ordering::SeqCst);
    })
    .await;
    md.shutdown(ShutdownMode::Immediate).await;
}
