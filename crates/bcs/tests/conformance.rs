//! Shared scenarios from `brokapi-conformance` against the BCS mock (no external network).

mod mock_server;

use brokapi_core::Price;
use brokapi_core::{Decimal, dec};
use std::sync::Arc;

use brokapi_bcs::limits::default_limits;
use brokapi_bcs::{MarketDataConfig, MarketDataConnector, UserDataConfig, UserDataConnector};
use brokapi_bcs_protocol::config::BcsConfig;
use brokapi_conformance as conf;
use brokapi_core::{
    Envelope, InstrumentId, InstrumentKey, InstrumentQuery, NativeId, ShutdownMode, TimeRange,
    Timeframe, UnixNanos,
};
use brokapi_runtime::BrokerLimiter;
use mock_server::{MockOptions, MockServer};
use tokio::sync::mpsc;

fn sber() -> InstrumentId {
    InstrumentId::new(
        InstrumentKey::new("SBER", "TQBR"),
        NativeId::TickerClass {
            ticker: "SBER".into(),
            class_code: "TQBR".into(),
        },
    )
}

fn limiter() -> Arc<BrokerLimiter> {
    BrokerLimiter::new(&default_limits())
}

async fn md(server: &MockServer) -> (MarketDataConnector, mpsc::Receiver<Envelope>) {
    let (tx, rx) = mpsc::channel(256);
    let cfg = BcsConfig::for_mock("mock-refresh-token", &server.http_base).with_write(false);
    let md = MarketDataConnector::start(MarketDataConfig::new(cfg, "bcs-md"), limiter(), tx)
        .await
        .expect("md start");
    (md, rx)
}

async fn ud(server: &MockServer) -> (UserDataConnector, mpsc::Receiver<Envelope>) {
    let (tx, rx) = mpsc::channel(256);
    let cfg = BcsConfig::for_mock("mock-refresh-token", &server.http_base).with_write(true);
    let ud = UserDataConnector::start(UserDataConfig::new(cfg, "bcs-ud"), limiter(), tx)
        .await
        .expect("ud start");
    (ud, rx)
}

#[tokio::test]
async fn market_data() {
    let server = MockServer::start(Arc::new(MockOptions::default())).await;
    let (md, mut rx) = md(&server).await;
    conf::order_book_snapshot(&md, &sber(), 20).await;
    let sub = conf::order_book_stream(&md, &mut rx, "bcs-md", &sber()).await;
    conf::unsubscribe(&md, sub).await;
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
    conf::portfolio(&ud, &mut rx, "bcs-ud").await;
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
    // BCS schedules are per instrument: "TICKER@CLASSCODE".
    conf::market_data_history(&md, &sber(), Timeframe::M1, day(), "SBER@TQBR", day()).await;
    conf::market_data_streams(&md, &mut rx, "bcs-md", &sber(), Timeframe::M1).await;
    md.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn own_orders_events_active_replace_and_executions() {
    let opts = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&opts)).await;
    let (ud, mut rx) = ud(&server).await;
    conf::reconcile(&ud).await;
    // The order-events socket connects in the background: events before that are not delivered.
    brokapi_mocks::time::wait_until("order events socket connected", || opts.event_sockets() > 0)
        .await;
    conf::order_state_events(&ud, &mut rx, "bcs-ud", &sber(), Price::new(dec!(100.0))).await;
    conf::active_orders_lifecycle(&ud, &sber(), Price::new(dec!(100.0))).await;
    assert!(ud.caps().supports_replace);
    conf::replace_order(&ud, true, &sber(), Price::from(100), Price::from(101)).await;
    conf::executions(
        &ud,
        &mut rx,
        "bcs-ud",
        &sber(),
        Price::new(dec!(100.0)),
        |cid| {
            let fill = server.fill(
                &cid.hyphenated().to_string(),
                brokapi_core::Qty::from(2),
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
