//! Shared scenarios from `brokapi-conformance` against the gRPC mock (no external network).

mod mock_server;

use brokapi_core::Price;
use std::sync::Arc;

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
