//! `connector.raw()`: any API method with the connector's token and rate limiter.
//!
//! The method used is the quotations of several instruments, which no connector wraps.

mod mock_server;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use brokapi_alor::{MarketDataConfig, MarketDataConnector, UserDataConfig, UserDataConnector};
use brokapi_alor_protocol::config::AlorConfig;
use brokapi_alor_protocol::ws::models::{WsIncoming, WsSubscribe};
use brokapi_alor_protocol::ws::session::WsOptions;
use brokapi_core::{Error, ShutdownMode};
use brokapi_runtime::{BrokerLimiter, BrokerLimits};
use mock_server::{MockOptions, MockServer};
use tokio::sync::mpsc;

/// Bucket `tiny` has room for exactly 2 requests and does not refill during the test.
const LIMITS: &str = r#"
[alor.buckets.global]
rate = 1000
per = "1s"
burst = 1000
[alor.buckets.tiny]
rate = 1
per = "60s"
burst = 2
[alor.buckets.default]
rate = 1
per = "60s"
burst = 1
[alor.route]
"*" = ["global", "default"]
GetQuotes = ["global", "tiny"]
ConnectWs = ["global"]
ConnectCws = ["global"]
[alor.conns]
max_conns = 10
max_subs_per_conn = 5000
supports_multiplex = true
"#;

fn limiter() -> Arc<BrokerLimiter> {
    BrokerLimiter::new(&BrokerLimits::from_toml_str(LIMITS).unwrap())
}

async fn md(server: &MockServer, limiter: Arc<BrokerLimiter>) -> MarketDataConnector {
    let (tx, _rx) = mpsc::channel(16);
    let cfg = MarketDataConfig::new(
        AlorConfig::for_mock("mock-refresh-token", &server.http_base),
        "alor-md",
    );
    MarketDataConnector::start(cfg, limiter, tx)
        .await
        .expect("md start")
}

#[tokio::test]
async fn raw_calls_a_method_no_shared_trait_wraps_with_the_token_of_the_connector() {
    let opts = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&opts)).await;
    let md = md(&server, limiter()).await;
    let quotes = md
        .raw()
        .call("GetQuotes", |http| async move {
            http.get_quotes(&[("MOEX", "SBER")]).await
        })
        .await
        .unwrap();
    assert_eq!(quotes[0].symbol, "SBER");
    assert_eq!(opts.quotes_calls.load(Ordering::SeqCst), 1);
    // The same token as the connector's own calls: one token request in all.
    assert_eq!(
        *opts.quotes_auth.lock().unwrap(),
        vec![format!("Bearer {}", brokapi_mocks::alor::DEFAULT_ACCESS)]
    );
    assert_eq!(opts.refresh_calls.load(Ordering::SeqCst), 1);
    md.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn raw_spends_the_quota_of_its_route_and_a_refusal_sends_nothing() {
    let opts = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&opts)).await;
    let md = md(&server, limiter()).await;
    let raw = md.raw();
    for _ in 0..2 {
        raw.call("GetQuotes", |http| async move {
            http.get_quotes(&[("MOEX", "SBER")]).await
        })
        .await
        .unwrap();
    }
    let third = raw
        .call("GetQuotes", |http| async move {
            http.get_quotes(&[("MOEX", "SBER")]).await
        })
        .await;
    assert!(matches!(third, Err(Error::RateLimited { .. })));
    assert_eq!(
        opts.quotes_calls.load(Ordering::SeqCst),
        2,
        "the refused call was not sent"
    );
    // A name with no route of its own spends the default route, which holds one call.
    raw.call(
        "GetSomethingNew",
        |http| async move { http.get_time().await },
    )
    .await
    .unwrap();
    let again = raw
        .call(
            "GetSomethingNew",
            |http| async move { http.get_time().await },
        )
        .await;
    assert!(matches!(again, Err(Error::RateLimited { .. })));
    md.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn raw_commands_report_the_refusal_of_the_broker_as_rejected_not_unknown() {
    let server = MockServer::start(Arc::default()).await;
    let (tx, _rx) = mpsc::channel(16);
    let cfg = AlorConfig::for_mock("mock-refresh-token", &server.http_base);
    let ud = UserDataConnector::start(UserDataConfig::new(cfg, "alor-ud"), limiter(), tx)
        .await
        .unwrap();
    // A cancel of an order that does not exist: the broker says 404, nothing was done.
    let result = ud
        .raw()
        .call_command("CancelOrder", |http| async move {
            http.cancel_order("MOEX", "D00001", "1", false).await
        })
        .await;
    assert!(
        matches!(result, Err(Error::Rejected { ref code, .. }) if code == "not_found"),
        "{result:?}"
    );
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn raw_opens_a_second_data_session_and_the_trading_channel() {
    let opts = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&opts)).await;
    let md = md(&server, limiter()).await;
    let raw = md.raw();

    let (tx, rx) = kanal::bounded_async(64);
    let session = raw.connect_ws(tx, WsOptions::default()).unwrap();
    session
        .subscribe(WsSubscribe::quotes(
            "my-quotes",
            "MOEX",
            "SBER",
            Some("TQBR"),
        ))
        .await
        .unwrap();
    loop {
        if let WsIncoming::Data { guid, .. } = rx.recv().await.unwrap() {
            assert_eq!(guid, "my-quotes");
            break;
        }
    }

    use serde_json::json;
    let cws = raw.connect_cws(Duration::from_secs(5)).await.unwrap();
    let placed = cws
        .command(
            brokapi_alor_protocol::ws::cws::opcodes::CREATE_LIMIT,
            json!({"side": "buy", "quantity": 1, "price": 100.5,
                   "instrument": {"symbol": "SBER", "exchange": "MOEX", "instrumentGroup": "TQBR"},
                   "user": {"portfolio": "D00001"}, "comment": "by cws"}),
        )
        .await
        .unwrap();
    assert_eq!(placed.code, 200);
    let order = placed.order_number.unwrap();
    let cancelled = cws
        .command(
            brokapi_alor_protocol::ws::cws::opcodes::DELETE_LIMIT,
            json!({"orderId": order}),
        )
        .await
        .unwrap();
    assert_eq!(cancelled.code, 200);
    md.shutdown(ShutdownMode::Immediate).await;
}
