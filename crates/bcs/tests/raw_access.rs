//! `connector.raw()`: any API method with the connector's token and rate limiter.
//!
//! The method used is `POST /api/v1/quotes`, which no connector wraps.

mod mock_server;

use brokapi_core::{Decimal, dec};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use brokapi_bcs::limits::default_limits;
use brokapi_bcs::{MarketDataConfig, MarketDataConnector, UserDataConfig, UserDataConnector};
use brokapi_bcs_protocol::config::BcsConfig;
use brokapi_bcs_protocol::http::models::{QuoteInstrument, QuotesRequest};
use brokapi_core::Error;
use brokapi_runtime::{BrokerLimiter, BrokerLimits};
use mock_server::{MockOptions, MockServer};
use tokio::sync::mpsc;

/// Bucket `tiny` has room for exactly 2 requests and does not refill during the test.
const LIMITS: &str = r#"
[bcs.buckets.global]
rate = 1000
per = "1s"
burst = 1000
[bcs.buckets.tiny]
rate = 1
per = "60s"
burst = 2
[bcs.buckets.default]
rate = 1
per = "60s"
burst = 1
[bcs.route]
"*" = ["global", "default"]
GetQuotes = ["global", "tiny"]
[bcs.conns]
max_conns = 4
max_subs_per_conn = 50
supports_multiplex = false
"#;

fn limiter() -> Arc<BrokerLimiter> {
    BrokerLimiter::new(&BrokerLimits::from_toml_str(LIMITS).unwrap())
}

fn req() -> QuotesRequest {
    QuotesRequest {
        instruments: vec![QuoteInstrument::new("SBER", "TQBR")],
    }
}

async fn md(server: &MockServer, limiter: Arc<BrokerLimiter>) -> MarketDataConnector {
    let (tx, _rx) = mpsc::channel(16);
    let cfg = MarketDataConfig::new(
        BcsConfig::for_mock("mock-refresh-token", &server.http_base),
        "bcs-md",
    );
    MarketDataConnector::start(cfg, limiter, tx)
        .await
        .expect("md")
}

#[tokio::test]
async fn raw_call_carries_the_access_token_and_spends_the_named_quota() {
    let opts: Arc<MockOptions> = Arc::default();
    let server = MockServer::start(Arc::clone(&opts)).await;
    let md = md(&server, limiter()).await;
    let raw = md.raw();

    let q = raw
        .call(
            "GetQuotes",
            |http| async move { http.get_quotes(&req()).await },
        )
        .await
        .unwrap();
    assert_eq!(q.records[0].ticker.as_deref(), Some("SBER"));
    assert_eq!(q.records[0].last, Some(dec!(300.15)));
    // The mock answers 401 to anything but the access token the connector got from the refresh token.
    assert_eq!(
        opts.quotes_auth.lock().unwrap().as_slice(),
        ["Bearer mock-access-token"]
    );

    raw.call(
        "GetQuotes",
        |http| async move { http.get_quotes(&req()).await },
    )
    .await
    .unwrap();
    let refused = raw
        .call(
            "GetQuotes",
            |http| async move { http.get_quotes(&req()).await },
        )
        .await
        .unwrap_err();
    assert!(matches!(refused, Error::RateLimited { .. }), "{refused:?}");
    assert_eq!(
        opts.quotes_calls.load(Ordering::SeqCst),
        2,
        "the refused call must not reach the broker"
    );
}

#[tokio::test]
async fn raw_shares_the_limiter_with_the_other_connector() {
    let server = MockServer::start(Arc::default()).await;
    let limiter = limiter();
    let md = md(&server, Arc::clone(&limiter)).await;
    let (tx, _rx) = mpsc::channel(16);
    let ud = UserDataConnector::start(
        UserDataConfig::new(
            BcsConfig::for_mock("mock-refresh-token", &server.http_base).with_write(false),
            "bcs-ud",
        ),
        limiter,
        tx,
    )
    .await
    .expect("ud");
    md.raw()
        .call(
            "GetQuotes",
            |http| async move { http.get_quotes(&req()).await },
        )
        .await
        .unwrap();
    ud.raw()
        .call(
            "GetQuotes",
            |http| async move { http.get_quotes(&req()).await },
        )
        .await
        .unwrap();
    let err = md
        .raw()
        .call(
            "GetQuotes",
            |http| async move { http.get_quotes(&req()).await },
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::RateLimited { .. }), "{err:?}");
}

#[tokio::test]
async fn method_without_a_route_spends_the_default_bucket() {
    let server = MockServer::start(Arc::default()).await;
    let md = md(&server, limiter()).await;
    let raw = md.raw();
    raw.call("MethodAddedLater", |http| async move {
        http.get_quotes(&req()).await
    })
    .await
    .unwrap();
    let err = raw
        .call("MethodAddedLater", |http| async move {
            http.get_quotes(&req()).await
        })
        .await
        .unwrap_err();
    assert!(matches!(err, Error::RateLimited { .. }), "{err:?}");
}

#[tokio::test]
async fn bundled_limits_route_the_raw_methods() {
    let l = default_limits();
    for m in [
        "GetQuotes",
        "GetOperationHistory",
        "GetLimits",
        "ConnectLimits",
    ] {
        assert!(!l.buckets_for(m).is_empty(), "{m} would go out for free");
    }
}

#[tokio::test]
async fn raw_opens_a_websocket_channel_with_the_same_token_and_quota() {
    use brokapi_bcs_protocol::models::messages::IncomingMessage;
    use brokapi_bcs_protocol::ws::client::BcsWsChannel;
    let opts: Arc<MockOptions> = Arc::default();
    let server = MockServer::start(Arc::clone(&opts)).await;
    let md = md(&server, limiter()).await;
    let before = opts.ws_connections();
    let (tx, rx) = kanal::bounded_async(16);
    // The default route "*" has a bucket of 1: the first channel opens, the second is refused.
    let _session = md
        .raw()
        .connect_ws(BcsWsChannel::OrderEvents, tx)
        .expect("channel opens");
    // The channel pushes nothing by itself: wait for the socket, then send it a status message.
    brokapi_mocks::time::wait_until("socket connected", || opts.event_sockets() > 0).await;
    server.push_event(
        &std::fs::read_to_string(format!(
            "{}/../bcs-protocol/tests/fixtures/ws/order_execution.json",
            env!("CARGO_MANIFEST_DIR")
        ))
        .unwrap(),
    );
    let msg = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
        .await
        .expect("a message")
        .expect("open");
    assert!(
        matches!(msg, IncomingMessage::OrderExecution(_)),
        "the status channel parses like execution-status: {msg:?}"
    );
    assert!(opts.ws_connections() > before);
    let (tx2, _rx2) = kanal::bounded_async(16);
    let err = md
        .raw()
        .connect_ws(BcsWsChannel::Limits, tx2)
        .err()
        .expect("refused");
    assert!(matches!(err, Error::RateLimited { .. }), "{err:?}");
}
