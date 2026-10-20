//! `connector.raw()`: any API method with the connector's token and rate limiter.
//!
//! The method used is `GetLastPrices`, which no connector wraps.

mod mock_server;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use brokapi_core::Error;
use brokapi_runtime::BrokerLimiter;
use brokapi_tbank::limits::limits_from_toml_str;
use brokapi_tbank::{MarketDataConfig, MarketDataConnector, UserDataConfig, UserDataConnector};
use brokapi_tbank_protocol::TbankConfig;
use brokapi_tbank_protocol::v1 as pb;
use mock_server::{ACCOUNT_A, MockServer, TOKEN};
use tokio::sync::mpsc;

/// Bucket `tiny` has room for exactly 2 requests and does not refill during the test.
const LIMITS: &str = r#"
[tbank.buckets.global]
rate = 1000
per = "1s"
burst = 1000
[tbank.buckets.tiny]
rate = 1
per = "60s"
burst = 2
[tbank.buckets.default]
rate = 1
per = "60s"
burst = 1
[tbank.route]
"*" = ["global", "default"]
GetLastPrices = ["global", "tiny"]
[tbank.conns]
max_conns = 4
max_subs_per_conn = 100
supports_multiplex = true
"#;

fn cfg(server: &MockServer) -> TbankConfig {
    TbankConfig::for_mock(TOKEN, &server.endpoint).with_request_timeout(Duration::from_secs(5))
}

fn last_prices_req() -> pb::GetLastPricesRequest {
    pb::GetLastPricesRequest {
        instrument_id: vec!["uid-sber".into()],
        ..Default::default()
    }
}

#[tokio::test]
async fn raw_call_passes_auth_and_spends_the_named_quota() {
    let server = MockServer::start(Arc::default()).await;
    let limiter = BrokerLimiter::new(&limits_from_toml_str(LIMITS).unwrap());
    let (tx, _rx) = mpsc::channel(16);
    let md = MarketDataConnector::start(
        MarketDataConfig::new(cfg(&server), "tbank-md"),
        Arc::clone(&limiter),
        tx,
    )
    .await
    .unwrap();
    let raw = md.raw();

    // Passes authorization (the mock refuses anything but the Bearer token) and returns the answer.
    let resp = raw
        .call("GetLastPrices", |c| async move {
            c.market_data().get_last_prices(last_prices_req()).await
        })
        .await
        .unwrap();
    assert_eq!(resp.last_prices.len(), 1);
    assert_eq!(resp.last_prices[0].ticker, "SBER");
    let seen = server.opts.auth_seen.lock().unwrap().clone();
    assert!(
        !seen.is_empty() && seen.iter().all(|v| v == &format!("Bearer {TOKEN}")),
        "{seen:?}"
    );

    // The second call uses up the bucket, the third is refused BEFORE it is sent.
    raw.call("GetLastPrices", |c| async move {
        c.market_data().get_last_prices(last_prices_req()).await
    })
    .await
    .unwrap();
    let refused = raw
        .call("GetLastPrices", |c| async move {
            c.market_data().get_last_prices(last_prices_req()).await
        })
        .await
        .unwrap_err();
    assert!(matches!(refused, Error::RateLimited { .. }), "{refused:?}");
    assert_eq!(
        server.opts.last_price_calls.load(Ordering::SeqCst),
        2,
        "the refused call must not reach the broker"
    );
}

#[tokio::test]
async fn raw_quota_is_the_one_the_connector_shares() {
    // The same limiter object is handed to a user data connector: what raw() spends, it feels.
    let server = MockServer::start(Arc::default()).await;
    let limiter = BrokerLimiter::new(&limits_from_toml_str(LIMITS).unwrap());
    let (tx, _rx) = mpsc::channel(16);
    let md = MarketDataConnector::start(
        MarketDataConfig::new(cfg(&server), "tbank-md"),
        Arc::clone(&limiter),
        tx.clone(),
    )
    .await
    .unwrap();
    let ud = UserDataConnector::start(
        UserDataConfig::new(cfg(&server).with_account(ACCOUNT_A), "tbank-ud"),
        Arc::clone(&limiter),
        tx,
    )
    .await
    .unwrap();
    assert_eq!(ud.raw().account_id(), Some(ACCOUNT_A));
    assert_eq!(md.raw().account_id(), None);

    md.raw()
        .call("GetLastPrices", |c| async move {
            c.market_data().get_last_prices(last_prices_req()).await
        })
        .await
        .unwrap();
    ud.raw()
        .call("GetLastPrices", |c| async move {
            c.market_data().get_last_prices(last_prices_req()).await
        })
        .await
        .unwrap();
    // Both connectors drew from the same 2-token bucket.
    let err = md
        .raw()
        .call("GetLastPrices", |c| async move {
            c.market_data().get_last_prices(last_prices_req()).await
        })
        .await
        .unwrap_err();
    assert!(matches!(err, Error::RateLimited { .. }), "{err:?}");
}

#[tokio::test]
async fn method_without_a_route_spends_the_default_bucket() {
    let server = MockServer::start(Arc::default()).await;
    let limiter = BrokerLimiter::new(&limits_from_toml_str(LIMITS).unwrap());
    let (tx, _rx) = mpsc::channel(16);
    let md =
        MarketDataConnector::start(MarketDataConfig::new(cfg(&server), "tbank-md"), limiter, tx)
            .await
            .unwrap();
    let raw = md.raw();
    // A name the limits file never heard of goes through the "*" route (bucket of 1).
    raw.call("SomeMethodAddedLater", |c| async move {
        c.market_data().get_last_prices(last_prices_req()).await
    })
    .await
    .unwrap();
    let err = raw
        .call("SomeMethodAddedLater", |c| async move {
            c.market_data().get_last_prices(last_prices_req()).await
        })
        .await
        .unwrap_err();
    assert!(matches!(err, Error::RateLimited { .. }), "{err:?}");
}

#[tokio::test]
async fn raw_errors_become_connector_errors() {
    let server = MockServer::start(Arc::default()).await;
    let limiter = BrokerLimiter::new(&limits_from_toml_str(LIMITS).unwrap());
    let (tx, _rx) = mpsc::channel(16);
    // A wrong token: the broker answers UNAUTHENTICATED, which is a configuration error.
    let bad = TbankConfig::for_mock("wrong", &server.endpoint)
        .with_request_timeout(Duration::from_secs(5));
    let md = MarketDataConnector::start(MarketDataConfig::new(bad, "tbank-md"), limiter, tx)
        .await
        .unwrap();
    let err = md
        .raw()
        .call("GetLastPrices", |c| async move {
            c.market_data().get_last_prices(last_prices_req()).await
        })
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Config(_)), "{err:?}");
}

#[tokio::test]
async fn raw_command_without_an_answer_is_unknown_and_not_repeated() {
    let opts: Arc<mock_server::MockOptions> = Arc::default();
    opts.post_delay_ms.store(600, Ordering::SeqCst);
    let server = MockServer::start(Arc::clone(&opts)).await;
    let limiter = BrokerLimiter::new(&limits_from_toml_str(LIMITS).unwrap());
    let (tx, _rx) = mpsc::channel(16);
    let short = TbankConfig::for_mock(TOKEN, &server.endpoint)
        .with_request_timeout(Duration::from_millis(150));
    let md = MarketDataConnector::start(MarketDataConfig::new(short, "tbank-md"), limiter, tx)
        .await
        .unwrap();
    let req = pb::PostOrderRequest {
        quantity: 1,
        account_id: ACCOUNT_A.into(),
        order_id: "00000000-0000-4000-8000-000000000001".into(),
        instrument_id: "uid-sber".into(),
        ..Default::default()
    };
    let err = brokapi_mocks::time::elapse_when(
        "the order reached the mock",
        || opts.post_calls.load(Ordering::SeqCst) == 1,
        Duration::from_millis(200),
        md.raw().call_command(
            "PostOrder",
            |c| async move { c.orders().post_order(req).await },
        ),
    )
    .await
    .unwrap_err();
    assert!(matches!(err, Error::Unknown), "{err:?}");
    // No retry, neither at once nor later: an hour ahead on the clock, then a round trip behind it.
    brokapi_mocks::time::jump(Duration::from_secs(3600)).await;
    brokapi_tbank_protocol::TbankClient::new(&TbankConfig::for_mock(TOKEN, &server.endpoint))
        .unwrap()
        .list_accounts()
        .await
        .unwrap();
    assert_eq!(
        opts.post_calls.load(Ordering::SeqCst),
        1,
        "one send, no retry"
    );
}
