//! `connector.raw()`: any API method with the connector's token and rate limiter.
//!
//! The method used is `LastQuote`, which no connector wraps.

mod mock_server;

use std::sync::Arc;

use brokapi_core::Error;
use brokapi_finam::limits::parse_limits;
use brokapi_finam::{MarketDataConfig, MarketDataConnector, UserDataConfig, UserDataConnector};
use brokapi_finam_protocol::pb::grpc::tradeapi::v1::marketdata as pm;
use brokapi_runtime::BrokerLimiter;
use mock_server::{MockServer, api};
use tokio::sync::mpsc;

/// Bucket `tiny` has room for exactly 2 requests and does not refill during the test.
const LIMITS: &str = r#"
[finam.buckets.global]
rate = 1000
per = "1s"
burst = 1000
[finam.buckets.tiny]
rate = 1
per = "60s"
burst = 2
[finam.buckets.default]
rate = 1
per = "60s"
burst = 1
[finam.route]
"*" = ["global", "default"]
LastQuote = ["global", "tiny"]
[finam.conns]
max_conns = 1
max_subs_per_conn = 100
supports_multiplex = true
"#;

fn quote_req() -> pm::QuoteRequest {
    pm::QuoteRequest {
        symbol: "SBER@MISX".into(),
    }
}

fn tiny_limiter() -> Arc<BrokerLimiter> {
    BrokerLimiter::new(&parse_limits(LIMITS).unwrap())
}

async fn md(server: &MockServer, limiter: Arc<BrokerLimiter>) -> MarketDataConnector {
    let (tx, _rx) = mpsc::channel(16);
    MarketDataConnector::start(MarketDataConfig::new(api(server), "finam-md"), limiter, tx)
        .await
        .unwrap()
}

#[tokio::test]
async fn raw_call_uses_the_current_jwt_and_spends_the_named_quota() {
    let server = MockServer::start().await;
    let md = md(&server, tiny_limiter()).await;
    let raw = md.raw();

    let resp = raw
        .call("LastQuote", |api| async move {
            api.market_data().last_quote(quote_req()).await
        })
        .await
        .unwrap();
    assert_eq!(resp.symbol, "SBER@MISX");
    // The mock accepts only tokens it issued: the call carried the connector's JWT.
    assert!(
        server
            .state
            .tokens_seen()
            .iter()
            .all(|t| t.starts_with("jwt")),
        "{:?}",
        server.state.tokens_seen()
    );
    assert_eq!(server.state.count_calls("LastQuote"), 1);

    raw.call("LastQuote", |api| async move {
        api.market_data().last_quote(quote_req()).await
    })
    .await
    .unwrap();
    let refused = raw
        .call("LastQuote", |api| async move {
            api.market_data().last_quote(quote_req()).await
        })
        .await
        .unwrap_err();
    assert!(matches!(refused, Error::RateLimited { .. }), "{refused:?}");
    assert_eq!(
        server.state.count_calls("LastQuote"),
        2,
        "the refused call must not reach the broker"
    );
}

#[tokio::test]
async fn raw_shares_the_limiter_with_the_other_connectors() {
    let server = MockServer::start().await;
    let limiter = tiny_limiter();
    let md = md(&server, Arc::clone(&limiter)).await;
    let (tx, _rx) = mpsc::channel(16);
    let ud = UserDataConnector::start(
        UserDataConfig::new(api(&server), "ACC1", "finam-ud"),
        Arc::clone(&limiter),
        tx,
    )
    .await
    .unwrap();
    assert_eq!(ud.raw().account_id(), Some("ACC1"));
    assert_eq!(md.raw().account_id(), None);

    md.raw()
        .call("LastQuote", |api| async move {
            api.market_data().last_quote(quote_req()).await
        })
        .await
        .unwrap();
    ud.raw()
        .call("LastQuote", |api| async move {
            api.market_data().last_quote(quote_req()).await
        })
        .await
        .unwrap();
    let err = ud
        .raw()
        .call("LastQuote", |api| async move {
            api.market_data().last_quote(quote_req()).await
        })
        .await
        .unwrap_err();
    assert!(matches!(err, Error::RateLimited { .. }), "{err:?}");
}

#[tokio::test]
async fn method_without_a_route_spends_the_default_bucket() {
    let server = MockServer::start().await;
    let md = md(&server, tiny_limiter()).await;
    let raw = md.raw();
    raw.call("MethodAddedLater", |api| async move {
        api.market_data().last_quote(quote_req()).await
    })
    .await
    .unwrap();
    let err = raw
        .call("MethodAddedLater", |api| async move {
            api.market_data().last_quote(quote_req()).await
        })
        .await
        .unwrap_err();
    assert!(matches!(err, Error::RateLimited { .. }), "{err:?}");
}

#[tokio::test]
async fn read_renews_the_jwt_once_a_command_does_not_retry() {
    let server = MockServer::start().await;
    let md = md(
        &server,
        BrokerLimiter::new(&brokapi_finam::limits::default_limits()),
    )
    .await;
    let raw = md.raw();
    let issued = server.state.issued_tokens();

    // One UNAUTHENTICATED answer: the read renews the token and succeeds.
    server
        .state
        .options
        .unauth_next
        .store(1, std::sync::atomic::Ordering::SeqCst);
    raw.call("LastQuote", |api| async move {
        api.market_data().last_quote(quote_req()).await
    })
    .await
    .unwrap();
    assert!(server.state.issued_tokens() > issued, "the JWT was renewed");

    // Same situation for a command: no retry, and the answer is a definite refusal (it never ran).
    server
        .state
        .options
        .unauth_next
        .store(1, std::sync::atomic::Ordering::SeqCst);
    let before = server.state.count_calls("LastQuote");
    let err = raw
        .call_command("LastQuote", |api| async move {
            api.market_data().last_quote(quote_req()).await
        })
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Rejected { .. }), "{err:?}");
    assert_eq!(server.state.count_calls("LastQuote"), before);
}
