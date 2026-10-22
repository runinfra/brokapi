//! The life of the tokens: renewal, the repeat of a read after a 401, no repeat of a command, no token in `Debug`.

use brokapi_alor_protocol::config::AlorConfig;
use brokapi_alor_protocol::error::AlorApiError;
use brokapi_alor_protocol::http::client::AlorHttpClient;
use mockito::{Matcher, Server};

fn client(server: &Server) -> AlorHttpClient {
    AlorHttpClient::new(AlorConfig::for_mock("the-refresh-token", server.url())).unwrap()
}

#[tokio::test]
async fn the_access_token_is_asked_once_and_reused() {
    let mut server = Server::new_async().await;
    let token = server
        .mock("POST", "/refresh")
        .match_body(Matcher::Json(
            serde_json::json!({"token": "the-refresh-token"}),
        ))
        .with_body(r#"{"AccessToken":"a1"}"#)
        .expect(1)
        .create_async()
        .await;
    let time = server
        .mock("GET", "/md/v2/time")
        .match_header("authorization", "Bearer a1")
        .with_body("1792368000")
        .expect(2)
        .create_async()
        .await;
    let c = client(&server);
    assert_eq!(c.get_time().await.unwrap(), 1_792_368_000);
    assert_eq!(c.get_time().await.unwrap(), 1_792_368_000);
    token.assert_async().await;
    time.assert_async().await;
}

#[tokio::test]
async fn a_read_is_repeated_once_after_a_401_with_a_new_token() {
    let mut server = Server::new_async().await;
    let _first = server
        .mock("POST", "/refresh")
        .with_body(r#"{"AccessToken":"old"}"#)
        .expect(1)
        .create_async()
        .await;
    let c = client(&server);
    c.get_access_token().await.unwrap();
    server.reset();
    let renewed = server
        .mock("POST", "/refresh")
        .with_body(r#"{"AccessToken":"new"}"#)
        .expect(1)
        .create_async()
        .await;
    let refused = server
        .mock("GET", "/md/v2/time")
        .match_header("authorization", "Bearer old")
        .with_status(401)
        .with_body(r#"{"message":"Invalid JWT token!"}"#)
        .expect(1)
        .create_async()
        .await;
    let repeated = server
        .mock("GET", "/md/v2/time")
        .match_header("authorization", "Bearer new")
        .with_body("5")
        .expect(1)
        .create_async()
        .await;
    assert_eq!(c.get_time().await.unwrap(), 5);
    renewed.assert_async().await;
    refused.assert_async().await;
    repeated.assert_async().await;
}

#[tokio::test]
async fn a_command_is_sent_once_even_after_a_401() {
    let mut server = Server::new_async().await;
    let _t = server
        .mock("POST", "/refresh")
        .with_body(r#"{"AccessToken":"tok"}"#)
        .expect(2) // the first token, then the renewal for the next call
        .create_async()
        .await;
    let cancel = server
        .mock(
            "DELETE",
            Matcher::Regex("^/commandapi/warptrans/TRADE/v2/client/orders/77".into()),
        )
        .with_status(401)
        .with_body(r#"{"message":"Invalid JWT token!"}"#)
        .expect(1)
        .create_async()
        .await;
    let c = client(&server);
    match c.cancel_order("MOEX", "D1", "77", false).await {
        Err(AlorApiError::Http {
            status: Some(401), ..
        }) => {}
        other => panic!("{other:?}"),
    }
    cancel.assert_async().await;
}

#[tokio::test]
async fn a_refused_refresh_token_is_an_auth_error() {
    let mut server = Server::new_async().await;
    let _t = server
        .mock("POST", "/refresh")
        .with_status(403)
        .with_body(r#"{"message":"Refresh token is not valid"}"#)
        .create_async()
        .await;
    match client(&server).get_access_token().await {
        Err(AlorApiError::Auth { message }) => assert!(message.contains("not valid"), "{message}"),
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn statuses_become_the_errors_they_mean() {
    let mut server = Server::new_async().await;
    let _t = server
        .mock("POST", "/refresh")
        .with_body(r#"{"AccessToken":"tok"}"#)
        .create_async()
        .await;
    let _a = server
        .mock(
            "GET",
            Matcher::Regex("^/md/v2/Clients/MOEX/D1/orders/1".into()),
        )
        .with_status(404)
        .with_body(r#"{"message":"order not found"}"#)
        .create_async()
        .await;
    let _b = server
        .mock("GET", "/md/v2/time")
        .with_status(429)
        .create_async()
        .await;
    let c = client(&server);
    assert!(matches!(
        c.get_order("MOEX", "D1", "1").await,
        Err(AlorApiError::NotFound { .. })
    ));
    assert!(matches!(
        c.get_time().await,
        Err(AlorApiError::RateLimitExceeded)
    ));
}

#[test]
fn debug_output_hides_the_refresh_token() {
    let cfg = AlorConfig::new("super-secret-refresh-token".into());
    let text = format!("{cfg:?}");
    assert!(!text.contains("super-secret"), "{text}");
    assert!(text.contains("<redacted>"));
}
