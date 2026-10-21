//! How `BcsHttpClient` turns the answers of the broker into errors, and what it does with the tokens it gets.

mod common;

use brokapi_bcs_protocol::config::BcsConfig;
use brokapi_bcs_protocol::error::BcsApiError;
use brokapi_bcs_protocol::http::client::{AUTH_TOKEN_PATH, BcsHttpClient};
use common::{Server, serve};

const TOKEN: &str = r#"{"access_token":"t1","expires_in":86400,"refresh_expires_in":7776000,"token_type":"bearer"}"#;

fn with_status(status: u16, body: &'static str) -> Server {
    serve(move |line, _a, _n| {
        if line.contains(AUTH_TOKEN_PATH) {
            (200, TOKEN.into())
        } else {
            (status, body.into())
        }
    })
}

fn client(server: &Server) -> BcsHttpClient {
    BcsHttpClient::new(BcsConfig::for_mock("refresh", &server.url)).unwrap()
}

async fn error_of(status: u16, body: &'static str) -> BcsApiError {
    let server = with_status(status, body);
    client(&server)
        .get::<serde_json::Value>("/x")
        .await
        .unwrap_err()
}

#[tokio::test]
async fn the_error_of_an_answer_is_told_by_its_type_then_by_its_status() {
    assert!(matches!(
        error_of(400, r#"{"type":"VALIDATION_ERROR","traceId":"tr","errors":[{"type":"INVALID","field":"price"}]}"#).await,
        BcsApiError::ValidationError { trace_id: Some(t), errors } if t == "tr" && errors.len() == 1
    ));
    assert!(matches!(
        error_of(404, r#"{"type":"NOT_FOUND","traceId":"tr"}"#).await,
        BcsApiError::NotFound { trace_id: Some(_) }
    ));
    // Another type: an HTTP error with the status and the type as the message.
    assert!(matches!(
        error_of(409, r#"{"type":"CONFLICT","traceId":"tr"}"#).await,
        BcsApiError::HttpError { status: Some(409), message, trace_id: Some(_) } if message == "CONFLICT"
    ));
    // 429 without a body of the broker's kind: a rate limit.
    assert!(matches!(
        error_of(429, "slow down").await,
        BcsApiError::RateLimitExceeded
    ));
    // Any other status without the broker's JSON: the body is the message.
    assert!(matches!(
        error_of(502, "bad gateway").await,
        BcsApiError::HttpError { status: Some(502), message, trace_id: None } if message == "bad gateway"
    ));
}

#[tokio::test]
async fn a_success_that_is_not_the_expected_json_is_a_parse_error_with_a_cut_body() {
    let long = "x".repeat(500);
    let server = serve(move |line, _a, _n| {
        if line.contains(AUTH_TOKEN_PATH) {
            (200, TOKEN.into())
        } else {
            (200, long.clone())
        }
    });
    let err = client(&server)
        .get::<serde_json::Value>("/x")
        .await
        .unwrap_err();
    match err {
        BcsApiError::ParseError(text) => {
            assert!(text.len() < 400, "the body is cut: {}", text.len())
        }
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn the_token_service_refusing_the_refresh_token_is_an_auth_error() {
    let server = serve(|_line, _a, _n| {
        (
            400,
            r#"{"error":"invalid_grant","error_description":"Token is not active"}"#.into(),
        )
    });
    let err = client(&server)
        .get::<serde_json::Value>("/x")
        .await
        .unwrap_err();
    assert!(
        matches!(err, BcsApiError::AuthError { ref message } if message == "Token is not active"),
        "{err:?}"
    );
    // Without a description: the error text.
    let server = serve(|_line, _a, _n| (400, r#"{"error":"invalid_grant"}"#.into()));
    let err = client(&server).refresh_access_token().await.unwrap_err();
    assert!(
        matches!(err, BcsApiError::AuthError { ref message } if message == "invalid_grant"),
        "{err:?}"
    );
    // A reply that is not the service's JSON: an HTTP error with the status.
    let server = serve(|_line, _a, _n| (503, "maintenance".into()));
    let err = client(&server).refresh_access_token().await.unwrap_err();
    assert!(
        matches!(
            err,
            BcsApiError::HttpError {
                status: Some(503),
                ..
            }
        ),
        "{err:?}"
    );
}

#[tokio::test]
async fn a_rotated_refresh_token_is_kept_in_memory() {
    let server = serve(|line, _a, n| {
        if line.contains(AUTH_TOKEN_PATH) {
            (
                200,
                format!(
                    r#"{{"access_token":"t{n}","expires_in":86400,"refresh_expires_in":7776000,"refresh_token":"rotated-{n}","token_type":"bearer"}}"#
                ),
            )
        } else {
            (200, "{}".into())
        }
    });
    let c = client(&server);
    assert_eq!(c.current_refresh_token().await, "refresh");
    assert_eq!(c.refresh_access_token().await.unwrap(), "t1");
    assert_eq!(c.current_refresh_token().await, "rotated-1");
    // The next renewal sends the rotated one.
    c.refresh_access_token().await.unwrap();
    let bodies = server.bodies(AUTH_TOKEN_PATH);
    assert!(bodies[0].contains("refresh_token=refresh"), "{}", bodies[0]);
    assert!(
        bodies[1].contains("refresh_token=rotated-1"),
        "{}",
        bodies[1]
    );
    assert!(bodies[0].contains("client_id=trade-api-read"));
    assert_eq!(c.base_url(), server.url);
    let _ = c.inner_client();
}

#[tokio::test]
async fn a_write_config_asks_for_the_write_client() {
    let server = with_status(200, "{}");
    let c =
        BcsHttpClient::new(BcsConfig::for_mock("refresh", &server.url).with_write(true)).unwrap();
    let _: serde_json::Value = c.get("/x").await.unwrap();
    assert!(server.bodies(AUTH_TOKEN_PATH)[0].contains("client_id=trade-api-write"));
}

#[tokio::test]
async fn a_connection_that_fails_is_an_http_error_without_a_status() {
    let c = BcsHttpClient::new(BcsConfig::for_mock("refresh", "http://127.0.0.1:1")).unwrap();
    let err = c.get::<serde_json::Value>("/x").await.unwrap_err();
    assert!(
        matches!(err, BcsApiError::HttpError { status: None, .. }),
        "{err:?}"
    );
    let err = c.get_raw("/x").await.unwrap_err();
    assert!(
        matches!(err, BcsApiError::HttpError { status: None, .. }),
        "{err:?}"
    );
}
