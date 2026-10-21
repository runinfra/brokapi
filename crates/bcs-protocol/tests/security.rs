//! What must not leak (tokens in `Debug`) and what must not be repeated (a command after a 401).
//!
//! The server is a few lines of plain TCP, so the test sees every request and answers 401 on purpose.

mod common;

use brokapi_bcs_protocol::config::BcsConfig;
use brokapi_bcs_protocol::error::BcsApiError;
use brokapi_bcs_protocol::http::client::{AUTH_TOKEN_PATH, BcsHttpClient};
use brokapi_bcs_protocol::http::models::{TokenRequest, TokenResponse};
use common::{Server, serve};

fn token(n: usize, expires_in: u64) -> (u16, String) {
    (
        200,
        format!(
            r#"{{"access_token":"tok{n}","expires_in":{expires_in},"refresh_expires_in":7776000,"token_type":"bearer"}}"#
        ),
    )
}

/// A server whose first token is refused by every other path with 401, and whose second token works.
fn first_token_is_stale() -> Server {
    serve(|line, auth, n| {
        if line.contains(AUTH_TOKEN_PATH) {
            return token(n, 86_400);
        }
        if auth == "Bearer tok1" {
            (401, r#"{"error":"expired"}"#.into())
        } else {
            (200, r#"{"ok":true}"#.into())
        }
    })
}

fn client(server: &Server) -> BcsHttpClient {
    BcsHttpClient::new(BcsConfig::for_mock("refresh-secret", &server.url)).unwrap()
}

#[test]
fn debug_output_hides_every_token() {
    let cfg = BcsConfig::new("refresh-secret-123".to_string()).with_write(true);
    let shown = format!("{cfg:?}");
    assert!(!shown.contains("refresh-secret-123"), "{shown}");
    assert!(shown.contains("<redacted>"));
    // What is not secret stays visible.
    assert!(shown.contains("is_write: true") && shown.contains("be.broker.ru"));

    let req = TokenRequest {
        grant_type: "refresh_token".into(),
        refresh_token: "refresh-secret-123".into(),
        client_id: "trade-api-read".into(),
    };
    let shown = format!("{req:?}");
    assert!(!shown.contains("refresh-secret-123"), "{shown}");
    assert!(shown.contains("trade-api-read"));

    let resp: TokenResponse = serde_json::from_str(
        r#"{"access_token":"access-secret-456","expires_in":86400,"refresh_expires_in":7776000,
            "refresh_token":"refresh-secret-789","token_type":"bearer","scope":"x"}"#,
    )
    .unwrap();
    let shown = format!("{resp:?}");
    assert!(
        !shown.contains("access-secret-456") && !shown.contains("refresh-secret-789"),
        "{shown}"
    );
    assert!(shown.contains("86400") && shown.contains("bearer"));
    // A reply without a rotated refresh token prints `None`, not a mask.
    let plain: TokenResponse = serde_json::from_str(
        r#"{"access_token":"a","expires_in":1,"refresh_expires_in":1,"token_type":"bearer"}"#,
    )
    .unwrap();
    assert!(format!("{plain:?}").contains("refresh_token: None"));
}

#[tokio::test]
async fn a_command_is_sent_once_even_after_a_401() {
    let server = first_token_is_stale();
    let client = client(&server);
    let err = client
        .post_command::<serde_json::Value, _>("/orders", &serde_json::json!({"x": 1}))
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            BcsApiError::HttpError {
                status: Some(401),
                ..
            }
        ),
        "{err:?}"
    );
    // One command went out. The token was renewed for the next call (two token requests in all).
    assert_eq!(server.count("POST /orders"), 1);
    assert_eq!(server.count(AUTH_TOKEN_PATH), 2);
    // The next command works, with the new token, and is sent once.
    let ok: serde_json::Value = client
        .post_command("/orders", &serde_json::json!({"x": 1}))
        .await
        .unwrap();
    assert_eq!(ok["ok"], true);
    assert_eq!(server.count("POST /orders"), 2);
}

#[tokio::test]
async fn a_failed_renewal_does_not_hide_the_401_of_a_command() {
    // The token service answers once, then refuses.
    let server = serve(|line, _auth, n| {
        if line.contains(AUTH_TOKEN_PATH) {
            if n == 1 {
                return token(1, 86_400);
            }
            return (
                400,
                r#"{"error":"invalid_grant","error_description":"gone"}"#.into(),
            );
        }
        (401, r#"{"error":"expired"}"#.into())
    });
    let err = client(&server)
        .post_command::<serde_json::Value, _>("/orders", &serde_json::json!({}))
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            BcsApiError::HttpError {
                status: Some(401),
                ..
            }
        ),
        "{err:?}"
    );
    assert_eq!(server.count("POST /orders"), 1);
}

#[tokio::test]
async fn a_read_is_repeated_once_after_a_401() {
    let server = first_token_is_stale();
    let client = client(&server);
    // `post` is for reads (a search): one refresh and one repeat.
    let ok: serde_json::Value = client
        .post("/search", &serde_json::json!({"q": 1}))
        .await
        .unwrap();
    assert_eq!(ok["ok"], true);
    assert_eq!(server.count("POST /search"), 2);
    // Same for `get`, `get_with_query` and `get_raw`.
    let first = first_token_is_stale();
    let c = self::client(&first);
    let v: serde_json::Value = c.get("/read").await.unwrap();
    assert_eq!(v["ok"], true);
    assert_eq!(first.count("GET /read"), 2);

    let second = first_token_is_stale();
    let c = self::client(&second);
    let v: serde_json::Value = c.get_with_query("/read", &[("a", "1")]).await.unwrap();
    assert_eq!(v["ok"], true);
    assert_eq!(second.count("GET /read"), 2);

    let third = first_token_is_stale();
    let c = self::client(&third);
    let (status, body) = c.get_raw("/read").await.unwrap();
    assert!(status.is_success() && body.contains("ok"));
    assert_eq!(third.count("GET /read"), 2);
}

#[tokio::test]
async fn a_token_that_is_about_to_expire_is_renewed_before_it_is_used() {
    // `expires_in` 0: every token is already old, so every call asks for a new one first and never meets a 401.
    let server = serve(|line, auth, n| {
        if line.contains(AUTH_TOKEN_PATH) {
            return token(n, 0);
        }
        // A request is good only with the newest token, which is the one just issued.
        if auth.starts_with("Bearer tok") {
            (200, r#"{"ok":true}"#.into())
        } else {
            (401, "{}".into())
        }
    });
    let client = client(&server);
    for _ in 0..3 {
        let _: serde_json::Value = client.get("/read").await.unwrap();
    }
    assert_eq!(server.count("GET /read"), 3, "no call was repeated");
    assert_eq!(server.count(AUTH_TOKEN_PATH), 3, "a fresh token for each");

    // A long-lived token is asked for once.
    let long = serve(|line, _auth, n| {
        if line.contains(AUTH_TOKEN_PATH) {
            token(n, 86_400)
        } else {
            (200, r#"{"ok":true}"#.into())
        }
    });
    let c = self::client(&long);
    for _ in 0..3 {
        let _: serde_json::Value = c.get("/read").await.unwrap();
    }
    assert_eq!(long.count(AUTH_TOKEN_PATH), 1);
}
