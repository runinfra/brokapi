//! Edges of the protocol client: the configuration from the environment, the answers of the authorization server that
//! are not a token, the raw read, the frames of the data channel, and the sessions of both channels.

use std::sync::Arc;
use std::time::Duration;

use brokapi_alor_protocol::config::{AlorConfig, AlorEnv};
use brokapi_alor_protocol::error::AlorApiError;
use brokapi_alor_protocol::http::client::AlorHttpClient;
use brokapi_alor_protocol::ws::cws::{CwsSession, opcodes};
use brokapi_alor_protocol::ws::models::{WsIncoming, WsSubscribe, parse_incoming};
use brokapi_alor_protocol::ws::session::{WS_MAX_SUBSCRIPTIONS, WsOptions, WsSession};
use brokapi_mocks::alor::{MockOptions, MockServer};
use mockito::Server;

// --- the configuration ---

/// The only test of this file that reads the environment: the variables are process-wide.
#[test]
fn the_environment_names_the_token_the_portfolio_the_exchange_and_the_contour() {
    let dir = std::env::temp_dir().join(format!("brokapi-alor-env-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let file = dir.join(".env");

    // No token: the error says which variable is missing.
    std::fs::write(&file, "SOMETHING_ELSE=1\n").unwrap();
    assert!(matches!(
        AlorConfig::from_env_file(&file),
        Err(std::env::VarError::NotPresent)
    ));

    std::fs::write(
        &file,
        "ALOR_REFRESH_TOKEN=from-the-file\nALOR_PORTFOLIO=D777\nALOR_EXCHANGE=SPBX\nALOR_ENV=test\n",
    )
    .unwrap();
    let cfg = AlorConfig::from_env_file(&file).unwrap();
    assert_eq!(cfg.portfolio.as_deref(), Some("D777"));
    assert_eq!(cfg.exchange, "SPBX");
    assert_eq!(cfg.env, AlorEnv::Test);
    assert!(
        cfg.http_base_url.contains("apidev"),
        "{}",
        cfg.http_base_url
    );
    // `from_env` reads the same variables, which are set by now.
    assert_eq!(
        AlorConfig::from_env().unwrap().portfolio.as_deref(),
        Some("D777")
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn the_builders_change_one_thing_each() {
    let cfg = AlorConfig::new("t".into())
        .with_portfolio("D1")
        .with_exchange("SPBX");
    assert_eq!(
        (cfg.portfolio.as_deref(), cfg.exchange.as_str()),
        (Some("D1"), "SPBX")
    );
    let prod = cfg.clone().with_env(AlorEnv::Production);
    assert_eq!(prod.env, AlorEnv::Production);
    let test = cfg.with_env(AlorEnv::Test);
    assert_ne!(test.http_base_url, prod.http_base_url);
    assert_ne!(test.ws_base_url, prod.ws_base_url);
    assert_ne!(test.auth_base_url, prod.auth_base_url);
}

#[test]
fn a_failure_of_the_json_or_of_the_address_is_a_parse_error() {
    let json: AlorApiError = serde_json::from_str::<u8>("x").unwrap_err().into();
    assert!(matches!(json, AlorApiError::Parse(_)));
    let url: AlorApiError = url::Url::parse("not a url").unwrap_err().into();
    assert!(matches!(url, AlorApiError::Parse(_)));
}

// --- the authorization server and the raw read ---

fn client(server: &Server) -> AlorHttpClient {
    AlorHttpClient::new(AlorConfig::for_mock("the-refresh-token", server.url())).unwrap()
}

#[tokio::test]
async fn a_refresh_that_fails_for_another_reason_than_the_token_is_a_transport_error() {
    let mut server = Server::new_async().await;
    let _m = server
        .mock("POST", "/refresh")
        .with_status(503)
        .with_body(r#"{"message":"maintenance"}"#)
        .create_async()
        .await;
    match client(&server).get_access_token().await {
        Err(AlorApiError::Http { status, message }) => {
            assert_eq!(status, Some(503));
            assert!(message.contains("maintenance"), "{message}");
        }
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn an_answer_without_the_token_is_a_parse_error() {
    let mut server = Server::new_async().await;
    let _m = server
        .mock("POST", "/refresh")
        .with_body(r#"{"Other":"x"}"#)
        .create_async()
        .await;
    assert!(matches!(
        client(&server).get_access_token().await,
        Err(AlorApiError::Parse(_))
    ));
}

/// A token with the claims the broker puts into the JWT: the portfolios are one text with spaces.
fn jwt(claims: &str) -> String {
    use base64::Engine;
    let enc = |s: &str| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(s);
    format!("{}.{}.sig", enc(r#"{"alg":"none"}"#), enc(claims))
}

#[tokio::test]
async fn the_portfolios_come_from_the_claims_of_the_token() {
    let mut server = Server::new_async().await;
    let token = jwt(r#"{"portfolios":"D1 7500K2 D3","exp":4102444800}"#);
    let _m = server
        .mock("POST", "/refresh")
        .with_body(format!(r#"{{"AccessToken":"{token}"}}"#))
        .create_async()
        .await;
    assert_eq!(
        client(&server).token_portfolios().await.unwrap(),
        ["D1", "7500K2", "D3"]
    );
}

#[tokio::test]
async fn the_raw_read_gives_the_status_and_the_body_and_the_addresses_are_the_configured_ones() {
    let mut server = Server::new_async().await;
    let _t = server
        .mock("POST", "/refresh")
        .with_body(r#"{"AccessToken":"a1"}"#)
        .create_async()
        .await;
    let _m = server
        .mock("GET", "/md/v2/nothing")
        .with_status(404)
        .with_body("no such thing")
        .create_async()
        .await;
    let c = client(&server);
    let (status, body) = c.read_raw("/md/v2/nothing", &[]).await.unwrap();
    assert_eq!(status.as_u16(), 404);
    assert_eq!(body, "no such thing");
    assert_eq!(c.base_url(), server.url());
    assert!(c.ws_base_url().starts_with("ws://"));
    let _ = c.inner_client();
}

#[tokio::test]
async fn the_raw_read_renews_the_token_once_after_a_401() {
    let mut server = Server::new_async().await;
    let refresh = server
        .mock("POST", "/refresh")
        .with_body(r#"{"AccessToken":"a1"}"#)
        .expect(2)
        .create_async()
        .await;
    let _m = server
        .mock("GET", "/md/v2/time")
        .with_status(401)
        .expect(2)
        .create_async()
        .await;
    let (status, _) = client(&server).read_raw("/md/v2/time", &[]).await.unwrap();
    assert_eq!(status.as_u16(), 401);
    refresh.assert_async().await;
}

// --- frames of the data channel ---

#[test]
fn every_subscription_names_its_opcode_and_carries_the_token_it_is_given() {
    let subs = [
        (
            WsSubscribe::instruments("g", "MOEX", "SBER", Some("TQBR")),
            "InstrumentsGetAndSubscribeV2",
        ),
        (
            WsSubscribe::stop_orders("g", "MOEX", "D1", true),
            "StopOrdersGetAndSubscribeV2",
        ),
        (
            WsSubscribe::summaries("g", "MOEX", "D1", false),
            "SummariesGetAndSubscribeV2",
        ),
        (
            WsSubscribe::risks("g", "MOEX", "D1", false),
            "RisksGetAndSubscribe",
        ),
        (
            WsSubscribe::spectra_risks("g", "MOEX", "D1", false),
            "SpectraRisksGetAndSubscribe",
        ),
    ];
    for (sub, opcode) in subs {
        let v: serde_json::Value = serde_json::from_str(&sub.to_json("tok")).unwrap();
        assert_eq!(v["opcode"], opcode);
        assert_eq!(v["token"], "tok");
        assert_eq!(v["guid"], "g");
        assert!(!sub.is_unsubscribe());
    }
    assert!(WsSubscribe::unsubscribe("g").is_unsubscribe());
}

#[test]
fn a_frame_that_is_neither_data_nor_status_is_kept_as_it_came() {
    for text in [
        r#"{"hello":1}"#,
        r#"{"data":{}}"#,
        r#"{"guid":"g"}"#,
        "[]",
        "nope",
    ] {
        assert_eq!(parse_incoming(text), WsIncoming::Unknown(text.to_string()));
    }
    assert_eq!(
        parse_incoming(r#"{"httpCode":400}"#),
        WsIncoming::Status {
            guid: None,
            code: 400,
            message: String::new()
        }
    );
}

// --- the session of the data channel ---

async fn session(
    server: &MockServer,
    options: WsOptions,
    room: usize,
) -> (WsSession, kanal::AsyncReceiver<WsIncoming>, AlorHttpClient) {
    let http = AlorHttpClient::new(AlorConfig::for_mock(
        "mock-refresh-token",
        &server.http_base,
    ))
    .unwrap();
    let (tx, rx) = kanal::bounded_async(room);
    (WsSession::spawn_with(http.clone(), tx, options), rx, http)
}

fn quick() -> WsOptions {
    WsOptions {
        announce_connects: true,
        reconnect_min: Duration::from_millis(10),
        reconnect_max: Duration::from_millis(20),
    }
}

async fn next_data(rx: &kanal::AsyncReceiver<WsIncoming>, guid: &str) -> serde_json::Value {
    loop {
        if let WsIncoming::Data { guid: g, data } = rx.recv().await.unwrap()
            && g == guid
        {
            return data;
        }
    }
}

#[tokio::test]
async fn a_session_that_is_full_drops_frames_and_counts_them() {
    let server = MockServer::start(Arc::default()).await;
    // One place in the channel and nobody reads: the frames after the first are dropped.
    let (session, _rx, _http) = session(&server, WsOptions::default(), 1).await;
    for n in 0..4 {
        session
            .subscribe(WsSubscribe::quotes(
                &format!("q{n}"),
                "MOEX",
                "SBER",
                Some("TQBR"),
            ))
            .await
            .unwrap();
    }
    brokapi_mocks::time::wait_until("frames are dropped", || session.dropped_events() > 0).await;
    assert!(session.is_connected());
}

#[tokio::test]
async fn the_subscriptions_are_limited_and_the_limit_says_so() {
    let server = MockServer::start(Arc::default()).await;
    let (session, rx, _http) = session(&server, WsOptions::default(), 64).await;
    // The answers are read and thrown away, so that the session is never blocked.
    let reader = tokio::spawn(async move { while rx.recv().await.is_ok() {} });
    for n in 0..WS_MAX_SUBSCRIPTIONS {
        session
            .subscribe(WsSubscribe::quotes(
                &format!("q{n}"),
                "MOEX",
                "SBER",
                Some("TQBR"),
            ))
            .await
            .unwrap();
    }
    match session
        .subscribe(WsSubscribe::quotes(
            "one-too-many",
            "MOEX",
            "SBER",
            Some("TQBR"),
        ))
        .await
    {
        Err(AlorApiError::Subscription { message }) => {
            assert!(message.contains("5000"), "{message}");
        }
        other => panic!("{other:?}"),
    }
    // Cancelling one makes room.
    session.unsubscribe("q0").await.unwrap();
    session
        .subscribe(WsSubscribe::quotes(
            "one-too-many",
            "MOEX",
            "SBER",
            Some("TQBR"),
        ))
        .await
        .unwrap();
    reader.abort();
}

#[tokio::test]
async fn a_subscription_is_cancelled_and_a_session_nobody_reads_ends_and_refuses_new_requests() {
    let server = MockServer::start(Arc::default()).await;
    let (session, rx, _http) = session(&server, quick(), 256).await;
    session
        .subscribe(WsSubscribe::quotes("q1", "MOEX", "SBER", Some("TQBR")))
        .await
        .unwrap();
    next_data(&rx, "q1").await;
    session.unsubscribe("q1").await.unwrap();
    // The reader goes away: the session ends at the next frame it cannot deliver.
    drop(rx);
    session
        .subscribe(WsSubscribe::quotes("q2", "MOEX", "SBER", Some("TQBR")))
        .await
        .unwrap();
    brokapi_mocks::time::wait_until_async("the session is closed", || async {
        session
            .subscribe(WsSubscribe::quotes("q3", "MOEX", "SBER", Some("TQBR")))
            .await
            .is_err()
    })
    .await;
    assert!(!session.is_connected());
    assert!(matches!(
        session.unsubscribe("q2").await,
        Err(AlorApiError::Subscription { .. })
    ));
}

#[tokio::test]
async fn a_cut_connection_is_restored_with_its_subscriptions_and_announced() {
    let opts = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&opts)).await;
    let (session, rx, _http) = session(&server, quick(), 256).await;
    session
        .subscribe(WsSubscribe::orders("o1", "MOEX", "D00001", true))
        .await
        .unwrap();
    brokapi_mocks::time::wait_until("the order stream is open", || opts.event_sockets() == 1).await;
    server.cut_event_sockets();
    brokapi_mocks::time::wait_until("a second connection", || opts.ws_connections() >= 2).await;
    brokapi_mocks::time::wait_until("the subscription is restored", || opts.event_sockets() == 1)
        .await;
    let mut announced = 0;
    while let Ok(Some(frame)) = rx.try_recv() {
        if frame == WsIncoming::Reconnected {
            announced += 1;
        }
    }
    assert!(
        announced >= 2,
        "every connect is announced, saw {announced}"
    );
}

#[tokio::test]
async fn a_cut_after_a_long_life_is_restored_as_well() {
    let opts = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&opts)).await;
    let (session, rx, _http) = session(&server, quick(), 256).await;
    session
        .subscribe(WsSubscribe::orders("o1", "MOEX", "D00001", true))
        .await
        .unwrap();
    brokapi_mocks::time::wait_until("the order stream is open", || opts.event_sockets() == 1).await;
    brokapi_mocks::time::jump(Duration::from_secs(31)).await;
    server.cut_event_sockets();
    brokapi_mocks::time::wait_until("a second connection", || opts.ws_connections() >= 2).await;
    brokapi_mocks::time::wait_until("the subscription is restored", || opts.event_sockets() == 1)
        .await;
    drop(rx);
}

#[tokio::test]
async fn a_401_on_the_channel_makes_the_session_renew_the_token() {
    let opts = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&opts)).await;
    let (session, _rx, _http) = session(&server, quick(), 256).await;
    session
        .subscribe(WsSubscribe::orders("o1", "MOEX", "D00001", true))
        .await
        .unwrap();
    brokapi_mocks::time::wait_until("the order stream is open", || opts.event_sockets() == 1).await;
    let before = opts.refresh_calls.load(std::sync::atomic::Ordering::SeqCst);
    server.push_event(r#"{"requestGuid":"o1","httpCode":401,"message":"Invalid JWT token!"}"#);
    brokapi_mocks::time::wait_until("the token is asked again", || {
        opts.refresh_calls.load(std::sync::atomic::Ordering::SeqCst) > before
    })
    .await;
}

// --- the trading channel ---

#[tokio::test]
async fn the_trading_channel_authorizes_commands_and_reports_what_goes_wrong() {
    let server = MockServer::start(Arc::default()).await;
    let http = AlorHttpClient::new(AlorConfig::for_mock(
        "mock-refresh-token",
        &server.http_base,
    ))
    .unwrap();
    let cws = CwsSession::connect(http.clone(), Duration::from_secs(5))
        .await
        .unwrap();
    // The fields must be an object.
    assert!(matches!(
        cws.command(opcodes::CREATE_LIMIT, serde_json::json!([1]))
            .await,
        Err(AlorApiError::Parse(_))
    ));
    // A command the channel does not know is a refusal in the reply, not an error.
    let reply = cws
        .command("create:nothing", serde_json::json!({}))
        .await
        .unwrap();
    assert_eq!(reply.code, 400);
    // After a quarter of an hour the session authorizes again before the command.
    brokapi_mocks::time::jump(Duration::from_secs(16 * 60)).await;
    let reply = cws
        .command(
            opcodes::CREATE_LIMIT,
            serde_json::json!({"side":"buy","quantity":1,"price":100,
                "instrument":{"symbol":"SBER","exchange":"MOEX","instrumentGroup":"TQBR"},
                "user":{"portfolio":"D00001"},"comment":"c"}),
        )
        .await
        .unwrap();
    assert_eq!(reply.code, 200);
    assert!(reply.order_number.is_some());
}

#[tokio::test]
async fn the_trading_channel_refuses_a_bad_token_and_a_closed_address() {
    let server = MockServer::start(Arc::default()).await;
    // The authorization server of the mock hands out `mock-access-...` for any token but the refused one,
    // so a token the channel does not accept is made by the configuration of a second client.
    let mut cfg = AlorConfig::for_mock("mock-refresh-token", &server.http_base);
    let mut other = Server::new_async().await;
    let _m = other
        .mock("POST", "/refresh")
        .with_body(r#"{"AccessToken":"not-a-mock-token"}"#)
        .create_async()
        .await;
    cfg.auth_base_url = other.url();
    let http = AlorHttpClient::new(cfg).unwrap();
    match CwsSession::connect(http, Duration::from_secs(5)).await {
        Err(AlorApiError::Ws { code, .. }) => assert_eq!(code, Some(401)),
        Ok(_) => panic!("the bad token was accepted"),
        Err(other) => panic!("{other:?}"),
    }
    // Nobody listens at this address.
    let dead = AlorHttpClient::new(AlorConfig::for_mock("t", "http://127.0.0.1:9")).unwrap();
    assert!(matches!(
        CwsSession::connect(dead, Duration::from_secs(1)).await,
        Err(AlorApiError::Ws { code: None, .. })
    ));
}
