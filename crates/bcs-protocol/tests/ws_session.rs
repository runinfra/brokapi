//! The WebSocket session against the in-process BCS imitation: no network, no tokens, no pauses.

use std::sync::Arc;
use std::time::Duration;

use brokapi_bcs_protocol::config::BcsConfig;
use brokapi_bcs_protocol::http::client::BcsHttpClient;
use brokapi_bcs_protocol::models::instrument::InstrumentKey;
use brokapi_bcs_protocol::models::messages::IncomingMessage;
use brokapi_bcs_protocol::ws::client::BcsWsChannel;
use brokapi_bcs_protocol::ws::models::MarketDataSubscribe;
use brokapi_bcs_protocol::ws::session::{WsOptions, WsSession};
use brokapi_mocks::bcs::{MockOptions, MockServer};
use brokapi_mocks::time::wait_until;

fn sber() -> Vec<InstrumentKey> {
    vec![InstrumentKey::new("SBER", "TQBR")]
}

/// `dataType` of every command the market data socket of the imitation received, in order.
fn data_types(opts: &MockOptions) -> Vec<i64> {
    opts.ws_commands
        .lock()
        .unwrap()
        .iter()
        .map(|c| {
            let v: serde_json::Value = serde_json::from_str(c).expect("a command is JSON");
            v["dataType"].as_i64().expect("a command has dataType")
        })
        .collect()
}

#[tokio::test]
async fn a_subscribe_before_the_first_connect_reaches_the_broker_once() {
    let opts = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&opts)).await;
    let http = BcsHttpClient::new(BcsConfig::for_mock("mock-refresh-token", &server.http_base))
        .expect("client");
    let (tx, _rx) = kanal::bounded_async::<IncomingMessage>(64);
    let session = WsSession::spawn(BcsWsChannel::MarketData, http, tx);

    // The session has not connected yet: the command waits in the restore list.
    session
        .subscribe(MarketDataSubscribe::order_book(sber(), 5))
        .await
        .expect("subscribe");
    wait_until("the session connects", || session.is_connected()).await;

    // A second command is the fence. Commands arrive in the order they were sent, so if the first one
    // had been sent twice, the second copy would come before this one.
    session
        .subscribe(MarketDataSubscribe::quotes(sber()))
        .await
        .expect("subscribe");
    wait_until("two commands reach the broker", || {
        opts.ws_commands.lock().unwrap().len() >= 2
    })
    .await;
    assert_eq!(
        data_types(&opts),
        vec![0, 3],
        "the order book once, then the quotes"
    );
}

#[tokio::test]
async fn a_subscribe_after_a_reconnect_is_restored_once() {
    let opts = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&opts)).await;
    let http = BcsHttpClient::new(BcsConfig::for_mock("mock-refresh-token", &server.http_base))
        .expect("client");
    let (tx, rx) = kanal::bounded_async::<IncomingMessage>(64);
    let options = WsOptions {
        announce_connects: true,
        reconnect_min: Duration::from_millis(10),
        ..WsOptions::default()
    };
    // The imitation closes the socket after the first order book it sends.
    opts.disconnect_after_data_frames
        .store(1, std::sync::atomic::Ordering::Relaxed);
    let session = WsSession::spawn_with(BcsWsChannel::MarketData, http, tx, options);
    session
        .subscribe(MarketDataSubscribe::order_book(sber(), 5))
        .await
        .expect("subscribe");

    // Two connects: the command was sent once at each, not more.
    let mut connects = 0;
    while connects < 2 {
        let msg = tokio::time::timeout(Duration::from_secs(10), rx.recv())
            .await
            .expect("a message in time")
            .expect("the channel is open");
        if matches!(msg, IncomingMessage::Reconnected) {
            connects += 1;
        }
    }
    wait_until("the order book is asked again", || {
        opts.ws_commands.lock().unwrap().len() >= 2
    })
    .await;
    assert_eq!(data_types(&opts)[..2], [0, 0], "one command per connect");
}

#[tokio::test]
async fn a_dropped_session_that_never_connected_stops_trying() {
    // Nothing listens on this port: the session cannot even fetch a token.
    let http = BcsHttpClient::new(BcsConfig::for_mock(
        "mock-refresh-token",
        "http://127.0.0.1:1",
    ))
    .expect("client");
    let (tx, rx) = kanal::bounded_async::<IncomingMessage>(8);
    let options = WsOptions {
        reconnect_min: Duration::from_millis(10),
        reconnect_max: Duration::from_millis(20),
        ..WsOptions::default()
    };
    let session = WsSession::spawn_with(BcsWsChannel::MarketData, http, tx, options);
    // Let it fail a few times.
    for _ in 0..3 {
        tokio::task::yield_now().await;
    }
    assert!(!session.is_connected());
    drop(session);

    // The task held the only sender of the channel: when it ends, the channel is closed.
    let closed = tokio::time::timeout(Duration::from_secs(10), rx.recv()).await;
    assert!(
        matches!(closed, Ok(Err(_))),
        "the session task is still running after the drop: {closed:?}"
    );
}

#[tokio::test]
async fn a_dropped_connected_session_stops() {
    let server = MockServer::start(Arc::new(MockOptions::default())).await;
    let http = BcsHttpClient::new(BcsConfig::for_mock("mock-refresh-token", &server.http_base))
        .expect("client");
    let (tx, rx) = kanal::bounded_async::<IncomingMessage>(8);
    let session = WsSession::spawn(BcsWsChannel::MarketData, http, tx);
    wait_until("the session connects", || session.is_connected()).await;
    drop(session);
    let closed = tokio::time::timeout(Duration::from_secs(10), rx.recv()).await;
    assert!(
        matches!(closed, Ok(Err(_))),
        "the session task is still running after the drop: {closed:?}"
    );
}
