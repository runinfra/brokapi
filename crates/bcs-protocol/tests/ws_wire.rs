//! The WebSocket session against a server written for the test: it can refuse the upgrade, send what the
//! broker's server may send (a ping, a binary frame, a close), and count what it gets.

mod common;

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use brokapi_bcs_protocol::config::BcsConfig;
use brokapi_bcs_protocol::http::client::{AUTH_TOKEN_PATH, BcsHttpClient};
use brokapi_bcs_protocol::models::messages::IncomingMessage;
use brokapi_bcs_protocol::ws::client::BcsWsChannel;
use brokapi_bcs_protocol::ws::models::MarketDataSubscribe;
use brokapi_bcs_protocol::ws::session::{WsOptions, WsSession};
use brokapi_mocks::time::wait_until;
use futures_util::{SinkExt, StreamExt};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message;

/// What the server does with the n-th WebSocket connection (counted from 1).
#[derive(Clone, Copy, Debug, PartialEq)]
enum Plan {
    /// Answer the upgrade with 403 Forbidden.
    Refuse,
    /// Accept, send a ping, a binary frame, then a text frame, then stay silent.
    Chatty,
    /// Accept, send a text frame, then close.
    CloseAfterOne,
    /// Accept and keep quiet.
    Quiet,
}

struct Wire {
    http_base: String,
    tokens: Arc<AtomicU32>,
    upgrades: Arc<AtomicU32>,
    received: Arc<Mutex<Vec<String>>>,
    pongs: Arc<AtomicU32>,
}

async fn start(plan: impl Fn(u32) -> Plan + Send + Sync + 'static) -> Wire {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let wire = Wire {
        http_base: format!("http://{}", listener.local_addr().unwrap()),
        tokens: Arc::default(),
        upgrades: Arc::default(),
        received: Arc::default(),
        pongs: Arc::default(),
    };
    let (tokens, upgrades, received, pongs) = (
        Arc::clone(&wire.tokens),
        Arc::clone(&wire.upgrades),
        Arc::clone(&wire.received),
        Arc::clone(&wire.pongs),
    );
    let plan = Arc::new(plan);
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            let (tokens, upgrades, received, pongs, plan) = (
                Arc::clone(&tokens),
                Arc::clone(&upgrades),
                Arc::clone(&received),
                Arc::clone(&pongs),
                Arc::clone(&plan),
            );
            tokio::spawn(async move {
                // Look at the request without taking it off the socket.
                let mut head = vec![0u8; 2048];
                let n = stream.peek(&mut head).await.unwrap_or(0);
                let head = String::from_utf8_lossy(&head[..n]).to_ascii_lowercase();
                if head.contains("upgrade: websocket") {
                    let n = upgrades.fetch_add(1, Ordering::SeqCst) + 1;
                    match plan(n) {
                        Plan::Refuse => {
                            let _ = stream
                                .write_all(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                                .await;
                        }
                        other => serve_ws(stream, other, received, pongs).await,
                    }
                } else {
                    tokens.fetch_add(1, Ordering::SeqCst);
                    let mut buf = vec![0u8; 4096];
                    let _ = tokio::io::AsyncReadExt::read(&mut stream, &mut buf).await;
                    let body = r#"{"access_token":"tok","expires_in":86400,"refresh_expires_in":1,"token_type":"bearer"}"#;
                    let reply = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = stream.write_all(reply.as_bytes()).await;
                }
            });
        }
    });
    wire
}

async fn serve_ws(
    stream: TcpStream,
    plan: Plan,
    received: Arc<Mutex<Vec<String>>>,
    pongs: Arc<AtomicU32>,
) {
    let Ok(mut ws) = tokio_tungstenite::accept_async(stream).await else {
        return;
    };
    match plan {
        Plan::Chatty => {
            let _ = ws.send(Message::Ping(vec![1, 2].into())).await;
            let _ = ws.send(Message::Binary(vec![0, 1].into())).await;
            let _ = ws.send(Message::Text(r#"{"hello":"world"}"#.into())).await;
        }
        Plan::CloseAfterOne => {
            let _ = ws.send(Message::Text(r#"{"hello":"again"}"#.into())).await;
            let _ = ws.send(Message::Close(None)).await;
            return;
        }
        _ => {}
    }
    while let Some(Ok(msg)) = ws.next().await {
        match msg {
            Message::Text(t) => received.lock().unwrap().push(t.to_string()),
            Message::Pong(_) => {
                pongs.fetch_add(1, Ordering::SeqCst);
            }
            _ => {}
        }
    }
}

fn session(
    wire: &Wire,
    channel: BcsWsChannel,
    options: WsOptions,
    cap: usize,
) -> (
    WsSession,
    kanal::AsyncReceiver<IncomingMessage>,
    BcsHttpClient,
) {
    let http = BcsHttpClient::new(BcsConfig::for_mock("refresh", &wire.http_base)).unwrap();
    let (tx, rx) = kanal::bounded_async::<IncomingMessage>(cap);
    (
        WsSession::spawn_with(channel, http.clone(), tx, options),
        rx,
        http,
    )
}

fn fast() -> WsOptions {
    WsOptions {
        announce_connects: true,
        reconnect_min: Duration::from_millis(10),
        reconnect_max: Duration::from_millis(20),
    }
}

async fn next(rx: &kanal::AsyncReceiver<IncomingMessage>) -> IncomingMessage {
    tokio::time::timeout(Duration::from_secs(10), rx.recv())
        .await
        .expect("a message in time")
        .expect("the channel is open")
}

#[tokio::test]
async fn a_refused_upgrade_renews_the_token_and_the_session_tries_again() {
    let wire = start(|n| if n <= 2 { Plan::Refuse } else { Plan::Quiet }).await;
    let (session, _rx, _http) = session(&wire, BcsWsChannel::MarketData, fast(), 16);
    wait_until("the third upgrade is accepted", || session.is_connected()).await;
    assert_eq!(wire.upgrades.load(Ordering::SeqCst), 3);
    // The first token, and a forced renewal after each of the two 403.
    assert_eq!(wire.tokens.load(Ordering::SeqCst), 3);
    assert_eq!(session.channel(), BcsWsChannel::MarketData);
}

#[tokio::test]
async fn pings_binary_frames_and_text_are_handled_and_the_text_is_parsed() {
    let wire = start(|_| Plan::Chatty).await;
    let (session, rx, _http) = session(&wire, BcsWsChannel::MarketData, fast(), 16);
    assert!(matches!(next(&rx).await, IncomingMessage::Reconnected));
    // The text frame is handed on (as a frame nobody knows: kept, not dropped).
    assert!(matches!(next(&rx).await, IncomingMessage::Unknown(_)));
    // The ping of the server was answered with a pong.
    wait_until("the pong arrives", || {
        wire.pongs.load(Ordering::SeqCst) >= 1
    })
    .await;
    assert!(session.is_connected());
}

#[tokio::test]
async fn a_close_by_the_server_is_a_reconnect_with_a_notice_each_time() {
    let wire = start(|_| Plan::CloseAfterOne).await;
    let (_session, rx, _http) = session(&wire, BcsWsChannel::MarketData, fast(), 64);
    let mut connects = 0;
    while connects < 3 {
        if matches!(next(&rx).await, IncomingMessage::Reconnected) {
            connects += 1;
        }
    }
    assert!(wire.upgrades.load(Ordering::SeqCst) >= 3);
}

#[tokio::test]
async fn a_subscribe_on_a_channel_that_has_none_is_ignored_and_unsubscribe_reaches_the_broker() {
    let wire = start(|_| Plan::Quiet).await;
    // The portfolio channel takes no subscriptions: the command is dropped, nothing reaches the server.
    let (portfolio, _rx, _http) = session(&wire, BcsWsChannel::Portfolio, fast(), 16);
    wait_until("the portfolio session connects", || {
        portfolio.is_connected()
    })
    .await;
    let keys = vec![brokapi_bcs_protocol::models::instrument::InstrumentKey::new("SBER", "TQBR")];
    let refused = portfolio
        .subscribe(MarketDataSubscribe::quotes(keys.clone()))
        .await
        .unwrap_err();
    assert!(
        matches!(
            refused,
            brokapi_bcs_protocol::error::BcsApiError::SubscriptionError { .. }
        ),
        "{refused:?}"
    );

    // The market data channel takes both.
    let (md, _rx2, _http2) = session(&wire, BcsWsChannel::MarketData, fast(), 16);
    wait_until("the market data session connects", || md.is_connected()).await;
    md.subscribe(MarketDataSubscribe::quotes(keys.clone()))
        .await
        .unwrap();
    md.unsubscribe(MarketDataSubscribe::quotes(keys))
        .await
        .unwrap();
    wait_until("both commands reach the server", || {
        wire.received.lock().unwrap().len() >= 2
    })
    .await;
    let got = wire.received.lock().unwrap().clone();
    // Nothing else reached it: the portfolio channel sent nothing.
    assert_eq!(got.len(), 2, "{got:?}");
    let types: Vec<i64> = got
        .iter()
        .map(|c| {
            serde_json::from_str::<serde_json::Value>(c).unwrap()["subscribeType"]
                .as_i64()
                .unwrap()
        })
        .collect();
    assert_eq!(types, vec![0, 1], "subscribe then unsubscribe");
}

#[tokio::test]
async fn a_full_downstream_drops_frames_and_counts_them() {
    let wire = start(|_| Plan::Chatty).await;
    // Room for one message and nobody reads: the connect notice fills it, the text frame is dropped.
    let (session, rx, _http) = session(&wire, BcsWsChannel::MarketData, fast(), 1);
    wait_until("a frame is dropped", || session.dropped_events() >= 1).await;
    assert!(matches!(
        rx.recv().await.unwrap(),
        IncomingMessage::Reconnected
    ));
}

#[tokio::test]
async fn a_session_whose_reader_is_gone_ends() {
    let wire = start(|_| Plan::Chatty).await;
    let (session, rx, _http) = session(&wire, BcsWsChannel::MarketData, fast(), 4);
    assert!(matches!(next(&rx).await, IncomingMessage::Reconnected));
    drop(rx);
    // The next frame meets a closed channel: the session stops and says it is down.
    wait_until("the session stops", || !session.is_connected()).await;
}

#[tokio::test]
async fn the_number_of_subscriptions_of_a_session_is_limited() {
    let wire = start(|_| Plan::Quiet).await;
    let (md, _rx, _http) = session(&wire, BcsWsChannel::MarketData, fast(), 16);
    let mut refused = None;
    for n in 0..1000u32 {
        let key = vec![
            brokapi_bcs_protocol::models::instrument::InstrumentKey::new(format!("T{n}"), "TQBR"),
        ];
        if let Err(e) = md.subscribe(MarketDataSubscribe::quotes(key)).await {
            refused = Some((n, e));
            break;
        }
    }
    let (n, err) = refused.expect("a limit exists");
    assert!((50..=500).contains(&n), "refused at {n}");
    assert!(err.to_string().contains("limit"), "{err}");
}

#[test]
fn the_token_path_is_the_one_of_the_docs() {
    assert!(AUTH_TOKEN_PATH.ends_with("/protocol/openid-connect/token"));
}
