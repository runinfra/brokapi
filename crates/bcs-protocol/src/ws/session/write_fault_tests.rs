use super::*;
use brokapi_mocks::io::{FaultyWrite, WriteFault};
use brokapi_mocks::time::{jump, wait_until};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::sync::atomic::AtomicUsize;
use tokio::io::{AsyncReadExt, DuplexStream};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::protocol::Role;

type Wire = WebSocketStream<FaultyWrite<DuplexStream>>;
type Peer = WebSocketStream<DuplexStream>;
const PAUSE: Duration = Duration::from_secs(100);

struct Harness {
    session: WsSession,
    events: kanal::AsyncReceiver<IncomingMessage>,
    connections: mpsc::UnboundedSender<Wire>,
    opens: Arc<AtomicUsize>,
    _server: brokapi_mocks::bcs::MockServer,
}

async fn pair() -> (Wire, WriteFault, Peer) {
    let (client, server) = tokio::io::duplex(4096);
    let (client, fault) = FaultyWrite::new(client);
    (
        WebSocketStream::from_raw_socket(client, Role::Client, None).await,
        fault,
        WebSocketStream::from_raw_socket(server, Role::Server, None).await,
    )
}

async fn harness() -> Harness {
    let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
    let http = BcsHttpClient::new(crate::config::BcsConfig::for_mock(
        "mock-refresh-token",
        &server.http_base,
    ))
    .unwrap();
    // Token acquisition uses real HTTP before time jumps. Later reads use the cache.
    http.get_access_token().await.unwrap();
    let (tx, events) = kanal::bounded_async(32);
    let (connections, pending) = mpsc::unbounded_channel();
    let pending = Arc::new(tokio::sync::Mutex::new(pending));
    let opens = Arc::new(AtomicUsize::new(0));
    let calls = Arc::clone(&opens);
    let options = WsOptions {
        announce_connects: true,
        reconnect_min: PAUSE,
        reconnect_max: PAUSE * 4,
    };
    let session = WsSession::spawn_using(
        BcsWsChannel::MarketData,
        http,
        tx,
        options,
        move |_url, _token| {
            let pending = Arc::clone(&pending);
            calls.fetch_add(1, Ordering::SeqCst);
            async move {
                Ok(pending
                    .lock()
                    .await
                    .recv()
                    .await
                    .expect("a planned connection"))
            }
        },
    );
    Harness {
        session,
        events,
        connections,
        opens,
        _server: server,
    }
}

fn subscription(ticker: &str) -> MarketDataSubscribe {
    MarketDataSubscribe::quotes(vec![crate::models::instrument::InstrumentKey::new(
        ticker, "TQBR",
    )])
}

async fn unsubscribe(session: &WsSession) {
    session.unsubscribe(subscription("SBER")).await.unwrap();
}

async fn notice(h: &Harness) {
    let event = tokio::time::timeout(Duration::from_secs(5), h.events.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(event, IncomingMessage::Reconnected), "{event:?}");
}

async fn frame(peer: &mut Peer) -> Message {
    tokio::time::timeout(Duration::from_secs(5), peer.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
}

async fn request(peer: &mut Peer) -> Value {
    let Message::Text(text) = frame(peer).await else {
        panic!("a subscription text frame")
    };
    serde_json::from_str(&text).unwrap()
}

fn ticker(request: &Value) -> &str {
    request["instruments"][0]["ticker"].as_str().unwrap()
}

async fn restored(h: &Harness, peer: &mut Peer, tickers: &[&str]) {
    wait_until("the restored connection is up", || h.session.is_connected()).await;
    // A pong is a later event than the restoration writes, so absence of subscriptions needs no sleep.
    peer.send(Message::Ping(vec![9].into())).await.unwrap();
    let mut got = Vec::new();
    loop {
        match frame(peer).await {
            Message::Text(text) => {
                let request: Value = serde_json::from_str(&text).unwrap();
                got.push(ticker(&request).to_string());
            }
            Message::Pong(data) => {
                assert_eq!(data.as_ref(), &[9]);
                break;
            }
            other => panic!("unexpected frame after restoration: {other:?}"),
        }
    }
    assert_eq!(got, tickers);
    peer.send(Message::Text(
        json!({"write_recovery": true}).to_string().into(),
    ))
    .await
    .unwrap();
    let event = tokio::time::timeout(Duration::from_secs(5), h.events.recv())
        .await
        .unwrap()
        .unwrap();
    let IncomingMessage::Unknown(text) = event else {
        panic!("data after reconnect: {event:?}")
    };
    assert_eq!(
        serde_json::from_str::<Value>(&text).unwrap(),
        json!({"write_recovery":true})
    );
}

#[derive(Clone, Copy)]
enum Cause {
    Subscribe,
    Unsubscribe,
    Ping,
    Pong,
}

async fn partial_write_reconnects(cause: Cause) {
    let h = harness().await;
    h.session.subscribe(subscription("SBER")).await.unwrap();
    let (wire, fault, mut peer) = pair().await;
    h.connections.send(wire).unwrap();
    notice(&h).await;
    let first = request(&mut peer).await;
    assert_eq!(ticker(&first), "SBER");
    wait_until("the first connection is up", || h.session.is_connected()).await;
    fault.fail_after(1);
    let (opcode, expected): (u8, &[&str]) = match cause {
        Cause::Subscribe => {
            h.session.subscribe(subscription("GAZP")).await.unwrap();
            (1, &["SBER", "GAZP"])
        }
        Cause::Unsubscribe => {
            unsubscribe(&h.session).await;
            (1, &[])
        }
        Cause::Ping => {
            jump(Duration::from_secs(30)).await;
            (9, &["SBER"])
        }
        Cause::Pong => {
            peer.send(Message::Ping(vec![1, 2].into())).await.unwrap();
            (10, &["SBER"])
        }
    };
    wait_until("the partial frame meets the injected write error", || {
        fault.failures() == 1
    })
    .await;
    assert_eq!(fault.written(), 1);
    let mut prefix = [0];
    peer.get_mut().read_exact(&mut prefix).await.unwrap();
    assert_eq!(
        prefix[0] & 0x0f,
        opcode,
        "the failure must interrupt the intended frame"
    );
    wait_until("the failed connection is down", || {
        !h.session.is_connected()
    })
    .await;
    assert_eq!(
        h.opens.load(Ordering::SeqCst),
        1,
        "do not reconnect before the backoff"
    );
    let (next, _, mut next_peer) = pair().await;
    h.connections.send(next).unwrap();
    jump(PAUSE).await;
    notice(&h).await;
    restored(&h, &mut next_peer, expected).await;
    assert_eq!(h.opens.load(Ordering::SeqCst), 2);
    assert_eq!(
        fault.failures(),
        1,
        "no write retry on the failed connection"
    );
}

#[tokio::test]
async fn a_partial_subscription_write_reconnects_and_restores_both_subscriptions() {
    partial_write_reconnects(Cause::Subscribe).await;
}

#[tokio::test]
async fn a_partial_unsubscribe_write_does_not_restore_the_cancelled_subscription() {
    partial_write_reconnects(Cause::Unsubscribe).await;
}

#[tokio::test]
async fn a_partial_ping_write_reconnects_and_restores_the_subscription() {
    partial_write_reconnects(Cause::Ping).await;
}

#[tokio::test]
async fn a_partial_pong_write_reconnects_and_restores_the_subscription() {
    partial_write_reconnects(Cause::Pong).await;
}

#[tokio::test]
async fn failed_restoration_writes_stay_down_and_back_off_before_each_reconnect() {
    let h = harness().await;
    h.session.subscribe(subscription("SBER")).await.unwrap();
    let (first, one, _peer_one) = pair().await;
    let (second, two, _peer_two) = pair().await;
    let (third, _, mut peer_three) = pair().await;
    one.fail_after(1);
    two.fail_after(1);
    h.connections.send(first).unwrap();
    h.connections.send(second).unwrap();
    h.connections.send(third).unwrap();
    notice(&h).await;
    wait_until("the first restoration write fails", || one.failures() == 1).await;
    assert_eq!(
        h.opens.load(Ordering::SeqCst),
        1,
        "restoration failures must respect backoff"
    );
    assert!(
        !h.session.is_connected(),
        "a failed restoration is never UP"
    );
    jump(PAUSE).await;
    notice(&h).await;
    wait_until("the second restoration write fails", || two.failures() == 1).await;
    assert!(!h.session.is_connected());
    assert_eq!(h.opens.load(Ordering::SeqCst), 2);
    jump(PAUSE).await;
    assert_eq!(
        h.opens.load(Ordering::SeqCst),
        2,
        "the second pause must double"
    );
    jump(PAUSE).await;
    notice(&h).await;
    restored(&h, &mut peer_three, &["SBER"]).await;
    assert_eq!(h.opens.load(Ordering::SeqCst), 3);
    assert_eq!((one.written(), two.written()), (1, 1));
}
