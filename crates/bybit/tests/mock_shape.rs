//! The imitation answers with the same fields and types as the recorded answers of the exchange.
//!
//! The imitation and the connector were written from one reading of the documentation. The recordings in
//! `brokapi-bybit-protocol/tests/fixtures` are what the exchange itself sent (public endpoints and streams, recorded on
//! 2026-10-23), so a field the connector reads and the imitation forgot, or one the imitation invented, shows here.

mod mock_server;

use std::sync::Arc;
use std::time::Duration;

use brokapi_bybit_protocol::config::BybitConfig;
use brokapi_bybit_protocol::http::client::BybitHttpClient;
use brokapi_bybit_protocol::ws::models::WsIncoming;
use brokapi_bybit_protocol::ws::session::{WsOptions, WsSession, WsStream};
use brokapi_mocks::live::shape::{Shape, compare};
use mock_server::{DEFAULT_KEY, MockOptions, MockServer, SECRET};
use serde_json::{Value, json};

fn recording(rel: &str) -> Value {
    let path = format!(
        "{}/../bybit-protocol/tests/fixtures/{rel}",
        env!("CARGO_MANIFEST_DIR")
    );
    serde_json::from_str(&std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path}: {e}")))
        .unwrap()
}

fn same_shape(what: &str, live: &Value, mock: &Value) {
    let problems = compare(&Shape::of(live), &Shape::of(mock));
    assert!(
        problems.is_empty(),
        "{what}: the imitation differs from the exchange:\n{}",
        problems
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n")
    );
}

#[tokio::test]
async fn the_public_answers_of_the_imitation_have_the_fields_of_the_exchange() {
    let server = MockServer::start(Arc::new(MockOptions::default())).await;
    let http = BybitHttpClient::new(BybitConfig::for_mock(
        DEFAULT_KEY,
        SECRET,
        &server.http_base,
    ))
    .unwrap();
    let get = |path: &'static str, query: Vec<(&'static str, String)>| {
        let http = http.clone();
        async move { http.get_public::<Value>(path, &query).await.unwrap() }
    };
    let linear = || ("category", "linear".to_string());
    let symbol = || ("symbol", "BTCUSDT".to_string());

    same_shape(
        "server time",
        &recording("rest/server_time.json")["result"],
        &get("/v5/market/time", vec![]).await,
    );
    same_shape(
        "instrument",
        &recording("rest/instrument_btcusdt.json")["result"],
        &get("/v5/market/instruments-info", vec![linear(), symbol()]).await,
    );
    same_shape(
        "order book",
        &recording("rest/order_book.json")["result"],
        &get(
            "/v5/market/orderbook",
            vec![linear(), symbol(), ("limit", "5".into())],
        )
        .await,
    );
    same_shape(
        "ticker",
        &recording("rest/ticker.json")["result"],
        &get("/v5/market/tickers", vec![linear(), symbol()]).await,
    );
    same_shape(
        "recent trades",
        &recording("rest/recent_trade.json")["result"],
        &get(
            "/v5/market/recent-trade",
            vec![linear(), symbol(), ("limit", "3".into())],
        )
        .await,
    );
    same_shape(
        "candles",
        &recording("rest/kline.json")["result"],
        &get(
            "/v5/market/kline",
            vec![
                linear(),
                symbol(),
                ("interval", "1".into()),
                ("limit", "3".into()),
            ],
        )
        .await,
    );
}

#[tokio::test]
async fn the_streams_of_the_imitation_send_the_frames_of_the_exchange() {
    let server = MockServer::start(Arc::new(MockOptions::default())).await;
    let http = BybitHttpClient::new(BybitConfig::for_mock(
        DEFAULT_KEY,
        SECRET,
        &server.http_base,
    ))
    .unwrap();
    let (tx, rx) = kanal::bounded_async(64);
    let ws = WsSession::spawn(http, WsStream::Public, tx, WsOptions::default());
    for (i, topic) in [
        "orderbook.50.BTCUSDT",
        "orderbook.1.BTCUSDT",
        "publicTrade.BTCUSDT",
        "kline.1.BTCUSDT",
    ]
    .iter()
    .enumerate()
    {
        ws.subscribe(&format!("r{i}"), topic).await.unwrap();
    }
    let mut seen = 0;
    let mut acked = false;
    while seen < 4 {
        let frame = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("a frame")
            .unwrap();
        match frame {
            WsIncoming::Ack { success, .. } => {
                assert!(success);
                acked = true;
            }
            WsIncoming::Data {
                topic,
                kind,
                ts,
                cts,
                data,
            } => {
                seen += 1;
                let (file, has_cts) = match topic.as_str() {
                    "orderbook.50.BTCUSDT" => ("ws/orderbook50_snapshot.json", true),
                    "orderbook.1.BTCUSDT" => ("ws/orderbook1_snapshot.json", true),
                    "publicTrade.BTCUSDT" => ("ws/public_trade.json", false),
                    _ => ("ws/kline.json", false),
                };
                let live = recording(file);
                same_shape(&topic, &live["data"], &data);
                assert_eq!(live["type"], "snapshot");
                assert!(matches!(
                    kind,
                    brokapi_bybit_protocol::ws::models::DataKind::Snapshot
                ));
                assert!(ts > 0);
                assert_eq!(
                    cts > 0,
                    has_cts,
                    "{topic}: the engine time is on the book frames only"
                );
                // The envelope of the frame has the fields of the recording too.
                let mut envelope =
                    json!({"topic": topic, "type": "snapshot", "ts": ts, "data": null});
                if has_cts {
                    envelope["cts"] = json!(cts);
                }
                let mut want = live.clone();
                want["data"] = Value::Null;
                same_shape(&format!("{topic} envelope"), &want, &envelope);
            }
            _ => {}
        }
    }
    assert!(acked);
    // The answers have the fields of the recorded ones.
    let live_ack = recording("ws/ack_subscribe_ok.json");
    let mock_ack =
        json!({"success": true, "ret_msg": "", "conn_id": "x", "req_id": "r0", "op": "subscribe"});
    same_shape("ack", &live_ack, &mock_ack);
}
