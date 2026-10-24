//! The messages of the WebSocket streams: the requests we send and the frames that come back.
//!
//! Every request is `{"op": ..., "req_id": ..., "args": [...]}`. The exchange answers a subscription once with
//! `{"success": true/false, "ret_msg": ..., "op": "subscribe", "req_id": ...}`, then data frames arrive with the
//! `topic` of the subscription until it is cancelled.

use serde::Serialize;
use serde_json::{Value, json};

/// The path of the public stream of a category.
pub fn public_path(category: &str) -> String {
    format!("/v5/public/{category}")
}

/// The path of the private stream.
pub const PRIVATE_PATH: &str = "/v5/private";

/// A request to the stream.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct WsRequest {
    /// `subscribe`, `unsubscribe`, `auth` or `ping`.
    pub op: String,
    /// An id of ours, repeated in the answer. Empty when none.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub req_id: String,
    /// The topics (or the login arguments).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<Value>,
}

impl WsRequest {
    /// Subscribes to one topic, for example `orderbook.50.BTCUSDT`.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_bybit_protocol::ws::models::WsRequest;
    ///
    /// let r = WsRequest::subscribe("7", "orderbook.1.BTCUSDT");
    /// assert_eq!(r.to_json(), r#"{"op":"subscribe","req_id":"7","args":["orderbook.1.BTCUSDT"]}"#);
    /// ```
    pub fn subscribe(req_id: &str, topic: &str) -> Self {
        Self {
            op: "subscribe".into(),
            req_id: req_id.into(),
            args: vec![json!(topic)],
        }
    }

    /// Cancels the subscription to one topic.
    pub fn unsubscribe(req_id: &str, topic: &str) -> Self {
        Self {
            op: "unsubscribe".into(),
            req_id: req_id.into(),
            args: vec![json!(topic)],
        }
    }

    /// The heartbeat the exchange asks for every 20 seconds.
    pub fn ping() -> Self {
        Self {
            op: "ping".into(),
            req_id: String::new(),
            args: Vec::new(),
        }
    }

    /// The login of the private stream: the key, the `expires` time (milliseconds) and the signature.
    pub fn auth(api_key: &str, api_secret: &str, expires_ms: i64) -> Self {
        let [key, expires, signature] = crate::sign::ws_auth_args(api_key, api_secret, expires_ms);
        Self {
            op: "auth".into(),
            req_id: String::new(),
            // The expires time is a number in the login, the key and the signature are texts.
            args: vec![
                json!(key),
                json!(expires.parse::<i64>().unwrap_or(expires_ms)),
                json!(signature),
            ],
        }
    }

    /// The text that goes on the wire.
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }
}

/// How a data frame relates to the earlier ones.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataKind {
    /// The whole state: the order book is replaced.
    Snapshot,
    /// A change to apply to the state held.
    Delta,
    /// A frame of a private stream: it has no `type`.
    Event,
}

/// A frame from the stream.
#[derive(Debug, Clone, PartialEq)]
pub enum WsIncoming {
    /// A connection was made (and, for the private stream, logged in). Sent by the session itself, before the data of
    /// the restored subscriptions.
    Connected,
    /// The answer to a request.
    Ack {
        /// `subscribe`, `unsubscribe` or `auth`.
        op: String,
        /// The `req_id` of the request, when it had one.
        req_id: Option<String>,
        /// Whether the exchange accepted it.
        success: bool,
        /// The reason when it did not.
        message: String,
    },
    /// Data of a topic.
    Data {
        /// The topic, `orderbook.50.BTCUSDT` or `order`.
        topic: String,
        /// Snapshot, delta or event.
        kind: DataKind,
        /// The time the exchange made the frame, milliseconds (`ts` of a public frame, `creationTime` of a private one).
        ts: i64,
        /// The time of the matching engine, milliseconds, when the frame has it.
        cts: i64,
        /// The `data` of the frame.
        data: Value,
    },
    /// The answer to a heartbeat.
    Pong,
    /// Something else: kept as text.
    Unknown(String),
}

/// Reads one text frame.
///
/// # Examples
///
/// ```
/// use brokapi_bybit_protocol::ws::models::{DataKind, WsIncoming, parse_incoming};
///
/// let ok = parse_incoming(r#"{"success":true,"ret_msg":"","conn_id":"x","req_id":"q1","op":"subscribe"}"#);
/// assert!(matches!(ok, WsIncoming::Ack { success: true, .. }));
///
/// let data = parse_incoming(r#"{"topic":"orderbook.1.BTCUSDT","ts":1,"type":"snapshot","data":{"s":"BTCUSDT"},"cts":2}"#);
/// assert!(matches!(data, WsIncoming::Data { kind: DataKind::Snapshot, cts: 2, .. }));
///
/// assert_eq!(parse_incoming(r#"{"op":"pong","args":["1"]}"#), WsIncoming::Pong);
/// ```
pub fn parse_incoming(text: &str) -> WsIncoming {
    let Ok(v) = serde_json::from_str::<Value>(text) else {
        return WsIncoming::Unknown(text.to_string());
    };
    let op = v.get("op").and_then(Value::as_str).unwrap_or("");
    let ret_msg = v.get("ret_msg").and_then(Value::as_str).unwrap_or("");
    if op == "pong" || ret_msg == "pong" {
        return WsIncoming::Pong;
    }
    if let (Some(topic), Some(data)) = (v.get("topic").and_then(Value::as_str), v.get("data")) {
        let kind = match v.get("type").and_then(Value::as_str) {
            Some("snapshot") => DataKind::Snapshot,
            Some("delta") => DataKind::Delta,
            _ => DataKind::Event,
        };
        let ts = v
            .get("ts")
            .or_else(|| v.get("creationTime"))
            .and_then(Value::as_i64)
            .unwrap_or(0);
        return WsIncoming::Data {
            topic: topic.to_string(),
            kind,
            ts,
            cts: v.get("cts").and_then(Value::as_i64).unwrap_or(0),
            data: data.clone(),
        };
    }
    if !op.is_empty()
        && let Some(success) = v.get("success").and_then(Value::as_bool)
    {
        return WsIncoming::Ack {
            op: op.to_string(),
            req_id: v
                .get("req_id")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .map(str::to_string),
            success,
            message: ret_msg.to_string(),
        };
    }
    WsIncoming::Unknown(text.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_login_carries_the_expires_time_as_a_number() {
        let r = WsRequest::auth("key", "secret", 1_700_000_000_000);
        let v: Value = serde_json::from_str(&r.to_json()).unwrap();
        assert_eq!(v["op"], "auth");
        assert_eq!(v["args"][0], "key");
        assert_eq!(v["args"][1], 1_700_000_000_000_i64);
        assert_eq!(v["args"][2].as_str().unwrap().len(), 64);
        assert!(v.get("req_id").is_none());
    }

    #[test]
    fn a_failed_answer_keeps_the_reason() {
        let f = parse_incoming(
            r#"{"success":false,"ret_msg":"error:handler not found,topic:orderbook.7.BTCUSDT","req_id":"b","op":"subscribe"}"#,
        );
        match f {
            WsIncoming::Ack {
                success,
                message,
                req_id,
                ..
            } => {
                assert!(!success);
                assert!(message.contains("handler not found"));
                assert_eq!(req_id.as_deref(), Some("b"));
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn a_private_frame_is_an_event_and_a_pong_of_the_public_stream_is_a_pong() {
        let f = parse_incoming(r#"{"id":"1","topic":"order","creationTime":5,"data":[]}"#);
        assert!(matches!(
            f,
            WsIncoming::Data {
                kind: DataKind::Event,
                ts: 5,
                ..
            }
        ));
        assert_eq!(
            parse_incoming(
                r#"{"success":true,"ret_msg":"pong","conn_id":"c","req_id":"","op":"ping"}"#
            ),
            WsIncoming::Pong
        );
        assert!(matches!(parse_incoming("not json"), WsIncoming::Unknown(_)));
    }
}
