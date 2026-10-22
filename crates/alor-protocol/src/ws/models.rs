//! The messages of the data channel `/ws`: the subscription requests and the frames that come back.
//!
//! Every request carries `opcode` (what to do), `guid` (an id of ours, repeated in every answer to it) and
//! `token` (the access token: the channel checks it on every message, so a session writes a fresh one into each
//! request when it sends it). A subscription is answered once with a status frame (`httpCode` 200), then data
//! frames arrive with the same `guid` until the subscription is cancelled with `unsubscribe`.

use serde::Serialize;
use serde_json::{Map, Value, json};

/// The path of the data channel on the WebSocket host.
pub const WS_PATH: &str = "/ws";
/// The path of the trading channel.
pub const CWS_PATH: &str = "/cws";

/// A subscription request, without the token (the session adds it when it sends).
///
/// Build one with the constructors ([`WsSubscribe::order_book`] and so on, one per opcode of the broker). The
/// `guid` is yours: it names the subscription for `unsubscribe` and marks its data frames.
///
/// # Examples
///
/// ```
/// use brokapi_alor_protocol::ws::models::WsSubscribe;
///
/// let sub = WsSubscribe::order_book("g1", "MOEX", "SBER", Some("TQBR"), 10);
/// let json: serde_json::Value = serde_json::from_str(&sub.to_json("token")).unwrap();
/// assert_eq!(json["opcode"], "OrderBookGetAndSubscribe");
/// assert_eq!(json["guid"], "g1");
/// assert_eq!(json["token"], "token");
/// assert_eq!(json["depth"], 10);
/// assert_eq!(json["format"], "Simple");
/// ```
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct WsSubscribe {
    /// What to subscribe to, in the words of the broker (`OrderBookGetAndSubscribe`, ...).
    pub opcode: String,
    /// The id of this subscription.
    pub guid: String,
    /// The other fields of the request.
    pub params: Map<String, Value>,
}

impl WsSubscribe {
    fn new(opcode: &str, guid: &str, params: Value) -> Self {
        let mut params = params.as_object().cloned().unwrap_or_default();
        params.insert("format".into(), json!("Simple"));
        Self {
            opcode: opcode.to_string(),
            guid: guid.to_string(),
            params,
        }
    }

    fn instrument(
        opcode: &str,
        guid: &str,
        exchange: &str,
        code: &str,
        group: Option<&str>,
        mut extra: Value,
    ) -> Self {
        extra["exchange"] = json!(exchange);
        extra["code"] = json!(code);
        if let Some(g) = group {
            extra["instrumentGroup"] = json!(g);
        }
        Self::new(opcode, guid, extra)
    }

    fn portfolio(
        opcode: &str,
        guid: &str,
        exchange: &str,
        portfolio: &str,
        skip_history: bool,
    ) -> Self {
        Self::new(
            opcode,
            guid,
            json!({"exchange": exchange, "portfolio": portfolio, "skipHistory": skip_history}),
        )
    }

    /// The order book of an instrument (`OrderBookGetAndSubscribe`): a whole book per message, `depth` levels
    /// (20 by default, 50 at most).
    pub fn order_book(
        guid: &str,
        exchange: &str,
        code: &str,
        group: Option<&str>,
        depth: u32,
    ) -> Self {
        Self::instrument(
            "OrderBookGetAndSubscribe",
            guid,
            exchange,
            code,
            group,
            json!({"depth": depth}),
        )
    }

    /// Candles of an instrument (`BarsGetAndSubscribe`). `tf` is a number of seconds or `D`, `W`, `M`, `Y`;
    /// `from` (Unix seconds) is the first candle of the history that comes first; `skip_history` leaves it out.
    pub fn bars(
        guid: &str,
        exchange: &str,
        code: &str,
        group: Option<&str>,
        tf: &str,
        from: i64,
        skip_history: bool,
    ) -> Self {
        Self::instrument(
            "BarsGetAndSubscribe",
            guid,
            exchange,
            code,
            group,
            json!({"tf": tf, "from": from, "skipHistory": skip_history}),
        )
    }

    /// Quotations of an instrument (`QuotesSubscribe`).
    pub fn quotes(guid: &str, exchange: &str, code: &str, group: Option<&str>) -> Self {
        Self::instrument("QuotesSubscribe", guid, exchange, code, group, json!({}))
    }

    /// All trades of the market in an instrument (`AllTradesGetAndSubscribe`).
    pub fn all_trades(guid: &str, exchange: &str, code: &str, group: Option<&str>) -> Self {
        Self::instrument(
            "AllTradesGetAndSubscribe",
            guid,
            exchange,
            code,
            group,
            json!({}),
        )
    }

    /// Changes of the instruments of an exchange (`InstrumentsGetAndSubscribeV2`).
    pub fn instruments(guid: &str, exchange: &str, code: &str, group: Option<&str>) -> Self {
        Self::instrument(
            "InstrumentsGetAndSubscribeV2",
            guid,
            exchange,
            code,
            group,
            json!({}),
        )
    }

    /// Orders of a portfolio (`OrdersGetAndSubscribeV2`). Without `skip_history` the orders of today come first
    /// (frames with `existing: true`), then the changes.
    pub fn orders(guid: &str, exchange: &str, portfolio: &str, skip_history: bool) -> Self {
        Self::portfolio(
            "OrdersGetAndSubscribeV2",
            guid,
            exchange,
            portfolio,
            skip_history,
        )
    }

    /// Stop orders of a portfolio (`StopOrdersGetAndSubscribeV2`).
    pub fn stop_orders(guid: &str, exchange: &str, portfolio: &str, skip_history: bool) -> Self {
        Self::portfolio(
            "StopOrdersGetAndSubscribeV2",
            guid,
            exchange,
            portfolio,
            skip_history,
        )
    }

    /// Trades (fills) of a portfolio (`TradesGetAndSubscribeV2`).
    pub fn trades(guid: &str, exchange: &str, portfolio: &str, skip_history: bool) -> Self {
        Self::portfolio(
            "TradesGetAndSubscribeV2",
            guid,
            exchange,
            portfolio,
            skip_history,
        )
    }

    /// Positions and money of a portfolio (`PositionsGetAndSubscribeV2`).
    pub fn positions(guid: &str, exchange: &str, portfolio: &str, skip_history: bool) -> Self {
        Self::portfolio(
            "PositionsGetAndSubscribeV2",
            guid,
            exchange,
            portfolio,
            skip_history,
        )
    }

    /// The summary of a portfolio (`SummariesGetAndSubscribeV2`).
    pub fn summaries(guid: &str, exchange: &str, portfolio: &str, skip_history: bool) -> Self {
        Self::portfolio(
            "SummariesGetAndSubscribeV2",
            guid,
            exchange,
            portfolio,
            skip_history,
        )
    }

    /// The risks of a portfolio (`RisksGetAndSubscribe`).
    pub fn risks(guid: &str, exchange: &str, portfolio: &str, skip_history: bool) -> Self {
        Self::portfolio(
            "RisksGetAndSubscribe",
            guid,
            exchange,
            portfolio,
            skip_history,
        )
    }

    /// The risks of the futures market of a portfolio (`SpectraRisksGetAndSubscribe`).
    pub fn spectra_risks(guid: &str, exchange: &str, portfolio: &str, skip_history: bool) -> Self {
        Self::portfolio(
            "SpectraRisksGetAndSubscribe",
            guid,
            exchange,
            portfolio,
            skip_history,
        )
    }

    /// The request that cancels the subscription named by `guid` (`unsubscribe`). It is sent in the connection
    /// the subscription lives in.
    pub fn unsubscribe(guid: &str) -> Self {
        Self {
            opcode: "unsubscribe".to_string(),
            guid: guid.to_string(),
            params: Map::new(),
        }
    }

    /// The JSON text of the request with `token` written into it.
    pub fn to_json(&self, token: &str) -> String {
        let mut m = self.params.clone();
        m.insert("opcode".into(), json!(self.opcode));
        m.insert("guid".into(), json!(self.guid));
        m.insert("token".into(), json!(token));
        Value::Object(m).to_string()
    }

    /// `true` for the request that cancels a subscription.
    pub fn is_unsubscribe(&self) -> bool {
        self.opcode == "unsubscribe"
    }
}

/// A frame of the data channel, parsed.
#[derive(Debug, Clone, PartialEq)]
pub enum WsIncoming {
    /// A data frame of a subscription: `{"data": ..., "guid": ...}`.
    Data {
        /// The id of the subscription that the frame belongs to.
        guid: String,
        /// The payload, in the `Simple` format.
        data: Value,
    },
    /// A status frame: `{"requestGuid": ..., "httpCode": ..., "message": ...}`. 200 confirms a request, 400 or 401
    /// refuses it (the broker closes the connection after a 401).
    Status {
        /// The `guid` of the request, if the frame names one.
        guid: Option<String>,
        /// The HTTP-like code.
        code: u16,
        /// The text.
        message: String,
    },
    /// The session connected (or reconnected): data frames of the restored subscriptions follow.
    Reconnected,
    /// A frame that is neither of the above.
    Unknown(String),
}

/// Parses one text frame of the data channel.
///
/// # Examples
///
/// ```
/// use brokapi_alor_protocol::ws::models::{WsIncoming, parse_incoming};
///
/// match parse_incoming(r#"{"data":{"bids":[],"asks":[]},"guid":"g1"}"#) {
///     WsIncoming::Data { guid, .. } => assert_eq!(guid, "g1"),
///     other => panic!("{other:?}"),
/// }
/// match parse_incoming(r#"{"requestGuid":"g1","httpCode":200,"message":"Handled successfully"}"#) {
///     WsIncoming::Status { code, .. } => assert_eq!(code, 200),
///     other => panic!("{other:?}"),
/// }
/// assert!(matches!(parse_incoming("not json"), WsIncoming::Unknown(_)));
/// ```
pub fn parse_incoming(text: &str) -> WsIncoming {
    let Ok(v) = serde_json::from_str::<Value>(text) else {
        return WsIncoming::Unknown(text.to_string());
    };
    if let Some(code) = v.get("httpCode").and_then(Value::as_u64) {
        return WsIncoming::Status {
            guid: v
                .get("requestGuid")
                .and_then(Value::as_str)
                .map(str::to_string),
            code: code as u16,
            message: v
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
        };
    }
    match (v.get("data"), v.get("guid").and_then(Value::as_str)) {
        (Some(data), Some(guid)) => WsIncoming::Data {
            guid: guid.to_string(),
            data: data.clone(),
        },
        _ => WsIncoming::Unknown(text.to_string()),
    }
}
