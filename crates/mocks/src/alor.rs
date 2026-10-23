//! Axum HTTP + WebSocket mock of the ALOR OpenAPI surface used by the connectors.
//!
//! The mock is stateful for orders, like the broker: every account (named by its access token) has its own
//! orders, stop orders and trades. It speaks the current methods of the documentation: `POST /refresh` of the
//! authorization server (the same host here), the reads under `/md/...`, the order commands under
//! `/commandapi/warptrans/TRADE/v2/client/orders/...` (with the `X-REQID` header: a repeated id gets a copy of
//! the first answer, as the documentation says), and the data channel `/ws` with the subscriptions
//! `OrderBookGetAndSubscribe`, `QuotesSubscribe`, `AllTradesGetAndSubscribe`, `BarsGetAndSubscribe`,
//! `OrdersGetAndSubscribeV2`, `TradesGetAndSubscribeV2`, `PositionsGetAndSubscribeV2` and `unsubscribe`, and the
//! trading channel `/cws` (`authorize`, `create:limit`, `delete:limit`).
//!
//! The instruments are `SBER` (board `TQBR`, lot 10, step 0.01) and `GAZP` (lot 10). A ticker called
//! [`UNKNOWN_TICKER`] is unknown: reads of it are answered 404.
//!
//! Test hooks: [`MockServer::fill`] executes an order (status, trade, events), [`MockServer::push_event`]
//! writes a raw message to the sockets, [`MockServer::cut_event_sockets`] breaks every socket. A limit order at
//! `REJECT_PRICE` is refused with HTTP 400.

use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::http::{HeaderMap, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get};
use brokapi_core::{Decimal, Price, Qty};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// The ticker no mock broker knows: reads of it are answered "not found".
pub const UNKNOWN_TICKER: &str = "NOPE";

/// Access token of the default account (refresh token `mock-refresh-token`).
pub const DEFAULT_ACCESS: &str = "mock-access-token";

/// The portfolio of the default account.
pub const PORTFOLIO: &str = "D00001";

/// The lot of the instruments of the mock.
pub const LOT: i64 = 10;

/// What the socket task takes as "close now" (never a real message).
const CLOSE_SENTINEL: &str = "\u{0}close";

/// Tunable mock behaviour for individual test scenarios.
#[derive(Debug, Default)]
pub struct MockOptions {
    /// When set, the order book read answers HTTP 429.
    pub rate_limit_order_book: AtomicBool,
    /// Count of WebSocket sessions established, on `/ws` and `/cws`.
    pub ws_connections: AtomicU32,
    /// Close the data socket after this many order book frames sent on it (per connection).
    pub disconnect_after_data_frames: AtomicU32,
    /// Every text message the data socket received, in the order of arrival.
    pub ws_commands: std::sync::Mutex<Vec<String>>,
    /// Requests that change an order (the POST, PUT and DELETE of the order methods) the mock has received,
    /// counted when they arrive, before any delay.
    pub order_commands: AtomicU32,
    /// The mock holds every order command this many milliseconds before it acts and answers. The clock is the
    /// tokio clock of the test, so `brokapi_mocks::time::jump` ends the wait.
    pub order_delay_ms: AtomicU64,
    /// The broker takes a stop order (`actions/stop`, `actions/stopLimit`), then the answer is lost: the stop order
    /// is in the book and the client gets HTTP 503.
    pub stop_answer_lost: AtomicBool,
    /// Every `"METHOD /path"` the order command methods served, in order.
    pub served: std::sync::Mutex<Vec<String>>,
    /// Every `X-REQID` of an order command, in order.
    pub request_ids: std::sync::Mutex<Vec<String>>,
    /// Every candle request, as (`tf`, `from`, `to`).
    pub history_requests: std::sync::Mutex<Vec<(String, i64, i64)>>,
    /// Every query of the trade history, as `dateFrom` and `limit`.
    pub history_asked: std::sync::Mutex<Vec<(Option<String>, Option<String>)>>,
    /// Calls of the quotations (a method no connector wraps, used to test `raw()`).
    pub quotes_calls: AtomicU32,
    /// Authorization header of every quotations call.
    pub quotes_auth: std::sync::Mutex<Vec<String>>,
    /// The next token request is refused with HTTP 403 (a revoked refresh token).
    pub refuse_refresh: AtomicBool,
    /// Calls of `POST /refresh`.
    pub refresh_calls: AtomicU32,
    /// Subscribe requests containing this text get a 400 status frame.
    pub fail_subscribe_text: tokio::sync::Mutex<Option<String>>,
    /// Holds every subscribe request of the data socket (the subscription is not made and not confirmed) until
    /// `stream_gate.release()`. For a broker that is slow to open its streams.
    pub stream_gate: crate::gate::StreamGate,
    /// Orders, trades and open sockets of all accounts.
    book: std::sync::Mutex<Book>,
}

impl MockOptions {
    /// Makes the order book read answer HTTP 429.
    pub fn enable_rate_limit_order_book(&self) {
        self.rate_limit_order_book.store(true, Ordering::Relaxed);
    }

    /// Count of WebSocket sessions established so far.
    pub fn ws_connections(&self) -> u32 {
        self.ws_connections.load(Ordering::Relaxed)
    }

    /// Open sockets of the data channel that hold at least one subscription of the orders of an account.
    pub fn event_sockets(&self) -> usize {
        let mut b = self.book.lock().unwrap();
        b.subs.retain(|s| !s.tx.is_closed());
        let mut seen = std::collections::HashSet::new();
        b.subs
            .iter()
            .filter(|s| s.kind == "orders")
            .filter(|s| seen.insert(s.socket))
            .count()
    }

    /// Number of order commands served for `"METHOD /path"`.
    pub fn served_count(&self, what: &str) -> usize {
        self.served
            .lock()
            .unwrap()
            .iter()
            .filter(|s| s.as_str() == what)
            .count()
    }
}

/// A JSON number as an exact decimal, from the text the sender wrote.
fn dec_of(v: &Value) -> Option<Decimal> {
    match v {
        Value::Number(n) => {
            let t = n.to_string();
            Decimal::from_str(&t)
                .or_else(|_| Decimal::from_scientific(&t))
                .ok()
        }
        _ => None,
    }
}

/// An exact decimal as a JSON number, written with the same digits (`244.30` stays `244.30`).
fn num(d: Decimal) -> Value {
    Value::Number(d.to_string().parse().expect("a decimal is a JSON number"))
}

fn now_text() -> String {
    chrono::Utc::now()
        .format("%Y-%m-%dT%H:%M:%S%.6fZ")
        .to_string()
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// One order or stop order at the mock broker.
#[derive(Debug, Clone)]
struct MockOrder {
    owner: String,
    id: String,
    comment: String,
    symbol: String,
    board: String,
    side: String,
    /// `limit`, `market`, `stop` or `stoplimit`.
    kind: String,
    /// `working`, `filled`, `canceled` or `rejected`.
    status: String,
    price: Option<Decimal>,
    stop_price: Option<Decimal>,
    condition: String,
    end_time: i64,
    qty_lots: i64,
    filled_lots: i64,
    filled_units: Decimal,
    time_in_force: String,
}

impl MockOrder {
    fn is_stop(&self) -> bool {
        self.kind.starts_with("stop")
    }

    fn working(&self) -> bool {
        self.status == "working"
    }

    fn json(&self) -> Value {
        let lot = Decimal::from(LOT);
        let qty_units = Decimal::from(self.qty_lots) * lot;
        let mut v = json!({
            "id": self.id,
            "symbol": self.symbol,
            "brokerSymbol": format!("MOEX:{}", self.symbol),
            "portfolio": PORTFOLIO,
            "exchange": "MOEX",
            "board": self.board,
            "comment": self.comment,
            "type": self.kind,
            "side": self.side,
            "status": self.status,
            "transTime": now_text(),
            "updateTime": now_text(),
            "endTime": chrono::DateTime::from_timestamp(self.end_time, 0).map(|d| d.format("%Y-%m-%dT%H:%M:%SZ").to_string()),
            "qtyUnits": num(qty_units),
            "qtyBatch": self.qty_lots,
            "qty": self.qty_lots,
            "filledQtyUnits": num(self.filled_units),
            "filledQtyBatch": self.filled_lots,
            "filled": self.filled_lots,
            "price": self.price.map(num),
            "timeInForce": self.time_in_force,
            "existing": false,
        });
        if self.is_stop() {
            v["stopPrice"] = self.stop_price.map(num).unwrap_or(Value::Null);
            v["condition"] = json!(self.condition);
        }
        v
    }
}

#[derive(Debug, Clone)]
struct MockTrade {
    owner: String,
    id: String,
    order_no: String,
    comment: String,
    symbol: String,
    board: String,
    side: String,
    price: Decimal,
    units: Decimal,
    date: String,
}

impl MockTrade {
    fn json(&self) -> Value {
        json!({
            "id": self.id,
            "orderno": self.order_no,
            "comment": self.comment,
            "symbol": self.symbol,
            "exchange": "MOEX",
            "board": self.board,
            "date": self.date,
            "qtyUnits": num(self.units),
            "qty": num(self.units / Decimal::from(LOT)),
            "price": num(self.price),
            "side": self.side,
            "commission": num(Decimal::new(5, 2)),
            "existing": false,
        })
    }
}

/// A subscription of a socket that the mock pushes events to.
#[derive(Debug)]
struct SocketSub {
    socket: u64,
    owner: String,
    guid: String,
    /// `orders`, `trades` or `positions`.
    kind: String,
    tx: tokio::sync::mpsc::UnboundedSender<String>,
}

#[derive(Debug, Default)]
struct Book {
    orders: Vec<MockOrder>,
    trades: Vec<MockTrade>,
    history_trades: Vec<MockTrade>,
    next_order: i64,
    next_trade: i64,
    next_socket: u64,
    subs: Vec<SocketSub>,
    /// Answers to commands by `X-REQID`.
    answers: std::collections::HashMap<String, (u16, String)>,
}

impl Book {
    fn push(&mut self, kind: &str, owner: &str, data: &Value) {
        self.subs.retain(|s| !s.tx.is_closed());
        for s in &self.subs {
            if s.kind == kind && s.owner == owner {
                let _ = s.tx.send(json!({"data": data, "guid": s.guid}).to_string());
            }
        }
    }

    fn find(&self, owner: &str, key: &str) -> Option<usize> {
        self.orders
            .iter()
            .position(|o| o.owner == owner && (o.id == key || o.comment == key))
    }
}

#[derive(Clone)]
struct MockState {
    options: Arc<MockOptions>,
}

/// Running mock server bound to `127.0.0.1:0`.
pub struct MockServer {
    /// The base URL of the mock, for `AlorConfig::for_mock`. The authorization server is the same host.
    pub http_base: String,
    options: Arc<MockOptions>,
    cancel: CancellationToken,
    _task: JoinHandle<()>,
}

impl MockServer {
    /// Starts the mock on a free port of `127.0.0.1`.
    pub async fn start(options: Arc<MockOptions>) -> Self {
        let state = MockState {
            options: Arc::clone(&options),
        };
        let app = Router::new()
            .route("/ws", get(ws_handler))
            .route("/cws", get(cws_handler))
            .fallback(any(http_handler))
            .with_state(state);

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock server");
        let addr = listener.local_addr().expect("mock addr");
        let http_base = format!("http://{addr}");
        let cancel = CancellationToken::new();
        let child = cancel.child_token();
        let task = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    child.cancelled().await;
                })
                .await
                .expect("mock server serve");
        });
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        Self {
            http_base,
            options,
            cancel,
            _task: task,
        }
    }

    /// Ends every open socket of the data channel from the broker side: the link breaks. The connector sees
    /// the end and opens a new one.
    pub fn cut_event_sockets(&self) {
        let mut b = self.options.book.lock().unwrap();
        b.subs.retain(|s| !s.tx.is_closed());
        for s in &b.subs {
            let _ = s.tx.send(CLOSE_SENTINEL.to_owned());
        }
    }

    /// Records `n` trades of the default account in the history of past days (no events). For the size of the
    /// answer of the history.
    pub fn add_history_trades(&self, n: usize, price: Price, qty: Qty) {
        let mut b = self.options.book.lock().unwrap();
        for _ in 0..n {
            b.next_trade += 1;
            let no = b.next_trade;
            b.history_trades.push(MockTrade {
                owner: DEFAULT_ACCESS.to_string(),
                id: format!("H{no}"),
                order_no: format!("79{no:07}"),
                comment: String::new(),
                symbol: "SBER".into(),
                board: "TQBR".into(),
                side: "buy".into(),
                price: price.decimal(),
                units: qty.decimal(),
                date: "2026-10-20T10:00:00Z".to_string(),
            });
        }
    }

    /// Pushes a raw message to every open socket that holds a subscription (all accounts).
    pub fn push_event(&self, text: &str) {
        let mut b = self.options.book.lock().unwrap();
        b.subs.retain(|s| !s.tx.is_closed());
        for s in &b.subs {
            let _ = s.tx.send(text.to_owned());
        }
    }

    /// Executes `qty` pieces of the order `key` (its comment, which is the client order id, or its number) of the
    /// default account at `price`: the order becomes partly filled or filled, a trade is recorded, the events go
    /// to the sockets of the account. Returns the id of the trade.
    pub fn fill(&self, key: &str, qty: Qty, price: Price) -> String {
        self.fill_for(DEFAULT_ACCESS, key, qty, price)
    }

    /// Like [`MockServer::fill`], for the account with the access token `owner`.
    pub fn fill_for(&self, owner: &str, key: &str, qty: Qty, price: Price) -> String {
        let (qty, price) = (qty.decimal(), price.decimal());
        let mut b = self.options.book.lock().unwrap();
        let idx = b.find(owner, key).expect("fill: unknown order");
        b.next_trade += 1;
        let no = b.next_trade;
        let o = &mut b.orders[idx];
        assert!(o.working(), "fill: order is not active");
        let total = Decimal::from(o.qty_lots) * Decimal::from(LOT);
        o.filled_units = (o.filled_units + qty).min(total);
        o.filled_lots = (o.filled_units / Decimal::from(LOT))
            .trunc()
            .try_into()
            .unwrap_or(0);
        o.price = o.price.or(Some(price));
        if o.filled_units >= total {
            o.status = "filled".into();
        }
        let o = o.clone();
        let trade = MockTrade {
            owner: owner.to_string(),
            id: format!("T{no}"),
            order_no: o.id.clone(),
            comment: o.comment.clone(),
            symbol: o.symbol.clone(),
            board: o.board.clone(),
            side: o.side.clone(),
            price,
            units: qty,
            date: now_text(),
        };
        let trade_id = trade.id.clone();
        // The order event comes first, then the fill.
        b.push("orders", owner, &o.json());
        b.push("trades", owner, &trade.json());
        b.trades.push(trade);
        trade_id
    }
}

impl Drop for MockServer {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

fn bearer(headers: &HeaderMap) -> Option<String> {
    headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::to_owned)
}

fn json_response(status: StatusCode, body: &str) -> Response {
    (
        status,
        [(http::header::CONTENT_TYPE, "application/json")],
        body.to_string(),
    )
        .into_response()
}

fn not_found(what: &str) -> Response {
    json_response(
        StatusCode::NOT_FOUND,
        &json!({"message": format!("{what} not found"), "code": "NotFound"}).to_string(),
    )
}

/// The query of a URI as pairs, with `%XX` escapes undone.
fn query_of(uri: &Uri) -> Vec<(String, String)> {
    uri.query()
        .unwrap_or("")
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|p| {
            let (k, v) = p.split_once('=').unwrap_or((p, ""));
            (pct_decode(k), pct_decode(v))
        })
        .collect()
}

fn pct_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'%' if i + 3 <= b.len() => {
                let hex = std::str::from_utf8(&b[i + 1..i + 3]).unwrap_or("");
                match u8::from_str_radix(hex, 16) {
                    Ok(v) => {
                        out.push(v);
                        i += 3;
                    }
                    Err(_) => {
                        out.push(b'%');
                        i += 1;
                    }
                }
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn param<'a>(q: &'a [(String, String)], key: &str) -> Option<&'a str> {
    q.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_str())
}

/// The security info of an instrument of the mock, or `None` for an unknown one.
fn security(symbol: &str, board: Option<&str>) -> Option<Value> {
    if symbol == UNKNOWN_TICKER {
        return None;
    }
    let (kind, market, lot) = match symbol {
        "SiZ6" => ("Фьючерсный контракт SI", "FORTS", 1),
        _ => ("CS", "FOND", LOT),
    };
    Some(json!({
        "symbol": symbol,
        "shortname": format!("{symbol} name"),
        "description": format!("{symbol} description"),
        "exchange": "MOEX",
        "market": market,
        "type": kind,
        "lotsize": lot,
        "facevalue": 3,
        "cfiCode": "ESXXXX",
        "minstep": 0.01,
        "pricestep": 0.01,
        "currency": "RUB",
        "ISIN": "RU0009029540",
        "board": board.unwrap_or("TQBR"),
        "primary_board": "TQBR",
        "tradingStatus": 17,
        "tradingStatusInfo": "Нормальный период торгов",
    }))
}

fn order_book_json() -> Value {
    json!({
        "snapshot": true,
        "bids": [{"price": 244.30, "volume": 157}, {"price": 244.29, "volume": 90}],
        "asks": [{"price": 244.31, "volume": 288}, {"price": 244.32, "volume": 120}],
        "timestamp": 1_792_368_000,
        "ms_timestamp": now_ms(),
        "existing": false,
    })
}

fn positions_json() -> Vec<Value> {
    vec![
        json!({"symbol": "RUB", "exchange": "MOEX", "portfolio": PORTFOLIO, "qtyUnits": 1_000_000.55, "qty": 1_000_000.55,
               "isCurrency": true, "avgPrice": 0, "lotSize": 1}),
        json!({"symbol": "SBER", "exchange": "MOEX", "portfolio": PORTFOLIO, "qtyUnits": 30, "qty": 3, "lotSize": 10,
               "avgPrice": 250.10, "isCurrency": false, "unrealisedPl": 12.5}),
    ]
}

async fn http_handler(
    State(state): State<MockState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let path = uri.path().to_string();
    let is_command = path.starts_with("/commandapi/") || path.starts_with("/warptrans/");
    if is_command {
        state.options.order_commands.fetch_add(1, Ordering::SeqCst);
        let delay = state.options.order_delay_ms.load(Ordering::SeqCst);
        if delay > 0 {
            // The broker takes the order after the delay even if the client gave up waiting and closed the
            // connection: the work runs in a task of its own, which the closed connection does not cancel.
            let held = tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                handle_http(state, method, uri, headers, body).await
            });
            return held.await.unwrap_or_else(|_| {
                json_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    r#"{"message":"internal"}"#,
                )
            });
        }
    }
    let lose_answer = state.options.stop_answer_lost.load(Ordering::SeqCst)
        && method == Method::POST
        && (path.ends_with("/actions/stop") || path.ends_with("/actions/stopLimit"));
    if lose_answer {
        let _ = handle_http(state, method, uri, headers, body).await;
        return json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            r#"{"message":"unavailable"}"#,
        );
    }
    handle_http(state, method, uri, headers, body).await
}

async fn handle_http(
    state: MockState,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let opts = &state.options;
    let path = uri.path().to_string();
    let q = query_of(&uri);

    if path == "/refresh" && method == Method::POST {
        opts.refresh_calls.fetch_add(1, Ordering::SeqCst);
        let given: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
        let token = given["token"]
            .as_str()
            .or_else(|| param(&q, "token"))
            .unwrap_or("");
        if opts.refuse_refresh.load(Ordering::SeqCst)
            || token.is_empty()
            || token == "bad-refresh-token"
        {
            return json_response(
                StatusCode::FORBIDDEN,
                r#"{"message":"Refresh token is not valid","code":"Forbidden"}"#,
            );
        }
        let access = if token == "mock-refresh-token" {
            DEFAULT_ACCESS.to_string()
        } else {
            format!("mock-access-{token}")
        };
        return json_response(StatusCode::OK, &json!({"AccessToken": access}).to_string());
    }

    let Some(owner) = bearer(&headers) else {
        return json_response(
            StatusCode::UNAUTHORIZED,
            r#"{"message":"Invalid JWT token!","code":"Unauthorized"}"#,
        );
    };
    let segs: Vec<&str> = path.trim_matches('/').split('/').collect();

    // ── market data and reference ───────────────────────────────────────────
    if method == Method::GET {
        match segs.as_slice() {
            ["md", "v2", "time"] => {
                return json_response(StatusCode::OK, &chrono::Utc::now().timestamp().to_string());
            }
            ["md", "status", _ex, market] => {
                return json_response(
                    StatusCode::OK,
                    &json!({"code": market, "status": "open"}).to_string(),
                );
            }
            ["md", "v2", "Securities"] => {
                let list: Vec<Value> = ["SBER", "GAZP", "SiZ6"]
                    .iter()
                    .filter(|s| {
                        param(&q, "query")
                            .is_none_or(|p| s.to_lowercase().contains(&p.to_lowercase()))
                    })
                    .filter(|s| match param(&q, "sector") {
                        Some("FORTS") => **s == "SiZ6",
                        Some("FOND") => **s != "SiZ6",
                        _ => true,
                    })
                    .filter_map(|s| security(s, None))
                    .collect();
                let offset: usize = param(&q, "offset")
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(0);
                let list: Vec<Value> = list.into_iter().skip(offset).collect();
                return json_response(StatusCode::OK, &Value::Array(list).to_string());
            }
            ["md", "v2", "Securities", ex, symbol] if *ex == "MOEX" || *ex == "SPBX" => {
                return match security(symbol, param(&q, "instrumentGroup")) {
                    Some(v) => json_response(StatusCode::OK, &v.to_string()),
                    None => not_found("instrument"),
                };
            }
            ["md", "v2", "Securities", symbols, "quotes"] => {
                opts.quotes_calls.fetch_add(1, Ordering::SeqCst);
                let auth = headers
                    .get("authorization")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_string();
                opts.quotes_auth.lock().unwrap().push(auth);
                let list: Vec<Value> = symbols
                    .split(',')
                    .map(|p| p.split_once(':').map(|x| x.1).unwrap_or(p))
                    .map(|s| quote_json(s, 300.1, 300.2))
                    .collect();
                return json_response(StatusCode::OK, &Value::Array(list).to_string());
            }
            ["md", "v2", "Securities", _ex, symbol, "alltrades"] => {
                if *symbol == UNKNOWN_TICKER {
                    return not_found("instrument");
                }
                let take: usize = param(&q, "take").and_then(|v| v.parse().ok()).unwrap_or(2);
                let trades: Vec<Value> = (0..take.min(3))
                    .map(|i| {
                        json!({"id": 9000 - i as i64, "orderno": "1", "symbol": symbol, "board": "TQBR", "qty": 100,
                               "price": 300.15, "time": now_text(), "timestamp": now_ms(), "side": "buy", "oi": 0, "existing": true})
                    })
                    .collect();
                return json_response(StatusCode::OK, &Value::Array(trades).to_string());
            }
            [
                "md",
                "v2",
                "Securities",
                _ex,
                _symbol,
                "alltrades",
                "history",
            ] => {
                return json_response(StatusCode::OK, r#"{"total":0,"list":[]}"#);
            }
            ["md", "v2", "orderbooks", _ex, symbol] => {
                if opts.rate_limit_order_book.load(Ordering::Relaxed) {
                    return json_response(
                        StatusCode::TOO_MANY_REQUESTS,
                        r#"{"message":"Too many requests"}"#,
                    );
                }
                if *symbol == UNKNOWN_TICKER {
                    return not_found("instrument");
                }
                // The REST answer names the time `msTimestamp`, the stream `ms_timestamp`.
                let mut book = order_book_json();
                let ms = book["ms_timestamp"].take();
                book.as_object_mut().unwrap().remove("ms_timestamp");
                book.as_object_mut().unwrap().remove("timestamp");
                book["msTimestamp"] = ms;
                return json_response(StatusCode::OK, &book.to_string());
            }
            ["md", "v2", "history"] => {
                let from: i64 = param(&q, "from")
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(1_792_368_000);
                let to: i64 = param(&q, "to")
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(from + 3600);
                let tf = param(&q, "tf").unwrap_or("60").to_string();
                opts.history_requests
                    .lock()
                    .unwrap()
                    .push((tf.clone(), from, to));
                let step: i64 = match tf.as_str() {
                    "D" => 86_400,
                    "W" => 604_800,
                    "M" => 2_592_000,
                    n => n.parse().unwrap_or(60),
                }
                .max(1);
                // One candle on every point of the grid (aligned to the epoch) in the range asked, like the
                // other imitations with `synthetic_candles`: a long history gets bars that depend on the range.
                let mut bars = Vec::new();
                let mut t = (from + step - 1).div_euclid(step) * step;
                while t < to && bars.len() < 50_000 {
                    let n = t.div_euclid(step);
                    let open = 100 + n.rem_euclid(20);
                    bars.push(json!({"time": t, "open": open, "high": open + 1, "low": open - 1, "close": open,
                                     "volume": 1000 + n.rem_euclid(7)}));
                    t += step;
                }
                return json_response(
                    StatusCode::OK,
                    &json!({"history": bars, "next": null, "prev": null}).to_string(),
                );
            }
            _ => {}
        }
    }

    // ── the portfolio ───────────────────────────────────────────────────────
    if method == Method::GET && segs.len() >= 5 && segs[..3] == ["md", "v2", "Clients"] {
        let tail = &segs[3..];
        let mut b = opts.book.lock().unwrap();
        match tail {
            [_ex, _pf, "orders"] => {
                let list: Vec<Value> = b
                    .orders
                    .iter()
                    .filter(|o| o.owner == owner && !o.is_stop())
                    .map(MockOrder::json)
                    .collect();
                return json_response(StatusCode::OK, &Value::Array(list).to_string());
            }
            [_ex, _pf, "orders", id] => {
                return match b
                    .orders
                    .iter()
                    .find(|o| o.owner == owner && !o.is_stop() && o.id == *id)
                {
                    Some(o) => json_response(StatusCode::OK, &o.json().to_string()),
                    None => not_found("order"),
                };
            }
            [_ex, _pf, "stoporders"] => {
                let list: Vec<Value> = b
                    .orders
                    .iter()
                    .filter(|o| o.owner == owner && o.is_stop())
                    .map(MockOrder::json)
                    .collect();
                return json_response(StatusCode::OK, &Value::Array(list).to_string());
            }
            [_ex, _pf, "stoporders", id] => {
                return match b
                    .orders
                    .iter()
                    .find(|o| o.owner == owner && o.is_stop() && o.id == *id)
                {
                    Some(o) => json_response(StatusCode::OK, &o.json().to_string()),
                    None => not_found("stop order"),
                };
            }
            [_ex, _pf, "positions"] => {
                return json_response(StatusCode::OK, &Value::Array(positions_json()).to_string());
            }
            [_ex, _pf, "summary"] => {
                return json_response(
                    StatusCode::OK,
                    &json!({"buyingPower": 900_000.5, "portfolioEvaluation": 1_001_000.0, "commission": 0.0,
                            "buyingPowerByCurrency": [{"currency": "RUB", "buyingPower": 900_000.5}]})
                    .to_string(),
                );
            }
            [_ex, _pf, "trades"] => {
                let list: Vec<Value> = b
                    .trades
                    .iter()
                    .filter(|t| t.owner == owner)
                    .map(MockTrade::json)
                    .collect();
                return json_response(StatusCode::OK, &Value::Array(list).to_string());
            }
            _ => {
                let _ = &mut b;
            }
        }
    }
    if method == Method::GET
        && segs.len() == 7
        && segs[..3] == ["md", "v2", "Stats"]
        && segs[5..] == ["history", "trades"]
    {
        let limit: usize = param(&q, "limit")
            .and_then(|v| v.parse().ok())
            .unwrap_or(1000);
        opts.history_asked.lock().unwrap().push((
            param(&q, "dateFrom").map(str::to_string),
            param(&q, "limit").map(str::to_string),
        ));
        let b = opts.book.lock().unwrap();
        let list: Vec<Value> = b
            .history_trades
            .iter()
            .filter(|t| t.owner == owner)
            .take(limit)
            .map(MockTrade::json)
            .collect();
        return json_response(StatusCode::OK, &Value::Array(list).to_string());
    }

    // ── order commands ──────────────────────────────────────────────────────
    if segs.starts_with(&["commandapi", "warptrans", "TRADE", "v2", "client", "orders"]) {
        let tail = &segs[6..];
        opts.served.lock().unwrap().push(format!("{method} {path}"));
        let req_id = headers
            .get("x-reqid")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        if let Some(id) = &req_id {
            opts.request_ids.lock().unwrap().push(id.clone());
            // The same id again: a copy of the first answer, nothing is done a second time.
            if let Some((code, text)) = opts.book.lock().unwrap().answers.get(id).cloned() {
                return json_response(StatusCode::from_u16(code).unwrap_or(StatusCode::OK), &text);
            }
        }
        let (code, text) = handle_command(opts, &owner, &method, tail, &q, &body, req_id.is_some());
        if let Some(id) = req_id {
            opts.book
                .lock()
                .unwrap()
                .answers
                .insert(id, (code, text.clone()));
        }
        return json_response(StatusCode::from_u16(code).unwrap_or(StatusCode::OK), &text);
    }

    not_found("method")
}

fn quote_json(symbol: &str, bid: f64, ask: f64) -> Value {
    json!({"symbol": symbol, "exchange": "MOEX", "last_price": 300.15, "bid": bid, "ask": ask,
           "bid_vol": 4, "ask_vol": 5, "ob_ms_timestamp": now_ms(), "lotsize": LOT})
}

/// One order command. Returns the status and the body.
fn handle_command(
    opts: &MockOptions,
    owner: &str,
    method: &Method,
    tail: &[&str],
    q: &[(String, String)],
    body: &Bytes,
    has_req_id: bool,
) -> (u16, String) {
    let err = |code: u16, m: &str| (code, json!({"message": m, "code": "Error"}).to_string());
    let ok = |id: &str| {
        (
            200,
            json!({"message": "success", "orderNumber": id}).to_string(),
        )
    };
    let mut b = opts.book.lock().unwrap();
    match (method.as_str(), tail) {
        (
            "POST",
            [
                "actions",
                kind @ ("limit" | "market" | "stop" | "stopLimit"),
            ],
        ) => {
            if !has_req_id {
                return err(400, "X-REQID is required");
            }
            let v: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
            let price = dec_of(&v["price"]);
            if *kind == "limit" && price == Some(brokapi_conformance::REJECT_PRICE.decimal()) {
                return err(400, "the price is refused by the mock");
            }
            let lots = v["quantity"].as_i64().unwrap_or(0);
            if lots <= 0 {
                return err(400, "quantity must be positive");
            }
            b.next_order += 1;
            let id = format!("7900{:07}", b.next_order);
            let order = MockOrder {
                owner: owner.to_string(),
                id: id.clone(),
                comment: v["comment"].as_str().unwrap_or("").to_string(),
                symbol: v["instrument"]["symbol"]
                    .as_str()
                    .unwrap_or("SBER")
                    .to_string(),
                board: v["instrument"]["instrumentGroup"]
                    .as_str()
                    .unwrap_or("TQBR")
                    .to_string(),
                side: v["side"].as_str().unwrap_or("buy").to_string(),
                kind: match *kind {
                    "stopLimit" => "stoplimit".to_string(),
                    other => other.to_string(),
                },
                status: "working".into(),
                price,
                stop_price: dec_of(&v["triggerPrice"]),
                condition: v["condition"].as_str().unwrap_or("").to_string(),
                end_time: v["stopEndUnixTime"].as_i64().unwrap_or(0),
                qty_lots: lots,
                filled_lots: 0,
                filled_units: Decimal::ZERO,
                time_in_force: v["timeInForce"].as_str().unwrap_or("oneday").to_string(),
            };
            if !order.is_stop() {
                b.push("orders", owner, &order.json());
            }
            b.orders.push(order);
            ok(&id)
        }
        ("PUT", ["actions", "limit", id]) => {
            if !has_req_id {
                return err(400, "X-REQID is required");
            }
            let v: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
            let Some(idx) = b
                .orders
                .iter()
                .position(|o| o.owner == owner && o.id == *id && !o.is_stop())
            else {
                return err(404, "order not found");
            };
            if !b.orders[idx].working() {
                return err(400, "the order is not active");
            }
            b.orders[idx].status = "canceled".into();
            let old = b.orders[idx].clone();
            b.push("orders", owner, &old.json());
            b.next_order += 1;
            let new_id = format!("7900{:07}", b.next_order);
            let new = MockOrder {
                id: new_id.clone(),
                comment: v["comment"].as_str().unwrap_or("").to_string(),
                price: dec_of(&v["price"]),
                qty_lots: v["quantity"].as_i64().unwrap_or(old.qty_lots),
                status: "working".into(),
                filled_lots: 0,
                filled_units: Decimal::ZERO,
                ..old
            };
            b.push("orders", owner, &new.json());
            b.orders.push(new);
            ok(&new_id)
        }
        ("DELETE", ["all"]) => {
            let ids: Vec<usize> = b
                .orders
                .iter()
                .enumerate()
                .filter(|(_, o)| o.owner == owner && o.working())
                .map(|(i, _)| i)
                .collect();
            for i in ids {
                b.orders[i].status = "canceled".into();
                let o = b.orders[i].clone();
                if !o.is_stop() {
                    b.push("orders", owner, &o.json());
                }
            }
            (200, json!({"message": "success"}).to_string())
        }
        ("DELETE", [id]) => {
            let stop = param(q, "stop") == Some("true");
            let Some(idx) = b
                .orders
                .iter()
                .position(|o| o.owner == owner && o.id == *id && o.is_stop() == stop)
            else {
                return err(404, "order not found");
            };
            if !b.orders[idx].working() {
                return err(400, "the order is not active");
            }
            b.orders[idx].status = "canceled".into();
            let o = b.orders[idx].clone();
            if !o.is_stop() {
                b.push("orders", owner, &o.json());
            }
            (
                200,
                json!({"message": "success", "orderNumber": id}).to_string(),
            )
        }
        _ => err(404, "method not found"),
    }
}

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<MockState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_ws(socket, state))
}

async fn cws_handler(ws: WebSocketUpgrade, State(state): State<MockState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_cws(socket, state))
}

async fn handle_ws(socket: WebSocket, state: MockState) {
    state.options.ws_connections.fetch_add(1, Ordering::Relaxed);
    let (mut write, mut read) = socket.split();
    let socket_id = {
        let mut b = state.options.book.lock().unwrap();
        b.next_socket += 1;
        b.next_socket
    };
    let (push_tx, mut push_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    let mut frames_sent = 0u32;
    // The subscriptions of this socket that are market data: guid -> opcode.
    loop {
        let msg = tokio::select! {
            Some(text) = push_rx.recv() => {
                if text == CLOSE_SENTINEL {
                    let _ = write.send(Message::Close(None)).await;
                    break;
                }
                let _ = write.send(Message::Text(text.into())).await;
                continue;
            }
            msg = read.next() => match msg {
                Some(m) => m,
                None => break,
            },
        };
        match msg {
            Ok(Message::Text(text)) => {
                state
                    .options
                    .ws_commands
                    .lock()
                    .unwrap()
                    .push(text.to_string());
                let req: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
                let guid = req["guid"].as_str().unwrap_or("").to_string();
                let opcode = req["opcode"].as_str().unwrap_or("").to_string();
                let status = |code: u16, message: &str| {
                    Message::Text(
                        json!({"requestGuid": guid, "httpCode": code, "message": message})
                            .to_string()
                            .into(),
                    )
                };
                if req["token"]
                    .as_str()
                    .is_none_or(|t| !t.starts_with("mock-access"))
                {
                    let _ = write.send(status(401, "Invalid JWT token!")).await;
                    let _ = write.send(Message::Close(None)).await;
                    break;
                }
                if opcode == "unsubscribe" {
                    state
                        .options
                        .book
                        .lock()
                        .unwrap()
                        .subs
                        .retain(|s| !(s.socket == socket_id && s.guid == guid));
                    let _ = write.send(status(200, "Handled successfully")).await;
                    continue;
                }
                // The subscription is made and confirmed when the broker gets to it.
                state.options.stream_gate.pass().await;
                let refuse = state.options.fail_subscribe_text.lock().await.clone();
                if refuse.is_some_and(|r| text.contains(&r)) {
                    let _ = write
                        .send(status(400, "Bad request: no such instrument"))
                        .await;
                    continue;
                }
                let owner = req["token"].as_str().unwrap_or("").to_string();
                let symbol = req["code"].as_str().unwrap_or("SBER").to_string();
                let frames: Vec<Value> = match opcode.as_str() {
                    "OrderBookGetAndSubscribe" => vec![order_book_json()],
                    "QuotesSubscribe" => vec![quote_json(&symbol, 300.1, 300.2)],
                    "AllTradesGetAndSubscribe" => vec![json!({
                        "id": 9001, "orderno": "1", "symbol": symbol, "board": "TQBR", "qty": 100, "price": 300.15,
                        "time": now_text(), "timestamp": now_ms(), "side": "buy", "oi": 0, "existing": false})],
                    "BarsGetAndSubscribe" => vec![json!({
                        "time": chrono::Utc::now().timestamp() / 60 * 60, "open": 300.0, "close": 300.5,
                        "high": 301.0, "low": 299.5, "volume": 1200})],
                    "OrdersGetAndSubscribeV2"
                    | "TradesGetAndSubscribeV2"
                    | "PositionsGetAndSubscribeV2" => {
                        let kind = match opcode.as_str() {
                            "OrdersGetAndSubscribeV2" => "orders",
                            "TradesGetAndSubscribeV2" => "trades",
                            _ => "positions",
                        };
                        let skip = req["skipHistory"].as_bool().unwrap_or(false);
                        let initial: Vec<Value> = if skip {
                            Vec::new()
                        } else {
                            match kind {
                                "orders" => state
                                    .options
                                    .book
                                    .lock()
                                    .unwrap()
                                    .orders
                                    .iter()
                                    .filter(|o| o.owner == owner && !o.is_stop())
                                    .map(|o| {
                                        let mut v = o.json();
                                        v["existing"] = json!(true);
                                        v
                                    })
                                    .collect(),
                                "positions" => positions_json(),
                                _ => Vec::new(),
                            }
                        };
                        state.options.book.lock().unwrap().subs.push(SocketSub {
                            socket: socket_id,
                            owner,
                            guid: guid.clone(),
                            kind: kind.to_string(),
                            tx: push_tx.clone(),
                        });
                        initial
                    }
                    _ => Vec::new(),
                };
                let _ = write.send(status(200, "Handled successfully")).await;
                for f in frames {
                    let _ = write
                        .send(Message::Text(
                            json!({"data": f, "guid": guid}).to_string().into(),
                        ))
                        .await;
                    if opcode == "OrderBookGetAndSubscribe" {
                        frames_sent += 1;
                    }
                }
                let limit = state
                    .options
                    .disconnect_after_data_frames
                    .load(Ordering::Relaxed);
                if limit > 0 && frames_sent >= limit {
                    let _ = write.send(Message::Close(None)).await;
                    break;
                }
            }
            Ok(Message::Ping(payload)) => {
                let _ = write.send(Message::Pong(payload)).await;
            }
            Ok(Message::Close(_)) | Err(_) => break,
            _ => {}
        }
    }
    state
        .options
        .book
        .lock()
        .unwrap()
        .subs
        .retain(|s| s.socket != socket_id);
}

/// The trading channel: `authorize`, `create:limit` and `delete:limit`, with the answers of the broker.
async fn handle_cws(socket: WebSocket, state: MockState) {
    state.options.ws_connections.fetch_add(1, Ordering::Relaxed);
    let (mut write, mut read) = socket.split();
    let mut owner: Option<String> = None;
    while let Some(Ok(msg)) = read.next().await {
        let Message::Text(text) = msg else { continue };
        let req: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
        let guid = req["guid"].as_str().unwrap_or("").to_string();
        let reply = |code: u16, message: &str, extra: Value| {
            let mut v = json!({"requestGuid": guid, "httpCode": code, "message": message});
            if let (Some(m), Some(e)) = (v.as_object_mut(), extra.as_object()) {
                m.extend(e.clone());
            }
            Message::Text(v.to_string().into())
        };
        match req["opcode"].as_str().unwrap_or("") {
            "authorize" => {
                if req["token"]
                    .as_str()
                    .is_some_and(|t| t.starts_with("mock-access"))
                {
                    owner = req["token"].as_str().map(str::to_string);
                    let _ = write
                        .send(reply(
                            200,
                            "The connection has been initialized.",
                            json!({}),
                        ))
                        .await;
                } else {
                    let _ = write
                        .send(reply(401, "Invalid JWT token!", json!({})))
                        .await;
                }
            }
            op @ ("create:limit" | "delete:limit") => {
                let Some(owner) = owner.as_deref() else {
                    let _ = write
                        .send(reply(401, "The connection is not authorized", json!({})))
                        .await;
                    continue;
                };
                state.options.order_commands.fetch_add(1, Ordering::SeqCst);
                // The answer is made with the book locked and sent after the lock is let go.
                let (code, message, extra) = {
                    let mut b = state.options.book.lock().unwrap();
                    if op == "create:limit" {
                        b.next_order += 1;
                        let id = format!("7900{:07}", b.next_order);
                        let lots = req["quantity"].as_i64().unwrap_or(1);
                        let order = MockOrder {
                            owner: owner.to_string(),
                            id: id.clone(),
                            comment: req["comment"].as_str().unwrap_or("").to_string(),
                            symbol: req["instrument"]["symbol"]
                                .as_str()
                                .unwrap_or("SBER")
                                .to_string(),
                            board: req["instrument"]["instrumentGroup"]
                                .as_str()
                                .unwrap_or("TQBR")
                                .to_string(),
                            side: req["side"].as_str().unwrap_or("buy").to_string(),
                            kind: "limit".into(),
                            status: "working".into(),
                            price: dec_of(&req["price"]),
                            stop_price: None,
                            condition: String::new(),
                            end_time: 0,
                            qty_lots: lots,
                            filled_lots: 0,
                            filled_units: Decimal::ZERO,
                            time_in_force: "oneday".into(),
                        };
                        b.push("orders", owner, &order.json());
                        b.orders.push(order);
                        (200, "success", json!({"orderNumber": id}))
                    } else {
                        let id = req["orderId"].as_str().unwrap_or("").to_string();
                        match b.orders.iter().position(|o| o.owner == owner && o.id == id) {
                            Some(i) => {
                                b.orders[i].status = "canceled".into();
                                let o = b.orders[i].clone();
                                b.push("orders", owner, &o.json());
                                (200, "success", json!({"orderNumber": id}))
                            }
                            None => (400, "order not found", json!({})),
                        }
                    }
                };
                let _ = write.send(reply(code, message, extra)).await;
            }
            _ => {
                let _ = write.send(reply(400, "unknown opcode", json!({}))).await;
            }
        }
    }
}
