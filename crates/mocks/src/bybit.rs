//! Axum HTTP + WebSocket mock of the Bybit API V5 surface used by the connectors (category `linear`).
//!
//! The mock is stateful for orders, like the exchange: every account (named by its API key) has its own orders and
//! fills. It checks the signature of every private request the way the exchange does (HMAC-SHA256 of
//! `timestamp + key + recv_window + query-or-body`, the timestamp inside the receive window of its own clock), so a
//! client that signs wrongly fails here. It speaks the methods of the documentation: the market reads under
//! `/v5/market/...`, the order methods under `/v5/order/...`, the fills, positions and wallet, and the streams
//! `/v5/public/linear` (`orderbook.N.SYMBOL`, `publicTrade.SYMBOL`, `kline.I.SYMBOL`) and `/v5/private`
//! (`auth`, then `order.linear`, `execution.linear`, `position.linear`, `wallet`).
//!
//! The instruments are `MOCKUSDT` (a perpetual with a quantity step of 10, tick 0.01), `BTCUSDT` (the figures of the
//! real contract: step 0.001, tick 0.10) and the dated future `BTC-26DEC26`. A symbol called [`UNKNOWN_SYMBOL`] is
//! unknown: reads of it are answered with `retCode` 10001 `params error: symbol invalid`.
//!
//! Test hooks: [`MockServer::fill`] executes an order (status, fill, events), [`MockServer::push_event`] writes a raw
//! message to the private sockets, [`MockServer::cut_event_sockets`] breaks them, and [`MockOptions`] holds the switches.
//! A limit order at `REJECT_PRICE` is refused with `retCode` 110003.

use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, Ordering};

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

/// The symbol no mock exchange knows: reads of it are refused as an invalid symbol.
pub const UNKNOWN_SYMBOL: &str = "NOPEUSDT";

/// The API key of the default account.
pub const DEFAULT_KEY: &str = "mock-key";

/// The secret every key of the mock is signed with.
pub const SECRET: &str = "mock-secret";

/// The quantity step of `MOCKUSDT`, the instrument of the shared scenarios (they order whole "lots" of 10).
pub const LOT: i64 = 10;

/// What the socket task takes as "close now" (never a real message).
const CLOSE_SENTINEL: &str = "\u{0}close";

const RECV_WINDOW_LIMIT_MS: i64 = 30_000;

/// Tunable mock behaviour for individual test scenarios.
#[derive(Debug, Default)]
pub struct MockOptions {
    /// When set, the order book read answers HTTP 429.
    pub rate_limit_order_book: AtomicBool,
    /// Count of WebSocket sessions established, public and private.
    pub ws_connections: AtomicU32,
    /// Close the public socket after this many order book frames sent on it (per connection).
    pub disconnect_after_data_frames: AtomicU32,
    /// Every text message the sockets received, in the order of arrival.
    pub ws_commands: std::sync::Mutex<Vec<String>>,
    /// Requests that change an order (the POSTs of `/v5/order/...`) the mock has received, counted when they arrive,
    /// before any delay.
    pub order_commands: AtomicU32,
    /// The mock holds every order command this many milliseconds before it acts and answers. The clock is the tokio
    /// clock of the test, so `brokapi_mocks::time::jump` ends the wait.
    pub order_delay_ms: AtomicU64,
    /// The exchange takes a conditional order, then the answer is lost: the order is in the book and the client gets
    /// HTTP 503.
    pub stop_answer_lost: AtomicBool,
    /// Every `"METHOD /path"` of the order methods served, in order.
    pub served: std::sync::Mutex<Vec<String>>,
    /// Every candle request, as (`interval`, `start`, `end`, `limit`).
    pub kline_requests: std::sync::Mutex<Vec<(String, Option<i64>, Option<i64>, Option<u32>)>>,
    /// Every request of fills, as (`startTime`, `endTime`).
    pub execution_requests: std::sync::Mutex<Vec<(Option<i64>, Option<i64>)>>,
    /// The clock of the exchange is this many milliseconds ahead of the local one.
    pub clock_offset_ms: AtomicI64,
    /// Calls of `/v5/market/time`.
    pub time_calls: AtomicU32,
    /// The login of the private stream is refused.
    pub refuse_ws_login: AtomicBool,
    /// Subscribe requests containing this text get a failure answer.
    pub fail_subscribe_text: tokio::sync::Mutex<Option<String>>,
    /// Holds every subscribe request of the sockets (the subscription is not made and not confirmed) until
    /// `stream_gate.release()`. For an exchange that is slow to open its streams.
    pub stream_gate: crate::gate::StreamGate,
    /// Frames of the order book streams sent so far.
    pub book_frames: AtomicU32,
    /// Orders, fills and open sockets of all accounts.
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

    /// Open private sockets that hold a subscription to the orders of an account.
    pub fn event_sockets(&self) -> usize {
        let mut b = self.book.lock().unwrap();
        b.subs.retain(|s| !s.tx.is_closed());
        let mut seen = std::collections::HashSet::new();
        b.subs
            .iter()
            .filter(|s| s.topic == "order.linear")
            .filter(|s| seen.insert(s.socket))
            .count()
    }

    /// Number of order methods served for `"METHOD /path"`.
    pub fn served_count(&self, what: &str) -> usize {
        self.served
            .lock()
            .unwrap()
            .iter()
            .filter(|s| s.as_str() == what)
            .count()
    }
}

/// A decimal from the text a request wrote.
fn dec_of(v: &Value) -> Option<Decimal> {
    match v {
        Value::String(s) if !s.is_empty() => Decimal::from_str(s).ok(),
        Value::Number(n) => Decimal::from_str(&n.to_string()).ok(),
        _ => None,
    }
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// One order or conditional order at the mock exchange.
#[derive(Debug, Clone)]
struct MockOrder {
    owner: String,
    id: String,
    link_id: String,
    symbol: String,
    side: String,
    /// `Limit` or `Market`.
    kind: String,
    /// `New`, `Untriggered`, `PartiallyFilled`, `Filled`, `Cancelled`, `Deactivated`, `Rejected`.
    status: String,
    price: Option<Decimal>,
    trigger_price: Option<Decimal>,
    trigger_direction: i64,
    qty: Decimal,
    filled: Decimal,
    created: i64,
    updated: i64,
}

impl MockOrder {
    fn working(&self) -> bool {
        matches!(
            self.status.as_str(),
            "New" | "Untriggered" | "PartiallyFilled"
        )
    }

    fn json(&self) -> Value {
        json!({
            "orderId": self.id,
            "orderLinkId": self.link_id,
            "symbol": self.symbol,
            "category": "linear",
            "price": self.price.map(|p| p.to_string()).unwrap_or_else(|| "0".into()),
            "qty": self.qty.to_string(),
            "side": self.side,
            "positionIdx": 0,
            "orderStatus": self.status,
            "cancelType": if matches!(self.status.as_str(), "Cancelled" | "Deactivated") { "CancelByUser" } else { "UNKNOWN" },
            "rejectReason": "EC_NoError",
            "avgPrice": if self.filled.is_zero() { "".to_string() } else { self.price.unwrap_or_default().to_string() },
            "leavesQty": (self.qty - self.filled).to_string(),
            "cumExecQty": self.filled.to_string(),
            "timeInForce": if self.kind == "Market" { "IOC" } else { "GTC" },
            "orderType": self.kind,
            "stopOrderType": if self.trigger_price.is_some() { "Stop" } else { "UNKNOWN" },
            "triggerPrice": self.trigger_price.map(|p| p.to_string()).unwrap_or_else(|| "0.00".into()),
            "triggerDirection": self.trigger_direction,
            "triggerBy": if self.trigger_price.is_some() { "LastPrice" } else { "UNKNOWN" },
            "reduceOnly": false,
            "closeOnTrigger": false,
            "createdTime": self.created.to_string(),
            "updatedTime": self.updated.to_string(),
        })
    }
}

#[derive(Debug, Clone)]
struct MockTrade {
    owner: String,
    exec_id: String,
    order_id: String,
    link_id: String,
    symbol: String,
    side: String,
    price: Decimal,
    qty: Decimal,
    time: i64,
}

impl MockTrade {
    fn json(&self) -> Value {
        json!({
            "category": "linear",
            "symbol": self.symbol,
            "orderId": self.order_id,
            "orderLinkId": self.link_id,
            "side": self.side,
            "orderPrice": self.price.to_string(),
            "orderQty": self.qty.to_string(),
            "leavesQty": "0",
            "orderType": "Limit",
            "stopOrderType": "UNKNOWN",
            "execFee": "0.0055",
            "execId": self.exec_id,
            "execPrice": self.price.to_string(),
            "execQty": self.qty.to_string(),
            "execType": "Trade",
            "execValue": (self.price * self.qty).to_string(),
            "execTime": self.time.to_string(),
            "isMaker": true,
            "feeRate": "0.0002",
            "feeCurrency": "USDT",
            "seq": self.time,
        })
    }
}

/// A subscription of a private socket that the mock pushes events to.
#[derive(Debug)]
struct SocketSub {
    socket: u64,
    owner: String,
    topic: String,
    tx: tokio::sync::mpsc::UnboundedSender<String>,
}

#[derive(Debug, Default)]
struct Book {
    orders: Vec<MockOrder>,
    trades: Vec<MockTrade>,
    next_order: i64,
    next_trade: i64,
    next_socket: u64,
    subs: Vec<SocketSub>,
    /// Every open private socket, subscribed or not: the way to cut them.
    private_sockets: Vec<(u64, tokio::sync::mpsc::UnboundedSender<String>)>,
    public_sockets: Vec<(u64, tokio::sync::mpsc::UnboundedSender<String>)>,
}

impl Book {
    fn push(&mut self, topic: &str, owner: &str, data: Vec<Value>) {
        self.subs.retain(|s| !s.tx.is_closed());
        let text = json!({"id": format!("m{}", now_ms()), "topic": topic, "creationTime": now_ms(), "data": data}).to_string();
        for s in &self.subs {
            if s.topic == topic && s.owner == owner {
                let _ = s.tx.send(text.clone());
            }
        }
    }

    fn find(&self, owner: &str, key: &str) -> Option<usize> {
        self.orders
            .iter()
            .position(|o| o.owner == owner && (o.id == key || o.link_id == key))
    }
}

#[derive(Clone)]
struct MockState {
    options: Arc<MockOptions>,
}

/// Running mock server bound to `127.0.0.1:0`.
pub struct MockServer {
    /// The base URL of the mock, for `BybitConfig::for_mock`. The streams are on the same host.
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
            .route("/v5/public/linear", get(public_ws_handler))
            .route("/v5/private", get(private_ws_handler))
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

    /// Ends every open private socket from the exchange side: the link breaks. The connector sees the end and opens a
    /// new one.
    pub fn cut_event_sockets(&self) {
        let mut b = self.options.book.lock().unwrap();
        b.private_sockets.retain(|(_, tx)| !tx.is_closed());
        for (_, tx) in &b.private_sockets {
            let _ = tx.send(CLOSE_SENTINEL.to_owned());
        }
    }

    /// Ends every open public socket from the exchange side.
    pub fn cut_public_sockets(&self) {
        let mut b = self.options.book.lock().unwrap();
        b.public_sockets.retain(|(_, tx)| !tx.is_closed());
        for (_, tx) in &b.public_sockets {
            let _ = tx.send(CLOSE_SENTINEL.to_owned());
        }
    }

    /// Pushes a raw message to every open public socket, whatever it is subscribed to.
    pub fn push_public_event(&self, text: &str) {
        let mut b = self.options.book.lock().unwrap();
        b.public_sockets.retain(|(_, tx)| !tx.is_closed());
        for (_, tx) in &b.public_sockets {
            let _ = tx.send(text.to_owned());
        }
    }

    /// Records `n` fills of the default account, spread one a minute back from `days_ago` days ago (no events). For the size of the
    /// answer of the fills and the windows of seven days.
    pub fn add_history_trades(&self, n: usize, price: Price, qty: Qty, days_ago: i64) {
        let mut b = self.options.book.lock().unwrap();
        for i in 0..n {
            b.next_trade += 1;
            let no = b.next_trade;
            b.trades.push(MockTrade {
                owner: DEFAULT_KEY.to_string(),
                exec_id: format!("H{no}"),
                order_id: format!("79{no:07}"),
                link_id: String::new(),
                symbol: "MOCKUSDT".into(),
                side: "Buy".into(),
                price: price.decimal(),
                qty: qty.decimal(),
                time: now_ms() - days_ago * 86_400_000 - i as i64 * 60_000,
            });
        }
    }

    /// Pushes a raw message to every open private socket that holds a subscription (all accounts).
    pub fn push_event(&self, text: &str) {
        let mut b = self.options.book.lock().unwrap();
        b.subs.retain(|s| !s.tx.is_closed());
        for s in &b.subs {
            let _ = s.tx.send(text.to_owned());
        }
    }

    /// Executes `qty` units of the order `key` (its `orderLinkId`, which is the client order id, or its number) of the
    /// default account at `price`: the order becomes partly filled or filled, a fill is recorded, the events go to the
    /// sockets of the account. Returns the id of the fill.
    pub fn fill(&self, key: &str, qty: Qty, price: Price) -> String {
        self.fill_for(DEFAULT_KEY, key, qty, price)
    }

    /// Like [`MockServer::fill`], for the account with the API key `owner`.
    pub fn fill_for(&self, owner: &str, key: &str, qty: Qty, price: Price) -> String {
        let (qty, price) = (qty.decimal(), price.decimal());
        let mut b = self.options.book.lock().unwrap();
        let idx = b.find(owner, key).expect("fill: unknown order");
        b.next_trade += 1;
        let no = b.next_trade;
        let o = &mut b.orders[idx];
        assert!(o.working(), "fill: order is not active");
        o.filled = (o.filled + qty).min(o.qty);
        o.price = o.price.or(Some(price));
        o.status = if o.filled >= o.qty {
            "Filled"
        } else {
            "PartiallyFilled"
        }
        .into();
        o.updated = now_ms();
        let o = o.clone();
        let trade = MockTrade {
            owner: owner.to_string(),
            exec_id: format!("T{no}"),
            order_id: o.id.clone(),
            link_id: o.link_id.clone(),
            symbol: o.symbol.clone(),
            side: o.side.clone(),
            price,
            qty,
            time: now_ms(),
        };
        let exec_id = trade.exec_id.clone();
        // The order event comes first, then the fill.
        b.push("order.linear", owner, vec![o.json()]);
        b.push("execution.linear", owner, vec![trade.json()]);
        b.trades.push(trade);
        exec_id
    }

    /// Fires a conditional order of the default account: it becomes a working order, with the event.
    pub fn trigger(&self, key: &str) {
        let mut b = self.options.book.lock().unwrap();
        let idx = b.find(DEFAULT_KEY, key).expect("trigger: unknown order");
        let o = &mut b.orders[idx];
        assert_eq!(
            o.status, "Untriggered",
            "trigger: not a waiting conditional order"
        );
        o.status = "New".into();
        o.updated = now_ms();
        let o = o.clone();
        b.push("order.linear", DEFAULT_KEY, vec![o.json()]);
    }
}

impl Drop for MockServer {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

fn json_response(status: StatusCode, body: &str) -> Response {
    (
        status,
        [(http::header::CONTENT_TYPE, "application/json")],
        body.to_string(),
    )
        .into_response()
}

fn ok(result: Value) -> Response {
    json_response(
        StatusCode::OK,
        &json!({"retCode": 0, "retMsg": "OK", "result": result, "retExtInfo": {}, "time": now_ms()}).to_string(),
    )
}

fn refuse(code: i64, message: &str) -> Response {
    json_response(
        StatusCode::OK,
        &json!({"retCode": code, "retMsg": message, "result": {}, "retExtInfo": {}, "time": now_ms()}).to_string(),
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

/// The instruments of the mock: (symbol, contract type, tick, quantity step, minimum quantity).
const INSTRUMENTS: [(&str, &str, &str, &str, &str); 3] = [
    ("MOCKUSDT", "LinearPerpetual", "0.01", "10", "10"),
    ("BTCUSDT", "LinearPerpetual", "0.10", "0.001", "0.001"),
    ("BTC-26DEC26", "LinearFutures", "5.00", "0.001", "0.001"),
];

fn instrument_row(
    symbol: &str,
) -> Option<&'static (
    &'static str,
    &'static str,
    &'static str,
    &'static str,
    &'static str,
)> {
    INSTRUMENTS.iter().find(|i| i.0 == symbol)
}

fn instrument_json(i: &(&str, &str, &str, &str, &str)) -> Value {
    let (symbol, contract, tick, step, min) = *i;
    json!({
        "symbol": symbol,
        "contractType": contract,
        "status": "Trading",
        "baseCoin": symbol.trim_end_matches("USDT"),
        "quoteCoin": "USDT",
        "launchTime": "1584230400000",
        "deliveryTime": "0",
        "deliveryFeeRate": "",
        "priceScale": "2",
        "leverageFilter": {"minLeverage": "1", "maxLeverage": "100.00", "leverageStep": "0.01"},
        "priceFilter": {"minPrice": tick, "maxPrice": "1999999.80", "tickSize": tick},
        "lotSizeFilter": {"maxOrderQty": "1500000.000", "minOrderQty": min, "qtyStep": step,
                          "postOnlyMaxOrderQty": "1500000.000", "maxMktOrderQty": "150000.000", "minNotionalValue": "5"},
        "unifiedMarginTrade": true,
        "fundingInterval": 480,
        "settleCoin": "USDT",
        "displayName": symbol,
        "symbolType": "",
        "symbolId": 10_001,
        "copyTrading": "both",
        "upperFundingRate": "0.00333",
        "lowerFundingRate": "-0.00333",
        "isPreListing": false,
        "preListingInfo": null,
        "riskParameters": {"priceLimitRatioX": "0.01", "priceLimitRatioY": "0.02"},
        "forbidUplWithdrawal": false,
        "fullName": "",
        "marketRegion": "",
        "underlyingTicker": "",
        "tags": [],
    })
}

/// The mid price the mock books quote around, by symbol.
fn mid(symbol: &str) -> Decimal {
    if symbol.starts_with("BTC") {
        Decimal::new(8_169_500, 2)
    } else {
        Decimal::new(10_000, 2)
    }
}

fn book_levels(symbol: &str, depth: usize) -> (Vec<Value>, Vec<Value>) {
    let m = mid(symbol);
    let tick = instrument_row(symbol)
        .map(|i| Decimal::from_str(i.2).unwrap())
        .unwrap_or(Decimal::new(1, 2));
    let step = instrument_row(symbol)
        .map(|i| Decimal::from_str(i.3).unwrap())
        .unwrap_or(Decimal::ONE);
    let mut bids = Vec::new();
    let mut asks = Vec::new();
    for i in 0..depth.clamp(1, 50) {
        let off = tick * Decimal::from(i as i64);
        let size = step * Decimal::from(i as i64 + 1);
        bids.push(json!([(m - off).to_string(), size.to_string()]));
        asks.push(json!([(m + tick + off).to_string(), size.to_string()]));
    }
    (bids, asks)
}

fn positions_json() -> Vec<Value> {
    vec![json!({
        "positionIdx": 0, "symbol": "BTCUSDT", "side": "Buy", "size": "0.500", "avgPrice": "81000.50",
        "markPrice": "81695.30", "unrealisedPnl": "347.4", "positionStatus": "Normal", "leverage": "10",
        "createdTime": "1684738540559", "updatedTime": "1684738540561", "takeProfit": "0.00", "stopLoss": "0.00",
    })]
}

fn wallet_json() -> Value {
    json!({"list": [{
        "accountType": "UNIFIED", "totalEquity": "10000.5", "totalWalletBalance": "10000", "totalAvailableBalance": "9800",
        "totalPerpUPL": "0.5", "totalInitialMargin": "200", "totalMaintenanceMargin": "10",
        "coin": [
            {"coin": "USDT", "equity": "10000.5", "usdValue": "10000.5", "walletBalance": "10000", "locked": "0",
             "totalOrderIM": "50", "totalPositionIM": "150", "unrealisedPnl": "0.5", "cumRealisedPnl": "12.5", "bonus": "0"},
            {"coin": "BTC", "equity": "0", "usdValue": "0", "walletBalance": "0", "locked": "0",
             "totalOrderIM": "0", "totalPositionIM": "0", "unrealisedPnl": "0", "cumRealisedPnl": "0", "bonus": "0"},
        ],
    }]})
}

async fn http_handler(
    State(state): State<MockState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let path = uri.path().to_string();
    let is_command = method == Method::POST && path.starts_with("/v5/order/");
    if is_command {
        state.options.order_commands.fetch_add(1, Ordering::SeqCst);
        let delay = state.options.order_delay_ms.load(Ordering::SeqCst);
        if delay > 0 {
            // The exchange takes the order after the delay even if the client gave up waiting and closed the
            // connection: the work runs in a task of its own, which the closed connection does not cancel.
            let held = tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                handle_http(state, method, uri, headers, body).await
            });
            return held.await.unwrap_or_else(|_| {
                json_response(StatusCode::INTERNAL_SERVER_ERROR, r#"{"retCode":10016}"#)
            });
        }
    }
    let lose_answer = state.options.stop_answer_lost.load(Ordering::SeqCst)
        && method == Method::POST
        && path == "/v5/order/create"
        && serde_json::from_slice::<Value>(&body).is_ok_and(|v| !v["triggerPrice"].is_null());
    if lose_answer {
        let _ = handle_http(state, method, uri, headers, body).await;
        return json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            r#"{"retCode":10016,"retMsg":"unavailable"}"#,
        );
    }
    handle_http(state, method, uri, headers, body).await
}

/// Checks the signature of a private request. Returns the key (the owner of the account) or the refusal.
fn authenticate(
    opts: &MockOptions,
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    body: &Bytes,
) -> Result<String, Response> {
    let get = |name: &str| {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned)
    };
    let (Some(key), Some(ts), Some(sign)) = (
        get("x-bapi-api-key"),
        get("x-bapi-timestamp"),
        get("x-bapi-sign"),
    ) else {
        return Err(json_response(StatusCode::UNAUTHORIZED, ""));
    };
    let recv = get("x-bapi-recv-window").unwrap_or_else(|| "5000".into());
    if !key.starts_with(DEFAULT_KEY) {
        return Err(refuse(10003, "API key is invalid."));
    }
    let payload = if *method == Method::GET {
        uri.query().unwrap_or("").to_string()
    } else {
        String::from_utf8_lossy(body).into_owned()
    };
    let expected = brokapi_bybit_protocol::sign::sign(SECRET, &format!("{ts}{key}{recv}{payload}"));
    if expected != sign {
        return Err(refuse(
            10004,
            "Error sign, please check your signature generation algorithm.",
        ));
    }
    // timestamp in [server_time - recv_window, server_time + 1000)
    let server = now_ms() + opts.clock_offset_ms.load(Ordering::Relaxed);
    let ts: i64 = ts.parse().unwrap_or(0);
    let recv: i64 = recv.parse().unwrap_or(5000).min(RECV_WINDOW_LIMIT_MS);
    if ts < server - recv || ts >= server + 1000 {
        return Err(refuse(
            10002,
            &format!(
                "invalid request, please check your server timestamp or recv_window param: req_timestamp[{ts}],server_timestamp[{server}],recv_window[{recv}]"
            ),
        ));
    }
    Ok(key)
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

    // ── public market data ──────────────────────────────────────────────────
    if method == Method::GET {
        match path.as_str() {
            "/v5/market/time" => {
                opts.time_calls.fetch_add(1, Ordering::SeqCst);
                let ms = now_ms() + opts.clock_offset_ms.load(Ordering::Relaxed);
                return ok(
                    json!({"timeSecond": (ms / 1000).to_string(), "timeNano": (ms as i128 * 1_000_000).to_string()}),
                );
            }
            "/v5/market/instruments-info" => {
                if let Some(symbol) = param(&q, "symbol") {
                    return match instrument_row(symbol) {
                        Some(i) => ok(
                            json!({"category": "linear", "list": [instrument_json(i)], "nextPageCursor": ""}),
                        ),
                        None => refuse(10001, "params error: symbol invalid"),
                    };
                }
                let limit: usize = param(&q, "limit")
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(500);
                let from: usize = param(&q, "cursor")
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(0);
                let rows: Vec<Value> = INSTRUMENTS
                    .iter()
                    .skip(from)
                    .take(limit)
                    .map(instrument_json)
                    .collect();
                let next = if from + limit < INSTRUMENTS.len() {
                    (from + limit).to_string()
                } else {
                    String::new()
                };
                return ok(json!({"category": "linear", "list": rows, "nextPageCursor": next}));
            }
            "/v5/market/orderbook" => {
                if opts.rate_limit_order_book.load(Ordering::Relaxed) {
                    return json_response(
                        StatusCode::TOO_MANY_REQUESTS,
                        r#"{"retCode":10006,"retMsg":"Too many visits!"}"#,
                    );
                }
                let symbol = param(&q, "symbol").unwrap_or("");
                if instrument_row(symbol).is_none() {
                    return refuse(10001, "params error: symbol invalid");
                }
                let limit: usize = param(&q, "limit")
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(25);
                let (bids, asks) = book_levels(symbol, limit);
                return ok(
                    json!({"s": symbol, "a": asks, "b": bids, "ts": now_ms(), "u": 4_428_270, "seq": 831_240_640_200_i64, "cts": now_ms()}),
                );
            }
            "/v5/market/tickers" => {
                let symbol = param(&q, "symbol").unwrap_or("");
                if instrument_row(symbol).is_none() {
                    return refuse(10001, "params error: symbol invalid");
                }
                let m = mid(symbol);
                return ok(json!({"category": "linear", "list": [{
                    "symbol": symbol, "lastPrice": m.to_string(), "indexPrice": m.to_string(), "markPrice": m.to_string(),
                    "prevPrice24h": m.to_string(), "price24hPcnt": "0.000057", "highPrice24h": (m + Decimal::ONE).to_string(),
                    "lowPrice24h": (m - Decimal::ONE).to_string(), "prevPrice1h": m.to_string(), "openInterest": "62852.792",
                    "openInterestValue": "5134856892.80", "singleOpenInterest": "31426.396", "singleOpenInterestValue": "2567428446.40",
                    "turnover24h": "3610728051.8099", "volume24h": "44298.2080", "fundingRate": "0.00005253",
                    "nextFundingTime": "1792771200000", "predictedDeliveryPrice": "", "basisRate": "", "deliveryFeeRate": "",
                    "deliveryTime": "0", "ask1Size": "6.499", "bid1Price": (m - Decimal::new(1, 2)).to_string(),
                    "ask1Price": (m + Decimal::new(1, 2)).to_string(), "bid1Size": "1.945", "basis": "", "preOpenPrice": "",
                    "preQty": "", "curPreListingPhase": "", "fundingIntervalHour": "8", "basisRateYear": "", "fundingCap": "0.00333"}]}));
            }
            "/v5/market/recent-trade" => {
                let symbol = param(&q, "symbol").unwrap_or("");
                if instrument_row(symbol).is_none() {
                    return refuse(10001, "params error: symbol invalid");
                }
                let limit: usize = param(&q, "limit")
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(500);
                let t = now_ms();
                let list: Vec<Value> = (0..limit.min(3))
                    .map(|i| json!({"execId": format!("e{}", 9000 - i), "symbol": symbol, "price": mid(symbol).to_string(), "size": "0.003",
                                    "side": if i % 2 == 0 { "Buy" } else { "Sell" }, "time": (t - i as i64 * 1000).to_string(),
                                    "isBlockTrade": false, "isRPITrade": false, "seq": "1"}))
                    .collect();
                return ok(json!({"category": "linear", "list": list}));
            }
            "/v5/market/kline" => {
                let symbol = param(&q, "symbol").unwrap_or("");
                if instrument_row(symbol).is_none() {
                    return refuse(10001, "params error: symbol invalid");
                }
                let interval = param(&q, "interval").unwrap_or("1").to_string();
                let start: Option<i64> = param(&q, "start").and_then(|v| v.parse().ok());
                let end: Option<i64> = param(&q, "end").and_then(|v| v.parse().ok());
                let limit: Option<u32> = param(&q, "limit").and_then(|v| v.parse().ok());
                opts.kline_requests
                    .lock()
                    .unwrap()
                    .push((interval.clone(), start, end, limit));
                let step_ms: i64 = match interval.as_str() {
                    "D" => 86_400_000,
                    "W" => 604_800_000,
                    "M" => 2_592_000_000,
                    n => n.parse::<i64>().unwrap_or(1) * 60_000,
                };
                let to = end.unwrap_or_else(now_ms);
                let from = start.unwrap_or(to - step_ms * 200);
                // One candle on every point of the grid (aligned to the epoch) in the range asked, newest first, at most
                // `limit`: a long history gets bars that depend on the range.
                let mut rows = Vec::new();
                let mut t = (from + step_ms - 1).div_euclid(step_ms) * step_ms;
                while t <= to && rows.len() < 20_000 {
                    let n = t.div_euclid(step_ms);
                    let open = 100 + n.rem_euclid(20);
                    rows.push(json!([
                        t.to_string(),
                        open.to_string(),
                        (open + 1).to_string(),
                        (open - 1).to_string(),
                        format!("{open}.5"),
                        (1000 + n.rem_euclid(7)).to_string(),
                        "1"
                    ]));
                    t += step_ms;
                }
                rows.reverse();
                rows.truncate(limit.unwrap_or(200) as usize);
                return ok(json!({"symbol": symbol, "category": "linear", "list": rows}));
            }
            _ => {}
        }
    }

    // ── private ─────────────────────────────────────────────────────────────
    let owner = match authenticate(opts, &method, &uri, &headers, &body) {
        Ok(k) => k,
        Err(r) => return r,
    };
    let v: Value = if method == Method::POST {
        serde_json::from_slice(&body).unwrap_or(Value::Null)
    } else {
        Value::Null
    };
    opts.served.lock().unwrap().push(format!("{method} {path}"));
    let mut b = opts.book.lock().unwrap();

    match (method.as_str(), path.as_str()) {
        ("POST", "/v5/order/create") => create_order(&mut b, &owner, &v),
        ("POST", "/v5/order/amend") => {
            let key = v["orderId"]
                .as_str()
                .or(v["orderLinkId"].as_str())
                .unwrap_or("");
            let Some(idx) = b.find(&owner, key) else {
                return refuse(110001, "Order does not exist");
            };
            if !b.orders[idx].working() {
                return refuse(110008, "The order has been completed or cancelled.");
            }
            let o = &mut b.orders[idx];
            if let Some(p) = dec_of(&v["price"]) {
                o.price = Some(p);
            }
            if let Some(qty) = dec_of(&v["qty"]) {
                o.qty = qty;
            }
            if let Some(t) = dec_of(&v["triggerPrice"]) {
                o.trigger_price = Some(t);
            }
            o.updated = now_ms();
            let o = o.clone();
            b.push("order.linear", &owner, vec![o.json()]);
            ok(json!({"orderId": o.id, "orderLinkId": o.link_id}))
        }
        ("POST", "/v5/order/cancel") => {
            let key = v["orderId"]
                .as_str()
                .or(v["orderLinkId"].as_str())
                .unwrap_or("");
            let Some(idx) = b.find(&owner, key) else {
                return refuse(110001, "Order does not exist");
            };
            if !b.orders[idx].working() {
                return refuse(110010, "The order has been cancelled");
            }
            let o = &mut b.orders[idx];
            o.status = if o.status == "Untriggered" {
                "Deactivated"
            } else {
                "Cancelled"
            }
            .into();
            o.updated = now_ms();
            let o = o.clone();
            b.push("order.linear", &owner, vec![o.json()]);
            ok(json!({"orderId": o.id, "orderLinkId": o.link_id}))
        }
        ("POST", "/v5/order/cancel-all") => {
            let symbol = v["symbol"].as_str();
            let ids: Vec<usize> = b
                .orders
                .iter()
                .enumerate()
                .filter(|(_, o)| {
                    o.owner == owner && o.working() && symbol.is_none_or(|s| s == o.symbol)
                })
                .map(|(i, _)| i)
                .collect();
            let mut list = Vec::new();
            for i in ids {
                let o = &mut b.orders[i];
                o.status = if o.status == "Untriggered" {
                    "Deactivated"
                } else {
                    "Cancelled"
                }
                .into();
                o.updated = now_ms();
                let o = o.clone();
                list.push(json!({"orderId": o.id, "orderLinkId": o.link_id}));
                b.push("order.linear", &owner, vec![o.json()]);
            }
            ok(json!({"list": list, "success": "1"}))
        }
        ("GET", "/v5/order/realtime") | ("GET", "/v5/order/history") => {
            let history = path.ends_with("history");
            if !history
                && param(&q, "symbol").is_none()
                && param(&q, "settleCoin").is_none()
                && param(&q, "baseCoin").is_none()
            {
                return refuse(
                    10001,
                    "params error: symbol or settleCoin or baseCoin is required",
                );
            }
            let by_id = param(&q, "orderId").or(param(&q, "orderLinkId"));
            let limit: usize = param(&q, "limit")
                .and_then(|v| v.parse().ok())
                .unwrap_or(20)
                .clamp(1, 50);
            let from: usize = param(&q, "cursor")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);
            let rows: Vec<&MockOrder> = b
                .orders
                .iter()
                .rev()
                .filter(|o| o.owner == owner)
                .filter(|o| param(&q, "symbol").is_none_or(|s| s == o.symbol))
                .filter(|o| match by_id {
                    // Asked by id, the list answers whatever the status (the exchange keeps the latest closed ones), the
                    // history only the closed ones.
                    Some(k) => (o.id == k || o.link_id == k) && (!history || !o.working()),
                    // The list of a category holds the working orders; the history, the closed ones.
                    None => o.working() != history,
                })
                .collect();
            let page: Vec<Value> = rows
                .iter()
                .skip(from)
                .take(limit)
                .map(|o| o.json())
                .collect();
            let next = if from + limit < rows.len() {
                (from + limit).to_string()
            } else {
                String::new()
            };
            ok(json!({"category": "linear", "list": page, "nextPageCursor": next}))
        }
        ("GET", "/v5/execution/list") => {
            let start: Option<i64> = param(&q, "startTime").and_then(|v| v.parse().ok());
            let end: Option<i64> = param(&q, "endTime").and_then(|v| v.parse().ok());
            opts.execution_requests.lock().unwrap().push((start, end));
            if let (Some(s), Some(e)) = (start, end)
                && e - s > 7 * 86_400_000
            {
                return refuse(
                    10001,
                    "params error: endTime - startTime must be at most 7 days",
                );
            }
            let limit: usize = param(&q, "limit")
                .and_then(|v| v.parse().ok())
                .unwrap_or(50)
                .clamp(1, 100);
            let from: usize = param(&q, "cursor")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);
            let lo = start.unwrap_or(now_ms() - 7 * 86_400_000);
            let hi = end.unwrap_or(i64::MAX);
            let rows: Vec<&MockTrade> = b
                .trades
                .iter()
                .rev()
                .filter(|t| t.owner == owner && t.time >= lo && t.time <= hi)
                .collect();
            let page: Vec<Value> = rows
                .iter()
                .skip(from)
                .take(limit)
                .map(|t| t.json())
                .collect();
            let next = if from + limit < rows.len() {
                (from + limit).to_string()
            } else {
                String::new()
            };
            ok(json!({"category": "linear", "list": page, "nextPageCursor": next}))
        }
        ("GET", "/v5/position/list") => {
            if param(&q, "symbol").is_none() && param(&q, "settleCoin").is_none() {
                return refuse(10001, "params error: symbol or settleCoin is required");
            }
            ok(json!({"category": "linear", "list": positions_json(), "nextPageCursor": ""}))
        }
        ("GET", "/v5/account/wallet-balance") => ok(wallet_json()),
        ("POST", "/v5/account/demo-apply-money") => ok(json!({})),
        _ => json_response(StatusCode::NOT_FOUND, ""),
    }
}

fn create_order(b: &mut Book, owner: &str, v: &Value) -> Response {
    let symbol = v["symbol"].as_str().unwrap_or("");
    let Some(row) = instrument_row(symbol) else {
        return refuse(10001, "params error: symbol invalid");
    };
    let Some(qty) = dec_of(&v["qty"]) else {
        return refuse(10001, "params error: qty invalid");
    };
    let step = Decimal::from_str(row.3).unwrap();
    let min = Decimal::from_str(row.4).unwrap();
    if qty < min || !(qty / step).fract().is_zero() {
        return refuse(10001, "params error: qty invalid");
    }
    let kind = v["orderType"].as_str().unwrap_or("Limit").to_string();
    let price = dec_of(&v["price"]);
    if kind == "Limit" {
        let Some(p) = price else {
            return refuse(10001, "params error: price is required for a limit order");
        };
        if p == brokapi_conformance::REJECT_PRICE.decimal() {
            return refuse(110003, "Order price exceeds the allowable range.");
        }
        let tick = Decimal::from_str(row.2).unwrap();
        if !(p / tick).fract().is_zero() {
            return refuse(10001, "params error: price invalid");
        }
    }
    let link_id = v["orderLinkId"].as_str().unwrap_or("").to_string();
    if !link_id.is_empty()
        && b.orders
            .iter()
            .any(|o| o.owner == owner && o.link_id == link_id)
    {
        return refuse(110072, "OrderLinkedID is duplicate");
    }
    if link_id.len() > 36 {
        return refuse(10001, "params error: orderLinkId is too long");
    }
    let trigger = dec_of(&v["triggerPrice"]);
    b.next_order += 1;
    let id = format!("16160243{:011}", b.next_order);
    let now = now_ms();
    let order = MockOrder {
        owner: owner.to_string(),
        id: id.clone(),
        link_id: link_id.clone(),
        symbol: symbol.to_string(),
        side: v["side"].as_str().unwrap_or("Buy").to_string(),
        kind,
        status: if trigger.is_some() {
            "Untriggered"
        } else {
            "New"
        }
        .into(),
        price,
        trigger_price: trigger,
        trigger_direction: v["triggerDirection"].as_i64().unwrap_or(0),
        qty,
        filled: Decimal::ZERO,
        created: now,
        updated: now,
    };
    b.push("order.linear", owner, vec![order.json()]);
    b.orders.push(order);
    ok(json!({"orderId": id, "orderLinkId": link_id}))
}

// ── sockets ─────────────────────────────────────────────────────────────────

async fn public_ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<MockState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_ws(socket, state, false))
}

async fn private_ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<MockState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_ws(socket, state, true))
}

/// The first frame of a topic after its subscription is confirmed.
fn first_frames(topic: &str) -> Vec<Value> {
    let parts: Vec<&str> = topic.split('.').collect();
    match parts.as_slice() {
        ["orderbook", depth, symbol] => {
            let depth: usize = depth.parse().unwrap_or(1);
            let (bids, asks) = book_levels(symbol, depth);
            vec![
                json!({"topic": topic, "type": "snapshot", "ts": now_ms(), "data": {"s": symbol, "b": bids, "a": asks, "u": 1_000, "seq": 7},
                        "cts": now_ms()}),
            ]
        }
        ["publicTrade", symbol] => vec![
            json!({"topic": topic, "type": "snapshot", "ts": now_ms(), "data": [
            {"T": now_ms(), "s": symbol, "S": "Buy", "v": "0.074", "p": mid(symbol).to_string(), "L": "PlusTick",
             "i": format!("t{}", now_ms()), "BT": false, "RPI": false, "seq": 1}]}),
        ],
        ["kline", interval, symbol] => {
            let step: i64 = match *interval {
                "D" => 86_400_000,
                "W" => 604_800_000,
                "M" => 2_592_000_000,
                n => n.parse::<i64>().unwrap_or(1) * 60_000,
            };
            let start = now_ms() / step * step;
            let m = mid(symbol);
            vec![
                json!({"topic": topic, "type": "snapshot", "ts": now_ms(), "data": [
                {"start": start, "end": start + step - 1, "interval": interval, "open": m.to_string(), "close": (m + Decimal::new(5, 1)).to_string(),
                 "high": (m + Decimal::ONE).to_string(), "low": (m - Decimal::ONE).to_string(), "volume": "1200", "turnover": "1", "confirm": false,
                 "timestamp": now_ms()}]}),
            ]
        }
        _ => Vec::new(),
    }
}

async fn handle_ws(socket: WebSocket, state: MockState, private: bool) {
    state.options.ws_connections.fetch_add(1, Ordering::Relaxed);
    let (mut write, mut read) = socket.split();
    let (push_tx, mut push_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    let socket_id = {
        let mut b = state.options.book.lock().unwrap();
        b.next_socket += 1;
        let id = b.next_socket;
        if private {
            b.private_sockets.push((id, push_tx.clone()));
        } else {
            b.public_sockets.push((id, push_tx.clone()));
        }
        id
    };
    let mut owner: Option<String> = None;
    let mut book_frames = 0u32;
    'socket: loop {
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
                let op = req["op"].as_str().unwrap_or("").to_string();
                let req_id = req["req_id"].as_str().unwrap_or("").to_string();
                let ack = |success: bool, msg: &str| {
                    Message::Text(
                        json!({"success": success, "ret_msg": msg, "conn_id": format!("mock-{socket_id}"), "req_id": req_id, "op": op})
                            .to_string()
                            .into(),
                    )
                };
                match op.as_str() {
                    "ping" => {
                        let _ = write.send(ack(true, "pong")).await;
                    }
                    "auth" if private => {
                        let args = req["args"].as_array().cloned().unwrap_or_default();
                        let key = args
                            .first()
                            .and_then(Value::as_str)
                            .unwrap_or("")
                            .to_string();
                        let expires = args.get(1).and_then(Value::as_i64).unwrap_or(0);
                        let sig = args.get(2).and_then(Value::as_str).unwrap_or("");
                        let good = brokapi_bybit_protocol::sign::sign(
                            SECRET,
                            &format!("GET/realtime{expires}"),
                        ) == sig
                            && key.starts_with(DEFAULT_KEY)
                            && expires
                                > now_ms() + state.options.clock_offset_ms.load(Ordering::Relaxed)
                            && !state.options.refuse_ws_login.load(Ordering::Relaxed);
                        if good {
                            owner = Some(key);
                            let _ = write.send(ack(true, "")).await;
                        } else {
                            let _ = write.send(ack(false, "Params Error")).await;
                            let _ = write.send(Message::Close(None)).await;
                            break 'socket;
                        }
                    }
                    "subscribe" | "unsubscribe" => {
                        if private && owner.is_none() {
                            let _ = write.send(ack(false, "Request not authorized")).await;
                            continue;
                        }
                        let topics: Vec<String> = req["args"]
                            .as_array()
                            .map(|a| {
                                a.iter()
                                    .filter_map(|t| t.as_str().map(str::to_owned))
                                    .collect()
                            })
                            .unwrap_or_default();
                        if op == "unsubscribe" {
                            {
                                let mut b = state.options.book.lock().unwrap();
                                b.subs.retain(|s| {
                                    !(s.socket == socket_id && topics.contains(&s.topic))
                                });
                            }
                            let _ = write.send(ack(true, "")).await;
                            continue;
                        }
                        // The subscription is made and confirmed when the exchange gets to it.
                        state.options.stream_gate.pass().await;
                        let refuse_text = state.options.fail_subscribe_text.lock().await.clone();
                        let valid = |t: &str| {
                            let parts: Vec<&str> = t.split('.').collect();
                            match (private, parts.as_slice()) {
                                (
                                    true,
                                    ["order" | "execution" | "position", "linear"] | ["wallet"],
                                ) => true,
                                (false, ["orderbook", d, s]) => {
                                    ["1", "50", "200", "1000"].contains(d)
                                        && instrument_row(s).is_some()
                                }
                                (false, ["publicTrade", s]) => instrument_row(s).is_some(),
                                (false, ["kline", _, s]) => instrument_row(s).is_some(),
                                _ => false,
                            }
                        };
                        if refuse_text
                            .as_ref()
                            .is_some_and(|r| text.contains(r.as_str()))
                            || !topics.iter().all(|t| valid(t))
                        {
                            let _ = write
                                .send(ack(
                                    false,
                                    &format!("error:handler not found,topic:{}", topics.join(",")),
                                ))
                                .await;
                            continue;
                        }
                        if private {
                            let mut b = state.options.book.lock().unwrap();
                            for t in &topics {
                                b.subs.push(SocketSub {
                                    socket: socket_id,
                                    owner: owner.clone().unwrap_or_default(),
                                    topic: t.clone(),
                                    tx: push_tx.clone(),
                                });
                            }
                        }
                        let _ = write.send(ack(true, "")).await;
                        for t in &topics {
                            for f in first_frames(t) {
                                let _ = write.send(Message::Text(f.to_string().into())).await;
                                if t.starts_with("orderbook") {
                                    book_frames += 1;
                                    state.options.book_frames.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                        let limit = state
                            .options
                            .disconnect_after_data_frames
                            .load(Ordering::Relaxed);
                        if !private && limit > 0 && book_frames >= limit {
                            let _ = write.send(Message::Close(None)).await;
                            break 'socket;
                        }
                    }
                    _ => {
                        let _ = write.send(ack(false, "unknown op")).await;
                    }
                }
            }
            Ok(Message::Ping(payload)) => {
                let _ = write.send(Message::Pong(payload)).await;
            }
            Ok(Message::Close(_)) | Err(_) => break,
            _ => {}
        }
    }
    let mut b = state.options.book.lock().unwrap();
    b.subs.retain(|s| s.socket != socket_id);
    b.private_sockets.retain(|(id, _)| *id != socket_id);
    b.public_sockets.retain(|(id, _)| *id != socket_id);
}
