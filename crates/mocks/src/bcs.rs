//! Axum HTTP + WebSocket mock of the BCS contract surface used by the connectors.
//!
//! The mock is stateful for orders, like the broker: every account (named by its access token) has its
//! own orders and trades. It speaks the CURRENT order methods of the docs (checked 2026-10-19):
//! `POST /orders`, `/orders/sync`, `/orders/cancel(/sync)`, `/orders/edit(/sync)`, `GET /orders`,
//! `POST /orders/search`, `POST /trades/search`, and the `orders/events/ws` channel. The deprecated
//! forms (`/orders/{id}/cancel`, `/orders/{id}`) are NOT served (404, counted in `deprecated_calls`),
//! so a connector that still calls them fails its tests. Conditional orders (`orderType = "5"`) are
//! kept with their stop/take prices.
//!
//! Test hooks: [`MockServer::fill`] executes an order (status, trade, event), [`MockServer::push_event`]
//! writes a raw message to the event sockets. A limit order at `REJECT_PRICE` is refused with HTTP 400,
//! at [`REJECT_IN_BODY_PRICE`] with HTTP 200 and `orderStatus = 8` (the two ways a broker can say no).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::http::{HeaderMap, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get};
use brokapi_bcs_protocol::http::client::AUTH_TOKEN_PATH;
use brokapi_bcs_protocol::ws::client::BcsWsChannel;
use brokapi_core::{Decimal, Price, Qty};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::str::FromStr;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

const ORDER_BOOK_PATH: &str = "/trade-api-market-data-connector/api/v1/order-book";
const CANDLES_PATH: &str = "/trade-api-market-data-connector/api/v1/candles-chart";
const LAST_TRADES_PATH: &str = "/trade-api-market-data-connector/api/v1/last-trades";
const PORTFOLIO_PATH: &str = "/trade-api-bff-portfolio/api/v1/portfolio";
const ORDERS_PATH: &str = "/trade-api-bff-operations/api/v1/orders";
const ORDERS_SEARCH_PATH: &str = "/trade-api-bff-order-details/api/v1/orders/search";
const TRADES_SEARCH_PATH: &str = "/trade-api-bff-trade-details/api/v1/trades/search";
const QUOTES_PATH: &str = "/trade-api-market-data-connector/api/v1/quotes";
const LIMITS_PATH: &str = "/trade-api-bff-limit/api/v1/limits";
const DISCOUNTS_PATH: &str = "/trade-api-bff-marginal-indicators/api/v1/instruments-discounts";
const OPERATIONS_SEARCH_PATH: &str = "/trade-api-bff-nontrade-operations/api/v1/operations/search";
const INSTRUMENTS_TICKERS_PATH: &str =
    "/trade-api-information-service/api/v1/instruments/by-tickers";
const INSTRUMENTS_ISINS_PATH: &str = "/trade-api-information-service/api/v1/instruments/by-isins";
const INSTRUMENTS_TYPE_PATH: &str = "/trade-api-information-service/api/v1/instruments/by-type";
const TRADING_STATUS_PATH: &str = "/trade-api-information-service/api/v1/trading-schedule/status";
const DAILY_SCHEDULE_PATH: &str =
    "/trade-api-information-service/api/v1/trading-schedule/daily-schedule";

/// A limit order at this price is refused with HTTP 200 and `orderStatus = 8` (`rejectReason` set).
pub const REJECT_IN_BODY_PRICE: Decimal = Decimal::from_parts(1338, 0, 0, false, 2);

/// Access token of the default account (refresh token `mock-refresh-token`).
pub const DEFAULT_ACCESS: &str = "mock-access-token";

/// Tunable mock behaviour for individual test scenarios.
#[derive(Debug, Default)]
pub struct MockOptions {
    /// When set, REST `order-book` returns HTTP 429.
    pub rate_limit_order_book: AtomicBool,
    /// Subscribe requests containing this ticker get an error frame.
    pub fail_subscribe_ticker: Mutex<Option<String>>,
    /// Close WS after N pushed market-data frames (per connection).
    pub disconnect_after_data_frames: AtomicU32,
    /// Count of established WS sessions (all channels).
    pub ws_connections: AtomicU32,
    /// Market-data book frames pushed over WS.
    pub ws_data_frames: AtomicU32,
    /// `POST /quotes` calls served (a method no connector wraps, used to test `raw()`).
    pub quotes_calls: AtomicU32,
    /// Authorization header of every `POST /quotes`.
    pub quotes_auth: std::sync::Mutex<Vec<String>>,
    /// Requests the mock got for the DEPRECATED order methods (`/orders/{id}/cancel`, `/orders/{id}`).
    /// They are answered 404; a connector on current methods leaves this at 0.
    pub deprecated_calls: AtomicU32,
    /// Every `"METHOD /path"` the order endpoints served, in order. For asserting which methods a
    /// connector uses (sync or not).
    pub served: std::sync::Mutex<Vec<String>>,
    /// When above 0, `GET /instruments/by-type` has this many instruments and serves them by `page` and `size`
    /// (`size` 1..100). Default 0: one record, whatever the page.
    pub by_type_total: AtomicU32,
    /// Every `(page, size)` the by-type list was asked for.
    pub by_type_asked: std::sync::Mutex<Vec<(Option<u32>, Option<u32>)>>,
    /// Orders, trades and open event sockets of all accounts.
    book: std::sync::Mutex<Book>,
}

impl MockOptions {
    pub fn enable_rate_limit_order_book(&self) {
        self.rate_limit_order_book.store(true, Ordering::Relaxed);
    }

    pub fn ws_connections(&self) -> u32 {
        self.ws_connections.load(Ordering::Relaxed)
    }

    /// Open `orders/events/ws` sockets of all accounts.
    pub fn event_sockets(&self) -> usize {
        let mut b = self.book.lock().unwrap();
        b.sockets.retain(|(_, tx)| !tx.is_closed());
        b.sockets.len()
    }

    /// Number of requests served for `"METHOD /orders..."`, e.g. `"POST /orders/sync"`.
    pub fn served_count(&self, what: &str) -> usize {
        self.served
            .lock()
            .unwrap()
            .iter()
            .filter(|s| s.as_str() == what)
            .count()
    }
}

/// One order at the mock broker.
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

#[derive(Debug, Clone)]
struct MockOrder {
    owner: String,
    cid: String,
    order_id: String,
    ticker: String,
    class_code: String,
    side: String,
    /// `1` market, `2` limit, `5` conditional.
    order_type: String,
    price: Option<Decimal>,
    stop_price: Option<Decimal>,
    take_price: Option<Decimal>,
    exec_stop: Option<i64>,
    exec_take: Option<i64>,
    qty: Decimal,
    executed: Decimal,
    /// `0` new, `1` partly filled, `2` filled, `4` cancelled, `5` replaced, `8` rejected.
    status: String,
    reject_reason: Option<String>,
}

impl MockOrder {
    fn is_conditional(&self) -> bool {
        self.order_type == "5"
    }

    fn active(&self) -> bool {
        matches!(self.status.as_str(), "0" | "1")
    }

    /// `orderStatus` of the order list: 1 cancelled, 2 executed, 3 active.
    fn list_status(&self) -> i32 {
        match self.status.as_str() {
            "0" | "1" => 3,
            "2" => 2,
            _ => 1,
        }
    }

    /// `orderType` of the order list: 1 market, 2 limit, 4 stop-limit, 5 take-profit, 6 stop-loss,
    /// 7 take-profit and stop-loss.
    fn list_type(&self) -> i32 {
        if !self.is_conditional() {
            return self.order_type.parse().unwrap_or(2);
        }
        match (self.stop_price.is_some(), self.take_price.is_some()) {
            (true, true) => 7,
            (false, true) => 5,
            _ if self.exec_stop == Some(2) => 4,
            _ => 6,
        }
    }

    fn data(&self, execution_type: &str, last_qty: Decimal, execution_id: Option<&str>) -> Value {
        json!({
            "messageType": "8",
            "orderStatus": self.status,
            "executionType": execution_type,
            "orderQuantity": num(self.qty),
            "executedQuantity": num(self.executed),
            "lastQuantity": num(last_qty),
            "remainedQuantity": num(self.qty - self.executed),
            "ticker": self.ticker,
            "classCode": self.class_code,
            "side": self.side,
            "orderType": self.order_type,
            "averagePrice": num(self.price.unwrap_or(Decimal::ZERO)),
            "orderId": self.order_id,
            "executionId": execution_id,
            "price": num(self.price.unwrap_or(Decimal::ZERO)),
            "stopPrice": self.stop_price.map(num),
            "takePrice": self.take_price.map(num),
            "rejectReason": self.reject_reason,
            "currency": "RUB",
            "transactionTime": now_ms(),
            "securityExchange": self.class_code,
        })
    }

    /// Answer of `GET /orders` and of the sync methods: `data` is a list of reports.
    fn state_list(&self, request_id: &str) -> Value {
        json!({
            "clientOrderId": request_id,
            "originalClientOrderId": self.cid,
            "data": [self.data("12", Decimal::ZERO, None)],
        })
    }

    fn list_record(&self) -> Value {
        json!({
            "orderNum": self.order_id.rsplit('-').next().and_then(|n| n.parse::<i64>().ok()),
            "orderId": self.order_id,
            "clientCode": "mock",
            "orderDateTime": now_ms(),
            "ticker": self.ticker,
            "classCode": self.class_code,
            "side": self.side.parse::<i32>().unwrap_or(1),
            "orderType": self.list_type(),
            "orderStatus": self.list_status(),
            "price": self.price.map(num),
            "stopPrice": self.stop_price.map(num),
            "takePrice": self.take_price.map(num),
            "marketStopLoss": self.exec_stop,
            "marketTakeProfit": self.exec_take,
            "orderQuantity": num(self.qty),
            "executedQuantity": num(self.executed),
            "remainedQuantity": num(self.qty - self.executed),
            "rejectReason": self.reject_reason,
        })
    }
}

#[derive(Debug, Clone)]
struct MockTrade {
    owner: String,
    order_no: i64,
    trade_no: i64,
    ticker: String,
    class_code: String,
    side: String,
    price: Decimal,
    qty: Decimal,
}

#[derive(Debug, Default)]
struct Book {
    orders: Vec<MockOrder>,
    trades: Vec<MockTrade>,
    next_order: i64,
    next_trade: i64,
    /// Open `orders/events/ws` sockets: (access token of the account, sender).
    sockets: Vec<(String, tokio::sync::mpsc::UnboundedSender<String>)>,
}

impl Book {
    fn push(&mut self, owner: &str, text: &str) {
        self.sockets.retain(|(_, tx)| !tx.is_closed());
        for (o, tx) in &self.sockets {
            if o == owner {
                let _ = tx.send(text.to_owned());
            }
        }
    }

    fn event(
        &mut self,
        o: &MockOrder,
        execution_type: &str,
        last_qty: Decimal,
        execution_id: Option<&str>,
    ) {
        let msg = json!({
            "originalClientOrderId": o.cid,
            "clientOrderId": o.cid,
            "data": o.data(execution_type, last_qty, execution_id),
        });
        self.push(&o.owner.clone(), &msg.to_string());
    }

    fn find(&self, owner: &str, id_type: &str, id: &str) -> Option<usize> {
        self.orders.iter().position(|o| {
            o.owner == owner
                && if id_type == "2" {
                    o.order_id == id
                } else {
                    o.cid == id
                }
        })
    }
}

#[derive(Clone)]
struct MockState {
    options: Arc<MockOptions>,
}

/// Running mock server bound to `127.0.0.1:0`.
pub struct MockServer {
    pub http_base: String,
    options: Arc<MockOptions>,
    cancel: CancellationToken,
    _task: JoinHandle<()>,
}

impl MockServer {
    pub async fn start(options: Arc<MockOptions>) -> Self {
        let state = MockState {
            options: Arc::clone(&options),
        };

        let app = Router::new()
            .route("/trade-api-bff-limit/api/v1/limits/ws", get(ws_handler))
            .route(
                "/trade-api-bff-portfolio/api/v1/portfolio/ws",
                get(ws_handler),
            )
            .route(
                "/trade-api-market-data-connector/api/v1/market-data/ws",
                get(ws_handler),
            )
            .route(
                "/trade-api-bff-operations/api/v1/orders/execution/ws",
                get(ws_handler),
            )
            .route(
                "/trade-api-bff-operations/api/v1/orders/transaction/ws",
                get(ws_handler),
            )
            .route(
                "/trade-api-bff-operations/api/v1/orders/events/ws",
                get(ws_handler),
            )
            .route(
                "/trade-api-bff-marginal-indicators/api/v1/marginal-indicators/ws",
                get(ws_handler),
            )
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
}

impl MockServer {
    /// Pushes a raw message to every open `orders/events/ws` socket (all accounts).
    pub fn push_event(&self, text: &str) {
        let mut b = self.options.book.lock().unwrap();
        b.sockets.retain(|(_, tx)| !tx.is_closed());
        for (_, tx) in &b.sockets {
            let _ = tx.send(text.to_owned());
        }
    }

    /// Executes `qty` of the order `cid` of the default account at `price`: the order becomes
    /// partly filled or filled, a trade is recorded, a fill event goes to the account's sockets.
    /// Returns the `executionId`.
    pub fn fill(&self, cid: &str, qty: Qty, price: Price) -> String {
        self.fill_for(DEFAULT_ACCESS, cid, qty, price)
    }

    pub fn fill_for(&self, owner: &str, cid: &str, qty: Qty, price: Price) -> String {
        let (qty, price) = (qty.decimal(), price.decimal());
        let mut b = self.options.book.lock().unwrap();
        let idx = b.find(owner, "1", cid).expect("fill: unknown order");
        b.next_trade += 1;
        let trade_no = b.next_trade;
        let exec_id = format!("TQBR-MOCK-{trade_no}");
        let o = &mut b.orders[idx];
        assert!(o.active(), "fill: order is not active");
        o.executed = (o.executed + qty).min(o.qty);
        o.price = o.price.or(Some(price));
        o.status = if o.executed >= o.qty {
            "2".into()
        } else {
            "1".into()
        };
        let o = o.clone();
        b.trades.push(MockTrade {
            owner: owner.to_string(),
            order_no: o
                .order_id
                .rsplit('-')
                .next()
                .and_then(|n| n.parse().ok())
                .unwrap_or(0),
            trade_no,
            ticker: o.ticker.clone(),
            class_code: o.class_code.clone(),
            side: o.side.clone(),
            price,
            qty,
        });
        let et = if o.status == "2" { "2" } else { "1" };
        b.event(&o, et, qty, Some(&exec_id));
        exec_id
    }
}

impl Drop for MockServer {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<MockState>,
    headers: HeaderMap,
    uri: Uri,
) -> impl IntoResponse {
    let path = uri.path().to_string();
    let owner = bearer(&headers).unwrap_or_default();
    ws.on_upgrade(move |socket| handle_ws(socket, path, owner, state))
}

fn bearer(headers: &HeaderMap) -> Option<String> {
    headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::to_owned)
}

async fn http_handler(
    State(state): State<MockState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if uri.path() == QUOTES_PATH && method == Method::POST {
        let auth = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        state.options.quotes_auth.lock().unwrap().push(auth.clone());
        if auth != format!("Bearer {DEFAULT_ACCESS}") {
            return json_response(
                StatusCode::UNAUTHORIZED,
                r#"{"type":"UNAUTHORIZED","traceId":"mock"}"#,
            );
        }
        state.options.quotes_calls.fetch_add(1, Ordering::SeqCst);
        return json_response(
            StatusCode::OK,
            r#"{"records":[{"ticker":"SBER","classCode":"TQBR","bid":300.1,"offer":300.2,"last":300.15}]}"#,
        );
    }
    handle_http(state, method, uri, headers, body).await
}

fn not_found() -> Response {
    json_response(
        StatusCode::NOT_FOUND,
        r#"{"type":"NOT_FOUND","traceId":"mock"}"#,
    )
}

fn validation(field: &str, message: &str) -> Response {
    json_response(
        StatusCode::BAD_REQUEST,
        &json!({"type":"VALIDATION_ERROR","traceId":"mock","errors":[{"type":"INVALID_VALUE","field":field,"payload":{"message":message}}]}).to_string(),
    )
}

fn query_param(uri: &Uri, name: &str) -> Option<String> {
    uri.query()?
        .split('&')
        .filter_map(|kv| kv.split_once('='))
        .find(|(k, _)| *k == name)
        .map(|(_, v)| v.to_owned())
}

fn now_ms() -> String {
    chrono::Utc::now()
        .format("%Y-%m-%dT%H:%M:%S%.3fZ")
        .to_string()
}

async fn handle_http(
    state: MockState,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let path = uri.path();

    if path == AUTH_TOKEN_PATH && method == Method::POST {
        // The refresh token names the account: `mock-refresh-token` is the default one, any other
        // `mock-refresh-X` is the account `mock-access-X`.
        let text = String::from_utf8_lossy(&body);
        let refresh = text
            .split('&')
            .filter_map(|kv| kv.split_once('='))
            .find(|(k, _)| *k == "refresh_token")
            .map(|(_, v)| v.to_owned())
            .unwrap_or_else(|| "mock-refresh-token".into());
        let access = match refresh.strip_prefix("mock-refresh-") {
            Some("token") | None => DEFAULT_ACCESS.to_owned(),
            Some(x) => format!("mock-access-{x}"),
        };
        return json_response(
            StatusCode::OK,
            &json!({
                "access_token": access,
                "expires_in": 86400,
                "refresh_expires_in": 7776000,
                "refresh_token": refresh,
                "token_type": "bearer",
            })
            .to_string(),
        );
    }

    if path == ORDER_BOOK_PATH && method == Method::GET {
        if state.options.rate_limit_order_book.load(Ordering::Relaxed) {
            return StatusCode::TOO_MANY_REQUESTS.into_response();
        }
        return json_response(StatusCode::OK, &fixture("rest/order_book.json"));
    }

    if path == PORTFOLIO_PATH && method == Method::GET {
        return json_response(StatusCode::OK, &fixture("ws/portfolio.json"));
    }

    if path == CANDLES_PATH && method == Method::GET {
        // The docs' timeFrame enum: M1 M5 M15 M30 H1 H4 D W MN (no M10, no D1 / W1).
        let allowed = ["M1", "M5", "M15", "M30", "H1", "H4", "D", "W", "MN"];
        match query_param(&uri, "timeFrame") {
            Some(tf) if allowed.contains(&tf.as_str()) => {}
            _ => return validation("timeFrame", "not one of M1 M5 M15 M30 H1 H4 D W MN"),
        }
        let ticker = query_param(&uri, "ticker").unwrap_or_else(|| "SBER".into());
        let class_code = query_param(&uri, "classCode").unwrap_or_else(|| "TQBR".into());
        let tf = query_param(&uri, "timeFrame").unwrap_or_else(|| "M1".into());
        let bars: Vec<Value> = (0..3)
            .map(|i| {
                json!({
                    "time": format!("2026-10-19T07:{:02}:00.000Z", i),
                    "open": 300.0 + i as f64, "close": 300.5 + i as f64,
                    "high": 301.0 + i as f64, "low": 299.5 + i as f64, "volume": 1000.0 + i as f64,
                })
            })
            .collect();
        return json_response(
            StatusCode::OK,
            &json!({
                "ticker": ticker, "classCode": class_code, "timeFrame": tf,
                "startDate": "2026-10-19T07:00:00.000Z", "endDate": "2026-10-19T08:00:00.000Z",
                "bars": bars,
            })
            .to_string(),
        );
    }

    if path == LAST_TRADES_PATH && method == Method::POST {
        // The documented request: ticker and classCode required, no `limit`; the answer is `{records}`.
        let req: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
        if req["ticker"].as_str().is_none() || req["classCode"].as_str().is_none() {
            return validation("ticker", "ticker and classCode are required");
        }
        if !req["limit"].is_null() {
            return validation("limit", "unknown field");
        }
        return json_response(StatusCode::OK, &fixture("rest/last_trades_records.json"));
    }

    if path == TRADING_STATUS_PATH && method == Method::GET {
        if query_param(&uri, "classCode").is_none() {
            return validation("classCode", "required");
        }
        return json_response(
            StatusCode::OK,
            r#"{"tradingSessionTypeId":1,"tradingSessionType":"MAIN","tradingSessionStatus":"OPEN","nextSessionDate":"2026-10-20T07:00:00.000Z"}"#,
        );
    }

    if path == DAILY_SCHEDULE_PATH && method == Method::GET {
        // classCode AND ticker are required; the times are times of day, the types Russian names.
        if query_param(&uri, "classCode").is_none()
            || query_param(&uri, "ticker").is_none_or(|t| t.is_empty())
        {
            return validation("ticker", "classCode and ticker are required");
        }
        return json_response(
            StatusCode::OK,
            r#"{"isWorkDay":true,"dailySchedule":[
                {"startDate":"09:50:00","endDate":"10:00:00","tradingSessionType":"Аукцион открытия","tradingSessionStatus":"OPEN"},
                {"startDate":"10:00:00","endDate":"18:50:00","tradingSessionType":"Торговый период","tradingSessionStatus":"OPEN"},
                {"startDate":"18:50:00","endDate":"19:05:00","tradingSessionType":"Технический перерыв QUIK","tradingSessionStatus":"CLOSE"},
                {"startDate":"19:05:00","endDate":"23:50:00","tradingSessionType":"Вечерняя торговая сессия","tradingSessionStatus":"OPEN"}]}"#,
        );
    }

    // Reference reads with fixed answers (limits, discounts, non-trade operations).
    if path == LIMITS_PATH && method == Method::GET {
        return json_response(
            StatusCode::OK,
            r#"{"depoLimit":[{"ticker":"SBER","classCode":"TQBR","exchange":"MOEX","averagePrice":300.0,
                "quantity":{"type":"T365","value":10},"quantityBatch":{"type":"T365","value":1},"instrumentType":"STOCK"}],
                "moneyLimits":[{"exchange":"MOEX","currencyCode":"RUB","locked":1000.0,
                "quantity":{"type":"T0","value":50000.0},"instrumentType":"CURRENCY"}],
                "futuresLimits":[]}"#,
        );
    }
    if path == DISCOUNTS_PATH && method == Method::GET {
        return json_response(
            StatusCode::OK,
            r#"[{"ticker":"SBER","discountLong":0.25,"discountShort":0.35}]"#,
        );
    }
    if path == OPERATIONS_SEARCH_PATH && method == Method::POST {
        return json_response(
            StatusCode::OK,
            r#"{"records":[{"id":"op-1","date":"2026-10-19","type":"PayIn","status":"Approved","sum":1000.0,
                "currency":"RUB","balanceChange":"Positive"}],"pageSize":50}"#,
        );
    }

    if (path.starts_with(ORDERS_PATH) || path == ORDERS_SEARCH_PATH || path == TRADES_SEARCH_PATH)
        && let Some(resp) = handle_orders(&state, &method, &uri, &headers, &body)
    {
        return resp;
    }

    if path == INSTRUMENTS_TICKERS_PATH && method == Method::POST {
        let wrapped = format!("[{}]", fixture("rest/instrument_info.json"));
        return json_response(StatusCode::OK, &wrapped);
    }
    if path == INSTRUMENTS_ISINS_PATH && method == Method::POST {
        let wrapped = format!("[{}]", fixture("rest/instrument_info.json"));
        return json_response(StatusCode::OK, &wrapped);
    }
    if path == INSTRUMENTS_TYPE_PATH && method == Method::GET {
        let allowed = [
            "CURRENCY",
            "STOCK",
            "FOREIGN_STOCK",
            "BONDS",
            "NOTES",
            "DEPOSITARY_RECEIPTS",
            "EURO_BONDS",
            "MUTUAL_FUNDS",
            "ETF",
            "FUTURES",
            "OPTIONS",
            "GOODS",
            "INDICES",
        ];
        match query_param(&uri, "type") {
            Some(t) if allowed.contains(&t.as_str()) => {}
            _ => return validation("type", "not one of the documented instrument types"),
        }
        let page = query_param(&uri, "page").and_then(|p| p.parse::<u32>().ok());
        let size = query_param(&uri, "size").and_then(|p| p.parse::<u32>().ok());
        state
            .options
            .by_type_asked
            .lock()
            .unwrap()
            .push((page, size));
        let total = state.options.by_type_total.load(Ordering::Relaxed);
        if total > 0 {
            // The docs: size 1..100 (default 50), pages from 0, a short page ends the list.
            let size = size.unwrap_or(50);
            if !(1..=100).contains(&size) {
                return validation("size", "must be between 1 and 100");
            }
            let from = page.unwrap_or(0).saturating_mul(size).min(total);
            let to = (from + size).min(total);
            let base: Value = serde_json::from_str(&fixture("rest/instrument_info.json")).unwrap();
            let items: Vec<Value> = (from..to)
                .map(|i| {
                    let mut item = base.clone();
                    item["ticker"] = json!(format!("T{i:05}"));
                    item
                })
                .collect();
            return json_response(StatusCode::OK, &Value::Array(items).to_string());
        }
        let wrapped = format!("[{}]", fixture("rest/instrument_info.json"));
        return json_response(StatusCode::OK, &wrapped);
    }

    (
        StatusCode::NOT_FOUND,
        format!("mock: unhandled {} {}", method, path),
    )
        .into_response()
}

/// The order endpoints. `None` = not an order path.
fn handle_orders(
    state: &MockState,
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    body: &Bytes,
) -> Option<Response> {
    let path = uri.path();
    let rest = path.strip_prefix(ORDERS_PATH);
    let owner = bearer(headers);
    if owner
        .as_deref()
        .is_none_or(|o| !o.starts_with("mock-access"))
    {
        return Some(json_response(
            StatusCode::UNAUTHORIZED,
            r#"{"type":"UNAUTHORIZED","traceId":"mock"}"#,
        ));
    }
    let owner = owner.unwrap_or_default();
    let parsed: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
    let opts = &state.options;
    let mut book = opts.book.lock().unwrap();

    if path == ORDERS_SEARCH_PATH && *method == Method::POST {
        opts.served
            .lock()
            .unwrap()
            .push("POST /orders/search".into());
        let ints = |key: &str| -> Vec<i64> {
            parsed[key]
                .as_array()
                .map(|a| a.iter().filter_map(Value::as_i64).collect())
                .unwrap_or_default()
        };
        let (wanted_status, wanted_types) = (ints("orderStatus"), ints("orderTypes"));
        let wanted_tickers: Vec<String> = parsed["tickers"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(str::to_owned))
                    .collect()
            })
            .unwrap_or_default();
        let all: Vec<&MockOrder> = book
            .orders
            .iter()
            .filter(|o| o.owner == owner && o.status != "8")
            .filter(|o| {
                wanted_status.is_empty() || wanted_status.contains(&(o.list_status() as i64))
            })
            .filter(|o| wanted_types.is_empty() || wanted_types.contains(&(o.list_type() as i64)))
            .filter(|o| wanted_tickers.is_empty() || wanted_tickers.contains(&o.ticker))
            .collect();
        let size = query_param(uri, "size")
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(50)
            .max(1);
        let page = query_param(uri, "page")
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(0);
        let records: Vec<Value> = all
            .iter()
            .skip(page * size)
            .take(size)
            .map(|o| o.list_record())
            .collect();
        return Some(json_response(
            StatusCode::OK,
            &json!({"records": records, "totalRecords": all.len(), "totalPages": all.len().div_ceil(size)}).to_string(),
        ));
    }

    if path == TRADES_SEARCH_PATH && *method == Method::POST {
        opts.served
            .lock()
            .unwrap()
            .push("POST /trades/search".into());
        let records: Vec<Value> = book
            .trades
            .iter()
            .filter(|t| t.owner == owner)
            .map(|t| {
                json!({
                    "orderNum": t.order_no, "tradeNum": t.trade_no, "ticker": t.ticker, "classCode": t.class_code,
                    "side": t.side, "price": num(t.price), "volume": num(t.price * t.qty), "tradeQuantity": num(t.qty),
                    "tradeDateTime": now_ms(), "instrumentType": "STOCK", "clientCode": "mock",
                })
            })
            .collect();
        let n = records.len();
        return Some(json_response(
            StatusCode::OK,
            &json!({"records": records, "totalRecords": n, "totalPages": if n == 0 { 0 } else { 1 }}).to_string(),
        ));
    }

    let rest = rest?;
    opts.served
        .lock()
        .unwrap()
        .push(format!("{} /orders{}", method, rest));

    // Anything but the current methods is a deprecated form: `/orders/{id}/cancel`, `/orders/{id}`.
    let is_current = matches!(
        (method.as_str(), rest),
        ("POST", "")
            | ("POST", "/sync")
            | ("POST", "/cancel")
            | ("POST", "/cancel/sync")
            | ("POST", "/edit")
            | ("POST", "/edit/sync")
            | ("GET", "")
    );
    if !is_current {
        opts.deprecated_calls.fetch_add(1, Ordering::SeqCst);
        return Some(not_found());
    }

    let sync = rest.ends_with("/sync");

    // GET /orders?orderIdType=&orderId=
    if *method == Method::GET {
        let id_type = query_param(uri, "orderIdType").unwrap_or_else(|| "1".into());
        let Some(id) = query_param(uri, "orderId") else {
            return Some(validation("orderId", "required"));
        };
        return Some(match book.find(&owner, &id_type, &id) {
            Some(i) => json_response(StatusCode::OK, &book.orders[i].state_list(&id).to_string()),
            None => not_found(),
        });
    }

    // Create.
    if rest.is_empty() || rest == "/sync" {
        let price = dec_of(&parsed["price"]);
        let order_type = parsed["orderType"].as_str().unwrap_or("2").to_owned();
        if price == Some(brokapi_conformance::REJECT_PRICE.decimal()) {
            return Some(validation("price", "rejected by mock"));
        }
        let Some(cid) = parsed["clientOrderId"].as_str().map(str::to_owned) else {
            return Some(validation("clientOrderId", "required"));
        };
        if order_type == "5" && parsed["stopPrice"].is_null() && parsed["takePrice"].is_null() {
            return Some(validation(
                "stopPrice",
                "a conditional order needs stopPrice or takePrice",
            ));
        }
        if book.orders.iter().any(|o| o.owner == owner && o.cid == cid) {
            return Some(validation("clientOrderId", "duplicate"));
        }
        book.next_order += 1;
        let n = book.next_order;
        let class_code = parsed["classCode"].as_str().unwrap_or("TQBR").to_owned();
        let mut o = MockOrder {
            owner: owner.clone(),
            cid: cid.clone(),
            order_id: format!("20261019-{class_code}-{}", 79_000_000 + n),
            ticker: parsed["ticker"].as_str().unwrap_or("SBER").to_owned(),
            class_code,
            side: parsed["side"].as_str().unwrap_or("1").to_owned(),
            order_type,
            price,
            stop_price: dec_of(&parsed["stopPrice"]),
            take_price: dec_of(&parsed["takePrice"]),
            exec_stop: parsed["executionStopLossType"].as_i64(),
            exec_take: parsed["executionTakeProfitType"].as_i64(),
            qty: dec_of(&parsed["orderQuantity"]).unwrap_or(Decimal::ONE),
            executed: Decimal::ZERO,
            status: "0".into(),
            reject_reason: None,
        };
        if price == Some(REJECT_IN_BODY_PRICE) {
            o.status = "8".into();
            o.reject_reason = Some("price is outside the allowed range".into());
        }
        book.orders.push(o.clone());
        let et = if o.status == "8" { "8" } else { "0" };
        book.event(&o, et, Decimal::ZERO, None);
        let resp = if sync {
            o.state_list(&cid)
        } else {
            json!({"clientOrderId": cid, "status": "OK"})
        };
        return Some(json_response(StatusCode::OK, &resp.to_string()));
    }

    // Cancel and edit name the order in the body.
    let id_type = parsed["orderIdType"].as_str().unwrap_or("1").to_owned();
    let Some(id) = parsed["orderId"].as_str().map(str::to_owned) else {
        return Some(validation("orderId", "required"));
    };
    let Some(request_id) = parsed["clientOrderId"].as_str().map(str::to_owned) else {
        return Some(validation("clientOrderId", "required"));
    };
    let Some(idx) = book.find(&owner, &id_type, &id) else {
        return Some(not_found());
    };
    if !book.orders[idx].active() {
        return Some(validation("orderId", "the order is not active"));
    }

    if rest.starts_with("/cancel") {
        book.orders[idx].status = "4".into();
        let o = book.orders[idx].clone();
        book.event(&o, "4", Decimal::ZERO, None);
        let resp = if sync {
            o.state_list(&request_id)
        } else {
            json!({"clientOrderId": request_id, "status": "OK"})
        };
        return Some(json_response(StatusCode::OK, &resp.to_string()));
    }

    // Edit: the original is replaced, a new order appears under the new cid.
    if book
        .orders
        .iter()
        .any(|o| o.owner == owner && o.cid == request_id)
    {
        return Some(validation("clientOrderId", "duplicate"));
    }
    if dec_of(&parsed["price"]) == Some(brokapi_conformance::REJECT_PRICE.decimal()) {
        return Some(validation("price", "rejected by mock"));
    }
    book.orders[idx].status = "5".into();
    let old = book.orders[idx].clone();
    book.next_order += 1;
    let mut new = old.clone();
    new.cid = request_id.clone();
    new.order_id = format!(
        "20261019-{}-{}",
        old.class_code,
        79_000_000 + book.next_order
    );
    new.status = "0".into();
    new.executed = Decimal::ZERO;
    if let Some(p) = dec_of(&parsed["price"]) {
        new.price = Some(p);
    }
    if let Some(q) = dec_of(&parsed["orderQuantity"]) {
        new.qty = q;
    }
    book.orders.push(new.clone());
    book.event(&old, "5", Decimal::ZERO, None);
    book.event(&new, "0", Decimal::ZERO, None);
    let resp = if sync {
        json!({"clientOrderId": request_id, "originalClientOrderId": old.cid, "data": [new.data("12", Decimal::ZERO, None)]})
    } else {
        json!({"clientOrderId": request_id, "status": "OK"})
    };
    Some(json_response(StatusCode::OK, &resp.to_string()))
}

async fn handle_ws(socket: WebSocket, path: String, owner: String, state: MockState) {
    state.options.ws_connections.fetch_add(1, Ordering::Relaxed);
    let channel = channel_from_path(&path);
    let (mut write, mut read) = socket.split();

    if channel == BcsWsChannel::Portfolio {
        let _ = write
            .send(Message::Text(fixture("ws/portfolio.json").into()))
            .await;
    }
    // Like the real broker, the limits and margin channels send a first snapshot on connect.
    if channel == BcsWsChannel::Limits {
        let first = r#"{"depoLimit":[{"ticker":"SBER","classCode":"TQBR","exchange":"MOEX","averagePrice":300.0,
            "quantity":{"type":"T365","value":10},"quantityBatch":{"type":"T365","value":1},"instrumentType":"STOCK"}],
            "moneyLimits":[{"exchange":"MOEX","currencyCode":"RUB","locked":1000.0,"quantity":{"type":"T0","value":50000.0}}]}"#;
        let _ = write.send(Message::Text(first.into())).await;
    }
    if channel == BcsWsChannel::MarginalIndicators {
        let first = r#"{"portfolioCurrentValue":{"currentValueRub":512000.0,"dailyPortfolioChangeRub":1200.0,
            "collateralAvailable":300000.0},"cashByCurrency":[{"currency":"RUB","sum":50000.0}]}"#;
        let _ = write.send(Message::Text(first.into())).await;
    }

    let mut pushes = tokio::sync::mpsc::unbounded_channel::<String>();
    if channel == BcsWsChannel::OrderEvents {
        state
            .options
            .book
            .lock()
            .unwrap()
            .sockets
            .push((owner, pushes.0.clone()));
    }

    let mut frames_sent = 0u32;
    loop {
        let msg = tokio::select! {
            Some(text) = pushes.1.recv() => {
                let _ = write.send(Message::Text(text.into())).await;
                continue;
            }
            msg = read.next() => match msg {
                Some(m) => m,
                None => break,
            },
        };
        match msg {
            Ok(Message::Text(text)) if channel == BcsWsChannel::MarketData => {
                let req: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
                // Unsubscribe (subscribeType 1) gets no answer.
                if req["subscribeType"].as_i64() == Some(1) {
                    continue;
                }
                if subscribe_should_fail(&state, &text).await {
                    let _ = write
                        .send(Message::Text(fixture("ws/order_book_error.json").into()))
                        .await;
                    continue;
                }
                let key = &req["instruments"][0];
                let ticker = key["ticker"].as_str().unwrap_or("SBER").to_owned();
                let class_code = key["classCode"].as_str().unwrap_or("TQBR").to_owned();
                // dataType: 0 order book, 1 last candle, 2 trades, 3 quotes.
                let (success, data) = match req["dataType"].as_i64() {
                    Some(1) => (
                        json!({"responseType": "CandleStickSuccess", "instruments": [key], "timeFrame": req["timeFrame"]}),
                        json!({"responseType": "CandleStick", "ticker": ticker, "classCode": class_code,
                               "timeFrame": req["timeFrame"], "dateTime": "2026-10-19T07:05:00.000Z",
                               "open": 300.0, "close": 300.5, "high": 301.0, "low": 299.5, "volume": 1200.0}),
                    ),
                    Some(2) => (
                        json!({"responseType": "LastTradesSuccess", "instruments": [key]}),
                        json!({"responseType": "LastTrades", "ticker": ticker, "classCode": class_code,
                               "side": "BUY", "volume": 30015.0, "price": 300.15, "quantity": 100,
                               "dateTime": "2026-10-19T07:05:01.000Z"}),
                    ),
                    Some(3) => (
                        json!({"responseType": "QuotesSuccess", "instruments": [key]}),
                        json!({"responseType": "Quotes", "ticker": ticker, "classCode": class_code,
                               "dateTime": "2026-10-19T07:05:02.000Z", "bid": 300.1, "ask": 300.2,
                               "lastPrice": 300.15, "volume": 1234567}),
                    ),
                    _ => (
                        serde_json::from_str(&fixture("ws/order_book_success.json")).unwrap(),
                        serde_json::from_str(&fixture("ws/order_book.json")).unwrap(),
                    ),
                };
                let _ = write.send(Message::Text(success.to_string().into())).await;
                let _ = write.send(Message::Text(data.to_string().into())).await;
                if req["dataType"].as_i64() == Some(0) {
                    frames_sent += 1;
                    state.options.ws_data_frames.fetch_add(1, Ordering::Relaxed);
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
}

async fn subscribe_should_fail(state: &MockState, text: &str) -> bool {
    let fail = state.options.fail_subscribe_ticker.lock().await;
    fail.as_ref().is_some_and(|ticker| text.contains(ticker))
}

fn channel_from_path(path: &str) -> BcsWsChannel {
    if path.contains("market-data/ws") {
        BcsWsChannel::MarketData
    } else if path.contains("portfolio/ws") {
        BcsWsChannel::Portfolio
    } else if path.contains("execution/ws") {
        BcsWsChannel::OrderExecution
    } else if path.contains("transaction/ws") {
        BcsWsChannel::OrderTransaction
    } else if path.contains("events/ws") {
        BcsWsChannel::OrderEvents
    } else if path.contains("marginal-indicators/ws") {
        BcsWsChannel::MarginalIndicators
    } else {
        BcsWsChannel::Limits
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

fn fixture(name: &str) -> String {
    let path = format!(
        "{}/../bcs-protocol/tests/fixtures/{}",
        env!("CARGO_MANIFEST_DIR"),
        name
    );
    std::fs::read_to_string(path).unwrap_or_else(|e| panic!("fixture {name}: {e}"))
}
