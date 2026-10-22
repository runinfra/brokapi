//! tonic implementation of the Finam services used by the connectors.

#![allow(dead_code)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use brokapi_finam_protocol::pb::google::r#type::{Decimal, Money};
use brokapi_finam_protocol::pb::grpc::tradeapi::v1;
use brokapi_finam_protocol::pb::grpc::tradeapi::v1::accounts as pa;
use brokapi_finam_protocol::pb::grpc::tradeapi::v1::accounts::accounts_service_server::{
    AccountsService, AccountsServiceServer,
};
use brokapi_finam_protocol::pb::grpc::tradeapi::v1::assets as ps;
use brokapi_finam_protocol::pb::grpc::tradeapi::v1::assets::assets_service_server::{
    AssetsService, AssetsServiceServer,
};
use brokapi_finam_protocol::pb::grpc::tradeapi::v1::auth as pu;
use brokapi_finam_protocol::pb::grpc::tradeapi::v1::auth::auth_service_server::{
    AuthService, AuthServiceServer,
};
use brokapi_finam_protocol::pb::grpc::tradeapi::v1::marketdata as pm;
use brokapi_finam_protocol::pb::grpc::tradeapi::v1::marketdata::market_data_service_server::{
    MarketDataService, MarketDataServiceServer,
};
use brokapi_finam_protocol::pb::grpc::tradeapi::v1::metrics as pt;
use brokapi_finam_protocol::pb::grpc::tradeapi::v1::metrics::usage_metrics_service_server::{
    UsageMetricsService, UsageMetricsServiceServer,
};
use brokapi_finam_protocol::pb::grpc::tradeapi::v1::orders as po;
use brokapi_finam_protocol::pb::grpc::tradeapi::v1::orders::orders_service_server::{
    OrdersService, OrdersServiceServer,
};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::{Code, Request, Response, Status};

pub const SECRET: &str = "mock-secret";
pub const SBER: &str = "SBER@MISX";

pub fn server_d(s: &str) -> Option<Decimal> {
    d(s)
}

pub fn d(s: &str) -> Option<Decimal> {
    Some(Decimal { value: s.into() })
}

fn now_ts() -> prost_types::Timestamp {
    let n = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    prost_types::Timestamp {
        seconds: n.as_secs() as i64,
        nanos: n.subsec_nanos() as i32,
    }
}

/// A history request seen by the mock: method, interval start and end in seconds (`None`: not sent).
pub type SeenInterval = (&'static str, Option<i64>, Option<i64>);

/// Knobs a test can turn while the server runs.
#[derive(Default)]
pub struct MockOptions {
    /// Holds the opening of every stream (orders, trades, account, quotes, order book, trades tape, bars): the
    /// call does not return a stream until `stream_gate.release()`. For a broker that is slow to open its streams.
    pub stream_gate: crate::gate::StreamGate,
    /// `Bars` answers a series for the range asked, one bar per step of the timeframe. Off: three fixed bars.
    pub synthetic_candles: std::sync::atomic::AtomicBool,
    /// Streams and unary calls for this symbol answer PERMISSION_DENIED.
    pub deny_symbol: Mutex<Option<String>>,
    /// End each book stream with UNAVAILABLE after N messages (0 = never).
    pub book_drop_after: AtomicU32,
    /// A book stream starts with an increment instead of a snapshot (until reopened).
    pub book_first_increment_streams: AtomicU32,
    /// Next `PlaceOrder` fails with this code before touching any state.
    pub place_error: Mutex<Option<Code>>,
    /// `PlaceOrder` applies the order but answers only after this delay (client timeout test).
    pub place_delay_ms: AtomicU64,
    /// The next N calls that need auth answer UNAUTHENTICATED.
    pub unauth_next: AtomicU32,
    /// Interval of periodic stream messages.
    pub tick_ms: AtomicU64,
    /// `Bars` (history) requests seen: (timeframe, interval start in seconds).
    pub bars_requests: Mutex<Vec<(i32, i64)>>,
    /// Timeframes of `SubscribeBars` streams opened.
    pub bars_streams: Mutex<Vec<i32>>,
    /// `trade_lot_size` that `GetAssetParams` answers (0: "no value", the default).
    pub trade_lot_size: std::sync::atomic::AtomicI64,
    /// `GetAssetParams` fails with this code.
    pub params_error: Mutex<Option<Code>>,
    /// `Trades` answers the newest trades first (the order is not documented, so both are possible).
    pub trades_newest_first: std::sync::atomic::AtomicBool,
    /// Every `Bars`, `Trades` and `Transactions` request seen: (method, interval start seconds, interval end
    /// seconds), `None` for a missing bound. For checking that the end of the period is always sent.
    pub intervals: Mutex<Vec<SeenInterval>>,
}

struct Account {
    orders: Vec<po::OrderState>,
    trades: Vec<v1::AccountTrade>,
    cash: Vec<Money>,
    positions: Vec<pa::Position>,
    orders_tx: broadcast::Sender<po::OrderState>,
    trades_tx: broadcast::Sender<v1::AccountTrade>,
    account_tx: broadcast::Sender<()>,
}

impl Account {
    fn new(cash: Vec<Money>, positions: Vec<pa::Position>) -> Self {
        Self {
            orders: Vec::new(),
            trades: Vec::new(),
            cash,
            positions,
            orders_tx: broadcast::channel(256).0,
            trades_tx: broadcast::channel(256).0,
            account_tx: broadcast::channel(16).0,
        }
    }

    fn response(&self, id: &str) -> pa::GetAccountResponse {
        pa::GetAccountResponse {
            account_id: id.into(),
            r#type: "UNIFIED".into(),
            status: "ACCOUNT_ACTIVE".into(),
            equity: d("1000000"),
            cash: self.cash.clone(),
            positions: self.positions.clone(),
            ..Default::default()
        }
    }
}

pub struct MockState {
    pub options: MockOptions,
    ttl_secs: i64,
    issued: AtomicU64,
    tokens: Mutex<Vec<String>>,
    accounts: Mutex<HashMap<String, Account>>,
    next_order: AtomicU64,
    calls: Mutex<Vec<String>>,
    /// Book streams opened so far.
    pub book_opens: AtomicU32,
    pub auth_calls: AtomicU32,
    pub order_stream_opens: AtomicU32,
    /// Counts the cuts of the user streams (orders and trades): every such stream task ends when it changes.
    cut: tokio::sync::watch::Sender<u64>,
    /// Stream request metadata seen (token used per call).
    tokens_seen: Mutex<Vec<String>>,
}

impl MockState {
    fn new(ttl_secs: i64) -> Arc<Self> {
        let mut accounts = HashMap::new();
        accounts.insert(
            "ACC1".to_string(),
            Account::new(
                vec![Money {
                    currency_code: "RUB".into(),
                    units: 100_000,
                    nanos: 500_000_000,
                }],
                vec![pa::Position {
                    symbol: SBER.into(),
                    quantity: d("10"),
                    average_price: d("250.5"),
                    current_price: d("260"),
                    ..Default::default()
                }],
            ),
        );
        accounts.insert(
            "ACC2".to_string(),
            Account::new(
                vec![Money {
                    currency_code: "USD".into(),
                    units: 5_000,
                    nanos: 0,
                }],
                vec![pa::Position {
                    symbol: "GAZP@MISX".into(),
                    quantity: d("-5"),
                    current_price: d("170"),
                    ..Default::default()
                }],
            ),
        );
        Arc::new(Self {
            options: MockOptions {
                tick_ms: AtomicU64::new(40),
                ..Default::default()
            },
            ttl_secs,
            issued: AtomicU64::new(0),
            tokens: Mutex::default(),
            accounts: Mutex::new(accounts),
            next_order: AtomicU64::new(1000),
            calls: Mutex::default(),
            book_opens: AtomicU32::new(0),
            auth_calls: AtomicU32::new(0),
            order_stream_opens: AtomicU32::new(0),
            cut: tokio::sync::watch::channel(0).0,
            tokens_seen: Mutex::default(),
        })
    }

    /// Ends every open orders and trades stream from the broker side: the link breaks. The connector sees
    /// the end of the stream and opens it again (`order_stream_opens` grows).
    pub fn cut_user_streams(&self) {
        self.cut.send_modify(|n| *n += 1);
    }

    /// Method names called so far, e.g. `PlaceOrder:ACC1`.
    pub fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }

    pub fn count_calls(&self, prefix: &str) -> usize {
        self.calls()
            .iter()
            .filter(|c| c.starts_with(prefix))
            .count()
    }

    pub fn tokens_seen(&self) -> Vec<String> {
        self.tokens_seen.lock().unwrap().clone()
    }

    pub fn issued_tokens(&self) -> u64 {
        self.issued.load(Ordering::SeqCst)
    }

    /// Orders of one account as the server stores them.
    pub fn orders_of(&self, account: &str) -> Vec<po::OrderState> {
        self.accounts
            .lock()
            .unwrap()
            .get(account)
            .map(|a| a.orders.clone())
            .unwrap_or_default()
    }

    /// Pushes an order update to `SubscribeOrders` streams of the account (and stores it).
    pub fn push_order(&self, account: &str, os: po::OrderState) {
        let mut accs = self.accounts.lock().unwrap();
        let acc = accs.get_mut(account).expect("account");
        match acc.orders.iter_mut().find(|o| o.order_id == os.order_id) {
            Some(slot) => *slot = os.clone(),
            None => acc.orders.push(os.clone()),
        }
        let _ = acc.orders_tx.send(os);
    }

    pub fn push_trade(&self, account: &str, t: v1::AccountTrade) {
        let mut accs = self.accounts.lock().unwrap();
        let acc = accs.get_mut(account).expect("account");
        acc.trades.push(t.clone());
        let _ = acc.trades_tx.send(t);
    }

    /// Changes cash and notifies `SubscribeAccount` streams.
    pub fn set_cash(&self, account: &str, cash: Vec<Money>) {
        let mut accs = self.accounts.lock().unwrap();
        let acc = accs.get_mut(account).expect("account");
        acc.cash = cash;
        let _ = acc.account_tx.send(());
    }

    pub fn set_cash_without_notice(&self, account: &str, cash: Vec<Money>) {
        self.accounts
            .lock()
            .unwrap()
            .get_mut(account)
            .expect("account")
            .cash = cash;
    }

    fn record(&self, what: impl Into<String>) {
        self.calls.lock().unwrap().push(what.into());
    }

    fn note_interval(
        &self,
        method: &'static str,
        interval: Option<&brokapi_finam_protocol::pb::google::r#type::Interval>,
    ) {
        let secs = |t: Option<&prost_types::Timestamp>| t.map(|t| t.seconds);
        self.options.intervals.lock().unwrap().push((
            method,
            secs(interval.and_then(|i| i.start_time.as_ref())),
            secs(interval.and_then(|i| i.end_time.as_ref())),
        ));
    }

    /// Gateway behaviour: every call except Auth needs a token we issued.
    fn check_auth<T>(&self, req: &Request<T>) -> Result<(), Status> {
        let n = self.options.unauth_next.load(Ordering::SeqCst);
        if n > 0 {
            self.options.unauth_next.store(n - 1, Ordering::SeqCst);
            return Err(Status::unauthenticated("token expired"));
        }
        let Some(v) = req.metadata().get("authorization") else {
            return Err(Status::unauthenticated("no authorization metadata"));
        };
        let v = v
            .to_str()
            .unwrap_or_default()
            .trim_start_matches("Bearer ")
            .to_string();
        self.tokens_seen.lock().unwrap().push(v.clone());
        if self.tokens.lock().unwrap().contains(&v) {
            Ok(())
        } else {
            Err(Status::unauthenticated("unknown token"))
        }
    }

    fn denied(&self, symbol: &str) -> Result<(), Status> {
        match self.options.deny_symbol.lock().unwrap().as_deref() {
            Some(s) if s == symbol => Err(Status::permission_denied("no market data permission")),
            _ => Ok(()),
        }
    }

    fn tick(&self) -> Duration {
        Duration::from_millis(self.options.tick_ms.load(Ordering::Relaxed))
    }

    fn account<R>(&self, id: &str, f: impl FnOnce(&mut Account) -> R) -> Result<R, Status> {
        let mut accs = self.accounts.lock().unwrap();
        match accs.get_mut(id) {
            Some(a) => Ok(f(a)),
            None => Err(Status::not_found(format!("account {id} not found"))),
        }
    }
}

pub struct MockServer {
    pub url: String,
    pub state: Arc<MockState>,
    task: JoinHandle<()>,
}

impl MockServer {
    pub async fn start() -> Self {
        Self::start_with_ttl(900).await
    }

    /// `ttl_secs` is the lifetime reported by `TokenDetails`.
    pub async fn start_with_ttl(ttl_secs: i64) -> Self {
        let state = MockState::new(ttl_secs);
        let svc = Mock(Arc::clone(&state));
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr: SocketAddr = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(AuthServiceServer::new(svc.clone()))
                .add_service(AccountsServiceServer::new(svc.clone()))
                .add_service(OrdersServiceServer::new(svc.clone()))
                .add_service(MarketDataServiceServer::new(svc.clone()))
                .add_service(AssetsServiceServer::new(svc.clone()))
                .add_service(UsageMetricsServiceServer::new(svc))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .expect("mock server");
        });
        Self {
            url: format!("http://{addr}"),
            state,
            task,
        }
    }
}

impl Drop for MockServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[derive(Clone)]
struct Mock(Arc<MockState>);

type Stream<T> = ReceiverStream<Result<T, Status>>;

fn unimpl<T>(what: &str) -> Result<T, Status> {
    Err(Status::unimplemented(format!("mock: {what}")))
}

// ---------------------------------------------------------------- auth

#[tonic::async_trait]
impl AuthService for Mock {
    async fn auth(
        &self,
        req: Request<pu::AuthRequest>,
    ) -> Result<Response<pu::AuthResponse>, Status> {
        self.0.auth_calls.fetch_add(1, Ordering::SeqCst);
        if req.into_inner().secret != SECRET {
            return Err(Status::unauthenticated("bad secret"));
        }
        let n = self.0.issued.fetch_add(1, Ordering::SeqCst) + 1;
        let token = format!("jwt-{n}");
        self.0.tokens.lock().unwrap().push(token.clone());
        Ok(Response::new(pu::AuthResponse { token }))
    }

    async fn token_details(
        &self,
        req: Request<pu::TokenDetailsRequest>,
    ) -> Result<Response<pu::TokenDetailsResponse>, Status> {
        if !self
            .0
            .tokens
            .lock()
            .unwrap()
            .contains(&req.into_inner().token)
        {
            return Err(Status::unauthenticated("unknown token"));
        }
        let now = now_ts();
        Ok(Response::new(pu::TokenDetailsResponse {
            created_at: Some(now),
            expires_at: Some(prost_types::Timestamp {
                seconds: now.seconds + self.0.ttl_secs,
                nanos: 0,
            }),
            account_ids: vec!["ACC1".into(), "ACC2".into()],
            ..Default::default()
        }))
    }

    type SubscribeJwtRenewalStream = Stream<pu::SubscribeJwtRenewalResponse>;

    async fn subscribe_jwt_renewal(
        &self,
        _req: Request<pu::SubscribeJwtRenewalRequest>,
    ) -> Result<Response<Self::SubscribeJwtRenewalStream>, Status> {
        unimpl("SubscribeJwtRenewal")
    }
}

// ------------------------------------------------------------ accounts

#[tonic::async_trait]
impl AccountsService for Mock {
    async fn get_account(
        &self,
        req: Request<pa::GetAccountRequest>,
    ) -> Result<Response<pa::GetAccountResponse>, Status> {
        self.0.check_auth(&req)?;
        let id = req.into_inner().account_id;
        self.0.record(format!("GetAccount:{id}"));
        let resp = self.0.account(&id, |a| a.response(&id))?;
        Ok(Response::new(resp))
    }

    async fn trades(
        &self,
        req: Request<pa::TradesRequest>,
    ) -> Result<Response<pa::TradesResponse>, Status> {
        self.0.check_auth(&req)?;
        let r = req.into_inner();
        self.0.note_interval("Trades", r.interval.as_ref());
        let id = r.account_id.clone();
        self.0.record(format!("Trades:{id}"));
        let mut trades = self.0.account(&id, |a| a.trades.clone())?;
        // Like the broker: only the trades of the period asked, oldest first, at most `limit` of them.
        let secs = |t: &prost_types::Timestamp| {
            i128::from(t.seconds) * 1_000_000_000 + i128::from(t.nanos)
        };
        if let Some(iv) = &r.interval {
            let (from, to) = (
                iv.start_time.as_ref().map(secs),
                iv.end_time.as_ref().map(secs),
            );
            // A trade the test pushed without a time is always in the period.
            trades.retain(|t| {
                t.timestamp
                    .as_ref()
                    .map(secs)
                    .is_none_or(|at| from.is_none_or(|f| at >= f) && to.is_none_or(|e| at < e))
            });
        }
        trades.sort_by(|a, b| {
            let key = |t: &v1::AccountTrade| t.timestamp.as_ref().map(|t| (t.seconds, t.nanos));
            key(a).cmp(&key(b))
        });
        if self
            .0
            .options
            .trades_newest_first
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            trades.reverse();
        }
        if r.limit > 0 {
            trades.truncate(r.limit as usize);
        }
        Ok(Response::new(pa::TradesResponse { trades }))
    }

    async fn transactions(
        &self,
        req: Request<pa::TransactionsRequest>,
    ) -> Result<Response<pa::TransactionsResponse>, Status> {
        self.0
            .note_interval("Transactions", req.get_ref().interval.as_ref());
        unimpl("Transactions")
    }

    type SubscribeAccountStream = Stream<pa::GetAccountResponse>;

    async fn subscribe_account(
        &self,
        req: Request<pa::GetAccountRequest>,
    ) -> Result<Response<Self::SubscribeAccountStream>, Status> {
        self.0.options.stream_gate.pass().await;
        self.0.check_auth(&req)?;
        let id = req.into_inner().account_id;
        self.0.record(format!("SubscribeAccount:{id}"));
        let (first, mut rx) = self
            .0
            .account(&id, |a| (a.response(&id), a.account_tx.subscribe()))?;
        let (tx, out) = mpsc::channel(16);
        let state = Arc::clone(&self.0);
        tokio::spawn(async move {
            if tx.send(Ok(first)).await.is_err() {
                return;
            }
            while rx.recv().await.is_ok() {
                let Ok(resp) = state.account(&id, |a| a.response(&id)) else {
                    return;
                };
                if tx.send(Ok(resp)).await.is_err() {
                    return;
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(out)))
    }
}

// -------------------------------------------------------------- orders

#[tonic::async_trait]
impl OrdersService for Mock {
    async fn place_order(
        &self,
        req: Request<po::Order>,
    ) -> Result<Response<po::OrderState>, Status> {
        self.0.check_auth(&req)?;
        let order = req.into_inner();
        self.0.record(format!("PlaceOrder:{}", order.account_id));
        if let Some(code) = self.0.options.place_error.lock().unwrap().take() {
            return Err(Status::new(code, "mock place error"));
        }
        // A limit order at the marker price is refused (conformance: a refusal must not block the instrument).
        let price = order
            .limit_price
            .as_ref()
            .and_then(|p| p.value.parse::<brokapi_core::Decimal>().ok());
        if price == Some(brokapi_conformance::REJECT_PRICE.decimal()) {
            return Err(Status::invalid_argument("price refused by mock"));
        }
        if order.symbol.is_empty() || order.quantity.is_none() {
            return Err(Status::invalid_argument("symbol and quantity required"));
        }
        let n = self.0.next_order.fetch_add(1, Ordering::SeqCst);
        let qty = order.quantity.clone();
        let is_stop = order.r#type == po::OrderType::Stop as i32
            || order.r#type == po::OrderType::StopLimit as i32;
        if is_stop && order.stop_price.is_none() {
            return Err(Status::invalid_argument(
                "stop_price required for a stop order",
            ));
        }
        let os = po::OrderState {
            order_id: format!("ORD{n}"),
            exec_id: String::new(),
            // A stop order watches the price until it triggers.
            status: if is_stop {
                po::OrderStatus::Watching
            } else {
                po::OrderStatus::New
            } as i32,
            order: Some(order.clone()),
            transact_at: Some(now_ts()),
            accept_at: Some(now_ts()),
            initial_quantity: qty.clone(),
            executed_quantity: d("0"),
            remaining_quantity: qty,
            ..Default::default()
        };
        let acc_id = order.account_id.clone();
        self.0.account(&acc_id, |a| {
            a.orders.push(os.clone());
            let _ = a.orders_tx.send(os.clone());
        })?;
        let delay = self.0.options.place_delay_ms.load(Ordering::SeqCst);
        if delay > 0 {
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }
        Ok(Response::new(os))
    }

    async fn cancel_order(
        &self,
        req: Request<po::CancelOrderRequest>,
    ) -> Result<Response<po::OrderState>, Status> {
        self.0.check_auth(&req)?;
        let r = req.into_inner();
        self.0.record(format!("CancelOrder:{}", r.account_id));
        let out = self.0.account(&r.account_id, |a| {
            let o = a
                .orders
                .iter_mut()
                .find(|o| o.order_id == r.order_id)
                .ok_or_else(|| Status::not_found("order not found"))?;
            if o.status == po::OrderStatus::Filled as i32 {
                return Err(Status::invalid_argument("order already executed"));
            }
            o.status = po::OrderStatus::Canceled as i32;
            o.withdraw_at = Some(now_ts());
            let o = o.clone();
            let _ = a.orders_tx.send(o.clone());
            Ok(o)
        })??;
        Ok(Response::new(out))
    }

    async fn get_orders(
        &self,
        req: Request<po::OrdersRequest>,
    ) -> Result<Response<po::OrdersResponse>, Status> {
        self.0.check_auth(&req)?;
        let id = req.into_inner().account_id;
        self.0.record(format!("GetOrders:{id}"));
        let orders = self.0.account(&id, |a| a.orders.clone())?;
        Ok(Response::new(po::OrdersResponse { orders }))
    }

    async fn get_order(
        &self,
        req: Request<po::GetOrderRequest>,
    ) -> Result<Response<po::OrderState>, Status> {
        self.0.check_auth(&req)?;
        let r = req.into_inner();
        self.0.record(format!("GetOrder:{}", r.account_id));
        let found = self.0.account(&r.account_id, |a| {
            a.orders.iter().find(|o| o.order_id == r.order_id).cloned()
        })?;
        found
            .map(Response::new)
            .ok_or_else(|| Status::not_found("order not found"))
    }

    type SubscribeOrderTradeStream = Stream<po::OrderTradeResponse>;

    async fn subscribe_order_trade(
        &self,
        _req: Request<tonic::Streaming<po::OrderTradeRequest>>,
    ) -> Result<Response<Self::SubscribeOrderTradeStream>, Status> {
        unimpl("SubscribeOrderTrade")
    }

    type SubscribeOrdersStream = Stream<po::SubscribeOrdersResponse>;

    async fn subscribe_orders(
        &self,
        req: Request<po::SubscribeOrdersRequest>,
    ) -> Result<Response<Self::SubscribeOrdersStream>, Status> {
        self.0.options.stream_gate.pass().await;
        self.0.check_auth(&req)?;
        let id = req.into_inner().account_id;
        self.0.record(format!("SubscribeOrders:{id}"));
        self.0.order_stream_opens.fetch_add(1, Ordering::SeqCst);
        let mut rx = self.0.account(&id, |a| a.orders_tx.subscribe())?;
        let mut cut = self.0.cut.subscribe();
        let (tx, out) = mpsc::channel(16);
        tokio::spawn(async move {
            loop {
                let os = tokio::select! {
                    os = rx.recv() => match os {
                        Ok(os) => os,
                        Err(_) => return,
                    },
                    _ = cut.changed() => return,
                };
                if tx
                    .send(Ok(po::SubscribeOrdersResponse { orders: vec![os] }))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(out)))
    }

    type SubscribeTradesStream = Stream<po::SubscribeTradesResponse>;

    async fn subscribe_trades(
        &self,
        req: Request<po::SubscribeTradesRequest>,
    ) -> Result<Response<Self::SubscribeTradesStream>, Status> {
        self.0.options.stream_gate.pass().await;
        self.0.check_auth(&req)?;
        let id = req.into_inner().account_id;
        self.0.record(format!("SubscribeTrades:{id}"));
        let mut rx = self.0.account(&id, |a| a.trades_tx.subscribe())?;
        let mut cut = self.0.cut.subscribe();
        let (tx, out) = mpsc::channel(16);
        tokio::spawn(async move {
            loop {
                let t = tokio::select! {
                    t = rx.recv() => match t {
                        Ok(t) => t,
                        Err(_) => return,
                    },
                    _ = cut.changed() => return,
                };
                if tx
                    .send(Ok(po::SubscribeTradesResponse { trades: vec![t] }))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(out)))
    }

    async fn place_sltp_order(
        &self,
        req: Request<po::SltpOrder>,
    ) -> Result<Response<po::OrderState>, Status> {
        self.0.check_auth(&req)?;
        let sltp = req.into_inner();
        self.0.record(format!("PlaceSLTPOrder:{}", sltp.account_id));
        if sltp.symbol.is_empty() || (sltp.quantity_tp.is_none() && sltp.quantity_sl.is_none()) {
            return Err(Status::invalid_argument("symbol and a quantity required"));
        }
        let n = self.0.next_order.fetch_add(1, Ordering::SeqCst);
        let qty = sltp
            .quantity_tp
            .clone()
            .or_else(|| sltp.quantity_sl.clone());
        let os = po::OrderState {
            order_id: format!("ORD{n}"),
            status: po::OrderStatus::Watching as i32,
            // Like the real gateway is assumed to do: the SL/TP order comes back in `sltp_order`.
            sltp_order: Some(sltp.clone()),
            transact_at: Some(now_ts()),
            accept_at: Some(now_ts()),
            initial_quantity: qty.clone(),
            executed_quantity: d("0"),
            remaining_quantity: qty,
            ..Default::default()
        };
        self.0.account(&sltp.account_id, |a| {
            a.orders.push(os.clone());
            let _ = a.orders_tx.send(os.clone());
        })?;
        Ok(Response::new(os))
    }
}

// --------------------------------------------------------- market data

fn book_rows(snapshot: bool, bump: u32) -> Vec<pm::stream_order_book::Row> {
    use pm::stream_order_book::{
        Row,
        row::{Action, Side},
    };
    let row = |price: &str, side: fn(Decimal) -> Side, size: String, action: Action| Row {
        price: d(price),
        side: Some(side(Decimal { value: size })),
        action: action as i32,
        ..Default::default()
    };
    if snapshot {
        vec![
            row("100.0", Side::BuySize, "10".into(), Action::Add),
            row("99.9", Side::BuySize, "20".into(), Action::Add),
            row("99.8", Side::BuySize, "30".into(), Action::Add),
            row("100.1", Side::SellSize, "10".into(), Action::Add),
            row("100.2", Side::SellSize, "20".into(), Action::Add),
            row("100.3", Side::SellSize, "30".into(), Action::Add),
        ]
    } else {
        vec![row(
            "100.0",
            Side::BuySize,
            format!("{}", 10 + bump),
            Action::Update,
        )]
    }
}

fn rest_book() -> pm::OrderBook {
    use pm::order_book::{Row, row::Side};
    let row = |price: &str, side: fn(Decimal) -> Side, size: &str| Row {
        price: d(price),
        side: Some(side(Decimal { value: size.into() })),
        ..Default::default()
    };
    // Deliberately unsorted: the connector must sort best-first.
    pm::OrderBook {
        rows: vec![
            row("99.9", Side::BuySize, "20"),
            row("100.2", Side::SellSize, "20"),
            row("100.0", Side::BuySize, "10"),
            row("100.1", Side::SellSize, "10"),
            row("99.8", Side::BuySize, "30"),
            row("100.3", Side::SellSize, "30"),
        ],
    }
}

fn quote(symbol: &str, n: u32) -> pm::Quote {
    pm::Quote {
        symbol: symbol.into(),
        timestamp: Some(now_ts()),
        bid: d("100.0"),
        bid_size: d("10"),
        ask: d("100.1"),
        ask_size: d(&format!("{}", 10 + n)),
        last: d("100.05"),
        ..Default::default()
    }
}

#[tonic::async_trait]
impl MarketDataService for Mock {
    async fn bars(
        &self,
        req: Request<pm::BarsRequest>,
    ) -> Result<Response<pm::BarsResponse>, Status> {
        self.0.check_auth(&req)?;
        let r = req.into_inner();
        self.0.note_interval("Bars", r.interval.as_ref());
        self.0.record("Bars");
        let start = r
            .interval
            .as_ref()
            .and_then(|i| i.start_time.as_ref())
            .map(|t| t.seconds)
            .unwrap_or(0);
        let end = r
            .interval
            .as_ref()
            .and_then(|i| i.end_time.as_ref())
            .map(|t| t.seconds)
            .unwrap_or(start);
        self.0
            .options
            .bars_requests
            .lock()
            .unwrap()
            .push((r.timeframe, start));
        let bars = if self
            .0
            .options
            .synthetic_candles
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            let step = match r.timeframe {
                1 => 60,
                5 => 300,
                9 => 900,
                11 => 1_800,
                12 => 3_600,
                13 => 7_200,
                15 => 14_400,
                17 => 28_800,
                19 => 86_400,
                20 => 7 * 86_400,
                21 => 30 * 86_400,
                _ => return Err(Status::invalid_argument("unknown timeframe")),
            };
            crate::synthetic_bars(start, end, step)
                .into_iter()
                .map(|(t, o, h, l, c, v)| pm::Bar {
                    timestamp: Some(prost_types::Timestamp {
                        seconds: t,
                        nanos: 0,
                    }),
                    open: d(&o.to_string()),
                    high: d(&h.to_string()),
                    low: d(&l.to_string()),
                    close: d(&c.to_string()),
                    volume: d(&v.to_string()),
                    is_data_snapshot: true,
                })
                .collect()
        } else {
            (0..3)
                .map(|i| pm::Bar {
                    timestamp: Some(prost_types::Timestamp {
                        seconds: start + i * 60,
                        nanos: 0,
                    }),
                    open: d("100"),
                    high: d("101"),
                    low: d("99"),
                    close: d("100.5"),
                    volume: d("1000"),
                    is_data_snapshot: true,
                })
                .collect()
        };
        Ok(Response::new(pm::BarsResponse {
            symbol: r.symbol,
            bars,
        }))
    }

    async fn last_quote(
        &self,
        req: Request<pm::QuoteRequest>,
    ) -> Result<Response<pm::QuoteResponse>, Status> {
        self.0.check_auth(&req)?;
        let s = req.into_inner().symbol;
        self.0.record(format!("LastQuote:{s}"));
        Ok(Response::new(pm::QuoteResponse {
            quote: Some(quote(&s, 0)),
            symbol: s,
        }))
    }

    async fn order_book(
        &self,
        req: Request<pm::OrderBookRequest>,
    ) -> Result<Response<pm::OrderBookResponse>, Status> {
        self.0.check_auth(&req)?;
        let s = req.into_inner().symbol;
        self.0.record(format!("OrderBook:{s}"));
        self.0.denied(&s)?;
        // A symbol no mock knows is "not found", as the broker answers.
        if s.starts_with("NOPE@") {
            return Err(Status::not_found("unknown instrument"));
        }
        Ok(Response::new(pm::OrderBookResponse {
            symbol: s,
            orderbook: Some(rest_book()),
        }))
    }

    async fn latest_trades(
        &self,
        req: Request<pm::LatestTradesRequest>,
    ) -> Result<Response<pm::LatestTradesResponse>, Status> {
        self.0.check_auth(&req)?;
        let s = req.into_inner().symbol;
        let trade = |i: u32, side: v1::Side| pm::Trade {
            trade_id: format!("T{i}"),
            timestamp: Some(now_ts()),
            price: d("100.05"),
            size: d("5"),
            side: side as i32,
            ..Default::default()
        };
        Ok(Response::new(pm::LatestTradesResponse {
            symbol: s,
            trades: vec![
                trade(1, v1::Side::Buy),
                trade(2, v1::Side::Sell),
                trade(3, v1::Side::Unspecified),
                trade(4, v1::Side::Buy),
            ],
        }))
    }

    type SubscribeQuoteStream = Stream<pm::SubscribeQuoteResponse>;

    async fn subscribe_quote(
        &self,
        req: Request<pm::SubscribeQuoteRequest>,
    ) -> Result<Response<Self::SubscribeQuoteStream>, Status> {
        self.0.options.stream_gate.pass().await;
        self.0.check_auth(&req)?;
        let symbol = req
            .into_inner()
            .symbols
            .first()
            .cloned()
            .unwrap_or_default();
        self.0.denied(&symbol)?;
        let (tx, out) = mpsc::channel(16);
        let tick = self.0.tick();
        tokio::spawn(async move {
            for n in 0.. {
                let msg = pm::SubscribeQuoteResponse {
                    quote: vec![quote(&symbol, n)],
                    error: None,
                };
                if tx.send(Ok(msg)).await.is_err() {
                    return;
                }
                tokio::time::sleep(tick).await;
            }
        });
        Ok(Response::new(ReceiverStream::new(out)))
    }

    type SubscribeOrderBookStream = Stream<pm::SubscribeOrderBookResponse>;

    async fn subscribe_order_book(
        &self,
        req: Request<pm::SubscribeOrderBookRequest>,
    ) -> Result<Response<Self::SubscribeOrderBookStream>, Status> {
        self.0.options.stream_gate.pass().await;
        self.0.check_auth(&req)?;
        let symbol = req.into_inner().symbol;
        self.0.record(format!("SubscribeOrderBook:{symbol}"));
        self.0.denied(&symbol)?;
        self.0.book_opens.fetch_add(1, Ordering::SeqCst);

        let first_incr = {
            let n = self
                .0
                .options
                .book_first_increment_streams
                .load(Ordering::SeqCst);
            if n > 0 {
                self.0
                    .options
                    .book_first_increment_streams
                    .store(n - 1, Ordering::SeqCst);
            }
            n > 0
        };
        let drop_after = self.0.options.book_drop_after.load(Ordering::SeqCst);
        let (tx, out) = mpsc::channel(16);
        let tick = self.0.tick();
        tokio::spawn(async move {
            let mut sent = 0u32;
            loop {
                let snapshot = sent == 0 && !first_incr;
                let msg = pm::SubscribeOrderBookResponse {
                    order_book: vec![pm::StreamOrderBook {
                        symbol: symbol.clone(),
                        rows: book_rows(snapshot, sent),
                        is_data_snapshot: snapshot,
                    }],
                };
                if tx.send(Ok(msg)).await.is_err() {
                    return;
                }
                sent += 1;
                if drop_after > 0 && sent >= drop_after {
                    let _ = tx
                        .send(Err(Status::unavailable("mock: stream dropped")))
                        .await;
                    return;
                }
                tokio::time::sleep(tick).await;
            }
        });
        Ok(Response::new(ReceiverStream::new(out)))
    }

    type SubscribeLatestTradesStream = Stream<pm::SubscribeLatestTradesResponse>;

    async fn subscribe_latest_trades(
        &self,
        req: Request<pm::SubscribeLatestTradesRequest>,
    ) -> Result<Response<Self::SubscribeLatestTradesStream>, Status> {
        self.0.options.stream_gate.pass().await;
        self.0.check_auth(&req)?;
        let symbol = req.into_inner().symbol;
        self.0.denied(&symbol)?;
        let (tx, out) = mpsc::channel(16);
        let tick = self.0.tick();
        tokio::spawn(async move {
            // Every stream starts by replaying T1 (a "snapshot" of recent trades), then new ones.
            for i in 1.. {
                let ids: Vec<u32> = if i == 1 { vec![1] } else { vec![1, i] };
                let trades = ids
                    .into_iter()
                    .map(|n| pm::Trade {
                        trade_id: format!("T{n}"),
                        timestamp: Some(now_ts()),
                        price: d("100.05"),
                        size: d("5"),
                        side: v1::Side::Buy as i32,
                        ..Default::default()
                    })
                    .collect();
                let msg = pm::SubscribeLatestTradesResponse {
                    symbol: symbol.clone(),
                    trades,
                };
                if tx.send(Ok(msg)).await.is_err() {
                    return;
                }
                tokio::time::sleep(tick).await;
            }
        });
        Ok(Response::new(ReceiverStream::new(out)))
    }

    type SubscribeBarsStream = Stream<pm::SubscribeBarsResponse>;

    async fn subscribe_bars(
        &self,
        req: Request<pm::SubscribeBarsRequest>,
    ) -> Result<Response<Self::SubscribeBarsStream>, Status> {
        self.0.options.stream_gate.pass().await;
        self.0.check_auth(&req)?;
        let r = req.into_inner();
        let symbol = r.symbol;
        self.0
            .options
            .bars_streams
            .lock()
            .unwrap()
            .push(r.timeframe);
        self.0.denied(&symbol)?;
        let (tx, out) = mpsc::channel(16);
        let tick = self.0.tick();
        tokio::spawn(async move {
            for n in 0..i64::MAX {
                let msg = pm::SubscribeBarsResponse {
                    symbol: symbol.clone(),
                    bars: vec![pm::Bar {
                        timestamp: Some(prost_types::Timestamp {
                            seconds: 1_700_000_000,
                            nanos: 0,
                        }),
                        open: d("100"),
                        high: d("101"),
                        low: d("99"),
                        close: d(&format!("{}", 100 + n)),
                        volume: d("1000"),
                        is_data_snapshot: n == 0,
                    }],
                };
                if tx.send(Ok(msg)).await.is_err() {
                    return;
                }
                tokio::time::sleep(tick).await;
            }
        });
        Ok(Response::new(ReceiverStream::new(out)))
    }
}

// -------------------------------------------------------------- assets

fn asset(symbol: &str, ty: &str, isin: &str) -> ps::Asset {
    let (ticker, mic) = symbol.split_once('@').unwrap();
    ps::Asset {
        symbol: symbol.into(),
        id: "1".into(),
        ticker: ticker.into(),
        mic: mic.into(),
        isin: isin.into(),
        r#type: ty.into(),
        name: format!("{ticker} name"),
        is_archived: false,
    }
}

#[tonic::async_trait]
impl AssetsService for Mock {
    async fn exchanges(
        &self,
        _r: Request<ps::ExchangesRequest>,
    ) -> Result<Response<ps::ExchangesResponse>, Status> {
        unimpl("Exchanges")
    }

    async fn assets(
        &self,
        _r: Request<ps::AssetsRequest>,
    ) -> Result<Response<ps::AssetsResponse>, Status> {
        unimpl("Assets")
    }

    async fn all_assets(
        &self,
        req: Request<ps::AllAssetsRequest>,
    ) -> Result<Response<ps::AllAssetsResponse>, Status> {
        self.0.check_auth(&req)?;
        // Two pages, to exercise the cursor.
        let cursor = req.into_inner().cursor;
        self.0.record(format!("AllAssets:{cursor}"));
        Ok(Response::new(if cursor == 0 {
            ps::AllAssetsResponse {
                assets: vec![asset(SBER, "EQUITIES", "RU0009029540")],
                next_cursor: 7,
            }
        } else {
            ps::AllAssetsResponse {
                assets: vec![
                    asset("GAZP@MISX", "EQUITIES", "RU0007661625"),
                    asset("SU26238RMFS4@MISX", "BONDS", ""),
                ],
                next_cursor: 0,
            }
        }))
    }

    async fn get_asset(
        &self,
        req: Request<ps::GetAssetRequest>,
    ) -> Result<Response<ps::GetAssetResponse>, Status> {
        self.0.check_auth(&req)?;
        let symbol = req.into_inner().symbol;
        self.0.record(format!("GetAsset:{symbol}"));
        if symbol != SBER && symbol != "GAZP@MISX" {
            return Err(Status::not_found("unknown instrument"));
        }
        let (ticker, mic) = symbol.split_once('@').unwrap();
        Ok(Response::new(ps::GetAssetResponse {
            board: "TQBR".into(),
            id: "1".into(),
            ticker: ticker.into(),
            mic: mic.into(),
            isin: "RU0009029540".into(),
            r#type: "EQUITIES".into(),
            name: "Sber".into(),
            decimals: 2,
            min_step: 1,
            lot_size: d("10"),
            quote_currency: "RUB".into(),
            ..Default::default()
        }))
    }

    async fn get_asset_params(
        &self,
        req: Request<ps::GetAssetParamsRequest>,
    ) -> Result<Response<ps::GetAssetParamsResponse>, Status> {
        self.0.check_auth(&req)?;
        let r = req.into_inner();
        self.0
            .record(format!("GetAssetParams:{}:{}", r.symbol, r.account_id));
        if let Some(code) = *self.0.options.params_error.lock().unwrap() {
            return Err(Status::new(code, "mock failure"));
        }
        if r.symbol != SBER && r.symbol != "GAZP@MISX" {
            return Err(Status::not_found("unknown instrument"));
        }
        Ok(Response::new(ps::GetAssetParamsResponse {
            symbol: r.symbol,
            account_id: r.account_id,
            trade_lot_size: self
                .0
                .options
                .trade_lot_size
                .load(std::sync::atomic::Ordering::Relaxed),
            ..Default::default()
        }))
    }

    async fn options_chain(
        &self,
        _r: Request<ps::OptionsChainRequest>,
    ) -> Result<Response<ps::OptionsChainResponse>, Status> {
        unimpl("OptionsChain")
    }

    async fn schedule(
        &self,
        req: Request<ps::ScheduleRequest>,
    ) -> Result<Response<ps::ScheduleResponse>, Status> {
        self.0.check_auth(&req)?;
        let symbol = req.into_inner().symbol;
        let now = now_ts().seconds;
        let iv = |from: i64, to: i64| {
            Some(brokapi_finam_protocol::pb::google::r#type::Interval {
                start_time: Some(prost_types::Timestamp {
                    seconds: from,
                    nanos: 0,
                }),
                end_time: Some(prost_types::Timestamp {
                    seconds: to,
                    nanos: 0,
                }),
            })
        };
        Ok(Response::new(ps::ScheduleResponse {
            symbol,
            sessions: vec![
                ps::schedule_response::Sessions {
                    r#type: "CLOSED".into(),
                    interval: iv(now - 86_400, now - 3_600),
                },
                ps::schedule_response::Sessions {
                    r#type: "CORE_TRADING".into(),
                    interval: iv(now - 3_600, now + 3_600),
                },
                ps::schedule_response::Sessions {
                    r#type: "LATE_TRADING".into(),
                    interval: iv(now + 3_600, now + 7_200),
                },
            ],
        }))
    }

    async fn clock(
        &self,
        _r: Request<ps::ClockRequest>,
    ) -> Result<Response<ps::ClockResponse>, Status> {
        unimpl("Clock")
    }

    async fn get_constituents(
        &self,
        _r: Request<ps::GetConstituentsRequest>,
    ) -> Result<Response<ps::GetConstituentsResponse>, Status> {
        unimpl("GetConstituents")
    }
}

#[tonic::async_trait]
impl UsageMetricsService for Mock {
    async fn get_usage_metrics(
        &self,
        _r: Request<pt::GetUsageMetricsRequest>,
    ) -> Result<Response<pt::GetUsageMetricsResponse>, Status> {
        Ok(Response::new(pt::GetUsageMetricsResponse {
            quotas: vec![],
        }))
    }
}
