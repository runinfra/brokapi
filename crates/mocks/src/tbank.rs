//! In-process gRPC mock of the T-Invest API (tonic server, plain HTTP on 127.0.0.1).
//!
//! Serves what the connectors call: market data (unary + bidirectional stream), orders (unary +
//! server streams), portfolio, instruments, accounts. Two accounts with different data, so tests can
//! prove account isolation. State is real: placed orders can be read, cancelled and replaced.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use brokapi_core::{Decimal, dec};
use brokapi_tbank_protocol::money::{decimal_to_money, decimal_to_quotation, quotation_to_decimal};
use futures_util::Stream;

/// A number as the money value of the mock (`rub`). The mock's numbers have at most 9 decimals.
fn money(d: Decimal) -> pb::MoneyValue {
    decimal_to_money("rub", d).expect("mock money fits units and nano")
}

/// A number as a quotation. The mock's numbers have at most 9 decimals.
fn quot(d: Decimal) -> pb::Quotation {
    decimal_to_quotation(d).expect("mock price fits units and nano")
}
use brokapi_tbank_protocol::v1 as pb;
use brokapi_tbank_protocol::v1::{
    instruments_service_server::{InstrumentsService, InstrumentsServiceServer},
    market_data_request::Payload as Req,
    market_data_response::Payload as Resp,
    market_data_service_server::{MarketDataService, MarketDataServiceServer},
    market_data_stream_service_server::{MarketDataStreamService, MarketDataStreamServiceServer},
    operations_service_server::{OperationsService, OperationsServiceServer},
    operations_stream_service_server::{OperationsStreamService, OperationsStreamServiceServer},
    order_state_stream_response as osr,
    orders_service_server::{OrdersService, OrdersServiceServer},
    orders_stream_service_server::{OrdersStreamService, OrdersStreamServiceServer},
    stop_orders_service_server::{StopOrdersService, StopOrdersServiceServer},
    users_service_server::{UsersService, UsersServiceServer},
};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

pub const TOKEN: &str = "mock-token";
pub const ACCOUNT_A: &str = "acc-a";
pub const ACCOUNT_B: &str = "acc-b";

type Out<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

/// Knobs a test can turn.
#[derive(Default)]
pub struct MockOptions {
    /// `GetCandles` answers a series of bars for the range asked (one per interval step) and refuses a range
    /// longer than the T-Invest limit for the interval with `INVALID_ARGUMENT` 30014. Off: two fixed bars.
    pub synthetic_candles: AtomicBool,
    /// With `synthetic_candles`: 1 makes `GetCandles` answer `INVALID_ARGUMENT` (the caller sees `Rejected`, the read
    /// fails again), 2 makes it answer `UNAVAILABLE` (a network error, a later try may pass), 0 is normal.
    pub candle_fail: std::sync::atomic::AtomicU8,
    /// Every `GetCandles` request seen: (interval, from seconds, to seconds).
    pub candle_requests: Mutex<Vec<(i32, i64, i64)>>,
    /// Delay before `PostOrder` answers (to provoke a client timeout).
    pub post_delay_ms: AtomicU64,
    /// `PostOrder` answers with this gRPC code instead of placing.
    pub post_fail: Mutex<Option<tonic::Code>>,
    /// Order book messages pushed after a book subscription is confirmed (default 3).
    pub book_burst: AtomicU32,
    /// Close the market data stream after this many data messages on a connection (0 = never).
    pub md_cut_after: AtomicU32,
    pub md_connections: AtomicU32,
    pub post_calls: AtomicU32,
    /// Subscribing to this ticker is answered `INSTRUMENT_NOT_FOUND`.
    pub md_refuse_ticker: Mutex<Option<String>>,
    /// Every subscribe request seen on the market data stream: (action, instrument_id, depth).
    pub md_requests: Mutex<Vec<(i32, String, i32)>>,
    /// The market data stream sends pushes only, never confirmations (provokes the confirm timeout).
    pub md_silent: AtomicBool,
    /// Whole-kind listing calls seen: (method, instrument_status).
    pub list_calls: Mutex<Vec<(String, i32)>>,
    /// Authorization header values seen.
    pub auth_seen: Mutex<Vec<String>>,
    /// `PostStopOrder` calls received (the delay `post_delay_ms` applies to them too).
    pub stop_post_calls: AtomicU32,
    /// `GetLastPrices` calls served (a method no connector wraps, used to test `raw()`).
    pub last_price_calls: AtomicU32,
}

#[derive(Clone)]
struct Inst {
    uid: &'static str,
    figi: &'static str,
    ticker: &'static str,
    class: &'static str,
    lot: i32,
}

const SBER: Inst = Inst {
    uid: "uid-sber",
    figi: "BBG004730N88",
    ticker: "SBER",
    class: "TQBR",
    lot: 10,
};
const GAZP: Inst = Inst {
    uid: "uid-gazp",
    figi: "BBG004730RP0",
    ticker: "GAZP",
    class: "TQBR",
    lot: 10,
};

fn lookup(r: &str) -> Option<Inst> {
    [SBER, GAZP].into_iter().find(|i| {
        r == i.uid || r == i.figi || r.eq_ignore_ascii_case(&format!("{}_{}", i.ticker, i.class))
    })
}

#[derive(Clone)]
struct MockOrder {
    exchange_id: String,
    request_id: String,
    account: String,
    inst: Inst,
    direction: i32,
    order_type: i32,
    lots: i64,
    executed: i64,
    price: Decimal,
    status: i32,
}

#[derive(Clone)]
struct OrderEvent {
    account: String,
    state: osr::OrderState,
    trades: Option<pb::OrderTrades>,
}

struct State {
    opts: Arc<MockOptions>,
    orders: Mutex<HashMap<String, MockOrder>>,
    /// Stop orders by broker id, with the account they belong to.
    stops: Mutex<HashMap<String, (String, pb::StopOrder)>>,
    next_id: AtomicU64,
    events: broadcast::Sender<OrderEvent>,
}

use pb::OrderExecutionReportStatus as St;

impl State {
    fn stream_state(o: &MockOrder) -> osr::OrderState {
        osr::OrderState {
            order_id: o.exchange_id.clone(),
            order_request_id: Some(o.request_id.clone()),
            execution_report_status: o.status,
            ticker: o.inst.ticker.into(),
            class_code: o.inst.class.into(),
            lot_size: o.inst.lot,
            direction: o.direction,
            order_type: o.order_type,
            account_id: o.account.clone(),
            order_price: Some(money(o.price)),
            lots_requested: o.lots,
            lots_executed: o.executed,
            lots_left: o.lots - o.executed,
            instrument_uid: o.inst.uid.into(),
            ..Default::default()
        }
    }

    fn unary_state(o: &MockOrder) -> pb::OrderState {
        pb::OrderState {
            order_id: o.exchange_id.clone(),
            execution_report_status: o.status,
            lots_requested: o.lots,
            lots_executed: o.executed,
            initial_security_price: Some(money(o.price)),
            figi: o.inst.figi.into(),
            direction: o.direction,
            order_type: o.order_type,
            instrument_uid: o.inst.uid.into(),
            order_request_id: o.request_id.clone(),
            ticker: o.inst.ticker.into(),
            class_code: o.inst.class.into(),
            ..Default::default()
        }
    }

    fn post_response(o: &MockOrder) -> pb::PostOrderResponse {
        pb::PostOrderResponse {
            order_id: o.exchange_id.clone(),
            execution_report_status: o.status,
            lots_requested: o.lots,
            lots_executed: o.executed,
            figi: o.inst.figi.into(),
            direction: o.direction,
            initial_security_price: Some(money(o.price)),
            order_type: o.order_type,
            instrument_uid: o.inst.uid.into(),
            ticker: o.inst.ticker.into(),
            class_code: o.inst.class.into(),
            order_request_id: o.request_id.clone(),
            ..Default::default()
        }
    }

    fn publish(&self, o: &MockOrder, trades: Option<pb::OrderTrades>) {
        let _ = self.events.send(OrderEvent {
            account: o.account.clone(),
            state: Self::stream_state(o),
            trades,
        });
    }

    fn find(&self, account: &str, id: &str, by_request: bool) -> Option<MockOrder> {
        self.orders
            .lock()
            .unwrap()
            .values()
            .find(|o| {
                o.account == account
                    && if by_request {
                        o.request_id == id
                    } else {
                        o.exchange_id == id
                    }
            })
            .cloned()
    }

    fn update(&self, exchange_id: &str, f: impl FnOnce(&mut MockOrder)) -> Option<MockOrder> {
        let mut orders = self.orders.lock().unwrap();
        let o = orders.get_mut(exchange_id)?;
        f(o);
        Some(o.clone())
    }
}

#[derive(Clone)]
struct Mock(Arc<State>);

impl Mock {
    fn list_called(&self, method: &str, r: &pb::InstrumentsRequest) {
        self.0
            .opts
            .list_calls
            .lock()
            .unwrap()
            .push((method.to_string(), r.instrument_status.unwrap_or(0)));
    }
}

fn ts(secs: i64) -> Option<prost_types::Timestamp> {
    Some(prost_types::Timestamp {
        seconds: secs,
        nanos: 0,
    })
}

fn order(price: Decimal, qty: i64) -> pb::Order {
    pb::Order {
        price: Some(quot(price)),
        quantity: qty,
    }
}

fn book_msg(i: &Inst, depth: i32, n: u32) -> pb::OrderBook {
    let step = Decimal::from(n) * dec!(0.01);
    pb::OrderBook {
        figi: i.figi.into(),
        depth,
        is_consistent: true,
        bids: (0..depth.min(5))
            .map(|k| {
                order(
                    dec!(273.90) - dec!(0.1) * Decimal::from(k) + step,
                    100 - 10 * i64::from(k),
                )
            })
            .collect(),
        asks: (0..depth.min(5))
            .map(|k| {
                order(
                    dec!(274.00) + dec!(0.1) * Decimal::from(k) + step,
                    80 + 10 * i64::from(k),
                )
            })
            .collect(),
        time: ts(1_700_000_000 + i64::from(n)),
        instrument_uid: i.uid.into(),
        ticker: i.ticker.into(),
        class_code: i.class.into(),
        ..Default::default()
    }
}

#[tonic::async_trait]
impl MarketDataService for Mock {
    async fn get_candles(
        &self,
        r: Request<pb::GetCandlesRequest>,
    ) -> Result<Response<pb::GetCandlesResponse>, Status> {
        let opts = &self.0.opts;
        if opts.synthetic_candles.load(Ordering::Relaxed) {
            let r = r.into_inner();
            let from = r.from.as_ref().map(|t| t.seconds).unwrap_or(0);
            let to = r.to.as_ref().map(|t| t.seconds).unwrap_or(0);
            opts.candle_requests
                .lock()
                .unwrap()
                .push((r.interval, from, to));
            match opts.candle_fail.load(Ordering::Relaxed) {
                1 => return Err(Status::invalid_argument("mock: candle read refused")),
                2 => return Err(Status::unavailable("mock: candle read failed")),
                _ => {}
            }
            if to < from {
                return Err(Status::invalid_argument("30012"));
            }
            // interval -> (step in seconds, longest span in days): the table of the T-Invest docs
            let (step, max_days) = match r.interval {
                1 => (60, 1),
                2 => (300, 7),
                3 => (900, 21),
                4 => (3_600, 90),
                5 => (86_400, 6 * 365),
                8 => (600, 7),
                9 => (1_800, 21),
                11 => (14_400, 90),
                12 => (7 * 86_400, 5 * 365),
                13 => (30 * 86_400, 10 * 365),
                _ => return Err(Status::invalid_argument("30011")),
            };
            if to - from > max_days * 86_400 {
                return Err(Status::invalid_argument("30014"));
            }
            let candles = crate::synthetic_bars(from, to, step)
                .into_iter()
                .map(|(t, o, h, l, c, v)| pb::HistoricCandle {
                    open: Some(quot(o)),
                    high: Some(quot(h)),
                    low: Some(quot(l)),
                    close: Some(quot(c)),
                    volume: v,
                    time: ts(t),
                    is_complete: true,
                    ..Default::default()
                })
                .collect();
            return Ok(Response::new(pb::GetCandlesResponse { candles }));
        }
        let c = |t: i64, o: Decimal| pb::HistoricCandle {
            open: Some(quot(o)),
            high: Some(quot(o + Decimal::ONE)),
            low: Some(quot(o - Decimal::ONE)),
            close: Some(quot(o + dec!(0.5))),
            volume: 1000,
            time: ts(t),
            is_complete: true,
            ..Default::default()
        };
        Ok(Response::new(pb::GetCandlesResponse {
            candles: vec![c(1_700_000_000, dec!(270)), c(1_700_000_300, dec!(271))],
        }))
    }

    async fn get_order_book(
        &self,
        r: Request<pb::GetOrderBookRequest>,
    ) -> Result<Response<pb::GetOrderBookResponse>, Status> {
        let r = r.into_inner();
        let i = lookup(r.instrument_id.as_deref().unwrap_or(""))
            .ok_or_else(|| Status::not_found("50002"))?;
        let b = book_msg(&i, r.depth, 0);
        Ok(Response::new(pb::GetOrderBookResponse {
            figi: i.figi.into(),
            depth: r.depth,
            bids: b.bids,
            asks: b.asks,
            instrument_uid: i.uid.into(),
            ticker: i.ticker.into(),
            class_code: i.class.into(),
            orderbook_ts: ts(1_700_000_000),
            ..Default::default()
        }))
    }

    async fn get_last_prices(
        &self,
        r: Request<pb::GetLastPricesRequest>,
    ) -> Result<Response<pb::GetLastPricesResponse>, Status> {
        self.0.opts.last_price_calls.fetch_add(1, Ordering::SeqCst);
        let prices = r
            .into_inner()
            .instrument_id
            .iter()
            .filter_map(|r| lookup(r))
            .map(|i| pb::LastPrice {
                price: Some(quot(dec!(274.05))),
                time: ts(1_700_000_000),
                ticker: i.ticker.into(),
                class_code: i.class.into(),
                instrument_uid: i.uid.into(),
                ..Default::default()
            })
            .collect();
        Ok(Response::new(pb::GetLastPricesResponse {
            last_prices: prices,
        }))
    }

    async fn get_trading_status(
        &self,
        r: Request<pb::GetTradingStatusRequest>,
    ) -> Result<Response<pb::GetTradingStatusResponse>, Status> {
        let _ = r;
        Ok(Response::new(pb::GetTradingStatusResponse {
            trading_status: pb::SecurityTradingStatus::NormalTrading as i32,
            limit_order_available_flag: true,
            market_order_available_flag: true,
            ..Default::default()
        }))
    }

    async fn get_last_trades(
        &self,
        r: Request<pb::GetLastTradesRequest>,
    ) -> Result<Response<pb::GetLastTradesResponse>, Status> {
        let _ = r;
        let t = |p: Decimal, d: i32| pb::Trade {
            direction: d,
            price: Some(quot(p)),
            quantity: 5,
            time: ts(1_700_000_000),
            ..Default::default()
        };
        Ok(Response::new(pb::GetLastTradesResponse {
            trades: vec![
                t(dec!(273.9), pb::TradeDirection::Buy as i32),
                t(dec!(274.0), pb::TradeDirection::Sell as i32),
                t(dec!(274.1), pb::TradeDirection::Buy as i32),
            ],
        }))
    }
}

#[tonic::async_trait]
impl MarketDataStreamService for Mock {
    async fn market_data_stream(
        &self,
        request: Request<Streaming<pb::MarketDataRequest>>,
    ) -> Result<Response<Out<pb::MarketDataResponse>>, Status> {
        let opts = Arc::clone(&self.0.opts);
        opts.md_connections.fetch_add(1, Ordering::SeqCst);
        let mut input = request.into_inner();
        let (tx, rx) = mpsc::channel::<Result<pb::MarketDataResponse, Status>>(1024);
        tokio::spawn(async move {
            let mut data_sent = 0u32;
            while let Ok(Some(msg)) = input.message().await {
                let Some(payload) = msg.payload else { continue };
                match payload {
                    Req::SubscribeOrderBookRequest(r) => {
                        for ins in &r.instruments {
                            opts.md_requests.lock().unwrap().push((
                                r.subscription_action,
                                ins.instrument_id.clone(),
                                ins.depth,
                            ));
                            let known = lookup(&ins.instrument_id);
                            let refuse = known.as_ref().is_some_and(|k| {
                                opts.md_refuse_ticker.lock().unwrap().as_deref() == Some(k.ticker)
                            });
                            let status = if known.is_none() || refuse {
                                pb::SubscriptionStatus::InstrumentNotFound
                            } else {
                                pb::SubscriptionStatus::Success
                            };
                            let i = known.clone().unwrap_or(SBER);
                            if !opts.md_silent.load(Ordering::SeqCst) {
                                let ack = pb::OrderBookSubscription {
                                    figi: i.figi.into(),
                                    depth: ins.depth,
                                    subscription_status: status as i32,
                                    instrument_uid: i.uid.into(),
                                    ticker: i.ticker.into(),
                                    class_code: i.class.into(),
                                    subscription_action: r.subscription_action,
                                    ..Default::default()
                                };
                                let resp = pb::SubscribeOrderBookResponse {
                                    order_book_subscriptions: vec![ack],
                                    ..Default::default()
                                };
                                let _ = tx
                                    .send(Ok(pb::MarketDataResponse {
                                        payload: Some(Resp::SubscribeOrderBookResponse(resp)),
                                    }))
                                    .await;
                            }
                            if status == pb::SubscriptionStatus::Success
                                && !opts.md_silent.load(Ordering::SeqCst)
                                && r.subscription_action == pb::SubscriptionAction::Subscribe as i32
                            {
                                let burst = match opts.book_burst.load(Ordering::SeqCst) {
                                    0 => 3,
                                    n => n,
                                };
                                let (tx2, opts2, depth) =
                                    (tx.clone(), Arc::clone(&opts), ins.depth);
                                tokio::spawn(async move {
                                    for n in 0..burst {
                                        let cut = opts2.md_cut_after.load(Ordering::SeqCst);
                                        let msg = pb::MarketDataResponse {
                                            payload: Some(Resp::Orderbook(book_msg(&i, depth, n))),
                                        };
                                        if tx2.send(Ok(msg)).await.is_err() {
                                            return;
                                        }
                                        let _ = cut;
                                        tokio::time::sleep(Duration::from_millis(5)).await;
                                    }
                                });
                                data_sent += burst;
                            }
                        }
                    }
                    Req::SubscribeTradesRequest(r) => {
                        for ins in &r.instruments {
                            let i = lookup(&ins.instrument_id).unwrap_or(SBER);
                            let ack = pb::TradeSubscription {
                                figi: i.figi.into(),
                                subscription_status: pb::SubscriptionStatus::Success as i32,
                                instrument_uid: i.uid.into(),
                                subscription_action: r.subscription_action,
                                ticker: i.ticker.into(),
                                class_code: i.class.into(),
                                ..Default::default()
                            };
                            let resp = pb::SubscribeTradesResponse {
                                trade_subscriptions: vec![ack],
                                ..Default::default()
                            };
                            let _ = tx
                                .send(Ok(pb::MarketDataResponse {
                                    payload: Some(Resp::SubscribeTradesResponse(resp)),
                                }))
                                .await;
                            let trade = pb::Trade {
                                figi: i.figi.into(),
                                direction: pb::TradeDirection::Buy as i32,
                                price: Some(quot(dec!(274))),
                                quantity: 7,
                                time: ts(1_700_000_001),
                                instrument_uid: i.uid.into(),
                                ticker: i.ticker.into(),
                                class_code: i.class.into(),
                                ..Default::default()
                            };
                            let _ = tx
                                .send(Ok(pb::MarketDataResponse {
                                    payload: Some(Resp::Trade(trade)),
                                }))
                                .await;
                        }
                    }
                    Req::SubscribeCandlesRequest(r) => {
                        for ins in &r.instruments {
                            let i = lookup(&ins.instrument_id).unwrap_or(SBER);
                            let ack = pb::CandleSubscription {
                                figi: i.figi.into(),
                                interval: ins.interval,
                                subscription_status: pb::SubscriptionStatus::Success as i32,
                                instrument_uid: i.uid.into(),
                                subscription_action: r.subscription_action,
                                ticker: i.ticker.into(),
                                class_code: i.class.into(),
                                ..Default::default()
                            };
                            let resp = pb::SubscribeCandlesResponse {
                                candles_subscriptions: vec![ack],
                                ..Default::default()
                            };
                            let _ = tx
                                .send(Ok(pb::MarketDataResponse {
                                    payload: Some(Resp::SubscribeCandlesResponse(resp)),
                                }))
                                .await;
                            let c = pb::Candle {
                                figi: i.figi.into(),
                                interval: ins.interval,
                                open: Some(quot(dec!(270))),
                                high: Some(quot(dec!(275))),
                                low: Some(quot(dec!(269))),
                                close: Some(quot(dec!(274))),
                                volume: 500,
                                time: ts(1_700_000_000),
                                instrument_uid: i.uid.into(),
                                ticker: i.ticker.into(),
                                class_code: i.class.into(),
                                ..Default::default()
                            };
                            let _ = tx
                                .send(Ok(pb::MarketDataResponse {
                                    payload: Some(Resp::Candle(c)),
                                }))
                                .await;
                        }
                    }
                    _ => {}
                }
                let cut = opts.md_cut_after.load(Ordering::SeqCst);
                if cut > 0 && data_sent >= cut {
                    // Break the stream (once per connection) to test the reconnect.
                    opts.md_cut_after.store(0, Ordering::SeqCst);
                    let _ = tx.send(Err(Status::unavailable("cut by mock"))).await;
                    return;
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

fn account_of(accounts: &[String]) -> String {
    accounts.first().cloned().unwrap_or_default()
}

#[tonic::async_trait]
impl OrdersService for Mock {
    async fn post_order(
        &self,
        request: Request<pb::PostOrderRequest>,
    ) -> Result<Response<pb::PostOrderResponse>, Status> {
        self.0.opts.post_calls.fetch_add(1, Ordering::SeqCst);
        if let Some(code) = *self.0.opts.post_fail.lock().unwrap() {
            return Err(Status::new(code, "mock failure"));
        }
        let r = request.into_inner();
        // A limit order at the marker price is refused (conformance: a refusal must not block the instrument).
        let price = r.price.as_ref().map(quotation_to_decimal);
        if price == Some(brokapi_conformance::REJECT_PRICE.decimal()) {
            return Err(Status::invalid_argument("30042"));
        }
        let inst = lookup(&r.instrument_id).ok_or_else(|| Status::not_found("50002"))?;
        if r.account_id != ACCOUNT_A && r.account_id != ACCOUNT_B {
            return Err(Status::not_found("50004"));
        }
        let id = self.0.next_id.fetch_add(1, Ordering::SeqCst);
        let o = MockOrder {
            exchange_id: format!("EX{id}"),
            request_id: r.order_id,
            account: r.account_id,
            inst,
            direction: r.direction,
            order_type: r.order_type,
            lots: r.quantity,
            executed: 0,
            price: r
                .price
                .map(|p| quotation_to_decimal(&p))
                .unwrap_or(Decimal::ZERO),
            status: St::ExecutionReportStatusNew as i32,
        };
        // The exchange takes the order after `post_delay_ms` even if the client gave up waiting
        // (the work runs detached, like a real broker whose answer is late).
        let delay = self.0.opts.post_delay_ms.load(Ordering::SeqCst);
        let state = Arc::clone(&self.0);
        let placed = o.clone();
        let work = tokio::spawn(async move {
            if delay > 0 {
                tokio::time::sleep(Duration::from_millis(delay)).await;
            }
            state
                .orders
                .lock()
                .unwrap()
                .insert(placed.exchange_id.clone(), placed.clone());
            state.publish(&placed, None);
        });
        let _ = work.await;
        Ok(Response::new(State::post_response(&o)))
    }

    async fn cancel_order(
        &self,
        request: Request<pb::CancelOrderRequest>,
    ) -> Result<Response<pb::CancelOrderResponse>, Status> {
        let r = request.into_inner();
        let by_request = r.order_id_type == Some(pb::OrderIdType::Request as i32);
        let o = self
            .0
            .find(&r.account_id, &r.order_id, by_request)
            .ok_or_else(|| Status::not_found("50005"))?;
        let o = self
            .0
            .update(&o.exchange_id, |o| {
                o.status = St::ExecutionReportStatusCancelled as i32
            })
            .expect("exists");
        self.0.publish(&o, None);
        Ok(Response::new(pb::CancelOrderResponse {
            time: ts(1_700_000_000),
            ..Default::default()
        }))
    }

    async fn get_order_state(
        &self,
        request: Request<pb::GetOrderStateRequest>,
    ) -> Result<Response<pb::OrderState>, Status> {
        let r = request.into_inner();
        let by_request = r.order_id_type == Some(pb::OrderIdType::Request as i32);
        let o = self
            .0
            .find(&r.account_id, &r.order_id, by_request)
            .ok_or_else(|| Status::not_found("50005"))?;
        Ok(Response::new(State::unary_state(&o)))
    }

    async fn get_orders(
        &self,
        request: Request<pb::GetOrdersRequest>,
    ) -> Result<Response<pb::GetOrdersResponse>, Status> {
        let r = request.into_inner();
        let mut orders: Vec<MockOrder> = self
            .0
            .orders
            .lock()
            .unwrap()
            .values()
            .filter(|o| {
                o.account == r.account_id
                    && (o.status == St::ExecutionReportStatusNew as i32
                        || o.status == St::ExecutionReportStatusPartiallyfill as i32)
            })
            .cloned()
            .collect();
        orders.sort_by(|a, b| a.exchange_id.cmp(&b.exchange_id));
        Ok(Response::new(pb::GetOrdersResponse {
            orders: orders.iter().map(State::unary_state).collect(),
        }))
    }

    async fn replace_order(
        &self,
        request: Request<pb::ReplaceOrderRequest>,
    ) -> Result<Response<pb::PostOrderResponse>, Status> {
        let r = request.into_inner();
        let old = self
            .0
            .find(&r.account_id, &r.order_id, false)
            .ok_or_else(|| Status::not_found("50005"))?;
        let old = self
            .0
            .update(&old.exchange_id, |o| {
                o.status = St::ExecutionReportStatusCancelled as i32
            })
            .expect("exists");
        self.0.publish(&old, None);
        let id = self.0.next_id.fetch_add(1, Ordering::SeqCst);
        let new = MockOrder {
            exchange_id: format!("EX{id}"),
            request_id: r.idempotency_key,
            lots: r.quantity,
            executed: 0,
            price: r
                .price
                .map(|p| quotation_to_decimal(&p))
                .unwrap_or(old.price),
            status: St::ExecutionReportStatusNew as i32,
            ..old
        };
        self.0
            .orders
            .lock()
            .unwrap()
            .insert(new.exchange_id.clone(), new.clone());
        self.0.publish(&new, None);
        Ok(Response::new(State::post_response(&new)))
    }
}

#[tonic::async_trait]
impl OrdersStreamService for Mock {
    async fn trades_stream(
        &self,
        request: Request<pb::TradesStreamRequest>,
    ) -> Result<Response<Out<pb::TradesStreamResponse>>, Status> {
        let account = account_of(&request.into_inner().accounts);
        let mut events = self.0.events.subscribe();
        let (tx, rx) = mpsc::channel(256);
        tokio::spawn(async move {
            let ack = pb::SubscriptionResponse {
                status: pb::ResultSubscriptionStatus::Ok as i32,
                accounts: vec![account.clone()],
                ..Default::default()
            };
            let _ = tx
                .send(Ok(pb::TradesStreamResponse {
                    payload: Some(pb::trades_stream_response::Payload::Subscription(ack)),
                }))
                .await;
            while let Ok(ev) = events.recv().await {
                if ev.account != account {
                    continue;
                }
                if let Some(t) = ev.trades {
                    let msg = pb::TradesStreamResponse {
                        payload: Some(pb::trades_stream_response::Payload::OrderTrades(t)),
                    };
                    if tx.send(Ok(msg)).await.is_err() {
                        return;
                    }
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn order_state_stream(
        &self,
        request: Request<pb::OrderStateStreamRequest>,
    ) -> Result<Response<Out<pb::OrderStateStreamResponse>>, Status> {
        let accounts = request.into_inner().accounts;
        let account = account_of(&accounts);
        let mut events = self.0.events.subscribe();
        let (tx, rx) = mpsc::channel(256);
        tokio::spawn(async move {
            let ack = pb::SubscriptionResponse {
                status: pb::ResultSubscriptionStatus::Ok as i32,
                accounts: vec![account.clone()],
                ..Default::default()
            };
            let _ = tx
                .send(Ok(pb::OrderStateStreamResponse {
                    payload: Some(osr::Payload::Subscription(ack)),
                }))
                .await;
            while let Ok(ev) = events.recv().await {
                if ev.account != account {
                    continue;
                }
                let msg = pb::OrderStateStreamResponse {
                    payload: Some(osr::Payload::OrderState(ev.state)),
                };
                if tx.send(Ok(msg)).await.is_err() {
                    return;
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

fn cash_of(account: &str) -> Decimal {
    if account == ACCOUNT_A {
        dec!(100000)
    } else {
        dec!(5000)
    }
}

fn portfolio_of(account: &str) -> pb::PortfolioResponse {
    let qty: Decimal = if account == ACCOUNT_A {
        dec!(30)
    } else {
        dec!(7)
    };
    pb::PortfolioResponse {
        total_amount_portfolio: Some(money(cash_of(account) + qty * dec!(274))),
        account_id: account.into(),
        positions: vec![
            pb::PortfolioPosition {
                figi: SBER.figi.into(),
                instrument_type: "share".into(),
                quantity: Some(quot(qty)),
                average_position_price: Some(money(dec!(250))),
                current_price: Some(money(dec!(274))),
                instrument_uid: SBER.uid.into(),
                ticker: SBER.ticker.into(),
                class_code: SBER.class.into(),
                ..Default::default()
            },
            pb::PortfolioPosition {
                instrument_type: "currency".into(),
                figi: "RUB000UTSTOM".into(),
                ..Default::default()
            },
        ],
        ..Default::default()
    }
}

#[tonic::async_trait]
impl OperationsService for Mock {
    async fn get_portfolio(
        &self,
        request: Request<pb::PortfolioRequest>,
    ) -> Result<Response<pb::PortfolioResponse>, Status> {
        Ok(Response::new(portfolio_of(
            &request.into_inner().account_id,
        )))
    }

    async fn get_positions(
        &self,
        request: Request<pb::PositionsRequest>,
    ) -> Result<Response<pb::PositionsResponse>, Status> {
        let account = request.into_inner().account_id;
        Ok(Response::new(pb::PositionsResponse {
            money: vec![money(cash_of(&account))],
            blocked: vec![money(dec!(150))],
            account_id: account,
            ..Default::default()
        }))
    }

    async fn get_operations_by_cursor(
        &self,
        request: Request<pb::GetOperationsByCursorRequest>,
    ) -> Result<Response<pb::GetOperationsByCursorResponse>, Status> {
        let account = request.into_inner().account_id;
        let qty = if account == ACCOUNT_A { 10 } else { 3 };
        let op = pb::OperationItem {
            id: format!("op-{account}"),
            r#type: pb::OperationType::Buy as i32,
            ticker: "SBER".into(),
            class_code: "TQBR".into(),
            instrument_uid: SBER.uid.into(),
            commission: Some(money(dec!(-1))),
            trades_info: Some(pb::OperationItemTrades {
                trades: vec![pb::OperationItemTrade {
                    num: "T1".into(),
                    quantity: qty,
                    price: Some(money(dec!(270))),
                    date: ts(1_700_000_000),
                    ..Default::default()
                }],
            }),
            ..Default::default()
        };
        Ok(Response::new(pb::GetOperationsByCursorResponse {
            has_next: false,
            next_cursor: String::new(),
            items: vec![op],
        }))
    }
}

#[tonic::async_trait]
impl OperationsStreamService for Mock {
    async fn portfolio_stream(
        &self,
        request: Request<pb::PortfolioStreamRequest>,
    ) -> Result<Response<Out<pb::PortfolioStreamResponse>>, Status> {
        let account = account_of(&request.into_inner().accounts);
        let (tx, rx) = mpsc::channel(16);
        tokio::spawn(async move {
            use pb::portfolio_stream_response::Payload as P;
            let ack = pb::PortfolioSubscriptionResult {
                accounts: vec![pb::AccountSubscriptionStatus {
                    account_id: account.clone(),
                    subscription_status: pb::PortfolioSubscriptionStatus::Success as i32,
                }],
                ..Default::default()
            };
            let _ = tx
                .send(Ok(pb::PortfolioStreamResponse {
                    payload: Some(P::Subscriptions(ack)),
                }))
                .await;
            let _ = tx
                .send(Ok(pb::PortfolioStreamResponse {
                    payload: Some(P::Portfolio(portfolio_of(&account))),
                }))
                .await;
            // Keep the stream open until the client goes away.
            tx.closed().await;
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn positions_stream(
        &self,
        request: Request<pb::PositionsStreamRequest>,
    ) -> Result<Response<Out<pb::PositionsStreamResponse>>, Status> {
        let account = account_of(&request.into_inner().accounts);
        let (tx, rx) = mpsc::channel(16);
        tokio::spawn(async move {
            use pb::positions_stream_response::Payload as P;
            let ack = pb::PositionsSubscriptionResult {
                accounts: vec![pb::PositionsSubscriptionStatus {
                    account_id: account.clone(),
                    subscription_status:
                        pb::PositionsAccountSubscriptionStatus::PositionsSubscriptionStatusSuccess
                            as i32,
                }],
                ..Default::default()
            };
            let _ = tx
                .send(Ok(pb::PositionsStreamResponse {
                    payload: Some(P::Subscriptions(ack)),
                }))
                .await;
            let initial = pb::PositionsResponse {
                money: vec![money(cash_of(&account))],
                account_id: account.clone(),
                ..Default::default()
            };
            let _ = tx
                .send(Ok(pb::PositionsStreamResponse {
                    payload: Some(P::InitialPositions(initial)),
                }))
                .await;
            tx.closed().await;
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

#[tonic::async_trait]
impl UsersService for Mock {
    async fn get_accounts(
        &self,
        _: Request<pb::GetAccountsRequest>,
    ) -> Result<Response<pb::GetAccountsResponse>, Status> {
        let acc = |id: &str| pb::Account {
            id: id.into(),
            name: id.into(),
            status: pb::AccountStatus::Open as i32,
            ..Default::default()
        };
        Ok(Response::new(pb::GetAccountsResponse {
            accounts: vec![acc(ACCOUNT_A), acc(ACCOUNT_B)],
        }))
    }
}

#[tonic::async_trait]
impl InstrumentsService for Mock {
    async fn find_instrument(
        &self,
        request: Request<pb::FindInstrumentRequest>,
    ) -> Result<Response<pb::FindInstrumentResponse>, Status> {
        let q = request.into_inner().query.to_uppercase();
        let instruments = [SBER, GAZP]
            .into_iter()
            .filter(|i| i.ticker.contains(&q) || q == "RU0009029540")
            .map(|i| pb::InstrumentShort {
                isin: if i.ticker == "SBER" {
                    "RU0009029540".into()
                } else {
                    "RU0007661625".into()
                },
                figi: i.figi.into(),
                ticker: i.ticker.into(),
                class_code: i.class.into(),
                instrument_type: "share".into(),
                name: i.ticker.into(),
                uid: i.uid.into(),
                instrument_kind: pb::InstrumentType::Share as i32,
                api_trade_available_flag: true,
                lot: i.lot,
                ..Default::default()
            })
            .collect();
        Ok(Response::new(pb::FindInstrumentResponse { instruments }))
    }

    async fn get_instrument_by(
        &self,
        request: Request<pb::InstrumentRequest>,
    ) -> Result<Response<pb::InstrumentResponse>, Status> {
        let r = request.into_inner();
        let i = [SBER, GAZP]
            .into_iter()
            .find(|i| r.id == i.uid || r.id == i.figi || r.id == i.ticker)
            .ok_or_else(|| Status::not_found("50002"))?;
        Ok(Response::new(pb::InstrumentResponse {
            instrument: Some(pb::Instrument {
                figi: i.figi.into(),
                ticker: i.ticker.into(),
                class_code: i.class.into(),
                isin: "RU0009029540".into(),
                lot: i.lot,
                currency: "rub".into(),
                name: i.ticker.into(),
                exchange: "MOEX".into(),
                min_price_increment: Some(quot(dec!(0.01))),
                uid: i.uid.into(),
                instrument_kind: pb::InstrumentType::Share as i32,
                ..Default::default()
            }),
        }))
    }

    async fn shares(
        &self,
        request: Request<pb::InstrumentsRequest>,
    ) -> Result<Response<pb::SharesResponse>, Status> {
        self.list_called("Shares", request.get_ref());
        let instruments = [SBER, GAZP]
            .into_iter()
            .map(|i| pb::Share {
                uid: i.uid.into(),
                figi: i.figi.into(),
                ticker: i.ticker.into(),
                class_code: i.class.into(),
                isin: if i.ticker == "SBER" {
                    "RU0009029540".into()
                } else {
                    "RU0007661625".into()
                },
                lot: i.lot,
                currency: "rub".into(),
                name: i.ticker.into(),
                ..Default::default()
            })
            .collect();
        Ok(Response::new(pb::SharesResponse { instruments }))
    }

    async fn bonds(
        &self,
        request: Request<pb::InstrumentsRequest>,
    ) -> Result<Response<pb::BondsResponse>, Status> {
        self.list_called("Bonds", request.get_ref());
        Ok(Response::new(pb::BondsResponse {
            instruments: vec![pb::Bond {
                uid: "bond-uid".into(),
                figi: "BBGBOND".into(),
                ticker: "SU26238RMFS4".into(),
                class_code: "TQOB".into(),
                isin: "RU000A1038V6".into(),
                lot: 1,
                currency: "rub".into(),
                name: "OFZ 26238".into(),
                ..Default::default()
            }],
        }))
    }

    async fn etfs(
        &self,
        request: Request<pb::InstrumentsRequest>,
    ) -> Result<Response<pb::EtfsResponse>, Status> {
        self.list_called("Etfs", request.get_ref());
        Ok(Response::new(pb::EtfsResponse {
            instruments: vec![pb::Etf {
                uid: "etf-uid".into(),
                figi: "BBGETF".into(),
                ticker: "TMOS".into(),
                class_code: "TQTF".into(),
                isin: "RU000A101X76".into(),
                lot: 1,
                currency: "rub".into(),
                name: "Tinkoff MOEX".into(),
                ..Default::default()
            }],
        }))
    }

    async fn futures(
        &self,
        request: Request<pb::InstrumentsRequest>,
    ) -> Result<Response<pb::FuturesResponse>, Status> {
        self.list_called("Futures", request.get_ref());
        Ok(Response::new(pb::FuturesResponse {
            instruments: vec![pb::Future {
                uid: "fut-uid".into(),
                figi: "FUTSBER".into(),
                ticker: "SRZ6".into(),
                class_code: "SPBFUT".into(),
                lot: 100,
                currency: "rub".into(),
                name: "SBER-12.26".into(),
                ..Default::default()
            }],
        }))
    }

    async fn options(
        &self,
        request: Request<pb::InstrumentsRequest>,
    ) -> Result<Response<pb::OptionsResponse>, Status> {
        self.list_called("Options", request.get_ref());
        Ok(Response::new(pb::OptionsResponse {
            instruments: vec![pb::Option {
                uid: "opt-uid".into(),
                ticker: "SR300BL6".into(),
                class_code: "SPBOPT".into(),
                lot: 1,
                currency: "rub".into(),
                name: "SBER call 300".into(),
                ..Default::default()
            }],
        }))
    }

    async fn currencies(
        &self,
        request: Request<pb::InstrumentsRequest>,
    ) -> Result<Response<pb::CurrenciesResponse>, Status> {
        self.list_called("Currencies", request.get_ref());
        Ok(Response::new(pb::CurrenciesResponse {
            instruments: vec![pb::Currency {
                uid: "cur-uid".into(),
                figi: "BBGCUR".into(),
                ticker: "CNYRUBF".into(),
                class_code: "CETS".into(),
                lot: 1000,
                currency: "rub".into(),
                name: "Yuan".into(),
                ..Default::default()
            }],
        }))
    }

    async fn trading_schedules(
        &self,
        _: Request<pb::TradingSchedulesRequest>,
    ) -> Result<Response<pb::TradingSchedulesResponse>, Status> {
        Ok(Response::new(pb::TradingSchedulesResponse {
            exchanges: vec![pb::TradingSchedule {
                exchange: "MOEX".into(),
                days: vec![pb::TradingDay {
                    is_trading_day: true,
                    start_time: ts(1_700_000_000),
                    end_time: ts(1_700_030_000),
                    ..Default::default()
                }],
            }],
        }))
    }
}

#[tonic::async_trait]
impl StopOrdersService for Mock {
    async fn post_stop_order(
        &self,
        r: Request<pb::PostStopOrderRequest>,
    ) -> Result<Response<pb::PostStopOrderResponse>, Status> {
        let r = r.into_inner();
        self.0.opts.stop_post_calls.fetch_add(1, Ordering::SeqCst);
        let delay = self.0.opts.post_delay_ms.load(Ordering::SeqCst);
        if delay > 0 {
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }
        let inst = lookup(&r.instrument_id).ok_or_else(|| Status::not_found("50002"))?;
        if r.quantity < 1 {
            return Err(Status::invalid_argument("30016"));
        }
        let n = self.0.next_id.fetch_add(1, Ordering::SeqCst);
        let id = format!("SO{n}");
        let money = |q: &Option<pb::Quotation>| q.as_ref().map(|q| money(quotation_to_decimal(q)));
        let stop = pb::StopOrder {
            stop_order_id: id.clone(),
            lots_requested: r.quantity,
            figi: inst.figi.into(),
            direction: r.direction,
            currency: "rub".into(),
            order_type: r.stop_order_type,
            create_date: ts(1_700_000_000),
            expiration_time: r.expire_date,
            price: money(&r.price),
            stop_price: money(&r.stop_price),
            instrument_uid: inst.uid.into(),
            status: pb::StopOrderStatusOption::StopOrderStatusActive as i32,
            exchange_order_type: r.exchange_order_type,
            ticker: inst.ticker.into(),
            class_code: inst.class.into(),
            ..Default::default()
        };
        self.0
            .stops
            .lock()
            .unwrap()
            .insert(id.clone(), (r.account_id, stop));
        Ok(Response::new(pb::PostStopOrderResponse {
            stop_order_id: id,
            order_request_id: r.order_id,
            response_metadata: None,
        }))
    }

    async fn get_stop_orders(
        &self,
        r: Request<pb::GetStopOrdersRequest>,
    ) -> Result<Response<pb::GetStopOrdersResponse>, Status> {
        let r = r.into_inner();
        let mut stop_orders: Vec<pb::StopOrder> = self
            .0
            .stops
            .lock()
            .unwrap()
            .values()
            .filter(|(acc, o)| {
                *acc == r.account_id && (r.status == 0 || r.status == 1 || o.status == r.status)
            })
            .map(|(_, o)| o.clone())
            .collect();
        stop_orders.sort_by(|a, b| a.stop_order_id.cmp(&b.stop_order_id));
        Ok(Response::new(pb::GetStopOrdersResponse { stop_orders }))
    }

    async fn cancel_stop_order(
        &self,
        r: Request<pb::CancelStopOrderRequest>,
    ) -> Result<Response<pb::CancelStopOrderResponse>, Status> {
        let r = r.into_inner();
        let mut stops = self.0.stops.lock().unwrap();
        match stops.get(&r.stop_order_id) {
            Some((acc, _)) if *acc == r.account_id => {
                stops.remove(&r.stop_order_id);
                Ok(Response::new(pb::CancelStopOrderResponse {
                    time: ts(1_700_000_050),
                }))
            }
            _ => Err(Status::not_found("50005")),
        }
    }
}

/// Running mock bound to `127.0.0.1:0`.
pub struct MockServer {
    pub endpoint: String,
    pub opts: Arc<MockOptions>,
    state: Arc<State>,
    cancel: CancellationToken,
}

impl MockServer {
    pub async fn start(opts: Arc<MockOptions>) -> Self {
        let (events, _) = broadcast::channel(1024);
        let state = Arc::new(State {
            opts: Arc::clone(&opts),
            orders: Mutex::new(HashMap::new()),
            stops: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
            events,
        });
        let mock = Mock(Arc::clone(&state));

        // Every call must carry the Bearer token.
        let auth_opts = Arc::clone(&opts);
        let auth = move |req: Request<()>| -> Result<Request<()>, Status> {
            let v = req
                .metadata()
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_string();
            auth_opts.auth_seen.lock().unwrap().push(v.clone());
            if v == format!("Bearer {TOKEN}") {
                Ok(req)
            } else {
                Err(Status::unauthenticated("40003"))
            }
        };

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind mock");
        let addr = listener.local_addr().expect("addr");
        let cancel = CancellationToken::new();
        let child = cancel.clone();
        let router = Server::builder()
            .add_service(MarketDataServiceServer::with_interceptor(
                mock.clone(),
                auth.clone(),
            ))
            .add_service(MarketDataStreamServiceServer::with_interceptor(
                mock.clone(),
                auth.clone(),
            ))
            .add_service(OrdersServiceServer::with_interceptor(
                mock.clone(),
                auth.clone(),
            ))
            .add_service(OrdersStreamServiceServer::with_interceptor(
                mock.clone(),
                auth.clone(),
            ))
            .add_service(OperationsServiceServer::with_interceptor(
                mock.clone(),
                auth.clone(),
            ))
            .add_service(OperationsStreamServiceServer::with_interceptor(
                mock.clone(),
                auth.clone(),
            ))
            .add_service(UsersServiceServer::with_interceptor(
                mock.clone(),
                auth.clone(),
            ))
            .add_service(StopOrdersServiceServer::with_interceptor(
                mock.clone(),
                auth.clone(),
            ))
            .add_service(InstrumentsServiceServer::with_interceptor(mock, auth));
        tokio::spawn(async move {
            router
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                    child.cancelled().await
                })
                .await
                .expect("mock serve");
        });
        Self {
            endpoint: format!("http://{addr}"),
            opts,
            state,
            cancel,
        }
    }

    /// Fully executes a placed order: state `FILL` plus a trade, both pushed to the account's streams.
    pub fn fill(&self, account: &str, request_id: &str) {
        let o = self
            .state
            .find(account, request_id, true)
            .expect("order exists");
        let o = self
            .state
            .update(&o.exchange_id, |o| {
                o.status = St::ExecutionReportStatusFill as i32;
                o.executed = o.lots;
            })
            .expect("exists");
        let trades = pb::OrderTrades {
            order_id: o.exchange_id.clone(),
            direction: o.direction,
            figi: o.inst.figi.into(),
            trades: vec![pb::OrderTrade {
                date_time: ts(1_700_000_100),
                price: Some(quot(o.price)),
                quantity: o.lots,
                trade_id: format!("TR-{}", o.exchange_id),
            }],
            account_id: o.account.clone(),
            instrument_uid: o.inst.uid.into(),
            ..Default::default()
        };
        self.state.publish(&o, Some(trades));
    }

    /// Orders the mock holds for an account (any status).
    pub fn orders_of(&self, account: &str) -> Vec<String> {
        self.state
            .orders
            .lock()
            .unwrap()
            .values()
            .filter(|o| o.account == account)
            .map(|o| o.request_id.clone())
            .collect()
    }
}

impl Drop for MockServer {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}
