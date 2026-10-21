//! Wave 2 of the code review of 2026-10-21 for Finam: a rejected token on a command, stop orders that must
//! not show up as ordinary ones, the cid of a foreign fill, the order of a `Resync` and the data before it,
//! a place that the caller drops.

mod mock_server;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use brokapi_core::{
    Backpressure, ClientOrderId, Error, Event, InstrumentId, InstrumentKey, NativeId,
    OrderExecutor, OrderType, PlaceOrderRequest, Price, Qty, ResyncReason, ShutdownMode, Side,
    SubscriptionId,
};
use brokapi_finam::core::ud::UdSubKind;
use brokapi_finam::core::{UdCore, UdEvent, UdFrame};
use brokapi_finam::index::{OrderIndex, foreign_cid};
use brokapi_finam::pump::{PumpIo, run_pump};
use brokapi_finam::rpc::unary;
use brokapi_finam_protocol::pb::grpc::tradeapi::v1;
use brokapi_finam_protocol::pb::grpc::tradeapi::v1::orders as po;
use brokapi_finam_protocol::{FinamError, TokenApi, TokenInfo, TokenManager};
use brokapi_runtime::BackpressureManager;
use brokapi_sansio::{BrokerCore, CoreEvent, EventSink};
use mock_server::{MockServer, sber, ud};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

// ----- UNAUTHENTICATED on a command renews the token, and the command is not repeated -----

struct CountingApi(AtomicU64);

#[async_trait]
impl TokenApi for CountingApi {
    async fn auth(&self, _s: &str, _a: &str) -> Result<String, FinamError> {
        Ok(format!("jwt-{}", self.0.fetch_add(1, Ordering::SeqCst) + 1))
    }
    async fn token_details(&self, _t: &str) -> Result<TokenInfo, FinamError> {
        Ok(TokenInfo {
            ttl: Some(Duration::from_secs(900)),
            ..TokenInfo::default()
        })
    }
}

#[tokio::test]
async fn a_rejected_token_on_a_command_starts_the_renewal_but_does_not_repeat_the_command() {
    let tokens = TokenManager::start(Arc::new(CountingApi(AtomicU64::new(0))), "secret", "")
        .await
        .unwrap();
    assert_eq!(tokens.generation(), 1);
    let calls = Arc::new(AtomicU64::new(0));
    let counted = Arc::clone(&calls);
    let call = move || {
        let counted = Arc::clone(&counted);
        async move {
            counted.fetch_add(1, Ordering::SeqCst);
            Err::<tonic::Response<()>, _>(tonic::Status::unauthenticated("expired"))
        }
    };
    // `retry_unauth = false` is a command.
    let err = unary(&tokens, Duration::from_secs(5), false, call)
        .await
        .unwrap_err();
    assert!(err.is_unauthenticated());
    assert_eq!(calls.load(Ordering::SeqCst), 1, "the command went out once");
    // The renewal runs in the background; the next call meets a fresh token.
    brokapi_mocks::time::wait_until("the token was renewed", || tokens.generation() == 2).await;
    assert_eq!(&*tokens.current(), "jwt-2");
}

// ----- stop orders and the stream of ordinary orders -----

fn frame(sub: SubscriptionId, event: UdEvent) -> UdFrame {
    UdFrame { sub, event }
}

fn orders_frame(sub: SubscriptionId, orders: Vec<po::OrderState>) -> UdFrame {
    frame(sub, UdEvent::Orders(po::SubscribeOrdersResponse { orders }))
}

fn plain(order_id: &str, comment: &str) -> po::OrderState {
    po::OrderState {
        order_id: order_id.into(),
        status: po::OrderStatus::New as i32,
        order: Some(po::Order {
            symbol: "SBER@MISX".into(),
            r#type: po::OrderType::Limit as i32,
            comment: comment.into(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn stop_limit(order_id: &str) -> po::OrderState {
    po::OrderState {
        order_id: order_id.into(),
        status: po::OrderStatus::Watching as i32,
        order: Some(po::Order {
            symbol: "SBER@MISX".into(),
            r#type: po::OrderType::StopLimit as i32,
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn sltp(order_id: &str) -> po::OrderState {
    po::OrderState {
        order_id: order_id.into(),
        status: po::OrderStatus::Watching as i32,
        sltp_order: Some(po::SltpOrder {
            symbol: "SBER@MISX".into(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[test]
fn stop_and_sltp_orders_do_not_show_up_in_the_stream_of_ordinary_orders() {
    let sub = SubscriptionId::new(1);
    let mut core = UdCore::new(Arc::new(OrderIndex::new()));
    core.register(sub, UdSubKind::Orders);
    let mut out = EventSink::new();
    core.on_frame(
        orders_frame(sub, vec![stop_limit("S1"), sltp("S2"), plain("P1", "")]),
        Instant::now(),
        &mut out,
    );
    let orders: Vec<_> = out
        .drain()
        .into_iter()
        .map(|e| match e.payload {
            Event::Order(o) => o,
            other => panic!("{other:?}"),
        })
        .collect();
    // Only the plain order: with its instrument, side and quantity, not an empty shell.
    assert_eq!(orders.len(), 1);
    assert_eq!(orders[0].broker_id.as_ref().unwrap().as_str(), "P1");
    assert_eq!(orders[0].instrument.key.symbol, "SBER");
}

// ----- the cid of a fill of an order that is not ours -----

fn trade(order_id: &str, comment: &str) -> v1::AccountTrade {
    v1::AccountTrade {
        trade_id: format!("T-{order_id}"),
        order_id: order_id.into(),
        symbol: "SBER@MISX".into(),
        comment: comment.into(),
        ..Default::default()
    }
}

fn trades_frame(sub: SubscriptionId, trades: Vec<v1::AccountTrade>) -> UdFrame {
    frame(sub, UdEvent::Trades(po::SubscribeTradesResponse { trades }))
}

#[test]
fn a_fill_of_a_foreign_order_keeps_no_cid_even_after_the_order_was_seen() {
    let (orders, fills) = (SubscriptionId::new(1), SubscriptionId::new(2));
    let index = Arc::new(OrderIndex::new());
    let mut core = UdCore::new(Arc::clone(&index));
    core.register(orders, UdSubKind::Orders);
    core.register(fills, UdSubKind::Executions);
    let now = Instant::now();
    let mut out = EventSink::new();

    // The order echo of an order placed by hand comes first (the stream, or a reconcile).
    core.on_frame(
        orders_frame(orders, vec![plain("HAND-1", "typed by hand")]),
        now,
        &mut out,
    );
    let echoed = out.drain();
    let Event::Order(snapshot) = &echoed[0].payload else {
        panic!()
    };
    assert_eq!(snapshot.cid, foreign_cid("HAND-1"));
    // Its fill has no cid: the made-up one of the echo is not handed to the user as a fill's cid.
    core.on_frame(
        trades_frame(fills, vec![trade("HAND-1", "typed by hand")]),
        now,
        &mut out,
    );
    let Event::Execution(fill) = &out.drain()[0].payload.clone() else {
        panic!()
    };
    assert_eq!(fill.cid, None);
    // The made-up cid still finds the order, so the order can be cancelled by it.
    assert_eq!(index.order_id(snapshot.cid).as_deref(), Some("HAND-1"));

    // An order of ours: bound by the place, its fill carries the cid.
    let ours = ClientOrderId::new_v4();
    index.bind(ours, "OURS-1");
    core.on_frame(
        trades_frame(fills, vec![trade("OURS-1", "")]),
        now,
        &mut out,
    );
    let Event::Execution(fill) = &out.drain()[0].payload.clone() else {
        panic!()
    };
    assert_eq!(fill.cid, Some(ours));
}

#[test]
fn the_index_forgets_a_finished_order_with_its_tag() {
    let index = OrderIndex::new();
    let cid = ClientOrderId::new_v4();
    index.know(cid);
    index.bind(cid, "O1");
    assert_eq!(index.order_id(cid).as_deref(), Some("O1"));
    assert_eq!(index.lookup("O1", ""), Some(cid));
    index.forget(cid);
    assert_eq!(index.order_id(cid), None);
    assert_eq!(index.lookup("O1", ""), None);
    // Forgetting what is not there is fine.
    index.forget(ClientOrderId::new_v4());
    // An echo with an empty order id is not remembered at all.
    let foreign = index.resolve("", "", "");
    assert_eq!(index.order_id(foreign), None);
}

#[test]
fn orders_that_finished_long_ago_are_forgotten_by_the_core() {
    let sub = SubscriptionId::new(1);
    let index = Arc::new(OrderIndex::new());
    let mut core = UdCore::new(Arc::clone(&index));
    core.register(sub, UdSubKind::Orders);
    let mut out = EventSink::new();
    let t0 = Instant::now();
    let mut done = plain("D1", "");
    done.status = po::OrderStatus::Filled as i32;
    core.on_frame(orders_frame(sub, vec![done]), t0, &mut out);
    let first = foreign_cid("D1");
    assert!(index.order_id(first).is_some());
    assert!(core.take_forgotten().is_empty());

    let later = t0 + brokapi_runtime::RETENTION + Duration::from_secs(1);
    let mut other = plain("D2", "");
    other.status = po::OrderStatus::Filled as i32;
    core.on_frame(orders_frame(sub, vec![other]), later, &mut out);
    assert_eq!(core.take_forgotten(), vec![first]);
    assert_eq!(index.order_id(first), None);
}

// ----- the data in the buffer goes out before the Resync that follows it -----

#[tokio::test]
async fn a_resync_does_not_overtake_the_data_that_is_already_buffered() {
    let sub = SubscriptionId::new(1);
    let backpressure = Arc::new(Mutex::new(BackpressureManager::new()));
    backpressure.lock().unwrap().register(
        sub,
        InstrumentId::new(
            InstrumentKey::new("SBER", "MISX"),
            NativeId::SymbolMic {
                symbol: "SBER".into(),
                mic: "MISX".into(),
            },
        ),
        Backpressure::Lossless { cap: 16 },
    );
    let (frames, rx) = mpsc::channel::<u8>(8);
    // A slow consumer: room for one envelope.
    let (out, mut envelopes) = mpsc::channel(1);
    let cancel = CancellationToken::new();
    let io = PumpIo {
        rx,
        out,
        source: Arc::from("finam-md"),
        backpressure,
        cancel: cancel.clone(),
    };
    let handler = move |_frame: u8, _now: Instant| {
        let mut events: Vec<CoreEvent> = (0..3)
            .map(|_| CoreEvent {
                sub: Some(sub),
                payload: Event::Positions(vec![]),
            })
            .collect();
        events.push(CoreEvent {
            sub: Some(sub),
            payload: Event::Resync {
                sub,
                instrument: brokapi_finam::util::instrument_from_symbol("SBER@MISX"),
                reason: ResyncReason::SubscriptionReset,
            },
        });
        events
    };
    let pump = tokio::spawn(run_pump(io, handler, Arc::new(|_| {})));
    frames.send(1).await.unwrap();

    let mut kinds = Vec::new();
    for _ in 0..4 {
        let env = tokio::time::timeout(Duration::from_secs(5), envelopes.recv())
            .await
            .unwrap()
            .unwrap();
        kinds.push(matches!(env.payload, Event::Resync { .. }));
    }
    // Three pieces of data, then the Resync: the consumer drops its state last, not first.
    assert_eq!(kinds, vec![false, false, false, true]);
    cancel.cancel();
    pump.await.unwrap();
}

// ----- a place that the caller drops -----

#[tokio::test]
async fn a_place_whose_caller_gives_up_still_ends_and_leaves_nothing_in_flight() {
    let server = MockServer::start().await;
    let (ud, _rx) = ud(&server, "ACC1").await;
    ud.reconcile(&[]).await.unwrap();
    server
        .state
        .options
        .place_delay_ms
        .store(900, Ordering::SeqCst);
    assert_eq!(ud.in_flight_orders(), 0);

    let req = PlaceOrderRequest {
        cid: ClientOrderId::new_v4(),
        instrument: sber(),
        side: Side::Buy,
        order_type: OrderType::Limit,
        qty: Qty::from(1),
        price: Some(Price::from(1)),
    };
    {
        let place = ud.place(req.clone());
        tokio::pin!(place);
        tokio::select! {
            biased;
            _ = &mut place => panic!("the answer is held back, the place cannot be done yet"),
            _ = brokapi_mocks::time::wait_until("the order reached the mock", || {
                server.state.count_calls("PlaceOrder") == 1
            }) => {}
        }
        assert_eq!(ud.in_flight_orders(), 1);
        // The caller gives up here: the future is dropped.
    }
    brokapi_mocks::time::jump_until("the order is settled", Duration::from_secs(1), || {
        ud.in_flight_orders() == 0
    })
    .await;
    // The order is known with its broker id: a cancel by cid needs no list of orders.
    let lists = server.state.count_calls("GetOrders");
    ud.cancel(req.cid).await.unwrap();
    assert_eq!(server.state.count_calls("GetOrders"), lists);
    let again = ud.place(req).await.unwrap_err();
    assert!(
        matches!(again, Error::Rejected { ref code, .. } if code == "duplicate_place"),
        "{again:?}"
    );
    ud.shutdown(ShutdownMode::Immediate).await;
}

// ----- the URL of the endpoint in the environment -----

#[test]
fn the_api_url_of_the_environment_must_be_https() {
    unsafe {
        std::env::set_var("FINAM_SECRET", "s");
        std::env::set_var("FINAM_API_URL", "http://evil.test:80");
    }
    let err = brokapi_finam::api_config_from_env().expect_err("refused");
    assert!(err.to_string().contains("https"), "{err}");
    unsafe { std::env::set_var("FINAM_API_URL", "https://api.example.test:443") };
    let (cfg, _) = brokapi_finam::api_config_from_env().unwrap();
    assert_eq!(cfg.endpoint.url, "https://api.example.test:443");
    unsafe {
        std::env::remove_var("FINAM_API_URL");
        std::env::remove_var("FINAM_SECRET");
    }
}
