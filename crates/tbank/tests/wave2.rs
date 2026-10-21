//! Wave 2 of the code review of 2026-10-21 for T-Bank: the history of trades of every kind, a trade that is
//! faster than the answer to its order, streams that are refused for good, stop orders, a place that the
//! caller drops, the certificate of the endpoint.

mod mock_server;

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use brokapi_core::{
    ClientOrderId, Envelope, Error, Event, ExecutionSource, InstrumentId, InstrumentKey, NativeId,
    OrderExecutor, OrderType, PlaceOrderRequest, Price, Qty, ShutdownMode, Side, TimeRange,
    UnixNanos,
};
use brokapi_runtime::BrokerLimiter;
use brokapi_sansio::{BrokerCore, EventSink};
use brokapi_tbank::core::{UdCore, UdFrame};
use brokapi_tbank::limits::default_limits;
use brokapi_tbank::{UserDataConfig, UserDataConnector};
use brokapi_tbank_protocol::v1 as pb;
use brokapi_tbank_protocol::{ProtocolError, TbankClient, TbankConfig};
use mock_server::{ACCOUNT_A, MockServer, TOKEN};
use tokio::sync::mpsc;

fn sber() -> InstrumentId {
    InstrumentId::new(
        InstrumentKey::new("SBER", "TQBR"),
        NativeId::Uid("uid-sber".into()),
    )
}

fn buy(qty: i64) -> PlaceOrderRequest {
    PlaceOrderRequest {
        cid: ClientOrderId::new_v4(),
        instrument: sber(),
        side: Side::Buy,
        order_type: OrderType::Limit,
        qty: Qty::from(qty),
        price: Some(Price::from(250)),
    }
}

async fn ud(server: &MockServer) -> (UserDataConnector, mpsc::Receiver<Envelope>) {
    let (tx, rx) = mpsc::channel(512);
    let cfg = TbankConfig::for_mock(TOKEN, &server.endpoint)
        .with_account(ACCOUNT_A)
        .with_request_timeout(Duration::from_secs(5));
    let ud = UserDataConnector::start(
        UserDataConfig::new(cfg, "tbank-ud"),
        BrokerLimiter::new(&default_limits()),
        tx,
    )
    .await
    .expect("ud");
    (ud, rx)
}

// ----- the history asks for every kind of trade -----

#[tokio::test]
async fn the_history_of_trades_asks_for_margin_and_card_trades_too() {
    let server = MockServer::start(Default::default()).await;
    let (ud, _rx) = ud(&server).await;
    let range = TimeRange::new(UnixNanos::ZERO, UnixNanos::from_nanos(i64::MAX / 2));
    let history = ud.executions(range).await.unwrap();
    assert!(!history.is_empty());
    let asked = server.opts.operation_types.lock().unwrap().clone();
    let types = asked.last().expect("one request");
    for wanted in [
        pb::OperationType::Buy,
        pb::OperationType::Sell,
        pb::OperationType::BuyMargin,
        pb::OperationType::SellMargin,
        pb::OperationType::BuyCard,
        pb::OperationType::SellCard,
    ] {
        assert!(
            types.contains(&(wanted as i32)),
            "{wanted:?} is not asked for: {types:?}"
        );
    }
    ud.shutdown(ShutdownMode::Immediate).await;
}

// ----- a trade that is faster than the answer to the order -----

fn trades_frame(order_id: &str) -> UdFrame {
    UdFrame::Trades(pb::TradesStreamResponse {
        payload: Some(pb::trades_stream_response::Payload::OrderTrades(
            pb::OrderTrades {
                order_id: order_id.into(),
                account_id: "acc".into(),
                instrument_uid: "uid-sber".into(),
                figi: String::new(),
                direction: pb::OrderDirection::Buy as i32,
                trades: vec![pb::OrderTrade {
                    price: Some(brokapi_tbank_protocol::money::quotation("250")),
                    quantity: 10,
                    trade_id: "trade-1".into(),
                    ..Default::default()
                }],
                ..Default::default()
            },
        )),
    })
}

fn post_response(order_id: &str, cid: ClientOrderId) -> pb::PostOrderResponse {
    pb::PostOrderResponse {
        order_id: order_id.into(),
        order_request_id: cid.hyphenated().to_string(),
        execution_report_status: pb::OrderExecutionReportStatus::ExecutionReportStatusFill as i32,
        lots_requested: 1,
        lots_executed: 1,
        instrument_uid: "uid-sber".into(),
        direction: pb::OrderDirection::Buy as i32,
        ..Default::default()
    }
}

fn executions(out: &mut EventSink) -> Vec<brokapi_core::Execution> {
    out.drain()
        .into_iter()
        .filter_map(|e| match e.payload {
            Event::Execution(x) => Some(x),
            _ => None,
        })
        .collect()
}

fn core_with_subs() -> UdCore {
    let mut core = UdCore::new("acc");
    core.add_sub(
        brokapi_core::SubscriptionId::new(1),
        brokapi_tbank::core::ud::UdSubKind::Executions,
    );
    core.lots().put("uid-sber", "", "SBER", "TQBR", 10);
    core
}

#[test]
fn a_trade_before_the_answer_gets_its_cid_when_the_answer_comes() {
    let mut core = core_with_subs();
    let now = Instant::now();
    let req = buy(1);
    core.begin_place(&req, now).unwrap();

    // The trade arrives while the placement is in flight: it waits.
    let mut out = EventSink::new();
    core.on_frame(trades_frame("X1"), now, &mut out);
    assert!(
        executions(&mut out).is_empty(),
        "held back until the order is known"
    );

    // The answer to PostOrder gives the exchange id: the trade goes out with the cid.
    let mut out = EventSink::new();
    core.place_response(req.cid, &post_response("X1", req.cid), now, &mut out);
    let sent = executions(&mut out);
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].cid, Some(req.cid));
    assert_eq!(sent[0].trade_id, "trade-1");
}

#[test]
fn a_trade_of_a_foreign_order_is_released_without_a_cid_once_nothing_is_in_flight() {
    let mut core = core_with_subs();
    let now = Instant::now();
    let req = buy(1);
    core.begin_place(&req, now).unwrap();
    let mut out = EventSink::new();
    core.on_frame(trades_frame("FOREIGN"), now, &mut out);
    assert!(executions(&mut out).is_empty());
    // Our placement ends with another order id: nobody claims the waiting trade.
    let mut out = EventSink::new();
    core.place_response(req.cid, &post_response("OURS", req.cid), now, &mut out);
    let sent = executions(&mut out);
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].cid, None);

    // With nothing in flight a trade goes out at once, with no cid for an unknown order.
    let mut out = EventSink::new();
    core.on_frame(trades_frame("FOREIGN-2"), now, &mut out);
    let sent = executions(&mut out);
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].cid, None);
}

#[test]
fn waiting_trades_are_released_when_the_placement_fails_or_is_given_up() {
    let now = Instant::now();
    // A refused placement.
    let mut core = core_with_subs();
    let req = buy(1);
    core.begin_place(&req, now).unwrap();
    let mut out = EventSink::new();
    core.on_frame(trades_frame("X1"), now, &mut out);
    core.place_rejected(req.cid, "no".into(), now, &mut out);
    assert_eq!(executions(&mut out).len(), 1);

    // An unknown outcome.
    let mut core = core_with_subs();
    let req = buy(1);
    core.begin_place(&req, now).unwrap();
    let mut out = EventSink::new();
    core.on_frame(trades_frame("X1"), now, &mut out);
    core.place_unknown(req.cid, now, &mut out);
    assert_eq!(executions(&mut out).len(), 1);

    // A placement that waited too long (the safety net) and one given up at shutdown.
    let mut core = core_with_subs();
    core.set_command_timeout(Duration::from_secs(10));
    let req = buy(1);
    core.begin_place(&req, now).unwrap();
    let mut out = EventSink::new();
    core.on_frame(trades_frame("X1"), now, &mut out);
    core.sweep_timeouts(now + Duration::from_secs(5), &mut out);
    assert!(executions(&mut out).is_empty(), "not yet");
    core.sweep_timeouts(now + Duration::from_secs(11), &mut out);
    assert_eq!(executions(&mut out).len(), 1);

    let mut core = core_with_subs();
    let req = buy(1);
    core.begin_place(&req, now).unwrap();
    let mut out = EventSink::new();
    core.on_frame(trades_frame("X1"), now, &mut out);
    assert_eq!(core.abandon_inflight(now, &mut out), vec![req.cid]);
    assert_eq!(executions(&mut out).len(), 1);
}

#[test]
fn the_ids_of_finished_orders_are_forgotten_after_the_window() {
    let mut core = UdCore::new("acc");
    let now = Instant::now();
    let req = buy(1);
    core.begin_place(&req, now).unwrap();
    let mut out = EventSink::new();
    core.place_response(req.cid, &post_response("X1", req.cid), now, &mut out);
    assert_eq!(core.exchange_id(req.cid), Some("X1"));
    assert!(core.take_forgotten().is_empty());

    // A day later another order finishes: the first one is past the window and goes.
    let later = now + brokapi_runtime::RETENTION + Duration::from_secs(1);
    let other = buy(1);
    core.begin_place(&other, later).unwrap();
    core.place_response(other.cid, &post_response("X2", other.cid), later, &mut out);
    assert_eq!(core.take_forgotten(), vec![req.cid]);
    assert_eq!(core.exchange_id(req.cid), None);
    assert_eq!(core.exchange_id(other.cid), Some("X2"));
}

// ----- a stream that the broker refuses for good -----

#[tokio::test]
async fn a_rejected_token_on_a_stream_fails_the_subscription_at_once() {
    let server = MockServer::start(Default::default()).await;
    let (ud, _rx) = ud(&server).await;
    *server.opts.user_stream_refuse.lock().unwrap() =
        Some((tonic::Code::Unauthenticated, "40003".into()));
    let started = Instant::now();
    let err = ud.subscribe_order_state().await.unwrap_err();
    match err {
        Error::Rejected { code, .. } => assert!(code.contains("unauthenticated"), "{code}"),
        other => panic!("{other:?}"),
    }
    // At once, not after the request timeout of 5 s, and without a loop of reopens.
    assert!(started.elapsed() < Duration::from_secs(4));
    assert_eq!(server.opts.user_stream_opens.load(Ordering::SeqCst), 0);
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn the_limit_of_open_streams_fails_the_subscription_and_a_later_one_can_try_again() {
    let server = MockServer::start(Default::default()).await;
    let (ud, _rx) = ud(&server).await;
    *server.opts.user_stream_refuse.lock().unwrap() =
        Some((tonic::Code::ResourceExhausted, "80001".into()));
    let err = ud.subscribe_order_state().await.unwrap_err();
    assert!(
        matches!(&err, Error::Rejected { code, .. } if code.starts_with("streams_limit")),
        "{err:?}"
    );
    // The limit is lifted: the same call works, a new stream is opened.
    *server.opts.user_stream_refuse.lock().unwrap() = None;
    ud.subscribe_order_state().await.unwrap();
    assert!(server.opts.user_stream_opens.load(Ordering::SeqCst) >= 1);
    ud.shutdown(ShutdownMode::Immediate).await;
}

// ----- a place that the caller drops -----

#[tokio::test]
async fn a_place_whose_caller_gives_up_still_ends_and_leaves_nothing_in_flight() {
    let server = MockServer::start(Default::default()).await;
    let (ud, _rx) = ud(&server).await;
    ud.reconcile(&[]).await.unwrap();
    server.opts.post_delay_ms.store(900, Ordering::SeqCst);

    let req = buy(10);
    {
        let place = ud.place(req.clone());
        tokio::pin!(place);
        tokio::select! {
            biased;
            r = &mut place => panic!("the answer is held back, the place cannot be done yet: {r:?}"),
            _ = brokapi_mocks::time::wait_until("the order reached the mock", || {
                server.opts.post_calls.load(Ordering::SeqCst) == 1
            }) => {}
        }
        assert_eq!(ud.in_flight_orders(), 1);
        // The caller gives up here: the future is dropped.
    }
    // The mock finishes the request; the order is settled although nobody waits for it.
    brokapi_mocks::time::jump_until("the order is settled", Duration::from_secs(1), || {
        ud.in_flight_orders() == 0
    })
    .await;
    // And the cid is used up, as for any place.
    let again = ud.place(req).await.unwrap_err();
    assert!(
        matches!(again, Error::Rejected { ref code, .. } if code == "duplicate_place"),
        "{again:?}"
    );
    ud.shutdown(ShutdownMode::Immediate).await;
}

// ----- the certificate of the endpoint -----

const SOME_PEM: &[u8] =
    b"-----BEGIN CERTIFICATE-----\nMIIBszCCAVmgAwIBAgIUZm9v\n-----END CERTIFICATE-----\n";

#[tokio::test]
async fn an_extra_root_certificate_is_accepted_only_when_it_is_a_pem() {
    let cfg = TbankConfig::production("t.token").with_root_certificate_pem(SOME_PEM.to_vec());
    assert!(cfg.root_certificate_pem.is_some());
    // The Debug output says there is one, and still hides the token.
    let shown = format!("{cfg:?}");
    assert!(
        shown.contains("root_certificate: true") && !shown.contains("t.token"),
        "{shown}"
    );
    assert!(TbankClient::new(&cfg).is_ok());

    let bad =
        TbankConfig::production("t.token").with_root_certificate_pem(b"not a certificate".to_vec());
    assert!(matches!(
        TbankClient::new(&bad),
        Err(ProtocolError::Config(_))
    ));

    // The default has none.
    assert!(
        TbankConfig::production("t.token")
            .root_certificate_pem
            .is_none()
    );
    // A plain-text mock endpoint ignores it.
    let mock = TbankConfig::for_mock("t.token", "http://127.0.0.1:1")
        .with_root_certificate_pem(SOME_PEM.to_vec());
    assert!(TbankClient::new(&mock).is_ok());
}

// ----- the config from the environment -----

#[test]
fn a_plain_text_endpoint_in_the_environment_is_refused_and_the_file_is_read_from_its_path() {
    let dir = std::env::temp_dir().join(format!("brokapi-tbank-env-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let file = dir.join("t.env");
    std::fs::write(
        &file,
        "TBANK_TOKEN=t.from-file\nTBANK_SANDBOX=yes\nTBANK_ENDPOINT=https://example.test:443\nTBANK_ACCOUNT_ID=acc-1\n",
    )
    .unwrap();
    unsafe {
        for k in [
            "TBANK_TOKEN",
            "TBANK_SANDBOX",
            "TBANK_ENDPOINT",
            "TBANK_ACCOUNT_ID",
            "TBANK_APP_NAME",
        ] {
            std::env::remove_var(k);
        }
    }
    let cfg = TbankConfig::from_env_file(&file).unwrap();
    assert_eq!(cfg.token, "t.from-file");
    assert_eq!(cfg.endpoint, "https://example.test:443");
    assert_eq!(cfg.account_id.as_deref(), Some("acc-1"));
    assert!(cfg.sandbox);

    unsafe { std::env::set_var("TBANK_ENDPOINT", "http://insecure.test:80") };
    let err = TbankConfig::from_env_file(dir.join("absent.env")).unwrap_err();
    assert!(
        matches!(err, ProtocolError::Config(ref m) if m.contains("https")),
        "{err:?}"
    );
    unsafe {
        std::env::remove_var("TBANK_ENDPOINT");
        std::env::remove_var("TBANK_TOKEN");
    }
    assert!(TbankConfig::from_env_file(dir.join("absent.env")).is_err());
    let _ = std::fs::remove_dir_all(&dir);
}
