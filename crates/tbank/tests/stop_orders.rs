//! Stop orders (`StopOrderExecutor`) against the gRPC mock: what the shared scenario does not cover.

mod mock_server;

use brokapi_core::{Decimal, dec};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use brokapi_core::{
    BrokerOrderId, ClientOrderId, Error, InstrumentId, InstrumentKey, NativeId, OrderExecutor,
    PlaceStopOrderRequest, Price, Qty, Side, StopExpiry, StopOrderExecutor, StopOrderKind,
    StopOrderRef, StopOrderState, UnixNanos,
};
use brokapi_runtime::BrokerLimiter;
use brokapi_tbank::limits::default_limits;
use brokapi_tbank::{UserDataConfig, UserDataConnector};
use brokapi_tbank_protocol::TbankConfig;
use mock_server::{ACCOUNT_A, ACCOUNT_B, MockOptions, MockServer, TOKEN};
use tokio::sync::mpsc;

fn sber() -> InstrumentId {
    InstrumentId::new(
        InstrumentKey::new("SBER", "TQBR"),
        NativeId::Uid("uid-sber".into()),
    )
}

async fn ud_for(server: &MockServer, account: &str, timeout: Duration) -> UserDataConnector {
    let (tx, rx) = mpsc::channel(64);
    std::mem::forget(rx);
    let cfg = TbankConfig::for_mock(TOKEN, &server.endpoint)
        .with_account(account)
        .with_request_timeout(timeout);
    UserDataConnector::start(
        UserDataConfig::new(cfg, format!("tbank-ud-{account}")),
        BrokerLimiter::new(&default_limits()),
        tx,
    )
    .await
    .expect("ud")
}

async fn ud(server: &MockServer) -> UserDataConnector {
    let ud = ud_for(server, ACCOUNT_A, Duration::from_secs(5)).await;
    ud.reconcile(&[]).await.unwrap();
    ud
}

fn stop(kind: StopOrderKind, stop: Decimal, limit: Option<Decimal>) -> PlaceStopOrderRequest {
    PlaceStopOrderRequest {
        cid: ClientOrderId::new_v4(),
        instrument: sber(),
        side: Side::Sell,
        kind,
        qty: Qty::from(2),
        stop_price: Price::new(stop),
        limit_price: limit.map(Price::new),
        expiry: StopExpiry::GoodTillCancel,
    }
}

#[tokio::test]
async fn stop_orders_wait_for_reconcile_like_plain_orders() {
    let server = MockServer::start(Arc::default()).await;
    let ud = ud_for(&server, ACCOUNT_A, Duration::from_secs(5)).await;
    let err = ud
        .place_stop(stop(StopOrderKind::StopLoss, dec!(100.0), None))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::NotReconciled), "{err:?}");
    let err = ud
        .cancel_stop(StopOrderRef::Broker(BrokerOrderId::new("x")))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::NotReconciled), "{err:?}");
    assert_eq!(server.opts.stop_post_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn parameters_reach_the_broker_and_come_back() {
    let server = MockServer::start(Arc::default()).await;
    let ud = ud(&server).await;
    let when = UnixNanos::from_nanos(1_900_000_000_000_000_000);
    let mut req = stop(StopOrderKind::TakeProfit, dec!(300.5), Some(dec!(300.0)));
    req.side = Side::Sell;
    req.expiry = StopExpiry::GoodTillDate(when);
    let cid = req.cid;
    ud.place_stop(req).await.unwrap();
    let active = ud.active_stop_orders().await.unwrap();
    assert_eq!(active.len(), 1);
    let s = &active[0];
    assert_eq!(s.cid, Some(cid));
    assert_eq!(s.kind, StopOrderKind::TakeProfit);
    assert_eq!(s.qty.decimal(), dec!(2.0));
    assert_eq!(s.stop_price.decimal(), dec!(300.5));
    assert_eq!(s.limit_price.map(Price::decimal), Some(dec!(300.0)));
    assert_eq!(s.expiry, StopExpiry::GoodTillDate(when));
    assert_eq!(s.state, StopOrderState::Active);
    assert_eq!(s.instrument.key, InstrumentKey::new("SBER", "TQBR"));
}

#[tokio::test]
async fn a_second_account_does_not_see_the_stop_orders() {
    let server = MockServer::start(Arc::default()).await;
    let a = ud(&server).await;
    let b = ud_for(&server, ACCOUNT_B, Duration::from_secs(5)).await;
    b.reconcile(&[]).await.unwrap();
    a.place_stop(stop(StopOrderKind::StopLoss, dec!(1.0), None))
        .await
        .unwrap();
    assert_eq!(a.active_stop_orders().await.unwrap().len(), 1);
    assert!(b.active_stop_orders().await.unwrap().is_empty());
}

#[tokio::test]
async fn cancel_by_unknown_cid_is_refused_without_a_request_and_unknown_broker_id_is_rejected() {
    let server = MockServer::start(Arc::default()).await;
    let ud = ud(&server).await;
    let err = ud
        .cancel_stop(StopOrderRef::Cid(ClientOrderId::new_v4()))
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::Rejected { ref code, .. } if code == "unknown_stop_order"),
        "{err:?}"
    );
    let err = ud
        .cancel_stop(StopOrderRef::Broker(BrokerOrderId::new("SO-none")))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Rejected { .. }), "{err:?}");
}

#[tokio::test]
async fn the_same_cid_is_sent_once() {
    let server = MockServer::start(Arc::default()).await;
    let ud = ud(&server).await;
    let req = stop(StopOrderKind::StopLoss, dec!(1.0), None);
    ud.place_stop(req.clone()).await.unwrap();
    let err = ud.place_stop(req).await.unwrap_err();
    assert!(
        matches!(err, Error::Rejected { ref code, .. } if code == "duplicate_place"),
        "{err:?}"
    );
    assert_eq!(server.opts.stop_post_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_broker_refusal_frees_the_cid_and_an_unknown_one_does_not() {
    let server = MockServer::start(Arc::default()).await;
    let ud = ud(&server).await;
    // Unknown instrument: the broker says NOT_FOUND, the stop order does not exist, the cid is clean.
    let mut req = stop(StopOrderKind::StopLoss, dec!(1.0), None);
    req.instrument = InstrumentId::new(
        InstrumentKey::new("NOPE", "TQBR"),
        NativeId::Uid("uid-nope".into()),
    );
    let err = ud.place_stop(req.clone()).await.unwrap_err();
    assert!(matches!(err, Error::Rejected { .. }), "{err:?}");
    req.instrument = sber();
    ud.place_stop(req)
        .await
        .expect("the same cid goes through after a clean refusal");
}

#[tokio::test]
async fn no_answer_is_unknown_and_never_repeated() {
    let opts: Arc<MockOptions> = Arc::default();
    opts.post_delay_ms.store(600, Ordering::SeqCst);
    let server = MockServer::start(Arc::clone(&opts)).await;
    let ud = ud_for(&server, ACCOUNT_A, Duration::from_millis(150)).await;
    ud.reconcile(&[]).await.unwrap();
    let req = stop(StopOrderKind::StopLoss, dec!(1.0), None);
    let err = brokapi_mocks::time::elapse_when(
        "the stop order reached the mock",
        || opts.stop_post_calls.load(Ordering::SeqCst) == 1,
        Duration::from_millis(200),
        ud.place_stop(req.clone()),
    )
    .await
    .unwrap_err();
    assert!(matches!(err, Error::Unknown), "{err:?}");
    // The cid stays used: the broker may have the order.
    let err = ud.place_stop(req).await.unwrap_err();
    assert!(
        matches!(err, Error::Rejected { ref code, .. } if code == "duplicate_place"),
        "{err:?}"
    );
    // No retry, neither at once nor later: an hour ahead on the clock, then a round trip behind it.
    brokapi_mocks::time::jump(Duration::from_secs(3600)).await;
    ud.reconcile(&[]).await.unwrap();
    assert_eq!(
        opts.stop_post_calls.load(Ordering::SeqCst),
        1,
        "one send, no retry"
    );
}

#[tokio::test]
async fn local_checks_spend_no_quota_and_send_nothing() {
    let server = MockServer::start(Arc::default()).await;
    let ud = ud(&server).await;
    for bad in [
        stop(StopOrderKind::StopLimit, dec!(1.0), None),
        {
            let mut r = stop(StopOrderKind::StopLoss, dec!(1.0), None);
            r.expiry = StopExpiry::EndOfDay;
            r
        },
        {
            let mut r = stop(StopOrderKind::StopLoss, dec!(1.0), None);
            r.qty = Qty::new(dec!(0.5));
            r
        },
    ] {
        assert!(matches!(
            ud.place_stop(bad).await,
            Err(Error::Rejected { .. })
        ));
    }
    assert_eq!(server.opts.stop_post_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn there_is_no_stream_of_stop_orders_at_t_bank() {
    let server = MockServer::start(Arc::default()).await;
    let ud = ud(&server).await;
    assert!(matches!(
        ud.subscribe_stop_orders().await,
        Err(Error::Unsupported { .. })
    ));
}

#[tokio::test]
async fn plain_orders_are_unaffected_by_stop_orders() {
    let server = MockServer::start(Arc::default()).await;
    let ud = ud(&server).await;
    ud.place_stop(stop(StopOrderKind::StopLoss, dec!(1.0), None))
        .await
        .unwrap();
    // A stop order is not an exchange order: the plain list stays empty.
    assert!(ud.active_orders().await.unwrap().is_empty());
}

/// A connector whose `GetStopOrders` bucket is wide enough for a long read in pieces (the default burst is 10).
async fn ud_with_wide_stop_reads(server: &MockServer) -> UserDataConnector {
    let toml = brokapi_tbank::limits::DEFAULT_LIMITS_TOML.replace(
        "rate = 60\nper = \"60s\"\nburst = 10",
        "rate = 60\nper = \"60s\"\nburst = 100",
    );
    assert_ne!(
        toml,
        brokapi_tbank::limits::DEFAULT_LIMITS_TOML,
        "the bucket was found"
    );
    let limits = brokapi_tbank::limits::limits_from_toml_str(&toml).expect("limits");
    let (tx, rx) = mpsc::channel(64);
    std::mem::forget(rx);
    let cfg = TbankConfig::for_mock(TOKEN, &server.endpoint).with_account(ACCOUNT_A);
    let ud = UserDataConnector::start(
        UserDataConfig::new(cfg, "tbank-ud-wide"),
        BrokerLimiter::new(&limits),
        tx,
    )
    .await
    .expect("ud");
    ud.reconcile(&[]).await.unwrap();
    ud
}

#[tokio::test]
async fn a_list_too_long_for_one_answer_is_read_in_pieces() {
    let opts: Arc<MockOptions> = Arc::default();
    // The broker answers 30261 for more than 2 stop orders at once; the stop orders are a day apart.
    opts.stop_response_limit.store(2, Ordering::SeqCst);
    opts.stop_create_step_secs.store(86_400, Ordering::SeqCst);
    let server = MockServer::start(Arc::clone(&opts)).await;
    let ud = ud_with_wide_stop_reads(&server).await;
    let mut placed = Vec::new();
    for _ in 0..5 {
        let req = stop(StopOrderKind::StopLoss, dec!(1.0), None);
        placed.push(req.cid);
        ud.place_stop(req).await.unwrap();
    }

    let active = ud.active_stop_orders().await.unwrap();
    assert_eq!(active.len(), 5, "all of them, none twice");
    let mut cids: Vec<_> = active.iter().filter_map(|s| s.cid).collect();
    cids.sort();
    placed.sort();
    assert_eq!(cids, placed, "the cids still match");

    let seen = opts.stop_get_requests.lock().unwrap().clone();
    assert_eq!(
        seen[0],
        (None, None),
        "the first request asks for the whole list"
    );
    assert!(seen.len() > 2, "then the period was cut: {seen:?}");
    assert!(
        seen[1..].iter().all(|(f, t)| f.is_some() && t.is_some()),
        "every later request has from and to: {seen:?}"
    );
}

#[tokio::test]
async fn a_short_list_is_one_request_without_a_period() {
    let opts: Arc<MockOptions> = Arc::default();
    opts.stop_response_limit.store(10, Ordering::SeqCst);
    let server = MockServer::start(Arc::clone(&opts)).await;
    let ud = ud(&server).await;
    ud.place_stop(stop(StopOrderKind::StopLoss, dec!(1.0), None))
        .await
        .unwrap();
    assert_eq!(ud.active_stop_orders().await.unwrap().len(), 1);
    assert_eq!(*opts.stop_get_requests.lock().unwrap(), vec![(None, None)]);
}

#[tokio::test]
async fn a_list_that_cannot_be_cut_gives_a_clear_refusal() {
    let opts: Arc<MockOptions> = Arc::default();
    // Three stop orders created in the same second: no period separates them.
    opts.stop_response_limit.store(2, Ordering::SeqCst);
    let server = MockServer::start(Arc::clone(&opts)).await;
    let ud = ud_with_wide_stop_reads(&server).await;
    for _ in 0..3 {
        ud.place_stop(stop(StopOrderKind::StopLoss, dec!(1.0), None))
            .await
            .unwrap();
    }
    let err = ud.active_stop_orders().await.unwrap_err();
    match err {
        Error::Rejected { code, message } => {
            assert_eq!(code, "stop_orders_response_limit");
            assert!(message.contains("30261"), "{message}");
        }
        other => panic!("{other:?}"),
    }
    let requests = opts.stop_get_requests.lock().unwrap().len();
    assert!(requests <= 40, "the read stops at its cap: {requests}");
}

#[tokio::test]
async fn the_stop_orders_limit_of_an_instrument_is_a_refusal_not_a_rate_limit() {
    let opts: Arc<MockOptions> = Arc::default();
    let server = MockServer::start(Arc::clone(&opts)).await;
    let ud = ud(&server).await;

    // 80007: a business refusal. A repeat cannot help, so it is not RateLimited.
    *opts.stop_post_fail.lock().unwrap() =
        Some((tonic::Code::ResourceExhausted, "80007".to_string()));
    let req = stop(StopOrderKind::StopLoss, dec!(1.0), None);
    let err = ud.place_stop(req.clone()).await.unwrap_err();
    assert!(
        matches!(err, Error::Rejected { ref code, .. } if code == "stop_orders_limit"),
        "{err:?}"
    );

    // 80002: a request limit. The same status, another number, and now it is a rate limit.
    *opts.stop_post_fail.lock().unwrap() =
        Some((tonic::Code::ResourceExhausted, "80002".to_string()));
    let err = ud
        .place_stop(stop(StopOrderKind::StopLoss, dec!(1.0), None))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::RateLimited { .. }), "{err:?}");

    // The refusal was clean (the broker did not act): the same cid goes through once the limit is gone.
    *opts.stop_post_fail.lock().unwrap() = None;
    ud.place_stop(req)
        .await
        .expect("the cid is free after a refusal");
}
