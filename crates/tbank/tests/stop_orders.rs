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
    let err = ud.place_stop(req.clone()).await.unwrap_err();
    assert!(matches!(err, Error::Unknown), "{err:?}");
    // The cid stays used: the broker may have the order.
    let err = ud.place_stop(req).await.unwrap_err();
    assert!(
        matches!(err, Error::Rejected { ref code, .. } if code == "duplicate_place"),
        "{err:?}"
    );
    tokio::time::sleep(Duration::from_millis(700)).await;
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
