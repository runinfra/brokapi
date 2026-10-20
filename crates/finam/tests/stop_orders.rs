//! Stop orders (`StopOrderExecutor`) against the gRPC mock: what the shared scenario does not cover.

mod mock_server;

use brokapi_core::{
    BrokerOrderId, ClientOrderId, Error, OrderExecutor, PlaceStopOrderRequest, Price, Qty,
    ReconcileReport, Side, StopExpiry, StopOrderExecutor, StopOrderKind, StopOrderRef,
    StopOrderState, UnixNanos,
};
use brokapi_core::{Decimal, dec};
use mock_server::{MockServer, sber, ud};

fn stop(
    side: Side,
    kind: StopOrderKind,
    stop: Decimal,
    limit: Option<Decimal>,
) -> PlaceStopOrderRequest {
    PlaceStopOrderRequest {
        cid: ClientOrderId::new_v4(),
        instrument: sber(),
        side,
        kind,
        qty: Qty::from(5),
        stop_price: Price::new(stop),
        limit_price: limit.map(Price::new),
        expiry: StopExpiry::GoodTillCancel,
    }
}

#[tokio::test]
async fn stop_orders_wait_for_reconcile_like_plain_orders() {
    let server = MockServer::start().await;
    let (ud, _rx) = ud(&server, "ACC1").await;
    let err = ud
        .place_stop(stop(Side::Sell, StopOrderKind::StopLoss, dec!(100.0), None))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::NotReconciled), "{err:?}");
    let err = ud
        .cancel_stop(StopOrderRef::Broker(BrokerOrderId::new("x")))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::NotReconciled), "{err:?}");
    assert_eq!(server.state.count_calls("PlaceOrder"), 0);
}

#[tokio::test]
async fn stop_loss_and_stop_limit_use_place_order_with_the_right_trigger() {
    let server = MockServer::start().await;
    let (ud, _rx) = ud(&server, "ACC1").await;
    ud.reconcile(&[]).await.unwrap();
    ud.place_stop(stop(Side::Sell, StopOrderKind::StopLoss, dec!(100.0), None))
        .await
        .unwrap();
    ud.place_stop(stop(
        Side::Buy,
        StopOrderKind::StopLimit,
        dec!(400.0),
        Some(dec!(401.0)),
    ))
    .await
    .unwrap();
    assert_eq!(server.state.count_calls("PlaceOrder:ACC1"), 2);
    assert_eq!(server.state.count_calls("PlaceSLTPOrder"), 0);

    let orders = server.state.orders_of("ACC1");
    use brokapi_finam_protocol::pb::grpc::tradeapi::v1::orders as po;
    let sell = orders[0].order.as_ref().unwrap();
    assert_eq!(sell.r#type, po::OrderType::Stop as i32);
    assert_eq!(
        sell.stop_condition,
        po::StopCondition::LastDown as i32,
        "a sell stop-loss triggers on a fall"
    );
    let buy = orders[1].order.as_ref().unwrap();
    assert_eq!(buy.r#type, po::OrderType::StopLimit as i32);
    assert_eq!(
        buy.stop_condition,
        po::StopCondition::LastUp as i32,
        "a buy stop-limit triggers on a rise"
    );
    assert_eq!(buy.limit_price.as_ref().unwrap().value, "401.0");

    let active = ud.active_stop_orders().await.unwrap();
    assert_eq!(active.len(), 2);
    let kinds: Vec<_> = active.iter().map(|s| s.kind).collect();
    assert!(
        kinds.contains(&StopOrderKind::StopLoss) && kinds.contains(&StopOrderKind::StopLimit),
        "{kinds:?}"
    );
    assert!(
        active
            .iter()
            .all(|s| s.cid.is_some() && s.state == StopOrderState::Active)
    );
}

#[tokio::test]
async fn take_profit_uses_the_sltp_call_and_comes_back_by_its_comment() {
    let server = MockServer::start().await;
    let (ud, _rx) = ud(&server, "ACC1").await;
    ud.reconcile(&[]).await.unwrap();
    let when = UnixNanos::from_nanos(1_900_000_000_000_000_000);
    let mut req = stop(Side::Sell, StopOrderKind::TakeProfit, dec!(500.0), None);
    req.expiry = StopExpiry::GoodTillDate(when);
    let cid = req.cid;
    let ack = ud.place_stop(req).await.unwrap();
    assert_eq!(server.state.count_calls("PlaceSLTPOrder:ACC1"), 1);
    assert_eq!(server.state.count_calls("PlaceOrder"), 0);

    let active = ud.active_stop_orders().await.unwrap();
    assert_eq!(active.len(), 1);
    let s = &active[0];
    assert_eq!(s.broker_id, ack.broker_id);
    assert_eq!(
        s.cid,
        Some(cid),
        "the cid is read back from the SL/TP comment"
    );
    assert_eq!(s.kind, StopOrderKind::TakeProfit);
    assert_eq!(s.stop_price.decimal(), dec!(500.0));
    assert_eq!(s.qty.decimal(), dec!(5.0));
    assert_eq!(s.expiry, StopExpiry::GoodTillDate(when));
    ud.cancel_stop(StopOrderRef::Cid(cid)).await.unwrap();
    assert!(ud.active_stop_orders().await.unwrap().is_empty());
}

#[tokio::test]
async fn a_stop_order_of_someone_else_has_no_cid() {
    let server = MockServer::start().await;
    let (ud, _rx) = ud(&server, "ACC1").await;
    ud.reconcile(&[]).await.unwrap();
    use brokapi_finam_protocol::pb::grpc::tradeapi::v1::orders as po;
    server.state.push_order(
        "ACC1",
        po::OrderState {
            order_id: "FOREIGN1".into(),
            status: po::OrderStatus::Watching as i32,
            order: Some(po::Order {
                account_id: "ACC1".into(),
                symbol: "SBER@MISX".into(),
                quantity: mock_server::server_d("3"),
                side: brokapi_finam_protocol::pb::grpc::tradeapi::v1::Side::Sell as i32,
                r#type: po::OrderType::Stop as i32,
                stop_price: mock_server::server_d("90"),
                stop_condition: po::StopCondition::LastDown as i32,
                comment: "set by hand".into(),
                ..Default::default()
            }),
            ..Default::default()
        },
    );
    let active = ud.active_stop_orders().await.unwrap();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].cid, None);
    assert_eq!(active[0].broker_id.as_str(), "FOREIGN1");
    // It can still be cancelled by the broker id.
    ud.cancel_stop(StopOrderRef::Broker(active[0].broker_id.clone()))
        .await
        .unwrap();
}

#[tokio::test]
async fn plain_lists_and_reconcile_do_not_show_stop_orders() {
    let server = MockServer::start().await;
    let (ud, _rx) = ud(&server, "ACC1").await;
    ud.reconcile(&[]).await.unwrap();
    ud.place_stop(stop(Side::Sell, StopOrderKind::StopLoss, dec!(100.0), None))
        .await
        .unwrap();
    assert!(ud.active_orders().await.unwrap().is_empty());
    let ReconcileReport {
        confirmed,
        closed_offline,
        foreign,
    } = ud.reconcile(&[]).await.unwrap();
    assert!(
        confirmed.is_empty() && closed_offline.is_empty() && foreign.is_empty(),
        "stops are not plain orders"
    );
}

#[tokio::test]
async fn the_same_cid_is_sent_once_and_local_checks_send_nothing() {
    let server = MockServer::start().await;
    let (ud, _rx) = ud(&server, "ACC1").await;
    ud.reconcile(&[]).await.unwrap();
    let req = stop(Side::Sell, StopOrderKind::StopLoss, dec!(100.0), None);
    ud.place_stop(req.clone()).await.unwrap();
    let err = ud.place_stop(req).await.unwrap_err();
    assert!(
        matches!(err, Error::Rejected { ref code, .. } if code == "duplicate_place"),
        "{err:?}"
    );
    assert_eq!(server.state.count_calls("PlaceOrder"), 1);

    let mut bad = stop(Side::Sell, StopOrderKind::StopLoss, dec!(100.0), None);
    bad.expiry = StopExpiry::GoodTillDate(UnixNanos::from_nanos(1_900_000_000_000_000_000));
    assert!(matches!(
        ud.place_stop(bad).await,
        Err(Error::Rejected { .. })
    ));
    assert!(matches!(
        ud.place_stop(stop(
            Side::Sell,
            StopOrderKind::TakeProfit,
            dec!(100.0),
            Some(dec!(99.0))
        ))
        .await,
        Err(Error::Rejected { .. })
    ));
    assert_eq!(server.state.count_calls("PlaceOrder"), 1);
    assert_eq!(server.state.count_calls("PlaceSLTPOrder"), 0);
}

#[tokio::test]
async fn a_refused_stop_frees_the_cid() {
    let server = MockServer::start().await;
    let (ud, _rx) = ud(&server, "ACC1").await;
    ud.reconcile(&[]).await.unwrap();
    *server.state.options.place_error.lock().unwrap() = Some(tonic::Code::InvalidArgument);
    let req = stop(Side::Sell, StopOrderKind::StopLoss, dec!(100.0), None);
    assert!(matches!(
        ud.place_stop(req.clone()).await,
        Err(Error::Rejected { .. })
    ));
    ud.place_stop(req)
        .await
        .expect("the same cid after a clean refusal");
}

#[tokio::test]
async fn no_answer_is_unknown() {
    let server = MockServer::start().await;
    let (ud, _rx) = mock_server::ud_with(
        brokapi_finam::UserDataConfig::new(
            {
                let mut api = mock_server::api(&server);
                api.request_timeout = std::time::Duration::from_millis(150);
                api
            },
            "ACC1",
            "finam-ud-short",
        ),
        mock_server::limiter(),
        64,
    )
    .await;
    ud.reconcile(&[]).await.unwrap();
    server
        .state
        .options
        .place_delay_ms
        .store(600, std::sync::atomic::Ordering::SeqCst);
    let err = brokapi_mocks::time::elapse_when(
        "the stop order reached the mock",
        || server.state.count_calls("PlaceOrder") == 1,
        std::time::Duration::from_millis(200),
        ud.place_stop(stop(Side::Sell, StopOrderKind::StopLoss, dec!(100.0), None)),
    )
    .await
    .unwrap_err();
    assert!(matches!(err, Error::Unknown), "{err:?}");
}

#[tokio::test]
async fn there_is_no_separate_stop_stream() {
    let server = MockServer::start().await;
    let (ud, _rx) = ud(&server, "ACC1").await;
    assert!(matches!(
        ud.subscribe_stop_orders().await,
        Err(Error::Unsupported { .. })
    ));
}
