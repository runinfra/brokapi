//! Connector-level tests of the BCS user-data side on the stateful mock: the CURRENT order methods,
//! the `orders/events/ws` stream, replace, active orders, reconcile, own trades, stop orders and two
//! accounts. The mock answers 404 to the deprecated forms, so a connector that fell back to them
//! would fail here.

mod mock_server;

use brokapi_core::{Decimal, dec};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use brokapi_bcs::{UserDataConfig, UserDataConnector};
use brokapi_bcs_protocol::config::BcsConfig;
use brokapi_conformance::REJECT_PRICE;
use brokapi_core::{
    ClientOrderId, Envelope, Error, Event, ExecutionSource, InstrumentId, InstrumentKey, NativeId,
    OrderExecutor, OrderState, OrderType, PlaceOrderRequest, PlaceStopOrderRequest, Price, Qty,
    ReplaceOrderRequest, ShutdownMode, Side, StopExpiry, StopOrderExecutor, StopOrderKind,
    StopOrderRef, StopOrderState, TimeRange, UnixNanos,
};
use brokapi_runtime::{BrokerLimiter, BrokerLimits};
use mock_server::{MockOptions, MockServer, REJECT_IN_BODY_PRICE};
use tokio::sync::mpsc;
use tokio::time::timeout;

fn sber() -> InstrumentId {
    InstrumentId::new(
        InstrumentKey::new("SBER", "TQBR"),
        NativeId::TickerClass {
            ticker: "SBER".into(),
            class_code: "TQBR".into(),
        },
    )
}

/// A roomy budget: these tests are about behaviour, not about the rate limits (those are in
/// `raw_access.rs` and in `brokapi-runtime`).
fn limiter() -> Arc<BrokerLimiter> {
    let raw = r#"
[bcs.buckets.big]
rate = 10000
per = "1s"
burst = 10000
[bcs.route]
"*" = ["big"]
[bcs.conns]
max_conns = 8
max_subs_per_conn = 50
supports_multiplex = false
"#;
    BrokerLimiter::new(&BrokerLimits::from_toml_str(raw).unwrap())
}

async fn ud_for(
    server: &MockServer,
    refresh: &str,
    source: &str,
) -> (UserDataConnector, mpsc::Receiver<Envelope>) {
    let (tx, rx) = mpsc::channel(256);
    let cfg = BcsConfig::for_mock(refresh, &server.http_base).with_write(true);
    let ud = UserDataConnector::start(UserDataConfig::new(cfg, source), limiter(), tx)
        .await
        .expect("ud start");
    // Commands wait for the first reconcile.
    ud.reconcile(&[]).await.expect("first reconcile");
    (ud, rx)
}

async fn ud(server: &MockServer) -> (UserDataConnector, mpsc::Receiver<Envelope>) {
    ud_for(server, "mock-refresh-token", "bcs-ud").await
}

fn limit_buy(price: Decimal) -> PlaceOrderRequest {
    PlaceOrderRequest {
        cid: ClientOrderId::new_v4(),
        instrument: sber(),
        side: Side::Buy,
        order_type: OrderType::Limit,
        qty: Qty::from(10),
        price: Some(Price::new(price)),
    }
}

fn stop_req(kind: StopOrderKind, stop: Decimal, limit: Option<Decimal>) -> PlaceStopOrderRequest {
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

async fn next_event(rx: &mut mpsc::Receiver<Envelope>, pred: impl Fn(&Event) -> bool) -> Envelope {
    timeout(Duration::from_secs(8), async {
        while let Some(env) = rx.recv().await {
            if pred(&env.payload) {
                return env;
            }
        }
        panic!("event channel closed");
    })
    .await
    .expect("event within 8s")
}

#[tokio::test]
async fn commands_use_the_current_methods_and_never_the_deprecated_ones() {
    let opts = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&opts)).await;
    let (ud, _rx) = ud(&server).await;

    let req = limit_buy(dec!(100.0));
    let cid = req.cid;
    let ack = ud.place(req).await.expect("place");
    // The ack carries the broker's own order id (`orderId`), not the cid.
    assert!(
        ack.broker_id.as_str().starts_with("20261019-TQBR-"),
        "{ack:?}"
    );
    ud.order_state(cid).await.expect("status");
    ud.active_orders().await.expect("active");
    let new = ClientOrderId::new_v4();
    ud.replace(ReplaceOrderRequest {
        cid: new,
        original_cid: cid,
        qty: None,
        price: Some(Price::from(101)),
    })
    .await
    .expect("replace");
    ud.cancel(new).await.expect("cancel");

    assert_eq!(
        opts.deprecated_calls.load(Ordering::SeqCst),
        0,
        "a deprecated method was called"
    );
    // Synchronous variants for commands (the broker's answer is known at once), plain GET for reads.
    assert_eq!(
        opts.served_count("POST /orders/sync"),
        1,
        "{:?}",
        opts.served.lock().unwrap()
    );
    assert_eq!(opts.served_count("POST /orders/edit/sync"), 1);
    assert_eq!(opts.served_count("POST /orders/cancel/sync"), 1);
    assert_eq!(
        opts.served_count("POST /orders"),
        0,
        "the asynchronous create is not used"
    );
    assert_eq!(opts.served_count("POST /orders/cancel"), 0);
    assert!(
        opts.served_count("GET /orders") >= 2,
        "order_state and the replace read the order with GET /orders"
    );
    // One from the first reconcile, one from `active_orders`.
    assert_eq!(opts.served_count("POST /orders/search"), 2);
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn a_price_off_the_step_is_refused_before_anything_is_sent() {
    let server = MockServer::start(Arc::new(MockOptions::default())).await;
    let (ud, _rx) = ud(&server).await;
    ud.price_grid().set(sber().key, Price::new(dec!(0.05)));
    let mut req = limit_buy(dec!(100.03));
    let err = ud.place(req.clone()).await.unwrap_err();
    assert!(
        matches!(&err, Error::Rejected { code, .. } if code == "price_off_step"),
        "{err:?}"
    );
    // The cid was not spent: the same cid goes out with a price on the grid.
    req.price = Some(Price::new(dec!(100.05)));
    ud.place(req).await.unwrap();
}

#[tokio::test]
async fn a_refusal_is_rejected_whether_it_is_http_400_or_status_8_in_a_200_answer() {
    let server = MockServer::start(Arc::new(MockOptions::default())).await;
    let (ud, _rx) = ud(&server).await;

    // HTTP 400 VALIDATION_ERROR.
    match ud.place(limit_buy(REJECT_PRICE.decimal())).await {
        Err(Error::Rejected { code, .. }) => assert_eq!(code, "validation"),
        other => panic!("{other:?}"),
    }
    // HTTP 200 with orderStatus = 8 and rejectReason: found in the answer of the sync method.
    match ud.place(limit_buy(REJECT_IN_BODY_PRICE)).await {
        Err(Error::Rejected { code, message }) => {
            assert_eq!(code, "rejected");
            assert!(
                message.contains("price is outside"),
                "the broker's reason is passed on: {message}"
            );
        }
        other => panic!("{other:?}"),
    }
    // The cid of a refused order is spent (no automatic retry), a fresh one works.
    ud.place(limit_buy(dec!(100.0)))
        .await
        .expect("the next order goes through");
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn shutdown_reports_only_commands_that_have_no_answer() {
    let server = MockServer::start(Arc::new(MockOptions::default())).await;
    let (ud, _rx) = ud(&server).await;
    // Three answered commands: accepted, refused in a 200 answer, refused with HTTP 400.
    ud.place(limit_buy(dec!(100.0))).await.unwrap();
    assert!(
        ud.place(limit_buy(REJECT_IN_BODY_PRICE)).await.is_err(),
        "a refusal in the body"
    );
    assert!(ud.place(limit_buy(REJECT_PRICE.decimal())).await.is_err());
    // None of them is still waiting for an answer, so none is reported as abandoned.
    let report = ud.shutdown(ShutdownMode::Immediate).await;
    assert!(report.inflight_abandoned.is_empty(), "{report:?}");
}

#[tokio::test]
async fn cancel_tells_the_truth_about_what_happened() {
    let server = MockServer::start(Arc::new(MockOptions::default())).await;
    let (ud, _rx) = ud(&server).await;

    // Unknown order: a definite refusal, not a transport problem.
    match ud.cancel(ClientOrderId::new_v4()).await {
        Err(Error::Rejected { code, .. }) => assert_eq!(code, "not_found"),
        other => panic!("{other:?}"),
    }
    // An order that is already gone: the broker refuses a second cancel.
    let req = limit_buy(dec!(100.0));
    let cid = req.cid;
    ud.place(req).await.unwrap();
    ud.cancel(cid).await.expect("first cancel");
    assert!(matches!(ud.cancel(cid).await, Err(Error::Rejected { .. })));
    assert!(matches!(
        ud.order_state(cid).await.unwrap().state,
        OrderState::Cancelled { .. }
    ));
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn replace_moves_the_order_and_fills_in_what_the_caller_left_out() {
    let server = MockServer::start(Arc::new(MockOptions::default())).await;
    let (ud, _rx) = ud(&server).await;
    let req = limit_buy(dec!(100.0));
    let cid = req.cid;
    ud.place(req).await.unwrap();

    // Only the price given: the quantity is the order's own (10).
    let new = ClientOrderId::new_v4();
    let ack = ud
        .replace(ReplaceOrderRequest {
            cid: new,
            original_cid: cid,
            qty: None,
            price: Some(Price::from(101)),
        })
        .await
        .expect("replace");
    assert_eq!(ack.cid, new);
    let snap = ud.order_state(new).await.unwrap();
    assert_eq!(snap.qty.decimal(), dec!(10.0));
    assert_eq!(snap.price.map(|p| p.decimal()), Some(dec!(101.0)));
    assert!(matches!(snap.state, OrderState::Acked { .. }));
    // The original is gone from the active list, the new one is there.
    let active = ud.active_orders().await.unwrap();
    assert_eq!(active.iter().filter(|o| o.cid == new).count(), 1);
    assert!(active.iter().all(|o| o.cid != cid));

    // Only the quantity given: the price is the order's own.
    let newer = ClientOrderId::new_v4();
    ud.replace(ReplaceOrderRequest {
        cid: newer,
        original_cid: new,
        qty: Some(Qty::from(4)),
        price: None,
    })
    .await
    .expect("replace qty");
    let snap = ud.order_state(newer).await.unwrap();
    assert_eq!(
        (snap.qty.decimal(), snap.price.map(|p| p.decimal())),
        (dec!(4.0), Some(dec!(101.0)))
    );

    // A partly filled order needs an explicit quantity: the docs do not say whether the edit's
    // quantity is the total or the rest, so the connector does not guess.
    server.fill(
        &newer.hyphenated().to_string(),
        Qty::from(1),
        Price::from(101),
    );
    match ud
        .replace(ReplaceOrderRequest {
            cid: ClientOrderId::new_v4(),
            original_cid: newer,
            qty: None,
            price: Some(Price::from(102)),
        })
        .await
    {
        Err(Error::Rejected { code, .. }) => assert_eq!(code, "qty_required"),
        other => panic!("{other:?}"),
    }
    // A refused price keeps the original alive.
    match ud
        .replace(ReplaceOrderRequest {
            cid: ClientOrderId::new_v4(),
            original_cid: newer,
            qty: Some(Qty::from(3)),
            price: Some(REJECT_PRICE),
        })
        .await
    {
        Err(Error::Rejected { .. }) => {}
        other => panic!("{other:?}"),
    }
    assert!(matches!(
        ud.order_state(newer).await.unwrap().state,
        OrderState::PartiallyFilled { .. }
    ));
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn order_events_stream_follows_place_fill_and_cancel_with_our_cid() {
    let opts = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&opts)).await;
    let (ud, mut rx) = ud(&server).await;
    let sub = ud.subscribe_order_state().await.unwrap();
    let exec_sub = ud.subscribe_executions().await.unwrap();

    let req = limit_buy(dec!(100.0));
    let cid = req.cid;
    ud.place(req).await.unwrap();
    let env = next_event(&mut rx, |e| matches!(e, Event::Order(o) if o.cid == cid && matches!(o.state, OrderState::Acked { .. }))).await;
    assert_eq!(env.sub, Some(sub));
    assert_eq!(env.source.as_ref(), "bcs-ud");

    // A partial fill: one order event (PartiallyFilled) and one Execution on the executions sub.
    let exec_id = server.fill(
        &cid.hyphenated().to_string(),
        Qty::from(4),
        Price::new(dec!(99.5)),
    );
    let env = next_event(
        &mut rx,
        |e| matches!(e, Event::Order(o) if matches!(o.state, OrderState::PartiallyFilled { .. })),
    )
    .await;
    let Event::Order(o) = env.payload else {
        unreachable!()
    };
    assert_eq!(o.cid, cid);
    let OrderState::PartiallyFilled { filled, left, .. } = o.state else {
        unreachable!()
    };
    assert_eq!((filled.decimal(), left.decimal()), (dec!(4.0), dec!(6.0)));
    let env = next_event(&mut rx, |e| matches!(e, Event::Execution(_))).await;
    assert_eq!(env.sub, Some(exec_sub));
    let Event::Execution(x) = env.payload else {
        unreachable!()
    };
    assert_eq!(
        (x.trade_id.as_str(), x.qty.decimal(), x.cid),
        (exec_id.as_str(), dec!(4.0), Some(cid))
    );

    ud.cancel(cid).await.unwrap();
    next_event(&mut rx, |e| matches!(e, Event::Order(o) if o.cid == cid && matches!(o.state, OrderState::Cancelled { .. }))).await;
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn active_orders_name_orders_this_process_did_not_place_by_their_real_cid() {
    let opts = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&opts)).await;
    let (first, _rx1) = ud(&server).await;
    let req = limit_buy(dec!(100.0));
    let cid = req.cid;
    first.place(req).await.unwrap();
    first.shutdown(ShutdownMode::Immediate).await;

    // A restarted process: its index is empty, the list carries only the broker's order id. One lookup
    // by the exchange id (GET /orders?orderIdType=2) gives the cid back.
    let (second, _rx2) = ud(&server).await;
    let active = second.active_orders().await.unwrap();
    let mine = active
        .iter()
        .find(|o| o.cid == cid)
        .expect("the order, under its own cid");
    assert!(mine.broker_id.is_some());
    assert_eq!(mine.qty.decimal(), dec!(10.0));
    assert_eq!(mine.side, Side::Buy);
    let gets = opts.served_count("GET /orders");
    // Learned: the next call does not look it up again.
    second.active_orders().await.unwrap();
    assert_eq!(opts.served_count("GET /orders"), gets);

    // Reconcile: the ledger order is confirmed, an order the ledger does not know is foreign,
    // a cid the broker never saw is closed offline.
    let other = limit_buy(dec!(90.0));
    let other_cid = other.cid;
    second.place(other).await.unwrap();
    let ghost = ClientOrderId::new_v4();
    let report = second.reconcile(&[cid, ghost]).await.unwrap();
    assert_eq!(
        report.confirmed.iter().map(|o| o.cid).collect::<Vec<_>>(),
        vec![cid]
    );
    assert_eq!(report.closed_offline, vec![ghost]);
    assert_eq!(
        report.foreign.iter().map(|o| o.cid).collect::<Vec<_>>(),
        vec![other_cid]
    );

    // A ledger order that was cancelled meanwhile is closed offline, not confirmed.
    second.cancel(cid).await.unwrap();
    let report = second.reconcile(&[cid]).await.unwrap();
    assert!(report.confirmed.is_empty() && report.closed_offline == vec![cid]);
    second.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn own_trades_history_reads_the_flat_records_of_the_docs() {
    let server = MockServer::start(Arc::new(MockOptions::default())).await;
    let (ud, _rx) = ud(&server).await;
    let range = TimeRange::new(
        UnixNanos::from_nanos(0),
        UnixNanos::from_nanos(i64::MAX / 4),
    );
    assert!(ud.executions(range).await.unwrap().is_empty());

    let req = limit_buy(dec!(100.0));
    let cid = req.cid;
    ud.place(req).await.unwrap();
    let exec_id = server.fill(&cid.hyphenated().to_string(), Qty::from(3), Price::from(99));
    server.fill(&cid.hyphenated().to_string(), Qty::from(7), Price::from(98));
    let trades = ud.executions(range).await.unwrap();
    assert_eq!(trades.len(), 2);
    let first = &trades[0];
    assert_eq!(first.qty.decimal(), dec!(3.0));
    assert_eq!(first.price.decimal(), dec!(99.0));
    assert_eq!(first.side, Side::Buy);
    assert_eq!(first.instrument.key.display(), sber().key.display());
    // The trade number is the id (the mock's execution id is its own text, only the count matters).
    assert!(!first.trade_id.is_empty() && !exec_id.is_empty());
    // The history record has no clientOrderId: the cid is not invented.
    assert_eq!(first.cid, None);
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn stop_orders_roundtrip_with_the_wire_the_docs_describe() {
    let opts = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&opts)).await;
    let (ud, _rx) = ud(&server).await;
    assert!(ud.caps().supports_stop_orders);

    // Stop-loss (market), stop-limit and take-profit side by side.
    let sl = stop_req(StopOrderKind::StopLoss, dec!(250.0), None);
    let sl_cid = sl.cid;
    let sl_ack = ud.place_stop(sl).await.expect("stop-loss");
    let sll = stop_req(StopOrderKind::StopLimit, dec!(240.0), Some(dec!(239.0)));
    let sll_ack = ud.place_stop(sll).await.expect("stop-limit");
    let tp = stop_req(StopOrderKind::TakeProfit, dec!(400.0), None);
    let tp_ack = ud.place_stop(tp).await.expect("take-profit");

    let stops = ud.active_stop_orders().await.unwrap();
    assert_eq!(stops.len(), 3, "{stops:?}");
    let by = |id: &brokapi_core::BrokerOrderId| stops.iter().find(|s| &s.broker_id == id).unwrap();
    let (a, b, c) = (
        by(&sl_ack.broker_id),
        by(&sll_ack.broker_id),
        by(&tp_ack.broker_id),
    );
    assert_eq!(
        (a.kind, a.stop_price.decimal(), a.limit_price),
        (StopOrderKind::StopLoss, dec!(250.0), None)
    );
    assert_eq!(a.cid, Some(sl_cid));
    assert_eq!(
        (
            b.kind,
            b.stop_price.decimal(),
            b.limit_price.map(|p| p.decimal())
        ),
        (StopOrderKind::StopLimit, dec!(240.0), Some(dec!(239.0)))
    );
    assert_eq!(
        (c.kind, c.stop_price.decimal(), c.limit_price),
        (StopOrderKind::TakeProfit, dec!(400.0), None)
    );
    assert!(stops.iter().all(|s| s.side == Side::Sell
        && s.qty.decimal() == dec!(2.0)
        && s.state == StopOrderState::Active));

    // Stops are not plain orders, plain orders are not stops.
    assert!(ud.active_orders().await.unwrap().is_empty());
    ud.place(limit_buy(dec!(100.0))).await.unwrap();
    assert_eq!(ud.active_stop_orders().await.unwrap().len(), 3);
    assert_eq!(ud.active_orders().await.unwrap().len(), 1);

    // Cancel by cid and by the broker's id.
    ud.cancel_stop(StopOrderRef::Cid(sl_cid))
        .await
        .expect("cancel by cid");
    ud.cancel_stop(StopOrderRef::Broker(sll_ack.broker_id.clone()))
        .await
        .expect("cancel by broker id");
    let left = ud.active_stop_orders().await.unwrap();
    assert_eq!(left.len(), 1);
    assert_eq!(left[0].broker_id, tp_ack.broker_id);
    // A second cancel of the same stop is refused by the broker.
    assert!(matches!(
        ud.cancel_stop(StopOrderRef::Cid(sl_cid)).await,
        Err(Error::Rejected { .. })
    ));

    // No stream of stop statuses.
    assert!(matches!(
        ud.subscribe_stop_orders().await,
        Err(Error::Unsupported { .. })
    ));
    assert_eq!(opts.deprecated_calls.load(Ordering::SeqCst), 0);
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn stop_order_input_is_refused_before_anything_is_sent() {
    let opts = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&opts)).await;
    let (ud, _rx) = ud(&server).await;
    let before = opts.served.lock().unwrap().len();

    let mut eod = stop_req(StopOrderKind::StopLoss, dec!(250.0), None);
    eod.expiry = StopExpiry::EndOfDay;
    for (req, code) in [
        (eod, "unsupported_expiry"),
        (
            stop_req(StopOrderKind::StopLimit, dec!(250.0), None),
            "missing_limit_price",
        ),
        (
            stop_req(StopOrderKind::StopLoss, dec!(-1.0), None),
            "invalid_price",
        ),
    ] {
        match ud.place_stop(req).await {
            Err(Error::Rejected { code: got, .. }) => assert_eq!(got, code),
            other => panic!("{code}: {other:?}"),
        }
    }
    assert_eq!(
        opts.served.lock().unwrap().len(),
        before,
        "nothing reached the broker"
    );

    // The same cid is never sent twice.
    let req = stop_req(StopOrderKind::StopLoss, dec!(250.0), None);
    ud.place_stop(req.clone()).await.unwrap();
    match ud.place_stop(req).await {
        Err(Error::Rejected { code, .. }) => assert_eq!(code, "duplicate_place"),
        other => panic!("{other:?}"),
    }
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn two_accounts_with_two_tokens_see_only_their_own_orders_and_events() {
    let opts = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&opts)).await;
    let (a, mut rx_a) = ud_for(&server, "mock-refresh-token", "bcs-a").await;
    let (b, mut rx_b) = ud_for(&server, "mock-refresh-second", "bcs-b").await;
    a.subscribe_order_state().await.unwrap();
    b.subscribe_order_state().await.unwrap();

    let req = limit_buy(dec!(100.0));
    let cid = req.cid;
    a.place(req).await.unwrap();
    let stop = stop_req(StopOrderKind::StopLoss, dec!(250.0), None);
    a.place_stop(stop).await.unwrap();

    // B cannot see, read or cancel A's orders.
    assert!(b.active_orders().await.unwrap().is_empty());
    assert!(b.active_stop_orders().await.unwrap().is_empty());
    assert!(b.order_state(cid).await.is_err());
    match b.cancel(cid).await {
        Err(Error::Rejected { code, .. }) => assert_eq!(code, "not_found"),
        other => panic!("{other:?}"),
    }
    assert_eq!(a.active_orders().await.unwrap().len(), 1);
    // Reconcile of B with A's cid: the broker never saw it for B.
    let report = b.reconcile(&[cid]).await.unwrap();
    assert_eq!(report.closed_offline, vec![cid]);

    // Events: A's stream has A's order, B's stream has nothing of A. B's own order is the fence: the
    // event of A was sent first, so if it had leaked it would be in B's channel before B's own.
    next_event(&mut rx_a, |e| matches!(e, Event::Order(o) if o.cid == cid)).await;

    // B places on its own, with its own limiter budget untouched by A.
    let own = limit_buy(dec!(100.0));
    let own_cid = own.cid;
    b.place(own).await.unwrap();
    assert_eq!(b.active_orders().await.unwrap().len(), 1);
    let first = next_event(&mut rx_b, |e| matches!(e, Event::Order(_))).await;
    assert!(
        matches!(&first.payload, Event::Order(o) if o.cid == own_cid),
        "B got an order event of A: {:?}",
        first.payload
    );
    a.shutdown(ShutdownMode::Immediate).await;
    b.shutdown(ShutdownMode::Immediate).await;
}

/// A budget of one order per 400 ms, for the tests about the quota.
fn tight_orders_limiter() -> Arc<BrokerLimiter> {
    let raw = r#"
[bcs.buckets.big]
rate = 10000
per = "1s"
burst = 10000
[bcs.buckets.one_order]
rate = 1
per = "400ms"
burst = 1
[bcs.route]
"*" = ["big"]
PlaceOrder = ["one_order"]
PlaceStopOrder = ["one_order"]
[bcs.conns]
max_conns = 8
max_subs_per_conn = 50
supports_multiplex = false
"#;
    BrokerLimiter::new(&BrokerLimits::from_toml_str(raw).unwrap())
}

/// As at T-Bank and Finam: the cid is marked after the quota and after the input check. A `place` that
/// was refused for `RateLimited` (or for a missing price) sent nothing, so the same cid is used again.
#[tokio::test]
async fn a_refusal_before_the_wire_leaves_the_cid_usable() {
    let opts = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&opts)).await;
    let (tx, _rx) = mpsc::channel(256);
    let cfg = BcsConfig::for_mock("mock-refresh-token", &server.http_base).with_write(true);
    let ud = UserDataConnector::start(
        UserDataConfig::new(cfg, "bcs-ud"),
        tight_orders_limiter(),
        tx,
    )
    .await
    .expect("ud start");
    ud.reconcile(&[]).await.expect("first reconcile");

    // Input: a limit order without a price is refused locally, the cid is not spent.
    let mut req = limit_buy(dec!(1.0));
    let price = req.price.take();
    assert!(
        matches!(ud.place(req.clone()).await, Err(Error::Rejected { ref code, .. }) if code == "missing_price")
    );
    req.price = price;
    ud.place(req)
        .await
        .expect("the cid of a refused input is usable");

    // Quota: the next order comes at once and finds the bucket empty.
    let second = limit_buy(dec!(1.0));
    let retry_after = match ud.place(second.clone()).await {
        Err(Error::RateLimited { retry_after }) => retry_after,
        other => panic!("expected RateLimited, got {other:?}"),
    };
    assert_eq!(
        opts.served_count("POST /orders/sync"),
        1,
        "nothing was sent for the refused place"
    );
    brokapi_mocks::time::jump(retry_after + Duration::from_millis(50)).await;
    ud.place(second.clone())
        .await
        .expect("the same cid after RateLimited");
    assert_eq!(opts.served_count("POST /orders/sync"), 2);
    // Once it has gone out, the cid is spent.
    brokapi_mocks::time::jump(Duration::from_millis(450)).await;
    assert!(
        matches!(ud.place(second).await, Err(Error::Rejected { ref code, .. }) if code == "duplicate_place")
    );

    // The same for a conditional order.
    brokapi_mocks::time::jump(Duration::from_millis(450)).await;
    let stop = stop_req(StopOrderKind::StopLoss, dec!(250.0), None);
    ud.place_stop(stop_req(StopOrderKind::StopLoss, dec!(251.0), None))
        .await
        .expect("a stop order");
    let retry_after = match ud.place_stop(stop.clone()).await {
        Err(Error::RateLimited { retry_after }) => retry_after,
        other => panic!("expected RateLimited, got {other:?}"),
    };
    brokapi_mocks::time::jump(retry_after + Duration::from_millis(50)).await;
    ud.place_stop(stop)
        .await
        .expect("the same cid of a stop order after RateLimited");

    ud.shutdown(ShutdownMode::Immediate).await;
}
