//! The fixes of the code review of 2026-10-21 for BCS: status 6, the price of a fill, the history of trades,
//! the cut of the order events, the reconcile by `GET`, the errors. Against the mock, no network.

mod mock_server;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use brokapi_bcs::UdCore;
use brokapi_bcs::limits::default_limits;
use brokapi_bcs::map::execution::{Cumulative, cumulative_of, map_ws_execution};
use brokapi_bcs::{UserDataConfig, UserDataConnector};
use brokapi_bcs_protocol::config::BcsConfig;
use brokapi_bcs_protocol::models::messages::IncomingMessage;
use brokapi_bcs_protocol::ws::client::BcsWsChannel;
use brokapi_bcs_protocol::ws::models::OrderMessage;
use brokapi_core::{
    ClientOrderId, Decimal, Envelope, Error, Event, ExecutionSource, InstrumentId, InstrumentKey,
    NativeId, OrderExecutor, OrderState, OrderType, PlaceOrderRequest, Price, Qty, ShutdownMode,
    Side, SubscriptionId, dec,
};
use brokapi_runtime::BrokerLimiter;
use brokapi_sansio::{
    BrokerCore, DesiredSubscription, EventSink, SubscriptionKind, SubscriptionSet,
};
use mock_server::{MockOptions, MockServer};
use tokio::sync::mpsc;

fn sber() -> InstrumentId {
    InstrumentId::new(
        InstrumentKey::new("SBER", "TQBR"),
        NativeId::TickerClass {
            ticker: "SBER".into(),
            class_code: "TQBR".into(),
        },
    )
}

fn limit_buy(qty: i64, price: Decimal) -> PlaceOrderRequest {
    PlaceOrderRequest {
        cid: ClientOrderId::new_v4(),
        instrument: sber(),
        side: Side::Buy,
        order_type: OrderType::Limit,
        qty: Qty::from(qty),
        price: Some(Price::new(price)),
    }
}

async fn ud(opts: &Arc<MockOptions>) -> (MockServer, UserDataConnector, mpsc::Receiver<Envelope>) {
    let server = MockServer::start(Arc::clone(opts)).await;
    let (tx, rx) = mpsc::channel(256);
    let cfg = BcsConfig::for_mock("mock-refresh-token", &server.http_base).with_write(true);
    let ud = UserDataConnector::start(
        UserDataConfig::new(cfg, "bcs-ud"),
        BrokerLimiter::new(&default_limits()),
        tx,
    )
    .await
    .expect("ud start");
    (server, ud, rx)
}

/// `order_state` that waits out the local quota (the clock of the test is moved, nobody sleeps).
async fn state_of(ud: &UserDataConnector, cid: ClientOrderId) -> brokapi_core::OrderSnapshot {
    loop {
        match ud.order_state(cid).await {
            Err(Error::RateLimited { retry_after }) => {
                // A whole second, not `retry_after`: a call that spends several tokens needs them all back.
                brokapi_mocks::time::jump(retry_after.max(Duration::from_secs(1))).await
            }
            other => return other.unwrap(),
        }
    }
}

/// `reconcile` that waits out the local quota (the clock of the test is moved, nobody sleeps).
async fn reconcile_of(
    ud: &UserDataConnector,
    ledger: &[ClientOrderId],
) -> brokapi_core::ReconcileReport {
    loop {
        match ud.reconcile(ledger).await {
            Err(Error::RateLimited { retry_after }) => {
                // A whole second, not `retry_after`: a call that spends several tokens needs them all back.
                brokapi_mocks::time::jump(retry_after.max(Duration::from_secs(1))).await
            }
            other => return other.unwrap(),
        }
    }
}

async fn next_event(rx: &mut mpsc::Receiver<Envelope>, pred: impl Fn(&Event) -> bool) -> Envelope {
    tokio::time::timeout(Duration::from_secs(8), async {
        while let Some(env) = rx.recv().await {
            if pred(&env.payload) {
                return env;
            }
        }
        panic!("the channel closed");
    })
    .await
    .expect("the event within 8s")
}

// ─── the price of a fill (К4) ────────────────────────────────────────────────

fn text(last: &str, executed: &str, avg: &str, value: &str, exec_id: &str) -> String {
    format!(
        r#"{{"originalClientOrderId":"550e8400-e29b-41d4-a716-446655440000","clientOrderId":"",
        "data":{{"messageType":"8","orderStatus":"1","executionType":"11","orderQuantity":100,
        "executedQuantity":{executed},"lastQuantity":{last},"remainedQuantity":0,"ticker":"SBER",
        "classCode":"TQBR","side":"1","orderType":"2","averagePrice":{avg},"orderId":"O-1",
        "executionId":"{exec_id}","price":245.0,"currency":"RUB","executionValue":{value}}}}}"#
    )
}

fn message(last: &str, executed: &str, avg: &str, value: &str, exec_id: &str) -> OrderMessage {
    serde_json::from_str(&text(last, executed, avg, value, exec_id)).unwrap()
}

#[test]
fn the_docs_sample_is_not_double_counted() {
    // The sample of the broker: 50 of 100 filled just now, executedQuantity 100, executionValue 24450. The old
    // code divided 24450 by 50 and got 489. Without an earlier message the best the message gives is the
    // average price of the order, 244.5.
    let m = message("50", "100", "244.5", "24450", "X1");
    let x = map_ws_execution(&m, Instant::now(), None).unwrap();
    assert_eq!(x.price.decimal(), dec!(244.5));
    assert_eq!(x.qty.decimal(), dec!(50));
}

#[test]
fn two_fills_at_different_prices_give_two_prices() {
    // 40 at 100, then 60 at 110. The messages carry running totals.
    let first = message("40", "40", "100", "4000", "X1");
    let second = message("60", "100", "106", "10600", "X2");
    let x1 = map_ws_execution(&first, Instant::now(), None).unwrap();
    assert_eq!(x1.price.decimal(), dec!(100));
    let after_first: Cumulative = cumulative_of(first.data.as_ref().unwrap()).unwrap();
    assert_eq!(after_first.qty, dec!(40));
    assert_eq!(after_first.value, dec!(4000));
    let x2 = map_ws_execution(&second, Instant::now(), Some(after_first)).unwrap();
    // (10600 - 4000) / (100 - 40) = 110, not the average 106
    assert_eq!(x2.price.decimal(), dec!(110));
    assert_eq!(x2.qty.decimal(), dec!(60));
}

#[test]
fn totals_from_the_average_price_when_the_value_is_missing() {
    let m: OrderMessage = serde_json::from_str(
        r#"{"originalClientOrderId":"550e8400-e29b-41d4-a716-446655440000","clientOrderId":"",
        "data":{"orderStatus":"1","executedQuantity":10,"lastQuantity":10,"ticker":"SBER","classCode":"TQBR",
        "side":"1","averagePrice":250.5,"orderId":"O-2","executionId":"Y1","price":251}}"#,
    )
    .unwrap();
    let totals = cumulative_of(m.data.as_ref().unwrap()).unwrap();
    assert_eq!(totals.value, dec!(2505.0));
    let x = map_ws_execution(&m, Instant::now(), None).unwrap();
    assert_eq!(x.price.decimal(), dec!(250.5));
    // Nothing to build totals from: no executed quantity, no value, no average.
    let bare: OrderMessage = serde_json::from_str(
        r#"{"originalClientOrderId":"550e8400-e29b-41d4-a716-446655440000","clientOrderId":"",
        "data":{"orderStatus":"1","lastQuantity":10,"ticker":"SBER","classCode":"TQBR","side":"1",
        "orderId":"O-3","executionId":"Y2","price":251}}"#,
    )
    .unwrap();
    assert!(cumulative_of(bare.data.as_ref().unwrap()).is_none());
    // Then the price of the order is all there is.
    let x = map_ws_execution(&bare, Instant::now(), None).unwrap();
    assert_eq!(x.price.decimal(), dec!(251));
}

#[test]
fn a_gap_in_the_totals_falls_back_to_the_average() {
    // A message was missed: the quantity grew by 70, not by the 60 of `lastQuantity`.
    let before = Cumulative {
        qty: dec!(30),
        value: dec!(3000),
    };
    let m = message("60", "100", "106", "10600", "X3");
    let x = map_ws_execution(&m, Instant::now(), Some(before)).unwrap();
    assert_eq!(x.price.decimal(), dec!(106));
}

fn core_with_executions() -> UdCore {
    let mut core = UdCore::new();
    core.apply(&SubscriptionSet::with_subscriptions(vec![
        DesiredSubscription {
            id: SubscriptionId::new(1),
            kind: SubscriptionKind::OrderState,
        },
        DesiredSubscription {
            id: SubscriptionId::new(2),
            kind: SubscriptionKind::Executions,
        },
    ]));
    core
}

fn fills_of(out: &mut EventSink) -> Vec<brokapi_core::Execution> {
    out.drain()
        .into_iter()
        .filter_map(|e| match e.payload {
            Event::Execution(x) => Some(x),
            _ => None,
        })
        .collect()
}

#[test]
fn the_core_keeps_the_totals_of_each_order_between_messages() {
    let mut core = core_with_executions();
    let mut out = EventSink::new();
    let now = Instant::now();
    let first = text("40", "40", "100", "4000", "X1");
    let second = text("60", "100", "106", "10600", "X2");
    core.feed_ws(BcsWsChannel::OrderEvents, &first, now, &mut out);
    core.feed_ws(BcsWsChannel::OrderEvents, &second, now, &mut out);
    // A repeated message does not make a second execution and does not move the totals.
    core.feed_ws(BcsWsChannel::OrderEvents, &second, now, &mut out);
    let fills = fills_of(&mut out);
    assert_eq!(fills.len(), 2);
    assert_eq!(fills[0].price.decimal(), dec!(100));
    assert_eq!(fills[1].price.decimal(), dec!(110));
}

#[test]
fn totals_are_kept_even_when_nobody_listens_to_executions() {
    let mut core = UdCore::new();
    core.apply(&SubscriptionSet::with_subscriptions(vec![
        DesiredSubscription {
            id: SubscriptionId::new(1),
            kind: SubscriptionKind::OrderState,
        },
    ]));
    let mut out = EventSink::new();
    let now = Instant::now();
    core.feed_ws(
        BcsWsChannel::OrderEvents,
        &text("40", "40", "100", "4000", "X1"),
        now,
        &mut out,
    );
    assert!(fills_of(&mut out).is_empty());
    // Someone subscribes to executions now: the second fill is priced from the totals seen before.
    core.apply(&SubscriptionSet::with_subscriptions(vec![
        DesiredSubscription {
            id: SubscriptionId::new(1),
            kind: SubscriptionKind::OrderState,
        },
        DesiredSubscription {
            id: SubscriptionId::new(2),
            kind: SubscriptionKind::Executions,
        },
    ]));
    core.feed_ws(
        BcsWsChannel::OrderEvents,
        &text("60", "100", "106", "10600", "X2"),
        now,
        &mut out,
    );
    let fills = fills_of(&mut out);
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].price.decimal(), dec!(110));
}

#[tokio::test]
async fn fills_at_different_prices_arrive_with_their_own_prices() {
    let opts = Arc::new(MockOptions::default());
    let (server, ud, mut rx) = ud(&opts).await;
    reconcile_of(&ud, &[]).await;
    brokapi_mocks::time::wait_until("order events socket", || opts.event_sockets() > 0).await;
    ud.subscribe_executions().await.unwrap();
    let req = limit_buy(100, dec!(120));
    let cid = req.cid;
    ud.place(req).await.unwrap();
    let cid_text = cid.hyphenated().to_string();
    server.fill(&cid_text, Qty::from(40), Price::new(dec!(100)));
    server.fill(&cid_text, Qty::from(60), Price::new(dec!(110)));
    let mut prices = Vec::new();
    while prices.len() < 2 {
        let env = next_event(&mut rx, |e| matches!(e, Event::Execution(_))).await;
        let Event::Execution(x) = env.payload else {
            unreachable!()
        };
        prices.push((x.qty.decimal(), x.price.decimal()));
    }
    assert_eq!(prices, vec![(dec!(40), dec!(100)), (dec!(60), dec!(110))]);
    ud.shutdown(ShutdownMode::Immediate).await;
}

// ─── cancelling (К3) ─────────────────────────────────────────────────────────

#[tokio::test]
async fn a_cancel_that_is_only_started_is_not_a_cancel() {
    let opts = Arc::new(MockOptions::default());
    opts.cancel_pending.store(true, Ordering::SeqCst);
    let (server, ud, mut rx) = ud(&opts).await;
    reconcile_of(&ud, &[]).await;
    brokapi_mocks::time::wait_until("order events socket", || opts.event_sockets() > 0).await;
    ud.subscribe_order_state().await.unwrap();
    let req = limit_buy(100, dec!(120));
    let cid = req.cid;
    ud.place(req).await.unwrap();

    // The broker answers 6, "cancelling": the order is alive, so the call is not Ok.
    let err = ud.cancel(cid).await.unwrap_err();
    assert!(matches!(err, Error::Unknown), "{err:?}");
    // The stream shows the same: an Unknown state, not Cancelled.
    let seen = next_event(
        &mut rx,
        |e| matches!(e, Event::Order(o) if o.cid == cid && matches!(o.state, OrderState::Unknown { .. })),
    )
    .await;
    assert!(matches!(seen.payload, Event::Order(_)));
    // GET shows it too, and the order is still among the active ones.
    let snap = state_of(&ud, cid).await;
    assert!(matches!(snap.state, OrderState::Unknown { .. }), "{snap:?}");
    assert!(
        ud.active_orders()
            .await
            .unwrap()
            .iter()
            .any(|o| o.cid == cid)
    );
    // The reconcile does not say "closed" for it either.
    let report = reconcile_of(&ud, &[cid]).await;
    assert_eq!(report.confirmed.len(), 1);
    assert!(report.closed_offline.is_empty());

    // Then the broker finishes the cancel: 4, and only now it is Cancelled.
    server.finish_cancel(&cid.hyphenated().to_string());
    next_event(
        &mut rx,
        |e| matches!(e, Event::Order(o) if o.cid == cid && matches!(o.state, OrderState::Cancelled { .. })),
    )
    .await;
    let snap = state_of(&ud, cid).await;
    assert!(matches!(snap.state, OrderState::Cancelled { .. }));
    let report = reconcile_of(&ud, &[cid]).await;
    assert_eq!(report.closed_offline, vec![cid]);
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn an_order_that_fills_while_it_is_cancelling_is_filled() {
    let opts = Arc::new(MockOptions::default());
    opts.cancel_pending.store(true, Ordering::SeqCst);
    let (server, ud, mut rx) = ud(&opts).await;
    reconcile_of(&ud, &[]).await;
    brokapi_mocks::time::wait_until("order events socket", || opts.event_sockets() > 0).await;
    ud.subscribe_order_state().await.unwrap();
    let req = limit_buy(10, dec!(120));
    let cid = req.cid;
    ud.place(req).await.unwrap();
    assert!(matches!(ud.cancel(cid).await, Err(Error::Unknown)));
    // The fill wins the race against the cancel.
    server.fill(
        &cid.hyphenated().to_string(),
        Qty::from(10),
        Price::new(dec!(120)),
    );
    next_event(
        &mut rx,
        |e| matches!(e, Event::Order(o) if o.cid == cid && matches!(o.state, OrderState::Filled { .. })),
    )
    .await;
    ud.shutdown(ShutdownMode::Immediate).await;
}

// ─── the reconcile decides by GET (К7) ───────────────────────────────────────

#[tokio::test]
async fn an_order_that_is_alive_by_get_but_missing_from_the_list_is_confirmed() {
    let opts = Arc::new(MockOptions::default());
    let (server, ud, _rx) = ud(&opts).await;
    reconcile_of(&ud, &[]).await;
    let req = limit_buy(10, dec!(120));
    let cid = req.cid;
    ud.place(req).await.unwrap();
    // The list is cut (the 1000 last orders, a time window): it does not show the order, GET does.
    server.hide_from_list(&cid.hyphenated().to_string());
    assert!(
        !ud.active_orders()
            .await
            .unwrap()
            .iter()
            .any(|o| o.cid == cid)
    );
    let report = reconcile_of(&ud, &[cid]).await;
    assert_eq!(report.confirmed.len(), 1, "alive by GET");
    assert_eq!(report.confirmed[0].cid, cid);
    assert!(
        report.closed_offline.is_empty(),
        "not closed: a second order would double it"
    );
    assert!(report.foreign.is_empty());
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn a_cid_the_broker_does_not_know_is_closed_and_an_unknown_order_is_foreign() {
    let opts = Arc::new(MockOptions::default());
    let (server, ud, _rx) = ud(&opts).await;
    reconcile_of(&ud, &[]).await;
    let ghost = ClientOrderId::new_v4();
    let report = reconcile_of(&ud, &[ghost]).await;
    assert_eq!(report.closed_offline, vec![ghost]);
    // An order of the account the ledger does not list is foreign.
    let other = limit_buy(10, dec!(120));
    ud.place(other.clone()).await.unwrap();
    let report = reconcile_of(&ud, &[]).await;
    assert_eq!(report.foreign.len(), 1);
    assert_eq!(report.foreign[0].cid, other.cid);
    // A finished order is closed offline.
    server.fill(
        &other.cid.hyphenated().to_string(),
        Qty::from(10),
        Price::new(dec!(120)),
    );
    let report = reconcile_of(&ud, &[other.cid]).await;
    assert_eq!(report.closed_offline, vec![other.cid]);
    ud.shutdown(ShutdownMode::Immediate).await;
}

// ─── history of trades in pages (К5) is in conformance.rs; the order list window ─────

#[tokio::test]
async fn the_order_list_is_asked_up_to_a_day_ahead_of_the_clock() {
    // A clock that lags behind the broker's must not hide the newest orders: covered by the window end.
    let opts = Arc::new(MockOptions::default());
    let (_server, ud, _rx) = ud(&opts).await;
    reconcile_of(&ud, &[]).await;
    let req = limit_buy(10, dec!(120));
    ud.place(req.clone()).await.unwrap();
    assert!(
        ud.active_orders()
            .await
            .unwrap()
            .iter()
            .any(|o| o.cid == req.cid)
    );
    ud.shutdown(ShutdownMode::Immediate).await;
}

// ─── the cut of the order events (К6) ────────────────────────────────────────

#[test]
fn the_core_announces_a_cut_from_the_second_connect_on() {
    let mut core = core_with_executions();
    let now = Instant::now();
    let mut out = EventSink::new();
    // The first connect: nothing was lost.
    core.on_incoming(IncomingMessage::Reconnected, now, &mut out);
    assert!(out.is_empty());
    // The second: both subscriptions that carry orders and fills are told.
    core.on_incoming(IncomingMessage::Reconnected, now, &mut out);
    let subs: Vec<_> = out
        .drain()
        .into_iter()
        .map(|e| match e.payload {
            Event::Reconnected { sub, .. } => sub,
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(subs, vec![SubscriptionId::new(1), SubscriptionId::new(2)]);
}

#[test]
fn a_cut_without_subscriptions_says_nothing() {
    let mut core = UdCore::new();
    let mut out = EventSink::new();
    core.on_incoming(IncomingMessage::Reconnected, Instant::now(), &mut out);
    core.on_incoming(IncomingMessage::Reconnected, Instant::now(), &mut out);
    assert!(out.is_empty());
}

// ─── errors ──────────────────────────────────────────────────────────────────

#[test]
fn errors_are_the_same_kind_as_at_the_other_brokers() {
    use brokapi_bcs::error::{map_command_err, map_err, map_order_cmd_err, map_order_read_err};
    use brokapi_bcs_protocol::error::BcsApiError;
    let auth = || BcsApiError::AuthError {
        message: "bad token".into(),
    };
    // A refused token is a configuration problem, for a read and for a command.
    assert!(matches!(map_err(auth()), Error::Config(_)));
    assert!(matches!(map_command_err(auth()), Error::Config(_)));
    // Not found is `Rejected(not_found)` for every call.
    let nf = || BcsApiError::NotFound { trace_id: None };
    for e in [
        map_err(nf()),
        map_command_err(nf()),
        map_order_read_err(nf()),
        map_order_cmd_err(nf()),
    ] {
        assert!(e.is_not_found(), "{e:?}");
        assert!(matches!(&e, Error::Rejected { code, .. } if code == brokapi_core::NOT_FOUND));
    }
    // A subscription error stays a network-class problem.
    let sub = BcsApiError::SubscriptionError {
        message: "no".into(),
    };
    assert!(matches!(map_err(sub), Error::Transport(_)));
}

#[test]
fn the_totals_of_old_orders_are_forgotten_so_the_memory_does_not_grow() {
    let mut core = core_with_executions();
    let mut out = EventSink::new();
    let now = Instant::now();
    let one = |order: usize, last: &str, executed: &str, value: &str, exec: &str| {
        text(last, executed, "0", value, exec).replace("\"O-1\"", &format!("\"O-{order}\""))
    };
    // The first order has a fill of 40 at 100.
    core.feed_ws(
        BcsWsChannel::OrderEvents,
        &one(0, "40", "40", "4000", "E0"),
        now,
        &mut out,
    );
    // Twenty thousand other orders come after it.
    for n in 1..=20_000 {
        core.feed_ws(
            BcsWsChannel::OrderEvents,
            &one(n, "1", "1", "100", &format!("E{n}")),
            now,
            &mut out,
        );
    }
    out.drain();
    // The totals of the first order are gone: its second fill (60 more, 100 in all) cannot be priced from them.
    // Without `averagePrice` the best the message says is the price of the order (245 in these messages).
    let second = one(0, "60", "100", "10600", "E0b");
    core.feed_ws(BcsWsChannel::OrderEvents, &second, now, &mut out);
    let fills = fills_of(&mut out);
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].price.decimal(), dec!(245.0));
}

#[tokio::test]
async fn a_reconcile_does_not_guess_when_the_answer_has_no_report() {
    let opts = Arc::new(MockOptions::default());
    let (_server, ud, _rx) = ud(&opts).await;
    reconcile_of(&ud, &[]).await;
    let req = limit_buy(10, dec!(120));
    ud.place(req.clone()).await.unwrap();
    opts.empty_reports.store(true, Ordering::SeqCst);
    // The broker answers about the order, and the answer says nothing: not closed, not alive. The reconcile
    // fails, and the gate stays as it was.
    let err = loop {
        match ud.reconcile(&[req.cid]).await {
            Err(Error::RateLimited { retry_after }) => {
                brokapi_mocks::time::jump(retry_after.max(Duration::from_secs(1))).await
            }
            other => break other.unwrap_err(),
        }
    };
    assert!(matches!(err, Error::Transport(_)), "{err:?}");
    opts.empty_reports.store(false, Ordering::SeqCst);
    let report = reconcile_of(&ud, &[req.cid]).await;
    assert_eq!(report.confirmed.len(), 1);
    ud.shutdown(ShutdownMode::Immediate).await;
}
