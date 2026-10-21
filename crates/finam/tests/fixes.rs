//! The fixes of the code review of 2026-10-21 for Finam: the cut of the order streams, the errors, the
//! lot check. Against the gRPC mock and the sans-io core, no network.

mod mock_server;

use std::sync::Arc;
use std::time::Instant;

use brokapi_core::{
    ClientOrderId, Error, Event, OrderExecutor, OrderType, PlaceOrderRequest,
    PlaceStopOrderRequest, Price, Qty, ShutdownMode, Side, StopExpiry, StopOrderExecutor,
    StopOrderKind, SubscriptionId,
};
use brokapi_finam::core::ud::UdSubKind;
use brokapi_finam::core::{UdCore, UdEvent, UdFrame};
use brokapi_finam::error::{map_command_err, map_protocol_err};
use brokapi_finam::index::OrderIndex;
use brokapi_finam_protocol::FinamError;
use brokapi_sansio::{BrokerCore, EventSink};
use mock_server::{MockServer, sber, ud};
use tonic::Code;

fn frame(sub: SubscriptionId, event: UdEvent) -> UdFrame {
    UdFrame { sub, event }
}

#[test]
fn a_reopened_stream_of_orders_or_fills_is_announced_once() {
    let (orders, fills, account) = (
        SubscriptionId::new(1),
        SubscriptionId::new(2),
        SubscriptionId::new(3),
    );
    let mut core = UdCore::new(Arc::new(OrderIndex::new()));
    core.register(orders, UdSubKind::Orders);
    core.register(fills, UdSubKind::Executions);
    core.register(account, UdSubKind::Portfolio);
    let now = Instant::now();
    let mut out = EventSink::new();

    // The first open of each stream: nothing was lost.
    for sub in [orders, fills, account] {
        core.on_frame(frame(sub, UdEvent::Connected), now, &mut out);
    }
    assert!(out.is_empty());

    // All three go down (twice for the orders: the first moment of the gap is kept) and open again.
    for sub in [orders, fills, account] {
        core.on_frame(
            frame(sub, UdEvent::Disconnected("cut".into())),
            now,
            &mut out,
        );
    }
    core.on_frame(
        frame(orders, UdEvent::Disconnected("again".into())),
        now,
        &mut out,
    );
    assert!(out.is_empty());
    for sub in [orders, fills, account] {
        core.on_frame(frame(sub, UdEvent::Connected), now, &mut out);
    }
    let told: Vec<_> = out
        .drain()
        .into_iter()
        .map(|e| match e.payload {
            Event::Reconnected { sub, gap_from } => {
                assert_eq!(e.sub, Some(sub));
                assert!(gap_from.as_nanos() > 0);
                sub
            }
            other => panic!("{other:?}"),
        })
        .collect();
    // The orders and the fills are told. The account stream is not: every message of it is a whole state.
    assert_eq!(told, vec![orders, fills]);

    // A stream that was never open and fails to open is no loss when it opens at last.
    let late = SubscriptionId::new(4);
    core.register(late, UdSubKind::Orders);
    core.on_frame(
        frame(late, UdEvent::Disconnected("not yet".into())),
        now,
        &mut out,
    );
    core.on_frame(frame(late, UdEvent::Connected), now, &mut out);
    assert!(out.is_empty());
    // A refusal of the broker is a status, not a cut.
    core.on_frame(
        frame(late, UdEvent::Failed("no permission".into())),
        now,
        &mut out,
    );
    assert!(out.is_empty());
    // A stream that is forgotten is not announced any more.
    core.unregister(orders);
    core.on_frame(frame(orders, UdEvent::Connected), now, &mut out);
    assert!(out.is_empty());
}

#[test]
fn errors_are_the_same_kind_as_at_the_other_brokers() {
    // A refused token: a configuration problem, whether it comes as a status or as a refused login.
    let status = FinamError::Status {
        code: Code::Unauthenticated,
        message: "token expired".into(),
    };
    assert!(matches!(map_protocol_err(&status), Error::Config(_)));
    assert!(matches!(map_command_err(&status), Error::Config(_)));
    assert!(matches!(
        map_protocol_err(&FinamError::Auth("secret refused".into())),
        Error::Config(_)
    ));
    // Not found has the one code everywhere.
    let missing = FinamError::Status {
        code: Code::NotFound,
        message: "no such order".into(),
    };
    assert!(map_protocol_err(&missing).is_not_found());
    assert!(map_command_err(&missing).is_not_found());
}

fn buy(qty: Qty) -> PlaceOrderRequest {
    PlaceOrderRequest {
        cid: ClientOrderId::new_v4(),
        instrument: sber(),
        side: Side::Buy,
        order_type: OrderType::Limit,
        qty,
        price: Some(Price::from(1)),
    }
}

#[tokio::test]
async fn the_lot_is_checked_only_when_it_is_known() {
    let server = MockServer::start().await;
    let (ud, _rx) = ud(&server, "ACC1").await;
    ud.reconcile(&[]).await.unwrap();
    // The lot is not known: the broker decides, nothing is refused locally.
    let odd = buy(Qty::from(15));
    ud.place(odd.clone()).await.unwrap();
    ud.cancel(odd.cid).await.unwrap();
    // The lot is known: 15 is not a whole number of lots of 10.
    ud.price_grid().set_lot(sber().key, Qty::from(10));
    let err = ud.place(buy(Qty::from(15))).await.unwrap_err();
    assert!(
        matches!(&err, Error::Rejected { code, .. } if code == "invalid_qty"),
        "{err:?}"
    );
    let stop = PlaceStopOrderRequest {
        cid: ClientOrderId::new_v4(),
        instrument: sber(),
        side: Side::Sell,
        kind: StopOrderKind::StopLoss,
        qty: Qty::from(15),
        stop_price: Price::from(1),
        limit_price: None,
        expiry: StopExpiry::GoodTillCancel,
    };
    let err = ud.place_stop(stop).await.unwrap_err();
    assert!(
        matches!(&err, Error::Rejected { code, .. } if code == "invalid_qty"),
        "{err:?}"
    );
    // Only the one order of the first step reached the broker.
    assert_eq!(server.state.count_calls("PlaceOrder"), 1);
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn the_cut_streams_open_again_and_the_orders_stream_goes_on() {
    let server = MockServer::start().await;
    let (ud, mut rx) = ud(&server, "ACC1").await;
    ud.reconcile(&[]).await.unwrap();
    ud.subscribe_order_state().await.unwrap();
    brokapi_mocks::time::wait_until("the orders stream is open", || {
        server
            .state
            .order_stream_opens
            .load(std::sync::atomic::Ordering::SeqCst)
            == 1
    })
    .await;
    server.state.cut_user_streams();
    brokapi_mocks::time::wait_until("the orders stream is open again", || {
        server
            .state
            .order_stream_opens
            .load(std::sync::atomic::Ordering::SeqCst)
            == 2
    })
    .await;
    // The first thing in the channel after the cut is the notice.
    let env = loop {
        let env = rx.recv().await.expect("the channel stays open");
        if matches!(env.payload, Event::Reconnected { .. }) {
            break env;
        }
    };
    assert!(env.sub.is_some());
    ud.shutdown(ShutdownMode::Immediate).await;
}
