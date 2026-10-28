//! The whole test of the page "Тесты против имитаций, Rust" (docs/src/rust/mocks.md): an order placed on the imitation of
//! T-Bank, filled by the test, and the fill seen in the events. The book includes the part between the anchors.

// ANCHOR: test
use std::time::Duration;

use brokapi::core::{
    Decimal, Event, ExecutionSource, InstrumentQuery, InstrumentSource, OrderExecutor, OrderType,
    PlaceOrderRequest, Price, Qty, Side, dec,
};
use brokapi::runtime::BrokerLimiter;
use brokapi::tbank::limits::default_limits;
use brokapi::tbank::protocol::TbankConfig;
use brokapi::tbank::{MarketDataConfig, MarketDataConnector, UserDataConfig, UserDataConnector};
use brokapi_mocks::tbank::{ACCOUNT_A, MockServer, TOKEN};
use tokio::sync::mpsc;

#[tokio::test]
async fn an_order_on_the_imitation_is_filled_and_the_fill_is_an_event() {
    let server = MockServer::start(Default::default()).await;
    let cfg = TbankConfig::for_mock(TOKEN, &server.endpoint).with_account(ACCOUNT_A);
    let limiter = BrokerLimiter::new(&default_limits());
    let (tx, mut events) = mpsc::channel(256);
    let md = MarketDataConnector::start(
        MarketDataConfig::new(cfg.clone(), "md"),
        limiter.clone(),
        tx.clone(),
    )
    .await
    .unwrap();
    let ud = UserDataConnector::start(UserDataConfig::new(cfg, "ud"), limiter, tx)
        .await
        .unwrap();

    let sber = md
        .find(InstrumentQuery::ByTicker {
            ticker: "SBER".into(),
            exchange: None,
        })
        .await
        .unwrap()
        .remove(0)
        .id;
    // The fills come as events only to a subscription: without this line the test waits for nothing.
    ud.subscribe_executions().await.unwrap();
    ud.reconcile(&[]).await.unwrap(); // the gate of the commands opens

    let cid = brokapi::core::ClientOrderId::new_v4();
    ud.place(PlaceOrderRequest {
        cid,
        instrument: sber,
        side: Side::Buy,
        order_type: OrderType::Limit,
        qty: Qty::from(10), // one lot of the imitation, in units of the instrument
        price: Some(Price::new(dec!(250))),
    })
    .await
    .unwrap();
    server.fill(ACCOUNT_A, &cid.to_string()); // the imitation fills it: the price of a mock does not move by itself

    let fill = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(envelope) = events.recv().await
                && let Event::Execution(fill) = envelope.payload
            {
                return fill;
            }
        }
    })
    .await
    .expect("a fill event within 5 seconds");
    assert_eq!(fill.cid, Some(cid));
    assert_eq!(fill.qty.decimal(), Decimal::from(10));
}
// ANCHOR_END: test
