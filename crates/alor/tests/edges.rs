//! Edges of the user data connector: stop orders found by their cid, refusals that name the reason, the ledger of
//! `reconcile` with stop orders, and the small parts of the trait surface.

mod mock_server;

use std::sync::Arc;

use brokapi_alor::limits::default_limits;
use brokapi_alor::util::instrument_id;
use brokapi_alor::{UserDataConfig, UserDataConnector};
use brokapi_alor_protocol::config::AlorConfig;
use brokapi_core::{
    BrokerOrderId, ClientOrderId, Connector, Decimal, Envelope, Error, OrderExecutor, OrderType,
    PlaceOrderRequest, PlaceStopOrderRequest, PortfolioSource, Price, Qty, ReplaceOrderRequest,
    ShutdownMode, Side, StopExpiry, StopOrderExecutor, StopOrderKind, StopOrderRef, dec,
};
use brokapi_runtime::BrokerLimiter;
use mock_server::{MockOptions, MockServer};
use tokio::sync::mpsc;

async fn ud(server: &MockServer) -> (UserDataConnector, mpsc::Receiver<Envelope>) {
    let (tx, rx) = mpsc::channel(256);
    let cfg = AlorConfig::for_mock("mock-refresh-token", &server.http_base);
    let ud = UserDataConnector::start(
        UserDataConfig::new(cfg, "alor-ud"),
        BrokerLimiter::new(&default_limits()),
        tx,
    )
    .await
    .expect("ud start");
    (ud, rx)
}

fn limit(qty: i64, price: Decimal) -> PlaceOrderRequest {
    PlaceOrderRequest {
        cid: ClientOrderId::new_v4(),
        instrument: instrument_id("SBER", "TQBR"),
        side: Side::Buy,
        order_type: OrderType::Limit,
        qty: Qty::from(qty),
        price: Some(Price::new(price)),
    }
}

fn stop(price: Decimal) -> PlaceStopOrderRequest {
    PlaceStopOrderRequest {
        cid: ClientOrderId::new_v4(),
        instrument: instrument_id("SBER", "TQBR"),
        side: Side::Sell,
        kind: StopOrderKind::StopLoss,
        qty: Qty::from(10),
        stop_price: Price::new(price),
        limit_price: None,
        expiry: StopExpiry::GoodTillCancel,
    }
}

#[tokio::test]
async fn the_connector_names_its_portfolio_and_answers_through_the_connector_trait() {
    let server = MockServer::start(Arc::default()).await;
    let (ud, _rx) = ud(&server).await;
    assert_eq!(ud.portfolio_id(), brokapi_mocks::alor::PORTFOLIO);
    // The same through the trait that every connector has.
    let caps = Connector::caps(&ud);
    assert!(caps.supports_replace && caps.supports_stop_orders);
    Connector::shutdown(ud, ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn positions_and_balances_are_the_two_halves_of_the_portfolio() {
    let server = MockServer::start(Arc::default()).await;
    let (ud, _rx) = ud(&server).await;
    let whole = ud.portfolio().await.unwrap();
    let key = |p: &brokapi_core::Position| (p.instrument.key.clone(), p.qty);
    assert_eq!(
        ud.positions()
            .await
            .unwrap()
            .iter()
            .map(key)
            .collect::<Vec<_>>(),
        whole.positions.iter().map(key).collect::<Vec<_>>()
    );
    assert_eq!(ud.balances().await.unwrap().len(), whole.balances.len());
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn the_stop_order_stream_does_not_exist_and_the_error_says_so() {
    let server = MockServer::start(Arc::default()).await;
    let (ud, _rx) = ud(&server).await;
    assert!(matches!(
        ud.subscribe_stop_orders().await,
        Err(Error::Unsupported { ref capability }) if capability == "stop_order_stream"
    ));
    ud.shutdown(ShutdownMode::Immediate).await;
}

/// A second program that never saw the stop order finds it by the cid in its comment, and cancels it.
#[tokio::test]
async fn a_stop_order_of_an_earlier_run_is_found_by_its_cid_and_cancelled() {
    let server = MockServer::start(Arc::default()).await;
    let (first, _rx1) = ud(&server).await;
    first.reconcile(&[]).await.unwrap();
    let req = stop(dec!(90));
    let cid = req.cid;
    first.place_stop(req).await.unwrap();
    first.shutdown(ShutdownMode::Immediate).await;

    let (second, _rx2) = ud(&server).await;
    second.reconcile(&[]).await.unwrap();
    // `cancel` of the plain order does look through the stop orders when the cid is not among the plain ones.
    second.cancel(cid).await.expect("the stop order is found");
    assert!(second.active_stop_orders().await.unwrap().is_empty());
    second.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn cancelling_what_the_broker_does_not_have_is_a_refusal_not_a_failure() {
    let server = MockServer::start(Arc::default()).await;
    let (ud, _rx) = ud(&server).await;
    ud.reconcile(&[]).await.unwrap();
    let unknown = ClientOrderId::new_v4();
    assert!(matches!(
        ud.cancel(unknown).await,
        Err(Error::Rejected { ref code, .. }) if code == brokapi_core::NOT_FOUND
    ));
    assert!(matches!(
        ud.cancel_stop(StopOrderRef::Cid(unknown)).await,
        Err(Error::Rejected { ref code, ref message })
            if code == brokapi_core::NOT_FOUND && message.contains("stop order")
    ));
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn a_stop_order_is_cancelled_by_the_id_of_the_broker_whether_or_not_this_process_placed_it() {
    let server = MockServer::start(Arc::default()).await;
    let (first, _rx1) = ud(&server).await;
    first.reconcile(&[]).await.unwrap();
    let mine = first.place_stop(stop(dec!(90))).await.unwrap();
    let theirs = first.place_stop(stop(dec!(91))).await.unwrap();
    // The id of an order this process placed is known with its cid.
    first
        .cancel_stop(StopOrderRef::Broker(mine.broker_id.clone()))
        .await
        .unwrap();
    first.shutdown(ShutdownMode::Immediate).await;
    // Another process has never seen it: the cid is made up from the id.
    let (second, _rx2) = ud(&server).await;
    second.reconcile(&[]).await.unwrap();
    second
        .cancel_stop(StopOrderRef::Broker(theirs.broker_id.clone()))
        .await
        .unwrap();
    // An id nobody has is the refusal of the broker.
    assert!(
        second
            .cancel_stop(StopOrderRef::Broker(BrokerOrderId::new("1")))
            .await
            .is_err()
    );
    second.shutdown(ShutdownMode::Immediate).await;
}

/// The ledger of a bot holds the stop orders too: a working one is confirmed, one that is gone is closed.
#[tokio::test]
async fn reconcile_sorts_the_stop_orders_of_the_ledger() {
    let server = MockServer::start(Arc::default()).await;
    let (ud, _rx) = ud(&server).await;
    ud.reconcile(&[]).await.unwrap();
    let working = stop(dec!(90));
    let gone = stop(dec!(91));
    let cids = (working.cid, gone.cid);
    ud.place_stop(working).await.unwrap();
    let gone_ack = ud.place_stop(gone).await.unwrap();
    ud.cancel_stop(StopOrderRef::Broker(gone_ack.broker_id))
        .await
        .unwrap();
    let report = ud.reconcile(&[cids.0, cids.1]).await.unwrap();
    assert_eq!(report.confirmed.len(), 1, "{report:?}");
    assert_eq!(report.confirmed[0].cid, cids.0);
    assert_eq!(report.closed_offline, vec![cids.1]);
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn a_partly_filled_order_is_replaced_only_with_a_new_quantity() {
    let server = MockServer::start(Arc::default()).await;
    let (ud, _rx) = ud(&server).await;
    ud.reconcile(&[]).await.unwrap();
    let req = limit(100, dec!(100));
    let cid = req.cid;
    ud.place(req).await.unwrap();
    server.fill(
        &cid.hyphenated().to_string(),
        Qty::from(10),
        Price::from(100),
    );
    brokapi_mocks::time::wait_until_async("the fill is seen", || async {
        ud.active_orders()
            .await
            .map(|o| o.iter().any(|s| s.filled.is_positive()))
            .unwrap_or(false)
    })
    .await;
    let replace = |qty: Option<i64>| ReplaceOrderRequest {
        original_cid: cid,
        cid: ClientOrderId::new_v4(),
        qty: qty.map(Qty::from),
        price: Some(Price::new(dec!(101))),
    };
    assert!(matches!(
        ud.replace(replace(None)).await,
        Err(Error::Rejected { ref code, .. }) if code == "qty_required"
    ));
    ud.replace(replace(Some(100)))
        .await
        .expect("a new quantity is a new order");
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn an_instrument_of_another_exchange_is_refused_with_the_name_of_the_exchange() {
    let opts = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&opts)).await;
    let (ud, _rx) = ud(&server).await;
    ud.reconcile(&[]).await.unwrap();
    let mut req = limit(10, dec!(100));
    req.instrument = instrument_id("SBER", "SPBX");
    match ud.place(req).await {
        Err(Error::Config(e)) => assert!(e.to_string().contains("start another connector"), "{e}"),
        other => panic!("{other:?}"),
    }
    assert_eq!(
        opts.order_commands
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    ud.shutdown(ShutdownMode::Immediate).await;
}

// --- market data ---

mod market_data {
    use super::*;
    use brokapi_alor::{MarketDataConfig, MarketDataConnector};
    use brokapi_core::{
        InstrumentKind, InstrumentQuery, InstrumentSource, MarketDataSource, SubscribeOpts,
    };

    async fn md(server: &MockServer) -> (MarketDataConnector, mpsc::Receiver<Envelope>) {
        let (tx, rx) = mpsc::channel(256);
        let cfg = AlorConfig::for_mock("mock-refresh-token", &server.http_base);
        let md = MarketDataConnector::start(
            MarketDataConfig::new(cfg, "alor-md"),
            BrokerLimiter::new(&default_limits()),
            tx,
        )
        .await
        .expect("md start");
        (md, rx)
    }

    #[tokio::test]
    async fn the_capabilities_are_the_same_through_the_connector_trait() {
        let server = MockServer::start(Arc::default()).await;
        let (md, _rx) = md(&server).await;
        let caps = md.caps();
        assert_eq!(Connector::caps(&md).streaming, caps.streaming);
        assert!(caps.streaming.order_book && caps.streaming.market_trades);
        Connector::shutdown(md, ShutdownMode::Immediate).await;
    }

    #[tokio::test]
    async fn instruments_are_listed_by_kind_and_the_unsupported_queries_say_so() {
        let server = MockServer::start(Arc::default()).await;
        let (md, _rx) = md(&server).await;
        let stocks = md
            .find(InstrumentQuery::ByKind(InstrumentKind::Stock))
            .await
            .unwrap();
        let tickers: Vec<_> = stocks.iter().map(|i| i.id.key.symbol.as_str()).collect();
        assert_eq!(tickers, ["SBER", "GAZP"]);
        let futures = md
            .find(InstrumentQuery::ByKind(InstrumentKind::Future))
            .await
            .unwrap();
        assert_eq!(futures.len(), 1);
        assert!(matches!(
            md.find(InstrumentQuery::ByKind(InstrumentKind::Other)).await,
            Err(Error::Unsupported { ref capability }) if capability == "find_other_by_kind"
        ));
        assert!(matches!(
            md.find(InstrumentQuery::ByIsin("RU0009029540".into()))
                .await,
            Err(Error::Unsupported { .. })
        ));
        assert_eq!(
            md.get(&instrument_id("SBER", "TQBR"))
                .await
                .unwrap()
                .id
                .key
                .symbol,
            "SBER"
        );
        md.shutdown(ShutdownMode::Immediate).await;
    }

    #[tokio::test]
    async fn a_graceful_shutdown_cancels_the_subscriptions_at_the_broker_and_says_how_many() {
        let server = MockServer::start(Arc::default()).await;
        let (md, _rx) = md(&server).await;
        md.subscribe_market_trades(&instrument_id("SBER", "TQBR"), SubscribeOpts::conflate())
            .await
            .unwrap();
        let report = md
            .shutdown(ShutdownMode::Graceful {
                deadline: std::time::Duration::from_secs(1),
            })
            .await;
        assert_eq!(report.unsubscribed, 1, "{report:?}");
    }
}
