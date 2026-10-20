//! Shared scenarios from `brokapi-conformance` against the Finam gRPC mock (no external network).

mod mock_server;

use brokapi_conformance as conf;
use brokapi_core::Price;
use brokapi_core::{CidCodec, InstrumentQuery, ShutdownMode, TimeRange, Timeframe, UnixNanos};
use mock_server::{MockServer, md, sber, ud};

#[tokio::test]
async fn market_data() {
    let server = MockServer::start().await;
    let (md, mut rx) = md(&server).await;
    conf::order_book_snapshot(&md, &sber(), 20).await;
    let sub = conf::order_book_stream(&md, &mut rx, "finam-md", &sber()).await;
    conf::unsubscribe(&md, sub).await;
    md.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn instruments() {
    let server = MockServer::start().await;
    let (md, _rx) = md(&server).await;
    let query = InstrumentQuery::ByTicker {
        ticker: "SBER".into(),
        exchange: None,
    };
    conf::instrument_lookup(&md, query, &sber()).await;
    // With an exchange it is a single GetAsset instead of a directory scan.
    let query = InstrumentQuery::ByTicker {
        ticker: "SBER".into(),
        exchange: Some("MISX".into()),
    };
    conf::instrument_lookup(&md, query, &sber()).await;
    md.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn orders() {
    let server = MockServer::start().await;
    let (ud, _rx) = ud(&server, "ACC1").await;
    conf::reconcile(&ud).await;
    conf::place_and_cancel(&ud, &sber(), Price::from(1)).await;
    conf::rejected_order_does_not_block_the_instrument(&ud, &sber(), Price::from(1)).await;
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn stop_orders() {
    let server = MockServer::start().await;
    let (ud, _rx) = ud(&server, "ACC1").await;
    conf::reconcile(&ud).await;
    assert!(ud.caps().supports_stop_orders);
    conf::stop_order_far_from_market(&ud, &sber(), Price::from(1)).await;
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn portfolio() {
    let server = MockServer::start().await;
    let (ud, mut rx) = ud(&server, "ACC1").await;
    conf::portfolio(&ud, &mut rx, "finam-ud-ACC1").await;
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn market_data_history_and_streams() {
    let server = MockServer::start().await;
    let (md, mut rx) = md(&server).await;
    let candles = TimeRange::new(
        UnixNanos::from_nanos(1_700_000_000 * 1_000_000_000),
        UnixNanos::from_nanos(1_700_003_600 * 1_000_000_000),
    );
    // The mock's schedule is built around "now".
    let now = brokapi_finam::util::now_nanos().as_nanos();
    let around_now = TimeRange::new(
        UnixNanos::from_nanos(now - 10_800_000_000_000),
        UnixNanos::from_nanos(now + 10_800_000_000_000),
    );
    // Finam schedules are per instrument: "TICKER@MIC".
    conf::market_data_history(
        &md,
        &sber(),
        Timeframe::M1,
        candles,
        "SBER@MISX",
        around_now,
    )
    .await;
    conf::market_data_streams(&md, &mut rx, "finam-md", &sber(), Timeframe::M5).await;
    md.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn own_orders_events_active_replace_and_executions() {
    let server = MockServer::start().await;
    let (ud, mut rx) = ud(&server, "ACC1").await;
    conf::reconcile(&ud).await;
    conf::order_state_events(&ud, &mut rx, "finam-ud-ACC1", &sber(), Price::from(1)).await;
    conf::active_orders_lifecycle(&ud, &sber(), Price::from(1)).await;
    // Finam has no replace method: the connector says so and leaves the order alone.
    assert!(!ud.caps().supports_replace);
    conf::replace_order(&ud, false, &sber(), Price::from(1), Price::from(2)).await;
    conf::executions(
        &ud,
        &mut rx,
        "finam-ud-ACC1",
        &sber(),
        Price::from(1),
        |cid| {
            let server = &server;
            async move {
                // The cid travels in the order's comment; find our order by it.
                let comment = brokapi_finam::cid::FinamCidCodec
                    .encode(cid)
                    .unwrap()
                    .as_str()
                    .to_string();
                let order = server
                    .state
                    .orders_of("ACC1")
                    .into_iter()
                    .find(|o| o.order.as_ref().is_some_and(|x| x.comment == comment))
                    .expect("our order at the mock");
                // The trades stream is opened by a background task: wait until the server has it.
                brokapi_mocks::time::wait_until("the trades stream is open", || {
                    server.state.count_calls("SubscribeTrades") > 0
                })
                .await;
                let dec = |v: &str| {
                    Some(brokapi_finam_protocol::pb::google::r#type::Decimal { value: v.into() })
                };
                server.state.push_trade(
                    "ACC1",
                    brokapi_finam_protocol::pb::grpc::tradeapi::v1::AccountTrade {
                        trade_id: "TR-CONF".into(),
                        symbol: "SBER@MISX".into(),
                        price: dec("1"),
                        size: dec("2"),
                        side: brokapi_finam_protocol::pb::grpc::tradeapi::v1::Side::Buy as i32,
                        order_id: order.order_id.clone(),
                        comment,
                        account_id: "ACC1".into(),
                        ..Default::default()
                    },
                );
            }
        },
    )
    .await;
    ud.shutdown(ShutdownMode::Immediate).await;
}
