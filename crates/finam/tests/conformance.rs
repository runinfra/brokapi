//! Shared scenarios from `brokapi-conformance` against the Finam gRPC mock (no external network).

mod mock_server;

use brokapi_conformance as conf;
use brokapi_core::Price;
use brokapi_core::{CidCodec, InstrumentQuery, ShutdownMode, TimeRange, Timeframe, UnixNanos};
use mock_server::{MockServer, api, limiter, md, sber, ud, ud_with};

#[tokio::test]
async fn market_data() {
    let server = MockServer::start().await;
    let (md, mut rx) = md(&server).await;
    conf::order_book_snapshot(&md, &sber(), 20).await;
    let sub = conf::order_book_stream(&md, &mut rx, "finam-md", &sber()).await;
    conf::unsubscribe(&md, sub).await;
    conf::unsubscribe_unknown(&md).await;
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
    conf::order_state_unknown_cid(&ud).await;
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

// ─── guarantees of the command path ───────────────────────────────────────────

use std::sync::atomic::Ordering;
use std::time::Duration;

use brokapi_core::{Error, InstrumentId, InstrumentKey, NativeId, Qty};
use brokapi_finam::UserDataConfig;

/// A connector whose lot is known to the grid.
async fn ud_lot(
    server: &MockServer,
    request_timeout: Option<Duration>,
) -> (
    brokapi_finam::UserDataConnector,
    tokio::sync::mpsc::Receiver<brokapi_core::Envelope>,
) {
    let mut cfg = UserDataConfig::new(api(server), "ACC1", "finam-ud-ACC1");
    if let Some(t) = request_timeout {
        cfg.api.request_timeout = t;
    }
    let (ud, rx) = ud_with(cfg, limiter(), 256).await;
    ud.price_grid().set_lot(sber().key, Qty::from(conf::LOT));
    (ud, rx)
}

#[tokio::test]
async fn a_cid_is_placed_once() {
    let server = MockServer::start().await;
    let (ud, _rx) = ud_lot(&server, None).await;
    conf::reconcile(&ud).await;
    conf::duplicate_place(&ud, &sber(), Price::from(1)).await;
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn a_quantity_that_is_not_whole_lots_is_refused() {
    let server = MockServer::start().await;
    let (ud, _rx) = ud_lot(&server, None).await;
    conf::reconcile(&ud).await;
    conf::invalid_qty(&ud, &sber(), Price::from(1)).await;
    // The four refusals sent nothing: one order reached the broker.
    assert_eq!(server.state.count_calls("PlaceOrder"), 1);
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn an_id_of_another_broker_is_a_config_error() {
    let server = MockServer::start().await;
    let (ud, _rx) = ud_lot(&server, None).await;
    conf::reconcile(&ud).await;
    let foreign = InstrumentId::new(
        InstrumentKey::new("SBER", "TQBR"),
        NativeId::TickerClass {
            ticker: "SBER".into(),
            class_code: "TQBR".into(),
        },
    );
    conf::foreign_instrument_id(&ud, &foreign, Price::from(1)).await;
    assert_eq!(server.state.count_calls("PlaceOrder"), 0);
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn an_unknown_instrument_is_not_found() {
    let server = MockServer::start().await;
    let (md, _rx) = md(&server).await;
    let nope = InstrumentId::new(
        InstrumentKey::new("NOPE", "MISX"),
        NativeId::SymbolMic {
            symbol: "NOPE".into(),
            mic: "MISX".into(),
        },
    );
    conf::unknown_instrument(&md, &nope).await;
    md.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn a_place_without_an_answer_is_unknown() {
    let server = MockServer::start().await;
    let (ud, _rx) = ud_lot(&server, Some(Duration::from_millis(250))).await;
    conf::reconcile(&ud).await;
    server
        .state
        .options
        .place_delay_ms
        .store(900, Ordering::SeqCst);
    conf::place_without_answer_is_unknown(
        &ud,
        &sber(),
        Price::from(1),
        |call| {
            brokapi_mocks::time::elapse_when(
                "the order reached the mock",
                || server.state.count_calls("PlaceOrder") == 1,
                Duration::from_millis(300),
                call,
            )
        },
        || async {
            brokapi_mocks::time::jump(Duration::from_millis(700)).await;
        },
    )
    .await;
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn a_cut_of_the_order_streams_degrades_until_reconcile() {
    let server = MockServer::start().await;
    let (ud, mut rx) = ud_lot(&server, None).await;
    conf::reconcile(&ud).await;
    conf::stream_cut_degrades_until_reconcile(&ud, &mut rx, &sber(), Price::from(1), || async {
        server.state.cut_user_streams();
        // The connector pauses before it reopens: move the clock until both streams are open again.
        brokapi_mocks::time::jump_until("the streams are back", Duration::from_secs(1), || {
            server.state.order_stream_opens.load(Ordering::SeqCst) >= 2
                && server.state.count_calls("SubscribeTrades") >= 2
        })
        .await;
    })
    .await;
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn the_gate_is_checked_again_after_the_queue() {
    let server = MockServer::start().await;
    let (ud, mut rx) = ud_lot(&server, Some(Duration::from_secs(120))).await;
    conf::reconcile(&ud).await;
    // The first order is held for 20 s of the test clock: longer than the jumps that reopen the streams.
    server
        .state
        .options
        .place_delay_ms
        .store(20_000, Ordering::SeqCst);
    conf::gate_checked_after_the_queue(
        &ud,
        &mut rx,
        &sber(),
        Price::from(1),
        || async {
            brokapi_mocks::time::wait_until("the first order is at the mock", || {
                server.state.count_calls("PlaceOrder") == 1
            })
            .await;
        },
        || async {
            let orders = server.state.order_stream_opens.load(Ordering::SeqCst);
            server.state.cut_user_streams();
            brokapi_mocks::time::jump_until(
                "the stream is back",
                Duration::from_millis(600),
                || server.state.order_stream_opens.load(Ordering::SeqCst) > orders,
            )
            .await;
        },
        || async {
            server
                .state
                .options
                .place_delay_ms
                .store(0, Ordering::SeqCst);
            brokapi_mocks::time::jump(Duration::from_secs(21)).await;
        },
    )
    .await;
    assert_eq!(server.state.count_calls("PlaceOrder"), 1);
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn the_order_book_is_resynced_after_a_cut() {
    let server = MockServer::start().await;
    let (md, mut rx) = md(&server).await;
    conf::order_book_resync(&md, &mut rx, &sber(), || {
        server
            .state
            .options
            .book_drop_after
            .store(1, Ordering::SeqCst);
    })
    .await;
    md.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn the_history_of_trades_is_paged_not_cut_at_the_limit() {
    let server = MockServer::start().await;
    let mut cfg = UserDataConfig::new(api(&server), "ACC1", "finam-ud-ACC1");
    cfg.executions_limit = 100;
    let (ud, _rx) = ud_with(cfg, limiter(), 256).await;
    let dec =
        |v: &str| Some(brokapi_finam_protocol::pb::google::r#type::Decimal { value: v.into() });
    for n in 0..250 {
        server.state.push_trade(
            "ACC1",
            brokapi_finam_protocol::pb::grpc::tradeapi::v1::AccountTrade {
                trade_id: format!("T{n}"),
                symbol: "SBER@MISX".into(),
                price: dec("1"),
                size: dec("10"),
                side: brokapi_finam_protocol::pb::grpc::tradeapi::v1::Side::Buy as i32,
                // One trade per second: the pages can be told apart by time.
                timestamp: Some(prost_types::Timestamp {
                    seconds: 1_700_000_000 + n,
                    nanos: 0,
                }),
                account_id: "ACC1".into(),
                ..Default::default()
            },
        );
    }
    conf::executions_history_is_complete(&ud, 250).await;
    // 100 + 100 + 100 (the last page holds the 50 left and the 1 repeated at the edge of the period).
    assert_eq!(server.state.count_calls("Trades"), 3);
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn trades_that_cannot_be_paged_are_an_error_not_a_cut_list() {
    use brokapi_core::ExecutionSource;
    let server = MockServer::start().await;
    let mut cfg = UserDataConfig::new(api(&server), "ACC1", "finam-ud-ACC1");
    cfg.executions_limit = 10;
    let (ud, _rx) = ud_with(cfg, limiter(), 256).await;
    let dec =
        |v: &str| Some(brokapi_finam_protocol::pb::google::r#type::Decimal { value: v.into() });
    // 30 trades at the same second: no time to cut the period at.
    for n in 0..30 {
        server.state.push_trade(
            "ACC1",
            brokapi_finam_protocol::pb::grpc::tradeapi::v1::AccountTrade {
                trade_id: format!("T{n}"),
                symbol: "SBER@MISX".into(),
                price: dec("1"),
                size: dec("10"),
                side: brokapi_finam_protocol::pb::grpc::tradeapi::v1::Side::Buy as i32,
                timestamp: Some(prost_types::Timestamp {
                    seconds: 1_700_000_000,
                    nanos: 0,
                }),
                account_id: "ACC1".into(),
                ..Default::default()
            },
        );
    }
    let range = TimeRange::new(UnixNanos::ZERO, UnixNanos::from_nanos(i64::MAX / 2));
    match ud.executions(range).await {
        Err(Error::Rejected { code, .. }) => assert_eq!(code, "range_too_long"),
        other => panic!("{other:?}"),
    }
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn trades_are_paged_when_the_broker_sends_the_newest_first() {
    let server = MockServer::start().await;
    server
        .state
        .options
        .trades_newest_first
        .store(true, Ordering::SeqCst);
    let mut cfg = UserDataConfig::new(api(&server), "ACC1", "finam-ud-ACC1");
    cfg.executions_limit = 100;
    let (ud, _rx) = ud_with(cfg, limiter(), 256).await;
    let dec =
        |v: &str| Some(brokapi_finam_protocol::pb::google::r#type::Decimal { value: v.into() });
    for n in 0..250 {
        server.state.push_trade(
            "ACC1",
            brokapi_finam_protocol::pb::grpc::tradeapi::v1::AccountTrade {
                trade_id: format!("T{n}"),
                symbol: "SBER@MISX".into(),
                price: dec("1"),
                size: dec("10"),
                side: brokapi_finam_protocol::pb::grpc::tradeapi::v1::Side::Buy as i32,
                timestamp: Some(prost_types::Timestamp {
                    seconds: 1_700_000_000 + n,
                    nanos: 0,
                }),
                account_id: "ACC1".into(),
                ..Default::default()
            },
        );
    }
    conf::executions_history_is_complete(&ud, 250).await;
    assert_eq!(server.state.count_calls("Trades"), 3);
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn trades_without_a_time_cannot_be_paged() {
    use brokapi_core::ExecutionSource;
    let server = MockServer::start().await;
    let mut cfg = UserDataConfig::new(api(&server), "ACC1", "finam-ud-ACC1");
    cfg.executions_limit = 5;
    let (ud, _rx) = ud_with(cfg, limiter(), 256).await;
    let dec =
        |v: &str| Some(brokapi_finam_protocol::pb::google::r#type::Decimal { value: v.into() });
    for n in 0..8 {
        server.state.push_trade(
            "ACC1",
            brokapi_finam_protocol::pb::grpc::tradeapi::v1::AccountTrade {
                trade_id: format!("T{n}"),
                symbol: "SBER@MISX".into(),
                price: dec("1"),
                size: dec("10"),
                side: brokapi_finam_protocol::pb::grpc::tradeapi::v1::Side::Buy as i32,
                account_id: "ACC1".into(),
                ..Default::default()
            },
        );
    }
    // A full answer with no times in it: there is nothing to cut the period at.
    let range = TimeRange::new(UnixNanos::ZERO, UnixNanos::from_nanos(i64::MAX / 2));
    match ud.executions(range).await {
        Err(Error::Rejected { code, .. }) => assert_eq!(code, "range_too_long"),
        other => panic!("{other:?}"),
    }
    ud.shutdown(ShutdownMode::Immediate).await;
}
