//! Every channel of the client against the imitation, the subscriptions the client offers, the limits of a session and
//! the frames the parser cannot use.

use std::sync::Arc;
use std::time::Duration;

use brokapi_bcs_protocol::config::BcsConfig;
use brokapi_bcs_protocol::error::BcsApiError;
use brokapi_bcs_protocol::models::instrument::InstrumentKey;
use brokapi_bcs_protocol::models::messages::IncomingMessage;
use brokapi_bcs_protocol::ws::client::{BcsWsChannel, BcsWsClient};
use brokapi_bcs_protocol::ws::models::{MarketDataMessage, MarketDataSubscribe};
use brokapi_bcs_protocol::ws::parse_incoming;
use brokapi_bcs_protocol::ws::session::{WsOptions, WsSession};
use brokapi_mocks::bcs::{MockOptions, MockServer};
use brokapi_mocks::time::wait_until;

fn sber() -> Vec<InstrumentKey> {
    vec![InstrumentKey::new("SBER", "TQBR")]
}

async fn client(server: &MockServer) -> BcsWsClient {
    BcsWsClient::new(BcsConfig::for_mock("mock-refresh-token", &server.http_base)).unwrap()
}

#[test]
fn text_that_is_not_json_is_kept_as_it_came_on_every_channel() {
    for channel in [
        BcsWsChannel::Limits,
        BcsWsChannel::Portfolio,
        BcsWsChannel::MarketData,
        BcsWsChannel::OrderExecution,
        BcsWsChannel::OrderTransaction,
        BcsWsChannel::OrderEvents,
        BcsWsChannel::MarginalIndicators,
    ] {
        assert!(
            matches!(parse_incoming("oops", channel), IncomingMessage::Unknown(t) if t == "oops"),
            "{channel:?}"
        );
    }
}

#[tokio::test]
async fn the_client_opens_a_session_for_each_channel_it_offers() {
    let server = MockServer::start(Arc::default()).await;
    let ws = client(&server).await;
    let (tx, _rx) = kanal::bounded_async::<IncomingMessage>(64);
    let sessions = [
        (
            ws.connect_order_transaction(tx.clone()),
            BcsWsChannel::OrderTransaction,
        ),
        (
            ws.connect_order_execution(tx.clone()),
            BcsWsChannel::OrderExecution,
        ),
        (
            ws.connect_marginal_indicators(tx.clone()),
            BcsWsChannel::MarginalIndicators,
        ),
        (ws.connect_limits(tx.clone()), BcsWsChannel::Limits),
        (
            ws.connect_order_events(tx.clone()),
            BcsWsChannel::OrderEvents,
        ),
        (ws.connect_portfolio(tx.clone()), BcsWsChannel::Portfolio),
    ];
    for (session, channel) in &sessions {
        assert_eq!(session.channel(), *channel);
        session.wait_connected().await;
        assert!(session.is_connected());
        assert_eq!(session.dropped_events(), 0);
    }
    let _ = ws.http_client();
}

#[tokio::test]
async fn the_client_subscribes_to_every_kind_of_market_data() {
    let opts = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&opts)).await;
    let ws = client(&server).await.with_options(WsOptions::default());
    let (tx, _rx) = kanal::bounded_async::<IncomingMessage>(256);
    let md = ws.connect_market_data(tx);
    ws.subscribe_quotes(&md, sber()).await.unwrap();
    ws.subscribe_order_book(&md, sber(), 5).await.unwrap();
    ws.subscribe_last_trades(&md, sber()).await.unwrap();
    ws.subscribe_last_candle(&md, sber(), "M1").await.unwrap();
    wait_until("four commands reach the broker", || {
        opts.ws_commands.lock().unwrap().len() >= 4
    })
    .await;
    let kinds: Vec<_> = opts
        .ws_commands
        .lock()
        .unwrap()
        .iter()
        .map(|c| serde_json::from_str::<serde_json::Value>(c).unwrap()["dataType"].as_i64())
        .collect();
    assert_eq!(kinds, [Some(3), Some(0), Some(2), Some(1)]);
    // Only the market data channel takes subscriptions, and it takes them away too.
    let (tx2, _rx2) = kanal::bounded_async::<IncomingMessage>(8);
    let portfolio = ws.connect_portfolio(tx2);
    for result in [
        ws.subscribe_quotes(&portfolio, sber()).await,
        portfolio
            .unsubscribe(MarketDataSubscribe::quotes(sber()))
            .await,
    ] {
        assert!(
            matches!(result, Err(BcsApiError::SubscriptionError { .. })),
            "{result:?}"
        );
    }
    md.unsubscribe(MarketDataSubscribe::quotes(sber()))
        .await
        .unwrap();
}

#[tokio::test]
async fn a_session_whose_task_has_ended_refuses_subscriptions_and_a_full_channel_counts_drops() {
    let server = MockServer::start(Arc::default()).await;
    let ws = client(&server).await;
    // Nobody reads and the channel has one place: what does not fit is dropped and counted.
    let (tx, rx) = kanal::bounded_async::<IncomingMessage>(1);
    let md = ws.connect_market_data(tx);
    for _ in 0..3 {
        ws.subscribe_quotes(&md, sber()).await.unwrap();
    }
    wait_until("frames are dropped", || md.dropped_events() > 0).await;
    // The reader leaves: the session ends at the next frame, and then it refuses.
    drop(rx);
    wait_until_refused(&ws, &md).await;
}

async fn wait_until_refused(ws: &BcsWsClient, md: &WsSession) {
    brokapi_mocks::time::wait_until_async("the session has ended", || async {
        ws.subscribe_quotes(md, sber()).await.is_err()
    })
    .await;
    assert!(!md.is_connected());
    assert!(
        md.unsubscribe(MarketDataSubscribe::quotes(sber()))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn a_cut_socket_is_connected_again_and_the_subscription_is_restored() {
    let opts = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&opts)).await;
    let ws = client(&server).await.with_options(WsOptions {
        announce_connects: true,
        reconnect_min: Duration::from_millis(10),
        reconnect_max: Duration::from_millis(20),
    });
    let (tx, rx) = kanal::bounded_async::<IncomingMessage>(64);
    let md = ws.connect_market_data(tx);
    ws.subscribe_order_book(&md, sber(), 5).await.unwrap();
    // The imitation closes the socket after the first book: the session connects again and restores the subscription.
    opts.disconnect_after_data_frames
        .store(1, std::sync::atomic::Ordering::Relaxed);
    wait_until("a second connection", || opts.ws_connections() >= 2).await;
    let mut connects = 0;
    let mut books = 0;
    while connects < 2 || books < 2 {
        match rx.recv().await.unwrap() {
            IncomingMessage::Reconnected => connects += 1,
            IncomingMessage::MarketData(MarketDataMessage::OrderBook(_)) => books += 1,
            _ => {}
        }
    }
}

// --- the parser ---

#[test]
fn a_frame_that_fits_no_shape_of_its_channel_stays_text() {
    for channel in [
        BcsWsChannel::Portfolio,
        BcsWsChannel::Limits,
        BcsWsChannel::OrderExecution,
        BcsWsChannel::OrderTransaction,
        BcsWsChannel::OrderEvents,
        BcsWsChannel::MarginalIndicators,
        BcsWsChannel::MarketData,
    ] {
        for text in ["[1,2,3]", "\"just text\""] {
            match parse_incoming(text, channel) {
                IncomingMessage::Unknown(t) => assert_eq!(t, text, "{channel:?}"),
                other => panic!("{channel:?} read {text} as {other:?}"),
            }
        }
    }
}

#[test]
fn market_data_that_does_not_fit_its_type_is_kept_as_text() {
    let text = r#"{"responseType":"Quotes","price":"x"}"#;
    assert!(matches!(
        parse_incoming(text, BcsWsChannel::MarketData),
        IncomingMessage::Unknown(t) if t == text
    ));
}

#[tokio::test]
async fn a_link_that_stayed_silent_for_two_minutes_is_dropped_and_opened_again() {
    let opts = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&opts)).await;
    let ws = client(&server).await.with_options(WsOptions {
        announce_connects: true,
        reconnect_min: Duration::from_millis(10),
        reconnect_max: Duration::from_millis(20),
    });
    let (tx, _rx) = kanal::bounded_async::<IncomingMessage>(64);
    let md = ws.connect_market_data(tx);
    md.wait_connected().await;
    assert_eq!(opts.ws_connections(), 1);
    // The watchdog counts the silence on the clock of the session: more than a minute without a pong or a ping.
    brokapi_mocks::time::jump(Duration::from_secs(120)).await;
    wait_until("a second connection", || opts.ws_connections() >= 2).await;
    md.wait_connected().await;
}
