//! The HTTP and WebSocket clients against the imitation of the exchange (no network).

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use brokapi_bybit_protocol::config::BybitConfig;
use brokapi_bybit_protocol::error::BybitApiError;
use brokapi_bybit_protocol::http::client::BybitHttpClient;
use brokapi_bybit_protocol::http::models::{CancelOrderRequest, CreateOrderRequest};
use brokapi_bybit_protocol::ws::models::{DataKind, WsIncoming};
use brokapi_bybit_protocol::ws::session::{WsOptions, WsSession, WsStream};
use brokapi_mocks::bybit::{DEFAULT_KEY, MockOptions, MockServer, SECRET};

async fn server() -> (Arc<MockOptions>, MockServer, BybitHttpClient) {
    let opts = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&opts)).await;
    let http = BybitHttpClient::new(BybitConfig::for_mock(
        DEFAULT_KEY,
        SECRET,
        &server.http_base,
    ))
    .unwrap();
    (opts, server, http)
}

fn limit(link: &str, price: &str) -> CreateOrderRequest {
    CreateOrderRequest {
        category: "linear".into(),
        symbol: "MOCKUSDT".into(),
        side: "Buy".into(),
        order_type: "Limit".into(),
        qty: "10".parse().unwrap(),
        price: Some(price.parse().unwrap()),
        order_link_id: Some(link.into()),
        ..Default::default()
    }
}

#[tokio::test]
async fn a_signed_read_passes_and_a_wrong_secret_or_key_is_refused() {
    let (_o, server, http) = server().await;
    assert!(!http.get_wallet_balance().await.unwrap().is_empty());
    let wrong_secret = BybitHttpClient::new(BybitConfig::for_mock(
        DEFAULT_KEY,
        "other-secret",
        &server.http_base,
    ))
    .unwrap();
    match wrong_secret.get_wallet_balance().await {
        Err(BybitApiError::Auth { message }) => assert!(message.starts_with("10004"), "{message}"),
        other => panic!("{other:?}"),
    }
    let wrong_key = BybitHttpClient::new(BybitConfig::for_mock(
        "someone-else",
        SECRET,
        &server.http_base,
    ))
    .unwrap();
    match wrong_key.get_wallet_balance().await {
        Err(BybitApiError::Auth { message }) => assert!(message.starts_with("10003"), "{message}"),
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn a_clock_that_runs_off_fails_the_window_until_the_offset_is_read() {
    let (opts, _server, http) = server().await;
    opts.clock_offset_ms.store(120_000, Ordering::SeqCst);
    match http.get_wallet_balance().await {
        Err(BybitApiError::Api { code, message }) => {
            assert_eq!(code, 10002);
            assert!(message.contains("recv_window"));
        }
        other => panic!("{other:?}"),
    }
    let offset = http.sync_time().await.unwrap();
    assert!(
        (offset - 120_000).abs() < 2_000,
        "the offset of the exchange clock is found: {offset}"
    );
    http.get_wallet_balance()
        .await
        .expect("the corrected timestamp is inside the window");
}

#[tokio::test]
async fn a_post_signs_the_body_it_sends_and_the_query_of_a_get_as_written() {
    let (opts, _server, http) = server().await;
    let r = http.create_order(&limit("link-1", "1.00")).await.unwrap();
    assert!(!r.order_id.is_empty());
    assert_eq!(r.order_link_id, "link-1");
    // A second order with the same link id is refused by the exchange with its own code.
    match http.create_order(&limit("link-1", "1.00")).await {
        Err(BybitApiError::Api { code, .. }) => assert_eq!(code, 110072),
        other => panic!("{other:?}"),
    }
    let page = http
        .get_open_orders("linear", None, Some("USDT"), None, Some("link-1"), 50, None)
        .await
        .unwrap();
    assert_eq!(page.list.len(), 1);
    assert_eq!(page.list[0].price.unwrap().to_string(), "1.00");
    http.cancel_order(&CancelOrderRequest {
        category: "linear".into(),
        symbol: "MOCKUSDT".into(),
        order_id: Some(r.order_id),
        order_link_id: None,
    })
    .await
    .unwrap();
    assert_eq!(opts.served_count("POST /v5/order/create"), 2);
    assert_eq!(opts.served_count("POST /v5/order/cancel"), 1);
}

#[tokio::test]
async fn the_lists_have_pages_and_the_cursor_goes_back_as_it_came() {
    let (_o, _server, http) = server().await;
    for i in 0..3 {
        http.create_order(&limit(&format!("p{i}"), "1.00"))
            .await
            .unwrap();
    }
    let first = http
        .get_open_orders("linear", None, Some("USDT"), None, None, 2, None)
        .await
        .unwrap();
    assert_eq!(first.list.len(), 2);
    assert!(!first.next_page_cursor.is_empty());
    let second = http
        .get_open_orders(
            "linear",
            None,
            Some("USDT"),
            None,
            None,
            2,
            Some(&first.next_page_cursor),
        )
        .await
        .unwrap();
    assert_eq!(second.list.len(), 1);
    assert!(second.next_page_cursor.is_empty());
    let all = http
        .cancel_all("linear", Some("MOCKUSDT"), "USDT")
        .await
        .unwrap();
    assert_eq!(all.list.len(), 3);
}

#[tokio::test]
async fn the_public_reads_need_no_key() {
    let (_o, server, _http) = server().await;
    let anonymous = BybitHttpClient::new(BybitConfig::for_mock("", "", &server.http_base)).unwrap();
    let book = anonymous
        .get_order_book("linear", "MOCKUSDT", Some(3))
        .await
        .unwrap();
    assert_eq!(book.bids.len(), 3);
    let info = anonymous
        .get_instrument("linear", "MOCKUSDT")
        .await
        .unwrap();
    assert_eq!(info.lot_size_filter.qty_step.to_string(), "10");
    let klines = anonymous
        .get_klines(
            "linear",
            "MOCKUSDT",
            "60",
            Some(0),
            Some(3_600_000 * 5),
            1000,
        )
        .await
        .unwrap();
    assert!(
        klines.windows(2).all(|w| w[0].start_ms < w[1].start_ms),
        "oldest first"
    );
    match anonymous.get_ticker("linear", "NOPEUSDT").await {
        Err(BybitApiError::Api {
            code: 10001,
            message,
        }) => assert!(message.contains("symbol invalid")),
        other => panic!("{other:?}"),
    }
    // A private read without a signature is a 401.
    match anonymous.get_wallet_balance().await {
        Err(BybitApiError::Auth { .. }) => {}
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn a_rate_limited_read_is_told_apart() {
    let (opts, _server, http) = server().await;
    opts.enable_rate_limit_order_book();
    assert!(matches!(
        http.get_order_book("linear", "MOCKUSDT", None).await,
        Err(BybitApiError::RateLimitExceeded)
    ));
}

async fn next(rx: &kanal::AsyncReceiver<WsIncoming>) -> WsIncoming {
    tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("a frame within 5 seconds")
        .expect("the channel is open")
}

async fn next_data(rx: &kanal::AsyncReceiver<WsIncoming>, topic: &str) -> WsIncoming {
    loop {
        let f = next(rx).await;
        if matches!(&f, WsIncoming::Data { topic: t, .. } if t == topic) {
            return f;
        }
    }
}

fn fast() -> WsOptions {
    WsOptions {
        announce_connects: true,
        reconnect_min: Duration::from_millis(10),
        reconnect_max: Duration::from_millis(20),
    }
}

#[tokio::test]
async fn a_public_session_subscribes_gets_the_answer_and_the_data() {
    let (_o, _server, http) = server().await;
    let (tx, rx) = kanal::bounded_async(64);
    let ws = WsSession::spawn(http, WsStream::Public, tx, fast());
    ws.subscribe("r1", "orderbook.50.MOCKUSDT").await.unwrap();
    assert_eq!(next(&rx).await, WsIncoming::Connected);
    match next(&rx).await {
        WsIncoming::Ack {
            op,
            req_id,
            success,
            ..
        } => {
            assert_eq!(
                (op.as_str(), req_id.as_deref(), success),
                ("subscribe", Some("r1"), true)
            );
        }
        other => panic!("{other:?}"),
    }
    let WsIncoming::Data { kind, data, .. } = next_data(&rx, "orderbook.50.MOCKUSDT").await else {
        unreachable!()
    };
    assert_eq!(kind, DataKind::Snapshot);
    assert_eq!(data["s"], "MOCKUSDT");
    // A topic the exchange does not have is refused with the reason and the request id.
    ws.subscribe("r2", "orderbook.7.MOCKUSDT").await.unwrap();
    match next(&rx).await {
        WsIncoming::Ack {
            req_id,
            success: false,
            message,
            ..
        } => {
            assert_eq!(req_id.as_deref(), Some("r2"));
            assert!(message.contains("handler not found"));
        }
        other => panic!("{other:?}"),
    }
    assert!(ws.is_connected());
    assert_eq!(ws.dropped_events(), 0);
}

#[tokio::test]
async fn a_cut_link_is_made_again_and_the_subscriptions_come_back() {
    let (opts, server, http) = server().await;
    let (tx, rx) = kanal::bounded_async(64);
    let ws = WsSession::spawn(http, WsStream::Public, tx, fast());
    ws.subscribe("r1", "publicTrade.MOCKUSDT").await.unwrap();
    next_data(&rx, "publicTrade.MOCKUSDT").await;
    let before = opts.ws_connections();
    server.cut_public_sockets();
    // The marker of the new connection comes before the data of the restored subscription.
    loop {
        if next(&rx).await == WsIncoming::Connected {
            break;
        }
    }
    next_data(&rx, "publicTrade.MOCKUSDT").await;
    assert!(opts.ws_connections() > before);
    // A subscription that was cancelled is not restored.
    ws.unsubscribe("publicTrade.MOCKUSDT").await.unwrap();
    server.cut_public_sockets();
    loop {
        if next(&rx).await == WsIncoming::Connected {
            break;
        }
    }
    ws.subscribe("r9", "kline.1.MOCKUSDT").await.unwrap();
    // Only the kline arrives: no trade frame comes before it.
    loop {
        match next(&rx).await {
            WsIncoming::Data { topic, .. } => {
                assert_eq!(topic, "kline.1.MOCKUSDT");
                break;
            }
            _ => continue,
        }
    }
}

#[tokio::test]
async fn the_private_session_logs_in_and_gets_the_events_of_its_account() {
    let (_o, _server, http) = server().await;
    let (tx, rx) = kanal::bounded_async(64);
    let ws = WsSession::spawn(http.clone(), WsStream::Private, tx, fast());
    ws.subscribe("r1", "order.linear").await.unwrap();
    assert_eq!(next(&rx).await, WsIncoming::Connected);
    assert!(matches!(
        next(&rx).await,
        WsIncoming::Ack { success: true, .. }
    ));
    http.create_order(&limit("w-1", "1.00")).await.unwrap();
    let WsIncoming::Data { kind, data, .. } = next_data(&rx, "order.linear").await else {
        unreachable!()
    };
    assert_eq!(kind, DataKind::Event);
    assert_eq!(data[0]["orderLinkId"], "w-1");
    assert_eq!(data[0]["orderStatus"], "New");
}

#[tokio::test]
async fn a_refused_login_is_told_to_the_reader_and_retried() {
    let (opts, _server, http) = server().await;
    opts.refuse_ws_login.store(true, Ordering::SeqCst);
    let (tx, rx) = kanal::bounded_async(64);
    let _ws = WsSession::spawn(http, WsStream::Private, tx, fast());
    match next(&rx).await {
        WsIncoming::Ack {
            op,
            success: false,
            message,
            ..
        } => {
            assert_eq!(op, "auth");
            assert!(!message.is_empty());
        }
        other => panic!("{other:?}"),
    }
    // The session tries again; when the exchange accepts, the connection is announced.
    opts.refuse_ws_login.store(false, Ordering::SeqCst);
    loop {
        if next(&rx).await == WsIncoming::Connected {
            break;
        }
    }
}
