/// Unit-тесты — не требуют сети, запускаются всегда
///
/// cargo test --test unit_tests
use brokapi_bcs_protocol::config::BcsConfig;
use brokapi_bcs_protocol::models::instrument::InstrumentKey;
use brokapi_bcs_protocol::ws::client::BcsWsChannel;
use brokapi_bcs_protocol::ws::models::MarketDataSubscribe;
use brokapi_bcs_protocol::ws::models::WsOutgoing;

// ─── helpers ────────────────────────────────────────────────────────────────

fn instruments(n: usize) -> Vec<InstrumentKey> {
    (0..n)
        .map(|i| InstrumentKey::new(format!("TICK{}", i), "TQBR"))
        .collect()
}

fn quotes_sub() -> MarketDataSubscribe {
    MarketDataSubscribe::quotes(instruments(1))
}

// ─── MarketDataSubscribe ─────────────────────────────────────────────────────

#[test]
fn test_into_unsubscribe_flips_subscribe_type() {
    let sub = MarketDataSubscribe::quotes(instruments(2));
    assert_eq!(sub.subscribe_type, 0);

    let unsub = sub.into_unsubscribe();
    assert_eq!(unsub.subscribe_type, 1);
}

#[test]
fn test_into_unsubscribe_preserves_fields() {
    let instrs = instruments(3);
    let sub = MarketDataSubscribe::order_book(instrs.clone(), 20);
    let unsub = sub.into_unsubscribe();

    assert_eq!(unsub.data_type, 0);
    assert_eq!(unsub.depth, Some(20));
    assert_eq!(unsub.instruments, instrs);
}

#[test]
fn test_last_candle_unsubscribe_preserves_time_frame() {
    let sub = MarketDataSubscribe::last_candle(instruments(1), "M5");
    let unsub = sub.into_unsubscribe();

    assert_eq!(unsub.subscribe_type, 1);
    assert_eq!(unsub.time_frame.as_deref(), Some("M5"));
}

#[test]
fn test_subscribe_type_values() {
    assert_eq!(
        MarketDataSubscribe::quotes(instruments(1)).subscribe_type,
        0
    );
    assert_eq!(
        MarketDataSubscribe::order_book(instruments(1), 5).subscribe_type,
        0
    );
    assert_eq!(
        MarketDataSubscribe::last_trades(instruments(1)).subscribe_type,
        0
    );
    assert_eq!(
        MarketDataSubscribe::last_candle(instruments(1), "M1").subscribe_type,
        0
    );
}

#[test]
fn test_unsubscribe_constructors() {
    assert_eq!(
        MarketDataSubscribe::unsubscribe_quotes(instruments(1)).subscribe_type,
        1
    );
    assert_eq!(
        MarketDataSubscribe::unsubscribe_order_book(instruments(1), 5).subscribe_type,
        1
    );
    assert_eq!(
        MarketDataSubscribe::unsubscribe_last_trades(instruments(1)).subscribe_type,
        1
    );
    assert_eq!(
        MarketDataSubscribe::unsubscribe_last_candle(instruments(1), "M1").subscribe_type,
        1
    );
}

// ─── WsOutgoing serialization ────────────────────────────────────────────────

#[test]
fn test_ws_outgoing_subscribe_serializes_untagged() {
    let sub = MarketDataSubscribe::quotes(instruments(1));
    let outgoing = WsOutgoing::Subscribe(sub);
    let json = serde_json::to_string(&outgoing).unwrap();

    // #[serde(untagged)] — не должно быть обёртки {"Subscribe": ...}
    assert!(!json.contains("\"Subscribe\""));
    assert!(json.contains("\"subscribeType\""));
    assert!(json.contains("\"dataType\""));
}

#[test]
fn test_ws_outgoing_unsubscribe_serializes_subscribe_type_1() {
    let sub = MarketDataSubscribe::quotes(instruments(1)).into_unsubscribe();
    let outgoing = WsOutgoing::Unsubscribe(sub);
    let json = serde_json::to_string(&outgoing).unwrap();

    assert!(json.contains("\"subscribeType\":1"));
}

#[test]
fn test_ws_outgoing_partial_eq() {
    let a = WsOutgoing::Subscribe(quotes_sub());
    let b = WsOutgoing::Subscribe(quotes_sub());
    let c = WsOutgoing::Unsubscribe(quotes_sub());

    assert_eq!(a, b);
    assert_ne!(a, c);
}

// ─── BcsConfig ───────────────────────────────────────────────────────────────

#[test]
fn test_bcs_config_new_defaults() {
    let cfg = BcsConfig::new("my_token".to_string());

    assert_eq!(cfg.refresh_token, "my_token");
    assert!(!cfg.is_write, "is_write should default to false");
    assert_eq!(cfg.request_timeout.as_secs(), 30);
    assert!(cfg.http_base_url.contains("be.broker.ru"));
    assert!(cfg.ws_base_url.contains("ws.broker.ru"));
}

#[test]
fn test_bcs_config_from_env_missing_token() {
    // from_env() вызывает dotenv() который может подгрузить .env файл,
    // поэтому тестируем напрямую через VarError без dotenv
    let result = std::env::var("DEFINITELY_MISSING_VAR_XYZ_123");
    assert!(result.is_err(), "Missing env var should return Err");
}

#[test]
fn test_bcs_config_from_env_with_token() {
    unsafe {
        std::env::set_var("BCS_REFRESH_TOKEN", "test_refresh_token");
        // Override values from workspace .env (dotenv loads before env::var).
        std::env::set_var("BCS_IS_WRITE", "false");
    }
    let cfg = BcsConfig::from_env().unwrap();
    assert_eq!(cfg.refresh_token, "test_refresh_token");
    assert!(!cfg.is_write);
    unsafe {
        std::env::remove_var("BCS_REFRESH_TOKEN");
        std::env::remove_var("BCS_IS_WRITE");
    }
}

// ─── BcsWsChannel ────────────────────────────────────────────────────────────

#[test]
fn test_ws_channel_variants_distinct() {
    let channels = [
        BcsWsChannel::Limits,
        BcsWsChannel::Portfolio,
        BcsWsChannel::MarketData,
        BcsWsChannel::OrderExecution,
        BcsWsChannel::OrderTransaction,
        BcsWsChannel::MarginalIndicators,
    ];

    // Все варианты попарно различны
    for i in 0..channels.len() {
        for j in 0..channels.len() {
            if i != j {
                assert_ne!(channels[i], channels[j]);
            }
        }
    }
}

#[test]
fn test_ws_channel_market_data_is_not_limits() {
    assert_ne!(BcsWsChannel::MarketData, BcsWsChannel::Limits);
    assert_ne!(BcsWsChannel::MarketData, BcsWsChannel::Portfolio);
}

// ─── WsSession subscribe/unsubscribe (через публичный API) ──────────────────

#[tokio::test]
async fn test_session_subscribe_wrong_channel_returns_error() {
    // Создаём сессию на Limits — подписки не поддерживаются
    let config = BcsConfig::new("dummy".to_string());
    let ws_client = brokapi_bcs_protocol::ws::client::BcsWsClient::new(config).unwrap();
    let (event_tx, _event_rx) = kanal::bounded_async(128);
    let session = ws_client.connect_limits(event_tx);

    let result = session.subscribe(quotes_sub()).await;
    assert!(result.is_err());

    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("non-MarketData"),
        "Error should mention channel: {}",
        msg
    );
}

#[tokio::test]
async fn test_session_unsubscribe_wrong_channel_returns_error() {
    let config = BcsConfig::new("dummy".to_string());
    let ws_client = brokapi_bcs_protocol::ws::client::BcsWsClient::new(config).unwrap();
    let (event_tx, _event_rx) = kanal::bounded_async(128);
    let session = ws_client.connect_portfolio(event_tx);

    let result = session.unsubscribe(quotes_sub()).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_session_subscribe_limit_exceeded() {
    let config = BcsConfig::new("dummy".to_string());
    let ws_client = brokapi_bcs_protocol::ws::client::BcsWsClient::new(config).unwrap();
    let (event_tx, _event_rx) = kanal::bounded_async(256);
    let session = ws_client.connect_market_data(event_tx);

    // user_tx канал = 128, WS_MAX_SUBSCRIPTIONS = 100
    // Все 100 send() пройдут без блокировки
    for i in 0..100 {
        let sub =
            MarketDataSubscribe::quotes(vec![InstrumentKey::new(format!("T{:03}", i), "TQBR")]);
        session
            .subscribe(sub)
            .await
            .expect("subscribe should succeed");
    }

    let over_limit = MarketDataSubscribe::quotes(vec![InstrumentKey::new("OVERLIMIT", "TQBR")]);
    let result = session.subscribe(over_limit).await;
    assert!(result.is_err());
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("limit") || msg.contains("100"),
        "Expected limit error: {}",
        msg
    );
}

// ─── JSON fixtures (level-1 parsing, no network) ─────────────────────────────

fn d(s: &str) -> rust_decimal::Decimal {
    s.parse().unwrap()
}

fn fixture(name: &str) -> String {
    let path = format!("{}/tests/fixtures/{}", env!("CARGO_MANIFEST_DIR"), name);
    std::fs::read_to_string(path).expect("fixture file")
}

#[test]
fn test_parse_rest_order_book_fixture() {
    use brokapi_bcs_protocol::http::models::OrderBookResponse;

    let book: OrderBookResponse = serde_json::from_str(&fixture("rest/order_book.json")).unwrap();
    assert_eq!(book.ticker, "SBER");
    assert_eq!(book.class_code, "TQBR");
    assert_eq!(book.bids.len(), 2);
    assert_eq!(book.asks.len(), 2);
    assert_eq!(book.bids[0].price, d("244.30"));
    // The number is read as written: the trailing zero is not lost on the way.
    assert_eq!(book.bids[0].price.to_string(), "244.30");
}

#[test]
fn test_parse_rest_last_trades_fixture() {
    use brokapi_bcs_protocol::http::models::LastTradesResponse;

    let trades: LastTradesResponse =
        serde_json::from_str(&fixture("rest/last_trades.json")).unwrap();
    assert_eq!(trades.len(), 2);
    assert_eq!(trades[0].side.as_deref(), Some("BUY"));
}

#[test]
fn test_instrument_info_trading_params_helper() {
    use brokapi_bcs_protocol::http::models::InstrumentInfo;

    let info: InstrumentInfo = serde_json::from_str(&fixture("rest/instrument_info.json")).unwrap();
    let params = info
        .trading_params_for_board("TQBR")
        .expect("trading params");
    assert_eq!(params.ticker, "SBER");
    assert_eq!(params.lot_size, d("1"));
    assert_eq!(params.price_step, d("0.01"));
    assert_eq!(params.currency, "RUB");
}

#[test]
fn test_parse_ws_order_book_fixture() {
    use brokapi_bcs_protocol::ws::models::MarketDataMessage;

    let msg: MarketDataMessage = serde_json::from_str(&fixture("ws/order_book.json")).unwrap();
    match msg {
        MarketDataMessage::OrderBook(data) => {
            assert_eq!(data.ticker, "SBER");
            assert_eq!(data.bids.len(), 2);
        }
        other => panic!("expected OrderBook, got {:?}", other),
    }
}

#[test]
fn test_parse_ws_quotes_fixture() {
    use brokapi_bcs_protocol::ws::models::MarketDataMessage;

    let msg: MarketDataMessage = serde_json::from_str(&fixture("ws/quotes.json")).unwrap();
    match msg {
        MarketDataMessage::Quotes(data) => {
            assert_eq!(data.ticker, "SBER");
            assert_eq!(data.bid, Some(d("244.30")));
        }
        other => panic!("expected Quotes, got {:?}", other),
    }
}

#[test]
fn test_parse_ws_order_book_error_fixture() {
    use brokapi_bcs_protocol::ws::models::MarketDataError;

    let err: MarketDataError = serde_json::from_str(&fixture("ws/order_book_error.json")).unwrap();
    assert_eq!(err.errors.len(), 1);
    assert_eq!(err.errors[0].code, "INCORRECT_JSON");
}

#[test]
fn test_create_order_request_cid_is_caller_supplied() {
    use brokapi_bcs_protocol::http::models::CreateOrderRequest;
    use brokapi_bcs_protocol::models::enums::OrderSide;
    use uuid::Uuid;

    let cid = Uuid::new_v4();
    let req = CreateOrderRequest::limit(cid, "SBER", "TQBR", OrderSide::Buy, 1, d("100.0"));
    assert_eq!(req.client_order_id, cid);
}

// ─── fixes found while documenting the models ────────────────────────────────

#[test]
fn portfolio_profit_fields_use_the_capital_letters_of_the_docs() {
    // The broker docs spell these four fields with capital PL: `unrealizedPL`, not `unrealizedPl`.
    let body = r#"{"type":"depoLimit","unrealizedPL":40.0,"unrealizedPercentPL":1.5,"dailyPL":2.0,"dailyPercentPL":0.1}"#;
    let rest: brokapi_bcs_protocol::http::models::PortfolioPosition =
        serde_json::from_str(body).unwrap();
    assert_eq!(rest.unrealized_pl, Some(d("40.0")));
    assert_eq!(rest.unrealized_percent_pl, Some(1.5));
    assert_eq!(rest.daily_pl, Some(d("2.0")));
    assert_eq!(rest.daily_percent_pl, Some(0.1));
    let ws: brokapi_bcs_protocol::ws::models::WsPortfolioPosition =
        serde_json::from_str(body).unwrap();
    assert_eq!(ws.unrealized_pl, Some(d("40.0")));
    assert_eq!(ws.daily_percent_pl, Some(0.1));

    // The spelling the code used before is still read.
    let old = r#"{"type":"depoLimit","unrealizedPl":7.0,"dailyPercentPl":0.3}"#;
    let ws_old: brokapi_bcs_protocol::ws::models::WsPortfolioPosition =
        serde_json::from_str(old).unwrap();
    assert_eq!(ws_old.unrealized_pl, Some(d("7.0")));
    assert_eq!(ws_old.daily_percent_pl, Some(0.3));
}

#[test]
fn quotes_frame_reads_offer_and_last_of_the_docs_and_the_older_names() {
    use brokapi_bcs_protocol::ws::models::QuotesData;

    let docs: QuotesData = serde_json::from_str(
        r#"{"ticker":"SBER","classCode":"TQBR","bid":305.86,"offer":305.96,"last":305.97}"#,
    )
    .unwrap();
    assert_eq!(
        (docs.ask, docs.last_price),
        (Some(d("305.96")), Some(d("305.97")))
    );

    let old: QuotesData = serde_json::from_str(&fixture("ws/quotes.json")).unwrap();
    assert_eq!(
        (old.ask, old.last_price),
        (Some(d("244.35")), Some(d("244.32")))
    );
}

#[tokio::test]
async fn a_long_non_ascii_reply_that_does_not_parse_is_an_error_not_a_panic() {
    use brokapi_bcs_protocol::error::BcsApiError;
    use brokapi_bcs_protocol::http::client::{AUTH_TOKEN_PATH, BcsHttpClient};

    let mut server = mockito::Server::new_async().await;
    let _auth = server
        .mock("POST", AUTH_TOKEN_PATH)
        .with_header("content-type", "application/json")
        .with_body(
            r#"{"access_token":"a","expires_in":1,"refresh_expires_in":1,"token_type":"bearer"}"#,
        )
        .create_async()
        .await;
    // One ASCII byte, then two-byte characters: byte 200 falls inside a character.
    let _page = server
        .mock("GET", "/page")
        .with_status(200)
        .with_body(format!("a{}", "ж".repeat(150)))
        .create_async()
        .await;

    let client = BcsHttpClient::new(BcsConfig::for_mock("refresh", server.url())).unwrap();
    let err = client.get::<serde_json::Value>("/page").await.unwrap_err();
    assert!(matches!(err, BcsApiError::ParseError(_)), "got {err:?}");
}
