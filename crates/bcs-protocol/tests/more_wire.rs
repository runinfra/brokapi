//! What the paging of instrument searches puts into the address, the methods that read one list, the 401 of a raw read, the
//! parts of the instrument card and the small types of the models.

mod common;

use brokapi_bcs_protocol::config::BcsConfig;
use brokapi_bcs_protocol::error::BcsApiError;
use brokapi_bcs_protocol::http::client::{AUTH_TOKEN_PATH, BcsHttpClient};
use brokapi_bcs_protocol::http::models::*;
use brokapi_bcs_protocol::models::enums::{OrderSide, OrderType as WsOrderType};
use brokapi_bcs_protocol::models::types::Pagination;
use common::{Server, serve};
use uuid::Uuid;

const TOKEN: &str = r#"{"access_token":"tok","expires_in":86400,"refresh_expires_in":7776000,"token_type":"bearer"}"#;

fn client(server: &Server) -> BcsHttpClient {
    BcsHttpClient::new(BcsConfig::for_mock("refresh", &server.url)).unwrap()
}

fn answers(body: &'static str) -> Server {
    serve(move |line, _auth, _n| {
        if line.contains(AUTH_TOKEN_PATH) {
            (200, TOKEN.into())
        } else {
            (200, body.into())
        }
    })
}

fn request_line(server: &Server, needle: &str) -> String {
    server
        .seen
        .lock()
        .unwrap()
        .iter()
        .find(|s| s.line.contains(needle))
        .unwrap_or_else(|| panic!("no request for {needle}"))
        .line
        .clone()
}

#[tokio::test]
async fn the_size_and_the_page_of_a_search_by_isin_or_ticker_go_into_the_address_when_given() {
    let server = answers("[]");
    let c = client(&server);
    let isins = InstrumentsByIsinsRequest {
        isins: vec!["RU0009029540".into()],
    };
    let tickers = InstrumentsByTickersRequest {
        tickers: vec!["SBER".into()],
    };
    c.get_instruments_by_isins(&isins, Some(10), Some(2))
        .await
        .unwrap();
    c.get_instruments_by_isins(&isins, None, None)
        .await
        .unwrap();
    c.get_instruments_by_tickers(&tickers, Some(5), None)
        .await
        .unwrap();
    c.get_instruments_by_tickers(&tickers, None, Some(3))
        .await
        .unwrap();
    let lines: Vec<String> = server
        .seen
        .lock()
        .unwrap()
        .iter()
        .filter(|s| s.line.contains("instruments/by-"))
        .map(|s| s.line.clone())
        .collect();
    assert!(lines[0].contains("?size=10&page=2"), "{}", lines[0]);
    assert!(!lines[1].contains('?'), "{}", lines[1]);
    assert!(lines[2].contains("?size=5 "), "{}", lines[2]);
    assert!(lines[3].contains("?page=3 "), "{}", lines[3]);
}

#[tokio::test]
async fn a_search_by_type_may_name_the_base_asset() {
    let server = answers("[]");
    let req = InstrumentsByTypeRequest {
        instrument_type: "FUTURES".into(),
        base_asset_ticker: Some("SI".into()),
        size: Some(20),
        page: Some(1),
    };
    client(&server).get_instruments_by_type(&req).await.unwrap();
    let line = request_line(&server, "instruments/by-type");
    for part in ["type=FUTURES", "baseAssetTicker=SI", "size=20", "page=1"] {
        assert!(line.contains(part), "{part} is not in {line}");
    }
}

#[tokio::test]
async fn limits_and_discounts_are_one_read_each() {
    let server = answers(r#"{"depoLimit":[],"moneyLimits":[],"futuresLimits":[]}"#);
    let limits = client(&server).get_limits().await.unwrap();
    assert!(limits.depo_limit.is_empty() && limits.money_limits.is_empty());
    let server = answers("[]");
    assert!(
        client(&server)
            .get_instruments_discounts()
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn a_raw_read_renews_the_token_once_after_a_401_and_gives_whatever_comes_back() {
    let server = serve(|line, auth, _n| {
        if line.contains(AUTH_TOKEN_PATH) {
            (200, TOKEN.into())
        } else if line.contains("/some/path") && auth == "Bearer tok" {
            (401, "again".into())
        } else {
            (200, "not reached".into())
        }
    });
    let (status, body) = client(&server).get_raw("/some/path").await.unwrap();
    assert_eq!(status.as_u16(), 401);
    assert_eq!(body, "again");
    assert_eq!(server.count("/some/path"), 2, "one read and one repeat");
    assert_eq!(
        server.count(AUTH_TOKEN_PATH),
        2,
        "the token was renewed once"
    );
}

#[test]
fn the_primary_board_of_a_card_gives_its_trading_parameters() {
    let card = |extra: &str| -> InstrumentInfo {
        serde_json::from_str(&format!(
            r#"{{"ticker":"SBER","boards":[{{"classCode":"TQBR","exchange":"MOEX"}},{{"classCode":"SMAL","exchange":"MOEX"}}],
                "lotSize":10,"minimumStep":0.05 {extra}}}"#
        ))
        .unwrap()
    };
    let with_primary = card(r#","primaryBoard":"SMAL""#)
        .trading_params_primary()
        .unwrap();
    assert_eq!(with_primary.class_code, "SMAL");
    assert_eq!(with_primary.lot_size.to_string(), "10");
    // Without a primary board the first one is the main one, and the missing fields get plain defaults.
    let first = card("").trading_params_primary().unwrap();
    assert_eq!(first.class_code, "TQBR");
    assert_eq!(first.currency, "RUB");
    assert_eq!(first.scale, 2);
    // A board the card does not list has no parameters, and a card with no boards at all has no primary.
    assert!(card("").trading_params_for_board("XXXX").is_none());
    let bare: InstrumentInfo = serde_json::from_str(r#"{"ticker":"X"}"#).unwrap();
    assert!(bare.trading_params_primary().is_none());
}

#[test]
fn a_market_order_has_no_price_and_the_small_types_print_their_names() {
    let order = CreateOrderRequest::market(Uuid::new_v4(), "SBER", "TQBR", OrderSide::Sell, 3);
    assert!(order.price.is_none());
    assert_eq!(order.order_quantity, 3);
    assert_eq!(
        [OrderSide::Buy, OrderSide::Sell, OrderSide::Unknown].map(|s| s.to_string()),
        ["Buy", "Sell", "Unknown"]
    );
    assert_eq!(
        [
            WsOrderType::Market,
            WsOrderType::Limit,
            WsOrderType::Unknown
        ]
        .map(|t| t.to_string()),
        ["Market", "Limit", "Unknown"]
    );
    let first = Pagination::default();
    assert_eq!((first.size, first.page), (50, 0));
    assert_eq!(Pagination::new(100, 2).page, 2);
}

#[test]
fn a_failure_of_the_json_or_of_the_address_is_a_parse_error() {
    let json: BcsApiError = serde_json::from_str::<u8>("x").unwrap_err().into();
    let url: BcsApiError = url::Url::parse("not a url").unwrap_err().into();
    for e in [json, url] {
        assert!(!e.to_string().is_empty());
    }
}
