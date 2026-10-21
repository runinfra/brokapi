//! The methods of `BcsHttpClient` that name a path, against a server that only records what it receives:
//! the method and path, the body, and the `Authorization` header are what the broker's docs say.
//! The answers are the smallest valid ones. The shape of the broker's real answers is checked elsewhere
//! (`rest_models`, the imitation of the broker, the connectors).

mod common;

use brokapi_bcs_protocol::config::BcsConfig;
use brokapi_bcs_protocol::error::BcsApiError;
use brokapi_bcs_protocol::http::client::{AUTH_TOKEN_PATH, BcsHttpClient};
use brokapi_bcs_protocol::http::models::*;
use brokapi_bcs_protocol::models::enums::OrderSide;
use chrono::{TimeZone, Utc};
use common::{Server, serve};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use uuid::Uuid;

const ID: &str = "550e8400-e29b-41d4-a716-446655440000";

fn json(path_and_body: &[(&str, &str)]) -> Server {
    let table: Vec<(String, String)> = path_and_body
        .iter()
        .map(|(p, b)| (p.to_string(), b.to_string()))
        .collect();
    serve(move |line, _auth, _n| {
        if line.contains(AUTH_TOKEN_PATH) {
            return (
                200,
                r#"{"access_token":"tok","expires_in":86400,"refresh_expires_in":7776000,"token_type":"bearer"}"#.into(),
            );
        }
        // The first entry whose path is in the request line wins: put the longer paths first.
        for (path, body) in &table {
            if line.contains(path.as_str()) {
                return (200, body.clone());
            }
        }
        (404, r#"{"type":"NOT_FOUND"}"#.into())
    })
}

fn client(server: &Server) -> BcsHttpClient {
    BcsHttpClient::new(BcsConfig::for_mock("refresh", &server.url).with_write(true)).unwrap()
}

const OPS: &str = "/trade-api-bff-operations/api/v1/orders";
const ACK: &str = r#"{"clientOrderId":"550e8400-e29b-41d4-a716-446655440000","status":"OK"}"#;
const STATE: &str = r#"{"clientOrderId":"x","originalClientOrderId":"550e8400-e29b-41d4-a716-446655440000","data":[]}"#;

fn limit() -> CreateOrderRequest {
    CreateOrderRequest::limit(
        Uuid::parse_str(ID).unwrap(),
        "SBER",
        "TQBR",
        OrderSide::Buy,
        1,
        dec!(250.0),
    )
}

#[tokio::test]
async fn the_order_commands_go_to_the_paths_of_the_docs() {
    let server = json(&[
        (&format!("{OPS}/cancel/sync"), STATE),
        (&format!("{OPS}/edit/sync"), STATE),
        (&format!("{OPS}/sync"), STATE),
        (&format!("{OPS}/cancel"), ACK),
        (&format!("{OPS}/edit"), ACK),
        (OPS, ACK),
    ]);
    let c = client(&server);
    let id = Uuid::parse_str(ID).unwrap();

    // The deprecated and the current forms of place, cancel and change.
    assert_eq!(c.create_order(limit()).await.unwrap().client_order_id, id);
    assert_eq!(
        c.cancel_order(
            id,
            CancelOrderRequest {
                client_order_id: Uuid::new_v4()
            }
        )
        .await
        .unwrap()
        .status,
        "OK"
    );
    c.modify_order(
        id,
        ModifyOrderRequest {
            client_order_id: Uuid::new_v4(),
            order_quantity: 2,
            price: Some(dec!(251.5)),
        },
    )
    .await
    .unwrap();
    c.create_order_sync(&limit()).await.unwrap();
    let by_id = CancelByIdRequest::by_client_order_id(id, Uuid::new_v4());
    c.cancel_order_by_id(&by_id).await.unwrap();
    c.cancel_order_by_id_sync(&by_id).await.unwrap();
    let edit = EditOrderRequest {
        order_id_type: Some("2".into()),
        order_id: "O-1".into(),
        client_order_id: Uuid::new_v4(),
        order_type: Some(OrderType::Limit),
        price: Some(dec!(251.5)),
        order_quantity: 3,
        extras: Default::default(),
    };
    c.edit_order(&edit).await.unwrap();
    c.edit_order_sync(&edit).await.unwrap();

    for (what, path, times) in [
        ("place, deprecated", format!("POST {OPS} "), 1),
        ("cancel, deprecated", format!("POST {OPS}/{ID}/cancel "), 1),
        ("modify, deprecated", format!("POST {OPS}/{ID} "), 1),
        ("place sync", format!("POST {OPS}/sync "), 1),
        ("cancel", format!("POST {OPS}/cancel "), 1),
        ("cancel sync", format!("POST {OPS}/cancel/sync "), 1),
        ("edit", format!("POST {OPS}/edit "), 1),
        ("edit sync", format!("POST {OPS}/edit/sync "), 1),
    ] {
        assert_eq!(server.count(&path), times, "{what}: {path}");
    }
    // The price goes as a JSON number with its digits.
    let placed = &server.bodies(&format!("POST {OPS} "))[0];
    assert!(placed.contains(r#""price":250.0"#), "{placed}");
    // Every command carries the access token.
    assert!(
        server
            .seen
            .lock()
            .unwrap()
            .iter()
            .filter(|s| s.line.contains(OPS))
            .all(|s| s.auth == "Bearer tok")
    );
}

#[tokio::test]
async fn a_refused_command_comes_back_as_the_error_of_the_answer() {
    let server = serve(|line, _a, _n| {
        if line.contains(AUTH_TOKEN_PATH) {
            return (200, r#"{"access_token":"t","expires_in":86400,"refresh_expires_in":1,"token_type":"bearer"}"#.into());
        }
        (400, r#"{"type":"VALIDATION_ERROR","traceId":"tr-1","errors":[{"type":"INVALID","field":"price"}]}"#.into())
    });
    let err = client(&server)
        .create_order_sync(&limit())
        .await
        .unwrap_err();
    assert!(
        matches!(err, BcsApiError::ValidationError { .. }),
        "{err:?}"
    );
}

#[tokio::test]
async fn the_reads_go_to_the_paths_of_the_docs() {
    let server = json(&[
        (
            "/orders/search",
            r#"{"records":[],"totalRecords":0,"totalPages":0}"#,
        ),
        (
            "/trades/search",
            r#"{"records":[],"totalRecords":0,"totalPages":0}"#,
        ),
        (
            &format!("{OPS}/{ID}"),
            r#"{"clientOrderId":"x","originalClientOrderId":"y"}"#,
        ),
        ("/orders", STATE),
        ("/portfolio", "[]"),
    ]);
    let c = client(&server);
    let from = Utc.with_ymd_and_hms(2026, 10, 1, 0, 0, 0).unwrap();
    let to = Utc.with_ymd_and_hms(2026, 10, 2, 0, 0, 0).unwrap();

    // The list of orders, with and without a page.
    let search = |tickers| OrderSearchRequest {
        start_date_time: from,
        end_date_time: to,
        side: None,
        order_status: Some(vec![3]),
        order_types: None,
        tickers,
        class_codes: None,
    };
    c.search_orders(search(Some(vec!["SBER".into()])), Some(2), Some(50))
        .await
        .unwrap();
    c.search_orders(search(None), None, None).await.unwrap();
    c.search_orders_simple(Some(vec!["SBER".into()]), 6)
        .await
        .unwrap();
    c.search_orders_simple(None, 1).await.unwrap();
    let lines: Vec<String> = server
        .seen
        .lock()
        .unwrap()
        .iter()
        .map(|s| s.line.clone())
        .collect();
    assert!(
        lines
            .iter()
            .any(|l| l.contains("/orders/search?page=2&size=50")),
        "{lines:?}"
    );
    assert_eq!(
        server.count("POST /trade-api-bff-order-details/api/v1/orders/search"),
        4
    );

    // One order by its exchange id and by the id of the deprecated status call.
    c.get_order("2", "O-1").await.unwrap();
    assert!(server.count("GET /trade-api-bff-operations/api/v1/orders") >= 1);
    let status = c
        .get_order_status(Uuid::parse_str(ID).unwrap())
        .await
        .unwrap();
    assert_eq!(status.original_client_order_id, "y");
    assert_eq!(server.count(&format!("GET {OPS}/{ID} ")), 1);
    assert_eq!(c.get_portfolio().await.unwrap().len(), 0);
}
