//! Разбор и отправка REST-методов, добавленных по официальной схеме (без сети):
//! `POST /api/v1/quotes` и `POST /api/v1/operations/search`.
//! Примеры ответов собраны по схемам из документации БКС (поля и типы), не снятые с живого API.

use brokapi_bcs_protocol::config::BcsConfig;
use brokapi_bcs_protocol::http::client::{AUTH_TOKEN_PATH, BcsHttpClient};
use brokapi_bcs_protocol::http::models::{
    OperationHistoryRequest, OperationHistoryResponse, QuoteInstrument, QuotesRequest,
    QuotesResponse,
};
use mockito::Matcher;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

#[test]
fn quotes_request_serializes_like_the_docs_example() {
    let req = QuotesRequest {
        instruments: vec![QuoteInstrument::new("SBER", "TQBR")],
    };
    let json = serde_json::to_value(&req).unwrap();
    assert_eq!(
        json,
        serde_json::json!({"instruments": [{"ticker": "SBER", "classCode": "TQBR"}]})
    );
}

#[test]
fn quotes_response_parses_full_and_sparse_records() {
    let body = r#"{"records":[
        {"ticker":"SBER","classCode":"TQBR","dateTime":"2026-10-19T10:00:00Z","securityTradingStatus":17,
         "currency":"SUR","bid":300.1,"offer":300.2,"open":299.0,"close":298.5,"high":301.0,"low":298.0,
         "last":300.15,"change":1.65,"changeRate":0.55},
        {"ticker":"RU000A0JX0J2","classCode":"TQCB","bidYield":14.2,"offerYield":14.1,"theoreticalPrice":99.5}
    ]}"#;
    let resp: QuotesResponse = serde_json::from_str(body).unwrap();
    assert_eq!(resp.records.len(), 2);
    let sber = &resp.records[0];
    assert_eq!(sber.ticker.as_deref(), Some("SBER"));
    assert_eq!(sber.security_trading_status, Some(17));
    assert_eq!(sber.bid, Some(dec!(300.1)));
    assert_eq!(sber.offer, Some(dec!(300.2)));
    assert_eq!(sber.last, Some(dec!(300.15)));
    assert_eq!(sber.change_rate, Some(0.55));
    let bond = &resp.records[1];
    assert_eq!(bond.bid_yield, Some(14.2));
    assert_eq!(bond.theoretical_price, Some(dec!(99.5)));
    assert_eq!(bond.last, None);
    // An empty answer is an empty list, not an error.
    let empty: QuotesResponse = serde_json::from_str("{}").unwrap();
    assert!(empty.records.is_empty());
}

#[test]
fn operation_history_request_skips_unset_filters() {
    let req = OperationHistoryRequest {
        operation_types: Some(vec!["PayIn".into(), "Dividend".into()]),
        statuses: Some(vec!["Approved".into()]),
        start_date_time: Some("2026-10-01T00:00:00Z".into()),
        ..Default::default()
    };
    let json = serde_json::to_value(&req).unwrap();
    assert_eq!(
        json,
        serde_json::json!({
            "operationTypes": ["PayIn", "Dividend"],
            "statuses": ["Approved"],
            "startDateTime": "2026-10-01T00:00:00Z"
        })
    );
    assert_eq!(
        serde_json::to_value(OperationHistoryRequest::default()).unwrap(),
        serde_json::json!({})
    );
}

#[test]
fn operation_history_response_parses() {
    let body = r#"{"pageSize":50,"records":[
        {"id":"7d444840-9dc0-11d1-b245-5ffdce74fad2","date":"2026-10-02T09:00:00Z","isIia":false,
         "type":"Dividend","status":"Approved","sum":1234.56,"currency":"SUR","ticker":"SBER","classCode":"TQBR",
         "isin":"RU0009029540","issuerName":"Sberbank","balanceChange":"Positive"},
        {"type":"PayOut","status":"Rejected","sum":500,"isRestsPayOut":true,"rejectionReason":"no funds",
         "balanceChange":"Neutral"}
    ]}"#;
    let resp: OperationHistoryResponse = serde_json::from_str(body).unwrap();
    assert_eq!(resp.page_size, Some(50));
    assert_eq!(resp.records.len(), 2);
    let div = &resp.records[0];
    assert_eq!(div.operation_type.as_deref(), Some("Dividend"));
    assert_eq!(div.sum, Some(dec!(1234.56)));
    assert_eq!(div.balance_change.as_deref(), Some("Positive"));
    assert_eq!(div.is_iia, Some(false));
    let out = &resp.records[1];
    assert_eq!(out.status.as_deref(), Some("Rejected"));
    assert_eq!(out.sum, Some(dec!(500.0)));
    assert_eq!(out.is_rests_pay_out, Some(true));
    assert_eq!(out.rejection_reason.as_deref(), Some("no funds"));
}

#[tokio::test]
async fn client_sends_both_methods_to_the_documented_paths() {
    let mut server = mockito::Server::new_async().await;
    let _auth = server
        .mock("POST", AUTH_TOKEN_PATH)
        .with_header("content-type", "application/json")
        .with_body(
            r#"{"access_token":"at","expires_in":300,"refresh_expires_in":1800,"refresh_token":"rt","token_type":"Bearer"}"#,
        )
        .create_async()
        .await;
    let quotes = server
        .mock("POST", "/trade-api-market-data-connector/api/v1/quotes")
        .match_header("authorization", "Bearer at")
        .match_body(Matcher::JsonString(
            r#"{"instruments":[{"ticker":"SBER","classCode":"TQBR"}]}"#.into(),
        ))
        .with_header("content-type", "application/json")
        .with_body(r#"{"records":[{"ticker":"SBER","classCode":"TQBR","last":300.0}]}"#)
        .create_async()
        .await;
    let ops = server
        .mock(
            "POST",
            "/trade-api-bff-nontrade-operations/api/v1/operations/search",
        )
        .match_query(Matcher::AllOf(vec![
            Matcher::UrlEncoded("size".into(), "20".into()),
            Matcher::UrlEncoded("page".into(), "1".into()),
        ]))
        .match_header("authorization", "Bearer at")
        .with_header("content-type", "application/json")
        .with_body(
            r#"{"pageSize":20,"records":[{"type":"PayIn","status":"Approved","sum":100.0}]}"#,
        )
        .create_async()
        .await;

    let client = BcsHttpClient::new(BcsConfig::for_mock("refresh", server.url())).unwrap();
    let q = client
        .get_quotes(&QuotesRequest {
            instruments: vec![QuoteInstrument::new("SBER", "TQBR")],
        })
        .await
        .unwrap();
    assert_eq!(q.records[0].last, Some(dec!(300.0)));
    let h = client
        .search_operations(&OperationHistoryRequest::default(), Some(20), Some(1))
        .await
        .unwrap();
    assert_eq!(h.records[0].operation_type.as_deref(), Some("PayIn"));
    quotes.assert_async().await;
    ops.assert_async().await;
}

#[test]
fn conditional_order_serializes_the_documented_fields() {
    use brokapi_bcs_protocol::http::models::{CreateOrderRequest, OrderExtras, OrderType};
    use brokapi_bcs_protocol::models::enums::OrderSide;
    use uuid::Uuid;
    let mut req =
        CreateOrderRequest::limit(Uuid::nil(), "SBER", "TQBR", OrderSide::Sell, 3, dec!(250.0));
    req.order_type = OrderType::Conditional;
    req.extras = OrderExtras {
        stop_price: Some(dec!(240.0)),
        execution_stop_loss_type: Some(2),
        take_price: Some(dec!(300.0)),
        execution_take_profit_type: Some(1),
        ..Default::default()
    };
    let v = serde_json::to_value(&req).unwrap();
    assert_eq!(v["orderType"], "5");
    assert_eq!(v["stopPrice"], 240.0);
    assert_eq!(v["takePrice"], 300.0);
    assert_eq!(v["executionStopLossType"], 2);
    assert_eq!(v["executionTakeProfitType"], 1);
    assert!(
        v.get("timeInForce").is_none() && v.get("visible").is_none(),
        "unset extras are omitted"
    );
    // A plain limit order serializes exactly as before.
    let plain = serde_json::to_value(CreateOrderRequest::limit(
        Uuid::nil(),
        "SBER",
        "TQBR",
        OrderSide::Buy,
        1,
        dec!(100.0),
    ))
    .unwrap();
    assert_eq!(plain.as_object().unwrap().len(), 7, "{plain}");
}

#[test]
fn order_state_list_parses_with_a_list_in_data() {
    use brokapi_bcs_protocol::http::models::OrderStateListResponse;
    let body = r#"{"clientOrderId":"11111111-1111-1111-1111-111111111111","originalClientOrderId":"22222222-2222-2222-2222-222222222222",
        "data":[{"messageType":"8","orderStatus":"0","orderQuantity":3,"ticker":"SBER","classCode":"TQBR","side":"2","orderType":"5",
                 "stopPrice":240.0,"takePrice":300.0,"timeInForce":1,"rejectReason":""}]}"#;
    let r: OrderStateListResponse = serde_json::from_str(body).unwrap();
    assert_eq!(r.data.len(), 1);
    assert_eq!(r.data[0].order_type.as_deref(), Some("5"));
    assert_eq!(r.data[0].stop_price, Some(dec!(240.0)));
    assert_eq!(r.data[0].take_price, Some(dec!(300.0)));
    assert_eq!(r.data[0].time_in_force, Some(1));
}

#[tokio::test]
async fn current_order_methods_go_to_their_documented_paths() {
    use brokapi_bcs_protocol::http::models::{
        CancelByIdRequest, CreateOrderRequest, EditOrderRequest, OrderExtras,
    };
    use brokapi_bcs_protocol::models::enums::OrderSide;
    use uuid::Uuid;
    let mut server = mockito::Server::new_async().await;
    let _auth = server
        .mock("POST", AUTH_TOKEN_PATH)
        .with_header("content-type", "application/json")
        .with_body(r#"{"access_token":"at","expires_in":300,"refresh_expires_in":1800,"refresh_token":"rt","token_type":"Bearer"}"#)
        .create_async()
        .await;
    let base = "/trade-api-bff-operations/api/v1/orders";
    let state =
        r#"{"clientOrderId":"11111111-1111-1111-1111-111111111111","data":[{"orderStatus":"0"}]}"#;
    let short = r#"{"clientOrderId":"11111111-1111-1111-1111-111111111111","status":"OK"}"#;
    let orig = Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap();
    let req_id = Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();

    let create_sync = server
        .mock("POST", format!("{base}/sync").as_str())
        .with_header("content-type", "application/json")
        .with_body(state)
        .create_async()
        .await;
    let cancel = server
        .mock("POST", format!("{base}/cancel").as_str())
        .match_body(Matcher::JsonString(
            r#"{"orderIdType":"1","orderId":"22222222-2222-2222-2222-222222222222","clientOrderId":"11111111-1111-1111-1111-111111111111"}"#.into(),
        ))
        .with_header("content-type", "application/json")
        .with_body(short)
        .create_async()
        .await;
    let cancel_sync = server
        .mock("POST", format!("{base}/cancel/sync").as_str())
        .with_header("content-type", "application/json")
        .with_body(state)
        .create_async()
        .await;
    let edit = server
        .mock("POST", format!("{base}/edit").as_str())
        .with_header("content-type", "application/json")
        .with_body(short)
        .create_async()
        .await;
    let edit_sync = server
        .mock("POST", format!("{base}/edit/sync").as_str())
        .with_header("content-type", "application/json")
        .with_body(state)
        .create_async()
        .await;
    let get = server
        .mock("GET", base)
        .match_query(Matcher::AllOf(vec![
            Matcher::UrlEncoded("orderIdType".into(), "1".into()),
            Matcher::UrlEncoded("orderId".into(), orig.hyphenated().to_string()),
        ]))
        .with_header("content-type", "application/json")
        .with_body(state)
        .create_async()
        .await;

    let c = BcsHttpClient::new(BcsConfig::for_mock("refresh", server.url())).unwrap();
    let create = CreateOrderRequest::limit(req_id, "SBER", "TQBR", OrderSide::Buy, 1, dec!(100.0));
    assert_eq!(c.create_order_sync(&create).await.unwrap().data.len(), 1);
    let cancel_req = CancelByIdRequest::by_client_order_id(orig, req_id);
    assert_eq!(
        c.cancel_order_by_id(&cancel_req).await.unwrap().status,
        "OK"
    );
    assert_eq!(
        c.cancel_order_by_id_sync(&cancel_req)
            .await
            .unwrap()
            .data
            .len(),
        1
    );
    let edit_req = EditOrderRequest {
        order_id_type: Some("1".into()),
        order_id: orig.hyphenated().to_string(),
        client_order_id: req_id,
        order_type: None,
        price: Some(dec!(101.0)),
        order_quantity: 2,
        extras: OrderExtras::default(),
    };
    assert_eq!(c.edit_order(&edit_req).await.unwrap().status, "OK");
    assert_eq!(c.edit_order_sync(&edit_req).await.unwrap().data.len(), 1);
    assert_eq!(
        c.get_order("1", &orig.hyphenated().to_string())
            .await
            .unwrap()
            .data
            .len(),
        1
    );
    for m in [create_sync, cancel, cancel_sync, edit, edit_sync, get] {
        m.assert_async().await;
    }
}
