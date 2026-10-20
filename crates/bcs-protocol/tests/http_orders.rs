#![cfg(feature = "integration-live")]

use brokapi_bcs_protocol::config::BcsConfig;
use brokapi_bcs_protocol::http::client::BcsHttpClient;
use brokapi_bcs_protocol::http::models::{
    CancelOrderRequest, CreateOrderRequest, ModifyOrderRequest, OrderSearchRequest,
};
use brokapi_bcs_protocol::models::enums::OrderSide;
use chrono::Utc;
use uuid::Uuid;

fn setup() -> (BcsConfig, BcsHttpClient) {
    dotenv::dotenv().ok();
    let config = BcsConfig::from_env().expect("Config error");
    let client = BcsHttpClient::new(config.clone()).expect("Client error");
    (config, client)
}

// ===== ORDERS =====
#[tokio::test]
async fn test_create_order() {
    let (_, client) = setup();

    let ticker = "MTLR";
    let class_code = "TQBR".to_string();

    // 3) Только LIMIT заявка
    let limit_price = rust_decimal::Decimal::new(3000, 2);

    println!("╔══════════════════════════════════════════╗");
    println!("║      CREATE ORDER (LIMIT ONLY)           ║");
    println!("╠══════════════════════════════════════════╣");
    println!("║ Тикер:          {} ({})", ticker, class_code);
    println!("║ Цена лимитки:   {:.2}", limit_price);
    println!("╚══════════════════════════════════════════╝");

    let request = CreateOrderRequest::limit(
        Uuid::new_v4(),
        ticker,
        &class_code,
        OrderSide::Buy,
        1,
        limit_price,
    );

    let side = format!("{:?}", request.side);
    let order_type = format!("{:?}", request.order_type);
    let notional =
        request.price.unwrap_or_default() * rust_decimal::Decimal::from(request.order_quantity);

    println!("╔════════════════════════════════════════════════════╗");
    println!("║                ORDER SEND REQUEST                 ║");
    println!("╠════════════════════════════════════════════════════╣");
    println!("║ clientOrderId:   {}", request.client_order_id);
    println!(
        "║ instrument:      {} ({})",
        request.ticker, request.class_code
    );
    println!(
        "║ side/type:       {} ({:?}) / {} ({:?})",
        side, request.side, order_type, request.order_type
    );
    println!("║ quantity:        {}", request.order_quantity);
    println!("║ price:           {:?}", request.price);
    println!("║ est. notional:   {:.2} RUB", notional);
    println!("╚════════════════════════════════════════════════════╝");

    let json_body = serde_json::to_string_pretty(&request).unwrap();
    println!("📤 REQUEST JSON:\n{}", json_body);

    let started = std::time::Instant::now();

    let resp = client
        .create_order(request)
        .await
        .expect("Limit create_order failed");

    let elapsed_ms = started.elapsed().as_millis();

    println!("╔════════════════════════════════════════════════════╗");
    println!("║               ORDER SEND RESPONSE                 ║");
    println!("╠════════════════════════════════════════════════════╣");
    println!("║ clientOrderId:   {}", resp.client_order_id);
    println!("║ operationStatus: {}", resp.status);
    println!("║ latency:         {} ms", elapsed_ms);
    println!("╚════════════════════════════════════════════════════╝");

    tokio::time::sleep(std::time::Duration::from_millis(700)).await;

    let status = client
        .get_order_status(resp.client_order_id)
        .await
        .expect("Limit get_order_status failed");

    if let Some(data) = &status.data {
        let order_status_code = data.order_status.as_deref().unwrap_or("?");
        let order_status = match order_status_code {
            "0" => "NEW",
            "1" => "PARTIALLY_FILLED",
            "2" => "FILLED",
            "4" => "CANCELLED",
            "8" => "REJECTED",
            "9" => "PENDING_REPLACE",
            "10" => "PENDING_CONFIRMATION",
            _ => "UNKNOWN",
        };

        let exec_type_code = data.execution_type.as_deref().unwrap_or("?");
        let exec_type = match exec_type_code {
            "0" => "NEW",
            "1" => "PARTIAL",
            "2" => "TRADE",
            "4" => "CANCEL",
            "8" => "REJECT",
            _ => "UNKNOWN",
        };

        let order_id = data.order_id.as_deref().unwrap_or("");
        let order_number = data.order_number.as_deref().unwrap_or("");
        let class_code_resp = data.class_code.as_deref().unwrap_or("");
        let exchange_resp = data.security_exchange.as_deref().unwrap_or("");

        println!("╔════════════════════════════════════════════════════╗");
        println!("║              ORDER STATUS DETAILS                 ║");
        println!("╠════════════════════════════════════════════════════╣");
        println!("║ client_order_id: {}", status.client_order_id);
        println!(
            "║ status:          {} ({})",
            order_status, order_status_code
        );
        println!("║ execution_type:  {} ({})", exec_type, exec_type_code);
        println!(
            "║ ticker:          {}",
            data.ticker.as_deref().unwrap_or("?")
        );
        println!(
            "║ class_code:      {}",
            if class_code_resp.is_empty() {
                "<empty>"
            } else {
                class_code_resp
            }
        );
        println!(
            "║ exchange:        {}",
            if exchange_resp.is_empty() {
                "<empty>"
            } else {
                exchange_resp
            }
        );
        println!("║ side/orderType:  {:?} / {:?}", data.side, data.order_type);
        println!(
            "║ qty:             {:?} (exec: {:?}, left: {:?})",
            data.order_quantity, data.executed_quantity, data.remained_quantity
        );
        println!(
            "║ price/avg:       {:?} / {:?}",
            data.price, data.average_price
        );
        println!(
            "║ order_id:        {}",
            if order_id.is_empty() {
                "<empty>"
            } else {
                order_id
            }
        );
        println!(
            "║ order_number:    {}",
            if order_number.is_empty() {
                "<empty>"
            } else {
                order_number
            }
        );
        println!("║ execution_id:    {:?}", data.execution_id);
        println!("║ transactionTime: {:?}", data.transaction_time);
        println!("╠════════════════════════════════════════════════════╣");

        if order_status_code == "8" && (order_id == "0" || order_id.is_empty()) {
            println!("║ ⚠ REJECTED до биржи: order_id=0, биржа не присвоила номер");
            println!("║ ⚠ Обычно это права/риск-контур брокера, а не формат JSON");
        } else if order_status_code == "0" {
            println!("║ ✅ Заявка принята биржей (NEW)");
        }

        println!("╚════════════════════════════════════════════════════╝");
    } else {
        println!("⚠️ status.data = None");
    }
}

#[tokio::test]
async fn test_cancel_order() {
    let (_, client) = setup();

    let original_order_id = std::env::var("ORIGINAL_CLIENT_ORDER_ID")
        .expect("❌ Set ORIGINAL_CLIENT_ORDER_ID env var from create test output\n   Example: export ORIGINAL_CLIENT_ORDER_ID=\"xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx\"");

    let original_order_id = Uuid::parse_str(&original_order_id)
        .expect("❌ ORIGINAL_CLIENT_ORDER_ID must be valid UUID");

    let cancel_client_order_id = Uuid::new_v4();
    let request = CancelOrderRequest {
        client_order_id: cancel_client_order_id,
    };

    let started = std::time::Instant::now();

    println!("╔════════════════════════════════════════════════════╗");
    println!("║               CANCEL ORDER REQUEST                ║");
    println!("╠════════════════════════════════════════════════════╣");
    println!("║ original_order_id: {}", original_order_id);
    println!("║ client_order_id:   {}", request.client_order_id);
    println!("╚════════════════════════════════════════════════════╝");

    match client.cancel_order(original_order_id, request).await {
        Ok(resp) => {
            println!("╔════════════════════════════════════════════════════╗");
            println!("║              CANCEL ORDER RESPONSE                ║");
            println!("╠════════════════════════════════════════════════════╣");
            println!("║ client_order_id:   {}", resp.client_order_id);
            println!("║ operation_status:  {}", resp.status);
            println!("║ latency:           {} ms", started.elapsed().as_millis());
            println!("╚════════════════════════════════════════════════════╝");
        }
        Err(e) => {
            println!("╔════════════════════════════════════════════════════╗");
            println!("║               CANCEL ORDER ERROR                  ║");
            println!("╠════════════════════════════════════════════════════╣");
            println!("║ error: {}", e);
            println!("║ hint:  если orderStatus=8 и orderId=0, отменять нечего");
            println!("╚════════════════════════════════════════════════════╝");
            panic!("Cancel order failed: {}", e);
        }
    }
}

#[tokio::test]
async fn test_modify_order() {
    let (_, client) = setup();

    let original_order_id = std::env::var("ORIGINAL_CLIENT_ORDER_ID")
        .expect("❌ Set ORIGINAL_CLIENT_ORDER_ID env var from your create test output");

    let original_order_id = Uuid::parse_str(&original_order_id)
        .expect("❌ ORIGINAL_CLIENT_ORDER_ID must be valid UUID");

    let request = ModifyOrderRequest {
        client_order_id: Uuid::new_v4(),
        order_quantity: 1,
        price: Some(rust_decimal::Decimal::new(3100, 2)),
    };

    let started = std::time::Instant::now();

    println!("╔════════════════════════════════════════════════════╗");
    println!("║               MODIFY ORDER REQUEST                ║");
    println!("╠════════════════════════════════════════════════════╣");
    println!("║ original_order_id: {}", original_order_id);
    println!("║ client_order_id:   {}", request.client_order_id);
    println!("║ new_qty:           {}", request.order_quantity);
    println!("║ new_price:         {:?}", request.price);
    println!("╚════════════════════════════════════════════════════╝");

    match client.modify_order(original_order_id, request).await {
        Ok(resp) => {
            println!("╔════════════════════════════════════════════════════╗");
            println!("║              MODIFY ORDER RESPONSE                ║");
            println!("╠════════════════════════════════════════════════════╣");
            println!("║ client_order_id:   {}", resp.client_order_id);
            println!("║ operation_status:  {}", resp.status);
            println!("║ latency:           {} ms", started.elapsed().as_millis());
            println!("╚════════════════════════════════════════════════════╝");
        }
        Err(e) => {
            println!("╔════════════════════════════════════════════════════╗");
            println!("║                MODIFY ORDER ERROR                 ║");
            println!("╠════════════════════════════════════════════════════╣");
            println!("║ error: {}", e);
            println!("║ hint:  если исходная заявка уже исполнена/снята, modify вернёт ошибку");
            println!("╚════════════════════════════════════════════════════╝");
            panic!("Modify order failed: {}", e);
        }
    }
}

#[tokio::test]
async fn test_orders_search() {
    let (_, client) = setup();
    let end_date = Utc::now();
    let start_date = end_date - chrono::Duration::hours(1); // Уменьшили диапазон

    let request = OrderSearchRequest {
        start_date_time: start_date,
        end_date_time: end_date,
        side: None,
        order_status: None,
        order_types: None,
        tickers: Some(vec!["SU26238RMFS4".to_string()]),
        class_codes: Some(vec!["TQOB".to_string()]),
    };

    let response = client
        .search_orders(request, Some(0), Some(50))
        .await
        .expect("Orders search failed");

    println!("╔══════════════════════════════════════╗");
    println!("║     ORDERS SEARCH (SU26238RMFS4/TQOB)      ║");
    println!("╠══════════════════════════════════════╣");
    println!("║ Всего заявок: {}", response.total_records);
    println!("╚══════════════════════════════════════╝");

    let mut rejected = 0usize;
    let mut active = 0usize;

    for order in &response.records {
        let status_code = order.order_status.unwrap_or(-1);
        let status_str = match status_code {
            1 => "CANCELLED",
            2 => "FILLED",
            3 => "ACTIVE",
            _ => "UNKNOWN",
        };

        if status_code == 1 {
            rejected += 1; // или rename переменной, если это не rejected
        }
        if status_code == 3 {
            active += 1;
        }

        println!("  📋 {}", order.order_id);
        println!(
            "     {} ({}) | ticker={} classCode={}",
            status_str,
            status_code,
            order.ticker.as_deref().unwrap_or("?"),
            order.class_code.as_deref().unwrap_or("?")
        );
        println!(
            "     qty: {:?} | exec: {:?} | price: {:?}",
            order.order_quantity, order.executed_quantity, order.price
        );
        println!();
    }

    println!("╔════════════════════════════════════════════════════╗");
    println!("║                ORDERS SUMMARY                     ║");
    println!("╠════════════════════════════════════════════════════╣");
    println!("║ total:    {}", response.records.len());
    println!("║ active:   {}", active);
    println!("║ rejected: {}", rejected);
    println!("╚════════════════════════════════════════════════════╝");
}

#[tokio::test]
async fn test_get_order_status() {
    let (_, client) = setup();

    let order_id =
        std::env::var("ORIGINAL_CLIENT_ORDER_ID").expect("❌ Set ORIGINAL_CLIENT_ORDER_ID env var");

    let order_id =
        Uuid::parse_str(&order_id).expect("❌ ORIGINAL_CLIENT_ORDER_ID must be valid UUID");

    println!("🔍 Checking order status: {}", order_id);

    match client.get_order_status(order_id).await {
        Ok(resp) => {
            println!("╔══════════════════════════════════════╗");
            println!("║      ORDER STATUS                    ║");
            println!("╠══════════════════════════════════════╣");
            println!("║ client_order_id: {}", resp.client_order_id);
            println!(
                "║ original_client_order_id: {}",
                resp.original_client_order_id
            );

            if let Some(data) = &resp.data {
                let order_status_code = data.order_status.as_deref().unwrap_or("?");
                let order_status_human = match order_status_code {
                    "0" => "NEW",
                    "1" => "PARTIALLY_FILLED",
                    "2" => "FILLED",
                    "4" => "CANCELLED",
                    "8" => "REJECTED",
                    "9" => "PENDING_REPLACE",
                    "10" => "PENDING_CONFIRMATION",
                    _ => "UNKNOWN",
                };

                let exec_type_code = data.execution_type.as_deref().unwrap_or("?");
                let exec_type_human = match exec_type_code {
                    "0" => "NEW",
                    "1" => "PARTIAL",
                    "2" => "TRADE",
                    "4" => "CANCEL",
                    "8" => "REJECT",
                    _ => "UNKNOWN",
                };

                let order_id = data.order_id.as_deref().unwrap_or("");
                let order_number = data.order_number.as_deref().unwrap_or("");
                let class_code_resp = data.class_code.as_deref().unwrap_or("");
                let exchange_resp = data.security_exchange.as_deref().unwrap_or("");

                println!("╔════════════════════════════════════════════════════╗");
                println!("║              ORDER STATUS DETAILS                 ║");
                println!("╠════════════════════════════════════════════════════╣");
                println!("║ client_order_id: {}", resp.client_order_id);
                println!(
                    "║ status:          {} ({})",
                    order_status_human, order_status_code
                );
                println!(
                    "║ execution_type:  {} ({})",
                    exec_type_human, exec_type_code
                );
                println!(
                    "║ ticker:          {}",
                    data.ticker.as_deref().unwrap_or("?")
                );
                println!(
                    "║ class_code:      {}",
                    if class_code_resp.is_empty() {
                        "<empty>"
                    } else {
                        class_code_resp
                    }
                );
                println!(
                    "║ exchange:        {}",
                    if exchange_resp.is_empty() {
                        "<empty>"
                    } else {
                        exchange_resp
                    }
                );
                println!("║ side/orderType:  {:?} / {:?}", data.side, data.order_type);
                println!(
                    "║ qty:             {:?} (exec: {:?}, left: {:?})",
                    data.order_quantity, data.executed_quantity, data.remained_quantity
                );
                println!(
                    "║ price/avg:       {:?} / {:?}",
                    data.price, data.average_price
                );
                println!(
                    "║ order_id:        {}",
                    if order_id.is_empty() {
                        "<empty>"
                    } else {
                        order_id
                    }
                );
                println!(
                    "║ order_number:    {}",
                    if order_number.is_empty() {
                        "<empty>"
                    } else {
                        order_number
                    }
                );
                println!("║ execution_id:    {:?}", data.execution_id);
                println!("║ transactionTime: {:?}", data.transaction_time);
                println!("╠════════════════════════════════════════════════════╣");

                if order_status_code == "8" && (order_id == "0" || order_id.is_empty()) {
                    println!("║ ⚠ REJECTED до биржи: order_id=0, биржа не присвоила номер");
                    println!("║ ⚠ Обычно это права/риск-контур брокера, а не формат JSON");
                } else if order_status_code == "0" {
                    println!("║ ✅ Заявка принята биржей (NEW)");
                }

                println!("╚════════════════════════════════════════════════════╝");
            } else {
                println!("⚠️ status.data = None");
            }
        }
        Err(e) => {
            println!("❌ Get status failed: {}", e);
            println!("ℹ️ Заявка не найдена — возможно уже исполнилась или отменена");
        }
    }
}

#[tokio::test]
async fn test_get_order_status_raw() {
    let (_, client) = setup();

    let order_id =
        std::env::var("ORIGINAL_CLIENT_ORDER_ID").expect("❌ Set ORIGINAL_CLIENT_ORDER_ID env var");

    let path = format!("/trade-api-bff-operations/api/v1/orders/{}", order_id);

    match client.get_raw(&path).await {
        Ok((status, body)) => {
            println!("╔════════════════════════════════════════════════╗");
            println!("║      RAW ORDER STATUS RESPONSE                 ║");
            println!("╠════════════════════════════════════════════════╣");
            println!("║ HTTP Status: {}", status);
            println!("╠════════════════════════════════════════════════╣");
            println!("║ requested_order_id: {}", order_id);
            println!("║ body_length: {} bytes", body.len());
            println!("║ body: {}", body);
            println!("╚════════════════════════════════════════════════╝");

            // Пытаемся красиво отформатировать
            if let Ok(json) = serde_json::from_str::<serde_json::Value>(&body) {
                println!("\n=== PRETTY JSON ===");
                println!("{}", serde_json::to_string_pretty(&json).unwrap());

                // Проверяем есть ли неизвестные поля с причиной отказа
                if let Some(data) = json.get("data") {
                    println!("\n=== DATA FIELDS ===");
                    if let Some(obj) = data.as_object() {
                        for (key, value) in obj {
                            println!("  {}: {}", key, value);
                        }
                    }
                }
            }

            if let Ok(json) = serde_json::from_str::<serde_json::Value>(&body) {
                let data = json.get("data").cloned().unwrap_or_default();
                println!("=== QUICK EXTRACT ===");
                println!("orderStatus:  {:?}", data.get("orderStatus"));
                println!("executionType:{:?}", data.get("executionType"));
                println!("orderId:      {:?}", data.get("orderId"));
                println!("orderNumber:  {:?}", data.get("orderNumber"));
                println!("classCode:    {:?}", data.get("classCode"));
                println!("exchange:     {:?}", data.get("securityExchange"));
            }
        }
        Err(e) => {
            println!("❌ Request failed: {}", e);
        }
    }
}
