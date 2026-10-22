//! Every endpoint method of the client sends the right HTTP method to the right path, with the right query and
//! headers. The server is a stand-in that only records what it was asked.

use std::future::Future;

use brokapi_alor_protocol::config::AlorConfig;
use brokapi_alor_protocol::error::AlorApiError;
use brokapi_alor_protocol::http::client::AlorHttpClient;
use brokapi_alor_protocol::http::models::*;
use mockito::{Matcher, Server};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde_json::json;

/// Runs `call` against a server that expects exactly one request, `method path` (with `query` pairs if any and
/// the `X-REQID` header if `req_id` is given), and checks that it came.
async fn check<F, Fut>(
    method: &str,
    path: &str,
    query: &[(&str, &str)],
    req_id: Option<&str>,
    call: F,
) where
    F: FnOnce(AlorHttpClient) -> Fut,
    Fut: Future<Output = Result<(), AlorApiError>>,
{
    let mut server = Server::new_async().await;
    let _token = server
        .mock("POST", "/refresh")
        .with_body(r#"{"AccessToken":"access-1"}"#)
        .create_async()
        .await;
    let mut expected = server
        .mock(method, path)
        .match_header("authorization", "Bearer access-1")
        .with_status(200)
        .with_body("null")
        .expect(1);
    if query.is_empty() {
        expected = expected.match_query(Matcher::Any);
    }
    for (k, v) in query {
        expected = expected.match_query(Matcher::UrlEncoded((*k).into(), (*v).into()));
    }
    if let Some(id) = req_id {
        expected = expected.match_header("x-reqid", id);
    }
    let expected = expected.create_async().await;
    let client = AlorHttpClient::new(AlorConfig::for_mock("refresh", server.url())).unwrap();
    // The answer is `null`: a typed method may fail to read it, what is checked is the request.
    let _ = call(client).await;
    expected.assert_async().await;
}

fn instrument() -> InstrumentRef {
    InstrumentRef {
        symbol: "SBER".into(),
        exchange: "MOEX".into(),
        instrument_group: Some("TQBR".into()),
    }
}

fn user() -> UserRef {
    UserRef {
        portfolio: "D1".into(),
    }
}

fn limit() -> LimitOrderRequest {
    LimitOrderRequest {
        side: "buy".into(),
        quantity: 1,
        price: dec!(100.50),
        instrument: instrument(),
        user: user(),
        comment: Some("c".into()),
        time_in_force: None,
        allow_margin: None,
        iceberg_fixed: None,
        iceberg_variance: None,
    }
}

fn market() -> MarketOrderRequest {
    MarketOrderRequest {
        side: "sell".into(),
        quantity: 2,
        instrument: instrument(),
        user: user(),
        comment: None,
        time_in_force: None,
        allow_margin: None,
    }
}

fn stop() -> StopOrderRequest {
    StopOrderRequest {
        side: "sell".into(),
        condition: "lessorequal".into(),
        trigger_price: dec!(90),
        stop_end_unix_time: Some(1_900_000_000),
        price: None,
        quantity: 1,
        instrument: instrument(),
        user: user(),
        comment: None,
        time_in_force: None,
        allow_margin: None,
        activate: Some(true),
    }
}

macro_rules! read_case {
    ($name:ident, $path:expr, $query:expr, |$c:ident| $call:expr) => {
        #[tokio::test]
        async fn $name() {
            check("GET", $path, $query, None, |$c| async move {
                $call.await.map(|_| ())
            })
            .await;
        }
    };
}

// ─── reference data ───────────────────────────────────────────────────────────

read_case!(
    securities,
    "/md/v2/Securities",
    &[("query", "SB"), ("format", "Simple")],
    |c| c.get_securities(&[("query", "SB".to_string())])
);
read_case!(
    securities_by_exchange,
    "/md/v2/Securities/MOEX",
    &[("sector", "FOND")],
    |c| c.get_securities_by_exchange("MOEX", &[("sector", "FOND".to_string())])
);
read_case!(
    security,
    "/md/v2/Securities/MOEX/SBER",
    &[("instrumentGroup", "TQBR")],
    |c| c.get_security("MOEX", "SBER", Some("TQBR"))
);
read_case!(
    available_boards,
    "/md/v2/Securities/MOEX/SBER/availableBoards",
    &[],
    |c| c.get_available_boards("MOEX", "SBER")
);
read_case!(
    quotes,
    "/md/v2/Securities/MOEX:SBER,MOEX:GAZP/quotes",
    &[],
    |c| c.get_quotes(&[("MOEX", "SBER"), ("MOEX", "GAZP")])
);
read_case!(
    currency_pairs,
    "/md/v2/Securities/currencyPairs",
    &[],
    |c| c.get_currency_pairs()
);
read_case!(
    actual_futures_quote,
    "/md/v2/Securities/MOEX/RTS/actualFuturesQuote",
    &[],
    |c| c.get_actual_futures_quote("MOEX", "RTS")
);
read_case!(risk_rates, "/md/v2/risk/rates", &[("limit", "5")], |c| c
    .get_risk_rates(&[("limit", "5".to_string())]));
read_case!(time, "/md/v2/time", &[], |c| c.get_time());
read_case!(market_status, "/md/status/MOEX/FOND", &[], |c| c
    .get_market_status("MOEX", "FOND"));

// ─── market data ──────────────────────────────────────────────────────────────

read_case!(
    order_book,
    "/md/v2/orderbooks/MOEX/SBER",
    &[("depth", "7"), ("instrumentGroup", "TQBR")],
    |c| c.get_order_book("MOEX", "SBER", Some("TQBR"), Some(7))
);
read_case!(
    history,
    "/md/v2/history",
    &[
        ("symbol", "SBER"),
        ("tf", "60"),
        ("from", "10"),
        ("to", "20"),
        ("countBack", "3")
    ],
    |c| c.get_history("MOEX", "SBER", None, "60", 10, 20, Some(3))
);
read_case!(
    all_trades,
    "/md/v2/Securities/MOEX/SBER/alltrades",
    &[("take", "2"), ("descending", "true")],
    |c| c.get_all_trades(
        "MOEX",
        "SBER",
        None,
        &[
            ("take", "2".to_string()),
            ("descending", "true".to_string())
        ]
    )
);
read_case!(
    all_trades_history,
    "/md/v2/Securities/MOEX/SBER/alltrades/history",
    &[("limit", "10")],
    |c| c.get_all_trades_history("MOEX", "SBER", None, &[("limit", "10".to_string())])
);

// ─── the portfolio ────────────────────────────────────────────────────────────

read_case!(
    orders,
    "/md/v2/Clients/MOEX/D1/orders",
    &[("format", "Simple")],
    |c| c.get_orders("MOEX", "D1")
);
read_case!(order, "/md/v2/Clients/MOEX/D1/orders/77", &[], |c| c
    .get_order("MOEX", "D1", "77"));
read_case!(stop_orders, "/md/v2/Clients/MOEX/D1/stoporders", &[], |c| c
    .get_stop_orders("MOEX", "D1"));
read_case!(
    stop_order,
    "/md/v2/Clients/MOEX/D1/stoporders/9",
    &[],
    |c| c.get_stop_order("MOEX", "D1", "9")
);
read_case!(
    money_legacy,
    "/md/v2/Clients/legacy/MOEX/D1/money",
    &[],
    |c| c.get_money_legacy("MOEX", "D1")
);
read_case!(summary, "/md/v2/Clients/MOEX/D1/summary", &[], |c| c
    .get_summary("MOEX", "D1"));
read_case!(
    positions,
    "/md/v2/Clients/MOEX/D1/positions",
    &[("withoutCurrency", "true")],
    |c| c.get_positions("MOEX", "D1", true)
);
read_case!(
    position,
    "/md/v2/Clients/MOEX/D1/positions/SBER",
    &[],
    |c| c.get_position("MOEX", "D1", "SBER")
);
read_case!(
    login_positions,
    "/md/v2/Clients/P39004/positions",
    &[],
    |c| c.get_login_positions("P39004")
);
read_case!(trades, "/md/v2/Clients/MOEX/D1/trades", &[], |c| c
    .get_trades("MOEX", "D1"));
read_case!(
    symbol_trades,
    "/md/v2/Clients/MOEX/D1/SBER/trades",
    &[],
    |c| c.get_symbol_trades("MOEX", "D1", "SBER")
);
read_case!(forts_risk, "/md/v2/Clients/MOEX/D1/fortsrisk", &[], |c| c
    .get_forts_risk("MOEX", "D1"));
read_case!(risk, "/md/v2/Clients/MOEX/D1/risk", &[], |c| c
    .get_risk("MOEX", "D1"));
read_case!(
    trade_stats,
    "/md/stats/MOEX/D1/history/trades",
    &[("limit", "3")],
    |c| c.get_trade_stats("MOEX", "D1", &[("limit", "3".to_string())])
);
read_case!(
    trade_stats_by_symbol,
    "/md/stats/MOEX/D1/history/trades/SBER",
    &[],
    |c| c.get_trade_stats_by_symbol("MOEX", "D1", "SBER", &[])
);
read_case!(
    history_trades,
    "/md/v2/Stats/MOEX/D1/history/trades",
    &[("dateFrom", "2026-10-01"), ("limit", "1000")],
    |c| c.get_history_trades(
        "MOEX",
        "D1",
        &[
            ("dateFrom", "2026-10-01".to_string()),
            ("limit", "1000".to_string())
        ]
    )
);
read_case!(
    history_trades_by_symbol,
    "/md/v2/Stats/MOEX/D1/history/trades/SBER",
    &[],
    |c| c.get_history_trades_by_symbol("MOEX", "D1", "SBER", &[])
);
read_case!(order_groups, "/commandapi/api/orderGroups", &[], |c| c
    .get_order_groups());
read_case!(order_group, "/commandapi/api/orderGroups/g-1", &[], |c| c
    .get_order_group("g-1"));

// ─── commands ─────────────────────────────────────────────────────────────────

const ACTIONS: &str = "/commandapi/warptrans/TRADE/v2/client/orders/actions";

#[tokio::test]
async fn create_limit_order() {
    check(
        "POST",
        &format!("{ACTIONS}/limit"),
        &[],
        Some("req-1"),
        |c| async move { c.create_limit_order("req-1", &limit()).await.map(|_| ()) },
    )
    .await;
}

#[tokio::test]
async fn create_market_order() {
    check(
        "POST",
        &format!("{ACTIONS}/market"),
        &[],
        Some("req-2"),
        |c| async move { c.create_market_order("req-2", &market()).await.map(|_| ()) },
    )
    .await;
}

#[tokio::test]
async fn update_orders() {
    check(
        "PUT",
        &format!("{ACTIONS}/limit/77"),
        &[],
        Some("r"),
        |c| async move { c.update_limit_order("r", "77", &limit()).await.map(|_| ()) },
    )
    .await;
    check(
        "PUT",
        &format!("{ACTIONS}/market/78"),
        &[],
        Some("r"),
        |c| async move {
            c.update_market_order("r", "78", &market())
                .await
                .map(|_| ())
        },
    )
    .await;
}

#[tokio::test]
async fn stop_orders_commands() {
    check(
        "POST",
        &format!("{ACTIONS}/stop"),
        &[],
        Some("s"),
        |c| async move { c.create_stop_order("s", &stop()).await.map(|_| ()) },
    )
    .await;
    check(
        "POST",
        &format!("{ACTIONS}/stopLimit"),
        &[],
        Some("s"),
        |c| async move { c.create_stop_limit_order("s", &stop()).await.map(|_| ()) },
    )
    .await;
    check(
        "PUT",
        &format!("{ACTIONS}/stop/9"),
        &[],
        Some("s"),
        |c| async move { c.update_stop_order("s", "9", &stop()).await.map(|_| ()) },
    )
    .await;
    check(
        "PUT",
        &format!("{ACTIONS}/stopLimit/9"),
        &[],
        Some("s"),
        |c| async move {
            c.update_stop_limit_order("s", "9", &stop())
                .await
                .map(|_| ())
        },
    )
    .await;
}

#[tokio::test]
async fn cancels() {
    check(
        "DELETE",
        "/commandapi/warptrans/TRADE/v2/client/orders/77",
        &[("portfolio", "D1"), ("exchange", "MOEX"), ("stop", "false")],
        None,
        |c| async move { c.cancel_order("MOEX", "D1", "77", false).await.map(|_| ()) },
    )
    .await;
    check(
        "DELETE",
        "/commandapi/warptrans/TRADE/v2/client/orders/all",
        &[("portfolio", "D1"), ("exchange", "MOEX"), ("stop", "true")],
        None,
        |c| async move {
            c.cancel_all_orders("MOEX", "D1", Some(true))
                .await
                .map(|_| ())
        },
    )
    .await;
}

#[tokio::test]
async fn estimates_and_groups() {
    let body = json!({"portfolio": "D1", "ticker": "SBER", "exchange": "MOEX", "price": 100, "lotQuantity": 1});
    check(
        "POST",
        "/commandapi/warptrans/TRADE/v2/client/orders/estimate",
        &[],
        None,
        |c| async move { c.estimate_order(&body).await.map(|_| ()) },
    )
    .await;
    check(
        "POST",
        "/commandapi/warptrans/TRADE/v2/client/orders/estimate/all",
        &[],
        None,
        |c| async move { c.estimate_orders(&json!([])).await.map(|_| ()) },
    )
    .await;
    check(
        "POST",
        "/commandapi/api/orderGroups",
        &[],
        None,
        |c| async move {
            c.create_order_group(&json!({"orders": [], "executionPolicy": "IgnoreCancel"}))
                .await
                .map(|_| ())
        },
    )
    .await;
    check(
        "PUT",
        "/commandapi/api/orderGroups/g-1",
        &[],
        None,
        |c| async move { c.update_order_group("g-1", &json!({})).await.map(|_| ()) },
    )
    .await;
    check(
        "DELETE",
        "/commandapi/api/orderGroups/g-1",
        &[],
        None,
        |c| async move { c.delete_order_group("g-1").await.map(|_| ()) },
    )
    .await;
}

#[tokio::test]
async fn the_legacy_family() {
    let body = json!({"Quantity": 1});
    for (kind, create, update) in [
        ("stopLoss", 0, 0),
        ("stopLossLimit", 1, 1),
        ("takeProfit", 2, 2),
        ("takeProfitLimit", 3, 3),
    ] {
        let path = format!("/warptrans/TRADE/v2/client/orders/actions/{kind}");
        let b = body.clone();
        check("POST", &path, &[], Some("l"), |c| async move {
            match create {
                0 => c.create_stop_loss("TRADE", "l", &b).await,
                1 => c.create_stop_loss_limit("TRADE", "l", &b).await,
                2 => c.create_take_profit("TRADE", "l", &b).await,
                _ => c.create_take_profit_limit("TRADE", "l", &b).await,
            }
            .map(|_| ())
        })
        .await;
        let b = body.clone();
        check(
            "PUT",
            &format!("{path}/5"),
            &[],
            Some("l"),
            |c| async move {
                match update {
                    0 => c.update_stop_loss("TRADE", "l", "5", &b).await,
                    1 => c.update_stop_loss_limit("TRADE", "l", "5", &b).await,
                    2 => c.update_take_profit("TRADE", "l", "5", &b).await,
                    _ => c.update_take_profit_limit("TRADE", "l", "5", &b).await,
                }
                .map(|_| ())
            },
        )
        .await;
    }
    check(
        "DELETE",
        "/warptrans/TRADE/v2/client/orders/5",
        &[("portfolio", "D1")],
        None,
        |c| async move {
            c.cancel_order_legacy("TRADE", "5", &[("portfolio", "D1".to_string())])
                .await
                .map(|_| ())
        },
    )
    .await;
}

#[tokio::test]
async fn a_command_body_carries_the_order_with_exact_numbers() {
    let mut server = Server::new_async().await;
    let _t = server
        .mock("POST", "/refresh")
        .with_body(r#"{"AccessToken":"a"}"#)
        .create_async()
        .await;
    let m = server
        .mock("POST", format!("{ACTIONS}/limit").as_str())
        .match_body(Matcher::Regex(r#""price":100\.50[,}]"#.into()))
        .match_body(Matcher::PartialJson(json!({
            "side": "buy", "quantity": 1,
            "instrument": {"symbol": "SBER", "exchange": "MOEX", "instrumentGroup": "TQBR"},
            "user": {"portfolio": "D1"}, "comment": "c"
        })))
        .with_body(r#"{"message":"success","orderNumber":"18995978560"}"#)
        .create_async()
        .await;
    let client = AlorHttpClient::new(AlorConfig::for_mock("r", server.url())).unwrap();
    let resp = client.create_limit_order("id", &limit()).await.unwrap();
    assert_eq!(resp.order_number.as_deref(), Some("18995978560"));
    m.assert_async().await;
}
