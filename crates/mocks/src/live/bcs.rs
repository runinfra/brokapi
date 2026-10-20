//! The probes of BCS: fixed requests made against the live API or against the imitation.
//!
//! BCS speaks HTTP and JSON, so a probe sends a raw request and keeps the JSON it got, whatever the
//! typed models of the library would make of it. [`read_only`] reads, [`order_roundtrip`] places one order far
//! from the market and cancels it. The names of the probes are the names of the reference files.

use brokapi_bcs_protocol::http::client::BcsHttpClient;
use reqwest::Method;
use serde_json::{Value, json};

use super::Outcome;

const BASE_MD: &str = "/trade-api-market-data-connector/api/v1";
const BASE_INFO: &str = "/trade-api-information-service/api/v1";
const ORDERS: &str = "/trade-api-bff-operations/api/v1/orders";
const PORTFOLIO: &str = "/trade-api-bff-portfolio/api/v1/portfolio";
const LIMITS: &str = "/trade-api-bff-limit/api/v1/limits";

/// Sends one request and keeps what came back: the JSON of the body and the status.
///
/// # Panics
///
/// When the request cannot be sent at all (no network, no token): that is not an answer of the broker.
async fn send(http: &BcsHttpClient, method: Method, path: &str, body: Option<&Value>) -> Outcome {
    let token = http
        .get_access_token()
        .await
        .unwrap_or_else(|e| panic!("no access token: {e}"));
    let url = format!("{}{path}", http.base_url());
    let mut request = http.inner_client().request(method, &url).bearer_auth(token);
    if let Some(body) = body {
        request = request.json(body);
    }
    let response = request
        .send()
        .await
        .unwrap_or_else(|e| panic!("cannot reach {url}: {e}"));
    let status = response.status();
    let text = response
        .text()
        .await
        .unwrap_or_else(|e| panic!("cannot read the answer of {url}: {e}"));
    let parsed: Option<Value> = serde_json::from_str(&text).ok();
    if status.is_success() {
        Outcome::Answered(parsed.unwrap_or(Value::String(text)))
    } else {
        Outcome::Refused {
            code: format!("http_{}", status.as_u16()),
            body: parsed,
        }
    }
}

/// Reads: the directory, the market data, the portfolio, and an order that does not exist. Nothing is traded.
///
/// `ticker` and `class_code` name an instrument that exists (`SBER`, `TQBR`).
pub async fn read_only(
    http: &BcsHttpClient,
    ticker: &str,
    class_code: &str,
) -> Vec<(String, Outcome)> {
    let q = format!("ticker={ticker}&classCode={class_code}");
    let instrument = json!({"ticker": ticker, "classCode": class_code});
    let probes: Vec<(&str, Method, String, Option<Value>)> = vec![
        (
            "instruments_by_tickers",
            Method::POST,
            format!("{BASE_INFO}/instruments/by-tickers"),
            Some(json!({"tickers": [ticker]})),
        ),
        (
            "trading_status",
            Method::GET,
            format!("{BASE_INFO}/trading-schedule/status?classCode={class_code}"),
            None,
        ),
        (
            "daily_schedule",
            Method::GET,
            format!("{BASE_INFO}/trading-schedule/daily-schedule?{q}"),
            None,
        ),
        (
            "order_book",
            Method::GET,
            format!("{BASE_MD}/order-book?{q}&depth=5"),
            None,
        ),
        (
            "candles",
            Method::GET,
            format!(
                "{BASE_MD}/candles-chart?{q}&timeFrame=H1&startDate=2026-10-13T00:00:00.000Z&endDate=2026-10-14T00:00:00.000Z"
            ),
            None,
        ),
        (
            "last_trades",
            Method::POST,
            format!("{BASE_MD}/last-trades"),
            Some(instrument.clone()),
        ),
        (
            "quotes",
            Method::POST,
            format!("{BASE_MD}/quotes"),
            Some(json!({"instruments": [instrument]})),
        ),
        ("portfolio", Method::GET, PORTFOLIO.to_owned(), None),
        ("limits", Method::GET, LIMITS.to_owned(), None),
        // What does the broker answer for an order it has never seen? `reconcile` relies on it.
        (
            "order_unknown_cid",
            Method::GET,
            format!("{ORDERS}?orderIdType=1&orderId=00000000-0000-4000-8000-000000000001"),
            None,
        ),
    ];
    let mut out = Vec::new();
    for (name, method, path, body) in probes {
        out.push((
            name.to_owned(),
            send(http, method, &path, body.as_ref()).await,
        ));
    }
    out
}

/// One limit buy of `qty` at `price` (far below the market), its state, and the cancel.
///
/// This puts a real order on the account, for a moment. Run it only with the word of the owner, with the
/// minimum quantity and a price the exchange will not fill. Against the imitation it is harmless.
pub async fn order_roundtrip(
    http: &BcsHttpClient,
    ticker: &str,
    class_code: &str,
    price: &str,
    qty: i64,
) -> Vec<(String, Outcome)> {
    let cid = uuid::Uuid::new_v4().to_string();
    let order = json!({
        "clientOrderId": cid, "side": "1", "orderType": "2", "orderQuantity": qty,
        "ticker": ticker, "classCode": class_code,
        "price": serde_json::from_str::<Value>(price).unwrap_or_else(|_| json!(price)),
    });
    let mut out = Vec::new();
    out.push((
        "place_order".to_owned(),
        send(http, Method::POST, &format!("{ORDERS}/sync"), Some(&order)).await,
    ));
    out.push((
        "order_state".to_owned(),
        send(
            http,
            Method::GET,
            &format!("{ORDERS}?orderIdType=1&orderId={cid}"),
            None,
        )
        .await,
    ));
    // The same cid again: what does the broker say to a repeat?
    out.push((
        "place_order_again".to_owned(),
        send(http, Method::POST, &format!("{ORDERS}/sync"), Some(&order)).await,
    ));
    let cancel = json!({"clientOrderId": uuid::Uuid::new_v4().to_string(), "orderId": cid, "orderIdType": "1"});
    out.push((
        "cancel_order".to_owned(),
        send(
            http,
            Method::POST,
            &format!("{ORDERS}/cancel/sync"),
            Some(&cancel),
        )
        .await,
    ));
    out
}
