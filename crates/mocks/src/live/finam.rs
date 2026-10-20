//! The probes of Finam: fixed gRPC calls made against the live API or against the imitation.
//!
//! Like [`tbank`](super::tbank): a probe keeps the message as JSON, or the code of the refusal. [`read_only`]
//! reads, [`order_roundtrip`] places one order far from the market and cancels it.

use brokapi_finam_protocol::FinamApi;
use brokapi_finam_protocol::pb::google::r#type::Decimal;
use brokapi_finam_protocol::pb::grpc::tradeapi::v1::accounts as pacc;
use brokapi_finam_protocol::pb::grpc::tradeapi::v1::assets as pa;
use brokapi_finam_protocol::pb::grpc::tradeapi::v1::marketdata as pm;
use brokapi_finam_protocol::pb::grpc::tradeapi::v1::orders as po;
use serde_json::json;
use tonic::{Code, Response, Status};

use super::{Outcome, tree};

/// The id of an order that was never placed.
const UNKNOWN_ORDER: &str = "999999999999";

/// A reply as an outcome. A broken connection is not an answer of the broker: it stops the run.
///
/// # Panics
///
/// On `Unavailable`, `DeadlineExceeded`, `Cancelled` and `Unknown`, which say nothing about the API.
fn outcome<T: std::fmt::Debug>(call: &str, reply: Result<Response<T>, Status>) -> Outcome {
    match reply {
        Ok(r) => Outcome::Answered(tree::of(&r.into_inner())),
        Err(s)
            if matches!(
                s.code(),
                Code::Unavailable | Code::DeadlineExceeded | Code::Cancelled | Code::Unknown
            ) =>
        {
            panic!("{call}: the connection failed, not the API: {s}")
        }
        Err(s) => Outcome::Refused {
            code: format!("{:?}", s.code()),
            body: Some(json!({"message": s.message()})),
        },
    }
}

/// Reads: the directory, market data, the account, and an order that does not exist.
///
/// `account` is an account of the secret, `symbol` an instrument that exists (`SBER@MISX`).
pub async fn read_only(api: &FinamApi, account: &str, symbol: &str) -> Vec<(String, Outcome)> {
    let mut out: Vec<(String, Outcome)> = Vec::new();
    let mut add = |name: &str, o: Outcome| out.push((name.to_owned(), o));

    add(
        "get_account",
        outcome(
            "GetAccount",
            api.accounts()
                .get_account(pacc::GetAccountRequest {
                    account_id: account.into(),
                })
                .await,
        ),
    );
    add(
        "get_asset",
        outcome(
            "GetAsset",
            api.assets()
                .get_asset(pa::GetAssetRequest {
                    symbol: symbol.into(),
                    account_id: String::new(),
                })
                .await,
        ),
    );
    add(
        "asset_schedule",
        outcome(
            "Schedule",
            api.assets()
                .schedule(pa::ScheduleRequest {
                    symbol: symbol.into(),
                })
                .await,
        ),
    );
    add(
        "order_book",
        outcome(
            "OrderBook",
            api.market_data()
                .order_book(pm::OrderBookRequest {
                    symbol: symbol.into(),
                })
                .await,
        ),
    );
    add(
        "latest_trades",
        outcome(
            "LatestTrades",
            api.market_data()
                .latest_trades(pm::LatestTradesRequest {
                    symbol: symbol.into(),
                })
                .await,
        ),
    );
    add(
        "orders",
        outcome(
            "GetOrders",
            api.orders()
                .get_orders(po::OrdersRequest {
                    account_id: account.into(),
                })
                .await,
        ),
    );
    // An order the broker has never seen.
    add(
        "order_unknown",
        outcome(
            "GetOrder",
            api.orders()
                .get_order(po::GetOrderRequest {
                    account_id: account.into(),
                    order_id: UNKNOWN_ORDER.into(),
                })
                .await,
        ),
    );
    add(
        "cancel_unknown",
        outcome(
            "CancelOrder",
            api.orders()
                .cancel_order(po::CancelOrderRequest {
                    account_id: account.into(),
                    order_id: UNKNOWN_ORDER.into(),
                })
                .await,
        ),
    );
    out
}

/// One limit buy of `qty` at `price` (far below the market), its state, and the cancel.
///
/// This puts a real order on the account, for a moment (Finam has no sandbox). Run it only with the word of the
/// owner, with the minimum quantity and a price the exchange will not fill. Against the imitation it is harmless.
pub async fn order_roundtrip(
    api: &FinamApi,
    account: &str,
    symbol: &str,
    price: &str,
    qty: &str,
) -> Vec<(String, Outcome)> {
    let client_order_id: String = uuid::Uuid::new_v4()
        .simple()
        .to_string()
        .chars()
        .take(20)
        .collect();
    let order = po::Order {
        account_id: account.into(),
        symbol: symbol.into(),
        quantity: Some(Decimal { value: qty.into() }),
        side: brokapi_finam_protocol::pb::grpc::tradeapi::v1::Side::Buy as i32,
        r#type: po::OrderType::Limit as i32,
        time_in_force: po::TimeInForce::Day as i32,
        limit_price: Some(Decimal {
            value: price.into(),
        }),
        client_order_id: client_order_id.clone(),
        comment: "brokapi-probe".into(),
        ..Default::default()
    };
    let mut out = Vec::new();
    let placed = api.orders().place_order(order).await;
    let order_id = placed.as_ref().ok().map(|r| r.get_ref().order_id.clone());
    out.push(("place_order".to_owned(), outcome("PlaceOrder", placed)));
    if let Some(order_id) = order_id {
        out.push((
            "order_state".to_owned(),
            outcome(
                "GetOrder",
                api.orders()
                    .get_order(po::GetOrderRequest {
                        account_id: account.into(),
                        order_id: order_id.clone(),
                    })
                    .await,
            ),
        ));
        out.push((
            "cancel_order".to_owned(),
            outcome(
                "CancelOrder",
                api.orders()
                    .cancel_order(po::CancelOrderRequest {
                        account_id: account.into(),
                        order_id,
                    })
                    .await,
            ),
        ));
    }
    out
}
