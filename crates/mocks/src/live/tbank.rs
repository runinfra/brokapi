//! The probes of T-Bank: fixed gRPC calls made against the live API (the sandbox) or against the imitation.
//!
//! A probe keeps the message the broker sent, turned into JSON by [`tree::of`], or the code
//! the broker refused with. [`read_only`] reads, [`order_roundtrip`] places one order far from the market and
//! cancels it, [`repeated_order_id`] sends the same `order_id` twice and counts the orders that stand.
//! The names of the probes are the names of the reference files.

use brokapi_tbank_protocol::v1 as pb;
use brokapi_tbank_protocol::{TbankClient, money};
use serde_json::{Value, json};
use tonic::{Code, Response, Status};

use super::{Outcome, tree};

/// The id of an order that was never placed.
const UNKNOWN_ORDER: &str = "00000000-0000-4000-8000-000000000001";

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

fn now_seconds() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(0))
}

fn stamp(seconds: i64) -> prost_types::Timestamp {
    prost_types::Timestamp { seconds, nanos: 0 }
}

/// Reads: accounts, the directory, market data, the portfolio, and things that do not exist.
///
/// `account` is an account of the token, `uid` the uid of an instrument that exists (SBER), `ticker` and
/// `class_code` name the same instrument.
pub async fn read_only(
    c: &TbankClient,
    account: &str,
    uid: &str,
    ticker: &str,
    class_code: &str,
) -> Vec<(String, Outcome)> {
    let now = now_seconds();
    let mut out: Vec<(String, Outcome)> = Vec::new();
    let mut add = |name: &str, o: Outcome| out.push((name.to_owned(), o));

    add(
        "get_accounts",
        outcome(
            "GetAccounts",
            c.users()
                .get_accounts(pb::GetAccountsRequest { status: None })
                .await,
        ),
    );
    add(
        "find_instrument",
        outcome(
            "FindInstrument",
            c.instruments()
                .find_instrument(pb::FindInstrumentRequest {
                    query: ticker.into(),
                    instrument_kind: None,
                    api_trade_available_flag: None,
                })
                .await,
        ),
    );
    let by_ticker = |id: &str| pb::InstrumentRequest {
        id_type: pb::InstrumentIdType::Ticker as i32,
        id: id.into(),
        class_code: Some(class_code.into()),
    };
    add(
        "instrument_by_ticker",
        outcome(
            "GetInstrumentBy",
            c.instruments().get_instrument_by(by_ticker(ticker)).await,
        ),
    );
    add(
        "instrument_unknown",
        outcome(
            "GetInstrumentBy",
            c.instruments()
                .get_instrument_by(by_ticker("NOSUCHTICKER"))
                .await,
        ),
    );
    add(
        "order_book",
        outcome(
            "GetOrderBook",
            c.market_data()
                .get_order_book(pb::GetOrderBookRequest {
                    depth: 10,
                    instrument_id: Some(uid.into()),
                    ..Default::default()
                })
                .await,
        ),
    );
    add(
        "last_prices",
        outcome(
            "GetLastPrices",
            c.market_data()
                .get_last_prices(pb::GetLastPricesRequest {
                    instrument_id: vec![uid.into()],
                    ..Default::default()
                })
                .await,
        ),
    );
    add(
        "candles",
        outcome(
            "GetCandles",
            c.market_data()
                .get_candles(pb::GetCandlesRequest {
                    from: Some(stamp(now - 2 * 86_400)),
                    to: Some(stamp(now - 86_400)),
                    interval: pb::CandleInterval::Hour as i32,
                    instrument_id: Some(uid.into()),
                    ..Default::default()
                })
                .await,
        ),
    );
    add(
        "trading_status",
        outcome(
            "GetTradingStatus",
            c.market_data()
                .get_trading_status(pb::GetTradingStatusRequest {
                    instrument_id: Some(uid.into()),
                    ..Default::default()
                })
                .await,
        ),
    );
    add(
        "last_trades",
        outcome(
            "GetLastTrades",
            c.market_data()
                .get_last_trades(pb::GetLastTradesRequest {
                    from: Some(stamp(now - 3_600)),
                    to: Some(stamp(now)),
                    instrument_id: Some(uid.into()),
                    ..Default::default()
                })
                .await,
        ),
    );
    add(
        "portfolio",
        outcome(
            "GetPortfolio",
            c.operations()
                .get_portfolio(pb::PortfolioRequest {
                    account_id: account.into(),
                    ..Default::default()
                })
                .await,
        ),
    );
    add(
        "positions",
        outcome(
            "GetPositions",
            c.operations()
                .get_positions(pb::PositionsRequest {
                    account_id: account.into(),
                })
                .await,
        ),
    );
    add(
        "orders",
        outcome(
            "GetOrders",
            c.orders()
                .get_orders(pb::GetOrdersRequest {
                    account_id: account.into(),
                    advanced_filters: None,
                })
                .await,
        ),
    );
    add(
        "operations",
        outcome(
            "GetOperationsByCursor",
            c.operations()
                .get_operations_by_cursor(pb::GetOperationsByCursorRequest {
                    account_id: account.into(),
                    from: Some(stamp(now - 30 * 86_400)),
                    to: Some(stamp(now)),
                    limit: Some(10),
                    ..Default::default()
                })
                .await,
        ),
    );
    add(
        "stop_orders",
        outcome(
            "GetStopOrders",
            c.stop_orders()
                .get_stop_orders(pb::GetStopOrdersRequest {
                    account_id: account.into(),
                    status: pb::StopOrderStatusOption::StopOrderStatusActive as i32,
                    from: None,
                    to: None,
                })
                .await,
        ),
    );
    // Orders the broker has never seen: the answer `reconcile` and the recovery from `Unknown` rely on.
    add(
        "order_state_unknown",
        outcome(
            "GetOrderState",
            c.orders()
                .get_order_state(pb::GetOrderStateRequest {
                    account_id: account.into(),
                    order_id: UNKNOWN_ORDER.into(),
                    price_type: 0,
                    order_id_type: Some(pb::OrderIdType::Request as i32),
                })
                .await,
        ),
    );
    add(
        "cancel_unknown",
        outcome(
            "CancelOrder",
            c.orders()
                .cancel_order(pb::CancelOrderRequest {
                    account_id: account.into(),
                    order_id: UNKNOWN_ORDER.into(),
                    order_id_type: Some(pb::OrderIdType::Request as i32),
                })
                .await,
        ),
    );
    out
}

fn limit_buy(
    account: &str,
    uid: &str,
    order_id: &str,
    price: &str,
    lots: i64,
) -> pb::PostOrderRequest {
    pb::PostOrderRequest {
        quantity: lots,
        price: Some(money::quotation_from_str(price).expect("a price like 1.50")),
        direction: pb::OrderDirection::Buy as i32,
        account_id: account.into(),
        order_type: pb::OrderType::Limit as i32,
        order_id: order_id.into(),
        instrument_id: uid.into(),
        ..Default::default()
    }
}

/// One limit buy of `lots` lots at `price` (far below the market), its state, the list of open orders, and the
/// cancel. For the sandbox: a real account would get a real order.
pub async fn order_roundtrip(
    c: &TbankClient,
    account: &str,
    uid: &str,
    price: &str,
    lots: i64,
) -> Vec<(String, Outcome)> {
    let order_id = uuid::Uuid::new_v4().to_string();
    let mut out = Vec::new();
    out.push((
        "place_order".to_owned(),
        outcome(
            "PostOrder",
            c.orders()
                .post_order(limit_buy(account, uid, &order_id, price, lots))
                .await,
        ),
    ));
    out.push((
        "order_state".to_owned(),
        outcome(
            "GetOrderState",
            c.orders()
                .get_order_state(pb::GetOrderStateRequest {
                    account_id: account.into(),
                    order_id: order_id.clone(),
                    price_type: 0,
                    order_id_type: Some(pb::OrderIdType::Request as i32),
                })
                .await,
        ),
    ));
    out.push((
        "orders_with_one_open".to_owned(),
        outcome(
            "GetOrders",
            c.orders()
                .get_orders(pb::GetOrdersRequest {
                    account_id: account.into(),
                    advanced_filters: None,
                })
                .await,
        ),
    ));
    out.push((
        "cancel_order".to_owned(),
        outcome(
            "CancelOrder",
            c.orders()
                .cancel_order(pb::CancelOrderRequest {
                    account_id: account.into(),
                    order_id: order_id.clone(),
                    order_id_type: Some(pb::OrderIdType::Request as i32),
                })
                .await,
        ),
    ));
    out
}

/// The same order with the same `order_id` twice: does the broker stand one order or two?
///
/// Returns the second reply and a summary `{"second_post": "answered"|"refused", "second_code": ...,
/// "orders_with_this_order_id": n, "same_exchange_id": bool}`. Both orders are cancelled at the end. For the
/// sandbox only.
pub async fn repeated_order_id(
    c: &TbankClient,
    account: &str,
    uid: &str,
    price: &str,
    lots: i64,
) -> Vec<(String, Outcome)> {
    let order_id = uuid::Uuid::new_v4().to_string();
    let first = c
        .orders()
        .post_order(limit_buy(account, uid, &order_id, price, lots))
        .await;
    let first_exchange_id = first.as_ref().ok().map(|r| r.get_ref().order_id.clone());
    let first = outcome("PostOrder", first);
    let second = c
        .orders()
        .post_order(limit_buy(account, uid, &order_id, price, lots))
        .await;
    let second_exchange_id = second.as_ref().ok().map(|r| r.get_ref().order_id.clone());
    let second = outcome("PostOrder", second);

    let open = c
        .orders()
        .get_orders(pb::GetOrdersRequest {
            account_id: account.into(),
            advanced_filters: None,
        })
        .await
        .map(|r| r.into_inner().orders)
        .unwrap_or_default();
    let standing: Vec<&pb::OrderState> = open
        .iter()
        .filter(|o| o.order_request_id == order_id)
        .collect();

    let summary = json!({
        "second_post": if matches!(second, Outcome::Answered(_)) { "answered" } else { "refused" },
        "second_code": match &second { Outcome::Refused { code, .. } => json!(code), Outcome::Answered(_) => Value::Null },
        "orders_with_this_order_id": standing.len(),
        "same_exchange_id": first_exchange_id.is_some() && first_exchange_id == second_exchange_id,
    });
    // Do not leave anything on the account.
    for id in [first_exchange_id, second_exchange_id]
        .into_iter()
        .flatten()
        .collect::<std::collections::BTreeSet<_>>()
    {
        let _ = c
            .orders()
            .cancel_order(pb::CancelOrderRequest {
                account_id: account.into(),
                order_id: id,
                order_id_type: Some(pb::OrderIdType::Exchange as i32),
            })
            .await;
    }
    vec![
        ("repeat_first_post".to_owned(), first),
        ("repeat_second_post".to_owned(), second),
        ("repeat_summary".to_owned(), Outcome::Answered(summary)),
    ]
}
