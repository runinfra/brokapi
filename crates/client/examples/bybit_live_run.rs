//! A live run on a real Bybit sub-account with a small deposit: a limit order far from the market (placed, seen in
//! the stream, cancelled), then the smallest market buy and an immediate market close, with the commission printed.
//!
//! This program spends real money. It is meant for a separate sub-account that holds about 20 USDT, with a key that
//! cannot withdraw or transfer. Put the pair in `.env` or the environment:
//!
//! ```text
//! BYBIT_API_KEY=...
//! BYBIT_API_SECRET=...
//! ```
//!
//! and run it with one command (`tools/bybit-live-run`):
//!
//! ```text
//! cargo run -p brokapi-client --example bybit_live_run
//! ```
//!
//! The rules the program keeps, each one checked in code and covered by a test against a fake exchange:
//!
//! - It never starts on the demo or the test contour. The contour is checked before the first order and again before
//!   every order: the main host, the main contour, and no sandbox flag from the connector.
//! - It starts only when the USDT balance of the account is from 15 to 25, and the account is clean: no position, no
//!   working order.
//! - The instrument is the most traded USDT linear perpetual whose smallest possible order (the minimum quantity, the
//!   minimum order value and the quantity step, all read from `instruments-info`, none from memory) costs no more than
//!   6 USDT. When no contract fits, the program says so and exits without an order.
//! - The leverage is set to 1 before the first order.
//! - Before every order that opens or adds a position its value must not exceed 6 USDT. A closing order is checked
//!   differently: it may not be larger than the position held, and it is never refused for its value, because
//!   refusing to close would leave the money at risk.
//! - At the end there is no position and no working order, or the program says it loudly and exits with code 1.
//!
//! Exit codes: `0` the run went through, `1` a step failed (the program tries to leave the account clean and says
//! what is left), `2` the key pair or the contour setting is missing or wrong, `3` a rule stopped the run before any
//! order (the balance, an account that is not clean, no contract that fits, the demo contour).
//!
//! The program has no network-free mode. Its logic is tested in this file against a fake exchange and against the
//! imitation of the exchange (`cargo test -p brokapi-client --example bybit_live_run`).

#![cfg_attr(test, allow(dead_code))]

use std::collections::HashMap;
use std::process::ExitCode;
use std::time::Duration;

use async_trait::async_trait;
use brokapi::bybit::protocol::config::{BybitConfig, BybitEnv, MAINNET_HTTP_BASE_URL};
use brokapi::core::{
    ClientOrderId, Decimal, Envelope, Error, Event, ExecutionSource, InstrumentId, OrderExecutor,
    OrderState, OrderType, PlaceOrderRequest, PortfolioSource, Price, Qty, Side, TimeRange,
    UnixNanos,
};
use brokapi::core::{InstrumentSource, StopOrderExecutor};
use brokapi_client::{BrokerConfig, Client, ClientError};
use serde_json::{Value, json};
use tokio::sync::{Mutex, mpsc};

/// The account must hold at least this many USDT.
const BALANCE_MIN: i64 = 15;
/// ... and at most this many.
const BALANCE_MAX: i64 = 25;
/// The most an order that opens a position may be worth, in USDT.
const ORDER_CAP: i64 = 6;
/// The leverage the run sets and uses.
const LEVERAGE: u32 = 1;
/// A contract is liquid enough when it traded at least this many USDT in 24 hours.
const MIN_TURNOVER: i64 = 50_000_000;
/// ... and its best bid and ask are no further apart than this share of the price.
const MAX_SPREAD: &str = "0.001";
/// The order value the exchange wants is met with this margin, and a market buy is priced at the ask plus this share.
const NOTIONAL_MARGIN: &str = "1.05";
const MARKET_SLIPPAGE: &str = "1.01";
/// How far under the best bid the limit order is tried, the farthest first. The exchange refuses a price outside its
/// range, then the next one is tried.
const LADDER: [&str; 5] = ["0.5", "0.7", "0.9", "0.95", "0.98"];
const WAIT: Duration = Duration::from_secs(30);

type Res<T = ()> = Result<T, Stop>;

/// Why the run ended early.
#[derive(Debug, PartialEq)]
enum Stop {
    /// A rule stopped it before any order (exit code 3).
    Rule(String),
    /// A step failed (exit code 1).
    Failed(String),
}

fn failed<T: std::fmt::Display>(what: &str) -> impl FnOnce(T) -> Stop + '_ {
    move |err| Stop::Failed(format!("{what}: {err}"))
}

fn dec(text: &str) -> Decimal {
    text.parse().expect("a decimal constant")
}

// ------------------------------------------------------------------------------------------------ the rules

/// Which contour the connection is on.
#[derive(Debug, Clone, PartialEq)]
struct Contour {
    /// How to name it in messages: the host and the contour.
    label: String,
    /// The main contour on the main host with no sandbox flag, which is the only one the run may trade on.
    real: bool,
}

impl Contour {
    /// What the configuration says about the contour. `sandbox` is the flag the connector reports itself.
    fn of(cfg: &BybitConfig, sandbox: bool) -> Self {
        let url = cfg.http_base_url.to_ascii_lowercase();
        let wrong_host = url.contains("demo") || url.contains("testnet");
        Self {
            label: format!("{} ({})", cfg.http_base_url, cfg.env.as_str()),
            real: cfg.env == BybitEnv::Mainnet && !wrong_host && !sandbox,
        }
    }
}

/// What an order does to the position.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Intent {
    /// Opens or adds: the value of the order is capped.
    Open,
    /// Closes: not larger than the position held (its absolute size).
    Close { held: Decimal },
}

/// The check that stands in front of every order.
struct Guard {
    cap: Decimal,
}

impl Guard {
    fn new() -> Self {
        Self {
            cap: Decimal::from(ORDER_CAP),
        }
    }

    /// `Ok(value of the order)` when the order may go, `Err(why not)` when it may not. `price` is the price the value is
    /// counted at: the limit price, or the ask plus the slippage margin for a market order.
    fn check(
        &self,
        contour: &Contour,
        intent: Intent,
        qty: Decimal,
        price: Decimal,
    ) -> Result<Decimal, String> {
        if !contour.real {
            return Err(format!(
                "the connection is not the main contour of the exchange ({}): the run trades only on a real sub-account",
                contour.label
            ));
        }
        if qty <= Decimal::ZERO || price <= Decimal::ZERO {
            return Err(format!(
                "the quantity {qty} or the price {price} is not positive"
            ));
        }
        let value = qty * price;
        match intent {
            Intent::Open if value > self.cap => Err(format!(
                "the order is worth {value} USDT, the cap is {}",
                self.cap
            )),
            Intent::Close { held } if qty > held.abs() => Err(format!(
                "the closing order is {qty}, larger than the position held {}",
                held.abs()
            )),
            _ => Ok(value),
        }
    }
}

/// The balance gate: from 15 to 25 USDT, otherwise the run ends without an order.
fn balance_gate(total: Decimal) -> Result<(), String> {
    let (lo, hi) = (Decimal::from(BALANCE_MIN), Decimal::from(BALANCE_MAX));
    if total < lo || total > hi {
        return Err(format!(
            "the USDT balance is {total}, the run needs it from {lo} to {hi}: nothing was ordered"
        ));
    }
    Ok(())
}

// --------------------------------------------------------------------------------------------- the contract

/// The contract the run trades and the size of the smallest order on it.
#[derive(Debug, Clone, PartialEq)]
struct Choice {
    symbol: String,
    /// The quantity of the smallest order the exchange takes, on its step.
    qty: Decimal,
    tick: Decimal,
    step: Decimal,
    min_qty: Decimal,
    min_value: Decimal,
    bid: Decimal,
    ask: Decimal,
    /// What the smallest order costs at the ask plus the slippage margin.
    cost: Decimal,
    turnover: Decimal,
}

fn text(v: &Value, key: &str) -> Option<Decimal> {
    v.get(key)?.as_str()?.parse().ok()
}

fn ceil_to(x: Decimal, step: Decimal) -> Decimal {
    (x / step).ceil() * step
}

fn floor_to(x: Decimal, step: Decimal) -> Decimal {
    (x / step).floor() * step
}

/// Why one contract does not fit, or the smallest order on it.
fn size_of(info: &Value, ticker: &Value, cap: Decimal) -> Result<Choice, String> {
    let symbol = info["symbol"].as_str().ok_or("no symbol")?.to_string();
    if info["status"] != "Trading"
        || info["contractType"] != "LinearPerpetual"
        || info["quoteCoin"] != "USDT"
        || info["settleCoin"] != "USDT"
    {
        return Err(format!("{symbol} is not a trading USDT linear perpetual"));
    }
    let lot = &info["lotSizeFilter"];
    let (Some(min_qty), Some(step), Some(min_value)) = (
        text(lot, "minOrderQty"),
        text(lot, "qtyStep"),
        text(lot, "minNotionalValue"),
    ) else {
        return Err(format!(
            "{symbol}: instruments-info has no minOrderQty, qtyStep or minNotionalValue, the smallest order is not known"
        ));
    };
    let Some(tick) = text(&info["priceFilter"], "tickSize") else {
        return Err(format!("{symbol}: no tickSize"));
    };
    if step <= Decimal::ZERO || tick <= Decimal::ZERO {
        return Err(format!("{symbol}: a step is not positive"));
    }
    let leverage = &info["leverageFilter"];
    let one = Decimal::from(LEVERAGE);
    match (text(leverage, "minLeverage"), text(leverage, "maxLeverage")) {
        (Some(lo), Some(hi)) if lo <= one && one <= hi => {}
        _ => return Err(format!("{symbol}: the leverage {LEVERAGE} is not allowed")),
    }
    let (Some(bid), Some(ask), Some(turnover)) = (
        text(ticker, "bid1Price"),
        text(ticker, "ask1Price"),
        text(ticker, "turnover24h"),
    ) else {
        return Err(format!("{symbol}: no quote in the ticker"));
    };
    if bid <= Decimal::ZERO || ask < bid {
        return Err(format!("{symbol}: the quote {bid}/{ask} is not usable"));
    }
    if turnover < Decimal::from(MIN_TURNOVER) {
        return Err(format!("{symbol}: traded only {turnover} USDT in 24 hours"));
    }
    if (ask - bid) / bid > dec(MAX_SPREAD) {
        return Err(format!("{symbol}: the spread {bid}/{ask} is wide"));
    }
    let want = (min_value * dec(NOTIONAL_MARGIN) / bid).max(min_qty);
    let qty = ceil_to(want, step);
    if let Some(max) = text(lot, "maxMktOrderQty")
        && qty > max
    {
        return Err(format!(
            "{symbol}: the smallest order {qty} is over the market order limit"
        ));
    }
    let cost = qty * ask * dec(MARKET_SLIPPAGE);
    if cost > cap {
        return Err(format!(
            "{symbol}: the smallest order is {qty} and costs about {cost} USDT, the cap is {cap}"
        ));
    }
    Ok(Choice {
        symbol,
        qty,
        tick,
        step,
        min_qty,
        min_value,
        bid,
        ask,
        cost,
        turnover,
    })
}

/// The most traded contract whose smallest order fits under the cap. `Err` names the cheapest one that was seen when
/// none fits.
fn choose(infos: &[Value], tickers: &[Value], cap: Decimal) -> Result<Choice, String> {
    let by_symbol: HashMap<&str, &Value> = infos
        .iter()
        .filter_map(|i| Some((i["symbol"].as_str()?, i)))
        .collect();
    let mut ranked: Vec<(Decimal, &Value)> = tickers
        .iter()
        .filter_map(|t| Some((text(t, "turnover24h")?, t)))
        .collect();
    ranked.sort_by_key(|r| std::cmp::Reverse(r.0));
    let mut seen = 0;
    let mut dearest_reason = None;
    let mut cheapest: Option<(Decimal, String)> = None;
    for (_, ticker) in ranked {
        let Some(info) = ticker["symbol"].as_str().and_then(|s| by_symbol.get(s)) else {
            continue;
        };
        match size_of(info, ticker, cap) {
            Ok(choice) => return Ok(choice),
            Err(reason) => {
                seen += 1;
                // The contracts that failed only on cost: remember the cheapest to say how far from the cap it was.
                if let Some(c) = reason
                    .split("costs about ")
                    .nth(1)
                    .and_then(|t| t.split(' ').next())
                    .and_then(|t| t.parse::<Decimal>().ok())
                    && cheapest.as_ref().is_none_or(|(best, _)| c < *best)
                {
                    cheapest = Some((c, reason.clone()));
                }
                dearest_reason.get_or_insert(reason);
            }
        }
    }
    Err(match cheapest {
        Some((cost, reason)) => format!(
            "no liquid USDT perpetual has a smallest order of {cap} USDT or less ({seen} looked at, the cheapest costs about {cost} USDT: {reason})"
        ),
        None => format!(
            "no usable USDT perpetual found ({seen} looked at{})",
            dearest_reason
                .map(|r| format!(", the first reason: {r}"))
                .unwrap_or_default()
        ),
    })
}

// -------------------------------------------------------------------------------------------- the exchange

/// An order to place.
#[derive(Debug, Clone, PartialEq)]
struct Order {
    cid: ClientOrderId,
    symbol: String,
    side: Side,
    qty: Decimal,
    /// `None` for a market order.
    price: Option<Decimal>,
}

/// What the run waits for.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Want {
    Working,
    Filled,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq)]
struct Held {
    symbol: String,
    /// Negative for a short.
    qty: Decimal,
}

#[derive(Debug, Clone, PartialEq)]
struct Fill {
    qty: Decimal,
    price: Decimal,
    fee: Decimal,
}

#[derive(Debug, Clone, PartialEq)]
struct Quote {
    bid: Decimal,
    ask: Decimal,
}

/// Why `place` did not place.
#[derive(Debug, PartialEq)]
enum PlaceError {
    /// The exchange said no. Nothing was placed.
    Refused(String),
    /// The outcome is not known: the order may be standing.
    Unknown(String),
}

/// What the run needs from the exchange. The real one is [`LiveVenue`], the tests have a fake.
#[async_trait]
trait Venue: Send + Sync {
    fn contour(&self) -> Contour;
    async fn usdt_total(&self) -> Res<Decimal>;
    async fn positions(&self) -> Res<Vec<Held>>;
    /// The orders that wait at the exchange: limit orders and conditional ones.
    async fn working_orders(&self) -> Res<usize>;
    /// The rows of `instruments-info` and of `tickers` for the linear category.
    async fn market(&self) -> Res<(Vec<Value>, Vec<Value>)>;
    async fn quote(&self, symbol: &str) -> Res<Quote>;
    async fn set_leverage(&self, symbol: &str, leverage: u32) -> Res;
    /// Subscribes to the orders and the fills, and reconciles the (empty) ledger.
    async fn start(&self, symbol: &str) -> Res;
    async fn place(&self, order: &Order) -> Result<(), PlaceError>;
    /// Waits until the order is in the state. `stream_only` means the private stream must say it.
    async fn wait(&self, cid: ClientOrderId, want: Want, stream_only: bool) -> Res;
    async fn cancel(&self, cid: ClientOrderId) -> Res;
    /// The fills of an order, waited for until they add up to `qty`.
    async fn fills(&self, cid: ClientOrderId, qty: Decimal) -> Res<Vec<Fill>>;
    /// A short pause between two reads of the account, which the exchange updates a moment after a fill. The fake does
    /// not pause.
    async fn pause(&self) {}
}

fn say(text: &str) {
    println!("{text}");
}

/// What the run reports at the end.
#[derive(Debug, PartialEq)]
struct Report {
    symbol: String,
    before: Decimal,
    after: Decimal,
    bought: Vec<Fill>,
    sold: Vec<Fill>,
    fee: Decimal,
}

/// Sends one order through the guard. The guard sees the contour as it is now.
async fn send<V: Venue>(
    v: &V,
    guard: &Guard,
    intent: Intent,
    order: &Order,
    price_for_value: Decimal,
) -> Result<Decimal, PlaceError> {
    let value = guard
        .check(&v.contour(), intent, order.qty, price_for_value)
        .map_err(PlaceError::Refused)?;
    v.place(order).await?;
    Ok(value)
}

async fn clean_state<V: Venue>(v: &V) -> Result<(), String> {
    let positions = v.positions().await.map_err(|s| format!("{s:?}"))?;
    let orders = v.working_orders().await.map_err(|s| format!("{s:?}"))?;
    if positions.is_empty() && orders == 0 {
        Ok(())
    } else {
        Err(format!(
            "{} position(s) {:?} and {orders} working order(s)",
            positions.len(),
            positions
        ))
    }
}

/// `clean_state`, read again a few times: the exchange shows a closed position a moment after the fill.
async fn settled_clean<V: Venue>(v: &V) -> Result<(), String> {
    let mut last = clean_state(v).await;
    for _ in 0..6 {
        if last.is_ok() {
            break;
        }
        v.pause().await;
        last = clean_state(v).await;
    }
    last
}

/// The position on `symbol`, read again a few times while the account shows none.
async fn position_of<V: Venue>(v: &V, symbol: &str) -> Res<Option<Held>> {
    for _ in 0..6 {
        if let Some(held) = v
            .positions()
            .await?
            .into_iter()
            .find(|p| p.symbol == symbol)
        {
            return Ok(Some(held));
        }
        v.pause().await;
    }
    Ok(None)
}

/// Takes back what a failed run left: cancels the orders it placed, closes the position it opened.
async fn clean_up<V: Venue>(v: &V, guard: &Guard, symbol: &str, standing: &[ClientOrderId]) {
    for cid in standing {
        if let Err(e) = v.cancel(*cid).await {
            say(&format!("clean-up: cancel {cid}: {e:?}"));
        }
    }
    // Twice: an order whose outcome was not known may fill a moment later.
    for pass in 0..2 {
        if pass > 0 {
            v.pause().await;
        }
        let Ok(positions) = v.positions().await else {
            say("clean-up: the positions cannot be read, check the account by hand");
            return;
        };
        for held in positions.iter().filter(|p| p.symbol == symbol) {
            let side = if held.qty > Decimal::ZERO {
                Side::Sell
            } else {
                Side::Buy
            };
            let Ok(q) = v.quote(symbol).await else {
                say("clean-up: no quote, close the position by hand");
                return;
            };
            let order = Order {
                cid: ClientOrderId::new_v4(),
                symbol: symbol.to_string(),
                side,
                qty: held.qty.abs(),
                price: None,
            };
            let price = if side == Side::Sell { q.bid } else { q.ask };
            match send(v, guard, Intent::Close { held: held.qty }, &order, price).await {
                Ok(_) => {
                    if let Err(e) = v.wait(order.cid, Want::Filled, false).await {
                        say(&format!(
                            "clean-up: the closing order is not seen filled: {e:?}"
                        ));
                    }
                }
                Err(e) => say(&format!(
                    "clean-up: the closing order was not placed: {e:?}"
                )),
            }
        }
    }
}

/// The run. `Stop::Rule` means no order was placed.
async fn run<V: Venue>(v: &V) -> Res<Report> {
    let guard = Guard::new();

    say("[1] the contour, the balance, the account");
    let contour = v.contour();
    if !contour.real {
        return Err(Stop::Rule(format!(
            "the connection is not the main contour of the exchange ({}): nothing was ordered",
            contour.label
        )));
    }
    say(&format!("    contour: {}", contour.label));
    let before = v.usdt_total().await?;
    balance_gate(before).map_err(Stop::Rule)?;
    say(&format!(
        "    USDT balance {before}, inside {BALANCE_MIN}..{BALANCE_MAX}"
    ));
    let held = v.positions().await?;
    let waiting = v.working_orders().await?;
    if !held.is_empty() || waiting > 0 {
        return Err(Stop::Rule(format!(
            "the account is not clean: positions {held:?}, working orders {waiting}. Use a sub-account that holds nothing: nothing was ordered"
        )));
    }
    say("    no position and no working order");

    say("[2] the contract, from instruments-info and tickers");
    let (infos, tickers) = v.market().await?;
    let choice = choose(&infos, &tickers, guard.cap).map_err(Stop::Rule)?;
    say(&format!(
        "    {}: smallest order {} (minOrderQty {}, step {}, minNotionalValue {} USDT), about {} USDT at the ask {}, turnover {} USDT a day",
        choice.symbol,
        choice.qty,
        choice.min_qty,
        choice.step,
        choice.min_value,
        choice.cost.round_dp(4),
        choice.ask,
        choice.turnover.round_dp(0)
    ));

    say(&format!("[3] leverage {LEVERAGE} on {}", choice.symbol));
    v.set_leverage(&choice.symbol, LEVERAGE).await?;
    v.start(&choice.symbol).await?;

    // From here on a failure takes back what the run placed.
    let mut standing: Vec<ClientOrderId> = Vec::new();
    let result = steps(v, &guard, &choice, before, &mut standing).await;
    match result {
        Ok(report) => Ok(report),
        Err(stop) => {
            say("the run failed, taking back what it left");
            clean_up(v, &guard, &choice.symbol, &standing).await;
            match settled_clean(v).await {
                Ok(()) => say("the account is clean: no position, no working order"),
                Err(left) => say(&format!("THE ACCOUNT IS NOT CLEAN: {left}")),
            }
            Err(stop)
        }
    }
}

async fn steps<V: Venue>(
    v: &V,
    guard: &Guard,
    choice: &Choice,
    before: Decimal,
    standing: &mut Vec<ClientOrderId>,
) -> Res<Report> {
    let symbol = &choice.symbol;

    say("[4] a limit buy far from the market: placed, seen in the stream, cancelled");
    let q = v.quote(symbol).await?;
    let mut limit = None;
    for ratio in LADDER {
        let price = floor_to(q.bid * dec(ratio), choice.tick);
        if price <= Decimal::ZERO {
            continue;
        }
        // The exchange wants a minimum order value at the limit price too, so the quantity grows as the price falls.
        let qty = ceil_to(
            (choice.min_value * dec(NOTIONAL_MARGIN) / price).max(choice.min_qty),
            choice.step,
        );
        let order = Order {
            cid: ClientOrderId::new_v4(),
            symbol: symbol.clone(),
            side: Side::Buy,
            qty,
            price: Some(price),
        };
        match send(v, guard, Intent::Open, &order, price).await {
            Ok(value) => {
                say(&format!(
                    "    placed: buy {} at {price} ({ratio} of the bid {}), worth {} USDT",
                    order.qty,
                    q.bid,
                    value.round_dp(4)
                ));
                standing.push(order.cid);
                limit = Some(order);
                break;
            }
            Err(PlaceError::Refused(why)) => {
                say(&format!("    not placed at {price}, qty {qty}: {why}"))
            }
            Err(PlaceError::Unknown(why)) => {
                standing.push(order.cid);
                return Err(Stop::Failed(format!(
                    "the limit order has an unknown outcome: {why}"
                )));
            }
        }
    }
    let limit = limit.ok_or_else(|| {
        Stop::Failed("the exchange refused the limit order at every price of the ladder".into())
    })?;
    v.wait(limit.cid, Want::Working, true).await?;
    say("    the stream reported the order as working");
    v.cancel(limit.cid).await?;
    v.wait(limit.cid, Want::Cancelled, true).await?;
    standing.clear();
    say("    cancelled, the stream reported it");

    say("[5] the smallest market buy");
    let q = v.quote(symbol).await?;
    let buy = Order {
        cid: ClientOrderId::new_v4(),
        symbol: symbol.clone(),
        side: Side::Buy,
        qty: choice.qty,
        price: None,
    };
    // Before it is sent: if the answer is lost the clean-up looks for the order and the position.
    standing.push(buy.cid);
    let value = send(v, guard, Intent::Open, &buy, q.ask * dec(MARKET_SLIPPAGE))
        .await
        .map_err(|e| Stop::Failed(format!("the market buy: {e:?}")))?;
    say(&format!(
        "    placed: buy {} at market, counted at {} USDT",
        buy.qty,
        value.round_dp(4)
    ));
    v.wait(buy.cid, Want::Filled, false).await?;
    let bought = v.fills(buy.cid, buy.qty).await?;
    let held = position_of(v, symbol).await?.ok_or_else(|| {
        Stop::Failed("the buy is filled but the account shows no position".into())
    })?;
    say(&format!("    filled, the position is {}", held.qty));

    say("[6] the market close of the whole position");
    let q = v.quote(symbol).await?;
    let sell = Order {
        cid: ClientOrderId::new_v4(),
        symbol: symbol.clone(),
        side: Side::Sell,
        qty: held.qty.abs(),
        price: None,
    };
    if held.qty < Decimal::ZERO {
        return Err(Stop::Failed(format!(
            "a long position was bought, the account shows {}",
            held.qty
        )));
    }
    standing.push(sell.cid);
    let value = send(v, guard, Intent::Close { held: held.qty }, &sell, q.bid)
        .await
        .map_err(|e| Stop::Failed(format!("the market close: {e:?}")))?;
    say(&format!(
        "    placed: sell {} at market, about {} USDT",
        sell.qty,
        value.round_dp(4)
    ));
    v.wait(sell.cid, Want::Filled, false).await?;
    let sold = v.fills(sell.cid, sell.qty).await?;
    standing.clear();

    say("[7] the account is clean");
    settled_clean(v)
        .await
        .map_err(|left| Stop::Failed(format!("the run ended but the account holds {left}")))?;
    let after = v.usdt_total().await?;
    let fee: Decimal = bought.iter().chain(&sold).map(|f| f.fee).sum();
    Ok(Report {
        symbol: symbol.clone(),
        before,
        after,
        bought,
        sold,
        fee,
    })
}

fn print_report(r: &Report) {
    say("");
    say(&format!("{}: the run went through", r.symbol));
    for (name, fills) in [("buy ", &r.bought), ("sell", &r.sold)] {
        for f in fills {
            say(&format!(
                "  {name} {} at {}, commission {} USDT",
                f.qty, f.price, f.fee
            ));
        }
    }
    say(&format!("  commission in all: {} USDT", r.fee));
    say(&format!(
        "  USDT balance {} -> {} (the difference {} includes the commission and the price move between the two fills)",
        r.before,
        r.after,
        r.after - r.before
    ));
    say("  no position and no working order are left");
}

// ------------------------------------------------------------------------------------------ the real exchange

struct LiveVenue {
    client: Client,
    contour: Contour,
    events: Mutex<mpsc::Receiver<Envelope>>,
    seen: Mutex<Vec<Event>>,
    ids: Mutex<HashMap<String, InstrumentId>>,
}

impl LiveVenue {
    async fn connect(cfg: BybitConfig) -> Result<Self, String> {
        let client = Client::start(BrokerConfig::Bybit(cfg.clone()), "live-run", None, 1024)
            .await
            .map_err(|e| format!("cannot connect: {e}"))?;
        let sandbox = client.caps().await.is_some_and(|c| c.ud.sandbox);
        let events = client
            .take_events()
            .await
            .ok_or("the event channel is already taken")?;
        Ok(Self {
            client,
            contour: Contour::of(&cfg, sandbox),
            events: Mutex::new(events),
            seen: Mutex::new(Vec::new()),
            ids: Mutex::new(HashMap::new()),
        })
    }

    async fn id(&self, symbol: &str) -> Res<InstrumentId> {
        let mut ids = self.ids.lock().await;
        if let Some(id) = ids.get(symbol) {
            return Ok(id.clone());
        }
        let id = self
            .client
            .instrument(symbol, None)
            .await
            .map_err(failed("instrument"))?;
        // The connector learns the steps of the instrument from here: an order is checked against them.
        self.client
            .trading_params(&id)
            .await
            .map_err(failed("trading_params"))?;
        ids.insert(symbol.to_string(), id.clone());
        Ok(id)
    }

    async fn get(&self, path: &str, query: Value) -> Res<Value> {
        self.client
            .raw(&format!("GET {path}"), &json!({ "query": query }), false)
            .await
            .map_err(failed(path))
    }

    /// Takes the events that are waiting, or waits for one a little.
    async fn pump(&self, wait: Duration) {
        let mut rx = self.events.lock().await;
        let mut seen = self.seen.lock().await;
        if let Ok(Some(env)) = tokio::time::timeout(wait, rx.recv()).await {
            seen.push(env.payload);
        }
        while let Ok(env) = rx.try_recv() {
            seen.push(env.payload);
        }
    }

    async fn stream_state(&self, cid: ClientOrderId, want: Want) -> Option<Result<(), String>> {
        let seen = self.seen.lock().await;
        for ev in seen.iter() {
            if let Event::Order(o) = ev {
                if o.cid != cid {
                    continue;
                }
                match (&o.state, want) {
                    (
                        OrderState::Acked { .. } | OrderState::PartiallyFilled { .. },
                        Want::Working,
                    )
                    | (OrderState::Filled { .. }, Want::Filled)
                    | (OrderState::Cancelled { .. }, Want::Cancelled) => return Some(Ok(())),
                    (OrderState::Rejected { .. }, _) => {
                        return Some(Err(format!("the order was rejected: {:?}", o.state)));
                    }
                    _ => {}
                }
            }
        }
        None
    }
}

#[async_trait]
impl Venue for LiveVenue {
    fn contour(&self) -> Contour {
        self.contour.clone()
    }

    async fn usdt_total(&self) -> Res<Decimal> {
        let p = self.client.portfolio().await.map_err(failed("portfolio"))?;
        let b = p
            .balances
            .iter()
            .find(|b| b.currency == "USDT")
            .ok_or_else(|| {
                Stop::Rule("the account has no USDT balance: nothing was ordered".into())
            })?;
        // Free money and what the margin holds: the equity of the coin.
        Ok(b.available.decimal() + b.blocked.decimal())
    }

    async fn positions(&self) -> Res<Vec<Held>> {
        let rows = self.client.positions().await.map_err(failed("positions"))?;
        Ok(rows
            .into_iter()
            .filter(|p| !p.qty.decimal().is_zero())
            .map(|p| Held {
                symbol: p.instrument.key.symbol.clone(),
                qty: p.qty.decimal(),
            })
            .collect())
    }

    async fn working_orders(&self) -> Res<usize> {
        let orders = self
            .client
            .active_orders()
            .await
            .map_err(failed("active_orders"))?;
        let stops = self
            .client
            .active_stop_orders()
            .await
            .map_err(failed("active_stop_orders"))?;
        Ok(orders.len() + stops.len())
    }

    async fn market(&self) -> Res<(Vec<Value>, Vec<Value>)> {
        let mut infos = Vec::new();
        let mut cursor = String::new();
        for _ in 0..8 {
            let mut query = json!({"category": "linear", "limit": "1000"});
            if !cursor.is_empty() {
                query["cursor"] = json!(cursor);
            }
            let page = self.get("/v5/market/instruments-info", query).await?;
            infos.extend(page["list"].as_array().cloned().unwrap_or_default());
            cursor = page["nextPageCursor"].as_str().unwrap_or("").to_string();
            if cursor.is_empty() {
                break;
            }
        }
        let tickers = self
            .get("/v5/market/tickers", json!({"category": "linear"}))
            .await?;
        Ok((
            infos,
            tickers["list"].as_array().cloned().unwrap_or_default(),
        ))
    }

    async fn quote(&self, symbol: &str) -> Res<Quote> {
        let reply = self
            .get(
                "/v5/market/tickers",
                json!({"category": "linear", "symbol": symbol}),
            )
            .await?;
        let row = reply["list"].get(0).cloned().unwrap_or(Value::Null);
        match (text(&row, "bid1Price"), text(&row, "ask1Price")) {
            (Some(bid), Some(ask)) if bid > Decimal::ZERO && ask > Decimal::ZERO => {
                Ok(Quote { bid, ask })
            }
            _ => Err(Stop::Failed(format!(
                "{symbol}: the ticker has no quote: {row}"
            ))),
        }
    }

    async fn set_leverage(&self, symbol: &str, leverage: u32) -> Res {
        let body = json!({
            "category": "linear",
            "symbol": symbol,
            "buyLeverage": leverage.to_string(),
            "sellLeverage": leverage.to_string(),
        });
        match self
            .client
            .raw(
                "POST /v5/position/set-leverage",
                &json!({ "body": body }),
                true,
            )
            .await
        {
            Ok(_) => Ok(()),
            // "leverage not modified": it is already what we want.
            Err(ClientError::Broker(Error::Rejected { code, .. })) if code == "bybit_110043" => {
                Ok(())
            }
            Err(e) => Err(Stop::Failed(format!("set-leverage: {e}"))),
        }
    }

    async fn start(&self, symbol: &str) -> Res {
        self.id(symbol).await?;
        self.client
            .subscribe_order_state()
            .await
            .map_err(failed("subscribe_order_state"))?;
        self.client
            .subscribe_executions()
            .await
            .map_err(failed("subscribe_executions"))?;
        self.client
            .reconcile(&[])
            .await
            .map_err(failed("reconcile"))?;
        Ok(())
    }

    async fn place(&self, order: &Order) -> Result<(), PlaceError> {
        let id = self
            .id(&order.symbol)
            .await
            .map_err(|e| PlaceError::Refused(format!("{e:?}")))?;
        let req = PlaceOrderRequest {
            cid: order.cid,
            instrument: id,
            side: order.side,
            order_type: if order.price.is_some() {
                OrderType::Limit
            } else {
                OrderType::Market
            },
            qty: Qty::new(order.qty),
            price: order.price.map(Price::new),
        };
        match self.client.place(req).await {
            Ok(_) => Ok(()),
            Err(Error::Unknown) => Err(PlaceError::Unknown("the answer was lost".into())),
            Err(e @ (Error::Rejected { .. } | Error::Config(_))) => {
                Err(PlaceError::Refused(e.to_string()))
            }
            Err(e) => Err(PlaceError::Unknown(e.to_string())),
        }
    }

    async fn wait(&self, cid: ClientOrderId, want: Want, stream_only: bool) -> Res {
        let end = tokio::time::Instant::now() + WAIT;
        loop {
            if let Some(found) = self.stream_state(cid, want).await {
                return found.map_err(Stop::Failed);
            }
            if !stream_only && let Ok(snap) = self.client.order_state(cid).await {
                match (&snap.state, want) {
                    (
                        OrderState::Acked { .. } | OrderState::PartiallyFilled { .. },
                        Want::Working,
                    )
                    | (OrderState::Filled { .. }, Want::Filled)
                    | (OrderState::Cancelled { .. }, Want::Cancelled) => return Ok(()),
                    (OrderState::Rejected { .. }, _) => {
                        return Err(Stop::Failed(format!(
                            "the order was rejected: {:?}",
                            snap.state
                        )));
                    }
                    _ => {}
                }
            }
            if tokio::time::Instant::now() > end {
                return Err(Stop::Failed(format!(
                    "order {cid}: no {want:?} state in {WAIT:?}{}",
                    if stream_only {
                        " in the private stream"
                    } else {
                        ""
                    }
                )));
            }
            self.pump(Duration::from_millis(500)).await;
        }
    }

    async fn cancel(&self, cid: ClientOrderId) -> Res {
        match self.client.cancel(cid).await {
            Ok(()) => Ok(()),
            Err(e) => Err(Stop::Failed(format!("cancel {cid}: {e}"))),
        }
    }

    async fn fills(&self, cid: ClientOrderId, qty: Decimal) -> Res<Vec<Fill>> {
        let end = tokio::time::Instant::now() + WAIT;
        loop {
            // By trade id: the stream and the history may both carry a fill.
            let mut fills: Vec<Fill> = {
                let seen = self.seen.lock().await;
                let mut by_id = std::collections::BTreeMap::new();
                for ev in seen.iter() {
                    if let Event::Execution(x) = ev
                        && x.cid == Some(cid)
                    {
                        by_id.insert(
                            x.trade_id.clone(),
                            Fill {
                                qty: x.qty.decimal(),
                                price: x.price.decimal(),
                                fee: x.commission.decimal(),
                            },
                        );
                    }
                }
                by_id.into_values().collect()
            };
            if fills.iter().map(|f| f.qty).sum::<Decimal>() < qty {
                // The stream may be late: the history of fills has them too.
                let from = UnixNanos::from_nanos(
                    (std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map_or(0, |d| d.as_nanos() as i64))
                        - 600_000_000_000,
                );
                let to = UnixNanos::from_nanos(i64::MAX / 2);
                if let Ok(history) = self.client.executions(TimeRange::new(from, to)).await {
                    fills = history
                        .iter()
                        .filter(|x| x.cid == Some(cid))
                        .map(|x| Fill {
                            qty: x.qty.decimal(),
                            price: x.price.decimal(),
                            fee: x.commission.decimal(),
                        })
                        .collect();
                }
            }
            let got: Decimal = fills.iter().map(|f| f.qty).sum();
            if got >= qty {
                return Ok(fills);
            }
            if tokio::time::Instant::now() > end {
                return Err(Stop::Failed(format!(
                    "order {cid}: the fills add up to {got} of {qty} after {WAIT:?}, the commission is not known"
                )));
            }
            self.pump(Duration::from_millis(500)).await;
        }
    }

    async fn pause(&self) {
        self.pump(Duration::from_millis(700)).await;
    }
}

// ------------------------------------------------------------------------------------------------ the program

/// The key pair of the main contour. A missing variable is named, never its value.
fn keys_from_env() -> Result<(String, String), Vec<&'static str>> {
    let get = |name: &str| std::env::var(name).ok().filter(|v| !v.trim().is_empty());
    match (get("BYBIT_API_KEY"), get("BYBIT_API_SECRET")) {
        (Some(k), Some(s)) => Ok((k, s)),
        (k, s) => {
            let mut missing = Vec::new();
            if k.is_none() {
                missing.push("BYBIT_API_KEY");
            }
            if s.is_none() {
                missing.push("BYBIT_API_SECRET");
            }
            Err(missing)
        }
    }
}

/// The configuration of the run, or the reason it cannot start (exit code 2).
fn config_from(
    key_pair: Result<(String, String), Vec<&'static str>>,
    env_name: Option<String>,
) -> Result<BybitConfig, String> {
    let (key, secret) = key_pair.map_err(|missing| {
        format!(
            "missing {}: put the key pair of the live sub-account in .env or the environment (the demo variables BYBIT_DEMO_API_KEY and BYBIT_DEMO_API_SECRET are not used here)",
            missing.join(" and ")
        )
    })?;
    if let Some(name) = env_name.filter(|n| !n.trim().is_empty())
        && BybitEnv::parse(&name) != Some(BybitEnv::Mainnet)
    {
        return Err(format!(
            "BYBIT_ENV is {name:?}: this run trades on the main contour only, remove the variable"
        ));
    }
    let cfg = BybitConfig::new(key, secret);
    debug_assert_eq!(cfg.http_base_url, MAINNET_HTTP_BASE_URL);
    Ok(cfg)
}

#[tokio::main]
async fn main() -> ExitCode {
    dotenvy::dotenv().ok();
    let cfg = match config_from(keys_from_env(), std::env::var("BYBIT_ENV").ok()) {
        Ok(cfg) => cfg,
        Err(why) => {
            eprintln!("{why}");
            return ExitCode::from(2);
        }
    };
    let venue = match LiveVenue::connect(cfg).await {
        Ok(v) => v,
        Err(why) => {
            eprintln!("{why}");
            return ExitCode::from(1);
        }
    };
    let outcome = run(&venue).await;
    venue
        .client
        .close(brokapi::core::ShutdownMode::Immediate)
        .await;
    match outcome {
        Ok(report) => {
            print_report(&report);
            ExitCode::SUCCESS
        }
        Err(Stop::Rule(why)) => {
            eprintln!("stopped by a rule, no order was placed: {why}");
            ExitCode::from(3)
        }
        Err(Stop::Failed(why)) => {
            eprintln!("FAILED: {why}");
            ExitCode::from(1)
        }
    }
}

// ------------------------------------------------------------------------------------------------- the tests

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex as StdMutex;

    fn info(symbol: &str, tick: &str, step: &str, min_qty: &str, min_value: &str) -> Value {
        json!({
            "symbol": symbol, "contractType": "LinearPerpetual", "status": "Trading",
            "quoteCoin": "USDT", "settleCoin": "USDT",
            "leverageFilter": {"minLeverage": "1", "maxLeverage": "100.00", "leverageStep": "0.01"},
            "priceFilter": {"minPrice": tick, "maxPrice": "1999999.80", "tickSize": tick},
            "lotSizeFilter": {"maxOrderQty": "1500000", "minOrderQty": min_qty, "qtyStep": step,
                              "maxMktOrderQty": "150000", "minNotionalValue": min_value},
        })
    }

    fn ticker(symbol: &str, bid: &str, ask: &str, turnover: &str) -> Value {
        json!({"symbol": symbol, "bid1Price": bid, "ask1Price": ask, "turnover24h": turnover})
    }

    /// The market of the tests: BTC is the most traded and costs far more than the cap, ETH too, XRP is third and fits.
    fn market() -> (Vec<Value>, Vec<Value>) {
        (
            vec![
                info("BTCUSDT", "0.10", "0.001", "0.001", "5"),
                info("ETHUSDT", "0.01", "0.01", "0.01", "5"),
                info("XRPUSDT", "0.0001", "0.1", "0.1", "5"),
                info("DOGEUSDT", "0.00001", "1", "1", "5"),
            ],
            vec![
                ticker("DOGEUSDT", "0.20000", "0.20010", "900000000"),
                ticker("BTCUSDT", "80000.0", "80000.1", "9000000000"),
                ticker("XRPUSDT", "2.0000", "2.0001", "1200000000"),
                ticker("ETHUSDT", "3000.00", "3000.01", "5000000000"),
            ],
        )
    }

    #[test]
    fn the_contract_is_the_most_traded_one_whose_smallest_order_fits() {
        let (infos, tickers) = market();
        let choice = choose(&infos, &tickers, Decimal::from(6)).unwrap();
        // BTC (80 USDT for 0.001) and ETH (30 for 0.01) are dearer than 6, XRP is next by turnover.
        assert_eq!(choice.symbol, "XRPUSDT");
        // 5 * 1.05 / 2.0 = 2.625, on the step 0.1 -> 2.7
        assert_eq!(choice.qty, dec("2.7"));
        assert!(choice.cost <= Decimal::from(6), "{}", choice.cost);
        assert!(choice.qty * choice.bid >= choice.min_value);
    }

    #[test]
    fn the_minimum_order_value_of_the_exchange_decides_not_a_number_in_the_code() {
        // The same contract with a minimum order value of 10 does not fit under 6.
        let (_, tickers) = market();
        let infos = vec![info("XRPUSDT", "0.0001", "0.1", "0.1", "10")];
        let why = choose(&infos, &tickers, Decimal::from(6)).unwrap_err();
        assert!(why.contains("XRPUSDT") && why.contains("cap is 6"), "{why}");
    }

    #[test]
    fn a_contract_with_no_known_minimum_is_not_used() {
        let mut bad = info("XRPUSDT", "0.0001", "0.1", "0.1", "5");
        bad["lotSizeFilter"]
            .as_object_mut()
            .unwrap()
            .remove("minNotionalValue");
        let (_, tickers) = market();
        let why = choose(&[bad], &tickers, Decimal::from(6)).unwrap_err();
        assert!(why.contains("not known"), "{why}");
    }

    #[test]
    fn when_nothing_fits_the_text_says_what_the_cheapest_costs() {
        let (infos, tickers) = market();
        let why = choose(&infos[..2], &tickers, Decimal::from(6)).unwrap_err();
        assert!(
            why.contains("no liquid USDT perpetual") && why.contains("cheapest costs about"),
            "{why}"
        );
    }

    #[test]
    fn a_thin_or_wide_market_is_skipped() {
        let (infos, mut tickers) = market();
        for t in &mut tickers {
            if t["symbol"] == "XRPUSDT" {
                t["turnover24h"] = json!("1000");
            }
            if t["symbol"] == "DOGEUSDT" {
                t["ask1Price"] = json!("0.30000");
            }
        }
        let why = choose(&infos, &tickers, Decimal::from(6)).unwrap_err();
        assert!(why.contains("no"), "{why}");
    }

    #[test]
    fn the_balance_gate_is_from_15_to_25() {
        for ok in ["15", "20", "25"] {
            assert!(balance_gate(dec(ok)).is_ok(), "{ok}");
        }
        for bad in ["14.99", "0", "25.01", "10000"] {
            assert!(balance_gate(dec(bad)).is_err(), "{bad}");
        }
    }

    fn real() -> Contour {
        Contour {
            label: "test".into(),
            real: true,
        }
    }

    #[test]
    fn the_guard_refuses_the_demo_contour_and_a_large_order() {
        let g = Guard::new();
        let demo = Contour {
            label: "demo".into(),
            real: false,
        };
        assert!(
            g.check(&demo, Intent::Open, dec("1"), dec("1"))
                .unwrap_err()
                .contains("not the main contour")
        );
        assert!(
            g.check(&demo, Intent::Close { held: dec("1") }, dec("1"), dec("1"))
                .is_err()
        );
        assert_eq!(
            g.check(&real(), Intent::Open, dec("3"), dec("2")),
            Ok(dec("6"))
        );
        assert!(
            g.check(&real(), Intent::Open, dec("3"), dec("2.01"))
                .unwrap_err()
                .contains("cap is 6")
        );
        assert!(g.check(&real(), Intent::Open, dec("0"), dec("1")).is_err());
    }

    #[test]
    fn a_closing_order_is_never_larger_than_the_position_and_is_not_capped_by_value() {
        let g = Guard::new();
        let held = Intent::Close { held: dec("3") };
        assert_eq!(g.check(&real(), held, dec("3"), dec("100")), Ok(dec("300")));
        assert!(
            g.check(&real(), held, dec("3.1"), dec("1"))
                .unwrap_err()
                .contains("larger than the position")
        );
        let short = Intent::Close { held: dec("-3") };
        assert!(g.check(&real(), short, dec("3"), dec("1")).is_ok());
    }

    #[test]
    fn the_contour_is_real_only_on_the_main_host_with_no_sandbox_flag() {
        let main = BybitConfig::new("k", "s");
        assert!(Contour::of(&main, false).real);
        assert!(!Contour::of(&main, true).real, "the connector says sandbox");
        assert!(!Contour::of(&main.clone().with_env(BybitEnv::Demo), false).real);
        assert!(!Contour::of(&main.clone().with_env(BybitEnv::Testnet), false).real);
        let mut odd = main;
        odd.http_base_url = "https://api-demo.bybit.com".into();
        assert!(
            !Contour::of(&odd, false).real,
            "the host says demo although the env says main"
        );
    }

    #[test]
    fn the_setup_names_the_missing_variables_and_refuses_another_contour() {
        let why = config_from(Err(vec!["BYBIT_API_KEY", "BYBIT_API_SECRET"]), None).unwrap_err();
        assert!(why.contains("BYBIT_API_KEY and BYBIT_API_SECRET"), "{why}");
        let pair = || Ok(("key".to_string(), "secret".to_string()));
        assert!(config_from(pair(), None).is_ok());
        assert!(config_from(pair(), Some("mainnet".into())).is_ok());
        assert!(config_from(pair(), Some(String::new())).is_ok());
        for other in ["demo", "testnet", "nonsense"] {
            assert!(
                config_from(pair(), Some(other.into()))
                    .unwrap_err()
                    .contains("main contour only"),
                "{other}"
            );
        }
    }

    // ---- the fake exchange

    #[derive(Debug, Clone, PartialEq)]
    enum Call {
        Place(Order),
        Cancel(ClientOrderId),
        Leverage(String, u32),
    }

    struct Fake {
        contour: Contour,
        balance: Decimal,
        held: StdMutex<Vec<Held>>,
        waiting: StdMutex<usize>,
        calls: StdMutex<Vec<Call>>,
        /// Limit prices above this share of the bid are refused (the exchange's range), 0 refuses none.
        refuse_below_ratio: Decimal,
        /// Every wait for this state fails, as if the exchange never got there.
        fail_wait_for: StdMutex<Option<Want>>,
        fee_rate: Decimal,
        /// The contour reads as the main one this many times, then as the demo one.
        real_reads: StdMutex<usize>,
        /// The market buy takes effect but its answer is lost.
        lose_market_buy_answer: bool,
    }

    impl Fake {
        fn new() -> Self {
            Self {
                contour: real(),
                balance: dec("20"),
                held: StdMutex::new(Vec::new()),
                waiting: StdMutex::new(0),
                calls: StdMutex::new(Vec::new()),
                refuse_below_ratio: Decimal::ZERO,
                fail_wait_for: StdMutex::new(None),
                fee_rate: dec("0.00055"),
                real_reads: StdMutex::new(usize::MAX),
                lose_market_buy_answer: false,
            }
        }

        fn calls(&self) -> Vec<Call> {
            self.calls.lock().unwrap().clone()
        }

        fn orders_placed(&self) -> usize {
            self.calls()
                .iter()
                .filter(|c| matches!(c, Call::Place(_)))
                .count()
        }
    }

    #[async_trait]
    impl Venue for Fake {
        fn contour(&self) -> Contour {
            let mut left = self.real_reads.lock().unwrap();
            if *left == 0 {
                return Contour {
                    label: "demo".into(),
                    real: false,
                };
            }
            *left = left.saturating_sub(1);
            self.contour.clone()
        }
        async fn usdt_total(&self) -> Res<Decimal> {
            Ok(self.balance)
        }
        async fn positions(&self) -> Res<Vec<Held>> {
            Ok(self.held.lock().unwrap().clone())
        }
        async fn working_orders(&self) -> Res<usize> {
            Ok(*self.waiting.lock().unwrap())
        }
        async fn market(&self) -> Res<(Vec<Value>, Vec<Value>)> {
            Ok(market())
        }
        async fn quote(&self, symbol: &str) -> Res<Quote> {
            assert_eq!(symbol, "XRPUSDT");
            Ok(Quote {
                bid: dec("2.0000"),
                ask: dec("2.0001"),
            })
        }
        async fn set_leverage(&self, symbol: &str, leverage: u32) -> Res {
            self.calls
                .lock()
                .unwrap()
                .push(Call::Leverage(symbol.into(), leverage));
            Ok(())
        }
        async fn start(&self, _: &str) -> Res {
            Ok(())
        }
        async fn place(&self, order: &Order) -> Result<(), PlaceError> {
            self.calls.lock().unwrap().push(Call::Place(order.clone()));
            if let Some(price) = order.price {
                // The fake range: a buy must be at or above this share of the bid.
                if price < dec("2.0000") * self.refuse_below_ratio {
                    return Err(PlaceError::Refused(
                        "bybit_110003 Order price exceeds the allowable range.".into(),
                    ));
                }
                *self.waiting.lock().unwrap() += 1;
                return Ok(());
            }
            let mut held = self.held.lock().unwrap();
            let delta = if order.side == Side::Buy {
                order.qty
            } else {
                -order.qty
            };
            let total = held.iter().map(|h| h.qty).sum::<Decimal>() + delta;
            held.clear();
            if !total.is_zero() {
                held.push(Held {
                    symbol: order.symbol.clone(),
                    qty: total,
                });
            }
            if self.lose_market_buy_answer && order.side == Side::Buy {
                return Err(PlaceError::Unknown("the answer was lost".into()));
            }
            Ok(())
        }
        async fn wait(&self, _: ClientOrderId, want: Want, _: bool) -> Res {
            if *self.fail_wait_for.lock().unwrap() == Some(want) {
                return Err(Stop::Failed(format!("no {want:?}")));
            }
            Ok(())
        }
        async fn cancel(&self, cid: ClientOrderId) -> Res {
            self.calls.lock().unwrap().push(Call::Cancel(cid));
            let mut waiting = self.waiting.lock().unwrap();
            *waiting = waiting.saturating_sub(1);
            Ok(())
        }
        async fn fills(&self, cid: ClientOrderId, qty: Decimal) -> Res<Vec<Fill>> {
            let order = self
                .calls()
                .into_iter()
                .find_map(|c| match c {
                    Call::Place(o) if o.cid == cid => Some(o),
                    _ => None,
                })
                .unwrap();
            let price = if order.side == Side::Buy {
                dec("2.0001")
            } else {
                dec("2.0000")
            };
            Ok(vec![Fill {
                qty,
                price,
                fee: qty * price * self.fee_rate,
            }])
        }
    }

    #[tokio::test]
    async fn the_run_goes_through_and_leaves_nothing() {
        let v = Fake::new();
        let report = run(&v).await.unwrap();
        assert_eq!(report.symbol, "XRPUSDT");
        assert!(v.positions().await.unwrap().is_empty());
        assert_eq!(v.working_orders().await.unwrap(), 0);

        let calls = v.calls();
        assert_eq!(calls[0], Call::Leverage("XRPUSDT".into(), 1));
        let orders: Vec<&Order> = calls
            .iter()
            .filter_map(|c| {
                if let Call::Place(o) = c {
                    Some(o)
                } else {
                    None
                }
            })
            .collect();
        // A limit buy, its cancel, a market buy, a market sell of the same size.
        assert_eq!(orders.len(), 3);
        assert!(orders[0].price.is_some() && orders[0].side == Side::Buy);
        // The limit order meets the minimum order value at its own price (5 USDT) and stays under the cap.
        let limit_value = orders[0].qty * orders[0].price.unwrap();
        assert!(
            limit_value >= dec("5") && limit_value <= dec("6"),
            "{limit_value}"
        );
        assert!(
            orders[0].price.unwrap() <= dec("2.0000") * dec("0.5"),
            "far from the market"
        );
        assert!(matches!(calls[2], Call::Cancel(c) if c == orders[0].cid));
        assert!(orders[1].price.is_none() && orders[1].side == Side::Buy);
        assert!(orders[2].price.is_none() && orders[2].side == Side::Sell);
        assert_eq!(orders[1].qty, orders[2].qty);
        // Every order that opens something is under the cap.
        for o in &orders[..2] {
            assert!(o.qty * o.price.unwrap_or(dec("2.0001") * dec("1.01")) <= Decimal::from(6));
        }
        // The commission is the sum of the two fills.
        assert_eq!(report.fee, report.bought[0].fee + report.sold[0].fee);
        assert!(report.fee > Decimal::ZERO);
    }

    #[tokio::test]
    async fn a_balance_out_of_range_ends_the_run_with_no_order() {
        for balance in ["14", "26", "10000"] {
            let mut v = Fake::new();
            v.balance = dec(balance);
            let stop = run(&v).await.unwrap_err();
            assert!(
                matches!(&stop, Stop::Rule(why) if why.contains("nothing was ordered")),
                "{balance}: {stop:?}"
            );
            assert!(v.calls().is_empty(), "{balance}: {:?}", v.calls());
        }
    }

    #[tokio::test]
    async fn the_demo_contour_ends_the_run_with_no_order() {
        let mut v = Fake::new();
        v.contour = Contour {
            label: "demo".into(),
            real: false,
        };
        let stop = run(&v).await.unwrap_err();
        assert!(matches!(stop, Stop::Rule(_)));
        assert!(v.calls().is_empty());
    }

    #[tokio::test]
    async fn an_account_that_is_not_clean_ends_the_run_with_no_order() {
        let v = Fake::new();
        v.held.lock().unwrap().push(Held {
            symbol: "BTCUSDT".into(),
            qty: dec("0.001"),
        });
        assert!(matches!(run(&v).await.unwrap_err(), Stop::Rule(why) if why.contains("not clean")));
        assert!(v.calls().is_empty());
        let v = Fake::new();
        *v.waiting.lock().unwrap() = 1;
        assert!(matches!(run(&v).await.unwrap_err(), Stop::Rule(_)));
        assert!(v.calls().is_empty());
    }

    #[tokio::test]
    async fn when_no_contract_fits_the_run_ends_with_no_order() {
        struct Dear(Fake);
        #[async_trait]
        impl Venue for Dear {
            fn contour(&self) -> Contour {
                self.0.contour()
            }
            async fn usdt_total(&self) -> Res<Decimal> {
                self.0.usdt_total().await
            }
            async fn positions(&self) -> Res<Vec<Held>> {
                self.0.positions().await
            }
            async fn working_orders(&self) -> Res<usize> {
                self.0.working_orders().await
            }
            async fn market(&self) -> Res<(Vec<Value>, Vec<Value>)> {
                let (infos, tickers) = market();
                Ok((infos[..2].to_vec(), tickers))
            }
            async fn quote(&self, s: &str) -> Res<Quote> {
                self.0.quote(s).await
            }
            async fn set_leverage(&self, s: &str, l: u32) -> Res {
                self.0.set_leverage(s, l).await
            }
            async fn start(&self, s: &str) -> Res {
                self.0.start(s).await
            }
            async fn place(&self, o: &Order) -> Result<(), PlaceError> {
                self.0.place(o).await
            }
            async fn wait(&self, c: ClientOrderId, w: Want, s: bool) -> Res {
                self.0.wait(c, w, s).await
            }
            async fn cancel(&self, c: ClientOrderId) -> Res {
                self.0.cancel(c).await
            }
            async fn fills(&self, c: ClientOrderId, q: Decimal) -> Res<Vec<Fill>> {
                self.0.fills(c, q).await
            }
        }
        let v = Dear(Fake::new());
        let stop = run(&v).await.unwrap_err();
        assert!(
            matches!(&stop, Stop::Rule(why) if why.contains("no liquid USDT perpetual")),
            "{stop:?}"
        );
        assert!(v.0.calls().is_empty());
    }

    #[tokio::test]
    async fn the_limit_price_is_tried_nearer_when_the_exchange_refuses_the_range() {
        let mut v = Fake::new();
        // The exchange takes only prices from 0.9 of the bid: the ladder passes 0.5 and 0.7 and stops at 0.9.
        v.refuse_below_ratio = dec("0.9");
        run(&v).await.unwrap();
        let limits: Vec<Decimal> = v
            .calls()
            .iter()
            .filter_map(|c| match c {
                Call::Place(o) => o.price,
                _ => None,
            })
            .collect();
        assert_eq!(limits, vec![dec("1.0000"), dec("1.4000"), dec("1.8000")]);
        assert!(v.positions().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn when_every_price_of_the_ladder_is_refused_the_run_fails_clean() {
        let mut v = Fake::new();
        v.refuse_below_ratio = dec("1.5");
        let stop = run(&v).await.unwrap_err();
        assert!(
            matches!(&stop, Stop::Failed(why) if why.contains("every price of the ladder")),
            "{stop:?}"
        );
        assert_eq!(v.orders_placed(), LADDER.len());
        assert!(v.positions().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_failure_after_the_buy_closes_the_position() {
        let v = Fake::new();
        // The buy is filled at the fake, but the wait for the fill of the buy fails: the position must still be taken back.
        // The fake fails every wait for Filled, so the clean-up cannot see the close filled either, and says so.
        *v.fail_wait_for.lock().unwrap() = Some(Want::Filled);
        let stop = run(&v).await.unwrap_err();
        assert!(matches!(stop, Stop::Failed(_)));
        assert!(
            v.positions().await.unwrap().is_empty(),
            "the position was closed by the clean-up"
        );
        assert_eq!(v.working_orders().await.unwrap(), 0);
        let sells = v
            .calls()
            .iter()
            .filter(|c| matches!(c, Call::Place(o) if o.side == Side::Sell))
            .count();
        assert_eq!(sells, 1);
    }

    #[tokio::test]
    async fn a_lost_answer_of_the_market_buy_still_ends_with_no_position() {
        let mut v = Fake::new();
        v.lose_market_buy_answer = true;
        let stop = run(&v).await.unwrap_err();
        assert!(
            matches!(&stop, Stop::Failed(why) if why.contains("market buy")),
            "{stop:?}"
        );
        assert!(v.positions().await.unwrap().is_empty());
        assert_eq!(v.working_orders().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn the_contour_is_checked_again_before_every_order() {
        // The first read (the start of the run) says main, every later one demo: no order may go.
        let v = Fake::new();
        *v.real_reads.lock().unwrap() = 1;
        let stop = run(&v).await.unwrap_err();
        assert!(
            matches!(&stop, Stop::Failed(why) if why.contains("every price of the ladder")),
            "{stop:?}"
        );
        assert_eq!(v.orders_placed(), 0);
    }

    #[tokio::test]
    async fn a_failure_after_the_limit_order_cancels_it() {
        let v = Fake::new();
        *v.fail_wait_for.lock().unwrap() = Some(Want::Cancelled);
        let stop = run(&v).await.unwrap_err();
        assert!(matches!(stop, Stop::Failed(_)));
        assert_eq!(
            v.working_orders().await.unwrap(),
            0,
            "the limit order was cancelled"
        );
        assert_eq!(v.orders_placed(), 1, "no market order after a failure");
    }

    async fn imitation(
        env: BybitEnv,
    ) -> (
        LiveVenue,
        std::sync::Arc<brokapi_mocks::bybit::MockOptions>,
        brokapi_mocks::bybit::MockServer,
    ) {
        let options = std::sync::Arc::new(brokapi_mocks::bybit::MockOptions::default());
        let server = brokapi_mocks::bybit::MockServer::start(std::sync::Arc::clone(&options)).await;
        let mut cfg = BybitConfig::for_mock(
            brokapi_mocks::bybit::DEFAULT_KEY,
            brokapi_mocks::bybit::SECRET,
            server.http_base.clone(),
        );
        cfg.env = env;
        (LiveVenue::connect(cfg).await.unwrap(), options, server)
    }

    #[tokio::test]
    async fn the_real_venue_on_a_demo_configuration_stops_before_any_order() {
        // The configuration for the imitation says demo, as the configuration of the demo account does.
        let (venue, options, _server) = imitation(BybitEnv::Demo).await;
        assert!(!venue.contour().real);
        let stop = run(&venue).await.unwrap_err();
        assert!(
            matches!(&stop, Stop::Rule(why) if why.contains("not the main contour")),
            "{stop:?}"
        );
        assert_eq!(
            options
                .order_commands
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
    }

    #[tokio::test]
    async fn the_real_venue_reads_the_wallet_and_stops_at_the_balance_gate() {
        // As the main contour, pointed at the imitation: its wallet holds 10000 USDT, far over 25.
        let (venue, options, _server) = imitation(BybitEnv::Mainnet).await;
        assert!(venue.contour().real);
        assert_eq!(venue.usdt_total().await.unwrap(), dec("10000.5"));
        let stop = run(&venue).await.unwrap_err();
        assert!(
            matches!(&stop, Stop::Rule(why) if why.contains("balance is 10000.5")),
            "{stop:?}"
        );
        assert_eq!(
            options
                .order_commands
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
    }

    #[tokio::test]
    async fn the_real_venue_reads_what_the_choice_needs() {
        let (venue, _options, _server) = imitation(BybitEnv::Mainnet).await;
        let (infos, tickers) = venue.market().await.unwrap();
        assert_eq!(infos.len(), 3, "the three instruments of the imitation");
        assert_eq!(tickers.len(), 3, "a ticker for each instrument");
        let q = venue.quote("BTCUSDT").await.unwrap();
        assert!(q.bid < q.ask);
        // The rows of the imitation have the fields the choice reads. MOCKUSDT (a step of 10 at 100 USDT) costs about
        // 1010 USDT for its smallest order, the dated future is not a perpetual, BTCUSDT is the one that fits 1000.
        let c = choose(&infos, &tickers, Decimal::from(1000)).unwrap();
        assert_eq!((c.symbol.as_str(), c.qty), ("BTCUSDT", dec("0.001")));
        // Under the cap of the run nothing of the imitation fits, and the text says what the cheapest costs.
        let why = choose(&infos, &tickers, Decimal::from(6)).unwrap_err();
        assert!(why.contains("cheapest costs about 82."), "{why}");
    }
}
