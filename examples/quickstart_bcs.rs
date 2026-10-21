//! Quick start on BCS: read first, then one order far from the market, cancelled at once.
//!
//! This program is the code of the page "Быстрый старт" of the book (docs/src/rust/quickstart.md). The book
//! includes the marked parts below, so the text and the code that is tested cannot drift apart.
//!
//! By default it runs against the in-process imitation of BCS (`brokapi-mocks`): no network, no account,
//! no money. With `BROKAPI_LIVE=1` it talks to the REAL BCS. BCS has no sandbox: the account is real. The
//! order step runs only when the token is a write token (`BCS_IS_WRITE=true`), and the order is one
//! minimal lot priced far below the market, cancelled right after it is accepted.
//!
//! ```text
//! cargo run --example quickstart_bcs
//! BROKAPI_LIVE=1 cargo run --example quickstart_bcs
//! ```
#![allow(dead_code)]

use std::time::Duration;

// ANCHOR: imports
use brokapi::bcs::limits::default_limits;
use brokapi::bcs::protocol::config::BcsConfig;
use brokapi::bcs::{MarketDataConfig, MarketDataConnector, UserDataConfig, UserDataConnector};
use brokapi::core::{
    ClientOrderId, Decimal, Envelope, Event, InstrumentId, InstrumentKey, InstrumentSource,
    MarketDataSource, NativeId, OrderBook, OrderBookSource, OrderExecutor, OrderState, OrderType,
    PlaceOrderRequest, PortfolioSource, RoundMode, ShutdownMode, Side, TimeRange, Timeframe,
    UnixNanos,
};
use brokapi::runtime::BrokerLimiter;
use tokio::sync::mpsc;
// ANCHOR_END: imports
use tokio::time::timeout;

/// The price of the test order is this share of the best bid. Far enough that it does not fill in a normal
/// session, near enough that the exchange does not refuse it as being outside the allowed corridor.
const FAR_FROM_MARKET: Decimal = Decimal::from_parts(9, 0, 0, false, 1);

/// The name of a state, for a short line of output.
fn state_name(state: &OrderState) -> &'static str {
    state.name()
}

/// Reads the event channel until the order `cid` reaches the state called `want`, and prints every state it
/// goes through. Gives up after `how_long`: an event is a notice, the answer of `order_state` is the proof.
async fn wait_for_order_event(
    events: &mut mpsc::Receiver<Envelope>,
    cid: ClientOrderId,
    want: &str,
    how_long: Duration,
) {
    let seen = timeout(how_long, async {
        while let Some(env) = events.recv().await {
            if let Event::Order(order) = env.payload
                && order.cid == cid
            {
                let name = state_name(&order.state);
                println!("event: order {cid} is {name}");
                if name == want {
                    return;
                }
            }
        }
    })
    .await;
    if seen.is_err() {
        println!(
            "no event {want} within {how_long:?}, the state is checked with order_state below"
        );
    }
}

/// Runs the whole quick start. `live` is `BROKAPI_LIVE=1`.
pub async fn run(live: bool) -> Result<(), Box<dyn std::error::Error>> {
    // ANCHOR: connect
    // The mock server must live as long as the connectors.
    let (cfg, _mock) = if live {
        // BCS_REFRESH_TOKEN and BCS_IS_WRITE come from the environment (a .env file works)
        dotenvy::dotenv().ok();
        (BcsConfig::from_env()?, None)
    } else {
        let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
        let cfg = BcsConfig::for_mock("mock-refresh-token", &server.http_base).with_write(true);
        (cfg, Some(server))
    };
    let can_trade = cfg.is_write;

    // One limiter for the token: BCS counts its limits per token, both connectors share it.
    let limiter = BrokerLimiter::new(&default_limits());
    // One channel for all events of both connectors.
    let (tx, mut events) = mpsc::channel::<Envelope>(1024);

    let md = MarketDataConnector::start(
        MarketDataConfig::new(cfg.clone(), "bcs-md"),
        limiter.clone(),
        tx.clone(),
    )
    .await?;
    let ud = UserDataConnector::start(UserDataConfig::new(cfg, "bcs-ud"), limiter, tx).await?;
    println!("connected: bcs-md and bcs-ud started");
    // ANCHOR_END: connect

    let sber = InstrumentId::new(
        InstrumentKey::new("SBER", "TQBR"),
        NativeId::TickerClass {
            ticker: "SBER".into(),
            class_code: "TQBR".into(),
        },
    );

    // ANCHOR: read
    // 1. Portfolio: money and positions of the account.
    let portfolio = ud.portfolio().await?;
    println!(
        "portfolio: {} balance(s), {} position(s)",
        portfolio.balances.len(),
        portfolio.positions.len()
    );
    for b in &portfolio.balances {
        println!(
            "  money {}: {} free, {} blocked",
            b.currency, b.available, b.blocked
        );
    }

    // 2. Order book: best bid and best ask.
    let OrderBook::Snapshot { bids, asks, .. } = md.order_book(&sber, 5).await? else {
        return Err("REST answers a snapshot".into());
    };
    let best_bid = bids.first().map(|l| l.price);
    println!(
        "order book SBER: best bid {:?}, best ask {:?}",
        best_bid,
        asks.first().map(|l| l.price)
    );

    // 3. Candles: the last hour of five-minute bars.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos() as i64;
    let hour = 3_600_000_000_000;
    let bars = md
        .candles(
            &sber,
            Timeframe::M5,
            TimeRange::new(
                UnixNanos::from_nanos(now - hour),
                UnixNanos::from_nanos(now),
            ),
        )
        .await?;
    println!(
        "candles M5: {} bar(s), last close {:?}",
        bars.len(),
        bars.last().map(|c| c.close)
    );
    // ANCHOR_END: read

    if !can_trade {
        println!("the token is read-only (BCS_IS_WRITE is not true): the order step is skipped");
        md.shutdown(ShutdownMode::Immediate).await;
        ud.shutdown(ShutdownMode::Immediate).await;
        return Ok(());
    }

    // ANCHOR: warning
    if live {
        println!(
            "WARNING: BCS has no sandbox. The account is real, the order below goes to the exchange."
        );
    }
    // ANCHOR_END: warning

    // ANCHOR: order
    // The price: far below the best bid, a multiple of the price step. The quantity: the minimum.
    let params = md.trading_params(&sber).await?;
    let reference = best_bid
        .or_else(|| bars.last().map(|c| c.close))
        .ok_or("no price to start from: the book and the candles are empty")?;
    // The rounding is exact: the price is a whole number of steps, no 0.9999999 noise.
    let step = params.price_step;
    let price = (reference * FAR_FROM_MARKET).round_to_step(step, RoundMode::Down)?;
    // Tell the connector the step: a price off the grid is then refused before anything is sent.
    ud.price_grid().learn(&params);
    println!(
        "order: buy {} at {price} (best bid {reference}, price step {step})",
        params.min_qty
    );

    // The gate is closed until the first reconcile. The ledger of this new bot is empty. Subscribe first,
    // so no event between the snapshot of the reconcile and the subscription is lost.
    ud.subscribe_order_state().await?;
    let report = ud.reconcile(&[]).await?;
    println!(
        "reconcile: {} foreign order(s) on the account",
        report.foreign.len()
    );

    // The cid is made by the bot and belongs in its journal before the order is sent.
    let cid = ClientOrderId::new_v4();
    // Here the bot writes `cid` to its journal as "sending".
    println!("cid {cid}");
    let ack = ud
        .place(PlaceOrderRequest {
            cid,
            instrument: sber.clone(),
            side: Side::Buy,
            order_type: OrderType::Limit,
            qty: params.min_qty,
            price: Some(price),
        })
        .await?;
    println!("accepted: broker order id {}", ack.broker_id.as_str());
    wait_for_order_event(&mut events, cid, "Acked", Duration::from_secs(5)).await;

    // Cancel at once and look at the final state.
    ud.cancel(cid).await?;
    wait_for_order_event(&mut events, cid, "Cancelled", Duration::from_secs(5)).await;
    let last = timeout(Duration::from_secs(10), async {
        loop {
            let snapshot = ud.order_state(cid).await?;
            if matches!(snapshot.state, OrderState::Cancelled { .. }) {
                break Ok::<_, brokapi::core::Error>(snapshot);
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await??;
    println!("order_state: {}", state_name(&last.state));
    // ANCHOR_END: order

    // ANCHOR: stop
    md.shutdown(ShutdownMode::Immediate).await;
    ud.shutdown(ShutdownMode::Immediate).await;
    println!("stopped");
    // ANCHOR_END: stop
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let live = std::env::var("BROKAPI_LIVE").as_deref() == Ok("1");
    run(live).await
}
