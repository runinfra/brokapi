//! Level-3: the ALOR connectors against the live API. Built only with `--features integration-live`, never run in
//! CI and never by an agent: they need tokens. Run by hand, one test at a time:
//!
//! ```text
//! # .env (git-ignored)
//! ALOR_REFRESH_TOKEN=...           # from the developer portal
//! ALOR_PORTFOLIO=D12345            # your portfolio
//! ALOR_EXCHANGE=MOEX               # optional, MOEX or SPBX
//! ALOR_ENV=test                    # optional: the test contour (a test account is needed)
//! ALOR_LIVE_TICKER=SBER            # optional, default SBER
//! ALOR_LIVE_BOARD=TQBR             # optional, default TQBR
//! ALOR_LIVE_TRADE=1                # opt-in: places ONE real limit order far from the market
//! ALOR_LIVE_PRICE=<price>          # far BELOW the market, a multiple of the price step
//!
//! cargo test -p brokapi-alor --features integration-live --test live -- --nocapture --test-threads=1 live_md_reference_and_streams
//! ```
//!
//! Order: read-only tests first (`live_md_*`, `live_ud_*`), then `live_order_roundtrip`. The test contour is the
//! safe place for the round trip; the production contour is a real account.

#![cfg(feature = "integration-live")]

use std::sync::Arc;
use std::time::Duration;

use brokapi_alor::limits::default_limits;
use brokapi_alor::util::instrument_id;
use brokapi_alor::{MarketDataConfig, MarketDataConnector, UserDataConfig, UserDataConnector};
use brokapi_alor_protocol::config::AlorConfig;
use brokapi_core::{
    ClientOrderId, Decimal, Envelope, Event, ExecutionSource, InstrumentId, InstrumentQuery,
    InstrumentSource, MarketDataSource, OrderBookSource, OrderExecutor, OrderType,
    PlaceOrderRequest, PortfolioSource, Price, Qty, ShutdownMode, Side, SubscribeOpts, TimeRange,
    Timeframe, UnixNanos,
};
use brokapi_runtime::BrokerLimiter;
use tokio::sync::mpsc;
use tokio::time::timeout;

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

fn config() -> AlorConfig {
    AlorConfig::from_env().expect("ALOR_REFRESH_TOKEN in .env")
}

fn limiter() -> Arc<BrokerLimiter> {
    BrokerLimiter::new(&default_limits())
}

fn instrument() -> InstrumentId {
    instrument_id(
        &env("ALOR_LIVE_TICKER").unwrap_or_else(|| "SBER".into()),
        &env("ALOR_LIVE_BOARD").unwrap_or_else(|| "TQBR".into()),
    )
}

fn now_nanos() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

/// Waits for an event the predicate accepts; panics with `what` after 30 s (a stream needs trading hours).
async fn next(
    rx: &mut mpsc::Receiver<Envelope>,
    what: &str,
    pred: impl Fn(&Event) -> bool,
) -> Envelope {
    timeout(Duration::from_secs(30), async {
        while let Some(env) = rx.recv().await {
            if pred(&env.payload) {
                return env;
            }
        }
        panic!("event channel closed while waiting for {what}");
    })
    .await
    .unwrap_or_else(|_| panic!("no {what} within 30 s (is the exchange open?)"))
}

#[tokio::test]
async fn live_md_reference_and_streams() {
    let (tx, mut rx) = mpsc::channel(256);
    let md = MarketDataConnector::start(MarketDataConfig::new(config(), "alor-md"), limiter(), tx)
        .await
        .expect("md start");
    let id = instrument();
    let found = md
        .find(InstrumentQuery::ByTicker {
            ticker: id.key.symbol.clone(),
            exchange: None,
        })
        .await
        .expect("find");
    println!("found: {found:?}");
    let params = md.trading_params(&id).await.expect("trading_params");
    println!(
        "lot {} step {} value of a step {}",
        params.lot_size, params.price_step, params.price_step_value
    );
    println!(
        "status: {:?}",
        md.trading_status(&id).await.expect("trading_status")
    );
    println!(
        "book: {:?}",
        md.order_book(&id, 5).await.expect("order_book")
    );
    println!(
        "tape: {:?}",
        md.last_trades(&id, 5).await.expect("last_trades")
    );
    let to = now_nanos();
    let from = to - 3 * 3_600 * 1_000_000_000;
    let bars = md
        .candles(
            &id,
            Timeframe::M10,
            TimeRange::new(UnixNanos::from_nanos(from), UnixNanos::from_nanos(to)),
        )
        .await
        .expect("candles");
    println!("{} candles of 10 minutes", bars.len());

    md.subscribe_order_book(&id, SubscribeOpts::conflate())
        .await
        .expect("subscribe_order_book");
    md.subscribe_quotes(&id, SubscribeOpts::conflate())
        .await
        .expect("subscribe_quotes");
    let book = next(&mut rx, "an order book", |e| {
        matches!(e, Event::OrderBook(_))
    })
    .await;
    println!("{book:?}");
    next(&mut rx, "a quote", |e| matches!(e, Event::Quote(_))).await;
    md.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn live_ud_portfolio_and_orders() {
    let (tx, mut rx) = mpsc::channel(256);
    let ud = UserDataConnector::start(UserDataConfig::new(config(), "alor-ud"), limiter(), tx)
        .await
        .expect("ud start");
    let portfolio = ud.portfolio().await.expect("portfolio");
    println!(
        "{} balances, {} positions",
        portfolio.balances.len(),
        portfolio.positions.len()
    );
    let report = ud.reconcile(&[]).await.expect("reconcile");
    println!("foreign orders: {}", report.foreign.len());
    use brokapi_core::StopOrderExecutor;
    println!(
        "active stop orders: {:?}",
        ud.active_stop_orders().await.expect("active_stop_orders")
    );
    ud.subscribe_portfolio(SubscribeOpts::lossless(64))
        .await
        .expect("subscribe_portfolio");
    next(&mut rx, "a position or a balance", |e| {
        matches!(
            e,
            Event::Positions(_) | Event::Balances(_) | Event::Portfolio(_)
        )
    })
    .await;
    let range = TimeRange::new(
        UnixNanos::from_nanos(now_nanos() - 7 * 24 * 3_600 * 1_000_000_000),
        UnixNanos::from_nanos(now_nanos()),
    );
    println!(
        "{} fills in the last week",
        ud.executions(range).await.expect("executions").len()
    );
    ud.shutdown(ShutdownMode::Immediate).await;
}

/// One real limit order far below the market, then replace and cancel. Needs `ALOR_LIVE_TRADE=1`.
#[tokio::test]
async fn live_order_roundtrip() {
    if env("ALOR_LIVE_TRADE").is_none() {
        println!("skipped: ALOR_LIVE_TRADE is not set");
        return;
    }
    let price: Decimal = env("ALOR_LIVE_PRICE")
        .expect("ALOR_LIVE_PRICE")
        .parse()
        .expect("a number");
    let (tx, mut rx) = mpsc::channel(256);
    let ud = UserDataConnector::start(UserDataConfig::new(config(), "alor-ud"), limiter(), tx)
        .await
        .expect("ud start");
    ud.reconcile(&[]).await.expect("reconcile");
    ud.subscribe_order_state()
        .await
        .expect("subscribe_order_state");
    let id = instrument();
    let cid = ClientOrderId::new_v4();
    let md_cfg = MarketDataConfig::new(config(), "alor-md");
    let (mtx, _mrx) = mpsc::channel(8);
    let md = MarketDataConnector::start(md_cfg, limiter(), mtx)
        .await
        .expect("md start");
    let lot = md
        .trading_params(&id)
        .await
        .expect("trading_params")
        .lot_size;
    let ack = ud
        .place(PlaceOrderRequest {
            cid,
            instrument: id.clone(),
            side: Side::Buy,
            order_type: OrderType::Limit,
            qty: lot,
            price: Some(Price::new(price)),
        })
        .await
        .expect("place");
    println!("placed {ack:?}");
    next(
        &mut rx,
        "the order event",
        |e| matches!(e, Event::Order(o) if o.cid == cid),
    )
    .await;
    println!(
        "state: {:?}",
        ud.order_state(cid).await.expect("order_state")
    );
    let second = ClientOrderId::new_v4();
    let replaced = ud
        .replace(brokapi_core::ReplaceOrderRequest {
            cid: second,
            original_cid: cid,
            qty: Some(Qty::new(lot.decimal())),
            price: Some(Price::new(price)),
        })
        .await
        .expect("replace");
    println!("replaced {replaced:?}");
    ud.cancel(second).await.expect("cancel");
    println!(
        "active after the cancel: {:?}",
        ud.active_orders().await.expect("active_orders")
    );
    md.shutdown(ShutdownMode::Immediate).await;
    ud.shutdown(ShutdownMode::Immediate).await;
}
