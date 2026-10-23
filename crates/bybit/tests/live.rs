//! Level-3: the Bybit connectors against the live API. Built only with `--features integration-live`, never run in CI and
//! never by an agent without the word of the owner of the account. Run by hand, one test at a time:
//!
//! ```text
//! # .env (git-ignored)
//! BYBIT_ENV=demo                    # mainnet (default), demo or testnet
//! BYBIT_DEMO_API_KEY=...            # the pair of the demo account (BYBIT_API_KEY / BYBIT_API_SECRET for the others)
//! BYBIT_DEMO_API_SECRET=...
//! BYBIT_LIVE_SYMBOL=BTCUSDT         # optional, default BTCUSDT
//! BYBIT_LIVE_TRADE=1                # opt-in: places ONE limit order far from the market, on the demo contour only
//!
//! cargo test -p brokapi-bybit --features integration-live --test live -- --nocapture --test-threads=1 live_md_reference_and_streams
//! ```
//!
//! Order: `live_md_*` need no key. `live_ud_*` read the account. `live_order_roundtrip` places an order and refuses to run
//! on any contour but the demo one.

#![cfg(feature = "integration-live")]

use std::sync::Arc;
use std::time::Duration;

use brokapi_bybit::limits::default_limits;
use brokapi_bybit::util::instrument_id;
use brokapi_bybit::{MarketDataConfig, MarketDataConnector, UserDataConfig, UserDataConnector};
use brokapi_bybit_protocol::config::BybitConfig;
use brokapi_core::{
    ClientOrderId, Decimal, Envelope, Event, InstrumentId, InstrumentSource, MarketDataSource,
    OrderBook, OrderBookSource, OrderExecutor, OrderType, PlaceOrderRequest, PortfolioSource,
    Price, Qty, RoundMode, ShutdownMode, Side, SubscribeOpts, Timeframe,
};
use brokapi_runtime::BrokerLimiter;
use tokio::sync::mpsc;
use tokio::time::timeout;

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

/// The config with a key pair, from `.env`.
fn config() -> BybitConfig {
    BybitConfig::from_env().expect("the key pair in .env")
}

/// The config for the public data: the contour of `.env`, and no key is needed.
fn public_config() -> BybitConfig {
    dotenvy::from_path(".env").ok();
    let env = env("BYBIT_ENV").and_then(|n| brokapi_bybit_protocol::config::BybitEnv::parse(&n));
    BybitConfig::new("", "")
        .with_env(env.unwrap_or(brokapi_bybit_protocol::config::BybitEnv::Mainnet))
}

fn limiter() -> Arc<BrokerLimiter> {
    BrokerLimiter::new(&default_limits())
}

fn instrument() -> InstrumentId {
    instrument_id(
        &env("BYBIT_LIVE_SYMBOL").unwrap_or_else(|| "BTCUSDT".into()),
        "linear",
    )
}

async fn next_event(
    rx: &mut mpsc::Receiver<Envelope>,
    what: &str,
    pred: impl Fn(&Event) -> bool,
) -> Event {
    timeout(Duration::from_secs(20), async {
        while let Some(env) = rx.recv().await {
            if pred(&env.payload) {
                return env.payload;
            }
        }
        panic!("the channel closed while waiting for {what}");
    })
    .await
    .unwrap_or_else(|_| panic!("no {what} in 20 seconds"))
}

#[tokio::test]
async fn live_md_reference_and_streams() {
    let (tx, mut rx) = mpsc::channel(256);
    let md = MarketDataConnector::start(
        MarketDataConfig::new(public_config(), "bybit-md"),
        limiter(),
        tx,
    )
    .await
    .expect("md start");
    let id = instrument();
    let params = md.trading_params(&id).await.expect("trading params");
    println!(
        "{}: step {}, lot {}, min {}",
        id.key.display(),
        params.price_step,
        params.lot_size,
        params.min_qty
    );
    let OrderBook::Snapshot { bids, asks, .. } = md.order_book(&id, 5).await.expect("order book")
    else {
        panic!("a snapshot");
    };
    assert!(bids[0].price < asks[0].price);
    assert!(!md.last_trades(&id, 3).await.expect("trades").is_empty());
    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    let range = brokapi_core::TimeRange::new(
        brokapi_core::UnixNanos::from_nanos(now - 3_600_000_000_000),
        brokapi_core::UnixNanos::from_nanos(now),
    );
    assert!(
        !md.candles(&id, Timeframe::M1, range)
            .await
            .expect("candles")
            .is_empty()
    );

    let book = md
        .subscribe_order_book(
            &id,
            SubscribeOpts {
                depth: Some(50),
                ..SubscribeOpts::lossless(256)
            },
        )
        .await
        .expect("book stream");
    next_event(&mut rx, "the first book", |e| {
        matches!(e, Event::OrderBook(_))
    })
    .await;
    // Several changes in a row: the local book follows the deltas, and no gap is reported on a quiet link.
    for _ in 0..20 {
        let ev = next_event(&mut rx, "a book", |e| {
            matches!(e, Event::OrderBook(_) | Event::Resync { .. })
        })
        .await;
        assert!(
            matches!(ev, Event::OrderBook(_)),
            "a gap in the deltas of the book: {ev:?}"
        );
    }
    md.unsubscribe(book).await.expect("unsubscribe");
    md.subscribe_quotes(&id, SubscribeOpts::conflate())
        .await
        .expect("quotes");
    next_event(&mut rx, "a quote", |e| matches!(e, Event::Quote(_))).await;
    md.subscribe_market_trades(&id, SubscribeOpts::lossless(64))
        .await
        .expect("trades stream");
    next_event(&mut rx, "a trade", |e| matches!(e, Event::MarketTrade(_))).await;
    md.subscribe_candles(&id, Timeframe::M1, SubscribeOpts::conflate())
        .await
        .expect("candles stream");
    next_event(&mut rx, "a candle", |e| matches!(e, Event::Candle(_))).await;
    md.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn live_ud_reads_the_account() {
    let (tx, mut rx) = mpsc::channel(256);
    let ud = UserDataConnector::start(UserDataConfig::new(config(), "bybit-ud"), limiter(), tx)
        .await
        .expect("ud start (the key pair is checked by a read of the wallet)");
    let p = ud.portfolio().await.expect("portfolio");
    println!(
        "{} balance(s), {} position(s)",
        p.balances.len(),
        p.positions.len()
    );
    let report = ud.reconcile(&[]).await.expect("reconcile");
    println!("{} order(s) of someone else", report.foreign.len());
    ud.subscribe_order_state()
        .await
        .expect("order stream: the login of the private stream");
    ud.subscribe_portfolio(SubscribeOpts::lossless(64))
        .await
        .expect("portfolio stream");
    drop(timeout(Duration::from_millis(1), rx.recv()).await);
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn live_order_roundtrip() {
    if env("BYBIT_LIVE_TRADE").as_deref() != Some("1") {
        eprintln!("BYBIT_LIVE_TRADE=1 is not set: no order is placed");
        return;
    }
    let cfg = config();
    assert!(
        cfg.is_demo(),
        "an order is placed only on the demo contour (BYBIT_ENV=demo)"
    );
    let (md_tx, _md_rx) = mpsc::channel(256);
    let md = MarketDataConnector::start(
        MarketDataConfig::new(cfg.clone(), "bybit-md"),
        limiter(),
        md_tx,
    )
    .await
    .expect("md start");
    let (tx, mut rx) = mpsc::channel(256);
    let ud = UserDataConnector::start(UserDataConfig::new(cfg, "bybit-ud"), limiter(), tx)
        .await
        .expect("ud start");
    let id = instrument();
    let params = md.trading_params(&id).await.expect("trading params");
    let OrderBook::Snapshot { bids, .. } = md.order_book(&id, 1).await.expect("book") else {
        panic!("a snapshot");
    };
    // A buy 30% under the best bid is far from a fill.
    let price = Price::new(bids[0].price.decimal() * brokapi_core::dec!(0.7))
        .round_to_step(params.price_step, RoundMode::Down)
        .expect("price");
    let qty = Qty::new(brokapi_core::dec!(12) / price.decimal())
        .round_to_step(params.lot_size, RoundMode::Up)
        .expect("qty")
        .max(params.min_qty);
    ud.subscribe_order_state().await.expect("order stream");
    ud.reconcile(&[]).await.expect("reconcile");
    let cid = ClientOrderId::new_v4();
    let ack = ud
        .place(PlaceOrderRequest {
            cid,
            instrument: id,
            side: Side::Buy,
            order_type: OrderType::Limit,
            qty,
            price: Some(price),
        })
        .await
        .expect("place");
    println!("placed {} at {price}: {}", ack.broker_id.as_str(), qty);
    next_event(
        &mut rx,
        "the order in the stream",
        |e| matches!(e, Event::Order(o) if o.cid == cid),
    )
    .await;
    ud.cancel(cid).await.expect("cancel");
    next_event(
        &mut rx,
        "the cancel in the stream",
        |e| matches!(e, Event::Order(o) if o.cid == cid && o.state.is_final()),
    )
    .await;
    ud.shutdown(ShutdownMode::Immediate).await;
    md.shutdown(ShutdownMode::Immediate).await;
}
