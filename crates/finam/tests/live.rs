//! Level-3: live Finam Trade API. Built only with `--features integration-live`, never in CI.
//!
//! Needs `.env` (git-ignored) with `FINAM_SECRET` and `FINAM_ACCOUNT_ID`.
//! Optional: `FINAM_API_URL`, `FINAM_LIVE_SYMBOL` (default `SBER@MISX`).
//! The order round trip is opt-in: `FINAM_LIVE_TRADE=1` and `FINAM_LIVE_PRICE` (a limit buy price
//! far below the market that will not execute, valid for the symbol's price step).
//!
//! Run: `cargo test -p brokapi-finam --features integration-live --test live -- --nocapture`.
//! What these runs should answer is listed on the live runs page of the book (<https://brokapi.runinfra.ru/live.html>).

use brokapi_core::{Decimal, dec};
use std::time::Duration;

use brokapi_core::{
    ClientOrderId, Envelope, Event, ExecutionSource, InstrumentId, InstrumentQuery,
    InstrumentSource, MarketDataSource, OrderBook, OrderBookSource, OrderExecutor, OrderState,
    OrderType, PlaceOrderRequest, PortfolioSource, Price, Qty, ShutdownMode, Side, SubscribeOpts,
    TimeRange, Timeframe, UnixNanos,
};
use brokapi_finam::limits::default_limits;
use brokapi_finam::{
    FinamApiConfig, MarketDataConfig, MarketDataConnector, UserDataConfig, UserDataConnector,
};
use brokapi_runtime::BrokerLimiter;
use tokio::sync::mpsc;

fn env(name: &str) -> Option<String> {
    dotenv::dotenv().ok();
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

fn api() -> FinamApiConfig {
    let mut cfg = FinamApiConfig::new(env("FINAM_SECRET").expect("FINAM_SECRET in .env"));
    if let Some(url) = env("FINAM_API_URL") {
        cfg = cfg.with_endpoint(url);
    }
    cfg
}

fn account() -> String {
    env("FINAM_ACCOUNT_ID").expect("FINAM_ACCOUNT_ID in .env")
}

fn symbol() -> InstrumentId {
    brokapi_finam::util::instrument_from_symbol(
        &env("FINAM_LIVE_SYMBOL").unwrap_or_else(|| "SBER@MISX".into()),
    )
}

fn limiter() -> std::sync::Arc<BrokerLimiter> {
    BrokerLimiter::new(&default_limits())
}

async fn next(
    rx: &mut mpsc::Receiver<Envelope>,
    what: &str,
    pred: impl Fn(&Event) -> bool,
) -> Envelope {
    tokio::time::timeout(Duration::from_secs(30), async {
        while let Some(e) = rx.recv().await {
            if pred(&e.payload) {
                return e;
            }
        }
        panic!("channel closed waiting for {what}");
    })
    .await
    .unwrap_or_else(|_| panic!("no {what} within 30s"))
}

#[tokio::test]
async fn live_token_and_accounts() {
    let tokens = api().shared_tokens().await.expect("auth");
    let info = tokens.info();
    println!(
        "token ttl: {:?}, readonly: {}, accounts: {:?}",
        info.ttl, info.readonly, info.account_ids
    );
    assert!(info.ttl.is_some(), "TokenDetails should tell the lifetime");
    assert!(
        info.account_ids.contains(&account()),
        "FINAM_ACCOUNT_ID is not available to this secret"
    );
}

#[tokio::test]
async fn live_market_data() {
    let (tx, mut rx) = mpsc::channel(1024);
    let md = MarketDataConnector::start(MarketDataConfig::new(api(), "finam-md"), limiter(), tx)
        .await
        .expect("md start");
    let id = symbol();

    let book = md.order_book(&id, 10).await.expect("order_book");
    let OrderBook::Snapshot { bids, asks, .. } = &book else {
        panic!("REST book must be a snapshot")
    };
    println!(
        "REST book: {} bids / {} asks, best {:?} / {:?}",
        bids.len(),
        asks.len(),
        bids.first(),
        asks.first()
    );

    let sub = md
        .subscribe_order_book(&id, SubscribeOpts::conflate())
        .await
        .expect("subscribe_order_book");
    let first = next(&mut rx, "first book", |e| matches!(e, Event::OrderBook(_))).await;
    assert_eq!(first.sub, Some(sub));
    // Let a few increments through: the local book must stay sane (not crossed).
    for _ in 0..20 {
        let env = next(&mut rx, "book update", |e| {
            matches!(e, Event::OrderBook(_) | Event::Resync { .. })
        })
        .await;
        match env.payload {
            Event::OrderBook(OrderBook::Snapshot { bids, asks, .. }) => {
                if let (Some(b), Some(a)) = (bids.first(), asks.first()) {
                    assert!(
                        b.price.value() < a.price.value(),
                        "crossed book: {b:?} / {a:?}"
                    );
                }
            }
            Event::Resync { reason, .. } => println!("resync: {reason:?}"),
            _ => {}
        }
    }
    println!("stream status: {:?}", md.sub_status(sub));

    md.subscribe_quotes(&id, SubscribeOpts::conflate())
        .await
        .expect("quotes");
    next(&mut rx, "quote", |e| matches!(e, Event::Quote(_))).await;
    md.subscribe_market_trades(&id, SubscribeOpts::lossless(1024))
        .await
        .expect("trades");
    println!(
        "last trades: {:?}",
        md.last_trades(&id, 5).await.map(|t| t.len())
    );

    let now = brokapi_finam::util::now_nanos().as_nanos();
    let week = 7 * 86_400 * 1_000_000_000i64;
    let bars = md
        .candles(
            &id,
            Timeframe::H1,
            TimeRange::new(
                UnixNanos::from_nanos(now - week),
                UnixNanos::from_nanos(now),
            ),
        )
        .await
        .expect("candles");
    println!("bars: {}", bars.len());

    let found = md
        .find(InstrumentQuery::ByTicker {
            ticker: id.key.symbol.clone(),
            exchange: Some(id.key.mic.clone()),
        })
        .await
        .expect("find");
    println!("instrument: {:?}", found.first());
    println!(
        "trading params: {:?}",
        md.trading_params(&id).await.expect("params")
    );
    println!(
        "trading status: {:?}",
        md.trading_status(&id).await.expect("status")
    );
    let report = md
        .shutdown(ShutdownMode::Graceful {
            deadline: Duration::from_secs(5),
        })
        .await;
    println!("shutdown: {report:?}");
}

async fn ud() -> (UserDataConnector, mpsc::Receiver<Envelope>) {
    let (tx, rx) = mpsc::channel(1024);
    let cfg = UserDataConfig::new(api(), account(), "finam-ud");
    (
        UserDataConnector::start(cfg, limiter(), tx)
            .await
            .expect("ud start"),
        rx,
    )
}

#[tokio::test]
async fn live_portfolio_and_orders_read_only() {
    let (ud, mut rx) = ud().await;
    let report = ud.reconcile(&[]).await.expect("reconcile");
    println!("foreign active orders: {}", report.foreign.len());
    let p = ud.portfolio().await.expect("portfolio");
    println!(
        "balances: {:?}",
        p.balances
            .iter()
            .map(|b| (&b.currency, b.available))
            .collect::<Vec<_>>()
    );
    println!("positions: {}", p.positions.len());
    ud.subscribe_portfolio(SubscribeOpts::lossless(64))
        .await
        .expect("subscribe_portfolio");
    next(&mut rx, "portfolio update", |e| {
        matches!(e, Event::Portfolio(_))
    })
    .await;
    ud.subscribe_order_state()
        .await
        .expect("subscribe_order_state");
    ud.subscribe_executions()
        .await
        .expect("subscribe_executions");
    let day = 86_400 * 1_000_000_000i64;
    let now = brokapi_finam::util::now_nanos().as_nanos();
    let execs = ud
        .executions(TimeRange::new(
            UnixNanos::from_nanos(now - 7 * day),
            UnixNanos::from_nanos(now),
        ))
        .await
        .expect("executions");
    println!("executions in 7 days: {}", execs.len());
    ud.shutdown(ShutdownMode::Graceful {
        deadline: Duration::from_secs(5),
    })
    .await;
}

/// Places a far limit buy, reads it back, checks what Finam echoes, cancels it.
#[tokio::test]
async fn live_order_roundtrip() {
    if env("FINAM_LIVE_TRADE").as_deref() != Some("1") {
        println!(
            "skipped: set FINAM_LIVE_TRADE=1 and FINAM_LIVE_PRICE to place a real (non-executing) order"
        );
        return;
    }
    let price: Decimal = env("FINAM_LIVE_PRICE")
        .expect("FINAM_LIVE_PRICE")
        .parse()
        .expect("number");
    let id = symbol();
    let (ud, mut rx) = ud().await;
    ud.reconcile(&[]).await.expect("reconcile");
    let sub = ud
        .subscribe_order_state()
        .await
        .expect("subscribe_order_state");

    let cid = ClientOrderId::new_v4();
    let ack = ud
        .place(PlaceOrderRequest {
            cid,
            instrument: id,
            side: Side::Buy,
            order_type: OrderType::Limit,
            qty: Qty::new(
                env("FINAM_LIVE_QTY")
                    .and_then(|q| q.parse().ok())
                    .unwrap_or(1.0),
            ),
            price: Some(Price::new(price)),
        })
        .await
        .expect("place");
    println!("placed: {ack:?}");

    // Open question: does the comment (our lossless cid) come back in the stream and in queries?
    let ev = next(&mut rx, "order event", |e| matches!(e, Event::Order(_))).await;
    assert_eq!(ev.sub, Some(sub));
    let Event::Order(s) = ev.payload else {
        unreachable!()
    };
    println!(
        "stream echo: cid {} (ours {cid}), state {:?}",
        s.cid, s.state
    );
    assert_eq!(
        s.cid, cid,
        "the cid did not survive the round trip through the stream"
    );

    let snap = ud.order_state(cid).await.expect("order_state");
    println!("order_state: {:?}", snap.state);
    assert_eq!(snap.cid, cid);
    assert!(!matches!(snap.state, OrderState::Rejected { .. }));

    // The same cid again must not be sent by us; what would the broker do? Not tested here: the
    // `PlaceIdempotency` experiment is a separate decision.
    ud.cancel(cid).await.expect("cancel");
    let after = ud.order_state(cid).await.expect("order_state after cancel");
    println!("after cancel: {:?}", after.state);
    ud.shutdown(ShutdownMode::Graceful {
        deadline: Duration::from_secs(5),
    })
    .await;
}

/// Stop orders: SELL stops far BELOW the market (they never trigger), put, listed, cancelled.
/// Opt-in like the order round trip: `FINAM_LIVE_TRADE=1` and `FINAM_LIVE_STOP_PRICE` (far below the
/// market). Open questions this answers: does Finam accept a SELL stop without a position, which
/// status a waiting stop has, does `GetOrders` list stops, does `CancelOrder` cancel them, does the
/// comment come back, is `time_in_force` needed on a stop (we leave it unset).
#[tokio::test]
async fn live_stop_order_roundtrip() {
    if env("FINAM_LIVE_TRADE").as_deref() != Some("1") {
        println!(
            "skipped: set FINAM_LIVE_TRADE=1 and FINAM_LIVE_STOP_PRICE (far below the market)"
        );
        return;
    }
    let stop: Decimal = env("FINAM_LIVE_STOP_PRICE")
        .expect("FINAM_LIVE_STOP_PRICE")
        .parse()
        .expect("number");
    let (ud, _rx) = ud().await;
    ud.reconcile(&[]).await.expect("reconcile");
    assert!(ud.caps().supports_stop_orders);
    brokapi_conformance::stop_order_far_from_market(&ud, &symbol(), stop).await;
    ud.shutdown(ShutdownMode::Graceful {
        deadline: Duration::from_secs(5),
    })
    .await;
}
