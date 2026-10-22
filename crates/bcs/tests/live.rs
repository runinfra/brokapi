//! Level-3: the BCS connectors against the live Trade API. Built only with `--features integration-live`,
//! never run in CI and never by an agent: they need tokens. Run by hand, one test at a time:
//!
//! ```text
//! # .env (git-ignored)
//! BCS_REFRESH_TOKEN=...            # a read token first, a trade token for the order round trip
//! BCS_LIVE_TICKER=SBER             # optional, default SBER
//! BCS_LIVE_CLASS=TQBR              # optional, default TQBR
//! BCS_LIVE_TRADE=1                 # opt-in: places ONE real limit order far from the market
//! BCS_LIVE_PRICE=<price>           # far BELOW the market, a multiple of the price step
//! BCS_LIVE_QTY=1                   # optional, shares (the unit - shares or lots - is an open question)
//! BCS_LIVE_STOP_PRICE=<price>      # for live_stop_order_roundtrip: far BELOW the market (the stops are SELL)
//!
//! cargo test -p brokapi-bcs --features integration-live --test live -- --nocapture --test-threads=1 live_md_reference_and_streams
//! ```
//!
//! Order: read-only tests first (`live_md_*`, `live_ud_*`), then `live_order_roundtrip` and
//! `live_stop_order_roundtrip` with a trade token.
//! There is no sandbox, the round trip uses the real account: one limit buy far from the market,
//! then replace and cancel.

#![cfg(feature = "integration-live")]

use brokapi_core::{Decimal, dec};
use std::sync::Arc;
use std::time::Duration;

use brokapi_bcs::limits::default_limits;
use brokapi_bcs::{MarketDataConfig, MarketDataConnector, UserDataConfig, UserDataConnector};
use brokapi_bcs_protocol::config::BcsConfig;
use brokapi_core::{
    ClientOrderId, Envelope, Error, Event, ExecutionSource, InstrumentId, InstrumentKey,
    InstrumentQuery, InstrumentSource, MarketDataSource, NativeId, OrderBook, OrderBookSource,
    OrderExecutor, OrderState, OrderType, PlaceOrderRequest, PlaceStopOrderRequest,
    PortfolioSource, Price, Qty, ReplaceOrderRequest, ShutdownMode, Side, StopExpiry,
    StopOrderExecutor, StopOrderKind, StopOrderRef, SubscribeOpts, TimeRange, Timeframe, UnixNanos,
};
use brokapi_runtime::BrokerLimiter;
use tokio::sync::mpsc;
use tokio::time::timeout;

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

/// Loads `.env`, reads the refresh token. `write` = the trade token round trip.
fn config(write: bool) -> BcsConfig {
    let cfg = BcsConfig::from_env().expect("BCS_REFRESH_TOKEN in .env");
    cfg.with_write(write)
}

fn limiter() -> Arc<BrokerLimiter> {
    BrokerLimiter::new(&default_limits())
}

fn instrument() -> InstrumentId {
    let ticker = env("BCS_LIVE_TICKER").unwrap_or_else(|| "SBER".into());
    let class = env("BCS_LIVE_CLASS").unwrap_or_else(|| "TQBR".into());
    InstrumentId::new(
        InstrumentKey::new(ticker.clone(), class.clone()),
        NativeId::TickerClass {
            ticker,
            class_code: class,
        },
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

async fn md() -> (MarketDataConnector, mpsc::Receiver<Envelope>) {
    let (tx, rx) = mpsc::channel(1024);
    let md = MarketDataConnector::start(
        MarketDataConfig::new(config(false), "bcs-md"),
        limiter(),
        tx,
    )
    .await
    .expect("md start");
    (md, rx)
}

async fn ud(
    write: bool,
    limiter: Arc<BrokerLimiter>,
) -> (UserDataConnector, mpsc::Receiver<Envelope>) {
    let (tx, rx) = mpsc::channel(1024);
    let ud = UserDataConnector::start(UserDataConfig::new(config(write), "bcs-ud"), limiter, tx)
        .await
        .expect("ud start");
    (ud, rx)
}

/// Read-only, market data connector: directory, parameters, status, schedule, candles, last trades, the
/// REST book and the WS book agree, quotes stream, unsubscribe, graceful shutdown.
#[tokio::test]
async fn live_md_reference_and_streams() {
    let id = instrument();
    let (md, mut rx) = md().await;

    let found = md
        .find(InstrumentQuery::ByTicker {
            ticker: id.key.symbol.clone(),
            exchange: None,
        })
        .await
        .expect("find by ticker");
    println!(
        "find: {} match(es), first {:?}",
        found.len(),
        found.first().map(|i| (&i.id.key, &i.kind, &i.currency))
    );
    assert!(!found.is_empty(), "the ticker is not in the directory");
    let got = md.get(&id).await.expect("get");
    println!("get: {} isin {:?}", got.name, got.isin);
    println!(
        "trading params: {:?}",
        md.trading_params(&id).await.expect("trading_params")
    );
    println!(
        "trading status: {:?}",
        md.trading_status(&id).await.expect("trading_status")
    );

    let day = 86_400 * 1_000_000_000i64;
    let now = now_nanos();
    let schedule = md
        // BCS schedules are per instrument: "TICKER@CLASSCODE".
        .schedule(
            &format!("{}@{}", id.key.symbol, id.key.mic),
            TimeRange::new(UnixNanos::from_nanos(now), UnixNanos::from_nanos(now + day)),
        )
        .await;
    println!("schedule: {schedule:?}");

    let candles = md
        .candles(
            &id,
            Timeframe::H1,
            TimeRange::new(
                UnixNanos::from_nanos(now - 7 * day),
                UnixNanos::from_nanos(now),
            ),
        )
        .await
        .expect("candles");
    println!("candles H1 over 7 days: {}", candles.len());
    println!(
        "last trades: {}",
        md.last_trades(&id, 10).await.expect("last_trades").len()
    );

    // The REST book (open question: depth, sorting) and then the stream, in trading hours.
    let book = md.order_book(&id, 10).await.expect("order_book");
    let OrderBook::Snapshot { bids, asks, .. } = &book else {
        panic!("REST book must be a snapshot")
    };
    println!(
        "REST book: {} bids, {} asks, best {:?} / {:?}",
        bids.len(),
        asks.len(),
        bids.first(),
        asks.first()
    );
    let sub = md
        .subscribe_order_book(&id, SubscribeOpts::conflate())
        .await
        .expect("subscribe_order_book");
    let ev = next(&mut rx, "order book update", |e| {
        matches!(e, Event::OrderBook(_))
    })
    .await;
    assert_eq!(ev.sub, Some(sub));
    println!("WS book arrived for sub {sub:?}");
    let q = md
        .subscribe_quotes(&id, SubscribeOpts::conflate())
        .await
        .expect("subscribe_quotes");
    let ev = next(&mut rx, "quote", |e| matches!(e, Event::Quote(_))).await;
    assert_eq!(ev.sub, Some(q));
    md.unsubscribe(sub).await.expect("unsubscribe");
    md.unsubscribe(q).await.expect("unsubscribe quotes");

    // The raw back door with the same token: a method no shared trait has.
    let quotes = md
        .raw()
        .call("GetQuotes", |http| {
            let (t, c) = (id.key.symbol.clone(), id.key.mic.clone());
            async move {
                http.get_quotes(&brokapi_bcs_protocol::http::models::QuotesRequest {
                    instruments: vec![brokapi_bcs_protocol::http::models::QuoteInstrument::new(
                        t, c,
                    )],
                })
                .await
            }
        })
        .await
        .expect("raw GetQuotes");
    println!("raw quotes: {:?}", quotes.records.first());

    let report = md
        .shutdown(ShutdownMode::Graceful {
            deadline: Duration::from_secs(5),
        })
        .await;
    println!("shutdown: {report:?}");
}

/// Read-only, user data connector with a READ token: reconcile on an empty ledger, the portfolio snapshot
/// and stream, the order and execution streams, history, and the answer for an unknown cid.
#[tokio::test]
async fn live_ud_portfolio_and_reconcile_read_only() {
    let (ud, mut rx) = ud(false, limiter()).await;

    let report = ud.reconcile(&[]).await.expect("reconcile");
    println!(
        "reconcile: {} active order(s) at the broker (all foreign)",
        report.foreign.len()
    );
    assert!(report.confirmed.is_empty() && report.closed_offline.is_empty());

    let p = ud.portfolio().await.expect("portfolio");
    println!(
        "portfolio: {} balance(s), {} position(s)",
        p.balances.len(),
        p.positions.len()
    );
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
    println!(
        "active orders: {}",
        ud.active_orders().await.expect("active_orders").len()
    );

    let day = 86_400 * 1_000_000_000i64;
    let now = now_nanos();
    let execs = ud
        .executions(TimeRange::new(
            UnixNanos::from_nanos(now - 7 * day),
            UnixNanos::from_nanos(now),
        ))
        .await
        .expect("executions");
    println!("executions in 7 days: {}", execs.len());

    // Open question 1: what does BCS answer for a cid it has never seen? We expect NOT_FOUND, which
    // `reconcile` turns into `closed_offline`.
    let ghost = ClientOrderId::new_v4();
    let r = ud
        .reconcile(&[ghost])
        .await
        .expect("reconcile with an unknown cid");
    println!(
        "reconcile(unknown cid): closed_offline {:?}",
        r.closed_offline
    );
    assert_eq!(
        r.closed_offline,
        vec![ghost],
        "an unknown cid must be reported closed offline"
    );
    match ud.order_state(ghost).await {
        Err(e) => println!("order_state(unknown cid) -> {e:?}"),
        Ok(s) => panic!("an unknown cid has a state?! {s:?}"),
    }

    let report = ud
        .shutdown(ShutdownMode::Graceful {
            deadline: Duration::from_secs(5),
        })
        .await;
    println!("shutdown: {report:?}");
}

/// Opt-in: ONE real limit buy far below the market, read by cid, replaced, cancelled. A TRADE token and
/// `BCS_LIVE_TRADE=1`, `BCS_LIVE_PRICE` are required, nothing else in this file touches the account.
#[tokio::test]
async fn live_order_roundtrip() {
    if env("BCS_LIVE_TRADE").as_deref() != Some("1") {
        println!(
            "skipped: set BCS_LIVE_TRADE=1 and BCS_LIVE_PRICE (far below the market) to place a real, non-executing order"
        );
        return;
    }
    let price: Decimal = env("BCS_LIVE_PRICE")
        .expect("BCS_LIVE_PRICE")
        .parse()
        .expect("a number");
    let qty: Decimal = env("BCS_LIVE_QTY")
        .and_then(|q| q.parse().ok())
        .unwrap_or(Decimal::ONE);
    let id = instrument();
    let (ud, mut rx) = ud(true, limiter()).await;
    ud.reconcile(&[]).await.expect("reconcile");
    let sub = ud
        .subscribe_order_state()
        .await
        .expect("subscribe_order_state");

    // 1. place
    let cid = ClientOrderId::new_v4();
    let ack = ud
        .place(PlaceOrderRequest {
            cid,
            instrument: id.clone(),
            side: Side::Buy,
            order_type: OrderType::Limit,
            qty: Qty::new(qty),
            price: Some(Price::new(price)),
        })
        .await
        .expect("place");
    println!("placed: {ack:?}");
    let ev = next(&mut rx, "order event", |e| matches!(e, Event::Order(_))).await;
    assert_eq!(ev.sub, Some(sub));
    let Event::Order(s) = ev.payload else {
        unreachable!()
    };
    println!(
        "stream echo: cid {} (ours {cid}), state {:?}",
        s.cid, s.state
    );

    // 2. state by cid and in the active list
    let snap = ud.order_state(cid).await.expect("order_state");
    println!("order_state: {:?}", snap.state);
    assert_eq!(snap.cid, cid);
    assert!(
        !matches!(snap.state, OrderState::Rejected { .. }),
        "{:?}",
        snap.state
    );
    let active = ud.active_orders().await.expect("active_orders");
    println!("active orders: {}", active.len());
    assert!(
        active.iter().any(|o| o.cid == cid),
        "the order is not in the active list"
    );

    // 3. replace (open question 7: which field carries the id of the new order)
    let new_cid = ClientOrderId::new_v4();
    let new_price = price + dec!(0.01);
    let rep = ud
        .replace(ReplaceOrderRequest {
            cid: new_cid,
            original_cid: cid,
            qty: Some(Qty::new(qty)),
            price: Some(Price::new(new_price)),
        })
        .await;
    println!("replace: {rep:?}");
    let live_cid = if rep.is_ok() { new_cid } else { cid };

    // 4. cancel whichever order is alive and check it is gone from the active list
    ud.cancel(live_cid).await.expect("cancel");
    let after = ud
        .order_state(live_cid)
        .await
        .expect("order_state after cancel");
    println!("after cancel: {:?}", after.state);
    assert!(
        !ud.active_orders()
            .await
            .expect("active_orders")
            .iter()
            .any(|o| o.cid == live_cid)
    );

    // 5. a rejected order must not block the instrument (checklist question 6): a price off the tick
    let bad = ud
        .place(PlaceOrderRequest {
            cid: ClientOrderId::new_v4(),
            instrument: id.clone(),
            side: Side::Buy,
            order_type: OrderType::Limit,
            qty: Qty::new(qty),
            price: Some(Price::new(dec!(0.0001))),
        })
        .await;
    println!("order with an invalid price: {bad:?}");
    if let Err(Error::Rejected { .. }) = bad {
        let again = ud
            .place(PlaceOrderRequest {
                cid: ClientOrderId::new_v4(),
                instrument: id,
                side: Side::Buy,
                order_type: OrderType::Limit,
                qty: Qty::new(qty),
                price: Some(Price::new(price)),
            })
            .await
            .expect("the next order after a refusal must pass");
        println!("next order after the refusal: {again:?}");
        // Clean up the last order too.
        let last = ud
            .active_orders()
            .await
            .expect("active")
            .into_iter()
            .find(|o| o.broker_id.as_ref() == Some(&again.broker_id));
        if let Some(o) = last {
            ud.cancel(o.cid).await.expect("cancel the last order");
        }
    }
    ud.shutdown(ShutdownMode::Graceful {
        deadline: Duration::from_secs(5),
    })
    .await;
}

/// Opt-in: conditional orders far from the market, SELL stops that must not trigger: a stop-loss, a
/// stop-limit and a take-profit (the take-profit price is far ABOVE the market: `BCS_LIVE_TAKE_PRICE`).
/// Needs the position to sell or a margin account: whether BCS accepts a SELL without a position is
/// itself a question (checklist 12). Every order is cancelled at the end, by cid and by broker id.
/// The lines `ANSWER:` are what the checklist wants to know.
#[tokio::test]
async fn live_stop_order_roundtrip() {
    if env("BCS_LIVE_TRADE").as_deref() != Some("1") {
        println!(
            "skipped: set BCS_LIVE_TRADE=1, BCS_LIVE_STOP_PRICE (far below) and BCS_LIVE_TAKE_PRICE (far above the market)"
        );
        return;
    }
    let stop: Decimal = env("BCS_LIVE_STOP_PRICE")
        .expect("BCS_LIVE_STOP_PRICE")
        .parse()
        .expect("a number");
    let take: Decimal = env("BCS_LIVE_TAKE_PRICE")
        .expect("BCS_LIVE_TAKE_PRICE")
        .parse()
        .expect("a number");
    let qty: Decimal = env("BCS_LIVE_QTY")
        .and_then(|q| q.parse().ok())
        .unwrap_or(Decimal::ONE);
    let id = instrument();
    let (ud, _rx) = ud(true, limiter()).await;
    ud.reconcile(&[]).await.expect("reconcile");
    let req = |kind, price, limit: Option<Decimal>| PlaceStopOrderRequest {
        cid: ClientOrderId::new_v4(),
        instrument: id.clone(),
        side: Side::Sell,
        kind,
        qty: Qty::new(qty),
        stop_price: Price::new(price),
        limit_price: limit.map(Price::new),
        expiry: StopExpiry::GoodTillCancel,
    };
    let sl = req(StopOrderKind::StopLoss, stop, None);
    let sll = req(StopOrderKind::StopLimit, stop, Some(stop * dec!(0.99)));
    let tp = req(StopOrderKind::TakeProfit, take, None);
    let mut placed = Vec::new();
    for r in [sl, sll, tp] {
        let (cid, kind) = (r.cid, r.kind);
        let res = ud.place_stop(r).await;
        println!("place_stop {kind:?}: {res:?}");
        if let Ok(ack) = res {
            placed.push((cid, kind, ack));
        }
    }
    let listed = ud.active_stop_orders().await.expect("active_stop_orders");
    for s in &listed {
        println!(
            "ANSWER: listed {:?} cid {:?} broker_id {} stop {} limit {:?} qty {} expiry {:?} state {:?}",
            s.kind,
            s.cid,
            s.broker_id.as_str(),
            s.stop_price.decimal(),
            s.limit_price.map(|p| p.decimal()),
            s.qty.decimal(),
            s.expiry,
            s.state
        );
    }
    for (cid, kind, ack) in &placed {
        let found = listed.iter().any(|s| s.broker_id == ack.broker_id);
        println!(
            "ANSWER: {kind:?}: the ack's broker id is in the list: {found} (ack {}, cid {cid})",
            ack.broker_id.as_str()
        );
    }
    // Cancel: the first by cid (does the plain cancel path take a conditional order?), the rest by broker id.
    for (i, (cid, kind, ack)) in placed.iter().enumerate() {
        let r = if i == 0 {
            ud.cancel_stop(StopOrderRef::Cid(*cid)).await
        } else {
            ud.cancel_stop(StopOrderRef::Broker(ack.broker_id.clone()))
                .await
        };
        println!(
            "ANSWER: cancel_stop {kind:?} ({}): {r:?}",
            if i == 0 { "by cid" } else { "by broker id" }
        );
    }
    let left = ud
        .active_stop_orders()
        .await
        .expect("active_stop_orders after cancel");
    println!("ANSWER: stops left after cancel: {}", left.len());
    ud.shutdown(ShutdownMode::Graceful {
        deadline: Duration::from_secs(5),
    })
    .await;
}

/// Where the answers of the live broker go: the reference files the imitation is checked against
/// (`tests/live_reference.rs`).
const REFERENCE_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/live");

/// Records the answers of the broker as reference files in `tests/fixtures/live`, cleaned: no tokens,
/// no account or order numbers (`brokapi_mocks::live::scrub`). Reads only, unless `BCS_LIVE_TRADE=1`
/// and `BCS_LIVE_PRICE` are set: then also ONE limit buy far below the market, its state, a repeat of the
/// same `clientOrderId`, and the cancel. A read token is enough for the reads; the order needs a trade token.
///
/// After the run: look at the files (`git diff`), run `cargo test -p brokapi-bcs --test live_reference` and
/// commit them. A difference reported there is a place where the imitation is wrong.
#[tokio::test]
async fn live_record_reference() {
    use brokapi_bcs_protocol::http::client::BcsHttpClient;
    use brokapi_mocks::live::{Recorder, bcs as probes};

    // `from_env` reads `.env` first, so the variables below are there.
    let base = BcsConfig::from_env().expect("BCS_REFRESH_TOKEN in .env");
    let trade = env("BCS_LIVE_TRADE").as_deref() == Some("1");
    let http = BcsHttpClient::new(base.with_write(trade)).expect("client");
    let ticker = env("BCS_LIVE_TICKER").unwrap_or_else(|| "SBER".into());
    let class = env("BCS_LIVE_CLASS").unwrap_or_else(|| "TQBR".into());

    let mut secrets = vec![env("BCS_REFRESH_TOKEN").expect("BCS_REFRESH_TOKEN in .env")];
    secrets.push(http.get_access_token().await.expect("access token"));
    let recorder = Recorder::new("bcs", REFERENCE_DIR).with_secrets(secrets);

    let mut answers = probes::read_only(&http, &ticker, &class).await;
    if trade {
        let price = env("BCS_LIVE_PRICE").expect("BCS_LIVE_PRICE with BCS_LIVE_TRADE=1");
        let qty: i64 = env("BCS_LIVE_QTY")
            .and_then(|q| q.parse().ok())
            .unwrap_or(1);
        answers.extend(probes::order_roundtrip(&http, &ticker, &class, &price, qty).await);
    } else {
        println!(
            "no order: set BCS_LIVE_TRADE=1 and BCS_LIVE_PRICE to record the order probes too"
        );
    }
    for (name, outcome) in &answers {
        let path = recorder.record(name, outcome);
        println!("recorded {name}: {}", path.display());
    }
    for (name, outcome) in &answers {
        if let brokapi_mocks::live::Outcome::Refused { code, .. } = outcome {
            println!("ANSWER: {name} -> refused with {code}");
        }
    }
}
