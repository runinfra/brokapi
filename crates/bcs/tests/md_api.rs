//! Connector-level tests of the BCS market-data side on the mock: market trades (history and stream),
//! quotes, candles, trading status, schedule, directory lookups. The mock checks the requests against
//! the docs (timeframe enum, required schedule `ticker`, instrument type enum, no `limit` on the tape),
//! so a request the real API would refuse fails here.

mod mock_server;

use brokapi_core::{Decimal, dec};
use brokapi_core::{Price, Qty};
use std::sync::Arc;
use std::time::Duration;

use brokapi_bcs::{MarketDataConfig, MarketDataConnector};
use brokapi_bcs_protocol::config::BcsConfig;
use brokapi_core::{
    Envelope, Error, Event, InstrumentId, InstrumentKey, InstrumentKind, InstrumentQuery,
    InstrumentSource, MarketDataSource, NativeId, OrderBookSource, SessionKind, ShutdownMode, Side,
    SubscribeOpts, TimeRange, Timeframe, TradingAvailability, UnixNanos,
};
use brokapi_runtime::{BrokerLimiter, BrokerLimits};
use mock_server::{MockOptions, MockServer};
use tokio::sync::mpsc;
use tokio::time::timeout;

fn sber() -> InstrumentId {
    InstrumentId::new(
        InstrumentKey::new("SBER", "TQBR"),
        NativeId::TickerClass {
            ticker: "SBER".into(),
            class_code: "TQBR".into(),
        },
    )
}

async fn md(server: &MockServer) -> (MarketDataConnector, mpsc::Receiver<Envelope>) {
    let raw = r#"
[bcs.buckets.big]
rate = 10000
per = "1s"
burst = 10000
[bcs.route]
"*" = ["big"]
[bcs.conns]
max_conns = 8
max_subs_per_conn = 50
supports_multiplex = false
dedicated_command_conns = 0
"#;
    let limiter = BrokerLimiter::new(&BrokerLimits::from_toml_str(raw).unwrap());
    let (tx, rx) = mpsc::channel(256);
    let cfg = BcsConfig::for_mock("mock-refresh-token", &server.http_base).with_write(false);
    let md = MarketDataConnector::start(MarketDataConfig::new(cfg, "bcs-md"), limiter, tx)
        .await
        .expect("md start");
    (md, rx)
}

fn day() -> TimeRange {
    // 2026-10-19 00:00 UTC .. 2026-10-20 00:00 UTC
    TimeRange::new(
        UnixNanos::from_nanos(1_792_368_000_000_000_000),
        UnixNanos::from_nanos(1_792_454_400_000_000_000),
    )
}

async fn event(rx: &mut mpsc::Receiver<Envelope>, pred: impl Fn(&Event) -> bool) -> Envelope {
    timeout(Duration::from_secs(8), async {
        while let Some(env) = rx.recv().await {
            if pred(&env.payload) {
                return env;
            }
        }
        panic!("event channel closed");
    })
    .await
    .expect("event within 8s")
}

#[tokio::test]
async fn the_tape_is_the_newest_trades_oldest_first_with_the_asked_instrument() {
    let server = MockServer::start(Arc::default()).await;
    let (md, _rx) = md(&server).await;
    // The mock tape has two trades: 07:05:00 buy (200) and 07:05:01 sell (100).
    let all = md.last_trades(&sber(), 10).await.unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!(
        (all[0].side, all[0].qty.decimal(), all[0].price.decimal()),
        (Side::Buy, dec!(200.0), dec!(300.1))
    );
    assert_eq!(
        (all[1].side, all[1].qty.decimal(), all[1].price.decimal()),
        (Side::Sell, dec!(100.0), dec!(300.2))
    );
    assert!(
        all[0]
            .ts
            .broker_ts
            .is_some_and(|a| a < all[1].ts.broker_ts.unwrap()),
        "oldest first"
    );
    assert!(
        all.iter()
            .all(|t| t.instrument.key.display() == "SBER@TQBR"),
        "the instrument asked for"
    );
    // The API has no limit: the connector keeps the newest ones.
    let one = md.last_trades(&sber(), 1).await.unwrap();
    assert_eq!(one.len(), 1);
    assert_eq!(one[0].side, Side::Sell);
    md.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn candles_use_the_documented_timeframe_labels() {
    let server = MockServer::start(Arc::default()).await;
    let (md, _rx) = md(&server).await;
    let bars = md.candles(&sber(), Timeframe::M1, day()).await.unwrap();
    assert_eq!(bars.len(), 3);
    for c in &bars {
        assert!(c.low.decimal() <= c.open.decimal() && c.open.decimal() <= c.high.decimal());
        assert!(c.low.decimal() <= c.close.decimal() && c.close.decimal() <= c.high.decimal());
        assert_eq!(c.timeframe, Timeframe::M1);
        assert_eq!(c.instrument.key.display(), "SBER@TQBR");
    }
    // `D1` / `W1` go out as `D` / `W`: the mock answers 400 to anything outside the docs' enum.
    md.candles(&sber(), Timeframe::D1, day())
        .await
        .expect("D1 is sent as D");
    md.candles(&sber(), Timeframe::W1, day())
        .await
        .expect("W1 is sent as W");
    md.candles(&sber(), Timeframe::MN, day()).await.expect("MN");
    // M10 does not exist at BCS: refused locally, nothing sent, no quota spent.
    assert!(matches!(
        md.candles(&sber(), Timeframe::M10, day()).await,
        Err(Error::Unsupported { .. })
    ));
    assert!(matches!(
        md.subscribe_candles(&sber(), Timeframe::M10, SubscribeOpts::conflate())
            .await,
        Err(Error::Unsupported { .. })
    ));
    md.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn status_and_schedule_read_the_documented_answers() {
    let server = MockServer::start(Arc::default()).await;
    let (md, _rx) = md(&server).await;
    let st = md.trading_status(&sber()).await.unwrap();
    assert_eq!(st.availability, TradingAvailability::Open);
    assert_eq!(st.instrument.key.display(), "SBER@TQBR");

    // Per instrument (the API needs the ticker): the closed technical break is not a session.
    let sched = md.schedule("SBER@TQBR", day()).await.unwrap();
    let kinds: Vec<_> = sched.sessions.iter().map(|s| s.kind).collect();
    assert_eq!(
        kinds,
        vec![
            SessionKind::Auction,
            SessionKind::Main,
            SessionKind::PostMarket
        ]
    );
    assert!(
        sched
            .sessions
            .iter()
            .all(|s| s.start.as_nanos() < s.end.as_nanos())
    );
    assert!(
        sched
            .sessions
            .windows(2)
            .all(|w| w[0].end.as_nanos() <= w[1].start.as_nanos()),
        "in order, not overlapping"
    );
    // 10:00 Moscow = 07:00 UTC on the day of the range
    let main = &sched.sessions[1];
    assert_eq!(
        main.start.as_nanos(),
        day().from.as_nanos() + 7 * 3600 * 1_000_000_000
    );
    // A bare exchange name cannot be answered.
    assert!(matches!(
        md.schedule("MOEX", day()).await,
        Err(Error::Config(_))
    ));
    md.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn directory_lookups_by_ticker_isin_and_kind() {
    let server = MockServer::start(Arc::default()).await;
    let (md, _rx) = md(&server).await;
    let by_ticker = md
        .find(InstrumentQuery::ByTicker {
            ticker: "SBER".into(),
            exchange: None,
        })
        .await
        .unwrap();
    assert!(by_ticker.iter().any(|i| i.id.key.display() == "SBER@TQBR"));
    let by_isin = md
        .find(InstrumentQuery::ByIsin("RU0009029540".into()))
        .await
        .unwrap();
    assert!(by_isin.iter().any(|i| i.id.key.symbol == "SBER"));
    // The kind goes out as the docs' type (`STOCK`), the mock refuses anything else.
    let stocks = md
        .find(InstrumentQuery::ByKind(InstrumentKind::Stock))
        .await
        .unwrap();
    assert!(stocks.iter().any(|i| i.id.key.symbol == "SBER"));
    md.find(InstrumentQuery::ByKind(InstrumentKind::Bond))
        .await
        .expect("BONDS");
    // Options need a base asset, `Other` has no type: refused, not guessed.
    assert!(matches!(
        md.find(InstrumentQuery::ByKind(InstrumentKind::Option))
            .await,
        Err(Error::Unsupported { .. })
    ));
    assert!(matches!(
        md.find(InstrumentQuery::ByKind(InstrumentKind::Other))
            .await,
        Err(Error::Unsupported { .. })
    ));

    let card = md.get(&sber()).await.unwrap();
    assert_eq!(
        (card.id.key.display().as_str(), card.currency.as_str()),
        ("SBER@TQBR", "RUB")
    );
    assert_eq!(card.isin.as_deref(), Some("RU0009029540"));
    let params = md.trading_params(&sber()).await.unwrap();
    assert!(params.lot_size > Qty::new(dec!(0.0)) && params.price_step > Price::new(dec!(0.0)));
    md.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn market_trades_quotes_and_candles_stream_with_our_subscription_ids() {
    let server = MockServer::start(Arc::default()).await;
    let (md, mut rx) = md(&server).await;

    let trades_sub = md
        .subscribe_market_trades(&sber(), SubscribeOpts::lossless(64))
        .await
        .unwrap();
    let env = event(&mut rx, |e| matches!(e, Event::MarketTrade(_))).await;
    assert_eq!((env.source.as_ref(), env.sub), ("bcs-md", Some(trades_sub)));
    let Event::MarketTrade(t) = env.payload else {
        unreachable!()
    };
    assert_eq!(
        (t.side, t.price.decimal(), t.qty.decimal()),
        (Side::Buy, dec!(300.15), dec!(100.0))
    );
    assert_eq!(t.instrument.key.display(), "SBER@TQBR");

    let quotes_sub = md
        .subscribe_quotes(&sber(), SubscribeOpts::conflate())
        .await
        .unwrap();
    let env = event(&mut rx, |e| matches!(e, Event::Quote(_))).await;
    assert_eq!(env.sub, Some(quotes_sub));
    let Event::Quote(q) = env.payload else {
        unreachable!()
    };
    assert_eq!(
        (
            q.bid.unwrap().price.decimal(),
            q.ask.unwrap().price.decimal()
        ),
        (dec!(300.1), dec!(300.2))
    );

    let candles_sub = md
        .subscribe_candles(&sber(), Timeframe::D1, SubscribeOpts::conflate())
        .await
        .unwrap();
    let env = event(&mut rx, |e| matches!(e, Event::Candle(_))).await;
    assert_eq!(env.sub, Some(candles_sub));
    let Event::Candle(c) = env.payload else {
        unreachable!()
    };
    // the mock echoes the label it got: `D`, which reads back as D1
    assert_eq!(
        (c.timeframe, c.open.decimal(), c.close.decimal()),
        (Timeframe::D1, dec!(300.0), dec!(300.5))
    );

    // Unsubscribe is fire and forget at BCS; a second one is not a panic.
    md.unsubscribe(trades_sub).await.unwrap();
    md.unsubscribe(quotes_sub).await.unwrap();
    md.unsubscribe(candles_sub).await.unwrap();
    let report = md
        .shutdown(ShutdownMode::Graceful {
            deadline: Duration::from_secs(2),
        })
        .await;
    assert_eq!(report.unsubscribe_failed, 0);
}

#[tokio::test]
async fn order_book_snapshot_and_stream_still_work_next_to_the_other_streams() {
    let server = MockServer::start(Arc::default()).await;
    let (md, mut rx) = md(&server).await;
    let book_sub = md
        .subscribe_order_book(&sber(), SubscribeOpts::conflate())
        .await
        .unwrap();
    md.subscribe_quotes(&sber(), SubscribeOpts::conflate())
        .await
        .unwrap();
    // Each event is attributed to its own subscription, not to the other one on the same socket.
    let b = event(&mut rx, |e| matches!(e, Event::OrderBook(_))).await;
    let q = event(&mut rx, |e| matches!(e, Event::Quote(_))).await;
    assert_eq!(b.sub, Some(book_sub));
    assert_ne!(q.sub, Some(book_sub));
    assert!(md.order_book(&sber(), 5).await.is_ok());
    md.shutdown(ShutdownMode::Immediate).await;
}

/// `find(ByKind)` reads every page: a full page of `size` means "ask for the next", a short one ends the list
/// (the docs of `GET /instruments/by-type`).
#[tokio::test]
async fn find_by_kind_reads_every_page() {
    for (total, pages) in [(250u32, 3usize), (100, 2), (7, 1)] {
        let opts = Arc::new(MockOptions::default());
        opts.by_type_total
            .store(total, std::sync::atomic::Ordering::Relaxed);
        let server = MockServer::start(Arc::clone(&opts)).await;
        let (md, _rx) = md(&server).await;
        let all = md
            .find(InstrumentQuery::ByKind(InstrumentKind::Stock))
            .await
            .unwrap();
        assert_eq!(all.len(), total as usize, "total {total}");
        let mut tickers: Vec<_> = all.iter().map(|i| i.id.key.symbol.clone()).collect();
        tickers.sort();
        tickers.dedup();
        assert_eq!(tickers.len(), total as usize, "every instrument once");
        let asked = opts.by_type_asked.lock().unwrap().clone();
        assert_eq!(asked.len(), pages, "requests for total {total}: {asked:?}");
        assert_eq!(
            asked[0],
            (Some(0), Some(100)),
            "from page 0, the biggest page the docs allow"
        );
        assert_eq!(asked.last().unwrap().0, Some(pages as u32 - 1));
        md.shutdown(ShutdownMode::Immediate).await;
    }
}
