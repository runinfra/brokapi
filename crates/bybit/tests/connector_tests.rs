//! The behaviour of the Bybit connectors that the shared scenarios do not cover, against the imitation.

mod mock_server;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use brokapi_bybit::limits::default_limits;
use brokapi_bybit::util::instrument_id;
use brokapi_bybit::{MarketDataConfig, MarketDataConnector, UserDataConfig, UserDataConnector};
use brokapi_bybit_protocol::config::BybitConfig;
use brokapi_bybit_protocol::http::models::CreateOrderRequest;
use brokapi_core::{
    ClientOrderId, Decimal, Envelope, Error, Event, ExecutionSource, InstrumentId, InstrumentKind,
    InstrumentQuery, InstrumentSource, MarketDataSource, OrderBook, OrderBookSource, OrderExecutor,
    OrderState, OrderType, PlaceOrderRequest, PlaceStopOrderRequest, PortfolioSource, Price, Qty,
    ResyncReason, ShutdownMode, Side, StopExpiry, StopOrderExecutor, StopOrderKind, StopOrderRef,
    SubscribeOpts, TimeRange, Timeframe, UnixNanos, dec,
};
use brokapi_runtime::BrokerLimiter;
use mock_server::{DEFAULT_KEY, MockOptions, MockServer, SECRET};
use tokio::sync::mpsc;

fn mock() -> InstrumentId {
    instrument_id("MOCKUSDT", "linear")
}

fn limiter() -> Arc<BrokerLimiter> {
    let mut limits = default_limits();
    for bucket in limits.buckets.values_mut() {
        bucket.burst = bucket.burst.max(1000);
    }
    BrokerLimiter::new(&limits)
}

async fn md(server: &MockServer) -> (MarketDataConnector, mpsc::Receiver<Envelope>) {
    let (tx, rx) = mpsc::channel(256);
    let cfg = BybitConfig::for_mock(DEFAULT_KEY, SECRET, &server.http_base);
    let md = MarketDataConnector::start(MarketDataConfig::new(cfg, "bybit-md"), limiter(), tx)
        .await
        .unwrap();
    (md, rx)
}

async fn ud_as(
    server: &MockServer,
    key: &str,
    secret: &str,
) -> Result<(UserDataConnector, mpsc::Receiver<Envelope>), Error> {
    let (tx, rx) = mpsc::channel(256);
    let cfg = BybitConfig::for_mock(key, secret, &server.http_base);
    let ud = UserDataConnector::start(UserDataConfig::new(cfg, "bybit-ud"), limiter(), tx).await?;
    Ok((ud, rx))
}

async fn ud(server: &MockServer) -> (UserDataConnector, mpsc::Receiver<Envelope>) {
    ud_as(server, DEFAULT_KEY, SECRET).await.unwrap()
}

fn limit_buy(price: Price) -> PlaceOrderRequest {
    PlaceOrderRequest {
        cid: ClientOrderId::new_v4(),
        instrument: mock(),
        side: Side::Buy,
        order_type: OrderType::Limit,
        qty: Qty::from(10),
        price: Some(price),
    }
}

fn stop_loss(price: Price) -> PlaceStopOrderRequest {
    PlaceStopOrderRequest {
        cid: ClientOrderId::new_v4(),
        instrument: mock(),
        side: Side::Sell,
        kind: StopOrderKind::StopLoss,
        qty: Qty::from(10),
        stop_price: price,
        limit_price: None,
        expiry: StopExpiry::GoodTillCancel,
    }
}

async fn next_event(
    rx: &mut mpsc::Receiver<Envelope>,
    what: &str,
    pred: impl Fn(&Envelope) -> bool,
) -> Envelope {
    loop {
        let env = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap_or_else(|_| panic!("no {what} within 5 seconds"))
            .expect("the channel is open");
        if pred(&env) {
            return env;
        }
    }
}

// ─── start and keys ───────────────────────────────────────────────────────────

#[tokio::test]
async fn a_wrong_key_pair_is_a_configuration_error_at_start() {
    let server = MockServer::start(Arc::default()).await;
    for (key, secret) in [(DEFAULT_KEY, "wrong-secret"), ("not-our-key", SECRET)] {
        match ud_as(&server, key, secret).await {
            Err(Error::Config(e)) => {
                assert!(e.to_string().contains("authentication refused"), "{e}")
            }
            Err(other) => panic!("{other:?}"),
            Ok(_) => panic!("the key pair is wrong"),
        }
    }
}

#[tokio::test]
async fn a_clock_that_runs_off_is_corrected_at_start() {
    let opts = Arc::new(MockOptions::default());
    opts.clock_offset_ms.store(90_000, Ordering::SeqCst);
    let server = MockServer::start(Arc::clone(&opts)).await;
    let (ud, _rx) = ud(&server).await;
    ud.reconcile(&[])
        .await
        .expect("the commands sign with the corrected time");
    assert!(opts.time_calls.load(Ordering::SeqCst) >= 1);
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn the_connector_says_what_it_can_do() {
    let server = MockServer::start(Arc::default()).await;
    let (ud, _rx) = ud(&server).await;
    let caps = ud.caps();
    assert!(caps.supports_stop_orders && caps.streams_executions);
    assert!(!caps.supports_replace && !caps.ws_rpc && !caps.ordered_command_channel);
    assert!(
        caps.sandbox,
        "the imitation is a virtual account, like the demo contour"
    );
    ud.shutdown(ShutdownMode::Immediate).await;
    let (md, _rx) = md(&server).await;
    let caps = md.caps();
    assert_eq!(caps.max_order_book_depth, 1000);
    assert!(
        caps.streaming.order_book
            && caps.streaming.quotes
            && caps.streaming.candles
            && caps.streaming.market_trades
    );
    md.shutdown(ShutdownMode::Immediate).await;
}

// ─── market data ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn the_directory_knows_perpetuals_and_dated_futures_apart() {
    let server = MockServer::start(Arc::default()).await;
    let (md, _rx) = md(&server).await;
    let perps = md
        .find(InstrumentQuery::ByKind(InstrumentKind::Perpetual))
        .await
        .unwrap();
    let mut names: Vec<_> = perps.iter().map(|i| i.id.key.display()).collect();
    names.sort();
    assert_eq!(names, ["BTCUSDT@linear", "MOCKUSDT@linear"]);
    let futures = md
        .find(InstrumentQuery::ByKind(InstrumentKind::Future))
        .await
        .unwrap();
    assert_eq!(futures.len(), 1);
    assert_eq!(futures[0].id.key.display(), "BTC-26DEC26@linear");
    assert!(
        md.find(InstrumentQuery::ByKind(InstrumentKind::Stock))
            .await
            .unwrap()
            .is_empty()
    );
    assert!(matches!(
        md.find(InstrumentQuery::ByKind(InstrumentKind::Other))
            .await,
        Err(Error::Unsupported { .. })
    ));
    assert!(matches!(
        md.find(InstrumentQuery::ByIsin("RU0009029540".into()))
            .await,
        Err(Error::Unsupported { .. })
    ));
    // A symbol the exchange does not have is an empty answer of a search, an error of a lookup.
    let none = md
        .find(InstrumentQuery::ByTicker {
            ticker: "nopeusdt".into(),
            exchange: None,
        })
        .await
        .unwrap();
    assert!(none.is_empty());
    let btc = instrument_id("BTCUSDT", "linear");
    let params = md.trading_params(&btc).await.unwrap();
    assert_eq!(
        (
            params.lot_size.to_string(),
            params.min_qty.to_string(),
            params.price_step.to_string()
        ),
        ("0.001".into(), "0.001".into(), "0.10".into())
    );
    let i = md.get(&btc).await.unwrap();
    assert_eq!(
        (i.kind, i.exchange.as_str(), i.currency.as_str()),
        (InstrumentKind::Perpetual, "BYBIT", "USDT")
    );
    md.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn a_book_read_and_the_tape_come_in_the_order_the_traits_promise() {
    let server = MockServer::start(Arc::default()).await;
    let (md, _rx) = md(&server).await;
    let OrderBook::Snapshot { bids, asks, .. } = md.order_book(&mock(), 5).await.unwrap() else {
        panic!("a snapshot")
    };
    assert_eq!((bids.len(), asks.len()), (5, 5));
    assert!(
        bids[0].price > bids[4].price && asks[0].price < asks[4].price,
        "best first"
    );
    let trades = md.last_trades(&mock(), 2).await.unwrap();
    assert_eq!(trades.len(), 2);
    assert!(
        trades[0].ts.broker_ts <= trades[1].ts.broker_ts,
        "the oldest first"
    );
    md.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn candles_over_the_cap_are_refused_and_the_cap_is_told() {
    let opts = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&opts)).await;
    let (md, _rx) = md(&server).await;
    assert_eq!(
        md.candles_max_span(Timeframe::M1),
        Some(Duration::from_secs(60_000))
    );
    assert_eq!(md.candles_max_span(Timeframe::M10), None);
    let day = TimeRange::new(
        UnixNanos::from_nanos(1_792_368_000_000_000_000),
        UnixNanos::from_nanos(1_792_454_400_000_000_000),
    );
    // A day of minutes is 1440 bars: more than one request holds.
    match md.candles(&mock(), Timeframe::M1, day).await {
        Err(Error::Rejected { code, .. }) => assert_eq!(code, "range_too_long"),
        other => panic!("{other:?}"),
    }
    match md.candles(&mock(), Timeframe::M10, day).await {
        Err(Error::Unsupported { capability }) => assert_eq!(capability, "candles_m10"),
        other => panic!("{other:?}"),
    }
    assert!(
        opts.kline_requests.lock().unwrap().is_empty(),
        "neither refusal was sent"
    );
    // The window of a request is cut at the half open end: the bar that starts at `to` is not in.
    let hour = TimeRange::new(
        day.from,
        UnixNanos::from_nanos(day.from.as_nanos() + 3_600_000_000_000),
    );
    let bars = md.candles(&mock(), Timeframe::M15, hour).await.unwrap();
    assert_eq!(bars.len(), 4);
    assert!(bars.windows(2).all(|w| w[0].open_ts < w[1].open_ts));
    md.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn a_lost_delta_asks_the_exchange_for_a_new_snapshot() {
    let opts = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&opts)).await;
    let (md, mut rx) = md(&server).await;
    let sub = md
        .subscribe_order_book(&mock(), SubscribeOpts::lossless(64))
        .await
        .unwrap();
    next_event(&mut rx, "the first book", |e| {
        e.sub == Some(sub) && matches!(e.payload, Event::OrderBook(_))
    })
    .await;
    let frames = opts.book_frames.load(Ordering::SeqCst);
    // The snapshot was update 1000: a delta numbered 1001 is the next one, one numbered 1005 is not.
    server.push_public_event(r#"{"topic":"orderbook.50.MOCKUSDT","type":"delta","ts":1,"data":{"s":"MOCKUSDT","b":[["99.99","7"]],"a":[],"u":1001,"seq":1}}"#);
    let book = next_event(&mut rx, "the book with the delta", |e| {
        e.sub == Some(sub) && matches!(e.payload, Event::OrderBook(_))
    })
    .await;
    let Event::OrderBook(OrderBook::Snapshot { bids, .. }) = book.payload else {
        panic!()
    };
    assert_eq!(
        bids[0].price.to_string(),
        "100.00",
        "the new level 99.99 is below the best bid"
    );
    assert!(
        bids.iter()
            .any(|l| l.price.to_string() == "99.99" && l.qty.to_string() == "7")
    );
    server.push_public_event(r#"{"topic":"orderbook.50.MOCKUSDT","type":"delta","ts":2,"data":{"s":"MOCKUSDT","b":[],"a":[],"u":1005,"seq":2}}"#);
    next_event(&mut rx, "the resync", |e| {
        matches!(e.payload, Event::Resync { sub: s, reason: ResyncReason::SequenceGap, .. } if s == sub)
    })
    .await;
    // The exchange sent a snapshot again: the book is back.
    next_event(&mut rx, "the book again", |e| {
        e.sub == Some(sub) && matches!(e.payload, Event::OrderBook(_))
    })
    .await;
    assert!(
        opts.book_frames.load(Ordering::SeqCst) > frames,
        "a new snapshot was asked for"
    );
    md.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn two_streams_of_one_topic_share_it_and_the_exchange_is_told_when_the_last_goes() {
    let opts = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&opts)).await;
    let (md, mut rx) = md(&server).await;
    let quotes = md
        .subscribe_quotes(&mock(), SubscribeOpts::conflate())
        .await
        .unwrap();
    let book = md
        .subscribe_order_book(
            &mock(),
            SubscribeOpts {
                depth: Some(1),
                ..SubscribeOpts::lossless(16)
            },
        )
        .await
        .unwrap();
    // Both read the one topic `orderbook.1.MOCKUSDT`.
    next_event(&mut rx, "a quote", |e| {
        e.sub == Some(quotes) && matches!(e.payload, Event::Quote(_))
    })
    .await;
    next_event(&mut rx, "a book of depth 1", |e| {
        e.sub == Some(book) && matches!(&e.payload, Event::OrderBook(OrderBook::Snapshot { bids, .. }) if bids.len() == 1)
    })
    .await;
    let unsubscribes = |opts: &MockOptions| {
        opts.ws_commands
            .lock()
            .unwrap()
            .iter()
            .filter(|c| c.contains("\"unsubscribe\""))
            .count()
    };
    // The late book asked the exchange for the topic again, which is one unsubscribe in the log already.
    let asked_again = unsubscribes(&opts);
    md.unsubscribe(quotes).await.unwrap();
    assert_eq!(
        unsubscribes(&opts),
        asked_again,
        "the book still reads the topic"
    );
    md.unsubscribe(book).await.unwrap();
    brokapi_mocks::time::wait_until("the exchange is told", || {
        unsubscribes(&opts) == asked_again + 1
    })
    .await;
    assert!(
        matches!(md.unsubscribe(book).await, Err(Error::Rejected { ref code, .. }) if code == "unknown_subscription")
    );
    md.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn a_topic_the_exchange_refuses_is_a_subscription_error_and_leaves_nothing() {
    let opts = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&opts)).await;
    let (md, _rx) = md(&server).await;
    match md
        .subscribe_quotes(
            &instrument_id("NOPEUSDT", "linear"),
            SubscribeOpts::conflate(),
        )
        .await
    {
        Err(Error::Rejected { code, message }) => {
            assert_eq!(code, "subscription_refused");
            assert!(message.contains("handler not found"), "{message}");
        }
        other => panic!("{other:?}"),
    }
    // The same call can be repeated with a good symbol.
    md.subscribe_quotes(&mock(), SubscribeOpts::conflate())
        .await
        .unwrap();
    md.shutdown(ShutdownMode::Immediate).await;
}

// ─── orders ───────────────────────────────────────────────────────────────────

#[tokio::test]
async fn an_order_is_found_by_its_cid_working_filled_and_cancelled() {
    let server = MockServer::start(Arc::default()).await;
    let (ud, _rx) = ud(&server).await;
    ud.reconcile(&[]).await.unwrap();
    let a = limit_buy(Price::new(dec!(100.00)));
    let b = limit_buy(Price::new(dec!(100.00)));
    let ack = ud.place(a.clone()).await.unwrap();
    ud.place(b.clone()).await.unwrap();
    let s = ud.order_state(a.cid).await.unwrap();
    assert!(matches!(s.state, OrderState::Acked { .. }));
    assert_eq!(s.broker_id.as_ref(), Some(&ack.broker_id));
    assert_eq!(
        (s.instrument.key.display().as_str(), s.qty, s.price),
        (
            "MOCKUSDT@linear",
            Qty::from(10),
            Some(Price::new(dec!(100.00)))
        )
    );
    server.fill(
        &a.cid.hyphenated().to_string(),
        Qty::from(4),
        Price::from(100),
    );
    match ud.order_state(a.cid).await.unwrap().state {
        OrderState::PartiallyFilled { filled, left, .. } => {
            assert_eq!((filled, left), (Qty::from(4), Qty::from(6)))
        }
        other => panic!("{other:?}"),
    }
    server.fill(
        &a.cid.hyphenated().to_string(),
        Qty::from(6),
        Price::from(100),
    );
    assert!(matches!(
        ud.order_state(a.cid).await.unwrap().state,
        OrderState::Filled { .. }
    ));
    ud.cancel(b.cid).await.unwrap();
    assert!(matches!(
        ud.order_state(b.cid).await.unwrap().state,
        OrderState::Cancelled { .. }
    ));
    // A second cancel of a closed order is a refusal of the exchange with its code, a cancel of an order that was never
    // there is `not_found`.
    assert!(matches!(
        ud.cancel(b.cid).await,
        Err(Error::Rejected { .. })
    ));
    match ud.cancel(ClientOrderId::new_v4()).await {
        Err(Error::Rejected { code, .. }) => assert_eq!(code, brokapi_core::NOT_FOUND),
        other => panic!("{other:?}"),
    }
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn the_replace_of_the_shared_interface_is_not_offered() {
    let server = MockServer::start(Arc::default()).await;
    let (ud, _rx) = ud(&server).await;
    ud.reconcile(&[]).await.unwrap();
    let a = limit_buy(Price::new(dec!(100.00)));
    ud.place(a.clone()).await.unwrap();
    let r = ud
        .replace(brokapi_core::ReplaceOrderRequest {
            cid: ClientOrderId::new_v4(),
            original_cid: a.cid,
            qty: None,
            price: Some(Price::new(dec!(101.00))),
        })
        .await;
    assert!(matches!(r, Err(Error::Unsupported { ref capability }) if capability == "replace"));
    assert_eq!(
        ud.order_state(a.cid).await.unwrap().price,
        Some(Price::new(dec!(100.00))),
        "the order is untouched"
    );
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn reconcile_sorts_the_ledger_into_confirmed_closed_and_foreign() {
    let server = MockServer::start(Arc::default()).await;
    let (ud, _rx) = ud(&server).await;
    ud.reconcile(&[]).await.unwrap();
    let working = limit_buy(Price::new(dec!(100.00)));
    let cancelled = limit_buy(Price::new(dec!(100.00)));
    let filled = limit_buy(Price::new(dec!(100.00)));
    let stop = stop_loss(Price::new(dec!(200.00)));
    for o in [&working, &cancelled, &filled] {
        ud.place(o.clone()).await.unwrap();
    }
    ud.place_stop(stop.clone()).await.unwrap();
    ud.cancel(cancelled.cid).await.unwrap();
    server.fill(
        &filled.cid.hyphenated().to_string(),
        Qty::from(10),
        Price::from(100),
    );
    // An order of another program: placed through the raw handle with a link id that is no uuid.
    ud.raw()
        .call_command("PlaceOrder", |http| async move {
            http.create_order(&CreateOrderRequest {
                category: "linear".into(),
                symbol: "MOCKUSDT".into(),
                side: "Sell".into(),
                order_type: "Limit".into(),
                qty: "10".parse().unwrap(),
                price: Some("500.00".parse().unwrap()),
                order_link_id: Some("from-the-app".into()),
                ..Default::default()
            })
            .await
        })
        .await
        .unwrap();
    let never_sent = ClientOrderId::new_v4();
    // A fresh process knows nothing of these orders: it must find them by their link ids.
    let (fresh, _rx2) = self::ud(&server).await;
    let ledger = [working.cid, cancelled.cid, filled.cid, stop.cid, never_sent];
    let report = fresh.reconcile(&ledger).await.unwrap();
    let mut confirmed: Vec<_> = report.confirmed.iter().map(|o| o.cid).collect();
    confirmed.sort();
    let mut want = vec![working.cid, stop.cid];
    want.sort();
    assert_eq!(
        confirmed, want,
        "the working order and the waiting stop order are alive"
    );
    let mut closed = report.closed_offline.clone();
    closed.sort();
    let mut want = vec![cancelled.cid, filled.cid, never_sent];
    want.sort();
    assert_eq!(
        closed, want,
        "a cancelled, a filled and an order the exchange never saw are closed"
    );
    assert_eq!(report.foreign.len(), 1);
    assert_eq!(report.foreign[0].price, Some(Price::new(dec!(500.00))));
    // The stop order of the ledger is shown as an order that waits.
    let stop_snap = report.confirmed.iter().find(|o| o.cid == stop.cid).unwrap();
    assert!(matches!(stop_snap.state, OrderState::Acked { .. }));
    // An order of another program can be cancelled by the cid it was listed with.
    let foreign_cid = report.foreign[0].cid;
    fresh.cancel(foreign_cid).await.unwrap();
    assert!(
        fresh
            .active_orders()
            .await
            .unwrap()
            .iter()
            .all(|o| o.cid != foreign_cid)
    );
    ud.shutdown(ShutdownMode::Immediate).await;
    fresh.shutdown(ShutdownMode::Immediate).await;
}

// ─── stop orders ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn a_stop_order_waits_apart_and_joins_the_order_stream_when_it_fires() {
    let server = MockServer::start(Arc::default()).await;
    let (ud, mut rx) = ud(&server).await;
    ud.reconcile(&[]).await.unwrap();
    let sub = ud.subscribe_order_state().await.unwrap();
    let stop = stop_loss(Price::new(dec!(200.00)));
    let ack = ud.place_stop(stop.clone()).await.unwrap();
    assert!(
        ud.active_orders().await.unwrap().is_empty(),
        "a waiting stop order is not an active order"
    );
    assert_eq!(ud.active_stop_orders().await.unwrap().len(), 1);
    // The stream says nothing of it while it waits ...
    let marker = limit_buy(Price::new(dec!(100.00)));
    ud.place(marker.clone()).await.unwrap();
    let first = next_event(&mut rx, "the marker", |e| {
        e.sub == Some(sub) && matches!(e.payload, Event::Order(_))
    })
    .await;
    let Event::Order(o) = first.payload else {
        unreachable!()
    };
    assert_eq!(
        o.cid, marker.cid,
        "the first order event is the marker, not the stop order"
    );
    // ... and tells of it, under its cid, when it fires.
    server.trigger(&stop.cid.hyphenated().to_string());
    let fired = next_event(&mut rx, "the fired order", |e| {
        e.sub == Some(sub) && matches!(&e.payload, Event::Order(o) if o.cid == stop.cid)
    })
    .await;
    let Event::Order(o) = fired.payload else {
        unreachable!()
    };
    assert_eq!(o.broker_id.as_ref(), Some(&ack.broker_id));
    assert!(ud.active_stop_orders().await.unwrap().is_empty());
    assert!(
        ud.active_orders()
            .await
            .unwrap()
            .iter()
            .any(|o| o.cid == stop.cid),
        "now it is an order"
    );
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn a_stop_order_needs_the_expiry_the_exchange_has_and_a_stream_it_does_not_have() {
    let server = MockServer::start(Arc::default()).await;
    let (ud, _rx) = ud(&server).await;
    ud.reconcile(&[]).await.unwrap();
    let mut s = stop_loss(Price::new(dec!(200.00)));
    s.expiry = StopExpiry::EndOfDay;
    assert!(
        matches!(ud.place_stop(s).await, Err(Error::Rejected { ref code, .. }) if code == "unsupported_expiry")
    );
    assert!(matches!(
        ud.subscribe_stop_orders().await,
        Err(Error::Unsupported { .. })
    ));
    match ud
        .cancel_stop(StopOrderRef::Cid(ClientOrderId::new_v4()))
        .await
    {
        Err(Error::Rejected { code, .. }) => assert_eq!(code, brokapi_core::NOT_FOUND),
        other => panic!("{other:?}"),
    }
    ud.shutdown(ShutdownMode::Immediate).await;
}

// ─── fills and the portfolio ──────────────────────────────────────────────────

#[tokio::test]
async fn fills_are_filtered_by_the_range_and_carry_the_cid() {
    let server = MockServer::start(Arc::default()).await;
    let (ud, mut rx) = ud(&server).await;
    ud.reconcile(&[]).await.unwrap();
    let sub = ud.subscribe_executions().await.unwrap();
    let o = limit_buy(Price::new(dec!(100.00)));
    ud.place(o.clone()).await.unwrap();
    server.fill(
        &o.cid.hyphenated().to_string(),
        Qty::from(10),
        Price::new(dec!(100.00)),
    );
    let e = next_event(&mut rx, "the fill", |e| {
        e.sub == Some(sub) && matches!(e.payload, Event::Execution(_))
    })
    .await;
    let Event::Execution(ex) = e.payload else {
        unreachable!()
    };
    assert_eq!(
        (ex.cid, ex.qty, ex.commission.to_string()),
        (Some(o.cid), Qty::from(10), "0.0055".into())
    );
    let now = chrono::Utc::now().timestamp_millis();
    let all = TimeRange::new(
        UnixNanos::from_nanos((now - 3_600_000) * 1_000_000),
        UnixNanos::from_nanos((now + 3_600_000) * 1_000_000),
    );
    let got = ud.executions(all).await.unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].cid, Some(o.cid));
    let past = TimeRange::new(
        UnixNanos::ZERO,
        UnixNanos::from_nanos((now - 3_600_000) * 1_000_000),
    );
    assert!(ud.executions(past).await.unwrap().is_empty());
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn the_portfolio_has_signed_positions_and_balances_with_the_margin_held() {
    let server = MockServer::start(Arc::default()).await;
    let (ud, mut rx) = ud(&server).await;
    let p = ud.portfolio().await.unwrap();
    assert_eq!(p.positions.len(), 1);
    assert_eq!(p.positions[0].instrument.key.display(), "BTCUSDT@linear");
    assert_eq!(p.positions[0].qty.to_string(), "0.500");
    assert_eq!(p.positions[0].avg_price.unwrap().to_string(), "81000.50");
    assert_eq!(
        p.positions[0].current_price.unwrap().to_string(),
        "81695.30"
    );
    // The empty BTC wallet is not listed; the USDT one has the margin of the orders and positions as blocked.
    assert_eq!(p.balances.len(), 1);
    let b = &p.balances[0];
    assert_eq!(
        (
            b.currency.as_str(),
            b.blocked.to_string(),
            b.available.to_string()
        ),
        ("USDT", "200".into(), "9800.5".into())
    );
    assert_eq!(ud.positions().await.unwrap().len(), 1);
    assert_eq!(ud.balances().await.unwrap().len(), 1);
    // A subscription starts with the portfolio as it is now: the streams of the exchange send changes only.
    let sub = ud
        .subscribe_portfolio(SubscribeOpts::lossless(16))
        .await
        .unwrap();
    next_event(&mut rx, "the positions", |e| {
        e.sub == Some(sub) && matches!(&e.payload, Event::Positions(p) if p.len() == 1)
    })
    .await;
    next_event(&mut rx, "the balances", |e| {
        e.sub == Some(sub) && matches!(&e.payload, Event::Balances(b) if b.len() == 1)
    })
    .await;
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn the_raw_handle_reaches_methods_the_traits_do_not_and_spends_the_quota() {
    let opts = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&opts)).await;
    let (ud, _rx) = ud(&server).await;
    ud.reconcile(&[]).await.unwrap();
    let v: serde_json::Value = ud
        .raw()
        .call("GetWallet", |http| async move {
            http.get_private(
                "/v5/account/wallet-balance",
                &[("accountType", "UNIFIED".into())],
            )
            .await
        })
        .await
        .unwrap();
    assert_eq!(v["list"][0]["accountType"], "UNIFIED");
    // The quota of a route is spent before anything is sent: a tight limiter refuses at once.
    let tight = {
        let mut limits = default_limits();
        for b in limits.buckets.values_mut() {
            b.burst = 1;
            b.rate = 1;
        }
        BrokerLimiter::new(&limits)
    };
    let (tx, _rx2) = mpsc::channel(8);
    let cfg = BybitConfig::for_mock(DEFAULT_KEY, SECRET, &server.http_base);
    let md = MarketDataConnector::start(MarketDataConfig::new(cfg, "bybit-md"), tight, tx)
        .await
        .unwrap();
    let r = md.raw();
    r.call("GetServerTime", |http| async move {
        http.get_server_time().await
    })
    .await
    .unwrap();
    let before = opts.time_calls.load(Ordering::SeqCst);
    assert!(matches!(
        r.call("GetServerTime", |http| async move {
            http.get_server_time().await
        })
        .await,
        Err(Error::RateLimited { .. })
    ));
    assert_eq!(
        opts.time_calls.load(Ordering::SeqCst),
        before,
        "a refused call was not sent"
    );
    md.shutdown(ShutdownMode::Immediate).await;
    ud.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn the_lists_of_orders_spend_the_quota_of_the_reads() {
    // One read of the order list a second, and nothing comes back within the test: the second call of each list is
    // refused before it is sent.
    let server = MockServer::start(Arc::default()).await;
    let limiter = {
        let mut limits = default_limits();
        for b in limits.buckets.values_mut() {
            b.burst = b.burst.max(1000);
        }
        let read = limits.buckets.get_mut("trade_read").unwrap();
        (read.rate, read.per, read.burst) = (1, Duration::from_secs(3600), 1);
        BrokerLimiter::new(&limits)
    };
    let (tx, _rx) = mpsc::channel(256);
    let cfg = BybitConfig::for_mock(DEFAULT_KEY, SECRET, &server.http_base);
    let ud = UserDataConnector::start(UserDataConfig::new(cfg, "bybit-ud"), limiter, tx)
        .await
        .unwrap();
    ud.active_orders().await.unwrap();
    assert!(
        matches!(ud.active_orders().await, Err(Error::RateLimited { .. })),
        "active_orders"
    );
    assert!(
        matches!(
            ud.active_stop_orders().await,
            Err(Error::RateLimited { .. })
        ),
        "active_stop_orders"
    );
    ud.shutdown(ShutdownMode::Immediate).await;
}
