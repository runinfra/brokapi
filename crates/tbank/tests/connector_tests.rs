//! Connectors against the gRPC mock: caps, trading rules, streams, reconnect, shutdown, limits.

mod mock_server;

use brokapi_core::{Decimal, dec};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use brokapi_core::{
    Backpressure, ClientOrderId, Envelope, Error, Event, ExecutionSource, InstrumentId,
    InstrumentKey, InstrumentQuery, InstrumentSource, MarketDataSource, NativeId, OrderBookSource,
    OrderExecutor, OrderState, OrderType, PlaceIdempotency, PlaceOrderRequest, PortfolioSource,
    Price, Qty, ResyncReason, ShutdownMode, Side, SubscribeOpts, TimeRange, Timeframe, UnixNanos,
};
use brokapi_runtime::BrokerLimiter;
use brokapi_tbank::limits::{default_limits, limits_from_toml_str};
use brokapi_tbank::{MarketDataConfig, MarketDataConnector, UserDataConfig, UserDataConnector};
use brokapi_tbank_protocol::TbankConfig;
use mock_server::{ACCOUNT_A, MockOptions, MockServer, TOKEN};
use tokio::sync::mpsc;
use tokio::time::timeout;

fn sber() -> InstrumentId {
    InstrumentId::new(
        InstrumentKey::new("SBER", "TQBR"),
        NativeId::Uid("uid-sber".into()),
    )
}

fn limiter() -> Arc<BrokerLimiter> {
    BrokerLimiter::new(&default_limits())
}

fn tb(server: &MockServer) -> TbankConfig {
    TbankConfig::for_mock(TOKEN, &server.endpoint).with_request_timeout(Duration::from_secs(5))
}

async fn md_with(
    cfg: TbankConfig,
    lim: Arc<BrokerLimiter>,
) -> (MarketDataConnector, mpsc::Receiver<Envelope>) {
    let (tx, rx) = mpsc::channel(512);
    let md = MarketDataConnector::start(MarketDataConfig::new(cfg, "tbank-md"), lim, tx)
        .await
        .expect("md");
    (md, rx)
}

async fn md(server: &MockServer) -> (MarketDataConnector, mpsc::Receiver<Envelope>) {
    md_with(tb(server), limiter()).await
}

async fn ud_cfg(cfg: UserDataConfig, cap: usize) -> (UserDataConnector, mpsc::Receiver<Envelope>) {
    let (tx, rx) = mpsc::channel(cap);
    let ud = UserDataConnector::start(cfg, limiter(), tx)
        .await
        .expect("ud");
    (ud, rx)
}

async fn ud(server: &MockServer) -> (UserDataConnector, mpsc::Receiver<Envelope>) {
    ud_cfg(
        UserDataConfig::new(tb(server).with_account(ACCOUNT_A), "tbank-ud"),
        512,
    )
    .await
}

fn limit_order(cid: ClientOrderId) -> PlaceOrderRequest {
    PlaceOrderRequest {
        cid,
        instrument: sber(),
        side: Side::Buy,
        order_type: OrderType::Limit,
        qty: Qty::from(2),
        price: Some(Price::from(250)),
    }
}

async fn next_order(
    rx: &mut mpsc::Receiver<Envelope>,
    pred: impl Fn(&OrderState) -> bool,
) -> brokapi_core::OrderSnapshot {
    timeout(Duration::from_secs(5), async {
        while let Some(env) = rx.recv().await {
            if let Event::Order(s) = env.payload
                && pred(&s.state)
            {
                return s;
            }
        }
        panic!("channel closed");
    })
    .await
    .expect("no matching order event")
}

// ---------- caps and config ----------

#[tokio::test]
async fn caps_are_declared() {
    let server = MockServer::start(Arc::default()).await;
    let (md, _rx) = md(&server).await;
    let caps = md.caps();
    assert_eq!(caps.max_order_book_depth, 50);
    assert!(!caps.incremental_order_book);
    assert!(caps.streaming.order_book && caps.streaming.market_trades);
    assert!(caps.streaming.quotes && caps.streaming.candles);

    let (ud, _rx) = ud(&server).await;
    let caps = ud.caps();
    assert_eq!(caps.place_idempotency, PlaceIdempotency::LabelOnly);
    assert!(!caps.ws_rpc && !caps.ordered_command_channel);
    assert!(caps.supports_replace && caps.supports_stop_orders);
    assert!(caps.streams_executions);
    assert!(caps.sandbox, "for_mock is a sandbox-style config");
    let prod = TbankConfig::production("t");
    assert!(!prod.sandbox && prod.endpoint.contains("//invest-public-api"));
    assert!(
        TbankConfig::sandbox("t")
            .endpoint
            .contains("sandbox-invest-public-api")
    );
}

#[tokio::test]
async fn every_call_carries_the_bearer_token() {
    let server = MockServer::start(Arc::default()).await;
    let (md, _rx) = md(&server).await;
    md.order_book(&sber(), 5).await.unwrap();
    let seen = server.opts.auth_seen.lock().unwrap().clone();
    assert!(!seen.is_empty() && seen.iter().all(|v| v == &format!("Bearer {TOKEN}")));
}

#[tokio::test]
async fn bad_token_is_a_config_error_not_a_retryable_one() {
    let server = MockServer::start(Arc::default()).await;
    let cfg = TbankConfig::for_mock("wrong", &server.endpoint);
    let (md, _rx) = md_with(cfg, limiter()).await;
    let err = md.order_book(&sber(), 5).await.unwrap_err();
    assert!(matches!(err, Error::Config(_)), "{err:?}");
}

#[tokio::test]
async fn token_with_several_accounts_needs_an_explicit_account() {
    let server = MockServer::start(Arc::default()).await;
    let (tx, _rx) = mpsc::channel(8);
    let cfg = UserDataConfig::new(tb(&server), "tbank-ud");
    let err = UserDataConnector::start(cfg, limiter(), tx)
        .await
        .err()
        .expect("must refuse");
    match err {
        Error::Config(e) => assert!(e.to_string().contains("2 accounts"), "{e}"),
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn token_never_appears_in_debug_output() {
    let cfg = TbankConfig::production("t.SECRET-TOKEN-VALUE");
    assert!(!format!("{cfg:?}").contains("SECRET"));
}

// ---------- trading ----------

#[tokio::test]
async fn commands_wait_for_reconcile() {
    let server = MockServer::start(Arc::default()).await;
    let (ud, _rx) = ud(&server).await;
    let cid = ClientOrderId::new_v4();
    assert!(matches!(
        ud.place(limit_order(cid)).await,
        Err(Error::NotReconciled)
    ));
    assert!(matches!(ud.cancel(cid).await, Err(Error::NotReconciled)));
    assert_eq!(
        server.opts.post_calls.load(Ordering::SeqCst),
        0,
        "refused without a network call"
    );
    ud.reconcile(&[]).await.unwrap();
    ud.place(limit_order(cid)).await.unwrap();
}

#[tokio::test]
async fn place_timeout_is_unknown_and_never_retried() {
    let server = MockServer::start(Arc::default()).await;
    let cfg = UserDataConfig::new(
        tb(&server)
            .with_account(ACCOUNT_A)
            .with_request_timeout(Duration::from_millis(150)),
        "tbank-ud",
    );
    let (ud, mut rx) = ud_cfg(cfg, 64).await;
    ud.reconcile(&[]).await.unwrap();
    ud.subscribe_order_state().await.unwrap();

    server.opts.post_delay_ms.store(600, Ordering::SeqCst);
    let cid = ClientOrderId::new_v4();
    // The request reaches the mock, which holds it for 600 ms. The clock goes 200 ms ahead: the 150 ms
    // timeout of the connector is due, the mock's delay is not.
    let err = brokapi_mocks::time::elapse_when(
        "the order reached the mock",
        || server.opts.post_calls.load(Ordering::SeqCst) == 1,
        Duration::from_millis(200),
        ud.place(limit_order(cid)),
    )
    .await
    .unwrap_err();
    assert!(matches!(err, Error::Unknown), "{err:?}");
    let snap = next_order(&mut rx, |s| matches!(s, OrderState::Unknown { .. })).await;
    assert_eq!(snap.cid, cid);

    // the same cid is never sent again
    let again = ud.place(limit_order(cid)).await.unwrap_err();
    assert!(
        matches!(&again, Error::Rejected { code, .. } if code == "duplicate_place"),
        "{again:?}"
    );

    // The mock's delay ends: the broker took the order after all. The stream shows it, Unknown resolves to Acked.
    brokapi_mocks::time::jump(Duration::from_millis(500)).await;
    let acked = next_order(&mut rx, |s| matches!(s, OrderState::Acked { .. })).await;
    assert_eq!(acked.cid, cid);

    // No retry came, neither at once nor later: let the clock run an hour ahead, then make a round trip
    // that ends behind anything a retry would have sent.
    brokapi_mocks::time::jump(Duration::from_secs(3600)).await;
    ud.reconcile(&[]).await.unwrap();
    assert_eq!(server.opts.post_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn broker_refusal_is_rejected_server_fault_is_unknown() {
    let server = MockServer::start(Arc::default()).await;
    let (ud, _rx) = ud(&server).await;
    ud.reconcile(&[]).await.unwrap();

    *server.opts.post_fail.lock().unwrap() = Some(tonic::Code::InvalidArgument);
    let e = ud
        .place(limit_order(ClientOrderId::new_v4()))
        .await
        .unwrap_err();
    assert!(matches!(e, Error::Rejected { .. }), "{e:?}");

    *server.opts.post_fail.lock().unwrap() = Some(tonic::Code::ResourceExhausted);
    let e = ud
        .place(limit_order(ClientOrderId::new_v4()))
        .await
        .unwrap_err();
    assert!(matches!(e, Error::RateLimited { .. }), "{e:?}");

    *server.opts.post_fail.lock().unwrap() = Some(tonic::Code::Internal);
    let e = ud
        .place(limit_order(ClientOrderId::new_v4()))
        .await
        .unwrap_err();
    assert!(matches!(e, Error::Unknown), "{e:?}");
}

#[tokio::test]
async fn order_input_is_checked_before_the_network() {
    let server = MockServer::start(Arc::default()).await;
    let (ud, _rx) = ud(&server).await;
    ud.reconcile(&[]).await.unwrap();
    let mut req = limit_order(ClientOrderId::new_v4());
    req.qty = Qty::new(dec!(1.5));
    assert!(matches!(
        ud.place(req.clone()).await,
        Err(Error::Rejected { .. })
    ));
    req.qty = Qty::from(1);
    req.price = None;
    assert!(
        matches!(ud.place(req).await, Err(Error::Rejected { code, .. }) if code == "missing_price")
    );
    assert_eq!(server.opts.post_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn a_price_off_the_step_is_refused_before_anything_is_sent() {
    let server = MockServer::start(Arc::default()).await;
    let (ud, _rx) = ud(&server).await;
    ud.reconcile(&[]).await.unwrap();
    // Without a known step nothing is checked. With the step 0.01 of the instrument it is.
    ud.price_grid()
        .set(sber().key, brokapi_core::Price::new(dec!(0.01)));
    let mut req = limit_order(ClientOrderId::new_v4());
    req.price = Some(brokapi_core::Price::new(dec!(250.005)));
    let err = ud.place(req.clone()).await.unwrap_err();
    assert!(
        matches!(&err, Error::Rejected { code, .. } if code == "price_off_step"),
        "{err:?}"
    );
    assert_eq!(
        server.opts.post_calls.load(Ordering::SeqCst),
        0,
        "nothing was sent"
    );
    // The cid was not spent: the same cid goes out with a price on the grid.
    req.price = Some(brokapi_core::Price::new(dec!(250.01)));
    ud.place(req).await.unwrap();
    assert_eq!(server.opts.post_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn order_state_events_follow_place_cancel_and_fill() {
    let server = MockServer::start(Arc::default()).await;
    let (ud, mut rx) = ud(&server).await;
    ud.reconcile(&[]).await.unwrap();
    let sub = ud.subscribe_order_state().await.unwrap();
    assert_eq!(
        ud.subscribe_order_state().await.unwrap(),
        sub,
        "repeat is the same subscription"
    );

    let (a, b) = (ClientOrderId::new_v4(), ClientOrderId::new_v4());
    ud.place(limit_order(a)).await.unwrap();
    let s = next_order(&mut rx, |s| matches!(s, OrderState::Acked { .. })).await;
    assert_eq!(s.cid, a);
    assert_eq!(s.price.unwrap().decimal(), dec!(250.0));
    ud.cancel(a).await.unwrap();
    assert_eq!(
        next_order(&mut rx, |s| matches!(s, OrderState::Cancelled { .. }))
            .await
            .cid,
        a
    );

    ud.place(limit_order(b)).await.unwrap();
    next_order(&mut rx, |s| matches!(s, OrderState::Acked { .. })).await;
    server.fill(ACCOUNT_A, &b.hyphenated().to_string());
    let filled = next_order(&mut rx, |s| matches!(s, OrderState::Filled { .. })).await;
    assert_eq!((filled.cid, filled.filled.decimal()), (b, dec!(2.0)));
}

#[tokio::test]
async fn replace_moves_the_order_to_the_new_cid() {
    let server = MockServer::start(Arc::default()).await;
    let (ud, mut rx) = ud(&server).await;
    ud.reconcile(&[]).await.unwrap();
    ud.subscribe_order_state().await.unwrap();
    let (old, new) = (ClientOrderId::new_v4(), ClientOrderId::new_v4());
    ud.place(limit_order(old)).await.unwrap();
    next_order(&mut rx, |s| matches!(s, OrderState::Acked { .. })).await;

    let ack = ud
        .replace(brokapi_core::ReplaceOrderRequest {
            cid: new,
            original_cid: old,
            qty: Some(Qty::from(5)),
            price: Some(Price::from(251)),
        })
        .await
        .unwrap();
    assert_eq!(ack.cid, new);
    let live = ud.active_orders().await.unwrap();
    assert_eq!(live.len(), 1);
    assert_eq!(
        (
            live[0].cid,
            live[0].qty.decimal(),
            live[0].price.unwrap().decimal()
        ),
        (new, dec!(5.0), dec!(251.0))
    );
    // the old cid is no longer live
    let st = ud.order_state(old).await.unwrap();
    assert!(matches!(st.state, OrderState::Cancelled { .. }));
    // and a replace key cannot be used twice
    let dup = ud
        .replace(brokapi_core::ReplaceOrderRequest {
            cid: new,
            original_cid: old,
            qty: None,
            price: None,
        })
        .await;
    assert!(matches!(dup, Err(Error::Rejected { .. })));
}

#[tokio::test]
async fn reconcile_sorts_confirmed_closed_and_foreign() {
    let server = MockServer::start(Arc::default()).await;
    let (ud, _rx) = ud(&server).await;
    ud.reconcile(&[]).await.unwrap();
    let (live, gone, ghost) = (
        ClientOrderId::new_v4(),
        ClientOrderId::new_v4(),
        ClientOrderId::new_v4(),
    );
    ud.place(limit_order(live)).await.unwrap();
    ud.place(limit_order(gone)).await.unwrap();
    ud.cancel(gone).await.unwrap();

    // an order from outside: the mock holds it for our account but nobody told us its cid
    let (other, _rx2) = ud_cfg(
        UserDataConfig::new(tb(&server).with_account(ACCOUNT_A), "other"),
        8,
    )
    .await;
    other.reconcile(&[]).await.unwrap();
    let foreign = ClientOrderId::new_v4();
    other.place(limit_order(foreign)).await.unwrap();

    let report = ud.reconcile(&[live, gone, ghost]).await.unwrap();
    assert_eq!(
        report.confirmed.iter().map(|o| o.cid).collect::<Vec<_>>(),
        vec![live]
    );
    let mut closed = report.closed_offline.clone();
    closed.sort();
    let mut want = vec![gone, ghost];
    want.sort();
    assert_eq!(closed, want);
    assert_eq!(
        report.foreign.iter().map(|o| o.cid).collect::<Vec<_>>(),
        vec![foreign]
    );
}

#[tokio::test]
async fn own_trades_stream_and_history() {
    let server = MockServer::start(Arc::default()).await;
    let (ud, mut rx) = ud(&server).await;
    ud.reconcile(&[]).await.unwrap();
    ud.subscribe_order_state().await.unwrap();
    let sub = ud.subscribe_executions().await.unwrap();
    let cid = ClientOrderId::new_v4();
    ud.place(limit_order(cid)).await.unwrap();
    next_order(&mut rx, |s| matches!(s, OrderState::Acked { .. })).await;
    server.fill(ACCOUNT_A, &cid.hyphenated().to_string());

    let exec = timeout(Duration::from_secs(5), async {
        while let Some(env) = rx.recv().await {
            if let Event::Execution(x) = env.payload {
                assert_eq!(env.sub, Some(sub));
                return x;
            }
        }
        panic!("closed");
    })
    .await
    .unwrap();
    assert_eq!(exec.cid, Some(cid));
    assert_eq!(
        (exec.side, exec.qty.decimal(), exec.price.decimal()),
        (Side::Buy, dec!(2.0), dec!(250.0))
    );
    assert_eq!(exec.instrument.key, InstrumentKey::new("SBER", "TQBR"));

    let hist = ud
        .executions(TimeRange::new(
            UnixNanos::from_nanos(0),
            UnixNanos::from_nanos(i64::MAX / 2),
        ))
        .await
        .unwrap();
    assert_eq!(hist.len(), 1);
    assert_eq!(
        (hist[0].qty.decimal(), hist[0].commission.decimal()),
        (dec!(10.0), dec!(1.0))
    );
}

#[tokio::test]
async fn portfolio_reads() {
    let server = MockServer::start(Arc::default()).await;
    let (ud, _rx) = ud(&server).await;
    let p = ud.portfolio().await.unwrap();
    assert_eq!(
        p.positions.len(),
        1,
        "the currency line is cash, not a position"
    );
    assert_eq!(p.positions[0].qty.decimal(), dec!(30.0));
    assert_eq!(
        p.positions[0].instrument.key,
        InstrumentKey::new("SBER", "TQBR")
    );
    let rub = &p.balances[0];
    assert_eq!(
        (
            rub.currency.as_str(),
            rub.available.decimal(),
            rub.blocked.decimal()
        ),
        ("RUB", dec!(100_000.0), dec!(150.0))
    );
    assert_eq!(ud.balances().await.unwrap().len(), 1);
    assert_eq!(ud.positions().await.unwrap().len(), 1);
}

#[tokio::test]
async fn overflow_of_a_lossless_subscription_degrades_until_reconcile() {
    let server = MockServer::start(Arc::default()).await;
    let mut cfg = UserDataConfig::new(tb(&server).with_account(ACCOUNT_A), "tbank-ud");
    cfg.order_state_buffer = 2;
    // a consumer that does not read: one slot in the channel
    let (ud, mut rx) = ud_cfg(cfg, 1).await;
    ud.reconcile(&[]).await.unwrap();
    ud.subscribe_order_state().await.unwrap();

    let mut refused = None;
    for _ in 0..30 {
        match ud.place(limit_order(ClientOrderId::new_v4())).await {
            Ok(_) => {}
            Err(e) => {
                refused = Some(e);
                break;
            }
        }
    }
    assert!(matches!(refused, Some(Error::Degraded)), "{refused:?}");

    // the consumer wakes up: it learns about the overflow explicitly
    let mut saw_overflow = false;
    timeout(Duration::from_secs(5), async {
        while let Some(env) = rx.recv().await {
            if matches!(env.payload, Event::Overflow { .. }) {
                saw_overflow = true;
                break;
            }
        }
    })
    .await
    .unwrap();
    assert!(saw_overflow);
    // trading resumes only through reconcile
    assert!(matches!(
        ud.place(limit_order(ClientOrderId::new_v4())).await,
        Err(Error::Degraded)
    ));
    while rx.try_recv().is_ok() {}
    ud.reconcile(&[]).await.unwrap();
    ud.place(limit_order(ClientOrderId::new_v4()))
        .await
        .unwrap();
}

// ---------- market data ----------

#[tokio::test]
async fn reference_and_history_calls() {
    let server = MockServer::start(Arc::default()).await;
    let (md, _rx) = md(&server).await;
    let id = sber();

    let trades = md.last_trades(&id, 2).await.unwrap();
    assert_eq!(trades.len(), 2, "the newest two");
    assert_eq!((trades[0].side, trades[1].side), (Side::Sell, Side::Buy));

    let c = md
        .candles(
            &id,
            Timeframe::M5,
            TimeRange::new(
                UnixNanos::from_nanos(1_700_000_000_000_000_000),
                UnixNanos::from_nanos(1_700_001_000_000_000_000),
            ),
        )
        .await
        .unwrap();
    assert_eq!(c.len(), 2);
    assert_eq!(
        c[1].close_ts.as_nanos() - c[1].open_ts.as_nanos(),
        300_000_000_000
    );

    let st = md.trading_status(&id).await.unwrap();
    assert_eq!(st.availability, brokapi_core::TradingAvailability::Open);
    let params = md.trading_params(&id).await.unwrap();
    assert_eq!(
        (
            params.lot_size.decimal(),
            params.price_step.decimal(),
            params.min_qty.decimal()
        ),
        (dec!(10.0), dec!(0.01), dec!(1.0))
    );
    let got = md.get(&id).await.unwrap();
    assert_eq!(
        (got.currency.as_str(), got.id.key.display()),
        ("rub", "SBER@TQBR".to_string())
    );
    let sched = md
        .schedule(
            "MOEX",
            TimeRange::new(UnixNanos::ZERO, UnixNanos::from_nanos(i64::MAX / 2)),
        )
        .await
        .unwrap();
    assert_eq!(sched.sessions.len(), 1);

    // by ticker and class, without the uid
    let by_ticker = InstrumentId::new(
        InstrumentKey::new("SBER", "TQBR"),
        NativeId::TickerClass {
            ticker: "SBER".into(),
            class_code: "TQBR".into(),
        },
    );
    assert_eq!(md.order_book(&by_ticker, 3).await.unwrap().clone_len(), 3);
    let found = md
        .find(InstrumentQuery::ByIsin("RU0009029540".into()))
        .await
        .unwrap();
    assert!(found.iter().any(|i| i.id.key.symbol == "SBER"));
}

trait BookLen {
    fn clone_len(&self) -> usize;
}
impl BookLen for brokapi_core::OrderBook {
    fn clone_len(&self) -> usize {
        match self {
            brokapi_core::OrderBook::Snapshot { bids, .. }
            | brokapi_core::OrderBook::Increment { bids, .. } => bids.len(),
        }
    }
}

/// One connector per call: the listing bucket (15 a minute, burst 3) is shared by all six methods.
async fn list_kinds(cases: &[(brokapi_core::InstrumentKind, &str, &str)], opts: &Arc<MockOptions>) {
    let server = MockServer::start(Arc::clone(opts)).await;
    let (md, _rx) = md(&server).await;
    for (kind, method, ticker) in cases {
        let found = md.find(InstrumentQuery::ByKind(*kind)).await.unwrap();
        assert!(!found.is_empty(), "{method}: empty");
        assert!(
            found.iter().all(|i| i.kind == *kind),
            "{method}: wrong kind"
        );
        assert!(
            found.iter().any(|i| i.id.key.symbol == *ticker),
            "{method}: {ticker} missing"
        );
        assert!(
            opts.list_calls
                .lock()
                .unwrap()
                .iter()
                .any(|(m, _)| m == method)
        );
        if *method == "Bonds" {
            assert_eq!(found[0].isin.as_deref(), Some("RU000A1038V6"));
        }
        if *method == "Futures" {
            assert_eq!(found[0].isin, None);
        }
    }
}

#[tokio::test]
async fn find_by_kind_calls_the_listing_of_that_kind() {
    use brokapi_core::InstrumentKind as K;
    let opts: Arc<MockOptions> = Arc::default();
    list_kinds(
        &[
            (K::Stock, "Shares", "SBER"),
            (K::Bond, "Bonds", "SU26238RMFS4"),
            (K::Etf, "Etfs", "TMOS"),
        ],
        &opts,
    )
    .await;
    list_kinds(
        &[
            (K::Future, "Futures", "SRZ6"),
            (K::Option, "Options", "SR300BL6"),
            (K::Currency, "Currencies", "CNYRUBF"),
        ],
        &opts,
    )
    .await;
    // The base list (what the API can trade) is requested every time.
    assert_eq!(opts.list_calls.lock().unwrap().len(), 6);
    assert!(
        opts.list_calls
            .lock()
            .unwrap()
            .iter()
            .all(|(_, st)| *st == 1)
    );
}

#[tokio::test]
async fn listings_have_their_own_rate_limit_bucket() {
    use brokapi_core::InstrumentKind as K;
    let opts: Arc<MockOptions> = Arc::default();
    let server = MockServer::start(Arc::clone(&opts)).await;
    let (md, _rx) = md(&server).await;
    for _ in 0..3 {
        md.find(InstrumentQuery::ByKind(K::Stock)).await.unwrap();
    }
    // 15 a minute, burst 3: the fourth is refused by the limiter, the network is not touched.
    let e = md.find(InstrumentQuery::ByKind(K::Bond)).await.unwrap_err();
    assert!(matches!(e, Error::RateLimited { .. }), "{e:?}");
    assert_eq!(opts.list_calls.lock().unwrap().len(), 3);
    // Other buckets are not starved by the listings.
    md.find(InstrumentQuery::ByTicker {
        ticker: "SBER".into(),
        exchange: None,
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn find_by_kind_without_a_listing_is_unsupported_without_the_network() {
    use brokapi_core::InstrumentKind as K;
    let opts: Arc<MockOptions> = Arc::default();
    let server = MockServer::start(Arc::clone(&opts)).await;
    let (md, _rx) = md(&server).await;
    for kind in [K::MutualFund, K::Other] {
        let e = md.find(InstrumentQuery::ByKind(kind)).await.unwrap_err();
        assert!(
            matches!(e, Error::Unsupported { capability } if capability.starts_with("find_by_kind"))
        );
    }
    assert!(opts.list_calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn quotes_and_book_of_one_instrument_use_one_wire_subscription() {
    let server = MockServer::start(Arc::default()).await;
    let (md, mut rx) = md(&server).await;
    let q = md
        .subscribe_quotes(&sber(), SubscribeOpts::conflate())
        .await
        .unwrap();
    let b = md
        .subscribe_order_book(
            &sber(),
            SubscribeOpts {
                backpressure: Backpressure::Conflate,
                depth: Some(20),
            },
        )
        .await
        .unwrap();

    let reqs = server.opts.md_requests.lock().unwrap().clone();
    let subs: Vec<_> = reqs.iter().filter(|r| r.0 == 1).map(|r| r.2).collect();
    assert_eq!(
        subs,
        vec![1, 20],
        "depth 1 first, then replaced by depth 20"
    );
    assert!(
        reqs.iter().any(|r| r.0 == 2 && r.2 == 1),
        "depth 1 was dropped"
    );

    let (mut got_quote, mut got_book) = (false, false);
    timeout(Duration::from_secs(5), async {
        while let Some(env) = rx.recv().await {
            match env.payload {
                Event::Quote(q2) if env.sub == Some(q) => {
                    assert!(q2.bid.unwrap().price.decimal() < q2.ask.unwrap().price.decimal());
                    got_quote = true;
                }
                Event::OrderBook(_) if env.sub == Some(b) => got_book = true,
                _ => {}
            }
            if got_quote && got_book {
                break;
            }
        }
    })
    .await
    .unwrap();

    // unsubscribing the book keeps quotes alive: depth goes back to 1
    md.unsubscribe(b).await.unwrap();
    brokapi_mocks::time::wait_until("the book subscription is back to depth 1", || {
        let reqs = server.opts.md_requests.lock().unwrap();
        reqs.last().map(|r| (r.0, r.2)) == Some((1, 1))
    })
    .await;
    md.unsubscribe(q).await.unwrap();
}

#[tokio::test]
async fn trades_and_candles_stream() {
    let server = MockServer::start(Arc::default()).await;
    let (md, mut rx) = md(&server).await;
    let t = md
        .subscribe_market_trades(&sber(), SubscribeOpts::lossless(64))
        .await
        .unwrap();
    let c = md
        .subscribe_candles(&sber(), Timeframe::M5, SubscribeOpts::conflate())
        .await
        .unwrap();
    let (mut tr, mut ca) = (false, false);
    timeout(Duration::from_secs(5), async {
        while let Some(env) = rx.recv().await {
            match &env.payload {
                Event::MarketTrade(m) if env.sub == Some(t) => {
                    assert_eq!(
                        (m.qty.decimal(), m.price.decimal()),
                        (dec!(7.0), dec!(274.0))
                    );
                    tr = true;
                }
                Event::Candle(k) if env.sub == Some(c) => {
                    assert_eq!(k.timeframe, Timeframe::M5);
                    ca = true;
                }
                _ => {}
            }
            if tr && ca {
                break;
            }
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn refused_subscription_is_an_error_not_a_silent_hole() {
    let server = MockServer::start(Arc::default()).await;
    *server.opts.md_refuse_ticker.lock().unwrap() = Some("SBER".into());
    let (md, _rx) = md(&server).await;
    let e = md
        .subscribe_order_book(&sber(), SubscribeOpts::conflate())
        .await
        .unwrap_err();
    match e {
        Error::Rejected { code, .. } => {
            assert_eq!(code, "SUBSCRIPTION_STATUS_INSTRUMENT_NOT_FOUND")
        }
        other => panic!("{other:?}"),
    }
    // and nothing is left on the wire for it
    let e2 = md.unsubscribe(brokapi_core::SubscriptionId::new(1)).await;
    assert!(e2.is_err());
}

#[tokio::test]
async fn unconfirmed_subscription_times_out() {
    let server = MockServer::start(Arc::default()).await;
    server.opts.md_silent.store(true, Ordering::SeqCst);
    let (md, _rx) = md_with(
        tb(&server).with_request_timeout(Duration::from_millis(200)),
        limiter(),
    )
    .await;
    // The mock reads the request and stays silent: the clock goes past the 200 ms timeout at once.
    let e = brokapi_mocks::time::elapse_when(
        "the subscribe request reached the mock",
        || !server.opts.md_requests.lock().unwrap().is_empty(),
        Duration::from_millis(300),
        md.subscribe_order_book(&sber(), SubscribeOpts::conflate()),
    )
    .await
    .unwrap_err();
    assert!(matches!(e, Error::Transport(_)), "{e:?}");
}

#[tokio::test]
async fn broken_stream_is_reopened_and_subscriptions_restored() {
    let server = MockServer::start(Arc::default()).await;
    let (md, mut rx) = md(&server).await;
    // the mock cuts the stream right after the first subscription is served
    server.opts.md_cut_after.store(1, Ordering::SeqCst);
    let sub = md
        .subscribe_order_book(&sber(), SubscribeOpts::conflate())
        .await
        .unwrap();

    let (mut resync, mut data_after) = (false, false);
    timeout(Duration::from_secs(10), async {
        while let Some(env) = rx.recv().await {
            match env.payload {
                Event::Resync {
                    reason: ResyncReason::SubscriptionReset,
                    sub: s,
                    ..
                } => {
                    assert_eq!(s, sub);
                    resync = true;
                }
                Event::OrderBook(_) if resync => {
                    data_after = true;
                    break;
                }
                _ => {}
            }
        }
    })
    .await
    .expect("stream did not come back");
    assert!(resync && data_after);
    assert!(server.opts.md_connections.load(Ordering::SeqCst) >= 2);
}

// ---------- shutdown and limits ----------

#[tokio::test]
async fn graceful_md_shutdown_unsubscribes_by_protocol() {
    let server = MockServer::start(Arc::default()).await;
    let (md, _rx) = md(&server).await;
    md.subscribe_order_book(&sber(), SubscribeOpts::conflate())
        .await
        .unwrap();
    let report = md
        .shutdown(ShutdownMode::Graceful {
            deadline: Duration::from_secs(3),
        })
        .await;
    assert_eq!((report.unsubscribed, report.unsubscribe_failed), (1, 0));
    let reqs = server.opts.md_requests.lock().unwrap().clone();
    assert!(
        reqs.iter().any(|r| r.0 == 2),
        "UNSUBSCRIBE went to the broker: {reqs:?}"
    );
}

#[tokio::test]
async fn immediate_shutdown_does_not_touch_subscriptions() {
    let server = MockServer::start(Arc::default()).await;
    let (md, _rx) = md(&server).await;
    md.subscribe_order_book(&sber(), SubscribeOpts::conflate())
        .await
        .unwrap();
    let report = md.shutdown(ShutdownMode::Immediate).await;
    assert_eq!(report.unsubscribed, 0);
    assert!(
        !server
            .opts
            .md_requests
            .lock()
            .unwrap()
            .iter()
            .any(|r| r.0 == 2)
    );
}

#[tokio::test]
async fn ud_shutdown_closes_streams_and_leaves_exchange_orders() {
    let server = MockServer::start(Arc::default()).await;
    let (ud, _rx) = ud(&server).await;
    ud.reconcile(&[]).await.unwrap();
    ud.subscribe_order_state().await.unwrap();
    ud.subscribe_executions().await.unwrap();
    ud.place(limit_order(ClientOrderId::new_v4()))
        .await
        .unwrap();
    let report = ud
        .shutdown(ShutdownMode::Graceful {
            deadline: Duration::from_secs(2),
        })
        .await;
    assert_eq!(report.unsubscribed, 2);
    assert!(
        report.inflight_abandoned.is_empty(),
        "the placement was answered"
    );
    // the order is still at the broker
    assert_eq!(server.orders_of(ACCOUNT_A).len(), 1);
}

#[tokio::test]
async fn one_limiter_for_both_connectors_of_a_user() {
    let server = MockServer::start(Arc::default()).await;
    let toml = r#"
        [tbank.buckets.global]
        rate = 1
        per = "60s"
        burst = 2
        [tbank.route]
        GetOrderBook = ["global"]
        GetPositions = ["global"]
        [tbank.conns]
        max_conns = 4
        max_subs_per_conn = 50
        supports_multiplex = true
    "#;
    let shared = BrokerLimiter::new(&limits_from_toml_str(toml).unwrap());
    let (md, _r1) = md_with(tb(&server), Arc::clone(&shared)).await;
    let (tx, _r2) = mpsc::channel(8);
    let ud = UserDataConnector::start(
        UserDataConfig::new(tb(&server).with_account(ACCOUNT_A), "tbank-ud"),
        shared,
        tx,
    )
    .await
    .unwrap();
    md.order_book(&sber(), 1).await.unwrap();
    ud.balances().await.unwrap();
    // the third call, from either connector, is over the shared quota
    assert!(matches!(
        md.order_book(&sber(), 1).await,
        Err(Error::RateLimited { .. })
    ));
    assert!(matches!(
        ud.balances().await,
        Err(Error::RateLimited { .. })
    ));
}

/// Input, then quota, then the cid is marked: a `place` refused for `RateLimited` sent nothing, so the same
/// cid is used again (the rule is the same for BCS and Finam).
#[tokio::test]
async fn rate_limited_place_leaves_the_cid_usable() {
    let server = MockServer::start(Arc::default()).await;
    let toml = r#"
        [tbank.buckets.big]
        rate = 10000
        per = "1s"
        burst = 10000
        [tbank.buckets.one_order]
        rate = 1
        per = "400ms"
        burst = 1
        [tbank.route]
        "*" = ["big"]
        PostOrder = ["one_order"]
        [tbank.conns]
        max_conns = 4
        max_subs_per_conn = 50
        supports_multiplex = true
    "#;
    let (tx, _rx) = mpsc::channel(64);
    let ud = UserDataConnector::start(
        UserDataConfig::new(tb(&server).with_account(ACCOUNT_A), "tbank-ud"),
        BrokerLimiter::new(&limits_from_toml_str(toml).unwrap()),
        tx,
    )
    .await
    .unwrap();
    ud.reconcile(&[]).await.unwrap();
    ud.place(limit_order(ClientOrderId::new_v4()))
        .await
        .unwrap();

    let cid = ClientOrderId::new_v4();
    let retry_after = match ud.place(limit_order(cid)).await {
        Err(Error::RateLimited { retry_after }) => retry_after,
        other => panic!("expected RateLimited, got {other:?}"),
    };
    brokapi_mocks::time::jump(retry_after + Duration::from_millis(50)).await;
    ud.place(limit_order(cid))
        .await
        .expect("the same cid after RateLimited");
    brokapi_mocks::time::jump(Duration::from_millis(450)).await;
    assert!(matches!(
        ud.place(limit_order(cid)).await,
        Err(Error::Rejected { ref code, .. }) if code == "duplicate_place"
    ));
}

// ---------- history that does not fit the page limit ----------

#[tokio::test]
async fn a_history_over_the_page_limit_is_an_error_not_a_cut_off_list() {
    let range = TimeRange::new(
        UnixNanos::from_nanos(0),
        UnixNanos::from_nanos(i64::MAX / 2),
    );
    let opts: Arc<MockOptions> = Arc::default();
    let server = MockServer::start(Arc::clone(&opts)).await;
    // The quota for operations is small (a page costs one request): a roomy one for 41 pages.
    let roomy = r#"
        [tbank.buckets.big]
        rate = 10000
        per = "1s"
        burst = 10000
        [tbank.route]
        "*" = ["big"]
        [tbank.conns]
        max_conns = 4
        max_subs_per_conn = 50
        supports_multiplex = true
    "#;
    let (tx, _rx) = mpsc::channel(64);
    let ud = UserDataConnector::start(
        UserDataConfig::new(tb(&server).with_account(ACCOUNT_A), "tbank-ud"),
        BrokerLimiter::new(&limits_from_toml_str(roomy).unwrap()),
        tx,
    )
    .await
    .unwrap();

    // Exactly 20 pages fit: all of them come back.
    opts.operations_pages.store(20, Ordering::SeqCst);
    assert_eq!(ud.executions(range).await.unwrap().len(), 20);

    // A 21st page exists: the call says so, it does not return the first 20 as if they were all.
    opts.operations_pages.store(21, Ordering::SeqCst);
    match ud.executions(range).await {
        Err(Error::Rejected { code, .. }) => assert_eq!(code, "range_too_long"),
        other => panic!("expected range_too_long, got {other:?}"),
    }
}

// ---------- ping period of the streams ----------

#[tokio::test]
async fn a_short_ping_period_does_not_make_a_healthy_stream_look_dead() {
    let server = MockServer::start(Arc::default()).await;
    server.opts.book_burst.store(1, Ordering::SeqCst); // one snapshot, then the stream is quiet
    let mut cfg = MarketDataConfig::new(tb(&server), "tbank-md");
    // The broker accepts 5 s at the least, so 1 s is asked as 5 s and the silence limit is 15 s.
    cfg.ping_interval = Duration::from_secs(1);
    let (tx, mut rx) = mpsc::channel(512);
    let md = MarketDataConnector::start(cfg, limiter(), tx)
        .await
        .unwrap();
    md.subscribe_order_book(&sber(), SubscribeOpts::conflate())
        .await
        .unwrap();
    next_event(&mut rx, |e| matches!(e, Event::OrderBook(_))).await;
    assert_eq!(server.opts.md_connections.load(Ordering::SeqCst), 1);

    // 10 s of silence is more than the 3 s that the unclamped period would allow, and less than 15 s.
    brokapi_mocks::time::jump(Duration::from_secs(10)).await;
    // A new subscription needs a live stream. If the stream had been dropped, it would be confirmed only
    // after a reopening, and the mock would have counted a second connection by then.
    md.subscribe_market_trades(&sber(), SubscribeOpts::lossless(64))
        .await
        .unwrap();
    assert_eq!(
        server.opts.md_connections.load(Ordering::SeqCst),
        1,
        "the stream was taken for dead too early"
    );

    // And a stream that stays silent for more than the limit is reopened: the check is alive.
    brokapi_mocks::time::jump(Duration::from_secs(20)).await;
    brokapi_mocks::time::wait_until("the silent stream is reopened", || {
        server.opts.md_connections.load(Ordering::SeqCst) >= 2
    })
    .await;
}

async fn next_event(rx: &mut mpsc::Receiver<Envelope>, pred: impl Fn(&Event) -> bool) -> Envelope {
    timeout(Duration::from_secs(5), async {
        while let Some(env) = rx.recv().await {
            if pred(&env.payload) {
                return env;
            }
        }
        panic!("channel closed");
    })
    .await
    .expect("no matching event")
}
