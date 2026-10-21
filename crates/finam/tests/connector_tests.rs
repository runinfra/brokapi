//! Level-2: connectors against the gRPC mock (no external network).

mod mock_server;

use brokapi_core::Money;
use brokapi_core::{Decimal, dec};
use std::sync::Arc;
use std::time::Duration;

use brokapi_conformance::next_matching;
use brokapi_core::{
    ClientOrderId, ConfigError, Envelope, Error, Event, ExecutionSource, InstrumentKind,
    InstrumentQuery, InstrumentSource, MarketDataSource, OrderBook, OrderBookSource, OrderExecutor,
    OrderState, OrderType, PlaceIdempotency, PlaceOrderRequest, PortfolioSource, Price, Qty,
    ResyncReason, ShutdownMode, Side, SubscribeOpts, TimeRange, Timeframe, TradingAvailability,
    UnixNanos,
};
use brokapi_finam::limits::{default_limits, parse_limits};
use brokapi_finam::{
    FinamApiConfig, FinamCidCodec, MarketDataConfig, MarketDataConnector, SubStatus,
    UserDataConfig, UserDataConnector,
};
use brokapi_finam_protocol::pb::grpc::tradeapi::v1;
use brokapi_finam_protocol::pb::grpc::tradeapi::v1::orders as po;
use brokapi_runtime::BrokerLimiter;
use mock_server::{MockServer, SECRET, api, gazp, limiter, md, sber, ud, ud_with};
use tokio::sync::mpsc;
use tonic::Code;

fn limit_buy(price: Decimal) -> PlaceOrderRequest {
    PlaceOrderRequest {
        cid: ClientOrderId::new_v4(),
        instrument: sber(),
        side: Side::Buy,
        order_type: OrderType::Limit,
        qty: Qty::from(1),
        price: Some(Price::new(price)),
    }
}

async fn reconciled(
    server: &MockServer,
    account: &str,
) -> (UserDataConnector, mpsc::Receiver<Envelope>) {
    let (ud, rx) = ud(server, account).await;
    ud.reconcile(&[]).await.expect("reconcile");
    (ud, rx)
}

// ------------------------------------------------------------------ caps and config

#[tokio::test]
async fn caps_state_what_finam_can_do() {
    let server = MockServer::start().await;
    let (md, _) = md(&server).await;
    let c = md.caps();
    assert!(!c.incremental_order_book, "books are folded into snapshots");
    assert!(
        c.streaming.order_book
            && c.streaming.market_trades
            && c.streaming.quotes
            && c.streaming.candles
    );
    assert!(c.max_order_book_depth > 0);

    let (ud, _) = ud(&server, "ACC1").await;
    let u = ud.caps();
    // Spec 7.3 / 15: not proven live yet.
    assert_eq!(u.place_idempotency, PlaceIdempotency::LabelOnly);
    assert!(!u.supports_replace && !u.sandbox && !u.ws_rpc && !u.ordered_command_channel);
    assert!(u.streams_executions);
    assert!(u.supports_stop_orders);
}

#[tokio::test]
async fn bad_secret_fails_at_start() {
    let server = MockServer::start().await;
    let (tx, _rx) = mpsc::channel(8);
    let cfg = MarketDataConfig::new(
        FinamApiConfig::new("wrong").with_endpoint(server.url.clone()),
        "finam-md",
    );
    let err = MarketDataConnector::start(cfg, limiter(), tx)
        .await
        .err()
        .expect("must fail");
    assert!(matches!(err, Error::Config(_)), "{err:?}");
}

#[tokio::test]
async fn account_outside_the_token_is_a_config_error() {
    let server = MockServer::start().await;
    let (tx, _rx) = mpsc::channel(8);
    let cfg = UserDataConfig::new(api(&server), "ACC9", "x");
    let err = UserDataConnector::start(cfg, limiter(), tx)
        .await
        .err()
        .expect("must fail");
    assert!(
        matches!(err, Error::Config(ConfigError::Message(ref m)) if m.contains("ACC9")),
        "{err:?}"
    );
    let cfg = UserDataConfig::new(api(&server), " ", "x");
    let (tx, _rx) = mpsc::channel(8);
    assert!(matches!(
        UserDataConnector::start(cfg, limiter(), tx).await.err(),
        Some(Error::Config(_))
    ));
}

#[test]
fn bundled_limits_cover_every_method_the_connectors_call() {
    let limits = default_limits();
    for m in [
        "PlaceOrder",
        "CancelOrder",
        "GetOrder",
        "GetOrders",
        "GetAccount",
        "Trades",
        "GetAsset",
        "Assets",
        "Schedule",
        "OrderBook",
        "LatestTrades",
        "Bars",
        "SubscribeOrderBook",
        "SubscribeQuote",
        "SubscribeLatestTrades",
        "SubscribeBars",
        "SubscribeOrders",
        "SubscribeTrades",
        "SubscribeAccount",
    ] {
        assert!(
            limits.route.contains_key(m),
            "{m} is not routed to any bucket"
        );
    }
    // Cancel has its own bucket, separate from placing.
    assert!(limits.route["CancelOrder"].contains(&"cancel".to_string()));
    assert!(!limits.route["CancelOrder"].contains(&"orders".to_string()));
    // Documented quota: 200 requests per minute.
    assert_eq!(limits.buckets["global"].rate, 200);
    assert_eq!(limits.buckets["global"].per, Duration::from_secs(60));
    assert!(
        parse_limits("[bcs.buckets.global]\nrate=1").is_err(),
        "[finam] section is required"
    );
}

#[tokio::test]
async fn non_finam_instrument_id_is_refused_without_network() {
    let server = MockServer::start().await;
    let (md, _) = md(&server).await;
    let bad = brokapi_core::InstrumentId::new(
        brokapi_core::InstrumentKey::new("SBER", "TQBR"),
        brokapi_core::NativeId::TickerClass {
            ticker: "SBER".into(),
            class_code: "TQBR".into(),
        },
    );
    assert!(matches!(
        md.order_book(&bad, 5).await,
        Err(Error::Config(_))
    ));
    assert!(matches!(
        md.subscribe_quotes(&bad, SubscribeOpts::conflate()).await,
        Err(Error::Config(_))
    ));
    assert!(server.state.calls().is_empty());
}

// ------------------------------------------------------------------------ tokens

#[tokio::test]
async fn every_call_carries_the_current_jwt_and_it_is_renewed() {
    let server = MockServer::start().await;
    let tokens = api(&server).shared_tokens().await.unwrap();
    let (tx, _rx) = mpsc::channel(8);
    let md = MarketDataConnector::start(
        MarketDataConfig::new(api(&server).with_tokens(Arc::clone(&tokens)), "finam-md"),
        limiter(),
        tx,
    )
    .await
    .unwrap();
    md.order_book(&sber(), 5).await.unwrap();
    assert_eq!(server.state.tokens_seen().last().unwrap(), "jwt-1");
    tokens.refresh().await.unwrap();
    md.order_book(&sber(), 5).await.unwrap();
    assert_eq!(server.state.tokens_seen().last().unwrap(), "jwt-2");
    // The connector did not authenticate by itself: it used the shared manager.
    assert_eq!(
        server
            .state
            .auth_calls
            .load(std::sync::atomic::Ordering::SeqCst),
        2
    );
}

#[tokio::test]
async fn read_survives_an_expired_token_by_renewing_and_retrying_once() {
    let server = MockServer::start().await;
    let (md, _rx) = md(&server).await;
    server
        .state
        .options
        .unauth_next
        .store(1, std::sync::atomic::Ordering::SeqCst);
    md.order_book(&sber(), 5)
        .await
        .expect("retried after re-auth");
    assert_eq!(
        server
            .state
            .auth_calls
            .load(std::sync::atomic::Ordering::SeqCst),
        2
    );
}

#[tokio::test]
async fn commands_are_never_retried_after_unauthenticated() {
    let server = MockServer::start().await;
    let (ud, _rx) = reconciled(&server, "ACC1").await;
    server
        .state
        .options
        .unauth_next
        .store(1, std::sync::atomic::Ordering::SeqCst);
    let err = ud.place(limit_buy(dec!(1.0))).await.unwrap_err();
    assert!(matches!(err, Error::Config(_)), "{err:?}");
    assert_eq!(server.state.count_calls("PlaceOrder"), 0);
    assert!(
        server.state.orders_of("ACC1").is_empty(),
        "no hidden second attempt"
    );
}

// ------------------------------------------------------------- several accounts

#[tokio::test]
async fn two_accounts_share_one_jwt_and_see_only_their_own_data() {
    let server = MockServer::start().await;
    let tokens = api(&server).shared_tokens().await.unwrap();
    let shared_limiter = limiter();
    let mk = |acc: &'static str| {
        let cfg = UserDataConfig::new(
            api(&server).with_tokens(Arc::clone(&tokens)),
            acc,
            format!("finam-ud-{acc}"),
        );
        ud_with(cfg, Arc::clone(&shared_limiter), 256)
    };
    let (ud1, mut rx1) = mk("ACC1").await;
    let (ud2, mut rx2) = mk("ACC2").await;
    // One secret, one Auth, however many accounts.
    assert_eq!(
        server
            .state
            .auth_calls
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    assert_eq!(tokens.account_ids(), vec!["ACC1", "ACC2"]);
    ud1.reconcile(&[]).await.unwrap();
    ud2.reconcile(&[]).await.unwrap();
    assert_eq!(ud1.account_id(), "ACC1");

    let sub1 = ud1.subscribe_order_state().await.unwrap();
    let _sub2 = ud2.subscribe_order_state().await.unwrap();

    let req = limit_buy(dec!(1.0));
    let cid = req.cid;
    ud1.place(req).await.unwrap();

    // Orders: only ACC1 has one.
    assert_eq!(ud1.active_orders().await.unwrap().len(), 1);
    assert!(ud2.active_orders().await.unwrap().is_empty());
    assert!(
        matches!(ud2.order_state(cid).await, Err(Error::Rejected { ref code, .. }) if code == "not_found")
    );
    assert!(
        matches!(ud2.cancel(cid).await, Err(Error::Rejected { ref code, .. }) if code == "not_found")
    );
    assert_eq!(server.state.orders_of("ACC2").len(), 0);

    // Stream: ACC1's subscription gets the order, ACC2's gets nothing.
    let env = next_matching("acc1 stream", &mut rx1, |e| {
        matches!(e.payload, Event::Order(_))
    })
    .await;
    assert_eq!(env.sub, Some(sub1));
    assert_eq!(env.source.as_ref(), "finam-ud-ACC1");
    // ACC2 must not see ACC1's order. ACC2's own order is the fence: ACC1's was pushed first, so if it
    // had leaked into this channel it would be here before ACC2's own.
    let own = limit_buy(dec!(2.0));
    let own_cid = own.cid;
    ud2.place(own).await.unwrap();
    let env2 = next_matching("acc2 stream", &mut rx2, |e| {
        matches!(e.payload, Event::Order(_))
    })
    .await;
    assert!(
        matches!(&env2.payload, Event::Order(s) if s.cid == own_cid),
        "ACC2 must not see ACC1's order: {:?}",
        env2.payload
    );

    // Portfolio: separate accounts, separate money.
    let p1 = ud1.portfolio().await.unwrap();
    let p2 = ud2.portfolio().await.unwrap();
    assert_eq!(p1.balances[0].currency, "RUB");
    assert_eq!(p1.balances[0].available, Money::new(dec!(100_000.5)));
    assert_eq!(p1.positions[0].instrument.key.display(), "SBER@MISX");
    assert_eq!(p2.balances[0].currency, "USD");
    assert_eq!(p2.positions[0].qty.decimal(), dec!(-5.0));
    assert!(
        server
            .state
            .calls()
            .contains(&"GetAccount:ACC1".to_string())
    );
    assert!(
        server
            .state
            .calls()
            .contains(&"GetAccount:ACC2".to_string())
    );

    ud1.shutdown(ShutdownMode::Immediate).await;
    ud2.shutdown(ShutdownMode::Immediate).await;
}

// -------------------------------------------------------------------- order flow

#[tokio::test]
async fn commands_wait_for_reconcile_and_replace_is_unsupported() {
    let server = MockServer::start().await;
    let (ud, _rx) = ud(&server, "ACC1").await;
    assert!(matches!(
        ud.place(limit_buy(dec!(1.0))).await,
        Err(Error::NotReconciled)
    ));
    assert!(matches!(
        ud.cancel(ClientOrderId::new_v4()).await,
        Err(Error::NotReconciled)
    ));
    assert!(server.state.calls().is_empty(), "refused locally");

    let req = brokapi_core::ReplaceOrderRequest {
        cid: ClientOrderId::new_v4(),
        original_cid: ClientOrderId::new_v4(),
        qty: None,
        price: Some(Price::from(2)),
    };
    assert!(matches!(
        ud.replace(req).await,
        Err(Error::Unsupported { .. })
    ));
    assert!(
        server.state.calls().is_empty(),
        "Unsupported means no network call"
    );
}

#[tokio::test]
async fn place_sends_the_order_with_both_cid_forms() {
    let server = MockServer::start().await;
    let (ud, _rx) = reconciled(&server, "ACC1").await;
    let req = limit_buy(dec!(123.45));
    let cid = req.cid;
    let ack = ud.place(req).await.unwrap();
    assert!(ack.broker_id.as_str().starts_with("ORD"));

    let sent = server.state.orders_of("ACC1").remove(0).order.unwrap();
    assert_eq!(sent.symbol, "SBER@MISX");
    assert_eq!(sent.limit_price.unwrap().value, "123.45");
    assert_eq!(sent.quantity.unwrap().value, "1");
    assert_eq!(sent.side, v1::Side::Buy as i32);
    assert_eq!(sent.r#type, po::OrderType::Limit as i32);
    assert!(
        sent.client_order_id.len() <= 20,
        "documented limit of client_order_id"
    );
    assert_eq!(sent.client_order_id, FinamCidCodec.client_order_id(cid));
    assert!(sent.comment.len() <= 128, "documented limit of comment");
    use brokapi_core::CidCodec;
    assert_eq!(
        FinamCidCodec.decode(&brokapi_core::NativeCid::new(sent.comment)),
        Some(cid)
    );
}

#[tokio::test]
async fn market_order_has_no_price_and_limit_order_needs_one() {
    let server = MockServer::start().await;
    let (ud, _rx) = reconciled(&server, "ACC1").await;
    let mut m = limit_buy(dec!(0.0));
    m.order_type = OrderType::Market;
    m.price = None;
    ud.place(m).await.unwrap();
    let sent = server.state.orders_of("ACC1").remove(0).order.unwrap();
    assert_eq!(sent.r#type, po::OrderType::Market as i32);
    assert!(sent.limit_price.is_none());

    let mut bad = limit_buy(dec!(1.0));
    bad.price = None;
    assert!(
        matches!(ud.place(bad).await, Err(Error::Rejected { ref code, .. }) if code == "missing_price")
    );
    assert_eq!(server.state.count_calls("PlaceOrder"), 1);
}

#[tokio::test]
async fn a_price_off_the_step_is_refused_before_anything_is_sent() {
    let server = MockServer::start().await;
    let (ud, _rx) = reconciled(&server, "ACC1").await;
    ud.price_grid()
        .set(sber().key, brokapi_core::Price::new(dec!(0.01)));
    let mut req = limit_buy(dec!(123.457));
    let err = ud.place(req.clone()).await.unwrap_err();
    assert!(
        matches!(&err, Error::Rejected { code, .. } if code == "price_off_step"),
        "{err:?}"
    );
    assert_eq!(
        server.state.count_calls("PlaceOrder"),
        0,
        "nothing was sent"
    );
    // The cid was not spent: the same cid goes out with a price on the grid.
    req.price = Some(brokapi_core::Price::new(dec!(123.45)));
    ud.place(req).await.unwrap();
    assert_eq!(server.state.count_calls("PlaceOrder"), 1);
}

#[tokio::test]
async fn definite_rejection_is_rejected_ambiguous_failure_is_unknown() {
    let server = MockServer::start().await;
    let (ud, _rx) = reconciled(&server, "ACC1").await;
    let set = |c| *server.state.options.place_error.lock().unwrap() = Some(c);

    set(Code::InvalidArgument);
    assert!(matches!(
        ud.place(limit_buy(dec!(1.0))).await,
        Err(Error::Rejected { .. })
    ));
    set(Code::PermissionDenied);
    assert!(matches!(
        ud.place(limit_buy(dec!(1.0))).await,
        Err(Error::Rejected { .. })
    ));
    set(Code::ResourceExhausted);
    assert!(matches!(
        ud.place(limit_buy(dec!(1.0))).await,
        Err(Error::RateLimited { .. })
    ));
    // A dropped connection may have happened after the order was written: not a rejection.
    set(Code::Unavailable);
    assert!(matches!(
        ud.place(limit_buy(dec!(1.0))).await,
        Err(Error::Unknown)
    ));
    set(Code::Internal);
    assert!(matches!(
        ud.place(limit_buy(dec!(1.0))).await,
        Err(Error::Unknown)
    ));
}

#[tokio::test]
async fn timeout_gives_unknown_never_a_retry_and_the_state_is_found_by_query() {
    let server = MockServer::start().await;
    let mut cfg = UserDataConfig::new(api(&server), "ACC1", "finam-ud");
    cfg.api.request_timeout = Duration::from_millis(250);
    let (ud, _rx) = ud_with(cfg, limiter(), 64).await;
    ud.reconcile(&[]).await.unwrap();
    server
        .state
        .options
        .place_delay_ms
        .store(900, std::sync::atomic::Ordering::SeqCst);

    let req = limit_buy(dec!(1.0));
    let cid = req.cid;
    // The request reaches the mock, which holds it for 900 ms. The clock goes 300 ms ahead: the 250 ms
    // timeout of the connector is due, the mock's delay is not.
    let err = brokapi_mocks::time::elapse_when(
        "the order reached the mock",
        || server.state.count_calls("PlaceOrder") == 1,
        Duration::from_millis(300),
        ud.place(req.clone()),
    )
    .await
    .unwrap_err();
    assert!(matches!(err, Error::Unknown), "{err:?}");
    assert_eq!(err.class(), brokapi_core::ErrorClass::Unknown);

    // Same cid again: refused locally, nothing is sent.
    let again = ud.place(req).await.unwrap_err();
    assert!(
        matches!(again, Error::Rejected { ref code, .. } if code == "duplicate_place"),
        "{again:?}"
    );
    assert_eq!(server.state.count_calls("PlaceOrder"), 1);

    // The way out of Unknown: ask the broker. The order was applied, it is found by its cid.
    brokapi_mocks::time::jump(Duration::from_secs(1)).await;
    let snap = ud.order_state(cid).await.unwrap();
    assert_eq!(snap.cid, cid);
    assert!(matches!(snap.state, OrderState::Acked { .. }));
}

#[tokio::test]
async fn local_quota_stops_a_command_before_the_network() {
    let server = MockServer::start().await;
    let mut limits = default_limits();
    limits.buckets.get_mut("orders").unwrap().burst = 1;
    let cfg = UserDataConfig::new(api(&server), "ACC1", "finam-ud");
    let (ud, _rx) = ud_with(cfg, BrokerLimiter::new(&limits), 64).await;
    ud.reconcile(&[]).await.unwrap();
    ud.place(limit_buy(dec!(1.0))).await.unwrap();
    let err = ud.place(limit_buy(dec!(1.0))).await.unwrap_err();
    assert!(matches!(err, Error::RateLimited { .. }), "{err:?}");
    assert_eq!(server.state.count_calls("PlaceOrder"), 1);
    // Cancel has its own bucket and is not starved by placing.
    let cid = ud.active_orders().await.unwrap()[0].cid;
    ud.cancel(cid).await.unwrap();
}

#[tokio::test]
async fn cancel_of_a_filled_order_is_rejected_and_unknown_cid_not_found() {
    let server = MockServer::start().await;
    let (ud, _rx) = reconciled(&server, "ACC1").await;
    let req = limit_buy(dec!(1.0));
    let cid = req.cid;
    ud.place(req).await.unwrap();
    let mut os = server.state.orders_of("ACC1").remove(0);
    os.status = po::OrderStatus::Filled as i32;
    server.state.push_order("ACC1", os);
    assert!(matches!(ud.cancel(cid).await, Err(Error::Rejected { .. })));
    assert!(matches!(
        ud.cancel(ClientOrderId::new_v4()).await,
        Err(Error::Rejected { ref code, .. }) if code == "not_found"
    ));
}

#[tokio::test]
async fn order_state_stream_delivers_updates_with_our_cid() {
    let server = MockServer::start().await;
    let (ud, mut rx) = reconciled(&server, "ACC1").await;
    let sub = ud.subscribe_order_state().await.unwrap();
    let req = limit_buy(dec!(1.0));
    let cid = req.cid;
    ud.place(req).await.unwrap();
    let env = next_matching("order new", &mut rx, |e| {
        matches!(e.payload, Event::Order(_))
    })
    .await;
    assert_eq!(env.sub, Some(sub));
    let Event::Order(s) = env.payload else {
        unreachable!()
    };
    assert_eq!(s.cid, cid);

    let mut os = server.state.orders_of("ACC1").remove(0);
    os.status = po::OrderStatus::PartiallyFilled as i32;
    os.executed_quantity = Some(brokapi_finam_protocol::pb::google::r#type::Decimal {
        value: "0.4".into(),
    });
    os.remaining_quantity = Some(brokapi_finam_protocol::pb::google::r#type::Decimal {
        value: "0.6".into(),
    });
    server.state.push_order("ACC1", os);
    let env = next_matching("order partial", &mut rx, |e| {
        matches!(&e.payload, Event::Order(o) if matches!(o.state, OrderState::PartiallyFilled { .. }))
    })
    .await;
    let Event::Order(s) = env.payload else {
        unreachable!()
    };
    assert_eq!(s.cid, cid, "cid survives the round trip through the broker");
}

#[tokio::test]
async fn reconcile_sorts_confirmed_closed_offline_and_foreign() {
    let server = MockServer::start().await;
    let (ud, _rx) = ud(&server, "ACC1").await;
    ud.reconcile(&[]).await.unwrap();
    let mine = limit_buy(dec!(1.0));
    let finished = limit_buy(dec!(2.0));
    let (c_mine, c_fin) = (mine.cid, finished.cid);
    ud.place(mine).await.unwrap();
    ud.place(finished).await.unwrap();
    // One order ends while "offline"; one is placed by someone else (no comment of ours).
    let mut orders = server.state.orders_of("ACC1");
    orders[1].status = po::OrderStatus::Filled as i32;
    server.state.push_order("ACC1", orders.remove(1));
    server.state.push_order(
        "ACC1",
        po::OrderState {
            order_id: "MANUAL1".into(),
            status: po::OrderStatus::New as i32,
            order: Some(po::Order {
                account_id: "ACC1".into(),
                symbol: "SBER@MISX".into(),
                comment: "from the app".into(),
                ..Default::default()
            }),
            ..Default::default()
        },
    );

    // A fresh connector (a restarted process) knows nothing but its ledger.
    let (ud2, _rx2) = self::ud(&server, "ACC1").await;
    let ghost = ClientOrderId::new_v4();
    let report = ud2.reconcile(&[c_mine, c_fin, ghost]).await.unwrap();
    assert_eq!(report.confirmed.len(), 1);
    assert_eq!(report.confirmed[0].cid, c_mine);
    assert!(
        report.closed_offline.contains(&c_fin),
        "filled while offline"
    );
    assert!(
        report.closed_offline.contains(&ghost),
        "unknown to the broker"
    );
    assert_eq!(report.foreign.len(), 1);
    assert_eq!(
        report.foreign[0].broker_id.as_ref().unwrap().as_str(),
        "MANUAL1"
    );
    // And now it may trade.
    ud2.place(limit_buy(dec!(3.0))).await.unwrap();
}

// -------------------------------------------------- lossless overflow -> Degraded

#[tokio::test]
async fn lossless_overflow_emits_overflow_degrades_and_reconcile_recovers() {
    let server = MockServer::start().await;
    let mut cfg = UserDataConfig::new(api(&server), "ACC1", "finam-ud");
    cfg.order_state_buffer = 2;
    // Consumer channel of 1 that nobody reads: the buffer of the subscription must absorb.
    let (ud, mut rx) = ud_with(cfg, limiter(), 1).await;
    ud.reconcile(&[]).await.unwrap();
    let sub = ud.subscribe_order_state().await.unwrap();

    let req = limit_buy(dec!(1.0));
    ud.place(req).await.unwrap();
    let base = server.state.orders_of("ACC1").remove(0);
    for i in 0..12 {
        let mut os = base.clone();
        os.exec_id = format!("{i}");
        server.state.push_order("ACC1", os);
    }

    // The reading task was not blocked: the connector becomes Degraded and refuses commands.
    brokapi_mocks::time::wait_until_async("overflow must degrade the connector", || async {
        matches!(
            ud.cancel(ClientOrderId::new_v4()).await,
            Err(Error::Degraded)
        )
    })
    .await;
    assert!(matches!(
        ud.place(limit_buy(dec!(1.0))).await,
        Err(Error::Degraded)
    ));

    // The consumer finally reads: the gap is announced, not hidden.
    next_matching(
        "the overflow reaches the consumer",
        &mut rx,
        |e| matches!(e.payload, Event::Overflow { sub: s, .. } if s == sub),
    )
    .await;

    // Exit from Degraded is reconcile and nothing else.
    ud.reconcile(&[]).await.unwrap();
    assert!(!matches!(
        ud.cancel(ClientOrderId::new_v4()).await,
        Err(Error::Degraded)
    ));
    // The events that were queued before are old. The order after recovery differs in its status.
    let mut os = base.clone();
    os.exec_id = "after".into();
    os.status = po::OrderStatus::Filled as i32;
    server.state.push_order("ACC1", os);
    next_matching("after recovery", &mut rx, |e| {
        matches!(&e.payload, Event::Order(o) if matches!(o.state, OrderState::Filled { .. }))
            && e.sub == Some(sub)
    })
    .await;
}

// ------------------------------------------------------------- executions, portfolio

#[tokio::test]
async fn executions_history_and_stream() {
    let server = MockServer::start().await;
    let (ud, mut rx) = reconciled(&server, "ACC1").await;
    let req = limit_buy(dec!(1.0));
    let cid = req.cid;
    ud.place(req).await.unwrap();
    let order = server.state.orders_of("ACC1").remove(0);
    let sub = ud.subscribe_executions().await.unwrap();
    let trade = v1::AccountTrade {
        trade_id: "TR1".into(),
        symbol: "SBER@MISX".into(),
        price: Some(brokapi_finam_protocol::pb::google::r#type::Decimal { value: "1".into() }),
        size: Some(brokapi_finam_protocol::pb::google::r#type::Decimal { value: "1".into() }),
        side: v1::Side::Buy as i32,
        order_id: order.order_id.clone(),
        comment: order.order.unwrap().comment,
        account_id: "ACC1".into(),
        ..Default::default()
    };
    // The stream is opened by a background task: wait until the server has it.
    brokapi_mocks::time::wait_until("the trades stream is open", || {
        server.state.count_calls("SubscribeTrades") > 0
    })
    .await;
    server.state.push_trade("ACC1", trade);
    let env = next_matching("execution", &mut rx, |e| {
        matches!(e.payload, Event::Execution(_))
    })
    .await;
    assert_eq!(env.sub, Some(sub));
    let Event::Execution(x) = env.payload else {
        unreachable!()
    };
    assert_eq!(
        (x.cid, x.trade_id.as_str(), x.broker_order_id.as_str()),
        (Some(cid), "TR1", order.order_id.as_str())
    );

    let history = ud
        .executions(TimeRange::new(
            UnixNanos::ZERO,
            UnixNanos::from_nanos(i64::MAX / 2),
        ))
        .await
        .unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].cid, Some(cid));
}

#[tokio::test]
async fn portfolio_stream_pushes_updates_and_positions_balances_come_from_one_call() {
    let server = MockServer::start().await;
    let (ud, mut rx) = ud(&server, "ACC1").await;
    let sub = ud
        .subscribe_portfolio(SubscribeOpts::lossless(16))
        .await
        .unwrap();
    let first = next_matching("portfolio first", &mut rx, |e| {
        matches!(e.payload, Event::Portfolio(_))
    })
    .await;
    assert_eq!(first.sub, Some(sub));
    server.state.set_cash(
        "ACC1",
        vec![brokapi_finam_protocol::pb::google::r#type::Money {
            currency_code: "RUB".into(),
            units: 1,
            nanos: 0,
        }],
    );
    let upd = next_matching(
        "portfolio update",
        &mut rx,
        |e| matches!(&e.payload, Event::Portfolio(p) if p.balances[0].available == Money::from(1)),
    )
    .await;
    assert_eq!(upd.sub, Some(sub));
    assert_eq!(ud.balances().await.unwrap()[0].available, Money::from(1));
    assert_eq!(ud.positions().await.unwrap().len(), 1);
}

// ------------------------------------------------------------------- market data

#[tokio::test]
async fn quotes_trades_and_candles() {
    let server = MockServer::start().await;
    let (md, mut rx) = md(&server).await;

    let q = md
        .subscribe_quotes(&sber(), SubscribeOpts::conflate())
        .await
        .unwrap();
    let env = next_matching("quote", &mut rx, |e| matches!(e.payload, Event::Quote(_))).await;
    assert_eq!(env.sub, Some(q));
    let Event::Quote(quote) = env.payload else {
        unreachable!()
    };
    assert_eq!(quote.bid.unwrap().price.decimal(), dec!(100.0));
    assert_eq!(quote.ask.unwrap().price.decimal(), dec!(100.1));

    let t = md
        .subscribe_market_trades(&sber(), SubscribeOpts::lossless(256))
        .await
        .unwrap();
    // The mock replays T1 in every message; it must be delivered once.
    let mut ids = Vec::new();
    while ids.len() < 3 {
        let env = next_matching("trade", &mut rx, |e| {
            matches!(e.payload, Event::MarketTrade(_))
        })
        .await;
        assert_eq!(env.sub, Some(t));
        let Event::MarketTrade(tr) = env.payload else {
            unreachable!()
        };
        ids.push(tr.trade_id.unwrap());
    }
    assert_eq!(ids, vec!["T1", "T2", "T3"]);

    let c = md
        .subscribe_candles(&sber(), Timeframe::M5, SubscribeOpts::conflate())
        .await
        .unwrap();
    let env = next_matching("candle", &mut rx, |e| matches!(e.payload, Event::Candle(_))).await;
    assert_eq!(env.sub, Some(c));

    // History.
    let range = TimeRange::new(
        UnixNanos::from_nanos(1_700_000_000 * 1_000_000_000),
        UnixNanos::from_nanos(1_700_003_600 * 1_000_000_000),
    );
    let bars = md.candles(&sber(), Timeframe::M1, range).await.unwrap();
    assert_eq!(bars.len(), 3);
    assert_eq!(
        bars[0].close_ts.as_nanos() - bars[0].open_ts.as_nanos(),
        60 * 1_000_000_000
    );

    // last_trades: 4 trades, one without side is dropped, newest `limit` kept.
    let last = md.last_trades(&sber(), 2).await.unwrap();
    let got: Vec<_> = last
        .iter()
        .map(|t| (t.trade_id.clone().unwrap(), t.side))
        .collect();
    assert_eq!(
        got,
        vec![("T2".into(), Side::Sell), ("T4".into(), Side::Buy)]
    );
}

/// Finam has no 10-minute bars: history is M5 asked from Finam and merged, the stream is an M5 stream.
#[tokio::test]
async fn m10_candles_are_built_from_m5() {
    let pb_timeframe_m5 = || v1::marketdata::TimeFrame::M5 as i32;
    let server = MockServer::start().await;
    let (md, mut rx) = md(&server).await;

    // History. The range starts mid-interval (1_700_000_000 s = 3 min 20 s after a boundary).
    let from = 1_700_000_000i64;
    let range = TimeRange::new(
        UnixNanos::from_nanos(from * 1_000_000_000),
        UnixNanos::from_nanos((from + 3600) * 1_000_000_000),
    );
    let candles = md.candles(&sber(), Timeframe::M10, range).await.unwrap();
    let (tf, start) = server.state.options.bars_requests.lock().unwrap()[0];
    assert_eq!(tf, pb_timeframe_m5(), "M5 is asked from Finam");
    assert_eq!(
        start, 1_699_999_800,
        "the request starts at the 10 minute boundary"
    );
    // The mock answers 3 candles one minute apart from the start: one 10-minute interval.
    assert_eq!(candles.len(), 1);
    let c = &candles[0];
    assert_eq!(c.timeframe, Timeframe::M10);
    assert_eq!(c.open_ts.as_nanos(), 1_699_999_800 * 1_000_000_000);
    assert_eq!(
        c.close_ts.as_nanos() - c.open_ts.as_nanos(),
        600 * 1_000_000_000
    );
    assert_eq!(
        (
            c.open.decimal(),
            c.high.decimal(),
            c.low.decimal(),
            c.close.decimal(),
            c.volume.decimal()
        ),
        (
            dec!(100.0),
            dec!(101.0),
            dec!(99.0),
            dec!(100.5),
            dec!(3000.0)
        )
    );

    // Stream.
    let sub = md
        .subscribe_candles(&sber(), Timeframe::M10, SubscribeOpts::conflate())
        .await
        .unwrap();
    assert_eq!(
        *server.state.options.bars_streams.lock().unwrap(),
        vec![pb_timeframe_m5()]
    );
    let env = next_matching("m10 candle", &mut rx, |e| {
        matches!(e.payload, Event::Candle(_))
    })
    .await;
    assert_eq!(env.sub, Some(sub));
    let Event::Candle(c) = env.payload else {
        unreachable!()
    };
    assert_eq!(c.timeframe, Timeframe::M10);
    assert_eq!(c.open_ts.as_nanos(), 1_699_999_800 * 1_000_000_000);
    assert_eq!(
        c.close_ts.as_nanos() - c.open_ts.as_nanos(),
        600 * 1_000_000_000
    );
}

#[tokio::test]
async fn refused_subscription_returns_the_reason_and_leaves_nothing_behind() {
    let server = MockServer::start().await;
    let (md, _rx) = md(&server).await;
    *server.state.options.deny_symbol.lock().unwrap() = Some("GAZP@MISX".into());
    let err = md
        .subscribe_order_book(&gazp(), SubscribeOpts::conflate())
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::Rejected { ref code, .. } if code == "permission_denied"),
        "{err:?}"
    );
    assert!(matches!(
        md.order_book(&gazp(), 5).await,
        Err(Error::Rejected { .. })
    ));
    // The failed subscription is gone; the shutdown has nothing to close.
    let report = md
        .shutdown(ShutdownMode::Graceful {
            deadline: Duration::from_secs(1),
        })
        .await;
    assert_eq!(report.unsubscribed, 0);
}

#[tokio::test]
async fn dropped_book_stream_is_reopened_with_resync() {
    let server = MockServer::start().await;
    let (md, mut rx) = md(&server).await;
    server
        .state
        .options
        .book_drop_after
        .store(3, std::sync::atomic::Ordering::SeqCst);
    let sub = md
        .subscribe_order_book(&sber(), SubscribeOpts::conflate())
        .await
        .unwrap();
    // The first stream ends after 3 messages; the reopened one announces the reset.
    let env = next_matching("resync", &mut rx, |e| {
        matches!(e.payload, Event::Resync { .. })
    })
    .await;
    assert_eq!(env.sub, Some(sub));
    assert!(
        matches!(env.payload, Event::Resync { reason: ResyncReason::SubscriptionReset, sub: s, .. } if s == sub)
    );
    // ... followed by a fresh snapshot with the book rebuilt.
    server
        .state
        .options
        .book_drop_after
        .store(0, std::sync::atomic::Ordering::SeqCst);
    let env = next_matching("book after resync", &mut rx, |e| {
        matches!(e.payload, Event::OrderBook(_))
    })
    .await;
    let Event::OrderBook(OrderBook::Snapshot { bids, .. }) = env.payload else {
        panic!()
    };
    assert_eq!(bids[0].price.decimal(), dec!(100.0));
    assert!(
        server
            .state
            .book_opens
            .load(std::sync::atomic::Ordering::SeqCst)
            >= 2
    );
    assert!(matches!(
        md.sub_status(sub),
        Some(SubStatus::Active | SubStatus::Reconnecting(_))
    ));
}

#[tokio::test]
async fn increment_without_a_base_forces_a_fresh_stream() {
    let server = MockServer::start().await;
    let (md, mut rx) = md(&server).await;
    server
        .state
        .options
        .book_first_increment_streams
        .store(1, std::sync::atomic::Ordering::SeqCst);
    let sub = md
        .subscribe_order_book(&sber(), SubscribeOpts::conflate())
        .await
        .unwrap();
    let env = next_matching("gap", &mut rx, |e| {
        matches!(e.payload, Event::Resync { .. })
    })
    .await;
    assert!(matches!(
        env.payload,
        Event::Resync {
            reason: ResyncReason::SequenceGap,
            ..
        }
    ));
    // The stream is restarted at once (not after the backoff) and its snapshot arrives.
    next_matching("book", &mut rx, |e| {
        matches!(e.payload, Event::OrderBook(_)) && e.sub == Some(sub)
    })
    .await;
    assert!(
        server
            .state
            .book_opens
            .load(std::sync::atomic::Ordering::SeqCst)
            >= 2
    );
}

#[tokio::test]
async fn graceful_shutdown_closes_every_stream() {
    let server = MockServer::start().await;
    let (md, mut rx) = md(&server).await;
    let s1 = md
        .subscribe_order_book(&sber(), SubscribeOpts::conflate())
        .await
        .unwrap();
    md.subscribe_quotes(&sber(), SubscribeOpts::conflate())
        .await
        .unwrap();
    next_matching("data", &mut rx, |e| {
        matches!(e.payload, Event::OrderBook(_))
    })
    .await;
    md.unsubscribe(s1).await.unwrap();
    assert!(
        md.unsubscribe(s1).await.is_err(),
        "second unsubscribe is an error, not a panic"
    );
    let report = md
        .shutdown(ShutdownMode::Graceful {
            deadline: Duration::from_secs(2),
        })
        .await;
    assert_eq!((report.unsubscribed, report.unsubscribe_failed), (1, 0));
    // The consumer's channel ends once the connector is gone.
    while rx.recv().await.is_some() {}

    let (ud, mut urx) = ud(&server, "ACC1").await;
    ud.subscribe_order_state().await.unwrap();
    ud.subscribe_portfolio(SubscribeOpts::lossless(8))
        .await
        .unwrap();
    let report = ud
        .shutdown(ShutdownMode::Graceful {
            deadline: Duration::from_secs(2),
        })
        .await;
    assert_eq!((report.unsubscribed, report.unsubscribe_failed), (2, 0));
    assert!(report.inflight_abandoned.is_empty());
    while urx.recv().await.is_some() {}
}

// ---------------------------------------------------------------------- instruments

#[tokio::test]
async fn directory_trading_params_status_and_schedule() {
    let server = MockServer::start().await;
    let (md, _rx) = md(&server).await;

    let by_isin = md
        .find(InstrumentQuery::ByIsin("RU0007661625".into()))
        .await
        .unwrap();
    assert_eq!(by_isin[0].id.key.display(), "GAZP@MISX");
    let bonds = md
        .find(InstrumentQuery::ByKind(InstrumentKind::Bond))
        .await
        .unwrap();
    assert_eq!(bonds.len(), 1);
    assert!(
        server.state.count_calls("AllAssets") >= 4,
        "two scans of two pages each"
    );

    let inst = md.get(&sber()).await.unwrap();
    assert_eq!(
        (inst.currency.as_str(), inst.exchange.as_str(), inst.kind),
        ("RUB", "MISX", InstrumentKind::Stock)
    );
    let p = md.trading_params(&sber()).await.unwrap();
    assert_eq!(
        (
            p.lot_size.decimal(),
            p.min_qty.decimal(),
            p.currency.as_str()
        ),
        (dec!(10.0), dec!(10.0), "RUB")
    );
    assert_eq!(
        p.price_step,
        Price::new(dec!(0.01)),
        "min_step 1 with 2 decimals"
    );
    assert_eq!(p.price_step.to_string(), "0.01");

    let st = md.trading_status(&sber()).await.unwrap();
    assert_eq!(st.availability, TradingAvailability::Open);
    assert_eq!(st.reason.as_deref(), Some("CORE_TRADING"));

    let now = brokapi_finam::util::now_nanos().as_nanos();
    let range = TimeRange::new(
        UnixNanos::from_nanos(now - 10_800_000_000_000),
        UnixNanos::from_nanos(now + 10_800_000_000_000),
    );
    let sched = md.schedule("SBER@MISX", range).await.unwrap();
    use brokapi_core::SessionKind::*;
    let kinds: Vec<_> = sched.sessions.iter().map(|s| s.kind).collect();
    assert_eq!(
        kinds,
        vec![Main, PostMarket],
        "the CLOSED interval is not a session"
    );
    assert!(
        matches!(md.schedule("MISX", range).await, Err(Error::Config(_))),
        "per instrument, not per exchange"
    );
    assert!(
        matches!(md.get(&gazp_unknown()).await, Err(Error::Rejected { ref code, .. }) if code == "not_found")
    );
}

fn gazp_unknown() -> brokapi_core::InstrumentId {
    brokapi_finam::util::instrument_from_symbol("NOPE@MISX")
}

#[tokio::test]
async fn rest_book_is_best_first_and_depth_limited() {
    let server = MockServer::start().await;
    let (md, _rx) = md(&server).await;
    let book = md.order_book(&sber(), 2).await.unwrap();
    let OrderBook::Snapshot { bids, asks, .. } = book else {
        panic!()
    };
    let p = |v: &[brokapi_core::BookLevel]| v.iter().map(|l| l.price.decimal()).collect::<Vec<_>>();
    assert_eq!(p(&bids), vec![dec!(100.0), dec!(99.9)]);
    assert_eq!(p(&asks), vec![dec!(100.1), dec!(100.2)]);
}

#[allow(dead_code)]
const _SECRET: &str = SECRET;

/// Input, then quota, then the cid is marked: a `place` refused for `RateLimited` sent nothing, so the same
/// cid is used again (the rule is the same for BCS and T-Bank).
#[tokio::test]
async fn rate_limited_place_leaves_the_cid_usable() {
    let server = MockServer::start().await;
    let toml = r#"
        [finam.buckets.big]
        rate = 10000
        per = "1s"
        burst = 10000
        [finam.buckets.one_order]
        rate = 1
        per = "400ms"
        burst = 1
        [finam.route]
        "*" = ["big"]
        PlaceOrder = ["one_order"]
        [finam.conns]
        max_conns = 4
        max_subs_per_conn = 50
        supports_multiplex = true
    "#;
    let lim = BrokerLimiter::new(&parse_limits(toml).unwrap());
    let (ud, _rx) = ud_with(
        UserDataConfig::new(api(&server), "ACC1", "finam-ud"),
        lim,
        64,
    )
    .await;
    ud.reconcile(&[]).await.unwrap();
    ud.place(limit_buy(dec!(1.0))).await.unwrap();

    let req = limit_buy(dec!(1.0));
    let retry_after = match ud.place(req.clone()).await {
        Err(Error::RateLimited { retry_after }) => retry_after,
        other => panic!("expected RateLimited, got {other:?}"),
    };
    brokapi_mocks::time::jump(retry_after + Duration::from_millis(50)).await;
    ud.place(req.clone())
        .await
        .expect("the same cid after RateLimited");
    brokapi_mocks::time::jump(Duration::from_millis(450)).await;
    assert!(matches!(
        ud.place(req).await,
        Err(Error::Rejected { ref code, .. }) if code == "duplicate_place"
    ));
}

// ------------------------------------------------------------- lot for trading, end of a period

#[tokio::test]
async fn trade_lot_size_replaces_the_lot_of_the_directory_only_when_it_is_above_zero() {
    let server = MockServer::start().await;
    let (md, _rx) = md(&server).await;
    let lots = |p: &brokapi_core::TradingParams| (p.lot_size.decimal(), p.min_qty.decimal());

    // 0 is "no value": the lot of GetAsset (10 in the mock) stays.
    let p = md.trading_params(&sber()).await.unwrap();
    assert_eq!(lots(&p), (dec!(10.0), dec!(10.0)));
    // The call went for the first account of the token.
    assert_eq!(
        server.state.count_calls("GetAssetParams:SBER@MISX:ACC1"),
        1,
        "{:?}",
        server.state.calls()
    );

    // Above 0 it replaces the lot, and the minimum quantity with it.
    server
        .state
        .options
        .trade_lot_size
        .store(100, std::sync::atomic::Ordering::SeqCst);
    let p = md.trading_params(&sber()).await.unwrap();
    assert_eq!(lots(&p), (dec!(100.0), dec!(100.0)));
    // The rest of the parameters come from GetAsset as before.
    assert_eq!(
        (p.currency.as_str(), p.price_step.to_string()),
        ("RUB", "0.01".to_string())
    );
}

#[tokio::test]
async fn a_refused_params_call_keeps_the_lot_of_the_directory_and_a_quota_error_is_returned() {
    let server = MockServer::start().await;
    let (md, _rx) = md(&server).await;
    server
        .state
        .options
        .trade_lot_size
        .store(100, std::sync::atomic::Ordering::SeqCst);

    // The broker refuses the extra call: the answer of GetAsset is still good.
    *server.state.options.params_error.lock().unwrap() = Some(Code::PermissionDenied);
    let p = md.trading_params(&sber()).await.unwrap();
    assert_eq!(p.lot_size.decimal(), dec!(10.0));

    // A limit is not a refusal of this call: the caller must see it and repeat the read.
    *server.state.options.params_error.lock().unwrap() = Some(Code::ResourceExhausted);
    let err = md.trading_params(&sber()).await.unwrap_err();
    assert!(matches!(err, Error::RateLimited { .. }), "{err:?}");
}

#[tokio::test]
async fn every_history_request_has_both_ends_of_the_period() {
    let server = MockServer::start().await;
    let (md, _rx) = md(&server).await;
    let (ud, _urx) = ud(&server, "ACC1").await;
    let range = TimeRange::new(
        UnixNanos::from_nanos(1_700_000_000 * 1_000_000_000),
        UnixNanos::from_nanos(1_700_003_600 * 1_000_000_000),
    );
    md.candles(&sber(), Timeframe::M5, range).await.unwrap();
    // M10 is built from M5 and widens the start: the end must still be there.
    md.candles(&sber(), Timeframe::M10, range).await.unwrap();
    ud.executions(range).await.unwrap();

    let seen = server.state.options.intervals.lock().unwrap().clone();
    assert!(seen.iter().any(|(m, ..)| *m == "Bars"));
    assert!(seen.iter().any(|(m, ..)| *m == "Trades"));
    for (method, start, end) in seen {
        assert!(
            start.is_some() && end.is_some(),
            "{method} was sent without a bound: {start:?} {end:?}"
        );
        assert!(
            end.unwrap() > start.unwrap(),
            "{method}: the end follows the start"
        );
    }
}
