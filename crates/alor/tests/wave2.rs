//! Guarantees of the connector under pressure: an overflow of a buffer, a quota that is spent, two accounts.

mod mock_server;

use std::sync::Arc;
use std::time::Duration;

use brokapi_alor::limits::default_limits;
use brokapi_alor::util::instrument_id;
use brokapi_alor::{MarketDataConfig, MarketDataConnector, UserDataConfig, UserDataConnector};
use brokapi_alor_protocol::config::AlorConfig;
use brokapi_core::{
    ClientOrderId, Decimal, Envelope, Error, Event, ExecutionSource, OrderExecutor, OrderType,
    PlaceOrderRequest, PlaceStopOrderRequest, Price, Qty, ShutdownMode, Side, StopExpiry,
    StopOrderExecutor, StopOrderKind, dec,
};
use brokapi_runtime::{BrokerLimiter, BrokerLimits};
use mock_server::{MockOptions, MockServer};
use tokio::sync::mpsc;
use tokio::time::timeout;

fn limiter() -> Arc<BrokerLimiter> {
    BrokerLimiter::new(&default_limits())
}

fn limit_buy(qty: i64, price: Decimal) -> PlaceOrderRequest {
    PlaceOrderRequest {
        cid: ClientOrderId::new_v4(),
        instrument: instrument_id("SBER", "TQBR"),
        side: Side::Buy,
        order_type: OrderType::Limit,
        qty: Qty::from(qty),
        price: Some(Price::new(price)),
    }
}

/// A buffer that fills while nobody reads the channel gives `Event::Overflow` once, the connector goes
/// `Degraded` (commands are refused), and a successful `reconcile` opens the gate again.
#[tokio::test]
async fn lossless_overflow_is_visible_and_degrades_until_reconcile() {
    let server = MockServer::start(Arc::default()).await;
    // A channel of one slot that nobody reads, and a buffer of two events.
    let (tx, mut rx) = mpsc::channel::<Envelope>(1);
    let mut cfg = UserDataConfig::new(
        AlorConfig::for_mock("mock-refresh-token", &server.http_base),
        "alor-ud",
    );
    cfg.order_state_buffer = 2;
    let ud = UserDataConnector::start(cfg, limiter(), tx)
        .await
        .expect("ud start");
    let sub = ud
        .subscribe_executions()
        .await
        .expect("subscribe_executions");
    ud.reconcile(&[])
        .await
        .expect("first reconcile opens the gate");
    let req = limit_buy(200, dec!(100));
    let cid = req.cid;
    ud.place(req).await.expect("a healthy connector trades");

    for _ in 0..12 {
        server.fill(
            &cid.hyphenated().to_string(),
            Qty::from(1),
            Price::from(100),
        );
    }
    // The buffer fills while nobody reads: commands are refused.
    brokapi_mocks::time::wait_until_async("Degraded after the overflow", || async {
        matches!(
            ud.place(limit_buy(10, dec!(100))).await,
            Err(Error::Degraded)
        )
    })
    .await;
    assert!(matches!(
        ud.cancel(ClientOrderId::new_v4()).await,
        Err(Error::Degraded)
    ));

    // The bot reads the channel: Overflow is there, for this subscription.
    let mut overflows = 0;
    timeout(Duration::from_secs(8), async {
        while let Some(env) = rx.recv().await {
            if let Event::Overflow { sub: s, .. } = env.payload {
                assert_eq!(s, sub);
                assert_eq!(env.sub, Some(sub));
                overflows += 1;
                break;
            }
        }
    })
    .await
    .expect("the Overflow reaches the bot");
    assert_eq!(overflows, 1);

    // A reconcile repairs it: the gate is open again.
    ud.reconcile(&[cid])
        .await
        .expect("reconcile after the overflow");
    ud.place(limit_buy(10, dec!(100)))
        .await
        .expect("the gate is open again");
    ud.shutdown(ShutdownMode::Immediate).await;
}

/// A refusal before the wire (bad input, `RateLimited`) leaves the cid usable. Once sent, it is spent.
#[tokio::test]
async fn a_refusal_before_the_wire_leaves_the_cid_usable() {
    let opts = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&opts)).await;
    let (tx, _rx) = mpsc::channel(256);
    // The placement bucket holds one order and refills in a second.
    let limits = BrokerLimits::from_toml_str(
        r#"
        [alor.buckets.big]
        rate = 1000
        per = "1s"
        burst = 1000
        [alor.buckets.tiny]
        rate = 1
        per = "1s"
        burst = 1
        [alor.route]
        "*" = ["big"]
        PlaceOrder = ["big", "tiny"]
        PlaceStopOrder = ["big", "tiny"]
        [alor.conns]
        max_conns = 10
        max_subs_per_conn = 5000
        supports_multiplex = true
        "#,
    )
    .unwrap();
    let cfg = AlorConfig::for_mock("mock-refresh-token", &server.http_base);
    let ud = UserDataConnector::start(
        UserDataConfig::new(cfg, "alor-ud"),
        BrokerLimiter::new(&limits),
        tx,
    )
    .await
    .expect("ud start");
    ud.reconcile(&[]).await.unwrap();

    // Input: a limit order without a price is refused locally, the cid is not spent.
    let mut req = limit_buy(10, dec!(100));
    let price = req.price.take();
    assert!(
        matches!(ud.place(req.clone()).await, Err(Error::Rejected { ref code, .. }) if code == "missing_price")
    );
    req.price = price;
    ud.place(req)
        .await
        .expect("the cid of a refused input is usable");

    // Quota: the next order comes at once and finds the bucket empty.
    let second = limit_buy(10, dec!(100));
    let retry_after = match ud.place(second.clone()).await {
        Err(Error::RateLimited { retry_after }) => retry_after,
        other => panic!("expected RateLimited, got {other:?}"),
    };
    assert_eq!(
        opts.served_count("POST /commandapi/warptrans/TRADE/v2/client/orders/actions/limit"),
        1
    );
    brokapi_mocks::time::jump(retry_after + Duration::from_millis(50)).await;
    ud.place(second.clone())
        .await
        .expect("the same cid after RateLimited");
    // Once it has gone out, the cid is spent.
    brokapi_mocks::time::jump(Duration::from_millis(1100)).await;
    assert!(
        matches!(ud.place(second).await, Err(Error::Rejected { ref code, .. }) if code == "duplicate_place")
    );

    // The same for a stop order.
    brokapi_mocks::time::jump(Duration::from_millis(1100)).await;
    let stop = |p: Decimal| PlaceStopOrderRequest {
        cid: ClientOrderId::new_v4(),
        instrument: instrument_id("SBER", "TQBR"),
        side: Side::Sell,
        kind: StopOrderKind::StopLoss,
        qty: Qty::from(10),
        stop_price: Price::new(p),
        limit_price: None,
        expiry: StopExpiry::GoodTillCancel,
    };
    ud.place_stop(stop(dec!(90))).await.expect("a stop order");
    let again = stop(dec!(91));
    let retry_after = match ud.place_stop(again.clone()).await {
        Err(Error::RateLimited { retry_after }) => retry_after,
        other => panic!("expected RateLimited, got {other:?}"),
    };
    brokapi_mocks::time::jump(retry_after + Duration::from_millis(50)).await;
    ud.place_stop(again)
        .await
        .expect("the same cid of a stop order after RateLimited");
    ud.shutdown(ShutdownMode::Immediate).await;
}

/// Two logins on one imitation: each sees only its own orders and trades.
#[tokio::test]
async fn two_accounts_with_two_tokens_see_only_their_own_orders() {
    let server = MockServer::start(Arc::default()).await;
    let start = |token: &str| {
        let cfg = AlorConfig::for_mock(token, &server.http_base);
        let (tx, rx) = mpsc::channel(64);
        async move {
            let ud = UserDataConnector::start(UserDataConfig::new(cfg, "alor-ud"), limiter(), tx)
                .await
                .expect("ud start");
            ud.reconcile(&[]).await.unwrap();
            (ud, rx)
        }
    };
    let (a, _ra) = start("mock-refresh-token-a").await;
    let (b, _rb) = start("mock-refresh-token-b").await;
    let order = limit_buy(10, dec!(100));
    let cid = order.cid;
    a.place(order).await.unwrap();
    assert_eq!(a.active_orders().await.unwrap().len(), 1);
    assert!(b.active_orders().await.unwrap().is_empty());
    assert!(b.order_state(cid).await.unwrap_err().is_not_found());
    let report = b.reconcile(&[cid]).await.unwrap();
    assert_eq!(report.closed_offline, vec![cid]);
    let range = brokapi_core::TimeRange::new(
        brokapi_core::UnixNanos::ZERO,
        brokapi_core::UnixNanos::from_nanos(i64::MAX / 2),
    );
    assert!(b.executions(range).await.unwrap().is_empty());
    a.shutdown(ShutdownMode::Immediate).await;
    b.shutdown(ShutdownMode::Immediate).await;
}

/// One limiter for both connectors of a login: market data and the account spend one budget.
#[tokio::test]
async fn one_limiter_for_both_connectors_of_a_login() {
    let server = MockServer::start(Arc::default()).await;
    let limits = BrokerLimits::from_toml_str(
        r#"
        [alor.buckets.one]
        rate = 1
        per = "60s"
        burst = 2
        [alor.route]
        "*" = ["one"]
        [alor.conns]
        max_conns = 10
        max_subs_per_conn = 5000
        supports_multiplex = true
        "#,
    )
    .unwrap();
    let shared = BrokerLimiter::new(&limits);
    let cfg = AlorConfig::for_mock("mock-refresh-token", &server.http_base);
    let (tx, _rx) = mpsc::channel(8);
    let md = MarketDataConnector::start(
        MarketDataConfig::new(cfg.clone(), "alor-md"),
        Arc::clone(&shared),
        tx.clone(),
    )
    .await
    .unwrap();
    let ud = UserDataConnector::start(UserDataConfig::new(cfg, "alor-ud"), shared, tx)
        .await
        .unwrap();
    use brokapi_core::OrderBookSource;
    let sber = instrument_id("SBER", "TQBR");
    md.order_book(&sber, 5).await.unwrap();
    md.order_book(&sber, 5).await.unwrap();
    // The two units of the budget are spent by the market data connector: the account finds none.
    assert!(matches!(
        ud.active_orders().await,
        Err(Error::RateLimited { .. })
    ));
    md.shutdown(ShutdownMode::Immediate).await;
    ud.shutdown(ShutdownMode::Immediate).await;
}
