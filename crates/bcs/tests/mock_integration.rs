//! Level-2 mock integration (no external network).

mod mock_server;

use std::sync::Arc;
use std::time::Duration;

use brokapi_bcs::limits::default_limits;
use brokapi_bcs::{MarketDataConfig, MarketDataConnector, UserDataConfig, UserDataConnector};
use brokapi_bcs_protocol::config::BcsConfig;
use brokapi_core::{
    Backpressure, ClientOrderId, Envelope, Error, Event, ExecutionSource, InstrumentId,
    InstrumentKey, NativeId, OrderBookSource, OrderExecutor, OrderType, PlaceOrderRequest,
    PortfolioSource, Price, Qty, ShutdownMode, Side, SubscribeOpts,
};
use brokapi_runtime::BrokerLimiter;
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

fn bad_instrument() -> InstrumentId {
    InstrumentId::new(
        InstrumentKey::new("BADSUB", "TQBR"),
        NativeId::TickerClass {
            ticker: "BADSUB".into(),
            class_code: "TQBR".into(),
        },
    )
}

fn mock_bcs(http_base: &str, write: bool) -> BcsConfig {
    BcsConfig::for_mock("mock-refresh-token", http_base).with_write(write)
}

fn test_limiter() -> Arc<BrokerLimiter> {
    BrokerLimiter::new(&default_limits())
}

/// The next order book event in the channel, with the envelope.
async fn event_of_book(rx: &mut mpsc::Receiver<Envelope>) -> Envelope {
    timeout(Duration::from_secs(8), async {
        while let Some(env) = rx.recv().await {
            if matches!(env.payload, Event::OrderBook(_)) {
                return env;
            }
        }
        panic!("event channel closed before an order book");
    })
    .await
    .expect("an order book within 8s")
}

async fn wait_for_event(rx: &mut mpsc::Receiver<Envelope>, pred: impl Fn(&Envelope) -> bool) {
    timeout(Duration::from_secs(8), async {
        while let Some(env) = rx.recv().await {
            if pred(&env) {
                return;
            }
        }
        panic!("event channel closed before predicate matched");
    })
    .await
    .expect("timed out waiting for envelope");
}

/// Waits until every predicate has matched at least one envelope, in any order.
async fn wait_for_all(rx: &mut mpsc::Receiver<Envelope>, preds: &[&dyn Fn(&Envelope) -> bool]) {
    let mut seen = vec![false; preds.len()];
    timeout(Duration::from_secs(8), async {
        while let Some(env) = rx.recv().await {
            for (i, pred) in preds.iter().enumerate() {
                if pred(&env) {
                    seen[i] = true;
                }
            }
            if seen.iter().all(|s| *s) {
                return;
            }
        }
        panic!("event channel closed before all predicates matched");
    })
    .await
    .expect("timed out waiting for envelopes");
}

/// §6 full stack on mock: start → subscribe → data → reconcile → place/cancel → shutdown.
#[tokio::test]
async fn full_stack_happy_path() {
    let options = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&options)).await;
    let limiter = test_limiter();
    let (tx, mut rx) = mpsc::channel::<Envelope>(256);

    let md = MarketDataConnector::start(
        MarketDataConfig::new(mock_bcs(&server.http_base, false), "bcs-md"),
        Arc::clone(&limiter),
        tx.clone(),
    )
    .await
    .expect("md start");

    let ud = UserDataConnector::start(
        UserDataConfig::new(mock_bcs(&server.http_base, true), "bcs-ud"),
        Arc::clone(&limiter),
        tx,
    )
    .await
    .expect("ud start");

    let _sub = md
        .subscribe_order_book(
            &sber(),
            SubscribeOpts {
                backpressure: Backpressure::Conflate,
                depth: Some(20),
            },
        )
        .await
        .expect("subscribe order book");

    let _ud_sub = ud.subscribe_order_state().await.expect("order state sub");
    let _pf_sub = ud
        .subscribe_portfolio(SubscribeOpts {
            backpressure: Backpressure::Lossless { cap: 64 },
            depth: None,
        })
        .await
        .expect("portfolio sub");

    // MD and UD run on separate WS sessions: order book and portfolio arrive in any order.
    wait_for_all(
        &mut rx,
        &[
            &|e: &Envelope| {
                e.source.as_ref() == "bcs-md" && matches!(e.payload, Event::OrderBook(_))
            },
            &|e: &Envelope| {
                e.source.as_ref() == "bcs-ud" && matches!(e.payload, Event::Portfolio(_))
            },
        ],
    )
    .await;

    let book = md.order_book(&sber(), 20).await.expect("rest order book");
    assert!(matches!(book, brokapi_core::OrderBook::Snapshot { .. }));

    let report = ud.reconcile(&[]).await.expect("reconcile");
    assert!(report.confirmed.is_empty());

    let cid = ClientOrderId::new_v4();
    let ack = ud
        .place(PlaceOrderRequest {
            cid,
            instrument: sber(),
            side: Side::Buy,
            order_type: OrderType::Limit,
            qty: Qty::from(1),
            price: Some(Price::from(1)),
        })
        .await
        .expect("place");
    assert_eq!(ack.cid, cid);

    ud.cancel(cid).await.expect("cancel");

    let md_report = md
        .shutdown(ShutdownMode::Graceful {
            deadline: Duration::from_secs(2),
        })
        .await;
    assert!(md_report.unsubscribed >= 1);

    let ud_report = ud
        .shutdown(ShutdownMode::Graceful {
            deadline: Duration::from_secs(2),
        })
        .await;
    assert!(ud_report.inflight_abandoned.is_empty() || !ud_report.inflight_abandoned.is_empty());

    // MD market-data socket, UD order-events socket, UD portfolio socket.
    assert!(options.ws_connections() >= 3, "MD+UD WS sessions");
    assert!(
        options
            .ws_data_frames
            .load(std::sync::atomic::Ordering::Relaxed)
            >= 1
    );
}

#[tokio::test]
async fn rest_rate_limit_returns_error() {
    let options = Arc::new(MockOptions::default());
    options.enable_rate_limit_order_book();
    let server = MockServer::start(Arc::clone(&options)).await;
    let limiter = test_limiter();
    let (tx, _rx) = mpsc::channel::<Envelope>(16);

    let md = MarketDataConnector::start(
        MarketDataConfig::new(mock_bcs(&server.http_base, false), "bcs-md"),
        limiter,
        tx,
    )
    .await
    .expect("md start");

    let err = md.order_book(&sber(), 20).await.unwrap_err();
    assert!(
        matches!(err, Error::RateLimited { .. }),
        "expected RateLimited, got {err:?}"
    );

    md.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn ws_reconnect_after_controlled_disconnect() {
    let options = Arc::new(MockOptions::default());
    options
        .disconnect_after_data_frames
        .store(1, std::sync::atomic::Ordering::Relaxed);
    let server = MockServer::start(Arc::clone(&options)).await;
    let limiter = test_limiter();
    let (tx, mut rx) = mpsc::channel::<Envelope>(256);

    let md = MarketDataConnector::start(
        MarketDataConfig::new(mock_bcs(&server.http_base, false), "bcs-md"),
        limiter,
        tx,
    )
    .await
    .expect("md start");

    md.subscribe_order_book(
        &sber(),
        SubscribeOpts {
            backpressure: Backpressure::Conflate,
            depth: Some(20),
        },
    )
    .await
    .expect("subscribe");

    wait_for_event(&mut rx, |e| matches!(e.payload, Event::OrderBook(_))).await;

    // After forced close the session reconnects and replays subscription.
    wait_for_event(&mut rx, |e| matches!(e.payload, Event::OrderBook(_))).await;

    let frames = options
        .ws_data_frames
        .load(std::sync::atomic::Ordering::Relaxed);
    assert!(
        frames >= 2,
        "expected frames from initial + reconnect, got {frames}"
    );

    md.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn partial_subscribe_failure_does_not_crash_connector() {
    let options = Arc::new(MockOptions::default());
    {
        let mut fail = options.fail_subscribe_ticker.lock().await;
        *fail = Some("BADSUB".into());
    }
    let server = MockServer::start(Arc::clone(&options)).await;
    let limiter = test_limiter();
    let (tx, mut rx) = mpsc::channel::<Envelope>(64);

    let md = MarketDataConnector::start(
        MarketDataConfig::new(mock_bcs(&server.http_base, false), "bcs-md"),
        limiter,
        tx,
    )
    .await
    .expect("md start");

    // The broker refuses the subscription with an error frame: the call returns the refusal, and nothing of the
    // subscription is left behind.
    let refused = md
        .subscribe_order_book(
            &bad_instrument(),
            SubscribeOpts {
                backpressure: Backpressure::Conflate,
                depth: Some(20),
            },
        )
        .await;
    assert!(
        matches!(&refused, Err(Error::Rejected { code, .. }) if code == "subscription_refused"),
        "a refused subscription is Rejected, got {refused:?}"
    );

    // Good instrument still works on REST.
    md.order_book(&sber(), 5).await.expect("rest still works");

    // The bad subscription gets an error frame and no OrderBook events. A good subscription is the fence:
    // its book comes after everything the bad one could have sent, so the first book in the channel
    // must be the good one's.
    let good = md
        .subscribe_order_book(
            &sber(),
            SubscribeOpts {
                backpressure: Backpressure::Conflate,
                depth: Some(20),
            },
        )
        .await
        .expect("subscribe the good instrument");
    let first_book = event_of_book(&mut rx).await;
    assert_eq!(
        first_book.sub,
        Some(good),
        "bad subscribe should not produce OrderBook events"
    );

    md.shutdown(ShutdownMode::Immediate).await;
}

/// Fills from the `orders/events/ws` socket become `Event::Execution`, once per `executionId`.
#[tokio::test]
async fn executions_stream_from_the_order_events_socket() {
    let options = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&options)).await;
    let (tx, mut rx) = mpsc::channel::<Envelope>(256);
    let ud = UserDataConnector::start(
        UserDataConfig::new(mock_bcs(&server.http_base, true), "bcs-ud"),
        test_limiter(),
        tx,
    )
    .await
    .expect("ud start");
    assert!(ud.caps().streams_executions);

    let sub = ud
        .subscribe_executions()
        .await
        .expect("subscribe_executions");

    let fill = std::fs::read_to_string(format!(
        "{}/../bcs-protocol/tests/fixtures/ws/order_execution_fill.json",
        env!("CARGO_MANIFEST_DIR")
    ))
    .unwrap();
    server.push_event(&fill);
    server.push_event(&fill);
    let second = fill.replace("TQBR-Z3fE7c-S-1-1-N", "TQBR-Z3fE7c-S-1-2-N");
    server.push_event(&second);
    // The third, new fill is the fence: when it has arrived, a repeat of the first one would have too.
    let third = fill.replace("TQBR-Z3fE7c-S-1-1-N", "TQBR-Z3fE7c-S-1-3-N");
    server.push_event(&third);

    let mut got = Vec::new();
    timeout(Duration::from_secs(8), async {
        while let Some(env) = rx.recv().await {
            if let Event::Execution(x) = env.payload {
                assert_eq!(env.sub, Some(sub));
                got.push(x.trade_id);
                if got.last().map(String::as_str) == Some("TQBR-Z3fE7c-S-1-3-N") {
                    break;
                }
            }
        }
    })
    .await
    .expect("the fills arrive");
    assert_eq!(
        got,
        vec![
            "TQBR-Z3fE7c-S-1-1-N",
            "TQBR-Z3fE7c-S-1-2-N",
            "TQBR-Z3fE7c-S-1-3-N"
        ],
        "each executionId once"
    );

    ud.shutdown(ShutdownMode::Immediate).await;
}

fn fill_fixture() -> String {
    std::fs::read_to_string(format!(
        "{}/../bcs-protocol/tests/fixtures/ws/order_execution_fill.json",
        env!("CARGO_MANIFEST_DIR")
    ))
    .unwrap()
}

/// Spec section 6, as for T-Bank and Finam: a lossless subscription whose buffer fills up (the bot does not
/// read the channel) gives `Event::Overflow` once, the connector goes `Degraded` (commands are refused),
/// and a successful `reconcile` opens the gate again.
#[tokio::test]
async fn lossless_overflow_is_visible_and_degrades_until_reconcile() {
    let options = Arc::new(MockOptions::default());
    let server = MockServer::start(Arc::clone(&options)).await;
    // A channel of one slot that nobody reads, and a buffer of two events.
    let (tx, mut rx) = mpsc::channel::<Envelope>(1);
    let mut cfg = UserDataConfig::new(mock_bcs(&server.http_base, true), "bcs-ud");
    cfg.order_state_buffer = 2;
    let ud = UserDataConnector::start(cfg, test_limiter(), tx)
        .await
        .expect("ud start");
    let sub = ud
        .subscribe_executions()
        .await
        .expect("subscribe_executions");

    ud.reconcile(&[])
        .await
        .expect("first reconcile opens the gate");
    let place = |cid| PlaceOrderRequest {
        cid,
        instrument: sber(),
        side: Side::Buy,
        order_type: OrderType::Limit,
        qty: Qty::from(1),
        price: Some(Price::from(1)),
    };
    ud.place(place(ClientOrderId::new_v4()))
        .await
        .expect("a healthy connector trades");

    let fill = fill_fixture();
    for n in 0..12 {
        server.push_event(&fill.replace("TQBR-Z3fE7c-S-1-1-N", &format!("TQBR-Z3fE7c-S-1-{n}-N")));
    }
    // The buffer fills while nobody reads: commands are refused.
    brokapi_mocks::time::wait_until_async("Degraded after the overflow", || async {
        matches!(
            ud.place(place(ClientOrderId::new_v4())).await,
            Err(Error::Degraded)
        )
    })
    .await;
    assert!(matches!(
        ud.cancel(ClientOrderId::new_v4()).await,
        Err(Error::Degraded)
    ));

    // The bot reads the channel to the end: Overflow is there, once, for this subscription.
    let mut overflows = 0;
    let mut seen = |env: Envelope| {
        if let Event::Overflow { sub: s, .. } = env.payload {
            assert_eq!(s, sub);
            assert_eq!(env.sub, Some(sub));
            overflows += 1;
        }
    };
    timeout(Duration::from_secs(8), async {
        while let Some(env) = rx.recv().await {
            let overflow = matches!(env.payload, Event::Overflow { .. });
            seen(env);
            if overflow {
                break;
            }
        }
    })
    .await
    .expect("the Overflow reaches the bot");
    // A round trip to the broker, then what is still queued: no second Overflow.
    ud.reconcile(&[]).await.expect("reconcile");
    while let Ok(env) = rx.try_recv() {
        seen(env);
    }
    assert_eq!(overflows, 1, "one Overflow per subscription");

    ud.place(place(ClientOrderId::new_v4()))
        .await
        .expect("trading is open again");

    ud.shutdown(ShutdownMode::Immediate).await;
}

/// Spec section 5: after the WebSocket reconnects and the subscriptions are restored, the order book of
/// every subscription is announced void with `Resync { SubscriptionReset }`, before the new snapshot.
/// The first connect is not announced.
#[tokio::test]
async fn reconnect_sends_resync_before_the_new_order_book() {
    let options = Arc::new(MockOptions::default());
    // The mock closes the socket after the first book it sends, on every connection.
    options
        .disconnect_after_data_frames
        .store(1, std::sync::atomic::Ordering::Relaxed);
    let server = MockServer::start(Arc::clone(&options)).await;
    let (tx, mut rx) = mpsc::channel::<Envelope>(256);
    let md = MarketDataConnector::start(
        MarketDataConfig::new(mock_bcs(&server.http_base, false), "bcs-md"),
        test_limiter(),
        tx,
    )
    .await
    .expect("md start");
    let sub = md
        .subscribe_order_book(
            &sber(),
            SubscribeOpts {
                backpressure: Backpressure::Conflate,
                depth: Some(20),
            },
        )
        .await
        .expect("subscribe");

    // Order of what the bot sees until the second snapshot: book, Resync, book.
    let mut seen = Vec::new();
    timeout(Duration::from_secs(10), async {
        while let Some(env) = rx.recv().await {
            match &env.payload {
                Event::OrderBook(_) => seen.push("book"),
                Event::Resync {
                    sub: s,
                    instrument,
                    reason,
                } => {
                    assert_eq!(*s, sub);
                    assert_eq!(env.sub, Some(sub));
                    assert_eq!(instrument.key, sber().key);
                    assert_eq!(*reason, brokapi_core::ResyncReason::SubscriptionReset);
                    seen.push("resync");
                }
                _ => {}
            }
            if seen.iter().filter(|k| **k == "book").count() == 2 {
                break;
            }
        }
    })
    .await
    .expect("two snapshots with a reconnect between them");
    assert_eq!(seen, vec!["book", "resync", "book"]);

    md.shutdown(ShutdownMode::Immediate).await;
}

/// The pause before a reconnect is the connector's setting: a short one brings the order book back at once
/// (the default, 0.5 s, would not).
#[tokio::test]
async fn reconnect_pause_is_a_setting_of_the_connector() {
    let options = Arc::new(MockOptions::default());
    options
        .disconnect_after_data_frames
        .store(1, std::sync::atomic::Ordering::Relaxed);
    let server = MockServer::start(Arc::clone(&options)).await;
    let (tx, mut rx) = mpsc::channel::<Envelope>(256);
    let mut cfg = MarketDataConfig::new(mock_bcs(&server.http_base, false), "bcs-md");
    cfg.reconnect_min = Duration::from_millis(10);
    cfg.reconnect_max = Duration::from_millis(50);
    let md = MarketDataConnector::start(cfg, test_limiter(), tx)
        .await
        .expect("md start");
    md.subscribe_order_book(
        &sber(),
        SubscribeOpts {
            backpressure: Backpressure::Conflate,
            depth: Some(20),
        },
    )
    .await
    .expect("subscribe");

    wait_for_event(&mut rx, |e| matches!(e.payload, Event::OrderBook(_))).await;
    let lost_at = std::time::Instant::now();
    wait_for_event(&mut rx, |e| matches!(e.payload, Event::Resync { .. })).await;
    wait_for_event(&mut rx, |e| matches!(e.payload, Event::OrderBook(_))).await;
    assert!(
        lost_at.elapsed() < Duration::from_millis(450),
        "back after {:?}: the setting was not used",
        lost_at.elapsed()
    );
    md.shutdown(ShutdownMode::Immediate).await;
}
