//! What the gateway promises, one promise per test, on the imitation of BCS (the road is the same for every broker:
//! `conformance.rs` runs the shared scenarios on all four).

mod common;

use std::time::Duration;

use brokapi::core::{ClientOrderId, Error, OrderBookSource, OrderExecutor, SubscribeOpts};
use brokapi_client::{Broker, Limiter};
use brokapi_gateway::{
    Address, ConnectOptions, Gateway, GatewayConfig, GatewayError, GatewayState, Listen,
    RemoteClient, RemoteEvent, Resume, RingConfig, pb,
};
use common::{Mock, Rig, limit_order, roomy_limits};
use tokio::time::timeout;

/// A real-time limit for a wait that must end: a test that hangs is a failed test.
const LIMIT: Duration = Duration::from_secs(20);

async fn bot(rig: &Rig, name: &str) -> RemoteClient {
    RemoteClient::connect_dir(rig.dir.path(), name)
        .await
        .expect("a bot connects")
}

/// One place and one cancel: a few events for whoever listens to the order states.
async fn cycle(bot: &RemoteClient, rig: &Rig) -> ClientOrderId {
    let request = limit_order(rig, rig.order_price());
    let cid = request.cid;
    bot.place(request).await.expect("place");
    bot.cancel(cid).await.expect("cancel");
    cid
}

/// Reads the stream until the event with this number, returning all numbers seen.
async fn read_to(stream: &mut brokapi_gateway::EventStream, last: u64) -> Vec<u64> {
    let mut seen = Vec::new();
    while seen.last().copied().unwrap_or(0) < last {
        match timeout(LIMIT, stream.next()).await.expect("an event comes") {
            Some(Ok(RemoteEvent::Event { seq, .. })) => seen.push(seq),
            other => panic!("expected an event, got {other:?}"),
        }
    }
    seen
}

fn contiguous(seqs: &[u64]) -> bool {
    seqs.windows(2).all(|w| w[1] == w[0] + 1)
}

#[tokio::test]
async fn two_bots_share_one_limiter() {
    let mock = Mock::start(Broker::Bcs).await;
    let dir = tempfile::tempdir().unwrap();
    // The placements of the whole gateway: two, and no refill in the time of the test.
    let mut limits = brokapi::bcs::limits::default_limits();
    let place = limits
        .buckets
        .get_mut("place")
        .expect("the bucket of placements");
    place.burst = 2;
    place.rate = 1;
    place.per = Duration::from_secs(3600);
    let limiter = Limiter::with_limits(Broker::Bcs, &limits);
    let gateway = Gateway::start(
        GatewayConfig::new("bcs", dir.path()),
        mock.config(Broker::Bcs),
        Some(limiter),
    )
    .await
    .unwrap();
    gateway.ready().await;
    let alpha = RemoteClient::connect_dir(dir.path(), "alpha")
        .await
        .unwrap();
    let beta = RemoteClient::connect_dir(dir.path(), "beta").await.unwrap();
    alpha
        .place(common::bcs_order("1.0"))
        .await
        .expect("the first placement");
    beta.place(common::bcs_order("1.0"))
        .await
        .expect("the second placement");
    // Each bot placed one, and now neither has anything left: the budget is the gateway's, not the bot's.
    for bot in [&alpha, &beta] {
        match bot.place(common::bcs_order("1.0")).await {
            Err(Error::RateLimited { retry_after }) => assert!(retry_after > Duration::ZERO),
            other => panic!("expected RateLimited, got {other:?}"),
        }
    }
    // Only the two that went out reached the broker.
    assert_eq!(
        mock.bcs()
            .1
            .order_commands
            .load(std::sync::atomic::Ordering::SeqCst),
        2
    );
    let health = gateway.health();
    let names: Vec<_> = health.bots.iter().map(|b| b.name.as_str()).collect();
    assert_eq!(names, ["alpha", "beta"]);
    gateway
        .shutdown(brokapi::core::ShutdownMode::Immediate)
        .await;
}

#[tokio::test]
async fn a_slow_bot_is_cut_off_and_does_not_slow_the_others() {
    let mock = Mock::start(Broker::Bcs).await;
    let dir = tempfile::tempdir().unwrap();
    let mut config = GatewayConfig::new("bcs", dir.path());
    config.bot_buffer = 4;
    let gateway = Gateway::start(
        config,
        mock.config(Broker::Bcs),
        Some(roomy_limits(Broker::Bcs)),
    )
    .await
    .unwrap();
    gateway.ready().await;
    let driver = RemoteClient::connect_dir(dir.path(), "driver")
        .await
        .unwrap();
    driver.subscribe_order_state().await.unwrap();

    // The window of the slow bot is small: the gateway may send it a few events and no more while it does not read.
    let slow = RemoteClient::connect_dir_with(
        dir.path(),
        "slow",
        ConnectOptions {
            http2_window: Some(2048),
        },
    )
    .await
    .unwrap();
    let mut slow_stream = slow.events(None, false).await.unwrap();
    let fast = RemoteClient::connect_dir(dir.path(), "fast").await.unwrap();
    let mut fast_stream = fast.events(None, false).await.unwrap();
    let first_for_fast = fast_stream.first_live_seq();
    // The fast bot reads all the time. The slow bot does not read at all.
    let reader = tokio::spawn(async move {
        let mut seen = Vec::new();
        while let Some(Ok(RemoteEvent::Event { seq, .. })) = fast_stream.next().await {
            seen.push(seq);
        }
        seen
    });

    // Events until the gateway has cut the slow bot off. The gateway never waits for the slow bot, so this ends.
    let mut cycles = 0;
    while gateway
        .health()
        .bots
        .iter()
        .find(|b| b.name == "slow")
        .map_or(0, |b| b.lagged)
        == 0
    {
        common::bcs_cycle(&driver).await;
        cycles += 1;
        assert!(cycles < 3000, "the slow bot was never cut off");
    }
    let last = gateway.health().last_seq;
    assert!(last > 4);

    // The slow bot gets what was in its buffer, then the word `Lagged`, and the stream ends.
    let mut delivered = Vec::new();
    let lagged_at = loop {
        match timeout(LIMIT, slow_stream.next()).await.expect("a frame") {
            Some(Ok(RemoteEvent::Event { seq, .. })) => delivered.push(seq),
            Some(Ok(RemoteEvent::Lagged { last_seq })) => break last_seq,
            other => panic!("unexpected {other:?}"),
        }
    };
    assert!(timeout(LIMIT, slow_stream.next()).await.unwrap().is_none());
    assert!(contiguous(&delivered));
    assert_eq!(delivered.last().copied(), Some(lagged_at));
    assert!(lagged_at < last, "it missed events");

    // It comes back with the last number it got and finds the rest in the ring.
    let mut again = slow
        .events(
            Some(Resume {
                epoch: gateway.health().epoch,
                last_seq: lagged_at,
            }),
            false,
        )
        .await
        .unwrap();
    let rest = read_to(&mut again, last).await;
    assert_eq!(rest.first().copied(), Some(lagged_at + 1));
    assert!(contiguous(&rest));

    // The fast bot has everything, in order, and was never cut.
    let health = gateway.health();
    assert_eq!(
        health
            .bots
            .iter()
            .find(|b| b.name == "fast")
            .unwrap()
            .lagged,
        0
    );
    drop(driver);
    drop(fast);
    drop(slow);
    gateway
        .shutdown(brokapi::core::ShutdownMode::Immediate)
        .await;
    let seen = timeout(LIMIT, reader).await.unwrap().unwrap();
    assert_eq!(seen.first().copied(), Some(first_for_fast));
    assert_eq!(seen.last().copied(), Some(last));
    assert!(contiguous(&seen), "the fast bot missed an event");
    assert_eq!(seen.len() as u64, last - first_for_fast + 1);
}

#[tokio::test]
async fn a_bot_that_reconnects_gets_what_it_missed() {
    let rig = Rig::start(Broker::Bcs).await;
    let watcher = bot(&rig, "watcher").await;
    let driver = bot(&rig, "driver").await;
    driver.subscribe_order_state().await.unwrap();

    let mut first = watcher.events(None, false).await.unwrap();
    cycle(&driver, &rig).await;
    let here = rig.gateway.health().last_seq;
    let seen = read_to(&mut first, here).await;
    assert!(contiguous(&seen));
    let resume = first.resume_point();
    assert_eq!(resume.last_seq, here);
    // The bot is gone (its process died, the connection broke) while the gateway goes on.
    drop(first);
    cycle(&driver, &rig).await;
    cycle(&driver, &rig).await;
    let total = rig.gateway.health().last_seq;
    assert!(total > here);

    let mut second = watcher.events(Some(resume), false).await.unwrap();
    assert_eq!(second.epoch(), resume.epoch);
    let missed = read_to(&mut second, total).await;
    assert_eq!(
        missed.first().copied(),
        Some(here + 1),
        "continues where it left off"
    );
    assert!(contiguous(&missed));
    assert_eq!(missed.last().copied(), Some(total));
    // And live events follow without a hole.
    cycle(&driver, &rig).await;
    let after = rig.gateway.health().last_seq;
    let live = read_to(&mut second, after).await;
    assert_eq!(live.first().copied(), Some(total + 1));
    rig.stop().await;
}

#[tokio::test]
async fn a_number_the_ring_has_dropped_is_a_gap() {
    let mock = Mock::start(Broker::Bcs).await;
    let dir = tempfile::tempdir().unwrap();
    let mut config = GatewayConfig::new("bcs", dir.path());
    config.ring = RingConfig {
        max_events: 3,
        max_age: Duration::from_secs(3600),
    };
    let gateway = Gateway::start(
        config,
        mock.config(Broker::Bcs),
        Some(roomy_limits(Broker::Bcs)),
    )
    .await
    .unwrap();
    gateway.ready().await;
    let driver = RemoteClient::connect_dir(dir.path(), "driver")
        .await
        .unwrap();
    driver.subscribe_order_state().await.unwrap();
    while gateway.health().last_seq < 10 {
        common::bcs_cycle(&driver).await;
    }
    let health = gateway.health();

    // A bot that last saw event 2 asks for the rest: the ring keeps only the last three.
    let mut stream = driver
        .events(
            Some(Resume {
                epoch: health.epoch,
                last_seq: 2,
            }),
            false,
        )
        .await
        .unwrap();
    match timeout(LIMIT, stream.next()).await.unwrap() {
        Some(Ok(RemoteEvent::Gap(gap))) => {
            assert_eq!(gap.reason, pb::gap::Reason::TooOld as i32);
            assert_eq!(gap.requested_after, 2);
            assert_eq!(gap.first_available, health.last_seq - 2);
        }
        other => panic!("expected a gap, got {other:?}"),
    }
    // The stream goes on from now; the bot's resume point is moved to it.
    assert_eq!(stream.resume_point().last_seq, health.last_seq);
    common::bcs_cycle(&driver).await;
    let live = read_to(&mut stream, gateway.health().last_seq).await;
    assert_eq!(live.first().copied(), Some(health.last_seq + 1));

    // A number from another run of the gateway is a gap too.
    let mut other_run = driver
        .events(
            Some(Resume {
                epoch: health.epoch + 2,
                last_seq: 5,
            }),
            false,
        )
        .await
        .unwrap();
    match timeout(LIMIT, other_run.next()).await.unwrap() {
        Some(Ok(RemoteEvent::Gap(gap))) => {
            assert_eq!(gap.reason, pb::gap::Reason::NewEpoch as i32)
        }
        other => panic!("expected a gap, got {other:?}"),
    }
    gateway
        .shutdown(brokapi::core::ShutdownMode::Immediate)
        .await;
}

#[tokio::test]
async fn a_restart_with_an_open_order_compares_the_journal_before_taking_commands() {
    let mock = Mock::start(Broker::Bcs).await;
    let (server, opts) = mock.bcs();
    let dir = tempfile::tempdir().unwrap();

    // Run 1: a bot places an order and the process dies.
    let first = Gateway::start(
        GatewayConfig::new("bcs", dir.path()),
        mock.config(Broker::Bcs),
        Some(roomy_limits(Broker::Bcs)),
    )
    .await
    .unwrap();
    first.ready().await;
    let alpha = RemoteClient::connect_dir(dir.path(), "alpha")
        .await
        .unwrap();
    let request = common::bcs_order("1.0");
    let cid = request.cid;
    alpha.place(request).await.unwrap();
    assert_eq!(first.health().open_orders, 1);
    let epoch_before = first.health().epoch;
    first.kill().await;
    let commands_before = opts
        .order_commands
        .load(std::sync::atomic::Ordering::SeqCst);

    // Run 2: the budget of the orders service is spent, so the reconcile of the start cannot run yet. The gateway
    // listens, answers `Health`, and takes no command.
    let mut limits = brokapi::bcs::limits::default_limits();
    let orders = limits.buckets.get_mut("orders").unwrap();
    orders.burst = 3;
    orders.rate = 1000;
    orders.per = Duration::from_secs(3600);
    let limiter = Limiter::with_limits(Broker::Bcs, &limits);
    for _ in 0..3 {
        limiter
            .shared()
            .acquire("SearchOrders", brokapi::runtime::now())
            .expect("the budget is there");
    }
    let second = Gateway::start(
        GatewayConfig::new("bcs", dir.path()),
        mock.config(Broker::Bcs),
        Some(limiter),
    )
    .await
    .unwrap();
    assert_eq!(second.state(), GatewayState::Starting);
    brokapi_mocks::time::wait_until("the first reconcile attempt failed", || {
        second.health().detail.contains("rate limited")
    })
    .await;
    let beta = RemoteClient::connect_dir(dir.path(), "beta").await.unwrap();
    let health = beta.health().await.unwrap();
    assert_eq!(health.state, pb::GatewayState::Starting as i32);
    assert_eq!(health.open_orders, 1, "the journal of run 1 is there");
    assert_ne!(health.epoch, epoch_before);
    // A command is refused, and nothing reached the broker.
    let refused = beta.place(common::bcs_order("1.0")).await;
    assert!(
        matches!(&refused, Err(Error::Transport(m)) if m.to_string().contains("starting")),
        "{refused:?}"
    );
    assert!(matches!(
        beta.active_orders().await,
        Err(Error::Transport(_))
    ));
    assert_eq!(
        opts.order_commands
            .load(std::sync::atomic::Ordering::SeqCst),
        commands_before
    );

    // The budget comes back with time; the gateway compares its journal with the broker and opens.
    brokapi_mocks::time::jump(Duration::from_secs(3600)).await;
    timeout(LIMIT, second.ready())
        .await
        .expect("the gateway becomes ready");
    assert_eq!(second.state(), GatewayState::Ready);
    // The order of run 1 is still active at the broker: it is confirmed, and the journal keeps it.
    assert_eq!(second.health().open_orders, 1);
    assert_eq!(second.health().detail, "");
    let state = beta.order_state(cid).await.unwrap();
    assert_eq!(state.cid, cid);
    beta.cancel(cid).await.unwrap();
    let _ = server;
    second
        .shutdown(brokapi::core::ShutdownMode::Immediate)
        .await;
}

#[tokio::test]
async fn an_order_that_ended_while_the_gateway_was_down_is_closed_in_the_journal() {
    let mock = Mock::start(Broker::Bcs).await;
    let dir = tempfile::tempdir().unwrap();
    let first = Gateway::start(
        GatewayConfig::new("bcs", dir.path()),
        mock.config(Broker::Bcs),
        Some(roomy_limits(Broker::Bcs)),
    )
    .await
    .unwrap();
    first.ready().await;
    let alpha = RemoteClient::connect_dir(dir.path(), "alpha")
        .await
        .unwrap();
    let request = common::bcs_order("100.0");
    let cid = request.cid;
    alpha.place(request).await.unwrap();
    assert_eq!(first.health().open_orders, 1);
    first.kill().await;
    // While nobody listens the order is executed.
    mock.fill(cid).await;

    let second = Gateway::start(
        GatewayConfig::new("bcs", dir.path()),
        mock.config(Broker::Bcs),
        Some(roomy_limits(Broker::Bcs)),
    )
    .await
    .unwrap();
    timeout(LIMIT, second.ready()).await.unwrap();
    assert_eq!(
        second.health().open_orders,
        0,
        "the journal was compared with the broker"
    );
    second
        .shutdown(brokapi::core::ShutdownMode::Immediate)
        .await;
}

#[tokio::test]
async fn a_bot_can_ask_for_the_events_of_its_own_orders_only() {
    let rig = Rig::start(Broker::Bcs).await;
    let alpha = bot(&rig, "alpha").await;
    let beta = bot(&rig, "beta").await;
    alpha.subscribe_order_state().await.unwrap();
    let mut alpha_stream = alpha.events(None, true).await.unwrap();
    let mut beta_stream = beta.events(None, true).await.unwrap();
    let mut everyone = beta.events(None, false).await.unwrap();

    let cid = cycle(&alpha, &rig).await;
    // A marker that is not about orders and reaches everybody, after the orders.
    alpha
        .subscribe_order_book(&common::sber_bcs(), SubscribeOpts::conflate())
        .await
        .unwrap();
    let marker = |event: &brokapi::core::Envelope| {
        matches!(event.payload, brokapi::core::Event::OrderBook(_))
    };
    async fn collect(
        stream: &mut brokapi_gateway::EventStream,
        marker: impl Fn(&brokapi::core::Envelope) -> bool,
    ) -> Vec<brokapi::core::Envelope> {
        let mut seen = Vec::new();
        loop {
            match timeout(LIMIT, stream.next()).await.expect("an event") {
                Some(Ok(RemoteEvent::Event { envelope, .. })) => {
                    let done = marker(&envelope);
                    seen.push(envelope);
                    if done {
                        return seen;
                    }
                }
                other => panic!("unexpected {other:?}"),
            }
        }
    }
    let is_order = |e: &brokapi::core::Envelope| matches!(&e.payload, brokapi::core::Event::Order(o) if o.cid == cid);
    let mine = collect(&mut alpha_stream, marker).await;
    let others = collect(&mut beta_stream, marker).await;
    let all = collect(&mut everyone, marker).await;
    assert!(mine.iter().any(is_order), "the owner sees its order");
    assert!(!others.iter().any(is_order), "another bot does not");
    assert!(
        all.iter().any(is_order),
        "a bot that did not ask for the filter sees everything"
    );
    rig.stop().await;
}

#[tokio::test]
async fn a_wrong_key_is_refused() {
    let rig = Rig::start(Broker::Bcs).await;
    let intruder = RemoteClient::connect(rig.gateway.address(), "not-the-key", "intruder")
        .await
        .unwrap();
    match intruder.health().await {
        Err(Error::Config(m)) => assert!(m.to_string().contains("access"), "{m}"),
        other => panic!("expected a refusal, got {other:?}"),
    }
    // Nothing was done on its behalf: it is not even a bot of the gateway.
    assert!(
        rig.gateway
            .health()
            .bots
            .iter()
            .all(|b| b.name != "intruder")
    );
    // A bot name that cannot be a name is refused before any call.
    let bad =
        RemoteClient::connect(rig.gateway.address(), rig.gateway.access_key(), "bad name").await;
    assert!(matches!(bad, Err(Error::Config(_))));
    rig.stop().await;
}

#[cfg(unix)]
#[tokio::test]
async fn the_key_and_the_socket_are_private() {
    use std::os::unix::fs::PermissionsExt;
    let rig = Rig::start(Broker::Bcs).await;
    let mode = |name: &str| {
        std::fs::metadata(rig.dir.path().join(name))
            .unwrap()
            .permissions()
            .mode()
            & 0o777
    };
    assert_eq!(mode("access.key"), 0o600);
    assert_eq!(mode("gateway.sock"), 0o600);
    assert!(matches!(rig.gateway.address(), Address::Unix(_)));
    // The key in the file is the key the gateway checks.
    let text = std::fs::read_to_string(rig.dir.path().join("access.key")).unwrap();
    assert_eq!(text.trim(), rig.gateway.access_key());
    rig.stop().await;
}

#[tokio::test]
async fn an_address_that_is_not_this_machine_is_refused() {
    let mock = Mock::start(Broker::Bcs).await;
    for listen in ["0.0.0.0:0", "192.168.1.5:7000", "[::]:0"] {
        let dir = tempfile::tempdir().unwrap();
        let mut config = GatewayConfig::new("bcs", dir.path());
        config.listen = Listen::Tcp(listen.parse().unwrap());
        let started = Gateway::start(config, mock.config(Broker::Bcs), None).await;
        assert!(
            matches!(&started, Err(GatewayError::NotLocal(a)) if a == listen),
            "{listen}: {:?}",
            started.err()
        );
    }
}

#[tokio::test]
async fn the_loopback_port_works_like_the_socket() {
    let mock = Mock::start(Broker::Bcs).await;
    let dir = tempfile::tempdir().unwrap();
    let mut config = GatewayConfig::new("bcs", dir.path());
    config.listen = Listen::Tcp("127.0.0.1:0".parse().unwrap());
    let gateway = Gateway::start(
        config,
        mock.config(Broker::Bcs),
        Some(roomy_limits(Broker::Bcs)),
    )
    .await
    .unwrap();
    gateway.ready().await;
    assert!(matches!(gateway.address(), Address::Tcp(a) if a.ip().is_loopback() && a.port() != 0));
    let bot = RemoteClient::connect_dir(dir.path(), "tcp-bot")
        .await
        .unwrap();
    let book = bot
        .order_book(&common::sber_bcs(), 5)
        .await
        .expect("a call over the port");
    assert!(matches!(book, brokapi::core::OrderBook::Snapshot { .. }));
    gateway
        .shutdown(brokapi::core::ShutdownMode::Immediate)
        .await;
}

#[cfg(unix)]
#[tokio::test]
async fn a_second_gateway_on_the_same_socket_is_refused() {
    let rig = Rig::start(Broker::Bcs).await;
    let other_dir = rig.dir.path().join("second");
    let mut config = GatewayConfig::new("bcs2", &other_dir);
    config.listen = Listen::Unix(rig.dir.path().join("gateway.sock"));
    let mock = Mock::start(Broker::Bcs).await;
    let started = Gateway::start(config, mock.config(Broker::Bcs), None).await;
    assert!(
        matches!(started, Err(GatewayError::AlreadyRunning(_))),
        "{:?}",
        started.err()
    );
    // The first one is untouched.
    assert!(rig.bot.health().await.is_ok());
    rig.stop().await;
}

#[tokio::test]
async fn a_command_runs_to_its_end_when_the_bot_stops_waiting() {
    let rig = Rig::start(Broker::Bcs).await;
    let (_, opts) = rig.mock.bcs();
    // The broker holds the order; the bot gives up at once. The order is placed all the same, and its id is in the journal.
    opts.order_delay_ms
        .store(5_000, std::sync::atomic::Ordering::SeqCst);
    let request = limit_order(&rig, rig.order_price());
    let cid = request.cid;
    let waited = timeout(Duration::from_millis(1), rig.bot.place(request)).await;
    assert!(waited.is_err(), "the bot stopped waiting");
    brokapi_mocks::time::wait_until("the order reached the broker", || {
        opts.order_commands
            .load(std::sync::atomic::Ordering::SeqCst)
            == 1
    })
    .await;
    brokapi_mocks::time::jump(Duration::from_secs(6)).await;
    opts.order_delay_ms
        .store(0, std::sync::atomic::Ordering::SeqCst);
    brokapi_mocks::time::wait_until_async("the order is active", || async {
        rig.bot
            .active_orders()
            .await
            .map(|orders| orders.iter().any(|o| o.cid == cid))
            .unwrap_or(false)
    })
    .await;
    assert_eq!(rig.gateway.health().open_orders, 1);
    rig.stop().await;
}

#[tokio::test]
async fn health_says_who_is_connected() {
    let rig = Rig::start(Broker::Bcs).await;
    let a = bot(&rig, "alpha").await;
    let _stream = a.events(None, false).await.unwrap();
    let health = rig.bot.health().await.unwrap();
    assert_eq!(health.state, pb::GatewayState::Ready as i32);
    assert_eq!(health.broker, "bcs");
    assert_eq!(health.name, "bcs");
    let alpha = health
        .bots
        .iter()
        .find(|b| b.name == "alpha")
        .expect("alpha is listed");
    assert!(alpha.streaming);
    assert!(alpha.calls >= 1);
    let conformance = health
        .bots
        .iter()
        .find(|b| b.name == "conformance")
        .unwrap();
    assert!(conformance.streaming, "the stream of the rig's own bot");
    rig.stop().await;
}

#[tokio::test]
async fn raw_reaches_the_api_of_the_broker() {
    let rig = Rig::start(Broker::Bcs).await;
    // A method of the broker by its name: the reply is the broker's own JSON.
    let reply = rig
        .bot
        .raw("GetPortfolio", &serde_json::json!({}), false)
        .await;
    assert!(reply.is_ok(), "{reply:?}");
    let refused = rig
        .bot
        .raw("PUT /nothing", &serde_json::Value::Null, false)
        .await;
    assert!(matches!(refused, Err(Error::Config(_))), "{refused:?}");
    rig.stop().await;
}
