//! The events of a bot as a `Feed`, the guard of commands, and the calls by name on a `RemoteClient`: what the Python
//! module stands on, tested on the imitation of BCS.

mod common;

use std::time::Duration;

use brokapi::core::{
    ClientOrderId, Error, Event, MarketDataSource, OrderBookSource, OrderExecutor, OrderState,
    ShutdownMode, SubscribeOpts, Timeframe,
};
use brokapi_client::{Account, Broker, call_by_name, candle_pages_from_json};
use brokapi_gateway::{ConnectOptions, FeedItem, Gateway, GatewayConfig, Listen, RemoteClient, pb};
use common::{Mock, bcs_order, roomy_limits, sber_bcs};
use serde_json::json;
use tokio::time::timeout;

/// A real-time limit for a wait that must end: a test that hangs is a failed test.
const LIMIT: Duration = Duration::from_secs(20);

async fn gated(dir: &std::path::Path, name: &str) -> RemoteClient {
    RemoteClient::connect_dir_with(
        dir,
        name,
        ConnectOptions {
            trading_gate: true,
            ..ConnectOptions::default()
        },
    )
    .await
    .expect("a bot connects")
}

/// Reads the feed until an order with this cid reaches a final state.
async fn until_final(feed: &mut brokapi_gateway::Feed, cid: ClientOrderId) {
    loop {
        match timeout(LIMIT, feed.next()).await.expect("an event comes") {
            Some(FeedItem::Event(env)) => {
                if let Event::Order(order) = env.payload
                    && order.cid == cid
                    && matches!(order.state, OrderState::Cancelled { .. })
                {
                    return;
                }
            }
            other => panic!("expected an event, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn a_feed_hands_over_events_and_ends_when_the_client_closes() {
    let mock = Mock::start(Broker::Bcs).await;
    let dir = tempfile::tempdir().unwrap();
    let gateway = Gateway::start(
        GatewayConfig::new("bcs", dir.path()),
        mock.config(Broker::Bcs),
        Some(roomy_limits(Broker::Bcs)),
    )
    .await
    .unwrap();
    gateway.ready().await;
    let bot = gated(dir.path(), "bot").await;
    let mut feed = bot.feed(64, false).await.unwrap();
    bot.subscribe_order_state().await.unwrap();
    bot.reconcile(&[]).await.unwrap();

    let request = bcs_order("1.0");
    let cid = request.cid;
    bot.place(request).await.unwrap();
    bot.cancel(cid).await.unwrap();
    until_final(&mut feed, cid).await;

    // A closed client refuses what it is asked, and its feed ends once what it has is read.
    bot.close();
    assert!(matches!(
        bot.active_orders().await,
        Err(Error::ShuttingDown)
    ));
    while timeout(LIMIT, feed.next())
        .await
        .expect("the feed ends")
        .is_some()
    {}
    gateway.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn a_feed_reports_a_new_gateway_as_a_gap_and_the_guard_closes_commands_until_reconcile() {
    a_new_gateway_is_a_gap_and_the_guard_closes_commands(Listen::Auto).await;
}

/// On Windows the gateway listens at a port that is free, and the new process listens at another one: the bot has to find
/// it by the file `gateway.addr`, not by the address it first knew. A port is asked for explicitly here to have the same on
/// every system.
#[tokio::test]
async fn a_feed_finds_a_new_gateway_that_listens_at_another_port() {
    a_new_gateway_is_a_gap_and_the_guard_closes_commands(Listen::Tcp(
        "127.0.0.1:0".parse().unwrap(),
    ))
    .await;
}

async fn a_new_gateway_is_a_gap_and_the_guard_closes_commands(listen: Listen) {
    let mock = Mock::start(Broker::Bcs).await;
    let dir = tempfile::tempdir().unwrap();
    let start = || async {
        let mut config = GatewayConfig::new("bcs", dir.path());
        config.listen = listen.clone();
        let gateway = Gateway::start(
            config,
            mock.config(Broker::Bcs),
            Some(roomy_limits(Broker::Bcs)),
        )
        .await
        .unwrap();
        gateway.ready().await;
        gateway
    };
    let first = start().await;
    let bot = gated(dir.path(), "bot").await;
    let mut feed = bot.feed(64, false).await.unwrap();

    // Before the first reconcile the guard refuses, and nothing is sent.
    assert!(matches!(
        bot.place(bcs_order("1.0")).await,
        Err(Error::NotReconciled)
    ));
    assert_eq!(mock.order_commands(), 0);
    bot.subscribe_order_state().await.unwrap();
    bot.reconcile(&[]).await.unwrap();
    let request = bcs_order("1.0");
    let cid = request.cid;
    bot.place(request).await.unwrap();
    bot.cancel(cid).await.unwrap();
    until_final(&mut feed, cid).await;

    // The gateway dies and a new process takes its place: its numbers begin again.
    first.kill().await;
    let second = start().await;
    let gap = loop {
        match timeout(LIMIT, feed.next()).await.expect("the gap comes") {
            Some(FeedItem::Gap(gap)) => break gap,
            Some(FeedItem::Event(_)) => {}
            None => panic!("the feed ended"),
        }
    };
    assert_eq!(gap.reason, pb::gap::Reason::NewEpoch as i32);

    // Events were lost, so commands wait for a reconcile, as after an `Overflow` of a connector.
    assert!(matches!(
        bot.place(bcs_order("1.0")).await,
        Err(Error::Degraded)
    ));
    assert!(matches!(bot.cancel(cid).await, Err(Error::Degraded)));
    // The subscriptions were the old process's: subscribe again, reconcile, and the feed goes on.
    bot.subscribe_order_state().await.unwrap();
    bot.reconcile(&[]).await.unwrap();
    let request = bcs_order("1.0");
    let cid = request.cid;
    bot.place(request).await.unwrap();
    bot.cancel(cid).await.unwrap();
    until_final(&mut feed, cid).await;
    second.shutdown(ShutdownMode::Immediate).await;
}

/// The state directory was made again, so the new gateway has another key: the bot reads it again after the gateway
/// refuses the old one, and the calls go on.
#[tokio::test]
async fn a_bot_takes_the_new_key_of_a_gateway_that_was_started_anew() {
    let mock = Mock::start(Broker::Bcs).await;
    let dir = tempfile::tempdir().unwrap();
    let start = || async {
        let gateway = Gateway::start(
            GatewayConfig::new("bcs", dir.path()),
            mock.config(Broker::Bcs),
            Some(roomy_limits(Broker::Bcs)),
        )
        .await
        .unwrap();
        gateway.ready().await;
        gateway
    };
    let first = start().await;
    let bot = RemoteClient::connect_dir(dir.path(), "bot").await.unwrap();
    bot.health().await.expect("the first gateway answers");
    let old_key = std::fs::read_to_string(dir.path().join("access.key")).unwrap();
    first.kill().await;
    std::fs::remove_file(dir.path().join("access.key")).unwrap();
    let second = start().await;
    assert_ne!(
        std::fs::read_to_string(dir.path().join("access.key")).unwrap(),
        old_key
    );

    // The call that meets the new gateway first may fail (the old connection, the old key), the ones after it do not.
    let mut answered = false;
    for _ in 0..10 {
        if timeout(LIMIT, bot.health())
            .await
            .expect("an answer")
            .is_ok()
        {
            answered = true;
            break;
        }
    }
    assert!(answered, "the bot did not find the new gateway and its key");
    second.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn the_guard_is_off_unless_asked_for() {
    let mock = Mock::start(Broker::Bcs).await;
    let dir = tempfile::tempdir().unwrap();
    let gateway = Gateway::start(
        GatewayConfig::new("bcs", dir.path()),
        mock.config(Broker::Bcs),
        Some(roomy_limits(Broker::Bcs)),
    )
    .await
    .unwrap();
    gateway.ready().await;
    let bot = RemoteClient::connect_dir(dir.path(), "bot").await.unwrap();
    let request = bcs_order("1.0");
    let cid = request.cid;
    bot.place(request).await.expect("the gateway took it");
    bot.cancel(cid).await.unwrap();
    gateway.shutdown(ShutdownMode::Immediate).await;
}

#[tokio::test]
async fn the_calls_by_name_and_the_history_work_on_a_remote_client() {
    let mock = Mock::start(Broker::Bcs).await;
    let dir = tempfile::tempdir().unwrap();
    let gateway = Gateway::start(
        GatewayConfig::new("bcs", dir.path()),
        mock.config(Broker::Bcs),
        Some(roomy_limits(Broker::Bcs)),
    )
    .await
    .unwrap();
    gateway.ready().await;
    let bot = gated(dir.path(), "bot").await;

    // The same call by name on the client of the broker and on the client of the gateway gives the same reply.
    let via_broker = gateway.client().clone();
    let args = json!({"ticker": "SBER", "board": "TQBR"});
    let a = call_by_name(&via_broker, "instrument", &args)
        .await
        .unwrap();
    let b = call_by_name(&bot, "instrument", &args).await.unwrap();
    assert_eq!(a, b);
    let args = json!({"instrument": sber_bcs(), "depth": 3});
    let book = call_by_name(&bot, "order_book", &args).await.unwrap();
    assert!(book.contains("Snapshot"), "{book}");
    // An unknown name and a closed client are the errors of a call by name.
    assert!(
        call_by_name(&bot, "no_such_method", &json!({}))
            .await
            .is_err()
    );

    // The limit of a history request is learned from the gateway by `caps`, and then the history is cut into windows.
    assert_eq!(bot.candles_max_span(Timeframe::M1), None);
    let (md, ud) = bot.caps().await.unwrap();
    assert_eq!(md, gateway.client().caps().await.unwrap().md);
    assert_eq!(ud, gateway.client().caps().await.unwrap().ud);
    assert_eq!(
        bot.candles_max_span(Timeframe::M1),
        gateway.client().candles_max_span(Timeframe::M1)
    );
    assert!(bot.candles_max_span(Timeframe::M1).is_some());
    let mut pager = candle_pages_from_json(
        &bot,
        &json!({"instrument": sber_bcs(), "timeframe": "M1", "start": 0, "end": 3_600_000_000_000i64}),
    )
    .unwrap();
    let mut bars = 0;
    while let Some(page) = pager.next_page().await.expect("a window") {
        bars += page.bars.len();
    }
    assert!(bars > 0);

    // The subscription goes through the trait like on a connector.
    let sub = bot
        .subscribe_order_book(&sber_bcs(), SubscribeOpts::conflate())
        .await
        .unwrap();
    bot.unsubscribe(sub).await.unwrap();
    assert!(!Account::is_closed(&bot));
    gateway.shutdown(ShutdownMode::Immediate).await;
}
