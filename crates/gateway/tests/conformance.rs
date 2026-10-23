//! The shared scenarios of `brokapi-conformance`, run through the gateway: the bot is a `RemoteClient`, the gateway
//! holds the connection to the imitation of a broker, and every call and every event crosses gRPC.
//!
//! A scenario that passes here proves that the gateway is a faithful road: the same refusals, the same errors, the same
//! events as with the connector itself, for each of the four brokers.

mod common;

use brokapi::core::{
    Error, InstrumentQuery, InstrumentSource, OrderExecutor, PlaceOrderRequest, PortfolioSource,
    Price, StopOrderExecutor, TimeRange, Timeframe, UnixNanos,
};
use brokapi_client::Broker;
use brokapi_conformance as conf;
use common::{Rig, day};

async fn market_data(broker: Broker) {
    let mut rig = Rig::start(broker).await;
    let sber = rig.sber();
    conf::order_book_snapshot(&rig.bot, &sber, rig.depth()).await;
    let source = rig.md_source();
    let sub = conf::order_book_stream(&rig.bot, &mut rig.events, &source, &sber).await;
    conf::unsubscribe(&rig.bot, sub).await;
    conf::unsubscribe_unknown(&rig.bot).await;
    rig.stop().await;
}

async fn instruments(broker: Broker) {
    let rig = Rig::start(broker).await;
    let query = InstrumentQuery::ByTicker {
        ticker: "SBER".into(),
        exchange: None,
    };
    conf::instrument_lookup(&rig.bot, query, &rig.sber()).await;
    rig.stop().await;
}

async fn orders(broker: Broker) {
    let rig = Rig::start(broker).await;
    let (sber, price) = (rig.sber(), rig.order_price());
    conf::reconcile(&rig.bot).await;
    conf::place_and_cancel(&rig.bot, &sber, price).await;
    conf::order_state_unknown_cid(&rig.bot).await;
    conf::rejected_order_does_not_block_the_instrument(&rig.bot, &sber, price).await;
    rig.stop().await;
}

async fn stop_orders(broker: Broker) {
    let rig = Rig::start(broker).await;
    let sber = rig.sber();
    conf::reconcile(&rig.bot).await;
    let (_, ud) = rig.bot.caps().await.expect("caps");
    // The caps that cross the wire are the caps of the connector.
    let own = rig.gateway.client().caps().await.expect("caps");
    assert_eq!(ud, own.ud);
    if ud.supports_stop_orders {
        conf::stop_order_far_from_market(&rig.bot, &sber, rig.stop_price()).await;
    } else {
        conf::stop_orders_unsupported(&rig.bot, &sber).await;
    }
    // Active stop orders are listed through the same road.
    assert!(rig.bot.active_stop_orders().await.expect("list").is_empty());
    rig.stop().await;
}

async fn portfolio(broker: Broker) {
    let mut rig = Rig::start(broker).await;
    let source = rig.ud_source();
    if broker == Broker::Bcs {
        // The BCS connector sends its first snapshot of the account, with no subscription, when its portfolio socket
        // opens, and the reconcile that every gateway runs at start opens it. A later `subscribe_portfolio` gets no
        // snapshot of its own (the same on a connector that was reconciled first), so the shared scenario, which waits
        // for one, cannot run behind a gateway. What a bot can check here: the read, and that the subscription is made.
        let read = rig.bot.portfolio().await.expect("portfolio");
        assert!(!read.balances.is_empty());
        rig.bot
            .subscribe_portfolio(brokapi::core::SubscribeOpts::lossless(64))
            .await
            .expect("subscribe_portfolio");
    } else {
        conf::portfolio(&rig.bot, &mut rig.events, &source).await;
    }
    rig.stop().await;
}

async fn history_and_streams(broker: Broker) {
    let mut rig = Rig::start(broker).await;
    let sber = rig.sber();
    let source = rig.md_source();
    match broker {
        Broker::Bcs => {
            conf::market_data_history(&rig.bot, &sber, Timeframe::M1, day(), "SBER@TQBR", day())
                .await
        }
        Broker::Tbank => {
            let candles = TimeRange::new(
                UnixNanos::from_nanos(1_700_000_000_000_000_000),
                UnixNanos::from_nanos(1_700_001_000_000_000_000),
            );
            let wide = TimeRange::new(UnixNanos::ZERO, UnixNanos::from_nanos(i64::MAX / 2));
            conf::market_data_history(&rig.bot, &sber, Timeframe::M5, candles, "MOEX", wide).await
        }
        Broker::Finam => {
            let candles = TimeRange::new(
                UnixNanos::from_nanos(1_700_000_000 * 1_000_000_000),
                UnixNanos::from_nanos(1_700_003_600 * 1_000_000_000),
            );
            // The schedule of the mock is built around "now".
            let now = brokapi::finam::util::now_nanos().as_nanos();
            let around_now = TimeRange::new(
                UnixNanos::from_nanos(now - 10_800_000_000_000),
                UnixNanos::from_nanos(now + 10_800_000_000_000),
            );
            conf::market_data_history(
                &rig.bot,
                &sber,
                Timeframe::M1,
                candles,
                "SBER@MISX",
                around_now,
            )
            .await
        }
        Broker::Alor => {
            // ALOR has no timetable of sessions: `schedule` is Unsupported, and that error crosses the wire as it is.
            conf::market_data_history_without_schedule(&rig.bot, &sber, Timeframe::M1, day()).await;
            match rig.bot.schedule("MOEX", day()).await {
                Err(Error::Unsupported { capability }) => assert_eq!(capability, "schedule"),
                other => panic!("{other:?}"),
            }
        }
    }
    let tf = rig.timeframe();
    conf::market_data_streams(&rig.bot, &mut rig.events, &source, &sber, tf).await;
    rig.stop().await;
}

async fn own_orders_events_active_replace_and_executions(broker: Broker) {
    let mut rig = Rig::start(broker).await;
    let (sber, price, source) = (rig.sber(), rig.own_price(), rig.ud_source());
    conf::reconcile(&rig.bot).await;
    conf::order_state_events(&rig.bot, &mut rig.events, &source, &sber, price).await;
    conf::active_orders_lifecycle(&rig.bot, &sber, price).await;
    let (_, ud) = rig.bot.caps().await.expect("caps");
    assert_eq!(ud.supports_replace, broker != Broker::Finam);
    let (from, to) = rig.replace_prices();
    conf::replace_order(&rig.bot, ud.supports_replace, &sber, from, to).await;
    conf::executions(&rig.bot, &mut rig.events, &source, &sber, price, |cid| {
        let mock = &rig.mock;
        async move { mock.fill(cid).await }
    })
    .await;
    rig.stop().await;
}

/// The lot of the instrument is learned from the trading parameters, as a bot that reads them first would.
async fn learn_lot(rig: &Rig) {
    let sber = rig.sber();
    let mut params = rig.bot.trading_params(&sber).await.expect("trading params");
    params.lot_size = conf::lots(1);
    rig.gateway.client().learn_grid(&params).await;
}

async fn a_cid_is_placed_once(broker: Broker) {
    let rig = Rig::start(broker).await;
    conf::reconcile(&rig.bot).await;
    conf::duplicate_place(&rig.bot, &rig.sber(), rig.own_price()).await;
    rig.stop().await;
}

async fn a_quantity_the_broker_cannot_take_is_refused(broker: Broker) {
    let rig = Rig::start(broker).await;
    learn_lot(&rig).await;
    conf::reconcile(&rig.bot).await;
    conf::invalid_qty(&rig.bot, &rig.sber(), rig.own_price()).await;
    rig.stop().await;
}

async fn an_id_of_another_broker_is_a_config_error(broker: Broker) {
    let rig = Rig::start(broker).await;
    conf::reconcile(&rig.bot).await;
    conf::foreign_instrument_id(&rig.bot, &rig.foreign(), rig.own_price()).await;
    rig.stop().await;
}

async fn an_unknown_instrument_is_not_found(broker: Broker) {
    let rig = Rig::start(broker).await;
    conf::unknown_instrument(&rig.bot, &rig.unknown_instrument()).await;
    rig.stop().await;
}

/// The same errors, from the road itself: a request the gateway cannot read is `Config`, not a crash.
async fn a_bad_request_is_a_config_error(broker: Broker) {
    let rig = Rig::start(broker).await;
    conf::reconcile(&rig.bot).await;
    let request = PlaceOrderRequest {
        qty: brokapi::core::Qty::ZERO,
        ..common::limit_order(&rig, rig.own_price())
    };
    // A zero quantity is refused by the connector, whatever the broker: a clear refusal, not an unknown outcome.
    match rig.bot.place(request).await {
        Err(Error::Rejected { .. }) | Err(Error::Config(_)) => {}
        other => panic!("{other:?}"),
    }
    rig.stop().await;
}

/// A command whose reply never comes is `Unknown` on the other side of the gateway too, and the order is not sent again.
async fn a_place_without_an_answer_is_unknown(broker: Broker) {
    use std::time::Duration;
    let rig = Rig::start_with(broker, |config| {
        use brokapi_client::BrokerConfig as C;
        let timeout = Duration::from_millis(150);
        match config {
            C::Bcs(cfg) => cfg.request_timeout = timeout,
            C::Tbank(cfg) => cfg.request_timeout = timeout,
            C::Finam { api, .. } => api.request_timeout = timeout,
            C::Alor(cfg) => cfg.request_timeout = timeout,
        }
    })
    .await;
    conf::reconcile(&rig.bot).await;
    rig.mock.hold_orders(600);
    let (sber, price) = (rig.sber(), rig.own_price());
    conf::place_without_answer_is_unknown(
        &rig.bot,
        &sber,
        price,
        |call| {
            brokapi_mocks::time::elapse_when(
                "the order reached the mock",
                || rig.mock.order_commands() == 1,
                Duration::from_millis(200),
                call,
            )
        },
        || async {
            // The delay of the mock ends: it took the order after all.
            brokapi_mocks::time::jump(Duration::from_millis(500)).await;
            rig.mock.hold_orders(0);
        },
    )
    .await;
    rig.stop().await;
}

/// The events of the orders are cut: trading stops with `Degraded` until a reconcile, and the word comes through.
async fn a_cut_of_the_order_events_degrades_until_reconcile() {
    let mut rig = Rig::start(Broker::Bcs).await;
    let (server, opts) = rig.mock.bcs();
    brokapi_mocks::time::wait_until("order events socket connected", || opts.event_sockets() > 0)
        .await;
    let before = opts.ws_connections();
    let sber = rig.sber();
    conf::reconcile(&rig.bot).await;
    conf::stream_cut_degrades_until_reconcile(
        &rig.bot,
        &mut rig.events,
        &sber,
        Price::from(100),
        || async {
            server.cut_event_sockets();
            // The connector pauses before it reopens the socket: move the clock until it has.
            brokapi_mocks::time::jump_until(
                "the socket is back",
                std::time::Duration::from_secs(1),
                || opts.ws_connections() > before,
            )
            .await;
        },
    )
    .await;
    rig.stop().await;
}

#[tokio::test]
async fn bcs_a_cut_of_the_order_events_degrades_until_reconcile() {
    a_cut_of_the_order_events_degrades_until_reconcile().await
}

macro_rules! per_broker {
    ($module:ident, $broker:expr) => {
        mod $module {
            use super::*;

            #[tokio::test]
            async fn market_data() {
                super::market_data($broker).await
            }

            #[tokio::test]
            async fn instruments() {
                super::instruments($broker).await
            }

            #[tokio::test]
            async fn orders() {
                super::orders($broker).await
            }

            #[tokio::test]
            async fn stop_orders() {
                super::stop_orders($broker).await
            }

            #[tokio::test]
            async fn portfolio() {
                super::portfolio($broker).await
            }

            #[tokio::test]
            async fn history_and_streams() {
                super::history_and_streams($broker).await
            }

            #[tokio::test]
            async fn own_orders_events_active_replace_and_executions() {
                super::own_orders_events_active_replace_and_executions($broker).await
            }

            #[tokio::test]
            async fn a_cid_is_placed_once() {
                super::a_cid_is_placed_once($broker).await
            }

            #[tokio::test]
            async fn a_quantity_the_broker_cannot_take_is_refused() {
                super::a_quantity_the_broker_cannot_take_is_refused($broker).await
            }

            #[tokio::test]
            async fn an_id_of_another_broker_is_a_config_error() {
                super::an_id_of_another_broker_is_a_config_error($broker).await
            }

            #[tokio::test]
            async fn an_unknown_instrument_is_not_found() {
                super::an_unknown_instrument_is_not_found($broker).await
            }

            #[tokio::test]
            async fn a_place_without_an_answer_is_unknown() {
                super::a_place_without_an_answer_is_unknown($broker).await
            }

            #[tokio::test]
            async fn a_bad_request_is_refused() {
                super::a_bad_request_is_a_config_error($broker).await
            }
        }
    };
}

per_broker!(bcs, Broker::Bcs);
per_broker!(tbank, Broker::Tbank);
per_broker!(finam, Broker::Finam);
per_broker!(alor, Broker::Alor);
