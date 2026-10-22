//! Edges of the Finam mapping and of the small public parts: what a type string means, a timeframe on the wire, the status
//! of a refused order, the codec check and the config that must not show its secret.

mod mock_server;

use brokapi_core::{
    CidCodec, Error, InstrumentKind, SessionKind, Timeframe, TradingAvailability, UnixNanos,
};
use brokapi_finam::map::instrument::{availability_at, kind_from_type, session_kind};
use brokapi_finam::map::market::{bar_close, timeframe_to_pb, wire_timeframe};
use brokapi_finam::map::order::{order_instrument, rejection_of};
use brokapi_finam::{FinamApiConfig, FinamCidCodec};
use brokapi_finam_protocol::pb::grpc::tradeapi::v1::assets as assets_pb;
use brokapi_finam_protocol::pb::grpc::tradeapi::v1::orders as orders_pb;

#[test]
fn the_kind_of_an_instrument_is_read_from_the_words_of_its_type() {
    for (text, kind) in [
        ("Option on a future", InstrumentKind::Option),
        ("FUTURES", InstrumentKind::Future),
        ("corporate bond", InstrumentKind::Bond),
        ("ETF", InstrumentKind::Etf),
        ("Mutual fund", InstrumentKind::Etf),
        ("Equities", InstrumentKind::Stock),
        ("Currency pair", InstrumentKind::Currency),
        ("something new", InstrumentKind::Other),
    ] {
        assert_eq!(kind_from_type(text), kind, "{text}");
    }
}

#[test]
fn a_session_is_told_by_its_name() {
    for (text, kind) in [
        ("OPENING_AUCTION", SessionKind::Auction),
        ("early", SessionKind::PreMarket),
        ("MORNING", SessionKind::PreMarket),
        ("LATE", SessionKind::PostMarket),
        ("evening", SessionKind::PostMarket),
        ("AFTER_HOURS", SessionKind::PostMarket),
        ("CORE", SessionKind::Main),
        ("regular", SessionKind::Main),
        ("TEA BREAK", SessionKind::Other),
    ] {
        assert_eq!(session_kind(text), kind, "{text}");
    }
}

fn at(seconds: i64) -> Option<prost_types::Timestamp> {
    Some(prost_types::Timestamp { seconds, nanos: 0 })
}

fn session(kind: &str, from: i64, to: i64) -> assets_pb::schedule_response::Sessions {
    assets_pb::schedule_response::Sessions {
        r#type: kind.into(),
        interval: Some(brokapi_finam_protocol::pb::google::r#type::Interval {
            start_time: at(from),
            end_time: at(to),
        }),
    }
}

#[test]
fn availability_follows_the_sessions_of_the_schedule() {
    let now = UnixNanos::from_nanos(150 * 1_000_000_000);
    let schedule = |sessions| assets_pb::ScheduleResponse {
        symbol: "SBER@MISX".into(),
        sessions,
    };
    // Inside a working session: open. Inside a closed one: closed, with the name of the session.
    assert_eq!(
        availability_at(&schedule(vec![session("CORE", 100, 200)]), now),
        (TradingAvailability::Open, Some("CORE".into()))
    );
    assert_eq!(
        availability_at(&schedule(vec![session("CLOSED", 100, 200)]), now),
        (TradingAvailability::Closed, Some("CLOSED".into()))
    );
    // Outside of all sessions, and a session without its times is skipped.
    let skipped = assets_pb::schedule_response::Sessions {
        r#type: "CORE".into(),
        interval: None,
    };
    let half = assets_pb::schedule_response::Sessions {
        r#type: "CORE".into(),
        interval: Some(brokapi_finam_protocol::pb::google::r#type::Interval {
            start_time: at(100),
            end_time: None,
        }),
    };
    assert_eq!(
        availability_at(
            &schedule(vec![skipped, half, session("CORE", 300, 400)]),
            now
        ),
        (
            TradingAvailability::Closed,
            Some("outside of scheduled sessions".into())
        )
    );
    assert_eq!(
        availability_at(&schedule(vec![]), now),
        (TradingAvailability::Unknown, Some("empty schedule".into()))
    );
}

#[test]
fn every_timeframe_has_a_wire_form_and_ten_minutes_are_built_from_five() {
    for tf in [
        Timeframe::M1,
        Timeframe::M5,
        Timeframe::M15,
        Timeframe::M30,
        Timeframe::H1,
        Timeframe::H4,
        Timeframe::D1,
        Timeframe::W1,
        Timeframe::MN,
    ] {
        assert!(timeframe_to_pb(tf).is_ok(), "{tf:?}");
        assert_eq!(wire_timeframe(tf).unwrap(), timeframe_to_pb(tf).unwrap());
    }
    assert!(matches!(
        timeframe_to_pb(Timeframe::M10),
        Err(Error::Unsupported { .. })
    ));
    assert_eq!(
        wire_timeframe(Timeframe::M10).unwrap(),
        timeframe_to_pb(Timeframe::M5).unwrap()
    );
}

#[test]
fn a_bar_closes_one_frame_after_it_opens() {
    let open = UnixNanos::from_nanos(1_000_000_000_000);
    let secs = |tf| (bar_close(tf, open).as_nanos() - open.as_nanos()) / 1_000_000_000;
    assert_eq!(
        [
            Timeframe::M1,
            Timeframe::M5,
            Timeframe::M10,
            Timeframe::M15,
            Timeframe::M30,
            Timeframe::H1,
            Timeframe::H4,
            Timeframe::D1,
            Timeframe::W1,
        ]
        .map(secs),
        [60, 300, 600, 900, 1800, 3600, 4 * 3600, 86_400, 7 * 86_400]
    );
}

fn state(status: orders_pb::OrderStatus, description: Option<&str>) -> orders_pb::OrderState {
    orders_pb::OrderState {
        status: status as i32,
        status_description: description.map(str::to_string),
        order: Some(orders_pb::Order {
            symbol: "SBER@MISX".into(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[test]
fn a_status_of_refusal_in_the_answer_to_a_place_is_a_rejection_with_the_text_of_the_broker() {
    use orders_pb::OrderStatus as S;
    for refused in [
        S::Rejected,
        S::Failed,
        S::DeniedByBroker,
        S::RejectedByExchange,
    ] {
        match rejection_of(&state(refused, Some("no money"))) {
            Some(Error::Rejected { code, message }) => {
                assert_eq!(code, format!("{refused:?}").to_lowercase());
                assert_eq!(message, "no money");
            }
            other => panic!("{refused:?}: {other:?}"),
        }
    }
    assert!(rejection_of(&state(S::New, None)).is_none());
    assert!(rejection_of(&state(S::Filled, None)).is_none());
    let echoed = order_instrument(&state(S::New, None));
    assert_eq!(echoed.key.symbol, "SBER");
    assert_eq!(
        order_instrument(&orders_pb::OrderState::default())
            .key
            .symbol,
        ""
    );
}

#[test]
fn the_codec_fits_the_limits_of_the_broker_and_says_what_it_can() {
    FinamCidCodec.validate().unwrap();
    let caps = FinamCidCodec.caps();
    assert!(caps.lossless);
    assert_eq!(caps.native_bits, 128);
}

#[test]
fn the_secret_is_never_printed() {
    let cfg = FinamApiConfig::new("super-secret-value");
    let shown = format!("{cfg:?}");
    assert!(!shown.contains("super-secret-value"), "{shown}");
    assert!(shown.contains("redacted"));
}

// --- the connectors ---

mod connectors {
    use std::sync::Arc;
    use std::time::Duration;

    use brokapi_core::{
        Connector, Error, ExecutionSource, MarketDataSource, OrderBookSource, OrderExecutor,
        ShutdownMode, SubscribeOpts, SubscriptionId,
    };
    use brokapi_finam::{MarketDataConfig, UserDataConfig};
    use brokapi_runtime::{BrokerLimiter, BrokerLimits};

    use crate::mock_server::{MockServer, api, md, sber, ud, ud_with};

    /// Limits that let one stream per connection through.
    fn one_stream() -> Arc<BrokerLimiter> {
        let limits = BrokerLimits::from_toml_str(
            r#"
            [finam.buckets.all]
            rate = 1000
            per = "1s"
            burst = 1000
            [finam.route]
            "*" = ["all"]
            [finam.conns]
            max_conns = 1
            max_subs_per_conn = 1
            supports_multiplex = true
            "#,
        )
        .unwrap();
        BrokerLimiter::new(&limits)
    }

    #[tokio::test]
    async fn a_stream_beyond_the_quota_of_a_connection_is_refused_before_anything_is_opened() {
        let server = MockServer::start().await;
        let cfg = UserDataConfig::new(api(&server), "ACC1", "finam-ud");
        let (ud, _rx) = ud_with(cfg, one_stream(), 64).await;
        ud.subscribe_order_state().await.unwrap();
        assert!(matches!(
            ud.subscribe_executions().await,
            Err(Error::Rejected { ref code, .. }) if code == "subscription_quota"
        ));
        ud.shutdown(ShutdownMode::Immediate).await;

        let (tx, _rx) = tokio::sync::mpsc::channel(64);
        let md = brokapi_finam::MarketDataConnector::start(
            MarketDataConfig::new(api(&server), "finam-md"),
            one_stream(),
            tx,
        )
        .await
        .unwrap();
        md.subscribe_order_book(&sber(), SubscribeOpts::conflate())
            .await
            .unwrap();
        assert!(matches!(
            md.subscribe_quotes(&sber(), SubscribeOpts::conflate()).await,
            Err(Error::Rejected { ref code, .. }) if code == "subscription_quota"
        ));
        md.shutdown(ShutdownMode::Immediate).await;
    }

    #[tokio::test]
    async fn the_capabilities_and_the_shutdown_are_also_there_through_the_connector_trait() {
        let server = MockServer::start().await;
        let (md, _rx) = md(&server).await;
        assert_eq!(Connector::caps(&md).streaming, md.caps().streaming);
        Connector::shutdown(md, ShutdownMode::Immediate).await;
        let (ud, _rx) = ud(&server, "ACC1").await;
        assert_eq!(
            Connector::caps(&ud).supports_replace,
            ud.caps().supports_replace
        );
        Connector::shutdown(ud, ShutdownMode::Immediate).await;
    }

    #[tokio::test]
    async fn the_status_of_a_stream_is_known_for_a_subscription_of_this_connector_only() {
        let server = MockServer::start().await;
        let (ud, _rx) = ud(&server, "ACC1").await;
        let sub = ud.subscribe_order_state().await.unwrap();
        assert!(ud.sub_status(sub).is_some());
        assert!(ud.sub_status(SubscriptionId::new(999_999)).is_none());
        // What a program does through `raw()` follows the settings of the connector.
        let raw = ud.raw();
        assert_eq!(raw.account_id(), Some("ACC1"));
        assert_eq!(raw.timeout(), Duration::from_secs(10));
        assert!(Arc::strong_count(raw.limiter()) >= 2);
        ud.shutdown(ShutdownMode::Immediate).await;
    }

    #[tokio::test]
    async fn a_graceful_shutdown_that_has_no_time_left_counts_the_streams_it_could_not_stop() {
        let server = MockServer::start().await;
        let (md, _rx) = md(&server).await;
        md.subscribe_quotes(&sber(), SubscribeOpts::conflate())
            .await
            .unwrap();
        let report = md
            .shutdown(ShutdownMode::Graceful {
                deadline: Duration::ZERO,
            })
            .await;
        assert_eq!(
            report.unsubscribed + report.unsubscribe_failed,
            1,
            "{report:?}"
        );
    }
}
