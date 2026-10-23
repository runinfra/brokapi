//! A gateway on an imitation of a broker, and a bot connected to it: what every test here starts from.

#![allow(dead_code)] // each test binary uses its own part

use std::sync::Arc;

use brokapi::core::{
    ClientOrderId, Envelope, InstrumentId, InstrumentKey, NativeId, OrderType, PlaceOrderRequest,
    Price, Qty, Side, TimeRange, Timeframe, UnixNanos,
};
use brokapi_client::{Broker, BrokerConfig, Limiter, Settings};
use brokapi_gateway::{Gateway, GatewayConfig, RemoteClient};
use brokapi_mocks::{alor, bcs, finam, settings, tbank};
use serde_json::Value;
use tokio::sync::mpsc;

/// The imitation of one broker.
pub enum Mock {
    Bcs(bcs::MockServer, Arc<bcs::MockOptions>),
    Tbank(tbank::MockServer, Arc<tbank::MockOptions>),
    Finam(finam::MockServer),
    Alor(alor::MockServer, Arc<alor::MockOptions>),
}

impl Mock {
    pub async fn start(broker: Broker) -> Self {
        match broker {
            Broker::Bcs => {
                let opts = Arc::new(bcs::MockOptions::default());
                Mock::Bcs(bcs::MockServer::start(Arc::clone(&opts)).await, opts)
            }
            Broker::Tbank => {
                let opts = Arc::new(tbank::MockOptions::default());
                // A stop order gets the time of the real clock, as the broker writes it.
                opts.stop_create_now
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                Mock::Tbank(tbank::MockServer::start(Arc::clone(&opts)).await, opts)
            }
            Broker::Finam => Mock::Finam(finam::MockServer::start().await),
            Broker::Alor => {
                let opts = Arc::new(alor::MockOptions::default());
                Mock::Alor(alor::MockServer::start(Arc::clone(&opts)).await, opts)
            }
        }
    }

    pub fn settings(&self) -> Settings {
        let json: Value = match self {
            Mock::Bcs(s, _) => settings::bcs(s),
            Mock::Tbank(s, _) => settings::tbank(s),
            Mock::Finam(s) => settings::finam(s),
            Mock::Alor(s, _) => settings::alor(s),
        };
        Settings::parse(&json.to_string()).expect("settings")
    }

    pub fn config(&self, broker: Broker) -> BrokerConfig {
        BrokerConfig::from_settings(broker, &self.settings()).expect("broker config")
    }

    pub fn bcs(&self) -> (&bcs::MockServer, &Arc<bcs::MockOptions>) {
        match self {
            Mock::Bcs(s, o) => (s, o),
            _ => panic!("not the BCS mock"),
        }
    }

    /// The imitation holds every order command this many milliseconds before it acts and answers.
    pub fn hold_orders(&self, ms: u64) {
        use std::sync::atomic::Ordering::SeqCst;
        match self {
            Mock::Bcs(_, o) => o.order_delay_ms.store(ms, SeqCst),
            Mock::Tbank(_, o) => o.post_delay_ms.store(ms, SeqCst),
            Mock::Finam(s) => s.state.options.place_delay_ms.store(ms, SeqCst),
            Mock::Alor(_, o) => o.order_delay_ms.store(ms, SeqCst),
        }
    }

    /// The imitation takes a stop order and then loses the answer (`true`), or answers as usual (`false`).
    pub fn lose_stop_answers(&self, lose: bool) {
        use std::sync::atomic::Ordering::SeqCst;
        match self {
            Mock::Bcs(_, o) => o.stop_answer_lost.store(lose, SeqCst),
            Mock::Tbank(_, o) => o.stop_answer_lost.store(lose, SeqCst),
            Mock::Finam(s) => s.state.options.stop_answer_lost.store(lose, SeqCst),
            Mock::Alor(_, o) => o.stop_answer_lost.store(lose, SeqCst),
        }
    }

    /// How many order commands have reached the imitation.
    pub fn order_commands(&self) -> u32 {
        use std::sync::atomic::Ordering::SeqCst;
        match self {
            Mock::Bcs(_, o) => o.order_commands.load(SeqCst),
            Mock::Tbank(_, o) => o.post_calls.load(SeqCst),
            Mock::Finam(s) => s.state.count_calls("PlaceOrder") as u32,
            Mock::Alor(_, o) => o.order_commands.load(SeqCst),
        }
    }

    /// Executes the order `cid` at the imitation, whatever the broker.
    pub async fn fill(&self, cid: ClientOrderId) {
        let text = cid.hyphenated().to_string();
        match self {
            Mock::Bcs(s, _) => {
                s.fill(&text, Qty::from(20), Price::from(100));
            }
            Mock::Tbank(s, _) => s.fill(tbank::ACCOUNT_A, &text),
            Mock::Alor(s, _) => {
                s.fill(&text, Qty::from(20), Price::from(100));
            }
            Mock::Finam(s) => {
                use brokapi::core::CidCodec;
                // The cid travels in the order's comment; find the order by it.
                let comment = brokapi::finam::cid::FinamCidCodec
                    .encode(cid)
                    .unwrap()
                    .as_str()
                    .to_string();
                let order = s
                    .state
                    .orders_of("ACC1")
                    .into_iter()
                    .find(|o| o.order.as_ref().is_some_and(|x| x.comment == comment))
                    .expect("our order at the mock");
                // The trades stream is opened by a background task: wait until the server has it.
                brokapi_mocks::time::wait_until("the trades stream is open", || {
                    s.state.count_calls("SubscribeTrades") > 0
                })
                .await;
                let dec = |v: &str| {
                    Some(brokapi::finam::protocol::pb::google::r#type::Decimal { value: v.into() })
                };
                s.state.push_trade(
                    "ACC1",
                    brokapi::finam::protocol::pb::grpc::tradeapi::v1::AccountTrade {
                        trade_id: "TR-CONF".into(),
                        symbol: "SBER@MISX".into(),
                        price: dec("1"),
                        size: dec("2"),
                        side: brokapi::finam::protocol::pb::grpc::tradeapi::v1::Side::Buy as i32,
                        order_id: order.order_id.clone(),
                        comment,
                        account_id: "ACC1".into(),
                        ..Default::default()
                    },
                );
            }
        }
    }
}

/// The limits of a broker, with room for a burst: the scenarios run a dozen calls in a fraction of a second against an
/// imitation. They test the road, not the budget.
pub fn roomy_limits(broker: Broker) -> Limiter {
    let mut limits = match broker {
        Broker::Bcs => brokapi::bcs::limits::default_limits(),
        Broker::Tbank => brokapi::tbank::limits::default_limits(),
        Broker::Finam => brokapi::finam::limits::default_limits(),
        Broker::Alor => brokapi::alor::limits::default_limits(),
    };
    for bucket in limits.buckets.values_mut() {
        bucket.burst = bucket.burst.max(1000);
    }
    Limiter::with_limits(broker, &limits)
}

/// A gateway, the imitation behind it, and one bot.
pub struct Rig {
    pub broker: Broker,
    pub mock: Mock,
    pub gateway: Gateway,
    pub dir: tempfile::TempDir,
    pub bot: RemoteClient,
    /// The live events, as the channel of envelopes a connector writes.
    pub events: mpsc::Receiver<Envelope>,
}

impl Rig {
    pub async fn start(broker: Broker) -> Self {
        Self::start_with(broker, |_| {}).await
    }

    /// [`Rig::start`] with a change to the configuration of the connectors (a short timeout, for one).
    pub async fn start_with(broker: Broker, tune: impl FnOnce(&mut BrokerConfig)) -> Self {
        let mock = Mock::start(broker).await;
        let dir = tempfile::tempdir().unwrap();
        let config = GatewayConfig::new(broker.as_str(), dir.path());
        let mut broker_config = mock.config(broker);
        tune(&mut broker_config);
        let gateway = Gateway::start(config, broker_config, Some(roomy_limits(broker)))
            .await
            .expect("the gateway starts");
        gateway.ready().await;
        if broker == Broker::Bcs {
            // The portfolio socket of BCS sends a first snapshot of the account when it opens, whoever asked for it. The
            // scenarios want the events that follow their own calls, so the bot connects after that snapshot.
            brokapi_mocks::time::wait_until("the first snapshot of the account", || {
                gateway.health().last_seq >= 1
            })
            .await;
        }
        let bot = RemoteClient::connect_dir(dir.path(), "conformance")
            .await
            .expect("the bot connects");
        let events = bot.event_channel(1024).await.expect("event stream");
        Self {
            broker,
            mock,
            gateway,
            dir,
            bot,
            events,
        }
    }

    pub async fn stop(self) {
        self.gateway
            .shutdown(brokapi::core::ShutdownMode::Immediate)
            .await;
    }

    /// The source names of the two connectors, as the scenarios expect them.
    pub fn md_source(&self) -> String {
        format!("{}-md", self.broker)
    }

    pub fn ud_source(&self) -> String {
        format!("{}-ud", self.broker)
    }

    pub fn sber(&self) -> InstrumentId {
        match self.broker {
            Broker::Bcs => InstrumentId::new(
                InstrumentKey::new("SBER", "TQBR"),
                NativeId::TickerClass {
                    ticker: "SBER".into(),
                    class_code: "TQBR".into(),
                },
            ),
            Broker::Tbank => InstrumentId::new(
                InstrumentKey::new("SBER", "TQBR"),
                NativeId::Uid("uid-sber".into()),
            ),
            Broker::Finam => InstrumentId::new(
                InstrumentKey::new("SBER", "MISX"),
                NativeId::SymbolMic {
                    symbol: "SBER".into(),
                    mic: "MISX".into(),
                },
            ),
            Broker::Alor => brokapi::alor::util::instrument_id("SBER", "TQBR"),
        }
    }

    /// The instrument of a ticker of another broker: the connector must refuse it.
    pub fn foreign(&self) -> InstrumentId {
        match self.broker {
            Broker::Bcs | Broker::Tbank | Broker::Alor => InstrumentId::new(
                InstrumentKey::new("SBER", "MISX"),
                NativeId::SymbolMic {
                    symbol: "SBER".into(),
                    mic: "MISX".into(),
                },
            ),
            Broker::Finam => InstrumentId::new(
                InstrumentKey::new("SBER", "TQBR"),
                NativeId::TickerClass {
                    ticker: "SBER".into(),
                    class_code: "TQBR".into(),
                },
            ),
        }
    }

    pub fn unknown_instrument(&self) -> InstrumentId {
        match self.broker {
            Broker::Bcs => InstrumentId::new(
                InstrumentKey::new(brokapi_mocks::bcs::UNKNOWN_TICKER, "TQBR"),
                NativeId::TickerClass {
                    ticker: brokapi_mocks::bcs::UNKNOWN_TICKER.into(),
                    class_code: "TQBR".into(),
                },
            ),
            Broker::Tbank => InstrumentId::new(
                InstrumentKey::new("NOPE", "TQBR"),
                NativeId::Uid("no-such-uid".into()),
            ),
            Broker::Finam | Broker::Alor => {
                // The same shape as the instrument of the broker, with a ticker no mock knows.
                let sber = self.sber();
                match sber.native {
                    NativeId::SymbolMic { .. } => InstrumentId::new(
                        InstrumentKey::new("NOPE", "MISX"),
                        NativeId::SymbolMic {
                            symbol: "NOPE".into(),
                            mic: "MISX".into(),
                        },
                    ),
                    _ => brokapi::alor::util::instrument_id("NOPE", "TQBR"),
                }
            }
        }
    }

    pub fn depth(&self) -> u32 {
        match self.broker {
            Broker::Tbank => 10,
            _ => 20,
        }
    }

    /// A price that is far from the market and on the grid of the instrument.
    pub fn order_price(&self) -> Price {
        match self.broker {
            Broker::Bcs | Broker::Alor => "1.0".parse().unwrap(),
            _ => Price::from(1),
        }
    }

    pub fn stop_price(&self) -> Price {
        match self.broker {
            Broker::Bcs | Broker::Alor => "200.0".parse().unwrap(),
            _ => Price::from(1),
        }
    }

    /// The price for the scenarios that fill the order.
    pub fn own_price(&self) -> Price {
        match self.broker {
            Broker::Bcs | Broker::Alor => "100.0".parse().unwrap(),
            _ => Price::from(1),
        }
    }

    pub fn replace_prices(&self) -> (Price, Price) {
        match self.broker {
            Broker::Bcs | Broker::Alor => (Price::from(100), Price::from(101)),
            _ => (Price::from(1), Price::from(2)),
        }
    }

    pub fn timeframe(&self) -> Timeframe {
        match self.broker {
            Broker::Tbank | Broker::Finam => Timeframe::M5,
            _ => Timeframe::M1,
        }
    }
}

/// A day of the mocks that keep a calendar (2026-10-19 UTC).
pub fn day() -> TimeRange {
    TimeRange::new(
        UnixNanos::from_nanos(1_792_368_000_000_000_000),
        UnixNanos::from_nanos(1_792_454_400_000_000_000),
    )
}

/// A limit order far from the market.
pub fn limit_order(rig: &Rig, price: Price) -> PlaceOrderRequest {
    PlaceOrderRequest {
        cid: ClientOrderId::new_v4(),
        instrument: rig.sber(),
        side: Side::Buy,
        order_type: OrderType::Limit,
        qty: Qty::from(brokapi_conformance::LOT),
        price: Some(price),
    }
}

/// SBER on the BCS imitation.
pub fn sber_bcs() -> InstrumentId {
    InstrumentId::new(
        InstrumentKey::new("SBER", "TQBR"),
        NativeId::TickerClass {
            ticker: "SBER".into(),
            class_code: "TQBR".into(),
        },
    )
}

/// A buy of one lot of SBER on the BCS imitation at this price.
pub fn bcs_order(price: &str) -> PlaceOrderRequest {
    PlaceOrderRequest {
        cid: ClientOrderId::new_v4(),
        instrument: sber_bcs(),
        side: Side::Buy,
        order_type: OrderType::Limit,
        qty: Qty::from(brokapi_conformance::LOT),
        price: Some(price.parse().unwrap()),
    }
}

/// One place and one cancel: a few events for whoever listens to the order states.
pub async fn bcs_cycle(bot: &RemoteClient) -> ClientOrderId {
    use brokapi::core::OrderExecutor;
    let request = bcs_order("1.0");
    let cid = request.cid;
    bot.place(request).await.expect("place");
    bot.cancel(cid).await.expect("cancel");
    cid
}
