//! The market data core: frames of the data channel to events.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Instant;

use brokapi_alor_protocol::http::models::{
    Bar, MarketTradeRecord, OrderBook as WireBook, QuoteRecord,
};
use brokapi_alor_protocol::ws::models::WsIncoming;
use brokapi_core::{Event, InstrumentId, ResyncReason, SubscriptionId};
use brokapi_sansio::{
    BrokerCore, CoreEvent, EventSink, Outbound, SubscriptionKind, SubscriptionSet,
};
use tokio::sync::Notify;
use tracing::{debug, warn};

use crate::map::market;

/// Where a subscription is in its life.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubState {
    /// The request was sent, the broker has not confirmed it.
    Pending,
    /// The broker confirmed it, or data came.
    Active,
    /// The broker refused it.
    Failed,
}

#[derive(Debug)]
struct Entry {
    id: SubscriptionId,
    kind: SubscriptionKind,
    state: SubState,
    /// Why the broker refused the subscription.
    reason: Option<String>,
}

/// How many trade ids a trades subscription remembers to drop a repeat.
const SEEN_TRADES_CAP: usize = 20_000;

/// The sans-io core of the market data connector.
#[derive(Debug, Default)]
pub struct MdCore {
    desired: SubscriptionSet,
    by_guid: HashMap<String, Entry>,
    seen_trades: HashMap<SubscriptionId, (HashSet<i64>, VecDeque<i64>)>,
    connections: u32,
    ack: Arc<Notify>,
}

impl MdCore {
    /// An empty core.
    pub fn new() -> Self {
        Self::default()
    }

    /// Woken whenever a subscription changes state (confirmed, refused, or data came). The connector waits on it
    /// for the confirmation of a new subscription.
    pub fn notifier(&self) -> Arc<Notify> {
        Arc::clone(&self.ack)
    }

    /// The subscriptions the core was told about, as a set.
    pub fn desired(&self) -> &SubscriptionSet {
        &self.desired
    }

    /// Registers a subscription: frames with this `guid` belong to `id` and are read as `kind`. The connector
    /// calls it before the request is sent, so the confirmation cannot overtake it.
    pub fn register(&mut self, guid: &str, id: SubscriptionId, kind: SubscriptionKind) {
        self.by_guid.insert(
            guid.to_string(),
            Entry {
                id,
                kind,
                state: SubState::Pending,
                reason: None,
            },
        );
        self.refresh_desired();
    }

    /// Forgets a subscription (the data that still arrives for its `guid` is dropped).
    pub fn forget(&mut self, guid: &str) {
        if let Some(e) = self.by_guid.remove(guid) {
            self.seen_trades.remove(&e.id);
        }
        self.refresh_desired();
    }

    fn refresh_desired(&mut self) {
        let mut subs: Vec<_> = self
            .by_guid
            .values()
            .map(|e| brokapi_sansio::DesiredSubscription {
                id: e.id,
                kind: e.kind.clone(),
            })
            .collect();
        subs.sort_by_key(|s| s.id.value());
        self.desired = SubscriptionSet::with_subscriptions(subs);
    }

    /// The state of a subscription.
    pub fn actual_state(&self, id: SubscriptionId) -> Option<SubState> {
        self.by_guid.values().find(|e| e.id == id).map(|e| e.state)
    }

    /// The reason the broker gave when it refused a subscription.
    pub fn failure(&self, id: SubscriptionId) -> Option<&str> {
        self.by_guid
            .values()
            .find(|e| e.id == id)
            .and_then(|e| e.reason.as_deref())
    }

    fn on_reconnected(&mut self, out: &mut EventSink) {
        self.connections += 1;
        if self.connections < 2 {
            return;
        }
        for e in self.by_guid.values_mut() {
            e.state = SubState::Pending;
            if let SubscriptionKind::OrderBook { instrument, .. } = &e.kind {
                out.push(CoreEvent {
                    sub: Some(e.id),
                    payload: Event::Resync {
                        sub: e.id,
                        instrument: instrument.clone(),
                        reason: ResyncReason::SubscriptionReset,
                    },
                });
            }
        }
    }

    fn on_data(&mut self, guid: &str, data: serde_json::Value, now: Instant, out: &mut EventSink) {
        let Some(entry) = self.by_guid.get_mut(guid) else {
            debug!("a frame of an unknown subscription {guid} is dropped");
            return;
        };
        if entry.state != SubState::Active {
            entry.state = SubState::Active;
            self.ack.notify_waiters();
        }
        let id = entry.id;
        let kind = entry.kind.clone();
        let payload = match &kind {
            SubscriptionKind::OrderBook { instrument, .. } => parse::<WireBook>(&data)
                .map(|b| Event::OrderBook(market::order_book(&b, instrument, now))),
            SubscriptionKind::Quotes { instrument } => parse::<QuoteRecord>(&data)
                .map(|q| Event::Quote(market::quote(&q, instrument, now))),
            SubscriptionKind::LastCandle {
                instrument,
                timeframe,
            } => parse::<Bar>(&data)
                .map(|b| Event::Candle(market::candle(&b, instrument, *timeframe, now))),
            SubscriptionKind::LastTrades { instrument } => {
                self.trade_event(id, instrument, &data, now)
            }
            _ => None,
        };
        if let Some(payload) = payload {
            out.push(CoreEvent {
                sub: Some(id),
                payload,
            });
        }
    }

    fn trade_event(
        &mut self,
        id: SubscriptionId,
        instrument: &InstrumentId,
        data: &serde_json::Value,
        now: Instant,
    ) -> Option<Event> {
        let rec = parse::<MarketTradeRecord>(data)?;
        // The frames of the history that come with a subscription are not new trades.
        if rec.existing == Some(true) {
            return None;
        }
        if let Some(tid) = rec.id {
            let (seen, order) = self.seen_trades.entry(id).or_default();
            if !seen.insert(tid) {
                return None;
            }
            order.push_back(tid);
            if order.len() > SEEN_TRADES_CAP
                && let Some(old) = order.pop_front()
            {
                seen.remove(&old);
            }
        }
        market::market_trade(&rec, instrument, now).map(Event::MarketTrade)
    }
}

fn parse<T: serde::de::DeserializeOwned>(data: &serde_json::Value) -> Option<T> {
    match serde_json::from_value(data.clone()) {
        Ok(v) => Some(v),
        Err(e) => {
            warn!("a data frame cannot be read: {e}");
            None
        }
    }
}

impl BrokerCore for MdCore {
    type Frame = WsIncoming;

    fn on_frame(&mut self, frame: Self::Frame, now: Instant, out: &mut EventSink) {
        match frame {
            WsIncoming::Reconnected => self.on_reconnected(out),
            WsIncoming::Data { guid, data } => self.on_data(&guid, data, now, out),
            WsIncoming::Status {
                guid: Some(guid),
                code,
                message,
            } => {
                if let Some(e) = self.by_guid.get_mut(&guid) {
                    if code == 200 {
                        e.state = SubState::Active;
                    } else {
                        e.state = SubState::Failed;
                        e.reason = Some(message);
                    }
                    self.ack.notify_waiters();
                }
            }
            WsIncoming::Status { .. } | WsIncoming::Unknown(_) => {}
        }
    }

    fn poll_transmit(&mut self) -> Option<Outbound> {
        // The requests go out through the WebSocket session of the connector, which also puts the token in.
        None
    }

    fn poll_timeout(&self) -> Option<Instant> {
        None
    }

    fn on_timeout(&mut self, _now: Instant, _out: &mut EventSink) {}

    fn apply(&mut self, desired: &SubscriptionSet) {
        self.desired = desired.clone();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::instrument_id;
    use brokapi_core::Timeframe;
    use serde_json::json;

    fn sber() -> InstrumentId {
        instrument_id("SBER", "TQBR")
    }

    fn data(guid: &str, d: serde_json::Value) -> WsIncoming {
        WsIncoming::Data {
            guid: guid.into(),
            data: d,
        }
    }

    #[test]
    fn data_of_a_registered_guid_becomes_an_event_of_its_subscription() {
        let mut core = MdCore::new();
        let id = SubscriptionId::new(1);
        core.register(
            "g",
            id,
            SubscriptionKind::OrderBook {
                instrument: sber(),
                depth: 5,
            },
        );
        let mut out = EventSink::new();
        let now = Instant::now();
        core.on_frame(
            WsIncoming::Status {
                guid: Some("g".into()),
                code: 200,
                message: "ok".into(),
            },
            now,
            &mut out,
        );
        assert_eq!(core.actual_state(id), Some(SubState::Active));
        core.on_frame(
            data(
                "g",
                json!({"bids":[{"price":1.5,"volume":2}],"asks":[],"ms_timestamp":1}),
            ),
            now,
            &mut out,
        );
        core.on_frame(data("other", json!({})), now, &mut out);
        let events = out.drain();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].sub, Some(id));
        assert!(matches!(events[0].payload, Event::OrderBook(_)));
    }

    #[test]
    fn a_refused_subscription_is_failed_with_the_reason() {
        let mut core = MdCore::new();
        let id = SubscriptionId::new(1);
        core.register("g", id, SubscriptionKind::Quotes { instrument: sber() });
        let mut out = EventSink::new();
        core.on_frame(
            WsIncoming::Status {
                guid: Some("g".into()),
                code: 400,
                message: "bad symbol".into(),
            },
            Instant::now(),
            &mut out,
        );
        assert_eq!(core.actual_state(id), Some(SubState::Failed));
        assert_eq!(core.failure(id), Some("bad symbol"));
    }

    #[test]
    fn trades_of_the_history_and_repeats_are_dropped() {
        let mut core = MdCore::new();
        core.register(
            "t",
            SubscriptionId::new(2),
            SubscriptionKind::LastTrades { instrument: sber() },
        );
        let mut out = EventSink::new();
        let now = Instant::now();
        let t = |id: i64, existing: bool| {
            data(
                "t",
                json!({"id": id, "qty": 1, "price": 10, "side": "buy", "existing": existing}),
            )
        };
        core.on_frame(t(1, true), now, &mut out);
        core.on_frame(t(2, false), now, &mut out);
        core.on_frame(t(2, false), now, &mut out);
        core.on_frame(t(3, false), now, &mut out);
        assert_eq!(out.drain().len(), 2);
    }

    #[test]
    fn a_reconnect_voids_the_books_but_the_first_connect_does_not() {
        let mut core = MdCore::new();
        let (book, quotes) = (SubscriptionId::new(1), SubscriptionId::new(2));
        core.register(
            "b",
            book,
            SubscriptionKind::OrderBook {
                instrument: sber(),
                depth: 20,
            },
        );
        core.register("q", quotes, SubscriptionKind::Quotes { instrument: sber() });
        let mut out = EventSink::new();
        let now = Instant::now();
        core.on_frame(WsIncoming::Reconnected, now, &mut out);
        assert!(out.drain().is_empty(), "the first connect is not a loss");
        core.on_frame(WsIncoming::Reconnected, now, &mut out);
        let events = out.drain();
        assert_eq!(
            events.len(),
            1,
            "one Resync per order book, none for quotes"
        );
        assert!(matches!(
            events[0].payload,
            Event::Resync { sub, reason: ResyncReason::SubscriptionReset, .. } if sub == book
        ));
        assert_eq!(core.actual_state(quotes), Some(SubState::Pending));
    }

    #[test]
    fn a_candle_frame_is_the_candle_of_the_timeframe() {
        let mut core = MdCore::new();
        core.register(
            "c",
            SubscriptionId::new(3),
            SubscriptionKind::LastCandle {
                instrument: sber(),
                timeframe: Timeframe::M5,
            },
        );
        let mut out = EventSink::new();
        core.on_frame(
            data("c", json!({"time": 1_700_000_000, "open": 1, "high": 2, "low": 1, "close": 2, "volume": 5})),
            Instant::now(),
            &mut out,
        );
        let events = out.drain();
        let Event::Candle(c) = &events[0].payload else {
            panic!("a candle")
        };
        assert_eq!(c.timeframe, Timeframe::M5);
    }

    #[test]
    fn a_refused_subscription_keeps_the_reason_and_a_bad_or_repeated_frame_gives_nothing() {
        let mut core = MdCore::new();
        let id = SubscriptionId::new(1);
        core.register("g", id, SubscriptionKind::LastTrades { instrument: sber() });
        assert_eq!(core.desired().subs().len(), 1);
        let mut out = EventSink::new();
        let now = Instant::now();
        core.on_frame(
            WsIncoming::Status {
                guid: Some("g".into()),
                code: 400,
                message: "no such instrument".into(),
            },
            now,
            &mut out,
        );
        assert_eq!(core.actual_state(id), Some(SubState::Failed));
        assert_eq!(core.failure(id), Some("no such instrument"));
        // A status without a guid and a frame nobody knows are not for any subscription.
        core.on_frame(
            WsIncoming::Status {
                guid: None,
                code: 200,
                message: String::new(),
            },
            now,
            &mut out,
        );
        core.on_frame(WsIncoming::Unknown("x".into()), now, &mut out);
        // A trade that cannot be read is dropped, the same trade twice is told once.
        core.on_frame(data("g", json!({"id": "not a number"})), now, &mut out);
        let trade = json!({"id": 7, "price": 100.5, "qty": 3, "timestamp": 1, "side": "buy", "symbol": "SBER"});
        core.on_frame(data("g", trade.clone()), now, &mut out);
        core.on_frame(data("g", trade), now, &mut out);
        assert_eq!(out.drain().len(), 1);
    }

    #[test]
    fn a_trades_subscription_forgets_the_oldest_ids_when_the_memory_is_full() {
        let mut core = MdCore::new();
        let id = SubscriptionId::new(1);
        core.register("g", id, SubscriptionKind::LastTrades { instrument: sber() });
        let mut out = EventSink::new();
        let now = Instant::now();
        let trade = |n: i64| json!({"id": n, "price": 100.5, "qty": 3, "timestamp": 1, "side": "buy", "symbol": "SBER"});
        for n in 0..=SEEN_TRADES_CAP as i64 {
            core.on_frame(data("g", trade(n)), now, &mut out);
        }
        assert_eq!(out.drain().len(), SEEN_TRADES_CAP + 1);
        // Trade 0 fell out of the memory: it is told again, trade 1 is still a repeat.
        core.on_frame(data("g", trade(0)), now, &mut out);
        core.on_frame(data("g", trade(SEEN_TRADES_CAP as i64)), now, &mut out);
        assert_eq!(out.drain().len(), 1);
    }

    #[test]
    fn the_core_sends_nothing_by_itself_and_has_no_timers() {
        let mut core = MdCore::new();
        assert!(core.poll_transmit().is_none());
        assert!(core.poll_timeout().is_none());
        core.on_timeout(Instant::now(), &mut EventSink::new());
        let mut set = SubscriptionSet::new();
        core.apply(&set);
        set = SubscriptionSet::with_subscriptions(vec![]);
        core.apply(&set);
        assert!(core.desired().is_empty());
    }
}
