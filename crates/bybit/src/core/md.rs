//! The market data core: frames of the public stream to events.
//!
//! The order book stream sends one snapshot, then deltas. The core keeps a book per subscription and sends the whole
//! book (cut to the depth asked) after every change, so a reader that falls behind loses nothing by skipping to the
//! latest. The deltas of one topic are numbered in sequence (`u`): a number that is not the next one means a delta was
//! lost. The core then drops the book, tells the reader with [`Event::Resync`] and notes the topic in [`MdCore::take_gaps`]:
//! the connector asks the exchange for a new snapshot.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::time::Instant;

use brokapi_bybit_protocol::http::models::Level;
use brokapi_bybit_protocol::ws::models::{DataKind, WsIncoming};
use brokapi_core::{
    BookLevel, Candle, Decimal, Event, InstrumentId, MarketTrade, OrderBook, Price, Qty,
    ResyncReason, Side, SubscriptionId, Timeframe,
};
use brokapi_sansio::{
    BrokerCore, CoreEvent, DesiredSubscription, EventSink, Outbound, SubscriptionKind,
    SubscriptionSet,
};
use serde::Deserialize;
use tokio::sync::Notify;
use tracing::{debug, warn};

use super::registry::{Registry, SubState};
use crate::map::{broker_ts, ms_to_nanos, timestamps};

const SEEN_TRADES_CAP: usize = 20_000;

/// The order book of one subscription, kept from the snapshot and the deltas.
#[derive(Debug, Default)]
struct LocalBook {
    bids: BTreeMap<Decimal, Decimal>,
    asks: BTreeMap<Decimal, Decimal>,
    last_u: i64,
    synced: bool,
}

impl LocalBook {
    fn replace(&mut self, bids: &[Level], asks: &[Level], u: i64) {
        self.bids.clear();
        self.asks.clear();
        self.apply(bids, asks);
        self.last_u = u;
        self.synced = true;
    }

    fn apply(&mut self, bids: &[Level], asks: &[Level]) {
        for (side, levels) in [(&mut self.bids, bids), (&mut self.asks, asks)] {
            for l in levels {
                if l.1.is_zero() {
                    side.remove(&l.0);
                } else {
                    side.insert(l.0, l.1);
                }
            }
        }
    }

    /// Applies a delta. `false` when it is not the next one: the book cannot be trusted any more.
    fn delta(&mut self, bids: &[Level], asks: &[Level], u: i64) -> bool {
        if !self.synced || u != self.last_u + 1 {
            return false;
        }
        self.apply(bids, asks);
        self.last_u = u;
        true
    }

    fn top(&self, depth: usize) -> (Vec<BookLevel>, Vec<BookLevel>) {
        let lv = |(p, q): (&Decimal, &Decimal)| BookLevel {
            price: Price::new(*p),
            qty: Qty::new(*q),
        };
        (
            self.bids.iter().rev().take(depth).map(lv).collect(),
            self.asks.iter().take(depth).map(lv).collect(),
        )
    }
}

#[derive(Debug, Deserialize)]
struct WsBook {
    #[serde(rename = "b", default)]
    bids: Vec<Level>,
    #[serde(rename = "a", default)]
    asks: Vec<Level>,
    #[serde(rename = "u", default)]
    u: i64,
}

#[derive(Debug, Deserialize)]
struct WsTrade {
    #[serde(rename = "T", default)]
    time: i64,
    #[serde(rename = "S")]
    side: String,
    #[serde(
        rename = "v",
        deserialize_with = "brokapi_bybit_protocol::http::models::dec"
    )]
    qty: Decimal,
    #[serde(
        rename = "p",
        deserialize_with = "brokapi_bybit_protocol::http::models::dec"
    )]
    price: Decimal,
    #[serde(rename = "i", default)]
    id: String,
}

#[derive(Debug, Deserialize)]
struct WsKline {
    start: i64,
    end: i64,
    #[serde(deserialize_with = "brokapi_bybit_protocol::http::models::dec")]
    open: Decimal,
    #[serde(deserialize_with = "brokapi_bybit_protocol::http::models::dec")]
    close: Decimal,
    #[serde(deserialize_with = "brokapi_bybit_protocol::http::models::dec")]
    high: Decimal,
    #[serde(deserialize_with = "brokapi_bybit_protocol::http::models::dec")]
    low: Decimal,
    #[serde(deserialize_with = "brokapi_bybit_protocol::http::models::dec")]
    volume: Decimal,
}

/// The sans-io core of the market data connector.
#[derive(Debug, Default)]
pub struct MdCore {
    reg: Registry<SubscriptionKind>,
    books: HashMap<SubscriptionId, LocalBook>,
    seen_trades: HashMap<SubscriptionId, (HashSet<String>, VecDeque<String>)>,
    gaps: Vec<String>,
    connections: u32,
    desired: SubscriptionSet,
}

impl MdCore {
    /// An empty core.
    pub fn new() -> Self {
        Self::default()
    }

    /// Woken whenever a subscription changes state.
    pub fn notifier(&self) -> std::sync::Arc<Notify> {
        self.reg.notifier()
    }

    /// The subscriptions the core serves.
    pub fn desired(&self) -> &SubscriptionSet {
        &self.desired
    }

    /// Registers a subscription: frames of `topic` belong to `id` and are read as `kind`. `req_id` ties the answer of
    /// the exchange to the topic.
    pub fn register(
        &mut self,
        id: SubscriptionId,
        kind: SubscriptionKind,
        topic: &str,
        req_id: Option<&str>,
    ) {
        if matches!(kind, SubscriptionKind::OrderBook { .. }) {
            self.books.insert(id, LocalBook::default());
        }
        self.reg.register(id, kind, vec![topic.to_string()]);
        if let Some(r) = req_id {
            self.reg.add_request(r, topic);
        }
        self.refresh_desired();
    }

    /// Remembers a request id for a topic (after a book was asked for again).
    pub fn add_request(&mut self, req_id: &str, topic: &str) {
        self.reg.add_request(req_id, topic);
    }

    /// Forgets a subscription. Returns the topics nobody listens to any more.
    pub fn forget(&mut self, id: SubscriptionId) -> Vec<String> {
        self.books.remove(&id);
        self.seen_trades.remove(&id);
        let orphans = self.reg.forget(id);
        self.refresh_desired();
        orphans
    }

    /// Whether any subscription listens to the topic.
    pub fn has_topic(&self, topic: &str) -> bool {
        self.reg.has_topic(topic)
    }

    /// Whether the subscription is an order book that waits for a snapshot.
    pub fn book_waits(&self, id: SubscriptionId) -> bool {
        self.books.get(&id).is_some_and(|b| !b.synced)
    }

    /// The state of a subscription.
    pub fn sub_state(&self, id: SubscriptionId) -> (SubState, Option<String>) {
        self.reg.state(id).unwrap_or((SubState::Pending, None))
    }

    /// The topics whose order book lost a delta since the last call. The connector asks the exchange for a new
    /// snapshot of each.
    pub fn take_gaps(&mut self) -> Vec<String> {
        std::mem::take(&mut self.gaps)
    }

    fn refresh_desired(&mut self) {
        let mut subs: Vec<DesiredSubscription> = self
            .reg
            .iter()
            .map(|(id, kind)| DesiredSubscription {
                id,
                kind: kind.clone(),
            })
            .collect();
        subs.sort_by_key(|s| s.id.value());
        self.desired = SubscriptionSet::with_subscriptions(subs);
    }

    fn on_connected(&mut self, out: &mut EventSink) {
        self.connections += 1;
        if self.connections < 2 {
            return;
        }
        // A new connection: the books are void until the new snapshot, and every topic is asked for again.
        self.reg.reset();
        let books: Vec<(SubscriptionId, InstrumentId)> = self
            .reg
            .iter()
            .filter_map(|(id, k)| match k {
                SubscriptionKind::OrderBook { instrument, .. } => Some((id, instrument.clone())),
                _ => None,
            })
            .collect();
        for (id, instrument) in books {
            if let Some(b) = self.books.get_mut(&id) {
                b.synced = false;
            }
            out.push(CoreEvent {
                sub: Some(id),
                payload: Event::Resync {
                    sub: id,
                    instrument,
                    reason: ResyncReason::SubscriptionReset,
                },
            });
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn on_data(
        &mut self,
        topic: &str,
        kind: DataKind,
        ts: i64,
        cts: i64,
        data: &serde_json::Value,
        now: Instant,
        out: &mut EventSink,
    ) {
        self.reg.on_data(topic);
        let stamp = broker_ts(if cts > 0 { cts } else { ts });
        for id in self.reg.subs_of(topic) {
            let Some(sub_kind) = self.reg.kind(id).cloned() else {
                continue;
            };
            let payload = match sub_kind {
                SubscriptionKind::OrderBook { instrument, depth } => self.book_event(
                    id,
                    &instrument,
                    depth as usize,
                    kind,
                    topic,
                    data,
                    stamp,
                    now,
                    out,
                ),
                SubscriptionKind::Quotes { instrument } => read::<WsBook>(data).map(|b| {
                    Event::Quote(crate::map::market::quote(
                        b.bids.first(),
                        b.asks.first(),
                        cts.max(ts),
                        &instrument,
                        now,
                    ))
                }),
                SubscriptionKind::LastTrades { instrument } => {
                    self.trade_events(id, &instrument, data, now, out);
                    None
                }
                SubscriptionKind::LastCandle {
                    instrument,
                    timeframe,
                } => {
                    self.candle_events(id, &instrument, timeframe, data, now, out);
                    None
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
    }

    #[allow(clippy::too_many_arguments)]
    fn book_event(
        &mut self,
        id: SubscriptionId,
        instrument: &InstrumentId,
        depth: usize,
        kind: DataKind,
        topic: &str,
        data: &serde_json::Value,
        stamp: Option<brokapi_core::UnixNanos>,
        now: Instant,
        out: &mut EventSink,
    ) -> Option<Event> {
        let wire = read::<WsBook>(data)?;
        let book = self.books.entry(id).or_default();
        match kind {
            DataKind::Delta => {
                if !book.synced {
                    // Waiting for a snapshot: a delta of the old book is of no use.
                    return None;
                }
                if !book.delta(&wire.bids, &wire.asks, wire.u) {
                    warn!(
                        "order book {topic}: delta {} after {} is not the next one",
                        wire.u, book.last_u
                    );
                    book.synced = false;
                    if !self.gaps.iter().any(|t| t == topic) {
                        self.gaps.push(topic.to_string());
                    }
                    out.push(CoreEvent {
                        sub: Some(id),
                        payload: Event::Resync {
                            sub: id,
                            instrument: instrument.clone(),
                            reason: ResyncReason::SequenceGap,
                        },
                    });
                    return None;
                }
            }
            DataKind::Snapshot | DataKind::Event => book.replace(&wire.bids, &wire.asks, wire.u),
        }
        let (bids, asks) = book.top(depth.max(1));
        Some(Event::OrderBook(OrderBook::Snapshot {
            instrument: instrument.clone(),
            bids,
            asks,
            ts: timestamps(now, stamp),
        }))
    }

    fn trade_events(
        &mut self,
        id: SubscriptionId,
        instrument: &InstrumentId,
        data: &serde_json::Value,
        now: Instant,
        out: &mut EventSink,
    ) {
        let Some(trades) = read::<Vec<WsTrade>>(data) else {
            return;
        };
        for t in trades {
            // The same trade twice is told once (a snapshot after a reconnect can repeat the last ones).
            if !t.id.is_empty() {
                let (seen, order) = self.seen_trades.entry(id).or_default();
                if !seen.insert(t.id.clone()) {
                    continue;
                }
                order.push_back(t.id.clone());
                if order.len() > SEEN_TRADES_CAP
                    && let Some(old) = order.pop_front()
                {
                    seen.remove(&old);
                }
            }
            let side = match t.side.as_str() {
                "Buy" => Side::Buy,
                "Sell" => Side::Sell,
                _ => continue,
            };
            out.push(CoreEvent {
                sub: Some(id),
                payload: Event::MarketTrade(MarketTrade {
                    instrument: instrument.clone(),
                    price: Price::new(t.price),
                    qty: Qty::new(t.qty),
                    side,
                    trade_id: (!t.id.is_empty()).then_some(t.id),
                    ts: timestamps(now, broker_ts(t.time)),
                }),
            });
        }
    }

    fn candle_events(
        &self,
        id: SubscriptionId,
        instrument: &InstrumentId,
        tf: Timeframe,
        data: &serde_json::Value,
        now: Instant,
        out: &mut EventSink,
    ) {
        let Some(klines) = read::<Vec<WsKline>>(data) else {
            return;
        };
        for k in klines {
            let open_ts = ms_to_nanos(k.start);
            out.push(CoreEvent {
                sub: Some(id),
                payload: Event::Candle(Candle {
                    instrument: instrument.clone(),
                    timeframe: tf,
                    open: Price::new(k.open),
                    high: Price::new(k.high),
                    low: Price::new(k.low),
                    close: Price::new(k.close),
                    volume: Qty::new(k.volume),
                    open_ts,
                    // `end` is the last millisecond of the candle: it closes one millisecond later.
                    close_ts: ms_to_nanos(k.end + 1),
                    ts: timestamps(now, Some(open_ts)),
                }),
            });
        }
    }
}

fn read<T: serde::de::DeserializeOwned>(data: &serde_json::Value) -> Option<T> {
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
            WsIncoming::Connected => self.on_connected(out),
            WsIncoming::Data {
                topic,
                kind,
                ts,
                cts,
                data,
            } => {
                if !self.reg.has_topic(&topic) {
                    debug!("a frame of an unknown topic {topic} is dropped");
                    return;
                }
                self.on_data(&topic, kind, ts, cts, &data, now, out);
            }
            WsIncoming::Ack {
                op,
                req_id: Some(req_id),
                success,
                message,
            } if op == "subscribe" => {
                self.reg.on_ack(&req_id, success, &message);
            }
            WsIncoming::Ack { .. } | WsIncoming::Pong | WsIncoming::Unknown(_) => {}
        }
    }

    fn poll_transmit(&mut self) -> Option<Outbound> {
        // The requests go out through the WebSocket session of the connector.
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
    use brokapi_bybit_protocol::ws::models::parse_incoming;
    use serde_json::json;

    fn btc() -> InstrumentId {
        instrument_id("BTCUSDT", "linear")
    }

    fn frame(v: serde_json::Value) -> WsIncoming {
        parse_incoming(&v.to_string())
    }

    fn book_core(depth: u32) -> (MdCore, SubscriptionId) {
        let mut core = MdCore::new();
        let id = SubscriptionId::new(1);
        core.register(
            id,
            SubscriptionKind::OrderBook {
                instrument: btc(),
                depth,
            },
            "orderbook.50.BTCUSDT",
            Some("r1"),
        );
        (core, id)
    }

    fn snapshot() -> WsIncoming {
        frame(
            json!({"topic":"orderbook.50.BTCUSDT","type":"snapshot","ts":5,"cts":4,"data":{
            "s":"BTCUSDT","b":[["100.0","1"],["99.0","2"],["98.0","3"]],"a":[["101.0","1"],["102.0","2"]],"u":10,"seq":1}}),
        )
    }

    fn delta(u: i64, bids: serde_json::Value, asks: serde_json::Value) -> WsIncoming {
        frame(
            json!({"topic":"orderbook.50.BTCUSDT","type":"delta","ts":6,"data":{"s":"BTCUSDT","b":bids,"a":asks,"u":u,"seq":2}}),
        )
    }

    #[test]
    fn a_snapshot_then_deltas_make_the_book_and_the_depth_cuts_it() {
        let (mut core, id) = book_core(2);
        let mut out = EventSink::new();
        let now = Instant::now();
        core.on_frame(
            WsIncoming::Ack {
                op: "subscribe".into(),
                req_id: Some("r1".into()),
                success: true,
                message: String::new(),
            },
            now,
            &mut out,
        );
        assert_eq!(core.sub_state(id).0, SubState::Active);
        core.on_frame(snapshot(), now, &mut out);
        // 99.0 is gone, 100.5 is new, 101.0 gets bigger.
        core.on_frame(
            delta(
                11,
                json!([["99.0", "0"], ["100.5", "4"]]),
                json!([["101.0", "7"]]),
            ),
            now,
            &mut out,
        );
        let events = out.drain();
        assert_eq!(events.len(), 2);
        let Event::OrderBook(OrderBook::Snapshot { bids, asks, ts, .. }) = &events[1].payload
        else {
            panic!("a book");
        };
        assert_eq!(events[1].sub, Some(id));
        assert_eq!(
            bids.iter().map(|l| l.price.to_string()).collect::<Vec<_>>(),
            ["100.0", "100.5"]
                .iter()
                .rev()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
        );
        assert_eq!(asks[0].qty.to_string(), "7");
        assert_eq!(asks.len(), 2, "cut to the depth asked");
        assert_eq!(
            ts.broker_ts.unwrap().as_nanos(),
            6_000_000,
            "a delta has no engine time: the time of the frame is used"
        );
    }

    #[test]
    fn a_delta_that_is_not_the_next_one_drops_the_book_and_asks_for_a_new_snapshot() {
        let (mut core, id) = book_core(50);
        let mut out = EventSink::new();
        let now = Instant::now();
        core.on_frame(snapshot(), now, &mut out);
        core.on_frame(delta(12, json!([["100.0", "9"]]), json!([])), now, &mut out);
        let events = out.drain();
        assert_eq!(
            events.len(),
            2,
            "the snapshot and the Resync, no book from the bad delta"
        );
        assert!(matches!(
            events[1].payload,
            Event::Resync {
                reason: ResyncReason::SequenceGap,
                ..
            }
        ));
        assert_eq!(core.take_gaps(), vec!["orderbook.50.BTCUSDT".to_string()]);
        assert!(core.take_gaps().is_empty());
        assert!(core.book_waits(id));
        // Deltas are ignored until the next snapshot, then the book is back.
        core.on_frame(delta(13, json!([["100.0", "9"]]), json!([])), now, &mut out);
        assert!(out.drain().is_empty());
        core.on_frame(snapshot(), now, &mut out);
        assert_eq!(out.drain().len(), 1);
        assert!(!core.book_waits(id));
    }

    #[test]
    fn a_reconnect_voids_the_books_but_the_first_connect_does_not() {
        let (mut core, book) = book_core(50);
        let quotes = SubscriptionId::new(2);
        core.register(
            quotes,
            SubscriptionKind::Quotes { instrument: btc() },
            "orderbook.1.BTCUSDT",
            None,
        );
        let mut out = EventSink::new();
        let now = Instant::now();
        core.on_frame(WsIncoming::Connected, now, &mut out);
        assert!(out.drain().is_empty(), "the first connect is not a loss");
        core.on_frame(snapshot(), now, &mut out);
        out.drain();
        core.on_frame(WsIncoming::Connected, now, &mut out);
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
        assert_eq!(core.sub_state(quotes).0, SubState::Pending);
        assert!(core.book_waits(book));
    }

    #[test]
    fn a_quote_comes_from_the_best_levels_and_a_frame_of_an_unknown_topic_is_dropped() {
        let mut core = MdCore::new();
        let id = SubscriptionId::new(1);
        core.register(
            id,
            SubscriptionKind::Quotes { instrument: btc() },
            "orderbook.1.BTCUSDT",
            None,
        );
        let mut out = EventSink::new();
        let now = Instant::now();
        core.on_frame(
            frame(json!({"topic":"orderbook.1.BTCUSDT","type":"snapshot","ts":1,"data":{"s":"BTCUSDT","b":[["81695.8","3.824"]],"a":[["81695.9","0.133"]],"u":1,"seq":1},"cts":2})),
            now,
            &mut out,
        );
        core.on_frame(frame(json!({"topic":"orderbook.1.ETHUSDT","type":"snapshot","ts":1,"data":{"b":[],"a":[]}})), now, &mut out);
        let events = out.drain();
        assert_eq!(events.len(), 1);
        let Event::Quote(q) = &events[0].payload else {
            panic!("a quote")
        };
        assert_eq!(q.bid.as_ref().unwrap().price.to_string(), "81695.8");
        assert_eq!(q.ask.as_ref().unwrap().qty.to_string(), "0.133");
    }

    #[test]
    fn two_listeners_of_one_topic_both_get_the_frames() {
        let mut core = MdCore::new();
        let (a, b) = (SubscriptionId::new(1), SubscriptionId::new(2));
        core.register(
            a,
            SubscriptionKind::Quotes { instrument: btc() },
            "orderbook.1.BTCUSDT",
            None,
        );
        core.register(
            b,
            SubscriptionKind::OrderBook {
                instrument: btc(),
                depth: 1,
            },
            "orderbook.1.BTCUSDT",
            None,
        );
        let mut out = EventSink::new();
        core.on_frame(
            frame(json!({"topic":"orderbook.1.BTCUSDT","type":"snapshot","ts":1,"data":{"b":[["1","1"]],"a":[["2","1"]],"u":1}})),
            Instant::now(),
            &mut out,
        );
        let events = out.drain();
        assert_eq!(
            events.iter().map(|e| e.sub).collect::<Vec<_>>(),
            vec![Some(a), Some(b)]
        );
        assert_eq!(core.forget(a), Vec::<String>::new());
        assert_eq!(core.forget(b), vec!["orderbook.1.BTCUSDT".to_string()]);
    }

    #[test]
    fn trades_are_told_once_and_a_candle_closes_a_millisecond_after_its_end() {
        let mut core = MdCore::new();
        let (t, c) = (SubscriptionId::new(1), SubscriptionId::new(2));
        core.register(
            t,
            SubscriptionKind::LastTrades { instrument: btc() },
            "publicTrade.BTCUSDT",
            None,
        );
        core.register(
            c,
            SubscriptionKind::LastCandle {
                instrument: btc(),
                timeframe: Timeframe::M1,
            },
            "kline.1.BTCUSDT",
            None,
        );
        let mut out = EventSink::new();
        let now = Instant::now();
        let trade = json!({"topic":"publicTrade.BTCUSDT","type":"snapshot","ts":9,"data":[
            {"T":9,"s":"BTCUSDT","S":"Buy","v":"0.074","p":"81695.90","L":"PlusTick","i":"x1","BT":false,"seq":1},
            {"T":9,"s":"BTCUSDT","S":"Sell","v":"0.001","p":"81695.80","L":"MinusTick","i":"x2","BT":false,"seq":2}]});
        core.on_frame(frame(trade.clone()), now, &mut out);
        core.on_frame(frame(trade), now, &mut out);
        let events = out.drain();
        assert_eq!(events.len(), 2, "the repeat adds nothing");
        let Event::MarketTrade(m) = &events[0].payload else {
            panic!("a trade")
        };
        assert_eq!(
            (m.side, m.price.to_string(), m.qty.to_string()),
            (Side::Buy, "81695.90".into(), "0.074".into())
        );
        core.on_frame(
            frame(json!({"topic":"kline.1.BTCUSDT","type":"snapshot","ts":1,"data":[{"start":1792760100000_i64,"end":1792760159999_i64,
                "interval":"1","open":"81682.4","close":"81695.9","high":"81704.9","low":"81672","volume":"8.011","turnover":"1","confirm":false,"timestamp":1}]})),
            now,
            &mut out,
        );
        let events = out.drain();
        let Event::Candle(k) = &events[0].payload else {
            panic!("a candle")
        };
        assert_eq!(k.close_ts.as_nanos() - k.open_ts.as_nanos(), 60_000_000_000);
        assert_eq!(k.close.to_string(), "81695.9");
    }

    #[test]
    fn a_refused_subscription_keeps_the_reason_and_the_core_has_no_timers() {
        let mut core = MdCore::new();
        let id = SubscriptionId::new(1);
        core.register(
            id,
            SubscriptionKind::Quotes { instrument: btc() },
            "orderbook.1.NOPE",
            Some("r9"),
        );
        assert_eq!(core.desired().subs().len(), 1);
        let mut out = EventSink::new();
        core.on_frame(
            WsIncoming::Ack {
                op: "subscribe".into(),
                req_id: Some("r9".into()),
                success: false,
                message: "handler not found".into(),
            },
            Instant::now(),
            &mut out,
        );
        assert_eq!(
            core.sub_state(id),
            (SubState::Failed, Some("handler not found".into()))
        );
        assert!(core.poll_transmit().is_none() && core.poll_timeout().is_none());
        core.on_timeout(Instant::now(), &mut out);
        core.apply(&SubscriptionSet::new());
        assert!(core.desired().is_empty());
        // An unreadable frame gives nothing.
        core.register(
            SubscriptionId::new(2),
            SubscriptionKind::LastTrades { instrument: btc() },
            "publicTrade.X",
            None,
        );
        core.on_frame(
            frame(json!({"topic":"publicTrade.X","type":"snapshot","ts":1,"data":"nonsense"})),
            Instant::now(),
            &mut out,
        );
        assert!(out.drain().is_empty());
    }
}
