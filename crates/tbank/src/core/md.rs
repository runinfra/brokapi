//! Market-data core: one bidirectional `MarketDataStream`.
//!
//! The runtime owns the stream; this core decides what to send and turns what arrives into events.
//!
//! Logical subscriptions (what the client asked for, each with its `SubscriptionId`) are mapped to
//! wire subscriptions (what T-Bank is told). Several logical subscriptions can share one wire
//! subscription: an order book and quotes of one instrument are one order book subscription of the
//! larger depth, two subscriptions to the same book are one on the wire, and unsubscribing one does not
//! end the other. Quotes have no stream of their own at T-Bank: they are the best level of the book.

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use brokapi_core::{Event, InstrumentId, ResyncReason, SubscriptionId, Timeframe};
use brokapi_sansio::{
    BrokerCore, DesiredSubscription, EventSink, Outbound, SubscriptionKind, SubscriptionSet,
};
use brokapi_tbank_protocol::v1 as pb;
use brokapi_tbank_protocol::v1::market_data_request::Payload as Req;
use brokapi_tbank_protocol::v1::market_data_response::Payload as Resp;

use crate::ids::{instrument_ref, matches};
use crate::map::market;

/// Pause before a subscription refused with a transient status (internal error, too many requests)
/// is sent again.
pub const RETRY_AFTER: Duration = Duration::from_secs(5);

/// One inbound item for [`MdCore`]: what the runtime saw on the market data stream.
// Boxing the large variant would change the public shape of the enum; these values are short-lived.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum MdFrame {
    /// A (new) stream is open: every subscription is sent again.
    Connected,
    /// A message the server sent on the stream: data, a subscription answer or a ping.
    Response(pb::MarketDataResponse),
}

/// A logical subscription changed state: confirmed or refused.
///
/// The connector takes these from [`MdCore::take_updates`] and wakes the caller that waits in
/// `subscribe_*`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubUpdate {
    /// The subscription that changed.
    pub sub: SubscriptionId,
    /// `Ok` when the broker confirmed it. `Err` holds the broker's subscription status name
    /// (for example `SUBSCRIPTION_STATUS_INSTRUMENT_NOT_FOUND`).
    pub result: Result<(), String>,
}

/// Where a logical subscription is in its life, see [`MdCore::state`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LState {
    /// Asked for, not confirmed yet.
    Pending,
    /// Confirmed by the broker. Data flows.
    Active,
    /// Refused by the broker. A transient refusal is retried after [`RETRY_AFTER`].
    Failed,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
enum WireKey {
    Book(String),
    Trades(String),
    Candles(String, i32),
}

#[derive(Debug, Clone)]
enum WireState {
    Pending,
    Active,
    Failed { retry_at: Option<Instant> },
}

#[derive(Debug, Clone)]
struct Wire {
    instrument: InstrumentId,
    /// Book depth on the wire (0 for trades / candles).
    depth: u32,
    state: WireState,
}

#[derive(Debug, Clone)]
enum Kind {
    Book { depth: u32 },
    Quotes,
    Trades,
    Candles(Timeframe),
}

#[derive(Debug, Clone)]
struct Logical {
    instrument: InstrumentId,
    kind: Kind,
    wire: WireKey,
    state: LState,
}

/// Sans-io core of the market data stream.
///
/// Logical subscriptions (what the client asked for) are mapped to wire subscriptions (what T-Bank
/// is told). The core queues the requests ([`poll_request`](Self::poll_request)), turns responses
/// into events, and reports confirmations ([`take_updates`](Self::take_updates)). It implements
/// [`BrokerCore`] with [`MdFrame`] as the frame type. See the [module docs](crate::core) for an
/// example.
#[derive(Debug)]
pub struct MdCore {
    desired: SubscriptionSet,
    logical: HashMap<SubscriptionId, Logical>,
    wires: HashMap<WireKey, Wire>,
    requests: VecDeque<pb::MarketDataRequest>,
    updates: Vec<SubUpdate>,
    /// Wire subscriptions we asked to drop and have not seen confirmed yet.
    unsubscribing: Vec<(WireKey, InstrumentId)>,
    unsub_ok: usize,
    unsub_failed: usize,
    connections: u32,
    ping_ms: i32,
}

impl Default for MdCore {
    fn default() -> Self {
        Self::new(Duration::from_secs(30))
    }
}

fn wire_key(instrument: &InstrumentId, kind: &Kind) -> WireKey {
    let r = instrument_ref(instrument);
    match kind {
        Kind::Book { .. } | Kind::Quotes => WireKey::Book(r),
        Kind::Trades => WireKey::Trades(r),
        Kind::Candles(tf) => WireKey::Candles(r, market::subscription_interval(*tf) as i32),
    }
}

impl MdCore {
    /// `ping_interval` is requested from the server (5000..180000 ms are valid).
    pub fn new(ping_interval: Duration) -> Self {
        Self {
            desired: SubscriptionSet::new(),
            logical: HashMap::new(),
            wires: HashMap::new(),
            requests: VecDeque::new(),
            updates: Vec::new(),
            unsubscribing: Vec::new(),
            unsub_ok: 0,
            unsub_failed: 0,
            connections: 0,
            ping_ms: ping_interval.as_millis().clamp(5_000, 180_000) as i32,
        }
    }

    /// Next request to write to the stream.
    pub fn poll_request(&mut self) -> Option<pb::MarketDataRequest> {
        self.requests.pop_front()
    }

    /// Takes the confirmations and refusals that happened since the last call.
    pub fn take_updates(&mut self) -> Vec<SubUpdate> {
        std::mem::take(&mut self.updates)
    }

    /// The set of subscriptions the consumer wants now, as last given to `apply`.
    pub fn desired(&self) -> &SubscriptionSet {
        &self.desired
    }

    /// The state of a logical subscription, or `None` if the core does not know the id.
    pub fn state(&self, sub: SubscriptionId) -> Option<LState> {
        self.logical.get(&sub).map(|l| l.state)
    }

    /// Wire subscriptions currently held at the broker (or being set up).
    pub fn wire_count(&self) -> usize {
        self.wires.len()
    }

    /// `(confirmed, refused, still waiting)` unsubscribe confirmations (graceful shutdown).
    pub fn unsubscribe_progress(&self) -> (usize, usize, usize) {
        (self.unsub_ok, self.unsub_failed, self.unsubscribing.len())
    }

    /// Counts what is still unconfirmed as failed (shutdown deadline passed).
    pub fn give_up_unsubscribing(&mut self) {
        self.unsub_failed += self.unsubscribing.len();
        self.unsubscribing.clear();
    }

    fn instruments_request(
        key: &WireKey,
        w: &Wire,
        action: pb::SubscriptionAction,
    ) -> pb::MarketDataRequest {
        let payload = match key {
            WireKey::Book(r) => Req::SubscribeOrderBookRequest(pb::SubscribeOrderBookRequest {
                subscription_action: action as i32,
                instruments: vec![pb::OrderBookInstrument {
                    depth: w.depth as i32,
                    instrument_id: r.clone(),
                    ..Default::default()
                }],
            }),
            WireKey::Trades(r) => Req::SubscribeTradesRequest(pb::SubscribeTradesRequest {
                subscription_action: action as i32,
                instruments: vec![pb::TradeInstrument {
                    instrument_id: r.clone(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            WireKey::Candles(r, interval) => {
                Req::SubscribeCandlesRequest(pb::SubscribeCandlesRequest {
                    subscription_action: action as i32,
                    instruments: vec![pb::CandleInstrument {
                        interval: *interval,
                        instrument_id: r.clone(),
                        ..Default::default()
                    }],
                    waiting_close: false,
                    candle_source_type: None,
                })
            }
        };
        pb::MarketDataRequest {
            payload: Some(payload),
        }
    }

    fn queue(&mut self, key: &WireKey, action: pb::SubscriptionAction) {
        if let Some(w) = self.wires.get(key) {
            let req = Self::instruments_request(key, w, action);
            self.requests.push_back(req);
        }
    }

    fn mark_active(&mut self, key: &WireKey) {
        if let Some(w) = self.wires.get_mut(key) {
            w.state = WireState::Active;
        }
        for (id, l) in self.logical.iter_mut() {
            if &l.wire == key && l.state != LState::Active {
                l.state = LState::Active;
                self.updates.push(SubUpdate {
                    sub: *id,
                    result: Ok(()),
                });
            }
        }
    }

    fn mark_failed(&mut self, key: &WireKey, status: pb::SubscriptionStatus, now: Instant) {
        let transient = matches!(
            status,
            pb::SubscriptionStatus::InternalError | pb::SubscriptionStatus::TooManyRequests
        );
        if let Some(w) = self.wires.get_mut(key) {
            w.state = WireState::Failed {
                retry_at: transient.then_some(now + RETRY_AFTER),
            };
        }
        for (id, l) in self.logical.iter_mut() {
            if &l.wire == key && l.state == LState::Pending {
                l.state = LState::Failed;
                self.updates.push(SubUpdate {
                    sub: *id,
                    result: Err(status.as_str_name().to_string()),
                });
            }
        }
    }

    /// Recomputes the wire subscriptions from the logical ones and queues the difference.
    fn sync_wires(&mut self) {
        let mut needed: HashMap<WireKey, (InstrumentId, u32)> = HashMap::new();
        for l in self.logical.values() {
            let depth = match l.kind {
                Kind::Book { depth } => market::wire_depth(depth),
                Kind::Quotes => 1,
                _ => 0,
            };
            let e = needed
                .entry(l.wire.clone())
                .or_insert_with(|| (l.instrument.clone(), 0));
            e.1 = e.1.max(depth);
        }

        let stale: Vec<WireKey> = self
            .wires
            .keys()
            .filter(|k| !needed.contains_key(*k))
            .cloned()
            .collect();
        for key in stale {
            self.queue(&key, pb::SubscriptionAction::Unsubscribe);
            if let Some(w) = self.wires.remove(&key) {
                self.unsubscribing.push((key, w.instrument));
            }
        }

        for (key, (instrument, depth)) in needed {
            match self.wires.get(&key).map(|w| w.depth) {
                None => {
                    self.wires.insert(
                        key.clone(),
                        Wire {
                            instrument,
                            depth,
                            state: WireState::Pending,
                        },
                    );
                    self.queue(&key, pb::SubscriptionAction::Subscribe);
                }
                Some(old) if old != depth => {
                    // Depth changed: drop the old one first, then take the new one.
                    self.queue(&key, pb::SubscriptionAction::Unsubscribe);
                    if let Some(w) = self.wires.get_mut(&key) {
                        w.depth = depth;
                        w.state = WireState::Pending;
                    }
                    self.queue(&key, pb::SubscriptionAction::Subscribe);
                }
                Some(_) => {}
            }
        }

        // New logical subscriptions on a wire that is already up are confirmed at once.
        let ready: Vec<(SubscriptionId, bool)> = self
            .logical
            .iter()
            .filter(|(_, l)| l.state == LState::Pending)
            .filter_map(|(id, l)| match self.wires.get(&l.wire).map(|w| &w.state) {
                Some(WireState::Active) => Some((*id, true)),
                _ => None,
            })
            .collect();
        for (id, _) in ready {
            if let Some(l) = self.logical.get_mut(&id) {
                l.state = LState::Active;
            }
            self.updates.push(SubUpdate {
                sub: id,
                result: Ok(()),
            });
        }
    }

    fn on_connected(&mut self, out: &mut EventSink) {
        self.connections += 1;
        self.requests.clear();
        self.unsubscribing.clear();
        self.requests.push_back(pb::MarketDataRequest {
            payload: Some(Req::PingSettings(pb::PingDelaySettings {
                ping_delay_ms: Some(self.ping_ms),
            })),
        });
        let keys: Vec<WireKey> = self.wires.keys().cloned().collect();
        for key in keys {
            if let Some(w) = self.wires.get_mut(&key) {
                w.state = WireState::Pending;
            }
            self.queue(&key, pb::SubscriptionAction::Subscribe);
        }
        let reconnect = self.connections > 1;
        let mut ids: Vec<SubscriptionId> = self.logical.keys().copied().collect();
        ids.sort_by_key(|i| i.value());
        for id in ids {
            let l = self.logical.get_mut(&id).expect("listed");
            l.state = LState::Pending;
            if reconnect && matches!(l.kind, Kind::Book { .. } | Kind::Quotes) {
                out.push_payload(
                    Some(id),
                    Event::Resync {
                        sub: id,
                        instrument: l.instrument.clone(),
                        reason: ResyncReason::SubscriptionReset,
                    },
                );
            }
        }
    }

    fn find_wire(
        &self,
        kind: fn(&WireKey) -> bool,
        uid: &str,
        figi: &str,
        ticker: &str,
        class_code: &str,
    ) -> Option<WireKey> {
        self.wires
            .iter()
            .find(|(k, w)| kind(k) && matches(&w.instrument, uid, figi, ticker, class_code))
            .map(|(k, _)| k.clone())
    }

    fn find_unsub(
        &mut self,
        kind: fn(&WireKey) -> bool,
        uid: &str,
        figi: &str,
        ticker: &str,
        class_code: &str,
    ) -> bool {
        let pos = self
            .unsubscribing
            .iter()
            .position(|(k, i)| kind(k) && matches(i, uid, figi, ticker, class_code));
        match pos {
            Some(p) => {
                self.unsubscribing.remove(p);
                true
            }
            None => false,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn subscription_result(
        &mut self,
        kind: fn(&WireKey) -> bool,
        action: i32,
        status: i32,
        ids: (&str, &str, &str, &str),
        interval: Option<i32>,
        now: Instant,
    ) {
        let (uid, figi, ticker, class_code) = ids;
        let status =
            pb::SubscriptionStatus::try_from(status).unwrap_or(pb::SubscriptionStatus::Unspecified);
        if action == pb::SubscriptionAction::Unsubscribe as i32 {
            if self.find_unsub(kind, uid, figi, ticker, class_code) {
                if status == pb::SubscriptionStatus::Success {
                    self.unsub_ok += 1;
                } else {
                    self.unsub_failed += 1;
                }
            }
            return;
        }
        let key = self
            .wires
            .iter()
            .find(|(k, w)| {
                kind(k)
                    && matches(&w.instrument, uid, figi, ticker, class_code)
                    && match (k, interval) {
                        (WireKey::Candles(_, i), Some(x)) => *i == x,
                        _ => true,
                    }
            })
            .map(|(k, _)| k.clone());
        let Some(key) = key else { return };
        if status == pb::SubscriptionStatus::Success {
            self.mark_active(&key);
        } else {
            self.mark_failed(&key, status, now);
        }
    }

    fn on_response(&mut self, resp: pb::MarketDataResponse, now: Instant, out: &mut EventSink) {
        let Some(payload) = resp.payload else { return };
        match payload {
            Resp::SubscribeOrderBookResponse(r) => {
                for s in &r.order_book_subscriptions {
                    self.subscription_result(
                        |k| matches!(k, WireKey::Book(_)),
                        s.subscription_action,
                        s.subscription_status,
                        (&s.instrument_uid, &s.figi, &s.ticker, &s.class_code),
                        None,
                        now,
                    );
                }
            }
            Resp::SubscribeTradesResponse(r) => {
                for s in &r.trade_subscriptions {
                    self.subscription_result(
                        |k| matches!(k, WireKey::Trades(_)),
                        s.subscription_action,
                        s.subscription_status,
                        (&s.instrument_uid, &s.figi, &s.ticker, &s.class_code),
                        None,
                        now,
                    );
                }
            }
            Resp::SubscribeCandlesResponse(r) => {
                for s in &r.candles_subscriptions {
                    self.subscription_result(
                        |k| matches!(k, WireKey::Candles(..)),
                        s.subscription_action,
                        s.subscription_status,
                        (&s.instrument_uid, &s.figi, &s.ticker, &s.class_code),
                        Some(s.interval),
                        now,
                    );
                }
            }
            Resp::Orderbook(b) => self.on_book(&b, now, out),
            Resp::Trade(t) => self.on_trade(&t, now, out),
            Resp::Candle(c) => self.on_candle(&c, now, out),
            // Ping, trading status, last price, open interest, info/last-price subscription results.
            _ => {}
        }
    }

    /// Data can arrive before the subscription result: a pending subscription that gets data is active.
    fn activate_on_data(&mut self, key: &WireKey) {
        if matches!(
            self.wires.get(key).map(|w| &w.state),
            Some(WireState::Pending)
        ) {
            self.mark_active(key);
        }
    }

    fn on_book(&mut self, b: &pb::OrderBook, now: Instant, out: &mut EventSink) {
        let Some(key) = self.find_wire(
            |k| matches!(k, WireKey::Book(_)),
            &b.instrument_uid,
            &b.figi,
            &b.ticker,
            &b.class_code,
        ) else {
            return;
        };
        self.activate_on_data(&key);
        let mut ids: Vec<SubscriptionId> = self
            .logical
            .iter()
            .filter(|(_, l)| l.wire == key && matches!(l.kind, Kind::Book { .. } | Kind::Quotes))
            .map(|(id, _)| *id)
            .collect();
        ids.sort_by_key(|i| i.value());
        for id in ids {
            let l = &self.logical[&id];
            if !b.is_consistent {
                // Not all orders made it into the book: do not publish it, tell the consumer.
                out.push_payload(
                    Some(id),
                    Event::Resync {
                        sub: id,
                        instrument: l.instrument.clone(),
                        reason: ResyncReason::SequenceGap,
                    },
                );
                continue;
            }
            let payload = match l.kind {
                Kind::Book { depth } => Event::OrderBook(market::book_snapshot(
                    &l.instrument,
                    &b.bids,
                    &b.asks,
                    depth as usize,
                    b.time.as_ref(),
                    now,
                )),
                Kind::Quotes => Event::Quote(market::quote(
                    &l.instrument,
                    &b.bids,
                    &b.asks,
                    b.time.as_ref(),
                    now,
                )),
                _ => continue,
            };
            out.push_payload(Some(id), payload);
        }
    }

    fn on_trade(&mut self, t: &pb::Trade, now: Instant, out: &mut EventSink) {
        let Some(key) = self.find_wire(
            |k| matches!(k, WireKey::Trades(_)),
            &t.instrument_uid,
            &t.figi,
            &t.ticker,
            &t.class_code,
        ) else {
            return;
        };
        self.activate_on_data(&key);
        let mut ids: Vec<SubscriptionId> = self
            .logical
            .iter()
            .filter(|(_, l)| l.wire == key && matches!(l.kind, Kind::Trades))
            .map(|(id, _)| *id)
            .collect();
        ids.sort_by_key(|i| i.value());
        for id in ids {
            if let Some(m) = market::trade(&self.logical[&id].instrument, t, now) {
                out.push_payload(Some(id), Event::MarketTrade(m));
            }
        }
    }

    fn on_candle(&mut self, c: &pb::Candle, now: Instant, out: &mut EventSink) {
        let Some(key) = self.find_wire(
            |k| matches!(k, WireKey::Candles(..)),
            &c.instrument_uid,
            &c.figi,
            &c.ticker,
            &c.class_code,
        ) else {
            return;
        };
        // Several intervals of one instrument are separate wire subscriptions: pick by interval.
        let Some(WireKey::Candles(r, _)) = Some(key) else {
            return;
        };
        let key = WireKey::Candles(r, c.interval);
        if !self.wires.contains_key(&key) {
            return;
        }
        self.activate_on_data(&key);
        let Some(tf) = market::timeframe_from_interval(c.interval()) else {
            return;
        };
        let mut ids: Vec<SubscriptionId> = self
            .logical
            .iter()
            .filter(|(_, l)| l.wire == key && matches!(l.kind, Kind::Candles(_)))
            .map(|(id, _)| *id)
            .collect();
        ids.sort_by_key(|i| i.value());
        for id in ids {
            out.push_payload(
                Some(id),
                Event::Candle(market::stream_candle(
                    &self.logical[&id].instrument,
                    tf,
                    c,
                    now,
                )),
            );
        }
    }
}

impl BrokerCore for MdCore {
    type Frame = MdFrame;

    fn on_frame(&mut self, frame: MdFrame, now: Instant, out: &mut EventSink) {
        match frame {
            MdFrame::Connected => self.on_connected(out),
            MdFrame::Response(r) => self.on_response(r, now, out),
        }
    }

    /// gRPC requests are typed: use [`MdCore::poll_request`]. `Outbound` only models text frames.
    fn poll_transmit(&mut self) -> Option<Outbound> {
        None
    }

    fn poll_timeout(&self) -> Option<Instant> {
        self.wires
            .values()
            .filter_map(|w| match w.state {
                WireState::Failed { retry_at } => retry_at,
                _ => None,
            })
            .min()
    }

    /// Sends again the subscriptions that were refused with a transient status.
    fn on_timeout(&mut self, now: Instant, _out: &mut EventSink) {
        let due: Vec<WireKey> = self
            .wires
            .iter()
            .filter(|(_, w)| matches!(w.state, WireState::Failed { retry_at: Some(t) } if t <= now))
            .map(|(k, _)| k.clone())
            .collect();
        for key in due {
            if let Some(w) = self.wires.get_mut(&key) {
                w.state = WireState::Pending;
            }
            for l in self.logical.values_mut() {
                if l.wire == key && l.state == LState::Failed {
                    l.state = LState::Pending;
                }
            }
            self.queue(&key, pb::SubscriptionAction::Subscribe);
        }
    }

    fn apply(&mut self, desired: &SubscriptionSet) {
        let keep: Vec<SubscriptionId> = desired.iter().map(|d| d.id).collect();
        self.logical.retain(|id, _| keep.contains(id));
        for DesiredSubscription { id, kind } in desired.iter() {
            if self.logical.contains_key(id) {
                continue;
            }
            let (instrument, kind) = match kind {
                SubscriptionKind::OrderBook { instrument, depth } => (
                    instrument.clone(),
                    Kind::Book {
                        depth: (*depth).max(1),
                    },
                ),
                SubscriptionKind::Quotes { instrument } => (instrument.clone(), Kind::Quotes),
                SubscriptionKind::LastTrades { instrument } => (instrument.clone(), Kind::Trades),
                SubscriptionKind::LastCandle {
                    instrument,
                    timeframe,
                } => (instrument.clone(), Kind::Candles(*timeframe)),
                // User-data kinds do not belong to this core.
                SubscriptionKind::OrderState
                | SubscriptionKind::Portfolio
                | SubscriptionKind::Executions => continue,
            };
            let wire = wire_key(&instrument, &kind);
            self.logical.insert(
                *id,
                Logical {
                    instrument,
                    kind,
                    wire,
                    state: LState::Pending,
                },
            );
        }
        self.desired = desired.clone();
        self.sync_wires();
    }
}
