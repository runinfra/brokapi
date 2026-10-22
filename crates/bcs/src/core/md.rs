//! Market-data sans-io core.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use brokapi_bcs_protocol::models::instrument::InstrumentKey as BcsInstrumentKey;
use brokapi_bcs_protocol::models::messages::IncomingMessage;
use brokapi_bcs_protocol::ws::client::BcsWsChannel;
use brokapi_bcs_protocol::ws::models::{MarketDataMessage, MarketDataSubscribe};
use brokapi_bcs_protocol::ws::parse_incoming;
use brokapi_core::{Event, ResyncReason, SubscriptionId, Timeframe};
use brokapi_sansio::{
    BrokerCore, CoreEvent, DesiredSubscription, EventSink, Outbound, SubscriptionKind,
    SubscriptionSet,
};
use tokio::sync::Notify;

use crate::map::market::{map_ws_market_data, parse_subscribe_error};

/// The state of one subscription as the core sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubState {
    /// The subscribe message is queued or sent, and the broker has not confirmed it yet. Also the
    /// state after a reconnect, until the broker confirms again. Data that arrives for a pending
    /// subscription marks it active, because data can overtake the confirmation.
    Pending,
    /// The broker confirmed the subscription, or data for it has arrived.
    Active,
    /// The broker answered with an error while the subscription was pending. The core does not
    /// retry. The connector reports it to the caller.
    Failed,
}

/// Sans-io market data core of the BCS market data WebSocket.
///
/// It turns inbound frames into [`Event`]s (order books, quotes, trades, candles), tracks which
/// subscriptions the broker has confirmed, and after a reconnect tells the consumer that the order
/// books are void (`Event::Resync` with [`ResyncReason::SubscriptionReset`]). It does no I/O.
///
/// # Examples
///
/// Ask for an order book subscription and read the message the core wants to send:
///
/// ```
/// use std::time::Instant;
/// use brokapi_bcs::{MdCore, core::SubState};
/// use brokapi_core::{InstrumentId, InstrumentKey, NativeId, SubscriptionId};
/// use brokapi_sansio::{
///     BrokerCore, DesiredSubscription, EventSink, Outbound, SubscriptionKind, SubscriptionSet,
/// };
///
/// let sber = InstrumentId::new(
///     InstrumentKey::new("SBER", "TQBR"),
///     NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() },
/// );
/// let sub = SubscriptionId::new(1);
/// let mut core = MdCore::new();
/// core.apply(&SubscriptionSet::with_subscriptions(vec![DesiredSubscription {
///     id: sub,
///     kind: SubscriptionKind::OrderBook { instrument: sber, depth: 10 },
/// }]));
///
/// let Some(Outbound::WsText(text)) = core.poll_transmit() else { panic!("a subscribe message") };
/// assert!(text.contains("\"subscribeType\":0"));
/// // Waiting for the broker's confirmation.
/// assert_eq!(core.actual_state(sub), Some(SubState::Pending));
///
/// // A subscribe error from the broker fails the subscriptions that wait.
/// let mut out = EventSink::new();
/// core.feed_raw(r#"{"errors":[{"message":"unknown instrument"}]}"#, Instant::now(), &mut out);
/// assert_eq!(core.actual_state(sub), Some(SubState::Failed));
/// ```
#[derive(Debug)]
pub struct MdCore {
    desired: SubscriptionSet,
    actual: HashMap<SubscriptionId, SubState>,
    sub_by_id: HashMap<SubscriptionId, DesiredSubscription>,
    outbound: VecDeque<Outbound>,
    book_seq: HashMap<SubscriptionId, u64>,
    heartbeat_deadline: Option<Instant>,
    /// How many times the WebSocket session has connected (the first connect included).
    connections: u32,
    /// Why a subscription failed, in the words of the broker.
    failures: HashMap<SubscriptionId, String>,
    /// Woken whenever a subscription changes state (confirmed or refused): the connector waits on it in
    /// `subscribe_*` until the broker has answered.
    ack: Arc<Notify>,
}

impl Default for MdCore {
    fn default() -> Self {
        Self::new()
    }
}

impl MdCore {
    /// Creates a core with no subscriptions.
    pub fn new() -> Self {
        Self {
            desired: SubscriptionSet::new(),
            actual: HashMap::new(),
            sub_by_id: HashMap::new(),
            outbound: VecDeque::new(),
            book_seq: HashMap::new(),
            heartbeat_deadline: None,
            connections: 0,
            failures: HashMap::new(),
            ack: Arc::new(Notify::new()),
        }
    }

    /// The signal that is given whenever a subscription is confirmed or refused. A connector waits on it
    /// (with [`brokapi_runtime::wait_ack`]) to hold `subscribe_*` until the broker has answered.
    pub fn notifier(&self) -> Arc<Notify> {
        Arc::clone(&self.ack)
    }

    /// Why subscription `id` failed, in the words of the broker, if it did.
    pub fn failure(&self, id: SubscriptionId) -> Option<&str> {
        self.failures.get(&id).map(String::as_str)
    }

    /// Subscriptions the connector asked for (input of the last `apply`).
    pub fn desired(&self) -> &SubscriptionSet {
        &self.desired
    }

    /// The state of subscription `id`, or `None` if the core does not know it (never applied, or
    /// already unsubscribed).
    pub fn actual_state(&self, id: SubscriptionId) -> Option<SubState> {
        self.actual.get(&id).copied()
    }

    /// The session connected again. Its subscriptions are restored by the session itself, so nothing is
    /// sent here, but every order book is void: the stream starts over with a new snapshot, and the
    /// consumer is told so with `Resync { SubscriptionReset }` (before the new snapshot, which comes after
    /// the restore). The subscriptions wait for their acknowledgements again.
    fn on_reconnected(&mut self, out: &mut EventSink) {
        self.connections += 1;
        if self.connections < 2 {
            return;
        }
        for sub in self.desired.subs() {
            if self.actual.contains_key(&sub.id) {
                self.actual.insert(sub.id, SubState::Pending);
            }
            if let SubscriptionKind::OrderBook { instrument, .. } = &sub.kind {
                self.book_seq.remove(&sub.id);
                out.push(CoreEvent {
                    sub: Some(sub.id),
                    payload: Event::Resync {
                        sub: sub.id,
                        instrument: instrument.clone(),
                        reason: ResyncReason::SubscriptionReset,
                    },
                });
            }
        }
    }

    fn queue_subscribe(&mut self, sub: &DesiredSubscription) {
        let msg = match &sub.kind {
            SubscriptionKind::OrderBook { instrument, depth } => {
                let key = bcs_key(instrument);
                MarketDataSubscribe::order_book(vec![key], *depth)
            }
            SubscriptionKind::Quotes { instrument } => {
                MarketDataSubscribe::quotes(vec![bcs_key(instrument)])
            }
            SubscriptionKind::LastTrades { instrument } => {
                MarketDataSubscribe::last_trades(vec![bcs_key(instrument)])
            }
            SubscriptionKind::LastCandle {
                instrument,
                timeframe,
            } => MarketDataSubscribe::last_candle(
                vec![bcs_key(instrument)],
                timeframe_label(*timeframe),
            ),
            SubscriptionKind::OrderState
            | SubscriptionKind::Portfolio
            | SubscriptionKind::Executions => return,
        };
        self.outbound
            .push_back(Outbound::WsText(serde_json::to_string(&msg).unwrap()));
        self.actual.insert(sub.id, SubState::Pending);
        self.sub_by_id.insert(sub.id, sub.clone());
    }

    fn queue_unsubscribe(&mut self, sub: &DesiredSubscription) {
        let msg = match &sub.kind {
            SubscriptionKind::OrderBook { instrument, depth } => {
                MarketDataSubscribe::unsubscribe_order_book(vec![bcs_key(instrument)], *depth)
            }
            SubscriptionKind::Quotes { instrument } => {
                MarketDataSubscribe::unsubscribe_quotes(vec![bcs_key(instrument)])
            }
            SubscriptionKind::LastTrades { instrument } => {
                MarketDataSubscribe::unsubscribe_last_trades(vec![bcs_key(instrument)])
            }
            SubscriptionKind::LastCandle {
                instrument,
                timeframe,
            } => MarketDataSubscribe::unsubscribe_last_candle(
                vec![bcs_key(instrument)],
                timeframe_label(*timeframe),
            ),
            SubscriptionKind::OrderState
            | SubscriptionKind::Portfolio
            | SubscriptionKind::Executions => return,
        };
        self.outbound
            .push_back(Outbound::WsText(serde_json::to_string(&msg).unwrap()));
        self.actual.remove(&sub.id);
        self.failures.remove(&sub.id);
        self.sub_by_id.remove(&sub.id);
        self.book_seq.remove(&sub.id);
    }

    fn handle_market_data(&mut self, md: MarketDataMessage, now: Instant, out: &mut EventSink) {
        match &md {
            MarketDataMessage::OrderBookSuccess(_)
            | MarketDataMessage::QuotesSuccess(_)
            | MarketDataMessage::LastTradesSuccess(_)
            | MarketDataMessage::LastCandleSuccess(_) => {
                if let Some(sub_id) = self.match_success(&md) {
                    self.actual.insert(sub_id, SubState::Active);
                    self.ack.notify_waiters();
                }
                return;
            }
            MarketDataMessage::ErrorResponse(err) => {
                let reason = err
                    .errors
                    .first()
                    .map(|e| e.message.clone())
                    .or_else(|| err.error.as_ref().map(|e| e.message.clone()))
                    .unwrap_or_else(|| "subscription error".into());
                self.fail_pending(&reason);
                return;
            }
            _ => {}
        }

        if let Some(mapped) = map_ws_market_data(&md, now, None) {
            let sub_id = self.match_data_sub(&md);
            // Data that comes for a pending subscription proves it: the confirmation can be late or lost.
            if let Some(id) = sub_id
                && self.actual.get(&id) == Some(&SubState::Pending)
            {
                self.actual.insert(id, SubState::Active);
                self.ack.notify_waiters();
            }
            let payload = mapped.event;
            if let (Some(sub), Event::OrderBook(_)) = (sub_id, &payload) {
                let seq = self.book_seq.entry(sub).or_insert(0);
                *seq += 1;
            }
            out.push(CoreEvent {
                sub: sub_id.or(mapped.sub),
                payload,
            });
        }
    }

    fn match_success(&self, md: &MarketDataMessage) -> Option<SubscriptionId> {
        for sub in self.desired.subs() {
            if self.actual.get(&sub.id) != Some(&SubState::Pending) {
                continue;
            }
            let matches = matches!(
                (&sub.kind, md),
                (
                    SubscriptionKind::OrderBook { .. },
                    MarketDataMessage::OrderBookSuccess(_)
                ) | (
                    SubscriptionKind::Quotes { .. },
                    MarketDataMessage::QuotesSuccess(_)
                ) | (
                    SubscriptionKind::LastTrades { .. },
                    MarketDataMessage::LastTradesSuccess(_)
                ) | (
                    SubscriptionKind::LastCandle { .. },
                    MarketDataMessage::LastCandleSuccess(_)
                )
            );
            if matches {
                return Some(sub.id);
            }
        }
        None
    }

    fn match_data_sub(&self, md: &MarketDataMessage) -> Option<SubscriptionId> {
        for sub in self.desired.subs() {
            // Data can overtake the success ack (or the ack can be lost on reconnect):
            // a pending subscription that receives data is effectively active.
            if !matches!(
                self.actual.get(&sub.id),
                Some(SubState::Active | SubState::Pending)
            ) {
                continue;
            }
            let matches = match (&sub.kind, md) {
                (
                    SubscriptionKind::OrderBook { instrument, .. },
                    MarketDataMessage::OrderBook(d),
                ) => same_instrument(instrument, &d.ticker, &d.class_code),
                (SubscriptionKind::Quotes { instrument }, MarketDataMessage::Quotes(d)) => {
                    same_instrument(instrument, &d.ticker, &d.class_code)
                }
                (SubscriptionKind::LastTrades { instrument }, MarketDataMessage::LastTrades(d)) => {
                    same_instrument(instrument, &d.ticker, &d.class_code)
                }
                (
                    SubscriptionKind::LastCandle { instrument, .. },
                    MarketDataMessage::LastCandle(d),
                ) => d
                    .ticker
                    .as_deref()
                    .zip(d.class_code.as_deref())
                    .is_some_and(|(t, c)| same_instrument(instrument, t, c)),
                _ => false,
            };
            if matches {
                return Some(sub.id);
            }
        }
        None
    }

    /// Fails the pending subscriptions the error frame is about. The frame names no subscription, only a
    /// code and a text. So: if exactly one subscription is pending, it is that one. If several are, the
    /// ones whose ticker the text mentions. If the text names none of them (or there is no text to read),
    /// the error cannot be placed and all pending subscriptions fail, as the safe reading.
    fn fail_pending(&mut self, reason: &str) {
        let pending: Vec<SubscriptionId> = self
            .actual
            .iter()
            .filter(|(_, st)| **st == SubState::Pending)
            .map(|(id, _)| *id)
            .collect();
        let text = reason.to_uppercase();
        let mentioned: Vec<SubscriptionId> = self
            .desired
            .subs()
            .iter()
            .filter(|s| pending.contains(&s.id))
            .filter(|s| {
                let symbol = match &s.kind {
                    SubscriptionKind::OrderBook { instrument, .. }
                    | SubscriptionKind::Quotes { instrument }
                    | SubscriptionKind::LastTrades { instrument }
                    | SubscriptionKind::LastCandle { instrument, .. } => &instrument.key.symbol,
                    _ => return false,
                };
                !symbol.is_empty() && text.contains(&symbol.to_uppercase())
            })
            .map(|s| s.id)
            .collect();
        let failed: Vec<SubscriptionId> = if pending.len() == 1 || mentioned.is_empty() {
            pending
        } else {
            mentioned
        };
        for id in failed {
            self.actual.insert(id, SubState::Failed);
            self.failures.insert(id, reason.to_string());
        }
        self.ack.notify_waiters();
    }

    /// Feeds one raw text frame of the market data WebSocket.
    ///
    /// A frame with an `errors` array is a subscribe error: the pending subscription it is about becomes
    /// [`SubState::Failed`] (all pending ones if the frame does not say which, see `fail_pending`) and no
    /// event is pushed. Any other frame is parsed and handled like
    /// [`on_frame`](BrokerCore::on_frame) does. A frame that cannot be parsed is skipped, there is
    /// no error to return.
    pub fn feed_raw(&mut self, text: &str, now: Instant, out: &mut EventSink) {
        if let Some(reason) = parse_subscribe_error(text) {
            self.fail_pending(&reason);
            return;
        }
        let frame = parse_incoming(text, BcsWsChannel::MarketData);
        self.on_frame(frame, now, out);
    }
}

impl BrokerCore for MdCore {
    type Frame = IncomingMessage;

    fn on_frame(&mut self, frame: Self::Frame, now: Instant, out: &mut EventSink) {
        self.heartbeat_deadline = Some(now + Duration::from_secs(30));
        match frame {
            IncomingMessage::MarketData(md) => self.handle_market_data(md, now, out),
            IncomingMessage::Error(err) => self.fail_pending(&err.message),
            // A refusal of a subscribe has an `errors` list in place of data. The parser of the session does not
            // know that shape and hands it over as text.
            IncomingMessage::Unknown(text) => {
                if let Some(reason) = parse_subscribe_error(&text) {
                    self.fail_pending(&reason);
                }
            }
            IncomingMessage::Reconnected => self.on_reconnected(out),
            _ => {}
        }
    }

    fn poll_transmit(&mut self) -> Option<Outbound> {
        self.outbound.pop_front()
    }

    fn poll_timeout(&self) -> Option<Instant> {
        self.heartbeat_deadline
    }

    fn on_timeout(&mut self, _now: Instant, _out: &mut EventSink) {
        self.heartbeat_deadline = None;
    }

    fn apply(&mut self, desired: &SubscriptionSet) {
        let old_ids: Vec<_> = self.desired.subs().iter().map(|s| s.id).collect();
        let new_ids: Vec<_> = desired.subs().iter().map(|s| s.id).collect();

        let to_unsub: Vec<_> = self
            .desired
            .subs()
            .iter()
            .filter(|sub| !new_ids.contains(&sub.id))
            .cloned()
            .collect();
        for sub in to_unsub {
            self.queue_unsubscribe(&sub);
        }

        self.desired = desired.clone();
        self.sub_by_id.clear();
        for sub in self.desired.subs() {
            self.sub_by_id.insert(sub.id, sub.clone());
        }

        let to_sub: Vec<_> = self
            .desired
            .subs()
            .iter()
            .filter(|sub| {
                !old_ids.contains(&sub.id) || self.actual.get(&sub.id) == Some(&SubState::Failed)
            })
            .cloned()
            .collect();
        for sub in to_sub {
            self.queue_subscribe(&sub);
        }
    }
}

fn bcs_key(instrument: &brokapi_core::InstrumentId) -> BcsInstrumentKey {
    match &instrument.native {
        brokapi_core::NativeId::TickerClass { ticker, class_code } => {
            BcsInstrumentKey::new(ticker.clone(), class_code.clone())
        }
        _ => BcsInstrumentKey::new(instrument.key.symbol.clone(), instrument.key.mic.clone()),
    }
}

fn same_instrument(
    instrument: &brokapi_core::InstrumentId,
    ticker: &str,
    class_code: &str,
) -> bool {
    match &instrument.native {
        brokapi_core::NativeId::TickerClass {
            ticker: t,
            class_code: c,
        } => t == ticker && c == class_code,
        _ => instrument.key.symbol == ticker && instrument.key.mic == class_code,
    }
}

fn timeframe_label(tf: Timeframe) -> &'static str {
    match tf {
        Timeframe::M1 => "M1",
        Timeframe::M5 => "M5",
        // BCS has no M10: the connector refuses it before a subscription reaches the core.
        Timeframe::M10 => "M10",
        Timeframe::M15 => "M15",
        Timeframe::M30 => "M30",
        Timeframe::H1 => "H1",
        Timeframe::H4 => "H4",
        Timeframe::D1 => "D",
        Timeframe::W1 => "W",
        Timeframe::MN => "MN",
    }
}

/// Detects a sequence gap and emits a resync: a hook for order book integrity.
pub fn check_book_seq(
    core: &mut MdCore,
    sub: SubscriptionId,
    expected: u64,
    instrument: brokapi_core::InstrumentId,
    out: &mut EventSink,
) {
    let seq = core.book_seq.get(&sub).copied().unwrap_or(0);
    if seq > 0 && seq != expected {
        out.push(CoreEvent {
            sub: Some(sub),
            payload: brokapi_core::Event::Resync {
                sub,
                instrument,
                reason: ResyncReason::SequenceGap,
            },
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use brokapi_core::{InstrumentId, InstrumentKey, NativeId};

    fn sber() -> InstrumentId {
        InstrumentId::new(
            InstrumentKey::new("SBER", "TQBR"),
            NativeId::TickerClass {
                ticker: "SBER".into(),
                class_code: "TQBR".into(),
            },
        )
    }

    #[test]
    fn apply_queues_subscribe_outbound() {
        let mut core = MdCore::new();
        let sub_id = SubscriptionId::new(1);
        let desired = SubscriptionSet::with_subscriptions(vec![DesiredSubscription {
            id: sub_id,
            kind: SubscriptionKind::OrderBook {
                instrument: sber(),
                depth: 20,
            },
        }]);
        core.apply(&desired);
        let msg = core.poll_transmit().expect("subscribe outbound");
        match msg {
            Outbound::WsText(json) => {
                assert!(json.contains("\"subscribeType\":0"));
                assert!(json.contains("\"dataType\":0"));
            }
        }
        assert_eq!(core.actual_state(sub_id), Some(SubState::Pending));
    }

    #[test]
    fn reconnect_marks_every_order_book_but_the_first_connect_does_not() {
        let mut core = MdCore::new();
        let (book, quotes) = (SubscriptionId::new(1), SubscriptionId::new(2));
        core.apply(&SubscriptionSet::with_subscriptions(vec![
            DesiredSubscription {
                id: book,
                kind: SubscriptionKind::OrderBook {
                    instrument: sber(),
                    depth: 20,
                },
            },
            DesiredSubscription {
                id: quotes,
                kind: SubscriptionKind::Quotes { instrument: sber() },
            },
        ]));
        let mut out = EventSink::new();
        let now = Instant::now();

        core.on_frame(IncomingMessage::Reconnected, now, &mut out);
        assert!(out.drain().is_empty(), "the first connect is not a loss");

        core.on_frame(IncomingMessage::Reconnected, now, &mut out);
        let events = out.drain();
        assert_eq!(
            events.len(),
            1,
            "one Resync per order book subscription, none for quotes"
        );
        assert_eq!(events[0].sub, Some(book));
        assert!(matches!(
            events[0].payload,
            Event::Resync { sub, reason: ResyncReason::SubscriptionReset, .. } if sub == book
        ));
        // The restored subscriptions are acknowledged again.
        assert_eq!(core.actual_state(book), Some(SubState::Pending));
        assert_eq!(core.actual_state(quotes), Some(SubState::Pending));
    }
}
