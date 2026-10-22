//! The user data core: frames of the data channel (own orders, fills, positions) to events.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use brokapi_alor_protocol::http::models::{OrderRecord, PositionRecord, TradeRecord};
use brokapi_alor_protocol::ws::models::WsIncoming;
use brokapi_core::{ClientOrderId, Event, SubscriptionId};
use brokapi_sansio::{
    BrokerCore, CoreEvent, DesiredSubscription, EventSink, Outbound, SubscriptionKind,
    SubscriptionSet,
};
use tokio::sync::Notify;
use tracing::warn;

use crate::cid::{cid_from_comment, foreign_cid};
use crate::core::md::SubState;
use crate::map::{execution, order, portfolio};
use crate::subs::UdSubKind;

/// The boards of the instruments of a portfolio, by `EXCHANGE:TICKER`. The positions of the broker do not name
/// the board of an instrument, so the connector looks it up and keeps it here, where the core reads it.
pub type BoardBook = Arc<Mutex<HashMap<String, String>>>;

/// The key of an instrument in a [`BoardBook`].
pub fn board_key(exchange: &str, ticker: &str) -> String {
    format!("{exchange}:{ticker}")
}

const SEEN_EXECUTIONS_CAP: usize = 20_000;
const ORDER_IDS_CAP: usize = 20_000;

/// The sans-io core of the user data connector.
#[derive(Debug)]
pub struct UdCore {
    desired: SubscriptionSet,
    by_guid: HashMap<String, (SubscriptionId, UdSubKind)>,
    seen_executions: HashSet<String>,
    seen_order: VecDeque<String>,
    order_ids: HashMap<String, ClientOrderId>,
    order_ids_order: VecDeque<String>,
    boards: BoardBook,
    exchange: String,
    connects: u32,
    states: HashMap<SubscriptionId, (SubState, Option<String>)>,
    ack: Arc<Notify>,
}

impl UdCore {
    /// An empty core for the exchange of the portfolio, with the book of boards it reads positions from.
    pub fn new(exchange: impl Into<String>, boards: BoardBook) -> Self {
        Self {
            desired: SubscriptionSet::new(),
            by_guid: HashMap::new(),
            seen_executions: HashSet::new(),
            seen_order: VecDeque::new(),
            order_ids: HashMap::new(),
            order_ids_order: VecDeque::new(),
            boards,
            exchange: exchange.into(),
            connects: 0,
            states: HashMap::new(),
            ack: Arc::new(Notify::new()),
        }
    }

    /// Woken whenever a subscription changes state. The connector waits on it for the confirmation of a new
    /// subscription.
    pub fn notifier(&self) -> Arc<Notify> {
        Arc::clone(&self.ack)
    }

    /// The state of a subscription.
    pub fn actual_state(&self, id: SubscriptionId) -> Option<SubState> {
        self.states.get(&id).map(|(s, _)| *s)
    }

    /// The reason the broker gave when it refused a subscription.
    pub fn failure(&self, id: SubscriptionId) -> Option<&str> {
        self.states.get(&id).and_then(|(_, r)| r.as_deref())
    }

    /// Registers a subscription: frames with this `guid` belong to `id` and are read as `kind`.
    pub fn register(&mut self, guid: &str, id: SubscriptionId, kind: UdSubKind) {
        self.by_guid.insert(guid.to_string(), (id, kind));
        self.states.insert(id, (SubState::Pending, None));
        self.refresh_desired();
    }

    /// Forgets a subscription.
    pub fn forget(&mut self, guid: &str) {
        if let Some((id, _)) = self.by_guid.remove(guid) {
            self.states.remove(&id);
        }
        self.refresh_desired();
    }

    fn refresh_desired(&mut self) {
        let mut subs: Vec<DesiredSubscription> = self
            .by_guid
            .values()
            .map(|(id, kind)| DesiredSubscription {
                id: *id,
                kind: match kind {
                    UdSubKind::OrderState => SubscriptionKind::OrderState,
                    UdSubKind::Portfolio => SubscriptionKind::Portfolio,
                    UdSubKind::Executions => SubscriptionKind::Executions,
                },
            })
            .collect();
        subs.sort_by_key(|s| s.id.value());
        self.desired = SubscriptionSet::with_subscriptions(subs);
    }

    /// Remembers that the order with this number is the order of `cid`.
    pub fn learn_order_id(&mut self, order_id: &str, cid: ClientOrderId) {
        if order_id.is_empty() || cid.is_nil() {
            return;
        }
        if self.order_ids.insert(order_id.to_string(), cid).is_none() {
            self.order_ids_order.push_back(order_id.to_string());
            if self.order_ids_order.len() > ORDER_IDS_CAP
                && let Some(old) = self.order_ids_order.pop_front()
            {
                self.order_ids.remove(&old);
            }
        }
    }

    /// The cid of the order with this number, if the core has seen it.
    pub fn cid_of_order_id(&self, order_id: &str) -> Option<ClientOrderId> {
        self.order_ids.get(order_id).copied()
    }

    /// The cid of an order of the broker: the one in its comment, else the one learned for its number, else a
    /// stable made-up one (the order is not ours).
    pub fn resolve_cid(&mut self, order_id: &str, comment: Option<&str>) -> ClientOrderId {
        if let Some(cid) = cid_from_comment(comment) {
            self.learn_order_id(order_id, cid);
            return cid;
        }
        self.cid_of_order_id(order_id)
            .unwrap_or_else(|| foreign_cid(order_id))
    }

    fn find(&self, kind: UdSubKind) -> Option<SubscriptionId> {
        self.by_guid
            .values()
            .find(|(_, k)| *k == kind)
            .map(|(id, _)| *id)
    }

    fn on_connect(&mut self, out: &mut EventSink) {
        self.connects += 1;
        if self.connects < 2 {
            return;
        }
        for (state, _) in self.states.values_mut() {
            *state = SubState::Pending;
        }
        let gap_from = crate::map::unix_nanos(chrono::Utc::now());
        for kind in [UdSubKind::OrderState, UdSubKind::Executions] {
            if let Some(sub) = self.find(kind) {
                out.push(CoreEvent {
                    sub: Some(sub),
                    payload: Event::Reconnected { sub, gap_from },
                });
            }
        }
    }

    fn on_data(&mut self, guid: &str, data: serde_json::Value, now: Instant, out: &mut EventSink) {
        let Some(&(id, kind)) = self.by_guid.get(guid) else {
            return;
        };
        if self
            .states
            .get(&id)
            .is_some_and(|(s, _)| *s != SubState::Active)
        {
            self.states.insert(id, (SubState::Active, None));
            self.ack.notify_waiters();
        }
        let payload = match kind {
            UdSubKind::OrderState => read::<OrderRecord>(&data).and_then(|r| {
                let cid = self.resolve_cid(&r.id, r.comment.as_deref());
                order::order_snapshot(&r, cid, now).map(Event::Order)
            }),
            UdSubKind::Executions => read::<TradeRecord>(&data).and_then(|r| {
                // A trade the history of the subscription repeats, or one seen twice, is told once.
                if !self.seen_executions.insert(r.id.clone()) {
                    return None;
                }
                self.seen_order.push_back(r.id.clone());
                if self.seen_order.len() > SEEN_EXECUTIONS_CAP
                    && let Some(old) = self.seen_order.pop_front()
                {
                    self.seen_executions.remove(&old);
                }
                let order_no = r.orderno.clone().unwrap_or_default();
                let cid = cid_from_comment(r.comment.as_deref())
                    .or_else(|| self.cid_of_order_id(&order_no));
                execution::execution(&r, cid, now).map(Event::Execution)
            }),
            UdSubKind::Portfolio => read::<PositionRecord>(&data).map(|r| {
                if portfolio::is_money(&r) {
                    Event::Balances(vec![portfolio::balance(&r, now)])
                } else {
                    let board = self
                        .boards
                        .lock()
                        .ok()
                        .and_then(|b| b.get(&board_key(&self.exchange, &r.symbol)).cloned())
                        .unwrap_or_default();
                    Event::Positions(vec![portfolio::position(&r, &board, now)])
                }
            }),
        };
        if let Some(payload) = payload {
            out.push(CoreEvent {
                sub: Some(id),
                payload,
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

impl BrokerCore for UdCore {
    type Frame = WsIncoming;

    fn on_frame(&mut self, frame: Self::Frame, now: Instant, out: &mut EventSink) {
        match frame {
            WsIncoming::Reconnected => self.on_connect(out),
            WsIncoming::Data { guid, data } => self.on_data(&guid, data, now, out),
            WsIncoming::Status {
                guid: Some(guid),
                code,
                message,
            } => {
                if let Some(&(id, _)) = self.by_guid.get(&guid) {
                    let st = if code == 200 {
                        (SubState::Active, None)
                    } else {
                        (SubState::Failed, Some(message))
                    };
                    self.states.insert(id, st);
                    self.ack.notify_waiters();
                }
            }
            WsIncoming::Status { .. } | WsIncoming::Unknown(_) => {}
        }
    }

    fn poll_transmit(&mut self) -> Option<Outbound> {
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
    use serde_json::json;
    use uuid::Uuid;

    fn core() -> UdCore {
        UdCore::new("MOEX", BoardBook::default())
    }

    fn data(guid: &str, d: serde_json::Value) -> WsIncoming {
        WsIncoming::Data {
            guid: guid.into(),
            data: d,
        }
    }

    #[test]
    fn the_cid_comes_from_the_comment_and_is_learned_for_the_number() {
        let mut c = core();
        let id = SubscriptionId::new(1001);
        c.register("o", id, UdSubKind::OrderState);
        let cid = Uuid::new_v4();
        let mut out = EventSink::new();
        let now = Instant::now();
        let rec = |status: &str, comment: Option<String>| {
            json!({"id":"77","symbol":"SBER","board":"TQBR","type":"limit","side":"buy","status":status,
                   "qtyUnits":20,"filledQtyUnits":0,"price":100,"comment":comment})
        };
        c.on_frame(
            data("o", rec("working", Some(cid.to_string()))),
            now,
            &mut out,
        );
        // Later frames of the same order may come without the comment: the number is known.
        c.on_frame(data("o", rec("canceled", None)), now, &mut out);
        let events = out.drain();
        assert_eq!(events.len(), 2);
        for e in &events {
            let Event::Order(s) = &e.payload else {
                panic!("an order")
            };
            assert_eq!(s.cid, cid);
            assert_eq!(e.sub, Some(id));
        }
    }

    #[test]
    fn an_order_that_is_not_ours_gets_a_stable_made_up_cid() {
        let mut c = core();
        assert_eq!(
            c.resolve_cid("500", Some("someone else's note")),
            foreign_cid("500")
        );
        assert_eq!(c.resolve_cid("500", None), foreign_cid("500"));
    }

    #[test]
    fn a_fill_is_told_once_and_carries_the_cid_of_its_order() {
        let mut c = core();
        c.register("t", SubscriptionId::new(1002), UdSubKind::Executions);
        let cid = Uuid::new_v4();
        c.learn_order_id("77", cid);
        let mut out = EventSink::new();
        let f = || {
            data(
                "t",
                json!({"id":"T1","orderno":"77","symbol":"SBER","board":"TQBR","qtyUnits":20,"price":100.5,"side":"buy"}),
            )
        };
        c.on_frame(f(), Instant::now(), &mut out);
        c.on_frame(f(), Instant::now(), &mut out);
        let events = out.drain();
        assert_eq!(events.len(), 1);
        let Event::Execution(e) = &events[0].payload else {
            panic!("an execution")
        };
        assert_eq!(e.cid, Some(cid));
    }

    #[test]
    fn a_position_uses_the_board_the_connector_looked_up() {
        let boards = BoardBook::default();
        boards
            .lock()
            .unwrap()
            .insert(board_key("MOEX", "SBER"), "TQBR".into());
        let mut c = UdCore::new("MOEX", boards);
        c.register("p", SubscriptionId::new(1003), UdSubKind::Portfolio);
        let mut out = EventSink::new();
        c.on_frame(
            data(
                "p",
                json!({"symbol":"SBER","qtyUnits":30,"isCurrency":false}),
            ),
            Instant::now(),
            &mut out,
        );
        c.on_frame(
            data(
                "p",
                json!({"symbol":"RUB","qtyUnits":1000,"isCurrency":true}),
            ),
            Instant::now(),
            &mut out,
        );
        let events = out.drain();
        let Event::Positions(p) = &events[0].payload else {
            panic!("positions")
        };
        assert_eq!(p[0].instrument.key.display(), "SBER@TQBR");
        assert!(matches!(events[1].payload, Event::Balances(_)));
    }

    #[test]
    fn a_reconnect_tells_the_order_and_fill_subscriptions_from_the_second_connect_on() {
        let mut c = core();
        c.register("o", SubscriptionId::new(1001), UdSubKind::OrderState);
        c.register("p", SubscriptionId::new(1003), UdSubKind::Portfolio);
        let mut out = EventSink::new();
        c.on_frame(WsIncoming::Reconnected, Instant::now(), &mut out);
        assert!(out.drain().is_empty());
        c.on_frame(WsIncoming::Reconnected, Instant::now(), &mut out);
        let events = out.drain();
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0].payload, Event::Reconnected { .. }));
    }
}
