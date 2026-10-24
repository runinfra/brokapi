//! The user data core: frames of the private stream (own orders, fills, positions, wallet) to events.

use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Instant;

use brokapi_bybit_protocol::http::models::{
    ExecutionRecord, OrderRecord, PositionRecord, WalletRecord,
};
use brokapi_bybit_protocol::ws::models::WsIncoming;
use brokapi_core::{ClientOrderId, Event, SubscriptionId};
use brokapi_sansio::{
    BrokerCore, CoreEvent, DesiredSubscription, EventSink, Outbound, SubscriptionKind,
    SubscriptionSet,
};
use tokio::sync::Notify;
use tracing::{debug, warn};

use super::registry::{Registry, SubState};
use crate::cid::{cid_from_link_id, foreign_cid};
use crate::map::{execution, order, portfolio};
use crate::subs::UdSubKind;

const SEEN_EXECUTIONS_CAP: usize = 20_000;
const ORDER_IDS_CAP: usize = 20_000;

/// The topic of the own orders of a category.
pub fn order_topic(category: &str) -> String {
    format!("order.{category}")
}

/// The topic of the own fills of a category.
pub fn execution_topic(category: &str) -> String {
    format!("execution.{category}")
}

/// The topic of the positions of a category.
pub fn position_topic(category: &str) -> String {
    format!("position.{category}")
}

/// The topic of the wallet.
pub const WALLET_TOPIC: &str = "wallet";

/// The sans-io core of the user data connector.
#[derive(Debug)]
pub struct UdCore {
    reg: Registry<UdSubKind>,
    category: String,
    seen_executions: HashSet<String>,
    seen_order: VecDeque<String>,
    order_ids: HashMap<String, ClientOrderId>,
    order_ids_order: VecDeque<String>,
    connects: u32,
    desired: SubscriptionSet,
}

impl UdCore {
    /// An empty core for the category of the account.
    pub fn new(category: impl Into<String>) -> Self {
        Self {
            reg: Registry::default(),
            category: category.into(),
            seen_executions: HashSet::new(),
            seen_order: VecDeque::new(),
            order_ids: HashMap::new(),
            order_ids_order: VecDeque::new(),
            connects: 0,
            desired: SubscriptionSet::new(),
        }
    }

    /// Woken whenever a subscription changes state.
    pub fn notifier(&self) -> std::sync::Arc<Notify> {
        self.reg.notifier()
    }

    /// The state of a subscription.
    pub fn sub_state(&self, id: SubscriptionId) -> (SubState, Option<String>) {
        self.reg.state(id).unwrap_or((SubState::Pending, None))
    }

    /// Registers a subscription for `topics`; `reqs` are (request id, topic) pairs of the requests that are about to
    /// be sent.
    pub fn register(
        &mut self,
        id: SubscriptionId,
        kind: UdSubKind,
        topics: &[String],
        reqs: &[(String, String)],
    ) {
        self.reg.register(id, kind, topics.to_vec());
        for (r, t) in reqs {
            self.reg.add_request(r, t);
        }
        self.refresh_desired();
    }

    /// Forgets a subscription. Returns the topics nobody listens to any more.
    pub fn forget(&mut self, id: SubscriptionId) -> Vec<String> {
        let orphans = self.reg.forget(id);
        self.refresh_desired();
        orphans
    }

    /// Whether any subscription listens to the topic.
    pub fn has_topic(&self, topic: &str) -> bool {
        self.reg.has_topic(topic)
    }

    fn refresh_desired(&mut self) {
        let mut subs: Vec<DesiredSubscription> = self
            .reg
            .iter()
            .map(|(id, kind)| DesiredSubscription {
                id,
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

    /// The cid of an order of the exchange: the one in its `orderLinkId`, else the one learned for its number, else a
    /// stable made-up one (the order is not ours).
    pub fn resolve_cid(&mut self, order_id: &str, link_id: &str) -> ClientOrderId {
        if let Some(cid) = cid_from_link_id(Some(link_id)) {
            self.learn_order_id(order_id, cid);
            return cid;
        }
        self.cid_of_order_id(order_id)
            .unwrap_or_else(|| foreign_cid(order_id))
    }

    fn on_connected(&mut self, out: &mut EventSink) {
        self.connects += 1;
        if self.connects < 2 {
            return;
        }
        // A new connection: what happened while it was down is not in the stream. The subscriptions are asked for
        // again, and the readers of orders and fills are told where the gap starts.
        self.reg.reset();
        let gap_from = crate::map::ms_to_nanos(chrono::Utc::now().timestamp_millis());
        for (id, kind) in self.reg.iter().map(|(i, k)| (i, *k)).collect::<Vec<_>>() {
            if matches!(kind, UdSubKind::OrderState | UdSubKind::Executions) {
                out.push(CoreEvent {
                    sub: Some(id),
                    payload: Event::Reconnected { sub: id, gap_from },
                });
            }
        }
    }

    fn on_data(
        &mut self,
        topic: &str,
        data: &serde_json::Value,
        now: Instant,
        out: &mut EventSink,
    ) {
        self.reg.on_data(topic);
        let subs = self.reg.subs_of(topic);
        if topic == order_topic(&self.category) {
            let Some(records) = read::<Vec<OrderRecord>>(data) else {
                return;
            };
            for r in &records {
                // A conditional order that waits is the business of the stop order list, not of the order stream.
                // It joins the stream when it fires.
                if matches!(
                    r.order_status.as_str(),
                    "Untriggered" | "Triggered" | "Deactivated"
                ) {
                    self.resolve_cid(&r.order_id, &r.order_link_id);
                    continue;
                }
                let cid = self.resolve_cid(&r.order_id, &r.order_link_id);
                if let Some(snap) = order::order_snapshot(r, cid, &self.category, now) {
                    for id in &subs {
                        out.push(CoreEvent {
                            sub: Some(*id),
                            payload: Event::Order(snap.clone()),
                        });
                    }
                }
            }
        } else if topic == execution_topic(&self.category) {
            let Some(records) = read::<Vec<ExecutionRecord>>(data) else {
                return;
            };
            for r in &records {
                // A fill the stream repeats is told once.
                if !self.seen_executions.insert(r.exec_id.clone()) {
                    continue;
                }
                self.seen_order.push_back(r.exec_id.clone());
                if self.seen_order.len() > SEEN_EXECUTIONS_CAP
                    && let Some(old) = self.seen_order.pop_front()
                {
                    self.seen_executions.remove(&old);
                }
                let cid = cid_from_link_id(Some(&r.order_link_id))
                    .inspect(|c| self.learn_order_id(&r.order_id, *c))
                    .or_else(|| self.cid_of_order_id(&r.order_id));
                if let Some(e) = execution::execution(r, cid, &self.category, now) {
                    for id in &subs {
                        out.push(CoreEvent {
                            sub: Some(*id),
                            payload: Event::Execution(e.clone()),
                        });
                    }
                }
            }
        } else if topic == position_topic(&self.category) {
            let Some(records) = read::<Vec<PositionRecord>>(data) else {
                return;
            };
            let positions: Vec<_> = records
                .iter()
                .map(|r| portfolio::position(r, &self.category, now))
                .collect();
            if !positions.is_empty() {
                for id in &subs {
                    out.push(CoreEvent {
                        sub: Some(*id),
                        payload: Event::Positions(positions.clone()),
                    });
                }
            }
        } else if topic == WALLET_TOPIC {
            let Some(records) = read::<Vec<WalletRecord>>(data) else {
                return;
            };
            let balances = portfolio::balances(&records, now);
            if !balances.is_empty() {
                for id in &subs {
                    out.push(CoreEvent {
                        sub: Some(*id),
                        payload: Event::Balances(balances.clone()),
                    });
                }
            }
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
            WsIncoming::Connected => self.on_connected(out),
            WsIncoming::Data { topic, data, .. } => {
                if !self.reg.has_topic(&topic) {
                    debug!("a frame of an unknown topic {topic} is dropped");
                    return;
                }
                self.on_data(&topic, &data, now, out);
            }
            WsIncoming::Ack {
                op,
                req_id: Some(req_id),
                success,
                message,
            } if op == "subscribe" => {
                self.reg.on_ack(&req_id, success, &message);
            }
            WsIncoming::Ack {
                op,
                success: false,
                message,
                ..
            } if op == "auth" => {
                self.reg
                    .fail_pending(&format!("the private stream refused the login: {message}"));
            }
            WsIncoming::Ack { .. } | WsIncoming::Pong | WsIncoming::Unknown(_) => {}
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
    use brokapi_bybit_protocol::ws::models::parse_incoming;
    use serde_json::json;
    use uuid::Uuid;

    fn frame(v: serde_json::Value) -> WsIncoming {
        parse_incoming(&v.to_string())
    }

    fn core_with(kind: UdSubKind, id: u64, topics: &[&str]) -> (UdCore, SubscriptionId) {
        let mut c = UdCore::new("linear");
        let sid = SubscriptionId::new(id);
        let topics: Vec<String> = topics.iter().map(|t| t.to_string()).collect();
        let reqs: Vec<(String, String)> = topics
            .iter()
            .enumerate()
            .map(|(i, t)| (format!("r{i}"), t.clone()))
            .collect();
        c.register(sid, kind, &topics, &reqs);
        (c, sid)
    }

    fn order_frame(status: &str, link: &str) -> WsIncoming {
        frame(
            json!({"id":"1","topic":"order.linear","creationTime":5,"data":[
            {"category":"linear","orderId":"77","orderLinkId":link,"symbol":"BTCUSDT","side":"Buy","orderType":"Limit",
             "orderStatus":status,"qty":"0.2","price":"100","cumExecQty":"0","updatedTime":"1700000000000"}]}),
        )
    }

    #[test]
    fn the_cid_comes_from_the_link_id_and_is_learned_for_the_number() {
        let (mut c, id) = core_with(UdSubKind::OrderState, 1001, &["order.linear"]);
        let cid = Uuid::new_v4();
        let mut out = EventSink::new();
        let now = Instant::now();
        c.on_frame(order_frame("New", &cid.to_string()), now, &mut out);
        // Later frames of the same order may come without the link id: the number is known.
        c.on_frame(order_frame("Cancelled", ""), now, &mut out);
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
        let (mut c, _) = core_with(UdSubKind::OrderState, 1, &["order.linear"]);
        let a = c.resolve_cid("9", "");
        assert_eq!(a, c.resolve_cid("9", "from the app"));
        assert_ne!(a, c.resolve_cid("10", ""));
    }

    #[test]
    fn a_conditional_order_that_waits_is_not_in_the_order_stream_but_one_that_fired_is() {
        let (mut c, _) = core_with(UdSubKind::OrderState, 1, &["order.linear"]);
        let cid = Uuid::new_v4();
        let mut out = EventSink::new();
        let now = Instant::now();
        c.on_frame(order_frame("Untriggered", &cid.to_string()), now, &mut out);
        assert!(out.drain().is_empty());
        c.on_frame(order_frame("New", ""), now, &mut out);
        let events = out.drain();
        assert_eq!(events.len(), 1);
        let Event::Order(s) = &events[0].payload else {
            panic!("an order")
        };
        assert_eq!(s.cid, cid, "the number was learned while it waited");
    }

    #[test]
    fn a_fill_is_told_once_with_the_cid_of_its_order() {
        let (mut c, id) = core_with(UdSubKind::Executions, 2, &["execution.linear"]);
        let cid = Uuid::new_v4();
        let mut out = EventSink::new();
        let now = Instant::now();
        let fill = |exec_id: &str, kind: &str| {
            frame(json!({"topic":"execution.linear","creationTime":1,"data":[
                {"category":"linear","symbol":"BTCUSDT","orderId":"77","orderLinkId":cid.to_string(),"side":"Buy",
                 "execId":exec_id,"execPrice":"100","execQty":"0.1","execFee":"0.01","execType":kind,"execTime":"1700000000000"}]}))
        };
        c.on_frame(fill("e1", "Trade"), now, &mut out);
        c.on_frame(fill("e1", "Trade"), now, &mut out);
        c.on_frame(fill("e2", "Funding"), now, &mut out);
        let events = out.drain();
        assert_eq!(
            events.len(),
            1,
            "the repeat and the funding payment are not told"
        );
        let Event::Execution(e) = &events[0].payload else {
            panic!("a fill")
        };
        assert_eq!((e.cid, events[0].sub), (Some(cid), Some(id)));
    }

    #[test]
    fn positions_and_the_wallet_go_to_the_portfolio_subscription() {
        let (mut c, id) = core_with(UdSubKind::Portfolio, 3, &["position.linear", "wallet"]);
        let mut out = EventSink::new();
        let now = Instant::now();
        c.on_frame(
            frame(json!({"topic":"position.linear","creationTime":1,"data":[
                {"category":"linear","symbol":"BTCUSDT","side":"Sell","size":"0.5","entryPrice":"81000","markPrice":"80000"}]})),
            now,
            &mut out,
        );
        c.on_frame(
            frame(json!({"topic":"wallet","creationTime":1,"data":[{"accountType":"UNIFIED","coin":[
                {"coin":"USDT","equity":"100","walletBalance":"100","totalOrderIM":"","totalPositionIM":"20"}]}]})),
            now,
            &mut out,
        );
        let events = out.drain();
        assert_eq!(events.len(), 2);
        let Event::Positions(p) = &events[0].payload else {
            panic!("positions")
        };
        assert_eq!(
            (p[0].qty.to_string(), p[0].avg_price.unwrap().to_string()),
            ("-0.5".into(), "81000".into())
        );
        let Event::Balances(b) = &events[1].payload else {
            panic!("balances")
        };
        assert_eq!(
            (b[0].blocked.to_string(), b[0].available.to_string()),
            ("20".into(), "80".into())
        );
        assert!(events.iter().all(|e| e.sub == Some(id)));
    }

    #[test]
    fn a_reconnect_tells_the_order_and_fill_readers_where_the_gap_starts() {
        let mut c = UdCore::new("linear");
        let (o, p) = (SubscriptionId::new(1), SubscriptionId::new(2));
        c.register(o, UdSubKind::OrderState, &["order.linear".into()], &[]);
        c.register(p, UdSubKind::Portfolio, &["wallet".into()], &[]);
        let mut out = EventSink::new();
        c.on_frame(WsIncoming::Connected, Instant::now(), &mut out);
        assert!(out.drain().is_empty());
        c.on_frame(WsIncoming::Connected, Instant::now(), &mut out);
        let events = out.drain();
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0].payload, Event::Reconnected { sub, .. } if sub == o));
        assert_eq!(c.sub_state(o).0, SubState::Pending);
    }

    #[test]
    fn a_refused_login_fails_the_subscriptions_that_wait() {
        let (mut c, id) = core_with(UdSubKind::OrderState, 1, &["order.linear"]);
        let mut out = EventSink::new();
        c.on_frame(
            WsIncoming::Ack {
                op: "auth".into(),
                req_id: None,
                success: false,
                message: "Params Error".into(),
            },
            Instant::now(),
            &mut out,
        );
        let (state, reason) = c.sub_state(id);
        assert_eq!(state, SubState::Failed);
        assert!(reason.unwrap().contains("Params Error"));
        assert!(c.poll_transmit().is_none() && c.poll_timeout().is_none());
    }
}
