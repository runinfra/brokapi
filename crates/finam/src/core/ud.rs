//! User-data core: order state, own trades, account (portfolio) updates.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use brokapi_core::{Event, OrderState, SubscriptionId};
use brokapi_finam_protocol::pb::grpc::tradeapi::v1::accounts::GetAccountResponse;
use brokapi_finam_protocol::pb::grpc::tradeapi::v1::orders as pb;
use brokapi_runtime::OrderRuntime;
use brokapi_sansio::{
    BrokerCore, CoreEvent, EventSink, OrderSignal, Outbound, SubscriptionKind, SubscriptionSet,
};

use super::SubStatus;
use crate::index::OrderIndex;
use crate::map::order::map_order_state;
use crate::map::portfolio::{map_account, map_trade};

/// What arrived on a user data stream.
// Boxing the large variant would change the public shape of the enum; these values are short-lived.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum UdEvent {
    /// A message of the order stream: current states of changed orders.
    Orders(pb::SubscribeOrdersResponse),
    /// A message of the own trades stream.
    Trades(pb::SubscribeTradesResponse),
    /// A message of the account stream: the whole account state.
    Account(GetAccountResponse),
    /// The stream was opened (also after a reopen).
    Connected,
    /// The stream broke or ended and is being reopened. The text says why.
    Disconnected(String),
    /// The broker refuses the stream (no permission). The text says why.
    Failed(String),
}

/// One stream event of one subscription.
#[derive(Debug, Clone)]
pub struct UdFrame {
    /// The subscription whose stream produced the event.
    pub sub: SubscriptionId,
    /// What happened on that stream.
    pub event: UdEvent,
}

/// What a user data subscription carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UdSubKind {
    /// Order state: `Event::Order`.
    Orders,
    /// Own trades: `Event::Execution`.
    Executions,
    /// Account: `Event::Portfolio`.
    Portfolio,
}

/// User data core: turns stream frames into order, execution and portfolio events.
///
/// It resolves the client order id of every order echo through the [`OrderIndex`] and advances the
/// local lifecycle of orders this process placed (so `order_state` and the stream agree).
///
/// Implements `BrokerCore` from `brokapi-sansio`.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use std::time::Instant;
/// use brokapi_core::{Event, SubscriptionId};
/// use brokapi_finam::core::ud::UdSubKind;
/// use brokapi_finam::core::{SubStatus, UdCore, UdEvent, UdFrame};
/// use brokapi_finam::index::OrderIndex;
/// use brokapi_sansio::{BrokerCore, EventSink};
/// use brokapi_finam_protocol::pb::grpc::tradeapi::v1::orders as po;
///
/// let sub = SubscriptionId::new(1000);
/// let mut core = UdCore::new(Arc::new(OrderIndex::new()));
/// core.register(sub, UdSubKind::Orders);
/// assert_eq!(core.status(sub), Some(SubStatus::Connecting));
///
/// // An order that this process did not place: it still reaches the consumer, under a made-up cid.
/// let echo = po::OrderState { order_id: "ORD-7".into(), ..Default::default() };
/// let mut out = EventSink::new();
/// core.on_frame(
///     UdFrame { sub, event: UdEvent::Orders(po::SubscribeOrdersResponse { orders: vec![echo] }) },
///     Instant::now(),
///     &mut out,
/// );
/// assert_eq!(core.status(sub), Some(SubStatus::Active));
/// assert!(matches!(out.drain()[0].payload, Event::Order(_)));
/// ```
pub struct UdCore {
    subs: HashMap<SubscriptionId, UdSubKind>,
    status: HashMap<SubscriptionId, SubStatus>,
    orders: OrderRuntime,
    index: Arc<OrderIndex>,
}

impl UdCore {
    /// A core with no subscriptions. `index` is shared with the connector.
    pub fn new(index: Arc<OrderIndex>) -> Self {
        Self {
            subs: HashMap::new(),
            status: HashMap::new(),
            orders: OrderRuntime::new(),
            index,
        }
    }

    /// Local lifecycle of orders placed through this connector.
    pub fn orders(&mut self) -> &mut OrderRuntime {
        &mut self.orders
    }

    /// The order index shared with the connector.
    pub fn index(&self) -> &Arc<OrderIndex> {
        &self.index
    }

    /// Adds a subscription of `kind` (status `Connecting`). Executions need this call because the
    /// shared `SubscriptionKind` has no variant for them.
    pub fn register(&mut self, sub: SubscriptionId, kind: UdSubKind) {
        self.subs.insert(sub, kind);
        self.status.entry(sub).or_insert(SubStatus::Connecting);
    }

    /// Forgets subscription `sub` and its status. Late frames for it are ignored.
    pub fn unregister(&mut self, sub: SubscriptionId) {
        self.subs.remove(&sub);
        self.status.remove(&sub);
    }

    /// Stream status of subscription `sub`. `None` for an id the core does not know.
    pub fn status(&self, sub: SubscriptionId) -> Option<SubStatus> {
        self.status.get(&sub).cloned()
    }

    fn order_signal(state: &OrderState) -> Option<OrderSignal> {
        Some(match state {
            OrderState::Acked { broker_id, .. } => OrderSignal::Ack {
                broker_id: broker_id.clone(),
            },
            OrderState::PartiallyFilled { filled, left, .. } => OrderSignal::PartialFill {
                filled: *filled,
                left: *left,
            },
            OrderState::Filled { .. } => OrderSignal::Filled,
            OrderState::Cancelled { .. } => OrderSignal::Cancelled,
            OrderState::Rejected { reason, .. } => OrderSignal::Rejected {
                reason: reason.clone(),
            },
            OrderState::InFlight { .. } | OrderState::Unknown { .. } => return None,
        })
    }

    /// Maps one order echo and advances the local lifecycle of orders we placed.
    pub fn ingest_order(
        &mut self,
        os: &pb::OrderState,
        now: Instant,
    ) -> brokapi_core::OrderSnapshot {
        // An SL/TP order carries its labels in `sltp_order`, not in `order`.
        let (client_order_id, comment) = crate::map::stop::echo_keys(os);
        let cid = self.index.resolve(&os.order_id, &client_order_id, &comment);
        let snap = map_order_state(os, cid, now);
        if let Some(signal) = Self::order_signal(&snap.state) {
            // Foreign orders are unknown to the registry: the error is expected and ignored.
            self.orders.apply_signal(cid, signal, now);
        }
        snap
    }
}

impl BrokerCore for UdCore {
    type Frame = UdFrame;

    fn on_frame(&mut self, frame: UdFrame, now: Instant, out: &mut EventSink) {
        let UdFrame { sub, event } = frame;
        let Some(kind) = self.subs.get(&sub).copied() else {
            return;
        };
        match event {
            UdEvent::Connected => {
                self.status.insert(sub, SubStatus::Active);
            }
            UdEvent::Disconnected(r) => {
                self.status.insert(sub, SubStatus::Reconnecting(r));
            }
            UdEvent::Failed(r) => {
                self.status.insert(sub, SubStatus::Failed(r));
            }
            UdEvent::Orders(msg) if kind == UdSubKind::Orders => {
                self.status.insert(sub, SubStatus::Active);
                for os in &msg.orders {
                    let snap = self.ingest_order(os, now);
                    out.push(CoreEvent {
                        sub: Some(sub),
                        payload: Event::Order(snap),
                    });
                }
            }
            UdEvent::Trades(msg) if kind == UdSubKind::Executions => {
                self.status.insert(sub, SubStatus::Active);
                for t in &msg.trades {
                    let cid = self.index.lookup(&t.order_id, &t.comment);
                    out.push(CoreEvent {
                        sub: Some(sub),
                        payload: Event::Execution(map_trade(t, cid, now)),
                    });
                }
            }
            UdEvent::Account(msg) if kind == UdSubKind::Portfolio => {
                self.status.insert(sub, SubStatus::Active);
                out.push(CoreEvent {
                    sub: Some(sub),
                    payload: Event::Portfolio(map_account(&msg, now)),
                });
            }
            // A message that does not match its subscription kind cannot happen.
            _ => {}
        }
    }

    fn poll_transmit(&mut self) -> Option<Outbound> {
        None
    }

    fn poll_timeout(&self) -> Option<Instant> {
        None
    }

    fn on_timeout(&mut self, _now: Instant, _out: &mut EventSink) {}

    /// Order-state and portfolio subscriptions. Executions are registered with `register`
    /// because `SubscriptionKind` has no variant for them.
    fn apply(&mut self, desired: &SubscriptionSet) {
        self.subs
            .retain(|id, k| *k == UdSubKind::Executions || desired.find(*id).is_some());
        for d in desired.iter() {
            match d.kind {
                SubscriptionKind::OrderState => self.register(d.id, UdSubKind::Orders),
                SubscriptionKind::Portfolio => self.register(d.id, UdSubKind::Portfolio),
                _ => {}
            }
        }
        let live: Vec<_> = self.subs.keys().copied().collect();
        self.status.retain(|id, _| live.contains(id));
    }
}
