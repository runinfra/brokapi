//! User-data sans-io core.

use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

use brokapi_bcs_protocol::http::models::{
    OrderOperationResponse, OrderSearchResponse, OrderStatusResponse,
};
use brokapi_bcs_protocol::models::messages::IncomingMessage;
use brokapi_bcs_protocol::ws::models::OrderMessage;
use brokapi_core::SubscriptionId;
use brokapi_sansio::{
    BrokerCore, CoreEvent, EventSink, OrderRegistry, OrderSignal, Outbound, SubscriptionKind,
    SubscriptionSet,
};

use crate::map::execution::{Cumulative, cumulative_of, map_ws_execution};
use crate::map::order::{map_order_status, map_place_response, map_ws_order_message};
use crate::map::portfolio::map_ws_portfolio;

/// The answer of a REST call, given to the user data core by hand.
///
/// [`UserDataConnector`](crate::UserDataConnector) reads the answers of its own commands directly and
/// does not send them through the core. These frames exist to drive the core in tests and replays.
// Boxing the large variant would change the public shape of the enum; these values are short-lived.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum UdRestFrame {
    /// The answer to a place request: the order is acknowledged, an `Event::Order` is pushed.
    Place(OrderOperationResponse),
    /// The answer to a cancel request: the order is marked cancelled in the core, no event is pushed.
    Cancel(OrderOperationResponse),
    /// The answer to a replace request: the new order is acknowledged, no event is pushed.
    Replace(OrderOperationResponse),
    /// The answer to an order status read: an `Event::Order` with the reported state is pushed.
    OrderState(OrderStatusResponse),
    /// The answer to an active orders search. The core ignores it.
    ActiveOrders(OrderSearchResponse),
}

/// A frame for the user data core: a WebSocket push or a REST answer.
// Boxing the large variant would change the public shape of the enum; these values are short-lived.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum UdFrame {
    /// A text frame from one of the WebSocket channels.
    Ws {
        /// The channel the frame came from. It decides how the text is parsed.
        channel: brokapi_bcs_protocol::ws::client::BcsWsChannel,
        /// The text of the frame, as it came off the socket.
        text: String,
    },
    /// The answer of a REST call.
    Rest(UdRestFrame),
}

/// Sans-io user data core: order statuses, fills and portfolio updates from the WebSocket channels.
///
/// It turns frames into [`Event`](brokapi_core::Event)s (orders, executions, portfolio), delivers each
/// fill once even if the broker repeats it (de-duplicated by `executionId`, the last 20 000 are
/// remembered), and remembers which broker order id belongs to which client order id. It does no I/O.
///
/// # Examples
///
/// Feed one order event from the `orders/events/ws` channel and read the event it makes:
///
/// ```
/// use std::time::Instant;
/// use brokapi_bcs_protocol::ws::client::BcsWsChannel;
/// use brokapi_bcs::UdCore;
/// use brokapi_core::{ClientOrderId, Event, OrderState, SubscriptionId};
/// use brokapi_sansio::{
///     BrokerCore, DesiredSubscription, EventSink, SubscriptionKind, SubscriptionSet,
/// };
///
/// let mut core = UdCore::new();
/// let sub = SubscriptionId::new(1001);
/// core.apply(&SubscriptionSet::with_subscriptions(vec![DesiredSubscription {
///     id: sub,
///     kind: SubscriptionKind::OrderState,
/// }]));
///
/// let cid = ClientOrderId::new_v4();
/// let frame = format!(
///     r#"{{"originalClientOrderId":"{cid}","clientOrderId":"{cid}","data":{{
///         "messageType":"8","orderStatus":"0","executionType":"0","orderQuantity":10,
///         "executedQuantity":0,"lastQuantity":0,"remainedQuantity":10,"ticker":"SBER",
///         "classCode":"TQBR","side":"1","orderType":"2","averagePrice":0,"price":100,
///         "orderId":"20261019-TQBR-1","currency":"RUB","transactionTime":"2026-10-19T10:00:00.000Z"}}}}"#
/// );
/// let mut out = EventSink::new();
/// core.feed_ws(BcsWsChannel::OrderEvents, &frame, Instant::now(), &mut out);
///
/// let events = out.drain();
/// assert_eq!(events.len(), 1);
/// assert_eq!(events[0].sub, Some(sub));
/// let Event::Order(order) = &events[0].payload else { panic!("an order event") };
/// assert!(matches!(order.state, OrderState::Acked { .. }));
/// // The core now knows which of our orders the broker's id is.
/// assert_eq!(core.cid_of_order_id("20261019-TQBR-1"), Some(cid));
/// ```
#[derive(Debug)]
pub struct UdCore {
    desired: SubscriptionSet,
    actual: HashMap<SubscriptionId, bool>,
    outbound: VecDeque<Outbound>,
    orders: OrderRegistry,
    command_timeout: Duration,
    next_deadline: Option<Instant>,
    /// `executionId`s already delivered (the broker repeats messages, and a reconnect replays them).
    seen_executions: HashSet<String>,
    seen_order: VecDeque<String>,
    /// Broker order id (`orderId`) -> our cid. The list of orders (`POST /orders/search`) carries only
    /// the broker's id, so this is how a listed order gets its cid back. Learned from place answers,
    /// status reads and the order-events stream.
    order_ids: HashMap<String, brokapi_core::ClientOrderId>,
    /// Running totals of each order from its latest message (the broker order id is the key): the price of
    /// a fill is the growth of the value over the growth of the quantity.
    totals: HashMap<String, Cumulative>,
    totals_order: VecDeque<String>,
    /// How many times the order events session announced a connect. From the second on, the connection
    /// was lost once and what happened to the orders in the gap is not known.
    connects: u32,
}

/// How many `executionId`s are remembered for de-duplication.
const SEEN_EXECUTIONS_CAP: usize = 20_000;
/// How many orders keep their running totals.
const TOTALS_CAP: usize = 20_000;

impl Default for UdCore {
    fn default() -> Self {
        Self::new()
    }
}

impl UdCore {
    /// Creates a core with no subscriptions and no known orders. The answer timer for a registered
    /// place command is 30 seconds (see [`register_place`](Self::register_place)).
    pub fn new() -> Self {
        Self {
            desired: SubscriptionSet::new(),
            actual: HashMap::new(),
            outbound: VecDeque::new(),
            orders: OrderRegistry::new(),
            command_timeout: Duration::from_secs(30),
            next_deadline: None,
            seen_executions: HashSet::new(),
            seen_order: VecDeque::new(),
            order_ids: HashMap::new(),
            totals: HashMap::new(),
            totals_order: VecDeque::new(),
            connects: 0,
        }
    }

    /// Remembers that the broker's order `order_id` is our `cid`.
    pub fn learn_order_id(&mut self, order_id: &str, cid: brokapi_core::ClientOrderId) {
        if !order_id.is_empty() && !cid.is_nil() {
            self.order_ids.insert(order_id.to_string(), cid);
        }
    }

    /// Our cid of the broker's order `order_id`, if this process has seen it.
    pub fn cid_of_order_id(&self, order_id: &str) -> Option<brokapi_core::ClientOrderId> {
        self.order_ids.get(order_id).copied()
    }

    /// The orders this core tracks (state machine per client order id).
    pub fn orders(&self) -> &OrderRegistry {
        &self.orders
    }

    /// The same registry, for changes. Meant for tests and replays.
    pub fn orders_mut(&mut self) -> &mut OrderRegistry {
        &mut self.orders
    }

    /// Registers a place command that was just sent, and starts the 30 second answer timer for it.
    ///
    /// [`UserDataConnector`](crate::UserDataConnector) does not call it (it tracks sent orders in its
    /// own runtime). It is here to drive the core by hand.
    pub fn register_place(&mut self, cid: brokapi_core::ClientOrderId, now: Instant) {
        self.orders.register_place(cid, now);
        self.next_deadline = Some(now + self.command_timeout);
    }

    fn handle_ws(
        &mut self,
        channel: brokapi_bcs_protocol::ws::client::BcsWsChannel,
        text: &str,
        now: Instant,
        out: &mut EventSink,
    ) {
        let msg = brokapi_bcs_protocol::ws::parse_incoming(text, channel);
        self.handle_incoming(msg, now, out);
    }

    /// Handles one message already parsed by the protocol layer.
    ///
    /// This is what the pump calls. Order messages push `Event::Order`, and a message with a fill
    /// (`lastQuantity` above zero) also pushes `Event::Execution` once per `executionId`, but only
    /// while somebody is subscribed to executions. Portfolio messages push `Event::Portfolio`. Other
    /// messages are skipped.
    pub fn on_incoming(&mut self, msg: IncomingMessage, now: Instant, out: &mut EventSink) {
        self.handle_incoming(msg, now, out);
    }

    fn handle_incoming(&mut self, msg: IncomingMessage, now: Instant, out: &mut EventSink) {
        match msg {
            IncomingMessage::OrderExecution(order) => {
                // The current `orders/events/ws` channel (and the deprecated execution-status):
                // status changes AND fills. A message is a fill when it has `lastQuantity > 0`.
                self.push_order(&order, now, out);
                self.push_execution(&order, now, out);
            }
            IncomingMessage::OrderTransaction(order) => self.push_order(&order, now, out),
            IncomingMessage::Reconnected => self.on_connect(out),
            IncomingMessage::Portfolio(positions) => {
                out.push(CoreEvent {
                    sub: self.find_portfolio_sub(),
                    payload: map_ws_portfolio(&positions, now),
                });
                if let Some(id) = self.find_portfolio_sub() {
                    self.actual.insert(id, true);
                }
            }
            _ => {}
        }
    }

    /// The session of the order events channel is (re)connected. From the second time on the connection
    /// was lost: tell the consumer on both subscriptions that carry orders and fills (the emitter
    /// closes the trading gate on it).
    fn on_connect(&mut self, out: &mut EventSink) {
        self.connects += 1;
        if self.connects < 2 {
            return;
        }
        let gap_from = crate::map::unix_nanos(chrono::Utc::now());
        for sub in [self.find_order_state_sub(), self.find_execution_sub()]
            .into_iter()
            .flatten()
        {
            out.push(CoreEvent {
                sub: Some(sub),
                payload: brokapi_core::Event::Reconnected { sub, gap_from },
            });
        }
    }

    fn push_order(&mut self, order: &OrderMessage, now: Instant, out: &mut EventSink) {
        if let Some(event) = map_ws_order_message(order, now) {
            if let brokapi_core::Event::Order(snapshot) = &event {
                if let Some(id) = &snapshot.broker_id {
                    self.learn_order_id(id.as_str(), snapshot.cid);
                }
                self.apply_order_snapshot(snapshot, now);
            }
            out.push(CoreEvent {
                sub: self.find_order_state_sub(),
                payload: event,
            });
        }
    }

    /// `Event::Execution` for a message with a fill, once per `executionId`. Nothing is emitted
    /// while nobody is subscribed to executions. The running totals of the order are kept in any case:
    /// the price of the next fill is computed from them.
    fn push_execution(&mut self, order: &OrderMessage, now: Instant, out: &mut EventSink) {
        let Some(data) = order.data.as_ref() else {
            return;
        };
        let order_id = data.order_id.clone().filter(|s| !s.is_empty());
        let before = order_id
            .as_ref()
            .and_then(|id| self.totals.get(id))
            .copied();
        let after = cumulative_of(data);
        let exec = match self.find_execution_sub() {
            Some(sub) => map_ws_execution(order, now, before).map(|e| (sub, e)),
            None => None,
        };
        if let (Some(id), Some(after)) = (order_id, after) {
            self.remember_totals(id, after);
        }
        let Some((sub, exec)) = exec else {
            return;
        };
        if !self.seen_executions.insert(exec.trade_id.clone()) {
            return;
        }
        self.seen_order.push_back(exec.trade_id.clone());
        if self.seen_order.len() > SEEN_EXECUTIONS_CAP
            && let Some(old) = self.seen_order.pop_front()
        {
            self.seen_executions.remove(&old);
        }
        out.push(CoreEvent {
            sub: Some(sub),
            payload: brokapi_core::Event::Execution(exec),
        });
    }

    fn remember_totals(&mut self, order_id: String, totals: Cumulative) {
        if self.totals.insert(order_id.clone(), totals).is_none() {
            self.totals_order.push_back(order_id);
            if self.totals_order.len() > TOTALS_CAP
                && let Some(old) = self.totals_order.pop_front()
            {
                self.totals.remove(&old);
            }
        }
    }

    fn find_execution_sub(&self) -> Option<SubscriptionId> {
        self.desired
            .subs()
            .iter()
            .find_map(|s| matches!(s.kind, SubscriptionKind::Executions).then_some(s.id))
    }

    fn handle_rest(&mut self, frame: UdRestFrame, now: Instant, out: &mut EventSink) {
        match frame {
            UdRestFrame::Place(resp) => {
                self.orders
                    .apply(
                        resp.client_order_id,
                        OrderSignal::Ack {
                            broker_id: brokapi_core::BrokerOrderId::new(
                                resp.client_order_id.hyphenated().to_string(),
                            ),
                        },
                        now,
                    )
                    .ok();
                out.push(CoreEvent {
                    sub: None,
                    payload: map_place_response(&resp, now),
                });
            }
            UdRestFrame::Cancel(resp) => {
                self.orders
                    .apply(resp.client_order_id, OrderSignal::Cancelled, now)
                    .ok();
            }
            UdRestFrame::Replace(resp) => {
                self.orders
                    .apply(
                        resp.client_order_id,
                        OrderSignal::Ack {
                            broker_id: brokapi_core::BrokerOrderId::new(
                                resp.client_order_id.hyphenated().to_string(),
                            ),
                        },
                        now,
                    )
                    .ok();
            }
            UdRestFrame::OrderState(resp) => {
                if let Some(event) = map_order_status(&resp, now) {
                    if let brokapi_core::Event::Order(snapshot) = &event {
                        self.apply_order_snapshot(snapshot, now);
                    }
                    out.push(CoreEvent {
                        sub: self.find_order_state_sub(),
                        payload: event,
                    });
                }
            }
            UdRestFrame::ActiveOrders(_resp) => {}
        }
    }

    fn apply_order_snapshot(&mut self, snapshot: &brokapi_core::OrderSnapshot, now: Instant) {
        let signal = match &snapshot.state {
            brokapi_core::OrderState::Acked { broker_id, .. } => OrderSignal::Ack {
                broker_id: broker_id.clone(),
            },
            brokapi_core::OrderState::PartiallyFilled { filled, left, .. } => {
                OrderSignal::PartialFill {
                    filled: *filled,
                    left: *left,
                }
            }
            brokapi_core::OrderState::Filled { .. } => OrderSignal::Filled,
            brokapi_core::OrderState::Cancelled { .. } => OrderSignal::Cancelled,
            brokapi_core::OrderState::Rejected { reason, .. } => OrderSignal::Rejected {
                reason: reason.clone(),
            },
            _ => return,
        };
        let _ = self.orders.apply(snapshot.cid, signal, now);
    }

    fn find_order_state_sub(&self) -> Option<SubscriptionId> {
        self.desired.subs().iter().find_map(|s| {
            if matches!(s.kind, SubscriptionKind::OrderState) {
                Some(s.id)
            } else {
                None
            }
        })
    }

    fn find_portfolio_sub(&self) -> Option<SubscriptionId> {
        self.desired.subs().iter().find_map(|s| {
            if matches!(s.kind, SubscriptionKind::Portfolio) {
                Some(s.id)
            } else {
                None
            }
        })
    }

    /// Parses one raw text frame of a WebSocket `channel` and handles it like
    /// [`on_incoming`](Self::on_incoming). A frame that cannot be parsed is skipped, there is no
    /// error to return.
    pub fn feed_ws(
        &mut self,
        channel: brokapi_bcs_protocol::ws::client::BcsWsChannel,
        text: &str,
        now: Instant,
        out: &mut EventSink,
    ) {
        let msg = brokapi_bcs_protocol::ws::parse_incoming(text, channel);
        self.handle_incoming(msg, now, out);
    }
}

impl BrokerCore for UdCore {
    type Frame = UdFrame;

    fn on_frame(&mut self, frame: Self::Frame, now: Instant, out: &mut EventSink) {
        match frame {
            UdFrame::Ws { channel, text } => self.handle_ws(channel, &text, now, out),
            UdFrame::Rest(rest) => self.handle_rest(rest, now, out),
        }
    }

    fn poll_transmit(&mut self) -> Option<Outbound> {
        self.outbound.pop_front()
    }

    fn poll_timeout(&self) -> Option<Instant> {
        self.next_deadline
    }

    fn on_timeout(&mut self, now: Instant, out: &mut EventSink) {
        let changed = self.orders.on_global_timeout(now, self.command_timeout);
        for (cid, state) in changed {
            out.push(CoreEvent {
                sub: None,
                payload: brokapi_core::Event::Order(brokapi_core::OrderSnapshot {
                    cid,
                    broker_id: None,
                    instrument: brokapi_core::InstrumentId::new(
                        brokapi_core::InstrumentKey::new("", ""),
                        brokapi_core::NativeId::TickerClass {
                            ticker: String::new(),
                            class_code: String::new(),
                        },
                    ),
                    state,
                    side: brokapi_core::Side::Buy,
                    order_type: brokapi_core::OrderType::Limit,
                    qty: brokapi_core::Qty::ZERO,
                    filled: brokapi_core::Qty::ZERO,
                    price: None,
                    ts: brokapi_core::Timestamps::new(brokapi_core::UnixNanos::ZERO, now),
                }),
            });
        }
        self.next_deadline = None;
    }

    fn apply(&mut self, desired: &SubscriptionSet) {
        self.desired = desired.clone();
        for sub in desired.subs() {
            match sub.kind {
                SubscriptionKind::OrderState
                | SubscriptionKind::Portfolio
                | SubscriptionKind::Executions => {
                    self.actual.insert(sub.id, false);
                }
                _ => {}
            }
        }
    }
}
