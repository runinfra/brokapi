//! User-data core: order lifecycle, own trades, portfolio of ONE account.
//!
//! The runtime owns the streams (`OrderStateStream`, `TradesStream`, `PortfolioStream`,
//! `PositionsStream`) and the unary calls; this core keeps the order state machine, links ids
//! (our cid <-> exchange order id <-> instrument) and turns everything into events.
//!
//! One account: every message with another `account_id` is dropped even if the broker
//! sends it on our stream.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;
use std::time::Instant;

use brokapi_core::{
    Balance, BrokerOrderId, ClientOrderId, Error, Event, InstrumentId, InstrumentKey, Money,
    OrderSnapshot, OrderState, OrderType, PlaceOrderRequest, Price, Qty, Side, SubscriptionId,
    Timestamps,
};
use brokapi_runtime::OrderRuntime;
use brokapi_sansio::{
    BrokerCore, EventSink, OrderSignal, Outbound, SubscriptionKind, SubscriptionSet,
};
use brokapi_tbank_protocol::money::money_to_decimal;
use brokapi_tbank_protocol::v1 as pb;
use brokapi_tbank_protocol::v1::order_state_stream_response::Payload as OrderStreamPayload;
use brokapi_tbank_protocol::v1::portfolio_stream_response::Payload as PortfolioPayload;
use brokapi_tbank_protocol::v1::positions_stream_response::Payload as PositionsPayload;
use brokapi_tbank_protocol::v1::trades_stream_response::Payload as TradesPayload;

use crate::core::md::SubUpdate;
use crate::ids::{id_from_parts, stamps};
use crate::lots::LotBook;
use crate::map::order::{self, OrderParts};
use crate::map::{execution, portfolio};

/// The user-data streams the runtime keeps open.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StreamKind {
    /// `OrdersStreamService/OrderStateStream`: state changes of own orders.
    OrderState,
    /// `OrdersStreamService/TradesStream`: own trades.
    Trades,
    /// `OperationsStreamService/PortfolioStream`: the portfolio with prices.
    Portfolio,
    /// `OperationsStreamService/PositionsStream`: cash and securities balances.
    Positions,
}

/// User-data subscriptions. `brokapi-sansio`'s `SubscriptionKind` has no executions variant yet, so
/// this core takes them through [`UdCore::add_sub`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum UdSubKind {
    /// State changes of own orders. Fed by [`StreamKind::OrderState`].
    OrderState,
    /// Own trades. Fed by [`StreamKind::Trades`].
    Executions,
    /// Portfolio, positions and cash. Fed by [`StreamKind::Portfolio`] and [`StreamKind::Positions`].
    Portfolio,
}

/// One inbound item for [`UdCore`]: a message from one of the user data streams.
#[derive(Debug, Clone)]
pub enum UdFrame {
    /// A stream was (re)opened.
    Connected(StreamKind),
    /// A message of `OrderStateStream`.
    OrderState(pb::OrderStateStreamResponse),
    /// A message of `TradesStream`.
    Trades(pb::TradesStreamResponse),
    /// A message of `PortfolioStream`.
    Portfolio(pb::PortfolioStreamResponse),
    /// A message of `PositionsStream`.
    Positions(pb::PositionsStreamResponse),
    /// `GetOrders` result used to refresh order state after a stream reconnect.
    ActiveOrders(pb::GetOrdersResponse),
}

#[derive(Debug, Clone)]
struct OrderMeta {
    instrument: InstrumentId,
    side: Side,
    order_type: OrderType,
    qty: Qty,
    filled: Qty,
    price: Option<Price>,
    exchange_id: Option<String>,
}

/// Sans-io core of the user data of ONE account: order state machine, own trades, portfolio.
///
/// It links our cid to the exchange order id and the instrument, drops every message that belongs to
/// another account, and turns the rest into events. The connector calls the command methods
/// ([`begin_place`](Self::begin_place), [`place_response`](Self::place_response), ...) around its
/// requests, and feeds stream messages through [`BrokerCore::on_frame`] as [`UdFrame`]s.
///
/// # Examples
///
/// ```
/// use std::time::Instant;
/// use brokapi_core::{ClientOrderId, InstrumentId, InstrumentKey, NativeId, OrderType, PlaceOrderRequest, Price, Qty, Side};
/// use brokapi_tbank::core::UdCore;
///
/// let mut core = UdCore::new("acc-main");
/// let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::Uid("uid-sber".into()));
/// let req = PlaceOrderRequest {
///     cid: ClientOrderId::new_v4(),
///     instrument: sber,
///     side: Side::Buy,
///     order_type: OrderType::Limit,
///     qty: Qty::from(1),
///     price: Some(Price::from(250)),
/// };
///
/// // A cid is registered for one placement at most. The second try is refused before any send.
/// assert!(core.begin_place(&req, Instant::now()).is_ok());
/// assert!(core.begin_place(&req, Instant::now()).is_err());
/// assert!(core.order_state(req.cid).is_some());
/// ```
#[derive(Debug)]
pub struct UdCore {
    account_id: String,
    orders: OrderRuntime,
    meta: HashMap<ClientOrderId, OrderMeta>,
    by_exchange: HashMap<String, ClientOrderId>,
    known: HashMap<String, InstrumentKey>,
    subs: HashMap<UdSubKind, SubscriptionId>,
    cash: BTreeMap<String, (Money, Money)>,
    portfolio_parts: (bool, bool),
    updates: Vec<SubUpdate>,
    connections: HashMap<StreamKind, u32>,
    reconnected: VecDeque<StreamKind>,
    lots: Arc<LotBook>,
}

impl UdCore {
    /// A core for the account `account_id`. Messages with another account id are dropped.
    pub fn new(account_id: impl Into<String>) -> Self {
        Self {
            account_id: account_id.into(),
            orders: OrderRuntime::new(),
            meta: HashMap::new(),
            by_exchange: HashMap::new(),
            known: HashMap::new(),
            subs: HashMap::new(),
            cash: BTreeMap::new(),
            portfolio_parts: (false, false),
            updates: Vec::new(),
            connections: HashMap::new(),
            reconnected: VecDeque::new(),
            lots: Arc::new(LotBook::new()),
        }
    }

    /// The lot sizes this core knows. The connector fills the book before it feeds a message of an
    /// instrument it has not met (the messages count in lots, the events in units).
    pub fn lots(&self) -> &Arc<LotBook> {
        &self.lots
    }

    /// The lot size of an instrument by the names of a message. 1 when it is not known, which would
    /// make the quantities lots: the connector fills the book first, so this is the safety net only.
    fn lot_of(&self, uid: &str, figi: &str, ticker: &str, class_code: &str) -> i64 {
        match self.lots.get_parts(uid, figi, ticker, class_code) {
            Some(lot) => lot,
            None => {
                tracing::warn!(
                    uid,
                    ticker,
                    "lot size of the instrument is not known, quantities stay in lots"
                );
                1
            }
        }
    }

    /// The account this core serves.
    pub fn account_id(&self) -> &str {
        &self.account_id
    }

    /// The order runtime, for the shutdown sequence.
    pub fn orders_mut(&mut self) -> &mut OrderRuntime {
        &mut self.orders
    }

    /// The state this core holds for `cid`, or `None` if it never saw that cid.
    pub fn order_state(&self, cid: ClientOrderId) -> Option<&OrderState> {
        self.orders.state(cid)
    }

    /// Takes the subscription confirmations and refusals since the last call.
    pub fn take_updates(&mut self) -> Vec<SubUpdate> {
        std::mem::take(&mut self.updates)
    }

    /// Streams that were reopened after a break (not the first open). The runtime refreshes order
    /// state from `GetOrders` for `OrderState`.
    pub fn take_reconnected(&mut self) -> Vec<StreamKind> {
        self.reconnected.drain(..).collect()
    }

    /// Registers a subscription of `kind` under `id`. A later one of the same kind replaces it.
    pub fn add_sub(&mut self, id: SubscriptionId, kind: UdSubKind) {
        self.subs.insert(kind, id);
    }

    /// Forgets the subscription of `kind`, if any.
    pub fn remove_sub(&mut self, kind: UdSubKind) {
        self.subs.remove(&kind);
    }

    /// The id of the active subscription of `kind`, if any.
    pub fn sub(&self, kind: UdSubKind) -> Option<SubscriptionId> {
        self.subs.get(&kind).copied()
    }

    /// Teach the core an instrument (uid/figi -> ticker@class), e.g. the one an order is placed on.
    pub fn learn(&mut self, id: &InstrumentId) {
        match &id.native {
            brokapi_core::NativeId::Uid(u) | brokapi_core::NativeId::Figi(u) => {
                self.known.insert(u.clone(), id.key.clone());
            }
            _ => {}
        }
    }

    fn learn_parts(&mut self, uid: &str, figi: &str, ticker: &str, class_code: &str) {
        if ticker.is_empty() {
            return;
        }
        let key = InstrumentKey::new(ticker, class_code);
        for k in [uid, figi] {
            if !k.is_empty() {
                self.known.insert(k.to_string(), key.clone());
            }
        }
    }

    /// Instrument id from message fields; the ticker is filled from earlier messages when missing.
    pub fn resolve(&self, uid: &str, figi: &str, ticker: &str, class_code: &str) -> InstrumentId {
        if ticker.is_empty() {
            let key = self.known.get(uid).or_else(|| self.known.get(figi));
            if let Some(key) = key {
                return id_from_parts(uid, figi, &key.symbol, &key.mic);
            }
        }
        id_from_parts(uid, figi, ticker, class_code)
    }

    fn other_account(&self, account_id: &str) -> bool {
        !account_id.is_empty() && account_id != self.account_id
    }

    // ----- commands (called by the connector around its RPCs) -----

    /// Registers a placement before the request is sent. At most one placement per cid, ever
    /// (no automatic retry of `place`).
    pub fn begin_place(&mut self, req: &PlaceOrderRequest, now: Instant) -> Result<(), Error> {
        if !self.orders.try_place_once(req.cid) {
            return Err(Error::Rejected {
                code: "duplicate_place".into(),
                message: "a cid is placed at most once; a repeated place is never sent".into(),
            });
        }
        self.orders.register_place(req.cid, now);
        self.learn(&req.instrument);
        self.meta.insert(
            req.cid,
            OrderMeta {
                instrument: req.instrument.clone(),
                side: req.side,
                order_type: req.order_type,
                qty: req.qty,
                filled: Qty::ZERO,
                price: req.price,
                exchange_id: None,
            },
        );
        Ok(())
    }

    /// The placement response arrived.
    pub fn place_response(
        &mut self,
        cid: ClientOrderId,
        resp: &pb::PostOrderResponse,
        now: Instant,
        out: &mut EventSink,
    ) {
        let lot = self.lot_of(
            &resp.instrument_uid,
            &resp.figi,
            &resp.ticker,
            &resp.class_code,
        );
        let parts = order::parts_from_post(resp).with_lot(lot);
        self.learn_parts(
            &resp.instrument_uid,
            &resp.figi,
            &resp.ticker,
            &resp.class_code,
        );
        if !parts.exchange_id.is_empty() {
            self.by_exchange.insert(parts.exchange_id.clone(), cid);
            if let Some(m) = self.meta.get_mut(&cid) {
                m.exchange_id = Some(parts.exchange_id.clone());
            }
        }
        self.apply_parts(cid, &parts, now, out);
    }

    /// The broker refused the placement: nothing exists at the broker.
    pub fn place_rejected(
        &mut self,
        cid: ClientOrderId,
        reason: String,
        now: Instant,
        out: &mut EventSink,
    ) {
        self.signal(cid, OrderSignal::Rejected { reason }, now, out);
    }

    /// Timeout or break during `place`: the outcome is unknown, never `Rejected`.
    pub fn place_unknown(&mut self, cid: ClientOrderId, now: Instant, out: &mut EventSink) {
        self.signal(cid, OrderSignal::Timeout, now, out);
    }

    /// The broker accepted a cancel.
    pub fn cancel_ok(&mut self, cid: ClientOrderId, now: Instant, out: &mut EventSink) {
        if self.orders.state(cid).is_none() {
            return;
        }
        self.signal(cid, OrderSignal::Cancelled, now, out);
    }

    /// Registers a replace before the request is sent: the new cid is placed at most once, and it
    /// inherits the instrument and side of the original when we know them.
    pub fn begin_replace(
        &mut self,
        new_cid: ClientOrderId,
        original: ClientOrderId,
        now: Instant,
    ) -> Result<(), Error> {
        if !self.orders.try_place_once(new_cid) {
            return Err(Error::Rejected {
                code: "duplicate_place".into(),
                message: "a cid is used for a placement or replace at most once".into(),
            });
        }
        self.orders.register_place(new_cid, now);
        if let Some(m) = self.meta.get(&original).cloned() {
            self.meta.insert(
                new_cid,
                OrderMeta {
                    exchange_id: None,
                    filled: Qty::ZERO,
                    ..m
                },
            );
        }
        Ok(())
    }

    /// The broker accepted a replace: the new cid is a new live order.
    pub fn replace_response(
        &mut self,
        new_cid: ClientOrderId,
        resp: &pb::PostOrderResponse,
        now: Instant,
        out: &mut EventSink,
    ) {
        let instrument = self.resolve(
            &resp.instrument_uid,
            &resp.figi,
            &resp.ticker,
            &resp.class_code,
        );
        let lot = self.lot_of(
            &resp.instrument_uid,
            &resp.figi,
            &resp.ticker,
            &resp.class_code,
        );
        let parts = order::parts_from_post(resp).with_lot(lot);
        let meta = self.meta.entry(new_cid).or_insert_with(|| OrderMeta {
            instrument,
            side: order::side(resp.direction()),
            order_type: order::order_type(resp.order_type()),
            qty: parts.qty_requested(),
            filled: Qty::ZERO,
            price: None,
            exchange_id: None,
        });
        meta.qty = parts.qty_requested();
        meta.filled = parts.qty_executed();
        if let Some(p) = resp
            .initial_security_price
            .as_ref()
            .map(|m| Price::new(money_to_decimal(m)))
            .filter(|p| !p.is_zero())
        {
            meta.price = Some(p);
        }
        self.place_response(new_cid, resp, now, out);
    }

    /// The replace failed with a definite refusal or an unknown outcome.
    pub fn replace_failed(
        &mut self,
        new_cid: ClientOrderId,
        unknown: bool,
        reason: String,
        now: Instant,
        out: &mut EventSink,
    ) {
        let signal = if unknown {
            OrderSignal::Timeout
        } else {
            OrderSignal::Rejected { reason }
        };
        self.signal(new_cid, signal, now, out);
    }

    /// In-flight placements become `Unknown` at shutdown, one event each.
    pub fn abandon_inflight(&mut self, now: Instant, out: &mut EventSink) -> Vec<ClientOrderId> {
        let abandoned = self.orders.abandon_inflight(now);
        for cid in &abandoned {
            self.emit_order(*cid, now, out);
        }
        abandoned
    }

    /// Exchange order id of a cid we know (placed here or seen on the stream).
    pub fn exchange_id(&self, cid: ClientOrderId) -> Option<&str> {
        self.meta.get(&cid).and_then(|m| m.exchange_id.as_deref())
    }

    /// Instrument of an order we know, for ordering commands per instrument.
    pub fn instrument_of(&self, cid: ClientOrderId) -> Option<InstrumentId> {
        self.meta.get(&cid).map(|m| m.instrument.clone())
    }

    /// Lots and price of an order we know, for building a replace without a round trip.
    pub fn order_basis(&self, cid: ClientOrderId) -> Option<(Qty, Option<Price>)> {
        self.meta.get(&cid).map(|m| (m.qty, m.price))
    }

    /// Learns ids from a `GetOrderState` / `GetOrders` answer.
    pub fn note_order(&mut self, parts: &OrderParts) {
        let cid = parts.cid();
        self.learn_parts(
            &instrument_uid(&parts.instrument),
            "",
            &parts.instrument.key.symbol,
            &parts.instrument.key.mic,
        );
        if !parts.exchange_id.is_empty() {
            self.by_exchange.insert(parts.exchange_id.clone(), cid);
        }
        let entry = self.meta.entry(cid).or_insert_with(|| OrderMeta {
            instrument: parts.instrument.clone(),
            side: order::side(parts.direction),
            order_type: order::order_type(parts.order_type),
            qty: parts.qty_requested(),
            filled: parts.qty_executed(),
            price: parts.unit_price,
            exchange_id: None,
        });
        if !parts.exchange_id.is_empty() {
            entry.exchange_id = Some(parts.exchange_id.clone());
        }
    }

    // ----- order state machine -----

    fn signal(
        &mut self,
        cid: ClientOrderId,
        signal: OrderSignal,
        now: Instant,
        out: &mut EventSink,
    ) {
        let before = self.orders.state(cid).cloned();
        if before.is_none() {
            self.orders.register_place(cid, now);
        }
        self.orders.apply_signal(cid, signal, now);
        if self.orders.state(cid) != before.as_ref() {
            self.emit_order(cid, now, out);
        }
    }

    fn emit_order(&mut self, cid: ClientOrderId, now: Instant, out: &mut EventSink) {
        let (Some(state), Some(meta)) = (self.orders.state(cid).cloned(), self.meta.get(&cid))
        else {
            return;
        };
        let snap = OrderSnapshot {
            cid,
            broker_id: meta.exchange_id.clone().map(BrokerOrderId::new),
            instrument: meta.instrument.clone(),
            state,
            side: meta.side,
            order_type: meta.order_type,
            qty: meta.qty,
            filled: meta.filled,
            price: meta.price,
            ts: Timestamps::new(crate::ids::wall_nanos(), now),
        };
        out.push_payload(self.sub(UdSubKind::OrderState), Event::Order(snap));
    }

    /// Applies a status report (placement response, stream update, refresh) to the order.
    fn apply_parts(
        &mut self,
        cid: ClientOrderId,
        p: &OrderParts,
        now: Instant,
        out: &mut EventSink,
    ) {
        use pb::OrderExecutionReportStatus as S;
        self.note_order(p);
        if let Some(m) = self.meta.get_mut(&cid) {
            m.filled = p.qty_executed();
            if m.qty.is_zero() {
                m.qty = p.qty_requested();
            }
            // For orders we placed, the price we sent is the truth (the stream's price field is unspecified).
            if m.price.is_none() {
                m.price = p.unit_price;
            }
        }
        let before = self.orders.state(cid).cloned();
        if before.is_none() {
            self.orders.register_place(cid, now);
        }
        let broker_id = BrokerOrderId::new(p.exchange_id.clone());
        let ack = OrderSignal::Ack { broker_id };
        let signals: Vec<OrderSignal> = match p.status {
            S::ExecutionReportStatusNew => vec![ack],
            S::ExecutionReportStatusPartiallyfill => vec![
                ack,
                OrderSignal::PartialFill {
                    filled: p.qty_executed(),
                    left: p.qty_left(),
                },
            ],
            S::ExecutionReportStatusFill => vec![ack, OrderSignal::Filled],
            S::ExecutionReportStatusCancelled => vec![ack, OrderSignal::Cancelled],
            S::ExecutionReportStatusRejected => vec![OrderSignal::Rejected {
                reason: if p.reason.is_empty() {
                    "REJECTED".into()
                } else {
                    p.reason.clone()
                },
            }],
            S::ExecutionReportStatusUnspecified => vec![],
        };
        for s in signals {
            // Invalid transitions (late duplicates) are ignored: the state machine keeps the newer state.
            self.orders.apply_signal(cid, s, now);
        }
        if self.orders.state(cid) != before.as_ref() {
            self.emit_order(cid, now, out);
        }
    }

    // ----- inbound messages -----

    fn on_order_state_stream(
        &mut self,
        r: pb::OrderStateStreamResponse,
        now: Instant,
        out: &mut EventSink,
    ) {
        match r.payload {
            Some(OrderStreamPayload::OrderState(o)) => {
                if self.other_account(&o.account_id) {
                    return;
                }
                self.learn_parts(&o.instrument_uid, "", &o.ticker, &o.class_code);
                // The stream message carries the lot size of the instrument: remember it.
                self.lots.put(
                    &o.instrument_uid,
                    "",
                    &o.ticker,
                    &o.class_code,
                    i64::from(o.lot_size),
                );
                let lot = self.lot_of(&o.instrument_uid, "", &o.ticker, &o.class_code);
                let parts = order::parts_from_stream(&o).with_lot(lot);
                let cid = parts.cid();
                self.apply_parts(cid, &parts, now, out);
            }
            Some(OrderStreamPayload::Subscription(s)) => {
                let ok = s.status() == pb::ResultSubscriptionStatus::Ok;
                let reason = s
                    .error
                    .map(|e| format!("{}: {}", e.code, e.message))
                    .unwrap_or_else(|| "subscription refused".into());
                self.resolve_sub(UdSubKind::OrderState, ok, reason);
            }
            _ => {}
        }
    }

    fn on_trades_stream(&mut self, r: pb::TradesStreamResponse, now: Instant, out: &mut EventSink) {
        match r.payload {
            Some(TradesPayload::OrderTrades(t)) => {
                if self.other_account(&t.account_id) {
                    return;
                }
                let instrument = self.resolve(&t.instrument_uid, &t.figi, "", "");
                let cid = self.by_exchange.get(&t.order_id).copied();
                let sub = self.sub(UdSubKind::Executions);
                for e in execution::from_order_trades(&t, instrument, cid, now) {
                    out.push_payload(sub, Event::Execution(e));
                }
            }
            Some(TradesPayload::Subscription(s)) => {
                let ok = s.status() == pb::ResultSubscriptionStatus::Ok;
                let reason = s
                    .error
                    .map(|e| format!("{}: {}", e.code, e.message))
                    .unwrap_or_else(|| "subscription refused".into());
                self.resolve_sub(UdSubKind::Executions, ok, reason);
            }
            _ => {}
        }
    }

    fn cash_balances(&self, now: Instant) -> Vec<Balance> {
        self.cash
            .iter()
            .map(|(currency, (available, blocked))| Balance {
                currency: currency.clone(),
                available: *available,
                blocked: *blocked,
                ts: stamps(now, None),
            })
            .collect()
    }

    fn set_cash(&mut self, balances: Vec<Balance>) {
        for b in balances {
            self.cash.insert(b.currency, (b.available, b.blocked));
        }
    }

    fn on_portfolio_stream(
        &mut self,
        r: pb::PortfolioStreamResponse,
        now: Instant,
        out: &mut EventSink,
    ) {
        match r.payload {
            Some(PortfolioPayload::Portfolio(p)) => {
                if self.other_account(&p.account_id) {
                    return;
                }
                for pos in &p.positions {
                    self.learn_parts(&pos.instrument_uid, &pos.figi, &pos.ticker, &pos.class_code);
                }
                let snap = portfolio::portfolio(&p, self.cash_balances(now), now);
                out.push_payload(self.sub(UdSubKind::Portfolio), Event::Portfolio(snap));
            }
            Some(PortfolioPayload::Subscriptions(s)) => {
                let st = s
                    .accounts
                    .iter()
                    .find(|a| a.account_id == self.account_id)
                    .map(|a| a.subscription_status());
                self.portfolio_parts.0 = st == Some(pb::PortfolioSubscriptionStatus::Success);
                self.check_portfolio_ack(st.is_some(), "portfolio stream: account not subscribed");
            }
            _ => {}
        }
    }

    fn on_positions_stream(
        &mut self,
        r: pb::PositionsStreamResponse,
        now: Instant,
        out: &mut EventSink,
    ) {
        let sub = self.sub(UdSubKind::Portfolio);
        match r.payload {
            Some(PositionsPayload::InitialPositions(p)) => {
                if self.other_account(&p.account_id) {
                    return;
                }
                self.set_cash(portfolio::balances_from_positions(&p, now));
                out.push_payload(sub, Event::Balances(self.cash_balances(now)));
            }
            Some(PositionsPayload::Position(p)) => {
                if self.other_account(&p.account_id) {
                    return;
                }
                if !p.money.is_empty() {
                    self.set_cash(portfolio::balances_from_stream(&p.money, now));
                    out.push_payload(sub, Event::Balances(self.cash_balances(now)));
                }
                if !p.securities.is_empty() {
                    let resolved: Vec<InstrumentId> = p
                        .securities
                        .iter()
                        .map(|s| self.resolve(&s.instrument_uid, &s.figi, &s.ticker, &s.class_code))
                        .collect();
                    let positions =
                        portfolio::positions_from_securities(&p.securities, resolved, now);
                    out.push_payload(sub, Event::Positions(positions));
                }
            }
            Some(PositionsPayload::Subscriptions(s)) => {
                let st = s
                    .accounts
                    .iter()
                    .find(|a| a.account_id == self.account_id)
                    .map(|a| a.subscription_status());
                self.portfolio_parts.1 = st
                    == Some(
                        pb::PositionsAccountSubscriptionStatus::PositionsSubscriptionStatusSuccess,
                    );
                self.check_portfolio_ack(st.is_some(), "positions stream: account not subscribed");
            }
            _ => {}
        }
    }

    /// The portfolio subscription is two streams; it is confirmed when both accept the account.
    fn check_portfolio_ack(&mut self, answered: bool, reason: &str) {
        if !answered {
            self.resolve_sub(UdSubKind::Portfolio, false, reason.to_string());
        } else if self.portfolio_parts == (true, true) {
            self.resolve_sub(UdSubKind::Portfolio, true, String::new());
        }
    }

    fn resolve_sub(&mut self, kind: UdSubKind, ok: bool, reason: String) {
        if let Some(id) = self.sub(kind) {
            self.updates.push(SubUpdate {
                sub: id,
                result: if ok { Ok(()) } else { Err(reason) },
            });
        }
    }

    fn on_active_orders(&mut self, r: pb::GetOrdersResponse, now: Instant, out: &mut EventSink) {
        for o in &r.orders {
            self.learn_parts(&o.instrument_uid, &o.figi, &o.ticker, &o.class_code);
            let lot = self.lot_of(&o.instrument_uid, &o.figi, &o.ticker, &o.class_code);
            let parts = order::parts_from_state(o).with_lot(lot);
            let cid = parts.cid();
            self.apply_parts(cid, &parts, now, out);
        }
    }
}

fn instrument_uid(id: &InstrumentId) -> String {
    match &id.native {
        brokapi_core::NativeId::Uid(u) => u.clone(),
        _ => String::new(),
    }
}

impl BrokerCore for UdCore {
    type Frame = UdFrame;

    fn on_frame(&mut self, frame: UdFrame, now: Instant, out: &mut EventSink) {
        match frame {
            UdFrame::Connected(kind) => {
                let n = self.connections.entry(kind).or_insert(0);
                *n += 1;
                if *n > 1 {
                    self.reconnected.push_back(kind);
                    // The streams of orders and fills carry money: whatever happened in the gap never
                    // reached us. Tell the consumer (the emitter closes the trading gate on this event).
                    let owner = match kind {
                        StreamKind::OrderState => Some(UdSubKind::OrderState),
                        StreamKind::Trades => Some(UdSubKind::Executions),
                        _ => None,
                    };
                    if let Some(sub) = owner.and_then(|k| self.sub(k)) {
                        out.push_payload(
                            Some(sub),
                            Event::Reconnected {
                                sub,
                                gap_from: crate::ids::wall_nanos(),
                            },
                        );
                    }
                }
                match kind {
                    StreamKind::Portfolio => self.portfolio_parts.0 = false,
                    StreamKind::Positions => self.portfolio_parts.1 = false,
                    _ => {}
                }
            }
            UdFrame::OrderState(r) => self.on_order_state_stream(r, now, out),
            UdFrame::Trades(r) => self.on_trades_stream(r, now, out),
            UdFrame::Portfolio(r) => self.on_portfolio_stream(r, now, out),
            UdFrame::Positions(r) => self.on_positions_stream(r, now, out),
            UdFrame::ActiveOrders(r) => self.on_active_orders(r, now, out),
        }
    }

    /// Streams are server-side and their requests are fixed: nothing to transmit.
    fn poll_transmit(&mut self) -> Option<Outbound> {
        None
    }

    fn poll_timeout(&self) -> Option<Instant> {
        None
    }

    fn on_timeout(&mut self, _now: Instant, _out: &mut EventSink) {}

    /// Takes the order-state and portfolio subscriptions from the set (executions: [`UdCore::add_sub`]).
    fn apply(&mut self, desired: &SubscriptionSet) {
        self.subs.remove(&UdSubKind::OrderState);
        self.subs.remove(&UdSubKind::Portfolio);
        for d in desired.iter() {
            match d.kind {
                SubscriptionKind::OrderState => {
                    self.subs.insert(UdSubKind::OrderState, d.id);
                }
                SubscriptionKind::Portfolio => {
                    self.subs.insert(UdSubKind::Portfolio, d.id);
                }
                _ => {}
            }
        }
    }
}
