//! Order types: requests ([`PlaceOrderRequest`], [`ReplaceOrderRequest`]), answers ([`OrderAck`],
//! [`OrderSnapshot`]), the lifecycle ([`OrderState`]) and the bootstrap result ([`ReconcileReport`]).
//!
//! Every order is named by a [`ClientOrderId`] that the caller makes. The broker gives its own
//! [`BrokerOrderId`] in the answer.

use std::time::Instant;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::instrument::InstrumentId;
use super::number::{Price, Qty};
use super::primitives::{OrderType, Side};
use super::time::Timestamps;

/// The id of an order that YOU choose: a UUID v4, made by the caller for each new order.
///
/// The library never makes one for you. Use a fresh `ClientOrderId::new_v4()` for every new order,
/// including after an [`Error::Unknown`](crate::Error::Unknown) or any other failed place: a cid
/// that was sent once is never sent again by a connector (a second place with the same cid is
/// refused with [`Error::Rejected`](crate::Error::Rejected), code `duplicate_place`). Keep the cid in
/// your own ledger, it is the key for [`OrderExecutor::order_state`](crate::OrderExecutor::order_state),
/// [`OrderExecutor::cancel`](crate::OrderExecutor::cancel) and
/// [`OrderExecutor::reconcile`](crate::OrderExecutor::reconcile).
///
/// It is a plain `uuid::Uuid`. The form that goes to a broker is made by a
/// [`CidCodec`](crate::CidCodec).
pub type ClientOrderId = Uuid;

/// The order id that the BROKER assigned, as a string.
///
/// Appears in [`OrderAck::broker_id`] and [`OrderSnapshot::broker_id`]. It is the id that the
/// broker's own app and reports show. Its format differs by broker, so treat it as opaque. For
/// your own bookkeeping use the [`ClientOrderId`], which you control.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BrokerOrderId(pub String);

impl BrokerOrderId {
    /// Wraps a string. Nothing is checked.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// The id as the broker writes it.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Where an order is in its life.
///
/// Read it from [`OrderSnapshot::state`], from [`Event::Order`](crate::Event::Order) on the order
/// stream, or from [`OrderExecutor::order_state`](crate::OrderExecutor::order_state). Every variant
/// carries the cid.
///
/// Typical path: `Acked` -> `PartiallyFilled` (zero or more times) -> `Filled`, or `Acked` ->
/// `Cancelled`, or straight to `Rejected`. `Filled`, `Cancelled` and `Rejected` are final.
///
/// The two states about doubt are `InFlight` and `Unknown`. A timeout or a cut connection on a
/// command gives `Unknown`, never `Rejected`: the order may exist. Leave `Unknown` only by asking
/// the broker ([`OrderExecutor::order_state`](crate::OrderExecutor::order_state),
/// [`OrderExecutor::active_orders`](crate::OrderExecutor::active_orders) or the executions).
///
/// # Examples
///
/// ```
/// use brokapi_core::{ClientOrderId, OrderState, Qty};
///
/// /// `true` when nothing more will happen to the order.
/// fn is_final(s: &OrderState) -> bool {
///     matches!(s, OrderState::Filled { .. } | OrderState::Cancelled { .. } | OrderState::Rejected { .. })
/// }
///
/// let cid = ClientOrderId::new_v4();
/// assert!(is_final(&OrderState::Filled { cid }));
/// let part = OrderState::PartiallyFilled { cid, filled: Qty::from(1), left: Qty::from(2) };
/// assert!(!is_final(&part));
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum OrderState {
    /// The command was sent and the answer has not come yet. Local state of the connector's order
    /// tracking, so a snapshot read from the broker normally does not have it.
    InFlight {
        /// The order.
        cid: ClientOrderId,
        /// When the command was sent, monotonic clock. Not serialized (after serde it is "now").
        #[serde(skip, default = "Instant::now")]
        sent_at: Instant,
    },
    /// The outcome of a command is not known (timeout, cut connection), or the broker reported a
    /// status this library does not know. The order may exist. Ask the broker, do not repeat the
    /// command, see [`Error::Unknown`](crate::Error::Unknown).
    Unknown {
        /// The order.
        cid: ClientOrderId,
        /// Since when the outcome is unknown, monotonic clock. Not serialized (after serde it is "now").
        #[serde(skip, default = "Instant::now")]
        since: Instant,
    },
    /// The broker accepted the order and it is working, nothing filled yet.
    Acked {
        /// The order.
        cid: ClientOrderId,
        /// The broker's id of the order.
        broker_id: BrokerOrderId,
    },
    /// Part of the order is filled and the rest is still working.
    PartiallyFilled {
        /// The order.
        cid: ClientOrderId,
        /// Quantity executed so far.
        filled: Qty,
        /// Quantity still to execute (the rest of the order).
        left: Qty,
    },
    /// The order is fully executed. Final.
    Filled {
        /// The order.
        cid: ClientOrderId,
    },
    /// The order is cancelled, by you or by the broker or exchange (for example at the end of the
    /// day). Part of it may have been filled before. Final.
    Cancelled {
        /// The order.
        cid: ClientOrderId,
    },
    /// The broker refused the order after accepting the request. Final. A refusal that comes as the
    /// direct answer of `place` is returned as [`Error::Rejected`](crate::Error::Rejected) instead.
    Rejected {
        /// The order.
        cid: ClientOrderId,
        /// The broker's reason, for a human.
        reason: String,
    },
}

impl OrderState {
    /// The client order id, the same in every state.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::{ClientOrderId, OrderState};
    ///
    /// let cid = ClientOrderId::new_v4();
    /// assert_eq!(OrderState::Filled { cid }.cid(), cid);
    /// ```
    pub fn cid(&self) -> ClientOrderId {
        match self {
            Self::InFlight { cid, .. }
            | Self::Unknown { cid, .. }
            | Self::Acked { cid, .. }
            | Self::PartiallyFilled { cid, .. }
            | Self::Filled { cid }
            | Self::Cancelled { cid }
            | Self::Rejected { cid, .. } => *cid,
        }
    }

    /// The name of the state, for logs and error texts (`"Filled"`, `"PartiallyFilled"`, ...).
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::{ClientOrderId, OrderState};
    ///
    /// assert_eq!(OrderState::Cancelled { cid: ClientOrderId::new_v4() }.name(), "Cancelled");
    /// ```
    pub fn name(&self) -> &'static str {
        match self {
            Self::InFlight { .. } => "InFlight",
            Self::Unknown { .. } => "Unknown",
            Self::Acked { .. } => "Acked",
            Self::PartiallyFilled { .. } => "PartiallyFilled",
            Self::Filled { .. } => "Filled",
            Self::Cancelled { .. } => "Cancelled",
            Self::Rejected { .. } => "Rejected",
        }
    }

    /// `true` for the states that never change again: `Filled`, `Cancelled`, `Rejected`.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::{ClientOrderId, OrderState};
    ///
    /// let cid = ClientOrderId::new_v4();
    /// assert!(OrderState::Filled { cid }.is_final());
    /// assert!(!OrderState::Acked { cid, broker_id: brokapi_core::BrokerOrderId::new("1") }.is_final());
    /// ```
    pub fn is_final(&self) -> bool {
        matches!(
            self,
            Self::Filled { .. } | Self::Cancelled { .. } | Self::Rejected { .. }
        )
    }
}

impl OrderSnapshot {
    /// A snapshot with the required fields: the order, its instrument, state, side, type, quantity and
    /// time. `broker_id` and `price` are `None`, `filled` is zero. Set them with
    /// [`with_broker_id`](Self::with_broker_id), [`with_filled`](Self::with_filled) and
    /// [`with_price`](Self::with_price). The struct is `#[non_exhaustive]`: it gets new fields, so build
    /// it with this function and not with a struct literal.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::{
    ///     BrokerOrderId, ClientOrderId, InstrumentId, InstrumentKey, NativeId, OrderSnapshot, OrderState,
    ///     OrderType, Price, Qty, Side, Timestamps, UnixNanos,
    /// };
    ///
    /// let cid = ClientOrderId::new_v4();
    /// let sber = InstrumentId::new(
    ///     InstrumentKey::new("SBER", "TQBR"),
    ///     NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() },
    /// );
    /// let snap = OrderSnapshot::new(
    ///     cid,
    ///     sber,
    ///     OrderState::Acked { cid, broker_id: BrokerOrderId::new("42") },
    ///     Side::Buy,
    ///     OrderType::Limit,
    ///     Qty::from(10),
    ///     Timestamps::new(UnixNanos::ZERO, std::time::Instant::now()),
    /// )
    /// .with_broker_id(Some(BrokerOrderId::new("42")))
    /// .with_price(Some(Price::from(250)));
    /// assert_eq!(snap.price, Some(Price::from(250)));
    /// assert!(snap.filled.is_zero());
    /// ```
    pub fn new(
        cid: ClientOrderId,
        instrument: InstrumentId,
        state: OrderState,
        side: Side,
        order_type: OrderType,
        qty: Qty,
        ts: Timestamps,
    ) -> Self {
        Self {
            cid,
            broker_id: None,
            instrument,
            state,
            side,
            order_type,
            qty,
            filled: Qty::ZERO,
            price: None,
            ts,
        }
    }

    /// Sets the broker's id of the order.
    #[must_use]
    pub fn with_broker_id(mut self, broker_id: Option<BrokerOrderId>) -> Self {
        self.broker_id = broker_id;
        self
    }

    /// Sets the executed quantity.
    #[must_use]
    pub fn with_filled(mut self, filled: Qty) -> Self {
        self.filled = filled;
        self
    }

    /// Sets the limit price.
    #[must_use]
    pub fn with_price(mut self, price: Option<Price>) -> Self {
        self.price = price;
        self
    }
}

impl ReconcileReport {
    /// A report of the three lists. The struct is `#[non_exhaustive]`: build it with this function.
    pub fn new(
        confirmed: Vec<OrderSnapshot>,
        closed_offline: Vec<ClientOrderId>,
        foreign: Vec<OrderSnapshot>,
    ) -> Self {
        Self {
            confirmed,
            closed_offline,
            foreign,
        }
    }
}

/// A request to put a new order: see [`OrderExecutor::place`](crate::OrderExecutor::place).
///
/// The library sends exactly what is here. It does not check the quantity against lot size or the
/// price against the price step or the account, that is the caller's job (see
/// [`TradingParams`](crate::TradingParams)).
///
/// # Examples
///
/// A limit buy of 10 units at 250.00:
///
/// ```
/// use brokapi_core::{
///     ClientOrderId, InstrumentId, InstrumentKey, NativeId, OrderType, PlaceOrderRequest, Price, Qty, Side,
/// };
///
/// let req = PlaceOrderRequest {
///     cid: ClientOrderId::new_v4(),
///     instrument: InstrumentId::new(
///         InstrumentKey::new("SBER", "TQBR"),
///         NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() },
///     ),
///     side: Side::Buy,
///     order_type: OrderType::Limit,
///     qty: Qty::from(10),
///     price: Some(Price::from(250)),
/// };
/// assert_eq!(req.side, Side::Buy);
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlaceOrderRequest {
    /// Your id of this order. Fresh for every order, never reused, see [`ClientOrderId`].
    pub cid: ClientOrderId,
    /// What to trade.
    pub instrument: InstrumentId,
    /// Buy or sell.
    pub side: Side,
    /// Market or limit.
    pub order_type: OrderType,
    /// How much, see [`Qty`] for the unit.
    pub qty: Qty,
    /// Limit price. Required for [`OrderType::Limit`], `None` for [`OrderType::Market`].
    pub price: Option<Price>,
}

/// A request to change a working order: see [`OrderExecutor::replace`](crate::OrderExecutor::replace).
///
/// The broker cancels the original order and puts a new one under the new `cid`, so the result is a
/// different order with a different cid and broker id. Only brokers with
/// [`UserDataCaps::supports_replace`](crate::UserDataCaps::supports_replace) have it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReplaceOrderRequest {
    /// The cid of the NEW order. Fresh, never reused, see [`ClientOrderId`].
    pub cid: ClientOrderId,
    /// The cid of the working order to replace.
    pub original_cid: ClientOrderId,
    /// New quantity. `None` keeps the current one (the BCS connector refuses `None` for a partly
    /// filled order with [`Error::Rejected`](crate::Error::Rejected), code `qty_required`).
    pub qty: Option<Qty>,
    /// New price. `None` keeps the current one.
    pub price: Option<Price>,
}

/// The broker's confirmation that it took an order: the answer of `place`, `replace` and `place_stop`.
///
/// It says the order is accepted, not that it is filled. Follow the order with
/// [`OrderExecutor::order_state`](crate::OrderExecutor::order_state) or the order stream.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderAck {
    /// The cid you sent. Equal to the one in the request.
    pub cid: ClientOrderId,
    /// The id the broker gave to the order.
    pub broker_id: BrokerOrderId,
    /// When the answer was received, and the broker time if it gave one.
    pub ts: Timestamps,
}

/// An order as the broker shows it at one moment.
///
/// Returned by [`OrderExecutor::order_state`](crate::OrderExecutor::order_state) and
/// [`OrderExecutor::active_orders`](crate::OrderExecutor::active_orders), and sent as
/// [`Event::Order`](crate::Event::Order) on every change.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct OrderSnapshot {
    /// The cid of the order. For an order that was not placed through this connector and whose cid
    /// the broker does not give, the BCS connector makes a stable cid of its own from the broker id,
    /// so such an order is not in your ledger and shows up as `foreign` in a [`ReconcileReport`].
    pub cid: ClientOrderId,
    /// The broker's id of the order, when known.
    pub broker_id: Option<BrokerOrderId>,
    /// What the order trades.
    pub instrument: InstrumentId,
    /// Where the order is in its life.
    pub state: OrderState,
    /// Buy or sell.
    pub side: Side,
    /// Market or limit.
    pub order_type: OrderType,
    /// Total quantity of the order, see [`Qty`] for the unit.
    pub qty: Qty,
    /// Quantity executed so far.
    pub filled: Qty,
    /// Limit price. `None` for a market order, or when the broker does not show one.
    pub price: Option<Price>,
    /// When this was received, and the broker time if it gave one.
    pub ts: Timestamps,
}

/// The result of [`OrderExecutor::reconcile`](crate::OrderExecutor::reconcile): your ledger
/// compared with the active orders at the broker.
///
/// Every order of the ledger is in `confirmed` or in `closed_offline`. Every active order that is
/// not in the ledger is in `foreign`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ReconcileReport {
    /// In the ledger and active at the broker. The snapshot is the broker's current view.
    pub confirmed: Vec<OrderSnapshot>,
    /// In the ledger and NOT active at the broker: filled, cancelled or refused while you were not
    /// watching, or never seen by the broker. The report does not say which. Ask
    /// [`OrderExecutor::order_state`](crate::OrderExecutor::order_state) for the final state, and
    /// [`ExecutionSource::executions`](crate::ExecutionSource::executions) for the fills.
    pub closed_offline: Vec<ClientOrderId>,
    /// Active at the broker and not in the ledger: placed by hand, by another program or by an
    /// earlier run that lost its ledger. The library never touches them, and you should not either
    /// unless you know whose they are.
    pub foreign: Vec<OrderSnapshot>,
}
