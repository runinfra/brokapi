//! Own executions: the fills of your orders.

use serde::{Deserialize, Serialize};

use super::instrument::InstrumentId;
use super::number::{Money, Price, Qty};
use super::order::{BrokerOrderId, ClientOrderId};
use super::primitives::Side;
use super::time::Timestamps;

/// One fill of one of your orders: a part of an order that traded.
///
/// From [`ExecutionSource::executions`](crate::ExecutionSource::executions) (history) and as
/// [`Event::Execution`](crate::Event::Execution) from
/// [`ExecutionSource::subscribe_executions`](crate::ExecutionSource::subscribe_executions). An order
/// can have many executions. A public deal of the tape is [`MarketTrade`](crate::MarketTrade), a
/// different type.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Execution {
    /// Your cid of the order, when the broker gives it back. `None` for history from a broker that
    /// does not echo it (the BCS history has none). Match by `broker_order_id` then.
    pub cid: Option<ClientOrderId>,
    /// The broker's id of the order that traded.
    pub broker_order_id: BrokerOrderId,
    /// What traded.
    pub instrument: InstrumentId,
    /// Buy or sell.
    pub side: Side,
    /// Price of this fill.
    pub price: Price,
    /// Quantity of this fill (not of the whole order), see [`Qty`] for the unit.
    pub qty: Qty,
    /// Commission charged for this fill, in money, as the broker reports it. zero when the broker
    /// gives none (the BCS history has none), so zero does not prove a free fill.
    pub commission: Money,
    /// The broker's or exchange's id of the fill. Unique per fill, use it to drop duplicates.
    pub trade_id: String,
    /// When received, and the broker time if given.
    pub ts: Timestamps,
}
