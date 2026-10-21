//! Stop orders - an additive capability next to plain orders (`StopOrderExecutor`).
//!
//! A stop order waits at the broker until the last price crosses `stop_price`, then the broker puts
//! an exchange order (market, or limit at `limit_price`). It is not an order on the exchange book
//! until it triggers.
//!
//! The client order id is the same UUID as for plain orders.

use serde::{Deserialize, Serialize};

use super::instrument::InstrumentId;
use super::number::{Price, Qty};
use super::order::{BrokerOrderId, ClientOrderId};
use super::primitives::Side;
use super::time::{Timestamps, UnixNanos};

/// What the stop order is for. Together with the side it fixes the direction of the trigger.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum StopOrderKind {
    /// Protects a position: a SELL triggers when the price falls to `stop_price`, a BUY when it
    /// rises. After the trigger the order is a market order, or a limit order at `limit_price`.
    StopLoss,
    /// Takes a profit: a SELL triggers when the price rises to `stop_price`, a BUY when it falls.
    /// After the trigger the order is a market order, or a limit order at `limit_price`.
    TakeProfit,
    /// Stop-limit: triggers like `StopLoss` and always puts a limit order, so `limit_price` is
    /// required.
    StopLimit,
}

impl StopOrderKind {
    /// `true` when the stop triggers on a RISING price for this side (the trigger price is above the
    /// current price). Falling otherwise.
    pub fn triggers_on_rise(self, side: Side) -> bool {
        match (self, side) {
            (Self::StopLoss | Self::StopLimit, Side::Buy) | (Self::TakeProfit, Side::Sell) => true,
            (Self::StopLoss | Self::StopLimit, Side::Sell) | (Self::TakeProfit, Side::Buy) => false,
        }
    }
}

/// How long the stop order lives.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum StopExpiry {
    /// Until cancelled.
    GoodTillCancel,
    /// Until the end of the trading day (not every broker has it).
    EndOfDay,
    /// Until the given moment (not every broker has it for every kind).
    GoodTillDate(UnixNanos),
}

/// A request to put a stop order: see [`StopOrderExecutor::place_stop`](crate::StopOrderExecutor::place_stop).
///
/// The library sends what is here. A [`StopOrderKind::StopLimit`] without `limit_price` is refused
/// with [`Error::Rejected`](crate::Error::Rejected) before anything is sent.
///
/// # Examples
///
/// A stop-loss that sells 10 units if the price falls to 240, at market:
///
/// ```
/// use brokapi_core::{
///     ClientOrderId, InstrumentId, InstrumentKey, NativeId, PlaceStopOrderRequest, Price, Qty, Side,
///     StopExpiry, StopOrderKind,
/// };
///
/// let req = PlaceStopOrderRequest {
///     cid: ClientOrderId::new_v4(),
///     instrument: InstrumentId::new(
///         InstrumentKey::new("SBER", "TQBR"),
///         NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() },
///     ),
///     side: Side::Sell,
///     kind: StopOrderKind::StopLoss,
///     qty: Qty::from(10),
///     stop_price: Price::from(240),
///     limit_price: None,
///     expiry: StopExpiry::GoodTillCancel,
/// };
/// assert!(!req.kind.triggers_on_rise(req.side));
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlaceStopOrderRequest {
    /// Your id of this stop order. Fresh, never reused, see [`ClientOrderId`].
    pub cid: ClientOrderId,
    /// What to trade.
    pub instrument: InstrumentId,
    /// Buy or sell. With `kind` it fixes the direction of the trigger.
    pub side: Side,
    /// Stop-loss, take-profit or stop-limit.
    pub kind: StopOrderKind,
    /// How much, see [`Qty`] for the unit.
    pub qty: Qty,
    /// The trigger price.
    pub stop_price: Price,
    /// Price of the exchange order put at the trigger. `None` = market. Required by `StopLimit`.
    pub limit_price: Option<Price>,
    /// How long the stop order lives.
    pub expiry: StopExpiry,
}

/// Lifecycle of a stop order at the broker.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum StopOrderState {
    /// Waiting for the trigger.
    Active,
    /// Triggered: the exchange order was put.
    Triggered,
    /// Cancelled before it triggered, by you or by the broker.
    Cancelled,
    /// The lifetime ended ([`StopExpiry`]) without a trigger.
    Expired,
    /// The broker's status does not fit the others. The text is the broker's.
    Other(String),
}

/// Which stop order a command is about.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum StopOrderRef {
    /// The cid given to `place_stop`. Resolved from what this connector saw. A connector that cannot
    /// recover it after a restart refuses with `Rejected` (`unknown_stop_order`), use `Broker`.
    Cid(ClientOrderId),
    /// The broker's id, from `OrderAck::broker_id` or `StopOrderSnapshot::broker_id`.
    Broker(BrokerOrderId),
}

/// A stop order as the broker shows it: see
/// [`StopOrderExecutor::active_stop_orders`](crate::StopOrderExecutor::active_stop_orders).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StopOrderSnapshot {
    /// `None` when the connector cannot tie the broker's stop order to a cid (placed by someone else,
    /// or the broker does not echo it and this process did not place it).
    pub cid: Option<ClientOrderId>,
    /// The broker's id of the stop order. Use it with [`StopOrderRef::Broker`].
    pub broker_id: BrokerOrderId,
    /// What the stop order trades.
    pub instrument: InstrumentId,
    /// Buy or sell.
    pub side: Side,
    /// Stop-loss, take-profit or stop-limit.
    pub kind: StopOrderKind,
    /// Quantity of the order put at the trigger, see [`Qty`] for the unit.
    pub qty: Qty,
    /// The trigger price.
    pub stop_price: Price,
    /// Price of the exchange order put at the trigger. `None` = market.
    pub limit_price: Option<Price>,
    /// How long the stop order lives.
    pub expiry: StopExpiry,
    /// Where it is in its life.
    pub state: StopOrderState,
    /// When received, and the broker time if given.
    pub ts: Timestamps,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trigger_direction_follows_kind_and_side() {
        assert!(!StopOrderKind::StopLoss.triggers_on_rise(Side::Sell));
        assert!(StopOrderKind::StopLoss.triggers_on_rise(Side::Buy));
        assert!(StopOrderKind::TakeProfit.triggers_on_rise(Side::Sell));
        assert!(!StopOrderKind::TakeProfit.triggers_on_rise(Side::Buy));
        assert!(!StopOrderKind::StopLimit.triggers_on_rise(Side::Sell));
        assert!(StopOrderKind::StopLimit.triggers_on_rise(Side::Buy));
    }
}
