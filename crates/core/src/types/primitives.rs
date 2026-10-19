//! Shared enum primitives: [`Side`] and [`OrderType`]. The numbers are in `number.rs`.

use serde::{Deserialize, Serialize};

/// Direction of an order, a trade or a stop order.
///
/// # Examples
///
/// ```
/// use brokapi_core::Side;
///
/// assert_ne!(Side::Buy, Side::Sell);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Side {
    /// Buy: the position grows.
    Buy,
    /// Sell: the position shrinks.
    Sell,
}

/// How a plain order is priced, see [`PlaceOrderRequest`](crate::PlaceOrderRequest).
///
/// Stop orders have their own kinds, see [`StopOrderKind`](crate::StopOrderKind).
///
/// # Examples
///
/// ```
/// use brokapi_core::OrderType;
///
/// assert_ne!(OrderType::Market, OrderType::Limit);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OrderType {
    /// Executes at the best available price now. `price` of the request should be `None`.
    Market,
    /// Executes at the given price or better. `price` of the request is required: the BCS
    /// connector refuses a limit order without it with [`Error::Rejected`](crate::Error::Rejected)
    /// (code `missing_price`) before anything is sent.
    Limit,
}
