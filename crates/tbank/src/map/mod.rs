//! T-Bank protocol messages -> `brokapi-core` domain types.
//!
//! Units. T-Bank counts orders, order book levels, trades and candle volumes in LOTS and prices per
//! one instrument (not per lot). The library counts in units of the instrument, so the functions here
//! that read a quantity in lots take the lot size and return units (`OrderParts::with_lot`, the `lot`
//! argument of `stop::snapshot`), and the connectors scale the market data (`crate::lots`). Own trades
//! (`OrderTrade.quantity`, the history) and portfolio positions (`Quotation`) are in pieces already,
//! and are kept as they are. `Timeframe::MN` is 30 days when a candle close time has to be derived.
//!
//! Every function here is pure: it takes broker messages and returns domain types, with no network
//! and no state. That makes them cheap to test and safe to call from anywhere. The connectors use
//! them for you. Use them yourself when you read raw messages (for example through
//! [`TbankRaw`](crate::TbankRaw)) and want the same domain types.
//!
//! # Examples
//!
//! ```
//! use brokapi_tbank::map::market::{levels, wire_depth};
//! use brokapi_tbank_protocol::money::quotation;
//! use brokapi_tbank_protocol::v1::Order;
//!
//! // Depth 15 is sent as 20: the broker accepts 1, 10, 20, 30, 40, 50.
//! assert_eq!(wire_depth(15), 20);
//!
//! // Book levels come out best first: bids by price descending.
//! let bids = [
//!     Order { price: Some(quotation("273")), quantity: 5 },
//!     Order { price: Some(quotation("274")), quantity: 9 },
//! ];
//! let top = levels(&bids, true, 1);
//! assert_eq!(top[0].price, "274".parse()?);
//! # Ok::<(), brokapi_core::NumberError>(())
//! ```

pub mod execution;
pub mod instrument;
pub mod market;
pub mod order;
pub mod portfolio;
pub mod schedule;
pub mod stop;
