//! Translation between Finam protobuf messages and `brokapi-core` types.
//!
//! Pure functions, no network. Decimals arrive as strings and become exact `Decimal` numbers (an absent or unparseable
//! value becomes 0 or `None`, as each function says). Times arrive as protobuf timestamps and become
//! Unix nanoseconds.
//!
//! | Module | Maps |
//! |---|---|
//! | [`instrument`] | directory entries, trading parameters, schedules |
//! | [`market`] | order book rows, quotes, trades, bars |
//! | [`m10`] | 10-minute candles built from 5-minute ones |
//! | [`order`] | plain orders: statuses, requests |
//! | [`stop`] | stop orders: `STOP`, `STOP_LIMIT`, SL/TP |
//! | [`portfolio`] | account state, own trades |
//!
//! # Examples
//!
//! ```
//! use brokapi_finam::map::instrument::kind_from_type;
//! use brokapi_core::InstrumentKind;
//!
//! // Finam's instrument types are not enumerated in its docs: they are matched by substring.
//! assert_eq!(kind_from_type("EQUITIES"), InstrumentKind::Stock);
//! assert_eq!(kind_from_type("BONDS"), InstrumentKind::Bond);
//! ```

pub mod instrument;
pub mod m10;
pub mod market;
pub mod order;
pub mod portfolio;
pub mod stop;
