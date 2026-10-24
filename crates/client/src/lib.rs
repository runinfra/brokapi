//! One account at any broker of the library, behind one type.
//!
//! The connectors of the brokers each have their own types. [`Client`] is the layer above them: it
//! starts the market data and the account connector of the broker you name, owns the event channel and the rate
//! limiter, and offers the same calls whichever broker it is.
//!
//! | You want | Use |
//! |---|---|
//! | Typed calls in Rust | the traits of `brokapi-core` implemented by [`Client`] ([`OrderExecutor`](brokapi::core::OrderExecutor), [`PortfolioSource`](brokapi::core::PortfolioSource), ...) |
//! | Calls by name with JSON, from another language or process | [`Client::call`], or [`call_by_name`] on any [`Account`] |
//! | Any method of the broker's own API | [`Client::raw`] |
//! | A long candle history | [`Client::candle_pages`] |
//! | A quota shared by several clients of one broker user | [`Limiter`] |
//!
//! [`Account`] is the trait of "one broker account as a bot uses it". [`Client`] is one, and so is the client of a
//! gateway (`RemoteClient` of `brokapi-gateway`), which is how the Python module reaches a gateway with the same calls.
//!
//! The Python module (`pip install brokapi`) and the gateway (`brokapi-gateway`) are built on this crate, so they
//! follow the same rules: a command is never repeated, a command runs to its end when the caller stops waiting, and
//! trading is refused until the first reconcile.
//!
//! Documentation of the whole library, with guides: <https://brokapi.ru/docs/>.

#![warn(missing_docs)]

mod account;
mod call;
mod client;
mod conn;
mod error;
mod raw;
mod settings;
mod traits;

pub use account::Account;
pub use call::{call_by_name, candle_pages_from_json, is_command};
pub use client::{
    Broker, BrokerConfig, CandlePage, CandlePageError, CandlePager, Caps, Client, CloseReport,
    Limiter,
};
pub use error::{ClientError, config_error};
pub use settings::Settings;
