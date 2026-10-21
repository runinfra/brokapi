//! The public surface of brokapi: domain types, capability traits, events and errors.
//!
//! This is the core crate of the workspace. It has no network code and does not depend on any
//! broker. A broker connector (`brokapi-bcs`, `brokapi-tbank`, `brokapi-finam`) implements the traits
//! of this crate, and your program is written against them, so the same code runs on every broker.
//! Documentation of the whole library, with guides: <https://brokapi.ru/docs/>.
//!
//! # What is here
//!
//! | Part | Items |
//! |---|---|
//! | Traits | [`OrderBookSource`], [`MarketDataSource`], [`InstrumentSource`] (market data), [`OrderExecutor`], [`ExecutionSource`], [`PortfolioSource`], [`StopOrderExecutor`] (your account) |
//! | Events | [`Envelope`] around an [`Event`], delivered into a `tokio::sync::mpsc` channel you give to the connector |
//! | Errors | [`Error`] with [`ErrorClass`], one enum for all brokers |
//! | Capabilities | [`MarketDataCaps`], [`UserDataCaps`]: what a connector can do, as data |
//! | Types | [`InstrumentId`], [`OrderBook`], [`PlaceOrderRequest`], [`OrderSnapshot`], [`Execution`], [`Portfolio`], [`Price`], [`Qty`], [`UnixNanos`] and the rest of [`types`] |
//! | Order ids | [`ClientOrderId`] and the [`CidCodec`] that maps it to a broker's form |
//!
//! # Rules to know before you trade
//!
//! - You make the [`ClientOrderId`] of every order and keep a ledger of them.
//! - Trading commands are refused until [`OrderExecutor::reconcile`] succeeds once
//!   ([`Error::NotReconciled`]).
//! - A command with no answer is [`Error::Unknown`]. The library never repeats a command, and neither
//!   should you before you have looked at the order.
//! - Read your event channel steadily. A subscription that must not lose events overflows loudly
//!   ([`Event::Overflow`]) and trading stops ([`Error::Degraded`]) until you reconcile.
//! - Shutdown never cancels orders at the exchange.
//!
//! # Examples
//!
//! Read an order book and handle the error by its class. The connector here is the BCS one on an
//! in-process imitation of the broker, so the example runs without a network.
//!
//! ```
//! use brokapi_bcs::limits::default_limits;
//! use brokapi_bcs::{MarketDataConfig, MarketDataConnector};
//! use brokapi_core::{
//!     ErrorClass, InstrumentId, InstrumentKey, NativeId, OrderBook, OrderBookSource, ShutdownMode,
//! };
//! use brokapi_runtime::BrokerLimiter;
//! # use brokapi_bcs_protocol::config::BcsConfig;
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
//! # let cfg = BcsConfig::for_mock("mock-refresh-token", &server.http_base);
//! // The connector writes stream events into this channel. A plain read does not need it.
//! let (events_tx, _events) = tokio::sync::mpsc::channel(256);
//! let limiter = BrokerLimiter::new(&default_limits());
//! let md = MarketDataConnector::start(MarketDataConfig::new(cfg, "bcs-md"), limiter, events_tx).await?;
//!
//! let sber = InstrumentId::new(
//!     InstrumentKey::new("SBER", "TQBR"),
//!     NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() },
//! );
//! match md.order_book(&sber, 5).await {
//!     Ok(OrderBook::Snapshot { bids, asks, .. }) => {
//!         println!("best bid {:?}, best ask {:?}", bids.first(), asks.first());
//!     }
//!     Ok(_) => unreachable!("a read answers a snapshot"),
//!     Err(e) if e.class() == ErrorClass::Retryable => println!("try again later: {e}"),
//!     Err(e) => println!("will not work as it is: {e}"),
//! }
//!
//! md.shutdown(ShutdownMode::Immediate).await;
//! # Ok(())
//! # }
//! ```

#![warn(missing_docs)]

pub mod cid;
pub mod error;
pub mod traits;
pub mod types;

pub use cid::{Charset, CidCaps, CidCodec, CidError, HexUuidCodec, NativeCid};
pub use error::{ConfigError, Error, ErrorClass, NOT_FOUND, TransportError};
pub use traits::{
    ExecutionSource, InstrumentSource, MarketDataSource, OrderBookSource, OrderExecutor,
    PortfolioSource, StopOrderExecutor,
};
pub use types::*;

/// The exact decimal number inside [`Price`], [`Qty`] and [`Money`] (from the `rust_decimal` crate).
pub use rust_decimal::Decimal;
/// Makes a [`Decimal`] from a literal: `dec!(250.15)`. Needs `Decimal` in scope
/// (`use brokapi_core::{Decimal, dec};`).
pub use rust_decimal_macros::dec;
