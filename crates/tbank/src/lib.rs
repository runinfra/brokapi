//! T-Bank (T-Invest) connectors: `MarketDataConnector` + `UserDataConnector` over gRPC.
//!
//! This crate is the connector layer for T-Bank. It sits on the protocol layer `brokapi-tbank-protocol`
//! (generated gRPC client, endpoints, token) and implements the capability traits of `brokapi-core`,
//! so code written against those traits also works with the other brokers. Timing, rate limits and
//! stream supervision come from `brokapi-runtime` and `brokapi-sansio`.
//!
//! - [`MarketDataConnector`] - order book, trades, quotes, candles, instrument directory. Not tied to
//!   an account.
//! - [`UserDataConnector`] - orders, own trades, portfolio and stop orders of ONE account
//!   (`account_id` in the config). Several accounts need several connectors with one token.
//! - [`TbankRaw`] (`connector.raw()`) - any other method of the API, with the connector's token,
//!   endpoint and rate limiter.
//! - Rate limits are per user: give every connector of one user the same `Arc<BrokerLimiter>`
//!   (see [`limits::default_limits`]).
//!
//! T-Bank specifics worth knowing before you trade:
//!
//! - Orders are sized in whole LOTS. Prices are per one instrument. Portfolio positions are in pieces.
//! - A trading command that gets no answer is never repeated. The result is `Error::Unknown` and the
//!   caller checks the order first.
//! - `reconcile` must succeed once before the first order. It opens the command gate.
//!
//! The full guide is in the book: <https://brokapi.runinfra.ru>.
//!
//! # Examples
//!
//! The example starts the market data connector against the in-process mock of the broker and reads
//! an order book. Nothing touches the network.
//!
//! ```
//! use brokapi_core::{InstrumentId, InstrumentKey, NativeId, OrderBook, OrderBookSource, ShutdownMode};
//! use brokapi_runtime::BrokerLimiter;
//! use brokapi_tbank::limits::default_limits;
//! use brokapi_tbank::{MarketDataConfig, MarketDataConnector};
//! use tokio::sync::mpsc;
//! # use brokapi_mocks::tbank::{MockServer, TOKEN};
//! # use brokapi_tbank_protocol::TbankConfig;
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let server = MockServer::start(Default::default()).await;
//! # let cfg = TbankConfig::for_mock(TOKEN, &server.endpoint);
//! // In real code: `let cfg = brokapi_tbank_protocol::TbankConfig::sandbox("t.your-token");`
//! let (events_tx, _events) = mpsc::channel(256);
//! let limiter = BrokerLimiter::new(&default_limits());
//! let md = MarketDataConnector::start(MarketDataConfig::new(cfg, "tbank-md"), limiter, events_tx).await?;
//!
//! let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::Uid("uid-sber".into()));
//! let OrderBook::Snapshot { bids, asks, .. } = md.order_book(&sber, 5).await? else {
//!     panic!("T-Bank answers a snapshot")
//! };
//! assert!(!bids.is_empty() && !asks.is_empty());
//!
//! md.shutdown(ShutdownMode::Immediate).await;
//! # Ok(())
//! # }
//! ```
//!
//! For trading see [`UserDataConnector`]: it has the full place, cancel and refusal example.

#![warn(missing_docs)]

pub mod cid;
pub mod config;
pub mod core;
pub mod emit;
pub mod error;
pub mod ids;
pub mod limits;
pub mod map;
pub mod md;
pub mod raw;
pub mod stream;
pub mod ud;
pub mod util;

pub use cid::TbankCidCodec;
pub use config::{MarketDataConfig, UserDataConfig};
pub use core::{MdCore, MdFrame, UdCore, UdFrame};
pub use md::MarketDataConnector;
pub use raw::TbankRaw;
pub use ud::UserDataConnector;
