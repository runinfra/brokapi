//! BCS broker connectors: the market data connector and the user data connector.
//!
//! This crate is the connector layer for the BCS Trade API. It sits on top of the protocol crate
//! `brokapi-bcs-protocol` (HTTP and WebSocket clients, wire models) and the runtime crates (`brokapi-runtime`,
//! `brokapi-sansio`), and it implements the traits of `brokapi-core`:
//!
//! - [`MarketDataConnector`] implements `OrderBookSource`, `MarketDataSource` and `InstrumentSource`.
//!   It is not tied to an account.
//! - [`UserDataConnector`] implements `OrderExecutor`, `StopOrderExecutor`, `ExecutionSource` and
//!   `PortfolioSource` for one account (the one the refresh token belongs to).
//!
//! Both connectors start with an `async fn start(config, limiter, out)`. Commands return their answer
//! directly. Subscriptions deliver `Envelope`s into the channel `out`.
//!
//! Anything the shared traits cannot express is reachable through `connector.raw()` ([`BcsRaw`]):
//! the same token, endpoint and rate limiter, one API method at a time.
//! [`limits::default_limits`] gives the bundled rate limits. Give every connector of one BCS account
//! the same limiter, because the limits belong to the token.
//!
//! The full guide is at <https://brokapi.ru/docs/>.
//!
//! # Examples
//!
//! The example runs against an in-process imitation of the broker, so it needs no network and no
//! account. With a real account, `cfg` is built with `BcsConfig::from_env()` instead.
//!
//! ```
//! use std::sync::Arc;
//! use brokapi_bcs::limits::default_limits;
//! use brokapi_bcs::{MarketDataConfig, MarketDataConnector, UserDataConfig, UserDataConnector};
//! use brokapi_core::{PortfolioSource, ShutdownMode};
//! use brokapi_runtime::BrokerLimiter;
//! use tokio::sync::mpsc;
//! # use brokapi_bcs_protocol::config::BcsConfig;
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
//! # let cfg = BcsConfig::for_mock("mock-refresh-token", &server.http_base).with_write(true);
//! // One limiter for the whole account.
//! let limiter: Arc<BrokerLimiter> = BrokerLimiter::new(&default_limits());
//! let (md_tx, _md_events) = mpsc::channel(256);
//! let (ud_tx, _ud_events) = mpsc::channel(256);
//!
//! let md = MarketDataConnector::start(
//!     MarketDataConfig::new(cfg.clone(), "bcs-md"),
//!     Arc::clone(&limiter),
//!     md_tx,
//! )
//! .await?;
//! let ud = UserDataConnector::start(UserDataConfig::new(cfg, "bcs-ud"), limiter, ud_tx).await?;
//!
//! let portfolio = ud.portfolio().await?;
//! println!("{} balances", portfolio.balances.len());
//!
//! md.shutdown(ShutdownMode::Immediate).await;
//! ud.shutdown(ShutdownMode::Immediate).await;
//! # Ok(())
//! # }
//! ```

#![warn(missing_docs)]

pub mod cid;
pub mod config;
pub mod core;
pub mod error;
pub mod limits;
pub mod map;
pub mod md;
pub mod pump;
pub mod raw;
pub mod subs;
pub mod ud;
pub mod util;

pub use cid::BcsCidCodec;
pub use config::{MarketDataConfig, UserDataConfig};
pub use core::{MdCore, UdCore, UdFrame, UdRestFrame};
pub use md::MarketDataConnector;
pub use raw::BcsRaw;
pub use ud::UserDataConnector;
