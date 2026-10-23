//! Bybit connectors: the market data connector and the user data connector.
//!
//! This crate is the connector layer for the Bybit API V5, category `linear` (USDT perpetuals and futures). It sits on
//! top of the protocol crate `brokapi-bybit-protocol` (HTTP and WebSocket clients, wire models) and the runtime crates
//! (`brokapi-runtime`, `brokapi-sansio`), and it implements the traits of `brokapi-core`:
//!
//! - [`MarketDataConnector`] implements `OrderBookSource`, `MarketDataSource` and `InstrumentSource`. It is not tied to
//!   an account and needs no key for the data.
//! - [`UserDataConnector`] implements `OrderExecutor`, `StopOrderExecutor`, `ExecutionSource` and `PortfolioSource` for
//!   one unified trading account.
//!
//! Both connectors start with an `async fn start(config, limiter, out)`. Commands return their answer directly.
//! Subscriptions deliver `Envelope`s into the channel `out`.
//!
//! The contour is a setting of the config: the main exchange by default, the demo account (`api-demo.bybit.com`, virtual
//! money, round the clock) with `BybitConfig::with_env(BybitEnv::Demo)`. Anything the shared traits cannot express is
//! reachable through `connector.raw()` ([`BybitRaw`]): the same key, endpoint and rate limiter, one API method at a time.
//! [`limits::default_limits`] gives the bundled rate limits. Give every connector of one Bybit login the same limiter,
//! because the limits belong to the login.
//!
//! The full guide is at <https://brokapi.ru/docs/>.
//!
//! # Examples
//!
//! The example runs against an in-process imitation of the exchange, so it needs no network and no account. With a real
//! account, `cfg` is built with `BybitConfig::from_env()` instead.
//!
//! ```
//! use std::sync::Arc;
//! use brokapi_bybit::limits::default_limits;
//! use brokapi_bybit::{MarketDataConfig, MarketDataConnector, UserDataConfig, UserDataConnector};
//! use brokapi_core::{PortfolioSource, ShutdownMode};
//! use brokapi_runtime::BrokerLimiter;
//! use tokio::sync::mpsc;
//! # use brokapi_bybit_protocol::config::BybitConfig;
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let server = brokapi_mocks::bybit::MockServer::start(Default::default()).await;
//! # let cfg = BybitConfig::for_mock("mock-key", "mock-secret", &server.http_base);
//! // One limiter for the whole login.
//! let limiter: Arc<BrokerLimiter> = BrokerLimiter::new(&default_limits());
//! let (md_tx, _md_events) = mpsc::channel(256);
//! let (ud_tx, _ud_events) = mpsc::channel(256);
//!
//! let md = MarketDataConnector::start(
//!     MarketDataConfig::new(cfg.clone(), "bybit-md"),
//!     Arc::clone(&limiter),
//!     md_tx,
//! )
//! .await?;
//! let ud = UserDataConnector::start(UserDataConfig::new(cfg, "bybit-ud"), limiter, ud_tx).await?;
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

pub use cid::BybitCidCodec;
pub use config::{MarketDataConfig, UserDataConfig};
pub use core::{MdCore, UdCore};
pub use md::MarketDataConnector;
pub use raw::BybitRaw;
pub use ud::UserDataConnector;
