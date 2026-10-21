//! Finam connectors: market data and user data over the Finam Trade API (gRPC).
//!
//! This is the connector layer. It implements the shared traits of `brokapi-core` on top of the protocol
//! layer (`brokapi-finam-protocol`), the sans-io logic (`brokapi-sansio`) and the runtime pieces (`brokapi-runtime`:
//! rate limiter, command gate, shutdown).
//!
//! | Type | Role | Traits |
//! |---|---|---|
//! | [`MarketDataConnector`] | order books, trades, quotes, candles, instrument directory. Not tied to an account | `OrderBookSource`, `MarketDataSource`, `InstrumentSource` |
//! | [`UserDataConnector`] | orders, stop orders, own trades, portfolio of ONE account | `OrderExecutor`, `StopOrderExecutor`, `ExecutionSource`, `PortfolioSource` |
//! | [`FinamRaw`] | any other Finam method, with the connector's token and rate limiter | none, see `connector.raw()` |
//!
//! Things that are true for the whole crate:
//!
//! - **One user data connector is one account.** To trade two accounts, start two connectors. They can
//!   share one token manager ([`FinamApiConfig::shared_tokens`]) and must share one rate limiter.
//! - **One rate limiter per Finam user.** Finam documents one request quota for the whole API. The bundled
//!   limits ([`limits::default_limits`]) are meant to be shared by every connector of the user.
//! - **A place command that got no answer is never repeated.** A timeout or a broken connection gives
//!   `Error::Unknown`, because a repeat could create a second order. Ask the order state or call
//!   `reconcile` to learn what happened.
//! - **A rejected order is a final answer.** `Error::Rejected` means the broker did not take the order.
//! - **Quantity is in pieces**, as everywhere in the Finam API, not in lots.
//! - **Instruments are `SYMBOL@MIC`** (for example `SBER@MISX`), carried as `NativeId::SymbolMic`.
//!
//! Finam has no sandbox. The examples in this documentation run against an in-process imitation of the
//! broker (`brokapi-mocks`). The imitation is written from the broker documentation. It does not prove how
//! the live broker behaves.
//!
//! The book with the full guide: <https://brokapi.ru/docs/>
//!
//! # Examples
//!
//! Read an order book, then place a limit order far from the market and cancel it:
//!
//! ```
//! use brokapi_core::{
//!     ClientOrderId, InstrumentId, InstrumentKey, NativeId, OrderBook, OrderBookSource, OrderExecutor,
//!     OrderType, PlaceOrderRequest, Price, Qty, ShutdownMode, Side,
//! };
//! use brokapi_finam::limits::default_limits;
//! use brokapi_finam::{MarketDataConfig, MarketDataConnector, UserDataConfig, UserDataConnector};
//! use brokapi_runtime::BrokerLimiter;
//! use tokio::sync::mpsc;
//! # use brokapi_finam::FinamApiConfig;
//! # use brokapi_mocks::finam::{MockServer, SECRET};
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let server = MockServer::start().await;
//! # let api = FinamApiConfig::new(SECRET).with_endpoint(server.url.clone());
//! # let account = "ACC1";
//! // `api` holds your secret (and the endpoint of the real broker by default).
//! // One limiter per Finam user, shared by every connector.
//! let limiter = BrokerLimiter::new(&default_limits());
//! let (events, _rx) = mpsc::channel(256);
//!
//! let md = MarketDataConnector::start(
//!     MarketDataConfig::new(api.clone(), "finam-md"),
//!     limiter.clone(),
//!     events.clone(),
//! )
//! .await?;
//! let sber = InstrumentId::new(
//!     InstrumentKey::new("SBER", "MISX"),
//!     NativeId::SymbolMic { symbol: "SBER".into(), mic: "MISX".into() },
//! );
//! let book = md.order_book(&sber, 5).await?;
//! assert!(matches!(book, OrderBook::Snapshot { .. }));
//!
//! let ud = UserDataConnector::start(
//!     UserDataConfig::new(api, account, "finam-ud"),
//!     limiter,
//!     events,
//! )
//! .await?;
//! // Orders are refused until the connector is reconciled with the broker (empty ledger here).
//! ud.reconcile(&[]).await?;
//! let cid = ClientOrderId::new_v4();
//! let ack = ud
//!     .place(PlaceOrderRequest {
//!         cid,
//!         instrument: sber,
//!         side: Side::Buy,
//!         order_type: OrderType::Limit,
//!         qty: Qty::from(1),
//!         price: Some(Price::from(1)),
//!     })
//!     .await?;
//! assert_eq!(ack.cid, cid);
//! ud.cancel(cid).await?;
//!
//! md.shutdown(ShutdownMode::Immediate).await;
//! ud.shutdown(ShutdownMode::Immediate).await;
//! # Ok(())
//! # }
//! ```
//!
//! `connector.raw()` ([`FinamRaw`]) reaches any other method of the API with the connector's token,
//! endpoint and rate limiter.

#![warn(missing_docs)]

pub mod cid;
pub mod config;
pub mod core;
pub mod error;
pub mod index;
pub mod limits;
pub mod map;
pub mod md;
pub mod pump;
pub mod raw;
pub mod rpc;
pub mod stream;
pub mod subs;
pub mod ud;
pub mod util;

pub use cid::FinamCidCodec;
pub use config::{FinamApiConfig, MarketDataConfig, UserDataConfig, api_config_from_env};
pub use core::{MdCore, SubStatus, UdCore};
pub use md::MarketDataConnector;
pub use raw::FinamRaw;
pub use ud::UserDataConnector;
