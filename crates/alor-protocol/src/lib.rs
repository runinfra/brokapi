//! ALOR Broker wire protocol: REST and WebSocket clients, serde models and small helpers.
//!
//! This is the protocol layer for the ALOR OpenAPI (a Russian broker). It talks to the broker in the broker's
//! own words and does nothing else: no order state, no mapping to common types, no retry of orders. The
//! connector crate `brokapi-alor` sits on top of it and maps these types to the broker-independent ones. Use
//! this crate directly when you want the raw API.
//!
//! The reference book is at <https://brokapi.ru/docs/>.
//!
//! # Parts
//!
//! * Module [`config`]: [`AlorConfig`](config::AlorConfig), the refresh token, the contour and the base URLs.
//! * Module [`http`]: [`AlorHttpClient`](http::client::AlorHttpClient) with one method per REST endpoint, and
//!   the request and response models in [`http::models`].
//! * Module [`ws`]: [`WsSession`](ws::session::WsSession), a background WebSocket session of the data channel
//!   that reconnects and restores subscriptions, the subscription messages in [`ws::models`], and the trading
//!   channel in [`ws::cws`].
//! * Module [`error`]: [`AlorApiError`](error::AlorApiError).
//!
//! # Authorization in one paragraph
//!
//! The broker gives you a long-lived refresh token (a year in the production contour). The client trades it for
//! an access token (it lives 30 minutes) at the first call, sends the access token as
//! `Authorization: Bearer ...` (on the WebSocket data channel: in every message), and asks for a new one before
//! it is spent. You never handle the access token yourself.
//!
//! # Example
//!
//! The example runs against the in-process imitation of the broker (`brokapi-mocks`), so it needs no network and
//! no account. With a real account you build `cfg` with [`AlorConfig::from_env`](config::AlorConfig::from_env)
//! or [`AlorConfig::new`](config::AlorConfig::new).
//!
//! ```
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let server = brokapi_mocks::alor::MockServer::start(Default::default()).await;
//! # let cfg = brokapi_alor_protocol::config::AlorConfig::for_mock("mock-refresh-token", &server.http_base);
//! use brokapi_alor_protocol::http::client::AlorHttpClient;
//!
//! let client = AlorHttpClient::new(cfg)?;
//! let book = client.get_order_book("MOEX", "SBER", Some("TQBR"), Some(5)).await?;
//! assert!(!book.bids.is_empty());
//! # Ok(())
//! # }
//! ```

#![warn(missing_docs)]

pub mod config;
pub mod error;
pub mod http;
pub mod ws;
