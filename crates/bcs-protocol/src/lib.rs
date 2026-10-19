//! BCS wire protocol: REST and WebSocket clients, serde models and small helpers.
//!
//! This is the protocol layer for the BCS Trade API (a Russian broker). It talks to the broker in the
//! broker's own words and does nothing else: no order state, no mapping to common types, no retry of
//! orders. The connector crate `brokapi-bcs` sits on top of it and maps these types to the
//! broker-independent ones. Use this crate directly when you want the raw API.
//!
//! The reference book is at <https://brokapi.runinfra.ru>.
//!
//! # Parts
//!
//! * Module [`config`]: [`BcsConfig`](config::BcsConfig), the refresh token and the base URLs.
//! * Module [`http`]: [`BcsHttpClient`](http::client::BcsHttpClient) with one method per REST endpoint, and the
//!   request and response models in [`http::models`].
//! * Module [`ws`]: [`BcsWsClient`](ws::client::BcsWsClient) and [`WsSession`](ws::session::WsSession), a
//!   background WebSocket session that reconnects and restores subscriptions, and the frame models in
//!   [`ws::models`].
//! * Module [`models`]: enums and small types shared by REST and WebSocket.
//! * Module [`utils`]: price-step arithmetic, the date format of the API and a token-bucket rate limiter.
//! * Module [`error`]: [`BcsApiError`](error::BcsApiError).
//!
//! # Authorization in one paragraph
//!
//! The broker gives you a long-lived refresh token (90 days by the broker docs). The client trades it
//! for an access token (24 hours) at the first call, sends the access token as
//! `Authorization: Bearer ...`, and asks for a new one when the broker answers 401. You never handle
//! the access token yourself.
//!
//! # Example
//!
//! The example runs against the in-process imitation of the broker (`brokapi-mocks`), so it needs no
//! network and no account. With a real account you build `cfg` with
//! [`BcsConfig::from_env`](config::BcsConfig::from_env) or [`BcsConfig::new`](config::BcsConfig::new).
//!
//! ```
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let server = brokapi_mocks::bcs::MockServer::start(Default::default()).await;
//! # let cfg = brokapi_bcs_protocol::config::BcsConfig::for_mock("mock-refresh-token", &server.http_base);
//! use brokapi_bcs_protocol::http::client::BcsHttpClient;
//! use brokapi_bcs_protocol::http::models::OrderBookRequest;
//!
//! let client = BcsHttpClient::new(cfg)?;
//! let book = client
//!     .get_order_book(&OrderBookRequest {
//!         ticker: "SBER".into(),
//!         class_code: "TQBR".into(),
//!         depth: 5,
//!     })
//!     .await?;
//! assert_eq!(book.ticker, "SBER");
//! assert!(!book.bids.is_empty());
//! # Ok(())
//! # }
//! ```

#![warn(missing_docs)]

pub mod config;
pub mod error;
pub mod http;
pub mod models;
pub mod utils;
pub mod ws;
