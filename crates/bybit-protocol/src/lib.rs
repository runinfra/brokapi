//! Bybit API V5 wire protocol: REST and WebSocket clients, serde models, request signing.
//!
//! This is the protocol layer for the Bybit API V5 (unified trading account, category `linear`: USDT perpetuals and
//! futures). It talks to the exchange in the exchange's own words and does nothing else: no order state, no mapping
//! to common types, no retry of orders. The connector crate `brokapi-bybit` sits on top of it and maps these types to
//! the broker-independent ones. Use this crate directly when you want the raw API.
//!
//! # Parts
//!
//! * Module [`config`]: [`BybitConfig`](config::BybitConfig), the key pair, the contour (main, demo or test) and the
//!   base URLs.
//! * Module [`sign`]: the HMAC-SHA256 signature of a request and of the WebSocket login.
//! * Module [`http`]: [`BybitHttpClient`](http::client::BybitHttpClient) with one method per REST endpoint, and the
//!   request and response models in [`http::models`].
//! * Module [`ws`]: [`WsSession`](ws::session::WsSession), a background WebSocket session (public data or the private
//!   streams) that reconnects and restores subscriptions, and the frames in [`ws::models`].
//! * Module [`error`]: [`BybitApiError`](error::BybitApiError).
//!
//! # Authorization in one paragraph
//!
//! There are no tokens to renew. Every private request carries the API key, a timestamp, the receive window and the
//! HMAC-SHA256 signature of `timestamp + key + recv_window + query-or-body` in headers. The client keeps the offset
//! of the exchange clock (read once, from the server time endpoint) so a clock that runs a little off does not fail
//! the receive window. The private WebSocket logs in with the same key and an `expires` time.
//!
//! # Example
//!
//! The example runs against the in-process imitation of the exchange (`brokapi-mocks`), so it needs no network and no
//! account. With a real account you build `cfg` with [`BybitConfig::from_env`](config::BybitConfig::from_env) or
//! [`BybitConfig::new`](config::BybitConfig::new).
//!
//! ```
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let server = brokapi_mocks::bybit::MockServer::start(Default::default()).await;
//! # let cfg = brokapi_bybit_protocol::config::BybitConfig::for_mock("mock-key", "mock-secret", &server.http_base);
//! use brokapi_bybit_protocol::http::client::BybitHttpClient;
//!
//! let client = BybitHttpClient::new(cfg)?;
//! let book = client.get_order_book("linear", "MOCKUSDT", Some(5)).await?;
//! assert!(!book.bids.is_empty());
//! # Ok(())
//! # }
//! ```

#![warn(missing_docs)]

pub mod config;
pub mod error;
pub mod http;
pub mod sign;
pub mod ws;
