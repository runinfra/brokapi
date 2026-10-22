//! Finam Trade API gRPC protocol layer.
//!
//! - [`pb`]: messages and clients generated from the official `.proto` (vendored in `proto/`,
//!   see `proto/README.md`), compiled without `protoc` by `build.rs`.
//! - [`auth`]: secret -> JWT, refreshed ahead of expiry; shareable between connectors.
//! - [`client`]: channel setup and service clients that attach the current JWT to every call.
//! - [`error`]: [`FinamError`], the error type of this layer.
//!
//! This is the protocol layer. The connectors that implement the shared traits (`brokapi-finam`) sit
//! on top of it. Use this crate directly only when you need a Finam call the connector does not wrap
//! (the connector's `raw()` is usually easier, it also keeps the rate limit).
//!
//! The book with the full guide: <https://brokapi.ru/docs/>
//!
//! # Examples
//!
//! Log in and ask for the last quote of an instrument. The example runs against an in-process
//! imitation of the broker, so it needs no account and no network:
//!
//! ```
//! use brokapi_finam_protocol::pb::grpc::tradeapi::v1::marketdata::QuoteRequest;
//! use brokapi_finam_protocol::{ApiEndpoint, FinamApi, start_token_manager};
//! # use brokapi_mocks::finam::{MockServer, SECRET};
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let server = MockServer::start().await;
//! # let endpoint = ApiEndpoint::new(server.url.clone());
//! # let secret = SECRET;
//! // For the real broker: `ApiEndpoint::default()` and your API secret.
//! let tokens = start_token_manager(&endpoint, secret, "my-app").await?;
//! let api = FinamApi::connect(&endpoint, tokens, false)?;
//! let reply = api
//!     .market_data()
//!     .last_quote(QuoteRequest { symbol: "SBER@MISX".into() })
//!     .await?
//!     .into_inner();
//! assert_eq!(reply.symbol, "SBER@MISX");
//! # Ok(())
//! # }
//! ```

#![warn(missing_docs)]

pub mod auth;
pub mod client;
pub mod error;

/// Generated messages and gRPC clients/servers.
#[allow(clippy::all, clippy::pedantic, missing_docs)]
pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/all.rs"));
}

/// The descriptors of every service and message of the API, as a serialized `FileDescriptorSet` (the
/// `.proto` files this crate is built from, with their imports). Load it into a descriptor pool to call a
/// method by its name, see [`FinamApi::grpc`].
pub const FILE_DESCRIPTOR_SET: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/descriptor.bin"));

pub use auth::{GrpcTokenApi, TokenApi, TokenInfo, TokenManager};
pub use client::{ApiEndpoint, Authorized, DEFAULT_ENDPOINT, FinamApi, start_token_manager};
pub use error::FinamError;
