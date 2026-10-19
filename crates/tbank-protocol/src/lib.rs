//! Low-level T-Bank (T-Invest) API wire protocol: the gRPC client generated from the official `.proto`.
//!
//! This is the protocol layer of the T-Bank connector. It knows how to talk to the broker and
//! nothing about orders, books or events. The connector crate `brokapi-tbank` sits on top of it and
//! turns these messages into the shared `brokapi-core` types. Use this crate directly only when you
//! need a call the connectors do not wrap.
//!
//! - [`v1`] - generated messages, enums and gRPC clients. Every service and method of the official
//!   contract is generated. Their documentation comes from the `.proto` comments.
//! - [`TbankConfig`] - endpoint (production, sandbox or a local mock), token, account, timeouts.
//! - [`TbankClient`] - one shared HTTP/2 channel with Bearer-token authorization. Each accessor
//!   (for example [`TbankClient::orders`]) returns a ready generated client.
//! - [`AuthInterceptor`] - the piece that adds `authorization: Bearer <token>` to every request.
//! - [`money`] - `Quotation` and `MoneyValue` (units and nano) converted to and from `Decimal`, without `f64` on the way.
//! - [`ProtocolError`] - configuration and transport errors raised while building the channel.
//!
//! The full guide is in the book: <https://brokapi.runinfra.ru>.
//!
//! # Examples
//!
//! The example below starts the in-process T-Bank mock, builds a client for it and lists the
//! accounts the token can see. Nothing touches the network and no money is involved.
//!
//! ```
//! use brokapi_tbank_protocol::{TbankClient, TbankConfig};
//! # use brokapi_mocks::tbank::{MockServer, TOKEN};
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let server = MockServer::start(Default::default()).await;
//! # let cfg = TbankConfig::for_mock(TOKEN, &server.endpoint);
//! // In real code: `TbankConfig::sandbox("t.your-token")` or `TbankConfig::production(..)`.
//! let client = TbankClient::new(&cfg)?;
//! let accounts = client.list_accounts().await?;
//! assert!(!accounts.is_empty());
//! for account in &accounts {
//!     println!("account {}", account.id);
//! }
//! # Ok(())
//! # }
//! ```

#![warn(missing_docs)]

pub mod client;
pub mod config;
pub mod error;
pub mod money;

/// Messages, enums and gRPC clients generated from the official T-Bank `.proto` files.
///
/// The documentation of each item is the comment from the `.proto` file, in the broker's own
/// words (Russian). Prices are [`Quotation`] and [`MoneyValue`] (units and nano), see [`crate::money`].
///
/// [`Quotation`]: crate::v1::Quotation
/// [`MoneyValue`]: crate::v1::MoneyValue
#[allow(clippy::all, clippy::pedantic, rustdoc::all, missing_docs)]
pub mod v1 {
    tonic::include_proto!("tinkoff.public.invest.api.contract.v1");
}

pub use client::{AuthInterceptor, Authed, TbankClient};
pub use config::{PROD_ENDPOINT, SANDBOX_ENDPOINT, TbankConfig};
pub use error::ProtocolError;
