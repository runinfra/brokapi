//! Direct access to any Finam Trade API method ("back door"): same secret and token manager, same
//! endpoint, same [`BrokerLimiter`] as the connector it came from.
//!
//! The connectors wrap what the shared traits can express. Everything else (reports, corporate
//! actions, options chains, constituents, `PlaceSLTPOrder`, usage metrics, ...) is reachable here:
//!
//! ```
//! use brokapi_finam::limits::default_limits;
//! use brokapi_finam::{MarketDataConfig, MarketDataConnector};
//! use brokapi_runtime::BrokerLimiter;
//! use brokapi_finam_protocol::pb::grpc::tradeapi::v1::marketdata::QuoteRequest;
//! use tokio::sync::mpsc;
//! # use brokapi_finam::FinamApiConfig;
//! # use brokapi_mocks::finam::{MockServer, SECRET};
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let server = MockServer::start().await;
//! # let api = FinamApiConfig::new(SECRET).with_endpoint(server.url.clone());
//! let (events, _rx) = mpsc::channel(16);
//! let md = MarketDataConnector::start(
//!     MarketDataConfig::new(api, "finam-md"),
//!     BrokerLimiter::new(&default_limits()),
//!     events,
//! )
//! .await?;
//!
//! // The name is the proto method name: the quota is looked up by it.
//! let raw = md.raw();
//! let quote = raw
//!     .call("LastQuote", |api| async move {
//!         api.market_data().last_quote(QuoteRequest { symbol: "SBER@MISX".into() }).await
//!     })
//!     .await?;
//! assert_eq!(quote.symbol, "SBER@MISX");
//! # Ok(())
//! # }
//! ```
//!
//! Rules of a call:
//! 1. The quota is taken first, by the proto method name (`"GetFutureSplits"`), from the route in
//!    `configs/finam_limits.toml`. A name without a route spends the `"*"` default route, so a call is
//!    never free. A refusal is `Error::RateLimited` and the request is not sent.
//! 2. The closure gets a [`FinamApi`] that attaches the CURRENT JWT to every request (the connector's
//!    token manager renews it ahead of expiry). One closure is one API method.
//! 3. The answer is cut by the connector's request timeout.
//! 4. [`call`](FinamRaw::call) is for reads: on `UNAUTHENTICATED` the JWT is renewed and the call is
//!    made once more. [`call_command`](FinamRaw::call_command) is for anything that changes the
//!    account: no retry, and a timeout or break is [`Error::Unknown`].
//!
//! A stream opened through `call` keeps the JWT it was opened with. Reopen it after the token
//! changes (the connector does that for its own streams).
//!
//! The raw layer does not know about the connector's order index: orders placed through it are not
//! tracked (no events, no `reconcile`). Use the shared traits for orders.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use brokapi_core::Error;
use brokapi_finam_protocol::FinamApi;
use brokapi_runtime::BrokerLimiter;
use tonic::{Response, Status};

use crate::error::map_command_err;
use crate::rpc::Rpc;

/// Handle for calling any Finam method with the connector's token, endpoint and rate limiter.
///
/// Get it from `MarketDataConnector::raw` or `UserDataConnector::raw`. Cheap to clone. It stays
/// usable while the connector it came from is alive. See the [module docs](self) for the rules of a call.
///
/// # Examples
///
/// A command goes through [`call_command`](Self::call_command), a read through [`call`](Self::call):
///
/// ```
/// use brokapi_core::Error;
/// use brokapi_finam::limits::default_limits;
/// use brokapi_finam::{UserDataConfig, UserDataConnector};
/// use brokapi_runtime::BrokerLimiter;
/// use brokapi_finam_protocol::pb::grpc::tradeapi::v1::accounts::GetAccountRequest;
/// use tokio::sync::mpsc;
/// # use brokapi_finam::FinamApiConfig;
/// # use brokapi_mocks::finam::{MockServer, SECRET};
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let server = MockServer::start().await;
/// # let api = FinamApiConfig::new(SECRET).with_endpoint(server.url.clone());
/// let (events, _rx) = mpsc::channel(16);
/// let ud = UserDataConnector::start(
///     UserDataConfig::new(api, "ACC1", "finam-ud"),
///     BrokerLimiter::new(&default_limits()),
///     events,
/// )
/// .await?;
/// let raw = ud.raw();
/// assert_eq!(raw.account_id(), Some("ACC1"));
///
/// let account = raw
///     .call("GetAccount", |api| async move {
///         api.accounts().get_account(GetAccountRequest { account_id: "ACC1".into() }).await
///     })
///     .await?;
/// assert_eq!(account.account_id, "ACC1");
///
/// // An account the broker does not know is a refusal.
/// let err = raw
///     .call("GetAccount", |api| async move {
///         api.accounts().get_account(GetAccountRequest { account_id: "NOPE".into() }).await
///     })
///     .await
///     .unwrap_err();
/// assert!(matches!(err, Error::Rejected { .. }));
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct FinamRaw {
    rpc: Rpc,
    account: Option<String>,
}

impl FinamRaw {
    pub(crate) fn new(rpc: Rpc, account: Option<String>) -> Self {
        Self { rpc, account }
    }

    /// Account of the connector this came from. `None` for a market data connector.
    pub fn account_id(&self) -> Option<&str> {
        self.account.as_deref()
    }

    /// Deadline applied to every call (`FinamApiConfig::request_timeout` of the connector).
    pub fn timeout(&self) -> Duration {
        self.rpc.timeout
    }

    /// The limiter shared with the connector.
    pub fn limiter(&self) -> &Arc<BrokerLimiter> {
        &self.rpc.limiter
    }

    /// Reads: quota for `method`, deadline, one re-auth retry, error mapping.
    ///
    /// `f` may run twice (after a re-auth), hence `Fn`. For a server-streaming method the result is
    /// `tonic::Streaming<_>`: the quota covers opening the stream. A stream opened here is not
    /// supervised: when it breaks or the token changes, open it again yourself.
    ///
    /// Do not use it for a call that changes the account, because of the repeat. Use
    /// [`call_command`](Self::call_command).
    ///
    /// # Errors
    ///
    /// - [`Error::RateLimited`]: the local quota for `method` is spent. Nothing was sent.
    /// - [`Error::Rejected`]: the broker refused (bad argument, unknown id, no permission, or the
    ///   token was refused again after the renewal).
    /// - [`Error::Transport`]: connection failure, or no answer within [`timeout`](Self::timeout).
    ///
    /// A failed token renewal is reported with the same mapping.
    ///
    /// # Examples
    ///
    /// See the [module docs](self).
    pub async fn call<T, F, Fut>(&self, method: &str, f: F) -> Result<T, Error>
    where
        F: Fn(FinamApi) -> Fut,
        Fut: Future<Output = Result<Response<T>, Status>>,
    {
        self.rpc.read(method, f).await
    }

    /// Commands: quota for `method`, deadline, NO retry. Anything that may have reached the broker
    /// (timeout, break, server fault) is `Error::Unknown`.
    ///
    /// There is no repeat on purpose: a second send could change the account twice. When the answer
    /// is `Unknown`, read the state (orders, positions) before deciding anything.
    ///
    /// The order index of the connector does not see what you place here: no events, no
    /// `reconcile`. For plain and stop orders use the shared traits.
    ///
    /// # Errors
    ///
    /// - [`Error::RateLimited`]: the local quota for `method` is spent. Nothing was sent.
    /// - [`Error::Rejected`]: the broker certainly did not process the request (for example
    ///   `INVALID_ARGUMENT`, `NOT_FOUND`, or `UNAUTHENTICATED`: a command is not repeated after a
    ///   token renewal).
    /// - [`Error::Unknown`]: timeout, broken connection or server fault. The command may have been applied.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::Error;
    /// use brokapi_finam::limits::default_limits;
    /// use brokapi_finam::{MarketDataConfig, MarketDataConnector};
    /// use brokapi_runtime::BrokerLimiter;
    /// use brokapi_finam_protocol::pb::grpc::tradeapi::v1::marketdata::QuoteRequest;
    /// use tokio::sync::mpsc;
    /// # use brokapi_finam::FinamApiConfig;
    /// # use brokapi_mocks::finam::{MockServer, SECRET};
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = MockServer::start().await;
    /// # let api = FinamApiConfig::new(SECRET).with_endpoint(server.url.clone());
    /// # let (events, _rx) = mpsc::channel(16);
    /// # let md = MarketDataConnector::start(
    /// #     MarketDataConfig::new(api, "finam-md"),
    /// #     BrokerLimiter::new(&default_limits()),
    /// #     events,
    /// # )
    /// # .await?;
    /// # // The mock has no state-changing call that md owns, so a read method stands in for a command.
    /// // Same shape as a read, but never repeated. The result of a real command is handled the same way.
    /// let sent = md
    ///     .raw()
    ///     .call_command("LastQuote", |api| async move {
    ///         api.market_data().last_quote(QuoteRequest { symbol: "SBER@MISX".into() }).await
    ///     })
    ///     .await;
    /// match sent {
    ///     Ok(reply) => assert_eq!(reply.symbol, "SBER@MISX"),
    ///     Err(Error::Unknown) => { /* read the state before acting */ }
    ///     Err(other) => panic!("refused: {other}"),
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn call_command<T, F, Fut>(&self, method: &str, f: F) -> Result<T, Error>
    where
        F: Fn(FinamApi) -> Fut,
        Fut: Future<Output = Result<Response<T>, Status>>,
    {
        self.rpc.acquire(method)?;
        self.rpc
            .send_command(f)
            .await
            .map_err(|e| map_command_err(&e))
    }
}
