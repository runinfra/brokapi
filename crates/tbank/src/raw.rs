//! Direct access to any T-Bank API method ("back door"): same token, same endpoint, same
//! [`BrokerLimiter`] as the connector it came from.
//!
//! The connectors wrap what the shared traits can express. Everything else the API offers (bond
//! coupons, forecasts, favorites, reports, signals, sandbox, ...) is reachable here, so nothing the
//! broker supports is out of reach.
//!
//! # Examples
//!
//! The example runs against the in-process mock. With a real connector the call is the same, only the
//! config differs. For a method of your own, pass its proto name and call the generated client.
//!
//! ```
//! use brokapi_runtime::BrokerLimiter;
//! use brokapi_tbank::limits::default_limits;
//! use brokapi_tbank::{MarketDataConfig, MarketDataConnector};
//! use brokapi_tbank_protocol::v1 as pb;
//! use tokio::sync::mpsc;
//! # use brokapi_core::ShutdownMode;
//! # use brokapi_mocks::tbank::{MockServer, TOKEN};
//! # use brokapi_tbank_protocol::TbankConfig;
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let server = MockServer::start(Default::default()).await;
//! # let cfg = TbankConfig::for_mock(TOKEN, &server.endpoint);
//! let (events_tx, _events) = mpsc::channel(64);
//! let limiter = BrokerLimiter::new(&default_limits());
//! let md = MarketDataConnector::start(MarketDataConfig::new(cfg, "tbank-md"), limiter, events_tx).await?;
//! let raw = md.raw();
//!
//! // The quota is taken by the proto method name, then the call runs with the connector's token.
//! let accounts = raw
//!     .call("GetAccounts", |c| async move {
//!         c.users().get_accounts(pb::GetAccountsRequest { status: None }).await
//!     })
//!     .await?;
//! assert!(!accounts.accounts.is_empty());
//! # md.shutdown(ShutdownMode::Immediate).await;
//! # Ok(())
//! # }
//! ```
//!
//! Rules of a call:
//! 1. The quota is taken first, by the proto method name (`"GetBondCoupons"`), from the route in
//!    `limits.toml`. A name without a route spends the `"*"` default route, so a call
//!    is never free. A refusal is `Error::RateLimited` and the request is not sent.
//! 2. One closure is one API method. Do not call several methods inside one closure: only one quota
//!    unit is taken for it.
//! 3. The answer is cut by the request timeout of the connector.
//! 4. Status codes become [`brokapi_core::Error`] like everywhere else in the connector.
//!
//! [`call`](TbankRaw::call) is for reads: timeout and transport trouble are retryable errors.
//! [`call_command`](TbankRaw::call_command) is for anything that changes the account: a timeout or a
//! break is [`Error::Unknown`] (the broker may have acted), and the library never repeats it.
//!
//! The raw layer does not know about the connector's order registry: orders placed through it are
//! not tracked (no events, no `reconcile`). Use the shared traits for orders.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use brokapi_core::Error;
use brokapi_runtime::BrokerLimiter;
use brokapi_tbank_protocol::TbankClient;
use tonic::{Response, Status};

use crate::util::{CommandOutcome, command, query};

/// Direct access to any T-Bank API method, made by [`MarketDataConnector::raw`] or
/// [`UserDataConnector::raw`]. See the [module docs](self). Cheap to clone.
///
/// Reads go through [`call`](Self::call). Anything that changes the account goes through
/// [`call_command`](Self::call_command), which never lets a lost answer look like a refusal.
///
/// [`MarketDataConnector::raw`]: crate::MarketDataConnector::raw
/// [`UserDataConnector::raw`]: crate::UserDataConnector::raw
///
/// # Examples
///
/// A command that the broker refuses is [`Error::Rejected`]. Here the order does not exist.
///
/// ```
/// use brokapi_core::Error;
/// use brokapi_runtime::BrokerLimiter;
/// use brokapi_tbank::limits::default_limits;
/// use brokapi_tbank::{UserDataConfig, UserDataConnector};
/// use brokapi_tbank_protocol::v1 as pb;
/// use tokio::sync::mpsc;
/// # use brokapi_core::ShutdownMode;
/// # use brokapi_mocks::tbank::{MockServer, ACCOUNT_A, TOKEN};
/// # use brokapi_tbank_protocol::TbankConfig;
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let server = MockServer::start(Default::default()).await;
/// # let cfg = TbankConfig::for_mock(TOKEN, &server.endpoint).with_account(ACCOUNT_A);
/// let (events_tx, _events) = mpsc::channel(64);
/// let limiter = BrokerLimiter::new(&default_limits());
/// let ud = UserDataConnector::start(UserDataConfig::new(cfg, "tbank-ud"), limiter, events_tx).await?;
/// let raw = ud.raw();
/// let account = raw.account_id().expect("a user data connector has an account").to_string();
///
/// let result = raw
///     .call_command("CancelOrder", |c| async move {
///         c.orders()
///             .cancel_order(pb::CancelOrderRequest {
///                 account_id: account,
///                 order_id: "no-such-order".into(),
///                 order_id_type: Some(pb::OrderIdType::Request as i32),
///             })
///             .await
///     })
///     .await;
/// assert!(matches!(result, Err(Error::Rejected { .. })));
/// # ud.shutdown(ShutdownMode::Immediate).await;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct TbankRaw {
    client: TbankClient,
    limiter: Arc<BrokerLimiter>,
    timeout: Duration,
    account: Option<String>,
}

impl TbankRaw {
    pub(crate) fn new(
        client: TbankClient,
        limiter: Arc<BrokerLimiter>,
        timeout: Duration,
        account: Option<String>,
    ) -> Self {
        Self {
            client,
            limiter,
            timeout,
            account,
        }
    }

    /// Account of the connector this came from (user data connector only).
    ///
    /// `None` for a market data connector, which has no account. The raw layer does not add the
    /// account to your requests: put it into the request yourself.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_runtime::BrokerLimiter;
    /// use brokapi_tbank::limits::default_limits;
    /// use brokapi_tbank::{MarketDataConfig, MarketDataConnector};
    /// use tokio::sync::mpsc;
    /// # use brokapi_core::ShutdownMode;
    /// # use brokapi_mocks::tbank::{MockServer, TOKEN};
    /// # use brokapi_tbank_protocol::TbankConfig;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = MockServer::start(Default::default()).await;
    /// # let cfg = TbankConfig::for_mock(TOKEN, &server.endpoint);
    /// let (events_tx, _events) = mpsc::channel(64);
    /// let md = MarketDataConnector::start(
    ///     MarketDataConfig::new(cfg, "tbank-md"),
    ///     BrokerLimiter::new(&default_limits()),
    ///     events_tx,
    /// )
    /// .await?;
    /// assert_eq!(md.raw().account_id(), None);
    /// # md.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    pub fn account_id(&self) -> Option<&str> {
        self.account.as_deref()
    }

    /// The deadline applied to every call: the `request_timeout` of the connector's config (30 s by
    /// default).
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use brokapi_runtime::BrokerLimiter;
    /// use brokapi_tbank::limits::default_limits;
    /// use brokapi_tbank::{MarketDataConfig, MarketDataConnector};
    /// use tokio::sync::mpsc;
    /// # use brokapi_core::ShutdownMode;
    /// # use brokapi_mocks::tbank::{MockServer, TOKEN};
    /// # use brokapi_tbank_protocol::TbankConfig;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = MockServer::start(Default::default()).await;
    /// # let cfg = TbankConfig::for_mock(TOKEN, &server.endpoint).with_request_timeout(Duration::from_secs(5));
    /// let (events_tx, _events) = mpsc::channel(64);
    /// let md = MarketDataConnector::start(
    ///     MarketDataConfig::new(cfg, "tbank-md"),
    ///     BrokerLimiter::new(&default_limits()),
    ///     events_tx,
    /// )
    /// .await?;
    /// assert_eq!(md.raw().timeout(), Duration::from_secs(5));
    /// # md.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Reads: takes quota for `method`, runs the call, maps errors.
    ///
    /// For a server-streaming method the result type is `tonic::Streaming<_>`: the quota covers
    /// opening the stream, the stream is then yours to read.
    ///
    /// # Errors
    ///
    /// - [`Error::RateLimited`] when the limiter has no quota for `method`. The request is not sent.
    ///   Wait `retry_after` and call again.
    /// - [`Error::Transport`] on a timeout or a network status. A read may be repeated.
    /// - [`Error::Rejected`] when the broker refuses the request (`InvalidArgument`,
    ///   `NotFound`, which is the code `not_found`, `PermissionDenied`, ...).
    /// - [`Error::Config`] when the token is not accepted (`Unauthenticated`).
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::Error;
    /// use brokapi_runtime::BrokerLimiter;
    /// use brokapi_tbank::limits::limits_from_toml_str;
    /// use brokapi_tbank::{MarketDataConfig, MarketDataConnector};
    /// use brokapi_tbank_protocol::v1 as pb;
    /// use tokio::sync::mpsc;
    /// # use brokapi_core::ShutdownMode;
    /// # use brokapi_mocks::tbank::{MockServer, TOKEN};
    /// # use brokapi_tbank_protocol::TbankConfig;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = MockServer::start(Default::default()).await;
    /// # let cfg = TbankConfig::for_mock(TOKEN, &server.endpoint);
    /// // A tiny quota for the demo: one call per minute for every method.
    /// let limits = limits_from_toml_str(
    ///     r#"
    /// [tbank.buckets.one]
    /// rate = 1
    /// per = "60s"
    /// burst = 1
    /// [tbank.route]
    /// "*" = ["one"]
    /// [tbank.conns]
    /// max_conns = 1
    /// max_subs_per_conn = 1
    /// supports_multiplex = false
    /// "#,
    /// )?;
    /// let (events_tx, _events) = mpsc::channel(64);
    /// let md = MarketDataConnector::start(MarketDataConfig::new(cfg, "tbank-md"), BrokerLimiter::new(&limits), events_tx).await?;
    /// let raw = md.raw();
    ///
    /// let list = |c: brokapi_tbank_protocol::TbankClient| async move {
    ///     c.users().get_accounts(pb::GetAccountsRequest { status: None }).await
    /// };
    /// assert!(raw.call("GetAccounts", list).await.is_ok());
    /// // The second call finds the bucket empty: it is refused locally and never sent.
    /// assert!(matches!(raw.call("GetAccounts", list).await, Err(Error::RateLimited { .. })));
    /// # md.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn call<T, F, Fut>(&self, method: &str, f: F) -> Result<T, Error>
    where
        F: FnOnce(TbankClient) -> Fut,
        Fut: Future<Output = Result<Response<T>, Status>>,
    {
        self.limiter.acquire(method, brokapi_runtime::now())?;
        query(self.timeout, f(self.client.clone())).await
    }

    /// Commands: like [`call`](Self::call), but a timeout, a break or a server fault is
    /// `Error::Unknown`, because the broker may have executed the command. Never retried.
    ///
    /// # Errors
    ///
    /// - [`Error::RateLimited`] when the limiter has no quota. Nothing was sent.
    /// - [`Error::Rejected`] when the broker refuses (`InvalidArgument`, `NotFound`,
    ///   `PermissionDenied`, `FailedPrecondition`, ...). The broker did not act.
    /// - [`Error::Config`] when the token is not accepted. The broker did not act.
    /// - [`Error::Unknown`] on a timeout, a broken connection or a server fault (`Unavailable`,
    ///   `Internal`, ...). The command may or may not have been done. Do not repeat it. Ask the
    ///   broker for the state of the order or account first.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::Error;
    /// use brokapi_runtime::BrokerLimiter;
    /// use brokapi_tbank::limits::default_limits;
    /// use brokapi_tbank::{UserDataConfig, UserDataConnector};
    /// use brokapi_tbank_protocol::v1 as pb;
    /// use tokio::sync::mpsc;
    /// # use brokapi_core::ShutdownMode;
    /// # use brokapi_mocks::tbank::{MockServer, ACCOUNT_A, TOKEN};
    /// # use brokapi_tbank_protocol::TbankConfig;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let server = MockServer::start(Default::default()).await;
    /// # let cfg = TbankConfig::for_mock(TOKEN, &server.endpoint).with_account(ACCOUNT_A);
    /// let (events_tx, _events) = mpsc::channel(64);
    /// let limiter = BrokerLimiter::new(&default_limits());
    /// let ud = UserDataConnector::start(UserDataConfig::new(cfg, "tbank-ud"), limiter, events_tx).await?;
    ///
    /// // Cancelling an order the broker does not know is a definite refusal.
    /// let outcome = ud
    ///     .raw()
    ///     .call_command("CancelOrder", |c| async move {
    ///         c.orders()
    ///             .cancel_order(pb::CancelOrderRequest {
    ///                 account_id: ACCOUNT_A.into(),
    ///                 order_id: "no-such-order".into(),
    ///                 order_id_type: Some(pb::OrderIdType::Request as i32),
    ///             })
    ///             .await
    ///     })
    ///     .await;
    /// match outcome {
    ///     Err(Error::Rejected { code, .. }) => assert_eq!(code, "not_found"),
    ///     other => panic!("expected a refusal, got {other:?}"),
    /// }
    /// # ud.shutdown(ShutdownMode::Immediate).await;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn call_command<T, F, Fut>(&self, method: &str, f: F) -> Result<T, Error>
    where
        F: FnOnce(TbankClient) -> Fut,
        Fut: Future<Output = Result<Response<T>, Status>>,
    {
        self.limiter.acquire(method, brokapi_runtime::now())?;
        match command(self.timeout, f(self.client.clone())).await {
            CommandOutcome::Done(v) => Ok(v),
            CommandOutcome::Refused(_, err) => Err(err),
            CommandOutcome::Unknown => Err(Error::Unknown),
        }
    }
}
