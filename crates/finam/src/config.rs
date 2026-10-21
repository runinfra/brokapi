//! Connector configuration: the credentials and endpoint shared by both roles ([`FinamApiConfig`]) and
//! the settings of each role ([`MarketDataConfig`], [`UserDataConfig`]).
//!
//! # Examples
//!
//! ```
//! use brokapi_finam::{FinamApiConfig, MarketDataConfig, UserDataConfig};
//!
//! // Secret from your own storage. The default endpoint is the real broker.
//! let api = FinamApiConfig::new("my-api-secret");
//! let md = MarketDataConfig::new(api.clone(), "finam-md");
//! let ud = UserDataConfig::new(api, "A12345", "finam-ud-A12345");
//! assert_eq!(ud.account_id, "A12345");
//! assert_eq!(&*md.source, "finam-md");
//! // `{:?}` never prints the secret.
//! assert!(!format!("{ud:?}").contains("my-api-secret"));
//! ```

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use brokapi_core::{ConfigError, Error};
use brokapi_finam_protocol::{ApiEndpoint, FinamError, TokenManager};
use brokapi_runtime::PriceGrid;

/// Connection and credentials, shared in shape by both connector roles.
///
/// Holds the API secret, so it is not `Display`, and its `Debug` output hides the secret.
///
/// # Examples
///
/// Two accounts of one secret share one JWT. The token manager is started once and put into both
/// configs. Here the broker is the in-process imitation:
///
/// ```
/// use brokapi_finam::{FinamApiConfig, UserDataConfig};
/// # use brokapi_mocks::finam::{MockServer, SECRET};
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let server = MockServer::start().await;
/// # let cfg = FinamApiConfig::new(SECRET).with_endpoint(server.url.clone());
/// // `cfg` is your config: secret plus endpoint.
/// let tokens = cfg.shared_tokens().await?;
/// let shared = cfg.with_tokens(tokens);
/// let first = UserDataConfig::new(shared.clone(), "ACC1", "finam-ud-ACC1");
/// let second = UserDataConfig::new(shared, "ACC2", "finam-ud-ACC2");
/// assert_ne!(first.account_id, second.account_id);
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct FinamApiConfig {
    /// Address and timeouts of the channel. Default: the production endpoint.
    pub endpoint: ApiEndpoint,
    /// API secret. Ignored when `tokens` is set.
    pub secret: String,
    /// Optional application id sent with `Auth`.
    pub source_app_id: String,
    /// Already running token manager to reuse (several accounts / roles on one secret).
    pub tokens: Option<Arc<TokenManager>>,
    /// Send `authorization: Bearer <jwt>` instead of the bare JWT (official SDKs send it bare).
    pub bearer: bool,
    /// Deadline of a unary call. Default 10 seconds.
    ///
    /// When it passes with no answer, a read fails with a transport error, and a command (place, cancel)
    /// ends as `Error::Unknown`, because the broker may still have applied it.
    pub request_timeout: Duration,
}

impl FinamApiConfig {
    /// Config for `secret` with the production endpoint and default settings.
    ///
    /// Nothing is checked or sent here. An empty or wrong secret shows up when a connector starts
    /// (`Error::Config` for an empty one, `Error::Rejected` for one the broker refuses).
    pub fn new(secret: impl Into<String>) -> Self {
        Self {
            endpoint: ApiEndpoint::default(),
            secret: secret.into(),
            source_app_id: String::new(),
            tokens: None,
            bearer: false,
            request_timeout: Duration::from_secs(10),
        }
    }

    /// Reuse a token manager (see [`FinamApiConfig::shared_tokens`]).
    ///
    /// When set, the connector does not log in itself and `secret` is not used.
    pub fn with_tokens(mut self, tokens: Arc<TokenManager>) -> Self {
        self.tokens = Some(tokens);
        self
    }

    /// Points the config at another gRPC server, with the default timeouts.
    ///
    /// `url` is `https://host:port` (TLS) or `http://host:port` (plain, for a proxy or the test
    /// imitation). It replaces the whole [`ApiEndpoint`], so set custom timeouts on
    /// [`FinamApiConfig::endpoint`] after this call.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_finam::FinamApiConfig;
    ///
    /// let cfg = FinamApiConfig::new("secret").with_endpoint("http://127.0.0.1:50051");
    /// assert_eq!(cfg.endpoint.url, "http://127.0.0.1:50051");
    /// ```
    pub fn with_endpoint(mut self, url: impl Into<String>) -> Self {
        self.endpoint = ApiEndpoint::new(url);
        self
    }

    /// Authenticates once and returns the manager to put into several connector configs, so that
    /// all accounts of one secret share a single JWT.
    ///
    /// The manager renews the token in the background until the last `Arc` is dropped.
    ///
    /// # Errors
    ///
    /// - [`Error::Config`]: the secret is empty or the endpoint URL is invalid.
    /// - [`Error::Config`] also: the broker refused the secret (a wrong token is a configuration problem at every broker).
    /// - [`Error::RateLimited`]: the broker answered `RESOURCE_EXHAUSTED`.
    /// - [`Error::Transport`]: the broker could not be reached or the call timed out.
    pub async fn shared_tokens(&self) -> Result<Arc<TokenManager>, Error> {
        brokapi_finam_protocol::start_token_manager(
            &self.endpoint,
            &self.secret,
            &self.source_app_id,
        )
        .await
        .map_err(|e| crate::error::map_protocol_err(&e))
    }

    pub(crate) async fn resolve_tokens(&self) -> Result<Arc<TokenManager>, Error> {
        match &self.tokens {
            Some(t) => Ok(Arc::clone(t)),
            None => self.shared_tokens().await,
        }
    }
}

impl fmt::Debug for FinamApiConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FinamApiConfig")
            .field("endpoint", &self.endpoint.url)
            .field("secret", &"<redacted>")
            .field("shared_tokens", &self.tokens.is_some())
            .field("bearer", &self.bearer)
            .finish()
    }
}

/// Market data connector config. Not bound to an account.
///
/// Used with [`MarketDataConnector::start`](crate::MarketDataConnector::start).
#[derive(Debug, Clone)]
pub struct MarketDataConfig {
    /// Credentials and endpoint.
    pub api: FinamApiConfig,
    /// Envelope source label, e.g. `"finam-md"`.
    pub source: Arc<str>,
    /// How long `subscribe_*` waits for the stream to open before returning (still pending).
    ///
    /// A refusal that arrives within this time (unknown symbol, no permission) is returned as an error.
    /// A slow or unreachable server leaves the subscription connecting: check
    /// [`MarketDataConnector::sub_status`](crate::MarketDataConnector::sub_status). Default 3 seconds.
    pub subscribe_ack_timeout: Duration,
}

impl MarketDataConfig {
    /// Config with default settings. `source` labels every envelope the connector emits.
    pub fn new(api: FinamApiConfig, source: impl Into<Arc<str>>) -> Self {
        Self {
            api,
            source: source.into(),
            subscribe_ack_timeout: Duration::from_secs(3),
        }
    }
}

/// User data connector config: one connector = one account.
///
/// Used with [`UserDataConnector::start`](crate::UserDataConnector::start).
#[derive(Debug, Clone)]
pub struct UserDataConfig {
    /// Credentials and endpoint.
    pub api: FinamApiConfig,
    /// Account all orders, executions and portfolio belong to.
    pub account_id: String,
    /// Envelope source label, e.g. `"finam-ud-A12345"`.
    pub source: Arc<str>,
    /// Lossless buffer capacity for order-state and execution subscriptions. Default 1024.
    ///
    /// When it fills up, the subscription reports an overflow and the connector turns `Degraded`:
    /// trading commands are refused until `reconcile` succeeds. Events are never dropped silently.
    pub order_state_buffer: usize,
    /// `limit` sent with the `Trades` history request. Default 1000.
    ///
    /// `executions(range)` returns at most this many trades.
    pub executions_limit: i32,
    /// How long `subscribe_*` waits for the stream to open before returning (still pending).
    /// Same meaning as in [`MarketDataConfig`]. Default 3 seconds.
    pub subscribe_ack_timeout: Duration,
    /// Price steps the connector checks order prices against (default: empty, nothing is checked).
    /// Fill it with [`PriceGrid::learn`] from the trading parameters of the instruments you trade. A
    /// price off the step is then refused with `Rejected` (`price_off_step`) before anything is sent.
    pub price_grid: PriceGrid,
}

impl UserDataConfig {
    /// Config for one `account_id` with default settings. `source` labels every envelope.
    ///
    /// The account is checked when the connector starts: it must not be empty and, when the token
    /// lists its accounts, it must be one of them.
    pub fn new(
        api: FinamApiConfig,
        account_id: impl Into<String>,
        source: impl Into<Arc<str>>,
    ) -> Self {
        Self {
            api,
            account_id: account_id.into(),
            source: source.into(),
            order_state_buffer: 1024,
            executions_limit: 1000,
            subscribe_ack_timeout: Duration::from_secs(3),
            price_grid: PriceGrid::default(),
        }
    }

    pub(crate) fn check(&self) -> Result<(), Error> {
        if self.account_id.trim().is_empty() {
            return Err(Error::Config(ConfigError::Message(
                "empty account_id".into(),
            )));
        }
        Ok(())
    }
}

/// Reads the config from environment variables. Meant for examples and live tests.
///
/// Variables:
///
/// - `FINAM_SECRET` (required): the API secret.
/// - `FINAM_API_URL` (optional): another endpoint instead of the production one. It must start with
///   `https://`: the address of a local imitation is set in code with `FinamApiConfig::with_endpoint`.
/// - `FINAM_ACCOUNT_ID` (optional): returned as the second value, for user data connectors.
///
/// # Errors
///
/// [`FinamError::Config`] when `FINAM_SECRET` is not set, or `FINAM_API_URL` is set and is not an
/// `https://` address. Nothing is sent to the network here.
///
/// # Examples
///
/// ```
/// use brokapi_finam::api_config_from_env;
///
/// // Without FINAM_SECRET the call fails and says which variable is missing.
/// if std::env::var_os("FINAM_SECRET").is_none() {
///     let err = api_config_from_env().unwrap_err();
///     assert!(err.to_string().contains("FINAM_SECRET"));
/// }
/// ```
pub fn api_config_from_env() -> Result<(FinamApiConfig, Option<String>), FinamError> {
    let secret = std::env::var("FINAM_SECRET")
        .map_err(|_| FinamError::Config("FINAM_SECRET is not set".into()))?;
    let mut cfg = FinamApiConfig::new(secret);
    if let Ok(url) = std::env::var("FINAM_API_URL") {
        // A secret must not go over plain text because of a stray variable. A local imitation is set in
        // code with `with_endpoint`.
        if !url.trim().starts_with("https://") {
            return Err(FinamError::Config(
                "FINAM_API_URL must start with https://".into(),
            ));
        }
        cfg = cfg.with_endpoint(url.trim().to_string());
    }
    Ok((cfg, std::env::var("FINAM_ACCOUNT_ID").ok()))
}
