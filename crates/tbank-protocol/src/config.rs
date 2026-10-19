//! Client configuration: endpoint, token, account.
//!
//! # Examples
//!
//! ```
//! use std::time::Duration;
//! use brokapi_tbank_protocol::{TbankConfig, SANDBOX_ENDPOINT};
//!
//! let cfg = TbankConfig::sandbox("t.my-token")
//!     .with_account("2000123456")
//!     .with_app_name("me.my-robot")
//!     .with_request_timeout(Duration::from_secs(10));
//! assert_eq!(cfg.endpoint, SANDBOX_ENDPOINT);
//! assert!(cfg.sandbox);
//! // The token never shows in `Debug` output.
//! assert!(!format!("{cfg:?}").contains("t.my-token"));
//! ```

use std::fmt;
use std::time::Duration;

/// Production gRPC endpoint.
pub const PROD_ENDPOINT: &str = "https://invest-public-api.tbank.ru:443";
/// Sandbox gRPC endpoint (same services as production, virtual accounts).
pub const SANDBOX_ENDPOINT: &str = "https://sandbox-invest-public-api.tbank.ru:443";

const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// gRPC client configuration. The token is never printed by `Debug`.
///
/// One config describes one token and one endpoint. Several accounts of the same token share the
/// token and differ in [`account_id`](Self::account_id): build one config per account with
/// [`with_account`](Self::with_account).
///
/// # Examples
///
/// ```
/// use brokapi_tbank_protocol::{TbankConfig, PROD_ENDPOINT};
///
/// let prod = TbankConfig::production("t.my-token");
/// assert_eq!(prod.endpoint, PROD_ENDPOINT);
/// assert!(!prod.sandbox);
///
/// // Two accounts of one token: same token, different `account_id`.
/// let main = prod.clone().with_account("acc-main");
/// let second = prod.with_account("acc-second");
/// assert_eq!(main.token, second.token);
/// assert_ne!(main.account_id, second.account_id);
/// ```
#[derive(Clone)]
pub struct TbankConfig {
    /// API token, issued in the T-Bank Invest settings (a sandbox token for the sandbox endpoint).
    ///
    /// It is sent in the `authorization: Bearer` header of every request. Treat it as a secret:
    /// keep it out of logs and source control. `Debug` prints `<redacted>` instead of it.
    pub token: String,
    /// Full URL, `https://host:443` (TLS) or `http://host:port` (plain, for mocks).
    pub endpoint: String,
    /// Sandbox flag. Selects the sandbox endpoint in `new`, and tells the connector the account is virtual.
    pub sandbox: bool,
    /// Brokerage account id. When `None` the connector picks the first open account of the token.
    pub account_id: Option<String>,
    /// `x-app-name` header (`<github nick>.<repo>` is the T-Bank convention).
    pub app_name: Option<String>,
    /// Deadline of one unary call. Default 30 s.
    ///
    /// [`TbankClient`](crate::TbankClient) does not apply it. The connectors in `brokapi-tbank` cut
    /// every call at this deadline. For a command (place, cancel, replace) a timeout means the outcome
    /// is unknown, because the broker may have acted. See the connector docs.
    pub request_timeout: Duration,
    /// How long the channel waits for the TCP and TLS connection to be set up. Default 10 s.
    ///
    /// It applies to every (re)connect of the lazy channel. There is no builder method, set the field.
    pub connect_timeout: Duration,
}

impl TbankConfig {
    /// Production or sandbox endpoint chosen by `sandbox`.
    ///
    /// Timeouts take their defaults (30 s per request, 10 s to connect). No account is chosen and no
    /// app name is set.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_tbank_protocol::{TbankConfig, PROD_ENDPOINT, SANDBOX_ENDPOINT};
    ///
    /// assert_eq!(TbankConfig::new("t.token", true).endpoint, SANDBOX_ENDPOINT);
    /// assert_eq!(TbankConfig::new("t.token", false).endpoint, PROD_ENDPOINT);
    /// ```
    pub fn new(token: impl Into<String>, sandbox: bool) -> Self {
        Self {
            token: token.into(),
            endpoint: if sandbox {
                SANDBOX_ENDPOINT
            } else {
                PROD_ENDPOINT
            }
            .to_string(),
            sandbox,
            account_id: None,
            app_name: None,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
        }
    }

    /// Sandbox endpoint ([`SANDBOX_ENDPOINT`]). Accounts there are virtual. Use a sandbox token.
    ///
    /// # Examples
    ///
    /// ```
    /// let cfg = brokapi_tbank_protocol::TbankConfig::sandbox("t.sandbox-token");
    /// assert!(cfg.sandbox);
    /// ```
    pub fn sandbox(token: impl Into<String>) -> Self {
        Self::new(token, true)
    }

    /// Production endpoint ([`PROD_ENDPOINT`]). Orders placed through it are real and cost money.
    ///
    /// # Examples
    ///
    /// ```
    /// let cfg = brokapi_tbank_protocol::TbankConfig::production("t.token");
    /// assert!(!cfg.sandbox);
    /// ```
    pub fn production(token: impl Into<String>) -> Self {
        Self::new(token, false)
    }

    /// Plain-HTTP endpoint of a local mock server (tests and examples).
    ///
    /// `endpoint` is a full URL such as `http://127.0.0.1:50051`. The config is marked as sandbox, so
    /// a connector built from it reports `sandbox: true` in its capabilities. Do not use it with a
    /// real token.
    ///
    /// # Examples
    ///
    /// ```
    /// # use brokapi_mocks::tbank::{MockServer, TOKEN};
    /// use brokapi_tbank_protocol::TbankConfig;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// # let server = MockServer::start(Default::default()).await;
    /// let cfg = TbankConfig::for_mock(TOKEN, &server.endpoint);
    /// assert!(cfg.endpoint.starts_with("http://"));
    /// assert!(cfg.sandbox);
    /// # }
    /// ```
    pub fn for_mock(token: impl Into<String>, endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            ..Self::new(token, true)
        }
    }

    /// Reads the config from the environment. Loads a `.env` file first, if there is one.
    ///
    /// Variables: `TBANK_TOKEN` (required), `TBANK_SANDBOX` (`true`, `1` or `yes`, default `false`, so
    /// production), `TBANK_ENDPOINT` (overrides the endpoint when not empty), `TBANK_ACCOUNT_ID` and
    /// `TBANK_APP_NAME` (both optional, empty means not set).
    ///
    /// Note the default: without `TBANK_SANDBOX` the config points at production.
    ///
    /// # Errors
    ///
    /// [`std::env::VarError`] when `TBANK_TOKEN` is not set or is not valid unicode.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_tbank_protocol::{TbankConfig, SANDBOX_ENDPOINT};
    ///
    /// # // Doctests run in their own process, so setting the environment here is contained.
    /// # unsafe {
    /// #     std::env::set_var("TBANK_TOKEN", "t.my-token");
    /// #     std::env::set_var("TBANK_SANDBOX", "true");
    /// #     std::env::set_var("TBANK_ACCOUNT_ID", "acc-main");
    /// #     std::env::remove_var("TBANK_ENDPOINT");
    /// #     std::env::remove_var("TBANK_APP_NAME");
    /// # }
    /// let cfg = TbankConfig::from_env()?;
    /// assert_eq!(cfg.endpoint, SANDBOX_ENDPOINT);
    /// assert_eq!(cfg.account_id.as_deref(), Some("acc-main"));
    ///
    /// # unsafe { std::env::remove_var("TBANK_TOKEN") };
    /// // Without a token the call fails, it never falls back to a default.
    /// assert!(TbankConfig::from_env().is_err());
    /// # Ok::<(), std::env::VarError>(())
    /// ```
    pub fn from_env() -> Result<Self, std::env::VarError> {
        dotenv::dotenv().ok();
        let token = std::env::var("TBANK_TOKEN")?;
        let sandbox = std::env::var("TBANK_SANDBOX")
            .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "true" | "1" | "yes"))
            .unwrap_or(false);
        let mut cfg = Self::new(token, sandbox);
        if let Ok(endpoint) = std::env::var("TBANK_ENDPOINT")
            && !endpoint.trim().is_empty()
        {
            cfg.endpoint = endpoint;
        }
        cfg.account_id = std::env::var("TBANK_ACCOUNT_ID")
            .ok()
            .filter(|s| !s.trim().is_empty());
        cfg.app_name = std::env::var("TBANK_APP_NAME")
            .ok()
            .filter(|s| !s.trim().is_empty());
        Ok(cfg)
    }

    /// Pins the brokerage account this config works with.
    ///
    /// A user data connector trades on exactly this account. Without it the connector takes the
    /// token's only open account and refuses to start when there are several. Use
    /// [`TbankClient::list_accounts`](crate::TbankClient::list_accounts) to see the ids.
    ///
    /// # Examples
    ///
    /// ```
    /// let cfg = brokapi_tbank_protocol::TbankConfig::sandbox("t.token").with_account("acc-main");
    /// assert_eq!(cfg.account_id.as_deref(), Some("acc-main"));
    /// ```
    pub fn with_account(mut self, account_id: impl Into<String>) -> Self {
        self.account_id = Some(account_id.into());
        self
    }

    /// Sets the `x-app-name` header sent with every request.
    ///
    /// The T-Bank convention is `<github nick>.<repo>`. The name must be a valid header value,
    /// otherwise [`TbankClient::new`](crate::TbankClient::new) returns
    /// [`ProtocolError::Config`](crate::ProtocolError::Config).
    ///
    /// # Examples
    ///
    /// ```
    /// let cfg = brokapi_tbank_protocol::TbankConfig::sandbox("t.token").with_app_name("me.my-robot");
    /// assert_eq!(cfg.app_name.as_deref(), Some("me.my-robot"));
    /// ```
    pub fn with_app_name(mut self, app_name: impl Into<String>) -> Self {
        self.app_name = Some(app_name.into());
        self
    }

    /// Sets the deadline of one unary call, see [`request_timeout`](Self::request_timeout).
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    ///
    /// let cfg = brokapi_tbank_protocol::TbankConfig::sandbox("t.token").with_request_timeout(Duration::from_secs(5));
    /// assert_eq!(cfg.request_timeout, Duration::from_secs(5));
    /// ```
    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }
}

impl fmt::Debug for TbankConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TbankConfig")
            .field("token", &"<redacted>")
            .field("endpoint", &self.endpoint)
            .field("sandbox", &self.sandbox)
            .field("account_id", &self.account_id)
            .field("app_name", &self.app_name)
            .field("request_timeout", &self.request_timeout)
            .finish()
    }
}
