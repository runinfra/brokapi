//! Client configuration: the refresh token, the token kind, the timeout and the base URLs.

use std::time::Duration;

use crate::http::client::{HTTP_BASE_URL, WS_BASE_URL};

/// Configuration of the HTTP and WebSocket clients.
///
/// The one secret in it is the refresh token, which you issue in the BCS web cabinet (section "API
/// tokens" of the account). The broker issues two kinds of refresh tokens:
///
/// * a read token: market data, portfolio, orders and trades history, no trading,
/// * a write token: everything the read token does, plus placing, changing and cancelling orders.
///
/// A token belongs to one brokerage account and lives 90 days by the broker docs. The token is shown
/// once, at issue. [`BcsConfig::is_write`] must say which kind you hold, because it picks the
/// `client_id` of the token request (`trade-api-write` or `trade-api-read`).
///
/// The `Debug` output of this struct hides the refresh token (`<redacted>`), so a config can be logged.
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::config::BcsConfig;
///
/// // A read-only configuration, then a trading one.
/// let read = BcsConfig::new("refresh-token-of-a-read-token".to_string());
/// assert!(!read.is_write);
///
/// let trade = BcsConfig::new("refresh-token-of-a-write-token".to_string()).with_write(true);
/// assert!(trade.is_write);
/// assert_eq!(trade.request_timeout.as_secs(), 30);
/// ```
#[derive(Clone)]
pub struct BcsConfig {
    /// The long-lived refresh token from the BCS web cabinet.
    ///
    /// The HTTP client exchanges it for short-lived access tokens. If the broker returns a new refresh
    /// token with an access token (rotation), the client keeps the new one in memory only. It does not
    /// write it back to this struct or to disk, so a restart starts again from the token you supply.
    pub refresh_token: String,
    /// Timeout of one REST request, from sending to the end of the body.
    ///
    /// Default 30 seconds. It does not apply to WebSocket sessions. A request that times out ends with
    /// [`BcsApiError::HttpError`](crate::error::BcsApiError::HttpError) and no status. For an order
    /// placement that is an unknown outcome: ask the broker for the order before you send it again.
    pub request_timeout: Duration,
    /// `true` if [`BcsConfig::refresh_token`] is a write (trading) token, `false` for a read token.
    ///
    /// It selects `client_id = "trade-api-write"` or `"trade-api-read"` in the token request, as the
    /// broker docs describe. What the broker does when the flag and the token kind disagree is not
    /// verified against the live API. A read token cannot place orders.
    pub is_write: bool,
    /// Base URL of the REST API, without a trailing slash.
    ///
    /// Default is the production host [`HTTP_BASE_URL`]. The path of each endpoint is appended to it.
    pub http_base_url: String,
    /// Base URL of the WebSocket API, without a trailing slash.
    ///
    /// Default is the production host [`WS_BASE_URL`]. The path of each channel is appended to it.
    pub ws_base_url: String,
}

impl std::fmt::Debug for BcsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BcsConfig")
            .field("refresh_token", &"<redacted>")
            .field("request_timeout", &self.request_timeout)
            .field("is_write", &self.is_write)
            .field("http_base_url", &self.http_base_url)
            .field("ws_base_url", &self.ws_base_url)
            .finish()
    }
}

const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

impl BcsConfig {
    /// Reads the configuration from the environment (and from a `.env` file, if there is one).
    ///
    /// It reads two variables:
    ///
    /// * `BCS_REFRESH_TOKEN`: required, the refresh token.
    /// * `BCS_IS_WRITE`: optional. The exact text `true` means a write token. Any other value, or no
    ///   variable, means a read token.
    ///
    /// A `.env` file in the working directory is loaded first, without overriding variables that are
    /// already set. Parent directories are not searched (see [`BcsConfig::from_env_file`] for another
    /// path). The URLs and the timeout are the defaults (production host,
    /// 30 seconds). No network call is made.
    ///
    /// # Errors
    ///
    /// Returns [`std::env::VarError::NotPresent`] when `BCS_REFRESH_TOKEN` is not set, and
    /// [`std::env::VarError::NotUnicode`] when it is not valid Unicode. A wrong or expired token is not
    /// detected here. It shows up as [`BcsApiError::AuthError`](crate::error::BcsApiError::AuthError)
    /// at the first request.
    ///
    /// # Examples
    ///
    /// The example does not need a token: it handles both outcomes, as a program would.
    ///
    /// ```
    /// use brokapi_bcs_protocol::config::BcsConfig;
    ///
    /// match BcsConfig::from_env() {
    ///     Ok(cfg) => assert_eq!(cfg.request_timeout.as_secs(), 30),
    ///     Err(std::env::VarError::NotPresent) => { /* BCS_REFRESH_TOKEN is not set */ }
    ///     Err(other) => panic!("unexpected: {other}"),
    /// }
    /// ```
    pub fn from_env() -> Result<Self, std::env::VarError> {
        // Only `.env` of the working directory: `dotenvy::dotenv` would also look in every parent.
        dotenvy::from_path(".env").ok();
        Self::from_vars()
    }

    /// Like [`BcsConfig::from_env`], but loads the `.env` file at `path` (a file that is not there is
    /// skipped, variables that are already set are kept).
    ///
    /// # Errors
    ///
    /// The same as [`BcsConfig::from_env`].
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_bcs_protocol::config::BcsConfig;
    ///
    /// // No such file: only the environment is read.
    /// let _ = BcsConfig::from_env_file("/no/such/.env");
    /// ```
    pub fn from_env_file(path: impl AsRef<std::path::Path>) -> Result<Self, std::env::VarError> {
        dotenvy::from_path(path.as_ref()).ok();
        Self::from_vars()
    }

    fn from_vars() -> Result<Self, std::env::VarError> {
        Ok(Self {
            refresh_token: std::env::var("BCS_REFRESH_TOKEN")?,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            is_write: std::env::var("BCS_IS_WRITE")
                .map(|v| v == "true")
                .unwrap_or(false),
            http_base_url: HTTP_BASE_URL.to_string(),
            ws_base_url: WS_BASE_URL.to_string(),
        })
    }

    /// Builds a read-only configuration for the production host from a refresh token.
    ///
    /// Chain [`BcsConfig::with_write`] if the token is a write token.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_bcs_protocol::config::BcsConfig;
    ///
    /// let cfg = BcsConfig::new("my-refresh-token".to_string());
    /// assert_eq!(cfg.refresh_token, "my-refresh-token");
    /// assert!(!cfg.is_write);
    /// assert!(cfg.http_base_url.starts_with("https://"));
    /// assert!(cfg.ws_base_url.starts_with("wss://"));
    /// ```
    pub fn new(refresh_token: String) -> Self {
        Self {
            refresh_token,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            is_write: false,
            http_base_url: HTTP_BASE_URL.to_string(),
            ws_base_url: WS_BASE_URL.to_string(),
        }
    }

    /// Points both the REST and the WebSocket client at a local server, for tests and examples.
    ///
    /// `http_base_url` is the base of the local server (for example `http://127.0.0.1:34567`). The
    /// WebSocket base is derived from it by swapping the scheme (`http` to `ws`, `https` to `wss`).
    /// The token is only a label for the imitation, it is never sent to the real broker. The result is
    /// a read configuration, add [`BcsConfig::with_write`] for the trading calls.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_bcs_protocol::config::BcsConfig;
    ///
    /// let cfg = BcsConfig::for_mock("mock-refresh-token", "http://127.0.0.1:9000").with_write(true);
    /// assert_eq!(cfg.http_base_url, "http://127.0.0.1:9000");
    /// assert_eq!(cfg.ws_base_url, "ws://127.0.0.1:9000");
    /// assert!(cfg.is_write);
    /// ```
    pub fn for_mock(refresh_token: impl Into<String>, http_base_url: impl Into<String>) -> Self {
        let http = http_base_url.into();
        let ws = http
            .replace("https://", "wss://")
            .replace("http://", "ws://");
        Self {
            refresh_token: refresh_token.into(),
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            is_write: false,
            http_base_url: http,
            ws_base_url: ws,
        }
    }

    /// Sets whether the refresh token is a write (trading) token. Returns the changed config.
    ///
    /// Pass `true` only when the token really is a write token. See [`BcsConfig::is_write`].
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_bcs_protocol::config::BcsConfig;
    ///
    /// let cfg = BcsConfig::new("t".to_string()).with_write(true);
    /// assert!(cfg.is_write);
    /// ```
    pub fn with_write(mut self, is_write: bool) -> Self {
        self.is_write = is_write;
        self
    }
}
