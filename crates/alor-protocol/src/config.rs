//! Client configuration: the refresh token, the contour, the timeout and the base URLs.

use std::time::Duration;

/// The production contour: real exchanges, real money. Orders go to the exchange.
pub const PROD_HTTP_BASE_URL: &str = "https://api.alor.ru";
/// The authorization server of the production contour (the access token is issued here).
pub const PROD_AUTH_BASE_URL: &str = "https://oauth.alor.ru";
/// The WebSocket host of the production contour.
pub const PROD_WS_BASE_URL: &str = "wss://api.alor.ru";
/// The test contour: simulated data and a test account, no effect on the market.
pub const TEST_HTTP_BASE_URL: &str = "https://apidev.alor.ru";
/// The authorization server of the test contour.
pub const TEST_AUTH_BASE_URL: &str = "https://oauthdev.alor.ru";
/// The WebSocket host of the test contour.
pub const TEST_WS_BASE_URL: &str = "wss://apidev.alor.ru";

/// Which contour of the broker a client talks to. The two are isolated: a token of one is useless in the other.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlorEnv {
    /// The production contour (real money).
    Production,
    /// The test contour (a test account on request from the broker, data is simulated).
    Test,
}

/// Configuration of the HTTP and WebSocket clients.
///
/// The one secret in it is the refresh token, which you issue on the developer portal after binding a trade
/// account (it lives a year in the production contour and a month in the test one). The broker has no
/// separate read and trade tokens: the access token made from it reaches everything the account may do.
///
/// The `Debug` output hides the refresh token (`<redacted>`), so a config can be logged.
///
/// # Examples
///
/// ```
/// use brokapi_alor_protocol::config::{AlorConfig, AlorEnv};
///
/// let cfg = AlorConfig::new("my-refresh-token".to_string()).with_portfolio("D12345");
/// assert_eq!(cfg.env, AlorEnv::Production);
/// assert_eq!(cfg.portfolio.as_deref(), Some("D12345"));
/// assert_eq!(cfg.exchange, "MOEX");
/// assert_eq!(cfg.request_timeout.as_secs(), 30);
///
/// // The test contour switches all three addresses at once.
/// let test = cfg.with_env(AlorEnv::Test);
/// assert_eq!(test.http_base_url, "https://apidev.alor.ru");
/// assert_eq!(test.auth_base_url, "https://oauthdev.alor.ru");
/// ```
#[derive(Clone)]
pub struct AlorConfig {
    /// The long-lived refresh token from the developer portal.
    pub refresh_token: String,
    /// Timeout of one REST request, from sending to the end of the body (default 30 seconds). A request
    /// that times out ends with [`AlorApiError::Http`](crate::error::AlorApiError::Http) and no status.
    /// For an order command that is an unknown outcome: ask the broker before you send it again.
    pub request_timeout: Duration,
    /// The contour. Set by [`AlorConfig::with_env`], which also sets the three addresses below.
    pub env: AlorEnv,
    /// Base URL of the REST API, without a trailing slash.
    pub http_base_url: String,
    /// Base URL of the authorization server, without a trailing slash.
    pub auth_base_url: String,
    /// Base URL of the WebSocket API, without a trailing slash. The channels are `/ws` and `/cws`.
    pub ws_base_url: String,
    /// The portfolio the user data connector works with (for example `D12345`). If it is empty, the connector
    /// takes the only portfolio named in the access token, and refuses to start when the token names several.
    pub portfolio: Option<String>,
    /// The exchange of the portfolio: `MOEX` (default) or `SPBX`. Orders, positions and trades are read per
    /// exchange, so one connector works with one.
    pub exchange: String,
}

impl std::fmt::Debug for AlorConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlorConfig")
            .field("refresh_token", &"<redacted>")
            .field("request_timeout", &self.request_timeout)
            .field("env", &self.env)
            .field("http_base_url", &self.http_base_url)
            .field("auth_base_url", &self.auth_base_url)
            .field("ws_base_url", &self.ws_base_url)
            .field("portfolio", &self.portfolio)
            .field("exchange", &self.exchange)
            .finish()
    }
}

const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

impl AlorConfig {
    /// A configuration for the production contour from a refresh token.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_alor_protocol::config::AlorConfig;
    ///
    /// let cfg = AlorConfig::new("t".to_string());
    /// assert!(cfg.http_base_url.starts_with("https://"));
    /// assert!(cfg.ws_base_url.starts_with("wss://"));
    /// ```
    pub fn new(refresh_token: String) -> Self {
        Self {
            refresh_token,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            env: AlorEnv::Production,
            http_base_url: PROD_HTTP_BASE_URL.to_string(),
            auth_base_url: PROD_AUTH_BASE_URL.to_string(),
            ws_base_url: PROD_WS_BASE_URL.to_string(),
            portfolio: None,
            exchange: "MOEX".to_string(),
        }
    }

    /// Reads the configuration from the environment (and from a `.env` file of the working directory, if
    /// there is one; variables that are already set win).
    ///
    /// * `ALOR_REFRESH_TOKEN`: required.
    /// * `ALOR_PORTFOLIO`: optional, see [`AlorConfig::portfolio`].
    /// * `ALOR_EXCHANGE`: optional, `MOEX` (default) or `SPBX`.
    /// * `ALOR_ENV`: optional, the exact text `test` selects the test contour.
    ///
    /// No network call is made.
    ///
    /// # Errors
    ///
    /// [`std::env::VarError::NotPresent`] when `ALOR_REFRESH_TOKEN` is not set. A wrong or expired token shows
    /// up as [`AlorApiError::Auth`](crate::error::AlorApiError::Auth) at the first request.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_alor_protocol::config::AlorConfig;
    ///
    /// match AlorConfig::from_env() {
    ///     Ok(cfg) => assert_eq!(cfg.request_timeout.as_secs(), 30),
    ///     Err(std::env::VarError::NotPresent) => { /* ALOR_REFRESH_TOKEN is not set */ }
    ///     Err(other) => panic!("unexpected: {other}"),
    /// }
    /// ```
    pub fn from_env() -> Result<Self, std::env::VarError> {
        dotenvy::from_path(".env").ok();
        Self::from_vars()
    }

    /// Like [`AlorConfig::from_env`], but loads the `.env` file at `path` (a file that is not there is skipped).
    ///
    /// # Errors
    ///
    /// The same as [`AlorConfig::from_env`].
    pub fn from_env_file(path: impl AsRef<std::path::Path>) -> Result<Self, std::env::VarError> {
        dotenvy::from_path(path.as_ref()).ok();
        Self::from_vars()
    }

    fn from_vars() -> Result<Self, std::env::VarError> {
        let mut cfg = Self::new(std::env::var("ALOR_REFRESH_TOKEN")?);
        cfg.portfolio = std::env::var("ALOR_PORTFOLIO")
            .ok()
            .filter(|p| !p.is_empty());
        if let Ok(ex) = std::env::var("ALOR_EXCHANGE")
            && !ex.is_empty()
        {
            cfg.exchange = ex;
        }
        if std::env::var("ALOR_ENV").is_ok_and(|v| v == "test") {
            cfg = cfg.with_env(AlorEnv::Test);
        }
        Ok(cfg)
    }

    /// Points all three addresses at the production or the test contour. Returns the changed config.
    pub fn with_env(mut self, env: AlorEnv) -> Self {
        let (http, auth, ws) = match env {
            AlorEnv::Production => (PROD_HTTP_BASE_URL, PROD_AUTH_BASE_URL, PROD_WS_BASE_URL),
            AlorEnv::Test => (TEST_HTTP_BASE_URL, TEST_AUTH_BASE_URL, TEST_WS_BASE_URL),
        };
        self.env = env;
        self.http_base_url = http.to_string();
        self.auth_base_url = auth.to_string();
        self.ws_base_url = ws.to_string();
        self
    }

    /// Sets the portfolio (see [`AlorConfig::portfolio`]). Returns the changed config.
    pub fn with_portfolio(mut self, portfolio: impl Into<String>) -> Self {
        self.portfolio = Some(portfolio.into());
        self
    }

    /// Sets the exchange, `MOEX` or `SPBX` (see [`AlorConfig::exchange`]). Returns the changed config.
    pub fn with_exchange(mut self, exchange: impl Into<String>) -> Self {
        self.exchange = exchange.into();
        self
    }

    /// Points the REST, authorization and WebSocket clients at one local server, for tests and examples.
    ///
    /// `http_base_url` is the base of the local server (for example `http://127.0.0.1:34567`). The
    /// authorization server is the same host, and the WebSocket base is derived by swapping the scheme. The token
    /// is only a label for the imitation, it is never sent to the real broker. The portfolio is `D00001`.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_alor_protocol::config::AlorConfig;
    ///
    /// let cfg = AlorConfig::for_mock("mock-refresh-token", "http://127.0.0.1:9000");
    /// assert_eq!(cfg.auth_base_url, "http://127.0.0.1:9000");
    /// assert_eq!(cfg.ws_base_url, "ws://127.0.0.1:9000");
    /// ```
    pub fn for_mock(refresh_token: impl Into<String>, http_base_url: impl Into<String>) -> Self {
        let http = http_base_url.into();
        let ws = http
            .replace("https://", "wss://")
            .replace("http://", "ws://");
        let mut cfg = Self::new(refresh_token.into());
        cfg.auth_base_url = http.clone();
        cfg.http_base_url = http;
        cfg.ws_base_url = ws;
        cfg.portfolio = Some("D00001".to_string());
        cfg
    }
}
