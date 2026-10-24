//! Client configuration: the key pair, the contour, the receive window and the base URLs.
//!
//! Bybit has three contours with their own hosts. The main one is the default. The demo contour
//! (`api-demo.bybit.com`) is a separate account with virtual money, open round the clock: it needs a key made in the
//! demo mode of the site, and it serves only the private streams on its own WebSocket host (public data comes from the
//! main host). The test contour is the exchange's testnet.

use std::time::Duration;

/// REST host of the main contour.
pub const MAINNET_HTTP_BASE_URL: &str = "https://api.bybit.com";
/// WebSocket host of the main contour (public and private streams).
pub const MAINNET_WS_BASE_URL: &str = "wss://stream.bybit.com";
/// REST host of the demo contour.
pub const DEMO_HTTP_BASE_URL: &str = "https://api-demo.bybit.com";
/// WebSocket host of the private streams of the demo contour.
pub const DEMO_WS_PRIVATE_BASE_URL: &str = "wss://stream-demo.bybit.com";
/// REST host of the test contour.
pub const TESTNET_HTTP_BASE_URL: &str = "https://api-testnet.bybit.com";
/// WebSocket host of the test contour.
pub const TESTNET_WS_BASE_URL: &str = "wss://stream-testnet.bybit.com";

/// The contour of the exchange the client talks to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BybitEnv {
    /// The real exchange. Orders spend real money.
    Mainnet,
    /// The demo account: real prices, virtual money. Needs a key made in the demo mode of the site.
    Demo,
    /// The testnet of the exchange (`testnet.bybit.com`).
    Testnet,
}

impl BybitEnv {
    /// `"mainnet"`, `"demo"` or `"testnet"`.
    pub const fn as_str(self) -> &'static str {
        match self {
            BybitEnv::Mainnet => "mainnet",
            BybitEnv::Demo => "demo",
            BybitEnv::Testnet => "testnet",
        }
    }

    /// Reads the name written by [`BybitEnv::as_str`] (case does not matter). `main` is taken for `mainnet`.
    pub fn parse(name: &str) -> Option<Self> {
        match name.trim().to_ascii_lowercase().as_str() {
            "mainnet" | "main" | "live" => Some(BybitEnv::Mainnet),
            "demo" => Some(BybitEnv::Demo),
            "testnet" | "test" => Some(BybitEnv::Testnet),
            _ => None,
        }
    }
}

/// The product category of the instruments. The category is part of every instrument id.
///
/// Only `linear` (USDT perpetuals and USDT futures) is implemented. The type is open for `spot`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Category {
    /// USDT and USDC perpetuals and futures.
    Linear,
}

impl Category {
    /// The word of the API: `"linear"`.
    pub const fn as_str(self) -> &'static str {
        match self {
            Category::Linear => "linear",
        }
    }

    /// Reads the word of the API.
    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "linear" => Some(Category::Linear),
            _ => None,
        }
    }
}

/// How long a request is valid after its timestamp, in milliseconds (the exchange default).
pub const DEFAULT_RECV_WINDOW_MS: u64 = 5_000;
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Settings of the client: the key pair, the contour and the addresses.
///
/// The secret never appears in `Debug` output.
#[derive(Clone)]
pub struct BybitConfig {
    /// The API key (public half).
    pub api_key: String,
    /// The API secret (signs requests).
    pub api_secret: String,
    /// The contour.
    pub env: BybitEnv,
    /// REST base URL, without a trailing slash.
    pub http_base_url: String,
    /// Base URL of the public streams (the path `/v5/public/<category>` is added).
    pub ws_public_base_url: String,
    /// Base URL of the private streams (the path `/v5/private` is added).
    pub ws_private_base_url: String,
    /// Timeout of one HTTP request.
    pub request_timeout: Duration,
    /// The receive window sent with every private request, in milliseconds.
    pub recv_window_ms: u64,
    /// The product category.
    pub category: Category,
    /// The settle coin used to list orders and positions of a whole category (`USDT` for linear).
    pub settle_coin: String,
}

impl std::fmt::Debug for BybitConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BybitConfig")
            .field("api_key", &self.api_key)
            .field("api_secret", &"<redacted>")
            .field("env", &self.env)
            .field("http_base_url", &self.http_base_url)
            .field("ws_public_base_url", &self.ws_public_base_url)
            .field("ws_private_base_url", &self.ws_private_base_url)
            .field("request_timeout", &self.request_timeout)
            .field("recv_window_ms", &self.recv_window_ms)
            .field("category", &self.category)
            .field("settle_coin", &self.settle_coin)
            .finish()
    }
}

impl BybitConfig {
    /// A config for the main contour.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_bybit_protocol::config::{BybitConfig, BybitEnv};
    ///
    /// let cfg = BybitConfig::new("key", "secret");
    /// assert_eq!(cfg.env, BybitEnv::Mainnet);
    /// assert_eq!(cfg.http_base_url, "https://api.bybit.com");
    /// assert_eq!(format!("{cfg:?}").contains("secret\""), false, "the secret is never printed");
    /// ```
    pub fn new(api_key: impl Into<String>, api_secret: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            api_secret: api_secret.into(),
            env: BybitEnv::Mainnet,
            http_base_url: MAINNET_HTTP_BASE_URL.to_string(),
            ws_public_base_url: MAINNET_WS_BASE_URL.to_string(),
            ws_private_base_url: MAINNET_WS_BASE_URL.to_string(),
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            recv_window_ms: DEFAULT_RECV_WINDOW_MS,
            category: Category::Linear,
            settle_coin: "USDT".to_string(),
        }
    }

    /// Reads the key pair and the contour from the environment (after loading `.env` from the current directory).
    ///
    /// `BYBIT_ENV` is `mainnet` (default), `demo` or `testnet`. The key pair is `BYBIT_API_KEY` and `BYBIT_API_SECRET`,
    /// and for the demo contour `BYBIT_DEMO_API_KEY` and `BYBIT_DEMO_API_SECRET`: a demo key is not valid on the main
    /// host, so the two pairs can live in one `.env`.
    ///
    /// # Errors
    ///
    /// The name of the missing variable. An unknown `BYBIT_ENV` is an error too.
    pub fn from_env() -> Result<Self, String> {
        dotenvy::from_path(".env").ok();
        Self::from_vars()
    }

    /// Like [`BybitConfig::from_env`], with the `.env` file at `path`.
    pub fn from_env_file(path: impl AsRef<std::path::Path>) -> Result<Self, String> {
        dotenvy::from_path(path.as_ref()).ok();
        Self::from_vars()
    }

    fn from_vars() -> Result<Self, String> {
        let env = match std::env::var("BYBIT_ENV").ok().filter(|v| !v.is_empty()) {
            None => BybitEnv::Mainnet,
            Some(name) => BybitEnv::parse(&name)
                .ok_or_else(|| format!("BYBIT_ENV={name:?}: use mainnet, demo or testnet"))?,
        };
        let (key_var, secret_var) = if env == BybitEnv::Demo {
            ("BYBIT_DEMO_API_KEY", "BYBIT_DEMO_API_SECRET")
        } else {
            ("BYBIT_API_KEY", "BYBIT_API_SECRET")
        };
        let get = |name: &str| {
            std::env::var(name)
                .ok()
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
                .ok_or_else(|| format!("{name} is not set"))
        };
        Ok(Self::new(get(key_var)?, get(secret_var)?).with_env(env))
    }

    /// Points the config at a contour: the hosts of the contour replace the current ones.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_bybit_protocol::config::{BybitConfig, BybitEnv};
    ///
    /// let cfg = BybitConfig::new("key", "secret").with_env(BybitEnv::Demo);
    /// assert_eq!(cfg.http_base_url, "https://api-demo.bybit.com");
    /// // The demo host has no public streams: they come from the main host.
    /// assert_eq!(cfg.ws_public_base_url, "wss://stream.bybit.com");
    /// assert_eq!(cfg.ws_private_base_url, "wss://stream-demo.bybit.com");
    /// ```
    pub fn with_env(mut self, env: BybitEnv) -> Self {
        let (http, ws_public, ws_private) = match env {
            BybitEnv::Mainnet => (
                MAINNET_HTTP_BASE_URL,
                MAINNET_WS_BASE_URL,
                MAINNET_WS_BASE_URL,
            ),
            BybitEnv::Demo => (
                DEMO_HTTP_BASE_URL,
                MAINNET_WS_BASE_URL,
                DEMO_WS_PRIVATE_BASE_URL,
            ),
            BybitEnv::Testnet => (
                TESTNET_HTTP_BASE_URL,
                TESTNET_WS_BASE_URL,
                TESTNET_WS_BASE_URL,
            ),
        };
        self.env = env;
        self.http_base_url = http.to_string();
        self.ws_public_base_url = ws_public.to_string();
        self.ws_private_base_url = ws_private.to_string();
        self
    }

    /// Sets the receive window in milliseconds.
    pub fn with_recv_window_ms(mut self, ms: u64) -> Self {
        self.recv_window_ms = ms;
        self
    }

    /// A config for the in-process imitation: every host is `http_base_url` (the streams use `ws://`).
    ///
    /// The config is marked as demo, so a connector built from it reports a virtual account.
    pub fn for_mock(
        api_key: impl Into<String>,
        api_secret: impl Into<String>,
        http_base_url: impl Into<String>,
    ) -> Self {
        let http = http_base_url.into();
        let ws = http
            .replace("https://", "wss://")
            .replace("http://", "ws://");
        let mut cfg = Self::new(api_key, api_secret);
        cfg.env = BybitEnv::Demo;
        cfg.http_base_url = http;
        cfg.ws_public_base_url = ws.clone();
        cfg.ws_private_base_url = ws;
        cfg
    }

    /// The URL of the public stream of the category.
    pub fn ws_public_url(&self) -> String {
        format!(
            "{}/v5/public/{}",
            self.ws_public_base_url.trim_end_matches('/'),
            self.category.as_str()
        )
    }

    /// The URL of the private stream.
    pub fn ws_private_url(&self) -> String {
        format!(
            "{}/v5/private",
            self.ws_private_base_url.trim_end_matches('/')
        )
    }

    /// Whether the account is virtual (the demo contour or the imitation).
    pub fn is_demo(&self) -> bool {
        self.env == BybitEnv::Demo
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_demo_contour_takes_public_data_from_the_main_host() {
        let cfg = BybitConfig::new("k", "s").with_env(BybitEnv::Demo);
        assert_eq!(
            cfg.ws_public_url(),
            "wss://stream.bybit.com/v5/public/linear"
        );
        assert_eq!(
            cfg.ws_private_url(),
            "wss://stream-demo.bybit.com/v5/private"
        );
        assert!(cfg.is_demo());
        let main = BybitConfig::new("k", "s");
        assert_eq!(main.ws_private_url(), "wss://stream.bybit.com/v5/private");
        assert!(!main.is_demo());
    }

    #[test]
    fn contour_names_round_trip() {
        for env in [BybitEnv::Mainnet, BybitEnv::Demo, BybitEnv::Testnet] {
            assert_eq!(BybitEnv::parse(env.as_str()), Some(env));
        }
        assert_eq!(BybitEnv::parse("DEMO"), Some(BybitEnv::Demo));
        assert_eq!(BybitEnv::parse("nope"), None);
        assert_eq!(Category::parse("linear"), Some(Category::Linear));
        assert_eq!(Category::parse("spot"), None);
    }

    #[test]
    fn the_secret_is_redacted() {
        let cfg = BybitConfig::new("the-key", "very-secret-value");
        let text = format!("{cfg:?}");
        assert!(text.contains("the-key"));
        assert!(!text.contains("very-secret-value"));
    }
}
