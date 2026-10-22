//! Configuration of the two connectors: the ALOR client settings plus the choices of the connector itself
//! (source label, reconnect pauses, buffer size).
//!
//! Both configs wrap an `AlorConfig` from the protocol crate. It holds the refresh token, the contour, the
//! timeouts, the portfolio and the exchange. Build it with `AlorConfig::from_env()` for a real account, or with
//! `AlorConfig::for_mock(token, url)` for tests against `brokapi-mocks`.

use std::sync::Arc;
use std::time::Duration;

use brokapi_alor_protocol::config::AlorConfig;
use brokapi_alor_protocol::ws::session::{DEFAULT_RECONNECT_MAX, DEFAULT_RECONNECT_MIN, WsOptions};
use brokapi_runtime::PriceGrid;

/// Settings of [`MarketDataConnector`](crate::MarketDataConnector).
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use brokapi_alor_protocol::config::AlorConfig;
/// use brokapi_alor::MarketDataConfig;
///
/// let mut cfg = MarketDataConfig::new(AlorConfig::new("refresh-token".into()), "alor-md");
/// assert_eq!(&*cfg.source, "alor-md");
/// // Wait at most 10 seconds between reconnects instead of the default 30.
/// cfg.reconnect_max = Duration::from_secs(10);
/// ```
#[derive(Debug, Clone)]
pub struct MarketDataConfig {
    /// Client settings of the ALOR API: refresh token, contour, timeouts, endpoints.
    pub alor: AlorConfig,
    /// Envelope source label, e.g. `"alor-md"`.
    pub source: Arc<str>,
    /// Pause before the first reconnect of the WebSocket, doubling up to `reconnect_max` (default 0.5 s, as at
    /// the other brokers).
    pub reconnect_min: Duration,
    /// Upper bound of the pause (default 30 s). The pause returns to `reconnect_min` once a connection has
    /// lived 10 seconds.
    pub reconnect_max: Duration,
    /// How long a `subscribe_*` call waits for the broker to confirm the subscription (default 3 s). A refusal
    /// inside that time is returned as an error. If nothing comes in time, the call returns and the subscription
    /// goes on opening in the background.
    pub subscribe_ack_timeout: Duration,
}

impl MarketDataConfig {
    /// Creates a config with the default reconnect pauses (0.5 s up to 30 s).
    ///
    /// `source` is the label put in every [`Envelope`](brokapi_core::Envelope) this connector emits, so a bot
    /// with several connectors can tell them apart.
    pub fn new(alor: AlorConfig, source: impl Into<Arc<str>>) -> Self {
        Self {
            alor,
            source: source.into(),
            reconnect_min: DEFAULT_RECONNECT_MIN,
            reconnect_max: DEFAULT_RECONNECT_MAX,
            subscribe_ack_timeout: Duration::from_secs(3),
        }
    }

    pub(crate) fn ws_options(&self) -> WsOptions {
        // The core is told of every connect: a reconnect voids the order books (`Resync`).
        WsOptions {
            announce_connects: true,
            reconnect_min: self.reconnect_min,
            reconnect_max: self.reconnect_max,
        }
    }
}

/// Settings of [`UserDataConnector`](crate::UserDataConnector).
///
/// The portfolio and the exchange come from the `alor` config: see
/// [`AlorConfig::portfolio`](brokapi_alor_protocol::config::AlorConfig::portfolio).
///
/// # Examples
///
/// ```
/// use brokapi_alor_protocol::config::AlorConfig;
/// use brokapi_alor::UserDataConfig;
///
/// let alor = AlorConfig::new("refresh-token".into()).with_portfolio("D12345");
/// let mut cfg = UserDataConfig::new(alor, "alor-ud");
/// assert_eq!(cfg.order_state_buffer, 1024);
/// // A bigger buffer gives a slow bot more room before an overflow closes the trading gate.
/// cfg.order_state_buffer = 4096;
/// ```
#[derive(Debug, Clone)]
pub struct UserDataConfig {
    /// Client settings of the ALOR API: refresh token, contour, timeouts, endpoints, portfolio, exchange.
    pub alor: AlorConfig,
    /// Envelope source label, e.g. `"alor-ud"`.
    pub source: Arc<str>,
    /// Capacity of the lossless buffer of each order state and execution subscription (default 1024 events).
    /// When it is full, the subscription reports an overflow and the connector becomes degraded: commands are
    /// refused until the next successful `reconcile`.
    pub order_state_buffer: usize,
    /// Pause before the first reconnect of the WebSocket, doubling up to `reconnect_max` (default 0.5 s).
    pub reconnect_min: Duration,
    /// Upper bound of the pause (default 30 s).
    pub reconnect_max: Duration,
    /// How long a `subscribe_*` call waits for the broker to confirm the subscription (default 3 s), see
    /// [`MarketDataConfig::subscribe_ack_timeout`].
    pub subscribe_ack_timeout: Duration,
    /// Price steps and lot sizes the connector checks orders against (default: empty, only the lot sizes the
    /// connector reads from the broker). Fill it with [`PriceGrid::learn`] from the trading parameters of the
    /// instruments you trade. A price off the step is then refused with `Rejected` (`price_off_step`) before
    /// anything is sent.
    pub price_grid: PriceGrid,
}

impl UserDataConfig {
    /// Creates a config with the default buffer (1024 events) and the default reconnect pauses.
    ///
    /// `source` is the label put in every [`Envelope`](brokapi_core::Envelope) this connector emits.
    pub fn new(alor: AlorConfig, source: impl Into<Arc<str>>) -> Self {
        Self {
            alor,
            source: source.into(),
            order_state_buffer: 1024,
            reconnect_min: DEFAULT_RECONNECT_MIN,
            reconnect_max: DEFAULT_RECONNECT_MAX,
            subscribe_ack_timeout: Duration::from_secs(3),
            price_grid: PriceGrid::default(),
        }
    }

    /// Options of the session: it announces every connect, so the core sees a lost connection
    /// ([`brokapi_core::Event::Reconnected`]).
    pub(crate) fn ws_options(&self) -> WsOptions {
        WsOptions {
            announce_connects: true,
            reconnect_min: self.reconnect_min,
            reconnect_max: self.reconnect_max,
        }
    }
}
