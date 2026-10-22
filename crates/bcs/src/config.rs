//! Configuration of the two connectors: the BCS client settings plus the choices of the connector
//! itself (source label, reconnect pauses, buffer size).
//!
//! Both configs wrap a `BcsConfig` from the protocol crate. It holds the refresh token, the
//! timeouts and the write switch. Build it with `BcsConfig::from_env()` for a real account, or with
//! `BcsConfig::for_mock(token, url)` for tests against `brokapi-mocks`.

use std::sync::Arc;
use std::time::Duration;

use brokapi_bcs_protocol::config::BcsConfig;
use brokapi_bcs_protocol::ws::session::{DEFAULT_RECONNECT_MAX, DEFAULT_RECONNECT_MIN, WsOptions};
use brokapi_runtime::PriceGrid;

/// Settings of [`MarketDataConnector`](crate::MarketDataConnector).
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use brokapi_bcs_protocol::config::BcsConfig;
/// use brokapi_bcs::MarketDataConfig;
///
/// let mut cfg = MarketDataConfig::new(BcsConfig::new("refresh-token".into()), "bcs-md");
/// assert_eq!(&*cfg.source, "bcs-md");
/// // Wait at most 10 seconds between reconnects instead of the default 30.
/// cfg.reconnect_max = Duration::from_secs(10);
/// ```
#[derive(Debug, Clone)]
pub struct MarketDataConfig {
    /// Client settings of the BCS API: refresh token, timeouts, endpoints, write switch.
    pub bcs: BcsConfig,
    /// Envelope source label, e.g. `"bcs-md"`.
    pub source: Arc<str>,
    /// Pause before the first reconnect of the WebSocket, doubling up to `reconnect_max`
    /// (default 0.5 s, as at T-Bank and Finam).
    pub reconnect_min: Duration,
    /// Upper bound of the pause (default 30 s). The pause returns to `reconnect_min` once a connection
    /// has lived 10 seconds.
    pub reconnect_max: Duration,
    /// How long a `subscribe_*` call waits for the broker to confirm the subscription (default 3 s). The call
    /// returns when the broker has confirmed: what happens after that is delivered. A refusal is returned as
    /// `Rejected` (`subscription_refused`). If nothing comes in time, the call fails with `Transport` and the
    /// subscription is taken back, so the call can be repeated.
    pub subscribe_ack_timeout: Duration,
}

impl MarketDataConfig {
    /// Creates a config with the default reconnect pauses (0.5 s up to 30 s) and a 3 s wait for the
    /// confirmation of a subscription.
    ///
    /// `source` is the label put in every [`Envelope`](brokapi_core::Envelope) this connector emits,
    /// so a bot with several connectors can tell them apart.
    pub fn new(bcs: BcsConfig, source: impl Into<Arc<str>>) -> Self {
        Self {
            bcs,
            source: source.into(),
            reconnect_min: DEFAULT_RECONNECT_MIN,
            reconnect_max: DEFAULT_RECONNECT_MAX,
            subscribe_ack_timeout: Duration::from_secs(3),
        }
    }
}

/// Settings of [`UserDataConnector`](crate::UserDataConnector).
///
/// For a connector that sends orders the `bcs` config must be built with `with_write(true)`.
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::config::BcsConfig;
/// use brokapi_bcs::UserDataConfig;
///
/// let bcs = BcsConfig::new("refresh-token".into()).with_write(true);
/// let mut cfg = UserDataConfig::new(bcs, "bcs-ud");
/// assert_eq!(cfg.order_state_buffer, 1024);
/// // A bigger buffer gives a slow bot more room before an overflow closes the trading gate.
/// cfg.order_state_buffer = 4096;
/// ```
#[derive(Debug, Clone)]
pub struct UserDataConfig {
    /// Client settings of the BCS API: refresh token, timeouts, endpoints, write switch.
    pub bcs: BcsConfig,
    /// Envelope source label, e.g. `"bcs-ud"`.
    pub source: Arc<str>,
    /// Capacity of the lossless buffer of each order state and execution subscription (default
    /// 1024 events). When it is full, the subscription reports an overflow and the connector
    /// becomes degraded: commands are refused until the next successful `reconcile`.
    pub order_state_buffer: usize,
    /// Pause before the first reconnect of the WebSocket, doubling up to `reconnect_max`
    /// (default 0.5 s, as at T-Bank and Finam).
    pub reconnect_min: Duration,
    /// Upper bound of the pause (default 30 s). The pause returns to `reconnect_min` once a connection
    /// has lived 10 seconds.
    pub reconnect_max: Duration,
    /// How long a `subscribe_*` call waits for the WebSocket of its channel to be open (default 3 s). The
    /// call returns when the socket is open: an order placed right after it is not missed. If the socket does
    /// not open in time, the call fails with `Transport` and the subscription is taken back, so the call can
    /// be repeated.
    pub subscribe_ack_timeout: Duration,
    /// Price steps the connector checks order prices against (default: empty, nothing is checked).
    /// Fill it with [`PriceGrid::learn`] from the trading parameters of the instruments you trade. A
    /// price off the step is then refused with `Rejected` (`price_off_step`) before anything is sent.
    pub price_grid: PriceGrid,
}

impl UserDataConfig {
    /// Creates a config with the default buffer (1024 events), the default reconnect pauses
    /// (0.5 s up to 30 s) and a 3 s wait for the socket in `subscribe_*`.
    ///
    /// `source` is the label put in every [`Envelope`](brokapi_core::Envelope) this connector emits.
    pub fn new(bcs: BcsConfig, source: impl Into<Arc<str>>) -> Self {
        Self {
            bcs,
            source: source.into(),
            order_state_buffer: 1024,
            reconnect_min: DEFAULT_RECONNECT_MIN,
            reconnect_max: DEFAULT_RECONNECT_MAX,
            subscribe_ack_timeout: Duration::from_secs(3),
            price_grid: PriceGrid::default(),
        }
    }
}

impl MarketDataConfig {
    pub(crate) fn ws_options(&self) -> WsOptions {
        // The core is told of every connect: a reconnect voids the order books (`Resync`).
        WsOptions {
            announce_connects: true,
            reconnect_min: self.reconnect_min,
            reconnect_max: self.reconnect_max,
        }
    }
}

impl UserDataConfig {
    /// Options of the order events session: it announces every connect, so the core sees a lost
    /// connection ([`brokapi_core::Event::Reconnected`]). The portfolio session does not (every message of
    /// it is a whole state).
    pub(crate) fn ws_options(&self) -> WsOptions {
        WsOptions {
            announce_connects: true,
            reconnect_min: self.reconnect_min,
            reconnect_max: self.reconnect_max,
        }
    }

    /// Options of the portfolio session: no announcements.
    pub(crate) fn portfolio_ws_options(&self) -> WsOptions {
        WsOptions {
            announce_connects: false,
            ..self.ws_options()
        }
    }
}
