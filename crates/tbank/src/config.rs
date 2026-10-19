//! Connector configuration
//!
//! [`MarketDataConfig`] and [`UserDataConfig`] wrap a [`TbankConfig`] (endpoint, token, account) and
//! add what the connector itself needs: the label of its events and the ping period of its streams.
//!
//! # Examples
//!
//! ```
//! use std::time::Duration;
//! use brokapi_tbank::{MarketDataConfig, UserDataConfig};
//! use brokapi_tbank_protocol::TbankConfig;
//!
//! let token = TbankConfig::sandbox("t.my-token");
//!
//! // Market data does not depend on the account.
//! let md = MarketDataConfig::new(token.clone(), "tbank-md");
//! assert_eq!(md.ping_interval, Duration::from_secs(30));
//!
//! // Two accounts of one token: two configs, each with its own account and its own source label.
//! let main = UserDataConfig::new(token.clone().with_account("acc-main"), "tbank-ud-main");
//! let second = UserDataConfig::new(token.with_account("acc-second"), "tbank-ud-second");
//! assert_ne!(main.source, second.source);
//! ```

use std::sync::Arc;
use std::time::Duration;

use brokapi_runtime::PriceGrid;
use brokapi_tbank_protocol::TbankConfig;

/// Market data connector config. Not tied to an account: one per broker (token) is enough.
///
/// Build it with [`MarketDataConfig::new`] and change the public fields if needed.
///
/// # Examples
///
/// ```
/// use brokapi_tbank::MarketDataConfig;
/// use brokapi_tbank_protocol::TbankConfig;
///
/// let mut cfg = MarketDataConfig::new(TbankConfig::sandbox("t.my-token"), "tbank-md");
/// cfg.ping_interval = std::time::Duration::from_secs(10);
/// assert_eq!(&*cfg.source, "tbank-md");
/// ```
#[derive(Debug, Clone)]
pub struct MarketDataConfig {
    /// Endpoint, token and timeouts of the API. `tbank.account_id` is ignored by market data.
    pub tbank: TbankConfig,
    /// Envelope source label, e.g. `"tbank-md"`.
    pub source: Arc<str>,
    /// Server ping period on the stream (T-Bank accepts 5000..180000 ms, the connector clamps the
    /// value it requests to that range). A stream silent for three periods is treated as dead and
    /// reconnected. Keep it at 5 s or more: the silence limit is three times this value as given,
    /// so a shorter period would drop a healthy stream. Default 30 s.
    pub ping_interval: Duration,
}

impl MarketDataConfig {
    /// A config with the default ping period (30 s).
    ///
    /// `source` labels every [`Envelope`](brokapi_core::Envelope) this connector emits, so that you
    /// can tell connectors apart when they write into one channel.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_tbank::MarketDataConfig;
    /// use brokapi_tbank_protocol::TbankConfig;
    ///
    /// let cfg = MarketDataConfig::new(TbankConfig::production("t.my-token"), "tbank-md");
    /// assert_eq!(&*cfg.source, "tbank-md");
    /// ```
    pub fn new(tbank: TbankConfig, source: impl Into<Arc<str>>) -> Self {
        Self {
            tbank,
            source: source.into(),
            ping_interval: Duration::from_secs(30),
        }
    }
}

/// User data connector config. One connector = one account: `tbank.account_id`.
/// With `None` the connector uses the token's only open account and refuses to start when there are several.
///
/// Several accounts of one token need several connectors, each with its own config that differs in
/// `tbank.account_id` and in `source`. Share one rate limiter between them.
///
/// # Examples
///
/// ```
/// use brokapi_tbank::UserDataConfig;
/// use brokapi_tbank_protocol::TbankConfig;
///
/// let cfg = UserDataConfig::new(TbankConfig::sandbox("t.my-token").with_account("acc-main"), "tbank-ud-main");
/// assert_eq!(cfg.tbank.account_id.as_deref(), Some("acc-main"));
/// assert_eq!(cfg.order_state_buffer, 1024);
/// ```
#[derive(Debug, Clone)]
pub struct UserDataConfig {
    /// Endpoint, token, account and timeouts of the API. Set `account_id` with
    /// [`TbankConfig::with_account`] when the token sees more than one account.
    pub tbank: TbankConfig,
    /// Envelope source label, e.g. `"tbank-ud-main"`. Give every account its own.
    pub source: Arc<str>,
    /// Lossless buffer capacity for the order state and executions subscriptions.
    ///
    /// Own order states and trades are never dropped. If the consumer is so slow that the buffer
    /// fills up, an overflow event is sent and the trading gate closes until the next successful
    /// reconcile. Default 1024.
    pub order_state_buffer: usize,
    /// Server ping period on the user data streams. The connector requests it clamped to
    /// 5000..120000 ms, and treats a stream silent for three periods (of the value as given) as
    /// dead. Keep it at 5 s or more. Default 30 s.
    pub ping_interval: Duration,
    /// Price steps the connector checks order prices against (default: empty, nothing is checked).
    /// Fill it with [`PriceGrid::learn`] from the trading parameters of the instruments you trade. A
    /// price off the step is then refused with `Rejected` (`price_off_step`) before anything is sent.
    pub price_grid: PriceGrid,
}

impl UserDataConfig {
    /// A config with the default buffer (1024 events) and ping period (30 s).
    ///
    /// `source` labels every [`Envelope`](brokapi_core::Envelope) of this connector. Give each account
    /// its own label.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_tbank::UserDataConfig;
    /// use brokapi_tbank_protocol::TbankConfig;
    ///
    /// let cfg = UserDataConfig::new(TbankConfig::sandbox("t.my-token"), "tbank-ud");
    /// assert_eq!(&*cfg.source, "tbank-ud");
    /// ```
    pub fn new(tbank: TbankConfig, source: impl Into<Arc<str>>) -> Self {
        Self {
            tbank,
            source: source.into(),
            order_state_buffer: 1024,
            ping_interval: Duration::from_secs(30),
            price_grid: PriceGrid::default(),
        }
    }
}
