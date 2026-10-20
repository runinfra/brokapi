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

/// The shortest ping period T-Bank accepts on its streams.
pub(crate) const MIN_PING: Duration = Duration::from_secs(5);
/// The longest ping period on the market data stream.
pub(crate) const MD_MAX_PING: Duration = Duration::from_secs(180);
/// The longest ping period on the user data streams. The order state stream accepts up to 120 s, and
/// the connector asks all its streams for the same period.
pub(crate) const UD_MAX_PING: Duration = Duration::from_secs(120);

/// The ping period the connector asks the server for, and the one its silence limit is built on:
/// `configured` brought into `5 s..=longest`. The request and the limit use this one value, so a
/// stream that pings as often as it was asked to is never taken for dead.
pub(crate) fn effective_ping(configured: Duration, longest: Duration) -> Duration {
    configured.clamp(MIN_PING, longest)
}

/// How long a stream may stay silent before it is closed and reopened: three ping periods.
pub(crate) fn silence_limit(ping: Duration) -> Duration {
    ping * 3
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_ping_period_is_brought_into_the_range_of_the_broker() {
        let secs = Duration::from_secs;
        assert_eq!(effective_ping(secs(1), MD_MAX_PING), secs(5), "too short");
        assert_eq!(effective_ping(secs(30), MD_MAX_PING), secs(30), "in range");
        assert_eq!(
            effective_ping(secs(600), MD_MAX_PING),
            secs(180),
            "too long"
        );
        assert_eq!(effective_ping(secs(150), UD_MAX_PING), secs(120));
        assert_eq!(effective_ping(Duration::ZERO, UD_MAX_PING), secs(5));
    }

    #[test]
    fn the_silence_limit_follows_the_period_that_was_asked_for() {
        // A period of 1 s is asked from the broker as 5 s: the limit is 15 s, not 3 s.
        let ping = effective_ping(Duration::from_secs(1), MD_MAX_PING);
        assert_eq!(silence_limit(ping), Duration::from_secs(15));
        // A period of 10 minutes is asked as 3 minutes: the limit is 9 minutes, not 30.
        let ping = effective_ping(Duration::from_secs(600), MD_MAX_PING);
        assert_eq!(silence_limit(ping), Duration::from_secs(540));
    }
}

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
    /// Server ping period on the stream. T-Bank accepts 5 s to 180 s: a value outside that range is
    /// brought into it, both for the request and for the silence limit. A stream silent for three
    /// periods is treated as dead and reconnected. Default 30 s.
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
    /// Server ping period on the user data streams. A value outside 5 s to 120 s is brought into
    /// that range, both for the request and for the silence limit. A stream silent for three periods
    /// is treated as dead and reconnected. Default 30 s.
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
