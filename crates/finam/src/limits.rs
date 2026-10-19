//! Rate limits of the Finam connectors, loaded from the bundled `configs/finam_limits.toml`.
//!
//! The only number Finam documents is 200 requests per minute for the whole API. The bundled buckets sit
//! below it (global 200 per minute, orders 60, cancel 100, market data 120, reference 100, reports
//! 20). Whether the quota is per user or per secret is not stated by the broker. The file reads it as
//! PER USER, so create ONE [`BrokerLimiter`](brokapi_runtime::BrokerLimiter) per Finam user and give it to
//! every connector. This reading is not verified against the live API.
//!
//! A request over the limit is refused locally with `Error::RateLimited` and never sent.
//!
//! # Examples
//!
//! ```
//! use brokapi_finam::limits::default_limits;
//! use brokapi_runtime::BrokerLimiter;
//!
//! let limits = default_limits();
//! // Every proto method name has a route. Unknown names spend the "*" default route.
//! assert!(limits.route.contains_key("PlaceOrder"));
//! let limiter = BrokerLimiter::new(&limits);
//! // Hand `limiter.clone()` to every connector of this Finam user.
//! assert!(limiter.acquire("GetOrders", std::time::Instant::now()).is_ok());
//! ```

use brokapi_core::{ConfigError, Error};
use brokapi_runtime::BrokerLimits;

/// Text of the bundled limits file. Written from the broker documentation, see the file header for sources.
pub const DEFAULT_LIMITS_TOML: &str = include_str!("../../../configs/finam_limits.toml");

/// Parses the `[finam]` section of a limits file through `BrokerLimits::from_toml_section`.
///
/// Use it to load your own limits, for example when the broker raises its quota.
///
/// # Errors
///
/// [`Error::Config`] when the text is not valid TOML or has no `[finam]` section. A section of
/// another broker is not accepted.
///
/// # Examples
///
/// ```
/// use brokapi_finam::limits::{DEFAULT_LIMITS_TOML, parse_limits};
///
/// assert!(parse_limits(DEFAULT_LIMITS_TOML).is_ok());
/// assert!(parse_limits("[bcs.buckets.global]\nrate = 1").is_err());
/// ```
pub fn parse_limits(raw: &str) -> Result<BrokerLimits, Error> {
    BrokerLimits::from_toml_section(raw, "finam").map_err(|e: ConfigError| Error::Config(e))
}

/// The bundled limits, ready for [`BrokerLimiter::new`](brokapi_runtime::BrokerLimiter::new).
///
/// # Panics
///
/// Never in a correct build: the bundled file is checked by a test.
pub fn default_limits() -> BrokerLimits {
    parse_limits(DEFAULT_LIMITS_TOML).expect("bundled finam_limits.toml is valid")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bundled_limits_parse_and_every_route_points_to_a_bucket() {
        let l = default_limits();
        assert!(!l.route.is_empty());
        for method in l.route.keys() {
            for b in l.buckets_for(method) {
                assert!(l.buckets.contains_key(&b), "{method}: unknown bucket {b}");
            }
        }
    }

    #[test]
    fn other_broker_section_is_not_accepted() {
        assert!(parse_limits("[bcs.buckets.global]\nrate=1").is_err());
    }
}
