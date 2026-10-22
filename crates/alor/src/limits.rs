//! Loads `limits.toml` (section `[alor]`) through `BrokerLimits::from_toml_section`.
//!
//! The broker publishes one number for requests (100 a second per user) and the sizes of a WebSocket connection.
//! The rest of its protection (the social rating) is not published, so the bundled values are the published ones
//! and nothing more. One `BrokerLimiter` per login: give every connector of one ALOR login the same
//! `Arc<BrokerLimiter>`.

use brokapi_core::ConfigError;
use brokapi_runtime::BrokerLimits;

/// The text of the bundled limits file `limits.toml`, compiled into the crate.
///
/// Parse it with [`limits_from_toml_str`], or take the result directly from [`default_limits`].
pub const DEFAULT_LIMITS_TOML: &str = include_str!("../limits.toml");

/// Reads the `[alor]` section of a limits file given as text.
///
/// Other sections of the file (other brokers) are ignored.
///
/// # Errors
///
/// [`brokapi_core::ConfigError::Message`] when the text is not valid TOML, when there is no `[alor]` section, or
/// when the section is not a valid set of limits (see [`BrokerLimits::from_toml_section`]).
///
/// # Examples
///
/// ```
/// use brokapi_alor::limits::{limits_from_toml_str, DEFAULT_LIMITS_TOML};
///
/// assert!(limits_from_toml_str(DEFAULT_LIMITS_TOML).is_ok());
/// // A file for another broker has no [alor] section.
/// assert!(limits_from_toml_str("[tbank.buckets.global]\nrate = 1").is_err());
/// ```
pub fn limits_from_toml_str(raw: &str) -> Result<BrokerLimits, ConfigError> {
    BrokerLimits::from_toml_section(raw, "alor")
}

/// Reads the `[alor]` section of a limits file on disk, to tune the limits without recompiling.
///
/// # Errors
///
/// [`brokapi_core::ConfigError::Message`] when the file cannot be read (the message names the path), and for
/// every reason [`limits_from_toml_str`] lists.
///
/// # Examples
///
/// ```
/// use brokapi_alor::limits::limits_from_toml_path;
///
/// let err = limits_from_toml_path("/definitely/not/here.toml").unwrap_err();
/// assert!(err.to_string().contains("/definitely/not/here.toml"));
/// ```
pub fn limits_from_toml_path(path: &str) -> Result<BrokerLimits, ConfigError> {
    let raw = std::fs::read_to_string(path)
        .map_err(|e| ConfigError::Message(format!("read {path}: {e}")))?;
    limits_from_toml_str(&raw)
}

/// The bundled ALOR limits, parsed.
///
/// A method with no route of its own spends the default route `"*"`, so no call is free. Placing goes through a
/// soft bucket of its own that cancel does not pass.
///
/// # Panics
///
/// Never in practice: the bundled file is checked by a test of this crate.
///
/// # Examples
///
/// ```
/// use brokapi_alor::limits::default_limits;
/// use brokapi_runtime::BrokerLimiter;
/// use std::time::Instant;
///
/// let limiter = BrokerLimiter::new(&default_limits());
/// assert!(limiter.acquire("PlaceOrder", Instant::now()).is_ok());
/// ```
pub fn default_limits() -> BrokerLimits {
    limits_from_toml_str(DEFAULT_LIMITS_TOML).expect("bundled alor limits.toml is valid")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bundled_limits_parse_and_every_route_points_to_a_bucket() {
        let l = default_limits();
        for method in [
            "PlaceOrder",
            "CancelOrder",
            "ReplaceOrder",
            "PlaceStopOrder",
            "CancelStopOrder",
            "GetOrder",
            "GetOrders",
            "GetStopOrders",
            "GetTrades",
            "GetPositions",
            "GetSummary",
            "GetOrderBook",
            "GetLastTrades",
            "GetCandles",
            "GetSecurity",
            "GetSecurities",
            "GetTradingStatus",
            "Reconcile",
        ] {
            let buckets = l.buckets_for(method);
            assert!(!buckets.is_empty(), "{method} is not routed");
            for b in buckets {
                assert!(l.buckets.contains_key(&b), "{method}: unknown bucket {b}");
            }
        }
        assert!(!l.buckets_for("SomethingNew").is_empty());
        // Placing goes through a soft gate that cancel does not pass.
        assert!(l.route["PlaceOrder"].contains(&"place".to_string()));
        assert!(!l.route["CancelOrder"].contains(&"place".to_string()));
        let (place, http) = (&l.buckets["place"], &l.buckets["http"]);
        assert!(place.rate < http.rate && place.burst < http.burst);
    }

    #[test]
    fn a_file_with_no_alor_section_is_refused() {
        assert!(limits_from_toml_str("[tbank.buckets.global]\nrate=1").is_err());
    }
}
