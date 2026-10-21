//! Loads `limits.toml` (section `[bcs]`) through `BrokerLimits::from_toml_section`.
//!
//! BCS publishes no numbers of its own, so the bundled values are cautious starting points, to be checked
//! on a live account (the live runs page of the book, BCS section). One `BrokerLimiter` per token: give every
//! connector of one BCS account the same `Arc<BrokerLimiter>`.

use brokapi_core::ConfigError;
use brokapi_runtime::BrokerLimits;

/// The text of the bundled limits file `limits.toml`, compiled into the crate.
///
/// Parse it with [`limits_from_toml_str`], or take the result directly from [`default_limits`].
pub const DEFAULT_LIMITS_TOML: &str = include_str!("../limits.toml");

/// Reads the `[bcs]` section of a limits file given as text.
///
/// Other sections of the file (other brokers) are ignored. Give every connector of one BCS account
/// the same `BrokerLimiter` built from the result, because the limits belong to the token.
///
/// # Errors
///
/// [`brokapi_core::ConfigError::Message`] when the text is not valid TOML, when there is no `[bcs]`
/// section, or when the section is not a valid set of limits (see
/// [`BrokerLimits::from_toml_section`]).
///
/// # Examples
///
/// ```
/// use brokapi_bcs::limits::{limits_from_toml_str, DEFAULT_LIMITS_TOML};
///
/// assert!(limits_from_toml_str(DEFAULT_LIMITS_TOML).is_ok());
/// // A file for another broker has no [bcs] section.
/// assert!(limits_from_toml_str("[tbank.buckets.global]\nrate = 1").is_err());
/// ```
pub fn limits_from_toml_str(raw: &str) -> Result<BrokerLimits, ConfigError> {
    BrokerLimits::from_toml_section(raw, "bcs")
}

/// Reads the `[bcs]` section of a limits file on disk.
///
/// Use it when you tune the limits for your account without recompiling. Same as
/// [`limits_from_toml_str`] after reading the file.
///
/// # Errors
///
/// [`brokapi_core::ConfigError::Message`] when the file cannot be read (the message names the path),
/// and for every reason [`limits_from_toml_str`] lists.
///
/// # Examples
///
/// ```
/// use brokapi_bcs::limits::limits_from_toml_path;
///
/// let err = limits_from_toml_path("/definitely/not/here.toml").unwrap_err();
/// assert!(err.to_string().contains("/definitely/not/here.toml"));
/// ```
pub fn limits_from_toml_path(path: &str) -> Result<BrokerLimits, ConfigError> {
    let raw = std::fs::read_to_string(path)
        .map_err(|e| ConfigError::Message(format!("read {path}: {e}")))?;
    limits_from_toml_str(&raw)
}

/// The bundled BCS limits, parsed.
///
/// BCS publishes no numbers of its own, so these are cautious starting values, not the broker's
/// real limits (not verified against the live API). A method with no route of its own spends the
/// default route `"*"`, so no call is free. Cancel has its own bucket, separate from placing, so a
/// burst of new orders cannot block a cancel.
///
/// # Panics
///
/// Never in practice: the bundled file is checked by a test of this crate.
///
/// # Examples
///
/// ```
/// use brokapi_bcs::limits::default_limits;
/// use brokapi_runtime::BrokerLimiter;
/// use std::time::Instant;
///
/// let limiter = BrokerLimiter::new(&default_limits());
/// assert!(limiter.acquire("PlaceOrder", Instant::now()).is_ok());
/// ```
pub fn default_limits() -> BrokerLimits {
    limits_from_toml_str(DEFAULT_LIMITS_TOML).expect("bundled bcs_limits.toml is valid")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bundled_limits_parse_and_every_route_points_to_a_bucket() {
        let l = default_limits();
        // Every method a connector or raw() names.
        for method in [
            "PlaceOrder",
            "CancelOrder",
            "ReplaceOrder",
            "PlaceStopOrder",
            "CancelStopOrder",
            "GetOrder",
            "SearchOrders",
            "SearchTrades",
            "GetOrderBook",
            "GetLastTrades",
            "GetCandles",
            "GetPortfolio",
            "Reconcile",
            "GetTradingStatus",
            "GetDailySchedule",
            "GetInstrumentsByTickers",
            "GetInstrumentsByIsins",
            "GetInstrumentsByType",
        ] {
            let buckets = l.buckets_for(method);
            assert!(!buckets.is_empty(), "{method} is not routed");
            for b in buckets {
                assert!(l.buckets.contains_key(&b), "{method}: unknown bucket {b}");
            }
        }
        // A method with no route of its own spends the default route.
        assert!(!l.buckets_for("SomethingNew").is_empty());
        // Placing goes through a soft gate of its own that cancel does not pass: a flood of placements cannot
        // use up the whole budget of the orders service, a cancel still has room.
        assert!(l.route["PlaceOrder"].contains(&"place".to_string()));
        assert!(!l.route["CancelOrder"].contains(&"place".to_string()));
        let (place, orders) = (&l.buckets["place"], &l.buckets["orders"]);
        assert!(place.rate < orders.rate && place.burst < orders.burst);
    }

    #[test]
    fn a_file_with_no_bcs_section_is_refused() {
        assert!(limits_from_toml_str("[tbank.buckets.global]\nrate=1").is_err());
    }
}
