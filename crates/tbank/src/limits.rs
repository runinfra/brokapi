//! Loads `limits.toml` (section `[tbank]`) through `BrokerLimits::from_toml_section`.
//!
//! One `BrokerLimiter` per user (all accounts, all tokens, one IP): see the scope notes in the toml.
//!
//! T-Bank counts its limits per user, not per account and not per token. So every connector of one
//! user must share ONE limiter, or the sum of their requests can pass the broker's cap while each
//! connector thinks it is within its own quota. Build the limiter once and give each connector a
//! clone of the `Arc`.
//!
//! The bundled defaults are the figures from the broker's documentation. T-Bank makes the real
//! limits dynamic (they depend on the user's tariff), so the figures here are a safe start and not
//! verified against the live API.
//!
//! # Examples
//!
//! ```
//! use std::sync::Arc;
//! use brokapi_runtime::BrokerLimiter;
//! use brokapi_tbank::limits::default_limits;
//!
//! // One limiter for everything one user runs. Give `Arc::clone(&limiter)` to every connector:
//! // all market data connectors and all user data connectors, whatever their account.
//! let limiter = BrokerLimiter::new(&default_limits());
//! let for_account_a = Arc::clone(&limiter);
//! let for_account_b = Arc::clone(&limiter);
//! assert!(Arc::ptr_eq(&for_account_a, &for_account_b));
//! ```

use brokapi_core::ConfigError;
use brokapi_runtime::BrokerLimits;

/// The bundled default limits, as documented by T-Bank (the text of `limits.toml`).
///
/// Parse it with [`limits_from_toml_str`], or call [`default_limits`].
pub const DEFAULT_LIMITS_TOML: &str = include_str!("../limits.toml");

/// Reads the `[tbank]` section of a limits file given as text.
///
/// Use it to replace the bundled figures, for example after `GetUserTariff` showed that your tariff
/// has other limits.
///
/// # Errors
///
/// [`ConfigError::Message`] when the text is not valid TOML, the `[tbank]` section is missing, or
/// it is invalid.
///
/// # Examples
///
/// ```
/// use brokapi_tbank::limits::{limits_from_toml_str, DEFAULT_LIMITS_TOML};
///
/// assert!(limits_from_toml_str(DEFAULT_LIMITS_TOML).is_ok());
/// // A file without a `[tbank]` section is refused.
/// assert!(limits_from_toml_str("[bcs.conns]").is_err());
/// ```
pub fn limits_from_toml_str(raw: &str) -> Result<BrokerLimits, ConfigError> {
    BrokerLimits::from_toml_section(raw, "tbank")
}

/// Reads the `[tbank]` section of a limits file on disk.
///
/// # Errors
///
/// [`ConfigError::Message`] when the file cannot be read (the message names the path), and for
/// every reason [`limits_from_toml_str`] lists.
///
/// # Examples
///
/// ```
/// let err = brokapi_tbank::limits::limits_from_toml_path("/no/such/limits.toml").unwrap_err();
/// assert!(err.to_string().contains("/no/such/limits.toml"));
/// ```
pub fn limits_from_toml_path(path: &str) -> Result<BrokerLimits, ConfigError> {
    let raw = std::fs::read_to_string(path)
        .map_err(|e| ConfigError::Message(format!("read {path}: {e}")))?;
    limits_from_toml_str(&raw)
}

/// The bundled default limits of T-Bank, ready for [`BrokerLimiter::new`](brokapi_runtime::BrokerLimiter::new).
///
/// Every unary method of the API has a route (placing and cancelling orders have buckets of their
/// own). A method name without a route spends the `"*"` default route, so no call is free.
///
/// # Panics
///
/// Never in practice: the bundled file is checked by this crate's tests.
///
/// # Examples
///
/// ```
/// use brokapi_tbank::limits::default_limits;
///
/// let limits = default_limits();
/// // Placing and cancelling spend different buckets: a busy cancel loop cannot starve placing.
/// assert!(limits.buckets_for("PostOrder").contains(&"order_post".to_string()));
/// assert!(limits.buckets_for("CancelOrder").contains(&"order_cancel".to_string()));
/// ```
pub fn default_limits() -> BrokerLimits {
    limits_from_toml_str(DEFAULT_LIMITS_TOML).expect("bundled tbank_limits.toml is valid")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bundled_limits_parse_and_route() {
        let l = default_limits();
        for method in [
            "PostOrder",
            "CancelOrder",
            "ReplaceOrder",
            "GetOrders",
            "GetOrderState",
            "GetOrderBook",
            "GetLastTrades",
            "GetCandles",
            "GetTradingStatus",
            "SubscribeMarketData",
            "FindInstrument",
            "GetInstrumentBy",
            "TradingSchedules",
            "GetPortfolio",
            "GetPositions",
            "GetOperationsByCursor",
            "GetAccounts",
            "Shares",
            "Bonds",
            "Etfs",
            "Futures",
            "Options",
            "Currencies",
        ] {
            let buckets = l.buckets_for(method);
            assert!(buckets.len() >= 2, "{method} not routed");
            for b in buckets {
                assert!(l.buckets.contains_key(&b), "{method}: unknown bucket {b}");
            }
        }
        assert!(l.conns.supports_multiplex);
    }

    #[test]
    fn cancel_has_its_own_bucket() {
        let l = default_limits();
        let place = l.buckets_for("PostOrder");
        let cancel = l.buckets_for("CancelOrder");
        assert!(place.contains(&"order_post".to_string()));
        assert!(cancel.contains(&"order_cancel".to_string()));
        assert!(!cancel.contains(&"order_post".to_string()));
    }

    /// Every unary method of every .proto has a route of its own (the documented bucket of its
    /// service). Streams are opened rarely and use the "*" default route.
    #[test]
    fn every_unary_method_of_the_api_is_routed() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../tbank-protocol/proto");
        let l = default_limits();
        let mut missing = Vec::new();
        let mut seen = 0;
        for entry in std::fs::read_dir(dir).expect("proto dir") {
            let path = entry.expect("entry").path();
            if path.extension().is_none_or(|e| e != "proto") {
                continue;
            }
            for line in std::fs::read_to_string(&path).expect("read proto").lines() {
                let line = line.trim();
                let Some(rest) = line.strip_prefix("rpc ") else {
                    continue;
                };
                if rest.contains("stream ") {
                    continue;
                }
                let name = rest.split('(').next().expect("rpc name").trim();
                seen += 1;
                if !l.route.contains_key(name) {
                    missing.push(name.to_string());
                }
            }
        }
        assert!(seen > 80, "proto files not found ({seen} methods)");
        assert!(
            missing.is_empty(),
            "unary methods without a route: {missing:?}"
        );
    }

    #[test]
    fn unnamed_method_uses_the_default_route() {
        let l = default_limits();
        let b = l.buckets_for("SomethingTheApiAddsNextYear");
        assert_eq!(b, vec!["default".to_string(), "global".to_string()]);
    }
}
