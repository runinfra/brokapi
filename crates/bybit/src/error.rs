//! Mapping of the errors of the protocol layer to the broker-independent [`Error`].
//!
//! Two functions: [`map_err`] for reads and for everything that changes nothing, [`map_command_err`] for a command on
//! an order. They differ on purpose: a command that got no answer may have been done, so it is [`Error::Unknown`].

use std::time::Duration;

use brokapi_bybit_protocol::error::BybitApiError;
use brokapi_core::{ConfigError, Error, TransportError};

/// How long to wait after the exchange said "too many requests".
const RATE_LIMIT_PAUSE: Duration = Duration::from_secs(1);

/// The `retCode`s after which an order command may or may not have been done: a timeout and a restart of the service.
fn outcome_unknown(code: i64) -> bool {
    matches!(code, 10000 | 10016 | 429)
}

/// Whether a refusal says the symbol is not one the exchange has.
fn symbol_unknown(code: i64, message: &str) -> bool {
    code == 10001 && message.to_ascii_lowercase().contains("symbol invalid")
}

/// Maps an error of a read.
pub fn map_err(err: BybitApiError) -> Error {
    match err {
        BybitApiError::RateLimitExceeded => Error::RateLimited {
            retry_after: RATE_LIMIT_PAUSE,
        },
        BybitApiError::Auth { message } => Error::Config(ConfigError::Message(format!(
            "authentication refused: {message}"
        ))),
        BybitApiError::Http {
            status: Some(429), ..
        } => Error::RateLimited {
            retry_after: RATE_LIMIT_PAUSE,
        },
        BybitApiError::Http {
            status: Some(403),
            message,
        } => Error::Transport(TransportError::Message(format!(
            "HTTP 403: the address is over the request limit (a ban of 10 minutes) or the region is not served: {message}"
        ))),
        BybitApiError::Api { code, message } if symbol_unknown(code, &message) => Error::not_found(
            format!("the exchange does not know this instrument: {message}"),
        ),
        BybitApiError::Api {
            code: 110001,
            message,
        } => Error::not_found(format!("the exchange does not know this order: {message}")),
        BybitApiError::Api { code, message } => Error::Rejected {
            code: format!("bybit_{code}"),
            message,
        },
        BybitApiError::Http { message, .. }
        | BybitApiError::Ws { message }
        | BybitApiError::Subscription { message }
        | BybitApiError::Parse(message) => Error::Transport(TransportError::Message(message)),
    }
}

/// Maps an error of the construction of the client.
pub fn map_config_err(err: BybitApiError) -> Error {
    Error::Config(ConfigError::Message(err.to_string()))
}

/// Maps an error of a command that changes an order.
pub fn map_command_err(err: BybitApiError) -> Error {
    match &err {
        BybitApiError::Http { status, .. } if status.is_none_or(|s| s >= 500) => Error::Unknown,
        BybitApiError::Parse(_) | BybitApiError::Ws { .. } => Error::Unknown,
        BybitApiError::Api { code, .. } if outcome_unknown(*code) => Error::Unknown,
        // The exchange answered and said no (a 4xx, a refusal code): nothing was done.
        BybitApiError::Http {
            status: Some(s),
            message,
        } if (400..500).contains(s) && *s != 429 => Error::Rejected {
            code: format!("http_{s}"),
            message: message.clone(),
        },
        // An order that is not there (already gone, or never was).
        BybitApiError::Api {
            code: 110001,
            message,
        } => Error::Rejected {
            code: brokapi_core::NOT_FOUND.into(),
            message: message.clone(),
        },
        _ => map_err(err),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn api(code: i64, message: &str) -> BybitApiError {
        BybitApiError::Api {
            code,
            message: message.into(),
        }
    }

    #[test]
    fn lost_commands_are_unknown_refused_ones_are_not() {
        let http = |status| BybitApiError::Http {
            status,
            message: "x".into(),
        };
        assert!(matches!(map_command_err(http(None)), Error::Unknown));
        assert!(matches!(map_command_err(http(Some(503))), Error::Unknown));
        assert!(matches!(
            map_command_err(BybitApiError::Parse("x".into())),
            Error::Unknown
        ));
        assert!(matches!(
            map_command_err(api(10016, "restarting")),
            Error::Unknown
        ));
        assert!(matches!(
            map_command_err(api(10000, "timeout")),
            Error::Unknown
        ));
        assert!(matches!(
            map_command_err(http(Some(429))),
            Error::RateLimited { .. }
        ));
        assert!(matches!(
            map_command_err(http(Some(403))),
            Error::Rejected { .. }
        ));
        assert!(matches!(
            map_command_err(BybitApiError::RateLimitExceeded),
            Error::RateLimited { .. }
        ));
    }

    #[test]
    fn a_refusal_keeps_its_code_and_an_unknown_order_is_not_found() {
        match map_command_err(api(110072, "OrderLinkedID is duplicate")) {
            Error::Rejected { code, .. } => assert_eq!(code, "bybit_110072"),
            other => panic!("{other:?}"),
        }
        match map_command_err(api(110001, "Order does not exist")) {
            Error::Rejected { code, .. } => assert_eq!(code, brokapi_core::NOT_FOUND),
            other => panic!("{other:?}"),
        }
        assert!(map_err(api(110001, "Order does not exist")).is_not_found());
        assert!(map_err(api(10001, "params error: symbol invalid")).is_not_found());
        assert!(matches!(
            map_err(api(10001, "params error: limit")),
            Error::Rejected { .. }
        ));
    }

    #[test]
    fn a_refused_key_is_a_configuration_error() {
        let e = map_err(BybitApiError::Auth {
            message: "10003: API key is invalid".into(),
        });
        assert!(matches!(e, Error::Config(_)));
    }
}
