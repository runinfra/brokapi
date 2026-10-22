//! Maps the errors of the protocol layer ([`AlorApiError`]) to the public [`brokapi_core::Error`].
//!
//! There are two mappings, and the difference matters. [`map_err`] is for reads. [`map_command_err`] is for
//! commands that change the account, where "no answer" must never look like "nothing happened".

use std::time::Duration;

use brokapi_alor_protocol::error::AlorApiError;
use brokapi_core::{ConfigError, Error, TransportError};

/// Maps a failed read (or a failed step before a command was sent) to a public error.
///
/// | Protocol error | Public error |
/// |---|---|
/// | `RateLimitExceeded`, or `Http` with status 429 | [`Error::RateLimited`] with `retry_after` of 1 second (the broker does not say how long: the value is a fixed guess) |
/// | `Auth` (the token is refused) | [`Error::Config`] with the broker's message: fix the token, a repeat will not help |
/// | `Subscription`, `Ws`, `Parse` | [`Error::Transport`] with the message |
/// | `Http` (any other status or no status) | [`Error::Transport`] with the message |
/// | `NotFound` (HTTP 404) | [`Error::Rejected`] with code `not_found` (an unknown instrument or order, as at the other brokers) |
///
/// All [`Error::Transport`] results are retryable for a read. Do not use this function for a command: it would
/// turn a lost answer into a plain transport error. Use [`map_command_err`].
///
/// # Examples
///
/// ```
/// use brokapi_alor_protocol::error::AlorApiError;
/// use brokapi_alor::error::map_err;
/// use brokapi_core::Error;
///
/// assert!(matches!(map_err(AlorApiError::RateLimitExceeded), Error::RateLimited { .. }));
/// assert!(map_err(AlorApiError::NotFound { message: "no".into() }).is_not_found());
/// assert!(matches!(map_err(AlorApiError::Auth { message: "bad token".into() }), Error::Config(_)));
/// ```
pub fn map_err(err: AlorApiError) -> Error {
    match err {
        AlorApiError::RateLimitExceeded => Error::RateLimited {
            retry_after: Duration::from_secs(1),
        },
        AlorApiError::Auth { message } => Error::Config(ConfigError::Message(format!(
            "authentication refused: {message}"
        ))),
        AlorApiError::Http {
            status: Some(429), ..
        } => Error::RateLimited {
            retry_after: Duration::from_secs(1),
        },
        AlorApiError::Http { message, .. }
        | AlorApiError::Ws { message, .. }
        | AlorApiError::Subscription { message }
        | AlorApiError::Parse(message) => Error::Transport(TransportError::Message(message)),
        AlorApiError::NotFound { message } => Error::not_found(format!(
            "the broker does not know this order or instrument: {message}"
        )),
    }
}

/// Maps a failure to build a client from the configuration to [`Error::Config`], with the text of the
/// original error.
pub fn map_config_err(err: AlorApiError) -> Error {
    Error::Config(ConfigError::Message(err.to_string()))
}

/// A command that failed: only answers that prove the broker did not act become definite errors. No answer, a
/// break, HTTP 5xx or an unreadable answer may have reached the broker: `Error::Unknown`, never retried by the
/// library.
///
/// | Protocol error | Public error |
/// |---|---|
/// | `Http` without a status (no answer, a break, a timeout) or with status 500 and above | [`Error::Unknown`] |
/// | `Parse` (the answer cannot be read), `Ws` | [`Error::Unknown`] |
/// | `Http` with status 400 to 499, except 429 | [`Error::Rejected`] with code `http_<status>` and the broker's message |
/// | everything else (429, `Auth`, `NotFound`, ...) | as in [`map_err`] |
///
/// # Examples
///
/// ```
/// use brokapi_alor_protocol::error::AlorApiError;
/// use brokapi_alor::error::map_command_err;
/// use brokapi_core::Error;
///
/// // No answer at all: the order may exist.
/// let lost = AlorApiError::Http { status: None, message: "timeout".into() };
/// assert!(matches!(map_command_err(lost), Error::Unknown));
/// // The broker answered 503: it may have acted.
/// let busy = AlorApiError::Http { status: Some(503), message: "busy".into() };
/// assert!(matches!(map_command_err(busy), Error::Unknown));
/// // The broker answered 400: it did nothing.
/// let bad = AlorApiError::Http { status: Some(400), message: "no".into() };
/// match map_command_err(bad) {
///     Error::Rejected { code, .. } => assert_eq!(code, "http_400"),
///     other => panic!("{other:?}"),
/// }
/// ```
pub fn map_command_err(err: AlorApiError) -> Error {
    match &err {
        AlorApiError::Http { status, .. } if status.is_none_or(|s| s >= 500) => Error::Unknown,
        AlorApiError::Parse(_) | AlorApiError::Ws { .. } => Error::Unknown,
        // The broker answered and said no (400, 401, 403, 409 ...): nothing was done.
        AlorApiError::Http {
            status: Some(s),
            message,
        } if (400..500).contains(s) && *s != 429 => Error::Rejected {
            code: format!("http_{s}"),
            message: message.clone(),
        },
        // A 404 on a command on an order: the order is not there (already gone, or never was).
        AlorApiError::NotFound { message } => Error::Rejected {
            code: "not_found".into(),
            message: message.clone(),
        },
        _ => map_err(err),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lost_commands_are_unknown_refused_ones_are_not() {
        let http = |status| AlorApiError::Http {
            status,
            message: "x".into(),
        };
        assert!(matches!(map_command_err(http(None)), Error::Unknown));
        assert!(matches!(map_command_err(http(Some(503))), Error::Unknown));
        assert!(matches!(
            map_command_err(AlorApiError::Parse("x".into())),
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
    }
}
