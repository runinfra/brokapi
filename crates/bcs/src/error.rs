//! Maps the errors of the protocol layer ([`BcsApiError`]) to the public [`brokapi_core::Error`].
//!
//! The protocol layer first sorts an HTTP answer. A body with `type = "VALIDATION_ERROR"` becomes
//! `ValidationError`, `type = "NOT_FOUND"` becomes `NotFound`, any other JSON body with a `type`
//! becomes `HttpError` with that type as the message. A body that is not that JSON becomes
//! `RateLimitExceeded` for HTTP 429 and `HttpError` with the body text for any other status. This
//! module then picks the public error.
//!
//! There are two mappings, and the difference matters. [`map_err`] is for reads. [`map_command_err`]
//! and [`map_order_cmd_err`] are for commands that change the account, where "no answer" must never
//! look like "nothing happened".

use std::time::Duration;

use brokapi_bcs_protocol::error::BcsApiError;
use brokapi_core::{ConfigError, Error, TransportError};

/// Maps a failed read (or a failed step before a command was sent) to a public error.
///
/// | Protocol error | Public error |
/// |---|---|
/// | `RateLimitExceeded`, or `HttpError` with status 429 | [`Error::RateLimited`] with `retry_after` of 1 second (the broker does not say how long: the value is a fixed guess) |
/// | `AuthError` (the token is refused) | [`Error::Config`] with the broker's message: fix the token, a repeat will not help |
/// | `SubscriptionError` | [`Error::Transport`] with the broker's message |
/// | `HttpError` (any other status or no status), `WsError`, `ParseError` | [`Error::Transport`] with the message |
/// | `NotFound` (HTTP 404, `type = "NOT_FOUND"`) | [`Error::Rejected`] with code `not_found` (an unknown instrument or order, as at the other brokers) |
/// | `ValidationError` (HTTP 400, `type = "VALIDATION_ERROR"`) | [`Error::Rejected`] with code `validation` and the field errors as the message |
///
/// All [`Error::Transport`] results are retryable for a read. Do not use this function for a
/// command: it would turn a lost answer into a plain transport error. Use [`map_command_err`].
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::error::BcsApiError;
/// use brokapi_bcs::error::map_err;
/// use brokapi_core::Error;
///
/// assert!(matches!(map_err(BcsApiError::RateLimitExceeded), Error::RateLimited { .. }));
/// assert!(map_err(BcsApiError::NotFound { trace_id: None }).is_not_found());
/// assert!(matches!(map_err(BcsApiError::AuthError { message: "bad token".into() }), Error::Config(_)));
/// let validation = BcsApiError::ValidationError { trace_id: None, errors: vec![] };
/// assert!(matches!(map_err(validation), Error::Rejected { .. }));
/// ```
pub fn map_err(err: BcsApiError) -> Error {
    match err {
        BcsApiError::RateLimitExceeded => Error::RateLimited {
            retry_after: Duration::from_secs(1),
        },
        BcsApiError::AuthError { message } => Error::Config(ConfigError::Message(format!(
            "authentication refused: {message}"
        ))),
        BcsApiError::SubscriptionError { message } => {
            Error::Transport(TransportError::Message(message))
        }
        BcsApiError::HttpError {
            status: Some(429), ..
        } => Error::RateLimited {
            retry_after: Duration::from_secs(1),
        },
        BcsApiError::HttpError { message, .. }
        | BcsApiError::WsError { message, .. }
        | BcsApiError::ParseError(message) => Error::Transport(TransportError::Message(message)),
        BcsApiError::NotFound { .. } => {
            Error::not_found("the broker does not know this order or instrument")
        }
        BcsApiError::ValidationError { errors, .. } => Error::Rejected {
            code: "validation".into(),
            message: format!("{errors:?}"),
        },
    }
}

/// Maps a failure to build a client from the configuration to [`Error::Config`], with the text of
/// the original error.
pub fn map_config_err(err: BcsApiError) -> Error {
    Error::Config(ConfigError::Message(err.to_string()))
}

/// Maps a failure to encode a client order id to [`Error::Config`], with the text of the original
/// error.
pub fn map_cid_err(err: brokapi_core::CidError) -> Error {
    Error::Config(ConfigError::Message(err.to_string()))
}

/// A command that failed: only answers that prove the broker did not act become definite errors.
/// No answer, a break, HTTP 5xx or an unreadable answer may have reached the broker: `Error::Unknown`,
/// never retried by the library.
///
/// | Protocol error | Public error |
/// |---|---|
/// | `HttpError` without a status (no answer, a break, a timeout) or with status 500 and above | [`Error::Unknown`] |
/// | `ParseError` (the answer cannot be read), `WsError` | [`Error::Unknown`] |
/// | `HttpError` with status 400 to 499, except 429 | [`Error::Rejected`] with code `http_<status>` and the broker's message |
/// | everything else (429, `AuthError`, `NotFound`, `ValidationError`, ...) | as in [`map_err`] |
///
/// Note that `ValidationError` (the usual way BCS refuses an order) is [`Error::Rejected`] with code
/// `validation`, and that `NotFound` is [`Error::Rejected`] with code `not_found`, as in [`map_err`].
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::error::BcsApiError;
/// use brokapi_bcs::error::map_command_err;
/// use brokapi_core::Error;
///
/// // No answer at all: the order may exist.
/// let lost = BcsApiError::HttpError { status: None, message: "timeout".into(), trace_id: None };
/// assert!(matches!(map_command_err(lost), Error::Unknown));
/// // The broker answered 503: it may have acted.
/// let busy = BcsApiError::HttpError { status: Some(503), message: "busy".into(), trace_id: None };
/// assert!(matches!(map_command_err(busy), Error::Unknown));
/// // The broker answered 403: it did nothing.
/// let denied = BcsApiError::HttpError { status: Some(403), message: "no".into(), trace_id: None };
/// match map_command_err(denied) {
///     Error::Rejected { code, .. } => assert_eq!(code, "http_403"),
///     other => panic!("{other:?}"),
/// }
/// ```
pub fn map_command_err(err: BcsApiError) -> Error {
    match &err {
        BcsApiError::HttpError { status, .. } if status.is_none_or(|s| s >= 500) => Error::Unknown,
        BcsApiError::ParseError(_) | BcsApiError::WsError { .. } => Error::Unknown,
        // The broker answered and said no (400, 403, 409, 422 ...): nothing was done.
        BcsApiError::HttpError {
            status: Some(s),
            message,
            ..
        } if (400..500).contains(s) && *s != 429 => Error::Rejected {
            code: format!("http_{s}"),
            message: message.clone(),
        },
        _ => map_err(err),
    }
}

/// [`map_err`] for a read of one order. Since "not found" is `Rejected` with code `not_found` in
/// [`map_err`] already, this is the same mapping, kept for the code that calls it.
///
/// Used by `order_state`. The way out of an `Unknown` is to ask again until the
/// order is found or the answer stays `not_found` long enough ([`Error::Unknown`]).
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::error::BcsApiError;
/// use brokapi_bcs::error::map_order_read_err;
/// use brokapi_core::Error;
///
/// match map_order_read_err(BcsApiError::NotFound { trace_id: None }) {
///     Error::Rejected { code, .. } => assert_eq!(code, "not_found"),
///     other => panic!("{other:?}"),
/// }
/// assert!(matches!(map_order_read_err(BcsApiError::RateLimitExceeded), Error::RateLimited { .. }));
/// ```
pub fn map_order_read_err(err: BcsApiError) -> Error {
    map_err(err)
}
/// [`map_command_err`] for commands on an existing order. "Not found" is a definite refusal
/// (`Rejected`, code `not_found`) in [`map_command_err`] already; this is the same mapping, kept for the
/// code that calls it.
///
/// Used by `cancel`, `replace` and `cancel_stop`.
///
/// # Examples
///
/// ```
/// use brokapi_bcs_protocol::error::BcsApiError;
/// use brokapi_bcs::error::map_order_cmd_err;
/// use brokapi_core::Error;
///
/// match map_order_cmd_err(BcsApiError::NotFound { trace_id: None }) {
///     Error::Rejected { code, .. } => assert_eq!(code, "not_found"),
///     other => panic!("{other:?}"),
/// }
/// ```
pub fn map_order_cmd_err(err: BcsApiError) -> Error {
    map_command_err(err)
}
