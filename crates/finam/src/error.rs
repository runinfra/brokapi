//! Map protocol errors to public connector errors.
//!
//! Two mappings exist, and the difference matters:
//!
//! - [`map_protocol_err`] for queries and subscriptions. A failure can be retried safely.
//! - [`map_command_err`] for `place` and `cancel`. Anything that may have reached the broker is
//!   `Error::Unknown`, because a retry could create a second order.
//!
//! | gRPC status | Query or stream | Command |
//! |---|---|---|
//! | `RESOURCE_EXHAUSTED` | `Error::RateLimited` | `Error::RateLimited` |
//! | `UNAUTHENTICATED` (and a login the broker refused) | `Error::Config` | `Error::Config` |
//! | `NOT_FOUND` | `Error::Rejected`, code `not_found` | `Error::Rejected`, code `not_found` |
//! | `PERMISSION_DENIED`, `INVALID_ARGUMENT`, `FAILED_PRECONDITION`, `OUT_OF_RANGE`, `ALREADY_EXISTS`, `UNIMPLEMENTED` | `Error::Rejected` | `Error::Rejected` |
//! | anything else (`UNAVAILABLE`, `DEADLINE_EXCEEDED`, `INTERNAL`, ...) | `Error::Transport` | `Error::Unknown` |
//!
//! # Examples
//!
//! ```
//! use brokapi_core::Error;
//! use brokapi_finam::error::{map_command_err, map_protocol_err};
//! use brokapi_finam_protocol::FinamError;
//! use tonic::Code;
//!
//! let lost = FinamError::Status { code: Code::Unavailable, message: "connection reset".into() };
//! // A read can simply be repeated.
//! assert!(matches!(map_protocol_err(&lost), Error::Transport(_)));
//! // A command may or may not have been applied.
//! assert!(matches!(map_command_err(&lost), Error::Unknown));
//!
//! let refused = FinamError::Status { code: Code::InvalidArgument, message: "bad price".into() };
//! assert!(matches!(map_command_err(&refused), Error::Rejected { .. }));
//! ```

use std::time::Duration;

use brokapi_core::{ConfigError, Error, TransportError};
use brokapi_finam_protocol::FinamError;
use tonic::Code;

/// Errors of queries and subscriptions. Commands use [`map_command_err`].
///
/// `RESOURCE_EXHAUSTED` becomes [`Error::RateLimited`] with a fixed `retry_after` of one second. Finam
/// documents the quota (200 requests per minute) but not the wait, so the pause is a guess, not
/// verified against the live API.
pub fn map_protocol_err(err: &FinamError) -> Error {
    match err {
        FinamError::Config(m) => Error::Config(ConfigError::Message(m.clone())),
        // A refused login is a problem of the secret, as at the other brokers: `Config`.
        FinamError::Auth(m) => Error::Config(ConfigError::Message(format!("unauthenticated: {m}"))),
        FinamError::Status { code, message } => match code {
            // Finam's documented quota is 200 requests per minute; the wait is a guess.
            Code::ResourceExhausted => Error::RateLimited {
                retry_after: Duration::from_secs(1),
            },
            Code::Unauthenticated => {
                Error::Config(ConfigError::Message(format!("unauthenticated: {message}")))
            }
            Code::PermissionDenied
            | Code::NotFound
            | Code::InvalidArgument
            | Code::FailedPrecondition
            | Code::OutOfRange
            | Code::AlreadyExists
            | Code::Unimplemented => Error::Rejected {
                code: code_name(*code).into(),
                message: message.clone(),
            },
            _ => Error::Transport(TransportError::Message(format!(
                "{}: {message}",
                code_name(*code)
            ))),
        },
    }
}

/// Errors of `place` / `cancel`: anything that may have reached the broker is `Unknown`
///, never `Transport` or `Rejected`.
///
/// Only a status where the server certainly did not process the request (see
/// [`FinamError::is_definite_rejection`]) is mapped as in [`map_protocol_err`]. A dropped connection or
/// a timeout is [`Error::Unknown`]: resolve it by asking for the order state, never by sending the
/// command again.
pub fn map_command_err(err: &FinamError) -> Error {
    match err {
        FinamError::Status { .. } if !err.is_definite_rejection() => Error::Unknown,
        _ => map_protocol_err(err),
    }
}

/// Lowercase name of a gRPC status code (`invalid_argument`), used as the `code` of `Error::Rejected`.
pub fn code_name(code: Code) -> &'static str {
    match code {
        Code::Ok => "ok",
        Code::Cancelled => "cancelled",
        Code::Unknown => "unknown",
        Code::InvalidArgument => "invalid_argument",
        Code::DeadlineExceeded => "deadline_exceeded",
        Code::NotFound => brokapi_core::NOT_FOUND,
        Code::AlreadyExists => "already_exists",
        Code::PermissionDenied => "permission_denied",
        Code::ResourceExhausted => "resource_exhausted",
        Code::FailedPrecondition => "failed_precondition",
        Code::Aborted => "aborted",
        Code::OutOfRange => "out_of_range",
        Code::Unimplemented => "unimplemented",
        Code::Internal => "internal",
        Code::Unavailable => "unavailable",
        Code::DataLoss => "data_loss",
        Code::Unauthenticated => "unauthenticated",
    }
}

/// A [`Error::Transport`] with a text. For failures of this process (a broken stream, an unknown
/// subscription), not for answers of the broker.
pub fn transport(msg: impl Into<String>) -> Error {
    Error::Transport(TransportError::Message(msg.into()))
}

/// An [`Error::Unsupported`] naming the capability the broker does not offer (for example `replace_order`).
pub fn unsupported(capability: &'static str) -> Error {
    Error::Unsupported {
        capability: capability.into(),
    }
}
