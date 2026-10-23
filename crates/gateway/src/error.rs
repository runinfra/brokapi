//! The errors of the library on the wire.
//!
//! A failed call is a gRPC status whose trailer `brokapi-error-bin` holds an [`pb::ErrorDetail`]: the whole error, so
//! that a bot gets back the same [`Error`] it would get from the library. The status code is a hint for generic gRPC
//! tools and for logs, the detail is what decides.
//!
//! | Error | gRPC code |
//! |---|---|
//! | `Config` | `INVALID_ARGUMENT` |
//! | `Transport` | `UNAVAILABLE` |
//! | `RateLimited` | `RESOURCE_EXHAUSTED` |
//! | `Rejected` (`not_found`) | `NOT_FOUND` |
//! | `Rejected` (any other code) | `FAILED_PRECONDITION` |
//! | `Unknown` | `UNKNOWN` |
//! | `Unsupported` | `UNIMPLEMENTED` |
//! | `NotReconciled` | `FAILED_PRECONDITION` |
//! | `Degraded` | `ABORTED` |
//! | `ShuttingDown` | `UNAVAILABLE` |

use std::borrow::Cow;
use std::time::Duration;

use brokapi::core::{ConfigError, Error, TransportError};
use prost::Message;
use tonic::metadata::MetadataValue;
use tonic::{Code, Status};

use crate::convert::Invalid;
use crate::pb;
use pb::error_detail::Kind;

/// The name of the trailer with the [`pb::ErrorDetail`].
pub const DETAIL_KEY: &str = "brokapi-error-bin";

/// A status with a detail.
fn status(code: Code, detail: pb::ErrorDetail, text: String) -> Status {
    let mut status = Status::new(code, text);
    status.metadata_mut().insert_bin(
        DETAIL_KEY,
        MetadataValue::from_bytes(&detail.encode_to_vec()),
    );
    status
}

/// The status for an error of the library.
pub fn status_of(e: &Error) -> Status {
    let text = e.to_string();
    let mut detail = pb::ErrorDetail::default();
    let code = match e {
        Error::Config(inner) => {
            detail.kind = Kind::Config as i32;
            detail.message = inner.to_string();
            Code::InvalidArgument
        }
        Error::Transport(inner) => {
            detail.kind = Kind::Transport as i32;
            detail.message = inner.to_string();
            Code::Unavailable
        }
        Error::RateLimited { retry_after } => {
            detail.kind = Kind::RateLimited as i32;
            detail.retry_after_ns = u64::try_from(retry_after.as_nanos()).unwrap_or(u64::MAX);
            Code::ResourceExhausted
        }
        Error::Rejected { code, message } => {
            detail.kind = Kind::Rejected as i32;
            detail.code = code.clone();
            detail.message = message.clone();
            if e.is_not_found() {
                Code::NotFound
            } else {
                Code::FailedPrecondition
            }
        }
        Error::Unknown => {
            detail.kind = Kind::Unknown as i32;
            Code::Unknown
        }
        Error::Unsupported { capability } => {
            detail.kind = Kind::Unsupported as i32;
            detail.capability = capability.to_string();
            Code::Unimplemented
        }
        Error::NotReconciled => {
            detail.kind = Kind::NotReconciled as i32;
            Code::FailedPrecondition
        }
        Error::Degraded => {
            detail.kind = Kind::Degraded as i32;
            Code::Aborted
        }
        Error::ShuttingDown => {
            detail.kind = Kind::ShuttingDown as i32;
            Code::Unavailable
        }
        // An error that a later version adds: the safe reading for a caller is "unknown".
        _ => {
            detail.kind = Kind::Unknown as i32;
            Code::Unknown
        }
    };
    status(code, detail, text)
}

/// The refusal of the gateway itself: it is starting, or the request does not follow the `.proto`. The command was
/// not run.
pub fn gateway_status(kind: Kind, message: impl Into<String>) -> Status {
    let message = message.into();
    let code = match kind {
        Kind::BadRequest => Code::InvalidArgument,
        _ => Code::Unavailable,
    };
    let detail = pb::ErrorDetail {
        kind: kind as i32,
        message: message.clone(),
        ..Default::default()
    };
    status(code, detail, message)
}

/// The status for a request that does not follow the `.proto`.
pub fn invalid_status(e: Invalid) -> Status {
    gateway_status(Kind::BadRequest, e.0)
}

/// The detail of a status, if it has one.
pub fn detail_of(status: &Status) -> Option<pb::ErrorDetail> {
    let value = status.metadata().get_bin(DETAIL_KEY)?;
    let bytes = value.to_bytes().ok()?;
    pb::ErrorDetail::decode(bytes.as_ref()).ok()
}

/// The error of the library that a failed call stands for.
///
/// A status without a detail did not come from the gateway's handling of the call: the connection broke, a deadline
/// passed, a proxy answered. For a `command` (it changes something at the broker) that means nobody knows whether it
/// was run, so it is [`Error::Unknown`] and must not be sent again. A refusal that is certain not to have run the
/// command (a wrong access key) is a configuration error.
pub fn error_of(status: &Status, command: bool) -> Error {
    let Some(d) = detail_of(status) else {
        return match status.code() {
            Code::Unauthenticated | Code::PermissionDenied => Error::Config(ConfigError::Message(
                format!("the gateway refused the access: {}", status.message()),
            )),
            Code::InvalidArgument | Code::Unimplemented => Error::Config(ConfigError::Message(
                format!("the gateway refused the request: {}", status.message()),
            )),
            _ if command => Error::Unknown,
            _ => Error::Transport(TransportError::Message(format!(
                "gateway: {}",
                status.message()
            ))),
        };
    };
    match Kind::try_from(d.kind) {
        Ok(Kind::Config) => Error::Config(ConfigError::Message(d.message)),
        Ok(Kind::Transport) => Error::Transport(TransportError::Message(d.message)),
        Ok(Kind::RateLimited) => Error::RateLimited {
            retry_after: Duration::from_nanos(d.retry_after_ns),
        },
        Ok(Kind::Rejected) => Error::Rejected {
            code: d.code,
            message: d.message,
        },
        Ok(Kind::Unknown) => Error::Unknown,
        Ok(Kind::Unsupported) => Error::Unsupported {
            capability: Cow::Owned(d.capability),
        },
        Ok(Kind::NotReconciled) => Error::NotReconciled,
        Ok(Kind::Degraded) => Error::Degraded,
        Ok(Kind::ShuttingDown) => Error::ShuttingDown,
        Ok(Kind::GatewayStarting) => {
            Error::Transport(TransportError::Message(format!("gateway: {}", d.message)))
        }
        Ok(Kind::BadRequest) => Error::Config(ConfigError::Message(format!(
            "the gateway refused the request: {}",
            d.message
        ))),
        // A kind of a later version: the safe reading for a caller is "unknown".
        Ok(Kind::Unspecified) | Err(_) => Error::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn samples() -> Vec<Error> {
        vec![
            Error::Config(ConfigError::Message("token is empty".into())),
            Error::Transport(TransportError::Message("connection reset".into())),
            Error::RateLimited {
                retry_after: Duration::from_millis(1500),
            },
            Error::Rejected {
                code: "price_off_step".into(),
                message: "price 10.005 is not a multiple of the price step 0.01".into(),
            },
            Error::not_found("no such order"),
            Error::Unknown,
            Error::Unsupported {
                capability: Cow::Borrowed("stop_order_stream"),
            },
            Error::NotReconciled,
            Error::Degraded,
            Error::ShuttingDown,
        ]
    }

    #[test]
    fn every_error_comes_back_the_same() {
        for e in samples() {
            let status = status_of(&e);
            // Over the wire the status is a code, a text and the trailer: nothing else survives.
            let back = error_of(&status, true);
            assert_eq!(back.to_string(), e.to_string(), "{e:?}");
            assert_eq!(back.class(), e.class(), "{e:?}");
            assert_eq!(
                std::mem::discriminant(&back),
                std::mem::discriminant(&e),
                "{e:?}"
            );
            // A read and a command read the detail the same way.
            assert_eq!(error_of(&status, false).to_string(), e.to_string());
        }
    }

    #[test]
    fn the_codes_tell_generic_tools_what_happened() {
        let code = |e: Error| status_of(&e).code();
        assert_eq!(code(Error::Unknown), Code::Unknown);
        assert_eq!(code(Error::not_found("x")), Code::NotFound);
        assert_eq!(code(Error::NotReconciled), Code::FailedPrecondition);
        assert_eq!(
            code(Error::RateLimited {
                retry_after: Duration::ZERO
            }),
            Code::ResourceExhausted
        );
        assert_eq!(code(Error::Degraded), Code::Aborted);
    }

    #[test]
    fn a_failure_of_the_road_is_unknown_for_a_command_and_transport_for_a_read() {
        // The connection broke: no detail, because the gateway did not answer.
        let cut = Status::unavailable("transport error");
        assert!(matches!(error_of(&cut, true), Error::Unknown));
        assert!(matches!(error_of(&cut, false), Error::Transport(_)));
        let late = Status::deadline_exceeded("deadline");
        assert!(matches!(error_of(&late, true), Error::Unknown));
        // A refusal that is certain not to have run the command is not unknown.
        let wrong_key = Status::unauthenticated("key");
        assert!(matches!(error_of(&wrong_key, true), Error::Config(_)));
    }

    #[test]
    fn the_refusals_of_the_gateway_itself_did_not_run_the_command() {
        let starting = gateway_status(Kind::GatewayStarting, "the gateway is starting");
        assert_eq!(starting.code(), Code::Unavailable);
        assert!(matches!(error_of(&starting, true), Error::Transport(_)));
        let bad = invalid_status(Invalid("qty is missing".into()));
        assert_eq!(bad.code(), Code::InvalidArgument);
        assert!(matches!(error_of(&bad, true), Error::Config(m) if m.to_string().contains("qty")));
    }

    #[test]
    fn an_error_kind_of_a_later_version_reads_as_unknown() {
        let mut status = Status::internal("?");
        let detail = pb::ErrorDetail {
            kind: 999,
            ..Default::default()
        };
        status.metadata_mut().insert_bin(
            DETAIL_KEY,
            MetadataValue::from_bytes(&detail.encode_to_vec()),
        );
        assert!(matches!(error_of(&status, false), Error::Unknown));
    }
}
