//! Protocol-layer errors.
//!
//! [`FinamError`] is what this crate returns. The connector crate (`brokapi-finam`) maps it to the
//! public `brokapi_core::Error`: a gRPC status that may have reached the broker becomes
//! `Error::Unknown` for commands.

use thiserror::Error;
use tonic::Code;

/// Error of the Finam protocol layer: a gRPC status, a configuration problem or a refused login.
///
/// The `Display` text never contains the API secret or a token.
///
/// # Examples
///
/// ```
/// use brokapi_finam_protocol::FinamError;
/// use tonic::Code;
///
/// let err = FinamError::from(tonic::Status::new(Code::ResourceExhausted, "too many requests"));
/// assert_eq!(err.code(), Some(Code::ResourceExhausted));
/// assert!(err.is_rate_limited());
/// // The server said "no" before doing anything: a command was not applied.
/// assert!(err.is_definite_rejection());
///
/// // A dropped connection may have happened after the request was written.
/// let lost = FinamError::from(tonic::Status::new(Code::Unavailable, "connection reset"));
/// assert!(!lost.is_definite_rejection());
/// ```
#[derive(Error, Debug, Clone)]
pub enum FinamError {
    /// gRPC status returned by the server (or synthesized by the transport).
    #[error("grpc {code:?}: {message}")]
    Status {
        /// gRPC status code. A failed connection also arrives as a status, the code depends on the failure.
        code: Code,
        /// Text sent by the server (or made up by the transport layer).
        message: String,
    },
    /// Bad endpoint, empty secret, malformed token payload.
    #[error("config: {0}")]
    Config(String),
    /// Authentication failed (bad secret, token refused).
    #[error("auth: {0}")]
    Auth(String),
}

impl From<tonic::Status> for FinamError {
    fn from(s: tonic::Status) -> Self {
        Self::Status {
            code: s.code(),
            message: s.message().to_string(),
        }
    }
}

impl FinamError {
    /// gRPC status code, or `None` for [`FinamError::Config`] and [`FinamError::Auth`].
    pub fn code(&self) -> Option<Code> {
        match self {
            Self::Status { code, .. } => Some(*code),
            _ => None,
        }
    }

    /// `true` for status `UNAUTHENTICATED`: the JWT is missing, wrong or expired.
    ///
    /// A read may renew the token and repeat. A command must not be repeated automatically.
    pub fn is_unauthenticated(&self) -> bool {
        self.code() == Some(Code::Unauthenticated)
    }

    /// `true` for status `RESOURCE_EXHAUSTED`: the request quota is spent and the request was not run.
    pub fn is_rate_limited(&self) -> bool {
        self.code() == Some(Code::ResourceExhausted)
    }

    /// The server certainly did not process the request (so a command was NOT applied).
    ///
    /// Conservative: `Unavailable` is NOT here, because a connection can drop after the request
    /// was written. For commands every other failure is an unknown outcome.
    pub fn is_definite_rejection(&self) -> bool {
        matches!(
            self.code(),
            Some(
                Code::InvalidArgument
                    | Code::NotFound
                    | Code::PermissionDenied
                    | Code::FailedPrecondition
                    | Code::Unauthenticated
                    | Code::ResourceExhausted
                    | Code::OutOfRange
                    | Code::Unimplemented
                    | Code::AlreadyExists
            )
        )
    }
}
