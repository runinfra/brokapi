//! What a call by name can fail with, and the helper for a configuration error.

use brokapi::core::{ConfigError, Error};

/// What a call of [`Client`](crate::Client) by method name can fail with: an error of the library, an argument that
/// cannot be read, or a client that is already closed.
///
/// The typed methods of [`Client`](crate::Client) (the traits of `brokapi-core`) fail with [`Error`] only.
#[derive(Debug)]
pub enum ClientError {
    /// An error of the connector, the same [`Error`] a direct use of the library gives.
    Broker(Error),
    /// An argument that cannot be read: a missing key, a wrong type, an unknown method name.
    Arg(String),
    /// The client was closed.
    Closed,
}

impl From<Error> for ClientError {
    fn from(e: Error) -> Self {
        ClientError::Broker(e)
    }
}

impl From<serde_json::Error> for ClientError {
    fn from(e: serde_json::Error) -> Self {
        ClientError::Arg(e.to_string())
    }
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientError::Broker(e) => e.fmt(f),
            ClientError::Arg(msg) => f.write_str(msg),
            ClientError::Closed => f.write_str("the client is closed"),
        }
    }
}

impl std::error::Error for ClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ClientError::Broker(e) => Some(e),
            _ => None,
        }
    }
}

/// A configuration problem, as a library error.
pub fn config_error(msg: impl Into<String>) -> Error {
    Error::Config(ConfigError::Message(msg.into()))
}
