//! Protocol-level errors.

use thiserror::Error;

/// Error raised while building the configuration or the channel. No request was sent yet.
///
/// The connectors turn it into `brokapi_core::Error::Config` at start. It never describes a failed
/// call: calls fail with a `tonic::Status`.
#[derive(Error, Debug)]
pub enum ProtocolError {
    /// The configuration cannot be used: the token or the app name has characters a header cannot
    /// carry, or the endpoint is not a valid URL. Fix the value, retrying changes nothing.
    #[error("invalid configuration: {0}")]
    Config(String),
    /// The transport could not be set up (for example the TLS configuration was refused). The
    /// channel connects lazily, so an unreachable server is not reported here but on the first call.
    #[error("transport error: {0}")]
    Transport(#[from] tonic::transport::Error),
}
