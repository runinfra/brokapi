//! Egress buffers in front of the shared channel: the code is shared with BCS, see
//! [`brokapi_runtime::Emitter`].
//!
//! The connectors register one buffer per subscription (lossy `Conflate` or lossless) and put the
//! events into the channel you gave them. The type is re-exported here so that the T-Bank connectors
//! name it from their own crate. Read [`brokapi_runtime::Emitter`] for its behavior.

pub use brokapi_runtime::Emitter;
