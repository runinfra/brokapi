//! In-process imitations of the three brokers, for tests and examples.
//!
//! **Not part of the library's public promise.** This crate exists so that every example in the
//! documentation (`cargo test --doc`), every program in `examples/` and the integration tests can run
//! against a broker without the network and without money. Its API can change in any release.
//!
//! Each module starts a server on `127.0.0.1` with a random port and speaks the real wire protocol of
//! one broker, so a connector talks to it exactly as it would talk to the broker:
//!
//! | Module | Broker | Wire protocol | Entry point |
//! |---|---|---|---|
//! | [`bcs`] | BCS | HTTP + WebSocket (axum) | [`bcs::MockServer::start`] |
//! | [`tbank`] | T-Bank | gRPC (tonic) | [`tbank::MockServer::start`] |
//! | [`finam`] | Finam | gRPC (tonic) | [`finam::MockServer::start`] |
//!
//! The servers keep real state (placed orders can be listed, cancelled, filled through a test hook), so
//! a scenario written for the mock reads the same as one for the live API. What they do **not** prove
//! is that the live broker behaves the same: the mocks are written from the broker documentation, and
//! each connector's live status is on the readiness page of the book.
//!
//! # Examples
//!
//! ```
//! use std::sync::Arc;
//! use brokapi_mocks::bcs::{MockOptions, MockServer};
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() {
//! let server = MockServer::start(Arc::new(MockOptions::default())).await;
//! assert!(server.http_base.starts_with("http://127.0.0.1:"));
//! # }
//! ```

#![allow(missing_docs)]

pub mod bcs;
pub mod finam;
pub mod tbank;
