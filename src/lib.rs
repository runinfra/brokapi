//! One interface to the APIs of Russian brokers: BCS, T-Bank, Finam and ALOR.
//!
//! This is the crate you depend on. It re-exports the library and switches brokers on with
//! features, the way `tokio` does:
//!
//! ```toml
//! [dependencies]
//! brokapi = { version = "0.1.0-alpha.3", features = ["bcs"] }   # or "tbank", "finam", "alor"
//! ```
//!
//! | Path | What is there | Feature |
//! |---|---|---|
//! | [`core`] | domain types, capability traits, events, errors | always |
//! | [`runtime`] | rate limits, subscription buffers, reconcile, shutdown | always |
//! | [`sansio`] | the network-free core of a connector and the recorded-frame test harness | always |
//! | `bcs` | BCS connectors (`MarketDataConnector`, `UserDataConnector`); `bcs::protocol` is the wire client | `bcs` |
//! | `tbank` | T-Bank connectors; `tbank::protocol` is the gRPC client | `tbank` |
//! | `finam` | Finam connectors; `finam::protocol` is the gRPC client | `finam` |
//! | `alor` | ALOR connectors; `alor::protocol` is the HTTP and WebSocket client | `alor` |
//!
//! The guide, the rules for orders and the readiness table are in the book:
//! <https://brokapi.ru/docs/>. The stage is open alpha: checked on broker imitations only.
//!
//! # Example
//!
//! A command with no answer is never repeated: the error says so by its class.
//!
//! ```
//! use brokapi::core::{Error, ErrorClass};
//!
//! assert_eq!(Error::Unknown.class(), ErrorClass::Unknown);
//! ```

#![warn(missing_docs)]

pub use brokapi_core as core;
pub use brokapi_runtime as runtime;
pub use brokapi_sansio as sansio;

/// BCS connectors and the BCS wire client.
#[cfg(feature = "bcs")]
pub mod bcs {
    pub use brokapi_bcs::*;
    /// The BCS REST and WebSocket client (lower layer).
    pub use brokapi_bcs_protocol as protocol;
}

/// T-Bank connectors and the T-Bank gRPC client.
#[cfg(feature = "tbank")]
pub mod tbank {
    pub use brokapi_tbank::*;
    /// The T-Bank Invest API gRPC client (lower layer).
    pub use brokapi_tbank_protocol as protocol;
}

/// Finam connectors and the Finam gRPC client.
#[cfg(feature = "finam")]
pub mod finam {
    pub use brokapi_finam::*;
    /// The Finam Trade API gRPC client (lower layer).
    pub use brokapi_finam_protocol as protocol;
}

/// ALOR connectors and the ALOR wire client.
#[cfg(feature = "alor")]
pub mod alor {
    pub use brokapi_alor::*;
    /// The ALOR HTTP and WebSocket client (lower layer).
    pub use brokapi_alor_protocol as protocol;
}
