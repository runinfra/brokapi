//! Connector runtime: the building blocks every broker connector shares.
//!
//! This is the layer between the broker-specific protocol code and the capability traits of
//! [`brokapi_core`]. It holds the parts that do not depend on a broker and do no network I/O, so
//! they are the same for BCS, T-Bank and Finam and can be tested with plain values.
//!
//! | Need | Type |
//! |---|---|
//! | Stay under the broker's rate limits | [`BrokerLimiter`], [`BrokerLimits`], [`BucketCfg`], [`ConnQuota`] |
//! | Refuse trading before the first reconcile, after an overflow, during shutdown | [`CommandGate`] |
//! | Compare the client's order ledger with the broker | [`reconcile_ledger`] |
//! | Send events to the consumer without blocking, with a loss policy per subscription | [`Emitter`], [`BackpressureManager`], [`SubscriptionBuffer`], [`PushResult`] |
//! | Hold `subscribe_*` until the broker has confirmed the stream | [`wait_ack`], [`AckState`] |
//! | Never send the same place twice, track unanswered orders | [`OrderRuntime`] |
//! | Finish a command even when the caller stops waiting | [`run_detached`] |
//! | One command at a time per instrument, in order | [`InstrumentLocks`] |
//! | Refuse an order price off the price step before sending | [`PriceGrid`] |
//! | Decide when to resubscribe or reconnect | [`Supervisor`], [`ConnectionState`], [`ConnectionHealth`] |
//! | Stop cleanly | [`ShutdownController`] |
//! | Read a long candle history within the broker's span and rate limits | [`history::CandlePages`], [`history::load_candles`] |
//! | Record the event stream to a file and play it back | [`record::EnvelopeWriter`], [`record::EnvelopeReader`], [`record::replay`] |
//!
//! Limits belong to the broker user, not to a connector: build one [`BrokerLimiter`] per user and
//! share its [`Arc`](std::sync::Arc) between all connectors of that user.
//!
//! The book is at <https://brokapi.ru/docs/>.
//!
//! # Examples
//!
//! A trading command passes two checks before it is sent: the command gate and the rate limiter.
//!
//! ```
//! use std::time::Instant;
//! use brokapi_core::Error;
//! use brokapi_runtime::{BrokerLimiter, BrokerLimits, CommandGate};
//!
//! # let raw = r#"
//! # [demo.buckets.orders]
//! # rate = 10
//! # per = "1s"
//! # burst = 1
//! # [demo.route]
//! # PlaceOrder = ["orders"]
//! # [demo.conns]
//! # max_conns = 2
//! # max_subs_per_conn = 50
//! # supports_multiplex = false
//! # "#;
//! let limiter = BrokerLimiter::new(&BrokerLimits::from_toml_str(raw)?);
//! let mut gate = CommandGate::new();
//!
//! // Before the first reconcile no command may go out.
//! assert!(matches!(gate.check_command(), Err(Error::NotReconciled)));
//! gate.mark_reconciled();
//!
//! // Now the gate is open and the limiter lets the first order through. The second one, sent
//! // at once, is refused locally. Nothing reached the broker.
//! let now = Instant::now();
//! assert!(gate.check_command().is_ok());
//! assert!(limiter.acquire("PlaceOrder", now).is_ok());
//! assert!(matches!(limiter.acquire("PlaceOrder", now), Err(Error::RateLimited { .. })));
//! # Ok::<(), brokapi_core::ConfigError>(())
//! ```

#![warn(missing_docs)]

mod ack;
mod backpressure;
mod clock;
mod command_gate;
mod connection;
mod detached;
mod emitter;
mod health;
pub mod history;
mod instrument_queue;
mod limits;
mod metrics;
mod orders;
mod price_grid;
mod reconcile;
pub mod record;
mod shutdown;
mod supervisor;

pub use ack::{AckState, SUBSCRIPTION_REFUSED, not_confirmed, wait_ack};
pub use backpressure::{BackpressureManager, PushResult, SubscriptionBuffer};
pub use clock::now;
pub use command_gate::CommandGate;
pub use connection::{ConnectionState, SubState};
pub use detached::run_detached;
pub use emitter::Emitter;
pub use health::{ConnectionHealth, HealthClass};
pub use instrument_queue::{BOUND_CIDS, InstrumentGuard, InstrumentLocks};
pub use limits::{
    BrokerLimiter, BrokerLimits, BucketCfg, BucketId, ConnQuota, DEFAULT_ROUTE, MethodId,
};
pub use metrics::RuntimeMetrics;
pub use orders::{FinishedWindow, OrderRuntime, RETAINED_FINISHED, RETENTION};
pub use price_grid::PriceGrid;
pub use reconcile::reconcile_ledger;
pub use shutdown::ShutdownController;
pub use supervisor::{ReconcileAction, Supervisor};
