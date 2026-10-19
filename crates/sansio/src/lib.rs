//! Sans-io core trait and test harness: protocol logic with no sockets, timers or async runtime.
//!
//! A broker connector is split in two. The protocol layer decides what a frame means and what to
//! send next. The runtime owns the sockets and the clock. This crate is the seam between them:
//!
//! - [`BrokerCore`] is the trait a protocol core implements. Frames and the current time go in.
//!   Events ([`EventSink`]), outbound messages ([`Outbound`]) and the next wake-up time come out.
//! - [`Harness`] drives a core with recorded frames and virtual time, so a protocol can be tested
//!   without a network and without waiting.
//! - Helpers every core needs: [`JsonFrameBuffer`] (reassemble JSON split over frames),
//!   [`SubscriptionSet`] (the wanted subscriptions), [`OrderFsm`] and [`OrderRegistry`] (the life
//!   of an order, including the `Unknown` outcome).
//!
//! The crate does no I/O at all, and the examples in it run with no mock. The connectors that
//! use it run against the broker mocks, see `brokapi-mocks`.
//!
//! The book is at <https://brokapi.runinfra.ru>.
//!
//! # Examples
//!
//! Reassemble a JSON message that arrived in two pieces, then track an order through its life:
//!
//! ```
//! use std::time::Instant;
//! use brokapi_core::{BrokerOrderId, ClientOrderId, OrderState};
//! use brokapi_sansio::{BufferPush, JsonFrameBuffer, OrderFsm, OrderSignal};
//!
//! let mut buf = JsonFrameBuffer::new();
//! assert_eq!(buf.push(r#"{"type":"order","#), BufferPush::Incomplete);
//! let BufferPush::Complete(frames) = buf.push(r#""id":"42"}"#) else { panic!("expected a frame") };
//! assert_eq!(frames, vec![r#"{"type":"order","id":"42"}"#.to_string()]);
//!
//! let mut order = OrderFsm::new(ClientOrderId::new_v4(), Instant::now());
//! order.apply(OrderSignal::Ack { broker_id: BrokerOrderId::new("42") }, Instant::now())?;
//! assert!(matches!(order.state(), OrderState::Acked { .. }));
//! # Ok::<(), brokapi_sansio::FsmError>(())
//! ```

#![warn(missing_docs)]

mod core;
mod event;
mod harness;
mod json_buffer;
mod order_fsm;
mod outbound;
mod subscription;

pub use core::BrokerCore;
pub use event::{CoreEvent, EventSink};
pub use harness::{FrameRecord, Harness};
pub use json_buffer::{BufferPush, JsonFrameBuffer};
pub use order_fsm::{FsmError, OrderFsm, OrderRegistry, OrderSignal};
pub use outbound::Outbound;
pub use subscription::{DesiredSubscription, SubscriptionKind, SubscriptionSet};
