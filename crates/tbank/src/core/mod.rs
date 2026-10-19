//! Sans-io cores: protocol state machines without sockets, timers or async runtime.
//!
//! A core never touches the network. The connector owns the streams and feeds each message to the
//! core as a frame, with the current time. The core answers with events (into an
//! [`EventSink`](brokapi_sansio::EventSink)) and with requests to write to the stream. This makes the
//! protocol logic testable without a server: frames in, events and requests out.
//!
//! - [`MdCore`] drives the one bidirectional market data stream.
//! - [`UdCore`] keeps the order lifecycle, own trades and portfolio of one account.
//!
//! # Examples
//!
//! ```
//! use std::time::{Duration, Instant};
//! use brokapi_core::{InstrumentId, InstrumentKey, NativeId, SubscriptionId};
//! use brokapi_sansio::{BrokerCore, DesiredSubscription, EventSink, SubscriptionKind, SubscriptionSet};
//! use brokapi_tbank::core::{MdCore, MdFrame};
//!
//! let mut core = MdCore::new(Duration::from_secs(30));
//! let mut events = EventSink::new();
//!
//! // The runtime tells the core that a stream is open. No socket is involved.
//! core.on_frame(MdFrame::Connected, Instant::now(), &mut events);
//!
//! // The consumer wants an order book. The core queues the wire request.
//! let sber = InstrumentId::new(InstrumentKey::new("SBER", "TQBR"), NativeId::Uid("uid-sber".into()));
//! core.apply(&SubscriptionSet::with_subscriptions(vec![DesiredSubscription {
//!     id: SubscriptionId::new(1),
//!     kind: SubscriptionKind::OrderBook { instrument: sber, depth: 10 },
//! }]));
//!
//! // First the ping settings, then the order book subscription.
//! let mut requests = 0;
//! while core.poll_request().is_some() {
//!     requests += 1;
//! }
//! assert_eq!(requests, 2);
//! assert_eq!(core.wire_count(), 1);
//! ```

pub mod md;
pub mod ud;

pub use md::{MdCore, MdFrame, SubUpdate};
pub use ud::{UdCore, UdFrame};
