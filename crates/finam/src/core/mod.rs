//! Sans-io cores: frames in, normalized events out. No sockets, timers or tasks here.
//!
//! A stream task reads gRPC messages and wraps them into frames ([`MdFrame`], [`UdFrame`]). The core
//! turns a frame into events of `brokapi-core` (order books, quotes, orders...) and keeps the state that
//! needs memory: the local order book, the M10 candle being built, the id of seen trades, the
//! order lifecycle. Because the core does no input and output, tests drive it with plain values and a
//! fixed clock.
//!
//! # Examples
//!
//! A quote subscription walked through by hand: no network, no clock besides `Instant::now()`.
//!
//! ```
//! use std::time::Instant;
//! use brokapi_core::{Event, SubscriptionId};
//! use brokapi_finam::core::{MdCore, MdEvent, MdFrame, SubStatus};
//! use brokapi_finam::util::instrument_from_symbol;
//! use brokapi_sansio::{BrokerCore, DesiredSubscription, EventSink, SubscriptionKind, SubscriptionSet};
//! use brokapi_finam_protocol::pb::google::r#type::Decimal;
//! use brokapi_finam_protocol::pb::grpc::tradeapi::v1::marketdata as pb;
//!
//! let sub = SubscriptionId::new(1);
//! let sber = instrument_from_symbol("SBER@MISX");
//! let mut core = MdCore::new();
//! core.apply(&SubscriptionSet::with_subscriptions(vec![DesiredSubscription {
//!     id: sub,
//!     kind: SubscriptionKind::Quotes { instrument: sber },
//! }]));
//! assert_eq!(core.status(sub), Some(SubStatus::Connecting));
//!
//! let mut out = EventSink::new();
//! core.on_frame(MdFrame { sub, event: MdEvent::Connected }, Instant::now(), &mut out);
//! assert_eq!(core.status(sub), Some(SubStatus::Active));
//!
//! let price = |v: &str| Some(Decimal { value: v.into() });
//! let quote = pb::Quote { symbol: "SBER@MISX".into(), bid: price("250.1"), ask: price("250.2"), ..Default::default() };
//! let message = pb::SubscribeQuoteResponse { quote: vec![quote], error: None };
//! core.on_frame(MdFrame { sub, event: MdEvent::Quotes(message) }, Instant::now(), &mut out);
//!
//! let events = out.drain();
//! assert!(matches!(events[0].payload, Event::Quote(_)));
//! ```

pub mod md;
pub mod ud;

pub use md::{MdCore, MdEvent, MdFrame};
pub use ud::{UdCore, UdEvent, UdFrame};

/// State of one subscription as seen by the core.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubStatus {
    /// Registered, stream not open yet.
    Connecting,
    /// Stream open.
    Active,
    /// Stream dropped, being reopened with backoff.
    Reconnecting(String),
    /// Server refuses the subscription (bad symbol, no permission...); retried slowly.
    Failed(String),
    /// Stream is open but carried an in-band error message.
    Erroring(String),
}

/// What a stream task reports about its stream (before it is wrapped into a core frame).
#[derive(Debug, Clone)]
pub enum StreamEvent<M> {
    /// The stream was opened (also after a reopen).
    Connected,
    /// One message of the stream.
    Msg(M),
    /// The stream broke or ended. It is being reopened. The text says why.
    Disconnected(String),
    /// The broker refuses the stream (bad symbol, no permission). It is retried slowly. The text says why.
    Failed(String),
}
