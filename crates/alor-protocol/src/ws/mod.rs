//! The WebSocket side: the subscription messages and the frames ([`models`]), the background session of the
//! data channel `/ws` ([`session`]) and the trading channel `/cws` ([`cws`]).

pub mod cws;
pub mod models;
pub mod session;

pub use models::{WsIncoming, WsSubscribe, parse_incoming};
