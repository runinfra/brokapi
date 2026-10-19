//! The WebSocket side: the client, the background session and the frame models.
//!
//! * [`client`]: [`BcsWsClient`](client::BcsWsClient) opens sessions, one per channel.
//! * [`session`]: [`WsSession`](session::WsSession) keeps one channel alive: reconnect, heartbeat,
//!   restoring subscriptions.
//! * [`models`]: the frames the broker sends and the commands you send.
//! * [`parse_incoming`]: turns one text frame into an
//!   [`IncomingMessage`](crate::models::messages::IncomingMessage).

pub mod client;
pub mod models;
mod parser;

pub use parser::parse_incoming;
pub mod session;
