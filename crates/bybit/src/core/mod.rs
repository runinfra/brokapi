//! The sans-io cores of the Bybit connectors.
//!
//! A core is a state machine without sockets, tasks or clocks. It gets inbound frames and the current time, and it
//! pushes events. The connectors in this crate run one core each ([`MdCore`] and [`UdCore`]) inside a pump task. You
//! can also drive a core by hand, for a test or a replay of recorded frames: the trait
//! [`brokapi_sansio::BrokerCore`] is the interface.
//!
//! The stream of Bybit names every data frame with its `topic` (`orderbook.50.BTCUSDT`, `order.linear`) and answers a
//! subscription with a frame that carries the `req_id` of the request. So a core does not guess which subscription a
//! frame is for: the connector registers the topics of each subscription ([`MdCore::register`],
//! [`UdCore::register`]) before it sends the request, and the core looks the frame up.

pub mod md;
pub mod registry;
pub mod ud;

pub use md::MdCore;
pub use registry::{Registry, SubState};
pub use ud::UdCore;
