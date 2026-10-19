//! The sans-io cores of the BCS connectors.
//!
//! A core is a state machine without sockets, tasks or clocks. It gets inbound frames and the
//! current time, and it pushes events and queues outbound messages. The connectors in this crate run
//! one core each ([`MdCore`] and [`UdCore`]) inside a pump task. You can also drive a core by hand,
//! for a test or a replay of recorded frames: the trait [`brokapi_sansio::BrokerCore`] is the
//! interface.

pub mod md;
pub mod ud;

pub use md::{MdCore, SubState};
pub use ud::{UdCore, UdFrame, UdRestFrame};
