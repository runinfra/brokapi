//! The sans-io cores of the ALOR connectors.
//!
//! A core is a state machine without sockets, tasks or clocks. It gets inbound frames and the current time, and
//! it pushes events. The connectors in this crate run one core each ([`MdCore`] and [`UdCore`]) inside a pump
//! task. You can also drive a core by hand, for a test or a replay of recorded frames: the trait
//! [`brokapi_sansio::BrokerCore`] is the interface.
//!
//! The data channel of ALOR names every frame with the `guid` of the subscription that asked for it, so a core
//! does not guess which subscription a frame is for: the connector registers the `guid` of each subscription
//! ([`MdCore::register`], [`UdCore::register`]) before it sends the request, and the core looks the frame up.

pub mod md;
pub mod ud;

pub use md::{MdCore, SubState};
pub use ud::UdCore;
