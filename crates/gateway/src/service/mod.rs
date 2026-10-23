//! The service of the program `brokapi`: it runs the gateways of one config file, one process each, watches them and
//! answers the commands.
//!
//! | Part | What |
//! |---|---|
//! | [`config`] | the file `brokapi.toml` and the layout of the state directory |
//! | [`Service`] | the service itself: gateway processes, restarts, the API of the commands |
//! | [`ServiceClient`] | the client of that API, which the commands use |
//! | [`registry`] | the services of the machine |
//! | [`logs`] | log files and reading them |
//! | [`run_gateway`] | the body of a gateway process, `brokapi gateway run` |

pub mod admin;
mod api;
mod client;
pub mod config;
mod gateway_run;
pub mod logs;
pub mod proc;
pub mod registry;
pub mod restart;
mod supervisor;

/// The messages and the service of `proto/brokapi/service/v1/service.proto`, generated.
#[allow(missing_docs, clippy::all)]
pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/service/brokapi.service.v1.rs"));
}

pub use client::{CallError, ServiceClient};
pub(crate) use gateway_run::stop_signal;
pub use gateway_run::{GatewayRunError, run_gateway};
pub use supervisor::{GatewayState, Service, ServiceError, ServiceOptions};
