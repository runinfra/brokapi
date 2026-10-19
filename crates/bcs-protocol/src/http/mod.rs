//! The REST side: the authorized client, one method per endpoint, and the models.
//!
//! * [`client`]: [`BcsHttpClient`](client::BcsHttpClient), tokens and the generic calls.
//! * [`endpoints`]: the typed endpoint methods, documented on the client type.
//! * [`models`]: request and response models.

pub mod client;
pub mod endpoints;
pub mod models;
