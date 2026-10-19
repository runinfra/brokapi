//! Helpers that are not tied to one endpoint.
//!
//! * [`date_format`]: the date-time text format of the API, for `#[serde(with = ...)]`.
//! * [`precision`]: price and quantity arithmetic in whole steps, free of binary rounding noise.
//! * [`rate_limiter`]: a token-bucket limiter to stay below the broker's request limits.

pub mod date_format;
pub mod precision;
pub mod rate_limiter;
