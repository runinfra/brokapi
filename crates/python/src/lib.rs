//! The native half of `brokapi` for Python.
//!
//! This crate is small on purpose. It starts the Rust connectors and passes calls and events across as JSON text:
//! the types of `brokapi-core` already serialize with `serde` (prices and quantities as decimal strings), and the
//! Python package (`python/brokapi`) turns the text into dataclasses with `decimal.Decimal`, so no number ever
//! passes through a `float`. The asyncio side is built by `pyo3-async-runtimes`: every `async fn` of the connectors
//! becomes an awaitable of the running event loop, and the tokio runtime that drives them lives in this module.
//!
//! The module is `brokapi._native` and is private: users import `brokapi`.

use pyo3::prelude::*;

mod client;
mod errors;
#[cfg(feature = "testing")]
mod mocks;
mod record;

/// Texts of the `Display` of sample errors, for a test that keeps the Python messages equal to the Rust ones.
#[pyfunction]
fn error_samples() -> Vec<(String, String)> {
    errors::samples()
}

/// Texts of the `Display` of sample log errors, for a test that keeps the Python messages equal to the Rust ones.
#[pyfunction]
fn log_error_samples() -> Vec<(String, String)> {
    errors::log_samples()
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<client::NativeClient>()?;
    m.add_class::<client::NativePager>()?;
    m.add_class::<client::NativeLimiter>()?;
    m.add_function(wrap_pyfunction!(client::connect, m)?)?;
    m.add_class::<record::NativeLogWriter>()?;
    m.add_class::<record::NativeLogReader>()?;
    m.add_class::<record::NativeReplay>()?;
    m.add_function(wrap_pyfunction!(error_samples, m)?)?;
    m.add_function(wrap_pyfunction!(log_error_samples, m)?)?;
    m.add("TESTING", cfg!(feature = "testing"))?;
    m.add("CORE_VERSION", env!("CARGO_PKG_VERSION"))?;
    #[cfg(feature = "testing")]
    {
        m.add_class::<mocks::NativeMock>()?;
        m.add_function(wrap_pyfunction!(mocks::start_mock, m)?)?;
    }
    Ok(())
}
