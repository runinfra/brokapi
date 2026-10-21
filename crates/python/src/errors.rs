//! `brokapi_core::Error` -> a Python exception.
//!
//! The classes live in Python (`brokapi/errors.py`), so that they can be documented, subclassed and caught by name.
//! This module only says which variant it is, with the text that Rust prints and the fields of the variant. The
//! Python function `brokapi.errors._from_native` builds the exception.

use std::time::Duration;

use brokapi::core::{ConfigError, Error, TransportError};
use pyo3::prelude::*;
use pyo3::types::PyDict;

/// What a native call can fail with: an error of the library, or a bad argument from the Python side.
#[derive(Debug)]
pub enum CallError {
    /// An error of the connector. Becomes an exception of `brokapi.errors`.
    Broker(Error),
    /// An argument that the Python layer should not have sent. Becomes `ValueError`.
    Arg(String),
    /// The client was closed. Becomes `RuntimeError`.
    Closed,
}

impl From<Error> for CallError {
    fn from(e: Error) -> Self {
        CallError::Broker(e)
    }
}

impl From<serde_json::Error> for CallError {
    fn from(e: serde_json::Error) -> Self {
        CallError::Arg(e.to_string())
    }
}

/// A configuration problem, as a library error.
pub fn config(msg: impl Into<String>) -> Error {
    Error::Config(ConfigError::Message(msg.into()))
}

/// The variant name and the fields that Python needs, besides the text.
fn parts(e: &Error) -> (&'static str, Vec<(&'static str, Field)>) {
    match e {
        Error::Config(inner) => ("Config", vec![("message", Field::Str(inner.to_string()))]),
        Error::Transport(inner) => (
            "Transport",
            vec![("message", Field::Str(inner.to_string()))],
        ),
        Error::RateLimited { retry_after } => (
            "RateLimited",
            vec![("retry_after_ns", Field::Int(nanos(*retry_after)))],
        ),
        Error::Rejected { code, message } => (
            "Rejected",
            vec![
                ("code", Field::Str(code.clone())),
                ("message", Field::Str(message.clone())),
            ],
        ),
        Error::Unknown => ("Unknown", vec![]),
        Error::Unsupported { capability } => (
            "Unsupported",
            vec![("capability", Field::Str(capability.to_string()))],
        ),
        Error::NotReconciled => ("NotReconciled", vec![]),
        Error::Degraded => ("Degraded", vec![]),
        Error::ShuttingDown => ("ShuttingDown", vec![]),
    }
}

enum Field {
    Str(String),
    Int(i128),
}

fn nanos(d: Duration) -> i128 {
    d.as_nanos() as i128
}

/// The Python exception for a library error.
pub fn to_pyerr(py: Python<'_>, e: &Error) -> PyErr {
    let (variant, fields) = parts(e);
    let build = || -> PyResult<PyErr> {
        let dict = PyDict::new(py);
        for (key, value) in fields {
            match value {
                Field::Str(s) => dict.set_item(key, s)?,
                Field::Int(n) => dict.set_item(key, n)?,
            }
        }
        let module = py.import("brokapi.errors")?;
        let exc = module
            .getattr("_from_native")?
            .call1((variant, e.to_string(), dict))?;
        Ok(PyErr::from_value(exc))
    };
    build().unwrap_or_else(|err| err)
}

impl CallError {
    pub fn into_pyerr(self) -> PyErr {
        match self {
            CallError::Broker(e) => Python::attach(|py| to_pyerr(py, &e)),
            CallError::Arg(msg) => pyo3::exceptions::PyValueError::new_err(msg),
            CallError::Closed => pyo3::exceptions::PyRuntimeError::new_err("the client is closed"),
        }
    }
}

/// One sample of every variant: the texts the Python errors must repeat.
pub fn samples() -> Vec<(String, String)> {
    let list: Vec<(&str, Error)> = vec![
        (
            "Config",
            Error::Config(ConfigError::Message("token is empty".into())),
        ),
        (
            "Transport",
            Error::Transport(TransportError::Message("connection reset".into())),
        ),
        (
            "RateLimited:1s",
            Error::RateLimited {
                retry_after: Duration::from_secs(1),
            },
        ),
        (
            "RateLimited:250ms",
            Error::RateLimited {
                retry_after: Duration::from_millis(250),
            },
        ),
        (
            "RateLimited:1.5s",
            Error::RateLimited {
                retry_after: Duration::from_millis(1500),
            },
        ),
        (
            "RateLimited:2m",
            Error::RateLimited {
                retry_after: Duration::from_secs(120),
            },
        ),
        (
            "RateLimited:15us",
            Error::RateLimited {
                retry_after: Duration::from_micros(15),
            },
        ),
        (
            "Rejected",
            Error::Rejected {
                code: "price_off_step".into(),
                message: "price 10.005 is not a multiple of the price step 0.01".into(),
            },
        ),
        ("Unknown", Error::Unknown),
        (
            "Unsupported",
            Error::Unsupported {
                capability: "stop_order_stream".into(),
            },
        ),
        ("NotReconciled", Error::NotReconciled),
        ("Degraded", Error::Degraded),
        ("ShuttingDown", Error::ShuttingDown),
    ];
    list.into_iter()
        .map(|(name, e)| (name.to_string(), e.to_string()))
        .collect()
}
