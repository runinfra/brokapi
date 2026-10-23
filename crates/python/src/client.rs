//! The connection to one broker account, for Python: the objects that the Python package wraps.
//!
//! Python sees a handful of objects: [`NativeClient`] (all calls go through [`NativeClient::call`] by method name,
//! JSON in, JSON out), [`NativePager`] (a long candle history, window by window) and [`NativeLimiter`] (the rate
//! limit counter of one broker area, shared by clients that must share it). The connection itself, the connectors
//! and the calls are `brokapi-client`; this file only turns them into awaitables of the asyncio loop.

use std::sync::Arc;
use std::time::Duration;

use brokapi::core::ShutdownMode;
use brokapi_client::{Broker, CandlePager, Client, Limiter, Settings};
use pyo3::prelude::*;
use serde_json::{Value, json};
use tokio::sync::Mutex;

use crate::errors::{CallError, config, into_pyerr};

/// The rate limit counter of one broker area. A clone is the same counter.
#[pyclass(module = "brokapi._native", frozen, from_py_object)]
#[derive(Clone)]
pub struct NativeLimiter {
    limiter: Limiter,
}

#[pymethods]
impl NativeLimiter {
    #[getter]
    fn broker(&self) -> &'static str {
        self.limiter.broker().as_str()
    }
}

fn broker_kind(name: &str) -> Result<Broker, CallError> {
    name.parse::<Broker>().map_err(CallError::Broker)
}

/// `connect(broker, name, settings_json, limiter, event_buffer)`: an awaitable of a [`NativeClient`].
#[pyfunction]
#[pyo3(signature = (broker, name, settings, limiter=None, event_buffer=8192))]
pub fn connect<'py>(
    py: Python<'py>,
    broker: String,
    name: String,
    settings: String,
    limiter: Option<NativeLimiter>,
    event_buffer: usize,
) -> PyResult<Bound<'py, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let go = async {
            let kind = broker_kind(&broker)?;
            let settings = Settings::parse(&settings)?;
            let limiter = match limiter {
                Some(l) if l.limiter.broker() == kind => Some(l.limiter),
                Some(l) => {
                    return Err(CallError::Broker(config(format!(
                        "this limiter counts the limits of {}, not of {kind}: a limiter is shared only inside one broker",
                        l.limiter.broker()
                    ))));
                }
                None => None,
            };
            let client = Client::connect(&broker, name, &settings, limiter, event_buffer).await?;
            Ok::<_, CallError>(NativeClient { client })
        };
        go.await.map_err(into_pyerr)
    })
}

/// The connection. Every method is an awaitable of the running asyncio loop.
#[pyclass(module = "brokapi._native", frozen)]
pub struct NativeClient {
    client: Client,
}

#[pymethods]
impl NativeClient {
    #[getter]
    fn broker(&self) -> &'static str {
        self.client.broker().as_str()
    }

    #[getter]
    fn name(&self) -> String {
        self.client.name().to_owned()
    }

    #[getter]
    fn limiter(&self) -> NativeLimiter {
        NativeLimiter {
            limiter: self.client.limiter(),
        }
    }

    /// One call by method name, arguments as a JSON object, result as JSON text.
    fn call<'py>(
        &self,
        py: Python<'py>,
        method: String,
        args: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let args: Value = serde_json::from_str(&args)
                .map_err(|e| CallError::Arg(format!("arguments of {method}: {e}")))
                .map_err(into_pyerr)?;
            // A command runs to its end even if the caller stops waiting: `Client::call` takes care of it.
            client.call(&method, &args).await.map_err(into_pyerr)
        })
    }

    /// The next event as JSON text, or `None` when no connector can send any more (after `close`).
    fn next_event<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            match client.next_event().await {
                Some(env) => serde_json::to_string(&env)
                    .map(Some)
                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string())),
                None => Ok(None),
            }
        })
    }

    /// Events that are already in the channel, without waiting. For a loop that wants to empty the queue.
    fn drain_events(&self, limit: usize) -> PyResult<Vec<String>> {
        self.client
            .drain_events(limit)
            .iter()
            .map(|env| {
                serde_json::to_string(env)
                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
            })
            .collect()
    }

    /// Stops both connectors. `deadline` is the seconds the graceful stop may take, `None` for an immediate stop.
    /// The result is JSON with the two shutdown reports.
    #[pyo3(signature = (deadline=None))]
    fn close<'py>(&self, py: Python<'py>, deadline: Option<f64>) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mode = match deadline {
                Some(secs) if secs > 0.0 => ShutdownMode::Graceful {
                    deadline: Duration::from_secs_f64(secs),
                },
                _ => ShutdownMode::Immediate,
            };
            let report = match client.close(mode).await {
                Some(report) => json!({
                    "md": serde_json::to_value(report.md).unwrap_or(Value::Null),
                    "ud": serde_json::to_value(report.ud).unwrap_or(Value::Null),
                }),
                None => Value::Null,
            };
            Ok(report.to_string())
        })
    }

    /// A reader of candle history, window by window. `args`: instrument, timeframe, start, end and the options.
    fn candle_pager(&self, args: String) -> PyResult<NativePager> {
        let args: Value =
            serde_json::from_str(&args).map_err(|e| into_pyerr(CallError::Arg(e.to_string())))?;
        let pager = self
            .client
            .candle_pages_from_json(&args)
            .map_err(into_pyerr)?;
        Ok(NativePager {
            pager: Arc::new(Mutex::new(pager)),
        })
    }
}

/// A candle history read window by window ([`CandlePages`](brokapi::runtime::history::CandlePages) of the Rust library).
#[pyclass(module = "brokapi._native", frozen)]
pub struct NativePager {
    pager: Arc<Mutex<CandlePager>>,
}

#[pymethods]
impl NativePager {
    /// The next window as JSON (`bars`, `resume_from`), or `None` when the range is read.
    /// An error carries `resume_from_ns`: the start of the window that failed.
    fn next_page<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let pager = self.pager.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            match pager.lock().await.next_page().await {
                Ok(None) => Ok(None),
                Ok(Some(page)) => {
                    let out = json!({
                        "bars": page.bars,
                        "resume_from": page.resume_from.as_nanos(),
                    });
                    Ok(Some(out.to_string()))
                }
                Err(e) => {
                    let ns = e.resume_from.map(|t| t.as_nanos());
                    let err = into_pyerr(e.error);
                    if let Some(ns) = ns {
                        Python::attach(|py| {
                            let _ = err.value(py).setattr("resume_from_ns", ns);
                        });
                    }
                    Err(err)
                }
            }
        })
    }
}
