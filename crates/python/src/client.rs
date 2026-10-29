//! The connection to one broker account, for Python: the objects that the Python package wraps.
//!
//! Python sees a handful of objects: [`NativeClient`] (all calls go through [`NativeClient::call`] by method name,
//! JSON in, JSON out), [`NativePager`] (a long candle history, window by window) and [`NativeLimiter`] (the rate
//! limit counter of one broker area, shared by clients that must share it). The connection itself, the connectors
//! and the calls are `brokapi-client`; this file only turns them into awaitables of the asyncio loop.
//!
//! A [`NativeClient`] is the connection to a broker, or to a gateway: the Python class above it is the same for both.
//! Behind a gateway the client is a `RemoteClient` of `brokapi-gateway`, the calls go through the same
//! `brokapi_client::call_by_name`, and the events come from its `Feed`, so the rules of events, gaps and reconcile are
//! the ones of the Rust crate and not a second copy.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use brokapi::core::{Envelope, Error, ShutdownMode, Timestamps, UnixNanos};
use brokapi_client::{
    Broker, CandlePage, CandlePageError, CandlePager, Client, Limiter, Settings, call_by_name,
};
use brokapi_gateway::{ConnectOptions, Feed, FeedItem, RemoteClient, pb};
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

/// `limiter_from_toml(broker, text)`: a counter with limits of your own, the `[broker]` section of a limits file.
#[pyfunction]
pub fn limiter_from_toml(broker: String, text: String) -> PyResult<NativeLimiter> {
    let make = || {
        let kind = broker_kind(&broker)?;
        let limits = brokapi::runtime::BrokerLimits::from_toml_section(&text, kind.as_str())
            .map_err(|e| CallError::Broker(config(format!("limits of {kind}: {e}"))))?;
        Ok::<_, CallError>(NativeLimiter {
            limiter: Limiter::with_limits(kind, &limits),
        })
    };
    make().map_err(into_pyerr)
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
    crate::runtime::future_into_py(py, async move {
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
            Ok::<_, CallError>(NativeClient {
                backend: Backend::Direct(client),
            })
        };
        go.await.map_err(into_pyerr)
    })
}

/// `open_tbank_sandbox_account(settings, money)`: an awaitable of the id of a new account in the sandbox of T-Bank, with
/// `money` roubles paid into it (text, or `None` for none). `settings` are the settings of `connect` as JSON.
#[pyfunction]
#[pyo3(signature = (settings, money=None))]
pub fn open_tbank_sandbox_account<'py>(
    py: Python<'py>,
    settings: String,
    money: Option<String>,
) -> PyResult<Bound<'py, PyAny>> {
    crate::runtime::future_into_py(py, async move {
        let go = async {
            let cfg = Settings::parse(&settings)?.tbank()?;
            let money = match money {
                Some(m) => Some(m.trim().parse::<brokapi::core::Decimal>().map_err(|_| {
                    CallError::Broker(config(format!(
                        "money must be a number of roubles, got {m:?}"
                    )))
                })?),
                None => None,
            };
            let account = brokapi::tbank::sandbox::open_sandbox_account(&cfg, money).await?;
            Ok::<_, CallError>(account)
        };
        go.await.map_err(into_pyerr)
    })
}

/// `connect_gateway(dir, bot, ready_timeout, event_buffer, own_orders_only)`: an awaitable of a [`NativeClient`] that
/// reaches the account through the gateway whose directory is `dir`.
#[pyfunction]
#[pyo3(signature = (dir, bot, ready_timeout=30.0, event_buffer=8192, own_orders_only=false))]
pub fn connect_gateway<'py>(
    py: Python<'py>,
    dir: PathBuf,
    bot: String,
    ready_timeout: f64,
    event_buffer: usize,
    own_orders_only: bool,
) -> PyResult<Bound<'py, PyAny>> {
    crate::runtime::future_into_py(py, async move {
        let go = async {
            // The guard of commands is on: the refusals (`NotReconciled`, `Degraded`) are those of a broker connection.
            let options = ConnectOptions {
                trading_gate: true,
                ..ConnectOptions::default()
            };
            let remote = RemoteClient::connect_dir_with(&dir, &bot, options).await?;
            let health = remote
                .wait_ready(Duration::from_secs_f64(ready_timeout.max(0.0)))
                .await?;
            let broker: Broker = health.broker.parse()?;
            // Learns the longest span of a history request (and finds out early that the key is wrong).
            remote.caps().await?;
            let feed = remote.feed(event_buffer, own_orders_only).await?;
            Ok::<_, CallError>(NativeClient {
                backend: Backend::Gateway(Arc::new(Gw {
                    remote,
                    gateway: health.name.clone(),
                    broker,
                    source: format!("{}-gateway", health.name),
                    feed: Mutex::new(feed),
                })),
            })
        };
        go.await.map_err(into_pyerr)
    })
}

/// What is behind a [`NativeClient`].
enum Backend {
    /// The connection to a broker.
    Direct(Client),
    /// The connection to a gateway.
    Gateway(Arc<Gw>),
}

/// Where a call goes.
enum Target {
    Direct(Client),
    Gateway(RemoteClient),
}

/// A client of a gateway.
struct Gw {
    remote: RemoteClient,
    gateway: String,
    broker: Broker,
    /// The `source` of the events that the gateway client makes itself (a gap).
    source: String,
    feed: Mutex<Feed>,
}

impl Gw {
    /// An item of the feed as the JSON text of an `Envelope`. A gap has no counterpart in the events of a connector, so
    /// it is an envelope whose payload is `{"Gap": {...}}`: the Python package knows it.
    fn text(&self, item: FeedItem) -> Result<String, serde_json::Error> {
        match item {
            FeedItem::Event(envelope) => serde_json::to_string(&envelope),
            FeedItem::Gap(gap) => {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX));
                let ts = Timestamps::new(UnixNanos::from_nanos(now), std::time::Instant::now());
                // Any event will do: the payload is replaced.
                let mut value = serde_json::to_value(Envelope::new(
                    Arc::from(self.source.as_str()),
                    ts,
                    brokapi::core::Event::Balances(Vec::new()),
                ))?;
                let reason = match pb::gap::Reason::try_from(gap.reason) {
                    Ok(pb::gap::Reason::TooOld) => "too_old",
                    Ok(pb::gap::Reason::NewEpoch) => "new_epoch",
                    Ok(pb::gap::Reason::Ahead) => "ahead",
                    _ => "unknown",
                };
                value["payload"] = json!({"Gap": {
                    "reason": reason,
                    "requested_after": gap.requested_after,
                    "first_available": gap.first_available,
                }});
                Ok(value.to_string())
            }
        }
    }
}

/// The connection. Every method is an awaitable of the running asyncio loop.
#[pyclass(module = "brokapi._native", frozen)]
pub struct NativeClient {
    backend: Backend,
}

fn bad_json(e: serde_json::Error) -> PyErr {
    pyo3::exceptions::PyRuntimeError::new_err(e.to_string())
}

#[pymethods]
impl NativeClient {
    #[getter]
    fn broker(&self) -> &'static str {
        match &self.backend {
            Backend::Direct(client) => client.broker().as_str(),
            Backend::Gateway(gw) => gw.broker.as_str(),
        }
    }

    #[getter]
    fn name(&self) -> String {
        match &self.backend {
            Backend::Direct(client) => client.name().to_owned(),
            Backend::Gateway(gw) => gw.remote.bot().to_owned(),
        }
    }

    /// The name of the gateway, `None` for a connection to a broker.
    #[getter]
    fn gateway(&self) -> Option<String> {
        match &self.backend {
            Backend::Direct(_) => None,
            Backend::Gateway(gw) => Some(gw.gateway.clone()),
        }
    }

    #[getter]
    fn limiter(&self) -> PyResult<NativeLimiter> {
        match &self.backend {
            Backend::Direct(client) => Ok(NativeLimiter {
                limiter: client.limiter(),
            }),
            // The counter is the gateway's, shared by all its bots, and it is not in this process.
            Backend::Gateway(_) => Err(into_pyerr(CallError::Broker(Error::Unsupported {
                capability: "limiter".into(),
            }))),
        }
    }

    /// One call by method name, arguments as a JSON object, result as JSON text.
    fn call<'py>(
        &self,
        py: Python<'py>,
        method: String,
        args: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let target = match &self.backend {
            Backend::Direct(client) => Target::Direct(client.clone()),
            Backend::Gateway(gw) => Target::Gateway(gw.remote.clone()),
        };
        crate::runtime::future_into_py(py, async move {
            let args: Value = serde_json::from_str(&args)
                .map_err(|e| CallError::Arg(format!("arguments of {method}: {e}")))
                .map_err(into_pyerr)?;
            // A command runs to its end even if the caller stops waiting: `call_by_name` takes care of it.
            match target {
                Target::Direct(client) => call_by_name(&client, &method, &args).await,
                Target::Gateway(remote) => call_by_name(&remote, &method, &args).await,
            }
            .map_err(into_pyerr)
        })
    }

    /// The next event as JSON text, or `None` when no connector can send any more (after `close`).
    fn next_event<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match &self.backend {
            Backend::Direct(client) => {
                let client = client.clone();
                crate::runtime::future_into_py(py, async move {
                    match client.next_event().await {
                        Some(env) => serde_json::to_string(&env).map(Some).map_err(bad_json),
                        None => Ok(None),
                    }
                })
            }
            Backend::Gateway(gw) => {
                let gw = Arc::clone(gw);
                crate::runtime::future_into_py(py, async move {
                    let item = gw.feed.lock().await.next().await;
                    item.map(|item| gw.text(item)).transpose().map_err(bad_json)
                })
            }
        }
    }

    /// Events that are already in the channel, without waiting. For a loop that wants to empty the queue.
    fn drain_events(&self, limit: usize) -> PyResult<Vec<String>> {
        match &self.backend {
            Backend::Direct(client) => client
                .drain_events(limit)
                .iter()
                .map(|env| serde_json::to_string(env).map_err(bad_json))
                .collect(),
            Backend::Gateway(gw) => {
                let Ok(mut feed) = gw.feed.try_lock() else {
                    return Ok(Vec::new());
                };
                let mut out = Vec::new();
                while out.len() < limit {
                    let Some(item) = feed.try_next() else { break };
                    out.push(gw.text(item).map_err(bad_json)?);
                }
                Ok(out)
            }
        }
    }

    /// Stops both connectors. `deadline` is the seconds the graceful stop may take, `None` for an immediate stop.
    /// The result is JSON with the two shutdown reports. A gateway client only ends its own connection and the
    /// result is `null`: the gateway and the orders are not touched.
    #[pyo3(signature = (deadline=None))]
    fn close<'py>(&self, py: Python<'py>, deadline: Option<f64>) -> PyResult<Bound<'py, PyAny>> {
        match &self.backend {
            Backend::Direct(client) => {
                let client = client.clone();
                crate::runtime::future_into_py(py, async move {
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
            Backend::Gateway(gw) => {
                gw.remote.close();
                crate::runtime::future_into_py(py, async move { Ok(Value::Null.to_string()) })
            }
        }
    }

    /// A reader of candle history, window by window. `args`: instrument, timeframe, start, end and the options.
    fn candle_pager(&self, args: String) -> PyResult<NativePager> {
        let args: Value =
            serde_json::from_str(&args).map_err(|e| into_pyerr(CallError::Arg(e.to_string())))?;
        let pager = match &self.backend {
            Backend::Direct(client) => {
                Pager::Direct(client.candle_pages_from_json(&args).map_err(into_pyerr)?)
            }
            Backend::Gateway(gw) => Pager::Gateway(
                brokapi_client::candle_pages_from_json(&gw.remote, &args).map_err(into_pyerr)?,
            ),
        };
        Ok(NativePager {
            pager: Arc::new(Mutex::new(pager)),
        })
    }
}

/// A candle history read window by window ([`CandlePages`](brokapi::runtime::history::CandlePages) of the Rust library).
#[pyclass(module = "brokapi._native", frozen)]
pub struct NativePager {
    pager: Arc<Mutex<Pager>>,
}

/// The reader of history of either kind of client.
enum Pager {
    Direct(CandlePager<Client>),
    Gateway(CandlePager<RemoteClient>),
}

impl Pager {
    async fn next_page(&mut self) -> Result<Option<CandlePage>, CandlePageError> {
        match self {
            Pager::Direct(p) => p.next_page().await,
            Pager::Gateway(p) => p.next_page().await,
        }
    }
}

#[pymethods]
impl NativePager {
    /// The next window as JSON (`bars`, `resume_from`), or `None` when the range is read.
    /// An error carries `resume_from_ns`: the start of the window that failed.
    fn next_page<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let pager = self.pager.clone();
        crate::runtime::future_into_py(py, async move {
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
