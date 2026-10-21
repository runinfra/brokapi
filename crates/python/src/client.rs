//! The connection to one broker account: market data and user data connectors, the event channel, and the calls.
//!
//! Python sees a handful of objects: [`NativeClient`] (all calls go through [`NativeClient::call`] by method name,
//! JSON in, JSON out), [`NativePager`] (a long candle history, window by window) and [`NativeLimiter`] (the rate
//! limit counter of one broker area, shared by clients that must share it).

use std::sync::Arc;
use std::time::Duration;

use brokapi::core::{
    ClientOrderId, Envelope, ExecutionSource, InstrumentId, InstrumentKey, InstrumentQuery,
    InstrumentSource, MarketDataSource, NativeId, OrderBookSource, OrderExecutor,
    PlaceOrderRequest, PortfolioSource, ReplaceOrderRequest, ShutdownMode, SubscribeOpts,
    SubscriptionId, TimeRange, Timeframe, UnixNanos,
};
use brokapi::runtime::BrokerLimiter;
use brokapi::runtime::history::{CandlePages, HistoryOpts};
use pyo3::prelude::*;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use tokio::sync::{Mutex, RwLock, mpsc};

use crate::errors::{CallError, config};
use crate::settings::Settings;

/// What the market data side of any connector can do.
trait Md: OrderBookSource + MarketDataSource + InstrumentSource {}
impl<T: OrderBookSource + MarketDataSource + InstrumentSource> Md for T {}

/// What the account side of any connector can do.
trait Ud: OrderExecutor + ExecutionSource + PortfolioSource {}
impl<T: OrderExecutor + ExecutionSource + PortfolioSource> Ud for T {}

/// The two connectors of one broker.
enum Conn {
    Bcs {
        md: brokapi::bcs::MarketDataConnector,
        ud: brokapi::bcs::UserDataConnector,
    },
    Tbank {
        md: brokapi::tbank::MarketDataConnector,
        ud: brokapi::tbank::UserDataConnector,
    },
    Finam {
        md: brokapi::finam::MarketDataConnector,
        ud: brokapi::finam::UserDataConnector,
    },
}

impl Conn {
    fn md(&self) -> &dyn Md {
        match self {
            Conn::Bcs { md, .. } => md,
            Conn::Tbank { md, .. } => md,
            Conn::Finam { md, .. } => md,
        }
    }

    fn ud(&self) -> &dyn Ud {
        match self {
            Conn::Bcs { ud, .. } => ud,
            Conn::Tbank { ud, .. } => ud,
            Conn::Finam { ud, .. } => ud,
        }
    }

    /// Tells the account connector the price step of an instrument: a price off the step is then refused before
    /// anything is sent.
    fn learn_grid(&self, params: &brokapi::core::TradingParams) {
        match self {
            Conn::Bcs { ud, .. } => ud.price_grid().learn(params),
            Conn::Tbank { ud, .. } => ud.price_grid().learn(params),
            Conn::Finam { ud, .. } => ud.price_grid().learn(params),
        }
    }

    fn caps(&self) -> Value {
        let (md, ud) = match self {
            Conn::Bcs { md, ud } => (
                serde_json::to_value(md.caps()),
                serde_json::to_value(ud.caps()),
            ),
            Conn::Tbank { md, ud } => (
                serde_json::to_value(md.caps()),
                serde_json::to_value(ud.caps()),
            ),
            Conn::Finam { md, ud } => (
                serde_json::to_value(md.caps()),
                serde_json::to_value(ud.caps()),
            ),
        };
        json!({ "md": md.unwrap_or(Value::Null), "ud": ud.unwrap_or(Value::Null) })
    }

    /// Stops the account side first, then market data, as the Rust examples do.
    async fn shutdown(self, mode: ShutdownMode) -> Value {
        let (md, ud) = match self {
            Conn::Bcs { md, ud } => {
                let u = ud.shutdown(mode).await;
                (md.shutdown(mode).await, u)
            }
            Conn::Tbank { md, ud } => {
                let u = ud.shutdown(mode).await;
                (md.shutdown(mode).await, u)
            }
            Conn::Finam { md, ud } => {
                let u = ud.shutdown(mode).await;
                (md.shutdown(mode).await, u)
            }
        };
        json!({
            "md": serde_json::to_value(md).unwrap_or(Value::Null),
            "ud": serde_json::to_value(ud).unwrap_or(Value::Null),
        })
    }
}

struct Inner {
    broker: &'static str,
    name: String,
    /// `None` after `close`.
    conn: RwLock<Option<Conn>>,
    events: Mutex<mpsc::Receiver<Envelope>>,
    limiter: NativeLimiter,
}

/// The rate limit counter of one broker area. A clone is the same counter.
#[pyclass(module = "brokapi._native", frozen, from_py_object)]
#[derive(Clone)]
pub struct NativeLimiter {
    broker: &'static str,
    limiter: Arc<BrokerLimiter>,
}

#[pymethods]
impl NativeLimiter {
    #[getter]
    fn broker(&self) -> &'static str {
        self.broker
    }
}

fn broker_kind(name: &str) -> Result<&'static str, CallError> {
    match name {
        "bcs" => Ok("bcs"),
        "tbank" => Ok("tbank"),
        "finam" => Ok("finam"),
        other => Err(CallError::Broker(config(format!(
            "unknown broker {other:?}: use \"bcs\", \"tbank\" or \"finam\""
        )))),
    }
}

fn default_limiter(broker: &'static str) -> NativeLimiter {
    let limiter = match broker {
        "bcs" => BrokerLimiter::new(&brokapi::bcs::limits::default_limits()),
        "tbank" => BrokerLimiter::new(&brokapi::tbank::limits::default_limits()),
        _ => BrokerLimiter::new(&brokapi::finam::limits::default_limits()),
    };
    NativeLimiter { broker, limiter }
}

/// Starts both connectors of a broker.
async fn start(
    broker: &'static str,
    name: &str,
    settings: &Settings,
    limiter: Arc<BrokerLimiter>,
    tx: mpsc::Sender<Envelope>,
) -> Result<Conn, brokapi::core::Error> {
    let md_source = format!("{name}-md");
    let ud_source = format!("{name}-ud");
    match broker {
        "bcs" => {
            use brokapi::bcs::{
                MarketDataConfig, MarketDataConnector, UserDataConfig, UserDataConnector,
            };
            let cfg = settings.bcs()?;
            let md = MarketDataConnector::start(
                MarketDataConfig::new(cfg.clone(), md_source),
                limiter.clone(),
                tx.clone(),
            )
            .await?;
            let ud =
                UserDataConnector::start(UserDataConfig::new(cfg, ud_source), limiter, tx).await?;
            Ok(Conn::Bcs { md, ud })
        }
        "tbank" => {
            use brokapi::tbank::{
                MarketDataConfig, MarketDataConnector, UserDataConfig, UserDataConnector,
            };
            let cfg = settings.tbank()?;
            let md = MarketDataConnector::start(
                MarketDataConfig::new(cfg.clone(), md_source),
                limiter.clone(),
                tx.clone(),
            )
            .await?;
            let ud =
                UserDataConnector::start(UserDataConfig::new(cfg, ud_source), limiter, tx).await?;
            Ok(Conn::Tbank { md, ud })
        }
        _ => {
            use brokapi::finam::{
                MarketDataConfig, MarketDataConnector, UserDataConfig, UserDataConnector,
            };
            let (api, account) = settings.finam()?;
            // One JWT for both connectors.
            let api = api.clone().with_tokens(api.shared_tokens().await?);
            let md = MarketDataConnector::start(
                MarketDataConfig::new(api.clone(), md_source),
                limiter.clone(),
                tx.clone(),
            )
            .await?;
            let ud =
                UserDataConnector::start(UserDataConfig::new(api, account, ud_source), limiter, tx)
                    .await?;
            Ok(Conn::Finam { md, ud })
        }
    }
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
            // `.env` in the working directory only (never a parent), like `from_env` of the Rust library.
            dotenvy::from_path(".env").ok();
            let limiter = match limiter {
                Some(l) if l.broker == kind => l,
                Some(l) => {
                    return Err(CallError::Broker(config(format!(
                        "this limiter counts the limits of {}, not of {kind}: a limiter is shared only inside one broker",
                        l.broker
                    ))));
                }
                None => default_limiter(kind),
            };
            let (tx, rx) = mpsc::channel::<Envelope>(event_buffer.max(1));
            let conn = start(kind, &name, &settings, limiter.limiter.clone(), tx).await?;
            Ok::<_, CallError>(NativeClient {
                inner: Arc::new(Inner {
                    broker: kind,
                    name,
                    conn: RwLock::new(Some(conn)),
                    events: Mutex::new(rx),
                    limiter,
                }),
            })
        };
        go.await.map_err(CallError::into_pyerr)
    })
}

/// The connection. Every method is an awaitable of the running asyncio loop.
#[pyclass(module = "brokapi._native", frozen)]
pub struct NativeClient {
    inner: Arc<Inner>,
}

#[pymethods]
impl NativeClient {
    #[getter]
    fn broker(&self) -> &'static str {
        self.inner.broker
    }

    #[getter]
    fn name(&self) -> String {
        self.inner.name.clone()
    }

    #[getter]
    fn limiter(&self) -> NativeLimiter {
        self.inner.limiter.clone()
    }

    /// One call by method name, arguments as a JSON object, result as JSON text.
    fn call<'py>(
        &self,
        py: Python<'py>,
        method: String,
        args: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let args: Value = serde_json::from_str(&args)
                .map_err(|e| CallError::Arg(format!("arguments of {method}: {e}")))
                .map_err(CallError::into_pyerr)?;
            // A command runs to its end even if the caller stops waiting (a Python task cancelled, a timeout):
            // a place that is dropped half way could be at the broker already, and nobody would know.
            let out = if is_command(&method) {
                match tokio::spawn(async move { dispatch(&inner, &method, &args).await }).await {
                    Ok(out) => out,
                    Err(e) => Err(CallError::Arg(format!("the command task failed: {e}"))),
                }
            } else {
                dispatch(&inner, &method, &args).await
            };
            out.map_err(CallError::into_pyerr)
        })
    }

    /// The next event as JSON text, or `None` when no connector can send any more (after `close`).
    fn next_event<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut rx = inner.events.lock().await;
            match rx.recv().await {
                Some(env) => serde_json::to_string(&env)
                    .map(Some)
                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string())),
                None => Ok(None),
            }
        })
    }

    /// Events that are already in the channel, without waiting. For a loop that wants to empty the queue.
    fn drain_events(&self, limit: usize) -> PyResult<Vec<String>> {
        let Ok(mut rx) = self.inner.events.try_lock() else {
            return Ok(Vec::new());
        };
        let mut out = Vec::new();
        while out.len() < limit {
            match rx.try_recv() {
                Ok(env) => out.push(
                    serde_json::to_string(&env)
                        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?,
                ),
                Err(_) => break,
            }
        }
        Ok(out)
    }

    /// Stops both connectors. `deadline` is the seconds the graceful stop may take, `None` for an immediate stop.
    /// The result is JSON with the two shutdown reports.
    #[pyo3(signature = (deadline=None))]
    fn close<'py>(&self, py: Python<'py>, deadline: Option<f64>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mode = match deadline {
                Some(secs) if secs > 0.0 => ShutdownMode::Graceful {
                    deadline: Duration::from_secs_f64(secs),
                },
                _ => ShutdownMode::Immediate,
            };
            let conn = inner.conn.write().await.take();
            let report = match conn {
                Some(conn) => conn.shutdown(mode).await,
                None => Value::Null,
            };
            Ok(report.to_string())
        })
    }

    /// A reader of candle history, window by window. `args`: instrument, timeframe, start, end and the options.
    fn candle_pager(&self, args: String) -> PyResult<NativePager> {
        let args: Value =
            serde_json::from_str(&args).map_err(|e| CallError::Arg(e.to_string()).into_pyerr())?;
        let build = || -> Result<PagerState, CallError> {
            let mut opts = HistoryOpts::default();
            if let Some(w) = args.get("window_secs").and_then(Value::as_f64) {
                opts.window = Some(Duration::from_secs_f64(w));
            }
            if let Some(n) = args.get("max_rate_waits").and_then(Value::as_u64) {
                opts.max_rate_waits = n as u32;
            }
            if let Some(n) = args.get("transport_retries").and_then(Value::as_u64) {
                opts.transport_retries = n as u32;
            }
            Ok(PagerState {
                instrument: get(&args, "instrument")?,
                timeframe: get(&args, "timeframe")?,
                next_from: UnixNanos::from_nanos(get(&args, "start")?),
                to: UnixNanos::from_nanos(get(&args, "end")?),
                last_open: None,
                opts,
            })
        };
        let state = build().map_err(CallError::into_pyerr)?;
        Ok(NativePager {
            inner: self.inner.clone(),
            state: Arc::new(Mutex::new(state)),
        })
    }
}

struct PagerState {
    instrument: InstrumentId,
    timeframe: Timeframe,
    next_from: UnixNanos,
    to: UnixNanos,
    last_open: Option<UnixNanos>,
    opts: HistoryOpts,
}

/// A candle history read window by window ([`CandlePages`] of the Rust library).
#[pyclass(module = "brokapi._native", frozen)]
pub struct NativePager {
    inner: Arc<Inner>,
    state: Arc<Mutex<PagerState>>,
}

#[pymethods]
impl NativePager {
    /// The next window as JSON (`bars`, `resume_from`), or `None` when the range is read.
    /// An error carries `resume_from_ns`: the start of the window that failed.
    fn next_page<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        let state = self.state.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut st = state.lock().await;
            let guard = inner.conn.read().await;
            let conn = guard.as_ref().ok_or_else(closed)?;
            let md = conn.md();
            let range = TimeRange::new(st.next_from, st.to);
            let instrument = st.instrument.clone();
            let mut pages = CandlePages::new(md, &instrument, st.timeframe, range, st.opts);
            match pages.next_page().await {
                Ok(None) => Ok(None),
                Ok(Some(mut bars)) => {
                    if let Some(last) = st.last_open {
                        bars.retain(|c| c.open_ts > last);
                    }
                    if let Some(c) = bars.last() {
                        st.last_open = Some(c.open_ts);
                    }
                    st.next_from = pages.resume_from();
                    let out = json!({ "bars": bars, "resume_from": st.next_from.as_nanos() });
                    Ok(Some(out.to_string()))
                }
                Err(e) => {
                    let ns = e.resume_from.as_nanos();
                    let err = CallError::Broker(e.error).into_pyerr();
                    Python::attach(|py| {
                        let _ = err.value(py).setattr("resume_from_ns", ns);
                    });
                    Err(err)
                }
            }
        })
    }
}

fn closed() -> PyErr {
    pyo3::exceptions::PyRuntimeError::new_err("the client is closed")
}

/// Commands that change something at the broker.
fn is_command(method: &str) -> bool {
    matches!(method, "place" | "cancel" | "replace" | "reconcile")
}

fn get<T: DeserializeOwned>(args: &Value, key: &str) -> Result<T, CallError> {
    let v = args
        .get(key)
        .ok_or_else(|| CallError::Arg(format!("missing argument {key}")))?;
    serde_json::from_value(v.clone()).map_err(|e| CallError::Arg(format!("argument {key}: {e}")))
}

fn get_opt<T: DeserializeOwned>(args: &Value, key: &str) -> Result<Option<T>, CallError> {
    match args.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(v) => serde_json::from_value(v.clone())
            .map(Some)
            .map_err(|e| CallError::Arg(format!("argument {key}: {e}"))),
    }
}

fn range(args: &Value) -> Result<TimeRange, CallError> {
    Ok(TimeRange::new(
        UnixNanos::from_nanos(get(args, "start")?),
        UnixNanos::from_nanos(get(args, "end")?),
    ))
}

fn out<T: Serialize>(value: T) -> Result<String, CallError> {
    Ok(serde_json::to_string(&value)?)
}

/// The instrument of a ticker. BCS and Finam build it from the text, T-Bank looks it up in its directory.
async fn resolve(
    inner: &Inner,
    conn: &Conn,
    ticker: &str,
    board: Option<&str>,
) -> Result<InstrumentId, CallError> {
    let (ticker, board) = match ticker.split_once('@') {
        Some((t, b)) if board.is_none() => (t, Some(b)),
        _ => (ticker, board),
    };
    if ticker.is_empty() {
        return Err(CallError::Arg("the ticker is empty".into()));
    }
    match inner.broker {
        "bcs" => {
            let class = board.unwrap_or("TQBR");
            Ok(InstrumentId::new(
                InstrumentKey::new(ticker, class),
                NativeId::TickerClass {
                    ticker: ticker.into(),
                    class_code: class.into(),
                },
            ))
        }
        "finam" => Ok(brokapi::finam::util::instrument_from_symbol(&format!(
            "{ticker}@{}",
            board.unwrap_or("MISX")
        ))),
        _ => {
            let found = conn
                .md()
                .find(InstrumentQuery::ByTicker {
                    ticker: ticker.into(),
                    exchange: Some(board.unwrap_or("TQBR").into()),
                })
                .await?;
            found.into_iter().next().map(|i| i.id).ok_or_else(|| {
                CallError::Broker(brokapi::core::Error::Rejected {
                    code: "not_found".into(),
                    message: format!("no instrument {ticker} on {}", board.unwrap_or("TQBR")),
                })
            })
        }
    }
}

async fn dispatch(inner: &Inner, method: &str, args: &Value) -> Result<String, CallError> {
    let guard = inner.conn.read().await;
    let conn = guard.as_ref().ok_or(CallError::Closed)?;
    let md = conn.md();
    let ud = conn.ud();
    match method {
        "caps" => out(conn.caps()),
        "instrument" => {
            let ticker: String = get(args, "ticker")?;
            let board: Option<String> = get_opt(args, "board")?;
            out(resolve(inner, conn, &ticker, board.as_deref()).await?)
        }

        // Market data: reads.
        "order_book" => out(md
            .order_book(&get(args, "instrument")?, get(args, "depth")?)
            .await?),
        "last_trades" => out(md
            .last_trades(&get(args, "instrument")?, get(args, "limit")?)
            .await?),
        "candles" => {
            let tf: Timeframe = get(args, "timeframe")?;
            out(md
                .candles(&get(args, "instrument")?, tf, range(args)?)
                .await?)
        }
        "find" => out(md.find(get::<InstrumentQuery>(args, "query")?).await?),
        "get_instrument" => out(md.get(&get(args, "instrument")?).await?),
        "trading_params" => {
            let params = md.trading_params(&get(args, "instrument")?).await?;
            // The price step goes to the account connector, so that an off-step price is refused locally.
            conn.learn_grid(&params);
            out(params)
        }
        "trading_status" => out(md.trading_status(&get(args, "instrument")?).await?),
        "schedule" => {
            let exchange: String = get(args, "exchange")?;
            out(md.schedule(&exchange, range(args)?).await?)
        }

        // Market data: subscriptions.
        "subscribe_order_book" => {
            let opts: SubscribeOpts = get(args, "opts")?;
            out(md
                .subscribe_order_book(&get(args, "instrument")?, opts)
                .await?
                .value())
        }
        "subscribe_trades" => {
            let opts: SubscribeOpts = get(args, "opts")?;
            out(md
                .subscribe_market_trades(&get(args, "instrument")?, opts)
                .await?
                .value())
        }
        "subscribe_quotes" => {
            let opts: SubscribeOpts = get(args, "opts")?;
            out(md
                .subscribe_quotes(&get(args, "instrument")?, opts)
                .await?
                .value())
        }
        "subscribe_candles" => {
            let opts: SubscribeOpts = get(args, "opts")?;
            let tf: Timeframe = get(args, "timeframe")?;
            out(md
                .subscribe_candles(&get(args, "instrument")?, tf, opts)
                .await?
                .value())
        }
        "unsubscribe" => {
            let sub = SubscriptionId::new(get(args, "sub")?);
            md.unsubscribe(sub).await?;
            out(Value::Null)
        }

        // The account: reads and subscriptions.
        "portfolio" => out(ud.portfolio().await?),
        "positions" => out(ud.positions().await?),
        "balances" => out(ud.balances().await?),
        "executions" => out(ud.executions(range(args)?).await?),
        "subscribe_executions" => out(ud.subscribe_executions().await?.value()),
        "subscribe_portfolio" => {
            let opts: SubscribeOpts = get(args, "opts")?;
            out(ud.subscribe_portfolio(opts).await?.value())
        }
        "subscribe_order_state" => out(ud.subscribe_order_state().await?.value()),

        // The account: orders.
        "reconcile" => {
            let ledger: Vec<ClientOrderId> = get(args, "ledger")?;
            out(ud.reconcile(&ledger).await?)
        }
        "place" => out(ud.place(get::<PlaceOrderRequest>(args, "request")?).await?),
        "cancel" => {
            ud.cancel(get(args, "cid")?).await?;
            out(Value::Null)
        }
        "replace" => out(ud
            .replace(get::<ReplaceOrderRequest>(args, "request")?)
            .await?),
        "order_state" => out(ud.order_state(get(args, "cid")?).await?),
        "active_orders" => out(ud.active_orders().await?),

        other => Err(CallError::Arg(format!("unknown method {other:?}"))),
    }
}
