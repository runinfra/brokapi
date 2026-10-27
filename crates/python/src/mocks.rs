//! The imitations of the brokers (`brokapi-mocks`) for `brokapi.testing`.
//!
//! A mock is a real server on `127.0.0.1` that speaks the wire protocol of one broker, so a `brokapi` client
//! connects to it exactly as it connects to the broker. What the mocks do not prove: that the live broker answers
//! the same. They are written from the documentation of the brokers.

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use brokapi::core::{CidCodec, ClientOrderId, Price, Qty, ShutdownMode};
use brokapi_client::{Broker, BrokerConfig, Settings};
use brokapi_gateway::{Gateway, GatewayConfig};
use brokapi_mocks::{alor, bcs, bybit, finam, tbank};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use serde_json::json;

enum Server {
    Bcs(bcs::MockServer),
    Tbank(tbank::MockServer),
    Finam(finam::MockServer),
    Alor(alor::MockServer),
    Bybit(bybit::MockServer),
}

/// Moves the market of a ticker to a price.
type PriceLever = Box<dyn Fn(&str, brokapi::core::Decimal) + Send + Sync>;

/// The faults of an imitation a test turns on and off. Each broker keeps them in its own options.
struct Levers {
    /// "The broker takes a stop order, the answer is lost".
    lose_stop_answers: Box<dyn Fn(bool) + Send + Sync>,
    /// "The broker takes a new order, the answer is lost".
    lose_order_answers: Box<dyn Fn(bool) + Send + Sync>,
    /// The broker holds a new order this many milliseconds before it answers.
    delay_orders: Box<dyn Fn(u64) + Send + Sync>,
    /// The market of a ticker moves to a price.
    set_price: PriceLever,
}

/// One closure per lever, each over its own handle of the options: `$opts` is an expression of an `Arc`, `$lose_stop`,
/// `$lose_order` and `$delay` the fields of the options.
macro_rules! levers {
    ($opts:expr, $lose_stop:ident, $lose_order:ident, $delay:ident) => {{
        let (a, b, c, d) = (
            Arc::clone(&$opts),
            Arc::clone(&$opts),
            Arc::clone(&$opts),
            Arc::clone(&$opts),
        );
        Levers {
            lose_stop_answers: Box::new(move |on| a.$lose_stop.store(on, Ordering::SeqCst)),
            lose_order_answers: Box::new(move |on| b.$lose_order.store(on, Ordering::SeqCst)),
            delay_orders: Box::new(move |ms| c.$delay.store(ms, Ordering::SeqCst)),
            set_price: Box::new(move |ticker, price| d.prices.set(ticker, price)),
        }
    }};
}

/// A running imitation of one broker.
#[pyclass(module = "brokapi._native", frozen)]
pub struct NativeMock {
    broker: &'static str,
    server: Mutex<Option<Server>>,
    levers: Levers,
}

/// Starts the imitation of `broker` (`"bcs"`, `"tbank"`, `"finam"` or `"alor"`) on a free port.
#[pyfunction]
#[pyo3(signature = (broker, synthetic_candles=false))]
pub fn start_mock(
    py: Python<'_>,
    broker: String,
    synthetic_candles: bool,
) -> PyResult<Bound<'_, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let (kind, server, levers) = match broker.as_str() {
            "bcs" => {
                let options = Arc::new(bcs::MockOptions::default());
                options
                    .synthetic_candles
                    .store(synthetic_candles, Ordering::Relaxed);
                let levers = levers!(options, stop_answer_lost, order_answer_lost, order_delay_ms);
                (
                    "bcs",
                    Server::Bcs(bcs::MockServer::start(options).await),
                    levers,
                )
            }
            "tbank" => {
                let options = Arc::new(tbank::MockOptions::default());
                options
                    .synthetic_candles
                    .store(synthetic_candles, Ordering::Relaxed);
                // A stop order gets the time of the real clock, as the broker writes it.
                options.stop_create_now.store(true, Ordering::Relaxed);
                let levers = levers!(options, stop_answer_lost, order_answer_lost, post_delay_ms);
                (
                    "tbank",
                    Server::Tbank(tbank::MockServer::start(options).await),
                    levers,
                )
            }
            "finam" => {
                let server = finam::MockServer::start().await;
                server
                    .state
                    .options
                    .synthetic_candles
                    .store(synthetic_candles, Ordering::Relaxed);
                let (a, b, c, d) = (
                    Arc::clone(&server.state),
                    Arc::clone(&server.state),
                    Arc::clone(&server.state),
                    Arc::clone(&server.state),
                );
                let levers = Levers {
                    lose_stop_answers: Box::new(move |on| {
                        a.options.stop_answer_lost.store(on, Ordering::SeqCst)
                    }),
                    lose_order_answers: Box::new(move |on| {
                        b.options.order_answer_lost.store(on, Ordering::SeqCst)
                    }),
                    delay_orders: Box::new(move |ms| {
                        c.options.place_delay_ms.store(ms, Ordering::SeqCst)
                    }),
                    set_price: Box::new(move |ticker, price| d.options.prices.set(ticker, price)),
                };
                ("finam", Server::Finam(server), levers)
            }
            "alor" => {
                let options = Arc::new(alor::MockOptions::default());
                let levers = levers!(options, stop_answer_lost, order_answer_lost, order_delay_ms);
                (
                    "alor",
                    Server::Alor(alor::MockServer::start(options).await),
                    levers,
                )
            }
            "bybit" => {
                let options = Arc::new(bybit::MockOptions::default());
                let levers = levers!(options, stop_answer_lost, order_answer_lost, order_delay_ms);
                (
                    "bybit",
                    Server::Bybit(bybit::MockServer::start(options).await),
                    levers,
                )
            }
            other => return Err(PyValueError::new_err(format!("no imitation of {other:?}"))),
        };
        Ok(NativeMock {
            broker: kind,
            server: Mutex::new(Some(server)),
            levers,
        })
    })
}

impl NativeMock {
    fn running(&self) -> PyResult<()> {
        match self
            .server
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
        {
            Some(_) => Ok(()),
            None => Err(gone()),
        }
    }
}

fn gone() -> PyErr {
    PyRuntimeError::new_err("the imitation is stopped")
}

fn dec<T: std::str::FromStr>(text: Option<&str>, what: &str) -> PyResult<T> {
    text.ok_or_else(|| PyValueError::new_err(format!("{what} is required for this broker")))?
        .parse::<T>()
        .map_err(|_| PyValueError::new_err(format!("{what} is not a number")))
}

#[pymethods]
impl NativeMock {
    #[getter]
    fn broker(&self) -> &'static str {
        self.broker
    }

    /// The settings that connect a client to this imitation, as JSON: `endpoint` and the credentials it accepts.
    fn settings(&self) -> PyResult<String> {
        let guard = self.server.lock().unwrap_or_else(|e| e.into_inner());
        let settings = match guard.as_ref().ok_or_else(gone)? {
            Server::Bcs(s) => json!({
                "endpoint": s.http_base,
                "refresh_token": "mock-refresh-token",
                "write": true,
            }),
            Server::Tbank(s) => json!({
                "endpoint": s.endpoint,
                "token": tbank::TOKEN,
                "sandbox": true,
                "account_id": tbank::ACCOUNT_A,
            }),
            Server::Finam(s) => json!({
                "endpoint": s.url,
                "secret": finam::SECRET,
                "account_id": "ACC1",
            }),
            Server::Alor(s) => json!({
                "endpoint": s.http_base,
                "refresh_token": "mock-refresh-token",
                "portfolio": alor::PORTFOLIO,
            }),
            Server::Bybit(s) => json!({
                "endpoint": s.http_base,
                "api_key": bybit::DEFAULT_KEY,
                "api_secret": bybit::SECRET,
            }),
        };
        Ok(settings.to_string())
    }

    /// Executes the order `cid`: it becomes filled (or partly filled), a fill event goes to the account.
    /// BCS, Finam and ALOR use `qty` and `price`. T-Bank fills the whole order at its price and ignores both.
    #[pyo3(signature = (cid, qty=None, price=None, account=None))]
    fn fill(
        &self,
        cid: &str,
        qty: Option<&str>,
        price: Option<&str>,
        account: Option<&str>,
    ) -> PyResult<()> {
        let cid: ClientOrderId = cid
            .parse()
            .map_err(|_| PyValueError::new_err("cid is not a UUID"))?;
        let guard = self.server.lock().unwrap_or_else(|e| e.into_inner());
        match guard.as_ref().ok_or_else(gone)? {
            Server::Bcs(s) => {
                let (qty, price): (Qty, Price) = (dec(qty, "qty")?, dec(price, "price")?);
                s.fill(&cid.hyphenated().to_string(), qty, price);
            }
            Server::Tbank(s) => {
                let account = account.unwrap_or(tbank::ACCOUNT_A);
                let id = cid.hyphenated().to_string();
                // The imitation panics for an order it does not have; a stop order is not one it can execute.
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| s.fill(account, &id)))
                    .map_err(|_| {
                        PyValueError::new_err(
                            "the T-Bank imitation has no such order to execute (it cannot trigger a stop order)",
                        )
                    })?;
            }
            Server::Finam(s) => {
                let (qty, price): (Qty, Price) = (dec(qty, "qty")?, dec(price, "price")?);
                fill_finam(s, account.unwrap_or("ACC1"), cid, qty, price)?;
            }
            Server::Alor(s) => {
                let (qty, price): (Qty, Price) = (dec(qty, "qty")?, dec(price, "price")?);
                // The imitation panics for an order it does not have or one that is not working.
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    s.fill(&cid.hyphenated().to_string(), qty, price)
                }))
                .map_err(|_| {
                    PyValueError::new_err("the ALOR imitation has no such working order to execute")
                })?;
            }
            Server::Bybit(s) => {
                let (qty, price): (Qty, Price) = (dec(qty, "qty")?, dec(price, "price")?);
                // The imitation panics for an order it does not have or one that is not working.
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    s.fill(&cid.hyphenated().to_string(), qty, price)
                }))
                .map_err(|_| {
                    PyValueError::new_err(
                        "the Bybit imitation has no such working order to execute",
                    )
                })?;
            }
        }
        Ok(())
    }

    /// With `lose == true` the broker takes the next stop orders and the answer is lost: the stop order is at the
    /// broker, the client gets a failure of the connection. `false` returns the usual answers.
    fn lose_stop_answers(&self, lose: bool) -> PyResult<()> {
        self.running()?;
        (self.levers.lose_stop_answers)(lose);
        Ok(())
    }

    /// With `lose == true` the broker takes the next new orders (not stop orders) and the answer is lost.
    fn lose_order_answers(&self, lose: bool) -> PyResult<()> {
        self.running()?;
        (self.levers.lose_order_answers)(lose);
        Ok(())
    }

    /// The broker holds every new order `ms` milliseconds before it answers (0: at once).
    fn delay_orders(&self, ms: u64) -> PyResult<()> {
        self.running()?;
        (self.levers.delay_orders)(ms);
        Ok(())
    }

    /// The market of `ticker` moves to `price`: the order book, the last deals and the bars follow it.
    fn set_price(&self, ticker: &str, price: &str) -> PyResult<()> {
        self.running()?;
        let price: Price = dec(Some(price), "price")?;
        (self.levers.set_price)(ticker, price.decimal());
        Ok(())
    }

    /// Cuts the streams of the orders and fills of every account from the broker side. The connector sees the end and
    /// opens them again.
    fn cut_streams(&self) -> PyResult<()> {
        let guard = self.server.lock().unwrap_or_else(|e| e.into_inner());
        match guard.as_ref().ok_or_else(gone)? {
            Server::Bcs(s) => s.cut_event_sockets(),
            Server::Tbank(s) => s.cut_user_streams(),
            Server::Finam(s) => s.state.cut_user_streams(),
            Server::Alor(s) => s.cut_event_sockets(),
            Server::Bybit(s) => s.cut_event_sockets(),
        }
        Ok(())
    }

    /// Starts a gateway on this imitation: an awaitable of a [`NativeMockGateway`]. It is ready when this returns.
    fn start_gateway<'py>(&self, py: Python<'py>, name: String) -> PyResult<Bound<'py, PyAny>> {
        let settings = self.settings()?;
        let broker = self.broker;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let settings =
                Settings::parse(&settings).map_err(|e| PyValueError::new_err(e.to_string()))?;
            let kind: Broker = broker
                .parse()
                .map_err(|e: brokapi::core::Error| PyValueError::new_err(e.to_string()))?;
            let config = BrokerConfig::from_settings(kind, &settings)
                .map_err(|e| PyValueError::new_err(e.to_string()))?;
            let dir = tempfile::tempdir().map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            let gateway = start_gateway(&name, dir.path(), &config).await?;
            Ok(NativeMockGateway {
                inner: Arc::new(MockGateway {
                    name,
                    config,
                    dir,
                    gateway: tokio::sync::Mutex::new(Some(gateway)),
                }),
            })
        })
    }

    /// Stops the server. A client still connected to it sees the connection drop.
    fn close(&self) {
        self.server.lock().unwrap_or_else(|e| e.into_inner()).take();
    }
}

/// Finam names an order by its comment, where the connector writes the cid.
fn fill_finam(
    s: &finam::MockServer,
    account: &str,
    cid: ClientOrderId,
    qty: Qty,
    price: Price,
) -> PyResult<()> {
    let comment = brokapi::finam::cid::FinamCidCodec
        .encode(cid)
        .map_err(|e| PyValueError::new_err(e.to_string()))?
        .as_str()
        .to_string();
    s.state
        .fill(account, &comment, qty.decimal(), price.decimal())
        .map_err(|e| PyValueError::new_err(format!("the Finam imitation cannot execute it: {e}")))
}

async fn start_gateway(
    name: &str,
    dir: &std::path::Path,
    config: &BrokerConfig,
) -> PyResult<Gateway> {
    let gateway = Gateway::start(GatewayConfig::new(name, dir), config.clone(), None)
        .await
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    gateway.ready().await;
    Ok(gateway)
}

/// A gateway that runs in this process on an imitation of a broker, in a directory of its own.
#[pyclass(module = "brokapi._native", frozen)]
pub struct NativeMockGateway {
    inner: Arc<MockGateway>,
}

struct MockGateway {
    name: String,
    config: BrokerConfig,
    dir: tempfile::TempDir,
    gateway: tokio::sync::Mutex<Option<Gateway>>,
}

#[pymethods]
impl NativeMockGateway {
    /// The directory of the gateway: the address and the access key a bot needs.
    #[getter]
    fn directory(&self) -> String {
        self.inner.dir.path().to_string_lossy().into_owned()
    }

    #[getter]
    fn name(&self) -> String {
        self.inner.name.clone()
    }

    /// Ends the gateway the way a crash would and starts a new one in the same directory: the bots see a new process
    /// (a new epoch), and the orders it knew are in its journal.
    fn restart<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = inner.gateway.lock().await;
            if let Some(old) = guard.take() {
                old.kill().await;
            }
            *guard = Some(start_gateway(&inner.name, inner.dir.path(), &inner.config).await?);
            Ok(())
        })
    }

    /// Stops the gateway. The orders at the imitation are not touched.
    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Some(gateway) = inner.gateway.lock().await.take() {
                gateway.shutdown(ShutdownMode::Immediate).await;
            }
            Ok(())
        })
    }
}
