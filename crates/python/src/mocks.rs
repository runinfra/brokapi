//! The imitations of the brokers (`brokapi-mocks`) for `brokapi.testing`.
//!
//! A mock is a real server on `127.0.0.1` that speaks the wire protocol of one broker, so a `brokapi` client
//! connects to it exactly as it connects to the broker. What the mocks do not prove: that the live broker answers
//! the same. They are written from the documentation of the brokers.

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use brokapi::core::{CidCodec, ClientOrderId, Price, Qty};
use brokapi_mocks::{alor, bcs, finam, tbank};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use serde_json::json;

enum Server {
    Bcs(bcs::MockServer),
    Tbank(tbank::MockServer),
    Finam(finam::MockServer),
    Alor(alor::MockServer),
}

/// A running imitation of one broker.
#[pyclass(module = "brokapi._native", frozen)]
pub struct NativeMock {
    broker: &'static str,
    server: Mutex<Option<Server>>,
    /// Turns on and off the fault "the broker takes a stop order, the answer is lost".
    lose_stop_answers: Box<dyn Fn(bool) + Send + Sync>,
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
        let (kind, server, lose): (_, _, Box<dyn Fn(bool) + Send + Sync>) = match broker.as_str() {
            "bcs" => {
                let options = Arc::new(bcs::MockOptions::default());
                options
                    .synthetic_candles
                    .store(synthetic_candles, Ordering::Relaxed);
                let lose = Arc::clone(&options);
                (
                    "bcs",
                    Server::Bcs(bcs::MockServer::start(options).await),
                    Box::new(move |on| lose.stop_answer_lost.store(on, Ordering::SeqCst)),
                )
            }
            "tbank" => {
                let options = Arc::new(tbank::MockOptions::default());
                options
                    .synthetic_candles
                    .store(synthetic_candles, Ordering::Relaxed);
                // A stop order gets the time of the real clock, as the broker writes it.
                options.stop_create_now.store(true, Ordering::Relaxed);
                let lose = Arc::clone(&options);
                (
                    "tbank",
                    Server::Tbank(tbank::MockServer::start(options).await),
                    Box::new(move |on| lose.stop_answer_lost.store(on, Ordering::SeqCst)),
                )
            }
            "finam" => {
                let server = finam::MockServer::start().await;
                server
                    .state
                    .options
                    .synthetic_candles
                    .store(synthetic_candles, Ordering::Relaxed);
                let state = Arc::clone(&server.state);
                (
                    "finam",
                    Server::Finam(server),
                    Box::new(move |on| state.options.stop_answer_lost.store(on, Ordering::SeqCst)),
                )
            }
            "alor" => {
                let options = Arc::new(alor::MockOptions::default());
                let lose = Arc::clone(&options);
                (
                    "alor",
                    Server::Alor(alor::MockServer::start(options).await),
                    Box::new(move |on| lose.stop_answer_lost.store(on, Ordering::SeqCst)),
                )
            }
            other => return Err(PyValueError::new_err(format!("no imitation of {other:?}"))),
        };
        Ok(NativeMock {
            broker: kind,
            server: Mutex::new(Some(server)),
            lose_stop_answers: lose,
        })
    })
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
        }
        Ok(())
    }

    /// With `lose == true` the broker takes the next stop orders and the answer is lost: the stop order is at the
    /// broker, the client gets a failure of the connection. `false` returns the usual answers.
    fn lose_stop_answers(&self, lose: bool) -> PyResult<()> {
        if self
            .server
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_none()
        {
            return Err(gone());
        }
        (self.lose_stop_answers)(lose);
        Ok(())
    }

    /// Stops the server. A client still connected to it sees the connection drop.
    fn close(&self) {
        self.server.lock().unwrap_or_else(|e| e.into_inner()).take();
    }
}

/// Finam has no fill hook: push the new state of the order and a trade, as the test of the connector does.
fn fill_finam(
    s: &finam::MockServer,
    account: &str,
    cid: ClientOrderId,
    qty: Qty,
    price: Price,
) -> PyResult<()> {
    use brokapi_mocks::finam::d;
    let comment = brokapi::finam::cid::FinamCidCodec
        .encode(cid)
        .map_err(|e| PyValueError::new_err(e.to_string()))?
        .as_str()
        .to_string();
    let mut order = s
        .state
        .orders_of(account)
        .into_iter()
        .find(|o| o.order.as_ref().is_some_and(|x| x.comment == comment))
        .ok_or_else(|| PyValueError::new_err("the mock has no such order"))?;
    let proto =
        brokapi::finam::protocol::pb::grpc::tradeapi::v1::orders::OrderStatus::Filled as i32;
    let left = d("0");
    order.status = proto;
    order.executed_quantity = d(&qty.to_string());
    order.remaining_quantity = left;
    let inner = order.order.clone().unwrap_or_default();
    let trade = brokapi::finam::protocol::pb::grpc::tradeapi::v1::AccountTrade {
        trade_id: format!("TR-{}", order.order_id),
        symbol: inner.symbol.clone(),
        price: d(&price.to_string()),
        size: d(&qty.to_string()),
        side: inner.side,
        order_id: order.order_id.clone(),
        comment,
        account_id: account.into(),
        ..Default::default()
    };
    s.state.push_order(account, order);
    s.state.push_trade(account, trade);
    Ok(())
}
