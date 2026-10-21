//! The event log: write the envelopes of a run to a file, read them back, play them again.
//!
//! This is `brokapi_runtime::record` of the Rust library, so the file is the same: a header line and one JSON line per
//! envelope. A log written here reads in Rust and the other way round. The envelopes cross the boundary as the JSON
//! text of `Envelope`, as everywhere else in this crate.

use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use brokapi::core::Envelope;
use brokapi::runtime::record::{
    EnvelopeReader, EnvelopeWriter, LogError, ReplayReport, ReplaySpeed, replay,
};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use tokio::sync::{Mutex as AsyncMutex, mpsc};
use tokio::task::JoinHandle;

use crate::errors::log_to_pyerr;

fn closed() -> PyErr {
    pyo3::exceptions::PyValueError::new_err("the log is closed")
}

/// Appends envelopes to a new log file.
#[pyclass(module = "brokapi._native", frozen)]
pub struct NativeLogWriter {
    inner: Mutex<Option<EnvelopeWriter<BufWriter<File>>>>,
}

#[pymethods]
impl NativeLogWriter {
    /// Creates the file and writes the header. Without `overwrite` an existing file is an error.
    #[new]
    #[pyo3(signature = (path, overwrite=false))]
    fn new(path: &str, overwrite: bool) -> PyResult<Self> {
        let mut options = OpenOptions::new();
        options.write(true);
        if overwrite {
            options.create(true).truncate(true);
        } else {
            options.create_new(true);
        }
        let file = options
            .open(path)
            .map_err(|e| log_to_pyerr(&LogError::Io(e)))?;
        let writer = EnvelopeWriter::new(BufWriter::new(file)).map_err(|e| log_to_pyerr(&e))?;
        Ok(Self {
            inner: Mutex::new(Some(writer)),
        })
    }

    /// Appends one envelope, given as its JSON text. The text is parsed as an `Envelope` first, so a line that Rust
    /// could not read is never written.
    fn write(&self, envelope: &str) -> PyResult<()> {
        let envelope: Envelope = serde_json::from_str(envelope)
            .map_err(|e| PyValueError::new_err(format!("not an envelope: {e}")))?;
        let mut guard = self.inner.lock().unwrap();
        let writer = guard.as_mut().ok_or_else(closed)?;
        writer.write(&envelope).map_err(|e| log_to_pyerr(&e))
    }

    /// How many envelopes were written (the header is not counted).
    fn written(&self) -> PyResult<u64> {
        let guard = self.inner.lock().unwrap();
        Ok(guard.as_ref().ok_or_else(closed)?.written())
    }

    fn flush(&self) -> PyResult<()> {
        let mut guard = self.inner.lock().unwrap();
        guard
            .as_mut()
            .ok_or_else(closed)?
            .flush()
            .map_err(|e| log_to_pyerr(&e))
    }

    /// Flushes and closes the file. Safe to call twice.
    fn close(&self) -> PyResult<()> {
        let writer = self.inner.lock().unwrap().take();
        match writer {
            Some(w) => w.finish().map(|_| ()).map_err(|e| log_to_pyerr(&e)),
            None => Ok(()),
        }
    }
}

/// Reads the envelopes of a log, in the order they were written.
#[pyclass(module = "brokapi._native", frozen)]
pub struct NativeLogReader {
    inner: Mutex<EnvelopeReader<BufReader<File>>>,
}

#[pymethods]
impl NativeLogReader {
    /// Opens the file and checks the header.
    #[new]
    fn new(path: &str) -> PyResult<Self> {
        let file = File::open(path).map_err(|e| log_to_pyerr(&LogError::Io(e)))?;
        let reader = EnvelopeReader::new(BufReader::new(file)).map_err(|e| log_to_pyerr(&e))?;
        Ok(Self {
            inner: Mutex::new(reader),
        })
    }

    /// The next envelope as JSON text, or `None` at the end. After an error the reader is at its end.
    fn next_envelope(&self) -> PyResult<Option<String>> {
        let mut reader = self.inner.lock().unwrap();
        match reader.next() {
            None => Ok(None),
            Some(Ok(env)) => serde_json::to_string(&env)
                .map(Some)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string())),
            Some(Err(e)) => Err(log_to_pyerr(&e)),
        }
    }
}

type ReplayTask = JoinHandle<Result<ReplayReport, LogError>>;

struct ReplayState {
    rx: AsyncMutex<mpsc::Receiver<Envelope>>,
    task: Mutex<Option<ReplayTask>>,
}

impl Drop for ReplayState {
    fn drop(&mut self) {
        if let Some(task) = self.task.lock().unwrap().take() {
            task.abort();
        }
    }
}

/// A log played into a channel with `replay` of the Rust library, read one envelope at a time.
#[pyclass(module = "brokapi._native", frozen)]
pub struct NativeReplay {
    state: Arc<ReplayState>,
}

#[pymethods]
impl NativeReplay {
    /// `factor` is `None` for as fast as the reader takes them, else the speed-up of the recorded pace, and
    /// `max_gap` the longest single pause in seconds.
    #[new]
    #[pyo3(signature = (path, factor=None, max_gap=1.0, buffer=256))]
    fn new(path: &str, factor: Option<f64>, max_gap: f64, buffer: usize) -> PyResult<Self> {
        let speed = match factor {
            None => ReplaySpeed::Instant,
            Some(f) if f > 0.0 && f.is_finite() => ReplaySpeed::Paced {
                factor: f,
                max_gap: Duration::from_secs_f64(max_gap.max(0.0)),
            },
            Some(_) => return Err(PyValueError::new_err("the speed must be above zero")),
        };
        let file = File::open(path).map_err(|e| log_to_pyerr(&LogError::Io(e)))?;
        let reader = EnvelopeReader::new(BufReader::new(file)).map_err(|e| log_to_pyerr(&e))?;
        let (tx, rx) = mpsc::channel(buffer.max(1));
        let task = pyo3_async_runtimes::tokio::get_runtime()
            .spawn(async move { replay(reader, &tx, speed).await });
        Ok(Self {
            state: Arc::new(ReplayState {
                rx: AsyncMutex::new(rx),
                task: Mutex::new(Some(task)),
            }),
        })
    }

    /// The next envelope as JSON text, or `None` when the log is played. A bad line ends the replay with the error
    /// after the envelopes before it.
    fn next_envelope<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let state = self.state.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let item = state.rx.lock().await.recv().await;
            match item {
                Some(env) => serde_json::to_string(&env)
                    .map(Some)
                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string())),
                None => {
                    let task = state.task.lock().unwrap().take();
                    match task {
                        Some(handle) => match handle.await {
                            Ok(Ok(_)) => Ok(None),
                            Ok(Err(e)) => Err(log_to_pyerr(&e)),
                            Err(_) => Ok(None),
                        },
                        None => Ok(None),
                    }
                }
            }
        })
    }
}
