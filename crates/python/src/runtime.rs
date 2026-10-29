//! The async bridge and its runtime have one owner, stopped before Python finalization.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3_async_runtimes::{TaskLocals, generic};
use tokio::runtime::{Handle, Runtime};
use tokio::task::JoinHandle;

static OWNER: Mutex<Option<Runtime>> = Mutex::new(None);
static HANDLE: OnceLock<Handle> = OnceLock::new();
static STOPPED: AtomicBool = AtomicBool::new(false);

tokio::task_local! {
    static LOCALS: TaskLocals;
}

/// The handle outlives shutdown, but never keeps the runtime or its threads alive.
pub(crate) fn handle() -> PyResult<&'static Handle> {
    let mut owner = OWNER.lock().unwrap_or_else(|e| e.into_inner());
    if STOPPED.load(Ordering::SeqCst) {
        return Err(PyRuntimeError::new_err("the brokapi runtime is shut down"));
    }
    if owner.is_none() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("brokapi-runtime")
            .build()
            .map_err(|e| {
                PyRuntimeError::new_err(format!("cannot start the brokapi runtime: {e}"))
            })?;
        let handle = runtime.handle().clone();
        *owner = Some(runtime);
        return Ok(HANDLE.get_or_init(|| handle));
    }
    Ok(HANDLE.get().expect("running runtime has a handle"))
}

struct Bridge;

impl generic::Runtime for Bridge {
    type JoinError = tokio::task::JoinError;
    type JoinHandle = JoinHandle<()>;

    fn spawn<F>(future: F) -> Self::JoinHandle
    where
        F: Future<Output = ()> + Send + 'static,
    {
        // The bridge is entered only after handle. Keeping this handle after
        // shutdown lets cancellation finish without trying to recreate the runtime.
        HANDLE
            .get()
            .expect("bridge runtime initialized")
            .spawn(future)
    }

    fn spawn_blocking<F>(function: F) -> Self::JoinHandle
    where
        F: FnOnce() + Send + 'static,
    {
        HANDLE
            .get()
            .expect("bridge runtime initialized")
            .spawn_blocking(function)
    }
}

impl generic::ContextExt for Bridge {
    fn scope<F, R>(locals: TaskLocals, future: F) -> Pin<Box<dyn Future<Output = R> + Send>>
    where
        F: Future<Output = R> + Send + 'static,
    {
        Box::pin(LOCALS.scope(locals, future))
    }

    fn get_task_locals() -> Option<TaskLocals> {
        LOCALS.try_with(Clone::clone).ok()
    }
}

pub(crate) fn future_into_py<F, T>(py: Python<'_>, future: F) -> PyResult<Bound<'_, PyAny>>
where
    F: Future<Output = PyResult<T>> + Send + 'static,
    T: for<'py> IntoPyObject<'py> + Send + 'static,
{
    handle()?;
    generic::future_into_py::<Bridge, _, _>(py, future)
}

/// Wait for async tasks and, crucially, every blocking Python callback to finish.
#[pyfunction]
pub(crate) fn _shutdown(py: Python<'_>) {
    let runtime = {
        let mut owner = OWNER.lock().unwrap_or_else(|e| e.into_inner());
        STOPPED.store(true, Ordering::SeqCst);
        owner.take()
    };
    // Blocking bridge callbacks acquire the GIL. Holding it while joining them
    // would deadlock, while a timeout would let them enter Python after finalization.
    py.detach(move || drop(runtime));
}

pub(crate) fn install(module: &Bound<'_, PyModule>) -> PyResult<()> {
    let shutdown = wrap_pyfunction!(_shutdown, module)?;
    module
        .py()
        .import("atexit")?
        .call_method1("register", (&shutdown,))?;
    module.add_function(shutdown)
}
