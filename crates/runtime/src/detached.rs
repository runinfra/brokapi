//! Run a command to its end even when the caller stops waiting.

use std::future::Future;

/// Runs `work` as a task of its own and waits for its result. If the caller's future is dropped
/// (a `select!` branch lost, a timeout around the call, a cancelled task), `work` is **not** stopped: it
/// runs to its end.
///
/// A trading command needs this. After the cid is marked and the order is registered as in flight, the
/// request, the answer and the update of the order must all happen. A command that stopped halfway would
/// leave the order in flight for ever, or the bot with a placed order it never heard about.
///
/// Returns `None` only when the task was torn down with the runtime (it panicked or the runtime is
/// shutting down). A caller maps that to an unknown outcome.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use std::sync::atomic::{AtomicBool, Ordering};
/// use std::time::Duration;
/// use brokapi_runtime::run_detached;
///
/// # #[tokio::main(flavor = "current_thread", start_paused = true)]
/// # async fn main() {
/// let done = Arc::new(AtomicBool::new(false));
/// let flag = Arc::clone(&done);
/// let work = async move {
///     tokio::time::sleep(Duration::from_secs(5)).await;
///     flag.store(true, Ordering::SeqCst);
///     7
/// };
///
/// // The caller gives up after one second ...
/// let waited = tokio::time::timeout(Duration::from_secs(1), run_detached(work)).await;
/// assert!(waited.is_err());
/// assert!(!done.load(Ordering::SeqCst));
///
/// // ... and the work still finishes.
/// tokio::time::sleep(Duration::from_secs(10)).await;
/// assert!(done.load(Ordering::SeqCst));
///
/// // Waited for to the end, it returns the value.
/// assert_eq!(run_detached(async { 1 + 1 }).await, Some(2));
/// # }
/// ```
pub async fn run_detached<T, F>(work: F) -> Option<T>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    tokio::spawn(work).await.ok()
}
