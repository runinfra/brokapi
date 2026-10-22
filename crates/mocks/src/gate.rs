//! A gate that holds the opening of a stream, to imitate a broker that is slow to open it.
//!
//! A real broker opens a stream some time after the request: the WebSocket handshake, the subscription
//! confirmation, the first frame. A connector that calls a subscription "done" before that has a window in which
//! an event at the broker is missed. To test the window without a pause in the test, each mock puts this gate in
//! front of the point where the stream becomes live for the client. The test holds the gate, makes the call and
//! moves the clock past the connector's wait (`brokapi_mocks::time::elapse_when`), then releases the gate.
//!
//! ```
//! use std::sync::Arc;
//! use brokapi_mocks::gate::StreamGate;
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() {
//! let gate = Arc::new(StreamGate::default());
//! gate.hold();
//! let held = Arc::clone(&gate);
//! let open = tokio::spawn(async move { held.pass().await });
//! brokapi_mocks::time::wait_until("the open is held", || gate.waiting() == 1).await;
//! gate.release();
//! open.await.unwrap();
//! assert_eq!(gate.waiting(), 0);
//! # }
//! ```

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use tokio::sync::Notify;

/// Holds the requests that come to it while it is closed, and lets them go when it is released.
///
/// Closed with [`hold`](Self::hold), opened with [`release`](Self::release). A request that comes while the gate
/// is open passes at once.
#[derive(Debug, Default)]
pub struct StreamGate {
    held: AtomicBool,
    waiting: AtomicU32,
    open: Notify,
}

/// Counts a held request, and stops counting it when it ends, also when its future is dropped (the client
/// gave up and the server cancelled the request).
struct Waiting<'a>(&'a AtomicU32);

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl StreamGate {
    /// Closes the gate: the requests that come from now on wait in [`pass`](Self::pass).
    pub fn hold(&self) {
        self.held.store(true, Ordering::SeqCst);
    }

    /// Opens the gate and lets every held request go on.
    pub fn release(&self) {
        self.held.store(false, Ordering::SeqCst);
        self.open.notify_waiters();
    }

    /// How many requests are held now. A test waits for it to be above 0 before it moves the clock: the
    /// request has reached the broker.
    pub fn waiting(&self) -> u32 {
        self.waiting.load(Ordering::SeqCst)
    }

    /// The mock calls it at the point where the stream would become live. Returns at once while the gate is
    /// open, otherwise when it is released.
    pub async fn pass(&self) {
        if !self.held.load(Ordering::SeqCst) {
            return;
        }
        self.waiting.fetch_add(1, Ordering::SeqCst);
        let _counted = Waiting(&self.waiting);
        loop {
            let released = self.open.notified();
            tokio::pin!(released);
            // Registered before the flag is read, so a release in between is not missed.
            released.as_mut().enable();
            if !self.held.load(Ordering::SeqCst) {
                return;
            }
            released.await;
        }
    }
}
