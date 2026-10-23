//! The task that feeds frames of the stream to a core and the events of the core to the emitter.

use std::sync::Arc;
use std::time::{Duration, Instant};

use brokapi_bybit_protocol::ws::models::WsIncoming;
use brokapi_runtime::Emitter;
use brokapi_sansio::{BrokerCore, EventSink};
use kanal::AsyncReceiver;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

const FLUSH_EVERY: Duration = Duration::from_millis(20);

/// Reads frames from `rx` until `cancel`, runs them through `core`, and gives the events to `emitter`. `after` runs
/// after every frame with the core locked out, so a connector can act on what the core noted (a book to ask for
/// again).
pub async fn run_pump<C, F, Fut>(
    core: Arc<Mutex<C>>,
    emitter: Arc<Emitter>,
    rx: AsyncReceiver<WsIncoming>,
    cancel: CancellationToken,
    after: F,
) where
    C: BrokerCore<Frame = WsIncoming>,
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    loop {
        let backlog = emitter.has_backlog();
        tokio::select! {
            _ = cancel.cancelled() => break,
            msg = rx.recv() => {
                let Ok(msg) = msg else { break };
                let now: Instant = brokapi_runtime::now();
                let mut sink = EventSink::new();
                core.lock().await.on_frame(msg, now, &mut sink);
                emitter.emit(sink.drain());
                after().await;
            }
            _ = tokio::time::sleep(FLUSH_EVERY), if backlog => emitter.flush(),
        }
    }
}

/// Waits until the emitter has nothing buffered, or `deadline` has passed.
pub async fn drain_backlog(emitter: &Emitter, deadline: Duration) {
    let end = brokapi_runtime::now() + deadline;
    while emitter.has_backlog() && brokapi_runtime::now() < end {
        emitter.flush();
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}
