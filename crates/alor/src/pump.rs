//! WS event pumps: parse in the receiver task, never block on a slow consumer.
//!
//! The frames of the WebSocket session come in through a channel. The pump gives each one to its core and hands
//! the events to the [`Emitter`]: per-subscription buffers in front of the bot's channel. A bot that reads
//! slowly fills the buffer of a subscription, and the policy of that subscription applies: `Conflate` keeps the
//! latest, `DropNewest` counts drops, `Lossless` overflows (`Event::Overflow`, connector `Degraded`).

use std::sync::Arc;
use std::time::{Duration, Instant};

use brokapi_alor_protocol::ws::models::WsIncoming;
use brokapi_runtime::Emitter;
use brokapi_sansio::{BrokerCore, EventSink};
use kanal::AsyncReceiver;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

/// How often the events that wait in the buffers are offered to the bot's channel again.
const FLUSH_EVERY: Duration = Duration::from_millis(20);

/// Runs an event pump until `cancel` fires or the frame channel closes.
///
/// Every frame from `rx` goes through `core` and the resulting events to `emitter`. `before` runs first for each
/// frame (the user data connector uses it to look up what a frame needs). Events that a slow bot left in the
/// subscription buffers are offered again every 20 ms.
pub async fn run_pump<C, F, Fut>(
    core: Arc<Mutex<C>>,
    emitter: Arc<Emitter>,
    rx: AsyncReceiver<WsIncoming>,
    cancel: CancellationToken,
    before: F,
) where
    C: BrokerCore<Frame = WsIncoming>,
    F: Fn(WsIncoming) -> Fut,
    Fut: std::future::Future<Output = WsIncoming>,
{
    loop {
        let backlog = emitter.has_backlog();
        tokio::select! {
            _ = cancel.cancelled() => break,
            msg = rx.recv() => {
                let Ok(msg) = msg else { break };
                let msg = before(msg).await;
                let now: Instant = brokapi_runtime::now();
                let mut sink = EventSink::new();
                core.lock().await.on_frame(msg, now, &mut sink);
                emitter.emit(sink.drain());
            }
            _ = tokio::time::sleep(FLUSH_EVERY), if backlog => emitter.flush(),
        }
    }
}

/// Gives the bot's channel time to take what the buffers still hold (shutdown).
pub async fn drain_backlog(emitter: &Emitter, deadline: Duration) {
    let end = brokapi_runtime::now() + deadline;
    while emitter.has_backlog() && brokapi_runtime::now() < end {
        emitter.flush();
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}
