//! WS event pumps: parse in the receiver task, never block on a slow consumer.
//!
//! The frames of the WebSocket sessions come in through a channel. The pump gives each one to its core
//! and hands the events to the [`Emitter`]: per-subscription buffers in front of the bot's channel
//!. A bot that reads slowly fills the buffer of a subscription, and the policy of that
//! subscription applies: `Conflate` keeps the latest, `DropNewest` counts drops, `Lossless` overflows
//! (`Event::Overflow`, connector `Degraded`).

use std::sync::Arc;
use std::time::{Duration, Instant};

use brokapi_bcs_protocol::models::messages::IncomingMessage;
use brokapi_runtime::Emitter;
use brokapi_sansio::{BrokerCore, EventSink};
use kanal::AsyncReceiver;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::core::{MdCore, UdCore};

/// How often the events that wait in the buffers are offered to the bot's channel again.
const FLUSH_EVERY: Duration = Duration::from_millis(20);

/// Runs the market data event pump until `cancel` fires or the frame channel closes.
///
/// Every frame from `rx` goes through `core` and the resulting events to `emitter`. Events that a
/// slow bot left in the subscription buffers are offered again every 20 ms.
pub async fn run_md_pump(
    core: Arc<Mutex<MdCore>>,
    emitter: Arc<Emitter>,
    rx: AsyncReceiver<IncomingMessage>,
    cancel: CancellationToken,
) {
    run_pump(core, emitter, rx, cancel, |c, msg, now, sink| {
        c.on_frame(msg, now, sink)
    })
    .await;
}

/// Runs the user data event pump until `cancel` fires or the frame channel closes.
///
/// Same loop as [`run_md_pump`], with the user data core (order status, fills, portfolio).
pub async fn run_ud_pump(
    core: Arc<Mutex<UdCore>>,
    emitter: Arc<Emitter>,
    rx: AsyncReceiver<IncomingMessage>,
    cancel: CancellationToken,
) {
    run_pump(core, emitter, rx, cancel, |c, msg, now, sink| {
        c.on_incoming(msg, now, sink)
    })
    .await;
}

async fn run_pump<C>(
    core: Arc<Mutex<C>>,
    emitter: Arc<Emitter>,
    rx: AsyncReceiver<IncomingMessage>,
    cancel: CancellationToken,
    step: fn(&mut C, IncomingMessage, Instant, &mut EventSink),
) {
    loop {
        let backlog = emitter.has_backlog();
        tokio::select! {
            _ = cancel.cancelled() => break,
            msg = rx.recv() => {
                let Ok(msg) = msg else { break };
                let now = Instant::now();
                let mut sink = EventSink::new();
                step(&mut *core.lock().await, msg, now, &mut sink);
                emitter.emit(sink.drain());
            }
            _ = tokio::time::sleep(FLUSH_EVERY), if backlog => emitter.flush(),
        }
    }
}

/// Gives the bot's channel time to take what the buffers still hold (shutdown).
pub async fn drain_backlog(emitter: &Emitter, deadline: Duration) {
    let end = Instant::now() + deadline;
    while emitter.has_backlog() && Instant::now() < end {
        emitter.flush();
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}
