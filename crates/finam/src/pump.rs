//! Event pump: frames -> core -> per-subscription buffers -> the client's `Sender`.
//!
//! Never blocks on a slow consumer: events wait in the subscription's buffer, and the
//! pump hands them to `out` only when it has room (`reserve`). A `Lossless` buffer that fills up
//! is terminal: one `Event::Overflow`, `on_overflow` (connector goes `Degraded`), later events of
//! that subscription are dropped until `reconcile` clears the state.

use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use brokapi_core::{Envelope, Event, SubscriptionId, Timestamps};
use brokapi_runtime::{BackpressureManager, PushResult};
use brokapi_sansio::CoreEvent;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::util::now_nanos;

/// The per-subscription buffers, shared between the pump and the connector (which registers and
/// unregisters subscriptions in it).
pub type SharedBackpressure = Arc<Mutex<BackpressureManager>>;

/// The channels and handles one [`run_pump`] task works with.
///
/// `F` is the frame type of the connector (`MdFrame` or `UdFrame`).
pub struct PumpIo<F> {
    /// Frames from the stream tasks.
    pub rx: mpsc::Receiver<F>,
    /// Where the envelopes go: the channel the user gave to `start`.
    pub out: mpsc::Sender<Envelope>,
    /// Source label written into every envelope.
    pub source: Arc<str>,
    /// The per-subscription buffers that hold events until `out` has room.
    pub backpressure: SharedBackpressure,
    /// Stops the pump. A child of the connector's root token.
    pub cancel: CancellationToken,
}

/// Events that must reach the consumer in order and must never be conflated away.
fn is_control(ev: &Event) -> bool {
    matches!(ev, Event::Resync { .. } | Event::Overflow { .. })
}

fn event_ts(ev: &Event) -> Timestamps {
    let ts = match ev {
        Event::OrderBook(brokapi_core::OrderBook::Snapshot { ts, .. })
        | Event::OrderBook(brokapi_core::OrderBook::Increment { ts, .. }) => Some(*ts),
        Event::MarketTrade(t) => Some(t.ts),
        Event::Quote(q) => Some(q.ts),
        Event::Candle(c) => Some(c.ts),
        Event::Order(o) => Some(o.ts),
        Event::Execution(e) => Some(e.ts),
        Event::Portfolio(p) => Some(p.ts),
        Event::Positions(v) => v.first().map(|p| p.ts),
        Event::Balances(v) => v.first().map(|b| b.ts),
        _ => None,
    };
    ts.unwrap_or_else(|| Timestamps::new(now_nanos(), brokapi_runtime::now()))
}

struct Egress {
    source: Arc<str>,
    control: VecDeque<Envelope>,
    ready: VecDeque<SubscriptionId>,
    overflowed: HashSet<SubscriptionId>,
}

impl Egress {
    fn envelope(&self, sub: Option<SubscriptionId>, payload: Event) -> Envelope {
        Envelope::new(Arc::clone(&self.source), event_ts(&payload), payload).with_sub_opt(sub)
    }

    fn pending(&self) -> bool {
        !self.control.is_empty() || !self.ready.is_empty()
    }

    /// Next envelope in delivery order: control first, then buffered data.
    fn next(&mut self, bp: &SharedBackpressure) -> Option<Envelope> {
        if let Some(e) = self.control.pop_front() {
            return Some(e);
        }
        while let Some(sub) = self.ready.pop_front() {
            let ev = bp.lock().expect("backpressure poisoned").pop_ready(sub);
            if let Some(ev) = ev {
                return Some(self.envelope(Some(sub), ev));
            }
        }
        None
    }

    fn ingest(
        &mut self,
        ev: CoreEvent,
        bp: &SharedBackpressure,
        on_overflow: &(dyn Fn(SubscriptionId) + Send + Sync),
    ) {
        let CoreEvent { sub, payload } = ev;
        let Some(sub) = sub else {
            let env = self.envelope(None, payload);
            self.control.push_back(env);
            return;
        };
        if is_control(&payload) {
            // A `Resync` says "drop what you hold". What is still waiting in the buffer of this
            // subscription was produced before it, so it goes out first, or the consumer would apply
            // stale data after it had started over. An `Overflow` is different: the buffer is full of
            // events that are already lost for the consumer's purposes, the notice goes out at once.
            if matches!(payload, Event::Resync { .. }) {
                loop {
                    let next = bp.lock().expect("backpressure poisoned").pop_ready(sub);
                    let Some(ev) = next else { break };
                    let env = self.envelope(Some(sub), ev);
                    self.control.push_back(env);
                }
            }
            let env = self.envelope(Some(sub), payload);
            self.control.push_back(env);
            return;
        }
        let lost_from = now_nanos();
        let result = bp
            .lock()
            .expect("backpressure poisoned")
            .ingest(sub, payload, lost_from);
        match result {
            PushResult::Delivered | PushResult::Conflated => {
                // Accepted again: the overflow was cleared (reconcile).
                self.overflowed.remove(&sub);
                self.ready.push_back(sub);
            }
            PushResult::DroppedNewest => {}
            PushResult::Overflow => {
                if self.overflowed.insert(sub) {
                    warn!(?sub, "lossless subscription overflowed: connector degraded");
                    let env = self.envelope(Some(sub), Event::Overflow { sub, lost_from });
                    self.control.push_back(env);
                    on_overflow(sub);
                }
            }
        }
    }
}

/// Runs until cancelled, the frame channel closes, or the consumer drops the receiver.
///
/// `handle` turns one frame into core events (it owns the core, so parsing happens right here in
/// the task that received the frame). `on_overflow` is called once when a `Lossless` buffer
/// overflows. The user data connector uses it to mark itself degraded.
///
/// Delivery order: control events (`Resync`, `Overflow`) first, then buffered data, one
/// subscription after another. A `Resync` does not overtake the data of its own subscription that is
/// already buffered: that data goes out just before it. The pump never waits for a slow consumer inside the core: events wait
/// in the subscription's buffer, and what the buffer's policy says happens when it is full (merge for
/// `Conflate`, overflow for `Lossless`).
///
/// When the pump is cancelled it makes one last pass and hands over what fits into `out` without
/// waiting. The rest is dropped.
///
/// # Examples
///
/// A frame goes in, an envelope with the source label comes out:
///
/// ```
/// use std::sync::{Arc, Mutex};
/// use std::time::{Duration, Instant};
/// use brokapi_core::{Event, ResyncReason, SubscriptionId};
/// use brokapi_finam::core::{MdFrame, MdEvent};
/// use brokapi_finam::pump::{PumpIo, run_pump};
/// use brokapi_runtime::BackpressureManager;
/// use brokapi_sansio::CoreEvent;
/// use tokio::sync::mpsc;
/// use tokio_util::sync::CancellationToken;
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// let (frames, rx) = mpsc::channel::<MdFrame>(8);
/// let (out, mut envelopes) = mpsc::channel(8);
/// let cancel = CancellationToken::new();
/// let io = PumpIo {
///     rx,
///     out,
///     source: Arc::from("finam-md"),
///     backpressure: Arc::new(Mutex::new(BackpressureManager::new())),
///     cancel: cancel.clone(),
/// };
/// // The handler stands in for the connector's core: every frame becomes one control event.
/// let sub = SubscriptionId::new(1);
/// let handler = move |_frame: MdFrame, _now: Instant| {
///     vec![CoreEvent {
///         sub: Some(sub),
///         payload: Event::Resync {
///             sub,
///             instrument: brokapi_finam::util::instrument_from_symbol("SBER@MISX"),
///             reason: ResyncReason::SubscriptionReset,
///         },
///     }]
/// };
/// let pump = tokio::spawn(run_pump(io, handler, Arc::new(|_| {})));
///
/// frames.send(MdFrame { sub, event: MdEvent::Connected }).await.unwrap();
/// let envelope = tokio::time::timeout(Duration::from_secs(5), envelopes.recv()).await.unwrap().unwrap();
/// assert_eq!(&*envelope.source, "finam-md");
/// assert!(matches!(envelope.payload, Event::Resync { .. }));
///
/// cancel.cancel();
/// pump.await.unwrap();
/// # }
/// ```
pub async fn run_pump<F, H>(
    io: PumpIo<F>,
    mut handle: H,
    on_overflow: Arc<dyn Fn(SubscriptionId) + Send + Sync>,
) where
    F: Send + 'static,
    H: FnMut(F, Instant) -> Vec<CoreEvent> + Send,
{
    let PumpIo {
        mut rx,
        out,
        source,
        backpressure,
        cancel,
    } = io;
    let mut eg = Egress {
        source,
        control: VecDeque::new(),
        ready: VecDeque::new(),
        overflowed: HashSet::new(),
    };
    loop {
        // Hand over whatever fits without waiting.
        while eg.pending() {
            match out.try_reserve() {
                Ok(permit) => match eg.next(&backpressure) {
                    Some(env) => permit.send(env),
                    None => break,
                },
                Err(mpsc::error::TrySendError::Full(_)) => break,
                Err(mpsc::error::TrySendError::Closed(_)) => return,
            }
        }
        let want_send = eg.pending();
        tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            permit = out.reserve(), if want_send => match permit {
                Ok(permit) => {
                    if let Some(env) = eg.next(&backpressure) {
                        permit.send(env);
                    }
                }
                Err(_) => return,
            },
            frame = rx.recv() => match frame {
                Some(f) => {
                    let now = brokapi_runtime::now();
                    for ev in handle(f, now) {
                        eg.ingest(ev, &backpressure, on_overflow.as_ref());
                    }
                }
                None => break,
            },
        }
    }
    // Last chance for what is already buffered.
    while eg.pending() {
        match out.try_reserve() {
            Ok(permit) => match eg.next(&backpressure) {
                Some(env) => permit.send(env),
                None => break,
            },
            Err(_) => {
                debug!("pump stopped with undelivered events (consumer not draining)");
                break;
            }
        }
    }
}
