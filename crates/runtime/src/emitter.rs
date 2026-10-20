//! Egress: per-subscription buffers in front of the shared `Sender<Envelope>`
//! Shared by the connectors that read their streams in a task of their own (T-Bank, BCS).
//!
//! The reader task never blocks: events go into the subscription's buffer (`BackpressureManager`),
//! then as many as the channel accepts are moved out with `try_send`. A full channel leaves events in
//! the buffer, so a slow consumer fills it and the policy applies: `Conflate` keeps the latest,
//! `DropNewest` counts drops, `Lossless` overflows: `Event::Overflow` is emitted and the connector is
//! `Degraded` (commands are refused) until `reconcile()`.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use brokapi_core::{
    Backpressure, ENVELOPE_SCHEMA, Envelope, Event, InstrumentId, InstrumentKey, NativeId,
    SubscriptionId, Timestamps, UnixNanos,
};
use brokapi_sansio::CoreEvent;
use tokio::sync::mpsc::{self, error::TrySendError};
use tracing::warn;

use crate::backpressure::{BackpressureManager, PushResult};

fn wall_nanos() -> UnixNanos {
    UnixNanos::from_nanos(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0),
    )
}

struct State {
    bp: BackpressureManager,
    subs: Vec<SubscriptionId>,
    /// Popped from the buffer but refused by a full channel; sent first next time.
    pending: HashMap<SubscriptionId, Envelope>,
    overflowed: HashSet<SubscriptionId>,
    conflating: HashSet<SubscriptionId>,
    notices: VecDeque<Envelope>,
}

/// Moves events from a connector's reader task into the consumer's channel, without ever blocking.
///
/// This is the egress half of a connector. Each subscription has a buffer with its
/// [`Backpressure`] policy. [`emit`](Self::emit) puts events into the buffers and moves as many as
/// the channel accepts into it with `try_send`. When the consumer is slow the channel fills and the
/// buffers take the strain:
///
/// | Policy | A slow consumer causes |
/// |---|---|
/// | [`Backpressure::Conflate`] | only the latest value is kept, older ones are replaced |
/// | [`Backpressure::DropNewest`] | new events are thrown away once the buffer is full |
/// | [`Backpressure::Lossless`] | the buffer overflows: an [`Event::Overflow`] notice is queued once, the event is lost, the emitter becomes degraded |
///
/// Degraded means events were lost that must not be lost. The connector copies the flag
/// ([`is_degraded`](Self::is_degraded)) into its command gate, so trading commands fail with
/// [`brokapi_core::Error::Degraded`] until a successful reconcile calls
/// [`clear_degraded`](Self::clear_degraded). Why: after a hole in the order or fill stream the
/// client's picture of its orders is wrong, and trading on it could be harmful.
///
/// Things to know:
///
/// - Events with no subscription (`sub` is `None`) skip the buffers. If the channel is full or
///   closed they are dropped and counted in [`dropped_unrouted`](Self::dropped_unrouted).
/// - Resync notices ([`Event::Resync`], made by the protocol cores after a sequence gap or a
///   reconnect) travel like any other event of their subscription. The emitter does not create
///   them and does not treat them specially.
/// - Each envelope is stamped with the time it is built, which is when it leaves the buffer, not
///   when the event was parsed. For a slow consumer the stamp is later than the arrival.
/// - The [`Event::Overflow`] notice has its own queue, which is sent before the buffers. So it can
///   reach the consumer before older events of the same subscription that are still waiting. Treat
///   it as "events of this subscription are missing, re-read the state", not as an ordered marker.
/// - Events for a subscription that was not [`register`](Self::register)ed are discarded.
/// - All methods take `&self` and are safe to call from several tasks. Share the emitter as an
///   [`Arc`].
///
/// # Examples
///
/// ```
/// use brokapi_core::{Backpressure, Event, SubscriptionId};
/// use brokapi_runtime::Emitter;
/// use brokapi_sansio::CoreEvent;
/// use tokio::sync::mpsc;
///
/// // A channel that holds one envelope, and a subscription whose buffer holds one event.
/// let (tx, mut rx) = mpsc::channel(1);
/// let emitter = Emitter::new("demo".into(), tx);
/// let sub = SubscriptionId::new(1);
/// emitter.register(sub, None, Backpressure::Lossless { cap: 1 });
///
/// let event = || CoreEvent { sub: Some(sub), payload: Event::Balances(vec![]) };
/// // The consumer does not read. The first event fills the channel, the second waits in a
/// // slot, the third fills the buffer, the fourth finds no room.
/// for _ in 0..4 {
///     emitter.emit(vec![event()]);
/// }
/// assert!(emitter.is_degraded());
/// assert!(emitter.has_backlog());
///
/// // The consumer wakes up. Flushing moves what is left into the channel.
/// let mut overflow_notices = 0;
/// let mut events = 0;
/// for _ in 0..10 {
///     while let Ok(envelope) = rx.try_recv() {
///         assert_eq!(&*envelope.source, "demo");
///         assert_eq!(envelope.sub, Some(sub));
///         match envelope.payload {
///             Event::Overflow { .. } => overflow_notices += 1,
///             _ => events += 1,
///         }
///     }
///     emitter.flush();
/// }
/// assert_eq!(overflow_notices, 1);
/// assert_eq!(events, 3); // the fourth event was lost
///
/// // A successful reconcile ends the degraded state.
/// emitter.clear_degraded();
/// assert!(!emitter.is_degraded());
/// ```
pub struct Emitter {
    source: Arc<str>,
    out: mpsc::Sender<Envelope>,
    state: Mutex<State>,
    degraded: AtomicBool,
    backlog: AtomicBool,
    /// Envelopes with no subscription that the channel refused.
    dropped_unrouted: AtomicU64,
}

impl Emitter {
    /// Creates an emitter that writes into `out`.
    ///
    /// `source` is the connector label from its config (for example `"bcs-ud"`). It is put into
    /// every [`Envelope`] so the consumer can tell connectors apart.
    pub fn new(source: Arc<str>, out: mpsc::Sender<Envelope>) -> Arc<Self> {
        Arc::new(Self {
            source,
            out,
            state: Mutex::new(State {
                bp: BackpressureManager::new(),
                subs: Vec::new(),
                pending: HashMap::new(),
                overflowed: HashSet::new(),
                conflating: HashSet::new(),
                notices: VecDeque::new(),
            }),
            degraded: AtomicBool::new(false),
            backlog: AtomicBool::new(false),
            dropped_unrouted: AtomicU64::new(0),
        })
    }

    /// The `source` label given to [`new`](Self::new).
    pub fn source(&self) -> &Arc<str> {
        &self.source
    }

    /// Starts buffering a subscription with the given policy.
    ///
    /// Call it before the first event of `sub` can arrive. `instrument` is the subscription's
    /// instrument, or `None` for account-level streams such as order state or portfolio.
    pub fn register(
        &self,
        sub: SubscriptionId,
        instrument: Option<InstrumentId>,
        policy: Backpressure,
    ) {
        let instrument = instrument.unwrap_or_else(|| {
            InstrumentId::new(
                InstrumentKey::new("", ""),
                NativeId::TickerClass {
                    ticker: String::new(),
                    class_code: String::new(),
                },
            )
        });
        let mut st = self.state.lock().expect("emitter poisoned");
        st.bp.register(sub, instrument, policy);
        st.subs.push(sub);
        if policy == Backpressure::Conflate {
            st.conflating.insert(sub);
        }
    }

    /// Stops buffering a subscription. Events still waiting for it are discarded.
    ///
    /// An [`Event::Overflow`] notice already queued for the subscription is still sent.
    pub fn unregister(&self, sub: SubscriptionId) {
        let mut st = self.state.lock().expect("emitter poisoned");
        st.bp.unregister(sub);
        st.subs.retain(|s| *s != sub);
        st.pending.remove(&sub);
        st.overflowed.remove(&sub);
        st.conflating.remove(&sub);
    }

    /// Whether a `Lossless` subscription overflowed and no reconcile has cleared it yet.
    pub fn is_degraded(&self) -> bool {
        self.degraded.load(Ordering::Acquire)
    }

    /// Leaves the degraded state. Call it from a successful reconcile.
    ///
    /// Overflowed `Lossless` buffers accept events again and a new overflow sends a new
    /// [`Event::Overflow`]. Events lost during the overflow are not recovered, the reconcile is
    /// what makes up for them. Events already waiting in the buffers are still delivered.
    pub fn clear_degraded(&self) {
        let mut st = self.state.lock().expect("emitter poisoned");
        st.bp.clear_degraded();
        st.overflowed.clear();
        self.degraded.store(false, Ordering::Release);
    }

    /// Whether some events or notices are waiting because the channel was full.
    ///
    /// While it is `true`, call [`flush`](Self::flush) again later. [`emit`](Self::emit) also
    /// retries on every call.
    pub fn has_backlog(&self) -> bool {
        self.backlog.load(Ordering::Acquire)
    }

    /// How many events with no subscription were dropped because the channel was full or closed.
    pub fn dropped_unrouted(&self) -> u64 {
        self.dropped_unrouted.load(Ordering::Relaxed)
    }

    fn envelope(&self, sub: Option<SubscriptionId>, payload: Event) -> Envelope {
        Envelope {
            schema: ENVELOPE_SCHEMA,
            source: Arc::clone(&self.source),
            sub,
            ts: Timestamps::new(wall_nanos(), crate::now()),
            payload,
        }
    }

    /// Buffers events and moves what the channel accepts. Never blocks.
    ///
    /// Events with a subscription go through that subscription's buffer and policy. A `Lossless`
    /// overflow queues one [`Event::Overflow`] (with `lost_from` set to the current wall clock)
    /// per subscription and marks the emitter degraded. Events with no subscription are sent
    /// straight to the channel or dropped, see the type docs. Nothing is returned: the outcome is
    /// visible through [`is_degraded`](Self::is_degraded), [`has_backlog`](Self::has_backlog) and
    /// [`dropped_unrouted`](Self::dropped_unrouted).
    pub fn emit(&self, events: Vec<CoreEvent>) {
        if events.is_empty() {
            return;
        }
        let mut st = self.state.lock().expect("emitter poisoned");
        for CoreEvent { sub, payload } in events {
            match sub {
                None => {
                    let env = self.envelope(None, payload);
                    if self.out.try_send(env).is_err() {
                        self.dropped_unrouted.fetch_add(1, Ordering::Relaxed);
                        warn!(
                            "egress channel full or closed: dropping an event with no subscription"
                        );
                    }
                }
                Some(sub) => {
                    let now = wall_nanos();
                    if st.conflating.contains(&sub) {
                        // A newer snapshot replaces one the channel has not taken yet.
                        st.pending.remove(&sub);
                    }
                    if st.bp.ingest(sub, payload, now) == PushResult::Overflow {
                        self.degraded.store(true, Ordering::Release);
                        if st.overflowed.insert(sub) {
                            let ev = Event::Overflow {
                                sub,
                                lost_from: now,
                            };
                            let env = self.envelope(Some(sub), ev);
                            st.notices.push_back(env);
                        }
                    }
                }
            }
        }
        self.flush_locked(&mut st);
    }

    /// Retries delivery of buffered events and notices. Never blocks.
    ///
    /// Call it periodically while [`has_backlog`](Self::has_backlog) is `true`, for example when
    /// the consumer has read from the channel.
    pub fn flush(&self) {
        let mut st = self.state.lock().expect("emitter poisoned");
        self.flush_locked(&mut st);
    }

    fn flush_locked(&self, st: &mut State) {
        while let Some(env) = st.notices.pop_front() {
            if let Err(e) = self.out.try_send(env) {
                if let TrySendError::Full(env) = e {
                    st.notices.push_front(env);
                }
                break;
            }
        }
        let subs = st.subs.clone();
        let mut backlog = !st.notices.is_empty();
        for sub in subs {
            loop {
                let env = match st.pending.remove(&sub) {
                    Some(env) => env,
                    None => match st.bp.pop_ready(sub) {
                        Some(ev) => self.envelope(Some(sub), ev),
                        None => break,
                    },
                };
                match self.out.try_send(env) {
                    Ok(()) => {}
                    Err(TrySendError::Full(env)) => {
                        st.pending.insert(sub, env);
                        backlog = true;
                        break;
                    }
                    Err(TrySendError::Closed(_)) => break,
                }
            }
        }
        self.backlog.store(backlog, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use brokapi_core::{Balance, Money};
    use std::time::Instant;

    fn ev(n: u32) -> CoreEvent {
        CoreEvent {
            sub: Some(SubscriptionId::new(1)),
            payload: Event::Balances(vec![Balance {
                currency: "RUB".into(),
                available: Money::from(n),
                blocked: Money::ZERO,
                ts: Timestamps::new(wall_nanos(), Instant::now()),
            }]),
        }
    }

    fn avail(env: &Envelope) -> Money {
        match &env.payload {
            Event::Balances(b) => b[0].available,
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn delivers_with_sub_and_source() {
        let (tx, mut rx) = mpsc::channel(8);
        let e = Emitter::new("src".into(), tx);
        e.register(
            SubscriptionId::new(1),
            None,
            Backpressure::Lossless { cap: 4 },
        );
        e.emit(vec![ev(1)]);
        let env = rx.try_recv().unwrap();
        assert_eq!(env.sub, Some(SubscriptionId::new(1)));
        assert_eq!(env.source.as_ref(), "src");
    }

    #[test]
    fn lossless_overflow_is_explicit_and_degrades() {
        let (tx, mut rx) = mpsc::channel(1);
        let e = Emitter::new("src".into(), tx);
        e.register(
            SubscriptionId::new(1),
            None,
            Backpressure::Lossless { cap: 2 },
        );
        // channel holds 1; buffer holds 2 (+1 popped waiting); then overflow
        for n in 1..=6 {
            e.emit(vec![ev(n)]);
        }
        assert!(e.is_degraded());
        assert!(e.has_backlog());
        let mut seen = Vec::new();
        let mut overflow = false;
        for _ in 0..20 {
            while let Ok(env) = rx.try_recv() {
                match env.payload {
                    Event::Overflow { sub, .. } => {
                        assert_eq!(sub, SubscriptionId::new(1));
                        overflow = true;
                    }
                    _ => seen.push(avail(&env)),
                }
            }
            e.flush();
        }
        assert!(overflow, "Event::Overflow must be delivered");
        // what was accepted before the overflow arrives in order, nothing after the hole
        assert_eq!(seen, [1, 2, 3, 4].map(Money::from).to_vec());
        e.clear_degraded();
        assert!(!e.is_degraded());
    }

    #[test]
    fn conflate_keeps_latest_for_slow_consumer() {
        let (tx, mut rx) = mpsc::channel(1);
        let e = Emitter::new("src".into(), tx);
        e.register(SubscriptionId::new(1), None, Backpressure::Conflate);
        for n in 1..=5 {
            e.emit(vec![ev(n)]);
        }
        assert_eq!(avail(&rx.try_recv().unwrap()), Money::from(1));
        e.flush();
        assert_eq!(avail(&rx.try_recv().unwrap()), Money::from(5));
        assert!(rx.try_recv().is_err());
        assert!(!e.is_degraded());
    }

    #[test]
    fn drop_newest_counts_not_degrades() {
        let (tx, mut rx) = mpsc::channel(1);
        let e = Emitter::new("src".into(), tx);
        e.register(
            SubscriptionId::new(1),
            None,
            Backpressure::DropNewest { cap: 1 },
        );
        for n in 1..=5 {
            e.emit(vec![ev(n)]);
        }
        assert!(!e.is_degraded());
        let mut got = vec![avail(&rx.try_recv().unwrap())];
        e.flush();
        while let Ok(env) = rx.try_recv() {
            got.push(avail(&env));
        }
        assert_eq!(got[0], Money::from(1));
        assert!(got.len() <= 3);
    }
}
