//! Per-subscription backpressure buffers

use std::collections::VecDeque;

use brokapi_core::{Backpressure, Event, InstrumentId, SubscriptionId, UnixNanos};

/// What happened to one event offered to a buffer. Returned by [`SubscriptionBuffer::try_push`] and
/// [`BackpressureManager::ingest`].
///
/// The meaning depends on the subscription's [`Backpressure`] policy:
///
/// | Policy | Possible results |
/// |---|---|
/// | [`Backpressure::Conflate`] | always [`Conflated`](Self::Conflated) |
/// | [`Backpressure::Lossless`] | [`Delivered`](Self::Delivered), then [`Overflow`](Self::Overflow) for ever |
/// | [`Backpressure::DropNewest`] | [`Delivered`](Self::Delivered) or [`DroppedNewest`](Self::DroppedNewest) |
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PushResult {
    /// The event is in the buffer and will be delivered in order.
    Delivered,
    /// The event is the buffer's single "latest" value. It replaced the previous one, if any.
    /// This is the only result of a `Conflate` subscription, even for its first event.
    Conflated,
    /// The buffer was full and the new event was thrown away. Older events are kept. The loss is
    /// counted in [`SubscriptionBuffer::dropped`]. The connector does not become degraded.
    DroppedNewest,
    /// A `Lossless` buffer was full. The event was not stored. This is terminal until the buffer is
    /// reset: every later event of this subscription is refused too, and the owner must tell the
    /// consumer with [`Event::Overflow`] and mark the connector degraded (see [`BackpressureManager`]).
    Overflow,
}

/// Egress buffer of one subscription. It applies the subscription's [`Backpressure`] policy.
///
/// The buffer never blocks and never waits. It is a plain in-memory queue (or single slot), not
/// thread-safe by itself. [`BackpressureManager`] owns many of them, and the emitter wraps the manager in a
/// lock.
///
/// # Examples
///
/// ```
/// use brokapi_core::{
///     Backpressure, Event, InstrumentId, InstrumentKey, NativeId, SubscriptionId,
/// };
/// use brokapi_runtime::{PushResult, SubscriptionBuffer};
///
/// let sber = InstrumentId::new(
///     InstrumentKey::new("SBER", "TQBR"),
///     NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() },
/// );
/// let mut buf = SubscriptionBuffer::new(SubscriptionId::new(1), sber, Backpressure::DropNewest { cap: 1 });
///
/// assert_eq!(buf.try_push(Event::Balances(vec![])), PushResult::Delivered);
/// // The buffer holds one event. The next one is thrown away and counted.
/// assert_eq!(buf.try_push(Event::Balances(vec![])), PushResult::DroppedNewest);
/// assert_eq!(buf.dropped(), 1);
/// assert_eq!(buf.drain().len(), 1);
/// ```
#[derive(Debug)]
pub struct SubscriptionBuffer {
    sub: SubscriptionId,
    instrument: InstrumentId,
    policy: Backpressure,
    queue: VecDeque<Event>,
    latest: Option<Event>,
    conflated: u64,
    dropped: u64,
    overflowed: bool,
}

impl SubscriptionBuffer {
    /// Creates an empty buffer for a subscription with the given policy.
    ///
    /// A `cap` of `0` in `Lossless` or `DropNewest` makes a buffer that accepts nothing: the first
    /// event already overflows (or is dropped).
    pub fn new(sub: SubscriptionId, instrument: InstrumentId, policy: Backpressure) -> Self {
        Self {
            sub,
            instrument,
            policy,
            queue: VecDeque::new(),
            latest: None,
            conflated: 0,
            dropped: 0,
            overflowed: false,
        }
    }

    /// The subscription this buffer serves.
    pub fn sub_id(&self) -> SubscriptionId {
        self.sub
    }

    /// The instrument of the subscription, as given to [`new`](Self::new).
    pub fn instrument(&self) -> &InstrumentId {
        &self.instrument
    }

    /// Whether a `Lossless` buffer has overflowed and not been reset yet.
    pub fn is_overflowed(&self) -> bool {
        self.overflowed
    }

    /// How many events a `Conflate` buffer replaced before they were taken.
    pub fn conflated(&self) -> u64 {
        self.conflated
    }

    /// How many events a `DropNewest` buffer threw away.
    pub fn dropped(&self) -> u64 {
        self.dropped
    }

    /// How many events wait in the buffer. For `Conflate` this is `0` or `1`.
    pub fn len(&self) -> usize {
        match self.policy {
            Backpressure::Conflate => usize::from(self.latest.is_some()),
            Backpressure::Lossless { .. } | Backpressure::DropNewest { .. } => self.queue.len(),
        }
    }

    /// Whether the buffer holds no event.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Offers an event to the buffer without blocking.
    ///
    /// - `Conflate`: the event becomes the single latest value. A value that was still waiting is
    ///   replaced and counted in [`conflated`](Self::conflated). Result: [`PushResult::Conflated`].
    /// - `Lossless { cap }`: stored while fewer than `cap` events wait. The first event that finds
    ///   the buffer full is not stored, the buffer is flagged overflowed and the result is
    ///   [`PushResult::Overflow`]. After that every event gets `Overflow` and is not stored.
    /// - `DropNewest { cap }`: stored while fewer than `cap` events wait. Otherwise the new event is
    ///   thrown away and counted in [`dropped`](Self::dropped).
    ///
    /// Why `Lossless` stops for good: it is used for events that must not have a hole (orders,
    /// fills). Once one is lost, later events would hide the gap, so the consumer must be told and
    /// re-read the state.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::{Backpressure, Event, InstrumentId, InstrumentKey, NativeId, SubscriptionId};
    /// use brokapi_runtime::{PushResult, SubscriptionBuffer};
    ///
    /// let sber = InstrumentId::new(
    ///     InstrumentKey::new("SBER", "TQBR"),
    ///     NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() },
    /// );
    /// let mut buf = SubscriptionBuffer::new(SubscriptionId::new(1), sber, Backpressure::Lossless { cap: 1 });
    ///
    /// assert_eq!(buf.try_push(Event::Balances(vec![])), PushResult::Delivered);
    /// assert_eq!(buf.try_push(Event::Balances(vec![])), PushResult::Overflow);
    /// assert!(buf.is_overflowed());
    /// // Even after the consumer takes the stored event, the buffer stays overflowed.
    /// assert_eq!(buf.drain().len(), 1);
    /// assert_eq!(buf.try_push(Event::Balances(vec![])), PushResult::Overflow);
    /// ```
    pub fn try_push(&mut self, event: Event) -> PushResult {
        match self.policy {
            Backpressure::Conflate => {
                if self.latest.is_some() {
                    self.conflated += 1;
                }
                self.latest = Some(event);
                PushResult::Conflated
            }
            Backpressure::Lossless { cap } => {
                if self.overflowed {
                    return PushResult::Overflow;
                }
                if self.queue.len() >= cap {
                    self.overflowed = true;
                    PushResult::Overflow
                } else {
                    self.queue.push_back(event);
                    PushResult::Delivered
                }
            }
            Backpressure::DropNewest { cap } => {
                if self.queue.len() >= cap {
                    self.dropped += 1;
                    PushResult::DroppedNewest
                } else {
                    self.queue.push_back(event);
                    PushResult::Delivered
                }
            }
        }
    }

    /// Builds the [`Event::Overflow`] notice for this subscription.
    ///
    /// `lost_from` is the wall-clock time from which events may be missing. The buffer does not
    /// send the notice itself, the owner does.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::{Backpressure, Event, InstrumentId, InstrumentKey, NativeId, SubscriptionId, UnixNanos};
    /// use brokapi_runtime::SubscriptionBuffer;
    ///
    /// let sber = InstrumentId::new(
    ///     InstrumentKey::new("SBER", "TQBR"),
    ///     NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() },
    /// );
    /// let buf = SubscriptionBuffer::new(SubscriptionId::new(7), sber, Backpressure::Lossless { cap: 1 });
    /// let notice = buf.overflow_event(UnixNanos::from_nanos(5));
    /// assert!(matches!(notice, Event::Overflow { sub, .. } if sub == SubscriptionId::new(7)));
    /// ```
    pub fn overflow_event(&self, lost_from: UnixNanos) -> Event {
        Event::Overflow {
            sub: self.sub,
            lost_from,
        }
    }

    /// Takes everything out of the buffer: the latest conflated value first, then the queue in
    /// arrival order.
    ///
    /// Draining does not clear the overflowed flag. Use [`BackpressureManager::clear_degraded`] for that.
    pub fn drain(&mut self) -> Vec<Event> {
        let mut out = Vec::new();
        if let Some(ev) = self.latest.take() {
            out.push(ev);
        }
        out.extend(self.queue.drain(..));
        out
    }
}

/// The buffers of all active subscriptions of one connector, plus the connector's degraded flag.
///
/// Used from the parser task: [`ingest`](Self::ingest) puts an event into the buffer of its
/// subscription without blocking, and [`pop_ready`](Self::pop_ready) takes the next one out for
/// delivery. When any `Lossless` buffer overflows, the manager marks the whole connector degraded.
/// A degraded connector refuses trading commands with [`brokapi_core::Error::Degraded`] (the
/// caller applies that, for example through [`CommandGate`](crate::CommandGate)) until a successful
/// reconcile calls [`clear_degraded`](Self::clear_degraded).
///
/// Most connectors use [`Emitter`](crate::Emitter), which owns a manager and also moves events
/// into the output channel.
///
/// # Examples
///
/// ```
/// use brokapi_core::{Backpressure, Event, InstrumentId, InstrumentKey, NativeId, SubscriptionId, UnixNanos};
/// use brokapi_runtime::{BackpressureManager, PushResult};
///
/// let sber = InstrumentId::new(
///     InstrumentKey::new("SBER", "TQBR"),
///     NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() },
/// );
/// let sub = SubscriptionId::new(1);
/// let mut mgr = BackpressureManager::new();
/// mgr.register(sub, sber, Backpressure::Lossless { cap: 1 });
///
/// assert_eq!(mgr.ingest(sub, Event::Balances(vec![]), UnixNanos::ZERO), PushResult::Delivered);
/// assert_eq!(mgr.ingest(sub, Event::Balances(vec![]), UnixNanos::ZERO), PushResult::Overflow);
/// assert!(mgr.is_degraded());
///
/// // The reconcile that follows an overflow clears the flag.
/// mgr.clear_degraded();
/// assert!(!mgr.is_degraded());
/// ```
#[derive(Debug, Default)]
pub struct BackpressureManager {
    buffers: Vec<SubscriptionBuffer>,
    connector_degraded: bool,
}

impl BackpressureManager {
    /// Creates a manager with no subscriptions, not degraded.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a buffer for a subscription.
    ///
    /// Registering the same `sub` twice creates two buffers. Only the first one is ever used, so
    /// register each subscription id once.
    pub fn register(
        &mut self,
        sub: SubscriptionId,
        instrument: InstrumentId,
        policy: Backpressure,
    ) {
        self.buffers
            .push(SubscriptionBuffer::new(sub, instrument, policy));
    }

    /// Removes the buffer of a subscription. Events still waiting in it are discarded.
    /// An unknown `sub` is ignored.
    pub fn unregister(&mut self, sub: SubscriptionId) {
        self.buffers.retain(|b| b.sub_id() != sub);
    }

    /// Whether some `Lossless` buffer overflowed since the last [`clear_degraded`](Self::clear_degraded).
    pub fn is_degraded(&self) -> bool {
        self.connector_degraded
    }

    /// Leaves the degraded state and reopens every overflowed buffer.
    ///
    /// Call it after a successful reconcile, when the consumer has re-read the state it lost.
    /// Events that were refused during the overflow are not recovered.
    pub fn clear_degraded(&mut self) {
        self.connector_degraded = false;
        for buf in &mut self.buffers {
            buf.overflowed = false;
        }
    }

    /// Offers an event to the buffer of `sub` without blocking.
    ///
    /// An [`PushResult::Overflow`] also sets the degraded flag of the manager. The `lost_from`
    /// argument is accepted for the caller's convenience and is not used by the manager: the
    /// caller builds the [`Event::Overflow`] notice itself (see [`SubscriptionBuffer::overflow_event`]).
    ///
    /// An event for a `sub` that is not registered is discarded and the result is still
    /// [`PushResult::Delivered`]. Register the subscription before its first event can arrive.
    pub fn ingest(
        &mut self,
        sub: SubscriptionId,
        event: Event,
        lost_from: UnixNanos,
    ) -> PushResult {
        let Some(buf) = self.buffers.iter_mut().find(|b| b.sub_id() == sub) else {
            return PushResult::Delivered;
        };
        let result = buf.try_push(event);
        if result == PushResult::Overflow {
            self.connector_degraded = true;
            let _ = lost_from;
        }
        result
    }

    /// Takes the next event of `sub` that is ready for delivery, if any.
    ///
    /// A `Conflate` subscription yields its latest value. Others yield their queue front, oldest
    /// first. Returns `None` for an empty buffer and for an unknown `sub`.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::{Backpressure, Event, InstrumentId, InstrumentKey, NativeId, SubscriptionId, UnixNanos};
    /// use brokapi_runtime::BackpressureManager;
    ///
    /// let sber = InstrumentId::new(
    ///     InstrumentKey::new("SBER", "TQBR"),
    ///     NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() },
    /// );
    /// let sub = SubscriptionId::new(1);
    /// let mut mgr = BackpressureManager::new();
    /// mgr.register(sub, sber, Backpressure::Conflate);
    ///
    /// // Three snapshots arrive before the consumer looks. Only the last one is kept.
    /// for _ in 0..3 {
    ///     mgr.ingest(sub, Event::Balances(vec![]), UnixNanos::ZERO);
    /// }
    /// assert!(mgr.pop_ready(sub).is_some());
    /// assert!(mgr.pop_ready(sub).is_none());
    /// ```
    pub fn pop_ready(&mut self, sub: SubscriptionId) -> Option<Event> {
        let buf = self.buffers.iter_mut().find(|b| b.sub_id() == sub)?;
        if let Some(ev) = buf.latest.take() {
            return Some(ev);
        }
        buf.queue.pop_front()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use brokapi_core::{InstrumentKey, NativeId};

    fn instrument() -> InstrumentId {
        InstrumentId::new(
            InstrumentKey::new("SBER", "TQBR"),
            NativeId::TickerClass {
                ticker: "SBER".into(),
                class_code: "TQBR".into(),
            },
        )
    }

    #[test]
    fn lossless_overflow_marks_degraded() {
        let mut mgr = BackpressureManager::new();
        let sub = SubscriptionId::new(1);
        mgr.register(sub, instrument(), Backpressure::Lossless { cap: 2 });

        assert_eq!(
            mgr.ingest(sub, Event::Balances(vec![]), UnixNanos::ZERO),
            PushResult::Delivered
        );
        assert_eq!(
            mgr.ingest(sub, Event::Balances(vec![]), UnixNanos::ZERO),
            PushResult::Delivered
        );
        assert_eq!(
            mgr.ingest(sub, Event::Balances(vec![]), UnixNanos::from_nanos(1)),
            PushResult::Overflow
        );
        assert!(mgr.is_degraded());
    }
}
