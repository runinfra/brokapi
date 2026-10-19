//! Deterministic replay harness for recorded frames.

use std::time::{Duration, Instant};

use crate::core::BrokerCore;
use crate::event::{CoreEvent, EventSink};
use crate::outbound::Outbound;
use crate::subscription::SubscriptionSet;

/// One inbound frame of a recorded session, with the instant it arrived.
///
/// A session is a list of these. Replay it with [`Harness::run_frames`].
#[derive(Debug, Clone)]
pub struct FrameRecord<F> {
    /// When the frame arrived, on the harness's virtual clock.
    pub at: Instant,
    /// The frame, in the core's [`BrokerCore::Frame`] type.
    pub frame: F,
}

impl<F> FrameRecord<F> {
    /// Creates a record of `frame` arriving at `at`.
    pub fn new(at: Instant, frame: F) -> Self {
        Self { at, frame }
    }
}

/// Drives a [`BrokerCore`] with virtual time and records what comes out.
///
/// The harness owns the core and a clock that only moves when you move it. Feed frames, move time,
/// then read the events the core emitted ([`events`](Self::events)) and the messages it wanted to
/// send ([`transmitted`](Self::transmitted)). There is no socket and no real waiting, so a test of
/// a minute of silence takes no time.
///
/// After every call into the core the harness drains [`BrokerCore::poll_transmit`] into the
/// `transmitted` log, and it fires the core's timers whenever time passes their deadline.
///
/// # Examples
///
/// ```
/// # use std::time::{Duration, Instant};
/// # use brokapi_core::{Event, InstrumentId, InstrumentKey, NativeId, ResyncReason, SubscriptionId};
/// # use brokapi_sansio::{BrokerCore, EventSink, Outbound, SubscriptionSet};
/// # struct WatchdogCore { silent_since: Instant, armed: bool, pongs: Vec<String> }
/// # impl BrokerCore for WatchdogCore {
/// #     type Frame = String;
/// #     fn on_frame(&mut self, frame: String, now: Instant, _out: &mut EventSink) {
/// #         self.silent_since = now;
/// #         self.armed = true;
/// #         self.pongs.push(format!("pong {frame}"));
/// #     }
/// #     fn poll_transmit(&mut self) -> Option<Outbound> {
/// #         if self.pongs.is_empty() { None } else { Some(Outbound::WsText(self.pongs.remove(0))) }
/// #     }
/// #     fn poll_timeout(&self) -> Option<Instant> {
/// #         self.armed.then(|| self.silent_since + Duration::from_secs(10))
/// #     }
/// #     fn on_timeout(&mut self, _now: Instant, out: &mut EventSink) {
/// #         self.armed = false;
/// #         let instrument = InstrumentId::new(
/// #             InstrumentKey::new("SBER", "TQBR"),
/// #             NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() },
/// #         );
/// #         out.push_payload(Some(SubscriptionId::new(1)), Event::Resync { sub: SubscriptionId::new(1), instrument, reason: ResyncReason::SubscriptionReset });
/// #     }
/// #     fn apply(&mut self, _desired: &SubscriptionSet) {}
/// # }
/// # let t0 = Instant::now();
/// # let core = WatchdogCore { silent_since: t0, armed: false, pongs: Vec::new() };
/// use brokapi_sansio::Harness;
///
/// let mut harness = Harness::new(core, t0);
///
/// // A frame arrives: the core answers with a pong.
/// harness.feed_frame("ping".to_string());
/// assert_eq!(harness.transmitted(), [Outbound::WsText("pong ping".to_string())]);
///
/// // 9 seconds of silence: the watchdog has not fired yet. 2 more: it has.
/// harness.advance_by(Duration::from_secs(9));
/// assert!(harness.events().is_empty());
/// harness.advance_by(Duration::from_secs(2));
/// let events = harness.take_events();
/// assert_eq!(events.len(), 1);
/// assert!(matches!(events[0].payload, Event::Resync { .. }));
/// assert!(harness.events().is_empty());
/// ```
pub struct Harness<C: BrokerCore> {
    core: C,
    now: Instant,
    events: Vec<CoreEvent>,
    transmitted: Vec<Outbound>,
}

impl<C: BrokerCore> Harness<C> {
    /// Wraps `core`. The virtual clock starts at `start`.
    pub fn new(core: C, start: Instant) -> Self {
        Self {
            core,
            now: start,
            events: Vec::new(),
            transmitted: Vec::new(),
        }
    }

    /// The current virtual time.
    pub fn now(&self) -> Instant {
        self.now
    }

    /// The core, for reading its state.
    pub fn core(&self) -> &C {
        &self.core
    }

    /// The core, for changing it directly. Messages it queues this way are collected on the next
    /// call that drains transmits, or by [`drain_transmit`](Self::drain_transmit).
    pub fn core_mut(&mut self) -> &mut C {
        &mut self.core
    }

    /// All events the core emitted since the last [`take_events`](Self::take_events), oldest first.
    pub fn events(&self) -> &[CoreEvent] {
        &self.events
    }

    /// The log of every message the core wanted sent, oldest first. It is never cleared.
    pub fn transmitted(&self) -> &[Outbound] {
        &self.transmitted
    }

    /// Tells the core the wanted subscriptions ([`BrokerCore::apply`]) and collects what it queues
    /// to send.
    pub fn apply(&mut self, desired: &SubscriptionSet) {
        self.core.apply(desired);
        self.drain_transmit();
    }

    /// Delivers a frame to the core at the current virtual time, and collects its events and
    /// messages. Time does not move.
    pub fn feed_frame(&mut self, frame: C::Frame) {
        let mut sink = EventSink::new();
        self.core.on_frame(frame, self.now, &mut sink);
        self.events.extend(sink.drain());
        self.drain_transmit();
    }

    /// Moves time to `at` (firing due timers on the way, see [`advance_to`](Self::advance_to)),
    /// then delivers the frame.
    pub fn feed_frame_at(&mut self, at: Instant, frame: C::Frame) {
        self.advance_to(at);
        self.feed_frame(frame);
    }

    /// Moves time forward by `delta`, firing every timer that falls due.
    pub fn advance_by(&mut self, delta: Duration) {
        self.advance_to(self.now + delta);
    }

    /// Moves time to `at`, firing every timer that falls due on the way.
    ///
    /// While the core's [`poll_timeout`](BrokerCore::poll_timeout) is at or before `at`, the
    /// harness calls [`on_timeout`](BrokerCore::on_timeout) with that deadline as the time, then
    /// asks again. So one call can fire several timers, each at its own time.
    ///
    /// A core whose `on_timeout` leaves the same deadline in place makes this loop for ever. Move
    /// or clear the deadline in `on_timeout`.
    ///
    /// `at` should not be earlier than [`now`](Self::now): the clock is set to `at` without a check.
    pub fn advance_to(&mut self, at: Instant) {
        while let Some(deadline) = self.core.poll_timeout() {
            if deadline > at {
                break;
            }
            let mut sink = EventSink::new();
            self.core.on_timeout(deadline, &mut sink);
            self.events.extend(sink.drain());
            self.drain_transmit();
            self.now = deadline;
        }
        self.now = at;
    }

    /// Replays a recorded session: each frame is delivered at its recorded time with
    /// [`feed_frame_at`](Self::feed_frame_at). Records must be in time order.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::time::{Duration, Instant};
    /// # use brokapi_core::{Event, InstrumentId, InstrumentKey, NativeId, ResyncReason, SubscriptionId};
    /// # use brokapi_sansio::{BrokerCore, EventSink, Outbound, SubscriptionSet};
    /// # struct WatchdogCore { silent_since: Instant, armed: bool, pongs: Vec<String> }
    /// # impl BrokerCore for WatchdogCore {
    /// #     type Frame = String;
    /// #     fn on_frame(&mut self, frame: String, now: Instant, _out: &mut EventSink) {
    /// #         self.silent_since = now;
    /// #         self.armed = true;
    /// #         self.pongs.push(format!("pong {frame}"));
    /// #     }
    /// #     fn poll_transmit(&mut self) -> Option<Outbound> {
    /// #         if self.pongs.is_empty() { None } else { Some(Outbound::WsText(self.pongs.remove(0))) }
    /// #     }
    /// #     fn poll_timeout(&self) -> Option<Instant> {
    /// #         self.armed.then(|| self.silent_since + Duration::from_secs(10))
    /// #     }
    /// #     fn on_timeout(&mut self, _now: Instant, out: &mut EventSink) {
    /// #         self.armed = false;
    /// #         let instrument = InstrumentId::new(
    /// #             InstrumentKey::new("SBER", "TQBR"),
    /// #             NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() },
    /// #         );
    /// #         out.push_payload(Some(SubscriptionId::new(1)), Event::Resync { sub: SubscriptionId::new(1), instrument, reason: ResyncReason::SubscriptionReset });
    /// #     }
    /// #     fn apply(&mut self, _desired: &SubscriptionSet) {}
    /// # }
    /// # let t0 = Instant::now();
    /// # let core = WatchdogCore { silent_since: t0, armed: false, pongs: Vec::new() };
    /// use brokapi_sansio::{FrameRecord, Harness};
    ///
    /// let mut harness = Harness::new(core, t0);
    /// let session = [
    ///     FrameRecord::new(t0 + Duration::from_secs(1), "a".to_string()),
    ///     FrameRecord::new(t0 + Duration::from_secs(2), "b".to_string()),
    /// ];
    /// harness.run_frames(&session);
    ///
    /// assert_eq!(harness.transmitted().len(), 2);
    /// assert_eq!(harness.now(), t0 + Duration::from_secs(2));
    /// ```
    pub fn run_frames(&mut self, frames: &[FrameRecord<C::Frame>])
    where
        C::Frame: Clone,
    {
        for record in frames {
            self.feed_frame_at(record.at, record.frame.clone());
        }
    }

    /// Polls the core for messages it wants sent and returns the ones found by this call.
    ///
    /// Every other harness method already does this and logs the result in
    /// [`transmitted`](Self::transmitted), so the return value is usually empty. Call it after
    /// changing the core through [`core_mut`](Self::core_mut). What it returns is also appended to
    /// the log.
    pub fn drain_transmit(&mut self) -> Vec<Outbound> {
        let mut out = Vec::new();
        while let Some(msg) = self.core.poll_transmit() {
            out.push(msg);
        }
        self.transmitted.extend(out.iter().cloned());
        out
    }

    /// Returns all collected events, oldest first, and empties the list.
    pub fn take_events(&mut self) -> Vec<CoreEvent> {
        std::mem::take(&mut self.events)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::EventSink;
    use brokapi_core::Event;

    struct EchoCore {
        pending: Vec<String>,
    }

    impl EchoCore {
        fn new() -> Self {
            Self {
                pending: Vec::new(),
            }
        }
    }

    impl BrokerCore for EchoCore {
        type Frame = String;

        fn on_frame(&mut self, frame: Self::Frame, _now: Instant, out: &mut EventSink) {
            out.push_payload(
                None,
                Event::Resync {
                    sub: brokapi_core::SubscriptionId::new(0),
                    instrument: brokapi_core::InstrumentId::new(
                        brokapi_core::InstrumentKey::new("SBER", "TQBR"),
                        brokapi_core::NativeId::TickerClass {
                            ticker: "SBER".into(),
                            class_code: "TQBR".into(),
                        },
                    ),
                    reason: brokapi_core::ResyncReason::Manual,
                },
            );
            self.pending.push(frame);
        }

        fn poll_transmit(&mut self) -> Option<Outbound> {
            self.pending.pop().map(Outbound::WsText)
        }

        fn poll_timeout(&self) -> Option<Instant> {
            None
        }

        fn on_timeout(&mut self, _now: Instant, _out: &mut EventSink) {}

        fn apply(&mut self, _desired: &SubscriptionSet) {}
    }

    #[test]
    fn harness_replays_frame_and_transmit() {
        let start = Instant::now();
        let mut h = Harness::new(EchoCore::new(), start);
        h.feed_frame("ping".into());
        assert_eq!(h.events().len(), 1);
        assert_eq!(h.transmitted().len(), 1);
    }
}
