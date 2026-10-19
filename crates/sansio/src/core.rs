//! Sans-io broker core trait.

use std::time::Instant;

use crate::event::EventSink;
use crate::outbound::Outbound;
use crate::subscription::SubscriptionSet;

/// Protocol logic of one broker stream, written without sockets, timers or an async runtime.
///
/// This is the "sans-io" pattern. A core is a plain state machine. The code that owns the socket
/// feeds it inbound frames and the current time, and collects what it wants to say: events for the
/// consumer ([`EventSink`]), messages to put on the wire ([`poll_transmit`](Self::poll_transmit))
/// and the next time it wants to be woken ([`poll_timeout`](Self::poll_timeout)). Because time is
/// an argument and there is no I/O, a core can be tested with recorded frames and virtual time, see
/// [`Harness`](crate::Harness).
///
/// How [`Harness`](crate::Harness) drives a core, and what a real driver should do the same way:
///
/// - call [`on_frame`](Self::on_frame) for each inbound frame, with the time it arrived;
/// - after every call into the core, drain [`poll_transmit`](Self::poll_transmit) until it returns
///   `None` and send the messages;
/// - sleep until [`poll_timeout`](Self::poll_timeout), then call [`on_timeout`](Self::on_timeout);
/// - call [`apply`](Self::apply) when the set of wanted subscriptions changes.
///
/// A core must never block, sleep or read the clock itself. Use the `now` it is given.
///
/// # Examples
///
/// A core that answers every frame and reports a resync after 10 seconds of silence, driven with
/// virtual time:
///
/// ```
/// use std::time::{Duration, Instant};
/// use brokapi_core::{Event, InstrumentId, InstrumentKey, NativeId, ResyncReason, SubscriptionId};
/// use brokapi_sansio::{BrokerCore, EventSink, Harness, Outbound, SubscriptionSet};
///
/// /// Answers every frame with a pong. If the line is silent for 10 seconds it reports a resync.
/// struct WatchdogCore {
///     silent_since: Instant,
///     armed: bool,
///     pongs: Vec<String>,
/// }
///
/// impl BrokerCore for WatchdogCore {
///     type Frame = String;
///
///     fn on_frame(&mut self, frame: String, now: Instant, _out: &mut EventSink) {
///         self.silent_since = now;
///         self.armed = true;
///         self.pongs.push(format!("pong {frame}"));
///     }
///
///     fn poll_transmit(&mut self) -> Option<Outbound> {
///         if self.pongs.is_empty() { None } else { Some(Outbound::WsText(self.pongs.remove(0))) }
///     }
///
///     fn poll_timeout(&self) -> Option<Instant> {
///         self.armed.then(|| self.silent_since + Duration::from_secs(10))
///     }
///
///     fn on_timeout(&mut self, _now: Instant, out: &mut EventSink) {
///         // Disarm first: a core that keeps the same deadline would be called again for ever.
///         self.armed = false;
///         let instrument = InstrumentId::new(
///             InstrumentKey::new("SBER", "TQBR"),
///             NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() },
///         );
///         out.push_payload(
///             Some(SubscriptionId::new(1)),
///             Event::Resync { sub: SubscriptionId::new(1), instrument, reason: ResyncReason::SubscriptionReset },
///         );
///     }
///
///     fn apply(&mut self, _desired: &SubscriptionSet) {}
/// }
///
/// let t0 = Instant::now();
/// let core = WatchdogCore { silent_since: t0, armed: false, pongs: Vec::new() };
/// let mut harness = Harness::new(core, t0);
///
/// harness.feed_frame("ping".to_string());
/// assert_eq!(harness.transmitted(), [Outbound::WsText("pong ping".to_string())]);
///
/// harness.advance_by(Duration::from_secs(9));
/// assert!(harness.events().is_empty());
/// harness.advance_by(Duration::from_secs(2));
/// assert_eq!(harness.events().len(), 1);
/// ```
pub trait BrokerCore {
    /// The inbound frame type: for example `String` for WebSocket text, or a decoded message.
    type Frame;

    /// Handles one inbound transport frame received at `now`.
    ///
    /// Events for the consumer are pushed into `out`. Messages for the broker are queued inside the
    /// core and picked up by [`poll_transmit`](Self::poll_transmit). The method does not return an
    /// error: a frame the core cannot use is its own business (skip it, or emit an event).
    fn on_frame(&mut self, frame: Self::Frame, now: Instant, out: &mut EventSink);

    /// Takes the next outbound wire message, if any. Call it until it returns `None`.
    fn poll_transmit(&mut self) -> Option<Outbound>;

    /// When the core wants [`on_timeout`](Self::on_timeout) to be called next, or `None` if it has
    /// no timer pending.
    ///
    /// After `on_timeout` the core must move this deadline forward or clear it. A core that
    /// returns the same past deadline again makes a driver loop for ever.
    fn poll_timeout(&self) -> Option<Instant>;

    /// Handles a timer that fired. `now` is the time the timer was due.
    fn on_timeout(&mut self, now: Instant, out: &mut EventSink);

    /// Tells the core the full set of subscriptions the consumer wants now.
    ///
    /// The argument is the whole target state, not a change. The core works out what to subscribe
    /// and unsubscribe and queues the messages for [`poll_transmit`](Self::poll_transmit).
    fn apply(&mut self, desired: &SubscriptionSet);
}
