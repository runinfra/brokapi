//! Normalized events emitted by sans-io cores.

use brokapi_core::{Event, SubscriptionId};

/// An event produced by a sans-io core, before it is wrapped in an [`Envelope`](brokapi_core::Envelope).
///
/// The connector's emitter (`brokapi_runtime::Emitter`) adds the source label, schema version and
/// timestamps when it wraps the event.
#[derive(Debug, Clone, PartialEq)]
pub struct CoreEvent {
    /// The subscription the event belongs to, or `None` for an event that belongs to no
    /// subscription (it is not buffered by a subscription policy downstream).
    pub sub: Option<SubscriptionId>,
    /// The event itself.
    pub payload: Event,
}

/// A list the core pushes events into while it handles a frame or a timeout.
///
/// A core receives `&mut EventSink` in [`BrokerCore::on_frame`](crate::BrokerCore::on_frame) and
/// [`BrokerCore::on_timeout`](crate::BrokerCore::on_timeout). The caller reads and empties it
/// afterwards. Events keep the order in which they were pushed.
///
/// # Examples
///
/// ```
/// use brokapi_core::{Event, SubscriptionId};
/// use brokapi_sansio::EventSink;
///
/// let mut sink = EventSink::new();
/// assert!(sink.is_empty());
///
/// sink.push_payload(Some(SubscriptionId::new(1)), Event::Balances(vec![]));
/// assert_eq!(sink.len(), 1);
/// assert_eq!(sink.events()[0].sub, Some(SubscriptionId::new(1)));
///
/// // Draining hands the events over and leaves the sink empty.
/// let events = sink.drain();
/// assert_eq!(events.len(), 1);
/// assert!(sink.is_empty());
/// ```
#[derive(Debug, Clone, Default)]
pub struct EventSink {
    events: Vec<CoreEvent>,
}

impl EventSink {
    /// Creates an empty sink.
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends an event.
    pub fn push(&mut self, event: CoreEvent) {
        self.events.push(event);
    }

    /// Appends an event built from its parts.
    pub fn push_payload(&mut self, sub: Option<SubscriptionId>, payload: Event) {
        self.push(CoreEvent { sub, payload });
    }

    /// Whether no event is waiting.
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// How many events are waiting.
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// The waiting events, oldest first. They stay in the sink.
    pub fn events(&self) -> &[CoreEvent] {
        &self.events
    }

    /// Takes all waiting events out, oldest first, and leaves the sink empty.
    pub fn drain(&mut self) -> Vec<CoreEvent> {
        std::mem::take(&mut self.events)
    }
}
