//! In-memory order tracking.

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use brokapi_core::{ClientOrderId, OrderState};
use brokapi_sansio::{FsmError, OrderRegistry, OrderSignal};
use tracing::warn;

/// Runtime order book: one state machine per client order id, a timeout sweep, and a guard that
/// never lets the same place command go out twice.
///
/// The rule: a place command that got no answer is never repeated. A retry could create a second
/// order at the broker. Instead the order goes to [`OrderState::Unknown`] and is settled by reading
/// the broker (reconcile or order state). [`try_place_once`](Self::try_place_once) enforces the
/// "once", and [`sweep_timeouts`](Self::sweep_timeouts) turns silent in-flight orders into
/// `Unknown`.
///
/// # Examples
///
/// ```
/// use std::time::{Duration, Instant};
/// use brokapi_core::{BrokerOrderId, ClientOrderId, OrderState};
/// use brokapi_runtime::OrderRuntime;
/// use brokapi_sansio::OrderSignal;
///
/// let mut orders = OrderRuntime::with_timeout(Duration::from_secs(30));
/// let t0 = Instant::now();
/// let cid = ClientOrderId::new_v4();
///
/// // First send of this cid is allowed. A second send of the same cid is not.
/// assert!(orders.try_place_once(cid));
/// orders.register_place(cid, t0);
/// assert!(!orders.try_place_once(cid));
///
/// // No answer within the timeout: the outcome is unknown, not "failed".
/// let changed = orders.sweep_timeouts(t0 + Duration::from_secs(31));
/// assert!(matches!(changed[0].1, OrderState::Unknown { .. }));
///
/// // A late ack from the stream or from a reconcile settles it.
/// let acked = orders.apply_signal(cid, OrderSignal::Ack { broker_id: BrokerOrderId::new("42") }, t0);
/// assert!(matches!(acked, Some(OrderState::Acked { .. })));
/// ```
///
/// # Memory
///
/// A finished order (filled, cancelled, rejected) is kept for a window and then forgotten, so a bot that
/// trades for months does not grow without end. The window is [`RETENTION`] (24 hours) and at most
/// [`RETAINED_FINISHED`] finished orders, whichever ends first (see
/// [`with_retention`](OrderRuntime::with_retention)). **The guarantee "a cid is placed once" holds inside
/// the window.** A cid older than the window is no longer remembered here, so a place with it would pass
/// the local check (the broker may still refuse it: T-Bank does, by its idempotency key). Never reuse a
/// cid: make a new one for every order.
#[derive(Debug)]
pub struct OrderRuntime {
    registry: OrderRegistry,
    command_timeout: Duration,
    place_attempts: HashMap<ClientOrderId, u8>,
    /// Orders in the order they finished, oldest first.
    finished: VecDeque<(Instant, ClientOrderId)>,
    retention: Duration,
    max_finished: usize,
}

/// How long a finished order is remembered by [`OrderRuntime`] (24 hours).
pub const RETENTION: Duration = Duration::from_secs(24 * 60 * 60);

/// How many finished orders [`OrderRuntime`] remembers at most (100 000).
pub const RETAINED_FINISHED: usize = 100_000;

impl Default for OrderRuntime {
    fn default() -> Self {
        Self::new()
    }
}

impl OrderRuntime {
    /// Creates an empty order book with a command timeout of 30 seconds and the default memory window
    /// ([`RETENTION`], [`RETAINED_FINISHED`]).
    pub fn new() -> Self {
        Self {
            registry: OrderRegistry::new(),
            command_timeout: Duration::from_secs(30),
            place_attempts: HashMap::new(),
            finished: VecDeque::new(),
            retention: RETENTION,
            max_finished: RETAINED_FINISHED,
        }
    }

    /// Creates an empty order book with the given command timeout, the age after which an in-flight
    /// order is considered unanswered by [`sweep_timeouts`](Self::sweep_timeouts).
    pub fn with_timeout(command_timeout: Duration) -> Self {
        Self {
            command_timeout,
            ..Self::new()
        }
    }

    /// Changes the command timeout of [`sweep_timeouts`](Self::sweep_timeouts). A connector sets it a
    /// little above its request timeout, so that an order is not given up while its call is still running.
    pub fn set_command_timeout(&mut self, command_timeout: Duration) {
        self.command_timeout = command_timeout;
    }

    /// Sets the memory window of finished orders: they are forgotten after `window`, or earlier when
    /// more than `max_finished` are kept (the oldest go first). See the type docs for what this means for
    /// the "a cid is placed once" guarantee.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::{Duration, Instant};
    /// use brokapi_core::ClientOrderId;
    /// use brokapi_runtime::OrderRuntime;
    /// use brokapi_sansio::OrderSignal;
    ///
    /// let mut orders = OrderRuntime::new().with_retention(Duration::from_secs(60), 1000);
    /// let t0 = Instant::now();
    /// let cid = ClientOrderId::new_v4();
    /// assert!(orders.try_place_once(cid));
    /// orders.register_place(cid, t0);
    /// orders.apply_signal(cid, OrderSignal::Rejected { reason: "no".into() }, t0);
    ///
    /// // Inside the window the cid is still refused ...
    /// orders.prune(t0 + Duration::from_secs(30));
    /// assert!(!orders.try_place_once(cid));
    /// // ... after it the order is forgotten.
    /// orders.prune(t0 + Duration::from_secs(61));
    /// assert!(orders.state(cid).is_none());
    /// assert!(orders.try_place_once(cid));
    /// ```
    #[must_use]
    pub fn with_retention(mut self, window: Duration, max_finished: usize) -> Self {
        self.retention = window;
        self.max_finished = max_finished;
        self
    }

    /// Forgets the finished orders that are older than the window, or beyond the count limit, as of `now`.
    /// Called by [`apply_signal`](Self::apply_signal) itself, so a caller rarely needs it.
    pub fn prune(&mut self, now: Instant) {
        while let Some(&(at, cid)) = self.finished.front() {
            let expired = now.saturating_duration_since(at) >= self.retention;
            if !(expired || self.finished.len() > self.max_finished) {
                break;
            }
            self.finished.pop_front();
            // A cid that was registered again after it finished is alive: leave it.
            if self.registry.forget_finished(cid) {
                self.place_attempts.remove(&cid);
            }
        }
    }

    /// How many orders are tracked now, in any state. For tests and metrics.
    pub fn tracked(&self) -> usize {
        self.registry.len()
    }

    /// How many cids are remembered as used for a place. For tests and metrics.
    pub fn remembered_places(&self) -> usize {
        self.place_attempts.len()
    }

    /// How many orders are `InFlight` now: sent, with no answer yet.
    pub fn in_flight_count(&self) -> usize {
        self.registry.in_flight_cids().len()
    }

    /// Starts tracking a place command that is being sent at `now`, in state `InFlight`.
    ///
    /// Records the first (and only allowed) attempt for `cid`. Registering a cid again restarts its
    /// state machine from `InFlight`.
    pub fn register_place(&mut self, cid: ClientOrderId, now: Instant) {
        self.registry.register_place(cid, now);
        self.place_attempts.insert(cid, 1);
    }

    /// Starts tracking a cid known only from a cancel, if it is not tracked yet.
    ///
    /// An order placed by an earlier run is not in memory. A cid that is already tracked keeps its
    /// state. A new one starts as `InFlight`, and does not count as a place attempt.
    pub fn register_cancel(&mut self, cid: ClientOrderId, now: Instant) {
        if self.registry.get(cid).is_none() {
            self.registry.register_place(cid, now);
        }
    }

    /// Returns `true` and records the attempt when `cid` has never been used for a place, `false`
    /// when it has.
    ///
    /// Why: the library must not retry a place on its own. Compare
    /// [`try_place_once`](Self::try_place_once), which does the same.
    pub fn may_send_place(&mut self, cid: ClientOrderId) -> bool {
        self.try_place_once(cid)
    }

    /// Returns `true` only on the first place attempt for this cid, and records that attempt.
    ///
    /// Call it before sending. The attempt is recorded even when the send then fails locally (for
    /// example a rate-limit refusal), so the same cid is never used twice. To try again, make a new
    /// cid. After [`register_place`](Self::register_place) the answer is always `false`. The record is
    /// kept for the memory window of the type, see there.
    ///
    /// # Examples
    ///
    /// ```
    /// use brokapi_core::ClientOrderId;
    /// use brokapi_runtime::OrderRuntime;
    ///
    /// let mut orders = OrderRuntime::new();
    /// let cid = ClientOrderId::new_v4();
    /// assert!(orders.try_place_once(cid));
    /// assert!(!orders.try_place_once(cid));
    /// assert!(orders.try_place_once(ClientOrderId::new_v4()));
    /// ```
    pub fn try_place_once(&mut self, cid: ClientOrderId) -> bool {
        match self.place_attempts.get(&cid) {
            None => {
                self.place_attempts.insert(cid, 1);
                true
            }
            Some(_) => false,
        }
    }

    /// Feeds a signal (ack, fill, cancel, reject, timeout) into the order's state machine.
    ///
    /// Returns the new state when the state changed. Returns `None` when nothing changed, when
    /// the cid is not tracked, and when the signal is not valid in the current state (for example
    /// a cancel after `Filled`). An invalid signal is logged at warn level. A caller that needs to tell
    /// these apart uses [`try_apply_signal`](Self::try_apply_signal).
    pub fn apply_signal(
        &mut self,
        cid: ClientOrderId,
        signal: OrderSignal,
        now: Instant,
    ) -> Option<OrderState> {
        match self.try_apply_signal(cid, signal, now) {
            Ok(changed) => changed,
            // A cid this process does not track is expected for foreign orders.
            Err(FsmError::UnknownOrder(_)) => None,
            Err(e) => {
                warn!(%cid, error = ?e, "an order signal contradicts the known state, ignored");
                None
            }
        }
    }

    /// Like [`apply_signal`](Self::apply_signal) but tells the reason when the signal is not applied.
    ///
    /// # Errors
    ///
    /// [`FsmError::UnknownOrder`] for a cid that is not tracked, [`FsmError::InvalidTransition`] for a
    /// signal that contradicts the state (a cancel of a filled order). The state is left as it was.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Instant;
    /// use brokapi_core::ClientOrderId;
    /// use brokapi_runtime::OrderRuntime;
    /// use brokapi_sansio::{FsmError, OrderSignal};
    ///
    /// let mut orders = OrderRuntime::new();
    /// let t0 = Instant::now();
    /// let cid = ClientOrderId::new_v4();
    /// assert!(matches!(
    ///     orders.try_apply_signal(cid, OrderSignal::Filled, t0),
    ///     Err(FsmError::UnknownOrder(_))
    /// ));
    ///
    /// // A fill that is faster than the answer to the place command is taken.
    /// orders.register_place(cid, t0);
    /// assert!(orders.try_apply_signal(cid, OrderSignal::Filled, t0).unwrap().is_some());
    /// // A cancel of a filled order contradicts it.
    /// assert!(orders.try_apply_signal(cid, OrderSignal::Cancelled, t0).is_err());
    /// ```
    pub fn try_apply_signal(
        &mut self,
        cid: ClientOrderId,
        signal: OrderSignal,
        now: Instant,
    ) -> Result<Option<OrderState>, FsmError> {
        let changed = self.registry.apply(cid, signal, now)?;
        if let Some(
            OrderState::Filled { .. } | OrderState::Cancelled { .. } | OrderState::Rejected { .. },
        ) = &changed
        {
            self.finished.push_back((now, cid));
            self.prune(now);
        }
        Ok(changed)
    }

    /// Moves every `InFlight` order older than the command timeout to `Unknown`.
    ///
    /// Returns the orders that changed, with their new state. It does not cancel or retry
    /// anything: the exchange order may well exist.
    pub fn sweep_timeouts(&mut self, now: Instant) -> Vec<(ClientOrderId, OrderState)> {
        self.registry.on_global_timeout(now, self.command_timeout)
    }

    /// The current state of a tracked order, or `None` for an unknown cid.
    pub fn state(&self, cid: ClientOrderId) -> Option<&OrderState> {
        self.registry.get(cid).map(|f| f.state())
    }

    /// Moves every `InFlight` order to `Unknown` now, whatever its age. Used at shutdown.
    ///
    /// Returns the cids that changed. Orders at the broker are untouched, only the local wait for
    /// an answer is given up.
    pub fn abandon_inflight(&mut self, now: Instant) -> Vec<ClientOrderId> {
        self.registry
            .abandon_inflight(now)
            .into_iter()
            .map(|(cid, _)| cid)
            .collect()
    }
}

/// A memory window for things that belong to finished orders: the cid maps of a connector.
///
/// A connector keeps small maps next to its [`OrderRuntime`] (cid to instrument, broker id to cid, ...).
/// When an order is final, the connector [`push`](Self::push)es its cid here. Whatever is older than the
/// window, or beyond the count limit, comes back from [`expired`](Self::expired), and the connector drops
/// the entries of those cids. So the maps stay as big as a day of trading and not as the whole life of the
/// process.
///
/// # Examples
///
/// ```
/// use std::time::{Duration, Instant};
/// use brokapi_runtime::FinishedWindow;
///
/// let mut window = FinishedWindow::with_limits(Duration::from_secs(60), 100);
/// let t0 = Instant::now();
/// window.push(t0, "a");
/// window.push(t0 + Duration::from_secs(30), "b");
/// assert!(window.expired(t0 + Duration::from_secs(59)).is_empty());
/// assert_eq!(window.expired(t0 + Duration::from_secs(61)), vec!["a"]);
/// assert_eq!(window.len(), 1);
/// ```
#[derive(Debug)]
pub struct FinishedWindow<K> {
    window: Duration,
    max: usize,
    items: VecDeque<(Instant, K)>,
}

impl<K> Default for FinishedWindow<K> {
    fn default() -> Self {
        Self::with_limits(RETENTION, RETAINED_FINISHED)
    }
}

impl<K> FinishedWindow<K> {
    /// A window of [`RETENTION`] and [`RETAINED_FINISHED`].
    pub fn new() -> Self {
        Self::default()
    }

    /// A window that keeps a key for `window`, and at most `max` keys.
    pub fn with_limits(window: Duration, max: usize) -> Self {
        Self {
            window,
            max,
            items: VecDeque::new(),
        }
    }

    /// Notes that the order `key` finished at `now`.
    pub fn push(&mut self, now: Instant, key: K) {
        self.items.push_back((now, key));
    }

    /// Takes out the keys that are older than the window as of `now`, or beyond the count limit, oldest
    /// first. The caller drops what it kept for them.
    pub fn expired(&mut self, now: Instant) -> Vec<K> {
        let mut out = Vec::new();
        while let Some((at, _)) = self.items.front() {
            let old = now.saturating_duration_since(*at) >= self.window;
            if !(old || self.items.len() > self.max) {
                break;
            }
            if let Some((_, key)) = self.items.pop_front() {
                out.push(key);
            }
        }
        out
    }

    /// How many keys are held.
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// `true` when no key is held.
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}
