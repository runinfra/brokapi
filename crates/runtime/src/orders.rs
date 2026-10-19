//! In-memory order tracking.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use brokapi_core::{ClientOrderId, OrderState};
use brokapi_sansio::{OrderRegistry, OrderSignal};

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
#[derive(Debug)]
pub struct OrderRuntime {
    registry: OrderRegistry,
    command_timeout: Duration,
    place_attempts: HashMap<ClientOrderId, u8>,
}

impl Default for OrderRuntime {
    fn default() -> Self {
        Self::new()
    }
}

impl OrderRuntime {
    /// Creates an empty order book with a command timeout of 30 seconds.
    pub fn new() -> Self {
        Self {
            registry: OrderRegistry::new(),
            command_timeout: Duration::from_secs(30),
            place_attempts: HashMap::new(),
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
        !self.place_attempts.contains_key(&cid) && {
            self.place_attempts.insert(cid, 1);
            true
        } || false
    }

    /// Returns `true` only on the first place attempt for this cid, and records that attempt.
    ///
    /// Call it before sending. The attempt is recorded even when the send then fails locally (for
    /// example a rate-limit refusal), so the same cid is never used twice. To try again, make a new
    /// cid. After [`register_place`](Self::register_place) the answer is always `false`.
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
    /// anything after `Filled`). The error is not reported, so a caller that needs to tell these
    /// apart should use [`brokapi_sansio::OrderFsm::apply`] directly.
    pub fn apply_signal(
        &mut self,
        cid: ClientOrderId,
        signal: OrderSignal,
        now: Instant,
    ) -> Option<OrderState> {
        self.registry.apply(cid, signal, now).ok().flatten()
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
