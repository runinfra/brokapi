//! In-memory order lifecycle FSM, level-1 tests.

use std::collections::HashMap;
use std::time::Instant;

use brokapi_core::{BrokerOrderId, ClientOrderId, OrderState, Qty};

/// Something that happened to an order, fed to [`OrderFsm::apply`].
///
/// See [`OrderFsm`] for which signal is valid in which state.
#[derive(Debug, Clone, PartialEq)]
pub enum OrderSignal {
    /// The place command went out on the wire. Changes nothing, the order is already in flight.
    PlaceSent,
    /// The broker accepted the order and gave it an id.
    Ack {
        /// The broker's id for the order.
        broker_id: BrokerOrderId,
    },
    /// Part of the order was filled.
    PartialFill {
        /// Quantity filled so far, in total.
        filled: Qty,
        /// Quantity still open.
        left: Qty,
    },
    /// The order is completely filled.
    Filled,
    /// The order was cancelled.
    Cancelled,
    /// The broker refused the order.
    Rejected {
        /// The broker's reason, for logs.
        reason: String,
    },
    /// No answer to the place command came in time. The order may or may not exist at the broker.
    Timeout,
}

/// Why a signal could not be applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FsmError {
    /// [`OrderRegistry::apply`] was given a client order id it does not track.
    UnknownOrder(ClientOrderId),
    /// The signal is not valid in the order's current state. The state is left as it was.
    InvalidTransition {
        /// Name of the current state, for example `"Filled"`.
        from: &'static str,
        /// Name of the signal, for example `"Cancelled"`.
        signal: &'static str,
    },
}

/// The life of one order as a finite state machine over [`OrderState`].
///
/// The order starts `InFlight` (the place command is on the wire). A broker answer moves it on. If
/// no answer comes, [`OrderSignal::Timeout`] moves it to `Unknown`. `Unknown` is not a failure: the
/// order may exist at the broker, and a later ack, fill or cancel (from the stream or from a
/// reconcile) settles it. The command is never repeated, because a retry could create a second
/// order.
///
/// Valid signals by state (anything else is [`FsmError::InvalidTransition`]):
///
/// | State | Signal -> new state |
/// |---|---|
/// | `InFlight` | `PlaceSent` -> no change, `Ack` -> `Acked`, `Timeout` -> `Unknown`, `Rejected` -> `Rejected` |
/// | `Unknown` | `Ack` -> `Acked`, `Rejected` -> `Rejected`, `Cancelled` -> `Cancelled`, `Filled` -> `Filled`, `Timeout` -> no change |
/// | `Acked` | `PartialFill` -> `PartiallyFilled`, `Filled` -> `Filled`, `Cancelled` -> `Cancelled`, `Rejected` -> `Rejected` |
/// | `PartiallyFilled` | `PartialFill` -> `PartiallyFilled` (new quantities), `Filled`, `Cancelled`, `Rejected` |
/// | `Filled`, `Cancelled`, `Rejected` | nothing: every signal is invalid |
///
/// Note that a fill or cancel signal in `InFlight` is invalid: the order must be acked first (or
/// have timed out into `Unknown`).
///
/// # Examples
///
/// ```
/// use std::time::Instant;
/// use brokapi_core::{BrokerOrderId, ClientOrderId, OrderState};
/// use brokapi_sansio::{FsmError, OrderFsm, OrderSignal};
///
/// let cid = ClientOrderId::new_v4();
/// let now = Instant::now();
/// let mut fsm = OrderFsm::new(cid, now);
/// assert!(matches!(fsm.state(), OrderState::InFlight { .. }));
///
/// // No answer in time: the outcome is unknown.
/// let next = fsm.apply(OrderSignal::Timeout, now)?;
/// assert!(matches!(next, Some(OrderState::Unknown { .. })));
///
/// // A second timeout changes nothing: Ok(None).
/// assert_eq!(fsm.apply(OrderSignal::Timeout, now), Ok(None));
///
/// // The stream later shows the order. It was alive all along.
/// let next = fsm.apply(OrderSignal::Ack { broker_id: BrokerOrderId::new("42") }, now)?;
/// assert!(matches!(next, Some(OrderState::Acked { .. })));
///
/// fsm.apply(OrderSignal::Filled, now)?;
/// // A finished order accepts nothing more.
/// assert!(matches!(
///     fsm.apply(OrderSignal::Cancelled, now),
///     Err(FsmError::InvalidTransition { from: "Filled", signal: "Cancelled" })
/// ));
/// # Ok::<(), FsmError>(())
/// ```
#[derive(Debug, Clone)]
pub struct OrderFsm {
    state: OrderState,
}

impl OrderFsm {
    /// Starts tracking an order whose place command is sent at `now`, in state `InFlight`.
    pub fn new(cid: ClientOrderId, now: Instant) -> Self {
        Self {
            state: OrderState::InFlight { cid, sent_at: now },
        }
    }

    /// Starts from a known state, for example an order read back from the broker.
    pub fn from_state(state: OrderState) -> Self {
        Self { state }
    }

    /// The current state.
    pub fn state(&self) -> &OrderState {
        &self.state
    }

    /// The client order id, the same in every state.
    pub fn cid(&self) -> ClientOrderId {
        match &self.state {
            OrderState::InFlight { cid, .. }
            | OrderState::Unknown { cid, .. }
            | OrderState::Acked { cid, .. }
            | OrderState::PartiallyFilled { cid, .. }
            | OrderState::Filled { cid }
            | OrderState::Cancelled { cid }
            | OrderState::Rejected { cid, .. } => *cid,
        }
    }

    /// Applies a signal at `now`.
    ///
    /// Returns `Ok(Some(new_state))` when the state changed, and `Ok(None)` when the signal is
    /// valid but changes nothing (a repeated `Timeout` in `Unknown`, `PlaceSent` in `InFlight`, a
    /// `PartialFill` with the same quantities). `now` is stored in the `Unknown` state as the time
    /// the doubt began.
    ///
    /// # Errors
    ///
    /// [`FsmError::InvalidTransition`] when the signal is not valid in the current state, see the
    /// table in the type docs. The state is left unchanged.
    pub fn apply(
        &mut self,
        signal: OrderSignal,
        now: Instant,
    ) -> Result<Option<OrderState>, FsmError> {
        let prev = self.state.clone();
        let next = transition(&self.state, signal, now)?;
        if next != prev {
            self.state = next.clone();
            Ok(Some(next))
        } else {
            Ok(None)
        }
    }
}

fn transition(
    state: &OrderState,
    signal: OrderSignal,
    now: Instant,
) -> Result<OrderState, FsmError> {
    let cid = match state {
        OrderState::InFlight { cid, .. }
        | OrderState::Unknown { cid, .. }
        | OrderState::Acked { cid, .. }
        | OrderState::PartiallyFilled { cid, .. }
        | OrderState::Filled { cid }
        | OrderState::Cancelled { cid }
        | OrderState::Rejected { cid, .. } => *cid,
    };

    let signal_label = signal_name(&signal);

    match (state, signal) {
        (OrderState::InFlight { .. }, OrderSignal::PlaceSent) => Ok(state.clone()),

        (OrderState::InFlight { .. }, OrderSignal::Ack { broker_id }) => {
            Ok(OrderState::Acked { cid, broker_id })
        }

        (OrderState::InFlight { .. }, OrderSignal::Timeout) => {
            Ok(OrderState::Unknown { cid, since: now })
        }

        (OrderState::InFlight { .. }, OrderSignal::Rejected { reason }) => {
            Ok(OrderState::Rejected { cid, reason })
        }

        (OrderState::Unknown { .. }, OrderSignal::Ack { broker_id }) => {
            Ok(OrderState::Acked { cid, broker_id })
        }

        (OrderState::Unknown { .. }, OrderSignal::Rejected { reason }) => {
            Ok(OrderState::Rejected { cid, reason })
        }

        (OrderState::Unknown { .. }, OrderSignal::Cancelled) => Ok(OrderState::Cancelled { cid }),

        (OrderState::Unknown { .. }, OrderSignal::Filled) => Ok(OrderState::Filled { cid }),

        (OrderState::Unknown { .. }, OrderSignal::Timeout) => Ok(state.clone()),

        (OrderState::Acked { .. }, OrderSignal::PartialFill { filled, left }) => {
            Ok(OrderState::PartiallyFilled { cid, filled, left })
        }

        (OrderState::Acked { .. }, OrderSignal::Filled) => Ok(OrderState::Filled { cid }),

        (OrderState::Acked { .. }, OrderSignal::Cancelled) => Ok(OrderState::Cancelled { cid }),

        (OrderState::Acked { .. }, OrderSignal::Rejected { reason }) => {
            Ok(OrderState::Rejected { cid, reason })
        }

        (OrderState::PartiallyFilled { .. }, OrderSignal::PartialFill { filled, left }) => {
            Ok(OrderState::PartiallyFilled { cid, filled, left })
        }

        (OrderState::PartiallyFilled { .. }, OrderSignal::Filled) => Ok(OrderState::Filled { cid }),

        (OrderState::PartiallyFilled { .. }, OrderSignal::Cancelled) => {
            Ok(OrderState::Cancelled { cid })
        }

        (OrderState::PartiallyFilled { .. }, OrderSignal::Rejected { reason }) => {
            Ok(OrderState::Rejected { cid, reason })
        }

        (OrderState::Filled { .. }, _)
        | (OrderState::Cancelled { .. }, _)
        | (OrderState::Rejected { .. }, _) => Err(FsmError::InvalidTransition {
            from: state_name(state),
            signal: signal_label,
        }),

        (from, _) => Err(FsmError::InvalidTransition {
            from: state_name(from),
            signal: signal_label,
        }),
    }
}

fn state_name(state: &OrderState) -> &'static str {
    match state {
        OrderState::InFlight { .. } => "InFlight",
        OrderState::Unknown { .. } => "Unknown",
        OrderState::Acked { .. } => "Acked",
        OrderState::PartiallyFilled { .. } => "PartiallyFilled",
        OrderState::Filled { .. } => "Filled",
        OrderState::Cancelled { .. } => "Cancelled",
        OrderState::Rejected { .. } => "Rejected",
    }
}

fn signal_name(signal: &OrderSignal) -> &'static str {
    match signal {
        OrderSignal::PlaceSent => "PlaceSent",
        OrderSignal::Ack { .. } => "Ack",
        OrderSignal::PartialFill { .. } => "PartialFill",
        OrderSignal::Filled => "Filled",
        OrderSignal::Cancelled => "Cancelled",
        OrderSignal::Rejected { .. } => "Rejected",
        OrderSignal::Timeout => "Timeout",
    }
}

/// Many [`OrderFsm`]s keyed by client order id.
///
/// # Examples
///
/// ```
/// use std::time::{Duration, Instant};
/// use brokapi_core::{ClientOrderId, OrderState};
/// use brokapi_sansio::OrderRegistry;
///
/// let mut registry = OrderRegistry::new();
/// let t0 = Instant::now();
/// let (a, b) = (ClientOrderId::new_v4(), ClientOrderId::new_v4());
/// registry.register_place(a, t0);
/// registry.register_place(b, t0 + Duration::from_secs(20));
///
/// // 31 s after `a` was sent, with a 30 s limit: only `a` has gone silent.
/// let changed = registry.on_global_timeout(t0 + Duration::from_secs(31), Duration::from_secs(30));
/// assert_eq!(changed.len(), 1);
/// assert_eq!(changed[0].0, a);
/// assert!(matches!(changed[0].1, OrderState::Unknown { .. }));
/// assert_eq!(registry.in_flight_cids(), vec![b]);
/// ```
#[derive(Debug, Default)]
pub struct OrderRegistry {
    orders: HashMap<ClientOrderId, OrderFsm>,
}

impl OrderRegistry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Starts tracking `cid` as `InFlight`, sent at `now`. An order already tracked under this cid
    /// is replaced.
    pub fn register_place(&mut self, cid: ClientOrderId, now: Instant) {
        self.orders.insert(cid, OrderFsm::new(cid, now));
    }

    /// Applies a signal to the order `cid`, see [`OrderFsm::apply`] for the result.
    ///
    /// # Errors
    ///
    /// [`FsmError::UnknownOrder`] when `cid` is not tracked, and every error of [`OrderFsm::apply`].
    pub fn apply(
        &mut self,
        cid: ClientOrderId,
        signal: OrderSignal,
        now: Instant,
    ) -> Result<Option<OrderState>, FsmError> {
        let fsm = self
            .orders
            .get_mut(&cid)
            .ok_or(FsmError::UnknownOrder(cid))?;
        fsm.apply(signal, now)
    }

    /// The state machine of `cid`, if tracked.
    pub fn get(&self, cid: ClientOrderId) -> Option<&OrderFsm> {
        self.orders.get(&cid)
    }

    /// The cids of all orders now in state `InFlight`, in no particular order.
    pub fn in_flight_cids(&self) -> Vec<ClientOrderId> {
        self.orders
            .iter()
            .filter_map(|(cid, fsm)| {
                if matches!(fsm.state(), OrderState::InFlight { .. }) {
                    Some(*cid)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Moves every `InFlight` order to `Unknown` now, whatever its age (shutdown).
    ///
    /// Returns each changed cid with its new state. Nothing is sent to the broker.
    pub fn abandon_inflight(&mut self, now: Instant) -> Vec<(ClientOrderId, OrderState)> {
        let inflight: Vec<ClientOrderId> = self.in_flight_cids();
        let mut abandoned = Vec::new();
        for cid in inflight {
            if let Ok(Some(next)) = self.apply(cid, OrderSignal::Timeout, now) {
                abandoned.push((cid, next));
            }
        }
        abandoned
    }

    /// Moves every `InFlight` order that has waited `timeout` or longer (since it was registered) to
    /// `Unknown`, and returns the changed ones with their new state.
    ///
    /// Nothing is sent to the broker. Orders in other states are not touched.
    pub fn on_global_timeout(
        &mut self,
        now: Instant,
        timeout: std::time::Duration,
    ) -> Vec<(ClientOrderId, OrderState)> {
        let mut changed = Vec::new();
        for (cid, fsm) in &mut self.orders {
            if let OrderState::InFlight { sent_at, .. } = fsm.state()
                && now.duration_since(*sent_at) >= timeout
                && let Ok(Some(next)) = fsm.apply(OrderSignal::Timeout, now)
            {
                changed.push((*cid, next));
            }
        }
        changed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn cid(n: u8) -> ClientOrderId {
        ClientOrderId::from_bytes([n; 16])
    }

    fn now() -> Instant {
        Instant::now()
    }

    #[test]
    fn in_flight_ack_to_acked() {
        let mut fsm = OrderFsm::new(cid(1), now());
        let next = fsm
            .apply(
                OrderSignal::Ack {
                    broker_id: BrokerOrderId::new("42"),
                },
                now(),
            )
            .unwrap()
            .unwrap();
        assert!(matches!(next, OrderState::Acked { .. }));
    }

    #[test]
    fn in_flight_timeout_to_unknown() {
        let mut fsm = OrderFsm::new(cid(1), now());
        let next = fsm.apply(OrderSignal::Timeout, now()).unwrap().unwrap();
        assert!(matches!(next, OrderState::Unknown { .. }));
    }

    #[test]
    fn unknown_recovery_via_ack() {
        let mut fsm = OrderFsm::from_state(OrderState::Unknown {
            cid: cid(1),
            since: now(),
        });
        let next = fsm
            .apply(
                OrderSignal::Ack {
                    broker_id: BrokerOrderId::new("99"),
                },
                now(),
            )
            .unwrap()
            .unwrap();
        assert!(matches!(next, OrderState::Acked { .. }));
    }

    #[test]
    fn terminal_states_reject_signals() {
        let mut fsm = OrderFsm::from_state(OrderState::Filled { cid: cid(1) });
        assert!(fsm.apply(OrderSignal::Cancelled, now()).is_err());
    }

    #[test]
    fn registry_timeout_batch() {
        let mut reg = OrderRegistry::new();
        let t0 = Instant::now();
        reg.register_place(cid(1), t0);
        let changed = reg.on_global_timeout(t0 + Duration::from_secs(31), Duration::from_secs(30));
        assert_eq!(changed.len(), 1);
        assert!(matches!(changed[0].1, OrderState::Unknown { .. }));
    }
}
