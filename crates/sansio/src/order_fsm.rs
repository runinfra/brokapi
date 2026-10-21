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
/// The machine takes the order in which news really arrives. The stream of fills can be faster than the
/// answer to the place command, so a fill or a cancel is valid in `InFlight` too.
///
/// | State | Signal -> new state |
/// |---|---|
/// | `InFlight` | `PlaceSent` -> no change, `Ack` -> `Acked`, `PartialFill` -> `PartiallyFilled`, `Filled`, `Cancelled`, `Rejected`, `Timeout` -> `Unknown` |
/// | `Unknown` | `Ack` -> `Acked`, `PartialFill` -> `PartiallyFilled`, `Filled`, `Cancelled`, `Rejected`, `Timeout` -> no change |
/// | `Acked` | `PartialFill` -> `PartiallyFilled`, `Filled`, `Cancelled`, `Rejected`, `Ack` -> no change (late repeat) |
/// | `PartiallyFilled` | `PartialFill` -> `PartiallyFilled` (new quantities, but a smaller `filled` than known is a stale snapshot and changes nothing), `Filled`, `Cancelled`, `Rejected`, `Ack` -> no change |
/// | `Filled`, `Cancelled`, `Rejected` | the same final state again, and a late `Ack` or `PartialFill` -> no change. A different final state (a cancel of a filled order) -> [`FsmError::InvalidTransition`] |
///
/// News that is late or repeated but agrees with what is known changes nothing (`Ok(None)`). News that
/// contradicts it is an error and the state is left as it was (the order book of `brokapi-runtime` logs
/// it at warn level).
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
/// // A finished order accepts nothing that contradicts it.
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
        self.state.cid()
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
    let cid = state.cid();

    let signal_label = signal_name(&signal);

    match (state, signal) {
        (OrderState::InFlight { .. }, OrderSignal::PlaceSent) => Ok(state.clone()),

        // The stream can be faster than the answer to the place command, and an order that is only
        // `Unknown` may have moved on without us: a fill, a partial fill or a cancel is a fact, so it is
        // taken from every state that has not seen it yet.
        (
            OrderState::InFlight { .. } | OrderState::Unknown { .. },
            OrderSignal::Ack { broker_id },
        ) => Ok(OrderState::Acked { cid, broker_id }),
        (
            OrderState::InFlight { .. } | OrderState::Unknown { .. } | OrderState::Acked { .. },
            OrderSignal::PartialFill { filled, left },
        ) => Ok(OrderState::PartiallyFilled { cid, filled, left }),
        (
            OrderState::InFlight { .. }
            | OrderState::Unknown { .. }
            | OrderState::Acked { .. }
            | OrderState::PartiallyFilled { .. },
            OrderSignal::Filled,
        ) => Ok(OrderState::Filled { cid }),
        (
            OrderState::InFlight { .. }
            | OrderState::Unknown { .. }
            | OrderState::Acked { .. }
            | OrderState::PartiallyFilled { .. },
            OrderSignal::Cancelled,
        ) => Ok(OrderState::Cancelled { cid }),
        (
            OrderState::InFlight { .. }
            | OrderState::Unknown { .. }
            | OrderState::Acked { .. }
            | OrderState::PartiallyFilled { .. },
            OrderSignal::Rejected { reason },
        ) => Ok(OrderState::Rejected { cid, reason }),

        (OrderState::InFlight { .. }, OrderSignal::Timeout) => {
            Ok(OrderState::Unknown { cid, since: now })
        }

        (
            OrderState::PartiallyFilled { filled: have, .. },
            OrderSignal::PartialFill { filled, left },
        ) => {
            // An older snapshot that arrives late must not take the fill back.
            if filled < *have {
                Ok(state.clone())
            } else {
                Ok(OrderState::PartiallyFilled { cid, filled, left })
            }
        }

        // Late, repeated or out-of-order news that agrees with what is known: nothing to change. An
        // answer to the place command that comes after the fills, a second `Timeout`, the same final
        // state told twice.
        (
            OrderState::Unknown { .. }
            | OrderState::Acked { .. }
            | OrderState::PartiallyFilled { .. }
            | OrderState::Filled { .. }
            | OrderState::Cancelled { .. }
            | OrderState::Rejected { .. },
            OrderSignal::Timeout | OrderSignal::PlaceSent | OrderSignal::Ack { .. },
        )
        | (
            OrderState::Filled { .. } | OrderState::Cancelled { .. } | OrderState::Rejected { .. },
            OrderSignal::PartialFill { .. },
        )
        | (OrderState::Filled { .. }, OrderSignal::Filled)
        | (OrderState::Cancelled { .. }, OrderSignal::Cancelled)
        | (OrderState::Rejected { .. }, OrderSignal::Rejected { .. }) => Ok(state.clone()),

        // The rest contradicts what is known (a cancel of a filled order, a fill of a cancelled one).
        (from, _) => Err(FsmError::InvalidTransition {
            from: state_name(from),
            signal: signal_label,
        }),
    }
}

fn state_name(state: &OrderState) -> &'static str {
    state.name()
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

    /// Stops tracking `cid`, but only if its order is finished (`Filled`, `Cancelled` or `Rejected`).
    /// Returns `true` when it was removed. A live order, or an unknown cid, is left alone.
    pub fn forget_finished(&mut self, cid: ClientOrderId) -> bool {
        let finished = self.orders.get(&cid).is_some_and(|f| f.state().is_final());
        if finished {
            self.orders.remove(&cid);
        }
        finished
    }

    /// How many orders are tracked, in any state.
    pub fn len(&self) -> usize {
        self.orders.len()
    }

    /// `true` when no order is tracked.
    pub fn is_empty(&self) -> bool {
        self.orders.is_empty()
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

    fn ack(id: &str) -> OrderSignal {
        OrderSignal::Ack {
            broker_id: BrokerOrderId::new(id),
        }
    }

    fn partial(filled: i64, left: i64) -> OrderSignal {
        OrderSignal::PartialFill {
            filled: Qty::from(filled),
            left: Qty::from(left),
        }
    }

    #[test]
    fn news_that_is_faster_than_the_answer_is_taken() {
        // A fill, a partial fill and a cancel can come before the answer to the place command.
        let mut fsm = OrderFsm::new(cid(1), now());
        assert!(matches!(
            fsm.apply(OrderSignal::Filled, now()),
            Ok(Some(OrderState::Filled { .. }))
        ));
        let mut fsm = OrderFsm::new(cid(2), now());
        assert!(matches!(
            fsm.apply(partial(1, 2), now()),
            Ok(Some(OrderState::PartiallyFilled { .. }))
        ));
        let mut fsm = OrderFsm::new(cid(3), now());
        assert!(matches!(
            fsm.apply(OrderSignal::Cancelled, now()),
            Ok(Some(OrderState::Cancelled { .. }))
        ));
        // And the doubt of an Unknown order is settled by a fill or a partial fill too.
        let mut fsm = OrderFsm::from_state(OrderState::Unknown {
            cid: cid(4),
            since: now(),
        });
        assert!(matches!(
            fsm.apply(partial(1, 2), now()),
            Ok(Some(OrderState::PartiallyFilled { .. }))
        ));
    }

    #[test]
    fn a_late_answer_changes_nothing() {
        // The answer to the place command comes after the fill: the order stays filled.
        let mut fsm = OrderFsm::new(cid(1), now());
        fsm.apply(OrderSignal::Filled, now()).unwrap();
        for late in [
            ack("42"),
            partial(1, 1),
            OrderSignal::Timeout,
            OrderSignal::PlaceSent,
        ] {
            assert_eq!(fsm.apply(late, now()), Ok(None));
        }
        assert!(matches!(fsm.state(), OrderState::Filled { .. }));
        // The same final state told twice is not an error either.
        assert_eq!(fsm.apply(OrderSignal::Filled, now()), Ok(None));
        let mut cancelled = OrderFsm::from_state(OrderState::Cancelled { cid: cid(2) });
        assert_eq!(cancelled.apply(OrderSignal::Cancelled, now()), Ok(None));
        let mut rejected = OrderFsm::from_state(OrderState::Rejected {
            cid: cid(3),
            reason: "no".into(),
        });
        assert_eq!(
            rejected.apply(
                OrderSignal::Rejected {
                    reason: "no".into()
                },
                now()
            ),
            Ok(None)
        );
        // A repeated ack of a working order, with the order partly filled.
        let mut working = OrderFsm::from_state(OrderState::Acked {
            cid: cid(4),
            broker_id: BrokerOrderId::new("1"),
        });
        assert_eq!(working.apply(ack("1"), now()), Ok(None));
        let mut part = OrderFsm::from_state(OrderState::PartiallyFilled {
            cid: cid(5),
            filled: Qty::from(1),
            left: Qty::from(1),
        });
        assert_eq!(part.apply(ack("1"), now()), Ok(None));
    }

    #[test]
    fn an_older_snapshot_does_not_take_a_fill_back() {
        let mut fsm = OrderFsm::from_state(OrderState::PartiallyFilled {
            cid: cid(1),
            filled: Qty::from(5),
            left: Qty::from(5),
        });
        // A snapshot with less filled than known arrives late.
        assert_eq!(fsm.apply(partial(3, 7), now()), Ok(None));
        assert!(matches!(
            fsm.state(),
            OrderState::PartiallyFilled { filled, .. } if *filled == Qty::from(5)
        ));
        // More filled moves it on.
        assert!(matches!(fsm.apply(partial(6, 4), now()), Ok(Some(_))));
    }

    #[test]
    fn contradicting_news_is_an_error_and_leaves_the_state() {
        let mut fsm = OrderFsm::from_state(OrderState::Filled { cid: cid(1) });
        assert_eq!(
            fsm.apply(OrderSignal::Cancelled, now()),
            Err(FsmError::InvalidTransition {
                from: "Filled",
                signal: "Cancelled"
            })
        );
        assert!(matches!(fsm.state(), OrderState::Filled { .. }));
        let mut cancelled = OrderFsm::from_state(OrderState::Cancelled { cid: cid(2) });
        assert!(cancelled.apply(OrderSignal::Filled, now()).is_err());
    }

    #[test]
    fn the_registry_forgets_only_finished_orders() {
        let mut reg = OrderRegistry::new();
        let t0 = Instant::now();
        reg.register_place(cid(1), t0);
        reg.register_place(cid(2), t0);
        reg.apply(cid(2), OrderSignal::Filled, t0).unwrap();
        assert_eq!(reg.len(), 2);
        assert!(!reg.is_empty());
        assert!(!reg.forget_finished(cid(1)), "a live order stays");
        assert!(!reg.forget_finished(cid(9)), "an unknown cid is left alone");
        assert!(reg.forget_finished(cid(2)));
        assert_eq!(reg.len(), 1);
        assert!(reg.get(cid(2)).is_none());
    }
}
