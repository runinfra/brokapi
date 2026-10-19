//! Graceful / immediate shutdown.

use std::time::Instant;

use brokapi_core::{ShutdownMode, ShutdownReport};
use tokio_util::sync::CancellationToken;

use crate::command_gate::CommandGate;
use crate::orders::OrderRuntime;

/// Shutdown coordinator for one connector instance: it closes the command gate, gives up waiting for
/// in-flight commands and cancels the background tasks.
///
/// The controller does no network I/O. The connector supplies the work that needs the network (the
/// unsubscribe calls) as a closure. Orders at the broker are never cancelled by a shutdown: only
/// the local wait for an answer is given up (orders in `InFlight` become `Unknown`).
///
/// # Examples
///
/// ```
/// use std::time::{Duration, Instant};
/// use brokapi_core::{ClientOrderId, Error, ShutdownMode};
/// use brokapi_runtime::{CommandGate, OrderRuntime, ShutdownController};
///
/// let mut gate = CommandGate::new();
/// gate.mark_reconciled();
/// let mut ctl = ShutdownController::new(gate);
/// let task_token = ctl.child_token();
///
/// let mut orders = OrderRuntime::new();
/// let now = Instant::now();
/// let cid = ClientOrderId::new_v4();
/// orders.register_place(cid, now);
///
/// // Two subscriptions were closed, none failed.
/// let report = ctl.shutdown_local(
///     ShutdownMode::Graceful { deadline: Duration::from_secs(5) },
///     now,
///     &mut orders,
///     || (2, 0),
/// );
///
/// assert_eq!((report.unsubscribed, report.unsubscribe_failed), (2, 0));
/// assert_eq!(report.inflight_abandoned, vec![cid]);
/// assert!(task_token.is_cancelled());
/// assert!(matches!(ctl.gate().check_command(), Err(Error::ShuttingDown)));
/// ```
#[derive(Debug)]
pub struct ShutdownController {
    /// Parent cancellation token. Cancelled by [`shutdown_local`](Self::shutdown_local).
    pub root: CancellationToken,
    gate: CommandGate,
}

impl ShutdownController {
    /// Creates a controller that owns the connector's command gate.
    pub fn new(gate: CommandGate) -> Self {
        Self {
            root: CancellationToken::new(),
            gate,
        }
    }

    /// A token for one background task. It is cancelled when the controller shuts down.
    pub fn child_token(&self) -> CancellationToken {
        self.root.child_token()
    }

    /// The command gate, for checking commands.
    pub fn gate(&self) -> &CommandGate {
        &self.gate
    }

    /// The command gate, for marking it reconciled or degraded.
    pub fn gate_mut(&mut self) -> &mut CommandGate {
        &mut self.gate
    }

    /// Runs the local shutdown steps in order and returns a report.
    ///
    /// 1. Closes the command gate: new commands fail with [`brokapi_core::Error::ShuttingDown`].
    /// 2. In [`ShutdownMode::Graceful`] calls `unsubscribe` once. It returns how many
    ///    subscriptions were closed and how many failed. In [`ShutdownMode::Immediate`] it is not
    ///    called and both counts are zero.
    /// 3. Moves in-flight orders to `Unknown` (see [`OrderRuntime::abandon_inflight`]).
    /// 4. Cancels the root token, which stops every task made with
    ///    [`child_token`](Self::child_token).
    ///
    /// The `deadline` of `Graceful` is not applied here. This method runs to completion. Use
    /// [`wait_graceful_deadline`](Self::wait_graceful_deadline) to wait for it. Calling the method
    /// twice is harmless: the second report has no abandoned orders.
    pub fn shutdown_local(
        &mut self,
        mode: ShutdownMode,
        now: Instant,
        orders: &mut OrderRuntime,
        mut unsubscribe: impl FnMut() -> (usize, usize),
    ) -> ShutdownReport {
        self.gate.begin_shutdown();

        let (unsubscribed, unsubscribe_failed) = match mode {
            ShutdownMode::Immediate => (0, 0),
            ShutdownMode::Graceful { .. } => unsubscribe(),
        };

        let inflight_abandoned = orders.abandon_inflight(now);
        self.root.cancel();

        ShutdownReport {
            unsubscribed,
            unsubscribe_failed,
            inflight_abandoned,
        }
    }

    /// Sleeps for the `deadline` of a `Graceful` mode, and returns at once for `Immediate`.
    ///
    /// It always sleeps the full deadline. It does not return early when the work is done, so use
    /// it only to bound a wait together with your own completion signal.
    pub async fn wait_graceful_deadline(&self, mode: ShutdownMode) {
        if let ShutdownMode::Graceful { deadline } = mode {
            tokio::time::sleep(deadline).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use brokapi_core::{ClientOrderId, OrderState, ShutdownMode};

    fn cid(n: u8) -> ClientOrderId {
        ClientOrderId::from_bytes([n; 16])
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_abandons_inflight_orders() {
        let gate = CommandGate::new();
        let mut ctl = ShutdownController::new(gate);
        let mut orders = OrderRuntime::new();
        let now = Instant::now();
        orders.register_place(cid(1), now);
        orders.register_place(cid(2), now);

        let report = ctl.shutdown_local(ShutdownMode::Immediate, now, &mut orders, || (0, 0));

        assert_eq!(report.inflight_abandoned.len(), 2);
        assert!(matches!(
            orders.state(cid(1)),
            Some(OrderState::Unknown { .. })
        ));
    }
}
