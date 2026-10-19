//! Trading command admission control.

use brokapi_core::Error;

/// Decides whether a trading command may go out now.
///
/// A connector asks the gate before it sends a place, cancel or replace. The gate refuses in three
/// cases, checked in this order: the connector is shutting down, it is degraded, it has not been
/// reconciled yet. The gate starts closed (not reconciled), so the first reconcile with the broker
/// opens it. Why: before the client knows which orders already exist at the broker, a new command
/// could duplicate or fight an order it cannot see.
///
/// The gate only holds flags. The connector sets them: [`mark_reconciled`](Self::mark_reconciled)
/// after a successful reconcile, [`mark_degraded`](Self::mark_degraded) when the
/// [`Emitter`](crate::Emitter) reports an overflow, [`begin_shutdown`](Self::begin_shutdown) when
/// closing. Reads (order book, portfolio, active orders) are not gated.
///
/// # Examples
///
/// ```
/// use brokapi_core::Error;
/// use brokapi_runtime::CommandGate;
///
/// let mut gate = CommandGate::new();
/// // A new gate refuses commands until the first reconcile.
/// assert!(matches!(gate.check_command(), Err(Error::NotReconciled)));
///
/// gate.mark_reconciled();
/// assert!(gate.check_command().is_ok());
///
/// // An overflow of a lossless stream closes it again until the next reconcile.
/// gate.mark_degraded();
/// assert!(matches!(gate.check_command(), Err(Error::Degraded)));
/// gate.clear_degraded();
/// assert!(gate.check_command().is_ok());
///
/// // Shutdown wins over everything else and is final.
/// gate.begin_shutdown();
/// assert!(matches!(gate.check_command(), Err(Error::ShuttingDown)));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommandGate {
    reconciled: bool,
    degraded: bool,
    shutting_down: bool,
}

impl Default for CommandGate {
    /// A closed gate: not reconciled, not degraded, not shutting down.
    fn default() -> Self {
        Self {
            reconciled: false,
            degraded: false,
            shutting_down: false,
        }
    }
}

impl CommandGate {
    /// Creates a closed gate. Same as [`Default::default`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether [`mark_reconciled`](Self::mark_reconciled) was called.
    pub fn is_reconciled(&self) -> bool {
        self.reconciled
    }

    /// Whether the gate is in the degraded state.
    pub fn is_degraded(&self) -> bool {
        self.degraded
    }

    /// Records a successful reconcile. There is no way to undo it.
    pub fn mark_reconciled(&mut self) {
        self.reconciled = true;
    }

    /// Records that events were lost (see [`Emitter::is_degraded`](crate::Emitter::is_degraded)).
    /// Commands are refused until [`clear_degraded`](Self::clear_degraded).
    pub fn mark_degraded(&mut self) {
        self.degraded = true;
    }

    /// Leaves the degraded state. Call it after the reconcile that made up for the lost events.
    pub fn clear_degraded(&mut self) {
        self.degraded = false;
    }

    /// Closes the gate for good: every later check fails with [`Error::ShuttingDown`].
    pub fn begin_shutdown(&mut self) {
        self.shutting_down = true;
    }

    /// Says whether a trading command may be sent now.
    ///
    /// # Errors
    ///
    /// In this order of priority:
    ///
    /// - [`Error::ShuttingDown`] after [`begin_shutdown`](Self::begin_shutdown).
    /// - [`Error::Degraded`] while the gate is degraded.
    /// - [`Error::NotReconciled`] until [`mark_reconciled`](Self::mark_reconciled).
    ///
    /// All three mean nothing was sent, so the command may be repeated once the cause is gone
    /// (except after a shutdown).
    pub fn check_command(&self) -> Result<(), Error> {
        if self.shutting_down {
            return Err(Error::ShuttingDown);
        }
        if self.degraded {
            return Err(Error::Degraded);
        }
        if !self.reconciled {
            return Err(Error::NotReconciled);
        }
        Ok(())
    }
}
