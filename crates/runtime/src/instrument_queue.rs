//! Per-instrument command serialization (ordered_command_channel: false).
//!
//! Commands on one instrument run one at a time, in arrival order (tokio mutexes are FIFO):
//! a cancel followed by a place cannot be reordered on the wire. The guard is RAII, so the
//! instrument is released on every exit path, including errors and dropped futures.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use brokapi_core::{ClientOrderId, InstrumentId, InstrumentKey};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

/// Held while a command on the instrument is on the wire.
pub type InstrumentGuard = OwnedMutexGuard<()>;

/// Per-instrument command serialization: one trading command at a time on each instrument.
///
/// The lock is keyed by [`InstrumentKey`] (symbol and mic), not by the whole [`InstrumentId`]: the
/// same instrument reached through a ticker, a uid or a figi is still one instrument. Waiters are
/// served in the order they asked (the lock is FIFO), so a cancel sent before a place reaches the
/// broker before it. Why: the broker's command channel is not ordered, and a place that overtakes
/// a cancel would act on a different book than the caller intended.
///
/// Different instruments do not wait for each other. The lock is released when the guard is dropped,
/// on every exit path, including errors and cancelled futures.
///
/// [`bind`](Self::bind) and [`instrument_of`](Self::instrument_of) remember which instrument a cid
/// belongs to, for commands that carry only a cid (cancel, replace).
///
/// # Examples
///
/// ```
/// use brokapi_core::{ClientOrderId, InstrumentId, InstrumentKey, NativeId};
/// use brokapi_runtime::InstrumentLocks;
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// let sber = InstrumentId::new(
///     InstrumentKey::new("SBER", "TQBR"),
///     NativeId::TickerClass { ticker: "SBER".into(), class_code: "TQBR".into() },
/// );
/// let locks = InstrumentLocks::new();
/// let cid = ClientOrderId::new_v4();
/// locks.bind(cid, &sber);
///
/// // A place holds the instrument while its command is on the wire.
/// let guard = locks.lock(&sber).await;
/// assert!(locks.is_busy(&sber));
/// drop(guard);
/// assert!(!locks.is_busy(&sber));
///
/// // A cancel carries only the cid. It finds the instrument and takes the same lock.
/// let guard = locks.lock_cid(cid).await;
/// assert!(guard.is_some() && locks.is_busy(&sber));
/// # }
/// ```
#[derive(Debug, Default)]
pub struct InstrumentLocks {
    locks: Mutex<HashMap<InstrumentKey, Arc<AsyncMutex<()>>>>,
    by_cid: Mutex<Bound>,
}

/// The cid -> instrument map with the order the cids came in, so that the oldest can go.
#[derive(Debug, Default)]
struct Bound {
    map: HashMap<ClientOrderId, InstrumentId>,
    order: std::collections::VecDeque<ClientOrderId>,
}

/// How many cids [`InstrumentLocks`] remembers. The oldest are forgotten first: a cancel of such an old
/// order is no longer serialized with the other commands of its instrument.
pub const BOUND_CIDS: usize = 100_000;

impl InstrumentLocks {
    /// Creates an empty set of locks.
    pub fn new() -> Self {
        Self::default()
    }

    /// Waits for earlier commands on `instrument`, then holds it until the guard drops.
    pub async fn lock(&self, instrument: &InstrumentId) -> InstrumentGuard {
        let lock = {
            let mut map = self.locks.lock().expect("instrument locks poisoned");
            Arc::clone(map.entry(instrument.key.clone()).or_default())
        };
        lock.lock_owned().await
    }

    /// Whether a command on the instrument is on the wire right now. For tests and diagnostics: the
    /// answer can be stale by the time the caller acts on it.
    pub fn is_busy(&self, instrument: &InstrumentId) -> bool {
        let map = self.locks.lock().expect("instrument locks poisoned");
        map.get(&instrument.key)
            .is_some_and(|l| l.try_lock().is_err())
    }

    /// Remembers the instrument of an order we placed.
    pub fn bind(&self, cid: ClientOrderId, instrument: &InstrumentId) {
        let mut b = self.by_cid.lock().expect("instrument locks poisoned");
        if b.map.insert(cid, instrument.clone()).is_none() {
            b.order.push_back(cid);
            while b.order.len() > BOUND_CIDS {
                if let Some(old) = b.order.pop_front() {
                    b.map.remove(&old);
                }
            }
        }
    }

    /// Forgets the instrument of a cid, for an order that is long finished. A later cancel of that cid is
    /// no longer serialized with the other commands of its instrument.
    pub fn forget(&self, cid: ClientOrderId) {
        self.by_cid
            .lock()
            .expect("instrument locks poisoned")
            .map
            .remove(&cid);
    }

    /// How many cids are bound to an instrument. For tests and metrics.
    pub fn bound(&self) -> usize {
        self.by_cid
            .lock()
            .expect("instrument locks poisoned")
            .map
            .len()
    }

    /// The instrument of a cid, if this process placed (or replaced) it.
    pub fn instrument_of(&self, cid: ClientOrderId) -> Option<InstrumentId> {
        self.by_cid
            .lock()
            .expect("instrument locks poisoned")
            .map
            .get(&cid)
            .cloned()
    }

    /// Locks the instrument of `cid` when it is known. `None`: an order from before this process
    /// started, its commands are not serialized with the others.
    pub async fn lock_cid(&self, cid: ClientOrderId) -> Option<InstrumentGuard> {
        match self.instrument_of(cid) {
            Some(i) => Some(self.lock(&i).await),
            None => None,
        }
    }
}
