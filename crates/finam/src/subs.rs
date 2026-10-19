//! Subscription id generator (unique within one connector).

use std::sync::atomic::{AtomicU64, Ordering};

use brokapi_core::SubscriptionId;

/// Hands out [`SubscriptionId`]s: 1, 2, 3, ... (after `start`), never repeating.
///
/// The market data connector starts at 0 and the user data connector at 1000, so that ids from the
/// two connectors of one process are easy to tell apart in logs. Thread safe.
///
/// # Examples
///
/// ```
/// use brokapi_finam::subs::SubIdGen;
///
/// let ids = SubIdGen::new(1000);
/// let first = ids.next();
/// let second = ids.next();
/// assert_ne!(first, second);
/// ```
pub struct SubIdGen(AtomicU64);

impl SubIdGen {
    /// A generator whose first id is `start + 1`.
    pub fn new(start: u64) -> Self {
        Self(AtomicU64::new(start))
    }

    /// The next unused id.
    pub fn next(&self) -> SubscriptionId {
        SubscriptionId::new(self.0.fetch_add(1, Ordering::Relaxed) + 1)
    }
}
