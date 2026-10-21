//! Client order id <-> Finam order id.
//!
//! Finam cancels and reads orders by its own `order_id`, while the library API speaks `cid`.
//! This index remembers the pairs seen in this process (placed here, echoed by streams, listed by
//! `GetOrders`) and resolves the cid of any order echo.
//!
//! The index lives in memory. After a restart it is empty and fills again from `reconcile` and from
//! the comments of order echoes, which carry the cid in a form that survives restarts
//! (see [`FinamCidCodec`]).
//!
//! # Examples
//!
//! ```
//! use brokapi_core::{CidCodec, ClientOrderId};
//! use brokapi_finam::FinamCidCodec;
//! use brokapi_finam::index::{OrderIndex, foreign_cid};
//!
//! let index = OrderIndex::new();
//! let cid = ClientOrderId::new_v4();
//!
//! // We placed an order: remember the pair when the broker answers with its id.
//! index.bind(cid, "ORD-1");
//! assert_eq!(index.order_id(cid).as_deref(), Some("ORD-1"));
//!
//! // An echo whose comment carries our cid resolves to it.
//! let comment = FinamCidCodec.encode(cid).unwrap();
//! assert_eq!(index.resolve("ORD-1", "", comment.as_str()), cid);
//!
//! // An order placed by someone else gets a stable made-up cid, and `lookup` says it is not ours.
//! assert_eq!(index.lookup("ORD-2", "set by hand"), None);
//! assert_eq!(index.resolve("ORD-2", "", "set by hand"), foreign_cid("ORD-2"));
//! ```

use std::collections::HashMap;
use std::sync::Mutex;

use brokapi_core::ClientOrderId;
use uuid::Uuid;

use crate::cid::FinamCidCodec;
use crate::map::order::cid_from_comment;

/// Namespace for cids made up for orders this library did not place (UUID v5, so they can never
/// collide with the client's v4 ids).
const FOREIGN_NS: Uuid = Uuid::from_u128(0x6f5e_1c2a_9b7d_4e3f_8a10_46fa_6d1b_0c01);

#[derive(Debug, Default)]
struct Inner {
    order_by_cid: HashMap<ClientOrderId, String>,
    cid_by_order: HashMap<String, ClientOrderId>,
    cid_by_tag: HashMap<String, ClientOrderId>,
}

/// Memory of which Finam order id belongs to which client order id.
///
/// Thread safe. The user data connector owns one and its core reads it for every order echo.
///
/// See the [module docs](self) for an example.
#[derive(Debug, Default)]
pub struct OrderIndex {
    inner: Mutex<Inner>,
}

impl OrderIndex {
    /// An empty index.
    pub fn new() -> Self {
        Self::default()
    }

    /// A cid this process knows about: its tag becomes a lookup key for echoes without comment.
    pub fn know(&self, cid: ClientOrderId) {
        let tag = FinamCidCodec.client_order_id(cid);
        self.inner
            .lock()
            .expect("index poisoned")
            .cid_by_tag
            .insert(tag, cid);
    }

    /// Remembers that Finam order `order_id` belongs to `cid`. An empty `order_id` is ignored.
    pub fn bind(&self, cid: ClientOrderId, order_id: &str) {
        if order_id.is_empty() {
            return;
        }
        let mut i = self.inner.lock().expect("index poisoned");
        i.order_by_cid.insert(cid, order_id.to_string());
        i.cid_by_order.insert(order_id.to_string(), cid);
    }

    /// Forgets everything kept for `cid`: the pair with its Finam order id and its tag. For an order that
    /// finished long ago. A later echo of that order resolves through its comment again, or to a made-up cid.
    pub fn forget(&self, cid: ClientOrderId) {
        let tag = FinamCidCodec.client_order_id(cid);
        let mut i = self.inner.lock().expect("index poisoned");
        if let Some(order_id) = i.order_by_cid.remove(&cid) {
            i.cid_by_order.remove(&order_id);
        }
        i.cid_by_tag.remove(&tag);
    }

    /// Finam order id of `cid`, if this process has seen the pair. `None` does not mean the order
    /// does not exist: after a restart call `reconcile` first.
    pub fn order_id(&self, cid: ClientOrderId) -> Option<String> {
        self.inner
            .lock()
            .expect("index poisoned")
            .order_by_cid
            .get(&cid)
            .cloned()
    }

    /// Like [`OrderIndex::resolve`] but never invents a cid: `None` for orders that are not ours.
    pub fn lookup(&self, order_id: &str, comment: &str) -> Option<ClientOrderId> {
        cid_from_comment(comment).or_else(|| {
            let i = self.inner.lock().expect("index poisoned");
            i.cid_by_order.get(order_id).copied()
        })
    }

    /// Cid of an order echo, in order of trust: comment, known order id, known tag, otherwise a
    /// stable made-up cid (UUID v5 of the order id). The result is bound to `order_id`.
    pub fn resolve(&self, order_id: &str, client_order_id: &str, comment: &str) -> ClientOrderId {
        let found = cid_from_comment(comment).or_else(|| {
            let i = self.inner.lock().expect("index poisoned");
            i.cid_by_order
                .get(order_id)
                .or_else(|| i.cid_by_tag.get(client_order_id))
                .copied()
        });
        match found {
            Some(cid) => {
                self.bind(cid, order_id);
                cid
            }
            None => {
                // Not ours. The made-up cid is remembered one way only (cid -> order id, so that the
                // order can be cancelled by it). The order id does not map back to it, so `lookup` keeps
                // saying "not ours" and the fills of foreign orders carry no cid.
                let cid = foreign_cid(order_id);
                if !order_id.is_empty() {
                    self.inner
                        .lock()
                        .expect("index poisoned")
                        .order_by_cid
                        .insert(cid, order_id.to_string());
                }
                cid
            }
        }
    }
}

/// Stable cid for an order that is not ours.
///
/// A UUID v5 of the Finam order id, so the same foreign order always gets the same cid and it cannot
/// collide with the v4 ids this library makes.
pub fn foreign_cid(order_id: &str) -> ClientOrderId {
    Uuid::new_v5(&FOREIGN_NS, order_id.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use brokapi_core::CidCodec;

    #[test]
    fn comment_wins_and_binds() {
        let idx = OrderIndex::new();
        let cid = ClientOrderId::from_bytes([3; 16]);
        let comment = FinamCidCodec.encode(cid).unwrap();
        assert_eq!(idx.resolve("100", "whatever", comment.as_str()), cid);
        assert_eq!(idx.order_id(cid).as_deref(), Some("100"));
    }

    #[test]
    fn tag_is_the_fallback_when_comment_is_lost() {
        let idx = OrderIndex::new();
        let cid = ClientOrderId::from_bytes([7; 16]);
        idx.know(cid);
        let tag = FinamCidCodec.client_order_id(cid);
        assert_eq!(idx.resolve("200", &tag, ""), cid);
        // Later echoes find it by order id alone.
        assert_eq!(idx.resolve("200", "", ""), cid);
    }

    #[test]
    fn unknown_orders_get_a_stable_foreign_cid() {
        let idx = OrderIndex::new();
        let a = idx.resolve("300", "", "someone else's label");
        let b = idx.resolve("300", "", "");
        assert_eq!(a, b);
        assert_eq!(a, foreign_cid("300"));
        assert_ne!(a, foreign_cid("301"));
        assert_eq!(a.get_version_num(), 5);
    }
}
