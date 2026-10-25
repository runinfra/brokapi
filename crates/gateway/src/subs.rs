//! Who owns which subscription.
//!
//! A subscription at the broker costs a slot of its quota, and the events it brings go to every bot that reads the
//! stream. So the gateway keeps one subscription at the broker for the same thing and counts who asked for it:
//!
//! - The same request from two bots (or twice from one) is one subscription at the broker with several references.
//! - A bot can only drop a reference it holds. Another bot's `Unsubscribe` is refused.
//! - The subscription is closed at the broker when the last reference goes.
//! - When a bot is gone its references go (see [`Subscriptions::release_bot`]).
//! - The gateway holds references of its own (the states of orders), which no bot can drop.

use std::collections::{BTreeMap, HashMap};
use std::future::Future;

use brokapi::core::{Error, SubscriptionId};
use tokio::sync::Mutex;

use crate::pb;

/// The owner name of the references the gateway holds itself. It is not a valid bot name, so no bot can be it.
pub(crate) const OWN: &str = "(gateway)";

struct Entry {
    sub: SubscriptionId,
    what: String,
    /// Owner -> number of references.
    refs: BTreeMap<String, u32>,
}

/// The subscriptions of one gateway.
#[derive(Default)]
pub(crate) struct Subscriptions {
    // Held across the calls to the broker on purpose: two bots asking for the same thing at once must find one entry.
    inner: Mutex<HashMap<String, Entry>>,
    /// The listing of `inner` for `Health`, which cannot wait for the lock.
    listing: std::sync::Mutex<Vec<pb::SubscriptionInfo>>,
}

fn refused(code: &str, sub: u64, why: &str) -> Error {
    Error::Rejected {
        code: code.into(),
        message: format!("subscription {sub}: {why}"),
    }
}

/// The subscriptions whose call to the broker takes no options: the gateway holds them itself, and the options a bot
/// sends with them (`backpressure`, `cap`) do not reach the broker.
const WITHOUT_OPTIONS: [&str; 3] = [
    "subscribe_order_state",
    "subscribe_stop_orders",
    "subscribe_executions",
];

/// The name under which a subscription is shared: the call and the request as the broker gets it. The same name is one
/// subscription at the broker. Options that do not reach the broker are not part of it, or a bot that sends them would
/// open a second subscription to the same stream and get every event of it twice.
pub(crate) fn key(kind: &str, request: &str) -> String {
    if WITHOUT_OPTIONS.contains(&kind) {
        kind.to_owned()
    } else {
        format!("{kind} {request}")
    }
}

impl Subscriptions {
    fn publish(&self, all: &HashMap<String, Entry>) {
        let mut out: Vec<_> = all
            .values()
            .map(|e| pb::SubscriptionInfo {
                sub: e.sub.value(),
                what: e.what.clone(),
                owners: e.refs.keys().cloned().collect(),
            })
            .collect();
        out.sort_by_key(|s| s.sub);
        *self.listing.lock().unwrap_or_else(|e| e.into_inner()) = out;
    }

    /// A reference for `owner` to the subscription named by `key`. `open` makes it at the broker when nobody holds one.
    pub(crate) async fn subscribe<F, Fut>(
        &self,
        owner: &str,
        key: String,
        what: String,
        open: F,
    ) -> Result<SubscriptionId, Error>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<SubscriptionId, Error>>,
    {
        let mut all = self.inner.lock().await;
        if let Some(entry) = all.get_mut(&key) {
            *entry.refs.entry(owner.to_owned()).or_insert(0) += 1;
            let sub = entry.sub;
            self.publish(&all);
            return Ok(sub);
        }
        let sub = open().await?;
        all.insert(
            key,
            Entry {
                sub,
                what,
                refs: BTreeMap::from([(owner.to_owned(), 1)]),
            },
        );
        self.publish(&all);
        Ok(sub)
    }

    /// Drops one reference of `owner`. `close` runs at the broker when it was the last one. If the broker refuses, the
    /// reference stays.
    pub(crate) async fn unsubscribe<F, Fut>(
        &self,
        owner: &str,
        sub: SubscriptionId,
        close: F,
    ) -> Result<(), Error>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<(), Error>>,
    {
        let mut all = self.inner.lock().await;
        let Some(key) = all
            .iter()
            .find(|(_, e)| e.sub == sub)
            .map(|(k, _)| k.clone())
        else {
            return Err(refused(
                "unknown_subscription",
                sub.value(),
                "there is no such subscription",
            ));
        };
        let entry = all.get_mut(&key).expect("found above");
        if !entry.refs.contains_key(owner) {
            return Err(refused(
                "not_your_subscription",
                sub.value(),
                "it was opened by another bot, and only the bot that opened it can close it",
            ));
        }
        let last = entry.refs.values().sum::<u32>() == 1;
        if last {
            close().await?;
            all.remove(&key);
        } else {
            let count = entry.refs.get_mut(owner).expect("checked above");
            *count -= 1;
            if *count == 0 {
                entry.refs.remove(owner);
            }
        }
        self.publish(&all);
        Ok(())
    }

    /// Drops every reference `owner` holds. `close` runs for each subscription nobody holds any more. Returns how many
    /// subscriptions were closed at the broker.
    pub(crate) async fn release_bot<F, Fut>(&self, owner: &str, close: F) -> usize
    where
        F: Fn(SubscriptionId) -> Fut,
        Fut: Future<Output = Result<(), Error>>,
    {
        let mut all = self.inner.lock().await;
        let mine: Vec<String> = all
            .iter()
            .filter(|(_, e)| e.refs.contains_key(owner))
            .map(|(k, _)| k.clone())
            .collect();
        let mut closed = 0;
        for key in mine {
            let entry = all.get_mut(&key).expect("listed above");
            entry.refs.remove(owner);
            if entry.refs.is_empty() {
                let sub = entry.sub;
                match close(sub).await {
                    Ok(()) => {
                        all.remove(&key);
                        closed += 1;
                    }
                    Err(e) => {
                        // The broker did not take it back: keep it as the gateway's own so that it is listed, not lost.
                        tracing::warn!(sub = sub.value(), "closing a subscription failed: {e}");
                        all.get_mut(&key)
                            .expect("still there")
                            .refs
                            .insert(OWN.to_owned(), 1);
                    }
                }
            }
        }
        self.publish(&all);
        closed
    }

    /// The subscriptions for `Health`.
    pub(crate) fn list(&self) -> Vec<pb::SubscriptionInfo> {
        self.listing
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    fn open(id: u64, opened: &AtomicU32) -> impl Future<Output = Result<SubscriptionId, Error>> {
        opened.fetch_add(1, Ordering::SeqCst);
        async move { Ok(SubscriptionId::new(id)) }
    }

    #[test]
    fn options_that_do_not_reach_the_broker_do_not_make_a_second_subscription() {
        let plain = format!("{:?}", pb::SubscribeRequest::default());
        let with_opts = format!(
            "{:?}",
            pb::SubscribeRequest {
                opts: Some(pb::SubscribeOpts {
                    backpressure: pb::Backpressure::Lossless as i32,
                    cap: 1000,
                    depth: None,
                }),
            }
        );
        for kind in WITHOUT_OPTIONS {
            assert_eq!(key(kind, &with_opts), key(kind, &plain), "{kind}");
        }
        // The options of a market subscription reach the broker (the depth of a book, the buffer): they stay apart.
        assert_ne!(
            key("subscribe_order_book", &with_opts),
            key("subscribe_order_book", &plain)
        );
    }

    #[tokio::test]
    async fn the_same_request_of_two_bots_is_one_subscription_at_the_broker() {
        let subs = Subscriptions::default();
        let opened = AtomicU32::new(0);
        let a = subs
            .subscribe("a", "book:SBER".into(), "book".into(), || open(7, &opened))
            .await
            .unwrap();
        let b = subs
            .subscribe("b", "book:SBER".into(), "book".into(), || open(8, &opened))
            .await
            .unwrap();
        assert_eq!((a.value(), b.value()), (7, 7));
        assert_eq!(opened.load(Ordering::SeqCst), 1);
        assert_eq!(subs.list()[0].owners, ["a", "b"]);
    }

    #[tokio::test]
    async fn only_the_owner_can_close_and_the_last_reference_closes_at_the_broker() {
        let subs = Subscriptions::default();
        let closed = AtomicU32::new(0);
        let close = || async {
            closed.fetch_add(1, Ordering::SeqCst);
            Ok(())
        };
        let opened = AtomicU32::new(0);
        let sub = subs
            .subscribe("a", "k".into(), "k".into(), || open(1, &opened))
            .await
            .unwrap();
        subs.subscribe("b", "k".into(), "k".into(), || open(1, &opened))
            .await
            .unwrap();
        // A stranger cannot.
        let err = subs.unsubscribe("c", sub, close).await.unwrap_err();
        assert!(matches!(err, Error::Rejected { ref code, .. } if code == "not_your_subscription"));
        // a lets go: b still holds it, the broker is not asked.
        subs.unsubscribe("a", sub, close).await.unwrap();
        assert_eq!(closed.load(Ordering::SeqCst), 0);
        // a cannot let go twice.
        assert!(subs.unsubscribe("a", sub, close).await.is_err());
        // b lets go: the last one.
        subs.unsubscribe("b", sub, close).await.unwrap();
        assert_eq!(closed.load(Ordering::SeqCst), 1);
        assert!(subs.list().is_empty());
    }

    #[tokio::test]
    async fn a_bot_that_is_gone_takes_its_references_and_the_gateways_own_stay() {
        let subs = Subscriptions::default();
        let opened = AtomicU32::new(0);
        subs.subscribe(OWN, "orders".into(), "orders".into(), || open(1, &opened))
            .await
            .unwrap();
        subs.subscribe("a", "orders".into(), "orders".into(), || open(1, &opened))
            .await
            .unwrap();
        subs.subscribe("a", "book".into(), "book".into(), || open(2, &opened))
            .await
            .unwrap();
        let closed = subs.release_bot("a", |_| async { Ok(()) }).await;
        assert_eq!(closed, 1, "only the book is closed");
        let left = subs.list();
        assert_eq!(left.len(), 1);
        assert_eq!(left[0].owners, [OWN]);
    }
}
