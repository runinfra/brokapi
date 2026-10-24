//! Which subscription listens to which topic, and which topics the exchange has confirmed.

use std::collections::HashMap;
use std::sync::Arc;

use brokapi_core::SubscriptionId;
use tokio::sync::Notify;

/// Where a subscription is with the exchange.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubState {
    /// Asked for, no answer yet.
    Pending,
    /// The exchange confirmed it, or data came.
    Active,
    /// The exchange refused it.
    Failed,
}

#[derive(Debug)]
struct TopicEntry {
    state: SubState,
    reason: Option<String>,
    subs: Vec<SubscriptionId>,
}

/// The subscriptions of a core, their topics and the state of each topic.
///
/// Several subscriptions may listen to one topic (two bots, or a quote stream and a book of depth 1): the exchange is
/// asked once, the frames go to all of them.
#[derive(Debug)]
pub struct Registry<K> {
    subs: HashMap<SubscriptionId, (K, Vec<String>)>,
    topics: HashMap<String, TopicEntry>,
    /// `req_id` -> topic, for the answers of the exchange.
    reqs: HashMap<String, String>,
    ack: Arc<Notify>,
}

impl<K> Default for Registry<K> {
    fn default() -> Self {
        Self {
            subs: HashMap::new(),
            topics: HashMap::new(),
            reqs: HashMap::new(),
            ack: Arc::new(Notify::new()),
        }
    }
}

impl<K> Registry<K> {
    /// Woken whenever a topic changes state. The connector waits on it for the confirmation of a new subscription.
    pub fn notifier(&self) -> Arc<Notify> {
        Arc::clone(&self.ack)
    }

    /// Registers a subscription for `topics`. A topic that already has subscriptions keeps its state.
    pub fn register(&mut self, id: SubscriptionId, kind: K, topics: Vec<String>) {
        for t in &topics {
            self.topics
                .entry(t.clone())
                .or_insert(TopicEntry {
                    state: SubState::Pending,
                    reason: None,
                    subs: Vec::new(),
                })
                .subs
                .push(id);
        }
        self.subs.insert(id, (kind, topics));
    }

    /// Remembers that the answer carrying `req_id` is about `topic`.
    pub fn add_request(&mut self, req_id: &str, topic: &str) {
        self.reqs.insert(req_id.to_string(), topic.to_string());
    }

    /// Forgets a subscription. Returns the topics nobody listens to any more.
    pub fn forget(&mut self, id: SubscriptionId) -> Vec<String> {
        let mut orphans = Vec::new();
        if let Some((_, topics)) = self.subs.remove(&id) {
            for t in topics {
                if let Some(e) = self.topics.get_mut(&t) {
                    e.subs.retain(|s| *s != id);
                    if e.subs.is_empty() {
                        self.topics.remove(&t);
                        self.reqs.retain(|_, v| *v != t);
                        orphans.push(t);
                    }
                }
            }
        }
        orphans
    }

    /// The kind of a subscription.
    pub fn kind(&self, id: SubscriptionId) -> Option<&K> {
        self.subs.get(&id).map(|(k, _)| k)
    }

    /// The topics of a subscription.
    pub fn topics_of(&self, id: SubscriptionId) -> &[String] {
        self.subs.get(&id).map(|(_, t)| t.as_slice()).unwrap_or(&[])
    }

    /// The subscriptions that listen to a topic.
    pub fn subs_of(&self, topic: &str) -> Vec<SubscriptionId> {
        self.topics
            .get(topic)
            .map(|e| e.subs.clone())
            .unwrap_or_default()
    }

    /// Whether anyone listens to the topic.
    pub fn has_topic(&self, topic: &str) -> bool {
        self.topics.contains_key(topic)
    }

    /// Whether the exchange has confirmed the topic.
    pub fn topic_active(&self, topic: &str) -> bool {
        self.topics
            .get(topic)
            .is_some_and(|e| e.state == SubState::Active)
    }

    /// The state of a subscription: failed when any topic was refused, active when all are, else pending. With the
    /// reason of the refusal.
    pub fn state(&self, id: SubscriptionId) -> Option<(SubState, Option<String>)> {
        let (_, topics) = self.subs.get(&id)?;
        let mut all_active = true;
        for t in topics {
            match self.topics.get(t) {
                Some(e) if e.state == SubState::Failed => {
                    return Some((SubState::Failed, e.reason.clone()));
                }
                Some(e) if e.state == SubState::Active => {}
                _ => all_active = false,
            }
        }
        Some((
            if all_active {
                SubState::Active
            } else {
                SubState::Pending
            },
            None,
        ))
    }

    /// An answer of the exchange to a subscription request.
    pub fn on_ack(&mut self, req_id: &str, success: bool, message: &str) {
        let Some(topic) = self.reqs.get(req_id).cloned() else {
            return;
        };
        if let Some(e) = self.topics.get_mut(&topic) {
            if success {
                e.state = SubState::Active;
                e.reason = None;
            } else {
                e.state = SubState::Failed;
                e.reason = Some(if message.is_empty() {
                    "refused by the exchange".to_string()
                } else {
                    message.to_string()
                });
            }
            self.ack.notify_waiters();
        }
    }

    /// Data came on a topic: it is open, whatever the answer said.
    pub fn on_data(&mut self, topic: &str) {
        if let Some(e) = self.topics.get_mut(topic)
            && e.state != SubState::Active
        {
            e.state = SubState::Active;
            e.reason = None;
            self.ack.notify_waiters();
        }
    }

    /// The login was refused: every topic still waiting fails with the reason.
    pub fn fail_pending(&mut self, reason: &str) {
        for e in self.topics.values_mut() {
            if e.state == SubState::Pending {
                e.state = SubState::Failed;
                e.reason = Some(reason.to_string());
            }
        }
        self.ack.notify_waiters();
    }

    /// A connection was lost and made again: every topic is asked for again and waits for its answer.
    pub fn reset(&mut self) {
        for e in self.topics.values_mut() {
            e.state = SubState::Pending;
            e.reason = None;
        }
    }

    /// Every subscription with its kind.
    pub fn iter(&self) -> impl Iterator<Item = (SubscriptionId, &K)> {
        self.subs.iter().map(|(id, (k, _))| (*id, k))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(n: u64) -> SubscriptionId {
        SubscriptionId::new(n)
    }

    #[test]
    fn a_subscription_with_two_topics_is_active_when_both_are() {
        let mut r: Registry<()> = Registry::default();
        r.register(id(1), (), vec!["position.linear".into(), "wallet".into()]);
        r.add_request("a", "position.linear");
        r.add_request("b", "wallet");
        assert_eq!(r.state(id(1)).unwrap().0, SubState::Pending);
        r.on_ack("a", true, "");
        assert_eq!(r.state(id(1)).unwrap().0, SubState::Pending);
        r.on_ack("b", true, "");
        assert_eq!(r.state(id(1)).unwrap().0, SubState::Active);
        r.on_ack("b", false, "denied");
        assert_eq!(
            r.state(id(1)),
            Some((SubState::Failed, Some("denied".into())))
        );
        r.reset();
        assert_eq!(r.state(id(1)).unwrap().0, SubState::Pending);
    }

    #[test]
    fn a_topic_is_dropped_when_its_last_listener_goes() {
        let mut r: Registry<()> = Registry::default();
        r.register(id(1), (), vec!["t".into()]);
        r.register(id(2), (), vec!["t".into()]);
        r.add_request("x", "t");
        assert_eq!(r.subs_of("t"), vec![id(1), id(2)]);
        assert!(r.forget(id(1)).is_empty());
        assert!(r.has_topic("t"));
        assert_eq!(r.forget(id(2)), vec!["t".to_string()]);
        assert!(!r.has_topic("t"));
        // An answer to a request nobody asked for any more is ignored.
        r.on_ack("x", true, "");
    }

    #[test]
    fn data_is_the_proof_that_a_topic_is_open_and_a_failed_login_fails_the_waiting_ones() {
        let mut r: Registry<()> = Registry::default();
        r.register(id(1), (), vec!["t".into()]);
        r.register(id(2), (), vec!["u".into()]);
        r.on_data("t");
        assert_eq!(r.state(id(1)).unwrap().0, SubState::Active);
        r.fail_pending("bad key");
        assert_eq!(r.state(id(1)).unwrap().0, SubState::Active);
        assert_eq!(
            r.state(id(2)),
            Some((SubState::Failed, Some("bad key".into())))
        );
    }
}
