//! The event hub: numbers the events of the connection, keeps the latest in a ring, and hands them to the bots.
//!
//! Every event gets the next number (from 1, no holes) in the order the gateway received it. The hub keeps the latest
//! ones in a ring, limited by count and by age, so that a bot that reconnects can ask for what it missed. Each bot that
//! reads the events has a buffer of its own. When a bot reads too slowly and its buffer is full, the hub does not wait
//! for it: the bot is cut off (its stream ends with `Lagged`) and everybody else goes on.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::mpsc;

use crate::pb;

/// How much of the past the gateway keeps for a bot that reconnects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RingConfig {
    /// At most this many events. Older ones are dropped first.
    pub max_events: usize,
    /// No event older than this stays (measured by the clock of the runtime, which tests move).
    pub max_age: Duration,
}

impl Default for RingConfig {
    fn default() -> Self {
        Self {
            max_events: 10_000,
            max_age: Duration::from_secs(300),
        }
    }
}

struct Slot {
    seq: u64,
    at: Instant,
    event: Arc<pb::Event>,
}

struct Subscriber {
    bot: String,
    tx: mpsc::Sender<Arc<pb::Event>>,
    lagged: Arc<AtomicBool>,
}

struct Inner {
    next_seq: u64,
    ring: VecDeque<Slot>,
    subscribers: HashMap<u64, Subscriber>,
    next_id: u64,
}

/// The hub of one gateway run.
pub struct Hub {
    epoch: u64,
    cfg: RingConfig,
    inner: Mutex<Inner>,
}

/// What a bot gets when it attaches.
pub struct Attached {
    /// The handle to detach with.
    pub id: u64,
    /// The first frame of the stream.
    pub ready: pb::Ready,
    /// A hole in the numbers, if there is one.
    pub gap: Option<pb::Gap>,
    /// The events the bot missed, in order.
    pub replay: Vec<Arc<pb::Event>>,
    /// The events from now on.
    pub live: mpsc::Receiver<Arc<pb::Event>>,
    /// Set when the hub cut the bot off: the stream ends and the last frame is `Lagged`.
    pub lagged: Arc<AtomicBool>,
}

impl Hub {
    /// A hub of one run. `epoch` tells this run from the others.
    pub fn new(cfg: RingConfig, epoch: u64) -> Self {
        Self {
            epoch,
            cfg,
            inner: Mutex::new(Inner {
                next_seq: 1,
                ring: VecDeque::new(),
                subscribers: HashMap::new(),
                next_id: 1,
            }),
        }
    }

    /// The epoch of this run.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// The number of the last event, 0 before the first.
    pub fn last_seq(&self) -> u64 {
        self.lock().next_seq - 1
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn trim(&self, inner: &mut Inner, now: Instant) {
        while inner.ring.len() > self.cfg.max_events {
            inner.ring.pop_front();
        }
        while let Some(front) = inner.ring.front() {
            if now.saturating_duration_since(front.at) > self.cfg.max_age {
                inner.ring.pop_front();
            } else {
                break;
            }
        }
    }

    /// Numbers an event, puts it in the ring and offers it to every bot. A bot whose buffer is full is cut off.
    /// Returns the number and the names of the bots that were cut off by this event.
    pub fn publish(&self, mut event: pb::Event) -> (u64, Vec<String>) {
        let now = brokapi::runtime::now();
        let mut inner = self.lock();
        let seq = inner.next_seq;
        inner.next_seq += 1;
        event.seq = seq;
        let event = Arc::new(event);
        inner.ring.push_back(Slot {
            seq,
            at: now,
            event: Arc::clone(&event),
        });
        self.trim(&mut inner, now);
        let mut cut = Vec::new();
        let mut lagged = Vec::new();
        for (id, sub) in &inner.subscribers {
            match sub.tx.try_send(Arc::clone(&event)) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    sub.lagged.store(true, Ordering::SeqCst);
                    cut.push(*id);
                    lagged.push(sub.bot.clone());
                }
                Err(mpsc::error::TrySendError::Closed(_)) => cut.push(*id),
            }
        }
        // Dropping the sender ends the stream of the bot once it has read what is in its buffer.
        for id in cut {
            inner.subscribers.remove(&id);
        }
        (seq, lagged)
    }

    /// Registers a bot's stream. With `resume` (the epoch and the last number the bot handled) the events it missed come
    /// back in `replay`, or a [`pb::Gap`] says they are gone. Replay and registration happen under one lock, so no event
    /// is missed or doubled between them.
    pub fn attach(&self, bot: &str, buffer: usize, resume: Option<(u64, u64)>) -> Attached {
        let now = brokapi::runtime::now();
        let mut inner = self.lock();
        self.trim(&mut inner, now);
        let first_available = inner.ring.front().map_or(inner.next_seq, |s| s.seq);
        let mut gap = None;
        let mut replay = Vec::new();
        if let Some((epoch, last_seq)) = resume {
            let reason = if epoch != self.epoch {
                Some(pb::gap::Reason::NewEpoch)
            } else if last_seq + 1 > inner.next_seq {
                Some(pb::gap::Reason::Ahead)
            } else if last_seq + 1 < first_available {
                Some(pb::gap::Reason::TooOld)
            } else {
                None
            };
            match reason {
                Some(reason) => {
                    gap = Some(pb::Gap {
                        reason: reason as i32,
                        requested_after: last_seq,
                        first_available,
                    });
                }
                None => {
                    replay = inner
                        .ring
                        .iter()
                        .filter(|s| s.seq > last_seq)
                        .map(|s| Arc::clone(&s.event))
                        .collect();
                }
            }
        }
        let (tx, live) = mpsc::channel(buffer.max(1));
        let lagged = Arc::new(AtomicBool::new(false));
        let id = inner.next_id;
        inner.next_id += 1;
        inner.subscribers.insert(
            id,
            Subscriber {
                bot: bot.to_owned(),
                tx,
                lagged: Arc::clone(&lagged),
            },
        );
        Attached {
            id,
            ready: pb::Ready {
                epoch: self.epoch,
                next_seq: inner.next_seq,
                replaying: replay.len() as u64,
            },
            gap,
            replay,
            live,
            lagged,
        }
    }

    /// Removes a bot's stream.
    pub fn detach(&self, id: u64) {
        self.lock().subscribers.remove(&id);
    }

    /// How many events wait in the buffer of each bot's streams, by bot name.
    pub fn queued_by_bot(&self) -> HashMap<String, u64> {
        let inner = self.lock();
        let mut out: HashMap<String, u64> = HashMap::new();
        for sub in inner.subscribers.values() {
            let queued = (sub.tx.max_capacity() - sub.tx.capacity()) as u64;
            *out.entry(sub.bot.clone()).or_default() += queued;
        }
        out
    }

    /// The names of the bots that have a stream open.
    pub fn streaming_bots(&self) -> Vec<String> {
        let inner = self.lock();
        let mut bots: Vec<String> = inner.subscribers.values().map(|s| s.bot.clone()).collect();
        bots.sort();
        bots.dedup();
        bots
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event() -> pb::Event {
        pb::Event::default()
    }

    #[test]
    fn numbers_are_in_order_and_start_at_one() {
        let hub = Hub::new(RingConfig::default(), 7);
        assert_eq!(hub.last_seq(), 0);
        assert_eq!(hub.publish(event()).0, 1);
        assert_eq!(hub.publish(event()).0, 2);
        assert_eq!(hub.last_seq(), 2);
    }

    #[test]
    fn a_bot_that_reconnects_gets_what_it_missed() {
        let hub = Hub::new(RingConfig::default(), 7);
        for _ in 0..5 {
            hub.publish(event());
        }
        let a = hub.attach("bot", 8, Some((7, 2)));
        assert!(a.gap.is_none());
        let seqs: Vec<u64> = a.replay.iter().map(|e| e.seq).collect();
        assert_eq!(seqs, vec![3, 4, 5]);
        assert_eq!(a.ready.next_seq, 6);
        assert_eq!(a.ready.replaying, 3);
        // Nothing missed: nothing replayed, no gap.
        let b = hub.attach("bot", 8, Some((7, 5)));
        assert!(b.gap.is_none() && b.replay.is_empty());
    }

    #[test]
    fn the_ring_is_limited_by_count_and_an_old_number_is_a_gap() {
        let hub = Hub::new(
            RingConfig {
                max_events: 3,
                max_age: Duration::from_secs(3600),
            },
            7,
        );
        for _ in 0..10 {
            hub.publish(event());
        }
        let a = hub.attach("bot", 8, Some((7, 4)));
        let gap = a.gap.expect("a gap");
        assert_eq!(gap.reason, pb::gap::Reason::TooOld as i32);
        assert_eq!((gap.requested_after, gap.first_available), (4, 8));
        assert!(a.replay.is_empty());
        // The edge: 7 is the last one dropped, the ring starts at 8.
        assert!(hub.attach("bot", 8, Some((7, 7))).gap.is_none());
    }

    #[test]
    fn another_epoch_or_a_number_from_the_future_is_a_gap() {
        let hub = Hub::new(RingConfig::default(), 7);
        hub.publish(event());
        let a = hub.attach("bot", 8, Some((8, 1)));
        assert_eq!(a.gap.unwrap().reason, pb::gap::Reason::NewEpoch as i32);
        let b = hub.attach("bot", 8, Some((7, 50)));
        assert_eq!(b.gap.unwrap().reason, pb::gap::Reason::Ahead as i32);
        // A fresh bot has no resume point: live only, no gap.
        let c = hub.attach("bot", 8, None);
        assert!(c.gap.is_none() && c.replay.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn the_ring_is_limited_by_age() {
        let hub = Hub::new(
            RingConfig {
                max_events: 100,
                max_age: Duration::from_secs(60),
            },
            7,
        );
        hub.publish(event());
        tokio::time::advance(Duration::from_secs(61)).await;
        hub.publish(event());
        let a = hub.attach("bot", 8, Some((7, 0)));
        assert_eq!(a.gap.expect("event 1 aged out").first_available, 2);
    }

    #[test]
    fn a_full_buffer_cuts_that_bot_off_and_nobody_else() {
        let hub = Hub::new(RingConfig::default(), 7);
        let mut slow = hub.attach("slow", 2, None);
        let mut fast = hub.attach("fast", 100, None);
        for _ in 0..5 {
            hub.publish(event());
        }
        assert!(slow.lagged.load(Ordering::SeqCst));
        assert!(!fast.lagged.load(Ordering::SeqCst));
        // The slow bot still reads what was in its buffer, then the stream ends.
        assert_eq!(slow.live.try_recv().unwrap().seq, 1);
        assert_eq!(slow.live.try_recv().unwrap().seq, 2);
        assert!(matches!(
            slow.live.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected)
        ));
        let seqs: Vec<u64> = std::iter::from_fn(|| fast.live.try_recv().ok())
            .map(|e| e.seq)
            .collect();
        assert_eq!(seqs, vec![1, 2, 3, 4, 5]);
    }
}
