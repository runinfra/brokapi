//! When a gateway that stopped on its own is started again, and when the service gives up.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use super::config::RestartPolicy;

/// What to do after a gateway stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Next {
    /// Start it again after this pause.
    After(Duration),
    /// Too many restarts in the window: mark it failed and wait for `brokapi start`.
    GiveUp,
}

/// The restarts of one gateway. The clock is an argument, so the policy is tested without waiting.
#[derive(Debug)]
pub struct Restarts {
    policy: RestartPolicy,
    recent: VecDeque<Instant>,
    total: u32,
}

impl Restarts {
    /// A gateway with no restarts yet.
    pub fn new(policy: RestartPolicy) -> Self {
        Self {
            policy,
            recent: VecDeque::new(),
            total: 0,
        }
    }

    /// The gateway stopped at `now`. Counts the stop and says what comes next.
    pub fn on_exit(&mut self, now: Instant) -> Next {
        while self
            .recent
            .front()
            .is_some_and(|t| now.saturating_duration_since(*t) >= self.policy.window)
        {
            self.recent.pop_front();
        }
        if self.recent.len() as u32 >= self.policy.max_restarts {
            return Next::GiveUp;
        }
        let doublings = u32::try_from(self.recent.len()).unwrap_or(u32::MAX).min(20);
        let pause = self
            .policy
            .initial
            .saturating_mul(1u32 << doublings)
            .min(self.policy.max_pause);
        self.recent.push_back(now);
        self.total += 1;
        Next::After(pause)
    }

    /// A start asked for by a person: the count within the window begins again.
    pub fn reset(&mut self) {
        self.recent.clear();
    }

    /// How many times the gateway was started again since the service began.
    pub fn total(&self) -> u32 {
        self.total
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> RestartPolicy {
        RestartPolicy {
            initial: Duration::from_millis(500),
            max_pause: Duration::from_secs(3),
            max_restarts: 4,
            window: Duration::from_secs(60),
        }
    }

    #[test]
    fn the_pause_doubles_up_to_the_ceiling_and_then_the_service_gives_up() {
        let t0 = Instant::now();
        let mut r = Restarts::new(policy());
        let pauses: Vec<_> = (0..5)
            .map(|i| r.on_exit(t0 + Duration::from_secs(i)))
            .collect();
        assert_eq!(
            pauses,
            [
                Next::After(Duration::from_millis(500)),
                Next::After(Duration::from_secs(1)),
                Next::After(Duration::from_secs(2)),
                Next::After(Duration::from_secs(3)),
                Next::GiveUp
            ]
        );
        assert_eq!(r.total(), 4);
    }

    #[test]
    fn a_gateway_that_runs_a_whole_window_starts_counting_again() {
        let t0 = Instant::now();
        let mut r = Restarts::new(policy());
        for i in 0..3 {
            r.on_exit(t0 + Duration::from_secs(i));
        }
        let later = t0 + Duration::from_secs(300);
        assert_eq!(r.on_exit(later), Next::After(Duration::from_millis(500)));
    }

    #[test]
    fn a_start_by_hand_clears_the_count() {
        let t0 = Instant::now();
        let mut r = Restarts::new(policy());
        for _ in 0..4 {
            r.on_exit(t0);
        }
        assert_eq!(r.on_exit(t0), Next::GiveUp);
        r.reset();
        assert_eq!(r.on_exit(t0), Next::After(Duration::from_millis(500)));
        assert_eq!(r.total(), 5);
    }
}
