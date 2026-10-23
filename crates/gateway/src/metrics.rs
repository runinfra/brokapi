//! The numbers a gateway reports about itself that are not a plain counter.

use std::time::{Duration, Instant};

/// How many events per second came in over the last [`RateMeter::WINDOW`] seconds. The clock is an argument, so a test
/// moves time by giving later instants.
#[derive(Debug)]
pub(crate) struct RateMeter {
    start: Instant,
    /// `(second since start, events in it)`, newest last.
    seconds: std::collections::VecDeque<(u64, u64)>,
}

impl RateMeter {
    /// The length of the window, in seconds.
    pub(crate) const WINDOW: u64 = 10;

    pub(crate) fn new(start: Instant) -> Self {
        Self {
            start,
            seconds: Default::default(),
        }
    }

    fn second(&self, now: Instant) -> u64 {
        now.saturating_duration_since(self.start).as_secs()
    }

    fn trim(&mut self, current: u64) {
        while self
            .seconds
            .front()
            .is_some_and(|(s, _)| s + Self::WINDOW <= current)
        {
            self.seconds.pop_front();
        }
    }

    /// One event at `now`.
    pub(crate) fn record(&mut self, now: Instant) {
        let current = self.second(now);
        self.trim(current);
        match self.seconds.back_mut() {
            Some((s, n)) if *s == current => *n += 1,
            _ => self.seconds.push_back((current, 1)),
        }
    }

    /// Events per second over the window that ends at `now`. A gateway younger than the window counts its own age.
    pub(crate) fn per_second(&mut self, now: Instant) -> f64 {
        let current = self.second(now);
        self.trim(current);
        let total: u64 = self.seconds.iter().map(|(_, n)| n).sum();
        let span = (current + 1).min(Self::WINDOW);
        total as f64 / span as f64
    }
}

/// Nanoseconds of a duration, saturating.
pub(crate) fn nanos(d: Duration) -> u64 {
    u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_rate_follows_the_window() {
        let t0 = Instant::now();
        let mut meter = RateMeter::new(t0);
        for _ in 0..20 {
            meter.record(t0);
        }
        // The first second: 20 events in a window of one second.
        assert_eq!(meter.per_second(t0), 20.0);
        // Ten seconds on, the window is full and holds the same 20 events.
        assert_eq!(meter.per_second(t0 + Duration::from_secs(9)), 2.0);
        // Eleven seconds on, they are out of the window.
        assert_eq!(meter.per_second(t0 + Duration::from_secs(10)), 0.0);
    }
}
