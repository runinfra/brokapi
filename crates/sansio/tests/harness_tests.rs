use std::time::Instant;

use brokapi_sansio::{BrokerCore, EventSink, Harness, SubscriptionSet};

#[test]
fn harness_advances_virtual_time() {
    let start = Instant::now();
    let mut h = Harness::new(
        TimeCore::new(start + std::time::Duration::from_secs(5)),
        start,
    );
    h.advance_by(std::time::Duration::from_secs(5));
    assert_eq!(h.now(), start + std::time::Duration::from_secs(5));
}

struct TimeCore {
    next: Instant,
    fired: bool,
}

impl TimeCore {
    fn new(next: Instant) -> Self {
        Self { next, fired: false }
    }
}

impl BrokerCore for TimeCore {
    type Frame = ();

    fn on_frame(&mut self, _frame: Self::Frame, _now: Instant, _out: &mut EventSink) {}

    fn poll_transmit(&mut self) -> Option<brokapi_sansio::Outbound> {
        None
    }

    fn poll_timeout(&self) -> Option<Instant> {
        if self.fired { None } else { Some(self.next) }
    }

    fn on_timeout(&mut self, _now: Instant, _out: &mut EventSink) {
        self.fired = true;
    }

    fn apply(&mut self, _desired: &SubscriptionSet) {}
}
