//! Edges of the sans-io kit: a message cut in pieces and glued together, a virtual clock driven by recorded frames, and
//! the state of an order after a repeated signal.

use std::time::{Duration, Instant};

use brokapi_core::{ClientOrderId, OrderState};
use brokapi_sansio::{
    BrokerCore, BufferPush, EventSink, FrameRecord, Harness, JsonFrameBuffer, OrderFsm,
    OrderSignal, Outbound, SubscriptionSet,
};

#[test]
fn a_message_cut_in_pieces_is_whole_when_its_last_piece_comes() {
    let mut buf = JsonFrameBuffer::new();
    assert!(buf.is_empty());
    // An array, with brackets and a quote inside strings, and an escaped quote.
    assert_eq!(buf.push(r#"[1, "]", "a\"]""#), BufferPush::Incomplete);
    assert_eq!(buf.pending(), r#"[1, "]", "a\"]""#);
    assert_eq!(
        buf.push("]"),
        BufferPush::Complete(vec![r#"[1, "]", "a\"]"]"#.to_string()])
    );
    assert!(buf.is_empty());
    // Half of a message is thrown away by `reset`.
    buf.push(r#"{"a":"#);
    buf.reset();
    assert!(buf.is_empty());
    // A string that is a backslash itself does not escape the quote after it.
    assert_eq!(
        buf.push(r#"{"p":"c:\\"}"#),
        BufferPush::Complete(vec![r#"{"p":"c:\\"}"#.to_string()])
    );
    // Text that cannot start a value blocks the buffer.
    assert_eq!(buf.push("garbage"), BufferPush::Incomplete);
    assert_eq!(buf.pending(), "garbage");
}

/// A core that answers every frame with the same text and has a timer it moves by itself.
struct Echo {
    queue: Vec<String>,
    deadline: Option<Instant>,
    fired: u32,
}

impl BrokerCore for Echo {
    type Frame = String;

    fn on_frame(&mut self, frame: String, now: Instant, _out: &mut EventSink) {
        self.queue.push(frame);
        self.deadline = Some(now + Duration::from_secs(10));
    }
    fn poll_transmit(&mut self) -> Option<Outbound> {
        (!self.queue.is_empty()).then(|| Outbound::WsText(self.queue.remove(0)))
    }
    fn poll_timeout(&self) -> Option<Instant> {
        self.deadline
    }
    fn on_timeout(&mut self, _now: Instant, _out: &mut EventSink) {
        self.fired += 1;
        self.deadline = None;
    }
    fn apply(&mut self, _desired: &SubscriptionSet) {}
}

#[test]
fn recorded_frames_move_the_clock_and_fire_the_timers_between_them() {
    let t0 = Instant::now();
    let mut h = Harness::new(
        Echo {
            queue: Vec::new(),
            deadline: None,
            fired: 0,
        },
        t0,
    );
    let at = |s| t0 + Duration::from_secs(s);
    h.run_frames(&[
        FrameRecord::new(at(1), "a".to_string()),
        // The timer of the first frame is due at 11: it fires before the frame at 30 comes.
        FrameRecord::new(at(30), "b".to_string()),
    ]);
    assert_eq!(h.core().fired, 1);
    assert_eq!(h.now(), at(30));
    assert_eq!(h.transmitted().len(), 2);
    h.feed_frame_at(at(31), "c".to_string());
    assert_eq!(h.now(), at(31));
    assert!(h.events().is_empty());
    // Nothing is waiting to be sent now: the answers were taken as they were made.
    assert!(h.drain_transmit().is_empty());
}

#[test]
fn a_place_that_is_sent_again_changes_nothing_in_the_state_of_the_order() {
    let cid = ClientOrderId::new_v4();
    let now = Instant::now();
    let mut fsm = OrderFsm::new(cid, now);
    assert_eq!(fsm.cid(), cid);
    // `PlaceSent` in flight is a repeat of what is known.
    assert_eq!(fsm.apply(OrderSignal::PlaceSent, now).unwrap(), None);
    assert!(matches!(fsm.state(), OrderState::InFlight { .. }));
}
