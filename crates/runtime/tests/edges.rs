//! Small parts of the runtime that a connector leans on: the command gate, the state of a connection, the silence of a
//! connection, the buffers, the errors of the log and of the history, the wait of a graceful shutdown.

use std::io::{self, BufRead, Write};
use std::sync::Arc;
use std::time::{Duration, Instant};

use brokapi_core::{
    Backpressure, Error, Event, InstrumentId, InstrumentKey, NativeId, ShutdownMode,
    SubscriptionId, UnixNanos,
};
use brokapi_runtime::history::HistoryError;
use brokapi_runtime::record::{EnvelopeReader, EnvelopeWriter, LogError};
use brokapi_runtime::{
    CommandGate, ConnectionState, Emitter, HealthClass, OrderRuntime, PushResult, ReconcileAction,
    ShutdownController, SubscriptionBuffer, Supervisor,
};
use brokapi_sansio::{CoreEvent, DesiredSubscription, SubscriptionKind, SubscriptionSet};

fn sber() -> InstrumentId {
    InstrumentId::new(
        InstrumentKey::new("SBER", "TQBR"),
        NativeId::TickerClass {
            ticker: "SBER".into(),
            class_code: "TQBR".into(),
        },
    )
}

#[test]
fn the_gate_says_why_it_is_closed_and_a_shutdown_beats_everything() {
    let mut gate = CommandGate::new();
    assert!(!gate.is_reconciled() && !gate.is_degraded());
    assert!(matches!(gate.check_command(), Err(Error::NotReconciled)));
    gate.mark_reconciled();
    assert!(gate.is_reconciled());
    gate.check_command().unwrap();
    gate.mark_degraded();
    assert!(gate.is_degraded());
    assert!(matches!(gate.check_command(), Err(Error::Degraded)));
    gate.clear_degraded();
    gate.begin_shutdown();
    assert!(matches!(gate.check_command(), Err(Error::ShuttingDown)));
    gate.mark_degraded();
    assert!(matches!(gate.check_command(), Err(Error::ShuttingDown)));
}

fn sub(id: u64) -> DesiredSubscription {
    DesiredSubscription {
        id: SubscriptionId::new(id),
        kind: SubscriptionKind::Quotes { instrument: sber() },
    }
}

#[test]
fn a_connection_knows_what_to_send_again_and_what_to_cancel() {
    let now = Instant::now();
    let mut conn = ConnectionState::new(1, now);
    let (a, b, c) = (sub(1), sub(2), sub(3));
    conn.set_desired(SubscriptionSet::with_subscriptions(vec![
        a.clone(),
        b.clone(),
        c.clone(),
    ]));
    assert_eq!(conn.needs_resubscribe().len(), 3, "nothing is known yet");
    conn.mark_pending(&a, now);
    conn.mark_active(b.id, 7);
    conn.mark_failed(c.id, "denied");
    let again: Vec<_> = conn.needs_resubscribe().iter().map(|s| s.id).collect();
    assert_eq!(
        again,
        [a.id, c.id],
        "a slow one and a refused one, not the active one"
    );
    conn.remove(a.id);
    assert_eq!(conn.actual.len(), 2);

    let before = SubscriptionSet::with_subscriptions(vec![a.clone(), b.clone(), sub(9)]);
    conn.set_desired(SubscriptionSet::with_subscriptions(vec![b]));
    let cancel: Vec<_> = conn
        .needs_unsubscribe(&before)
        .iter()
        .map(|s| s.id)
        .collect();
    assert_eq!(cancel, [a.id, SubscriptionId::new(9)]);
}

#[test]
fn the_silence_of_a_connection_decides_what_the_supervisor_does() {
    let t0 = Instant::now();
    let mut sup = Supervisor::new(t0);
    let at = |secs| t0 + Duration::from_secs(secs);
    assert_eq!(
        sup.reconcile(at(1)),
        vec![(1, ReconcileAction::ApplyDesired)]
    );
    assert_eq!(
        sup.reconcile(at(20)),
        vec![(1, ReconcileAction::ResubscribeMissing)]
    );
    assert_eq!(
        sup.reconcile(at(70)),
        vec![(1, ReconcileAction::ScheduleReconnect)]
    );
    // A frame resets the silence.
    sup.connection_mut(1).unwrap().health.on_rx(at(70));
    assert_eq!(
        sup.reconcile(at(71)),
        vec![(1, ReconcileAction::ApplyDesired)]
    );
    let health = sup.connection_mut(1).unwrap().classify(
        at(200),
        Duration::from_secs(60),
        Duration::from_secs(15),
    );
    assert_eq!(health, HealthClass::Dead);
}

#[test]
fn a_buffer_tells_what_it_holds_what_it_threw_away_and_when_it_overflowed() {
    let id = SubscriptionId::new(3);
    let mut buf = SubscriptionBuffer::new(id, sber(), Backpressure::Lossless { cap: 1 });
    assert!(buf.is_empty());
    assert_eq!(buf.instrument(), &sber());
    assert_eq!(buf.try_push(Event::Balances(vec![])), PushResult::Delivered);
    assert_eq!(buf.len(), 1);
    assert_eq!(buf.try_push(Event::Balances(vec![])), PushResult::Overflow);
    assert!(buf.is_overflowed());
    assert!(matches!(
        buf.overflow_event(UnixNanos::from_nanos(5)),
        Event::Overflow { sub, .. } if sub == id
    ));

    let mut conflating = SubscriptionBuffer::new(id, sber(), Backpressure::Conflate);
    conflating.try_push(Event::Balances(vec![]));
    conflating.try_push(Event::Balances(vec![]));
    assert_eq!((conflating.len(), conflating.conflated()), (1, 1));

    let mut dropping = SubscriptionBuffer::new(id, sber(), Backpressure::DropNewest { cap: 1 });
    dropping.try_push(Event::Balances(vec![]));
    assert_eq!(
        dropping.try_push(Event::Balances(vec![])),
        PushResult::DroppedNewest
    );
    assert_eq!(dropping.dropped(), 1);
}

#[tokio::test]
async fn the_emitter_names_its_source_and_counts_what_it_could_not_route() {
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    let emitter = Emitter::new(Arc::from("test-source"), tx);
    assert_eq!(&**emitter.source(), "test-source");
    assert_eq!(emitter.dropped_unrouted(), 0);
    // The reader is gone: an event with no subscription has nowhere to go.
    drop(rx);
    emitter.emit(vec![CoreEvent {
        sub: None,
        payload: Event::Balances(vec![]),
    }]);
    assert_eq!(emitter.dropped_unrouted(), 1);
}

#[test]
fn a_stopped_read_of_history_says_where_and_why() {
    let e = HistoryError {
        error: Error::Unknown,
        resume_from: UnixNanos::from_nanos(42),
    };
    let text = e.to_string();
    assert!(text.contains("42"), "{text}");
    assert!(std::error::Error::source(&e).is_some());
}

struct Broken;

impl Write for Broken {
    fn write(&mut self, _: &[u8]) -> io::Result<usize> {
        Err(io::Error::other("disk is full"))
    }
    fn flush(&mut self) -> io::Result<()> {
        Err(io::Error::other("disk is full"))
    }
}

struct BrokenRead;

impl io::Read for BrokenRead {
    fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
        Err(io::Error::other("cable pulled"))
    }
}

impl BufRead for BrokenRead {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        Err(io::Error::other("cable pulled"))
    }
    fn consume(&mut self, _: usize) {}
}

#[test]
fn the_errors_of_a_log_say_what_went_wrong_and_the_cause_of_a_disk_failure_is_kept() {
    // A header that cannot be written, and a flush that fails.
    let e = EnvelopeWriter::new(Broken)
        .err()
        .expect("the header is not written");
    assert!(matches!(e, LogError::Io(_)));
    assert!(e.to_string().contains("disk is full"), "{e}");
    assert!(std::error::Error::source(&e).is_some());
    let mut writer = EnvelopeWriter::new(Vec::new()).unwrap();
    writer.flush().unwrap();
    let from_io: LogError = io::Error::other("x").into();
    assert!(matches!(from_io, LogError::Io(_)));

    // A read that fails once, and then the reader ends.
    let first = EnvelopeReader::new(BrokenRead).err();
    // The header cannot be read: the reader is not made.
    assert!(matches!(first, Some(LogError::Io(_))), "{first:?}");

    // A line that is not an envelope, a newer schema: told once, and the reader stops.
    let header = r#"{"brokapi_log":1,"envelope_schema":1}"#;
    let bad = format!("{header}\nnot json\n{{}}\n");
    let mut reader = EnvelopeReader::new(bad.as_bytes()).unwrap();
    let err = reader.next().unwrap().unwrap_err();
    assert!(matches!(err, LogError::Format { line: 2, .. }), "{err:?}");
    assert!(std::error::Error::source(&err).is_none());
    assert!(err.to_string().contains("line 2"));
    assert!(reader.next().is_none());
    let schema = LogError::Schema {
        found: 9,
        supported: 1,
    };
    assert!(schema.to_string().contains('9'));
}

#[tokio::test(start_paused = true)]
async fn a_graceful_shutdown_waits_for_its_deadline_and_an_immediate_one_does_not() {
    let controller = ShutdownController::new(CommandGate::new());
    let started = tokio::time::Instant::now();
    controller
        .wait_graceful_deadline(ShutdownMode::Immediate)
        .await;
    assert_eq!(started.elapsed(), Duration::ZERO);
    controller
        .wait_graceful_deadline(ShutdownMode::Graceful {
            deadline: Duration::from_secs(5),
        })
        .await;
    assert_eq!(started.elapsed(), Duration::from_secs(5));
    let _ = OrderRuntime::default();
}
