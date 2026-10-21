//! Memory windows, the order lifecycle in its real order, conflation by kind, the units of a limits file.

use std::time::{Duration, Instant};

use brokapi_core::{
    Backpressure, BrokerOrderId, ClientOrderId, Event, InstrumentId, InstrumentKey, NativeId,
    OrderState, SubscriptionId, UnixNanos,
};
use brokapi_runtime::{
    BOUND_CIDS, BackpressureManager, BrokerLimits, FinishedWindow, InstrumentLocks, OrderRuntime,
    PushResult, SubscriptionBuffer, run_detached,
};
use brokapi_sansio::{FsmError, OrderSignal};

fn sber() -> InstrumentId {
    InstrumentId::new(
        InstrumentKey::new("SBER", "TQBR"),
        NativeId::TickerClass {
            ticker: "SBER".into(),
            class_code: "TQBR".into(),
        },
    )
}

fn cid(n: u8) -> ClientOrderId {
    ClientOrderId::from_bytes([n; 16])
}

// ----- the memory window of OrderRuntime -----

#[test]
fn finished_orders_are_forgotten_after_the_window_and_the_cid_is_free_again() {
    let mut orders = OrderRuntime::new().with_retention(Duration::from_secs(60), 1000);
    let t0 = Instant::now();
    assert!(orders.try_place_once(cid(1)));
    orders.register_place(cid(1), t0);
    assert_eq!(orders.in_flight_count(), 1);
    orders.apply_signal(cid(1), OrderSignal::Filled, t0);
    assert_eq!((orders.tracked(), orders.remembered_places()), (1, 1));

    // Inside the window the guarantee "a cid is placed once" holds.
    orders.prune(t0 + Duration::from_secs(59));
    assert!(!orders.try_place_once(cid(1)));
    // Past it the order is forgotten, in both maps.
    orders.prune(t0 + Duration::from_secs(60));
    assert_eq!((orders.tracked(), orders.remembered_places()), (0, 0));
    assert!(orders.state(cid(1)).is_none());
}

#[test]
fn the_count_limit_drops_the_oldest_finished_orders_first() {
    let mut orders = OrderRuntime::new().with_retention(Duration::from_secs(3600), 2);
    let t0 = Instant::now();
    for n in 1..=4u8 {
        orders.register_place(cid(n), t0);
        orders.apply_signal(
            cid(n),
            OrderSignal::Rejected {
                reason: "no".into(),
            },
            t0,
        );
    }
    // Only the last two are kept.
    assert_eq!(orders.tracked(), 2);
    assert!(orders.state(cid(1)).is_none() && orders.state(cid(2)).is_none());
    assert!(orders.state(cid(3)).is_some() && orders.state(cid(4)).is_some());
}

#[test]
fn a_cid_registered_again_after_it_finished_is_not_forgotten_with_the_old_record() {
    let mut orders = OrderRuntime::new().with_retention(Duration::from_secs(10), 1000);
    let t0 = Instant::now();
    orders.register_place(cid(1), t0);
    orders.apply_signal(cid(1), OrderSignal::Filled, t0);
    // The same cid is registered again (a restart of the machine): alive now.
    orders.register_place(cid(1), t0 + Duration::from_secs(5));
    orders.prune(t0 + Duration::from_secs(30));
    assert!(matches!(
        orders.state(cid(1)),
        Some(OrderState::InFlight { .. })
    ));
}

#[test]
fn a_live_order_is_never_pruned() {
    let mut orders = OrderRuntime::new().with_retention(Duration::from_secs(1), 0);
    let t0 = Instant::now();
    orders.register_place(cid(1), t0);
    orders.apply_signal(
        cid(1),
        OrderSignal::Ack {
            broker_id: BrokerOrderId::new("1"),
        },
        t0,
    );
    orders.prune(t0 + Duration::from_secs(1000));
    assert!(matches!(
        orders.state(cid(1)),
        Some(OrderState::Acked { .. })
    ));
}

#[test]
fn the_default_window_is_a_day() {
    assert_eq!(brokapi_runtime::RETENTION, Duration::from_secs(86_400));
    assert_eq!(brokapi_runtime::RETAINED_FINISHED, 100_000);
}

#[test]
fn signals_that_do_not_fit_are_told_apart() {
    let mut orders = OrderRuntime::new();
    let t0 = Instant::now();
    // Unknown cid: an error for the one who asks, and `None` for the one who does not.
    assert!(matches!(
        orders.try_apply_signal(cid(9), OrderSignal::Filled, t0),
        Err(FsmError::UnknownOrder(_))
    ));
    assert_eq!(orders.apply_signal(cid(9), OrderSignal::Filled, t0), None);
    // A contradiction is logged and ignored.
    orders.register_place(cid(1), t0);
    orders.apply_signal(cid(1), OrderSignal::Filled, t0);
    assert!(matches!(
        orders.try_apply_signal(cid(1), OrderSignal::Cancelled, t0),
        Err(FsmError::InvalidTransition { .. })
    ));
    assert_eq!(
        orders.apply_signal(cid(1), OrderSignal::Cancelled, t0),
        None
    );
    assert!(matches!(
        orders.state(cid(1)),
        Some(OrderState::Filled { .. })
    ));
}

#[test]
fn the_command_timeout_can_be_set_and_sweeps_what_waited_too_long() {
    let mut orders = OrderRuntime::new();
    orders.set_command_timeout(Duration::from_secs(100));
    let t0 = Instant::now();
    orders.register_place(cid(1), t0);
    assert!(
        orders
            .sweep_timeouts(t0 + Duration::from_secs(99))
            .is_empty()
    );
    let changed = orders.sweep_timeouts(t0 + Duration::from_secs(100));
    assert!(matches!(changed[0].1, OrderState::Unknown { .. }));
    assert_eq!(orders.in_flight_count(), 0);
}

#[test]
fn may_send_place_is_try_place_once() {
    let mut orders = OrderRuntime::new();
    assert!(orders.may_send_place(cid(1)));
    assert!(!orders.may_send_place(cid(1)));
    // A cancel-only cid is tracked but does not count as a place attempt.
    orders.register_cancel(cid(2), Instant::now());
    assert!(orders.state(cid(2)).is_some());
    assert!(orders.try_place_once(cid(2)));
    orders.register_cancel(cid(2), Instant::now());
}

#[test]
fn the_finished_window_gives_back_the_old_keys_in_order() {
    let mut window = FinishedWindow::with_limits(Duration::from_secs(10), 2);
    assert!(window.is_empty());
    let t0 = Instant::now();
    window.push(t0, 1);
    window.push(t0, 2);
    window.push(t0, 3);
    // Three held with a limit of two: the oldest goes at once.
    assert_eq!(window.expired(t0), vec![1]);
    assert_eq!(window.len(), 2);
    assert_eq!(window.expired(t0 + Duration::from_secs(10)), vec![2, 3]);
    assert!(window.is_empty());
    let default: FinishedWindow<u8> = FinishedWindow::default();
    assert!(default.is_empty());
    assert!(FinishedWindow::<u8>::new().is_empty());
}

// ----- InstrumentLocks -----

#[test]
fn the_cid_map_of_the_locks_can_forget_and_is_bounded() {
    let locks = InstrumentLocks::new();
    locks.bind(cid(1), &sber());
    assert_eq!(locks.bound(), 1);
    // The same cid again does not count twice.
    locks.bind(cid(1), &sber());
    assert_eq!(locks.bound(), 1);
    assert!(locks.instrument_of(cid(1)).is_some());
    locks.forget(cid(1));
    assert_eq!(locks.bound(), 0);
    assert!(locks.instrument_of(cid(1)).is_none());

    // Past the bound the oldest are dropped.
    for n in 0..(BOUND_CIDS + 5) {
        let mut bytes = [0u8; 16];
        bytes[..8].copy_from_slice(&(n as u64).to_be_bytes());
        locks.bind(ClientOrderId::from_bytes(bytes), &sber());
    }
    assert_eq!(locks.bound(), BOUND_CIDS);
    assert!(
        locks
            .instrument_of(ClientOrderId::from_bytes([0; 16]))
            .is_none()
    );
}

// ----- Conflate holds the latest value of each kind -----

fn bal(n: i64) -> Event {
    Event::Balances(vec![brokapi_core::Balance {
        currency: "RUB".into(),
        available: brokapi_core::Money::from(n),
        blocked: brokapi_core::Money::ZERO,
        ts: brokapi_core::Timestamps::new(UnixNanos::ZERO, Instant::now()),
    }])
}

fn pos() -> Event {
    Event::Positions(vec![])
}

#[test]
fn conflate_keeps_the_latest_event_of_every_kind() {
    let mut buf = SubscriptionBuffer::new(SubscriptionId::new(1), sber(), Backpressure::Conflate);
    assert_eq!(buf.try_push(pos()), PushResult::Conflated);
    assert_eq!(buf.try_push(bal(1)), PushResult::Conflated);
    // A newer balance replaces the older balance and leaves the positions alone.
    assert_eq!(buf.try_push(bal(2)), PushResult::Conflated);
    assert_eq!(buf.conflated(), 1);
    assert_eq!(buf.len(), 2);
    let drained = buf.drain();
    assert_eq!(drained.len(), 2);
    assert!(matches!(drained[0], Event::Positions(_)));
    match &drained[1] {
        Event::Balances(v) => assert_eq!(v[0].available, brokapi_core::Money::from(2)),
        other => panic!("{other:?}"),
    }
}

#[test]
fn the_manager_hands_the_kinds_out_one_by_one() {
    let sub = SubscriptionId::new(1);
    let mut mgr = BackpressureManager::new();
    mgr.register(sub, sber(), Backpressure::Conflate);
    mgr.ingest(sub, pos(), UnixNanos::ZERO);
    mgr.ingest(sub, bal(1), UnixNanos::ZERO);
    mgr.ingest(sub, bal(2), UnixNanos::ZERO);
    assert!(matches!(mgr.pop_ready(sub), Some(Event::Positions(_))));
    assert!(matches!(mgr.pop_ready(sub), Some(Event::Balances(_))));
    assert!(mgr.pop_ready(sub).is_none());
}

// ----- the units of a limits file -----

fn file(per: &str) -> String {
    format!(
        "[demo.buckets.b]\nrate = 1\nper = \"{per}\"\nburst = 1\n[demo.route]\n\"*\" = [\"b\"]\n[demo.conns]\nmax_conns = 1\nmax_subs_per_conn = 1\nsupports_multiplex = false\n"
    )
}

#[test]
fn a_period_can_be_given_in_hours_and_days() {
    let get = |per: &str| BrokerLimits::from_toml_str(&file(per)).unwrap().buckets["b"].per;
    assert_eq!(get("250ms"), Duration::from_millis(250));
    assert_eq!(get("2s"), Duration::from_secs(2));
    assert_eq!(get("3m"), Duration::from_secs(180));
    assert_eq!(get("24h"), Duration::from_secs(86_400));
    assert_eq!(get("2d"), Duration::from_secs(172_800));
}

#[test]
fn a_period_that_cannot_be_right_is_refused() {
    for bad in [
        "0s",
        "0ms",
        "1w",
        "1.5s",
        "1 s",
        "s",
        "",
        "999999999999999999d",
    ] {
        assert!(
            BrokerLimits::from_toml_str(&file(bad)).is_err(),
            "{bad:?} must be refused"
        );
    }
}

// ----- run_detached -----

#[tokio::test]
async fn run_detached_returns_the_value_and_reports_a_panic_as_an_error() {
    assert_eq!(run_detached(async { 3 }).await, Some(3));
    let lost = run_detached(async {
        if true {
            panic!("the work died");
        }
        1
    })
    .await;
    assert_eq!(lost, None);
}
