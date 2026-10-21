//! Level-1 runtime tests — no network, virtual time.

use std::time::{Duration, Instant};

use brokapi_core::{
    Backpressure, ClientOrderId, Error, Event, InstrumentId, InstrumentKey, NativeId,
    OrderSnapshot, OrderState, OrderType, Price, Qty, ReconcileReport, ShutdownMode, Side,
    SubscriptionId, Timestamps, UnixNanos,
};
use brokapi_runtime::{
    BackpressureManager, BrokerLimiter, BrokerLimits, CommandGate, InstrumentLocks, OrderRuntime,
    PushResult, ShutdownController, reconcile_ledger,
};
use brokapi_sansio::OrderSignal;

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

fn snapshot(cid_byte: u8) -> OrderSnapshot {
    let cid = cid(cid_byte);
    OrderSnapshot::new(
        cid,
        sber(),
        OrderState::Acked {
            cid,
            broker_id: brokapi_core::BrokerOrderId::new("1"),
        },
        Side::Buy,
        OrderType::Limit,
        Qty::from(1),
        Timestamps::new(UnixNanos::ZERO, Instant::now()),
    )
    .with_filled(Qty::from(0))
    .with_price(Some(Price::from(100)))
}

#[tokio::test(start_paused = true)]
async fn lossless_overflow_degrades_and_rejects_commands() {
    let mut bp = BackpressureManager::new();
    let sub = SubscriptionId::new(1);
    bp.register(sub, sber(), Backpressure::Lossless { cap: 1 });

    assert_eq!(
        bp.ingest(sub, Event::Balances(vec![]), UnixNanos::ZERO),
        PushResult::Delivered
    );
    assert_eq!(
        bp.ingest(sub, Event::Balances(vec![]), UnixNanos::from_nanos(1)),
        PushResult::Overflow
    );
    assert!(bp.is_degraded());

    let mut gate = CommandGate::new();
    gate.mark_reconciled();
    gate.mark_degraded();
    assert!(matches!(gate.check_command(), Err(Error::Degraded)));
}

#[test]
fn reconcile_ledger_categories() {
    let ledger = vec![snapshot(1), snapshot(2)];
    let broker = vec![snapshot(1), snapshot(3)];
    let report: ReconcileReport = reconcile_ledger(&ledger, &broker);
    assert_eq!(report.confirmed.len(), 1);
    assert_eq!(report.closed_offline, vec![cid(2)]);
    assert_eq!(report.foreign.len(), 1);
}

#[tokio::test]
async fn cancel_then_place_serialized_per_instrument() {
    let locks = std::sync::Arc::new(InstrumentLocks::new());
    let inst = sber();
    let order = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));

    let cancel = locks.lock(&inst).await;
    assert!(locks.is_busy(&inst));
    let place = {
        let (locks, inst, order) = (locks.clone(), inst.clone(), order.clone());
        tokio::spawn(async move {
            let _g = locks.lock(&inst).await;
            order.lock().unwrap().push("place");
        })
    };
    tokio::task::yield_now().await;
    order.lock().unwrap().push("cancel");
    drop(cancel);
    place.await.unwrap();
    assert_eq!(*order.lock().unwrap(), vec!["cancel", "place"]);
    assert!(!locks.is_busy(&inst));
}

#[tokio::test]
async fn failed_command_releases_instrument() {
    let locks = InstrumentLocks::new();
    let inst = sber();
    let failing = async {
        let _g = locks.lock(&inst).await;
        Err::<(), &str>("broker rejected")
    };
    assert!(failing.await.is_err());
    assert!(!locks.is_busy(&inst));
}

#[tokio::test]
async fn same_instrument_under_another_native_id_is_one_lock() {
    use brokapi_core::NativeId;
    let locks = InstrumentLocks::new();
    let by_ticker = sber();
    let mut by_uid = sber();
    by_uid.native = NativeId::Uid("uid-sber".into());
    let _g = locks.lock(&by_ticker).await;
    assert!(locks.is_busy(&by_uid));
}

#[tokio::test]
async fn cid_remembers_its_instrument() {
    let locks = InstrumentLocks::new();
    let inst = sber();
    assert!(
        locks.lock_cid(cid(5)).await.is_none(),
        "unknown cid is not serialized"
    );
    locks.bind(cid(5), &inst);
    assert_eq!(locks.instrument_of(cid(5)), Some(inst.clone()));
    let g = locks.lock_cid(cid(5)).await;
    assert!(g.is_some() && locks.is_busy(&inst));
}

#[tokio::test(start_paused = true)]
async fn shutdown_inflight_becomes_unknown_in_report() {
    let gate = CommandGate::new();
    let mut ctl = ShutdownController::new(gate);
    let mut orders = OrderRuntime::new();
    let t0 = Instant::now();
    orders.register_place(cid(9), t0);

    let report = ctl.shutdown_local(
        ShutdownMode::Graceful {
            deadline: Duration::from_secs(5),
        },
        t0,
        &mut orders,
        || (2, 0),
    );

    assert_eq!(report.unsubscribed, 2);
    assert_eq!(report.inflight_abandoned, vec![cid(9)]);
    assert!(matches!(
        orders.state(cid(9)),
        Some(OrderState::Unknown { .. })
    ));
}

#[test]
fn command_gate_blocks_until_reconcile() {
    let mut gate = CommandGate::new();
    assert!(matches!(gate.check_command(), Err(Error::NotReconciled)));
    gate.mark_reconciled();
    assert!(gate.check_command().is_ok());
}

#[test]
fn place_not_auto_retried() {
    let mut orders = OrderRuntime::new();
    assert!(orders.try_place_once(cid(1)));
    assert!(!orders.try_place_once(cid(1)));
}

#[test]
fn command_timeout_moves_to_unknown() {
    let mut orders = OrderRuntime::with_timeout(Duration::from_secs(30));
    let t0 = Instant::now();
    orders.register_place(cid(4), t0);
    let changed = orders.sweep_timeouts(t0 + Duration::from_secs(31));
    assert_eq!(changed.len(), 1);
    assert!(matches!(changed[0].1, OrderState::Unknown { .. }));
}

#[test]
fn a_limits_file_on_disk_loads() {
    let dir = std::env::temp_dir().join(format!("brokapi-limits-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("limits.toml");
    std::fs::write(
        &path,
        "[demo.buckets.orders]\nrate = 10\nper = \"1s\"\nburst = 3\n\
         [demo.route]\nCancelOrder = [\"orders\"]\n\
         [demo.conns]\nmax_conns = 4\nmax_subs_per_conn = 50\nsupports_multiplex = false\n",
    )
    .unwrap();
    let limits = BrokerLimits::from_toml_path(path.to_str().unwrap()).expect("load limits");
    let _ = std::fs::remove_dir_all(&dir);
    let limiter = BrokerLimiter::new(&limits);
    assert!(limiter.acquire("CancelOrder", Instant::now()).is_ok());
}

#[test]
fn ack_after_place_updates_fsm() {
    let mut orders = OrderRuntime::new();
    let t0 = Instant::now();
    orders.register_place(cid(7), t0);
    let next = orders.apply_signal(
        cid(7),
        OrderSignal::Ack {
            broker_id: brokapi_core::BrokerOrderId::new("42"),
        },
        t0,
    );
    assert!(matches!(next, Some(OrderState::Acked { .. })));
}
