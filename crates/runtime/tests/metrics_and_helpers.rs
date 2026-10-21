//! Counters, health, supervisor: the small state machines of the runtime.

use brokapi_runtime::RuntimeMetrics;

#[test]
fn the_counters_count_and_the_buckets_are_kept_apart() {
    let mut m = RuntimeMetrics::new();
    assert_eq!(m, RuntimeMetrics::default());
    m.record_resync();
    m.record_resync();
    m.record_seq_gap();
    m.record_conflated(3);
    m.record_conflated(2);
    m.record_dropped(4);
    m.record_overflow();
    m.record_rate_limit("orders");
    m.record_rate_limit("orders");
    m.record_rate_limit("market_data");
    assert_eq!((m.resync_count, m.seq_gaps), (2, 1));
    assert_eq!(
        (m.buffer_conflated, m.buffer_dropped, m.buffer_overflow),
        (5, 4, 1)
    );
    assert_eq!(m.rate_limit_hits, 3);
    assert_eq!(
        m.rate_limit_by_bucket,
        vec![("orders".to_string(), 2), ("market_data".to_string(), 1)]
    );
}
