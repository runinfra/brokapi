//! Runtime counters (structure only, no exporter).

use std::time::Duration;

/// A plain set of counters about the connector runtime.
///
/// It is a snapshot-style struct for the owner to fill and read. The runtime does not update it by
/// itself and there is no exporter. All counts start at zero and only grow.
///
/// # Examples
///
/// ```
/// use brokapi_runtime::RuntimeMetrics;
///
/// let mut m = RuntimeMetrics::new();
/// m.record_rate_limit("orders");
/// m.record_rate_limit("orders");
/// m.record_rate_limit("global");
/// assert_eq!(m.rate_limit_hits, 3);
/// assert_eq!(m.rate_limit_by_bucket, vec![("orders".to_string(), 2), ("global".to_string(), 1)]);
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RuntimeMetrics {
    /// Number of resyncs recorded with [`record_resync`](Self::record_resync).
    pub resync_count: u64,
    /// Number of sequence gaps recorded with [`record_seq_gap`](Self::record_seq_gap).
    pub seq_gaps: u64,
    /// Number of orders that entered the `Unknown` state. The owner counts them, no method here does.
    pub unknown_orders: u64,
    /// Milliseconds spent resolving `Unknown` orders. The owner decides how to sum it, the runtime
    /// does not fill it.
    pub unknown_resolved_ms: u64,
    /// Number of calls refused by the rate limiter.
    pub rate_limit_hits: u64,
    /// The same refusals split by bucket name, in order of first appearance.
    pub rate_limit_by_bucket: Vec<(String, u64)>,
    /// Events replaced by newer ones in `Conflate` buffers, added with [`record_conflated`](Self::record_conflated).
    pub buffer_conflated: u64,
    /// Events thrown away by `DropNewest` buffers, added with [`record_dropped`](Self::record_dropped).
    pub buffer_dropped: u64,
    /// Overflows of `Lossless` buffers, counted with [`record_overflow`](Self::record_overflow).
    pub buffer_overflow: u64,
    /// Median time from parsing an event to handing it to the consumer. Not filled by the runtime.
    pub egress_latency_p50_ms: Duration,
    /// 99th percentile of the same time. Not filled by the runtime.
    pub egress_latency_p99_ms: Duration,
}

impl RuntimeMetrics {
    /// Creates a zeroed set. Same as [`Default::default`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds one to [`resync_count`](Self::resync_count).
    pub fn record_resync(&mut self) {
        self.resync_count += 1;
    }

    /// Adds one to [`seq_gaps`](Self::seq_gaps).
    pub fn record_seq_gap(&mut self) {
        self.seq_gaps += 1;
    }

    /// Counts one rate-limit refusal, in total and for `bucket`.
    pub fn record_rate_limit(&mut self, bucket: &str) {
        self.rate_limit_hits += 1;
        if let Some(entry) = self
            .rate_limit_by_bucket
            .iter_mut()
            .find(|(b, _)| b == bucket)
        {
            entry.1 += 1;
        } else {
            self.rate_limit_by_bucket.push((bucket.to_string(), 1));
        }
    }

    /// Adds `n` to [`buffer_conflated`](Self::buffer_conflated).
    pub fn record_conflated(&mut self, n: u64) {
        self.buffer_conflated += n;
    }

    /// Adds `n` to [`buffer_dropped`](Self::buffer_dropped).
    pub fn record_dropped(&mut self, n: u64) {
        self.buffer_dropped += n;
    }

    /// Adds one to [`buffer_overflow`](Self::buffer_overflow).
    pub fn record_overflow(&mut self) {
        self.buffer_overflow += 1;
    }
}
