//! Fixed-cardinality timing attribution for VaulticDB operations.

use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        OnceLock,
    },
    time::{Duration, Instant},
};

const ACTIVE_TIMING_CAPACITY: usize = 128;

#[derive(Default, serde::Serialize)]
pub(crate) struct FutureTiming {
    elapsed_ns: u64,
    poll_ns: u64,
    polls: u64,
    pending_polls: u64,
}

impl FutureTiming {
    pub(crate) async fn measure<F: std::future::Future>(
        &mut self,
        enabled: bool,
        future: F,
    ) -> F::Output {
        if !enabled {
            return future.await;
        }
        let guard = FutureTimingGuard {
            timing: self,
            started: Instant::now(),
        };
        let mut future = std::pin::pin!(future);
        std::future::poll_fn(|context| {
            let started = Instant::now();
            let result = future.as_mut().poll(context);
            guard.timing.poll_ns = guard
                .timing
                .poll_ns
                .saturating_add(duration_ns(started.elapsed()));
            guard.timing.polls = guard.timing.polls.saturating_add(1);
            if result.is_pending() {
                guard.timing.pending_polls = guard.timing.pending_polls.saturating_add(1);
            }
            result
        })
        .await
    }
}

struct FutureTimingGuard<'a> {
    timing: &'a mut FutureTiming,
    started: Instant,
}

impl Drop for FutureTimingGuard<'_> {
    fn drop(&mut self) {
        self.timing.elapsed_ns = self
            .timing
            .elapsed_ns
            .saturating_add(duration_ns(self.started.elapsed()));
    }
}

fn duration_ns(elapsed: Duration) -> u64 {
    elapsed.as_nanos().try_into().unwrap_or(u64::MAX)
}

pub(crate) struct ScanStreamTiming {
    pub(crate) enabled: bool,
    pub(crate) setup: FutureTiming,
    pub(crate) delivery: FutureTiming,
    pub(crate) collect: FutureTiming,
    pub(crate) validation: FutureTiming,
    pub(crate) outcome: &'static str,
    started: Instant,
}

impl ScanStreamTiming {
    pub(crate) fn new(enabled: bool) -> Self {
        Self {
            enabled,
            setup: FutureTiming::default(),
            delivery: FutureTiming::default(),
            collect: FutureTiming::default(),
            validation: FutureTiming::default(),
            outcome: "cancelled",
            started: Instant::now(),
        }
    }
}

impl Drop for ScanStreamTiming {
    fn drop(&mut self) {
        if self.enabled {
            use std::io::Write;
            let _ = writeln!(
                std::io::stderr().lock(),
                "{}",
                serde_json::json!({
                    "category": "diagnostic", "component": "vaulticdb", "event": "scan_stream_timing",
                    "fields": {
                        "elapsed_ns": duration_ns(self.started.elapsed()), "outcome": self.outcome,
                        "setup": self.setup, "delivery": self.delivery,
                        "collect": self.collect, "validation": self.validation,
                    },
                })
            );
        }
    }
}

pub(crate) const LATENCY_BUCKET_UPPER_US: [u64; 8] = [
    10,
    100,
    1_000,
    10_000,
    100_000,
    1_000_000,
    10_000_000,
    u64::MAX,
];

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct TimingSnapshot {
    pub(crate) attempts: u64,
    pub(crate) failures: u64,
    pub(crate) total_us: u64,
    pub(crate) max_us: u64,
    pub(crate) completed: u64,
    pub(crate) successes: u64,
    pub(crate) cancellations: u64,
    pub(crate) timeouts: u64,
    pub(crate) active: u64,
    pub(crate) oldest_active_us: u64,
    pub(crate) active_overflow: u64,
    pub(crate) latency_buckets: [u64; LATENCY_BUCKET_UPPER_US.len()],
    pub(crate) contention_available: bool,
    pub(crate) contentions: u64,
}

#[derive(Debug)]
pub(crate) struct TimingMetric {
    enabled: bool,
    attempts: AtomicU64,
    completed: AtomicU64,
    failures: AtomicU64,
    successes: AtomicU64,
    cancellations: AtomicU64,
    timeouts: AtomicU64,
    total_us: AtomicU64,
    max_us: AtomicU64,
    latency_buckets: [AtomicU64; LATENCY_BUCKET_UPPER_US.len()],
    contention_available: bool,
    contentions: AtomicU64,
    active_count: AtomicU64,
    active: [AtomicU64; ACTIVE_TIMING_CAPACITY],
    active_overflow: AtomicU64,
}

impl Default for TimingMetric {
    fn default() -> Self {
        Self::new(true, false)
    }
}

impl TimingMetric {
    const fn new(enabled: bool, contention_available: bool) -> Self {
        Self {
            enabled,
            attempts: AtomicU64::new(0),
            completed: AtomicU64::new(0),
            failures: AtomicU64::new(0),
            successes: AtomicU64::new(0),
            cancellations: AtomicU64::new(0),
            timeouts: AtomicU64::new(0),
            total_us: AtomicU64::new(0),
            max_us: AtomicU64::new(0),
            latency_buckets: [const { AtomicU64::new(0) }; LATENCY_BUCKET_UPPER_US.len()],
            contention_available,
            contentions: AtomicU64::new(0),
            active_count: AtomicU64::new(0),
            active: [const { AtomicU64::new(0) }; ACTIVE_TIMING_CAPACITY],
            active_overflow: AtomicU64::new(0),
        }
    }

    pub(crate) const fn with_contention() -> Self {
        Self::new(true, true)
    }

    pub(crate) const fn disabled(contention_available: bool) -> Self {
        Self::new(false, contention_available)
    }

    #[cfg(test)]
    pub(crate) fn observe(&self, elapsed: Duration, outcome: TimingOutcome) {
        self.settle(elapsed, outcome);
    }

    pub(crate) fn timer(&self) -> TimingGuard<'_> {
        TimingGuard {
            metric: self,
            token: self.start(),
            outcome: TimingOutcome::Cancellation,
        }
    }

    pub(crate) fn timer_owned(self: &std::sync::Arc<Self>) -> OwnedTimingGuard {
        OwnedTimingGuard {
            metric: std::sync::Arc::clone(self),
            token: self.start(),
            outcome: TimingOutcome::Cancellation,
        }
    }

    pub(crate) fn start(&self) -> Option<TimingToken> {
        if !self.enabled {
            return None;
        }
        let started = Instant::now();
        saturating_increment(&self.attempts, 1);
        saturating_increment(&self.active_count, 1);
        let stamp = monotonic_us();
        let slot = self.active.iter().position(|active| {
            active
                .compare_exchange(0, stamp, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
        });
        if slot.is_none() {
            saturating_increment(&self.active_overflow, 1);
        }
        Some(TimingToken {
            started,
            slot,
            stamp,
        })
    }

    pub(crate) fn complete(&self, token: TimingToken, outcome: TimingOutcome) {
        if let Some(slot) = token.slot {
            let _ = self.active[slot].compare_exchange(
                token.stamp,
                0,
                Ordering::AcqRel,
                Ordering::Relaxed,
            );
        }
        saturating_decrement(&self.active_count);
        self.settle(token.started.elapsed(), outcome);
    }

    pub(crate) fn record_contention(&self) {
        if self.enabled && self.contention_available {
            saturating_increment(&self.contentions, 1);
        }
    }

    pub(crate) fn snapshot(&self) -> TimingSnapshot {
        if !self.enabled {
            return TimingSnapshot::default();
        }
        let now = monotonic_us();
        let mut oldest = u64::MAX;
        for active in &self.active {
            let stamp = active.load(Ordering::Acquire);
            if stamp != 0 {
                oldest = oldest.min(stamp);
            }
        }
        let oldest_active_us = (oldest != u64::MAX)
            .then(|| now.saturating_sub(oldest))
            .unwrap_or(0);
        let completed = self.completed.load(Ordering::Relaxed);
        TimingSnapshot {
            attempts: self.attempts.load(Ordering::Relaxed),
            failures: self.failures.load(Ordering::Relaxed),
            total_us: self.total_us.load(Ordering::Relaxed),
            max_us: self.max_us.load(Ordering::Relaxed),
            completed,
            successes: self.successes.load(Ordering::Relaxed),
            cancellations: self.cancellations.load(Ordering::Relaxed),
            timeouts: self.timeouts.load(Ordering::Relaxed),
            active: self.active_count.load(Ordering::Relaxed),
            oldest_active_us,
            active_overflow: self.active_overflow.load(Ordering::Relaxed),
            latency_buckets: std::array::from_fn(|index| {
                self.latency_buckets[index].load(Ordering::Relaxed)
            }),
            contention_available: self.contention_available,
            contentions: self.contentions.load(Ordering::Relaxed),
        }
    }

    fn settle(&self, elapsed: Duration, outcome: TimingOutcome) {
        let elapsed_us = duration_us(elapsed);
        saturating_increment(&self.completed, 1);
        match outcome {
            TimingOutcome::Success => saturating_increment(&self.successes, 1),
            TimingOutcome::Failure => saturating_increment(&self.failures, 1),
            TimingOutcome::Cancellation => saturating_increment(&self.cancellations, 1),
            TimingOutcome::Timeout => saturating_increment(&self.timeouts, 1),
        }
        saturating_increment(&self.total_us, elapsed_us);
        self.max_us.fetch_max(elapsed_us, Ordering::Relaxed);
        let bucket = LATENCY_BUCKET_UPPER_US.partition_point(|upper| *upper < elapsed_us);
        saturating_increment(&self.latency_buckets[bucket], 1);
    }
}

fn duration_us(elapsed: Duration) -> u64 {
    elapsed.as_micros().try_into().unwrap_or(u64::MAX)
}

fn monotonic_us() -> u64 {
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    duration_us(EPOCH.get_or_init(Instant::now).elapsed()).saturating_add(1)
}

fn saturating_increment(value: &AtomicU64, increment: u64) {
    let _ = value.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(increment))
    });
}

fn saturating_decrement(value: &AtomicU64) {
    let _ = value.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_sub(1))
    });
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TimingOutcome {
    Success,
    Failure,
    Cancellation,
    #[allow(dead_code)]
    Timeout,
}

#[derive(Debug)]
pub(crate) struct TimingToken {
    started: Instant,
    slot: Option<usize>,
    stamp: u64,
}

pub(crate) struct TimingGuard<'a> {
    metric: &'a TimingMetric,
    token: Option<TimingToken>,
    outcome: TimingOutcome,
}

impl TimingGuard<'_> {
    pub(crate) fn record_result<T, E>(&mut self, result: &Result<T, E>) {
        if result.is_ok() {
            self.succeeded();
        } else {
            self.failed();
        }
    }

    pub(crate) fn succeeded(&mut self) {
        self.outcome = TimingOutcome::Success;
    }

    pub(crate) fn failed(&mut self) {
        self.outcome = TimingOutcome::Failure;
    }

    #[cfg(test)]
    pub(crate) fn timed_out(&mut self) {
        self.outcome = TimingOutcome::Timeout;
    }
}

impl Drop for TimingGuard<'_> {
    fn drop(&mut self) {
        if let Some(token) = self.token.take() {
            self.metric.complete(token, self.outcome);
        }
    }
}

pub(crate) struct OwnedTimingGuard {
    metric: std::sync::Arc<TimingMetric>,
    token: Option<TimingToken>,
    outcome: TimingOutcome,
}

impl OwnedTimingGuard {
    pub(crate) fn succeeded(&mut self) {
        self.outcome = TimingOutcome::Success;
    }

    pub(crate) fn failed(&mut self) {
        self.outcome = TimingOutcome::Failure;
    }
}

impl Drop for OwnedTimingGuard {
    fn drop(&mut self) {
        if let Some(token) = self.token.take() {
            self.metric.complete(token, self.outcome);
        }
    }
}

#[derive(Debug)]
pub(crate) struct ServiceAttribution {
    pub(crate) admission_wait: TimingMetric,
    pub(crate) admission_lock_hold: TimingMetric,
    pub(crate) fence_check: TimingMetric,
    pub(crate) write_batch_request: TimingMetric,
    pub(crate) begin_request: TimingMetric,
    pub(crate) commit_request: TimingMetric,
    pub(crate) rollback_request: TimingMetric,
}

impl Default for ServiceAttribution {
    fn default() -> Self {
        Self::new(true)
    }
}

impl ServiceAttribution {
    pub(crate) fn new(enabled: bool) -> Self {
        if !enabled {
            return Self {
                admission_wait: TimingMetric::disabled(false),
                admission_lock_hold: TimingMetric::disabled(false),
                fence_check: TimingMetric::disabled(false),
                write_batch_request: TimingMetric::disabled(false),
                begin_request: TimingMetric::disabled(false),
                commit_request: TimingMetric::disabled(false),
                rollback_request: TimingMetric::disabled(false),
            };
        }
        Self {
            admission_wait: TimingMetric::with_contention(),
            admission_lock_hold: TimingMetric::default(),
            fence_check: TimingMetric::default(),
            write_batch_request: TimingMetric::default(),
            begin_request: TimingMetric::default(),
            commit_request: TimingMetric::default(),
            rollback_request: TimingMetric::default(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct StorageAttribution {
    pub(crate) transaction_begin: TimingMetric,
    pub(crate) transaction_map_lock_wait: TimingMetric,
    pub(crate) transaction_slot_lock_wait: TimingMetric,
    pub(crate) engine_submit: TimingMetric,
    pub(crate) durable_wait: TimingMetric,
    pub(crate) finalization: TimingMetric,
    pub(crate) object_store_main: std::sync::Arc<crate::storage::ObjectStoreRoleMetrics>,
    pub(crate) object_store_wal: std::sync::Arc<crate::storage::ObjectStoreRoleMetrics>,
    pub(crate) object_store_coordination: std::sync::Arc<crate::storage::ObjectStoreRoleMetrics>,
}

impl Default for StorageAttribution {
    fn default() -> Self {
        Self::new(true)
    }
}

impl StorageAttribution {
    pub(crate) fn new(enabled: bool) -> Self {
        let metric = || {
            if enabled {
                TimingMetric::default()
            } else {
                TimingMetric::disabled(false)
            }
        };
        Self {
            transaction_begin: metric(),
            transaction_map_lock_wait: metric(),
            transaction_slot_lock_wait: metric(),
            engine_submit: metric(),
            durable_wait: metric(),
            finalization: metric(),
            object_store_main: std::sync::Arc::new(crate::storage::ObjectStoreRoleMetrics::new(
                enabled,
            )),
            object_store_wal: std::sync::Arc::new(crate::storage::ObjectStoreRoleMetrics::new(
                enabled,
            )),
            object_store_coordination: std::sync::Arc::new(
                crate::storage::ObjectStoreRoleMetrics::new(enabled),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn future_timing_preserves_pending_and_error_results() {
        let mut timing = FutureTiming::default();
        let mut first = true;
        let result = timing
            .measure(
                true,
                std::future::poll_fn(|context| {
                    if first {
                        first = false;
                        context.waker().wake_by_ref();
                        std::task::Poll::Pending
                    } else {
                        std::task::Poll::Ready(Err::<(), _>("expected"))
                    }
                }),
            )
            .await;
        assert_eq!(result, Err("expected"));
        assert_eq!(timing.polls, 2);
        assert_eq!(timing.pending_polls, 1);
        assert!(timing.elapsed_ns >= timing.poll_ns);
        assert!(timing.elapsed_ns > 0);
    }

    #[test]
    fn future_timing_settles_on_cancellation_and_disabled_is_empty() {
        use std::future::Future;
        let mut timing = FutureTiming::default();
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        {
            let mut future = std::pin::pin!(timing.measure(true, std::future::pending::<()>()));
            assert!(future.as_mut().poll(&mut context).is_pending());
        }
        assert_eq!(timing.polls, 1);
        assert_eq!(timing.pending_polls, 1);
        assert!(timing.elapsed_ns >= timing.poll_ns);
        let mut disabled = FutureTiming::default();
        {
            let mut future = std::pin::pin!(disabled.measure(false, std::future::ready(42)));
            assert_eq!(
                future.as_mut().poll(&mut context),
                std::task::Poll::Ready(42)
            );
        }
        assert_eq!(disabled.polls, 0);
        assert_eq!(disabled.elapsed_ns, 0);
    }

    #[test]
    fn guard_is_active_until_drop_and_settles_success() {
        let metric = TimingMetric::default();
        let mut guard = metric.timer();
        assert_eq!(metric.snapshot().active, 1);
        guard.succeeded();
        drop(guard);
        let snapshot = metric.snapshot();
        assert_eq!(snapshot.active, 0);
        assert_eq!(snapshot.completed, 1);
        assert_eq!(snapshot.successes, 1);
        assert_eq!(snapshot.attempts, 1);
    }

    #[test]
    fn dropped_and_explicitly_failed_guards_have_distinct_outcomes() {
        let metric = TimingMetric::default();
        drop(metric.timer());
        let mut failed = metric.timer();
        failed.failed();
        drop(failed);
        let mut timed_out = metric.timer();
        timed_out.timed_out();
        drop(timed_out);
        let snapshot = metric.snapshot();
        assert_eq!(snapshot.cancellations, 1);
        assert_eq!(snapshot.failures, 1);
        assert_eq!(snapshot.timeouts, 1);
    }

    #[test]
    fn histogram_boundaries_and_saturation_are_bounded() {
        let metric = TimingMetric::default();
        for micros in [0, 10, 11, 100, 101, 10_000_001, u64::MAX] {
            metric.observe(Duration::from_micros(micros), TimingOutcome::Success);
        }
        assert_eq!(metric.snapshot().latency_buckets, [2, 2, 1, 0, 0, 0, 0, 2]);
        metric.completed.store(u64::MAX, Ordering::Relaxed);
        metric.latency_buckets[0].store(u64::MAX, Ordering::Relaxed);
        metric.observe(Duration::ZERO, TimingOutcome::Success);
        assert_eq!(metric.snapshot().completed, u64::MAX);
        assert_eq!(metric.snapshot().latency_buckets[0], u64::MAX);
    }

    #[test]
    fn oldest_active_age_tracks_the_oldest_guard() {
        let metric = TimingMetric::default();
        let first = metric.timer();
        std::thread::sleep(Duration::from_millis(2));
        let second = metric.timer();
        let with_both = metric.snapshot();
        assert_eq!(with_both.active, 2);
        assert!(with_both.oldest_active_us >= 1_000);
        drop(first);
        assert_eq!(metric.snapshot().active, 1);
        drop(second);
    }

    #[test]
    fn active_tracking_is_bounded_and_overflow_still_settles() {
        let metric = TimingMetric::default();
        let guards = (0..ACTIVE_TIMING_CAPACITY + 7)
            .map(|_| metric.timer())
            .collect::<Vec<_>>();
        let active = metric.snapshot();
        assert_eq!(active.active, (ACTIVE_TIMING_CAPACITY + 7) as u64);
        assert_eq!(active.active_overflow, 7);
        drop(guards);
        let settled = metric.snapshot();
        assert_eq!(settled.active, 0);
        assert_eq!(settled.completed, (ACTIVE_TIMING_CAPACITY + 7) as u64);
        assert_eq!(settled.cancellations, (ACTIVE_TIMING_CAPACITY + 7) as u64);
    }

    #[test]
    fn concurrent_observations_produce_consistent_histograms() {
        let metric = Arc::new(TimingMetric::default());
        let workers = (0..8)
            .map(|_| {
                let metric = metric.clone();
                std::thread::spawn(move || {
                    for _ in 0..1_000 {
                        metric.observe(Duration::from_micros(100), TimingOutcome::Success);
                    }
                })
            })
            .collect::<Vec<_>>();
        for worker in workers {
            worker.join().unwrap();
        }
        let snapshot = metric.snapshot();
        assert_eq!(snapshot.completed, 8_000);
        assert_eq!(snapshot.successes, 8_000);
        assert_eq!(snapshot.latency_buckets.iter().sum::<u64>(), 8_000);
        assert_eq!(snapshot.latency_buckets[1], 8_000);
    }

    #[test]
    fn contention_is_explicitly_available_or_unavailable() {
        let unavailable = TimingMetric::default();
        unavailable.record_contention();
        assert!(!unavailable.snapshot().contention_available);
        assert_eq!(unavailable.snapshot().contentions, 0);
        let available = TimingMetric::with_contention();
        available.record_contention();
        assert!(available.snapshot().contention_available);
        assert_eq!(available.snapshot().contentions, 1);
    }

    #[test]
    fn disabled_metric_guards_are_noops() {
        let metric = TimingMetric::disabled(true);
        let mut guard = metric.timer();
        guard.succeeded();
        metric.record_contention();
        drop(guard);
        assert_eq!(metric.snapshot(), TimingSnapshot::default());
    }
}
