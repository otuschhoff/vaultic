use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};
use std::time::{Duration, Instant};

#[derive(Debug, Default)]
pub struct FileReadMetrics {
    calls: AtomicU64,
    started: AtomicU64,
    finished: AtomicU64,
    errors: AtomicU64,
    panics: AtomicU64,
    abandoned: AtomicU64,
    active: AtomicU64,
    queue_ns: AtomicU64,
    service_ns: AtomicU64,
    resumed: AtomicU64,
    completion_ns: AtomicU64,
    cancellations: AtomicU64,
    direct_calls: AtomicU64,
    range: RangeReadMetrics,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct FileReadSnapshot {
    pub calls: u64,
    pub started: u64,
    pub finished: u64,
    pub errors: u64,
    pub panics: u64,
    pub abandoned: u64,
    pub active: u64,
    pub queue_ns: u64,
    pub service_ns: u64,
    pub resumed: u64,
    pub completion_ns: u64,
    pub cancellations: u64,
    pub direct_calls: u64,
    pub range: RangeReadSnapshot,
}

#[derive(Debug, Default)]
struct ReadPhaseMetrics {
    calls: AtomicU64,
    ns: AtomicU64,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct ReadPhaseSnapshot {
    pub calls: u64,
    pub ns: u64,
}

#[derive(Debug, Default)]
struct RangeReadMetrics {
    calls: AtomicU64,
    finished: AtomicU64,
    errors: AtomicU64,
    panics: AtomicU64,
    active: AtomicU64,
    requested_bytes: AtomicU64,
    empty_calls: AtomicU64,
    read_bytes: AtomicU64,
    read_errors: AtomicU64,
    read_interrupted: AtomicU64,
    read_eof: AtomicU64,
    unsupported_calls: AtomicU64,
    total_ns: AtomicU64,
    allocation: ReadPhaseMetrics,
    zero_fill: ReadPhaseMetrics,
    read: ReadPhaseMetrics,
    conversion: ReadPhaseMetrics,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct RangeReadSnapshot {
    pub calls: u64,
    pub finished: u64,
    pub errors: u64,
    pub panics: u64,
    pub active: u64,
    pub requested_bytes: u64,
    pub empty_calls: u64,
    pub read_bytes: u64,
    pub read_errors: u64,
    pub read_interrupted: u64,
    pub read_eof: u64,
    pub unsupported_calls: u64,
    pub total_ns: u64,
    pub allocation: ReadPhaseSnapshot,
    pub zero_fill: ReadPhaseSnapshot,
    pub read: ReadPhaseSnapshot,
    pub conversion: ReadPhaseSnapshot,
}

impl RangeReadMetrics {
    fn snapshot(&self) -> RangeReadSnapshot {
        let phase = |metrics: &ReadPhaseMetrics| ReadPhaseSnapshot {
            calls: metrics.calls.load(Ordering::Relaxed),
            ns: metrics.ns.load(Ordering::Relaxed),
        };
        RangeReadSnapshot {
            calls: self.calls.load(Ordering::Relaxed),
            finished: self.finished.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            panics: self.panics.load(Ordering::Relaxed),
            active: self.active.load(Ordering::Relaxed),
            requested_bytes: self.requested_bytes.load(Ordering::Relaxed),
            empty_calls: self.empty_calls.load(Ordering::Relaxed),
            read_bytes: self.read_bytes.load(Ordering::Relaxed),
            read_errors: self.read_errors.load(Ordering::Relaxed),
            read_interrupted: self.read_interrupted.load(Ordering::Relaxed),
            read_eof: self.read_eof.load(Ordering::Relaxed),
            unsupported_calls: self.unsupported_calls.load(Ordering::Relaxed),
            total_ns: self.total_ns.load(Ordering::Relaxed),
            allocation: phase(&self.allocation),
            zero_fill: phase(&self.zero_fill),
            read: phase(&self.read),
            conversion: phase(&self.conversion),
        }
    }
}

impl FileReadMetrics {
    pub fn snapshot(&self) -> FileReadSnapshot {
        FileReadSnapshot {
            calls: self.calls.load(Ordering::Relaxed),
            started: self.started.load(Ordering::Relaxed),
            finished: self.finished.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            panics: self.panics.load(Ordering::Relaxed),
            abandoned: self.abandoned.load(Ordering::Relaxed),
            active: self.active.load(Ordering::Relaxed),
            queue_ns: self.queue_ns.load(Ordering::Relaxed),
            service_ns: self.service_ns.load(Ordering::Relaxed),
            resumed: self.resumed.load(Ordering::Relaxed),
            completion_ns: self.completion_ns.load(Ordering::Relaxed),
            cancellations: self.cancellations.load(Ordering::Relaxed),
            direct_calls: self.direct_calls.load(Ordering::Relaxed),
            range: self.range.snapshot(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct FileReadObservation(pub Arc<FileReadMetrics>);

impl FileReadObservation {
    pub fn submit(&self, blocking: bool) -> FileReadWorker {
        let submitted = Instant::now();
        self.0.calls.fetch_add(1, Ordering::Relaxed);
        self.0.active.fetch_add(1, Ordering::Relaxed);
        if !blocking {
            self.0.direct_calls.fetch_add(1, Ordering::Relaxed);
        }
        FileReadWorker {
            metrics: self.0.clone(),
            submitted,
            blocking,
            started: None,
            finished: false,
            completed_at: Arc::new(Mutex::new(None)),
        }
    }
}

pub struct FileReadWorker {
    metrics: Arc<FileReadMetrics>,
    submitted: Instant,
    blocking: bool,
    started: Option<Instant>,
    finished: bool,
    completed_at: Arc<Mutex<Option<Instant>>>,
}

fn add_time(counter: &AtomicU64, elapsed: Duration) {
    counter.fetch_add(
        elapsed.as_nanos().try_into().unwrap_or(u64::MAX),
        Ordering::Relaxed,
    );
}

impl FileReadWorker {
    pub fn range_read(&self, requested_bytes: u64) -> RangeReadObservation {
        let started = Instant::now();
        let metrics = &self.metrics.range;
        metrics.calls.fetch_add(1, Ordering::Relaxed);
        metrics.active.fetch_add(1, Ordering::Relaxed);
        metrics
            .requested_bytes
            .fetch_add(requested_bytes, Ordering::Relaxed);
        if requested_bytes == 0 {
            metrics.empty_calls.fetch_add(1, Ordering::Relaxed);
        }
        RangeReadObservation {
            metrics: self.metrics.clone(),
            started,
            finished: false,
        }
    }

    pub fn waiter(&self) -> FileReadWaiter {
        FileReadWaiter {
            metrics: self.metrics.clone(),
            completed_at: self.completed_at.clone(),
            resumed: false,
        }
    }

    pub fn start(mut self) -> Self {
        let started = Instant::now();
        if self.blocking {
            add_time(
                &self.metrics.queue_ns,
                started.duration_since(self.submitted),
            );
        }
        self.metrics.started.fetch_add(1, Ordering::Relaxed);
        self.started = Some(Instant::now());
        self
    }

    pub fn finish(mut self, success: bool) {
        self.finish_at(Instant::now(), !success, false);
    }

    fn finish_at(&mut self, completed: Instant, error: bool, panic: bool) {
        if let Some(started) = self.started {
            add_time(&self.metrics.service_ns, completed.duration_since(started));
            self.metrics.finished.fetch_add(1, Ordering::Relaxed);
            if error {
                self.metrics.errors.fetch_add(1, Ordering::Relaxed);
            }
            if panic {
                self.metrics.panics.fetch_add(1, Ordering::Relaxed);
            }
        } else {
            self.metrics.abandoned.fetch_add(1, Ordering::Relaxed);
        }
        *self
            .completed_at
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(completed);
        self.metrics.active.fetch_sub(1, Ordering::Relaxed);
        self.finished = true;
    }
}

impl Drop for FileReadWorker {
    fn drop(&mut self) {
        if !self.finished {
            self.finish_at(Instant::now(), false, self.started.is_some());
        }
    }
}

pub struct FileReadWaiter {
    metrics: Arc<FileReadMetrics>,
    completed_at: Arc<Mutex<Option<Instant>>>,
    resumed: bool,
}

impl FileReadWaiter {
    pub fn resume(mut self) {
        let resumed = Instant::now();
        if let Some(completed) = *self
            .completed_at
            .lock()
            .unwrap_or_else(|error| error.into_inner())
        {
            add_time(
                &self.metrics.completion_ns,
                resumed.duration_since(completed),
            );
        }
        self.metrics.resumed.fetch_add(1, Ordering::Relaxed);
        self.resumed = true;
    }
}

impl Drop for FileReadWaiter {
    fn drop(&mut self) {
        if !self.resumed {
            self.metrics.cancellations.fetch_add(1, Ordering::Relaxed);
        }
    }
}

pub enum RangeReadPhase {
    Allocation,
    ZeroFill,
    Read,
    Conversion,
}

pub struct RangeReadObservation {
    metrics: Arc<FileReadMetrics>,
    started: Instant,
    finished: bool,
}

impl RangeReadObservation {
    pub fn phase(&self, phase: RangeReadPhase) -> ReadPhaseGuard<'_> {
        let metrics = match phase {
            RangeReadPhase::Allocation => &self.metrics.range.allocation,
            RangeReadPhase::ZeroFill => &self.metrics.range.zero_fill,
            RangeReadPhase::Read => &self.metrics.range.read,
            RangeReadPhase::Conversion => &self.metrics.range.conversion,
        };
        let started = Instant::now();
        metrics.calls.fetch_add(1, Ordering::Relaxed);
        ReadPhaseGuard { metrics, started }
    }

    pub fn read_result(&self, result: &std::io::Result<usize>) {
        match result {
            Ok(0) => &self.metrics.range.read_eof,
            Ok(bytes) => {
                self.metrics
                    .range
                    .read_bytes
                    .fetch_add(*bytes as u64, Ordering::Relaxed);
                return;
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {
                &self.metrics.range.read_interrupted
            }
            Err(_) => &self.metrics.range.read_errors,
        }
        .fetch_add(1, Ordering::Relaxed);
    }

    pub fn unsupported(&self) {
        self.metrics
            .range
            .unsupported_calls
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn finish(mut self) {
        self.complete(false, false);
    }

    fn complete(&mut self, error: bool, panic: bool) {
        let completed = Instant::now();
        let metrics = &self.metrics.range;
        add_time(&metrics.total_ns, completed.duration_since(self.started));
        metrics.finished.fetch_add(1, Ordering::Relaxed);
        if error {
            metrics.errors.fetch_add(1, Ordering::Relaxed);
        }
        if panic {
            metrics.panics.fetch_add(1, Ordering::Relaxed);
        }
        metrics.active.fetch_sub(1, Ordering::Relaxed);
        self.finished = true;
    }
}

impl Drop for RangeReadObservation {
    fn drop(&mut self) {
        if !self.finished {
            let panic = std::thread::panicking();
            self.complete(!panic, panic);
        }
    }
}

pub struct ReadPhaseGuard<'a> {
    metrics: &'a ReadPhaseMetrics,
    started: Instant,
}

impl Drop for ReadPhaseGuard<'_> {
    fn drop(&mut self) {
        add_time(&self.metrics.ns, self.started.elapsed());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completed_worker_and_waiter_reconcile() {
        let metrics = Arc::new(FileReadMetrics::default());
        let worker = FileReadObservation(metrics.clone()).submit(true);
        let waiter = worker.waiter();
        assert_eq!(metrics.snapshot().active, 1);
        assert_eq!(metrics.snapshot().started, 0);
        worker.start().finish(true);
        waiter.resume();
        let snapshot = metrics.snapshot();
        assert_eq!(
            (
                snapshot.calls,
                snapshot.started,
                snapshot.finished,
                snapshot.resumed
            ),
            (1, 1, 1, 1)
        );
        assert_eq!(
            (
                snapshot.active,
                snapshot.errors,
                snapshot.panics,
                snapshot.cancellations
            ),
            (0, 0, 0, 0)
        );
    }

    #[test]
    fn cancelled_waiter_does_not_hide_unfinished_worker() {
        let metrics = Arc::new(FileReadMetrics::default());
        let worker = FileReadObservation(metrics.clone()).submit(true);
        drop(worker.waiter());
        assert_eq!(
            (metrics.snapshot().active, metrics.snapshot().cancellations),
            (1, 1)
        );
        worker.start().finish(false);
        assert_eq!(
            (metrics.snapshot().active, metrics.snapshot().errors),
            (0, 1)
        );
    }

    #[test]
    fn abandoned_and_panicked_workers_release_active() {
        let metrics = Arc::new(FileReadMetrics::default());
        drop(FileReadObservation(metrics.clone()).submit(true));
        drop(FileReadObservation(metrics.clone()).submit(true).start());
        let snapshot = metrics.snapshot();
        assert_eq!(
            (snapshot.calls, snapshot.started, snapshot.finished),
            (2, 1, 1)
        );
        assert_eq!(
            (snapshot.abandoned, snapshot.panics, snapshot.active),
            (1, 1, 0)
        );
    }

    #[test]
    fn direct_reads_do_not_report_blocking_queue_time() {
        let metrics = Arc::new(FileReadMetrics::default());
        let worker = FileReadObservation(metrics.clone()).submit(false);
        let waiter = worker.waiter();
        worker.start().finish(true);
        waiter.resume();
        assert_eq!(
            (metrics.snapshot().direct_calls, metrics.snapshot().queue_ns),
            (1, 0)
        );
    }

    #[test]
    fn range_stages_reconcile_inside_actual_worker() {
        let metrics = Arc::new(FileReadMetrics::default());
        let worker = FileReadObservation(metrics.clone()).submit(true).start();
        let range = worker.range_read(5);
        for phase in [
            RangeReadPhase::Allocation,
            RangeReadPhase::ZeroFill,
            RangeReadPhase::Read,
            RangeReadPhase::Conversion,
        ] {
            drop(range.phase(phase));
        }
        range.read_result(&Ok(5));
        range.finish();
        worker.finish(true);
        let snapshot = metrics.snapshot();
        let range = snapshot.range;
        assert_eq!(
            (
                range.calls,
                range.finished,
                range.active,
                range.errors,
                range.panics
            ),
            (1, 1, 0, 0, 0)
        );
        assert_eq!(
            (range.requested_bytes, range.read_bytes, range.read.calls),
            (5, 5, 1)
        );
        assert_eq!(
            (
                range.allocation.calls,
                range.zero_fill.calls,
                range.conversion.calls
            ),
            (1, 1, 1)
        );
        assert!(
            range.allocation.ns + range.zero_fill.ns + range.read.ns + range.conversion.ns
                <= range.total_ns
        );
        assert!(range.total_ns <= snapshot.service_ns);
    }

    #[test]
    fn range_error_and_read_outcomes_remain_distinct() {
        let metrics = Arc::new(FileReadMetrics::default());
        let worker = FileReadObservation(metrics.clone()).submit(true).start();
        let range = worker.range_read(5);
        for result in [
            Ok(2),
            Ok(0),
            Err(std::io::ErrorKind::Interrupted.into()),
            Err(std::io::ErrorKind::PermissionDenied.into()),
        ] {
            let phase = range.phase(RangeReadPhase::Read);
            drop(phase);
            range.read_result(&result);
        }
        drop(range);
        worker.finish(false);
        let range = metrics.snapshot().range;
        assert_eq!(
            (
                range.read.calls,
                range.read_bytes,
                range.read_eof,
                range.read_interrupted,
                range.read_errors
            ),
            (4, 2, 1, 1, 1)
        );
        assert_eq!(
            (
                range.calls,
                range.finished,
                range.active,
                range.errors,
                range.panics
            ),
            (1, 1, 0, 1, 0)
        );
        assert_eq!(range.conversion.calls, 0);
    }

    #[test]
    fn range_unwind_and_empty_unsupported_read_release_active() {
        let metrics = Arc::new(FileReadMetrics::default());
        let observed = metrics.clone();
        let _ = std::panic::catch_unwind(move || {
            let worker = FileReadObservation(observed).submit(true).start();
            let range = worker.range_read(5);
            let _phase = range.phase(RangeReadPhase::Read);
            panic!("range probe");
        });
        let worker = FileReadObservation(metrics.clone()).submit(true).start();
        let range = worker.range_read(0);
        range.unsupported();
        range.finish();
        worker.finish(true);
        let range = metrics.snapshot().range;
        assert_eq!(
            (
                range.calls,
                range.finished,
                range.active,
                range.errors,
                range.panics
            ),
            (2, 2, 0, 0, 1)
        );
        assert_eq!((range.empty_calls, range.unsupported_calls), (1, 1));
    }
}
