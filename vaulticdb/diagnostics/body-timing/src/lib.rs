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
}
