//! Whole-daemon lifecycle state and validated transitions.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DaemonPhase {
    LoadingStorage,
    ReadOnly,
    ReadWrite,
    Promoting,
    Demoting,
    Fenced,
    Draining,
    Failed,
    Stopping,
}

impl DaemonPhase {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::LoadingStorage => "loading_storage",
            Self::ReadOnly => "read_only",
            Self::ReadWrite => "read_write",
            Self::Promoting => "promoting",
            Self::Demoting => "demoting",
            Self::Fenced => "fenced",
            Self::Draining => "draining",
            Self::Failed => "failed",
            Self::Stopping => "stopping",
        }
    }

    pub(crate) const fn ready(self) -> bool {
        matches!(self, Self::ReadOnly | Self::ReadWrite | Self::Fenced)
    }

    const fn allows(self, next: Self) -> bool {
        if self as u8 == next as u8 {
            return true;
        }
        match self {
            Self::LoadingStorage => matches!(
                next,
                Self::ReadOnly | Self::ReadWrite | Self::Draining | Self::Failed | Self::Stopping
            ),
            Self::ReadOnly => matches!(
                next,
                Self::Promoting | Self::Draining | Self::Failed | Self::Stopping
            ),
            Self::ReadWrite => matches!(
                next,
                Self::Demoting | Self::Fenced | Self::Draining | Self::Failed | Self::Stopping
            ),
            Self::Promoting => matches!(
                next,
                Self::ReadOnly
                    | Self::ReadWrite
                    | Self::Fenced
                    | Self::Draining
                    | Self::Failed
                    | Self::Stopping
            ),
            Self::Demoting => matches!(
                next,
                Self::ReadOnly
                    | Self::ReadWrite
                    | Self::Fenced
                    | Self::Draining
                    | Self::Failed
                    | Self::Stopping
            ),
            Self::Fenced => matches!(
                next,
                Self::ReadWrite | Self::Promoting | Self::Draining | Self::Failed | Self::Stopping
            ),
            Self::Draining => matches!(next, Self::Failed | Self::Stopping),
            Self::Failed => matches!(next, Self::Stopping),
            Self::Stopping => false,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DaemonLifecycleStatus {
    pub(crate) phase: DaemonPhase,
    pub(crate) detail: String,
    pub(crate) changed_at_unix_ms: i64,
}

#[derive(Debug)]
pub(crate) struct DaemonLifecycle {
    status: DaemonLifecycleStatus,
}

impl DaemonLifecycle {
    pub(crate) fn loading(now_unix_ms: i64) -> Self {
        Self {
            status: DaemonLifecycleStatus {
                phase: DaemonPhase::LoadingStorage,
                detail: "opening SlateDB".to_owned(),
                changed_at_unix_ms: now_unix_ms,
            },
        }
    }

    pub(crate) fn status(&self) -> DaemonLifecycleStatus {
        self.status.clone()
    }

    pub(crate) fn transition(
        &mut self,
        phase: DaemonPhase,
        detail: impl Into<String>,
        now_unix_ms: i64,
    ) -> Result<(), String> {
        if !self.status.phase.allows(phase) {
            return Err(format!(
                "invalid daemon lifecycle transition from {} to {}",
                self.status.phase.as_str(),
                phase.as_str()
            ));
        }
        self.status = DaemonLifecycleStatus {
            phase,
            detail: detail.into(),
            changed_at_unix_ms: now_unix_ms,
        };
        Ok(())
    }

    pub(crate) fn finish_loading(
        &mut self,
        phase: DaemonPhase,
        detail: impl Into<String>,
        now_unix_ms: i64,
    ) -> Result<bool, String> {
        match self.status.phase {
            DaemonPhase::LoadingStorage => {
                self.transition(phase, detail, now_unix_ms)?;
                Ok(true)
            }
            DaemonPhase::Draining | DaemonPhase::Stopping => Ok(false),
            current => Err(format!(
                "cannot finish loading while daemon is {}",
                current.as_str()
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifecycle_covers_writer_and_process_states() {
        let mut lifecycle = DaemonLifecycle::loading(1);
        assert_eq!(lifecycle.status().phase, DaemonPhase::LoadingStorage);
        lifecycle
            .transition(DaemonPhase::ReadOnly, "writer already claimed", 2)
            .unwrap();
        lifecycle
            .transition(DaemonPhase::Promoting, "operator request", 3)
            .unwrap();
        lifecycle
            .transition(DaemonPhase::ReadWrite, "promotion complete", 4)
            .unwrap();
        lifecycle
            .transition(DaemonPhase::Demoting, "idle", 5)
            .unwrap();
        lifecycle
            .transition(DaemonPhase::Fenced, "claim changed", 6)
            .unwrap();
        lifecycle
            .transition(DaemonPhase::Draining, "shutdown requested", 7)
            .unwrap();
        lifecycle
            .transition(DaemonPhase::Stopping, "server stopped", 8)
            .unwrap();
        assert_eq!(lifecycle.status().phase, DaemonPhase::Stopping);
    }

    #[test]
    fn lifecycle_rejects_impossible_transitions() {
        let mut lifecycle = DaemonLifecycle::loading(1);
        let error = lifecycle
            .transition(DaemonPhase::Promoting, "too early", 2)
            .unwrap_err();
        assert!(error.contains("loading_storage to promoting"));
        lifecycle
            .transition(DaemonPhase::Failed, "open failed", 3)
            .unwrap();
        assert!(!lifecycle.status().phase.ready());
    }

    #[test]
    fn loading_completion_preserves_concurrent_drain() {
        let mut lifecycle = DaemonLifecycle::loading(1);
        lifecycle
            .transition(DaemonPhase::Draining, "shutdown", 2)
            .unwrap();
        assert!(!lifecycle
            .finish_loading(DaemonPhase::ReadWrite, "ready", 3)
            .unwrap());
        assert_eq!(lifecycle.status().phase, DaemonPhase::Draining);
        assert!(DaemonPhase::Fenced.ready());
    }
}
