//! Metadata writer-role state machine and fencing transitions.

use std::time::{Duration, Instant};

use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WriterRole {
    ReadOnly,
    Promoting,
    ReadWrite,
    Demoting,
    Fenced,
}

#[derive(Clone, Debug)]
pub struct WriterStatus {
    pub role: WriterRole,
    pub current_epoch: u64,
    pub observed_epoch: u64,
    pub active_write_intents: u64,
    pub active_transactions: u64,
    pub transition_reason: String,
    pub transition_started: Instant,
    pub writer_since: Option<Instant>,
    pub promotion_safe: bool,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum RoleError {
    #[error("vaulticdb is not the metadata writer")]
    NotWriter,
    #[error("vaulticdb writer role is transitioning")]
    Transitioning,
    #[error("vaulticdb is fenced by epoch {observed_epoch}")]
    Fenced { observed_epoch: u64 },
    #[error("writer minimum tenure has not elapsed")]
    MinimumTenure,
    #[error("writer quiescence deadline expired")]
    QuiescenceTimeout,
    #[error("writer epoch must increase monotonically")]
    StaleEpoch,
}

pub struct WriterRoleState {
    status: WriterStatus,
    minimum_tenure: Duration,
}

impl WriterRoleState {
    pub fn read_write(epoch: u64, now: Instant, minimum_tenure: Duration) -> Self {
        Self {
            status: WriterStatus {
                role: WriterRole::ReadWrite,
                current_epoch: epoch,
                observed_epoch: epoch,
                active_write_intents: 0,
                active_transactions: 0,
                transition_reason: "startup".to_owned(),
                transition_started: now,
                writer_since: Some(now),
                promotion_safe: false,
            },
            minimum_tenure,
        }
    }

    pub fn read_only(observed_epoch: u64, now: Instant, minimum_tenure: Duration) -> Self {
        Self {
            status: WriterStatus {
                role: WriterRole::ReadOnly,
                current_epoch: 0,
                observed_epoch,
                active_write_intents: 0,
                active_transactions: 0,
                transition_reason: "writer epoch already claimed".to_owned(),
                transition_started: now,
                writer_since: None,
                promotion_safe: true,
            },
            minimum_tenure,
        }
    }

    pub fn status(&self) -> WriterStatus {
        self.status.clone()
    }

    pub fn admit_write(&mut self) -> Result<(), RoleError> {
        match self.status.role {
            WriterRole::ReadWrite => {
                self.status.active_write_intents += 1;
                Ok(())
            }
            WriterRole::ReadOnly => Err(RoleError::NotWriter),
            WriterRole::Promoting | WriterRole::Demoting => Err(RoleError::Transitioning),
            WriterRole::Fenced => Err(RoleError::Fenced {
                observed_epoch: self.status.observed_epoch,
            }),
        }
    }

    pub fn finish_write(&mut self) {
        self.status.active_write_intents = self.status.active_write_intents.saturating_sub(1);
    }

    pub fn transaction_opened(&mut self) -> Result<(), RoleError> {
        self.admit_write()?;
        self.finish_write();
        self.status.active_transactions += 1;
        Ok(())
    }

    pub fn transaction_closed(&mut self) {
        self.status.active_transactions = self.status.active_transactions.saturating_sub(1);
    }

    pub fn begin_demotion(
        &mut self,
        now: Instant,
        reason: impl Into<String>,
        force: bool,
    ) -> Result<(), RoleError> {
        if self.status.role != WriterRole::ReadWrite {
            return self.admit_write().map(|_| self.finish_write());
        }
        if !force
            && self
                .status
                .writer_since
                .is_some_and(|since| now.duration_since(since) < self.minimum_tenure)
        {
            return Err(RoleError::MinimumTenure);
        }
        self.status.role = WriterRole::Demoting;
        self.status.transition_reason = reason.into();
        self.status.transition_started = now;
        self.status.promotion_safe = false;
        Ok(())
    }

    pub fn complete_demotion(&mut self, now: Instant) -> Result<(), RoleError> {
        if self.status.role != WriterRole::Demoting {
            return Err(RoleError::Transitioning);
        }
        if self.status.active_write_intents != 0 || self.status.active_transactions != 0 {
            return Err(RoleError::Transitioning);
        }
        self.status.role = WriterRole::ReadOnly;
        self.status.transition_started = now;
        self.status.writer_since = None;
        self.status.promotion_safe = true;
        Ok(())
    }

    pub fn fail_demotion(&mut self, now: Instant) {
        self.status.role = WriterRole::Fenced;
        self.status.transition_reason = "writer close or quiescence failed".to_owned();
        self.status.transition_started = now;
        self.status.promotion_safe = false;
    }

    pub fn begin_promotion(
        &mut self,
        now: Instant,
        reason: impl Into<String>,
    ) -> Result<(), RoleError> {
        if self.status.role != WriterRole::ReadOnly || !self.status.promotion_safe {
            return Err(RoleError::Transitioning);
        }
        self.status.role = WriterRole::Promoting;
        self.status.transition_reason = reason.into();
        self.status.transition_started = now;
        self.status.promotion_safe = false;
        Ok(())
    }

    pub fn complete_promotion(&mut self, epoch: u64, now: Instant) -> Result<(), RoleError> {
        if self.status.role != WriterRole::Promoting {
            return Err(RoleError::Transitioning);
        }
        if epoch <= self.status.current_epoch || epoch < self.status.observed_epoch {
            self.fence(
                self.status.observed_epoch.max(epoch),
                now,
                "stale promotion epoch",
            );
            return Err(RoleError::StaleEpoch);
        }
        self.status.role = WriterRole::ReadWrite;
        self.status.current_epoch = epoch;
        self.status.observed_epoch = epoch;
        self.status.transition_started = now;
        self.status.writer_since = Some(now);
        Ok(())
    }

    pub fn fence(&mut self, winning_epoch: u64, now: Instant, reason: impl Into<String>) {
        self.status.role = WriterRole::Fenced;
        self.status.observed_epoch = self.status.observed_epoch.max(winning_epoch);
        self.status.transition_reason = reason.into();
        self.status.transition_started = now;
        self.status.promotion_safe = false;
    }
}

include!("writer_role/tests.rs");
