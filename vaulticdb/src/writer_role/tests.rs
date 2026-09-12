#[cfg(test)]
mod tests {
    //! Writer-role state machine tests.

    use super::*;

    #[test]
    fn demotion_waits_for_writes_and_transactions() {
        let start = Instant::now();
        let mut state = WriterRoleState::read_write(7, start, Duration::ZERO);
        state.admit_write().unwrap();
        state.transaction_opened().unwrap();
        state.begin_demotion(start, "operator", true).unwrap();
        assert_eq!(
            state.complete_demotion(start),
            Err(RoleError::Transitioning)
        );
        state.finish_write();
        state.transaction_closed();
        state.complete_demotion(start).unwrap();
        assert_eq!(state.status().role, WriterRole::ReadOnly);
        assert!(state.status().promotion_safe);
    }

    #[test]
    fn transaction_accounting_tracks_every_consumption_outcome() {
        let now = Instant::now();
        let mut state = WriterRoleState::read_write(7, now, Duration::ZERO);

        state.transaction_opened().unwrap();
        state.transaction_closed();
        assert_eq!(state.status().active_transactions, 0, "successful commit");

        state.transaction_opened().unwrap();
        state.transaction_closed();
        assert_eq!(state.status().active_transactions, 0, "consumed commit failure");

        state.transaction_opened().unwrap();
        assert_eq!(state.status().active_transactions, 1, "pre-consumption failure");
        state.transaction_closed();

        state.transaction_opened().unwrap();
        state.transaction_closed();
        assert_eq!(state.status().active_transactions, 0, "rollback");

        state.transaction_opened().unwrap();
        state.transaction_opened().unwrap();
        state.transaction_closed();
        state.transaction_closed();
        assert_eq!(state.status().active_transactions, 0, "expiry reconciliation");
    }

    #[test]
    fn promotion_requires_a_fresh_epoch() {
        let start = Instant::now();
        let mut state = WriterRoleState::read_write(7, start, Duration::ZERO);
        state.begin_demotion(start, "idle", false).unwrap();
        state.complete_demotion(start).unwrap();
        state.begin_promotion(start, "write requested").unwrap();
        assert_eq!(
            state.complete_promotion(7, start),
            Err(RoleError::StaleEpoch)
        );
        assert_eq!(state.status().role, WriterRole::Fenced);
    }

    #[test]
    fn transitions_reject_new_mutations() {
        let start = Instant::now();
        let mut state = WriterRoleState::read_write(1, start, Duration::ZERO);
        state.begin_demotion(start, "operator", true).unwrap();
        assert_eq!(state.admit_write(), Err(RoleError::Transitioning));
        state.fail_demotion(start);
        assert_eq!(
            state.admit_write(),
            Err(RoleError::Fenced { observed_epoch: 1 })
        );
    }

    #[test]
    fn cancelled_demotion_restores_the_writer() {
        let start = Instant::now();
        let mut state = WriterRoleState::read_write(7, start, Duration::ZERO);
        state.begin_demotion(start, "operator", true).unwrap();
        state.cancel_demotion(start, "timed out");
        assert_eq!(state.status().role, WriterRole::ReadWrite);
        assert_eq!(state.status().current_epoch, 7);
        assert!(state.admit_write().is_ok());
    }

    #[test]
    fn failed_promotion_can_return_to_a_safe_reader() {
        let start = Instant::now();
        let mut state = WriterRoleState::read_only(7, start, Duration::ZERO);
        state.begin_promotion(start, "operator").unwrap();
        state.fail_promotion_to_reader(8, start);
        assert_eq!(state.status().role, WriterRole::ReadOnly);
        assert_eq!(state.status().current_epoch, 0);
        assert_eq!(state.status().observed_epoch, 8);
        assert!(state.status().promotion_safe);
    }

    #[test]
    fn fenced_writer_recovers_only_on_a_new_epoch() {
        let start = Instant::now();
        let mut state = WriterRoleState::read_write(7, start, Duration::ZERO);
        state.fence(7, start, "reconciliation pending");
        assert_eq!(
            state.recover_fenced_writer(7, start),
            Err(RoleError::StaleEpoch)
        );
        state.recover_fenced_writer(8, start).unwrap();
        assert_eq!(state.status().role, WriterRole::ReadWrite);
        assert_eq!(state.status().current_epoch, 8);
    }
}
