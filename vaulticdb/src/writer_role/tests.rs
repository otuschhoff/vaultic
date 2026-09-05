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
}
