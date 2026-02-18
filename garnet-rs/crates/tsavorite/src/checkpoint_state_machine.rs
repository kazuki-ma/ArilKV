//! Checkpoint state machine used by checkpoint/AOF orchestration.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointMode {
    FoldOver,
    Snapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointPhase {
    Rest,
    Prepare,
    InProgress,
    WaitFlush,
    PersistenceCallback,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointState {
    Rest,
    Prepare { token: u64, mode: CheckpointMode },
    InProgress { token: u64, mode: CheckpointMode },
    WaitFlush { token: u64, mode: CheckpointMode },
    PersistenceCallback { token: u64, mode: CheckpointMode },
}

impl CheckpointState {
    pub fn phase(self) -> CheckpointPhase {
        match self {
            Self::Rest => CheckpointPhase::Rest,
            Self::Prepare { .. } => CheckpointPhase::Prepare,
            Self::InProgress { .. } => CheckpointPhase::InProgress,
            Self::WaitFlush { .. } => CheckpointPhase::WaitFlush,
            Self::PersistenceCallback { .. } => CheckpointPhase::PersistenceCallback,
        }
    }

    pub fn token(self) -> Option<u64> {
        match self {
            Self::Rest => None,
            Self::Prepare { token, .. }
            | Self::InProgress { token, .. }
            | Self::WaitFlush { token, .. }
            | Self::PersistenceCallback { token, .. } => Some(token),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointTransitionError {
    Busy {
        state: CheckpointState,
    },
    InvalidState {
        expected: CheckpointPhase,
        actual: CheckpointState,
    },
    TokenMismatch {
        expected: u64,
        actual: u64,
    },
}

impl core::fmt::Display for CheckpointTransitionError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Busy { state } => write!(f, "checkpoint already active in state {:?}", state),
            Self::InvalidState { expected, actual } => write!(
                f,
                "invalid checkpoint state transition: expected {:?}, actual {:?}",
                expected,
                actual.phase()
            ),
            Self::TokenMismatch { expected, actual } => {
                write!(
                    f,
                    "checkpoint token mismatch: expected {}, got {}",
                    expected, actual
                )
            }
        }
    }
}

impl std::error::Error for CheckpointTransitionError {}

#[derive(Debug, Clone)]
pub struct CheckpointStateMachine {
    state: CheckpointState,
    next_token: u64,
}

impl Default for CheckpointStateMachine {
    fn default() -> Self {
        Self::new()
    }
}

impl CheckpointStateMachine {
    pub fn new() -> Self {
        Self {
            state: CheckpointState::Rest,
            next_token: 1,
        }
    }

    pub fn state(&self) -> CheckpointState {
        self.state
    }

    pub fn begin(&mut self, mode: CheckpointMode) -> Result<u64, CheckpointTransitionError> {
        if self.state != CheckpointState::Rest {
            return Err(CheckpointTransitionError::Busy { state: self.state });
        }

        let token = self.alloc_token();
        self.state = CheckpointState::Prepare { token, mode };
        Ok(token)
    }

    pub fn mark_in_progress(&mut self, token: u64) -> Result<(), CheckpointTransitionError> {
        let mode = self.validate_token_state(CheckpointPhase::Prepare, token)?;
        self.state = CheckpointState::InProgress { token, mode };
        Ok(())
    }

    pub fn mark_wait_flush(&mut self, token: u64) -> Result<(), CheckpointTransitionError> {
        let mode = self.validate_token_state(CheckpointPhase::InProgress, token)?;
        self.state = CheckpointState::WaitFlush { token, mode };
        Ok(())
    }

    pub fn mark_persistence_callback(
        &mut self,
        token: u64,
    ) -> Result<(), CheckpointTransitionError> {
        let mode = self.validate_token_state(CheckpointPhase::WaitFlush, token)?;
        self.state = CheckpointState::PersistenceCallback { token, mode };
        Ok(())
    }

    pub fn complete(&mut self, token: u64) -> Result<(), CheckpointTransitionError> {
        self.validate_token_state(CheckpointPhase::PersistenceCallback, token)?;
        self.state = CheckpointState::Rest;
        Ok(())
    }

    pub fn abort(&mut self, token: u64) -> Result<(), CheckpointTransitionError> {
        if self.state == CheckpointState::Rest {
            return Err(CheckpointTransitionError::InvalidState {
                expected: CheckpointPhase::Prepare,
                actual: self.state,
            });
        }

        let expected = self.state.token().expect("non-rest state has token");
        if expected != token {
            return Err(CheckpointTransitionError::TokenMismatch {
                expected,
                actual: token,
            });
        }

        self.state = CheckpointState::Rest;
        Ok(())
    }

    fn alloc_token(&mut self) -> u64 {
        let token = self.next_token;
        self.next_token = self.next_token.wrapping_add(1);
        if self.next_token == 0 {
            self.next_token = 1;
        }
        token
    }

    fn validate_token_state(
        &self,
        expected_phase: CheckpointPhase,
        token: u64,
    ) -> Result<CheckpointMode, CheckpointTransitionError> {
        match self.state {
            CheckpointState::Prepare {
                token: expected,
                mode,
            } if expected_phase == CheckpointPhase::Prepare => {
                if expected == token {
                    Ok(mode)
                } else {
                    Err(CheckpointTransitionError::TokenMismatch {
                        expected,
                        actual: token,
                    })
                }
            }
            CheckpointState::InProgress {
                token: expected,
                mode,
            } if expected_phase == CheckpointPhase::InProgress => {
                if expected == token {
                    Ok(mode)
                } else {
                    Err(CheckpointTransitionError::TokenMismatch {
                        expected,
                        actual: token,
                    })
                }
            }
            CheckpointState::WaitFlush {
                token: expected,
                mode,
            } if expected_phase == CheckpointPhase::WaitFlush => {
                if expected == token {
                    Ok(mode)
                } else {
                    Err(CheckpointTransitionError::TokenMismatch {
                        expected,
                        actual: token,
                    })
                }
            }
            CheckpointState::PersistenceCallback {
                token: expected,
                mode,
            } if expected_phase == CheckpointPhase::PersistenceCallback => {
                if expected == token {
                    Ok(mode)
                } else {
                    Err(CheckpointTransitionError::TokenMismatch {
                        expected,
                        actual: token,
                    })
                }
            }
            _ => Err(CheckpointTransitionError::InvalidState {
                expected: expected_phase,
                actual: self.state,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn progresses_through_all_checkpoint_phases() {
        let mut machine = CheckpointStateMachine::new();
        let token = machine.begin(CheckpointMode::FoldOver).unwrap();
        assert_eq!(
            machine.state(),
            CheckpointState::Prepare {
                token,
                mode: CheckpointMode::FoldOver
            }
        );

        machine.mark_in_progress(token).unwrap();
        assert_eq!(
            machine.state(),
            CheckpointState::InProgress {
                token,
                mode: CheckpointMode::FoldOver
            }
        );

        machine.mark_wait_flush(token).unwrap();
        assert_eq!(
            machine.state(),
            CheckpointState::WaitFlush {
                token,
                mode: CheckpointMode::FoldOver
            }
        );

        machine.mark_persistence_callback(token).unwrap();
        assert_eq!(
            machine.state(),
            CheckpointState::PersistenceCallback {
                token,
                mode: CheckpointMode::FoldOver
            }
        );

        machine.complete(token).unwrap();
        assert_eq!(machine.state(), CheckpointState::Rest);
    }

    #[test]
    fn begin_fails_when_checkpoint_is_already_active() {
        let mut machine = CheckpointStateMachine::new();
        let token = machine.begin(CheckpointMode::FoldOver).unwrap();
        assert!(matches!(
            machine.begin(CheckpointMode::Snapshot),
            Err(CheckpointTransitionError::Busy {
                state: CheckpointState::Prepare { token: active, .. }
            }) if active == token
        ));
    }

    #[test]
    fn transitions_validate_checkpoint_token() {
        let mut machine = CheckpointStateMachine::new();
        let token = machine.begin(CheckpointMode::Snapshot).unwrap();
        assert!(matches!(
            machine.mark_in_progress(token + 1),
            Err(CheckpointTransitionError::TokenMismatch {
                expected,
                actual
            }) if expected == token && actual == token + 1
        ));
    }

    #[test]
    fn wrong_state_transition_is_rejected() {
        let mut machine = CheckpointStateMachine::new();
        let token = machine.begin(CheckpointMode::FoldOver).unwrap();
        assert!(matches!(
            machine.mark_wait_flush(token),
            Err(CheckpointTransitionError::InvalidState {
                expected: CheckpointPhase::InProgress,
                actual: CheckpointState::Prepare { token: active, .. }
            }) if active == token
        ));
    }

    #[test]
    fn abort_resets_active_checkpoint_to_rest() {
        let mut machine = CheckpointStateMachine::new();
        let token = machine.begin(CheckpointMode::FoldOver).unwrap();
        machine.mark_in_progress(token).unwrap();
        machine.abort(token).unwrap();
        assert_eq!(machine.state(), CheckpointState::Rest);
    }
}
