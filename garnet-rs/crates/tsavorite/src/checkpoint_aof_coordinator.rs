//! Coordination between checkpoint lifecycle and AOF replay windows.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryPlan {
    pub durable_checkpoint_id: u64,
    pub replay_from_aof_offset: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct CheckpointToken(u64);

impl CheckpointToken {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointAofCoordinatorError {
    Busy {
        active_checkpoint_id: u64,
    },
    TokenMismatch {
        expected: CheckpointToken,
        actual: CheckpointToken,
    },
    NoActiveCheckpoint,
}

impl core::fmt::Display for CheckpointAofCoordinatorError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Busy {
                active_checkpoint_id,
            } => write!(
                f,
                "checkpoint {} is still active; cannot begin another",
                active_checkpoint_id
            ),
            Self::TokenMismatch { expected, actual } => write!(
                f,
                "checkpoint token mismatch: expected {}, got {}",
                expected.0, actual.0
            ),
            Self::NoActiveCheckpoint => write!(f, "no active checkpoint"),
        }
    }
}

impl std::error::Error for CheckpointAofCoordinatorError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ActiveCheckpoint {
    checkpoint_id: u64,
    checkpoint_token: CheckpointToken,
    aof_begin_offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointAofCoordinator {
    durable_checkpoint_id: u64,
    replay_from_aof_offset: u64,
    active: Option<ActiveCheckpoint>,
    next_checkpoint_id: u64,
}

impl Default for CheckpointAofCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

impl CheckpointAofCoordinator {
    pub fn new() -> Self {
        Self {
            durable_checkpoint_id: 0,
            replay_from_aof_offset: 0,
            active: None,
            next_checkpoint_id: 1,
        }
    }

    pub fn begin_checkpoint(
        &mut self,
        checkpoint_token: CheckpointToken,
        aof_begin_offset: u64,
    ) -> Result<u64, CheckpointAofCoordinatorError> {
        if let Some(active) = self.active {
            return Err(CheckpointAofCoordinatorError::Busy {
                active_checkpoint_id: active.checkpoint_id,
            });
        }

        let checkpoint_id = self.next_checkpoint_id;
        self.next_checkpoint_id = self.next_checkpoint_id.wrapping_add(1).max(1);
        self.active = Some(ActiveCheckpoint {
            checkpoint_id,
            checkpoint_token,
            aof_begin_offset,
        });
        Ok(checkpoint_id)
    }

    pub fn complete_checkpoint(
        &mut self,
        checkpoint_token: CheckpointToken,
        aof_tail_offset: u64,
    ) -> Result<RecoveryPlan, CheckpointAofCoordinatorError> {
        let active = self
            .active
            .ok_or(CheckpointAofCoordinatorError::NoActiveCheckpoint)?;
        if active.checkpoint_token != checkpoint_token {
            return Err(CheckpointAofCoordinatorError::TokenMismatch {
                expected: active.checkpoint_token,
                actual: checkpoint_token,
            });
        }

        self.durable_checkpoint_id = active.checkpoint_id;
        self.replay_from_aof_offset = aof_tail_offset;
        self.active = None;
        Ok(self.recovery_plan())
    }

    pub fn abort_checkpoint(
        &mut self,
        checkpoint_token: CheckpointToken,
    ) -> Result<(), CheckpointAofCoordinatorError> {
        let active = self
            .active
            .ok_or(CheckpointAofCoordinatorError::NoActiveCheckpoint)?;
        if active.checkpoint_token != checkpoint_token {
            return Err(CheckpointAofCoordinatorError::TokenMismatch {
                expected: active.checkpoint_token,
                actual: checkpoint_token,
            });
        }
        self.active = None;
        Ok(())
    }

    pub fn active_checkpoint_id(&self) -> Option<u64> {
        self.active.map(|checkpoint| checkpoint.checkpoint_id)
    }

    pub fn active_aof_begin_offset(&self) -> Option<u64> {
        self.active.map(|checkpoint| checkpoint.aof_begin_offset)
    }

    pub fn recovery_plan(&self) -> RecoveryPlan {
        RecoveryPlan {
            durable_checkpoint_id: self.durable_checkpoint_id,
            replay_from_aof_offset: self.replay_from_aof_offset,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completed_checkpoint_updates_recovery_plan() {
        let mut coordinator = CheckpointAofCoordinator::new();
        let checkpoint_id = coordinator
            .begin_checkpoint(CheckpointToken::new(10), 128)
            .unwrap();
        assert_eq!(checkpoint_id, 1);
        assert_eq!(coordinator.active_checkpoint_id(), Some(1));
        assert_eq!(coordinator.active_aof_begin_offset(), Some(128));

        let plan = coordinator
            .complete_checkpoint(CheckpointToken::new(10), 512)
            .unwrap();
        assert_eq!(
            plan,
            RecoveryPlan {
                durable_checkpoint_id: 1,
                replay_from_aof_offset: 512,
            }
        );
        assert_eq!(coordinator.active_checkpoint_id(), None);
    }

    #[test]
    fn begin_fails_when_another_checkpoint_is_active() {
        let mut coordinator = CheckpointAofCoordinator::new();
        let checkpoint_id = coordinator
            .begin_checkpoint(CheckpointToken::new(10), 64)
            .unwrap();
        assert_eq!(checkpoint_id, 1);
        assert!(matches!(
            coordinator.begin_checkpoint(CheckpointToken::new(11), 65),
            Err(CheckpointAofCoordinatorError::Busy {
                active_checkpoint_id
            }) if active_checkpoint_id == 1
        ));
    }

    #[test]
    fn token_mismatch_is_rejected_on_complete_and_abort() {
        let mut coordinator = CheckpointAofCoordinator::new();
        coordinator
            .begin_checkpoint(CheckpointToken::new(10), 64)
            .unwrap();
        assert!(matches!(
            coordinator.complete_checkpoint(CheckpointToken::new(11), 128),
            Err(CheckpointAofCoordinatorError::TokenMismatch {
                expected,
                actual
            }) if expected == CheckpointToken::new(10) && actual == CheckpointToken::new(11)
        ));
        assert!(matches!(
            coordinator.abort_checkpoint(CheckpointToken::new(11)),
            Err(CheckpointAofCoordinatorError::TokenMismatch {
                expected,
                actual
            }) if expected == CheckpointToken::new(10) && actual == CheckpointToken::new(11)
        ));
    }

    #[test]
    fn abort_keeps_previous_recovery_plan() {
        let mut coordinator = CheckpointAofCoordinator::new();
        coordinator
            .begin_checkpoint(CheckpointToken::new(10), 64)
            .unwrap();
        coordinator
            .complete_checkpoint(CheckpointToken::new(10), 100)
            .unwrap();
        let checkpoint_id = coordinator
            .begin_checkpoint(CheckpointToken::new(20), 101)
            .unwrap();
        assert_eq!(checkpoint_id, 2);
        coordinator
            .abort_checkpoint(CheckpointToken::new(20))
            .unwrap();
        assert_eq!(
            coordinator.recovery_plan(),
            RecoveryPlan {
                durable_checkpoint_id: 1,
                replay_from_aof_offset: 100,
            }
        );
    }
}
