//! Owner execution identity for shard-affine work.
//!
//! The server has a single execution model: parsed work is executed on the
//! owner lane for the shard/node serving the connection. This handle is a
//! validation boundary for code paths that still need to prove a shard belongs
//! to the current owner context.

use crate::request_lifecycle::ShardIndex;

#[derive(Debug)]
pub enum ShardOwnerThreadPoolError {
    InvalidShardCount,
    InvalidShardIndex {
        shard_index: ShardIndex,
        shard_count: usize,
    },
}

impl core::fmt::Display for ShardOwnerThreadPoolError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::InvalidShardCount => write!(f, "owner shard_count must be > 0"),
            Self::InvalidShardIndex {
                shard_index,
                shard_count,
            } => write!(
                f,
                "invalid shard index {} (shard_count={})",
                shard_index.as_usize(),
                shard_count
            ),
        }
    }
}

impl std::error::Error for ShardOwnerThreadPoolError {}

pub struct ShardOwnerThreadPool {
    shard_count: usize,
}

impl ShardOwnerThreadPool {
    pub fn new(shard_count: usize) -> Result<Self, ShardOwnerThreadPoolError> {
        if shard_count == 0 {
            return Err(ShardOwnerThreadPoolError::InvalidShardCount);
        }
        Ok(Self { shard_count })
    }

    #[inline]
    pub fn shard_count(&self) -> usize {
        self.shard_count
    }

    #[inline]
    pub fn owner_thread_for_shard(
        &self,
        shard_index: ShardIndex,
    ) -> Result<usize, ShardOwnerThreadPoolError> {
        self.validate_shard(shard_index)?;
        Ok(shard_index.as_usize())
    }

    pub fn execute_sync<R, F>(
        &self,
        shard_index: ShardIndex,
        op: F,
    ) -> Result<R, ShardOwnerThreadPoolError>
    where
        F: FnOnce() -> R,
    {
        self.validate_shard(shard_index)?;
        Ok(op())
    }

    #[inline]
    fn validate_shard(&self, shard_index: ShardIndex) -> Result<(), ShardOwnerThreadPoolError> {
        if shard_index.as_usize() >= self.shard_count {
            return Err(ShardOwnerThreadPoolError::InvalidShardIndex {
                shard_index,
                shard_count: self.shard_count,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owner_thread_mapping_is_identity_based() {
        let pool = ShardOwnerThreadPool::new(16).unwrap();
        assert_eq!(pool.owner_thread_for_shard(ShardIndex::new(0)).unwrap(), 0);
        assert_eq!(pool.owner_thread_for_shard(ShardIndex::new(1)).unwrap(), 1);
        assert_eq!(
            pool.owner_thread_for_shard(ShardIndex::new(15)).unwrap(),
            15
        );
    }

    #[test]
    fn execute_sync_runs_on_owner_context_after_validating_shard() {
        let pool = ShardOwnerThreadPool::new(4).unwrap();
        assert_eq!(pool.execute_sync(ShardIndex::new(1), || 7u8).unwrap(), 7);
        assert!(pool.execute_sync(ShardIndex::new(4), || 9u8).is_err());
    }
}
