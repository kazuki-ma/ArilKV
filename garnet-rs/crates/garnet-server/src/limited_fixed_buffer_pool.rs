//! Tiered fixed-size buffer pool for network receive/send buffers.
//!
//! The pool maintains N power-of-two levels where level `i` stores buffers of
//! `min_allocation_size * 2^i`.

use crossbeam_queue::ArrayQueue;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LimitedFixedBufferPoolConfig {
    pub min_allocation_size: usize,
    pub num_levels: usize,
    pub max_entries_per_level: usize,
}

impl Default for LimitedFixedBufferPoolConfig {
    fn default() -> Self {
        Self {
            min_allocation_size: 128 * 1024,
            num_levels: 4,
            max_entries_per_level: 16,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LimitedFixedBufferPoolError {
    InvalidConfiguration {
        min_allocation_size: usize,
        num_levels: usize,
        max_entries_per_level: usize,
    },
    InvalidRequestSize {
        requested_size: usize,
        min_allocation_size: usize,
        max_allocation_size: usize,
    },
    InvalidEntry {
        level: usize,
        length: usize,
    },
}

impl core::fmt::Display for LimitedFixedBufferPoolError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::InvalidConfiguration {
                min_allocation_size,
                num_levels,
                max_entries_per_level,
            } => write!(
                f,
                "invalid pool config: min={}, levels={}, max_entries_per_level={}",
                min_allocation_size, num_levels, max_entries_per_level
            ),
            Self::InvalidRequestSize {
                requested_size,
                min_allocation_size,
                max_allocation_size,
            } => write!(
                f,
                "invalid request size {} (valid power-of-two range: {}..={})",
                requested_size, min_allocation_size, max_allocation_size
            ),
            Self::InvalidEntry { level, length } => {
                write!(f, "invalid pool entry level={} length={}", level, length)
            }
        }
    }
}

impl std::error::Error for LimitedFixedBufferPoolError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReturnStatus {
    Returned,
    DroppedLevelFull,
}

#[derive(Debug)]
pub struct PoolEntry {
    level: usize,
    buffer: Vec<u8>,
}

impl PoolEntry {
    #[inline]
    pub fn level(&self) -> usize {
        self.level
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        &self.buffer
    }

    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.buffer
    }
}

pub struct LimitedFixedBufferPool {
    min_allocation_size: usize,
    max_allocation_size: usize,
    levels: Box<[ArrayQueue<Vec<u8>>]>,
}

impl LimitedFixedBufferPool {
    pub fn new(config: LimitedFixedBufferPoolConfig) -> Result<Self, LimitedFixedBufferPoolError> {
        let valid = config.min_allocation_size > 0
            && config.min_allocation_size.is_power_of_two()
            && config.num_levels > 0
            && config.max_entries_per_level > 0;
        if !valid {
            return Err(LimitedFixedBufferPoolError::InvalidConfiguration {
                min_allocation_size: config.min_allocation_size,
                num_levels: config.num_levels,
                max_entries_per_level: config.max_entries_per_level,
            });
        }

        let max_allocation_size = config.min_allocation_size << (config.num_levels - 1);
        let levels = (0..config.num_levels)
            .map(|_| ArrayQueue::new(config.max_entries_per_level))
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Ok(Self {
            min_allocation_size: config.min_allocation_size,
            max_allocation_size,
            levels,
        })
    }

    #[inline]
    pub fn min_allocation_size(&self) -> usize {
        self.min_allocation_size
    }

    #[inline]
    pub fn max_allocation_size(&self) -> usize {
        self.max_allocation_size
    }

    #[inline]
    pub fn level_count(&self) -> usize {
        self.levels.len()
    }

    pub fn level_size(&self, level: usize) -> Option<usize> {
        if level >= self.levels.len() {
            return None;
        }
        Some(self.min_allocation_size << level)
    }

    pub fn available_count(&self, level: usize) -> Option<usize> {
        self.levels.get(level).map(ArrayQueue::len)
    }

    pub fn get(&self, requested_size: usize) -> Result<PoolEntry, LimitedFixedBufferPoolError> {
        let level = self.level_for_size(requested_size)?;
        let expected_size = self
            .level_size(level)
            .expect("level derived from request must be in range");
        let buffer = self.levels[level]
            .pop()
            .unwrap_or_else(|| vec![0u8; expected_size]);
        Ok(PoolEntry { level, buffer })
    }

    pub fn return_buffer(
        &self,
        mut entry: PoolEntry,
    ) -> Result<ReturnStatus, LimitedFixedBufferPoolError> {
        let level = entry.level;
        let expected_len =
            self.level_size(level)
                .ok_or(LimitedFixedBufferPoolError::InvalidEntry {
                    level,
                    length: entry.buffer.len(),
                })?;

        if entry.buffer.len() != expected_len {
            return Err(LimitedFixedBufferPoolError::InvalidEntry {
                level,
                length: entry.buffer.len(),
            });
        }

        entry.buffer.fill(0);
        match self.levels[level].push(entry.buffer) {
            Ok(()) => Ok(ReturnStatus::Returned),
            Err(_buffer) => Ok(ReturnStatus::DroppedLevelFull),
        }
    }

    fn level_for_size(&self, requested_size: usize) -> Result<usize, LimitedFixedBufferPoolError> {
        if requested_size < self.min_allocation_size
            || requested_size > self.max_allocation_size
            || !requested_size.is_power_of_two()
            || !requested_size.is_multiple_of(self.min_allocation_size)
        {
            return Err(LimitedFixedBufferPoolError::InvalidRequestSize {
                requested_size,
                min_allocation_size: self.min_allocation_size,
                max_allocation_size: self.max_allocation_size,
            });
        }

        let ratio = requested_size / self.min_allocation_size;
        if !ratio.is_power_of_two() {
            return Err(LimitedFixedBufferPoolError::InvalidRequestSize {
                requested_size,
                min_allocation_size: self.min_allocation_size,
                max_allocation_size: self.max_allocation_size,
            });
        }

        Ok(ratio.trailing_zeros() as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn rejects_invalid_configuration() {
        let err = LimitedFixedBufferPool::new(LimitedFixedBufferPoolConfig {
            min_allocation_size: 100,
            num_levels: 0,
            max_entries_per_level: 0,
        })
        .err()
        .unwrap();
        assert!(matches!(
            err,
            LimitedFixedBufferPoolError::InvalidConfiguration { .. }
        ));
    }

    #[test]
    fn get_returns_expected_size_class() {
        let pool = LimitedFixedBufferPool::new(LimitedFixedBufferPoolConfig {
            min_allocation_size: 1024,
            num_levels: 4,
            max_entries_per_level: 2,
        })
        .unwrap();

        for level in 0..pool.level_count() {
            let requested_size = pool.level_size(level).unwrap();
            let entry = pool.get(requested_size).unwrap();
            assert_eq!(entry.level(), level);
            assert_eq!(entry.len(), requested_size);
        }
    }

    #[test]
    fn return_clears_buffer_before_reuse() {
        let pool = LimitedFixedBufferPool::new(LimitedFixedBufferPoolConfig {
            min_allocation_size: 256,
            num_levels: 2,
            max_entries_per_level: 4,
        })
        .unwrap();

        let mut entry = pool.get(256).unwrap();
        entry.as_mut_slice().fill(7);
        assert_eq!(pool.return_buffer(entry).unwrap(), ReturnStatus::Returned);

        let reused = pool.get(256).unwrap();
        assert!(reused.as_slice().iter().all(|byte| *byte == 0));
    }

    #[test]
    fn return_drops_when_level_is_full() {
        let pool = LimitedFixedBufferPool::new(LimitedFixedBufferPoolConfig {
            min_allocation_size: 256,
            num_levels: 1,
            max_entries_per_level: 1,
        })
        .unwrap();

        let entry1 = pool.get(256).unwrap();
        let entry2 = pool.get(256).unwrap();
        assert_eq!(pool.return_buffer(entry1).unwrap(), ReturnStatus::Returned);
        assert_eq!(
            pool.return_buffer(entry2).unwrap(),
            ReturnStatus::DroppedLevelFull
        );
    }

    #[test]
    fn concurrent_get_and_return_stays_within_capacity() {
        let pool = Arc::new(
            LimitedFixedBufferPool::new(LimitedFixedBufferPoolConfig {
                min_allocation_size: 512,
                num_levels: 2,
                max_entries_per_level: 8,
            })
            .unwrap(),
        );

        let mut handles = Vec::new();
        for _ in 0..4 {
            let shared = Arc::clone(&pool);
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    let mut entry = shared.get(512).unwrap();
                    entry.as_mut_slice()[0] = 1;
                    let _ = shared.return_buffer(entry).unwrap();
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert!(pool.available_count(0).unwrap() <= 8);
    }
}
