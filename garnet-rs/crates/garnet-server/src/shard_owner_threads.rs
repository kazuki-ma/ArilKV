//! Owner-thread execution lanes for shard-affine work.
//!
//! This is an incremental building block toward lock-free shard ownership:
//! callers map a shard id to a stable owner thread and submit closures.

use crate::request_lifecycle::ShardIndex;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::Sender;
use std::sync::mpsc::{self};
use std::thread::JoinHandle;
use std::thread::{self};

type Job = Box<dyn FnOnce() + Send + 'static>;

enum WorkerMessage {
    Run(Job),
    Stop,
}

#[derive(Debug)]
pub enum ShardOwnerThreadPoolError {
    InvalidWorkerCount,
    InvalidShardCount,
    InvalidShardIndex {
        shard_index: ShardIndex,
        shard_count: usize,
    },
    WorkerSpawn(std::io::Error),
    WorkerStopped,
}

impl core::fmt::Display for ShardOwnerThreadPoolError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::InvalidWorkerCount => write!(f, "owner-thread worker_count must be > 0"),
            Self::InvalidShardCount => write!(f, "owner-thread shard_count must be > 0"),
            Self::InvalidShardIndex {
                shard_index,
                shard_count,
            } => write!(
                f,
                "invalid shard index {} (shard_count={})",
                shard_index.as_usize(),
                shard_count
            ),
            Self::WorkerSpawn(error) => write!(f, "failed to spawn owner thread: {error}"),
            Self::WorkerStopped => write!(f, "owner-thread worker stopped"),
        }
    }
}

impl std::error::Error for ShardOwnerThreadPoolError {}

pub struct ShardOwnerThreadPool {
    shard_count: usize,
    worker_count: usize,
    inline_execution: bool,
    senders: Vec<Sender<WorkerMessage>>,
    joins: Vec<JoinHandle<()>>,
}

impl ShardOwnerThreadPool {
    pub fn new(worker_count: usize, shard_count: usize) -> Result<Self, ShardOwnerThreadPoolError> {
        if worker_count == 0 {
            return Err(ShardOwnerThreadPoolError::InvalidWorkerCount);
        }
        if shard_count == 0 {
            return Err(ShardOwnerThreadPoolError::InvalidShardCount);
        }

        let mut senders = Vec::with_capacity(worker_count);
        let mut joins = Vec::with_capacity(worker_count);
        for worker_idx in 0..worker_count {
            let (sender, receiver) = mpsc::channel::<WorkerMessage>();
            let thread_name = format!("garnet-owner-{worker_idx}");
            let join = thread::Builder::new()
                .name(thread_name)
                .spawn(move || worker_loop(receiver))
                .map_err(ShardOwnerThreadPoolError::WorkerSpawn)?;
            senders.push(sender);
            joins.push(join);
        }

        Ok(Self {
            shard_count,
            worker_count,
            inline_execution: false,
            senders,
            joins,
        })
    }

    pub fn new_inline(shard_count: usize) -> Result<Self, ShardOwnerThreadPoolError> {
        if shard_count == 0 {
            return Err(ShardOwnerThreadPoolError::InvalidShardCount);
        }
        Ok(Self {
            shard_count,
            worker_count: 1,
            inline_execution: true,
            senders: Vec::new(),
            joins: Vec::new(),
        })
    }

    #[inline]
    pub fn shard_count(&self) -> usize {
        self.shard_count
    }

    #[inline]
    pub fn worker_count(&self) -> usize {
        self.worker_count
    }

    #[inline]
    pub fn is_inline_execution(&self) -> bool {
        self.inline_execution
    }

    #[inline]
    pub fn owner_thread_for_shard(
        &self,
        shard_index: ShardIndex,
    ) -> Result<usize, ShardOwnerThreadPoolError> {
        let shard = shard_index.as_usize();
        if shard >= self.shard_count {
            return Err(ShardOwnerThreadPoolError::InvalidShardIndex {
                shard_index,
                shard_count: self.shard_count,
            });
        }
        Ok(shard % self.worker_count)
    }

    pub fn submit<R, F>(
        &self,
        shard_index: ShardIndex,
        op: F,
    ) -> Result<Receiver<R>, ShardOwnerThreadPoolError>
    where
        R: Send + 'static,
        F: FnOnce() -> R + Send + 'static,
    {
        let shard = shard_index.as_usize();
        if self.inline_execution {
            if shard >= self.shard_count {
                return Err(ShardOwnerThreadPoolError::InvalidShardIndex {
                    shard_index,
                    shard_count: self.shard_count,
                });
            }
            let (result_tx, result_rx) = mpsc::channel::<R>();
            let _ = result_tx.send(op());
            return Ok(result_rx);
        }

        let owner = self.owner_thread_for_shard(shard_index)?;
        let (result_tx, result_rx) = mpsc::sync_channel::<R>(1);
        let job = Box::new(move || {
            let _ = result_tx.send(op());
        });
        self.senders[owner]
            .send(WorkerMessage::Run(job))
            .map_err(|_| ShardOwnerThreadPoolError::WorkerStopped)?;
        Ok(result_rx)
    }

    pub fn execute_sync<R, F>(
        &self,
        shard_index: ShardIndex,
        op: F,
    ) -> Result<R, ShardOwnerThreadPoolError>
    where
        R: Send + 'static,
        F: FnOnce() -> R + Send + 'static,
    {
        let receiver = self.submit(shard_index, op)?;
        receiver
            .recv()
            .map_err(|_| ShardOwnerThreadPoolError::WorkerStopped)
    }
}

impl Drop for ShardOwnerThreadPool {
    fn drop(&mut self) {
        if self.inline_execution {
            return;
        }
        for sender in &self.senders {
            let _ = sender.send(WorkerMessage::Stop);
        }
        for join in self.joins.drain(..) {
            let _ = join.join();
        }
    }
}

fn worker_loop(receiver: Receiver<WorkerMessage>) {
    while let Ok(message) = receiver.recv() {
        match message {
            WorkerMessage::Run(job) => {
                job();
            }
            WorkerMessage::Stop => break,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;
    use std::time::Instant;

    #[test]
    fn owner_thread_mapping_is_stable_and_modulo_based() {
        let pool = ShardOwnerThreadPool::new(4, 16).unwrap();
        assert_eq!(pool.owner_thread_for_shard(ShardIndex::new(0)).unwrap(), 0);
        assert_eq!(pool.owner_thread_for_shard(ShardIndex::new(1)).unwrap(), 1);
        assert_eq!(pool.owner_thread_for_shard(ShardIndex::new(2)).unwrap(), 2);
        assert_eq!(pool.owner_thread_for_shard(ShardIndex::new(3)).unwrap(), 3);
        assert_eq!(pool.owner_thread_for_shard(ShardIndex::new(4)).unwrap(), 0);
        assert_eq!(pool.owner_thread_for_shard(ShardIndex::new(15)).unwrap(), 3);
    }

    #[test]
    fn same_shard_jobs_run_on_same_owner_thread() {
        let pool = ShardOwnerThreadPool::new(4, 16).unwrap();
        let expected = pool
            .execute_sync(ShardIndex::new(7), || thread::current().id())
            .unwrap();
        for _ in 0..32 {
            let observed = pool
                .execute_sync(ShardIndex::new(7), || thread::current().id())
                .unwrap();
            assert_eq!(observed, expected);
        }
    }

    #[test]
    fn distinct_owner_threads_execute_in_parallel() {
        let pool = ShardOwnerThreadPool::new(2, 2).unwrap();
        let start = Instant::now();
        let slow_a = pool
            .submit(ShardIndex::new(0), || {
                thread::sleep(Duration::from_millis(150));
                1u8
            })
            .unwrap();
        let slow_b = pool
            .submit(ShardIndex::new(1), || {
                thread::sleep(Duration::from_millis(150));
                1u8
            })
            .unwrap();
        assert_eq!(slow_a.recv().unwrap(), 1);
        assert_eq!(slow_b.recv().unwrap(), 1);
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_millis(260),
            "expected parallel execution; elapsed={elapsed:?}"
        );
    }

    #[test]
    fn pool_is_shareable_across_threads() {
        let pool = Arc::new(ShardOwnerThreadPool::new(2, 4).unwrap());
        let pool_clone = Arc::clone(&pool);
        let worker =
            thread::spawn(move || pool_clone.execute_sync(ShardIndex::new(3), || 7u8).unwrap());
        assert_eq!(worker.join().unwrap(), 7);
        assert_eq!(pool.execute_sync(ShardIndex::new(3), || 9u8).unwrap(), 9);
    }

    #[test]
    fn inline_pool_executes_on_caller_thread() {
        let pool = ShardOwnerThreadPool::new_inline(4).unwrap();
        assert!(pool.is_inline_execution());
        let caller = thread::current().id();
        let observed = pool
            .execute_sync(ShardIndex::new(1), || thread::current().id())
            .unwrap();
        assert_eq!(caller, observed);
    }
}
