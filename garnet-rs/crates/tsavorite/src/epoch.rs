//! LightEpoch: epoch protection and deferred reclamation callbacks.
//!
//! See [Doc 04] for the epoch-table and drain model.

use core::sync::atomic::{AtomicU64, Ordering};
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

const DEFAULT_MIN_TABLE_SIZE: usize = 128;

static NEXT_THREAD_ID: AtomicU64 = AtomicU64::new(1);

thread_local! {
    static THREAD_ID: u64 = NEXT_THREAD_ID.fetch_add(1, Ordering::Relaxed);
    static PIN_STATE: RefCell<HashMap<usize, PinState>> = RefCell::new(HashMap::new());
}

#[derive(Debug, Clone, Copy)]
struct PinState {
    slot: usize,
    depth: usize,
}

struct EpochAction {
    trigger_epoch: u64,
    action: Box<dyn FnOnce() + Send + 'static>,
}

/// Cache-line-padded epoch table entry to avoid false sharing.
#[repr(C, align(64))]
#[derive(Debug)]
pub struct EpochEntry {
    local_current_epoch: AtomicU64,
    thread_id: AtomicU64,
    _padding: [u8; 48],
}

impl Default for EpochEntry {
    fn default() -> Self {
        Self {
            local_current_epoch: AtomicU64::new(0),
            thread_id: AtomicU64::new(0),
            _padding: [0; 48],
        }
    }
}

/// Epoch manager with per-thread table entries and deferred-drain callbacks.
pub struct LightEpoch {
    current_epoch: AtomicU64,
    safe_to_reclaim_epoch: AtomicU64,
    table: Box<[EpochEntry]>,
    drain_list: Mutex<VecDeque<EpochAction>>,
}

impl LightEpoch {
    /// Creates an epoch manager using a default table size.
    pub fn new() -> Self {
        let cpu_count = std::thread::available_parallelism()
            .map(|count| count.get())
            .unwrap_or(1);
        let table_size = DEFAULT_MIN_TABLE_SIZE.max(cpu_count.saturating_mul(2));
        Self::with_table_size(table_size)
    }

    /// Creates an epoch manager with an explicit table size.
    pub fn with_table_size(table_size: usize) -> Self {
        assert!(table_size > 0, "table_size must be > 0");

        let table = (0..table_size)
            .map(|_| EpochEntry::default())
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Self {
            current_epoch: AtomicU64::new(1),
            safe_to_reclaim_epoch: AtomicU64::new(0),
            table,
            drain_list: Mutex::new(VecDeque::new()),
        }
    }

    /// Returns current global epoch.
    #[inline]
    pub fn current_epoch(&self) -> u64 {
        self.current_epoch.load(Ordering::Acquire)
    }

    /// Returns latest epoch considered safe to reclaim.
    #[inline]
    pub fn safe_to_reclaim_epoch(&self) -> u64 {
        self.safe_to_reclaim_epoch.load(Ordering::Acquire)
    }

    /// Pins the current thread in epoch protection scope.
    ///
    /// The returned guard releases protection on drop.
    pub fn pin(&self) -> EpochGuard<'_> {
        let instance_key = self as *const Self as usize;
        let epoch = self.current_epoch();

        let slot = PIN_STATE.with(|state| {
            let mut state = state.borrow_mut();
            if let Some(pin_state) = state.get_mut(&instance_key) {
                pin_state.depth += 1;
                pin_state.slot
            } else {
                let thread_id = THREAD_ID.with(|thread_id| *thread_id);
                let slot = self.reserve_entry(thread_id);
                state.insert(instance_key, PinState { slot, depth: 1 });
                slot
            }
        });

        self.table[slot]
            .local_current_epoch
            .store(epoch, Ordering::Release);

        if !self.is_drain_list_empty() {
            self.drain(epoch);
        }

        EpochGuard {
            epoch: self,
            instance_key,
        }
    }

    /// Increments current epoch and updates reclamation/drain state.
    pub fn bump_current_epoch(&self) -> u64 {
        let next_epoch = self.current_epoch.fetch_add(1, Ordering::AcqRel) + 1;
        self.drain(next_epoch);
        next_epoch
    }

    /// Increments current epoch and registers a callback for deferred execution.
    ///
    /// The callback runs once `safe_to_reclaim_epoch >= prior_epoch`.
    pub fn bump_current_epoch_with_callback<F>(&self, callback: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let prior_epoch = self.bump_current_epoch().saturating_sub(1);

        self.drain_list
            .lock()
            .expect("drain list lock poisoned")
            .push_back(EpochAction {
                trigger_epoch: prior_epoch,
                action: Box::new(callback),
            });

        self.drain(self.current_epoch());
    }

    fn is_drain_list_empty(&self) -> bool {
        self.drain_list
            .lock()
            .expect("drain list lock poisoned")
            .is_empty()
    }

    fn reserve_entry(&self, thread_id: u64) -> usize {
        let table_len = self.table.len();
        let mut start = (thread_id as usize) % table_len;

        for _ in 0..2 {
            for _ in 0..table_len {
                let entry = &self.table[start];
                if entry
                    .thread_id
                    .compare_exchange(0, thread_id, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    return start;
                }

                start = (start + 1) % table_len;
            }
            std::thread::yield_now();
        }

        panic!("LightEpoch table is full; increase table size");
    }

    fn release_entry(&self, instance_key: usize) {
        PIN_STATE.with(|state| {
            let mut state = state.borrow_mut();
            let mut remove = false;
            let slot = {
                let pin_state = state
                    .get_mut(&instance_key)
                    .expect("pin state missing for epoch guard");

                pin_state.depth -= 1;
                if pin_state.depth == 0 {
                    remove = true;
                }
                pin_state.slot
            };

            if remove {
                self.table[slot]
                    .local_current_epoch
                    .store(0, Ordering::Release);
                self.table[slot].thread_id.store(0, Ordering::Release);
                state.remove(&instance_key);
            }
        });
    }

    fn compute_new_safe_to_reclaim_epoch(&self, current_epoch: u64) -> u64 {
        let mut oldest_ongoing = current_epoch;

        for entry in &self.table {
            let entry_epoch = entry.local_current_epoch.load(Ordering::Acquire);
            if entry_epoch != 0 && entry_epoch < oldest_ongoing {
                oldest_ongoing = entry_epoch;
            }
        }

        let safe_epoch = oldest_ongoing.saturating_sub(1);
        self.safe_to_reclaim_epoch
            .store(safe_epoch, Ordering::Release);
        safe_epoch
    }

    fn drain(&self, next_epoch: u64) {
        let safe_epoch = self.compute_new_safe_to_reclaim_epoch(next_epoch);

        let mut runnable = Vec::new();
        {
            let mut drain_list = self.drain_list.lock().expect("drain list lock poisoned");
            let len = drain_list.len();
            for _ in 0..len {
                let action = drain_list
                    .pop_front()
                    .expect("drain list length and pop_front out of sync");
                if action.trigger_epoch <= safe_epoch {
                    runnable.push(action.action);
                } else {
                    drain_list.push_back(action);
                }
            }
        }

        for action in runnable {
            action();
        }
    }

    #[cfg(test)]
    fn active_entries(&self) -> usize {
        self.table
            .iter()
            .filter(|entry| entry.thread_id.load(Ordering::Acquire) != 0)
            .count()
    }
}

impl Default for LightEpoch {
    fn default() -> Self {
        Self::new()
    }
}

/// RAII guard that keeps the thread pinned to the current epoch.
pub struct EpochGuard<'a> {
    epoch: &'a LightEpoch,
    instance_key: usize,
}

impl Drop for EpochGuard<'_> {
    fn drop(&mut self) {
        self.epoch.release_entry(self.instance_key);

        if !self.epoch.is_drain_list_empty() {
            self.epoch.drain(self.epoch.current_epoch());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::align_of;
    use std::sync::Arc;

    #[test]
    fn epoch_entry_is_cache_line_aligned() {
        assert_eq!(align_of::<EpochEntry>(), 64);
    }

    #[test]
    fn pin_and_drop_acquire_and_release_entry() {
        let epoch = LightEpoch::with_table_size(8);
        assert_eq!(epoch.active_entries(), 0);

        {
            let _guard = epoch.pin();
            assert_eq!(epoch.active_entries(), 1);
        }

        assert_eq!(epoch.active_entries(), 0);
    }

    #[test]
    fn nested_pin_reuses_same_thread_entry() {
        let epoch = LightEpoch::with_table_size(8);

        let guard1 = epoch.pin();
        assert_eq!(epoch.active_entries(), 1);

        {
            let _guard2 = epoch.pin();
            assert_eq!(epoch.active_entries(), 1);
        }

        assert_eq!(epoch.active_entries(), 1);
        drop(guard1);
        assert_eq!(epoch.active_entries(), 0);
    }

    #[test]
    fn deferred_callback_runs_after_safe_epoch_advances() {
        let epoch = Arc::new(LightEpoch::with_table_size(8));
        let fired = Arc::new(AtomicU64::new(0));

        {
            let _guard = epoch.pin();
            let fired_clone = Arc::clone(&fired);
            epoch.bump_current_epoch_with_callback(move || {
                fired_clone.store(1, Ordering::Release);
            });

            assert_eq!(fired.load(Ordering::Acquire), 0);
        }

        epoch.bump_current_epoch();
        assert_eq!(fired.load(Ordering::Acquire), 1);
    }
}
