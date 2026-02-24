//! Atomic address pointer set for hybrid-log regions.
//!
//! See [Doc 02 Section 2.3] for pointer semantics.

use core::sync::atomic::AtomicU64;
use core::sync::atomic::Ordering;

/// Immutable snapshot of hybrid-log region pointers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogAddressPointersSnapshot {
    pub tail_address: u64,
    pub read_only_address: u64,
    pub safe_read_only_address: u64,
    pub head_address: u64,
    pub safe_head_address: u64,
    pub begin_address: u64,
}

/// Atomic pointer set partitioning the logical hybrid-log address space.
#[derive(Debug)]
pub struct LogAddressPointers {
    tail_address: AtomicU64,
    read_only_address: AtomicU64,
    safe_read_only_address: AtomicU64,
    head_address: AtomicU64,
    safe_head_address: AtomicU64,
    begin_address: AtomicU64,
}

impl LogAddressPointers {
    /// Creates a pointer set initialized at a single logical address.
    pub fn new(initial_address: u64) -> Self {
        Self {
            tail_address: AtomicU64::new(initial_address),
            read_only_address: AtomicU64::new(initial_address),
            safe_read_only_address: AtomicU64::new(initial_address),
            head_address: AtomicU64::new(initial_address),
            safe_head_address: AtomicU64::new(initial_address),
            begin_address: AtomicU64::new(initial_address),
        }
    }

    /// Returns a consistent snapshot using acquire loads.
    pub fn snapshot(&self) -> LogAddressPointersSnapshot {
        LogAddressPointersSnapshot {
            tail_address: self.tail_address(),
            read_only_address: self.read_only_address(),
            safe_read_only_address: self.safe_read_only_address(),
            head_address: self.head_address(),
            safe_head_address: self.safe_head_address(),
            begin_address: self.begin_address(),
        }
    }

    #[inline]
    pub fn tail_address(&self) -> u64 {
        self.tail_address.load(Ordering::Acquire)
    }

    #[inline]
    pub fn read_only_address(&self) -> u64 {
        self.read_only_address.load(Ordering::Acquire)
    }

    #[inline]
    pub fn safe_read_only_address(&self) -> u64 {
        self.safe_read_only_address.load(Ordering::Acquire)
    }

    #[inline]
    pub fn head_address(&self) -> u64 {
        self.head_address.load(Ordering::Acquire)
    }

    #[inline]
    pub fn safe_head_address(&self) -> u64 {
        self.safe_head_address.load(Ordering::Acquire)
    }

    #[inline]
    pub fn begin_address(&self) -> u64 {
        self.begin_address.load(Ordering::Acquire)
    }

    /// Monotonically advances tail address and returns old value.
    #[inline]
    pub fn advance_tail_by(&self, delta: u64) -> u64 {
        self.tail_address.fetch_add(delta, Ordering::AcqRel)
    }

    /// Attempts to move tail address to `new_address` if it is greater than current value.
    #[inline]
    pub fn shift_tail_address(&self, new_address: u64) -> bool {
        monotonic_update(&self.tail_address, new_address)
    }

    /// Attempts to move read-only address to `new_address` if it is greater than current value.
    #[inline]
    pub fn shift_read_only_address(&self, new_address: u64) -> bool {
        monotonic_update(&self.read_only_address, new_address)
    }

    /// Attempts to move safe read-only address to `new_address` if it is greater than current value.
    #[inline]
    pub fn shift_safe_read_only_address(&self, new_address: u64) -> bool {
        monotonic_update(&self.safe_read_only_address, new_address)
    }

    /// Attempts to move head address to `new_address` if it is greater than current value.
    #[inline]
    pub fn shift_head_address(&self, new_address: u64) -> bool {
        monotonic_update(&self.head_address, new_address)
    }

    /// Attempts to move safe head address to `new_address` if it is greater than current value.
    #[inline]
    pub fn shift_safe_head_address(&self, new_address: u64) -> bool {
        monotonic_update(&self.safe_head_address, new_address)
    }

    /// Attempts to move begin address to `new_address` if it is greater than current value.
    #[inline]
    pub fn shift_begin_address(&self, new_address: u64) -> bool {
        monotonic_update(&self.begin_address, new_address)
    }
}

impl Default for LogAddressPointers {
    fn default() -> Self {
        Self::new(0)
    }
}

fn monotonic_update(pointer: &AtomicU64, new_value: u64) -> bool {
    let mut current = pointer.load(Ordering::Acquire);
    while new_value > current {
        match pointer.compare_exchange(current, new_value, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return true,
            Err(observed) => current = observed,
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pointers_start_from_initial_address() {
        let pointers = LogAddressPointers::new(64);
        let snapshot = pointers.snapshot();

        assert_eq!(snapshot.tail_address, 64);
        assert_eq!(snapshot.read_only_address, 64);
        assert_eq!(snapshot.safe_read_only_address, 64);
        assert_eq!(snapshot.head_address, 64);
        assert_eq!(snapshot.safe_head_address, 64);
        assert_eq!(snapshot.begin_address, 64);
    }

    #[test]
    fn monotonic_shifts_do_not_move_backwards() {
        let pointers = LogAddressPointers::new(100);
        assert!(pointers.shift_read_only_address(120));
        assert!(!pointers.shift_read_only_address(110));
        assert_eq!(pointers.read_only_address(), 120);
    }

    #[test]
    fn tail_fetch_add_advances_monotonically() {
        let pointers = LogAddressPointers::new(256);
        let old = pointers.advance_tail_by(32);

        assert_eq!(old, 256);
        assert_eq!(pointers.tail_address(), 288);
    }

    #[test]
    fn independent_pointer_updates_are_visible_in_snapshot() {
        let pointers = LogAddressPointers::new(0);
        pointers.shift_begin_address(64);
        pointers.shift_head_address(128);
        pointers.shift_safe_head_address(192);
        pointers.shift_read_only_address(224);
        pointers.shift_safe_read_only_address(256);
        pointers.shift_tail_address(320);

        let snapshot = pointers.snapshot();
        assert_eq!(snapshot.begin_address, 64);
        assert_eq!(snapshot.head_address, 128);
        assert_eq!(snapshot.safe_head_address, 192);
        assert_eq!(snapshot.read_only_address, 224);
        assert_eq!(snapshot.safe_read_only_address, 256);
        assert_eq!(snapshot.tail_address, 320);
    }
}
