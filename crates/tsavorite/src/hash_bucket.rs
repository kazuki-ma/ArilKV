//! HashBucket: cache-line-aligned hash index bucket.
//!
//! See [Doc 03 Section 2.3] for bucket layout.

use crate::hash_bucket_entry::ADDRESS_BITS;
use crate::hash_bucket_entry::ADDRESS_MASK;
use crate::hash_bucket_entry::HashBucketEntry;
use crate::hash_bucket_entry::HashBucketEntryError;
use crate::hybrid_log::LogicalAddress;
use core::array;
use core::sync::atomic::AtomicU64;
use core::sync::atomic::Ordering;
use std::thread;

pub const HASH_BUCKET_ENTRY_COUNT: usize = 8;
pub const HASH_BUCKET_DATA_ENTRY_COUNT: usize = HASH_BUCKET_ENTRY_COUNT - 1;
pub const HASH_BUCKET_OVERFLOW_INDEX: usize = HASH_BUCKET_DATA_ENTRY_COUNT;

pub const SHARED_LATCH_BITS: u32 = 63 - ADDRESS_BITS;
pub const SHARED_LATCH_BIT_OFFSET: u32 = ADDRESS_BITS;
pub const EXCLUSIVE_LATCH_BIT_OFFSET: u32 = SHARED_LATCH_BIT_OFFSET + SHARED_LATCH_BITS;

pub const SHARED_LATCH_BIT_MASK: u64 = ((1u64 << SHARED_LATCH_BITS) - 1) << SHARED_LATCH_BIT_OFFSET;
pub const SHARED_LATCH_INCREMENT: u64 = 1u64 << SHARED_LATCH_BIT_OFFSET;
pub const EXCLUSIVE_LATCH_BIT_MASK: u64 = 1u64 << EXCLUSIVE_LATCH_BIT_OFFSET;
pub const LATCH_BIT_MASK: u64 = SHARED_LATCH_BIT_MASK | EXCLUSIVE_LATCH_BIT_MASK;

pub const MAX_LOCK_SPINS: usize = 10;
pub const MAX_READER_LOCK_DRAIN_SPINS: usize = MAX_LOCK_SPINS * 10;

/// Hash bucket aligned to one cache line.
///
/// Layout: `7 x HashBucketEntry (8 bytes each) + 1 overflow/latch word (8 bytes)`.
#[repr(C, align(64))]
pub struct HashBucket {
    entries: [HashBucketEntry; HASH_BUCKET_DATA_ENTRY_COUNT],
    overflow_entry_word: AtomicU64,
}

impl HashBucket {
    /// Returns a reference to a data entry [0..7).
    #[inline]
    pub fn entry(&self, index: usize) -> Option<&HashBucketEntry> {
        self.entries.get(index)
    }

    /// Returns all data entries.
    #[inline]
    pub fn entries(&self) -> &[HashBucketEntry; HASH_BUCKET_DATA_ENTRY_COUNT] {
        &self.entries
    }

    /// Returns overflow+latch packed word.
    #[inline]
    pub fn overflow_word(&self, ordering: Ordering) -> u64 {
        self.overflow_entry_word.load(ordering)
    }

    /// Stores overflow+latch packed word.
    #[inline]
    pub fn store_overflow_word(&self, word: u64, ordering: Ordering) {
        self.overflow_entry_word.store(word, ordering);
    }

    /// CAS on overflow+latch packed word.
    #[inline]
    pub fn compare_exchange_overflow_word(
        &self,
        current: u64,
        new: u64,
        success: Ordering,
        failure: Ordering,
    ) -> Result<u64, u64> {
        self.overflow_entry_word
            .compare_exchange(current, new, success, failure)
    }

    /// Returns overflow bucket address bits (lower 48 bits).
    #[inline]
    pub fn overflow_address(&self, ordering: Ordering) -> u64 {
        self.overflow_word(ordering) & ADDRESS_MASK
    }

    /// Sets overflow bucket address while preserving latch bits.
    #[inline]
    pub fn set_overflow_address(
        &self,
        address: u64,
        ordering: Ordering,
    ) -> Result<(), HashBucketEntryError> {
        if address > ADDRESS_MASK {
            return Err(HashBucketEntryError::AddressOutOfRange {
                address: LogicalAddress(address),
                max_address: LogicalAddress(ADDRESS_MASK),
            });
        }

        let mut current = self.overflow_word(Ordering::Acquire);
        loop {
            let new = (current & !ADDRESS_MASK) | (address & ADDRESS_MASK);
            match self.compare_exchange_overflow_word(current, new, ordering, Ordering::Acquire) {
                Ok(_) => return Ok(()),
                Err(observed) => current = observed,
            }
        }
    }

    /// Returns shared latch count encoded in overflow entry.
    #[inline]
    pub fn shared_latch_count(&self, ordering: Ordering) -> u16 {
        ((self.overflow_word(ordering) & SHARED_LATCH_BIT_MASK) >> SHARED_LATCH_BIT_OFFSET) as u16
    }

    /// Returns whether an exclusive latch is held.
    #[inline]
    pub fn is_latched_exclusive(&self, ordering: Ordering) -> bool {
        (self.overflow_word(ordering) & EXCLUSIVE_LATCH_BIT_MASK) != 0
    }

    /// Returns whether any latch bits are set.
    #[inline]
    pub fn is_latched(&self, ordering: Ordering) -> bool {
        (self.overflow_word(ordering) & LATCH_BIT_MASK) != 0
    }

    /// Attempts to acquire shared latch on this bucket.
    pub fn try_acquire_shared_latch(&self) -> bool {
        for _ in 0..MAX_LOCK_SPINS {
            let expected_word = self.overflow_word(Ordering::Acquire);
            if (expected_word & EXCLUSIVE_LATCH_BIT_MASK) == 0
                && (expected_word & SHARED_LATCH_BIT_MASK) != SHARED_LATCH_BIT_MASK
            {
                let new_word = expected_word + SHARED_LATCH_INCREMENT;
                if self
                    .compare_exchange_overflow_word(
                        expected_word,
                        new_word,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
                {
                    return true;
                }
            }
            thread::yield_now();
        }
        false
    }

    /// Releases shared latch.
    #[inline]
    pub fn release_shared_latch(&self) {
        self.overflow_entry_word
            .fetch_sub(SHARED_LATCH_INCREMENT, Ordering::AcqRel);
    }

    /// Attempts to acquire exclusive latch and drain active shared latch holders.
    pub fn try_acquire_exclusive_latch(&self) -> bool {
        for _ in 0..MAX_LOCK_SPINS {
            let expected_word = self.overflow_word(Ordering::Acquire);
            if (expected_word & EXCLUSIVE_LATCH_BIT_MASK) == 0 {
                let new_word = expected_word | EXCLUSIVE_LATCH_BIT_MASK;
                if self
                    .compare_exchange_overflow_word(
                        expected_word,
                        new_word,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
                {
                    for _ in 0..MAX_READER_LOCK_DRAIN_SPINS {
                        if (self.overflow_word(Ordering::Acquire) & SHARED_LATCH_BIT_MASK) == 0 {
                            return true;
                        }
                        thread::yield_now();
                    }

                    self.release_exclusive_latch();
                    return false;
                }
            }
            thread::yield_now();
        }
        false
    }

    /// Promotes a shared latch to exclusive and drains remaining readers.
    pub fn try_promote_latch(&self) -> bool {
        for _ in 0..MAX_LOCK_SPINS {
            let expected_word = self.overflow_word(Ordering::Acquire);
            if (expected_word & SHARED_LATCH_BIT_MASK) == 0 {
                return false;
            }
            if (expected_word & EXCLUSIVE_LATCH_BIT_MASK) == 0 {
                let new_word = (expected_word | EXCLUSIVE_LATCH_BIT_MASK) - SHARED_LATCH_INCREMENT;
                if self
                    .compare_exchange_overflow_word(
                        expected_word,
                        new_word,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
                {
                    for _ in 0..MAX_READER_LOCK_DRAIN_SPINS {
                        if (self.overflow_word(Ordering::Acquire) & SHARED_LATCH_BIT_MASK) == 0 {
                            return true;
                        }
                        thread::yield_now();
                    }

                    for _ in 0..MAX_LOCK_SPINS {
                        let rollback_expected = self.overflow_word(Ordering::Acquire);
                        let rollback_new = (rollback_expected & !EXCLUSIVE_LATCH_BIT_MASK)
                            + SHARED_LATCH_INCREMENT;
                        if self
                            .compare_exchange_overflow_word(
                                rollback_expected,
                                rollback_new,
                                Ordering::AcqRel,
                                Ordering::Acquire,
                            )
                            .is_ok()
                        {
                            break;
                        }
                        thread::yield_now();
                    }
                    return false;
                }
            }
            thread::yield_now();
        }
        false
    }

    /// Releases exclusive latch while preserving address and shared-latch bits.
    pub fn release_exclusive_latch(&self) {
        loop {
            let expected_word = self.overflow_word(Ordering::Acquire);
            let new_word = expected_word & !EXCLUSIVE_LATCH_BIT_MASK;
            if self
                .compare_exchange_overflow_word(
                    expected_word,
                    new_word,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                return;
            }
            thread::yield_now();
        }
    }

    /// Clears all data entries and overflow/latch word.
    #[inline]
    pub fn clear(&self, ordering: Ordering) {
        for entry in &self.entries {
            entry.store(HashBucketEntry::EMPTY_WORD, ordering);
        }
        self.overflow_entry_word.store(0, ordering);
    }
}

impl Default for HashBucket {
    fn default() -> Self {
        Self {
            entries: array::from_fn(|_| HashBucketEntry::default()),
            overflow_entry_word: AtomicU64::new(0),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::align_of;
    use core::mem::size_of;

    #[test]
    fn hash_bucket_is_one_cache_line() {
        assert_eq!(size_of::<HashBucket>(), 64);
        assert_eq!(align_of::<HashBucket>(), 64);
    }

    #[test]
    fn hash_bucket_has_seven_data_entries() {
        assert_eq!(HASH_BUCKET_ENTRY_COUNT, 8);
        assert_eq!(HASH_BUCKET_DATA_ENTRY_COUNT, 7);
        assert_eq!(HASH_BUCKET_OVERFLOW_INDEX, 7);

        let bucket = HashBucket::default();
        assert_eq!(bucket.entries().len(), HASH_BUCKET_DATA_ENTRY_COUNT);
    }

    #[test]
    fn overflow_address_roundtrip_preserves_latch_bits() {
        let bucket = HashBucket::default();
        let latched_word = EXCLUSIVE_LATCH_BIT_MASK | (3u64 << SHARED_LATCH_BIT_OFFSET);
        bucket.store_overflow_word(latched_word, Ordering::Release);

        bucket
            .set_overflow_address(0x0000_1234_5678, Ordering::AcqRel)
            .unwrap();

        assert_eq!(bucket.overflow_address(Ordering::Acquire), 0x0000_1234_5678);
        assert!(bucket.is_latched_exclusive(Ordering::Acquire));
        assert_eq!(bucket.shared_latch_count(Ordering::Acquire), 3);
    }

    #[test]
    fn entries_are_mutable_via_atomic_words() {
        let bucket = HashBucket::default();
        let entry = bucket.entry(0).unwrap();
        let new_word = HashBucketEntry::pack(42, LogicalAddress(99), true, false).unwrap();

        entry.store(new_word, Ordering::Release);

        assert_eq!(entry.tag(Ordering::Acquire), 42);
        assert_eq!(entry.address(Ordering::Acquire), LogicalAddress(99));
        assert!(entry.tentative(Ordering::Acquire));
        assert!(!entry.pending(Ordering::Acquire));
    }

    #[test]
    fn shared_latch_acquire_and_release() {
        let bucket = HashBucket::default();
        assert!(bucket.try_acquire_shared_latch());
        assert_eq!(bucket.shared_latch_count(Ordering::Acquire), 1);
        bucket.release_shared_latch();
        assert_eq!(bucket.shared_latch_count(Ordering::Acquire), 0);
    }

    #[test]
    fn exclusive_latch_acquire_and_release() {
        let bucket = HashBucket::default();
        assert!(bucket.try_acquire_exclusive_latch());
        assert!(bucket.is_latched_exclusive(Ordering::Acquire));
        bucket.release_exclusive_latch();
        assert!(!bucket.is_latched_exclusive(Ordering::Acquire));
    }

    #[test]
    fn shared_latch_fails_while_exclusive_latched() {
        let bucket = HashBucket::default();
        assert!(bucket.try_acquire_exclusive_latch());
        assert!(!bucket.try_acquire_shared_latch());
        bucket.release_exclusive_latch();
    }

    #[test]
    fn promote_shared_to_exclusive() {
        let bucket = HashBucket::default();
        assert!(bucket.try_acquire_shared_latch());
        assert!(bucket.try_promote_latch());
        assert!(bucket.is_latched_exclusive(Ordering::Acquire));
        assert_eq!(bucket.shared_latch_count(Ordering::Acquire), 0);
        bucket.release_exclusive_latch();
        assert!(!bucket.is_latched(Ordering::Acquire));
    }
}
