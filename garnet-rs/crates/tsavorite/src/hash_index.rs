//! Hash-index table structure and hash-to-bucket/tag mapping.
//!
//! See [Doc 03 Sections 2.1-2.3] for bucket-index and tag extraction semantics.

use crate::hash_bucket::{HashBucket, HASH_BUCKET_DATA_ENTRY_COUNT};
use crate::hash_bucket_entry::{HashBucketEntry, HashBucketEntryError};
use core::sync::atomic::Ordering;

pub const HASH_TAG_BITS: u32 = 14;
pub const HASH_TAG_SHIFT: u32 = 64 - HASH_TAG_BITS;
pub const HASH_TAG_MASK: u64 = (1u64 << HASH_TAG_BITS) - 1;
pub const TEMP_INVALID_ADDRESS: u64 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HashLocation {
    pub bucket_index: u64,
    pub tag: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FindOrCreateTagStatus {
    FoundExisting,
    Created,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FindOrCreateTagResult {
    pub bucket_index: u64,
    pub slot: usize,
    pub status: FindOrCreateTagStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HashIndexError {
    InvalidSizeBits { size_bits: u8 },
    BucketFullNeedsOverflow { bucket_index: u64 },
    InvalidTentativeEntry(HashBucketEntryError),
}

impl core::fmt::Display for HashIndexError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::InvalidSizeBits { size_bits } => {
                write!(f, "invalid index size bits {} (expected 1..=30)", size_bits)
            }
            Self::BucketFullNeedsOverflow { bucket_index } => {
                write!(
                    f,
                    "bucket {} has no free entry slots; overflow allocation required",
                    bucket_index
                )
            }
            Self::InvalidTentativeEntry(inner) => inner.fmt(f),
        }
    }
}

impl std::error::Error for HashIndexError {}

/// Fixed-size power-of-two hash-index bucket array.
pub struct HashIndex {
    size_bits: u8,
    size_mask: u64,
    buckets: Box<[HashBucket]>,
}

impl HashIndex {
    pub fn with_size_bits(size_bits: u8) -> Result<Self, HashIndexError> {
        if !(1..=30).contains(&size_bits) {
            return Err(HashIndexError::InvalidSizeBits { size_bits });
        }

        let bucket_count = 1usize << size_bits;
        let mut buckets = Vec::with_capacity(bucket_count);
        buckets.resize_with(bucket_count, HashBucket::default);

        Ok(Self {
            size_bits,
            size_mask: (bucket_count as u64) - 1,
            buckets: buckets.into_boxed_slice(),
        })
    }

    #[inline]
    pub const fn size_bits(&self) -> u8 {
        self.size_bits
    }

    #[inline]
    pub const fn size_mask(&self) -> u64 {
        self.size_mask
    }

    #[inline]
    pub fn bucket_count(&self) -> usize {
        self.buckets.len()
    }

    #[inline]
    pub const fn tag_from_hash(hash: u64) -> u16 {
        ((hash >> HASH_TAG_SHIFT) & HASH_TAG_MASK) as u16
    }

    #[inline]
    pub fn bucket_index_from_hash(&self, hash: u64) -> u64 {
        hash & self.size_mask
    }

    #[inline]
    pub fn locate_hash(&self, hash: u64) -> HashLocation {
        HashLocation {
            bucket_index: self.bucket_index_from_hash(hash),
            tag: Self::tag_from_hash(hash),
        }
    }

    #[inline]
    pub fn bucket(&self, bucket_index: u64) -> Option<&HashBucket> {
        self.buckets.get(bucket_index as usize)
    }

    #[inline]
    pub fn bucket_mut(&mut self, bucket_index: u64) -> Option<&mut HashBucket> {
        self.buckets.get_mut(bucket_index as usize)
    }

    /// Scans only the primary bucket for a matching non-tentative tag.
    #[inline]
    pub fn find_tag_in_primary_bucket(&self, hash: u64, ordering: Ordering) -> Option<usize> {
        let location = self.locate_hash(hash);
        let bucket = self.bucket(location.bucket_index)?;

        for slot in 0..HASH_BUCKET_DATA_ENTRY_COUNT {
            let entry = bucket.entry(slot)?;
            let word = entry.load(ordering);
            if word == HashBucketEntry::EMPTY_WORD {
                continue;
            }

            if HashBucketEntry::tag_from_word(word) == location.tag
                && !HashBucketEntry::tentative_from_word(word)
            {
                return Some(slot);
            }
        }

        None
    }

    #[inline]
    pub fn find_tag(&self, hash: u64) -> Option<usize> {
        self.find_tag_in_primary_bucket(hash, Ordering::Acquire)
    }

    /// Finds an existing non-tentative tag slot or inserts a new slot via CAS.
    ///
    /// This implementation currently operates on the primary bucket only; callers should
    /// handle `BucketFullNeedsOverflow` by invoking overflow-bucket logic.
    pub fn find_or_create_tag(
        &self,
        hash: u64,
        begin_address: u64,
    ) -> Result<FindOrCreateTagResult, HashIndexError> {
        let location = self.locate_hash(hash);
        let bucket = self
            .bucket(location.bucket_index)
            .expect("bucket index derived from size_mask must be in bounds");

        loop {
            match self.find_tag_or_free_slot_in_primary_bucket(
                bucket,
                location.tag,
                begin_address,
                Ordering::Acquire,
            ) {
                FindTagOrFreeSlot::Found(slot) => {
                    return Ok(FindOrCreateTagResult {
                        bucket_index: location.bucket_index,
                        slot,
                        status: FindOrCreateTagStatus::FoundExisting,
                    });
                }
                FindTagOrFreeSlot::NoFree => {
                    return Err(HashIndexError::BucketFullNeedsOverflow {
                        bucket_index: location.bucket_index,
                    });
                }
                FindTagOrFreeSlot::Free(free_slot) => {
                    let tentative_word =
                        HashBucketEntry::pack(location.tag, TEMP_INVALID_ADDRESS, true, false)
                            .map_err(HashIndexError::InvalidTentativeEntry)?;
                    let entry = bucket
                        .entry(free_slot)
                        .expect("free slot from scan must be valid");

                    if entry
                        .compare_exchange_word(
                            HashBucketEntry::EMPTY_WORD,
                            tentative_word,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_err()
                    {
                        continue;
                    }

                    if self
                        .find_other_slot_for_tag_maybe_tentative(
                            bucket,
                            location.tag,
                            free_slot,
                            Ordering::Acquire,
                        )
                        .is_some()
                    {
                        entry.store(HashBucketEntry::EMPTY_WORD, Ordering::Release);
                        continue;
                    }

                    let committed_word = HashBucketEntry::with_tentative(tentative_word, false);
                    entry.store(committed_word, Ordering::Release);
                    return Ok(FindOrCreateTagResult {
                        bucket_index: location.bucket_index,
                        slot: free_slot,
                        status: FindOrCreateTagStatus::Created,
                    });
                }
            }
        }
    }

    fn find_tag_or_free_slot_in_primary_bucket(
        &self,
        bucket: &HashBucket,
        tag: u16,
        begin_address: u64,
        ordering: Ordering,
    ) -> FindTagOrFreeSlot {
        let mut free_slot = None;

        for slot in 0..HASH_BUCKET_DATA_ENTRY_COUNT {
            let entry = bucket
                .entry(slot)
                .expect("slot index bounded by HASH_BUCKET_DATA_ENTRY_COUNT");
            let word = entry.load(ordering);

            if word == HashBucketEntry::EMPTY_WORD {
                if free_slot.is_none() {
                    free_slot = Some(slot);
                }
                continue;
            }

            let address = HashBucketEntry::address_from_word(word);
            if address < begin_address && address != TEMP_INVALID_ADDRESS {
                if entry
                    .compare_exchange_word(
                        word,
                        HashBucketEntry::EMPTY_WORD,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
                {
                    if free_slot.is_none() {
                        free_slot = Some(slot);
                    }
                    continue;
                }
            }

            if HashBucketEntry::tag_from_word(word) == tag
                && !HashBucketEntry::tentative_from_word(word)
            {
                return FindTagOrFreeSlot::Found(slot);
            }
        }

        match free_slot {
            Some(slot) => FindTagOrFreeSlot::Free(slot),
            None => FindTagOrFreeSlot::NoFree,
        }
    }

    fn find_other_slot_for_tag_maybe_tentative(
        &self,
        bucket: &HashBucket,
        tag: u16,
        except_slot: usize,
        ordering: Ordering,
    ) -> Option<usize> {
        for slot in 0..HASH_BUCKET_DATA_ENTRY_COUNT {
            if slot == except_slot {
                continue;
            }

            let entry = bucket
                .entry(slot)
                .expect("slot index bounded by HASH_BUCKET_DATA_ENTRY_COUNT");
            let word = entry.load(ordering);
            if word == HashBucketEntry::EMPTY_WORD {
                continue;
            }

            if HashBucketEntry::tag_from_word(word) == tag {
                return Some(slot);
            }
        }

        None
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FindTagOrFreeSlot {
    Found(usize),
    Free(usize),
    NoFree,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::HashBucketEntry;

    #[test]
    fn initializes_power_of_two_bucket_array() {
        let index = HashIndex::with_size_bits(4).unwrap();
        assert_eq!(index.bucket_count(), 16);
        assert_eq!(index.size_mask(), 15);
        assert_eq!(index.size_bits(), 4);
    }

    #[test]
    fn rejects_invalid_size_bits() {
        assert!(matches!(
            HashIndex::with_size_bits(0),
            Err(HashIndexError::InvalidSizeBits { .. })
        ));
        assert!(matches!(
            HashIndex::with_size_bits(31),
            Err(HashIndexError::InvalidSizeBits { .. })
        ));
    }

    #[test]
    fn extracts_bucket_index_and_tag_from_hash() {
        let index = HashIndex::with_size_bits(8).unwrap();
        let tag: u16 = 0x2abc & (HASH_TAG_MASK as u16);
        let hash = ((tag as u64) << HASH_TAG_SHIFT) | 0x5a;

        let location = index.locate_hash(hash);
        assert_eq!(location.bucket_index, 0x5a);
        assert_eq!(location.tag, tag);
    }

    #[test]
    fn finds_matching_non_tentative_tag_in_primary_bucket() {
        let mut index = HashIndex::with_size_bits(2).unwrap();
        let hash = (17u64 << HASH_TAG_SHIFT) | 1;

        let bucket = index.bucket_mut(1).unwrap();
        let word = HashBucketEntry::pack(17, 1234, false, false).unwrap();
        bucket.entry(0).unwrap().store(word, Ordering::Release);

        assert_eq!(index.find_tag(hash), Some(0));
    }

    #[test]
    fn skips_tentative_entries_during_tag_lookup() {
        let mut index = HashIndex::with_size_bits(2).unwrap();
        let hash = (23u64 << HASH_TAG_SHIFT) | 2;

        let bucket = index.bucket_mut(2).unwrap();
        let word = HashBucketEntry::pack(23, 5678, true, false).unwrap();
        bucket.entry(0).unwrap().store(word, Ordering::Release);

        assert_eq!(index.find_tag(hash), None);
    }

    #[test]
    fn find_or_create_tag_creates_then_finds_existing_slot() {
        let index = HashIndex::with_size_bits(2).unwrap();
        let hash = (71u64 << HASH_TAG_SHIFT) | 1;

        let created = index.find_or_create_tag(hash, 0).unwrap();
        assert_eq!(created.bucket_index, 1);
        assert_eq!(created.status, FindOrCreateTagStatus::Created);
        assert_eq!(index.find_tag(hash), Some(created.slot));

        let found = index.find_or_create_tag(hash, 0).unwrap();
        assert_eq!(found.bucket_index, 1);
        assert_eq!(found.slot, created.slot);
        assert_eq!(found.status, FindOrCreateTagStatus::FoundExisting);
    }

    #[test]
    fn find_or_create_reclaims_truncated_entry() {
        let mut index = HashIndex::with_size_bits(2).unwrap();
        let bucket = index.bucket_mut(3).unwrap();

        for slot in 0..HASH_BUCKET_DATA_ENTRY_COUNT {
            if slot == 2 {
                continue;
            }
            let live_word =
                HashBucketEntry::pack((slot + 10) as u16, (1000 + slot) as u64, false, false)
                    .unwrap();
            bucket.entry(slot).unwrap().store(live_word, Ordering::Release);
        }

        let stale_word = HashBucketEntry::pack(1, 20, false, false).unwrap();
        bucket
            .entry(2)
            .unwrap()
            .store(stale_word, Ordering::Release);

        let hash = (42u64 << HASH_TAG_SHIFT) | 3;
        let created = index.find_or_create_tag(hash, 100).unwrap();

        assert_eq!(created.slot, 2);
        assert_eq!(created.status, FindOrCreateTagStatus::Created);
        assert_eq!(index.find_tag(hash), Some(2));
    }

    #[test]
    fn find_or_create_returns_overflow_needed_when_primary_bucket_full() {
        let mut index = HashIndex::with_size_bits(2).unwrap();
        let bucket = index.bucket_mut(0).unwrap();

        for slot in 0..HASH_BUCKET_DATA_ENTRY_COUNT {
            let word = HashBucketEntry::pack((slot + 1) as u16, (1000 + slot) as u64, false, false)
                .unwrap();
            bucket.entry(slot).unwrap().store(word, Ordering::Release);
        }

        let hash = (999u64 << HASH_TAG_SHIFT) | 0;
        let err = index.find_or_create_tag(hash, 0).unwrap_err();
        assert!(matches!(
            err,
            HashIndexError::BucketFullNeedsOverflow { bucket_index: 0 }
        ));
    }
}
