//! Hash-index table structure and hash-to-bucket/tag mapping.
//!
//! See [Doc 03 Sections 2.1-2.7] for bucket-index, tag extraction, and overflow-chain behavior.

use crate::hash_bucket::HASH_BUCKET_DATA_ENTRY_COUNT;
use crate::hash_bucket::HashBucket;
use crate::hash_bucket_entry::ADDRESS_MASK;
use crate::hash_bucket_entry::HashBucketEntry;
use crate::hash_bucket_entry::HashBucketEntryError;
use crate::overflow_bucket_allocator::OverflowBucketAllocator;
use crate::overflow_bucket_allocator::OverflowBucketAllocatorError;
use crate::overflow_bucket_allocator::OverflowBucketHandle;
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
    pub overflow_bucket_address: Option<u64>,
    pub slot: usize,
    pub status: FindOrCreateTagStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HashEntryLocation {
    pub bucket_index: u64,
    pub overflow_bucket_address: Option<u64>,
    pub slot: usize,
    pub word: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HashIndexError {
    InvalidSizeBits { size_bits: u8 },
    InvalidTentativeEntry(HashBucketEntryError),
    OverflowAllocator(OverflowBucketAllocatorError),
}

impl core::fmt::Display for HashIndexError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::InvalidSizeBits { size_bits } => {
                write!(f, "invalid index size bits {} (expected 1..=30)", size_bits)
            }
            Self::InvalidTentativeEntry(inner) => inner.fmt(f),
            Self::OverflowAllocator(inner) => inner.fmt(f),
        }
    }
}

impl std::error::Error for HashIndexError {}

/// Fixed-size power-of-two hash-index bucket array with overflow-bucket support.
pub struct HashIndex {
    size_bits: u8,
    size_mask: u64,
    buckets: Box<[HashBucket]>,
    overflow_allocator: OverflowBucketAllocator,
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
            overflow_allocator: OverflowBucketAllocator::new(),
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
    pub fn overflow_allocator(&self) -> &OverflowBucketAllocator {
        &self.overflow_allocator
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
        self.find_tag_in_bucket(bucket, location.tag, ordering)
    }

    /// Finds a matching non-tentative tag, traversing overflow buckets if needed.
    pub fn find_tag(&self, hash: u64) -> Option<usize> {
        let location = self.locate_hash(hash);
        let primary_bucket = self.bucket(location.bucket_index)?;
        let mut cursor = BucketCursor::Primary(primary_bucket);

        loop {
            if let Some(slot) =
                self.find_tag_in_bucket(cursor.bucket(), location.tag, Ordering::Acquire)
            {
                return Some(slot);
            }

            let overflow_address = cursor.bucket().overflow_address(Ordering::Acquire);
            if overflow_address == 0 {
                return None;
            }

            cursor = BucketCursor::Overflow(self.overflow_allocator.get(overflow_address).ok()?);
        }
    }

    /// Finds the address currently stored for a matching tag.
    pub fn find_tag_address(&self, hash: u64) -> Option<u64> {
        let location = self.locate_hash(hash);
        let primary_bucket = self.bucket(location.bucket_index)?;
        let mut cursor = BucketCursor::Primary(primary_bucket);

        loop {
            if let Some(word) =
                self.find_tag_word_in_bucket(cursor.bucket(), location.tag, Ordering::Acquire)
            {
                return Some(HashBucketEntry::address_from_word(word));
            }

            let overflow_address = cursor.bucket().overflow_address(Ordering::Acquire);
            if overflow_address == 0 {
                return None;
            }

            cursor = BucketCursor::Overflow(self.overflow_allocator.get(overflow_address).ok()?);
        }
    }

    /// Finds the matching hash entry location (bucket/slot/word).
    pub fn find_tag_entry(&self, hash: u64) -> Option<HashEntryLocation> {
        let location = self.locate_hash(hash);
        let primary_bucket = self.bucket(location.bucket_index)?;
        let mut cursor = BucketCursor::Primary(primary_bucket);

        loop {
            if let Some((slot, word)) = self.find_tag_slot_and_word_in_bucket(
                cursor.bucket(),
                location.tag,
                Ordering::Acquire,
            ) {
                return Some(HashEntryLocation {
                    bucket_index: location.bucket_index,
                    overflow_bucket_address: cursor.overflow_bucket_address(),
                    slot,
                    word,
                });
            }

            let overflow_address = cursor.bucket().overflow_address(Ordering::Acquire);
            if overflow_address == 0 {
                return None;
            }

            cursor = BucketCursor::Overflow(self.overflow_allocator.get(overflow_address).ok()?);
        }
    }

    /// CAS-updates the address bits of an existing entry location.
    pub fn compare_exchange_entry_address(
        &self,
        location: &HashEntryLocation,
        new_address: u64,
    ) -> Result<bool, HashIndexError> {
        let desired_word = HashBucketEntry::with_address(location.word, new_address)
            .map_err(HashIndexError::InvalidTentativeEntry)?;

        let updated = match location.overflow_bucket_address {
            Some(overflow_address) => {
                let handle = self
                    .overflow_allocator
                    .get(overflow_address)
                    .map_err(HashIndexError::OverflowAllocator)?;
                let entry = handle
                    .bucket()
                    .entry(location.slot)
                    .expect("slot index bounded by HASH_BUCKET_DATA_ENTRY_COUNT");
                entry
                    .compare_exchange_word(
                        location.word,
                        desired_word,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
            }
            None => {
                let entry = self
                    .bucket(location.bucket_index)
                    .expect("bucket index derived from size_mask must be in bounds")
                    .entry(location.slot)
                    .expect("slot index bounded by HASH_BUCKET_DATA_ENTRY_COUNT");
                entry
                    .compare_exchange_word(
                        location.word,
                        desired_word,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
            }
        };

        Ok(updated)
    }

    /// Finds an existing non-tentative tag slot or inserts a new slot via CAS.
    ///
    /// This includes overflow-chain traversal and overflow-bucket allocation.
    pub fn find_or_create_tag(
        &self,
        hash: u64,
        begin_address: u64,
    ) -> Result<FindOrCreateTagResult, HashIndexError> {
        let location = self.locate_hash(hash);
        let primary_bucket = self
            .bucket(location.bucket_index)
            .expect("bucket index derived from size_mask must be in bounds");

        'restart: loop {
            let mut first_free: Option<FreeSlot<'_>> = None;
            let mut cursor = BucketCursor::Primary(primary_bucket);

            loop {
                match self.find_tag_or_free_slot_in_bucket(
                    cursor.bucket(),
                    location.tag,
                    begin_address,
                    Ordering::Acquire,
                ) {
                    FindTagOrFreeSlot::Found(slot) => {
                        return Ok(self.make_find_or_create_result(
                            location.bucket_index,
                            &cursor,
                            slot,
                            FindOrCreateTagStatus::FoundExisting,
                        ));
                    }
                    FindTagOrFreeSlot::Free(slot) => {
                        if first_free.is_none() {
                            first_free = Some(FreeSlot {
                                cursor: cursor.clone(),
                                slot,
                            });
                        }
                    }
                    FindTagOrFreeSlot::NoFree => {}
                }

                let overflow_address = cursor.bucket().overflow_address(Ordering::Acquire);
                if overflow_address == 0 {
                    let free_slot = if let Some(free_slot) = first_free.take() {
                        free_slot
                    } else {
                        let overflow_handle = self.allocate_and_link_overflow_bucket(&cursor)?;
                        FreeSlot {
                            cursor: BucketCursor::Overflow(overflow_handle),
                            slot: 0,
                        }
                    };

                    match self.try_create_tag_in_free_slot(
                        location.bucket_index,
                        location.tag,
                        &free_slot,
                    )? {
                        Some(result) => return Ok(result),
                        None => continue 'restart,
                    }
                }

                let overflow_handle = self
                    .overflow_allocator
                    .get(overflow_address)
                    .map_err(HashIndexError::OverflowAllocator)?;
                cursor = BucketCursor::Overflow(overflow_handle);
            }
        }
    }

    fn make_find_or_create_result(
        &self,
        bucket_index: u64,
        cursor: &BucketCursor<'_>,
        slot: usize,
        status: FindOrCreateTagStatus,
    ) -> FindOrCreateTagResult {
        FindOrCreateTagResult {
            bucket_index,
            overflow_bucket_address: cursor.overflow_bucket_address(),
            slot,
            status,
        }
    }

    fn try_create_tag_in_free_slot(
        &self,
        bucket_index: u64,
        tag: u16,
        free_slot: &FreeSlot<'_>,
    ) -> Result<Option<FindOrCreateTagResult>, HashIndexError> {
        let tentative_word = HashBucketEntry::pack(tag, TEMP_INVALID_ADDRESS, true, false)
            .map_err(HashIndexError::InvalidTentativeEntry)?;
        let entry = free_slot
            .cursor
            .bucket()
            .entry(free_slot.slot)
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
            return Ok(None);
        }

        if self.find_other_slot_for_tag_maybe_tentative_in_chain(
            bucket_index,
            tag,
            free_slot.cursor.as_ptr(),
            free_slot.slot,
            Ordering::Acquire,
        )? {
            entry.store(HashBucketEntry::EMPTY_WORD, Ordering::Release);
            return Ok(None);
        }

        let committed_word = HashBucketEntry::with_tentative(tentative_word, false);
        entry.store(committed_word, Ordering::Release);
        Ok(Some(self.make_find_or_create_result(
            bucket_index,
            &free_slot.cursor,
            free_slot.slot,
            FindOrCreateTagStatus::Created,
        )))
    }

    fn find_other_slot_for_tag_maybe_tentative_in_chain(
        &self,
        bucket_index: u64,
        tag: u16,
        except_bucket_ptr: *const HashBucket,
        except_slot: usize,
        ordering: Ordering,
    ) -> Result<bool, HashIndexError> {
        let primary_bucket = self
            .bucket(bucket_index)
            .expect("bucket index derived from size_mask must be in bounds");
        let mut cursor = BucketCursor::Primary(primary_bucket);

        loop {
            for slot in 0..HASH_BUCKET_DATA_ENTRY_COUNT {
                let entry = cursor
                    .bucket()
                    .entry(slot)
                    .expect("slot index bounded by HASH_BUCKET_DATA_ENTRY_COUNT");
                let word = entry.load(ordering);
                if word == HashBucketEntry::EMPTY_WORD {
                    continue;
                }

                if HashBucketEntry::tag_from_word(word) == tag {
                    if cursor.as_ptr() == except_bucket_ptr && slot == except_slot {
                        continue;
                    }
                    return Ok(true);
                }
            }

            let overflow_address = cursor.bucket().overflow_address(Ordering::Acquire);
            if overflow_address == 0 {
                return Ok(false);
            }

            let overflow_handle = self
                .overflow_allocator
                .get(overflow_address)
                .map_err(HashIndexError::OverflowAllocator)?;
            cursor = BucketCursor::Overflow(overflow_handle);
        }
    }

    fn allocate_and_link_overflow_bucket(
        &self,
        cursor: &BucketCursor<'_>,
    ) -> Result<OverflowBucketHandle, HashIndexError> {
        loop {
            let current_word = cursor.bucket().overflow_word(Ordering::Acquire);
            let current_address = current_word & ADDRESS_MASK;
            if current_address != 0 {
                return self
                    .overflow_allocator
                    .get(current_address)
                    .map_err(HashIndexError::OverflowAllocator);
            }

            let new_address = self
                .overflow_allocator
                .allocate()
                .map_err(HashIndexError::OverflowAllocator)?;
            let new_word = (current_word & !ADDRESS_MASK) | new_address;

            match cursor.bucket().compare_exchange_overflow_word(
                current_word,
                new_word,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return self
                        .overflow_allocator
                        .get(new_address)
                        .map_err(HashIndexError::OverflowAllocator);
                }
                Err(observed_word) => {
                    self.overflow_allocator
                        .free(new_address)
                        .map_err(HashIndexError::OverflowAllocator)?;

                    let observed_address = observed_word & ADDRESS_MASK;
                    if observed_address != 0 {
                        return self
                            .overflow_allocator
                            .get(observed_address)
                            .map_err(HashIndexError::OverflowAllocator);
                    }
                }
            }
        }
    }

    fn find_tag_in_bucket(
        &self,
        bucket: &HashBucket,
        tag: u16,
        ordering: Ordering,
    ) -> Option<usize> {
        for slot in 0..HASH_BUCKET_DATA_ENTRY_COUNT {
            let entry = bucket
                .entry(slot)
                .expect("slot index bounded by HASH_BUCKET_DATA_ENTRY_COUNT");
            let word = entry.load(ordering);
            if word == HashBucketEntry::EMPTY_WORD {
                continue;
            }

            if HashBucketEntry::tag_from_word(word) == tag
                && !HashBucketEntry::tentative_from_word(word)
            {
                return Some(slot);
            }
        }
        None
    }

    fn find_tag_word_in_bucket(
        &self,
        bucket: &HashBucket,
        tag: u16,
        ordering: Ordering,
    ) -> Option<u64> {
        for slot in 0..HASH_BUCKET_DATA_ENTRY_COUNT {
            let entry = bucket
                .entry(slot)
                .expect("slot index bounded by HASH_BUCKET_DATA_ENTRY_COUNT");
            let word = entry.load(ordering);
            if word == HashBucketEntry::EMPTY_WORD {
                continue;
            }

            if HashBucketEntry::tag_from_word(word) == tag
                && !HashBucketEntry::tentative_from_word(word)
            {
                return Some(word);
            }
        }
        None
    }

    fn find_tag_slot_and_word_in_bucket(
        &self,
        bucket: &HashBucket,
        tag: u16,
        ordering: Ordering,
    ) -> Option<(usize, u64)> {
        for slot in 0..HASH_BUCKET_DATA_ENTRY_COUNT {
            let entry = bucket
                .entry(slot)
                .expect("slot index bounded by HASH_BUCKET_DATA_ENTRY_COUNT");
            let word = entry.load(ordering);
            if word == HashBucketEntry::EMPTY_WORD {
                continue;
            }

            if HashBucketEntry::tag_from_word(word) == tag
                && !HashBucketEntry::tentative_from_word(word)
            {
                return Some((slot, word));
            }
        }
        None
    }

    fn find_tag_or_free_slot_in_bucket(
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
}

#[derive(Clone)]
enum BucketCursor<'a> {
    Primary(&'a HashBucket),
    Overflow(OverflowBucketHandle),
}

impl<'a> BucketCursor<'a> {
    fn bucket(&self) -> &HashBucket {
        match self {
            Self::Primary(bucket) => bucket,
            Self::Overflow(handle) => handle.bucket(),
        }
    }

    fn as_ptr(&self) -> *const HashBucket {
        self.bucket() as *const HashBucket
    }

    fn overflow_bucket_address(&self) -> Option<u64> {
        match self {
            Self::Primary(_) => None,
            Self::Overflow(handle) => Some(handle.logical_address()),
        }
    }
}

#[derive(Clone)]
struct FreeSlot<'a> {
    cursor: BucketCursor<'a>,
    slot: usize,
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
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::thread;

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
        assert_eq!(created.overflow_bucket_address, None);
        assert_eq!(created.status, FindOrCreateTagStatus::Created);
        assert_eq!(index.find_tag(hash), Some(created.slot));

        let found = index.find_or_create_tag(hash, 0).unwrap();
        assert_eq!(found.bucket_index, 1);
        assert_eq!(found.overflow_bucket_address, None);
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
            bucket
                .entry(slot)
                .unwrap()
                .store(live_word, Ordering::Release);
        }

        let stale_word = HashBucketEntry::pack(1, 20, false, false).unwrap();
        bucket
            .entry(2)
            .unwrap()
            .store(stale_word, Ordering::Release);

        let hash = (42u64 << HASH_TAG_SHIFT) | 3;
        let created = index.find_or_create_tag(hash, 100).unwrap();

        assert_eq!(created.slot, 2);
        assert_eq!(created.overflow_bucket_address, None);
        assert_eq!(created.status, FindOrCreateTagStatus::Created);
        assert_eq!(index.find_tag(hash), Some(2));
    }

    #[test]
    fn find_or_create_allocates_overflow_bucket_when_primary_bucket_is_full() {
        let mut index = HashIndex::with_size_bits(2).unwrap();
        {
            let bucket = index.bucket_mut(0).unwrap();
            for slot in 0..HASH_BUCKET_DATA_ENTRY_COUNT {
                let word =
                    HashBucketEntry::pack((slot + 1) as u16, (1000 + slot) as u64, false, false)
                        .unwrap();
                bucket.entry(slot).unwrap().store(word, Ordering::Release);
            }
        }

        let hash = (999u64 << HASH_TAG_SHIFT) | 0;
        let created = index.find_or_create_tag(hash, 0).unwrap();

        assert_eq!(created.bucket_index, 0);
        assert_eq!(created.status, FindOrCreateTagStatus::Created);
        assert!(created.overflow_bucket_address.is_some());

        let bucket = index.bucket(0).unwrap();
        let overflow_address = bucket.overflow_address(Ordering::Acquire);
        assert_eq!(created.overflow_bucket_address, Some(overflow_address));
        assert_ne!(overflow_address, 0);
        assert_eq!(index.find_tag(hash), Some(created.slot));
    }

    #[test]
    fn concurrent_find_or_create_for_same_tag_converges_on_single_slot() {
        let index = Arc::new(HashIndex::with_size_bits(4).unwrap());
        let hash = (77u64 << HASH_TAG_SHIFT) | 3;

        let mut handles = Vec::new();
        for _ in 0..8 {
            let index = Arc::clone(&index);
            handles.push(thread::spawn(move || {
                index.find_or_create_tag(hash, 0).unwrap()
            }));
        }

        let mut results = Vec::new();
        for handle in handles {
            results.push(handle.join().expect("worker thread panicked"));
        }

        let created_count = results
            .iter()
            .filter(|result| result.status == FindOrCreateTagStatus::Created)
            .count();
        assert_eq!(created_count, 1);

        let unique_slots: HashSet<_> = results.iter().map(|result| result.slot).collect();
        assert_eq!(unique_slots.len(), 1);
        assert_eq!(index.find_tag(hash), Some(results[0].slot));
    }

    #[test]
    fn concurrent_colliding_tags_allocate_overflow_chain_and_remain_findable() {
        let index = Arc::new(HashIndex::with_size_bits(2).unwrap());
        let tags: Vec<u16> = (1..=40).collect();

        let mut handles = Vec::new();
        for tag in tags.iter().copied() {
            let index = Arc::clone(&index);
            handles.push(thread::spawn(move || {
                let hash = ((tag as u64) << HASH_TAG_SHIFT) | 0;
                index.find_or_create_tag(hash, 0).unwrap()
            }));
        }

        for handle in handles {
            handle.join().expect("worker thread panicked");
        }

        for tag in tags {
            let hash = ((tag as u64) << HASH_TAG_SHIFT) | 0;
            assert!(
                index.find_tag(hash).is_some(),
                "tag {} missing from collision chain",
                tag
            );
        }

        let mut overflow_addresses = Vec::new();
        let mut current = index.bucket(0).unwrap().overflow_address(Ordering::Acquire);
        while current != 0 {
            overflow_addresses.push(current);
            current = index
                .overflow_allocator()
                .get(current)
                .unwrap()
                .bucket()
                .overflow_address(Ordering::Acquire);
        }

        assert!(!overflow_addresses.is_empty());
        let unique_addresses: HashSet<_> = overflow_addresses.iter().copied().collect();
        assert_eq!(unique_addresses.len(), overflow_addresses.len());
    }

    #[test]
    fn compare_exchange_entry_address_updates_tag_head() {
        let index = HashIndex::with_size_bits(2).unwrap();
        let hash = (88u64 << HASH_TAG_SHIFT) | 1;
        let created = index.find_or_create_tag(hash, 0).unwrap();

        let entry = index.find_tag_entry(hash).unwrap();
        assert_eq!(entry.slot, created.slot);
        assert_eq!(
            HashBucketEntry::address_from_word(entry.word),
            TEMP_INVALID_ADDRESS
        );

        let updated = index.compare_exchange_entry_address(&entry, 1234).unwrap();
        assert!(updated);
        assert_eq!(index.find_tag_address(hash), Some(1234));
    }
}
