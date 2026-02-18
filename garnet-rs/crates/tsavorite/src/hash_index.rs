//! Hash-index table structure and hash-to-bucket/tag mapping.
//!
//! See [Doc 03 Sections 2.1-2.3] for bucket-index and tag extraction semantics.

use crate::hash_bucket::{HashBucket, HASH_BUCKET_DATA_ENTRY_COUNT};
use crate::hash_bucket_entry::HashBucketEntry;
use core::sync::atomic::Ordering;

pub const HASH_TAG_BITS: u32 = 14;
pub const HASH_TAG_SHIFT: u32 = 64 - HASH_TAG_BITS;
pub const HASH_TAG_MASK: u64 = (1u64 << HASH_TAG_BITS) - 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HashLocation {
    pub bucket_index: u64,
    pub tag: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HashIndexError {
    InvalidSizeBits { size_bits: u8 },
}

impl core::fmt::Display for HashIndexError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::InvalidSizeBits { size_bits } => {
                write!(f, "invalid index size bits {} (expected 1..=30)", size_bits)
            }
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

        assert_eq!(
            index.find_tag_in_primary_bucket(hash, Ordering::Acquire),
            Some(0)
        );
    }

    #[test]
    fn skips_tentative_entries_during_tag_lookup() {
        let mut index = HashIndex::with_size_bits(2).unwrap();
        let hash = (23u64 << HASH_TAG_SHIFT) | 2;

        let bucket = index.bucket_mut(2).unwrap();
        let word = HashBucketEntry::pack(23, 5678, true, false).unwrap();
        bucket.entry(0).unwrap().store(word, Ordering::Release);

        assert_eq!(
            index.find_tag_in_primary_bucket(hash, Ordering::Acquire),
            None
        );
    }
}
