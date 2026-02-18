//! HashBucketEntry: packed 8-byte hash-index entry with CAS update primitives.
//!
//! See [Doc 03 Section 2.2] for field layout.

use core::sync::atomic::{AtomicU64, Ordering};

pub const ADDRESS_BITS: u32 = 48;
pub const ADDRESS_MASK: u64 = (1u64 << ADDRESS_BITS) - 1;

pub const READ_CACHE_BIT_SHIFT: u32 = 47;
pub const READ_CACHE_BIT_MASK: u64 = 1u64 << READ_CACHE_BIT_SHIFT;

pub const TAG_BITS: u32 = 14;
pub const TAG_SHIFT: u32 = 62 - TAG_BITS;
pub const TAG_MASK: u64 = (1u64 << TAG_BITS) - 1;
pub const TAG_POSITION_MASK: u64 = TAG_MASK << TAG_SHIFT;

pub const PENDING_BIT_SHIFT: u32 = 62;
pub const PENDING_BIT_MASK: u64 = 1u64 << PENDING_BIT_SHIFT;

pub const TENTATIVE_BIT_SHIFT: u32 = 63;
pub const TENTATIVE_BIT_MASK: u64 = 1u64 << TENTATIVE_BIT_SHIFT;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HashBucketEntryError {
    TagOutOfRange { tag: u16, max_tag: u16 },
    AddressOutOfRange { address: u64, max_address: u64 },
}

impl core::fmt::Display for HashBucketEntryError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::TagOutOfRange { tag, max_tag } => {
                write!(f, "tag {} exceeds max {}", tag, max_tag)
            }
            Self::AddressOutOfRange {
                address,
                max_address,
            } => {
                write!(
                    f,
                    "address {} exceeds {}-bit max {}",
                    address, ADDRESS_BITS, max_address
                )
            }
        }
    }
}

impl std::error::Error for HashBucketEntryError {}

/// Atomic packed hash-bucket entry.
///
/// Layout: `[tentative:1][pending:1][tag:14][address:48]`.
#[repr(transparent)]
pub struct HashBucketEntry {
    word: AtomicU64,
}

impl HashBucketEntry {
    pub const EMPTY_WORD: u64 = 0;

    #[inline]
    pub const fn max_tag() -> u16 {
        TAG_MASK as u16
    }

    #[inline]
    pub const fn max_address() -> u64 {
        ADDRESS_MASK
    }

    pub fn pack(
        tag: u16,
        address: u64,
        tentative: bool,
        pending: bool,
    ) -> Result<u64, HashBucketEntryError> {
        if u64::from(tag) > TAG_MASK {
            return Err(HashBucketEntryError::TagOutOfRange {
                tag,
                max_tag: Self::max_tag(),
            });
        }

        if address > ADDRESS_MASK {
            return Err(HashBucketEntryError::AddressOutOfRange {
                address,
                max_address: Self::max_address(),
            });
        }

        let mut word = (address & ADDRESS_MASK) | ((u64::from(tag) & TAG_MASK) << TAG_SHIFT);

        if tentative {
            word |= TENTATIVE_BIT_MASK;
        }
        if pending {
            word |= PENDING_BIT_MASK;
        }

        Ok(word)
    }

    #[inline]
    pub const fn address_from_word(word: u64) -> u64 {
        word & ADDRESS_MASK
    }

    #[inline]
    pub const fn tag_from_word(word: u64) -> u16 {
        ((word & TAG_POSITION_MASK) >> TAG_SHIFT) as u16
    }

    #[inline]
    pub const fn tentative_from_word(word: u64) -> bool {
        (word & TENTATIVE_BIT_MASK) != 0
    }

    #[inline]
    pub const fn pending_from_word(word: u64) -> bool {
        (word & PENDING_BIT_MASK) != 0
    }

    #[inline]
    pub const fn read_cache_from_word(word: u64) -> bool {
        (word & READ_CACHE_BIT_MASK) != 0
    }

    #[inline]
    pub const fn with_address(word: u64, address: u64) -> Result<u64, HashBucketEntryError> {
        if address > ADDRESS_MASK {
            return Err(HashBucketEntryError::AddressOutOfRange {
                address,
                max_address: Self::max_address(),
            });
        }

        Ok((word & !ADDRESS_MASK) | (address & ADDRESS_MASK))
    }

    #[inline]
    pub const fn with_tentative(word: u64, tentative: bool) -> u64 {
        if tentative {
            word | TENTATIVE_BIT_MASK
        } else {
            word & !TENTATIVE_BIT_MASK
        }
    }

    #[inline]
    pub const fn with_pending(word: u64, pending: bool) -> u64 {
        if pending {
            word | PENDING_BIT_MASK
        } else {
            word & !PENDING_BIT_MASK
        }
    }

    #[inline]
    pub fn from_parts(
        tag: u16,
        address: u64,
        tentative: bool,
        pending: bool,
    ) -> Result<Self, HashBucketEntryError> {
        let word = Self::pack(tag, address, tentative, pending)?;
        Ok(Self {
            word: AtomicU64::new(word),
        })
    }

    #[inline]
    pub const fn from_packed(word: u64) -> Self {
        Self {
            word: AtomicU64::new(word),
        }
    }

    #[inline]
    pub fn load(&self, ordering: Ordering) -> u64 {
        self.word.load(ordering)
    }

    #[inline]
    pub fn store(&self, word: u64, ordering: Ordering) {
        self.word.store(word, ordering);
    }

    #[inline]
    pub fn compare_exchange_word(
        &self,
        current: u64,
        new: u64,
        success: Ordering,
        failure: Ordering,
    ) -> Result<u64, u64> {
        self.word.compare_exchange(current, new, success, failure)
    }

    #[inline]
    pub fn compare_exchange_parts(
        &self,
        current: u64,
        tag: u16,
        address: u64,
        tentative: bool,
        pending: bool,
        success: Ordering,
        failure: Ordering,
    ) -> Result<u64, HashBucketEntryCasError> {
        let new = Self::pack(tag, address, tentative, pending)
            .map_err(HashBucketEntryCasError::InvalidNewWord)?;

        self.compare_exchange_word(current, new, success, failure)
            .map_err(HashBucketEntryCasError::CompareExchangeFailed)
    }

    #[inline]
    pub fn address(&self, ordering: Ordering) -> u64 {
        Self::address_from_word(self.load(ordering))
    }

    #[inline]
    pub fn tag(&self, ordering: Ordering) -> u16 {
        Self::tag_from_word(self.load(ordering))
    }

    #[inline]
    pub fn tentative(&self, ordering: Ordering) -> bool {
        Self::tentative_from_word(self.load(ordering))
    }

    #[inline]
    pub fn pending(&self, ordering: Ordering) -> bool {
        Self::pending_from_word(self.load(ordering))
    }

    #[inline]
    pub fn read_cache(&self, ordering: Ordering) -> bool {
        Self::read_cache_from_word(self.load(ordering))
    }

    #[inline]
    pub fn is_empty(&self, ordering: Ordering) -> bool {
        self.load(ordering) == Self::EMPTY_WORD
    }
}

impl Default for HashBucketEntry {
    fn default() -> Self {
        Self::from_packed(Self::EMPTY_WORD)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HashBucketEntryCasError {
    InvalidNewWord(HashBucketEntryError),
    CompareExchangeFailed(u64),
}

impl core::fmt::Display for HashBucketEntryCasError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::InvalidNewWord(inner) => inner.fmt(f),
            Self::CompareExchangeFailed(actual) => {
                write!(
                    f,
                    "compare_exchange failed; observed current word {}",
                    actual
                )
            }
        }
    }
}

impl std::error::Error for HashBucketEntryCasError {}

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::size_of;

    #[test]
    fn hash_bucket_entry_is_8_bytes() {
        assert_eq!(size_of::<HashBucketEntry>(), 8);
    }

    #[test]
    fn pack_and_unpack_roundtrip() {
        let word = HashBucketEntry::pack(0x1234, 0x0000_abcd_ef12, true, true).unwrap();

        assert_eq!(HashBucketEntry::tag_from_word(word), 0x1234);
        assert_eq!(HashBucketEntry::address_from_word(word), 0x0000_abcd_ef12);
        assert!(HashBucketEntry::tentative_from_word(word));
        assert!(HashBucketEntry::pending_from_word(word));
    }

    #[test]
    fn read_cache_is_part_of_address() {
        let address = READ_CACHE_BIT_MASK | 0x0123;
        let word = HashBucketEntry::pack(7, address, false, false).unwrap();

        assert_eq!(HashBucketEntry::address_from_word(word), address);
        assert!(HashBucketEntry::read_cache_from_word(word));
    }

    #[test]
    fn compare_exchange_parts_updates_entry() {
        let entry = HashBucketEntry::from_parts(1, 10, false, false).unwrap();
        let current = entry.load(Ordering::Acquire);

        entry
            .compare_exchange_parts(
                current,
                2,
                20,
                true,
                true,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .unwrap();

        assert_eq!(entry.tag(Ordering::Acquire), 2);
        assert_eq!(entry.address(Ordering::Acquire), 20);
        assert!(entry.tentative(Ordering::Acquire));
        assert!(entry.pending(Ordering::Acquire));
    }

    #[test]
    fn reject_out_of_range_inputs() {
        let err = HashBucketEntry::pack(
            (HashBucketEntry::max_tag() as u32 + 1) as u16,
            0,
            false,
            false,
        )
        .unwrap_err();
        assert!(matches!(err, HashBucketEntryError::TagOutOfRange { .. }));

        let err = HashBucketEntry::pack(0, ADDRESS_MASK + 1, false, false).unwrap_err();
        assert!(matches!(
            err,
            HashBucketEntryError::AddressOutOfRange { .. }
        ));
    }
}
