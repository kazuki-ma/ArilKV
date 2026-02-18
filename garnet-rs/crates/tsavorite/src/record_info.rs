//! RecordInfo: packed 8-byte metadata header for hybrid-log records.
//!
//! See [Doc 06 Section 2.1] for bit-layout background.

use core::mem::size_of;

pub const RECORD_INFO_LENGTH: usize = size_of::<u64>();

pub const PREVIOUS_ADDRESS_BITS: u32 = 48;
pub const PREVIOUS_ADDRESS_MASK: u64 = (1u64 << PREVIOUS_ADDRESS_BITS) - 1;

const LEFTOVER_BIT_COUNT: u32 = 7;
const HAS_EXPIRATION_BIT_OFFSET: u32 = PREVIOUS_ADDRESS_BITS + LEFTOVER_BIT_COUNT - 1;

const TOMBSTONE_BIT_OFFSET: u32 = PREVIOUS_ADDRESS_BITS + LEFTOVER_BIT_COUNT;
const VALID_BIT_OFFSET: u32 = TOMBSTONE_BIT_OFFSET + 1;
const SEALED_BIT_OFFSET: u32 = VALID_BIT_OFFSET + 1;
const ETAG_BIT_OFFSET: u32 = SEALED_BIT_OFFSET + 1;
const DIRTY_BIT_OFFSET: u32 = ETAG_BIT_OFFSET + 1;
const FILLER_BIT_OFFSET: u32 = DIRTY_BIT_OFFSET + 1;
const IN_NEW_VERSION_BIT_OFFSET: u32 = FILLER_BIT_OFFSET + 1;
const MODIFIED_BIT_OFFSET: u32 = IN_NEW_VERSION_BIT_OFFSET + 1;
const VECTOR_SET_BIT_OFFSET: u32 = MODIFIED_BIT_OFFSET + 1;

const HAS_EXPIRATION_BIT_MASK: u64 = 1u64 << HAS_EXPIRATION_BIT_OFFSET;
const TOMBSTONE_BIT_MASK: u64 = 1u64 << TOMBSTONE_BIT_OFFSET;
const VALID_BIT_MASK: u64 = 1u64 << VALID_BIT_OFFSET;
const SEALED_BIT_MASK: u64 = 1u64 << SEALED_BIT_OFFSET;
const ETAG_BIT_MASK: u64 = 1u64 << ETAG_BIT_OFFSET;
const DIRTY_BIT_MASK: u64 = 1u64 << DIRTY_BIT_OFFSET;
const FILLER_BIT_MASK: u64 = 1u64 << FILLER_BIT_OFFSET;
const IN_NEW_VERSION_BIT_MASK: u64 = 1u64 << IN_NEW_VERSION_BIT_OFFSET;
const MODIFIED_BIT_MASK: u64 = 1u64 << MODIFIED_BIT_OFFSET;
const VECTOR_SET_BIT_MASK: u64 = 1u64 << VECTOR_SET_BIT_OFFSET;

/// Packed 64-bit record metadata.
///
/// The lower 48 bits are previous-address chain pointers.
#[repr(C)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RecordInfo {
    word: u64,
}

impl RecordInfo {
    /// Returns the serialized size in bytes.
    #[inline]
    pub const fn get_length() -> usize {
        RECORD_INFO_LENGTH
    }

    /// Returns raw packed word.
    #[inline]
    pub const fn raw_word(self) -> u64 {
        self.word
    }

    /// Returns a RecordInfo from raw packed word.
    #[inline]
    pub const fn from_raw_word(word: u64) -> Self {
        Self { word }
    }

    /// Returns whether this record header has all bits cleared.
    #[inline]
    pub const fn is_null(self) -> bool {
        self.word == 0
    }

    /// Returns previous logical address (lower 48 bits).
    #[inline]
    pub const fn previous_address(self) -> u64 {
        self.word & PREVIOUS_ADDRESS_MASK
    }

    /// Sets previous logical address (masked to lower 48 bits).
    #[inline]
    pub fn set_previous_address(&mut self, previous_address: u64) {
        self.word =
            (self.word & !PREVIOUS_ADDRESS_MASK) | (previous_address & PREVIOUS_ADDRESS_MASK);
    }

    #[inline]
    const fn has_flag(self, mask: u64) -> bool {
        (self.word & mask) != 0
    }

    #[inline]
    fn set_flag(&mut self, mask: u64, value: bool) {
        if value {
            self.word |= mask;
        } else {
            self.word &= !mask;
        }
    }

    #[inline]
    pub const fn tombstone(self) -> bool {
        self.has_flag(TOMBSTONE_BIT_MASK)
    }

    #[inline]
    pub fn set_tombstone(&mut self) {
        self.word |= TOMBSTONE_BIT_MASK;
    }

    #[inline]
    pub fn clear_tombstone(&mut self) {
        self.word &= !TOMBSTONE_BIT_MASK;
    }

    #[inline]
    pub const fn valid(self) -> bool {
        self.has_flag(VALID_BIT_MASK)
    }

    #[inline]
    pub fn set_valid(&mut self, value: bool) {
        self.set_flag(VALID_BIT_MASK, value);
    }

    #[inline]
    pub const fn invalid(self) -> bool {
        !self.valid()
    }

    #[inline]
    pub const fn is_sealed(self) -> bool {
        self.has_flag(SEALED_BIT_MASK)
    }

    #[inline]
    pub fn seal(&mut self) {
        self.word |= SEALED_BIT_MASK;
    }

    #[inline]
    pub const fn dirty(self) -> bool {
        self.has_flag(DIRTY_BIT_MASK)
    }

    #[inline]
    pub fn set_dirty(&mut self) {
        self.word |= DIRTY_BIT_MASK;
    }

    #[inline]
    pub fn clear_dirty(&mut self) {
        self.word &= !DIRTY_BIT_MASK;
    }

    #[inline]
    pub const fn modified(self) -> bool {
        self.has_flag(MODIFIED_BIT_MASK)
    }

    #[inline]
    pub fn set_modified(&mut self, value: bool) {
        self.set_flag(MODIFIED_BIT_MASK, value);
    }

    #[inline]
    pub const fn has_filler(self) -> bool {
        self.has_flag(FILLER_BIT_MASK)
    }

    #[inline]
    pub fn set_has_filler(&mut self) {
        self.word |= FILLER_BIT_MASK;
    }

    #[inline]
    pub fn clear_has_filler(&mut self) {
        self.word &= !FILLER_BIT_MASK;
    }

    #[inline]
    pub const fn is_in_new_version(self) -> bool {
        self.has_flag(IN_NEW_VERSION_BIT_MASK)
    }

    #[inline]
    pub fn set_is_in_new_version(&mut self) {
        self.word |= IN_NEW_VERSION_BIT_MASK;
    }

    #[inline]
    pub const fn has_etag(self) -> bool {
        self.has_flag(ETAG_BIT_MASK)
    }

    #[inline]
    pub fn set_has_etag(&mut self) {
        self.word |= ETAG_BIT_MASK;
    }

    #[inline]
    pub fn clear_has_etag(&mut self) {
        self.word &= !ETAG_BIT_MASK;
    }

    #[inline]
    pub const fn has_expiration(self) -> bool {
        self.has_flag(HAS_EXPIRATION_BIT_MASK)
    }

    #[inline]
    pub fn set_has_expiration(&mut self) {
        self.word |= HAS_EXPIRATION_BIT_MASK;
    }

    #[inline]
    pub fn clear_has_expiration(&mut self) {
        self.word &= !HAS_EXPIRATION_BIT_MASK;
    }

    #[inline]
    pub const fn vector_set(self) -> bool {
        self.has_flag(VECTOR_SET_BIT_MASK)
    }

    #[inline]
    pub fn set_vector_set(&mut self, value: bool) {
        self.set_flag(VECTOR_SET_BIT_MASK, value);
    }

    #[inline]
    pub const fn is_closed(self) -> bool {
        (self.word & (VALID_BIT_MASK | SEALED_BIT_MASK)) != VALID_BIT_MASK
    }

    /// `SkipOnScan` semantic from Tsavorite: closed records are skipped.
    #[inline]
    pub const fn skip_on_scan(self) -> bool {
        self.is_closed()
    }

    #[inline]
    pub fn set_invalid(&mut self) {
        self.word &= !VALID_BIT_MASK;
    }

    #[inline]
    pub fn initialize_to_sealed_and_invalid(&mut self) {
        self.word = SEALED_BIT_MASK;
    }

    #[inline]
    pub fn unseal_and_validate(&mut self) {
        self.word = (self.word & !SEALED_BIT_MASK) | VALID_BIT_MASK;
    }

    #[inline]
    pub fn seal_and_invalidate(&mut self) {
        self.word = (self.word & !VALID_BIT_MASK) | SEALED_BIT_MASK;
    }

    #[inline]
    pub fn set_dirty_and_modified(&mut self) {
        self.word |= DIRTY_BIT_MASK | MODIFIED_BIT_MASK;
    }

    /// Clears transient bits that should not be persisted in disk images.
    #[inline]
    pub fn clear_bits_for_disk_images(&mut self) {
        self.word &= !(DIRTY_BIT_MASK | SEALED_BIT_MASK);
    }

    /// Initializes this record as sealed+invalid and updates chaining metadata.
    #[inline]
    pub fn write_info(&mut self, in_new_version: bool, previous_address: u64) {
        self.initialize_to_sealed_and_invalid();
        self.set_previous_address(previous_address);
        if in_new_version {
            self.set_is_in_new_version();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_info_is_8_bytes() {
        assert_eq!(size_of::<RecordInfo>(), 8);
        assert_eq!(RecordInfo::get_length(), 8);
    }

    #[test]
    fn previous_address_uses_lower_48_bits() {
        let mut info = RecordInfo::default();
        info.set_previous_address(u64::MAX);
        assert_eq!(info.previous_address(), PREVIOUS_ADDRESS_MASK);
    }

    #[test]
    fn write_info_initializes_sealed_invalid_and_previous_address() {
        let mut info = RecordInfo::default();
        info.write_info(true, 0x1234_5678_9abc_def0);

        assert!(info.is_sealed());
        assert!(info.invalid());
        assert!(info.is_in_new_version());
        assert_eq!(info.previous_address(), 0x5678_9abc_def0);
    }

    #[test]
    fn closed_and_skip_on_scan_follow_valid_plus_sealed_rule() {
        let mut info = RecordInfo::default();
        info.set_valid(true);
        assert!(!info.is_closed());
        assert!(!info.skip_on_scan());

        info.seal();
        assert!(info.is_closed());
        assert!(info.skip_on_scan());
    }

    #[test]
    fn expiration_etag_and_tombstone_flags_roundtrip() {
        let mut info = RecordInfo::default();
        info.set_valid(true);
        info.set_tombstone();
        info.set_has_etag();
        info.set_has_expiration();

        assert!(info.valid());
        assert!(info.tombstone());
        assert!(info.has_etag());
        assert!(info.has_expiration());

        info.clear_tombstone();
        info.clear_has_etag();
        info.clear_has_expiration();

        assert!(!info.tombstone());
        assert!(!info.has_etag());
        assert!(!info.has_expiration());
    }

    #[test]
    fn disk_image_clear_removes_dirty_and_sealed_only() {
        let mut info = RecordInfo::default();
        info.set_valid(true);
        info.set_dirty();
        info.seal();
        info.set_modified(true);

        info.clear_bits_for_disk_images();

        assert!(!info.dirty());
        assert!(!info.is_sealed());
        assert!(info.valid());
        assert!(info.modified());
    }
}
