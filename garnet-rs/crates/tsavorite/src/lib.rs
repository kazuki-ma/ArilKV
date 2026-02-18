//! Tsavorite storage engine for garnet-rs.

pub mod epoch;
pub mod hash_bucket;
pub mod hash_bucket_entry;
pub mod hash_index;
pub mod hybrid_log;
pub mod record_info;

pub use epoch::{EpochEntry, EpochGuard, LightEpoch};
pub use hash_bucket::{
    HashBucket, EXCLUSIVE_LATCH_BIT_MASK, HASH_BUCKET_DATA_ENTRY_COUNT, HASH_BUCKET_ENTRY_COUNT,
    HASH_BUCKET_OVERFLOW_INDEX, LATCH_BIT_MASK, SHARED_LATCH_BITS, SHARED_LATCH_BIT_MASK,
    SHARED_LATCH_BIT_OFFSET, SHARED_LATCH_INCREMENT,
};
pub use hash_bucket_entry::{
    HashBucketEntry, HashBucketEntryCasError, HashBucketEntryError, ADDRESS_BITS, ADDRESS_MASK,
    PENDING_BIT_MASK, PENDING_BIT_SHIFT, READ_CACHE_BIT_MASK, READ_CACHE_BIT_SHIFT, TAG_BITS,
    TAG_MASK, TAG_POSITION_MASK, TAG_SHIFT, TENTATIVE_BIT_MASK, TENTATIVE_BIT_SHIFT,
};
pub use hash_index::{
    FindOrCreateTagResult, FindOrCreateTagStatus, HashIndex, HashIndexError, HashLocation,
    HASH_TAG_BITS, HASH_TAG_MASK, HASH_TAG_SHIFT, TEMP_INVALID_ADDRESS,
};
pub use hybrid_log::{
    flush_page_to_device, load_page_from_device, parse_key_span, parse_record_info,
    parse_record_layout, parse_value_span, read_with_callback, round_up,
    shift_head_address_and_evict, write_record, InMemoryPageDevice, LogAddressPointers,
    LogAddressPointersSnapshot, LogicalAddress, Page, PageAddressSpace, PageDevice, PageIoError,
    PageManager, PageManagerError, PageResidencyError, ReadPathStatus, RecordFormatError,
    RecordLayout, RecordParsedLayout, TailAllocationStatus, TailAllocator, TailAllocatorError,
    RECORD_ALIGNMENT, RECORD_INFO_SIZE,
};
pub use record_info::{
    RecordInfo, PREVIOUS_ADDRESS_BITS, PREVIOUS_ADDRESS_MASK, RECORD_INFO_LENGTH,
};
