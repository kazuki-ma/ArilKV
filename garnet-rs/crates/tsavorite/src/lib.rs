//! Tsavorite storage engine for garnet-rs.

pub mod aof_log;
pub mod checkpoint_aof_coordinator;
pub mod checkpoint_state_machine;
pub mod delete_operation;
pub mod epoch;
pub mod hash_bucket;
pub mod hash_bucket_entry;
pub mod hash_index;
pub mod hybrid_log;
pub mod overflow_bucket_allocator;
pub mod read_operation;
pub mod record_info;
pub mod rmw_operation;
pub mod session_functions;
pub mod tsavorite_kv;
pub mod upsert_operation;

pub use aof_log::{AofReader, AofWriter, AofWriterConfig, compact_aof_file};
pub use checkpoint_aof_coordinator::{
    CheckpointAofCoordinator, CheckpointAofCoordinatorError, RecoveryPlan,
};
pub use checkpoint_state_machine::{
    CheckpointMode, CheckpointPhase, CheckpointState, CheckpointStateMachine,
    CheckpointTransitionError,
};
pub use delete_operation::{
    DeleteOperationContext, DeleteOperationError, DeleteOperationStatus, HybridLogDeleteAdapter,
    delete,
};
pub use epoch::{EpochEntry, EpochGuard, LightEpoch};
pub use hash_bucket::{
    EXCLUSIVE_LATCH_BIT_MASK, HASH_BUCKET_DATA_ENTRY_COUNT, HASH_BUCKET_ENTRY_COUNT,
    HASH_BUCKET_OVERFLOW_INDEX, HashBucket, LATCH_BIT_MASK, SHARED_LATCH_BIT_MASK,
    SHARED_LATCH_BIT_OFFSET, SHARED_LATCH_BITS, SHARED_LATCH_INCREMENT,
};
pub use hash_bucket_entry::{
    ADDRESS_BITS, ADDRESS_MASK, HashBucketEntry, HashBucketEntryCasError, HashBucketEntryError,
    PENDING_BIT_MASK, PENDING_BIT_SHIFT, READ_CACHE_BIT_MASK, READ_CACHE_BIT_SHIFT, TAG_BITS,
    TAG_MASK, TAG_POSITION_MASK, TAG_SHIFT, TENTATIVE_BIT_MASK, TENTATIVE_BIT_SHIFT,
};
pub use hash_index::{
    FindOrCreateTagResult, FindOrCreateTagStatus, HASH_TAG_BITS, HASH_TAG_MASK, HASH_TAG_SHIFT,
    HashEntryLocation, HashIndex, HashIndexError, HashLocation, TEMP_INVALID_ADDRESS,
};
pub use hybrid_log::{
    InMemoryPageDevice, LogAddressPointers, LogAddressPointersSnapshot, LogicalAddress, Page,
    PageAddressSpace, PageDevice, PageIoError, PageManager, PageManagerError, PageResidencyError,
    RECORD_ALIGNMENT, RECORD_INFO_SIZE, ReadPathStatus, RecordFormatError, RecordLayout,
    RecordParsedLayout, TailAllocationStatus, TailAllocator, TailAllocatorError,
    flush_page_to_device, load_page_from_device, parse_key_span, parse_record_info,
    parse_record_layout, parse_value_span, read_with_callback, round_up,
    shift_head_address_and_evict, write_record,
};
pub use overflow_bucket_allocator::{
    OVERFLOW_PAGE_SIZE, OVERFLOW_PAGE_SIZE_BITS, OverflowBucketAllocator,
    OverflowBucketAllocatorError, OverflowBucketHandle,
};
pub use read_operation::{
    HybridLogReadAdapter, ReadOperationContext, ReadOperationError, ReadOperationStatus, read,
};
pub use record_info::{
    PREVIOUS_ADDRESS_BITS, PREVIOUS_ADDRESS_MASK, RECORD_INFO_LENGTH, RecordInfo,
};
pub use rmw_operation::{
    HybridLogRmwAdapter, RmwOperationContext, RmwOperationError, RmwOperationStatus, rmw,
};
pub use session_functions::{
    DeleteInfo, ISessionFunctions, ReadInfo, RmwInfo, UpsertInfo, WriteReason,
};
pub use tsavorite_kv::{TsavoriteKV, TsavoriteKvConfig, TsavoriteKvInitError, TsavoriteSession};
pub use upsert_operation::{
    HybridLogUpsertAdapter, UpsertOperationContext, UpsertOperationError, UpsertOperationStatus,
    upsert,
};
