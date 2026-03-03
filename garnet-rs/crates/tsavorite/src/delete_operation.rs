//! Delete operation path that materializes tombstones.
//!
//! See [Doc 06 Section 2.12] for mutable in-place delete vs. tombstone append behavior.

use crate::DeleteInfo;
use crate::HashIndex;
use crate::HashIndexError;
use crate::ISessionFunctions;
use crate::RecordInfo;
use crate::hybrid_log::LogAddressPointers;
use crate::hybrid_log::LogicalAddress;
use crate::hybrid_log::PageManager;
use crate::hybrid_log::PageManagerError;
use crate::hybrid_log::RecordFormatError;
use crate::hybrid_log::RecordLayout;
use crate::hybrid_log::parse_key_span;
use crate::hybrid_log::parse_record_info;
use crate::hybrid_log::parse_record_layout;
use crate::hybrid_log::parse_value_span;
use crate::hybrid_log::write_record;
use garnet_common::SpanByteError;

pub trait HybridLogDeleteAdapter: ISessionFunctions {
    fn record_key_equals(&self, requested_key: &Self::Key, record_key: &[u8]) -> bool;
    fn key_to_record_bytes(&self, key: &Self::Key) -> Vec<u8>;
    fn value_from_record(&self, record_value: &[u8]) -> Self::Value;
    fn value_to_record_bytes(&self, value: &Self::Value) -> Vec<u8>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteOperationStatus {
    TombstonedInPlace,
    AppendedTombstone,
    NotFound,
    RetryLater,
}

#[derive(Debug)]
pub enum DeleteOperationError {
    HashIndex(HashIndexError),
    PageManager(PageManagerError),
    RecordFormat(RecordFormatError),
    SpanByte(SpanByteError),
    CompareExchangeConflict,
    RecordTooLarge {
        record_size: usize,
        page_size: usize,
    },
    OperationCancelled,
}

impl core::fmt::Display for DeleteOperationError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::HashIndex(inner) => inner.fmt(f),
            Self::PageManager(inner) => inner.fmt(f),
            Self::RecordFormat(inner) => inner.fmt(f),
            Self::SpanByte(inner) => inner.fmt(f),
            Self::CompareExchangeConflict => write!(f, "hash-entry compare_exchange failed"),
            Self::RecordTooLarge {
                record_size,
                page_size,
            } => {
                write!(
                    f,
                    "record size {} exceeds page size {}",
                    record_size, page_size
                )
            }
            Self::OperationCancelled => write!(f, "session function cancelled delete operation"),
        }
    }
}

impl std::error::Error for DeleteOperationError {}

impl From<HashIndexError> for DeleteOperationError {
    fn from(value: HashIndexError) -> Self {
        Self::HashIndex(value)
    }
}

impl From<PageManagerError> for DeleteOperationError {
    fn from(value: PageManagerError) -> Self {
        Self::PageManager(value)
    }
}

impl From<RecordFormatError> for DeleteOperationError {
    fn from(value: RecordFormatError) -> Self {
        Self::RecordFormat(value)
    }
}

impl From<SpanByteError> for DeleteOperationError {
    fn from(value: SpanByteError) -> Self {
        Self::SpanByte(value)
    }
}

pub struct DeleteOperationContext<'a> {
    pub hash_index: &'a HashIndex,
    pub page_manager: &'a mut PageManager,
    pub pointers: &'a LogAddressPointers,
}

struct MaterializedRecord {
    record_info: RecordInfo,
    key_bytes: Vec<u8>,
    value_bytes: Vec<u8>,
    allocated_size: usize,
}

struct ChainRecordMatch {
    address: LogicalAddress,
    record: MaterializedRecord,
}

pub fn delete<F>(
    context: &mut DeleteOperationContext<'_>,
    functions: &F,
    key_hash: u64,
    key: &F::Key,
    delete_info: &mut DeleteInfo,
) -> Result<DeleteOperationStatus, DeleteOperationError>
where
    F: HybridLogDeleteAdapter,
    F::Value: Clone + Default,
{
    let head_entry = match context.hash_index.find_tag_entry(key_hash) {
        Some(entry) => entry,
        None => return Ok(DeleteOperationStatus::NotFound),
    };

    let head_address = entry_address(head_entry.word);
    if head_address == LogicalAddress(0) {
        return Ok(DeleteOperationStatus::NotFound);
    }

    let record_match = match find_matching_record_in_chain(
        context.page_manager,
        context.pointers.begin_address(),
        functions,
        key,
        head_address,
    )? {
        Some(found) => found,
        None => return Ok(DeleteOperationStatus::NotFound),
    };
    let matched_address = record_match.address;
    let matched_record = record_match.record;

    if matched_record.record_info.is_closed() {
        return Ok(DeleteOperationStatus::RetryLater);
    }
    if matched_record.record_info.tombstone() || matched_record.record_info.invalid() {
        return Ok(DeleteOperationStatus::NotFound);
    }

    let key_bytes = functions.key_to_record_bytes(key);
    if matched_address >= context.pointers.read_only_address() {
        let mut value = functions.value_from_record(&matched_record.value_bytes);
        let mut updated_record_info = matched_record.record_info;
        if functions.concurrent_deleter(key, &mut value, delete_info, &mut updated_record_info) {
            updated_record_info.set_tombstone();
            let mut value_bytes = functions.value_to_record_bytes(&value);
            if value_bytes.len() != matched_record.value_bytes.len() {
                value_bytes = matched_record.value_bytes.clone();
            }
            rewrite_record(
                context.page_manager,
                matched_address,
                &key_bytes,
                &value_bytes,
                updated_record_info,
                matched_record.allocated_size,
            )?;
            return Ok(DeleteOperationStatus::TombstonedInPlace);
        } else {
            return Ok(DeleteOperationStatus::RetryLater);
        }
    }

    let mut value = functions.value_from_record(&matched_record.value_bytes);
    let mut new_record_info = RecordInfo::default();
    new_record_info.set_valid(true);
    new_record_info.set_tombstone();
    new_record_info.set_previous_address(matched_address);

    if !functions.single_deleter(key, &mut value, delete_info, &mut new_record_info) {
        return Err(DeleteOperationError::OperationCancelled);
    }

    let value_bytes = functions.value_to_record_bytes(&value);
    let new_address = append_record(
        context.page_manager,
        context.pointers,
        &key_bytes,
        &value_bytes,
        new_record_info,
    )?;
    let updated = context
        .hash_index
        .compare_exchange_entry_address(&head_entry, new_address)?;
    if !updated {
        return Err(DeleteOperationError::CompareExchangeConflict);
    }

    Ok(DeleteOperationStatus::AppendedTombstone)
}

fn entry_address(word: crate::PackedEntryWord) -> LogicalAddress {
    crate::HashBucketEntry::address_from_word(word)
}

fn find_matching_record_in_chain<F>(
    page_manager: &mut PageManager,
    begin_address: LogicalAddress,
    functions: &F,
    key: &F::Key,
    mut current_address: LogicalAddress,
) -> Result<Option<ChainRecordMatch>, DeleteOperationError>
where
    F: HybridLogDeleteAdapter,
{
    while current_address != LogicalAddress(0) && current_address >= begin_address {
        let record = materialize_record_at(page_manager, current_address)?;
        if functions.record_key_equals(key, &record.key_bytes) {
            return Ok(Some(ChainRecordMatch {
                address: current_address,
                record,
            }));
        }
        current_address = record.record_info.previous_address();
    }
    Ok(None)
}

fn materialize_record_at(
    page_manager: &mut PageManager,
    logical: LogicalAddress,
) -> Result<MaterializedRecord, DeleteOperationError> {
    let page_space = page_manager.address_space();
    let decoded = page_space.decode(logical);
    let page_offset = decoded.page_offset as usize;
    let available = page_manager.page_size() - page_offset;
    let source = page_manager.read_at(logical, available)?;
    let layout = parse_record_layout(source)?;
    let record_slice = &source[..layout.allocated_size];

    Ok(MaterializedRecord {
        record_info: parse_record_info(record_slice)?,
        key_bytes: parse_key_span(record_slice)?.as_slice().to_vec(),
        value_bytes: parse_value_span(record_slice)?.as_slice().to_vec(),
        allocated_size: layout.allocated_size,
    })
}

fn rewrite_record(
    page_manager: &mut PageManager,
    address: LogicalAddress,
    key_bytes: &[u8],
    value_bytes: &[u8],
    record_info: RecordInfo,
    expected_allocated_size: usize,
) -> Result<(), DeleteOperationError> {
    let mut record = vec![0u8; expected_allocated_size];
    let layout = write_record(&mut record, record_info, key_bytes, value_bytes)?;
    if layout.allocated_size != expected_allocated_size {
        return Err(DeleteOperationError::OperationCancelled);
    }
    page_manager.write_at(address, &record)?;
    Ok(())
}

fn append_record(
    page_manager: &mut PageManager,
    pointers: &LogAddressPointers,
    key_bytes: &[u8],
    value_bytes: &[u8],
    record_info: RecordInfo,
) -> Result<LogicalAddress, DeleteOperationError> {
    let layout = RecordLayout::for_payload_lengths(key_bytes.len(), value_bytes.len())?;
    let allocated_size = layout.allocated_size;
    if allocated_size > page_manager.page_size() {
        return Err(DeleteOperationError::RecordTooLarge {
            record_size: allocated_size,
            page_size: page_manager.page_size(),
        });
    }

    let mut record = vec![0u8; allocated_size];
    write_record(&mut record, record_info, key_bytes, value_bytes)?;
    let address = reserve_tail_space(page_manager, pointers, allocated_size)?;
    page_manager.write_at(address, &record)?;
    Ok(address)
}

fn reserve_tail_space(
    page_manager: &mut PageManager,
    pointers: &LogAddressPointers,
    allocated_size: usize,
) -> Result<LogicalAddress, DeleteOperationError> {
    loop {
        let old_tail = pointers.advance_tail_by(allocated_size as u64);
        let logical = old_tail;
        let space = page_manager.address_space();
        let decoded = space.decode(logical);
        let page_index = decoded.page_index;
        let page_offset = decoded.page_offset as usize;

        if page_offset + allocated_size <= page_manager.page_size() {
            if !page_manager.is_page_allocated(page_index) {
                page_manager.allocate_page(page_index)?;
            }
            return Ok(logical);
        }

        let next_page = page_index + 1;
        let next_page_start = next_page << space.page_size_bits();
        pointers.shift_tail_address(LogicalAddress(next_page_start));
        if !page_manager.is_page_allocated(next_page) {
            page_manager.allocate_page(next_page)?;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DeleteInfo;
    use crate::HybridLogUpsertAdapter;
    use crate::ReadInfo;
    use crate::RmwInfo;
    use crate::UpsertInfo;
    use crate::UpsertOperationContext;
    use crate::UpsertOperationStatus;
    use crate::WriteReason;
    use crate::read_operation::HybridLogReadAdapter;
    use crate::read_operation::ReadOperationContext;
    use crate::read_operation::ReadOperationStatus;
    use crate::read_operation::read;
    use crate::upsert;

    struct ByteDeleteFunctions;

    impl ISessionFunctions for ByteDeleteFunctions {
        type Key = Vec<u8>;
        type Value = Vec<u8>;
        type Input = Vec<u8>;
        type Output = Vec<u8>;
        type Context = ();
        type Reader = ();
        type Writer = ();
        type Comparer = ();

        fn single_reader(
            &self,
            _key: &Self::Key,
            _input: &Self::Input,
            value: &Self::Value,
            output: &mut Self::Output,
            _read_info: &ReadInfo,
        ) -> bool {
            *output = value.clone();
            true
        }

        fn concurrent_reader(
            &self,
            _key: &Self::Key,
            _input: &Self::Input,
            value: &Self::Value,
            output: &mut Self::Output,
            _read_info: &ReadInfo,
            _record_info: &RecordInfo,
        ) -> bool {
            *output = value.clone();
            true
        }

        fn single_writer(
            &self,
            _key: &Self::Key,
            input: &Self::Input,
            _src: &Self::Value,
            dst: &mut Self::Value,
            output: &mut Self::Output,
            _upsert_info: &mut UpsertInfo,
            _reason: WriteReason,
            _record_info: &mut RecordInfo,
        ) -> bool {
            *dst = input.clone();
            *output = dst.clone();
            true
        }

        fn concurrent_writer(
            &self,
            _key: &Self::Key,
            input: &Self::Input,
            _src: &Self::Value,
            dst: &mut Self::Value,
            output: &mut Self::Output,
            _upsert_info: &mut UpsertInfo,
            _record_info: &mut RecordInfo,
        ) -> bool {
            *dst = input.clone();
            *output = dst.clone();
            true
        }

        fn in_place_updater(
            &self,
            _key: &Self::Key,
            _input: &Self::Input,
            _value: &mut Self::Value,
            _output: &mut Self::Output,
            _rmw_info: &mut RmwInfo,
            _record_info: &mut RecordInfo,
        ) -> bool {
            false
        }

        fn copy_updater(
            &self,
            _key: &Self::Key,
            _input: &Self::Input,
            _old_value: &Self::Value,
            _new_value: &mut Self::Value,
            _output: &mut Self::Output,
            _rmw_info: &mut RmwInfo,
            _record_info: &mut RecordInfo,
        ) -> bool {
            false
        }

        fn single_deleter(
            &self,
            _key: &Self::Key,
            value: &mut Self::Value,
            _delete_info: &mut DeleteInfo,
            _record_info: &mut RecordInfo,
        ) -> bool {
            value.clear();
            true
        }

        fn concurrent_deleter(
            &self,
            _key: &Self::Key,
            value: &mut Self::Value,
            _delete_info: &mut DeleteInfo,
            _record_info: &mut RecordInfo,
        ) -> bool {
            value.clear();
            true
        }
    }

    impl HybridLogDeleteAdapter for ByteDeleteFunctions {
        fn record_key_equals(&self, requested_key: &Self::Key, record_key: &[u8]) -> bool {
            requested_key.as_slice() == record_key
        }

        fn key_to_record_bytes(&self, key: &Self::Key) -> Vec<u8> {
            key.clone()
        }

        fn value_from_record(&self, record_value: &[u8]) -> Self::Value {
            record_value.to_vec()
        }

        fn value_to_record_bytes(&self, value: &Self::Value) -> Vec<u8> {
            value.clone()
        }
    }

    impl HybridLogUpsertAdapter for ByteDeleteFunctions {
        fn record_key_equals(&self, requested_key: &Self::Key, record_key: &[u8]) -> bool {
            requested_key.as_slice() == record_key
        }

        fn key_to_record_bytes(&self, key: &Self::Key) -> Vec<u8> {
            key.clone()
        }

        fn value_from_record(&self, record_value: &[u8]) -> Self::Value {
            record_value.to_vec()
        }

        fn value_to_record_bytes(&self, value: &Self::Value) -> Vec<u8> {
            value.clone()
        }
    }

    impl HybridLogReadAdapter for ByteDeleteFunctions {
        fn record_key_equals(&self, requested_key: &Self::Key, record_key: &[u8]) -> bool {
            requested_key.as_slice() == record_key
        }

        fn value_from_record(&self, record_value: &[u8]) -> Self::Value {
            record_value.to_vec()
        }
    }

    fn seed_record(
        hash_index: &HashIndex,
        page_manager: &mut PageManager,
        pointers: &LogAddressPointers,
        key_hash: u64,
        key: &[u8],
        value: &[u8],
    ) {
        let functions = ByteDeleteFunctions;
        let mut upsert_info = UpsertInfo::default();
        let mut output = Vec::new();
        let mut context = UpsertOperationContext {
            hash_index,
            page_manager,
            pointers,
        };
        let status = upsert(
            &mut context,
            &functions,
            key_hash,
            &key.to_vec(),
            &value.to_vec(),
            &mut output,
            &mut upsert_info,
        )
        .unwrap();
        assert!(matches!(
            status,
            UpsertOperationStatus::Inserted | UpsertOperationStatus::CopiedToTail
        ));
    }

    #[test]
    fn delete_tombstones_in_place_in_mutable_region() {
        let hash_index = HashIndex::with_size_bits(2).unwrap();
        let mut page_manager = PageManager::new(8, 8).unwrap();
        page_manager.allocate_page(0).unwrap();
        let pointers = LogAddressPointers::new(LogicalAddress(crate::RECORD_ALIGNMENT as u64));
        pointers.shift_read_only_address(LogicalAddress(0));

        let key_hash = 41u64 << crate::HASH_TAG_SHIFT;
        seed_record(
            &hash_index,
            &mut page_manager,
            &pointers,
            key_hash,
            b"key",
            b"value",
        );

        let functions = ByteDeleteFunctions;
        let mut delete_info = DeleteInfo::default();
        let key = b"key".to_vec();
        let mut context = DeleteOperationContext {
            hash_index: &hash_index,
            page_manager: &mut page_manager,
            pointers: &pointers,
        };

        let status = delete(&mut context, &functions, key_hash, &key, &mut delete_info).unwrap();
        assert_eq!(status, DeleteOperationStatus::TombstonedInPlace);

        let device = crate::InMemoryPageDevice::new(context.page_manager.page_size());
        let mut output = Vec::new();
        let mut read_context = ReadOperationContext {
            hash_index: &hash_index,
            page_manager: context.page_manager,
            pointers: &pointers,
            device: &device,
        };
        let read_status = read(
            &mut read_context,
            &functions,
            key_hash,
            &key,
            &Vec::new(),
            &mut output,
            &ReadInfo::default(),
        )
        .unwrap();
        assert_eq!(read_status, ReadOperationStatus::NotFound);
    }

    #[test]
    fn delete_appends_tombstone_in_immutable_region() {
        let hash_index = HashIndex::with_size_bits(2).unwrap();
        let mut page_manager = PageManager::new(8, 8).unwrap();
        page_manager.allocate_page(0).unwrap();
        let pointers = LogAddressPointers::new(LogicalAddress(crate::RECORD_ALIGNMENT as u64));
        pointers.shift_read_only_address(LogicalAddress(128));

        let key_hash = 42u64 << crate::HASH_TAG_SHIFT;
        seed_record(
            &hash_index,
            &mut page_manager,
            &pointers,
            key_hash,
            b"key",
            b"value",
        );
        let before = hash_index.find_tag_address(key_hash).unwrap();

        let functions = ByteDeleteFunctions;
        let mut delete_info = DeleteInfo::default();
        let key = b"key".to_vec();
        let mut context = DeleteOperationContext {
            hash_index: &hash_index,
            page_manager: &mut page_manager,
            pointers: &pointers,
        };

        let status = delete(&mut context, &functions, key_hash, &key, &mut delete_info).unwrap();
        let after = hash_index.find_tag_address(key_hash).unwrap();
        assert_eq!(status, DeleteOperationStatus::AppendedTombstone);
        assert_ne!(before, after);
    }

    #[test]
    fn delete_missing_key_returns_not_found() {
        let hash_index = HashIndex::with_size_bits(2).unwrap();
        let mut page_manager = PageManager::new(8, 8).unwrap();
        page_manager.allocate_page(0).unwrap();
        let pointers = LogAddressPointers::new(LogicalAddress(crate::RECORD_ALIGNMENT as u64));

        let functions = ByteDeleteFunctions;
        let mut delete_info = DeleteInfo::default();
        let key_hash = 43u64 << crate::HASH_TAG_SHIFT;
        let key = b"absent".to_vec();
        let mut context = DeleteOperationContext {
            hash_index: &hash_index,
            page_manager: &mut page_manager,
            pointers: &pointers,
        };

        let status = delete(&mut context, &functions, key_hash, &key, &mut delete_info).unwrap();
        assert_eq!(status, DeleteOperationStatus::NotFound);
    }
}
