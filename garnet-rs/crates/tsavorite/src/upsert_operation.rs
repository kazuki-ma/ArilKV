//! Upsert operation path over hash index + hybrid log records.
//!
//! See [Doc 06 Section 2.12] for mutable in-place write vs. copy-update flow.

use crate::FindOrCreateTagStatus;
use crate::HashIndex;
use crate::HashIndexError;
use crate::ISessionFunctions;
use crate::RecordInfo;
use crate::UpsertInfo;
use crate::WriteReason;
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

pub trait HybridLogUpsertAdapter: ISessionFunctions {
    fn record_key_equals(&self, requested_key: &Self::Key, record_key: &[u8]) -> bool;
    fn key_to_record_bytes(&self, key: &Self::Key) -> Vec<u8>;
    fn value_from_record(&self, record_value: &[u8]) -> Self::Value;
    fn value_to_record_bytes(&self, value: &Self::Value) -> Vec<u8>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpsertOperationStatus {
    InPlaceUpdated,
    CopiedToTail,
    Inserted,
}

#[derive(Debug)]
pub enum UpsertOperationError {
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

impl core::fmt::Display for UpsertOperationError {
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
            Self::OperationCancelled => write!(f, "session function cancelled upsert operation"),
        }
    }
}

impl std::error::Error for UpsertOperationError {}

impl From<HashIndexError> for UpsertOperationError {
    fn from(value: HashIndexError) -> Self {
        Self::HashIndex(value)
    }
}

impl From<PageManagerError> for UpsertOperationError {
    fn from(value: PageManagerError) -> Self {
        Self::PageManager(value)
    }
}

impl From<RecordFormatError> for UpsertOperationError {
    fn from(value: RecordFormatError) -> Self {
        Self::RecordFormat(value)
    }
}

impl From<SpanByteError> for UpsertOperationError {
    fn from(value: SpanByteError) -> Self {
        Self::SpanByte(value)
    }
}

pub struct UpsertOperationContext<'a> {
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

pub fn upsert<F>(
    context: &mut UpsertOperationContext<'_>,
    functions: &F,
    key_hash: u64,
    key: &F::Key,
    input: &F::Input,
    output: &mut F::Output,
    upsert_info: &mut UpsertInfo,
) -> Result<UpsertOperationStatus, UpsertOperationError>
where
    F: HybridLogUpsertAdapter,
    F::Value: Clone + Default,
{
    let key_bytes = functions.key_to_record_bytes(key);
    let maybe_head_entry = context.hash_index.find_tag_entry(key_hash);
    let mut previous_address = 0u64;
    let mut matched_address = None;
    let mut matched_record = None;

    if let Some(head_entry) = maybe_head_entry {
        previous_address = entry_address(head_entry.word);

        if previous_address != 0 {
            if let Some((address, record)) = find_matching_record_in_chain(
                context.page_manager,
                context.pointers.begin_address(),
                functions,
                key,
                previous_address,
            )? {
                if !record.record_info.tombstone() && !record.record_info.invalid() {
                    matched_address = Some(address);
                    matched_record = Some(record);
                }
            }
        }
    }

    if let (Some(address), Some(record), Some(head_entry)) =
        (matched_address, matched_record, maybe_head_entry)
    {
        if address >= context.pointers.read_only_address() {
            let old_value = functions.value_from_record(&record.value_bytes);
            let mut new_value = old_value.clone();
            let mut updated_record_info = record.record_info;
            if functions.concurrent_writer(
                key,
                input,
                &old_value,
                &mut new_value,
                output,
                upsert_info,
                &mut updated_record_info,
            ) {
                let new_value_bytes = functions.value_to_record_bytes(&new_value);
                if new_value_bytes.len() == record.value_bytes.len() {
                    rewrite_record(
                        context.page_manager,
                        LogicalAddress(address),
                        &key_bytes,
                        &new_value_bytes,
                        updated_record_info,
                        record.allocated_size,
                    )?;
                    return Ok(UpsertOperationStatus::InPlaceUpdated);
                }
            }
        }

        let old_value = functions.value_from_record(&record.value_bytes);
        let mut new_value = old_value.clone();
        let mut new_record_info = RecordInfo::default();
        new_record_info.set_valid(true);
        new_record_info.set_previous_address(address);
        if !functions.single_writer(
            key,
            input,
            &old_value,
            &mut new_value,
            output,
            upsert_info,
            WriteReason::CopyUpdate,
            &mut new_record_info,
        ) {
            return Err(UpsertOperationError::OperationCancelled);
        }

        let new_value_bytes = functions.value_to_record_bytes(&new_value);
        let new_address = append_record(
            context.page_manager,
            context.pointers,
            &key_bytes,
            &new_value_bytes,
            new_record_info,
        )?;
        let updated = context
            .hash_index
            .compare_exchange_entry_address(&head_entry, new_address)?;
        if !updated {
            return Err(UpsertOperationError::CompareExchangeConflict);
        }

        return Ok(UpsertOperationStatus::CopiedToTail);
    }

    let head_entry = if let Some(entry) = maybe_head_entry {
        entry
    } else {
        let result = context
            .hash_index
            .find_or_create_tag(key_hash, context.pointers.begin_address())?;
        if result.status != FindOrCreateTagStatus::Created
            && result.status != FindOrCreateTagStatus::FoundExisting
        {
            return Err(UpsertOperationError::OperationCancelled);
        }
        context
            .hash_index
            .find_tag_entry(key_hash)
            .ok_or(UpsertOperationError::OperationCancelled)?
    };

    let src_value = F::Value::default();
    let mut dst_value = F::Value::default();
    let mut new_record_info = RecordInfo::default();
    new_record_info.set_valid(true);
    new_record_info.set_previous_address(previous_address);
    if !functions.single_writer(
        key,
        input,
        &src_value,
        &mut dst_value,
        output,
        upsert_info,
        WriteReason::Insert,
        &mut new_record_info,
    ) {
        return Err(UpsertOperationError::OperationCancelled);
    }

    let value_bytes = functions.value_to_record_bytes(&dst_value);
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
        return Err(UpsertOperationError::CompareExchangeConflict);
    }
    Ok(UpsertOperationStatus::Inserted)
}

fn entry_address(word: u64) -> u64 {
    word & crate::ADDRESS_MASK
}

fn find_matching_record_in_chain<F>(
    page_manager: &mut PageManager,
    begin_address: u64,
    functions: &F,
    key: &F::Key,
    mut current_address: u64,
) -> Result<Option<(u64, MaterializedRecord)>, UpsertOperationError>
where
    F: HybridLogUpsertAdapter,
{
    while current_address != 0 && current_address >= begin_address {
        let record = materialize_record_at(page_manager, LogicalAddress(current_address))?;
        if functions.record_key_equals(key, &record.key_bytes) {
            return Ok(Some((current_address, record)));
        }
        current_address = record.record_info.previous_address();
    }
    Ok(None)
}

fn materialize_record_at(
    page_manager: &mut PageManager,
    logical: LogicalAddress,
) -> Result<MaterializedRecord, UpsertOperationError> {
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
) -> Result<(), UpsertOperationError> {
    let mut record = vec![0u8; expected_allocated_size];
    let layout = write_record(&mut record, record_info, key_bytes, value_bytes)?;
    if layout.allocated_size != expected_allocated_size {
        return Err(UpsertOperationError::OperationCancelled);
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
) -> Result<u64, UpsertOperationError> {
    let layout = RecordLayout::for_payload_lengths(key_bytes.len(), value_bytes.len())?;
    let allocated_size = layout.allocated_size;
    if allocated_size > page_manager.page_size() {
        return Err(UpsertOperationError::RecordTooLarge {
            record_size: allocated_size,
            page_size: page_manager.page_size(),
        });
    }

    let mut record = vec![0u8; allocated_size];
    write_record(&mut record, record_info, key_bytes, value_bytes)?;
    let address = reserve_tail_space(page_manager, pointers, allocated_size)?;
    page_manager.write_at(address, &record)?;
    Ok(address.raw())
}

fn reserve_tail_space(
    page_manager: &mut PageManager,
    pointers: &LogAddressPointers,
    allocated_size: usize,
) -> Result<LogicalAddress, UpsertOperationError> {
    loop {
        let old_tail = pointers.advance_tail_by(allocated_size as u64);
        let logical = LogicalAddress(old_tail);
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
        pointers.shift_tail_address(next_page_start);
        if !page_manager.is_page_allocated(next_page) {
            page_manager.allocate_page(next_page)?;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DeleteInfo;
    use crate::ReadInfo;
    use crate::RmwInfo;
    use crate::delete_operation::DeleteOperationContext;
    use crate::delete_operation::DeleteOperationStatus;
    use crate::delete_operation::delete;
    use crate::read_operation::HybridLogReadAdapter;
    use crate::read_operation::ReadOperationContext;
    use crate::read_operation::ReadOperationStatus;
    use crate::read_operation::read;

    struct ByteUpsertFunctions;

    impl ISessionFunctions for ByteUpsertFunctions {
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
            _read_info: &crate::ReadInfo,
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
            _read_info: &crate::ReadInfo,
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
            _delete_info: &mut crate::DeleteInfo,
            record_info: &mut RecordInfo,
        ) -> bool {
            value.clear();
            record_info.set_tombstone();
            true
        }

        fn concurrent_deleter(
            &self,
            _key: &Self::Key,
            value: &mut Self::Value,
            _delete_info: &mut crate::DeleteInfo,
            record_info: &mut RecordInfo,
        ) -> bool {
            value.clear();
            record_info.set_tombstone();
            true
        }
    }

    impl HybridLogUpsertAdapter for ByteUpsertFunctions {
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

    impl HybridLogReadAdapter for ByteUpsertFunctions {
        fn record_key_equals(&self, requested_key: &Self::Key, record_key: &[u8]) -> bool {
            requested_key.as_slice() == record_key
        }

        fn value_from_record(&self, record_value: &[u8]) -> Self::Value {
            record_value.to_vec()
        }
    }

    impl crate::delete_operation::HybridLogDeleteAdapter for ByteUpsertFunctions {
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

    #[test]
    fn upsert_inserts_new_record() {
        let hash_index = HashIndex::with_size_bits(2).unwrap();
        let mut page_manager = PageManager::new(8, 8).unwrap();
        let pointers = LogAddressPointers::new(crate::RECORD_ALIGNMENT as u64);
        page_manager.allocate_page(0).unwrap();

        let functions = ByteUpsertFunctions;
        let key = b"key".to_vec();
        let input = b"value".to_vec();
        let mut output = Vec::new();
        let mut upsert_info = UpsertInfo::default();
        let key_hash = (21u64 << crate::HASH_TAG_SHIFT) | 0;

        let mut context = UpsertOperationContext {
            hash_index: &hash_index,
            page_manager: &mut page_manager,
            pointers: &pointers,
        };

        let status = upsert(
            &mut context,
            &functions,
            key_hash,
            &key,
            &input,
            &mut output,
            &mut upsert_info,
        )
        .unwrap();
        assert_eq!(status, UpsertOperationStatus::Inserted);

        let device = crate::InMemoryPageDevice::new(context.page_manager.page_size());
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
        assert!(matches!(
            read_status,
            ReadOperationStatus::FoundInMemory | ReadOperationStatus::FoundOnDisk
        ));
        assert_eq!(output, b"value");
    }

    #[test]
    fn upsert_in_place_updates_when_value_size_is_unchanged() {
        let hash_index = HashIndex::with_size_bits(2).unwrap();
        let mut page_manager = PageManager::new(8, 8).unwrap();
        let pointers = LogAddressPointers::new(crate::RECORD_ALIGNMENT as u64);
        page_manager.allocate_page(0).unwrap();

        let functions = ByteUpsertFunctions;
        let key = b"key".to_vec();
        let key_hash = (22u64 << crate::HASH_TAG_SHIFT) | 0;
        let mut output = Vec::new();
        let mut upsert_info = UpsertInfo::default();

        let mut context = UpsertOperationContext {
            hash_index: &hash_index,
            page_manager: &mut page_manager,
            pointers: &pointers,
        };

        upsert(
            &mut context,
            &functions,
            key_hash,
            &key,
            &b"abc".to_vec(),
            &mut output,
            &mut upsert_info,
        )
        .unwrap();
        let before = hash_index.find_tag_address(key_hash).unwrap();

        let status = upsert(
            &mut context,
            &functions,
            key_hash,
            &key,
            &b"xyz".to_vec(),
            &mut output,
            &mut upsert_info,
        )
        .unwrap();

        let after = hash_index.find_tag_address(key_hash).unwrap();
        assert_eq!(status, UpsertOperationStatus::InPlaceUpdated);
        assert_eq!(before, after);
    }

    #[test]
    fn upsert_copy_updates_when_value_size_changes() {
        let hash_index = HashIndex::with_size_bits(2).unwrap();
        let mut page_manager = PageManager::new(8, 8).unwrap();
        let pointers = LogAddressPointers::new(crate::RECORD_ALIGNMENT as u64);
        page_manager.allocate_page(0).unwrap();

        let functions = ByteUpsertFunctions;
        let key = b"key".to_vec();
        let key_hash = (23u64 << crate::HASH_TAG_SHIFT) | 0;
        let mut output = Vec::new();
        let mut upsert_info = UpsertInfo::default();

        let mut context = UpsertOperationContext {
            hash_index: &hash_index,
            page_manager: &mut page_manager,
            pointers: &pointers,
        };

        upsert(
            &mut context,
            &functions,
            key_hash,
            &key,
            &b"abc".to_vec(),
            &mut output,
            &mut upsert_info,
        )
        .unwrap();
        let before = hash_index.find_tag_address(key_hash).unwrap();

        let status = upsert(
            &mut context,
            &functions,
            key_hash,
            &key,
            &b"longer-value".to_vec(),
            &mut output,
            &mut upsert_info,
        )
        .unwrap();
        let after = hash_index.find_tag_address(key_hash).unwrap();

        assert_eq!(status, UpsertOperationStatus::CopiedToTail);
        assert_ne!(before, after);
    }

    #[test]
    fn upsert_after_tombstone_resurrects_key() {
        let hash_index = HashIndex::with_size_bits(2).unwrap();
        let mut page_manager = PageManager::new(8, 8).unwrap();
        let pointers = LogAddressPointers::new(crate::RECORD_ALIGNMENT as u64);
        page_manager.allocate_page(0).unwrap();

        let functions = ByteUpsertFunctions;
        let key = b"key".to_vec();
        let key_hash = (24u64 << crate::HASH_TAG_SHIFT) | 0;
        let mut output = Vec::new();
        let mut upsert_info = UpsertInfo::default();

        {
            let mut upsert_context = UpsertOperationContext {
                hash_index: &hash_index,
                page_manager: &mut page_manager,
                pointers: &pointers,
            };
            let status = upsert(
                &mut upsert_context,
                &functions,
                key_hash,
                &key,
                &b"abc".to_vec(),
                &mut output,
                &mut upsert_info,
            )
            .unwrap();
            assert_eq!(status, UpsertOperationStatus::Inserted);
        }

        {
            let mut delete_context = DeleteOperationContext {
                hash_index: &hash_index,
                page_manager: &mut page_manager,
                pointers: &pointers,
            };
            let mut delete_info = DeleteInfo::default();
            let status = delete(
                &mut delete_context,
                &functions,
                key_hash,
                &key,
                &mut delete_info,
            )
            .unwrap();
            assert!(matches!(
                status,
                DeleteOperationStatus::TombstonedInPlace | DeleteOperationStatus::AppendedTombstone
            ));
        }

        {
            let mut upsert_context = UpsertOperationContext {
                hash_index: &hash_index,
                page_manager: &mut page_manager,
                pointers: &pointers,
            };
            let status = upsert(
                &mut upsert_context,
                &functions,
                key_hash,
                &key,
                &b"xyz".to_vec(),
                &mut output,
                &mut upsert_info,
            )
            .unwrap();
            assert_eq!(status, UpsertOperationStatus::Inserted);
        }

        let device = crate::InMemoryPageDevice::new(page_manager.page_size());
        let mut read_context = ReadOperationContext {
            hash_index: &hash_index,
            page_manager: &mut page_manager,
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
        assert!(matches!(
            read_status,
            ReadOperationStatus::FoundInMemory | ReadOperationStatus::FoundOnDisk
        ));
        assert_eq!(output, b"xyz");
    }

    #[test]
    fn upsert_inserts_multiple_records_larger_than_half_page() {
        let hash_index = HashIndex::with_size_bits(2).unwrap();
        let mut page_manager = PageManager::new(9, 8).unwrap();
        let pointers = LogAddressPointers::new(crate::RECORD_ALIGNMENT as u64);
        page_manager.allocate_page(0).unwrap();

        let functions = ByteUpsertFunctions;
        let mut output = Vec::new();
        let mut upsert_info = UpsertInfo::default();
        let value = vec![b'x'; 300];

        let mut context = UpsertOperationContext {
            hash_index: &hash_index,
            page_manager: &mut page_manager,
            pointers: &pointers,
        };

        for i in 0u64..3 {
            let key = format!("k{i}").into_bytes();
            let key_hash = ((40 + i) << crate::HASH_TAG_SHIFT) | i;
            let status = upsert(
                &mut context,
                &functions,
                key_hash,
                &key,
                &value,
                &mut output,
                &mut upsert_info,
            )
            .unwrap();
            assert_eq!(status, UpsertOperationStatus::Inserted);
        }
    }
}
