//! RMW operation path with fuzzy-region retry handling.
//!
//! See [Doc 06 Section 2.12] for mutable/immutable/fuzzy-region behavior.

use crate::hybrid_log::{
    parse_key_span, parse_record_info, parse_record_layout, parse_value_span, write_record,
    LogAddressPointers, LogicalAddress, PageManager, PageManagerError, RecordFormatError,
    RecordLayout,
};
use crate::{HashIndex, HashIndexError, ISessionFunctions, RecordInfo, RmwInfo};
use garnet_common::SpanByteError;

pub trait HybridLogRmwAdapter: ISessionFunctions {
    fn record_key_equals(&self, requested_key: &Self::Key, record_key: &[u8]) -> bool;
    fn key_to_record_bytes(&self, key: &Self::Key) -> Vec<u8>;
    fn value_from_record(&self, record_value: &[u8]) -> Self::Value;
    fn value_to_record_bytes(&self, value: &Self::Value) -> Vec<u8>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RmwOperationStatus {
    InPlaceUpdated,
    CopiedToTail,
    Inserted,
    RetryLater,
    NotFound,
}

#[derive(Debug)]
pub enum RmwOperationError {
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

impl core::fmt::Display for RmwOperationError {
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
            Self::OperationCancelled => write!(f, "session function cancelled RMW operation"),
        }
    }
}

impl std::error::Error for RmwOperationError {}

impl From<HashIndexError> for RmwOperationError {
    fn from(value: HashIndexError) -> Self {
        Self::HashIndex(value)
    }
}

impl From<PageManagerError> for RmwOperationError {
    fn from(value: PageManagerError) -> Self {
        Self::PageManager(value)
    }
}

impl From<RecordFormatError> for RmwOperationError {
    fn from(value: RecordFormatError) -> Self {
        Self::RecordFormat(value)
    }
}

impl From<SpanByteError> for RmwOperationError {
    fn from(value: SpanByteError) -> Self {
        Self::SpanByte(value)
    }
}

pub struct RmwOperationContext<'a> {
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

pub fn rmw<F>(
    context: &mut RmwOperationContext<'_>,
    functions: &F,
    key_hash: u64,
    key: &F::Key,
    input: &F::Input,
    output: &mut F::Output,
    rmw_info: &mut RmwInfo,
) -> Result<RmwOperationStatus, RmwOperationError>
where
    F: HybridLogRmwAdapter,
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
                matched_address = Some(address);
                matched_record = Some(record);
            }
        }
    }

    if let (Some(address), Some(record), Some(head_entry)) =
        (matched_address, matched_record, maybe_head_entry)
    {
        if record.record_info.tombstone() || record.record_info.invalid() {
            return Ok(RmwOperationStatus::NotFound);
        }

        let safe_read_only = context.pointers.safe_read_only_address();
        let read_only = context.pointers.read_only_address();
        if address >= safe_read_only && address < read_only && !record.record_info.tombstone() {
            return Ok(RmwOperationStatus::RetryLater);
        }

        if address >= read_only {
            let old_value = functions.value_from_record(&record.value_bytes);
            let mut in_place_value = old_value.clone();
            let mut updated_record_info = record.record_info;
            if functions.in_place_updater(
                key,
                input,
                &mut in_place_value,
                output,
                rmw_info,
                &mut updated_record_info,
            ) {
                let new_value_bytes = functions.value_to_record_bytes(&in_place_value);
                if new_value_bytes.len() == record.value_bytes.len() {
                    rewrite_record(
                        context.page_manager,
                        LogicalAddress(address),
                        &key_bytes,
                        &new_value_bytes,
                        updated_record_info,
                        record.allocated_size,
                    )?;
                    return Ok(RmwOperationStatus::InPlaceUpdated);
                }
            }
        }

        let old_value = functions.value_from_record(&record.value_bytes);
        let mut new_value = old_value.clone();
        let mut new_record_info = RecordInfo::default();
        new_record_info.set_valid(true);
        new_record_info.set_previous_address(address);
        if !functions.copy_updater(
            key,
            input,
            &old_value,
            &mut new_value,
            output,
            rmw_info,
            &mut new_record_info,
        ) {
            return Err(RmwOperationError::OperationCancelled);
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
            return Err(RmwOperationError::CompareExchangeConflict);
        }

        return Ok(RmwOperationStatus::CopiedToTail);
    }

    let head_entry = if let Some(entry) = maybe_head_entry {
        entry
    } else {
        context
            .hash_index
            .find_or_create_tag(key_hash, context.pointers.begin_address())?;
        context
            .hash_index
            .find_tag_entry(key_hash)
            .ok_or(RmwOperationError::OperationCancelled)?
    };

    let old_value = F::Value::default();
    let mut new_value = F::Value::default();
    let mut new_record_info = RecordInfo::default();
    new_record_info.set_valid(true);
    new_record_info.set_previous_address(previous_address);
    if !functions.copy_updater(
        key,
        input,
        &old_value,
        &mut new_value,
        output,
        rmw_info,
        &mut new_record_info,
    ) {
        return Err(RmwOperationError::OperationCancelled);
    }

    let value_bytes = functions.value_to_record_bytes(&new_value);
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
        return Err(RmwOperationError::CompareExchangeConflict);
    }
    Ok(RmwOperationStatus::Inserted)
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
) -> Result<Option<(u64, MaterializedRecord)>, RmwOperationError>
where
    F: HybridLogRmwAdapter,
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
) -> Result<MaterializedRecord, RmwOperationError> {
    let page_space = page_manager.address_space();
    let (_, page_offset) = page_space.decode(logical);
    let page_offset = page_offset as usize;
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
) -> Result<(), RmwOperationError> {
    let mut record = vec![0u8; expected_allocated_size];
    let layout = write_record(&mut record, record_info, key_bytes, value_bytes)?;
    if layout.allocated_size != expected_allocated_size {
        return Err(RmwOperationError::OperationCancelled);
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
) -> Result<u64, RmwOperationError> {
    let layout = RecordLayout::for_payload_lengths(key_bytes.len(), value_bytes.len())?;
    let allocated_size = layout.allocated_size;
    if allocated_size > page_manager.page_size() {
        return Err(RmwOperationError::RecordTooLarge {
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
) -> Result<LogicalAddress, RmwOperationError> {
    loop {
        let old_tail = pointers.advance_tail_by(allocated_size as u64);
        let logical = LogicalAddress(old_tail);
        let space = page_manager.address_space();
        let (page_index, page_offset) = space.decode(logical);
        let page_offset = page_offset as usize;

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
    use crate::{upsert, HybridLogUpsertAdapter, UpsertOperationContext, UpsertOperationStatus};
    use crate::{UpsertInfo, WriteReason};

    struct ByteRmwFunctions;

    impl ISessionFunctions for ByteRmwFunctions {
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
            input: &Self::Input,
            value: &mut Self::Value,
            output: &mut Self::Output,
            _rmw_info: &mut RmwInfo,
            _record_info: &mut RecordInfo,
        ) -> bool {
            *value = input.clone();
            *output = value.clone();
            true
        }

        fn copy_updater(
            &self,
            _key: &Self::Key,
            input: &Self::Input,
            _old_value: &Self::Value,
            new_value: &mut Self::Value,
            output: &mut Self::Output,
            _rmw_info: &mut RmwInfo,
            _record_info: &mut RecordInfo,
        ) -> bool {
            *new_value = input.clone();
            *output = new_value.clone();
            true
        }

        fn single_deleter(
            &self,
            _key: &Self::Key,
            _value: &mut Self::Value,
            _delete_info: &mut crate::DeleteInfo,
            _record_info: &mut RecordInfo,
        ) -> bool {
            false
        }

        fn concurrent_deleter(
            &self,
            _key: &Self::Key,
            _value: &mut Self::Value,
            _delete_info: &mut crate::DeleteInfo,
            _record_info: &mut RecordInfo,
        ) -> bool {
            false
        }
    }

    impl HybridLogRmwAdapter for ByteRmwFunctions {
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

    impl HybridLogUpsertAdapter for ByteRmwFunctions {
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

    fn seed_record(
        hash_index: &HashIndex,
        page_manager: &mut PageManager,
        pointers: &LogAddressPointers,
        key_hash: u64,
        key: &[u8],
        value: &[u8],
    ) {
        let functions = ByteRmwFunctions;
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
    fn rmw_returns_retry_later_in_fuzzy_region() {
        let hash_index = HashIndex::with_size_bits(2).unwrap();
        let mut page_manager = PageManager::new(8, 8).unwrap();
        page_manager.allocate_page(0).unwrap();
        let pointers = LogAddressPointers::new(crate::RECORD_ALIGNMENT as u64);

        let key_hash = (31u64 << crate::HASH_TAG_SHIFT) | 0;
        seed_record(
            &hash_index,
            &mut page_manager,
            &pointers,
            key_hash,
            b"key",
            b"aaa",
        );

        pointers.shift_safe_read_only_address(0);
        pointers.shift_read_only_address(128);

        let functions = ByteRmwFunctions;
        let mut rmw_info = RmwInfo::default();
        let mut output = Vec::new();
        let key = b"key".to_vec();
        let input = b"bbb".to_vec();

        let mut context = RmwOperationContext {
            hash_index: &hash_index,
            page_manager: &mut page_manager,
            pointers: &pointers,
        };

        let status = rmw(
            &mut context,
            &functions,
            key_hash,
            &key,
            &input,
            &mut output,
            &mut rmw_info,
        )
        .unwrap();

        assert_eq!(status, RmwOperationStatus::RetryLater);
    }

    #[test]
    fn rmw_in_place_updates_in_mutable_region() {
        let hash_index = HashIndex::with_size_bits(2).unwrap();
        let mut page_manager = PageManager::new(8, 8).unwrap();
        page_manager.allocate_page(0).unwrap();
        let pointers = LogAddressPointers::new(crate::RECORD_ALIGNMENT as u64);

        let key_hash = (32u64 << crate::HASH_TAG_SHIFT) | 0;
        seed_record(
            &hash_index,
            &mut page_manager,
            &pointers,
            key_hash,
            b"key",
            b"aaa",
        );
        pointers.shift_read_only_address(0);
        pointers.shift_safe_read_only_address(0);

        let before = hash_index.find_tag_address(key_hash).unwrap();

        let functions = ByteRmwFunctions;
        let mut rmw_info = RmwInfo::default();
        let mut output = Vec::new();
        let key = b"key".to_vec();
        let input = b"bbb".to_vec();

        let mut context = RmwOperationContext {
            hash_index: &hash_index,
            page_manager: &mut page_manager,
            pointers: &pointers,
        };
        let status = rmw(
            &mut context,
            &functions,
            key_hash,
            &key,
            &input,
            &mut output,
            &mut rmw_info,
        )
        .unwrap();
        let after = hash_index.find_tag_address(key_hash).unwrap();

        assert_eq!(status, RmwOperationStatus::InPlaceUpdated);
        assert_eq!(before, after);
    }

    #[test]
    fn rmw_copy_updates_outside_mutable_region() {
        let hash_index = HashIndex::with_size_bits(2).unwrap();
        let mut page_manager = PageManager::new(8, 8).unwrap();
        page_manager.allocate_page(0).unwrap();
        let pointers = LogAddressPointers::new(crate::RECORD_ALIGNMENT as u64);

        let key_hash = (33u64 << crate::HASH_TAG_SHIFT) | 0;
        seed_record(
            &hash_index,
            &mut page_manager,
            &pointers,
            key_hash,
            b"key",
            b"aaa",
        );
        pointers.shift_safe_read_only_address(64);
        pointers.shift_read_only_address(128);

        let before = hash_index.find_tag_address(key_hash).unwrap();

        let functions = ByteRmwFunctions;
        let mut rmw_info = RmwInfo::default();
        let mut output = Vec::new();
        let key = b"key".to_vec();
        let input = b"longer-value".to_vec();

        let mut context = RmwOperationContext {
            hash_index: &hash_index,
            page_manager: &mut page_manager,
            pointers: &pointers,
        };
        let status = rmw(
            &mut context,
            &functions,
            key_hash,
            &key,
            &input,
            &mut output,
            &mut rmw_info,
        )
        .unwrap();
        let after = hash_index.find_tag_address(key_hash).unwrap();

        assert_eq!(status, RmwOperationStatus::CopiedToTail);
        assert_ne!(before, after);
    }
}
