//! Read operation path over hash index + hybrid log records.
//!
//! See [Doc 06 Section 2.12] for read flow and region-dependent reader callback selection.

use crate::HashIndex;
use crate::HashIndexError;
use crate::ISessionFunctions;
use crate::ReadInfo;
use crate::RecordInfo;
use crate::hybrid_log::LogAddressPointers;
use crate::hybrid_log::LogicalAddress;
use crate::hybrid_log::PageDevice;
use crate::hybrid_log::PageManager;
use crate::hybrid_log::PageManagerError;
use crate::hybrid_log::PageResidencyError;
use crate::hybrid_log::RecordFormatError;
use crate::hybrid_log::parse_key_span;
use crate::hybrid_log::parse_record_info;
use crate::hybrid_log::parse_record_layout;
use crate::hybrid_log::parse_value_span;
use crate::hybrid_log::read_with_callback;

pub trait HybridLogReadAdapter: ISessionFunctions {
    fn record_key_equals(&self, requested_key: &Self::Key, record_key: &[u8]) -> bool;
    fn value_from_record(&self, record_value: &[u8]) -> Self::Value;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadOperationStatus {
    FoundInMemory,
    FoundOnDisk,
    NotFound,
    RetryLater,
}

#[derive(Debug)]
pub enum ReadOperationError {
    HashIndex(HashIndexError),
    PageManager(PageManagerError),
    PageResidency(PageResidencyError),
    RecordFormat(RecordFormatError),
}

impl core::fmt::Display for ReadOperationError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::HashIndex(inner) => inner.fmt(f),
            Self::PageManager(inner) => inner.fmt(f),
            Self::PageResidency(inner) => inner.fmt(f),
            Self::RecordFormat(inner) => inner.fmt(f),
        }
    }
}

impl std::error::Error for ReadOperationError {}

impl From<HashIndexError> for ReadOperationError {
    fn from(value: HashIndexError) -> Self {
        Self::HashIndex(value)
    }
}

impl From<PageManagerError> for ReadOperationError {
    fn from(value: PageManagerError) -> Self {
        Self::PageManager(value)
    }
}

impl From<PageResidencyError> for ReadOperationError {
    fn from(value: PageResidencyError) -> Self {
        Self::PageResidency(value)
    }
}

impl From<RecordFormatError> for ReadOperationError {
    fn from(value: RecordFormatError) -> Self {
        Self::RecordFormat(value)
    }
}

pub struct ReadOperationContext<'a, D: PageDevice> {
    pub hash_index: &'a HashIndex,
    pub page_manager: &'a mut PageManager,
    pub pointers: &'a LogAddressPointers,
    pub device: &'a D,
}

struct MaterializedRecord {
    record_info: RecordInfo,
    key_bytes: Vec<u8>,
    value_bytes: Vec<u8>,
    from_disk: bool,
}

pub fn read<F, D>(
    context: &mut ReadOperationContext<'_, D>,
    functions: &F,
    key_hash: u64,
    key: &F::Key,
    input: &F::Input,
    output: &mut F::Output,
    read_info: &ReadInfo,
) -> Result<ReadOperationStatus, ReadOperationError>
where
    F: HybridLogReadAdapter,
    D: PageDevice,
{
    let mut current_address = match context.hash_index.find_tag_address(key_hash) {
        Some(address) => address,
        None => return Ok(ReadOperationStatus::NotFound),
    };

    while current_address != 0 {
        if current_address < context.pointers.begin_address() {
            return Ok(ReadOperationStatus::NotFound);
        }

        let record = materialize_record_at(context, current_address)?;
        if functions.record_key_equals(key, &record.key_bytes) {
            if record.record_info.is_closed() {
                return Ok(ReadOperationStatus::RetryLater);
            }
            if record.record_info.tombstone() || record.record_info.invalid() {
                return Ok(ReadOperationStatus::NotFound);
            }

            let value = functions.value_from_record(&record.value_bytes);
            let handled = if current_address >= context.pointers.safe_read_only_address() {
                functions.concurrent_reader(
                    key,
                    input,
                    &value,
                    output,
                    read_info,
                    &record.record_info,
                )
            } else {
                functions.single_reader(key, input, &value, output, read_info)
            };

            if !handled {
                return Ok(ReadOperationStatus::NotFound);
            }
            return Ok(if record.from_disk {
                ReadOperationStatus::FoundOnDisk
            } else {
                ReadOperationStatus::FoundInMemory
            });
        }

        current_address = record.record_info.previous_address();
    }

    Ok(ReadOperationStatus::NotFound)
}

fn materialize_record_at<D: PageDevice>(
    context: &mut ReadOperationContext<'_, D>,
    logical_address: u64,
) -> Result<MaterializedRecord, ReadOperationError> {
    let logical = LogicalAddress(logical_address);
    let page_space = context.page_manager.address_space();
    let (_, page_offset) = page_space.decode(logical);
    let page_offset = page_offset as usize;

    let head = context.pointers.head_address();
    let begin = context.pointers.begin_address();
    let from_disk = logical_address < head && logical_address >= begin;

    if from_disk {
        read_with_callback(
            context.page_manager,
            context.pointers,
            context.device,
            logical,
            1,
            |_| {},
        )?;
    }

    let available = context.page_manager.page_size() - page_offset;
    let source = context.page_manager.read_at(logical, available)?;
    let layout = parse_record_layout(source)?;
    let record_slice = &source[..layout.allocated_size];
    let record_info = parse_record_info(record_slice)?;
    let key = parse_key_span(record_slice)?;
    let value = parse_value_span(record_slice)?;

    Ok(MaterializedRecord {
        record_info,
        key_bytes: key.as_slice().to_vec(),
        value_bytes: value.as_slice().to_vec(),
        from_disk,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FindOrCreateTagResult;
    use crate::HashBucketEntry;
    use crate::RmwInfo;
    use crate::UpsertInfo;
    use crate::WriteReason;
    use crate::hybrid_log::InMemoryPageDevice;
    use crate::hybrid_log::flush_page_to_device;
    use crate::hybrid_log::write_record;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering as AtomicOrdering;

    #[derive(Default)]
    struct ByteSessionFunctions {
        single_reads: AtomicUsize,
        concurrent_reads: AtomicUsize,
    }

    impl ISessionFunctions for ByteSessionFunctions {
        type Key = Vec<u8>;
        type Value = Vec<u8>;
        type Input = ();
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
            self.single_reads.fetch_add(1, AtomicOrdering::Relaxed);
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
            self.concurrent_reads.fetch_add(1, AtomicOrdering::Relaxed);
            *output = value.clone();
            true
        }

        fn single_writer(
            &self,
            _key: &Self::Key,
            _input: &Self::Input,
            _src: &Self::Value,
            _dst: &mut Self::Value,
            _output: &mut Self::Output,
            _upsert_info: &mut UpsertInfo,
            _reason: WriteReason,
            _record_info: &mut RecordInfo,
        ) -> bool {
            false
        }

        fn concurrent_writer(
            &self,
            _key: &Self::Key,
            _input: &Self::Input,
            _src: &Self::Value,
            _dst: &mut Self::Value,
            _output: &mut Self::Output,
            _upsert_info: &mut UpsertInfo,
            _record_info: &mut RecordInfo,
        ) -> bool {
            false
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

    impl HybridLogReadAdapter for ByteSessionFunctions {
        fn record_key_equals(&self, requested_key: &Self::Key, record_key: &[u8]) -> bool {
            requested_key.as_slice() == record_key
        }

        fn value_from_record(&self, record_value: &[u8]) -> Self::Value {
            record_value.to_vec()
        }
    }

    fn install_record(
        hash_index: &HashIndex,
        page_manager: &mut PageManager,
        key_hash: u64,
        logical_address: LogicalAddress,
        key: &[u8],
        value: &[u8],
        record_info: RecordInfo,
    ) {
        let mut record_buffer = vec![0u8; 256];
        let layout = write_record(&mut record_buffer, record_info, key, value).unwrap();
        page_manager
            .write_at(logical_address, &record_buffer[..layout.allocated_size])
            .unwrap();

        let result = hash_index.find_or_create_tag(key_hash, 0).unwrap();
        set_hash_entry_address(hash_index, result, logical_address.raw());
    }

    fn set_hash_entry_address(
        hash_index: &HashIndex,
        result: FindOrCreateTagResult,
        logical_address: u64,
    ) {
        match result.overflow_bucket_address {
            Some(overflow_address) => {
                let handle = hash_index
                    .overflow_allocator()
                    .get(overflow_address)
                    .unwrap();
                let entry = handle.bucket().entry(result.slot).unwrap();
                let current = entry.load(AtomicOrdering::Acquire);
                let updated = HashBucketEntry::with_address(current, logical_address).unwrap();
                entry.store(updated, AtomicOrdering::Release);
            }
            None => {
                let entry = hash_index
                    .bucket(result.bucket_index)
                    .unwrap()
                    .entry(result.slot)
                    .unwrap();
                let current = entry.load(AtomicOrdering::Acquire);
                let updated = HashBucketEntry::with_address(current, logical_address).unwrap();
                entry.store(updated, AtomicOrdering::Release);
            }
        }
    }

    #[test]
    fn read_uses_concurrent_reader_in_mutable_region() {
        let hash_index = HashIndex::with_size_bits(2).unwrap();
        let mut page_manager = PageManager::new(8, 4).unwrap();
        page_manager.allocate_page(0).unwrap();

        let key_hash = (10u64 << crate::HASH_TAG_SHIFT) | 0;
        let logical_address = page_manager.address_space().encode(0, 8).unwrap();

        let mut info = RecordInfo::default();
        info.set_valid(true);
        install_record(
            &hash_index,
            &mut page_manager,
            key_hash,
            logical_address,
            b"foo",
            b"bar",
            info,
        );

        let pointers = LogAddressPointers::new(0);
        let device = InMemoryPageDevice::new(page_manager.page_size());
        let functions = ByteSessionFunctions::default();
        let key = b"foo".to_vec();
        let mut output = Vec::new();
        let read_info = ReadInfo::default();

        let mut context = ReadOperationContext {
            hash_index: &hash_index,
            page_manager: &mut page_manager,
            pointers: &pointers,
            device: &device,
        };

        let status = read(
            &mut context,
            &functions,
            key_hash,
            &key,
            &(),
            &mut output,
            &read_info,
        )
        .unwrap();

        assert_eq!(status, ReadOperationStatus::FoundInMemory);
        assert_eq!(output, b"bar");
        assert_eq!(functions.single_reads.load(AtomicOrdering::Relaxed), 0);
        assert_eq!(functions.concurrent_reads.load(AtomicOrdering::Relaxed), 1);
    }

    #[test]
    fn read_uses_single_reader_in_immutable_region() {
        let hash_index = HashIndex::with_size_bits(2).unwrap();
        let mut page_manager = PageManager::new(8, 4).unwrap();
        page_manager.allocate_page(0).unwrap();

        let key_hash = (11u64 << crate::HASH_TAG_SHIFT) | 0;
        let logical_address = page_manager.address_space().encode(0, 8).unwrap();

        let mut info = RecordInfo::default();
        info.set_valid(true);
        install_record(
            &hash_index,
            &mut page_manager,
            key_hash,
            logical_address,
            b"foo",
            b"baz",
            info,
        );

        let pointers = LogAddressPointers::new(0);
        pointers.shift_safe_read_only_address(1u64 << 8);

        let device = InMemoryPageDevice::new(page_manager.page_size());
        let functions = ByteSessionFunctions::default();
        let key = b"foo".to_vec();
        let mut output = Vec::new();
        let read_info = ReadInfo::default();

        let mut context = ReadOperationContext {
            hash_index: &hash_index,
            page_manager: &mut page_manager,
            pointers: &pointers,
            device: &device,
        };

        let status = read(
            &mut context,
            &functions,
            key_hash,
            &key,
            &(),
            &mut output,
            &read_info,
        )
        .unwrap();

        assert_eq!(status, ReadOperationStatus::FoundInMemory);
        assert_eq!(output, b"baz");
        assert_eq!(functions.single_reads.load(AtomicOrdering::Relaxed), 1);
        assert_eq!(functions.concurrent_reads.load(AtomicOrdering::Relaxed), 0);
    }

    #[test]
    fn read_loads_from_disk_when_address_is_below_head() {
        let hash_index = HashIndex::with_size_bits(2).unwrap();
        let mut page_manager = PageManager::new(8, 4).unwrap();
        page_manager.allocate_page(0).unwrap();

        let key_hash = (12u64 << crate::HASH_TAG_SHIFT) | 0;
        let logical_address = page_manager.address_space().encode(0, 8).unwrap();

        let mut info = RecordInfo::default();
        info.set_valid(true);
        install_record(
            &hash_index,
            &mut page_manager,
            key_hash,
            logical_address,
            b"foo",
            b"disk",
            info,
        );

        let device = InMemoryPageDevice::new(page_manager.page_size());
        flush_page_to_device(&mut page_manager, &device, 0).unwrap();
        page_manager.evict_page(0).unwrap();

        let pointers = LogAddressPointers::new(0);
        pointers.shift_head_address(1u64 << 8);
        pointers.shift_safe_read_only_address(1u64 << 8);

        let functions = ByteSessionFunctions::default();
        let key = b"foo".to_vec();
        let mut output = Vec::new();
        let read_info = ReadInfo::default();

        let mut context = ReadOperationContext {
            hash_index: &hash_index,
            page_manager: &mut page_manager,
            pointers: &pointers,
            device: &device,
        };

        let status = read(
            &mut context,
            &functions,
            key_hash,
            &key,
            &(),
            &mut output,
            &read_info,
        )
        .unwrap();

        assert_eq!(status, ReadOperationStatus::FoundOnDisk);
        assert_eq!(output, b"disk");
        assert_eq!(functions.single_reads.load(AtomicOrdering::Relaxed), 1);
    }

    #[test]
    fn read_returns_retry_later_for_closed_record() {
        let hash_index = HashIndex::with_size_bits(2).unwrap();
        let mut page_manager = PageManager::new(8, 4).unwrap();
        page_manager.allocate_page(0).unwrap();

        let key_hash = (13u64 << crate::HASH_TAG_SHIFT) | 0;
        let logical_address = page_manager.address_space().encode(0, 8).unwrap();

        let mut info = RecordInfo::default();
        info.set_valid(true);
        info.seal();
        install_record(
            &hash_index,
            &mut page_manager,
            key_hash,
            logical_address,
            b"foo",
            b"closed",
            info,
        );

        let pointers = LogAddressPointers::new(0);
        let device = InMemoryPageDevice::new(page_manager.page_size());
        let functions = ByteSessionFunctions::default();
        let key = b"foo".to_vec();
        let mut output = Vec::new();
        let read_info = ReadInfo::default();

        let mut context = ReadOperationContext {
            hash_index: &hash_index,
            page_manager: &mut page_manager,
            pointers: &pointers,
            device: &device,
        };

        let status = read(
            &mut context,
            &functions,
            key_hash,
            &key,
            &(),
            &mut output,
            &read_info,
        )
        .unwrap();

        assert_eq!(status, ReadOperationStatus::RetryLater);
    }
}
