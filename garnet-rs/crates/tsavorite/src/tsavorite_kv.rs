//! High-level Tsavorite key-value facade.
//!
//! This wraps hash-index + hybrid-log operation modules into a single store/session API.

use crate::DeleteInfo;
use crate::HashIndex;
use crate::HashIndexError;
use crate::HybridLogDeleteAdapter;
use crate::HybridLogReadAdapter;
use crate::HybridLogRmwAdapter;
use crate::HybridLogUpsertAdapter;
use crate::LightEpoch;
use crate::ReadInfo;
use crate::RmwInfo;
use crate::UpsertInfo;
use crate::checkpoint_state_machine::CheckpointMode;
use crate::checkpoint_state_machine::CheckpointState;
use crate::checkpoint_state_machine::CheckpointStateMachine;
use crate::checkpoint_state_machine::CheckpointTransitionError;
use crate::delete_operation::DeleteOperationContext;
use crate::delete_operation::DeleteOperationError;
use crate::delete_operation::DeleteOperationStatus;
use crate::hybrid_log::InMemoryPageDevice;
use crate::hybrid_log::LogAddressPointers;
use crate::hybrid_log::LogAddressPointersSnapshot;
use crate::hybrid_log::PageDevice;
use crate::hybrid_log::PageManager;
use crate::hybrid_log::PageManagerError;
use crate::hybrid_log::PageResidencyError;
use crate::hybrid_log::shift_head_address_and_evict;
use crate::read_operation::ReadOperationContext;
use crate::read_operation::ReadOperationError;
use crate::read_operation::ReadOperationStatus;
use crate::rmw_operation::RmwOperationContext;
use crate::rmw_operation::RmwOperationError;
use crate::rmw_operation::RmwOperationStatus;
use crate::upsert_operation::UpsertOperationContext;
use crate::upsert_operation::UpsertOperationError;
use crate::upsert_operation::UpsertOperationStatus;
use core::marker::PhantomData;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TsavoriteKvConfig {
    pub hash_index_size_bits: u8,
    pub page_size_bits: u8,
    pub max_in_memory_pages: usize,
    pub initial_logical_address: u64,
}

impl Default for TsavoriteKvConfig {
    fn default() -> Self {
        Self {
            hash_index_size_bits: 10,
            page_size_bits: 12,
            max_in_memory_pages: 64,
            initial_logical_address: crate::RECORD_ALIGNMENT as u64,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TsavoriteKvInitError {
    HashIndex(HashIndexError),
    PageManager(PageManagerError),
    InvalidInitialAddress {
        initial_logical_address: u64,
        record_alignment: usize,
    },
}

impl core::fmt::Display for TsavoriteKvInitError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::HashIndex(inner) => inner.fmt(f),
            Self::PageManager(inner) => inner.fmt(f),
            Self::InvalidInitialAddress {
                initial_logical_address,
                record_alignment,
            } => write!(
                f,
                "invalid initial logical address {} (must be non-zero and {}-byte aligned)",
                initial_logical_address, record_alignment
            ),
        }
    }
}

impl std::error::Error for TsavoriteKvInitError {}

impl From<HashIndexError> for TsavoriteKvInitError {
    fn from(value: HashIndexError) -> Self {
        Self::HashIndex(value)
    }
}

impl From<PageManagerError> for TsavoriteKvInitError {
    fn from(value: PageManagerError) -> Self {
        Self::PageManager(value)
    }
}

pub struct TsavoriteKV<K, V, D = InMemoryPageDevice>
where
    D: PageDevice,
{
    hash_index: HashIndex,
    page_manager: PageManager,
    pointers: LogAddressPointers,
    device: D,
    epoch: LightEpoch,
    checkpoint_state: CheckpointStateMachine,
    checkpoint_wait_flush_epoch: Option<(u64, u64)>,
    _marker: PhantomData<(K, V)>,
}

impl<K, V> TsavoriteKV<K, V, InMemoryPageDevice> {
    pub fn new(config: TsavoriteKvConfig) -> Result<Self, TsavoriteKvInitError> {
        let page_manager = PageManager::new(config.page_size_bits, config.max_in_memory_pages)?;
        let device = InMemoryPageDevice::new(page_manager.page_size());
        Self::build(config, page_manager, device)
    }
}

impl<K, V, D> TsavoriteKV<K, V, D>
where
    D: PageDevice,
{
    pub fn with_device(config: TsavoriteKvConfig, device: D) -> Result<Self, TsavoriteKvInitError> {
        let page_manager = PageManager::new(config.page_size_bits, config.max_in_memory_pages)?;
        Self::build(config, page_manager, device)
    }

    fn build(
        config: TsavoriteKvConfig,
        page_manager: PageManager,
        device: D,
    ) -> Result<Self, TsavoriteKvInitError> {
        if config.initial_logical_address == 0
            || config.initial_logical_address % (crate::RECORD_ALIGNMENT as u64) != 0
        {
            return Err(TsavoriteKvInitError::InvalidInitialAddress {
                initial_logical_address: config.initial_logical_address,
                record_alignment: crate::RECORD_ALIGNMENT,
            });
        }

        Ok(Self {
            hash_index: HashIndex::with_size_bits(config.hash_index_size_bits)?,
            page_manager,
            pointers: LogAddressPointers::new(config.initial_logical_address),
            device,
            epoch: LightEpoch::new(),
            checkpoint_state: CheckpointStateMachine::new(),
            checkpoint_wait_flush_epoch: None,
            _marker: PhantomData,
        })
    }

    #[inline]
    pub fn hash_index(&self) -> &HashIndex {
        &self.hash_index
    }

    #[inline]
    pub fn page_manager(&self) -> &PageManager {
        &self.page_manager
    }

    #[inline]
    pub fn pointers_snapshot(&self) -> LogAddressPointersSnapshot {
        self.pointers.snapshot()
    }

    #[inline]
    pub fn epoch(&self) -> &LightEpoch {
        &self.epoch
    }

    #[inline]
    pub fn bump_epoch(&self) -> u64 {
        self.epoch.bump_current_epoch()
    }

    #[inline]
    pub fn checkpoint_state(&self) -> CheckpointState {
        self.checkpoint_state.state()
    }

    #[inline]
    pub fn checkpoint_begin(
        &mut self,
        mode: CheckpointMode,
    ) -> Result<u64, CheckpointTransitionError> {
        let token = self.checkpoint_state.begin(mode)?;
        let barrier_epoch = self.epoch.bump_current_epoch();
        self.checkpoint_wait_flush_epoch = Some((token, barrier_epoch));
        Ok(token)
    }

    #[inline]
    pub fn checkpoint_mark_in_progress(
        &mut self,
        token: u64,
    ) -> Result<(), CheckpointTransitionError> {
        self.checkpoint_state.mark_in_progress(token)
    }

    #[inline]
    pub fn checkpoint_mark_wait_flush(
        &mut self,
        token: u64,
    ) -> Result<(), CheckpointTransitionError> {
        self.checkpoint_state.mark_wait_flush(token)
    }

    #[inline]
    pub fn checkpoint_try_mark_wait_flush(
        &mut self,
        token: u64,
    ) -> Result<bool, CheckpointTransitionError> {
        let Some((expected_token, barrier_epoch)) = self.checkpoint_wait_flush_epoch else {
            self.checkpoint_state.mark_wait_flush(token)?;
            return Ok(true);
        };
        if expected_token != token {
            return Err(CheckpointTransitionError::TokenMismatch {
                expected: expected_token,
                actual: token,
            });
        }
        if self.epoch.safe_to_reclaim_epoch() < barrier_epoch {
            return Ok(false);
        }
        self.checkpoint_state.mark_wait_flush(token)?;
        Ok(true)
    }

    #[inline]
    pub fn checkpoint_mark_persistence_callback(
        &mut self,
        token: u64,
    ) -> Result<(), CheckpointTransitionError> {
        self.checkpoint_state.mark_persistence_callback(token)
    }

    #[inline]
    pub fn checkpoint_complete(&mut self, token: u64) -> Result<(), CheckpointTransitionError> {
        self.checkpoint_state.complete(token)?;
        self.checkpoint_wait_flush_epoch = None;
        Ok(())
    }

    #[inline]
    pub fn checkpoint_abort(&mut self, token: u64) -> Result<(), CheckpointTransitionError> {
        self.checkpoint_state.abort(token)?;
        self.checkpoint_wait_flush_epoch = None;
        Ok(())
    }

    #[inline]
    pub fn shift_read_only_address(&self, new_read_only_address: u64) -> bool {
        self.pointers.shift_read_only_address(new_read_only_address)
    }

    #[inline]
    pub fn shift_safe_read_only_address(&self, new_safe_read_only_address: u64) -> bool {
        self.pointers
            .shift_safe_read_only_address(new_safe_read_only_address)
    }

    #[inline]
    pub fn shift_head_address_and_evict(
        &mut self,
        new_head_address: u64,
    ) -> Result<Vec<u64>, PageResidencyError> {
        shift_head_address_and_evict(
            &mut self.page_manager,
            &self.pointers,
            &self.device,
            new_head_address,
        )
    }

    #[inline]
    pub fn session<'a, F>(&'a mut self, functions: &'a F) -> TsavoriteSession<'a, K, V, D, F> {
        TsavoriteSession {
            store: self,
            functions,
        }
    }
}

pub struct TsavoriteSession<'a, K, V, D, F>
where
    D: PageDevice,
{
    store: &'a mut TsavoriteKV<K, V, D>,
    functions: &'a F,
}

impl<K, V, D, F> TsavoriteSession<'_, K, V, D, F>
where
    D: PageDevice,
{
    pub fn read(
        &mut self,
        key: &K,
        input: &F::Input,
        output: &mut F::Output,
        read_info: &ReadInfo,
    ) -> Result<ReadOperationStatus, ReadOperationError>
    where
        K: Hash,
        F: HybridLogReadAdapter<Key = K, Value = V>,
    {
        let _guard = self.store.epoch.pin();
        let key_hash = hash_key(key);
        let mut context = ReadOperationContext {
            hash_index: &self.store.hash_index,
            page_manager: &mut self.store.page_manager,
            pointers: &self.store.pointers,
            device: &self.store.device,
        };
        crate::read(
            &mut context,
            self.functions,
            key_hash,
            key,
            input,
            output,
            read_info,
        )
    }

    pub fn upsert(
        &mut self,
        key: &K,
        input: &F::Input,
        output: &mut F::Output,
        upsert_info: &mut UpsertInfo,
    ) -> Result<UpsertOperationStatus, UpsertOperationError>
    where
        K: Hash,
        V: Clone + Default,
        F: HybridLogUpsertAdapter<Key = K, Value = V>,
    {
        let _guard = self.store.epoch.pin();
        let key_hash = hash_key(key);
        let mut context = UpsertOperationContext {
            hash_index: &self.store.hash_index,
            page_manager: &mut self.store.page_manager,
            pointers: &self.store.pointers,
        };
        crate::upsert(
            &mut context,
            self.functions,
            key_hash,
            key,
            input,
            output,
            upsert_info,
        )
    }

    pub fn rmw(
        &mut self,
        key: &K,
        input: &F::Input,
        output: &mut F::Output,
        rmw_info: &mut RmwInfo,
    ) -> Result<RmwOperationStatus, RmwOperationError>
    where
        K: Hash,
        V: Clone + Default,
        F: HybridLogRmwAdapter<Key = K, Value = V>,
    {
        let _guard = self.store.epoch.pin();
        let key_hash = hash_key(key);
        let mut context = RmwOperationContext {
            hash_index: &self.store.hash_index,
            page_manager: &mut self.store.page_manager,
            pointers: &self.store.pointers,
        };
        crate::rmw(
            &mut context,
            self.functions,
            key_hash,
            key,
            input,
            output,
            rmw_info,
        )
    }

    pub fn delete(
        &mut self,
        key: &K,
        delete_info: &mut DeleteInfo,
    ) -> Result<DeleteOperationStatus, DeleteOperationError>
    where
        K: Hash,
        V: Clone + Default,
        F: HybridLogDeleteAdapter<Key = K, Value = V>,
    {
        let _guard = self.store.epoch.pin();
        let key_hash = hash_key(key);
        let mut context = DeleteOperationContext {
            hash_index: &self.store.hash_index,
            page_manager: &mut self.store.page_manager,
            pointers: &self.store.pointers,
        };
        crate::delete(&mut context, self.functions, key_hash, key, delete_info)
    }
}

fn hash_key<K: Hash>(key: &K) -> u64 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ISessionFunctions;
    use crate::RecordInfo;
    use crate::WriteReason;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::thread;

    struct ByteSessionFunctions;

    impl ISessionFunctions for ByteSessionFunctions {
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
            value: &mut Self::Value,
            _delete_info: &mut DeleteInfo,
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
            _delete_info: &mut DeleteInfo,
            record_info: &mut RecordInfo,
        ) -> bool {
            value.clear();
            record_info.set_tombstone();
            true
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

    impl HybridLogUpsertAdapter for ByteSessionFunctions {
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

    impl HybridLogRmwAdapter for ByteSessionFunctions {
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

    impl HybridLogDeleteAdapter for ByteSessionFunctions {
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

    fn test_config() -> TsavoriteKvConfig {
        TsavoriteKvConfig {
            hash_index_size_bits: 3,
            page_size_bits: 8,
            max_in_memory_pages: 16,
            initial_logical_address: crate::RECORD_ALIGNMENT as u64,
        }
    }

    #[test]
    fn rejects_invalid_initial_address() {
        let config = TsavoriteKvConfig {
            initial_logical_address: 0,
            ..test_config()
        };
        let err = TsavoriteKV::<Vec<u8>, Vec<u8>>::new(config).err().unwrap();
        assert!(matches!(
            err,
            TsavoriteKvInitError::InvalidInitialAddress { .. }
        ));
    }

    #[test]
    fn session_wraps_crud_operations() {
        let mut store = TsavoriteKV::<Vec<u8>, Vec<u8>>::new(test_config()).unwrap();
        let functions = ByteSessionFunctions;
        let key = b"key".to_vec();
        let mut output = Vec::new();

        {
            let mut session = store.session(&functions);
            let mut upsert_info = UpsertInfo::default();
            let upsert_status = session
                .upsert(&key, &b"value".to_vec(), &mut output, &mut upsert_info)
                .unwrap();
            assert!(matches!(
                upsert_status,
                UpsertOperationStatus::Inserted | UpsertOperationStatus::CopiedToTail
            ));
        }

        {
            let mut session = store.session(&functions);
            let read_status = session
                .read(
                    &key,
                    &Vec::new(),
                    &mut output,
                    &ReadInfo {
                        logical_address: 0,
                        user_data: 0,
                    },
                )
                .unwrap();
            assert_eq!(read_status, ReadOperationStatus::FoundInMemory);
            assert_eq!(output, b"value".to_vec());
        }

        {
            let mut session = store.session(&functions);
            let mut rmw_info = RmwInfo::default();
            let rmw_status = session
                .rmw(&key, &b"value2".to_vec(), &mut output, &mut rmw_info)
                .unwrap();
            assert!(matches!(
                rmw_status,
                RmwOperationStatus::InPlaceUpdated | RmwOperationStatus::CopiedToTail
            ));
            assert_eq!(output, b"value2".to_vec());
        }

        {
            let mut session = store.session(&functions);
            let mut delete_info = DeleteInfo::default();
            let delete_status = session.delete(&key, &mut delete_info).unwrap();
            assert!(matches!(
                delete_status,
                DeleteOperationStatus::TombstonedInPlace | DeleteOperationStatus::AppendedTombstone
            ));
        }

        {
            let mut session = store.session(&functions);
            let read_status = session
                .read(
                    &key,
                    &Vec::new(),
                    &mut output,
                    &ReadInfo {
                        logical_address: 0,
                        user_data: 0,
                    },
                )
                .unwrap();
            assert_eq!(read_status, ReadOperationStatus::NotFound);
        }
    }

    #[test]
    fn rmw_returns_retry_later_in_fuzzy_region_via_facade() {
        let mut store = TsavoriteKV::<Vec<u8>, Vec<u8>>::new(test_config()).unwrap();
        let functions = ByteSessionFunctions;
        let key = b"k-fuzzy".to_vec();
        let mut output = Vec::new();

        {
            let mut session = store.session(&functions);
            let mut upsert_info = UpsertInfo::default();
            session
                .upsert(&key, &b"seed".to_vec(), &mut output, &mut upsert_info)
                .unwrap();
        }

        let key_hash = hash_key(&key);
        let address = store
            .hash_index()
            .find_tag_address(key_hash)
            .expect("seeded key address");
        let _ = store.shift_safe_read_only_address(address);
        assert!(store.shift_read_only_address(address + crate::RECORD_ALIGNMENT as u64));

        {
            let mut session = store.session(&functions);
            let mut rmw_info = RmwInfo::default();
            let status = session
                .rmw(&key, &b"next".to_vec(), &mut output, &mut rmw_info)
                .unwrap();
            assert_eq!(status, RmwOperationStatus::RetryLater);
        }
    }

    #[test]
    fn checkpoint_state_machine_transitions_via_kv_facade() {
        let mut store = TsavoriteKV::<Vec<u8>, Vec<u8>>::new(test_config()).unwrap();
        let token = store.checkpoint_begin(CheckpointMode::FoldOver).unwrap();
        assert_eq!(
            store.checkpoint_state(),
            CheckpointState::Prepare {
                token,
                mode: CheckpointMode::FoldOver,
            }
        );
        store.checkpoint_mark_in_progress(token).unwrap();
        store.checkpoint_mark_wait_flush(token).unwrap();
        store.checkpoint_mark_persistence_callback(token).unwrap();
        store.checkpoint_complete(token).unwrap();
        assert_eq!(store.checkpoint_state(), CheckpointState::Rest);
    }

    #[test]
    fn checkpoint_wait_flush_can_be_gated_by_safe_epoch() {
        let mut store = TsavoriteKV::<Vec<u8>, Vec<u8>>::new(test_config()).unwrap();
        let token = store.checkpoint_begin(CheckpointMode::Snapshot).unwrap();
        store.checkpoint_mark_in_progress(token).unwrap();
        assert!(!store.checkpoint_try_mark_wait_flush(token).unwrap());
        store.bump_epoch();
        assert!(store.checkpoint_try_mark_wait_flush(token).unwrap());
    }

    #[test]
    fn concurrent_read_write_stress_via_session() {
        let store = Arc::new(Mutex::new(
            TsavoriteKV::<Vec<u8>, Vec<u8>>::new(test_config()).unwrap(),
        ));
        let functions = Arc::new(ByteSessionFunctions);
        let mut threads = Vec::new();

        for worker_id in 0u8..4 {
            let shared_store = Arc::clone(&store);
            let shared_functions = Arc::clone(&functions);
            threads.push(thread::spawn(move || {
                for i in 0u8..64 {
                    let key = format!("key-{}", i % 16).into_bytes();
                    let value = vec![worker_id, i];
                    let mut output = Vec::new();

                    let mut guard = shared_store.lock().expect("store mutex poisoned");
                    let mut session = guard.session(shared_functions.as_ref());
                    let mut upsert_info = UpsertInfo::default();
                    session
                        .upsert(&key, &value, &mut output, &mut upsert_info)
                        .unwrap();

                    let read_status = session
                        .read(
                            &key,
                            &Vec::new(),
                            &mut output,
                            &ReadInfo {
                                logical_address: 0,
                                user_data: 0,
                            },
                        )
                        .unwrap();
                    assert_eq!(read_status, ReadOperationStatus::FoundInMemory);
                }
            }));
        }

        for handle in threads {
            handle.join().expect("worker panicked");
        }

        for i in 0u8..16 {
            let key = format!("key-{}", i).into_bytes();
            let mut output = Vec::new();
            let mut guard = store.lock().expect("store mutex poisoned");
            let mut session = guard.session(functions.as_ref());
            let read_status = session
                .read(
                    &key,
                    &Vec::new(),
                    &mut output,
                    &ReadInfo {
                        logical_address: 0,
                        user_data: 0,
                    },
                )
                .unwrap();
            assert_eq!(read_status, ReadOperationStatus::FoundInMemory);
            assert_eq!(output.len(), 2);
        }
    }
}
