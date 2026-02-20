//! Request lifecycle: parse result -> dispatch -> storage op -> RESP response.

use crate::debug_concurrency::{LockClass, OrderedMutex, OrderedMutexGuard};
use crate::{dispatch_from_arg_slices, CommandId};
use garnet_cluster::redis_hash_slot;
use garnet_common::ArgSlice;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::mem::size_of;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::OnceLock;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tsavorite::{
    DeleteInfo, DeleteOperationError, DeleteOperationStatus, HybridLogDeleteAdapter,
    HybridLogReadAdapter, HybridLogRmwAdapter, HybridLogUpsertAdapter, ISessionFunctions,
    PageManagerError, PageResidencyError, ReadInfo, ReadOperationError, ReadOperationStatus,
    RecordInfo, RmwInfo, RmwOperationError, RmwOperationStatus, TsavoriteKV, TsavoriteKvConfig,
    TsavoriteKvInitError, UpsertInfo, UpsertOperationError, WriteReason,
};

const UPSERT_USER_DATA_HAS_EXPIRATION: u8 = 0x1;
const VALUE_EXPIRATION_PREFIX_LEN: usize = size_of::<u64>();
const HASH_OBJECT_TYPE_TAG: u8 = 3;
const LIST_OBJECT_TYPE_TAG: u8 = 2;
const SET_OBJECT_TYPE_TAG: u8 = 4;
const ZSET_OBJECT_TYPE_TAG: u8 = 5;
const WATCH_VERSION_MAP_SIZE: usize = 1024;
const WATCH_VERSION_MAP_MASK: usize = WATCH_VERSION_MAP_SIZE - 1;
const GARNET_HASH_INDEX_SIZE_BITS_ENV: &str = "GARNET_TSAVORITE_HASH_INDEX_SIZE_BITS";
const GARNET_PAGE_SIZE_BITS_ENV: &str = "GARNET_TSAVORITE_PAGE_SIZE_BITS";
const GARNET_MAX_IN_MEMORY_PAGES_ENV: &str = "GARNET_TSAVORITE_MAX_IN_MEMORY_PAGES";
const GARNET_STRING_STORE_SHARDS_ENV: &str = "GARNET_TSAVORITE_STRING_STORE_SHARDS";
const GARNET_STRING_OWNER_THREADS_ENV: &str = "GARNET_STRING_OWNER_THREADS";
const DEFAULT_SERVER_HASH_INDEX_SIZE_BITS: u8 = 16;
const DEFAULT_STRING_STORE_SHARDS: usize = 2;
const SINGLE_OWNER_THREAD_STRING_STORE_SHARDS: usize = 1;
const GARNET_LOG_STORAGE_FAILURES_ENV: &str = "GARNET_LOG_STORAGE_FAILURES";
const STORAGE_FAILURE_LOG_LIMIT: usize = 64;
static STORAGE_FAILURE_LOG_ENABLED: OnceLock<bool> = OnceLock::new();
static STORAGE_FAILURE_LOG_COUNT: AtomicUsize = AtomicUsize::new(0);

mod hash_commands;
mod list_commands;
mod server_commands;
mod set_commands;
mod string_commands;
mod zset_commands;

#[derive(Debug, Clone, Copy)]
struct ExpirationMetadata {
    deadline: Instant,
    unix_millis: u64,
}

#[derive(Debug)]
pub enum RequestProcessorInitError {
    Tsavorite(TsavoriteKvInitError),
}

impl core::fmt::Display for RequestProcessorInitError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Tsavorite(inner) => inner.fmt(f),
        }
    }
}

impl std::error::Error for RequestProcessorInitError {}

impl From<TsavoriteKvInitError> for RequestProcessorInitError {
    fn from(value: TsavoriteKvInitError) -> Self {
        Self::Tsavorite(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MigrationValue {
    String(Vec<u8>),
    Object { object_type: u8, payload: Vec<u8> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationEntry {
    pub key: Vec<u8>,
    pub value: MigrationValue,
    pub expiration_unix_millis: Option<u64>,
}

pub struct RequestProcessor {
    string_stores: Vec<OrderedMutex<TsavoriteKV<Vec<u8>, Vec<u8>>>>,
    object_store: OrderedMutex<TsavoriteKV<Vec<u8>, Vec<u8>>>,
    string_expirations: Vec<OrderedMutex<HashMap<Vec<u8>, Instant>>>,
    string_expiration_counts: Vec<AtomicUsize>,
    string_key_registries: Vec<OrderedMutex<HashSet<Vec<u8>>>>,
    object_key_registry: OrderedMutex<HashSet<Vec<u8>>>,
    watch_versions: Vec<AtomicU64>,
    functions: KvSessionFunctions,
    object_functions: ObjectSessionFunctions,
}

impl RequestProcessor {
    pub fn new() -> Result<Self, RequestProcessorInitError> {
        Self::new_with_string_store_shards(string_store_shard_count_from_env())
    }

    fn new_with_string_store_shards(
        store_shard_count: usize,
    ) -> Result<Self, RequestProcessorInitError> {
        let store_shard_count = store_shard_count.max(1);
        let store_config = tsavorite_config_from_env();
        let mut string_store_config = store_config;
        string_store_config.hash_index_size_bits =
            scale_hash_index_bits_for_shards(store_config.hash_index_size_bits, store_shard_count);
        let mut string_stores = Vec::with_capacity(store_shard_count);
        let mut string_expirations = Vec::with_capacity(store_shard_count);
        let mut string_expiration_counts = Vec::with_capacity(store_shard_count);
        let mut string_key_registries = Vec::with_capacity(store_shard_count);
        for _ in 0..store_shard_count {
            string_stores.push(OrderedMutex::new(
                TsavoriteKV::new(string_store_config)?,
                LockClass::Store,
                "request_processor.store",
            ));
            string_expirations.push(OrderedMutex::new(
                HashMap::new(),
                LockClass::Expirations,
                "request_processor.expirations",
            ));
            string_expiration_counts.push(AtomicUsize::new(0));
            string_key_registries.push(OrderedMutex::new(
                HashSet::new(),
                LockClass::KeyRegistry,
                "request_processor.key_registry",
            ));
        }
        Ok(Self {
            string_stores,
            string_expirations,
            string_expiration_counts,
            string_key_registries,
            object_store: OrderedMutex::new(
                TsavoriteKV::new(store_config)?,
                LockClass::ObjectStore,
                "request_processor.object_store",
            ),
            object_key_registry: OrderedMutex::new(
                HashSet::new(),
                LockClass::ObjectKeyRegistry,
                "request_processor.object_key_registry",
            ),
            watch_versions: (0..WATCH_VERSION_MAP_SIZE)
                .map(|_| AtomicU64::new(0))
                .collect(),
            functions: KvSessionFunctions,
            object_functions: ObjectSessionFunctions,
        })
    }

    pub fn watch_key_version(&self, key: &[u8]) -> u64 {
        let slot = watch_version_slot(key);
        self.watch_versions[slot].load(Ordering::SeqCst)
    }

    pub fn watch_versions_match(&self, watched_keys: &[(Vec<u8>, u64)]) -> bool {
        watched_keys
            .iter()
            .all(|(key, expected)| self.watch_key_version(key) == *expected)
    }

    #[inline]
    pub fn string_store_shard_count(&self) -> usize {
        self.string_stores.len()
    }

    #[inline]
    pub fn string_store_shard_index(&self, key: &[u8]) -> usize {
        self.string_store_shard_index_for_key(key)
    }

    pub fn execute(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.is_empty() {
            return Err(RequestExecutionError::UnknownCommand);
        }

        // SAFETY: caller ensures ArgSlice points into a live request buffer.
        let command = unsafe { dispatch_from_arg_slices(args) };
        match command {
            CommandId::Get => self.handle_get(args, response_out),
            CommandId::Set => self.handle_set(args, response_out),
            CommandId::Del => self.handle_del(args, response_out),
            CommandId::Incr => self.handle_incr_decr(args, 1, response_out),
            CommandId::Decr => self.handle_incr_decr(args, -1, response_out),
            CommandId::Expire => self.handle_expire(args, response_out),
            CommandId::Ttl => self.handle_ttl(args, response_out),
            CommandId::Pexpire => self.handle_pexpire(args, response_out),
            CommandId::Pttl => self.handle_pttl(args, response_out),
            CommandId::Persist => self.handle_persist(args, response_out),
            CommandId::Hset => self.handle_hset(args, response_out),
            CommandId::Hget => self.handle_hget(args, response_out),
            CommandId::Hdel => self.handle_hdel(args, response_out),
            CommandId::Hgetall => self.handle_hgetall(args, response_out),
            CommandId::Lpush => self.handle_lpush(args, response_out),
            CommandId::Rpush => self.handle_rpush(args, response_out),
            CommandId::Lpop => self.handle_lpop(args, response_out),
            CommandId::Rpop => self.handle_rpop(args, response_out),
            CommandId::Lrange => self.handle_lrange(args, response_out),
            CommandId::Sadd => self.handle_sadd(args, response_out),
            CommandId::Srem => self.handle_srem(args, response_out),
            CommandId::Smembers => self.handle_smembers(args, response_out),
            CommandId::Sismember => self.handle_sismember(args, response_out),
            CommandId::Zadd => self.handle_zadd(args, response_out),
            CommandId::Zrem => self.handle_zrem(args, response_out),
            CommandId::Zrange => self.handle_zrange(args, response_out),
            CommandId::Zscore => self.handle_zscore(args, response_out),
            CommandId::Ping => self.handle_ping(args, response_out),
            CommandId::Echo => self.handle_echo(args, response_out),
            CommandId::Info => self.handle_info(args, response_out),
            CommandId::Dbsize => self.handle_dbsize(args, response_out),
            CommandId::Command => self.handle_command(args, response_out),
            CommandId::Multi
            | CommandId::Exec
            | CommandId::Discard
            | CommandId::Watch
            | CommandId::Unwatch
            | CommandId::Asking
            | CommandId::Unknown => Err(RequestExecutionError::UnknownCommand),
        }
    }

    pub fn object_upsert(
        &self,
        key: &[u8],
        object_type: u8,
        payload: &[u8],
    ) -> Result<(), RequestExecutionError> {
        let key = key.to_vec();
        let value = encode_object_value(object_type, payload);
        let mut store = self
            .object_store
            .lock()
            .expect("object store mutex poisoned");
        let mut session = store.session(&self.object_functions);
        let mut output = Vec::new();
        let mut info = UpsertInfo::default();
        session
            .upsert(&key, &value, &mut output, &mut info)
            .map_err(map_upsert_error)?;
        self.object_key_registry
            .lock()
            .expect("object key registry mutex poisoned")
            .insert(key.clone());
        self.bump_watch_version(&key);
        Ok(())
    }

    pub fn object_read(&self, key: &[u8]) -> Result<Option<(u8, Vec<u8>)>, RequestExecutionError> {
        let key = key.to_vec();
        let mut store = self
            .object_store
            .lock()
            .expect("object store mutex poisoned");
        let mut session = store.session(&self.object_functions);
        let mut output = Vec::new();
        let status = session
            .read(&key, &Vec::new(), &mut output, &ReadInfo::default())
            .map_err(map_read_error)?;
        match status {
            ReadOperationStatus::FoundInMemory | ReadOperationStatus::FoundOnDisk => {
                decode_object_value(&output).map(Some).ok_or_else(|| {
                    storage_failure("object_read", "failed to decode object value payload")
                })
            }
            ReadOperationStatus::NotFound => Ok(None),
            ReadOperationStatus::RetryLater => Err(RequestExecutionError::StorageBusy),
        }
    }

    pub fn object_delete(&self, key: &[u8]) -> Result<bool, RequestExecutionError> {
        let key = key.to_vec();
        let mut store = self
            .object_store
            .lock()
            .expect("object store mutex poisoned");
        let mut session = store.session(&self.object_functions);
        let mut info = DeleteInfo::default();
        let status = session.delete(&key, &mut info).map_err(map_delete_error)?;
        match status {
            DeleteOperationStatus::TombstonedInPlace | DeleteOperationStatus::AppendedTombstone => {
                self.object_key_registry
                    .lock()
                    .expect("object key registry mutex poisoned")
                    .remove(&key);
                self.bump_watch_version(&key);
                Ok(true)
            }
            DeleteOperationStatus::NotFound => {
                self.object_key_registry
                    .lock()
                    .expect("object key registry mutex poisoned")
                    .remove(&key);
                Ok(false)
            }
            DeleteOperationStatus::RetryLater => Err(RequestExecutionError::StorageBusy),
        }
    }

    pub fn export_migration_entry(
        &self,
        key: &[u8],
    ) -> Result<Option<MigrationEntry>, RequestExecutionError> {
        self.expire_key_if_needed(key)?;

        if let Some(value) = self.read_string_value(key)? {
            return Ok(Some(MigrationEntry {
                key: key.to_vec(),
                value: MigrationValue::String(value),
                expiration_unix_millis: self.expiration_unix_millis_for_key(key),
            }));
        }

        if let Some((object_type, payload)) = self.object_read(key)? {
            return Ok(Some(MigrationEntry {
                key: key.to_vec(),
                value: MigrationValue::Object {
                    object_type,
                    payload,
                },
                expiration_unix_millis: None,
            }));
        }

        Ok(None)
    }

    pub fn import_migration_entry(
        &self,
        entry: &MigrationEntry,
    ) -> Result<(), RequestExecutionError> {
        match &entry.value {
            MigrationValue::String(value) => {
                self.upsert_string_value_for_migration(
                    &entry.key,
                    value,
                    entry.expiration_unix_millis,
                )?;
                let _ = self.object_delete(&entry.key)?;
            }
            MigrationValue::Object {
                object_type,
                payload,
            } => {
                self.delete_string_key_for_migration(&entry.key)?;
                self.object_upsert(&entry.key, *object_type, payload)?;
            }
        }
        Ok(())
    }

    pub fn migrate_keys_to(
        &self,
        target: &RequestProcessor,
        keys: &[Vec<u8>],
        delete_source: bool,
    ) -> Result<usize, RequestExecutionError> {
        let mut moved = 0usize;
        for key in keys {
            let Some(entry) = self.export_migration_entry(key)? else {
                continue;
            };
            target.import_migration_entry(&entry)?;
            if delete_source {
                self.delete_string_key_for_migration(key)?;
                let _ = self.object_delete(key)?;
            }
            moved += 1;
        }
        Ok(moved)
    }

    pub fn migration_keys_for_slot(&self, slot: u16, max_keys: usize) -> Vec<Vec<u8>> {
        if max_keys == 0 {
            return Vec::new();
        }

        let mut slot_keys = BTreeSet::new();

        let string_keys = self.string_keys_snapshot();
        for key in string_keys {
            if redis_hash_slot(&key) == slot {
                slot_keys.insert(key);
                if slot_keys.len() >= max_keys {
                    return slot_keys.into_iter().collect();
                }
            }
        }

        let object_keys: Vec<Vec<u8>> = self
            .object_key_registry
            .lock()
            .expect("object key registry mutex poisoned")
            .iter()
            .cloned()
            .collect();
        for key in object_keys {
            if redis_hash_slot(&key) == slot {
                slot_keys.insert(key);
                if slot_keys.len() >= max_keys {
                    return slot_keys.into_iter().collect();
                }
            }
        }

        slot_keys.into_iter().collect()
    }

    pub fn migrate_slot_to(
        &self,
        target: &RequestProcessor,
        slot: u16,
        max_keys: usize,
        delete_source: bool,
    ) -> Result<usize, RequestExecutionError> {
        let keys = self.migration_keys_for_slot(slot, max_keys);
        self.migrate_keys_to(target, &keys, delete_source)
    }

    fn load_hash_object(
        &self,
        key: &[u8],
    ) -> Result<Option<BTreeMap<Vec<u8>, Vec<u8>>>, RequestExecutionError> {
        let object = match self.object_read(key)? {
            Some(object) => object,
            None => {
                if self.key_exists(key)? {
                    return Err(RequestExecutionError::WrongType);
                }
                return Ok(None);
            }
        };
        if object.0 != HASH_OBJECT_TYPE_TAG {
            return Err(RequestExecutionError::WrongType);
        }
        deserialize_hash_object_payload(&object.1)
            .map(Some)
            .ok_or_else(|| {
                storage_failure("load_hash_object", "failed to deserialize hash payload")
            })
    }

    fn save_hash_object(
        &self,
        key: &[u8],
        hash: &BTreeMap<Vec<u8>, Vec<u8>>,
    ) -> Result<(), RequestExecutionError> {
        let payload = serialize_hash_object_payload(hash);
        self.object_upsert(key, HASH_OBJECT_TYPE_TAG, &payload)
    }

    fn load_list_object(&self, key: &[u8]) -> Result<Option<Vec<Vec<u8>>>, RequestExecutionError> {
        let object = match self.object_read(key)? {
            Some(object) => object,
            None => {
                if self.key_exists(key)? {
                    return Err(RequestExecutionError::WrongType);
                }
                return Ok(None);
            }
        };
        if object.0 != LIST_OBJECT_TYPE_TAG {
            return Err(RequestExecutionError::WrongType);
        }
        deserialize_list_object_payload(&object.1)
            .map(Some)
            .ok_or_else(|| {
                storage_failure("load_list_object", "failed to deserialize list payload")
            })
    }

    fn save_list_object(&self, key: &[u8], list: &[Vec<u8>]) -> Result<(), RequestExecutionError> {
        let payload = serialize_list_object_payload(list);
        self.object_upsert(key, LIST_OBJECT_TYPE_TAG, &payload)
    }

    fn load_set_object(
        &self,
        key: &[u8],
    ) -> Result<Option<BTreeSet<Vec<u8>>>, RequestExecutionError> {
        let object = match self.object_read(key)? {
            Some(object) => object,
            None => {
                if self.key_exists(key)? {
                    return Err(RequestExecutionError::WrongType);
                }
                return Ok(None);
            }
        };
        if object.0 != SET_OBJECT_TYPE_TAG {
            return Err(RequestExecutionError::WrongType);
        }
        deserialize_set_object_payload(&object.1)
            .map(Some)
            .ok_or_else(|| storage_failure("load_set_object", "failed to deserialize set payload"))
    }

    fn save_set_object(
        &self,
        key: &[u8],
        set: &BTreeSet<Vec<u8>>,
    ) -> Result<(), RequestExecutionError> {
        let payload = serialize_set_object_payload(set);
        self.object_upsert(key, SET_OBJECT_TYPE_TAG, &payload)
    }

    fn load_zset_object(
        &self,
        key: &[u8],
    ) -> Result<Option<BTreeMap<Vec<u8>, f64>>, RequestExecutionError> {
        let object = match self.object_read(key)? {
            Some(object) => object,
            None => {
                if self.key_exists(key)? {
                    return Err(RequestExecutionError::WrongType);
                }
                return Ok(None);
            }
        };
        if object.0 != ZSET_OBJECT_TYPE_TAG {
            return Err(RequestExecutionError::WrongType);
        }
        deserialize_zset_object_payload(&object.1)
            .map(Some)
            .ok_or_else(|| {
                storage_failure("load_zset_object", "failed to deserialize zset payload")
            })
    }

    fn save_zset_object(
        &self,
        key: &[u8],
        zset: &BTreeMap<Vec<u8>, f64>,
    ) -> Result<(), RequestExecutionError> {
        let payload = serialize_zset_object_payload(zset);
        self.object_upsert(key, ZSET_OBJECT_TYPE_TAG, &payload)
    }

    pub fn expire_stale_keys(&self, max_keys: usize) -> Result<usize, RequestExecutionError> {
        if max_keys == 0 {
            return Ok(0);
        }

        let now = Instant::now();
        let mut expired_keys: Vec<Vec<u8>> = Vec::with_capacity(max_keys);
        for (shard_index, expirations) in self.string_expirations.iter().enumerate() {
            if expired_keys.len() >= max_keys {
                break;
            }
            if self.string_expiration_count_for_shard(shard_index) == 0 {
                continue;
            }
            let remaining = max_keys - expired_keys.len();
            let mut shard_expired: Vec<Vec<u8>> = expirations
                .lock()
                .expect("expiration mutex poisoned")
                .iter()
                .filter_map(|(key, deadline)| {
                    if *deadline <= now {
                        Some(key.clone())
                    } else {
                        None
                    }
                })
                .take(remaining)
                .collect();
            expired_keys.append(&mut shard_expired);
        }

        let mut removed = 0usize;
        for key in expired_keys {
            let status = {
                let mut store = self.lock_string_store_for_key(&key);
                let mut session = store.session(&self.functions);
                let mut info = DeleteInfo::default();
                session.delete(&key, &mut info).map_err(map_delete_error)?
            };

            self.remove_string_key_metadata(&key);

            match status {
                DeleteOperationStatus::TombstonedInPlace
                | DeleteOperationStatus::AppendedTombstone => {
                    removed += 1;
                    self.bump_watch_version(&key);
                }
                DeleteOperationStatus::NotFound => {}
                DeleteOperationStatus::RetryLater => {
                    return Err(RequestExecutionError::StorageBusy);
                }
            }
        }

        Ok(removed)
    }

    fn read_string_value(&self, key: &[u8]) -> Result<Option<Vec<u8>>, RequestExecutionError> {
        let mut store = self.lock_string_store_for_key(key);
        let mut session = store.session(&self.functions);
        let mut output = Vec::new();
        let status = session
            .read(
                &key.to_vec(),
                &Vec::new(),
                &mut output,
                &ReadInfo::default(),
            )
            .map_err(map_read_error)?;
        match status {
            ReadOperationStatus::FoundInMemory | ReadOperationStatus::FoundOnDisk => {
                Ok(Some(output))
            }
            ReadOperationStatus::NotFound => Ok(None),
            ReadOperationStatus::RetryLater => Err(RequestExecutionError::StorageBusy),
        }
    }

    fn key_exists(&self, key: &[u8]) -> Result<bool, RequestExecutionError> {
        let mut store = self.lock_string_store_for_key(key);
        let mut session = store.session(&self.functions);
        let mut output = Vec::new();
        let key_vec = key.to_vec();
        let status = session
            .read(&key_vec, &Vec::new(), &mut output, &ReadInfo::default())
            .map_err(map_read_error)?;

        match status {
            ReadOperationStatus::FoundInMemory | ReadOperationStatus::FoundOnDisk => Ok(true),
            ReadOperationStatus::NotFound => Ok(false),
            ReadOperationStatus::RetryLater => Err(RequestExecutionError::StorageBusy),
        }
    }

    fn object_key_exists(&self, key: &[u8]) -> Result<bool, RequestExecutionError> {
        let mut store = self
            .object_store
            .lock()
            .expect("object store mutex poisoned");
        let mut session = store.session(&self.object_functions);
        let mut output = Vec::new();
        let status = session
            .read(
                &key.to_vec(),
                &Vec::new(),
                &mut output,
                &ReadInfo::default(),
            )
            .map_err(map_read_error)?;

        match status {
            ReadOperationStatus::FoundInMemory | ReadOperationStatus::FoundOnDisk => Ok(true),
            ReadOperationStatus::NotFound => Ok(false),
            ReadOperationStatus::RetryLater => Err(RequestExecutionError::StorageBusy),
        }
    }

    fn upsert_string_value_for_migration(
        &self,
        key: &[u8],
        user_value: &[u8],
        expiration_unix_millis: Option<u64>,
    ) -> Result<(), RequestExecutionError> {
        let mut store = self.lock_string_store_for_key(key);
        let mut session = store.session(&self.functions);
        let mut output = Vec::new();
        let mut upsert_info = UpsertInfo::default();
        if expiration_unix_millis.is_some() {
            upsert_info.user_data |= UPSERT_USER_DATA_HAS_EXPIRATION;
        }
        let stored_value = encode_stored_value(user_value, expiration_unix_millis);
        session
            .upsert(&key.to_vec(), &stored_value, &mut output, &mut upsert_info)
            .map_err(map_upsert_error)?;
        drop(session);
        drop(store);

        self.set_string_expiration_deadline(
            key,
            expiration_unix_millis.and_then(instant_from_unix_millis),
        );
        self.track_string_key(key);
        self.bump_watch_version(key);
        Ok(())
    }

    fn delete_string_key_for_migration(&self, key: &[u8]) -> Result<(), RequestExecutionError> {
        let mut store = self.lock_string_store_for_key(key);
        let mut session = store.session(&self.functions);
        let mut info = DeleteInfo::default();
        let status = session
            .delete(&key.to_vec(), &mut info)
            .map_err(map_delete_error)?;

        match status {
            DeleteOperationStatus::TombstonedInPlace
            | DeleteOperationStatus::AppendedTombstone
            | DeleteOperationStatus::NotFound => {
                self.remove_string_key_metadata(key);
                if !matches!(status, DeleteOperationStatus::NotFound) {
                    self.bump_watch_version(key);
                }
                Ok(())
            }
            DeleteOperationStatus::RetryLater => Err(RequestExecutionError::StorageBusy),
        }
    }

    fn expiration_unix_millis_for_key(&self, key: &[u8]) -> Option<u64> {
        let deadline = self.string_expiration_deadline(key)?;
        let now = Instant::now();
        let now_unix_millis = current_unix_time_millis()?;
        if deadline <= now {
            return Some(now_unix_millis);
        }
        let remaining = deadline.duration_since(now);
        let remaining_millis = u64::try_from(remaining.as_millis()).ok()?;
        now_unix_millis.checked_add(remaining_millis)
    }

    fn rewrite_existing_value_expiration(
        &self,
        key: &[u8],
        expiration_unix_millis: Option<u64>,
    ) -> Result<bool, RequestExecutionError> {
        let key_vec = key.to_vec();
        let mut store = self.lock_string_store_for_key(key);
        let mut session = store.session(&self.functions);
        let mut current = Vec::new();
        let status = session
            .read(&key_vec, &Vec::new(), &mut current, &ReadInfo::default())
            .map_err(map_read_error)?;
        match status {
            ReadOperationStatus::FoundInMemory | ReadOperationStatus::FoundOnDisk => {
                let mut output = Vec::new();
                let mut upsert_info = UpsertInfo::default();
                if expiration_unix_millis.is_some() {
                    upsert_info.user_data |= UPSERT_USER_DATA_HAS_EXPIRATION;
                }
                let stored_value = encode_stored_value(&current, expiration_unix_millis);
                session
                    .upsert(&key_vec, &stored_value, &mut output, &mut upsert_info)
                    .map_err(map_upsert_error)?;
                Ok(true)
            }
            ReadOperationStatus::NotFound => Ok(false),
            ReadOperationStatus::RetryLater => Err(RequestExecutionError::StorageBusy),
        }
    }

    fn expire_key_if_needed(&self, key: &[u8]) -> Result<(), RequestExecutionError> {
        let shard_index = self.string_store_shard_index_for_key(key);
        self.expire_key_if_needed_in_shard(key, shard_index)
    }

    fn expire_key_if_needed_in_shard(
        &self,
        key: &[u8],
        shard_index: usize,
    ) -> Result<(), RequestExecutionError> {
        if self.string_expiration_count_for_shard(shard_index) == 0 {
            return Ok(());
        }
        crate::debug_sync_point!("request_processor.expire_key_if_needed.enter");
        let should_expire = {
            let mut expirations = self.lock_string_expirations_for_shard(shard_index);
            match expirations.get(key) {
                Some(deadline) if *deadline <= Instant::now() => {
                    if expirations.remove(key).is_some() {
                        self.decrement_string_expiration_count(shard_index);
                    }
                    true
                }
                _ => false,
            }
        };
        crate::debug_sync_point!("request_processor.expire_key_if_needed.after_expiration_lookup");

        if !should_expire {
            return Ok(());
        }

        crate::debug_sync_point!("request_processor.expire_key_if_needed.before_store_lock");
        let mut store = self.lock_string_store_for_shard(shard_index);
        let mut session = store.session(&self.functions);
        let mut info = DeleteInfo::default();
        let status = session
            .delete(&key.to_vec(), &mut info)
            .map_err(map_delete_error)?;
        if matches!(
            status,
            DeleteOperationStatus::TombstonedInPlace | DeleteOperationStatus::AppendedTombstone
        ) {
            self.bump_watch_version(key);
        }
        self.untrack_string_key_in_shard(key, shard_index);
        Ok(())
    }

    #[inline]
    fn string_store_shard_index_for_key(&self, key: &[u8]) -> usize {
        let shard_count = self.string_stores.len();
        debug_assert!(shard_count > 0);
        if shard_count == 1 {
            return 0;
        }
        (fnv1a_hash64(key) as usize) % shard_count
    }

    #[inline]
    fn string_expiration_count_for_shard(&self, shard_index: usize) -> usize {
        debug_assert!(shard_index < self.string_expiration_counts.len());
        self.string_expiration_counts[shard_index].load(Ordering::Acquire)
    }

    #[inline]
    fn increment_string_expiration_count(&self, shard_index: usize) {
        debug_assert!(shard_index < self.string_expiration_counts.len());
        self.string_expiration_counts[shard_index].fetch_add(1, Ordering::Release);
    }

    #[inline]
    fn decrement_string_expiration_count(&self, shard_index: usize) {
        debug_assert!(shard_index < self.string_expiration_counts.len());
        let previous = self.string_expiration_counts[shard_index].fetch_sub(1, Ordering::Release);
        debug_assert!(previous > 0, "expiration count underflow");
    }

    #[inline]
    fn lock_string_store_for_shard(
        &self,
        shard_index: usize,
    ) -> OrderedMutexGuard<'_, TsavoriteKV<Vec<u8>, Vec<u8>>> {
        debug_assert!(shard_index < self.string_stores.len());
        self.string_stores[shard_index]
            .lock()
            .expect("store mutex poisoned")
    }

    #[inline]
    fn lock_string_store_for_key(
        &self,
        key: &[u8],
    ) -> OrderedMutexGuard<'_, TsavoriteKV<Vec<u8>, Vec<u8>>> {
        let shard_index = self.string_store_shard_index_for_key(key);
        self.lock_string_store_for_shard(shard_index)
    }

    #[inline]
    fn lock_string_expirations_for_shard(
        &self,
        shard_index: usize,
    ) -> OrderedMutexGuard<'_, HashMap<Vec<u8>, Instant>> {
        debug_assert!(shard_index < self.string_expirations.len());
        self.string_expirations[shard_index]
            .lock()
            .expect("expiration mutex poisoned")
    }

    #[inline]
    fn lock_string_key_registry_for_shard(
        &self,
        shard_index: usize,
    ) -> OrderedMutexGuard<'_, HashSet<Vec<u8>>> {
        debug_assert!(shard_index < self.string_key_registries.len());
        self.string_key_registries[shard_index]
            .lock()
            .expect("key registry mutex poisoned")
    }

    fn track_string_key_in_shard(&self, key: &[u8], shard_index: usize) {
        self.lock_string_key_registry_for_shard(shard_index)
            .insert(key.to_vec());
    }

    fn track_string_key(&self, key: &[u8]) {
        let shard_index = self.string_store_shard_index_for_key(key);
        self.track_string_key_in_shard(key, shard_index);
    }

    fn untrack_string_key_in_shard(&self, key: &[u8], shard_index: usize) {
        self.lock_string_key_registry_for_shard(shard_index)
            .remove(key);
    }

    fn untrack_string_key(&self, key: &[u8]) {
        let shard_index = self.string_store_shard_index_for_key(key);
        self.untrack_string_key_in_shard(key, shard_index);
    }

    fn set_string_expiration_deadline_in_shard(
        &self,
        key: &[u8],
        shard_index: usize,
        deadline: Option<Instant>,
    ) {
        let mut expirations = self.lock_string_expirations_for_shard(shard_index);
        match deadline {
            Some(deadline) => {
                let previous = expirations.insert(key.to_vec(), deadline);
                if previous.is_none() {
                    self.increment_string_expiration_count(shard_index);
                }
            }
            None => {
                if expirations.remove(key).is_some() {
                    self.decrement_string_expiration_count(shard_index);
                }
            }
        }
    }

    fn set_string_expiration_deadline(&self, key: &[u8], deadline: Option<Instant>) {
        let shard_index = self.string_store_shard_index_for_key(key);
        self.set_string_expiration_deadline_in_shard(key, shard_index, deadline);
    }

    fn remove_string_key_metadata_in_shard(&self, key: &[u8], shard_index: usize) {
        self.set_string_expiration_deadline_in_shard(key, shard_index, None);
        self.untrack_string_key_in_shard(key, shard_index);
    }

    fn remove_string_key_metadata(&self, key: &[u8]) {
        let shard_index = self.string_store_shard_index_for_key(key);
        self.remove_string_key_metadata_in_shard(key, shard_index);
    }

    fn string_expiration_deadline_in_shard(
        &self,
        key: &[u8],
        shard_index: usize,
    ) -> Option<Instant> {
        if self.string_expiration_count_for_shard(shard_index) == 0 {
            return None;
        }
        self.lock_string_expirations_for_shard(shard_index)
            .get(key)
            .copied()
    }

    fn string_expiration_deadline(&self, key: &[u8]) -> Option<Instant> {
        let shard_index = self.string_store_shard_index_for_key(key);
        self.string_expiration_deadline_in_shard(key, shard_index)
    }

    fn string_keys_snapshot(&self) -> Vec<Vec<u8>> {
        self.string_key_registries
            .iter()
            .flat_map(|registry| {
                registry
                    .lock()
                    .expect("key registry mutex poisoned")
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    fn bump_watch_version(&self, key: &[u8]) {
        let slot = watch_version_slot(key);
        self.watch_versions[slot].fetch_add(1, Ordering::SeqCst);
    }
}

fn tsavorite_config_from_env() -> TsavoriteKvConfig {
    tsavorite_config_from_values(
        parse_env_u8(GARNET_HASH_INDEX_SIZE_BITS_ENV),
        parse_env_u8(GARNET_PAGE_SIZE_BITS_ENV),
        parse_env_usize(GARNET_MAX_IN_MEMORY_PAGES_ENV),
    )
}

fn tsavorite_config_from_values(
    hash_index_size_bits: Option<u8>,
    page_size_bits: Option<u8>,
    max_in_memory_pages: Option<usize>,
) -> TsavoriteKvConfig {
    let mut config = TsavoriteKvConfig::default();
    config.hash_index_size_bits = DEFAULT_SERVER_HASH_INDEX_SIZE_BITS;
    if let Some(bits) = hash_index_size_bits {
        if (1..=30).contains(&bits) {
            config.hash_index_size_bits = bits;
        }
    }
    if let Some(bits) = page_size_bits {
        if (1..=30).contains(&bits) {
            config.page_size_bits = bits;
        }
    }
    if let Some(max_pages) = max_in_memory_pages {
        if max_pages > 0 {
            config.max_in_memory_pages = max_pages;
        }
    }
    config
}

fn string_store_shard_count_from_env() -> usize {
    let explicit_shards =
        parse_env_usize(GARNET_STRING_STORE_SHARDS_ENV).filter(|count| *count > 0);
    let owner_threads = parse_env_usize(GARNET_STRING_OWNER_THREADS_ENV).filter(|count| *count > 0);
    string_store_shard_count_from_values(explicit_shards, owner_threads)
}

fn string_store_shard_count_from_values(
    explicit_shards: Option<usize>,
    owner_threads: Option<usize>,
) -> usize {
    if let Some(explicit) = explicit_shards {
        return explicit;
    }

    match owner_threads {
        Some(1) => SINGLE_OWNER_THREAD_STRING_STORE_SHARDS,
        Some(_) => DEFAULT_STRING_STORE_SHARDS,
        None => DEFAULT_STRING_STORE_SHARDS,
    }
}

fn scale_hash_index_bits_for_shards(base_bits: u8, shard_count: usize) -> u8 {
    if shard_count <= 1 {
        return base_bits;
    }

    let shard_shift = usize::BITS - (shard_count.saturating_sub(1)).leading_zeros();
    base_bits.saturating_sub(shard_shift as u8).max(1)
}

fn parse_env_u8(key: &str) -> Option<u8> {
    std::env::var(key).ok()?.parse::<u8>().ok()
}

fn parse_env_usize(key: &str) -> Option<usize> {
    std::env::var(key).ok()?.parse::<usize>().ok()
}

#[inline]
fn fnv1a_hash64(key: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in key {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn watch_version_slot(key: &[u8]) -> usize {
    (fnv1a_hash64(key) as usize) & WATCH_VERSION_MAP_MASK
}

#[derive(Debug, Clone, Copy, Default)]
struct SetOptions {
    only_if_absent: bool,
    only_if_present: bool,
    expiration: Option<ExpirationMetadata>,
}

fn parse_set_options(args: &[ArgSlice]) -> Result<SetOptions, RequestExecutionError> {
    let mut options = SetOptions::default();
    let mut index = 3usize;

    while index < args.len() {
        // SAFETY: caller guarantees argument backing memory validity.
        let option = unsafe { args[index].as_slice() };

        if ascii_eq_ignore_case(option, b"NX") {
            if options.only_if_absent || options.only_if_present {
                return Err(RequestExecutionError::SyntaxError);
            }
            options.only_if_absent = true;
            index += 1;
            continue;
        }

        if ascii_eq_ignore_case(option, b"XX") {
            if options.only_if_absent || options.only_if_present {
                return Err(RequestExecutionError::SyntaxError);
            }
            options.only_if_present = true;
            index += 1;
            continue;
        }

        if ascii_eq_ignore_case(option, b"EX") || ascii_eq_ignore_case(option, b"PX") {
            if options.expiration.is_some() {
                return Err(RequestExecutionError::SyntaxError);
            }
            if index + 1 >= args.len() {
                return Err(RequestExecutionError::SyntaxError);
            }

            // SAFETY: caller guarantees argument backing memory validity.
            let value = unsafe { args[index + 1].as_slice() };
            let amount = parse_u64_ascii(value).ok_or(RequestExecutionError::InvalidExpireTime)?;
            if amount == 0 {
                return Err(RequestExecutionError::InvalidExpireTime);
            }

            let duration = if ascii_eq_ignore_case(option, b"EX") {
                Duration::from_secs(amount)
            } else {
                Duration::from_millis(amount)
            };
            options.expiration = Some(
                expiration_metadata_from_duration(duration)
                    .ok_or(RequestExecutionError::InvalidExpireTime)?,
            );
            index += 2;
            continue;
        }

        return Err(RequestExecutionError::SyntaxError);
    }

    Ok(options)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestExecutionError {
    WrongArity {
        command: &'static str,
        expected: &'static str,
    },
    UnknownCommand,
    SyntaxError,
    InvalidExpireTime,
    WrongType,
    StorageBusy,
    StorageCapacityExceeded,
    StorageFailure,
    ValueNotInteger,
    ValueNotFloat,
}

impl RequestExecutionError {
    pub fn append_resp_error(self, response_out: &mut Vec<u8>) {
        match self {
            Self::WrongArity { command, .. } => append_error(
                response_out,
                &format!("ERR wrong number of arguments for '{}' command", command),
            ),
            Self::UnknownCommand => append_error(response_out, "ERR unknown command"),
            Self::SyntaxError => append_error(response_out, "ERR syntax error"),
            Self::InvalidExpireTime => {
                append_error(response_out, "ERR invalid expire time in 'set' command")
            }
            Self::WrongType => append_error(
                response_out,
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            ),
            Self::StorageBusy => append_error(response_out, "ERR storage busy, retry later"),
            Self::StorageCapacityExceeded => append_error(
                response_out,
                "ERR storage capacity exceeded (increase max in-memory pages)",
            ),
            Self::StorageFailure => append_error(response_out, "ERR internal storage failure"),
            Self::ValueNotInteger => {
                append_error(response_out, "ERR value is not an integer or out of range")
            }
            Self::ValueNotFloat => append_error(response_out, "ERR value is not a valid float"),
        }
    }
}

fn storage_failure_logging_enabled() -> bool {
    *STORAGE_FAILURE_LOG_ENABLED.get_or_init(|| {
        std::env::var(GARNET_LOG_STORAGE_FAILURES_ENV)
            .map(|value| !matches!(value.as_str(), "0" | "false" | "FALSE"))
            .unwrap_or(true)
    })
}

fn log_storage_failure(context: &str, detail: &str) {
    if !storage_failure_logging_enabled() {
        return;
    }

    let count = STORAGE_FAILURE_LOG_COUNT.fetch_add(1, Ordering::Relaxed);
    if count >= STORAGE_FAILURE_LOG_LIMIT {
        if count == STORAGE_FAILURE_LOG_LIMIT {
            eprintln!(
                "garnet-server storage failure logging suppressed after {} entries",
                STORAGE_FAILURE_LOG_LIMIT
            );
        }
        return;
    }

    let backtrace = std::backtrace::Backtrace::force_capture();
    eprintln!(
        "garnet-server storage failure [{}]: {}\nbacktrace:\n{}",
        context, detail, backtrace
    );
}

fn storage_failure(context: &str, detail: &str) -> RequestExecutionError {
    log_storage_failure(context, detail);
    RequestExecutionError::StorageFailure
}

fn map_read_error(error: ReadOperationError) -> RequestExecutionError {
    match error {
        ReadOperationError::PageManager(PageManagerError::BufferFull { .. }) => {
            RequestExecutionError::StorageCapacityExceeded
        }
        ReadOperationError::PageResidency(PageResidencyError::PageManager(
            PageManagerError::BufferFull { .. },
        )) => RequestExecutionError::StorageCapacityExceeded,
        ReadOperationError::PageResidency(PageResidencyError::NoEvictablePage { .. }) => {
            RequestExecutionError::StorageBusy
        }
        other => storage_failure("read", &format!("{other:?}")),
    }
}

fn map_upsert_error(error: UpsertOperationError) -> RequestExecutionError {
    match error {
        UpsertOperationError::PageManager(PageManagerError::BufferFull { .. }) => {
            RequestExecutionError::StorageCapacityExceeded
        }
        UpsertOperationError::CompareExchangeConflict => RequestExecutionError::StorageBusy,
        other => storage_failure("upsert", &format!("{other:?}")),
    }
}

fn map_delete_error(error: DeleteOperationError) -> RequestExecutionError {
    match error {
        DeleteOperationError::PageManager(PageManagerError::BufferFull { .. }) => {
            RequestExecutionError::StorageCapacityExceeded
        }
        DeleteOperationError::CompareExchangeConflict => RequestExecutionError::StorageBusy,
        other => storage_failure("delete", &format!("{other:?}")),
    }
}

fn map_rmw_error(error: RmwOperationError) -> RequestExecutionError {
    match error {
        RmwOperationError::PageManager(PageManagerError::BufferFull { .. }) => {
            RequestExecutionError::StorageCapacityExceeded
        }
        RmwOperationError::CompareExchangeConflict => RequestExecutionError::StorageBusy,
        other => storage_failure("rmw", &format!("{other:?}")),
    }
}

fn parse_i64_ascii(input: &[u8]) -> Option<i64> {
    if input.is_empty() {
        return None;
    }
    let text = core::str::from_utf8(input).ok()?;
    text.parse::<i64>().ok()
}

fn parse_u64_ascii(input: &[u8]) -> Option<u64> {
    if input.is_empty() {
        return None;
    }
    let text = core::str::from_utf8(input).ok()?;
    text.parse::<u64>().ok()
}

fn parse_f64_ascii(input: &[u8]) -> Option<f64> {
    if input.is_empty() {
        return None;
    }
    let text = core::str::from_utf8(input).ok()?;
    let parsed = text.parse::<f64>().ok()?;
    if !parsed.is_finite() {
        return None;
    }
    Some(parsed)
}

#[derive(Debug, Clone, Copy)]
struct DecodedStoredValue<'a> {
    expiration_unix_millis: Option<u64>,
    user_value: &'a [u8],
}

fn current_unix_time_millis() -> Option<u64> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?;
    u64::try_from(now.as_millis()).ok()
}

fn expiration_metadata_from_duration(duration: Duration) -> Option<ExpirationMetadata> {
    let deadline = Instant::now().checked_add(duration)?;
    let now_millis = u128::from(current_unix_time_millis()?);
    let expiration_millis = now_millis.checked_add(duration.as_millis())?;
    let unix_millis = u64::try_from(expiration_millis).ok()?;
    Some(ExpirationMetadata {
        deadline,
        unix_millis,
    })
}

fn instant_from_unix_millis(unix_millis: u64) -> Option<Instant> {
    let now = Instant::now();
    let now_unix_millis = current_unix_time_millis()?;
    if unix_millis <= now_unix_millis {
        return Some(now);
    }
    let delta_millis = unix_millis.checked_sub(now_unix_millis)?;
    now.checked_add(Duration::from_millis(delta_millis))
}

fn decode_stored_value(stored: &[u8]) -> DecodedStoredValue<'_> {
    if stored.len() < VALUE_EXPIRATION_PREFIX_LEN {
        return DecodedStoredValue {
            expiration_unix_millis: None,
            user_value: stored,
        };
    }

    let mut metadata = [0u8; VALUE_EXPIRATION_PREFIX_LEN];
    metadata.copy_from_slice(&stored[..VALUE_EXPIRATION_PREFIX_LEN]);
    let expiration_millis = u64::from_le_bytes(metadata);
    DecodedStoredValue {
        expiration_unix_millis: if expiration_millis == 0 {
            None
        } else {
            Some(expiration_millis)
        },
        user_value: &stored[VALUE_EXPIRATION_PREFIX_LEN..],
    }
}

fn encode_stored_value(user_value: &[u8], expiration_unix_millis: Option<u64>) -> Vec<u8> {
    let mut stored = Vec::with_capacity(VALUE_EXPIRATION_PREFIX_LEN + user_value.len());
    stored.extend_from_slice(&expiration_unix_millis.unwrap_or(0).to_le_bytes());
    stored.extend_from_slice(user_value);
    stored
}

fn encode_object_value(object_type: u8, payload: &[u8]) -> Vec<u8> {
    let mut value = Vec::with_capacity(1 + payload.len());
    value.push(object_type);
    value.extend_from_slice(payload);
    value
}

fn decode_object_value(encoded: &[u8]) -> Option<(u8, Vec<u8>)> {
    let (&object_type, payload) = encoded.split_first()?;
    Some((object_type, payload.to_vec()))
}

fn serialize_hash_object_payload(hash: &BTreeMap<Vec<u8>, Vec<u8>>) -> Vec<u8> {
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&(hash.len() as u32).to_le_bytes());
    for (field, value) in hash {
        encoded.extend_from_slice(&(field.len() as u32).to_le_bytes());
        encoded.extend_from_slice(field);
        encoded.extend_from_slice(&(value.len() as u32).to_le_bytes());
        encoded.extend_from_slice(value);
    }
    encoded
}

fn deserialize_hash_object_payload(encoded: &[u8]) -> Option<BTreeMap<Vec<u8>, Vec<u8>>> {
    let mut cursor = 0usize;

    fn take_u32(encoded: &[u8], cursor: &mut usize) -> Option<u32> {
        let end = (*cursor).checked_add(size_of::<u32>())?;
        let bytes = encoded.get(*cursor..end)?;
        let mut raw = [0u8; size_of::<u32>()];
        raw.copy_from_slice(bytes);
        *cursor = end;
        Some(u32::from_le_bytes(raw))
    }

    let count = take_u32(encoded, &mut cursor)? as usize;
    let mut hash = BTreeMap::new();
    for _ in 0..count {
        let field_len = usize::try_from(take_u32(encoded, &mut cursor)?).ok()?;
        let field_end = cursor.checked_add(field_len)?;
        let field = encoded.get(cursor..field_end)?.to_vec();
        cursor = field_end;

        let value_len = usize::try_from(take_u32(encoded, &mut cursor)?).ok()?;
        let value_end = cursor.checked_add(value_len)?;
        let value = encoded.get(cursor..value_end)?.to_vec();
        cursor = value_end;

        hash.insert(field, value);
    }

    if cursor != encoded.len() {
        return None;
    }
    Some(hash)
}

fn serialize_list_object_payload(list: &[Vec<u8>]) -> Vec<u8> {
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&(list.len() as u32).to_le_bytes());
    for value in list {
        encoded.extend_from_slice(&(value.len() as u32).to_le_bytes());
        encoded.extend_from_slice(value);
    }
    encoded
}

fn deserialize_list_object_payload(encoded: &[u8]) -> Option<Vec<Vec<u8>>> {
    let mut cursor = 0usize;

    fn take_u32(encoded: &[u8], cursor: &mut usize) -> Option<u32> {
        let end = (*cursor).checked_add(size_of::<u32>())?;
        let bytes = encoded.get(*cursor..end)?;
        let mut raw = [0u8; size_of::<u32>()];
        raw.copy_from_slice(bytes);
        *cursor = end;
        Some(u32::from_le_bytes(raw))
    }

    let count = take_u32(encoded, &mut cursor)? as usize;
    let mut list = Vec::with_capacity(count);
    for _ in 0..count {
        let value_len = usize::try_from(take_u32(encoded, &mut cursor)?).ok()?;
        let value_end = cursor.checked_add(value_len)?;
        let value = encoded.get(cursor..value_end)?.to_vec();
        cursor = value_end;
        list.push(value);
    }

    if cursor != encoded.len() {
        return None;
    }
    Some(list)
}

fn serialize_set_object_payload(set: &BTreeSet<Vec<u8>>) -> Vec<u8> {
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&(set.len() as u32).to_le_bytes());
    for member in set {
        encoded.extend_from_slice(&(member.len() as u32).to_le_bytes());
        encoded.extend_from_slice(member);
    }
    encoded
}

fn deserialize_set_object_payload(encoded: &[u8]) -> Option<BTreeSet<Vec<u8>>> {
    let mut cursor = 0usize;

    fn take_u32(encoded: &[u8], cursor: &mut usize) -> Option<u32> {
        let end = (*cursor).checked_add(size_of::<u32>())?;
        let bytes = encoded.get(*cursor..end)?;
        let mut raw = [0u8; size_of::<u32>()];
        raw.copy_from_slice(bytes);
        *cursor = end;
        Some(u32::from_le_bytes(raw))
    }

    let count = take_u32(encoded, &mut cursor)? as usize;
    let mut set = BTreeSet::new();
    for _ in 0..count {
        let member_len = usize::try_from(take_u32(encoded, &mut cursor)?).ok()?;
        let member_end = cursor.checked_add(member_len)?;
        let member = encoded.get(cursor..member_end)?.to_vec();
        cursor = member_end;
        set.insert(member);
    }

    if cursor != encoded.len() {
        return None;
    }
    Some(set)
}

fn serialize_zset_object_payload(zset: &BTreeMap<Vec<u8>, f64>) -> Vec<u8> {
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&(zset.len() as u32).to_le_bytes());
    for (member, score) in zset {
        encoded.extend_from_slice(&(member.len() as u32).to_le_bytes());
        encoded.extend_from_slice(member);
        encoded.extend_from_slice(&score.to_le_bytes());
    }
    encoded
}

fn deserialize_zset_object_payload(encoded: &[u8]) -> Option<BTreeMap<Vec<u8>, f64>> {
    let mut cursor = 0usize;

    fn take_u32(encoded: &[u8], cursor: &mut usize) -> Option<u32> {
        let end = (*cursor).checked_add(size_of::<u32>())?;
        let bytes = encoded.get(*cursor..end)?;
        let mut raw = [0u8; size_of::<u32>()];
        raw.copy_from_slice(bytes);
        *cursor = end;
        Some(u32::from_le_bytes(raw))
    }

    fn take_f64(encoded: &[u8], cursor: &mut usize) -> Option<f64> {
        let end = (*cursor).checked_add(size_of::<f64>())?;
        let bytes = encoded.get(*cursor..end)?;
        let mut raw = [0u8; size_of::<f64>()];
        raw.copy_from_slice(bytes);
        let value = f64::from_le_bytes(raw);
        if !value.is_finite() {
            return None;
        }
        *cursor = end;
        Some(value)
    }

    let count = take_u32(encoded, &mut cursor)? as usize;
    let mut zset = BTreeMap::new();
    for _ in 0..count {
        let member_len = usize::try_from(take_u32(encoded, &mut cursor)?).ok()?;
        let member_end = cursor.checked_add(member_len)?;
        let member = encoded.get(cursor..member_end)?.to_vec();
        cursor = member_end;
        let score = take_f64(encoded, &mut cursor)?;
        zset.insert(member, score);
    }

    if cursor != encoded.len() {
        return None;
    }
    Some(zset)
}

fn ascii_eq_ignore_case(input: &[u8], expected_upper: &[u8]) -> bool {
    if input.len() != expected_upper.len() {
        return false;
    }
    input
        .iter()
        .zip(expected_upper.iter())
        .all(|(lhs, rhs)| lhs.to_ascii_uppercase() == *rhs)
}

fn append_simple_string(response_out: &mut Vec<u8>, value: &[u8]) {
    response_out.push(b'+');
    response_out.extend_from_slice(value);
    response_out.extend_from_slice(b"\r\n");
}

fn append_error(response_out: &mut Vec<u8>, message: &str) {
    response_out.push(b'-');
    response_out.extend_from_slice(message.as_bytes());
    response_out.extend_from_slice(b"\r\n");
}

fn append_bulk_string(response_out: &mut Vec<u8>, value: &[u8]) {
    response_out.extend_from_slice(b"$");
    response_out.extend_from_slice(value.len().to_string().as_bytes());
    response_out.extend_from_slice(b"\r\n");
    response_out.extend_from_slice(value);
    response_out.extend_from_slice(b"\r\n");
}

fn append_bulk_array(response_out: &mut Vec<u8>, items: &[&[u8]]) {
    response_out.push(b'*');
    response_out.extend_from_slice(items.len().to_string().as_bytes());
    response_out.extend_from_slice(b"\r\n");
    for item in items {
        append_bulk_string(response_out, item);
    }
}

fn append_null_bulk_string(response_out: &mut Vec<u8>) {
    response_out.extend_from_slice(b"$-1\r\n");
}

fn append_integer(response_out: &mut Vec<u8>, value: i64) {
    response_out.push(b':');
    response_out.extend_from_slice(value.to_string().as_bytes());
    response_out.extend_from_slice(b"\r\n");
}

struct KvSessionFunctions;

impl ISessionFunctions for KvSessionFunctions {
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
        let decoded = decode_stored_value(value);
        if let Some(expiration) = decoded.expiration_unix_millis {
            if let Some(now) = current_unix_time_millis() {
                if now >= expiration {
                    return false;
                }
            }
        }
        *output = decoded.user_value.to_vec();
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
        let decoded = decode_stored_value(value);
        if let Some(expiration) = decoded.expiration_unix_millis {
            if let Some(now) = current_unix_time_millis() {
                if now >= expiration {
                    return false;
                }
            }
        }
        *output = decoded.user_value.to_vec();
        true
    }

    fn single_writer(
        &self,
        _key: &Self::Key,
        input: &Self::Input,
        _src: &Self::Value,
        dst: &mut Self::Value,
        output: &mut Self::Output,
        upsert_info: &mut UpsertInfo,
        _reason: WriteReason,
        record_info: &mut RecordInfo,
    ) -> bool {
        *dst = input.clone();
        *output = dst.clone();
        if (upsert_info.user_data & UPSERT_USER_DATA_HAS_EXPIRATION) != 0 {
            record_info.set_has_expiration();
        } else {
            record_info.clear_has_expiration();
        }
        true
    }

    fn concurrent_writer(
        &self,
        _key: &Self::Key,
        input: &Self::Input,
        _src: &Self::Value,
        dst: &mut Self::Value,
        output: &mut Self::Output,
        upsert_info: &mut UpsertInfo,
        record_info: &mut RecordInfo,
    ) -> bool {
        *dst = input.clone();
        *output = dst.clone();
        if (upsert_info.user_data & UPSERT_USER_DATA_HAS_EXPIRATION) != 0 {
            record_info.set_has_expiration();
        } else {
            record_info.clear_has_expiration();
        }
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
        let decoded = decode_stored_value(value);
        let current = parse_i64_ascii(decoded.user_value).unwrap_or(0);
        let delta = match parse_i64_ascii(input) {
            Some(v) => v,
            None => return false,
        };
        let next = match current.checked_add(delta) {
            Some(v) => v,
            None => return false,
        };
        let next_bytes = next.to_string().into_bytes();
        *value = encode_stored_value(&next_bytes, decoded.expiration_unix_millis);
        *output = next_bytes;
        true
    }

    fn copy_updater(
        &self,
        _key: &Self::Key,
        input: &Self::Input,
        old_value: &Self::Value,
        new_value: &mut Self::Value,
        output: &mut Self::Output,
        _rmw_info: &mut RmwInfo,
        _record_info: &mut RecordInfo,
    ) -> bool {
        let decoded = decode_stored_value(old_value);
        let current = if decoded.user_value.is_empty() {
            0
        } else {
            match parse_i64_ascii(decoded.user_value) {
                Some(v) => v,
                None => return false,
            }
        };
        let delta = match parse_i64_ascii(input) {
            Some(v) => v,
            None => return false,
        };
        let next = match current.checked_add(delta) {
            Some(v) => v,
            None => return false,
        };
        let next_bytes = next.to_string().into_bytes();
        *new_value = encode_stored_value(&next_bytes, decoded.expiration_unix_millis);
        *output = next_bytes;
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
        record_info.clear_has_expiration();
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
        record_info.clear_has_expiration();
        record_info.set_tombstone();
        true
    }
}

impl HybridLogReadAdapter for KvSessionFunctions {
    fn record_key_equals(&self, requested_key: &Self::Key, record_key: &[u8]) -> bool {
        requested_key.as_slice() == record_key
    }

    fn value_from_record(&self, record_value: &[u8]) -> Self::Value {
        record_value.to_vec()
    }
}

impl HybridLogUpsertAdapter for KvSessionFunctions {
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

impl HybridLogRmwAdapter for KvSessionFunctions {
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

impl HybridLogDeleteAdapter for KvSessionFunctions {
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

struct ObjectSessionFunctions;

impl ISessionFunctions for ObjectSessionFunctions {
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

impl HybridLogReadAdapter for ObjectSessionFunctions {
    fn record_key_equals(&self, requested_key: &Self::Key, record_key: &[u8]) -> bool {
        requested_key.as_slice() == record_key
    }

    fn value_from_record(&self, record_value: &[u8]) -> Self::Value {
        record_value.to_vec()
    }
}

impl HybridLogUpsertAdapter for ObjectSessionFunctions {
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

impl HybridLogRmwAdapter for ObjectSessionFunctions {
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

impl HybridLogDeleteAdapter for ObjectSessionFunctions {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::debug_concurrency;
    use crate::testkit::execute_command_line;
    use garnet_common::parse_resp_command_arg_slices;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    fn parse_integer_response(response: &[u8]) -> i64 {
        assert!(response.len() >= 4);
        assert_eq!(response[0], b':');
        assert!(response.ends_with(b"\r\n"));
        core::str::from_utf8(&response[1..response.len() - 2])
            .unwrap()
            .parse::<i64>()
            .unwrap()
    }

    fn encode_resp(parts: &[&[u8]]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
        for part in parts {
            out.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
            out.extend_from_slice(part);
            out.extend_from_slice(b"\r\n");
        }
        out
    }

    fn execute_frame(processor: &RequestProcessor, frame: &[u8]) -> Vec<u8> {
        let mut args = [ArgSlice::EMPTY; 16];
        let meta = parse_resp_command_arg_slices(frame, &mut args).unwrap();
        let mut response = Vec::new();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        response
    }

    fn find_key_for_shard(processor: &RequestProcessor, shard: usize) -> Vec<u8> {
        for i in 0..20_000 {
            let candidate = format!("key-shard-{shard}-{i}").into_bytes();
            if processor.string_store_shard_index_for_key(&candidate) == shard {
                return candidate;
            }
        }
        panic!("failed to find key for shard index {shard}");
    }

    #[test]
    fn command_line_testkit_executes_against_in_memory_processor() {
        let processor = RequestProcessor::new().unwrap();
        assert_eq!(
            execute_command_line(&processor, "SET alpha one").unwrap(),
            b"+OK\r\n"
        );
        assert_eq!(
            execute_command_line(&processor, "GET alpha").unwrap(),
            b"$3\r\none\r\n"
        );
        assert_eq!(
            execute_command_line(&processor, "DEL alpha").unwrap(),
            b":1\r\n"
        );
        assert_eq!(
            execute_command_line(&processor, "GET alpha").unwrap(),
            b"$-1\r\n"
        );
    }

    #[test]
    fn derives_default_string_store_shards_from_owner_thread_hint() {
        assert_eq!(string_store_shard_count_from_values(None, None), 2);
        assert_eq!(string_store_shard_count_from_values(Some(4), None), 4);
        assert_eq!(string_store_shard_count_from_values(None, Some(1)), 1);
        assert_eq!(string_store_shard_count_from_values(None, Some(8)), 2);
        assert_eq!(string_store_shard_count_from_values(Some(3), Some(8)), 3);
    }

    #[test]
    fn scales_hash_index_bits_with_shard_count() {
        assert_eq!(scale_hash_index_bits_for_shards(25, 1), 25);
        assert_eq!(scale_hash_index_bits_for_shards(25, 2), 24);
        assert_eq!(scale_hash_index_bits_for_shards(25, 4), 23);
        assert_eq!(scale_hash_index_bits_for_shards(25, 16), 21);
        assert_eq!(scale_hash_index_bits_for_shards(25, 17), 20);
        assert_eq!(scale_hash_index_bits_for_shards(3, 16), 1);
    }

    #[test]
    fn tsavorite_config_values_use_server_hash_index_default() {
        let config = tsavorite_config_from_values(None, None, None);
        assert_eq!(
            config.hash_index_size_bits,
            DEFAULT_SERVER_HASH_INDEX_SIZE_BITS
        );
    }

    #[test]
    fn tsavorite_config_values_apply_valid_overrides() {
        let config = tsavorite_config_from_values(Some(20), Some(14), Some(4096));
        assert_eq!(config.hash_index_size_bits, 20);
        assert_eq!(config.page_size_bits, 14);
        assert_eq!(config.max_in_memory_pages, 4096);
    }

    #[test]
    fn tsavorite_config_values_ignore_invalid_overrides() {
        let config = tsavorite_config_from_values(Some(31), Some(31), Some(0));
        assert_eq!(
            config.hash_index_size_bits,
            DEFAULT_SERVER_HASH_INDEX_SIZE_BITS
        );
        assert_eq!(
            config.page_size_bits,
            TsavoriteKvConfig::default().page_size_bits
        );
        assert_eq!(
            config.max_in_memory_pages,
            TsavoriteKvConfig::default().max_in_memory_pages
        );
    }

    #[test]
    fn writers_toggle_record_has_expiration_from_upsert_user_data() {
        let functions = KvSessionFunctions;
        let key = b"key".to_vec();
        let input = b"value".to_vec();
        let src = Vec::new();
        let mut dst = Vec::new();
        let mut output = Vec::new();
        let mut info = UpsertInfo::default();
        let mut record_info = RecordInfo::default();

        info.user_data = UPSERT_USER_DATA_HAS_EXPIRATION;
        assert!(functions.single_writer(
            &key,
            &input,
            &src,
            &mut dst,
            &mut output,
            &mut info,
            WriteReason::Insert,
            &mut record_info
        ));
        assert!(record_info.has_expiration());

        info.user_data = 0;
        assert!(functions.concurrent_writer(
            &key,
            &input,
            &src,
            &mut dst,
            &mut output,
            &mut info,
            &mut record_info
        ));
        assert!(!record_info.has_expiration());
    }

    #[test]
    fn executes_set_then_get_roundtrip() {
        let processor = RequestProcessor::new().unwrap();
        let mut args = [ArgSlice::EMPTY; 3];
        let frame_set = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        let meta = parse_resp_command_arg_slices(frame_set, &mut args).unwrap();
        let mut response = Vec::new();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"+OK\r\n");

        let frame_get = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
        let meta = parse_resp_command_arg_slices(frame_get, &mut args).unwrap();
        response.clear();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"$5\r\nvalue\r\n");
    }

    #[test]
    fn debug_sync_points_can_force_get_set_ordering_between_threads() {
        let _test_guard = debug_concurrency::SYNC_TEST_MUTEX.lock().unwrap();
        debug_concurrency::reset_sync_points();
        let processor = Arc::new(RequestProcessor::new().unwrap());
        let key = b"key";

        let initial_set = encode_resp(&[b"SET", key, b"old"]);
        assert_eq!(execute_frame(&processor, &initial_set), b"+OK\r\n");

        const GET_BEFORE_STORE: &str = "request_processor.handle_get.before_store_lock";
        debug_concurrency::block_sync_point(GET_BEFORE_STORE);

        let getter = {
            let processor = Arc::clone(&processor);
            thread::spawn(move || {
                let get_frame = encode_resp(&[b"GET", key]);
                execute_frame(&processor, &get_frame)
            })
        };

        assert!(debug_concurrency::wait_for_sync_point_hits(
            GET_BEFORE_STORE,
            1,
            Duration::from_secs(2)
        ));

        let set_new = encode_resp(&[b"SET", key, b"new"]);
        assert_eq!(execute_frame(&processor, &set_new), b"+OK\r\n");

        debug_concurrency::unblock_sync_point(GET_BEFORE_STORE);
        let get_response = getter.join().expect("GET worker thread panicked");
        assert_eq!(get_response, b"$3\r\nnew\r\n");
        debug_concurrency::reset_sync_points();
    }

    #[test]
    fn sharded_string_stores_support_parallel_get_set_on_distinct_shards() {
        let processor = Arc::new(RequestProcessor::new_with_string_store_shards(4).unwrap());
        let key_a = find_key_for_shard(&processor, 0);
        let key_b = find_key_for_shard(&processor, 1);
        assert_ne!(
            processor.string_store_shard_index_for_key(&key_a),
            processor.string_store_shard_index_for_key(&key_b)
        );

        let worker_a = {
            let processor = Arc::clone(&processor);
            let key_a = key_a.clone();
            thread::spawn(move || {
                for i in 0..200 {
                    let value = format!("a-{i}").into_bytes();
                    let set = encode_resp(&[b"SET", &key_a, &value]);
                    assert_eq!(execute_frame(&processor, &set), b"+OK\r\n");
                    let get = encode_resp(&[b"GET", &key_a]);
                    let expected = format!(
                        "${}\r\n{}\r\n",
                        value.len(),
                        String::from_utf8_lossy(&value)
                    );
                    assert_eq!(execute_frame(&processor, &get), expected.as_bytes());
                }
            })
        };

        let worker_b = {
            let processor = Arc::clone(&processor);
            let key_b = key_b.clone();
            thread::spawn(move || {
                for i in 0..200 {
                    let value = format!("b-{i}").into_bytes();
                    let set = encode_resp(&[b"SET", &key_b, &value]);
                    assert_eq!(execute_frame(&processor, &set), b"+OK\r\n");
                    let get = encode_resp(&[b"GET", &key_b]);
                    let expected = format!(
                        "${}\r\n{}\r\n",
                        value.len(),
                        String::from_utf8_lossy(&value)
                    );
                    assert_eq!(execute_frame(&processor, &get), expected.as_bytes());
                }
            })
        };

        worker_a.join().unwrap();
        worker_b.join().unwrap();
    }

    #[test]
    fn sharded_string_metadata_tracks_keys_and_expiration_per_shard() {
        let processor = RequestProcessor::new_with_string_store_shards(4).unwrap();
        let key_a = find_key_for_shard(&processor, 0);
        let key_b = find_key_for_shard(&processor, 1);
        let shard_a = processor.string_store_shard_index_for_key(&key_a);
        let shard_b = processor.string_store_shard_index_for_key(&key_b);
        assert_ne!(shard_a, shard_b);

        let set_a = encode_resp(&[b"SET", &key_a, b"value-a", b"PX", b"5000"]);
        let set_b = encode_resp(&[b"SET", &key_b, b"value-b"]);
        assert_eq!(execute_frame(&processor, &set_a), b"+OK\r\n");
        assert_eq!(execute_frame(&processor, &set_b), b"+OK\r\n");

        assert!(processor.string_expiration_deadline(&key_a).is_some());
        assert!(processor.string_expiration_deadline(&key_b).is_none());
        assert_eq!(processor.string_expiration_count_for_shard(shard_a), 1);
        assert_eq!(processor.string_expiration_count_for_shard(shard_b), 0);

        assert!(processor.string_key_registries[shard_a]
            .lock()
            .expect("key registry mutex poisoned")
            .contains(&key_a));
        assert!(processor.string_key_registries[shard_b]
            .lock()
            .expect("key registry mutex poisoned")
            .contains(&key_b));
        assert!(!processor.string_key_registries[shard_a]
            .lock()
            .expect("key registry mutex poisoned")
            .contains(&key_b));
        assert!(!processor.string_key_registries[shard_b]
            .lock()
            .expect("key registry mutex poisoned")
            .contains(&key_a));
    }

    #[test]
    fn string_expiration_counts_stay_consistent_across_updates() {
        let processor = RequestProcessor::new_with_string_store_shards(4).unwrap();
        let key = find_key_for_shard(&processor, 0);
        let shard = processor.string_store_shard_index_for_key(&key);
        assert_eq!(processor.string_expiration_count_for_shard(shard), 0);

        let set_px = encode_resp(&[b"SET", &key, b"value", b"PX", b"5000"]);
        assert_eq!(execute_frame(&processor, &set_px), b"+OK\r\n");
        assert_eq!(processor.string_expiration_count_for_shard(shard), 1);

        let set_px_again = encode_resp(&[b"SET", &key, b"value", b"PX", b"7000"]);
        assert_eq!(execute_frame(&processor, &set_px_again), b"+OK\r\n");
        assert_eq!(processor.string_expiration_count_for_shard(shard), 1);

        let persist = encode_resp(&[b"PERSIST", &key]);
        assert_eq!(execute_frame(&processor, &persist), b":1\r\n");
        assert_eq!(processor.string_expiration_count_for_shard(shard), 0);

        let expire = encode_resp(&[b"EXPIRE", &key, b"1"]);
        assert_eq!(execute_frame(&processor, &expire), b":1\r\n");
        assert_eq!(processor.string_expiration_count_for_shard(shard), 1);

        let del = encode_resp(&[b"DEL", &key]);
        assert_eq!(execute_frame(&processor, &del), b":1\r\n");
        assert_eq!(processor.string_expiration_count_for_shard(shard), 0);
    }

    #[test]
    fn set_and_get_supports_1kb_payload_without_storage_failure() {
        let processor = RequestProcessor::new().unwrap();
        let mut args = [ArgSlice::EMPTY; 3];
        let value = vec![b'x'; 1024];

        let mut frame_set =
            format!("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n${}\r\n", value.len()).into_bytes();
        frame_set.extend_from_slice(&value);
        frame_set.extend_from_slice(b"\r\n");

        let meta = parse_resp_command_arg_slices(&frame_set, &mut args).unwrap();
        let mut response = Vec::new();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"+OK\r\n");

        let frame_get = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
        let meta = parse_resp_command_arg_slices(frame_get, &mut args).unwrap();
        response.clear();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();

        let mut expected = format!("${}\r\n", value.len()).into_bytes();
        expected.extend_from_slice(&value);
        expected.extend_from_slice(b"\r\n");
        assert_eq!(response, expected);
    }

    #[test]
    fn object_store_roundtrip_is_isolated_from_main_store() {
        let processor = RequestProcessor::new().unwrap();

        processor.object_upsert(b"obj", 3, b"payload").unwrap();
        let (object_type, payload) = processor.object_read(b"obj").unwrap().unwrap();
        assert_eq!(object_type, 3);
        assert_eq!(payload, b"payload");

        let mut args = [ArgSlice::EMPTY; 3];
        let mut response = Vec::new();
        let get = b"*2\r\n$3\r\nGET\r\n$3\r\nobj\r\n";
        let meta = parse_resp_command_arg_slices(get, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"$-1\r\n");

        response.clear();
        let set = b"*3\r\n$3\r\nSET\r\n$3\r\nobj\r\n$3\r\nstr\r\n";
        let meta = parse_resp_command_arg_slices(set, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"+OK\r\n");

        let (object_type, payload) = processor.object_read(b"obj").unwrap().unwrap();
        assert_eq!(object_type, 3);
        assert_eq!(payload, b"payload");

        assert!(processor.object_delete(b"obj").unwrap());
        assert!(processor.object_read(b"obj").unwrap().is_none());

        response.clear();
        let meta = parse_resp_command_arg_slices(get, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"$3\r\nstr\r\n");
    }

    #[test]
    fn migration_entry_roundtrip_preserves_string_and_expiration() {
        let source = RequestProcessor::new().unwrap();
        let target = RequestProcessor::new().unwrap();
        let mut args = [ArgSlice::EMPTY; 8];
        let mut response = Vec::new();

        let set = b"*5\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\nPX\r\n$4\r\n1000\r\n";
        let meta = parse_resp_command_arg_slices(set, &mut args).unwrap();
        source
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"+OK\r\n");

        let entry = source
            .export_migration_entry(b"key")
            .unwrap()
            .expect("source key should be exportable");
        assert!(matches!(&entry.value, MigrationValue::String(value) if value == b"value"));
        assert!(entry.expiration_unix_millis.is_some());

        target.import_migration_entry(&entry).unwrap();

        response.clear();
        let get = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
        let meta = parse_resp_command_arg_slices(get, &mut args).unwrap();
        target
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"$5\r\nvalue\r\n");

        response.clear();
        let pttl = b"*2\r\n$4\r\nPTTL\r\n$3\r\nkey\r\n";
        let meta = parse_resp_command_arg_slices(pttl, &mut args).unwrap();
        target
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        let ttl = parse_integer_response(&response);
        assert!(ttl > 0);
        assert!(ttl <= 1000);
    }

    #[test]
    fn migrate_keys_to_transfers_string_and_deletes_source() {
        let source = RequestProcessor::new().unwrap();
        let target = RequestProcessor::new().unwrap();
        let mut args = [ArgSlice::EMPTY; 4];
        let mut response = Vec::new();

        let set = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        let meta = parse_resp_command_arg_slices(set, &mut args).unwrap();
        source
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"+OK\r\n");

        let moved = source
            .migrate_keys_to(&target, &[b"key".to_vec()], true)
            .unwrap();
        assert_eq!(moved, 1);

        response.clear();
        let get = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
        let meta = parse_resp_command_arg_slices(get, &mut args).unwrap();
        source
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"$-1\r\n");

        response.clear();
        let meta = parse_resp_command_arg_slices(get, &mut args).unwrap();
        target
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"$5\r\nvalue\r\n");
    }

    #[test]
    fn migrate_keys_to_transfers_object_value() {
        let source = RequestProcessor::new().unwrap();
        let target = RequestProcessor::new().unwrap();
        let mut args = [ArgSlice::EMPTY; 8];
        let mut response = Vec::new();

        let hset = b"*4\r\n$4\r\nHSET\r\n$3\r\nkey\r\n$5\r\nfield\r\n$5\r\nvalue\r\n";
        let meta = parse_resp_command_arg_slices(hset, &mut args).unwrap();
        source
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":1\r\n");

        let moved = source
            .migrate_keys_to(&target, &[b"key".to_vec()], true)
            .unwrap();
        assert_eq!(moved, 1);

        response.clear();
        let hget = b"*3\r\n$4\r\nHGET\r\n$3\r\nkey\r\n$5\r\nfield\r\n";
        let meta = parse_resp_command_arg_slices(hget, &mut args).unwrap();
        source
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"$-1\r\n");

        response.clear();
        let meta = parse_resp_command_arg_slices(hget, &mut args).unwrap();
        target
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"$5\r\nvalue\r\n");
    }

    #[test]
    fn migrate_slot_to_moves_only_slot_matched_keys() {
        let source = RequestProcessor::new().unwrap();
        let target = RequestProcessor::new().unwrap();
        let mut args = [ArgSlice::EMPTY; 16];
        let mut response = Vec::new();

        let string_key = b"{slot-a}string".to_vec();
        let object_key = b"{slot-a}object".to_vec();
        let other_key = b"{slot-b}other".to_vec();
        let slot = redis_hash_slot(&string_key);
        assert_eq!(slot, redis_hash_slot(&object_key));
        assert_ne!(slot, redis_hash_slot(&other_key));

        let set_string = encode_resp(&[b"SET", &string_key, b"value-a"]);
        let meta = parse_resp_command_arg_slices(&set_string, &mut args).unwrap();
        source
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"+OK\r\n");

        response.clear();
        let hset_object = encode_resp(&[b"HSET", &object_key, b"field", b"value-b"]);
        let meta = parse_resp_command_arg_slices(&hset_object, &mut args).unwrap();
        source
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":1\r\n");

        response.clear();
        let set_other = encode_resp(&[b"SET", &other_key, b"value-c"]);
        let meta = parse_resp_command_arg_slices(&set_other, &mut args).unwrap();
        source
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"+OK\r\n");

        let moved = source.migrate_slot_to(&target, slot, 16, true).unwrap();
        assert_eq!(moved, 2);

        response.clear();
        let get_string = encode_resp(&[b"GET", &string_key]);
        let meta = parse_resp_command_arg_slices(&get_string, &mut args).unwrap();
        source
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"$-1\r\n");

        response.clear();
        let get_other = encode_resp(&[b"GET", &other_key]);
        let meta = parse_resp_command_arg_slices(&get_other, &mut args).unwrap();
        source
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"$7\r\nvalue-c\r\n");

        response.clear();
        let hget_object = encode_resp(&[b"HGET", &object_key, b"field"]);
        let meta = parse_resp_command_arg_slices(&hget_object, &mut args).unwrap();
        source
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"$-1\r\n");

        response.clear();
        let meta = parse_resp_command_arg_slices(&get_string, &mut args).unwrap();
        target
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"$7\r\nvalue-a\r\n");

        response.clear();
        let meta = parse_resp_command_arg_slices(&hget_object, &mut args).unwrap();
        target
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"$7\r\nvalue-b\r\n");

        response.clear();
        let meta = parse_resp_command_arg_slices(&get_other, &mut args).unwrap();
        target
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"$-1\r\n");
    }

    #[test]
    fn hash_commands_roundtrip_over_object_store() {
        let processor = RequestProcessor::new().unwrap();
        let mut args = [ArgSlice::EMPTY; 16];
        let mut response = Vec::new();

        let hset1 = b"*4\r\n$4\r\nHSET\r\n$3\r\nkey\r\n$6\r\nfield1\r\n$2\r\nv1\r\n";
        let meta = parse_resp_command_arg_slices(hset1, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":1\r\n");

        response.clear();
        let hset2 = b"*4\r\n$4\r\nHSET\r\n$3\r\nkey\r\n$6\r\nfield1\r\n$2\r\nv2\r\n";
        let meta = parse_resp_command_arg_slices(hset2, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":0\r\n");

        response.clear();
        let hget = b"*3\r\n$4\r\nHGET\r\n$3\r\nkey\r\n$6\r\nfield1\r\n";
        let meta = parse_resp_command_arg_slices(hget, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"$2\r\nv2\r\n");

        response.clear();
        let hset3 = b"*4\r\n$4\r\nHSET\r\n$3\r\nkey\r\n$6\r\nfield2\r\n$2\r\nv3\r\n";
        let meta = parse_resp_command_arg_slices(hset3, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":1\r\n");

        response.clear();
        let hgetall = b"*2\r\n$7\r\nHGETALL\r\n$3\r\nkey\r\n";
        let meta = parse_resp_command_arg_slices(hgetall, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(
            response,
            b"*4\r\n$6\r\nfield1\r\n$2\r\nv2\r\n$6\r\nfield2\r\n$2\r\nv3\r\n"
        );

        response.clear();
        let hdel = b"*4\r\n$4\r\nHDEL\r\n$3\r\nkey\r\n$6\r\nfield1\r\n$6\r\nfield9\r\n";
        let meta = parse_resp_command_arg_slices(hdel, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":1\r\n");

        response.clear();
        let hdel_last = b"*3\r\n$4\r\nHDEL\r\n$3\r\nkey\r\n$6\r\nfield2\r\n";
        let meta = parse_resp_command_arg_slices(hdel_last, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":1\r\n");

        response.clear();
        let meta = parse_resp_command_arg_slices(hgetall, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"*0\r\n");
    }

    #[test]
    fn hash_commands_return_wrongtype_for_string_keys() {
        let processor = RequestProcessor::new().unwrap();
        let mut args = [ArgSlice::EMPTY; 8];
        let mut response = Vec::new();

        let set = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        let meta = parse_resp_command_arg_slices(set, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"+OK\r\n");

        response.clear();
        let hget = b"*3\r\n$4\r\nHGET\r\n$3\r\nkey\r\n$5\r\nfield\r\n";
        let meta = parse_resp_command_arg_slices(hget, &mut args).unwrap();
        let err = processor
            .execute(&args[..meta.argument_count], &mut response)
            .err()
            .unwrap();
        err.append_resp_error(&mut response);
        assert_eq!(
            response,
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        );
    }

    #[test]
    fn list_commands_roundtrip_over_object_store() {
        let processor = RequestProcessor::new().unwrap();
        let mut args = [ArgSlice::EMPTY; 16];
        let mut response = Vec::new();

        let lpush = b"*4\r\n$5\r\nLPUSH\r\n$3\r\nkey\r\n$1\r\na\r\n$1\r\nb\r\n";
        let meta = parse_resp_command_arg_slices(lpush, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":2\r\n");

        response.clear();
        let rpush = b"*3\r\n$5\r\nRPUSH\r\n$3\r\nkey\r\n$1\r\nc\r\n";
        let meta = parse_resp_command_arg_slices(rpush, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":3\r\n");

        response.clear();
        let lrange_all = b"*4\r\n$6\r\nLRANGE\r\n$3\r\nkey\r\n$1\r\n0\r\n$2\r\n-1\r\n";
        let meta = parse_resp_command_arg_slices(lrange_all, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"*3\r\n$1\r\nb\r\n$1\r\na\r\n$1\r\nc\r\n");

        response.clear();
        let lpop = b"*2\r\n$4\r\nLPOP\r\n$3\r\nkey\r\n";
        let meta = parse_resp_command_arg_slices(lpop, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"$1\r\nb\r\n");

        response.clear();
        let rpop = b"*2\r\n$4\r\nRPOP\r\n$3\r\nkey\r\n";
        let meta = parse_resp_command_arg_slices(rpop, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"$1\r\nc\r\n");

        response.clear();
        let meta = parse_resp_command_arg_slices(lpop, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"$1\r\na\r\n");

        response.clear();
        let meta = parse_resp_command_arg_slices(lpop, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"$-1\r\n");
    }

    #[test]
    fn lrange_supports_negative_indexes() {
        let processor = RequestProcessor::new().unwrap();
        let mut args = [ArgSlice::EMPTY; 16];
        let mut response = Vec::new();

        let rpush =
            b"*6\r\n$5\r\nRPUSH\r\n$3\r\nkey\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n";
        let meta = parse_resp_command_arg_slices(rpush, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":4\r\n");

        response.clear();
        let lrange = b"*4\r\n$6\r\nLRANGE\r\n$3\r\nkey\r\n$2\r\n-3\r\n$2\r\n-2\r\n";
        let meta = parse_resp_command_arg_slices(lrange, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"*2\r\n$1\r\nb\r\n$1\r\nc\r\n");
    }

    #[test]
    fn list_commands_return_wrongtype_for_string_keys() {
        let processor = RequestProcessor::new().unwrap();
        let mut args = [ArgSlice::EMPTY; 8];
        let mut response = Vec::new();

        let set = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        let meta = parse_resp_command_arg_slices(set, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"+OK\r\n");

        response.clear();
        let lpush = b"*3\r\n$5\r\nLPUSH\r\n$3\r\nkey\r\n$1\r\nv\r\n";
        let meta = parse_resp_command_arg_slices(lpush, &mut args).unwrap();
        let err = processor
            .execute(&args[..meta.argument_count], &mut response)
            .err()
            .unwrap();
        err.append_resp_error(&mut response);
        assert_eq!(
            response,
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        );
    }

    #[test]
    fn set_commands_roundtrip_over_object_store() {
        let processor = RequestProcessor::new().unwrap();
        let mut args = [ArgSlice::EMPTY; 16];
        let mut response = Vec::new();

        let sadd = b"*5\r\n$4\r\nSADD\r\n$3\r\nkey\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nb\r\n";
        let meta = parse_resp_command_arg_slices(sadd, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":2\r\n");

        response.clear();
        let sismember_yes = b"*3\r\n$9\r\nSISMEMBER\r\n$3\r\nkey\r\n$1\r\nb\r\n";
        let meta = parse_resp_command_arg_slices(sismember_yes, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":1\r\n");

        response.clear();
        let sismember_no = b"*3\r\n$9\r\nSISMEMBER\r\n$3\r\nkey\r\n$1\r\nz\r\n";
        let meta = parse_resp_command_arg_slices(sismember_no, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":0\r\n");

        response.clear();
        let smembers = b"*2\r\n$8\r\nSMEMBERS\r\n$3\r\nkey\r\n";
        let meta = parse_resp_command_arg_slices(smembers, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"*2\r\n$1\r\na\r\n$1\r\nb\r\n");

        response.clear();
        let srem = b"*4\r\n$4\r\nSREM\r\n$3\r\nkey\r\n$1\r\na\r\n$1\r\nx\r\n";
        let meta = parse_resp_command_arg_slices(srem, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":1\r\n");

        response.clear();
        let meta = parse_resp_command_arg_slices(smembers, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"*1\r\n$1\r\nb\r\n");

        response.clear();
        let srem_last = b"*3\r\n$4\r\nSREM\r\n$3\r\nkey\r\n$1\r\nb\r\n";
        let meta = parse_resp_command_arg_slices(srem_last, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":1\r\n");

        response.clear();
        let meta = parse_resp_command_arg_slices(smembers, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"*0\r\n");
    }

    #[test]
    fn set_commands_return_wrongtype_for_string_keys() {
        let processor = RequestProcessor::new().unwrap();
        let mut args = [ArgSlice::EMPTY; 8];
        let mut response = Vec::new();

        let set = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        let meta = parse_resp_command_arg_slices(set, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"+OK\r\n");

        response.clear();
        let sadd = b"*3\r\n$4\r\nSADD\r\n$3\r\nkey\r\n$1\r\nv\r\n";
        let meta = parse_resp_command_arg_slices(sadd, &mut args).unwrap();
        let err = processor
            .execute(&args[..meta.argument_count], &mut response)
            .err()
            .unwrap();
        err.append_resp_error(&mut response);
        assert_eq!(
            response,
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        );
    }

    #[test]
    fn zset_commands_roundtrip_over_object_store() {
        let processor = RequestProcessor::new().unwrap();
        let mut args = [ArgSlice::EMPTY; 20];
        let mut response = Vec::new();

        let zadd =
            b"*6\r\n$4\r\nZADD\r\n$3\r\nkey\r\n$1\r\n2\r\n$3\r\ntwo\r\n$1\r\n1\r\n$3\r\none\r\n";
        let meta = parse_resp_command_arg_slices(zadd, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":2\r\n");

        response.clear();
        let zscore = b"*3\r\n$6\r\nZSCORE\r\n$3\r\nkey\r\n$3\r\none\r\n";
        let meta = parse_resp_command_arg_slices(zscore, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"$1\r\n1\r\n");

        response.clear();
        let zrange = b"*4\r\n$6\r\nZRANGE\r\n$3\r\nkey\r\n$1\r\n0\r\n$2\r\n-1\r\n";
        let meta = parse_resp_command_arg_slices(zrange, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"*2\r\n$3\r\none\r\n$3\r\ntwo\r\n");

        response.clear();
        let zadd_update = b"*4\r\n$4\r\nZADD\r\n$3\r\nkey\r\n$1\r\n3\r\n$3\r\none\r\n";
        let meta = parse_resp_command_arg_slices(zadd_update, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":0\r\n");

        response.clear();
        let zrange_after_update = b"*4\r\n$6\r\nZRANGE\r\n$3\r\nkey\r\n$1\r\n0\r\n$1\r\n1\r\n";
        let meta = parse_resp_command_arg_slices(zrange_after_update, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"*2\r\n$3\r\ntwo\r\n$3\r\none\r\n");

        response.clear();
        let zrem = b"*4\r\n$4\r\nZREM\r\n$3\r\nkey\r\n$3\r\none\r\n$4\r\nnone\r\n";
        let meta = parse_resp_command_arg_slices(zrem, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":1\r\n");

        response.clear();
        let zrem_last = b"*3\r\n$4\r\nZREM\r\n$3\r\nkey\r\n$3\r\ntwo\r\n";
        let meta = parse_resp_command_arg_slices(zrem_last, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":1\r\n");

        response.clear();
        let meta = parse_resp_command_arg_slices(zrange, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"*0\r\n");
    }

    #[test]
    fn zset_commands_return_wrongtype_for_string_keys() {
        let processor = RequestProcessor::new().unwrap();
        let mut args = [ArgSlice::EMPTY; 8];
        let mut response = Vec::new();

        let set = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        let meta = parse_resp_command_arg_slices(set, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"+OK\r\n");

        response.clear();
        let zadd = b"*4\r\n$4\r\nZADD\r\n$3\r\nkey\r\n$1\r\n1\r\n$1\r\nv\r\n";
        let meta = parse_resp_command_arg_slices(zadd, &mut args).unwrap();
        let err = processor
            .execute(&args[..meta.argument_count], &mut response)
            .err()
            .unwrap();
        err.append_resp_error(&mut response);
        assert_eq!(
            response,
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        );
    }

    #[test]
    fn executes_incr_command() {
        let processor = RequestProcessor::new().unwrap();
        let mut args = [ArgSlice::EMPTY; 2];
        let frame = b"*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n";
        let meta = parse_resp_command_arg_slices(frame, &mut args).unwrap();

        let mut response = Vec::new();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":1\r\n");

        response.clear();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":2\r\n");
    }

    #[test]
    fn executes_del_command() {
        let processor = RequestProcessor::new().unwrap();
        let mut args = [ArgSlice::EMPTY; 3];
        let set_frame = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        let set_meta = parse_resp_command_arg_slices(set_frame, &mut args).unwrap();
        let mut response = Vec::new();
        processor
            .execute(&args[..set_meta.argument_count], &mut response)
            .unwrap();

        let del_frame = b"*2\r\n$3\r\nDEL\r\n$3\r\nkey\r\n";
        let del_meta = parse_resp_command_arg_slices(del_frame, &mut args).unwrap();
        response.clear();
        processor
            .execute(&args[..del_meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":1\r\n");
    }

    #[test]
    fn key_can_be_recreated_after_delete() {
        let processor = RequestProcessor::new().unwrap();
        let mut args = [ArgSlice::EMPTY; 3];
        let mut response = Vec::new();

        let set_first = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        let meta = parse_resp_command_arg_slices(set_first, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"+OK\r\n");

        response.clear();
        let del = b"*2\r\n$3\r\nDEL\r\n$3\r\nkey\r\n";
        let meta = parse_resp_command_arg_slices(del, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":1\r\n");

        response.clear();
        let set_second = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$7\r\nupdated\r\n";
        let meta = parse_resp_command_arg_slices(set_second, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"+OK\r\n");

        response.clear();
        let get = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
        let meta = parse_resp_command_arg_slices(get, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"$7\r\nupdated\r\n");
    }

    #[test]
    fn set_supports_nx_and_xx_conditions() {
        let processor = RequestProcessor::new().unwrap();
        let mut args = [ArgSlice::EMPTY; 6];
        let mut response = Vec::new();

        let set_nx = b"*4\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\nNX\r\n";
        let meta = parse_resp_command_arg_slices(set_nx, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"+OK\r\n");

        response.clear();
        let set_nx_again = b"*4\r\n$3\r\nSET\r\n$3\r\nkey\r\n$6\r\nvalue2\r\n$2\r\nNX\r\n";
        let meta = parse_resp_command_arg_slices(set_nx_again, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"$-1\r\n");

        response.clear();
        let set_xx = b"*4\r\n$3\r\nSET\r\n$3\r\nkey\r\n$7\r\nupdated\r\n$2\r\nXX\r\n";
        let meta = parse_resp_command_arg_slices(set_xx, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"+OK\r\n");
    }

    #[test]
    fn set_with_px_expires_key() {
        let processor = RequestProcessor::new().unwrap();
        let mut args = [ArgSlice::EMPTY; 8];
        let mut response = Vec::new();

        let set_px = b"*5\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\nPX\r\n$2\r\n10\r\n";
        let meta = parse_resp_command_arg_slices(set_px, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"+OK\r\n");

        thread::sleep(Duration::from_millis(20));

        response.clear();
        let get = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
        let meta = parse_resp_command_arg_slices(get, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"$-1\r\n");
    }

    #[test]
    fn expiration_scan_removes_expired_keys_in_background_style() {
        let processor = RequestProcessor::new().unwrap();
        let mut args = [ArgSlice::EMPTY; 8];
        let mut response = Vec::new();

        let set_px = b"*5\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\nPX\r\n$2\r\n10\r\n";
        let meta = parse_resp_command_arg_slices(set_px, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"+OK\r\n");

        thread::sleep(Duration::from_millis(20));
        let removed = processor.expire_stale_keys(16).unwrap();
        assert_eq!(removed, 1);

        response.clear();
        let get = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
        let meta = parse_resp_command_arg_slices(get, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"$-1\r\n");
    }

    #[test]
    fn set_returns_error_for_invalid_expire_time() {
        let processor = RequestProcessor::new().unwrap();
        let mut args = [ArgSlice::EMPTY; 8];
        let mut response = Vec::new();

        let invalid = b"*5\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\nPX\r\n$1\r\n0\r\n";
        let meta = parse_resp_command_arg_slices(invalid, &mut args).unwrap();
        let err = processor
            .execute(&args[..meta.argument_count], &mut response)
            .err()
            .unwrap();
        err.append_resp_error(&mut response);
        assert_eq!(response, b"-ERR invalid expire time in 'set' command\r\n");
    }

    #[test]
    fn expire_ttl_and_pexpire_commands_work() {
        let processor = RequestProcessor::new().unwrap();
        let mut args = [ArgSlice::EMPTY; 8];
        let mut response = Vec::new();

        let set = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        let meta = parse_resp_command_arg_slices(set, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"+OK\r\n");

        response.clear();
        let ttl = b"*2\r\n$3\r\nTTL\r\n$3\r\nkey\r\n";
        let meta = parse_resp_command_arg_slices(ttl, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":-1\r\n");

        response.clear();
        let expire = b"*3\r\n$6\r\nEXPIRE\r\n$3\r\nkey\r\n$1\r\n1\r\n";
        let meta = parse_resp_command_arg_slices(expire, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":1\r\n");

        response.clear();
        let pttl = b"*2\r\n$4\r\nPTTL\r\n$3\r\nkey\r\n";
        let meta = parse_resp_command_arg_slices(pttl, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        let remaining = parse_integer_response(&response);
        assert!((0..=1000).contains(&remaining));

        response.clear();
        let pexpire_now = b"*3\r\n$7\r\nPEXPIRE\r\n$3\r\nkey\r\n$1\r\n0\r\n";
        let meta = parse_resp_command_arg_slices(pexpire_now, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":1\r\n");

        response.clear();
        let get = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
        let meta = parse_resp_command_arg_slices(get, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"$-1\r\n");

        response.clear();
        let ttl_after_delete = b"*2\r\n$3\r\nTTL\r\n$3\r\nkey\r\n";
        let meta = parse_resp_command_arg_slices(ttl_after_delete, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":-2\r\n");
    }

    #[test]
    fn expire_and_ttl_on_missing_key_follow_redis_codes() {
        let processor = RequestProcessor::new().unwrap();
        let mut args = [ArgSlice::EMPTY; 8];
        let mut response = Vec::new();

        let expire = b"*3\r\n$6\r\nEXPIRE\r\n$7\r\nmissing\r\n$2\r\n10\r\n";
        let meta = parse_resp_command_arg_slices(expire, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":0\r\n");

        response.clear();
        let ttl = b"*2\r\n$3\r\nTTL\r\n$7\r\nmissing\r\n";
        let meta = parse_resp_command_arg_slices(ttl, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":-2\r\n");

        response.clear();
        let pexpire = b"*3\r\n$7\r\nPEXPIRE\r\n$7\r\nmissing\r\n$2\r\n10\r\n";
        let meta = parse_resp_command_arg_slices(pexpire, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":0\r\n");

        response.clear();
        let pttl = b"*2\r\n$4\r\nPTTL\r\n$7\r\nmissing\r\n";
        let meta = parse_resp_command_arg_slices(pttl, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":-2\r\n");
    }

    #[test]
    fn expire_with_invalid_timeout_returns_integer_error() {
        let processor = RequestProcessor::new().unwrap();
        let mut args = [ArgSlice::EMPTY; 8];
        let mut response = Vec::new();

        let invalid = b"*3\r\n$6\r\nEXPIRE\r\n$7\r\nmissing\r\n$3\r\nbad\r\n";
        let meta = parse_resp_command_arg_slices(invalid, &mut args).unwrap();
        let err = processor
            .execute(&args[..meta.argument_count], &mut response)
            .err()
            .unwrap();
        err.append_resp_error(&mut response);
        assert_eq!(
            response,
            b"-ERR value is not an integer or out of range\r\n"
        );
    }

    #[test]
    fn persist_removes_existing_expiration() {
        let processor = RequestProcessor::new().unwrap();
        let mut args = [ArgSlice::EMPTY; 8];
        let mut response = Vec::new();

        let set_px = b"*5\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\nPX\r\n$4\r\n1000\r\n";
        let meta = parse_resp_command_arg_slices(set_px, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"+OK\r\n");

        response.clear();
        let persist = b"*2\r\n$7\r\nPERSIST\r\n$3\r\nkey\r\n";
        let meta = parse_resp_command_arg_slices(persist, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":1\r\n");

        response.clear();
        let ttl = b"*2\r\n$3\r\nTTL\r\n$3\r\nkey\r\n";
        let meta = parse_resp_command_arg_slices(ttl, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":-1\r\n");

        response.clear();
        let meta = parse_resp_command_arg_slices(persist, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":0\r\n");

        response.clear();
        let persist_missing = b"*2\r\n$7\r\nPERSIST\r\n$7\r\nmissing\r\n";
        let meta = parse_resp_command_arg_slices(persist_missing, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":0\r\n");
    }

    #[test]
    fn dbsize_counts_string_and_object_keys() {
        let processor = RequestProcessor::new().unwrap();
        let mut args = [ArgSlice::EMPTY; 8];
        let mut response = Vec::new();

        let set = b"*3\r\n$3\r\nSET\r\n$4\r\nskey\r\n$5\r\nvalue\r\n";
        let meta = parse_resp_command_arg_slices(set, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"+OK\r\n");

        response.clear();
        let hset = b"*4\r\n$4\r\nHSET\r\n$4\r\nhkey\r\n$5\r\nfield\r\n$5\r\nvalue\r\n";
        let meta = parse_resp_command_arg_slices(hset, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":1\r\n");

        response.clear();
        let dbsize = b"*1\r\n$6\r\nDBSIZE\r\n";
        let meta = parse_resp_command_arg_slices(dbsize, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":2\r\n");
    }

    #[test]
    fn info_dbsize_and_command_responses_are_generated() {
        let processor = RequestProcessor::new().unwrap();
        let mut args = [ArgSlice::EMPTY; 8];
        let mut response = Vec::new();

        let set = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        let meta = parse_resp_command_arg_slices(set, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();

        response.clear();
        let dbsize = b"*1\r\n$6\r\nDBSIZE\r\n";
        let meta = parse_resp_command_arg_slices(dbsize, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":1\r\n");

        response.clear();
        let info = b"*1\r\n$4\r\nINFO\r\n";
        let meta = parse_resp_command_arg_slices(info, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert!(response.starts_with(b"$"));
        assert!(response.windows("dbsize:1".len()).any(|w| w == b"dbsize:1"));

        response.clear();
        let command = b"*1\r\n$7\r\nCOMMAND\r\n";
        let meta = parse_resp_command_arg_slices(command, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert!(response.starts_with(b"*"));
        assert!(response.windows(7).any(|w| w == b"$3\r\nGET"));
        assert!(response.windows(10).any(|w| w == b"$6\r\nEXPIRE"));
    }

    #[test]
    fn storage_error_mapping_marks_buffer_full_as_capacity_exceeded() {
        let read = map_read_error(ReadOperationError::PageManager(
            PageManagerError::BufferFull {
                max_in_memory_pages: 64,
            },
        ));
        let upsert = map_upsert_error(UpsertOperationError::PageManager(
            PageManagerError::BufferFull {
                max_in_memory_pages: 64,
            },
        ));
        let delete = map_delete_error(DeleteOperationError::PageManager(
            PageManagerError::BufferFull {
                max_in_memory_pages: 64,
            },
        ));
        let rmw = map_rmw_error(RmwOperationError::PageManager(
            PageManagerError::BufferFull {
                max_in_memory_pages: 64,
            },
        ));
        assert_eq!(read, RequestExecutionError::StorageCapacityExceeded);
        assert_eq!(upsert, RequestExecutionError::StorageCapacityExceeded);
        assert_eq!(delete, RequestExecutionError::StorageCapacityExceeded);
        assert_eq!(rmw, RequestExecutionError::StorageCapacityExceeded);
    }

    #[test]
    fn read_error_mapping_marks_no_evictable_page_as_busy() {
        let mapped = map_read_error(ReadOperationError::PageResidency(
            PageResidencyError::NoEvictablePage {
                requested_page_index: 42,
            },
        ));
        assert_eq!(mapped, RequestExecutionError::StorageBusy);
    }

    #[test]
    fn storage_capacity_exceeded_formats_distinct_resp_error() {
        let mut response = Vec::new();
        RequestExecutionError::StorageCapacityExceeded.append_resp_error(&mut response);
        assert_eq!(
            response,
            b"-ERR storage capacity exceeded (increase max in-memory pages)\r\n"
        );
    }
}
