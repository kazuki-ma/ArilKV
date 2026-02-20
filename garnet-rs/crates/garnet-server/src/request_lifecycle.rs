//! Request lifecycle: parse result -> dispatch -> storage op -> RESP response.

use crate::debug_concurrency::{LockClass, OrderedMutex, OrderedMutexGuard};
use crate::{dispatch_from_arg_slices, CommandId};
use garnet_cluster::redis_hash_slot;
use garnet_common::ArgSlice;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tsavorite::{
    DeleteInfo, DeleteOperationStatus, HybridLogDeleteAdapter, HybridLogReadAdapter,
    HybridLogRmwAdapter, HybridLogUpsertAdapter, ISessionFunctions, ReadInfo, ReadOperationStatus,
    RecordInfo, RmwInfo, RmwOperationStatus, TsavoriteKV, TsavoriteKvConfig, TsavoriteKvInitError,
    UpsertInfo, WriteReason,
};

const UPSERT_USER_DATA_HAS_EXPIRATION: u8 = 0x1;
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

mod errors;
mod hash_commands;
mod list_commands;
mod resp;
mod server_commands;
mod set_commands;
mod string_commands;
mod value_codec;
mod zset_commands;

pub use self::errors::RequestExecutionError;
use self::errors::{
    map_delete_error, map_read_error, map_rmw_error, map_upsert_error, storage_failure,
};
use self::resp::{
    append_bulk_array, append_bulk_string, append_integer, append_null_bulk_string,
    append_simple_string, ascii_eq_ignore_case,
};
use self::value_codec::{
    decode_object_value, decode_stored_value, deserialize_hash_object_payload,
    deserialize_list_object_payload, deserialize_set_object_payload,
    deserialize_zset_object_payload, encode_object_value, encode_stored_value, parse_f64_ascii,
    parse_i64_ascii, parse_u64_ascii, serialize_hash_object_payload, serialize_list_object_payload,
    serialize_set_object_payload, serialize_zset_object_payload,
};

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
mod tests;
