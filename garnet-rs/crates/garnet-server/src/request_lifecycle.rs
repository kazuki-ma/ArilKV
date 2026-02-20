//! Request lifecycle: parse result -> dispatch -> storage op -> RESP response.

use crate::debug_concurrency::{LockClass, OrderedMutex, OrderedMutexGuard};
use crate::{dispatch_from_arg_slices, CommandId};
use garnet_cluster::redis_hash_slot;
use garnet_common::ArgSlice;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tsavorite::{
    DeleteInfo, DeleteOperationStatus, ReadInfo, ReadOperationStatus, RmwOperationStatus,
    TsavoriteKV, TsavoriteKvInitError, UpsertInfo,
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

mod config;
mod errors;
mod hash_commands;
mod list_commands;
mod migration;
mod object_store;
mod resp;
mod server_commands;
mod session_functions;
mod set_commands;
mod string_commands;
mod value_codec;
mod zset_commands;

use self::config::{
    scale_hash_index_bits_for_shards, string_store_shard_count_from_env, tsavorite_config_from_env,
};
#[cfg(test)]
use self::config::{string_store_shard_count_from_values, tsavorite_config_from_values};
pub use self::errors::RequestExecutionError;
use self::errors::{
    map_delete_error, map_read_error, map_rmw_error, map_upsert_error, storage_failure,
};
use self::resp::{
    append_bulk_array, append_bulk_string, append_integer, append_null_bulk_string,
    append_simple_string, ascii_eq_ignore_case,
};
use self::session_functions::{KvSessionFunctions, ObjectSessionFunctions};
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

#[cfg(test)]
mod tests;
