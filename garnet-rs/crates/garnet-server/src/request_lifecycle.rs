//! Request lifecycle: parse result -> dispatch -> storage op -> RESP response.

use crate::debug_concurrency::{LockClass, OrderedMutex, OrderedMutexGuard};
use crate::{dispatch_command_name, CommandId};
use garnet_cluster::redis_hash_slot;
use garnet_common::ArgSlice;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::Mutex;
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
const STREAM_OBJECT_TYPE_TAG: u8 = 6;
const WATCH_VERSION_MAP_SIZE: usize = 1024;
const WATCH_VERSION_MAP_MASK: usize = WATCH_VERSION_MAP_SIZE - 1;
const GARNET_HASH_INDEX_SIZE_BITS_ENV: &str = "GARNET_TSAVORITE_HASH_INDEX_SIZE_BITS";
const GARNET_PAGE_SIZE_BITS_ENV: &str = "GARNET_TSAVORITE_PAGE_SIZE_BITS";
const GARNET_MAX_IN_MEMORY_PAGES_ENV: &str = "GARNET_TSAVORITE_MAX_IN_MEMORY_PAGES";
const GARNET_STRING_STORE_SHARDS_ENV: &str = "GARNET_TSAVORITE_STRING_STORE_SHARDS";
const GARNET_STRING_OWNER_THREADS_ENV: &str = "GARNET_STRING_OWNER_THREADS";
const GARNET_SCRIPTING_ENABLED_ENV: &str = "GARNET_SCRIPTING_ENABLED";
const GARNET_SCRIPTING_MAX_SCRIPT_BYTES_ENV: &str = "GARNET_SCRIPTING_MAX_SCRIPT_BYTES";
const GARNET_SCRIPTING_CACHE_MAX_ENTRIES_ENV: &str = "GARNET_SCRIPTING_CACHE_MAX_ENTRIES";
const GARNET_SCRIPTING_MAX_MEMORY_BYTES_ENV: &str = "GARNET_SCRIPTING_MAX_MEMORY_BYTES";
const GARNET_SCRIPTING_MAX_EXECUTION_MILLIS_ENV: &str = "GARNET_SCRIPTING_MAX_EXECUTION_MILLIS";
const DEFAULT_SERVER_HASH_INDEX_SIZE_BITS: u8 = 16;
const DEFAULT_STRING_STORE_PAGE_SIZE_BITS: u8 = 18;
const DEFAULT_OBJECT_STORE_PAGE_SIZE_BITS: u8 = 18;
const DEFAULT_ZSET_MAX_LISTPACK_ENTRIES: usize = 128;
const DEFAULT_LIST_MAX_LISTPACK_SIZE: i64 = -2;
const DEFAULT_STRING_STORE_SHARDS: usize = 2;
const SINGLE_OWNER_THREAD_STRING_STORE_SHARDS: usize = 1;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct ScriptingRuntimeConfig {
    max_script_bytes: usize,
    cache_max_entries: usize,
    max_memory_bytes: usize,
    max_execution_millis: u64,
}

mod command_helpers;
mod config;
mod errors;
mod geo_commands;
mod hash_commands;
mod list_commands;
mod migration;
mod object_store;
mod resp;
mod scripting;
mod server_commands;
mod session_functions;
mod set_commands;
mod stream_commands;
mod string_commands;
mod string_store;
mod value_codec;
mod zset_commands;

#[allow(unused_imports)]
use self::command_helpers::{
    ensure_min_arity, ensure_one_of_arities, ensure_paired_arity_after, ensure_ranged_arity,
    parse_scan_match_count_options, require_exact_arity,
};
use self::config::{
    scale_hash_index_bits_for_shards, scripting_enabled_from_env,
    scripting_runtime_config_from_env, string_store_shard_count_from_env,
    tsavorite_config_from_env,
};
#[cfg(test)]
use self::config::{
    scripting_runtime_config_from_values, string_store_shard_count_from_values,
    tsavorite_config_from_values,
};
pub use self::errors::RequestExecutionError;
use self::errors::{
    map_delete_error, map_read_error, map_rmw_error, map_upsert_error, storage_failure,
};
use self::resp::{
    append_bulk_array, append_bulk_string, append_error, append_integer, append_null,
    append_null_array, append_null_bulk_string, append_simple_string, ascii_eq_ignore_case,
};
use self::session_functions::{KvSessionFunctions, ObjectSessionFunctions};
use self::value_codec::{
    decode_object_value, decode_stored_value, deserialize_hash_object_payload,
    deserialize_list_object_payload, deserialize_set_object_payload,
    deserialize_stream_object_payload, deserialize_zset_object_payload, encode_object_value,
    encode_stored_value, parse_f64_ascii, parse_i64_ascii, parse_u64_ascii,
    serialize_hash_object_payload, serialize_list_object_payload, serialize_set_object_payload,
    serialize_stream_object_payload, serialize_zset_object_payload,
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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct StreamObject {
    entries: BTreeMap<Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>>,
    groups: BTreeMap<Vec<u8>, Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LoadedFunctionDescriptor {
    library_name: String,
    read_only: bool,
}

#[derive(Debug, Default)]
struct FunctionRegistry {
    functions: HashMap<String, LoadedFunctionDescriptor>,
    library_sources: HashMap<String, Vec<u8>>,
    library_function_names: HashMap<String, Vec<String>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClientUnblockMode {
    Timeout,
    Error,
}

pub struct RequestProcessor {
    string_stores: Vec<OrderedMutex<TsavoriteKV<Vec<u8>, Vec<u8>>>>,
    object_stores: Vec<OrderedMutex<TsavoriteKV<Vec<u8>, Vec<u8>>>>,
    string_expirations: Vec<OrderedMutex<HashMap<Vec<u8>, Instant>>>,
    string_expiration_counts: Vec<AtomicUsize>,
    string_key_registries: Vec<OrderedMutex<HashSet<Vec<u8>>>>,
    object_key_registries: Vec<OrderedMutex<HashSet<Vec<u8>>>>,
    watch_versions: Vec<AtomicU64>,
    blocking_wait_queues: Mutex<HashMap<Vec<u8>, VecDeque<u64>>>,
    pending_client_unblocks: Mutex<HashMap<u64, ClientUnblockMode>>,
    forced_list_quicklist_keys: Mutex<HashSet<Vec<u8>>>,
    random_state: AtomicU64,
    lastsave_unix_seconds: AtomicU64,
    rdb_changes_since_last_save: AtomicU64,
    resp_protocol_version: AtomicUsize,
    blocked_clients: AtomicU64,
    watching_clients: AtomicU64,
    command_calls: Mutex<HashMap<Vec<u8>, u64>>,
    script_cache: Mutex<HashMap<String, Vec<u8>>>,
    script_cache_insertion_order: Mutex<VecDeque<String>>,
    script_cache_hits: AtomicU64,
    script_cache_misses: AtomicU64,
    script_cache_evictions: AtomicU64,
    script_runtime_timeouts: AtomicU64,
    scripting_runtime_config: ScriptingRuntimeConfig,
    function_registry: Mutex<FunctionRegistry>,
    scripting_enabled: bool,
    zset_max_listpack_entries: AtomicUsize,
    list_max_listpack_size: AtomicI64,
    functions: KvSessionFunctions,
    object_functions: ObjectSessionFunctions,
}

impl RequestProcessor {
    pub fn new() -> Result<Self, RequestProcessorInitError> {
        Self::new_with_options(
            string_store_shard_count_from_env(),
            scripting_enabled_from_env(),
            scripting_runtime_config_from_env(),
        )
    }

    #[cfg(test)]
    fn new_with_string_store_shards(
        store_shard_count: usize,
    ) -> Result<Self, RequestProcessorInitError> {
        Self::new_with_options(
            store_shard_count,
            scripting_enabled_from_env(),
            scripting_runtime_config_from_env(),
        )
    }

    #[cfg(test)]
    pub(crate) fn new_with_string_store_shards_and_scripting(
        store_shard_count: usize,
        scripting_enabled: bool,
    ) -> Result<Self, RequestProcessorInitError> {
        Self::new_with_options(
            store_shard_count,
            scripting_enabled,
            scripting_runtime_config_from_values(None, None, None, None),
        )
    }

    #[cfg(test)]
    fn new_with_string_store_shards_scripting_and_runtime(
        store_shard_count: usize,
        scripting_enabled: bool,
        max_script_bytes: Option<usize>,
        cache_max_entries: Option<usize>,
        max_memory_bytes: Option<usize>,
        max_execution_millis: Option<u64>,
    ) -> Result<Self, RequestProcessorInitError> {
        Self::new_with_options(
            store_shard_count,
            scripting_enabled,
            scripting_runtime_config_from_values(
                max_script_bytes,
                cache_max_entries,
                max_memory_bytes,
                max_execution_millis,
            ),
        )
    }

    fn new_with_options(
        store_shard_count: usize,
        scripting_enabled: bool,
        scripting_runtime_config: ScriptingRuntimeConfig,
    ) -> Result<Self, RequestProcessorInitError> {
        let store_shard_count = store_shard_count.max(1);
        let store_config = tsavorite_config_from_env();
        let mut string_store_config = store_config;
        string_store_config.hash_index_size_bits =
            scale_hash_index_bits_for_shards(store_config.hash_index_size_bits, store_shard_count);
        string_store_config.page_size_bits = string_store_config
            .page_size_bits
            .max(DEFAULT_STRING_STORE_PAGE_SIZE_BITS);
        string_store_config.max_in_memory_pages = string_store_config.max_in_memory_pages.max(256);
        let mut object_store_config = store_config;
        object_store_config.hash_index_size_bits =
            scale_hash_index_bits_for_shards(store_config.hash_index_size_bits, store_shard_count);
        // Redis compatibility tests exercise object payloads (including streams) well beyond 16 KiB.
        // Keep object pages large enough by default so those writes fit in a single record.
        object_store_config.page_size_bits = object_store_config
            .page_size_bits
            .max(DEFAULT_OBJECT_STORE_PAGE_SIZE_BITS);
        object_store_config.max_in_memory_pages = object_store_config.max_in_memory_pages.max(256);
        let mut string_stores = Vec::with_capacity(store_shard_count);
        let mut object_stores = Vec::with_capacity(store_shard_count);
        let mut string_expirations = Vec::with_capacity(store_shard_count);
        let mut string_expiration_counts = Vec::with_capacity(store_shard_count);
        let mut string_key_registries = Vec::with_capacity(store_shard_count);
        let mut object_key_registries = Vec::with_capacity(store_shard_count);
        for _ in 0..store_shard_count {
            string_stores.push(OrderedMutex::new(
                TsavoriteKV::new(string_store_config)?,
                LockClass::Store,
                "request_processor.store",
            ));
            object_stores.push(OrderedMutex::new(
                TsavoriteKV::new(object_store_config)?,
                LockClass::ObjectStore,
                "request_processor.object_store",
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
            object_key_registries.push(OrderedMutex::new(
                HashSet::new(),
                LockClass::ObjectKeyRegistry,
                "request_processor.object_key_registry",
            ));
        }
        Ok(Self {
            string_stores,
            object_stores,
            string_expirations,
            string_expiration_counts,
            string_key_registries,
            object_key_registries,
            watch_versions: (0..WATCH_VERSION_MAP_SIZE)
                .map(|_| AtomicU64::new(0))
                .collect(),
            blocking_wait_queues: Mutex::new(HashMap::new()),
            pending_client_unblocks: Mutex::new(HashMap::new()),
            forced_list_quicklist_keys: Mutex::new(HashSet::new()),
            random_state: AtomicU64::new(current_unix_time_millis().unwrap_or(0x9e3779b97f4a7c15)),
            lastsave_unix_seconds: AtomicU64::new(current_unix_time_millis().unwrap_or(0) / 1000),
            rdb_changes_since_last_save: AtomicU64::new(0),
            resp_protocol_version: AtomicUsize::new(2),
            blocked_clients: AtomicU64::new(0),
            watching_clients: AtomicU64::new(0),
            command_calls: Mutex::new(HashMap::new()),
            script_cache: Mutex::new(HashMap::new()),
            script_cache_insertion_order: Mutex::new(VecDeque::new()),
            script_cache_hits: AtomicU64::new(0),
            script_cache_misses: AtomicU64::new(0),
            script_cache_evictions: AtomicU64::new(0),
            script_runtime_timeouts: AtomicU64::new(0),
            scripting_runtime_config,
            function_registry: Mutex::new(FunctionRegistry::default()),
            scripting_enabled,
            zset_max_listpack_entries: AtomicUsize::new(DEFAULT_ZSET_MAX_LISTPACK_ENTRIES),
            list_max_listpack_size: AtomicI64::new(DEFAULT_LIST_MAX_LISTPACK_SIZE),
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

    pub(super) fn set_resp_protocol_version(&self, version: usize) {
        self.resp_protocol_version.store(version, Ordering::Release);
    }

    pub(super) fn resp_protocol_version(&self) -> usize {
        self.resp_protocol_version.load(Ordering::Acquire)
    }

    pub(crate) fn blocked_clients(&self) -> u64 {
        self.blocked_clients.load(Ordering::Acquire)
    }

    pub(super) fn watching_clients(&self) -> u64 {
        self.watching_clients.load(Ordering::Acquire)
    }

    pub(crate) fn record_command_call(&self, command_name: &[u8]) {
        if command_name.is_empty() {
            return;
        }
        let normalized = command_name
            .iter()
            .map(|byte| byte.to_ascii_lowercase())
            .collect::<Vec<u8>>();
        if let Ok(mut calls) = self.command_calls.lock() {
            *calls.entry(normalized).or_insert(0) += 1;
        }
    }

    pub(crate) fn reset_commandstats(&self) {
        if let Ok(mut calls) = self.command_calls.lock() {
            calls.clear();
        }
    }

    pub(crate) fn render_commandstats_info_payload(&self) -> String {
        let mut payload = String::from("# Commandstats\r\n");
        let Ok(calls) = self.command_calls.lock() else {
            return payload;
        };
        let mut ordered = BTreeMap::new();
        for (command, count) in calls.iter() {
            ordered.insert(command.clone(), *count);
        }
        for (command, count) in ordered {
            payload.push_str("cmdstat_");
            payload.push_str(&String::from_utf8_lossy(&command));
            payload.push_str(":calls=");
            payload.push_str(&count.to_string());
            payload.push_str(",usec=0,usec_per_call=0.00,rejected_calls=0,failed_calls=0\r\n");
        }
        payload
    }

    pub(super) fn scripting_enabled(&self) -> bool {
        self.scripting_enabled
    }

    pub(super) fn scripting_runtime_config(&self) -> ScriptingRuntimeConfig {
        self.scripting_runtime_config
    }

    pub(super) fn script_cache_entry_count(&self) -> usize {
        self.script_cache
            .lock()
            .map(|cache| cache.len())
            .unwrap_or_default()
    }

    pub(crate) fn cached_script_for_sha(&self, sha1: &[u8]) -> Option<Vec<u8>> {
        let normalized = String::from_utf8_lossy(sha1).to_ascii_lowercase();
        let Ok(cache) = self.script_cache.lock() else {
            return None;
        };
        cache.get(&normalized).cloned()
    }

    pub(super) fn script_cache_hits(&self) -> u64 {
        self.script_cache_hits.load(Ordering::Relaxed)
    }

    pub(super) fn script_cache_misses(&self) -> u64 {
        self.script_cache_misses.load(Ordering::Relaxed)
    }

    pub(super) fn script_cache_evictions(&self) -> u64 {
        self.script_cache_evictions.load(Ordering::Relaxed)
    }

    pub(super) fn script_runtime_timeouts(&self) -> u64 {
        self.script_runtime_timeouts.load(Ordering::Relaxed)
    }

    pub(super) fn record_script_cache_hit(&self) {
        self.script_cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn record_script_cache_miss(&self) {
        self.script_cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn record_script_cache_eviction(&self) {
        self.script_cache_evictions.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn record_script_runtime_timeout(&self) {
        self.script_runtime_timeouts.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn increment_blocked_clients(&self) {
        self.blocked_clients.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn decrement_blocked_clients(&self) {
        let mut current = self.blocked_clients.load(Ordering::Relaxed);
        loop {
            if current == 0 {
                break;
            }
            match self.blocked_clients.compare_exchange_weak(
                current,
                current - 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }
    }

    pub(crate) fn register_blocking_wait(&self, client_id: u64, keys: &[Vec<u8>]) {
        if keys.is_empty() {
            return;
        }
        // TLA+ (`BlockingDisconnectLeak`) `Block(c,k)` queue-enqueue critical section.
        if let Ok(mut queues) = self.blocking_wait_queues.lock() {
            for key in keys {
                let queue = queues.entry(key.clone()).or_insert_with(VecDeque::new);
                if !queue.contains(&client_id) {
                    queue.push_back(client_id);
                }
            }
        }
    }

    pub(crate) fn unregister_blocking_wait(&self, client_id: u64, keys: &[Vec<u8>]) {
        if keys.is_empty() {
            return;
        }
        // TLA+ cleanup side for both wake completion and `Disconnect(c)` handling.
        if let Ok(mut queues) = self.blocking_wait_queues.lock() {
            for key in keys {
                let mut should_remove = false;
                if let Some(queue) = queues.get_mut(key) {
                    queue.retain(|entry| *entry != client_id);
                    should_remove = queue.is_empty();
                }
                if should_remove {
                    let _ = queues.remove(key);
                }
            }
        }
    }

    pub(crate) fn is_blocking_wait_turn(&self, client_id: u64, keys: &[Vec<u8>]) -> bool {
        if keys.is_empty() {
            return true;
        }
        // TLA+ `WakeHead(k)` guard: caller may proceed only when it is at queue front.
        let Ok(queues) = self.blocking_wait_queues.lock() else {
            return true;
        };
        for key in keys {
            let front = queues.get(key).and_then(|queue| queue.front().copied());
            match front {
                Some(front_client) => return front_client == client_id,
                None => continue,
            }
        }
        true
    }

    pub(crate) fn request_client_unblock(&self, client_id: u64, mode: ClientUnblockMode) -> bool {
        let is_blocked = self
            .blocking_wait_queues
            .lock()
            .map(|queues| queues.values().any(|queue| queue.contains(&client_id)))
            .unwrap_or(false);
        if !is_blocked {
            return false;
        }
        if let Ok(mut pending) = self.pending_client_unblocks.lock() {
            pending.insert(client_id, mode);
            return true;
        }
        false
    }

    pub(crate) fn take_client_unblock_request(&self, client_id: u64) -> Option<ClientUnblockMode> {
        self.pending_client_unblocks
            .lock()
            .ok()
            .and_then(|mut pending| pending.remove(&client_id))
    }

    pub(crate) fn clear_client_unblock_request(&self, client_id: u64) {
        if let Ok(mut pending) = self.pending_client_unblocks.lock() {
            let _ = pending.remove(&client_id);
        }
    }

    pub(super) fn force_list_quicklist_encoding(&self, key: &[u8]) {
        if let Ok(mut forced) = self.forced_list_quicklist_keys.lock() {
            forced.insert(key.to_vec());
        }
    }

    pub(super) fn clear_forced_list_quicklist_encoding(&self, key: &[u8]) {
        if let Ok(mut forced) = self.forced_list_quicklist_keys.lock() {
            let _ = forced.remove(key);
        }
    }

    pub(super) fn clear_all_forced_list_quicklist_encodings(&self) {
        if let Ok(mut forced) = self.forced_list_quicklist_keys.lock() {
            forced.clear();
        }
    }

    pub(super) fn list_quicklist_encoding_is_forced(&self, key: &[u8]) -> bool {
        self.forced_list_quicklist_keys
            .lock()
            .map(|forced| forced.contains(key))
            .unwrap_or(false)
    }

    pub(super) fn lastsave_unix_seconds(&self) -> u64 {
        self.lastsave_unix_seconds.load(Ordering::Acquire)
    }

    pub(crate) fn record_rdb_change(&self, delta: u64) {
        if delta == 0 {
            return;
        }
        self.rdb_changes_since_last_save
            .fetch_add(delta, Ordering::Relaxed);
    }

    pub(super) fn rdb_changes_since_last_save(&self) -> u64 {
        self.rdb_changes_since_last_save.load(Ordering::Relaxed)
    }

    pub(super) fn reset_rdb_changes_since_last_save(&self) {
        self.rdb_changes_since_last_save.store(0, Ordering::Relaxed);
    }

    pub(super) fn next_random_u64(&self) -> u64 {
        let mut current = self.random_state.load(Ordering::Relaxed);
        loop {
            let seed = if current == 0 {
                0x9e3779b97f4a7c15
            } else {
                current
            };
            let mut next = seed;
            next ^= next >> 12;
            next ^= next << 25;
            next ^= next >> 27;
            if self
                .random_state
                .compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                return next.wrapping_mul(0x2545_F491_4F6C_DD1D);
            }
            current = self.random_state.load(Ordering::Relaxed);
        }
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
        let mut arg_bytes = Vec::with_capacity(args.len());
        extend_arg_bytes_from_slices(args, &mut arg_bytes);
        self.execute_bytes(&arg_bytes, response_out)
    }

    pub(crate) fn execute_bytes(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.is_empty() {
            return Err(RequestExecutionError::UnknownCommand);
        }

        let command = dispatch_command_name(args[0]);
        match command {
            CommandId::Get => self.handle_get(args, response_out),
            CommandId::Set => self.handle_set(args, response_out),
            CommandId::Setex => self.handle_setex(args, response_out),
            CommandId::Setnx => self.handle_setnx(args, response_out),
            CommandId::Strlen => self.handle_strlen(args, response_out),
            CommandId::Getrange => self.handle_getrange(args, response_out),
            CommandId::Substr => self.handle_substr(args, response_out),
            CommandId::Getbit => self.handle_getbit(args, response_out),
            CommandId::Setbit => self.handle_setbit(args, response_out),
            CommandId::Setrange => self.handle_setrange(args, response_out),
            CommandId::Bitcount => self.handle_bitcount(args, response_out),
            CommandId::Bitpos => self.handle_bitpos(args, response_out),
            CommandId::Bitop => self.handle_bitop(args, response_out),
            CommandId::Bitfield => self.handle_bitfield(args, response_out),
            CommandId::BitfieldRo => self.handle_bitfield_ro(args, response_out),
            CommandId::Lcs => self.handle_lcs(args, response_out),
            CommandId::Sort => self.handle_sort(args, response_out),
            CommandId::SortRo => self.handle_sort_ro(args, response_out),
            CommandId::Append => self.handle_append(args, response_out),
            CommandId::Getex => self.handle_getex(args, response_out),
            CommandId::Incrbyfloat => self.handle_incrbyfloat(args, response_out),
            CommandId::Msetnx => self.handle_msetnx(args, response_out),
            CommandId::Pfadd => self.handle_pfadd(args, response_out),
            CommandId::Pfcount => self.handle_pfcount(args, response_out),
            CommandId::Pfmerge => self.handle_pfmerge(args, response_out),
            CommandId::Pfdebug => self.handle_pfdebug(args, response_out),
            CommandId::Pfselftest => self.handle_pfselftest(args, response_out),
            CommandId::Touch => self.handle_touch(args, response_out),
            CommandId::Unlink => self.handle_unlink(args, response_out),
            CommandId::Move => self.handle_move(args, response_out),
            CommandId::Psetex => self.handle_psetex(args, response_out),
            CommandId::Getset => self.handle_getset(args, response_out),
            CommandId::Getdel => self.handle_getdel(args, response_out),
            CommandId::Del => self.handle_del(args, response_out),
            CommandId::Rename => self.handle_rename(args, response_out),
            CommandId::Renamenx => self.handle_renamenx(args, response_out),
            CommandId::Copy => self.handle_copy(args, response_out),
            CommandId::Incr => self.handle_incr_decr(args, 1, response_out),
            CommandId::Decr => self.handle_incr_decr(args, -1, response_out),
            CommandId::Incrby => self.handle_incrby_decrby(args, false, response_out),
            CommandId::Decrby => self.handle_incrby_decrby(args, true, response_out),
            CommandId::Exists => self.handle_exists(args, response_out),
            CommandId::Type => self.handle_type(args, response_out),
            CommandId::Mget => self.handle_mget(args, response_out),
            CommandId::Mset => self.handle_mset(args, response_out),
            CommandId::Expire => self.handle_expire(args, response_out),
            CommandId::Expireat => self.handle_expireat(args, response_out),
            CommandId::Expiretime => self.handle_expiretime(args, response_out),
            CommandId::Ttl => self.handle_ttl(args, response_out),
            CommandId::Pexpire => self.handle_pexpire(args, response_out),
            CommandId::Pexpireat => self.handle_pexpireat(args, response_out),
            CommandId::Pexpiretime => self.handle_pexpiretime(args, response_out),
            CommandId::Pttl => self.handle_pttl(args, response_out),
            CommandId::Persist => self.handle_persist(args, response_out),
            CommandId::Hset => self.handle_hset(args, response_out),
            CommandId::Hget => self.handle_hget(args, response_out),
            CommandId::Hdel => self.handle_hdel(args, response_out),
            CommandId::Hgetall => self.handle_hgetall(args, response_out),
            CommandId::Hlen => self.handle_hlen(args, response_out),
            CommandId::Hmget => self.handle_hmget(args, response_out),
            CommandId::Hmset => self.handle_hmset(args, response_out),
            CommandId::Hsetnx => self.handle_hsetnx(args, response_out),
            CommandId::Hexists => self.handle_hexists(args, response_out),
            CommandId::Hkeys => self.handle_hkeys(args, response_out),
            CommandId::Hvals => self.handle_hvals(args, response_out),
            CommandId::Hstrlen => self.handle_hstrlen(args, response_out),
            CommandId::Hincrby => self.handle_hincrby(args, response_out),
            CommandId::Hincrbyfloat => self.handle_hincrbyfloat(args, response_out),
            CommandId::Hrandfield => self.handle_hrandfield(args, response_out),
            CommandId::Lpush => self.handle_lpush(args, response_out),
            CommandId::Rpush => self.handle_rpush(args, response_out),
            CommandId::Lpop => self.handle_lpop(args, response_out),
            CommandId::Rpop => self.handle_rpop(args, response_out),
            CommandId::Lrange => self.handle_lrange(args, response_out),
            CommandId::Llen => self.handle_llen(args, response_out),
            CommandId::Lindex => self.handle_lindex(args, response_out),
            CommandId::Lpos => self.handle_lpos(args, response_out),
            CommandId::Lset => self.handle_lset(args, response_out),
            CommandId::Ltrim => self.handle_ltrim(args, response_out),
            CommandId::Lpushx => self.handle_lpushx(args, response_out),
            CommandId::Rpushx => self.handle_rpushx(args, response_out),
            CommandId::Lrem => self.handle_lrem(args, response_out),
            CommandId::Linsert => self.handle_linsert(args, response_out),
            CommandId::Lmove => self.handle_lmove(args, response_out),
            CommandId::Rpoplpush => self.handle_rpoplpush(args, response_out),
            CommandId::Lmpop => self.handle_lmpop(args, response_out),
            CommandId::Blmpop => self.handle_blmpop(args, response_out),
            CommandId::Blpop => self.handle_blpop(args, response_out),
            CommandId::Brpop => self.handle_brpop(args, response_out),
            CommandId::Blmove => self.handle_blmove(args, response_out),
            CommandId::Brpoplpush => self.handle_brpoplpush(args, response_out),
            CommandId::Sadd => self.handle_sadd(args, response_out),
            CommandId::Srem => self.handle_srem(args, response_out),
            CommandId::Smembers => self.handle_smembers(args, response_out),
            CommandId::Sismember => self.handle_sismember(args, response_out),
            CommandId::Scard => self.handle_scard(args, response_out),
            CommandId::Smismember => self.handle_smismember(args, response_out),
            CommandId::Srandmember => self.handle_srandmember(args, response_out),
            CommandId::Spop => self.handle_spop(args, response_out),
            CommandId::Smove => self.handle_smove(args, response_out),
            CommandId::Sdiff => self.handle_sdiff(args, response_out),
            CommandId::Sdiffstore => self.handle_sdiffstore(args, response_out),
            CommandId::Sinter => self.handle_sinter(args, response_out),
            CommandId::Sintercard => self.handle_sintercard(args, response_out),
            CommandId::Sinterstore => self.handle_sinterstore(args, response_out),
            CommandId::Sunion => self.handle_sunion(args, response_out),
            CommandId::Sunionstore => self.handle_sunionstore(args, response_out),
            CommandId::Zadd => self.handle_zadd(args, response_out),
            CommandId::Zrem => self.handle_zrem(args, response_out),
            CommandId::Zrange => self.handle_zrange(args, response_out),
            CommandId::Zrevrange => self.handle_zrevrange(args, response_out),
            CommandId::Zrangebyscore => self.handle_zrangebyscore(args, response_out),
            CommandId::Zrevrangebyscore => self.handle_zrevrangebyscore(args, response_out),
            CommandId::Zscore => self.handle_zscore(args, response_out),
            CommandId::Zcard => self.handle_zcard(args, response_out),
            CommandId::Zcount => self.handle_zcount(args, response_out),
            CommandId::Zrank => self.handle_zrank(args, response_out),
            CommandId::Zrevrank => self.handle_zrevrank(args, response_out),
            CommandId::Zincrby => self.handle_zincrby(args, response_out),
            CommandId::Zremrangebyrank => self.handle_zremrangebyrank(args, response_out),
            CommandId::Zremrangebyscore => self.handle_zremrangebyscore(args, response_out),
            CommandId::Zmscore => self.handle_zmscore(args, response_out),
            CommandId::Zrandmember => self.handle_zrandmember(args, response_out),
            CommandId::Zpopmin => self.handle_zpopmin(args, response_out),
            CommandId::Zpopmax => self.handle_zpopmax(args, response_out),
            CommandId::Bzpopmin => self.handle_bzpopmin(args, response_out),
            CommandId::Bzpopmax => self.handle_bzpopmax(args, response_out),
            CommandId::Zdiff => self.handle_zdiff(args, response_out),
            CommandId::Zdiffstore => self.handle_zdiffstore(args, response_out),
            CommandId::Zinter => self.handle_zinter(args, response_out),
            CommandId::Zinterstore => self.handle_zinterstore(args, response_out),
            CommandId::Zlexcount => self.handle_zlexcount(args, response_out),
            CommandId::Zrangestore => self.handle_zrangestore(args, response_out),
            CommandId::Zrangebylex => self.handle_zrangebylex(args, response_out),
            CommandId::Zrevrangebylex => self.handle_zrevrangebylex(args, response_out),
            CommandId::Zremrangebylex => self.handle_zremrangebylex(args, response_out),
            CommandId::Zintercard => self.handle_zintercard(args, response_out),
            CommandId::Zmpop => self.handle_zmpop(args, response_out),
            CommandId::Bzmpop => self.handle_bzmpop(args, response_out),
            CommandId::Zunion => self.handle_zunion(args, response_out),
            CommandId::Zunionstore => self.handle_zunionstore(args, response_out),
            CommandId::Xadd => self.handle_xadd(args, response_out),
            CommandId::Xdel => self.handle_xdel(args, response_out),
            CommandId::Xgroup => self.handle_xgroup(args, response_out),
            CommandId::Xreadgroup => self.handle_xreadgroup(args, response_out),
            CommandId::Xread => self.handle_xread(args, response_out),
            CommandId::Xack => self.handle_xack(args, response_out),
            CommandId::Xpending => self.handle_xpending(args, response_out),
            CommandId::Xclaim => self.handle_xclaim(args, response_out),
            CommandId::Xautoclaim => self.handle_xautoclaim(args, response_out),
            CommandId::Xsetid => self.handle_xsetid(args, response_out),
            CommandId::Xinfo => self.handle_xinfo(args, response_out),
            CommandId::Xlen => self.handle_xlen(args, response_out),
            CommandId::Xrange => self.handle_xrange(args, response_out),
            CommandId::Xrevrange => self.handle_xrevrange(args, response_out),
            CommandId::Xtrim => self.handle_xtrim(args, response_out),
            CommandId::Ping => self.handle_ping(args, response_out),
            CommandId::Echo => self.handle_echo(args, response_out),
            CommandId::Info => self.handle_info(args, response_out),
            CommandId::Memory => self.handle_memory(args, response_out),
            CommandId::Dbsize => self.handle_dbsize(args, response_out),
            CommandId::Debug => self.handle_debug(args, response_out),
            CommandId::Object => self.handle_object(args, response_out),
            CommandId::Keys => self.handle_keys(args, response_out),
            CommandId::Randomkey => self.handle_randomkey(args, response_out),
            CommandId::Scan => self.handle_scan(args, response_out),
            CommandId::Hscan => self.handle_hscan(args, response_out),
            CommandId::Sscan => self.handle_sscan(args, response_out),
            CommandId::Zscan => self.handle_zscan(args, response_out),
            CommandId::Flushdb => self.handle_flushdb(args, response_out),
            CommandId::Flushall => self.handle_flushall(args, response_out),
            CommandId::Function => self.handle_function(args, response_out),
            CommandId::Script => self.handle_script(args, response_out),
            CommandId::Eval => self.handle_eval(args, response_out),
            CommandId::EvalRo => self.handle_eval_ro(args, response_out),
            CommandId::Evalsha => self.handle_evalsha(args, response_out),
            CommandId::EvalshaRo => self.handle_evalsha_ro(args, response_out),
            CommandId::Fcall => self.handle_fcall(args, response_out),
            CommandId::FcallRo => self.handle_fcall_ro(args, response_out),
            CommandId::Config => self.handle_config(args, response_out),
            CommandId::Command => self.handle_command(args, response_out),
            CommandId::Dump => self.handle_dump(args, response_out),
            CommandId::Restore => self.handle_restore(args, response_out),
            CommandId::RestoreAsking => self.handle_restore_asking(args, response_out),
            CommandId::Latency => self.handle_latency(args, response_out),
            CommandId::Module => self.handle_module(args, response_out),
            CommandId::Slowlog => self.handle_slowlog(args, response_out),
            CommandId::Acl => self.handle_acl(args, response_out),
            CommandId::Cluster => self.handle_cluster(args, response_out),
            CommandId::Failover => self.handle_failover(args, response_out),
            CommandId::Subscribe => self.handle_subscribe(args, response_out),
            CommandId::Psubscribe => self.handle_psubscribe(args, response_out),
            CommandId::Ssubscribe => self.handle_ssubscribe(args, response_out),
            CommandId::Unsubscribe => self.handle_unsubscribe(args, response_out),
            CommandId::Punsubscribe => self.handle_punsubscribe(args, response_out),
            CommandId::Sunsubscribe => self.handle_sunsubscribe(args, response_out),
            CommandId::Publish => self.handle_publish(args, response_out),
            CommandId::Spublish => self.handle_spublish(args, response_out),
            CommandId::Pubsub => self.handle_pubsub(args, response_out),
            CommandId::Geoadd => self.handle_geoadd(args, response_out),
            CommandId::Geopos => self.handle_geopos(args, response_out),
            CommandId::Geodist => self.handle_geodist(args, response_out),
            CommandId::Geohash => self.handle_geohash(args, response_out),
            CommandId::Geosearch => self.handle_geosearch(args, response_out),
            CommandId::Geosearchstore => self.handle_geosearchstore(args, response_out),
            CommandId::Georadius => self.handle_georadius(args, response_out),
            CommandId::GeoradiusRo => self.handle_georadius_ro(args, response_out),
            CommandId::Georadiusbymember => self.handle_georadiusbymember(args, response_out),
            CommandId::GeoradiusbymemberRo => self.handle_georadiusbymember_ro(args, response_out),
            CommandId::Migrate => self.handle_migrate(args, response_out),
            CommandId::Monitor => self.handle_monitor(args, response_out),
            CommandId::Shutdown => self.handle_shutdown(args, response_out),
            CommandId::Hello => self.handle_hello(args, response_out),
            CommandId::Lastsave => self.handle_lastsave(args, response_out),
            CommandId::Auth => self.handle_auth(args, response_out),
            CommandId::Select => self.handle_select(args, response_out),
            CommandId::Client => self.handle_client(args, response_out),
            CommandId::Role => self.handle_role(args, response_out),
            CommandId::Wait => self.handle_wait(args, response_out),
            CommandId::Waitaof => self.handle_waitaof(args, response_out),
            CommandId::Save => self.handle_save(args, response_out),
            CommandId::Bgsave => self.handle_bgsave(args, response_out),
            CommandId::Bgrewriteaof => self.handle_bgrewriteaof(args, response_out),
            CommandId::Readonly => self.handle_readonly(args, response_out),
            CommandId::Readwrite => self.handle_readwrite(args, response_out),
            CommandId::Reset => self.handle_reset(args, response_out),
            CommandId::Lolwut => self.handle_lolwut(args, response_out),
            CommandId::Quit => self.handle_quit(args, response_out),
            CommandId::Time => self.handle_time(args, response_out),
            CommandId::Swapdb => self.handle_swapdb(args, response_out),
            CommandId::Multi
            | CommandId::Exec
            | CommandId::Discard
            | CommandId::Watch
            | CommandId::Unwatch
            | CommandId::Asking
            | CommandId::Unknown => Err(RequestExecutionError::UnknownCommand),
        }
    }
}

#[inline]
fn extend_arg_bytes_from_slices<'a>(args: &'a [ArgSlice], out: &mut Vec<&'a [u8]>) {
    out.clear();
    out.reserve(args.len().saturating_sub(out.capacity()));
    for arg in args {
        // SAFETY: callers invoke this at command-frame boundaries where each ArgSlice
        // points into a live frame buffer for the duration of the dispatch.
        out.push(unsafe { arg.as_slice() });
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

fn parse_set_options(args: &[&[u8]]) -> Result<SetOptions, RequestExecutionError> {
    let mut options = SetOptions::default();
    let mut index = 3usize;

    while index < args.len() {
        let option = args[index];

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

            let value = args[index + 1];
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
