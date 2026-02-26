//! Request lifecycle: parse result -> dispatch -> storage op -> RESP response.

use crate::CommandId;
use crate::command_spec::command_allowed_while_script_busy;
use crate::debug_concurrency::LockClass;
use crate::debug_concurrency::OrderedMutex;
use crate::debug_concurrency::OrderedMutexGuard;
use crate::dispatch_command_name;
use garnet_cluster::redis_hash_slot;
use garnet_common::ArgSlice;
use std::cell::Cell;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use tsavorite::DeleteInfo;
use tsavorite::DeleteOperationStatus;
use tsavorite::ReadInfo;
use tsavorite::ReadOperationStatus;
use tsavorite::RmwOperationStatus;
use tsavorite::TsavoriteKV;
use tsavorite::TsavoriteKvInitError;
use tsavorite::UpsertInfo;

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
const GARNET_INTEROP_FORCE_RESP3_ZSET_PAIRS_ENV: &str = "GARNET_INTEROP_FORCE_RESP3_ZSET_PAIRS";
const DEFAULT_SERVER_HASH_INDEX_SIZE_BITS: u8 = 16;
const DEFAULT_STRING_STORE_PAGE_SIZE_BITS: u8 = 22;
const DEFAULT_OBJECT_STORE_PAGE_SIZE_BITS: u8 = 20;
const DEFAULT_ZSET_MAX_LISTPACK_ENTRIES: usize = 128;
const DEFAULT_LIST_MAX_LISTPACK_SIZE: i64 = -2;
const DEFAULT_STRING_STORE_SHARDS: usize = 2;
const SINGLE_OWNER_THREAD_STRING_STORE_SHARDS: usize = 1;
const LATENCY_EVENT_HISTORY_CAPACITY: usize = 160;

thread_local! {
    static REQUEST_CLIENT_NO_TOUCH_MODE: Cell<bool> = const { Cell::new(false) };
}

struct ClientNoTouchScope {
    previous: bool,
}

impl ClientNoTouchScope {
    #[inline]
    fn enter(enabled: bool) -> Self {
        let previous = REQUEST_CLIENT_NO_TOUCH_MODE.with(|state| {
            let previous = state.get();
            state.set(enabled);
            previous
        });
        Self { previous }
    }
}

impl Drop for ClientNoTouchScope {
    fn drop(&mut self) {
        REQUEST_CLIENT_NO_TOUCH_MODE.with(|state| state.set(self.previous));
    }
}

#[inline]
fn current_client_no_touch_mode() -> bool {
    REQUEST_CLIENT_NO_TOUCH_MODE.with(Cell::get)
}

fn default_config_overrides() -> HashMap<Vec<u8>, Vec<u8>> {
    let mut values = HashMap::new();
    values.insert(b"appendonly".to_vec(), b"no".to_vec());
    values.insert(b"save".to_vec(), b"".to_vec());
    values.insert(b"dbfilename".to_vec(), b"dump.rdb".to_vec());
    values.insert(b"dir".to_vec(), b".".to_vec());
    values.insert(b"repl-backlog-size".to_vec(), b"1048576".to_vec());
    values.insert(b"min-replicas-to-write".to_vec(), b"0".to_vec());
    values.insert(b"min-slaves-to-write".to_vec(), b"0".to_vec());
    values.insert(b"replica-serve-stale-data".to_vec(), b"yes".to_vec());
    values.insert(b"maxmemory-samples".to_vec(), b"5".to_vec());
    values.insert(b"maxmemory-clients".to_vec(), b"0".to_vec());
    values.insert(
        b"client-query-buffer-limit".to_vec(),
        b"1073741824".to_vec(),
    );
    values.insert(b"bind".to_vec(), b"127.0.0.1".to_vec());
    values.insert(b"replicaof".to_vec(), b"".to_vec());
    values.insert(b"slaveof".to_vec(), b"".to_vec());
    values.insert(b"daemonize".to_vec(), b"no".to_vec());
    values.insert(b"key-load-delay".to_vec(), b"0".to_vec());
    values.insert(b"lazyfree-lazy-server-del".to_vec(), b"no".to_vec());
    values.insert(b"port".to_vec(), b"6379".to_vec());
    values
}

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
use self::command_helpers::ensure_min_arity;
#[allow(unused_imports)]
use self::command_helpers::ensure_one_of_arities;
#[allow(unused_imports)]
use self::command_helpers::ensure_paired_arity_after;
#[allow(unused_imports)]
use self::command_helpers::ensure_ranged_arity;
#[allow(unused_imports)]
use self::command_helpers::parse_scan_match_count_options;
#[allow(unused_imports)]
use self::command_helpers::require_exact_arity;
use self::config::interop_force_resp3_zset_pairs_from_env;
use self::config::scale_hash_index_bits_for_shards;
use self::config::scripting_enabled_from_env;
use self::config::scripting_runtime_config_from_env;
#[cfg(test)]
use self::config::scripting_runtime_config_from_values;
use self::config::string_store_shard_count_from_env;
#[cfg(test)]
use self::config::string_store_shard_count_from_values;
use self::config::tsavorite_config_from_env;
#[cfg(test)]
use self::config::tsavorite_config_from_values;
pub use self::errors::RequestExecutionError;
use self::errors::map_delete_error;
use self::errors::map_read_error;
use self::errors::map_rmw_error;
use self::errors::map_upsert_error;
use self::errors::storage_failure;
use self::resp::append_bulk_array;
use self::resp::append_bulk_string;
use self::resp::append_error;
use self::resp::append_integer;
use self::resp::append_null;
use self::resp::append_null_array;
use self::resp::append_null_bulk_string;
use self::resp::append_simple_string;
use self::resp::ascii_eq_ignore_case;
use self::session_functions::KvSessionFunctions;
use self::session_functions::ObjectSessionFunctions;
use self::value_codec::decode_object_value;
use self::value_codec::decode_stored_value;
use self::value_codec::deserialize_hash_object_payload;
use self::value_codec::deserialize_list_object_payload;
use self::value_codec::deserialize_set_object_payload;
use self::value_codec::deserialize_stream_object_payload;
use self::value_codec::deserialize_zset_object_payload;
use self::value_codec::encode_object_value;
use self::value_codec::encode_stored_value;
use self::value_codec::parse_f64_ascii;
use self::value_codec::parse_i64_ascii;
use self::value_codec::parse_u64_ascii;
use self::value_codec::serialize_hash_object_payload;
use self::value_codec::serialize_list_object_payload;
use self::value_codec::serialize_set_object_payload;
use self::value_codec::serialize_stream_object_payload;
use self::value_codec::serialize_zset_object_payload;

#[derive(Debug, Clone, Copy)]
struct TimestampMillis(u64);

impl TimestampMillis {
    const fn new(unix_millis: u64) -> Self {
        Self(unix_millis)
    }

    const fn as_u64(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct RedisKey(Vec<u8>);

impl RedisKey {
    pub(crate) fn as_slice(&self) -> &[u8] {
        &self.0
    }

    pub(crate) fn into_vec(self) -> Vec<u8> {
        self.0
    }
}

impl AsRef<[u8]> for RedisKey {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl std::borrow::Borrow<[u8]> for RedisKey {
    fn borrow(&self) -> &[u8] {
        self.as_slice()
    }
}

impl From<Vec<u8>> for RedisKey {
    fn from(value: Vec<u8>) -> Self {
        Self(value)
    }
}

impl From<&[u8]> for RedisKey {
    fn from(value: &[u8]) -> Self {
        Self(value.to_vec())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct HashField(Vec<u8>);

impl AsRef<[u8]> for HashField {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl std::borrow::Borrow<[u8]> for HashField {
    fn borrow(&self) -> &[u8] {
        self.as_ref()
    }
}

impl From<Vec<u8>> for HashField {
    fn from(value: Vec<u8>) -> Self {
        Self(value)
    }
}

impl From<&[u8]> for HashField {
    fn from(value: &[u8]) -> Self {
        Self(value.to_vec())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StringValue(Vec<u8>);

impl StringValue {
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }

    pub fn into_vec(self) -> Vec<u8> {
        self.0
    }
}

impl AsRef<[u8]> for StringValue {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl std::borrow::Borrow<[u8]> for StringValue {
    fn borrow(&self) -> &[u8] {
        self.as_slice()
    }
}

impl From<Vec<u8>> for StringValue {
    fn from(value: Vec<u8>) -> Self {
        Self(value)
    }
}

impl From<&[u8]> for StringValue {
    fn from(value: &[u8]) -> Self {
        Self(value.to_vec())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ItemKey(Vec<u8>);

impl ItemKey {
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }

    pub fn into_vec(self) -> Vec<u8> {
        self.0
    }
}

impl AsRef<[u8]> for ItemKey {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl std::borrow::Borrow<[u8]> for ItemKey {
    fn borrow(&self) -> &[u8] {
        self.as_slice()
    }
}

impl From<Vec<u8>> for ItemKey {
    fn from(value: Vec<u8>) -> Self {
        Self(value)
    }
}

impl From<&[u8]> for ItemKey {
    fn from(value: &[u8]) -> Self {
        Self(value.to_vec())
    }
}

#[derive(Debug, Clone, Copy)]
struct ExpirationMetadata {
    deadline: Instant,
    unix_millis: TimestampMillis,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MovedKeysizesEntry {
    histogram_type_index: usize,
    length: usize,
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
    String(StringValue),
    Object { object_type: u8, payload: Vec<u8> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationEntry {
    pub key: ItemKey,
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
    description: String,
    read_only: bool,
    allow_oom: bool,
    allow_stale: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RunningScriptKind {
    Script,
    Function,
}

#[derive(Debug, Clone)]
struct RunningScriptState {
    kind: RunningScriptKind,
    name: String,
    command: String,
    started_at: Instant,
    thread_id: std::thread::ThreadId,
}

#[derive(Debug, Default)]
struct FunctionRegistry {
    functions: HashMap<String, LoadedFunctionDescriptor>,
    library_sources: HashMap<String, Vec<u8>>,
    library_function_names: HashMap<String, Vec<String>>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct LatencySample {
    pub(crate) unix_seconds: u64,
    pub(crate) latency_millis: u64,
}

#[derive(Debug, Default)]
struct LatencyEventState {
    samples: VecDeque<LatencySample>,
    max_latency_millis: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClientUnblockMode {
    Timeout,
    Error,
}

pub struct RequestProcessor {
    string_stores: Vec<OrderedMutex<TsavoriteKV<Vec<u8>, Vec<u8>>>>,
    object_stores: Vec<OrderedMutex<TsavoriteKV<Vec<u8>, Vec<u8>>>>,
    string_expirations: Vec<OrderedMutex<HashMap<Vec<u8>, ExpirationMetadata>>>,
    hash_field_expirations:
        Vec<OrderedMutex<HashMap<Vec<u8>, HashMap<HashField, ExpirationMetadata>>>>,
    string_expiration_counts: Vec<AtomicUsize>,
    string_key_registries: Vec<OrderedMutex<HashSet<Vec<u8>>>>,
    object_key_registries: Vec<OrderedMutex<HashSet<Vec<u8>>>>,
    watch_versions: Vec<AtomicU64>,
    blocking_wait_queues: Mutex<HashMap<RedisKey, VecDeque<u64>>>,
    pending_client_unblocks: Mutex<HashMap<u64, ClientUnblockMode>>,
    forced_list_quicklist_keys: Mutex<HashSet<Vec<u8>>>,
    random_state: AtomicU64,
    active_expire_enabled: AtomicBool,
    expired_keys: AtomicU64,
    expired_keys_active: AtomicU64,
    lastsave_unix_seconds: AtomicU64,
    rdb_changes_since_last_save: AtomicU64,
    resp_protocol_version: AtomicUsize,
    interop_force_resp3_zset_pairs: bool,
    blocked_clients: AtomicU64,
    connected_clients: AtomicU64,
    watching_clients: AtomicU64,
    executed_command_count: AtomicU64,
    rdb_key_save_delay_micros: AtomicU64,
    rdb_bgsave_deadline_unix_millis: AtomicU64,
    command_calls: Mutex<HashMap<Vec<u8>, u64>>,
    command_failed_calls: Mutex<HashMap<Vec<u8>, u64>>,
    latency_events: Mutex<HashMap<Vec<u8>, LatencyEventState>>,
    moved_keysizes_by_db: Mutex<HashMap<usize, HashMap<RedisKey, MovedKeysizesEntry>>>,
    key_lru_access_millis: Mutex<HashMap<RedisKey, u64>>,
    key_lfu_frequency: Mutex<HashMap<RedisKey, u8>>,
    config_overrides: Mutex<HashMap<Vec<u8>, Vec<u8>>>,
    lazy_expired_keys_for_replication: Mutex<Vec<RedisKey>>,
    script_cache: Mutex<HashMap<String, Vec<u8>>>,
    script_cache_insertion_order: Mutex<VecDeque<String>>,
    script_cache_hits: AtomicU64,
    script_cache_misses: AtomicU64,
    script_cache_evictions: AtomicU64,
    script_runtime_timeouts: AtomicU64,
    used_memory_vm_functions: AtomicU64,
    lazyfreed_objects: AtomicU64,
    maxmemory_limit_bytes: AtomicU64,
    min_replicas_to_write: AtomicU64,
    replica_serve_stale_data: AtomicBool,
    script_running: AtomicBool,
    running_script: Mutex<Option<RunningScriptState>>,
    script_kill_requested: Arc<AtomicBool>,
    function_kill_requested: Arc<AtomicBool>,
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
        // Redis compatibility includes large-string update paths (for example APPEND growth into MiB
        // ranges). Keep default string pages large enough to fit those records in one page.
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
        let mut hash_field_expirations = Vec::with_capacity(store_shard_count);
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
            hash_field_expirations.push(OrderedMutex::new(
                HashMap::new(),
                LockClass::Expirations,
                "request_processor.hash_field_expirations",
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
            hash_field_expirations,
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
            active_expire_enabled: AtomicBool::new(true),
            expired_keys: AtomicU64::new(0),
            expired_keys_active: AtomicU64::new(0),
            lastsave_unix_seconds: AtomicU64::new(current_unix_time_millis().unwrap_or(0) / 1000),
            rdb_changes_since_last_save: AtomicU64::new(0),
            resp_protocol_version: AtomicUsize::new(2),
            interop_force_resp3_zset_pairs: interop_force_resp3_zset_pairs_from_env(),
            blocked_clients: AtomicU64::new(0),
            connected_clients: AtomicU64::new(0),
            watching_clients: AtomicU64::new(0),
            executed_command_count: AtomicU64::new(0),
            rdb_key_save_delay_micros: AtomicU64::new(0),
            rdb_bgsave_deadline_unix_millis: AtomicU64::new(0),
            command_calls: Mutex::new(HashMap::new()),
            command_failed_calls: Mutex::new(HashMap::new()),
            latency_events: Mutex::new(HashMap::new()),
            moved_keysizes_by_db: Mutex::new(HashMap::new()),
            key_lru_access_millis: Mutex::new(HashMap::new()),
            key_lfu_frequency: Mutex::new(HashMap::new()),
            config_overrides: Mutex::new(default_config_overrides()),
            lazy_expired_keys_for_replication: Mutex::new(Vec::new()),
            script_cache: Mutex::new(HashMap::new()),
            script_cache_insertion_order: Mutex::new(VecDeque::new()),
            script_cache_hits: AtomicU64::new(0),
            script_cache_misses: AtomicU64::new(0),
            script_cache_evictions: AtomicU64::new(0),
            script_runtime_timeouts: AtomicU64::new(0),
            used_memory_vm_functions: AtomicU64::new(0),
            lazyfreed_objects: AtomicU64::new(0),
            maxmemory_limit_bytes: AtomicU64::new(0),
            min_replicas_to_write: AtomicU64::new(0),
            replica_serve_stale_data: AtomicBool::new(true),
            script_running: AtomicBool::new(false),
            running_script: Mutex::new(None),
            script_kill_requested: Arc::new(AtomicBool::new(false)),
            function_kill_requested: Arc::new(AtomicBool::new(false)),
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

    pub(crate) fn expire_watch_key_if_needed(
        &self,
        key: &[u8],
    ) -> Result<(), RequestExecutionError> {
        self.expire_key_if_needed(key)
    }

    pub(crate) fn string_value_len_for_replication(
        &self,
        key: &[u8],
    ) -> Result<Option<usize>, RequestExecutionError> {
        self.expire_key_if_needed(key)?;
        let Some(value) = self.read_string_value(key)? else {
            if self.object_key_exists(key)? {
                return Err(RequestExecutionError::WrongType);
            }
            return Ok(None);
        };
        Ok(Some(value.len()))
    }

    pub(crate) fn refresh_watched_keys_before_exec(&self, watched_keys: &[(Vec<u8>, u64)]) {
        for (key, _) in watched_keys {
            let _ = self.expire_key_if_needed(key);
        }
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

    pub(super) fn emit_resp3_zset_pairs(&self) -> bool {
        self.resp_protocol_version() == 3 || self.interop_force_resp3_zset_pairs
    }

    pub(crate) fn blocked_clients(&self) -> u64 {
        self.blocked_clients.load(Ordering::Acquire)
    }

    pub(crate) fn set_connected_clients(&self, count: u64) {
        self.connected_clients.store(count, Ordering::Release);
    }

    pub(super) fn connected_clients(&self) -> u64 {
        self.connected_clients.load(Ordering::Acquire)
    }

    pub(super) fn watching_clients(&self) -> u64 {
        self.watching_clients.load(Ordering::Acquire)
    }

    pub(crate) fn executed_command_count(&self) -> u64 {
        self.executed_command_count.load(Ordering::Acquire)
    }

    pub(super) fn rdb_key_save_delay_micros(&self) -> u64 {
        self.rdb_key_save_delay_micros.load(Ordering::Acquire)
    }

    pub(super) fn set_rdb_key_save_delay_micros(&self, value: u64) {
        self.rdb_key_save_delay_micros
            .store(value, Ordering::Release);
    }

    pub(super) fn start_bgsave(&self) {
        let now = current_unix_time_millis().unwrap_or(0);
        let delay_micros = self.rdb_key_save_delay_micros();
        let delay_millis = if delay_micros == 0 {
            0
        } else {
            (delay_micros / 1_000).max(1)
        };
        self.rdb_bgsave_deadline_unix_millis
            .store(now.saturating_add(delay_millis), Ordering::Release);
    }

    pub(super) fn rdb_bgsave_in_progress(&self) -> bool {
        let deadline = self.rdb_bgsave_deadline_unix_millis.load(Ordering::Acquire);
        if deadline == 0 {
            return false;
        }
        let now = current_unix_time_millis().unwrap_or(0);
        if now < deadline {
            return true;
        }
        let _ = self.rdb_bgsave_deadline_unix_millis.compare_exchange(
            deadline,
            0,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        false
    }

    pub(super) fn set_config_value(&self, key: &[u8], value: Vec<u8>) {
        let normalized = key
            .iter()
            .map(|byte| byte.to_ascii_lowercase())
            .collect::<Vec<u8>>();
        if let Ok(mut values) = self.config_overrides.lock() {
            values.insert(normalized, value);
        }
    }

    pub(super) fn config_items_snapshot(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.config_overrides
            .lock()
            .map(|values| values.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default()
    }

    pub(crate) fn lazyfree_lazy_server_del_enabled(&self) -> bool {
        let Ok(values) = self.config_overrides.lock() else {
            return false;
        };
        values
            .get(b"lazyfree-lazy-server-del".as_slice())
            .map(|value| value.eq_ignore_ascii_case(b"yes"))
            .unwrap_or(false)
    }

    pub(crate) fn lazyexpire_nested_arbitrary_keys_enabled(&self) -> bool {
        let Ok(values) = self.config_overrides.lock() else {
            return true;
        };
        match values.get(b"lazyexpire-nested-arbitrary-keys".as_slice()) {
            Some(value) if value.eq_ignore_ascii_case(b"no") => false,
            _ => true,
        }
    }

    pub(crate) fn record_command_call(&self, command_name: &[u8]) {
        if command_name.is_empty() {
            return;
        }
        let normalized = normalize_ascii_lower(command_name);
        if let Ok(mut calls) = self.command_calls.lock() {
            *calls.entry(normalized).or_insert(0) += 1;
        }
    }

    pub(crate) fn command_call_counts_snapshot(&self) -> Vec<(Vec<u8>, u64)> {
        let Ok(calls) = self.command_calls.lock() else {
            return Vec::new();
        };
        let mut ordered = BTreeMap::new();
        for (command, count) in calls.iter() {
            ordered.insert(command.clone(), *count);
        }
        ordered.into_iter().collect()
    }

    pub(crate) fn record_command_failure(&self, command_name: &[u8]) {
        if command_name.is_empty() {
            return;
        }
        let normalized = normalize_ascii_lower(command_name);
        if let Ok(mut failed_calls) = self.command_failed_calls.lock() {
            *failed_calls.entry(normalized).or_insert(0) += 1;
        }
    }

    pub(crate) fn command_failed_call_counts_snapshot(&self) -> Vec<(Vec<u8>, u64)> {
        let Ok(failed_calls) = self.command_failed_calls.lock() else {
            return Vec::new();
        };
        let mut ordered = BTreeMap::new();
        for (command, count) in failed_calls.iter() {
            ordered.insert(command.clone(), *count);
        }
        ordered.into_iter().collect()
    }

    pub(crate) fn record_latency_event(&self, event_name: &[u8], latency_millis: u64) {
        if event_name.is_empty() || latency_millis == 0 {
            return;
        }
        let normalized = normalize_ascii_lower(event_name);
        let unix_seconds = current_unix_time_millis().unwrap_or(0) / 1000;
        let Ok(mut events) = self.latency_events.lock() else {
            return;
        };
        let event = events.entry(normalized).or_default();
        event.samples.push_back(LatencySample {
            unix_seconds,
            latency_millis,
        });
        while event.samples.len() > LATENCY_EVENT_HISTORY_CAPACITY {
            let _ = event.samples.pop_front();
        }
        event.max_latency_millis = event.max_latency_millis.max(latency_millis);
    }

    pub(crate) fn latency_history(&self, event_name: &[u8]) -> Vec<LatencySample> {
        let normalized = normalize_ascii_lower(event_name);
        let Ok(events) = self.latency_events.lock() else {
            return Vec::new();
        };
        let Some(state) = events.get(&normalized) else {
            return Vec::new();
        };
        state.samples.iter().copied().collect()
    }

    pub(crate) fn latency_latest(&self) -> Vec<(Vec<u8>, LatencySample, u64)> {
        let Ok(events) = self.latency_events.lock() else {
            return Vec::new();
        };
        let mut ordered = BTreeMap::new();
        for (event_name, state) in events.iter() {
            let Some(sample) = state.samples.back().copied() else {
                continue;
            };
            ordered.insert(event_name.clone(), (sample, state.max_latency_millis));
        }
        ordered
            .into_iter()
            .map(|(name, (sample, max_latency_millis))| (name, sample, max_latency_millis))
            .collect()
    }

    pub(crate) fn latency_graph_range(&self, event_name: &[u8]) -> Option<(u64, u64)> {
        let normalized = normalize_ascii_lower(event_name);
        let Ok(events) = self.latency_events.lock() else {
            return None;
        };
        let state = events.get(&normalized)?;
        let min_latency = state
            .samples
            .iter()
            .map(|sample| sample.latency_millis)
            .min()?;
        Some((state.max_latency_millis, min_latency))
    }

    pub(crate) fn reset_latency_events(&self, event_names: Option<&[Vec<u8>]>) -> usize {
        let Ok(mut events) = self.latency_events.lock() else {
            return 0;
        };
        match event_names {
            None => {
                let removed = events.len();
                events.clear();
                removed
            }
            Some(names) => {
                let mut removed = 0usize;
                for event_name in names {
                    let normalized = normalize_ascii_lower(event_name);
                    if events.remove(&normalized).is_some() {
                        removed += 1;
                    }
                }
                removed
            }
        }
    }

    pub(crate) fn enqueue_lazy_expired_key_for_replication(&self, key: &[u8]) {
        if let Ok(mut pending) = self.lazy_expired_keys_for_replication.lock() {
            pending.push(RedisKey::from(key));
        }
    }

    pub(crate) fn take_lazy_expired_keys_for_replication(&self) -> Vec<RedisKey> {
        let Ok(mut pending) = self.lazy_expired_keys_for_replication.lock() else {
            return Vec::new();
        };
        std::mem::take(&mut *pending)
    }

    pub(crate) fn reset_commandstats(&self) {
        if let Ok(mut calls) = self.command_calls.lock() {
            calls.clear();
        }
        if let Ok(mut failed_calls) = self.command_failed_calls.lock() {
            failed_calls.clear();
        }
    }

    pub(crate) fn render_commandstats_info_payload(&self) -> String {
        let mut payload = String::from("# Commandstats\r\n");
        let mut ordered = BTreeMap::<Vec<u8>, u64>::new();
        for (command, count) in self.command_call_counts_snapshot() {
            ordered.insert(command, count);
        }
        let failed_calls_by_command = self
            .command_failed_call_counts_snapshot()
            .into_iter()
            .collect::<HashMap<Vec<u8>, u64>>();
        for command in failed_calls_by_command.keys() {
            ordered.entry(command.clone()).or_insert(0);
        }
        for (command, count) in ordered {
            let failed_calls = failed_calls_by_command.get(&command).copied().unwrap_or(0);
            payload.push_str("cmdstat_");
            payload.push_str(&String::from_utf8_lossy(&command));
            payload.push_str(":calls=");
            payload.push_str(&count.to_string());
            payload.push_str(",usec=0,usec_per_call=0.00,rejected_calls=0,failed_calls=");
            payload.push_str(&failed_calls.to_string());
            payload.push_str("\r\n");
        }
        payload
    }

    pub(super) fn record_key_access(&self, key: &[u8], force_touch: bool) {
        if key.is_empty() {
            return;
        }
        if !force_touch && current_client_no_touch_mode() {
            return;
        }
        let now_millis = current_unix_time_millis().unwrap_or(0);
        if let Ok(mut lru_state) = self.key_lru_access_millis.lock() {
            lru_state.insert(RedisKey::from(key), now_millis);
        }
    }

    pub(super) fn clear_key_access(&self, key: &[u8]) {
        if key.is_empty() {
            return;
        }
        if let Ok(mut lru_state) = self.key_lru_access_millis.lock() {
            let _ = lru_state.remove(key);
        }
        if let Ok(mut lfu_state) = self.key_lfu_frequency.lock() {
            let _ = lfu_state.remove(key);
        }
    }

    pub(super) fn key_lru_millis(&self, key: &[u8]) -> Option<u64> {
        let Ok(lru_state) = self.key_lru_access_millis.lock() else {
            return None;
        };
        lru_state.get(key).copied()
    }

    pub(super) fn key_idle_seconds(&self, key: &[u8]) -> Option<i64> {
        let last_access_millis = self.key_lru_millis(key)?;
        let now_millis = current_unix_time_millis().unwrap_or(last_access_millis);
        let idle_millis = now_millis.saturating_sub(last_access_millis);
        i64::try_from(idle_millis / 1000).ok()
    }

    pub(super) fn set_key_idle_seconds(&self, key: &[u8], idle_seconds: u64) {
        if key.is_empty() {
            return;
        }
        let now_millis = current_unix_time_millis().unwrap_or(0);
        let idle_millis = idle_seconds.saturating_mul(1000);
        let lru_millis = now_millis.saturating_sub(idle_millis);
        if let Ok(mut lru_state) = self.key_lru_access_millis.lock() {
            lru_state.insert(RedisKey::from(key), lru_millis);
        }
    }

    pub(super) fn set_key_frequency(&self, key: &[u8], frequency: u8) {
        if key.is_empty() {
            return;
        }
        if let Ok(mut lfu_state) = self.key_lfu_frequency.lock() {
            lfu_state.insert(RedisKey::from(key), frequency);
        }
    }

    pub(super) fn key_frequency(&self, key: &[u8]) -> Option<u8> {
        let Ok(lfu_state) = self.key_lfu_frequency.lock() else {
            return None;
        };
        lfu_state.get(key).copied()
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

    pub(super) fn used_memory_vm_functions(&self) -> u64 {
        self.used_memory_vm_functions.load(Ordering::Relaxed)
    }

    pub(super) fn set_used_memory_vm_functions(&self, bytes: u64) {
        self.used_memory_vm_functions
            .store(bytes, Ordering::Relaxed);
    }

    pub(super) fn lazyfreed_objects(&self) -> u64 {
        self.lazyfreed_objects.load(Ordering::Relaxed)
    }

    pub(super) fn record_lazyfreed_objects(&self, count: u64) {
        if count == 0 {
            return;
        }
        self.lazyfreed_objects.fetch_add(count, Ordering::Relaxed);
    }

    pub(super) fn reset_lazyfreed_objects(&self) {
        self.lazyfreed_objects.store(0, Ordering::Relaxed);
    }

    pub(super) fn maxmemory_limit_bytes(&self) -> u64 {
        self.maxmemory_limit_bytes.load(Ordering::Acquire)
    }

    pub(super) fn set_maxmemory_limit_bytes(&self, value: u64) {
        self.maxmemory_limit_bytes.store(value, Ordering::Release);
    }

    pub(super) fn min_replicas_to_write(&self) -> u64 {
        self.min_replicas_to_write.load(Ordering::Acquire)
    }

    pub(super) fn set_min_replicas_to_write(&self, value: u64) {
        self.min_replicas_to_write.store(value, Ordering::Release);
    }

    pub(super) fn replica_serve_stale_data(&self) -> bool {
        self.replica_serve_stale_data.load(Ordering::Acquire)
    }

    pub(super) fn set_replica_serve_stale_data(&self, value: bool) {
        self.replica_serve_stale_data
            .store(value, Ordering::Release);
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

    pub(crate) fn register_blocking_wait(&self, client_id: u64, keys: &[RedisKey]) {
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

    pub(crate) fn unregister_blocking_wait(&self, client_id: u64, keys: &[RedisKey]) {
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

    pub(crate) fn is_blocking_wait_turn(&self, client_id: u64, keys: &[RedisKey]) -> bool {
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

    pub(super) fn active_expire_enabled(&self) -> bool {
        self.active_expire_enabled.load(Ordering::Acquire)
    }

    pub(super) fn set_active_expire_enabled(&self, enabled: bool) {
        self.active_expire_enabled.store(enabled, Ordering::Release);
    }

    pub(super) fn expired_keys(&self) -> u64 {
        self.expired_keys.load(Ordering::Relaxed)
    }

    pub(super) fn expired_keys_active(&self) -> u64 {
        self.expired_keys_active.load(Ordering::Relaxed)
    }

    pub(super) fn record_lazy_expired_keys(&self, count: u64) {
        if count == 0 {
            return;
        }
        self.expired_keys.fetch_add(count, Ordering::Relaxed);
        self.record_latency_event(b"expire-cycle", 25);
    }

    pub(super) fn record_active_expired_keys(&self, count: u64) {
        if count == 0 {
            return;
        }
        self.expired_keys.fetch_add(count, Ordering::Relaxed);
        self.expired_keys_active.fetch_add(count, Ordering::Relaxed);
        self.record_latency_event(b"expire-cycle", 25);
    }

    pub(super) fn reset_expiration_stats(&self) {
        self.expired_keys.store(0, Ordering::Relaxed);
        self.expired_keys_active.store(0, Ordering::Relaxed);
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

    pub(crate) fn execute_with_client_no_touch(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
        client_no_touch: bool,
    ) -> Result<(), RequestExecutionError> {
        let _scope = ClientNoTouchScope::enter(client_no_touch);
        self.execute(args, response_out)
    }

    pub(crate) fn command_rejected_while_script_busy(
        &self,
        command: CommandId,
        subcommand: Option<&[u8]>,
    ) -> Result<bool, RequestExecutionError> {
        if !self.script_running.load(Ordering::Acquire)
            || command_allowed_while_script_busy(command, subcommand)
        {
            return Ok(false);
        }
        let Ok(running_script) = self.running_script.lock() else {
            return Err(storage_failure(
                "script.running_state",
                "running script state lock poisoned",
            ));
        };
        let Some(state) = running_script.as_ref() else {
            return Ok(false);
        };
        Ok(state.thread_id != std::thread::current().id())
    }

    pub(crate) fn execute_bytes(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.is_empty() {
            return Err(RequestExecutionError::UnknownCommand);
        }
        self.executed_command_count.fetch_add(1, Ordering::Relaxed);

        let command = dispatch_command_name(args[0]);
        let subcommand = args.get(1).copied();
        if self.command_rejected_while_script_busy(command, subcommand)? {
            return Err(RequestExecutionError::BusyScript);
        }
        if command == CommandId::Unknown
            && self.maybe_handle_hash_field_expire_extension(args, response_out)?
        {
            return Ok(());
        }
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
    keep_ttl: bool,
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

        if ascii_eq_ignore_case(option, b"KEEPTTL") {
            if options.keep_ttl || options.expiration.is_some() {
                return Err(RequestExecutionError::SyntaxError);
            }
            options.keep_ttl = true;
            index += 1;
            continue;
        }

        if ascii_eq_ignore_case(option, b"EX") || ascii_eq_ignore_case(option, b"PX") {
            if options.expiration.is_some() || options.keep_ttl {
                return Err(RequestExecutionError::SyntaxError);
            }
            if index + 1 >= args.len() {
                return Err(RequestExecutionError::SyntaxError);
            }

            let value = args[index + 1];
            let amount = parse_i64_ascii(value).ok_or(RequestExecutionError::ValueNotInteger)?;
            if amount <= 0 {
                return Err(RequestExecutionError::InvalidExpireTime);
            }
            options.expiration = Some(
                expiration_metadata_from_relative_expire_amount(
                    amount,
                    ascii_eq_ignore_case(option, b"PX"),
                )
                .ok_or(RequestExecutionError::InvalidExpireTime)?,
            );
            index += 2;
            continue;
        }

        if ascii_eq_ignore_case(option, b"EXAT") || ascii_eq_ignore_case(option, b"PXAT") {
            if options.expiration.is_some() || options.keep_ttl {
                return Err(RequestExecutionError::SyntaxError);
            }
            if index + 1 >= args.len() {
                return Err(RequestExecutionError::SyntaxError);
            }

            let value = args[index + 1];
            let amount = parse_i64_ascii(value).ok_or(RequestExecutionError::ValueNotInteger)?;
            if amount <= 0 {
                return Err(RequestExecutionError::InvalidExpireTime);
            }
            let unix_millis_i64 = if ascii_eq_ignore_case(option, b"EXAT") {
                amount.checked_mul(1000)
            } else {
                Some(amount)
            };
            let unix_millis =
                u64::try_from(unix_millis_i64.ok_or(RequestExecutionError::InvalidExpireTime)?)
                    .map_err(|_| RequestExecutionError::InvalidExpireTime)?;
            let deadline = instant_from_unix_millis(unix_millis)
                .ok_or(RequestExecutionError::InvalidExpireTime)?;
            options.expiration = Some(ExpirationMetadata {
                deadline,
                unix_millis: TimestampMillis::new(unix_millis),
            });
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

fn normalize_ascii_lower(value: &[u8]) -> Vec<u8> {
    value
        .iter()
        .map(|byte| byte.to_ascii_lowercase())
        .collect::<Vec<u8>>()
}

fn expiration_metadata_from_relative_expire_amount(
    amount: i64,
    milliseconds: bool,
) -> Option<ExpirationMetadata> {
    if amount <= 0 {
        return None;
    }
    let now_millis = i64::try_from(current_unix_time_millis()?).ok()?;
    let delta = if milliseconds {
        amount
    } else {
        amount.checked_mul(1000)?
    };
    let unix_millis_i64 = now_millis.checked_add(delta)?;
    let unix_millis = u64::try_from(unix_millis_i64).ok()?;
    let deadline = instant_from_unix_millis(unix_millis)?;
    Some(ExpirationMetadata {
        deadline,
        unix_millis: TimestampMillis::new(unix_millis),
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
