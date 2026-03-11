//! Request lifecycle: parse result -> dispatch -> storage op -> RESP response.
//!
//! TLA+ model linkage:
//! - formal/tla/specs/BlockingDisconnectLeak.tla
//! - formal/tla/specs/BlockingWaitClassIsolation.tla
//! - formal/tla/specs/BlockingStreamAckGate.tla
//! - formal/tla/specs/BlockingXreadgroupClaimWait.tla
//! - formal/tla/specs/BlockingCountVisibility.tla
//! - formal/tla/specs/ClientPauseUnblockRace.tla
//! - formal/tla/specs/StreamPelOwnership.tla

use crate::ClientId;
use crate::CommandId;
use crate::command_spec::command_allowed_while_script_busy;
use crate::command_spec::command_is_effectively_mutating;
use crate::debug_concurrency::LockClass;
use crate::debug_concurrency::OrderedMutex;
use crate::debug_concurrency::OrderedMutexGuard;
use crate::dispatch_command_name;
use garnet_cluster::redis_hash_slot;
use garnet_common::ArgSlice;
use std::cell::Cell;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::process::Child;
use std::process::Command;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::AtomicU32;
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
use tsavorite::SessionUserData;
use tsavorite::TsavoriteKV;
use tsavorite::TsavoriteKvConfig;
use tsavorite::TsavoriteKvInitError;
use tsavorite::UpsertInfo;

const UPSERT_USER_DATA_HAS_EXPIRATION: SessionUserData = SessionUserData::from_bits(0x1);
const HASH_OBJECT_TYPE_TAG: ObjectTypeTag = ObjectTypeTag::Hash;
const LIST_OBJECT_TYPE_TAG: ObjectTypeTag = ObjectTypeTag::List;
const SET_OBJECT_TYPE_TAG: ObjectTypeTag = ObjectTypeTag::Set;
const ZSET_OBJECT_TYPE_TAG: ObjectTypeTag = ObjectTypeTag::Zset;
const STREAM_OBJECT_TYPE_TAG: ObjectTypeTag = ObjectTypeTag::Stream;
const WATCH_VERSION_MAP_SIZE: usize = 1024;
const WATCH_VERSION_MAP_MASK: usize = WATCH_VERSION_MAP_SIZE - 1;

// Keyspace notification flags (matching Valkey/Redis bit layout).
const NOTIFY_KEYSPACE: u32 = 1 << 0; // K
const NOTIFY_KEYEVENT: u32 = 1 << 1; // E
const NOTIFY_GENERIC: u32 = 1 << 2; // g
const NOTIFY_STRING: u32 = 1 << 3; // $
const NOTIFY_LIST: u32 = 1 << 4; // l
const NOTIFY_SET: u32 = 1 << 5; // s
const NOTIFY_HASH: u32 = 1 << 6; // h
const NOTIFY_ZSET: u32 = 1 << 7; // z
const NOTIFY_EXPIRED: u32 = 1 << 8; // x
const NOTIFY_EVICTED: u32 = 1 << 9; // e
const NOTIFY_STREAM: u32 = 1 << 10; // t
const NOTIFY_KEY_MISS: u32 = 1 << 11; // m
const NOTIFY_MODULE: u32 = 1 << 13; // d
const NOTIFY_NEW: u32 = 1 << 14; // n
const NOTIFY_OVERWRITTEN: u32 = 1 << 15; // o
const NOTIFY_TYPE_CHANGED: u32 = 1 << 16; // c
const NOTIFY_ALL: u32 = NOTIFY_GENERIC
    | NOTIFY_STRING
    | NOTIFY_LIST
    | NOTIFY_SET
    | NOTIFY_HASH
    | NOTIFY_ZSET
    | NOTIFY_EXPIRED
    | NOTIFY_EVICTED
    | NOTIFY_STREAM
    | NOTIFY_MODULE;
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
const DEFAULT_OBJECT_STORE_PAGE_SIZE_BITS: u8 = 22;
const DEFAULT_ZSET_MAX_LISTPACK_ENTRIES: usize = 128;
const DEFAULT_LIST_MAX_LISTPACK_SIZE: i64 = -2;
/// Default quicklist packed threshold: 0 means disabled (no per-element byte-size limit
/// beyond the normal listpack byte-budget).  Redis uses 0 as the disabled sentinel.
const DEFAULT_QUICKLIST_PACKED_THRESHOLD: usize = 0;
const DEFAULT_HASH_MAX_LISTPACK_ENTRIES: usize = 128;
const DEFAULT_HASH_MAX_LISTPACK_VALUE: usize = 64;
const DEFAULT_SET_MAX_LISTPACK_ENTRIES: usize = 128;
const DEFAULT_SET_MAX_LISTPACK_VALUE: usize = 64;
const DEFAULT_SET_MAX_INTSET_ENTRIES: usize = 512;
const DEFAULT_PROTO_MAX_BULK_LEN: usize = 512 * 1024 * 1024;
const DEFAULT_STRING_STORE_SHARDS: usize = 2;
const SINGLE_OWNER_THREAD_STRING_STORE_SHARDS: usize = 1;
const LATENCY_EVENT_HISTORY_CAPACITY: usize = 160;
const TRACKING_INVALIDATE_CHANNEL: &[u8] = b"__redis__:invalidate";
const SET_OBJECT_HOT_STATE_CAPACITY: usize = 8;
const SET_DEBUG_HT_MIN_TABLE_SIZE: usize = 4;

thread_local! {
    static REQUEST_EXECUTION_CONTEXT: Cell<RequestExecutionContext> = const {
        Cell::new(RequestExecutionContext {
            client_no_touch: false,
            client_id: None,
            in_transaction: false,
            selected_db: DbName::new(0),
            tracking_reads_enabled: false,
        })
    };
    static TRACKING_INVALIDATION_STACK: RefCell<Vec<Vec<RedisKey>>> = const {
        RefCell::new(Vec::new())
    };
    static TRACKING_READ_STACK: RefCell<Vec<Vec<RedisKey>>> = const {
        RefCell::new(Vec::new())
    };
    static RESP_PROTOCOL_OVERRIDE: Cell<Option<RespProtocolVersion>> = const {
        Cell::new(None)
    };
    static CURRENT_PROCESSOR_PTR: Cell<*const RequestProcessor> = const {
        Cell::new(std::ptr::null())
    };
    static EXPIRED_DATA_ACCESS_OVERRIDE: Cell<bool> = const {
        Cell::new(false)
    };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RequestExecutionContext {
    client_no_touch: bool,
    client_id: Option<ClientId>,
    in_transaction: bool,
    selected_db: DbName,
    tracking_reads_enabled: bool,
}

#[derive(Debug, Clone, Copy)]
struct ScriptTimeSnapshot {
    unix_micros: u64,
    instant: Instant,
}

struct RequestExecutionContextScope {
    previous: RequestExecutionContext,
}

struct CurrentProcessorScope {
    previous: *const RequestProcessor,
}

impl RequestExecutionContextScope {
    #[inline]
    fn enter(context: RequestExecutionContext) -> Self {
        let previous = REQUEST_EXECUTION_CONTEXT.with(|state| {
            let previous = state.get();
            state.set(context);
            previous
        });
        Self { previous }
    }
}

impl CurrentProcessorScope {
    #[inline]
    fn enter(processor: *const RequestProcessor) -> Self {
        let previous = CURRENT_PROCESSOR_PTR.with(|state| {
            let previous = state.get();
            state.set(processor);
            previous
        });
        Self { previous }
    }
}

impl Drop for CurrentProcessorScope {
    fn drop(&mut self) {
        CURRENT_PROCESSOR_PTR.with(|state| state.set(self.previous));
    }
}

impl Drop for RequestExecutionContextScope {
    fn drop(&mut self) {
        REQUEST_EXECUTION_CONTEXT.with(|state| state.set(self.previous));
    }
}

#[inline]
fn current_client_no_touch_mode() -> bool {
    REQUEST_EXECUTION_CONTEXT.with(|state| state.get().client_no_touch)
}

#[inline]
fn current_request_client_id() -> Option<ClientId> {
    REQUEST_EXECUTION_CONTEXT.with(|state| state.get().client_id)
}

#[inline]
fn current_request_in_transaction() -> bool {
    REQUEST_EXECUTION_CONTEXT.with(|state| state.get().in_transaction)
}

#[inline]
fn current_request_selected_db() -> DbName {
    REQUEST_EXECUTION_CONTEXT.with(|state| state.get().selected_db)
}

#[inline]
fn current_request_tracking_reads_enabled() -> bool {
    REQUEST_EXECUTION_CONTEXT.with(|state| state.get().tracking_reads_enabled)
}

#[inline]
fn begin_tracking_invalidation_collection() {
    TRACKING_INVALIDATION_STACK.with(|stack| stack.borrow_mut().push(Vec::new()));
}

#[inline]
fn finish_tracking_invalidation_collection() -> Vec<RedisKey> {
    TRACKING_INVALIDATION_STACK.with(|stack| stack.borrow_mut().pop().unwrap_or_default())
}

#[inline]
fn finish_tracking_invalidation_collection_into_parent() -> Vec<RedisKey> {
    TRACKING_INVALIDATION_STACK.with(|stack| {
        let mut stack = stack.borrow_mut();
        let Some(current) = stack.pop() else {
            return Vec::new();
        };
        let Some(parent) = stack.last_mut() else {
            return current;
        };
        for key in current {
            if parent
                .iter()
                .any(|existing| existing.as_slice() == key.as_slice())
            {
                continue;
            }
            parent.push(key);
        }
        Vec::new()
    })
}

#[inline]
fn collect_tracking_invalidation_key(key: &[u8]) -> bool {
    TRACKING_INVALIDATION_STACK.with(|stack| {
        let mut stack = stack.borrow_mut();
        let Some(current) = stack.last_mut() else {
            return false;
        };
        if current.iter().any(|existing| existing.as_slice() == key) {
            return true;
        }
        current.push(RedisKey::from(key));
        true
    })
}

#[inline]
fn begin_tracking_read_collection() {
    TRACKING_READ_STACK.with(|stack| stack.borrow_mut().push(Vec::new()));
}

#[inline]
fn finish_tracking_read_collection() -> Vec<RedisKey> {
    TRACKING_READ_STACK.with(|stack| stack.borrow_mut().pop().unwrap_or_default())
}

#[inline]
fn finish_tracking_read_collection_into_parent() -> Vec<RedisKey> {
    TRACKING_READ_STACK.with(|stack| {
        let mut stack = stack.borrow_mut();
        let Some(current) = stack.pop() else {
            return Vec::new();
        };
        let Some(parent) = stack.last_mut() else {
            return current;
        };
        for key in current {
            if parent
                .iter()
                .any(|existing| existing.as_slice() == key.as_slice())
            {
                continue;
            }
            parent.push(key);
        }
        Vec::new()
    })
}

#[inline]
fn collect_tracking_read_key(key: &[u8]) -> bool {
    TRACKING_READ_STACK.with(|stack| {
        let mut stack = stack.borrow_mut();
        let Some(current) = stack.last_mut() else {
            return false;
        };
        if current.iter().any(|existing| existing.as_slice() == key) {
            return true;
        }
        current.push(RedisKey::from(key));
        true
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub(crate) struct WatchVersion(u64);

impl WatchVersion {
    pub(crate) const fn new(value: u64) -> Self {
        Self(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WatchedKey {
    pub(crate) db: DbName,
    pub(crate) key: RedisKey,
    pub(crate) version: WatchVersion,
}

impl WatchedKey {
    pub(crate) const fn new(db: DbName, key: RedisKey, version: WatchVersion) -> Self {
        Self { db, key, version }
    }
}

fn default_config_overrides() -> HashMap<Vec<u8>, Vec<u8>> {
    let mut values = HashMap::new();
    values.insert(b"appendonly".to_vec(), b"no".to_vec());
    values.insert(b"save".to_vec(), b"".to_vec());
    values.insert(b"dbfilename".to_vec(), b"dump.rdb".to_vec());
    values.insert(b"dir".to_vec(), default_config_dir_value());
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
    values.insert(b"lazyfree-lazy-expire".to_vec(), b"no".to_vec());
    values.insert(b"lazyfree-lazy-eviction".to_vec(), b"no".to_vec());
    values.insert(b"lazyfree-lazy-user-del".to_vec(), b"no".to_vec());
    values.insert(b"port".to_vec(), b"6379".to_vec());
    values.insert(b"notify-keyspace-events".to_vec(), b"".to_vec());
    values.insert(b"maxmemory-policy".to_vec(), b"noeviction".to_vec());
    values.insert(b"hz".to_vec(), b"10".to_vec());
    values.insert(b"dynamic-hz".to_vec(), b"yes".to_vec());
    values.insert(b"timeout".to_vec(), b"0".to_vec());
    values.insert(b"tcp-keepalive".to_vec(), b"300".to_vec());
    values.insert(b"loglevel".to_vec(), b"notice".to_vec());
    values.insert(b"logfile".to_vec(), b"".to_vec());
    values.insert(b"databases".to_vec(), b"16".to_vec());
    values.insert(b"lua-time-limit".to_vec(), b"5000".to_vec());
    values.insert(b"slowlog-log-slower-than".to_vec(), b"10000".to_vec());
    values.insert(b"slowlog-max-len".to_vec(), b"128".to_vec());
    values.insert(b"hash-max-listpack-entries".to_vec(), b"128".to_vec());
    values.insert(b"hash-max-listpack-value".to_vec(), b"64".to_vec());
    values.insert(b"set-max-intset-entries".to_vec(), b"512".to_vec());
    values.insert(b"set-max-listpack-entries".to_vec(), b"128".to_vec());
    values.insert(b"set-max-listpack-value".to_vec(), b"64".to_vec());
    values.insert(b"activedefrag".to_vec(), b"no".to_vec());
    values.insert(
        b"proto-max-bulk-len".to_vec(),
        DEFAULT_PROTO_MAX_BULK_LEN.to_string().into_bytes(),
    );
    values.insert(b"appendfilename".to_vec(), b"appendonly.aof".to_vec());
    values.insert(b"appendfsync".to_vec(), b"everysec".to_vec());
    values.insert(b"stop-writes-on-bgsave-error".to_vec(), b"yes".to_vec());
    values.insert(b"rdbcompression".to_vec(), b"yes".to_vec());
    values.insert(b"rdbchecksum".to_vec(), b"yes".to_vec());
    values.insert(b"replica-read-only".to_vec(), b"yes".to_vec());
    values.insert(b"repl-diskless-sync".to_vec(), b"yes".to_vec());
    values.insert(b"repl-diskless-sync-delay".to_vec(), b"5".to_vec());
    values.insert(b"aof-use-rdb-preamble".to_vec(), b"yes".to_vec());
    values.insert(b"list-compress-depth".to_vec(), b"0".to_vec());
    values.insert(b"zset-max-ziplist-value".to_vec(), b"64".to_vec());
    values.insert(b"zset-max-listpack-value".to_vec(), b"64".to_vec());
    values.insert(b"stream-node-max-bytes".to_vec(), b"4096".to_vec());
    values.insert(b"stream-node-max-entries".to_vec(), b"100".to_vec());
    values.insert(b"stream-idmp-duration".to_vec(), b"100".to_vec());
    values.insert(b"stream-idmp-maxsize".to_vec(), b"100".to_vec());
    values.insert(b"maxclients".to_vec(), b"10000".to_vec());
    values.insert(b"repl-backlog-ttl".to_vec(), b"3600".to_vec());
    values.insert(b"cluster-enabled".to_vec(), b"no".to_vec());
    values.insert(b"cluster-node-timeout".to_vec(), b"15000".to_vec());
    values.insert(b"latency-tracking".to_vec(), b"yes".to_vec());
    values.insert(
        b"latency-tracking-info-percentiles".to_vec(),
        b"50 99 99.9".to_vec(),
    );
    values.insert(b"tracking-table-max-keys".to_vec(), b"1000000".to_vec());
    values.insert(b"close-on-oom".to_vec(), b"no".to_vec());
    values.insert(b"close-files-after-invoked-defer".to_vec(), b"no".to_vec());
    values.insert(b"min-replicas-max-lag".to_vec(), b"10".to_vec());
    values.insert(b"min-slaves-max-lag".to_vec(), b"10".to_vec());
    values.insert(b"lfu-log-factor".to_vec(), b"10".to_vec());
    values.insert(b"lfu-decay-time".to_vec(), b"1".to_vec());
    values.insert(b"lazyfree-lazy-user-flush".to_vec(), b"no".to_vec());
    values.insert(b"jemalloc-bg-thread".to_vec(), b"yes".to_vec());
    values.insert(b"activerehashing".to_vec(), b"yes".to_vec());
    values.insert(b"no-appendfsync-on-rewrite".to_vec(), b"no".to_vec());
    values.insert(b"set-proc-title".to_vec(), b"yes".to_vec());
    values.insert(b"repl-min-slaves-to-write".to_vec(), b"0".to_vec());
    values.insert(b"repl-min-slaves-max-lag".to_vec(), b"10".to_vec());
    values
}

fn default_config_dir_value() -> Vec<u8> {
    std::env::current_dir()
        .unwrap_or_else(|_| std::path::PathBuf::from("."))
        .to_string_lossy()
        .into_owned()
        .into_bytes()
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
use self::resp::append_array_length;
use self::resp::append_bulk_array;
use self::resp::append_bulk_string;
use self::resp::append_double;
use self::resp::append_error;
use self::resp::append_integer;
use self::resp::append_map_length;
use self::resp::append_null;
use self::resp::append_null_array;
use self::resp::append_null_bulk_string;
use self::resp::append_push_length;
use self::resp::append_resp3_bignum;
use self::resp::append_resp3_boolean;
use self::resp::append_set_length;
use self::resp::append_simple_string;
use self::resp::append_verbatim_string;
use self::resp::ascii_eq_ignore_case;
use self::resp::format_resp_double;
use self::session_functions::KvSessionFunctions;
use self::session_functions::ObjectSessionFunctions;
use self::value_codec::ContiguousI64RangeSet;
use self::value_codec::DecodedObjectValue;
use self::value_codec::DecodedSetObjectPayload;
use self::value_codec::decode_object_value;
use self::value_codec::decode_set_object_payload;
use self::value_codec::decode_stored_value;
use self::value_codec::deserialize_hash_object_payload;
use self::value_codec::deserialize_list_object_payload;
use self::value_codec::deserialize_set_object_payload;
use self::value_codec::deserialize_stream_object_payload;
use self::value_codec::deserialize_zset_object_payload;
use self::value_codec::encode_object_value;
use self::value_codec::encode_stored_value;
use self::value_codec::materialize_contiguous_i64_range_set;
use self::value_codec::parse_f64_ascii;
use self::value_codec::parse_f64_ascii_allow_non_finite;
use self::value_codec::parse_i64_ascii;
use self::value_codec::parse_u64_ascii;
use self::value_codec::serialize_contiguous_i64_range_set_payload;
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

impl std::ops::Deref for RedisKey {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.0
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

impl From<RedisKey> for ItemKey {
    fn from(value: RedisKey) -> Self {
        Self(value.into_vec())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub(crate) struct DbName(usize);

impl DbName {
    pub(crate) const fn new(raw: usize) -> Self {
        Self(raw)
    }
}

impl From<DbName> for usize {
    fn from(value: DbName) -> Self {
        value.0
    }
}

pub(crate) type KeyBytes = [u8];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct DbKeyRef<'a> {
    db: DbName,
    key: &'a KeyBytes,
}

impl<'a> DbKeyRef<'a> {
    pub(crate) const fn new(db: DbName, key: &'a KeyBytes) -> Self {
        Self { db, key }
    }

    pub(crate) const fn db(self) -> DbName {
        self.db
    }

    pub(crate) const fn key(self) -> &'a KeyBytes {
        self.key
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct DbScopedKey {
    pub(crate) db: DbName,
    pub(crate) key: RedisKey,
}

impl DbScopedKey {
    pub(crate) fn new(db: DbName, key: &[u8]) -> Self {
        Self {
            db,
            key: RedisKey::from(key),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct ShardIndex(usize);

impl ShardIndex {
    pub const fn new(raw: usize) -> Self {
        Self(raw)
    }

    pub const fn as_usize(self) -> usize {
        self.0
    }
}

impl From<ShardIndex> for usize {
    fn from(value: ShardIndex) -> Self {
        value.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct PerShard<T>(Vec<T>);

impl<T> PerShard<T> {
    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }

    pub(crate) fn iter(&self) -> std::slice::Iter<'_, T> {
        self.0.iter()
    }

    pub(crate) fn contains_shard(&self, shard_index: ShardIndex) -> bool {
        shard_index.as_usize() < self.0.len()
    }

    pub(crate) fn indices(&self) -> impl Iterator<Item = ShardIndex> + '_ {
        (0..self.len()).map(ShardIndex::new)
    }
}

impl<T> From<Vec<T>> for PerShard<T> {
    fn from(value: Vec<T>) -> Self {
        Self(value)
    }
}

impl<T> std::ops::Index<ShardIndex> for PerShard<T> {
    type Output = T;

    fn index(&self, index: ShardIndex) -> &Self::Output {
        &self.0[index.as_usize()]
    }
}

impl<T> std::ops::IndexMut<ShardIndex> for PerShard<T> {
    fn index_mut(&mut self, index: ShardIndex) -> &mut Self::Output {
        &mut self.0[index.as_usize()]
    }
}

impl<'a, T> IntoIterator for &'a PerShard<T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl<'a, T> IntoIterator for &'a mut PerShard<T> {
    type Item = &'a mut T;
    type IntoIter = std::slice::IterMut<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter_mut()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(u8)]
pub(crate) enum RespProtocolVersion {
    #[default]
    Resp2 = 2,
    Resp3 = 3,
}

impl RespProtocolVersion {
    pub(crate) const fn from_u8(value: u8) -> Option<Self> {
        match value {
            2 => Some(Self::Resp2),
            3 => Some(Self::Resp3),
            _ => None,
        }
    }

    pub(crate) fn from_u64(value: u64) -> Option<Self> {
        let value = u8::try_from(value).ok()?;
        Self::from_u8(value)
    }

    pub(crate) const fn as_u8(self) -> u8 {
        self as u8
    }

    pub(crate) const fn is_resp3(self) -> bool {
        matches!(self, Self::Resp3)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct StreamId {
    timestamp_millis: u64,
    sequence: u64,
}

impl StreamId {
    pub(crate) const fn new(timestamp_millis: u64, sequence: u64) -> Self {
        Self {
            timestamp_millis,
            sequence,
        }
    }

    pub(crate) const fn zero() -> Self {
        Self::new(0, 0)
    }

    pub(crate) fn parse(id: &[u8]) -> Option<Self> {
        Self::parse_with_missing_sequence(id, 0)
    }

    pub(crate) fn parse_with_missing_sequence(id: &[u8], missing_sequence: u64) -> Option<Self> {
        let text = core::str::from_utf8(id).ok()?;
        if let Some((ms, seq)) = text.split_once('-') {
            return Some(Self::new(ms.parse::<u64>().ok()?, seq.parse::<u64>().ok()?));
        }
        Some(Self::new(text.parse::<u64>().ok()?, missing_sequence))
    }

    pub(crate) fn encode(self) -> Vec<u8> {
        format!("{}-{}", self.timestamp_millis, self.sequence).into_bytes()
    }

    pub(crate) const fn timestamp_millis(self) -> u64 {
        self.timestamp_millis
    }

    pub(crate) const fn sequence(self) -> u64 {
        self.sequence
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct NormalizedRange {
    pub(crate) start: usize,
    pub(crate) end_inclusive: usize,
}

impl NormalizedRange {
    pub(crate) const fn new(start: usize, end_inclusive: usize) -> Self {
        Self {
            start,
            end_inclusive,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ObjectTypeTag {
    List = 2,
    Hash = 3,
    Set = 4,
    Zset = 5,
    Stream = 6,
}

impl ObjectTypeTag {
    pub(crate) fn from_u8(value: u8) -> Option<Self> {
        match value {
            2 => Some(Self::List),
            3 => Some(Self::Hash),
            4 => Some(Self::Set),
            5 => Some(Self::Zset),
            6 => Some(Self::Stream),
            _ => None,
        }
    }

    pub(crate) const fn as_u8(self) -> u8 {
        self as u8
    }

    pub(crate) fn write_to(self, target: &mut Vec<u8>) {
        target.push(self.as_u8());
    }

    pub(crate) const fn name(self) -> &'static [u8] {
        match self {
            Self::Hash => b"hash",
            Self::List => b"list",
            Self::Set => b"set",
            Self::Zset => b"zset",
            Self::Stream => b"stream",
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct ExpirationMetadata {
    deadline: Instant,
    unix_millis: TimestampMillis,
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
    Object {
        object_type: ObjectTypeTag,
        payload: Vec<u8>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationEntry {
    pub key: ItemKey,
    pub value: MigrationValue,
    pub expiration_unix_millis: Option<u64>,
}

#[derive(Debug, Clone, Default)]
struct AuxiliaryDbValue {
    value: Option<MigrationValue>,
    expiration: Option<ExpirationMetadata>,
    hash_field_expirations: HashMap<HashField, ExpirationMetadata>,
}

#[derive(Debug, Default)]
struct AuxiliaryDbState {
    entries: HashMap<RedisKey, AuxiliaryDbValue>,
}

const DEFAULT_STREAM_IDMP_DURATION_SECONDS: u64 = 100;
const DEFAULT_STREAM_IDMP_MAXSIZE: u64 = 100;

#[derive(Debug, Clone, PartialEq, Eq)]
struct StreamObject {
    #[allow(clippy::type_complexity)]
    entries: BTreeMap<StreamId, Vec<(Vec<u8>, Vec<u8>)>>,
    node_sizes: VecDeque<usize>,
    groups: BTreeMap<Vec<u8>, StreamGroupState>,
    last_generated_id: StreamId,
    max_deleted_entry_id: StreamId,
    entries_added: u64,
    idmp_duration_seconds: u64,
    idmp_maxsize: u64,
    idmp_producers: BTreeMap<Vec<u8>, StreamIdmpProducerState>,
    iids_added: u64,
    iids_duplicates: u64,
}

impl StreamObject {
    fn with_idmp_config(idmp_duration_seconds: u64, idmp_maxsize: u64) -> Self {
        Self {
            entries: BTreeMap::new(),
            node_sizes: VecDeque::new(),
            groups: BTreeMap::new(),
            last_generated_id: StreamId::zero(),
            max_deleted_entry_id: StreamId::zero(),
            entries_added: 0,
            idmp_duration_seconds,
            idmp_maxsize,
            idmp_producers: BTreeMap::new(),
            iids_added: 0,
            iids_duplicates: 0,
        }
    }

    fn normalized_node_max_entries(configured_max_entries: usize) -> usize {
        configured_max_entries.max(1)
    }

    fn rebuild_node_sizes(&mut self, configured_max_entries: usize) {
        self.node_sizes.clear();
        let node_max_entries = Self::normalized_node_max_entries(configured_max_entries);
        let mut remaining = self.entries.len();
        while remaining > 0 {
            let node_len = remaining.min(node_max_entries);
            self.node_sizes.push_back(node_len);
            remaining -= node_len;
        }
    }

    fn ensure_node_sizes(&mut self, configured_max_entries: usize) {
        let total_entries: usize = self.node_sizes.iter().sum();
        if total_entries == self.entries.len() {
            return;
        }
        self.rebuild_node_sizes(configured_max_entries);
    }

    fn note_appended_entry(&mut self, configured_max_entries: usize) {
        let node_max_entries = Self::normalized_node_max_entries(configured_max_entries);
        if let Some(tail_size) = self.node_sizes.back_mut()
            && *tail_size < node_max_entries
        {
            *tail_size += 1;
            return;
        }
        self.node_sizes.push_back(1);
    }

    fn remove_entry_at_rank(&mut self, entry_rank: usize) {
        let mut consumed = 0usize;
        let mut node_index = 0usize;
        while node_index < self.node_sizes.len() {
            let node_size = self.node_sizes[node_index];
            if entry_rank < consumed + node_size {
                if node_size == 1 {
                    let _ = self.node_sizes.remove(node_index);
                } else {
                    self.node_sizes[node_index] = node_size - 1;
                }
                return;
            }
            consumed += node_size;
            node_index += 1;
        }
    }

    fn idmp_tracked_count(&self) -> usize {
        self.idmp_producers
            .values()
            .map(|producer| producer.entries.len())
            .sum()
    }

    fn expire_idmp_entries(&mut self, now_millis: u64) {
        if self.idmp_duration_seconds == 0 || self.idmp_producers.is_empty() {
            return;
        }
        let expire_before =
            now_millis.saturating_sub(self.idmp_duration_seconds.saturating_mul(1000));
        let producer_ids = self.idmp_producers.keys().cloned().collect::<Vec<_>>();
        for producer_id in producer_ids {
            let mut remove_producer = false;
            if let Some(producer) = self.idmp_producers.get_mut(&producer_id) {
                while let Some(oldest_iid) = producer.insertion_order.front().cloned() {
                    let Some(oldest_id) = producer.entries.get(&oldest_iid).copied() else {
                        let _ = producer.insertion_order.pop_front();
                        continue;
                    };
                    if oldest_id.timestamp_millis() > expire_before {
                        break;
                    }
                    let _ = producer.insertion_order.pop_front();
                    producer.entries.remove(&oldest_iid);
                }
                remove_producer = producer.entries.is_empty();
            }
            if remove_producer {
                self.idmp_producers.remove(&producer_id);
            }
        }
    }

    fn idmp_lookup(&mut self, producer_id: &[u8], iid: &[u8], now_millis: u64) -> Option<StreamId> {
        self.expire_idmp_entries(now_millis);
        self.idmp_producers
            .get(producer_id)
            .and_then(|producer| producer.entries.get(iid).copied())
    }

    fn remember_idmp_entry(&mut self, producer_id: Vec<u8>, iid: Vec<u8>, stream_id: StreamId) {
        let remove_producer = {
            let producer = self.idmp_producers.entry(producer_id).or_default();
            producer.insertion_order.push_back(iid.clone());
            producer.entries.insert(iid, stream_id);
            self.iids_added = self.iids_added.saturating_add(1);

            while u64::try_from(producer.entries.len()).unwrap_or(u64::MAX) > self.idmp_maxsize {
                let Some(oldest_iid) = producer.insertion_order.pop_front() else {
                    break;
                };
                producer.entries.remove(&oldest_iid);
            }
            producer.entries.is_empty()
        };
        if remove_producer {
            self.idmp_producers
                .retain(|_, producer| !producer.entries.is_empty());
        }
    }

    fn clear_idmp_history(&mut self) {
        self.idmp_producers.clear();
    }
}

impl Default for StreamObject {
    fn default() -> Self {
        Self::with_idmp_config(
            DEFAULT_STREAM_IDMP_DURATION_SECONDS,
            DEFAULT_STREAM_IDMP_MAXSIZE,
        )
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct StreamIdmpProducerState {
    entries: BTreeMap<Vec<u8>, StreamId>,
    insertion_order: VecDeque<Vec<u8>>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct StreamGroupState {
    last_delivered_id: StreamId,
    entries_read: Option<u64>,
    consumers: BTreeMap<Vec<u8>, StreamConsumerState>,
    pending: BTreeMap<StreamId, StreamPendingEntry>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct StreamConsumerState {
    pending: BTreeSet<StreamId>,
    seen_time_millis: u64,
    active_time_millis: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StreamPendingEntry {
    consumer: Vec<u8>,
    last_delivery_time_millis: u64,
    delivery_count: u64,
}

fn stream_has_entry_after_id(stream: &StreamObject, pivot: StreamId) -> bool {
    stream
        .entries
        .range((std::ops::Bound::Excluded(pivot), std::ops::Bound::Unbounded))
        .next()
        .is_some()
}

fn stream_pending_entry_idle_millis(pending_entry: &StreamPendingEntry, now_millis: u64) -> u64 {
    now_millis.saturating_sub(pending_entry.last_delivery_time_millis)
}

fn stream_group_has_claimable_pending_entry(
    group_state: &StreamGroupState,
    now_millis: u64,
    min_idle_millis: u64,
) -> bool {
    group_state.pending.values().any(|pending_entry| {
        stream_pending_entry_idle_millis(pending_entry, now_millis) >= min_idle_millis
    })
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
    frozen_time: ScriptTimeSnapshot,
    thread_id: std::thread::ThreadId,
}

#[derive(Clone, Debug)]
pub(crate) struct ScriptReplicationEffect {
    pub(crate) selected_db: DbName,
    pub(crate) command: CommandId,
    pub(crate) frame: Vec<u8>,
    pub(crate) response: Vec<u8>,
}

#[derive(Debug, Default)]
struct FunctionRegistry {
    functions: HashMap<String, LoadedFunctionDescriptor>,
    library_sources: HashMap<String, Vec<u8>>,
    library_function_names: HashMap<String, Vec<String>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LatencySample {
    pub(crate) unix_seconds: u64,
    pub(crate) latency_millis: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LatencyRange {
    pub(crate) max_millis: u64,
    pub(crate) min_millis: u64,
}

impl LatencyRange {
    pub(crate) const fn new(max_millis: u64, min_millis: u64) -> Self {
        Self {
            max_millis,
            min_millis,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LatencyEvent {
    pub(crate) event_name: Vec<u8>,
    pub(crate) latest_sample: LatencySample,
    pub(crate) max_latency_millis: u64,
}

#[derive(Debug, Default)]
struct LatencyEventState {
    samples: VecDeque<LatencySample>,
    max_latency_millis: u64,
}

#[derive(Debug, Default)]
struct PubSubState {
    channel_subscribers: HashMap<Vec<u8>, HashSet<ClientId>>,
    pattern_subscribers: HashMap<Vec<u8>, HashSet<ClientId>>,
    client_channels: HashMap<ClientId, HashSet<Vec<u8>>>,
    client_patterns: HashMap<ClientId, HashSet<Vec<u8>>>,
    pending_messages: HashMap<ClientId, VecDeque<Vec<u8>>>,
    client_resp_versions: HashMap<ClientId, RespProtocolVersion>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClientTrackingModeSetting {
    Off,
    On,
    OptIn,
    OptOut,
}

#[derive(Debug, Clone)]
pub(crate) struct ClientTrackingConfig {
    pub(crate) mode: ClientTrackingModeSetting,
    pub(crate) redirect_id: Option<ClientId>,
    pub(crate) bcast: bool,
    pub(crate) noloop: bool,
    pub(crate) prefixes: Vec<Vec<u8>>,
    pub(crate) caching: Option<bool>,
}

impl Default for ClientTrackingConfig {
    fn default() -> Self {
        Self {
            mode: ClientTrackingModeSetting::Off,
            redirect_id: None,
            bcast: false,
            noloop: false,
            prefixes: Vec::new(),
            caching: None,
        }
    }
}

impl ClientTrackingConfig {
    fn should_track_reads(&self) -> bool {
        match self.mode {
            ClientTrackingModeSetting::Off => false,
            ClientTrackingModeSetting::On => true,
            ClientTrackingModeSetting::OptIn => self.caching == Some(true),
            ClientTrackingModeSetting::OptOut => self.caching != Some(false),
        }
    }
}

#[derive(Debug, Clone, Default)]
struct TrackingClientState {
    config: ClientTrackingConfig,
    tracked_keys: HashSet<RedisKey>,
    broken_redirect: bool,
    broken_redirect_notified: bool,
}

#[derive(Debug, Default)]
struct TrackingState {
    clients: HashMap<ClientId, TrackingClientState>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PubSubTargetKind {
    Channel,
    Pattern,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClientUnblockMode {
    Timeout,
    Error,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum SetEncodingFloor {
    Listpack,
    Hashtable,
}

#[derive(Debug, Clone)]
struct SetObjectHotEntry {
    payload: DecodedSetObjectPayload,
    ordered_members: Option<Vec<Vec<u8>>>,
    dirty: bool,
}

impl SetObjectHotEntry {
    fn new(payload: DecodedSetObjectPayload, dirty: bool) -> Self {
        Self {
            payload,
            ordered_members: None,
            dirty,
        }
    }

    fn ordered_members(&mut self) -> &[Vec<u8>] {
        self.ordered_members
            .get_or_insert_with(|| self.payload.ordered_members())
            .as_slice()
    }

    fn invalidate_ordered_members(&mut self) {
        self.ordered_members = None;
    }
}

#[derive(Debug, Default)]
struct SetObjectHotState {
    entries: HashMap<DbScopedKey, SetObjectHotEntry>,
    lru: VecDeque<DbScopedKey>,
}

impl SetObjectHotState {
    fn touch(&mut self, key: &DbScopedKey) {
        if let Some(index) = self.lru.iter().position(|existing| existing == key) {
            let _ = self.lru.remove(index);
        }
        self.lru.push_back(key.clone());
    }
}

#[derive(Debug, Clone, Copy)]
struct SetDebugHtState {
    main_table_size: usize,
    rehash_target_size: Option<usize>,
    rehash_steps_remaining: usize,
    pending_shrink_target_size: Option<usize>,
}

#[derive(Debug, Clone, Copy)]
struct SetDebugHtStatsSnapshot {
    main_table_size: usize,
    rehash_target_size: Option<usize>,
}

impl SetDebugHtState {
    fn new(member_count: usize) -> Self {
        Self {
            main_table_size: Self::stable_table_size(member_count),
            rehash_target_size: None,
            rehash_steps_remaining: 0,
            pending_shrink_target_size: None,
        }
    }

    fn stable_table_size(member_count: usize) -> usize {
        member_count
            .saturating_mul(2)
            .max(SET_DEBUG_HT_MIN_TABLE_SIZE)
            .next_power_of_two()
    }

    fn initial_rehash_steps(main_table_size: usize, target_size: usize) -> usize {
        let shrink_ratio = (main_table_size / target_size.max(1)).max(1);
        (shrink_ratio.ilog2() as usize + 1).clamp(2, 8)
    }

    fn start_rehash(&mut self, target_size: usize) {
        if target_size >= self.main_table_size {
            self.main_table_size = target_size;
            self.rehash_target_size = None;
            self.rehash_steps_remaining = 0;
            self.pending_shrink_target_size = None;
            return;
        }
        self.rehash_target_size = Some(target_size);
        self.rehash_steps_remaining = Self::initial_rehash_steps(self.main_table_size, target_size);
        self.pending_shrink_target_size = None;
    }

    fn finish_rehash(&mut self, target_size: usize) {
        self.main_table_size = target_size;
        self.rehash_target_size = None;
        self.rehash_steps_remaining = 0;
        self.pending_shrink_target_size = None;
    }

    fn update_for_member_count(&mut self, member_count: usize, bgsave_in_progress: bool) {
        let desired_size = Self::stable_table_size(member_count);

        if let Some(current_target_size) = self.rehash_target_size {
            if desired_size >= self.main_table_size {
                self.finish_rehash(desired_size);
                return;
            }

            if desired_size < current_target_size {
                self.rehash_target_size = Some(desired_size);
                let next_steps = Self::initial_rehash_steps(self.main_table_size, desired_size);
                self.rehash_steps_remaining = if self.rehash_steps_remaining == 0 {
                    next_steps
                } else {
                    self.rehash_steps_remaining.min(next_steps)
                };
            }
            self.pending_shrink_target_size = None;
            return;
        }

        if desired_size > self.main_table_size {
            self.main_table_size = desired_size;
            self.pending_shrink_target_size = None;
            return;
        }

        if desired_size == self.main_table_size {
            self.pending_shrink_target_size = None;
            return;
        }

        if bgsave_in_progress {
            self.pending_shrink_target_size = Some(desired_size);
            return;
        }

        self.start_rehash(desired_size);
    }

    fn advance_rehash(&mut self, _member_count: usize, bgsave_in_progress: bool) {
        if self.rehash_target_size.is_none()
            && let Some(target_size) = self.pending_shrink_target_size
            && !bgsave_in_progress
        {
            self.start_rehash(target_size);
        }

        let Some(target_size) = self.rehash_target_size else {
            return;
        };

        if self.rehash_steps_remaining > 0 {
            self.rehash_steps_remaining -= 1;
        }
        if self.rehash_steps_remaining == 0 {
            self.finish_rehash(target_size);
        }
    }

    fn snapshot(self) -> SetDebugHtStatsSnapshot {
        SetDebugHtStatsSnapshot {
            main_table_size: self.main_table_size,
            rehash_target_size: self.rehash_target_size,
        }
    }
}

impl DecodedSetObjectPayload {
    fn member_count(&self) -> usize {
        match self {
            Self::Members(set) => set.len(),
            Self::ContiguousI64Range(range) => {
                let span = i128::from(range.end()) - i128::from(range.start()) + 1;
                usize::try_from(span).unwrap_or(usize::MAX)
            }
        }
    }

    fn is_empty(&self) -> bool {
        self.member_count() == 0
    }

    fn ordered_members(&self) -> Vec<Vec<u8>> {
        match self {
            Self::Members(set) => {
                let mut members = set.iter().cloned().collect::<Vec<_>>();
                members.sort_by(|left, right| compare_ordered_set_members(left, right));
                members
            }
            Self::ContiguousI64Range(range) => materialize_contiguous_i64_range_set(*range)
                .into_iter()
                .collect(),
        }
    }

    fn contains_member(&self, member: &[u8]) -> bool {
        match self {
            Self::Members(set) => set.contains(member),
            Self::ContiguousI64Range(range) => {
                parse_canonical_i64_set_member(member).is_some_and(|value| range.contains(value))
            }
        }
    }

    fn insert_member(&mut self, member: &[u8]) -> bool {
        match self {
            Self::Members(set) => set.insert(member.to_vec()),
            Self::ContiguousI64Range(range) => {
                let Some(value) = parse_canonical_i64_set_member(member) else {
                    let mut set = materialize_contiguous_i64_range_set(*range);
                    let inserted = set.insert(member.to_vec());
                    *self = Self::Members(set);
                    return inserted;
                };
                if range.contains(value) {
                    return false;
                }
                if range.can_extend_left_with(value) {
                    range.extend_left();
                    return true;
                }
                if range.can_extend_right_with(value) {
                    range.extend_right();
                    return true;
                }
                let mut set = materialize_contiguous_i64_range_set(*range);
                let inserted = set.insert(member.to_vec());
                *self = Self::Members(set);
                inserted
            }
        }
    }

    fn remove_member(&mut self, member: &[u8]) -> bool {
        match self {
            Self::Members(set) => set.remove(member),
            Self::ContiguousI64Range(range) => {
                let Some(value) = parse_canonical_i64_set_member(member) else {
                    return false;
                };
                if !range.contains(value) {
                    return false;
                }
                if range.start() == range.end() {
                    *self = Self::Members(BTreeSet::new());
                    return true;
                }
                if value == range.start() {
                    *range = ContiguousI64RangeSet::new(range.start() + 1, range.end()).unwrap();
                    return true;
                }
                if value == range.end() {
                    *range = ContiguousI64RangeSet::new(range.start(), range.end() - 1).unwrap();
                    return true;
                }
                let mut set = materialize_contiguous_i64_range_set(*range);
                let removed = set.remove(member);
                *self = Self::Members(set);
                removed
            }
        }
    }
}

fn compare_ordered_set_members(left: &[u8], right: &[u8]) -> std::cmp::Ordering {
    let left_integer = parse_canonical_i64_set_member(left);
    let right_integer = parse_canonical_i64_set_member(right);
    match (left_integer, right_integer) {
        (Some(left_value), Some(right_value)) => left_value.cmp(&right_value),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => left.cmp(right),
    }
}

fn parse_canonical_i64_set_member(member: &[u8]) -> Option<i64> {
    let value = parse_i64_ascii(member)?;
    if value.to_string().as_bytes() != member {
        return None;
    }
    Some(value)
}

const SLOWLOG_ENTRY_MAX_ARGC: usize = 32;
const SLOWLOG_ENTRY_MAX_STRING: usize = 128;

#[derive(Debug, Clone)]
pub(crate) struct SlowlogEntry {
    pub(crate) id: u64,
    pub(crate) unix_time_seconds: i64,
    pub(crate) duration_micros: i64,
    pub(crate) args: Vec<Vec<u8>>,
    pub(crate) peer_id: Vec<u8>,
    pub(crate) client_name: Vec<u8>,
}

fn should_skip_slowlog_for_command(command: CommandId, args: &[&[u8]]) -> bool {
    if command == CommandId::Exec {
        return true;
    }
    if command == CommandId::Slowlog {
        return args
            .get(1)
            .is_none_or(|subcommand| !subcommand.eq_ignore_ascii_case(b"RESET"));
    }
    false
}

fn is_slowlog_blocking_command(command: CommandId) -> bool {
    matches!(
        command,
        CommandId::Blpop
            | CommandId::Brpop
            | CommandId::Brpoplpush
            | CommandId::Blmove
            | CommandId::Bzpopmin
            | CommandId::Bzpopmax
            | CommandId::Bzmpop
            | CommandId::Blmpop
            | CommandId::Xread
            | CommandId::Xreadgroup
    )
}

fn slowlog_arguments_for_command(args: &[&[u8]]) -> Vec<Vec<u8>> {
    let redacted = redact_slowlog_arguments(args);
    let mut trimmed = Vec::with_capacity(redacted.len().min(SLOWLOG_ENTRY_MAX_ARGC));
    let max_args = redacted.len().min(SLOWLOG_ENTRY_MAX_ARGC);
    for index in 0..max_args {
        if redacted.len() > SLOWLOG_ENTRY_MAX_ARGC && index + 1 == SLOWLOG_ENTRY_MAX_ARGC {
            trimmed.push(
                format!(
                    "... ({} more arguments)",
                    redacted.len() - SLOWLOG_ENTRY_MAX_ARGC + 1
                )
                .into_bytes(),
            );
            break;
        }
        trimmed.push(trim_slowlog_argument(&redacted[index]));
    }
    trimmed
}

fn redact_slowlog_arguments(args: &[&[u8]]) -> Vec<Vec<u8>> {
    let normalized = args
        .iter()
        .map(|arg| {
            arg.iter()
                .map(|byte| byte.to_ascii_lowercase())
                .collect::<Vec<u8>>()
        })
        .collect::<Vec<_>>();

    if normalized.first().is_some_and(|command| command == b"acl")
        && let Some(subcommand) = normalized.get(1)
    {
        if subcommand == b"setuser" {
            return vec![
                b"acl".to_vec(),
                b"setuser".to_vec(),
                b"(redacted)".to_vec(),
                b"(redacted)".to_vec(),
                b"(redacted)".to_vec(),
            ];
        }
        if subcommand == b"getuser" {
            return vec![b"acl".to_vec(), b"getuser".to_vec(), b"(redacted)".to_vec()];
        }
        if subcommand == b"deluser" {
            let mut redacted = vec![b"acl".to_vec(), b"deluser".to_vec()];
            for _ in args.iter().skip(2) {
                redacted.push(b"(redacted)".to_vec());
            }
            return redacted;
        }
    }

    if normalized
        .first()
        .is_some_and(|command| command == b"config")
        && normalized
            .get(1)
            .is_some_and(|subcommand| subcommand == b"set")
        && normalized.len() >= 4
    {
        let mut redacted = args.iter().map(|arg| (*arg).to_vec()).collect::<Vec<_>>();
        if matches!(
            normalized[2].as_slice(),
            b"masteruser"
                | b"masterauth"
                | b"requirepass"
                | b"tls-key-file-pass"
                | b"tls-client-key-file-pass"
        ) {
            redacted[3] = b"(redacted)".to_vec();
        }
        return redacted;
    }

    if normalized
        .first()
        .is_some_and(|command| command == b"migrate")
    {
        let mut redacted = args.iter().map(|arg| (*arg).to_vec()).collect::<Vec<_>>();
        let mut index = 6usize;
        while index < normalized.len() {
            if normalized[index] == b"auth" && index + 1 < redacted.len() {
                redacted[index + 1] = b"(redacted)".to_vec();
                index += 2;
                continue;
            }
            if normalized[index] == b"auth2" && index + 2 < redacted.len() {
                redacted[index + 1] = b"(redacted)".to_vec();
                redacted[index + 2] = b"(redacted)".to_vec();
                index += 3;
                continue;
            }
            index += 1;
        }
        return redacted;
    }

    args.iter().map(|arg| (*arg).to_vec()).collect()
}

fn trim_slowlog_argument(argument: &[u8]) -> Vec<u8> {
    if argument.len() <= SLOWLOG_ENTRY_MAX_STRING {
        return argument.to_vec();
    }
    let mut trimmed = argument[..SLOWLOG_ENTRY_MAX_STRING].to_vec();
    trimmed.extend_from_slice(
        format!(
            "... ({} more bytes)",
            argument.len() - SLOWLOG_ENTRY_MAX_STRING
        )
        .as_bytes(),
    );
    trimmed
}

pub struct RequestProcessor {
    string_stores: PerShard<OrderedMutex<TsavoriteKV<Vec<u8>, Vec<u8>>>>,
    object_stores: PerShard<OrderedMutex<TsavoriteKV<Vec<u8>, Vec<u8>>>>,
    string_expirations: PerShard<OrderedMutex<HashMap<RedisKey, ExpirationMetadata>>>,
    hash_field_expirations:
        PerShard<OrderedMutex<HashMap<RedisKey, HashMap<HashField, ExpirationMetadata>>>>,
    string_expiration_counts: PerShard<AtomicUsize>,
    string_key_registries: PerShard<OrderedMutex<HashSet<RedisKey>>>,
    object_key_registries: PerShard<OrderedMutex<HashSet<RedisKey>>>,
    watch_versions: Vec<AtomicU64>,
    blocking_wait_queues: Mutex<HashMap<BlockingWaitKey, VecDeque<ClientId>>>,
    blocked_stream_wait_states: Mutex<HashMap<ClientId, BlockingStreamWaitState>>,
    blocked_xread_tail_ids: Mutex<HashMap<ClientId, Vec<Option<StreamId>>>>,
    pending_client_unblocks: Mutex<HashMap<ClientId, ClientUnblockMode>>,
    pause_blocked_blocking_clients: Mutex<HashSet<ClientId>>,
    forced_list_quicklist_keys: Mutex<HashSet<RedisKey>>,
    forced_raw_string_keys: Mutex<HashSet<RedisKey>>,
    forced_set_encoding_floors: Mutex<HashMap<DbScopedKey, SetEncodingFloor>>,
    set_object_hot_state: Mutex<SetObjectHotState>,
    set_debug_ht_state: Mutex<HashMap<RedisKey, SetDebugHtState>>,
    random_state: AtomicU64,
    active_expire_enabled: AtomicBool,
    debug_pause_cron: AtomicBool,
    debug_disable_deny_scripts: AtomicBool,
    expired_keys: AtomicU64,
    expired_keys_active: AtomicU64,
    total_error_replies: AtomicU64,
    lastsave_unix_seconds: AtomicU64,
    rdb_changes_since_last_save: AtomicU64,
    saved_rdb_snapshot: Mutex<Option<Vec<u8>>>,
    resp_protocol_version: AtomicU8,
    interop_force_resp3_zset_pairs: bool,
    blocked_clients: AtomicU64,
    connected_clients: AtomicU64,
    watching_clients: AtomicU64,
    executed_command_count: AtomicU64,
    rdb_key_save_delay_micros: AtomicU64,
    rdb_bgsave_deadline_unix_millis: AtomicU64,
    rdb_bgsave_child: Mutex<Option<Child>>,
    command_calls: Mutex<HashMap<Vec<u8>, u64>>,
    command_rejected_calls: Mutex<HashMap<Vec<u8>, u64>>,
    command_failed_calls: Mutex<HashMap<Vec<u8>, u64>>,
    error_reply_counts: Mutex<HashMap<Vec<u8>, u64>>,
    latency_events: Mutex<HashMap<Vec<u8>, LatencyEventState>>,
    slowlog_entries: Mutex<VecDeque<SlowlogEntry>>,
    slowlog_next_id: AtomicU64,
    pubsub_state: Mutex<PubSubState>,
    tracking_state: Mutex<TrackingState>,
    tracking_table_max_keys: AtomicU64,
    auxiliary_databases: Mutex<HashMap<DbName, AuxiliaryDbState>>,
    key_lru_access_millis: Mutex<HashMap<RedisKey, u64>>,
    key_lfu_frequency: Mutex<HashMap<RedisKey, u8>>,
    config_overrides: Mutex<HashMap<Vec<u8>, Vec<u8>>>,
    lazy_expired_keys_for_replication: Mutex<Vec<DbScopedKey>>,
    script_replication_effects: Mutex<Vec<ScriptReplicationEffect>>,
    script_cache: Mutex<HashMap<String, Vec<u8>>>,
    script_cache_insertion_order: Mutex<VecDeque<String>>,
    script_cache_hits: AtomicU64,
    script_cache_misses: AtomicU64,
    script_cache_evictions: AtomicU64,
    script_runtime_timeouts: AtomicU64,
    used_memory_vm_functions: AtomicU64,
    lazyfree_pending_objects: AtomicU64,
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
    notify_keyspace_events_flags: AtomicU32,
    zset_max_listpack_entries: AtomicUsize,
    list_max_listpack_size: AtomicI64,
    /// DEBUG QUICKLIST-PACKED-THRESHOLD value.  When non-zero, any individual
    /// list element whose byte length meets or exceeds this threshold forces
    /// quicklist encoding instead of listpack.
    quicklist_packed_threshold: AtomicUsize,
    hash_max_listpack_entries: AtomicUsize,
    hash_max_listpack_value: AtomicUsize,
    set_max_listpack_entries: AtomicUsize,
    set_max_listpack_value: AtomicUsize,
    set_max_intset_entries: AtomicUsize,
    allow_access_expired: AtomicBool,
    functions: KvSessionFunctions,
    object_functions: ObjectSessionFunctions,
    /// CLIENT PAUSE end time as milliseconds since UNIX epoch. 0 means not paused.
    client_pause_end_millis: AtomicU64,
    /// CLIENT PAUSE type: 0 = none, 1 = WRITE, 2 = ALL.
    client_pause_type: AtomicU8,
    /// CLIENT SETNAME connection name (empty = no name set).
    client_name: Mutex<Vec<u8>>,
    /// CLIENT REPLY mode: 0 = ON (default), 1 = OFF, 2 = SKIP (one-shot).
    client_reply_mode: AtomicU8,
    /// CLIENT SETINFO LIB-NAME value (empty = not set).
    client_lib_name: Mutex<Vec<u8>>,
    /// CLIENT SETINFO LIB-VER value (empty = not set).
    client_lib_ver: Mutex<Vec<u8>>,
    server_metrics: OnceLock<Arc<crate::ServerMetrics>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum BlockingWaitClass {
    List,
    Zset,
    Stream,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct BlockingWaitKey {
    db: DbName,
    key: RedisKey,
    class: BlockingWaitClass,
}

impl BlockingWaitKey {
    pub(crate) fn new(db: DbName, key: RedisKey, class: BlockingWaitClass) -> Self {
        Self { db, key, class }
    }

    pub(crate) fn db(&self) -> DbName {
        self.db
    }

    pub(crate) fn key(&self) -> &RedisKey {
        &self.key
    }

    pub(crate) fn class(&self) -> BlockingWaitClass {
        self.class
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BlockingStreamWaitState {
    Xread(Vec<BlockingXreadStreamWait>),
    Xreadgroup(BlockingXreadgroupWait),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BlockingXreadStreamWait {
    pub(crate) key: RedisKey,
    pub(crate) selection: BlockingXreadWaitSelection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BlockingXreadWaitSelection {
    After(StreamId),
    LastEntry,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BlockingXreadgroupWait {
    pub(crate) stream_keys: Vec<RedisKey>,
    pub(crate) group: Vec<u8>,
    pub(crate) claim_min_idle_millis: Option<u64>,
}

const CLIENT_PAUSE_TYPE_NONE: u8 = 0;
const CLIENT_PAUSE_TYPE_WRITE: u8 = 1;
const CLIENT_PAUSE_TYPE_ALL: u8 = 2;

const CLIENT_REPLY_ON: u8 = 0;
const CLIENT_REPLY_OFF: u8 = 1;
const CLIENT_REPLY_SKIP: u8 = 2;

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
        Self::new_with_options_and_store_config(
            store_shard_count,
            scripting_enabled,
            scripting_runtime_config,
            tsavorite_config_from_env(),
        )
    }

    fn new_with_options_and_store_config(
        store_shard_count: usize,
        scripting_enabled: bool,
        scripting_runtime_config: ScriptingRuntimeConfig,
        store_config: TsavoriteKvConfig,
    ) -> Result<Self, RequestProcessorInitError> {
        let store_shard_count = store_shard_count.max(1);
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
        // Redis compatibility tests exercise object payloads (including streams and list-3 backlink
        // stress cases) beyond 1 MiB. Keep object pages large enough by default so those writes fit
        // in a single record under the current monolithic object-value model.
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
            string_stores: string_stores.into(),
            object_stores: object_stores.into(),
            string_expirations: string_expirations.into(),
            hash_field_expirations: hash_field_expirations.into(),
            string_expiration_counts: string_expiration_counts.into(),
            string_key_registries: string_key_registries.into(),
            object_key_registries: object_key_registries.into(),
            watch_versions: (0..WATCH_VERSION_MAP_SIZE)
                .map(|_| AtomicU64::new(0))
                .collect(),
            blocking_wait_queues: Mutex::new(HashMap::new()),
            blocked_stream_wait_states: Mutex::new(HashMap::new()),
            blocked_xread_tail_ids: Mutex::new(HashMap::new()),
            pending_client_unblocks: Mutex::new(HashMap::new()),
            pause_blocked_blocking_clients: Mutex::new(HashSet::new()),
            forced_list_quicklist_keys: Mutex::new(HashSet::new()),
            forced_raw_string_keys: Mutex::new(HashSet::new()),
            forced_set_encoding_floors: Mutex::new(HashMap::new()),
            set_object_hot_state: Mutex::new(SetObjectHotState::default()),
            set_debug_ht_state: Mutex::new(HashMap::new()),
            random_state: AtomicU64::new(current_unix_time_millis().unwrap_or(0x9e3779b97f4a7c15)),
            active_expire_enabled: AtomicBool::new(true),
            debug_pause_cron: AtomicBool::new(false),
            debug_disable_deny_scripts: AtomicBool::new(false),
            expired_keys: AtomicU64::new(0),
            expired_keys_active: AtomicU64::new(0),
            total_error_replies: AtomicU64::new(0),
            lastsave_unix_seconds: AtomicU64::new(current_unix_time_millis().unwrap_or(0) / 1000),
            rdb_changes_since_last_save: AtomicU64::new(0),
            saved_rdb_snapshot: Mutex::new(None),
            resp_protocol_version: AtomicU8::new(RespProtocolVersion::Resp2.as_u8()),
            interop_force_resp3_zset_pairs: interop_force_resp3_zset_pairs_from_env(),
            blocked_clients: AtomicU64::new(0),
            connected_clients: AtomicU64::new(0),
            watching_clients: AtomicU64::new(0),
            executed_command_count: AtomicU64::new(0),
            rdb_key_save_delay_micros: AtomicU64::new(0),
            rdb_bgsave_deadline_unix_millis: AtomicU64::new(0),
            rdb_bgsave_child: Mutex::new(None),
            command_calls: Mutex::new(HashMap::new()),
            command_rejected_calls: Mutex::new(HashMap::new()),
            command_failed_calls: Mutex::new(HashMap::new()),
            error_reply_counts: Mutex::new(HashMap::new()),
            latency_events: Mutex::new(HashMap::new()),
            slowlog_entries: Mutex::new(VecDeque::new()),
            slowlog_next_id: AtomicU64::new(0),
            pubsub_state: Mutex::new(PubSubState::default()),
            tracking_state: Mutex::new(TrackingState::default()),
            tracking_table_max_keys: AtomicU64::new(1_000_000),
            auxiliary_databases: Mutex::new(HashMap::new()),
            key_lru_access_millis: Mutex::new(HashMap::new()),
            key_lfu_frequency: Mutex::new(HashMap::new()),
            config_overrides: Mutex::new(default_config_overrides()),
            lazy_expired_keys_for_replication: Mutex::new(Vec::new()),
            script_replication_effects: Mutex::new(Vec::new()),
            script_cache: Mutex::new(HashMap::new()),
            script_cache_insertion_order: Mutex::new(VecDeque::new()),
            script_cache_hits: AtomicU64::new(0),
            script_cache_misses: AtomicU64::new(0),
            script_cache_evictions: AtomicU64::new(0),
            script_runtime_timeouts: AtomicU64::new(0),
            used_memory_vm_functions: AtomicU64::new(0),
            lazyfree_pending_objects: AtomicU64::new(0),
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
            notify_keyspace_events_flags: AtomicU32::new(0),
            zset_max_listpack_entries: AtomicUsize::new(DEFAULT_ZSET_MAX_LISTPACK_ENTRIES),
            list_max_listpack_size: AtomicI64::new(DEFAULT_LIST_MAX_LISTPACK_SIZE),
            quicklist_packed_threshold: AtomicUsize::new(DEFAULT_QUICKLIST_PACKED_THRESHOLD),
            hash_max_listpack_entries: AtomicUsize::new(DEFAULT_HASH_MAX_LISTPACK_ENTRIES),
            hash_max_listpack_value: AtomicUsize::new(DEFAULT_HASH_MAX_LISTPACK_VALUE),
            set_max_listpack_entries: AtomicUsize::new(DEFAULT_SET_MAX_LISTPACK_ENTRIES),
            set_max_listpack_value: AtomicUsize::new(DEFAULT_SET_MAX_LISTPACK_VALUE),
            set_max_intset_entries: AtomicUsize::new(DEFAULT_SET_MAX_INTSET_ENTRIES),
            allow_access_expired: AtomicBool::new(false),
            functions: KvSessionFunctions,
            object_functions: ObjectSessionFunctions,
            client_pause_end_millis: AtomicU64::new(0),
            client_pause_type: AtomicU8::new(CLIENT_PAUSE_TYPE_NONE),
            client_name: Mutex::new(Vec::new()),
            client_reply_mode: AtomicU8::new(CLIENT_REPLY_ON),
            client_lib_name: Mutex::new(Vec::new()),
            client_lib_ver: Mutex::new(Vec::new()),
            server_metrics: OnceLock::new(),
        })
    }

    pub(crate) fn watch_key_version(&self, key: &[u8]) -> WatchVersion {
        self.watch_key_version_in_db(current_request_selected_db(), key)
    }

    pub(crate) fn watch_key_version_in_db(&self, db: DbName, key: &[u8]) -> WatchVersion {
        let slot = watch_version_slot(db, key);
        WatchVersion::new(self.watch_versions[slot].load(Ordering::SeqCst))
    }

    pub(crate) fn expire_watch_key_if_needed(
        &self,
        key: &[u8],
    ) -> Result<(), RequestExecutionError> {
        self.expire_watch_key_if_needed_in_db(current_request_selected_db(), key)?;
        Ok(())
    }

    pub(crate) fn expire_watch_key_if_needed_in_db(
        &self,
        db: DbName,
        key: &[u8],
    ) -> Result<(), RequestExecutionError> {
        self.with_selected_db(db, || self.expire_key_if_needed(key))?;
        Ok(())
    }

    pub(crate) fn string_value_len_for_replication(
        &self,
        key: &[u8],
    ) -> Result<Option<usize>, RequestExecutionError> {
        self.expire_key_if_needed(key)?;
        let Some(value) =
            self.read_string_value(DbKeyRef::new(current_request_selected_db(), key))?
        else {
            if self.object_key_exists(DbKeyRef::new(current_request_selected_db(), key))? {
                return Err(RequestExecutionError::WrongType);
            }
            return Ok(None);
        };
        Ok(Some(value.len()))
    }

    pub(crate) fn refresh_watched_keys_before_exec(&self, watched_keys: &[WatchedKey]) {
        for watched in watched_keys {
            let _ = self.with_selected_db(watched.db, || {
                self.expire_key_if_needed(watched.key.as_slice())
            });
        }
    }

    pub(crate) fn watch_versions_match(&self, watched_keys: &[WatchedKey]) -> bool {
        watched_keys.iter().all(|watched| {
            self.watch_key_version_in_db(watched.db, watched.key.as_slice()) == watched.version
        })
    }

    pub(super) fn set_resp_protocol_version(&self, version: RespProtocolVersion) {
        self.resp_protocol_version
            .store(version.as_u8(), Ordering::Release);
    }

    pub(super) fn resp_protocol_version(&self) -> RespProtocolVersion {
        if let Some(version) = RESP_PROTOCOL_OVERRIDE.with(|cell| cell.get()) {
            return version;
        }
        RespProtocolVersion::from_u8(self.resp_protocol_version.load(Ordering::Acquire))
            .unwrap_or(RespProtocolVersion::Resp2)
    }

    pub(super) fn with_resp_protocol_version_override<T>(
        &self,
        version: RespProtocolVersion,
        f: impl FnOnce() -> T,
    ) -> T {
        struct RespProtocolOverrideReset<'a> {
            cell: &'a Cell<Option<RespProtocolVersion>>,
            previous: Option<RespProtocolVersion>,
        }

        impl Drop for RespProtocolOverrideReset<'_> {
            fn drop(&mut self) {
                self.cell.set(self.previous);
            }
        }

        RESP_PROTOCOL_OVERRIDE.with(|cell| {
            let previous = cell.replace(Some(version));
            let _reset = RespProtocolOverrideReset { cell, previous };
            f()
        })
    }

    pub(super) fn emit_resp3_zset_pairs(&self) -> bool {
        self.resp_protocol_version().is_resp3() || self.interop_force_resp3_zset_pairs
    }

    /// Returns the current CLIENT REPLY mode (CLIENT_REPLY_ON / OFF / SKIP).
    pub(crate) fn client_reply_mode(&self) -> u8 {
        self.client_reply_mode.load(Ordering::Acquire)
    }

    /// Reset SKIP back to ON (called after one response is suppressed).
    pub(crate) fn clear_client_reply_skip(&self) {
        let _ = self.client_reply_mode.compare_exchange(
            CLIENT_REPLY_SKIP,
            CLIENT_REPLY_ON,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    pub(crate) fn blocked_clients(&self) -> u64 {
        self.blocked_clients.load(Ordering::Acquire)
    }

    pub(crate) fn blocking_key_counts(&self) -> (u64, u64) {
        let Ok(queues) = self.blocking_wait_queues.lock() else {
            return (0, 0);
        };
        let mut blocking_keys = HashSet::new();
        let mut blocking_keys_on_nokey = HashSet::new();
        for wait_key in queues.keys() {
            blocking_keys.insert(wait_key.key().clone());
            if wait_key.class() == BlockingWaitClass::Stream {
                blocking_keys_on_nokey.insert(wait_key.key().clone());
            }
        }
        (
            blocking_keys.len() as u64,
            blocking_keys_on_nokey.len() as u64,
        )
    }

    pub(crate) fn set_connected_clients(&self, count: u64) {
        self.connected_clients.store(count, Ordering::Release);
    }

    pub(crate) fn register_pubsub_client(&self, client_id: ClientId) {
        let Ok(mut state) = self.pubsub_state.lock() else {
            return;
        };
        state.pending_messages.entry(client_id).or_default();
        state
            .client_resp_versions
            .entry(client_id)
            .or_insert(RespProtocolVersion::Resp2);
    }

    pub(crate) fn update_pubsub_client_resp_version(
        &self,
        client_id: ClientId,
        version: RespProtocolVersion,
    ) {
        let Ok(mut state) = self.pubsub_state.lock() else {
            return;
        };
        state.client_resp_versions.insert(client_id, version);
    }

    pub(crate) fn unregister_pubsub_client(&self, client_id: ClientId) {
        let source_resp_versions = {
            let Ok(mut state) = self.pubsub_state.lock() else {
                return;
            };
            remove_all_pubsub_client_subscriptions(
                &mut state,
                client_id,
                PubSubTargetKind::Channel,
            );
            remove_all_pubsub_client_subscriptions(
                &mut state,
                client_id,
                PubSubTargetKind::Pattern,
            );
            let _ = state.pending_messages.remove(&client_id);
            let _ = state.client_resp_versions.remove(&client_id);
            state.client_resp_versions.clone()
        };
        self.mark_tracking_redirect_target_broken(client_id, &source_resp_versions);
        self.clear_client_tracking(client_id);
    }

    fn mark_tracking_redirect_target_broken(
        &self,
        disconnected_client_id: ClientId,
        source_resp_versions: &HashMap<ClientId, RespProtocolVersion>,
    ) {
        let Ok(mut state) = self.tracking_state.lock() else {
            return;
        };
        for (source_client_id, client_state) in &mut state.clients {
            if client_state.config.redirect_id != Some(disconnected_client_id) {
                continue;
            }
            client_state.broken_redirect = true;
            if !source_resp_versions
                .get(source_client_id)
                .copied()
                .unwrap_or(RespProtocolVersion::Resp2)
                .is_resp3()
            {
                client_state.broken_redirect_notified = true;
            }
        }
    }

    pub(crate) fn configure_client_tracking(
        &self,
        client_id: ClientId,
        config: ClientTrackingConfig,
    ) {
        let Ok(mut state) = self.tracking_state.lock() else {
            return;
        };
        if config.mode == ClientTrackingModeSetting::Off {
            let _ = state.clients.remove(&client_id);
            return;
        }

        let client_state = state.clients.entry(client_id).or_default();
        if config.bcast {
            client_state.tracked_keys.clear();
        }
        client_state.config = config;
        client_state.broken_redirect = false;
        client_state.broken_redirect_notified = false;
    }

    pub(crate) fn set_client_tracking_caching(&self, client_id: ClientId, caching: Option<bool>) {
        let Ok(mut state) = self.tracking_state.lock() else {
            return;
        };
        let Some(client_state) = state.clients.get_mut(&client_id) else {
            return;
        };
        client_state.config.caching = caching;
    }

    fn clear_client_tracking_caching_override_for_current_client(&self) {
        let Some(client_id) = current_request_client_id() else {
            return;
        };
        let Ok(mut state) = self.tracking_state.lock() else {
            return;
        };
        let Some(client_state) = state.clients.get_mut(&client_id) else {
            return;
        };
        if !matches!(
            client_state.config.mode,
            ClientTrackingModeSetting::OptIn | ClientTrackingModeSetting::OptOut
        ) {
            return;
        }
        client_state.config.caching = None;
    }

    pub(crate) fn clear_client_tracking(&self, client_id: ClientId) {
        let Ok(mut state) = self.tracking_state.lock() else {
            return;
        };
        let _ = state.clients.remove(&client_id);
    }

    pub(crate) fn client_tracking_redirect_broken(&self, client_id: ClientId) -> bool {
        let Ok(state) = self.tracking_state.lock() else {
            return false;
        };
        state
            .clients
            .get(&client_id)
            .is_some_and(|client_state| client_state.broken_redirect)
    }

    pub(crate) fn tracking_info_snapshot(&self) -> (usize, usize, usize, usize) {
        let Ok(state) = self.tracking_state.lock() else {
            return (0, 0, 0, 0);
        };
        let mut total_items = 0usize;
        let mut unique_keys = HashSet::<RedisKey>::new();
        let mut total_prefixes = 0usize;
        for client_state in state.clients.values() {
            total_items = total_items.saturating_add(client_state.tracked_keys.len());
            for key in &client_state.tracked_keys {
                unique_keys.insert(key.clone());
            }
            if client_state.config.bcast {
                total_prefixes = total_prefixes.saturating_add(client_state.config.prefixes.len());
            }
        }
        (
            total_items,
            unique_keys.len(),
            total_prefixes,
            state.clients.len(),
        )
    }

    pub(crate) fn set_tracking_table_max_keys(&self, value: u64) {
        self.tracking_table_max_keys.store(value, Ordering::Release);
    }

    pub(crate) fn invalidate_all_tracking_entries(&self) {
        let Ok(mut state) = self.tracking_state.lock() else {
            return;
        };
        let mut targets = Vec::new();
        for (client_id, client_state) in &mut state.clients {
            // Valkey semantics: FLUSHDB/FLUSHALL broadcast a global invalidation
            // (empty payload) to every tracking-enabled client.
            client_state.tracked_keys.clear();
            targets.push(client_state.config.redirect_id.unwrap_or(*client_id));
        }
        drop(state);
        for target in targets {
            self.enqueue_tracking_invalidation_message(target, &[]);
        }
    }

    pub(crate) fn enforce_tracking_table_max_keys(&self) {
        let max_keys = usize::try_from(self.tracking_table_max_keys.load(Ordering::Acquire))
            .unwrap_or(usize::MAX);
        let Ok(mut state) = self.tracking_state.lock() else {
            return;
        };
        let mut total_items = state
            .clients
            .values()
            .map(|client_state| client_state.tracked_keys.len())
            .sum::<usize>();
        if total_items <= max_keys {
            return;
        }

        let mut evicted = Vec::new();
        for (client_id, client_state) in &mut state.clients {
            while total_items > max_keys {
                let Some(key) = client_state.tracked_keys.iter().next().cloned() else {
                    break;
                };
                client_state.tracked_keys.remove(key.as_slice());
                let target = client_state.config.redirect_id.unwrap_or(*client_id);
                evicted.push((target, key));
                total_items = total_items.saturating_sub(1);
            }
            if total_items <= max_keys {
                break;
            }
        }
        drop(state);

        for (target, key) in evicted {
            let single_key = [key];
            self.enqueue_tracking_invalidation_message(target, &single_key);
        }
    }

    pub(crate) fn track_read_key_for_current_client(&self, key: &[u8]) {
        if !current_request_tracking_reads_enabled() {
            return;
        }
        if current_request_in_transaction() && collect_tracking_read_key(key) {
            return;
        }
        let Some(client_id) = current_request_client_id() else {
            return;
        };
        let Ok(mut state) = self.tracking_state.lock() else {
            return;
        };
        let Some(client_state) = state.clients.get_mut(&client_id) else {
            return;
        };
        if !client_state.config.should_track_reads() || client_state.config.bcast {
            return;
        }
        client_state.tracked_keys.insert(RedisKey::from(key));
        drop(state);
        self.enforce_tracking_table_max_keys();
    }

    fn apply_deferred_tracking_reads_for_current_client(&self, keys: &[RedisKey]) {
        let Some(client_id) = current_request_client_id() else {
            return;
        };
        self.apply_deferred_tracking_reads_for_client(client_id, keys);
    }

    fn apply_deferred_tracking_reads_for_client(&self, client_id: ClientId, keys: &[RedisKey]) {
        if keys.is_empty() {
            return;
        }
        let Ok(mut state) = self.tracking_state.lock() else {
            return;
        };
        let Some(client_state) = state.clients.get_mut(&client_id) else {
            return;
        };
        if client_state.config.bcast {
            return;
        }
        let mut inserted_any = false;
        for key in keys {
            inserted_any |= client_state.tracked_keys.insert(key.clone());
        }
        drop(state);
        if inserted_any {
            self.enforce_tracking_table_max_keys();
        }
    }

    pub(crate) fn begin_tracking_invalidation_batch(&self) {
        begin_tracking_invalidation_collection();
        begin_tracking_read_collection();
    }

    pub(crate) fn finish_tracking_invalidation_batch(&self, origin_client_id: Option<ClientId>) {
        let invalidated_keys = finish_tracking_invalidation_collection();
        let deferred_reads = finish_tracking_read_collection();
        if !invalidated_keys.is_empty() {
            self.emit_tracking_invalidations_for_keys_with_origin(
                &invalidated_keys,
                origin_client_id,
            );
        }
        if let Some(client_id) = origin_client_id {
            self.apply_deferred_tracking_reads_for_client(client_id, &deferred_reads);
        }
    }

    pub(crate) fn enqueue_pending_client_message(&self, client_id: ClientId, frame: Vec<u8>) {
        let Ok(mut state) = self.pubsub_state.lock() else {
            return;
        };
        state
            .pending_messages
            .entry(client_id)
            .or_default()
            .push_back(frame);
    }

    pub(crate) fn pubsub_subscription_count(&self, client_id: ClientId) -> usize {
        let Ok(state) = self.pubsub_state.lock() else {
            return 0;
        };
        pubsub_total_subscriptions_for_client(&state, client_id)
    }

    pub(crate) fn pubsub_subscribe_channels(
        &self,
        client_id: ClientId,
        channels: &[&[u8]],
    ) -> Vec<(Vec<u8>, usize)> {
        self.pubsub_subscribe_targets(client_id, channels, PubSubTargetKind::Channel)
    }

    pub(crate) fn pubsub_subscribe_patterns(
        &self,
        client_id: ClientId,
        patterns: &[&[u8]],
    ) -> Vec<(Vec<u8>, usize)> {
        self.pubsub_subscribe_targets(client_id, patterns, PubSubTargetKind::Pattern)
    }

    fn pubsub_subscribe_targets(
        &self,
        client_id: ClientId,
        targets: &[&[u8]],
        target_kind: PubSubTargetKind,
    ) -> Vec<(Vec<u8>, usize)> {
        let Ok(mut state) = self.pubsub_state.lock() else {
            return Vec::new();
        };
        state.pending_messages.entry(client_id).or_default();
        state
            .client_resp_versions
            .insert(client_id, self.resp_protocol_version());
        let mut acks = Vec::with_capacity(targets.len());
        for target in targets {
            let target_vec = (*target).to_vec();
            let inserted = match target_kind {
                PubSubTargetKind::Channel => state
                    .client_channels
                    .entry(client_id)
                    .or_default()
                    .insert(target_vec.clone()),
                PubSubTargetKind::Pattern => state
                    .client_patterns
                    .entry(client_id)
                    .or_default()
                    .insert(target_vec.clone()),
            };
            if inserted {
                match target_kind {
                    PubSubTargetKind::Channel => {
                        state
                            .channel_subscribers
                            .entry(target_vec.clone())
                            .or_default()
                            .insert(client_id);
                    }
                    PubSubTargetKind::Pattern => {
                        state
                            .pattern_subscribers
                            .entry(target_vec.clone())
                            .or_default()
                            .insert(client_id);
                    }
                }
            }
            acks.push((
                target_vec,
                pubsub_total_subscriptions_for_client(&state, client_id),
            ));
        }
        acks
    }

    pub(crate) fn pubsub_unsubscribe_channels(
        &self,
        client_id: ClientId,
        channels: &[&[u8]],
    ) -> Vec<(Option<Vec<u8>>, usize)> {
        self.pubsub_unsubscribe_targets(client_id, channels, PubSubTargetKind::Channel)
    }

    pub(crate) fn pubsub_unsubscribe_patterns(
        &self,
        client_id: ClientId,
        patterns: &[&[u8]],
    ) -> Vec<(Option<Vec<u8>>, usize)> {
        self.pubsub_unsubscribe_targets(client_id, patterns, PubSubTargetKind::Pattern)
    }

    fn pubsub_unsubscribe_targets(
        &self,
        client_id: ClientId,
        targets: &[&[u8]],
        target_kind: PubSubTargetKind,
    ) -> Vec<(Option<Vec<u8>>, usize)> {
        let Ok(mut state) = self.pubsub_state.lock() else {
            return Vec::new();
        };
        let existing_subscriptions = match target_kind {
            PubSubTargetKind::Channel => state.client_channels.get(&client_id),
            PubSubTargetKind::Pattern => state.client_patterns.get(&client_id),
        };
        let targets = collect_pubsub_unsubscribe_targets(existing_subscriptions, targets);
        if targets.is_empty() {
            return vec![(None, 0)];
        }

        let mut acks = Vec::with_capacity(targets.len());
        for target in targets {
            let removed = match target_kind {
                PubSubTargetKind::Channel => remove_pubsub_client_subscription(
                    &mut state.client_channels,
                    client_id,
                    &target,
                ),
                PubSubTargetKind::Pattern => remove_pubsub_client_subscription(
                    &mut state.client_patterns,
                    client_id,
                    &target,
                ),
            };
            if removed {
                match target_kind {
                    PubSubTargetKind::Channel => {
                        remove_pubsub_subscriber(&mut state.channel_subscribers, &target, client_id)
                    }
                    PubSubTargetKind::Pattern => {
                        remove_pubsub_subscriber(&mut state.pattern_subscribers, &target, client_id)
                    }
                }
            }
            acks.push((
                Some(target),
                pubsub_total_subscriptions_for_client(&state, client_id),
            ));
        }
        acks
    }

    pub(crate) fn pubsub_publish(&self, channel: &[u8], message: &[u8]) -> i64 {
        let Ok(mut state) = self.pubsub_state.lock() else {
            return 0;
        };
        let channel_targets = collect_pubsub_channel_targets(&state, channel);
        let pattern_targets = collect_pubsub_pattern_targets(&state, channel);
        let channel_delivery_count = channel_targets.len();
        let pattern_delivery_count = pattern_targets.len();
        let total_delivery_count = channel_delivery_count.saturating_add(pattern_delivery_count);
        if total_delivery_count == 0 {
            return 0;
        }

        // Pre-encode RESP2 and RESP3 variants lazily to avoid redundant encoding.
        let mut resp2_message_frame: Option<Vec<u8>> = None;
        let mut resp3_message_frame: Option<Vec<u8>> = None;
        for target in channel_targets {
            let resp3 = state
                .client_resp_versions
                .get(&target)
                .map(|v| v.is_resp3())
                .unwrap_or(false);
            let frame = if resp3 {
                resp3_message_frame
                    .get_or_insert_with(|| {
                        encode_pubsub_message_frame(b"message", None, channel, message, true)
                    })
                    .clone()
            } else {
                resp2_message_frame
                    .get_or_insert_with(|| {
                        encode_pubsub_message_frame(b"message", None, channel, message, false)
                    })
                    .clone()
            };
            state
                .pending_messages
                .entry(target)
                .or_default()
                .push_back(frame);
        }

        for (target, pattern) in pattern_targets {
            let resp3 = state
                .client_resp_versions
                .get(&target)
                .map(|v| v.is_resp3())
                .unwrap_or(false);
            let frame =
                encode_pubsub_message_frame(b"pmessage", Some(&pattern), channel, message, resp3);
            state
                .pending_messages
                .entry(target)
                .or_default()
                .push_back(frame);
        }

        saturating_usize_to_i64(total_delivery_count)
    }

    pub(crate) fn pubsub_numsub(&self, channels: &[&[u8]]) -> Vec<(Vec<u8>, usize)> {
        let Ok(state) = self.pubsub_state.lock() else {
            return channels
                .iter()
                .map(|channel| ((*channel).to_vec(), 0usize))
                .collect();
        };
        channels
            .iter()
            .map(|channel| {
                (
                    (*channel).to_vec(),
                    state
                        .channel_subscribers
                        .get(*channel)
                        .map_or(0, HashSet::len),
                )
            })
            .collect()
    }

    pub(crate) fn pubsub_numpat(&self) -> usize {
        let Ok(state) = self.pubsub_state.lock() else {
            return 0;
        };
        state.pattern_subscribers.len()
    }

    pub(crate) fn pubsub_channels(&self, pattern: Option<&[u8]>) -> Vec<Vec<u8>> {
        let Ok(state) = self.pubsub_state.lock() else {
            return Vec::new();
        };
        let mut channels = state
            .channel_subscribers
            .keys()
            .filter(|channel| match pattern {
                Some(pattern) => self::server_commands::redis_glob_match(
                    pattern,
                    channel.as_slice(),
                    self::server_commands::CaseSensitivity::Sensitive,
                    0,
                ),
                None => true,
            })
            .cloned()
            .collect::<Vec<_>>();
        channels.sort();
        channels
    }

    pub(crate) fn take_pending_pubsub_messages(&self, client_id: ClientId) -> Vec<Vec<u8>> {
        let Ok(mut state) = self.pubsub_state.lock() else {
            return Vec::new();
        };
        let Some(messages) = state.pending_messages.get_mut(&client_id) else {
            return Vec::new();
        };
        let mut drained = Vec::with_capacity(messages.len());
        while let Some(message) = messages.pop_front() {
            drained.push(message);
        }
        drained
    }

    pub(crate) fn clear_pending_tracking_messages_for_client(&self, client_id: ClientId) {
        let Ok(mut state) = self.pubsub_state.lock() else {
            return;
        };
        let Some(messages) = state.pending_messages.get_mut(&client_id) else {
            return;
        };
        let mut retained = VecDeque::with_capacity(messages.len());
        while let Some(message) = messages.pop_front() {
            if pending_message_is_tracking_message(&message) {
                continue;
            }
            retained.push_back(message);
        }
        *messages = retained;
    }

    pub(crate) fn emit_tracking_invalidations_for_keys(&self, keys: &[RedisKey]) {
        self.emit_tracking_invalidations_for_keys_with_origin(keys, current_request_client_id());
    }

    fn emit_tracking_invalidations_for_keys_with_origin(
        &self,
        keys: &[RedisKey],
        origin_client_id: Option<ClientId>,
    ) {
        if keys.is_empty() {
            return;
        }
        let (connected_clients, source_resp_versions) = {
            let Ok(pubsub_state) = self.pubsub_state.lock() else {
                return;
            };
            (
                pubsub_state
                    .pending_messages
                    .keys()
                    .copied()
                    .collect::<HashSet<_>>(),
                pubsub_state.client_resp_versions.clone(),
            )
        };
        let Ok(mut state) = self.tracking_state.lock() else {
            return;
        };
        // Group keys by redirection target and prefix group to mirror BCAST behavior:
        // one invalidation message per matching prefix group.
        let mut grouped: HashMap<(ClientId, Vec<u8>), Vec<RedisKey>> = HashMap::new();
        let mut broken_redirect_clients = HashSet::new();
        for (tracked_client_id, tracked_state) in &mut state.clients {
            if tracked_state.config.noloop && origin_client_id == Some(*tracked_client_id) {
                continue;
            }
            for key in keys {
                let groups = tracking_groups_for_key(&tracked_state.config, key);
                if groups.is_empty() {
                    continue;
                }
                if !tracked_state.config.bcast && !tracked_state.tracked_keys.remove(key.as_slice())
                {
                    continue;
                }
                let target = tracked_state
                    .config
                    .redirect_id
                    .unwrap_or(*tracked_client_id);
                if tracked_state.config.redirect_id.is_some()
                    && !connected_clients.contains(&target)
                {
                    tracked_state.broken_redirect = true;
                    if !tracked_state.broken_redirect_notified
                        && source_resp_versions
                            .get(tracked_client_id)
                            .copied()
                            .unwrap_or(RespProtocolVersion::Resp2)
                            .is_resp3()
                    {
                        tracked_state.broken_redirect_notified = true;
                        broken_redirect_clients.insert(*tracked_client_id);
                    }
                    continue;
                }
                for group in groups {
                    grouped
                        .entry((target, group))
                        .or_default()
                        .push(key.clone());
                }
            }
        }
        drop(state);

        for ((target_client_id, _group), grouped_keys) in grouped {
            self.enqueue_tracking_invalidation_message(target_client_id, &grouped_keys);
        }
        for client_id in broken_redirect_clients {
            self.enqueue_pending_client_message(
                client_id,
                encode_tracking_redir_broken_push_frame(),
            );
        }
    }

    pub(crate) fn enqueue_tracking_invalidation_for_key(&self, key: &[u8]) {
        self.enqueue_tracking_invalidation_for_key_with_origin(key, current_request_client_id());
    }

    pub(crate) fn enqueue_tracking_invalidation_for_key_as_server(&self, key: &[u8]) {
        self.enqueue_tracking_invalidation_for_key_with_origin(key, None);
    }

    fn enqueue_tracking_invalidation_for_key_with_origin(
        &self,
        key: &[u8],
        origin_client_id: Option<ClientId>,
    ) {
        if origin_client_id.is_some() && collect_tracking_invalidation_key(key) {
            return;
        }
        self.emit_tracking_invalidations_for_keys_with_origin(
            &[RedisKey::from(key)],
            origin_client_id,
        );
    }

    fn enqueue_tracking_invalidation_message(&self, target_client_id: ClientId, keys: &[RedisKey]) {
        let (resp3, payload) = {
            let Ok(state) = self.pubsub_state.lock() else {
                return;
            };
            let resp3 = state
                .client_resp_versions
                .get(&target_client_id)
                .copied()
                .unwrap_or(RespProtocolVersion::Resp2)
                .is_resp3();
            let payload = encode_tracking_invalidation_payload(keys);
            (resp3, payload)
        };

        let frame = if resp3 {
            encode_tracking_push_frame(&payload)
        } else {
            encode_pubsub_message_frame(
                b"message",
                None,
                TRACKING_INVALIDATE_CHANNEL,
                &payload,
                false,
            )
        };
        self.enqueue_pending_client_message(target_client_id, frame);
    }

    /// Emit keyspace/keyevent notifications through pubsub.
    ///
    /// `event_type` is the notification class (NOTIFY_STRING, NOTIFY_GENERIC, etc.).
    /// `event` is the event name (e.g. "set", "del", "expire").
    /// `key` is the affected key.
    pub(super) fn notify_keyspace_event(&self, event_type: u32, event: &[u8], key: &[u8]) {
        let flags = self.notify_keyspace_events_flags.load(Ordering::Acquire);
        if flags == 0 {
            return;
        }
        // Event must match the configured type filter.
        if flags & event_type == 0 {
            return;
        }

        let selected_db = usize::from(current_request_selected_db());
        let selected_db_text = selected_db.to_string();

        // __keyspace@<db>__:<key> notifications (event name as message)
        if flags & NOTIFY_KEYSPACE != 0 {
            let mut channel = Vec::with_capacity(14 + selected_db_text.len() + key.len());
            channel.extend_from_slice(b"__keyspace@");
            channel.extend_from_slice(selected_db_text.as_bytes());
            channel.extend_from_slice(b"__:");
            channel.extend_from_slice(key);
            self.pubsub_publish(&channel, event);
        }

        // __keyevent@<db>__:<event> notifications (key name as message)
        if flags & NOTIFY_KEYEVENT != 0 {
            let mut channel = Vec::with_capacity(14 + selected_db_text.len() + event.len());
            channel.extend_from_slice(b"__keyevent@");
            channel.extend_from_slice(selected_db_text.as_bytes());
            channel.extend_from_slice(b"__:");
            channel.extend_from_slice(event);
            self.pubsub_publish(&channel, key);
        }
    }

    pub(crate) fn key_type_snapshot_for_setkey_overwrite(
        &self,
        key: &[u8],
    ) -> Result<(bool, Option<ObjectTypeTag>), RequestExecutionError> {
        let string_exists = self.key_exists(DbKeyRef::new(current_request_selected_db(), key))?;
        let object_type = self
            .object_read(DbKeyRef::new(current_request_selected_db(), key))?
            .map(|object| object.object_type);
        Ok((string_exists, object_type))
    }

    pub(crate) fn notify_setkey_overwrite_events(
        &self,
        key: &[u8],
        previous_string_exists: bool,
        previous_object_type: Option<ObjectTypeTag>,
        new_type: Option<ObjectTypeTag>,
    ) {
        if !previous_string_exists && previous_object_type.is_none() {
            return;
        }

        self.notify_keyspace_event(NOTIFY_OVERWRITTEN, b"overwritten", key);

        let type_changed = if previous_string_exists {
            new_type.is_some()
        } else {
            previous_object_type != new_type
        };
        if type_changed {
            self.notify_keyspace_event(NOTIFY_TYPE_CHANGED, b"type_changed", key);
        }
    }

    pub(super) fn set_notify_keyspace_events_flags(&self, flags: u32) {
        self.notify_keyspace_events_flags
            .store(flags, Ordering::Release);
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
        self.ensure_bgsave_child_running();
        self.rdb_bgsave_deadline_unix_millis
            .store(now.saturating_add(delay_millis), Ordering::Release);
    }

    pub(super) fn rdb_bgsave_in_progress(&self) -> bool {
        if self.reap_bgsave_child_if_finished() {
            self.rdb_bgsave_deadline_unix_millis
                .store(0, Ordering::Release);
            return false;
        }
        let deadline = self.rdb_bgsave_deadline_unix_millis.load(Ordering::Acquire);
        if deadline == 0 {
            self.stop_bgsave_child();
            return false;
        }
        let now = current_unix_time_millis().unwrap_or(0);
        if now < deadline {
            return true;
        }
        self.stop_bgsave_child();
        let _ = self.rdb_bgsave_deadline_unix_millis.compare_exchange(
            deadline,
            0,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        false
    }

    #[cfg(test)]
    pub(crate) fn force_finish_bgsave_for_tests(&self) {
        self.stop_bgsave_child();
        self.rdb_bgsave_deadline_unix_millis
            .store(0, Ordering::Release);
    }

    fn ensure_bgsave_child_running(&self) {
        let Ok(mut child_guard) = self.rdb_bgsave_child.lock() else {
            return;
        };
        if let Some(child) = child_guard.as_mut() {
            match child.try_wait() {
                Ok(None) => return,
                Ok(Some(_)) | Err(_) => {
                    *child_guard = None;
                }
            }
        }

        let child = Command::new("sh")
            .arg("-c")
            .arg("sleep 3600")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn();
        if let Ok(child) = child {
            *child_guard = Some(child);
        }
    }

    fn stop_bgsave_child(&self) {
        let Ok(mut child_guard) = self.rdb_bgsave_child.lock() else {
            return;
        };
        let Some(mut child) = child_guard.take() else {
            return;
        };
        let _ = child.kill();
        let _ = child.wait();
    }

    fn reap_bgsave_child_if_finished(&self) -> bool {
        let Ok(mut child_guard) = self.rdb_bgsave_child.lock() else {
            return false;
        };
        let Some(child) = child_guard.as_mut() else {
            return false;
        };
        match child.try_wait() {
            Ok(None) => false,
            Ok(Some(_)) | Err(_) => {
                *child_guard = None;
                true
            }
        }
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

    pub(crate) fn attach_metrics(&self, metrics: Arc<crate::ServerMetrics>) {
        let _ = self.server_metrics.set(metrics);
    }

    pub(crate) fn slowlog_len(&self) -> usize {
        self.slowlog_entries
            .lock()
            .map(|entries| entries.len())
            .unwrap_or(0)
    }

    pub(crate) fn reset_slowlog(&self) {
        if let Ok(mut entries) = self.slowlog_entries.lock() {
            entries.clear();
        }
    }

    pub(crate) fn slowlog_entries(&self, count: Option<usize>) -> Vec<SlowlogEntry> {
        let Ok(entries) = self.slowlog_entries.lock() else {
            return Vec::new();
        };
        let take_count = count.unwrap_or(10).min(entries.len());
        entries.iter().take(take_count).cloned().collect()
    }

    pub(crate) fn record_slowlog_command(
        &self,
        args: &[&[u8]],
        command: CommandId,
        duration: Duration,
        client_id: Option<ClientId>,
    ) {
        if should_skip_slowlog_for_command(command, args) {
            return;
        }
        let threshold_micros = self.slowlog_threshold_micros();
        if threshold_micros < 0 {
            return;
        }
        let max_len = self.slowlog_max_len();
        if max_len == 0 {
            return;
        }
        let duration_micros = duration.as_micros().min(i64::MAX as u128) as i64;
        if duration_micros < threshold_micros {
            return;
        }

        let logged_args = slowlog_arguments_for_command(args);
        let (peer_id, client_name) = self.slowlog_client_metadata(client_id);
        let unix_time_seconds = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_secs().min(i64::MAX as u64) as i64)
            .unwrap_or(0);
        let entry = SlowlogEntry {
            id: self.slowlog_next_id.fetch_add(1, Ordering::Relaxed),
            unix_time_seconds,
            duration_micros,
            args: logged_args,
            peer_id,
            client_name,
        };
        let Ok(mut entries) = self.slowlog_entries.lock() else {
            return;
        };
        entries.push_front(entry);
        while entries.len() > max_len {
            entries.pop_back();
        }
    }

    fn slowlog_threshold_micros(&self) -> i64 {
        let Ok(values) = self.config_overrides.lock() else {
            return 10_000;
        };
        values
            .get(b"slowlog-log-slower-than".as_slice())
            .and_then(|value| std::str::from_utf8(value).ok())
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(10_000)
    }

    fn slowlog_max_len(&self) -> usize {
        let Ok(values) = self.config_overrides.lock() else {
            return 128;
        };
        values
            .get(b"slowlog-max-len".as_slice())
            .and_then(|value| std::str::from_utf8(value).ok())
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(128)
    }

    fn slowlog_client_metadata(&self, client_id: Option<ClientId>) -> (Vec<u8>, Vec<u8>) {
        if let Some(client_id) = client_id
            && let Some(metrics) = self.server_metrics.get()
        {
            let peer_id = metrics
                .client_peer_id(client_id)
                .unwrap_or_else(|| b"127.0.0.1:0".to_vec());
            let client_name = metrics.client_name(client_id).unwrap_or_default();
            return (peer_id, client_name);
        }

        let client_name = self
            .client_name
            .lock()
            .map(|name| name.clone())
            .unwrap_or_default();
        (b"127.0.0.1:0".to_vec(), client_name)
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
        !matches!(values.get(b"lazyexpire-nested-arbitrary-keys".as_slice()), Some(value) if value.eq_ignore_ascii_case(b"no"))
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

    pub(crate) fn record_command_rejection(&self, command_name: &[u8]) {
        if command_name.is_empty() {
            return;
        }
        let normalized = normalize_ascii_lower(command_name);
        if let Ok(mut rejected_calls) = self.command_rejected_calls.lock() {
            *rejected_calls.entry(normalized).or_insert(0) += 1;
        }
    }

    pub(crate) fn record_error_reply(&self, error_name: &[u8]) {
        if error_name.is_empty() {
            return;
        }
        let normalized = normalize_ascii_upper(error_name);
        self.total_error_replies.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut error_reply_counts) = self.error_reply_counts.lock() {
            *error_reply_counts.entry(normalized).or_insert(0) += 1;
        }
    }

    pub(crate) fn error_reply_counts_snapshot(&self) -> Vec<(Vec<u8>, u64)> {
        let Ok(error_reply_counts) = self.error_reply_counts.lock() else {
            return Vec::new();
        };
        let mut ordered = BTreeMap::new();
        for (error_name, count) in error_reply_counts.iter() {
            ordered.insert(error_name.clone(), *count);
        }
        ordered.into_iter().collect()
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

    pub(crate) fn command_rejected_call_counts_snapshot(&self) -> Vec<(Vec<u8>, u64)> {
        let Ok(rejected_calls) = self.command_rejected_calls.lock() else {
            return Vec::new();
        };
        let mut ordered = BTreeMap::new();
        for (command, count) in rejected_calls.iter() {
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

    pub(crate) fn latency_latest(&self) -> Vec<LatencyEvent> {
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
            .map(
                |(event_name, (latest_sample, max_latency_millis))| LatencyEvent {
                    event_name,
                    latest_sample,
                    max_latency_millis,
                },
            )
            .collect()
    }

    pub(crate) fn latency_graph_range(&self, event_name: &[u8]) -> Option<LatencyRange> {
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
        Some(LatencyRange::new(state.max_latency_millis, min_latency))
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
            pending.push(DbScopedKey::new(current_request_selected_db(), key));
        }
    }

    pub(crate) fn take_lazy_expired_keys_for_replication(&self) -> Vec<DbScopedKey> {
        let Ok(mut pending) = self.lazy_expired_keys_for_replication.lock() else {
            return Vec::new();
        };
        std::mem::take(&mut *pending)
    }

    pub(crate) fn clear_script_replication_effects(&self) {
        let Ok(mut pending) = self.script_replication_effects.lock() else {
            return;
        };
        pending.clear();
    }

    pub(crate) fn enqueue_script_replication_effect(
        &self,
        command: CommandId,
        frame: Vec<u8>,
        response: Vec<u8>,
    ) {
        let Ok(mut pending) = self.script_replication_effects.lock() else {
            return;
        };
        pending.push(ScriptReplicationEffect {
            selected_db: current_request_selected_db(),
            command,
            frame,
            response,
        });
    }

    pub(crate) fn take_script_replication_effects(&self) -> Vec<ScriptReplicationEffect> {
        let Ok(mut pending) = self.script_replication_effects.lock() else {
            return Vec::new();
        };
        std::mem::take(&mut *pending)
    }

    pub(crate) fn reset_commandstats(&self) {
        if let Ok(mut calls) = self.command_calls.lock() {
            calls.clear();
        }
        if let Ok(mut rejected_calls) = self.command_rejected_calls.lock() {
            rejected_calls.clear();
        }
        if let Ok(mut failed_calls) = self.command_failed_calls.lock() {
            failed_calls.clear();
        }
    }

    pub(crate) fn reset_error_reply_stats(&self) {
        self.total_error_replies.store(0, Ordering::Relaxed);
        if let Ok(mut error_reply_counts) = self.error_reply_counts.lock() {
            error_reply_counts.clear();
        }
    }

    pub(crate) fn render_commandstats_info_payload(&self) -> String {
        let mut payload = String::from("# Commandstats\r\n");
        let mut ordered = BTreeMap::<Vec<u8>, u64>::new();
        for (command, count) in self.command_call_counts_snapshot() {
            ordered.insert(command, count);
        }
        let rejected_calls_by_command = self
            .command_rejected_call_counts_snapshot()
            .into_iter()
            .collect::<HashMap<Vec<u8>, u64>>();
        let failed_calls_by_command = self
            .command_failed_call_counts_snapshot()
            .into_iter()
            .collect::<HashMap<Vec<u8>, u64>>();
        for command in rejected_calls_by_command.keys() {
            ordered.entry(command.clone()).or_insert(0);
        }
        for command in failed_calls_by_command.keys() {
            ordered.entry(command.clone()).or_insert(0);
        }
        for (command, count) in ordered {
            let rejected_calls = rejected_calls_by_command
                .get(&command)
                .copied()
                .unwrap_or(0);
            let failed_calls = failed_calls_by_command.get(&command).copied().unwrap_or(0);
            payload.push_str("cmdstat_");
            payload.push_str(&String::from_utf8_lossy(&command));
            payload.push_str(":calls=");
            payload.push_str(&count.to_string());
            payload.push_str(",usec=0,usec_per_call=0.00,rejected_calls=");
            payload.push_str(&rejected_calls.to_string());
            payload.push_str(",failed_calls=");
            payload.push_str(&failed_calls.to_string());
            payload.push_str("\r\n");
        }
        payload
    }

    pub(crate) fn render_errorstats_info_payload(&self) -> String {
        let mut payload = String::from("# Errorstats\r\n");
        for (error_name, count) in self.error_reply_counts_snapshot() {
            payload.push_str("errorstat_");
            payload.push_str(&String::from_utf8_lossy(&error_name));
            payload.push_str(":count=");
            payload.push_str(&count.to_string());
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

    pub(crate) fn script_execution_in_progress(&self) -> bool {
        self.script_running.load(Ordering::Acquire)
    }

    fn current_script_time_snapshot(&self) -> Option<ScriptTimeSnapshot> {
        let running_script = self.running_script.lock().ok()?;
        let state = running_script.as_ref()?;
        if state.thread_id != std::thread::current().id() {
            return None;
        }
        Some(state.frozen_time)
    }

    pub(super) fn used_memory_vm_functions(&self) -> u64 {
        self.used_memory_vm_functions.load(Ordering::Relaxed)
    }

    pub(super) fn set_used_memory_vm_functions(&self, bytes: u64) {
        self.used_memory_vm_functions
            .store(bytes, Ordering::Relaxed);
    }

    pub(super) fn lazyfree_pending_objects(&self) -> u64 {
        self.lazyfree_pending_objects.load(Ordering::Relaxed)
    }

    pub(super) fn set_lazyfree_pending_objects(&self, count: u64) {
        self.lazyfree_pending_objects
            .store(count, Ordering::Relaxed);
    }

    pub(super) fn reset_lazyfree_pending_objects(&self) {
        self.lazyfree_pending_objects.store(0, Ordering::Relaxed);
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

    pub(crate) fn register_blocking_wait(
        &self,
        client_id: ClientId,
        wait_keys: &[BlockingWaitKey],
    ) {
        if wait_keys.is_empty() {
            return;
        }
        // TLA+ (`BlockingDisconnectLeak`) `Block(c,k)` queue-enqueue critical section.
        // TLA+ (`BlockingWaitClassIsolation`) `Block(c,k,cl)` queue-enqueue critical section.
        if let Ok(mut queues) = self.blocking_wait_queues.lock() {
            for wait_key in wait_keys {
                let queue = queues.entry(wait_key.clone()).or_insert_with(VecDeque::new);
                if !queue.contains(&client_id) {
                    queue.push_back(client_id);
                }
            }
        }
    }

    pub(crate) fn unregister_blocking_wait(
        &self,
        client_id: ClientId,
        wait_keys: &[BlockingWaitKey],
    ) {
        if wait_keys.is_empty() {
            return;
        }
        // TLA+ cleanup side for both wake completion and `Disconnect(c)` handling.
        if let Ok(mut queues) = self.blocking_wait_queues.lock() {
            for wait_key in wait_keys {
                let mut should_remove = false;
                if let Some(queue) = queues.get_mut(wait_key) {
                    queue.retain(|entry| *entry != client_id);
                    should_remove = queue.is_empty();
                }
                if should_remove {
                    let _ = queues.remove(wait_key);
                }
            }
        }
    }

    pub(crate) fn is_blocking_wait_turn(
        &self,
        client_id: ClientId,
        wait_keys: &[BlockingWaitKey],
    ) -> bool {
        if wait_keys.is_empty() {
            return true;
        }
        // TLA+ `WakeHead(k)` guard: caller may proceed only when it is at queue front.
        // TLA+ (`BlockingWaitClassIsolation`) `ServeReadyHead(c,k,cl)` queue-front guard.
        let Ok(queues) = self.blocking_wait_queues.lock() else {
            return true;
        };
        for wait_key in wait_keys {
            let front = queues
                .get(wait_key)
                .and_then(|queue| queue.front().copied());
            match front {
                Some(front_client) => return front_client == client_id,
                None => continue,
            }
        }
        true
    }

    pub(crate) fn blocked_xread_tail_id(
        &self,
        client_id: ClientId,
        stream_index: usize,
        stream_count: usize,
        current_tail: StreamId,
    ) -> StreamId {
        let Ok(mut blocked_xread_tail_ids) = self.blocked_xread_tail_ids.lock() else {
            return current_tail;
        };
        let entry = blocked_xread_tail_ids
            .entry(client_id)
            .or_insert_with(|| vec![None; stream_count]);
        if entry.len() < stream_count {
            entry.resize(stream_count, None);
        }
        if let Some(saved_tail) = entry[stream_index] {
            return saved_tail;
        }
        entry[stream_index] = Some(current_tail);
        current_tail
    }

    pub(crate) fn clear_blocked_xread_tail_ids(&self, client_id: ClientId) {
        if let Ok(mut blocked_xread_tail_ids) = self.blocked_xread_tail_ids.lock() {
            let _ = blocked_xread_tail_ids.remove(&client_id);
        }
    }

    pub(crate) fn set_blocked_stream_wait_state(
        &self,
        client_id: ClientId,
        state: BlockingStreamWaitState,
    ) {
        // TLA+ : TrackWaitPredicate
        if let Ok(mut blocked_stream_wait_states) = self.blocked_stream_wait_states.lock() {
            blocked_stream_wait_states.insert(client_id, state);
        }
    }

    pub(crate) fn clear_blocked_stream_wait_state(&self, client_id: ClientId) {
        if let Ok(mut blocked_stream_wait_states) = self.blocked_stream_wait_states.lock() {
            let _ = blocked_stream_wait_states.remove(&client_id);
        }
    }

    pub(crate) fn request_client_unblock(
        &self,
        client_id: ClientId,
        mode: ClientUnblockMode,
    ) -> bool {
        // TLA+ : ClientUnblockRequest
        let blocked_in_wait_queue = self
            .blocking_wait_queues
            .lock()
            .map(|queues| queues.values().any(|queue| queue.contains(&client_id)))
            .unwrap_or(false);
        let blocked_by_pause_while_running_blocking_command = self
            .pause_blocked_blocking_clients
            .lock()
            .map(|clients| clients.contains(&client_id))
            .unwrap_or(false);
        if blocked_by_pause_while_running_blocking_command
            && (self.is_client_paused(true) || self.is_client_paused(false))
        {
            return false;
        }
        if !blocked_in_wait_queue && !blocked_by_pause_while_running_blocking_command {
            return false;
        }
        if let Ok(mut pending) = self.pending_client_unblocks.lock() {
            pending.insert(client_id, mode);
            return true;
        }
        false
    }

    pub(crate) fn take_client_unblock_request(
        &self,
        client_id: ClientId,
    ) -> Option<ClientUnblockMode> {
        self.pending_client_unblocks
            .lock()
            .ok()
            .and_then(|mut pending| pending.remove(&client_id))
    }

    pub(crate) fn clear_client_unblock_request(&self, client_id: ClientId) {
        if let Ok(mut pending) = self.pending_client_unblocks.lock() {
            let _ = pending.remove(&client_id);
        }
    }

    pub(crate) fn register_pause_blocked_blocking_client(&self, client_id: ClientId) {
        // TLA+ : PauseGateRegisterBlockingClient
        if let Ok(mut clients) = self.pause_blocked_blocking_clients.lock() {
            clients.insert(client_id);
        }
    }

    pub(crate) fn unregister_pause_blocked_blocking_client(&self, client_id: ClientId) {
        if let Ok(mut clients) = self.pause_blocked_blocking_clients.lock() {
            clients.remove(&client_id);
        }
    }

    pub(crate) fn is_pause_blocked_blocking_client(&self, client_id: ClientId) -> bool {
        self.pause_blocked_blocking_clients
            .lock()
            .map(|clients| clients.contains(&client_id))
            .unwrap_or(false)
    }

    /// Apply CLIENT PAUSE with the given timeout (in millis) and mode.
    /// Follows Valkey semantics:
    /// - If currently paused with ALL and new pause is WRITE, keep ALL.
    /// - End time is only extended, never shortened.
    pub(crate) fn client_pause(&self, timeout_millis: u64, pause_all: bool) {
        let now_millis = current_unix_time_millis().unwrap_or(0);
        let new_end = now_millis.saturating_add(timeout_millis);

        let current_type = self.client_pause_type.load(Ordering::Acquire);
        let new_type = if pause_all || current_type == CLIENT_PAUSE_TYPE_ALL {
            CLIENT_PAUSE_TYPE_ALL
        } else {
            CLIENT_PAUSE_TYPE_WRITE
        };

        self.client_pause_type.store(new_type, Ordering::Release);

        loop {
            let current_end = self.client_pause_end_millis.load(Ordering::Acquire);
            if new_end <= current_end {
                break;
            }
            match self.client_pause_end_millis.compare_exchange_weak(
                current_end,
                new_end,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(_) => continue,
            }
        }
    }

    /// Remove CLIENT PAUSE state.
    pub(crate) fn client_unpause(&self) {
        // TLA+ : ClientUnpause
        self.client_pause_type
            .store(CLIENT_PAUSE_TYPE_NONE, Ordering::Release);
        self.client_pause_end_millis.store(0, Ordering::Release);
    }

    /// Check whether the server is currently paused for the given command type.
    /// Returns true if the command should be blocked.
    pub(crate) fn is_client_paused(&self, is_write_or_may_replicate: bool) -> bool {
        let pause_type = self.client_pause_type.load(Ordering::Acquire);
        if pause_type == CLIENT_PAUSE_TYPE_NONE {
            return false;
        }
        let end_millis = self.client_pause_end_millis.load(Ordering::Acquire);
        if end_millis == 0 {
            return false;
        }
        let now_millis = current_unix_time_millis().unwrap_or(0);
        // Keep pause active through the exact end-millisecond to avoid
        // undercutting timeout-based compatibility assertions on boundary ticks.
        if now_millis > end_millis {
            self.client_pause_type
                .store(CLIENT_PAUSE_TYPE_NONE, Ordering::Release);
            self.client_pause_end_millis.store(0, Ordering::Release);
            return false;
        }
        if pause_type == CLIENT_PAUSE_TYPE_ALL {
            return true;
        }
        is_write_or_may_replicate
    }

    /// Check whether key expiration is suppressed by an active CLIENT PAUSE.
    /// In Valkey, both WRITE and ALL pause modes include PAUSE_ACTION_EXPIRE.
    pub(crate) fn is_expire_action_paused(&self) -> bool {
        let pause_type = self.client_pause_type.load(Ordering::Acquire);
        if pause_type == CLIENT_PAUSE_TYPE_NONE {
            return false;
        }
        let end_millis = self.client_pause_end_millis.load(Ordering::Acquire);
        if end_millis == 0 {
            return false;
        }
        let now_millis = current_unix_time_millis().unwrap_or(0);
        // Keep expire suppression active through the exact end-millisecond.
        now_millis <= end_millis
    }

    /// Returns the remaining pause duration, or None if not paused.
    /// Look up a cached script by SHA1 and return its body for shebang inspection.
    pub(crate) fn cached_script_body(&self, sha1: &[u8]) -> Option<Vec<u8>> {
        let normalized: String = sha1
            .iter()
            .map(|&byte| byte.to_ascii_lowercase() as char)
            .collect();
        let Ok(cache) = self.script_cache.lock() else {
            return None;
        };
        cache.get(&normalized).cloned()
    }

    /// Check if a registered function has the no-writes (read-only) flag.
    pub(crate) fn is_function_read_only(&self, function_name: &[u8]) -> bool {
        let name = String::from_utf8_lossy(function_name).to_lowercase();
        let Ok(registry) = self.function_registry.lock() else {
            return false;
        };
        registry
            .functions
            .get(&name)
            .map(|desc| desc.read_only)
            .unwrap_or(false)
    }

    fn xread_waiter_is_ready(
        &self,
        wait_key: &BlockingWaitKey,
        object: Option<&DecodedObjectValue>,
        waits: &[BlockingXreadStreamWait],
    ) -> bool {
        let Some(wait) = waits
            .iter()
            .find(|wait| wait.key.as_slice() == wait_key.key().as_slice())
        else {
            return false;
        };
        let Some(object) = object else {
            return false;
        };
        if object.object_type != STREAM_OBJECT_TYPE_TAG {
            return false;
        }
        let Some(stream) = deserialize_stream_object_payload(&object.payload) else {
            return false;
        };
        match wait.selection {
            BlockingXreadWaitSelection::After(pivot) => stream_has_entry_after_id(&stream, pivot),
            BlockingXreadWaitSelection::LastEntry => !stream.entries.is_empty(),
        }
    }

    fn xreadgroup_waiter_is_ready(
        &self,
        wait_key: &BlockingWaitKey,
        object: Option<&DecodedObjectValue>,
        wait: &BlockingXreadgroupWait,
    ) -> bool {
        if !wait
            .stream_keys
            .iter()
            .any(|key| key.as_slice() == wait_key.key().as_slice())
        {
            return false;
        }
        let Some(object) = object else {
            return true;
        };
        if object.object_type != STREAM_OBJECT_TYPE_TAG {
            return true;
        }
        let Some(stream) = deserialize_stream_object_payload(&object.payload) else {
            return true;
        };
        let Some(group_state) = stream.groups.get(wait.group.as_slice()) else {
            return true;
        };
        if stream_has_entry_after_id(&stream, group_state.last_delivered_id) {
            return true;
        }
        let Some(claim_min_idle_millis) = wait.claim_min_idle_millis else {
            return false;
        };
        let now_millis = current_unix_time_millis().unwrap_or(0);
        // TLA+ : ClaimWaitReady
        stream_group_has_claimable_pending_entry(group_state, now_millis, claim_min_idle_millis)
    }

    fn blocking_wait_object_without_expiring(
        &self,
        wait_key: &BlockingWaitKey,
    ) -> Result<Option<DecodedObjectValue>, RequestExecutionError> {
        self.with_selected_db(wait_key.db(), || {
            if self
                .expiration_unix_millis(DbKeyRef::new(wait_key.db(), wait_key.key().as_slice()))
                .is_some_and(|expiration_unix_millis| {
                    current_unix_time_millis()
                        .is_some_and(|now_unix_millis| expiration_unix_millis <= now_unix_millis)
                })
            {
                return Ok(None);
            }
            self.object_read(DbKeyRef::new(
                current_request_selected_db(),
                wait_key.key().as_slice(),
            ))
        })
    }

    fn blocking_wait_key_ready(
        &self,
        client_id: ClientId,
        wait_key: &BlockingWaitKey,
        blocked_stream_wait_states: &HashMap<ClientId, BlockingStreamWaitState>,
    ) -> Result<bool, RequestExecutionError> {
        let object = self.blocking_wait_object_without_expiring(wait_key)?;
        match wait_key.class() {
            BlockingWaitClass::List => {
                let Some(object) = object.as_ref() else {
                    return Ok(false);
                };
                if object.object_type != LIST_OBJECT_TYPE_TAG {
                    return Ok(false);
                }
                let Some(list) = deserialize_list_object_payload(&object.payload) else {
                    return Ok(true);
                };
                Ok(!list.is_empty())
            }
            BlockingWaitClass::Zset => {
                let Some(object) = object.as_ref() else {
                    return Ok(false);
                };
                if object.object_type != ZSET_OBJECT_TYPE_TAG {
                    return Ok(false);
                }
                let Some(zset) = deserialize_zset_object_payload(&object.payload) else {
                    return Ok(true);
                };
                Ok(!zset.is_empty())
            }
            BlockingWaitClass::Stream => {
                let Some(stream_wait_state) = blocked_stream_wait_states.get(&client_id) else {
                    // A same-key waiter can reach queue front before it has ever executed
                    // once. Let that first execution populate per-client stream wait state.
                    return Ok(true);
                };
                // TLA+ : ProducerAckWaitReady
                let ready = match stream_wait_state {
                    BlockingStreamWaitState::Xread(waits) => {
                        self.xread_waiter_is_ready(wait_key, object.as_ref(), waits)
                    }
                    BlockingStreamWaitState::Xreadgroup(wait) => {
                        self.xreadgroup_waiter_is_ready(wait_key, object.as_ref(), wait)
                    }
                };
                Ok(ready)
            }
        }
    }

    pub(crate) fn blocking_wait_keys_ready(
        &self,
        client_id: ClientId,
        blocking_keys: &[BlockingWaitKey],
    ) -> bool {
        let blocked_stream_wait_states = match self.blocked_stream_wait_states.lock() {
            Ok(states) => states.clone(),
            Err(_) => return false,
        };
        for wait_key in blocking_keys {
            match self.blocking_wait_key_ready(client_id, wait_key, &blocked_stream_wait_states) {
                Ok(true) => return true,
                Ok(false) => continue,
                Err(_) => return true,
            }
        }
        false
    }

    pub(crate) fn has_ready_blocking_waiters(&self) -> bool {
        let queue_fronts = match self.blocking_wait_queues.lock() {
            Ok(queues) => queues
                .iter()
                .filter_map(|(wait_key, queue)| {
                    queue
                        .front()
                        .copied()
                        .map(|client_id| (wait_key.clone(), client_id))
                })
                .collect::<Vec<_>>(),
            Err(_) => return false,
        };
        let blocked_stream_wait_states = match self.blocked_stream_wait_states.lock() {
            Ok(states) => states.clone(),
            Err(_) => return false,
        };
        for (wait_key, front_client_id) in queue_fronts {
            match self.blocking_wait_key_ready(
                front_client_id,
                &wait_key,
                &blocked_stream_wait_states,
            ) {
                Ok(true) => return true,
                Ok(false) => continue,
                Err(_) => return true,
            }
        }
        false
    }

    pub(super) fn force_list_quicklist_encoding(&self, key: &[u8]) {
        if let Ok(mut forced) = self.forced_list_quicklist_keys.lock() {
            forced.insert(RedisKey::from(key));
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

    pub(super) fn string_encoding_is_forced_raw(&self, key: &[u8]) -> bool {
        self.forced_raw_string_keys
            .lock()
            .map(|forced| forced.contains(key))
            .unwrap_or(false)
    }

    pub(super) fn force_raw_string_encoding(&self, key: &[u8]) {
        if let Ok(mut forced) = self.forced_raw_string_keys.lock() {
            forced.insert(RedisKey::from(key));
        }
    }

    pub(super) fn clear_forced_raw_string_encoding(&self, key: &[u8]) {
        if let Ok(mut forced) = self.forced_raw_string_keys.lock() {
            let _ = forced.remove(key);
        }
    }

    pub(super) fn set_encoding_floor(&self, key: &[u8]) -> Option<SetEncodingFloor> {
        let scoped_key = DbScopedKey::new(current_request_selected_db(), key);
        self.forced_set_encoding_floors
            .lock()
            .ok()
            .and_then(|floors| floors.get(&scoped_key).copied())
    }

    pub(super) fn clear_forced_set_encoding_floor(&self, key: &[u8]) {
        let scoped_key = DbScopedKey::new(current_request_selected_db(), key);
        if let Ok(mut floors) = self.forced_set_encoding_floors.lock() {
            let _ = floors.remove(&scoped_key);
        }
    }

    pub(super) fn proto_max_bulk_len(&self) -> usize {
        let Ok(values) = self.config_overrides.lock() else {
            return DEFAULT_PROTO_MAX_BULK_LEN;
        };
        values
            .get(b"proto-max-bulk-len".as_slice())
            .and_then(|value| std::str::from_utf8(value).ok())
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(DEFAULT_PROTO_MAX_BULK_LEN)
    }

    pub(super) fn stream_idmp_duration_seconds(&self) -> u64 {
        let Ok(values) = self.config_overrides.lock() else {
            return 100;
        };
        values
            .get(b"stream-idmp-duration".as_slice())
            .and_then(|value| std::str::from_utf8(value).ok())
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(100)
    }

    pub(super) fn stream_idmp_maxsize(&self) -> u64 {
        let Ok(values) = self.config_overrides.lock() else {
            return 100;
        };
        values
            .get(b"stream-idmp-maxsize".as_slice())
            .and_then(|value| std::str::from_utf8(value).ok())
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(100)
    }

    pub(super) fn stream_node_max_entries(&self) -> usize {
        let Ok(values) = self.config_overrides.lock() else {
            return 100;
        };
        values
            .get(b"stream-node-max-entries".as_slice())
            .and_then(|value| std::str::from_utf8(value).ok())
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(100)
    }

    pub(super) fn list_compress_depth(&self) -> i64 {
        let Ok(values) = self.config_overrides.lock() else {
            return 0;
        };
        values
            .get(b"list-compress-depth".as_slice())
            .and_then(|value| std::str::from_utf8(value).ok())
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(0)
    }

    pub(super) fn ensure_string_length_within_limit(
        &self,
        string_len: usize,
        error: RequestExecutionError,
    ) -> Result<(), RequestExecutionError> {
        if string_len > self.proto_max_bulk_len() {
            return Err(error);
        }
        Ok(())
    }

    fn clear_set_debug_ht_state(&self, key: &[u8]) {
        if let Ok(mut states) = self.set_debug_ht_state.lock() {
            let _ = states.remove(key);
        }
    }

    fn record_set_debug_ht_activity(&self, key: &[u8], member_count: usize) {
        if member_count == 0 {
            self.clear_set_debug_ht_state(key);
            return;
        }

        let bgsave_in_progress = self.rdb_bgsave_in_progress();
        if let Ok(mut states) = self.set_debug_ht_state.lock() {
            let state = states
                .entry(RedisKey::from(key))
                .or_insert_with(|| SetDebugHtState::new(member_count));
            state.update_for_member_count(member_count, bgsave_in_progress);
            state.advance_rehash(member_count, bgsave_in_progress);
        }
    }

    fn set_debug_ht_stats_snapshot(
        &self,
        key: &[u8],
        member_count: usize,
    ) -> SetDebugHtStatsSnapshot {
        self.set_debug_ht_state
            .lock()
            .ok()
            .and_then(|states| states.get(key).copied())
            .unwrap_or_else(|| SetDebugHtState::new(member_count))
            .snapshot()
    }

    pub(super) fn update_set_encoding_floor_for_members(
        &self,
        key: &[u8],
        set: &BTreeSet<Vec<u8>>,
        replace_existing: bool,
    ) {
        let set_max_intset_entries = self.set_max_intset_entries.load(Ordering::Acquire);
        let set_max_listpack_entries = self.set_max_listpack_entries.load(Ordering::Acquire);
        let set_max_listpack_value = self.set_max_listpack_value.load(Ordering::Acquire);
        let candidate = classify_set_encoding_floor(
            set,
            set_max_intset_entries,
            set_max_listpack_entries,
            set_max_listpack_value,
        );

        if let Ok(mut floors) = self.forced_set_encoding_floors.lock() {
            let scoped_key = DbScopedKey::new(current_request_selected_db(), key);
            let previous = if replace_existing {
                None
            } else {
                floors.get(&scoped_key).copied()
            };
            let next = match (previous, candidate) {
                (Some(current), Some(candidate)) => Some(current.max(candidate)),
                (Some(current), None) => Some(current),
                (None, Some(candidate)) => Some(candidate),
                (None, None) => None,
            };
            if let Some(next) = next {
                floors.insert(scoped_key, next);
            } else {
                let _ = floors.remove(&scoped_key);
            }
        }
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

    pub(crate) fn saved_rdb_snapshot(&self) -> Option<Vec<u8>> {
        self.saved_rdb_snapshot
            .lock()
            .ok()
            .and_then(|snapshot| snapshot.clone())
    }

    pub(crate) fn set_saved_rdb_snapshot(&self, snapshot: Vec<u8>) {
        if let Ok(mut current) = self.saved_rdb_snapshot.lock() {
            *current = Some(snapshot);
        }
    }

    pub(super) fn total_error_replies(&self) -> u64 {
        self.total_error_replies.load(Ordering::Relaxed)
    }

    pub(super) fn active_expire_enabled(&self) -> bool {
        self.active_expire_enabled.load(Ordering::Acquire)
    }

    pub(super) fn set_active_expire_enabled(&self, enabled: bool) {
        self.active_expire_enabled.store(enabled, Ordering::Release);
    }

    pub(super) fn allow_access_expired(&self) -> bool {
        self.allow_access_expired.load(Ordering::Acquire)
    }

    pub(super) fn set_allow_access_expired(&self, enabled: bool) {
        self.allow_access_expired.store(enabled, Ordering::Release);
    }

    pub(crate) fn debug_pause_cron(&self) -> bool {
        self.debug_pause_cron.load(Ordering::Acquire)
    }

    pub(super) fn set_debug_pause_cron(&self, paused: bool) {
        self.debug_pause_cron.store(paused, Ordering::Release);
    }

    pub(super) fn set_debug_reply_buffer_peak_reset_time_millis(&self, millis: Option<u64>) {
        if let Some(metrics) = self.server_metrics.get() {
            metrics.set_reply_buffer_peak_reset_time_millis(millis);
        }
    }

    pub(super) fn reset_debug_reply_buffer_peak_reset_time(&self) {
        if let Some(metrics) = self.server_metrics.get() {
            metrics.reset_reply_buffer_peak_reset_time();
        }
    }

    pub(super) fn set_debug_reply_buffer_resizing_enabled(&self, enabled: bool) {
        if let Some(metrics) = self.server_metrics.get() {
            metrics.set_reply_buffer_resizing_enabled(enabled);
        }
    }

    pub(super) fn set_debug_reply_copy_avoidance_enabled(&self, enabled: bool) {
        if let Some(metrics) = self.server_metrics.get() {
            metrics.set_reply_copy_avoidance_enabled(enabled);
        }
    }

    pub(super) fn debug_disable_deny_scripts(&self) -> bool {
        self.debug_disable_deny_scripts.load(Ordering::Acquire)
    }

    pub(super) fn set_debug_disable_deny_scripts(&self, disabled: bool) {
        self.debug_disable_deny_scripts
            .store(disabled, Ordering::Release);
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
    pub fn string_store_shard_index(&self, key: &[u8]) -> ShardIndex {
        self.string_store_shard_index_for_key(key)
    }

    fn execute_in_current_context(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        let mut arg_bytes = Vec::with_capacity(args.len());
        extend_arg_bytes_from_slices(args, &mut arg_bytes);
        self.execute_bytes_in_current_context(&arg_bytes, response_out)
    }

    pub(crate) fn execute_in_db(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
        selected_db: DbName,
    ) -> Result<(), RequestExecutionError> {
        self.execute_with_client_context_in_db(args, response_out, false, None, false, selected_db)
    }

    pub(crate) fn execute_with_client_context_in_db(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
        client_no_touch: bool,
        client_id: Option<ClientId>,
        in_transaction: bool,
        selected_db: DbName,
    ) -> Result<(), RequestExecutionError> {
        let _scope = RequestExecutionContextScope::enter(RequestExecutionContext {
            client_no_touch,
            client_id,
            in_transaction,
            selected_db,
            tracking_reads_enabled: false,
        });
        self.execute_in_current_context(args, response_out)
    }

    pub(crate) fn with_selected_db<T>(&self, selected_db: DbName, f: impl FnOnce() -> T) -> T {
        let previous = REQUEST_EXECUTION_CONTEXT.with(|state| {
            let mut context = state.get();
            let previous = context;
            context.selected_db = selected_db;
            state.set(context);
            previous
        });
        struct Reset(RequestExecutionContext);
        impl Drop for Reset {
            fn drop(&mut self) {
                REQUEST_EXECUTION_CONTEXT.with(|state| state.set(self.0));
            }
        }
        let _reset = Reset(previous);
        f()
    }

    pub(crate) fn execute_with_client_no_touch_in_transaction_in_db(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
        client_no_touch: bool,
        client_id: Option<ClientId>,
        selected_db: DbName,
    ) -> Result<(), RequestExecutionError> {
        self.execute_with_client_context_in_db(
            args,
            response_out,
            client_no_touch,
            client_id,
            true,
            selected_db,
        )
    }

    pub(crate) fn configured_databases(&self) -> usize {
        let Ok(values) = self.config_overrides.lock() else {
            return 1;
        };
        let Some(raw) = values.get(b"databases".as_slice()) else {
            return 1;
        };
        let Ok(text) = std::str::from_utf8(raw) else {
            return 1;
        };
        text.parse::<usize>()
            .ok()
            .filter(|value| *value > 0)
            .unwrap_or(1)
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

    fn execute_bytes_in_current_context(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.is_empty() {
            return Err(RequestExecutionError::UnknownCommand);
        }
        self.executed_command_count.fetch_add(1, Ordering::Relaxed);
        let _current_processor_scope = CurrentProcessorScope::enter(self as *const _);

        let command = dispatch_command_name(args[0]);
        let subcommand = args.get(1).copied();
        let command_mutating = command_is_effectively_mutating(command, subcommand);
        let slowlog_started_at = Instant::now();
        if self.command_rejected_while_script_busy(command, subcommand)? {
            return Err(RequestExecutionError::BusyScript);
        }
        let previous_tracking_reads_enabled = REQUEST_EXECUTION_CONTEXT.with(|state| {
            let mut context = state.get();
            let previous = context.tracking_reads_enabled;
            context.tracking_reads_enabled = !command_mutating;
            state.set(context);
            previous
        });
        begin_tracking_invalidation_collection();
        begin_tracking_read_collection();
        let maxmemory_limit_bytes = self.maxmemory_limit_bytes();
        if maxmemory_limit_bytes > 0 {
            let evicted_any = self
                .evict_keys_to_fit_maxmemory(maxmemory_limit_bytes)
                .unwrap_or(false);
            if evicted_any && let Some(client_id) = current_request_client_id() {
                for pending in self.take_pending_pubsub_messages(client_id) {
                    response_out.extend_from_slice(&pending);
                }
            }
        }

        let execution_result = match command {
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
            CommandId::Digest => self.handle_digest(args, response_out),
            CommandId::Delex => self.handle_delex(args, response_out),
            CommandId::Getset => self.handle_getset(args, response_out),
            CommandId::Getdel => self.handle_getdel(args, response_out),
            CommandId::Msetex => self.handle_msetex(args, response_out),
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
            CommandId::Hsetex => self.handle_hsetex(args, response_out),
            CommandId::Hgetex => self.handle_hgetex(args, response_out),
            CommandId::Hgetdel => self.handle_hgetdel(args, response_out),
            CommandId::Hexpire => self.handle_hexpire(args, response_out),
            CommandId::Hpexpire => self.handle_hpexpire(args, response_out),
            CommandId::Hpexpireat => self.handle_hpexpireat(args, response_out),
            CommandId::Hexpireat => self.handle_hexpireat(args, response_out),
            CommandId::Hpersist => self.handle_hpersist(args, response_out),
            CommandId::Hpttl => self.handle_hpttl(args, response_out),
            CommandId::Httl => self.handle_httl(args, response_out),
            CommandId::Hpexpiretime => self.handle_hpexpiretime(args, response_out),
            CommandId::Hexpiretime => self.handle_hexpiretime(args, response_out),
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
            CommandId::Xdelex => self.handle_xdelex(args, response_out),
            CommandId::Xgroup => self.handle_xgroup(args, response_out),
            CommandId::Xreadgroup => self.handle_xreadgroup(args, response_out),
            CommandId::Xread => self.handle_xread(args, response_out),
            CommandId::Xack => self.handle_xack(args, response_out),
            CommandId::Xackdel => self.handle_xackdel(args, response_out),
            CommandId::Xpending => self.handle_xpending(args, response_out),
            CommandId::Xclaim => self.handle_xclaim(args, response_out),
            CommandId::Xautoclaim => self.handle_xautoclaim(args, response_out),
            CommandId::Xsetid => self.handle_xsetid(args, response_out),
            CommandId::Xinfo => self.handle_xinfo(args, response_out),
            CommandId::Xlen => self.handle_xlen(args, response_out),
            CommandId::Xrange => self.handle_xrange(args, response_out),
            CommandId::Xrevrange => self.handle_xrevrange(args, response_out),
            CommandId::Xtrim => self.handle_xtrim(args, response_out),
            CommandId::Xcfgset => self.handle_xcfgset(args, response_out),
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
        };

        let invalidated_keys = finish_tracking_invalidation_collection_into_parent();
        let deferred_read_keys = finish_tracking_read_collection_into_parent();
        REQUEST_EXECUTION_CONTEXT.with(|state| {
            let mut context = state.get();
            context.tracking_reads_enabled = previous_tracking_reads_enabled;
            state.set(context);
        });
        self.apply_deferred_tracking_reads_for_current_client(&deferred_read_keys);
        self.clear_client_tracking_caching_override_for_current_client();
        if execution_result.is_ok() && !invalidated_keys.is_empty() {
            self.emit_tracking_invalidations_for_keys(&invalidated_keys);
        }
        if !is_slowlog_blocking_command(command) {
            self.record_slowlog_command(
                args,
                command,
                slowlog_started_at.elapsed(),
                current_request_client_id(),
            );
        }
        execution_result
    }
}

fn pubsub_total_subscriptions_for_client(state: &PubSubState, client_id: ClientId) -> usize {
    let channels = state
        .client_channels
        .get(&client_id)
        .map_or(0, HashSet::len);
    let patterns = state
        .client_patterns
        .get(&client_id)
        .map_or(0, HashSet::len);
    channels.saturating_add(patterns)
}

fn tracking_groups_for_key(config: &ClientTrackingConfig, key: &[u8]) -> Vec<Vec<u8>> {
    if !config.bcast {
        return vec![Vec::new()];
    }
    let mut groups = Vec::new();
    for prefix in &config.prefixes {
        if key.starts_with(prefix.as_slice()) {
            groups.push(prefix.clone());
        }
    }
    groups
}

fn remove_all_pubsub_client_subscriptions(
    state: &mut PubSubState,
    client_id: ClientId,
    target_kind: PubSubTargetKind,
) {
    let targets = match target_kind {
        PubSubTargetKind::Channel => state.client_channels.remove(&client_id),
        PubSubTargetKind::Pattern => state.client_patterns.remove(&client_id),
    };
    let Some(targets) = targets else {
        return;
    };
    for target in targets {
        match target_kind {
            PubSubTargetKind::Channel => {
                remove_pubsub_subscriber(&mut state.channel_subscribers, &target, client_id)
            }
            PubSubTargetKind::Pattern => {
                remove_pubsub_subscriber(&mut state.pattern_subscribers, &target, client_id)
            }
        }
    }
}

fn collect_pubsub_unsubscribe_targets(
    existing_subscriptions: Option<&HashSet<Vec<u8>>>,
    requested_targets: &[&[u8]],
) -> Vec<Vec<u8>> {
    if requested_targets.is_empty() {
        let mut targets = existing_subscriptions
            .map(|subscriptions| subscriptions.iter().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        targets.sort();
        return targets;
    }
    requested_targets
        .iter()
        .map(|target| (*target).to_vec())
        .collect()
}

fn remove_pubsub_client_subscription(
    client_subscriptions: &mut HashMap<ClientId, HashSet<Vec<u8>>>,
    client_id: ClientId,
    target: &[u8],
) -> bool {
    let removed = if let Some(subscriptions) = client_subscriptions.get_mut(&client_id) {
        subscriptions.remove(target)
    } else {
        false
    };

    if matches!(
        client_subscriptions.get(&client_id),
        Some(subscriptions) if subscriptions.is_empty()
    ) {
        client_subscriptions.remove(&client_id);
    }

    removed
}

fn remove_pubsub_subscriber(
    subscriber_index: &mut HashMap<Vec<u8>, HashSet<ClientId>>,
    target: &[u8],
    client_id: ClientId,
) {
    if let Some(subscribers) = subscriber_index.get_mut(target) {
        subscribers.remove(&client_id);
        if subscribers.is_empty() {
            subscriber_index.remove(target);
        }
    }
}

fn collect_pubsub_channel_targets(state: &PubSubState, channel: &[u8]) -> Vec<ClientId> {
    state
        .channel_subscribers
        .get(channel)
        .map(|subscribers| subscribers.iter().copied().collect::<Vec<_>>())
        .unwrap_or_default()
}

fn collect_pubsub_pattern_targets(state: &PubSubState, channel: &[u8]) -> Vec<(ClientId, Vec<u8>)> {
    let mut pattern_targets = Vec::new();
    for (pattern, subscribers) in &state.pattern_subscribers {
        if self::server_commands::redis_glob_match(
            pattern,
            channel,
            self::server_commands::CaseSensitivity::Sensitive,
            0,
        ) {
            for subscriber in subscribers {
                pattern_targets.push((*subscriber, pattern.clone()));
            }
        }
    }
    pattern_targets
}

fn saturating_usize_to_i64(value: usize) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn encode_pubsub_message_frame(
    kind: &[u8],
    pattern: Option<&[u8]>,
    channel: &[u8],
    payload: &[u8],
    resp3: bool,
) -> Vec<u8> {
    let mut frame = Vec::new();
    match pattern {
        Some(pattern) => {
            if resp3 {
                append_push_length(&mut frame, 4);
            } else {
                frame.extend_from_slice(b"*4\r\n");
            }
            append_bulk_string(&mut frame, kind);
            append_bulk_string(&mut frame, pattern);
            append_bulk_string(&mut frame, channel);
            append_bulk_string(&mut frame, payload);
        }
        None => {
            if resp3 {
                append_push_length(&mut frame, 3);
            } else {
                frame.extend_from_slice(b"*3\r\n");
            }
            append_bulk_string(&mut frame, kind);
            append_bulk_string(&mut frame, channel);
            append_bulk_string(&mut frame, payload);
        }
    }
    frame
}

fn encode_tracking_invalidation_payload(keys: &[RedisKey]) -> Vec<u8> {
    let mut payload_len = 0usize;
    for key in keys {
        payload_len = payload_len.saturating_add(key.len());
    }
    payload_len = payload_len.saturating_add(keys.len().saturating_sub(1));
    let mut payload = Vec::with_capacity(payload_len);
    for (index, key) in keys.iter().enumerate() {
        if index > 0 {
            payload.push(b' ');
        }
        payload.extend_from_slice(key);
    }
    payload
}

fn encode_tracking_push_frame(payload: &[u8]) -> Vec<u8> {
    let mut frame = Vec::new();
    append_push_length(&mut frame, 2);
    append_bulk_string(&mut frame, b"invalidate");
    append_bulk_string(&mut frame, payload);
    frame
}

fn encode_tracking_redir_broken_push_frame() -> Vec<u8> {
    let mut frame = Vec::new();
    append_push_length(&mut frame, 1);
    append_bulk_string(&mut frame, b"tracking-redir-broken");
    frame
}

fn pending_message_is_tracking_message(message: &[u8]) -> bool {
    const RESP2_TRACKING_PREFIX: &[u8] = b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n";
    const RESP3_INVALIDATE_PREFIX: &[u8] = b">2\r\n$10\r\ninvalidate\r\n";
    const RESP3_BROKEN_REDIRECT_PREFIX: &[u8] = b">1\r\n$21\r\ntracking-redir-broken\r\n";

    message.starts_with(RESP2_TRACKING_PREFIX)
        || message.starts_with(RESP3_INVALIDATE_PREFIX)
        || message.starts_with(RESP3_BROKEN_REDIRECT_PREFIX)
}

/// Parse a notify-keyspace-events flag string (e.g. "KEA", "Kx$") into a bitmask.
/// Returns `None` if the string contains an invalid character.
fn keyspace_events_string_to_flags(classes: &[u8]) -> Option<u32> {
    let mut flags = 0u32;
    for &c in classes {
        match c {
            b'A' => flags |= NOTIFY_ALL,
            b'g' => flags |= NOTIFY_GENERIC,
            b'$' => flags |= NOTIFY_STRING,
            b'l' => flags |= NOTIFY_LIST,
            b's' => flags |= NOTIFY_SET,
            b'h' => flags |= NOTIFY_HASH,
            b'z' => flags |= NOTIFY_ZSET,
            b'x' => flags |= NOTIFY_EXPIRED,
            b'e' => flags |= NOTIFY_EVICTED,
            b'K' => flags |= NOTIFY_KEYSPACE,
            b'E' => flags |= NOTIFY_KEYEVENT,
            b't' => flags |= NOTIFY_STREAM,
            b'm' => flags |= NOTIFY_KEY_MISS,
            b'd' => flags |= NOTIFY_MODULE,
            b'n' => flags |= NOTIFY_NEW,
            b'o' => flags |= NOTIFY_OVERWRITTEN,
            b'c' => flags |= NOTIFY_TYPE_CHANGED,
            _ => return None,
        }
    }
    Some(flags)
}

/// Convert a keyspace events bitmask back to its string representation.
fn keyspace_events_flags_to_string(flags: u32) -> Vec<u8> {
    let mut res = Vec::with_capacity(16);
    if flags & NOTIFY_ALL == NOTIFY_ALL {
        res.push(b'A');
    } else {
        if flags & NOTIFY_GENERIC != 0 {
            res.push(b'g');
        }
        if flags & NOTIFY_STRING != 0 {
            res.push(b'$');
        }
        if flags & NOTIFY_LIST != 0 {
            res.push(b'l');
        }
        if flags & NOTIFY_SET != 0 {
            res.push(b's');
        }
        if flags & NOTIFY_HASH != 0 {
            res.push(b'h');
        }
        if flags & NOTIFY_ZSET != 0 {
            res.push(b'z');
        }
        if flags & NOTIFY_EXPIRED != 0 {
            res.push(b'x');
        }
        if flags & NOTIFY_EVICTED != 0 {
            res.push(b'e');
        }
        if flags & NOTIFY_STREAM != 0 {
            res.push(b't');
        }
        if flags & NOTIFY_MODULE != 0 {
            res.push(b'd');
        }
        if flags & NOTIFY_NEW != 0 {
            res.push(b'n');
        }
        if flags & NOTIFY_OVERWRITTEN != 0 {
            res.push(b'o');
        }
        if flags & NOTIFY_TYPE_CHANGED != 0 {
            res.push(b'c');
        }
    }
    if flags & NOTIFY_KEYSPACE != 0 {
        res.push(b'K');
    }
    if flags & NOTIFY_KEYEVENT != 0 {
        res.push(b'E');
    }
    if flags & NOTIFY_KEY_MISS != 0 {
        res.push(b'm');
    }
    res
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

fn watch_version_slot(db: DbName, key: &[u8]) -> usize {
    let mut hash = fnv1a_hash64(key);
    hash ^= usize::from(db) as u64;
    hash = hash.wrapping_mul(0x100000001b3);
    (hash as usize) & WATCH_VERSION_MAP_MASK
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StringWriteConditionKind {
    Nx,
    Xx,
    IfEq,
    IfNe,
    IfDigestEq,
    IfDigestNe,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StringWriteCondition {
    kind: StringWriteConditionKind,
    match_value: Vec<u8>,
}

#[derive(Debug, Clone, Default)]
struct SetOptions {
    condition: Option<StringWriteCondition>,
    expiration: Option<ExpirationMetadata>,
    keep_ttl: bool,
    return_old_value: bool,
}

fn parse_set_options(args: &[&[u8]]) -> Result<SetOptions, RequestExecutionError> {
    parse_string_write_options(args, 3, true, true)
}

#[derive(Debug, Clone, Default)]
struct MsetexOptions {
    only_if_absent: bool,
    only_if_present: bool,
    expiration: Option<ExpirationMetadata>,
    keep_ttl: bool,
}

fn parse_msetex_options(
    args: &[&[u8]],
    start_index: usize,
) -> Result<MsetexOptions, RequestExecutionError> {
    let parsed = parse_string_write_options(args, start_index, false, false)?;
    let mut options = MsetexOptions {
        expiration: parsed.expiration,
        keep_ttl: parsed.keep_ttl,
        ..Default::default()
    };
    match parsed.condition {
        None => {}
        Some(StringWriteCondition {
            kind: StringWriteConditionKind::Nx,
            ..
        }) => {
            options.only_if_absent = true;
        }
        Some(StringWriteCondition {
            kind: StringWriteConditionKind::Xx,
            ..
        }) => {
            options.only_if_present = true;
        }
        Some(StringWriteCondition {
            kind: StringWriteConditionKind::IfEq,
            ..
        }) => {
            return Err(RequestExecutionError::SyntaxError);
        }
        Some(StringWriteCondition {
            kind: StringWriteConditionKind::IfNe,
            ..
        }) => {
            return Err(RequestExecutionError::SyntaxError);
        }
        Some(StringWriteCondition {
            kind: StringWriteConditionKind::IfDigestEq | StringWriteConditionKind::IfDigestNe,
            ..
        }) => {
            return Err(RequestExecutionError::SyntaxError);
        }
    }
    Ok(options)
}

fn parse_string_write_options(
    args: &[&[u8]],
    start_index: usize,
    allow_get: bool,
    allow_compare_conditions: bool,
) -> Result<SetOptions, RequestExecutionError> {
    let mut options = SetOptions::default();
    let mut index = start_index;

    while index < args.len() {
        let option = args[index];

        if ascii_eq_ignore_case(option, b"NX") {
            if options.condition.is_some() {
                return Err(RequestExecutionError::SyntaxError);
            }
            options.condition = Some(StringWriteCondition {
                kind: StringWriteConditionKind::Nx,
                match_value: Vec::new(),
            });
            index += 1;
            continue;
        }

        if ascii_eq_ignore_case(option, b"XX") {
            if options.condition.is_some() {
                return Err(RequestExecutionError::SyntaxError);
            }
            options.condition = Some(StringWriteCondition {
                kind: StringWriteConditionKind::Xx,
                match_value: Vec::new(),
            });
            index += 1;
            continue;
        }

        if ascii_eq_ignore_case(option, b"GET") {
            if !allow_get {
                return Err(RequestExecutionError::SyntaxError);
            }
            if options.return_old_value {
                return Err(RequestExecutionError::SyntaxError);
            }
            options.return_old_value = true;
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

        if allow_compare_conditions
            && (ascii_eq_ignore_case(option, b"IFEQ")
                || ascii_eq_ignore_case(option, b"IFNE")
                || ascii_eq_ignore_case(option, b"IFDEQ")
                || ascii_eq_ignore_case(option, b"IFDNE"))
        {
            if options.condition.is_some() || index + 1 >= args.len() {
                return Err(RequestExecutionError::SyntaxError);
            }
            let kind = if ascii_eq_ignore_case(option, b"IFEQ") {
                StringWriteConditionKind::IfEq
            } else if ascii_eq_ignore_case(option, b"IFNE") {
                StringWriteConditionKind::IfNe
            } else if ascii_eq_ignore_case(option, b"IFDEQ") {
                StringWriteConditionKind::IfDigestEq
            } else {
                StringWriteConditionKind::IfDigestNe
            };
            options.condition = Some(StringWriteCondition {
                kind,
                match_value: args[index + 1].to_vec(),
            });
            index += 2;
            continue;
        }

        return Err(RequestExecutionError::SyntaxError);
    }

    Ok(options)
}

fn classify_set_encoding_floor(
    set: &BTreeSet<Vec<u8>>,
    set_max_intset_entries: usize,
    set_max_listpack_entries: usize,
    set_max_listpack_value: usize,
) -> Option<SetEncodingFloor> {
    let intset_compatible = set.iter().all(|member| parse_i64_ascii(member).is_some())
        && set.len() <= set_max_intset_entries;
    if intset_compatible {
        return None;
    }

    let listpack_compatible = set.len() <= set_max_listpack_entries
        && set
            .iter()
            .all(|member| member.len() <= set_max_listpack_value);
    if listpack_compatible {
        Some(SetEncodingFloor::Listpack)
    } else {
        Some(SetEncodingFloor::Hashtable)
    }
}

fn current_unix_time_millis() -> Option<u64> {
    Some(current_unix_time_micros()? / 1000)
}

fn current_unix_time_micros() -> Option<u64> {
    if let Some(snapshot) = current_script_time_snapshot() {
        return Some(snapshot.unix_micros);
    }
    let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?;
    u64::try_from(now.as_micros()).ok()
}

fn current_instant() -> Instant {
    current_script_time_snapshot()
        .map(|snapshot| snapshot.instant)
        .unwrap_or_else(Instant::now)
}

fn current_script_time_snapshot() -> Option<ScriptTimeSnapshot> {
    let processor_ptr = CURRENT_PROCESSOR_PTR.with(|state| state.get());
    if processor_ptr.is_null() {
        return None;
    }
    // SAFETY: `CURRENT_PROCESSOR_PTR` is set only for the dynamic extent of
    // `RequestProcessor::execute_bytes`, while the borrowed processor remains
    // alive on that stack frame.
    let processor = unsafe { &*processor_ptr };
    processor.current_script_time_snapshot()
}

fn allow_expired_data_access() -> bool {
    EXPIRED_DATA_ACCESS_OVERRIDE.with(|state| state.get())
}

fn with_expired_data_access<T>(enabled: bool, f: impl FnOnce() -> T) -> T {
    let previous = EXPIRED_DATA_ACCESS_OVERRIDE.with(|state| {
        let previous = state.get();
        state.set(enabled);
        previous
    });
    let output = f();
    EXPIRED_DATA_ACCESS_OVERRIDE.with(|state| state.set(previous));
    output
}

fn normalize_ascii_lower(value: &[u8]) -> Vec<u8> {
    value
        .iter()
        .map(|byte| byte.to_ascii_lowercase())
        .collect::<Vec<u8>>()
}

fn normalize_ascii_upper(value: &[u8]) -> Vec<u8> {
    value
        .iter()
        .map(|byte| byte.to_ascii_uppercase())
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
    let now = current_instant();
    let now_unix_millis = current_unix_time_millis()?;
    if unix_millis <= now_unix_millis {
        return Some(now);
    }
    let delta_millis = unix_millis.checked_sub(now_unix_millis)?;
    now.checked_add(Duration::from_millis(delta_millis))
}

#[cfg(test)]
mod tests;
