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
mod string_store;
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
    object_stores: Vec<OrderedMutex<TsavoriteKV<Vec<u8>, Vec<u8>>>>,
    string_expirations: Vec<OrderedMutex<HashMap<Vec<u8>, Instant>>>,
    string_expiration_counts: Vec<AtomicUsize>,
    string_key_registries: Vec<OrderedMutex<HashSet<Vec<u8>>>>,
    object_key_registries: Vec<OrderedMutex<HashSet<Vec<u8>>>>,
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
        let mut object_store_config = store_config;
        object_store_config.hash_index_size_bits =
            scale_hash_index_bits_for_shards(store_config.hash_index_size_bits, store_shard_count);
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
