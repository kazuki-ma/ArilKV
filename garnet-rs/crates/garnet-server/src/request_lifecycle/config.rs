use tsavorite::TsavoriteKvConfig;

use super::DEFAULT_SERVER_HASH_INDEX_SIZE_BITS;
use super::DEFAULT_STRING_STORE_SHARDS;
use super::GARNET_HASH_INDEX_SIZE_BITS_ENV;
use super::GARNET_INTEROP_FORCE_RESP3_ZSET_PAIRS_ENV;
use super::GARNET_MAX_IN_MEMORY_PAGES_ENV;
use super::GARNET_PAGE_SIZE_BITS_ENV;
use super::GARNET_SCRIPTING_CACHE_MAX_ENTRIES_ENV;
use super::GARNET_SCRIPTING_ENABLED_ENV;
use super::GARNET_SCRIPTING_MAX_EXECUTION_MILLIS_ENV;
use super::GARNET_SCRIPTING_MAX_MEMORY_BYTES_ENV;
use super::GARNET_SCRIPTING_MAX_SCRIPT_BYTES_ENV;
use super::GARNET_STRING_OWNER_THREADS_ENV;
use super::GARNET_STRING_STORE_SHARDS_ENV;
use super::SINGLE_OWNER_THREAD_STRING_STORE_SHARDS;
use super::ScriptingRuntimeConfig;

pub(super) fn tsavorite_config_from_env() -> TsavoriteKvConfig {
    tsavorite_config_from_values(
        parse_env_u8(GARNET_HASH_INDEX_SIZE_BITS_ENV),
        parse_env_u8(GARNET_PAGE_SIZE_BITS_ENV),
        parse_env_usize(GARNET_MAX_IN_MEMORY_PAGES_ENV),
    )
}

pub(super) fn tsavorite_config_from_values(
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

pub(super) fn string_store_shard_count_from_env() -> usize {
    let explicit_shards =
        parse_env_usize(GARNET_STRING_STORE_SHARDS_ENV).filter(|count| *count > 0);
    let owner_threads = parse_env_usize(GARNET_STRING_OWNER_THREADS_ENV).filter(|count| *count > 0);
    string_store_shard_count_from_values(explicit_shards, owner_threads)
}

pub(super) fn scripting_enabled_from_env() -> bool {
    parse_env_bool(GARNET_SCRIPTING_ENABLED_ENV).unwrap_or(false)
}

pub(super) fn scripting_runtime_config_from_env() -> ScriptingRuntimeConfig {
    scripting_runtime_config_from_values(
        parse_env_usize(GARNET_SCRIPTING_MAX_SCRIPT_BYTES_ENV),
        parse_env_usize(GARNET_SCRIPTING_CACHE_MAX_ENTRIES_ENV),
        parse_env_usize(GARNET_SCRIPTING_MAX_MEMORY_BYTES_ENV),
        parse_env_u64(GARNET_SCRIPTING_MAX_EXECUTION_MILLIS_ENV),
    )
}

pub(super) fn interop_force_resp3_zset_pairs_from_env() -> bool {
    parse_env_bool(GARNET_INTEROP_FORCE_RESP3_ZSET_PAIRS_ENV).unwrap_or(false)
}

pub(super) fn scripting_runtime_config_from_values(
    max_script_bytes: Option<usize>,
    cache_max_entries: Option<usize>,
    max_memory_bytes: Option<usize>,
    max_execution_millis: Option<u64>,
) -> ScriptingRuntimeConfig {
    ScriptingRuntimeConfig {
        max_script_bytes: max_script_bytes.unwrap_or(0),
        cache_max_entries: cache_max_entries.unwrap_or(0),
        max_memory_bytes: max_memory_bytes.unwrap_or(0),
        max_execution_millis: max_execution_millis.unwrap_or(0),
    }
}

pub(super) fn string_store_shard_count_from_values(
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

pub(super) fn scale_hash_index_bits_for_shards(base_bits: u8, shard_count: usize) -> u8 {
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

fn parse_env_u64(key: &str) -> Option<u64> {
    std::env::var(key).ok()?.parse::<u64>().ok()
}

fn parse_env_bool(key: &str) -> Option<bool> {
    let value = std::env::var(key).ok()?;
    match value.as_str() {
        "1" | "true" | "TRUE" | "True" | "yes" | "YES" | "on" | "ON" => Some(true),
        "0" | "false" | "FALSE" | "False" | "no" | "NO" | "off" | "OFF" => Some(false),
        _ => None,
    }
}
