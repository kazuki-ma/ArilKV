use tsavorite::TsavoriteKvConfig;

use super::{
    DEFAULT_SERVER_HASH_INDEX_SIZE_BITS, DEFAULT_STRING_STORE_SHARDS,
    GARNET_HASH_INDEX_SIZE_BITS_ENV, GARNET_MAX_IN_MEMORY_PAGES_ENV, GARNET_PAGE_SIZE_BITS_ENV,
    GARNET_STRING_OWNER_THREADS_ENV, GARNET_STRING_STORE_SHARDS_ENV,
    SINGLE_OWNER_THREAD_STRING_STORE_SHARDS,
};

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
