use super::*;
use crate::debug_concurrency;
use crate::testkit::CommandHarnessError;
use crate::testkit::assert_command_error;
use crate::testkit::assert_command_integer;
use crate::testkit::assert_command_response;
use crate::testkit::execute_command_line;
use garnet_common::parse_resp_command_arg_slices;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tsavorite::DeleteOperationError;
use tsavorite::ISessionFunctions;
use tsavorite::PageManagerError;
use tsavorite::PageResidencyError;
use tsavorite::ReadOperationError;
use tsavorite::RecordInfo;
use tsavorite::RmwOperationError;
use tsavorite::TsavoriteKvConfig;
use tsavorite::UpsertOperationError;
use tsavorite::WriteReason;

fn parse_integer_response(response: &[u8]) -> i64 {
    assert!(response.len() >= 4);
    assert_eq!(response[0], b':');
    assert!(response.ends_with(b"\r\n"));
    core::str::from_utf8(&response[1..response.len() - 2])
        .unwrap()
        .parse::<i64>()
        .unwrap()
}

fn parse_bulk_payload(response: &[u8]) -> Option<Vec<u8>> {
    if response == b"$-1\r\n" {
        return None;
    }
    assert!(response.starts_with(b"$"));
    let mut index = 1usize;
    while index + 1 < response.len() {
        if response[index] == b'\r' && response[index + 1] == b'\n' {
            break;
        }
        index += 1;
    }
    let len = core::str::from_utf8(&response[1..index])
        .unwrap()
        .parse::<usize>()
        .unwrap();
    let payload_start = index + 2;
    let payload_end = payload_start + len;
    Some(response[payload_start..payload_end].to_vec())
}

fn parse_bulk_array_payloads(response: &[u8]) -> Vec<Vec<u8>> {
    assert!(!response.is_empty());
    assert_eq!(response[0], b'*');

    let mut index = 1usize;
    while index + 1 < response.len() {
        if response[index] == b'\r' && response[index + 1] == b'\n' {
            break;
        }
        index += 1;
    }
    let array_len = core::str::from_utf8(&response[1..index])
        .unwrap()
        .parse::<usize>()
        .unwrap();
    index += 2;

    let mut out = Vec::with_capacity(array_len);
    for _ in 0..array_len {
        assert_eq!(response[index], b'$');
        let len_start = index + 1;
        index = len_start;
        while index + 1 < response.len() {
            if response[index] == b'\r' && response[index + 1] == b'\n' {
                break;
            }
            index += 1;
        }
        let bulk_len = core::str::from_utf8(&response[len_start..index])
            .unwrap()
            .parse::<usize>()
            .unwrap();
        index += 2;
        let payload_end = index + bulk_len;
        out.push(response[index..payload_end].to_vec());
        index = payload_end + 2;
    }
    out
}

fn parse_resp_array_len(response: &[u8], index: &mut usize) -> usize {
    assert_eq!(response[*index], b'*');
    *index += 1;
    let start = *index;
    while *index + 1 < response.len() {
        if response[*index] == b'\r' && response[*index + 1] == b'\n' {
            break;
        }
        *index += 1;
    }
    let value = core::str::from_utf8(&response[start..*index])
        .unwrap()
        .parse::<usize>()
        .unwrap();
    *index += 2;
    value
}

fn parse_resp_bulk_bytes(response: &[u8], index: &mut usize) -> Vec<u8> {
    assert_eq!(response[*index], b'$');
    *index += 1;
    let start = *index;
    while *index + 1 < response.len() {
        if response[*index] == b'\r' && response[*index + 1] == b'\n' {
            break;
        }
        *index += 1;
    }
    let len = core::str::from_utf8(&response[start..*index])
        .unwrap()
        .parse::<usize>()
        .unwrap();
    *index += 2;
    let payload_end = *index + len;
    let payload = response[*index..payload_end].to_vec();
    *index = payload_end + 2;
    payload
}

fn parse_command_getkeysandflags_response(response: &[u8]) -> Vec<(Vec<u8>, Vec<Vec<u8>>)> {
    let mut index = 0usize;
    let outer_len = parse_resp_array_len(response, &mut index);
    let mut out = Vec::with_capacity(outer_len);
    for _ in 0..outer_len {
        assert_eq!(parse_resp_array_len(response, &mut index), 2);
        let key = parse_resp_bulk_bytes(response, &mut index);
        let flags_len = parse_resp_array_len(response, &mut index);
        let mut flags = Vec::with_capacity(flags_len);
        for _ in 0..flags_len {
            flags.push(parse_resp_bulk_bytes(response, &mut index));
        }
        out.push((key, flags));
    }
    assert_eq!(index, response.len());
    out
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

fn arg_bytes_from_slices(args: &[ArgSlice]) -> Vec<&[u8]> {
    let mut out = Vec::with_capacity(args.len());
    for arg in args {
        // SAFETY: test frames own backing storage for the full assertion scope.
        out.push(unsafe { arg.as_slice() });
    }
    out
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
    assert_command_response(&processor, "SET alpha one", b"+OK\r\n");
    assert_command_response(&processor, "GET alpha", b"$3\r\none\r\n");
    assert_command_integer(&processor, "DEL alpha", 1);
    assert_command_response(&processor, "GET alpha", b"$-1\r\n");
}

#[test]
fn parse_scan_match_count_options_supports_match_and_count() {
    let frame = encode_resp(&[b"SSCAN", b"k", b"0", b"COUNT", b"5", b"MATCH", b"a*"]);
    let mut args = [ArgSlice::EMPTY; 16];
    let meta = parse_resp_command_arg_slices(&frame, &mut args).unwrap();
    let arg_bytes = arg_bytes_from_slices(&args[..meta.argument_count]);
    let options = parse_scan_match_count_options(&arg_bytes, 3).unwrap();
    assert_eq!(options.pattern, Some(b"a*".as_slice()));
    assert_eq!(options.count, 5);
}

#[test]
fn parse_scan_match_count_options_rejects_zero_count() {
    let frame = encode_resp(&[b"SSCAN", b"k", b"0", b"COUNT", b"0"]);
    let mut args = [ArgSlice::EMPTY; 16];
    let meta = parse_resp_command_arg_slices(&frame, &mut args).unwrap();
    let arg_bytes = arg_bytes_from_slices(&args[..meta.argument_count]);
    let error = parse_scan_match_count_options(&arg_bytes, 3).expect_err("must fail");
    assert_eq!(error, RequestExecutionError::ValueOutOfRange);
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

    assert!(
        processor.string_key_registries[shard_a]
            .lock()
            .expect("key registry mutex poisoned")
            .contains(&key_a)
    );
    assert!(
        processor.string_key_registries[shard_b]
            .lock()
            .expect("key registry mutex poisoned")
            .contains(&key_b)
    );
    assert!(
        !processor.string_key_registries[shard_a]
            .lock()
            .expect("key registry mutex poisoned")
            .contains(&key_b)
    );
    assert!(
        !processor.string_key_registries[shard_b]
            .lock()
            .expect("key registry mutex poisoned")
            .contains(&key_a)
    );
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
fn set_and_get_supports_50kb_key_for_keyspace_regression() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 3];
    let key = vec![b'a'; 50_000];

    let mut frame_set = format!("*3\r\n$3\r\nSET\r\n${}\r\n", key.len()).into_bytes();
    frame_set.extend_from_slice(&key);
    frame_set.extend_from_slice(b"\r\n$1\r\n1\r\n");

    let meta = parse_resp_command_arg_slices(&frame_set, &mut args).unwrap();
    let mut response = Vec::new();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    let mut frame_get = format!("*2\r\n$3\r\nGET\r\n${}\r\n", key.len()).into_bytes();
    frame_get.extend_from_slice(&key);
    frame_get.extend_from_slice(b"\r\n");

    let meta = parse_resp_command_arg_slices(&frame_get, &mut args).unwrap();
    response.clear();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"$1\r\n1\r\n");
}

#[test]
fn object_store_roundtrip_respects_redis_type_semantics() {
    let processor = RequestProcessor::new().unwrap();

    processor.object_upsert(b"obj", 3, b"payload").unwrap();
    let (object_type, payload) = processor.object_read(b"obj").unwrap().unwrap();
    assert_eq!(object_type, 3);
    assert_eq!(payload, b"payload");

    let mut args = [ArgSlice::EMPTY; 3];
    let mut response = Vec::new();
    let get = b"*2\r\n$3\r\nGET\r\n$3\r\nobj\r\n";
    let meta = parse_resp_command_arg_slices(get, &mut args).unwrap();
    let err = processor
        .execute(&args[..meta.argument_count], &mut response)
        .err()
        .unwrap();
    err.append_resp_error(&mut response);
    assert_eq!(
        response,
        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
    );

    response.clear();
    let set = b"*3\r\n$3\r\nSET\r\n$3\r\nobj\r\n$3\r\nstr\r\n";
    let meta = parse_resp_command_arg_slices(set, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

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
fn additional_hash_commands_cover_common_redis_semantics() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 16];
    let mut response = Vec::new();

    let hmset =
        b"*6\r\n$5\r\nHMSET\r\n$3\r\nkey\r\n$2\r\nf1\r\n$2\r\nv1\r\n$2\r\nf2\r\n$2\r\nv2\r\n";
    let meta = parse_resp_command_arg_slices(hmset, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let hlen = b"*2\r\n$4\r\nHLEN\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(hlen, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":2\r\n");

    response.clear();
    let hmget = b"*4\r\n$5\r\nHMGET\r\n$3\r\nkey\r\n$2\r\nf1\r\n$7\r\nmissing\r\n";
    let meta = parse_resp_command_arg_slices(hmget, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"*2\r\n$2\r\nv1\r\n$-1\r\n");

    response.clear();
    let hsetnx_exists = b"*4\r\n$6\r\nHSETNX\r\n$3\r\nkey\r\n$2\r\nf1\r\n$2\r\nzz\r\n";
    let meta = parse_resp_command_arg_slices(hsetnx_exists, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let hsetnx_insert = b"*4\r\n$6\r\nHSETNX\r\n$3\r\nkey\r\n$2\r\nf3\r\n$2\r\nv3\r\n";
    let meta = parse_resp_command_arg_slices(hsetnx_insert, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let hexists = b"*3\r\n$7\r\nHEXISTS\r\n$3\r\nkey\r\n$2\r\nf3\r\n";
    let meta = parse_resp_command_arg_slices(hexists, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let hkeys = b"*2\r\n$5\r\nHKEYS\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(hkeys, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"*3\r\n$2\r\nf1\r\n$2\r\nf2\r\n$2\r\nf3\r\n");

    response.clear();
    let hvals = b"*2\r\n$5\r\nHVALS\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(hvals, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"*3\r\n$2\r\nv1\r\n$2\r\nv2\r\n$2\r\nv3\r\n");

    response.clear();
    let hstrlen = b"*3\r\n$7\r\nHSTRLEN\r\n$3\r\nkey\r\n$2\r\nf2\r\n";
    let meta = parse_resp_command_arg_slices(hstrlen, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":2\r\n");

    response.clear();
    let hincrby = b"*4\r\n$7\r\nHINCRBY\r\n$3\r\nkey\r\n$1\r\nn\r\n$1\r\n2\r\n";
    let meta = parse_resp_command_arg_slices(hincrby, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":2\r\n");

    response.clear();
    let hincrby_negative = b"*4\r\n$7\r\nHINCRBY\r\n$3\r\nkey\r\n$1\r\nn\r\n$2\r\n-5\r\n";
    let meta = parse_resp_command_arg_slices(hincrby_negative, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":-3\r\n");

    response.clear();
    let hset_max =
        b"*4\r\n$4\r\nHSET\r\n$3\r\nkey\r\n$7\r\novrflow\r\n$19\r\n9223372036854775807\r\n";
    let meta = parse_resp_command_arg_slices(hset_max, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let hincrby_overflow = b"*4\r\n$7\r\nHINCRBY\r\n$3\r\nkey\r\n$7\r\novrflow\r\n$1\r\n1\r\n";
    let meta = parse_resp_command_arg_slices(hincrby_overflow, &mut args).unwrap();
    let err = processor
        .execute(&args[..meta.argument_count], &mut response)
        .err()
        .unwrap();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR increment or decrement would overflow\r\n");

    response.clear();
    let hincrbyfloat = b"*4\r\n$12\r\nHINCRBYFLOAT\r\n$3\r\nkey\r\n$1\r\nf\r\n$3\r\n2.5\r\n";
    let meta = parse_resp_command_arg_slices(hincrbyfloat, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"$3\r\n2.5\r\n");

    response.clear();
    let hincrbyfloat_again = b"*4\r\n$12\r\nHINCRBYFLOAT\r\n$3\r\nkey\r\n$1\r\nf\r\n$3\r\n3.5\r\n";
    let meta = parse_resp_command_arg_slices(hincrbyfloat_again, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"$1\r\n6\r\n");
}

#[test]
fn hrandfield_supports_count_withvalues_and_resp3_shape() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 16];
    let mut response = Vec::new();

    let hset = b"*8\r\n$4\r\nHSET\r\n$3\r\nkey\r\n$2\r\nf1\r\n$1\r\n1\r\n$2\r\nf2\r\n$1\r\n2\r\n$2\r\nf3\r\n$1\r\n3\r\n";
    let meta = parse_resp_command_arg_slices(hset, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":3\r\n");

    response.clear();
    let hrand_single = b"*2\r\n$10\r\nHRANDFIELD\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(hrand_single, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert!(response.starts_with(b"$"));

    response.clear();
    let hrand_count = b"*3\r\n$10\r\nHRANDFIELD\r\n$3\r\nkey\r\n$1\r\n2\r\n";
    let meta = parse_resp_command_arg_slices(hrand_count, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert!(response.starts_with(b"*2\r\n"));

    response.clear();
    let hrand_withvalues =
        b"*4\r\n$10\r\nHRANDFIELD\r\n$3\r\nkey\r\n$2\r\n-4\r\n$10\r\nWITHVALUES\r\n";
    let meta = parse_resp_command_arg_slices(hrand_withvalues, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert!(response.starts_with(b"*8\r\n"));

    response.clear();
    let hello3 = b"*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n";
    let meta = parse_resp_command_arg_slices(hello3, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let hrand_resp3 = b"*4\r\n$10\r\nHRANDFIELD\r\n$3\r\nkey\r\n$1\r\n2\r\n$10\r\nWITHVALUES\r\n";
    let meta = parse_resp_command_arg_slices(hrand_resp3, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert!(response.starts_with(b"*2\r\n*2\r\n"));

    response.clear();
    let hello2 = b"*2\r\n$5\r\nHELLO\r\n$1\r\n2\r\n";
    let meta = parse_resp_command_arg_slices(hello2, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let meta = parse_resp_command_arg_slices(hrand_resp3, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert!(response.starts_with(b"*4\r\n"));
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

    response.clear();
    let lpos = b"*3\r\n$4\r\nLPOS\r\n$3\r\nkey\r\n$1\r\nv\r\n";
    let meta = parse_resp_command_arg_slices(lpos, &mut args).unwrap();
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
fn list_pop_commands_support_optional_count() {
    let processor = RequestProcessor::new().unwrap();

    assert_command_response(&processor, "LPOP missing 2", b"*-1\r\n");
    assert_command_response(&processor, "RPOP missing 0", b"*-1\r\n");
    assert_command_response(&processor, "RPUSH key a b c", b":3\r\n");
    assert_command_response(&processor, "LPOP key 2", b"*2\r\n$1\r\na\r\n$1\r\nb\r\n");
    assert_command_response(&processor, "LPOP key 0", b"*0\r\n");
    assert_command_response(&processor, "RPOP key 2", b"*1\r\n$1\r\nc\r\n");
    assert_command_response(&processor, "LPOP key 1", b"*-1\r\n");
    assert_command_error(
        &processor,
        "LPOP key -1",
        b"-ERR value is out of range, must be positive\r\n",
    );

    processor.set_resp_protocol_version(3);
    assert_command_response(&processor, "LPOP missing", b"_\r\n");
    assert_command_response(&processor, "LPOP missing 2", b"_\r\n");
    assert_command_response(&processor, "RPOP missing 2", b"_\r\n");
    processor.set_resp_protocol_version(2);
}

#[test]
fn lrange_supports_negative_indexes() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 16];
    let mut response = Vec::new();

    let rpush = b"*6\r\n$5\r\nRPUSH\r\n$3\r\nkey\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n";
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
fn additional_list_commands_cover_common_redis_semantics() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 16];
    let mut response = Vec::new();

    let rpush = b"*5\r\n$5\r\nRPUSH\r\n$3\r\nkey\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n";
    let meta = parse_resp_command_arg_slices(rpush, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":3\r\n");

    response.clear();
    let llen = b"*2\r\n$4\r\nLLEN\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(llen, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":3\r\n");

    response.clear();
    let lindex_zero = b"*3\r\n$6\r\nLINDEX\r\n$3\r\nkey\r\n$1\r\n0\r\n";
    let meta = parse_resp_command_arg_slices(lindex_zero, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"$1\r\na\r\n");

    response.clear();
    let lindex_negative = b"*3\r\n$6\r\nLINDEX\r\n$3\r\nkey\r\n$2\r\n-1\r\n";
    let meta = parse_resp_command_arg_slices(lindex_negative, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"$1\r\nc\r\n");

    response.clear();
    let lset = b"*4\r\n$4\r\nLSET\r\n$3\r\nkey\r\n$1\r\n1\r\n$1\r\nz\r\n";
    let meta = parse_resp_command_arg_slices(lset, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let ltrim = b"*4\r\n$5\r\nLTRIM\r\n$3\r\nkey\r\n$1\r\n1\r\n$1\r\n2\r\n";
    let meta = parse_resp_command_arg_slices(ltrim, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let lpushx = b"*4\r\n$6\r\nLPUSHX\r\n$3\r\nkey\r\n$1\r\nx\r\n$1\r\ny\r\n";
    let meta = parse_resp_command_arg_slices(lpushx, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":4\r\n");

    response.clear();
    let rpushx_missing = b"*3\r\n$6\r\nRPUSHX\r\n$7\r\nmissing\r\n$1\r\nq\r\n";
    let meta = parse_resp_command_arg_slices(rpushx_missing, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let lrem_all_x = b"*4\r\n$4\r\nLREM\r\n$3\r\nkey\r\n$1\r\n0\r\n$1\r\nx\r\n";
    let meta = parse_resp_command_arg_slices(lrem_all_x, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let lrem_tail_c = b"*4\r\n$4\r\nLREM\r\n$3\r\nkey\r\n$2\r\n-1\r\n$1\r\nc\r\n";
    let meta = parse_resp_command_arg_slices(lrem_tail_c, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let lrange = b"*4\r\n$6\r\nLRANGE\r\n$3\r\nkey\r\n$1\r\n0\r\n$2\r\n-1\r\n";
    let meta = parse_resp_command_arg_slices(lrange, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"*2\r\n$1\r\ny\r\n$1\r\nz\r\n");

    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"RPUSH", b"lsrc", b"a", b"b", b"c"])
        ),
        b":3\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"LINSERT", b"lsrc", b"BEFORE", b"b", b"x"])
        ),
        b":4\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"LINSERT", b"lsrc", b"AFTER", b"c", b"z"])
        ),
        b":5\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"LINSERT", b"lsrc", b"BEFORE", b"missing", b"m"])
        ),
        b":-1\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"LINSERT", b"no_list", b"BEFORE", b"a", b"m"])
        ),
        b":0\r\n"
    );

    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"LMOVE", b"lsrc", b"ldst", b"LEFT", b"RIGHT"])
        ),
        b"$1\r\na\r\n"
    );
    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"LRANGE", b"lsrc", b"0", b"-1"])),
        b"*4\r\n$1\r\nx\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nz\r\n"
    );
    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"LRANGE", b"ldst", b"0", b"-1"])),
        b"*1\r\n$1\r\na\r\n"
    );
    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"RPOPLPUSH", b"lsrc", b"ldst"])),
        b"$1\r\nz\r\n"
    );
    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"LRANGE", b"ldst", b"0", b"-1"])),
        b"*2\r\n$1\r\nz\r\n$1\r\na\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"RPOPLPUSH", b"missing_list", b"ldst"])
        ),
        b"$-1\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"RPUSH", b"rot", b"1", b"2", b"3"])
        ),
        b":3\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"LMOVE", b"rot", b"rot", b"LEFT", b"RIGHT"])
        ),
        b"$1\r\n1\r\n"
    );
    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"LRANGE", b"rot", b"0", b"-1"])),
        b"*3\r\n$1\r\n2\r\n$1\r\n3\r\n$1\r\n1\r\n"
    );

    response.clear();
    let lset_missing = b"*4\r\n$4\r\nLSET\r\n$7\r\nmissing\r\n$1\r\n0\r\n$1\r\nx\r\n";
    let meta = parse_resp_command_arg_slices(lset_missing, &mut args).unwrap();
    let err = processor
        .execute(&args[..meta.argument_count], &mut response)
        .err()
        .unwrap();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR no such key\r\n");

    response.clear();
    let lset_oob = b"*4\r\n$4\r\nLSET\r\n$3\r\nkey\r\n$2\r\n99\r\n$1\r\nx\r\n";
    let meta = parse_resp_command_arg_slices(lset_oob, &mut args).unwrap();
    let err = processor
        .execute(&args[..meta.argument_count], &mut response)
        .err()
        .unwrap();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR index out of range\r\n");

    response.clear();
    let ltrim_missing = b"*4\r\n$5\r\nLTRIM\r\n$7\r\nmissing\r\n$1\r\n0\r\n$1\r\n1\r\n";
    let meta = parse_resp_command_arg_slices(ltrim_missing, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"RPUSH", b"lpos", b"a", b"b", b"a", b"c", b"a"])
        ),
        b":5\r\n"
    );
    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"LPOS", b"lpos", b"a"])),
        b":0\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"LPOS", b"lpos", b"a", b"RANK", b"2"])
        ),
        b":2\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"LPOS", b"lpos", b"a", b"RANK", b"-1"])
        ),
        b":4\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"LPOS", b"lpos", b"a", b"COUNT", b"2"])
        ),
        b"*2\r\n:0\r\n:2\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"LPOS", b"lpos", b"a", b"COUNT", b"0"])
        ),
        b"*3\r\n:0\r\n:2\r\n:4\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"LPOS", b"lpos", b"a", b"RANK", b"-2", b"COUNT", b"2"])
        ),
        b"*2\r\n:2\r\n:0\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"LPOS", b"lpos", b"a", b"MAXLEN", b"2"])
        ),
        b":0\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"LPOS", b"lpos", b"a", b"RANK", b"2", b"MAXLEN", b"2"])
        ),
        b"$-1\r\n"
    );
    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"LPOS", b"missing", b"a"])),
        b"$-1\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"LPOS", b"missing", b"a", b"COUNT", b"2"])
        ),
        b"*0\r\n"
    );

    response.clear();
    let lpos_rank_zero = b"*5\r\n$4\r\nLPOS\r\n$4\r\nlpos\r\n$1\r\na\r\n$4\r\nRANK\r\n$1\r\n0\r\n";
    let meta = parse_resp_command_arg_slices(lpos_rank_zero, &mut args).unwrap();
    let err = processor
        .execute(&args[..meta.argument_count], &mut response)
        .err()
        .unwrap();
    err.append_resp_error(&mut response);
    assert_eq!(
        response,
        b"-ERR RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list\r\n"
    );

    response.clear();
    let lpos_rank_min = b"*5\r\n$4\r\nLPOS\r\n$4\r\nlpos\r\n$1\r\na\r\n$4\r\nRANK\r\n$20\r\n-9223372036854775808\r\n";
    let meta = parse_resp_command_arg_slices(lpos_rank_min, &mut args).unwrap();
    let err = processor
        .execute(&args[..meta.argument_count], &mut response)
        .err()
        .unwrap();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR value is out of range\r\n");

    response.clear();
    let lpos_count_negative =
        b"*5\r\n$4\r\nLPOS\r\n$4\r\nlpos\r\n$1\r\na\r\n$5\r\nCOUNT\r\n$2\r\n-1\r\n";
    let meta = parse_resp_command_arg_slices(lpos_count_negative, &mut args).unwrap();
    let err = processor
        .execute(&args[..meta.argument_count], &mut response)
        .err()
        .unwrap();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR value is out of range\r\n");

    response.clear();
    let lpos_maxlen_negative =
        b"*5\r\n$4\r\nLPOS\r\n$4\r\nlpos\r\n$1\r\na\r\n$6\r\nMAXLEN\r\n$2\r\n-1\r\n";
    let meta = parse_resp_command_arg_slices(lpos_maxlen_negative, &mut args).unwrap();
    let err = processor
        .execute(&args[..meta.argument_count], &mut response)
        .err()
        .unwrap();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR value is out of range\r\n");

    response.clear();
    let lpos_unknown_option =
        b"*5\r\n$4\r\nLPOS\r\n$4\r\nlpos\r\n$1\r\na\r\n$7\r\nUNKNOWN\r\n$1\r\n1\r\n";
    let meta = parse_resp_command_arg_slices(lpos_unknown_option, &mut args).unwrap();
    let err = processor
        .execute(&args[..meta.argument_count], &mut response)
        .err()
        .unwrap();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR syntax error\r\n");
}

#[test]
fn blocking_and_mpop_list_commands_cover_redis_shapes() {
    let processor = RequestProcessor::new().unwrap();

    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"RPUSH", b"queue", b"a", b"b", b"c", b"d"])
        ),
        b":4\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"LMPOP", b"1", b"queue", b"LEFT", b"COUNT", b"2"])
        ),
        b"*2\r\n$5\r\nqueue\r\n*2\r\n$1\r\na\r\n$1\r\nb\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"LMPOP", b"1", b"queue", b"RIGHT"])
        ),
        b"*2\r\n$5\r\nqueue\r\n*1\r\n$1\r\nd\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"BLMPOP", b"0.01", b"1", b"queue", b"LEFT"])
        ),
        b"*2\r\n$5\r\nqueue\r\n*1\r\n$1\r\nc\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"BLMPOP", b"0.01", b"1", b"queue", b"LEFT"])
        ),
        b"*-1\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"LMPOP", b"1", b"missing", b"LEFT"])
        ),
        b"*-1\r\n"
    );

    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"RPUSH", b"k2", b"x", b"y"])),
        b":2\r\n"
    );
    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"BLPOP", b"k1", b"k2", b"0.02"])),
        b"*2\r\n$2\r\nk2\r\n$1\r\nx\r\n"
    );
    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"BRPOP", b"k1", b"k2", b"0.02"])),
        b"*2\r\n$2\r\nk2\r\n$1\r\ny\r\n"
    );
    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"BRPOP", b"k1", b"k2", b"0.02"])),
        b"*-1\r\n"
    );

    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"RPUSH", b"src", b"1", b"2"])),
        b":2\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"BLMOVE", b"src", b"dst", b"RIGHT", b"LEFT", b"0.01"])
        ),
        b"$1\r\n2\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"BRPOPLPUSH", b"src", b"dst", b"0.01"])
        ),
        b"$1\r\n1\r\n"
    );
    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"LRANGE", b"dst", b"0", b"-1"])),
        b"*2\r\n$1\r\n1\r\n$1\r\n2\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"BRPOPLPUSH", b"missing", b"dst", b"0.01"])
        ),
        b"$-1\r\n"
    );

    let mut args = [ArgSlice::EMPTY; 16];
    let mut response = Vec::new();

    let lmpop_zero_count = encode_resp(&[b"LMPOP", b"1", b"missing", b"LEFT", b"COUNT", b"0"]);
    let meta = parse_resp_command_arg_slices(&lmpop_zero_count, &mut args).unwrap();
    let err = processor
        .execute(&args[..meta.argument_count], &mut response)
        .err()
        .unwrap();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR count should be greater than 0\r\n");

    response.clear();
    let lmpop_non_integer_count =
        encode_resp(&[b"LMPOP", b"1", b"missing", b"LEFT", b"COUNT", b"abc"]);
    let meta = parse_resp_command_arg_slices(&lmpop_non_integer_count, &mut args).unwrap();
    let err = processor
        .execute(&args[..meta.argument_count], &mut response)
        .err()
        .unwrap();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR count should be greater than 0\r\n");

    response.clear();
    let lmpop_negative_count = encode_resp(&[b"LMPOP", b"1", b"missing", b"LEFT", b"COUNT", b"-1"]);
    let meta = parse_resp_command_arg_slices(&lmpop_negative_count, &mut args).unwrap();
    let err = processor
        .execute(&args[..meta.argument_count], &mut response)
        .err()
        .unwrap();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR count should be greater than 0\r\n");

    response.clear();
    let lmpop_zero_numkeys = encode_resp(&[b"LMPOP", b"0", b"missing", b"LEFT"]);
    let meta = parse_resp_command_arg_slices(&lmpop_zero_numkeys, &mut args).unwrap();
    let err = processor
        .execute(&args[..meta.argument_count], &mut response)
        .err()
        .unwrap();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR numkeys should be greater than 0\r\n");

    response.clear();
    let lmpop_non_integer_numkeys = encode_resp(&[b"LMPOP", b"a", b"missing", b"LEFT"]);
    let meta = parse_resp_command_arg_slices(&lmpop_non_integer_numkeys, &mut args).unwrap();
    let err = processor
        .execute(&args[..meta.argument_count], &mut response)
        .err()
        .unwrap();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR numkeys should be greater than 0\r\n");

    response.clear();
    let lmpop_negative_numkeys = encode_resp(&[b"LMPOP", b"-1", b"missing", b"LEFT"]);
    let meta = parse_resp_command_arg_slices(&lmpop_negative_numkeys, &mut args).unwrap();
    let err = processor
        .execute(&args[..meta.argument_count], &mut response)
        .err()
        .unwrap();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR numkeys should be greater than 0\r\n");

    response.clear();
    let blpop_bad_timeout = encode_resp(&[b"BLPOP", b"k1", b"not-a-float"]);
    let meta = parse_resp_command_arg_slices(&blpop_bad_timeout, &mut args).unwrap();
    let err = processor
        .execute(&args[..meta.argument_count], &mut response)
        .err()
        .unwrap();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR value is not a valid float\r\n");

    response.clear();
    let blpop_negative_timeout = encode_resp(&[b"BLPOP", b"k1", b"-1"]);
    let meta = parse_resp_command_arg_slices(&blpop_negative_timeout, &mut args).unwrap();
    let err = processor
        .execute(&args[..meta.argument_count], &mut response)
        .err()
        .unwrap();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR timeout is negative\r\n");
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

    response.clear();
    let lmove = b"*5\r\n$5\r\nLMOVE\r\n$3\r\nkey\r\n$3\r\ndst\r\n$4\r\nLEFT\r\n$4\r\nLEFT\r\n";
    let meta = parse_resp_command_arg_slices(lmove, &mut args).unwrap();
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
fn additional_set_commands_cover_common_redis_semantics() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 20];
    let mut response = Vec::new();

    let sadd_src = b"*6\r\n$4\r\nSADD\r\n$3\r\nsrc\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n";
    let meta = parse_resp_command_arg_slices(sadd_src, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":4\r\n");

    response.clear();
    let sadd_dst = b"*3\r\n$4\r\nSADD\r\n$3\r\ndst\r\n$1\r\nz\r\n";
    let meta = parse_resp_command_arg_slices(sadd_dst, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let scard = b"*2\r\n$5\r\nSCARD\r\n$3\r\nsrc\r\n";
    let meta = parse_resp_command_arg_slices(scard, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":4\r\n");

    response.clear();
    let smismember = b"*5\r\n$10\r\nSMISMEMBER\r\n$3\r\nsrc\r\n$1\r\na\r\n$1\r\nx\r\n$1\r\nc\r\n";
    let meta = parse_resp_command_arg_slices(smismember, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"*3\r\n:1\r\n:0\r\n:1\r\n");

    response.clear();
    let srandmember = b"*3\r\n$11\r\nSRANDMEMBER\r\n$3\r\nsrc\r\n$1\r\n2\r\n";
    let meta = parse_resp_command_arg_slices(srandmember, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert!(response.starts_with(b"*2\r\n"));

    response.clear();
    let smove = b"*4\r\n$5\r\nSMOVE\r\n$3\r\nsrc\r\n$3\r\ndst\r\n$1\r\na\r\n";
    let meta = parse_resp_command_arg_slices(smove, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let sismember_dst = b"*3\r\n$9\r\nSISMEMBER\r\n$3\r\ndst\r\n$1\r\na\r\n";
    let meta = parse_resp_command_arg_slices(sismember_dst, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let spop_count = b"*3\r\n$4\r\nSPOP\r\n$3\r\nsrc\r\n$1\r\n2\r\n";
    let meta = parse_resp_command_arg_slices(spop_count, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert!(response.starts_with(b"*2\r\n"));

    response.clear();
    let scard_after_pop = b"*2\r\n$5\r\nSCARD\r\n$3\r\nsrc\r\n";
    let meta = parse_resp_command_arg_slices(scard_after_pop, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let spop_single = b"*2\r\n$4\r\nSPOP\r\n$3\r\nsrc\r\n";
    let meta = parse_resp_command_arg_slices(spop_single, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert!(response.starts_with(b"$"));

    response.clear();
    let scard_empty = b"*2\r\n$5\r\nSCARD\r\n$3\r\nsrc\r\n";
    let meta = parse_resp_command_arg_slices(scard_empty, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let spop_negative = b"*3\r\n$4\r\nSPOP\r\n$3\r\ndst\r\n$2\r\n-1\r\n";
    let meta = parse_resp_command_arg_slices(spop_negative, &mut args).unwrap();
    let err = processor
        .execute(&args[..meta.argument_count], &mut response)
        .err()
        .unwrap();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR value is out of range\r\n");
}

#[test]
fn set_algebra_commands_cover_union_intersection_difference_and_store_variants() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 20];
    let mut response = Vec::new();

    let sadd_s1 = encode_resp(&[b"SADD", b"s1", b"a", b"b", b"c"]);
    let meta = parse_resp_command_arg_slices(&sadd_s1, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":3\r\n");

    response.clear();
    let sadd_s2 = encode_resp(&[b"SADD", b"s2", b"b", b"c", b"d"]);
    let meta = parse_resp_command_arg_slices(&sadd_s2, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":3\r\n");

    response.clear();
    let sadd_s3 = encode_resp(&[b"SADD", b"s3", b"c", b"e"]);
    let meta = parse_resp_command_arg_slices(&sadd_s3, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":2\r\n");

    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"SUNION", b"s1", b"s2", b"s3"])),
        b"*5\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n"
    );
    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"SINTER", b"s1", b"s2", b"s3"])),
        b"*1\r\n$1\r\nc\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"SINTERCARD", b"2", b"s1", b"s2"])
        ),
        b":2\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"SINTERCARD", b"3", b"s1", b"s2", b"s3", b"LIMIT", b"1"])
        ),
        b":1\r\n"
    );
    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"SDIFF", b"s1", b"s2", b"s3"])),
        b"*1\r\n$1\r\na\r\n"
    );

    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"SUNIONSTORE", b"dst_union", b"s1", b"s2"])
        ),
        b":4\r\n"
    );
    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"SMEMBERS", b"dst_union"])),
        b"*4\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n"
    );

    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"SINTERSTORE", b"dst_inter", b"s1", b"s2", b"s3"])
        ),
        b":1\r\n"
    );
    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"SMEMBERS", b"dst_inter"])),
        b"*1\r\n$1\r\nc\r\n"
    );

    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"SDIFFSTORE", b"dst_diff", b"s1", b"s2", b"s3"])
        ),
        b":1\r\n"
    );
    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"SMEMBERS", b"dst_diff"])),
        b"*1\r\n$1\r\na\r\n"
    );

    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"SADD", b"to_drop", b"z"])),
        b":1\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"SINTERSTORE", b"to_drop", b"missing1", b"missing2"])
        ),
        b":0\r\n"
    );
    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"EXISTS", b"to_drop"])),
        b":0\r\n"
    );

    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"SET", b"dst_string", b"value"])),
        b"+OK\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"SUNIONSTORE", b"dst_string", b"s1"])
        ),
        b":3\r\n"
    );
    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"TYPE", b"dst_string"])),
        b"+set\r\n"
    );
    response.clear();
    let get_dst_string = encode_resp(&[b"GET", b"dst_string"]);
    let meta = parse_resp_command_arg_slices(&get_dst_string, &mut args).unwrap();
    let err = processor
        .execute(&args[..meta.argument_count], &mut response)
        .err()
        .unwrap();
    err.append_resp_error(&mut response);
    assert_eq!(
        response,
        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
    );

    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"SET", b"plain", b"x"])),
        b"+OK\r\n"
    );
    response.clear();
    let sinter_wrongtype = encode_resp(&[b"SINTER", b"plain", b"s1"]);
    let meta = parse_resp_command_arg_slices(&sinter_wrongtype, &mut args).unwrap();
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

    let zadd = b"*6\r\n$4\r\nZADD\r\n$3\r\nkey\r\n$1\r\n2\r\n$3\r\ntwo\r\n$1\r\n1\r\n$3\r\none\r\n";
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
    let zcard = b"*2\r\n$5\r\nZCARD\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(zcard, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":2\r\n");

    response.clear();
    let zcount_all = encode_resp(&[b"ZCOUNT", b"key", b"-inf", b"+inf"]);
    let meta = parse_resp_command_arg_slices(&zcount_all, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":2\r\n");

    response.clear();
    let zcount_exclusive = encode_resp(&[b"ZCOUNT", b"key", b"(1", b"2"]);
    let meta = parse_resp_command_arg_slices(&zcount_exclusive, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let zrank_one = b"*3\r\n$5\r\nZRANK\r\n$3\r\nkey\r\n$3\r\none\r\n";
    let meta = parse_resp_command_arg_slices(zrank_one, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let zrevrank_one = b"*3\r\n$8\r\nZREVRANK\r\n$3\r\nkey\r\n$3\r\none\r\n";
    let meta = parse_resp_command_arg_slices(zrevrank_one, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let zincrby = b"*4\r\n$7\r\nZINCRBY\r\n$3\r\nkey\r\n$1\r\n2\r\n$3\r\none\r\n";
    let meta = parse_resp_command_arg_slices(zincrby, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"$1\r\n3\r\n");

    response.clear();
    let meta = parse_resp_command_arg_slices(zrank_one, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let zrange = b"*4\r\n$6\r\nZRANGE\r\n$3\r\nkey\r\n$1\r\n0\r\n$2\r\n-1\r\n";
    let meta = parse_resp_command_arg_slices(zrange, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"*2\r\n$3\r\ntwo\r\n$3\r\none\r\n");

    response.clear();
    let zrange_withscores = encode_resp(&[b"ZRANGE", b"key", b"0", b"-1", b"WITHSCORES"]);
    let meta = parse_resp_command_arg_slices(&zrange_withscores, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(
        response,
        b"*4\r\n$3\r\ntwo\r\n$1\r\n2\r\n$3\r\none\r\n$1\r\n3\r\n"
    );

    processor.set_resp_protocol_version(3);
    response.clear();
    let meta = parse_resp_command_arg_slices(&zrange_withscores, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(
        response,
        b"*2\r\n*2\r\n$3\r\ntwo\r\n$1\r\n2\r\n*2\r\n$3\r\none\r\n$1\r\n3\r\n"
    );
    processor.set_resp_protocol_version(2);

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

    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"ZADD", b"rangekey", b"1", b"a", b"2", b"b", b"3", b"c"])
        ),
        b":3\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"ZREVRANGE", b"rangekey", b"0", b"-1", b"WITHSCORES"])
        ),
        b"*6\r\n$1\r\nc\r\n$1\r\n3\r\n$1\r\nb\r\n$1\r\n2\r\n$1\r\na\r\n$1\r\n1\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[
                b"ZRANGEBYSCORE",
                b"rangekey",
                b"(1",
                b"+inf",
                b"WITHSCORES",
                b"LIMIT",
                b"0",
                b"1"
            ])
        ),
        b"*2\r\n$1\r\nb\r\n$1\r\n2\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[
                b"ZREVRANGEBYSCORE",
                b"rangekey",
                b"+inf",
                b"(2",
                b"WITHSCORES"
            ])
        ),
        b"*2\r\n$1\r\nc\r\n$1\r\n3\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"ZLEXCOUNT", b"rangekey", b"[a", b"[c"])
        ),
        b":3\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[
                b"ZRANGEBYLEX",
                b"rangekey",
                b"-",
                b"+",
                b"LIMIT",
                b"1",
                b"2"
            ])
        ),
        b"*2\r\n$1\r\nb\r\n$1\r\nc\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[
                b"ZREVRANGEBYLEX",
                b"rangekey",
                b"+",
                b"-",
                b"LIMIT",
                b"0",
                b"2"
            ])
        ),
        b"*2\r\n$1\r\nc\r\n$1\r\nb\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"ZREMRANGEBYLEX", b"rangekey", b"[b", b"[b"])
        ),
        b":1\r\n"
    );
    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"ZCARD", b"rangekey"])),
        b":2\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"ZREMRANGEBYRANK", b"rangekey", b"0", b"0"])
        ),
        b":1\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"ZREMRANGEBYSCORE", b"rangekey", b"-inf", b"2"])
        ),
        b":0\r\n"
    );
    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"ZCARD", b"rangekey"])),
        b":1\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"ZADD", b"iz1", b"1", b"a", b"2", b"b", b"3", b"c"])
        ),
        b":3\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"ZADD", b"iz2", b"2", b"b", b"3", b"c", b"4", b"d"])
        ),
        b":3\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"ZADD", b"iz3", b"3", b"c", b"4", b"e"])
        ),
        b":2\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"ZINTERCARD", b"2", b"iz1", b"iz2"])
        ),
        b":2\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"ZINTERCARD", b"3", b"iz1", b"iz2", b"iz3", b"LIMIT", b"1"])
        ),
        b":1\r\n"
    );
    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"ZDIFF", b"2", b"iz1", b"iz2"])),
        b"*1\r\n$1\r\na\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"ZDIFF", b"2", b"iz1", b"iz2", b"WITHSCORES"])
        ),
        b"*2\r\n$1\r\na\r\n$1\r\n1\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"ZDIFFSTORE", b"zdst", b"2", b"iz1", b"iz2"])
        ),
        b":1\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"ZRANGEBYSCORE", b"zdst", b"-inf", b"+inf", b"WITHSCORES"])
        ),
        b"*2\r\n$1\r\na\r\n$1\r\n1\r\n"
    );
    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"ZINTER", b"2", b"iz1", b"iz2"])),
        b"*2\r\n$1\r\nb\r\n$1\r\nc\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"ZINTER", b"2", b"iz1", b"iz2", b"WITHSCORES"])
        ),
        b"*4\r\n$1\r\nb\r\n$1\r\n4\r\n$1\r\nc\r\n$1\r\n6\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[
                b"ZINTER",
                b"2",
                b"iz1",
                b"iz2",
                b"WEIGHTS",
                b"2",
                b"3",
                b"WITHSCORES"
            ])
        ),
        b"*4\r\n$1\r\nb\r\n$2\r\n10\r\n$1\r\nc\r\n$2\r\n15\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[
                b"ZINTERSTORE",
                b"zinterdst",
                b"2",
                b"iz1",
                b"iz2",
                b"AGGREGATE",
                b"MAX"
            ])
        ),
        b":2\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[
                b"ZRANGEBYSCORE",
                b"zinterdst",
                b"-inf",
                b"+inf",
                b"WITHSCORES"
            ])
        ),
        b"*4\r\n$1\r\nb\r\n$1\r\n2\r\n$1\r\nc\r\n$1\r\n3\r\n"
    );
    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"ZUNION", b"2", b"iz1", b"iz2"])),
        b"*4\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nd\r\n$1\r\nc\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"ZUNION", b"2", b"iz1", b"iz2", b"WITHSCORES"])
        ),
        b"*8\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n4\r\n$1\r\nd\r\n$1\r\n4\r\n$1\r\nc\r\n$1\r\n6\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[
                b"ZUNIONSTORE",
                b"zuniondst",
                b"2",
                b"iz1",
                b"iz2",
                b"WEIGHTS",
                b"2",
                b"1",
                b"AGGREGATE",
                b"MIN"
            ])
        ),
        b":4\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[
                b"ZRANGEBYSCORE",
                b"zuniondst",
                b"-inf",
                b"+inf",
                b"WITHSCORES"
            ])
        ),
        b"*8\r\n$1\r\na\r\n$1\r\n2\r\n$1\r\nb\r\n$1\r\n2\r\n$1\r\nc\r\n$1\r\n3\r\n$1\r\nd\r\n$1\r\n4\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[
                b"ZADD", b"zrsrc", b"1", b"a", b"2", b"b", b"3", b"c", b"4", b"d"
            ])
        ),
        b":4\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"ZRANGESTORE", b"zrdst", b"zrsrc", b"1", b"2"])
        ),
        b":2\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"ZRANGEBYSCORE", b"zrdst", b"-inf", b"+inf", b"WITHSCORES"])
        ),
        b"*4\r\n$1\r\nb\r\n$1\r\n2\r\n$1\r\nc\r\n$1\r\n3\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[
                b"ZRANGESTORE",
                b"zrdst",
                b"zrsrc",
                b"-inf",
                b"(4",
                b"BYSCORE",
                b"LIMIT",
                b"1",
                b"2"
            ])
        ),
        b":2\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"ZRANGEBYSCORE", b"zrdst", b"-inf", b"+inf", b"WITHSCORES"])
        ),
        b"*4\r\n$1\r\nb\r\n$1\r\n2\r\n$1\r\nc\r\n$1\r\n3\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[
                b"ZRANGESTORE",
                b"zrdst",
                b"zrsrc",
                b"+",
                b"-",
                b"BYLEX",
                b"REV",
                b"LIMIT",
                b"0",
                b"2"
            ])
        ),
        b":2\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"ZRANGEBYSCORE", b"zrdst", b"-inf", b"+inf", b"WITHSCORES"])
        ),
        b"*4\r\n$1\r\nc\r\n$1\r\n3\r\n$1\r\nd\r\n$1\r\n4\r\n"
    );
    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"SET", b"zstrdst", b"x"])),
        b"+OK\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"ZRANGESTORE", b"zstrdst", b"zrsrc", b"0", b"0"])
        ),
        b":1\r\n"
    );
    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"TYPE", b"zstrdst"])),
        b"+zset\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"ZADD", b"zm1", b"1", b"a", b"2", b"b"])
        ),
        b":2\r\n"
    );
    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"ZADD", b"zm2", b"3", b"c"])),
        b":1\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"ZMPOP", b"2", b"missing_zm", b"zm1", b"MIN"])
        ),
        b"*2\r\n$3\r\nzm1\r\n*1\r\n*2\r\n$1\r\na\r\n$1\r\n1\r\n"
    );
    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"ZCARD", b"zm1"])),
        b":1\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"ZMPOP", b"2", b"zm1", b"zm2", b"MAX", b"COUNT", b"2"])
        ),
        b"*2\r\n$3\r\nzm1\r\n*1\r\n*2\r\n$1\r\nb\r\n$1\r\n2\r\n"
    );
    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"ZCARD", b"zm1"])),
        b":0\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"ZMPOP", b"2", b"zm1", b"zm2", b"MAX", b"COUNT", b"2"])
        ),
        b"*2\r\n$3\r\nzm2\r\n*1\r\n*2\r\n$1\r\nc\r\n$1\r\n3\r\n"
    );
    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"ZMPOP", b"1", b"zm2", b"MIN"])),
        b"*-1\r\n"
    );

    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"ZADD", b"bz1", b"1", b"a", b"2", b"b"])
        ),
        b":2\r\n"
    );
    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"ZADD", b"bz2", b"3", b"c"])),
        b":1\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"BZPOPMIN", b"missing_bz", b"bz1", b"0.01"])
        ),
        b"*3\r\n$3\r\nbz1\r\n$1\r\na\r\n$1\r\n1\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"BZPOPMAX", b"bz1", b"bz2", b"0.01"])
        ),
        b"*3\r\n$3\r\nbz1\r\n$1\r\nb\r\n$1\r\n2\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"BZPOPMAX", b"bz1", b"bz2", b"0.01"])
        ),
        b"*3\r\n$3\r\nbz2\r\n$1\r\nc\r\n$1\r\n3\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"BZPOPMIN", b"bz1", b"bz2", b"0.01"])
        ),
        b"*-1\r\n"
    );

    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"ZADD", b"bzm1", b"1", b"x", b"2", b"y"])
        ),
        b":2\r\n"
    );
    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"ZADD", b"bzm2", b"3", b"z"])),
        b":1\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[
                b"BZMPOP",
                b"0.01",
                b"2",
                b"missing_bzm",
                b"bzm1",
                b"MIN",
                b"COUNT",
                b"2"
            ])
        ),
        b"*2\r\n$4\r\nbzm1\r\n*2\r\n*2\r\n$1\r\nx\r\n$1\r\n1\r\n*2\r\n$1\r\ny\r\n$1\r\n2\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"BZMPOP", b"0.01", b"2", b"bzm1", b"bzm2", b"MAX"])
        ),
        b"*2\r\n$4\r\nbzm2\r\n*1\r\n*2\r\n$1\r\nz\r\n$1\r\n3\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"BZMPOP", b"0.01", b"1", b"bzm2", b"MIN"])
        ),
        b"*-1\r\n"
    );

    response.clear();
    let zmscore =
        b"*5\r\n$7\r\nZMSCORE\r\n$3\r\nkey\r\n$3\r\none\r\n$7\r\nmissing\r\n$3\r\ntwo\r\n";
    let meta = parse_resp_command_arg_slices(zmscore, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"*3\r\n$1\r\n3\r\n$-1\r\n$1\r\n2\r\n");

    response.clear();
    let zrandmember = b"*2\r\n$11\r\nZRANDMEMBER\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(zrandmember, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert!(response.starts_with(b"$"));

    response.clear();
    let zrandmember_withscores =
        b"*4\r\n$11\r\nZRANDMEMBER\r\n$3\r\nkey\r\n$1\r\n2\r\n$10\r\nWITHSCORES\r\n";
    let meta = parse_resp_command_arg_slices(zrandmember_withscores, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert!(response.starts_with(b"*4\r\n"));
    assert!(response.windows(9).any(|w| w == b"$3\r\none\r\n"));
    assert!(response.windows(9).any(|w| w == b"$3\r\ntwo\r\n"));

    response.clear();
    let zadd_popkey =
        b"*8\r\n$4\r\nZADD\r\n$6\r\npopkey\r\n$1\r\n1\r\n$1\r\na\r\n$1\r\n2\r\n$1\r\nb\r\n$1\r\n3\r\n$1\r\nc\r\n";
    let meta = parse_resp_command_arg_slices(zadd_popkey, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":3\r\n");

    response.clear();
    let zpopmin = b"*2\r\n$7\r\nZPOPMIN\r\n$6\r\npopkey\r\n";
    let meta = parse_resp_command_arg_slices(zpopmin, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"*2\r\n$1\r\na\r\n$1\r\n1\r\n");

    response.clear();
    let zpopmax = b"*3\r\n$7\r\nZPOPMAX\r\n$6\r\npopkey\r\n$1\r\n2\r\n";
    let meta = parse_resp_command_arg_slices(zpopmax, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(
        response,
        b"*4\r\n$1\r\nc\r\n$1\r\n3\r\n$1\r\nb\r\n$1\r\n2\r\n"
    );

    response.clear();
    let zcard_popkey = b"*2\r\n$5\r\nZCARD\r\n$6\r\npopkey\r\n";
    let meta = parse_resp_command_arg_slices(zcard_popkey, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let bzpopmin_bad_timeout = b"*3\r\n$8\r\nBZPOPMIN\r\n$3\r\nbz1\r\n$3\r\nbad\r\n";
    let meta = parse_resp_command_arg_slices(bzpopmin_bad_timeout, &mut args).unwrap();
    let err = processor
        .execute(&args[..meta.argument_count], &mut response)
        .err()
        .unwrap();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR value is not a valid float\r\n");

    response.clear();
    let bzmpop_negative_timeout =
        b"*7\r\n$6\r\nBZMPOP\r\n$2\r\n-1\r\n$1\r\n1\r\n$3\r\nbz1\r\n$3\r\nMIN\r\n$5\r\nCOUNT\r\n$1\r\n1\r\n";
    let meta = parse_resp_command_arg_slices(bzmpop_negative_timeout, &mut args).unwrap();
    let err = processor
        .execute(&args[..meta.argument_count], &mut response)
        .err()
        .unwrap();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR value is out of range\r\n");

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
fn incr_returns_wrongtype_for_object_key() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 4];
    let mut response = Vec::new();

    let hset = b"*4\r\n$4\r\nHSET\r\n$3\r\nkey\r\n$5\r\nfield\r\n$5\r\nvalue\r\n";
    let meta = parse_resp_command_arg_slices(hset, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let incr = b"*2\r\n$4\r\nINCR\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(incr, &mut args).unwrap();
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
fn executes_incrby_and_decrby_commands() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 3];
    let mut response = Vec::new();

    let incrby = b"*3\r\n$6\r\nINCRBY\r\n$7\r\ncounter\r\n$1\r\n5\r\n";
    let meta = parse_resp_command_arg_slices(incrby, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":5\r\n");

    response.clear();
    let decrby = b"*3\r\n$6\r\nDECRBY\r\n$7\r\ncounter\r\n$1\r\n2\r\n";
    let meta = parse_resp_command_arg_slices(decrby, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":3\r\n");
}

#[test]
fn exists_counts_duplicates_and_object_keys() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 8];
    let mut response = Vec::new();

    let set = b"*3\r\n$3\r\nSET\r\n$1\r\ns\r\n$1\r\n1\r\n";
    let meta = parse_resp_command_arg_slices(set, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let hset = b"*4\r\n$4\r\nHSET\r\n$1\r\no\r\n$1\r\nf\r\n$1\r\nv\r\n";
    let meta = parse_resp_command_arg_slices(hset, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let exists = b"*5\r\n$6\r\nEXISTS\r\n$1\r\ns\r\n$1\r\ns\r\n$1\r\no\r\n$1\r\nx\r\n";
    let meta = parse_resp_command_arg_slices(exists, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":3\r\n");
}

#[test]
fn type_reports_string_object_and_none() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 8];
    let mut response = Vec::new();

    let set = b"*3\r\n$3\r\nSET\r\n$1\r\ns\r\n$1\r\n1\r\n";
    let meta = parse_resp_command_arg_slices(set, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let sadd = b"*4\r\n$4\r\nSADD\r\n$2\r\nst\r\n$1\r\na\r\n$1\r\nb\r\n";
    let meta = parse_resp_command_arg_slices(sadd, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":2\r\n");

    response.clear();
    let type_string = b"*2\r\n$4\r\nTYPE\r\n$1\r\ns\r\n";
    let meta = parse_resp_command_arg_slices(type_string, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+string\r\n");

    response.clear();
    let type_set = b"*2\r\n$4\r\nTYPE\r\n$2\r\nst\r\n";
    let meta = parse_resp_command_arg_slices(type_set, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+set\r\n");

    response.clear();
    let type_none = b"*2\r\n$4\r\nTYPE\r\n$1\r\nx\r\n";
    let meta = parse_resp_command_arg_slices(type_none, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+none\r\n");
}

#[test]
fn mset_and_mget_support_multi_key_and_object_replacement() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 12];
    let mut response = Vec::new();

    let mset = b"*5\r\n$4\r\nMSET\r\n$2\r\nk1\r\n$2\r\nv1\r\n$2\r\nk2\r\n$2\r\nv2\r\n";
    let meta = parse_resp_command_arg_slices(mset, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let mget = b"*4\r\n$4\r\nMGET\r\n$2\r\nk1\r\n$2\r\nk2\r\n$2\r\nk3\r\n";
    let meta = parse_resp_command_arg_slices(mget, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"*3\r\n$2\r\nv1\r\n$2\r\nv2\r\n$-1\r\n");

    response.clear();
    let hset = b"*4\r\n$4\r\nHSET\r\n$3\r\nobj\r\n$1\r\nf\r\n$1\r\nv\r\n";
    let meta = parse_resp_command_arg_slices(hset, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let mget_obj = b"*2\r\n$4\r\nMGET\r\n$3\r\nobj\r\n";
    let meta = parse_resp_command_arg_slices(mget_obj, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"*1\r\n$-1\r\n");

    response.clear();
    let mset_overwrite_obj = b"*3\r\n$4\r\nMSET\r\n$3\r\nobj\r\n$3\r\nstr\r\n";
    let meta = parse_resp_command_arg_slices(mset_overwrite_obj, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let get = b"*2\r\n$3\r\nGET\r\n$3\r\nobj\r\n";
    let meta = parse_resp_command_arg_slices(get, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"$3\r\nstr\r\n");
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
fn del_removes_object_key() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 4];
    let mut response = Vec::new();

    let hset = b"*4\r\n$4\r\nHSET\r\n$3\r\nobj\r\n$5\r\nfield\r\n$5\r\nvalue\r\n";
    let meta = parse_resp_command_arg_slices(hset, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let del = b"*2\r\n$3\r\nDEL\r\n$3\r\nobj\r\n";
    let meta = parse_resp_command_arg_slices(del, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let hget = b"*3\r\n$4\r\nHGET\r\n$3\r\nobj\r\n$5\r\nfield\r\n";
    let meta = parse_resp_command_arg_slices(hget, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"$-1\r\n");
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
fn rename_moves_value_and_renamenx_respects_existing_destination() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 8];
    let mut response = Vec::new();

    let set_src = b"*3\r\n$3\r\nSET\r\n$3\r\nsrc\r\n$5\r\nvalue\r\n";
    let meta = parse_resp_command_arg_slices(set_src, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let rename = b"*3\r\n$6\r\nRENAME\r\n$3\r\nsrc\r\n$3\r\ndst\r\n";
    let meta = parse_resp_command_arg_slices(rename, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let get_dst = b"*2\r\n$3\r\nGET\r\n$3\r\ndst\r\n";
    let meta = parse_resp_command_arg_slices(get_dst, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"$5\r\nvalue\r\n");

    response.clear();
    let set_src_again = b"*3\r\n$3\r\nSET\r\n$3\r\nsrc\r\n$6\r\nvalue2\r\n";
    let meta = parse_resp_command_arg_slices(set_src_again, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let renamenx_existing = b"*3\r\n$8\r\nRENAMENX\r\n$3\r\nsrc\r\n$3\r\ndst\r\n";
    let meta = parse_resp_command_arg_slices(renamenx_existing, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let get_src = b"*2\r\n$3\r\nGET\r\n$3\r\nsrc\r\n";
    let meta = parse_resp_command_arg_slices(get_src, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"$6\r\nvalue2\r\n");
}

#[test]
fn rename_moves_ttl_and_missing_source_returns_no_such_key() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 8];
    let mut response = Vec::new();

    let set_src = b"*3\r\n$3\r\nSET\r\n$3\r\nsrc\r\n$5\r\nvalue\r\n";
    let meta = parse_resp_command_arg_slices(set_src, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();

    response.clear();
    let expire_src = b"*3\r\n$6\r\nEXPIRE\r\n$3\r\nsrc\r\n$3\r\n100\r\n";
    let meta = parse_resp_command_arg_slices(expire_src, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let rename = b"*3\r\n$6\r\nRENAME\r\n$3\r\nsrc\r\n$3\r\ndst\r\n";
    let meta = parse_resp_command_arg_slices(rename, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let ttl_dst = b"*2\r\n$3\r\nTTL\r\n$3\r\ndst\r\n";
    let meta = parse_resp_command_arg_slices(ttl_dst, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    let ttl_value = std::str::from_utf8(&response[1..response.len() - 2])
        .unwrap()
        .parse::<i64>()
        .unwrap();
    assert!(ttl_value > 0);

    response.clear();
    let rename_missing = b"*3\r\n$6\r\nRENAME\r\n$7\r\nmissing\r\n$3\r\ndst\r\n";
    let meta = parse_resp_command_arg_slices(rename_missing, &mut args).unwrap();
    let error = processor.execute(&args[..meta.argument_count], &mut response);
    assert_eq!(error, Err(RequestExecutionError::NoSuchKey));
}

#[test]
fn copy_copies_string_and_ttl_with_replace() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 12];
    let mut response = Vec::new();

    let set_src = b"*3\r\n$3\r\nSET\r\n$3\r\nsrc\r\n$5\r\nvalue\r\n";
    let meta = parse_resp_command_arg_slices(set_src, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();

    response.clear();
    let expire_src = b"*3\r\n$6\r\nEXPIRE\r\n$3\r\nsrc\r\n$3\r\n100\r\n";
    let meta = parse_resp_command_arg_slices(expire_src, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let copy = b"*3\r\n$4\r\nCOPY\r\n$3\r\nsrc\r\n$3\r\ndst\r\n";
    let meta = parse_resp_command_arg_slices(copy, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let get_dst = b"*2\r\n$3\r\nGET\r\n$3\r\ndst\r\n";
    let meta = parse_resp_command_arg_slices(get_dst, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"$5\r\nvalue\r\n");

    response.clear();
    let ttl_dst = b"*2\r\n$3\r\nTTL\r\n$3\r\ndst\r\n";
    let meta = parse_resp_command_arg_slices(ttl_dst, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    let ttl_value = std::str::from_utf8(&response[1..response.len() - 2])
        .unwrap()
        .parse::<i64>()
        .unwrap();
    assert!(ttl_value > 0);

    response.clear();
    let set_dst = b"*3\r\n$3\r\nSET\r\n$3\r\ndst\r\n$3\r\nold\r\n";
    let meta = parse_resp_command_arg_slices(set_dst, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();

    response.clear();
    let copy_without_replace = b"*3\r\n$4\r\nCOPY\r\n$3\r\nsrc\r\n$3\r\ndst\r\n";
    let meta = parse_resp_command_arg_slices(copy_without_replace, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let copy_with_replace = b"*4\r\n$4\r\nCOPY\r\n$3\r\nsrc\r\n$3\r\ndst\r\n$7\r\nREPLACE\r\n";
    let meta = parse_resp_command_arg_slices(copy_with_replace, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":1\r\n");
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
fn set_nx_and_xx_respect_object_key_existence() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 8];
    let mut response = Vec::new();

    let hset = b"*4\r\n$4\r\nHSET\r\n$3\r\nkey\r\n$5\r\nfield\r\n$5\r\nvalue\r\n";
    let meta = parse_resp_command_arg_slices(hset, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let set_nx = b"*4\r\n$3\r\nSET\r\n$3\r\nkey\r\n$3\r\nstr\r\n$2\r\nNX\r\n";
    let meta = parse_resp_command_arg_slices(set_nx, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"$-1\r\n");

    response.clear();
    let set_xx = b"*4\r\n$3\r\nSET\r\n$3\r\nkey\r\n$3\r\nstr\r\n$2\r\nXX\r\n";
    let meta = parse_resp_command_arg_slices(set_xx, &mut args).unwrap();
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
    assert_eq!(response, b"$3\r\nstr\r\n");

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
fn expiration_scan_removes_expired_object_keys_in_background_style() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 8];
    let mut response = Vec::new();

    let hset = b"*4\r\n$4\r\nHSET\r\n$3\r\nkey\r\n$5\r\nfield\r\n$5\r\nvalue\r\n";
    let meta = parse_resp_command_arg_slices(hset, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let pexpire = b"*3\r\n$7\r\nPEXPIRE\r\n$3\r\nkey\r\n$2\r\n10\r\n";
    let meta = parse_resp_command_arg_slices(pexpire, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":1\r\n");

    thread::sleep(Duration::from_millis(20));
    let removed = processor.expire_stale_keys(16).unwrap();
    assert_eq!(removed, 1);

    response.clear();
    let hget = b"*3\r\n$4\r\nHGET\r\n$3\r\nkey\r\n$5\r\nfield\r\n";
    let meta = parse_resp_command_arg_slices(hget, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"$-1\r\n");
}

#[test]
fn expiration_scan_can_target_single_shard() {
    let processor = RequestProcessor::new_with_string_store_shards(4).unwrap();
    let key_shard_1 = find_key_for_shard(&processor, 1);
    let key_shard_2 = find_key_for_shard(&processor, 2);
    assert_ne!(key_shard_1, key_shard_2);
    assert_eq!(processor.string_store_shard_index_for_key(&key_shard_1), 1);
    assert_eq!(processor.string_store_shard_index_for_key(&key_shard_2), 2);

    let set_1 = encode_resp(&[b"SET", key_shard_1.as_slice(), b"v1", b"PX", b"10"]);
    assert_eq!(execute_frame(&processor, &set_1), b"+OK\r\n");
    let set_2 = encode_resp(&[b"SET", key_shard_2.as_slice(), b"v2", b"PX", b"10"]);
    assert_eq!(execute_frame(&processor, &set_2), b"+OK\r\n");

    thread::sleep(Duration::from_millis(20));
    assert_eq!(processor.expire_stale_keys_in_shard(1, 16).unwrap(), 1);
    assert_eq!(processor.expire_stale_keys_in_shard(1, 16).unwrap(), 0);
    assert_eq!(processor.expire_stale_keys_in_shard(2, 16).unwrap(), 1);
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
fn set_and_getex_validate_expire_arguments_like_redis() {
    let processor = RequestProcessor::new().unwrap();

    assert_command_error(
        &processor,
        "SET key value EX not-a-number",
        b"-ERR value is not an integer or out of range\r\n",
    );
    assert_command_error(
        &processor,
        "SET key value EX 10000000000000000",
        b"-ERR invalid expire time in 'set' command\r\n",
    );
    assert_command_error(
        &processor,
        "SET key value EXAT 0",
        b"-ERR invalid expire time in 'set' command\r\n",
    );

    assert_command_response(&processor, "SET key value", b"+OK\r\n");
    assert_command_error(
        &processor,
        "GETEX key EX not-a-number",
        b"-ERR value is not an integer or out of range\r\n",
    );
    assert_command_error(
        &processor,
        "GETEX key EX 10000000000000000",
        b"-ERR invalid expire time in 'getex' command\r\n",
    );
    assert_command_error(
        &processor,
        "GETEX key EXAT 0",
        b"-ERR invalid expire time in 'getex' command\r\n",
    );
}

#[test]
fn set_supports_exat_pxat_and_keepttl() {
    let processor = RequestProcessor::new().unwrap();

    let now_secs = current_unix_time_millis().unwrap() / 1000;
    let exat_secs = now_secs + 2;
    let set_exat = format!("SET key-exat value EXAT {exat_secs}");
    assert_command_response(&processor, &set_exat, b"+OK\r\n");
    assert_command_integer(&processor, "EXPIRETIME key-exat", exat_secs as i64);

    let now_millis = current_unix_time_millis().unwrap();
    let pxat_millis = now_millis + 1_500;
    let set_pxat = format!("SET key-pxat value PXAT {pxat_millis}");
    assert_command_response(&processor, &set_pxat, b"+OK\r\n");
    assert_command_integer(&processor, "PEXPIRETIME key-pxat", pxat_millis as i64);

    assert_command_response(&processor, "SET key-keep value EX 100", b"+OK\r\n");
    assert_command_response(&processor, "SET key-keep value2 KEEPTTL", b"+OK\r\n");
    let ttl = parse_integer_response(
        &execute_command_line(&processor, "TTL key-keep").expect("TTL key-keep should succeed"),
    );
    assert!((90..=100).contains(&ttl));
}

#[test]
fn expire_overflow_returns_command_specific_errors() {
    let processor = RequestProcessor::new().unwrap();
    assert_command_response(&processor, "SET foo bar", b"+OK\r\n");

    assert_command_error(
        &processor,
        "EXPIRE foo 9223370399119966",
        b"-ERR invalid expire time in 'expire' command\r\n",
    );
    assert_command_error(
        &processor,
        "PEXPIRE foo 9223372036854770000",
        b"-ERR invalid expire time in 'pexpire' command\r\n",
    );
    assert_command_integer(&processor, "TTL foo", -1);
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
fn set_after_expire_and_del_recreates_key() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 8];
    let mut response = Vec::new();

    let set = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$3\r\nfoo\r\n";
    let meta = parse_resp_command_arg_slices(set, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let expire = b"*3\r\n$6\r\nEXPIRE\r\n$3\r\nkey\r\n$3\r\n100\r\n";
    let meta = parse_resp_command_arg_slices(expire, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let del = b"*2\r\n$3\r\nDEL\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(del, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let set_again = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$3\r\nbar\r\n";
    let meta = parse_resp_command_arg_slices(set_again, &mut args).unwrap();
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
    assert_eq!(response, b"$3\r\nbar\r\n");
}

#[test]
fn expire_ttl_and_persist_apply_to_object_keys() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 8];
    let mut response = Vec::new();

    let hset = b"*4\r\n$4\r\nHSET\r\n$3\r\nkey\r\n$5\r\nfield\r\n$5\r\nvalue\r\n";
    let meta = parse_resp_command_arg_slices(hset, &mut args).unwrap();
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
    let persist = b"*2\r\n$7\r\nPERSIST\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(persist, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let meta = parse_resp_command_arg_slices(ttl, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":-1\r\n");

    response.clear();
    let pexpire_now = b"*3\r\n$7\r\nPEXPIRE\r\n$3\r\nkey\r\n$1\r\n0\r\n";
    let meta = parse_resp_command_arg_slices(pexpire_now, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let hget = b"*3\r\n$4\r\nHGET\r\n$3\r\nkey\r\n$5\r\nfield\r\n";
    let meta = parse_resp_command_arg_slices(hget, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"$-1\r\n");
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
fn expire_condition_options_follow_redis_matrix() {
    let processor = RequestProcessor::new().unwrap();
    let exec = |line: &str| execute_command_line(&processor, line).unwrap();

    // Redis upstream: tests/unit/expire.tcl
    // - EXPIRE with NX option on a key with ttl
    // - EXPIRE with NX option on a key without ttl
    assert_eq!(exec("SET foo bar EX 100"), b"+OK\r\n");
    assert_eq!(exec("EXPIRE foo 200 NX"), b":0\r\n");
    let ttl_after_nx_on_volatile = parse_integer_response(&exec("TTL foo"));
    assert!((50..=100).contains(&ttl_after_nx_on_volatile));

    assert_eq!(exec("SET foo bar"), b"+OK\r\n");
    assert_eq!(exec("EXPIRE foo 200 NX"), b":1\r\n");
    let ttl_after_nx_on_persistent = parse_integer_response(&exec("TTL foo"));
    assert!((100..=200).contains(&ttl_after_nx_on_persistent));

    // Redis upstream: tests/unit/expire.tcl
    // - EXPIRE with XX option on a key with ttl
    // - EXPIRE with XX option on a key without ttl
    assert_eq!(exec("SET foo bar EX 100"), b"+OK\r\n");
    assert_eq!(exec("EXPIRE foo 200 XX"), b":1\r\n");
    let ttl_after_xx_on_volatile = parse_integer_response(&exec("TTL foo"));
    assert!((100..=200).contains(&ttl_after_xx_on_volatile));

    assert_eq!(exec("SET foo bar"), b"+OK\r\n");
    assert_eq!(exec("EXPIRE foo 200 XX"), b":0\r\n");
    assert_eq!(exec("TTL foo"), b":-1\r\n");

    // Redis upstream: tests/unit/expire.tcl
    // - EXPIRE with GT option on a key with lower ttl
    // - EXPIRE with GT option on a key with higher ttl
    // - EXPIRE with GT option on a key without ttl
    assert_eq!(exec("SET foo bar EX 100"), b"+OK\r\n");
    assert_eq!(exec("EXPIRE foo 200 GT"), b":1\r\n");
    let ttl_after_gt_raise = parse_integer_response(&exec("TTL foo"));
    assert!((100..=200).contains(&ttl_after_gt_raise));

    assert_eq!(exec("SET foo bar EX 200"), b"+OK\r\n");
    assert_eq!(exec("EXPIRE foo 100 GT"), b":0\r\n");
    let ttl_after_gt_reject = parse_integer_response(&exec("TTL foo"));
    assert!((100..=200).contains(&ttl_after_gt_reject));

    assert_eq!(exec("SET foo bar"), b"+OK\r\n");
    assert_eq!(exec("EXPIRE foo 200 GT"), b":0\r\n");
    assert_eq!(exec("TTL foo"), b":-1\r\n");

    // Redis upstream: tests/unit/expire.tcl
    // - EXPIRE with LT option on a key with higher ttl
    // - EXPIRE with LT option on a key with lower ttl
    // - EXPIRE with LT option on a key without ttl
    assert_eq!(exec("SET foo bar EX 100"), b"+OK\r\n");
    assert_eq!(exec("EXPIRE foo 200 LT"), b":0\r\n");
    let ttl_after_lt_reject = parse_integer_response(&exec("TTL foo"));
    assert!((50..=100).contains(&ttl_after_lt_reject));

    assert_eq!(exec("SET foo bar EX 200"), b"+OK\r\n");
    assert_eq!(exec("EXPIRE foo 100 LT"), b":1\r\n");
    let ttl_after_lt_shrink = parse_integer_response(&exec("TTL foo"));
    assert!((50..=100).contains(&ttl_after_lt_shrink));

    assert_eq!(exec("SET foo bar"), b"+OK\r\n");
    assert_eq!(exec("EXPIRE foo 100 LT"), b":1\r\n");
    let ttl_after_lt_on_persistent = parse_integer_response(&exec("TTL foo"));
    assert!((50..=100).contains(&ttl_after_lt_on_persistent));

    // Redis upstream: tests/unit/expire.tcl
    // - EXPIRE with LT and XX option on a key with ttl
    // - EXPIRE with LT and XX option on a key without ttl
    assert_eq!(exec("SET foo bar EX 200"), b"+OK\r\n");
    assert_eq!(exec("EXPIRE foo 100 LT XX"), b":1\r\n");
    let ttl_after_lt_xx = parse_integer_response(&exec("TTL foo"));
    assert!((50..=100).contains(&ttl_after_lt_xx));

    assert_eq!(exec("SET foo bar"), b"+OK\r\n");
    assert_eq!(exec("EXPIRE foo 200 LT XX"), b":0\r\n");
    assert_eq!(exec("TTL foo"), b":-1\r\n");
}

#[test]
fn expire_condition_option_errors_follow_redis_messages() {
    let processor = RequestProcessor::new().unwrap();
    let exec = |line: &str| execute_command_line(&processor, line).unwrap();
    assert_eq!(exec("SET foo bar"), b"+OK\r\n");

    // Redis upstream: tests/unit/expire.tcl
    // - EXPIRE with conflicting options: LT GT
    assert_eq!(
        exec("EXPIRE foo 200 LT GT"),
        b"-ERR GT and LT options at the same time are not compatible\r\n"
    );

    // Redis upstream: tests/unit/expire.tcl
    // - EXPIRE with conflicting options: NX GT / NX LT / NX XX
    assert_eq!(
        exec("EXPIRE foo 200 NX GT"),
        b"-ERR NX and XX, GT or LT options at the same time are not compatible\r\n"
    );
    assert_eq!(
        exec("EXPIRE foo 200 NX LT"),
        b"-ERR NX and XX, GT or LT options at the same time are not compatible\r\n"
    );
    assert_eq!(
        exec("EXPIRE foo 200 NX XX"),
        b"-ERR NX and XX, GT or LT options at the same time are not compatible\r\n"
    );

    // Redis upstream: tests/unit/expire.tcl
    // - EXPIRE with unsupported options
    assert_eq!(exec("EXPIRE foo 200 AB"), b"-ERR Unsupported option AB\r\n");
    assert_eq!(
        exec("EXPIRE foo 200 XX AB"),
        b"-ERR Unsupported option AB\r\n"
    );
}

#[test]
fn expire_condition_options_on_missing_and_negative_follow_redis_behavior() {
    let processor = RequestProcessor::new().unwrap();
    let exec = |line: &str| execute_command_line(&processor, line).unwrap();

    // Redis upstream: tests/unit/expire.tcl
    // - EXPIRE with non-existed key
    assert_eq!(exec("EXPIRE none 100 NX"), b":0\r\n");
    assert_eq!(exec("EXPIRE none 100 XX"), b":0\r\n");
    assert_eq!(exec("EXPIRE none 100 GT"), b":0\r\n");
    assert_eq!(exec("EXPIRE none 100 LT"), b":0\r\n");

    // Redis upstream: tests/unit/expire.tcl
    // - EXPIRE with negative expiry
    // - EXPIRE with negative expiry on a non-valitale key
    assert_eq!(exec("SET foo bar EX 100"), b"+OK\r\n");
    assert_eq!(exec("EXPIRE foo -10 LT"), b":1\r\n");
    assert_eq!(exec("TTL foo"), b":-2\r\n");

    assert_eq!(exec("SET foo bar"), b"+OK\r\n");
    assert_eq!(exec("EXPIRE foo -10 LT"), b":1\r\n");
    assert_eq!(exec("TTL foo"), b":-2\r\n");
}

#[test]
fn expireat_and_expiretime_use_unix_seconds() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 8];
    let mut response = Vec::new();

    let set = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
    let meta = parse_resp_command_arg_slices(set, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    let now_secs = current_unix_time_millis().unwrap() / 1000;
    let expireat_secs = (now_secs + 2).to_string();
    let expireat = encode_resp(&[b"EXPIREAT", b"key", expireat_secs.as_bytes()]);

    response.clear();
    let meta = parse_resp_command_arg_slices(&expireat, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let expiretime = b"*2\r\n$10\r\nEXPIRETIME\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(expiretime, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    let absolute_secs = parse_integer_response(&response);
    assert!((now_secs as i64..=now_secs as i64 + 3).contains(&absolute_secs));
}

#[test]
fn pexpireat_and_pexpiretime_report_expected_codes() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 8];
    let mut response = Vec::new();

    let pexpiretime_missing = b"*2\r\n$11\r\nPEXPIRETIME\r\n$7\r\nmissing\r\n";
    let meta = parse_resp_command_arg_slices(pexpiretime_missing, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":-2\r\n");

    response.clear();
    let set = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
    let meta = parse_resp_command_arg_slices(set, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let pexpiretime_without_expire = b"*2\r\n$11\r\nPEXPIRETIME\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(pexpiretime_without_expire, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":-1\r\n");

    let now_millis = current_unix_time_millis().unwrap();
    let pexpireat_millis = (now_millis + 1500).to_string();
    let pexpireat = encode_resp(&[b"PEXPIREAT", b"key", pexpireat_millis.as_bytes()]);

    response.clear();
    let meta = parse_resp_command_arg_slices(&pexpireat, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let meta = parse_resp_command_arg_slices(pexpiretime_without_expire, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    let absolute_millis = parse_integer_response(&response);
    assert!((now_millis as i64..=now_millis as i64 + 2000).contains(&absolute_millis));
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
fn dump_restore_and_restore_asking_roundtrip_string_payloads() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 16];
    let mut response = Vec::new();

    let dump_missing = b"*2\r\n$4\r\nDUMP\r\n$7\r\nmissing\r\n";
    let meta = parse_resp_command_arg_slices(dump_missing, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"$-1\r\n");

    response.clear();
    let set = b"*3\r\n$3\r\nSET\r\n$4\r\nrkey\r\n$5\r\nvalue\r\n";
    let meta = parse_resp_command_arg_slices(set, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let dump = b"*2\r\n$4\r\nDUMP\r\n$4\r\nrkey\r\n";
    let meta = parse_resp_command_arg_slices(dump, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    let dump_payload = parse_bulk_payload(&response).expect("dump payload must exist");

    response.clear();
    let del = b"*2\r\n$3\r\nDEL\r\n$4\r\nrkey\r\n";
    let meta = parse_resp_command_arg_slices(del, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let restore = encode_resp(&[b"RESTORE", b"rkey", b"0", &dump_payload]);
    let meta = parse_resp_command_arg_slices(&restore, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let get = b"*2\r\n$3\r\nGET\r\n$4\r\nrkey\r\n";
    let meta = parse_resp_command_arg_slices(get, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"$5\r\nvalue\r\n");

    response.clear();
    let restore_busy = encode_resp(&[b"RESTORE", b"rkey", b"0", &dump_payload]);
    let meta = parse_resp_command_arg_slices(&restore_busy, &mut args).unwrap();
    let err = processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap_err();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-BUSYKEY Target key name already exists.\r\n");

    response.clear();
    let restore_replace = encode_resp(&[b"RESTORE", b"rkey", b"1000", &dump_payload, b"REPLACE"]);
    let meta = parse_resp_command_arg_slices(&restore_replace, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let pttl = b"*2\r\n$4\r\nPTTL\r\n$4\r\nrkey\r\n";
    let meta = parse_resp_command_arg_slices(pttl, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    let ttl = parse_integer_response(&response);
    assert!(ttl > 0);
    assert!(ttl <= 1000);

    response.clear();
    let restore_asking =
        encode_resp(&[b"RESTORE-ASKING", b"rkey2", b"0", &dump_payload, b"REPLACE"]);
    let meta = parse_resp_command_arg_slices(&restore_asking, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let get_rkey2 = b"*2\r\n$3\r\nGET\r\n$5\r\nrkey2\r\n";
    let meta = parse_resp_command_arg_slices(get_rkey2, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"$5\r\nvalue\r\n");

    response.clear();
    let restore_invalid = b"*4\r\n$7\r\nRESTORE\r\n$4\r\nbadk\r\n$1\r\n0\r\n$3\r\nbad\r\n";
    let meta = parse_resp_command_arg_slices(restore_invalid, &mut args).unwrap();
    let err = processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap_err();
    err.append_resp_error(&mut response);
    assert_eq!(
        response,
        b"-ERR DUMP payload version or checksum are wrong\r\n"
    );
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
fn dbsize_does_not_lazy_expire_until_key_access() {
    let processor = RequestProcessor::new().unwrap();
    assert_command_response(&processor, "DEBUG SET-ACTIVE-EXPIRE 0", b"+OK\r\n");
    assert_command_response(&processor, "PSETEX key1 500 a", b"+OK\r\n");
    assert_command_response(&processor, "PSETEX key2 500 a", b"+OK\r\n");
    assert_command_response(&processor, "PSETEX key3 500 a", b"+OK\r\n");

    assert_command_integer(&processor, "DBSIZE", 3);
    thread::sleep(Duration::from_millis(650));
    assert_command_integer(&processor, "DBSIZE", 3);

    assert_command_response(
        &processor,
        "MGET key1 key2 key3",
        b"*3\r\n$-1\r\n$-1\r\n$-1\r\n",
    );
    assert_command_integer(&processor, "DBSIZE", 0);
}

#[test]
fn lazy_expire_tracks_replication_delete_keys_on_read_access() {
    let processor = RequestProcessor::new().unwrap();
    assert_command_response(&processor, "DEBUG SET-ACTIVE-EXPIRE 0", b"+OK\r\n");
    assert_command_response(&processor, "PSETEX lazy:key 1 value", b"+OK\r\n");
    thread::sleep(Duration::from_millis(10));

    assert_command_response(&processor, "GET lazy:key", b"$-1\r\n");
    let queued = processor.take_lazy_expired_keys_for_replication();
    assert_eq!(queued, vec![b"lazy:key".to_vec()]);
    assert!(
        processor
            .take_lazy_expired_keys_for_replication()
            .is_empty()
    );
}

#[test]
fn flushdb_clears_string_and_object_keys() {
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
    let flushdb = b"*1\r\n$7\r\nFLUSHDB\r\n";
    let meta = parse_resp_command_arg_slices(flushdb, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let dbsize = b"*1\r\n$6\r\nDBSIZE\r\n";
    let meta = parse_resp_command_arg_slices(dbsize, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":0\r\n");
}

#[test]
fn flushall_clears_keys_across_string_and_object_store() {
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
    let sadd = b"*4\r\n$4\r\nSADD\r\n$4\r\nsset\r\n$1\r\na\r\n$1\r\nb\r\n";
    let meta = parse_resp_command_arg_slices(sadd, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":2\r\n");

    response.clear();
    let flushall = b"*1\r\n$8\r\nFLUSHALL\r\n";
    let meta = parse_resp_command_arg_slices(flushall, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let dbsize = b"*1\r\n$6\r\nDBSIZE\r\n";
    let meta = parse_resp_command_arg_slices(dbsize, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":0\r\n");
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
    assert!(
        response
            .windows("expired_keys:0".len())
            .any(|w| w == b"expired_keys:0")
    );
    assert!(
        response
            .windows("expired_keys_active:0".len())
            .any(|w| w == b"expired_keys_active:0")
    );

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
fn info_supports_section_filters_and_multi_section_arguments() {
    let processor = RequestProcessor::new().unwrap();

    let info_default = parse_bulk_payload(&execute_frame(&processor, &encode_resp(&[b"INFO"])))
        .expect("INFO returns bulk payload");
    let info_default_text = String::from_utf8_lossy(&info_default);
    assert!(info_default_text.contains("redis_version:garnet-rs"));
    assert!(info_default_text.contains("used_cpu_user:"));
    assert!(info_default_text.contains("used_memory:"));
    assert!(info_default_text.contains("master_repl_offset:"));
    assert!(!info_default_text.contains("rejected_calls"));

    let info_default_section = parse_bulk_payload(&execute_frame(
        &processor,
        &encode_resp(&[b"INFO", b"default"]),
    ))
    .expect("INFO default returns bulk payload");
    let info_default_section_text = String::from_utf8_lossy(&info_default_section);
    assert!(info_default_section_text.contains("used_cpu_user:"));
    assert!(info_default_section_text.contains("used_memory:"));
    assert!(info_default_section_text.contains("master_repl_offset:"));
    assert!(!info_default_section_text.contains("rejected_calls"));

    let info_cpu = parse_bulk_payload(&execute_frame(&processor, &encode_resp(&[b"INFO", b"cpu"])))
        .expect("INFO cpu returns bulk payload");
    let info_cpu_text = String::from_utf8_lossy(&info_cpu);
    assert!(info_cpu_text.contains("used_cpu_user:"));
    assert!(!info_cpu_text.contains("used_memory:"));
    assert!(!info_cpu_text.contains("master_repl_offset:"));

    let info_cpu_sentinel = parse_bulk_payload(&execute_frame(
        &processor,
        &encode_resp(&[b"INFO", b"cpu", b"sentinel"]),
    ))
    .expect("INFO cpu sentinel returns bulk payload");
    let info_cpu_sentinel_text = String::from_utf8_lossy(&info_cpu_sentinel);
    assert!(info_cpu_sentinel_text.contains("used_cpu_user:"));
    assert!(!info_cpu_sentinel_text.contains("master_repl_offset:"));

    processor.record_command_call(b"set");
    let info_all = parse_bulk_payload(&execute_frame(&processor, &encode_resp(&[b"INFO", b"all"])))
        .expect("INFO all returns bulk payload");
    let info_all_text = String::from_utf8_lossy(&info_all);
    assert!(info_all_text.contains("used_cpu_user:"));
    assert!(info_all_text.contains("used_memory:"));
    assert!(info_all_text.contains("master_repl_offset:"));
    assert!(info_all_text.contains("rejected_calls"));
    assert_eq!(info_all_text.matches("used_cpu_user_children").count(), 1);

    let info_everything = parse_bulk_payload(&execute_frame(
        &processor,
        &encode_resp(&[b"INFO", b"everything"]),
    ))
    .expect("INFO everything returns bulk payload");
    let info_everything_text = String::from_utf8_lossy(&info_everything);
    assert!(info_everything_text.contains("rejected_calls"));

    let info_cpu_default = parse_bulk_payload(&execute_frame(
        &processor,
        &encode_resp(&[b"INFO", b"cpu", b"default"]),
    ))
    .expect("INFO cpu default returns bulk payload");
    let info_cpu_default_text = String::from_utf8_lossy(&info_cpu_default);
    assert!(info_cpu_default_text.contains("used_cpu_user:"));
    assert!(info_cpu_default_text.contains("used_memory:"));
    assert!(info_cpu_default_text.contains("master_repl_offset:"));
    assert!(!info_cpu_default_text.contains("rejected_calls"));
    assert_eq!(
        info_cpu_default_text
            .matches("used_cpu_user_children")
            .count(),
        1
    );

    let info_commandstats = parse_bulk_payload(&execute_frame(
        &processor,
        &encode_resp(&[b"INFO", b"commandstats"]),
    ))
    .expect("INFO commandstats returns bulk payload");
    let info_commandstats_text = String::from_utf8_lossy(&info_commandstats);
    assert!(info_commandstats_text.contains("rejected_calls"));
    assert!(!info_commandstats_text.contains("used_memory:"));
}

#[test]
fn memory_usage_reports_positive_values_and_null_for_missing_key() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 8];
    let mut response = Vec::new();

    let memory_missing = b"*3\r\n$6\r\nMEMORY\r\n$5\r\nUSAGE\r\n$7\r\nmissing\r\n";
    let meta = parse_resp_command_arg_slices(memory_missing, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"$-1\r\n");

    response.clear();
    let set = b"*3\r\n$3\r\nSET\r\n$4\r\nskey\r\n$5\r\nvalue\r\n";
    let meta = parse_resp_command_arg_slices(set, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let memory_string = b"*3\r\n$6\r\nMEMORY\r\n$5\r\nUSAGE\r\n$4\r\nskey\r\n";
    let meta = parse_resp_command_arg_slices(memory_string, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert!(response.starts_with(b":"));
    assert_ne!(response, b":0\r\n");

    response.clear();
    let hset = b"*4\r\n$4\r\nHSET\r\n$4\r\nhkey\r\n$5\r\nfield\r\n$5\r\nvalue\r\n";
    let meta = parse_resp_command_arg_slices(hset, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let memory_hash = b"*3\r\n$6\r\nMEMORY\r\n$5\r\nUSAGE\r\n$4\r\nhkey\r\n";
    let meta = parse_resp_command_arg_slices(memory_hash, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert!(response.starts_with(b":"));
    assert_ne!(response, b":0\r\n");
}

#[test]
fn keys_returns_glob_matches_across_string_and_object_keys() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 8];
    let mut response = Vec::new();

    let set_foo1 = b"*3\r\n$3\r\nSET\r\n$4\r\nfoo1\r\n$1\r\n1\r\n";
    let meta = parse_resp_command_arg_slices(set_foo1, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();

    response.clear();
    let set_foo2 = b"*3\r\n$3\r\nSET\r\n$4\r\nfoo2\r\n$1\r\n2\r\n";
    let meta = parse_resp_command_arg_slices(set_foo2, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();

    response.clear();
    let set_bar = b"*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$1\r\n3\r\n";
    let meta = parse_resp_command_arg_slices(set_bar, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();

    response.clear();
    let hset_foo3 = b"*4\r\n$4\r\nHSET\r\n$4\r\nfoo3\r\n$1\r\nf\r\n$1\r\nv\r\n";
    let meta = parse_resp_command_arg_slices(hset_foo3, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();

    response.clear();
    let keys_foo = b"*2\r\n$4\r\nKEYS\r\n$4\r\nfoo*\r\n";
    let meta = parse_resp_command_arg_slices(keys_foo, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert!(response.starts_with(b"*3\r\n"));
    assert!(response.windows(10).any(|w| w == b"$4\r\nfoo1\r\n"));
    assert!(response.windows(10).any(|w| w == b"$4\r\nfoo2\r\n"));
    assert!(response.windows(10).any(|w| w == b"$4\r\nfoo3\r\n"));

    response.clear();
    let keys_all = b"*2\r\n$4\r\nKEYS\r\n$1\r\n*\r\n";
    let meta = parse_resp_command_arg_slices(keys_all, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert!(response.starts_with(b"*4\r\n"));
}

#[test]
fn randomkey_returns_existing_keys_and_null_for_empty_db() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 8];
    let mut response = Vec::new();

    let randomkey = b"*1\r\n$9\r\nRANDOMKEY\r\n";
    let meta = parse_resp_command_arg_slices(randomkey, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"$-1\r\n");

    response.clear();
    let set_foo = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$1\r\n1\r\n";
    let meta = parse_resp_command_arg_slices(set_foo, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let hset_bar = b"*4\r\n$4\r\nHSET\r\n$3\r\nbar\r\n$1\r\nf\r\n$1\r\n2\r\n";
    let meta = parse_resp_command_arg_slices(hset_bar, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":1\r\n");

    let mut seen_foo = false;
    let mut seen_bar = false;
    for _ in 0..8 {
        response.clear();
        let meta = parse_resp_command_arg_slices(randomkey, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        if response == b"$3\r\nfoo\r\n" {
            seen_foo = true;
        }
        if response == b"$3\r\nbar\r\n" {
            seen_bar = true;
        }
    }
    assert!(seen_foo);
    assert!(seen_bar);
}

#[test]
fn scan_supports_cursor_match_count_and_type_filters() {
    let processor = RequestProcessor::new().unwrap();

    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"SET", b"a_key", b"1"])),
        b"+OK\r\n"
    );
    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"SET", b"b_key", b"2"])),
        b"+OK\r\n"
    );
    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"SET", b"c_key", b"3"])),
        b"+OK\r\n"
    );

    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"HSET", b"hkey", b"field", b"value"])
        ),
        b":1\r\n"
    );
    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"SADD", b"setkey", b"member"])),
        b":1\r\n"
    );

    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"SCAN", b"0", b"COUNT", b"2"])),
        b"*2\r\n$1\r\n2\r\n*2\r\n$5\r\na_key\r\n$5\r\nb_key\r\n"
    );
    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"SCAN", b"2", b"COUNT", b"2"])),
        b"*2\r\n$1\r\n4\r\n*2\r\n$5\r\nc_key\r\n$4\r\nhkey\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"SCAN", b"0", b"MATCH", b"*key", b"TYPE", b"set"])
        ),
        b"*2\r\n$1\r\n0\r\n*1\r\n$6\r\nsetkey\r\n"
    );
}

#[test]
fn hscan_supports_cursor_match_and_count() {
    let processor = RequestProcessor::new().unwrap();
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"HSET", b"h", b"a", b"1", b"b", b"2", b"c", b"3"])
        ),
        b":3\r\n"
    );

    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"HSCAN", b"h", b"0", b"COUNT", b"2"])
        ),
        b"*2\r\n$1\r\n2\r\n*4\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"HSCAN", b"h", b"2", b"COUNT", b"2"])
        ),
        b"*2\r\n$1\r\n0\r\n*2\r\n$1\r\nc\r\n$1\r\n3\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"HSCAN", b"h", b"0", b"MATCH", b"b*", b"COUNT", b"10"])
        ),
        b"*2\r\n$1\r\n0\r\n*2\r\n$1\r\nb\r\n$1\r\n2\r\n"
    );
}

#[test]
fn sscan_supports_cursor_match_and_count() {
    let processor = RequestProcessor::new().unwrap();
    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"SADD", b"s", b"c", b"a", b"b"])),
        b":3\r\n"
    );

    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"SSCAN", b"s", b"0", b"COUNT", b"2"])
        ),
        b"*2\r\n$1\r\n2\r\n*2\r\n$1\r\na\r\n$1\r\nb\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"SSCAN", b"s", b"0", b"MATCH", b"c*", b"COUNT", b"10"])
        ),
        b"*2\r\n$1\r\n0\r\n*1\r\n$1\r\nc\r\n"
    );
}

#[test]
fn zscan_supports_cursor_match_and_count() {
    let processor = RequestProcessor::new().unwrap();
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"ZADD", b"z", b"1", b"a", b"2", b"b", b"3", b"c"])
        ),
        b":3\r\n"
    );

    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"ZSCAN", b"z", b"0", b"COUNT", b"2"])
        ),
        b"*2\r\n$1\r\n2\r\n*4\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"ZSCAN", b"z", b"2", b"COUNT", b"2"])
        ),
        b"*2\r\n$1\r\n0\r\n*2\r\n$1\r\nc\r\n$1\r\n3\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"ZSCAN", b"z", b"0", b"MATCH", b"b*", b"COUNT", b"10"])
        ),
        b"*2\r\n$1\r\n0\r\n*2\r\n$1\r\nb\r\n$1\r\n2\r\n"
    );
}

#[test]
fn setex_sets_value_with_expiration() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 8];
    let mut response = Vec::new();

    let setex = b"*4\r\n$5\r\nSETEX\r\n$3\r\nkey\r\n$2\r\n10\r\n$5\r\nvalue\r\n";
    let meta = parse_resp_command_arg_slices(setex, &mut args).unwrap();
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
    assert_eq!(response, b"$5\r\nvalue\r\n");

    response.clear();
    let ttl = b"*2\r\n$3\r\nTTL\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(ttl, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert!(response.starts_with(b":"));
    let ttl_value = std::str::from_utf8(&response[1..response.len() - 2])
        .unwrap()
        .parse::<i64>()
        .unwrap();
    assert!(ttl_value > 0);
}

#[test]
fn setnx_sets_only_when_key_absent() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 8];
    let mut response = Vec::new();

    let setnx_first = b"*3\r\n$5\r\nSETNX\r\n$3\r\nkey\r\n$3\r\none\r\n";
    let meta = parse_resp_command_arg_slices(setnx_first, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let setnx_second = b"*3\r\n$5\r\nSETNX\r\n$3\r\nkey\r\n$3\r\ntwo\r\n";
    let meta = parse_resp_command_arg_slices(setnx_second, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let get = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(get, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"$3\r\none\r\n");
}

#[test]
fn strlen_returns_zero_for_missing_and_length_for_string() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 8];
    let mut response = Vec::new();

    let strlen_missing = b"*2\r\n$6\r\nSTRLEN\r\n$7\r\nmissing\r\n";
    let meta = parse_resp_command_arg_slices(strlen_missing, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let set = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
    let meta = parse_resp_command_arg_slices(set, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let strlen = b"*2\r\n$6\r\nSTRLEN\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(strlen, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":5\r\n");
}

#[test]
fn getrange_and_substr_return_expected_slices() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 8];
    let mut response = Vec::new();

    let set = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$10\r\n0123456789\r\n";
    let meta = parse_resp_command_arg_slices(set, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let getrange = b"*4\r\n$8\r\nGETRANGE\r\n$3\r\nkey\r\n$1\r\n2\r\n$1\r\n5\r\n";
    let meta = parse_resp_command_arg_slices(getrange, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"$4\r\n2345\r\n");

    response.clear();
    let getrange_negative = b"*4\r\n$8\r\nGETRANGE\r\n$3\r\nkey\r\n$2\r\n-3\r\n$2\r\n-1\r\n";
    let meta = parse_resp_command_arg_slices(getrange_negative, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"$3\r\n789\r\n");

    response.clear();
    let substr = b"*4\r\n$6\r\nSUBSTR\r\n$3\r\nkey\r\n$1\r\n1\r\n$1\r\n3\r\n";
    let meta = parse_resp_command_arg_slices(substr, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"$3\r\n123\r\n");
}

#[test]
fn getbit_setbit_setrange_and_bitcount_follow_string_semantics() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 8];
    let mut response = Vec::new();

    let setrange = b"*4\r\n$8\r\nSETRANGE\r\n$3\r\nkey\r\n$1\r\n0\r\n$3\r\nabc\r\n";
    let meta = parse_resp_command_arg_slices(setrange, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":3\r\n");

    response.clear();
    let getbit = b"*3\r\n$6\r\nGETBIT\r\n$3\r\nkey\r\n$1\r\n6\r\n";
    let meta = parse_resp_command_arg_slices(getbit, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let setbit = b"*4\r\n$6\r\nSETBIT\r\n$3\r\nkey\r\n$1\r\n6\r\n$1\r\n1\r\n";
    let meta = parse_resp_command_arg_slices(setbit, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let meta = parse_resp_command_arg_slices(getbit, &mut args).unwrap();
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
    assert_eq!(response, b"$3\r\ncbc\r\n");

    response.clear();
    let bitcount = b"*2\r\n$8\r\nBITCOUNT\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(bitcount, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":11\r\n");

    response.clear();
    let bitcount_byte =
        b"*5\r\n$8\r\nBITCOUNT\r\n$3\r\nkey\r\n$1\r\n0\r\n$1\r\n0\r\n$4\r\nBYTE\r\n";
    let meta = parse_resp_command_arg_slices(bitcount_byte, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":4\r\n");

    response.clear();
    let bitcount_bit = b"*5\r\n$8\r\nBITCOUNT\r\n$3\r\nkey\r\n$1\r\n8\r\n$2\r\n15\r\n$3\r\nBIT\r\n";
    let meta = parse_resp_command_arg_slices(bitcount_bit, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":3\r\n");

    assert_command_integer(&processor, "BITPOS missing 0", 0);
    assert_command_integer(&processor, "BITPOS missing 1", -1);

    for bit in 0..8 {
        assert_command_integer(&processor, &format!("SETBIT bpff {bit} 1"), 0);
    }
    assert_command_integer(&processor, "BITPOS bpff 0", 8);
    assert_command_integer(&processor, "BITPOS bpff 1", 0);
    assert_command_integer(&processor, "BITPOS bpff 0 1", -1);
    assert_command_integer(&processor, "BITPOS bpff 0 0 0", -1);
    assert_command_integer(&processor, "BITPOS bpff 0 0 7 BIT", -1);

    assert_command_integer(&processor, "SETBIT bpbits 8 1", 0);
    assert_command_integer(&processor, "BITPOS bpbits 1 8 15 BIT", 8);
    assert_command_integer(&processor, "BITPOS bpbits 1 16 16 BIT", -1);

    response.clear();
    let set_b1 = b"*3\r\n$3\r\nSET\r\n$2\r\nb1\r\n$1\r\nA\r\n";
    let meta = parse_resp_command_arg_slices(set_b1, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let set_b2 = b"*3\r\n$3\r\nSET\r\n$2\r\nb2\r\n$1\r\na\r\n";
    let meta = parse_resp_command_arg_slices(set_b2, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let bitop_and = b"*5\r\n$5\r\nBITOP\r\n$3\r\nAND\r\n$4\r\nbdst\r\n$2\r\nb1\r\n$2\r\nb2\r\n";
    let meta = parse_resp_command_arg_slices(bitop_and, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let get_bdst = b"*2\r\n$3\r\nGET\r\n$4\r\nbdst\r\n";
    let meta = parse_resp_command_arg_slices(get_bdst, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"$1\r\nA\r\n");

    response.clear();
    let bitop_or = b"*5\r\n$5\r\nBITOP\r\n$2\r\nOR\r\n$4\r\nbdst\r\n$2\r\nb1\r\n$2\r\nb2\r\n";
    let meta = parse_resp_command_arg_slices(bitop_or, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let meta = parse_resp_command_arg_slices(get_bdst, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"$1\r\na\r\n");

    response.clear();
    let bitop_xor = b"*5\r\n$5\r\nBITOP\r\n$3\r\nXOR\r\n$4\r\nbdst\r\n$2\r\nb1\r\n$2\r\nb2\r\n";
    let meta = parse_resp_command_arg_slices(bitop_xor, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let meta = parse_resp_command_arg_slices(get_bdst, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"$1\r\n \r\n");

    response.clear();
    let bitop_not = b"*4\r\n$5\r\nBITOP\r\n$3\r\nNOT\r\n$4\r\nbdst\r\n$2\r\nb1\r\n";
    let meta = parse_resp_command_arg_slices(bitop_not, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let meta = parse_resp_command_arg_slices(get_bdst, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"$1\r\n\xbe\r\n");

    response.clear();
    let bitop_one = b"*5\r\n$5\r\nBITOP\r\n$3\r\nONE\r\n$4\r\nbdst\r\n$2\r\nb1\r\n$2\r\nb2\r\n";
    let meta = parse_resp_command_arg_slices(bitop_one, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let meta = parse_resp_command_arg_slices(get_bdst, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"$1\r\n \r\n");

    response.clear();
    let bitop_diff = b"*5\r\n$5\r\nBITOP\r\n$4\r\nDIFF\r\n$4\r\nbdst\r\n$2\r\nb1\r\n$2\r\nb2\r\n";
    let meta = parse_resp_command_arg_slices(bitop_diff, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let meta = parse_resp_command_arg_slices(get_bdst, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"$1\r\n\x00\r\n");

    response.clear();
    let bitop_diff1 = b"*5\r\n$5\r\nBITOP\r\n$5\r\nDIFF1\r\n$4\r\nbdst\r\n$2\r\nb1\r\n$2\r\nb2\r\n";
    let meta = parse_resp_command_arg_slices(bitop_diff1, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let meta = parse_resp_command_arg_slices(get_bdst, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"$1\r\n \r\n");

    response.clear();
    let bitop_andor = b"*5\r\n$5\r\nBITOP\r\n$5\r\nANDOR\r\n$4\r\nbdst\r\n$2\r\nb1\r\n$2\r\nb2\r\n";
    let meta = parse_resp_command_arg_slices(bitop_andor, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let meta = parse_resp_command_arg_slices(get_bdst, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"$1\r\nA\r\n");

    response.clear();
    let bitop_and_missing =
        b"*5\r\n$5\r\nBITOP\r\n$3\r\nAND\r\n$6\r\nbempty\r\n$8\r\nmissing1\r\n$8\r\nmissing2\r\n";
    let meta = parse_resp_command_arg_slices(bitop_and_missing, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let exists_bempty = b"*2\r\n$6\r\nEXISTS\r\n$6\r\nbempty\r\n";
    let meta = parse_resp_command_arg_slices(exists_bempty, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let setrange_extend = b"*4\r\n$8\r\nSETRANGE\r\n$3\r\nkey\r\n$1\r\n5\r\n$1\r\nZ\r\n";
    let meta = parse_resp_command_arg_slices(setrange_extend, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":6\r\n");

    response.clear();
    let getbit_missing = b"*3\r\n$6\r\nGETBIT\r\n$7\r\nmissing\r\n$1\r\n0\r\n";
    let meta = parse_resp_command_arg_slices(getbit_missing, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let bitcount_missing = b"*2\r\n$8\r\nBITCOUNT\r\n$7\r\nmissing\r\n";
    let meta = parse_resp_command_arg_slices(bitcount_missing, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let bitcount_invalid_mode =
        b"*5\r\n$8\r\nBITCOUNT\r\n$3\r\nkey\r\n$1\r\n0\r\n$1\r\n1\r\n$5\r\nWORDS\r\n";
    let meta = parse_resp_command_arg_slices(bitcount_invalid_mode, &mut args).unwrap();
    let err = processor
        .execute(&args[..meta.argument_count], &mut response)
        .err()
        .unwrap();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR syntax error\r\n");

    response.clear();
    let set_empty = b"*3\r\n$3\r\nSET\r\n$8\r\nbitempty\r\n$0\r\n\r\n";
    let meta = parse_resp_command_arg_slices(set_empty, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");
    assert_command_integer(&processor, "BITPOS bitempty 0", -1);

    response.clear();
    let bitpos_invalid_mode =
        b"*6\r\n$6\r\nBITPOS\r\n$4\r\nbpff\r\n$1\r\n0\r\n$1\r\n0\r\n$1\r\n7\r\n$5\r\nWORDS\r\n";
    let meta = parse_resp_command_arg_slices(bitpos_invalid_mode, &mut args).unwrap();
    let err = processor
        .execute(&args[..meta.argument_count], &mut response)
        .err()
        .unwrap();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR syntax error\r\n");

    response.clear();
    let bitpos_invalid_bit = b"*3\r\n$6\r\nBITPOS\r\n$4\r\nbpff\r\n$1\r\n2\r\n";
    let meta = parse_resp_command_arg_slices(bitpos_invalid_bit, &mut args).unwrap();
    let err = processor
        .execute(&args[..meta.argument_count], &mut response)
        .err()
        .unwrap();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR value is out of range\r\n");

    response.clear();
    let hset = b"*4\r\n$4\r\nHSET\r\n$2\r\noh\r\n$1\r\nf\r\n$1\r\nv\r\n";
    let meta = parse_resp_command_arg_slices(hset, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let bitop_wrongtype =
        b"*5\r\n$5\r\nBITOP\r\n$2\r\nOR\r\n$4\r\nbdst\r\n$2\r\noh\r\n$2\r\nb1\r\n";
    let meta = parse_resp_command_arg_slices(bitop_wrongtype, &mut args).unwrap();
    let err = processor
        .execute(&args[..meta.argument_count], &mut response)
        .err()
        .unwrap();
    err.append_resp_error(&mut response);
    assert_eq!(
        response,
        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
    );

    response.clear();
    let bitpos_wrongtype = b"*3\r\n$6\r\nBITPOS\r\n$2\r\noh\r\n$1\r\n1\r\n";
    let meta = parse_resp_command_arg_slices(bitpos_wrongtype, &mut args).unwrap();
    let err = processor
        .execute(&args[..meta.argument_count], &mut response)
        .err()
        .unwrap();
    err.append_resp_error(&mut response);
    assert_eq!(
        response,
        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
    );

    assert_command_response(
        &processor,
        "BITOP NOT bdst b1 b2",
        b"-ERR BITOP NOT must be called with a single source key.\r\n",
    );
    assert_command_response(
        &processor,
        "BITOP DIFF bdst b1",
        b"-ERR BITOP DIFF must be called with multiple source keys.\r\n",
    );
}

#[test]
fn bitfield_and_bitfield_ro_cover_wrap_sat_fail_and_validation_paths() {
    let processor = RequestProcessor::new().unwrap();

    assert_command_response(&processor, "BITFIELD bfw GET u8 0", b"*1\r\n:0\r\n");
    assert_command_integer(&processor, "EXISTS bfw", 0);

    assert_command_response(&processor, "BITFIELD bfw SET i8 0 127", b"*1\r\n:0\r\n");
    assert_command_response(&processor, "BITFIELD bfw INCRBY i8 0 1", b"*1\r\n:-128\r\n");
    assert_command_response(&processor, "BITFIELD bfw GET i8 0", b"*1\r\n:-128\r\n");

    assert_command_response(&processor, "BITFIELD bfw SET i8 0 127", b"*1\r\n:-128\r\n");
    assert_command_response(
        &processor,
        "BITFIELD bfw OVERFLOW SAT INCRBY i8 0 1",
        b"*1\r\n:127\r\n",
    );
    assert_command_response(&processor, "BITFIELD bfw GET i8 0", b"*1\r\n:127\r\n");

    assert_command_response(&processor, "BITFIELD bfw SET i8 0 127", b"*1\r\n:127\r\n");
    assert_command_response(
        &processor,
        "BITFIELD bfw OVERFLOW FAIL INCRBY i8 0 1",
        b"*1\r\n$-1\r\n",
    );
    assert_command_response(&processor, "BITFIELD bfw GET i8 0", b"*1\r\n:127\r\n");

    assert_command_response(
        &processor,
        "BITFIELD bfn SET u4 #1 15 GET u4 #1",
        b"*2\r\n:0\r\n:15\r\n",
    );
    assert_command_response(&processor, "BITFIELD_RO bfw GET i8 0", b"*1\r\n:127\r\n");

    assert_command_response(&processor, "BITFIELD bfw", b"*0\r\n");
    assert_command_error(
        &processor,
        "BITFIELD bfw OVERFLOW WRAP",
        b"-ERR syntax error\r\n",
    );
    assert_command_error(
        &processor,
        "BITFIELD bfw GET i8 -1",
        b"-ERR value is out of range\r\n",
    );
    assert_command_error(
        &processor,
        "BITFIELD bfw GET i8 bad",
        b"-ERR value is not an integer or out of range\r\n",
    );
    assert_command_response(
        &processor,
        "BITFIELD_RO bfw SET i8 0 1",
        b"-ERR BITFIELD_RO only supports the GET subcommand\r\n",
    );
    assert_command_response(
        &processor,
        "BITFIELD_RO bfw INCRBY i8 0 1",
        b"-ERR BITFIELD_RO only supports the GET subcommand\r\n",
    );

    assert_command_integer(&processor, "HSET hbf f v", 1);
    assert_command_error(
        &processor,
        "BITFIELD hbf GET u8 0",
        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
    );
}

#[test]
fn lcs_supports_sequence_len_idx_and_validation_paths() {
    let processor = RequestProcessor::new().unwrap();

    assert_command_response(&processor, "SET lcs1 ohmytext", b"+OK\r\n");
    assert_command_response(&processor, "SET lcs2 mynewtext", b"+OK\r\n");
    assert_command_response(&processor, "LCS lcs1 lcs2", b"$6\r\nmytext\r\n");
    assert_command_response(&processor, "LCS lcs1 lcs2 LEN", b":6\r\n");
    assert_command_response(
        &processor,
        "LCS lcs1 lcs2 IDX",
        b"*4\r\n$7\r\nmatches\r\n*2\r\n*2\r\n*2\r\n:2\r\n:3\r\n*2\r\n:0\r\n:1\r\n*2\r\n*2\r\n:4\r\n:7\r\n*2\r\n:5\r\n:8\r\n$3\r\nlen\r\n:6\r\n",
    );
    assert_command_response(
        &processor,
        "LCS lcs1 lcs2 IDX MINMATCHLEN 3 WITHMATCHLEN",
        b"*4\r\n$7\r\nmatches\r\n*1\r\n*3\r\n*2\r\n:4\r\n:7\r\n*2\r\n:5\r\n:8\r\n:4\r\n$3\r\nlen\r\n:6\r\n",
    );

    assert_command_response(&processor, "LCS missing1 missing2", b"$0\r\n\r\n");
    assert_command_response(&processor, "LCS missing1 missing2 LEN", b":0\r\n");

    assert_command_error(
        &processor,
        "LCS lcs1 lcs2 LEN IDX",
        b"-ERR syntax error\r\n",
    );
    assert_command_error(
        &processor,
        "LCS lcs1 lcs2 WITHMATCHLEN",
        b"-ERR syntax error\r\n",
    );
    assert_command_error(
        &processor,
        "LCS lcs1 lcs2 MINMATCHLEN 1",
        b"-ERR syntax error\r\n",
    );
    assert_command_error(
        &processor,
        "LCS lcs1 lcs2 IDX MINMATCHLEN -1",
        b"-ERR value is out of range\r\n",
    );
    assert_command_error(
        &processor,
        "LCS lcs1 lcs2 IDX MINMATCHLEN bad",
        b"-ERR value is not an integer or out of range\r\n",
    );

    assert_command_integer(&processor, "HSET hlcs f v", 1);
    assert_command_error(
        &processor,
        "LCS hlcs lcs2",
        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
    );
}

#[test]
fn sort_and_sort_ro_support_options_store_and_validation_paths() {
    let processor = RequestProcessor::new().unwrap();

    assert_command_integer(&processor, "RPUSH sortnum 3 1 2", 3);
    assert_command_response(
        &processor,
        "SORT sortnum",
        b"*3\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n",
    );
    assert_command_response(
        &processor,
        "SORT sortnum DESC",
        b"*3\r\n$1\r\n3\r\n$1\r\n2\r\n$1\r\n1\r\n",
    );
    assert_command_response(&processor, "SORT sortnum LIMIT 1 1", b"*1\r\n$1\r\n2\r\n");

    assert_command_integer(&processor, "RPUSH sortalpha b a c", 3);
    assert_command_response(
        &processor,
        "SORT sortalpha ALPHA",
        b"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n",
    );
    assert_command_response(
        &processor,
        "SORT_RO sortalpha ALPHA DESC",
        b"*3\r\n$1\r\nc\r\n$1\r\nb\r\n$1\r\na\r\n",
    );

    assert_command_integer(&processor, "RPUSH sortby one two three", 3);
    assert_command_response(&processor, "SET weight:one 5", b"+OK\r\n");
    assert_command_response(&processor, "SET weight:two 1", b"+OK\r\n");
    assert_command_response(&processor, "SET weight:three 3", b"+OK\r\n");
    assert_command_response(&processor, "SET data:one O", b"+OK\r\n");
    assert_command_response(&processor, "SET data:two T", b"+OK\r\n");
    assert_command_response(&processor, "SET data:three H", b"+OK\r\n");
    assert_command_response(
        &processor,
        "SORT sortby BY weight:*",
        b"*3\r\n$3\r\ntwo\r\n$5\r\nthree\r\n$3\r\none\r\n",
    );
    assert_command_response(
        &processor,
        "SORT sortby BY weight:* GET data:* GET #",
        b"*6\r\n$1\r\nT\r\n$3\r\ntwo\r\n$1\r\nH\r\n$5\r\nthree\r\n$1\r\nO\r\n$3\r\none\r\n",
    );

    assert_command_integer(&processor, "SORT sortby BY weight:* STORE sortdest", 3);
    assert_command_response(
        &processor,
        "LRANGE sortdest 0 -1",
        b"*3\r\n$3\r\ntwo\r\n$5\r\nthree\r\n$3\r\none\r\n",
    );
    assert_command_error(
        &processor,
        "SORT_RO sortby STORE no",
        b"-ERR syntax error\r\n",
    );

    assert_command_response(&processor, "SET sortstr hello", b"+OK\r\n");
    assert_command_error(
        &processor,
        "SORT sortstr",
        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
    );

    assert_command_integer(&processor, "RPUSH sortbad bad", 1);
    assert_command_response(&processor, "SET weight:bad notfloat", b"+OK\r\n");
    assert_command_error(
        &processor,
        "SORT sortbad BY weight:*",
        b"-ERR value is not a valid float\r\n",
    );
}

#[test]
fn psetex_sets_value_with_millisecond_expiration() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 8];
    let mut response = Vec::new();

    let psetex = b"*4\r\n$6\r\nPSETEX\r\n$3\r\nkey\r\n$3\r\n250\r\n$5\r\nvalue\r\n";
    let meta = parse_resp_command_arg_slices(psetex, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let pttl = b"*2\r\n$4\r\nPTTL\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(pttl, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    let ttl_millis = parse_integer_response(&response);
    assert!((0..=250).contains(&ttl_millis));
}

#[test]
fn getset_returns_old_value_and_overwrites_key() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 8];
    let mut response = Vec::new();

    let getset_missing = b"*3\r\n$6\r\nGETSET\r\n$3\r\nkey\r\n$3\r\none\r\n";
    let meta = parse_resp_command_arg_slices(getset_missing, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"$-1\r\n");

    response.clear();
    let getset_existing = b"*3\r\n$6\r\nGETSET\r\n$3\r\nkey\r\n$3\r\ntwo\r\n";
    let meta = parse_resp_command_arg_slices(getset_existing, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"$3\r\none\r\n");

    response.clear();
    let get = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(get, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"$3\r\ntwo\r\n");
}

#[test]
fn getdel_returns_value_then_removes_key() {
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
    let getdel = b"*2\r\n$6\r\nGETDEL\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(getdel, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"$5\r\nvalue\r\n");

    response.clear();
    let get = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(get, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"$-1\r\n");

    response.clear();
    let meta = parse_resp_command_arg_slices(getdel, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"$-1\r\n");
}

#[test]
fn append_and_incrbyfloat_update_string_values() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 8];
    let mut response = Vec::new();

    let set = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$1\r\na\r\n";
    let meta = parse_resp_command_arg_slices(set, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let append = b"*3\r\n$6\r\nAPPEND\r\n$3\r\nkey\r\n$2\r\nbc\r\n";
    let meta = parse_resp_command_arg_slices(append, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":3\r\n");

    response.clear();
    let get = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(get, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"$3\r\nabc\r\n");

    response.clear();
    let incrbyfloat = b"*3\r\n$11\r\nINCRBYFLOAT\r\n$3\r\nnum\r\n$4\r\n1.25\r\n";
    let meta = parse_resp_command_arg_slices(incrbyfloat, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"$4\r\n1.25\r\n");

    response.clear();
    let incrbyfloat_again = b"*3\r\n$11\r\nINCRBYFLOAT\r\n$3\r\nnum\r\n$4\r\n0.75\r\n";
    let meta = parse_resp_command_arg_slices(incrbyfloat_again, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"$1\r\n2\r\n");
}

#[test]
fn getex_updates_expiration_and_persist() {
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
    let getex_px = b"*4\r\n$5\r\nGETEX\r\n$3\r\nkey\r\n$2\r\nPX\r\n$3\r\n250\r\n";
    let meta = parse_resp_command_arg_slices(getex_px, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"$5\r\nvalue\r\n");

    response.clear();
    let pttl = b"*2\r\n$4\r\nPTTL\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(pttl, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    let ttl_millis = parse_integer_response(&response);
    assert!((0..=250).contains(&ttl_millis));

    response.clear();
    let getex_persist = b"*3\r\n$5\r\nGETEX\r\n$3\r\nkey\r\n$7\r\nPERSIST\r\n";
    let meta = parse_resp_command_arg_slices(getex_persist, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"$5\r\nvalue\r\n");

    response.clear();
    let ttl = b"*2\r\n$3\r\nTTL\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(ttl, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":-1\r\n");
}

#[test]
fn getex_exat_in_the_past_returns_value_and_deletes_key() {
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
    let getex_exat_past = b"*4\r\n$5\r\nGETEX\r\n$3\r\nkey\r\n$4\r\nEXAT\r\n$1\r\n1\r\n";
    let meta = parse_resp_command_arg_slices(getex_exat_past, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"$5\r\nvalue\r\n");

    response.clear();
    let get = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(get, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"$-1\r\n");
}

#[test]
fn msetnx_sets_only_when_all_keys_are_absent() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 16];
    let mut response = Vec::new();

    let msetnx_first = b"*5\r\n$6\r\nMSETNX\r\n$2\r\nk1\r\n$2\r\nv1\r\n$2\r\nk2\r\n$2\r\nv2\r\n";
    let meta = parse_resp_command_arg_slices(msetnx_first, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let msetnx_second = b"*5\r\n$6\r\nMSETNX\r\n$2\r\nk2\r\n$1\r\nx\r\n$2\r\nk3\r\n$1\r\ny\r\n";
    let meta = parse_resp_command_arg_slices(msetnx_second, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let mget = b"*4\r\n$4\r\nMGET\r\n$2\r\nk1\r\n$2\r\nk2\r\n$2\r\nk3\r\n";
    let meta = parse_resp_command_arg_slices(mget, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"*3\r\n$2\r\nv1\r\n$2\r\nv2\r\n$-1\r\n");
}

#[test]
fn touch_and_unlink_count_existing_keys() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 16];
    let mut response = Vec::new();

    let set_a = b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n";
    let meta = parse_resp_command_arg_slices(set_a, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let set_b = b"*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\n2\r\n";
    let meta = parse_resp_command_arg_slices(set_b, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let touch = b"*4\r\n$5\r\nTOUCH\r\n$1\r\na\r\n$1\r\nb\r\n$7\r\nmissing\r\n";
    let meta = parse_resp_command_arg_slices(touch, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":2\r\n");

    response.clear();
    let unlink = b"*4\r\n$6\r\nUNLINK\r\n$1\r\na\r\n$1\r\nb\r\n$7\r\nmissing\r\n";
    let meta = parse_resp_command_arg_slices(unlink, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":2\r\n");

    response.clear();
    let get_a = b"*2\r\n$3\r\nGET\r\n$1\r\na\r\n";
    let meta = parse_resp_command_arg_slices(get_a, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"$-1\r\n");
}

#[test]
fn execute_with_client_no_touch_is_scoped_per_request() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 8];
    let mut response = Vec::new();

    let set = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
    let meta = parse_resp_command_arg_slices(set, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    let lru_before = processor.key_lru_millis(b"key").unwrap();

    thread::sleep(Duration::from_millis(5));
    response.clear();
    let get = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(get, &mut args).unwrap();
    processor
        .execute_with_client_no_touch(&args[..meta.argument_count], &mut response, true)
        .unwrap();
    assert_eq!(response, b"$5\r\nvalue\r\n");
    let lru_after_no_touch = processor.key_lru_millis(b"key").unwrap();
    assert_eq!(lru_after_no_touch, lru_before);

    thread::sleep(Duration::from_millis(5));
    response.clear();
    let meta = parse_resp_command_arg_slices(get, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"$5\r\nvalue\r\n");
    let lru_after_touch = processor.key_lru_millis(b"key").unwrap();
    assert!(lru_after_touch > lru_after_no_touch);
}

#[test]
fn command_getkeys_getkeysandflags_list_and_info_cover_introspection_paths() {
    let processor = RequestProcessor::new().unwrap();

    let count_response = execute_command_line(&processor, "COMMAND COUNT").unwrap();
    assert!(parse_integer_response(&count_response) > 0);

    assert_eq!(
        parse_bulk_array_payloads(
            &execute_command_line(&processor, "COMMAND GETKEYS GET key").unwrap()
        ),
        vec![b"key".to_vec()]
    );
    assert_eq!(
        parse_bulk_array_payloads(
            &execute_command_line(&processor, "COMMAND GETKEYS MEMORY USAGE key").unwrap()
        ),
        vec![b"key".to_vec()]
    );
    assert_eq!(
        parse_bulk_array_payloads(
            &execute_command_line(&processor, "COMMAND GETKEYS XGROUP CREATE key groupname $")
                .unwrap()
        ),
        vec![b"key".to_vec()]
    );
    assert_eq!(
        parse_bulk_array_payloads(
            &execute_command_line(&processor, "COMMAND GETKEYS EVAL \"return 1\" 1 key").unwrap()
        ),
        vec![b"key".to_vec()]
    );
    assert_eq!(
        execute_command_line(&processor, "COMMAND GETKEYS EVAL \"return 1\" 0").unwrap(),
        b"*0\r\n"
    );
    assert_eq!(
        parse_bulk_array_payloads(
            &execute_command_line(&processor, "COMMAND GETKEYS LCS key1 key2").unwrap()
        ),
        vec![b"key1".to_vec(), b"key2".to_vec()]
    );

    let numkeys = 260usize;
    let mut getkeys_parts = vec![
        b"COMMAND".to_vec(),
        b"GETKEYS".to_vec(),
        b"ZUNIONSTORE".to_vec(),
        b"target".to_vec(),
        numkeys.to_string().into_bytes(),
    ];
    let mut expected_keys = vec![b"target".to_vec()];
    for i in 1..=numkeys {
        let key = format!("key{i}").into_bytes();
        expected_keys.push(key.clone());
        getkeys_parts.push(key);
    }
    let getkeys_part_refs: Vec<&[u8]> = getkeys_parts.iter().map(Vec::as_slice).collect();
    let getkeys_frame = encode_resp(&getkeys_part_refs);
    let mut large_args = [ArgSlice::EMPTY; 320];
    let getkeys_meta = parse_resp_command_arg_slices(&getkeys_frame, &mut large_args).unwrap();
    let mut getkeys_response = Vec::new();
    processor
        .execute(
            &large_args[..getkeys_meta.argument_count],
            &mut getkeys_response,
        )
        .unwrap();
    assert_eq!(parse_bulk_array_payloads(&getkeys_response), expected_keys);

    assert_eq!(
        parse_command_getkeysandflags_response(
            &execute_command_line(&processor, "COMMAND GETKEYSANDFLAGS SET k1 v1").unwrap()
        ),
        vec![(b"k1".to_vec(), vec![b"OW".to_vec(), b"update".to_vec()])]
    );
    assert_eq!(
        parse_command_getkeysandflags_response(
            &execute_command_line(&processor, "COMMAND GETKEYSANDFLAGS MSET k1 v1 k2 v2").unwrap()
        ),
        vec![
            (b"k1".to_vec(), vec![b"OW".to_vec(), b"update".to_vec()]),
            (b"k2".to_vec(), vec![b"OW".to_vec(), b"update".to_vec()]),
        ]
    );
    assert_eq!(
        parse_command_getkeysandflags_response(
            &execute_command_line(&processor, "COMMAND GETKEYSANDFLAGS LMOVE k1 k2 left right",)
                .unwrap()
        ),
        vec![
            (
                b"k1".to_vec(),
                vec![b"RW".to_vec(), b"access".to_vec(), b"delete".to_vec()],
            ),
            (b"k2".to_vec(), vec![b"RW".to_vec(), b"insert".to_vec()]),
        ]
    );
    assert_eq!(
        parse_command_getkeysandflags_response(
            &execute_command_line(&processor, "COMMAND GETKEYSANDFLAGS SORT k1 STORE k2").unwrap()
        ),
        vec![
            (b"k1".to_vec(), vec![b"RO".to_vec(), b"access".to_vec()]),
            (b"k2".to_vec(), vec![b"OW".to_vec(), b"update".to_vec()]),
        ]
    );
    assert_eq!(
        parse_command_getkeysandflags_response(
            &execute_command_line(&processor, "COMMAND GETKEYSANDFLAGS SET k1 v1 IFEQ v1").unwrap()
        ),
        vec![(b"k1".to_vec(), vec![b"RW".to_vec(), b"update".to_vec()])]
    );
    assert_eq!(
        parse_command_getkeysandflags_response(
            &execute_command_line(&processor, "COMMAND GETKEYSANDFLAGS SET k1 v1 GET").unwrap()
        ),
        vec![(
            b"k1".to_vec(),
            vec![b"RW".to_vec(), b"access".to_vec(), b"update".to_vec()]
        )]
    );
    assert_eq!(
        parse_command_getkeysandflags_response(
            &execute_command_line(&processor, "COMMAND GETKEYSANDFLAGS DELEX k1").unwrap()
        ),
        vec![(b"k1".to_vec(), vec![b"RM".to_vec(), b"delete".to_vec()])]
    );
    assert_eq!(
        parse_command_getkeysandflags_response(
            &execute_command_line(&processor, "COMMAND GETKEYSANDFLAGS DELEX k1 IFEQ v1").unwrap()
        ),
        vec![(b"k1".to_vec(), vec![b"RW".to_vec(), b"delete".to_vec()])]
    );
    assert_eq!(
        parse_command_getkeysandflags_response(
            &execute_command_line(
                &processor,
                "COMMAND GETKEYSANDFLAGS MSETEX 2 k1 v1 k2 v2 EX 10 NX"
            )
            .unwrap()
        ),
        vec![
            (b"k1".to_vec(), vec![b"OW".to_vec(), b"update".to_vec()]),
            (b"k2".to_vec(), vec![b"OW".to_vec(), b"update".to_vec()]),
        ]
    );
    assert_command_error(
        &processor,
        "COMMAND GETKEYSANDFLAGS ZINTERSTORE zz 1443677133621497600 asdf",
        b"-ERR Invalid arguments specified for command\r\n",
    );

    assert_command_error(&processor, "COMMAND LIST bad_arg", b"-ERR syntax error\r\n");
    assert_command_error(
        &processor,
        "COMMAND LIST FILTERBY bad_arg",
        b"-ERR syntax error\r\n",
    );
    assert_command_error(
        &processor,
        "COMMAND LIST FILTERBY bad_arg bad_arg2",
        b"-ERR syntax error\r\n",
    );

    let command_list =
        parse_bulk_array_payloads(&execute_command_line(&processor, "COMMAND LIST").unwrap());
    assert!(command_list.contains(&b"set".to_vec()));
    assert!(command_list.contains(&b"client|list".to_vec()));

    assert_eq!(
        parse_bulk_array_payloads(
            &execute_command_line(
                &processor,
                "COMMAND LIST FILTERBY ACLCAT non_existing_category"
            )
            .unwrap()
        ),
        Vec::<Vec<u8>>::new()
    );

    let scripting_list = parse_bulk_array_payloads(
        &execute_command_line(&processor, "COMMAND LIST FILTERBY ACLCAT scripting").unwrap(),
    );
    assert!(scripting_list.contains(&b"eval".to_vec()));
    assert!(scripting_list.contains(&b"script|kill".to_vec()));
    assert!(!scripting_list.contains(&b"set".to_vec()));

    assert_eq!(
        parse_bulk_array_payloads(
            &execute_command_line(&processor, "COMMAND LIST FILTERBY PATTERN set").unwrap()
        ),
        vec![b"set".to_vec()]
    );
    assert_eq!(
        parse_bulk_array_payloads(
            &execute_command_line(&processor, "COMMAND LIST FILTERBY PATTERN get").unwrap()
        ),
        vec![b"get".to_vec()]
    );

    let config_pattern = parse_bulk_array_payloads(
        &execute_command_line(&processor, "COMMAND LIST FILTERBY PATTERN config*").unwrap(),
    );
    assert!(config_pattern.contains(&b"config".to_vec()));
    assert!(config_pattern.contains(&b"config|get".to_vec()));

    let config_sub_pattern = parse_bulk_array_payloads(
        &execute_command_line(&processor, "COMMAND LIST FILTERBY PATTERN config|*re*").unwrap(),
    );
    assert!(config_sub_pattern.contains(&b"config|resetstat".to_vec()));
    assert!(config_sub_pattern.contains(&b"config|rewrite".to_vec()));

    let help_pattern = parse_bulk_array_payloads(
        &execute_command_line(&processor, "COMMAND LIST FILTERBY PATTERN cl*help").unwrap(),
    );
    assert!(help_pattern.contains(&b"client|help".to_vec()));
    assert!(help_pattern.contains(&b"cluster|help".to_vec()));

    assert_eq!(
        parse_bulk_array_payloads(
            &execute_command_line(&processor, "COMMAND LIST FILTERBY PATTERN non_exists").unwrap()
        ),
        Vec::<Vec<u8>>::new()
    );
    assert_eq!(
        parse_bulk_array_payloads(
            &execute_command_line(&processor, "COMMAND LIST FILTERBY PATTERN non_exists*").unwrap()
        ),
        Vec::<Vec<u8>>::new()
    );
    assert_eq!(
        parse_bulk_array_payloads(
            &execute_command_line(
                &processor,
                "COMMAND LIST FILTERBY MODULE non_existing_module"
            )
            .unwrap()
        ),
        Vec::<Vec<u8>>::new()
    );

    assert_command_response(&processor, "COMMAND INFO get|key", b"*1\r\n*0\r\n");
    assert_command_response(&processor, "COMMAND INFO config|get|key", b"*1\r\n*0\r\n");

    let set_info = execute_command_line(&processor, "COMMAND INFO SET").unwrap();
    assert!(!String::from_utf8_lossy(&set_info).contains("movablekeys"));
    let memory_info = execute_command_line(&processor, "COMMAND INFO MEMORY|USAGE").unwrap();
    assert!(!String::from_utf8_lossy(&memory_info).contains("movablekeys"));
    let georadius_ro_info = execute_command_line(&processor, "COMMAND INFO GEORADIUS_RO").unwrap();
    assert!(!String::from_utf8_lossy(&georadius_ro_info).contains("movablekeys"));

    let zunionstore_info = execute_command_line(&processor, "COMMAND INFO ZUNIONSTORE").unwrap();
    assert!(String::from_utf8_lossy(&zunionstore_info).contains("movablekeys"));
    let eval_info = execute_command_line(&processor, "COMMAND INFO EVAL").unwrap();
    assert!(String::from_utf8_lossy(&eval_info).contains("movablekeys"));
    let sort_info = execute_command_line(&processor, "COMMAND INFO SORT").unwrap();
    assert!(String::from_utf8_lossy(&sort_info).contains("movablekeys"));
    let migrate_info = execute_command_line(&processor, "COMMAND INFO MIGRATE").unwrap();
    assert!(String::from_utf8_lossy(&migrate_info).contains("movablekeys"));
    let georadius_info = execute_command_line(&processor, "COMMAND INFO GEORADIUS").unwrap();
    assert!(String::from_utf8_lossy(&georadius_info).contains("movablekeys"));
}

#[test]
fn pfadd_pfcount_pfmerge_pfdebug_and_pfselftest_cover_basic_paths() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 16];
    let mut response = Vec::new();

    let pfadd_empty_create = b"*2\r\n$5\r\nPFADD\r\n$8\r\nemptyhll\r\n";
    let meta = parse_resp_command_arg_slices(pfadd_empty_create, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let pfadd_empty_noop = b"*2\r\n$5\r\nPFADD\r\n$8\r\nemptyhll\r\n";
    let meta = parse_resp_command_arg_slices(pfadd_empty_noop, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let pfcount_empty = b"*2\r\n$7\r\nPFCOUNT\r\n$8\r\nemptyhll\r\n";
    let meta = parse_resp_command_arg_slices(pfcount_empty, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let pfadd_first = b"*5\r\n$5\r\nPFADD\r\n$2\r\nh1\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n";
    let meta = parse_resp_command_arg_slices(pfadd_first, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let pfadd_second = b"*4\r\n$5\r\nPFADD\r\n$2\r\nh1\r\n$1\r\nb\r\n$1\r\nc\r\n";
    let meta = parse_resp_command_arg_slices(pfadd_second, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let pfcount_single = b"*2\r\n$7\r\nPFCOUNT\r\n$2\r\nh1\r\n";
    let meta = parse_resp_command_arg_slices(pfcount_single, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":3\r\n");

    response.clear();
    let pfadd_other = b"*4\r\n$5\r\nPFADD\r\n$2\r\nh2\r\n$1\r\nc\r\n$1\r\nd\r\n";
    let meta = parse_resp_command_arg_slices(pfadd_other, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let pfmerge = b"*4\r\n$7\r\nPFMERGE\r\n$2\r\nhm\r\n$2\r\nh1\r\n$2\r\nh2\r\n";
    let meta = parse_resp_command_arg_slices(pfmerge, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let pfcount_merged = b"*2\r\n$7\r\nPFCOUNT\r\n$2\r\nhm\r\n";
    let meta = parse_resp_command_arg_slices(pfcount_merged, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":4\r\n");

    response.clear();
    let pfmerge_empty_dest = b"*2\r\n$7\r\nPFMERGE\r\n$6\r\ndest11\r\n";
    let meta = parse_resp_command_arg_slices(pfmerge_empty_dest, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let pfcount_empty_dest = b"*2\r\n$7\r\nPFCOUNT\r\n$6\r\ndest11\r\n";
    let meta = parse_resp_command_arg_slices(pfcount_empty_dest, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let pfmerge_dest_source_seed =
        b"*5\r\n$5\r\nPFADD\r\n$6\r\ndest22\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n";
    let meta = parse_resp_command_arg_slices(pfmerge_dest_source_seed, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let pfmerge_dest_source_only = b"*2\r\n$7\r\nPFMERGE\r\n$6\r\ndest22\r\n";
    let meta = parse_resp_command_arg_slices(pfmerge_dest_source_only, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let pfcount_dest_source_only = b"*2\r\n$7\r\nPFCOUNT\r\n$6\r\ndest22\r\n";
    let meta = parse_resp_command_arg_slices(pfcount_dest_source_only, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":3\r\n");

    response.clear();
    let pfdebug_encoding = b"*3\r\n$7\r\nPFDEBUG\r\n$8\r\nENCODING\r\n$2\r\nhm\r\n";
    let meta = parse_resp_command_arg_slices(pfdebug_encoding, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"$6\r\nsparse\r\n");

    response.clear();
    let pfdebug_getreg = b"*3\r\n$7\r\nPFDEBUG\r\n$6\r\nGETREG\r\n$2\r\nhm\r\n";
    let meta = parse_resp_command_arg_slices(pfdebug_getreg, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert!(response.starts_with(b"*16384\r\n"));

    response.clear();
    let pfdebug_simd_off = b"*3\r\n$7\r\nPFDEBUG\r\n$4\r\nSIMD\r\n$3\r\nOFF\r\n";
    let meta = parse_resp_command_arg_slices(pfdebug_simd_off, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let pfdebug_simd_on = b"*3\r\n$7\r\nPFDEBUG\r\n$4\r\nSIMD\r\n$2\r\nON\r\n";
    let meta = parse_resp_command_arg_slices(pfdebug_simd_on, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let append_corrupt_hll = b"*3\r\n$6\r\nAPPEND\r\n$2\r\nhm\r\n$5\r\nhello\r\n";
    let meta = parse_resp_command_arg_slices(append_corrupt_hll, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert!(response.starts_with(b":"));

    response.clear();
    let pfcount_invalidobj = b"*2\r\n$7\r\nPFCOUNT\r\n$2\r\nhm\r\n";
    let meta = parse_resp_command_arg_slices(pfcount_invalidobj, &mut args).unwrap();
    let err = processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap_err();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-INVALIDOBJ Corrupted HLL object detected\r\n");

    response.clear();
    let set_hyll_like = b"*3\r\n$3\r\nSET\r\n$4\r\nhyll\r\n$4\r\nHYLL\r\n";
    let meta = parse_resp_command_arg_slices(set_hyll_like, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let pfdebug_getreg_invalidobj = b"*3\r\n$7\r\nPFDEBUG\r\n$6\r\nGETREG\r\n$4\r\nhyll\r\n";
    let meta = parse_resp_command_arg_slices(pfdebug_getreg_invalidobj, &mut args).unwrap();
    let err = processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap_err();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-INVALIDOBJ Corrupted HLL object detected\r\n");

    response.clear();
    let pfselftest = b"*1\r\n$10\r\nPFSELFTEST\r\n";
    let meta = parse_resp_command_arg_slices(pfselftest, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let set_plain = b"*3\r\n$3\r\nSET\r\n$5\r\nplain\r\n$5\r\nvalue\r\n";
    let meta = parse_resp_command_arg_slices(set_plain, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let pfcount_wrongtype = b"*2\r\n$7\r\nPFCOUNT\r\n$5\r\nplain\r\n";
    let meta = parse_resp_command_arg_slices(pfcount_wrongtype, &mut args).unwrap();
    let err = processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap_err();
    err.append_resp_error(&mut response);
    assert_eq!(
        response,
        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
    );
}

#[test]
fn pfdebug_encoding_and_strlen_follow_sparse_dense_transitions() {
    let processor = RequestProcessor::new().unwrap();

    assert_command_response(&processor, "CONFIG SET hll-sparse-max-bytes 30", b"+OK\r\n");
    assert_command_response(
        &processor,
        "PFADD dense_key a b c d e f g h i j k",
        b":1\r\n",
    );
    assert_command_response(&processor, "PFDEBUG ENCODING dense_key", b"$5\r\ndense\r\n");
    assert_command_response(&processor, "STRLEN dense_key", b":31\r\n");

    assert_command_response(&processor, "PFADD key2 a b c", b":1\r\n");
    assert_command_response(&processor, "PFDEBUG ENCODING key2", b"$6\r\nsparse\r\n");
    assert_command_response(&processor, "STRLEN key2", b":25\r\n");
    assert_command_response(&processor, "PFDEBUG TODENSE key2", b"+OK\r\n");
    assert_command_response(&processor, "PFDEBUG ENCODING key2", b"$5\r\ndense\r\n");
    assert_command_response(&processor, "STRLEN key2", b":31\r\n");
}

#[test]
fn oversized_hyll_set_is_canonicalized_to_invalid_hll_marker() {
    let processor = RequestProcessor::new().unwrap();

    let mut payload = String::from("HYLL");
    payload.push_str(&"x".repeat(270_000));
    let command = format!("SET hll {}", payload);
    let response = execute_command_line(&processor, &command).unwrap();
    assert_eq!(response, b"+OK\r\n");

    assert_command_response(&processor, "GET hll", b"$4\r\nHYLL\r\n");
    assert_command_error(
        &processor,
        "PFCOUNT hll",
        b"-INVALIDOBJ Corrupted HLL object detected\r\n",
    );
    assert_command_error(
        &processor,
        "PFDEBUG GETREG hll",
        b"-INVALIDOBJ Corrupted HLL object detected\r\n",
    );
}

#[test]
fn quit_and_time_commands_return_expected_responses() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 8];
    let mut response = Vec::new();

    let quit = b"*1\r\n$4\r\nQUIT\r\n";
    let meta = parse_resp_command_arg_slices(quit, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let time = b"*1\r\n$4\r\nTIME\r\n";
    let meta = parse_resp_command_arg_slices(time, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    let tokens: Vec<&str> = std::str::from_utf8(&response)
        .unwrap()
        .split("\r\n")
        .filter(|token| !token.is_empty())
        .collect();
    assert_eq!(tokens[0], "*2");
    assert!(tokens[1].starts_with('$'));
    assert!(tokens[2].bytes().all(|byte| byte.is_ascii_digit()));
    assert!(tokens[3].starts_with('$'));
    assert!(tokens[4].bytes().all(|byte| byte.is_ascii_digit()));
}

#[test]
fn server_mode_and_reset_commands_follow_expected_responses() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 8];
    let mut response = Vec::new();

    let lastsave = b"*1\r\n$8\r\nLASTSAVE\r\n";
    let meta = parse_resp_command_arg_slices(lastsave, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    let first_lastsave = parse_integer_response(&response);
    assert!(first_lastsave > 0);

    response.clear();
    let meta = parse_resp_command_arg_slices(lastsave, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(parse_integer_response(&response), first_lastsave);

    response.clear();
    let readonly = b"*1\r\n$8\r\nREADONLY\r\n";
    let meta = parse_resp_command_arg_slices(readonly, &mut args).unwrap();
    let err = processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap_err();
    err.append_resp_error(&mut response);
    assert_eq!(
        response,
        b"-ERR This instance has cluster support disabled\r\n"
    );

    response.clear();
    let readwrite = b"*1\r\n$9\r\nREADWRITE\r\n";
    let meta = parse_resp_command_arg_slices(readwrite, &mut args).unwrap();
    let err = processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap_err();
    err.append_resp_error(&mut response);
    assert_eq!(
        response,
        b"-ERR This instance has cluster support disabled\r\n"
    );

    response.clear();
    let hello3 = b"*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n";
    let meta = parse_resp_command_arg_slices(hello3, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");
    assert_eq!(processor.resp_protocol_version(), 3);

    response.clear();
    let reset = b"*1\r\n$5\r\nRESET\r\n";
    let meta = parse_resp_command_arg_slices(reset, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+RESET\r\n");
    assert_eq!(processor.resp_protocol_version(), 2);

    response.clear();
    let lolwut = b"*1\r\n$6\r\nLOLWUT\r\n";
    let meta = parse_resp_command_arg_slices(lolwut, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert!(response.starts_with(b"$"));
    assert!(response.windows(21).any(|w| w == b"Redis ver. garnet-rs\n"));

    response.clear();
    let lolwut_bad_version = b"*3\r\n$6\r\nLOLWUT\r\n$7\r\nVERSION\r\n$3\r\nbad\r\n";
    let meta = parse_resp_command_arg_slices(lolwut_bad_version, &mut args).unwrap();
    let err = processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap_err();
    err.append_resp_error(&mut response);
    assert_eq!(
        response,
        b"-ERR value is not an integer or out of range\r\n"
    );
}

#[test]
fn server_admin_commands_cover_auth_select_move_swapdb_client_role_wait_and_save_family() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 8];
    let mut response = Vec::new();

    let auth = b"*2\r\n$4\r\nAUTH\r\n$3\r\npwd\r\n";
    let meta = parse_resp_command_arg_slices(auth, &mut args).unwrap();
    let err = processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap_err();
    err.append_resp_error(&mut response);
    assert_eq!(
        response,
        b"-ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?\r\n"
    );

    response.clear();
    let select_zero = b"*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n";
    let meta = parse_resp_command_arg_slices(select_zero, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let select_one = b"*2\r\n$6\r\nSELECT\r\n$1\r\n1\r\n";
    let meta = parse_resp_command_arg_slices(select_one, &mut args).unwrap();
    let err = processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap_err();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR DB index is out of range\r\n");

    response.clear();
    let move_same_db = b"*3\r\n$4\r\nMOVE\r\n$3\r\nkey\r\n$1\r\n0\r\n";
    let meta = parse_resp_command_arg_slices(move_same_db, &mut args).unwrap();
    let err = processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap_err();
    err.append_resp_error(&mut response);
    assert_eq!(
        response,
        b"-ERR source and destination objects are the same\r\n"
    );

    response.clear();
    let move_other_db = b"*3\r\n$4\r\nMOVE\r\n$3\r\nkey\r\n$1\r\n1\r\n";
    let meta = parse_resp_command_arg_slices(move_other_db, &mut args).unwrap();
    let err = processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap_err();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR DB index is out of range\r\n");

    response.clear();
    let swapdb_zero = b"*3\r\n$6\r\nSWAPDB\r\n$1\r\n0\r\n$1\r\n0\r\n";
    let meta = parse_resp_command_arg_slices(swapdb_zero, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let swapdb_other = b"*3\r\n$6\r\nSWAPDB\r\n$1\r\n0\r\n$1\r\n1\r\n";
    let meta = parse_resp_command_arg_slices(swapdb_other, &mut args).unwrap();
    let err = processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap_err();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR DB index is out of range\r\n");

    response.clear();
    let client_id = b"*2\r\n$6\r\nCLIENT\r\n$2\r\nID\r\n";
    let meta = parse_resp_command_arg_slices(client_id, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let client_getname = b"*2\r\n$6\r\nCLIENT\r\n$7\r\nGETNAME\r\n";
    let meta = parse_resp_command_arg_slices(client_getname, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"$-1\r\n");

    response.clear();
    let client_setname = b"*3\r\n$6\r\nCLIENT\r\n$7\r\nSETNAME\r\n$3\r\napp\r\n";
    let meta = parse_resp_command_arg_slices(client_setname, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let client_list = b"*2\r\n$6\r\nCLIENT\r\n$4\r\nLIST\r\n";
    let meta = parse_resp_command_arg_slices(client_list, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"$13\r\nid=1 cmd=exec\r\n");

    response.clear();
    let client_unblock = b"*3\r\n$6\r\nCLIENT\r\n$7\r\nUNBLOCK\r\n$1\r\n1\r\n";
    let meta = parse_resp_command_arg_slices(client_unblock, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let client_pause = b"*4\r\n$6\r\nCLIENT\r\n$5\r\nPAUSE\r\n$3\r\n100\r\n$5\r\nWRITE\r\n";
    let meta = parse_resp_command_arg_slices(client_pause, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let client_unpause = b"*2\r\n$6\r\nCLIENT\r\n$7\r\nUNPAUSE\r\n";
    let meta = parse_resp_command_arg_slices(client_unpause, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let client_no_touch = b"*3\r\n$6\r\nCLIENT\r\n$8\r\nNO-TOUCH\r\n$2\r\nON\r\n";
    let meta = parse_resp_command_arg_slices(client_no_touch, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let role = b"*1\r\n$4\r\nROLE\r\n";
    let meta = parse_resp_command_arg_slices(role, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"*3\r\n$6\r\nmaster\r\n:0\r\n*0\r\n");

    response.clear();
    let wait = b"*3\r\n$4\r\nWAIT\r\n$1\r\n1\r\n$2\r\n10\r\n";
    let meta = parse_resp_command_arg_slices(wait, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let waitaof = b"*4\r\n$7\r\nWAITAOF\r\n$1\r\n0\r\n$1\r\n1\r\n$2\r\n10\r\n";
    let meta = parse_resp_command_arg_slices(waitaof, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let waitaof_err = b"*4\r\n$7\r\nWAITAOF\r\n$1\r\n1\r\n$1\r\n1\r\n$2\r\n10\r\n";
    let meta = parse_resp_command_arg_slices(waitaof_err, &mut args).unwrap();
    let err = processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap_err();
    err.append_resp_error(&mut response);
    assert_eq!(
        response,
        b"-ERR WAITAOF cannot be used when numlocal is set but appendonly is disabled.\r\n"
    );

    response.clear();
    let save = b"*1\r\n$4\r\nSAVE\r\n";
    let meta = parse_resp_command_arg_slices(save, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let bgsave = b"*1\r\n$6\r\nBGSAVE\r\n";
    let meta = parse_resp_command_arg_slices(bgsave, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+Background saving started\r\n");

    response.clear();
    let bgrewriteaof = b"*1\r\n$12\r\nBGREWRITEAOF\r\n";
    let meta = parse_resp_command_arg_slices(bgrewriteaof, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(
        response,
        b"+Background append only file rewriting started\r\n"
    );
}

#[test]
fn latency_module_and_slowlog_commands_cover_supported_subcommands() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 8];
    let mut response = Vec::new();

    let module_list = b"*2\r\n$6\r\nMODULE\r\n$4\r\nLIST\r\n";
    let meta = parse_resp_command_arg_slices(module_list, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"*0\r\n");

    response.clear();
    let module_help = b"*2\r\n$6\r\nMODULE\r\n$4\r\nHELP\r\n";
    let meta = parse_resp_command_arg_slices(module_help, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert!(response.starts_with(b"*11\r\n"));
    let module_help_text = std::str::from_utf8(&response).unwrap();
    assert!(module_help_text.contains("MODULE <subcommand>"));

    response.clear();
    let latency_help = b"*2\r\n$7\r\nLATENCY\r\n$4\r\nHELP\r\n";
    let meta = parse_resp_command_arg_slices(latency_help, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert!(response.starts_with(b"*15\r\n"));
    let latency_help_text = std::str::from_utf8(&response).unwrap();
    assert!(latency_help_text.contains("\r\nLATEST\r\n"));

    response.clear();
    let latency_latest = b"*2\r\n$7\r\nLATENCY\r\n$6\r\nLATEST\r\n";
    let meta = parse_resp_command_arg_slices(latency_latest, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"*0\r\n");

    response.clear();
    let latency_history = b"*3\r\n$7\r\nLATENCY\r\n$7\r\nHISTORY\r\n$7\r\ncommand\r\n";
    let meta = parse_resp_command_arg_slices(latency_history, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"*0\r\n");

    response.clear();
    let latency_reset = b"*2\r\n$7\r\nLATENCY\r\n$5\r\nRESET\r\n";
    let meta = parse_resp_command_arg_slices(latency_reset, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let latency_doctor = b"*2\r\n$7\r\nLATENCY\r\n$6\r\nDOCTOR\r\n";
    let meta = parse_resp_command_arg_slices(latency_doctor, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert!(response.starts_with(b"$"));
    let latency_doctor_text = std::str::from_utf8(&response).unwrap();
    assert!(latency_doctor_text.contains("Latency monitoring is disabled"));

    response.clear();
    let slowlog_len = b"*2\r\n$7\r\nSLOWLOG\r\n$3\r\nLEN\r\n";
    let meta = parse_resp_command_arg_slices(slowlog_len, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let slowlog_get_default = b"*2\r\n$7\r\nSLOWLOG\r\n$3\r\nGET\r\n";
    let meta = parse_resp_command_arg_slices(slowlog_get_default, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"*0\r\n");

    response.clear();
    let slowlog_get_count = b"*3\r\n$7\r\nSLOWLOG\r\n$3\r\nGET\r\n$1\r\n2\r\n";
    let meta = parse_resp_command_arg_slices(slowlog_get_count, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"*0\r\n");

    response.clear();
    let slowlog_reset = b"*2\r\n$7\r\nSLOWLOG\r\n$5\r\nRESET\r\n";
    let meta = parse_resp_command_arg_slices(slowlog_reset, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let slowlog_help = b"*2\r\n$7\r\nSLOWLOG\r\n$4\r\nHELP\r\n";
    let meta = parse_resp_command_arg_slices(slowlog_help, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert!(response.starts_with(b"*12\r\n"));

    response.clear();
    let module_unknown = b"*2\r\n$6\r\nMODULE\r\n$4\r\nNOPE\r\n";
    let meta = parse_resp_command_arg_slices(module_unknown, &mut args).unwrap();
    let err = processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap_err();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR unknown command\r\n");

    response.clear();
    let latency_unknown = b"*2\r\n$7\r\nLATENCY\r\n$4\r\nNOPE\r\n";
    let meta = parse_resp_command_arg_slices(latency_unknown, &mut args).unwrap();
    let err = processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap_err();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR unknown command\r\n");

    response.clear();
    let slowlog_bad_count = b"*3\r\n$7\r\nSLOWLOG\r\n$3\r\nGET\r\n$3\r\nbad\r\n";
    let meta = parse_resp_command_arg_slices(slowlog_bad_count, &mut args).unwrap();
    let err = processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap_err();
    err.append_resp_error(&mut response);
    assert_eq!(
        response,
        b"-ERR value is not an integer or out of range\r\n"
    );
}

#[test]
fn latency_histogram_and_event_queries_cover_compatibility_shapes() {
    let processor = RequestProcessor::new().unwrap();
    processor.record_command_call(b"config|resetstat");
    processor.record_command_call(b"set");
    processor.record_command_call(b"set");
    processor.record_command_call(b"client|id");
    processor.record_command_call(b"client|list");
    processor.record_command_call(b"latency|histogram");

    let histogram_all = execute_frame(&processor, &encode_resp(&[b"LATENCY", b"HISTOGRAM"]));
    let histogram_all_text = String::from_utf8_lossy(&histogram_all);
    assert!(histogram_all_text.contains("config|resetstat"));
    assert!(histogram_all_text.contains("calls 2 histogram_usec"));
    assert!(!histogram_all_text.contains("latency|histogram"));

    let histogram_client = execute_frame(
        &processor,
        &encode_resp(&[b"LATENCY", b"HISTOGRAM", b"client"]),
    );
    let histogram_client_text = String::from_utf8_lossy(&histogram_client);
    assert!(histogram_client_text.contains("client|id"));
    assert!(histogram_client_text.contains("client|list"));
    assert!(!histogram_client_text.contains("$3\r\nset\r\n"));

    let histogram_filtered = execute_frame(
        &processor,
        &encode_resp(&[b"LATENCY", b"HISTOGRAM", b"blabla", b"set"]),
    );
    let histogram_filtered_text = String::from_utf8_lossy(&histogram_filtered);
    assert!(histogram_filtered_text.contains("$3\r\nset\r\n"));
    assert!(!histogram_filtered_text.contains("blabla"));

    processor.record_latency_event(b"command", 300);
    processor.record_latency_event(b"command", 400);
    processor.record_latency_event(b"command", 500);

    let history = execute_frame(
        &processor,
        &encode_resp(&[b"LATENCY", b"HISTORY", b"command"]),
    );
    let history_text = String::from_utf8_lossy(&history);
    assert!(history_text.contains(":300\r\n"));
    assert!(history_text.contains(":400\r\n"));
    assert!(history_text.contains(":500\r\n"));

    let latest = execute_frame(&processor, &encode_resp(&[b"LATENCY", b"LATEST"]));
    let latest_text = String::from_utf8_lossy(&latest);
    assert!(latest_text.contains("$7\r\ncommand\r\n"));
    assert!(latest_text.contains(":500\r\n"));

    let graph = parse_bulk_payload(&execute_frame(
        &processor,
        &encode_resp(&[b"LATENCY", b"GRAPH", b"command"]),
    ))
    .expect("LATENCY GRAPH should return a bulk payload");
    let graph_text = String::from_utf8_lossy(&graph);
    assert!(graph_text.contains("command - high 500 ms, low 300 ms"));

    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"LATENCY", b"RESET", b"command"])
        ),
        b":1\r\n"
    );
    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"LATENCY", b"LATEST"])),
        b"*0\r\n"
    );

    assert_command_error(
        &processor,
        "LATENCY HELP extra",
        b"-ERR wrong number of arguments for 'latency|help' command\r\n",
    );
}

#[test]
fn acl_cluster_failover_monitor_and_shutdown_commands_cover_basic_shapes() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 16];
    let mut response = Vec::new();

    let acl_whoami = b"*2\r\n$3\r\nACL\r\n$6\r\nWHOAMI\r\n";
    let meta = parse_resp_command_arg_slices(acl_whoami, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"$7\r\ndefault\r\n");

    response.clear();
    let acl_users = b"*2\r\n$3\r\nACL\r\n$5\r\nUSERS\r\n";
    let meta = parse_resp_command_arg_slices(acl_users, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"*1\r\n$7\r\ndefault\r\n");

    response.clear();
    let acl_setuser = b"*4\r\n$3\r\nACL\r\n$7\r\nSETUSER\r\n$7\r\ndefault\r\n$2\r\non\r\n";
    let meta = parse_resp_command_arg_slices(acl_setuser, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let cluster_keyslot = b"*3\r\n$7\r\nCLUSTER\r\n$7\r\nKEYSLOT\r\n$5\r\nuser1\r\n";
    let meta = parse_resp_command_arg_slices(cluster_keyslot, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(
        parse_integer_response(&response) as u16,
        redis_hash_slot(b"user1")
    );

    response.clear();
    let cluster_info = b"*2\r\n$7\r\nCLUSTER\r\n$4\r\nINFO\r\n";
    let meta = parse_resp_command_arg_slices(cluster_info, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    let cluster_info_text = std::str::from_utf8(&response).unwrap();
    assert!(cluster_info_text.contains("cluster_slots_assigned:16384"));

    response.clear();
    let failover = b"*1\r\n$8\r\nFAILOVER\r\n";
    let meta = parse_resp_command_arg_slices(failover, &mut args).unwrap();
    let err = processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap_err();
    err.append_resp_error(&mut response);
    assert_eq!(
        response,
        b"-ERR This instance has cluster support disabled\r\n"
    );

    response.clear();
    let monitor = b"*1\r\n$7\r\nMONITOR\r\n";
    let meta = parse_resp_command_arg_slices(monitor, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let shutdown = b"*2\r\n$8\r\nSHUTDOWN\r\n$6\r\nNOSAVE\r\n";
    let meta = parse_resp_command_arg_slices(shutdown, &mut args).unwrap();
    let err = processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap_err();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR SHUTDOWN is disabled in this server\r\n");
}

#[test]
fn pubsub_commands_cover_minimal_ack_and_introspection_shapes() {
    let processor = RequestProcessor::new().unwrap();

    assert_command_response(
        &processor,
        "SUBSCRIBE chan1 chan2",
        b"*3\r\n$9\r\nsubscribe\r\n$5\r\nchan1\r\n:1\r\n*3\r\n$9\r\nsubscribe\r\n$5\r\nchan2\r\n:2\r\n",
    );
    assert_command_response(
        &processor,
        "PSUBSCRIBE p1",
        b"*3\r\n$10\r\npsubscribe\r\n$2\r\np1\r\n:1\r\n",
    );
    assert_command_response(
        &processor,
        "SSUBSCRIBE s1",
        b"*3\r\n$10\r\nssubscribe\r\n$2\r\ns1\r\n:1\r\n",
    );
    assert_command_response(
        &processor,
        "UNSUBSCRIBE chan1 chan2",
        b"*3\r\n$11\r\nunsubscribe\r\n$5\r\nchan1\r\n:1\r\n*3\r\n$11\r\nunsubscribe\r\n$5\r\nchan2\r\n:0\r\n",
    );
    assert_command_response(
        &processor,
        "PUNSUBSCRIBE",
        b"*3\r\n$12\r\npunsubscribe\r\n$-1\r\n:0\r\n",
    );
    assert_command_response(
        &processor,
        "SUNSUBSCRIBE",
        b"*3\r\n$12\r\nsunsubscribe\r\n$-1\r\n:0\r\n",
    );
    assert_command_response(&processor, "PUBLISH c msg", b":0\r\n");
    assert_command_response(&processor, "SPUBLISH sc msg", b":0\r\n");

    assert_command_response(&processor, "PUBSUB CHANNELS", b"*0\r\n");
    assert_command_response(&processor, "PUBSUB SHARDCHANNELS", b"*0\r\n");
    assert_command_response(
        &processor,
        "PUBSUB NUMSUB chan1 chan2",
        b"*4\r\n$5\r\nchan1\r\n:0\r\n$5\r\nchan2\r\n:0\r\n",
    );
    assert_command_response(
        &processor,
        "PUBSUB SHARDNUMSUB s1",
        b"*2\r\n$2\r\ns1\r\n:0\r\n",
    );
    assert_command_response(&processor, "PUBSUB NUMPAT", b":0\r\n");

    assert_command_error(
        &processor,
        "SUBSCRIBE",
        b"-ERR wrong number of arguments for 'subscribe' command\r\n",
    );
    assert_command_error(&processor, "PUBSUB NOPE", b"-ERR unknown command\r\n");
}

#[test]
fn geoadd_supports_basic_options_and_validation_paths() {
    let processor = RequestProcessor::new().unwrap();

    assert_command_integer(
        &processor,
        "GEOADD sicily 13.361389 38.115556 palermo 15.087269 37.502669 catania",
        2,
    );
    assert_command_integer(&processor, "GEOADD sicily 13.361389 38.115556 palermo", 0);
    assert_command_integer(
        &processor,
        "GEOADD sicily CH 13.371389 38.125556 palermo",
        1,
    );
    assert_command_integer(
        &processor,
        "GEOADD sicily NX 13.381389 38.135556 palermo",
        0,
    );
    assert_command_integer(
        &processor,
        "GEOADD sicily XX CH 13.391389 38.145556 palermo",
        1,
    );
    assert_command_integer(
        &processor,
        "GEOADD sicily XX 13.361389 38.115556 agrigento",
        0,
    );
    assert_command_integer(&processor, "EXISTS sicily", 1);

    assert_command_error(
        &processor,
        "GEOADD sicily NX XX 13.5 38.1 x",
        b"-ERR syntax error\r\n",
    );
    assert_command_error(
        &processor,
        "GEOADD sicily GT 13.5 38.1 x",
        b"-ERR syntax error\r\n",
    );
    assert_command_error(
        &processor,
        "GEOADD sicily 181 38.1 x",
        b"-ERR value is out of range\r\n",
    );
    assert_command_error(
        &processor,
        "GEOADD sicily 13.5 86 x",
        b"-ERR value is out of range\r\n",
    );
    assert_command_error(
        &processor,
        "GEOADD sicily nope 38.1 x",
        b"-ERR value is not a valid float\r\n",
    );
    assert_command_error(
        &processor,
        "GEOADD sicily 13.5 38.1",
        b"-ERR wrong number of arguments for 'geoadd' command\r\n",
    );
}

#[test]
fn geopos_returns_coordinates_for_geo_members_and_null_for_missing_entries() {
    let processor = RequestProcessor::new().unwrap();

    assert_command_integer(
        &processor,
        "GEOADD sicily 13.361389 38.115556 palermo 15.087269 37.502669 catania",
        2,
    );

    let response = execute_frame(
        &processor,
        &encode_resp(&[b"GEOPOS", b"sicily", b"palermo"]),
    );
    assert!(response.starts_with(b"*1\r\n*2\r\n$"));
    assert!(response.contains(&b'\n'));

    assert_command_response(&processor, "GEOPOS sicily unknown", b"*1\r\n$-1\r\n");
    assert_command_response(&processor, "GEOPOS missing unknown", b"*1\r\n$-1\r\n");
    assert_command_response(
        &processor,
        "GEOPOS sicily palermo unknown",
        b"*2\r\n*2\r\n$20\r\n13.36138933897018433\r\n$20\r\n38.11555639549629859\r\n$-1\r\n",
    );

    assert_command_response(&processor, "SET plain value", b"+OK\r\n");
    assert_command_error(
        &processor,
        "GEOPOS plain member",
        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
    );
    assert_command_response(&processor, "GEOPOS sicily", b"*0\r\n");
}

#[test]
fn geodist_supports_units_and_missing_member_semantics() {
    let processor = RequestProcessor::new().unwrap();
    assert_command_integer(
        &processor,
        "GEOADD sicily 13.361389 38.115556 palermo 15.087269 37.502669 catania",
        2,
    );

    let meters = parse_bulk_payload(&execute_frame(
        &processor,
        &encode_resp(&[b"GEODIST", b"sicily", b"palermo", b"catania"]),
    ))
    .expect("distance should exist");
    let meters = core::str::from_utf8(&meters)
        .unwrap()
        .parse::<f64>()
        .unwrap();
    assert!(meters > 100000.0);

    let kilometers = parse_bulk_payload(&execute_frame(
        &processor,
        &encode_resp(&[b"GEODIST", b"sicily", b"palermo", b"catania", b"km"]),
    ))
    .expect("distance should exist");
    let kilometers = core::str::from_utf8(&kilometers)
        .unwrap()
        .parse::<f64>()
        .unwrap();
    assert!((meters / 1000.0 - kilometers).abs() < 1.0);

    assert_command_response(&processor, "GEODIST sicily palermo unknown", b"$-1\r\n");
    assert_command_response(&processor, "GEODIST missing palermo catania", b"$-1\r\n");

    assert_command_response(&processor, "SET plain value", b"+OK\r\n");
    assert_command_error(
        &processor,
        "GEODIST plain palermo catania",
        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
    );
    assert_command_error(
        &processor,
        "GEODIST sicily palermo catania parsec",
        b"-ERR unsupported unit provided. please use M, KM, FT, MI\r\n",
    );
    assert_command_error(
        &processor,
        "GEODIST sicily palermo",
        b"-ERR wrong number of arguments for 'geodist' command\r\n",
    );
}

#[test]
fn geohash_returns_expected_shape_and_null_for_missing_members() {
    let processor = RequestProcessor::new().unwrap();
    assert_command_integer(
        &processor,
        "GEOADD sicily 13.361389 38.115556 palermo 15.087269 37.502669 catania",
        2,
    );

    assert_command_response(
        &processor,
        "GEOHASH sicily palermo catania",
        b"*2\r\n$11\r\nsqc8b49rny0\r\n$11\r\nsqdtr74hyu0\r\n",
    );
    assert_command_response(&processor, "GEOHASH sicily unknown", b"*1\r\n$-1\r\n");
    assert_command_response(&processor, "GEOHASH missing unknown", b"*1\r\n$-1\r\n");

    assert_command_response(&processor, "SET plain value", b"+OK\r\n");
    assert_command_error(
        &processor,
        "GEOHASH plain member",
        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
    );
    assert_command_response(&processor, "GEOHASH sicily", b"*0\r\n");
}

#[test]
fn geosearch_supports_radius_box_and_response_options() {
    let processor = RequestProcessor::new().unwrap();
    assert_command_integer(
        &processor,
        "GEOADD sicily 13.361389 38.115556 palermo 15.087269 37.502669 catania",
        2,
    );

    assert_command_response(
        &processor,
        "GEOSEARCH sicily FROMMEMBER palermo BYRADIUS 200 km ASC COUNT 1",
        b"*1\r\n$7\r\npalermo\r\n",
    );
    assert_command_response(
        &processor,
        "GEOSEARCH sicily FROMMEMBER palermo BYRADIUS 200 km DESC COUNT 1",
        b"*1\r\n$7\r\ncatania\r\n",
    );
    assert_command_response(
        &processor,
        "GEOSEARCH sicily FROMLONLAT 13.361389 38.115556 BYBOX 400 400 km ASC COUNT 2",
        b"*2\r\n$7\r\npalermo\r\n$7\r\ncatania\r\n",
    );

    let with_options = execute_frame(
        &processor,
        &encode_resp(&[
            b"GEOSEARCH",
            b"sicily",
            b"FROMMEMBER",
            b"palermo",
            b"BYRADIUS",
            b"200",
            b"km",
            b"WITHDIST",
            b"WITHHASH",
            b"WITHCOORD",
            b"ASC",
            b"COUNT",
            b"1",
        ]),
    );
    assert!(with_options.starts_with(b"*1\r\n*4\r\n$7\r\npalermo\r\n$6\r\n0.0000\r\n:"));
    assert!(with_options.windows(5).any(|window| window == b"*2\r\n$"));

    assert_command_response(
        &processor,
        "GEOSEARCH missing FROMMEMBER palermo BYRADIUS 200 km",
        b"*0\r\n",
    );
    assert_command_error(
        &processor,
        "GEOSEARCH sicily FROMMEMBER unknown BYRADIUS 200 km",
        b"-ERR no such key\r\n",
    );
    assert_command_error(
        &processor,
        "GEOSEARCH sicily FROMMEMBER palermo BYRADIUS -1 km",
        b"-ERR value is out of range\r\n",
    );
    assert_command_error(
        &processor,
        "GEOSEARCH sicily FROMMEMBER palermo BYRADIUS 200 km COUNT 0",
        b"-ERR value is out of range\r\n",
    );

    assert_command_response(&processor, "SET plain value", b"+OK\r\n");
    assert_command_error(
        &processor,
        "GEOSEARCH plain FROMMEMBER palermo BYRADIUS 200 km",
        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
    );
    assert_command_error(
        &processor,
        "GEOSEARCH sicily BYRADIUS 200 km",
        b"-ERR wrong number of arguments for 'geosearch' command\r\n",
    );
}

#[test]
fn geosearchstore_stores_results_and_clears_destination_on_empty_source() {
    let processor = RequestProcessor::new().unwrap();
    assert_command_integer(
        &processor,
        "GEOADD sicily 13.361389 38.115556 palermo 15.087269 37.502669 catania",
        2,
    );

    assert_command_integer(
        &processor,
        "GEOSEARCHSTORE gdst sicily FROMMEMBER palermo BYRADIUS 200 km STOREDIST",
        2,
    );
    assert_command_response(
        &processor,
        "ZRANGE gdst 0 -1",
        b"*2\r\n$7\r\npalermo\r\n$7\r\ncatania\r\n",
    );

    assert_command_integer(
        &processor,
        "GEOSEARCHSTORE gdst missing FROMMEMBER palermo BYRADIUS 200 km",
        0,
    );
    assert_command_integer(&processor, "EXISTS gdst", 0);

    assert_command_response(&processor, "SET plain value", b"+OK\r\n");
    assert_command_error(
        &processor,
        "GEOSEARCHSTORE gdst plain FROMMEMBER palermo BYRADIUS 200 km",
        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
    );
    assert_command_error(
        &processor,
        "GEOSEARCHSTORE gdst sicily FROMMEMBER palermo BYRADIUS 200 km WITHDIST",
        b"-ERR syntax error\r\n",
    );
    assert_command_error(
        &processor,
        "GEOSEARCHSTORE gdst sicily FROMMEMBER palermo",
        b"-ERR wrong number of arguments for 'geosearchstore' command\r\n",
    );
}

#[test]
fn georadius_family_supports_query_and_store_paths() {
    let processor = RequestProcessor::new().unwrap();
    assert_command_integer(
        &processor,
        "GEOADD sicily 13.361389 38.115556 palermo 15.087269 37.502669 catania",
        2,
    );

    assert_command_response(
        &processor,
        "GEORADIUS sicily 15 37 200 km ASC COUNT 1",
        b"*1\r\n$7\r\ncatania\r\n",
    );
    assert_command_response(
        &processor,
        "GEORADIUSBYMEMBER sicily palermo 200 km DESC COUNT 1",
        b"*1\r\n$7\r\ncatania\r\n",
    );
    assert_command_integer(
        &processor,
        "GEOADD nyc -73.9733487 40.7648057 \"central park n/q/r\" -73.9903085 40.7362513 \"union square\" -74.0131604 40.7126674 \"wtc one\" -73.7858139 40.6428986 \"jfk\" -73.9375699 40.7498929 \"q4\" -73.9564142 40.7480973 4545",
        6,
    );
    assert_command_response(
        &processor,
        "GEORADIUS nyc -73.9798091 40.7598464 10 km COUNT 3 ANY ASC",
        b"*3\r\n$18\r\ncentral park n/q/r\r\n$12\r\nunion square\r\n$7\r\nwtc one\r\n",
    );
    assert_command_integer(&processor, "GEOADD k1 45 65 n1 -135 85.05 n2", 2);
    assert_command_response(
        &processor,
        "GEORADIUSBYMEMBER k1 n1 5009431 m",
        b"*2\r\n$2\r\nn1\r\n$2\r\nn2\r\n",
    );
    assert_command_error(
        &processor,
        "GEORADIUSBYMEMBER_RO sicily missing 200 km",
        b"-ERR no such key\r\n",
    );

    let with_options = execute_frame(
        &processor,
        &encode_resp(&[
            b"GEORADIUS_RO",
            b"sicily",
            b"15",
            b"37",
            b"200",
            b"km",
            b"WITHDIST",
            b"WITHHASH",
            b"WITHCOORD",
            b"ASC",
            b"COUNT",
            b"1",
        ]),
    );
    assert!(with_options.starts_with(b"*1\r\n*4\r\n$7\r\ncatania\r\n$"));
    assert!(with_options.windows(5).any(|window| window == b"*2\r\n$"));

    assert_command_integer(
        &processor,
        "GEORADIUS sicily 15 37 200 km STOREDIST rdist",
        2,
    );
    assert_command_response(
        &processor,
        "ZRANGE rdist 0 -1",
        b"*2\r\n$7\r\ncatania\r\n$7\r\npalermo\r\n",
    );

    assert_command_integer(&processor, "GEORADIUS sicily 15 37 200 km STORE rstore", 2);
    assert_command_integer(
        &processor,
        "GEORADIUS missing 15 37 200 km STOREDIST rstore",
        0,
    );
    assert_command_integer(&processor, "EXISTS rstore", 0);

    assert_command_error(
        &processor,
        "GEORADIUS_RO sicily 15 37 200 km STORE bad",
        b"-ERR syntax error\r\n",
    );
    assert_command_error(
        &processor,
        "GEORADIUSBYMEMBER_RO sicily palermo 200 km STOREDIST bad",
        b"-ERR syntax error\r\n",
    );
    assert_command_error(
        &processor,
        "GEORADIUS sicily 15 37 -1 km",
        b"-ERR value is out of range\r\n",
    );
    assert_command_error(
        &processor,
        "GEORADIUS sicily 15 37 200 km COUNT 0",
        b"-ERR value is out of range\r\n",
    );
    assert_command_error(
        &processor,
        "GEORADIUSBYMEMBER sicily palermo 200 km ANY",
        b"-ERR ANY option requires COUNT option\r\n",
    );

    assert_command_response(&processor, "SET plain value", b"+OK\r\n");
    assert_command_error(
        &processor,
        "GEORADIUS plain 15 37 200 km",
        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
    );
    assert_command_error(
        &processor,
        "GEORADIUS_RO sicily 15 37",
        b"-ERR wrong number of arguments for 'georadius_ro' command\r\n",
    );
    assert_command_error(
        &processor,
        "GEORADIUSBYMEMBER_RO sicily palermo",
        b"-ERR wrong number of arguments for 'georadiusbymember_ro' command\r\n",
    );
}

#[test]
fn migrate_command_validates_arguments_before_disabled_response() {
    let processor = RequestProcessor::new().unwrap();

    assert_command_error(
        &processor,
        "MIGRATE 127.0.0.1 6379 key 0 1000",
        b"-ERR MIGRATE is disabled in this server\r\n",
    );
    assert_command_error(
        &processor,
        "MIGRATE 127.0.0.1 notaport key 0 1000",
        b"-ERR value is not an integer or out of range\r\n",
    );
    assert_command_error(
        &processor,
        "MIGRATE 127.0.0.1 6379 key 1 1000",
        b"-ERR MIGRATE is disabled in this server\r\n",
    );
    assert_command_error(
        &processor,
        "MIGRATE 127.0.0.1 6379 key 0 -1",
        b"-ERR value is out of range\r\n",
    );
    assert_command_error(
        &processor,
        "MIGRATE 127.0.0.1 6379 key 0 1000 KEYS other",
        b"-ERR syntax error\r\n",
    );

    let mut args = [ArgSlice::EMPTY; 16];
    let empty_key_with_keys = encode_resp(&[
        b"MIGRATE",
        b"127.0.0.1",
        b"6379",
        b"",
        b"0",
        b"1000",
        b"KEYS",
        b"one",
        b"two",
    ]);
    let meta = parse_resp_command_arg_slices(&empty_key_with_keys, &mut args).unwrap();
    let mut response = Vec::new();
    let err = processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap_err();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR MIGRATE is disabled in this server\r\n");

    response.clear();
    let empty_key_without_keys =
        encode_resp(&[b"MIGRATE", b"127.0.0.1", b"6379", b"", b"0", b"1000"]);
    let meta = parse_resp_command_arg_slices(&empty_key_without_keys, &mut args).unwrap();
    let err = processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap_err();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR syntax error\r\n");

    assert_command_error(
        &processor,
        "MIGRATE 127.0.0.1 6379 key 0",
        b"-ERR wrong number of arguments for 'migrate' command\r\n",
    );
}

#[test]
fn function_flush_returns_ok() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 4];
    let mut response = Vec::new();

    let function_flush = b"*2\r\n$8\r\nFUNCTION\r\n$5\r\nFLUSH\r\n";
    let meta = parse_resp_command_arg_slices(function_flush, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");
}

#[test]
fn debug_set_active_expire_returns_ok() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 4];
    let mut response = Vec::new();
    assert!(processor.active_expire_enabled());

    let debug_disable = b"*3\r\n$5\r\nDEBUG\r\n$17\r\nSET-ACTIVE-EXPIRE\r\n$1\r\n0\r\n";
    let meta = parse_resp_command_arg_slices(debug_disable, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");
    assert!(!processor.active_expire_enabled());

    response.clear();
    let debug_enable = b"*3\r\n$5\r\nDEBUG\r\n$17\r\nSET-ACTIVE-EXPIRE\r\n$1\r\n1\r\n";
    let meta = parse_resp_command_arg_slices(debug_enable, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");
    assert!(processor.active_expire_enabled());
}

#[test]
fn debug_loadaof_returns_ok() {
    let processor = RequestProcessor::new().unwrap();
    assert_command_response(&processor, "DEBUG LOADAOF", b"+OK\r\n");
}

#[test]
fn object_encoding_and_refcount_report_basic_metadata() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 8];
    let mut response = Vec::new();

    let lpush = b"*5\r\n$5\r\nLPUSH\r\n$4\r\nlist\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n";
    let meta = parse_resp_command_arg_slices(lpush, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":3\r\n");

    response.clear();
    let object_encoding = b"*3\r\n$6\r\nOBJECT\r\n$8\r\nENCODING\r\n$4\r\nlist\r\n";
    let meta = parse_resp_command_arg_slices(object_encoding, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"$8\r\nlistpack\r\n");

    response.clear();
    let object_refcount = b"*3\r\n$6\r\nOBJECT\r\n$8\r\nREFCOUNT\r\n$4\r\nlist\r\n";
    let meta = parse_resp_command_arg_slices(object_refcount, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":1\r\n");
}

#[test]
fn debug_digest_value_matches_for_equal_payloads() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 8];
    let mut response = Vec::new();

    let set_a = b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$5\r\nvalue\r\n";
    let meta = parse_resp_command_arg_slices(set_a, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let copy = b"*3\r\n$4\r\nCOPY\r\n$1\r\na\r\n$1\r\nb\r\n";
    let meta = parse_resp_command_arg_slices(copy, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let digest_a = b"*3\r\n$5\r\nDEBUG\r\n$12\r\nDIGEST-VALUE\r\n$1\r\na\r\n";
    let meta = parse_resp_command_arg_slices(digest_a, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    let digest_a_response = response.clone();
    assert!(digest_a_response.starts_with(b"$16\r\n"));

    response.clear();
    let digest_b = b"*3\r\n$5\r\nDEBUG\r\n$12\r\nDIGEST-VALUE\r\n$1\r\nb\r\n";
    let meta = parse_resp_command_arg_slices(digest_b, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, digest_a_response);
}

#[test]
fn stream_commands_support_copy_and_xinfo_full_digest() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 24];
    let mut response = Vec::new();

    let xadd = b"*5\r\n$4\r\nXADD\r\n$7\r\nstream1\r\n$1\r\n*\r\n$5\r\nfield\r\n$5\r\nvalue\r\n";
    let meta = parse_resp_command_arg_slices(xadd, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert!(response.starts_with(b"$"));

    response.clear();
    let xlen = b"*2\r\n$4\r\nXLEN\r\n$7\r\nstream1\r\n";
    let meta = parse_resp_command_arg_slices(xlen, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let xadd_second =
        b"*5\r\n$4\r\nXADD\r\n$7\r\nstream1\r\n$1\r\n*\r\n$5\r\nfield\r\n$6\r\nvalue2\r\n";
    let meta = parse_resp_command_arg_slices(xadd_second, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert!(response.starts_with(b"$"));

    response.clear();
    let meta = parse_resp_command_arg_slices(xlen, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":2\r\n");

    response.clear();
    let xrange_all = b"*4\r\n$6\r\nXRANGE\r\n$7\r\nstream1\r\n$1\r\n-\r\n$1\r\n+\r\n";
    let meta = parse_resp_command_arg_slices(xrange_all, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert!(response.starts_with(b"*2\r\n"));

    response.clear();
    let xrange_count_1 =
        b"*6\r\n$6\r\nXRANGE\r\n$7\r\nstream1\r\n$1\r\n-\r\n$1\r\n+\r\n$5\r\nCOUNT\r\n$1\r\n1\r\n";
    let meta = parse_resp_command_arg_slices(xrange_count_1, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    let xrange_head = response.clone();
    assert!(xrange_head.starts_with(b"*1\r\n"));

    response.clear();
    let xrevrange_count_1 = b"*6\r\n$9\r\nXREVRANGE\r\n$7\r\nstream1\r\n$1\r\n+\r\n$1\r\n-\r\n$5\r\nCOUNT\r\n$1\r\n1\r\n";
    let meta = parse_resp_command_arg_slices(xrevrange_count_1, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert!(response.starts_with(b"*1\r\n"));
    assert_ne!(response, xrange_head);

    response.clear();
    let xgroup_create =
        b"*5\r\n$6\r\nXGROUP\r\n$6\r\nCREATE\r\n$7\r\nstream1\r\n$2\r\ng1\r\n$1\r\n0\r\n";
    let meta = parse_resp_command_arg_slices(xgroup_create, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let xreadgroup = b"*9\r\n$10\r\nXREADGROUP\r\n$5\r\nGROUP\r\n$2\r\ng1\r\n$8\r\nconsumer\r\n$5\r\nCOUNT\r\n$1\r\n1\r\n$7\r\nSTREAMS\r\n$7\r\nstream1\r\n$1\r\n>\r\n";
    let meta = parse_resp_command_arg_slices(xreadgroup, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert!(response.starts_with(b"*"));

    response.clear();
    let xinfo_stream = b"*4\r\n$5\r\nXINFO\r\n$6\r\nSTREAM\r\n$7\r\nstream1\r\n$4\r\nFULL\r\n";
    let meta = parse_resp_command_arg_slices(xinfo_stream, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    let source_info = response.clone();
    assert!(source_info.starts_with(b"$16\r\n"));

    response.clear();
    let copy = b"*3\r\n$4\r\nCOPY\r\n$7\r\nstream1\r\n$7\r\nstream2\r\n";
    let meta = parse_resp_command_arg_slices(copy, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let xinfo_stream_copy = b"*4\r\n$5\r\nXINFO\r\n$6\r\nSTREAM\r\n$7\r\nstream2\r\n$4\r\nFULL\r\n";
    let meta = parse_resp_command_arg_slices(xinfo_stream_copy, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, source_info);
}

#[test]
fn stream_commands_cover_xread_xpending_xclaim_xautoclaim_xack_and_xsetid() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 24];
    let mut response = Vec::new();

    let xadd_first =
        b"*5\r\n$4\r\nXADD\r\n$7\r\nstreamx\r\n$3\r\n1-0\r\n$5\r\nfield\r\n$3\r\none\r\n";
    let meta = parse_resp_command_arg_slices(xadd_first, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"$3\r\n1-0\r\n");

    response.clear();
    let xadd_second =
        b"*5\r\n$4\r\nXADD\r\n$7\r\nstreamx\r\n$3\r\n2-0\r\n$5\r\nfield\r\n$3\r\ntwo\r\n";
    let meta = parse_resp_command_arg_slices(xadd_second, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"$3\r\n2-0\r\n");

    response.clear();
    let xgroup_create =
        b"*5\r\n$6\r\nXGROUP\r\n$6\r\nCREATE\r\n$7\r\nstreamx\r\n$2\r\ng1\r\n$1\r\n0\r\n";
    let meta = parse_resp_command_arg_slices(xgroup_create, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let xread = b"*6\r\n$5\r\nXREAD\r\n$5\r\nCOUNT\r\n$1\r\n1\r\n$7\r\nSTREAMS\r\n$7\r\nstreamx\r\n$3\r\n0-0\r\n";
    let meta = parse_resp_command_arg_slices(xread, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert!(response.starts_with(b"*1\r\n"));
    assert!(
        response
            .windows(13)
            .any(|window| window == b"$7\r\nstreamx\r\n")
    );
    assert!(response.windows(9).any(|window| window == b"$3\r\n1-0\r\n"));

    response.clear();
    let xread_tail = b"*4\r\n$5\r\nXREAD\r\n$7\r\nSTREAMS\r\n$7\r\nstreamx\r\n$1\r\n$\r\n";
    let meta = parse_resp_command_arg_slices(xread_tail, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"*-1\r\n");

    response.clear();
    let xack = b"*4\r\n$4\r\nXACK\r\n$7\r\nstreamx\r\n$2\r\ng1\r\n$3\r\n1-0\r\n";
    let meta = parse_resp_command_arg_slices(xack, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let xpending_summary = b"*3\r\n$8\r\nXPENDING\r\n$7\r\nstreamx\r\n$2\r\ng1\r\n";
    let meta = parse_resp_command_arg_slices(xpending_summary, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"*4\r\n:0\r\n$-1\r\n$-1\r\n*0\r\n");

    response.clear();
    let xpending_detail =
        b"*6\r\n$8\r\nXPENDING\r\n$7\r\nstreamx\r\n$2\r\ng1\r\n$1\r\n-\r\n$1\r\n+\r\n$1\r\n1\r\n";
    let meta = parse_resp_command_arg_slices(xpending_detail, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"*0\r\n");

    response.clear();
    let xclaim =
        b"*7\r\n$6\r\nXCLAIM\r\n$7\r\nstreamx\r\n$2\r\ng1\r\n$2\r\nc1\r\n$1\r\n1\r\n$3\r\n1-0\r\n$6\r\nJUSTID\r\n";
    let meta = parse_resp_command_arg_slices(xclaim, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"*0\r\n");

    response.clear();
    let xautoclaim = b"*9\r\n$10\r\nXAUTOCLAIM\r\n$7\r\nstreamx\r\n$2\r\ng1\r\n$2\r\nc1\r\n$1\r\n1\r\n$3\r\n0-0\r\n$5\r\nCOUNT\r\n$2\r\n10\r\n$6\r\nJUSTID\r\n";
    let meta = parse_resp_command_arg_slices(xautoclaim, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert!(response.starts_with(b"*3\r\n"));
    assert!(response.windows(9).any(|window| window == b"$3\r\n0-0\r\n"));

    response.clear();
    let xsetid = b"*3\r\n$6\r\nXSETID\r\n$7\r\nstreamx\r\n$3\r\n2-0\r\n";
    let meta = parse_resp_command_arg_slices(xsetid, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let xsetid_missing = b"*3\r\n$6\r\nXSETID\r\n$8\r\nmissing1\r\n$3\r\n1-0\r\n";
    let meta = parse_resp_command_arg_slices(xsetid_missing, &mut args).unwrap();
    let err = processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap_err();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR no such key\r\n");

    response.clear();
    let xpending_missing = b"*3\r\n$8\r\nXPENDING\r\n$8\r\nmissing1\r\n$2\r\ng1\r\n";
    let meta = parse_resp_command_arg_slices(xpending_missing, &mut args).unwrap();
    let err = processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap_err();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-NOGROUP No such key or consumer group\r\n");
}

#[test]
fn xtrim_supports_maxlen_minid_and_limit_options() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 24];
    let mut response = Vec::new();

    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"XADD", b"streamx", b"1-0", b"field", b"one"])
        ),
        b"$3\r\n1-0\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"XADD", b"streamx", b"2-0", b"field", b"two"])
        ),
        b"$3\r\n2-0\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"XADD", b"streamx", b"3-0", b"field", b"three"])
        ),
        b"$3\r\n3-0\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"XTRIM", b"streamx", b"MAXLEN", b"2"])
        ),
        b":1\r\n"
    );
    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"XLEN", b"streamx"])),
        b":2\r\n"
    );
    let range_after_maxlen = execute_frame(
        &processor,
        &encode_resp(&[b"XRANGE", b"streamx", b"-", b"+"]),
    );
    assert!(range_after_maxlen.starts_with(b"*2\r\n"));
    assert!(range_after_maxlen.windows(3).any(|window| window == b"2-0"));
    assert!(range_after_maxlen.windows(3).any(|window| window == b"3-0"));

    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"XADD", b"streamx", b"4-0", b"field", b"four"])
        ),
        b"$3\r\n4-0\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"XTRIM", b"streamx", b"MINID", b"4-0", b"LIMIT", b"1"])
        ),
        b":1\r\n"
    );
    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"XLEN", b"streamx"])),
        b":2\r\n"
    );

    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"XTRIM", b"streamx", b"MAXLEN", b"~", b"1", b"LIMIT", b"1"])
        ),
        b":1\r\n"
    );

    response.clear();
    let xtrim_bad_strategy = b"*4\r\n$5\r\nXTRIM\r\n$7\r\nstreamx\r\n$7\r\nUNKNOWN\r\n$1\r\n1\r\n";
    let meta = parse_resp_command_arg_slices(xtrim_bad_strategy, &mut args).unwrap();
    let err = processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap_err();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR syntax error\r\n");

    response.clear();
    let xtrim_negative_limit =
        b"*6\r\n$5\r\nXTRIM\r\n$7\r\nstreamx\r\n$6\r\nMAXLEN\r\n$1\r\n1\r\n$5\r\nLIMIT\r\n$2\r\n-1\r\n";
    let meta = parse_resp_command_arg_slices(xtrim_negative_limit, &mut args).unwrap();
    let err = processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap_err();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR value is out of range\r\n");
}

#[test]
fn script_flush_returns_ok() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 4];
    let mut response = Vec::new();

    let script_flush = b"*2\r\n$6\r\nSCRIPT\r\n$5\r\nFLUSH\r\n";
    let meta = parse_resp_command_arg_slices(script_flush, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");
}

#[test]
fn script_flush_sync_returns_ok() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 4];
    let mut response = Vec::new();

    let script_flush_sync = b"*3\r\n$6\r\nSCRIPT\r\n$5\r\nFLUSH\r\n$4\r\nSYNC\r\n";
    let meta = parse_resp_command_arg_slices(script_flush_sync, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");
}

#[test]
fn script_help_debug_and_kill_cover_minimal_surface() {
    let processor = RequestProcessor::new().unwrap();

    let help_response = execute_frame(&processor, &encode_resp(&[b"SCRIPT", b"HELP"]));
    assert!(help_response.starts_with(b"*6\r\n"));
    let help_text = String::from_utf8_lossy(&help_response);
    assert!(help_text.contains("LOAD <script>"));

    assert_command_response(&processor, "SCRIPT DEBUG YES", b"+OK\r\n");
    assert_command_response(&processor, "SCRIPT DEBUG SYNC", b"+OK\r\n");
    assert_command_response(&processor, "SCRIPT DEBUG NO", b"+OK\r\n");
    assert_command_error(&processor, "SCRIPT DEBUG MAYBE", b"-ERR syntax error\r\n");

    assert_command_response(
        &processor,
        "SCRIPT KILL",
        b"-NOTBUSY No scripts in execution right now.\r\n",
    );
}

#[test]
fn scripting_eval_and_fcall_commands_validate_numkeys_then_return_disabled() {
    let processor = RequestProcessor::new().unwrap();

    for command in [
        "EVAL \"return 1\" 0",
        "EVAL_RO \"return 1\" 0",
        "EVALSHA deadbeef 0",
        "EVALSHA_RO deadbeef 0",
        "FCALL fn 0",
        "FCALL_RO fn 0",
    ] {
        assert_command_error(
            &processor,
            command,
            b"-ERR scripting is disabled in this server\r\n",
        );
    }

    assert_command_error(
        &processor,
        "EVAL \"return 1\" -1",
        b"-ERR value is out of range\r\n",
    );
    assert_command_error(
        &processor,
        "EVAL \"return 1\" notint",
        b"-ERR value is not an integer or out of range\r\n",
    );
    assert_command_error(&processor, "EVAL \"return 1\" 1", b"-ERR syntax error\r\n");
    assert_command_error(
        &processor,
        "SCRIPT LOAD \"return 1\"",
        b"-ERR scripting is disabled in this server\r\n",
    );
}

#[test]
fn scripting_eval_executes_when_enabled() {
    let processor = RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap();

    assert_command_response(&processor, "EVAL \"return 1\" 0", b":1\r\n");
    assert_command_response(&processor, "EVAL \"return 'pong'\" 0", b"$4\r\npong\r\n");
    assert_command_response(
        &processor,
        "EVAL \"redis.call('SET','k','v'); return redis.call('GET','k')\" 0",
        b"$1\r\nv\r\n",
    );
}

#[test]
fn scripting_eval_ro_rejects_write_calls() {
    let processor = RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap();
    let response = execute_frame(
        &processor,
        &encode_resp(&[b"EVAL_RO", b"return redis.call('SET','k','v')", b"0"]),
    );
    let response_text = String::from_utf8_lossy(&response);
    assert!(response_text.starts_with("-ERR Error running script:"));
    assert!(response_text.contains("Write commands are not allowed from read-only scripts"));
}

#[test]
fn script_load_exists_evalsha_and_flush_when_enabled() {
    let processor = RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap();

    let script_load = encode_resp(&[b"SCRIPT", b"LOAD", b"return redis.call('PING')"]);
    let sha_response = execute_frame(&processor, &script_load);
    let sha = parse_bulk_payload(&sha_response).expect("SCRIPT LOAD returns sha1 bulk string");
    assert_eq!(sha.len(), 40);

    let exists_frame = encode_resp(&[b"SCRIPT", b"EXISTS", sha.as_slice(), b"deadbeef"]);
    assert_eq!(
        execute_frame(&processor, &exists_frame),
        b"*2\r\n:1\r\n:0\r\n"
    );

    let evalsha_frame = encode_resp(&[b"EVALSHA", sha.as_slice(), b"0"]);
    assert_eq!(execute_frame(&processor, &evalsha_frame), b"+PONG\r\n");

    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"SCRIPT", b"FLUSH"])),
        b"+OK\r\n"
    );

    let mut args = [ArgSlice::EMPTY; 16];
    let evalsha_after_flush = encode_resp(&[b"EVALSHA", sha.as_slice(), b"0"]);
    let meta = parse_resp_command_arg_slices(&evalsha_after_flush, &mut args).unwrap();
    let mut response = Vec::new();
    let error = processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap_err();
    error.append_resp_error(&mut response);
    assert_eq!(
        response,
        b"-NOSCRIPT No matching script. Please use EVAL.\r\n"
    );
}

#[test]
fn function_load_and_fcall_ro_work_when_scripting_is_enabled() {
    let processor = RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap();
    let library_source = b"#!lua name=lib_readonly\nredis.register_function{function_name='ro_get', callback=function(keys, args) return redis.call('GET', keys[1]) end, flags={'no-writes'}}";
    let function_load = encode_resp(&[b"FUNCTION", b"LOAD", library_source]);
    assert_eq!(
        execute_frame(&processor, &function_load),
        b"$12\r\nlib_readonly\r\n"
    );
    assert_command_response(&processor, "SET mykey myvalue", b"+OK\r\n");
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"FCALL_RO", b"ro_get", b"1", b"mykey"])
        ),
        b"$7\r\nmyvalue\r\n"
    );
}

#[test]
fn fcall_ro_rejects_non_readonly_registered_function() {
    let processor = RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap();
    let library_source = b"#!lua name=lib_rw\nredis.register_function{function_name='rw_set', callback=function(keys, args) return redis.call('SET', keys[1], args[1]) end}";
    let function_load = encode_resp(&[b"FUNCTION", b"LOAD", library_source]);
    assert_eq!(
        execute_frame(&processor, &function_load),
        b"$6\r\nlib_rw\r\n"
    );
    assert_command_error(
        &processor,
        "FCALL_RO rw_set 1 k v",
        b"-ERR Can not execute a script with write flag using *_ro command\r\n",
    );
}

#[test]
fn function_flush_clears_loaded_functions() {
    let processor = RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap();
    let library_source = b"#!lua name=lib_flush\nredis.register_function{function_name='ro_ping', callback=function(keys, args) return redis.call('PING') end, flags={'no-writes'}}";
    let function_load = encode_resp(&[b"FUNCTION", b"LOAD", library_source]);
    assert_eq!(
        execute_frame(&processor, &function_load),
        b"$9\r\nlib_flush\r\n"
    );
    assert_command_response(&processor, "FUNCTION FLUSH", b"+OK\r\n");
    assert_command_error(
        &processor,
        "FCALL_RO ro_ping 0",
        b"-ERR Function not found\r\n",
    );
}

#[test]
fn function_help_list_kill_delete_flush_and_stats_cover_minimal_surface() {
    let processor = RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap();
    let library_source = b"#!lua name=lib_admin\nredis.register_function{function_name='rw_set', callback=function(keys, args) return redis.call('SET', keys[1], args[1]) end}\nredis.register_function{function_name='ro_get', callback=function(keys, args) return redis.call('GET', keys[1]) end, flags={'no-writes'}}";
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"FUNCTION", b"LOAD", library_source])
        ),
        b"$9\r\nlib_admin\r\n"
    );

    let help_response = execute_frame(&processor, &encode_resp(&[b"FUNCTION", b"HELP"]));
    assert!(help_response.starts_with(b"*9\r\n"));
    assert!(String::from_utf8_lossy(&help_response).contains("LIST [WITHCODE]"));
    assert!(String::from_utf8_lossy(&help_response).contains("KILL"));

    let list_response = execute_frame(&processor, &encode_resp(&[b"FUNCTION", b"LIST"]));
    let list_text = String::from_utf8_lossy(&list_response);
    assert!(list_text.contains("library_name"));
    assert!(list_text.contains("lib_admin"));
    assert!(list_text.contains("rw_set"));
    assert!(list_text.contains("ro_get"));
    assert!(list_text.contains("description"));
    assert!(list_text.contains("flags"));
    assert!(list_text.contains("no-writes"));
    assert!(!list_text.contains("library_code"));

    let list_with_code = execute_frame(
        &processor,
        &encode_resp(&[b"FUNCTION", b"LIST", b"WITHCODE"]),
    );
    let list_with_code_text = String::from_utf8_lossy(&list_with_code);
    assert!(list_with_code_text.contains("library_code"));
    assert!(list_with_code_text.contains("#!lua name=lib_admin"));

    let stats_response = execute_frame(&processor, &encode_resp(&[b"FUNCTION", b"STATS"]));
    let stats_text = String::from_utf8_lossy(&stats_response);
    assert!(stats_text.contains("running_script"));
    assert!(stats_text.contains("engines"));
    assert!(stats_text.contains("libraries_count"));
    assert!(stats_text.contains("functions_count"));
    assert!(stats_text.contains(":1\r\n"));
    assert!(stats_text.contains(":2\r\n"));

    assert_command_response(
        &processor,
        "FUNCTION KILL",
        b"-NOTBUSY No scripts in execution right now.\r\n",
    );

    assert_command_response(&processor, "FUNCTION DELETE lib_admin", b"+OK\r\n");
    assert_command_error(
        &processor,
        "FCALL rw_set 1 k v",
        b"-ERR Function not found\r\n",
    );
    assert_command_error(
        &processor,
        "FUNCTION DELETE lib_admin",
        b"-ERR Library not found\r\n",
    );

    assert_command_response(&processor, "FUNCTION FLUSH ASYNC", b"+OK\r\n");
    assert_command_response(
        &processor,
        "FUNCTION FLUSH maybe",
        b"-ERR FUNCTION FLUSH only supports SYNC|ASYNC\r\n",
    );
    assert_command_response(
        &processor,
        "FUNCTION FLUSH SYNC extra",
        b"-ERR unknown subcommand or wrong number of arguments for 'flush'. Try FUNCTION HELP.\r\n",
    );
}

#[test]
fn function_dump_and_restore_roundtrip_supports_append_and_replace_modes() {
    let processor = RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap();
    let lib_one = b"#!lua name=lib_dump_one\nredis.register_function{function_name='rw_set', callback=function(keys, args) return redis.call('SET', keys[1], args[1]) end}";
    let lib_two = b"#!lua name=lib_dump_two\nredis.register_function{function_name='ro_get', callback=function(keys, args) return redis.call('GET', keys[1]) end, flags={'no-writes'}}";
    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"FUNCTION", b"LOAD", lib_one])),
        b"$12\r\nlib_dump_one\r\n"
    );
    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"FUNCTION", b"LOAD", lib_two])),
        b"$12\r\nlib_dump_two\r\n"
    );

    let dump_payload = parse_bulk_payload(&execute_frame(
        &processor,
        &encode_resp(&[b"FUNCTION", b"DUMP"]),
    ))
    .expect("FUNCTION DUMP returns bulk payload");
    assert!(dump_payload.len() >= 8);

    assert_command_response(&processor, "FUNCTION FLUSH", b"+OK\r\n");
    assert_command_error(
        &processor,
        "FCALL rw_set 1 restore:key restore:value",
        b"-ERR Function not found\r\n",
    );

    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"FUNCTION", b"RESTORE", dump_payload.as_slice(), b"FLUSH"])
        ),
        b"+OK\r\n"
    );
    assert_command_response(
        &processor,
        "FCALL rw_set 1 restore:key restore:value",
        b"+OK\r\n",
    );
    assert_command_response(
        &processor,
        "FCALL_RO ro_get 1 restore:key",
        b"$13\r\nrestore:value\r\n",
    );

    assert_command_response(
        &processor,
        "FUNCTION RESTORE abc APPEND",
        b"-ERR DUMP payload version or checksum are wrong\r\n",
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"FUNCTION", b"RESTORE", dump_payload.as_slice(), b"APPEND"])
        ),
        b"-ERR Library lib_dump_one already exists\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"FUNCTION", b"RESTORE", dump_payload.as_slice(), b"REPLACE"])
        ),
        b"+OK\r\n"
    );
}

#[test]
fn function_restore_requires_scripting_enabled_and_valid_mode() {
    let disabled_processor =
        RequestProcessor::new_with_string_store_shards_and_scripting(1, false).unwrap();
    assert_command_error(
        &disabled_processor,
        "FUNCTION RESTORE abc",
        b"-ERR scripting is disabled in this server\r\n",
    );

    let enabled_processor =
        RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap();
    assert_command_error(
        &enabled_processor,
        "FUNCTION RESTORE abc BOGUS",
        b"-ERR syntax error\r\n",
    );
}

#[test]
fn fcall_executes_write_function_when_scripting_is_enabled() {
    let processor = RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap();
    let library_source = b"#!lua name=lib_mut\nredis.register_function{function_name='rw_set', callback=function(keys, args) return redis.call('SET', keys[1], args[1]) end}";
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"FUNCTION", b"LOAD", library_source])
        ),
        b"$7\r\nlib_mut\r\n"
    );
    assert_command_response(
        &processor,
        "FCALL rw_set 1 fcall:key fcall:value",
        b"+OK\r\n",
    );
    assert_command_response(&processor, "GET fcall:key", b"$11\r\nfcall:value\r\n");
}

#[test]
fn function_list_includes_description_field_from_named_descriptor() {
    let processor = RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap();
    let library_source = b"#!lua name=lib_desc\nredis.register_function{function_name='f1', description='some desc', callback=function(keys, args) return 'ok' end}";
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"FUNCTION", b"LOAD", library_source])
        ),
        b"$8\r\nlib_desc\r\n"
    );
    let list_response = execute_frame(&processor, &encode_resp(&[b"FUNCTION", b"LIST"]));
    let list_text = String::from_utf8_lossy(&list_response);
    assert!(list_text.contains("description"));
    assert!(list_text.contains("some desc"));
}

#[test]
fn function_list_supports_libraryname_filter_and_argument_errors() {
    let processor = RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap();
    let lib_one =
        b"#!lua name=library1\nredis.register_function('f6', function(keys, args) return 7 end)";
    let lib_two =
        b"#!lua name=lib1\nredis.register_function('f7', function(keys, args) return 8 end)";

    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"FUNCTION", b"LOAD", lib_one])),
        b"$8\r\nlibrary1\r\n"
    );
    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"FUNCTION", b"LOAD", lib_two])),
        b"$4\r\nlib1\r\n"
    );

    let filtered = execute_frame(
        &processor,
        &encode_resp(&[b"FUNCTION", b"LIST", b"LIBRARYNAME", b"library*"]),
    );
    let filtered_text = String::from_utf8_lossy(&filtered);
    assert!(filtered_text.contains("library1"));
    assert!(!filtered_text.contains("lib1"));

    assert_command_response(
        &processor,
        "FUNCTION LIST bad_argument",
        b"-ERR Unknown argument bad_argument\r\n",
    );
    assert_command_response(
        &processor,
        "FUNCTION LIST libraryname",
        b"-ERR library name argument was not given\r\n",
    );
    assert_command_response(
        &processor,
        "FUNCTION LIST withcode withcode",
        b"-ERR Unknown argument withcode\r\n",
    );
    assert_command_response(
        &processor,
        "FUNCTION LIST withcode libraryname foo libraryname foo",
        b"-ERR Unknown argument libraryname\r\n",
    );
}

#[test]
fn function_load_metadata_validation_matches_redis_messages() {
    let processor = RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap();
    let body = b"redis.register_function('foo', function() return 1 end)";

    let empty_engine = [b"#! name=test\n".as_slice(), body].concat();
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"FUNCTION", b"LOAD", b"REPLACE", empty_engine.as_slice()])
        ),
        b"-ERR Engine '' not found\r\n"
    );

    let unknown_metadata = [b"#!lua name=test foo=bar\n".as_slice(), body].concat();
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[
                b"FUNCTION",
                b"LOAD",
                b"REPLACE",
                unknown_metadata.as_slice()
            ])
        ),
        b"-ERR Invalid metadata value given: foo=bar\r\n"
    );

    let no_name = [b"#!lua\n".as_slice(), body].concat();
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"FUNCTION", b"LOAD", b"REPLACE", no_name.as_slice()])
        ),
        b"-ERR Library name was not given\r\n"
    );

    let duplicate_name = [b"#!lua name=foo name=bar\n".as_slice(), body].concat();
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"FUNCTION", b"LOAD", b"REPLACE", duplicate_name.as_slice()])
        ),
        b"-ERR Invalid metadata value, name argument was given multiple times\r\n"
    );

    let quoted_name = [b"#!lua name=\"foo\"\n".as_slice(), body].concat();
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"FUNCTION", b"LOAD", b"REPLACE", quoted_name.as_slice()])
        ),
        b"$3\r\nfoo\r\n"
    );
}

#[test]
fn function_load_compile_error_prefix_and_replace_keeps_previous_library() {
    let processor = RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap();
    let valid_source =
        b"#!lua name=test\nredis.register_function('test', function(keys, args) return 'hello1' end)";
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"FUNCTION", b"LOAD", valid_source])
        ),
        b"$4\r\ntest\r\n"
    );
    assert_command_response(&processor, "FCALL test 0", b"$6\r\nhello1\r\n");

    let invalid_source =
        b"#!lua name=test\nredis.register_function('test', function(keys, args) bad script end)";
    let error_response = execute_frame(
        &processor,
        &encode_resp(&[b"FUNCTION", b"LOAD", b"REPLACE", invalid_source]),
    );
    let error_text = String::from_utf8_lossy(&error_response);
    assert!(error_text.starts_with("-ERR Error compiling function:"));

    assert_command_response(&processor, "FCALL test 0", b"$6\r\nhello1\r\n");
}

#[test]
fn function_runtime_version_api_and_global_protection_match_expected_behavior() {
    let processor = RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap();

    let version_library = b"#!lua name=test\nlocal version = redis.REDIS_VERSION_NUM\nredis.register_function{function_name='get_version_v1', callback=function() return string.format('%s.%s.%s', bit.band(bit.rshift(version, 16), 0x000000ff), bit.band(bit.rshift(version, 8), 0x000000ff), bit.band(version, 0x000000ff)) end}\nredis.register_function{function_name='get_version_v2', callback=function() return redis.REDIS_VERSION end}";
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"FUNCTION", b"LOAD", b"REPLACE", version_library])
        ),
        b"$4\r\ntest\r\n"
    );
    let version_v1 = parse_bulk_payload(&execute_frame(
        &processor,
        &encode_resp(&[b"FCALL", b"get_version_v1", b"0"]),
    ))
    .expect("get_version_v1 should return bulk string");
    let version_v2 = parse_bulk_payload(&execute_frame(
        &processor,
        &encode_resp(&[b"FCALL", b"get_version_v2", b"0"]),
    ))
    .expect("get_version_v2 should return bulk string");
    assert_eq!(version_v1, version_v2);

    let protected_library = b"#!lua name=test1\nredis.register_function('f1', function() mt = getmetatable(_G) original_globals = mt.__index original_globals['redis'] = function() return 1 end end)";
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"FUNCTION", b"LOAD", protected_library])
        ),
        b"$5\r\ntest1\r\n"
    );
    let protected_error = execute_frame(&processor, &encode_resp(&[b"FCALL", b"f1", b"0"]));
    let protected_error_text = String::from_utf8_lossy(&protected_error);
    assert!(protected_error_text.contains("Attempt to modify a readonly table"));
}

#[test]
fn function_oom_behavior_respects_allow_oom_and_blocks_load_restore() {
    let processor = RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap();

    let base_library =
        b"#!lua name=test\nredis.register_function('f1', function() return redis.call('SET', 'x', '1') end)";
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"FUNCTION", b"LOAD", b"REPLACE", base_library])
        ),
        b"$4\r\ntest\r\n"
    );
    let payload = parse_bulk_payload(&execute_frame(
        &processor,
        &encode_resp(&[b"FUNCTION", b"DUMP"]),
    ))
    .expect("FUNCTION DUMP should return payload");

    assert_command_response(&processor, "CONFIG SET maxmemory 1", b"+OK\r\n");
    assert_command_response(&processor, "FUNCTION FLUSH", b"+OK\r\n");
    let blocked_load = b"#!lua name=test\nredis.register_function('f1', function() return 1 end)";
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"FUNCTION", b"LOAD", b"REPLACE", blocked_load])
        ),
        b"-OOM command not allowed when used memory > 'maxmemory'.\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"FUNCTION", b"RESTORE", payload.as_slice()])
        ),
        b"-OOM command not allowed when used memory > 'maxmemory'.\r\n"
    );

    let deny_oom_library =
        b"#!lua name=test\nredis.register_function('f1', function() return redis.call('SET', 'x', '1') end)";
    assert_command_response(&processor, "CONFIG SET maxmemory 0", b"+OK\r\n");
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"FUNCTION", b"LOAD", b"REPLACE", deny_oom_library])
        ),
        b"$4\r\ntest\r\n"
    );
    assert_command_response(&processor, "CONFIG SET maxmemory 1", b"+OK\r\n");
    let deny_oom_response = execute_frame(&processor, &encode_resp(&[b"FCALL", b"f1", b"1", b"x"]));
    let deny_oom_text = String::from_utf8_lossy(&deny_oom_response);
    assert!(deny_oom_text.contains("OOM command not allowed when used memory > 'maxmemory'."));

    assert_command_response(&processor, "CONFIG SET maxmemory 0", b"+OK\r\n");
    assert_command_response(&processor, "FUNCTION FLUSH", b"+OK\r\n");
    let allow_oom_library = b"#!lua name=f1\nredis.register_function{function_name='f1', callback=function() return redis.call('SET', 'x', '1') end, flags={'allow-oom'}}";
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"FUNCTION", b"LOAD", b"REPLACE", allow_oom_library])
        ),
        b"$2\r\nf1\r\n"
    );
    assert_command_response(&processor, "CONFIG SET maxmemory 1", b"+OK\r\n");
    assert_command_response(&processor, "FCALL f1 1 x", b"+OK\r\n");
    assert_command_response(&processor, "GET x", b"$1\r\n1\r\n");
    assert_command_response(&processor, "CONFIG SET maxmemory 0", b"+OK\r\n");
}

#[test]
fn function_flush_async_updates_vm_memory_and_lazyfree_info_metrics() {
    let processor = RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap();
    let lib1 = b"#!lua name=lib_lf1\nredis.register_function{function_name='f1', callback=function(keys, args) return 1 end}";
    let lib2 = b"#!lua name=lib_lf2\nredis.register_function{function_name='f2', callback=function(keys, args) return 2 end}";

    let loaded_1 = parse_bulk_payload(&execute_frame(
        &processor,
        &encode_resp(&[b"FUNCTION", b"LOAD", lib1]),
    ))
    .expect("FUNCTION LOAD should return library name");
    assert_eq!(loaded_1, b"lib_lf1");
    let loaded_2 = parse_bulk_payload(&execute_frame(
        &processor,
        &encode_resp(&[b"FUNCTION", b"LOAD", lib2]),
    ))
    .expect("FUNCTION LOAD should return library name");
    assert_eq!(loaded_2, b"lib_lf2");

    let info_before = parse_bulk_payload(&execute_frame(&processor, &encode_resp(&[b"INFO"])))
        .expect("INFO returns bulk payload");
    let info_before_text = String::from_utf8_lossy(&info_before);
    assert!(info_before_text.contains("used_memory_vm_functions:"));
    assert!(
        !info_before_text.contains("used_memory_vm_functions:0\r\n"),
        "used_memory_vm_functions should be non-zero after loading libraries: {}",
        info_before_text
    );

    assert_command_response(&processor, "CONFIG RESETSTAT", b"+OK\r\n");
    assert_command_response(&processor, "FUNCTION FLUSH ASYNC", b"+OK\r\n");

    let info_after = parse_bulk_payload(&execute_frame(&processor, &encode_resp(&[b"INFO"])))
        .expect("INFO returns bulk payload");
    let info_after_text = String::from_utf8_lossy(&info_after);
    assert!(info_after_text.contains("used_memory_vm_functions:0"));
    assert!(info_after_text.contains("lazyfreed_objects:3"));
}

#[test]
fn command_getkeys_supports_fcall_and_fcall_ro() {
    let processor = RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap();
    assert_command_response(
        &processor,
        "COMMAND GETKEYS FCALL fn 2 key1 key2 arg1",
        b"*2\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n",
    );
    assert_command_response(
        &processor,
        "COMMAND GETKEYS FCALL_RO fn 1 keyA arg1",
        b"*1\r\n$4\r\nkeyA\r\n",
    );
}

#[test]
fn scripting_kill_and_busy_semantics_cover_function_and_eval_paths() {
    let processor = Arc::new(
        RequestProcessor::new_with_string_store_shards_and_scripting(1, true)
            .expect("processor should initialize"),
    );
    let function_library = "FUNCTION LOAD REPLACE \"#!lua name=spinlib\nredis.register_function{function_name='spin', callback=function(keys, args) local a = 1 while true do a = a + 1 end end}\"";
    assert_command_response(processor.as_ref(), function_library, b"$7\r\nspinlib\r\n");

    let (function_tx, function_rx) = std::sync::mpsc::channel();
    let function_worker = Arc::clone(&processor);
    thread::spawn(move || {
        let response = match execute_command_line(function_worker.as_ref(), "FCALL spin 0") {
            Ok(response) => response,
            Err(CommandHarnessError::Request(error)) => {
                let mut encoded = Vec::new();
                error.append_resp_error(&mut encoded);
                encoded
            }
            Err(error) => panic!("FCALL failed with non-request error: {error}"),
        };
        function_tx
            .send(response)
            .expect("function response receiver should remain alive");
    });

    let mut saw_busy_ping = false;
    for _ in 0..200 {
        match execute_command_line(processor.as_ref(), "PING") {
            Err(CommandHarnessError::Request(RequestExecutionError::BusyScript)) => {
                saw_busy_ping = true;
                break;
            }
            Ok(response) => {
                assert_eq!(response, b"+PONG\r\n");
            }
            Err(error) => panic!("PING failed with non-request error: {error}"),
        }
        thread::sleep(Duration::from_millis(10));
    }
    assert!(
        saw_busy_ping,
        "PING should return BUSY while a function is running"
    );
    let stats_response = execute_command_line(processor.as_ref(), "FUNCTION STATS")
        .expect("FUNCTION STATS should execute");
    let stats_text = String::from_utf8_lossy(&stats_response);
    assert!(stats_text.contains("running_script"));
    assert!(stats_text.contains("spin"));
    assert!(stats_text.contains("fcall spin 0"));

    assert_command_response(
        processor.as_ref(),
        "SCRIPT KILL",
        b"-BUSY Redis is busy running a script. You can only call FUNCTION KILL or SCRIPT KILL.\r\n",
    );
    assert_command_response(processor.as_ref(), "FUNCTION KILL", b"+OK\r\n");
    let function_result = function_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("function call should terminate after FUNCTION KILL");
    assert!(
        function_result.starts_with(b"-ERR"),
        "killed function should return an error frame: {}",
        String::from_utf8_lossy(&function_result)
    );
    assert_command_response(processor.as_ref(), "PING", b"+PONG\r\n");

    let (script_tx, script_rx) = std::sync::mpsc::channel();
    let script_worker = Arc::clone(&processor);
    thread::spawn(move || {
        let response = match execute_command_line(
            script_worker.as_ref(),
            "EVAL \"local a = 1 while true do a = a + 1 end\" 0",
        ) {
            Ok(response) => response,
            Err(CommandHarnessError::Request(error)) => {
                let mut encoded = Vec::new();
                error.append_resp_error(&mut encoded);
                encoded
            }
            Err(error) => panic!("EVAL failed with non-request error: {error}"),
        };
        script_tx
            .send(response)
            .expect("script response receiver should remain alive");
    });

    let mut saw_busy_ping_again = false;
    for _ in 0..200 {
        match execute_command_line(processor.as_ref(), "PING") {
            Err(CommandHarnessError::Request(RequestExecutionError::BusyScript)) => {
                saw_busy_ping_again = true;
                break;
            }
            Ok(response) => {
                assert_eq!(response, b"+PONG\r\n");
            }
            Err(error) => panic!("PING failed with non-request error: {error}"),
        }
        thread::sleep(Duration::from_millis(10));
    }
    assert!(
        saw_busy_ping_again,
        "PING should return BUSY while an eval script is running"
    );

    assert_command_response(
        processor.as_ref(),
        "FUNCTION KILL",
        b"-BUSY Redis is busy running a script. You can only call FUNCTION KILL or SCRIPT KILL.\r\n",
    );
    assert_command_response(processor.as_ref(), "SCRIPT KILL", b"+OK\r\n");
    let script_result = script_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("eval should terminate after SCRIPT KILL");
    assert!(
        script_result.starts_with(b"-ERR"),
        "killed eval should return an error frame: {}",
        String::from_utf8_lossy(&script_result)
    );
    assert_command_response(processor.as_ref(), "PING", b"+PONG\r\n");
}

#[test]
fn fcall_and_function_load_gating_behavior() {
    let processor = RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap();
    assert_command_error(&processor, "FCALL fn 0", b"-ERR Function not found\r\n");

    let processor_disabled =
        RequestProcessor::new_with_string_store_shards_and_scripting(1, false).unwrap();
    assert_command_error(
        &processor_disabled,
        "FCALL fn 0",
        b"-ERR scripting is disabled in this server\r\n",
    );
    assert_command_error(
        &processor_disabled,
        "FUNCTION LOAD \"#!lua name=lib redis.register_function('f', function(keys, args) return 1 end)\"",
        b"-ERR scripting is disabled in this server\r\n",
    );
}

#[test]
fn scripting_runtime_rejects_oversized_script_payloads() {
    let processor = RequestProcessor::new_with_string_store_shards_scripting_and_runtime(
        1,
        true,
        Some(8),
        None,
        None,
        None,
    )
    .unwrap();
    assert_command_error(
        &processor,
        "EVAL \"return 'too-long'\" 0",
        b"-ERR script is larger than the configured max script size\r\n",
    );
    assert_command_error(
        &processor,
        "SCRIPT LOAD \"return 'too-long'\"",
        b"-ERR script is larger than the configured max script size\r\n",
    );
}

#[test]
fn scripting_runtime_applies_cache_eviction_and_exposes_info_metrics() {
    let processor = RequestProcessor::new_with_string_store_shards_scripting_and_runtime(
        1,
        true,
        None,
        Some(2),
        Some(2048),
        None,
    )
    .unwrap();

    let sha1 = parse_bulk_payload(&execute_frame(
        &processor,
        &encode_resp(&[b"SCRIPT", b"LOAD", b"return 1"]),
    ))
    .unwrap();
    let sha2 = parse_bulk_payload(&execute_frame(
        &processor,
        &encode_resp(&[b"SCRIPT", b"LOAD", b"return 2"]),
    ))
    .unwrap();
    let sha3 = parse_bulk_payload(&execute_frame(
        &processor,
        &encode_resp(&[b"SCRIPT", b"LOAD", b"return 3"]),
    ))
    .unwrap();

    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"SCRIPT", b"EXISTS", &sha1, &sha2, &sha3]),
        ),
        b"*3\r\n:0\r\n:1\r\n:1\r\n"
    );

    let info_payload = parse_bulk_payload(&execute_frame(&processor, &encode_resp(&[b"INFO"])))
        .expect("INFO returns bulk payload");
    let info_text = String::from_utf8_lossy(&info_payload);
    assert!(info_text.contains("scripting_cache_entries:2"));
    assert!(info_text.contains("scripting_cache_evictions:1"));
    assert!(info_text.contains("scripting_cache_hits:2"));
    assert!(info_text.contains("scripting_cache_misses:1"));
    assert!(info_text.contains("scripting_cache_max_entries:2"));
    assert!(info_text.contains("scripting_max_memory_bytes:2048"));
}

#[test]
fn scripting_runtime_times_out_long_running_script() {
    let processor = RequestProcessor::new_with_string_store_shards_scripting_and_runtime(
        1,
        true,
        None,
        None,
        None,
        Some(1),
    )
    .unwrap();

    assert_command_response(
        &processor,
        "EVAL \"local x = 0 for i=1,100000000 do x = x + i end return x\" 0",
        b"-ERR script execution timed out\r\n",
    );

    let info_payload = parse_bulk_payload(&execute_frame(&processor, &encode_resp(&[b"INFO"])))
        .expect("INFO returns bulk payload");
    let info_text = String::from_utf8_lossy(&info_payload);
    assert!(info_text.contains("scripting_runtime_timeouts:1"));
    assert!(info_text.contains("scripting_max_execution_millis:1"));
}

#[test]
fn config_resetstat_returns_ok() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 8];
    let mut response = Vec::new();

    assert_command_response(&processor, "DEBUG SET-ACTIVE-EXPIRE 0", b"+OK\r\n");
    assert_command_response(&processor, "PSETEX reset:key 1 value", b"+OK\r\n");
    thread::sleep(Duration::from_millis(10));
    assert_command_response(&processor, "GET reset:key", b"$-1\r\n");

    let info_before = parse_bulk_payload(&execute_frame(&processor, &encode_resp(&[b"INFO"])))
        .expect("INFO returns bulk payload before reset");
    let info_before_text = String::from_utf8_lossy(&info_before);
    assert!(info_before_text.contains("expired_keys:1"));

    let config_resetstat = b"*2\r\n$6\r\nCONFIG\r\n$9\r\nRESETSTAT\r\n";
    let meta = parse_resp_command_arg_slices(config_resetstat, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    let info_after = parse_bulk_payload(&execute_frame(&processor, &encode_resp(&[b"INFO"])))
        .expect("INFO returns bulk payload after reset");
    let info_after_text = String::from_utf8_lossy(&info_after);
    assert!(info_after_text.contains("expired_keys:0"));
    assert!(info_after_text.contains("expired_keys_active:0"));
}

#[test]
fn config_get_known_and_unknown_keys() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 4];
    let mut response = Vec::new();

    let config_get_appendonly = b"*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$10\r\nappendonly\r\n";
    let meta = parse_resp_command_arg_slices(config_get_appendonly, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"*2\r\n$10\r\nappendonly\r\n$2\r\nno\r\n");

    response.clear();
    let config_get_unknown = b"*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$7\r\nunknown\r\n";
    let meta = parse_resp_command_arg_slices(config_get_unknown, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"*0\r\n");
}

#[test]
fn config_set_port_accepts_current_value_and_rejects_port_changes() {
    let processor = RequestProcessor::new().unwrap();
    processor.set_config_value(b"port", b"6380".to_vec());

    assert_command_response(&processor, "CONFIG SET port 6380", b"+OK\r\n");
    assert_command_response(
        &processor,
        "CONFIG SET port 6381",
        b"-ERR CONFIG SET failed (possibly related to argument 'port') - Unable to listen on this port\r\n",
    );
    assert_command_error(
        &processor,
        "CONFIG SET port not-a-port",
        b"-ERR value is not an integer or out of range\r\n",
    );
}

#[test]
fn config_set_zset_max_ziplist_entries_changes_object_encoding() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 16];
    let mut response = Vec::new();

    let config_set =
        b"*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$24\r\nzset-max-ziplist-entries\r\n$1\r\n0\r\n";
    let meta = parse_resp_command_arg_slices(config_set, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    for member in [b"a", b"b", b"c"] {
        response.clear();
        let zadd = encode_resp(&[b"ZADD", b"zset", b"1", member]);
        let meta = parse_resp_command_arg_slices(&zadd, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert!(response == b":1\r\n" || response == b":0\r\n");
    }

    response.clear();
    let object_encoding = b"*3\r\n$6\r\nOBJECT\r\n$8\r\nENCODING\r\n$4\r\nzset\r\n";
    let meta = parse_resp_command_arg_slices(object_encoding, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"$8\r\nskiplist\r\n");
}

#[test]
fn config_set_list_max_ziplist_size_changes_list_object_encoding() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 16];
    let mut response = Vec::new();

    let config_set_small =
        b"*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$21\r\nlist-max-ziplist-size\r\n$1\r\n5\r\n";
    let meta = parse_resp_command_arg_slices(config_set_small, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    for index in 0..10 {
        response.clear();
        let value = index.to_string();
        let rpush = encode_resp(&[b"RPUSH", b"list", value.as_bytes()]);
        let meta = parse_resp_command_arg_slices(&rpush, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
    }

    response.clear();
    let object_encoding = b"*3\r\n$6\r\nOBJECT\r\n$8\r\nENCODING\r\n$4\r\nlist\r\n";
    let meta = parse_resp_command_arg_slices(object_encoding, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"$9\r\nquicklist\r\n");

    response.clear();
    let config_set_compact =
        b"*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$22\r\nlist-max-listpack-size\r\n$2\r\n-1\r\n";
    let meta = parse_resp_command_arg_slices(config_set_compact, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let meta = parse_resp_command_arg_slices(object_encoding, &mut args).unwrap();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .unwrap();
    assert_eq!(response, b"$8\r\nlistpack\r\n");
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
