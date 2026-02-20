use super::*;
use crate::debug_concurrency;
use crate::testkit::execute_command_line;
use garnet_common::parse_resp_command_arg_slices;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tsavorite::{
    DeleteOperationError, ISessionFunctions, PageManagerError, PageResidencyError,
    ReadOperationError, RecordInfo, RmwOperationError, UpsertOperationError, WriteReason,
};

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
