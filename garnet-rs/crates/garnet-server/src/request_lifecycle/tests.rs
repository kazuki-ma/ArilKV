use super::*;
use crate::debug_concurrency;
use crate::testkit::CommandHarnessError;
use crate::testkit::assert_command_error;
use crate::testkit::assert_command_integer;
use crate::testkit::assert_command_response;
use crate::testkit::assert_command_response_in_db;
use crate::testkit::execute_command_line;
use garnet_cluster::SlotNumber;
use garnet_common::parse_resp_command_arg_slices;
use garnet_common::parse_resp_command_arg_slices_dynamic;
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

fn parse_integer_array_response(response: &[u8]) -> Vec<i64> {
    let mut index = 0usize;
    let array_len = parse_resp_array_len(response, &mut index);
    let mut out = Vec::with_capacity(array_len);
    for _ in 0..array_len {
        assert_eq!(response[index], b':');
        index += 1;
        let start = index;
        while index + 1 < response.len() {
            if response[index] == b'\r' && response[index + 1] == b'\n' {
                break;
            }
            index += 1;
        }
        let value = core::str::from_utf8(&response[start..index])
            .unwrap()
            .parse::<i64>()
            .unwrap();
        out.push(value);
        index += 2;
    }
    assert_eq!(index, response.len());
    out
}

fn parse_resp_integer(response: &[u8], index: &mut usize) -> i64 {
    assert_eq!(response[*index], b':');
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
        .parse::<i64>()
        .unwrap();
    *index += 2;
    value
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

fn parse_nullable_resp_bulk_bytes(response: &[u8], index: &mut usize) -> Option<Vec<u8>> {
    assert_eq!(response[*index], b'$');
    if response.get(*index + 1) == Some(&b'-') {
        *index += 1;
        while *index + 1 < response.len() {
            if response[*index] == b'\r' && response[*index + 1] == b'\n' {
                break;
            }
            *index += 1;
        }
        *index += 2;
        return None;
    }
    Some(parse_resp_bulk_bytes(response, index))
}

fn parse_stream_field_pairs(response: &[u8], index: &mut usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    let field_count = parse_resp_array_len(response, index);
    let mut fields = Vec::with_capacity(field_count / 2);
    for _ in 0..(field_count / 2) {
        let field = parse_resp_bulk_bytes(response, index);
        let value = parse_resp_bulk_bytes(response, index);
        fields.push((field, value));
    }
    fields
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RespTestValue {
    Integer(i64),
    Bulk(Vec<u8>),
    Array(Vec<RespTestValue>),
    Null,
}

fn parse_resp_test_value(response: &[u8]) -> RespTestValue {
    let mut index = 0usize;
    let value = parse_resp_test_value_at(response, &mut index);
    assert_eq!(index, response.len());
    value
}

fn parse_resp_test_value_at(response: &[u8], index: &mut usize) -> RespTestValue {
    match response[*index] {
        b':' => RespTestValue::Integer(parse_resp_integer(response, index)),
        b'$' => match parse_nullable_resp_bulk_bytes(response, index) {
            Some(payload) => RespTestValue::Bulk(payload),
            None => RespTestValue::Null,
        },
        b'*' => {
            let array_len = parse_resp_array_len(response, index);
            let mut items = Vec::with_capacity(array_len);
            for _ in 0..array_len {
                items.push(parse_resp_test_value_at(response, index));
            }
            RespTestValue::Array(items)
        }
        other => panic!("unsupported RESP test token: {}", other as char),
    }
}

fn resp_test_array(value: &RespTestValue) -> &[RespTestValue] {
    match value {
        RespTestValue::Array(items) => items,
        other => panic!("expected RESP array, got {other:?}"),
    }
}

fn resp_test_bulk(value: &RespTestValue) -> &[u8] {
    match value {
        RespTestValue::Bulk(payload) => payload,
        other => panic!("expected RESP bulk string, got {other:?}"),
    }
}

fn resp_test_integer(value: &RespTestValue) -> i64 {
    match value {
        RespTestValue::Integer(number) => *number,
        other => panic!("expected RESP integer, got {other:?}"),
    }
}

fn resp_test_is_null(value: &RespTestValue) -> bool {
    matches!(value, RespTestValue::Null)
}

fn resp_test_flat_map<'a>(
    value: &'a RespTestValue,
) -> std::collections::BTreeMap<Vec<u8>, &'a RespTestValue> {
    let items = resp_test_array(value);
    assert_eq!(items.len() % 2, 0);
    let mut map = std::collections::BTreeMap::new();
    let mut index = 0usize;
    while index < items.len() {
        let key = resp_test_bulk(&items[index]).to_vec();
        map.insert(key, &items[index + 1]);
        index += 2;
    }
    map
}

fn xinfo_full_group_by_name<'a>(
    full_map: &'a std::collections::BTreeMap<Vec<u8>, &'a RespTestValue>,
    group_name: &[u8],
) -> std::collections::BTreeMap<Vec<u8>, &'a RespTestValue> {
    let groups = resp_test_array(full_map[&b"groups".to_vec()]);
    groups
        .iter()
        .map(resp_test_flat_map)
        .find(|group| resp_test_bulk(group[&b"name".to_vec()]) == group_name)
        .unwrap_or_else(|| panic!("missing group {}", String::from_utf8_lossy(group_name)))
}

fn parse_stream_entry_list_at(
    response: &[u8],
    index: &mut usize,
) -> Vec<(Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>)> {
    let entry_count = parse_resp_array_len(response, index);
    let mut entries = Vec::with_capacity(entry_count);
    for _ in 0..entry_count {
        assert_eq!(parse_resp_array_len(response, index), 2);
        let entry_id = parse_resp_bulk_bytes(response, index);
        let fields = parse_stream_field_pairs(response, index);
        entries.push((entry_id, fields));
    }
    entries
}

fn parse_stream_entry_list_response(response: &[u8]) -> Vec<(Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>)> {
    let mut index = 0usize;
    let entries = parse_stream_entry_list_at(response, &mut index);
    assert_eq!(index, response.len());
    entries
}

fn parse_bulk_byte_array_at(response: &[u8], index: &mut usize) -> Vec<Vec<u8>> {
    let item_count = parse_resp_array_len(response, index);
    let mut items = Vec::with_capacity(item_count);
    for _ in 0..item_count {
        items.push(parse_resp_bulk_bytes(response, index));
    }
    items
}

fn parse_xread_single_stream_response(
    response: &[u8],
) -> Option<(Vec<u8>, Vec<(Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>)>)> {
    if response == b"*-1\r\n" || response == b"_\r\n" {
        return None;
    }

    let mut index = 0usize;
    let stream_count = parse_resp_array_len(response, &mut index);
    assert_eq!(stream_count, 1);
    assert_eq!(parse_resp_array_len(response, &mut index), 2);
    let stream_name = parse_resp_bulk_bytes(response, &mut index);
    let entries = parse_stream_entry_list_at(response, &mut index);
    assert_eq!(index, response.len());
    Some((stream_name, entries))
}

fn resp_test_field_pairs(value: &RespTestValue) -> Vec<(Vec<u8>, Vec<u8>)> {
    let items = resp_test_array(value);
    assert!(items.len().is_multiple_of(2));
    let mut pairs = Vec::with_capacity(items.len() / 2);
    let mut index = 0usize;
    while index < items.len() {
        pairs.push((
            resp_test_bulk(&items[index]).to_vec(),
            resp_test_bulk(&items[index + 1]).to_vec(),
        ));
        index += 2;
    }
    pairs
}

fn parse_xreadgroup_maybe_claim_single_stream_response(
    response: &[u8],
) -> Option<(
    Vec<u8>,
    Vec<(Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>, Option<i64>, Option<i64>)>,
)> {
    if response == b"*-1\r\n" || response == b"_\r\n" {
        return None;
    }

    let root = parse_resp_test_value(response);
    let streams = resp_test_array(&root);
    assert_eq!(streams.len(), 1);
    let stream = resp_test_array(&streams[0]);
    assert_eq!(stream.len(), 2);
    let stream_name = resp_test_bulk(&stream[0]).to_vec();
    let messages = resp_test_array(&stream[1]);
    let mut entries = Vec::with_capacity(messages.len());
    for message in messages {
        let parts = resp_test_array(message);
        match parts.len() {
            2 => entries.push((
                resp_test_bulk(&parts[0]).to_vec(),
                resp_test_field_pairs(&parts[1]),
                None,
                None,
            )),
            4 => entries.push((
                resp_test_bulk(&parts[0]).to_vec(),
                resp_test_field_pairs(&parts[1]),
                Some(resp_test_integer(&parts[2])),
                Some(resp_test_integer(&parts[3])),
            )),
            _ => panic!("unexpected XREADGROUP entry shape: {parts:?}"),
        }
    }
    Some((stream_name, entries))
}

fn parse_xautoclaim_entry_response(
    response: &[u8],
) -> (
    Vec<u8>,
    Vec<(Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>)>,
    Vec<Vec<u8>>,
) {
    let mut index = 0usize;
    assert_eq!(parse_resp_array_len(response, &mut index), 3);
    let cursor = parse_resp_bulk_bytes(response, &mut index);
    let claimed_entries = parse_stream_entry_list_at(response, &mut index);
    let deleted_ids = parse_bulk_byte_array_at(response, &mut index);
    assert_eq!(index, response.len());
    (cursor, claimed_entries, deleted_ids)
}

fn parse_xautoclaim_justid_response(response: &[u8]) -> (Vec<u8>, Vec<Vec<u8>>, Vec<Vec<u8>>) {
    let mut index = 0usize;
    assert_eq!(parse_resp_array_len(response, &mut index), 3);
    let cursor = parse_resp_bulk_bytes(response, &mut index);
    let claimed_ids = parse_bulk_byte_array_at(response, &mut index);
    let deleted_ids = parse_bulk_byte_array_at(response, &mut index);
    assert_eq!(index, response.len());
    (cursor, claimed_ids, deleted_ids)
}

fn parse_xpending_detail_response(response: &[u8]) -> Vec<(Vec<u8>, Vec<u8>, i64, i64)> {
    let mut index = 0usize;
    let pending_count = parse_resp_array_len(response, &mut index);
    let mut pending = Vec::with_capacity(pending_count);
    for _ in 0..pending_count {
        assert_eq!(parse_resp_array_len(response, &mut index), 4);
        let entry_id = parse_resp_bulk_bytes(response, &mut index);
        let consumer = parse_resp_bulk_bytes(response, &mut index);
        let idle = parse_resp_integer(response, &mut index);
        let deliveries = parse_resp_integer(response, &mut index);
        pending.push((entry_id, consumer, idle, deliveries));
    }
    pending
}

fn parse_xpending_summary_response(
    response: &[u8],
) -> (i64, Option<Vec<u8>>, Option<Vec<u8>>, Vec<(Vec<u8>, i64)>) {
    let mut index = 0usize;
    assert_eq!(parse_resp_array_len(response, &mut index), 4);
    let total = parse_resp_integer(response, &mut index);
    let smallest = parse_nullable_resp_bulk_bytes(response, &mut index);
    let greatest = parse_nullable_resp_bulk_bytes(response, &mut index);
    let consumer_count = parse_resp_array_len(response, &mut index);
    let mut consumers = Vec::with_capacity(consumer_count);
    for _ in 0..consumer_count {
        assert_eq!(parse_resp_array_len(response, &mut index), 2);
        let name = parse_resp_bulk_bytes(response, &mut index);
        let count = parse_resp_integer(response, &mut index);
        consumers.push((name, count));
    }
    assert_eq!(index, response.len());
    (total, smallest, greatest, consumers)
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    response
}

fn execute_frame_with_client(
    processor: &RequestProcessor,
    frame: &[u8],
    client_id: ClientId,
    client_no_touch: bool,
) -> Vec<u8> {
    let mut args = [ArgSlice::EMPTY; 16];
    let meta = parse_resp_command_arg_slices(frame, &mut args).unwrap();
    let mut response = Vec::new();
    processor
        .execute_with_client_context_in_db(
            &args[..meta.argument_count],
            &mut response,
            client_no_touch,
            Some(client_id),
            false,
            DbName::default(),
        )
        .unwrap();
    response
}

fn execute_command_line_with_client(
    processor: &RequestProcessor,
    command_line: &str,
    client_id: ClientId,
) -> Vec<u8> {
    let parts = command_line
        .split_whitespace()
        .map(str::as_bytes)
        .collect::<Vec<_>>();
    let frame = encode_resp(&parts);
    execute_frame_with_client(processor, &frame, client_id, false)
}

fn execute_command_line_in_db(
    processor: &RequestProcessor,
    command_line: &str,
    selected_db: DbName,
) -> Vec<u8> {
    let parts = command_line
        .split_whitespace()
        .map(str::as_bytes)
        .collect::<Vec<_>>();
    execute_command_args_in_db(processor, &parts, selected_db)
}

fn execute_command_args_in_db(
    processor: &RequestProcessor,
    parts: &[&[u8]],
    selected_db: DbName,
) -> Vec<u8> {
    let frame = encode_resp(&parts);
    let mut args = Vec::new();
    let meta = parse_resp_command_arg_slices_dynamic(&frame, &mut args, usize::MAX).unwrap();
    let mut response = Vec::new();
    processor
        .execute_with_client_context_in_db(
            &args[..meta.argument_count],
            &mut response,
            false,
            None,
            false,
            selected_db,
        )
        .unwrap();
    response
}

fn assert_client_command_response(
    processor: &RequestProcessor,
    command_line: &str,
    client_id: ClientId,
    expected: &[u8],
) {
    let actual = execute_command_line_with_client(processor, command_line, client_id);
    assert_eq!(
        actual, expected,
        "unexpected response for {command_line:?} on client {client_id}"
    );
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
        if processor.string_store_shard_index_for_key(&candidate) == ShardIndex::new(shard) {
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

    info.user_data = SessionUserData::empty();
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    let frame_get = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(frame_get, &mut args).unwrap();
    response.clear();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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

    assert!(
        processor
            .string_expiration_deadline(DbKeyRef::new(DbName::default(), &key_a))
            .is_some()
    );
    assert!(
        processor
            .string_expiration_deadline(DbKeyRef::new(DbName::default(), &key_b))
            .is_none()
    );
    assert_eq!(processor.string_expiration_count_for_shard(shard_a), 1);
    assert_eq!(processor.string_expiration_count_for_shard(shard_b), 0);

    assert!(
        processor.db_catalog.main_db_runtime.string_key_registries[shard_a]
            .lock()
            .expect("key registry mutex poisoned")
            .contains(key_a.as_slice())
    );
    assert!(
        processor.db_catalog.main_db_runtime.string_key_registries[shard_b]
            .lock()
            .expect("key registry mutex poisoned")
            .contains(key_b.as_slice())
    );
    assert!(
        !processor.db_catalog.main_db_runtime.string_key_registries[shard_a]
            .lock()
            .expect("key registry mutex poisoned")
            .contains(key_b.as_slice())
    );
    assert!(
        !processor.db_catalog.main_db_runtime.string_key_registries[shard_b]
            .lock()
            .expect("key registry mutex poisoned")
            .contains(key_a.as_slice())
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    let frame_get = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(frame_get, &mut args).unwrap();
    response.clear();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    let mut frame_get = format!("*2\r\n$3\r\nGET\r\n${}\r\n", key.len()).into_bytes();
    frame_get.extend_from_slice(&key);
    frame_get.extend_from_slice(b"\r\n");

    let meta = parse_resp_command_arg_slices(&frame_get, &mut args).unwrap();
    response.clear();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$1\r\n1\r\n");
}

#[test]
fn object_store_roundtrip_respects_redis_type_semantics() {
    let processor = RequestProcessor::new().unwrap();

    processor
        .object_upsert(
            DbKeyRef::new(current_request_selected_db(), b"obj"),
            ObjectTypeTag::Hash,
            b"payload",
        )
        .unwrap();
    let object = processor
        .object_read(DbKeyRef::new(current_request_selected_db(), b"obj"))
        .unwrap()
        .unwrap();
    assert_eq!(object.object_type, ObjectTypeTag::Hash);
    assert_eq!(object.payload, b"payload");

    let mut args = [ArgSlice::EMPTY; 3];
    let mut response = Vec::new();
    let get = b"*2\r\n$3\r\nGET\r\n$3\r\nobj\r\n";
    let meta = parse_resp_command_arg_slices(get, &mut args).unwrap();
    let err = processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    assert!(
        processor
            .object_read(DbKeyRef::new(current_request_selected_db(), b"obj"))
            .unwrap()
            .is_none()
    );

    response.clear();
    let meta = parse_resp_command_arg_slices(get, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    let entry = source
        .export_migration_entry(DbName::default(), b"key")
        .unwrap()
        .expect("source key should be exportable");
    assert!(matches!(&entry.value, MigrationValue::String(value) if value.as_slice() == b"value"));
    assert!(entry.expiration_unix_millis.is_some());

    target
        .import_migration_entry(DbName::default(), &entry)
        .unwrap();

    response.clear();
    let get = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(get, &mut args).unwrap();
    target
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$5\r\nvalue\r\n");

    response.clear();
    let pttl = b"*2\r\n$4\r\nPTTL\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(pttl, &mut args).unwrap();
    target
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    let moved = source
        .migrate_keys_to(
            &target,
            DbName::default(),
            DbName::default(),
            &[b"key".to_vec()],
            true,
        )
        .unwrap();
    assert_eq!(moved, 1);

    response.clear();
    let get = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(get, &mut args).unwrap();
    source
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$-1\r\n");

    response.clear();
    let meta = parse_resp_command_arg_slices(get, &mut args).unwrap();
    target
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$5\r\nvalue\r\n");
}

#[test]
fn migration_entry_roundtrip_preserves_string_and_expiration_in_nonzero_db() {
    let source = RequestProcessor::new().unwrap();
    let target = RequestProcessor::new().unwrap();
    let source_db = DbName::new(9);
    let target_db = DbName::new(11);
    let mut args = [ArgSlice::EMPTY; 8];
    let mut response = Vec::new();

    let set = b"*5\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\nPX\r\n$4\r\n1000\r\n";
    let meta = parse_resp_command_arg_slices(set, &mut args).unwrap();
    source
        .execute_in_db(&args[..meta.argument_count], &mut response, source_db)
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    let entry = source
        .export_migration_entry(source_db, b"key")
        .unwrap()
        .expect("source key should be exportable");
    assert!(matches!(&entry.value, MigrationValue::String(value) if value.as_slice() == b"value"));
    assert!(entry.expiration_unix_millis.is_some());

    target.import_migration_entry(target_db, &entry).unwrap();

    response.clear();
    let get = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(get, &mut args).unwrap();
    target
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$-1\r\n");

    response.clear();
    let meta = parse_resp_command_arg_slices(get, &mut args).unwrap();
    target
        .execute_in_db(&args[..meta.argument_count], &mut response, target_db)
        .unwrap();
    assert_eq!(response, b"$5\r\nvalue\r\n");
    assert!(
        target
            .expiration_unix_millis(DbKeyRef::new(target_db, b"key"))
            .is_some()
    );
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    let moved = source
        .migrate_keys_to(
            &target,
            DbName::default(),
            DbName::default(),
            &[b"key".to_vec()],
            true,
        )
        .unwrap();
    assert_eq!(moved, 1);

    response.clear();
    let hget = b"*3\r\n$4\r\nHGET\r\n$3\r\nkey\r\n$5\r\nfield\r\n";
    let meta = parse_resp_command_arg_slices(hget, &mut args).unwrap();
    source
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$-1\r\n");

    response.clear();
    let meta = parse_resp_command_arg_slices(hget, &mut args).unwrap();
    target
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let hset_object = encode_resp(&[b"HSET", &object_key, b"field", b"value-b"]);
    let meta = parse_resp_command_arg_slices(&hset_object, &mut args).unwrap();
    source
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let set_other = encode_resp(&[b"SET", &other_key, b"value-c"]);
    let meta = parse_resp_command_arg_slices(&set_other, &mut args).unwrap();
    source
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    let moved = source.migrate_slot_to(&target, slot, 16, true).unwrap();
    assert_eq!(moved, 2);

    response.clear();
    let get_string = encode_resp(&[b"GET", &string_key]);
    let meta = parse_resp_command_arg_slices(&get_string, &mut args).unwrap();
    source
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$-1\r\n");

    response.clear();
    let get_other = encode_resp(&[b"GET", &other_key]);
    let meta = parse_resp_command_arg_slices(&get_other, &mut args).unwrap();
    source
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$7\r\nvalue-c\r\n");

    response.clear();
    let hget_object = encode_resp(&[b"HGET", &object_key, b"field"]);
    let meta = parse_resp_command_arg_slices(&hget_object, &mut args).unwrap();
    source
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$-1\r\n");

    response.clear();
    let meta = parse_resp_command_arg_slices(&get_string, &mut args).unwrap();
    target
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$7\r\nvalue-a\r\n");

    response.clear();
    let meta = parse_resp_command_arg_slices(&hget_object, &mut args).unwrap();
    target
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$7\r\nvalue-b\r\n");

    response.clear();
    let meta = parse_resp_command_arg_slices(&get_other, &mut args).unwrap();
    target
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let hset2 = b"*4\r\n$4\r\nHSET\r\n$3\r\nkey\r\n$6\r\nfield1\r\n$2\r\nv2\r\n";
    let meta = parse_resp_command_arg_slices(hset2, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let hget = b"*3\r\n$4\r\nHGET\r\n$3\r\nkey\r\n$6\r\nfield1\r\n";
    let meta = parse_resp_command_arg_slices(hget, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$2\r\nv2\r\n");

    response.clear();
    let hset3 = b"*4\r\n$4\r\nHSET\r\n$3\r\nkey\r\n$6\r\nfield2\r\n$2\r\nv3\r\n";
    let meta = parse_resp_command_arg_slices(hset3, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let hgetall = b"*2\r\n$7\r\nHGETALL\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(hgetall, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(
        response,
        b"*4\r\n$6\r\nfield1\r\n$2\r\nv2\r\n$6\r\nfield2\r\n$2\r\nv3\r\n"
    );

    response.clear();
    let hdel = b"*4\r\n$4\r\nHDEL\r\n$3\r\nkey\r\n$6\r\nfield1\r\n$6\r\nfield9\r\n";
    let meta = parse_resp_command_arg_slices(hdel, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let hdel_last = b"*3\r\n$4\r\nHDEL\r\n$3\r\nkey\r\n$6\r\nfield2\r\n";
    let meta = parse_resp_command_arg_slices(hdel_last, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let meta = parse_resp_command_arg_slices(hgetall, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"*0\r\n");
}

#[test]
fn hash_batch_mutations_with_duplicate_fields_keep_redis_counts() {
    let processor = RequestProcessor::new().unwrap();

    // Two distinct fields are inserted; duplicate f1 in one command only counts once.
    assert_command_integer(&processor, "HSET hdup f1 v1 f1 v2 f2 v3", 2);
    assert_command_response(&processor, "HGET hdup f1", b"$2\r\nv2\r\n");
    assert_command_response(
        &processor,
        "HGETALL hdup",
        b"*4\r\n$2\r\nf1\r\n$2\r\nv2\r\n$2\r\nf2\r\n$2\r\nv3\r\n",
    );

    // Duplicate deletes in one command also count at most once per existing field.
    assert_command_integer(&processor, "HDEL hdup f1 f1 missing", 1);
    assert_command_response(
        &processor,
        "HGETALL hdup",
        b"*2\r\n$2\r\nf2\r\n$2\r\nv3\r\n",
    );

    // Deleting the final field removes the key.
    assert_command_integer(&processor, "HDEL hdup f2 f2", 1);
    assert_command_response(&processor, "HGETALL hdup", b"*0\r\n");
}

#[test]
fn hmset_and_hgetdel_multi_field_paths_preserve_duplicate_and_order_semantics() {
    let processor = RequestProcessor::new().unwrap();

    assert_command_response(&processor, "HMSET hmdup f1 v1 f1 v2 f2 v3", b"+OK\r\n");
    assert_command_response(&processor, "HGET hmdup f1", b"$2\r\nv2\r\n");

    assert_command_response(
        &processor,
        "HGETDEL hmdup FIELDS 4 f1 missing f1 f2",
        b"*4\r\n$2\r\nv2\r\n$-1\r\n$-1\r\n$2\r\nv3\r\n",
    );
    assert_command_response(&processor, "HGETALL hmdup", b"*0\r\n");
}

#[test]
fn hsetex_hgetex_fast_paths_preserve_duplicate_and_immediate_expire_semantics() {
    let processor = RequestProcessor::new().unwrap();

    assert_command_integer(
        &processor,
        "HSETEX hsx PX 60000 FIELDS 3 f1 v1 f1 v2 f2 v3",
        1,
    );
    assert_command_response(&processor, "HGET hsx f1", b"$2\r\nv2\r\n");
    assert_command_response(
        &processor,
        "HGETEX hsx FIELDS 4 f1 missing f1 f2",
        b"*4\r\n$2\r\nv2\r\n$-1\r\n$2\r\nv2\r\n$2\r\nv3\r\n",
    );

    // PXAT in the past still returns current values first, then removes matching fields.
    assert_command_integer(&processor, "HSET hsx_past f1 v1 f2 v2", 2);
    assert_command_response(
        &processor,
        "HGETEX hsx_past PXAT 1 FIELDS 3 f1 missing f1",
        b"*3\r\n$2\r\nv1\r\n$-1\r\n$2\r\nv1\r\n",
    );
    assert_command_response(
        &processor,
        "HGETALL hsx_past",
        b"*2\r\n$2\r\nf2\r\n$2\r\nv2\r\n",
    );

    assert_command_integer(&processor, "HSETEX hsx_drop PXAT 1 FIELDS 2 a 1 b 2", 1);
    assert_command_response(&processor, "HGETALL hsx_drop", b"*0\r\n");
}

#[test]
fn hgetdel_input_validation_matches_external_hash_scenario() {
    let processor = RequestProcessor::new().unwrap();

    assert_command_error(
        &processor,
        "HGETDEL",
        b"-ERR wrong number of arguments for 'hgetdel' command\r\n",
    );
    assert_command_error(
        &processor,
        "HGETDEL key1",
        b"-ERR wrong number of arguments for 'hgetdel' command\r\n",
    );
    assert_command_error(
        &processor,
        "HGETDEL key1 FIELDS",
        b"-ERR wrong number of arguments for 'hgetdel' command\r\n",
    );
    assert_command_error(
        &processor,
        "HGETDEL key1 FIELDS 0",
        b"-ERR wrong number of arguments for 'hgetdel' command\r\n",
    );
    assert_command_error(
        &processor,
        "HGETDEL key1 FIELDX",
        b"-ERR wrong number of arguments for 'hgetdel' command\r\n",
    );
    assert_command_error(
        &processor,
        "HGETDEL key1 XFIELDX 1 a",
        b"-ERR Mandatory argument FIELDS is missing or not at the right position\r\n",
    );
    assert_command_error(
        &processor,
        "HGETDEL key1 FIELDS 2 a",
        b"-ERR The `numfields` parameter must match the number of arguments\r\n",
    );
    assert_command_error(
        &processor,
        "HGETDEL key1 FIELDS 2 a b c",
        b"-ERR The `numfields` parameter must match the number of arguments\r\n",
    );
    assert_command_error(
        &processor,
        "HGETDEL key1 FIELDS 0 a",
        b"-ERR Number of fields must be a positive integer\r\n",
    );
    assert_command_error(
        &processor,
        "HGETDEL key1 FIELDS -1 a",
        b"-ERR Number of fields must be a positive integer\r\n",
    );
    assert_command_error(
        &processor,
        "HGETDEL key1 FIELDS b a",
        b"-ERR Number of fields must be a positive integer\r\n",
    );
    assert_command_error(
        &processor,
        "HGETDEL key1 FIELDS 9223372036854775808 a",
        b"-ERR Number of fields must be a positive integer\r\n",
    );
}

#[test]
fn hsetex_hgetex_flexible_argument_parsing_matches_external_hash_field_expire_scenarios() {
    let processor = RequestProcessor::new().unwrap();

    assert_command_integer(&processor, "HSETEX myhash EX 60 FIELDS 2 f1 v1 f2 v2", 1);
    let ttl = parse_integer_array_response(
        &execute_command_line(&processor, "HTTL myhash FIELDS 2 f1 f2").unwrap(),
    );
    assert!((0..=60).contains(&ttl[0]));
    assert!((0..=60).contains(&ttl[1]));

    assert_command_response(&processor, "DEL myhash", b":1\r\n");
    assert_command_integer(&processor, "HSETEX myhash FIELDS 2 f1 v1 f2 v2 EX 60", 1);
    let ttl = parse_integer_array_response(
        &execute_command_line(&processor, "HTTL myhash FIELDS 2 f1 f2").unwrap(),
    );
    assert!((0..=60).contains(&ttl[0]));
    assert!((0..=60).contains(&ttl[1]));

    assert_command_integer(&processor, "HSETEX myhash KEEPTTL FIELDS 1 f2 v22", 1);
    assert_command_response(&processor, "HGET myhash f2", b"$3\r\nv22\r\n");
    assert_ne!(
        parse_integer_array_response(
            &execute_command_line(&processor, "HTTL myhash FIELDS 1 f2").unwrap()
        )[0],
        -1
    );

    assert_command_response(&processor, "DEL myhash", b":1\r\n");
    assert_command_response(&processor, "HSET myhash f1 v1 f2 v2 f3 v3", b":3\r\n");
    assert_command_response(
        &processor,
        "HGETEX myhash EX 60 FIELDS 2 f1 f2",
        b"*2\r\n$2\r\nv1\r\n$2\r\nv2\r\n",
    );
    let ttl = parse_integer_array_response(
        &execute_command_line(&processor, "HTTL myhash FIELDS 2 f1 f2").unwrap(),
    );
    assert!((0..=60).contains(&ttl[0]));
    assert!((0..=60).contains(&ttl[1]));

    assert_command_response(&processor, "DEL myhash", b":1\r\n");
    assert_command_response(&processor, "HSET myhash f1 v1 f2 v2 f3 v3", b":3\r\n");
    assert_command_response(
        &processor,
        "HGETEX myhash FIELDS 2 f1 f2 EX 60",
        b"*2\r\n$2\r\nv1\r\n$2\r\nv2\r\n",
    );
    let ttl = parse_integer_array_response(
        &execute_command_line(&processor, "HTTL myhash FIELDS 2 f1 f2").unwrap(),
    );
    assert!((0..=60).contains(&ttl[0]));
    assert!((0..=60).contains(&ttl[1]));

    assert_command_response(
        &processor,
        "HGETEX myhash FIELDS 1 f3 PERSIST",
        b"*1\r\n$2\r\nv3\r\n",
    );
    assert_eq!(
        parse_integer_array_response(
            &execute_command_line(&processor, "HTTL myhash FIELDS 1 f3").unwrap()
        )[0],
        -1
    );
}

#[test]
fn hsetex_hgetex_field_count_and_condition_semantics_match_external_scenarios() {
    let processor = RequestProcessor::new().unwrap();

    assert_command_error(
        &processor,
        "HSETEX myhash FIELDS 2 f1 v1",
        b"-ERR wrong number of arguments for 'hsetex' command\r\n",
    );
    assert_command_error(
        &processor,
        "HSETEX myhash FIELDS 1 f1 v1 f2 v2",
        b"-ERR unknown argument\r\n",
    );
    assert_command_error(
        &processor,
        "HSETEX myhash FIELDS 0 f1 v1 EX 60",
        b"-ERR invalid number of fields\r\n",
    );
    assert_command_error(
        &processor,
        "HGETEX myhash FIELDS 2 f1",
        b"-ERR wrong number of arguments for 'hgetex' command\r\n",
    );
    assert_command_error(
        &processor,
        "HGETEX myhash FIELDS 1 f1 f2 f3",
        b"-ERR unknown argument\r\n",
    );
    assert_command_error(
        &processor,
        "HGETEX myhash FIELDS 0 f1 EX 60",
        b"-ERR invalid number of fields\r\n",
    );

    assert_command_response(&processor, "HSET myhash f1 v1", b":1\r\n");
    assert_command_integer(&processor, "HSETEX myhash FXX FIELDS 2 f1 x f2 y", 0);
    assert_command_response(
        &processor,
        "HGETALL myhash",
        b"*2\r\n$2\r\nf1\r\n$2\r\nv1\r\n",
    );

    assert_command_integer(&processor, "HSETEX myhash FNX FIELDS 2 f2 v2 f3 v3", 1);
    assert_command_integer(&processor, "HSETEX myhash FNX FIELDS 2 f1 x f4 y", 0);
    assert_command_response(
        &processor,
        "HGETALL myhash",
        b"*6\r\n$2\r\nf1\r\n$2\r\nv1\r\n$2\r\nf2\r\n$2\r\nv2\r\n$2\r\nf3\r\n$2\r\nv3\r\n",
    );
}

#[test]
fn hash_field_expire_error_surface_matches_external_scenarios() {
    let processor = RequestProcessor::new().unwrap();

    for (encoding_name, max_listpack_entries) in [("listpackex", 512usize), ("hashtable", 0)] {
        assert_command_response(
            &processor,
            &format!("CONFIG SET hash-max-listpack-entries {max_listpack_entries}"),
            b"+OK\r\n",
        );
        let _ = execute_command_line(&processor, "DEL myhash").unwrap();
        assert_command_response(
            &processor,
            "HSET myhash f1 v1 field1 value1 field2 value2 field3 value3",
            b":4\r\n",
        );

        assert_command_error(
            &processor,
            "HEXPIRE myhash 60 FIELDS 0 f1",
            b"-ERR Parameter numFields should be greater than 0\r\n",
        );
        assert_command_error(
            &processor,
            "HEXPIRE myhash 60 FIELDS -1 f1",
            b"-ERR Parameter numFields should be greater than 0\r\n",
        );
        assert_command_error(
            &processor,
            "HSETEX myhash FIELDS 0 f1 v1 EX 60",
            b"-ERR invalid number of fields\r\n",
        );
        assert_command_error(
            &processor,
            "HGETEX myhash FIELDS 0 f1 EX 60",
            b"-ERR invalid number of fields\r\n",
        );
        assert_command_error(
            &processor,
            "HEXPIRE myhash 60 2 f1 f2",
            b"-ERR unknown argument\r\n",
        );
        assert_command_error(
            &processor,
            "HSETEX myhash EX 60 2 f1 v1 f2 v2",
            b"-ERR unknown argument\r\n",
        );
        assert_command_error(
            &processor,
            "HEXPIRE myhash NX FIELDS 1 f1",
            b"-ERR value is not an integer or out of range\r\n",
        );
        assert_command_error(
            &processor,
            "HPEXPIRE myhash FIELDS 1 f1 XX",
            b"-ERR value is not an integer or out of range\r\n",
        );
        assert_command_error(
            &processor,
            "HEXPIRE myhash 60 NX XX FIELDS 1 f1",
            b"-ERR Multiple condition flags specified\r\n",
        );
        assert_command_error(
            &processor,
            "HPEXPIRE myhash 5000 GT LT FIELDS 1 f1",
            b"-ERR Multiple condition flags specified\r\n",
        );
        assert_command_error(
            &processor,
            "HEXPIRE myhash 60 FIELDS 1 f1 NX XX",
            b"-ERR Multiple condition flags specified\r\n",
        );
        assert_command_error(
            &processor,
            "HPEXPIRE myhash 5000 FIELDS 1 f1 FIELDS 1 f2",
            b"-ERR FIELDS keyword specified multiple times\r\n",
        );
        assert_command_error(
            &processor,
            "HPEXPIRE myhash 7200000 FIELDS 3 field1 field2 field3 field4 field5",
            b"-ERR unknown argument\r\n",
        );

        let ttl = parse_integer_array_response(
            &execute_command_line(&processor, "HTTL myhash FIELDS 4 f1 field1 field2 field3")
                .unwrap(),
        );
        assert_eq!(ttl.len(), 4, "{encoding_name}");
        assert_eq!(ttl, vec![-1, -1, -1, -1], "{encoding_name}");
    }
}

#[test]
fn hash_field_expire_complex_and_backward_compatibility_match_external_scenarios() {
    let processor = RequestProcessor::new().unwrap();

    for (encoding_name, max_listpack_entries) in [("listpackex", 512usize), ("hashtable", 0)] {
        assert_command_response(
            &processor,
            &format!("CONFIG SET hash-max-listpack-entries {max_listpack_entries}"),
            b"+OK\r\n",
        );

        let mut hset_command = String::from("HSET myhash");
        for i in 1..=20 {
            hset_command.push_str(&format!(" field{i} value{i}"));
        }
        let _ = execute_command_line(&processor, "DEL myhash").unwrap();
        assert_command_integer(&processor, &hset_command, 20);

        let mut first_ten_fields = String::new();
        for i in 1..=10 {
            first_ten_fields.push_str(&format!(" field{i}"));
        }
        let expire_ten = execute_command_line(
            &processor,
            &format!("HEXPIRE myhash 3600 NX FIELDS 10{first_ten_fields}"),
        )
        .unwrap();
        assert_eq!(
            parse_integer_array_response(&expire_ten),
            vec![1; 10],
            "{encoding_name}",
        );

        let expire_next_five = execute_command_line(
            &processor,
            "HPEXPIRE myhash 3600000 NX FIELDS 5 field11 field12 field13 field14 field15",
        )
        .unwrap();
        assert_eq!(
            parse_integer_array_response(&expire_next_five),
            vec![1; 5],
            "{encoding_name}",
        );
        let refresh_next_five = execute_command_line(
            &processor,
            "HPEXPIRE myhash 7200000 XX FIELDS 5 field11 field12 field13 field14 field15",
        )
        .unwrap();
        assert_eq!(
            parse_integer_array_response(&refresh_next_five),
            vec![1; 5],
            "{encoding_name}",
        );
        let gt_first_three = execute_command_line(
            &processor,
            "HEXPIRE myhash 7200 GT FIELDS 3 field1 field2 field3",
        )
        .unwrap();
        assert_eq!(
            parse_integer_array_response(&gt_first_three),
            vec![1; 3],
            "{encoding_name}",
        );

        assert_command_error(
            &processor,
            "HEXPIRE myhash 3600 FIELDS 15 field1 field2 field3",
            b"-ERR wrong number of arguments for 'hexpire' command\r\n",
        );
        assert_command_error(
            &processor,
            "HPEXPIRE myhash 7200000 FIELDS 3 field1 field2 field3 field4 field5",
            b"-ERR unknown argument\r\n",
        );

        let mut first_fifteen_fields = String::new();
        for i in 1..=15 {
            first_fifteen_fields.push_str(&format!(" field{i}"));
        }
        let ttl_result = parse_integer_array_response(
            &execute_command_line(
                &processor,
                &format!("HTTL myhash FIELDS 15{first_fifteen_fields}"),
            )
            .unwrap(),
        );
        assert_eq!(ttl_result.len(), 15, "{encoding_name}");
        assert!(ttl_result.iter().all(|ttl| *ttl > 0), "{encoding_name}");

        assert_command_response(&processor, "DEL myhash", b":1\r\n");
        assert_command_response(&processor, "HSET myhash f1 v1 f2 v2 f3 v3", b":3\r\n");
        assert_eq!(
            parse_integer_array_response(
                &execute_command_line(&processor, "HEXPIRE myhash 60 FIELDS 2 f1 f2").unwrap(),
            ),
            vec![1, 1],
            "{encoding_name}",
        );
        assert_eq!(
            parse_integer_array_response(
                &execute_command_line(&processor, "HPEXPIRE myhash 5000 NX FIELDS 1 f3").unwrap(),
            ),
            vec![1],
            "{encoding_name}",
        );
        let future_seconds = current_unix_time_millis()
            .unwrap()
            .checked_div(1000)
            .unwrap()
            + 300;
        assert_eq!(
            parse_integer_array_response(
                &execute_command_line(
                    &processor,
                    &format!("HEXPIREAT myhash {future_seconds} XX FIELDS 1 f1"),
                )
                .unwrap(),
            ),
            vec![1],
            "{encoding_name}",
        );

        assert_command_response(&processor, "DEL myhash", b":1\r\n");
        assert_command_integer(&processor, "HSETEX myhash EX 60 FIELDS 2 f1 v1 f2 v2", 1);
        assert_command_response(
            &processor,
            "HGETEX myhash PX 5000 FIELDS 2 f1 f2",
            b"*2\r\n$2\r\nv1\r\n$2\r\nv2\r\n",
        );

        assert_command_error(
            &processor,
            "HEXPIRE myhash 60 FIELDS 0 f1",
            b"-ERR Parameter numFields should be greater than 0\r\n",
        );
        assert_command_error(
            &processor,
            "HEXPIRE myhash 60 2 f1 f2",
            b"-ERR unknown argument\r\n",
        );
    }
}

#[test]
fn hash_object_encoding_and_allow_access_expired_match_external_scenarios() {
    let processor = RequestProcessor::new().unwrap();

    assert_command_response(
        &processor,
        "CONFIG SET hash-max-ziplist-value 8",
        b"+OK\r\n",
    );
    assert_command_response(
        &processor,
        "CONFIG GET hash-max-ziplist-value",
        b"*2\r\n$22\r\nhash-max-ziplist-value\r\n$1\r\n8\r\n",
    );

    let _ = execute_command_line(&processor, "DEL smallhash").unwrap();
    let _ = execute_command_line(&processor, "DEL bighash").unwrap();
    assert_command_response(&processor, "HSET smallhash tmp 0", b":1\r\n");
    assert_command_response(&processor, "HSET bighash tmp 0", b":1\r\n");
    assert_command_response(
        &processor,
        "HINCRBYFLOAT smallhash tmp 0.000005",
        b"$8\r\n0.000005\r\n",
    );
    assert_command_response(
        &processor,
        "HINCRBYFLOAT bighash tmp 0.0000005",
        b"$9\r\n0.0000005\r\n",
    );
    assert_command_response(
        &processor,
        "OBJECT ENCODING smallhash",
        b"$8\r\nlistpack\r\n",
    );
    assert_command_response(
        &processor,
        "OBJECT ENCODING bighash",
        b"$9\r\nhashtable\r\n",
    );

    let _ = execute_command_line(&processor, "DEL smallhash").unwrap();
    let _ = execute_command_line(&processor, "DEL bighash").unwrap();
    assert_command_response(
        &processor,
        "HINCRBYFLOAT smallhash abcdefgh 1",
        b"$1\r\n1\r\n",
    );
    assert_command_response(
        &processor,
        "HINCRBYFLOAT bighash abcdefghi 1",
        b"$1\r\n1\r\n",
    );
    assert_command_response(
        &processor,
        "OBJECT ENCODING smallhash",
        b"$8\r\nlistpack\r\n",
    );
    assert_command_response(
        &processor,
        "OBJECT ENCODING bighash",
        b"$9\r\nhashtable\r\n",
    );

    assert_command_response(&processor, "DEBUG SET-ALLOW-ACCESS-EXPIRED 1", b"+OK\r\n");
    assert_command_response(&processor, "DEBUG SET-ACTIVE-EXPIRE 0", b"+OK\r\n");
    assert_command_response(&processor, "FLUSHALL", b"+OK\r\n");
    assert_command_response(&processor, "SET key1 value1", b"+OK\r\n");
    assert_command_response(&processor, "PEXPIRE key1 1", b":1\r\n");
    thread::sleep(Duration::from_millis(5));
    assert_command_response(&processor, "KEYS *", b"*1\r\n$4\r\nkey1\r\n");
    assert_command_response(&processor, "DEBUG SET-ALLOW-ACCESS-EXPIRED 0", b"+OK\r\n");
    assert_command_response(&processor, "KEYS *", b"*0\r\n");
    assert_command_response(&processor, "DEBUG SET-ACTIVE-EXPIRE 1", b"+OK\r\n");
}

#[test]
fn hash_hgetall_preserves_payload_order_across_restore_match_external_scenarios() {
    let processor = RequestProcessor::new().unwrap();
    let mut fields = [ArgSlice::EMPTY; 16];

    assert_command_response(
        &processor,
        "CONFIG SET hash-max-ziplist-entries 1000000000",
        b"+OK\r\n",
    );
    assert_command_response(
        &processor,
        "CONFIG SET hash-max-ziplist-value 1000000000",
        b"+OK\r\n",
    );
    let _ = execute_command_line(&processor, "DEL k").unwrap();
    let _ = execute_command_line(&processor, "DEL kk").unwrap();

    let long_31 = vec![b'x'; 31];
    let long_8191 = vec![b'x'; 8191];
    let long_65535 = vec![b'x'; 65535];
    let hset_entries = vec![
        (b"ZIP_INT_8B".as_slice(), b"127".as_slice()),
        (b"ZIP_INT_16B".as_slice(), b"32767".as_slice()),
        (b"ZIP_INT_32B".as_slice(), b"2147483647".as_slice()),
        (b"ZIP_INT_64B".as_slice(), b"9223372036854775808".as_slice()),
        (b"ZIP_INT_IMM_MIN".as_slice(), b"0".as_slice()),
        (b"ZIP_INT_IMM_MAX".as_slice(), b"12".as_slice()),
        (b"ZIP_STR_06B".as_slice(), long_31.as_slice()),
        (b"ZIP_STR_14B".as_slice(), long_8191.as_slice()),
        (b"ZIP_STR_32B".as_slice(), long_65535.as_slice()),
    ];
    for (field, value) in &hset_entries {
        assert_eq!(
            execute_frame(&processor, &encode_resp(&[b"HSET", b"k", field, value])),
            b":1\r\n",
        );
    }

    let expected_filtered = vec![
        b"ZIP_INT_8B".to_vec(),
        b"127".to_vec(),
        b"ZIP_INT_16B".to_vec(),
        b"32767".to_vec(),
        b"ZIP_INT_32B".to_vec(),
        b"2147483647".to_vec(),
        b"ZIP_INT_64B".to_vec(),
        b"9223372036854775808".to_vec(),
        b"ZIP_INT_IMM_MIN".to_vec(),
        b"0".to_vec(),
        b"ZIP_INT_IMM_MAX".to_vec(),
        b"12".to_vec(),
    ];
    let original_hgetall = parse_bulk_array_payloads(&execute_frame(
        &processor,
        &encode_resp(&[b"HGETALL", b"k"]),
    ));
    let filtered_original = original_hgetall
        .chunks_exact(2)
        .filter(|entry| !entry[0].starts_with(b"ZIP_STR_"))
        .flat_map(|entry| entry.iter().cloned())
        .collect::<Vec<_>>();
    assert_eq!(filtered_original, expected_filtered);

    let dump_payload =
        parse_bulk_payload(&execute_frame(&processor, &encode_resp(&[b"DUMP", b"k"]))).unwrap();
    assert_command_response(
        &processor,
        "CONFIG SET hash-max-listpack-entries 2",
        b"+OK\r\n",
    );
    let restore = encode_resp(&[b"RESTORE", b"kk", b"0", dump_payload.as_slice()]);
    let meta = parse_resp_command_arg_slices(&restore, &mut fields).unwrap();
    let mut response = Vec::new();
    processor
        .execute_in_db(
            &fields[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    let restored_hgetall = parse_bulk_array_payloads(&execute_frame(
        &processor,
        &encode_resp(&[b"HGETALL", b"kk"]),
    ));
    assert_eq!(restored_hgetall, original_hgetall);
}

#[test]
fn hgetall_returns_map_in_resp3_and_flat_array_in_resp2() {
    let processor = RequestProcessor::new().unwrap();
    assert_command_response(&processor, "HSET mh f1 v1 f2 v2", b":2\r\n");

    // RESP2: flat array with 2*N elements
    processor.set_resp_protocol_version(RespProtocolVersion::Resp2);
    assert_command_response(
        &processor,
        "HGETALL mh",
        b"*4\r\n$2\r\nf1\r\n$2\r\nv1\r\n$2\r\nf2\r\n$2\r\nv2\r\n",
    );

    // RESP3: map with N entries
    processor.set_resp_protocol_version(RespProtocolVersion::Resp3);
    assert_command_response(
        &processor,
        "HGETALL mh",
        b"%2\r\n$2\r\nf1\r\n$2\r\nv1\r\n$2\r\nf2\r\n$2\r\nv2\r\n",
    );

    // Empty key in RESP3 returns empty map
    assert_command_response(&processor, "HGETALL nonexistent", b"%0\r\n");

    // Reset to RESP2
    processor.set_resp_protocol_version(RespProtocolVersion::Resp2);
    assert_command_response(&processor, "HGETALL nonexistent", b"*0\r\n");
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let hlen = b"*2\r\n$4\r\nHLEN\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(hlen, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":2\r\n");

    response.clear();
    let hmget = b"*4\r\n$5\r\nHMGET\r\n$3\r\nkey\r\n$2\r\nf1\r\n$7\r\nmissing\r\n";
    let meta = parse_resp_command_arg_slices(hmget, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"*2\r\n$2\r\nv1\r\n$-1\r\n");

    response.clear();
    let hsetnx_exists = b"*4\r\n$6\r\nHSETNX\r\n$3\r\nkey\r\n$2\r\nf1\r\n$2\r\nzz\r\n";
    let meta = parse_resp_command_arg_slices(hsetnx_exists, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let hsetnx_insert = b"*4\r\n$6\r\nHSETNX\r\n$3\r\nkey\r\n$2\r\nf3\r\n$2\r\nv3\r\n";
    let meta = parse_resp_command_arg_slices(hsetnx_insert, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let hexists = b"*3\r\n$7\r\nHEXISTS\r\n$3\r\nkey\r\n$2\r\nf3\r\n";
    let meta = parse_resp_command_arg_slices(hexists, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let hkeys = b"*2\r\n$5\r\nHKEYS\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(hkeys, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"*3\r\n$2\r\nf1\r\n$2\r\nf2\r\n$2\r\nf3\r\n");

    response.clear();
    let hvals = b"*2\r\n$5\r\nHVALS\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(hvals, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"*3\r\n$2\r\nv1\r\n$2\r\nv2\r\n$2\r\nv3\r\n");

    response.clear();
    let hstrlen = b"*3\r\n$7\r\nHSTRLEN\r\n$3\r\nkey\r\n$2\r\nf2\r\n";
    let meta = parse_resp_command_arg_slices(hstrlen, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":2\r\n");

    response.clear();
    let hincrby = b"*4\r\n$7\r\nHINCRBY\r\n$3\r\nkey\r\n$1\r\nn\r\n$1\r\n2\r\n";
    let meta = parse_resp_command_arg_slices(hincrby, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":2\r\n");

    response.clear();
    let hincrby_negative = b"*4\r\n$7\r\nHINCRBY\r\n$3\r\nkey\r\n$1\r\nn\r\n$2\r\n-5\r\n";
    let meta = parse_resp_command_arg_slices(hincrby_negative, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":-3\r\n");

    response.clear();
    let hset_max =
        b"*4\r\n$4\r\nHSET\r\n$3\r\nkey\r\n$7\r\novrflow\r\n$19\r\n9223372036854775807\r\n";
    let meta = parse_resp_command_arg_slices(hset_max, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let hincrby_overflow = b"*4\r\n$7\r\nHINCRBY\r\n$3\r\nkey\r\n$7\r\novrflow\r\n$1\r\n1\r\n";
    let meta = parse_resp_command_arg_slices(hincrby_overflow, &mut args).unwrap();
    let err = processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .err()
        .unwrap();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR increment or decrement would overflow\r\n");

    response.clear();
    let hincrbyfloat = b"*4\r\n$12\r\nHINCRBYFLOAT\r\n$3\r\nkey\r\n$1\r\nf\r\n$3\r\n2.5\r\n";
    let meta = parse_resp_command_arg_slices(hincrbyfloat, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$3\r\n2.5\r\n");

    response.clear();
    let hincrbyfloat_again = b"*4\r\n$12\r\nHINCRBYFLOAT\r\n$3\r\nkey\r\n$1\r\nf\r\n$3\r\n3.5\r\n";
    let meta = parse_resp_command_arg_slices(hincrbyfloat_again, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$1\r\n6\r\n");

    assert_command_error(
        &processor,
        "HINCRBYFLOAT hfoo field +inf",
        b"-ERR value is NaN or Infinity\r\n",
    );
    assert_command_response(&processor, "EXISTS hfoo", b":0\r\n");
}

#[test]
fn hash_field_expiration_extension_commands_cover_ttl_expiretime_and_persist() {
    let processor = RequestProcessor::new().unwrap();
    assert_command_integer(&processor, "HSET hx f1 v1 f2 v2", 2);

    let now_millis = current_unix_time_millis().unwrap();
    let future_secs = (now_millis / 1000) + 30;
    let future_millis = future_secs * 1000;

    let hpexpireat = format!("HPEXPIREAT hx {future_millis} FIELDS 2 f1 missing");
    let response = execute_command_line(&processor, &hpexpireat).unwrap();
    assert_eq!(parse_integer_array_response(&response), vec![1, -2]);

    let hpexpiretime = execute_command_line(&processor, "HPEXPIRETIME hx FIELDS 2 f1 f2").unwrap();
    assert_eq!(
        parse_integer_array_response(&hpexpiretime),
        vec![future_millis as i64, -1]
    );

    let hexpiretime = execute_command_line(&processor, "HEXPIRETIME hx FIELDS 2 f1 f2").unwrap();
    assert_eq!(
        parse_integer_array_response(&hexpiretime),
        vec![future_secs as i64, -1]
    );

    let hpttl = execute_command_line(&processor, "HPTTL hx FIELDS 2 f1 f2").unwrap();
    let hpttl_values = parse_integer_array_response(&hpttl);
    assert_eq!(hpttl_values.len(), 2);
    assert!((0..=30_000).contains(&hpttl_values[0]));
    assert_eq!(hpttl_values[1], -1);

    let httl = execute_command_line(&processor, "HTTL hx FIELDS 2 f1 f2").unwrap();
    let httl_values = parse_integer_array_response(&httl);
    assert_eq!(httl_values.len(), 2);
    assert!((0..=30).contains(&httl_values[0]));
    assert_eq!(httl_values[1], -1);

    let hpersist = execute_command_line(&processor, "HPERSIST hx FIELDS 3 f1 f2 missing").unwrap();
    assert_eq!(parse_integer_array_response(&hpersist), vec![1, -1, -2]);
    assert_eq!(
        parse_integer_array_response(
            &execute_command_line(&processor, "HPEXPIRETIME hx FIELDS 1 f1").unwrap()
        ),
        vec![-1]
    );

    let future_secs2 = (current_unix_time_millis().unwrap() / 1000) + 45;
    let hexpireat = format!("HEXPIREAT hx {future_secs2} FIELDS 1 f2");
    assert_eq!(
        parse_integer_array_response(&execute_command_line(&processor, &hexpireat).unwrap()),
        vec![1]
    );
    assert_eq!(
        parse_integer_array_response(
            &execute_command_line(&processor, "HEXPIRETIME hx FIELDS 1 f2").unwrap()
        ),
        vec![future_secs2 as i64]
    );

    assert_command_error(
        &processor,
        "HEXPIREAT hx -1 FIELDS 1 f2",
        b"-ERR value is out of range\r\n",
    );
    assert_eq!(
        parse_integer_array_response(
            &execute_command_line(&processor, "HPERSIST missing-hx FIELDS 1 f").unwrap()
        ),
        vec![-2]
    );
}

#[test]
fn hash_field_expiration_commands_are_scoped_by_selected_db_without_db0_fallback() {
    let processor = RequestProcessor::new().unwrap();
    let db0 = DbName::default();
    let db9 = DbName::new(9);

    assert_command_response_in_db(&processor, "HSET shared field db0", b":1\r\n", db0);
    assert_command_response_in_db(&processor, "HSET shared field db9", b":1\r\n", db9);

    let future_millis = current_unix_time_millis().unwrap() + 30_000;
    let hpexpireat = format!("HPEXPIREAT shared {future_millis} FIELDS 1 field");
    assert_eq!(
        parse_integer_array_response(&execute_command_line_in_db(&processor, &hpexpireat, db9)),
        vec![1]
    );

    assert_eq!(
        parse_integer_array_response(&execute_command_line_in_db(
            &processor,
            "HPTTL shared FIELDS 1 field",
            db0,
        )),
        vec![-1]
    );

    let db9_hpttl = parse_integer_array_response(&execute_command_line_in_db(
        &processor,
        "HPTTL shared FIELDS 1 field",
        db9,
    ));
    assert_eq!(db9_hpttl.len(), 1);
    assert!((0..=30_000).contains(&db9_hpttl[0]));

    assert_eq!(
        parse_integer_array_response(&execute_command_line_in_db(
            &processor,
            "HPERSIST shared FIELDS 1 field",
            db0,
        )),
        vec![-1]
    );
    assert_eq!(
        parse_integer_array_response(&execute_command_line_in_db(
            &processor,
            "HPERSIST shared FIELDS 1 field",
            db9,
        )),
        vec![1]
    );

    assert_command_response_in_db(&processor, "HGET shared field", b"$3\r\ndb0\r\n", db0);
    assert_command_response_in_db(&processor, "HGET shared field", b"$3\r\ndb9\r\n", db9);
}

#[test]
fn hsetex_hgetex_hgetdel_hexpire_hpexpire_cover_basic_operations() {
    let processor = RequestProcessor::new().unwrap();

    // --- HSETEX: set hash fields with per-field expiration ---
    let now_millis = current_unix_time_millis().unwrap();
    let future_millis = now_millis + 60_000;
    let cmd = format!("HSETEX hsx PXAT {future_millis} FIELDS 2 f1 v1 f2 v2");
    assert_command_integer(&processor, &cmd, 1);
    // Fields exist and have expiration.
    let hpttl = execute_command_line(&processor, "HPTTL hsx FIELDS 2 f1 f2").unwrap();
    let ttls = parse_integer_array_response(&hpttl);
    assert_eq!(ttls.len(), 2);
    assert!((0..=60_000).contains(&ttls[0]));
    assert!((0..=60_000).contains(&ttls[1]));
    // Values are stored correctly.
    let v1 = execute_command_line(&processor, "HGET hsx f1").unwrap();
    assert_eq!(v1, b"$2\r\nv1\r\n");
    let v2 = execute_command_line(&processor, "HGET hsx f2").unwrap();
    assert_eq!(v2, b"$2\r\nv2\r\n");
    // HSETEX arity error (fewer than 5 args).
    assert_command_error(
        &processor,
        "HSETEX hsx PX 100",
        b"-ERR wrong number of arguments for 'hsetex' command\r\n",
    );

    // --- HGETEX: get fields and set new expiration ---
    let future_millis2 = current_unix_time_millis().unwrap() + 90_000;
    let cmd = format!("HGETEX hsx PXAT {future_millis2} FIELDS 2 f1 missing");
    let response = execute_command_line(&processor, &cmd).unwrap();
    // Returns array of bulk strings: f1 value, nil for missing.
    assert!(response.starts_with(b"*2\r\n"));
    assert!(
        response
            .windows(b"$2\r\nv1\r\n".len())
            .any(|w| w == b"$2\r\nv1\r\n")
    );
    assert!(response.windows(b"$-1\r\n".len()).any(|w| w == b"$-1\r\n"));
    // f1 now has new expiration.
    let hpttl = execute_command_line(&processor, "HPTTL hsx FIELDS 1 f1").unwrap();
    let ttls = parse_integer_array_response(&hpttl);
    assert!((0..=90_000).contains(&ttls[0]));
    // Missing key returns all nil.
    let response = execute_command_line(&processor, "HGETEX nokey FIELDS 1 f1").unwrap();
    assert!(response.starts_with(b"*1\r\n"));
    assert!(response.windows(b"$-1\r\n".len()).any(|w| w == b"$-1\r\n"));

    // --- HGETDEL: get and atomically delete fields ---
    assert_command_integer(&processor, "HSET hdel f1 v1 f2 v2 f3 v3", 3);
    let response = execute_command_line(&processor, "HGETDEL hdel FIELDS 2 f1 missing").unwrap();
    assert!(response.starts_with(b"*2\r\n"));
    assert!(
        response
            .windows(b"$2\r\nv1\r\n".len())
            .any(|w| w == b"$2\r\nv1\r\n")
    );
    assert!(response.windows(b"$-1\r\n".len()).any(|w| w == b"$-1\r\n"));
    // f1 is now deleted.
    let v1 = execute_command_line(&processor, "HGET hdel f1").unwrap();
    assert_eq!(v1, b"$-1\r\n");
    // f2, f3 remain.
    let v2 = execute_command_line(&processor, "HGET hdel f2").unwrap();
    assert_eq!(v2, b"$2\r\nv2\r\n");
    // Missing key returns all nil.
    let response = execute_command_line(&processor, "HGETDEL nokey FIELDS 1 f1").unwrap();
    assert!(response.windows(b"$-1\r\n".len()).any(|w| w == b"$-1\r\n"));

    // --- HEXPIRE: set seconds-based expiration on hash fields ---
    assert_command_integer(&processor, "HSET hexp f1 v1 f2 v2", 2);
    let response = execute_command_line(&processor, "HEXPIRE hexp 30 FIELDS 2 f1 missing").unwrap();
    assert_eq!(parse_integer_array_response(&response), vec![1, -2]);
    // f1 has TTL, f2 does not.
    let httl = execute_command_line(&processor, "HTTL hexp FIELDS 2 f1 f2").unwrap();
    let ttls = parse_integer_array_response(&httl);
    assert!((0..=30).contains(&ttls[0]));
    assert_eq!(ttls[1], -1);
    // Negative seconds rejected.
    assert_command_error(
        &processor,
        "HEXPIRE hexp -1 FIELDS 1 f1",
        b"-ERR value is out of range\r\n",
    );

    // --- HPEXPIRE: set milliseconds-based expiration on hash fields ---
    assert_command_integer(&processor, "HSET hpexp f1 v1 f2 v2", 2);
    let response =
        execute_command_line(&processor, "HPEXPIRE hpexp 45000 FIELDS 2 f1 missing").unwrap();
    assert_eq!(parse_integer_array_response(&response), vec![1, -2]);
    let hpttl = execute_command_line(&processor, "HPTTL hpexp FIELDS 1 f1").unwrap();
    let ttls = parse_integer_array_response(&hpttl);
    assert!((0..=45_000).contains(&ttls[0]));
    // Negative millis rejected.
    assert_command_error(
        &processor,
        "HPEXPIRE hpexp -1 FIELDS 1 f1",
        b"-ERR value is out of range\r\n",
    );
}

#[test]
fn hrandfield_supports_count_withvalues_and_resp3_shape() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 16];
    let mut response = Vec::new();

    let hset = b"*8\r\n$4\r\nHSET\r\n$3\r\nkey\r\n$2\r\nf1\r\n$1\r\n1\r\n$2\r\nf2\r\n$1\r\n2\r\n$2\r\nf3\r\n$1\r\n3\r\n";
    let meta = parse_resp_command_arg_slices(hset, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":3\r\n");

    response.clear();
    let hrand_single = b"*2\r\n$10\r\nHRANDFIELD\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(hrand_single, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert!(response.starts_with(b"$"));

    response.clear();
    let hrand_count = b"*3\r\n$10\r\nHRANDFIELD\r\n$3\r\nkey\r\n$1\r\n2\r\n";
    let meta = parse_resp_command_arg_slices(hrand_count, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert!(response.starts_with(b"*2\r\n"));

    response.clear();
    let hrand_withvalues =
        b"*4\r\n$10\r\nHRANDFIELD\r\n$3\r\nkey\r\n$2\r\n-4\r\n$10\r\nWITHVALUES\r\n";
    let meta = parse_resp_command_arg_slices(hrand_withvalues, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert!(response.starts_with(b"*8\r\n"));

    // Switch to RESP3 via set_resp_protocol_version (HELLO returns server info now).
    processor.set_resp_protocol_version(RespProtocolVersion::Resp3);

    response.clear();
    let hrand_resp3 = b"*4\r\n$10\r\nHRANDFIELD\r\n$3\r\nkey\r\n$1\r\n2\r\n$10\r\nWITHVALUES\r\n";
    let meta = parse_resp_command_arg_slices(hrand_resp3, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert!(response.starts_with(b"*2\r\n*2\r\n"));

    // RESP3 without WITHVALUES: flat array of bulk strings (no nested `*1` wrappers).
    response.clear();
    let hrand_resp3_no_values = b"*3\r\n$10\r\nHRANDFIELD\r\n$3\r\nkey\r\n$1\r\n2\r\n";
    let meta = parse_resp_command_arg_slices(hrand_resp3_no_values, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert!(response.starts_with(b"*2\r\n$"));

    // Switch back to RESP2.
    processor.set_resp_protocol_version(RespProtocolVersion::Resp2);

    response.clear();
    let meta = parse_resp_command_arg_slices(hrand_resp3, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let hget = b"*3\r\n$4\r\nHGET\r\n$3\r\nkey\r\n$5\r\nfield\r\n";
    let meta = parse_resp_command_arg_slices(hget, &mut args).unwrap();
    let err = processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .err()
        .unwrap();
    err.append_resp_error(&mut response);
    assert_eq!(
        response,
        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
    );

    assert_command_error(
        &processor,
        "SINTERCARD 0 myset",
        b"-ERR numkeys should be greater than 0\r\n",
    );
    assert_command_error(
        &processor,
        "SINTERCARD a myset",
        b"-ERR numkeys should be greater than 0\r\n",
    );
    assert_command_error(
        &processor,
        "SINTERCARD 2 myset",
        b"-ERR Number of keys can't be greater than number of args\r\n",
    );
    assert_command_error(
        &processor,
        "SINTERCARD 3 myset myset2",
        b"-ERR Number of keys can't be greater than number of args\r\n",
    );
    assert_command_error(
        &processor,
        "SINTERCARD 1 myset LIMIT -1",
        b"-ERR LIMIT can't be negative\r\n",
    );
    assert_command_error(
        &processor,
        "SINTERCARD 1 myset LIMIT a",
        b"-ERR LIMIT can't be negative\r\n",
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":2\r\n");

    response.clear();
    let rpush = b"*3\r\n$5\r\nRPUSH\r\n$3\r\nkey\r\n$1\r\nc\r\n";
    let meta = parse_resp_command_arg_slices(rpush, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":3\r\n");

    response.clear();
    let lrange_all = b"*4\r\n$6\r\nLRANGE\r\n$3\r\nkey\r\n$1\r\n0\r\n$2\r\n-1\r\n";
    let meta = parse_resp_command_arg_slices(lrange_all, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"*3\r\n$1\r\nb\r\n$1\r\na\r\n$1\r\nc\r\n");

    response.clear();
    let lpop = b"*2\r\n$4\r\nLPOP\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(lpop, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$1\r\nb\r\n");

    response.clear();
    let rpop = b"*2\r\n$4\r\nRPOP\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(rpop, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$1\r\nc\r\n");

    response.clear();
    let meta = parse_resp_command_arg_slices(lpop, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$1\r\na\r\n");

    response.clear();
    let meta = parse_resp_command_arg_slices(lpop, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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

    processor.set_resp_protocol_version(RespProtocolVersion::Resp3);
    assert_command_response(&processor, "LPOP missing", b"_\r\n");
    assert_command_response(&processor, "LPOP missing 2", b"_\r\n");
    assert_command_response(&processor, "RPOP missing 2", b"_\r\n");
    processor.set_resp_protocol_version(RespProtocolVersion::Resp2);
}

#[test]
fn lrange_supports_negative_indexes() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 16];
    let mut response = Vec::new();

    let rpush = b"*6\r\n$5\r\nRPUSH\r\n$3\r\nkey\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n";
    let meta = parse_resp_command_arg_slices(rpush, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":4\r\n");

    response.clear();
    let lrange = b"*4\r\n$6\r\nLRANGE\r\n$3\r\nkey\r\n$2\r\n-3\r\n$2\r\n-2\r\n";
    let meta = parse_resp_command_arg_slices(lrange, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":3\r\n");

    response.clear();
    let llen = b"*2\r\n$4\r\nLLEN\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(llen, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":3\r\n");

    response.clear();
    let lindex_zero = b"*3\r\n$6\r\nLINDEX\r\n$3\r\nkey\r\n$1\r\n0\r\n";
    let meta = parse_resp_command_arg_slices(lindex_zero, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$1\r\na\r\n");

    response.clear();
    let lindex_negative = b"*3\r\n$6\r\nLINDEX\r\n$3\r\nkey\r\n$2\r\n-1\r\n";
    let meta = parse_resp_command_arg_slices(lindex_negative, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$1\r\nc\r\n");

    response.clear();
    let lset = b"*4\r\n$4\r\nLSET\r\n$3\r\nkey\r\n$1\r\n1\r\n$1\r\nz\r\n";
    let meta = parse_resp_command_arg_slices(lset, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let ltrim = b"*4\r\n$5\r\nLTRIM\r\n$3\r\nkey\r\n$1\r\n1\r\n$1\r\n2\r\n";
    let meta = parse_resp_command_arg_slices(ltrim, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let lpushx = b"*4\r\n$6\r\nLPUSHX\r\n$3\r\nkey\r\n$1\r\nx\r\n$1\r\ny\r\n";
    let meta = parse_resp_command_arg_slices(lpushx, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":4\r\n");

    response.clear();
    let rpushx_missing = b"*3\r\n$6\r\nRPUSHX\r\n$7\r\nmissing\r\n$1\r\nq\r\n";
    let meta = parse_resp_command_arg_slices(rpushx_missing, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let lrem_all_x = b"*4\r\n$4\r\nLREM\r\n$3\r\nkey\r\n$1\r\n0\r\n$1\r\nx\r\n";
    let meta = parse_resp_command_arg_slices(lrem_all_x, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let lrem_tail_c = b"*4\r\n$4\r\nLREM\r\n$3\r\nkey\r\n$2\r\n-1\r\n$1\r\nc\r\n";
    let meta = parse_resp_command_arg_slices(lrem_tail_c, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let lrange = b"*4\r\n$6\r\nLRANGE\r\n$3\r\nkey\r\n$1\r\n0\r\n$2\r\n-1\r\n";
    let meta = parse_resp_command_arg_slices(lrange, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .err()
        .unwrap();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR no such key\r\n");

    response.clear();
    let lset_oob = b"*4\r\n$4\r\nLSET\r\n$3\r\nkey\r\n$2\r\n99\r\n$1\r\nx\r\n";
    let meta = parse_resp_command_arg_slices(lset_oob, &mut args).unwrap();
    let err = processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .err()
        .unwrap();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR index out of range\r\n");

    response.clear();
    let ltrim_missing = b"*4\r\n$5\r\nLTRIM\r\n$7\r\nmissing\r\n$1\r\n0\r\n$1\r\n1\r\n";
    let meta = parse_resp_command_arg_slices(ltrim_missing, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .err()
        .unwrap();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR value is out of range\r\n");

    response.clear();
    let lpos_count_negative =
        b"*5\r\n$4\r\nLPOS\r\n$4\r\nlpos\r\n$1\r\na\r\n$5\r\nCOUNT\r\n$2\r\n-1\r\n";
    let meta = parse_resp_command_arg_slices(lpos_count_negative, &mut args).unwrap();
    let err = processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .err()
        .unwrap();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR value is out of range\r\n");

    response.clear();
    let lpos_maxlen_negative =
        b"*5\r\n$4\r\nLPOS\r\n$4\r\nlpos\r\n$1\r\na\r\n$6\r\nMAXLEN\r\n$2\r\n-1\r\n";
    let meta = parse_resp_command_arg_slices(lpos_maxlen_negative, &mut args).unwrap();
    let err = processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .err()
        .unwrap();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR value is out of range\r\n");

    response.clear();
    let lpos_unknown_option =
        b"*5\r\n$4\r\nLPOS\r\n$4\r\nlpos\r\n$1\r\na\r\n$7\r\nUNKNOWN\r\n$1\r\n1\r\n";
    let meta = parse_resp_command_arg_slices(lpos_unknown_option, &mut args).unwrap();
    let err = processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .err()
        .unwrap();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR syntax error\r\n");
}

#[test]
fn list_commands_are_scoped_by_selected_db_without_db0_fallback() {
    let processor = RequestProcessor::new().unwrap();
    let db0 = DbName::default();
    let db9 = DbName::new(9);

    assert_eq!(
        execute_command_line_in_db(&processor, "RPUSH shared zero-a zero-b", db0),
        b":2\r\n".to_vec()
    );
    assert_eq!(
        execute_command_line_in_db(&processor, "RPUSH shared nine-a nine-b nine-c", db9),
        b":3\r\n".to_vec()
    );

    assert_eq!(
        execute_command_line_in_db(&processor, "LRANGE shared 0 -1", db0),
        b"*2\r\n$6\r\nzero-a\r\n$6\r\nzero-b\r\n".to_vec()
    );
    assert_eq!(
        execute_command_line_in_db(&processor, "LRANGE shared 0 -1", db9),
        b"*3\r\n$6\r\nnine-a\r\n$6\r\nnine-b\r\n$6\r\nnine-c\r\n".to_vec()
    );

    assert_eq!(
        execute_command_line_in_db(&processor, "LPOP shared", db9),
        b"$6\r\nnine-a\r\n".to_vec()
    );
    assert_eq!(
        execute_command_line_in_db(&processor, "LLEN shared", db0),
        b":2\r\n".to_vec()
    );
    assert_eq!(
        execute_command_line_in_db(&processor, "LTRIM shared 1 1", db0),
        b"+OK\r\n".to_vec()
    );

    assert_eq!(
        execute_command_line_in_db(&processor, "LRANGE shared 0 -1", db0),
        b"*1\r\n$6\r\nzero-b\r\n".to_vec()
    );
    assert_eq!(
        execute_command_line_in_db(&processor, "LRANGE shared 0 -1", db9),
        b"*2\r\n$6\r\nnine-b\r\n$6\r\nnine-c\r\n".to_vec()
    );
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .err()
        .unwrap();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR count should be greater than 0\r\n");

    response.clear();
    let lmpop_non_integer_count =
        encode_resp(&[b"LMPOP", b"1", b"missing", b"LEFT", b"COUNT", b"abc"]);
    let meta = parse_resp_command_arg_slices(&lmpop_non_integer_count, &mut args).unwrap();
    let err = processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .err()
        .unwrap();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR count should be greater than 0\r\n");

    response.clear();
    let lmpop_negative_count = encode_resp(&[b"LMPOP", b"1", b"missing", b"LEFT", b"COUNT", b"-1"]);
    let meta = parse_resp_command_arg_slices(&lmpop_negative_count, &mut args).unwrap();
    let err = processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .err()
        .unwrap();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR count should be greater than 0\r\n");

    response.clear();
    let lmpop_zero_numkeys = encode_resp(&[b"LMPOP", b"0", b"missing", b"LEFT"]);
    let meta = parse_resp_command_arg_slices(&lmpop_zero_numkeys, &mut args).unwrap();
    let err = processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .err()
        .unwrap();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR numkeys should be greater than 0\r\n");

    response.clear();
    let lmpop_non_integer_numkeys = encode_resp(&[b"LMPOP", b"a", b"missing", b"LEFT"]);
    let meta = parse_resp_command_arg_slices(&lmpop_non_integer_numkeys, &mut args).unwrap();
    let err = processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .err()
        .unwrap();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR numkeys should be greater than 0\r\n");

    response.clear();
    let lmpop_negative_numkeys = encode_resp(&[b"LMPOP", b"-1", b"missing", b"LEFT"]);
    let meta = parse_resp_command_arg_slices(&lmpop_negative_numkeys, &mut args).unwrap();
    let err = processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .err()
        .unwrap();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR numkeys should be greater than 0\r\n");

    response.clear();
    let blpop_bad_timeout = encode_resp(&[b"BLPOP", b"k1", b"not-a-float"]);
    let meta = parse_resp_command_arg_slices(&blpop_bad_timeout, &mut args).unwrap();
    let err = processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .err()
        .unwrap();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR value is not a valid float\r\n");

    response.clear();
    let blpop_negative_timeout = encode_resp(&[b"BLPOP", b"k1", b"-1"]);
    let meta = parse_resp_command_arg_slices(&blpop_negative_timeout, &mut args).unwrap();
    let err = processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let lpush = b"*3\r\n$5\r\nLPUSH\r\n$3\r\nkey\r\n$1\r\nv\r\n";
    let meta = parse_resp_command_arg_slices(lpush, &mut args).unwrap();
    let err = processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":2\r\n");

    response.clear();
    let sismember_yes = b"*3\r\n$9\r\nSISMEMBER\r\n$3\r\nkey\r\n$1\r\nb\r\n";
    let meta = parse_resp_command_arg_slices(sismember_yes, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let sismember_no = b"*3\r\n$9\r\nSISMEMBER\r\n$3\r\nkey\r\n$1\r\nz\r\n";
    let meta = parse_resp_command_arg_slices(sismember_no, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let smembers = b"*2\r\n$8\r\nSMEMBERS\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(smembers, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"*2\r\n$1\r\na\r\n$1\r\nb\r\n");

    response.clear();
    let srem = b"*4\r\n$4\r\nSREM\r\n$3\r\nkey\r\n$1\r\na\r\n$1\r\nx\r\n";
    let meta = parse_resp_command_arg_slices(srem, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let meta = parse_resp_command_arg_slices(smembers, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"*1\r\n$1\r\nb\r\n");

    response.clear();
    let srem_last = b"*3\r\n$4\r\nSREM\r\n$3\r\nkey\r\n$1\r\nb\r\n";
    let meta = parse_resp_command_arg_slices(srem_last, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let meta = parse_resp_command_arg_slices(smembers, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"*0\r\n");
}

#[test]
fn sadd_uses_contiguous_range_encoding_for_canonical_integer_sequences() {
    let processor = RequestProcessor::new().unwrap();

    for value in 0..200 {
        let response = execute_frame(
            &processor,
            &encode_resp(&[b"SADD", b"numbers", value.to_string().as_bytes()]),
        );
        assert_eq!(response, b":1\r\n");
    }

    let object = processor
        .object_read(DbKeyRef::new(current_request_selected_db(), b"numbers"))
        .unwrap()
        .unwrap();
    assert_eq!(object.object_type, ObjectTypeTag::Set);
    assert!(matches!(
        decode_set_object_payload(&object.payload),
        Some(DecodedSetObjectPayload::ContiguousI64Range(range))
            if range.start() == 0 && range.end() == 199
    ));

    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"SCARD", b"numbers"])),
        b":200\r\n"
    );
}

#[test]
fn sadd_falls_back_from_contiguous_range_encoding_for_non_canonical_members() {
    let processor = RequestProcessor::new().unwrap();

    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"SADD", b"numbers", b"1"])),
        b":1\r\n"
    );
    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"SADD", b"numbers", b"2"])),
        b":1\r\n"
    );
    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"SADD", b"numbers", b"01"])),
        b":1\r\n"
    );

    let object = processor
        .object_read(DbKeyRef::new(current_request_selected_db(), b"numbers"))
        .unwrap()
        .unwrap();
    assert!(matches!(
        decode_set_object_payload(&object.payload),
        Some(DecodedSetObjectPayload::Members(_))
    ));

    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"SISMEMBER", b"numbers", b"1"])),
        b":1\r\n"
    );
    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"SISMEMBER", b"numbers", b"01"])),
        b":1\r\n"
    );
}

#[test]
fn set_object_encoding_does_not_downgrade_after_intset_to_listpack_upgrade() {
    let processor = RequestProcessor::new().unwrap();

    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"SADD", b"myset", b"1", b"2", b"3"])
        ),
        b":3\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"OBJECT", b"ENCODING", b"myset"])
        ),
        b"$6\r\nintset\r\n"
    );

    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"SADD", b"myset", b"a"])),
        b":1\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"OBJECT", b"ENCODING", b"myset"])
        ),
        b"$8\r\nlistpack\r\n"
    );

    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"SREM", b"myset", b"a"])),
        b":1\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"OBJECT", b"ENCODING", b"myset"])
        ),
        b"$8\r\nlistpack\r\n"
    );
}

#[test]
fn set_object_encoding_respects_set_max_listpack_value_and_hashtable_floor() {
    let processor = RequestProcessor::new().unwrap();

    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"CONFIG", b"SET", b"set-max-listpack-value", b"32"]),
        ),
        b"+OK\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"CONFIG", b"GET", b"set-max-listpack-value"]),
        ),
        b"*2\r\n$22\r\nset-max-listpack-value\r\n$2\r\n32\r\n"
    );

    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[
                b"SADD",
                b"myset",
                b"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
                b"a",
                b"b",
            ]),
        ),
        b":3\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"OBJECT", b"ENCODING", b"myset"])
        ),
        b"$9\r\nhashtable\r\n"
    );

    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"SREM", b"myset", b"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"]),
        ),
        b":1\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"OBJECT", b"ENCODING", b"myset"])
        ),
        b"$9\r\nhashtable\r\n"
    );
}

#[test]
fn additional_set_commands_cover_common_redis_semantics() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 20];
    let mut response = Vec::new();

    let sadd_src = b"*6\r\n$4\r\nSADD\r\n$3\r\nsrc\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n";
    let meta = parse_resp_command_arg_slices(sadd_src, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":4\r\n");

    response.clear();
    let sadd_dst = b"*3\r\n$4\r\nSADD\r\n$3\r\ndst\r\n$1\r\nz\r\n";
    let meta = parse_resp_command_arg_slices(sadd_dst, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let scard = b"*2\r\n$5\r\nSCARD\r\n$3\r\nsrc\r\n";
    let meta = parse_resp_command_arg_slices(scard, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":4\r\n");

    response.clear();
    let smismember = b"*5\r\n$10\r\nSMISMEMBER\r\n$3\r\nsrc\r\n$1\r\na\r\n$1\r\nx\r\n$1\r\nc\r\n";
    let meta = parse_resp_command_arg_slices(smismember, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"*3\r\n:1\r\n:0\r\n:1\r\n");

    response.clear();
    let srandmember = b"*3\r\n$11\r\nSRANDMEMBER\r\n$3\r\nsrc\r\n$1\r\n2\r\n";
    let meta = parse_resp_command_arg_slices(srandmember, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert!(response.starts_with(b"*2\r\n"));

    response.clear();
    let smove = b"*4\r\n$5\r\nSMOVE\r\n$3\r\nsrc\r\n$3\r\ndst\r\n$1\r\na\r\n";
    let meta = parse_resp_command_arg_slices(smove, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let sismember_dst = b"*3\r\n$9\r\nSISMEMBER\r\n$3\r\ndst\r\n$1\r\na\r\n";
    let meta = parse_resp_command_arg_slices(sismember_dst, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let spop_count = b"*3\r\n$4\r\nSPOP\r\n$3\r\nsrc\r\n$1\r\n2\r\n";
    let meta = parse_resp_command_arg_slices(spop_count, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert!(response.starts_with(b"*2\r\n"));

    response.clear();
    let scard_after_pop = b"*2\r\n$5\r\nSCARD\r\n$3\r\nsrc\r\n";
    let meta = parse_resp_command_arg_slices(scard_after_pop, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let spop_single = b"*2\r\n$4\r\nSPOP\r\n$3\r\nsrc\r\n";
    let meta = parse_resp_command_arg_slices(spop_single, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert!(response.starts_with(b"$"));

    response.clear();
    let scard_empty = b"*2\r\n$5\r\nSCARD\r\n$3\r\nsrc\r\n";
    let meta = parse_resp_command_arg_slices(scard_empty, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let spop_negative = b"*3\r\n$4\r\nSPOP\r\n$3\r\ndst\r\n$2\r\n-1\r\n";
    let meta = parse_resp_command_arg_slices(spop_negative, &mut args).unwrap();
    let err = processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":3\r\n");

    response.clear();
    let sadd_s2 = encode_resp(&[b"SADD", b"s2", b"b", b"c", b"d"]);
    let meta = parse_resp_command_arg_slices(&sadd_s2, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":3\r\n");

    response.clear();
    let sadd_s3 = encode_resp(&[b"SADD", b"s3", b"c", b"e"]);
    let meta = parse_resp_command_arg_slices(&sadd_s3, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .err()
        .unwrap();
    err.append_resp_error(&mut response);
    assert_eq!(
        response,
        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
    );

    // Redis still validates all key types even when intermediate set algebra
    // result becomes empty or first key is missing.
    response.clear();
    let sinter_missing_then_wrongtype = encode_resp(&[b"SINTER", b"missing", b"plain"]);
    let meta = parse_resp_command_arg_slices(&sinter_missing_then_wrongtype, &mut args).unwrap();
    let err = processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .err()
        .unwrap();
    err.append_resp_error(&mut response);
    assert_eq!(
        response,
        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
    );

    response.clear();
    let sdiff_empty_then_wrongtype = encode_resp(&[b"SDIFF", b"s1", b"s2", b"plain"]);
    let meta = parse_resp_command_arg_slices(&sdiff_empty_then_wrongtype, &mut args).unwrap();
    let err = processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .err()
        .unwrap();
    err.append_resp_error(&mut response);
    assert_eq!(
        response,
        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
    );

    response.clear();
    let sinterstore_missing_then_wrongtype =
        encode_resp(&[b"SINTERSTORE", b"dst_bad", b"missing", b"plain"]);
    let meta =
        parse_resp_command_arg_slices(&sinterstore_missing_then_wrongtype, &mut args).unwrap();
    let err = processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .err()
        .unwrap();
    err.append_resp_error(&mut response);
    assert_eq!(
        response,
        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
    );

    response.clear();
    let sdiffstore_empty_then_wrongtype =
        encode_resp(&[b"SDIFFSTORE", b"dst_bad", b"s1", b"s2", b"plain"]);
    let meta = parse_resp_command_arg_slices(&sdiffstore_empty_then_wrongtype, &mut args).unwrap();
    let err = processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let sadd = b"*3\r\n$4\r\nSADD\r\n$3\r\nkey\r\n$1\r\nv\r\n";
    let meta = parse_resp_command_arg_slices(sadd, &mut args).unwrap();
    let err = processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":2\r\n");

    response.clear();
    let zscore = b"*3\r\n$6\r\nZSCORE\r\n$3\r\nkey\r\n$3\r\none\r\n";
    let meta = parse_resp_command_arg_slices(zscore, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$1\r\n1\r\n");

    response.clear();
    let zcard = b"*2\r\n$5\r\nZCARD\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(zcard, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":2\r\n");

    response.clear();
    let zcount_all = encode_resp(&[b"ZCOUNT", b"key", b"-inf", b"+inf"]);
    let meta = parse_resp_command_arg_slices(&zcount_all, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":2\r\n");

    response.clear();
    let zcount_exclusive = encode_resp(&[b"ZCOUNT", b"key", b"(1", b"2"]);
    let meta = parse_resp_command_arg_slices(&zcount_exclusive, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let zrank_one = b"*3\r\n$5\r\nZRANK\r\n$3\r\nkey\r\n$3\r\none\r\n";
    let meta = parse_resp_command_arg_slices(zrank_one, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let zrevrank_one = b"*3\r\n$8\r\nZREVRANK\r\n$3\r\nkey\r\n$3\r\none\r\n";
    let meta = parse_resp_command_arg_slices(zrevrank_one, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let zincrby = b"*4\r\n$7\r\nZINCRBY\r\n$3\r\nkey\r\n$1\r\n2\r\n$3\r\none\r\n";
    let meta = parse_resp_command_arg_slices(zincrby, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$1\r\n3\r\n");

    response.clear();
    let meta = parse_resp_command_arg_slices(zrank_one, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let zrange = b"*4\r\n$6\r\nZRANGE\r\n$3\r\nkey\r\n$1\r\n0\r\n$2\r\n-1\r\n";
    let meta = parse_resp_command_arg_slices(zrange, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"*2\r\n$3\r\ntwo\r\n$3\r\none\r\n");

    response.clear();
    let zrange_withscores = encode_resp(&[b"ZRANGE", b"key", b"0", b"-1", b"WITHSCORES"]);
    let meta = parse_resp_command_arg_slices(&zrange_withscores, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(
        response,
        b"*4\r\n$3\r\ntwo\r\n$1\r\n2\r\n$3\r\none\r\n$1\r\n3\r\n"
    );

    processor.set_resp_protocol_version(RespProtocolVersion::Resp3);
    response.clear();
    let meta = parse_resp_command_arg_slices(&zrange_withscores, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(
        response,
        b"*2\r\n*2\r\n$3\r\ntwo\r\n,2\r\n*2\r\n$3\r\none\r\n,3\r\n"
    );
    processor.set_resp_protocol_version(RespProtocolVersion::Resp2);

    response.clear();
    let zadd_update = b"*4\r\n$4\r\nZADD\r\n$3\r\nkey\r\n$1\r\n3\r\n$3\r\none\r\n";
    let meta = parse_resp_command_arg_slices(zadd_update, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let zrange_after_update = b"*4\r\n$6\r\nZRANGE\r\n$3\r\nkey\r\n$1\r\n0\r\n$1\r\n1\r\n";
    let meta = parse_resp_command_arg_slices(zrange_after_update, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"*3\r\n$1\r\n3\r\n$-1\r\n$1\r\n2\r\n");

    response.clear();
    let zrandmember = b"*2\r\n$11\r\nZRANDMEMBER\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(zrandmember, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert!(response.starts_with(b"$"));

    response.clear();
    let zrandmember_withscores =
        b"*4\r\n$11\r\nZRANDMEMBER\r\n$3\r\nkey\r\n$1\r\n2\r\n$10\r\nWITHSCORES\r\n";
    let meta = parse_resp_command_arg_slices(zrandmember_withscores, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert!(response.starts_with(b"*4\r\n"));
    assert!(response.windows(9).any(|w| w == b"$3\r\none\r\n"));
    assert!(response.windows(9).any(|w| w == b"$3\r\ntwo\r\n"));

    response.clear();
    let zadd_popkey =
        b"*8\r\n$4\r\nZADD\r\n$6\r\npopkey\r\n$1\r\n1\r\n$1\r\na\r\n$1\r\n2\r\n$1\r\nb\r\n$1\r\n3\r\n$1\r\nc\r\n";
    let meta = parse_resp_command_arg_slices(zadd_popkey, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":3\r\n");

    response.clear();
    let zpopmin = b"*2\r\n$7\r\nZPOPMIN\r\n$6\r\npopkey\r\n";
    let meta = parse_resp_command_arg_slices(zpopmin, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"*2\r\n$1\r\na\r\n$1\r\n1\r\n");

    response.clear();
    let zpopmax = b"*3\r\n$7\r\nZPOPMAX\r\n$6\r\npopkey\r\n$1\r\n2\r\n";
    let meta = parse_resp_command_arg_slices(zpopmax, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(
        response,
        b"*4\r\n$1\r\nc\r\n$1\r\n3\r\n$1\r\nb\r\n$1\r\n2\r\n"
    );

    response.clear();
    let zcard_popkey = b"*2\r\n$5\r\nZCARD\r\n$6\r\npopkey\r\n";
    let meta = parse_resp_command_arg_slices(zcard_popkey, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let bzpopmin_bad_timeout = b"*3\r\n$8\r\nBZPOPMIN\r\n$3\r\nbz1\r\n$3\r\nbad\r\n";
    let meta = parse_resp_command_arg_slices(bzpopmin_bad_timeout, &mut args).unwrap();
    let err = processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .err()
        .unwrap();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR value is not a valid float\r\n");

    response.clear();
    let bzmpop_negative_timeout =
        b"*7\r\n$6\r\nBZMPOP\r\n$2\r\n-1\r\n$1\r\n1\r\n$3\r\nbz1\r\n$3\r\nMIN\r\n$5\r\nCOUNT\r\n$1\r\n1\r\n";
    let meta = parse_resp_command_arg_slices(bzmpop_negative_timeout, &mut args).unwrap();
    let err = processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .err()
        .unwrap();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR value is out of range\r\n");

    response.clear();
    let zrem = b"*4\r\n$4\r\nZREM\r\n$3\r\nkey\r\n$3\r\none\r\n$4\r\nnone\r\n";
    let meta = parse_resp_command_arg_slices(zrem, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let zrem_last = b"*3\r\n$4\r\nZREM\r\n$3\r\nkey\r\n$3\r\ntwo\r\n";
    let meta = parse_resp_command_arg_slices(zrem_last, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let meta = parse_resp_command_arg_slices(zrange, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"*0\r\n");
}

#[test]
fn zset_commands_are_scoped_by_selected_db_without_db0_fallback() {
    let processor = RequestProcessor::new().unwrap();
    let db0 = DbName::default();
    let db9 = DbName::new(9);

    assert_eq!(
        execute_command_line_in_db(&processor, "ZADD shared 1 one 2 two", db0),
        b":2\r\n".to_vec()
    );
    assert_eq!(
        execute_command_line_in_db(&processor, "ZADD shared 5 five 6 six", db9),
        b":2\r\n".to_vec()
    );

    assert_eq!(
        execute_command_line_in_db(&processor, "ZRANGE shared 0 -1 WITHSCORES", db0),
        b"*4\r\n$3\r\none\r\n$1\r\n1\r\n$3\r\ntwo\r\n$1\r\n2\r\n".to_vec()
    );
    assert_eq!(
        execute_command_line_in_db(&processor, "ZRANGE shared 0 -1 WITHSCORES", db9),
        b"*4\r\n$4\r\nfive\r\n$1\r\n5\r\n$3\r\nsix\r\n$1\r\n6\r\n".to_vec()
    );

    assert_eq!(
        execute_command_line_in_db(&processor, "ZRANGESTORE out shared 0 -1", db9),
        b":2\r\n".to_vec()
    );
    assert_eq!(
        execute_command_line_in_db(&processor, "EXISTS out", db0),
        b":0\r\n".to_vec()
    );
    assert_eq!(
        execute_command_line_in_db(&processor, "EXISTS out", db9),
        b":1\r\n".to_vec()
    );

    assert_eq!(
        execute_command_line_in_db(&processor, "ZREMRANGEBYSCORE shared 0 10", db9),
        b":2\r\n".to_vec()
    );
    assert_eq!(
        execute_command_line_in_db(&processor, "ZCARD shared", db0),
        b":2\r\n".to_vec()
    );
    assert_eq!(
        execute_command_line_in_db(&processor, "ZCARD shared", db9),
        b":0\r\n".to_vec()
    );
}

#[test]
fn zadd_supports_option_flags_and_incr_mode() {
    let processor = RequestProcessor::new().unwrap();

    assert_command_response(&processor, "ZADD z 1 one 2 two", b":2\r\n");
    assert_command_response(&processor, "ZADD z NX 3 one 4 three", b":1\r\n");
    assert_command_response(&processor, "ZSCORE z one", b"$1\r\n1\r\n");
    assert_command_response(&processor, "ZSCORE z three", b"$1\r\n4\r\n");

    assert_command_response(&processor, "ZADD z XX 5 four 6 one", b":0\r\n");
    assert_command_response(&processor, "ZSCORE z one", b"$1\r\n6\r\n");
    assert_command_response(&processor, "ZSCORE z four", b"$-1\r\n");

    assert_command_response(&processor, "ZADD z GT CH 5 one 10 two", b":1\r\n");
    assert_command_response(&processor, "ZSCORE z one", b"$1\r\n6\r\n");
    assert_command_response(&processor, "ZSCORE z two", b"$2\r\n10\r\n");
    assert_command_response(&processor, "ZADD z GT 7 four", b":1\r\n");
    assert_command_response(&processor, "ZSCORE z four", b"$1\r\n7\r\n");

    assert_command_response(&processor, "ZADD z LT CH 9 two 1 three", b":2\r\n");
    assert_command_response(&processor, "ZSCORE z two", b"$1\r\n9\r\n");
    assert_command_response(&processor, "ZSCORE z three", b"$1\r\n1\r\n");

    let incr_resp = execute_command_line(&processor, "ZADD z INCR 2 one").unwrap();
    let incr = parse_bulk_payload(&incr_resp).expect("ZADD INCR should return a score");
    assert_eq!(incr, b"8");

    assert_command_error(&processor, "ZADD z NX XX 1 bad", b"-ERR syntax error\r\n");
    assert_command_error(&processor, "ZADD z GT NX 1 bad", b"-ERR syntax error\r\n");
    assert_command_error(&processor, "ZADD z LT GT 1 bad", b"-ERR syntax error\r\n");
    assert_command_error(&processor, "ZADD z INCR 1 a 2 b", b"-ERR syntax error\r\n");

    assert_command_response(&processor, "ZADD z LT INCR 1 one", b"$-1\r\n");

    processor.set_resp_protocol_version(RespProtocolVersion::Resp3);
    assert_command_response(&processor, "ZADD z INCR 2 one", b",10\r\n");
    assert_command_response(&processor, "ZADD z LT INCR 1 one", b"_\r\n");
    processor.set_resp_protocol_version(RespProtocolVersion::Resp2);
}

#[test]
fn zset_commands_return_wrongtype_for_string_keys() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 8];
    let mut response = Vec::new();

    let set = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
    let meta = parse_resp_command_arg_slices(set, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let zadd = b"*4\r\n$4\r\nZADD\r\n$3\r\nkey\r\n$1\r\n1\r\n$1\r\nv\r\n";
    let meta = parse_resp_command_arg_slices(zadd, &mut args).unwrap();
    let err = processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let incr = b"*2\r\n$4\r\nINCR\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(incr, &mut args).unwrap();
    let err = processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":5\r\n");

    response.clear();
    let decrby = b"*3\r\n$6\r\nDECRBY\r\n$7\r\ncounter\r\n$1\r\n2\r\n";
    let meta = parse_resp_command_arg_slices(decrby, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":3\r\n");

    assert_command_error(
        &processor,
        "DECRBY counter -9223372036854775808",
        b"-ERR increment or decrement would overflow\r\n",
    );
}

#[test]
fn exists_counts_duplicates_and_object_keys() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 8];
    let mut response = Vec::new();

    let set = b"*3\r\n$3\r\nSET\r\n$1\r\ns\r\n$1\r\n1\r\n";
    let meta = parse_resp_command_arg_slices(set, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let hset = b"*4\r\n$4\r\nHSET\r\n$1\r\no\r\n$1\r\nf\r\n$1\r\nv\r\n";
    let meta = parse_resp_command_arg_slices(hset, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let exists = b"*5\r\n$6\r\nEXISTS\r\n$1\r\ns\r\n$1\r\ns\r\n$1\r\no\r\n$1\r\nx\r\n";
    let meta = parse_resp_command_arg_slices(exists, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let sadd = b"*4\r\n$4\r\nSADD\r\n$2\r\nst\r\n$1\r\na\r\n$1\r\nb\r\n";
    let meta = parse_resp_command_arg_slices(sadd, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":2\r\n");

    response.clear();
    let type_string = b"*2\r\n$4\r\nTYPE\r\n$1\r\ns\r\n";
    let meta = parse_resp_command_arg_slices(type_string, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+string\r\n");

    response.clear();
    let type_set = b"*2\r\n$4\r\nTYPE\r\n$2\r\nst\r\n";
    let meta = parse_resp_command_arg_slices(type_set, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+set\r\n");

    response.clear();
    let type_none = b"*2\r\n$4\r\nTYPE\r\n$1\r\nx\r\n";
    let meta = parse_resp_command_arg_slices(type_none, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let mget = b"*4\r\n$4\r\nMGET\r\n$2\r\nk1\r\n$2\r\nk2\r\n$2\r\nk3\r\n";
    let meta = parse_resp_command_arg_slices(mget, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"*3\r\n$2\r\nv1\r\n$2\r\nv2\r\n$-1\r\n");

    response.clear();
    let hset = b"*4\r\n$4\r\nHSET\r\n$3\r\nobj\r\n$1\r\nf\r\n$1\r\nv\r\n";
    let meta = parse_resp_command_arg_slices(hset, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let mget_obj = b"*2\r\n$4\r\nMGET\r\n$3\r\nobj\r\n";
    let meta = parse_resp_command_arg_slices(mget_obj, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"*1\r\n$-1\r\n");

    response.clear();
    let mset_overwrite_obj = b"*3\r\n$4\r\nMSET\r\n$3\r\nobj\r\n$3\r\nstr\r\n";
    let meta = parse_resp_command_arg_slices(mset_overwrite_obj, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let get = b"*2\r\n$3\r\nGET\r\n$3\r\nobj\r\n";
    let meta = parse_resp_command_arg_slices(get, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..set_meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();

    let del_frame = b"*2\r\n$3\r\nDEL\r\n$3\r\nkey\r\n";
    let del_meta = parse_resp_command_arg_slices(del_frame, &mut args).unwrap();
    response.clear();
    processor
        .execute_in_db(
            &args[..del_meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");
}

#[test]
fn smove_wrong_dst_key_type_matches_redis_external_scenario() {
    let processor = RequestProcessor::new().unwrap();

    assert_command_response(&processor, "DEL myset3{t} myset4{t}", b":0\r\n");
    assert_command_response(&processor, "SADD myset1{t} 1 a b", b":3\r\n");
    assert_command_response(&processor, "SADD myset2{t} 2 3 4", b":3\r\n");
    assert_command_response(&processor, "SET x{t} 10", b"+OK\r\n");
    assert_command_error(
        &processor,
        "SMOVE myset2{t} x{t} foo",
        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
    );
    assert_command_response(
        &processor,
        "SMEMBERS myset2{t}",
        b"*3\r\n$1\r\n2\r\n$1\r\n3\r\n$1\r\n4\r\n",
    );
}

#[test]
fn del_removes_object_key() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 4];
    let mut response = Vec::new();

    let hset = b"*4\r\n$4\r\nHSET\r\n$3\r\nobj\r\n$5\r\nfield\r\n$5\r\nvalue\r\n";
    let meta = parse_resp_command_arg_slices(hset, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let del = b"*2\r\n$3\r\nDEL\r\n$3\r\nobj\r\n";
    let meta = parse_resp_command_arg_slices(del, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let hget = b"*3\r\n$4\r\nHGET\r\n$3\r\nobj\r\n$5\r\nfield\r\n";
    let meta = parse_resp_command_arg_slices(hget, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let del = b"*2\r\n$3\r\nDEL\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(del, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let set_second = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$7\r\nupdated\r\n";
    let meta = parse_resp_command_arg_slices(set_second, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let get = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(get, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let rename = b"*3\r\n$6\r\nRENAME\r\n$3\r\nsrc\r\n$3\r\ndst\r\n";
    let meta = parse_resp_command_arg_slices(rename, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let get_dst = b"*2\r\n$3\r\nGET\r\n$3\r\ndst\r\n";
    let meta = parse_resp_command_arg_slices(get_dst, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$5\r\nvalue\r\n");

    response.clear();
    let set_src_again = b"*3\r\n$3\r\nSET\r\n$3\r\nsrc\r\n$6\r\nvalue2\r\n";
    let meta = parse_resp_command_arg_slices(set_src_again, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let renamenx_existing = b"*3\r\n$8\r\nRENAMENX\r\n$3\r\nsrc\r\n$3\r\ndst\r\n";
    let meta = parse_resp_command_arg_slices(renamenx_existing, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let get_src = b"*2\r\n$3\r\nGET\r\n$3\r\nsrc\r\n";
    let meta = parse_resp_command_arg_slices(get_src, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();

    response.clear();
    let expire_src = b"*3\r\n$6\r\nEXPIRE\r\n$3\r\nsrc\r\n$3\r\n100\r\n";
    let meta = parse_resp_command_arg_slices(expire_src, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let rename = b"*3\r\n$6\r\nRENAME\r\n$3\r\nsrc\r\n$3\r\ndst\r\n";
    let meta = parse_resp_command_arg_slices(rename, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let ttl_dst = b"*2\r\n$3\r\nTTL\r\n$3\r\ndst\r\n";
    let meta = parse_resp_command_arg_slices(ttl_dst, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    let ttl_value = std::str::from_utf8(&response[1..response.len() - 2])
        .unwrap()
        .parse::<i64>()
        .unwrap();
    assert!(ttl_value > 0);

    response.clear();
    let rename_missing = b"*3\r\n$6\r\nRENAME\r\n$7\r\nmissing\r\n$3\r\ndst\r\n";
    let meta = parse_resp_command_arg_slices(rename_missing, &mut args).unwrap();
    let error = processor.execute_in_db(
        &args[..meta.argument_count],
        &mut response,
        DbName::default(),
    );
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();

    response.clear();
    let expire_src = b"*3\r\n$6\r\nEXPIRE\r\n$3\r\nsrc\r\n$3\r\n100\r\n";
    let meta = parse_resp_command_arg_slices(expire_src, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let copy = b"*3\r\n$4\r\nCOPY\r\n$3\r\nsrc\r\n$3\r\ndst\r\n";
    let meta = parse_resp_command_arg_slices(copy, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let get_dst = b"*2\r\n$3\r\nGET\r\n$3\r\ndst\r\n";
    let meta = parse_resp_command_arg_slices(get_dst, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$5\r\nvalue\r\n");

    response.clear();
    let ttl_dst = b"*2\r\n$3\r\nTTL\r\n$3\r\ndst\r\n";
    let meta = parse_resp_command_arg_slices(ttl_dst, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();

    response.clear();
    let copy_without_replace = b"*3\r\n$4\r\nCOPY\r\n$3\r\nsrc\r\n$3\r\ndst\r\n";
    let meta = parse_resp_command_arg_slices(copy_without_replace, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let copy_with_replace = b"*4\r\n$4\r\nCOPY\r\n$3\r\nsrc\r\n$3\r\ndst\r\n$7\r\nREPLACE\r\n";
    let meta = parse_resp_command_arg_slices(copy_with_replace, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let set_nx_again = b"*4\r\n$3\r\nSET\r\n$3\r\nkey\r\n$6\r\nvalue2\r\n$2\r\nNX\r\n";
    let meta = parse_resp_command_arg_slices(set_nx_again, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$-1\r\n");

    response.clear();
    let set_xx = b"*4\r\n$3\r\nSET\r\n$3\r\nkey\r\n$7\r\nupdated\r\n$2\r\nXX\r\n";
    let meta = parse_resp_command_arg_slices(set_xx, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let set_nx = b"*4\r\n$3\r\nSET\r\n$3\r\nkey\r\n$3\r\nstr\r\n$2\r\nNX\r\n";
    let meta = parse_resp_command_arg_slices(set_nx, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$-1\r\n");

    response.clear();
    let set_xx = b"*4\r\n$3\r\nSET\r\n$3\r\nkey\r\n$3\r\nstr\r\n$2\r\nXX\r\n";
    let meta = parse_resp_command_arg_slices(set_xx, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let get = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(get, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$3\r\nstr\r\n");

    response.clear();
    let hget = b"*3\r\n$4\r\nHGET\r\n$3\r\nkey\r\n$5\r\nfield\r\n";
    let meta = parse_resp_command_arg_slices(hget, &mut args).unwrap();
    let err = processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    thread::sleep(Duration::from_millis(20));

    response.clear();
    let get = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(get, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    thread::sleep(Duration::from_millis(20));
    let removed = processor.expire_stale_keys(16).unwrap();
    assert_eq!(removed, 1);

    response.clear();
    let get = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(get, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let pexpire = b"*3\r\n$7\r\nPEXPIRE\r\n$3\r\nkey\r\n$2\r\n10\r\n";
    let meta = parse_resp_command_arg_slices(pexpire, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    thread::sleep(Duration::from_millis(20));
    let removed = processor.expire_stale_keys(16).unwrap();
    assert_eq!(removed, 1);

    response.clear();
    let hget = b"*3\r\n$4\r\nHGET\r\n$3\r\nkey\r\n$5\r\nfield\r\n";
    let meta = parse_resp_command_arg_slices(hget, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$-1\r\n");
}

#[test]
fn expiration_scan_can_target_single_shard() {
    let processor = RequestProcessor::new_with_string_store_shards(4).unwrap();
    let key_shard_1 = find_key_for_shard(&processor, 1);
    let key_shard_2 = find_key_for_shard(&processor, 2);
    assert_ne!(key_shard_1, key_shard_2);
    assert_eq!(
        processor.string_store_shard_index_for_key(&key_shard_1),
        ShardIndex::new(1)
    );
    assert_eq!(
        processor.string_store_shard_index_for_key(&key_shard_2),
        ShardIndex::new(2)
    );

    let set_1 = encode_resp(&[b"SET", key_shard_1.as_slice(), b"v1", b"PX", b"10"]);
    assert_eq!(execute_frame(&processor, &set_1), b"+OK\r\n");
    let set_2 = encode_resp(&[b"SET", key_shard_2.as_slice(), b"v2", b"PX", b"10"]);
    assert_eq!(execute_frame(&processor, &set_2), b"+OK\r\n");

    thread::sleep(Duration::from_millis(20));
    assert_eq!(
        processor
            .expire_stale_keys_in_shard(ShardIndex::new(1), 16)
            .unwrap(),
        1
    );
    assert_eq!(
        processor
            .expire_stale_keys_in_shard(ShardIndex::new(1), 16)
            .unwrap(),
        0
    );
    assert_eq!(
        processor
            .expire_stale_keys_in_shard(ShardIndex::new(2), 16)
            .unwrap(),
        1
    );
}

#[test]
fn set_returns_error_for_invalid_expire_time() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 8];
    let mut response = Vec::new();

    let invalid = b"*5\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\nPX\r\n$1\r\n0\r\n";
    let meta = parse_resp_command_arg_slices(invalid, &mut args).unwrap();
    let err = processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let ttl = b"*2\r\n$3\r\nTTL\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(ttl, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":-1\r\n");

    response.clear();
    let expire = b"*3\r\n$6\r\nEXPIRE\r\n$3\r\nkey\r\n$1\r\n1\r\n";
    let meta = parse_resp_command_arg_slices(expire, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let pttl = b"*2\r\n$4\r\nPTTL\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(pttl, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    let remaining = parse_integer_response(&response);
    assert!((0..=1000).contains(&remaining));

    response.clear();
    let pexpire_now = b"*3\r\n$7\r\nPEXPIRE\r\n$3\r\nkey\r\n$1\r\n0\r\n";
    let meta = parse_resp_command_arg_slices(pexpire_now, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let get = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(get, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$-1\r\n");

    response.clear();
    let ttl_after_delete = b"*2\r\n$3\r\nTTL\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(ttl_after_delete, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let expire = b"*3\r\n$6\r\nEXPIRE\r\n$3\r\nkey\r\n$3\r\n100\r\n";
    let meta = parse_resp_command_arg_slices(expire, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let del = b"*2\r\n$3\r\nDEL\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(del, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let set_again = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$3\r\nbar\r\n";
    let meta = parse_resp_command_arg_slices(set_again, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let get = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(get, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let ttl = b"*2\r\n$3\r\nTTL\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(ttl, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":-1\r\n");

    response.clear();
    let expire = b"*3\r\n$6\r\nEXPIRE\r\n$3\r\nkey\r\n$1\r\n1\r\n";
    let meta = parse_resp_command_arg_slices(expire, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let pttl = b"*2\r\n$4\r\nPTTL\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(pttl, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    let remaining = parse_integer_response(&response);
    assert!((0..=1000).contains(&remaining));

    response.clear();
    let persist = b"*2\r\n$7\r\nPERSIST\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(persist, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let meta = parse_resp_command_arg_slices(ttl, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":-1\r\n");

    response.clear();
    let pexpire_now = b"*3\r\n$7\r\nPEXPIRE\r\n$3\r\nkey\r\n$1\r\n0\r\n";
    let meta = parse_resp_command_arg_slices(pexpire_now, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let hget = b"*3\r\n$4\r\nHGET\r\n$3\r\nkey\r\n$5\r\nfield\r\n";
    let meta = parse_resp_command_arg_slices(hget, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let ttl = b"*2\r\n$3\r\nTTL\r\n$7\r\nmissing\r\n";
    let meta = parse_resp_command_arg_slices(ttl, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":-2\r\n");

    response.clear();
    let pexpire = b"*3\r\n$7\r\nPEXPIRE\r\n$7\r\nmissing\r\n$2\r\n10\r\n";
    let meta = parse_resp_command_arg_slices(pexpire, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let pttl = b"*2\r\n$4\r\nPTTL\r\n$7\r\nmissing\r\n";
    let meta = parse_resp_command_arg_slices(pttl, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    let now_secs = current_unix_time_millis().unwrap() / 1000;
    let expireat_secs = (now_secs + 2).to_string();
    let expireat = encode_resp(&[b"EXPIREAT", b"key", expireat_secs.as_bytes()]);

    response.clear();
    let meta = parse_resp_command_arg_slices(&expireat, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let expiretime = b"*2\r\n$10\r\nEXPIRETIME\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(expiretime, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":-2\r\n");

    response.clear();
    let set = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
    let meta = parse_resp_command_arg_slices(set, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let pexpiretime_without_expire = b"*2\r\n$11\r\nPEXPIRETIME\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(pexpiretime_without_expire, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":-1\r\n");

    let now_millis = current_unix_time_millis().unwrap();
    let pexpireat_millis = (now_millis + 1500).to_string();
    let pexpireat = encode_resp(&[b"PEXPIREAT", b"key", pexpireat_millis.as_bytes()]);

    response.clear();
    let meta = parse_resp_command_arg_slices(&pexpireat, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let meta = parse_resp_command_arg_slices(pexpiretime_without_expire, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let persist = b"*2\r\n$7\r\nPERSIST\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(persist, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let ttl = b"*2\r\n$3\r\nTTL\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(ttl, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":-1\r\n");

    response.clear();
    let meta = parse_resp_command_arg_slices(persist, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let persist_missing = b"*2\r\n$7\r\nPERSIST\r\n$7\r\nmissing\r\n";
    let meta = parse_resp_command_arg_slices(persist_missing, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$-1\r\n");

    response.clear();
    let set = b"*3\r\n$3\r\nSET\r\n$4\r\nrkey\r\n$5\r\nvalue\r\n";
    let meta = parse_resp_command_arg_slices(set, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let dump = b"*2\r\n$4\r\nDUMP\r\n$4\r\nrkey\r\n";
    let meta = parse_resp_command_arg_slices(dump, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    let dump_payload = parse_bulk_payload(&response).expect("dump payload must exist");

    response.clear();
    let del = b"*2\r\n$3\r\nDEL\r\n$4\r\nrkey\r\n";
    let meta = parse_resp_command_arg_slices(del, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let restore = encode_resp(&[b"RESTORE", b"rkey", b"0", &dump_payload]);
    let meta = parse_resp_command_arg_slices(&restore, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let get = b"*2\r\n$3\r\nGET\r\n$4\r\nrkey\r\n";
    let meta = parse_resp_command_arg_slices(get, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$5\r\nvalue\r\n");

    response.clear();
    let restore_busy = encode_resp(&[b"RESTORE", b"rkey", b"0", &dump_payload]);
    let meta = parse_resp_command_arg_slices(&restore_busy, &mut args).unwrap();
    let err = processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap_err();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-BUSYKEY Target key name already exists.\r\n");

    response.clear();
    let restore_busy_invalid = encode_resp(&[b"RESTORE", b"rkey", b"0", b"..."]);
    let meta = parse_resp_command_arg_slices(&restore_busy_invalid, &mut args).unwrap();
    let err = processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap_err();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-BUSYKEY Target key name already exists.\r\n");

    response.clear();
    let restore_replace = encode_resp(&[b"RESTORE", b"rkey", b"1000", &dump_payload, b"REPLACE"]);
    let meta = parse_resp_command_arg_slices(&restore_replace, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let pttl = b"*2\r\n$4\r\nPTTL\r\n$4\r\nrkey\r\n";
    let meta = parse_resp_command_arg_slices(pttl, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    let ttl = parse_integer_response(&response);
    assert!(ttl > 0);
    assert!(ttl <= 1000);

    response.clear();
    let restore_asking =
        encode_resp(&[b"RESTORE-ASKING", b"rkey2", b"0", &dump_payload, b"REPLACE"]);
    let meta = parse_resp_command_arg_slices(&restore_asking, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let get_rkey2 = b"*2\r\n$3\r\nGET\r\n$5\r\nrkey2\r\n";
    let meta = parse_resp_command_arg_slices(get_rkey2, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$5\r\nvalue\r\n");

    response.clear();
    let restore_with_metadata = encode_resp(&[
        b"RESTORE",
        b"rmeta",
        b"0",
        &dump_payload,
        b"REPLACE",
        b"IDLETIME",
        b"1000",
        b"FREQ",
        b"100",
    ]);
    let meta = parse_resp_command_arg_slices(&restore_with_metadata, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");
    assert_command_integer(&processor, "OBJECT FREQ rmeta", 100);
    let idle_meta = execute_command_line(&processor, "OBJECT IDLETIME rmeta").unwrap();
    let idle_meta_value = parse_integer_response(&idle_meta);
    assert!(idle_meta_value >= 1000);

    response.clear();
    let restore_invalid = b"*4\r\n$7\r\nRESTORE\r\n$4\r\nbadk\r\n$1\r\n0\r\n$3\r\nbad\r\n";
    let meta = parse_resp_command_arg_slices(restore_invalid, &mut args).unwrap();
    let err = processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap_err();
    err.append_resp_error(&mut response);
    assert_eq!(
        response,
        b"-ERR DUMP payload version or checksum are wrong\r\n"
    );
}

#[test]
fn restore_with_absttl_in_the_past_matches_external_dump_scenario() {
    let processor = RequestProcessor::new().unwrap();
    let dump_payload =
        parse_bulk_payload(&execute_command_line(&processor, "DUMP missing").unwrap());
    assert!(dump_payload.is_none());

    assert_command_response(&processor, "SET key value", b"+OK\r\n");
    let dump_payload =
        parse_bulk_payload(&execute_command_line(&processor, "DUMP key").unwrap()).unwrap();

    assert_command_response(&processor, "DEBUG SET-ACTIVE-EXPIRE 0", b"+OK\r\n");
    let past_unix_millis = current_unix_time_millis().unwrap().saturating_sub(3000);
    let past_unix_millis_text = past_unix_millis.to_string();
    let restore = encode_resp(&[
        b"RESTORE",
        b"foo",
        past_unix_millis_text.as_bytes(),
        dump_payload.as_slice(),
        b"ABSTTL",
        b"REPLACE",
    ]);
    assert_eq!(execute_frame(&processor, &restore), b"+OK\r\n");
    assert_command_error(&processor, "DEBUG OBJECT foo", b"-ERR no such key\r\n");
    assert_command_response(&processor, "DEBUG SET-ACTIVE-EXPIRE 1", b"+OK\r\n");
}

#[test]
fn restore_legacy_stream_cgroups_rdb_ver_lt_10_matches_external_scenario() {
    let processor = RequestProcessor::new().unwrap();
    let payload = b"\x0F\x01\x10\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\xC3\x40\x4A\x40\x57\x16\x57\x00\x00\x00\x23\x00\x02\x01\x04\x01\x01\x01\x84\x64\x61\x74\x61\x05\x00\x01\x03\x01\x00\x20\x01\x03\x81\x61\x02\x04\x20\x0A\x00\x01\x40\x0A\x00\x62\x60\x0A\x00\x02\x40\x0A\x00\x63\x60\x0A\x40\x22\x01\x81\x64\x20\x0A\x40\x39\x20\x0A\x00\x65\x60\x0A\x00\x05\x40\x0A\x00\x66\x20\x0A\x00\xFF\x02\x06\x00\x02\x02\x67\x31\x05\x00\x04\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x3E\xF7\x83\x43\x7A\x01\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x3E\xF7\x83\x43\x7A\x01\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x3E\xF7\x83\x43\x7A\x01\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x00\x3E\xF7\x83\x43\x7A\x01\x00\x00\x01\x01\x03\x63\x31\x31\x3E\xF7\x83\x43\x7A\x01\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x00\x02\x67\x32\x00\x00\x00\x00\x09\x00\x3D\x52\xEF\x68\x67\x52\x1D\xFA";

    let restore = encode_resp(&[b"RESTORE", b"x", b"0", payload]);
    assert_eq!(execute_frame(&processor, &restore), b"+OK\r\n");

    let response = execute_command_line(&processor, "XINFO STREAM x FULL").unwrap();
    let root = parse_resp_test_value(&response);
    let full = resp_test_flat_map(&root);
    assert_eq!(
        resp_test_bulk(full[&b"max-deleted-entry-id".to_vec()]),
        b"0-0"
    );
    assert_eq!(resp_test_integer(full[&b"entries-added".to_vec()]), 2);

    let group_g1 = xinfo_full_group_by_name(&full, b"g1");
    assert_eq!(resp_test_integer(group_g1[&b"entries-read".to_vec()]), 1);
    assert_eq!(resp_test_integer(group_g1[&b"lag".to_vec()]), 1);

    let group_g2 = xinfo_full_group_by_name(&full, b"g2");
    assert_eq!(resp_test_integer(group_g2[&b"entries-read".to_vec()]), 0);
    assert_eq!(resp_test_integer(group_g2[&b"lag".to_vec()]), 2);
}

#[test]
fn restore_legacy_stream_cgroups_rdb_ver_lt_11_matches_external_scenario() {
    let processor = RequestProcessor::new().unwrap();
    let payload = b"\x13\x01\x10\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x01\x1D\x1D\x00\x00\x00\x0A\x00\x01\x01\x00\x01\x01\x01\x81\x66\x02\x00\x01\x02\x01\x00\x01\x00\x01\x81\x76\x02\x04\x01\xFF\x01\x01\x01\x01\x01\x00\x00\x01\x01\x01\x67\x01\x01\x01\x01\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x01\xF5\x5A\x71\xC7\x84\x01\x00\x00\x01\x01\x05\x41\x6C\x69\x63\x65\xF5\x5A\x71\xC7\x84\x01\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x01\x0B\x00\xA7\xA9\x14\xA5\x27\xFF\x9B\x9B";

    let restore = encode_resp(&[b"RESTORE", b"x", b"0", payload]);
    assert_eq!(execute_frame(&processor, &restore), b"+OK\r\n");

    let response = execute_command_line(&processor, "XINFO STREAM x FULL").unwrap();
    let root = parse_resp_test_value(&response);
    let full = resp_test_flat_map(&root);
    let group = xinfo_full_group_by_name(&full, b"g");
    let consumers = resp_test_array(group[&b"consumers".to_vec()]);
    assert_eq!(consumers.len(), 1);
    let consumer = resp_test_flat_map(&consumers[0]);
    assert_eq!(
        resp_test_integer(consumer[&b"seen-time".to_vec()]),
        resp_test_integer(consumer[&b"active-time".to_vec()])
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let hset = b"*4\r\n$4\r\nHSET\r\n$4\r\nhkey\r\n$5\r\nfield\r\n$5\r\nvalue\r\n";
    let meta = parse_resp_command_arg_slices(hset, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let dbsize = b"*1\r\n$6\r\nDBSIZE\r\n";
    let meta = parse_resp_command_arg_slices(dbsize, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
    assert_eq!(queued.len(), 1);
    assert_eq!(queued[0].db, DbName::default());
    assert_eq!(queued[0].key.as_slice(), b"lazy:key");
    assert!(
        processor
            .take_lazy_expired_keys_for_replication()
            .is_empty()
    );
}

#[test]
fn lazy_expire_replication_queue_preserves_auxiliary_db_name() {
    let processor = RequestProcessor::new().unwrap();
    let db = DbName::new(9);

    assert_command_response(&processor, "CONFIG SET databases 10", b"+OK\r\n");
    assert_command_response_in_db(&processor, "DEBUG SET-ACTIVE-EXPIRE 0", b"+OK\r\n", db);
    assert_command_response_in_db(&processor, "PSETEX aux:key 1 value", b"+OK\r\n", db);
    thread::sleep(Duration::from_millis(10));

    assert_command_response_in_db(&processor, "GET aux:key", b"$-1\r\n", db);
    let queued = processor.take_lazy_expired_keys_for_replication();
    assert_eq!(queued.len(), 1);
    assert_eq!(queued[0].db, db);
    assert_eq!(queued[0].key.as_slice(), b"aux:key");
}

#[test]
fn script_replication_effects_preserve_selected_db() {
    let processor = RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap();
    let db = DbName::new(9);

    assert_command_response(&processor, "CONFIG SET databases 10", b"+OK\r\n");
    let response = execute_command_args_in_db(
        &processor,
        &[
            b"EVAL",
            b"return redis.call('SET','script:key','db9')",
            b"0",
        ],
        db,
    );
    assert_eq!(response, b"+OK\r\n");

    let effects = processor.take_script_replication_effects();
    assert_eq!(effects.len(), 1);
    assert_eq!(effects[0].selected_db, db);
}

#[test]
fn db_scoped_encoding_state_does_not_leak_between_databases() {
    let processor = RequestProcessor::new().unwrap();
    let db0 = DbName::default();
    let db9 = DbName::new(9);

    assert_command_response(&processor, "CONFIG SET databases 10", b"+OK\r\n");
    assert_command_response_in_db(&processor, "SET shared 1", b"+OK\r\n", db0);
    assert_command_response_in_db(&processor, "SET shared 1", b"+OK\r\n", db9);
    assert_command_response_in_db(&processor, "APPEND shared x", b":2\r\n", db9);
    assert_command_response_in_db(&processor, "OBJECT ENCODING shared", b"$3\r\nint\r\n", db0);
    assert_command_response_in_db(&processor, "OBJECT ENCODING shared", b"$3\r\nraw\r\n", db9);

    assert_command_response(
        &processor,
        "CONFIG SET set-max-listpack-value 1",
        b"+OK\r\n",
    );
    assert_command_response_in_db(&processor, "SADD shared:set 1 2", b":2\r\n", db0);
    assert_command_response_in_db(&processor, "SADD shared:set aa bb", b":2\r\n", db9);
    assert_command_response_in_db(
        &processor,
        "OBJECT ENCODING shared:set",
        b"$6\r\nintset\r\n",
        db0,
    );
    assert_command_response_in_db(
        &processor,
        "OBJECT ENCODING shared:set",
        b"$9\r\nhashtable\r\n",
        db9,
    );
}

#[test]
fn db_scoped_object_access_metadata_does_not_leak_between_databases() {
    let processor = RequestProcessor::new().unwrap();
    let db0 = DbName::default();
    let db9 = DbName::new(9);

    assert_command_response(&processor, "CONFIG SET databases 10", b"+OK\r\n");
    assert_command_response_in_db(&processor, "SET shared value", b"+OK\r\n", db0);
    assert_command_response_in_db(&processor, "SET shared value", b"+OK\r\n", db9);

    processor.set_key_idle_seconds(DbKeyRef::new(db0, b"shared"), 60);
    processor.set_key_frequency(DbKeyRef::new(db0, b"shared"), 42);
    processor.record_key_access(DbKeyRef::new(db9, b"shared"), true);

    assert_command_response_in_db(&processor, "OBJECT FREQ shared", b":42\r\n", db0);
    assert_command_response_in_db(&processor, "OBJECT FREQ shared", b":0\r\n", db9);

    let db0_idle = parse_integer_response(&execute_command_line_in_db(
        &processor,
        "OBJECT IDLETIME shared",
        db0,
    ));
    let db9_idle = parse_integer_response(&execute_command_line_in_db(
        &processor,
        "OBJECT IDLETIME shared",
        db9,
    ));
    assert!(
        db0_idle >= 59,
        "db0 idle time should remain scoped: {db0_idle}"
    );
    assert!(
        db9_idle <= 1,
        "db9 idle time should reflect its own access: {db9_idle}"
    );
}

#[test]
fn string_read_and_bit_commands_are_scoped_by_selected_db_without_db0_fallback() {
    let processor = RequestProcessor::new().unwrap();
    let db0 = DbName::default();
    let db9 = DbName::new(9);

    assert_command_response(&processor, "CONFIG SET databases 10", b"+OK\r\n");
    assert_command_response_in_db(&processor, "SET shared hello", b"+OK\r\n", db0);
    assert_command_response_in_db(&processor, "SET shared world", b"+OK\r\n", db9);

    assert_command_response_in_db(&processor, "GET shared", b"$5\r\nhello\r\n", db0);
    assert_command_response_in_db(&processor, "GET shared", b"$5\r\nworld\r\n", db9);
    assert_command_response_in_db(&processor, "STRLEN shared", b":5\r\n", db0);
    assert_command_response_in_db(&processor, "STRLEN shared", b":5\r\n", db9);

    assert_command_response_in_db(&processor, "SETRANGE shared 5 !", b":6\r\n", db9);
    assert_command_response_in_db(&processor, "GETRANGE shared 0 -1", b"$5\r\nhello\r\n", db0);
    assert_command_response_in_db(&processor, "GETRANGE shared 0 -1", b"$6\r\nworld!\r\n", db9);

    assert_command_response_in_db(&processor, "SETBIT bits 0 1", b":0\r\n", db9);
    assert_command_response_in_db(&processor, "GETBIT bits 0", b":0\r\n", db0);
    assert_command_response_in_db(&processor, "GETBIT bits 0", b":1\r\n", db9);
    assert_command_response_in_db(&processor, "BITCOUNT bits", b":0\r\n", db0);
    assert_command_response_in_db(&processor, "BITCOUNT bits", b":1\r\n", db9);
    assert_command_response_in_db(&processor, "BITPOS bits 1", b":-1\r\n", db0);
    assert_command_response_in_db(&processor, "BITPOS bits 1", b":0\r\n", db9);
}

#[test]
fn string_advanced_commands_are_scoped_by_selected_db_without_db0_fallback() {
    let processor = RequestProcessor::new().unwrap();
    let db0 = DbName::default();
    let db9 = DbName::new(9);

    assert_command_response(&processor, "CONFIG SET databases 10", b"+OK\r\n");

    assert_command_response_in_db(&processor, "SET shared alpha", b"+OK\r\n", db0);
    assert_command_response_in_db(&processor, "SET shared beta", b"+OK\r\n", db9);
    assert_command_response_in_db(&processor, "APPEND shared !", b":5\r\n", db9);
    assert_command_response_in_db(&processor, "GET shared", b"$5\r\nalpha\r\n", db0);
    assert_command_response_in_db(&processor, "GET shared", b"$5\r\nbeta!\r\n", db9);

    assert_command_response_in_db(&processor, "SET ttl zero EX 60", b"+OK\r\n", db0);
    assert_command_response_in_db(&processor, "SET ttl nine EX 60", b"+OK\r\n", db9);
    assert_command_response_in_db(&processor, "GETEX ttl PERSIST", b"$4\r\nnine\r\n", db9);
    let db0_pttl = parse_integer_response(&execute_command_line_in_db(&processor, "PTTL ttl", db0));
    assert!(db0_pttl > 0, "db0 ttl should remain intact: {db0_pttl}");
    assert_command_response_in_db(&processor, "PTTL ttl", b":-1\r\n", db9);

    assert_command_response_in_db(&processor, "SET num 1.25", b"+OK\r\n", db0);
    assert_command_response_in_db(&processor, "SET num 2.5", b"+OK\r\n", db9);
    assert_command_response_in_db(&processor, "INCRBYFLOAT num 0.5", b"$1\r\n3\r\n", db9);
    assert_command_response_in_db(&processor, "GET num", b"$4\r\n1.25\r\n", db0);
    assert_command_response_in_db(&processor, "GET num", b"$1\r\n3\r\n", db9);

    assert_command_response_in_db(&processor, "BITFIELD bf SET u8 0 255", b"*1\r\n:0\r\n", db9);
    assert_command_response_in_db(&processor, "BITFIELD bf GET u8 0", b"*1\r\n:0\r\n", db0);
    assert_command_response_in_db(&processor, "BITFIELD bf GET u8 0", b"*1\r\n:255\r\n", db9);

    assert_command_response_in_db(&processor, "SET src1 A", b"+OK\r\n", db0);
    assert_command_response_in_db(&processor, "SET src2 B", b"+OK\r\n", db0);
    assert_command_response_in_db(&processor, "SET dest X", b"+OK\r\n", db0);
    assert_command_response_in_db(&processor, "SET src1 a", b"+OK\r\n", db9);
    assert_command_response_in_db(&processor, "SET src2 b", b"+OK\r\n", db9);
    assert_command_response_in_db(&processor, "BITOP OR dest src1 src2", b":1\r\n", db9);
    assert_command_response_in_db(&processor, "GET dest", b"$1\r\nX\r\n", db0);
    assert_command_response_in_db(&processor, "GET dest", b"$1\r\nc\r\n", db9);

    assert_command_response_in_db(&processor, "SET lcs1 abc", b"+OK\r\n", db0);
    assert_command_response_in_db(&processor, "SET lcs2 xyz", b"+OK\r\n", db0);
    assert_command_response_in_db(&processor, "SET lcs1 ohmytext", b"+OK\r\n", db9);
    assert_command_response_in_db(&processor, "SET lcs2 mynewtext", b"+OK\r\n", db9);
    assert_command_response_in_db(&processor, "LCS lcs1 lcs2 LEN", b":0\r\n", db0);
    assert_command_response_in_db(&processor, "LCS lcs1 lcs2 LEN", b":6\r\n", db9);
}

#[test]
fn string_write_and_delete_commands_are_scoped_by_selected_db_without_db0_fallback() {
    let processor = RequestProcessor::new().unwrap();
    let db0 = DbName::default();
    let db9 = DbName::new(9);

    assert_command_response(&processor, "CONFIG SET databases 10", b"+OK\r\n");

    assert_command_response_in_db(&processor, "SET shared zero", b"+OK\r\n", db0);
    assert_command_response_in_db(&processor, "SET shared nine", b"+OK\r\n", db9);
    assert_command_response_in_db(&processor, "GETSET shared newer", b"$4\r\nnine\r\n", db9);
    assert_command_response_in_db(&processor, "GET shared", b"$4\r\nzero\r\n", db0);
    assert_command_response_in_db(&processor, "GET shared", b"$5\r\nnewer\r\n", db9);

    assert_command_response_in_db(&processor, "SET victim alive", b"+OK\r\n", db0);
    assert_command_response_in_db(&processor, "SET victim dead", b"+OK\r\n", db9);
    assert_command_response_in_db(&processor, "GETDEL victim", b"$4\r\ndead\r\n", db9);
    assert_command_response_in_db(&processor, "GET victim", b"$5\r\nalive\r\n", db0);
    assert_command_response_in_db(&processor, "GET victim", b"$-1\r\n", db9);

    assert_command_response_in_db(&processor, "SET doomed stay", b"+OK\r\n", db0);
    assert_command_response_in_db(&processor, "SET doomed go", b"+OK\r\n", db9);
    assert_command_response_in_db(&processor, "DELEX doomed IFEQ go", b":1\r\n", db9);
    assert_command_response_in_db(&processor, "GET doomed", b"$4\r\nstay\r\n", db0);
    assert_command_response_in_db(&processor, "GET doomed", b"$-1\r\n", db9);

    assert_command_response_in_db(&processor, "SET digest alpha", b"+OK\r\n", db0);
    assert_command_response_in_db(&processor, "SET digest omega", b"+OK\r\n", db9);
    let db0_digest = execute_command_line_in_db(&processor, "DIGEST digest", db0);
    let db9_digest = execute_command_line_in_db(&processor, "DIGEST digest", db9);
    assert_ne!(db0_digest, db9_digest, "digest must stay DB-scoped");

    assert_command_response_in_db(&processor, "SET gone keep", b"+OK\r\n", db0);
    assert_command_response_in_db(&processor, "SET gone drop", b"+OK\r\n", db9);
    assert_command_response_in_db(&processor, "DEL gone", b":1\r\n", db9);
    assert_command_response_in_db(&processor, "EXISTS gone", b":1\r\n", db0);
    assert_command_response_in_db(&processor, "EXISTS gone", b":0\r\n", db9);

    assert_command_response_in_db(&processor, "SET unlinkme keep", b"+OK\r\n", db0);
    assert_command_response_in_db(&processor, "SET unlinkme drop", b"+OK\r\n", db9);
    assert_command_response_in_db(&processor, "UNLINK unlinkme", b":1\r\n", db9);
    assert_command_response_in_db(&processor, "GET unlinkme", b"$4\r\nkeep\r\n", db0);
    assert_command_response_in_db(&processor, "GET unlinkme", b"$-1\r\n", db9);

    assert_command_response_in_db(&processor, "SET touchme yes", b"+OK\r\n", db9);
    assert_command_response_in_db(&processor, "TOUCH touchme", b":0\r\n", db0);
    assert_command_response_in_db(&processor, "TOUCH touchme", b":1\r\n", db9);
}

#[test]
fn watch_versions_are_scoped_by_explicit_db() {
    let processor = RequestProcessor::new().unwrap();
    let db0 = DbName::default();
    let db9 = DbName::new(9);

    assert_command_response(&processor, "CONFIG SET databases 10", b"+OK\r\n");
    let version0_before = processor.watch_key_version_in_db(db0, b"shared");
    let version9_before = processor.watch_key_version_in_db(db9, b"shared");

    assert_command_response_in_db(&processor, "SET shared db9", b"+OK\r\n", db9);

    let version0_after = processor.watch_key_version_in_db(db0, b"shared");
    let version9_after = processor.watch_key_version_in_db(db9, b"shared");
    assert_eq!(version0_after, version0_before);
    assert_ne!(version9_after, version9_before);
}

#[test]
fn capture_watched_key_expires_only_target_db_without_db0_fallback() {
    let processor = RequestProcessor::new().unwrap();
    let db0 = DbName::default();
    let db9 = DbName::new(9);

    assert_command_response(&processor, "SET shared db0", b"+OK\r\n");
    processor
        .write_string_value(DbKeyRef::new(db9, b"shared"), b"db9", None)
        .unwrap();
    processor.set_string_expiration_deadline(
        DbKeyRef::new(db9, b"shared"),
        Some(current_instant() - Duration::from_millis(1)),
    );

    let watched = processor.capture_watched_key(db9, b"shared").unwrap();

    assert_eq!(watched.db, db9);
    assert_eq!(watched.fingerprint, WatchedValueFingerprint::new(0));
    assert_eq!(
        processor.watch_key_version_in_db(db9, b"shared"),
        watched.version
    );
    assert_eq!(
        processor
            .read_string_value(DbKeyRef::new(db0, b"shared"))
            .unwrap()
            .unwrap(),
        b"db0"
    );
    assert!(
        processor
            .read_string_value(DbKeyRef::new(db9, b"shared"))
            .unwrap()
            .is_none()
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let hset = b"*4\r\n$4\r\nHSET\r\n$4\r\nhkey\r\n$5\r\nfield\r\n$5\r\nvalue\r\n";
    let meta = parse_resp_command_arg_slices(hset, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let flushdb = b"*1\r\n$7\r\nFLUSHDB\r\n";
    let meta = parse_resp_command_arg_slices(flushdb, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let dbsize = b"*1\r\n$6\r\nDBSIZE\r\n";
    let meta = parse_resp_command_arg_slices(dbsize, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let sadd = b"*4\r\n$4\r\nSADD\r\n$4\r\nsset\r\n$1\r\na\r\n$1\r\nb\r\n";
    let meta = parse_resp_command_arg_slices(sadd, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":2\r\n");

    response.clear();
    let flushall = b"*1\r\n$8\r\nFLUSHALL\r\n";
    let meta = parse_resp_command_arg_slices(flushall, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let dbsize = b"*1\r\n$6\r\nDBSIZE\r\n";
    let meta = parse_resp_command_arg_slices(dbsize, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":0\r\n");
}

#[test]
fn flushall_modes_update_lazyfree_stats_and_info_fields() {
    let processor = RequestProcessor::new().unwrap();

    assert_command_response(&processor, "SET x 1", b"+OK\r\n");
    assert_command_response(&processor, "SET y 2", b"+OK\r\n");
    assert_command_response(&processor, "CONFIG RESETSTAT", b"+OK\r\n");

    assert_command_response(&processor, "FLUSHALL ASYNC", b"+OK\r\n");
    let info_after_async = parse_bulk_payload(&execute_frame(
        &processor,
        &encode_resp(&[b"INFO", b"stats"]),
    ))
    .unwrap();
    let info_after_async_text = String::from_utf8_lossy(&info_after_async);
    assert!(info_after_async_text.contains("lazyfree_pending_objects:0"));
    assert!(info_after_async_text.contains("lazyfreed_objects:2"));

    assert_command_response(&processor, "SET x 1", b"+OK\r\n");
    assert_command_response(&processor, "SET y 2", b"+OK\r\n");
    assert_command_response(&processor, "CONFIG RESETSTAT", b"+OK\r\n");

    assert_command_response(&processor, "FLUSHALL SYNC", b"+OK\r\n");
    let info_after_sync = parse_bulk_payload(&execute_frame(
        &processor,
        &encode_resp(&[b"INFO", b"stats"]),
    ))
    .unwrap();
    let info_after_sync_text = String::from_utf8_lossy(&info_after_sync);
    assert!(info_after_sync_text.contains("lazyfree_pending_objects:0"));
    assert!(info_after_sync_text.contains("lazyfreed_objects:0"));
}

#[test]
fn flushall_default_in_transaction_context_does_not_record_lazyfree() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 8];
    let mut response = Vec::new();

    assert_command_response(&processor, "SET x 1", b"+OK\r\n");
    assert_command_response(&processor, "SET y 2", b"+OK\r\n");
    assert_command_response(&processor, "CONFIG RESETSTAT", b"+OK\r\n");

    let flushall = b"*1\r\n$8\r\nFLUSHALL\r\n";
    let meta = parse_resp_command_arg_slices(flushall, &mut args).unwrap();
    processor
        .execute_with_client_no_touch_in_transaction_in_db(
            &args[..meta.argument_count],
            &mut response,
            false,
            None,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    let info_stats = parse_bulk_payload(&execute_frame(
        &processor,
        &encode_resp(&[b"INFO", b"stats"]),
    ))
    .unwrap();
    let info_stats_text = String::from_utf8_lossy(&info_stats);
    assert!(info_stats_text.contains("lazyfree_pending_objects:0"));
    assert!(info_stats_text.contains("lazyfreed_objects:0"));
}

#[test]
fn info_dbsize_and_command_responses_are_generated() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 8];
    let mut response = Vec::new();

    let set = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
    let meta = parse_resp_command_arg_slices(set, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();

    response.clear();
    let dbsize = b"*1\r\n$6\r\nDBSIZE\r\n";
    let meta = parse_resp_command_arg_slices(dbsize, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let info = b"*1\r\n$4\r\nINFO\r\n";
    let meta = parse_resp_command_arg_slices(info, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
    assert!(info_default_text.contains("redis_version:8.4.0"));
    assert!(info_default_text.contains("redis_git_sha1:"));
    assert!(info_default_text.contains("process_id:"));
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
    assert!(info_all_text.contains("# Keysizes"));
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
fn info_keysizes_reports_type_histograms_with_power_of_two_bins() {
    let processor = RequestProcessor::new().unwrap();

    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"SET", b"s_empty", b""])),
        b"+OK\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"SET", b"s_16", b"0123456789ABCDEF"]),
        ),
        b"+OK\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"RPUSH", b"l", b"1", b"2", b"3", b"4", b"5"]),
        ),
        b":5\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"SADD", b"set", b"a", b"b", b"c", b"d", b"e"]),
        ),
        b":5\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"ZADD", b"z", b"1", b"a", b"2", b"b", b"3", b"c"]),
        ),
        b":3\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"HSET", b"h", b"f1", b"v1", b"f2", b"v2", b"f3", b"v3",]),
        ),
        b":3\r\n"
    );

    let info_keysizes = parse_bulk_payload(&execute_frame(
        &processor,
        &encode_resp(&[b"INFO", b"KEYSIZES"]),
    ))
    .expect("INFO KEYSIZES returns bulk payload");
    let info_keysizes_text = String::from_utf8_lossy(&info_keysizes);
    assert!(info_keysizes_text.contains("# Keysizes"));
    assert!(info_keysizes_text.contains("db0_distrib_strings_sizes:0=1,16=1"));
    assert!(info_keysizes_text.contains("db0_distrib_lists_items:4=1"));
    assert!(info_keysizes_text.contains("db0_distrib_sets_items:4=1"));
    assert!(info_keysizes_text.contains("db0_distrib_zsets_items:2=1"));
    assert!(info_keysizes_text.contains("db0_distrib_hashes_items:2=1"));
}

#[test]
fn info_keyspace_reports_keys_and_expires() {
    let processor = RequestProcessor::new().unwrap();

    // Empty database: no db0 line.
    let info_ks = parse_bulk_payload(&execute_frame(
        &processor,
        &encode_resp(&[b"INFO", b"keyspace"]),
    ))
    .expect("INFO keyspace returns bulk payload");
    let info_ks_text = String::from_utf8_lossy(&info_ks);
    assert!(info_ks_text.contains("# Keyspace"));
    assert!(!info_ks_text.contains("db0:"));

    // Add some keys.
    execute_frame(&processor, &encode_resp(&[b"SET", b"a", b"1"]));
    execute_frame(&processor, &encode_resp(&[b"SET", b"b", b"2"]));
    execute_frame(&processor, &encode_resp(&[b"HSET", b"h", b"f", b"v"]));

    let info_ks = parse_bulk_payload(&execute_frame(
        &processor,
        &encode_resp(&[b"INFO", b"keyspace"]),
    ))
    .expect("INFO keyspace returns bulk payload");
    let info_ks_text = String::from_utf8_lossy(&info_ks);
    assert!(info_ks_text.contains("db0:keys=3,expires=0,avg_ttl=0"));

    // Set an expiration on one key.
    execute_frame(&processor, &encode_resp(&[b"PEXPIRE", b"a", b"100000"]));
    let info_ks = parse_bulk_payload(&execute_frame(
        &processor,
        &encode_resp(&[b"INFO", b"keyspace"]),
    ))
    .expect("INFO keyspace returns bulk payload");
    let info_ks_text = String::from_utf8_lossy(&info_ks);
    assert!(info_ks_text.contains("db0:keys=3,expires=1,avg_ttl=0"));

    // Keyspace is part of default INFO output.
    let info_default = parse_bulk_payload(&execute_frame(&processor, &encode_resp(&[b"INFO"])))
        .expect("INFO returns bulk payload");
    let info_default_text = String::from_utf8_lossy(&info_default);
    assert!(info_default_text.contains("# Keyspace"));
    assert!(info_default_text.contains("db0:keys=3,expires=1,avg_ttl=0"));
}

#[test]
fn move_to_nonzero_db_updates_keysizes_histograms() {
    let processor = RequestProcessor::new().unwrap();
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"RPUSH", b"l1", b"1", b"2", b"3", b"4"]),
        ),
        b":4\r\n"
    );
    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"RPUSH", b"l2", b"1"])),
        b":1\r\n"
    );
    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"MOVE", b"l1", b"1"])),
        b":1\r\n"
    );

    let info_keysizes = parse_bulk_payload(&execute_frame(
        &processor,
        &encode_resp(&[b"INFO", b"KEYSIZES"]),
    ))
    .expect("INFO KEYSIZES returns bulk payload");
    let info_keysizes_text = String::from_utf8_lossy(&info_keysizes);
    assert!(info_keysizes_text.contains("db0_distrib_lists_items:1=1"));
    assert!(info_keysizes_text.contains("db1_distrib_lists_items:4=1"));

    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"FLUSHALL"])),
        b"+OK\r\n"
    );
    let info_after_flushall = parse_bulk_payload(&execute_frame(
        &processor,
        &encode_resp(&[b"INFO", b"KEYSIZES"]),
    ))
    .expect("INFO KEYSIZES returns bulk payload");
    let info_after_flushall_text = String::from_utf8_lossy(&info_after_flushall);
    assert!(!info_after_flushall_text.contains("db1_distrib_lists_items:4=1"));
}

#[test]
fn info_keysizes_keeps_db0_histogram_when_current_client_selected_db_is_nonzero() {
    let processor = RequestProcessor::new().unwrap();
    let db1 = DbName::new(1);

    assert_eq!(
        execute_command_line_in_db(&processor, "RPUSH l1 1 2 3 4", DbName::default()),
        b":4\r\n"
    );
    assert_eq!(
        execute_command_line_in_db(&processor, "ZADD z1 1 A", db1),
        b":1\r\n"
    );

    let info_before_swap = parse_bulk_payload(&execute_command_line_in_db(
        &processor,
        "INFO KEYSIZES",
        db1,
    ))
    .expect("INFO KEYSIZES returns bulk payload");
    let info_before_swap_text = String::from_utf8_lossy(&info_before_swap);
    assert!(info_before_swap_text.contains("db0_distrib_lists_items:4=1"));
    assert!(info_before_swap_text.contains("db1_distrib_zsets_items:1=1"));
    assert!(!info_before_swap_text.contains("db0_distrib_zsets_items:1=1"));

    assert_eq!(
        execute_command_line_in_db(&processor, "SWAPDB 0 1", db1),
        b"+OK\r\n"
    );

    let info_after_swap = parse_bulk_payload(&execute_command_line_in_db(
        &processor,
        "INFO KEYSIZES",
        db1,
    ))
    .expect("INFO KEYSIZES returns bulk payload");
    let info_after_swap_text = String::from_utf8_lossy(&info_after_swap);
    assert!(info_after_swap_text.contains("db0_distrib_zsets_items:1=1"));
    assert!(info_after_swap_text.contains("db1_distrib_lists_items:4=1"));
    assert!(!info_after_swap_text.contains("db0_distrib_lists_items:4=1"));
}

#[test]
fn swapdb_non_zero_databases_swap_content_with_matching_key_names() {
    let processor = RequestProcessor::new().unwrap();
    let db1 = DbName::new(1);
    let db2 = DbName::new(2);
    let run = |command: &str, selected_db: DbName| -> Vec<u8> {
        match crate::testkit::execute_command_line_in_db(&processor, command, selected_db) {
            Ok(response) => response,
            Err(CommandHarnessError::Request(error)) => {
                let mut response = Vec::new();
                error.append_resp_error(&mut response);
                response
            }
            Err(error) => panic!("command failed: `{command}` ({selected_db:?}): {error}"),
        }
    };

    assert_eq!(run("SET shared v1", db1), b"+OK\r\n");
    assert_eq!(run("LPUSH shared one", db2), b":1\r\n");

    assert_eq!(run("TYPE shared", db1), b"+string\r\n");
    assert_eq!(run("TYPE shared", db2), b"+list\r\n");
    assert_eq!(run("SWAPDB 1 2", db1), b"+OK\r\n");

    assert_eq!(run("TYPE shared", db1), b"+list\r\n");
    assert_eq!(run("LRANGE shared 0 -1", db1), b"*1\r\n$3\r\none\r\n");
    assert_eq!(
        run("GET shared", db1),
        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
    );
    assert_eq!(run("TYPE shared", db2), b"+string\r\n");
    assert_eq!(run("GET shared", db2), b"$2\r\nv1\r\n");
}

#[test]
fn swapdb_zero_and_nonzero_databases_swap_content_with_matching_key_names() {
    let processor = RequestProcessor::new().unwrap();
    let db0 = DbName::default();
    let db1 = DbName::new(1);
    let run = |command: &str, selected_db: DbName| -> Vec<u8> {
        match crate::testkit::execute_command_line_in_db(&processor, command, selected_db) {
            Ok(response) => response,
            Err(CommandHarnessError::Request(error)) => {
                let mut response = Vec::new();
                error.append_resp_error(&mut response);
                response
            }
            Err(error) => panic!("command failed: `{command}` ({selected_db:?}): {error}"),
        }
    };

    assert_eq!(run("CONFIG SET databases 2", db0), b"+OK\r\n");
    assert_eq!(run("SET shared main", db0), b"+OK\r\n");
    assert_eq!(run("LPUSH shared side", db1), b":1\r\n");

    assert_eq!(run("TYPE shared", db0), b"+string\r\n");
    assert_eq!(run("TYPE shared", db1), b"+list\r\n");
    assert_eq!(run("SWAPDB 0 1", db0), b"+OK\r\n");

    assert_eq!(run("TYPE shared", db0), b"+list\r\n");
    assert_eq!(run("LRANGE shared 0 -1", db0), b"*1\r\n$4\r\nside\r\n");
    assert_eq!(
        run("GET shared", db0),
        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
    );
    assert_eq!(run("TYPE shared", db1), b"+string\r\n");
    assert_eq!(run("GET shared", db1), b"$4\r\nmain\r\n");
}

#[test]
fn swapdb_zero_and_nonzero_preserves_quicklist_encoding_and_debug_object_state() {
    let processor = RequestProcessor::new().unwrap();
    let db0 = DbName::default();
    let db1 = DbName::new(1);
    let large_quicklist_value = vec![b'x'; 8192];

    assert_command_response(&processor, "CONFIG SET databases 2", b"+OK\r\n");
    assert_eq!(
        execute_command_args_in_db(
            &processor,
            &[b"RPUSH", b"lst", b"a", large_quicklist_value.as_slice()],
            db0,
        ),
        b":2\r\n"
    );
    assert_eq!(
        execute_command_line_in_db(&processor, "RPUSH lst a b", db1),
        b":2\r\n"
    );

    assert_command_response_in_db(
        &processor,
        "OBJECT ENCODING lst",
        b"$9\r\nquicklist\r\n",
        db0,
    );
    assert_command_response_in_db(
        &processor,
        "OBJECT ENCODING lst",
        b"$8\r\nlistpack\r\n",
        db1,
    );

    let debug_before_db0 = parse_bulk_payload(&execute_command_line_in_db(
        &processor,
        "DEBUG OBJECT lst",
        db0,
    ))
    .expect("DEBUG OBJECT returns bulk payload");
    let debug_before_db1 = parse_bulk_payload(&execute_command_line_in_db(
        &processor,
        "DEBUG OBJECT lst",
        db1,
    ))
    .expect("DEBUG OBJECT returns bulk payload");
    let debug_before_db0_text = String::from_utf8_lossy(&debug_before_db0);
    let debug_before_db1_text = String::from_utf8_lossy(&debug_before_db1);
    assert!(debug_before_db0_text.contains("ql_listpack_max:"));
    assert!(!debug_before_db1_text.contains("ql_listpack_max:"));

    assert_eq!(
        execute_command_line_in_db(&processor, "SWAPDB 0 1", db0),
        b"+OK\r\n"
    );

    assert_command_response_in_db(
        &processor,
        "OBJECT ENCODING lst",
        b"$8\r\nlistpack\r\n",
        db0,
    );
    assert_command_response_in_db(
        &processor,
        "OBJECT ENCODING lst",
        b"$9\r\nquicklist\r\n",
        db1,
    );

    let debug_after_db0 = parse_bulk_payload(&execute_command_line_in_db(
        &processor,
        "DEBUG OBJECT lst",
        db0,
    ))
    .expect("DEBUG OBJECT returns bulk payload");
    let debug_after_db1 = parse_bulk_payload(&execute_command_line_in_db(
        &processor,
        "DEBUG OBJECT lst",
        db1,
    ))
    .expect("DEBUG OBJECT returns bulk payload");
    let debug_after_db0_text = String::from_utf8_lossy(&debug_after_db0);
    let debug_after_db1_text = String::from_utf8_lossy(&debug_after_db1);
    assert!(!debug_after_db0_text.contains("ql_listpack_max:"));
    assert!(debug_after_db1_text.contains("ql_listpack_max:"));
}

#[test]
fn swapdb_zero_and_nonzero_preserves_object_access_metadata() {
    let processor = RequestProcessor::new().unwrap();
    let db0 = DbName::default();
    let db1 = DbName::new(1);

    assert_command_response(&processor, "CONFIG SET databases 2", b"+OK\r\n");
    assert_command_response_in_db(&processor, "SET shared value0", b"+OK\r\n", db0);
    assert_command_response_in_db(&processor, "SET shared value1", b"+OK\r\n", db1);

    processor.set_key_idle_seconds(DbKeyRef::new(db0, b"shared"), 60);
    processor.set_key_frequency(DbKeyRef::new(db0, b"shared"), 42);
    processor.record_key_access(DbKeyRef::new(db1, b"shared"), true);

    assert_eq!(
        execute_command_line_in_db(&processor, "SWAPDB 0 1", db0),
        b"+OK\r\n"
    );

    assert_command_response_in_db(&processor, "OBJECT FREQ shared", b":0\r\n", db0);
    assert_command_response_in_db(&processor, "OBJECT FREQ shared", b":42\r\n", db1);

    let db0_idle = parse_integer_response(&execute_command_line_in_db(
        &processor,
        "OBJECT IDLETIME shared",
        db0,
    ));
    let db1_idle = parse_integer_response(&execute_command_line_in_db(
        &processor,
        "OBJECT IDLETIME shared",
        db1,
    ));
    assert!(
        db0_idle <= 1,
        "db0 idle time should swap with metadata: {db0_idle}"
    );
    assert!(
        db1_idle >= 59,
        "db1 idle time should swap with metadata: {db1_idle}"
    );
}

#[test]
fn swapdb_zero_and_nonzero_preserves_dirty_set_hot_state() {
    let processor = RequestProcessor::new().unwrap();
    let db0 = DbName::default();
    let db1 = DbName::new(1);

    assert_command_response(&processor, "CONFIG SET databases 2", b"+OK\r\n");
    assert_command_response_in_db(&processor, "SADD shared a", b":1\r\n", db0);
    assert_command_response_in_db(&processor, "SADD shared b c", b":2\r\n", db1);

    assert_eq!(
        execute_command_line_in_db(&processor, "SWAPDB 0 1", db0),
        b"+OK\r\n"
    );

    assert_command_response_in_db(&processor, "SCARD shared", b":2\r\n", db0);
    assert_command_response_in_db(&processor, "SISMEMBER shared a", b":0\r\n", db0);
    assert_command_response_in_db(&processor, "SISMEMBER shared b", b":1\r\n", db0);

    assert_command_response_in_db(&processor, "SCARD shared", b":1\r\n", db1);
    assert_command_response_in_db(&processor, "SISMEMBER shared a", b":1\r\n", db1);
    assert_command_response_in_db(&processor, "SISMEMBER shared b", b":0\r\n", db1);
}

#[test]
fn swapdb_zero_and_nonzero_makes_blocked_list_waiter_ready_in_target_logical_db() {
    let processor = RequestProcessor::new().unwrap();
    let db0 = DbName::default();
    let db1 = DbName::new(1);
    let client_id = ClientId::new(9001);
    let wait_key = BlockingWaitKey::new(
        db1,
        RedisKey::from(b"shared".as_slice()),
        BlockingWaitClass::List,
    );

    assert_command_response(&processor, "CONFIG SET databases 2", b"+OK\r\n");
    assert_eq!(
        execute_command_line_in_db(&processor, "LPUSH shared one", db0),
        b":1\r\n"
    );

    processor.register_blocking_wait(client_id, std::slice::from_ref(&wait_key));
    assert!(
        !processor.blocking_wait_keys_ready(client_id, std::slice::from_ref(&wait_key)),
        "db1 waiter should not observe db0 content before swap"
    );
    assert!(
        !processor.has_ready_blocking_waiters(),
        "no logical waiter should be ready before swap"
    );

    assert_eq!(
        execute_command_line_in_db(&processor, "SWAPDB 0 1", db0),
        b"+OK\r\n"
    );

    assert!(
        processor.blocking_wait_keys_ready(client_id, std::slice::from_ref(&wait_key)),
        "db1 waiter should observe swapped-in list after swap"
    );
    assert!(
        processor.has_ready_blocking_waiters(),
        "swap should expose the ready waiter"
    );
}

#[test]
fn swapdb_emits_global_tracking_invalidation() {
    let processor = RequestProcessor::new().unwrap();
    let source_client = ClientId::new(303);
    let redirect_client = ClientId::new(404);
    let expected_invalidation =
        b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n$0\r\n\r\n".to_vec();

    processor.register_pubsub_client(source_client);
    processor.register_pubsub_client(redirect_client);
    processor.configure_client_tracking(
        source_client,
        ClientTrackingConfig {
            mode: ClientTrackingModeSetting::On,
            redirect_id: Some(redirect_client),
            ..ClientTrackingConfig::default()
        },
    );

    assert_command_response(&processor, "CONFIG SET databases 2", b"+OK\r\n");
    assert_eq!(
        execute_command_line_in_db(&processor, "SET left a", DbName::default()),
        b"+OK\r\n"
    );
    assert_eq!(
        execute_command_line_in_db(&processor, "SET right b", DbName::new(1)),
        b"+OK\r\n"
    );

    assert_client_command_response(&processor, "SWAPDB 0 1", source_client, b"+OK\r\n");
    assert_eq!(
        processor.take_pending_pubsub_messages(redirect_client),
        vec![expected_invalidation]
    );
}

#[test]
fn info_keysizes_uses_hyperloglog_logical_length_bins() {
    let processor = RequestProcessor::new().unwrap();
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"PFADD", b"hll", b"a", b"b", b"c"])
        ),
        b":1\r\n"
    );
    let info_keysizes = parse_bulk_payload(&execute_frame(
        &processor,
        &encode_resp(&[b"INFO", b"KEYSIZES"]),
    ))
    .expect("INFO KEYSIZES returns bulk payload");
    let info_keysizes_text = String::from_utf8_lossy(&info_keysizes);
    assert!(info_keysizes_text.contains("db0_distrib_strings_sizes:16=1"));
}

#[test]
fn zadd_accepts_infinite_scores() {
    let processor = RequestProcessor::new().unwrap();
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"ZADD", b"z", b"+inf", b"hi", b"-inf", b"lo"]),
        ),
        b":2\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"ZRANGE", b"z", b"0", b"-1", b"WITHSCORES"]),
        ),
        b"*4\r\n$2\r\nlo\r\n$4\r\n-inf\r\n$2\r\nhi\r\n$3\r\ninf\r\n"
    );
}

#[test]
fn zset_nan_inf_and_zpop_count_edge_cases_match_external_scenarios() {
    let processor = RequestProcessor::new().unwrap();

    // Redis tests/unit/type/zset.tcl:
    // - "ZADD INCR LT/GT with inf - listpack"
    // - "ZADD INCR LT/GT with inf - skiplist"
    assert_command_response(&processor, "DEL ztmp", b":0\r\n");
    assert_command_response(&processor, "ZADD ztmp +inf x -inf y", b":2\r\n");
    assert_command_response(&processor, "ZADD ztmp LT INCR 1 x", b"$-1\r\n");
    assert_command_response(&processor, "ZSCORE ztmp x", b"$3\r\ninf\r\n");
    assert_command_response(&processor, "ZADD ztmp GT INCR -1 x", b"$-1\r\n");
    assert_command_response(&processor, "ZSCORE ztmp x", b"$3\r\ninf\r\n");
    assert_command_response(&processor, "ZADD ztmp LT INCR 1 y", b"$-1\r\n");
    assert_command_response(&processor, "ZSCORE ztmp y", b"$4\r\n-inf\r\n");
    assert_command_response(&processor, "ZADD ztmp GT INCR -1 y", b"$-1\r\n");
    assert_command_response(&processor, "ZSCORE ztmp y", b"$4\r\n-inf\r\n");

    // Redis tests/unit/type/zset.tcl:
    // - "ZINCRBY calls leading to NaN result in error - listpack"
    // - "ZINCRBY calls leading to NaN result in error - skiplist"
    assert_command_response(&processor, "DEL myzset", b":0\r\n");
    assert_command_response(&processor, "ZINCRBY myzset +inf abc", b"$3\r\ninf\r\n");
    assert_command_error(
        &processor,
        "ZINCRBY myzset -inf abc",
        b"-ERR resulting score is not a number (NaN)\r\n",
    );

    // Redis tests/unit/type/zset.tcl:
    // - "ZUNIONSTORE regression, should not create NaN in scores"
    assert_command_response(&processor, "DEL z{t} out{t}", b":0\r\n");
    assert_command_response(&processor, "ZADD z{t} -inf neginf", b":1\r\n");
    assert_command_response(&processor, "ZUNIONSTORE out{t} 1 z{t} WEIGHTS 0", b":1\r\n");
    assert_command_response(
        &processor,
        "ZRANGE out{t} 0 -1 WITHSCORES",
        b"*2\r\n$6\r\nneginf\r\n$1\r\n0\r\n",
    );

    // Redis tests/unit/type/zset.tcl:
    // - "ZPOPMIN with the count 0 returns an empty array"
    // - "ZPOPMAX with the count 0 returns an empty array"
    // - "ZPOPMIN with negative count"
    // - "ZPOPMAX with negative count"
    assert_command_response(&processor, "DEL zset", b":0\r\n");
    assert_command_response(&processor, "ZADD zset 1 a 2 b 3 c", b":3\r\n");
    assert_command_response(&processor, "ZPOPMIN zset 0", b"*0\r\n");
    assert_command_response(&processor, "ZPOPMAX zset 0", b"*0\r\n");
    assert_command_response(&processor, "ZCARD zset", b":3\r\n");
    assert_command_response(&processor, "SET zset foo", b"+OK\r\n");
    assert_command_error(
        &processor,
        "ZPOPMIN zset -1",
        b"-ERR value is out of range, must be positive\r\n",
    );
    assert_command_error(
        &processor,
        "ZPOPMAX zset -1",
        b"-ERR value is out of range, must be positive\r\n",
    );
    assert_command_error(
        &processor,
        "ZPOPMIN zset 0",
        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
    );
    assert_command_error(
        &processor,
        "ZPOPMAX zset 0",
        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
    );
}

#[test]
fn zset_regular_set_algebra_matches_external_scenarios() {
    for configured_value in ["64", "0"] {
        let processor = RequestProcessor::new().unwrap();
        assert_command_response(
            &processor,
            &format!("CONFIG SET zset-max-ziplist-value {configured_value}"),
            b"+OK\r\n",
        );

        assert_command_response(&processor, "SADD seta a b c", b":3\r\n");
        assert_command_response(&processor, "ZADD zsetb 1 b 2 c 3 d", b":3\r\n");
        // Redis tests/unit/type/zset.tcl:
        // - "ZUNIONSTORE with a regular set and weights - listpack|skiplist"
        // - "ZINTERSTORE with a regular set and weights - listpack|skiplist"
        // - "ZDIFFSTORE with a regular set - listpack|skiplist"
        assert_command_response(
            &processor,
            "ZUNIONSTORE zsetc 2 seta zsetb WEIGHTS 2 3",
            b":4\r\n",
        );
        assert_command_response(
            &processor,
            "ZRANGE zsetc 0 -1 WITHSCORES",
            b"*8\r\n$1\r\na\r\n$1\r\n2\r\n$1\r\nb\r\n$1\r\n5\r\n$1\r\nc\r\n$1\r\n8\r\n$1\r\nd\r\n$1\r\n9\r\n",
        );
        assert_command_response(
            &processor,
            "ZINTERSTORE zsetc 2 seta zsetb WEIGHTS 2 3",
            b":2\r\n",
        );
        assert_command_response(
            &processor,
            "ZRANGE zsetc 0 -1 WITHSCORES",
            b"*4\r\n$1\r\nb\r\n$1\r\n5\r\n$1\r\nc\r\n$1\r\n8\r\n",
        );
        assert_command_response(&processor, "ZDIFFSTORE zsetc 2 seta zsetb", b":1\r\n");
        assert_command_response(
            &processor,
            "ZRANGE zsetc 0 -1 WITHSCORES",
            b"*2\r\n$1\r\na\r\n$1\r\n1\r\n",
        );

        // Redis tests/unit/type/zset.tcl:
        // - "ZINTERSTORE regression with two sets, intset+hashtable"
        // - "ZINTERSTORE #516 regression, mixed sets and ziplist zsets"
        assert_command_response(&processor, "SADD set1 a", b":1\r\n");
        assert_command_response(&processor, "SADD set2 10", b":1\r\n");
        assert_command_response(&processor, "ZINTERSTORE set3 2 set1 set2", b":0\r\n");

        assert_command_response(&processor, "SADD one 100 101 102 103", b":4\r\n");
        assert_command_response(&processor, "SADD two 100 200 201 202", b":4\r\n");
        assert_command_response(
            &processor,
            "ZADD three 1 500 1 501 1 502 1 503 1 100",
            b":5\r\n",
        );
        assert_command_response(
            &processor,
            "ZINTERSTORE to_here 3 one two three WEIGHTS 0 0 1",
            b":1\r\n",
        );
        assert_command_response(&processor, "ZRANGE to_here 0 -1", b"*1\r\n$3\r\n100\r\n");
    }
}

#[test]
fn zrandmember_overflow_and_emptyarray_match_external_scenarios() {
    let processor = RequestProcessor::new().unwrap();

    // Redis tests/unit/type/zset.tcl:
    // - "ZRANDMEMBER count overflow"
    // - "ZRANDMEMBER count of 0 is handled correctly - emptyarray"
    // - "ZRANDMEMBER with <count> against non existing key - emptyarray"
    assert_command_response(&processor, "ZADD myzset 0 a", b":1\r\n");
    assert_command_error(
        &processor,
        "ZRANDMEMBER myzset -9223372036854770000 WITHSCORES",
        b"-ERR value is out of range\r\n",
    );
    assert_command_error(
        &processor,
        "ZRANDMEMBER myzset -9223372036854775808 WITHSCORES",
        b"-ERR value is out of range\r\n",
    );
    assert_command_error(
        &processor,
        "ZRANDMEMBER myzset -9223372036854775808",
        b"-ERR value is out of range\r\n",
    );
    assert_command_response(&processor, "ZRANDMEMBER myzset 0", b"*0\r\n");
    assert_command_response(&processor, "ZRANDMEMBER nonexisting_key 100", b"*0\r\n");

    // Keep serving requests after the out-of-range errors.
    let after = execute_command_line(&processor, "ZRANDMEMBER myzset -3").unwrap();
    let members = parse_bulk_array_payloads(&after);
    assert_eq!(members.len(), 3);
    assert!(members.iter().all(|member| member.as_slice() == b"a"));

    assert_command_response(
        &processor,
        "CONFIG GET zset-max-ziplist-value",
        b"*2\r\n$22\r\nzset-max-ziplist-value\r\n$2\r\n64\r\n",
    );
}

#[test]
fn zset_combine_validation_matches_external_scenarios() {
    let processor = RequestProcessor::new().unwrap();

    // Redis tests/unit/type/zset.tcl:
    // - "ZINTERCARD with illegal arguments"
    assert_command_response(&processor, "ZADD zseta 1 a 2 b 3 c", b":3\r\n");
    assert_command_response(&processor, "ZADD zsetb 1 b 2 c 3 d", b":3\r\n");
    assert_command_error(
        &processor,
        "ZINTERCARD 1 zseta zseta",
        b"-ERR syntax error\r\n",
    );
    assert_command_error(
        &processor,
        "ZINTERCARD 1 zseta bar_arg",
        b"-ERR syntax error\r\n",
    );
    assert_command_error(
        &processor,
        "ZINTERCARD 1 zseta LIMIT",
        b"-ERR syntax error\r\n",
    );
    let negative_limit = match execute_command_line(&processor, "ZINTERCARD 1 zseta LIMIT -1") {
        Ok(response) => panic!(
            "command unexpectedly succeeded: {}",
            String::from_utf8_lossy(&response)
        ),
        Err(CommandHarnessError::Request(error)) => {
            let mut response = Vec::new();
            error.append_resp_error(&mut response);
            response
        }
        Err(error) => panic!("unexpected harness error: {error}"),
    };
    assert!(
        negative_limit.starts_with(b"-ERR LIMIT"),
        "unexpected response: {}",
        String::from_utf8_lossy(&negative_limit)
    );
    let invalid_limit = match execute_command_line(&processor, "ZINTERCARD 1 zseta LIMIT a") {
        Ok(response) => panic!(
            "command unexpectedly succeeded: {}",
            String::from_utf8_lossy(&response)
        ),
        Err(CommandHarnessError::Request(error)) => {
            let mut response = Vec::new();
            error.append_resp_error(&mut response);
            response
        }
        Err(error) => panic!("unexpected harness error: {error}"),
    };
    assert!(
        invalid_limit.starts_with(b"-ERR LIMIT"),
        "unexpected response: {}",
        String::from_utf8_lossy(&invalid_limit)
    );

    // Redis tests/unit/type/zset.tcl:
    // - "ZUNIONSTORE with NaN weights - listpack|skiplist"
    // - "ZINTERSTORE with NaN weights - listpack|skiplist"
    assert_command_error(
        &processor,
        "ZUNIONSTORE out 2 zseta zsetb WEIGHTS nan nan",
        b"-ERR weight value is not a float\r\n",
    );
    assert_command_error(
        &processor,
        "ZINTERSTORE out 2 zseta zsetb WEIGHTS nan nan",
        b"-ERR weight value is not a float\r\n",
    );

    // Redis tests/unit/type/zset.tcl:
    // - "zunionInterDiffGenericCommand at least 1 input key"
    assert_command_error(
        &processor,
        "ZUNION 0 key",
        b"-ERR at least 1 input key is needed for 'zunion' command\r\n",
    );
    assert_command_error(
        &processor,
        "ZUNIONSTORE dst_key 0 key",
        b"-ERR at least 1 input key is needed for 'zunionstore' command\r\n",
    );
    assert_command_error(
        &processor,
        "ZINTER 0 key",
        b"-ERR at least 1 input key is needed for 'zinter' command\r\n",
    );
    assert_command_error(
        &processor,
        "ZINTERSTORE dst_key 0 key",
        b"-ERR at least 1 input key is needed for 'zinterstore' command\r\n",
    );
    assert_command_error(
        &processor,
        "ZDIFF 0 key",
        b"-ERR at least 1 input key is needed for 'zdiff' command\r\n",
    );
    assert_command_error(
        &processor,
        "ZDIFFSTORE dst_key 0 key",
        b"-ERR at least 1 input key is needed for 'zdiffstore' command\r\n",
    );
    assert_command_error(
        &processor,
        "ZINTERCARD 0 key",
        b"-ERR at least 1 input key is needed for 'zintercard' command\r\n",
    );
}

#[test]
fn zset_range_parser_and_hex_float_match_external_scenarios() {
    let processor = RequestProcessor::new().unwrap();

    // Redis tests/unit/type/zset.tcl:
    // - "ZINCRBY accepts hexadecimal inputs - listpack|skiplist"
    assert_command_integer(&processor, "ZADD zhexa 0x0p+0 zero 0x1p+0 one", 2);
    assert_command_response(&processor, "ZINCRBY zhexa 0x0p+0 zero", b"$1\r\n0\r\n");
    assert_command_response(&processor, "ZINCRBY zhexa 0x1p+0 one", b"$1\r\n2\r\n");
    assert_command_response(&processor, "ZSCORE zhexa zero", b"$1\r\n0\r\n");
    assert_command_response(&processor, "ZSCORE zhexa one", b"$1\r\n2\r\n");

    // Redis tests/unit/type/zset.tcl:
    // - "ZRANGEBYLEX with invalid lex range specifiers - listpack|skiplist"
    assert_command_error(
        &processor,
        "ZRANGEBYLEX fooz foo bar",
        b"-ERR min or max not valid string range item\r\n",
    );
    assert_command_error(
        &processor,
        "ZRANGEBYLEX fooz [foo bar",
        b"-ERR min or max not valid string range item\r\n",
    );
    assert_command_error(
        &processor,
        "ZRANGEBYLEX fooz foo [bar",
        b"-ERR min or max not valid string range item\r\n",
    );
    assert_command_error(
        &processor,
        "ZRANGEBYLEX fooz +x [bar",
        b"-ERR min or max not valid string range item\r\n",
    );
    assert_command_error(
        &processor,
        "ZRANGEBYLEX fooz -x [bar",
        b"-ERR min or max not valid string range item\r\n",
    );

    // Redis tests/unit/type/zset.tcl:
    // - "ZRANGE BYSCORE REV LIMIT"
    // - "ZRANGE BYLEX"
    // - "ZRANGESTORE invalid syntax"
    // - "ZRANGE invalid syntax"
    assert_command_integer(&processor, "ZADD z1 1 a 2 b 3 c 4 d", 4);
    assert_command_response(
        &processor,
        "ZRANGE z1 5 0 BYSCORE REV LIMIT 0 2 WITHSCORES",
        b"*4\r\n$1\r\nd\r\n$1\r\n4\r\n$1\r\nc\r\n$1\r\n3\r\n",
    );
    assert_command_response(
        &processor,
        "ZRANGE z1 [b [c BYLEX",
        b"*2\r\n$1\r\nb\r\n$1\r\nc\r\n",
    );
    assert_command_error(
        &processor,
        "ZRANGESTORE z2 z1 0 -1 LIMIT 1 2",
        b"-ERR syntax error\r\n",
    );
    assert_command_error(
        &processor,
        "ZRANGESTORE z2 z1 0 -1 WITHSCORES",
        b"-ERR syntax error\r\n",
    );
    assert_command_error(
        &processor,
        "ZRANGE z1 0 -1 LIMIT 1 2",
        b"-ERR syntax error\r\n",
    );
    assert_command_error(
        &processor,
        "ZRANGE z1 0 -1 BYLEX WITHSCORES",
        b"-ERR syntax error\r\n",
    );
    assert_command_error(
        &processor,
        "ZREVRANGE z1 0 -1 BYSCORE",
        b"-ERR syntax error\r\n",
    );
    assert_command_error(
        &processor,
        "ZRANGEBYSCORE z1 0 -1 REV",
        b"-ERR syntax error\r\n",
    );
}

#[test]
fn zset_score_double_range_matches_external_scenario() {
    let processor = RequestProcessor::new().unwrap();

    let dblmax = "179769313486231570814527423731704356798070567525844996598917476803157260780028538760589558632766878171540458953514382464234321326889464182768467546703537516986049910576551282076245490090389328944075868508455133942304583236903222948165808559332123348274797826204144723168738177180919299881250404026184124858368.00000000000000000";
    assert_command_response(&processor, "DEL zz", b":0\r\n");
    assert_command_integer(&processor, &format!("ZADD zz {dblmax} dblmax"), 1);
    assert_command_response(&processor, "OBJECT ENCODING zz", b"$8\r\nlistpack\r\n");
    assert_command_response(
        &processor,
        "ZSCORE zz dblmax",
        b"$23\r\n1.7976931348623157e+308\r\n",
    );
}

#[test]
fn memory_usage_reports_positive_values_and_null_for_missing_key() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 8];
    let mut response = Vec::new();

    let memory_missing = b"*3\r\n$6\r\nMEMORY\r\n$5\r\nUSAGE\r\n$7\r\nmissing\r\n";
    let meta = parse_resp_command_arg_slices(memory_missing, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$-1\r\n");

    response.clear();
    let set = b"*3\r\n$3\r\nSET\r\n$4\r\nskey\r\n$5\r\nvalue\r\n";
    let meta = parse_resp_command_arg_slices(set, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let memory_string = b"*3\r\n$6\r\nMEMORY\r\n$5\r\nUSAGE\r\n$4\r\nskey\r\n";
    let meta = parse_resp_command_arg_slices(memory_string, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert!(response.starts_with(b":"));
    assert_ne!(response, b":0\r\n");

    response.clear();
    let hset = b"*4\r\n$4\r\nHSET\r\n$4\r\nhkey\r\n$5\r\nfield\r\n$5\r\nvalue\r\n";
    let meta = parse_resp_command_arg_slices(hset, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let memory_hash = b"*3\r\n$6\r\nMEMORY\r\n$5\r\nUSAGE\r\n$4\r\nhkey\r\n";
    let meta = parse_resp_command_arg_slices(memory_hash, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert!(response.starts_with(b":"));
    assert_ne!(response, b":0\r\n");

    response.clear();
    let memory_help = b"*2\r\n$6\r\nMEMORY\r\n$4\r\nHELP\r\n";
    let meta = parse_resp_command_arg_slices(memory_help, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert!(response.starts_with(b"*13\r\n"));
    assert!(
        String::from_utf8_lossy(&response).contains("MEMORY <subcommand>"),
        "unexpected MEMORY HELP payload: {}",
        String::from_utf8_lossy(&response)
    );
}

#[test]
fn keys_returns_glob_matches_across_string_and_object_keys() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 8];
    let mut response = Vec::new();

    let set_foo1 = b"*3\r\n$3\r\nSET\r\n$4\r\nfoo1\r\n$1\r\n1\r\n";
    let meta = parse_resp_command_arg_slices(set_foo1, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();

    response.clear();
    let set_foo2 = b"*3\r\n$3\r\nSET\r\n$4\r\nfoo2\r\n$1\r\n2\r\n";
    let meta = parse_resp_command_arg_slices(set_foo2, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();

    response.clear();
    let set_bar = b"*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$1\r\n3\r\n";
    let meta = parse_resp_command_arg_slices(set_bar, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();

    response.clear();
    let hset_foo3 = b"*4\r\n$4\r\nHSET\r\n$4\r\nfoo3\r\n$1\r\nf\r\n$1\r\nv\r\n";
    let meta = parse_resp_command_arg_slices(hset_foo3, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();

    response.clear();
    let keys_foo = b"*2\r\n$4\r\nKEYS\r\n$4\r\nfoo*\r\n";
    let meta = parse_resp_command_arg_slices(keys_foo, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert!(response.starts_with(b"*3\r\n"));
    assert!(response.windows(10).any(|w| w == b"$4\r\nfoo1\r\n"));
    assert!(response.windows(10).any(|w| w == b"$4\r\nfoo2\r\n"));
    assert!(response.windows(10).any(|w| w == b"$4\r\nfoo3\r\n"));

    response.clear();
    let keys_all = b"*2\r\n$4\r\nKEYS\r\n$1\r\n*\r\n";
    let meta = parse_resp_command_arg_slices(keys_all, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$-1\r\n");

    response.clear();
    let set_foo = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$1\r\n1\r\n";
    let meta = parse_resp_command_arg_slices(set_foo, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let hset_bar = b"*4\r\n$4\r\nHSET\r\n$3\r\nbar\r\n$1\r\nf\r\n$1\r\n2\r\n";
    let meta = parse_resp_command_arg_slices(hset_bar, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    let mut seen_foo = false;
    let mut seen_bar = false;
    for _ in 0..8 {
        response.clear();
        let meta = parse_resp_command_arg_slices(randomkey, &mut args).unwrap();
        processor
            .execute_in_db(
                &args[..meta.argument_count],
                &mut response,
                DbName::default(),
            )
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
fn hscan_novalues_returns_only_field_names() {
    let processor = RequestProcessor::new().unwrap();
    execute_frame(
        &processor,
        &encode_resp(&[b"HSET", b"h", b"a", b"1", b"b", b"2", b"c", b"3"]),
    );
    // NOVALUES: returns 3 field names (no values) in one batch.
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"HSCAN", b"h", b"0", b"NOVALUES"])
        ),
        b"*2\r\n$1\r\n0\r\n*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n"
    );
    // NOVALUES with MATCH.
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"HSCAN", b"h", b"0", b"MATCH", b"b*", b"NOVALUES"])
        ),
        b"*2\r\n$1\r\n0\r\n*1\r\n$1\r\nb\r\n"
    );
}

#[test]
fn scan_unknown_type_returns_empty() {
    let processor = RequestProcessor::new().unwrap();
    execute_frame(&processor, &encode_resp(&[b"SET", b"a", b"1"]));
    execute_frame(&processor, &encode_resp(&[b"SET", b"b", b"2"]));
    // Unknown type "foobar" should return empty result, not error.
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"SCAN", b"0", b"TYPE", b"foobar"])
        ),
        b"*2\r\n$1\r\n0\r\n*0\r\n"
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let get = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(get, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$5\r\nvalue\r\n");

    response.clear();
    let ttl = b"*2\r\n$3\r\nTTL\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(ttl, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let setnx_second = b"*3\r\n$5\r\nSETNX\r\n$3\r\nkey\r\n$3\r\ntwo\r\n";
    let meta = parse_resp_command_arg_slices(setnx_second, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let get = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(get, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let set = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
    let meta = parse_resp_command_arg_slices(set, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let strlen = b"*2\r\n$6\r\nSTRLEN\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(strlen, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let getrange = b"*4\r\n$8\r\nGETRANGE\r\n$3\r\nkey\r\n$1\r\n2\r\n$1\r\n5\r\n";
    let meta = parse_resp_command_arg_slices(getrange, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$4\r\n2345\r\n");

    response.clear();
    let getrange_negative = b"*4\r\n$8\r\nGETRANGE\r\n$3\r\nkey\r\n$2\r\n-3\r\n$2\r\n-1\r\n";
    let meta = parse_resp_command_arg_slices(getrange_negative, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$3\r\n789\r\n");

    response.clear();
    let substr = b"*4\r\n$6\r\nSUBSTR\r\n$3\r\nkey\r\n$1\r\n1\r\n$1\r\n3\r\n";
    let meta = parse_resp_command_arg_slices(substr, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":3\r\n");

    response.clear();
    let getbit = b"*3\r\n$6\r\nGETBIT\r\n$3\r\nkey\r\n$1\r\n6\r\n";
    let meta = parse_resp_command_arg_slices(getbit, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let setbit = b"*4\r\n$6\r\nSETBIT\r\n$3\r\nkey\r\n$1\r\n6\r\n$1\r\n1\r\n";
    let meta = parse_resp_command_arg_slices(setbit, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let meta = parse_resp_command_arg_slices(getbit, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let get = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(get, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$3\r\ncbc\r\n");

    response.clear();
    let bitcount = b"*2\r\n$8\r\nBITCOUNT\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(bitcount, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":11\r\n");

    response.clear();
    let bitcount_byte =
        b"*5\r\n$8\r\nBITCOUNT\r\n$3\r\nkey\r\n$1\r\n0\r\n$1\r\n0\r\n$4\r\nBYTE\r\n";
    let meta = parse_resp_command_arg_slices(bitcount_byte, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":4\r\n");

    response.clear();
    let bitcount_bit = b"*5\r\n$8\r\nBITCOUNT\r\n$3\r\nkey\r\n$1\r\n8\r\n$2\r\n15\r\n$3\r\nBIT\r\n";
    let meta = parse_resp_command_arg_slices(bitcount_bit, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let set_b2 = b"*3\r\n$3\r\nSET\r\n$2\r\nb2\r\n$1\r\na\r\n";
    let meta = parse_resp_command_arg_slices(set_b2, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let bitop_and = b"*5\r\n$5\r\nBITOP\r\n$3\r\nAND\r\n$4\r\nbdst\r\n$2\r\nb1\r\n$2\r\nb2\r\n";
    let meta = parse_resp_command_arg_slices(bitop_and, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let get_bdst = b"*2\r\n$3\r\nGET\r\n$4\r\nbdst\r\n";
    let meta = parse_resp_command_arg_slices(get_bdst, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$1\r\nA\r\n");

    response.clear();
    let bitop_or = b"*5\r\n$5\r\nBITOP\r\n$2\r\nOR\r\n$4\r\nbdst\r\n$2\r\nb1\r\n$2\r\nb2\r\n";
    let meta = parse_resp_command_arg_slices(bitop_or, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let meta = parse_resp_command_arg_slices(get_bdst, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$1\r\na\r\n");

    response.clear();
    let bitop_xor = b"*5\r\n$5\r\nBITOP\r\n$3\r\nXOR\r\n$4\r\nbdst\r\n$2\r\nb1\r\n$2\r\nb2\r\n";
    let meta = parse_resp_command_arg_slices(bitop_xor, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let meta = parse_resp_command_arg_slices(get_bdst, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$1\r\n \r\n");

    response.clear();
    let bitop_not = b"*4\r\n$5\r\nBITOP\r\n$3\r\nNOT\r\n$4\r\nbdst\r\n$2\r\nb1\r\n";
    let meta = parse_resp_command_arg_slices(bitop_not, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let meta = parse_resp_command_arg_slices(get_bdst, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$1\r\n\xbe\r\n");

    response.clear();
    let bitop_one = b"*5\r\n$5\r\nBITOP\r\n$3\r\nONE\r\n$4\r\nbdst\r\n$2\r\nb1\r\n$2\r\nb2\r\n";
    let meta = parse_resp_command_arg_slices(bitop_one, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let meta = parse_resp_command_arg_slices(get_bdst, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$1\r\n \r\n");

    response.clear();
    let bitop_diff = b"*5\r\n$5\r\nBITOP\r\n$4\r\nDIFF\r\n$4\r\nbdst\r\n$2\r\nb1\r\n$2\r\nb2\r\n";
    let meta = parse_resp_command_arg_slices(bitop_diff, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let meta = parse_resp_command_arg_slices(get_bdst, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$1\r\n\x00\r\n");

    response.clear();
    let bitop_diff1 = b"*5\r\n$5\r\nBITOP\r\n$5\r\nDIFF1\r\n$4\r\nbdst\r\n$2\r\nb1\r\n$2\r\nb2\r\n";
    let meta = parse_resp_command_arg_slices(bitop_diff1, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let meta = parse_resp_command_arg_slices(get_bdst, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$1\r\n \r\n");

    response.clear();
    let bitop_andor = b"*5\r\n$5\r\nBITOP\r\n$5\r\nANDOR\r\n$4\r\nbdst\r\n$2\r\nb1\r\n$2\r\nb2\r\n";
    let meta = parse_resp_command_arg_slices(bitop_andor, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let meta = parse_resp_command_arg_slices(get_bdst, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$1\r\nA\r\n");

    response.clear();
    let bitop_and_missing =
        b"*5\r\n$5\r\nBITOP\r\n$3\r\nAND\r\n$6\r\nbempty\r\n$8\r\nmissing1\r\n$8\r\nmissing2\r\n";
    let meta = parse_resp_command_arg_slices(bitop_and_missing, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let exists_bempty = b"*2\r\n$6\r\nEXISTS\r\n$6\r\nbempty\r\n";
    let meta = parse_resp_command_arg_slices(exists_bempty, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let setrange_extend = b"*4\r\n$8\r\nSETRANGE\r\n$3\r\nkey\r\n$1\r\n5\r\n$1\r\nZ\r\n";
    let meta = parse_resp_command_arg_slices(setrange_extend, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":6\r\n");

    response.clear();
    let getbit_missing = b"*3\r\n$6\r\nGETBIT\r\n$7\r\nmissing\r\n$1\r\n0\r\n";
    let meta = parse_resp_command_arg_slices(getbit_missing, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let bitcount_missing = b"*2\r\n$8\r\nBITCOUNT\r\n$7\r\nmissing\r\n";
    let meta = parse_resp_command_arg_slices(bitcount_missing, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let bitcount_invalid_mode =
        b"*5\r\n$8\r\nBITCOUNT\r\n$3\r\nkey\r\n$1\r\n0\r\n$1\r\n1\r\n$5\r\nWORDS\r\n";
    let meta = parse_resp_command_arg_slices(bitcount_invalid_mode, &mut args).unwrap();
    let err = processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .err()
        .unwrap();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR syntax error\r\n");

    response.clear();
    let set_empty = b"*3\r\n$3\r\nSET\r\n$8\r\nbitempty\r\n$0\r\n\r\n";
    let meta = parse_resp_command_arg_slices(set_empty, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");
    assert_command_integer(&processor, "BITPOS bitempty 0", -1);

    response.clear();
    let bitpos_invalid_mode =
        b"*6\r\n$6\r\nBITPOS\r\n$4\r\nbpff\r\n$1\r\n0\r\n$1\r\n0\r\n$1\r\n7\r\n$5\r\nWORDS\r\n";
    let meta = parse_resp_command_arg_slices(bitpos_invalid_mode, &mut args).unwrap();
    let err = processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .err()
        .unwrap();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR syntax error\r\n");

    response.clear();
    let bitpos_invalid_bit = b"*3\r\n$6\r\nBITPOS\r\n$4\r\nbpff\r\n$1\r\n2\r\n";
    let meta = parse_resp_command_arg_slices(bitpos_invalid_bit, &mut args).unwrap();
    let err = processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .err()
        .unwrap();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR value is out of range\r\n");

    response.clear();
    let hset = b"*4\r\n$4\r\nHSET\r\n$2\r\noh\r\n$1\r\nf\r\n$1\r\nv\r\n";
    let meta = parse_resp_command_arg_slices(hset, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let bitop_wrongtype =
        b"*5\r\n$5\r\nBITOP\r\n$2\r\nOR\r\n$4\r\nbdst\r\n$2\r\noh\r\n$2\r\nb1\r\n";
    let meta = parse_resp_command_arg_slices(bitop_wrongtype, &mut args).unwrap();
    let err = processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        b"*4\r\n$7\r\nmatches\r\n*2\r\n*2\r\n*2\r\n:4\r\n:7\r\n*2\r\n:5\r\n:8\r\n*2\r\n*2\r\n:2\r\n:3\r\n*2\r\n:0\r\n:1\r\n$3\r\nlen\r\n:6\r\n",
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
fn lcs_idx_and_matchlen_match_external_string_scenarios() {
    let processor = RequestProcessor::new().unwrap();
    let rna1 = b"CACCTTCCCAGGTAACAAACCAACCAACTTTCGATCTCTTGTAGATCTGTTCTCTAAACGAACTTTAAAATCTGTGTGGCTGTCACTCGGCTGCATGCTTAGTGCACTCACGCAGTATAATTAATAACTAATTACTGTCGTTGACAGGACACGAGTAACTCGTCTATCTTCTGCAGGCTGCTTACGGTTTCGTCCGTGTTGCAGCCGATCATCAGCACATCTAGGTTTCGTCCGGGTGTG";
    let rna2 = b"ATTAAAGGTTTATACCTTCCCAGGTAACAAACCAACCAACTTTCGATCTCTTGTAGATCTGTTCTCTAAACGAACTTTAAAATCTGTGTGGCTGTCACTCGGCTGCATGCTTAGTGCACTCACGCAGTATAATTAATAACTAATTACTGTCGTTGACAGGACACGAGTAACTCGTCTATCTTCTGCAGGCTGCTTACGGTTTCGTCCGTGTTGCAGCCGATCATCAGCACATCTAGGTTT";

    execute_frame(&processor, &encode_resp(&[b"SET", b"virus1{t}", rna1]));
    execute_frame(&processor, &encode_resp(&[b"SET", b"virus2{t}", rna2]));

    let idx = parse_resp_test_value(&execute_frame(
        &processor,
        &encode_resp(&[b"LCS", b"virus1{t}", b"virus2{t}", b"IDX"]),
    ));
    let idx_map = resp_test_flat_map(&idx);
    assert_eq!(resp_test_integer(idx_map[&b"len".to_vec()]), 227);
    let matches = resp_test_array(idx_map[&b"matches".to_vec()]);
    let actual_matches: Vec<((i64, i64), (i64, i64), Option<i64>)> = matches
        .iter()
        .map(|entry| {
            let parts = resp_test_array(entry);
            (
                (
                    resp_test_integer(&resp_test_array(&parts[0])[0]),
                    resp_test_integer(&resp_test_array(&parts[0])[1]),
                ),
                (
                    resp_test_integer(&resp_test_array(&parts[1])[0]),
                    resp_test_integer(&resp_test_array(&parts[1])[1]),
                ),
                None,
            )
        })
        .collect();
    assert_eq!(
        actual_matches,
        vec![
            ((238, 238), (239, 239), None),
            ((236, 236), (238, 238), None),
            ((229, 230), (236, 237), None),
            ((224, 224), (235, 235), None),
            ((1, 222), (13, 234), None),
        ]
    );

    let idx_with_len = parse_resp_test_value(&execute_frame(
        &processor,
        &encode_resp(&[b"LCS", b"virus1{t}", b"virus2{t}", b"IDX", b"WITHMATCHLEN"]),
    ));
    let idx_with_len_map = resp_test_flat_map(&idx_with_len);
    let matches = resp_test_array(idx_with_len_map[&b"matches".to_vec()]);
    let actual_matches: Vec<((i64, i64), (i64, i64), Option<i64>)> = matches
        .iter()
        .map(|entry| {
            let parts = resp_test_array(entry);
            (
                (
                    resp_test_integer(&resp_test_array(&parts[0])[0]),
                    resp_test_integer(&resp_test_array(&parts[0])[1]),
                ),
                (
                    resp_test_integer(&resp_test_array(&parts[1])[0]),
                    resp_test_integer(&resp_test_array(&parts[1])[1]),
                ),
                Some(resp_test_integer(&parts[2])),
            )
        })
        .collect();
    assert_eq!(
        actual_matches,
        vec![
            ((238, 238), (239, 239), Some(1)),
            ((236, 236), (238, 238), Some(1)),
            ((229, 230), (236, 237), Some(2)),
            ((224, 224), (235, 235), Some(1)),
            ((1, 222), (13, 234), Some(222)),
        ]
    );

    let idx_with_len_min = parse_resp_test_value(&execute_frame(
        &processor,
        &encode_resp(&[
            b"LCS",
            b"virus1{t}",
            b"virus2{t}",
            b"IDX",
            b"WITHMATCHLEN",
            b"MINMATCHLEN",
            b"5",
        ]),
    ));
    let idx_with_len_min_map = resp_test_flat_map(&idx_with_len_min);
    let matches = resp_test_array(idx_with_len_min_map[&b"matches".to_vec()]);
    let actual_matches: Vec<((i64, i64), (i64, i64), Option<i64>)> = matches
        .iter()
        .map(|entry| {
            let parts = resp_test_array(entry);
            (
                (
                    resp_test_integer(&resp_test_array(&parts[0])[0]),
                    resp_test_integer(&resp_test_array(&parts[0])[1]),
                ),
                (
                    resp_test_integer(&resp_test_array(&parts[1])[0]),
                    resp_test_integer(&resp_test_array(&parts[1])[1]),
                ),
                Some(resp_test_integer(&parts[2])),
            )
        })
        .collect();
    assert_eq!(actual_matches, vec![((1, 222), (13, 234), Some(222))]);
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
        b"-ERR One or more scores can't be converted into double\r\n",
    );
}

#[test]
fn sort_exact_external_scenarios_match_redis_sort_tcl() {
    let processor = RequestProcessor::new().unwrap();

    assert_eq!(
        parse_bulk_array_payloads(
            &execute_command_line(&processor, "COMMAND GETKEYS SORT abc STORE def").unwrap()
        ),
        vec![b"abc".to_vec(), b"def".to_vec()]
    );
    assert_eq!(
        parse_bulk_array_payloads(
            &execute_command_line(&processor, "COMMAND GETKEYS SORT_RO abc").unwrap()
        ),
        vec![b"abc".to_vec()]
    );
    assert_eq!(
        parse_bulk_array_payloads(
            &execute_command_line(
                &processor,
                "COMMAND GETKEYS SORT abc STORE invalid STORE stillbad STORE def",
            )
            .unwrap()
        ),
        vec![b"abc".to_vec(), b"def".to_vec()]
    );

    assert_command_integer(&processor, "ZADD zset 1 a 5 b 2 c 10 d 3 e", 5);
    assert_command_response(
        &processor,
        "SORT zset BY nosort ASC",
        b"*5\r\n$1\r\na\r\n$1\r\nc\r\n$1\r\ne\r\n$1\r\nb\r\n$1\r\nd\r\n",
    );
    assert_command_response(
        &processor,
        "SORT zset BY nosort DESC",
        b"*5\r\n$1\r\nd\r\n$1\r\nb\r\n$1\r\ne\r\n$1\r\nc\r\n$1\r\na\r\n",
    );
    assert_command_response(
        &processor,
        "SORT zset BY nosort LIMIT -10 100",
        b"*5\r\n$1\r\na\r\n$1\r\nc\r\n$1\r\ne\r\n$1\r\nb\r\n$1\r\nd\r\n",
    );

    assert_command_integer(&processor, "SADD myset 1 2 3 4 not-a-double", 5);
    assert_command_error(
        &processor,
        "SORT myset",
        b"-ERR One or more scores can't be converted into double\r\n",
    );

    assert_command_integer(&processor, "DEL myset", 1);
    assert_command_integer(&processor, "SADD myset 1 2 3 4", 4);
    assert_command_response(
        &processor,
        "MSET score:1 10 score:2 20 score:3 30 score:4 not-a-double",
        b"+OK\r\n",
    );
    assert_command_error(
        &processor,
        "SORT myset BY score:*",
        b"-ERR One or more scores can't be converted into double\r\n",
    );

    assert_command_integer(&processor, "LPUSH mylist a", 1);
    assert_command_response(&processor, "SET x:a-> 100", b"+OK\r\n");
    assert_command_response(
        &processor,
        "SORT mylist BY num GET x:*->",
        b"*1\r\n$3\r\n100\r\n",
    );

    assert_command_integer(&processor, "DEL mylist", 1);
    assert_command_integer(&processor, "LPUSH mylist a", 1);
    assert_command_response(&processor, "SET x:a 100", b"+OK\r\n");
    // Redis' SORT_RO scenario relies on the previous test leaving x:a-> behind.
    assert_command_response(
        &processor,
        "SORT_RO mylist BY nosort GET x:*->",
        b"*1\r\n$3\r\n100\r\n",
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let pttl = b"*2\r\n$4\r\nPTTL\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(pttl, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$-1\r\n");

    response.clear();
    let getset_existing = b"*3\r\n$6\r\nGETSET\r\n$3\r\nkey\r\n$3\r\ntwo\r\n";
    let meta = parse_resp_command_arg_slices(getset_existing, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$3\r\none\r\n");

    response.clear();
    let get = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(get, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let getdel = b"*2\r\n$6\r\nGETDEL\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(getdel, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$5\r\nvalue\r\n");

    response.clear();
    let get = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(get, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$-1\r\n");

    response.clear();
    let meta = parse_resp_command_arg_slices(getdel, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let append = b"*3\r\n$6\r\nAPPEND\r\n$3\r\nkey\r\n$2\r\nbc\r\n";
    let meta = parse_resp_command_arg_slices(append, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":3\r\n");

    response.clear();
    let get = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(get, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$3\r\nabc\r\n");

    response.clear();
    let incrbyfloat = b"*3\r\n$11\r\nINCRBYFLOAT\r\n$3\r\nnum\r\n$4\r\n1.25\r\n";
    let meta = parse_resp_command_arg_slices(incrbyfloat, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$4\r\n1.25\r\n");

    response.clear();
    let incrbyfloat_again = b"*3\r\n$11\r\nINCRBYFLOAT\r\n$3\r\nnum\r\n$4\r\n0.75\r\n";
    let meta = parse_resp_command_arg_slices(incrbyfloat_again, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$1\r\n2\r\n");

    assert_command_error(
        &processor,
        "INCRBYFLOAT num +inf",
        b"-ERR increment would produce NaN or Infinity\r\n",
    );
}

#[test]
fn getex_updates_expiration_and_persist() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 8];
    let mut response = Vec::new();

    let set = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
    let meta = parse_resp_command_arg_slices(set, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let getex_px = b"*4\r\n$5\r\nGETEX\r\n$3\r\nkey\r\n$2\r\nPX\r\n$3\r\n250\r\n";
    let meta = parse_resp_command_arg_slices(getex_px, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$5\r\nvalue\r\n");

    response.clear();
    let pttl = b"*2\r\n$4\r\nPTTL\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(pttl, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    let ttl_millis = parse_integer_response(&response);
    assert!((0..=250).contains(&ttl_millis));

    response.clear();
    let getex_persist = b"*3\r\n$5\r\nGETEX\r\n$3\r\nkey\r\n$7\r\nPERSIST\r\n";
    let meta = parse_resp_command_arg_slices(getex_persist, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$5\r\nvalue\r\n");

    response.clear();
    let ttl = b"*2\r\n$3\r\nTTL\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(ttl, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let getex_exat_past = b"*4\r\n$5\r\nGETEX\r\n$3\r\nkey\r\n$4\r\nEXAT\r\n$1\r\n1\r\n";
    let meta = parse_resp_command_arg_slices(getex_exat_past, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$5\r\nvalue\r\n");

    response.clear();
    let get = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(get, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let msetnx_second = b"*5\r\n$6\r\nMSETNX\r\n$2\r\nk2\r\n$1\r\nx\r\n$2\r\nk3\r\n$1\r\ny\r\n";
    let meta = parse_resp_command_arg_slices(msetnx_second, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let mget = b"*4\r\n$4\r\nMGET\r\n$2\r\nk1\r\n$2\r\nk2\r\n$2\r\nk3\r\n";
    let meta = parse_resp_command_arg_slices(mget, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"*3\r\n$2\r\nv1\r\n$2\r\nv2\r\n$-1\r\n");
}

#[test]
fn digest_and_delex_match_external_string_scenarios() {
    let processor = RequestProcessor::new().unwrap();

    assert_command_response(&processor, "SET mykey hello", b"+OK\r\n");
    let digest = parse_bulk_payload(&execute_command_line(&processor, "DIGEST mykey").unwrap())
        .expect("digest should return a hex string");
    assert_eq!(digest.len(), 16);

    assert_command_response(
        &processor,
        "SET foo v8lf0c11xh8ymlqztfd3eeq16kfn4sspw7fqmnuuq3k3t75em5wdizgcdw7uc26nnf961u2jkfzkjytls2kwlj7626sd",
        b"+OK\r\n",
    );
    assert_command_response(&processor, "DIGEST foo", b"$16\r\n00006c38adf31777\r\n");
    assert_command_response(&processor, "DIGEST missing", b"$-1\r\n");

    assert_command_integer(&processor, "DELEX mykey IFEQ hello", 1);
    assert_command_integer(&processor, "EXISTS mykey", 0);

    assert_command_response(&processor, "SET mykey hello", b"+OK\r\n");
    assert_command_integer(&processor, "DELEX mykey IFEQ world", 0);
    assert_command_response(&processor, "GET mykey", b"$5\r\nhello\r\n");

    let uppercase_digest =
        parse_bulk_payload(&execute_command_line(&processor, "DIGEST mykey").unwrap())
            .unwrap()
            .iter()
            .map(|byte| byte.to_ascii_uppercase())
            .collect::<Vec<_>>();
    let uppercase_digest = String::from_utf8(uppercase_digest).unwrap();
    assert_command_integer(
        &processor,
        &format!("DELEX mykey IFDEQ {uppercase_digest}"),
        1,
    );
    assert_command_integer(&processor, "EXISTS mykey", 0);

    assert_command_integer(&processor, "DELEX nonexistent IFDEQ 1234567890", 0);

    assert_command_response(&processor, "HSET myhash field value", b":1\r\n");
    assert_command_integer(&processor, "DELEX myhash", 1);

    assert_command_response(&processor, "SET mykey hello", b"+OK\r\n");
    assert_command_error(
        &processor,
        "DELEX mykey INVALID hello",
        b"-ERR Invalid condition. Use IFEQ, IFNE, IFDEQ, or IFDNE\r\n",
    );
    assert_command_error(
        &processor,
        "DELEX mykey IFDEQ short",
        b"-ERR must be exactly 16 hexadecimal characters\r\n",
    );

    assert_command_response(&processor, "LPUSH mylist element", b":1\r\n");
    assert_command_error(
        &processor,
        "DELEX mylist IFEQ element",
        b"-ERR Key should be of string type if conditions are specified\r\n",
    );
}

#[test]
fn extended_set_conditions_match_external_string_scenarios() {
    let processor = RequestProcessor::new().unwrap();

    assert_command_response(&processor, "SET mykey hello", b"+OK\r\n");
    assert_command_response(&processor, "SET mykey world IFEQ hello", b"+OK\r\n");
    assert_command_response(&processor, "GET mykey", b"$5\r\nworld\r\n");

    assert_command_response(&processor, "SET mykey hello", b"+OK\r\n");
    assert_command_response(&processor, "SET mykey world IFEQ different", b"$-1\r\n");
    assert_command_response(&processor, "GET mykey", b"$5\r\nhello\r\n");

    assert_command_integer(&processor, "DEL mykey", 1);
    assert_command_response(&processor, "SET mykey world IFNE hello", b"+OK\r\n");
    assert_command_response(&processor, "GET mykey", b"$5\r\nworld\r\n");

    assert_command_response(&processor, "SET mykey hello", b"+OK\r\n");
    assert_command_response(
        &processor,
        "SET mykey world IFNE hello GET",
        b"$5\r\nhello\r\n",
    );
    assert_command_response(&processor, "GET mykey", b"$5\r\nhello\r\n");

    assert_command_integer(&processor, "DEL mykey", 1);
    assert_command_response(&processor, "SET mykey world IFNE hello GET", b"$-1\r\n");
    assert_command_response(&processor, "GET mykey", b"$5\r\nworld\r\n");

    assert_command_response(&processor, "SET mykey hello", b"+OK\r\n");
    let digest = String::from_utf8(
        parse_bulk_payload(&execute_command_line(&processor, "DIGEST mykey").unwrap()).unwrap(),
    )
    .unwrap();
    assert_command_response(
        &processor,
        &format!("SET mykey world IFDEQ {digest}"),
        b"+OK\r\n",
    );
    assert_command_response(&processor, "GET mykey", b"$5\r\nworld\r\n");

    assert_command_response(&processor, "SET mykey hello", b"+OK\r\n");
    let digest = String::from_utf8(
        parse_bulk_payload(&execute_command_line(&processor, "DIGEST mykey").unwrap()).unwrap(),
    )
    .unwrap();
    let uppercase_digest = digest.to_ascii_uppercase();
    assert_command_response(
        &processor,
        &format!("SET mykey world IFDNE {uppercase_digest}"),
        b"$-1\r\n",
    );
    assert_command_response(&processor, "GET mykey", b"$5\r\nhello\r\n");

    assert_command_response(&processor, "SET mykey hello", b"+OK\r\n");
    assert_command_response(
        &processor,
        &format!("SET mykey world IFDEQ {digest} EX 10"),
        b"+OK\r\n",
    );
    let ttl_response = execute_command_line(&processor, "TTL mykey").unwrap();
    let ttl = parse_integer_response(&ttl_response);
    assert!((0..=10).contains(&ttl));

    assert_command_response(&processor, "SET mykey 12345", b"+OK\r\n");
    assert_command_response(&processor, "OBJECT ENCODING mykey", b"$3\r\nint\r\n");
    assert_command_response(&processor, "SET mykey world ifeq 12345", b"+OK\r\n");
    assert_command_response(&processor, "GET mykey", b"$5\r\nworld\r\n");

    assert_command_response(&processor, "LPUSH mylist element", b":1\r\n");
    assert_command_error(
        &processor,
        "SET mylist value IFEQ element",
        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
    );
    assert_command_response(&processor, "SADD myset member", b":1\r\n");
    assert_command_error(
        &processor,
        "SET myset value IFDEQ 1234567890",
        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
    );

    assert_command_response(&processor, "SET mykey hello", b"+OK\r\n");
    assert_command_error(
        &processor,
        "SET mykey world IFDEQ short",
        b"-ERR must be exactly 16 hexadecimal characters\r\n",
    );
}

#[test]
fn msetex_options_match_external_string_scenarios() {
    let processor = RequestProcessor::new().unwrap();

    assert_command_error(
        &processor,
        "MSETEX",
        b"-ERR wrong number of arguments for 'msetex' command\r\n",
    );
    assert_command_error(
        &processor,
        "MSETEX key1 val1 EX 10",
        b"-ERR invalid numkeys value\r\n",
    );
    assert_command_error(
        &processor,
        "MSETEX 2 key1 val1 key2",
        b"-ERR wrong number of key-value pairs\r\n",
    );

    assert_command_integer(&processor, "MSETEX 2 ex:key1 val1 ex:key2 val2 EX 5", 1);
    assert_command_response(&processor, "GET ex:key1", b"$4\r\nval1\r\n");
    let ttl = parse_integer_response(&execute_command_line(&processor, "TTL ex:key1").unwrap());
    assert!((0..=5).contains(&ttl));

    let future_sec = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 10;
    let future_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
        + 10_000;
    assert_command_integer(
        &processor,
        &format!("MSETEX 2 exat:key1 val3 exat:key2 val4 EXAT {future_sec}"),
        1,
    );
    assert_command_integer(
        &processor,
        &format!("MSETEX 2 pxat:key1 val3 pxat:key2 val4 PXAT {future_ms}"),
        1,
    );
    assert!(
        parse_integer_response(&execute_command_line(&processor, "TTL exat:key1").unwrap()) > 0
    );
    assert!(
        parse_integer_response(&execute_command_line(&processor, "PTTL pxat:key1").unwrap()) > 0
    );

    assert_command_response(&processor, "SETEX keepttl:key 100 oldval", b"+OK\r\n");
    let old_ttl =
        parse_integer_response(&execute_command_line(&processor, "TTL keepttl:key").unwrap());
    assert_command_integer(&processor, "MSETEX 1 keepttl:key newval KEEPTTL", 1);
    assert_command_response(&processor, "GET keepttl:key", b"$6\r\nnewval\r\n");
    let new_ttl =
        parse_integer_response(&execute_command_line(&processor, "TTL keepttl:key").unwrap());
    assert!(new_ttl >= old_ttl - 5);

    assert_command_integer(&processor, "DEL nx:new nx:new2 xx:existing xx:nonexist", 0);
    assert_command_response(&processor, "SET xx:existing oldval", b"+OK\r\n");
    assert_command_integer(&processor, "MSETEX 2 nx:new val1 nx:new2 val2 NX EX 10", 1);
    assert_command_integer(&processor, "MSETEX 1 xx:existing newval NX EX 10", 0);
    assert_command_integer(&processor, "MSETEX 1 xx:nonexist newval XX EX 10", 0);
    assert_command_integer(&processor, "MSETEX 1 xx:existing newval XX EX 10", 1);
    assert_command_response(&processor, "GET nx:new", b"$4\r\nval1\r\n");
    assert_command_response(&processor, "GET xx:existing", b"$6\r\nnewval\r\n");

    assert_command_integer(&processor, "MSETEX 2 flex:1 val1 flex:2 val2 EX 3 NX", 1);
    assert_command_integer(&processor, "MSETEX 2 flex:3 val3 flex:4 val4 PX 3000 XX", 0);
    assert_command_response(&processor, "GET flex:1", b"$4\r\nval1\r\n");
    assert_command_integer(&processor, "EXISTS flex:3", 0);

    assert_command_error(
        &processor,
        "MSETEX 2 key1 val1 key2 val2 NX XX EX 10",
        b"-ERR syntax error\r\n",
    );
    assert_command_error(
        &processor,
        "MSETEX 2 key1 val1 key2 val2 EX 10 PX 5000",
        b"-ERR syntax error\r\n",
    );
    assert_command_error(
        &processor,
        "MSETEX 2147483648 key1 val1 EX 10",
        b"-ERR invalid numkeys value\r\n",
    );
    assert_command_error(
        &processor,
        "MSETEX 2147483647 key1 val1 EX 10",
        b"-ERR wrong number of key-value pairs\r\n",
    );
}

#[test]
fn touch_and_unlink_count_existing_keys() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 16];
    let mut response = Vec::new();

    let set_a = b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n";
    let meta = parse_resp_command_arg_slices(set_a, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let set_b = b"*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\n2\r\n";
    let meta = parse_resp_command_arg_slices(set_b, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let touch = b"*4\r\n$5\r\nTOUCH\r\n$1\r\na\r\n$1\r\nb\r\n$7\r\nmissing\r\n";
    let meta = parse_resp_command_arg_slices(touch, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":2\r\n");

    response.clear();
    let unlink = b"*4\r\n$6\r\nUNLINK\r\n$1\r\na\r\n$1\r\nb\r\n$7\r\nmissing\r\n";
    let meta = parse_resp_command_arg_slices(unlink, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":2\r\n");

    response.clear();
    let get_a = b"*2\r\n$3\r\nGET\r\n$1\r\na\r\n";
    let meta = parse_resp_command_arg_slices(get_a, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    let lru_before = processor
        .key_lru_millis(DbKeyRef::new(DbName::default(), b"key"))
        .unwrap();

    thread::sleep(Duration::from_millis(5));
    response.clear();
    let get = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(get, &mut args).unwrap();
    processor
        .execute_with_client_context_in_db(
            &args[..meta.argument_count],
            &mut response,
            true,
            None,
            false,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$5\r\nvalue\r\n");
    let lru_after_no_touch = processor
        .key_lru_millis(DbKeyRef::new(DbName::default(), b"key"))
        .unwrap();
    assert_eq!(lru_after_no_touch, lru_before);

    thread::sleep(Duration::from_millis(5));
    response.clear();
    let meta = parse_resp_command_arg_slices(get, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$5\r\nvalue\r\n");
    let lru_after_touch = processor
        .key_lru_millis(DbKeyRef::new(DbName::default(), b"key"))
        .unwrap();
    assert!(lru_after_touch > lru_after_no_touch);
}

#[test]
fn command_getkeys_getkeysandflags_list_and_info_cover_introspection_paths() {
    let processor = RequestProcessor::new().unwrap();

    let count_response = execute_command_line(&processor, "COMMAND COUNT").unwrap();
    assert!(parse_integer_response(&count_response) > 0);
    let command_help = execute_command_line(&processor, "COMMAND HELP").unwrap();
    assert!(
        String::from_utf8_lossy(&command_help).contains("COMMAND <subcommand>"),
        "unexpected COMMAND HELP payload: {}",
        String::from_utf8_lossy(&command_help)
    );

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
    // Generic FirstKey fallback for hash field expire commands.
    assert_eq!(
        parse_bulk_array_payloads(
            &execute_command_line(&processor, "COMMAND GETKEYS HEXPIRE mykey 30 FIELDS 1 f1")
                .unwrap()
        ),
        vec![b"mykey".to_vec()]
    );
    assert_eq!(
        parse_bulk_array_payloads(
            &execute_command_line(&processor, "COMMAND GETKEYS HPTTL mykey FIELDS 1 f1").unwrap()
        ),
        vec![b"mykey".to_vec()]
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
        .execute_in_db(
            &large_args[..getkeys_meta.argument_count],
            &mut getkeys_response,
            DbName::default(),
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
            &execute_command_line(
                &processor,
                "COMMAND GETKEYSANDFLAGS SET k1 v1 IFDEQ deadbeefdeadbeef"
            )
            .unwrap()
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
                "COMMAND GETKEYSANDFLAGS DELEX k1 IFDNE deadbeefdeadbeef"
            )
            .unwrap()
        ),
        vec![(b"k1".to_vec(), vec![b"RW".to_vec(), b"delete".to_vec()])]
    );
    // Generic FirstKey fallback for hash field expire commands.
    assert_eq!(
        parse_command_getkeysandflags_response(
            &execute_command_line(
                &processor,
                "COMMAND GETKEYSANDFLAGS HEXPIRE mykey 30 FIELDS 1 f1"
            )
            .unwrap()
        ),
        vec![(b"mykey".to_vec(), vec![b"OW".to_vec(), b"update".to_vec()])]
    );
    assert_eq!(
        parse_command_getkeysandflags_response(
            &execute_command_line(
                &processor,
                "COMMAND GETKEYSANDFLAGS HPTTL mykey FIELDS 1 f1"
            )
            .unwrap()
        ),
        vec![(b"mykey".to_vec(), vec![b"RO".to_vec(), b"access".to_vec()])]
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

    assert_command_response(&processor, "COMMAND INFO get|key", b"*1\r\n*-1\r\n");
    assert_command_response(&processor, "COMMAND INFO config|get|key", b"*1\r\n*-1\r\n");

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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let pfadd_empty_noop = b"*2\r\n$5\r\nPFADD\r\n$8\r\nemptyhll\r\n";
    let meta = parse_resp_command_arg_slices(pfadd_empty_noop, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let pfcount_empty = b"*2\r\n$7\r\nPFCOUNT\r\n$8\r\nemptyhll\r\n";
    let meta = parse_resp_command_arg_slices(pfcount_empty, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let pfadd_first = b"*5\r\n$5\r\nPFADD\r\n$2\r\nh1\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n";
    let meta = parse_resp_command_arg_slices(pfadd_first, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let pfadd_second = b"*4\r\n$5\r\nPFADD\r\n$2\r\nh1\r\n$1\r\nb\r\n$1\r\nc\r\n";
    let meta = parse_resp_command_arg_slices(pfadd_second, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let pfcount_single = b"*2\r\n$7\r\nPFCOUNT\r\n$2\r\nh1\r\n";
    let meta = parse_resp_command_arg_slices(pfcount_single, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":3\r\n");

    response.clear();
    let pfadd_other = b"*4\r\n$5\r\nPFADD\r\n$2\r\nh2\r\n$1\r\nc\r\n$1\r\nd\r\n";
    let meta = parse_resp_command_arg_slices(pfadd_other, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let pfmerge = b"*4\r\n$7\r\nPFMERGE\r\n$2\r\nhm\r\n$2\r\nh1\r\n$2\r\nh2\r\n";
    let meta = parse_resp_command_arg_slices(pfmerge, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let pfcount_merged = b"*2\r\n$7\r\nPFCOUNT\r\n$2\r\nhm\r\n";
    let meta = parse_resp_command_arg_slices(pfcount_merged, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":4\r\n");

    response.clear();
    let pfmerge_empty_dest = b"*2\r\n$7\r\nPFMERGE\r\n$6\r\ndest11\r\n";
    let meta = parse_resp_command_arg_slices(pfmerge_empty_dest, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let pfcount_empty_dest = b"*2\r\n$7\r\nPFCOUNT\r\n$6\r\ndest11\r\n";
    let meta = parse_resp_command_arg_slices(pfcount_empty_dest, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let pfmerge_dest_source_seed =
        b"*5\r\n$5\r\nPFADD\r\n$6\r\ndest22\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n";
    let meta = parse_resp_command_arg_slices(pfmerge_dest_source_seed, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let pfmerge_dest_source_only = b"*2\r\n$7\r\nPFMERGE\r\n$6\r\ndest22\r\n";
    let meta = parse_resp_command_arg_slices(pfmerge_dest_source_only, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let pfcount_dest_source_only = b"*2\r\n$7\r\nPFCOUNT\r\n$6\r\ndest22\r\n";
    let meta = parse_resp_command_arg_slices(pfcount_dest_source_only, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":3\r\n");

    response.clear();
    let pfdebug_encoding = b"*3\r\n$7\r\nPFDEBUG\r\n$8\r\nENCODING\r\n$2\r\nhm\r\n";
    let meta = parse_resp_command_arg_slices(pfdebug_encoding, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$6\r\nsparse\r\n");

    response.clear();
    let pfdebug_getreg = b"*3\r\n$7\r\nPFDEBUG\r\n$6\r\nGETREG\r\n$2\r\nhm\r\n";
    let meta = parse_resp_command_arg_slices(pfdebug_getreg, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert!(response.starts_with(b"*16384\r\n"));

    response.clear();
    let pfdebug_simd_off = b"*3\r\n$7\r\nPFDEBUG\r\n$4\r\nSIMD\r\n$3\r\nOFF\r\n";
    let meta = parse_resp_command_arg_slices(pfdebug_simd_off, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let pfdebug_simd_on = b"*3\r\n$7\r\nPFDEBUG\r\n$4\r\nSIMD\r\n$2\r\nON\r\n";
    let meta = parse_resp_command_arg_slices(pfdebug_simd_on, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let append_corrupt_hll = b"*3\r\n$6\r\nAPPEND\r\n$2\r\nhm\r\n$5\r\nhello\r\n";
    let meta = parse_resp_command_arg_slices(append_corrupt_hll, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert!(response.starts_with(b":"));

    response.clear();
    let pfcount_invalidobj = b"*2\r\n$7\r\nPFCOUNT\r\n$2\r\nhm\r\n";
    let meta = parse_resp_command_arg_slices(pfcount_invalidobj, &mut args).unwrap();
    let err = processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap_err();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-INVALIDOBJ Corrupted HLL object detected\r\n");

    response.clear();
    let set_hyll_like = b"*3\r\n$3\r\nSET\r\n$4\r\nhyll\r\n$4\r\nHYLL\r\n";
    let meta = parse_resp_command_arg_slices(set_hyll_like, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let pfdebug_getreg_invalidobj = b"*3\r\n$7\r\nPFDEBUG\r\n$6\r\nGETREG\r\n$4\r\nhyll\r\n";
    let meta = parse_resp_command_arg_slices(pfdebug_getreg_invalidobj, &mut args).unwrap();
    let err = processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap_err();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-INVALIDOBJ Corrupted HLL object detected\r\n");

    response.clear();
    let pfselftest = b"*1\r\n$10\r\nPFSELFTEST\r\n";
    let meta = parse_resp_command_arg_slices(pfselftest, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let set_plain = b"*3\r\n$3\r\nSET\r\n$5\r\nplain\r\n$5\r\nvalue\r\n";
    let meta = parse_resp_command_arg_slices(set_plain, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let pfcount_wrongtype = b"*2\r\n$7\r\nPFCOUNT\r\n$5\r\nplain\r\n";
    let meta = parse_resp_command_arg_slices(pfcount_wrongtype, &mut args).unwrap();
    let err = processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
fn hyperloglog_sparse_thresholds_match_external_scenario_in_auxiliary_db() {
    let processor = RequestProcessor::new().unwrap();

    processor.with_selected_db(DbName::new(9), || {
        for sparse_max_bytes in [100usize, 500, 3_000] {
            assert_command_response_in_db(
                &processor,
                &format!("CONFIG SET hll-sparse-max-bytes {sparse_max_bytes}"),
                b"+OK\r\n",
                DbName::new(9),
            );
            let _ = execute_command_line_in_db(&processor, "DEL hll", DbName::new(9));
            assert_command_response_in_db(&processor, "PFADD hll", b":1\r\n", DbName::new(9));

            let mut logical_len = parse_integer_response(&execute_command_line_in_db(
                &processor,
                "STRLEN hll",
                DbName::new(9),
            )) as usize;
            let mut element_index = 0usize;
            let mut guard = 0usize;

            while logical_len <= sparse_max_bytes {
                assert_command_response_in_db(
                    &processor,
                    "PFDEBUG ENCODING hll",
                    b"$6\r\nsparse\r\n",
                    DbName::new(9),
                );

                let mut command = String::from("PFADD hll");
                for _ in 0..10 {
                    command.push(' ');
                    command.push_str(&format!("elem:{sparse_max_bytes}:{element_index}"));
                    element_index += 1;
                }
                let _ = execute_command_line_in_db(&processor, &command, DbName::new(9));
                logical_len = parse_integer_response(&execute_command_line_in_db(
                    &processor,
                    "STRLEN hll",
                    DbName::new(9),
                )) as usize;
                guard += 1;
                assert!(guard < 1_000, "HLL sparse threshold test did not converge");
            }

            assert_command_response_in_db(
                &processor,
                "PFDEBUG ENCODING hll",
                b"$5\r\ndense\r\n",
                DbName::new(9),
            );
        }
    });
}

#[test]
fn hyperloglog_sparse_stress_matches_external_scenario_in_auxiliary_db() {
    let processor = RequestProcessor::new().unwrap();

    processor.with_selected_db(DbName::new(9), || {
        assert_command_response_in_db(
            &processor,
            "CONFIG SET hll-sparse-max-bytes 3000",
            b"+OK\r\n",
            DbName::new(9),
        );

        for case_index in 0..128usize {
            let _ = execute_command_line_in_db(&processor, "DEL hll1 hll2", DbName::new(9));

            let element_count = (case_index * 37) % 100;
            let mut pfadd_hll1 = String::from("PFADD hll1");
            let mut pfadd_hll2 = String::from("PFADD hll2");
            for element_index in 0..element_count {
                let element = format!("stress:{case_index}:{element_index}");
                pfadd_hll1.push(' ');
                pfadd_hll1.push_str(&element);
                pfadd_hll2.push(' ');
                pfadd_hll2.push_str(&element);
            }

            assert_command_response_in_db(&processor, "PFADD hll2", b":1\r\n", DbName::new(9));
            assert_command_response_in_db(
                &processor,
                "PFDEBUG TODENSE hll2",
                b"+OK\r\n",
                DbName::new(9),
            );
            let _ = execute_command_line_in_db(&processor, &pfadd_hll1, DbName::new(9));
            let _ = execute_command_line_in_db(&processor, &pfadd_hll2, DbName::new(9));

            assert_command_response_in_db(
                &processor,
                "PFDEBUG ENCODING hll1",
                b"$6\r\nsparse\r\n",
                DbName::new(9),
            );
            assert_command_response_in_db(
                &processor,
                "PFDEBUG ENCODING hll2",
                b"$5\r\ndense\r\n",
                DbName::new(9),
            );
            let hll1_count = execute_command_line_in_db(&processor, "PFCOUNT hll1", DbName::new(9));
            let hll2_count = execute_command_line_in_db(&processor, "PFCOUNT hll2", DbName::new(9));
            assert_eq!(hll1_count, hll2_count);
        }
    });
}

#[test]
fn hyperloglog_cache_invalidation_matches_external_scenario_in_auxiliary_db() {
    let processor = RequestProcessor::new().unwrap();

    processor.with_selected_db(DbName::new(9), || {
        let _ = execute_command_line_in_db(&processor, "DEL hll", DbName::new(9));
        assert_command_response_in_db(&processor, "PFADD hll a b c", b":1\r\n", DbName::new(9));
        let _ = execute_command_line_in_db(&processor, "PFCOUNT hll", DbName::new(9));
        assert_command_response_in_db(
            &processor,
            "GETRANGE hll 15 15",
            b"$1\r\n\x00\r\n",
            DbName::new(9),
        );
        assert_command_response_in_db(&processor, "PFADD hll a b c", b":0\r\n", DbName::new(9));
        assert_command_response_in_db(
            &processor,
            "GETRANGE hll 15 15",
            b"$1\r\n\x00\r\n",
            DbName::new(9),
        );
        assert_command_response_in_db(&processor, "PFADD hll 1 2 3", b":1\r\n", DbName::new(9));
        assert_command_response_in_db(
            &processor,
            "GETRANGE hll 15 15",
            b"$1\r\n\x80\r\n",
            DbName::new(9),
        );
    });
}

#[test]
fn db_key_ref_reads_are_scoped_by_explicit_db_without_db0_fallback() {
    let processor = RequestProcessor::new().unwrap();

    assert_command_response(&processor, "SET shared db0", b"+OK\r\n");
    processor.with_selected_db(DbName::new(9), || {
        assert_command_response_in_db(&processor, "SET shared db9", b"+OK\r\n", DbName::new(9));
        assert_command_response_in_db(
            &processor,
            "HSET obj field value",
            b":1\r\n",
            DbName::new(9),
        );
    });

    let db0_string = processor
        .read_string_value(DbKeyRef::new(DbName::default(), b"shared"))
        .unwrap()
        .unwrap();
    let db9_string = processor
        .read_string_value(DbKeyRef::new(DbName::new(9), b"shared"))
        .unwrap()
        .unwrap();
    assert_eq!(db0_string, b"db0");
    assert_eq!(db9_string, b"db9");

    assert!(
        processor
            .object_read(DbKeyRef::new(DbName::default(), b"obj"))
            .unwrap()
            .is_none()
    );
    let db9_object = processor
        .object_read(DbKeyRef::new(DbName::new(9), b"obj"))
        .unwrap();
    assert!(db9_object.is_some());
}

#[test]
fn db_key_ref_mutations_and_ttl_reads_are_scoped_by_explicit_db_without_db0_fallback() {
    let processor = RequestProcessor::new().unwrap();

    assert_command_response(&processor, "SET shared db0", b"+OK\r\n");
    processor.with_selected_db(DbName::new(9), || {
        assert_command_response_in_db(&processor, "SET shared db9", b"+OK\r\n", DbName::new(9));
        assert_command_response_in_db(
            &processor,
            "SET ttlkey value PX 5000",
            b"+OK\r\n",
            DbName::new(9),
        );
    });

    processor
        .write_string_value(
            DbKeyRef::new(DbName::new(9), b"shared"),
            b"db9-updated",
            None,
        )
        .unwrap();
    assert_eq!(
        processor
            .read_string_value(DbKeyRef::new(DbName::default(), b"shared"))
            .unwrap()
            .unwrap(),
        b"db0"
    );
    assert_eq!(
        processor
            .read_string_value(DbKeyRef::new(DbName::new(9), b"shared"))
            .unwrap()
            .unwrap(),
        b"db9-updated"
    );

    assert!(
        processor
            .delete_string_value(DbKeyRef::new(DbName::new(9), b"shared"))
            .unwrap()
    );
    assert_eq!(
        processor
            .read_string_value(DbKeyRef::new(DbName::default(), b"shared"))
            .unwrap()
            .unwrap(),
        b"db0"
    );
    assert!(
        processor
            .read_string_value(DbKeyRef::new(DbName::new(9), b"shared"))
            .unwrap()
            .is_none()
    );

    assert!(
        processor
            .expiration_unix_millis(DbKeyRef::new(DbName::new(9), b"ttlkey"))
            .is_some()
    );
    assert!(
        processor
            .expiration_unix_millis(DbKeyRef::new(DbName::default(), b"ttlkey"))
            .is_none()
    );
}

#[test]
fn execute_in_db_scopes_command_invocation_without_db0_fallback() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 8];
    let mut response = Vec::new();

    let set = b"*3\r\n$3\r\nSET\r\n$6\r\nshared\r\n$3\r\ndb9\r\n";
    let meta = parse_resp_command_arg_slices(set, &mut args).unwrap();
    processor
        .execute_in_db(&args[..meta.argument_count], &mut response, DbName::new(9))
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let get = b"*2\r\n$3\r\nGET\r\n$6\r\nshared\r\n";
    let meta = parse_resp_command_arg_slices(get, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$-1\r\n");

    response.clear();
    let meta = parse_resp_command_arg_slices(get, &mut args).unwrap();
    processor
        .execute_in_db(&args[..meta.argument_count], &mut response, DbName::new(9))
        .unwrap();
    assert_eq!(response, b"$3\r\ndb9\r\n");
}

#[test]
fn db_key_ref_object_mutations_are_scoped_by_explicit_db_without_db0_fallback() {
    let processor = RequestProcessor::new().unwrap();

    assert_command_response(&processor, "HSET obj field db0", b":1\r\n");
    processor.with_selected_db(DbName::new(9), || {
        assert_command_response_in_db(&processor, "HSET obj field db9", b":1\r\n", DbName::new(9));
    });

    let mut replacement = std::collections::BTreeMap::new();
    replacement.insert(b"field".to_vec(), b"db9-updated".to_vec());
    let payload = serialize_hash_object_payload(&replacement);

    processor
        .object_upsert(
            DbKeyRef::new(DbName::new(9), b"obj"),
            HASH_OBJECT_TYPE_TAG,
            &payload,
        )
        .unwrap();

    let db0_payload = processor
        .object_read(DbKeyRef::new(DbName::default(), b"obj"))
        .unwrap()
        .unwrap();
    let db0_hash = deserialize_hash_object_payload(&db0_payload.payload).unwrap();
    assert_eq!(db0_hash.get(&b"field"[..]).unwrap(), b"db0");

    let db9_payload = processor
        .object_read(DbKeyRef::new(DbName::new(9), b"obj"))
        .unwrap()
        .unwrap();
    let db9_hash = deserialize_hash_object_payload(&db9_payload.payload).unwrap();
    assert_eq!(db9_hash.get(&b"field"[..]).unwrap(), b"db9-updated");

    processor
        .object_delete(DbKeyRef::new(DbName::new(9), b"obj"))
        .unwrap();

    assert!(
        processor
            .object_read(DbKeyRef::new(DbName::new(9), b"obj"))
            .unwrap()
            .is_none()
    );
    assert!(
        processor
            .object_read(DbKeyRef::new(DbName::default(), b"obj"))
            .unwrap()
            .is_some()
    );
}

#[test]
fn db_key_ref_expiration_metadata_is_scoped_by_explicit_db_without_db0_fallback() {
    let processor = RequestProcessor::new().unwrap();

    assert_command_response(&processor, "SET shared db0", b"+OK\r\n");
    processor.with_selected_db(DbName::new(9), || {
        assert_command_response_in_db(&processor, "SET shared db9", b"+OK\r\n", DbName::new(9));
    });

    let deadline = current_instant() + std::time::Duration::from_secs(30);
    processor
        .set_string_expiration_deadline(DbKeyRef::new(DbName::new(9), b"shared"), Some(deadline));

    assert!(
        processor
            .string_expiration_deadline(DbKeyRef::new(DbName::default(), b"shared"))
            .is_none()
    );
    assert!(
        processor
            .string_expiration_deadline(DbKeyRef::new(DbName::new(9), b"shared"))
            .is_some()
    );

    processor.remove_string_key_metadata(DbKeyRef::new(DbName::new(9), b"shared"));

    assert_eq!(
        processor
            .read_string_value(DbKeyRef::new(DbName::default(), b"shared"))
            .unwrap()
            .unwrap(),
        b"db0"
    );
    assert_eq!(
        processor
            .read_string_value(DbKeyRef::new(DbName::new(9), b"shared"))
            .unwrap()
            .unwrap(),
        b"db9"
    );
    assert!(
        processor
            .string_expiration_deadline(DbKeyRef::new(DbName::default(), b"shared"))
            .is_none()
    );
    assert!(
        processor
            .string_expiration_deadline(DbKeyRef::new(DbName::new(9), b"shared"))
            .is_none()
    );
}

#[test]
fn db_key_ref_hash_field_expiration_metadata_is_scoped_by_explicit_db_without_db0_fallback() {
    let processor = RequestProcessor::new().unwrap();

    assert_command_response(&processor, "HSET hash field db0", b":1\r\n");
    processor.with_selected_db(DbName::new(9), || {
        assert_command_response_in_db(&processor, "HSET hash field db9", b":1\r\n", DbName::new(9));
    });

    let expiration_unix_millis = current_unix_time_millis().unwrap() + 30_000;
    processor.set_hash_field_expiration_unix_millis(
        DbKeyRef::new(DbName::new(9), b"hash"),
        b"field",
        Some(expiration_unix_millis),
    );

    assert!(
        processor
            .hash_field_expiration_unix_millis(DbKeyRef::new(DbName::default(), b"hash"), b"field")
            .is_none()
    );
    assert_eq!(
        processor
            .hash_field_expiration_unix_millis(DbKeyRef::new(DbName::new(9), b"hash"), b"field",),
        Some(expiration_unix_millis)
    );

    let shard_index = processor.object_store_shard_index_for_key(b"hash");
    processor.clear_hash_field_expirations_for_key_in_shard(
        DbKeyRef::new(DbName::new(9), b"hash"),
        shard_index,
    );

    assert!(
        processor
            .hash_field_expiration_unix_millis(DbKeyRef::new(DbName::default(), b"hash"), b"field")
            .is_none()
    );
    assert!(
        processor
            .hash_field_expiration_unix_millis(DbKeyRef::new(DbName::new(9), b"hash"), b"field")
            .is_none()
    );
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let time = b"*1\r\n$4\r\nTIME\r\n";
    let meta = parse_resp_command_arg_slices(time, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    let first_lastsave = parse_integer_response(&response);
    assert!(first_lastsave > 0);

    response.clear();
    let meta = parse_resp_command_arg_slices(lastsave, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(parse_integer_response(&response), first_lastsave);

    response.clear();
    let readonly = b"*1\r\n$8\r\nREADONLY\r\n";
    let meta = parse_resp_command_arg_slices(readonly, &mut args).unwrap();
    let err = processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    // HELLO returns a map (%7) in RESP3 with server info.
    assert!(
        response.starts_with(b"%7\r\n"),
        "expected RESP3 map from HELLO 3"
    );
    assert_eq!(
        processor.resp_protocol_version(),
        RespProtocolVersion::Resp3
    );

    response.clear();
    let reset = b"*1\r\n$5\r\nRESET\r\n";
    let meta = parse_resp_command_arg_slices(reset, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+RESET\r\n");
    assert_eq!(
        processor.resp_protocol_version(),
        RespProtocolVersion::Resp2
    );

    response.clear();
    let lolwut = b"*1\r\n$6\r\nLOLWUT\r\n";
    let meta = parse_resp_command_arg_slices(lolwut, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert!(response.starts_with(b"$"));
    assert!(response.windows(17).any(|w| w == b"Redis ver. 8.4.0\n"));

    response.clear();
    let lolwut_bad_version = b"*3\r\n$6\r\nLOLWUT\r\n$7\r\nVERSION\r\n$3\r\nbad\r\n";
    let meta = parse_resp_command_arg_slices(lolwut_bad_version, &mut args).unwrap();
    let err = processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let select_out_of_range = b"*2\r\n$6\r\nSELECT\r\n$2\r\n16\r\n";
    let meta = parse_resp_command_arg_slices(select_out_of_range, &mut args).unwrap();
    let err = processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap_err();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR DB index is out of range\r\n");

    response.clear();
    let move_same_db = b"*3\r\n$4\r\nMOVE\r\n$3\r\nkey\r\n$1\r\n0\r\n";
    let meta = parse_resp_command_arg_slices(move_same_db, &mut args).unwrap();
    let err = processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap_err();
    err.append_resp_error(&mut response);
    assert_eq!(
        response,
        b"-ERR source and destination objects are the same\r\n"
    );

    response.clear();
    let move_other_db = b"*3\r\n$4\r\nMOVE\r\n$3\r\nkey\r\n$1\r\n1\r\n";
    let meta = parse_resp_command_arg_slices(move_other_db, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let swapdb_zero = b"*3\r\n$6\r\nSWAPDB\r\n$1\r\n0\r\n$1\r\n0\r\n";
    let meta = parse_resp_command_arg_slices(swapdb_zero, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let swapdb_out_of_range = b"*3\r\n$6\r\nSWAPDB\r\n$1\r\n0\r\n$2\r\n16\r\n";
    let meta = parse_resp_command_arg_slices(swapdb_out_of_range, &mut args).unwrap();
    let err = processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap_err();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR DB index is out of range\r\n");

    response.clear();
    let client_id = b"*2\r\n$6\r\nCLIENT\r\n$2\r\nID\r\n";
    let meta = parse_resp_command_arg_slices(client_id, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let client_getname = b"*2\r\n$6\r\nCLIENT\r\n$7\r\nGETNAME\r\n";
    let meta = parse_resp_command_arg_slices(client_getname, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$-1\r\n");

    response.clear();
    let client_setname = b"*3\r\n$6\r\nCLIENT\r\n$7\r\nSETNAME\r\n$3\r\napp\r\n";
    let meta = parse_resp_command_arg_slices(client_setname, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let client_list = b"*2\r\n$6\r\nCLIENT\r\n$4\r\nLIST\r\n";
    let meta = parse_resp_command_arg_slices(client_list, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    let response_str = String::from_utf8_lossy(&response);
    assert!(
        response_str.starts_with("$"),
        "CLIENT LIST should return bulk string"
    );
    assert!(
        response_str.contains("id=1 "),
        "CLIENT LIST should contain client id"
    );
    assert!(
        response_str.contains("cmd=client|list"),
        "CLIENT LIST should contain cmd field"
    );

    response.clear();
    let client_help = b"*2\r\n$6\r\nCLIENT\r\n$4\r\nHELP\r\n";
    let meta = parse_resp_command_arg_slices(client_help, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert!(
        String::from_utf8_lossy(&response).contains("CLIENT <subcommand>"),
        "unexpected CLIENT HELP payload: {}",
        String::from_utf8_lossy(&response)
    );

    response.clear();
    let client_unblock = b"*3\r\n$6\r\nCLIENT\r\n$7\r\nUNBLOCK\r\n$1\r\n1\r\n";
    let meta = parse_resp_command_arg_slices(client_unblock, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let client_pause = b"*4\r\n$6\r\nCLIENT\r\n$5\r\nPAUSE\r\n$3\r\n100\r\n$5\r\nWRITE\r\n";
    let meta = parse_resp_command_arg_slices(client_pause, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let client_unpause = b"*2\r\n$6\r\nCLIENT\r\n$7\r\nUNPAUSE\r\n";
    let meta = parse_resp_command_arg_slices(client_unpause, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let client_no_touch = b"*3\r\n$6\r\nCLIENT\r\n$8\r\nNO-TOUCH\r\n$2\r\nON\r\n";
    let meta = parse_resp_command_arg_slices(client_no_touch, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let client_no_evict = b"*3\r\n$6\r\nCLIENT\r\n$8\r\nNO-EVICT\r\n$2\r\nON\r\n";
    let meta = parse_resp_command_arg_slices(client_no_evict, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let client_no_evict_off = b"*3\r\n$6\r\nCLIENT\r\n$8\r\nNO-EVICT\r\n$3\r\nOFF\r\n";
    let meta = parse_resp_command_arg_slices(client_no_evict_off, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let role = b"*1\r\n$4\r\nROLE\r\n";
    let meta = parse_resp_command_arg_slices(role, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"*3\r\n$6\r\nmaster\r\n:0\r\n*0\r\n");

    response.clear();
    let wait = b"*3\r\n$4\r\nWAIT\r\n$1\r\n1\r\n$2\r\n10\r\n";
    let meta = parse_resp_command_arg_slices(wait, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let waitaof = b"*4\r\n$7\r\nWAITAOF\r\n$1\r\n0\r\n$1\r\n1\r\n$2\r\n10\r\n";
    let meta = parse_resp_command_arg_slices(waitaof, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"*2\r\n:0\r\n:0\r\n");

    response.clear();
    let waitaof_err = b"*4\r\n$7\r\nWAITAOF\r\n$1\r\n1\r\n$1\r\n1\r\n$2\r\n10\r\n";
    let meta = parse_resp_command_arg_slices(waitaof_err, &mut args).unwrap();
    let err = processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let bgsave = b"*1\r\n$6\r\nBGSAVE\r\n";
    let meta = parse_resp_command_arg_slices(bgsave, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+Background saving started\r\n");

    response.clear();
    let bgrewriteaof = b"*1\r\n$12\r\nBGREWRITEAOF\r\n";
    let meta = parse_resp_command_arg_slices(bgrewriteaof, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(
        response,
        b"+Background append only file rewriting started\r\n"
    );
}

#[test]
fn select_respects_configured_database_limit_and_move_uses_selected_db_context() {
    let processor = RequestProcessor::new().unwrap();
    assert_command_response(&processor, "CONFIG SET databases 4", b"+OK\r\n");
    assert_command_response(&processor, "SELECT 3", b"+OK\r\n");
    assert_command_error(&processor, "SELECT 4", b"-ERR DB index is out of range\r\n");
    assert_command_response(&processor, "SWAPDB 1 1", b"+OK\r\n");
    assert_command_response(&processor, "SWAPDB 1 2", b"+OK\r\n");
    assert_command_error(
        &processor,
        "SWAPDB 4 5",
        b"-ERR DB index is out of range\r\n",
    );
    assert_command_error(
        &processor,
        "SWAPDB 4 a",
        b"-ERR invalid second DB index\r\n",
    );
    assert_command_error(&processor, "SWAPDB a 5", b"-ERR invalid first DB index\r\n");

    let frame = encode_resp(&[b"MOVE", b"ctx-key", b"2"]);
    let mut args = [ArgSlice::EMPTY; 8];
    let meta = parse_resp_command_arg_slices(&frame, &mut args).unwrap();
    let mut response = Vec::new();
    let err = processor
        .execute_with_client_context_in_db(
            &args[..meta.argument_count],
            &mut response,
            false,
            None,
            false,
            DbName::new(2),
        )
        .unwrap_err();
    err.append_resp_error(&mut response);
    assert_eq!(
        response,
        b"-ERR source and destination objects are the same\r\n"
    );

    let frame = encode_resp(&[b"MOVE", b"ctx-key", b"1"]);
    let meta = parse_resp_command_arg_slices(&frame, &mut args).unwrap();
    response.clear();
    processor
        .execute_with_client_context_in_db(
            &args[..meta.argument_count],
            &mut response,
            false,
            None,
            false,
            DbName::new(2),
        )
        .unwrap();
    assert_eq!(response, b":0\r\n");
}

#[test]
fn multidb_select_keeps_keyspaces_separate_for_strings_and_objects() {
    let processor = RequestProcessor::new().unwrap();
    assert_command_response(&processor, "CONFIG SET databases 4", b"+OK\r\n");

    assert_eq!(
        execute_command_line_in_db(&processor, "SET shared zero", DbName::new(0)),
        b"+OK\r\n"
    );
    assert_eq!(
        execute_command_line_in_db(&processor, "RPUSH list a", DbName::new(2)),
        b":1\r\n"
    );

    assert_eq!(
        execute_command_line_in_db(&processor, "GET shared", DbName::new(0)),
        b"$4\r\nzero\r\n"
    );
    assert_eq!(
        execute_command_line_in_db(&processor, "GET shared", DbName::new(1)),
        b"$-1\r\n"
    );
    assert_eq!(
        execute_command_line_in_db(&processor, "TYPE list", DbName::new(0)),
        b"+none\r\n"
    );
    assert_eq!(
        execute_command_line_in_db(&processor, "TYPE list", DbName::new(2)),
        b"+list\r\n"
    );
    assert_eq!(
        execute_command_line_in_db(&processor, "DBSIZE", DbName::new(0)),
        b":1\r\n"
    );
    assert_eq!(
        execute_command_line_in_db(&processor, "DBSIZE", DbName::new(2)),
        b":1\r\n"
    );
    assert_eq!(
        execute_command_line_in_db(&processor, "KEYS *", DbName::new(2)),
        b"*1\r\n$4\r\nlist\r\n"
    );
}

#[test]
fn multidb_copy_move_and_flushdb_preserve_values_and_expiration() {
    let processor = RequestProcessor::new().unwrap();
    assert_command_response(&processor, "CONFIG SET databases 4", b"+OK\r\n");

    assert_eq!(
        execute_command_line_in_db(&processor, "SET src hello PX 60000", DbName::new(0)),
        b"+OK\r\n"
    );
    assert_eq!(
        execute_command_line_in_db(&processor, "COPY src copied DB 1", DbName::new(0)),
        b":1\r\n"
    );
    assert_eq!(
        execute_command_line_in_db(&processor, "GET copied", DbName::new(1)),
        b"$5\r\nhello\r\n"
    );
    let copied_pttl = execute_command_line_in_db(&processor, "PTTL copied", DbName::new(1));
    let copied_ttl = parse_integer_response(&copied_pttl);
    assert!(copied_ttl > 0, "expected positive TTL, got {copied_ttl}");

    assert_eq!(
        execute_command_line_in_db(&processor, "MOVE copied 3", DbName::new(1)),
        b":1\r\n"
    );
    assert_eq!(
        execute_command_line_in_db(&processor, "GET copied", DbName::new(1)),
        b"$-1\r\n"
    );
    assert_eq!(
        execute_command_line_in_db(&processor, "GET copied", DbName::new(3)),
        b"$5\r\nhello\r\n"
    );
    let moved_pttl = execute_command_line_in_db(&processor, "PTTL copied", DbName::new(3));
    let moved_ttl = parse_integer_response(&moved_pttl);
    assert!(
        moved_ttl > 0,
        "expected moved key to keep expiration, got {moved_ttl}"
    );

    assert_eq!(
        execute_command_line_in_db(&processor, "FLUSHDB", DbName::new(3)),
        b"+OK\r\n"
    );
    assert_eq!(
        execute_command_line_in_db(&processor, "DBSIZE", DbName::new(3)),
        b":0\r\n"
    );
    assert_eq!(
        execute_command_line_in_db(&processor, "GET src", DbName::new(0)),
        b"$5\r\nhello\r\n"
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"*0\r\n");

    response.clear();
    let module_help = b"*2\r\n$6\r\nMODULE\r\n$4\r\nHELP\r\n";
    let meta = parse_resp_command_arg_slices(module_help, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert!(response.starts_with(b"*11\r\n"));
    let module_help_text = std::str::from_utf8(&response).unwrap();
    assert!(module_help_text.contains("MODULE <subcommand>"));

    response.clear();
    let latency_help = b"*2\r\n$7\r\nLATENCY\r\n$4\r\nHELP\r\n";
    let meta = parse_resp_command_arg_slices(latency_help, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert!(response.starts_with(b"*15\r\n"));
    let latency_help_text = std::str::from_utf8(&response).unwrap();
    assert!(latency_help_text.contains("\r\nLATEST\r\n"));

    response.clear();
    let latency_help_extra = b"*3\r\n$7\r\nLATENCY\r\n$4\r\nHELP\r\n$3\r\nxxx\r\n";
    let meta = parse_resp_command_arg_slices(latency_help_extra, &mut args).unwrap();
    let err = processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap_err();
    err.append_resp_error(&mut response);
    assert_eq!(
        response,
        b"-ERR wrong number of arguments for 'latency|help' command\r\n"
    );

    response.clear();
    let latency_latest = b"*2\r\n$7\r\nLATENCY\r\n$6\r\nLATEST\r\n";
    let meta = parse_resp_command_arg_slices(latency_latest, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"*0\r\n");

    response.clear();
    let latency_history = b"*3\r\n$7\r\nLATENCY\r\n$7\r\nHISTORY\r\n$7\r\ncommand\r\n";
    let meta = parse_resp_command_arg_slices(latency_history, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"*0\r\n");

    response.clear();
    let latency_reset = b"*2\r\n$7\r\nLATENCY\r\n$5\r\nRESET\r\n";
    let meta = parse_resp_command_arg_slices(latency_reset, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let latency_doctor = b"*2\r\n$7\r\nLATENCY\r\n$6\r\nDOCTOR\r\n";
    let meta = parse_resp_command_arg_slices(latency_doctor, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert!(response.starts_with(b"$"));
    let latency_doctor_text = std::str::from_utf8(&response).unwrap();
    assert!(latency_doctor_text.contains("Latency monitoring is disabled"));

    response.clear();
    let slowlog_len = b"*2\r\n$7\r\nSLOWLOG\r\n$3\r\nLEN\r\n";
    let meta = parse_resp_command_arg_slices(slowlog_len, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let slowlog_get_default = b"*2\r\n$7\r\nSLOWLOG\r\n$3\r\nGET\r\n";
    let meta = parse_resp_command_arg_slices(slowlog_get_default, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"*0\r\n");

    response.clear();
    let slowlog_get_count = b"*3\r\n$7\r\nSLOWLOG\r\n$3\r\nGET\r\n$1\r\n2\r\n";
    let meta = parse_resp_command_arg_slices(slowlog_get_count, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"*0\r\n");

    response.clear();
    let slowlog_reset = b"*2\r\n$7\r\nSLOWLOG\r\n$5\r\nRESET\r\n";
    let meta = parse_resp_command_arg_slices(slowlog_reset, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let slowlog_help = b"*2\r\n$7\r\nSLOWLOG\r\n$4\r\nHELP\r\n";
    let meta = parse_resp_command_arg_slices(slowlog_help, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert!(response.starts_with(b"*12\r\n"));

    response.clear();
    let module_unknown = b"*2\r\n$6\r\nMODULE\r\n$4\r\nNOPE\r\n";
    let meta = parse_resp_command_arg_slices(module_unknown, &mut args).unwrap();
    let err = processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap_err();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR unknown subcommand\r\n");

    response.clear();
    let latency_unknown = b"*2\r\n$7\r\nLATENCY\r\n$4\r\nNOPE\r\n";
    let meta = parse_resp_command_arg_slices(latency_unknown, &mut args).unwrap();
    let err = processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap_err();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR unknown subcommand\r\n");

    response.clear();
    let slowlog_bad_count = b"*3\r\n$7\r\nSLOWLOG\r\n$3\r\nGET\r\n$3\r\nbad\r\n";
    let meta = parse_resp_command_arg_slices(slowlog_bad_count, &mut args).unwrap();
    let err = processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap_err();
    err.append_resp_error(&mut response);
    assert_eq!(
        response,
        b"-ERR value is not an integer or out of range\r\n"
    );
}

#[test]
fn slowlog_surface_validation_matches_external_scenarios() {
    let processor = RequestProcessor::new().unwrap();

    assert_command_response(
        &processor,
        "CONFIG SET slowlog-log-slower-than -1",
        b"+OK\r\n",
    );
    assert_command_response(
        &processor,
        "CONFIG GET slowlog-log-slower-than",
        b"*2\r\n$23\r\nslowlog-log-slower-than\r\n$2\r\n-1\r\n",
    );

    assert_command_error(
        &processor,
        "SLOWLOG GET -2",
        b"-ERR count should be greater than or equal to -1\r\n",
    );
    assert_command_error(
        &processor,
        "SLOWLOG GET -222",
        b"-ERR count should be greater than or equal to -1\r\n",
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$7\r\ndefault\r\n");

    response.clear();
    let acl_users = b"*2\r\n$3\r\nACL\r\n$5\r\nUSERS\r\n";
    let meta = parse_resp_command_arg_slices(acl_users, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"*1\r\n$7\r\ndefault\r\n");

    response.clear();
    let acl_setuser = b"*4\r\n$3\r\nACL\r\n$7\r\nSETUSER\r\n$7\r\ndefault\r\n$2\r\non\r\n";
    let meta = parse_resp_command_arg_slices(acl_setuser, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let cluster_keyslot = b"*3\r\n$7\r\nCLUSTER\r\n$7\r\nKEYSLOT\r\n$5\r\nuser1\r\n";
    let meta = parse_resp_command_arg_slices(cluster_keyslot, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    let slot = SlotNumber::new(u16::try_from(parse_integer_response(&response)).unwrap());
    assert_eq!(slot, redis_hash_slot(b"user1"));

    response.clear();
    let cluster_info = b"*2\r\n$7\r\nCLUSTER\r\n$4\r\nINFO\r\n";
    let meta = parse_resp_command_arg_slices(cluster_info, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    let cluster_info_text = std::str::from_utf8(&response).unwrap();
    assert!(cluster_info_text.contains("cluster_slots_assigned:16384"));

    response.clear();
    let failover = b"*1\r\n$8\r\nFAILOVER\r\n";
    let meta = parse_resp_command_arg_slices(failover, &mut args).unwrap();
    let err = processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let shutdown = b"*2\r\n$8\r\nSHUTDOWN\r\n$6\r\nNOSAVE\r\n";
    let meta = parse_resp_command_arg_slices(shutdown, &mut args).unwrap();
    let err = processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
    let pubsub_help = execute_command_line(&processor, "PUBSUB HELP").unwrap();
    assert!(
        String::from_utf8_lossy(&pubsub_help).contains("PUBSUB <subcommand>"),
        "unexpected PUBSUB HELP payload: {}",
        String::from_utf8_lossy(&pubsub_help)
    );

    assert_command_error(
        &processor,
        "SUBSCRIBE",
        b"-ERR wrong number of arguments for 'subscribe' command\r\n",
    );
    assert_command_error(&processor, "PUBSUB NOPE", b"-ERR unknown subcommand\r\n");
}

#[test]
fn pubsub_subscribe_publish_and_pattern_delivery_use_stateful_client_context() {
    let processor = RequestProcessor::new().unwrap();
    let client1 = ClientId::new(1);
    let client2 = ClientId::new(2);

    processor.register_pubsub_client(client1);
    processor.register_pubsub_client(client2);

    assert_client_command_response(
        &processor,
        "SUBSCRIBE chan1 chan1 chan1",
        client1,
        b"*3\r\n$9\r\nsubscribe\r\n$5\r\nchan1\r\n:1\r\n*3\r\n$9\r\nsubscribe\r\n$5\r\nchan1\r\n:1\r\n*3\r\n$9\r\nsubscribe\r\n$5\r\nchan1\r\n:1\r\n",
    );
    assert_command_response(&processor, "PUBLISH chan1 hello", b":1\r\n");
    assert_eq!(
        processor.take_pending_pubsub_messages(client1),
        vec![b"*3\r\n$7\r\nmessage\r\n$5\r\nchan1\r\n$5\r\nhello\r\n".to_vec()]
    );

    assert_client_command_response(
        &processor,
        "SUBSCRIBE chan1",
        client2,
        b"*3\r\n$9\r\nsubscribe\r\n$5\r\nchan1\r\n:1\r\n",
    );
    assert_command_response(&processor, "PUBLISH chan1 world", b":2\r\n");
    assert_eq!(
        processor.take_pending_pubsub_messages(client1),
        vec![b"*3\r\n$7\r\nmessage\r\n$5\r\nchan1\r\n$5\r\nworld\r\n".to_vec()]
    );
    assert_eq!(
        processor.take_pending_pubsub_messages(client2),
        vec![b"*3\r\n$7\r\nmessage\r\n$5\r\nchan1\r\n$5\r\nworld\r\n".to_vec()]
    );

    assert_client_command_response(
        &processor,
        "PSUBSCRIBE chan.*",
        client1,
        b"*3\r\n$10\r\npsubscribe\r\n$6\r\nchan.*\r\n:2\r\n",
    );
    assert_command_response(&processor, "PUBLISH chan.foo hello", b":1\r\n");
    assert_eq!(
        processor.take_pending_pubsub_messages(client1),
        vec![b"*4\r\n$8\r\npmessage\r\n$6\r\nchan.*\r\n$8\r\nchan.foo\r\n$5\r\nhello\r\n".to_vec()]
    );
}

#[test]
fn flush_commands_emit_global_tracking_invalidation_even_without_tracked_keys() {
    let processor = RequestProcessor::new().unwrap();
    let source_client = ClientId::new(101);
    let redirect_client = ClientId::new(202);
    let expected_invalidation =
        b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n$0\r\n\r\n".to_vec();

    processor.register_pubsub_client(source_client);
    processor.register_pubsub_client(redirect_client);
    processor.configure_client_tracking(
        source_client,
        ClientTrackingConfig {
            mode: ClientTrackingModeSetting::On,
            redirect_id: Some(redirect_client),
            ..ClientTrackingConfig::default()
        },
    );

    assert_client_command_response(&processor, "FLUSHDB", source_client, b"+OK\r\n");
    assert_eq!(
        processor.take_pending_pubsub_messages(redirect_client),
        vec![expected_invalidation.clone()]
    );

    assert_client_command_response(&processor, "FLUSHALL", source_client, b"+OK\r\n");
    assert_eq!(
        processor.take_pending_pubsub_messages(redirect_client),
        vec![expected_invalidation]
    );
}

#[test]
fn keyspace_notifications_use_selected_db_index() {
    let processor = RequestProcessor::new().unwrap();
    let client = ClientId::new(7);
    processor.register_pubsub_client(client);

    assert_command_response(
        &processor,
        "CONFIG SET notify-keyspace-events KEA",
        b"+OK\r\n",
    );
    assert_client_command_response(
        &processor,
        "SUBSCRIBE __keyevent@1__:set",
        client,
        b"*3\r\n$9\r\nsubscribe\r\n$18\r\n__keyevent@1__:set\r\n:1\r\n",
    );
    assert_client_command_response(
        &processor,
        "SUBSCRIBE __keyspace@1__:notify:key",
        client,
        b"*3\r\n$9\r\nsubscribe\r\n$25\r\n__keyspace@1__:notify:key\r\n:2\r\n",
    );

    let frame = encode_resp(&[b"SET", b"notify:key", b"v"]);
    let mut args = [ArgSlice::EMPTY; 16];
    let meta = parse_resp_command_arg_slices(&frame, &mut args).unwrap();
    let mut response = Vec::new();
    processor
        .execute_with_client_context_in_db(
            &args[..meta.argument_count],
            &mut response,
            false,
            Some(client),
            false,
            DbName::new(1),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    assert_eq!(
        processor.take_pending_pubsub_messages(client),
        vec![
            b"*3\r\n$7\r\nmessage\r\n$25\r\n__keyspace@1__:notify:key\r\n$3\r\nset\r\n".to_vec(),
            b"*3\r\n$7\r\nmessage\r\n$18\r\n__keyevent@1__:set\r\n$10\r\nnotify:key\r\n".to_vec(),
        ]
    );
}

#[test]
fn keyspace_notifications_set_overwrite_emits_overwritten_before_set() {
    let processor = RequestProcessor::new().unwrap();
    let client = ClientId::new(71);
    processor.register_pubsub_client(client);

    assert_command_response(
        &processor,
        "CONFIG SET notify-keyspace-events EAo",
        b"+OK\r\n",
    );
    assert_client_command_response(
        &processor,
        "PSUBSCRIBE __keyevent@0__:*",
        client,
        b"*3\r\n$10\r\npsubscribe\r\n$16\r\n__keyevent@0__:*\r\n:1\r\n",
    );

    assert_command_response(&processor, "SET owkey v1", b"+OK\r\n");
    let _ = processor.take_pending_pubsub_messages(client);

    assert_command_response(&processor, "SET owkey v2", b"+OK\r\n");
    assert_eq!(
        processor.take_pending_pubsub_messages(client),
        vec![
            b"*4\r\n$8\r\npmessage\r\n$16\r\n__keyevent@0__:*\r\n$26\r\n__keyevent@0__:overwritten\r\n$5\r\nowkey\r\n".to_vec(),
            b"*4\r\n$8\r\npmessage\r\n$16\r\n__keyevent@0__:*\r\n$18\r\n__keyevent@0__:set\r\n$5\r\nowkey\r\n".to_vec(),
        ]
    );
}

#[test]
fn keyspace_notifications_setkey_commands_emit_overwritten_and_type_changed() {
    let processor = RequestProcessor::new().unwrap();
    let client = ClientId::new(72);
    processor.register_pubsub_client(client);

    assert_command_response(
        &processor,
        "CONFIG SET notify-keyspace-events Eoc",
        b"+OK\r\n",
    );
    assert_client_command_response(
        &processor,
        "PSUBSCRIBE __keyevent@0__:*",
        client,
        b"*3\r\n$10\r\npsubscribe\r\n$16\r\n__keyevent@0__:*\r\n:1\r\n",
    );

    assert_command_response(&processor, "SET dst x", b"+OK\r\n");
    assert_command_response(&processor, "SADD s1 a", b":1\r\n");
    assert_command_response(&processor, "SADD s2 b", b":1\r\n");
    assert_command_response(&processor, "SUNIONSTORE dst s1 s2", b":2\r\n");

    assert_eq!(
        processor.take_pending_pubsub_messages(client),
        vec![
            b"*4\r\n$8\r\npmessage\r\n$16\r\n__keyevent@0__:*\r\n$26\r\n__keyevent@0__:overwritten\r\n$3\r\ndst\r\n".to_vec(),
            b"*4\r\n$8\r\npmessage\r\n$16\r\n__keyevent@0__:*\r\n$27\r\n__keyevent@0__:type_changed\r\n$3\r\ndst\r\n".to_vec(),
        ]
    );
}

#[test]
fn hsetex_px_one_emits_hexpire_not_immediate_hdel() {
    let processor = RequestProcessor::new().unwrap();
    let client = ClientId::new(73);
    processor.register_pubsub_client(client);

    assert_command_response(
        &processor,
        "CONFIG SET notify-keyspace-events Kh",
        b"+OK\r\n",
    );
    assert_client_command_response(
        &processor,
        "PSUBSCRIBE __keyspace@0__:myhash",
        client,
        b"*3\r\n$10\r\npsubscribe\r\n$21\r\n__keyspace@0__:myhash\r\n:1\r\n",
    );

    assert_command_response(&processor, "HSETEX myhash PX 1 FIELDS 1 f1 v1", b":1\r\n");
    let messages = processor.take_pending_pubsub_messages(client);
    assert!(
        messages.len() >= 2,
        "expected at least 2 pubsub messages, got {}",
        messages.len()
    );
    assert_eq!(
        messages[0],
        b"*4\r\n$8\r\npmessage\r\n$21\r\n__keyspace@0__:myhash\r\n$21\r\n__keyspace@0__:myhash\r\n$4\r\nhset\r\n".to_vec()
    );
    assert_eq!(
        messages[1],
        b"*4\r\n$8\r\npmessage\r\n$21\r\n__keyspace@0__:myhash\r\n$21\r\n__keyspace@0__:myhash\r\n$7\r\nhexpire\r\n".to_vec()
    );
}

#[test]
fn stream_keyspace_notifications_match_external_pubsub_scenario() {
    let processor = RequestProcessor::new().unwrap();
    let client = ClientId::new(74);
    processor.register_pubsub_client(client);

    assert_command_response(
        &processor,
        "CONFIG SET notify-keyspace-events Kt",
        b"+OK\r\n",
    );
    assert_client_command_response(
        &processor,
        "PSUBSCRIBE *",
        client,
        b"*3\r\n$10\r\npsubscribe\r\n$1\r\n*\r\n:1\r\n",
    );

    assert_command_response(&processor, "DEL mystream", b":0\r\n");
    assert_command_response(
        &processor,
        "XGROUP CREATE mystream mygroup $ MKSTREAM",
        b"+OK\r\n",
    );
    assert_command_response(
        &processor,
        "XGROUP CREATECONSUMER mystream mygroup Bob",
        b":1\r\n",
    );
    assert_command_response(&processor, "XADD mystream 1 field1 A", b"$3\r\n1-0\r\n");

    let xreadgroup = execute_command_line(
        &processor,
        "XREADGROUP GROUP mygroup Alice STREAMS mystream >",
    )
    .unwrap();
    assert!(
        xreadgroup.starts_with(b"*1\r\n"),
        "unexpected XREADGROUP reply: {}",
        String::from_utf8_lossy(&xreadgroup)
    );

    let xclaim =
        execute_command_line(&processor, "XCLAIM mystream mygroup Mike 0 1-0 FORCE").unwrap();
    assert!(
        xclaim.starts_with(b"*1\r\n"),
        "unexpected XCLAIM reply: {}",
        String::from_utf8_lossy(&xclaim)
    );

    assert_command_response(
        &processor,
        "XGROUP DELCONSUMER mystream mygroup Lee",
        b":0\r\n",
    );

    let xautoclaim =
        execute_command_line(&processor, "XAUTOCLAIM mystream mygroup Bob 0 1-0").unwrap();
    assert!(
        xautoclaim.starts_with(b"*3\r\n"),
        "unexpected XAUTOCLAIM reply: {}",
        String::from_utf8_lossy(&xautoclaim)
    );

    assert_command_response(
        &processor,
        "XGROUP DELCONSUMER mystream mygroup Bob",
        b":1\r\n",
    );

    assert_eq!(
        processor.take_pending_pubsub_messages(client),
        vec![
            encode_resp(&[
                b"pmessage",
                b"*",
                b"__keyspace@0__:mystream",
                b"xgroup-create",
            ]),
            encode_resp(&[
                b"pmessage",
                b"*",
                b"__keyspace@0__:mystream",
                b"xgroup-createconsumer",
            ]),
            encode_resp(&[b"pmessage", b"*", b"__keyspace@0__:mystream", b"xadd"]),
            encode_resp(&[
                b"pmessage",
                b"*",
                b"__keyspace@0__:mystream",
                b"xgroup-createconsumer",
            ]),
            encode_resp(&[
                b"pmessage",
                b"*",
                b"__keyspace@0__:mystream",
                b"xgroup-createconsumer",
            ]),
            encode_resp(&[
                b"pmessage",
                b"*",
                b"__keyspace@0__:mystream",
                b"xgroup-delconsumer",
            ]),
        ]
    );
}

#[test]
fn stream_mutation_notifications_cover_xdel_xtrim_setid_destroy_and_zero_pending_delconsumer() {
    let processor = RequestProcessor::new().unwrap();

    assert_command_response(
        &processor,
        "CONFIG SET notify-keyspace-events Kt",
        b"+OK\r\n",
    );
    assert_command_response(
        &processor,
        "XGROUP CREATE mystream mygroup $ MKSTREAM",
        b"+OK\r\n",
    );
    assert_command_response(
        &processor,
        "XGROUP CREATECONSUMER mystream mygroup idle",
        b":1\r\n",
    );

    let client = ClientId::new(75);
    processor.register_pubsub_client(client);
    assert_client_command_response(
        &processor,
        "PSUBSCRIBE __keyspace@0__:mystream",
        client,
        b"*3\r\n$10\r\npsubscribe\r\n$23\r\n__keyspace@0__:mystream\r\n:1\r\n",
    );

    assert_command_response(
        &processor,
        "XGROUP DELCONSUMER mystream mygroup idle",
        b":0\r\n",
    );
    assert_command_response(&processor, "XADD mystream 1 field value", b"$3\r\n1-0\r\n");
    assert_command_response(&processor, "XDEL mystream 1-0", b":1\r\n");
    assert_command_response(&processor, "XADD mystream 2 field value", b"$3\r\n2-0\r\n");
    assert_command_response(&processor, "XTRIM mystream MAXLEN = 0", b":1\r\n");
    assert_command_response(&processor, "XGROUP SETID mystream mygroup 0-0", b"+OK\r\n");
    assert_command_response(&processor, "XGROUP DESTROY mystream mygroup", b":1\r\n");

    assert_eq!(
        processor.take_pending_pubsub_messages(client),
        vec![
            encode_resp(&[
                b"pmessage",
                b"__keyspace@0__:mystream",
                b"__keyspace@0__:mystream",
                b"xgroup-delconsumer",
            ]),
            encode_resp(&[
                b"pmessage",
                b"__keyspace@0__:mystream",
                b"__keyspace@0__:mystream",
                b"xadd",
            ]),
            encode_resp(&[
                b"pmessage",
                b"__keyspace@0__:mystream",
                b"__keyspace@0__:mystream",
                b"xdel",
            ]),
            encode_resp(&[
                b"pmessage",
                b"__keyspace@0__:mystream",
                b"__keyspace@0__:mystream",
                b"xadd",
            ]),
            encode_resp(&[
                b"pmessage",
                b"__keyspace@0__:mystream",
                b"__keyspace@0__:mystream",
                b"xtrim",
            ]),
            encode_resp(&[
                b"pmessage",
                b"__keyspace@0__:mystream",
                b"__keyspace@0__:mystream",
                b"xgroup-setid",
            ]),
            encode_resp(&[
                b"pmessage",
                b"__keyspace@0__:mystream",
                b"__keyspace@0__:mystream",
                b"xgroup-destroy",
            ]),
        ]
    );
}

#[test]
fn pubsub_unsubscribe_non_subscribed_targets_keep_zero_subscription_count() {
    let processor = RequestProcessor::new().unwrap();
    let client = ClientId::new(9);
    processor.register_pubsub_client(client);

    assert_client_command_response(
        &processor,
        "UNSUBSCRIBE foo bar quux",
        client,
        b"*3\r\n$11\r\nunsubscribe\r\n$3\r\nfoo\r\n:0\r\n*3\r\n$11\r\nunsubscribe\r\n$3\r\nbar\r\n:0\r\n*3\r\n$11\r\nunsubscribe\r\n$4\r\nquux\r\n:0\r\n",
    );
    assert_client_command_response(
        &processor,
        "PUNSUBSCRIBE foo.* bar.* quux.*",
        client,
        b"*3\r\n$12\r\npunsubscribe\r\n$5\r\nfoo.*\r\n:0\r\n*3\r\n$12\r\npunsubscribe\r\n$5\r\nbar.*\r\n:0\r\n*3\r\n$12\r\npunsubscribe\r\n$6\r\nquux.*\r\n:0\r\n",
    );
}

#[test]
fn pubsub_ping_uses_resp2_pubsub_array_shape_while_client_is_subscribed() {
    let processor = RequestProcessor::new().unwrap();
    let client = ClientId::new(42);
    processor.register_pubsub_client(client);
    processor.set_resp_protocol_version(RespProtocolVersion::Resp2);

    assert_client_command_response(
        &processor,
        "SUBSCRIBE somechannel",
        client,
        b"*3\r\n$9\r\nsubscribe\r\n$11\r\nsomechannel\r\n:1\r\n",
    );
    assert_client_command_response(
        &processor,
        "PING",
        client,
        b"*2\r\n$4\r\npong\r\n$0\r\n\r\n",
    );
    assert_client_command_response(
        &processor,
        "PING foo",
        client,
        b"*2\r\n$4\r\npong\r\n$3\r\nfoo\r\n",
    );
    assert_client_command_response(
        &processor,
        "UNSUBSCRIBE somechannel",
        client,
        b"*3\r\n$11\r\nunsubscribe\r\n$11\r\nsomechannel\r\n:0\r\n",
    );
    assert_client_command_response(&processor, "PING", client, b"+PONG\r\n");
}

#[test]
fn pubsub_numpat_counts_unique_patterns_across_clients() {
    let processor = RequestProcessor::new().unwrap();
    let client1 = ClientId::new(70);
    let client2 = ClientId::new(71);
    processor.register_pubsub_client(client1);
    processor.register_pubsub_client(client2);

    assert_client_command_response(
        &processor,
        "PSUBSCRIBE foo*",
        client1,
        b"*3\r\n$10\r\npsubscribe\r\n$4\r\nfoo*\r\n:1\r\n",
    );
    assert_client_command_response(
        &processor,
        "PSUBSCRIBE foo* bar*",
        client2,
        b"*3\r\n$10\r\npsubscribe\r\n$4\r\nfoo*\r\n:1\r\n*3\r\n$10\r\npsubscribe\r\n$4\r\nbar*\r\n:2\r\n",
    );
    assert_client_command_response(
        &processor,
        "PSUBSCRIBE baz*",
        client1,
        b"*3\r\n$10\r\npsubscribe\r\n$4\r\nbaz*\r\n:2\r\n",
    );
    assert_command_response(&processor, "PUBSUB NUMPAT", b":3\r\n");
}

#[test]
fn pubsub_unsubscribe_inside_transaction_context_uses_client_id() {
    let processor = RequestProcessor::new().unwrap();
    let client = ClientId::new(99);
    processor.register_pubsub_client(client);

    // Subscribe to 3 channels.
    assert_client_command_response(
        &processor,
        "SUBSCRIBE foo bar baz",
        client,
        b"*3\r\n$9\r\nsubscribe\r\n$3\r\nfoo\r\n:1\r\n\
          *3\r\n$9\r\nsubscribe\r\n$3\r\nbar\r\n:2\r\n\
          *3\r\n$9\r\nsubscribe\r\n$3\r\nbaz\r\n:3\r\n",
    );

    // Execute UNSUBSCRIBE bar inside a transaction context with client_id.
    let frame = encode_resp(&[b"UNSUBSCRIBE", b"bar"]);
    let mut args = [ArgSlice::EMPTY; 16];
    let meta = parse_resp_command_arg_slices(&frame, &mut args).unwrap();
    let mut response = Vec::new();
    processor
        .execute_with_client_no_touch_in_transaction_in_db(
            &args[..meta.argument_count],
            &mut response,
            false,
            Some(client),
            DbName::default(),
        )
        .unwrap();
    // After removing bar, 2 subscriptions remain (foo, baz).
    assert_eq!(
        response, b"*3\r\n$11\r\nunsubscribe\r\n$3\r\nbar\r\n:2\r\n",
        "UNSUBSCRIBE bar inside transaction should report 2 remaining subscriptions"
    );

    // Execute UNSUBSCRIBE baz inside a transaction context with client_id.
    let frame = encode_resp(&[b"UNSUBSCRIBE", b"baz"]);
    let meta = parse_resp_command_arg_slices(&frame, &mut args).unwrap();
    response.clear();
    processor
        .execute_with_client_no_touch_in_transaction_in_db(
            &args[..meta.argument_count],
            &mut response,
            false,
            Some(client),
            DbName::default(),
        )
        .unwrap();
    // After removing baz, 1 subscription remains (foo).
    assert_eq!(
        response, b"*3\r\n$11\r\nunsubscribe\r\n$3\r\nbaz\r\n:1\r\n",
        "UNSUBSCRIBE baz inside transaction should report 1 remaining subscription"
    );

    // Publish to foo — the client still has that subscription.
    assert_command_response(&processor, "PUBLISH foo hello", b":1\r\n");
    assert_eq!(
        processor.take_pending_pubsub_messages(client),
        vec![b"*3\r\n$7\r\nmessage\r\n$3\r\nfoo\r\n$5\r\nhello\r\n".to_vec()]
    );

    // Publish to bar — nobody is subscribed anymore.
    assert_command_response(&processor, "PUBLISH bar hello", b":0\r\n");
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

    assert_command_response(&processor, "GEOPOS sicily unknown", b"*1\r\n*-1\r\n");
    assert_command_response(&processor, "GEOPOS missing unknown", b"*1\r\n*-1\r\n");
    assert_command_response(
        &processor,
        "GEOPOS sicily palermo unknown",
        b"*2\r\n*2\r\n$20\r\n13.36138933897018433\r\n$20\r\n38.11555639549629859\r\n*-1\r\n",
    );

    assert_command_response(&processor, "SET plain value", b"+OK\r\n");
    assert_command_error(
        &processor,
        "GEOPOS plain member",
        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
    );
    assert_command_response(&processor, "GEOPOS sicily", b"*0\r\n");

    // RESP3: GEOPOS coordinates should use double type (,value\r\n).
    processor.set_resp_protocol_version(RespProtocolVersion::Resp3);
    let response = execute_frame(
        &processor,
        &encode_resp(&[b"GEOPOS", b"sicily", b"palermo"]),
    );
    // Expect *1 outer array, *2 coordinate pair, then ,<lon>\r\n,<lat>\r\n.
    assert!(response.starts_with(b"*1\r\n*2\r\n,"));
    processor.set_resp_protocol_version(RespProtocolVersion::Resp2);
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
fn geo_commands_are_scoped_by_selected_db_without_db0_fallback() {
    let processor = RequestProcessor::new().unwrap();
    let db0 = DbName::default();
    let db9 = DbName::new(9);

    assert_eq!(
        execute_command_line_in_db(&processor, "GEOADD shared 0 0 tokyo 30 0 far", db0),
        b":2\r\n".to_vec()
    );
    assert_eq!(
        execute_command_line_in_db(&processor, "GEOADD shared 0 0 tokyo 0.1 0 near", db9),
        b":2\r\n".to_vec()
    );

    assert_eq!(
        execute_command_line_in_db(&processor, "GEORADIUSBYMEMBER shared tokyo 20 km ASC", db0,),
        b"*1\r\n$5\r\ntokyo\r\n".to_vec()
    );
    assert_eq!(
        execute_command_line_in_db(&processor, "GEORADIUSBYMEMBER shared tokyo 20 km ASC", db9,),
        b"*2\r\n$5\r\ntokyo\r\n$4\r\nnear\r\n".to_vec()
    );

    assert_eq!(
        execute_command_line_in_db(
            &processor,
            "GEOSEARCHSTORE out shared FROMMEMBER tokyo BYRADIUS 20 km",
            db9,
        ),
        b":2\r\n".to_vec()
    );
    assert_eq!(
        execute_command_line_in_db(&processor, "EXISTS out", db0),
        b":0\r\n".to_vec()
    );
    assert_eq!(
        execute_command_line_in_db(&processor, "EXISTS out", db9),
        b":1\r\n".to_vec()
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap_err();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR MIGRATE is disabled in this server\r\n");

    response.clear();
    let empty_key_without_keys =
        encode_resp(&[b"MIGRATE", b"127.0.0.1", b"6379", b"", b"0", b"1000"]);
    let meta = parse_resp_command_arg_slices(&empty_key_without_keys, &mut args).unwrap();
    let err = processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");
    assert!(!processor.active_expire_enabled());

    response.clear();
    let debug_enable = b"*3\r\n$5\r\nDEBUG\r\n$17\r\nSET-ACTIVE-EXPIRE\r\n$1\r\n1\r\n";
    let meta = parse_resp_command_arg_slices(debug_enable, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");
    assert!(processor.active_expire_enabled());

    assert_command_error(
        &processor,
        "DEBUG HELP extra",
        b"-ERR wrong number of arguments for 'debug' command\r\n",
    );
}

#[test]
fn debug_set_disable_deny_scripts_toggles_script_command_gate() {
    let processor = RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap();

    let denied_before = execute_command_line(
        &processor,
        "EVAL \"redis.call('client','id'); return 'ok'\" 0",
    )
    .unwrap();
    assert!(
        denied_before.starts_with(b"-ERR This Redis command is not allowed from script"),
        "unexpected initial script deny error: {:?}",
        String::from_utf8_lossy(&denied_before)
    );

    assert_command_response(&processor, "DEBUG SET-DISABLE-DENY-SCRIPTS 1", b"+OK\r\n");

    assert_command_response(
        &processor,
        "EVAL \"redis.call('client','id'); return 'ok'\" 0",
        b"$2\r\nok\r\n",
    );

    assert_command_response(&processor, "DEBUG SET-DISABLE-DENY-SCRIPTS 0", b"+OK\r\n");

    let denied_after = execute_command_line(
        &processor,
        "EVAL \"redis.call('client','id'); return 'ok'\" 0",
    )
    .unwrap();
    assert!(
        denied_after.starts_with(b"-ERR This Redis command is not allowed from script"),
        "unexpected script deny error after reset: {:?}",
        String::from_utf8_lossy(&denied_after)
    );
}

#[test]
fn debug_loadaof_returns_ok() {
    let processor = RequestProcessor::new().unwrap();
    assert_command_response(&processor, "DEBUG LOADAOF", b"+OK\r\n");
}

#[test]
fn debug_populate_creates_requested_string_keys_without_overwriting_existing_values() {
    let processor = RequestProcessor::new().unwrap();

    assert_command_response(&processor, "SET seed:1 keep", b"+OK\r\n");
    assert_command_response(&processor, "DEBUG POPULATE 3 seed 4", b"+OK\r\n");
    assert_command_integer(&processor, "DBSIZE", 3);
    assert_command_response(&processor, "GET seed:0", b"$4\r\nvalu\r\n");
    assert_command_response(&processor, "GET seed:1", b"$4\r\nkeep\r\n");
    assert_command_integer(&processor, "STRLEN seed:2", 4);
    assert_command_response(&processor, "GET seed:3", b"$-1\r\n");

    assert_command_response(&processor, "DEBUG POPULATE 1", b"+OK\r\n");
    assert_command_integer(&processor, "DBSIZE", 4);
    assert_command_response(&processor, "GET key:0", b"$7\r\nvalue:0\r\n");

    assert_command_error(
        &processor,
        "DEBUG POPULATE -1",
        b"-ERR value is out of range, must be positive\r\n",
    );
    assert_command_error(
        &processor,
        "DEBUG POPULATE 1 seed -1",
        b"-ERR value is out of range, must be positive\r\n",
    );
}

#[test]
fn debug_protocol_subcommands_cover_resp2_and_resp3_shapes() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 8];
    let mut response = Vec::new();

    let debug_attrib = b"*3\r\n$5\r\nDEBUG\r\n$8\r\nPROTOCOL\r\n$6\r\nATTRIB\r\n";
    let debug_bignum = b"*3\r\n$5\r\nDEBUG\r\n$8\r\nPROTOCOL\r\n$6\r\nBIGNUM\r\n";
    let debug_double = b"*3\r\n$5\r\nDEBUG\r\n$8\r\nPROTOCOL\r\n$6\r\nDOUBLE\r\n";
    let debug_null = b"*3\r\n$5\r\nDEBUG\r\n$8\r\nPROTOCOL\r\n$4\r\nNULL\r\n";
    let debug_map = b"*3\r\n$5\r\nDEBUG\r\n$8\r\nPROTOCOL\r\n$3\r\nMAP\r\n";
    let debug_set = b"*3\r\n$5\r\nDEBUG\r\n$8\r\nPROTOCOL\r\n$3\r\nSET\r\n";
    let debug_true = b"*3\r\n$5\r\nDEBUG\r\n$8\r\nPROTOCOL\r\n$4\r\nTRUE\r\n";
    let debug_false = b"*3\r\n$5\r\nDEBUG\r\n$8\r\nPROTOCOL\r\n$5\r\nFALSE\r\n";
    let debug_verbatim = b"*3\r\n$5\r\nDEBUG\r\n$8\r\nPROTOCOL\r\n$8\r\nVERBATIM\r\n";

    processor.set_resp_protocol_version(RespProtocolVersion::Resp2);

    let meta = parse_resp_command_arg_slices(debug_attrib, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(
        response,
        b"$39\r\nSome real reply following the attribute\r\n"
    );

    response.clear();
    let meta = parse_resp_command_arg_slices(debug_bignum, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(
        response,
        b"$37\r\n1234567999999999999999999999999999999\r\n"
    );

    response.clear();
    let meta = parse_resp_command_arg_slices(debug_double, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$5\r\n3.141\r\n");

    response.clear();
    let meta = parse_resp_command_arg_slices(debug_null, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$-1\r\n");

    response.clear();
    let meta = parse_resp_command_arg_slices(debug_map, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"*6\r\n:0\r\n:0\r\n:1\r\n:1\r\n:2\r\n:0\r\n");

    response.clear();
    let meta = parse_resp_command_arg_slices(debug_set, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"*3\r\n:0\r\n:1\r\n:2\r\n");

    response.clear();
    let meta = parse_resp_command_arg_slices(debug_true, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let meta = parse_resp_command_arg_slices(debug_false, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let meta = parse_resp_command_arg_slices(debug_verbatim, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$25\r\nThis is a verbatim\nstring\r\n");

    processor.set_resp_protocol_version(RespProtocolVersion::Resp3);

    response.clear();
    let meta = parse_resp_command_arg_slices(debug_attrib, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(
        response,
        b"|1\r\n$14\r\nkey-popularity\r\n*2\r\n$7\r\nkey:123\r\n:90\r\n$39\r\nSome real reply following the attribute\r\n"
    );

    response.clear();
    let meta = parse_resp_command_arg_slices(debug_bignum, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"(1234567999999999999999999999999999999\r\n");

    response.clear();
    let meta = parse_resp_command_arg_slices(debug_double, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b",3.141\r\n");

    response.clear();
    let meta = parse_resp_command_arg_slices(debug_null, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"_\r\n");

    response.clear();
    let meta = parse_resp_command_arg_slices(debug_map, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"%3\r\n:0\r\n#f\r\n:1\r\n#t\r\n:2\r\n#f\r\n");

    response.clear();
    let meta = parse_resp_command_arg_slices(debug_set, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"~3\r\n:0\r\n:1\r\n:2\r\n");

    response.clear();
    let meta = parse_resp_command_arg_slices(debug_true, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"#t\r\n");

    response.clear();
    let meta = parse_resp_command_arg_slices(debug_false, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"#f\r\n");

    response.clear();
    let meta = parse_resp_command_arg_slices(debug_verbatim, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"=29\r\ntxt:This is a verbatim\nstring\r\n");
}

#[test]
fn object_encoding_and_refcount_report_basic_metadata() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 8];
    let mut response = Vec::new();

    let lpush = b"*5\r\n$5\r\nLPUSH\r\n$4\r\nlist\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n";
    let meta = parse_resp_command_arg_slices(lpush, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":3\r\n");

    response.clear();
    let object_encoding = b"*3\r\n$6\r\nOBJECT\r\n$8\r\nENCODING\r\n$4\r\nlist\r\n";
    let meta = parse_resp_command_arg_slices(object_encoding, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$8\r\nlistpack\r\n");

    response.clear();
    let object_refcount = b"*3\r\n$6\r\nOBJECT\r\n$8\r\nREFCOUNT\r\n$4\r\nlist\r\n";
    let meta = parse_resp_command_arg_slices(object_refcount, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let object_help = b"*2\r\n$6\r\nOBJECT\r\n$4\r\nHELP\r\n";
    let meta = parse_resp_command_arg_slices(object_help, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert!(response.starts_with(b"*11\r\n"));
    assert!(
        String::from_utf8_lossy(&response).contains("OBJECT <subcommand>"),
        "unexpected OBJECT HELP payload: {}",
        String::from_utf8_lossy(&response)
    );
}

#[test]
fn object_freq_returns_zero_for_existing_key() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 8];
    let mut response = Vec::new();

    let set = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
    let meta = parse_resp_command_arg_slices(set, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let freq = b"*3\r\n$6\r\nOBJECT\r\n$4\r\nFREQ\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(freq, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":0\r\n");
}

#[test]
fn object_freq_returns_null_for_missing_key() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 8];
    let mut response = Vec::new();

    let freq = b"*3\r\n$6\r\nOBJECT\r\n$4\r\nFREQ\r\n$11\r\nnonexistent\r\n";
    let meta = parse_resp_command_arg_slices(freq, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$-1\r\n");
}

#[test]
fn object_idletime_returns_integer_for_existing_key() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 8];
    let mut response = Vec::new();

    let set = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
    let meta = parse_resp_command_arg_slices(set, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let idletime = b"*3\r\n$6\r\nOBJECT\r\n$8\r\nIDLETIME\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(idletime, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    // Should return a non-negative integer in `:N\r\n` format
    let resp_str = String::from_utf8_lossy(&response);
    assert!(
        resp_str.starts_with(':') && resp_str.ends_with("\r\n"),
        "expected integer response, got: {}",
        resp_str
    );
    let num_str = &resp_str[1..resp_str.len() - 2];
    let idle: i64 = num_str.parse().unwrap_or_else(|_| {
        panic!(
            "expected integer in OBJECT IDLETIME response, got: {}",
            resp_str
        )
    });
    assert!(idle >= 0, "expected non-negative idle time, got: {}", idle);
}

#[test]
fn object_idletime_returns_null_for_missing_key() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 8];
    let mut response = Vec::new();

    let idletime = b"*3\r\n$6\r\nOBJECT\r\n$8\r\nIDLETIME\r\n$11\r\nnonexistent\r\n";
    let meta = parse_resp_command_arg_slices(idletime, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$-1\r\n");
}

#[test]
fn object_encoding_distinguishes_string_types() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 8];
    let mut response = Vec::new();

    // Integer-representable string should report "int" encoding
    let set_int = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\n12345\r\n";
    let meta = parse_resp_command_arg_slices(set_int, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let enc = b"*3\r\n$6\r\nOBJECT\r\n$8\r\nENCODING\r\n$3\r\nkey\r\n";
    let meta = parse_resp_command_arg_slices(enc, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$3\r\nint\r\n");

    // Short non-numeric string should report "embstr" encoding
    response.clear();
    let set_short = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nshort\r\n";
    let meta = parse_resp_command_arg_slices(set_short, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let meta = parse_resp_command_arg_slices(enc, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$6\r\nembstr\r\n");

    // Long string (45+ bytes) should report "raw" encoding
    response.clear();
    let set_long = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$50\r\naaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeee\r\n";
    let meta = parse_resp_command_arg_slices(set_long, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let meta = parse_resp_command_arg_slices(enc, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$3\r\nraw\r\n");
}

#[test]
fn debug_digest_value_matches_for_equal_payloads() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 8];
    let mut response = Vec::new();

    let set_a = b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$5\r\nvalue\r\n";
    let meta = parse_resp_command_arg_slices(set_a, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let copy = b"*3\r\n$4\r\nCOPY\r\n$1\r\na\r\n$1\r\nb\r\n";
    let meta = parse_resp_command_arg_slices(copy, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let digest_a = b"*3\r\n$5\r\nDEBUG\r\n$12\r\nDIGEST-VALUE\r\n$1\r\na\r\n";
    let meta = parse_resp_command_arg_slices(digest_a, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    let digest_a_response = response.clone();
    assert!(digest_a_response.starts_with(b"$16\r\n"));

    response.clear();
    let digest_b = b"*3\r\n$5\r\nDEBUG\r\n$12\r\nDIGEST-VALUE\r\n$1\r\nb\r\n";
    let meta = parse_resp_command_arg_slices(digest_b, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, digest_a_response);

    response.clear();
    let digest_all = b"*2\r\n$5\r\nDEBUG\r\n$6\r\nDIGEST\r\n";
    let meta = parse_resp_command_arg_slices(digest_all, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert!(response.starts_with(b"$16\r\n"));
}

#[test]
fn stream_commands_support_copy_and_xinfo_full_digest() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 24];
    let mut response = Vec::new();

    let xadd = b"*5\r\n$4\r\nXADD\r\n$7\r\nstream1\r\n$1\r\n*\r\n$5\r\nfield\r\n$5\r\nvalue\r\n";
    let meta = parse_resp_command_arg_slices(xadd, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert!(response.starts_with(b"$"));

    response.clear();
    let xlen = b"*2\r\n$4\r\nXLEN\r\n$7\r\nstream1\r\n";
    let meta = parse_resp_command_arg_slices(xlen, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let xadd_second =
        b"*5\r\n$4\r\nXADD\r\n$7\r\nstream1\r\n$1\r\n*\r\n$5\r\nfield\r\n$6\r\nvalue2\r\n";
    let meta = parse_resp_command_arg_slices(xadd_second, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert!(response.starts_with(b"$"));

    response.clear();
    let meta = parse_resp_command_arg_slices(xlen, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":2\r\n");

    response.clear();
    let xrange_all = b"*4\r\n$6\r\nXRANGE\r\n$7\r\nstream1\r\n$1\r\n-\r\n$1\r\n+\r\n";
    let meta = parse_resp_command_arg_slices(xrange_all, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert!(response.starts_with(b"*2\r\n"));

    response.clear();
    let xrange_count_1 =
        b"*6\r\n$6\r\nXRANGE\r\n$7\r\nstream1\r\n$1\r\n-\r\n$1\r\n+\r\n$5\r\nCOUNT\r\n$1\r\n1\r\n";
    let meta = parse_resp_command_arg_slices(xrange_count_1, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    let xrange_head = response.clone();
    assert!(xrange_head.starts_with(b"*1\r\n"));

    response.clear();
    let xrevrange_count_1 = b"*6\r\n$9\r\nXREVRANGE\r\n$7\r\nstream1\r\n$1\r\n+\r\n$1\r\n-\r\n$5\r\nCOUNT\r\n$1\r\n1\r\n";
    let meta = parse_resp_command_arg_slices(xrevrange_count_1, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert!(response.starts_with(b"*1\r\n"));
    assert_ne!(response, xrange_head);

    response.clear();
    let xgroup_create =
        b"*5\r\n$6\r\nXGROUP\r\n$6\r\nCREATE\r\n$7\r\nstream1\r\n$2\r\ng1\r\n$1\r\n0\r\n";
    let meta = parse_resp_command_arg_slices(xgroup_create, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let xgroup_create_mkstream = b"*6\r\n$6\r\nXGROUP\r\n$6\r\nCREATE\r\n$7\r\nstream3\r\n$3\r\ngmk\r\n$1\r\n$\r\n$8\r\nMKSTREAM\r\n";
    let meta = parse_resp_command_arg_slices(xgroup_create_mkstream, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let xgroup_destroy = b"*4\r\n$6\r\nXGROUP\r\n$7\r\nDESTROY\r\n$7\r\nstream3\r\n$3\r\ngmk\r\n";
    let meta = parse_resp_command_arg_slices(xgroup_destroy, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let meta = parse_resp_command_arg_slices(xgroup_destroy, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let xreadgroup = b"*9\r\n$10\r\nXREADGROUP\r\n$5\r\nGROUP\r\n$2\r\ng1\r\n$8\r\nconsumer\r\n$5\r\nCOUNT\r\n$1\r\n1\r\n$7\r\nSTREAMS\r\n$7\r\nstream1\r\n$1\r\n>\r\n";
    let meta = parse_resp_command_arg_slices(xreadgroup, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert!(response.starts_with(b"*"));

    response.clear();
    let xinfo_stream = b"*4\r\n$5\r\nXINFO\r\n$6\r\nSTREAM\r\n$7\r\nstream1\r\n$4\r\nFULL\r\n";
    let meta = parse_resp_command_arg_slices(xinfo_stream, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    let source_info = response.clone();
    // Redis 8+ returns a 15-field map = 30-element flat array in RESP2.
    assert!(source_info.starts_with(b"*30\r\n"));

    response.clear();
    let copy = b"*3\r\n$4\r\nCOPY\r\n$7\r\nstream1\r\n$7\r\nstream2\r\n";
    let meta = parse_resp_command_arg_slices(copy, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":1\r\n");

    response.clear();
    let xinfo_stream_copy = b"*4\r\n$5\r\nXINFO\r\n$6\r\nSTREAM\r\n$7\r\nstream2\r\n$4\r\nFULL\r\n";
    let meta = parse_resp_command_arg_slices(xinfo_stream_copy, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, source_info);
}

#[test]
fn xinfo_stream_returns_structured_summary_with_entries_and_groups() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 24];
    let mut response = Vec::new();

    assert_command_error(
        &processor,
        "XINFO HELP extra",
        b"-ERR wrong number of arguments for 'xinfo|help' command\r\n",
    );

    // Add two entries.
    let xadd1 = b"*5\r\n$4\r\nXADD\r\n$4\r\nxkey\r\n$3\r\n1-0\r\n$1\r\nf\r\n$1\r\nv\r\n";
    let meta = parse_resp_command_arg_slices(xadd1, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();

    response.clear();
    let xadd2 = b"*5\r\n$4\r\nXADD\r\n$4\r\nxkey\r\n$3\r\n2-0\r\n$1\r\nf\r\n$2\r\nv2\r\n";
    let meta = parse_resp_command_arg_slices(xadd2, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();

    // Create a group.
    response.clear();
    let xgroup = b"*5\r\n$6\r\nXGROUP\r\n$6\r\nCREATE\r\n$4\r\nxkey\r\n$2\r\nmg\r\n$1\r\n0\r\n";
    let meta = parse_resp_command_arg_slices(xgroup, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    // Redis 8+ returns a 16-field map = 32-element flat array in RESP2.
    response.clear();
    let xinfo = b"*3\r\n$5\r\nXINFO\r\n$6\r\nSTREAM\r\n$4\r\nxkey\r\n";
    let meta = parse_resp_command_arg_slices(xinfo, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert!(response.starts_with(b"*32\r\n"));
    // Verify length=2 is present after "length" label.
    assert!(
        response
            .windows(b"$6\r\nlength\r\n:2\r\n".len())
            .any(|w| w == b"$6\r\nlength\r\n:2\r\n")
    );
    // Verify groups=1 is present after "groups" label.
    assert!(
        response
            .windows(b"$6\r\ngroups\r\n:1\r\n".len())
            .any(|w| w == b"$6\r\ngroups\r\n:1\r\n")
    );

    // XINFO STREAM on non-existent key: error.
    response.clear();
    let xinfo_missing = b"*3\r\n$5\r\nXINFO\r\n$6\r\nSTREAM\r\n$7\r\nno_such\r\n";
    let meta = parse_resp_command_arg_slices(xinfo_missing, &mut args).unwrap();
    let err = processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap_err();
    err.append_resp_error(&mut response);
    assert!(response.starts_with(b"-ERR no such key"));

    // XINFO STREAM key FULL COUNT 1: entries limited to 1.
    response.clear();
    let xinfo_full_count =
        b"*6\r\n$5\r\nXINFO\r\n$6\r\nSTREAM\r\n$4\r\nxkey\r\n$4\r\nFULL\r\n$5\r\nCOUNT\r\n$1\r\n1\r\n";
    let meta = parse_resp_command_arg_slices(xinfo_full_count, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    // Redis 8+ returns a 15-field FULL map = 30-element array.
    assert!(response.starts_with(b"*30\r\n"));
    // Entries limited to 1: after "entries" label, array should be *1.
    let entries_idx = response
        .windows(b"$7\r\nentries\r\n".len())
        .position(|w| w == b"$7\r\nentries\r\n")
        .unwrap();
    let after_entries = &response[entries_idx + b"$7\r\nentries\r\n".len()..];
    assert!(after_entries.starts_with(b"*1\r\n"));

    // RESP3: XINFO STREAM returns map prefix.
    processor.set_resp_protocol_version(RespProtocolVersion::Resp3);
    response.clear();
    let meta = parse_resp_command_arg_slices(xinfo, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert!(response.starts_with(b"%16\r\n"));
    processor.set_resp_protocol_version(RespProtocolVersion::Resp2);

    // XINFO GROUPS: returns array with 1 group, each group is 12-element flat array (6 fields).
    response.clear();
    let xinfo_groups = b"*3\r\n$5\r\nXINFO\r\n$6\r\nGROUPS\r\n$4\r\nxkey\r\n";
    let meta = parse_resp_command_arg_slices(xinfo_groups, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert!(response.starts_with(b"*1\r\n*12\r\n"));
    // Verify group name "mg" is present.
    assert!(
        response
            .windows(b"$2\r\nmg\r\n".len())
            .any(|w| w == b"$2\r\nmg\r\n")
    );

    // XINFO CONSUMERS: returns empty array (no consumer tracking).
    response.clear();
    let xinfo_consumers = b"*4\r\n$5\r\nXINFO\r\n$9\r\nCONSUMERS\r\n$4\r\nxkey\r\n$2\r\nmg\r\n";
    let meta = parse_resp_command_arg_slices(xinfo_consumers, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"*0\r\n");

    // XINFO CONSUMERS on non-existent group: error.
    response.clear();
    let xinfo_consumers_bad =
        b"*4\r\n$5\r\nXINFO\r\n$9\r\nCONSUMERS\r\n$4\r\nxkey\r\n$6\r\nnosuch\r\n";
    let meta = parse_resp_command_arg_slices(xinfo_consumers_bad, &mut args).unwrap();
    let err = processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap_err();
    err.append_resp_error(&mut response);
    assert!(response.starts_with(b"-NOGROUP"));
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$3\r\n1-0\r\n");

    response.clear();
    let xadd_second =
        b"*5\r\n$4\r\nXADD\r\n$7\r\nstreamx\r\n$3\r\n2-0\r\n$5\r\nfield\r\n$3\r\ntwo\r\n";
    let meta = parse_resp_command_arg_slices(xadd_second, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$3\r\n2-0\r\n");

    response.clear();
    let xgroup_create =
        b"*5\r\n$6\r\nXGROUP\r\n$6\r\nCREATE\r\n$7\r\nstreamx\r\n$2\r\ng1\r\n$1\r\n0\r\n";
    let meta = parse_resp_command_arg_slices(xgroup_create, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let xread = b"*6\r\n$5\r\nXREAD\r\n$5\r\nCOUNT\r\n$1\r\n1\r\n$7\r\nSTREAMS\r\n$7\r\nstreamx\r\n$3\r\n0-0\r\n";
    let meta = parse_resp_command_arg_slices(xread, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"*-1\r\n");

    response.clear();
    let xack = b"*4\r\n$4\r\nXACK\r\n$7\r\nstreamx\r\n$2\r\ng1\r\n$3\r\n1-0\r\n";
    let meta = parse_resp_command_arg_slices(xack, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b":0\r\n");

    response.clear();
    let xpending_summary = b"*3\r\n$8\r\nXPENDING\r\n$7\r\nstreamx\r\n$2\r\ng1\r\n";
    let meta = parse_resp_command_arg_slices(xpending_summary, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"*4\r\n:0\r\n$-1\r\n$-1\r\n*0\r\n");

    response.clear();
    let xpending_detail =
        b"*6\r\n$8\r\nXPENDING\r\n$7\r\nstreamx\r\n$2\r\ng1\r\n$1\r\n-\r\n$1\r\n+\r\n$1\r\n1\r\n";
    let meta = parse_resp_command_arg_slices(xpending_detail, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"*0\r\n");

    response.clear();
    let xclaim =
        b"*7\r\n$6\r\nXCLAIM\r\n$7\r\nstreamx\r\n$2\r\ng1\r\n$2\r\nc1\r\n$1\r\n1\r\n$3\r\n1-0\r\n$6\r\nJUSTID\r\n";
    let meta = parse_resp_command_arg_slices(xclaim, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"*0\r\n");

    response.clear();
    let xautoclaim = b"*9\r\n$10\r\nXAUTOCLAIM\r\n$7\r\nstreamx\r\n$2\r\ng1\r\n$2\r\nc1\r\n$1\r\n1\r\n$3\r\n0-0\r\n$5\r\nCOUNT\r\n$2\r\n10\r\n$6\r\nJUSTID\r\n";
    let meta = parse_resp_command_arg_slices(xautoclaim, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert!(response.starts_with(b"*3\r\n"));
    assert!(response.windows(9).any(|window| window == b"$3\r\n0-0\r\n"));

    response.clear();
    let xsetid = b"*3\r\n$6\r\nXSETID\r\n$7\r\nstreamx\r\n$3\r\n2-0\r\n";
    let meta = parse_resp_command_arg_slices(xsetid, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let xsetid_missing = b"*3\r\n$6\r\nXSETID\r\n$8\r\nmissing1\r\n$3\r\n1-0\r\n";
    let meta = parse_resp_command_arg_slices(xsetid_missing, &mut args).unwrap();
    let err = processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap_err();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR no such key\r\n");

    response.clear();
    let xpending_missing = b"*3\r\n$8\r\nXPENDING\r\n$8\r\nmissing1\r\n$2\r\ng1\r\n";
    let meta = parse_resp_command_arg_slices(xpending_missing, &mut args).unwrap();
    let err = processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap_err();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-NOGROUP No such key or consumer group\r\n");
}

#[test]
fn stream_commands_are_scoped_by_selected_db_without_db0_fallback() {
    let processor = RequestProcessor::new().unwrap();
    let db0 = DbName::default();
    let db9 = DbName::new(9);

    assert_command_response_in_db(
        &processor,
        "XADD shared 1-0 field zero",
        b"$3\r\n1-0\r\n",
        db0,
    );
    assert_command_response_in_db(
        &processor,
        "XADD shared 1-0 field nine-a",
        b"$3\r\n1-0\r\n",
        db9,
    );
    assert_command_response_in_db(
        &processor,
        "XADD shared 2-0 field nine-b",
        b"$3\r\n2-0\r\n",
        db9,
    );

    assert_command_response_in_db(&processor, "XLEN shared", b":1\r\n", db0);
    assert_command_response_in_db(&processor, "XLEN shared", b":2\r\n", db9);

    assert_command_response_in_db(&processor, "XTRIM shared MAXLEN = 1", b":1\r\n", db9);
    assert_command_response_in_db(&processor, "XLEN shared", b":1\r\n", db0);
    assert_command_response_in_db(&processor, "XLEN shared", b":1\r\n", db9);

    assert_command_response_in_db(&processor, "XGROUP CREATE shared g0 0", b"+OK\r\n", db0);
    assert_command_response_in_db(&processor, "XGROUP CREATE shared g9 0", b"+OK\r\n", db9);
    assert_command_response_in_db(
        &processor,
        "XGROUP CREATECONSUMER shared g9 c9",
        b":1\r\n",
        db9,
    );

    let db0_groups = execute_command_line_in_db(&processor, "XINFO GROUPS shared", db0);
    assert!(
        db0_groups
            .windows(b"$2\r\ng0\r\n".len())
            .any(|window| window == b"$2\r\ng0\r\n")
    );
    assert!(
        !db0_groups
            .windows(b"$2\r\ng9\r\n".len())
            .any(|window| window == b"$2\r\ng9\r\n")
    );

    let db9_groups = execute_command_line_in_db(&processor, "XINFO GROUPS shared", db9);
    assert!(
        db9_groups
            .windows(b"$2\r\ng9\r\n".len())
            .any(|window| window == b"$2\r\ng9\r\n")
    );
    assert!(
        !db9_groups
            .windows(b"$2\r\ng0\r\n".len())
            .any(|window| window == b"$2\r\ng0\r\n")
    );

    let db9_consumers = execute_command_line_in_db(&processor, "XINFO CONSUMERS shared g9", db9);
    assert!(
        db9_consumers
            .windows(b"$2\r\nc9\r\n".len())
            .any(|window| window == b"$2\r\nc9\r\n")
    );
    assert_command_response_in_db(
        &processor,
        "XREADGROUP GROUP g9 c9 STREAMS shared >",
        b"*1\r\n*2\r\n$6\r\nshared\r\n*1\r\n*2\r\n$3\r\n2-0\r\n*2\r\n$5\r\nfield\r\n$6\r\nnine-b\r\n",
        db9,
    );
    assert_command_response_in_db(&processor, "XLEN shared", b":1\r\n", db0);
}

#[test]
fn xsetid_help_and_partial_id_match_external_stream_scenarios() {
    let processor = RequestProcessor::new().unwrap();

    // Redis tests/unit/type/stream.tcl:
    // - "XSETID can set a specific ID"
    // - "XSETID cannot SETID with smaller ID"
    // - "XSETID cannot run with an offset but without a maximal tombstone"
    // - "XSETID cannot run with a maximal tombstone but without an offset"
    // - "XSETID errors on negstive offset"
    // - "XSETID cannot set the maximal tombstone with larger ID"
    // - "XSETID cannot set the offset to less than the length"
    // - "XSETID cannot set smaller ID than current MAXDELETEDID"
    // - "XADD with artial ID with maximal seq"
    // - "XGROUP HELP should not have unexpected options"
    // - "XINFO HELP should not have unexpected options"
    let created_empty = execute_command_line(&processor, "XADD mystream MAXLEN 0 * a b").unwrap();
    assert!(created_empty.starts_with(b"$"));

    assert_command_response(&processor, "XSETID mystream 200-0", b"+OK\r\n");
    let xinfo =
        parse_resp_test_value(&execute_command_line(&processor, "XINFO STREAM mystream").unwrap());
    let xinfo_map = resp_test_flat_map(&xinfo);
    assert_eq!(
        resp_test_bulk(xinfo_map[&b"last-generated-id".to_vec()]),
        b"200-0"
    );
    assert_eq!(resp_test_integer(xinfo_map[&b"entries-added".to_vec()]), 1);

    let appended = execute_command_line(&processor, "XADD mystream * a b").unwrap();
    assert!(appended.starts_with(b"$"));
    assert_command_error(
        &processor,
        "XSETID mystream 1-1",
        b"-ERR The ID specified in XSETID is smaller than the target stream top item\r\n",
    );
    assert_command_error(&processor, "XSETID stream 1-1 0", b"-ERR syntax error\r\n");
    assert_command_error(
        &processor,
        "XSETID stream 1-1 0-0",
        b"-ERR syntax error\r\n",
    );
    // ENTRIESADDED without MAXDELETEDID is a syntax error.
    assert_command_error(
        &processor,
        "XSETID mystream 200-0 ENTRIESADDED 5",
        b"-ERR syntax error\r\n",
    );
    // MAXDELETEDID without ENTRIESADDED is a syntax error.
    assert_command_error(
        &processor,
        "XSETID mystream 200-0 MAXDELETEDID 0-0",
        b"-ERR syntax error\r\n",
    );
    assert_command_error(
        &processor,
        "XSETID stream 1-1 ENTRIESADDED -1 MAXDELETEDID 0-0",
        b"-ERR entries_added must be positive\r\n",
    );

    assert_command_integer(&processor, "DEL x", 0);
    assert_command_response(&processor, "XADD x 1-0 a b", b"$3\r\n1-0\r\n");
    assert_command_error(
        &processor,
        "XSETID x 1-0 ENTRIESADDED 1 MAXDELETEDID 2-0",
        b"-ERR The ID specified in XSETID is smaller than the provided max_deleted_entry_id\r\n",
    );
    assert_command_error(
        &processor,
        "XSETID x 1-0 ENTRIESADDED 0 MAXDELETEDID 0-0",
        b"-ERR The entries_added specified in XSETID is smaller than the target stream length\r\n",
    );

    assert_command_integer(&processor, "DEL x", 1);
    assert_command_response(&processor, "XADD x 1-1 a 1", b"$3\r\n1-1\r\n");
    assert_command_response(&processor, "XADD x 1-2 b 2", b"$3\r\n1-2\r\n");
    assert_command_response(&processor, "XADD x 1-3 c 3", b"$3\r\n1-3\r\n");
    assert_command_integer(&processor, "XDEL x 1-2", 1);
    assert_command_integer(&processor, "XDEL x 1-3", 1);
    let deleted_info =
        parse_resp_test_value(&execute_command_line(&processor, "XINFO STREAM x").unwrap());
    let deleted_info_map = resp_test_flat_map(&deleted_info);
    assert_eq!(
        resp_test_bulk(deleted_info_map[&b"max-deleted-entry-id".to_vec()]),
        b"1-3"
    );
    assert_command_error(
        &processor,
        "XSETID x 1-2",
        b"-ERR The ID specified in XSETID is smaller than current max_deleted_entry_id\r\n",
    );

    assert_command_integer(&processor, "DEL x", 1);
    assert_command_response(
        &processor,
        "XADD x 1-18446744073709551615 f1 v1",
        b"$22\r\n1-18446744073709551615\r\n",
    );
    assert_command_error(
        &processor,
        "XADD x 1-* f2 v2",
        b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n",
    );

    assert_command_error(
        &processor,
        "XGROUP HELP extra",
        b"-ERR wrong number of arguments for 'xgroup|help' command\r\n",
    );
    assert_command_error(
        &processor,
        "XINFO HELP extra",
        b"-ERR wrong number of arguments for 'xinfo|help' command\r\n",
    );
}

#[test]
fn xpending_accepts_idle_filter() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 16];
    let mut response = Vec::new();

    // Set up: create stream and consumer group
    let xadd = encode_resp(&[b"XADD", b"xs1", b"*", b"f1", b"v1"]);
    let meta = parse_resp_command_arg_slices(&xadd, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();

    response.clear();
    let xgroup = encode_resp(&[b"XGROUP", b"CREATE", b"xs1", b"g1", b"0"]);
    let meta = parse_resp_command_arg_slices(&xgroup, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    // XPENDING with IDLE filter (no consumer)
    response.clear();
    let xpending_idle = encode_resp(&[
        b"XPENDING",
        b"xs1",
        b"g1",
        b"IDLE",
        b"5000",
        b"-",
        b"+",
        b"10",
    ]);
    let meta = parse_resp_command_arg_slices(&xpending_idle, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"*0\r\n");

    // XPENDING with IDLE filter and consumer
    response.clear();
    let xpending_idle_consumer = encode_resp(&[
        b"XPENDING",
        b"xs1",
        b"g1",
        b"IDLE",
        b"5000",
        b"-",
        b"+",
        b"10",
        b"c1",
    ]);
    let meta = parse_resp_command_arg_slices(&xpending_idle_consumer, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"*0\r\n");

    // XPENDING with IDLE but missing min-idle-time should error
    response.clear();
    let xpending_bad_idle = encode_resp(&[b"XPENDING", b"xs1", b"g1", b"IDLE"]);
    let meta = parse_resp_command_arg_slices(&xpending_bad_idle, &mut args).unwrap();
    let err = processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap_err();
    assert!(matches!(err, RequestExecutionError::SyntaxError));
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
            &encode_resp(&[b"XTRIM", b"streamx", b"MINID", b"4-0"])
        ),
        b":2\r\n"
    );
    assert_eq!(
        execute_frame(&processor, &encode_resp(&[b"XLEN", b"streamx"])),
        b":1\r\n"
    );

    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"XTRIM", b"streamx", b"MAXLEN", b"~", b"1", b"LIMIT", b"1"])
        ),
        b":0\r\n"
    );

    response.clear();
    let xtrim_bad_strategy = b"*4\r\n$5\r\nXTRIM\r\n$7\r\nstreamx\r\n$7\r\nUNKNOWN\r\n$1\r\n1\r\n";
    let meta = parse_resp_command_arg_slices(xtrim_bad_strategy, &mut args).unwrap();
    let err = processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap_err();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR syntax error\r\n");

    response.clear();
    let xtrim_negative_limit =
        b"*6\r\n$5\r\nXTRIM\r\n$7\r\nstreamx\r\n$6\r\nMAXLEN\r\n$1\r\n1\r\n$5\r\nLIMIT\r\n$2\r\n-1\r\n";
    let meta = parse_resp_command_arg_slices(xtrim_negative_limit, &mut args).unwrap();
    let err = processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap_err();
    err.append_resp_error(&mut response);
    assert_eq!(response, b"-ERR value is out of range\r\n");
}

#[test]
fn stream_range_orders_entries_by_numeric_stream_id() {
    let processor = RequestProcessor::new().unwrap();

    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"XADD", b"streamn", b"2-10", b"field", b"early"])
        ),
        b"$4\r\n2-10\r\n"
    );
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"XADD", b"streamn", b"10-1", b"field", b"late"])
        ),
        b"$4\r\n10-1\r\n"
    );

    let range = execute_frame(
        &processor,
        &encode_resp(&[b"XRANGE", b"streamn", b"-", b"+"]),
    );
    let early_id = b"$4\r\n2-10\r\n";
    let late_id = b"$4\r\n10-1\r\n";
    let early_pos = range
        .windows(early_id.len())
        .position(|window| window == early_id)
        .expect("2-10 entry should exist in XRANGE response");
    let late_pos = range
        .windows(late_id.len())
        .position(|window| window == late_id)
        .expect("10-1 entry should exist in XRANGE response");
    assert!(early_pos < late_pos);
}

#[test]
fn xrange_returns_map_entries_in_resp3() {
    let processor = RequestProcessor::new().unwrap();
    // Add a stream entry with a fixed ID and two fields.
    assert_command_response(
        &processor,
        "XADD smap 1-0 name Alice age 30",
        b"$3\r\n1-0\r\n",
    );

    // RESP2: flat array entries → *2\r\n <id> *4\r\n <f> <v> <f> <v>
    processor.set_resp_protocol_version(RespProtocolVersion::Resp2);
    let resp2 = execute_command_line(&processor, "XRANGE smap - +").unwrap();
    let resp2_str = String::from_utf8_lossy(&resp2);
    // Outer array with 1 entry, inner entry has *2 (id + flat field array)
    assert!(
        resp2_str.starts_with("*1\r\n*2\r\n"),
        "RESP2 XRANGE should start with *1 array: {resp2_str}"
    );
    // Field-value pairs as flat array: *4 (2 fields × 2)
    assert!(
        resp2_str.contains("*4\r\n"),
        "RESP2 XRANGE should have *4 flat field array: {resp2_str}"
    );

    // RESP3: map entries → *2\r\n <id> %2\r\n <f> <v> <f> <v>
    processor.set_resp_protocol_version(RespProtocolVersion::Resp3);
    let resp3 = execute_command_line(&processor, "XRANGE smap - +").unwrap();
    let resp3_str = String::from_utf8_lossy(&resp3);
    assert!(
        resp3_str.starts_with("*1\r\n*2\r\n"),
        "RESP3 XRANGE should start with *1 array: {resp3_str}"
    );
    // Field-value pairs as map: %2 (2 entries)
    assert!(
        resp3_str.contains("%2\r\n"),
        "RESP3 XRANGE should have %2 map for fields: {resp3_str}"
    );
    assert!(
        !resp3_str.contains("*4\r\n"),
        "RESP3 XRANGE should NOT have *4 flat array: {resp3_str}"
    );
    processor.set_resp_protocol_version(RespProtocolVersion::Resp2);
}

#[test]
fn script_flush_returns_ok() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 4];
    let mut response = Vec::new();

    let script_flush = b"*2\r\n$6\r\nSCRIPT\r\n$5\r\nFLUSH\r\n";
    let meta = parse_resp_command_arg_slices(script_flush, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        b"-ERR Number of keys can't be negative\r\n",
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
    assert!(
        response_text.contains("Write commands are not allowed from read-only scripts"),
        "expected write-reject error, got: {}",
        response_text
    );

    let publish_response = execute_frame(
        &processor,
        &encode_resp(&[b"EVAL_RO", b"return redis.call('PUBLISH','ch','msg')", b"0"]),
    );
    let publish_response_text = String::from_utf8_lossy(&publish_response);
    assert!(
        publish_response_text.contains("Write commands are not allowed from read-only scripts"),
        "expected publish-reject error from EVAL_RO, got: {}",
        publish_response_text
    );
    assert!(
        publish_response_text.contains("script:"),
        "expected Redis-style script suffix, got: {}",
        publish_response_text
    );
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
fn function_list_and_stats_use_resp3_map_types() {
    let processor = RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap();
    let library_source =
        b"#!lua name=testlib\nredis.register_function('myfn', function(keys, args) return 1 end)";
    execute_frame(
        &processor,
        &encode_resp(&[b"FUNCTION", b"LOAD", library_source]),
    );

    // RESP3: FUNCTION LIST should use map (%N) for library entries
    processor.set_resp_protocol_version(RespProtocolVersion::Resp3);
    let list_resp3 = execute_frame(&processor, &encode_resp(&[b"FUNCTION", b"LIST"]));
    let list_text = String::from_utf8_lossy(&list_resp3);
    assert!(
        list_text.contains("%3\r\n"),
        "RESP3 FUNCTION LIST library entry should use map: {list_text}"
    );

    // RESP3: FUNCTION STATS should use map (%N)
    let stats_resp3 = execute_frame(&processor, &encode_resp(&[b"FUNCTION", b"STATS"]));
    let stats_text = String::from_utf8_lossy(&stats_resp3);
    assert!(
        stats_text.contains("%2\r\n"),
        "RESP3 FUNCTION STATS should use top-level map: {stats_text}"
    );
    assert!(
        stats_text.contains("%1\r\n"),
        "RESP3 FUNCTION STATS engines should use map: {stats_text}"
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
fn script_flush_async_and_info_memory_fields_match_external_scripting_scenarios() {
    let processor = RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap();

    for value in 0..100 {
        let script = format!("return {value}");
        let frame = encode_resp(&[b"SCRIPT", b"LOAD", script.as_bytes()]);
        let sha = parse_bulk_payload(&execute_frame(&processor, &frame))
            .expect("SCRIPT LOAD should return sha1");
        assert_eq!(sha.len(), 40);
    }

    let info_before = parse_bulk_payload(&execute_frame(
        &processor,
        &encode_resp(&[b"INFO", b"MEMORY"]),
    ))
    .expect("INFO MEMORY should return bulk payload");
    let info_before_text = String::from_utf8_lossy(&info_before);
    assert!(info_before_text.contains("number_of_cached_scripts:100"));
    assert!(info_before_text.contains("allocator_allocated:"));
    assert!(info_before_text.contains("allocator_active:"));
    assert!(info_before_text.contains("allocator_resident:"));

    assert_command_response(&processor, "SCRIPT FLUSH ASYNC", b"+OK\r\n");

    let info_after = parse_bulk_payload(&execute_frame(
        &processor,
        &encode_resp(&[b"INFO", b"MEMORY"]),
    ))
    .expect("INFO MEMORY should return bulk payload");
    let info_after_text = String::from_utf8_lossy(&info_after);
    assert!(info_after_text.contains("number_of_cached_scripts:0"));
}

#[test]
fn eval_shebang_validation_and_allow_oom_match_external_scripting_scenarios() {
    let processor = RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap();

    let unexpected_engine = execute_frame(
        &processor,
        &encode_resp(&[b"EVAL", b"#!not-lua\nreturn 1", b"0"]),
    );
    assert!(
        String::from_utf8_lossy(&unexpected_engine).contains("Unexpected engine in script shebang"),
        "unexpected response: {}",
        String::from_utf8_lossy(&unexpected_engine)
    );

    assert_command_response(&processor, "EVAL \"#!lua\nreturn 1\" 0", b":1\r\n");

    let unknown_option = execute_frame(
        &processor,
        &encode_resp(&[b"EVAL", b"#!lua badger=data\nreturn 1", b"0"]),
    );
    assert!(
        String::from_utf8_lossy(&unknown_option).contains("Unknown lua shebang option"),
        "unexpected response: {}",
        String::from_utf8_lossy(&unknown_option)
    );

    let unknown_flag = execute_frame(
        &processor,
        &encode_resp(&[b"EVAL", b"#!lua flags=allow-oom,what?\nreturn 1", b"0"]),
    );
    assert!(
        String::from_utf8_lossy(&unknown_flag).contains("Unexpected flag in script shebang"),
        "unexpected response: {}",
        String::from_utf8_lossy(&unknown_flag)
    );

    assert_command_response(&processor, "SET x 123", b"+OK\r\n");
    assert_command_response(&processor, "CONFIG SET maxmemory 1", b"+OK\r\n");

    let compat_deny_oom = execute_frame(
        &processor,
        &encode_resp(&[b"EVAL", b"redis.call('set','x',1)\nreturn 1", b"1", b"x"]),
    );
    assert!(
        String::from_utf8_lossy(&compat_deny_oom)
            .contains("OOM command not allowed when used memory > 'maxmemory'"),
        "unexpected response: {}",
        String::from_utf8_lossy(&compat_deny_oom)
    );

    assert_command_response(
        &processor,
        "EVAL \"return redis.call('get','x')\" 1 x",
        b"$3\r\n123\r\n",
    );

    assert_command_response(
        &processor,
        "EVAL \"#!lua flags=allow-oom\nredis.call('set','x',1)\nreturn 1\" 1 x",
        b":1\r\n",
    );
    assert_command_response(
        &processor,
        "EVAL \"#!lua flags=no-writes\nredis.call('get','x')\nreturn 1\" 0",
        b":1\r\n",
    );
    assert_command_response(&processor, "GET x", b"$1\r\n1\r\n");
    assert_command_response(&processor, "CONFIG SET maxmemory 0", b"+OK\r\n");
}

#[test]
fn reject_script_does_not_cause_lua_stack_leak_external_scenario() {
    let processor = RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap();

    assert_command_response(&processor, "CONFIG SET maxmemory 1", b"+OK\r\n");
    for _ in 0..50 {
        let response = execute_frame(
            &processor,
            &encode_resp(&[b"EVAL", b"#!lua\nreturn 1", b"0"]),
        );
        assert!(
            String::from_utf8_lossy(&response)
                .contains("OOM command not allowed when used memory > 'maxmemory'"),
            "unexpected response: {}",
            String::from_utf8_lossy(&response)
        );
    }
    assert_command_response(&processor, "CONFIG SET maxmemory 0", b"+OK\r\n");
    assert_command_response(&processor, "EVAL \"#!lua\nreturn 1\" 0", b":1\r\n");
}

#[test]
fn consistent_eval_error_reporting_matches_external_scripting_scenario() {
    let processor = RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap();
    let execute_counted = |command_name: &[u8], frame: Vec<u8>| {
        processor.record_command_call(command_name);
        execute_frame(&processor, &frame)
    };

    assert_command_response(&processor, "CONFIG RESETSTAT", b"+OK\r\n");
    assert_command_response(&processor, "CONFIG SET maxmemory 1", b"+OK\r\n");
    let oom_call = execute_counted(
        b"eval",
        encode_resp(&[b"EVAL", b"return redis.call('set','x','y')", b"1", b"x"]),
    );
    assert!(
        String::from_utf8_lossy(&oom_call)
            .contains("OOM command not allowed when used memory > 'maxmemory'"),
        "unexpected response: {}",
        String::from_utf8_lossy(&oom_call)
    );

    let errorstats = parse_bulk_payload(&execute_frame(
        &processor,
        &encode_resp(&[b"INFO", b"ERRORSTATS"]),
    ))
    .expect("INFO ERRORSTATS should return bulk payload");
    let errorstats_text = String::from_utf8_lossy(&errorstats);
    assert!(errorstats_text.contains("errorstat_OOM:count=1"));

    let stats = parse_bulk_payload(&execute_frame(
        &processor,
        &encode_resp(&[b"INFO", b"STATS"]),
    ))
    .expect("INFO STATS should return bulk payload");
    assert!(String::from_utf8_lossy(&stats).contains("total_error_replies:1"));

    let commandstats = parse_bulk_payload(&execute_frame(
        &processor,
        &encode_resp(&[b"INFO", b"COMMANDSTATS"]),
    ))
    .expect("INFO COMMANDSTATS should return bulk payload");
    let commandstats_text = String::from_utf8_lossy(&commandstats);
    assert!(
        commandstats_text.contains(
            "cmdstat_set:calls=0,usec=0,usec_per_call=0.00,rejected_calls=1,failed_calls=0"
        ),
        "unexpected commandstats payload: {commandstats_text}"
    );
    assert!(
        commandstats_text.contains(
            "cmdstat_eval:calls=1,usec=0,usec_per_call=0.00,rejected_calls=0,failed_calls=1"
        ),
        "unexpected commandstats payload: {commandstats_text}"
    );

    assert_command_response(&processor, "CONFIG RESETSTAT", b"+OK\r\n");
    let pcall_oom_eval = execute_counted(
        b"eval",
        encode_resp(&[
            b"EVAL",
            b"local t = redis.pcall('set','x','y') if t['err'] == \"OOM command not allowed when used memory > 'maxmemory'.\" then return 1 else return 0 end",
            b"1",
            b"x",
        ]),
    );
    assert_eq!(pcall_oom_eval, b":1\r\n");
    let errorstats_after_pcall = parse_bulk_payload(&execute_frame(
        &processor,
        &encode_resp(&[b"INFO", b"ERRORSTATS"]),
    ))
    .expect("INFO ERRORSTATS should return bulk payload");
    let errorstats_after_pcall_text = String::from_utf8_lossy(&errorstats_after_pcall);
    assert!(!errorstats_after_pcall_text.contains("errorstat_ERR:"));
    assert!(errorstats_after_pcall_text.contains("errorstat_OOM:count=1"));
    let stats_after_pcall = parse_bulk_payload(&execute_frame(
        &processor,
        &encode_resp(&[b"INFO", b"STATS"]),
    ))
    .expect("INFO STATS should return bulk payload");
    assert!(String::from_utf8_lossy(&stats_after_pcall).contains("total_error_replies:1"));
    let commandstats_after_pcall = parse_bulk_payload(&execute_frame(
        &processor,
        &encode_resp(&[b"INFO", b"COMMANDSTATS"]),
    ))
    .expect("INFO COMMANDSTATS should return bulk payload");
    let commandstats_after_pcall_text = String::from_utf8_lossy(&commandstats_after_pcall);
    assert!(
        commandstats_after_pcall_text.contains(
            "cmdstat_set:calls=0,usec=0,usec_per_call=0.00,rejected_calls=1,failed_calls=0"
        ),
        "unexpected commandstats payload: {commandstats_after_pcall_text}"
    );
    assert!(
        commandstats_after_pcall_text.contains(
            "cmdstat_eval:calls=1,usec=0,usec_per_call=0.00,rejected_calls=0,failed_calls=0"
        ),
        "unexpected commandstats payload: {commandstats_after_pcall_text}"
    );

    assert_command_response(&processor, "CONFIG RESETSTAT", b"+OK\r\n");
    let returned_pcall_error = execute_counted(
        b"eval",
        encode_resp(&[b"EVAL", b"return redis.pcall('set','x','y')", b"1", b"x"]),
    );
    assert_eq!(
        returned_pcall_error,
        b"-OOM command not allowed when used memory > 'maxmemory'.\r\n"
    );
    let errorstats_after_returned_error = parse_bulk_payload(&execute_frame(
        &processor,
        &encode_resp(&[b"INFO", b"ERRORSTATS"]),
    ))
    .expect("INFO ERRORSTATS should return bulk payload");
    let errorstats_after_returned_error_text =
        String::from_utf8_lossy(&errorstats_after_returned_error);
    assert!(!errorstats_after_returned_error_text.contains("errorstat_ERR:"));
    assert!(errorstats_after_returned_error_text.contains("errorstat_OOM:count=1"));
    let commandstats_after_returned_error = parse_bulk_payload(&execute_frame(
        &processor,
        &encode_resp(&[b"INFO", b"COMMANDSTATS"]),
    ))
    .expect("INFO COMMANDSTATS should return bulk payload");
    let commandstats_after_returned_error_text =
        String::from_utf8_lossy(&commandstats_after_returned_error);
    assert!(
        commandstats_after_returned_error_text.contains(
            "cmdstat_set:calls=0,usec=0,usec_per_call=0.00,rejected_calls=1,failed_calls=0"
        ),
        "unexpected commandstats payload: {commandstats_after_returned_error_text}"
    );
    assert!(
        commandstats_after_returned_error_text.contains(
            "cmdstat_eval:calls=1,usec=0,usec_per_call=0.00,rejected_calls=0,failed_calls=1"
        ),
        "unexpected commandstats payload: {commandstats_after_returned_error_text}"
    );

    assert_command_response(&processor, "CONFIG SET maxmemory 0", b"+OK\r\n");
    assert_command_response(&processor, "CONFIG RESETSTAT", b"+OK\r\n");
    let select_error = execute_counted(
        b"eval",
        encode_resp(&[b"EVAL", b"return redis.call('select',99)", b"0"]),
    );
    assert!(
        String::from_utf8_lossy(&select_error).contains("ERR DB index is out of range"),
        "unexpected response: {}",
        String::from_utf8_lossy(&select_error)
    );
    let errorstats_after_select = parse_bulk_payload(&execute_frame(
        &processor,
        &encode_resp(&[b"INFO", b"ERRORSTATS"]),
    ))
    .expect("INFO ERRORSTATS should return bulk payload");
    assert!(String::from_utf8_lossy(&errorstats_after_select).contains("errorstat_ERR:count=1"));
    let commandstats_after_select = parse_bulk_payload(&execute_frame(
        &processor,
        &encode_resp(&[b"INFO", b"COMMANDSTATS"]),
    ))
    .expect("INFO COMMANDSTATS should return bulk payload");
    let commandstats_after_select_text = String::from_utf8_lossy(&commandstats_after_select);
    assert!(
        commandstats_after_select_text.contains(
            "cmdstat_select:calls=1,usec=0,usec_per_call=0.00,rejected_calls=0,failed_calls=1"
        ),
        "unexpected commandstats payload: {commandstats_after_select_text}"
    );
    assert!(
        commandstats_after_select_text.contains(
            "cmdstat_eval:calls=1,usec=0,usec_per_call=0.00,rejected_calls=0,failed_calls=1"
        ),
        "unexpected commandstats payload: {commandstats_after_select_text}"
    );

    assert_command_response(&processor, "CONFIG RESETSTAT", b"+OK\r\n");
    let pcall_select_eval = execute_counted(
        b"eval",
        encode_resp(&[
            b"EVAL",
            b"local t = redis.pcall('select',99) if t['err'] == \"ERR DB index is out of range\" then return 1 else return 0 end",
            b"0",
        ]),
    );
    assert_eq!(pcall_select_eval, b":1\r\n");
    let commandstats_after_select_pcall = parse_bulk_payload(&execute_frame(
        &processor,
        &encode_resp(&[b"INFO", b"COMMANDSTATS"]),
    ))
    .expect("INFO COMMANDSTATS should return bulk payload");
    let commandstats_after_select_pcall_text =
        String::from_utf8_lossy(&commandstats_after_select_pcall);
    assert!(
        commandstats_after_select_pcall_text.contains(
            "cmdstat_select:calls=1,usec=0,usec_per_call=0.00,rejected_calls=0,failed_calls=1"
        ),
        "unexpected commandstats payload: {commandstats_after_select_pcall_text}"
    );
    assert!(
        commandstats_after_select_pcall_text.contains(
            "cmdstat_eval:calls=1,usec=0,usec_per_call=0.00,rejected_calls=0,failed_calls=0"
        ),
        "unexpected commandstats payload: {commandstats_after_select_pcall_text}"
    );

    assert_command_response(&processor, "CONFIG RESETSTAT", b"+OK\r\n");
    let eval_ro_error = execute_counted(
        b"eval_ro",
        encode_resp(&[b"EVAL_RO", b"return redis.call('set','x','y')", b"1", b"x"]),
    );
    assert!(
        String::from_utf8_lossy(&eval_ro_error)
            .contains("ERR Write commands are not allowed from read-only scripts."),
        "unexpected response: {}",
        String::from_utf8_lossy(&eval_ro_error)
    );
    let commandstats_after_eval_ro = parse_bulk_payload(&execute_frame(
        &processor,
        &encode_resp(&[b"INFO", b"COMMANDSTATS"]),
    ))
    .expect("INFO COMMANDSTATS should return bulk payload");
    let commandstats_after_eval_ro_text = String::from_utf8_lossy(&commandstats_after_eval_ro);
    assert!(
        commandstats_after_eval_ro_text.contains(
            "cmdstat_set:calls=0,usec=0,usec_per_call=0.00,rejected_calls=1,failed_calls=0"
        ),
        "unexpected commandstats payload: {commandstats_after_eval_ro_text}"
    );
    assert!(
        commandstats_after_eval_ro_text.contains(
            "cmdstat_eval_ro:calls=1,usec=0,usec_per_call=0.00,rejected_calls=0,failed_calls=1"
        ),
        "unexpected commandstats payload: {commandstats_after_eval_ro_text}"
    );

    assert_command_response(&processor, "CONFIG RESETSTAT", b"+OK\r\n");
    let eval_ro_pcall = execute_counted(
        b"eval_ro",
        encode_resp(&[
            b"EVAL_RO",
            b"local t = redis.pcall('set','x','y') if t['err'] == \"ERR Write commands are not allowed from read-only scripts.\" then return 1 else return 0 end",
            b"1",
            b"x",
        ]),
    );
    assert_eq!(eval_ro_pcall, b":1\r\n");
    let commandstats_after_eval_ro_pcall = parse_bulk_payload(&execute_frame(
        &processor,
        &encode_resp(&[b"INFO", b"COMMANDSTATS"]),
    ))
    .expect("INFO COMMANDSTATS should return bulk payload");
    let commandstats_after_eval_ro_pcall_text =
        String::from_utf8_lossy(&commandstats_after_eval_ro_pcall);
    assert!(
        commandstats_after_eval_ro_pcall_text.contains(
            "cmdstat_set:calls=0,usec=0,usec_per_call=0.00,rejected_calls=1,failed_calls=0"
        ),
        "unexpected commandstats payload: {commandstats_after_eval_ro_pcall_text}"
    );
    assert!(
        commandstats_after_eval_ro_pcall_text.contains(
            "cmdstat_eval_ro:calls=1,usec=0,usec_per_call=0.00,rejected_calls=0,failed_calls=0"
        ),
        "unexpected commandstats payload: {commandstats_after_eval_ro_pcall_text}"
    );
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"*2\r\n$10\r\nappendonly\r\n$2\r\nno\r\n");

    response.clear();
    let config_get_unknown = b"*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$7\r\nunknown\r\n";
    let meta = parse_resp_command_arg_slices(config_get_unknown, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"*0\r\n");

    let config_help = execute_command_line(&processor, "CONFIG HELP").unwrap();
    assert!(
        String::from_utf8_lossy(&config_help).contains("CONFIG <subcommand>"),
        "unexpected CONFIG HELP payload: {}",
        String::from_utf8_lossy(&config_help)
    );

    assert_command_error(&processor, "CONFIG GET_XX", b"-ERR unknown subcommand\r\n");
}

#[test]
fn lazyexpire_nested_arbitrary_keys_config_defaults_yes_and_honors_no() {
    let processor = RequestProcessor::new().unwrap();
    assert!(processor.lazyexpire_nested_arbitrary_keys_enabled());

    assert_command_response(
        &processor,
        "CONFIG SET lazyexpire-nested-arbitrary-keys no",
        b"+OK\r\n",
    );
    assert!(!processor.lazyexpire_nested_arbitrary_keys_enabled());

    assert_command_response(
        &processor,
        "CONFIG SET lazyexpire-nested-arbitrary-keys yes",
        b"+OK\r\n",
    );
    assert!(processor.lazyexpire_nested_arbitrary_keys_enabled());
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
fn config_set_notify_keyspace_events_accepts_valid_flags() {
    let processor = RequestProcessor::new().unwrap();
    assert_command_response(
        &processor,
        "CONFIG SET notify-keyspace-events KEA",
        b"+OK\r\n",
    );
    // Verify flags are stored and can be retrieved
    assert_command_response(
        &processor,
        "CONFIG GET notify-keyspace-events",
        b"*2\r\n$22\r\nnotify-keyspace-events\r\n$3\r\nAKE\r\n",
    );
}

#[test]
fn config_set_notify_keyspace_events_rejects_invalid_flags() {
    let processor = RequestProcessor::new().unwrap();
    assert_command_response(
        &processor,
        "CONFIG SET notify-keyspace-events QQ",
        b"-ERR Invalid event class character. Use 'g$lszhxeKEtmdnoc'.\r\n",
    );
}

#[test]
fn config_set_notify_keyspace_events_empty_disables() {
    let processor = RequestProcessor::new().unwrap();
    // First enable
    assert_command_response(
        &processor,
        "CONFIG SET notify-keyspace-events KEA",
        b"+OK\r\n",
    );
    // Then disable with empty string — use raw RESP frame because the test
    // tokenizer strips empty quoted strings.
    let mut args = [ArgSlice::EMPTY; 16];
    let mut response = Vec::new();
    let frame = b"*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$22\r\nnotify-keyspace-events\r\n$0\r\n\r\n";
    let meta = parse_resp_command_arg_slices(frame, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    assert_command_response(
        &processor,
        "CONFIG GET notify-keyspace-events",
        b"*2\r\n$22\r\nnotify-keyspace-events\r\n$0\r\n\r\n",
    );
}

#[test]
fn config_set_validates_maxmemory_policy_loglevel_hz_and_boolean_params() {
    let processor = RequestProcessor::new().unwrap();

    // maxmemory-policy accepts valid policies
    assert_command_response(
        &processor,
        "CONFIG SET maxmemory-policy allkeys-lru",
        b"+OK\r\n",
    );
    assert_command_response(
        &processor,
        "CONFIG SET maxmemory-policy noeviction",
        b"+OK\r\n",
    );
    // maxmemory-policy rejects invalid
    let bad_policy =
        execute_command_line(&processor, "CONFIG SET maxmemory-policy badpolicy").unwrap();
    assert!(
        bad_policy.starts_with(b"-ERR"),
        "Invalid maxmemory-policy should error"
    );

    // loglevel accepts valid levels
    assert_command_response(&processor, "CONFIG SET loglevel debug", b"+OK\r\n");
    assert_command_response(&processor, "CONFIG SET loglevel warning", b"+OK\r\n");
    // loglevel rejects invalid
    let bad_level = execute_command_line(&processor, "CONFIG SET loglevel trace").unwrap();
    assert!(
        bad_level.starts_with(b"-ERR"),
        "Invalid loglevel should error"
    );

    // hz accepts valid range
    assert_command_response(&processor, "CONFIG SET hz 100", b"+OK\r\n");
    // hz rejects out-of-range
    let bad_hz = execute_command_line(&processor, "CONFIG SET hz 0").unwrap();
    assert!(bad_hz.starts_with(b"-ERR"), "hz=0 should error");
    let big_hz = execute_command_line(&processor, "CONFIG SET hz 999").unwrap();
    assert!(big_hz.starts_with(b"-ERR"), "hz=999 should error");

    // Boolean params accept yes/no
    assert_command_response(&processor, "CONFIG SET dynamic-hz yes", b"+OK\r\n");
    assert_command_response(&processor, "CONFIG SET lazyfree-lazy-expire no", b"+OK\r\n");
    // Boolean params reject non-boolean
    let bad_bool = execute_command_line(&processor, "CONFIG SET dynamic-hz maybe").unwrap();
    assert!(
        bad_bool.starts_with(b"-ERR"),
        "dynamic-hz=maybe should error"
    );

    // appendfsync accepts valid values
    assert_command_response(&processor, "CONFIG SET appendfsync always", b"+OK\r\n");
    assert_command_response(&processor, "CONFIG SET appendfsync everysec", b"+OK\r\n");
    // appendfsync rejects invalid
    let bad_sync = execute_command_line(&processor, "CONFIG SET appendfsync sometimes").unwrap();
    assert!(
        bad_sync.starts_with(b"-ERR"),
        "Invalid appendfsync should error"
    );

    // Numeric params accept integers
    assert_command_response(&processor, "CONFIG SET timeout 300", b"+OK\r\n");
    assert_command_response(
        &processor,
        "CONFIG SET repl-min-slaves-to-write 2",
        b"+OK\r\n",
    );
    // Numeric params reject non-integers
    let bad_num = execute_command_line(&processor, "CONFIG SET timeout abc").unwrap();
    assert!(
        bad_num.starts_with(b"-ERR"),
        "Non-integer timeout should error"
    );
    let bad_repl_min_slaves_to_write =
        execute_command_line(&processor, "CONFIG SET repl-min-slaves-to-write notnum").unwrap();
    assert!(
        bad_repl_min_slaves_to_write.starts_with(b"-ERR"),
        "Non-integer repl-min-slaves-to-write should error"
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    for member in [b"a", b"b", b"c"] {
        response.clear();
        let zadd = encode_resp(&[b"ZADD", b"zset", b"1", member]);
        let meta = parse_resp_command_arg_slices(&zadd, &mut args).unwrap();
        processor
            .execute_in_db(
                &args[..meta.argument_count],
                &mut response,
                DbName::default(),
            )
            .unwrap();
        assert!(response == b":1\r\n" || response == b":0\r\n");
    }

    response.clear();
    let object_encoding = b"*3\r\n$6\r\nOBJECT\r\n$8\r\nENCODING\r\n$4\r\nzset\r\n";
    let meta = parse_resp_command_arg_slices(object_encoding, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
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
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    for index in 0..10 {
        response.clear();
        let value = index.to_string();
        let rpush = encode_resp(&[b"RPUSH", b"list", value.as_bytes()]);
        let meta = parse_resp_command_arg_slices(&rpush, &mut args).unwrap();
        processor
            .execute_in_db(
                &args[..meta.argument_count],
                &mut response,
                DbName::default(),
            )
            .unwrap();
    }

    response.clear();
    let object_encoding = b"*3\r\n$6\r\nOBJECT\r\n$8\r\nENCODING\r\n$4\r\nlist\r\n";
    let meta = parse_resp_command_arg_slices(object_encoding, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$9\r\nquicklist\r\n");

    response.clear();
    let config_set_compact =
        b"*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$22\r\nlist-max-listpack-size\r\n$2\r\n-1\r\n";
    let meta = parse_resp_command_arg_slices(config_set_compact, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");

    response.clear();
    let meta = parse_resp_command_arg_slices(object_encoding, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"$8\r\nlistpack\r\n");
}

#[test]
fn list_object_encoding_ignores_quicklist_packed_threshold_external_scenarios() {
    let processor = RequestProcessor::new().unwrap();

    assert_command_response(
        &processor,
        "CONFIG SET list-max-listpack-size -2",
        b"+OK\r\n",
    );
    assert_command_response(
        &processor,
        "DEBUG QUICKLIST-PACKED-THRESHOLD 1b",
        b"+OK\r\n",
    );
    assert_command_integer(&processor, "LPUSH lst 9", 1);
    assert_command_integer(&processor, "LPUSH lst xxxxxxxxxx", 2);
    assert_command_integer(&processor, "LPUSH lst xxxxxxxxxx", 3);
    assert_command_response(&processor, "OBJECT ENCODING lst", b"$8\r\nlistpack\r\n");

    assert_command_response(&processor, "FLUSHDB", b"+OK\r\n");
    assert_command_response(
        &processor,
        "CONFIG SET list-max-listpack-size 1",
        b"+OK\r\n",
    );
    assert_command_response(
        &processor,
        "DEBUG QUICKLIST-PACKED-THRESHOLD 1b",
        b"+OK\r\n",
    );
    assert_command_integer(&processor, "LPUSH lst 9", 1);
    assert_command_integer(&processor, "LPUSH lst xxxxxxxxxx", 2);
    assert_command_integer(&processor, "LPUSH lst xxxxxxxxxx", 3);
    assert_command_response(&processor, "OBJECT ENCODING lst", b"$9\r\nquicklist\r\n");

    assert_command_response(&processor, "FLUSHDB", b"+OK\r\n");
    assert_command_response(
        &processor,
        "CONFIG SET list-max-listpack-size 3",
        b"+OK\r\n",
    );
    assert_command_integer(&processor, "RPUSH lst a b c", 3);
    assert_command_response(&processor, "OBJECT ENCODING lst", b"$8\r\nlistpack\r\n");
    assert_command_integer(&processor, "RPUSH lst hello", 4);
    assert_command_response(&processor, "OBJECT ENCODING lst", b"$9\r\nquicklist\r\n");
}

#[test]
fn config_get_returns_array_for_known_param() {
    let processor = RequestProcessor::new().unwrap();
    // "appendonly" is in default config with value "no"
    assert_command_response(
        &processor,
        "CONFIG GET appendonly",
        b"*2\r\n$10\r\nappendonly\r\n$2\r\nno\r\n",
    );
}

#[test]
fn config_get_returns_map_in_resp3() {
    let processor = RequestProcessor::new().unwrap();
    // RESP3: map with 1 entry
    processor.set_resp_protocol_version(RespProtocolVersion::Resp3);
    assert_command_response(
        &processor,
        "CONFIG GET appendonly",
        b"%1\r\n$10\r\nappendonly\r\n$2\r\nno\r\n",
    );

    // Reset to RESP2: flat array with 2 elements
    processor.set_resp_protocol_version(RespProtocolVersion::Resp2);
    assert_command_response(
        &processor,
        "CONFIG GET appendonly",
        b"*2\r\n$10\r\nappendonly\r\n$2\r\nno\r\n",
    );
}

#[test]
fn config_set_and_get_round_trip() {
    let processor = RequestProcessor::new().unwrap();
    assert_command_response(&processor, "CONFIG SET hz 20", b"+OK\r\n");
    assert_command_response(
        &processor,
        "CONFIG GET hz",
        b"*2\r\n$2\r\nhz\r\n$2\r\n20\r\n",
    );
}

#[test]
fn config_resetstat_clears_stats_and_returns_ok() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 8];
    let mut response = Vec::new();

    let config_resetstat = b"*2\r\n$6\r\nCONFIG\r\n$9\r\nRESETSTAT\r\n";
    let meta = parse_resp_command_arg_slices(config_resetstat, &mut args).unwrap();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(response, b"+OK\r\n");
}

#[test]
fn config_get_glob_pattern() {
    let processor = RequestProcessor::new().unwrap();
    // "max*" should match parameters like maxmemory, maxmemory-samples, etc.
    let response = execute_command_line(&processor, "CONFIG GET max*").unwrap();
    let response_str = String::from_utf8_lossy(&response);
    // Should return an array (not an error), with at least one match from defaults
    assert!(
        response.starts_with(b"*"),
        "expected array response for CONFIG GET max*, got: {response_str}"
    );
    // maxmemory-samples is in defaults, so we expect at least one key-value pair (*2 or more)
    assert!(
        !response.starts_with(b"*0\r\n"),
        "expected at least one match for CONFIG GET max*, got: {response_str}"
    );
    // Verify the response contains a known default key
    assert!(
        response_str.contains("maxmemory-samples"),
        "expected maxmemory-samples in CONFIG GET max* response, got: {response_str}"
    );
}

#[test]
fn config_set_immutable_param_returns_error() {
    let processor = RequestProcessor::new().unwrap();
    // "daemonize" is an immutable config parameter that cannot be changed at runtime
    assert_command_response(
        &processor,
        "CONFIG SET daemonize yes",
        b"-ERR CONFIG SET failed (possibly related to argument 'daemonize') - immutable config\r\n",
    );
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

#[test]
fn eval_unpack_with_huge_range_returns_error_instead_of_crash() {
    let processor = RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap();

    // This previously caused SIGSEGV due to signed integer overflow in Lua's luaB_unpack.
    let response = execute_frame(
        &processor,
        &encode_resp(&[b"EVAL", b"return {unpack({1,2,3}, 0, 2147483647)}", b"0"]),
    );
    let text = String::from_utf8_lossy(&response);
    assert!(
        text.contains("too many results to unpack"),
        "expected 'too many results to unpack' error, got: {text}"
    );

    // Negative start to INT_MAX should also be caught.
    let response2 = execute_frame(
        &processor,
        &encode_resp(&[b"EVAL", b"return {unpack({1,2,3}, -2, 2147483647)}", b"0"]),
    );
    let text2 = String::from_utf8_lossy(&response2);
    assert!(
        text2.contains("too many results to unpack"),
        "expected 'too many results to unpack' error, got: {text2}"
    );

    // Valid unpack usage should still work normally.
    assert_command_response(
        &processor,
        "EVAL \"return unpack({10,20,30})\" 0",
        b":10\r\n",
    );

    // Empty range (i > j) should return empty.
    let response3 = execute_frame(
        &processor,
        &encode_resp(&[b"EVAL", b"return {unpack({1,2,3}, 1, -1)}", b"0"]),
    );
    assert_eq!(response3, b"*0\r\n");
}

#[test]
fn scripting_massive_unpack_arguments_match_external_scenario_for_eval_and_function() {
    let processor = RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap();
    let script_body =
        "local a = {} for i=1,7999 do a[i] = 1 end return redis.call('lpush', 'l', unpack(a))";

    let eval_response = execute_frame(
        &processor,
        &encode_resp(&[b"EVAL", script_body.as_bytes(), b"1", b"l"]),
    );
    assert_eq!(eval_response, b":7999\r\n");
    assert_command_response(&processor, "LLEN l", b":7999\r\n");

    assert_command_response(&processor, "DEL l", b":1\r\n");

    let library = format!(
        "#!lua name=test\nredis.register_function('test', function(keys, args)\n{script_body}\nend)"
    );
    let loaded_library = execute_frame(
        &processor,
        &encode_resp(&[b"FUNCTION", b"LOAD", b"REPLACE", library.as_bytes()]),
    );
    assert_eq!(loaded_library, b"$4\r\ntest\r\n");

    let fcall_response = execute_frame(&processor, &encode_resp(&[b"FCALL", b"test", b"1", b"l"]));
    assert_eq!(fcall_response, b":7999\r\n");
    assert_command_response(&processor, "LLEN l", b":7999\r\n");
}

#[test]
fn evalsha_honours_no_writes_shebang_flag() {
    let processor = RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap();

    // SCRIPT LOAD a script with a no-writes shebang.
    let script = b"#!lua flags=no-writes\nreturn 1";
    let load_frame = encode_resp(&[b"SCRIPT", b"LOAD", script.as_slice()]);
    let sha_response = execute_frame(&processor, &load_frame);
    let sha = parse_bulk_payload(&sha_response).expect("SCRIPT LOAD returns sha1 bulk string");
    assert_eq!(sha.len(), 40);

    // EVALSHA with the returned SHA should succeed and return 1.
    let evalsha_frame = encode_resp(&[b"EVALSHA", sha.as_slice(), b"0"]);
    assert_eq!(execute_frame(&processor, &evalsha_frame), b":1\r\n");

    // The no-writes flag should block write commands from within the script.
    let write_script = b"#!lua flags=no-writes\nreturn redis.call('SET', KEYS[1], 'val')";
    let write_load = encode_resp(&[b"SCRIPT", b"LOAD", write_script.as_slice()]);
    let write_sha_response = execute_frame(&processor, &write_load);
    let write_sha =
        parse_bulk_payload(&write_sha_response).expect("SCRIPT LOAD returns sha1 bulk string");

    let evalsha_write = encode_resp(&[b"EVALSHA", write_sha.as_slice(), b"1", b"mykey"]);
    let response = execute_frame(&processor, &evalsha_write);
    let response_text = String::from_utf8_lossy(&response);
    assert!(
        response_text.contains("Write commands are not allowed"),
        "expected write-reject error from EVALSHA no-writes script, got: {response_text}"
    );
}

#[test]
fn eval_with_no_writes_shebang_allows_read_commands() {
    let processor = RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap();

    // First, SET a key so we can read it back.
    assert_command_response(&processor, "SET mykey hello", b"+OK\r\n");

    // EVAL a script with no-writes shebang that calls GET (a read command).
    let script = b"#!lua flags=no-writes\nreturn redis.call('GET', KEYS[1])";
    let eval_frame = encode_resp(&[b"EVAL", script.as_slice(), b"1", b"mykey"]);
    assert_eq!(execute_frame(&processor, &eval_frame), b"$5\r\nhello\r\n");
}

#[test]
fn script_exists_returns_array_for_multiple_shas() {
    let processor = RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap();

    // Load two scripts.
    let script_a = b"return 'a'";
    let script_b = b"return 'b'";
    let sha_a = parse_bulk_payload(&execute_frame(
        &processor,
        &encode_resp(&[b"SCRIPT", b"LOAD", script_a.as_slice()]),
    ))
    .expect("SCRIPT LOAD returns sha1");
    let sha_b = parse_bulk_payload(&execute_frame(
        &processor,
        &encode_resp(&[b"SCRIPT", b"LOAD", script_b.as_slice()]),
    ))
    .expect("SCRIPT LOAD returns sha1");

    let bogus_sha = b"0000000000000000000000000000000000000000";

    // SCRIPT EXISTS with three SHAs: loaded, bogus, loaded.
    let exists_frame = encode_resp(&[
        b"SCRIPT",
        b"EXISTS",
        sha_a.as_slice(),
        bogus_sha.as_slice(),
        sha_b.as_slice(),
    ]);
    assert_eq!(
        execute_frame(&processor, &exists_frame),
        b"*3\r\n:1\r\n:0\r\n:1\r\n"
    );

    // SCRIPT EXISTS with a single SHA also works.
    let exists_single = encode_resp(&[b"SCRIPT", b"EXISTS", sha_a.as_slice()]);
    assert_eq!(execute_frame(&processor, &exists_single), b"*1\r\n:1\r\n");
}

#[test]
fn debug_sleep_returns_ok() {
    let processor = RequestProcessor::new().unwrap();
    assert_command_response(&processor, "DEBUG SLEEP 0", b"+OK\r\n");
}

#[test]
fn debug_reload_returns_ok() {
    let processor = RequestProcessor::new().unwrap();
    assert_command_response(&processor, "DEBUG RELOAD", b"+OK\r\n");
}

#[test]
fn scripting_restore_expired_keys_with_expiration_time_matches_external_scenario() {
    let processor = RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap();

    assert_command_response(&processor, "DEBUG set-disable-deny-scripts 1", b"+OK\r\n");

    let eval_response = execute_frame(
        &processor,
        &encode_resp(&[
            b"EVAL",
            b"redis.call('SET', 'key1{t}', 'value'); local encoded = redis.call('DUMP', 'key1{t}'); redis.call('RESTORE', 'key2{t}', 1, encoded, 'REPLACE'); redis.call('DEBUG', 'SLEEP', 0.01); redis.call('RESTORE', 'key3{t}', 1, encoded, 'REPLACE'); return {redis.call('PEXPIRETIME', 'key2{t}'), redis.call('PEXPIRETIME', 'key3{t}')}",
            b"3",
            b"key1{t}",
            b"key2{t}",
            b"key3{t}",
        ]),
    );
    let eval_value = parse_resp_test_value(&eval_response);
    let eval_values = resp_test_array(&eval_value);
    assert_eq!(eval_values.len(), 2);
    let eval_first = resp_test_integer(&eval_values[0]);
    let eval_second = resp_test_integer(&eval_values[1]);
    assert!(
        eval_first > 0,
        "expected positive PEXPIRETIME values, got {eval_values:?}"
    );
    assert_eq!(eval_first, eval_second);

    let function_source = b"#!lua name=test\nredis.register_function('test', function(KEYS, ARGV)\n redis.call('SET', 'key1{t}', 'value'); local encoded = redis.call('DUMP', 'key1{t}'); redis.call('RESTORE', 'key2{t}', 1, encoded, 'REPLACE'); redis.call('DEBUG', 'SLEEP', 0.01); redis.call('RESTORE', 'key3{t}', 1, encoded, 'REPLACE'); return {redis.call('PEXPIRETIME', 'key2{t}'), redis.call('PEXPIRETIME', 'key3{t}')}\nend)";
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"FUNCTION", b"LOAD", b"REPLACE", function_source]),
        ),
        b"$4\r\ntest\r\n"
    );
    let fcall_response = execute_frame(
        &processor,
        &encode_resp(&[b"FCALL", b"test", b"3", b"key1{t}", b"key2{t}", b"key3{t}"]),
    );
    let fcall_value = parse_resp_test_value(&fcall_response);
    let fcall_values = resp_test_array(&fcall_value);
    assert_eq!(fcall_values.len(), 2);
    let fcall_first = resp_test_integer(&fcall_values[0]);
    let fcall_second = resp_test_integer(&fcall_values[1]);
    assert!(
        fcall_first > 0,
        "expected positive FCALL PEXPIRETIME values, got {fcall_values:?}"
    );
    assert_eq!(fcall_first, fcall_second);

    assert_command_response(&processor, "DEBUG set-disable-deny-scripts 0", b"+OK\r\n");
}

#[test]
fn scripting_freezes_key_expiration_during_execution_matches_external_scenario() {
    let processor = RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap();

    assert_command_response(&processor, "DEBUG set-disable-deny-scripts 1", b"+OK\r\n");

    let eval_response = execute_frame(
        &processor,
        &encode_resp(&[
            b"EVAL",
            b"redis.call('SET', 'key', 'value', 'PX', '1'); redis.call('DEBUG', 'SLEEP', 0.01); return redis.call('EXISTS', 'key')",
            b"1",
            b"key",
        ]),
    );
    assert_eq!(
        parse_resp_test_value(&eval_response),
        RespTestValue::Integer(1)
    );
    assert_command_integer(&processor, "EXISTS key", 0);

    let function_body = b"#!lua name=test\nredis.register_function('test', function(KEYS, ARGV)\n redis.call('SET', 'key', 'value', 'PX', '1'); redis.call('DEBUG', 'SLEEP', 0.01); return redis.call('EXISTS', 'key')\nend)";
    assert_eq!(
        execute_frame(
            &processor,
            &encode_resp(&[b"FUNCTION", b"LOAD", b"REPLACE", function_body]),
        ),
        b"$4\r\ntest\r\n"
    );
    let fcall_response =
        execute_frame(&processor, &encode_resp(&[b"FCALL", b"test", b"1", b"key"]));
    assert_eq!(
        parse_resp_test_value(&fcall_response),
        RespTestValue::Integer(1)
    );
    assert_command_integer(&processor, "EXISTS key", 0);

    assert_command_response(&processor, "DEBUG set-disable-deny-scripts 0", b"+OK\r\n");
}

#[test]
fn list_encoding_conversion_and_debug_reload_match_external_list_scenarios() {
    let processor = RequestProcessor::new().unwrap();

    assert_command_response(
        &processor,
        "CONFIG SET list-max-listpack-size 3",
        b"+OK\r\n",
    );

    assert_command_integer(&processor, "DEL lst", 0);
    assert_command_integer(&processor, "RPUSH lst a b c", 3);
    assert_command_response(&processor, "OBJECT ENCODING lst", b"$8\r\nlistpack\r\n");
    assert_command_integer(&processor, "RPUSH lst hello", 4);
    assert_command_response(&processor, "OBJECT ENCODING lst", b"$9\r\nquicklist\r\n");

    assert_command_integer(&processor, "DEL lst", 1);
    assert_command_integer(&processor, "RPUSH lst a b c", 3);
    assert_command_response(&processor, "OBJECT ENCODING lst", b"$8\r\nlistpack\r\n");
    assert_command_integer(&processor, "LINSERT lst AFTER b hello", 4);
    assert_command_response(&processor, "OBJECT ENCODING lst", b"$9\r\nquicklist\r\n");

    assert_command_integer(&processor, "DEL lst", 1);
    assert_command_integer(&processor, "RPUSH lst a b c", 3);
    assert_command_response(&processor, "OBJECT ENCODING lst", b"$8\r\nlistpack\r\n");
    assert_command_response(&processor, "LSET lst 0 hello", b"+OK\r\n");
    assert_command_response(&processor, "OBJECT ENCODING lst", b"$9\r\nquicklist\r\n");

    assert_command_integer(&processor, "DEL lsrc{t} ldes{t}", 0);
    assert_command_integer(&processor, "RPUSH lsrc{t} a b c hello", 4);
    assert_command_response(
        &processor,
        "OBJECT ENCODING lsrc{t}",
        b"$9\r\nquicklist\r\n",
    );
    assert_command_integer(&processor, "RPUSH ldes{t} d e f", 3);
    assert_command_response(&processor, "OBJECT ENCODING ldes{t}", b"$8\r\nlistpack\r\n");
    assert_command_response(
        &processor,
        "LMOVE lsrc{t} ldes{t} RIGHT RIGHT",
        b"$5\r\nhello\r\n",
    );
    assert_command_response(
        &processor,
        "OBJECT ENCODING ldes{t}",
        b"$9\r\nquicklist\r\n",
    );

    assert_command_integer(&processor, "DEL lst", 1);
    assert_command_integer(&processor, "RPUSH lst a b c", 3);
    assert_command_response(&processor, "OBJECT ENCODING lst", b"$8\r\nlistpack\r\n");
    assert_command_response(&processor, "DEBUG RELOAD", b"+OK\r\n");
    assert_command_response(&processor, "OBJECT ENCODING lst", b"$8\r\nlistpack\r\n");

    assert_command_integer(&processor, "RPUSH lst d", 4);
    assert_command_response(&processor, "OBJECT ENCODING lst", b"$9\r\nquicklist\r\n");
    assert_command_response(&processor, "DEBUG RELOAD", b"+OK\r\n");
    assert_command_response(&processor, "OBJECT ENCODING lst", b"$9\r\nquicklist\r\n");

    assert_command_response(&processor, "RPOP lst", b"$1\r\nd\r\n");
    assert_command_response(&processor, "OBJECT ENCODING lst", b"$9\r\nquicklist\r\n");
    assert_command_response(&processor, "DEBUG RELOAD", b"+OK\r\n");
    assert_command_response(&processor, "OBJECT ENCODING lst", b"$8\r\nlistpack\r\n");
}

#[test]
fn list_quicklist_to_listpack_encoding_conversion_matches_external_scenario() {
    let processor = RequestProcessor::new().unwrap();
    let large_quicklist_value = vec![b'x'; 8192];

    assert_command_response(
        &processor,
        "CONFIG SET list-max-listpack-size 3",
        b"+OK\r\n",
    );

    assert_command_integer(&processor, "DEL lst", 0);
    assert_command_integer(&processor, "RPUSH lst a b c d", 4);
    assert_command_response(&processor, "OBJECT ENCODING lst", b"$9\r\nquicklist\r\n");
    assert_command_response(
        &processor,
        "RPOP lst 3",
        b"*3\r\n$1\r\nd\r\n$1\r\nc\r\n$1\r\nb\r\n",
    );
    assert_command_response(&processor, "OBJECT ENCODING lst", b"$8\r\nlistpack\r\n");

    assert_command_integer(&processor, "DEL lst", 1);
    assert_command_integer(&processor, "RPUSH lst a a a d", 4);
    assert_command_response(&processor, "OBJECT ENCODING lst", b"$9\r\nquicklist\r\n");
    assert_command_integer(&processor, "LREM lst 3 a", 3);
    assert_command_response(&processor, "OBJECT ENCODING lst", b"$8\r\nlistpack\r\n");

    assert_command_integer(&processor, "DEL lst", 1);
    assert_command_integer(&processor, "RPUSH lst a b c d", 4);
    assert_command_response(&processor, "OBJECT ENCODING lst", b"$9\r\nquicklist\r\n");
    assert_command_response(&processor, "LTRIM lst 1 1", b"+OK\r\n");
    assert_command_response(&processor, "OBJECT ENCODING lst", b"$8\r\nlistpack\r\n");

    assert_command_response(
        &processor,
        "CONFIG SET list-max-listpack-size -1",
        b"+OK\r\n",
    );

    assert_command_integer(&processor, "DEL lst", 1);
    let create_quicklist_rpop = encode_resp(&[
        b"RPUSH",
        b"lst",
        b"a",
        b"b",
        b"c",
        large_quicklist_value.as_slice(),
    ]);
    assert_eq!(execute_frame(&processor, &create_quicklist_rpop), b":4\r\n");
    assert_command_response(&processor, "OBJECT ENCODING lst", b"$9\r\nquicklist\r\n");
    assert_command_response(
        &processor,
        "RPOP lst 1",
        format!(
            "*1\r\n${}\r\n{}\r\n",
            large_quicklist_value.len(),
            String::from_utf8_lossy(&large_quicklist_value)
        )
        .as_bytes(),
    );
    assert_command_response(&processor, "OBJECT ENCODING lst", b"$8\r\nlistpack\r\n");

    assert_command_integer(&processor, "DEL lst", 1);
    let create_quicklist_lrem =
        encode_resp(&[b"RPUSH", b"lst", b"a", large_quicklist_value.as_slice()]);
    assert_eq!(execute_frame(&processor, &create_quicklist_lrem), b":2\r\n");
    assert_command_response(&processor, "OBJECT ENCODING lst", b"$9\r\nquicklist\r\n");
    let lrem_large = encode_resp(&[b"LREM", b"lst", b"1", large_quicklist_value.as_slice()]);
    assert_eq!(execute_frame(&processor, &lrem_large), b":1\r\n");
    assert_command_response(&processor, "OBJECT ENCODING lst", b"$8\r\nlistpack\r\n");

    assert_command_integer(&processor, "DEL lst", 1);
    let create_quicklist_ltrim = encode_resp(&[
        b"RPUSH",
        b"lst",
        b"a",
        b"b",
        large_quicklist_value.as_slice(),
    ]);
    assert_eq!(
        execute_frame(&processor, &create_quicklist_ltrim),
        b":3\r\n"
    );
    assert_command_response(&processor, "OBJECT ENCODING lst", b"$9\r\nquicklist\r\n");
    assert_command_response(&processor, "LTRIM lst 0 1", b"+OK\r\n");
    assert_command_response(&processor, "OBJECT ENCODING lst", b"$8\r\nlistpack\r\n");

    assert_command_integer(&processor, "DEL lst", 1);
    let create_quicklist_lset = encode_resp(&[
        b"RPUSH",
        b"lst",
        large_quicklist_value.as_slice(),
        b"a",
        b"b",
    ]);
    assert_eq!(execute_frame(&processor, &create_quicklist_lset), b":3\r\n");
    assert_command_response(&processor, "OBJECT ENCODING lst", b"$9\r\nquicklist\r\n");
    assert_command_response(&processor, "RPOP lst 2", b"*2\r\n$1\r\nb\r\n$1\r\na\r\n");
    assert_command_response(&processor, "OBJECT ENCODING lst", b"$9\r\nquicklist\r\n");
    assert_command_response(&processor, "LSET lst -1 c", b"+OK\r\n");
    assert_command_response(&processor, "OBJECT ENCODING lst", b"$8\r\nlistpack\r\n");
}

#[test]
fn list3_ziplist_value_encoding_and_backlink_external_scenario_runs_without_storage_failure() {
    let processor = RequestProcessor::new_with_options_and_store_config(
        1,
        false,
        scripting_runtime_config_from_values(None, None, None, None),
        TsavoriteKvConfig {
            max_in_memory_pages: 16_384,
            ..TsavoriteKvConfig::default()
        },
    )
    .unwrap();
    assert_command_response(
        &processor,
        "CONFIG SET list-max-listpack-size 16",
        b"+OK\r\n",
    );

    let build_external_values = || -> Vec<Vec<u8>> {
        let mut values = Vec::with_capacity(200);
        for index in 0usize..200 {
            let value = match index % 7 {
                0 => {
                    let len = 20_000usize + ((index / 7) % 5) * 10_000usize;
                    vec![b'x'; len]
                }
                1 => (1_000usize + index * 17).to_string().into_bytes(),
                2 => (70_000usize + index * 12_345).to_string().into_bytes(),
                3 => (5_000_000_000u64 + index as u64 * 1_000_000_003u64)
                    .to_string()
                    .into_bytes(),
                4 => format!("-{}", 1_000usize + index * 31).into_bytes(),
                5 => format!("-{}", 90_000usize + index * 123).into_bytes(),
                _ => format!("-{}", 9_000_000_000u64 + index as u64 * 97_531u64).into_bytes(),
            };
            values.push(value);
        }
        values
    };

    for iteration in 0usize..10 {
        assert_command_integer(&processor, "DEL l", if iteration == 0 { 0 } else { 1 });
        let values = build_external_values();
        for (position, value) in values.iter().enumerate() {
            let response = execute_frame(&processor, &encode_resp(&[b"RPUSH", b"l", value]));
            assert_eq!(
                parse_integer_response(&response),
                i64::try_from(position + 1).unwrap(),
                "RPUSH failed in external list-3 iteration {iteration} position {position} value_len={}",
                value.len()
            );
        }
        assert_command_integer(&processor, "LLEN l", 200);
        for index in (0usize..200).rev() {
            let index_token = index.to_string();
            let response = execute_frame(
                &processor,
                &encode_resp(&[b"LINDEX", b"l", index_token.as_bytes()]),
            );
            assert_eq!(
                parse_bulk_payload(&response).unwrap(),
                values[index],
                "LINDEX mismatch in external list-3 iteration {iteration} index {index}"
            );
        }
    }
}

#[test]
fn debug_object_reports_metadata_for_existing_key() {
    let processor = RequestProcessor::new().unwrap();

    // Missing key returns an error.
    assert_command_error(
        &processor,
        "DEBUG OBJECT nosuchkey",
        b"-ERR no such key\r\n",
    );

    // Set a key and verify DEBUG OBJECT returns a bulk string with expected metadata.
    assert_command_response(&processor, "SET mykey myvalue", b"+OK\r\n");
    let response = execute_command_line(&processor, "DEBUG OBJECT mykey").unwrap();
    let response_str = std::str::from_utf8(&response).expect("valid UTF-8 response");
    assert!(
        response_str.starts_with('$'),
        "expected bulk string, got: {response_str}"
    );
    assert!(
        response_str.contains("Value at:"),
        "expected 'Value at:' in response: {response_str}"
    );
    assert!(
        response_str.contains("refcount:1"),
        "expected 'refcount:1' in response: {response_str}"
    );
    assert!(
        response_str.contains("encoding:embstr"),
        "expected 'encoding:embstr' for short string in response: {response_str}"
    );
}

#[test]
fn debug_htstats_key_rejects_non_hashtable_values() {
    let processor = RequestProcessor::new().unwrap();
    assert_command_response(&processor, "SADD myset a b c", b":3\r\n");
    assert_command_response(
        &processor,
        "DEBUG HTSTATS-KEY myset",
        b"-ERR The value stored at the specified key is not represented using an hash table\r\n",
    );
}

#[test]
fn sort_store_quicklist_debug_object_matches_external_scenario() {
    let processor = RequestProcessor::new().unwrap();
    assert_command_response(&processor, "DEL lst{t} lst_dst{t}", b":0\r\n");
    assert_command_response(
        &processor,
        "CONFIG SET list-max-listpack-size -1",
        b"+OK\r\n",
    );
    assert_command_response(&processor, "CONFIG SET list-compress-depth 12", b"+OK\r\n");

    let mut lpush_parts = Vec::with_capacity(6002);
    lpush_parts.push(b"LPUSH".to_vec());
    lpush_parts.push(b"lst{t}".to_vec());
    for _ in 0..6000 {
        lpush_parts.push(b"1".to_vec());
    }
    let lpush_slices = lpush_parts.iter().map(Vec::as_slice).collect::<Vec<_>>();
    let lpush_frame = encode_resp(&lpush_slices);
    let mut lpush_args = vec![ArgSlice::EMPTY; lpush_slices.len()];
    let lpush_meta = parse_resp_command_arg_slices(&lpush_frame, &mut lpush_args).unwrap();
    let mut lpush_response = Vec::new();
    processor
        .execute_in_db(
            &lpush_args[..lpush_meta.argument_count],
            &mut lpush_response,
            DbName::default(),
        )
        .unwrap();
    assert_eq!(lpush_response, b":6000\r\n");

    assert_command_response(&processor, "SORT lst{t} STORE lst_dst{t}", b":6000\r\n");
    assert_command_response(
        &processor,
        "OBJECT ENCODING lst_dst{t}",
        b"$9\r\nquicklist\r\n",
    );

    let debug_object = parse_bulk_payload(&execute_frame(
        &processor,
        &encode_resp(&[b"DEBUG", b"OBJECT", b"lst_dst{t}"]),
    ))
    .expect("debug object bulk payload");
    let debug_text = std::str::from_utf8(&debug_object).expect("utf-8 debug payload");
    assert!(
        debug_text.contains("ql_listpack_max:-1 ql_compressed:1"),
        "expected quicklist metadata in DEBUG OBJECT: {debug_text}"
    );
}

#[test]
fn debug_htstats_key_reports_synthetic_set_hash_table_stats() {
    let processor = RequestProcessor::new().unwrap();
    assert_command_response(
        &processor,
        "CONFIG SET set-max-listpack-entries 0",
        b"+OK\r\n",
    );
    assert_command_response(&processor, "SADD myset m:1 m:2 m:3", b":3\r\n");

    let response = execute_command_line(&processor, "DEBUG HTSTATS-KEY myset full").unwrap();
    let payload = parse_bulk_payload(&response).expect("DEBUG HTSTATS-KEY should return bulk");
    let payload = String::from_utf8(payload).expect("payload must be valid UTF-8");

    assert!(
        payload.contains("Hash table 0 stats (main hash table):"),
        "missing main hash table header: {payload}"
    );
    assert!(
        payload.contains("table size: 8"),
        "expected synthetic table size 8: {payload}"
    );
    assert!(
        payload.contains("number of elements: 3"),
        "expected 3 members: {payload}"
    );
    assert!(
        payload.contains("different slots: 1"),
        "expected synthetic slot summary: {payload}"
    );
    assert!(
        payload.contains("max chain length: 3"),
        "expected synthetic chain summary: {payload}"
    );
}

#[test]
fn debug_pause_cron_toggles_active_expire() {
    let processor = RequestProcessor::new().unwrap();
    assert_command_response(&processor, "DEBUG PAUSE-CRON 1", b"+OK\r\n");
    assert_command_response(&processor, "DEBUG PAUSE-CRON 0", b"+OK\r\n");
}

#[test]
fn memory_malloc_stats_returns_bulk_string() {
    let processor = RequestProcessor::new().unwrap();
    let frame = encode_resp(&[b"MEMORY", b"MALLOC-STATS"]);
    let response = execute_frame(&processor, &frame);
    assert!(
        response.starts_with(b"$"),
        "MEMORY MALLOC-STATS should return a bulk string, got: {:?}",
        String::from_utf8_lossy(&response)
    );
}

#[test]
fn memory_purge_returns_ok() {
    let processor = RequestProcessor::new().unwrap();
    let frame = encode_resp(&[b"MEMORY", b"PURGE"]);
    let response = execute_frame(&processor, &frame);
    assert_eq!(response, b"+OK\r\n");
}

#[test]
fn lolwut_returns_bulk_with_version_info() {
    let processor = RequestProcessor::new().unwrap();

    // Plain LOLWUT returns a bulk string containing version info.
    let frame = encode_resp(&[b"LOLWUT"]);
    let response = execute_frame(&processor, &frame);
    assert!(
        response.starts_with(b"$"),
        "LOLWUT should return a bulk string, got: {:?}",
        String::from_utf8_lossy(&response)
    );
    assert!(
        String::from_utf8_lossy(&response).contains("8.4.0"),
        "LOLWUT should contain version info, got: {:?}",
        String::from_utf8_lossy(&response)
    );

    // LOLWUT VERSION 6 also returns a bulk string.
    let frame_v6 = encode_resp(&[b"LOLWUT", b"VERSION", b"6"]);
    let response_v6 = execute_frame(&processor, &frame_v6);
    assert!(
        response_v6.starts_with(b"$"),
        "LOLWUT VERSION 6 should return a bulk string, got: {:?}",
        String::from_utf8_lossy(&response_v6)
    );
}

#[test]
fn pubsub_subscribe_acks_use_push_type_in_resp3_and_array_in_resp2() {
    let processor = RequestProcessor::new().unwrap();

    // RESP2: subscribe ack uses *3 (array).
    let resp2_sub = execute_command_line(&processor, "SUBSCRIBE ch1 ch2").unwrap();
    assert!(
        resp2_sub.starts_with(b"*3\r\n"),
        "RESP2 subscribe ack should use *3 array, got: {:?}",
        String::from_utf8_lossy(&resp2_sub)
    );
    assert_eq!(
        resp2_sub,
        b"*3\r\n$9\r\nsubscribe\r\n$3\r\nch1\r\n:1\r\n\
          *3\r\n$9\r\nsubscribe\r\n$3\r\nch2\r\n:2\r\n"
            .as_slice()
    );

    // RESP2: unsubscribe uses *3.
    let resp2_unsub = execute_command_line(&processor, "UNSUBSCRIBE ch1").unwrap();
    assert!(
        resp2_unsub.starts_with(b"*3\r\n"),
        "RESP2 unsubscribe ack should use *3 array"
    );

    // Switch to RESP3.
    processor.set_resp_protocol_version(RespProtocolVersion::Resp3);

    // RESP3: subscribe ack uses >3 (push type).
    let resp3_sub = execute_command_line(&processor, "SUBSCRIBE ch3").unwrap();
    assert!(
        resp3_sub.starts_with(b">3\r\n"),
        "RESP3 subscribe ack should use >3 push type, got: {:?}",
        String::from_utf8_lossy(&resp3_sub)
    );
    assert_eq!(
        resp3_sub,
        b">3\r\n$9\r\nsubscribe\r\n$3\r\nch3\r\n:1\r\n".as_slice()
    );

    // RESP3: psubscribe ack uses >3.
    let resp3_psub = execute_command_line(&processor, "PSUBSCRIBE p*").unwrap();
    assert!(
        resp3_psub.starts_with(b">3\r\n"),
        "RESP3 psubscribe ack should use >3 push type, got: {:?}",
        String::from_utf8_lossy(&resp3_psub)
    );

    // RESP3: unsubscribe ack uses >3.
    let resp3_unsub = execute_command_line(&processor, "UNSUBSCRIBE ch3").unwrap();
    assert!(
        resp3_unsub.starts_with(b">3\r\n"),
        "RESP3 unsubscribe ack should use >3 push type, got: {:?}",
        String::from_utf8_lossy(&resp3_unsub)
    );

    // RESP3: punsubscribe with no args uses >3.
    let resp3_punsub = execute_command_line(&processor, "PUNSUBSCRIBE").unwrap();
    assert!(
        resp3_punsub.starts_with(b">3\r\n"),
        "RESP3 punsubscribe ack should use >3 push type, got: {:?}",
        String::from_utf8_lossy(&resp3_punsub)
    );
}

#[test]
fn pubsub_message_delivery_uses_push_type_for_resp3_subscribers() {
    let processor = RequestProcessor::new().unwrap();
    let resp2_client = ClientId::new(10);
    let resp3_client = ClientId::new(20);

    processor.register_pubsub_client(resp2_client);
    processor.register_pubsub_client(resp3_client);

    // Subscribe resp2_client on RESP2 (default).
    assert_client_command_response(
        &processor,
        "SUBSCRIBE news",
        resp2_client,
        b"*3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:1\r\n",
    );

    // Switch resp3_client to RESP3 and subscribe.
    processor.set_resp_protocol_version(RespProtocolVersion::Resp3);
    processor.update_pubsub_client_resp_version(resp3_client, RespProtocolVersion::Resp3);
    assert_client_command_response(
        &processor,
        "SUBSCRIBE news",
        resp3_client,
        b">3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:1\r\n",
    );

    // Publish a message — both clients receive it.
    assert_command_response(&processor, "PUBLISH news hello", b":2\r\n");

    // RESP2 subscriber gets *3 (array).
    let resp2_msgs = processor.take_pending_pubsub_messages(resp2_client);
    assert_eq!(resp2_msgs.len(), 1);
    assert_eq!(
        resp2_msgs[0],
        b"*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$5\r\nhello\r\n".to_vec()
    );

    // RESP3 subscriber gets >3 (push type).
    let resp3_msgs = processor.take_pending_pubsub_messages(resp3_client);
    assert_eq!(resp3_msgs.len(), 1);
    assert_eq!(
        resp3_msgs[0],
        b">3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$5\r\nhello\r\n".to_vec()
    );

    // Pattern subscribe on RESP3 client.
    assert_client_command_response(
        &processor,
        "PSUBSCRIBE new*",
        resp3_client,
        b">3\r\n$10\r\npsubscribe\r\n$4\r\nnew*\r\n:2\r\n",
    );

    // Publish again — resp3_client gets both channel and pattern messages.
    assert_command_response(&processor, "PUBLISH news world", b":3\r\n");
    let resp3_msgs2 = processor.take_pending_pubsub_messages(resp3_client);
    assert_eq!(resp3_msgs2.len(), 2);
    // Channel message: >3
    assert!(
        resp3_msgs2[0].starts_with(b">3\r\n"),
        "RESP3 channel message should use >3 push type"
    );
    // Pattern message: >4
    assert!(
        resp3_msgs2[1].starts_with(b">4\r\n"),
        "RESP3 pattern message should use >4 push type"
    );
}

#[test]
fn hscan_returns_map_in_resp3_and_flat_array_in_resp2() {
    let processor = RequestProcessor::new().unwrap();

    // Populate a hash.
    assert_command_response(&processor, "HSET myhash f1 v1 f2 v2", b":2\r\n");

    // RESP2: HSCAN inner data is a flat array (*4 for 2 pairs).
    let resp2 = execute_command_line(&processor, "HSCAN myhash 0").unwrap();
    assert!(
        resp2.starts_with(b"*2\r\n"),
        "HSCAN outer should be *2 array"
    );
    assert!(
        resp2.windows(4).any(|w| w == b"*4\r\n"),
        "RESP2 HSCAN inner data should be *4 flat array, got: {:?}",
        String::from_utf8_lossy(&resp2)
    );

    // RESP3: HSCAN inner data is a map (%2 for 2 pairs).
    processor.set_resp_protocol_version(RespProtocolVersion::Resp3);
    let resp3 = execute_command_line(&processor, "HSCAN myhash 0").unwrap();
    assert!(
        resp3.starts_with(b"*2\r\n"),
        "HSCAN outer should remain *2 array in RESP3"
    );
    assert!(
        resp3.windows(4).any(|w| w == b"%2\r\n"),
        "RESP3 HSCAN inner data should be %2 map, got: {:?}",
        String::from_utf8_lossy(&resp3)
    );

    // HSCAN NOVALUES: always an array even in RESP3.
    let resp3_novalues = execute_command_line(&processor, "HSCAN myhash 0 NOVALUES").unwrap();
    assert!(
        !resp3_novalues.windows(1).any(|w| w == b"%"),
        "HSCAN NOVALUES should not use map in RESP3, got: {:?}",
        String::from_utf8_lossy(&resp3_novalues)
    );
}

#[test]
fn zscan_returns_map_in_resp3_and_flat_array_in_resp2() {
    let processor = RequestProcessor::new().unwrap();

    // Populate a sorted set.
    assert_command_response(&processor, "ZADD myzset 1.5 alpha 2.5 beta", b":2\r\n");

    // RESP2: ZSCAN inner data is a flat array (*4 for 2 members).
    let resp2 = execute_command_line(&processor, "ZSCAN myzset 0").unwrap();
    assert!(
        resp2.windows(4).any(|w| w == b"*4\r\n"),
        "RESP2 ZSCAN inner data should be *4 flat array, got: {:?}",
        String::from_utf8_lossy(&resp2)
    );

    // RESP3: ZSCAN inner data is a map (%2).
    processor.set_resp_protocol_version(RespProtocolVersion::Resp3);
    let resp3 = execute_command_line(&processor, "ZSCAN myzset 0").unwrap();
    assert!(
        resp3.windows(4).any(|w| w == b"%2\r\n"),
        "RESP3 ZSCAN inner data should be %2 map, got: {:?}",
        String::from_utf8_lossy(&resp3)
    );
}

#[test]
fn pubsub_numsub_returns_map_in_resp3() {
    let processor = RequestProcessor::new().unwrap();

    // RESP2: PUBSUB NUMSUB returns flat array *4 (2 channels × 2 elements each).
    let resp2 = execute_command_line(&processor, "PUBSUB NUMSUB ch1 ch2").unwrap();
    assert!(
        resp2.starts_with(b"*4\r\n"),
        "RESP2 PUBSUB NUMSUB should return *4 flat array, got: {:?}",
        String::from_utf8_lossy(&resp2)
    );

    // RESP3: PUBSUB NUMSUB returns map %2 (2 channel → count pairs).
    processor.set_resp_protocol_version(RespProtocolVersion::Resp3);
    let resp3 = execute_command_line(&processor, "PUBSUB NUMSUB ch1 ch2").unwrap();
    assert!(
        resp3.starts_with(b"%2\r\n"),
        "RESP3 PUBSUB NUMSUB should return %2 map, got: {:?}",
        String::from_utf8_lossy(&resp3)
    );
}

#[test]
fn lpop_returns_resp3_null_for_missing_key() {
    let processor = RequestProcessor::new().unwrap();
    // RESP2: returns $-1
    let resp2 = execute_command_line(&processor, "LPOP nokey").unwrap();
    assert_eq!(resp2, b"$-1\r\n");
    // RESP3: returns _
    processor.set_resp_protocol_version(RespProtocolVersion::Resp3);
    let resp3 = execute_command_line(&processor, "LPOP nokey").unwrap();
    assert_eq!(resp3, b"_\r\n");
}

#[test]
fn zscore_returns_resp3_null_for_missing_member() {
    let processor = RequestProcessor::new().unwrap();
    assert_command_response(&processor, "ZADD myz 1.0 alpha", b":1\r\n");
    // RESP2: ZSCORE missing member returns $-1
    let resp2 = execute_command_line(&processor, "ZSCORE myz beta").unwrap();
    assert_eq!(resp2, b"$-1\r\n");
    // RESP3: returns _
    processor.set_resp_protocol_version(RespProtocolVersion::Resp3);
    let resp3 = execute_command_line(&processor, "ZSCORE myz beta").unwrap();
    assert_eq!(resp3, b"_\r\n");
}

#[test]
fn get_returns_resp3_null_for_missing_key() {
    let processor = RequestProcessor::new().unwrap();
    // RESP2: GET on missing key returns $-1
    let resp2 = execute_command_line(&processor, "GET nokey").unwrap();
    assert_eq!(resp2, b"$-1\r\n");

    // RESP3: GET on missing key returns _
    processor.set_resp_protocol_version(RespProtocolVersion::Resp3);
    let resp3 = execute_command_line(&processor, "GET nokey").unwrap();
    assert_eq!(resp3, b"_\r\n");
}

#[test]
fn hget_returns_resp3_null_for_missing_field() {
    let processor = RequestProcessor::new().unwrap();
    assert_command_response(&processor, "HSET h1 f1 v1", b":1\r\n");

    // RESP2: HGET on missing field returns $-1
    let resp2 = execute_command_line(&processor, "HGET h1 nofield").unwrap();
    assert_eq!(resp2, b"$-1\r\n");

    // RESP3: HGET on missing field returns _
    processor.set_resp_protocol_version(RespProtocolVersion::Resp3);
    let resp3 = execute_command_line(&processor, "HGET h1 nofield").unwrap();
    assert_eq!(resp3, b"_\r\n");
}

#[test]
fn smembers_returns_set_type_in_resp3_and_array_in_resp2() {
    let processor = RequestProcessor::new().unwrap();
    assert_command_response(&processor, "SADD myset a b c", b":3\r\n");

    // RESP2: SMEMBERS returns *N array
    let resp2 = execute_command_line(&processor, "SMEMBERS myset").unwrap();
    assert!(
        resp2.starts_with(b"*3\r\n"),
        "RESP2 SMEMBERS should start with *3, got: {:?}",
        String::from_utf8_lossy(&resp2)
    );

    // RESP3: SMEMBERS returns ~N set type
    processor.set_resp_protocol_version(RespProtocolVersion::Resp3);
    let resp3 = execute_command_line(&processor, "SMEMBERS myset").unwrap();
    assert!(
        resp3.starts_with(b"~3\r\n"),
        "RESP3 SMEMBERS should start with ~3, got: {:?}",
        String::from_utf8_lossy(&resp3)
    );

    // RESP3: SMEMBERS on missing key returns ~0
    let resp3_empty = execute_command_line(&processor, "SMEMBERS nokey").unwrap();
    assert_eq!(resp3_empty, b"~0\r\n");

    // RESP2: SMEMBERS on missing key returns *0
    processor.set_resp_protocol_version(RespProtocolVersion::Resp2);
    let resp2_empty = execute_command_line(&processor, "SMEMBERS nokey").unwrap();
    assert_eq!(resp2_empty, b"*0\r\n");
}

#[test]
fn sunion_sinter_sdiff_return_set_type_in_resp3() {
    let processor = RequestProcessor::new().unwrap();
    assert_command_response(&processor, "SADD s1 a b", b":2\r\n");
    assert_command_response(&processor, "SADD s2 b c", b":2\r\n");

    // RESP2: returns *N array
    let resp2 = execute_command_line(&processor, "SUNION s1 s2").unwrap();
    assert!(
        resp2.starts_with(b"*3\r\n"),
        "RESP2 SUNION should use array type"
    );

    // RESP3: returns ~N set type
    processor.set_resp_protocol_version(RespProtocolVersion::Resp3);

    let sunion = execute_command_line(&processor, "SUNION s1 s2").unwrap();
    assert!(
        sunion.starts_with(b"~3\r\n"),
        "RESP3 SUNION should use set type, got: {:?}",
        String::from_utf8_lossy(&sunion)
    );

    let sinter = execute_command_line(&processor, "SINTER s1 s2").unwrap();
    assert!(
        sinter.starts_with(b"~1\r\n"),
        "RESP3 SINTER should use set type, got: {:?}",
        String::from_utf8_lossy(&sinter)
    );

    let sdiff = execute_command_line(&processor, "SDIFF s1 s2").unwrap();
    assert!(
        sdiff.starts_with(b"~1\r\n"),
        "RESP3 SDIFF should use set type, got: {:?}",
        String::from_utf8_lossy(&sdiff)
    );
}

#[test]
fn zscore_returns_double_type_in_resp3() {
    let processor = RequestProcessor::new().unwrap();
    assert_command_response(&processor, "ZADD myz 1.5 alpha", b":1\r\n");

    // RESP2: ZSCORE returns bulk string
    let resp2 = execute_command_line(&processor, "ZSCORE myz alpha").unwrap();
    assert_eq!(resp2, b"$3\r\n1.5\r\n");

    // RESP3: ZSCORE returns double type
    processor.set_resp_protocol_version(RespProtocolVersion::Resp3);
    let resp3 = execute_command_line(&processor, "ZSCORE myz alpha").unwrap();
    assert_eq!(resp3, b",1.5\r\n");
}

#[test]
fn zincrby_returns_double_type_in_resp3() {
    let processor = RequestProcessor::new().unwrap();
    assert_command_response(&processor, "ZADD myz 1.0 alpha", b":1\r\n");

    // RESP2: ZINCRBY returns bulk string
    let resp2 = execute_command_line(&processor, "ZINCRBY myz 2.5 alpha").unwrap();
    assert_eq!(resp2, b"$3\r\n3.5\r\n");

    // RESP3: ZINCRBY returns double type
    processor.set_resp_protocol_version(RespProtocolVersion::Resp3);
    let resp3 = execute_command_line(&processor, "ZINCRBY myz 1.0 alpha").unwrap();
    assert_eq!(resp3, b",4.5\r\n");
}

#[test]
fn command_info_returns_map_in_resp3_and_array_in_resp2() {
    let processor = RequestProcessor::new().unwrap();

    // RESP2: COMMAND INFO GET returns *1 array with one entry
    let resp2 = execute_command_line(&processor, "COMMAND INFO GET").unwrap();
    assert!(
        resp2.starts_with(b"*1\r\n*3\r\n"),
        "RESP2 COMMAND INFO should return array of entries, got: {:?}",
        String::from_utf8_lossy(&resp2)
    );

    // RESP2: unknown command returns null array placeholder
    let resp2_unknown = execute_command_line(&processor, "COMMAND INFO GET NOTACOMMAND").unwrap();
    assert!(
        resp2_unknown.starts_with(b"*2\r\n"),
        "RESP2 should include placeholders for unknown commands"
    );
    // Should contain null array placeholder for the unknown command
    assert!(
        resp2_unknown.ends_with(b"*-1\r\n"),
        "RESP2 unknown command should be null array placeholder"
    );

    // RESP3: COMMAND INFO GET returns %1 map with one entry
    processor.set_resp_protocol_version(RespProtocolVersion::Resp3);
    let resp3 = execute_command_line(&processor, "COMMAND INFO GET").unwrap();
    assert!(
        resp3.starts_with(b"%1\r\n$3\r\nget\r\n*3\r\n"),
        "RESP3 COMMAND INFO should return map keyed by command name, got: {:?}",
        String::from_utf8_lossy(&resp3)
    );

    // RESP3: unknown commands are omitted from map
    let resp3_mixed = execute_command_line(&processor, "COMMAND INFO GET NOTACOMMAND SET").unwrap();
    assert!(
        resp3_mixed.starts_with(b"%2\r\n"),
        "RESP3 should have map with 2 entries (unknown omitted), got: {:?}",
        String::from_utf8_lossy(&resp3_mixed)
    );

    // RESP3: empty COMMAND INFO returns %0
    let resp3_empty = execute_command_line(&processor, "COMMAND INFO").unwrap();
    assert_eq!(resp3_empty, b"%0\r\n");

    // RESP2: empty COMMAND INFO returns *0
    processor.set_resp_protocol_version(RespProtocolVersion::Resp2);
    let resp2_empty = execute_command_line(&processor, "COMMAND INFO").unwrap();
    assert_eq!(resp2_empty, b"*0\r\n");
}

#[test]
fn zset_external_surface_regressions_match_redis_zset_tcl() {
    let processor = RequestProcessor::new().unwrap();

    assert_command_integer(&processor, "ZADD zset -inf a 1 b 2 c 3 d 4 e 5 f +inf g", 7);
    assert_command_response(
        &processor,
        "ZRANGEBYSCORE zset (-inf (2",
        b"*1\r\n$1\r\nb\r\n",
    );
    assert_command_response(
        &processor,
        "ZRANGEBYSCORE zset (4 (+inf",
        b"*1\r\n$1\r\nf\r\n",
    );
    assert_command_response(
        &processor,
        "ZREVRANGEBYSCORE zset (+inf (4",
        b"*1\r\n$1\r\nf\r\n",
    );
    assert_command_response(&processor, "ZRANGEBYSCORE zset 0 10 LIMIT -1 2", b"*0\r\n");
    assert_command_response(
        &processor,
        "ZREVRANGEBYSCORE zset 10 0 LIMIT -1 2",
        b"*0\r\n",
    );

    assert_command_integer(
        &processor,
        "ZADD zlex 0 alpha 0 bar 0 cool 0 down 0 elephant 0 foo 0 great 0 hill 0 omega",
        9,
    );
    assert_command_response(&processor, "ZRANGEBYLEX zlex - [cool LIMIT -1 2", b"*0\r\n");
    assert_command_response(&processor, "ZREVRANGEBYLEX zlex + [d LIMIT -1 5", b"*0\r\n");

    assert_command_integer(&processor, "ZADD zranktmp 10 x 20 y 30 z", 3);
    assert_command_response(
        &processor,
        "ZRANK zranktmp x WITHSCORE",
        b"*2\r\n:0\r\n$2\r\n10\r\n",
    );
    assert_command_response(
        &processor,
        "ZREVRANK zranktmp z WITHSCORE",
        b"*2\r\n:0\r\n$2\r\n30\r\n",
    );
    assert_command_response(&processor, "ZRANK zranktmp foo WITHSCORE", b"*-1\r\n");
    assert_command_response(&processor, "ZREVRANK zranktmp foo WITHSCORE", b"*-1\r\n");

    assert_command_response(&processor, "SET wrongtype foo", b"+OK\r\n");
    assert_command_error(
        &processor,
        "ZPOPMIN wrongtype -1",
        b"-ERR value is out of range, must be positive\r\n",
    );
    assert_command_error(
        &processor,
        "ZPOPMIN wrongtype 0",
        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
    );
    assert_command_error(
        &processor,
        "ZPOPMAX wrongtype -1",
        b"-ERR value is out of range, must be positive\r\n",
    );
    assert_command_error(
        &processor,
        "ZPOPMAX wrongtype 0",
        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
    );
}

#[test]
fn scan_and_sscan_return_set_type_in_resp3() {
    let processor = RequestProcessor::new().unwrap();
    assert_command_response(&processor, "SET k1 v1", b"+OK\r\n");
    assert_command_response(&processor, "SADD s1 a b c", b":3\r\n");

    // Helper: find the inner data type prefix in a scan response.
    // Scan response format: *2\r\n $N\r\ncursor\r\n [*|~]N\r\n ...
    // We search for the cursor bulk string end (0\r\n) followed by the inner
    // type prefix.
    let inner_type_byte = |resp: &[u8]| -> u8 {
        // Skip outer *2\r\n, then the cursor bulk string ($N\r\nvalue\r\n).
        // The cursor value ends with \r\n, and the next byte is the inner type.
        let cursor_str = b"0\r\n";
        let pos = resp
            .windows(cursor_str.len())
            .rposition(|w| w == cursor_str)
            .expect("cursor 0 not found");
        resp[pos + cursor_str.len()]
    };

    // RESP2: SCAN inner data should use array type (*N)
    let resp2_scan = execute_command_line(&processor, "SCAN 0").unwrap();
    assert_eq!(
        inner_type_byte(&resp2_scan),
        b'*',
        "RESP2 SCAN inner data should use array type"
    );

    // RESP3: SCAN inner data should use set type (~N)
    processor.set_resp_protocol_version(RespProtocolVersion::Resp3);
    let resp3_scan = execute_command_line(&processor, "SCAN 0").unwrap();
    assert_eq!(
        inner_type_byte(&resp3_scan),
        b'~',
        "RESP3 SCAN inner data should use set type"
    );

    // RESP2: SSCAN inner data should use array type (*N)
    processor.set_resp_protocol_version(RespProtocolVersion::Resp2);
    let resp2_sscan = execute_command_line(&processor, "SSCAN s1 0").unwrap();
    assert_eq!(
        inner_type_byte(&resp2_sscan),
        b'*',
        "RESP2 SSCAN inner data should use array type"
    );

    // RESP3: SSCAN inner data should use set type (~N)
    processor.set_resp_protocol_version(RespProtocolVersion::Resp3);
    let resp3_sscan = execute_command_line(&processor, "SSCAN s1 0").unwrap();
    assert_eq!(
        inner_type_byte(&resp3_sscan),
        b'~',
        "RESP3 SSCAN inner data should use set type"
    );
}

#[test]
fn srandmember_and_spop_with_count_return_set_type_in_resp3() {
    let processor = RequestProcessor::new().unwrap();
    assert_command_response(&processor, "SADD s a b c d e", b":5\r\n");

    // RESP2: SRANDMEMBER with positive count returns *N array
    let resp2 = execute_command_line(&processor, "SRANDMEMBER s 2").unwrap();
    assert!(
        resp2.starts_with(b"*2\r\n"),
        "RESP2 SRANDMEMBER +count should use array type"
    );

    // RESP3: SRANDMEMBER with positive count returns ~N set type
    processor.set_resp_protocol_version(RespProtocolVersion::Resp3);
    let resp3 = execute_command_line(&processor, "SRANDMEMBER s 2").unwrap();
    assert!(
        resp3.starts_with(b"~2\r\n"),
        "RESP3 SRANDMEMBER +count should use set type, got: {:?}",
        String::from_utf8_lossy(&resp3)
    );

    // RESP3: SRANDMEMBER with negative count stays *N (may have duplicates)
    let resp3_neg = execute_command_line(&processor, "SRANDMEMBER s -3").unwrap();
    assert!(
        resp3_neg.starts_with(b"*3\r\n"),
        "RESP3 SRANDMEMBER -count should use array type (duplicates possible), got: {:?}",
        String::from_utf8_lossy(&resp3_neg)
    );

    // RESP2: SPOP with count returns *N array
    processor.set_resp_protocol_version(RespProtocolVersion::Resp2);
    assert_command_response(&processor, "SADD s2 x y z", b":3\r\n");
    let resp2_spop = execute_command_line(&processor, "SPOP s2 2").unwrap();
    assert!(
        resp2_spop.starts_with(b"*2\r\n"),
        "RESP2 SPOP with count should use array type"
    );

    // RESP3: SPOP with count returns ~N set type
    processor.set_resp_protocol_version(RespProtocolVersion::Resp3);
    assert_command_response(&processor, "SADD s3 p q r", b":3\r\n");
    let resp3_spop = execute_command_line(&processor, "SPOP s3 2").unwrap();
    assert!(
        resp3_spop.starts_with(b"~2\r\n"),
        "RESP3 SPOP with count should use set type, got: {:?}",
        String::from_utf8_lossy(&resp3_spop)
    );
}

#[test]
fn srandmember_rejects_i64_min_count_and_keeps_server_healthy() {
    let processor = RequestProcessor::new().unwrap();
    assert_command_response(&processor, "SADD s a b c", b":3\r\n");
    assert_command_error(
        &processor,
        "SRANDMEMBER s -9223372036854775808",
        b"-ERR value is out of range\r\n",
    );

    // Ensure the request processor continues serving subsequent commands.
    assert_command_response(&processor, "SCARD s", b":3\r\n");
    let after = execute_command_line(&processor, "SRANDMEMBER s -2").unwrap();
    assert!(
        after.starts_with(b"*2\r\n"),
        "SRANDMEMBER after out-of-range error should still execute normally"
    );
}

#[test]
fn info_and_cluster_info_return_verbatim_string_in_resp3() {
    let processor = RequestProcessor::new().unwrap();

    // RESP2: INFO returns bulk string ($N)
    let resp2_info = execute_command_line(&processor, "INFO server").unwrap();
    assert!(
        resp2_info.starts_with(b"$"),
        "RESP2 INFO should return bulk string"
    );

    // RESP3: INFO returns verbatim string (=N\r\ntxt:...)
    processor.set_resp_protocol_version(RespProtocolVersion::Resp3);
    let resp3_info = execute_command_line(&processor, "INFO server").unwrap();
    assert!(
        resp3_info.starts_with(b"="),
        "RESP3 INFO should return verbatim string, got: {:?}",
        String::from_utf8_lossy(&resp3_info[..resp3_info.len().min(40)])
    );
    // Verify format prefix is "txt:"
    let after_crlf_pos = resp3_info.windows(2).position(|w| w == b"\r\n").unwrap() + 2;
    assert!(
        resp3_info[after_crlf_pos..].starts_with(b"txt:"),
        "RESP3 verbatim string should use txt format"
    );

    // RESP2: CLUSTER INFO returns bulk string
    processor.set_resp_protocol_version(RespProtocolVersion::Resp2);
    let resp2_cluster = execute_command_line(&processor, "CLUSTER INFO").unwrap();
    assert!(
        resp2_cluster.starts_with(b"$"),
        "RESP2 CLUSTER INFO should return bulk string"
    );

    // RESP3: CLUSTER INFO returns verbatim string
    processor.set_resp_protocol_version(RespProtocolVersion::Resp3);
    let resp3_cluster = execute_command_line(&processor, "CLUSTER INFO").unwrap();
    assert!(
        resp3_cluster.starts_with(b"="),
        "RESP3 CLUSTER INFO should return verbatim string"
    );
}

#[test]
fn client_info_kill_caching_reply_and_config_rewrite_stubs() {
    let processor = RequestProcessor::new().unwrap();

    // CLIENT INFO returns bulk string with id= field
    let info = execute_command_line(&processor, "CLIENT INFO").unwrap();
    assert!(
        info.starts_with(b"$"),
        "CLIENT INFO should return bulk string"
    );
    assert!(
        info.windows(3).any(|w| w == b"id="),
        "CLIENT INFO should contain id= field"
    );

    // CLIENT KILL returns integer 0
    let kill = execute_command_line(&processor, "CLIENT KILL 127.0.0.1:1234").unwrap();
    assert_eq!(kill, b":0\r\n");

    // CLIENT CACHING YES returns OK
    let caching = execute_command_line(&processor, "CLIENT CACHING YES").unwrap();
    assert_eq!(caching, b"+OK\r\n");

    // CLIENT CACHING NO returns OK
    let caching_no = execute_command_line(&processor, "CLIENT CACHING NO").unwrap();
    assert_eq!(caching_no, b"+OK\r\n");

    // CLIENT REPLY ON returns OK and sets mode
    let reply = execute_command_line(&processor, "CLIENT REPLY ON").unwrap();
    assert_eq!(reply, b"+OK\r\n");
    assert_eq!(processor.client_reply_mode(), 0, "ON should be mode 0");

    // CLIENT REPLY OFF returns OK and sets mode
    let reply_off = execute_command_line(&processor, "CLIENT REPLY OFF").unwrap();
    assert_eq!(reply_off, b"+OK\r\n");
    assert_eq!(processor.client_reply_mode(), 1, "OFF should be mode 1");

    // CLIENT REPLY SKIP returns OK and sets mode
    let reply_skip = execute_command_line(&processor, "CLIENT REPLY SKIP").unwrap();
    assert_eq!(reply_skip, b"+OK\r\n");
    assert_eq!(processor.client_reply_mode(), 2, "SKIP should be mode 2");

    // clear_client_reply_skip resets SKIP back to ON
    processor.clear_client_reply_skip();
    assert_eq!(
        processor.client_reply_mode(),
        0,
        "SKIP should be cleared to ON"
    );

    // clear_client_reply_skip is a no-op when mode is OFF
    let _ = execute_command_line(&processor, "CLIENT REPLY OFF").unwrap();
    processor.clear_client_reply_skip();
    assert_eq!(
        processor.client_reply_mode(),
        1,
        "OFF should remain OFF after clear_skip"
    );

    // Restore to ON for remaining tests
    let _ = execute_command_line(&processor, "CLIENT REPLY ON").unwrap();

    // CLIENT TRACKING ON returns OK
    let tracking_on = execute_command_line(&processor, "CLIENT TRACKING ON").unwrap();
    assert_eq!(tracking_on, b"+OK\r\n");

    // CLIENT TRACKING OFF returns OK
    let tracking_off = execute_command_line(&processor, "CLIENT TRACKING OFF").unwrap();
    assert_eq!(tracking_off, b"+OK\r\n");

    // CLIENT TRACKING ON with BCAST prefix returns OK
    let tracking_bcast = execute_frame(
        &processor,
        &encode_resp(&[b"CLIENT", b"TRACKING", b"ON", b"BCAST", b"PREFIX", b"foo"]),
    );
    assert_eq!(tracking_bcast, b"+OK\r\n");

    // CLIENT TRACKINGINFO returns tracking info
    let trackinginfo = execute_command_line(&processor, "CLIENT TRACKINGINFO").unwrap();
    assert!(
        trackinginfo.windows(3).any(|w| w == b"off"),
        "CLIENT TRACKINGINFO should include 'off' flag: {}",
        String::from_utf8_lossy(&trackinginfo)
    );

    // CLIENT GETREDIR returns -1 (no redirect)
    let getredir = execute_command_line(&processor, "CLIENT GETREDIR").unwrap();
    assert_eq!(getredir, b":-1\r\n");

    // CONFIG REWRITE returns proper error
    let rewrite = execute_command_line(&processor, "CONFIG REWRITE").unwrap();
    assert!(
        rewrite.starts_with(b"-ERR"),
        "CONFIG REWRITE should return error"
    );
    assert!(
        rewrite.windows(11).any(|w| w == b"config file"),
        "CONFIG REWRITE should mention config file"
    );

    // COMMAND DOCS currently returns an error so redis-cli can fall back to
    // bundled command metadata instead of trusting an empty docs table.
    assert_command_error(&processor, "COMMAND DOCS", b"-ERR unknown subcommand\r\n");

    // RESP3: CLIENT INFO returns verbatim string
    processor.set_resp_protocol_version(RespProtocolVersion::Resp3);
    let info_resp3 = execute_command_line(&processor, "CLIENT INFO").unwrap();
    assert!(
        info_resp3.starts_with(b"="),
        "RESP3 CLIENT INFO should return verbatim string"
    );

    // RESP3 keeps the same error shape.
    assert_command_error(&processor, "COMMAND DOCS", b"-ERR unknown subcommand\r\n");
}

#[test]
fn acl_deluser_genpass_log_save_load_stubs() {
    let processor = RequestProcessor::new().unwrap();

    // ACL DELUSER of unknown user returns integer 0
    let del_unknown = execute_command_line(&processor, "ACL DELUSER nonexistent").unwrap();
    assert_eq!(del_unknown, b":0\r\n");

    // ACL DELUSER of multiple unknown users returns integer 0
    let del_multi = execute_command_line(&processor, "ACL DELUSER userA userB userC").unwrap();
    assert_eq!(del_multi, b":0\r\n");

    // ACL DELUSER of "default" returns error
    let del_default = execute_command_line(&processor, "ACL DELUSER default").unwrap();
    assert!(
        del_default.starts_with(b"-ERR"),
        "ACL DELUSER default should return error"
    );
    assert!(
        del_default.windows(7).any(|w| w == b"default"),
        "error should mention 'default'"
    );

    // ACL DELUSER with no username is arity error
    let del_no_arg = execute_command_line(&processor, "ACL DELUSER");
    assert!(del_no_arg.is_err(), "ACL DELUSER without args should fail");

    // ACL GENPASS returns 64-char hex string (256 bits default)
    let genpass = execute_command_line(&processor, "ACL GENPASS").unwrap();
    assert!(
        genpass.starts_with(b"$64\r\n"),
        "ACL GENPASS should return 64-byte bulk string"
    );
    // Verify it's valid hex
    let hex_part = &genpass[5..5 + 64];
    assert!(
        hex_part.iter().all(|b| b.is_ascii_hexdigit()),
        "ACL GENPASS output should be valid hex"
    );

    // ACL GENPASS with valid bits (128 bits = 32 hex chars)
    let genpass_128 = execute_command_line(&processor, "ACL GENPASS 128").unwrap();
    assert!(
        genpass_128.starts_with(b"$32\r\n"),
        "ACL GENPASS 128 should return 32-byte bulk string"
    );

    // ACL GENPASS with bits=0 returns error
    let genpass_0 = execute_command_line(&processor, "ACL GENPASS 0").unwrap();
    assert!(
        genpass_0.starts_with(b"-ERR"),
        "ACL GENPASS 0 should return error"
    );

    // ACL GENPASS with bits > 4096 returns error
    let genpass_big = execute_command_line(&processor, "ACL GENPASS 5000").unwrap();
    assert!(
        genpass_big.starts_with(b"-ERR"),
        "ACL GENPASS 5000 should return error"
    );

    // ACL GENPASS with non-integer returns error
    let genpass_nan = execute_command_line(&processor, "ACL GENPASS abc");
    assert!(genpass_nan.is_err(), "ACL GENPASS abc should fail");

    // ACL LOG returns empty array
    let log_empty = execute_command_line(&processor, "ACL LOG").unwrap();
    assert_eq!(log_empty, b"*0\r\n");

    // ACL LOG with count returns empty array
    let log_count = execute_command_line(&processor, "ACL LOG 10").unwrap();
    assert_eq!(log_count, b"*0\r\n");

    // ACL LOG RESET returns OK
    let log_reset = execute_command_line(&processor, "ACL LOG RESET").unwrap();
    assert_eq!(log_reset, b"+OK\r\n");

    // ACL LOG with non-integer count returns error
    let log_nan = execute_command_line(&processor, "ACL LOG abc");
    assert!(log_nan.is_err(), "ACL LOG abc should fail");

    // ACL SAVE returns OK
    let save = execute_command_line(&processor, "ACL SAVE").unwrap();
    assert_eq!(save, b"+OK\r\n");

    // ACL LOAD returns OK
    let load = execute_command_line(&processor, "ACL LOAD").unwrap();
    assert_eq!(load, b"+OK\r\n");
}

#[test]
fn memory_doctor_and_stats_stubs() {
    let processor = RequestProcessor::new().unwrap();

    // MEMORY DOCTOR returns bulk string
    let doctor = execute_command_line(&processor, "MEMORY DOCTOR").unwrap();
    assert!(
        doctor.starts_with(b"$"),
        "MEMORY DOCTOR should return bulk string"
    );
    assert!(
        doctor.windows(10).any(|w| w == b"no memory "),
        "MEMORY DOCTOR should mention 'no memory'"
    );

    // MEMORY STATS returns 8-element array in RESP2 (4 field-value pairs)
    let stats = execute_command_line(&processor, "MEMORY STATS").unwrap();
    assert!(
        stats.starts_with(b"*8\r\n"),
        "MEMORY STATS should return 8-element array in RESP2"
    );
    assert!(
        stats.windows(14).any(|w| w == b"peak.allocated"),
        "MEMORY STATS should contain peak.allocated"
    );
    assert!(
        stats.windows(15).any(|w| w == b"total.allocated"),
        "MEMORY STATS should contain total.allocated"
    );

    // RESP3: MEMORY STATS returns map
    processor.set_resp_protocol_version(RespProtocolVersion::Resp3);
    let stats_resp3 = execute_command_line(&processor, "MEMORY STATS").unwrap();
    assert!(
        stats_resp3.starts_with(b"%4\r\n"),
        "RESP3 MEMORY STATS should return map with 4 entries"
    );
}

#[test]
fn client_id_getname_setname_list_noevict_notouch_help_stubs() {
    let processor = RequestProcessor::new().unwrap();

    // CLIENT ID returns integer
    let id = execute_command_line(&processor, "CLIENT ID").unwrap();
    assert!(id.starts_with(b":"), "CLIENT ID should return integer");

    // CLIENT GETNAME returns null bulk string in RESP2
    let getname = execute_command_line(&processor, "CLIENT GETNAME").unwrap();
    assert_eq!(getname, b"$-1\r\n", "CLIENT GETNAME should return null");

    // CLIENT SETNAME returns OK and stores the name
    let setname = execute_command_line(&processor, "CLIENT SETNAME myconn").unwrap();
    assert_eq!(setname, b"+OK\r\n");

    // CLIENT GETNAME returns the stored name after SETNAME
    let getname_after = execute_command_line(&processor, "CLIENT GETNAME").unwrap();
    assert_eq!(getname_after, b"$6\r\nmyconn\r\n");

    // CLIENT SETNAME rejects names with spaces (use frame to send space in single arg)
    let setname_space = execute_frame(
        &processor,
        &encode_resp(&[b"CLIENT", b"SETNAME", b"bad name"]),
    );
    assert!(
        setname_space.starts_with(b"-ERR Client names cannot contain"),
        "CLIENT SETNAME should reject names with spaces: {}",
        String::from_utf8_lossy(&setname_space)
    );

    // CLIENT SETINFO LIB-NAME stores the library name
    let setinfo_name = execute_frame(
        &processor,
        &encode_resp(&[b"CLIENT", b"SETINFO", b"LIB-NAME", b"redis-py"]),
    );
    assert_eq!(setinfo_name, b"+OK\r\n");

    // CLIENT SETINFO LIB-VER stores the library version
    let setinfo_ver = execute_frame(
        &processor,
        &encode_resp(&[b"CLIENT", b"SETINFO", b"LIB-VER", b"5.0.1"]),
    );
    assert_eq!(setinfo_ver, b"+OK\r\n");

    // CLIENT SETINFO LIB-NAME rejects names with spaces
    let setinfo_space = execute_frame(
        &processor,
        &encode_resp(&[b"CLIENT", b"SETINFO", b"LIB-NAME", b"bad name"]),
    );
    assert!(
        setinfo_space.starts_with(b"-ERR"),
        "CLIENT SETINFO LIB-NAME should reject spaces"
    );

    // CLIENT SETINFO with unknown option returns error
    let setinfo_bad = execute_frame(
        &processor,
        &encode_resp(&[b"CLIENT", b"SETINFO", b"UNKNOWN", b"val"]),
    );
    assert!(
        setinfo_bad.starts_with(b"-ERR"),
        "CLIENT SETINFO with unknown option should error"
    );

    // CLIENT LIST should include the stored name and lib info in output
    let list_with_name = execute_command_line(&processor, "CLIENT LIST").unwrap();
    let list_text = String::from_utf8_lossy(&list_with_name);
    assert!(
        list_text.contains("name=myconn"),
        "CLIENT LIST should include stored name: {list_text}"
    );
    assert!(
        list_text.contains("lib-name=redis-py"),
        "CLIENT LIST should include lib-name: {list_text}"
    );
    assert!(
        list_text.contains("lib-ver=5.0.1"),
        "CLIENT LIST should include lib-ver: {list_text}"
    );

    // CLIENT SETNAME with empty string clears the name
    let setname_empty = execute_frame(&processor, &encode_resp(&[b"CLIENT", b"SETNAME", b""]));
    assert_eq!(setname_empty, b"+OK\r\n");
    let getname_cleared = execute_command_line(&processor, "CLIENT GETNAME").unwrap();
    assert_eq!(
        getname_cleared, b"$-1\r\n",
        "Empty SETNAME should return null GETNAME"
    );

    // CLIENT LIST returns bulk string in RESP2
    let list = execute_command_line(&processor, "CLIENT LIST").unwrap();
    assert!(
        list.starts_with(b"$"),
        "CLIENT LIST should return bulk string"
    );

    // CLIENT UNBLOCK returns integer 0
    let unblock = execute_command_line(&processor, "CLIENT UNBLOCK 123").unwrap();
    assert_eq!(unblock, b":0\r\n");

    // CLIENT UNBLOCK with TIMEOUT flag
    let unblock_timeout = execute_command_line(&processor, "CLIENT UNBLOCK 123 TIMEOUT").unwrap();
    assert_eq!(unblock_timeout, b":0\r\n");

    // CLIENT PAUSE returns OK
    let pause = execute_command_line(&processor, "CLIENT PAUSE 100").unwrap();
    assert_eq!(pause, b"+OK\r\n");

    // CLIENT PAUSE with WRITE mode
    let pause_write = execute_command_line(&processor, "CLIENT PAUSE 100 WRITE").unwrap();
    assert_eq!(pause_write, b"+OK\r\n");

    // CLIENT PAUSE with ALL mode
    let pause_all = execute_command_line(&processor, "CLIENT PAUSE 100 ALL").unwrap();
    assert_eq!(pause_all, b"+OK\r\n");

    // CLIENT UNPAUSE returns OK
    let unpause = execute_command_line(&processor, "CLIENT UNPAUSE").unwrap();
    assert_eq!(unpause, b"+OK\r\n");

    // CLIENT NO-EVICT ON returns OK
    let noevict_on = execute_command_line(&processor, "CLIENT NO-EVICT ON").unwrap();
    assert_eq!(noevict_on, b"+OK\r\n");

    // CLIENT NO-EVICT OFF returns OK
    let noevict_off = execute_command_line(&processor, "CLIENT NO-EVICT OFF").unwrap();
    assert_eq!(noevict_off, b"+OK\r\n");

    // CLIENT NO-TOUCH ON returns OK
    let notouch_on = execute_command_line(&processor, "CLIENT NO-TOUCH ON").unwrap();
    assert_eq!(notouch_on, b"+OK\r\n");

    // CLIENT NO-TOUCH OFF returns OK
    let notouch_off = execute_command_line(&processor, "CLIENT NO-TOUCH OFF").unwrap();
    assert_eq!(notouch_off, b"+OK\r\n");

    // CLIENT HELP returns bulk array
    let help = execute_command_line(&processor, "CLIENT HELP").unwrap();
    assert!(
        help.starts_with(b"*35\r\n"),
        "CLIENT HELP should return 35-element array"
    );

    // RESP3: CLIENT GETNAME returns _\r\n (null)
    processor.set_resp_protocol_version(RespProtocolVersion::Resp3);
    let getname_resp3 = execute_command_line(&processor, "CLIENT GETNAME").unwrap();
    assert_eq!(
        getname_resp3, b"_\r\n",
        "RESP3 CLIENT GETNAME should return null"
    );

    // RESP3: CLIENT LIST returns verbatim string
    let list_resp3 = execute_command_line(&processor, "CLIENT LIST").unwrap();
    assert!(
        list_resp3.starts_with(b"="),
        "RESP3 CLIENT LIST should return verbatim string"
    );
}

#[test]
fn client_list_type_and_id_filters() {
    let processor = RequestProcessor::new().unwrap();

    // CLIENT LIST TYPE NORMAL returns bulk string with client info fields
    let list_normal = execute_command_line(&processor, "CLIENT LIST TYPE NORMAL").unwrap();
    assert!(
        list_normal.starts_with(b"$"),
        "CLIENT LIST TYPE NORMAL should return bulk string"
    );
    assert!(
        list_normal.windows(3).any(|w| w == b"id="),
        "CLIENT LIST output should contain id= field"
    );

    // CLIENT LIST TYPE REPLICA also accepted
    let list_replica = execute_command_line(&processor, "CLIENT LIST TYPE REPLICA").unwrap();
    assert!(list_replica.starts_with(b"$"));

    // CLIENT LIST TYPE with invalid type returns error
    let list_bad = execute_command_line(&processor, "CLIENT LIST TYPE INVALID");
    assert!(list_bad.is_err(), "Invalid CLIENT LIST TYPE should error");

    // CLIENT LIST ID with valid ID
    let list_id = execute_command_line(&processor, "CLIENT LIST ID 1").unwrap();
    assert!(list_id.starts_with(b"$"));

    // CLIENT LIST ID with non-integer returns error
    let list_bad_id = execute_command_line(&processor, "CLIENT LIST ID notanum");
    assert!(
        list_bad_id.is_err(),
        "CLIENT LIST ID non-integer should error"
    );

    // CLIENT LIST with unknown filter returns error
    let list_bad_filter = execute_command_line(&processor, "CLIENT LIST BADFILTER value");
    assert!(
        list_bad_filter.is_err(),
        "CLIENT LIST unknown filter should error"
    );
}

#[test]
fn acl_dryrun_and_cluster_saveconfig_countkeysinslot_getkeysinslot() {
    let processor = RequestProcessor::new().unwrap();

    // ACL DRYRUN returns OK for any user/command
    let dryrun = execute_command_line(&processor, "ACL DRYRUN default GET key").unwrap();
    assert_eq!(dryrun, b"+OK\r\n");

    // ACL DRYRUN requires at least username + command
    let dryrun_short = execute_command_line(&processor, "ACL DRYRUN default");
    assert!(dryrun_short.is_err(), "ACL DRYRUN needs username + command");

    // ACL HELP reflects new count
    let help = execute_command_line(&processor, "ACL HELP").unwrap();
    assert!(
        help.starts_with(b"*27\r\n"),
        "ACL HELP should return 27-element array"
    );

    // CLUSTER SAVECONFIG returns OK
    let saveconfig = execute_command_line(&processor, "CLUSTER SAVECONFIG").unwrap();
    assert_eq!(saveconfig, b"+OK\r\n");

    // CLUSTER COUNTKEYSINSLOT returns 0 for valid slot
    let count_0 = execute_command_line(&processor, "CLUSTER COUNTKEYSINSLOT 0").unwrap();
    assert_eq!(count_0, b":0\r\n");

    let count_max = execute_command_line(&processor, "CLUSTER COUNTKEYSINSLOT 16383").unwrap();
    assert_eq!(count_max, b":0\r\n");

    // CLUSTER COUNTKEYSINSLOT out of range returns error
    let count_bad = execute_command_line(&processor, "CLUSTER COUNTKEYSINSLOT 16384").unwrap();
    assert!(
        count_bad.starts_with(b"-ERR"),
        "CLUSTER COUNTKEYSINSLOT 16384 should return error"
    );

    // CLUSTER COUNTKEYSINSLOT non-integer returns error
    let count_nan = execute_command_line(&processor, "CLUSTER COUNTKEYSINSLOT abc");
    assert!(
        count_nan.is_err(),
        "CLUSTER COUNTKEYSINSLOT abc should fail"
    );

    // COUNT/GETKEYSINSLOT should reflect actual keys in the requested slot.
    let slot_key_a = "{slot-a}alpha";
    let slot_key_b = "{slot-a}beta";
    execute_command_line(&processor, &format!("SET {slot_key_a} v")).unwrap();
    execute_command_line(&processor, &format!("HSET {slot_key_b} field 1")).unwrap();
    let populated_slot = u16::from(redis_hash_slot(slot_key_a.as_bytes()));
    let populated_count = execute_command_line(
        &processor,
        &format!("CLUSTER COUNTKEYSINSLOT {populated_slot}"),
    )
    .unwrap();
    assert_eq!(populated_count, b":2\r\n");

    let populated_keys = execute_command_line(
        &processor,
        &format!("CLUSTER GETKEYSINSLOT {populated_slot} 10"),
    )
    .unwrap();
    let populated_keys_text = String::from_utf8_lossy(&populated_keys);
    assert!(
        populated_keys_text.contains(slot_key_a) && populated_keys_text.contains(slot_key_b),
        "GETKEYSINSLOT should include both slot keys: {}",
        populated_keys_text
    );

    let populated_keys_limited = execute_command_line(
        &processor,
        &format!("CLUSTER GETKEYSINSLOT {populated_slot} 1"),
    )
    .unwrap();
    assert!(
        populated_keys_limited.starts_with(b"*1\r\n"),
        "GETKEYSINSLOT count limit should cap returned keys"
    );

    // CLUSTER GETKEYSINSLOT returns empty array for valid slot
    let keys_0 = execute_command_line(&processor, "CLUSTER GETKEYSINSLOT 0 10").unwrap();
    assert_eq!(keys_0, b"*0\r\n");

    // CLUSTER GETKEYSINSLOT out of range returns error
    let keys_bad = execute_command_line(&processor, "CLUSTER GETKEYSINSLOT 20000 10").unwrap();
    assert!(
        keys_bad.starts_with(b"-ERR"),
        "CLUSTER GETKEYSINSLOT 20000 should return error"
    );

    // CLUSTER HELP reflects new count
    let cluster_help = execute_command_line(&processor, "CLUSTER HELP").unwrap();
    assert!(
        cluster_help.starts_with(b"*24\r\n"),
        "CLUSTER HELP should return 24-element array"
    );
}

#[test]
fn set_get_option_returns_old_value() {
    let processor = RequestProcessor::new().unwrap();

    // SET GET on nonexistent key → returns null, sets value
    let r1 = execute_command_line(&processor, "SET mykey hello GET").unwrap();
    assert_eq!(
        r1, b"$-1\r\n",
        "SET GET on missing key should return null bulk string"
    );

    // Verify the key was set
    let get1 = execute_command_line(&processor, "GET mykey").unwrap();
    assert_eq!(get1, b"$5\r\nhello\r\n");

    // SET GET on existing key → returns old value, overwrites
    let r2 = execute_command_line(&processor, "SET mykey world GET").unwrap();
    assert_eq!(
        r2, b"$5\r\nhello\r\n",
        "SET GET on existing key should return old value"
    );

    let get2 = execute_command_line(&processor, "GET mykey").unwrap();
    assert_eq!(get2, b"$5\r\nworld\r\n");

    // SET NX GET: key exists → NX fails, returns old value
    let r3 = execute_command_line(&processor, "SET mykey newval NX GET").unwrap();
    assert_eq!(
        r3, b"$5\r\nworld\r\n",
        "SET NX GET on existing key should return old value without setting"
    );

    let get3 = execute_command_line(&processor, "GET mykey").unwrap();
    assert_eq!(
        get3, b"$5\r\nworld\r\n",
        "Value should not have changed with NX"
    );

    // SET NX GET: key does not exist → NX succeeds, returns null
    let r4 = execute_command_line(&processor, "SET nxkey val1 NX GET").unwrap();
    assert_eq!(
        r4, b"$-1\r\n",
        "SET NX GET on missing key should return null"
    );

    let get4 = execute_command_line(&processor, "GET nxkey").unwrap();
    assert_eq!(get4, b"$4\r\nval1\r\n");

    // SET XX GET: key exists → XX succeeds, returns old value
    let r5 = execute_command_line(&processor, "SET mykey replaced XX GET").unwrap();
    assert_eq!(
        r5, b"$5\r\nworld\r\n",
        "SET XX GET on existing key should return old value"
    );

    let get5 = execute_command_line(&processor, "GET mykey").unwrap();
    assert_eq!(get5, b"$8\r\nreplaced\r\n");

    // SET XX GET: key does not exist → XX fails, returns null
    let r6 = execute_command_line(&processor, "SET nokey val2 XX GET").unwrap();
    assert_eq!(
        r6, b"$-1\r\n",
        "SET XX GET on missing key should return null"
    );

    // SET GET with expiration
    let r7 = execute_command_line(&processor, "SET mykey withttl GET EX 100").unwrap();
    assert_eq!(
        r7, b"$8\r\nreplaced\r\n",
        "SET GET with EX should return old value"
    );

    // SET GET with KEEPTTL
    let r8 = execute_command_line(&processor, "SET mykey keepit GET KEEPTTL").unwrap();
    assert_eq!(
        r8, b"$7\r\nwithttl\r\n",
        "SET GET with KEEPTTL should return old value"
    );

    // SET GET on non-string type → WRONGTYPE
    execute_command_line(&processor, "RPUSH mylist a b c").unwrap();
    let r9 = execute_command_line(&processor, "SET mylist val GET");
    assert!(
        r9.is_err() || {
            let resp = r9.unwrap();
            resp.starts_with(b"-WRONGTYPE")
        },
        "SET GET on list key should return WRONGTYPE error"
    );

    // Duplicate GET → syntax error
    let r10 = execute_command_line(&processor, "SET k v GET GET");
    assert!(
        r10.is_err() || {
            let resp = r10.unwrap();
            resp.starts_with(b"-ERR")
        },
        "SET with duplicate GET should return syntax error"
    );
}

#[test]
fn module_load_loadex_unload_stubs() {
    let processor = RequestProcessor::new().unwrap();

    // MODULE LIST → empty array
    let list = execute_command_line(&processor, "MODULE LIST").unwrap();
    assert_eq!(list, b"*0\r\n");

    // MODULE LOAD → error (not supported)
    let load = execute_command_line(&processor, "MODULE LOAD /path/to/mod.so").unwrap();
    assert!(load.starts_with(b"-ERR"), "MODULE LOAD should return error");

    // MODULE LOADEX → error (not supported)
    let loadex = execute_command_line(&processor, "MODULE LOADEX /path/to/mod.so").unwrap();
    assert!(
        loadex.starts_with(b"-ERR"),
        "MODULE LOADEX should return error"
    );

    // MODULE UNLOAD → error (no such module)
    let unload = execute_command_line(&processor, "MODULE UNLOAD mymod").unwrap();
    assert!(
        unload.starts_with(b"-ERR"),
        "MODULE UNLOAD should return error about no such module"
    );

    // MODULE LOAD arity check
    let load_noargs = execute_command_line(&processor, "MODULE LOAD");
    assert!(
        load_noargs.is_err(),
        "MODULE LOAD without path should be arity error"
    );

    // MODULE UNLOAD arity check
    let unload_noargs = execute_command_line(&processor, "MODULE UNLOAD");
    assert!(
        unload_noargs.is_err(),
        "MODULE UNLOAD without name should be arity error"
    );

    // MODULE unknown subcommand → UnknownSubcommand
    let unknown = execute_command_line(&processor, "MODULE NOPE");
    assert!(unknown.is_err(), "MODULE unknown subcommand should error");

    // MODULE HELP → 11-element array
    let help = execute_command_line(&processor, "MODULE HELP").unwrap();
    assert!(
        help.starts_with(b"*11\r\n"),
        "MODULE HELP should return 11-element array"
    );
}

#[test]
fn cluster_reset_and_shards_stubs() {
    let processor = RequestProcessor::new().unwrap();

    // CLUSTER RESET (no mode) → OK
    let reset = execute_command_line(&processor, "CLUSTER RESET").unwrap();
    assert_eq!(reset, b"+OK\r\n");

    // CLUSTER RESET SOFT → OK
    let reset_soft = execute_command_line(&processor, "CLUSTER RESET SOFT").unwrap();
    assert_eq!(reset_soft, b"+OK\r\n");

    // CLUSTER RESET HARD → OK
    let reset_hard = execute_command_line(&processor, "CLUSTER RESET HARD").unwrap();
    assert_eq!(reset_hard, b"+OK\r\n");

    // CLUSTER RESET INVALID → syntax error
    let reset_bad = execute_command_line(&processor, "CLUSTER RESET INVALID");
    assert!(
        reset_bad.is_err(),
        "CLUSTER RESET with invalid mode should error"
    );

    // CLUSTER SHARDS → returns shard topology
    let shards = execute_command_line(&processor, "CLUSTER SHARDS").unwrap();
    assert!(
        shards.starts_with(b"*1\r\n"),
        "CLUSTER SHARDS should return 1-element array (single shard)"
    );
    // Verify the response contains expected fields
    assert!(
        shards.windows(b"slots".len()).any(|w| w == b"slots"),
        "CLUSTER SHARDS should contain 'slots' field"
    );
    assert!(
        shards.windows(b"nodes".len()).any(|w| w == b"nodes"),
        "CLUSTER SHARDS should contain 'nodes' field"
    );
    assert!(
        shards.windows(b"master".len()).any(|w| w == b"master"),
        "CLUSTER SHARDS should contain 'master' role"
    );

    // CLUSTER HELP reflects updated count
    let help = execute_command_line(&processor, "CLUSTER HELP").unwrap();
    assert!(
        help.starts_with(b"*24\r\n"),
        "CLUSTER HELP should return 24-element array"
    );
}

#[test]
fn acl_cat_returns_full_category_list_and_getuser_returns_default_profile() {
    let processor = RequestProcessor::new().unwrap();

    // ACL CAT returns all 21 categories
    let cat = execute_command_line(&processor, "ACL CAT").unwrap();
    assert!(
        cat.starts_with(b"*21\r\n"),
        "ACL CAT should return 21-element array, got: {:?}",
        &cat[..cat.len().min(20)]
    );
    // Verify specific categories are present
    let cat_str = std::str::from_utf8(&cat).unwrap();
    for expected in &[
        "keyspace",
        "read",
        "write",
        "sortedset",
        "bitmap",
        "hyperloglog",
        "geo",
        "pubsub",
        "admin",
        "fast",
        "slow",
        "blocking",
        "dangerous",
        "connection",
        "transaction",
        "scripting",
    ] {
        assert!(
            cat_str.contains(expected),
            "ACL CAT should contain '{expected}'"
        );
    }

    // ACL CAT with category returns empty array (stub)
    let cat_read = execute_command_line(&processor, "ACL CAT read").unwrap();
    assert_eq!(cat_read, b"*0\r\n");

    // ACL GETUSER default returns 12-element array in RESP2
    let getuser = execute_command_line(&processor, "ACL GETUSER default").unwrap();
    assert!(
        getuser.starts_with(b"*12\r\n"),
        "ACL GETUSER default should return 12-element array in RESP2, got: {:?}",
        std::str::from_utf8(&getuser[..getuser.len().min(60)]).unwrap_or("(non-utf8)")
    );
    let getuser_str = std::str::from_utf8(&getuser).unwrap();
    assert!(
        getuser_str.contains("flags"),
        "ACL GETUSER should contain 'flags'"
    );
    assert!(
        getuser_str.contains("nopass"),
        "ACL GETUSER should contain 'nopass'"
    );
    assert!(
        getuser_str.contains("+@all"),
        "ACL GETUSER should contain '+@all'"
    );

    // ACL GETUSER nonexistent returns null
    let getuser_none = execute_command_line(&processor, "ACL GETUSER unknown").unwrap();
    assert_eq!(getuser_none, b"$-1\r\n");

    // ACL GETUSER in RESP3 returns map
    processor.set_resp_protocol_version(RespProtocolVersion::Resp3);
    let getuser_resp3 = execute_command_line(&processor, "ACL GETUSER default").unwrap();
    assert!(
        getuser_resp3.starts_with(b"%6\r\n"),
        "ACL GETUSER in RESP3 should return 6-entry map"
    );

    // ACL GETUSER nonexistent in RESP3 returns _\r\n
    let getuser_none_resp3 = execute_command_line(&processor, "ACL GETUSER nobody").unwrap();
    assert_eq!(getuser_none_resp3, b"_\r\n");

    // ACL unknown subcommand returns error
    processor.set_resp_protocol_version(RespProtocolVersion::Resp2);
    let unknown = execute_command_line(&processor, "ACL BADCMD");
    assert!(unknown.is_err(), "ACL BADCMD should return error");
}

#[test]
fn xgroup_createconsumer_delconsumer_and_help() {
    let processor = RequestProcessor::new().unwrap();

    // Set up a stream with a group
    execute_command_line(&processor, "XADD mystream * field1 value1").unwrap();
    execute_command_line(&processor, "XGROUP CREATE mystream mygroup 0").unwrap();

    // XGROUP CREATECONSUMER on existing stream/group returns 1
    let create_consumer = execute_command_line(
        &processor,
        "XGROUP CREATECONSUMER mystream mygroup consumer1",
    )
    .unwrap();
    assert_eq!(create_consumer, b":1\r\n");

    // XGROUP CREATECONSUMER on missing stream returns error (NoSuchKey)
    let create_missing = execute_command_line(
        &processor,
        "XGROUP CREATECONSUMER nostream mygroup consumer1",
    );
    assert!(
        create_missing.is_err(),
        "CREATECONSUMER on missing stream should fail"
    );

    // XGROUP DELCONSUMER on existing stream returns 0 pending entries
    let del_consumer =
        execute_command_line(&processor, "XGROUP DELCONSUMER mystream mygroup consumer1").unwrap();
    assert_eq!(del_consumer, b":0\r\n");

    // XGROUP DELCONSUMER on missing stream returns error
    let del_missing =
        execute_command_line(&processor, "XGROUP DELCONSUMER nostream mygroup consumer1");
    assert!(
        del_missing.is_err(),
        "DELCONSUMER on missing stream should fail"
    );

    // XGROUP HELP returns updated count (17 entries)
    let help = execute_command_line(&processor, "XGROUP HELP").unwrap();
    assert!(
        help.starts_with(b"*17\r\n"),
        "XGROUP HELP should return 17-element array"
    );
    assert_command_error(
        &processor,
        "XGROUP HELP extra",
        b"-ERR wrong number of arguments for 'xgroup|help' command\r\n",
    );

    // XGROUP unknown subcommand returns error
    let unknown = execute_command_line(&processor, "XGROUP BADCMD mystream mygroup");
    assert!(unknown.is_err(), "XGROUP BADCMD should return error");
}

#[test]
fn xgroup_create_surface_matches_stream_cgroups_external_scenarios() {
    let processor = RequestProcessor::new().unwrap();

    // Redis tests/unit/type/stream-cgroups.tcl:
    // - "XGROUP CREATE: creation and duplicate group name detection"
    // - "XGROUP CREATE: with ENTRIESREAD parameter"
    // - "XGROUP CREATE: automatic stream creation fails without MKSTREAM"
    assert_command_integer(&processor, "DEL mystream", 0);
    assert_command_response(&processor, "XADD mystream 1-1 a 1", b"$3\r\n1-1\r\n");
    assert_command_response(&processor, "XADD mystream 1-2 b 2", b"$3\r\n1-2\r\n");
    assert_command_response(&processor, "XADD mystream 1-3 c 3", b"$3\r\n1-3\r\n");
    assert_command_response(&processor, "XADD mystream 1-4 d 4", b"$3\r\n1-4\r\n");

    assert_command_response(&processor, "XGROUP CREATE mystream mygroup $", b"+OK\r\n");
    assert_command_error(
        &processor,
        "XGROUP CREATE mystream mygroup $",
        b"-BUSYGROUP Consumer Group name already exists\r\n",
    );
    assert_command_error(
        &processor,
        "XGROUP CREATE mystream badgroup $ ENTRIESREAD -3",
        b"-ERR value for ENTRIESREAD must be positive or -1\r\n",
    );

    assert_command_response(
        &processor,
        "XGROUP CREATE mystream mygroup1 $ ENTRIESREAD 0",
        b"+OK\r\n",
    );
    assert_command_response(
        &processor,
        "XGROUP CREATE mystream mygroup2 $ ENTRIESREAD 3",
        b"+OK\r\n",
    );
    assert_command_error(
        &processor,
        "XGROUP CREATE mystream_missing mygroup $",
        b"-ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.\r\n",
    );
    assert_command_response(
        &processor,
        "XGROUP CREATE mystream_created mygroup $ MKSTREAM",
        b"+OK\r\n",
    );

    let xinfo_groups = execute_command_line(&processor, "XINFO GROUPS mystream").unwrap();
    assert!(
        xinfo_groups
            .windows(b"$7\r\nmygroup\r\n".len())
            .any(|window| window == b"$7\r\nmygroup\r\n"),
        "XINFO GROUPS should include the first created group"
    );
    assert!(
        xinfo_groups
            .windows(b"$8\r\nmygroup1\r\n".len())
            .any(|window| window == b"$8\r\nmygroup1\r\n"),
        "XINFO GROUPS should include ENTRIESREAD=0 group"
    );
    assert!(
        xinfo_groups
            .windows(b"$8\r\nmygroup2\r\n".len())
            .any(|window| window == b"$8\r\nmygroup2\r\n"),
        "XINFO GROUPS should include ENTRIESREAD=3 group"
    );
    assert!(
        xinfo_groups
            .windows(b"$12\r\nentries-read\r\n:0\r\n$3\r\nlag\r\n:4\r\n".len())
            .any(|window| window == b"$12\r\nentries-read\r\n:0\r\n$3\r\nlag\r\n:4\r\n"),
        "XINFO GROUPS should expose ENTRIESREAD=0 and derived lag"
    );
    assert!(
        xinfo_groups
            .windows(b"$12\r\nentries-read\r\n:3\r\n$3\r\nlag\r\n:1\r\n".len())
            .any(|window| window == b"$12\r\nentries-read\r\n:3\r\n$3\r\nlag\r\n:1\r\n"),
        "XINFO GROUPS should expose ENTRIESREAD=3 and derived lag"
    );
}

#[test]
fn xread_and_xreadgroup_argument_surface_matches_stream_cgroups_external_scenarios() {
    let processor = RequestProcessor::new().unwrap();

    // Redis tests/unit/type/stream-cgroups.tcl:
    // - "XREADGROUP stream and ID pairing"
    // - "XREADGROUP stream ID format validation"
    // - "XREAD and XREADGROUP against wrong parameter"
    assert_command_response(&processor, "XADD mystream 666 f v", b"$5\r\n666-0\r\n");
    assert_command_response(&processor, "XGROUP CREATE mystream mygroup $", b"+OK\r\n");

    assert_command_error(
        &processor,
        "XREADGROUP GROUP mygroup consumer STREAMS mystream > stream2",
        b"-ERR Unbalanced 'xreadgroup' list of streams: for each stream key an ID or '>' must be specified.\r\n",
    );
    assert_command_error(
        &processor,
        "XREADGROUP GROUP mygroup consumer STREAMS mystream stream2 >",
        b"-ERR Unbalanced 'xreadgroup' list of streams: for each stream key an ID or '>' must be specified.\r\n",
    );
    assert_command_error(
        &processor,
        "XREADGROUP GROUP mygroup Alice COUNT 1 STREAMS mystream",
        b"-ERR Unbalanced 'xreadgroup' list of streams: for each stream key an ID or '>' must be specified.\r\n",
    );
    assert_command_error(
        &processor,
        "XREAD COUNT 1 STREAMS mystream",
        b"-ERR Unbalanced 'xread' list of streams: for each stream key an ID, '+', or '$' must be specified.\r\n",
    );
    assert_command_error(
        &processor,
        "XREAD COUNT 2 CLAIM 10 STREAMS mystream 0-0",
        b"-ERR The CLAIM option is only supported by XREADGROUP. You called XREAD instead.\r\n",
    );

    for invalid_id_command in [
        "XREADGROUP GROUP mygroup consumer STREAMS mystream invalid-id",
        "XREADGROUP GROUP mygroup consumer STREAMS mystream 123-",
        "XREADGROUP GROUP mygroup consumer STREAMS mystream -123",
        "XREADGROUP GROUP mygroup consumer STREAMS mystream abc-def",
        "XREADGROUP GROUP mygroup consumer STREAMS mystream --",
        "XREADGROUP GROUP mygroup consumer STREAMS mystream 123-abc",
    ] {
        assert_command_error(
            &processor,
            invalid_id_command,
            b"-ERR Invalid stream ID specified as stream command argument\r\n",
        );
    }
}

#[test]
fn xread_last_element_plus_matches_external_stream_scenarios() {
    let processor = RequestProcessor::new().unwrap();

    // Redis tests/unit/type/stream.tcl:
    // - "XREAD last element from non-empty stream"
    // - "XREAD last element from empty stream"
    // - "XREAD last element from multiple streams"
    // - "XREAD last element with count > 1"
    // - "XREAD: read last element after XDEL (issue #13628)"
    let expect_single_last =
        |response: &[u8], stream: &[u8], id: &[u8], field: &[u8], value: &[u8]| {
            let (actual_stream, entries) = parse_xread_single_stream_response(response).unwrap();
            assert_eq!(actual_stream, stream);
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].0, id);
            assert_eq!(entries[0].1, vec![(field.to_vec(), value.to_vec())]);
        };

    let _ = execute_command_line(&processor, "DEL lestream").unwrap();
    assert_command_response(&processor, "XADD lestream 1-0 k1 v1", b"$3\r\n1-0\r\n");
    assert_command_response(&processor, "XADD lestream 2-0 k2 v2", b"$3\r\n2-0\r\n");
    assert_command_response(&processor, "XADD lestream 3-0 k3 v3", b"$3\r\n3-0\r\n");

    let first_last = execute_command_line(&processor, "XREAD STREAMS lestream +").unwrap();
    expect_single_last(&first_last, b"lestream", b"3-0", b"k3", b"v3");

    assert_command_response(
        &processor,
        "XADD lestream 3-18446744073709551614 k4 v4",
        b"$22\r\n3-18446744073709551614\r\n",
    );
    assert_command_response(
        &processor,
        "XADD lestream 3-18446744073709551615 k5 v5",
        b"$22\r\n3-18446744073709551615\r\n",
    );

    let max_seq_last = execute_command_line(&processor, "XREAD STREAMS lestream +").unwrap();
    expect_single_last(
        &max_seq_last,
        b"lestream",
        b"3-18446744073709551615",
        b"k5",
        b"v5",
    );

    let count_ignored =
        execute_command_line(&processor, "XREAD COUNT 3 STREAMS lestream +").unwrap();
    expect_single_last(
        &count_ignored,
        b"lestream",
        b"3-18446744073709551615",
        b"k5",
        b"v5",
    );

    let _ = execute_command_line(&processor, "DEL lestream").unwrap();
    let empty_stream = execute_command_line(&processor, "XREAD STREAMS lestream +").unwrap();
    assert!(parse_xread_single_stream_response(&empty_stream).is_none());

    assert_command_response(&processor, "XADD lestream 1-0 k1 v1", b"$3\r\n1-0\r\n");
    assert_command_integer(&processor, "XDEL lestream 1-0", 1);
    let deleted_last = execute_command_line(&processor, "XREAD STREAMS lestream +").unwrap();
    assert!(parse_xread_single_stream_response(&deleted_last).is_none());

    let _ = execute_command_line(&processor, "DEL lestream").unwrap();
    assert_command_response(
        &processor,
        "XGROUP CREATE lestream legroup $ MKSTREAM",
        b"+OK\r\n",
    );
    let mkstream_empty = execute_command_line(&processor, "XREAD STREAMS lestream +").unwrap();
    assert!(parse_xread_single_stream_response(&mkstream_empty).is_none());

    let _ = execute_command_line(&processor, "DEL {lestream}1 {lestream}2 {lestream}3").unwrap();
    assert_command_response(&processor, "XADD {lestream}1 1-0 k1 v1", b"$3\r\n1-0\r\n");
    assert_command_response(&processor, "XADD {lestream}1 2-0 k2 v2", b"$3\r\n2-0\r\n");
    assert_command_response(&processor, "XADD {lestream}1 3-0 k3 v3", b"$3\r\n3-0\r\n");
    assert_command_response(&processor, "XADD {lestream}2 1-0 k1 v4", b"$3\r\n1-0\r\n");
    assert_command_response(&processor, "XADD {lestream}2 2-0 k2 v5", b"$3\r\n2-0\r\n");
    assert_command_response(&processor, "XADD {lestream}2 3-0 k3 v6", b"$3\r\n3-0\r\n");

    let multi_last = execute_command_line(
        &processor,
        "XREAD STREAMS {lestream}1 {lestream}2 {lestream}3 + + +",
    )
    .unwrap();
    let multi_root = parse_resp_test_value(&multi_last);
    let streams = resp_test_array(&multi_root);
    assert_eq!(streams.len(), 2);

    let first_stream = resp_test_array(&streams[0]);
    assert_eq!(resp_test_bulk(&first_stream[0]), b"{lestream}1");
    let first_entries = resp_test_array(&first_stream[1]);
    assert_eq!(first_entries.len(), 1);
    let first_entry = resp_test_array(&first_entries[0]);
    assert_eq!(resp_test_bulk(&first_entry[0]), b"3-0");
    assert_eq!(
        resp_test_field_pairs(&first_entry[1]),
        vec![(b"k3".to_vec(), b"v3".to_vec())]
    );

    let second_stream = resp_test_array(&streams[1]);
    assert_eq!(resp_test_bulk(&second_stream[0]), b"{lestream}2");
    let second_entries = resp_test_array(&second_stream[1]);
    assert_eq!(second_entries.len(), 1);
    let second_entry = resp_test_array(&second_entries[0]);
    assert_eq!(resp_test_bulk(&second_entry[0]), b"3-0");
    assert_eq!(
        resp_test_field_pairs(&second_entry[1]),
        vec![(b"k3".to_vec(), b"v6".to_vec())]
    );

    let _ = execute_command_line(&processor, "DEL stream").unwrap();
    assert_command_response(&processor, "XADD stream 1-0 f 1", b"$3\r\n1-0\r\n");
    assert_command_response(&processor, "XADD stream 2-0 f 2", b"$3\r\n2-0\r\n");
    assert_command_integer(&processor, "XDEL stream 2-0", 1);
    let after_xdel = execute_command_line(&processor, "XREAD STREAMS stream +").unwrap();
    expect_single_last(&after_xdel, b"stream", b"1-0", b"f", b"1");
}

#[test]
fn xadd_id_overflow_and_future_streamid_edge_match_external_stream_scenarios() {
    let processor = RequestProcessor::new().unwrap();

    // Redis tests/unit/type/stream.tcl:
    // - "XADD IDs correctly report an error when overflowing"
    // - "XADD streamID edge"
    let _ = execute_command_line(&processor, "DEL mystream").unwrap();
    assert_command_response(
        &processor,
        "XADD mystream 18446744073709551615-18446744073709551615 a b",
        b"$41\r\n18446744073709551615-18446744073709551615\r\n",
    );
    assert_command_error(
        &processor,
        "XADD mystream * c d",
        b"-ERR value is out of range\r\n",
    );

    let _ = execute_command_line(&processor, "DEL x").unwrap();
    assert_command_response(
        &processor,
        "XADD x 2577343934890-18446744073709551615 f v",
        b"$34\r\n2577343934890-18446744073709551615\r\n",
    );
    assert_command_response(&processor, "XADD x * f2 v2", b"$15\r\n2577343934891-0\r\n");
    assert_command_response(
        &processor,
        "XRANGE x - +",
        b"*2\r\n*2\r\n$34\r\n2577343934890-18446744073709551615\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n*2\r\n$15\r\n2577343934891-0\r\n*2\r\n$2\r\nf2\r\n$2\r\nv2\r\n",
    );
}

#[test]
fn config_set_stream_idmp_validation_matches_external_stream_scenario() {
    let processor = RequestProcessor::new().unwrap();

    // Redis tests/unit/type/stream.tcl:
    // - "CONFIG SET stream-idmp-duration and stream-idmp-maxsize validation"
    for command in [
        "CONFIG SET stream-idmp-duration 86401",
        "CONFIG SET stream-idmp-duration 100000",
        "CONFIG SET stream-idmp-duration 0",
        "CONFIG SET stream-idmp-duration -1",
        "CONFIG SET stream-idmp-duration -100",
        "CONFIG SET stream-idmp-maxsize 10001",
        "CONFIG SET stream-idmp-maxsize 50000",
        "CONFIG SET stream-idmp-maxsize 0",
        "CONFIG SET stream-idmp-maxsize -1",
        "CONFIG SET stream-idmp-maxsize -100",
    ] {
        let response = execute_command_line(&processor, command).unwrap();
        let response_text = String::from_utf8_lossy(&response);
        assert!(
            response_text.contains("must be between"),
            "expected range validation error for `{command}`, got: {response_text}"
        );
    }

    assert_command_response(
        &processor,
        "CONFIG SET stream-idmp-duration 86400",
        b"+OK\r\n",
    );
    let duration = execute_command_line(&processor, "CONFIG GET stream-idmp-duration").unwrap();
    assert_eq!(
        parse_bulk_array_payloads(&duration),
        vec![b"stream-idmp-duration".to_vec(), b"86400".to_vec(),]
    );

    assert_command_response(
        &processor,
        "CONFIG SET stream-idmp-maxsize 10000",
        b"+OK\r\n",
    );
    let maxsize = execute_command_line(&processor, "CONFIG GET stream-idmp-maxsize").unwrap();
    assert_eq!(
        parse_bulk_array_payloads(&maxsize),
        vec![b"stream-idmp-maxsize".to_vec(), b"10000".to_vec(),]
    );

    assert_command_response(&processor, "CONFIG SET stream-idmp-duration 1", b"+OK\r\n");
    let duration = execute_command_line(&processor, "CONFIG GET stream-idmp-duration").unwrap();
    assert_eq!(
        parse_bulk_array_payloads(&duration),
        vec![b"stream-idmp-duration".to_vec(), b"1".to_vec(),]
    );

    assert_command_response(&processor, "CONFIG SET stream-idmp-maxsize 1", b"+OK\r\n");
    let maxsize = execute_command_line(&processor, "CONFIG GET stream-idmp-maxsize").unwrap();
    assert_eq!(
        parse_bulk_array_payloads(&maxsize),
        vec![b"stream-idmp-maxsize".to_vec(), b"1".to_vec(),]
    );

    assert_command_response(
        &processor,
        "CONFIG SET stream-idmp-duration 100",
        b"+OK\r\n",
    );
    assert_command_response(&processor, "CONFIG SET stream-idmp-maxsize 100", b"+OK\r\n");
}

#[test]
fn xrange_exclusive_ranges_match_external_stream_scenario() {
    let processor = RequestProcessor::new().unwrap();

    // Redis tests/unit/type/stream.tcl: "XRANGE exclusive ranges"
    let _ = execute_command_line(&processor, "DEL vipstream").unwrap();
    for id in [
        "0-1",
        "0-18446744073709551615",
        "1-0",
        "42-0",
        "42-42",
        "18446744073709551615-18446744073709551614",
        "18446744073709551615-18446744073709551615",
    ] {
        let command = format!("XADD vipstream {id} foo bar");
        assert!(
            execute_command_line(&processor, &command).is_ok(),
            "failed to seed `{command}`"
        );
    }

    let all_entries = execute_command_line(&processor, "XRANGE vipstream - +").unwrap();
    assert_eq!(parse_stream_entry_list_response(&all_entries).len(), 7);

    let exclude_first = execute_command_line(&processor, "XRANGE vipstream (0-1 +").unwrap();
    assert_eq!(parse_stream_entry_list_response(&exclude_first).len(), 6);

    let exclude_last = execute_command_line(
        &processor,
        "XRANGE vipstream - (18446744073709551615-18446744073709551615",
    )
    .unwrap();
    assert_eq!(parse_stream_entry_list_response(&exclude_last).len(), 6);

    let middle_one = execute_command_line(&processor, "XRANGE vipstream (0-1 (1-0").unwrap();
    let middle_entries = parse_stream_entry_list_response(&middle_one);
    assert_eq!(middle_entries.len(), 1);
    assert_eq!(middle_entries[0].0, b"0-18446744073709551615".to_vec());

    let another_middle = execute_command_line(&processor, "XRANGE vipstream (1-0 (42-42").unwrap();
    let middle_entries = parse_stream_entry_list_response(&another_middle);
    assert_eq!(middle_entries.len(), 1);
    assert_eq!(middle_entries[0].0, b"42-0".to_vec());

    for command in [
        "XRANGE vipstream (- +",
        "XRANGE vipstream - (+",
        "XRANGE vipstream (18446744073709551615-18446744073709551615 +",
        "XRANGE vipstream - (0-0",
    ] {
        let error = execute_command_line(&processor, command).unwrap_err();
        let CommandHarnessError::Request(error) = error else {
            panic!("expected request error for `{command}`");
        };
        let mut response = Vec::new();
        error.append_resp_error(&mut response);
        assert!(
            response.starts_with(b"-ERR "),
            "expected ERR response for `{command}`, got {:?}",
            String::from_utf8_lossy(&response)
        );
    }
}

#[test]
fn xadd_and_xtrim_limit_semantics_match_external_stream_scenarios() {
    let processor = RequestProcessor::new().unwrap();

    // Redis tests/unit/type/stream.tcl:
    // - "XADD with LIMIT delete entries no more than limit"
    // - "XTRIM with MAXLEN option basic test"
    // - "XADD with LIMIT consecutive calls"
    // - "XTRIM with ~ is limited"
    // - "XTRIM without ~ and with LIMIT"
    // - "XTRIM with LIMIT delete entries no more than limit"
    let _ = execute_command_line(&processor, "DEL yourstream").unwrap();
    for _ in 0..3 {
        let _ = execute_command_line(&processor, "XADD yourstream * xitem v").unwrap();
    }
    let _ =
        execute_command_line(&processor, "XADD yourstream MAXLEN ~ 0 LIMIT 1 * xitem v").unwrap();
    assert_command_integer(&processor, "XLEN yourstream", 4);

    let _ = execute_command_line(&processor, "DEL mystream").unwrap();
    for index in 0..1000 {
        let command = format!("XADD mystream * xitem {index}");
        let _ = execute_command_line(&processor, &command).unwrap();
    }
    assert_command_integer(&processor, "XTRIM mystream MAXLEN 666", 334);
    assert_command_integer(&processor, "XLEN mystream", 666);
    assert_command_integer(&processor, "XTRIM mystream MAXLEN = 555", 111);
    assert_command_integer(&processor, "XLEN mystream", 555);
    assert_command_integer(&processor, "XTRIM mystream MAXLEN ~ 444", 55);
    assert_command_integer(&processor, "XLEN mystream", 500);
    assert_command_integer(&processor, "XTRIM mystream MAXLEN ~ 400", 100);
    assert_command_integer(&processor, "XLEN mystream", 400);

    assert_command_response(
        &processor,
        "CONFIG SET stream-node-max-entries 10",
        b"+OK\r\n",
    );
    let _ = execute_command_line(&processor, "DEL mystream").unwrap();
    for _ in 0..100 {
        let _ = execute_command_line(&processor, "XADD mystream * xitem v").unwrap();
    }
    let _ =
        execute_command_line(&processor, "XADD mystream MAXLEN ~ 55 LIMIT 30 * xitem v").unwrap();
    assert_command_integer(&processor, "XLEN mystream", 71);
    let _ =
        execute_command_line(&processor, "XADD mystream MAXLEN ~ 55 LIMIT 30 * xitem v").unwrap();
    assert_command_integer(&processor, "XLEN mystream", 62);
    assert_command_response(
        &processor,
        "CONFIG SET stream-node-max-entries 100",
        b"+OK\r\n",
    );

    assert_command_response(
        &processor,
        "CONFIG SET stream-node-max-entries 1",
        b"+OK\r\n",
    );
    let _ = execute_command_line(&processor, "DEL mystream").unwrap();
    for _ in 0..102 {
        let _ = execute_command_line(&processor, "XADD mystream * xitem v").unwrap();
    }
    assert_command_integer(&processor, "XTRIM mystream MAXLEN ~ 1", 100);
    assert_command_integer(&processor, "XLEN mystream", 2);
    assert_command_response(
        &processor,
        "CONFIG SET stream-node-max-entries 100",
        b"+OK\r\n",
    );

    assert_command_response(
        &processor,
        "CONFIG SET stream-node-max-entries 1",
        b"+OK\r\n",
    );
    let _ = execute_command_line(&processor, "DEL mystream").unwrap();
    for _ in 0..102 {
        let _ = execute_command_line(&processor, "XADD mystream * xitem v").unwrap();
    }
    let error = execute_command_line(&processor, "XTRIM mystream MAXLEN 1 LIMIT 30").unwrap_err();
    let CommandHarnessError::Request(error) = error else {
        panic!("expected request error for XTRIM exact LIMIT");
    };
    let mut response = Vec::new();
    error.append_resp_error(&mut response);
    assert!(
        response.starts_with(b"-ERR "),
        "expected ERR response for XTRIM exact LIMIT, got {:?}",
        String::from_utf8_lossy(&response)
    );
    assert_command_response(
        &processor,
        "CONFIG SET stream-node-max-entries 100",
        b"+OK\r\n",
    );

    assert_command_response(
        &processor,
        "CONFIG SET stream-node-max-entries 2",
        b"+OK\r\n",
    );
    let _ = execute_command_line(&processor, "DEL mystream").unwrap();
    for _ in 0..3 {
        let _ = execute_command_line(&processor, "XADD mystream * xitem v").unwrap();
    }
    assert_command_integer(&processor, "XTRIM mystream MAXLEN ~ 0 LIMIT 1", 0);
    assert_command_integer(&processor, "XTRIM mystream MAXLEN ~ 0 LIMIT 2", 2);
    assert_command_response(
        &processor,
        "CONFIG SET stream-node-max-entries 100",
        b"+OK\r\n",
    );
}

#[test]
fn xtrim_acked_and_delref_match_external_stream_scenarios() {
    let processor = RequestProcessor::new().unwrap();

    // Redis tests/unit/type/stream.tcl:
    // - "XTRIM with approx and ACKED deletes entries correctly"
    // - "XTRIM with approx and DELREF deletes entries correctly"
    assert_command_response(
        &processor,
        "CONFIG SET stream-node-max-entries 2",
        b"+OK\r\n",
    );

    let _ = execute_command_line(&processor, "DEL mystream").unwrap();
    for id in ["1-0", "2-0", "3-0", "4-0", "5-0"] {
        let command = format!("XADD mystream {id} f v");
        let _ = execute_command_line(&processor, &command).unwrap();
    }
    assert_command_response(&processor, "XGROUP CREATE mystream mygroup 0", b"+OK\r\n");
    let _ = execute_command_line(
        &processor,
        "XREADGROUP GROUP mygroup consumer1 STREAMS mystream >",
    )
    .unwrap();
    assert_command_integer(&processor, "XACK mystream mygroup 1-0 2-0 4-0", 3);
    assert_command_integer(&processor, "XTRIM mystream MINID ~ 6-0 ACKED", 3);
    assert_command_integer(&processor, "XLEN mystream", 2);
    assert_command_response(
        &processor,
        "XRANGE mystream - +",
        b"*2\r\n*2\r\n$3\r\n3-0\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n*2\r\n$3\r\n5-0\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n",
    );

    let _ = execute_command_line(&processor, "DEL mystream").unwrap();
    for id in ["1-0", "2-0", "3-0", "4-0"] {
        let command = format!("XADD mystream {id} f v");
        let _ = execute_command_line(&processor, &command).unwrap();
    }
    assert_command_response(&processor, "XGROUP CREATE mystream mygroup 0", b"+OK\r\n");
    let _ = execute_command_line(
        &processor,
        "XREADGROUP GROUP mygroup consumer1 STREAMS mystream >",
    )
    .unwrap();
    assert_command_integer(&processor, "XTRIM mystream MINID ~ 5-0 DELREF", 4);
    assert_command_integer(&processor, "XLEN mystream", 0);
    assert_command_response(
        &processor,
        "XPENDING mystream mygroup",
        b"*4\r\n:0\r\n$-1\r\n$-1\r\n*0\r\n",
    );

    assert_command_response(
        &processor,
        "CONFIG SET stream-node-max-entries 100",
        b"+OK\r\n",
    );
}

#[test]
fn xadd_acked_and_delref_match_external_stream_scenarios() {
    let processor = RequestProcessor::new().unwrap();

    // Redis tests/unit/type/stream.tcl:
    // - "XADD with MAXLEN option and ACKED option"
    // - "XADD with ACKED option doesn't crash after DEBUG RELOAD"
    // - "XADD with MAXLEN option and DELREF option"
    let _ = execute_command_line(&processor, "DEL mystream").unwrap();
    for id in ["1-0", "2-0", "3-0", "4-0", "5-0"] {
        let command = format!("XADD mystream {id} f v");
        let _ = execute_command_line(&processor, &command).unwrap();
    }
    assert_command_integer(&processor, "XLEN mystream", 5);

    assert_command_response(&processor, "XGROUP CREATE mystream mygroup 0", b"+OK\r\n");
    assert_command_response(
        &processor,
        "XADD mystream MAXLEN = 1 ACKED 6-0 f v",
        b"$3\r\n6-0\r\n",
    );
    assert_command_integer(&processor, "XLEN mystream", 6);

    let first_read = execute_command_line(
        &processor,
        "XREADGROUP GROUP mygroup consumer1 COUNT 1 STREAMS mystream >",
    )
    .unwrap();
    let (_, first_entries) = parse_xread_single_stream_response(&first_read).unwrap();
    assert_eq!(first_entries.len(), 1);
    let first_id = String::from_utf8(first_entries[0].0.clone()).unwrap();
    assert_command_integer(&processor, &format!("XACK mystream mygroup {first_id}"), 1);
    let (pending_count, _, _, _) = parse_xpending_summary_response(
        &execute_command_line(&processor, "XPENDING mystream mygroup").unwrap(),
    );
    assert_eq!(pending_count, 0);

    assert_command_response(
        &processor,
        "XADD mystream MAXLEN = 1 ACKED 7-0 f v",
        b"$3\r\n7-0\r\n",
    );
    assert_command_integer(&processor, "XLEN mystream", 6);

    let remaining_read = execute_command_line(
        &processor,
        "XREADGROUP GROUP mygroup consumer1 STREAMS mystream >",
    )
    .unwrap();
    let (_, remaining_entries) = parse_xread_single_stream_response(&remaining_read).unwrap();
    let mut remaining_ids = Vec::with_capacity(remaining_entries.len());
    for (id, _) in remaining_entries {
        remaining_ids.push(String::from_utf8(id).unwrap());
    }
    assert_eq!(remaining_ids.len(), 6);
    assert_command_integer(
        &processor,
        &format!("XACK mystream mygroup {}", remaining_ids.join(" ")),
        6,
    );
    let (pending_count, _, _, _) = parse_xpending_summary_response(
        &execute_command_line(&processor, "XPENDING mystream mygroup").unwrap(),
    );
    assert_eq!(pending_count, 0);

    let auto_id = execute_command_line(&processor, "XADD mystream MAXLEN = 1 ACKED * f v").unwrap();
    assert!(auto_id.starts_with(b"$"));
    assert_command_integer(&processor, "XLEN mystream", 1);

    let _ = execute_command_line(&processor, "DEL mystream").unwrap();
    assert_command_response(&processor, "XADD mystream 1-0 f v", b"$3\r\n1-0\r\n");
    assert_command_response(&processor, "XGROUP CREATE mystream mygroup 0", b"+OK\r\n");
    let reloaded_read = execute_command_line(
        &processor,
        "XREADGROUP GROUP mygroup consumer1 COUNT 1 STREAMS mystream >",
    )
    .unwrap();
    let (_, reloaded_entries) = parse_xread_single_stream_response(&reloaded_read).unwrap();
    assert_eq!(reloaded_entries.len(), 1);
    let reloaded_id = String::from_utf8(reloaded_entries[0].0.clone()).unwrap();
    let (pending_count, _, _, _) = parse_xpending_summary_response(
        &execute_command_line(&processor, "XPENDING mystream mygroup").unwrap(),
    );
    assert_eq!(pending_count, 1);

    assert_command_response(&processor, "DEBUG RELOAD", b"+OK\r\n");
    assert_command_response(
        &processor,
        "XADD mystream MAXLEN = 1 ACKED 2-0 f v",
        b"$3\r\n2-0\r\n",
    );
    assert_command_integer(&processor, "XLEN mystream", 2);

    assert_command_integer(
        &processor,
        &format!("XACK mystream mygroup {reloaded_id}"),
        1,
    );
    let (pending_count, _, _, _) = parse_xpending_summary_response(
        &execute_command_line(&processor, "XPENDING mystream mygroup").unwrap(),
    );
    assert_eq!(pending_count, 0);

    assert_command_response(&processor, "DEBUG RELOAD", b"+OK\r\n");
    assert_command_response(
        &processor,
        "XADD mystream MAXLEN = 1 ACKED 3-0 f v",
        b"$3\r\n3-0\r\n",
    );
    assert_command_integer(&processor, "XLEN mystream", 2);

    let _ = execute_command_line(&processor, "DEL mystream").unwrap();
    for id in ["1-0", "2-0", "3-0", "4-0", "5-0"] {
        let command = format!("XADD mystream {id} f v");
        let _ = execute_command_line(&processor, &command).unwrap();
    }
    assert_command_response(&processor, "XGROUP CREATE mystream mygroup 0", b"+OK\r\n");
    let delref_read = execute_command_line(
        &processor,
        "XREADGROUP GROUP mygroup consumer1 COUNT 1 STREAMS mystream >",
    )
    .unwrap();
    let (_, delref_entries) = parse_xread_single_stream_response(&delref_read).unwrap();
    assert_eq!(delref_entries.len(), 1);
    let delref_auto_id =
        execute_command_line(&processor, "XADD mystream MAXLEN = 1 DELREF * f v").unwrap();
    assert!(delref_auto_id.starts_with(b"$"));
    assert_command_integer(&processor, "XLEN mystream", 1);
    let (pending_count, _, _, _) = parse_xpending_summary_response(
        &execute_command_line(&processor, "XPENDING mystream mygroup").unwrap(),
    );
    assert_eq!(pending_count, 0);
}

#[test]
fn xadd_idmp_basic_and_auto_match_external_stream_scenarios() {
    let processor = RequestProcessor::new().unwrap();

    fn execute_frame_error(processor: &RequestProcessor, parts: &[&[u8]]) -> Vec<u8> {
        let frame = encode_resp(parts);
        let mut args = [ArgSlice::EMPTY; 32];
        let meta = parse_resp_command_arg_slices(&frame, &mut args).unwrap();
        let mut response = Vec::new();
        let err = processor
            .execute_in_db(
                &args[..meta.argument_count],
                &mut response,
                DbName::default(),
            )
            .unwrap_err();
        err.append_resp_error(&mut response);
        response
    }

    fn execute_frame_bulk(processor: &RequestProcessor, parts: &[&[u8]]) -> Vec<u8> {
        let response = execute_frame(processor, &encode_resp(parts));
        resp_test_bulk(&parse_resp_test_value(&response)).to_vec()
    }

    // Redis tests/unit/type/stream.tcl:
    // - "XADD IDMP with invalid syntax"
    // - "XADD IDMP duplicate request returns same ID"
    // - "XADD IDMP multiple different IIDs create multiple entries"
    // - "XADD IDMPAUTO basic deduplication based on field-value pairs"
    // - "XADD IDMPAUTO deduplicates regardless of field order"
    assert_eq!(
        execute_frame_error(
            &processor,
            &[b"XADD", b"mystream", b"IDMP", b"p1", b"*", b"f", b"v"]
        ),
        b"-ERR Invalid stream ID specified as stream command argument\r\n",
    );
    assert_eq!(
        execute_frame_error(
            &processor,
            &[
                b"XADD",
                b"mystream",
                b"IDMP",
                b"p1",
                b"iid1",
                b"1-1",
                b"f",
                b"v"
            ],
        ),
        b"-ERR syntax error, IDMP/IDMPAUTO can be used only with auto-generated IDs\r\n",
    );
    assert_eq!(
        execute_frame_error(
            &processor,
            &[
                b"XADD",
                b"mystream",
                b"IDMP",
                b"p1",
                b"iid1",
                b"IDMP",
                b"p2",
                b"iid2",
                b"*",
                b"f",
                b"v",
            ],
        ),
        b"-ERR syntax error, IDMP/IDMPAUTO specified multiple times\r\n",
    );
    assert_eq!(
        execute_frame_error(
            &processor,
            &[
                b"XADD",
                b"mystream",
                b"IDMPAUTO",
                b"p1",
                b"IDMP",
                b"p2",
                b"iid2",
                b"*",
                b"f",
                b"v"
            ],
        ),
        b"-ERR syntax error, IDMP/IDMPAUTO specified multiple times\r\n",
    );
    assert_eq!(
        execute_frame_error(
            &processor,
            &[
                b"XADD",
                b"mystream",
                b"IDMP",
                b"",
                b"iid1",
                b"*",
                b"f",
                b"v"
            ],
        ),
        b"-ERR syntax error, IDMP requires a non-empty producer ID\r\n",
    );
    assert_eq!(
        execute_frame_error(
            &processor,
            &[b"XADD", b"mystream", b"IDMP", b"p1", b"", b"*", b"f", b"v"],
        ),
        b"-ERR syntax error, IDMP requires a non-empty idempotent ID\r\n",
    );
    assert_eq!(
        execute_frame_error(
            &processor,
            &[b"XADD", b"mystream", b"IDMPAUTO", b"", b"*", b"f", b"v"],
        ),
        b"-ERR syntax error, IDMPAUTO requires a non-empty producer ID\r\n",
    );

    assert_command_integer(&processor, "DEL mystream", 0);
    let first_id = execute_frame_bulk(
        &processor,
        &[
            b"XADD",
            b"mystream",
            b"IDMP",
            b"p1",
            b"payment-abc",
            b"*",
            b"amount",
            b"100",
            b"currency",
            b"USD",
        ],
    );
    let duplicate_id = execute_frame_bulk(
        &processor,
        &[
            b"XADD",
            b"mystream",
            b"IDMP",
            b"p1",
            b"payment-abc",
            b"*",
            b"amount",
            b"200",
            b"currency",
            b"EUR",
        ],
    );
    assert_eq!(duplicate_id, first_id);
    assert_command_integer(&processor, "XLEN mystream", 1);
    let duplicate_entries = parse_stream_entry_list_response(
        &execute_command_line(&processor, "XRANGE mystream - +").unwrap(),
    );
    assert_eq!(duplicate_entries.len(), 1);
    assert_eq!(
        duplicate_entries[0].1,
        vec![
            (b"amount".to_vec(), b"100".to_vec()),
            (b"currency".to_vec(), b"USD".to_vec()),
        ]
    );

    assert_command_integer(&processor, "DEL mystream", 1);
    let id1 = execute_frame_bulk(
        &processor,
        &[
            b"XADD",
            b"mystream",
            b"IDMP",
            b"p1",
            b"req-1",
            b"*",
            b"user",
            b"alice",
        ],
    );
    let id2 = execute_frame_bulk(
        &processor,
        &[
            b"XADD",
            b"mystream",
            b"IDMP",
            b"p1",
            b"req-2",
            b"*",
            b"user",
            b"bob",
        ],
    );
    let id3 = execute_frame_bulk(
        &processor,
        &[
            b"XADD",
            b"mystream",
            b"IDMP",
            b"p1",
            b"req-3",
            b"*",
            b"user",
            b"charlie",
        ],
    );
    assert_ne!(id1, id2);
    assert_ne!(id2, id3);
    assert_ne!(id1, id3);
    assert_command_integer(&processor, "XLEN mystream", 3);
    let entries = parse_stream_entry_list_response(
        &execute_command_line(&processor, "XRANGE mystream - +").unwrap(),
    );
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].1, vec![(b"user".to_vec(), b"alice".to_vec())]);
    assert_eq!(entries[1].1, vec![(b"user".to_vec(), b"bob".to_vec())]);
    assert_eq!(entries[2].1, vec![(b"user".to_vec(), b"charlie".to_vec())]);

    assert_command_integer(&processor, "DEL mystream", 1);
    let auto_id_1 = execute_frame_bulk(
        &processor,
        &[
            b"XADD",
            b"mystream",
            b"IDMPAUTO",
            b"p1",
            b"*",
            b"field",
            b"value",
            b"amount",
            b"10",
        ],
    );
    let auto_id_2 = execute_frame_bulk(
        &processor,
        &[
            b"XADD",
            b"mystream",
            b"IDMPAUTO",
            b"p1",
            b"*",
            b"field",
            b"value",
            b"amount",
            b"10",
        ],
    );
    assert_eq!(auto_id_2, auto_id_1);
    assert_command_integer(&processor, "XLEN mystream", 1);

    let ordered_id_1 = execute_frame_bulk(
        &processor,
        &[
            b"XADD",
            b"mystream",
            b"IDMPAUTO",
            b"p1",
            b"*",
            b"f1",
            b"v1",
            b"f2",
            b"v2",
        ],
    );
    let ordered_id_2 = execute_frame_bulk(
        &processor,
        &[
            b"XADD",
            b"mystream",
            b"IDMPAUTO",
            b"p1",
            b"*",
            b"f2",
            b"v2",
            b"f1",
            b"v1",
        ],
    );
    assert_eq!(ordered_id_2, ordered_id_1);
}

#[test]
fn xcfgset_and_xinfo_idmp_match_external_stream_scenarios() {
    let processor = RequestProcessor::new().unwrap();

    fn xinfo_stream_map(
        processor: &RequestProcessor,
        command: &str,
    ) -> std::collections::BTreeMap<Vec<u8>, RespTestValue> {
        let response = execute_command_line(processor, command).unwrap();
        let root = parse_resp_test_value(&response);
        resp_test_flat_map(&root)
            .into_iter()
            .map(|(key, value)| (key, value.clone()))
            .collect()
    }

    // Redis tests/unit/type/stream.tcl:
    // - "XCFGSET set both IDMP-DURATION and IDMP-MAXSIZE"
    // - "XCFGSET invalid syntax"
    // - "XCFGSET changing IDMP-DURATION clears all iids history"
    // - "XCFGSET setting same value preserves iids-tracked count"
    // - "XINFO STREAM returns iids-tracked and iids-added fields"
    // - "XINFO STREAM returns pids-tracked field"
    // - "XINFO STREAM returns idmp-duration and idmp-maxsize fields"
    assert_command_integer(&processor, "DEL mystream", 0);
    assert!(
        execute_command_line(&processor, "XADD mystream * field value")
            .unwrap()
            .starts_with(b"$")
    );
    assert_command_response(
        &processor,
        "XCFGSET mystream IDMP-DURATION 3 IDMP-MAXSIZE 10000",
        b"+OK\r\n",
    );
    let info = xinfo_stream_map(&processor, "XINFO STREAM mystream");
    assert_eq!(resp_test_integer(&info[&b"idmp-duration".to_vec()]), 3);
    assert_eq!(resp_test_integer(&info[&b"idmp-maxsize".to_vec()]), 10000);

    assert_command_error(
        &processor,
        "XCFGSET mystream",
        b"-ERR At least one parameter of IDMP-DURATION and IDMP-MAXSIZE should be specified\r\n",
    );
    assert_command_error(
        &processor,
        "XCFGSET mystream IDMP-DURATION",
        b"-ERR syntax error\r\n",
    );
    assert_command_error(
        &processor,
        "XCFGSET mystream IDMP-MAXSIZE",
        b"-ERR syntax error\r\n",
    );
    assert_command_error(
        &processor,
        "XCFGSET mystream IDMP-DURATION A",
        b"-ERR value is not an integer or out of range\r\n",
    );
    assert_command_error(
        &processor,
        "XCFGSET mystream IDMP-DURATION 000000000",
        b"-ERR value is not an integer or out of range\r\n",
    );
    assert_command_error(
        &processor,
        "XCFGSET mystream IDMP-MAXSIZE 0",
        b"-ERR IDMP-MAXSIZE must be between 1 and 10000 entries\r\n",
    );

    assert_command_integer(&processor, "DEL mystream", 1);
    let old_id = resp_test_bulk(&parse_resp_test_value(
        &execute_command_line(&processor, "XADD mystream IDMP p1 req-1 * field value1").unwrap(),
    ))
    .to_vec();
    assert!(
        execute_command_line(&processor, "XADD mystream IDMP p1 req-2 * field value2")
            .unwrap()
            .starts_with(b"$")
    );
    let duplicate_before_clear = resp_test_bulk(&parse_resp_test_value(
        &execute_command_line(&processor, "XADD mystream IDMP p1 req-1 * field dup").unwrap(),
    ))
    .to_vec();
    assert_eq!(duplicate_before_clear, old_id);
    assert_command_response(&processor, "XCFGSET mystream IDMP-DURATION 5", b"+OK\r\n");
    let new_id = resp_test_bulk(&parse_resp_test_value(
        &execute_command_line(&processor, "XADD mystream IDMP p1 req-1 * field new1").unwrap(),
    ))
    .to_vec();
    assert_ne!(new_id, old_id);
    assert_command_integer(&processor, "XLEN mystream", 3);

    assert_command_integer(&processor, "DEL mystream", 1);
    let p1_req1_id = resp_test_bulk(&parse_resp_test_value(
        &execute_command_line(&processor, "XADD mystream IDMP p1 req-1 * field value1").unwrap(),
    ))
    .to_vec();
    let p1_req2_id = resp_test_bulk(&parse_resp_test_value(
        &execute_command_line(&processor, "XADD mystream IDMP p1 req-2 * field value2").unwrap(),
    ))
    .to_vec();
    let _p2_req3_id = resp_test_bulk(&parse_resp_test_value(
        &execute_command_line(&processor, "XADD mystream IDMP p2 req-3 * field value3").unwrap(),
    ))
    .to_vec();
    let before_same_value = xinfo_stream_map(&processor, "XINFO STREAM mystream");
    assert_eq!(
        resp_test_integer(&before_same_value[&b"iids-tracked".to_vec()]),
        3
    );
    assert_eq!(
        resp_test_integer(&before_same_value[&b"iids-added".to_vec()]),
        3
    );
    assert_eq!(
        resp_test_integer(&before_same_value[&b"pids-tracked".to_vec()]),
        2
    );
    assert_command_response(
        &processor,
        "XCFGSET mystream IDMP-DURATION 100 IDMP-MAXSIZE 100",
        b"+OK\r\n",
    );
    let after_same_value = xinfo_stream_map(&processor, "XINFO STREAM mystream");
    assert_eq!(
        resp_test_integer(&after_same_value[&b"iids-tracked".to_vec()]),
        3
    );
    assert_eq!(
        resp_test_integer(&after_same_value[&b"iids-added".to_vec()]),
        3
    );
    assert_eq!(
        resp_test_integer(&after_same_value[&b"pids-tracked".to_vec()]),
        2
    );

    let duplicate_after_same_value = resp_test_bulk(&parse_resp_test_value(
        &execute_command_line(&processor, "XADD mystream IDMP p1 req-1 * field dup2").unwrap(),
    ))
    .to_vec();
    let duplicate_second_id = resp_test_bulk(&parse_resp_test_value(
        &execute_command_line(&processor, "XADD mystream IDMP p1 req-2 * field dup3").unwrap(),
    ))
    .to_vec();
    assert_eq!(duplicate_after_same_value, p1_req1_id);
    assert_eq!(duplicate_second_id, p1_req2_id);

    let full_info = xinfo_stream_map(&processor, "XINFO STREAM mystream FULL");
    assert_eq!(
        resp_test_integer(&full_info[&b"idmp-duration".to_vec()]),
        100
    );
    assert_eq!(
        resp_test_integer(&full_info[&b"idmp-maxsize".to_vec()]),
        100
    );
    assert_eq!(resp_test_integer(&full_info[&b"iids-tracked".to_vec()]), 3);
    assert_eq!(resp_test_integer(&full_info[&b"iids-added".to_vec()]), 3);
    assert_eq!(resp_test_integer(&full_info[&b"pids-tracked".to_vec()]), 2);
}

#[test]
fn idmp_and_xcfgset_persistence_match_external_stream_scenarios() {
    let processor = RequestProcessor::new().unwrap();

    fn xinfo_stream_map(
        processor: &RequestProcessor,
        command: &str,
    ) -> std::collections::BTreeMap<Vec<u8>, RespTestValue> {
        let response = execute_command_line(processor, command).unwrap();
        let root = parse_resp_test_value(&response);
        resp_test_flat_map(&root)
            .into_iter()
            .map(|(key, value)| (key, value.clone()))
            .collect()
    }

    // Redis tests/unit/type/stream.tcl:
    // - "XADD IDMP persists in RDB"
    // - "XADD IDMP multiple producers persistence in RDB"
    assert_command_integer(&processor, "DEL mystream", 0);
    let p1_id = resp_test_bulk(&parse_resp_test_value(
        &execute_command_line(&processor, "XADD mystream IDMP p1 persist-1 * field value1")
            .unwrap(),
    ))
    .to_vec();
    let p2_id = resp_test_bulk(&parse_resp_test_value(
        &execute_command_line(&processor, "XADD mystream IDMP p2 persist-1 * field value2")
            .unwrap(),
    ))
    .to_vec();
    let p3_id = resp_test_bulk(&parse_resp_test_value(
        &execute_command_line(&processor, "XADD mystream IDMP p3 persist-1 * field value3")
            .unwrap(),
    ))
    .to_vec();
    let before_save = xinfo_stream_map(&processor, "XINFO STREAM mystream");
    assert_eq!(
        resp_test_integer(&before_save[&b"pids-tracked".to_vec()]),
        3
    );
    assert_eq!(
        resp_test_integer(&before_save[&b"iids-tracked".to_vec()]),
        3
    );
    assert_command_response(&processor, "SAVE", b"+OK\r\n");
    assert_command_response(&processor, "DEBUG RELOAD", b"+OK\r\n");

    let restored_info = xinfo_stream_map(&processor, "XINFO STREAM mystream");
    assert_eq!(
        resp_test_integer(&restored_info[&b"pids-tracked".to_vec()]),
        3
    );
    assert_eq!(
        resp_test_integer(&restored_info[&b"iids-tracked".to_vec()]),
        3
    );
    let p1_duplicate_after_reload = resp_test_bulk(&parse_resp_test_value(
        &execute_command_line(&processor, "XADD mystream IDMP p1 persist-1 * field dup1").unwrap(),
    ))
    .to_vec();
    let p2_duplicate_after_reload = resp_test_bulk(&parse_resp_test_value(
        &execute_command_line(&processor, "XADD mystream IDMP p2 persist-1 * field dup2").unwrap(),
    ))
    .to_vec();
    let p3_duplicate_after_reload = resp_test_bulk(&parse_resp_test_value(
        &execute_command_line(&processor, "XADD mystream IDMP p3 persist-1 * field dup3").unwrap(),
    ))
    .to_vec();
    assert_eq!(p1_duplicate_after_reload, p1_id);
    assert_eq!(p2_duplicate_after_reload, p2_id);
    assert_eq!(p3_duplicate_after_reload, p3_id);
    assert_command_integer(&processor, "XLEN mystream", 3);

    // Redis tests/unit/type/stream.tcl:
    // - "XCFGSET configuration persists in RDB"
    assert_command_integer(&processor, "DEL cfgstream", 0);
    assert!(
        execute_command_line(&processor, "XADD cfgstream IDMP p1 req-1 * field value")
            .unwrap()
            .starts_with(b"$")
    );
    assert_command_response(
        &processor,
        "XCFGSET cfgstream IDMP-DURATION 75 IDMP-MAXSIZE 7500",
        b"+OK\r\n",
    );
    assert_command_response(&processor, "SAVE", b"+OK\r\n");
    assert_command_response(&processor, "DEBUG RELOAD", b"+OK\r\n");
    let cfg_info = xinfo_stream_map(&processor, "XINFO STREAM cfgstream");
    assert_eq!(resp_test_integer(&cfg_info[&b"idmp-duration".to_vec()]), 75);
    assert_eq!(
        resp_test_integer(&cfg_info[&b"idmp-maxsize".to_vec()]),
        7500
    );

    // Redis tests/unit/type/stream.tcl:
    // - "XADD IDMP set in AOF"
    assert_command_integer(&processor, "DEL aofstream", 0);
    let aof_id = resp_test_bulk(&parse_resp_test_value(
        &execute_command_line(&processor, "XADD aofstream IDMP p1 aof-1 * field value1").unwrap(),
    ))
    .to_vec();
    assert!(
        execute_command_line(&processor, "XADD aofstream IDMP p1 aof-2 * field value2")
            .unwrap()
            .starts_with(b"$")
    );
    let duplicate_before_loadaof = resp_test_bulk(&parse_resp_test_value(
        &execute_command_line(&processor, "XADD aofstream IDMP p1 aof-1 * field dup").unwrap(),
    ))
    .to_vec();
    assert_eq!(duplicate_before_loadaof, aof_id);
    assert_command_response(
        &processor,
        "BGREWRITEAOF",
        b"+Background append only file rewriting started\r\n",
    );
    assert_command_response(&processor, "DEBUG LOADAOF", b"+OK\r\n");
    assert_command_integer(&processor, "XLEN aofstream", 2);
    let duplicate_after_loadaof = resp_test_bulk(&parse_resp_test_value(
        &execute_command_line(&processor, "XADD aofstream IDMP p1 aof-1 * field dup2").unwrap(),
    ))
    .to_vec();
    assert_eq!(duplicate_after_loadaof, aof_id);

    // Redis tests/unit/type/stream.tcl:
    // - "XCFGSET configuration in AOF"
    assert_command_integer(&processor, "DEL aofcfg", 0);
    assert!(
        execute_command_line(&processor, "XADD aofcfg IDMP p1 req-1 * field value")
            .unwrap()
            .starts_with(b"$")
    );
    assert_command_response(
        &processor,
        "XCFGSET aofcfg IDMP-DURATION 45 IDMP-MAXSIZE 4500",
        b"+OK\r\n",
    );
    assert_command_response(
        &processor,
        "BGREWRITEAOF",
        b"+Background append only file rewriting started\r\n",
    );
    assert_command_response(&processor, "DEBUG LOADAOF", b"+OK\r\n");
    let aof_info = xinfo_stream_map(&processor, "XINFO STREAM aofcfg");
    assert_eq!(resp_test_integer(&aof_info[&b"idmp-duration".to_vec()]), 45);
    assert_eq!(
        resp_test_integer(&aof_info[&b"idmp-maxsize".to_vec()]),
        4500
    );
}

#[test]
fn debug_reload_preserves_mixed_dataset_digest_like_external_other_scenario() {
    let processor = RequestProcessor::new().unwrap();

    // Redis tests/unit/other.tcl:
    // - "Check consistency of different data types after a reload"
    assert_command_response(&processor, "FLUSHDB", b"+OK\r\n");
    assert_command_response(&processor, "SET s value", b"+OK\r\n");
    assert_command_integer(&processor, "LPUSH l a b c", 3);
    assert_command_integer(&processor, "SADD set x y z", 3);
    assert_command_integer(&processor, "HSET h f1 v1 f2 v2", 2);
    assert_command_integer(&processor, "ZADD z 1 one 2 two", 2);
    assert_command_response(&processor, "XADD stream 1-0 f v", b"$3\r\n1-0\r\n");

    let digest_before = execute_command_line(&processor, "DEBUG DIGEST").unwrap();
    assert_command_response(&processor, "DEBUG RELOAD", b"+OK\r\n");
    let digest_after = execute_command_line(&processor, "DEBUG DIGEST").unwrap();
    assert_eq!(digest_after, digest_before);
}

#[test]
fn expires_survive_debug_reload_and_loadaof_like_external_other_scenario() {
    let processor = RequestProcessor::new().unwrap();

    // Redis tests/unit/other.tcl:
    // - "EXPIRES after a reload (snapshot + append only file rewrite)"
    assert_command_response(&processor, "FLUSHDB", b"+OK\r\n");
    assert_command_response(&processor, "SET x 10", b"+OK\r\n");
    assert_command_integer(&processor, "EXPIRE x 1000", 1);
    assert_command_response(&processor, "SAVE", b"+OK\r\n");
    assert_command_response(&processor, "DEBUG RELOAD", b"+OK\r\n");

    let ttl_after_reload =
        parse_integer_response(&execute_command_line(&processor, "TTL x").unwrap());
    assert!(
        ttl_after_reload > 900 && ttl_after_reload <= 1000,
        "expected TTL after DEBUG RELOAD to stay in (900, 1000], got {ttl_after_reload}"
    );

    assert_command_response(
        &processor,
        "BGREWRITEAOF",
        b"+Background append only file rewriting started\r\n",
    );
    assert_command_response(&processor, "DEBUG LOADAOF", b"+OK\r\n");

    let ttl_after_loadaof =
        parse_integer_response(&execute_command_line(&processor, "TTL x").unwrap());
    assert!(
        ttl_after_loadaof > 900 && ttl_after_loadaof <= 1000,
        "expected TTL after DEBUG LOADAOF to stay in (900, 1000], got {ttl_after_loadaof}"
    );
}

#[test]
fn xadd_maxlen_and_xreadgroup_default_count_cover_external_stream_scenarios() {
    let processor = RequestProcessor::new().unwrap();

    // Redis tests/unit/type/stream.tcl: "XADD with MAXLEN option"
    assert_command_response(
        &processor,
        "XADD trimstream MAXLEN 2 1-0 f a",
        b"$3\r\n1-0\r\n",
    );
    assert_command_response(
        &processor,
        "XADD trimstream MAXLEN 2 2-0 f b",
        b"$3\r\n2-0\r\n",
    );
    assert_command_response(
        &processor,
        "XADD trimstream MAXLEN 2 3-0 f c",
        b"$3\r\n3-0\r\n",
    );
    assert_command_response(
        &processor,
        "XRANGE trimstream - +",
        b"*2\r\n*2\r\n$3\r\n2-0\r\n*2\r\n$1\r\nf\r\n$1\r\nb\r\n*2\r\n$3\r\n3-0\r\n*2\r\n$1\r\nf\r\n$1\r\nc\r\n",
    );

    // Redis tests/unit/type/stream-cgroups.tcl:
    // - "XREADGROUP will return only new elements"
    assert_command_response(&processor, "XADD mystream2 1-0 foo bar", b"$3\r\n1-0\r\n");
    assert_command_response(&processor, "XGROUP CREATE mystream2 mygroup $", b"+OK\r\n");
    assert_command_response(&processor, "XADD mystream2 1-1 a 1", b"$3\r\n1-1\r\n");
    assert_command_response(&processor, "XADD mystream2 1-2 b 2", b"$3\r\n1-2\r\n");
    assert_command_response(
        &processor,
        "XREADGROUP GROUP mygroup consumer-1 STREAMS mystream2 >",
        b"*1\r\n*2\r\n$9\r\nmystream2\r\n*2\r\n*2\r\n$3\r\n1-1\r\n*2\r\n$1\r\na\r\n$1\r\n1\r\n*2\r\n$3\r\n1-2\r\n*2\r\n$1\r\nb\r\n$1\r\n2\r\n",
    );
}

#[test]
fn xreadgroup_history_xpending_and_xack_match_stream_cgroups_external_scenarios() {
    let processor = RequestProcessor::new().unwrap();

    assert_command_response(
        &processor,
        "XADD mystream 1-0 field value",
        b"$3\r\n1-0\r\n",
    );
    assert_command_response(&processor, "XGROUP CREATE mystream mygroup $", b"+OK\r\n");
    assert_command_response(&processor, "XADD mystream 2-0 a 1", b"$3\r\n2-0\r\n");
    assert_command_response(&processor, "XADD mystream 3-0 b 2", b"$3\r\n3-0\r\n");

    let first_read = execute_command_line(
        &processor,
        "XREADGROUP GROUP mygroup consumer-1 STREAMS mystream >",
    )
    .unwrap();
    let (_, first_entries) = parse_xread_single_stream_response(&first_read).unwrap();
    assert_eq!(first_entries.len(), 2);
    assert_eq!(first_entries[0].0, b"2-0".to_vec());
    assert_eq!(first_entries[1].0, b"3-0".to_vec());

    assert_command_response(&processor, "XADD mystream 4-0 c 3", b"$3\r\n4-0\r\n");
    assert_command_response(&processor, "XADD mystream 5-0 d 4", b"$3\r\n5-0\r\n");

    let second_read = execute_command_line(
        &processor,
        "XREADGROUP GROUP mygroup consumer-2 STREAMS mystream >",
    )
    .unwrap();
    let (_, second_entries) = parse_xread_single_stream_response(&second_read).unwrap();
    assert_eq!(second_entries.len(), 2);
    assert_eq!(second_entries[0].0, b"4-0".to_vec());
    assert_eq!(second_entries[1].0, b"5-0".to_vec());

    let history_consumer_1 = execute_command_line(
        &processor,
        "XREADGROUP GROUP mygroup consumer-1 COUNT 10 STREAMS mystream 0",
    )
    .unwrap();
    let (_, history_entries_1) = parse_xread_single_stream_response(&history_consumer_1).unwrap();
    assert_eq!(
        history_entries_1
            .iter()
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>(),
        vec![b"2-0".to_vec(), b"3-0".to_vec()]
    );

    let history_consumer_2 = execute_command_line(
        &processor,
        "XREADGROUP GROUP mygroup consumer-2 COUNT 10 STREAMS mystream 0",
    )
    .unwrap();
    let (_, history_entries_2) = parse_xread_single_stream_response(&history_consumer_2).unwrap();
    assert_eq!(
        history_entries_2
            .iter()
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>(),
        vec![b"4-0".to_vec(), b"5-0".to_vec()]
    );

    let pending_detail =
        execute_command_line(&processor, "XPENDING mystream mygroup - + 10").unwrap();
    let pending_entries = parse_xpending_detail_response(&pending_detail);
    assert_eq!(pending_entries.len(), 4);
    assert_eq!(pending_entries[0].1, b"consumer-1".to_vec());
    assert_eq!(pending_entries[1].1, b"consumer-1".to_vec());
    assert_eq!(pending_entries[2].1, b"consumer-2".to_vec());
    assert_eq!(pending_entries[3].1, b"consumer-2".to_vec());
    assert!(
        pending_entries
            .iter()
            .all(|(_, _, _, deliveries)| *deliveries == 1)
    );

    let pending_consumer_1 =
        execute_command_line(&processor, "XPENDING mystream mygroup - + 10 consumer-1").unwrap();
    assert_eq!(parse_xpending_detail_response(&pending_consumer_1).len(), 2);

    thread::sleep(Duration::from_millis(20));
    let pending_idle_filtered = execute_command_line(
        &processor,
        "XPENDING mystream mygroup IDLE 1 - + 10 consumer-1",
    )
    .unwrap();
    assert_eq!(
        parse_xpending_detail_response(&pending_idle_filtered).len(),
        2
    );

    let pending_summary = execute_command_line(&processor, "XPENDING mystream mygroup").unwrap();
    let (pending_count, smallest, greatest, pending_by_consumer) =
        parse_xpending_summary_response(&pending_summary);
    assert_eq!(pending_count, 4);
    assert_eq!(smallest, Some(b"2-0".to_vec()));
    assert_eq!(greatest, Some(b"5-0".to_vec()));
    assert_eq!(
        pending_by_consumer,
        vec![(b"consumer-1".to_vec(), 2), (b"consumer-2".to_vec(), 2)]
    );

    let first_pending_id = pending_entries[0].0.clone();
    let second_pending_id = pending_entries[1].0.clone();
    assert_command_response(
        &processor,
        &format!(
            "XACK mystream mygroup {}",
            String::from_utf8(first_pending_id.clone()).unwrap()
        ),
        b":1\r\n",
    );
    let pending_after_first_ack =
        execute_command_line(&processor, "XPENDING mystream mygroup - + 10 consumer-1").unwrap();
    let consumer_1_after_first_ack = parse_xpending_detail_response(&pending_after_first_ack);
    assert_eq!(consumer_1_after_first_ack.len(), 1);
    assert_eq!(consumer_1_after_first_ack[0].0, second_pending_id);

    assert_command_response(
        &processor,
        &format!(
            "XACK mystream mygroup {} {}",
            String::from_utf8(first_pending_id).unwrap(),
            String::from_utf8(consumer_1_after_first_ack[0].0.clone()).unwrap()
        ),
        b":1\r\n",
    );
    assert_command_error(
        &processor,
        "XACK mystream mygroup invalid-id",
        b"-ERR Invalid stream ID specified as stream command argument\r\n",
    );
}

#[test]
fn xgroup_setid_and_xreadgroup_history_edge_cases_match_stream_cgroups_external_scenarios() {
    let processor = RequestProcessor::new().unwrap();

    assert_command_integer(&processor, "DEL events", 0);
    assert_command_response(&processor, "XADD events 1-0 f1 v1", b"$3\r\n1-0\r\n");
    assert_command_response(&processor, "XADD events 2-0 f1 v1", b"$3\r\n2-0\r\n");
    assert_command_response(&processor, "XADD events 3-0 f1 v1", b"$3\r\n3-0\r\n");
    assert_command_response(&processor, "XADD events 4-0 f1 v1", b"$3\r\n4-0\r\n");
    assert_command_response(&processor, "XGROUP CREATE events g1 $", b"+OK\r\n");
    assert_command_response(&processor, "XADD events 5-0 f1 v1", b"$3\r\n5-0\r\n");

    let first_delivery =
        execute_command_line(&processor, "XREADGROUP GROUP g1 c1 STREAMS events >").unwrap();
    let (_, first_delivery_entries) = parse_xread_single_stream_response(&first_delivery).unwrap();
    assert_eq!(first_delivery_entries.len(), 1);
    assert_eq!(first_delivery_entries[0].0, b"5-0".to_vec());

    assert_command_response(&processor, "XGROUP SETID events g1 -", b"+OK\r\n");
    let reset_delivery =
        execute_command_line(&processor, "XREADGROUP GROUP g1 c2 STREAMS events >").unwrap();
    let (_, reset_delivery_entries) = parse_xread_single_stream_response(&reset_delivery).unwrap();
    assert_eq!(reset_delivery_entries.len(), 5);

    assert_command_integer(&processor, "DEL events2", 0);
    assert_command_response(&processor, "XADD events2 1-0 a 1", b"$3\r\n1-0\r\n");
    assert_command_response(&processor, "XADD events2 2-0 b 2", b"$3\r\n2-0\r\n");
    assert_command_response(&processor, "XADD events2 3-0 c 3", b"$3\r\n3-0\r\n");
    assert_command_response(&processor, "XGROUP CREATE events2 mygroup 0", b"+OK\r\n");

    let empty_history = execute_command_line(
        &processor,
        "XREADGROUP GROUP mygroup myconsumer COUNT 3 STREAMS events2 0",
    )
    .unwrap();
    let (_, empty_history_entries) = parse_xread_single_stream_response(&empty_history).unwrap();
    assert!(empty_history_entries.is_empty());

    let fresh_delivery = execute_command_line(
        &processor,
        "XREADGROUP GROUP mygroup myconsumer COUNT 3 STREAMS events2 >",
    )
    .unwrap();
    let (_, fresh_delivery_entries) = parse_xread_single_stream_response(&fresh_delivery).unwrap();
    assert_eq!(fresh_delivery_entries.len(), 3);

    let replay_history = execute_command_line(
        &processor,
        "XREADGROUP GROUP mygroup myconsumer COUNT 3 STREAMS events2 0",
    )
    .unwrap();
    let (_, replay_history_entries) = parse_xread_single_stream_response(&replay_history).unwrap();
    assert_eq!(replay_history_entries.len(), 3);
}

#[test]
fn xreadgroup_history_reports_deleted_entries_like_stream_cgroups_external_scenario() {
    let processor = RequestProcessor::new().unwrap();

    assert_command_integer(&processor, "DEL mystream3", 0);
    assert_command_response(
        &processor,
        "XGROUP CREATE mystream3 mygroup $ MKSTREAM",
        b"+OK\r\n",
    );
    assert_command_response(&processor, "XADD mystream3 1-0 field1 A", b"$3\r\n1-0\r\n");
    let first_delivery = execute_command_line(
        &processor,
        "XREADGROUP GROUP mygroup myconsumer STREAMS mystream3 >",
    )
    .unwrap();
    let (_, first_delivery_entries) = parse_xread_single_stream_response(&first_delivery).unwrap();
    assert_eq!(first_delivery_entries.len(), 1);

    assert_command_response(
        &processor,
        "XADD mystream3 MAXLEN 1 2-0 field1 B",
        b"$3\r\n2-0\r\n",
    );
    let second_delivery = execute_command_line(
        &processor,
        "XREADGROUP GROUP mygroup myconsumer STREAMS mystream3 >",
    )
    .unwrap();
    let (_, second_delivery_entries) =
        parse_xread_single_stream_response(&second_delivery).unwrap();
    assert_eq!(second_delivery_entries.len(), 1);

    let history = execute_command_line(
        &processor,
        "XREADGROUP GROUP mygroup myconsumer STREAMS mystream3 0-1",
    )
    .unwrap();
    let (_, history_entries) = parse_xread_single_stream_response(&history).unwrap();
    assert_eq!(history_entries.len(), 2);
    assert_eq!(history_entries[0].0, b"1-0".to_vec());
    assert!(history_entries[0].1.is_empty());
    assert_eq!(history_entries[1].0, b"2-0".to_vec());
    assert_eq!(
        history_entries[1].1,
        vec![(b"field1".to_vec(), b"B".to_vec())]
    );
}

#[test]
fn xclaim_matches_stream_cgroups_external_scenarios() {
    let processor = RequestProcessor::new().unwrap();

    // Redis tests/unit/type/stream-cgroups.tcl:
    // - "XCLAIM can claim PEL items from another consumer"
    // - "XCLAIM without JUSTID increments delivery count"
    // - "XCLAIM same consumer"
    // - "XCLAIM with XDEL"
    // - "XCLAIM with trimming"
    assert_command_integer(&processor, "DEL mystream", 0);
    let id1 =
        parse_bulk_payload(&execute_command_line(&processor, "XADD mystream 1-0 a 1").unwrap())
            .unwrap();
    let id2 =
        parse_bulk_payload(&execute_command_line(&processor, "XADD mystream 2-0 b 2").unwrap())
            .unwrap();
    let id3 =
        parse_bulk_payload(&execute_command_line(&processor, "XADD mystream 3-0 c 3").unwrap())
            .unwrap();
    assert_command_response(&processor, "XGROUP CREATE mystream mygroup 0", b"+OK\r\n");

    let first_read = execute_command_line(
        &processor,
        "XREADGROUP GROUP mygroup consumer1 COUNT 1 STREAMS mystream >",
    )
    .unwrap();
    let (_, first_entries) = parse_xread_single_stream_response(&first_read).unwrap();
    assert_eq!(first_entries.len(), 1);
    assert_eq!(first_entries[0].0, id1);
    assert_eq!(first_entries[0].1, vec![(b"a".to_vec(), b"1".to_vec())]);

    let pending_before_claim =
        execute_command_line(&processor, "XPENDING mystream mygroup - + 10").unwrap();
    assert_eq!(
        parse_xpending_detail_response(&pending_before_claim).len(),
        1
    );
    let pending_consumer1 =
        execute_command_line(&processor, "XPENDING mystream mygroup - + 10 consumer1").unwrap();
    assert_eq!(parse_xpending_detail_response(&pending_consumer1).len(), 1);
    let pending_consumer2 =
        execute_command_line(&processor, "XPENDING mystream mygroup - + 10 consumer2").unwrap();
    assert!(parse_xpending_detail_response(&pending_consumer2).is_empty());

    thread::sleep(Duration::from_millis(20));
    let id1_text = String::from_utf8(first_entries[0].0.clone()).unwrap();
    let claimed = execute_command_line(
        &processor,
        &format!("XCLAIM mystream mygroup consumer2 10 {id1_text}"),
    )
    .unwrap();
    let claimed_entries = parse_stream_entry_list_response(&claimed);
    assert_eq!(claimed_entries.len(), 1);
    assert_eq!(claimed_entries[0].0, b"1-0".to_vec());
    assert_eq!(claimed_entries[0].1, vec![(b"a".to_vec(), b"1".to_vec())]);

    let pending_after_claim =
        execute_command_line(&processor, "XPENDING mystream mygroup - + 10").unwrap();
    assert_eq!(
        parse_xpending_detail_response(&pending_after_claim).len(),
        1
    );
    let pending_consumer1_after_claim =
        execute_command_line(&processor, "XPENDING mystream mygroup - + 10 consumer1").unwrap();
    assert!(parse_xpending_detail_response(&pending_consumer1_after_claim).is_empty());
    let pending_consumer2_after_claim =
        execute_command_line(&processor, "XPENDING mystream mygroup - + 10 consumer2").unwrap();
    assert_eq!(
        parse_xpending_detail_response(&pending_consumer2_after_claim).len(),
        1
    );

    execute_command_line(
        &processor,
        "XREADGROUP GROUP mygroup consumer1 COUNT 2 STREAMS mystream >",
    )
    .unwrap();
    thread::sleep(Duration::from_millis(20));

    let id2_text = String::from_utf8(id2.clone()).unwrap();
    assert_command_integer(&processor, &format!("XDEL mystream {id2_text}"), 1);
    let deleted_claim = execute_command_line(
        &processor,
        &format!("XCLAIM mystream mygroup consumer2 10 {id2_text}"),
    )
    .unwrap();
    assert!(parse_stream_entry_list_response(&deleted_claim).is_empty());

    thread::sleep(Duration::from_millis(20));
    let id3_text = String::from_utf8(id3.clone()).unwrap();
    assert_command_integer(&processor, &format!("XDEL mystream {id3_text}"), 1);
    let deleted_claim_2 = execute_command_line(
        &processor,
        &format!("XCLAIM mystream mygroup consumer2 10 {id3_text}"),
    )
    .unwrap();
    assert!(parse_stream_entry_list_response(&deleted_claim_2).is_empty());

    assert_command_integer(&processor, "DEL mystream", 1);
    assert_command_response(&processor, "XADD mystream 1-0 a 1", b"$3\r\n1-0\r\n");
    assert_command_response(&processor, "XADD mystream 2-0 b 2", b"$3\r\n2-0\r\n");
    assert_command_response(&processor, "XADD mystream 3-0 c 3", b"$3\r\n3-0\r\n");
    assert_command_response(&processor, "XGROUP CREATE mystream mygroup 0", b"+OK\r\n");
    execute_command_line(
        &processor,
        "XREADGROUP GROUP mygroup consumer1 COUNT 1 STREAMS mystream >",
    )
    .unwrap();
    thread::sleep(Duration::from_millis(20));

    let delivery_count_claim =
        execute_command_line(&processor, "XCLAIM mystream mygroup consumer2 10 1-0").unwrap();
    let delivery_count_entries = parse_stream_entry_list_response(&delivery_count_claim);
    assert_eq!(delivery_count_entries.len(), 1);
    let pending_after_delivery_claim =
        execute_command_line(&processor, "XPENDING mystream mygroup - + 10").unwrap();
    let pending_after_delivery_claim =
        parse_xpending_detail_response(&pending_after_delivery_claim);
    assert_eq!(pending_after_delivery_claim.len(), 1);
    assert_eq!(pending_after_delivery_claim[0].3, 2);

    thread::sleep(Duration::from_millis(20));
    let justid_claim = execute_command_line(
        &processor,
        "XCLAIM mystream mygroup consumer3 10 1-0 JUSTID",
    )
    .unwrap();
    assert_eq!(
        parse_bulk_array_payloads(&justid_claim),
        vec![b"1-0".to_vec()]
    );
    let pending_after_justid = parse_xpending_detail_response(
        &execute_command_line(&processor, "XPENDING mystream mygroup - + 10").unwrap(),
    );
    assert_eq!(pending_after_justid[0].3, 2);

    thread::sleep(Duration::from_millis(20));
    let same_consumer_claim =
        execute_command_line(&processor, "XCLAIM mystream mygroup consumer3 10 1-0").unwrap();
    assert_eq!(
        parse_stream_entry_list_response(&same_consumer_claim).len(),
        1
    );
    let pending_same_consumer = parse_xpending_detail_response(
        &execute_command_line(&processor, "XPENDING mystream mygroup - + 10").unwrap(),
    );
    assert_eq!(pending_same_consumer.len(), 1);
    assert_eq!(pending_same_consumer[0].1, b"consumer3".to_vec());

    assert_command_integer(&processor, "DEL x", 0);
    assert_command_response(&processor, "XADD x 1-0 f v", b"$3\r\n1-0\r\n");
    assert_command_response(&processor, "XADD x 2-0 f v", b"$3\r\n2-0\r\n");
    assert_command_response(&processor, "XADD x 3-0 f v", b"$3\r\n3-0\r\n");
    assert_command_response(&processor, "XGROUP CREATE x grp 0", b"+OK\r\n");
    assert_command_response(
        &processor,
        "XREADGROUP GROUP grp Alice STREAMS x >",
        b"*1\r\n*2\r\n$1\r\nx\r\n*3\r\n*2\r\n$3\r\n1-0\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n*2\r\n$3\r\n2-0\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n*2\r\n$3\r\n3-0\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n",
    );
    assert_command_integer(&processor, "XDEL x 2-0", 1);
    let xdel_claim = execute_command_line(&processor, "XCLAIM x grp Bob 0 1-0 2-0 3-0").unwrap();
    let xdel_claim_entries = parse_stream_entry_list_response(&xdel_claim);
    assert_eq!(
        xdel_claim_entries
            .iter()
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>(),
        vec![b"1-0".to_vec(), b"3-0".to_vec()]
    );
    assert!(
        parse_xpending_detail_response(
            &execute_command_line(&processor, "XPENDING x grp - + 10 Alice").unwrap()
        )
        .is_empty()
    );

    assert_command_integer(&processor, "DEL x", 1);
    assert_command_response(&processor, "XADD x 1-0 f v", b"$3\r\n1-0\r\n");
    assert_command_response(&processor, "XADD x 2-0 f v", b"$3\r\n2-0\r\n");
    assert_command_response(&processor, "XADD x 3-0 f v", b"$3\r\n3-0\r\n");
    assert_command_response(&processor, "XGROUP CREATE x grp 0", b"+OK\r\n");
    execute_command_line(&processor, "XREADGROUP GROUP grp Alice STREAMS x >").unwrap();
    assert_command_integer(&processor, "XTRIM x MAXLEN 1", 2);
    let trimmed_claim = execute_command_line(&processor, "XCLAIM x grp Bob 0 1-0 2-0 3-0").unwrap();
    let trimmed_claim_entries = parse_stream_entry_list_response(&trimmed_claim);
    assert_eq!(
        trimmed_claim_entries
            .iter()
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>(),
        vec![b"3-0".to_vec()]
    );
    assert!(
        parse_xpending_detail_response(
            &execute_command_line(&processor, "XPENDING x grp - + 10 Alice").unwrap()
        )
        .is_empty()
    );
}

#[test]
fn xautoclaim_matches_stream_cgroups_external_scenarios() {
    let processor = RequestProcessor::new().unwrap();

    // Redis tests/unit/type/stream-cgroups.tcl:
    // - "XAUTOCLAIM can claim PEL items from another consumer"
    // - "XAUTOCLAIM as an iterator"
    // - "XAUTOCLAIM COUNT must be > 0"
    // - "XAUTOCLAIM with XDEL"
    // - "XAUTOCLAIM with XDEL and count"
    // - "XAUTOCLAIM with trimming"
    assert_command_integer(&processor, "DEL mystream", 0);
    assert_command_response(&processor, "XADD mystream 1-0 a 1", b"$3\r\n1-0\r\n");
    assert_command_response(&processor, "XADD mystream 2-0 b 2", b"$3\r\n2-0\r\n");
    assert_command_response(&processor, "XADD mystream 3-0 c 3", b"$3\r\n3-0\r\n");
    assert_command_response(&processor, "XADD mystream 4-0 d 4", b"$3\r\n4-0\r\n");
    assert_command_response(&processor, "XGROUP CREATE mystream mygroup 0", b"+OK\r\n");
    execute_command_line(
        &processor,
        "XREADGROUP GROUP mygroup consumer1 COUNT 1 STREAMS mystream >",
    )
    .unwrap();

    thread::sleep(Duration::from_millis(20));
    let first_autoclaim = execute_command_line(
        &processor,
        "XAUTOCLAIM mystream mygroup consumer2 10 - COUNT 1",
    )
    .unwrap();
    let (cursor, claimed_entries, deleted_ids) = parse_xautoclaim_entry_response(&first_autoclaim);
    assert_eq!(cursor, b"0-0".to_vec());
    assert_eq!(claimed_entries.len(), 1);
    assert_eq!(claimed_entries[0].0, b"1-0".to_vec());
    assert_eq!(claimed_entries[0].1, vec![(b"a".to_vec(), b"1".to_vec())]);
    assert!(deleted_ids.is_empty());

    execute_command_line(
        &processor,
        "XREADGROUP GROUP mygroup consumer1 COUNT 3 STREAMS mystream >",
    )
    .unwrap();
    thread::sleep(Duration::from_millis(20));

    assert_command_integer(&processor, "XDEL mystream 2-0", 1);
    let second_autoclaim = execute_command_line(
        &processor,
        "XAUTOCLAIM mystream mygroup consumer2 10 - COUNT 3",
    )
    .unwrap();
    let (cursor, claimed_entries, deleted_ids) = parse_xautoclaim_entry_response(&second_autoclaim);
    assert_eq!(cursor, b"4-0".to_vec());
    assert_eq!(
        claimed_entries
            .iter()
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>(),
        vec![b"1-0".to_vec(), b"3-0".to_vec()]
    );
    assert_eq!(deleted_ids, vec![b"2-0".to_vec()]);

    thread::sleep(Duration::from_millis(20));
    assert_command_integer(&processor, "XDEL mystream 4-0", 1);
    let justid_autoclaim = execute_command_line(
        &processor,
        "XAUTOCLAIM mystream mygroup consumer2 10 - JUSTID",
    )
    .unwrap();
    let (cursor, claimed_ids, deleted_ids) = parse_xautoclaim_justid_response(&justid_autoclaim);
    assert_eq!(cursor, b"0-0".to_vec());
    assert_eq!(claimed_ids, vec![b"1-0".to_vec(), b"3-0".to_vec()]);
    assert_eq!(deleted_ids, vec![b"4-0".to_vec()]);

    assert_command_integer(&processor, "DEL mystream", 1);
    assert_command_response(&processor, "XADD mystream 1-0 a 1", b"$3\r\n1-0\r\n");
    assert_command_response(&processor, "XADD mystream 2-0 b 2", b"$3\r\n2-0\r\n");
    assert_command_response(&processor, "XADD mystream 3-0 c 3", b"$3\r\n3-0\r\n");
    assert_command_response(&processor, "XADD mystream 4-0 d 4", b"$3\r\n4-0\r\n");
    assert_command_response(&processor, "XADD mystream 5-0 e 5", b"$3\r\n5-0\r\n");
    assert_command_response(&processor, "XGROUP CREATE mystream mygroup 0", b"+OK\r\n");
    execute_command_line(
        &processor,
        "XREADGROUP GROUP mygroup consumer1 COUNT 90 STREAMS mystream >",
    )
    .unwrap();

    thread::sleep(Duration::from_millis(20));
    let iterator_first = execute_command_line(
        &processor,
        "XAUTOCLAIM mystream mygroup consumer2 10 - COUNT 2",
    )
    .unwrap();
    let (cursor, claimed_entries, deleted_ids) = parse_xautoclaim_entry_response(&iterator_first);
    assert_eq!(cursor, b"3-0".to_vec());
    assert_eq!(claimed_entries.len(), 2);
    assert_eq!(claimed_entries[0].1, vec![(b"a".to_vec(), b"1".to_vec())]);
    assert!(deleted_ids.is_empty());

    let iterator_second = execute_command_line(
        &processor,
        "XAUTOCLAIM mystream mygroup consumer2 10 3-0 COUNT 2",
    )
    .unwrap();
    let (cursor, claimed_entries, deleted_ids) = parse_xautoclaim_entry_response(&iterator_second);
    assert_eq!(cursor, b"5-0".to_vec());
    assert_eq!(claimed_entries.len(), 2);
    assert_eq!(claimed_entries[0].1, vec![(b"c".to_vec(), b"3".to_vec())]);
    assert!(deleted_ids.is_empty());

    let iterator_third = execute_command_line(
        &processor,
        "XAUTOCLAIM mystream mygroup consumer2 10 5-0 COUNT 1",
    )
    .unwrap();
    let (cursor, claimed_entries, deleted_ids) = parse_xautoclaim_entry_response(&iterator_third);
    assert_eq!(cursor, b"0-0".to_vec());
    assert_eq!(claimed_entries.len(), 1);
    assert_eq!(claimed_entries[0].1, vec![(b"e".to_vec(), b"5".to_vec())]);
    assert!(deleted_ids.is_empty());

    assert_command_error(
        &processor,
        "XAUTOCLAIM key group consumer 1 1 COUNT 0",
        b"-ERR COUNT must be > 0\r\n",
    );
    assert_command_error(
        &processor,
        "XAUTOCLAIM x grp Bob 0 3-0 COUNT 8070450532247928833",
        b"-ERR COUNT must be > 0\r\n",
    );

    assert_command_integer(&processor, "DEL x", 0);
    assert_command_response(&processor, "XADD x 1-0 f v", b"$3\r\n1-0\r\n");
    assert_command_response(&processor, "XADD x 2-0 f v", b"$3\r\n2-0\r\n");
    assert_command_response(&processor, "XADD x 3-0 f v", b"$3\r\n3-0\r\n");
    assert_command_response(&processor, "XGROUP CREATE x grp 0", b"+OK\r\n");
    execute_command_line(&processor, "XREADGROUP GROUP grp Alice STREAMS x >").unwrap();
    assert_command_integer(&processor, "XDEL x 2-0", 1);
    let xdel_autoclaim = execute_command_line(&processor, "XAUTOCLAIM x grp Bob 0 0-0").unwrap();
    let (cursor, claimed_entries, deleted_ids) = parse_xautoclaim_entry_response(&xdel_autoclaim);
    assert_eq!(cursor, b"0-0".to_vec());
    assert_eq!(
        claimed_entries
            .iter()
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>(),
        vec![b"1-0".to_vec(), b"3-0".to_vec()]
    );
    assert_eq!(deleted_ids, vec![b"2-0".to_vec()]);
    assert!(
        parse_xpending_detail_response(
            &execute_command_line(&processor, "XPENDING x grp - + 10 Alice").unwrap()
        )
        .is_empty()
    );

    assert_command_integer(&processor, "DEL x", 1);
    assert_command_response(&processor, "XADD x 1-0 f v", b"$3\r\n1-0\r\n");
    assert_command_response(&processor, "XADD x 2-0 f v", b"$3\r\n2-0\r\n");
    assert_command_response(&processor, "XADD x 3-0 f v", b"$3\r\n3-0\r\n");
    assert_command_response(&processor, "XGROUP CREATE x grp 0", b"+OK\r\n");
    execute_command_line(&processor, "XREADGROUP GROUP grp Alice STREAMS x >").unwrap();
    assert_command_integer(&processor, "XDEL x 1-0", 1);
    assert_command_integer(&processor, "XDEL x 2-0", 1);

    let xdel_count_first =
        execute_command_line(&processor, "XAUTOCLAIM x grp Bob 0 0-0 COUNT 1").unwrap();
    let (cursor, claimed_entries, deleted_ids) = parse_xautoclaim_entry_response(&xdel_count_first);
    assert_eq!(cursor, b"2-0".to_vec());
    assert!(claimed_entries.is_empty());
    assert_eq!(deleted_ids, vec![b"1-0".to_vec()]);

    let xdel_count_second =
        execute_command_line(&processor, "XAUTOCLAIM x grp Bob 0 2-0 COUNT 1").unwrap();
    let (cursor, claimed_entries, deleted_ids) =
        parse_xautoclaim_entry_response(&xdel_count_second);
    assert_eq!(cursor, b"3-0".to_vec());
    assert!(claimed_entries.is_empty());
    assert_eq!(deleted_ids, vec![b"2-0".to_vec()]);

    let xdel_count_third =
        execute_command_line(&processor, "XAUTOCLAIM x grp Bob 0 3-0 COUNT 1").unwrap();
    let (cursor, claimed_entries, deleted_ids) = parse_xautoclaim_entry_response(&xdel_count_third);
    assert_eq!(cursor, b"0-0".to_vec());
    assert_eq!(
        claimed_entries
            .iter()
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>(),
        vec![b"3-0".to_vec()]
    );
    assert!(deleted_ids.is_empty());

    assert_command_integer(&processor, "DEL x", 1);
    assert_command_response(&processor, "XADD x 1-0 f v", b"$3\r\n1-0\r\n");
    assert_command_response(&processor, "XADD x 2-0 f v", b"$3\r\n2-0\r\n");
    assert_command_response(&processor, "XADD x 3-0 f v", b"$3\r\n3-0\r\n");
    assert_command_response(&processor, "XGROUP CREATE x grp 0", b"+OK\r\n");
    execute_command_line(&processor, "XREADGROUP GROUP grp Alice STREAMS x >").unwrap();
    assert_command_integer(&processor, "XTRIM x MAXLEN 1", 2);
    let trimmed_autoclaim = execute_command_line(&processor, "XAUTOCLAIM x grp Bob 0 0-0").unwrap();
    let (cursor, claimed_entries, deleted_ids) =
        parse_xautoclaim_entry_response(&trimmed_autoclaim);
    assert_eq!(cursor, b"0-0".to_vec());
    assert_eq!(
        claimed_entries
            .iter()
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>(),
        vec![b"3-0".to_vec()]
    );
    assert_eq!(deleted_ids, vec![b"1-0".to_vec(), b"2-0".to_vec()]);
}

#[test]
fn xreadgroup_claim_surface_and_basic_semantics_match_stream_cgroups_external_scenarios() {
    let processor = RequestProcessor::new().unwrap();

    // Redis tests/unit/type/stream-cgroups.tcl:
    // - "XREADGROUP CLAIM field types are correct"
    // - "XREADGROUP CLAIM returns unacknowledged messages"
    // - "XREADGROUP CLAIM respects min-idle-time threshold"
    // - "XREADGROUP CLAIM with COUNT limit"
    // - "XREADGROUP CLAIM without messages"
    // - "XREADGROUP CLAIM without pending messages"
    // - "XREADGROUP CLAIM message response format"
    // - "XREADGROUP CLAIM delivery count"
    // - "XREADGROUP CLAIM idle time"
    assert_command_integer(&processor, "DEL mystream", 0);
    assert_command_response(&processor, "XADD mystream 1-0 f v1", b"$3\r\n1-0\r\n");
    assert_command_response(&processor, "XADD mystream 2-0 f v2", b"$3\r\n2-0\r\n");
    assert_command_response(&processor, "XGROUP CREATE mystream group1 0", b"+OK\r\n");
    execute_command_line(
        &processor,
        "XREADGROUP GROUP group1 consumer1 STREAMS mystream >",
    )
    .unwrap();
    thread::sleep(Duration::from_millis(100));

    let claim = execute_command_line(
        &processor,
        "XREADGROUP GROUP group1 consumer2 CLAIM 50 STREAMS mystream >",
    )
    .unwrap();
    let (stream_name, messages) =
        parse_xreadgroup_maybe_claim_single_stream_response(&claim).unwrap();
    assert_eq!(stream_name, b"mystream".to_vec());
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0].0, b"1-0".to_vec());
    assert_eq!(messages[0].1, vec![(b"f".to_vec(), b"v1".to_vec())]);
    assert!(messages[0].2.unwrap() >= 50);
    assert_eq!(messages[0].3, Some(1));
    assert_eq!(messages[1].0, b"2-0".to_vec());
    assert_eq!(messages[1].1, vec![(b"f".to_vec(), b"v2".to_vec())]);
    assert!(messages[1].2.unwrap() >= 50);
    assert_eq!(messages[1].3, Some(1));
    let pending = parse_xpending_detail_response(
        &execute_command_line(&processor, "XPENDING mystream group1 - + 10").unwrap(),
    );
    assert_eq!(pending.len(), 2);
    assert_eq!(pending[0].1, b"consumer2".to_vec());
    assert_eq!(pending[0].3, 2);
    assert_eq!(pending[1].1, b"consumer2".to_vec());
    assert_eq!(pending[1].3, 2);

    assert_command_integer(&processor, "DEL mystream", 1);
    assert_command_response(&processor, "XADD mystream 1-0 f v1", b"$3\r\n1-0\r\n");
    assert_command_response(&processor, "XADD mystream 2-0 f v2", b"$3\r\n2-0\r\n");
    assert_command_response(&processor, "XGROUP CREATE mystream group1 0", b"+OK\r\n");
    execute_command_line(
        &processor,
        "XREADGROUP GROUP group1 consumer1 STREAMS mystream >",
    )
    .unwrap();
    let threshold_miss = execute_command_line(
        &processor,
        "XREADGROUP GROUP group1 consumer2 CLAIM 100 STREAMS mystream >",
    )
    .unwrap();
    assert_eq!(parse_bulk_array_payloads(&threshold_miss).len(), 0);

    assert_command_integer(&processor, "DEL mystream", 1);
    assert_command_response(&processor, "XADD mystream 1-0 f v1", b"$3\r\n1-0\r\n");
    assert_command_response(&processor, "XADD mystream 2-0 f v2", b"$3\r\n2-0\r\n");
    assert_command_response(&processor, "XADD mystream 3-0 f v3", b"$3\r\n3-0\r\n");
    assert_command_response(&processor, "XGROUP CREATE mystream group1 0", b"+OK\r\n");
    execute_command_line(
        &processor,
        "XREADGROUP GROUP group1 consumer1 STREAMS mystream >",
    )
    .unwrap();
    thread::sleep(Duration::from_millis(100));
    let count_limited = execute_command_line(
        &processor,
        "XREADGROUP GROUP group1 consumer2 COUNT 2 CLAIM 50 STREAMS mystream >",
    )
    .unwrap();
    let (_, count_limited_messages) =
        parse_xreadgroup_maybe_claim_single_stream_response(&count_limited).unwrap();
    assert_eq!(count_limited_messages.len(), 2);
    assert_eq!(count_limited_messages[0].0, b"1-0".to_vec());
    assert_eq!(count_limited_messages[0].3, Some(1));
    assert_eq!(count_limited_messages[1].0, b"2-0".to_vec());
    assert_eq!(count_limited_messages[1].3, Some(1));

    assert_command_integer(&processor, "DEL mystream", 1);
    assert_command_response(&processor, "XADD mystream 1-0 f v1", b"$3\r\n1-0\r\n");
    assert_command_integer(&processor, "XDEL mystream 1-0", 1);
    assert_command_response(&processor, "XGROUP CREATE mystream group1 0", b"+OK\r\n");
    let no_messages = execute_command_line(
        &processor,
        "XREADGROUP GROUP group1 consumer1 CLAIM 100 STREAMS mystream >",
    )
    .unwrap();
    assert_eq!(parse_bulk_array_payloads(&no_messages).len(), 0);

    assert_command_integer(&processor, "DEL mystream", 1);
    assert_command_response(&processor, "XADD mystream 1-0 f v1", b"$3\r\n1-0\r\n");
    assert_command_response(&processor, "XADD mystream 2-0 f v2", b"$3\r\n2-0\r\n");
    assert_command_response(&processor, "XGROUP CREATE mystream group1 0", b"+OK\r\n");
    let no_pending = execute_command_line(
        &processor,
        "XREADGROUP GROUP group1 consumer1 CLAIM 100 STREAMS mystream >",
    )
    .unwrap();
    let (_, no_pending_messages) =
        parse_xreadgroup_maybe_claim_single_stream_response(&no_pending).unwrap();
    assert_eq!(no_pending_messages.len(), 2);
    assert_eq!(no_pending_messages[0].2, Some(0));
    assert_eq!(no_pending_messages[0].3, Some(0));
    assert_eq!(no_pending_messages[1].2, Some(0));
    assert_eq!(no_pending_messages[1].3, Some(0));

    assert_command_integer(&processor, "DEL mystream", 1);
    assert_command_response(&processor, "XADD mystream 1-0 f v1", b"$3\r\n1-0\r\n");
    assert_command_response(&processor, "XADD mystream 2-0 f v2", b"$3\r\n2-0\r\n");
    assert_command_response(&processor, "XGROUP CREATE mystream group1 0", b"+OK\r\n");
    execute_command_line(
        &processor,
        "XREADGROUP GROUP group1 consumer1 COUNT 1 STREAMS mystream >",
    )
    .unwrap();
    thread::sleep(Duration::from_millis(100));
    let mixed = execute_command_line(
        &processor,
        "XREADGROUP GROUP group1 consumer2 CLAIM 50 STREAMS mystream >",
    )
    .unwrap();
    let (_, mixed_messages) = parse_xreadgroup_maybe_claim_single_stream_response(&mixed).unwrap();
    assert_eq!(mixed_messages.len(), 2);
    assert_eq!(mixed_messages[0].0, b"1-0".to_vec());
    assert_eq!(mixed_messages[0].3, Some(1));
    assert_eq!(mixed_messages[1].0, b"2-0".to_vec());
    assert_eq!(mixed_messages[1].3, Some(0));

    assert_command_integer(&processor, "DEL mystream", 1);
    assert_command_response(&processor, "XADD mystream 1-0 f v1", b"$3\r\n1-0\r\n");
    assert_command_response(&processor, "XGROUP CREATE mystream group1 0", b"+OK\r\n");
    execute_command_line(
        &processor,
        "XREADGROUP GROUP group1 consumer1 STREAMS mystream >",
    )
    .unwrap();
    thread::sleep(Duration::from_millis(100));
    let first_delivery_count = execute_command_line(
        &processor,
        "XREADGROUP GROUP group1 consumer2 CLAIM 50 STREAMS mystream >",
    )
    .unwrap();
    let (_, first_delivery_messages) =
        parse_xreadgroup_maybe_claim_single_stream_response(&first_delivery_count).unwrap();
    assert_eq!(first_delivery_messages[0].3, Some(1));
    thread::sleep(Duration::from_millis(100));
    execute_command_line(
        &processor,
        "XREADGROUP GROUP group1 consumer3 CLAIM 50 STREAMS mystream >",
    )
    .unwrap();
    thread::sleep(Duration::from_millis(100));
    execute_command_line(
        &processor,
        "XREADGROUP GROUP group1 consumer3 CLAIM 50 STREAMS mystream >",
    )
    .unwrap();
    thread::sleep(Duration::from_millis(100));
    let repeated_delivery_count = execute_command_line(
        &processor,
        "XREADGROUP GROUP group1 consumer3 CLAIM 50 STREAMS mystream >",
    )
    .unwrap();
    let (_, repeated_delivery_messages) =
        parse_xreadgroup_maybe_claim_single_stream_response(&repeated_delivery_count).unwrap();
    assert_eq!(repeated_delivery_messages[0].3, Some(4));

    assert_command_integer(&processor, "DEL mystream", 1);
    assert_command_response(&processor, "XADD mystream 1-0 f v1", b"$3\r\n1-0\r\n");
    assert_command_response(&processor, "XGROUP CREATE mystream group1 0", b"+OK\r\n");
    execute_command_line(
        &processor,
        "XREADGROUP GROUP group1 consumer1 STREAMS mystream >",
    )
    .unwrap();
    thread::sleep(Duration::from_millis(100));
    let first_idle = execute_command_line(
        &processor,
        "XREADGROUP GROUP group1 consumer2 CLAIM 50 STREAMS mystream >",
    )
    .unwrap();
    let (_, first_idle_messages) =
        parse_xreadgroup_maybe_claim_single_stream_response(&first_idle).unwrap();
    assert!(first_idle_messages[0].2.unwrap() >= 50);
    thread::sleep(Duration::from_millis(70));
    let second_idle = execute_command_line(
        &processor,
        "XREADGROUP GROUP group1 consumer3 CLAIM 60 STREAMS mystream >",
    )
    .unwrap();
    let (_, second_idle_messages) =
        parse_xreadgroup_maybe_claim_single_stream_response(&second_idle).unwrap();
    assert!(second_idle_messages[0].2.unwrap() >= 60);
}

#[test]
fn xreadgroup_claim_noack_positions_and_multistream_match_stream_cgroups_external_scenarios() {
    let processor = RequestProcessor::new().unwrap();

    // Redis tests/unit/type/stream-cgroups.tcl:
    // - "XREADGROUP CLAIM with NOACK"
    // - "XREADGROUP CLAIM with NOACK and pending messages"
    // - "XREADGROUP CLAIM with multiple streams"
    // - "XREADGROUP CLAIM with min-idle-time equal to zero"
    // - "XREADGROUP CLAIM with large min-idle-time"
    // - "XREADGROUP CLAIM with not integer for min-idle-time"
    // - "XREADGROUP CLAIM with negative integer for min-idle-time"
    // - "XREADGROUP CLAIM with different position"
    // - "XREADGROUP CLAIM with specific ID"
    // - "XREADGROUP CLAIM on non-existing consumer group"
    // - "XREADGROUP CLAIM on non-existing consumer"
    assert_command_integer(&processor, "DEL mystream", 0);
    assert_command_response(&processor, "XADD mystream 1-0 f v1", b"$3\r\n1-0\r\n");
    assert_command_response(&processor, "XADD mystream 2-0 f v2", b"$3\r\n2-0\r\n");
    assert_command_response(&processor, "XGROUP CREATE mystream group1 0", b"+OK\r\n");
    thread::sleep(Duration::from_millis(100));
    let noack = execute_command_line(
        &processor,
        "XREADGROUP GROUP group1 consumer1 NOACK CLAIM 50 STREAMS mystream >",
    )
    .unwrap();
    let (_, noack_messages) = parse_xreadgroup_maybe_claim_single_stream_response(&noack).unwrap();
    assert_eq!(noack_messages.len(), 2);
    let (pending_total, smallest, greatest, consumers) = parse_xpending_summary_response(
        &execute_command_line(&processor, "XPENDING mystream group1").unwrap(),
    );
    assert_eq!(pending_total, 0);
    assert_eq!(smallest, None);
    assert_eq!(greatest, None);
    assert!(consumers.is_empty());

    assert_command_integer(&processor, "DEL mystream", 1);
    assert_command_response(&processor, "XADD mystream 1-0 f v1", b"$3\r\n1-0\r\n");
    assert_command_response(&processor, "XADD mystream 2-0 f v2", b"$3\r\n2-0\r\n");
    assert_command_response(&processor, "XGROUP CREATE mystream group1 0", b"+OK\r\n");
    execute_command_line(
        &processor,
        "XREADGROUP GROUP group1 consumer1 COUNT 1 STREAMS mystream >",
    )
    .unwrap();
    thread::sleep(Duration::from_millis(100));
    let noack_pending = execute_command_line(
        &processor,
        "XREADGROUP GROUP group1 consumer1 NOACK CLAIM 50 STREAMS mystream >",
    )
    .unwrap();
    let (_, noack_pending_messages) =
        parse_xreadgroup_maybe_claim_single_stream_response(&noack_pending).unwrap();
    assert_eq!(noack_pending_messages.len(), 2);
    assert_eq!(
        parse_xpending_detail_response(
            &execute_command_line(&processor, "XPENDING mystream group1 - + 10").unwrap()
        )
        .len(),
        1
    );
    thread::sleep(Duration::from_millis(100));
    let noack_pending_again = execute_command_line(
        &processor,
        "XREADGROUP GROUP group1 consumer1 NOACK CLAIM 50 STREAMS mystream >",
    )
    .unwrap();
    let (_, noack_pending_again_messages) =
        parse_xreadgroup_maybe_claim_single_stream_response(&noack_pending_again).unwrap();
    assert_eq!(noack_pending_again_messages.len(), 1);
    assert_eq!(noack_pending_again_messages[0].0, b"1-0".to_vec());

    for key in ["mystream{t}1", "mystream{t}2", "mystream{t}3"] {
        let delete = format!("DEL {key}");
        assert_command_integer(&processor, &delete, 0);
    }
    execute_command_line(&processor, "XADD mystream{t}1 1-0 f v1").unwrap();
    execute_command_line(&processor, "XADD mystream{t}1 2-0 f v2").unwrap();
    execute_command_line(&processor, "XADD mystream{t}2 3-0 f v1").unwrap();
    execute_command_line(&processor, "XADD mystream{t}2 4-0 f v2").unwrap();
    execute_command_line(&processor, "XADD mystream{t}3 5-0 f v1").unwrap();
    execute_command_line(&processor, "XADD mystream{t}3 6-0 f v2").unwrap();
    execute_command_line(&processor, "XGROUP CREATE mystream{t}1 group1 0").unwrap();
    execute_command_line(&processor, "XGROUP CREATE mystream{t}2 group1 0").unwrap();
    execute_command_line(&processor, "XGROUP CREATE mystream{t}3 group1 0").unwrap();
    execute_command_line(
        &processor,
        "XREADGROUP GROUP group1 consumer1 COUNT 1 STREAMS mystream{t}1 mystream{t}2 mystream{t}3 > > >",
    )
    .unwrap();
    thread::sleep(Duration::from_millis(100));
    let multistream = parse_resp_test_value(
        &execute_command_line(
            &processor,
            "XREADGROUP GROUP group1 consumer1 CLAIM 50 STREAMS mystream{t}1 mystream{t}2 mystream{t}3 > > >",
        )
        .unwrap(),
    );
    let multistream_items = resp_test_array(&multistream);
    assert_eq!(multistream_items.len(), 3);
    let stream_1 = resp_test_array(&multistream_items[0]);
    assert_eq!(resp_test_bulk(&stream_1[0]), b"mystream{t}1");
    let stream_1_messages = resp_test_array(&stream_1[1]);
    assert_eq!(stream_1_messages.len(), 2);
    assert_eq!(
        resp_test_bulk(&resp_test_array(&stream_1_messages[0])[0]),
        b"1-0"
    );
    assert_eq!(
        resp_test_integer(&resp_test_array(&stream_1_messages[0])[3]),
        1
    );
    assert_eq!(
        resp_test_bulk(&resp_test_array(&stream_1_messages[1])[0]),
        b"2-0"
    );
    assert_eq!(
        resp_test_integer(&resp_test_array(&stream_1_messages[1])[3]),
        0
    );

    let stream_2 = resp_test_array(&multistream_items[1]);
    assert_eq!(resp_test_bulk(&stream_2[0]), b"mystream{t}2");
    let stream_2_messages = resp_test_array(&stream_2[1]);
    assert_eq!(stream_2_messages.len(), 2);
    assert_eq!(
        resp_test_bulk(&resp_test_array(&stream_2_messages[0])[0]),
        b"3-0"
    );
    assert_eq!(
        resp_test_integer(&resp_test_array(&stream_2_messages[0])[3]),
        1
    );
    assert_eq!(
        resp_test_bulk(&resp_test_array(&stream_2_messages[1])[0]),
        b"4-0"
    );
    assert_eq!(
        resp_test_integer(&resp_test_array(&stream_2_messages[1])[3]),
        0
    );

    let stream_3 = resp_test_array(&multistream_items[2]);
    assert_eq!(resp_test_bulk(&stream_3[0]), b"mystream{t}3");
    let stream_3_messages = resp_test_array(&stream_3[1]);
    assert_eq!(stream_3_messages.len(), 2);
    assert_eq!(
        resp_test_bulk(&resp_test_array(&stream_3_messages[0])[0]),
        b"5-0"
    );
    assert_eq!(
        resp_test_integer(&resp_test_array(&stream_3_messages[0])[3]),
        1
    );
    assert_eq!(
        resp_test_bulk(&resp_test_array(&stream_3_messages[1])[0]),
        b"6-0"
    );
    assert_eq!(
        resp_test_integer(&resp_test_array(&stream_3_messages[1])[3]),
        0
    );

    assert_command_integer(&processor, "DEL mystream", 1);
    assert_command_response(&processor, "XADD mystream 1-0 f v1", b"$3\r\n1-0\r\n");
    assert_command_response(&processor, "XADD mystream 2-0 f v2", b"$3\r\n2-0\r\n");
    assert_command_response(&processor, "XGROUP CREATE mystream group1 0", b"+OK\r\n");
    execute_command_line(
        &processor,
        "XREADGROUP GROUP group1 consumer1 COUNT 1 STREAMS mystream >",
    )
    .unwrap();
    let zero_idle = execute_command_line(
        &processor,
        "XREADGROUP GROUP group1 consumer1 CLAIM 0 STREAMS mystream >",
    )
    .unwrap();
    let (_, zero_idle_messages) =
        parse_xreadgroup_maybe_claim_single_stream_response(&zero_idle).unwrap();
    assert_eq!(zero_idle_messages.len(), 2);

    assert_command_integer(&processor, "DEL mystream", 1);
    assert_command_response(&processor, "XADD mystream 1-0 f v1", b"$3\r\n1-0\r\n");
    assert_command_response(&processor, "XADD mystream 2-0 f v2", b"$3\r\n2-0\r\n");
    assert_command_response(&processor, "XGROUP CREATE mystream group1 0", b"+OK\r\n");
    execute_command_line(
        &processor,
        "XREADGROUP GROUP group1 consumer1 COUNT 1 STREAMS mystream >",
    )
    .unwrap();
    thread::sleep(Duration::from_millis(100));
    let huge_idle = execute_command_line(
        &processor,
        "XREADGROUP GROUP group1 consumer1 CLAIM 9223372036854775807 STREAMS mystream >",
    )
    .unwrap();
    let (_, huge_idle_messages) =
        parse_xreadgroup_maybe_claim_single_stream_response(&huge_idle).unwrap();
    assert_eq!(huge_idle_messages.len(), 1);
    assert_eq!(huge_idle_messages[0].0, b"2-0".to_vec());
    assert_eq!(huge_idle_messages[0].3, Some(0));

    assert_command_error(
        &processor,
        "XREADGROUP GROUP group1 consumer1 CLAIM test STREAMS mystream >",
        b"-ERR min-idle-time is not an integer\r\n",
    );
    assert_command_error(
        &processor,
        "XREADGROUP GROUP group1 consumer1 CLAIM +10 STREAMS mystream >",
        b"-ERR min-idle-time is not an integer\r\n",
    );
    assert_command_error(
        &processor,
        "XREADGROUP GROUP group1 consumer1 CLAIM -10 STREAMS mystream >",
        b"-ERR min-idle-time must be a positive integer\r\n",
    );
    assert_command_error(
        &processor,
        "XREADGROUP GROUP group1 consumer1 CLAIM -0 STREAMS mystream >",
        b"-ERR min-idle-time is not an integer\r\n",
    );

    assert_command_integer(&processor, "DEL mystream", 1);
    execute_command_line(&processor, "XADD mystream 1-0 f v1").unwrap();
    execute_command_line(&processor, "XADD mystream 2-0 f v2").unwrap();
    execute_command_line(&processor, "XGROUP CREATE mystream group1 0").unwrap();
    execute_command_line(
        &processor,
        "XREADGROUP GROUP group1 consumer1 COUNT 1 STREAMS mystream >",
    )
    .unwrap();
    thread::sleep(Duration::from_millis(100));
    let count_before_claim = execute_command_line(
        &processor,
        "XREADGROUP GROUP group1 consumer1 COUNT 1 CLAIM 50 STREAMS mystream >",
    )
    .unwrap();
    let (_, count_before_claim_messages) =
        parse_xreadgroup_maybe_claim_single_stream_response(&count_before_claim).unwrap();
    assert_eq!(count_before_claim_messages.len(), 1);
    assert_eq!(count_before_claim_messages[0].0, b"1-0".to_vec());
    thread::sleep(Duration::from_millis(100));
    let claim_before_count = execute_command_line(
        &processor,
        "XREADGROUP GROUP group1 consumer1 CLAIM 50 COUNT 1 STREAMS mystream >",
    )
    .unwrap();
    let (_, claim_before_count_messages) =
        parse_xreadgroup_maybe_claim_single_stream_response(&claim_before_count).unwrap();
    assert_eq!(claim_before_count_messages.len(), 1);
    assert_eq!(claim_before_count_messages[0].3, Some(2));
    thread::sleep(Duration::from_millis(100));
    let multiple_claims = execute_command_line(
        &processor,
        "XREADGROUP GROUP group1 consumer1 CLAIM 50 COUNT 1 CLAIM 60 STREAMS mystream >",
    )
    .unwrap();
    let (_, multiple_claim_messages) =
        parse_xreadgroup_maybe_claim_single_stream_response(&multiple_claims).unwrap();
    assert_eq!(multiple_claim_messages.len(), 1);
    assert_eq!(multiple_claim_messages[0].3, Some(3));
    thread::sleep(Duration::from_millis(100));
    let claim_before_group = execute_command_line(
        &processor,
        "XREADGROUP CLAIM 50 GROUP group1 consumer1 COUNT 1 STREAMS mystream >",
    )
    .unwrap();
    let (_, claim_before_group_messages) =
        parse_xreadgroup_maybe_claim_single_stream_response(&claim_before_group).unwrap();
    assert_eq!(claim_before_group_messages.len(), 1);
    assert_eq!(claim_before_group_messages[0].3, Some(4));

    assert_command_integer(&processor, "DEL mystream", 1);
    execute_command_line(&processor, "XADD mystream 1-0 f v1").unwrap();
    execute_command_line(&processor, "XADD mystream 2-0 f v2").unwrap();
    execute_command_line(&processor, "XADD mystream 3-0 f v3").unwrap();
    execute_command_line(&processor, "XGROUP CREATE mystream group1 0").unwrap();
    execute_command_line(
        &processor,
        "XREADGROUP GROUP group1 consumer1 STREAMS mystream >",
    )
    .unwrap();
    thread::sleep(Duration::from_millis(100));
    let claim_ignored_for_history = execute_command_line(
        &processor,
        "XREADGROUP GROUP group1 consumer1 CLAIM 1000 STREAMS mystream 0",
    )
    .unwrap();
    let (_, history_messages) =
        parse_xreadgroup_maybe_claim_single_stream_response(&claim_ignored_for_history).unwrap();
    assert_eq!(history_messages.len(), 3);
    assert!(history_messages.iter().all(|entry| entry.2.is_none()));

    assert_command_error(
        &processor,
        "XREADGROUP GROUP not_existing_group consumer1 CLAIM 50 STREAMS mystream >",
        b"-NOGROUP No such key or consumer group\r\n",
    );
    thread::sleep(Duration::from_millis(100));
    let created_consumer = execute_command_line(
        &processor,
        "XREADGROUP GROUP group1 consumer2 CLAIM 50 STREAMS mystream >",
    )
    .unwrap();
    let (_, created_consumer_messages) =
        parse_xreadgroup_maybe_claim_single_stream_response(&created_consumer).unwrap();
    assert_eq!(created_consumer_messages.len(), 3);
}

#[test]
fn xreadgroup_claim_ownership_and_delivery_count_match_stream_cgroups_external_scenarios() {
    let processor = RequestProcessor::new().unwrap();

    // Redis tests/unit/type/stream-cgroups.tcl:
    // - "XREADGROUP CLAIM verify ownership transfer and delivery count updates"
    // - "XREADGROUP CLAIM verify XACK removes messages from CLAIM pool"
    // - "XREADGROUP CLAIM verify that XCLAIM updates delivery count"
    // - "XREADGROUP CLAIM verify forced entries are claimable"
    assert_command_integer(&processor, "DEL mystream", 0);
    execute_command_line(&processor, "XADD mystream 1-0 f v1").unwrap();
    execute_command_line(&processor, "XADD mystream 2-0 f v2").unwrap();
    execute_command_line(&processor, "XADD mystream 3-0 f v3").unwrap();
    execute_command_line(&processor, "XGROUP CREATE mystream group1 0").unwrap();
    execute_command_line(
        &processor,
        "XREADGROUP GROUP group1 consumer1 STREAMS mystream >",
    )
    .unwrap();
    thread::sleep(Duration::from_millis(100));
    let transfer = execute_command_line(
        &processor,
        "XREADGROUP GROUP group1 consumer2 CLAIM 50 STREAMS mystream >",
    )
    .unwrap();
    let (_, transfer_messages) =
        parse_xreadgroup_maybe_claim_single_stream_response(&transfer).unwrap();
    assert_eq!(transfer_messages.len(), 3);
    let transfer_pending = parse_xpending_detail_response(
        &execute_command_line(&processor, "XPENDING mystream group1 - + 10").unwrap(),
    );
    assert_eq!(transfer_pending.len(), 3);
    assert!(
        transfer_pending
            .iter()
            .all(|entry| entry.1 == b"consumer2".to_vec())
    );
    assert!(transfer_pending.iter().all(|entry| entry.3 == 2));

    assert_command_integer(&processor, "DEL mystream", 1);
    execute_command_line(&processor, "XADD mystream 1-0 f v1").unwrap();
    execute_command_line(&processor, "XADD mystream 2-0 f v2").unwrap();
    execute_command_line(&processor, "XADD mystream 3-0 f v3").unwrap();
    execute_command_line(&processor, "XGROUP CREATE mystream group1 0").unwrap();
    execute_command_line(
        &processor,
        "XREADGROUP GROUP group1 consumer1 STREAMS mystream >",
    )
    .unwrap();
    execute_command_line(&processor, "XACK mystream group1 1-0 3-0").unwrap();
    thread::sleep(Duration::from_millis(100));
    let after_ack = execute_command_line(
        &processor,
        "XREADGROUP GROUP group1 consumer2 CLAIM 50 STREAMS mystream >",
    )
    .unwrap();
    let (_, after_ack_messages) =
        parse_xreadgroup_maybe_claim_single_stream_response(&after_ack).unwrap();
    assert_eq!(after_ack_messages.len(), 1);
    assert_eq!(after_ack_messages[0].0, b"2-0".to_vec());
    execute_command_line(&processor, "XACK mystream group1 2-0").unwrap();
    let after_ack_empty = execute_command_line(
        &processor,
        "XREADGROUP GROUP group1 consumer2 CLAIM 50 STREAMS mystream >",
    )
    .unwrap();
    assert_eq!(parse_bulk_array_payloads(&after_ack_empty).len(), 0);

    assert_command_integer(&processor, "DEL mystream", 1);
    execute_command_line(&processor, "XADD mystream 1-0 f v1").unwrap();
    execute_command_line(&processor, "XADD mystream 2-0 f v2").unwrap();
    execute_command_line(&processor, "XADD mystream 3-0 f v3").unwrap();
    execute_command_line(&processor, "XGROUP CREATE mystream group1 0").unwrap();
    execute_command_line(
        &processor,
        "XREADGROUP GROUP group1 consumer1 STREAMS mystream >",
    )
    .unwrap();
    thread::sleep(Duration::from_millis(100));
    execute_command_line(&processor, "XCLAIM mystream group1 consumer3 50 2-0 3-0").unwrap();
    thread::sleep(Duration::from_millis(100));
    let after_xclaim = execute_command_line(
        &processor,
        "XREADGROUP GROUP group1 consumer2 CLAIM 50 STREAMS mystream >",
    )
    .unwrap();
    let (_, after_xclaim_messages) =
        parse_xreadgroup_maybe_claim_single_stream_response(&after_xclaim).unwrap();
    assert_eq!(after_xclaim_messages.len(), 3);
    assert_eq!(after_xclaim_messages[0].0, b"1-0".to_vec());
    assert_eq!(after_xclaim_messages[0].3, Some(1));
    assert_eq!(after_xclaim_messages[1].0, b"2-0".to_vec());
    assert_eq!(after_xclaim_messages[1].3, Some(2));
    assert_eq!(after_xclaim_messages[2].0, b"3-0".to_vec());
    assert_eq!(after_xclaim_messages[2].3, Some(2));

    assert_command_integer(&processor, "DEL mystream", 1);
    execute_command_line(&processor, "XADD mystream 1-0 f v1").unwrap();
    execute_command_line(&processor, "XADD mystream 2-0 f v2").unwrap();
    execute_command_line(&processor, "XADD mystream 3-0 f v3").unwrap();
    execute_command_line(&processor, "XGROUP CREATE mystream group1 0").unwrap();
    execute_command_line(
        &processor,
        "XCLAIM mystream group1 consumer3 0 1-0 2-0 FORCE JUSTID",
    )
    .unwrap();
    let forced = execute_command_line(
        &processor,
        "XREADGROUP GROUP group1 consumer2 CLAIM 0 COUNT 2 STREAMS mystream >",
    )
    .unwrap();
    let (_, forced_messages) =
        parse_xreadgroup_maybe_claim_single_stream_response(&forced).unwrap();
    assert_eq!(forced_messages.len(), 2);
    assert_eq!(forced_messages[0].0, b"1-0".to_vec());
    assert_eq!(forced_messages[0].3, Some(1));
    assert_eq!(forced_messages[1].0, b"2-0".to_vec());
    assert_eq!(forced_messages[1].3, Some(1));
}

#[test]
fn xreadgroup_claim_pending_cleanup_and_setid_edge_cases_match_stream_cgroups_external_scenarios() {
    let processor = RequestProcessor::new().unwrap();

    // Redis tests/unit/type/stream-cgroups.tcl:
    // - "XREADGROUP CLAIM after consumer deleted with pending messages"
    // - "XREADGROUP CLAIM after XGROUP SETID moves past pending messages"
    // - "XREADGROUP CLAIM after XGROUP SETID moves before pending messages"
    // - "XREADGROUP CLAIM when pending messages get trimmed"
    assert_command_integer(&processor, "DEL mystream", 0);
    execute_command_line(&processor, "XADD mystream 1-0 f v1").unwrap();
    execute_command_line(&processor, "XGROUP CREATE mystream group1 0").unwrap();
    execute_command_line(
        &processor,
        "XREADGROUP GROUP group1 consumer1 STREAMS mystream >",
    )
    .unwrap();
    assert_command_integer(
        &processor,
        "XGROUP DELCONSUMER mystream group1 consumer1",
        1,
    );
    assert!(
        parse_xpending_detail_response(
            &execute_command_line(&processor, "XPENDING mystream group1 - + 10").unwrap(),
        )
        .is_empty()
    );
    thread::sleep(Duration::from_millis(100));
    let deleted_consumer_claim = execute_command_line(
        &processor,
        "XREADGROUP GROUP group1 consumer2 CLAIM 50 STREAMS mystream >",
    )
    .unwrap();
    assert_eq!(parse_bulk_array_payloads(&deleted_consumer_claim).len(), 0);

    assert_command_integer(&processor, "DEL mystream", 1);
    execute_command_line(&processor, "XADD mystream 1-0 f v1").unwrap();
    execute_command_line(&processor, "XADD mystream 2-0 f v2").unwrap();
    execute_command_line(&processor, "XGROUP CREATE mystream group1 0").unwrap();
    execute_command_line(
        &processor,
        "XREADGROUP GROUP group1 consumer1 STREAMS mystream >",
    )
    .unwrap();
    execute_command_line(&processor, "XGROUP SETID mystream group1 2-0").unwrap();
    thread::sleep(Duration::from_millis(100));
    let moved_past = execute_command_line(
        &processor,
        "XREADGROUP GROUP group1 consumer2 CLAIM 50 STREAMS mystream >",
    )
    .unwrap();
    let (_, moved_past_messages) =
        parse_xreadgroup_maybe_claim_single_stream_response(&moved_past).unwrap();
    assert_eq!(moved_past_messages.len(), 2);

    assert_command_integer(&processor, "DEL mystream", 1);
    execute_command_line(&processor, "XADD mystream 1-0 f v1").unwrap();
    execute_command_line(&processor, "XADD mystream 2-0 f v2").unwrap();
    execute_command_line(&processor, "XGROUP CREATE mystream group1 0").unwrap();
    execute_command_line(
        &processor,
        "XREADGROUP GROUP group1 consumer1 STREAMS mystream >",
    )
    .unwrap();
    execute_command_line(
        &processor,
        "XREADGROUP GROUP group1 consumer2 CLAIM 0 STREAMS mystream >",
    )
    .unwrap();
    execute_command_line(&processor, "XGROUP SETID mystream group1 0").unwrap();
    thread::sleep(Duration::from_millis(100));
    let moved_before = execute_command_line(
        &processor,
        "XREADGROUP GROUP group1 consumer2 CLAIM 50 STREAMS mystream >",
    )
    .unwrap();
    let (_, moved_before_messages) =
        parse_xreadgroup_maybe_claim_single_stream_response(&moved_before).unwrap();
    assert_eq!(moved_before_messages.len(), 4);
    assert_eq!(moved_before_messages[0].0, b"1-0".to_vec());
    assert_eq!(moved_before_messages[0].3, Some(2));
    assert_eq!(moved_before_messages[1].0, b"2-0".to_vec());
    assert_eq!(moved_before_messages[1].3, Some(2));
    assert_eq!(moved_before_messages[2].0, b"1-0".to_vec());
    assert_eq!(moved_before_messages[2].3, Some(0));
    assert_eq!(moved_before_messages[3].0, b"2-0".to_vec());
    assert_eq!(moved_before_messages[3].3, Some(0));
    thread::sleep(Duration::from_millis(100));
    let moved_before_again = execute_command_line(
        &processor,
        "XREADGROUP GROUP group1 consumer2 CLAIM 50 STREAMS mystream >",
    )
    .unwrap();
    let (_, moved_before_again_messages) =
        parse_xreadgroup_maybe_claim_single_stream_response(&moved_before_again).unwrap();
    assert_eq!(moved_before_again_messages.len(), 2);
    assert_eq!(moved_before_again_messages[0].0, b"1-0".to_vec());
    assert_eq!(moved_before_again_messages[0].3, Some(1));
    assert_eq!(moved_before_again_messages[1].0, b"2-0".to_vec());
    assert_eq!(moved_before_again_messages[1].3, Some(1));

    assert_command_integer(&processor, "DEL mystream", 1);
    execute_command_line(&processor, "XADD mystream 1-0 f v1").unwrap();
    execute_command_line(&processor, "XADD mystream 2-0 f v2").unwrap();
    execute_command_line(&processor, "XADD mystream 3-0 f v3").unwrap();
    execute_command_line(&processor, "XGROUP CREATE mystream group1 0").unwrap();
    execute_command_line(
        &processor,
        "XREADGROUP GROUP group1 consumer1 STREAMS mystream >",
    )
    .unwrap();
    assert_command_integer(&processor, "XTRIM mystream MAXLEN 0", 3);
    thread::sleep(Duration::from_millis(100));
    let trimmed = execute_command_line(
        &processor,
        "XREADGROUP GROUP group1 consumer2 CLAIM 50 STREAMS mystream >",
    )
    .unwrap();
    assert_eq!(parse_bulk_array_payloads(&trimmed).len(), 0);
}

#[test]
fn xinfo_full_matches_stream_cgroups_external_scenario() {
    let processor = RequestProcessor::new().unwrap();

    assert_command_integer(&processor, "DEL x", 0);
    assert_command_response(&processor, "XADD x 100 a 1", b"$5\r\n100-0\r\n");
    assert_command_response(&processor, "XADD x 101 b 1", b"$5\r\n101-0\r\n");
    assert_command_response(&processor, "XADD x 102 c 1", b"$5\r\n102-0\r\n");
    assert_command_response(&processor, "XADD x 103 e 1", b"$5\r\n103-0\r\n");
    assert_command_response(&processor, "XADD x 104 f 1", b"$5\r\n104-0\r\n");
    assert_command_response(&processor, "XGROUP CREATE x g1 0", b"+OK\r\n");
    assert_command_response(&processor, "XGROUP CREATE x g2 0", b"+OK\r\n");
    execute_command_line(&processor, "XREADGROUP GROUP g1 Alice COUNT 1 STREAMS x >").unwrap();
    execute_command_line(&processor, "XREADGROUP GROUP g1 Bob COUNT 1 STREAMS x >").unwrap();
    execute_command_line(
        &processor,
        "XREADGROUP GROUP g1 Bob NOACK COUNT 1 STREAMS x >",
    )
    .unwrap();
    execute_command_line(
        &processor,
        "XREADGROUP GROUP g2 Charlie COUNT 4 STREAMS x >",
    )
    .unwrap();
    assert_command_integer(&processor, "XDEL x 103", 1);

    let full =
        parse_resp_test_value(&execute_command_line(&processor, "XINFO STREAM x FULL").unwrap());
    let full_items = resp_test_array(&full);
    assert_eq!(full_items.len(), 30);
    let full_map = resp_test_flat_map(&full);
    assert_eq!(resp_test_integer(full_map[&b"length".to_vec()]), 4);
    assert_eq!(
        resp_test_array(full_map[&b"entries".to_vec()])
            .iter()
            .map(|entry| resp_test_bulk(&resp_test_array(entry)[0]).to_vec())
            .collect::<Vec<_>>(),
        vec![
            b"100-0".to_vec(),
            b"101-0".to_vec(),
            b"102-0".to_vec(),
            b"104-0".to_vec(),
        ]
    );

    let groups = resp_test_array(full_map[&b"groups".to_vec()]);
    assert_eq!(groups.len(), 2);

    let g1 = resp_test_flat_map(&groups[0]);
    assert_eq!(resp_test_bulk(g1[&b"name".to_vec()]), b"g1");
    let g1_pending = resp_test_array(g1[&b"pending".to_vec()]);
    assert_eq!(
        resp_test_bulk(&resp_test_array(&g1_pending[0])[0]),
        b"100-0"
    );
    let g1_consumers = resp_test_array(g1[&b"consumers".to_vec()]);
    let alice = resp_test_flat_map(&g1_consumers[0]);
    assert_eq!(resp_test_bulk(alice[&b"name".to_vec()]), b"Alice");
    let alice_pending = resp_test_array(alice[&b"pending".to_vec()]);
    assert_eq!(
        resp_test_bulk(&resp_test_array(&alice_pending[0])[0]),
        b"100-0"
    );

    let g2 = resp_test_flat_map(&groups[1]);
    assert_eq!(resp_test_bulk(g2[&b"name".to_vec()]), b"g2");
    let g2_consumers = resp_test_array(g2[&b"consumers".to_vec()]);
    let charlie = resp_test_flat_map(&g2_consumers[0]);
    assert_eq!(resp_test_bulk(charlie[&b"name".to_vec()]), b"Charlie");
    let charlie_pending = resp_test_array(charlie[&b"pending".to_vec()]);
    assert_eq!(
        resp_test_bulk(&resp_test_array(&charlie_pending[0])[0]),
        b"100-0"
    );
    assert_eq!(
        resp_test_bulk(&resp_test_array(&charlie_pending[1])[0]),
        b"101-0"
    );

    let limited = parse_resp_test_value(
        &execute_command_line(&processor, "XINFO STREAM x FULL COUNT 1").unwrap(),
    );
    let limited_items = resp_test_array(&limited);
    assert_eq!(limited_items.len(), 30);
    let limited_map = resp_test_flat_map(&limited);
    let limited_entries = resp_test_array(limited_map[&b"entries".to_vec()]);
    assert_eq!(limited_entries.len(), 1);
    assert_eq!(
        resp_test_bulk(&resp_test_array(&limited_entries[0])[0]),
        b"100-0"
    );
}

#[test]
fn consumer_group_read_counter_and_lag_sanity_matches_stream_cgroups_external_scenario() {
    let processor = RequestProcessor::new().unwrap();

    assert_command_integer(&processor, "DEL x", 0);
    for (id, value) in [
        ("1-0", "a"),
        ("2-0", "b"),
        ("3-0", "c"),
        ("4-0", "d"),
        ("5-0", "e"),
    ] {
        let command = format!("XADD x {id} data {value}");
        execute_command_line(&processor, &command).unwrap();
    }
    assert_command_response(&processor, "XGROUP CREATE x g1 0", b"+OK\r\n");

    let full =
        parse_resp_test_value(&execute_command_line(&processor, "XINFO STREAM x FULL").unwrap());
    let full_map = resp_test_flat_map(&full);
    let group = xinfo_full_group_by_name(&full_map, b"g1");
    assert!(resp_test_is_null(group[&b"entries-read".to_vec()]));
    assert_eq!(resp_test_integer(group[&b"lag".to_vec()]), 5);

    execute_command_line(&processor, "XREADGROUP GROUP g1 c11 COUNT 1 STREAMS x >").unwrap();
    let full =
        parse_resp_test_value(&execute_command_line(&processor, "XINFO STREAM x FULL").unwrap());
    let full_map = resp_test_flat_map(&full);
    let group = xinfo_full_group_by_name(&full_map, b"g1");
    assert_eq!(resp_test_integer(group[&b"entries-read".to_vec()]), 1);
    assert_eq!(resp_test_integer(group[&b"lag".to_vec()]), 4);

    execute_command_line(&processor, "XREADGROUP GROUP g1 c12 COUNT 10 STREAMS x >").unwrap();
    let full =
        parse_resp_test_value(&execute_command_line(&processor, "XINFO STREAM x FULL").unwrap());
    let full_map = resp_test_flat_map(&full);
    let group = xinfo_full_group_by_name(&full_map, b"g1");
    assert_eq!(resp_test_integer(group[&b"entries-read".to_vec()]), 5);
    assert_eq!(resp_test_integer(group[&b"lag".to_vec()]), 0);

    execute_command_line(&processor, "XADD x 6-0 data f").unwrap();
    let full =
        parse_resp_test_value(&execute_command_line(&processor, "XINFO STREAM x FULL").unwrap());
    let full_map = resp_test_flat_map(&full);
    let group = xinfo_full_group_by_name(&full_map, b"g1");
    assert_eq!(resp_test_integer(group[&b"entries-read".to_vec()]), 5);
    assert_eq!(resp_test_integer(group[&b"lag".to_vec()]), 1);
}

#[test]
fn consumer_group_lag_with_xdels_matches_stream_cgroups_external_scenario() {
    let processor = RequestProcessor::new().unwrap();

    assert_command_integer(&processor, "DEL x", 0);
    for (id, value) in [
        ("1-0", "a"),
        ("2-0", "b"),
        ("3-0", "c"),
        ("4-0", "d"),
        ("5-0", "e"),
    ] {
        let command = format!("XADD x {id} data {value}");
        execute_command_line(&processor, &command).unwrap();
    }
    assert_command_integer(&processor, "XDEL x 3-0", 1);
    assert_command_response(&processor, "XGROUP CREATE x g1 0", b"+OK\r\n");
    assert_command_response(&processor, "XGROUP CREATE x g2 0", b"+OK\r\n");

    for _ in 0..3 {
        execute_command_line(&processor, "XREADGROUP GROUP g1 c11 COUNT 1 STREAMS x >").unwrap();
        let full = parse_resp_test_value(
            &execute_command_line(&processor, "XINFO STREAM x FULL").unwrap(),
        );
        let full_map = resp_test_flat_map(&full);
        let group = xinfo_full_group_by_name(&full_map, b"g1");
        assert!(resp_test_is_null(group[&b"entries-read".to_vec()]));
        assert!(
            resp_test_is_null(group[&b"lag".to_vec()]),
            "expected null lag, got {:?}; group={group:?}; max_deleted={:?}; entries_added={:?}; length={:?}; last_generated={:?}",
            group[&b"lag".to_vec()],
            full_map[&b"max-deleted-entry-id".to_vec()],
            full_map[&b"entries-added".to_vec()],
            full_map[&b"length".to_vec()],
            full_map[&b"last-generated-id".to_vec()]
        );
    }

    execute_command_line(&processor, "XREADGROUP GROUP g1 c11 COUNT 1 STREAMS x >").unwrap();
    let full =
        parse_resp_test_value(&execute_command_line(&processor, "XINFO STREAM x FULL").unwrap());
    let full_map = resp_test_flat_map(&full);
    let g1 = xinfo_full_group_by_name(&full_map, b"g1");
    assert_eq!(resp_test_integer(g1[&b"entries-read".to_vec()]), 5);
    assert_eq!(resp_test_integer(g1[&b"lag".to_vec()]), 0);

    execute_command_line(&processor, "XADD x 6-0 data f").unwrap();
    let full =
        parse_resp_test_value(&execute_command_line(&processor, "XINFO STREAM x FULL").unwrap());
    let full_map = resp_test_flat_map(&full);
    let g1 = xinfo_full_group_by_name(&full_map, b"g1");
    assert_eq!(resp_test_integer(g1[&b"entries-read".to_vec()]), 5);
    assert_eq!(resp_test_integer(g1[&b"lag".to_vec()]), 1);

    assert_command_integer(&processor, "XTRIM x MINID = 3-0", 2);
    let full =
        parse_resp_test_value(&execute_command_line(&processor, "XINFO STREAM x FULL").unwrap());
    let full_map = resp_test_flat_map(&full);
    let g1 = xinfo_full_group_by_name(&full_map, b"g1");
    let g2 = xinfo_full_group_by_name(&full_map, b"g2");
    assert_eq!(resp_test_integer(g1[&b"entries-read".to_vec()]), 5);
    assert_eq!(resp_test_integer(g1[&b"lag".to_vec()]), 1);
    assert!(resp_test_is_null(g2[&b"entries-read".to_vec()]));
    assert_eq!(resp_test_integer(g2[&b"lag".to_vec()]), 3);

    assert_command_integer(&processor, "XTRIM x MINID = 5-0", 1);
    let full =
        parse_resp_test_value(&execute_command_line(&processor, "XINFO STREAM x FULL").unwrap());
    let full_map = resp_test_flat_map(&full);
    let g1 = xinfo_full_group_by_name(&full_map, b"g1");
    let g2 = xinfo_full_group_by_name(&full_map, b"g2");
    assert_eq!(resp_test_integer(g1[&b"entries-read".to_vec()]), 5);
    assert_eq!(resp_test_integer(g1[&b"lag".to_vec()]), 1);
    assert!(resp_test_is_null(g2[&b"entries-read".to_vec()]));
    assert_eq!(resp_test_integer(g2[&b"lag".to_vec()]), 2);
}

#[test]
fn consumer_group_lag_with_tombstone_after_last_id_matches_stream_cgroups_external_scenario() {
    let processor = RequestProcessor::new().unwrap();

    assert_command_integer(&processor, "DEL x", 0);
    assert_command_response(&processor, "XGROUP CREATE x g1 $ MKSTREAM", b"+OK\r\n");
    execute_command_line(&processor, "XADD x 1-0 data a").unwrap();
    execute_command_line(&processor, "XREADGROUP GROUP g1 alice STREAMS x >").unwrap();
    execute_command_line(&processor, "XADD x 2-0 data c").unwrap();
    execute_command_line(&processor, "XADD x 3-0 data d").unwrap();
    assert_command_integer(&processor, "XDEL x 2-0", 1);

    let full =
        parse_resp_test_value(&execute_command_line(&processor, "XINFO STREAM x FULL").unwrap());
    let full_map = resp_test_flat_map(&full);
    let group = xinfo_full_group_by_name(&full_map, b"g1");
    assert_eq!(resp_test_integer(group[&b"entries-read".to_vec()]), 1);
    assert!(
        resp_test_is_null(group[&b"lag".to_vec()]),
        "expected null lag, got {:?}; group={group:?}",
        group[&b"lag".to_vec()]
    );

    assert_command_integer(&processor, "XDEL x 1-0", 1);
    let full =
        parse_resp_test_value(&execute_command_line(&processor, "XINFO STREAM x FULL").unwrap());
    let full_map = resp_test_flat_map(&full);
    let group = xinfo_full_group_by_name(&full_map, b"g1");
    assert_eq!(resp_test_integer(group[&b"entries-read".to_vec()]), 1);
    assert_eq!(resp_test_integer(group[&b"lag".to_vec()]), 1);

    execute_command_line(&processor, "XREADGROUP GROUP g1 alice STREAMS x >").unwrap();
    let full =
        parse_resp_test_value(&execute_command_line(&processor, "XINFO STREAM x FULL").unwrap());
    let full_map = resp_test_flat_map(&full);
    let group = xinfo_full_group_by_name(&full_map, b"g1");
    assert_eq!(resp_test_integer(group[&b"entries-read".to_vec()]), 3);
    assert_eq!(resp_test_integer(group[&b"lag".to_vec()]), 0);
}

#[test]
fn consumer_group_lag_with_xtrim_matches_stream_cgroups_external_scenario() {
    let processor = RequestProcessor::new().unwrap();

    assert_command_integer(&processor, "DEL x", 0);
    assert_command_response(&processor, "XGROUP CREATE x mygroup $ MKSTREAM", b"+OK\r\n");
    for (id, value) in [
        ("1-0", "a"),
        ("2-0", "b"),
        ("3-0", "c"),
        ("4-0", "d"),
        ("5-0", "e"),
    ] {
        let command = format!("XADD x {id} data {value}");
        execute_command_line(&processor, &command).unwrap();
    }
    execute_command_line(
        &processor,
        "XREADGROUP GROUP mygroup alice COUNT 1 STREAMS x >",
    )
    .unwrap();

    let full =
        parse_resp_test_value(&execute_command_line(&processor, "XINFO STREAM x FULL").unwrap());
    let full_map = resp_test_flat_map(&full);
    let group = xinfo_full_group_by_name(&full_map, b"mygroup");
    assert_eq!(resp_test_integer(group[&b"entries-read".to_vec()]), 1);
    assert_eq!(resp_test_integer(group[&b"lag".to_vec()]), 4);

    assert_command_integer(&processor, "XTRIM x MAXLEN 1", 4);
    let full =
        parse_resp_test_value(&execute_command_line(&processor, "XINFO STREAM x FULL").unwrap());
    let full_map = resp_test_flat_map(&full);
    assert_eq!(
        resp_test_bulk(full_map[&b"max-deleted-entry-id".to_vec()]),
        b"0-0"
    );
    let group = xinfo_full_group_by_name(&full_map, b"mygroup");
    assert_eq!(resp_test_integer(group[&b"entries-read".to_vec()]), 1);
    assert_eq!(resp_test_integer(group[&b"lag".to_vec()]), 1);

    execute_command_line(&processor, "XREADGROUP GROUP mygroup alice STREAMS x >").unwrap();
    let full =
        parse_resp_test_value(&execute_command_line(&processor, "XINFO STREAM x FULL").unwrap());
    let full_map = resp_test_flat_map(&full);
    let group = xinfo_full_group_by_name(&full_map, b"mygroup");
    assert_eq!(resp_test_integer(group[&b"entries-read".to_vec()]), 5);
    assert_eq!(resp_test_integer(group[&b"lag".to_vec()]), 0);

    execute_command_line(&processor, "XADD x 6-0 data f").unwrap();
    let full =
        parse_resp_test_value(&execute_command_line(&processor, "XINFO STREAM x FULL").unwrap());
    let full_map = resp_test_flat_map(&full);
    let group = xinfo_full_group_by_name(&full_map, b"mygroup");
    assert_eq!(resp_test_integer(group[&b"entries-read".to_vec()]), 5);
    assert_eq!(resp_test_integer(group[&b"lag".to_vec()]), 1);

    assert_command_integer(&processor, "XTRIM x MAXLEN 0", 2);
    let full =
        parse_resp_test_value(&execute_command_line(&processor, "XINFO STREAM x FULL").unwrap());
    let full_map = resp_test_flat_map(&full);
    let group = xinfo_full_group_by_name(&full_map, b"mygroup");
    assert_eq!(resp_test_integer(group[&b"lag".to_vec()]), 0);
}

#[test]
fn memory_usage_accepts_samples_option() {
    let processor = RequestProcessor::new().unwrap();

    // Set up a key
    execute_command_line(&processor, "SET mykey hello").unwrap();

    // MEMORY USAGE without SAMPLES
    let usage = execute_command_line(&processor, "MEMORY USAGE mykey").unwrap();
    assert!(
        usage.starts_with(b":"),
        "MEMORY USAGE should return integer"
    );

    // MEMORY USAGE with SAMPLES option (ignored but accepted)
    let usage_samples = execute_command_line(&processor, "MEMORY USAGE mykey SAMPLES 5").unwrap();
    assert!(
        usage_samples.starts_with(b":"),
        "MEMORY USAGE SAMPLES should return integer"
    );

    // MEMORY USAGE with SAMPLES 0
    let usage_samples_0 = execute_command_line(&processor, "MEMORY USAGE mykey SAMPLES 0").unwrap();
    assert!(
        usage_samples_0.starts_with(b":"),
        "MEMORY USAGE SAMPLES 0 should return integer"
    );

    // MEMORY USAGE with invalid SAMPLES count
    let usage_bad = execute_command_line(&processor, "MEMORY USAGE mykey SAMPLES abc");
    assert!(usage_bad.is_err(), "MEMORY USAGE SAMPLES abc should fail");

    // MEMORY USAGE with wrong option name
    let usage_wrong = execute_command_line(&processor, "MEMORY USAGE mykey BADOPT 5");
    assert!(
        usage_wrong.is_err(),
        "MEMORY USAGE with bad option should fail"
    );
}

#[test]
fn memory_usage_string_bounds_match_external_scenario() {
    let processor = RequestProcessor::new().unwrap();
    let sizes = [
        1usize, 5, 8, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129, 255, 256, 257,
    ];
    let header_size = if cfg!(target_pointer_width = "32") {
        12usize
    } else {
        16usize
    };

    for key_size in sizes {
        let key = "k".repeat(key_size);

        for value_size in sizes {
            let value = "v".repeat(value_size);
            execute_frame(
                &processor,
                &encode_resp(&[b"SET", key.as_bytes(), value.as_bytes()]),
            );
            let memory_used = parse_integer_response(&execute_frame(
                &processor,
                &encode_resp(&[b"MEMORY", b"USAGE", key.as_bytes()]),
            ));
            let min = i64::try_from(header_size + key_size + value_size).unwrap();
            assert!(
                memory_used >= min,
                "MEMORY USAGE below Redis lower bound for key_size={key_size} value_size={value_size}: got {memory_used}, min {min}"
            );
            let max = if min < 32 { 64 } else { min * 2 };
            assert!(
                memory_used <= max,
                "MEMORY USAGE above Redis upper bound for key_size={key_size} value_size={value_size}: got {memory_used}, max {max}"
            );
        }

        for value in ["1", "100", "10000", "10000000"] {
            execute_frame(
                &processor,
                &encode_resp(&[b"SET", key.as_bytes(), value.as_bytes()]),
            );
            let memory_used = parse_integer_response(&execute_frame(
                &processor,
                &encode_resp(&[b"MEMORY", b"USAGE", key.as_bytes()]),
            ));
            let min = i64::try_from(header_size + key_size).unwrap();
            assert!(
                memory_used >= min,
                "MEMORY USAGE below Redis integer lower bound for key_size={key_size} value={value}: got {memory_used}, min {min}"
            );
        }
    }
}

#[test]
fn latency_histogram_uses_resp3_map_types() {
    let processor = RequestProcessor::new().unwrap();
    processor.record_command_call(b"set");
    processor.record_command_call(b"set");
    processor.record_command_call(b"get");

    // RESP2: flat array with key-value pairs as bulk strings.
    let resp2 = execute_frame(&processor, &encode_resp(&[b"LATENCY", b"HISTOGRAM"]));
    let resp2_text = String::from_utf8_lossy(&resp2);
    assert!(
        resp2_text.starts_with("*"),
        "RESP2 LATENCY HISTOGRAM should start with array: {resp2_text}"
    );
    assert!(
        resp2_text.contains("calls 2 histogram_usec"),
        "RESP2 SET should show calls 2: {resp2_text}"
    );

    // RESP3: top-level map, each value is a map with "calls" and "histogram_usec" keys.
    processor.set_resp_protocol_version(RespProtocolVersion::Resp3);
    let resp3 = execute_frame(&processor, &encode_resp(&[b"LATENCY", b"HISTOGRAM"]));
    let resp3_text = String::from_utf8_lossy(&resp3);
    assert!(
        resp3_text.starts_with("%"),
        "RESP3 LATENCY HISTOGRAM should start with map: {resp3_text}"
    );
    assert!(
        resp3_text.contains("%2\r\n"),
        "RESP3 each command entry should be a map with 2 keys: {resp3_text}"
    );
    assert!(
        resp3_text.contains("$5\r\ncalls\r\n"),
        "RESP3 entry should contain 'calls' key: {resp3_text}"
    );
    assert!(
        resp3_text.contains("$14\r\nhistogram_usec\r\n"),
        "RESP3 entry should contain 'histogram_usec' key: {resp3_text}"
    );
}
