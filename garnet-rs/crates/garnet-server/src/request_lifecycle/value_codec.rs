use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::VecDeque;
use std::mem::size_of;

use super::ObjectTypeTag;
use super::StreamConsumerState;
use super::StreamGroupState;
use super::StreamId;
use super::StreamIdmpProducerState;
use super::StreamObject;
use super::StreamPendingEntry;

const VALUE_EXPIRATION_PREFIX_LEN: usize = size_of::<u64>();
const SET_CONTIGUOUS_I64_RANGE_MAGIC: [u8; 4] = *b"SRNG";

#[derive(Debug, Clone, Copy)]
pub(super) struct DecodedStoredValue<'a> {
    pub(super) expiration_unix_millis: Option<u64>,
    pub(super) user_value: &'a [u8],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct DecodedObjectValue {
    pub(super) object_type: ObjectTypeTag,
    pub(super) payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct EncodedObjectValue {
    bytes: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ContiguousI64RangeSet {
    start: i64,
    end: i64,
}

impl ContiguousI64RangeSet {
    pub(super) fn new(start: i64, end: i64) -> Option<Self> {
        if start > end {
            return None;
        }
        Some(Self { start, end })
    }

    pub(super) fn start(self) -> i64 {
        self.start
    }

    pub(super) fn end(self) -> i64 {
        self.end
    }

    pub(super) fn contains(self, value: i64) -> bool {
        value >= self.start && value <= self.end
    }

    pub(super) fn can_extend_left_with(self, value: i64) -> bool {
        self.start.checked_sub(1).is_some_and(|left| left == value)
    }

    pub(super) fn can_extend_right_with(self, value: i64) -> bool {
        self.end.checked_add(1).is_some_and(|right| right == value)
    }

    pub(super) fn extend_left(&mut self) {
        self.start -= 1;
    }

    pub(super) fn extend_right(&mut self) {
        self.end += 1;
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum DecodedSetObjectPayload {
    Members(BTreeSet<Vec<u8>>),
    ContiguousI64Range(ContiguousI64RangeSet),
}

impl EncodedObjectValue {
    pub(super) fn from_parts(object_type: ObjectTypeTag, payload: &[u8]) -> Self {
        let mut bytes = Vec::with_capacity(1 + payload.len());
        object_type.write_to(&mut bytes);
        bytes.extend_from_slice(payload);
        Self { bytes }
    }

    pub(super) fn as_vec(&self) -> &Vec<u8> {
        &self.bytes
    }

    pub(super) fn as_slice(&self) -> &[u8] {
        &self.bytes
    }
}

impl DecodedObjectValue {
    pub(super) fn encode(&self) -> EncodedObjectValue {
        EncodedObjectValue::from_parts(self.object_type, &self.payload)
    }
}

pub(super) fn parse_i64_ascii(input: &[u8]) -> Option<i64> {
    if input.is_empty() {
        return None;
    }
    let text = core::str::from_utf8(input).ok()?;
    text.parse::<i64>().ok()
}

pub(super) fn parse_u64_ascii(input: &[u8]) -> Option<u64> {
    if input.is_empty() {
        return None;
    }
    let text = core::str::from_utf8(input).ok()?;
    text.parse::<u64>().ok()
}

pub(super) fn parse_f64_ascii(input: &[u8]) -> Option<f64> {
    let parsed = parse_f64_ascii_allow_non_finite(input)?;
    if !parsed.is_finite() {
        return None;
    }
    Some(parsed)
}

pub(super) fn parse_f64_ascii_allow_non_finite(input: &[u8]) -> Option<f64> {
    if input.is_empty() {
        return None;
    }
    let text = core::str::from_utf8(input).ok()?;
    if let Some(parsed) = parse_hex_f64_ascii(text) {
        return Some(parsed);
    }
    text.parse::<f64>().ok()
}

fn parse_hex_f64_ascii(text: &str) -> Option<f64> {
    let bytes = text.as_bytes();
    let mut index = 0usize;
    let mut sign = 1.0f64;
    if bytes.first() == Some(&b'+') {
        index += 1;
    } else if bytes.first() == Some(&b'-') {
        sign = -1.0;
        index += 1;
    }

    let remaining = bytes.get(index..)?;
    if remaining.len() < 3 || remaining[0] != b'0' || !matches!(remaining[1], b'x' | b'X') {
        return None;
    }
    index += 2;

    let mut significand = 0.0f64;
    let mut saw_digit = false;
    while let Some(digit) = bytes.get(index).and_then(hex_digit_value) {
        significand = significand * 16.0 + f64::from(digit);
        saw_digit = true;
        index += 1;
    }

    let mut fractional_nibbles = 0i32;
    if bytes.get(index) == Some(&b'.') {
        index += 1;
        while let Some(digit) = bytes.get(index).and_then(hex_digit_value) {
            significand = significand * 16.0 + f64::from(digit);
            fractional_nibbles += 1;
            saw_digit = true;
            index += 1;
        }
    }

    if !saw_digit {
        return None;
    }

    let exponent_marker = *bytes.get(index)?;
    if !matches!(exponent_marker, b'p' | b'P') {
        return None;
    }
    index += 1;

    let mut exponent_sign = 1i32;
    if bytes.get(index) == Some(&b'+') {
        index += 1;
    } else if bytes.get(index) == Some(&b'-') {
        exponent_sign = -1;
        index += 1;
    }

    let exponent_bytes = bytes.get(index..)?;
    if exponent_bytes.is_empty() {
        return None;
    }
    let exponent_text = core::str::from_utf8(exponent_bytes).ok()?;
    let exponent_value = exponent_text.parse::<i32>().ok()?;
    let adjusted_exponent = exponent_sign
        .checked_mul(exponent_value)?
        .checked_sub(fractional_nibbles.checked_mul(4)?)?;
    let parsed = sign * significand * 2.0f64.powi(adjusted_exponent);
    if parsed.is_nan() || !parsed.is_finite() {
        return None;
    }
    Some(parsed)
}

fn hex_digit_value(byte: &u8) -> Option<u8> {
    match *byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

pub(super) fn decode_stored_value(stored: &[u8]) -> DecodedStoredValue<'_> {
    if stored.len() < VALUE_EXPIRATION_PREFIX_LEN {
        return DecodedStoredValue {
            expiration_unix_millis: None,
            user_value: stored,
        };
    }

    let mut metadata = [0u8; VALUE_EXPIRATION_PREFIX_LEN];
    metadata.copy_from_slice(&stored[..VALUE_EXPIRATION_PREFIX_LEN]);
    let expiration_millis = u64::from_le_bytes(metadata);
    DecodedStoredValue {
        expiration_unix_millis: if expiration_millis == 0 {
            None
        } else {
            Some(expiration_millis)
        },
        user_value: &stored[VALUE_EXPIRATION_PREFIX_LEN..],
    }
}

pub(super) fn encode_stored_value(
    user_value: &[u8],
    expiration_unix_millis: Option<u64>,
) -> Vec<u8> {
    let mut stored = Vec::with_capacity(VALUE_EXPIRATION_PREFIX_LEN + user_value.len());
    stored.extend_from_slice(&expiration_unix_millis.unwrap_or(0).to_le_bytes());
    stored.extend_from_slice(user_value);
    stored
}

pub(super) fn encode_object_value(
    object_type: ObjectTypeTag,
    payload: &[u8],
) -> EncodedObjectValue {
    EncodedObjectValue::from_parts(object_type, payload)
}

pub(super) fn decode_object_value(encoded: &[u8]) -> Option<DecodedObjectValue> {
    let (&object_type, payload) = encoded.split_first()?;
    Some(DecodedObjectValue {
        object_type: ObjectTypeTag::from_u8(object_type)?,
        payload: payload.to_vec(),
    })
}

pub(super) fn serialize_hash_object_payload(hash: &BTreeMap<Vec<u8>, Vec<u8>>) -> Vec<u8> {
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&(hash.len() as u32).to_le_bytes());
    for (field, value) in hash {
        encoded.extend_from_slice(&(field.len() as u32).to_le_bytes());
        encoded.extend_from_slice(field);
        encoded.extend_from_slice(&(value.len() as u32).to_le_bytes());
        encoded.extend_from_slice(value);
    }
    encoded
}

pub(super) fn deserialize_hash_object_payload(
    encoded: &[u8],
) -> Option<BTreeMap<Vec<u8>, Vec<u8>>> {
    let mut cursor = 0usize;

    fn take_u32(encoded: &[u8], cursor: &mut usize) -> Option<u32> {
        let end = (*cursor).checked_add(size_of::<u32>())?;
        let bytes = encoded.get(*cursor..end)?;
        let mut raw = [0u8; size_of::<u32>()];
        raw.copy_from_slice(bytes);
        *cursor = end;
        Some(u32::from_le_bytes(raw))
    }

    let count = take_u32(encoded, &mut cursor)? as usize;
    let mut hash = BTreeMap::new();
    for _ in 0..count {
        let field_len = usize::try_from(take_u32(encoded, &mut cursor)?).ok()?;
        let field_end = cursor.checked_add(field_len)?;
        let field = encoded.get(cursor..field_end)?.to_vec();
        cursor = field_end;

        let value_len = usize::try_from(take_u32(encoded, &mut cursor)?).ok()?;
        let value_end = cursor.checked_add(value_len)?;
        let value = encoded.get(cursor..value_end)?.to_vec();
        cursor = value_end;

        hash.insert(field, value);
    }

    if cursor != encoded.len() {
        return None;
    }
    Some(hash)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct HashPayloadUpsertResult {
    pub(super) payload: Vec<u8>,
    pub(super) inserted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum HashPayloadDeleteResult {
    NotFound,
    Removed { payload: Vec<u8>, is_empty: bool },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct HashPayloadBatchUpsertResult {
    pub(super) payload: Vec<u8>,
    pub(super) inserted: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum HashPayloadBatchDeleteResult {
    NotFound,
    Removed {
        payload: Vec<u8>,
        removed: i64,
        is_empty: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum HashPayloadBatchGetDeleteResult {
    NotFound {
        values: Vec<Option<Vec<u8>>>,
    },
    Removed {
        values: Vec<Option<Vec<u8>>>,
        payload: Vec<u8>,
        removed: i64,
        is_empty: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct HashPayloadBatchGetResult {
    pub(super) values: Vec<Option<Vec<u8>>>,
}

fn take_u32(encoded: &[u8], cursor: &mut usize) -> Option<u32> {
    let end = (*cursor).checked_add(size_of::<u32>())?;
    let bytes = encoded.get(*cursor..end)?;
    let mut raw = [0u8; size_of::<u32>()];
    raw.copy_from_slice(bytes);
    *cursor = end;
    Some(u32::from_le_bytes(raw))
}

fn append_hash_entry(encoded: &mut Vec<u8>, field: &[u8], value: &[u8]) -> Option<()> {
    let field_len = u32::try_from(field.len()).ok()?;
    let value_len = u32::try_from(value.len()).ok()?;
    encoded.extend_from_slice(&field_len.to_le_bytes());
    encoded.extend_from_slice(field);
    encoded.extend_from_slice(&value_len.to_le_bytes());
    encoded.extend_from_slice(value);
    Some(())
}

pub(super) fn upsert_single_hash_field_payload(
    encoded: &[u8],
    field: &[u8],
    value: &[u8],
) -> Option<HashPayloadUpsertResult> {
    let mut cursor = 0usize;
    let count = take_u32(encoded, &mut cursor)?;
    let mut insert_offset = encoded.len();
    let mut update_value_len_offset = None;
    let mut update_value_end = 0usize;

    for _ in 0..count {
        let entry_start = cursor;
        let field_len = usize::try_from(take_u32(encoded, &mut cursor)?).ok()?;
        let field_end = cursor.checked_add(field_len)?;
        let existing_field = encoded.get(cursor..field_end)?;
        cursor = field_end;

        let value_len_offset = cursor;
        let existing_value_len = usize::try_from(take_u32(encoded, &mut cursor)?).ok()?;
        let value_end = cursor.checked_add(existing_value_len)?;
        encoded.get(cursor..value_end)?;
        cursor = value_end;

        match existing_field.cmp(field) {
            std::cmp::Ordering::Equal => {
                update_value_len_offset = Some(value_len_offset);
                update_value_end = value_end;
                break;
            }
            std::cmp::Ordering::Greater => {
                if insert_offset == encoded.len() {
                    insert_offset = entry_start;
                }
            }
            std::cmp::Ordering::Less => {}
        }
    }

    let value_len_u32 = u32::try_from(value.len()).ok()?;
    if let Some(value_len_offset) = update_value_len_offset {
        let mut next = Vec::with_capacity(
            encoded
                .len()
                .checked_sub(update_value_end.checked_sub(value_len_offset)?)?
                .checked_add(size_of::<u32>() + value.len())?,
        );
        next.extend_from_slice(&encoded[..value_len_offset]);
        next.extend_from_slice(&value_len_u32.to_le_bytes());
        next.extend_from_slice(value);
        next.extend_from_slice(&encoded[update_value_end..]);
        return Some(HashPayloadUpsertResult {
            payload: next,
            inserted: false,
        });
    }

    if cursor != encoded.len() {
        return None;
    }

    let field_len_u32 = u32::try_from(field.len()).ok()?;
    let next_count = count.checked_add(1)?;
    if insert_offset < size_of::<u32>() || insert_offset > encoded.len() {
        return None;
    }
    let mut next = Vec::with_capacity(
        encoded
            .len()
            .checked_add(size_of::<u32>() * 2)?
            .checked_add(field.len())?
            .checked_add(value.len())?,
    );
    next.extend_from_slice(&next_count.to_le_bytes());
    next.extend_from_slice(&encoded[size_of::<u32>()..insert_offset]);
    next.extend_from_slice(&field_len_u32.to_le_bytes());
    next.extend_from_slice(field);
    next.extend_from_slice(&value_len_u32.to_le_bytes());
    next.extend_from_slice(value);
    next.extend_from_slice(&encoded[insert_offset..]);
    Some(HashPayloadUpsertResult {
        payload: next,
        inserted: true,
    })
}

pub(super) fn delete_single_hash_field_payload(
    encoded: &[u8],
    field: &[u8],
) -> Option<HashPayloadDeleteResult> {
    let mut cursor = 0usize;
    let count = take_u32(encoded, &mut cursor)?;
    if count == 0 {
        return Some(HashPayloadDeleteResult::NotFound);
    }

    let mut found_entry_start = 0usize;
    let mut found_entry_end = 0usize;
    let mut found = false;

    for _ in 0..count {
        let entry_start = cursor;
        let field_len = usize::try_from(take_u32(encoded, &mut cursor)?).ok()?;
        let field_end = cursor.checked_add(field_len)?;
        let existing_field = encoded.get(cursor..field_end)?;
        cursor = field_end;

        let value_len = usize::try_from(take_u32(encoded, &mut cursor)?).ok()?;
        let value_end = cursor.checked_add(value_len)?;
        encoded.get(cursor..value_end)?;
        cursor = value_end;

        match existing_field.cmp(field) {
            std::cmp::Ordering::Equal => {
                found_entry_start = entry_start;
                found_entry_end = value_end;
                found = true;
                break;
            }
            std::cmp::Ordering::Greater => break,
            std::cmp::Ordering::Less => {}
        }
    }

    if cursor > encoded.len() {
        return None;
    }
    if !found {
        return Some(HashPayloadDeleteResult::NotFound);
    }

    let next_count = count.checked_sub(1)?;
    if next_count == 0 {
        return Some(HashPayloadDeleteResult::Removed {
            payload: 0u32.to_le_bytes().to_vec(),
            is_empty: true,
        });
    }

    if found_entry_start < size_of::<u32>() || found_entry_end > encoded.len() {
        return None;
    }
    let mut next = Vec::with_capacity(
        encoded
            .len()
            .checked_sub(found_entry_end.checked_sub(found_entry_start)?)?,
    );
    next.extend_from_slice(&next_count.to_le_bytes());
    next.extend_from_slice(&encoded[size_of::<u32>()..found_entry_start]);
    next.extend_from_slice(&encoded[found_entry_end..]);
    Some(HashPayloadDeleteResult::Removed {
        payload: next,
        is_empty: false,
    })
}

pub(super) fn upsert_hash_fields_payload_batch(
    encoded: &[u8],
    field_values: &[(&[u8], &[u8])],
) -> Option<HashPayloadBatchUpsertResult> {
    if let [(field, value)] = field_values {
        let result = upsert_single_hash_field_payload(encoded, field, value)?;
        return Some(HashPayloadBatchUpsertResult {
            payload: result.payload,
            inserted: if result.inserted { 1 } else { 0 },
        });
    }

    let mut updates = BTreeMap::<Vec<u8>, &[u8]>::new();
    for (field, value) in field_values {
        u32::try_from(field.len()).ok()?;
        u32::try_from(value.len()).ok()?;
        updates.insert((*field).to_vec(), *value);
    }
    let updates = updates.into_iter().collect::<Vec<_>>();

    let mut cursor = 0usize;
    let original_count = usize::try_from(take_u32(encoded, &mut cursor)?).ok()?;
    let mut merged = Vec::with_capacity(encoded.len());
    merged.extend_from_slice(&0u32.to_le_bytes());

    let mut update_index = 0usize;
    let mut inserted_count = 0usize;

    for _ in 0..original_count {
        let field_len = usize::try_from(take_u32(encoded, &mut cursor)?).ok()?;
        let field_end = cursor.checked_add(field_len)?;
        let existing_field = encoded.get(cursor..field_end)?;
        cursor = field_end;

        let value_len = usize::try_from(take_u32(encoded, &mut cursor)?).ok()?;
        let value_end = cursor.checked_add(value_len)?;
        let existing_value = encoded.get(cursor..value_end)?;
        cursor = value_end;

        while update_index < updates.len() && updates[update_index].0.as_slice() < existing_field {
            append_hash_entry(
                &mut merged,
                updates[update_index].0.as_slice(),
                updates[update_index].1,
            )?;
            inserted_count = inserted_count.checked_add(1)?;
            update_index += 1;
        }

        if update_index < updates.len() && updates[update_index].0.as_slice() == existing_field {
            append_hash_entry(&mut merged, existing_field, updates[update_index].1)?;
            update_index += 1;
        } else {
            append_hash_entry(&mut merged, existing_field, existing_value)?;
        }
    }
    if cursor != encoded.len() {
        return None;
    }

    while update_index < updates.len() {
        append_hash_entry(
            &mut merged,
            updates[update_index].0.as_slice(),
            updates[update_index].1,
        )?;
        inserted_count = inserted_count.checked_add(1)?;
        update_index += 1;
    }

    let merged_count = original_count.checked_add(inserted_count)?;
    let merged_count_u32 = u32::try_from(merged_count).ok()?;
    merged[..size_of::<u32>()].copy_from_slice(&merged_count_u32.to_le_bytes());

    Some(HashPayloadBatchUpsertResult {
        payload: merged,
        inserted: i64::try_from(inserted_count).ok()?,
    })
}

pub(super) fn delete_hash_fields_payload_batch(
    encoded: &[u8],
    fields: &[&[u8]],
) -> Option<HashPayloadBatchDeleteResult> {
    if let [field] = fields {
        let result = delete_single_hash_field_payload(encoded, field)?;
        return Some(match result {
            HashPayloadDeleteResult::NotFound => HashPayloadBatchDeleteResult::NotFound,
            HashPayloadDeleteResult::Removed { payload, is_empty } => {
                HashPayloadBatchDeleteResult::Removed {
                    payload,
                    removed: 1,
                    is_empty,
                }
            }
        });
    }

    let mut delete_set = BTreeSet::<Vec<u8>>::new();
    for field in fields {
        delete_set.insert((*field).to_vec());
    }

    let mut cursor = 0usize;
    let original_count = usize::try_from(take_u32(encoded, &mut cursor)?).ok()?;
    if original_count == 0 {
        return Some(HashPayloadBatchDeleteResult::NotFound);
    }

    let mut retained = Vec::with_capacity(encoded.len());
    retained.extend_from_slice(&0u32.to_le_bytes());
    let mut removed_count = 0usize;

    for _ in 0..original_count {
        let field_len = usize::try_from(take_u32(encoded, &mut cursor)?).ok()?;
        let field_end = cursor.checked_add(field_len)?;
        let existing_field = encoded.get(cursor..field_end)?;
        cursor = field_end;
        let value_len = usize::try_from(take_u32(encoded, &mut cursor)?).ok()?;
        let value_end = cursor.checked_add(value_len)?;
        let existing_value = encoded.get(cursor..value_end)?;
        cursor = value_end;

        if delete_set.remove(existing_field) {
            removed_count = removed_count.checked_add(1)?;
            continue;
        }

        append_hash_entry(&mut retained, existing_field, existing_value)?;
    }

    if cursor != encoded.len() {
        return None;
    }
    if removed_count == 0 {
        return Some(HashPayloadBatchDeleteResult::NotFound);
    }

    let next_count = original_count.checked_sub(removed_count)?;
    if next_count == 0 {
        return Some(HashPayloadBatchDeleteResult::Removed {
            payload: 0u32.to_le_bytes().to_vec(),
            removed: i64::try_from(removed_count).ok()?,
            is_empty: true,
        });
    }

    let next_count_u32 = u32::try_from(next_count).ok()?;
    retained[..size_of::<u32>()].copy_from_slice(&next_count_u32.to_le_bytes());
    Some(HashPayloadBatchDeleteResult::Removed {
        payload: retained,
        removed: i64::try_from(removed_count).ok()?,
        is_empty: false,
    })
}

pub(super) fn get_hash_fields_payload_batch(
    encoded: &[u8],
    fields: &[&[u8]],
) -> Option<HashPayloadBatchGetResult> {
    let mut requested_positions = BTreeMap::<Vec<u8>, Vec<usize>>::new();
    for (index, field) in fields.iter().enumerate() {
        requested_positions
            .entry((*field).to_vec())
            .or_default()
            .push(index);
    }
    let mut values = vec![None; fields.len()];

    let mut cursor = 0usize;
    let count = usize::try_from(take_u32(encoded, &mut cursor)?).ok()?;
    for _ in 0..count {
        let field_len = usize::try_from(take_u32(encoded, &mut cursor)?).ok()?;
        let field_end = cursor.checked_add(field_len)?;
        let existing_field = encoded.get(cursor..field_end)?;
        cursor = field_end;

        let value_len = usize::try_from(take_u32(encoded, &mut cursor)?).ok()?;
        let value_end = cursor.checked_add(value_len)?;
        let existing_value = encoded.get(cursor..value_end)?;
        cursor = value_end;

        if let Some(indices) = requested_positions.remove(existing_field) {
            for index in indices {
                values[index] = Some(existing_value.to_vec());
            }
        }
    }
    if cursor != encoded.len() {
        return None;
    }
    Some(HashPayloadBatchGetResult { values })
}

pub(super) fn get_and_delete_hash_fields_payload_batch(
    encoded: &[u8],
    fields: &[&[u8]],
) -> Option<HashPayloadBatchGetDeleteResult> {
    let mut requested_positions = BTreeMap::<Vec<u8>, Vec<usize>>::new();
    for (index, field) in fields.iter().enumerate() {
        requested_positions
            .entry((*field).to_vec())
            .or_default()
            .push(index);
    }
    let mut values = vec![None; fields.len()];

    let mut cursor = 0usize;
    let original_count = usize::try_from(take_u32(encoded, &mut cursor)?).ok()?;
    let mut retained = Vec::with_capacity(encoded.len());
    retained.extend_from_slice(&0u32.to_le_bytes());
    let mut removed_count = 0usize;

    for _ in 0..original_count {
        let field_len = usize::try_from(take_u32(encoded, &mut cursor)?).ok()?;
        let field_end = cursor.checked_add(field_len)?;
        let existing_field = encoded.get(cursor..field_end)?;
        cursor = field_end;

        let value_len = usize::try_from(take_u32(encoded, &mut cursor)?).ok()?;
        let value_end = cursor.checked_add(value_len)?;
        let existing_value = encoded.get(cursor..value_end)?;
        cursor = value_end;

        if let Some(indices) = requested_positions.remove(existing_field) {
            if let Some(first_index) = indices.first() {
                values[*first_index] = Some(existing_value.to_vec());
            }
            removed_count = removed_count.checked_add(1)?;
            continue;
        }

        append_hash_entry(&mut retained, existing_field, existing_value)?;
    }
    if cursor != encoded.len() {
        return None;
    }
    if removed_count == 0 {
        return Some(HashPayloadBatchGetDeleteResult::NotFound { values });
    }

    let next_count = original_count.checked_sub(removed_count)?;
    if next_count == 0 {
        return Some(HashPayloadBatchGetDeleteResult::Removed {
            values,
            payload: 0u32.to_le_bytes().to_vec(),
            removed: i64::try_from(removed_count).ok()?,
            is_empty: true,
        });
    }

    let next_count_u32 = u32::try_from(next_count).ok()?;
    retained[..size_of::<u32>()].copy_from_slice(&next_count_u32.to_le_bytes());
    Some(HashPayloadBatchGetDeleteResult::Removed {
        values,
        payload: retained,
        removed: i64::try_from(removed_count).ok()?,
        is_empty: false,
    })
}

pub(super) fn serialize_list_object_payload(list: &[Vec<u8>]) -> Vec<u8> {
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&(list.len() as u32).to_le_bytes());
    for value in list {
        encoded.extend_from_slice(&(value.len() as u32).to_le_bytes());
        encoded.extend_from_slice(value);
    }
    encoded
}

pub(super) fn deserialize_list_object_payload(encoded: &[u8]) -> Option<Vec<Vec<u8>>> {
    let mut cursor = 0usize;

    fn take_u32(encoded: &[u8], cursor: &mut usize) -> Option<u32> {
        let end = (*cursor).checked_add(size_of::<u32>())?;
        let bytes = encoded.get(*cursor..end)?;
        let mut raw = [0u8; size_of::<u32>()];
        raw.copy_from_slice(bytes);
        *cursor = end;
        Some(u32::from_le_bytes(raw))
    }

    let count = take_u32(encoded, &mut cursor)? as usize;
    let mut list = Vec::with_capacity(count);
    for _ in 0..count {
        let value_len = usize::try_from(take_u32(encoded, &mut cursor)?).ok()?;
        let value_end = cursor.checked_add(value_len)?;
        let value = encoded.get(cursor..value_end)?.to_vec();
        cursor = value_end;
        list.push(value);
    }

    if cursor != encoded.len() {
        return None;
    }
    Some(list)
}

pub(super) fn serialize_set_object_payload(set: &BTreeSet<Vec<u8>>) -> Vec<u8> {
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&(set.len() as u32).to_le_bytes());
    for member in set {
        encoded.extend_from_slice(&(member.len() as u32).to_le_bytes());
        encoded.extend_from_slice(member);
    }
    encoded
}

pub(super) fn serialize_contiguous_i64_range_set_payload(range: ContiguousI64RangeSet) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(
        SET_CONTIGUOUS_I64_RANGE_MAGIC.len() + size_of::<i64>() + size_of::<i64>(),
    );
    encoded.extend_from_slice(&SET_CONTIGUOUS_I64_RANGE_MAGIC);
    encoded.extend_from_slice(&range.start().to_le_bytes());
    encoded.extend_from_slice(&range.end().to_le_bytes());
    encoded
}

pub(super) fn materialize_contiguous_i64_range_set(
    range: ContiguousI64RangeSet,
) -> BTreeSet<Vec<u8>> {
    let mut set = BTreeSet::new();
    for value in range.start()..=range.end() {
        set.insert(value.to_string().into_bytes());
    }
    set
}

pub(super) fn decode_set_object_payload(encoded: &[u8]) -> Option<DecodedSetObjectPayload> {
    if let Some(range) = deserialize_contiguous_i64_range_set_payload(encoded) {
        return Some(DecodedSetObjectPayload::ContiguousI64Range(range));
    }
    deserialize_legacy_set_object_payload(encoded).map(DecodedSetObjectPayload::Members)
}

pub(super) fn deserialize_set_object_payload(encoded: &[u8]) -> Option<BTreeSet<Vec<u8>>> {
    match decode_set_object_payload(encoded)? {
        DecodedSetObjectPayload::Members(set) => Some(set),
        DecodedSetObjectPayload::ContiguousI64Range(range) => {
            Some(materialize_contiguous_i64_range_set(range))
        }
    }
}

fn deserialize_legacy_set_object_payload(encoded: &[u8]) -> Option<BTreeSet<Vec<u8>>> {
    let mut cursor = 0usize;

    fn take_u32(encoded: &[u8], cursor: &mut usize) -> Option<u32> {
        let end = (*cursor).checked_add(size_of::<u32>())?;
        let bytes = encoded.get(*cursor..end)?;
        let mut raw = [0u8; size_of::<u32>()];
        raw.copy_from_slice(bytes);
        *cursor = end;
        Some(u32::from_le_bytes(raw))
    }

    let count = take_u32(encoded, &mut cursor)? as usize;
    let mut set = BTreeSet::new();
    for _ in 0..count {
        let member_len = usize::try_from(take_u32(encoded, &mut cursor)?).ok()?;
        let member_end = cursor.checked_add(member_len)?;
        let member = encoded.get(cursor..member_end)?.to_vec();
        cursor = member_end;
        set.insert(member);
    }

    if cursor != encoded.len() {
        return None;
    }
    Some(set)
}

fn deserialize_contiguous_i64_range_set_payload(encoded: &[u8]) -> Option<ContiguousI64RangeSet> {
    let expected_len = SET_CONTIGUOUS_I64_RANGE_MAGIC.len() + size_of::<i64>() + size_of::<i64>();
    if encoded.len() != expected_len {
        return None;
    }
    if !encoded.starts_with(&SET_CONTIGUOUS_I64_RANGE_MAGIC) {
        return None;
    }
    let mut start_bytes = [0u8; size_of::<i64>()];
    let mut end_bytes = [0u8; size_of::<i64>()];
    let start_offset = SET_CONTIGUOUS_I64_RANGE_MAGIC.len();
    let end_offset = start_offset + size_of::<i64>();
    start_bytes.copy_from_slice(&encoded[start_offset..end_offset]);
    end_bytes.copy_from_slice(&encoded[end_offset..expected_len]);
    ContiguousI64RangeSet::new(
        i64::from_le_bytes(start_bytes),
        i64::from_le_bytes(end_bytes),
    )
}

pub(super) fn serialize_zset_object_payload(zset: &BTreeMap<Vec<u8>, f64>) -> Vec<u8> {
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&(zset.len() as u32).to_le_bytes());
    for (member, score) in zset {
        encoded.extend_from_slice(&(member.len() as u32).to_le_bytes());
        encoded.extend_from_slice(member);
        encoded.extend_from_slice(&score.to_le_bytes());
    }
    encoded
}

pub(super) fn deserialize_zset_object_payload(encoded: &[u8]) -> Option<BTreeMap<Vec<u8>, f64>> {
    let mut cursor = 0usize;

    fn take_u32(encoded: &[u8], cursor: &mut usize) -> Option<u32> {
        let end = (*cursor).checked_add(size_of::<u32>())?;
        let bytes = encoded.get(*cursor..end)?;
        let mut raw = [0u8; size_of::<u32>()];
        raw.copy_from_slice(bytes);
        *cursor = end;
        Some(u32::from_le_bytes(raw))
    }

    fn take_f64(encoded: &[u8], cursor: &mut usize) -> Option<f64> {
        let end = (*cursor).checked_add(size_of::<f64>())?;
        let bytes = encoded.get(*cursor..end)?;
        let mut raw = [0u8; size_of::<f64>()];
        raw.copy_from_slice(bytes);
        let value = f64::from_le_bytes(raw);
        if value.is_nan() {
            return None;
        }
        *cursor = end;
        Some(value)
    }

    let count = take_u32(encoded, &mut cursor)? as usize;
    let mut zset = BTreeMap::new();
    for _ in 0..count {
        let member_len = usize::try_from(take_u32(encoded, &mut cursor)?).ok()?;
        let member_end = cursor.checked_add(member_len)?;
        let member = encoded.get(cursor..member_end)?.to_vec();
        cursor = member_end;
        let score = take_f64(encoded, &mut cursor)?;
        zset.insert(member, score);
    }

    if cursor != encoded.len() {
        return None;
    }
    Some(zset)
}

pub(super) fn serialize_stream_object_payload(stream: &StreamObject) -> Vec<u8> {
    const STREAM_OBJECT_CODEC_V5_MAGIC: &[u8] = b"GSTM5";

    let mut encoded = Vec::new();
    encoded.extend_from_slice(STREAM_OBJECT_CODEC_V5_MAGIC);
    encoded.extend_from_slice(&(stream.entries.len() as u32).to_le_bytes());
    for (id, fields) in &stream.entries {
        let encoded_id = id.encode();
        encoded.extend_from_slice(&(encoded_id.len() as u32).to_le_bytes());
        encoded.extend_from_slice(&encoded_id);
        encoded.extend_from_slice(&(fields.len() as u32).to_le_bytes());
        for (field, value) in fields {
            encoded.extend_from_slice(&(field.len() as u32).to_le_bytes());
            encoded.extend_from_slice(field);
            encoded.extend_from_slice(&(value.len() as u32).to_le_bytes());
            encoded.extend_from_slice(value);
        }
    }
    encoded.extend_from_slice(&(stream.groups.len() as u32).to_le_bytes());
    for (group, group_state) in &stream.groups {
        let encoded_last_id = group_state.last_delivered_id.encode();
        encoded.extend_from_slice(&(group.len() as u32).to_le_bytes());
        encoded.extend_from_slice(group);
        encoded.extend_from_slice(&(encoded_last_id.len() as u32).to_le_bytes());
        encoded.extend_from_slice(&encoded_last_id);
        match group_state.entries_read {
            Some(entries_read) => {
                encoded.push(1);
                encoded.extend_from_slice(&entries_read.to_le_bytes());
            }
            None => encoded.push(0),
        }
        encoded.extend_from_slice(&(group_state.consumers.len() as u32).to_le_bytes());
        for (consumer, consumer_state) in &group_state.consumers {
            encoded.extend_from_slice(&(consumer.len() as u32).to_le_bytes());
            encoded.extend_from_slice(consumer);
            encoded.extend_from_slice(&consumer_state.seen_time_millis.to_le_bytes());
            match consumer_state.active_time_millis {
                Some(active_time_millis) => {
                    encoded.push(1);
                    encoded.extend_from_slice(&active_time_millis.to_le_bytes());
                }
                None => encoded.push(0),
            }
        }
        encoded.extend_from_slice(&(group_state.pending.len() as u32).to_le_bytes());
        for (pending_id, pending_entry) in &group_state.pending {
            let encoded_pending_id = pending_id.encode();
            encoded.extend_from_slice(&(encoded_pending_id.len() as u32).to_le_bytes());
            encoded.extend_from_slice(&encoded_pending_id);
            encoded.extend_from_slice(&(pending_entry.consumer.len() as u32).to_le_bytes());
            encoded.extend_from_slice(&pending_entry.consumer);
            encoded.extend_from_slice(&pending_entry.last_delivery_time_millis.to_le_bytes());
            encoded.extend_from_slice(&pending_entry.delivery_count.to_le_bytes());
        }
    }

    let encoded_last_generated_id = stream.last_generated_id.encode();
    encoded.extend_from_slice(&(encoded_last_generated_id.len() as u32).to_le_bytes());
    encoded.extend_from_slice(&encoded_last_generated_id);

    let encoded_max_deleted_entry_id = stream.max_deleted_entry_id.encode();
    encoded.extend_from_slice(&(encoded_max_deleted_entry_id.len() as u32).to_le_bytes());
    encoded.extend_from_slice(&encoded_max_deleted_entry_id);
    encoded.extend_from_slice(&stream.entries_added.to_le_bytes());
    encoded.extend_from_slice(&(stream.node_sizes.len() as u32).to_le_bytes());
    for node_size in &stream.node_sizes {
        encoded.extend_from_slice(&(*node_size as u32).to_le_bytes());
    }
    encoded.extend_from_slice(&stream.idmp_duration_seconds.to_le_bytes());
    encoded.extend_from_slice(&stream.idmp_maxsize.to_le_bytes());
    encoded.extend_from_slice(&stream.iids_added.to_le_bytes());
    encoded.extend_from_slice(&stream.iids_duplicates.to_le_bytes());
    encoded.extend_from_slice(&(stream.idmp_producers.len() as u32).to_le_bytes());
    for (producer_id, producer_state) in &stream.idmp_producers {
        encoded.extend_from_slice(&(producer_id.len() as u32).to_le_bytes());
        encoded.extend_from_slice(producer_id);
        encoded.extend_from_slice(&(producer_state.insertion_order.len() as u32).to_le_bytes());
        for iid in &producer_state.insertion_order {
            let Some(stream_id) = producer_state.entries.get(iid) else {
                continue;
            };
            let encoded_stream_id = stream_id.encode();
            encoded.extend_from_slice(&(iid.len() as u32).to_le_bytes());
            encoded.extend_from_slice(iid);
            encoded.extend_from_slice(&(encoded_stream_id.len() as u32).to_le_bytes());
            encoded.extend_from_slice(&encoded_stream_id);
        }
    }
    encoded
}

pub(super) fn deserialize_stream_object_payload(encoded: &[u8]) -> Option<StreamObject> {
    const STREAM_OBJECT_CODEC_V5_MAGIC: &[u8] = b"GSTM5";
    const STREAM_OBJECT_CODEC_V4_MAGIC: &[u8] = b"GSTM4";
    const STREAM_OBJECT_CODEC_V3_MAGIC: &[u8] = b"GSTM3";
    const STREAM_OBJECT_CODEC_V2_MAGIC: &[u8] = b"GSTM2";

    if let Some(encoded) = encoded.strip_prefix(STREAM_OBJECT_CODEC_V5_MAGIC) {
        return deserialize_stream_object_payload_v5(encoded);
    }
    if let Some(encoded) = encoded.strip_prefix(STREAM_OBJECT_CODEC_V4_MAGIC) {
        return deserialize_stream_object_payload_v4(encoded);
    }
    if let Some(encoded) = encoded.strip_prefix(STREAM_OBJECT_CODEC_V3_MAGIC) {
        return deserialize_stream_object_payload_v3(encoded);
    }
    if let Some(encoded) = encoded.strip_prefix(STREAM_OBJECT_CODEC_V2_MAGIC) {
        return deserialize_stream_object_payload_v2(encoded);
    }
    deserialize_stream_object_payload_v1(encoded)
}

fn deserialize_stream_object_payload_v4_prefix(encoded: &[u8]) -> Option<(StreamObject, usize)> {
    fn take_u32(encoded: &[u8], cursor: &mut usize) -> Option<u32> {
        let end = (*cursor).checked_add(size_of::<u32>())?;
        let bytes = encoded.get(*cursor..end)?;
        let mut raw = [0u8; size_of::<u32>()];
        raw.copy_from_slice(bytes);
        *cursor = end;
        Some(u32::from_le_bytes(raw))
    }

    let (mut stream, mut cursor) = deserialize_stream_object_payload_v3_prefix(encoded)?;
    let node_count = usize::try_from(take_u32(encoded, &mut cursor)?).ok()?;
    let mut node_sizes = VecDeque::with_capacity(node_count);
    for _ in 0..node_count {
        let node_size = usize::try_from(take_u32(encoded, &mut cursor)?).ok()?;
        node_sizes.push_back(node_size);
    }
    let total_entries: usize = node_sizes.iter().sum();
    if total_entries != stream.entries.len() {
        return None;
    }
    stream.node_sizes = node_sizes;
    Some((stream, cursor))
}

fn deserialize_stream_object_payload_v5(encoded: &[u8]) -> Option<StreamObject> {
    fn take_u32(encoded: &[u8], cursor: &mut usize) -> Option<u32> {
        let end = (*cursor).checked_add(size_of::<u32>())?;
        let bytes = encoded.get(*cursor..end)?;
        let mut raw = [0u8; size_of::<u32>()];
        raw.copy_from_slice(bytes);
        *cursor = end;
        Some(u32::from_le_bytes(raw))
    }

    fn take_u64(encoded: &[u8], cursor: &mut usize) -> Option<u64> {
        let end = (*cursor).checked_add(size_of::<u64>())?;
        let bytes = encoded.get(*cursor..end)?;
        let mut raw = [0u8; size_of::<u64>()];
        raw.copy_from_slice(bytes);
        *cursor = end;
        Some(u64::from_le_bytes(raw))
    }

    fn take_bytes(encoded: &[u8], cursor: &mut usize) -> Option<Vec<u8>> {
        let len = usize::try_from(take_u32(encoded, cursor)?).ok()?;
        let end = (*cursor).checked_add(len)?;
        let bytes = encoded.get(*cursor..end)?.to_vec();
        *cursor = end;
        Some(bytes)
    }

    let (mut stream, mut cursor) = deserialize_stream_object_payload_v4_prefix(encoded)?;
    stream.idmp_duration_seconds = take_u64(encoded, &mut cursor)?;
    stream.idmp_maxsize = take_u64(encoded, &mut cursor)?;
    stream.iids_added = take_u64(encoded, &mut cursor)?;
    stream.iids_duplicates = take_u64(encoded, &mut cursor)?;

    let producer_count = usize::try_from(take_u32(encoded, &mut cursor)?).ok()?;
    let mut idmp_producers = BTreeMap::new();
    for _ in 0..producer_count {
        let producer_id = take_bytes(encoded, &mut cursor)?;
        let entry_count = usize::try_from(take_u32(encoded, &mut cursor)?).ok()?;
        let mut producer_state = StreamIdmpProducerState::default();
        for _ in 0..entry_count {
            let iid = take_bytes(encoded, &mut cursor)?;
            let encoded_stream_id = take_bytes(encoded, &mut cursor)?;
            let stream_id = StreamId::parse(&encoded_stream_id)?;
            producer_state.insertion_order.push_back(iid.clone());
            producer_state.entries.insert(iid, stream_id);
        }
        idmp_producers.insert(producer_id, producer_state);
    }
    if cursor != encoded.len() {
        return None;
    }
    stream.idmp_producers = idmp_producers;
    Some(stream)
}

fn deserialize_stream_object_payload_v4(encoded: &[u8]) -> Option<StreamObject> {
    let (stream, cursor) = deserialize_stream_object_payload_v4_prefix(encoded)?;
    if cursor != encoded.len() {
        return None;
    }
    Some(stream)
}

fn deserialize_stream_object_payload_v3_prefix(encoded: &[u8]) -> Option<(StreamObject, usize)> {
    let mut cursor = 0usize;

    fn take_u32(encoded: &[u8], cursor: &mut usize) -> Option<u32> {
        let end = (*cursor).checked_add(size_of::<u32>())?;
        let bytes = encoded.get(*cursor..end)?;
        let mut raw = [0u8; size_of::<u32>()];
        raw.copy_from_slice(bytes);
        *cursor = end;
        Some(u32::from_le_bytes(raw))
    }

    fn take_u64(encoded: &[u8], cursor: &mut usize) -> Option<u64> {
        let end = (*cursor).checked_add(size_of::<u64>())?;
        let bytes = encoded.get(*cursor..end)?;
        let mut raw = [0u8; size_of::<u64>()];
        raw.copy_from_slice(bytes);
        *cursor = end;
        Some(u64::from_le_bytes(raw))
    }

    fn take_bytes(encoded: &[u8], cursor: &mut usize) -> Option<Vec<u8>> {
        let len = usize::try_from(take_u32(encoded, cursor)?).ok()?;
        let end = (*cursor).checked_add(len)?;
        let bytes = encoded.get(*cursor..end)?.to_vec();
        *cursor = end;
        Some(bytes)
    }

    let entry_count = usize::try_from(take_u32(encoded, &mut cursor)?).ok()?;
    let mut entries = BTreeMap::new();
    for _ in 0..entry_count {
        let encoded_id = take_bytes(encoded, &mut cursor)?;
        let id = StreamId::parse(&encoded_id)?;
        let field_count = usize::try_from(take_u32(encoded, &mut cursor)?).ok()?;
        let mut fields = Vec::with_capacity(field_count);
        for _ in 0..field_count {
            let field = take_bytes(encoded, &mut cursor)?;
            let value = take_bytes(encoded, &mut cursor)?;
            fields.push((field, value));
        }
        entries.insert(id, fields);
    }

    let group_count = usize::try_from(take_u32(encoded, &mut cursor)?).ok()?;
    let mut groups = BTreeMap::new();
    for _ in 0..group_count {
        let group = take_bytes(encoded, &mut cursor)?;
        let encoded_last_id = take_bytes(encoded, &mut cursor)?;
        let last_id = StreamId::parse(&encoded_last_id)?;
        let has_entries_read = *encoded.get(cursor)?;
        cursor += 1;
        let entries_read = match has_entries_read {
            0 => None,
            1 => Some(take_u64(encoded, &mut cursor)?),
            _ => return None,
        };

        let consumer_count = usize::try_from(take_u32(encoded, &mut cursor)?).ok()?;
        let mut consumers = BTreeMap::new();
        for _ in 0..consumer_count {
            let consumer = take_bytes(encoded, &mut cursor)?;
            let seen_time_millis = take_u64(encoded, &mut cursor)?;
            let has_active_time = *encoded.get(cursor)?;
            cursor += 1;
            let active_time_millis = match has_active_time {
                0 => None,
                1 => Some(take_u64(encoded, &mut cursor)?),
                _ => return None,
            };
            consumers.insert(
                consumer,
                StreamConsumerState {
                    pending: BTreeSet::new(),
                    seen_time_millis,
                    active_time_millis,
                },
            );
        }

        let pending_count = usize::try_from(take_u32(encoded, &mut cursor)?).ok()?;
        let mut pending = BTreeMap::new();
        for _ in 0..pending_count {
            let encoded_pending_id = take_bytes(encoded, &mut cursor)?;
            let pending_id = StreamId::parse(&encoded_pending_id)?;
            let consumer = take_bytes(encoded, &mut cursor)?;
            let last_delivery_time_millis = take_u64(encoded, &mut cursor)?;
            let delivery_count = take_u64(encoded, &mut cursor)?;
            consumers.entry(consumer.clone()).or_default();
            pending.insert(
                pending_id,
                StreamPendingEntry {
                    consumer,
                    last_delivery_time_millis,
                    delivery_count,
                },
            );
        }
        for (pending_id, pending_entry) in &pending {
            consumers
                .entry(pending_entry.consumer.clone())
                .or_default()
                .pending
                .insert(*pending_id);
        }

        groups.insert(
            group,
            StreamGroupState {
                last_delivered_id: last_id,
                entries_read,
                consumers,
                pending,
            },
        );
    }

    let encoded_last_generated_id = take_bytes(encoded, &mut cursor)?;
    let last_generated_id = StreamId::parse(&encoded_last_generated_id)?;
    let encoded_max_deleted_entry_id = take_bytes(encoded, &mut cursor)?;
    let max_deleted_entry_id = StreamId::parse(&encoded_max_deleted_entry_id)?;
    let entries_added = take_u64(encoded, &mut cursor)?;

    Some((
        StreamObject {
            entries,
            node_sizes: VecDeque::new(),
            groups,
            last_generated_id,
            max_deleted_entry_id,
            entries_added,
            idmp_duration_seconds: super::DEFAULT_STREAM_IDMP_DURATION_SECONDS,
            idmp_maxsize: super::DEFAULT_STREAM_IDMP_MAXSIZE,
            idmp_producers: BTreeMap::new(),
            iids_added: 0,
            iids_duplicates: 0,
        },
        cursor,
    ))
}

fn deserialize_stream_object_payload_v3(encoded: &[u8]) -> Option<StreamObject> {
    let (stream, cursor) = deserialize_stream_object_payload_v3_prefix(encoded)?;
    if cursor != encoded.len() {
        return None;
    }
    Some(stream)
}

fn deserialize_stream_object_payload_v2(encoded: &[u8]) -> Option<StreamObject> {
    let mut cursor = 0usize;

    fn take_u32(encoded: &[u8], cursor: &mut usize) -> Option<u32> {
        let end = (*cursor).checked_add(size_of::<u32>())?;
        let bytes = encoded.get(*cursor..end)?;
        let mut raw = [0u8; size_of::<u32>()];
        raw.copy_from_slice(bytes);
        *cursor = end;
        Some(u32::from_le_bytes(raw))
    }

    fn take_u64(encoded: &[u8], cursor: &mut usize) -> Option<u64> {
        let end = (*cursor).checked_add(size_of::<u64>())?;
        let bytes = encoded.get(*cursor..end)?;
        let mut raw = [0u8; size_of::<u64>()];
        raw.copy_from_slice(bytes);
        *cursor = end;
        Some(u64::from_le_bytes(raw))
    }

    fn take_bytes(encoded: &[u8], cursor: &mut usize) -> Option<Vec<u8>> {
        let len = usize::try_from(take_u32(encoded, cursor)?).ok()?;
        let end = (*cursor).checked_add(len)?;
        let bytes = encoded.get(*cursor..end)?.to_vec();
        *cursor = end;
        Some(bytes)
    }

    let entry_count = usize::try_from(take_u32(encoded, &mut cursor)?).ok()?;
    let mut entries = BTreeMap::new();
    for _ in 0..entry_count {
        let encoded_id = take_bytes(encoded, &mut cursor)?;
        let id = StreamId::parse(&encoded_id)?;
        let field_count = usize::try_from(take_u32(encoded, &mut cursor)?).ok()?;
        let mut fields = Vec::with_capacity(field_count);
        for _ in 0..field_count {
            let field = take_bytes(encoded, &mut cursor)?;
            let value = take_bytes(encoded, &mut cursor)?;
            fields.push((field, value));
        }
        entries.insert(id, fields);
    }

    let group_count = usize::try_from(take_u32(encoded, &mut cursor)?).ok()?;
    let mut groups = BTreeMap::new();
    for _ in 0..group_count {
        let group = take_bytes(encoded, &mut cursor)?;
        let encoded_last_id = take_bytes(encoded, &mut cursor)?;
        let last_id = StreamId::parse(&encoded_last_id)?;
        let has_entries_read = *encoded.get(cursor)?;
        cursor += 1;
        let entries_read = match has_entries_read {
            0 => None,
            1 => Some(take_u64(encoded, &mut cursor)?),
            _ => return None,
        };
        let consumer_count = usize::try_from(take_u32(encoded, &mut cursor)?).ok()?;
        let mut consumers = BTreeMap::new();
        for _ in 0..consumer_count {
            let consumer = take_bytes(encoded, &mut cursor)?;
            consumers.insert(consumer, StreamConsumerState::default());
        }
        groups.insert(
            group,
            StreamGroupState {
                last_delivered_id: last_id,
                entries_read,
                consumers,
                pending: BTreeMap::new(),
            },
        );
    }

    let encoded_last_generated_id = take_bytes(encoded, &mut cursor)?;
    let last_generated_id = StreamId::parse(&encoded_last_generated_id)?;
    let encoded_max_deleted_entry_id = take_bytes(encoded, &mut cursor)?;
    let max_deleted_entry_id = StreamId::parse(&encoded_max_deleted_entry_id)?;
    let entries_added = take_u64(encoded, &mut cursor)?;

    if cursor != encoded.len() {
        return None;
    }

    Some(StreamObject {
        entries,
        node_sizes: VecDeque::new(),
        groups,
        last_generated_id,
        max_deleted_entry_id,
        entries_added,
        idmp_duration_seconds: super::DEFAULT_STREAM_IDMP_DURATION_SECONDS,
        idmp_maxsize: super::DEFAULT_STREAM_IDMP_MAXSIZE,
        idmp_producers: BTreeMap::new(),
        iids_added: 0,
        iids_duplicates: 0,
    })
}

fn deserialize_stream_object_payload_v1(encoded: &[u8]) -> Option<StreamObject> {
    let mut cursor = 0usize;

    fn take_u32(encoded: &[u8], cursor: &mut usize) -> Option<u32> {
        let end = (*cursor).checked_add(size_of::<u32>())?;
        let bytes = encoded.get(*cursor..end)?;
        let mut raw = [0u8; size_of::<u32>()];
        raw.copy_from_slice(bytes);
        *cursor = end;
        Some(u32::from_le_bytes(raw))
    }

    fn take_bytes(encoded: &[u8], cursor: &mut usize) -> Option<Vec<u8>> {
        let len = usize::try_from(take_u32(encoded, cursor)?).ok()?;
        let end = (*cursor).checked_add(len)?;
        let bytes = encoded.get(*cursor..end)?.to_vec();
        *cursor = end;
        Some(bytes)
    }

    let entry_count = usize::try_from(take_u32(encoded, &mut cursor)?).ok()?;
    let mut entries = BTreeMap::new();
    for _ in 0..entry_count {
        let encoded_id = take_bytes(encoded, &mut cursor)?;
        let id = StreamId::parse(&encoded_id)?;
        let field_count = usize::try_from(take_u32(encoded, &mut cursor)?).ok()?;
        let mut fields = Vec::with_capacity(field_count);
        for _ in 0..field_count {
            let field = take_bytes(encoded, &mut cursor)?;
            let value = take_bytes(encoded, &mut cursor)?;
            fields.push((field, value));
        }
        entries.insert(id, fields);
    }

    let group_count = usize::try_from(take_u32(encoded, &mut cursor)?).ok()?;
    let mut groups = BTreeMap::new();
    for _ in 0..group_count {
        let group = take_bytes(encoded, &mut cursor)?;
        let encoded_last_id = take_bytes(encoded, &mut cursor)?;
        let last_id = StreamId::parse(&encoded_last_id)?;
        groups.insert(
            group,
            StreamGroupState {
                last_delivered_id: last_id,
                entries_read: None,
                consumers: BTreeMap::new(),
                pending: BTreeMap::new(),
            },
        );
    }

    if cursor < encoded.len() {
        let consumer_group_count = usize::try_from(take_u32(encoded, &mut cursor)?).ok()?;
        for _ in 0..consumer_group_count {
            let group = take_bytes(encoded, &mut cursor)?;
            let consumer_count = usize::try_from(take_u32(encoded, &mut cursor)?).ok()?;
            let group_state = groups.entry(group).or_default();
            for _ in 0..consumer_count {
                let consumer = take_bytes(encoded, &mut cursor)?;
                group_state
                    .consumers
                    .entry(consumer)
                    .or_insert_with(StreamConsumerState::default);
            }
        }
    }

    if cursor != encoded.len() {
        return None;
    }

    let last_generated_id = entries
        .keys()
        .next_back()
        .copied()
        .unwrap_or_else(StreamId::zero);
    let entries_added = entries.len() as u64;

    Some(StreamObject {
        entries,
        node_sizes: VecDeque::new(),
        groups,
        last_generated_id,
        max_deleted_entry_id: StreamId::zero(),
        entries_added,
        idmp_duration_seconds: super::DEFAULT_STREAM_IDMP_DURATION_SECONDS,
        idmp_maxsize: super::DEFAULT_STREAM_IDMP_MAXSIZE,
        idmp_producers: BTreeMap::new(),
        iids_added: 0,
        iids_duplicates: 0,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_object_payload_v3_roundtrips_metadata_and_group_state() {
        let mut groups = BTreeMap::new();
        groups.insert(
            b"group-a".to_vec(),
            StreamGroupState {
                last_delivered_id: StreamId::new(2, 0),
                entries_read: Some(1),
                consumers: BTreeMap::from([
                    (
                        b"alice".to_vec(),
                        StreamConsumerState {
                            pending: BTreeSet::from([StreamId::new(1, 0)]),
                            seen_time_millis: 10,
                            active_time_millis: Some(11),
                        },
                    ),
                    (
                        b"bob".to_vec(),
                        StreamConsumerState {
                            pending: BTreeSet::new(),
                            seen_time_millis: 12,
                            active_time_millis: None,
                        },
                    ),
                ]),
                pending: BTreeMap::from([(
                    StreamId::new(1, 0),
                    StreamPendingEntry {
                        consumer: b"alice".to_vec(),
                        last_delivery_time_millis: 11,
                        delivery_count: 2,
                    },
                )]),
            },
        );

        let mut entries = BTreeMap::new();
        entries.insert(StreamId::new(1, 0), vec![(b"f".to_vec(), b"v".to_vec())]);
        let stream = StreamObject {
            entries,
            node_sizes: VecDeque::from([1]),
            groups,
            last_generated_id: StreamId::new(3, 0),
            max_deleted_entry_id: StreamId::new(1, 0),
            entries_added: 5,
            idmp_duration_seconds: 100,
            idmp_maxsize: 100,
            idmp_producers: BTreeMap::new(),
            iids_added: 0,
            iids_duplicates: 0,
        };

        let encoded = serialize_stream_object_payload(&stream);
        let decoded = deserialize_stream_object_payload(&encoded).expect("decode v2 payload");
        assert_eq!(decoded, stream);
    }

    #[test]
    fn stream_object_payload_v1_upgrades_into_new_metadata_shape() {
        fn append_len_bytes(target: &mut Vec<u8>, value: &[u8]) {
            target.extend_from_slice(&(value.len() as u32).to_le_bytes());
            target.extend_from_slice(value);
        }

        let mut encoded = Vec::new();
        encoded.extend_from_slice(&1u32.to_le_bytes());
        append_len_bytes(&mut encoded, b"2-0");
        encoded.extend_from_slice(&1u32.to_le_bytes());
        append_len_bytes(&mut encoded, b"field");
        append_len_bytes(&mut encoded, b"value");

        encoded.extend_from_slice(&1u32.to_le_bytes());
        append_len_bytes(&mut encoded, b"group-a");
        append_len_bytes(&mut encoded, b"2-0");

        encoded.extend_from_slice(&1u32.to_le_bytes());
        append_len_bytes(&mut encoded, b"group-a");
        encoded.extend_from_slice(&1u32.to_le_bytes());
        append_len_bytes(&mut encoded, b"alice");

        let decoded = deserialize_stream_object_payload(&encoded).expect("decode v1 payload");
        assert_eq!(decoded.last_generated_id, StreamId::new(2, 0));
        assert_eq!(decoded.max_deleted_entry_id, StreamId::zero());
        assert_eq!(decoded.entries_added, 1);
        let group_state = decoded.groups.get(b"group-a".as_slice()).unwrap();
        assert_eq!(group_state.last_delivered_id, StreamId::new(2, 0));
        assert_eq!(group_state.entries_read, None);
        assert!(group_state.consumers.contains_key(b"alice".as_slice()));
        assert!(group_state.pending.is_empty());
    }

    #[test]
    fn upsert_hash_fields_payload_batch_preserves_sorted_order_for_single_update() {
        let mut map = BTreeMap::new();
        map.insert(b"b".to_vec(), b"2".to_vec());
        map.insert(b"d".to_vec(), b"4".to_vec());
        let encoded = serialize_hash_object_payload(&map);

        let inserted = upsert_hash_fields_payload_batch(&encoded, &[(b"a", b"1")])
            .expect("payload must stay decodable");
        assert_eq!(inserted.inserted, 1);
        let decoded = deserialize_hash_object_payload(&inserted.payload).expect("decode inserted");
        let fields = decoded.keys().cloned().collect::<Vec<_>>();
        assert_eq!(fields, vec![b"a".to_vec(), b"b".to_vec(), b"d".to_vec()]);

        let updated = upsert_hash_fields_payload_batch(&inserted.payload, &[(b"b", b"22")])
            .expect("payload must stay decodable");
        assert_eq!(updated.inserted, 0);
        let decoded = deserialize_hash_object_payload(&updated.payload).expect("decode updated");
        assert_eq!(decoded.get(b"a".as_slice()), Some(&b"1".to_vec()));
        assert_eq!(decoded.get(b"b".as_slice()), Some(&b"22".to_vec()));
        assert_eq!(decoded.get(b"d".as_slice()), Some(&b"4".to_vec()));
    }

    #[test]
    fn delete_hash_fields_payload_batch_handles_missing_and_empty_for_single_delete() {
        let mut map = BTreeMap::new();
        map.insert(b"x".to_vec(), b"1".to_vec());
        map.insert(b"y".to_vec(), b"2".to_vec());
        let encoded = serialize_hash_object_payload(&map);

        let missing = delete_hash_fields_payload_batch(&encoded, &[b"z"]).expect("decode");
        assert_eq!(missing, HashPayloadBatchDeleteResult::NotFound);

        let removed_y = delete_hash_fields_payload_batch(&encoded, &[b"y"]).expect("decode");
        let HashPayloadBatchDeleteResult::Removed {
            payload,
            removed,
            is_empty,
        } = removed_y
        else {
            panic!("y must be removed");
        };
        assert_eq!(removed, 1);
        assert!(!is_empty);
        let decoded = deserialize_hash_object_payload(&payload).expect("decode after y remove");
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded.get(b"x".as_slice()), Some(&b"1".to_vec()));

        let removed_x = delete_hash_fields_payload_batch(&payload, &[b"x"]).expect("decode");
        let HashPayloadBatchDeleteResult::Removed {
            payload,
            removed,
            is_empty,
        } = removed_x
        else {
            panic!("x must be removed");
        };
        assert_eq!(removed, 1);
        assert!(is_empty);
        let decoded = deserialize_hash_object_payload(&payload).expect("decode empty");
        assert!(decoded.is_empty());
    }

    #[test]
    fn upsert_hash_fields_payload_batch_merges_sorted_and_counts_unique_inserts() {
        let mut map = BTreeMap::new();
        map.insert(b"b".to_vec(), b"2".to_vec());
        map.insert(b"d".to_vec(), b"4".to_vec());
        let encoded = serialize_hash_object_payload(&map);

        let updates: [(&[u8], &[u8]); 5] = [
            (b"c", b"3"),
            (b"a", b"1"),
            (b"b", b"22"),
            (b"c", b"33"),
            (b"e", b"5"),
        ];
        let merged =
            upsert_hash_fields_payload_batch(&encoded, &updates).expect("batch upsert must work");
        assert_eq!(merged.inserted, 3);

        let decoded = deserialize_hash_object_payload(&merged.payload).expect("decode merged");
        let fields = decoded.keys().cloned().collect::<Vec<_>>();
        assert_eq!(
            fields,
            vec![
                b"a".to_vec(),
                b"b".to_vec(),
                b"c".to_vec(),
                b"d".to_vec(),
                b"e".to_vec()
            ]
        );
        assert_eq!(decoded.get(b"a".as_slice()), Some(&b"1".to_vec()));
        assert_eq!(decoded.get(b"b".as_slice()), Some(&b"22".to_vec()));
        assert_eq!(decoded.get(b"c".as_slice()), Some(&b"33".to_vec()));
        assert_eq!(decoded.get(b"d".as_slice()), Some(&b"4".to_vec()));
        assert_eq!(decoded.get(b"e".as_slice()), Some(&b"5".to_vec()));
    }

    #[test]
    fn delete_hash_fields_payload_batch_deduplicates_fields_and_tracks_removed_count() {
        let mut map = BTreeMap::new();
        map.insert(b"a".to_vec(), b"1".to_vec());
        map.insert(b"b".to_vec(), b"2".to_vec());
        map.insert(b"c".to_vec(), b"3".to_vec());
        let encoded = serialize_hash_object_payload(&map);

        let fields: [&[u8]; 4] = [b"b", b"x", b"b", b"c"];
        let removed =
            delete_hash_fields_payload_batch(&encoded, &fields).expect("batch delete must work");
        let HashPayloadBatchDeleteResult::Removed {
            payload,
            removed,
            is_empty,
        } = removed
        else {
            panic!("b and c should be removed");
        };
        assert_eq!(removed, 2);
        assert!(!is_empty);
        let decoded = deserialize_hash_object_payload(&payload).expect("decode retained");
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded.get(b"a".as_slice()), Some(&b"1".to_vec()));

        let not_found = delete_hash_fields_payload_batch(&payload, &[b"z"]).expect("not found");
        assert_eq!(not_found, HashPayloadBatchDeleteResult::NotFound);

        let remove_last =
            delete_hash_fields_payload_batch(&payload, &[b"a", b"a"]).expect("remove last");
        let HashPayloadBatchDeleteResult::Removed {
            payload,
            removed,
            is_empty,
        } = remove_last
        else {
            panic!("a should be removed");
        };
        assert_eq!(removed, 1);
        assert!(is_empty);
        let decoded = deserialize_hash_object_payload(&payload).expect("decode empty");
        assert!(decoded.is_empty());
    }

    #[test]
    fn get_hash_fields_payload_batch_returns_values_for_duplicate_requests_in_order() {
        let mut map = BTreeMap::new();
        map.insert(b"a".to_vec(), b"1".to_vec());
        map.insert(b"b".to_vec(), b"2".to_vec());
        map.insert(b"c".to_vec(), b"3".to_vec());
        let encoded = serialize_hash_object_payload(&map);

        let fields: [&[u8]; 5] = [b"b", b"x", b"b", b"c", b"b"];
        let result = get_hash_fields_payload_batch(&encoded, &fields).expect("batch get works");
        assert_eq!(
            result.values,
            vec![
                Some(b"2".to_vec()),
                None,
                Some(b"2".to_vec()),
                Some(b"3".to_vec()),
                Some(b"2".to_vec())
            ]
        );
    }

    #[test]
    fn get_and_delete_hash_fields_payload_batch_returns_ordered_values_and_updates_payload() {
        let mut map = BTreeMap::new();
        map.insert(b"a".to_vec(), b"1".to_vec());
        map.insert(b"b".to_vec(), b"2".to_vec());
        map.insert(b"c".to_vec(), b"3".to_vec());
        let encoded = serialize_hash_object_payload(&map);

        let fields: [&[u8]; 4] = [b"b", b"x", b"b", b"c"];
        let result =
            get_and_delete_hash_fields_payload_batch(&encoded, &fields).expect("getdel works");
        let HashPayloadBatchGetDeleteResult::Removed {
            values,
            payload,
            removed,
            is_empty,
        } = result
        else {
            panic!("b and c should be removed");
        };
        assert_eq!(
            values,
            vec![Some(b"2".to_vec()), None, None, Some(b"3".to_vec())]
        );
        assert_eq!(removed, 2);
        assert!(!is_empty);
        let decoded = deserialize_hash_object_payload(&payload).expect("decode retained");
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded.get(b"a".as_slice()), Some(&b"1".to_vec()));

        let not_found =
            get_and_delete_hash_fields_payload_batch(&payload, &[b"z"]).expect("not found");
        let HashPayloadBatchGetDeleteResult::NotFound { values } = not_found else {
            panic!("z should not exist");
        };
        assert_eq!(values, vec![None]);

        let remove_last =
            get_and_delete_hash_fields_payload_batch(&payload, &[b"a", b"a"]).expect("remove a");
        let HashPayloadBatchGetDeleteResult::Removed {
            values,
            payload,
            removed,
            is_empty,
        } = remove_last
        else {
            panic!("a should be removed");
        };
        assert_eq!(values, vec![Some(b"1".to_vec()), None]);
        assert_eq!(removed, 1);
        assert!(is_empty);
        let decoded = deserialize_hash_object_payload(&payload).expect("decode empty");
        assert!(decoded.is_empty());
    }
}
