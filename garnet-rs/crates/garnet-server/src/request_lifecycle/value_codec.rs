use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::mem::size_of;

use super::StreamObject;

const VALUE_EXPIRATION_PREFIX_LEN: usize = size_of::<u64>();

#[derive(Debug, Clone, Copy)]
pub(super) struct DecodedStoredValue<'a> {
    pub(super) expiration_unix_millis: Option<u64>,
    pub(super) user_value: &'a [u8],
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
    if input.is_empty() {
        return None;
    }
    let text = core::str::from_utf8(input).ok()?;
    let parsed = text.parse::<f64>().ok()?;
    if !parsed.is_finite() {
        return None;
    }
    Some(parsed)
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

pub(super) fn encode_object_value(object_type: u8, payload: &[u8]) -> Vec<u8> {
    let mut value = Vec::with_capacity(1 + payload.len());
    value.push(object_type);
    value.extend_from_slice(payload);
    value
}

pub(super) fn decode_object_value(encoded: &[u8]) -> Option<(u8, Vec<u8>)> {
    let (&object_type, payload) = encoded.split_first()?;
    Some((object_type, payload.to_vec()))
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

pub(super) fn deserialize_set_object_payload(encoded: &[u8]) -> Option<BTreeSet<Vec<u8>>> {
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
        if !value.is_finite() {
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
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&(stream.entries.len() as u32).to_le_bytes());
    for (id, fields) in &stream.entries {
        encoded.extend_from_slice(&(id.len() as u32).to_le_bytes());
        encoded.extend_from_slice(id);
        encoded.extend_from_slice(&(fields.len() as u32).to_le_bytes());
        for (field, value) in fields {
            encoded.extend_from_slice(&(field.len() as u32).to_le_bytes());
            encoded.extend_from_slice(field);
            encoded.extend_from_slice(&(value.len() as u32).to_le_bytes());
            encoded.extend_from_slice(value);
        }
    }
    encoded.extend_from_slice(&(stream.groups.len() as u32).to_le_bytes());
    for (group, last_id) in &stream.groups {
        encoded.extend_from_slice(&(group.len() as u32).to_le_bytes());
        encoded.extend_from_slice(group);
        encoded.extend_from_slice(&(last_id.len() as u32).to_le_bytes());
        encoded.extend_from_slice(last_id);
    }
    encoded
}

pub(super) fn deserialize_stream_object_payload(encoded: &[u8]) -> Option<StreamObject> {
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
        let id = take_bytes(encoded, &mut cursor)?;
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
        let last_id = take_bytes(encoded, &mut cursor)?;
        groups.insert(group, last_id);
    }

    if cursor != encoded.len() {
        return None;
    }

    Some(StreamObject { entries, groups })
}
