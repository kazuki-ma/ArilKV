use tsavorite::DeleteInfo;
use tsavorite::HybridLogDeleteAdapter;
use tsavorite::HybridLogReadAdapter;
use tsavorite::HybridLogRmwAdapter;
use tsavorite::HybridLogUpsertAdapter;
use tsavorite::ISessionFunctions;
use tsavorite::ReadInfo;
use tsavorite::RecordInfo;
use tsavorite::RmwInfo;
use tsavorite::UpsertInfo;
use tsavorite::WriteReason;

use super::UPSERT_USER_DATA_HAS_EXPIRATION;
use super::current_unix_time_millis;
use super::decode_stored_value;
use super::encode_stored_value;
use super::parse_i64_ascii;

pub(super) struct KvSessionFunctions;

impl ISessionFunctions for KvSessionFunctions {
    type Key = Vec<u8>;
    type Value = Vec<u8>;
    type Input = Vec<u8>;
    type Output = Vec<u8>;
    type Context = ();
    type Reader = ();
    type Writer = ();
    type Comparer = ();

    fn single_reader(
        &self,
        _key: &Self::Key,
        _input: &Self::Input,
        value: &Self::Value,
        output: &mut Self::Output,
        _read_info: &ReadInfo,
    ) -> bool {
        let decoded = decode_stored_value(value);
        if let Some(expiration) = decoded.expiration_unix_millis
            && let Some(now) = current_unix_time_millis()
            && now >= expiration
        {
            return false;
        }
        *output = decoded.user_value.to_vec();
        true
    }

    fn concurrent_reader(
        &self,
        _key: &Self::Key,
        _input: &Self::Input,
        value: &Self::Value,
        output: &mut Self::Output,
        _read_info: &ReadInfo,
        _record_info: &RecordInfo,
    ) -> bool {
        let decoded = decode_stored_value(value);
        if let Some(expiration) = decoded.expiration_unix_millis
            && let Some(now) = current_unix_time_millis()
            && now >= expiration
        {
            return false;
        }
        *output = decoded.user_value.to_vec();
        true
    }

    fn single_writer(
        &self,
        _key: &Self::Key,
        input: &Self::Input,
        _src: &Self::Value,
        dst: &mut Self::Value,
        output: &mut Self::Output,
        upsert_info: &mut UpsertInfo,
        _reason: WriteReason,
        record_info: &mut RecordInfo,
    ) -> bool {
        *dst = input.clone();
        *output = dst.clone();
        if upsert_info
            .user_data
            .contains(UPSERT_USER_DATA_HAS_EXPIRATION)
        {
            record_info.set_has_expiration();
        } else {
            record_info.clear_has_expiration();
        }
        true
    }

    fn concurrent_writer(
        &self,
        _key: &Self::Key,
        input: &Self::Input,
        _src: &Self::Value,
        dst: &mut Self::Value,
        output: &mut Self::Output,
        upsert_info: &mut UpsertInfo,
        record_info: &mut RecordInfo,
    ) -> bool {
        *dst = input.clone();
        *output = dst.clone();
        if upsert_info
            .user_data
            .contains(UPSERT_USER_DATA_HAS_EXPIRATION)
        {
            record_info.set_has_expiration();
        } else {
            record_info.clear_has_expiration();
        }
        true
    }

    fn in_place_updater(
        &self,
        _key: &Self::Key,
        input: &Self::Input,
        value: &mut Self::Value,
        output: &mut Self::Output,
        _rmw_info: &mut RmwInfo,
        _record_info: &mut RecordInfo,
    ) -> bool {
        let decoded = decode_stored_value(value);
        let current = parse_i64_ascii(decoded.user_value).unwrap_or(0);
        let delta = match parse_i64_ascii(input) {
            Some(v) => v,
            None => return false,
        };
        let next = match current.checked_add(delta) {
            Some(v) => v,
            None => return false,
        };
        let next_bytes = next.to_string().into_bytes();
        *value = encode_stored_value(&next_bytes, decoded.expiration_unix_millis);
        *output = next_bytes;
        true
    }

    fn copy_updater(
        &self,
        _key: &Self::Key,
        input: &Self::Input,
        old_value: &Self::Value,
        new_value: &mut Self::Value,
        output: &mut Self::Output,
        _rmw_info: &mut RmwInfo,
        _record_info: &mut RecordInfo,
    ) -> bool {
        let decoded = decode_stored_value(old_value);
        let current = if decoded.user_value.is_empty() {
            0
        } else {
            match parse_i64_ascii(decoded.user_value) {
                Some(v) => v,
                None => return false,
            }
        };
        let delta = match parse_i64_ascii(input) {
            Some(v) => v,
            None => return false,
        };
        let next = match current.checked_add(delta) {
            Some(v) => v,
            None => return false,
        };
        let next_bytes = next.to_string().into_bytes();
        *new_value = encode_stored_value(&next_bytes, decoded.expiration_unix_millis);
        *output = next_bytes;
        true
    }

    fn single_deleter(
        &self,
        _key: &Self::Key,
        value: &mut Self::Value,
        _delete_info: &mut DeleteInfo,
        record_info: &mut RecordInfo,
    ) -> bool {
        value.clear();
        record_info.clear_has_expiration();
        record_info.set_tombstone();
        true
    }

    fn concurrent_deleter(
        &self,
        _key: &Self::Key,
        value: &mut Self::Value,
        _delete_info: &mut DeleteInfo,
        record_info: &mut RecordInfo,
    ) -> bool {
        value.clear();
        record_info.clear_has_expiration();
        record_info.set_tombstone();
        true
    }
}

impl HybridLogReadAdapter for KvSessionFunctions {
    fn record_key_equals(&self, requested_key: &Self::Key, record_key: &[u8]) -> bool {
        requested_key.as_slice() == record_key
    }

    fn value_from_record(&self, record_value: &[u8]) -> Self::Value {
        record_value.to_vec()
    }
}

impl HybridLogUpsertAdapter for KvSessionFunctions {
    fn record_key_equals(&self, requested_key: &Self::Key, record_key: &[u8]) -> bool {
        requested_key.as_slice() == record_key
    }

    fn key_to_record_bytes(&self, key: &Self::Key) -> Vec<u8> {
        key.clone()
    }

    fn value_from_record(&self, record_value: &[u8]) -> Self::Value {
        record_value.to_vec()
    }

    fn value_to_record_bytes(&self, value: &Self::Value) -> Vec<u8> {
        value.clone()
    }
}

impl HybridLogRmwAdapter for KvSessionFunctions {
    fn record_key_equals(&self, requested_key: &Self::Key, record_key: &[u8]) -> bool {
        requested_key.as_slice() == record_key
    }

    fn key_to_record_bytes(&self, key: &Self::Key) -> Vec<u8> {
        key.clone()
    }

    fn value_from_record(&self, record_value: &[u8]) -> Self::Value {
        record_value.to_vec()
    }

    fn value_to_record_bytes(&self, value: &Self::Value) -> Vec<u8> {
        value.clone()
    }
}

impl HybridLogDeleteAdapter for KvSessionFunctions {
    fn record_key_equals(&self, requested_key: &Self::Key, record_key: &[u8]) -> bool {
        requested_key.as_slice() == record_key
    }

    fn key_to_record_bytes(&self, key: &Self::Key) -> Vec<u8> {
        key.clone()
    }

    fn value_from_record(&self, record_value: &[u8]) -> Self::Value {
        record_value.to_vec()
    }

    fn value_to_record_bytes(&self, value: &Self::Value) -> Vec<u8> {
        value.clone()
    }
}

pub(super) struct ObjectSessionFunctions;

impl ISessionFunctions for ObjectSessionFunctions {
    type Key = Vec<u8>;
    type Value = Vec<u8>;
    type Input = Vec<u8>;
    type Output = Vec<u8>;
    type Context = ();
    type Reader = ();
    type Writer = ();
    type Comparer = ();

    fn single_reader(
        &self,
        _key: &Self::Key,
        _input: &Self::Input,
        value: &Self::Value,
        output: &mut Self::Output,
        _read_info: &ReadInfo,
    ) -> bool {
        *output = value.clone();
        true
    }

    fn concurrent_reader(
        &self,
        _key: &Self::Key,
        _input: &Self::Input,
        value: &Self::Value,
        output: &mut Self::Output,
        _read_info: &ReadInfo,
        _record_info: &RecordInfo,
    ) -> bool {
        *output = value.clone();
        true
    }

    fn single_writer(
        &self,
        _key: &Self::Key,
        input: &Self::Input,
        _src: &Self::Value,
        dst: &mut Self::Value,
        output: &mut Self::Output,
        _upsert_info: &mut UpsertInfo,
        _reason: WriteReason,
        _record_info: &mut RecordInfo,
    ) -> bool {
        *dst = input.clone();
        *output = dst.clone();
        true
    }

    fn concurrent_writer(
        &self,
        _key: &Self::Key,
        input: &Self::Input,
        _src: &Self::Value,
        dst: &mut Self::Value,
        output: &mut Self::Output,
        _upsert_info: &mut UpsertInfo,
        _record_info: &mut RecordInfo,
    ) -> bool {
        *dst = input.clone();
        *output = dst.clone();
        true
    }

    fn in_place_updater(
        &self,
        _key: &Self::Key,
        input: &Self::Input,
        value: &mut Self::Value,
        output: &mut Self::Output,
        _rmw_info: &mut RmwInfo,
        _record_info: &mut RecordInfo,
    ) -> bool {
        *value = input.clone();
        *output = value.clone();
        true
    }

    fn copy_updater(
        &self,
        _key: &Self::Key,
        input: &Self::Input,
        _old_value: &Self::Value,
        new_value: &mut Self::Value,
        output: &mut Self::Output,
        _rmw_info: &mut RmwInfo,
        _record_info: &mut RecordInfo,
    ) -> bool {
        *new_value = input.clone();
        *output = new_value.clone();
        true
    }

    fn single_deleter(
        &self,
        _key: &Self::Key,
        value: &mut Self::Value,
        _delete_info: &mut DeleteInfo,
        record_info: &mut RecordInfo,
    ) -> bool {
        value.clear();
        record_info.set_tombstone();
        true
    }

    fn concurrent_deleter(
        &self,
        _key: &Self::Key,
        value: &mut Self::Value,
        _delete_info: &mut DeleteInfo,
        record_info: &mut RecordInfo,
    ) -> bool {
        value.clear();
        record_info.set_tombstone();
        true
    }
}

impl HybridLogReadAdapter for ObjectSessionFunctions {
    fn record_key_equals(&self, requested_key: &Self::Key, record_key: &[u8]) -> bool {
        requested_key.as_slice() == record_key
    }

    fn value_from_record(&self, record_value: &[u8]) -> Self::Value {
        record_value.to_vec()
    }
}

impl HybridLogUpsertAdapter for ObjectSessionFunctions {
    fn record_key_equals(&self, requested_key: &Self::Key, record_key: &[u8]) -> bool {
        requested_key.as_slice() == record_key
    }

    fn key_to_record_bytes(&self, key: &Self::Key) -> Vec<u8> {
        key.clone()
    }

    fn value_from_record(&self, record_value: &[u8]) -> Self::Value {
        record_value.to_vec()
    }

    fn value_to_record_bytes(&self, value: &Self::Value) -> Vec<u8> {
        value.clone()
    }
}

impl HybridLogRmwAdapter for ObjectSessionFunctions {
    fn record_key_equals(&self, requested_key: &Self::Key, record_key: &[u8]) -> bool {
        requested_key.as_slice() == record_key
    }

    fn key_to_record_bytes(&self, key: &Self::Key) -> Vec<u8> {
        key.clone()
    }

    fn value_from_record(&self, record_value: &[u8]) -> Self::Value {
        record_value.to_vec()
    }

    fn value_to_record_bytes(&self, value: &Self::Value) -> Vec<u8> {
        value.clone()
    }
}

impl HybridLogDeleteAdapter for ObjectSessionFunctions {
    fn record_key_equals(&self, requested_key: &Self::Key, record_key: &[u8]) -> bool {
        requested_key.as_slice() == record_key
    }

    fn key_to_record_bytes(&self, key: &Self::Key) -> Vec<u8> {
        key.clone()
    }

    fn value_from_record(&self, record_value: &[u8]) -> Self::Value {
        record_value.to_vec()
    }

    fn value_to_record_bytes(&self, value: &Self::Value) -> Vec<u8> {
        value.clone()
    }
}
