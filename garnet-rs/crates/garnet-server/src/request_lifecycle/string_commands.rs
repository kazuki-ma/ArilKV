use super::*;
use tsavorite::{RmwInfo, RmwOperationError};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BitopOperation {
    And,
    Or,
    Xor,
    Not,
}

impl RequestProcessor {
    pub(super) fn handle_get(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        crate::debug_sync_point!("request_processor.handle_get.enter");
        if args.len() != 2 {
            return Err(RequestExecutionError::WrongArity {
                command: "GET",
                expected: "GET key",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        let shard_index = self.string_store_shard_index_for_key(&key);
        self.expire_key_if_needed_in_shard(&key, shard_index)?;
        crate::debug_sync_point!("request_processor.handle_get.before_store_lock");

        let mut store = self.lock_string_store_for_shard(shard_index);
        crate::debug_sync_point!("request_processor.handle_get.after_store_lock");
        let mut session = store.session(&self.functions);
        let mut output = Vec::new();
        let status = session
            .read(&key, &Vec::new(), &mut output, &ReadInfo::default())
            .map_err(map_read_error)?;

        match status {
            ReadOperationStatus::FoundInMemory | ReadOperationStatus::FoundOnDisk => {
                append_bulk_string(response_out, &output);
                Ok(())
            }
            ReadOperationStatus::NotFound => {
                if self.object_key_exists(&key)? {
                    return Err(RequestExecutionError::WrongType);
                }
                append_null_bulk_string(response_out);
                Ok(())
            }
            ReadOperationStatus::RetryLater => Err(RequestExecutionError::StorageBusy),
        }
    }

    pub(super) fn handle_strlen(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 2 {
            return Err(RequestExecutionError::WrongArity {
                command: "STRLEN",
                expected: "STRLEN key",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        let shard_index = self.string_store_shard_index_for_key(&key);
        self.expire_key_if_needed_in_shard(&key, shard_index)?;

        let mut store = self.lock_string_store_for_shard(shard_index);
        let mut session = store.session(&self.functions);
        let mut output = Vec::new();
        let status = session
            .read(&key, &Vec::new(), &mut output, &ReadInfo::default())
            .map_err(map_read_error)?;

        match status {
            ReadOperationStatus::FoundInMemory | ReadOperationStatus::FoundOnDisk => {
                append_integer(response_out, output.len() as i64);
                Ok(())
            }
            ReadOperationStatus::NotFound => {
                if self.object_key_exists(&key)? {
                    return Err(RequestExecutionError::WrongType);
                }
                append_integer(response_out, 0);
                Ok(())
            }
            ReadOperationStatus::RetryLater => Err(RequestExecutionError::StorageBusy),
        }
    }

    pub(super) fn handle_getrange(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_getrange_like(args, response_out, false)
    }

    pub(super) fn handle_substr(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_getrange_like(args, response_out, true)
    }

    fn handle_getrange_like(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
        substr_alias: bool,
    ) -> Result<(), RequestExecutionError> {
        let (command, expected) = if substr_alias {
            ("SUBSTR", "SUBSTR key start end")
        } else {
            ("GETRANGE", "GETRANGE key start end")
        };
        if args.len() != 4 {
            return Err(RequestExecutionError::WrongArity { command, expected });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let start = parse_i64_ascii(unsafe { args[2].as_slice() })
            .ok_or(RequestExecutionError::ValueNotInteger)?;
        // SAFETY: caller guarantees argument backing memory validity.
        let end = parse_i64_ascii(unsafe { args[3].as_slice() })
            .ok_or(RequestExecutionError::ValueNotInteger)?;
        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        let shard_index = self.string_store_shard_index_for_key(&key);
        self.expire_key_if_needed_in_shard(&key, shard_index)?;

        let mut store = self.lock_string_store_for_shard(shard_index);
        let mut session = store.session(&self.functions);
        let mut output = Vec::new();
        let status = session
            .read(&key, &Vec::new(), &mut output, &ReadInfo::default())
            .map_err(map_read_error)?;

        match status {
            ReadOperationStatus::FoundInMemory | ReadOperationStatus::FoundOnDisk => {
                if let Some((start_index, end_index)) =
                    normalize_string_range(output.len(), start, end)
                {
                    append_bulk_string(response_out, &output[start_index..=end_index]);
                } else {
                    append_bulk_string(response_out, b"");
                }
                Ok(())
            }
            ReadOperationStatus::NotFound => {
                if self.object_key_exists(&key)? {
                    return Err(RequestExecutionError::WrongType);
                }
                append_bulk_string(response_out, b"");
                Ok(())
            }
            ReadOperationStatus::RetryLater => Err(RequestExecutionError::StorageBusy),
        }
    }

    pub(super) fn handle_getbit(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 3 {
            return Err(RequestExecutionError::WrongArity {
                command: "GETBIT",
                expected: "GETBIT key offset",
            });
        }
        // SAFETY: caller guarantees argument backing memory validity.
        let offset = parse_i64_ascii(unsafe { args[2].as_slice() })
            .ok_or(RequestExecutionError::ValueNotInteger)?;
        if offset < 0 {
            return Err(RequestExecutionError::ValueOutOfRange);
        }
        let offset = usize::try_from(offset).map_err(|_| RequestExecutionError::ValueOutOfRange)?;
        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        self.expire_key_if_needed(&key)?;
        let Some(value) = self.read_string_value(&key)? else {
            if self.object_key_exists(&key)? {
                return Err(RequestExecutionError::WrongType);
            }
            append_integer(response_out, 0);
            return Ok(());
        };

        let byte_index = offset / 8;
        let old_bit = if byte_index < value.len() {
            let bit_index = 7usize - (offset % 8);
            i64::from((value[byte_index] >> bit_index) & 1)
        } else {
            0
        };
        append_integer(response_out, old_bit);
        Ok(())
    }

    pub(super) fn handle_setbit(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 4 {
            return Err(RequestExecutionError::WrongArity {
                command: "SETBIT",
                expected: "SETBIT key offset value",
            });
        }
        // SAFETY: caller guarantees argument backing memory validity.
        let offset = parse_i64_ascii(unsafe { args[2].as_slice() })
            .ok_or(RequestExecutionError::ValueNotInteger)?;
        if offset < 0 {
            return Err(RequestExecutionError::ValueOutOfRange);
        }
        let offset = usize::try_from(offset).map_err(|_| RequestExecutionError::ValueOutOfRange)?;
        // SAFETY: caller guarantees argument backing memory validity.
        let bit_value = parse_i64_ascii(unsafe { args[3].as_slice() })
            .ok_or(RequestExecutionError::ValueNotInteger)?;
        let bit_value = match bit_value {
            0 => 0u8,
            1 => 1u8,
            _ => return Err(RequestExecutionError::ValueOutOfRange),
        };
        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        self.expire_key_if_needed(&key)?;
        let expiration_unix_millis = self.expiration_unix_millis_for_key(&key);
        let mut value = match self.read_string_value(&key)? {
            Some(value) => value,
            None => {
                if self.object_key_exists(&key)? {
                    return Err(RequestExecutionError::WrongType);
                }
                Vec::new()
            }
        };

        let byte_index = offset / 8;
        let bit_index = 7usize - (offset % 8);
        let old_bit = if byte_index < value.len() {
            i64::from((value[byte_index] >> bit_index) & 1)
        } else {
            0
        };
        let required_len = byte_index
            .checked_add(1)
            .ok_or(RequestExecutionError::ValueOutOfRange)?;
        if value.len() < required_len {
            value.resize(required_len, 0);
        }

        if bit_value == 1 {
            value[byte_index] |= 1u8 << bit_index;
        } else {
            value[byte_index] &= !(1u8 << bit_index);
        }

        self.upsert_string_value_with_expiration_unix_millis(&key, &value, expiration_unix_millis)?;
        self.track_string_key(&key);
        self.bump_watch_version(&key);
        append_integer(response_out, old_bit);
        Ok(())
    }

    pub(super) fn handle_setrange(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 4 {
            return Err(RequestExecutionError::WrongArity {
                command: "SETRANGE",
                expected: "SETRANGE key offset value",
            });
        }
        // SAFETY: caller guarantees argument backing memory validity.
        let offset = parse_i64_ascii(unsafe { args[2].as_slice() })
            .ok_or(RequestExecutionError::ValueNotInteger)?;
        if offset < 0 {
            return Err(RequestExecutionError::ValueOutOfRange);
        }
        let offset = usize::try_from(offset).map_err(|_| RequestExecutionError::ValueOutOfRange)?;
        // SAFETY: caller guarantees argument backing memory validity.
        let new_segment = unsafe { args[3].as_slice() };
        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        self.expire_key_if_needed(&key)?;

        let expiration_unix_millis = self.expiration_unix_millis_for_key(&key);
        let mut value = match self.read_string_value(&key)? {
            Some(value) => value,
            None => {
                if self.object_key_exists(&key)? {
                    return Err(RequestExecutionError::WrongType);
                }
                Vec::new()
            }
        };
        if new_segment.is_empty() {
            append_integer(response_out, value.len() as i64);
            return Ok(());
        }

        let new_len = offset
            .checked_add(new_segment.len())
            .ok_or(RequestExecutionError::ValueOutOfRange)?;
        if value.len() < new_len {
            value.resize(new_len, 0);
        }
        value[offset..offset + new_segment.len()].copy_from_slice(new_segment);

        self.upsert_string_value_with_expiration_unix_millis(&key, &value, expiration_unix_millis)?;
        self.track_string_key(&key);
        self.bump_watch_version(&key);
        append_integer(response_out, value.len() as i64);
        Ok(())
    }

    pub(super) fn handle_bitcount(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 2 && args.len() != 4 && args.len() != 5 {
            return Err(RequestExecutionError::WrongArity {
                command: "BITCOUNT",
                expected: "BITCOUNT key [start end [BYTE|BIT]]",
            });
        }
        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        self.expire_key_if_needed(&key)?;
        let Some(value) = self.read_string_value(&key)? else {
            if self.object_key_exists(&key)? {
                return Err(RequestExecutionError::WrongType);
            }
            append_integer(response_out, 0);
            return Ok(());
        };
        if value.is_empty() {
            append_integer(response_out, 0);
            return Ok(());
        }
        if args.len() == 2 {
            let bits = value.iter().map(|byte| i64::from(byte.count_ones())).sum();
            append_integer(response_out, bits);
            return Ok(());
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let start = parse_i64_ascii(unsafe { args[2].as_slice() })
            .ok_or(RequestExecutionError::ValueNotInteger)?;
        // SAFETY: caller guarantees argument backing memory validity.
        let end = parse_i64_ascii(unsafe { args[3].as_slice() })
            .ok_or(RequestExecutionError::ValueNotInteger)?;
        let bit_mode = if args.len() == 5 {
            // SAFETY: caller guarantees argument backing memory validity.
            let mode = unsafe { args[4].as_slice() };
            if ascii_eq_ignore_case(mode, b"BYTE") {
                false
            } else if ascii_eq_ignore_case(mode, b"BIT") {
                true
            } else {
                return Err(RequestExecutionError::SyntaxError);
            }
        } else {
            false
        };

        let count = if bit_mode {
            let total_bits = value
                .len()
                .checked_mul(8)
                .ok_or(RequestExecutionError::ValueOutOfRange)?;
            if let Some((start_bit, end_bit)) = normalize_string_range(total_bits, start, end) {
                let mut count = 0i64;
                for bit_index in start_bit..=end_bit {
                    let byte = value[bit_index / 8];
                    let shift = 7usize - (bit_index % 8);
                    count += i64::from((byte >> shift) & 1);
                }
                count
            } else {
                0
            }
        } else if let Some((start_byte, end_byte)) = normalize_string_range(value.len(), start, end)
        {
            value[start_byte..=end_byte]
                .iter()
                .map(|byte| i64::from(byte.count_ones()))
                .sum()
        } else {
            0
        };
        append_integer(response_out, count);
        Ok(())
    }

    pub(super) fn handle_bitop(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() < 4 {
            return Err(RequestExecutionError::WrongArity {
                command: "BITOP",
                expected: "BITOP operation destkey key [key ...]",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let operation = parse_bitop_operation(unsafe { args[1].as_slice() })
            .ok_or(RequestExecutionError::SyntaxError)?;
        if operation == BitopOperation::Not && args.len() != 4 {
            return Err(RequestExecutionError::WrongArity {
                command: "BITOP",
                expected: "BITOP NOT destkey key",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let destination = unsafe { args[2].as_slice() }.to_vec();
        let source_keys = args[3..]
            .iter()
            .map(|key| {
                // SAFETY: caller guarantees argument backing memory validity.
                unsafe { key.as_slice() }.to_vec()
            })
            .collect::<Vec<_>>();

        let mut source_values = Vec::with_capacity(source_keys.len());
        for key in &source_keys {
            self.expire_key_if_needed(key)?;
            let value = match self.read_string_value(key)? {
                Some(value) => value,
                None => {
                    if self.object_key_exists(key)? {
                        return Err(RequestExecutionError::WrongType);
                    }
                    Vec::new()
                }
            };
            source_values.push(value);
        }

        let result = apply_bitop(operation, &source_values);
        if result.is_empty() {
            self.delete_string_key_for_migration(&destination)?;
            let _ = self.object_delete(&destination)?;
        } else {
            let _ = self.object_delete(&destination)?;
            self.upsert_string_value_for_migration(&destination, &result, None)?;
        }
        append_integer(response_out, result.len() as i64);
        Ok(())
    }

    fn upsert_string_value_with_expiration_unix_millis(
        &self,
        key: &[u8],
        value: &[u8],
        expiration_unix_millis: Option<u64>,
    ) -> Result<(), RequestExecutionError> {
        let mut upsert_info = UpsertInfo::default();
        if expiration_unix_millis.is_some() {
            upsert_info.user_data |= UPSERT_USER_DATA_HAS_EXPIRATION;
        }
        let stored_value = encode_stored_value(value, expiration_unix_millis);
        let mut output = Vec::new();
        let mut store = self.lock_string_store_for_key(key);
        let mut session = store.session(&self.functions);
        session
            .upsert(&key.to_vec(), &stored_value, &mut output, &mut upsert_info)
            .map_err(map_upsert_error)?;
        Ok(())
    }

    pub(super) fn handle_append(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 3 {
            return Err(RequestExecutionError::WrongArity {
                command: "APPEND",
                expected: "APPEND key value",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        // SAFETY: caller guarantees argument backing memory validity.
        let append_value = unsafe { args[2].as_slice() };
        let shard_index = self.string_store_shard_index_for_key(&key);
        self.expire_key_if_needed_in_shard(&key, shard_index)?;

        let mut store = self.lock_string_store_for_shard(shard_index);
        let mut session = store.session(&self.functions);
        let mut object_store = self.lock_object_store_for_shard(shard_index);
        let mut object_session = object_store.session(&self.object_functions);

        let mut current_value = Vec::new();
        let string_exists = match session
            .read(&key, &Vec::new(), &mut current_value, &ReadInfo::default())
            .map_err(map_read_error)?
        {
            ReadOperationStatus::FoundInMemory | ReadOperationStatus::FoundOnDisk => true,
            ReadOperationStatus::NotFound => false,
            ReadOperationStatus::RetryLater => return Err(RequestExecutionError::StorageBusy),
        };

        let mut object_output = Vec::new();
        let object_exists = match object_session
            .read(&key, &Vec::new(), &mut object_output, &ReadInfo::default())
            .map_err(map_read_error)?
        {
            ReadOperationStatus::FoundInMemory | ReadOperationStatus::FoundOnDisk => true,
            ReadOperationStatus::NotFound => false,
            ReadOperationStatus::RetryLater => return Err(RequestExecutionError::StorageBusy),
        };
        if object_exists {
            return Err(RequestExecutionError::WrongType);
        }

        if !string_exists {
            current_value.clear();
        }
        current_value.extend_from_slice(append_value);
        let expiration_unix_millis = self.expiration_unix_millis_for_key(&key);

        let mut upsert_info = UpsertInfo::default();
        if expiration_unix_millis.is_some() {
            upsert_info.user_data |= UPSERT_USER_DATA_HAS_EXPIRATION;
        }
        let stored_value = encode_stored_value(&current_value, expiration_unix_millis);
        let mut upsert_output = Vec::new();
        session
            .upsert(&key, &stored_value, &mut upsert_output, &mut upsert_info)
            .map_err(map_upsert_error)?;
        drop(object_session);
        drop(object_store);
        drop(session);
        drop(store);

        self.track_string_key_in_shard(&key, shard_index);
        self.bump_watch_version(&key);
        append_integer(response_out, current_value.len() as i64);
        Ok(())
    }

    pub(super) fn handle_getex(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        let action = parse_getex_action(args)?;
        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        self.expire_key_if_needed(&key)?;

        let Some(value) = self.read_string_value(&key)? else {
            if self.object_key_exists(&key)? {
                return Err(RequestExecutionError::WrongType);
            }
            append_null_bulk_string(response_out);
            return Ok(());
        };

        match action {
            GetExAction::KeepTtl => {}
            GetExAction::Persist => {
                if self.string_expiration_deadline(&key).is_some() {
                    self.set_string_expiration_deadline(&key, None);
                    if !self.rewrite_existing_value_expiration(&key, None)? {
                        return Err(storage_failure(
                            "getex",
                            "string key disappeared while clearing expiration",
                        ));
                    }
                    self.bump_watch_version(&key);
                }
            }
            GetExAction::SetExpiration(expiration) => {
                self.set_string_expiration_deadline(&key, Some(expiration.deadline));
                if !self.rewrite_existing_value_expiration(&key, Some(expiration.unix_millis))? {
                    self.set_string_expiration_deadline(&key, None);
                    return Err(storage_failure(
                        "getex",
                        "string key disappeared while rewriting expiration",
                    ));
                }
                self.bump_watch_version(&key);
            }
            GetExAction::DeleteNow => {
                let mut store = self.lock_string_store_for_key(&key);
                let mut session = store.session(&self.functions);
                let mut info = DeleteInfo::default();
                let status = session.delete(&key, &mut info).map_err(map_delete_error)?;
                match status {
                    DeleteOperationStatus::TombstonedInPlace
                    | DeleteOperationStatus::AppendedTombstone => {
                        self.remove_string_key_metadata(&key);
                        self.bump_watch_version(&key);
                    }
                    DeleteOperationStatus::NotFound => {}
                    DeleteOperationStatus::RetryLater => {
                        return Err(RequestExecutionError::StorageBusy);
                    }
                }
            }
        }

        append_bulk_string(response_out, &value);
        Ok(())
    }

    pub(super) fn handle_incrbyfloat(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 3 {
            return Err(RequestExecutionError::WrongArity {
                command: "INCRBYFLOAT",
                expected: "INCRBYFLOAT key increment",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let increment = parse_f64_ascii(unsafe { args[2].as_slice() })
            .ok_or(RequestExecutionError::ValueNotFloat)?;
        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        self.expire_key_if_needed(&key)?;

        let (current, expiration_unix_millis) = match self.read_string_value(&key)? {
            Some(value) => (
                parse_f64_ascii(&value).ok_or(RequestExecutionError::ValueNotFloat)?,
                self.expiration_unix_millis_for_key(&key),
            ),
            None => {
                if self.object_key_exists(&key)? {
                    return Err(RequestExecutionError::WrongType);
                }
                (0.0, None)
            }
        };

        let updated = current + increment;
        if !updated.is_finite() {
            return Err(RequestExecutionError::ValueNotFloat);
        }
        let updated_text = updated.to_string().into_bytes();

        let mut upsert_info = UpsertInfo::default();
        if expiration_unix_millis.is_some() {
            upsert_info.user_data |= UPSERT_USER_DATA_HAS_EXPIRATION;
        }
        let stored_value = encode_stored_value(&updated_text, expiration_unix_millis);
        let mut upsert_output = Vec::new();
        let mut store = self.lock_string_store_for_key(&key);
        let mut session = store.session(&self.functions);
        session
            .upsert(&key, &stored_value, &mut upsert_output, &mut upsert_info)
            .map_err(map_upsert_error)?;
        self.track_string_key(&key);
        self.bump_watch_version(&key);
        append_bulk_string(response_out, &updated_text);
        Ok(())
    }

    pub(super) fn handle_set(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        crate::debug_sync_point!("request_processor.handle_set.enter");
        if args.len() < 3 {
            return Err(RequestExecutionError::WrongArity {
                command: "SET",
                expected: "SET key value",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        // SAFETY: caller guarantees argument backing memory validity.
        let value = unsafe { args[2].as_slice() }.to_vec();
        let shard_index = self.string_store_shard_index_for_key(&key);
        let options = parse_set_options(args)?;
        self.expire_key_if_needed_in_shard(&key, shard_index)?;
        crate::debug_sync_point!("request_processor.handle_set.before_store_lock");

        let mut store = self.lock_string_store_for_shard(shard_index);
        crate::debug_sync_point!("request_processor.handle_set.after_store_lock");
        let mut session = store.session(&self.functions);
        let mut object_store = self.lock_object_store_for_shard(shard_index);
        let mut object_session = object_store.session(&self.object_functions);

        let mut existence_output = Vec::new();
        let string_exists = match session
            .read(
                &key,
                &Vec::new(),
                &mut existence_output,
                &ReadInfo::default(),
            )
            .map_err(map_read_error)?
        {
            ReadOperationStatus::FoundInMemory | ReadOperationStatus::FoundOnDisk => true,
            ReadOperationStatus::NotFound => false,
            ReadOperationStatus::RetryLater => return Err(RequestExecutionError::StorageBusy),
        };

        existence_output.clear();
        let object_exists = match object_session
            .read(
                &key,
                &Vec::new(),
                &mut existence_output,
                &ReadInfo::default(),
            )
            .map_err(map_read_error)?
        {
            ReadOperationStatus::FoundInMemory | ReadOperationStatus::FoundOnDisk => true,
            ReadOperationStatus::NotFound => false,
            ReadOperationStatus::RetryLater => return Err(RequestExecutionError::StorageBusy),
        };

        if options.only_if_absent || options.only_if_present {
            let exists_any = string_exists || object_exists;
            if options.only_if_absent && exists_any {
                append_null_bulk_string(response_out);
                return Ok(());
            }
            if options.only_if_present && !exists_any {
                append_null_bulk_string(response_out);
                return Ok(());
            }
        }

        let mut output = Vec::new();
        let mut info = UpsertInfo::default();
        let stored_value = encode_stored_value(
            &value,
            options.expiration.map(|expiration| expiration.unix_millis),
        );
        if options.expiration.is_some() {
            info.user_data |= UPSERT_USER_DATA_HAS_EXPIRATION;
        }
        session
            .upsert(&key, &stored_value, &mut output, &mut info)
            .map_err(map_upsert_error)?;
        if object_exists {
            let mut delete_info = DeleteInfo::default();
            let status = object_session
                .delete(&key, &mut delete_info)
                .map_err(map_delete_error)?;
            if matches!(status, DeleteOperationStatus::RetryLater) {
                return Err(RequestExecutionError::StorageBusy);
            }
        }
        drop(object_session);
        drop(object_store);
        drop(session);
        drop(store);
        crate::debug_sync_point!("request_processor.handle_set.before_metadata_locks");

        if object_exists {
            self.untrack_object_key_in_shard(&key, shard_index);
        }
        self.set_string_expiration_deadline_in_shard(
            &key,
            shard_index,
            options.expiration.map(|e| e.deadline),
        );
        self.track_string_key_in_shard(&key, shard_index);
        self.bump_watch_version(&key);

        append_simple_string(response_out, b"OK");
        Ok(())
    }

    pub(super) fn handle_setnx(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 3 {
            return Err(RequestExecutionError::WrongArity {
                command: "SETNX",
                expected: "SETNX key value",
            });
        }
        let set_command = ArgSlice::from_slice(b"SET")
            .expect("static SET command name must fit within ArgSlice length");
        let nx_option =
            ArgSlice::from_slice(b"NX").expect("static NX option must fit within ArgSlice length");
        let translated = [set_command, args[1], args[2], nx_option];
        let mut set_response = Vec::new();
        self.handle_set(&translated, &mut set_response)?;
        if set_response == b"+OK\r\n" {
            append_integer(response_out, 1);
        } else {
            append_integer(response_out, 0);
        }
        Ok(())
    }

    pub(super) fn handle_setex(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 4 {
            return Err(RequestExecutionError::WrongArity {
                command: "SETEX",
                expected: "SETEX key seconds value",
            });
        }
        let set_command = ArgSlice::from_slice(b"SET")
            .expect("static SET command name must fit within ArgSlice length");
        let ex_option =
            ArgSlice::from_slice(b"EX").expect("static EX option must fit within ArgSlice length");
        let translated = [set_command, args[1], args[3], ex_option, args[2]];
        self.handle_set(&translated, response_out)
    }

    pub(super) fn handle_psetex(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 4 {
            return Err(RequestExecutionError::WrongArity {
                command: "PSETEX",
                expected: "PSETEX key milliseconds value",
            });
        }
        let set_command = ArgSlice::from_slice(b"SET")
            .expect("static SET command name must fit within ArgSlice length");
        let px_option =
            ArgSlice::from_slice(b"PX").expect("static PX option must fit within ArgSlice length");
        let translated = [set_command, args[1], args[3], px_option, args[2]];
        self.handle_set(&translated, response_out)
    }

    pub(super) fn handle_getset(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 3 {
            return Err(RequestExecutionError::WrongArity {
                command: "GETSET",
                expected: "GETSET key value",
            });
        }

        let get_command = ArgSlice::from_slice(b"GET")
            .expect("static GET command name must fit within ArgSlice length");
        let set_command = ArgSlice::from_slice(b"SET")
            .expect("static SET command name must fit within ArgSlice length");

        let mut previous_value_response = Vec::new();
        let get_args = [get_command, args[1]];
        self.handle_get(&get_args, &mut previous_value_response)?;

        let set_args = [set_command, args[1], args[2]];
        let mut set_response = Vec::new();
        self.handle_set(&set_args, &mut set_response)?;

        response_out.extend_from_slice(&previous_value_response);
        Ok(())
    }

    pub(super) fn handle_getdel(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 2 {
            return Err(RequestExecutionError::WrongArity {
                command: "GETDEL",
                expected: "GETDEL key",
            });
        }

        let get_command = ArgSlice::from_slice(b"GET")
            .expect("static GET command name must fit within ArgSlice length");
        let del_command = ArgSlice::from_slice(b"DEL")
            .expect("static DEL command name must fit within ArgSlice length");

        let get_args = [get_command, args[1]];
        let mut previous_value_response = Vec::new();
        self.handle_get(&get_args, &mut previous_value_response)?;
        if previous_value_response.as_slice() == b"$-1\r\n" {
            response_out.extend_from_slice(&previous_value_response);
            return Ok(());
        }

        let mut del_response = Vec::new();
        let del_args = [del_command, args[1]];
        self.handle_del(&del_args, &mut del_response)?;
        response_out.extend_from_slice(&previous_value_response);
        Ok(())
    }

    pub(super) fn handle_del(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() < 2 {
            return Err(RequestExecutionError::WrongArity {
                command: "DEL",
                expected: "DEL key [key ...]",
            });
        }

        let keys: Vec<Vec<u8>> = args[1..]
            .iter()
            .map(|arg| {
                // SAFETY: caller guarantees argument backing memory validity.
                unsafe { arg.as_slice() }.to_vec()
            })
            .collect();

        for key in &keys {
            self.expire_key_if_needed(key)?;
        }

        let mut deleted = 0i64;
        for key in keys {
            let mut string_deleted = false;
            let mut store = self.lock_string_store_for_key(&key);
            let mut session = store.session(&self.functions);
            let mut info = DeleteInfo::default();
            let status = session.delete(&key, &mut info).map_err(map_delete_error)?;
            match status {
                DeleteOperationStatus::TombstonedInPlace
                | DeleteOperationStatus::AppendedTombstone => {
                    string_deleted = true;
                    self.remove_string_key_metadata(&key);
                }
                DeleteOperationStatus::NotFound => {}
                DeleteOperationStatus::RetryLater => {
                    return Err(RequestExecutionError::StorageBusy);
                }
            }

            let object_deleted = self.object_delete(&key)?;
            if string_deleted || object_deleted {
                deleted += 1;
            }
            if string_deleted && !object_deleted {
                self.bump_watch_version(&key);
            }
        }

        append_integer(response_out, deleted);
        Ok(())
    }

    pub(super) fn handle_touch(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() < 2 {
            return Err(RequestExecutionError::WrongArity {
                command: "TOUCH",
                expected: "TOUCH key [key ...]",
            });
        }

        let mut touched = 0i64;
        for arg in &args[1..] {
            // SAFETY: caller guarantees argument backing memory validity.
            let key = unsafe { arg.as_slice() }.to_vec();
            self.expire_key_if_needed(&key)?;
            if self.key_exists_any(&key)? {
                touched += 1;
            }
        }
        append_integer(response_out, touched);
        Ok(())
    }

    pub(super) fn handle_unlink(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() < 2 {
            return Err(RequestExecutionError::WrongArity {
                command: "UNLINK",
                expected: "UNLINK key [key ...]",
            });
        }
        self.handle_del(args, response_out)
    }

    pub(super) fn handle_rename(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_rename_internal(args, false, response_out)
    }

    pub(super) fn handle_renamenx(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_rename_internal(args, true, response_out)
    }

    fn handle_rename_internal(
        &self,
        args: &[ArgSlice],
        only_if_absent: bool,
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 3 {
            return Err(RequestExecutionError::WrongArity {
                command: if only_if_absent { "RENAMENX" } else { "RENAME" },
                expected: if only_if_absent {
                    "RENAMENX key newkey"
                } else {
                    "RENAME key newkey"
                },
            });
        }
        // SAFETY: caller guarantees argument backing memory validity.
        let source = unsafe { args[1].as_slice() }.to_vec();
        // SAFETY: caller guarantees argument backing memory validity.
        let destination = unsafe { args[2].as_slice() }.to_vec();

        self.expire_key_if_needed(&source)?;
        self.expire_key_if_needed(&destination)?;

        let Some(mut source_entry) = self.export_migration_entry(&source)? else {
            return Err(RequestExecutionError::NoSuchKey);
        };

        if source == destination {
            if only_if_absent {
                append_integer(response_out, 0);
            } else {
                append_simple_string(response_out, b"OK");
            }
            return Ok(());
        }

        if only_if_absent && self.key_exists_any(&destination)? {
            append_integer(response_out, 0);
            return Ok(());
        }

        source_entry.key = destination.clone();
        self.import_migration_entry(&source_entry)?;
        self.delete_string_key_for_migration(&source)?;
        let _ = self.object_delete(&source)?;

        if only_if_absent {
            append_integer(response_out, 1);
        } else {
            append_simple_string(response_out, b"OK");
        }
        Ok(())
    }

    pub(super) fn handle_copy(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() < 3 {
            return Err(RequestExecutionError::WrongArity {
                command: "COPY",
                expected: "COPY source destination [DB destination-db] [REPLACE]",
            });
        }
        // SAFETY: caller guarantees argument backing memory validity.
        let source = unsafe { args[1].as_slice() }.to_vec();
        // SAFETY: caller guarantees argument backing memory validity.
        let destination = unsafe { args[2].as_slice() }.to_vec();

        let mut replace = false;
        let mut destination_db = 0u64;
        let mut index = 3usize;
        while index < args.len() {
            // SAFETY: caller guarantees argument backing memory validity.
            let option = unsafe { args[index].as_slice() };
            if ascii_eq_ignore_case(option, b"REPLACE") {
                replace = true;
                index += 1;
                continue;
            }
            if ascii_eq_ignore_case(option, b"DB") {
                if index + 1 >= args.len() {
                    return Err(RequestExecutionError::SyntaxError);
                }
                // SAFETY: caller guarantees argument backing memory validity.
                let db_value = unsafe { args[index + 1].as_slice() };
                destination_db =
                    parse_u64_ascii(db_value).ok_or(RequestExecutionError::ValueNotInteger)?;
                index += 2;
                continue;
            }
            return Err(RequestExecutionError::SyntaxError);
        }

        if destination_db != 0 {
            return Err(RequestExecutionError::ValueNotInteger);
        }

        self.expire_key_if_needed(&source)?;
        self.expire_key_if_needed(&destination)?;

        let Some(mut source_entry) = self.export_migration_entry(&source)? else {
            append_integer(response_out, 0);
            return Ok(());
        };

        if source == destination {
            append_integer(response_out, 0);
            return Ok(());
        }

        if !replace && self.key_exists_any(&destination)? {
            append_integer(response_out, 0);
            return Ok(());
        }

        source_entry.key = destination;
        self.import_migration_entry(&source_entry)?;
        append_integer(response_out, 1);
        Ok(())
    }

    pub(super) fn handle_incr_decr(
        &self,
        args: &[ArgSlice],
        delta: i64,
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 2 {
            return Err(RequestExecutionError::WrongArity {
                command: if delta > 0 { "INCR" } else { "DECR" },
                expected: if delta > 0 { "INCR key" } else { "DECR key" },
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        self.apply_incr_decr_delta(&key, delta, response_out)
    }

    pub(super) fn handle_incrby_decrby(
        &self,
        args: &[ArgSlice],
        decrement: bool,
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        let (command, expected) = if decrement {
            ("DECRBY", "DECRBY key decrement")
        } else {
            ("INCRBY", "INCRBY key increment")
        };
        if args.len() != 3 {
            return Err(RequestExecutionError::WrongArity { command, expected });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let amount = parse_i64_ascii(unsafe { args[2].as_slice() })
            .ok_or(RequestExecutionError::ValueNotInteger)?;
        let delta = if decrement {
            amount
                .checked_neg()
                .ok_or(RequestExecutionError::ValueNotInteger)?
        } else {
            amount
        };
        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        self.apply_incr_decr_delta(&key, delta, response_out)
    }

    fn apply_incr_decr_delta(
        &self,
        key: &[u8],
        delta: i64,
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.expire_key_if_needed(&key)?;
        if !self.key_exists(&key)? && self.object_key_exists(&key)? {
            return Err(RequestExecutionError::WrongType);
        }
        let input = delta.to_string().into_bytes();
        let mut output = Vec::new();
        let mut info = RmwInfo::default();
        let mut store = self.lock_string_store_for_key(&key);
        let mut session = store.session(&self.functions);
        let status = session.rmw(&key.to_vec(), &input, &mut output, &mut info);

        match status {
            Ok(RmwOperationStatus::InPlaceUpdated)
            | Ok(RmwOperationStatus::CopiedToTail)
            | Ok(RmwOperationStatus::Inserted) => {
                let parsed =
                    parse_i64_ascii(&output).ok_or(RequestExecutionError::ValueNotInteger)?;
                self.track_string_key(&key);
                self.bump_watch_version(&key);
                append_integer(response_out, parsed);
                Ok(())
            }
            Ok(RmwOperationStatus::RetryLater) => Err(RequestExecutionError::StorageBusy),
            Ok(RmwOperationStatus::NotFound) => {
                if self.object_key_exists(&key)? {
                    return Err(RequestExecutionError::WrongType);
                }
                let mut upsert_info = UpsertInfo::default();
                let mut upsert_output = Vec::new();
                let stored_value = encode_stored_value(&input, None);
                session
                    .upsert(
                        &key.to_vec(),
                        &stored_value,
                        &mut upsert_output,
                        &mut upsert_info,
                    )
                    .map_err(map_upsert_error)?;
                self.track_string_key(&key);
                self.bump_watch_version(&key);
                append_integer(response_out, delta);
                Ok(())
            }
            Err(RmwOperationError::OperationCancelled) => {
                Err(RequestExecutionError::ValueNotInteger)
            }
            Err(error) => Err(map_rmw_error(error)),
        }
    }

    pub(super) fn handle_exists(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() < 2 {
            return Err(RequestExecutionError::WrongArity {
                command: "EXISTS",
                expected: "EXISTS key [key ...]",
            });
        }

        let mut exists = 0i64;
        for arg in &args[1..] {
            // SAFETY: caller guarantees argument backing memory validity.
            let key = unsafe { arg.as_slice() }.to_vec();
            self.expire_key_if_needed(&key)?;
            if self.key_exists_any(&key)? {
                exists += 1;
            }
        }

        append_integer(response_out, exists);
        Ok(())
    }

    pub(super) fn handle_type(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 2 {
            return Err(RequestExecutionError::WrongArity {
                command: "TYPE",
                expected: "TYPE key",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        self.expire_key_if_needed(&key)?;
        if self.key_exists(&key)? {
            append_simple_string(response_out, b"string");
            return Ok(());
        }

        let value_type = match self.object_read(&key)? {
            Some((object_type, _)) => object_type_name(object_type).ok_or_else(|| {
                storage_failure("type", "unknown object type tag in object store")
            })?,
            None => b"none",
        };
        append_simple_string(response_out, value_type);
        Ok(())
    }

    pub(super) fn handle_mget(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() < 2 {
            return Err(RequestExecutionError::WrongArity {
                command: "MGET",
                expected: "MGET key [key ...]",
            });
        }

        response_out.push(b'*');
        response_out.extend_from_slice((args.len() - 1).to_string().as_bytes());
        response_out.extend_from_slice(b"\r\n");
        for arg in &args[1..] {
            // SAFETY: caller guarantees argument backing memory validity.
            let key = unsafe { arg.as_slice() }.to_vec();
            self.expire_key_if_needed(&key)?;
            if let Some(value) = self.read_string_value(&key)? {
                append_bulk_string(response_out, &value);
            } else {
                append_null_bulk_string(response_out);
            }
        }
        Ok(())
    }

    pub(super) fn handle_mset(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() < 3 || args.len() % 2 == 0 {
            return Err(RequestExecutionError::WrongArity {
                command: "MSET",
                expected: "MSET key value [key value ...]",
            });
        }

        for pair in args[1..].chunks_exact(2) {
            // SAFETY: caller guarantees argument backing memory validity.
            let key = unsafe { pair[0].as_slice() }.to_vec();
            // SAFETY: caller guarantees argument backing memory validity.
            let value = unsafe { pair[1].as_slice() }.to_vec();
            self.mset_single_pair(&key, &value)?;
        }

        append_simple_string(response_out, b"OK");
        Ok(())
    }

    pub(super) fn handle_msetnx(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() < 3 || args.len() % 2 == 0 {
            return Err(RequestExecutionError::WrongArity {
                command: "MSETNX",
                expected: "MSETNX key value [key value ...]",
            });
        }

        let key_value_pairs: Vec<(Vec<u8>, Vec<u8>)> = args[1..]
            .chunks_exact(2)
            .map(|pair| {
                // SAFETY: caller guarantees argument backing memory validity.
                (
                    unsafe { pair[0].as_slice() }.to_vec(),
                    // SAFETY: caller guarantees argument backing memory validity.
                    unsafe { pair[1].as_slice() }.to_vec(),
                )
            })
            .collect();

        for (key, _) in &key_value_pairs {
            self.expire_key_if_needed(key)?;
            if self.key_exists_any(key)? {
                append_integer(response_out, 0);
                return Ok(());
            }
        }

        for (key, value) in &key_value_pairs {
            self.mset_single_pair(key, value)?;
        }
        append_integer(response_out, 1);
        Ok(())
    }

    fn mset_single_pair(&self, key: &[u8], value: &[u8]) -> Result<(), RequestExecutionError> {
        let shard_index = self.string_store_shard_index_for_key(key);
        self.expire_key_if_needed_in_shard(key, shard_index)?;

        let key_vec = key.to_vec();
        let mut store = self.lock_string_store_for_shard(shard_index);
        let mut session = store.session(&self.functions);
        let mut object_store = self.lock_object_store_for_shard(shard_index);
        let mut object_session = object_store.session(&self.object_functions);

        let mut existence_output = Vec::new();
        let object_exists = match object_session
            .read(
                &key_vec,
                &Vec::new(),
                &mut existence_output,
                &ReadInfo::default(),
            )
            .map_err(map_read_error)?
        {
            ReadOperationStatus::FoundInMemory | ReadOperationStatus::FoundOnDisk => true,
            ReadOperationStatus::NotFound => false,
            ReadOperationStatus::RetryLater => return Err(RequestExecutionError::StorageBusy),
        };

        let mut output = Vec::new();
        let mut info = UpsertInfo::default();
        let stored_value = encode_stored_value(value, None);
        session
            .upsert(&key_vec, &stored_value, &mut output, &mut info)
            .map_err(map_upsert_error)?;
        if object_exists {
            let mut delete_info = DeleteInfo::default();
            let status = object_session
                .delete(&key_vec, &mut delete_info)
                .map_err(map_delete_error)?;
            if matches!(status, DeleteOperationStatus::RetryLater) {
                return Err(RequestExecutionError::StorageBusy);
            }
        }
        drop(object_session);
        drop(object_store);
        drop(session);
        drop(store);

        if object_exists {
            self.untrack_object_key_in_shard(&key_vec, shard_index);
        }
        self.set_string_expiration_deadline_in_shard(&key_vec, shard_index, None);
        self.track_string_key_in_shard(&key_vec, shard_index);
        self.bump_watch_version(&key_vec);
        Ok(())
    }

    pub(super) fn handle_expire(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_expire_like(args, response_out, false)
    }

    pub(super) fn handle_pexpire(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_expire_like(args, response_out, true)
    }

    pub(super) fn handle_expireat(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_expireat_like(args, response_out, false)
    }

    pub(super) fn handle_pexpireat(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_expireat_like(args, response_out, true)
    }

    fn handle_expire_like(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
        milliseconds: bool,
    ) -> Result<(), RequestExecutionError> {
        let (command, expected) = if milliseconds {
            ("PEXPIRE", "PEXPIRE key milliseconds")
        } else {
            ("EXPIRE", "EXPIRE key seconds")
        };
        if args.len() != 3 {
            return Err(RequestExecutionError::WrongArity { command, expected });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let amount = parse_i64_ascii(unsafe { args[2].as_slice() })
            .ok_or(RequestExecutionError::ValueNotInteger)?;
        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();

        self.expire_key_if_needed(&key)?;
        let string_exists = self.key_exists(&key)?;
        let object_exists = self.object_key_exists(&key)?;
        if !string_exists && !object_exists {
            append_integer(response_out, 0);
            return Ok(());
        }

        if amount <= 0 {
            return self.expire_existing_key_immediately(
                &key,
                string_exists,
                object_exists,
                response_out,
            );
        }

        let duration = if milliseconds {
            Duration::from_millis(amount as u64)
        } else {
            Duration::from_secs(amount as u64)
        };
        let expiration = expiration_metadata_from_duration(duration)
            .ok_or(RequestExecutionError::ValueNotInteger)?;
        self.set_string_expiration_deadline(&key, Some(expiration.deadline));
        if string_exists
            && !self.rewrite_existing_value_expiration(&key, Some(expiration.unix_millis))?
        {
            self.set_string_expiration_deadline(&key, None);
            append_integer(response_out, 0);
            return Ok(());
        }
        self.bump_watch_version(&key);
        append_integer(response_out, 1);
        Ok(())
    }

    fn handle_expireat_like(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
        milliseconds: bool,
    ) -> Result<(), RequestExecutionError> {
        let (command, expected) = if milliseconds {
            ("PEXPIREAT", "PEXPIREAT key milliseconds-unix-time")
        } else {
            ("EXPIREAT", "EXPIREAT key seconds-unix-time")
        };
        if args.len() != 3 {
            return Err(RequestExecutionError::WrongArity { command, expected });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let amount = parse_i64_ascii(unsafe { args[2].as_slice() })
            .ok_or(RequestExecutionError::ValueNotInteger)?;
        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();

        self.expire_key_if_needed(&key)?;
        let string_exists = self.key_exists(&key)?;
        let object_exists = self.object_key_exists(&key)?;
        if !string_exists && !object_exists {
            append_integer(response_out, 0);
            return Ok(());
        }

        let target_unix_millis = if milliseconds {
            i128::from(amount)
        } else {
            i128::from(amount)
                .checked_mul(1000)
                .ok_or(RequestExecutionError::ValueNotInteger)?
        };
        let now_unix_millis =
            i128::from(current_unix_time_millis().ok_or(RequestExecutionError::ValueNotInteger)?);

        if target_unix_millis <= now_unix_millis {
            return self.expire_existing_key_immediately(
                &key,
                string_exists,
                object_exists,
                response_out,
            );
        }

        let unix_millis = u64::try_from(target_unix_millis)
            .map_err(|_| RequestExecutionError::ValueNotInteger)?;
        let deadline =
            instant_from_unix_millis(unix_millis).ok_or(RequestExecutionError::ValueNotInteger)?;
        self.set_string_expiration_deadline(&key, Some(deadline));
        if string_exists && !self.rewrite_existing_value_expiration(&key, Some(unix_millis))? {
            self.set_string_expiration_deadline(&key, None);
            append_integer(response_out, 0);
            return Ok(());
        }
        self.bump_watch_version(&key);
        append_integer(response_out, 1);
        Ok(())
    }

    fn expire_existing_key_immediately(
        &self,
        key: &[u8],
        string_exists: bool,
        object_exists: bool,
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        let key_vec = key.to_vec();
        let mut string_deleted = false;
        if string_exists {
            let mut store = self.lock_string_store_for_key(key);
            let mut session = store.session(&self.functions);
            let mut info = DeleteInfo::default();
            let status = session
                .delete(&key_vec, &mut info)
                .map_err(map_delete_error)?;
            match status {
                DeleteOperationStatus::TombstonedInPlace
                | DeleteOperationStatus::AppendedTombstone => {
                    string_deleted = true;
                }
                DeleteOperationStatus::NotFound => {}
                DeleteOperationStatus::RetryLater => {
                    return Err(RequestExecutionError::StorageBusy);
                }
            }
            self.remove_string_key_metadata(key);
        }

        let object_deleted = if object_exists {
            self.object_delete(key)?
        } else {
            false
        };
        if string_deleted || object_deleted {
            if string_deleted && !object_deleted {
                self.bump_watch_version(key);
            }
            append_integer(response_out, 1);
        } else {
            append_integer(response_out, 0);
        }
        Ok(())
    }

    pub(super) fn handle_ttl(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_ttl_like(args, response_out, false)
    }

    pub(super) fn handle_pttl(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_ttl_like(args, response_out, true)
    }

    pub(super) fn handle_expiretime(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_expiretime_like(args, response_out, false)
    }

    pub(super) fn handle_pexpiretime(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_expiretime_like(args, response_out, true)
    }

    fn handle_ttl_like(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
        milliseconds: bool,
    ) -> Result<(), RequestExecutionError> {
        let (command, expected) = if milliseconds {
            ("PTTL", "PTTL key")
        } else {
            ("TTL", "TTL key")
        };
        if args.len() != 2 {
            return Err(RequestExecutionError::WrongArity { command, expected });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        self.expire_key_if_needed(&key)?;

        if !self.key_exists_any(&key)? {
            append_integer(response_out, -2);
            return Ok(());
        }

        let deadline = self.string_expiration_deadline(&key);
        match deadline {
            None => append_integer(response_out, -1),
            Some(deadline) => {
                let now = Instant::now();
                if deadline <= now {
                    self.expire_key_if_needed(&key)?;
                    append_integer(response_out, -2);
                } else {
                    let remaining = deadline.duration_since(now);
                    let ttl = if milliseconds {
                        remaining.as_millis().min(i64::MAX as u128) as i64
                    } else {
                        remaining.as_secs().min(i64::MAX as u64) as i64
                    };
                    append_integer(response_out, ttl);
                }
            }
        }
        Ok(())
    }

    fn handle_expiretime_like(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
        milliseconds: bool,
    ) -> Result<(), RequestExecutionError> {
        let (command, expected) = if milliseconds {
            ("PEXPIRETIME", "PEXPIRETIME key")
        } else {
            ("EXPIRETIME", "EXPIRETIME key")
        };
        if args.len() != 2 {
            return Err(RequestExecutionError::WrongArity { command, expected });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        self.expire_key_if_needed(&key)?;

        if !self.key_exists_any(&key)? {
            append_integer(response_out, -2);
            return Ok(());
        }

        match self.string_expiration_deadline(&key) {
            None => {
                append_integer(response_out, -1);
                Ok(())
            }
            Some(deadline) => {
                let now = Instant::now();
                if deadline <= now {
                    self.expire_key_if_needed(&key)?;
                    append_integer(response_out, -2);
                    return Ok(());
                }

                let now_unix_millis = u128::from(
                    current_unix_time_millis().ok_or(RequestExecutionError::ValueNotInteger)?,
                );
                let expiration_unix_millis = now_unix_millis
                    .checked_add(deadline.duration_since(now).as_millis())
                    .ok_or(RequestExecutionError::ValueNotInteger)?;
                let value = if milliseconds {
                    expiration_unix_millis.min(i64::MAX as u128) as i64
                } else {
                    (expiration_unix_millis / 1000).min(i64::MAX as u128) as i64
                };
                append_integer(response_out, value);
                Ok(())
            }
        }
    }

    pub(super) fn handle_persist(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 2 {
            return Err(RequestExecutionError::WrongArity {
                command: "PERSIST",
                expected: "PERSIST key",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        self.expire_key_if_needed(&key)?;
        let string_exists = self.key_exists(&key)?;
        let object_exists = self.object_key_exists(&key)?;
        if !string_exists && !object_exists {
            append_integer(response_out, 0);
            return Ok(());
        }

        let shard_index = self.string_store_shard_index_for_key(&key);
        let removed_deadline = {
            let mut expirations = self.lock_string_expirations_for_shard(shard_index);
            let removed = expirations.remove(&key).is_some();
            if removed {
                self.decrement_string_expiration_count(shard_index);
            }
            removed
        };
        if !removed_deadline {
            append_integer(response_out, 0);
            return Ok(());
        }

        if string_exists && !self.rewrite_existing_value_expiration(&key, None)? {
            append_integer(response_out, 0);
            return Ok(());
        }

        self.bump_watch_version(&key);
        append_integer(response_out, 1);
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
enum GetExAction {
    KeepTtl,
    Persist,
    SetExpiration(ExpirationMetadata),
    DeleteNow,
}

fn parse_getex_action(args: &[ArgSlice]) -> Result<GetExAction, RequestExecutionError> {
    if args.len() < 2 {
        return Err(RequestExecutionError::WrongArity {
            command: "GETEX",
            expected: "GETEX key [EX seconds|PX milliseconds|EXAT unix-time-seconds|PXAT unix-time-milliseconds|PERSIST]",
        });
    }
    if args.len() == 2 {
        return Ok(GetExAction::KeepTtl);
    }
    if args.len() == 3 {
        // SAFETY: caller guarantees argument backing memory validity.
        let option = unsafe { args[2].as_slice() };
        if ascii_eq_ignore_case(option, b"PERSIST") {
            return Ok(GetExAction::Persist);
        }
        return Err(RequestExecutionError::SyntaxError);
    }
    if args.len() != 4 {
        return Err(RequestExecutionError::WrongArity {
            command: "GETEX",
            expected: "GETEX key [EX seconds|PX milliseconds|EXAT unix-time-seconds|PXAT unix-time-milliseconds|PERSIST]",
        });
    }

    // SAFETY: caller guarantees argument backing memory validity.
    let option = unsafe { args[2].as_slice() };
    // SAFETY: caller guarantees argument backing memory validity.
    let value = unsafe { args[3].as_slice() };

    if ascii_eq_ignore_case(option, b"EX") || ascii_eq_ignore_case(option, b"PX") {
        let amount = parse_u64_ascii(value).ok_or(RequestExecutionError::InvalidGetExExpireTime)?;
        if amount == 0 {
            return Err(RequestExecutionError::InvalidGetExExpireTime);
        }
        let duration = if ascii_eq_ignore_case(option, b"EX") {
            Duration::from_secs(amount)
        } else {
            Duration::from_millis(amount)
        };
        let expiration = expiration_metadata_from_duration(duration)
            .ok_or(RequestExecutionError::InvalidGetExExpireTime)?;
        return Ok(GetExAction::SetExpiration(expiration));
    }

    if ascii_eq_ignore_case(option, b"EXAT") || ascii_eq_ignore_case(option, b"PXAT") {
        let amount = parse_u64_ascii(value).ok_or(RequestExecutionError::InvalidGetExExpireTime)?;
        let unix_millis = if ascii_eq_ignore_case(option, b"EXAT") {
            amount
                .checked_mul(1000)
                .ok_or(RequestExecutionError::InvalidGetExExpireTime)?
        } else {
            amount
        };
        let now_unix_millis =
            current_unix_time_millis().ok_or(RequestExecutionError::InvalidGetExExpireTime)?;
        if unix_millis <= now_unix_millis {
            return Ok(GetExAction::DeleteNow);
        }
        let deadline = instant_from_unix_millis(unix_millis)
            .ok_or(RequestExecutionError::InvalidGetExExpireTime)?;
        return Ok(GetExAction::SetExpiration(ExpirationMetadata {
            deadline,
            unix_millis,
        }));
    }

    Err(RequestExecutionError::SyntaxError)
}

fn parse_bitop_operation(token: &[u8]) -> Option<BitopOperation> {
    if ascii_eq_ignore_case(token, b"AND") {
        return Some(BitopOperation::And);
    }
    if ascii_eq_ignore_case(token, b"OR") {
        return Some(BitopOperation::Or);
    }
    if ascii_eq_ignore_case(token, b"XOR") {
        return Some(BitopOperation::Xor);
    }
    if ascii_eq_ignore_case(token, b"NOT") {
        return Some(BitopOperation::Not);
    }
    None
}

fn apply_bitop(operation: BitopOperation, source_values: &[Vec<u8>]) -> Vec<u8> {
    if source_values.is_empty() {
        return Vec::new();
    }
    match operation {
        BitopOperation::Not => source_values[0].iter().map(|byte| !*byte).collect(),
        BitopOperation::And | BitopOperation::Or | BitopOperation::Xor => {
            let max_len = source_values.iter().map(Vec::len).max().unwrap_or(0);
            let mut result = vec![0u8; max_len];
            for index in 0..max_len {
                let mut value = match operation {
                    BitopOperation::And => 0xFFu8,
                    BitopOperation::Or | BitopOperation::Xor => 0u8,
                    BitopOperation::Not => unreachable!(),
                };
                for source in source_values {
                    let source_byte = source.get(index).copied().unwrap_or(0);
                    value = match operation {
                        BitopOperation::And => value & source_byte,
                        BitopOperation::Or => value | source_byte,
                        BitopOperation::Xor => value ^ source_byte,
                        BitopOperation::Not => unreachable!(),
                    };
                }
                result[index] = value;
            }
            result
        }
    }
}

fn normalize_string_range(len: usize, start: i64, end: i64) -> Option<(usize, usize)> {
    if len == 0 {
        return None;
    }

    let len_i = len as i128;
    let mut start_i = i128::from(start);
    let mut end_i = i128::from(end);

    if start_i < 0 {
        start_i += len_i;
    }
    if end_i < 0 {
        end_i += len_i;
    }

    if start_i < 0 {
        start_i = 0;
    }
    if end_i < 0 {
        return None;
    }

    if start_i >= len_i {
        return None;
    }
    if end_i >= len_i {
        end_i = len_i - 1;
    }
    if start_i > end_i {
        return None;
    }

    Some((start_i as usize, end_i as usize))
}

fn object_type_name(object_type: u8) -> Option<&'static [u8]> {
    match object_type {
        HASH_OBJECT_TYPE_TAG => Some(b"hash"),
        LIST_OBJECT_TYPE_TAG => Some(b"list"),
        SET_OBJECT_TYPE_TAG => Some(b"set"),
        ZSET_OBJECT_TYPE_TAG => Some(b"zset"),
        STREAM_OBJECT_TYPE_TAG => Some(b"stream"),
        _ => None,
    }
}
