use super::value_codec::HashPayloadBatchDeleteResult;
use super::value_codec::HashPayloadBatchGetDeleteResult;
use super::value_codec::HashPayloadDeleteResult;
use super::value_codec::delete_hash_fields_payload_batch;
use super::value_codec::delete_single_hash_field_payload;
use super::value_codec::deserialize_hash_object_payload_entries;
use super::value_codec::get_and_delete_hash_fields_payload_batch;
use super::value_codec::get_hash_fields_payload_batch;
use super::value_codec::upsert_hash_fields_payload_batch;
use super::value_codec::upsert_single_hash_field_payload;
use super::*;
use std::collections::BTreeSet;

impl RequestProcessor {
    pub(super) fn handle_hset(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_paired_arity_after(args, 4, 2, "HSET", "HSET key field value [field value ...]")?;

        let selected_db = current_request_selected_db();
        let key = DbKeyRef::new(selected_db, args[1]);
        if !self.has_hash_field_expirations_for_key(key) {
            if args.len() == 4 {
                self.handle_hset_single_field_fast_path(key, args[2], args[3], response_out)?;
            } else {
                self.handle_hset_batch_fast_path(key, &args[2..], response_out)?;
            }
            return Ok(());
        }
        let mut hash = self.load_hash_object(key)?.unwrap_or_default();
        let access_fields = args[2..]
            .chunks_exact(2)
            .map(|entry| entry[0])
            .collect::<Vec<_>>();
        self.apply_hash_field_lazy_expiration(key, &mut hash, &access_fields);
        let mut inserted = 0i64;

        let mut index = 2usize;
        while index < args.len() {
            let field = args[index].to_vec();
            let value = args[index + 1].to_vec();
            if hash.insert(field, value).is_none() {
                inserted += 1;
            }
            self.set_hash_field_expiration_unix_millis(key, args[index], None);
            index += 2;
        }

        self.save_hash_object(key, &hash)?;
        self.notify_keyspace_event(selected_db, NOTIFY_HASH, b"hset", key.key());
        append_integer(response_out, inserted);
        Ok(())
    }

    pub(super) fn handle_hget(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 3, "HGET", "HGET key field")?;

        let key = RedisKey::from(args[1]);
        let field = args[2];
        let resp3 = self.resp_protocol_version().is_resp3();
        let mut hash =
            match self.load_hash_object(DbKeyRef::new(current_request_selected_db(), &key))? {
                Some(hash) => hash,
                None => {
                    if resp3 {
                        append_null(response_out);
                    } else {
                        append_null_bulk_string(response_out);
                    }
                    return Ok(());
                }
            };
        let lazy_expired = self.apply_hash_field_lazy_expiration(
            DbKeyRef::new(current_request_selected_db(), &key),
            &mut hash,
            &[field],
        );
        if lazy_expired {
            self.persist_hash_after_field_expiration(
                DbKeyRef::new(current_request_selected_db(), &key),
                &hash,
            )?;
            if hash.is_empty() {
                if resp3 {
                    append_null(response_out);
                } else {
                    append_null_bulk_string(response_out);
                }
                return Ok(());
            }
        }

        match hash.get(field) {
            Some(value) => append_bulk_string(response_out, value),
            None => {
                if resp3 {
                    append_null(response_out);
                } else {
                    append_null_bulk_string(response_out);
                }
            }
        }
        Ok(())
    }

    pub(super) fn handle_hdel(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 3, "HDEL", "HDEL key field [field ...]")?;

        let selected_db = current_request_selected_db();
        let key = DbKeyRef::new(selected_db, args[1]);
        if !self.has_hash_field_expirations_for_key(key) {
            if args.len() == 3 {
                self.handle_hdel_single_field_fast_path(key, args[2], response_out)?;
            } else {
                self.handle_hdel_batch_fast_path(key, &args[2..], response_out)?;
            }
            return Ok(());
        }
        let mut hash = match self.load_hash_object(key)? {
            Some(hash) => hash,
            None => {
                append_integer(response_out, 0);
                return Ok(());
            }
        };
        self.apply_hash_field_lazy_expiration(key, &mut hash, &args[2..]);

        let mut removed = 0i64;
        for field in &args[2..] {
            if hash.remove(*field).is_some() {
                removed += 1;
                self.set_hash_field_expiration_unix_millis(key, field, None);
            }
        }

        if removed > 0 {
            self.notify_keyspace_event(selected_db, NOTIFY_HASH, b"hdel", key.key());
        }
        if hash.is_empty() {
            let _ = self.object_delete(key)?;
            if removed > 0 {
                self.notify_keyspace_event(selected_db, NOTIFY_GENERIC, b"del", key.key());
            }
        } else {
            self.save_hash_object(key, &hash)?;
        }
        append_integer(response_out, removed);
        Ok(())
    }

    pub(super) fn handle_hgetall(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 2, "HGETALL", "HGETALL key")?;

        let selected_db = current_request_selected_db();
        let key = DbKeyRef::new(selected_db, args[1]);
        let resp3 = self.resp_protocol_version().is_resp3();
        if !self.has_hash_field_expirations_for_key(key) {
            self.expire_key_if_needed(key)?;
            let object = match self.object_read(key)? {
                Some(object) => object,
                None => {
                    if self.key_exists(key)? {
                        return Err(RequestExecutionError::WrongType);
                    }
                    if resp3 {
                        append_map_length(response_out, 0);
                    } else {
                        append_array_length(response_out, 0);
                    }
                    return Ok(());
                }
            };
            if object.object_type != HASH_OBJECT_TYPE_TAG {
                return Err(RequestExecutionError::WrongType);
            }
            let entries =
                deserialize_hash_object_payload_entries(&object.payload).ok_or_else(|| {
                    storage_failure("handle_hgetall", "failed to deserialize hash payload")
                })?;
            if resp3 {
                append_map_length(response_out, entries.len());
            } else {
                append_array_length(response_out, entries.len().saturating_mul(2));
            }
            for (field, value) in &entries {
                append_bulk_string(response_out, field);
                append_bulk_string(response_out, value);
            }
            return Ok(());
        }
        let hash = match self.load_hash_object(key)? {
            Some(hash) => hash,
            None => {
                if resp3 {
                    append_map_length(response_out, 0);
                } else {
                    append_array_length(response_out, 0);
                }
                return Ok(());
            }
        };

        if resp3 {
            append_map_length(response_out, hash.len());
        } else {
            append_array_length(response_out, hash.len().saturating_mul(2));
        }
        for (field, value) in &hash {
            append_bulk_string(response_out, field);
            append_bulk_string(response_out, value);
        }
        Ok(())
    }

    pub(super) fn handle_hlen(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 2, "HLEN", "HLEN key")?;

        let key = DbKeyRef::new(current_request_selected_db(), args[1]);
        let len = self
            .load_hash_object(key)?
            .map(|hash| hash.len() as i64)
            .unwrap_or(0);
        append_integer(response_out, len);
        Ok(())
    }

    pub(super) fn handle_hmget(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 3, "HMGET", "HMGET key field [field ...]")?;

        let key = DbKeyRef::new(current_request_selected_db(), args[1]);
        let mut hash = self.load_hash_object(key)?;
        if let Some(hash_mut) = hash.as_mut() {
            let lazy_expired = self.apply_hash_field_lazy_expiration(key, hash_mut, &args[2..]);
            if lazy_expired {
                self.persist_hash_after_field_expiration(key, hash_mut)?;
            }
        }
        let resp3 = self.resp_protocol_version().is_resp3();
        let field_count = args.len() - 2;
        append_array_length(response_out, field_count);
        for field in &args[2..] {
            match hash.as_ref().and_then(|hash| hash.get(*field)) {
                Some(value) => append_bulk_string(response_out, value),
                None => {
                    if resp3 {
                        append_null(response_out);
                    } else {
                        append_null_bulk_string(response_out);
                    }
                }
            }
        }
        Ok(())
    }

    pub(super) fn handle_hmset(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_paired_arity_after(
            args,
            4,
            2,
            "HMSET",
            "HMSET key field value [field value ...]",
        )?;

        let selected_db = current_request_selected_db();
        let key = DbKeyRef::new(selected_db, args[1]);
        if !self.has_hash_field_expirations_for_key(key) {
            self.handle_hmset_batch_fast_path(key, &args[2..], response_out)?;
            return Ok(());
        }
        let mut hash = self.load_hash_object(key)?.unwrap_or_default();
        let access_fields = args[2..]
            .chunks_exact(2)
            .map(|entry| entry[0])
            .collect::<Vec<_>>();
        self.apply_hash_field_lazy_expiration(key, &mut hash, &access_fields);
        let mut index = 2usize;
        while index < args.len() {
            let field = args[index].to_vec();
            let value = args[index + 1].to_vec();
            hash.insert(field, value);
            self.set_hash_field_expiration_unix_millis(key, args[index], None);
            index += 2;
        }
        self.save_hash_object(key, &hash)?;
        self.notify_keyspace_event(selected_db, NOTIFY_HASH, b"hset", key.key());
        append_simple_string(response_out, b"OK");
        Ok(())
    }

    pub(super) fn handle_hsetnx(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 4, "HSETNX", "HSETNX key field value")?;

        let selected_db = current_request_selected_db();
        let key = DbKeyRef::new(selected_db, args[1]);
        let mut hash = self.load_hash_object(key)?.unwrap_or_default();
        let field = args[2].to_vec();
        self.apply_hash_field_lazy_expiration(key, &mut hash, &[args[2]]);
        if hash.contains_key(&field) {
            append_integer(response_out, 0);
            return Ok(());
        }
        let value = args[3].to_vec();
        hash.insert(field, value);
        self.set_hash_field_expiration_unix_millis(key, args[2], None);
        self.save_hash_object(key, &hash)?;
        self.notify_keyspace_event(selected_db, NOTIFY_HASH, b"hset", key.key());
        append_integer(response_out, 1);
        Ok(())
    }

    pub(super) fn handle_hexists(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 3, "HEXISTS", "HEXISTS key field")?;

        let key = DbKeyRef::new(current_request_selected_db(), args[1]);
        let field = args[2];
        let mut hash = match self.load_hash_object(key)? {
            Some(hash) => hash,
            None => {
                append_integer(response_out, 0);
                return Ok(());
            }
        };
        let lazy_expired = self.apply_hash_field_lazy_expiration(key, &mut hash, &[field]);
        if lazy_expired {
            self.persist_hash_after_field_expiration(key, &hash)?;
        }
        let exists = hash.contains_key(field);
        append_integer(response_out, i64::from(exists));
        Ok(())
    }

    pub(super) fn handle_hkeys(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 2, "HKEYS", "HKEYS key")?;

        let key = DbKeyRef::new(current_request_selected_db(), args[1]);
        let hash = match self.load_hash_object(key)? {
            Some(hash) => hash,
            None => {
                append_array_length(response_out, 0);
                return Ok(());
            }
        };
        append_array_length(response_out, hash.len());
        for field in hash.keys() {
            append_bulk_string(response_out, field);
        }
        Ok(())
    }

    pub(super) fn handle_hvals(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 2, "HVALS", "HVALS key")?;

        let key = DbKeyRef::new(current_request_selected_db(), args[1]);
        let hash = match self.load_hash_object(key)? {
            Some(hash) => hash,
            None => {
                append_array_length(response_out, 0);
                return Ok(());
            }
        };
        append_array_length(response_out, hash.len());
        for value in hash.values() {
            append_bulk_string(response_out, value);
        }
        Ok(())
    }

    pub(super) fn handle_hstrlen(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 3, "HSTRLEN", "HSTRLEN key field")?;

        let key = DbKeyRef::new(current_request_selected_db(), args[1]);
        let field = args[2];
        let mut hash = match self.load_hash_object(key)? {
            Some(hash) => hash,
            None => {
                append_integer(response_out, 0);
                return Ok(());
            }
        };
        let lazy_expired = self.apply_hash_field_lazy_expiration(key, &mut hash, &[field]);
        if lazy_expired {
            self.persist_hash_after_field_expiration(key, &hash)?;
        }
        let len = hash.get(field).map(|value| value.len() as i64).unwrap_or(0);
        append_integer(response_out, len);
        Ok(())
    }

    pub(super) fn handle_hscan(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            3,
            "HSCAN",
            "HSCAN key cursor [MATCH pattern] [COUNT count] [NOVALUES]",
        )?;

        let key = DbKeyRef::new(current_request_selected_db(), args[1]);
        let cursor = parse_u64_ascii(args[2]).ok_or(RequestExecutionError::ValueNotInteger)?;
        let scan_options = parse_scan_match_count_options(args, 3)?;
        let novalues = scan_options.novalues;
        let resp3 = self.resp_protocol_version().is_resp3();

        let Some(hash) = self.load_hash_object(key)? else {
            append_hash_scan_response(
                response_out,
                cursor,
                scan_options.count,
                &[],
                novalues,
                resp3,
            );
            return Ok(());
        };
        let mut hash = hash;
        let all_fields_owned = hash.keys().cloned().collect::<Vec<_>>();
        let all_fields = all_fields_owned
            .iter()
            .map(Vec::as_slice)
            .collect::<Vec<_>>();
        let lazy_expired = self.apply_hash_field_lazy_expiration(key, &mut hash, &all_fields);
        if lazy_expired {
            self.persist_hash_after_field_expiration(key, &hash)?;
            if hash.is_empty() {
                append_hash_scan_response(
                    response_out,
                    cursor,
                    scan_options.count,
                    &[],
                    novalues,
                    resp3,
                );
                return Ok(());
            }
        }

        let mut matched = Vec::new();
        for (field, value) in &hash {
            if let Some(pattern) = scan_options.pattern
                && !super::server_commands::redis_glob_match(
                    pattern,
                    field,
                    super::server_commands::CaseSensitivity::Sensitive,
                    0,
                )
            {
                continue;
            }
            matched.push((field.as_slice(), value.as_slice()));
        }

        let resp3 = self.resp_protocol_version().is_resp3();
        append_hash_scan_response(
            response_out,
            cursor,
            scan_options.count,
            &matched,
            novalues,
            resp3,
        );
        Ok(())
    }

    pub(super) fn handle_hincrby(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 4, "HINCRBY", "HINCRBY key field increment")?;

        let increment = parse_i64_ascii(args[3]).ok_or(RequestExecutionError::ValueNotInteger)?;
        let selected_db = current_request_selected_db();
        let key = DbKeyRef::new(selected_db, args[1]);
        let field = args[2].to_vec();
        let mut hash = self.load_hash_object(key)?.unwrap_or_default();
        self.apply_hash_field_lazy_expiration(key, &mut hash, &[args[2]]);

        let current = match hash.get(&field) {
            Some(value) => parse_i64_ascii(value).ok_or(RequestExecutionError::ValueNotInteger)?,
            None => 0,
        };
        let updated = current
            .checked_add(increment)
            .ok_or(RequestExecutionError::IncrementOverflow)?;
        hash.insert(field, updated.to_string().into_bytes());
        self.save_hash_object(key, &hash)?;
        self.notify_keyspace_event(selected_db, NOTIFY_HASH, b"hincrby", key.key());
        append_integer(response_out, updated);
        Ok(())
    }

    pub(super) fn handle_hincrbyfloat(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 4, "HINCRBYFLOAT", "HINCRBYFLOAT key field increment")?;

        let increment = parse_f64_ascii_allow_non_finite(args[3])
            .ok_or(RequestExecutionError::ValueNotFloat)?;
        if !increment.is_finite() {
            return Err(RequestExecutionError::ValueIsNanOrInfinity);
        }
        let selected_db = current_request_selected_db();
        let key = DbKeyRef::new(selected_db, args[1]);
        let field = args[2].to_vec();
        let mut hash = self.load_hash_object(key)?.unwrap_or_default();
        self.apply_hash_field_lazy_expiration(key, &mut hash, &[args[2]]);

        let current = match hash.get(&field) {
            Some(value) => parse_f64_ascii(value).ok_or(RequestExecutionError::ValueNotFloat)?,
            None => 0.0,
        };
        let updated = current + increment;
        if !updated.is_finite() {
            return Err(RequestExecutionError::ValueIsNanOrInfinity);
        }
        let updated_text = updated.to_string().into_bytes();
        hash.insert(field, updated_text.clone());
        self.save_hash_object(key, &hash)?;
        self.notify_keyspace_event(selected_db, NOTIFY_HASH, b"hincrbyfloat", key.key());
        append_bulk_string(response_out, &updated_text);
        Ok(())
    }

    pub(super) fn handle_hrandfield(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_ranged_arity(
            args,
            2,
            4,
            "HRANDFIELD",
            "HRANDFIELD key [count [WITHVALUES]]",
        )?;

        let key = DbKeyRef::new(current_request_selected_db(), args[1]);
        let mut hash = self.load_hash_object(key)?;
        if let Some(hash_mut) = hash.as_mut() {
            let all_fields_owned = hash_mut.keys().cloned().collect::<Vec<_>>();
            let all_fields = all_fields_owned
                .iter()
                .map(Vec::as_slice)
                .collect::<Vec<_>>();
            let lazy_expired = self.apply_hash_field_lazy_expiration(key, hash_mut, &all_fields);
            if lazy_expired {
                self.persist_hash_after_field_expiration(key, hash_mut)?;
            }
        }
        let resp3 = self.resp_protocol_version().is_resp3();

        if args.len() == 2 {
            let Some(hash) = hash else {
                if resp3 {
                    append_null(response_out);
                } else {
                    append_null_bulk_string(response_out);
                }
                return Ok(());
            };
            if hash.is_empty() {
                if resp3 {
                    append_null(response_out);
                } else {
                    append_null_bulk_string(response_out);
                }
                return Ok(());
            }
            let entries: Vec<(&Vec<u8>, &Vec<u8>)> = hash.iter().collect();
            let index = (self.next_random_u64() as usize) % entries.len();
            append_bulk_string(response_out, entries[index].0);
            return Ok(());
        }

        let count_bytes = args[2];
        let count = parse_i64_ascii(count_bytes).ok_or_else(|| {
            if looks_like_signed_integer(count_bytes) {
                RequestExecutionError::ValueOutOfRange
            } else {
                RequestExecutionError::ValueNotInteger
            }
        })?;
        let with_values = if args.len() == 4 {
            let option = args[3];
            if !ascii_eq_ignore_case(option, b"WITHVALUES") {
                return Err(RequestExecutionError::SyntaxError);
            }
            true
        } else {
            false
        };

        let abs_count = count
            .checked_abs()
            .ok_or(RequestExecutionError::ValueOutOfRange)?;
        let requested_count =
            usize::try_from(abs_count).map_err(|_| RequestExecutionError::ValueOutOfRange)?;
        const MAX_HRANDFIELD_COUNT: usize = 1_000_000;
        if requested_count > MAX_HRANDFIELD_COUNT {
            return Err(RequestExecutionError::ValueOutOfRange);
        }
        if requested_count == 0 {
            append_array_length(response_out, 0);
            return Ok(());
        }

        let Some(hash) = hash else {
            append_array_length(response_out, 0);
            return Ok(());
        };
        if hash.is_empty() {
            append_array_length(response_out, 0);
            return Ok(());
        }

        let entries: Vec<(&Vec<u8>, &Vec<u8>)> = hash.iter().collect();
        let sampled = if count > 0 {
            let unique_count = requested_count.min(entries.len());
            let mut shuffled = entries.clone();
            for index in 0..unique_count {
                let remaining = shuffled.len() - index;
                let random_offset = (self.next_random_u64() as usize) % remaining;
                shuffled.swap(index, index + random_offset);
            }
            shuffled.into_iter().take(unique_count).collect::<Vec<_>>()
        } else {
            let mut picks = Vec::with_capacity(requested_count);
            for _ in 0..requested_count {
                let index = (self.next_random_u64() as usize) % entries.len();
                picks.push(entries[index]);
            }
            picks
        };

        if with_values {
            if resp3 {
                append_array_length(response_out, sampled.len());
                for (field, value) in sampled {
                    append_array_length(response_out, 2);
                    append_bulk_string(response_out, field);
                    append_bulk_string(response_out, value);
                }
            } else {
                append_array_length(response_out, sampled.len() * 2);
                for (field, value) in sampled {
                    append_bulk_string(response_out, field);
                    append_bulk_string(response_out, value);
                }
            }
            return Ok(());
        }

        // HRANDFIELD without WITHVALUES: flat array of field names in both
        // RESP2 and RESP3 (Valkey emits the same shape for both protocols).
        append_array_length(response_out, sampled.len());
        for (field, _) in sampled {
            append_bulk_string(response_out, field);
        }
        Ok(())
    }

    pub(super) fn handle_hsetex(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            6,
            "HSETEX",
            "HSETEX key [FNX|FXX] [EX seconds|PX milliseconds|EXAT unix-time-seconds|PXAT milliseconds-unix-time|KEEPTTL] FIELDS num field value [field value ...]",
        )?;

        let key = RedisKey::from(args[1]);
        let expire_options = parse_hsetex_options(args)?;
        let field_values = collect_hash_field_values(
            args,
            expire_options.first_field_index,
            expire_options.field_count,
        )?;

        let access_fields = field_values
            .iter()
            .map(|(field, _)| *field)
            .collect::<Vec<_>>();
        let mut hash = self
            .load_hash_object(DbKeyRef::new(current_request_selected_db(), &key))?
            .unwrap_or_default();
        let lazy_expired = self.apply_hash_field_lazy_expiration(
            DbKeyRef::new(current_request_selected_db(), &key),
            &mut hash,
            &access_fields,
        );

        let condition_matches = match expire_options.field_set_condition {
            HashFieldSetCondition::Any => true,
            HashFieldSetCondition::Fxx => field_values
                .iter()
                .all(|(field, _)| hash.contains_key(*field)),
            HashFieldSetCondition::Fnx => field_values
                .iter()
                .all(|(field, _)| !hash.contains_key(*field)),
        };
        if !condition_matches {
            if lazy_expired {
                self.persist_hash_after_field_expiration(
                    DbKeyRef::new(current_request_selected_db(), &key),
                    &hash,
                )?;
            }
            append_integer(response_out, 0);
            return Ok(());
        }

        let mut applied_fields = BTreeSet::new();
        for (field, value) in &field_values {
            hash.insert((*field).to_vec(), (*value).to_vec());
            applied_fields.insert((*field).to_vec());
            if let Some(expiration_unix_millis) = expire_options.expiration_unix_millis {
                self.set_hash_field_expiration_unix_millis(
                    DbKeyRef::new(current_request_selected_db(), &key),
                    field,
                    Some(expiration_unix_millis),
                );
            } else if !expire_options.keep_ttl {
                self.set_hash_field_expiration_unix_millis(
                    DbKeyRef::new(current_request_selected_db(), &key),
                    field,
                    None,
                );
            }
        }
        self.notify_keyspace_event(current_request_selected_db(), NOTIFY_HASH, b"hset", &key);

        let immediate_expire = expire_options.expire_if_past_immediately
            && expire_options
                .expiration_unix_millis
                .is_some_and(|expiration| {
                    current_unix_time_millis().is_some_and(|now| expiration <= now)
                });

        if immediate_expire && !applied_fields.is_empty() {
            let mut removed_any = false;
            for field in &applied_fields {
                if hash.remove(field.as_slice()).is_some() {
                    removed_any = true;
                    self.set_hash_field_expiration_unix_millis(
                        DbKeyRef::new(current_request_selected_db(), &key),
                        field,
                        None,
                    );
                }
            }
            if removed_any {
                self.notify_keyspace_event(
                    current_request_selected_db(),
                    NOTIFY_HASH,
                    b"hdel",
                    &key,
                );
            }
        } else if expire_options.expiration_unix_millis.is_some() && !applied_fields.is_empty() {
            self.notify_keyspace_event(
                current_request_selected_db(),
                NOTIFY_HASH,
                b"hexpire",
                &key,
            );
        }

        if hash.is_empty() {
            let _ = self.object_delete(DbKeyRef::new(current_request_selected_db(), &key))?;
            if !applied_fields.is_empty() {
                self.notify_keyspace_event(
                    current_request_selected_db(),
                    NOTIFY_GENERIC,
                    b"del",
                    &key,
                );
            }
        } else {
            self.save_hash_object(DbKeyRef::new(current_request_selected_db(), &key), &hash)?;
        }
        append_integer(response_out, 1);
        Ok(())
    }

    pub(super) fn handle_hgetex(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            5,
            "HGETEX",
            "HGETEX key [EX seconds|PX milliseconds|EXAT unix-time-seconds|PXAT milliseconds-unix-time|PERSIST] FIELDS num field [field ...]",
        )?;
        let key = RedisKey::from(args[1]);
        let expire_options = parse_hgetex_options(args)?;
        let fields = collect_hash_fields(
            args,
            expire_options.first_field_index,
            expire_options.field_count,
        )?;
        let resp3 = self.resp_protocol_version().is_resp3();
        let mut hash =
            match self.load_hash_object(DbKeyRef::new(current_request_selected_db(), &key))? {
                Some(hash) => hash,
                None => {
                    append_array_length(response_out, fields.len());
                    for _ in 0..fields.len() {
                        if resp3 {
                            append_null(response_out);
                        } else {
                            append_null_bulk_string(response_out);
                        }
                    }
                    return Ok(());
                }
            };

        let lazy_expired = self.apply_hash_field_lazy_expiration(
            DbKeyRef::new(current_request_selected_db(), &key),
            &mut hash,
            &fields,
        );
        append_array_length(response_out, fields.len());
        for field in &fields {
            match hash.get(*field) {
                Some(value) => append_bulk_string(response_out, value),
                None => {
                    if resp3 {
                        append_null(response_out);
                    } else {
                        append_null_bulk_string(response_out);
                    }
                }
            }
        }

        let mut updated_expiration_count = 0usize;
        let mut persisted_expiration_count = 0usize;
        for field in &fields {
            if hash.contains_key(*field) {
                if expire_options.persist {
                    if self
                        .hash_field_expiration_unix_millis(
                            DbKeyRef::new(current_request_selected_db(), &key),
                            field,
                        )
                        .is_some()
                    {
                        persisted_expiration_count += 1;
                    }
                    self.set_hash_field_expiration_unix_millis(
                        DbKeyRef::new(current_request_selected_db(), &key),
                        field,
                        None,
                    );
                } else {
                    self.set_hash_field_expiration_unix_millis(
                        DbKeyRef::new(current_request_selected_db(), &key),
                        field,
                        expire_options.expiration_unix_millis,
                    );
                    if expire_options.expiration_unix_millis.is_some() {
                        updated_expiration_count += 1;
                    }
                }
            } else {
                self.set_hash_field_expiration_unix_millis(
                    DbKeyRef::new(current_request_selected_db(), &key),
                    field,
                    None,
                );
            }
        }

        let mut removed_any = false;
        if expire_options.expire_if_past_immediately
            && let Some(expiration) = expire_options.expiration_unix_millis
            && expiration
                <= current_unix_time_millis().ok_or(RequestExecutionError::ValueOutOfRange)?
        {
            for field in &fields {
                if hash.remove(*field).is_some() {
                    removed_any = true;
                    self.set_hash_field_expiration_unix_millis(
                        DbKeyRef::new(current_request_selected_db(), &key),
                        field,
                        None,
                    );
                }
            }
            if removed_any {
                self.notify_keyspace_event(
                    current_request_selected_db(),
                    NOTIFY_HASH,
                    b"hdel",
                    &key,
                );
            }
        } else if updated_expiration_count > 0 {
            self.notify_keyspace_event(
                current_request_selected_db(),
                NOTIFY_HASH,
                b"hexpire",
                &key,
            );
        } else if persisted_expiration_count > 0 {
            self.notify_keyspace_event(
                current_request_selected_db(),
                NOTIFY_HASH,
                b"hpersist",
                &key,
            );
        }

        if lazy_expired || removed_any {
            self.persist_hash_after_field_expiration(
                DbKeyRef::new(current_request_selected_db(), &key),
                &hash,
            )?;
        }
        Ok(())
    }

    pub(super) fn handle_hgetdel(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            5,
            "HGETDEL",
            "HGETDEL key FIELDS num field [field ...]",
        )?;
        let key = RedisKey::from(args[1]);
        let fields = parse_hgetdel_fields(args)?;
        let resp3 = self.resp_protocol_version().is_resp3();
        let mut hash =
            match self.load_hash_object(DbKeyRef::new(current_request_selected_db(), &key))? {
                Some(hash) => hash,
                None => {
                    append_array_length(response_out, fields.len());
                    for _ in 0..fields.len() {
                        if resp3 {
                            append_null(response_out);
                        } else {
                            append_null_bulk_string(response_out);
                        }
                    }
                    return Ok(());
                }
            };

        self.apply_hash_field_lazy_expiration(
            DbKeyRef::new(current_request_selected_db(), &key),
            &mut hash,
            &fields,
        );
        let mut removed = 0usize;
        append_array_length(response_out, fields.len());
        for field in &fields {
            match hash.remove(*field) {
                Some(value) => {
                    self.set_hash_field_expiration_unix_millis(
                        DbKeyRef::new(current_request_selected_db(), &key),
                        field,
                        None,
                    );
                    removed += 1;
                    append_bulk_string(response_out, &value);
                }
                None => {
                    if resp3 {
                        append_null(response_out);
                    } else {
                        append_null_bulk_string(response_out);
                    }
                }
            }
        }
        if removed > 0 {
            self.notify_keyspace_event(current_request_selected_db(), NOTIFY_HASH, b"hdel", &key);
        }

        if hash.is_empty() {
            let _ = self.object_delete(DbKeyRef::new(current_request_selected_db(), &key))?;
            if removed > 0 {
                self.notify_keyspace_event(
                    current_request_selected_db(),
                    NOTIFY_GENERIC,
                    b"del",
                    &key,
                );
            }
        } else {
            self.save_hash_object(DbKeyRef::new(current_request_selected_db(), &key), &hash)?;
        }
        Ok(())
    }

    pub(super) fn handle_hexpire(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            5,
            "HEXPIRE",
            "HEXPIRE key seconds FIELDS num field [field ...]",
        )?;
        let seconds = parse_i64_ascii(args[2]).ok_or(RequestExecutionError::ValueNotInteger)?;
        if seconds < 0 {
            return Err(RequestExecutionError::ValueOutOfRange);
        }
        let now = current_unix_time_millis().ok_or(RequestExecutionError::ValueOutOfRange)?;
        let delta = u64::try_from(seconds)
            .ok()
            .and_then(|value| value.checked_mul(1000))
            .ok_or(RequestExecutionError::ValueOutOfRange)?;
        let expiration_unix_millis = now
            .checked_add(delta)
            .ok_or(RequestExecutionError::ValueOutOfRange)?;
        let parsed = parse_hash_field_expire_arguments(
            args,
            3,
            "HEXPIRE",
            "HEXPIRE key seconds [NX|XX|GT|LT] FIELDS num field [field ...]",
        )?;
        self.handle_hash_field_expire_common(
            DbKeyRef::new(current_request_selected_db(), args[1]),
            parsed.fields,
            expiration_unix_millis,
            true,
            parsed.condition,
            response_out,
        )
    }

    pub(super) fn handle_hpexpire(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            5,
            "HPEXPIRE",
            "HPEXPIRE key milliseconds FIELDS num field [field ...]",
        )?;
        let millis = parse_i64_ascii(args[2]).ok_or(RequestExecutionError::ValueNotInteger)?;
        if millis < 0 {
            return Err(RequestExecutionError::ValueOutOfRange);
        }
        let now = current_unix_time_millis().ok_or(RequestExecutionError::ValueOutOfRange)?;
        let delta = u64::try_from(millis).map_err(|_| RequestExecutionError::ValueOutOfRange)?;
        let expiration_unix_millis = now
            .checked_add(delta)
            .ok_or(RequestExecutionError::ValueOutOfRange)?;
        let parsed = parse_hash_field_expire_arguments(
            args,
            3,
            "HPEXPIRE",
            "HPEXPIRE key milliseconds [NX|XX|GT|LT] FIELDS num field [field ...]",
        )?;
        self.handle_hash_field_expire_common(
            DbKeyRef::new(current_request_selected_db(), args[1]),
            parsed.fields,
            expiration_unix_millis,
            true,
            parsed.condition,
            response_out,
        )
    }

    pub(super) fn handle_hpexpireat(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            5,
            "HPEXPIREAT",
            "HPEXPIREAT key milliseconds-unix-time FIELDS num field [field ...]",
        )?;
        let expiration_unix_millis =
            parse_u64_ascii(args[2]).ok_or(RequestExecutionError::ValueNotInteger)?;
        let parsed = parse_hash_field_expire_arguments(
            args,
            3,
            "HPEXPIREAT",
            "HPEXPIREAT key milliseconds-unix-time [NX|XX|GT|LT] FIELDS num field [field ...]",
        )?;
        self.handle_hash_field_expire_common(
            DbKeyRef::new(current_request_selected_db(), args[1]),
            parsed.fields,
            expiration_unix_millis,
            true,
            parsed.condition,
            response_out,
        )
    }

    pub(super) fn handle_hexpireat(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            5,
            "HEXPIREAT",
            "HEXPIREAT key unix-time-seconds FIELDS num field [field ...]",
        )?;
        let seconds = parse_i64_ascii(args[2]).ok_or(RequestExecutionError::ValueNotInteger)?;
        if seconds < 0 {
            return Err(RequestExecutionError::ValueOutOfRange);
        }
        let seconds = u64::try_from(seconds).map_err(|_| RequestExecutionError::ValueOutOfRange)?;
        let expiration_unix_millis = seconds
            .checked_mul(1000)
            .ok_or(RequestExecutionError::ValueOutOfRange)?;
        let parsed = parse_hash_field_expire_arguments(
            args,
            3,
            "HEXPIREAT",
            "HEXPIREAT key unix-time-seconds [NX|XX|GT|LT] FIELDS num field [field ...]",
        )?;
        self.handle_hash_field_expire_common(
            DbKeyRef::new(current_request_selected_db(), args[1]),
            parsed.fields,
            expiration_unix_millis,
            true,
            parsed.condition,
            response_out,
        )
    }

    pub(super) fn handle_hpersist(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            5,
            "HPERSIST",
            "HPERSIST key FIELDS num field [field ...]",
        )?;
        let selected_db = current_request_selected_db();
        let key = DbKeyRef::new(selected_db, args[1]);
        let fields = parse_hash_fields(
            args,
            2,
            "HPERSIST",
            "HPERSIST key FIELDS num field [field ...]",
        )?;
        let mut hash = match self.load_hash_object(key)? {
            Some(hash) => hash,
            None => {
                append_hash_integer_array(response_out, fields.len(), -2);
                return Ok(());
            }
        };
        let lazy_expired = self.apply_hash_field_lazy_expiration(key, &mut hash, &fields);

        let mut statuses = Vec::with_capacity(fields.len());
        let mut persisted_count = 0usize;
        for field in &fields {
            if !hash.contains_key(*field) {
                self.set_hash_field_expiration_unix_millis(key, field, None);
                statuses.push(-2);
                continue;
            }
            if self.hash_field_expiration_unix_millis(key, field).is_some() {
                self.set_hash_field_expiration_unix_millis(key, field, None);
                statuses.push(1);
                persisted_count += 1;
            } else {
                statuses.push(-1);
            }
        }

        if persisted_count > 0 {
            self.notify_keyspace_event(selected_db, NOTIFY_HASH, b"hpersist", key.key());
        }
        if lazy_expired {
            self.persist_hash_after_field_expiration(key, &hash)?;
        }
        append_integer_array(response_out, &statuses);
        Ok(())
    }

    pub(super) fn handle_hpttl(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 5, "HPTTL", "HPTTL key FIELDS num field [field ...]")?;
        self.handle_hash_field_ttl_query(
            args,
            true,
            "HPTTL",
            "HPTTL key FIELDS num field [field ...]",
            response_out,
        )
    }

    pub(super) fn handle_httl(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 5, "HTTL", "HTTL key FIELDS num field [field ...]")?;
        self.handle_hash_field_ttl_query(
            args,
            false,
            "HTTL",
            "HTTL key FIELDS num field [field ...]",
            response_out,
        )
    }

    pub(super) fn handle_hpexpiretime(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            5,
            "HPEXPIRETIME",
            "HPEXPIRETIME key FIELDS num field [field ...]",
        )?;
        self.handle_hash_field_expiretime_query(
            args,
            true,
            "HPEXPIRETIME",
            "HPEXPIRETIME key FIELDS num field [field ...]",
            response_out,
        )
    }

    pub(super) fn handle_hexpiretime(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            5,
            "HEXPIRETIME",
            "HEXPIRETIME key FIELDS num field [field ...]",
        )?;
        self.handle_hash_field_expiretime_query(
            args,
            false,
            "HEXPIRETIME",
            "HEXPIRETIME key FIELDS num field [field ...]",
            response_out,
        )
    }

    fn handle_hash_field_ttl_query(
        &self,
        args: &[&[u8]],
        as_millis: bool,
        command_name: &'static str,
        command_usage: &'static str,
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        let key = DbKeyRef::new(current_request_selected_db(), args[1]);
        let fields = parse_hash_fields(args, 2, command_name, command_usage)?;
        let mut hash = match self.load_hash_object(key)? {
            Some(hash) => hash,
            None => {
                append_hash_integer_array(response_out, fields.len(), -2);
                return Ok(());
            }
        };
        let lazy_expired = self.apply_hash_field_lazy_expiration(key, &mut hash, &fields);
        let now_unix_millis =
            current_unix_time_millis().ok_or(RequestExecutionError::ValueOutOfRange)?;

        let mut values = Vec::with_capacity(fields.len());
        for field in &fields {
            if !hash.contains_key(*field) {
                values.push(-2);
                continue;
            }
            let Some(expiration_unix_millis) = self.hash_field_expiration_unix_millis(key, field)
            else {
                values.push(-1);
                continue;
            };
            let remaining_millis = expiration_unix_millis.saturating_sub(now_unix_millis);
            if as_millis {
                values.push(i64::try_from(remaining_millis).unwrap_or(i64::MAX));
            } else {
                values.push(i64::try_from(remaining_millis / 1000).unwrap_or(i64::MAX));
            }
        }

        if lazy_expired {
            self.persist_hash_after_field_expiration(key, &hash)?;
        }
        append_integer_array(response_out, &values);
        Ok(())
    }

    fn handle_hash_field_expiretime_query(
        &self,
        args: &[&[u8]],
        as_millis: bool,
        command_name: &'static str,
        command_usage: &'static str,
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        let key = DbKeyRef::new(current_request_selected_db(), args[1]);
        let fields = parse_hash_fields(args, 2, command_name, command_usage)?;
        let mut hash = match self.load_hash_object(key)? {
            Some(hash) => hash,
            None => {
                append_hash_integer_array(response_out, fields.len(), -2);
                return Ok(());
            }
        };
        let lazy_expired = self.apply_hash_field_lazy_expiration(key, &mut hash, &fields);

        let mut values = Vec::with_capacity(fields.len());
        for field in &fields {
            if !hash.contains_key(*field) {
                values.push(-2);
                continue;
            }
            let Some(expiration_unix_millis) = self.hash_field_expiration_unix_millis(key, field)
            else {
                values.push(-1);
                continue;
            };
            let value = if as_millis {
                expiration_unix_millis
            } else {
                expiration_unix_millis / 1000
            };
            values.push(i64::try_from(value).unwrap_or(i64::MAX));
        }

        if lazy_expired {
            self.persist_hash_after_field_expiration(key, &hash)?;
        }
        append_integer_array(response_out, &values);
        Ok(())
    }

    pub(super) fn hash_get_field_for_sort_lookup(
        &self,
        key: DbKeyRef<'_>,
        field: &[u8],
    ) -> Result<Option<Vec<u8>>, RequestExecutionError> {
        let mut hash = match self.load_hash_object(key)? {
            Some(hash) => hash,
            None => return Ok(None),
        };
        let lazy_expired = self.apply_hash_field_lazy_expiration(key, &mut hash, &[field]);
        if lazy_expired {
            self.persist_hash_after_field_expiration(key, &hash)?;
            if hash.is_empty() {
                return Ok(None);
            }
        }
        Ok(hash.get(field).cloned())
    }

    pub(super) fn active_expire_hash_fields_for_key(
        &self,
        key: DbKeyRef<'_>,
    ) -> Result<(), RequestExecutionError> {
        if !self.active_expire_enabled() {
            return Ok(());
        }
        let expired_fields = self.remove_all_expired_hash_fields_for_key(key);
        if expired_fields.is_empty() {
            return Ok(());
        }

        let mut hash = match self.load_hash_object(key)? {
            Some(hash) => hash,
            None => return Ok(()),
        };
        for field in &expired_fields {
            hash.remove(field.as_ref());
        }
        self.notify_keyspace_event(key.db(), NOTIFY_HASH, b"hexpired", key.key());
        self.persist_hash_after_field_expiration(key, &hash)
    }

    pub(crate) fn active_expire_hash_fields_in_shard(
        &self,
        shard_index: ShardIndex,
        max_keys: usize,
    ) -> Result<(), RequestExecutionError> {
        if max_keys == 0 || !self.active_expire_enabled() {
            return Ok(());
        }
        let keys = {
            self.lock_hash_field_expirations_for_shard(shard_index)
                .keys()
                .take(max_keys)
                .cloned()
                .collect::<Vec<_>>()
        };
        let main_runtime_db = self.logical_db_for_main_runtime()?;
        for key in keys {
            self.active_expire_hash_fields_for_key(DbKeyRef::new(main_runtime_db, key.as_slice()))?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn handle_hash_field_expire_common(
        &self,
        key: DbKeyRef<'_>,
        fields: Vec<&[u8]>,
        expiration_unix_millis: u64,
        expire_if_past_immediately: bool,
        condition: HashFieldExpireCondition,
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        let mut hash = match self.load_hash_object(key)? {
            Some(hash) => hash,
            None => {
                append_hash_integer_array(response_out, fields.len(), -2);
                return Ok(());
            }
        };
        self.apply_hash_field_lazy_expiration(key, &mut hash, &fields);

        let mut statuses = Vec::with_capacity(fields.len());
        let mut applied_count = 0usize;
        let immediate_expire = expire_if_past_immediately
            && expiration_unix_millis
                <= current_unix_time_millis().ok_or(RequestExecutionError::ValueOutOfRange)?;
        for field in &fields {
            if hash.contains_key(*field) {
                let current_expiration = self.hash_field_expiration_unix_millis(key, field);
                let apply =
                    match condition {
                        HashFieldExpireCondition::None => true,
                        HashFieldExpireCondition::Nx => current_expiration.is_none(),
                        HashFieldExpireCondition::Xx => current_expiration.is_some(),
                        HashFieldExpireCondition::Gt => current_expiration
                            .is_some_and(|current| expiration_unix_millis > current),
                        HashFieldExpireCondition::Lt => current_expiration
                            .is_some_and(|current| expiration_unix_millis < current),
                    };
                if !apply {
                    statuses.push(0);
                    continue;
                }
                self.set_hash_field_expiration_unix_millis(
                    key,
                    field,
                    Some(expiration_unix_millis),
                );
                applied_count += 1;
                statuses.push(1);
            } else {
                self.set_hash_field_expiration_unix_millis(key, field, None);
                statuses.push(-2);
            }
        }

        let mut immediate_removed = 0usize;
        if immediate_expire {
            for field in &fields {
                if hash.remove(*field).is_some() {
                    self.set_hash_field_expiration_unix_millis(key, field, None);
                    immediate_removed += 1;
                }
            }
            if immediate_removed > 0 {
                self.notify_keyspace_event(key.db(), NOTIFY_HASH, b"hdel", key.key());
            }
        } else if applied_count > 0 {
            self.notify_keyspace_event(key.db(), NOTIFY_HASH, b"hexpire", key.key());
        }

        self.persist_hash_after_field_expiration(key, &hash)?;
        append_integer_array(response_out, &statuses);
        Ok(())
    }

    fn handle_hset_batch_fast_path(
        &self,
        key: DbKeyRef<'_>,
        field_values: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        let current_payload = match self.object_read(key)? {
            Some(object) => {
                if object.object_type != HASH_OBJECT_TYPE_TAG {
                    return Err(RequestExecutionError::WrongType);
                }
                object.payload
            }
            None => {
                if self.key_exists(key)? {
                    return Err(RequestExecutionError::WrongType);
                }
                0u32.to_le_bytes().to_vec()
            }
        };

        let field_updates = field_values
            .chunks_exact(2)
            .map(|entry| (entry[0], entry[1]))
            .collect::<Vec<_>>();
        let result = upsert_hash_fields_payload_batch(&current_payload, &field_updates)
            .ok_or_else(|| {
                storage_failure(
                    "handle_hset_batch_fast_path",
                    "failed to update serialized hash payload",
                )
            })?;

        self.object_upsert(key, HASH_OBJECT_TYPE_TAG, &result.payload)?;
        for field in field_values.chunks_exact(2).map(|entry| entry[0]) {
            self.set_hash_field_expiration_unix_millis(key, field, None);
        }
        self.notify_keyspace_event(key.db(), NOTIFY_HASH, b"hset", key.key());
        append_integer(response_out, result.inserted);
        Ok(())
    }

    fn handle_hmset_batch_fast_path(
        &self,
        key: DbKeyRef<'_>,
        field_values: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        let current_payload = match self.object_read(key)? {
            Some(object) => {
                if object.object_type != HASH_OBJECT_TYPE_TAG {
                    return Err(RequestExecutionError::WrongType);
                }
                object.payload
            }
            None => {
                if self.key_exists(key)? {
                    return Err(RequestExecutionError::WrongType);
                }
                0u32.to_le_bytes().to_vec()
            }
        };

        let field_updates = field_values
            .chunks_exact(2)
            .map(|entry| (entry[0], entry[1]))
            .collect::<Vec<_>>();
        let result = upsert_hash_fields_payload_batch(&current_payload, &field_updates)
            .ok_or_else(|| {
                storage_failure(
                    "handle_hmset_batch_fast_path",
                    "failed to update serialized hash payload",
                )
            })?;

        self.object_upsert(key, HASH_OBJECT_TYPE_TAG, &result.payload)?;
        for field in field_values.chunks_exact(2).map(|entry| entry[0]) {
            self.set_hash_field_expiration_unix_millis(key, field, None);
        }
        self.notify_keyspace_event(key.db(), NOTIFY_HASH, b"hset", key.key());
        append_simple_string(response_out, b"OK");
        Ok(())
    }

    fn handle_hset_single_field_fast_path(
        &self,
        key: DbKeyRef<'_>,
        field: &[u8],
        value: &[u8],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        let current_payload = match self.object_read(key)? {
            Some(object) => {
                if object.object_type != HASH_OBJECT_TYPE_TAG {
                    return Err(RequestExecutionError::WrongType);
                }
                object.payload
            }
            None => {
                if self.key_exists(key)? {
                    return Err(RequestExecutionError::WrongType);
                }
                0u32.to_le_bytes().to_vec()
            }
        };

        let result =
            upsert_single_hash_field_payload(&current_payload, field, value).ok_or_else(|| {
                storage_failure(
                    "handle_hset_single_field_fast_path",
                    "failed to update serialized hash payload",
                )
            })?;

        self.object_upsert(key, HASH_OBJECT_TYPE_TAG, &result.payload)?;
        self.set_hash_field_expiration_unix_millis(key, field, None);
        self.notify_keyspace_event(key.db(), NOTIFY_HASH, b"hset", key.key());
        append_integer(response_out, if result.inserted { 1 } else { 0 });
        Ok(())
    }

    fn handle_hsetex_batch_fast_path(
        &self,
        key: DbKeyRef<'_>,
        field_values: &[(&[u8], &[u8])],
        expire_options: &ParsedHashExpireOptions,
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        let current_payload = match self.object_read(key)? {
            Some(object) => {
                if object.object_type != HASH_OBJECT_TYPE_TAG {
                    return Err(RequestExecutionError::WrongType);
                }
                object.payload
            }
            None => {
                if self.key_exists(key)? {
                    return Err(RequestExecutionError::WrongType);
                }
                0u32.to_le_bytes().to_vec()
            }
        };

        let result =
            upsert_hash_fields_payload_batch(&current_payload, field_values).ok_or_else(|| {
                storage_failure(
                    "handle_hsetex_batch_fast_path",
                    "failed to update serialized hash payload",
                )
            })?;
        for (field, _) in field_values {
            self.set_hash_field_expiration_unix_millis(
                key,
                field,
                expire_options.expiration_unix_millis,
            );
        }

        let mut payload = result.payload;
        let mut delete_key = false;
        if expire_options.expire_if_past_immediately
            && let Some(expiration) = expire_options.expiration_unix_millis
            && expiration
                <= current_unix_time_millis().ok_or(RequestExecutionError::ValueOutOfRange)?
        {
            let fields = field_values
                .iter()
                .map(|(field, _)| *field)
                .collect::<Vec<_>>();
            let delete_result =
                delete_hash_fields_payload_batch(&payload, &fields).ok_or_else(|| {
                    storage_failure(
                        "handle_hsetex_batch_fast_path",
                        "failed to remove immediately expired fields from serialized hash payload",
                    )
                })?;
            if let HashPayloadBatchDeleteResult::Removed {
                payload: next_payload,
                is_empty,
                ..
            } = delete_result
            {
                payload = next_payload;
                delete_key = is_empty;
                for field in &fields {
                    self.set_hash_field_expiration_unix_millis(key, field, None);
                }
            }
        }

        if delete_key {
            let _ = self.object_delete(key)?;
        } else {
            self.object_upsert(key, HASH_OBJECT_TYPE_TAG, &payload)?;
        }
        append_integer(response_out, result.inserted);
        Ok(())
    }

    fn handle_hgetex_batch_fast_path(
        &self,
        key: DbKeyRef<'_>,
        fields: &[&[u8]],
        expire_options: &ParsedHashExpireOptions,
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        let resp3 = self.resp_protocol_version().is_resp3();
        let current_payload = match self.object_read(key)? {
            Some(object) => {
                if object.object_type != HASH_OBJECT_TYPE_TAG {
                    return Err(RequestExecutionError::WrongType);
                }
                object.payload
            }
            None => {
                if self.key_exists(key)? {
                    return Err(RequestExecutionError::WrongType);
                }
                append_array_length(response_out, fields.len());
                for _ in 0..fields.len() {
                    if resp3 {
                        append_null(response_out);
                    } else {
                        append_null_bulk_string(response_out);
                    }
                }
                return Ok(());
            }
        };

        let get_result =
            get_hash_fields_payload_batch(&current_payload, fields).ok_or_else(|| {
                storage_failure(
                    "handle_hgetex_batch_fast_path",
                    "failed to read fields from serialized hash payload",
                )
            })?;
        append_array_length(response_out, fields.len());
        for value in &get_result.values {
            match value {
                Some(value) => append_bulk_string(response_out, value),
                None => {
                    if resp3 {
                        append_null(response_out);
                    } else {
                        append_null_bulk_string(response_out);
                    }
                }
            }
        }

        for (index, field) in fields.iter().enumerate() {
            if get_result.values[index].is_some() {
                self.set_hash_field_expiration_unix_millis(
                    key,
                    field,
                    expire_options.expiration_unix_millis,
                );
            } else {
                self.set_hash_field_expiration_unix_millis(key, field, None);
            }
        }

        let mut payload = current_payload;
        let mut delete_key = false;
        if expire_options.expire_if_past_immediately
            && let Some(expiration) = expire_options.expiration_unix_millis
            && expiration
                <= current_unix_time_millis().ok_or(RequestExecutionError::ValueOutOfRange)?
        {
            let delete_result =
                delete_hash_fields_payload_batch(&payload, fields).ok_or_else(|| {
                    storage_failure(
                        "handle_hgetex_batch_fast_path",
                        "failed to remove immediately expired fields from serialized hash payload",
                    )
                })?;
            if let HashPayloadBatchDeleteResult::Removed {
                payload: next_payload,
                is_empty,
                ..
            } = delete_result
            {
                payload = next_payload;
                delete_key = is_empty;
                for (index, field) in fields.iter().enumerate() {
                    if get_result.values[index].is_some() {
                        self.set_hash_field_expiration_unix_millis(key, field, None);
                    }
                }
            }
        }

        if expire_options.expiration_unix_millis.is_some() {
            if delete_key {
                let _ = self.object_delete(key)?;
            } else {
                self.object_upsert(key, HASH_OBJECT_TYPE_TAG, &payload)?;
            }
        }
        Ok(())
    }

    fn handle_hdel_batch_fast_path(
        &self,
        key: DbKeyRef<'_>,
        fields: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        let current_payload = match self.object_read(key)? {
            Some(object) => {
                if object.object_type != HASH_OBJECT_TYPE_TAG {
                    return Err(RequestExecutionError::WrongType);
                }
                object.payload
            }
            None => {
                if self.key_exists(key)? {
                    return Err(RequestExecutionError::WrongType);
                }
                append_integer(response_out, 0);
                return Ok(());
            }
        };

        let delete_result =
            delete_hash_fields_payload_batch(&current_payload, fields).ok_or_else(|| {
                storage_failure(
                    "handle_hdel_batch_fast_path",
                    "failed to remove fields from serialized hash payload",
                )
            })?;
        match delete_result {
            HashPayloadBatchDeleteResult::NotFound => {
                append_integer(response_out, 0);
            }
            HashPayloadBatchDeleteResult::Removed {
                payload,
                removed,
                is_empty,
            } => {
                for field in fields {
                    self.set_hash_field_expiration_unix_millis(key, field, None);
                }
                self.notify_keyspace_event(key.db(), NOTIFY_HASH, b"hdel", key.key());
                if is_empty {
                    let _ = self.object_delete(key)?;
                    self.notify_keyspace_event(key.db(), NOTIFY_GENERIC, b"del", key.key());
                } else {
                    self.object_upsert(key, HASH_OBJECT_TYPE_TAG, &payload)?;
                }
                append_integer(response_out, removed);
            }
        }
        Ok(())
    }

    fn handle_hdel_single_field_fast_path(
        &self,
        key: DbKeyRef<'_>,
        field: &[u8],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        let current_payload = match self.object_read(key)? {
            Some(object) => {
                if object.object_type != HASH_OBJECT_TYPE_TAG {
                    return Err(RequestExecutionError::WrongType);
                }
                object.payload
            }
            None => {
                if self.key_exists(key)? {
                    return Err(RequestExecutionError::WrongType);
                }
                append_integer(response_out, 0);
                return Ok(());
            }
        };

        let delete_result =
            delete_single_hash_field_payload(&current_payload, field).ok_or_else(|| {
                storage_failure(
                    "handle_hdel_single_field_fast_path",
                    "failed to remove field from serialized hash payload",
                )
            })?;
        match delete_result {
            HashPayloadDeleteResult::NotFound => {
                append_integer(response_out, 0);
            }
            HashPayloadDeleteResult::Removed { payload, is_empty } => {
                self.set_hash_field_expiration_unix_millis(key, field, None);
                self.notify_keyspace_event(key.db(), NOTIFY_HASH, b"hdel", key.key());
                if is_empty {
                    let _ = self.object_delete(key)?;
                    self.notify_keyspace_event(key.db(), NOTIFY_GENERIC, b"del", key.key());
                } else {
                    self.object_upsert(key, HASH_OBJECT_TYPE_TAG, &payload)?;
                }
                append_integer(response_out, 1);
            }
        }
        Ok(())
    }

    fn handle_hgetdel_batch_fast_path(
        &self,
        key: DbKeyRef<'_>,
        fields: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        let resp3 = self.resp_protocol_version().is_resp3();
        let current_payload = match self.object_read(key)? {
            Some(object) => {
                if object.object_type != HASH_OBJECT_TYPE_TAG {
                    return Err(RequestExecutionError::WrongType);
                }
                object.payload
            }
            None => {
                if self.key_exists(key)? {
                    return Err(RequestExecutionError::WrongType);
                }
                append_array_length(response_out, fields.len());
                for _ in 0..fields.len() {
                    if resp3 {
                        append_null(response_out);
                    } else {
                        append_null_bulk_string(response_out);
                    }
                }
                return Ok(());
            }
        };

        let delete_result = get_and_delete_hash_fields_payload_batch(&current_payload, fields)
            .ok_or_else(|| {
                storage_failure(
                    "handle_hgetdel_batch_fast_path",
                    "failed to get and remove fields from serialized hash payload",
                )
            })?;

        match delete_result {
            HashPayloadBatchGetDeleteResult::NotFound { values } => {
                append_array_length(response_out, values.len());
                for value in values {
                    match value {
                        Some(value) => append_bulk_string(response_out, &value),
                        None => {
                            if resp3 {
                                append_null(response_out);
                            } else {
                                append_null_bulk_string(response_out);
                            }
                        }
                    }
                }
            }
            HashPayloadBatchGetDeleteResult::Removed {
                values,
                payload,
                is_empty,
                ..
            } => {
                append_array_length(response_out, values.len());
                for (index, value) in values.into_iter().enumerate() {
                    match value {
                        Some(value) => {
                            self.set_hash_field_expiration_unix_millis(key, fields[index], None);
                            append_bulk_string(response_out, &value);
                        }
                        None => {
                            if resp3 {
                                append_null(response_out);
                            } else {
                                append_null_bulk_string(response_out);
                            }
                        }
                    }
                }

                if is_empty {
                    let _ = self.object_delete(key)?;
                } else {
                    self.object_upsert(key, HASH_OBJECT_TYPE_TAG, &payload)?;
                }
            }
        }
        Ok(())
    }

    fn apply_hash_field_lazy_expiration(
        &self,
        key: DbKeyRef<'_>,
        hash: &mut BTreeMap<Vec<u8>, Vec<u8>>,
        fields: &[&[u8]],
    ) -> bool {
        let expired_fields = self.remove_expired_hash_fields_for_access(key, fields);
        if expired_fields.is_empty() {
            return false;
        }
        self.notify_keyspace_event(key.db(), NOTIFY_HASH, b"hexpired", key.key());
        for field in expired_fields {
            hash.remove(field.as_ref());
        }
        true
    }

    fn persist_hash_after_field_expiration(
        &self,
        key: DbKeyRef<'_>,
        hash: &BTreeMap<Vec<u8>, Vec<u8>>,
    ) -> Result<(), RequestExecutionError> {
        if hash.is_empty() {
            let _ = self.object_delete(key)?;
            self.notify_keyspace_event(key.db(), NOTIFY_GENERIC, b"del", key.key());
        } else {
            self.save_hash_object(key, hash)?;
        }
        Ok(())
    }
}

fn append_hash_scan_response(
    response_out: &mut Vec<u8>,
    cursor: u64,
    count: usize,
    pairs: &[(&[u8], &[u8])],
    novalues: bool,
    resp3: bool,
) {
    let raw_start = usize::try_from(cursor).unwrap_or(usize::MAX);
    let start = if raw_start >= pairs.len() {
        0
    } else {
        raw_start
    };
    let end = start.saturating_add(count).min(pairs.len());
    let next_cursor = if end >= pairs.len() { 0 } else { end };
    let page = &pairs[start..end];

    append_array_length(response_out, 2);
    let next_cursor_bytes = next_cursor.to_string();
    append_bulk_string(response_out, next_cursor_bytes.as_bytes());

    // In RESP3 with values, emit a map instead of a flat array.
    if resp3 && !novalues {
        append_map_length(response_out, page.len());
    } else if novalues {
        append_array_length(response_out, page.len());
    } else {
        append_array_length(response_out, page.len() * 2);
    }

    for (field, value) in page {
        append_bulk_string(response_out, field);
        if !novalues {
            append_bulk_string(response_out, value);
        }
    }
}

fn looks_like_signed_integer(input: &[u8]) -> bool {
    if input.is_empty() {
        return false;
    }
    let mut index = 0usize;
    if input[0] == b'+' || input[0] == b'-' {
        index = 1;
    }
    index < input.len() && input[index..].iter().all(u8::is_ascii_digit)
}

fn parse_hsetex_options(args: &[&[u8]]) -> Result<ParsedHashExpireOptions, RequestExecutionError> {
    const EXPECTED: &str = "HSETEX key [FNX|FXX] [EX seconds|PX milliseconds|EXAT unix-time-seconds|PXAT milliseconds-unix-time|KEEPTTL] FIELDS num field value [field value ...]";
    if args.len() < 6 {
        return Err(RequestExecutionError::WrongArity {
            command: "HSETEX",
            expected: EXPECTED,
        });
    }
    let mut index = 2usize;
    let mut field_set_condition = HashFieldSetCondition::Any;
    let mut expiration_unix_millis = None;
    let mut expire_if_past_immediately = false;
    let mut first_field_index = None;
    let mut field_count = None;
    let mut keep_ttl = false;

    while index < args.len() {
        let token = args[index];
        if ascii_eq_ignore_case(token, b"FIELDS") {
            if first_field_index.is_some() {
                return Err(RequestExecutionError::HashFieldsKeywordSpecifiedMultipleTimes);
            }
            let fields = parse_hash_fields_block(
                args,
                index,
                2,
                RequestExecutionError::HashInvalidNumberOfFields,
                "HSETEX",
                EXPECTED,
            )?;
            first_field_index = Some(fields.first_field_index);
            field_count = Some(fields.field_count);
            index = fields.next_index;
            continue;
        }
        if ascii_eq_ignore_case(token, b"FXX") {
            if field_set_condition != HashFieldSetCondition::Any {
                return Err(RequestExecutionError::HashOnlyOneOfFxxFnxArgumentsCanBeSpecified);
            }
            field_set_condition = HashFieldSetCondition::Fxx;
            index += 1;
            continue;
        }
        if ascii_eq_ignore_case(token, b"FNX") {
            if field_set_condition != HashFieldSetCondition::Any {
                return Err(RequestExecutionError::HashOnlyOneOfFxxFnxArgumentsCanBeSpecified);
            }
            field_set_condition = HashFieldSetCondition::Fnx;
            index += 1;
            continue;
        }
        if ascii_eq_ignore_case(token, b"KEEPTTL") {
            if keep_ttl || expiration_unix_millis.is_some() {
                return Err(
                    RequestExecutionError::HashOnlyOneOfHsetexExpireArgumentsCanBeSpecified,
                );
            }
            keep_ttl = true;
            index += 1;
            continue;
        }
        if ascii_eq_ignore_case(token, b"PERSIST") {
            return Err(RequestExecutionError::HashUnknownArgument);
        }
        if ascii_eq_ignore_case(token, b"PX")
            || ascii_eq_ignore_case(token, b"EX")
            || ascii_eq_ignore_case(token, b"PXAT")
            || ascii_eq_ignore_case(token, b"EXAT")
        {
            if expiration_unix_millis.is_some() || keep_ttl {
                return Err(
                    RequestExecutionError::HashOnlyOneOfHsetexExpireArgumentsCanBeSpecified,
                );
            }
            if index + 1 >= args.len() {
                return Err(RequestExecutionError::HashMissingExpireTime);
            }
            let value =
                parse_i64_ascii(args[index + 1]).ok_or(RequestExecutionError::ValueNotInteger)?;
            let now = current_unix_time_millis().ok_or(RequestExecutionError::ValueOutOfRange)?;
            let expiration = if ascii_eq_ignore_case(token, b"PX") {
                if value < 0 {
                    return Err(RequestExecutionError::ValueOutOfRange);
                }
                expire_if_past_immediately = value == 0;
                let delta =
                    u64::try_from(value).map_err(|_| RequestExecutionError::ValueOutOfRange)?;
                now.checked_add(delta)
                    .ok_or(RequestExecutionError::ValueOutOfRange)?
            } else if ascii_eq_ignore_case(token, b"EX") {
                if value < 0 {
                    return Err(RequestExecutionError::ValueOutOfRange);
                }
                expire_if_past_immediately = value == 0;
                let delta_secs =
                    u64::try_from(value).map_err(|_| RequestExecutionError::ValueOutOfRange)?;
                let delta_millis = delta_secs
                    .checked_mul(1000)
                    .ok_or(RequestExecutionError::ValueOutOfRange)?;
                now.checked_add(delta_millis)
                    .ok_or(RequestExecutionError::ValueOutOfRange)?
            } else if ascii_eq_ignore_case(token, b"PXAT") {
                if value < 0 {
                    return Err(RequestExecutionError::ValueOutOfRange);
                }
                expire_if_past_immediately = true;
                u64::try_from(value).map_err(|_| RequestExecutionError::ValueOutOfRange)?
            } else {
                if value < 0 {
                    return Err(RequestExecutionError::ValueOutOfRange);
                }
                expire_if_past_immediately = true;
                let at_secs =
                    u64::try_from(value).map_err(|_| RequestExecutionError::ValueOutOfRange)?;
                at_secs
                    .checked_mul(1000)
                    .ok_or(RequestExecutionError::ValueOutOfRange)?
            };
            expiration_unix_millis = Some(expiration);
            index += 2;
            continue;
        }
        return Err(RequestExecutionError::HashUnknownArgument);
    }

    let first_field_index =
        first_field_index.ok_or(RequestExecutionError::HashMandatoryFieldsArgumentMissing)?;
    let field_count =
        field_count.ok_or(RequestExecutionError::HashMandatoryFieldsArgumentMissing)?;

    Ok(ParsedHashExpireOptions {
        expiration_unix_millis,
        keep_ttl,
        first_field_index,
        field_count,
        expire_if_past_immediately,
        field_set_condition,
        persist: false,
    })
}

fn parse_hgetex_options(args: &[&[u8]]) -> Result<ParsedHashExpireOptions, RequestExecutionError> {
    const EXPECTED: &str = "HGETEX key [EX seconds|PX milliseconds|EXAT unix-time-seconds|PXAT milliseconds-unix-time|PERSIST] FIELDS num field [field ...]";
    if args.len() < 5 {
        return Err(RequestExecutionError::WrongArity {
            command: "HGETEX",
            expected: EXPECTED,
        });
    }
    let mut index = 2usize;
    let mut expiration_unix_millis = None;
    let mut expire_if_past_immediately = false;
    let mut first_field_index = None;
    let mut field_count = None;
    let mut persist = false;

    while index < args.len() {
        let token = args[index];
        if ascii_eq_ignore_case(token, b"FIELDS") {
            if first_field_index.is_some() {
                return Err(RequestExecutionError::HashFieldsKeywordSpecifiedMultipleTimes);
            }
            let fields = parse_hash_fields_block(
                args,
                index,
                1,
                RequestExecutionError::HashInvalidNumberOfFields,
                "HGETEX",
                EXPECTED,
            )?;
            first_field_index = Some(fields.first_field_index);
            field_count = Some(fields.field_count);
            index = fields.next_index;
            continue;
        }
        if ascii_eq_ignore_case(token, b"PERSIST") {
            if persist || expiration_unix_millis.is_some() {
                return Err(
                    RequestExecutionError::HashOnlyOneOfHgetexExpireArgumentsCanBeSpecified,
                );
            }
            persist = true;
            index += 1;
            continue;
        }
        if ascii_eq_ignore_case(token, b"FXX")
            || ascii_eq_ignore_case(token, b"FNX")
            || ascii_eq_ignore_case(token, b"KEEPTTL")
        {
            return Err(RequestExecutionError::HashUnknownArgument);
        }
        if ascii_eq_ignore_case(token, b"EX")
            || ascii_eq_ignore_case(token, b"PX")
            || ascii_eq_ignore_case(token, b"EXAT")
            || ascii_eq_ignore_case(token, b"PXAT")
        {
            if persist || expiration_unix_millis.is_some() {
                return Err(
                    RequestExecutionError::HashOnlyOneOfHgetexExpireArgumentsCanBeSpecified,
                );
            }
            if index + 1 >= args.len() {
                return Err(RequestExecutionError::HashMissingExpireTime);
            }
            let value =
                parse_i64_ascii(args[index + 1]).ok_or(RequestExecutionError::ValueNotInteger)?;
            let now = current_unix_time_millis().ok_or(RequestExecutionError::ValueOutOfRange)?;
            let (expiration, immediate) = if ascii_eq_ignore_case(token, b"EX") {
                if value < 0 {
                    return Err(RequestExecutionError::ValueOutOfRange);
                }
                let delta_secs =
                    u64::try_from(value).map_err(|_| RequestExecutionError::ValueOutOfRange)?;
                let delta_millis = delta_secs
                    .checked_mul(1000)
                    .ok_or(RequestExecutionError::ValueOutOfRange)?;
                (
                    now.checked_add(delta_millis)
                        .ok_or(RequestExecutionError::ValueOutOfRange)?,
                    value == 0,
                )
            } else if ascii_eq_ignore_case(token, b"PX") {
                if value < 0 {
                    return Err(RequestExecutionError::ValueOutOfRange);
                }
                let delta =
                    u64::try_from(value).map_err(|_| RequestExecutionError::ValueOutOfRange)?;
                (
                    now.checked_add(delta)
                        .ok_or(RequestExecutionError::ValueOutOfRange)?,
                    value == 0,
                )
            } else if ascii_eq_ignore_case(token, b"EXAT") {
                if value < 0 {
                    return Err(RequestExecutionError::ValueOutOfRange);
                }
                let at_secs =
                    u64::try_from(value).map_err(|_| RequestExecutionError::ValueOutOfRange)?;
                (
                    at_secs
                        .checked_mul(1000)
                        .ok_or(RequestExecutionError::ValueOutOfRange)?,
                    true,
                )
            } else {
                if value < 0 {
                    return Err(RequestExecutionError::ValueOutOfRange);
                }
                (
                    u64::try_from(value).map_err(|_| RequestExecutionError::ValueOutOfRange)?,
                    true,
                )
            };
            expiration_unix_millis = Some(expiration);
            expire_if_past_immediately = immediate;
            index += 2;
            continue;
        }
        return Err(RequestExecutionError::HashUnknownArgument);
    }

    let first_field_index =
        first_field_index.ok_or(RequestExecutionError::HashMandatoryFieldsArgumentMissing)?;
    let field_count =
        field_count.ok_or(RequestExecutionError::HashMandatoryFieldsArgumentMissing)?;
    Ok(ParsedHashExpireOptions {
        expiration_unix_millis,
        keep_ttl: false,
        first_field_index,
        field_count,
        expire_if_past_immediately,
        field_set_condition: HashFieldSetCondition::Any,
        persist,
    })
}

struct ParsedHashExpireOptions {
    expiration_unix_millis: Option<u64>,
    keep_ttl: bool,
    first_field_index: usize,
    field_count: usize,
    expire_if_past_immediately: bool,
    field_set_condition: HashFieldSetCondition,
    persist: bool,
}

struct ParsedHashFieldsBlock {
    first_field_index: usize,
    field_count: usize,
    next_index: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HashFieldSetCondition {
    Any,
    Fxx,
    Fnx,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HashFieldExpireCondition {
    None,
    Nx,
    Xx,
    Gt,
    Lt,
}

struct ParsedHashFieldExpireArguments<'a> {
    condition: HashFieldExpireCondition,
    fields: Vec<&'a [u8]>,
}

fn parse_hash_field_expire_arguments<'a>(
    args: &'a [&'a [u8]],
    first_option_index: usize,
    command: &'static str,
    expected: &'static str,
) -> Result<ParsedHashFieldExpireArguments<'a>, RequestExecutionError> {
    if args.len() <= first_option_index {
        return Err(RequestExecutionError::WrongArity { command, expected });
    }
    let mut index = first_option_index;
    let mut condition_flags = 0u8;
    let mut fields = None;

    while index < args.len() {
        let token = args[index];
        if ascii_eq_ignore_case(token, b"FIELDS") {
            if fields.is_some() {
                return Err(RequestExecutionError::HashFieldsKeywordSpecifiedMultipleTimes);
            }
            if index + 2 >= args.len() {
                return Err(RequestExecutionError::WrongArity { command, expected });
            }

            let field_count =
                parse_i64_ascii(args[index + 1]).ok_or(RequestExecutionError::ValueNotInteger)?;
            if field_count <= 0 {
                return Err(RequestExecutionError::HashNumFieldsParameterShouldBeGreaterThanZero);
            }
            let field_count =
                usize::try_from(field_count).map_err(|_| RequestExecutionError::ValueOutOfRange)?;
            let first_field_index = index + 2;
            let last_field_index = first_field_index
                .checked_add(field_count)
                .ok_or(RequestExecutionError::ValueOutOfRange)?;
            if last_field_index > args.len() {
                return Err(RequestExecutionError::WrongArity { command, expected });
            }

            fields = Some(args[first_field_index..last_field_index].to_vec());
            index = last_field_index;
            continue;
        }
        if ascii_eq_ignore_case(token, b"NX") {
            condition_flags |= 0b0001;
            index += 1;
            continue;
        }
        if ascii_eq_ignore_case(token, b"XX") {
            condition_flags |= 0b0010;
            index += 1;
            continue;
        }
        if ascii_eq_ignore_case(token, b"GT") {
            condition_flags |= 0b0100;
            index += 1;
            continue;
        }
        if ascii_eq_ignore_case(token, b"LT") {
            condition_flags |= 0b1000;
            index += 1;
            continue;
        }
        return Err(RequestExecutionError::HashUnknownArgument);
    }

    if condition_flags.count_ones() > 1 {
        return Err(RequestExecutionError::HashMultipleConditionFlagsSpecified);
    }

    let condition = if condition_flags & 0b0001 != 0 {
        HashFieldExpireCondition::Nx
    } else if condition_flags & 0b0010 != 0 {
        HashFieldExpireCondition::Xx
    } else if condition_flags & 0b0100 != 0 {
        HashFieldExpireCondition::Gt
    } else if condition_flags & 0b1000 != 0 {
        HashFieldExpireCondition::Lt
    } else {
        HashFieldExpireCondition::None
    };

    let fields = fields.ok_or(RequestExecutionError::HashMandatoryFieldsArgumentMissing)?;
    Ok(ParsedHashFieldExpireArguments { condition, fields })
}

fn parse_hash_fields<'a>(
    args: &'a [&'a [u8]],
    fields_index: usize,
    command: &'static str,
    expected: &'static str,
) -> Result<Vec<&'a [u8]>, RequestExecutionError> {
    if args.len() <= fields_index + 2 {
        return Err(RequestExecutionError::WrongArity { command, expected });
    }
    if !ascii_eq_ignore_case(args[fields_index], b"FIELDS") {
        return Err(RequestExecutionError::SyntaxError);
    }
    let field_count =
        parse_u64_ascii(args[fields_index + 1]).ok_or(RequestExecutionError::ValueNotInteger)?;
    if field_count == 0 {
        return Err(RequestExecutionError::ValueOutOfRange);
    }
    let field_count =
        usize::try_from(field_count).map_err(|_| RequestExecutionError::ValueOutOfRange)?;
    let first_field_index = fields_index + 2;
    let last_field_index = first_field_index
        .checked_add(field_count)
        .ok_or(RequestExecutionError::ValueOutOfRange)?;
    if args.len() != last_field_index {
        return Err(RequestExecutionError::WrongArity { command, expected });
    }
    Ok(args[first_field_index..last_field_index].to_vec())
}

fn parse_hgetdel_fields<'a>(args: &'a [&'a [u8]]) -> Result<Vec<&'a [u8]>, RequestExecutionError> {
    if !ascii_eq_ignore_case(args[2], b"FIELDS") {
        return Err(RequestExecutionError::HashMandatoryFieldsArgumentMissing);
    }
    let field_count = parse_i64_ascii(args[3])
        .ok_or(RequestExecutionError::HashNumberOfFieldsMustBePositiveInteger)?;
    if field_count <= 0 {
        return Err(RequestExecutionError::HashNumberOfFieldsMustBePositiveInteger);
    }
    let field_count = usize::try_from(field_count)
        .map_err(|_| RequestExecutionError::HashNumberOfFieldsMustBePositiveInteger)?;
    if field_count != args.len().saturating_sub(4) {
        return Err(RequestExecutionError::HashNumfieldsParameterMustMatchArguments);
    }
    Ok(args[4..].to_vec())
}

fn parse_hash_fields_block(
    args: &[&[u8]],
    fields_index: usize,
    args_per_field: usize,
    invalid_count_error: RequestExecutionError,
    command: &'static str,
    expected: &'static str,
) -> Result<ParsedHashFieldsBlock, RequestExecutionError> {
    if fields_index + 1 >= args.len() {
        return Err(RequestExecutionError::WrongArity { command, expected });
    }
    let field_count = parse_i64_ascii(args[fields_index + 1]).ok_or(invalid_count_error)?;
    if field_count <= 0 {
        return Err(invalid_count_error);
    }
    let field_count = usize::try_from(field_count).map_err(|_| invalid_count_error)?;
    let first_field_index = fields_index + 2;
    let values_len = field_count
        .checked_mul(args_per_field)
        .ok_or(invalid_count_error)?;
    let next_index = first_field_index
        .checked_add(values_len)
        .ok_or(invalid_count_error)?;
    if next_index > args.len() {
        return Err(RequestExecutionError::WrongArity { command, expected });
    }
    Ok(ParsedHashFieldsBlock {
        first_field_index,
        field_count,
        next_index,
    })
}

fn collect_hash_fields<'a>(
    args: &'a [&'a [u8]],
    first_field_index: usize,
    field_count: usize,
) -> Result<Vec<&'a [u8]>, RequestExecutionError> {
    let end_index = first_field_index
        .checked_add(field_count)
        .ok_or(RequestExecutionError::HashInvalidNumberOfFields)?;
    Ok(args[first_field_index..end_index].to_vec())
}

#[allow(clippy::type_complexity)]
fn collect_hash_field_values<'a>(
    args: &'a [&'a [u8]],
    first_field_index: usize,
    field_count: usize,
) -> Result<Vec<(&'a [u8], &'a [u8])>, RequestExecutionError> {
    let values_len = field_count
        .checked_mul(2)
        .ok_or(RequestExecutionError::HashInvalidNumberOfFields)?;
    let end_index = first_field_index
        .checked_add(values_len)
        .ok_or(RequestExecutionError::HashInvalidNumberOfFields)?;
    let mut out = Vec::with_capacity(field_count);
    let mut index = first_field_index;
    while index < end_index {
        out.push((args[index], args[index + 1]));
        index += 2;
    }
    Ok(out)
}

fn append_integer_array(response_out: &mut Vec<u8>, values: &[i64]) {
    append_array_length(response_out, values.len());
    for value in values {
        append_integer(response_out, *value);
    }
}

fn append_hash_integer_array(response_out: &mut Vec<u8>, len: usize, value: i64) {
    append_array_length(response_out, len);
    for _ in 0..len {
        append_integer(response_out, value);
    }
}
