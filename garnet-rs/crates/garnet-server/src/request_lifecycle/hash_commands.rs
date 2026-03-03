use super::*;

impl RequestProcessor {
    pub(super) fn maybe_handle_hash_field_expire_extension(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<bool, RequestExecutionError> {
        if args.is_empty() {
            return Ok(false);
        }
        let command = args[0];
        if ascii_eq_ignore_case(command, b"HSETEX") {
            self.handle_hsetex(args, response_out)?;
            return Ok(true);
        }
        if ascii_eq_ignore_case(command, b"HGETEX") {
            self.handle_hgetex(args, response_out)?;
            return Ok(true);
        }
        if ascii_eq_ignore_case(command, b"HGETDEL") {
            self.handle_hgetdel(args, response_out)?;
            return Ok(true);
        }
        if ascii_eq_ignore_case(command, b"HEXPIRE") {
            self.handle_hexpire(args, response_out)?;
            return Ok(true);
        }
        if ascii_eq_ignore_case(command, b"HPEXPIRE") {
            self.handle_hpexpire(args, response_out)?;
            return Ok(true);
        }
        if ascii_eq_ignore_case(command, b"HPEXPIREAT") {
            self.handle_hpexpireat(args, response_out)?;
            return Ok(true);
        }
        if ascii_eq_ignore_case(command, b"HEXPIREAT") {
            self.handle_hexpireat(args, response_out)?;
            return Ok(true);
        }
        if ascii_eq_ignore_case(command, b"HPERSIST") {
            self.handle_hpersist(args, response_out)?;
            return Ok(true);
        }
        if ascii_eq_ignore_case(command, b"HPTTL") {
            self.handle_hpttl(args, response_out)?;
            return Ok(true);
        }
        if ascii_eq_ignore_case(command, b"HTTL") {
            self.handle_httl(args, response_out)?;
            return Ok(true);
        }
        if ascii_eq_ignore_case(command, b"HPEXPIRETIME") {
            self.handle_hpexpiretime(args, response_out)?;
            return Ok(true);
        }
        if ascii_eq_ignore_case(command, b"HEXPIRETIME") {
            self.handle_hexpiretime(args, response_out)?;
            return Ok(true);
        }
        Ok(false)
    }

    pub(super) fn handle_hset(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_paired_arity_after(args, 4, 2, "HSET", "HSET key field value [field value ...]")?;

        let key = RedisKey::from(args[1]);
        let mut hash = self.load_hash_object(&key)?.unwrap_or_default();
        let access_fields = args[2..]
            .chunks_exact(2)
            .map(|entry| entry[0])
            .collect::<Vec<_>>();
        self.apply_hash_field_lazy_expiration(&key, &mut hash, &access_fields);
        let mut inserted = 0i64;

        let mut index = 2usize;
        while index < args.len() {
            let field = args[index].to_vec();
            let value = args[index + 1].to_vec();
            if hash.insert(field, value).is_none() {
                inserted += 1;
            }
            self.set_hash_field_expiration_unix_millis(&key, args[index], None);
            index += 2;
        }

        self.save_hash_object(&key, &hash)?;
        self.notify_keyspace_event(NOTIFY_HASH, b"hset", &key);
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
        let mut hash = match self.load_hash_object(&key)? {
            Some(hash) => hash,
            None => {
                append_null_bulk_string(response_out);
                return Ok(());
            }
        };
        let lazy_expired = self.apply_hash_field_lazy_expiration(&key, &mut hash, &[field]);
        if lazy_expired {
            self.persist_hash_after_field_expiration(&key, &hash)?;
            if hash.is_empty() {
                append_null_bulk_string(response_out);
                return Ok(());
            }
        }

        match hash.get(field) {
            Some(value) => append_bulk_string(response_out, value),
            None => append_null_bulk_string(response_out),
        }
        Ok(())
    }

    pub(super) fn handle_hdel(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 3, "HDEL", "HDEL key field [field ...]")?;

        let key = RedisKey::from(args[1]);
        let mut hash = match self.load_hash_object(&key)? {
            Some(hash) => hash,
            None => {
                append_integer(response_out, 0);
                return Ok(());
            }
        };
        self.apply_hash_field_lazy_expiration(&key, &mut hash, &args[2..]);

        let mut removed = 0i64;
        for field in &args[2..] {
            if hash.remove(*field).is_some() {
                removed += 1;
                self.set_hash_field_expiration_unix_millis(&key, field, None);
            }
        }

        if removed > 0 {
            self.notify_keyspace_event(NOTIFY_HASH, b"hdel", &key);
        }
        if hash.is_empty() {
            let _ = self.object_delete(&key)?;
            if removed > 0 {
                self.notify_keyspace_event(NOTIFY_GENERIC, b"del", &key);
            }
        } else {
            self.save_hash_object(&key, &hash)?;
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

        let key = RedisKey::from(args[1]);
        let resp3 = self.resp_protocol_version().is_resp3();
        let hash = match self.load_hash_object(&key)? {
            Some(hash) => hash,
            None => {
                if resp3 {
                    append_map_length(response_out, 0);
                } else {
                    response_out.extend_from_slice(b"*0\r\n");
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

        let key = RedisKey::from(args[1]);
        let len = self
            .load_hash_object(&key)?
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

        let key = RedisKey::from(args[1]);
        let mut hash = self.load_hash_object(&key)?;
        if let Some(hash_mut) = hash.as_mut() {
            let lazy_expired = self.apply_hash_field_lazy_expiration(&key, hash_mut, &args[2..]);
            if lazy_expired {
                self.persist_hash_after_field_expiration(&key, hash_mut)?;
            }
        }
        let field_count = args.len() - 2;
        response_out.push(b'*');
        response_out.extend_from_slice(field_count.to_string().as_bytes());
        response_out.extend_from_slice(b"\r\n");
        for field in &args[2..] {
            match hash.as_ref().and_then(|hash| hash.get(*field)) {
                Some(value) => append_bulk_string(response_out, value),
                None => append_null_bulk_string(response_out),
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

        let key = RedisKey::from(args[1]);
        let mut hash = self.load_hash_object(&key)?.unwrap_or_default();
        let access_fields = args[2..]
            .chunks_exact(2)
            .map(|entry| entry[0])
            .collect::<Vec<_>>();
        self.apply_hash_field_lazy_expiration(&key, &mut hash, &access_fields);
        let mut index = 2usize;
        while index < args.len() {
            let field = args[index].to_vec();
            let value = args[index + 1].to_vec();
            hash.insert(field, value);
            self.set_hash_field_expiration_unix_millis(&key, args[index], None);
            index += 2;
        }
        self.save_hash_object(&key, &hash)?;
        self.notify_keyspace_event(NOTIFY_HASH, b"hset", &key);
        append_simple_string(response_out, b"OK");
        Ok(())
    }

    pub(super) fn handle_hsetnx(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 4, "HSETNX", "HSETNX key field value")?;

        let key = RedisKey::from(args[1]);
        let mut hash = self.load_hash_object(&key)?.unwrap_or_default();
        let field = args[2].to_vec();
        self.apply_hash_field_lazy_expiration(&key, &mut hash, &[args[2]]);
        if hash.contains_key(&field) {
            append_integer(response_out, 0);
            return Ok(());
        }
        let value = args[3].to_vec();
        hash.insert(field, value);
        self.set_hash_field_expiration_unix_millis(&key, args[2], None);
        self.save_hash_object(&key, &hash)?;
        self.notify_keyspace_event(NOTIFY_HASH, b"hset", &key);
        append_integer(response_out, 1);
        Ok(())
    }

    pub(super) fn handle_hexists(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 3, "HEXISTS", "HEXISTS key field")?;

        let key = RedisKey::from(args[1]);
        let field = args[2];
        let mut hash = match self.load_hash_object(&key)? {
            Some(hash) => hash,
            None => {
                append_integer(response_out, 0);
                return Ok(());
            }
        };
        let lazy_expired = self.apply_hash_field_lazy_expiration(&key, &mut hash, &[field]);
        if lazy_expired {
            self.persist_hash_after_field_expiration(&key, &hash)?;
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

        let key = RedisKey::from(args[1]);
        let hash = match self.load_hash_object(&key)? {
            Some(hash) => hash,
            None => {
                response_out.extend_from_slice(b"*0\r\n");
                return Ok(());
            }
        };
        response_out.push(b'*');
        response_out.extend_from_slice(hash.len().to_string().as_bytes());
        response_out.extend_from_slice(b"\r\n");
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

        let key = RedisKey::from(args[1]);
        let hash = match self.load_hash_object(&key)? {
            Some(hash) => hash,
            None => {
                response_out.extend_from_slice(b"*0\r\n");
                return Ok(());
            }
        };
        response_out.push(b'*');
        response_out.extend_from_slice(hash.len().to_string().as_bytes());
        response_out.extend_from_slice(b"\r\n");
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

        let key = RedisKey::from(args[1]);
        let field = args[2];
        let mut hash = match self.load_hash_object(&key)? {
            Some(hash) => hash,
            None => {
                append_integer(response_out, 0);
                return Ok(());
            }
        };
        let lazy_expired = self.apply_hash_field_lazy_expiration(&key, &mut hash, &[field]);
        if lazy_expired {
            self.persist_hash_after_field_expiration(&key, &hash)?;
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

        let key = RedisKey::from(args[1]);
        let cursor = parse_u64_ascii(args[2]).ok_or(RequestExecutionError::ValueNotInteger)?;
        let scan_options = parse_scan_match_count_options(args, 3)?;
        let novalues = scan_options.novalues;

        let Some(hash) = self.load_hash_object(&key)? else {
            append_hash_scan_response(response_out, cursor, scan_options.count, &[], novalues);
            return Ok(());
        };
        let mut hash = hash;
        let all_fields_owned = hash.keys().cloned().collect::<Vec<_>>();
        let all_fields = all_fields_owned
            .iter()
            .map(Vec::as_slice)
            .collect::<Vec<_>>();
        let lazy_expired = self.apply_hash_field_lazy_expiration(&key, &mut hash, &all_fields);
        if lazy_expired {
            self.persist_hash_after_field_expiration(&key, &hash)?;
            if hash.is_empty() {
                append_hash_scan_response(response_out, cursor, scan_options.count, &[], novalues);
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

        append_hash_scan_response(response_out, cursor, scan_options.count, &matched, novalues);
        Ok(())
    }

    pub(super) fn handle_hincrby(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 4, "HINCRBY", "HINCRBY key field increment")?;

        let increment = parse_i64_ascii(args[3]).ok_or(RequestExecutionError::ValueNotInteger)?;
        let key = RedisKey::from(args[1]);
        let field = args[2].to_vec();
        let mut hash = self.load_hash_object(&key)?.unwrap_or_default();
        self.apply_hash_field_lazy_expiration(&key, &mut hash, &[args[2]]);

        let current = match hash.get(&field) {
            Some(value) => parse_i64_ascii(value).ok_or(RequestExecutionError::ValueNotInteger)?,
            None => 0,
        };
        let updated = current
            .checked_add(increment)
            .ok_or(RequestExecutionError::IncrementOverflow)?;
        hash.insert(field, updated.to_string().into_bytes());
        self.save_hash_object(&key, &hash)?;
        self.notify_keyspace_event(NOTIFY_HASH, b"hincrby", &key);
        append_integer(response_out, updated);
        Ok(())
    }

    pub(super) fn handle_hincrbyfloat(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 4, "HINCRBYFLOAT", "HINCRBYFLOAT key field increment")?;

        let increment = parse_f64_ascii(args[3]).ok_or(RequestExecutionError::ValueNotFloat)?;
        let key = RedisKey::from(args[1]);
        let field = args[2].to_vec();
        let mut hash = self.load_hash_object(&key)?.unwrap_or_default();
        self.apply_hash_field_lazy_expiration(&key, &mut hash, &[args[2]]);

        let current = match hash.get(&field) {
            Some(value) => parse_f64_ascii(value).ok_or(RequestExecutionError::ValueNotFloat)?,
            None => 0.0,
        };
        let updated = current + increment;
        if !updated.is_finite() {
            return Err(RequestExecutionError::ValueNotFloat);
        }
        let updated_text = updated.to_string().into_bytes();
        hash.insert(field, updated_text.clone());
        self.save_hash_object(&key, &hash)?;
        self.notify_keyspace_event(NOTIFY_HASH, b"hincrbyfloat", &key);
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

        let key = RedisKey::from(args[1]);
        let mut hash = self.load_hash_object(&key)?;
        if let Some(hash_mut) = hash.as_mut() {
            let all_fields_owned = hash_mut.keys().cloned().collect::<Vec<_>>();
            let all_fields = all_fields_owned
                .iter()
                .map(Vec::as_slice)
                .collect::<Vec<_>>();
            let lazy_expired = self.apply_hash_field_lazy_expiration(&key, hash_mut, &all_fields);
            if lazy_expired {
                self.persist_hash_after_field_expiration(&key, hash_mut)?;
            }
        }
        let resp3 = self.resp_protocol_version().is_resp3();

        if args.len() == 2 {
            let Some(hash) = hash else {
                append_null_bulk_string(response_out);
                return Ok(());
            };
            if hash.is_empty() {
                append_null_bulk_string(response_out);
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
            response_out.extend_from_slice(b"*0\r\n");
            return Ok(());
        }

        let Some(hash) = hash else {
            response_out.extend_from_slice(b"*0\r\n");
            return Ok(());
        };
        if hash.is_empty() {
            response_out.extend_from_slice(b"*0\r\n");
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
                response_out.push(b'*');
                response_out.extend_from_slice(sampled.len().to_string().as_bytes());
                response_out.extend_from_slice(b"\r\n");
                for (field, value) in sampled {
                    response_out.extend_from_slice(b"*2\r\n");
                    append_bulk_string(response_out, field);
                    append_bulk_string(response_out, value);
                }
            } else {
                response_out.push(b'*');
                response_out.extend_from_slice((sampled.len() * 2).to_string().as_bytes());
                response_out.extend_from_slice(b"\r\n");
                for (field, value) in sampled {
                    append_bulk_string(response_out, field);
                    append_bulk_string(response_out, value);
                }
            }
            return Ok(());
        }

        if resp3 {
            response_out.push(b'*');
            response_out.extend_from_slice(sampled.len().to_string().as_bytes());
            response_out.extend_from_slice(b"\r\n");
            for (field, _) in sampled {
                response_out.extend_from_slice(b"*1\r\n");
                append_bulk_string(response_out, field);
            }
        } else {
            response_out.push(b'*');
            response_out.extend_from_slice(sampled.len().to_string().as_bytes());
            response_out.extend_from_slice(b"\r\n");
            for (field, _) in sampled {
                append_bulk_string(response_out, field);
            }
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
            5,
            "HSETEX",
            "HSETEX key [PX milliseconds|PXAT milliseconds-unix-time] FIELDS num field value [field value ...]",
        )?;

        let key = RedisKey::from(args[1]);
        let expire_options = parse_hsetex_options(args)?;
        let field_values = parse_hash_fields_with_values(
            args,
            expire_options.fields_index,
            "HSETEX",
            "HSETEX key [PX milliseconds|PXAT milliseconds-unix-time] FIELDS num field value [field value ...]",
        )?;

        let access_fields = field_values
            .iter()
            .map(|(field, _)| *field)
            .collect::<Vec<_>>();
        let mut hash = self.load_hash_object(&key)?.unwrap_or_default();
        self.apply_hash_field_lazy_expiration(&key, &mut hash, &access_fields);

        let mut inserted = 0i64;
        for (field, value) in &field_values {
            if hash.insert((*field).to_vec(), (*value).to_vec()).is_none() {
                inserted += 1;
            }
            self.set_hash_field_expiration_unix_millis(
                &key,
                field,
                expire_options.expiration_unix_millis,
            );
        }

        if expire_options.expire_if_past_immediately
            && let Some(expiration) = expire_options.expiration_unix_millis
            && expiration
                <= current_unix_time_millis().ok_or(RequestExecutionError::ValueOutOfRange)?
        {
            self.apply_hash_field_lazy_expiration(&key, &mut hash, &access_fields);
        }

        if hash.is_empty() {
            let _ = self.object_delete(&key)?;
        } else {
            self.save_hash_object(&key, &hash)?;
        }
        append_integer(response_out, inserted);
        Ok(())
    }

    pub(super) fn handle_hgetex(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            4,
            "HGETEX",
            "HGETEX key [PX milliseconds|PXAT milliseconds-unix-time] FIELDS num field [field ...]",
        )?;
        let key = RedisKey::from(args[1]);
        let expire_options = parse_hgetex_options(args)?;
        let fields = parse_hash_fields(
            args,
            expire_options.fields_index,
            "HGETEX",
            "HGETEX key [PX milliseconds|PXAT milliseconds-unix-time] FIELDS num field [field ...]",
        )?;

        let mut hash = match self.load_hash_object(&key)? {
            Some(hash) => hash,
            None => {
                response_out.push(b'*');
                response_out.extend_from_slice(fields.len().to_string().as_bytes());
                response_out.extend_from_slice(b"\r\n");
                for _ in 0..fields.len() {
                    append_null_bulk_string(response_out);
                }
                return Ok(());
            }
        };

        let lazy_expired = self.apply_hash_field_lazy_expiration(&key, &mut hash, &fields);
        response_out.push(b'*');
        response_out.extend_from_slice(fields.len().to_string().as_bytes());
        response_out.extend_from_slice(b"\r\n");
        for field in &fields {
            match hash.get(*field) {
                Some(value) => append_bulk_string(response_out, value),
                None => append_null_bulk_string(response_out),
            }
        }

        for field in &fields {
            if hash.contains_key(*field) {
                self.set_hash_field_expiration_unix_millis(
                    &key,
                    field,
                    expire_options.expiration_unix_millis,
                );
            } else {
                self.set_hash_field_expiration_unix_millis(&key, field, None);
            }
        }

        if expire_options.expire_if_past_immediately
            && let Some(expiration) = expire_options.expiration_unix_millis
            && expiration
                <= current_unix_time_millis().ok_or(RequestExecutionError::ValueOutOfRange)?
        {
            self.apply_hash_field_lazy_expiration(&key, &mut hash, &fields);
        }

        if lazy_expired || expire_options.expiration_unix_millis.is_some() {
            self.persist_hash_after_field_expiration(&key, &hash)?;
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
            4,
            "HGETDEL",
            "HGETDEL key FIELDS num field [field ...]",
        )?;
        let key = RedisKey::from(args[1]);
        let fields = parse_hash_fields(
            args,
            2,
            "HGETDEL",
            "HGETDEL key FIELDS num field [field ...]",
        )?;

        let mut hash = match self.load_hash_object(&key)? {
            Some(hash) => hash,
            None => {
                response_out.push(b'*');
                response_out.extend_from_slice(fields.len().to_string().as_bytes());
                response_out.extend_from_slice(b"\r\n");
                for _ in 0..fields.len() {
                    append_null_bulk_string(response_out);
                }
                return Ok(());
            }
        };

        self.apply_hash_field_lazy_expiration(&key, &mut hash, &fields);
        response_out.push(b'*');
        response_out.extend_from_slice(fields.len().to_string().as_bytes());
        response_out.extend_from_slice(b"\r\n");
        for field in &fields {
            match hash.remove(*field) {
                Some(value) => {
                    self.set_hash_field_expiration_unix_millis(&key, field, None);
                    append_bulk_string(response_out, &value);
                }
                None => append_null_bulk_string(response_out),
            }
        }

        if hash.is_empty() {
            let _ = self.object_delete(&key)?;
        } else {
            self.save_hash_object(&key, &hash)?;
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
        self.handle_hash_field_expire_common(
            args,
            3,
            expiration_unix_millis,
            false,
            "HEXPIRE",
            "HEXPIRE key seconds FIELDS num field [field ...]",
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
        self.handle_hash_field_expire_common(
            args,
            3,
            expiration_unix_millis,
            false,
            "HPEXPIRE",
            "HPEXPIRE key milliseconds FIELDS num field [field ...]",
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
        self.handle_hash_field_expire_common(
            args,
            3,
            expiration_unix_millis,
            true,
            "HPEXPIREAT",
            "HPEXPIREAT key milliseconds-unix-time FIELDS num field [field ...]",
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
        self.handle_hash_field_expire_common(
            args,
            3,
            expiration_unix_millis,
            true,
            "HEXPIREAT",
            "HEXPIREAT key unix-time-seconds FIELDS num field [field ...]",
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
        let key = RedisKey::from(args[1]);
        let fields = parse_hash_fields(
            args,
            2,
            "HPERSIST",
            "HPERSIST key FIELDS num field [field ...]",
        )?;
        let mut hash = match self.load_hash_object(&key)? {
            Some(hash) => hash,
            None => {
                append_hash_integer_array(response_out, fields.len(), -2);
                return Ok(());
            }
        };
        let lazy_expired = self.apply_hash_field_lazy_expiration(&key, &mut hash, &fields);

        let mut statuses = Vec::with_capacity(fields.len());
        for field in &fields {
            if !hash.contains_key(*field) {
                self.set_hash_field_expiration_unix_millis(&key, field, None);
                statuses.push(-2);
                continue;
            }
            if self
                .hash_field_expiration_unix_millis(&key, field)
                .is_some()
            {
                self.set_hash_field_expiration_unix_millis(&key, field, None);
                statuses.push(1);
            } else {
                statuses.push(-1);
            }
        }

        if lazy_expired {
            self.persist_hash_after_field_expiration(&key, &hash)?;
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
        let key = RedisKey::from(args[1]);
        let fields = parse_hash_fields(args, 2, command_name, command_usage)?;
        let mut hash = match self.load_hash_object(&key)? {
            Some(hash) => hash,
            None => {
                append_hash_integer_array(response_out, fields.len(), -2);
                return Ok(());
            }
        };
        let lazy_expired = self.apply_hash_field_lazy_expiration(&key, &mut hash, &fields);
        let now_unix_millis =
            current_unix_time_millis().ok_or(RequestExecutionError::ValueOutOfRange)?;

        let mut values = Vec::with_capacity(fields.len());
        for field in &fields {
            if !hash.contains_key(*field) {
                values.push(-2);
                continue;
            }
            let Some(expiration_unix_millis) = self.hash_field_expiration_unix_millis(&key, field)
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
            self.persist_hash_after_field_expiration(&key, &hash)?;
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
        let key = RedisKey::from(args[1]);
        let fields = parse_hash_fields(args, 2, command_name, command_usage)?;
        let mut hash = match self.load_hash_object(&key)? {
            Some(hash) => hash,
            None => {
                append_hash_integer_array(response_out, fields.len(), -2);
                return Ok(());
            }
        };
        let lazy_expired = self.apply_hash_field_lazy_expiration(&key, &mut hash, &fields);

        let mut values = Vec::with_capacity(fields.len());
        for field in &fields {
            if !hash.contains_key(*field) {
                values.push(-2);
                continue;
            }
            let Some(expiration_unix_millis) = self.hash_field_expiration_unix_millis(&key, field)
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
            self.persist_hash_after_field_expiration(&key, &hash)?;
        }
        append_integer_array(response_out, &values);
        Ok(())
    }

    pub(super) fn hash_get_field_for_sort_lookup(
        &self,
        key: &[u8],
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
        key: &[u8],
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
        self.persist_hash_after_field_expiration(key, &hash)
    }

    #[allow(clippy::too_many_arguments)]
    fn handle_hash_field_expire_common(
        &self,
        args: &[&[u8]],
        fields_index: usize,
        expiration_unix_millis: u64,
        expire_if_past_immediately: bool,
        command_name: &'static str,
        command_usage: &'static str,
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        let key = RedisKey::from(args[1]);
        let fields = parse_hash_fields(args, fields_index, command_name, command_usage)?;
        let mut hash = match self.load_hash_object(&key)? {
            Some(hash) => hash,
            None => {
                append_hash_integer_array(response_out, fields.len(), -2);
                return Ok(());
            }
        };
        self.apply_hash_field_lazy_expiration(&key, &mut hash, &fields);

        let mut statuses = Vec::with_capacity(fields.len());
        for field in &fields {
            if hash.contains_key(*field) {
                self.set_hash_field_expiration_unix_millis(
                    &key,
                    field,
                    Some(expiration_unix_millis),
                );
                statuses.push(1);
            } else {
                self.set_hash_field_expiration_unix_millis(&key, field, None);
                statuses.push(-2);
            }
        }

        if expire_if_past_immediately
            && expiration_unix_millis
                <= current_unix_time_millis().ok_or(RequestExecutionError::ValueOutOfRange)?
        {
            self.apply_hash_field_lazy_expiration(&key, &mut hash, &fields);
        }
        self.persist_hash_after_field_expiration(&key, &hash)?;
        append_integer_array(response_out, &statuses);
        Ok(())
    }

    fn apply_hash_field_lazy_expiration(
        &self,
        key: &[u8],
        hash: &mut BTreeMap<Vec<u8>, Vec<u8>>,
        fields: &[&[u8]],
    ) -> bool {
        let expired_fields = self.remove_expired_hash_fields_for_access(key, fields);
        if expired_fields.is_empty() {
            return false;
        }
        for field in expired_fields {
            hash.remove(field.as_ref());
        }
        true
    }

    fn persist_hash_after_field_expiration(
        &self,
        key: &[u8],
        hash: &BTreeMap<Vec<u8>, Vec<u8>>,
    ) -> Result<(), RequestExecutionError> {
        if hash.is_empty() {
            let _ = self.object_delete(key)?;
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
) {
    let raw_start = usize::try_from(cursor).unwrap_or(usize::MAX);
    let start = if raw_start >= pairs.len() {
        0
    } else {
        raw_start
    };
    let end = start.saturating_add(count).min(pairs.len());
    let next_cursor = if end >= pairs.len() { 0 } else { end };

    response_out.push(b'*');
    response_out.extend_from_slice(b"2\r\n");
    let next_cursor_bytes = next_cursor.to_string();
    append_bulk_string(response_out, next_cursor_bytes.as_bytes());
    response_out.push(b'*');
    let items_per_entry = if novalues { 1 } else { 2 };
    response_out.extend_from_slice(((end - start) * items_per_entry).to_string().as_bytes());
    response_out.extend_from_slice(b"\r\n");
    for (field, value) in &pairs[start..end] {
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
    if args.len() < 5 {
        return Err(RequestExecutionError::WrongArity {
            command: "HSETEX",
            expected: "HSETEX key [PX milliseconds|PXAT milliseconds-unix-time] FIELDS num field value [field value ...]",
        });
    }
    if ascii_eq_ignore_case(args[2], b"FIELDS") {
        return Ok(ParsedHashExpireOptions {
            expiration_unix_millis: None,
            fields_index: 2,
            expire_if_past_immediately: false,
        });
    }
    if args.len() < 7 {
        return Err(RequestExecutionError::WrongArity {
            command: "HSETEX",
            expected: "HSETEX key [PX milliseconds|PXAT milliseconds-unix-time] FIELDS num field value [field value ...]",
        });
    }
    let option = args[2];
    let value = parse_i64_ascii(args[3]).ok_or(RequestExecutionError::ValueNotInteger)?;
    let now = current_unix_time_millis().ok_or(RequestExecutionError::ValueOutOfRange)?;
    let mut expire_if_past_immediately = false;
    let expiration_unix_millis = if ascii_eq_ignore_case(option, b"PX") {
        if value < 0 {
            return Err(RequestExecutionError::ValueOutOfRange);
        }
        let delta = u64::try_from(value).map_err(|_| RequestExecutionError::ValueOutOfRange)?;
        now.checked_add(delta)
            .ok_or(RequestExecutionError::ValueOutOfRange)?
    } else if ascii_eq_ignore_case(option, b"PXAT") {
        if value < 0 {
            return Err(RequestExecutionError::ValueOutOfRange);
        }
        expire_if_past_immediately = true;
        u64::try_from(value).map_err(|_| RequestExecutionError::ValueOutOfRange)?
    } else {
        return Err(RequestExecutionError::SyntaxError);
    };
    if !ascii_eq_ignore_case(args[4], b"FIELDS") {
        return Err(RequestExecutionError::SyntaxError);
    }
    Ok(ParsedHashExpireOptions {
        expiration_unix_millis: Some(expiration_unix_millis),
        fields_index: 4,
        expire_if_past_immediately,
    })
}

fn parse_hgetex_options(args: &[&[u8]]) -> Result<ParsedHashExpireOptions, RequestExecutionError> {
    if args.len() < 4 {
        return Err(RequestExecutionError::WrongArity {
            command: "HGETEX",
            expected: "HGETEX key [PX milliseconds|PXAT milliseconds-unix-time] FIELDS num field [field ...]",
        });
    }
    if ascii_eq_ignore_case(args[2], b"FIELDS") {
        return Ok(ParsedHashExpireOptions {
            expiration_unix_millis: None,
            fields_index: 2,
            expire_if_past_immediately: false,
        });
    }
    if args.len() < 6 {
        return Err(RequestExecutionError::WrongArity {
            command: "HGETEX",
            expected: "HGETEX key [PX milliseconds|PXAT milliseconds-unix-time] FIELDS num field [field ...]",
        });
    }
    let option = args[2];
    let value = parse_i64_ascii(args[3]).ok_or(RequestExecutionError::ValueNotInteger)?;
    let now = current_unix_time_millis().ok_or(RequestExecutionError::ValueOutOfRange)?;
    let mut expire_if_past_immediately = false;
    let expiration_unix_millis = if ascii_eq_ignore_case(option, b"PX") {
        if value < 0 {
            return Err(RequestExecutionError::ValueOutOfRange);
        }
        let delta = u64::try_from(value).map_err(|_| RequestExecutionError::ValueOutOfRange)?;
        now.checked_add(delta)
            .ok_or(RequestExecutionError::ValueOutOfRange)?
    } else if ascii_eq_ignore_case(option, b"PXAT") {
        if value < 0 {
            return Err(RequestExecutionError::ValueOutOfRange);
        }
        expire_if_past_immediately = true;
        u64::try_from(value).map_err(|_| RequestExecutionError::ValueOutOfRange)?
    } else {
        return Err(RequestExecutionError::SyntaxError);
    };
    if !ascii_eq_ignore_case(args[4], b"FIELDS") {
        return Err(RequestExecutionError::SyntaxError);
    }
    Ok(ParsedHashExpireOptions {
        expiration_unix_millis: Some(expiration_unix_millis),
        fields_index: 4,
        expire_if_past_immediately,
    })
}

struct ParsedHashExpireOptions {
    expiration_unix_millis: Option<u64>,
    fields_index: usize,
    expire_if_past_immediately: bool,
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

#[allow(clippy::type_complexity)]
fn parse_hash_fields_with_values<'a>(
    args: &'a [&'a [u8]],
    fields_index: usize,
    command: &'static str,
    expected: &'static str,
) -> Result<Vec<(&'a [u8], &'a [u8])>, RequestExecutionError> {
    if args.len() <= fields_index + 3 {
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
    let values_len = field_count
        .checked_mul(2)
        .ok_or(RequestExecutionError::ValueOutOfRange)?;
    let first_value_index = fields_index + 2;
    let last_value_index = first_value_index
        .checked_add(values_len)
        .ok_or(RequestExecutionError::ValueOutOfRange)?;
    if args.len() != last_value_index {
        return Err(RequestExecutionError::WrongArity { command, expected });
    }

    let mut out = Vec::with_capacity(field_count);
    let mut index = first_value_index;
    while index < last_value_index {
        out.push((args[index], args[index + 1]));
        index += 2;
    }
    Ok(out)
}

fn append_integer_array(response_out: &mut Vec<u8>, values: &[i64]) {
    response_out.push(b'*');
    response_out.extend_from_slice(values.len().to_string().as_bytes());
    response_out.extend_from_slice(b"\r\n");
    for value in values {
        append_integer(response_out, *value);
    }
}

fn append_hash_integer_array(response_out: &mut Vec<u8>, len: usize, value: i64) {
    response_out.push(b'*');
    response_out.extend_from_slice(len.to_string().as_bytes());
    response_out.extend_from_slice(b"\r\n");
    for _ in 0..len {
        append_integer(response_out, value);
    }
}
