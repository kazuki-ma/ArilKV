use super::*;

impl RequestProcessor {
    pub(super) fn handle_hset(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_paired_arity_after(args, 4, 2, "HSET", "HSET key field value [field value ...]")?;

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        let mut hash = self.load_hash_object(&key)?.unwrap_or_default();
        let mut inserted = 0i64;

        let mut index = 2usize;
        while index < args.len() {
            // SAFETY: caller guarantees argument backing memory validity.
            let field = unsafe { args[index].as_slice() }.to_vec();
            // SAFETY: caller guarantees argument backing memory validity.
            let value = unsafe { args[index + 1].as_slice() }.to_vec();
            if hash.insert(field, value).is_none() {
                inserted += 1;
            }
            index += 2;
        }

        self.save_hash_object(&key, &hash)?;
        append_integer(response_out, inserted);
        Ok(())
    }

    pub(super) fn handle_hget(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 3, "HGET", "HGET key field")?;

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        // SAFETY: caller guarantees argument backing memory validity.
        let field = unsafe { args[2].as_slice() };
        let hash = match self.load_hash_object(&key)? {
            Some(hash) => hash,
            None => {
                append_null_bulk_string(response_out);
                return Ok(());
            }
        };

        match hash.get(field) {
            Some(value) => append_bulk_string(response_out, value),
            None => append_null_bulk_string(response_out),
        }
        Ok(())
    }

    pub(super) fn handle_hdel(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 3, "HDEL", "HDEL key field [field ...]")?;

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        let mut hash = match self.load_hash_object(&key)? {
            Some(hash) => hash,
            None => {
                append_integer(response_out, 0);
                return Ok(());
            }
        };

        let mut removed = 0i64;
        for field in &args[2..] {
            // SAFETY: caller guarantees argument backing memory validity.
            if hash.remove(unsafe { field.as_slice() }).is_some() {
                removed += 1;
            }
        }

        if hash.is_empty() {
            let _ = self.object_delete(&key)?;
        } else {
            self.save_hash_object(&key, &hash)?;
        }
        append_integer(response_out, removed);
        Ok(())
    }

    pub(super) fn handle_hgetall(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 2, "HGETALL", "HGETALL key")?;

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        let hash = match self.load_hash_object(&key)? {
            Some(hash) => hash,
            None => {
                response_out.extend_from_slice(b"*0\r\n");
                return Ok(());
            }
        };

        let pair_count = hash.len().saturating_mul(2);
        response_out.push(b'*');
        response_out.extend_from_slice(pair_count.to_string().as_bytes());
        response_out.extend_from_slice(b"\r\n");
        for (field, value) in &hash {
            append_bulk_string(response_out, field);
            append_bulk_string(response_out, value);
        }
        Ok(())
    }

    pub(super) fn handle_hlen(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 2, "HLEN", "HLEN key")?;

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        let len = self
            .load_hash_object(&key)?
            .map(|hash| hash.len() as i64)
            .unwrap_or(0);
        append_integer(response_out, len);
        Ok(())
    }

    pub(super) fn handle_hmget(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 3, "HMGET", "HMGET key field [field ...]")?;

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        let hash = self.load_hash_object(&key)?;
        let field_count = args.len() - 2;
        response_out.push(b'*');
        response_out.extend_from_slice(field_count.to_string().as_bytes());
        response_out.extend_from_slice(b"\r\n");
        for field in &args[2..] {
            // SAFETY: caller guarantees argument backing memory validity.
            let field = unsafe { field.as_slice() };
            match hash.as_ref().and_then(|hash| hash.get(field)) {
                Some(value) => append_bulk_string(response_out, value),
                None => append_null_bulk_string(response_out),
            }
        }
        Ok(())
    }

    pub(super) fn handle_hmset(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_paired_arity_after(
            args,
            4,
            2,
            "HMSET",
            "HMSET key field value [field value ...]",
        )?;

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        let mut hash = self.load_hash_object(&key)?.unwrap_or_default();
        let mut index = 2usize;
        while index < args.len() {
            // SAFETY: caller guarantees argument backing memory validity.
            let field = unsafe { args[index].as_slice() }.to_vec();
            // SAFETY: caller guarantees argument backing memory validity.
            let value = unsafe { args[index + 1].as_slice() }.to_vec();
            hash.insert(field, value);
            index += 2;
        }
        self.save_hash_object(&key, &hash)?;
        append_simple_string(response_out, b"OK");
        Ok(())
    }

    pub(super) fn handle_hsetnx(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 4, "HSETNX", "HSETNX key field value")?;

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        let mut hash = self.load_hash_object(&key)?.unwrap_or_default();
        // SAFETY: caller guarantees argument backing memory validity.
        let field = unsafe { args[2].as_slice() }.to_vec();
        if hash.contains_key(&field) {
            append_integer(response_out, 0);
            return Ok(());
        }
        // SAFETY: caller guarantees argument backing memory validity.
        let value = unsafe { args[3].as_slice() }.to_vec();
        hash.insert(field, value);
        self.save_hash_object(&key, &hash)?;
        append_integer(response_out, 1);
        Ok(())
    }

    pub(super) fn handle_hexists(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 3, "HEXISTS", "HEXISTS key field")?;

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        // SAFETY: caller guarantees argument backing memory validity.
        let field = unsafe { args[2].as_slice() };
        let exists = self
            .load_hash_object(&key)?
            .map(|hash| hash.contains_key(field))
            .unwrap_or(false);
        append_integer(response_out, i64::from(exists));
        Ok(())
    }

    pub(super) fn handle_hkeys(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 2, "HKEYS", "HKEYS key")?;

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
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
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 2, "HVALS", "HVALS key")?;

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
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
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 3, "HSTRLEN", "HSTRLEN key field")?;

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        // SAFETY: caller guarantees argument backing memory validity.
        let field = unsafe { args[2].as_slice() };
        let len = self
            .load_hash_object(&key)?
            .and_then(|hash| hash.get(field).map(|value| value.len() as i64))
            .unwrap_or(0);
        append_integer(response_out, len);
        Ok(())
    }

    pub(super) fn handle_hscan(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            3,
            "HSCAN",
            "HSCAN key cursor [MATCH pattern] [COUNT count]",
        )?;

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        // SAFETY: caller guarantees argument backing memory validity.
        let cursor = parse_u64_ascii(unsafe { args[2].as_slice() })
            .ok_or(RequestExecutionError::ValueNotInteger)?;
        let scan_options = parse_scan_match_count_options(args, 3)?;

        let Some(hash) = self.load_hash_object(&key)? else {
            append_hash_scan_response(response_out, cursor, scan_options.count, &[]);
            return Ok(());
        };

        let mut matched = Vec::new();
        for (field, value) in &hash {
            if let Some(pattern) = scan_options.pattern {
                if !super::server_commands::redis_glob_match(pattern, field, false, 0) {
                    continue;
                }
            }
            matched.push((field.as_slice(), value.as_slice()));
        }

        append_hash_scan_response(response_out, cursor, scan_options.count, &matched);
        Ok(())
    }

    pub(super) fn handle_hincrby(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 4, "HINCRBY", "HINCRBY key field increment")?;

        // SAFETY: caller guarantees argument backing memory validity.
        let increment = parse_i64_ascii(unsafe { args[3].as_slice() })
            .ok_or(RequestExecutionError::ValueNotInteger)?;
        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        // SAFETY: caller guarantees argument backing memory validity.
        let field = unsafe { args[2].as_slice() }.to_vec();
        let mut hash = self.load_hash_object(&key)?.unwrap_or_default();

        let current = match hash.get(&field) {
            Some(value) => parse_i64_ascii(value).ok_or(RequestExecutionError::ValueNotInteger)?,
            None => 0,
        };
        let updated = current
            .checked_add(increment)
            .ok_or(RequestExecutionError::IncrementOverflow)?;
        hash.insert(field, updated.to_string().into_bytes());
        self.save_hash_object(&key, &hash)?;
        append_integer(response_out, updated);
        Ok(())
    }

    pub(super) fn handle_hincrbyfloat(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 4, "HINCRBYFLOAT", "HINCRBYFLOAT key field increment")?;

        // SAFETY: caller guarantees argument backing memory validity.
        let increment = parse_f64_ascii(unsafe { args[3].as_slice() })
            .ok_or(RequestExecutionError::ValueNotFloat)?;
        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        // SAFETY: caller guarantees argument backing memory validity.
        let field = unsafe { args[2].as_slice() }.to_vec();
        let mut hash = self.load_hash_object(&key)?.unwrap_or_default();

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
        append_bulk_string(response_out, &updated_text);
        Ok(())
    }

    pub(super) fn handle_hrandfield(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_ranged_arity(
            args,
            2,
            4,
            "HRANDFIELD",
            "HRANDFIELD key [count [WITHVALUES]]",
        )?;

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        let hash = self.load_hash_object(&key)?;
        let resp3 = self.resp_protocol_version() == 3;

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

        // SAFETY: caller guarantees argument backing memory validity.
        let count_bytes = unsafe { args[2].as_slice() };
        let count = parse_i64_ascii(count_bytes).ok_or_else(|| {
            if looks_like_signed_integer(count_bytes) {
                RequestExecutionError::ValueOutOfRange
            } else {
                RequestExecutionError::ValueNotInteger
            }
        })?;
        let with_values = if args.len() == 4 {
            // SAFETY: caller guarantees argument backing memory validity.
            let option = unsafe { args[3].as_slice() };
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
}

fn append_hash_scan_response(
    response_out: &mut Vec<u8>,
    cursor: u64,
    count: usize,
    pairs: &[(&[u8], &[u8])],
) {
    let start = usize::try_from(cursor)
        .unwrap_or(usize::MAX)
        .min(pairs.len());
    let end = start.saturating_add(count).min(pairs.len());
    let next_cursor = if end >= pairs.len() { 0 } else { end };

    response_out.push(b'*');
    response_out.extend_from_slice(b"2\r\n");
    let next_cursor_bytes = next_cursor.to_string();
    append_bulk_string(response_out, next_cursor_bytes.as_bytes());
    response_out.push(b'*');
    response_out.extend_from_slice(((end - start) * 2).to_string().as_bytes());
    response_out.extend_from_slice(b"\r\n");
    for (field, value) in &pairs[start..end] {
        append_bulk_string(response_out, field);
        append_bulk_string(response_out, value);
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
