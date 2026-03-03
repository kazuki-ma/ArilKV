use super::*;

impl RequestProcessor {
    pub(super) fn handle_sadd(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 3, "SADD", "SADD key member [member ...]")?;

        let key = RedisKey::from(args[1]);
        let mut inserted = 0i64;

        match self.load_set_object_payload(&key)? {
            None => {
                if let Some((range, inserted_count)) = try_build_contiguous_i64_range(&args[2..]) {
                    self.save_contiguous_i64_range_set_object(&key, range)?;
                    inserted = inserted_count as i64;
                } else {
                    let mut set = BTreeSet::new();
                    for member in &args[2..] {
                        if set.insert((*member).to_vec()) {
                            inserted += 1;
                        }
                    }
                    self.save_set_object(&key, &set)?;
                }
            }
            Some(DecodedSetObjectPayload::Members(mut set)) => {
                for member in &args[2..] {
                    if set.insert((*member).to_vec()) {
                        inserted += 1;
                    }
                }
                self.save_set_object(&key, &set)?;
            }
            Some(DecodedSetObjectPayload::ContiguousI64Range(mut range)) => {
                let mut fallback_set: Option<BTreeSet<Vec<u8>>> = None;

                for member in &args[2..] {
                    if let Some(set) = fallback_set.as_mut() {
                        if set.insert((*member).to_vec()) {
                            inserted += 1;
                        }
                        continue;
                    }

                    let Some(value) = parse_canonical_i64_member(member) else {
                        let mut set = materialize_contiguous_i64_range_set(range);
                        if set.insert((*member).to_vec()) {
                            inserted += 1;
                        }
                        fallback_set = Some(set);
                        continue;
                    };

                    if range.contains(value) {
                        continue;
                    }
                    if range.can_extend_left_with(value) {
                        range.extend_left();
                        inserted += 1;
                        continue;
                    }
                    if range.can_extend_right_with(value) {
                        range.extend_right();
                        inserted += 1;
                        continue;
                    }

                    let mut set = materialize_contiguous_i64_range_set(range);
                    if set.insert((*member).to_vec()) {
                        inserted += 1;
                    }
                    fallback_set = Some(set);
                }

                if let Some(set) = fallback_set {
                    self.save_set_object(&key, &set)?;
                } else {
                    self.save_contiguous_i64_range_set_object(&key, range)?;
                }
            }
        }

        if inserted > 0 {
            self.notify_keyspace_event(NOTIFY_SET, b"sadd", &key);
        }
        append_integer(response_out, inserted);
        Ok(())
    }

    pub(super) fn handle_srem(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 3, "SREM", "SREM key member [member ...]")?;

        let key = RedisKey::from(args[1]);
        let mut set = match self.load_set_object(&key)? {
            Some(set) => set,
            None => {
                append_integer(response_out, 0);
                return Ok(());
            }
        };
        let mut removed = 0i64;
        for member in &args[2..] {
            if set.remove(*member) {
                removed += 1;
            }
        }

        if removed > 0 {
            self.notify_keyspace_event(NOTIFY_SET, b"srem", &key);
        }
        if set.is_empty() {
            let _ = self.object_delete(&key)?;
            if removed > 0 {
                self.notify_keyspace_event(NOTIFY_GENERIC, b"del", &key);
            }
        } else {
            self.save_set_object(&key, &set)?;
        }
        append_integer(response_out, removed);
        Ok(())
    }

    pub(super) fn handle_smembers(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 2, "SMEMBERS", "SMEMBERS key")?;

        let key = RedisKey::from(args[1]);
        let resp3 = self.resp_protocol_version().is_resp3();
        let set = match self.load_set_object(&key)? {
            Some(set) => set,
            None => {
                if resp3 {
                    response_out.extend_from_slice(b"~0\r\n");
                } else {
                    response_out.extend_from_slice(b"*0\r\n");
                }
                return Ok(());
            }
        };

        if resp3 {
            append_set_length(response_out, set.len());
        } else {
            response_out.push(b'*');
            response_out.extend_from_slice(set.len().to_string().as_bytes());
            response_out.extend_from_slice(b"\r\n");
        }
        for member in &set {
            append_bulk_string(response_out, member);
        }
        Ok(())
    }

    pub(super) fn handle_sismember(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 3, "SISMEMBER", "SISMEMBER key member")?;

        let key = RedisKey::from(args[1]);
        let member = args[2];
        let set = match self.load_set_object(&key)? {
            Some(set) => set,
            None => {
                append_integer(response_out, 0);
                return Ok(());
            }
        };
        append_integer(response_out, if set.contains(member) { 1 } else { 0 });
        Ok(())
    }

    pub(super) fn handle_scard(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 2, "SCARD", "SCARD key")?;

        let key = RedisKey::from(args[1]);
        let len = self
            .load_set_object(&key)?
            .map_or(0, |set| set.len() as i64);
        append_integer(response_out, len);
        Ok(())
    }

    pub(super) fn handle_sscan(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            3,
            "SSCAN",
            "SSCAN key cursor [MATCH pattern] [COUNT count]",
        )?;

        let key = RedisKey::from(args[1]);
        let cursor = parse_u64_ascii(args[2]).ok_or(RequestExecutionError::ValueNotInteger)?;
        let scan_options = parse_scan_match_count_options(args, 3)?;
        let resp3 = self.resp_protocol_version().is_resp3();

        let Some(set) = self.load_set_object(&key)? else {
            append_set_scan_response(response_out, cursor, scan_options.count, &[], resp3);
            return Ok(());
        };

        let mut matched = Vec::new();
        for member in &set {
            if let Some(pattern) = scan_options.pattern
                && !super::server_commands::redis_glob_match(
                    pattern,
                    member,
                    super::server_commands::CaseSensitivity::Sensitive,
                    0,
                )
            {
                continue;
            }
            matched.push(member.as_slice());
        }

        append_set_scan_response(response_out, cursor, scan_options.count, &matched, resp3);
        Ok(())
    }

    pub(super) fn handle_smismember(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 3, "SMISMEMBER", "SMISMEMBER key member [member ...]")?;

        let key = RedisKey::from(args[1]);
        let set = self.load_set_object(&key)?;
        let members = &args[2..];
        response_out.push(b'*');
        response_out.extend_from_slice(members.len().to_string().as_bytes());
        response_out.extend_from_slice(b"\r\n");
        for member in members {
            let exists = set.as_ref().is_some_and(|set| set.contains(*member));
            append_integer(response_out, if exists { 1 } else { 0 });
        }
        Ok(())
    }

    pub(super) fn handle_srandmember(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_ranged_arity(args, 2, 3, "SRANDMEMBER", "SRANDMEMBER key [count]")?;

        let key = RedisKey::from(args[1]);
        let resp3 = self.resp_protocol_version().is_resp3();
        let set = self.load_set_object(&key)?;
        if args.len() == 2 {
            let Some(set) = set else {
                if resp3 {
                    append_null(response_out);
                } else {
                    append_null_bulk_string(response_out);
                }
                return Ok(());
            };
            if set.is_empty() {
                if resp3 {
                    append_null(response_out);
                } else {
                    append_null_bulk_string(response_out);
                }
                return Ok(());
            }
            let members = select_random_members_distinct(self, &set, 1);
            append_bulk_string(response_out, &members[0]);
            return Ok(());
        }

        let count = parse_i64_ascii(args[2]).ok_or(RequestExecutionError::ValueNotInteger)?;
        // Positive count: distinct sampling → set type in RESP3.
        // Negative count: with-replacement → array type (may have duplicates).
        let use_set_type = resp3 && count > 0;
        let Some(set) = set else {
            if use_set_type {
                append_set_length(response_out, 0);
            } else {
                append_array_length(response_out, 0);
            }
            return Ok(());
        };
        if set.is_empty() || count == 0 {
            append_array_length(response_out, 0);
            return Ok(());
        }

        let sampled = if count > 0 {
            let requested = usize::try_from(count).unwrap_or(usize::MAX);
            select_random_members_distinct(self, &set, requested)
        } else {
            let requested = usize::try_from(count.unsigned_abs())
                .map_err(|_| RequestExecutionError::ValueOutOfRange)?;
            select_random_members_with_replacement(self, &set, requested)
        };
        if use_set_type {
            append_set_length(response_out, sampled.len());
        } else {
            append_array_length(response_out, sampled.len());
        }
        for member in sampled {
            append_bulk_string(response_out, &member);
        }
        Ok(())
    }

    pub(super) fn handle_spop(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_ranged_arity(args, 2, 3, "SPOP", "SPOP key [count]")?;

        let key = RedisKey::from(args[1]);
        let resp3 = self.resp_protocol_version().is_resp3();
        let mut set = self.load_set_object(&key)?;
        if args.len() == 2 {
            let Some(mut set) = set.take() else {
                if resp3 {
                    append_null(response_out);
                } else {
                    append_null_bulk_string(response_out);
                }
                return Ok(());
            };
            if set.is_empty() {
                let _ = self.object_delete(&key)?;
                if resp3 {
                    append_null(response_out);
                } else {
                    append_null_bulk_string(response_out);
                }
                return Ok(());
            }
            let selected = select_random_members_distinct(self, &set, 1);
            let member = selected[0].clone();
            let removed = set.remove(&member);
            debug_assert!(removed, "selected member must exist in set");
            self.notify_keyspace_event(NOTIFY_SET, b"spop", &key);
            if set.is_empty() {
                let _ = self.object_delete(&key)?;
                self.notify_keyspace_event(NOTIFY_GENERIC, b"del", &key);
            } else {
                self.save_set_object(&key, &set)?;
            }
            append_bulk_string(response_out, &member);
            return Ok(());
        }

        let count = parse_i64_ascii(args[2]).ok_or(RequestExecutionError::ValueNotInteger)?;
        if count < 0 {
            return Err(RequestExecutionError::ValueOutOfRange);
        }
        let count = usize::try_from(count).map_err(|_| RequestExecutionError::ValueOutOfRange)?;
        let Some(mut set) = set else {
            if resp3 {
                append_set_length(response_out, 0);
            } else {
                append_array_length(response_out, 0);
            }
            return Ok(());
        };
        if set.is_empty() || count == 0 {
            if resp3 {
                append_set_length(response_out, 0);
            } else {
                append_array_length(response_out, 0);
            }
            return Ok(());
        }

        let selected = select_random_members_distinct(self, &set, count);
        for member in &selected {
            set.remove(member);
        }
        if !selected.is_empty() {
            self.notify_keyspace_event(NOTIFY_SET, b"spop", &key);
        }
        if set.is_empty() {
            let _ = self.object_delete(&key)?;
            if !selected.is_empty() {
                self.notify_keyspace_event(NOTIFY_GENERIC, b"del", &key);
            }
        } else {
            self.save_set_object(&key, &set)?;
        }

        if resp3 {
            append_set_length(response_out, selected.len());
        } else {
            append_array_length(response_out, selected.len());
        }
        for member in selected {
            append_bulk_string(response_out, &member);
        }
        Ok(())
    }

    pub(super) fn handle_smove(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 4, "SMOVE", "SMOVE source destination member")?;

        let source = RedisKey::from(args[1]);
        let destination = RedisKey::from(args[2]);
        let member = args[3].to_vec();

        let mut source_set = match self.load_set_object(&source)? {
            Some(set) => set,
            None => {
                append_integer(response_out, 0);
                return Ok(());
            }
        };
        if !source_set.contains(&member) {
            append_integer(response_out, 0);
            return Ok(());
        }
        if source == destination {
            append_integer(response_out, 1);
            return Ok(());
        }

        source_set.remove(&member);
        let mut destination_set = self.load_set_object(&destination)?.unwrap_or_default();
        destination_set.insert(member);

        if source_set.is_empty() {
            let _ = self.object_delete(&source)?;
        } else {
            self.save_set_object(&source, &source_set)?;
        }
        self.save_set_object(&destination, &destination_set)?;
        append_integer(response_out, 1);
        Ok(())
    }

    pub(super) fn handle_sunion(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 2, "SUNION", "SUNION key [key ...]")?;
        let keys = collect_set_keys(args, 1);
        let union = compute_sunion(self, &keys)?;
        let resp3 = self.resp_protocol_version().is_resp3();
        append_set_members(response_out, &union, resp3);
        Ok(())
    }

    pub(super) fn handle_sinter(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 2, "SINTER", "SINTER key [key ...]")?;
        let keys = collect_set_keys(args, 1);
        let inter = compute_sinter(self, &keys)?;
        let resp3 = self.resp_protocol_version().is_resp3();
        append_set_members(response_out, &inter, resp3);
        Ok(())
    }

    pub(super) fn handle_sintercard(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            3,
            "SINTERCARD",
            "SINTERCARD numkeys key [key ...] [LIMIT limit]",
        )?;
        let num_keys = parse_u64_ascii(args[1]).ok_or(RequestExecutionError::ValueNotInteger)?;
        if num_keys == 0 {
            return Err(RequestExecutionError::SyntaxError);
        }
        let num_keys =
            usize::try_from(num_keys).map_err(|_| RequestExecutionError::ValueOutOfRange)?;
        let key_start = 2usize;
        let key_end = key_start + num_keys;
        if args.len() < key_end {
            return Err(RequestExecutionError::SyntaxError);
        }

        let mut limit = 0usize;
        if args.len() > key_end {
            if args.len() != key_end + 2 {
                return Err(RequestExecutionError::SyntaxError);
            }
            let option = args[key_end];
            if !ascii_eq_ignore_case(option, b"LIMIT") {
                return Err(RequestExecutionError::SyntaxError);
            }
            let parsed_limit =
                parse_u64_ascii(args[key_end + 1]).ok_or(RequestExecutionError::ValueNotInteger)?;
            limit = usize::try_from(parsed_limit)
                .map_err(|_| RequestExecutionError::ValueOutOfRange)?;
        }

        let keys: Vec<Vec<u8>> = args[key_start..key_end]
            .iter()
            .map(|key| key.to_vec())
            .collect();
        let inter = compute_sinter(self, &keys)?;
        let mut cardinality = inter.len();
        if limit > 0 {
            cardinality = cardinality.min(limit);
        }
        append_integer(response_out, cardinality as i64);
        Ok(())
    }

    pub(super) fn handle_sdiff(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 2, "SDIFF", "SDIFF key [key ...]")?;
        let keys = collect_set_keys(args, 1);
        let diff = compute_sdiff(self, &keys)?;
        let resp3 = self.resp_protocol_version().is_resp3();
        append_set_members(response_out, &diff, resp3);
        Ok(())
    }

    pub(super) fn handle_sunionstore(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            3,
            "SUNIONSTORE",
            "SUNIONSTORE destination key [key ...]",
        )?;
        let destination = RedisKey::from(args[1]);
        let keys = collect_set_keys(args, 2);
        let union = compute_sunion(self, &keys)?;
        store_set_result(self, &destination, &union)?;
        append_integer(response_out, union.len() as i64);
        Ok(())
    }

    pub(super) fn handle_sinterstore(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            3,
            "SINTERSTORE",
            "SINTERSTORE destination key [key ...]",
        )?;
        let destination = RedisKey::from(args[1]);
        let keys = collect_set_keys(args, 2);
        let inter = compute_sinter(self, &keys)?;
        store_set_result(self, &destination, &inter)?;
        append_integer(response_out, inter.len() as i64);
        Ok(())
    }

    pub(super) fn handle_sdiffstore(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            3,
            "SDIFFSTORE",
            "SDIFFSTORE destination key [key ...]",
        )?;
        let destination = RedisKey::from(args[1]);
        let keys = collect_set_keys(args, 2);
        let diff = compute_sdiff(self, &keys)?;
        store_set_result(self, &destination, &diff)?;
        append_integer(response_out, diff.len() as i64);
        Ok(())
    }
}

fn select_random_members_distinct(
    processor: &RequestProcessor,
    set: &BTreeSet<Vec<u8>>,
    count: usize,
) -> Vec<Vec<u8>> {
    let mut members: Vec<Vec<u8>> = set.iter().cloned().collect();
    if members.len() <= 1 {
        members.truncate(count.min(members.len()));
        return members;
    }
    for index in (1..members.len()).rev() {
        let random_index = (processor.next_random_u64() as usize) % (index + 1);
        members.swap(index, random_index);
    }
    members.truncate(count.min(members.len()));
    members
}

fn select_random_members_with_replacement(
    processor: &RequestProcessor,
    set: &BTreeSet<Vec<u8>>,
    count: usize,
) -> Vec<Vec<u8>> {
    let members: Vec<Vec<u8>> = set.iter().cloned().collect();
    if members.is_empty() || count == 0 {
        return Vec::new();
    }
    let mut selected = Vec::with_capacity(count);
    for _ in 0..count {
        let index = (processor.next_random_u64() as usize) % members.len();
        selected.push(members[index].clone());
    }
    selected
}

fn collect_set_keys(args: &[&[u8]], first_key_index: usize) -> Vec<Vec<u8>> {
    args[first_key_index..]
        .iter()
        .map(|key| key.to_vec())
        .collect()
}

fn append_set_scan_response(
    response_out: &mut Vec<u8>,
    cursor: u64,
    count: usize,
    values: &[&[u8]],
    resp3: bool,
) {
    // When the collection shrinks between iterations (elements removed), the
    // cursor may point past the new end.  Wrap around to 0 so elements that
    // shifted are not silently skipped — this may produce duplicates but
    // guarantees completeness, matching Redis SCAN semantics.
    let raw_start = usize::try_from(cursor).unwrap_or(usize::MAX);
    let start = if raw_start >= values.len() {
        0
    } else {
        raw_start
    };
    let end = start.saturating_add(count).min(values.len());
    let next_cursor = if end >= values.len() { 0 } else { end };

    response_out.extend_from_slice(b"*2\r\n");
    let next_cursor_bytes = next_cursor.to_string();
    append_bulk_string(response_out, next_cursor_bytes.as_bytes());

    // In RESP3, emit set members as set type.
    let page_len = end - start;
    if resp3 {
        append_set_length(response_out, page_len);
    } else {
        append_array_length(response_out, page_len);
    }
    for value in &values[start..end] {
        append_bulk_string(response_out, value);
    }
}

fn append_set_members(response_out: &mut Vec<u8>, set: &BTreeSet<Vec<u8>>, resp3: bool) {
    if resp3 {
        append_set_length(response_out, set.len());
    } else {
        response_out.push(b'*');
        response_out.extend_from_slice(set.len().to_string().as_bytes());
        response_out.extend_from_slice(b"\r\n");
    }
    for member in set {
        append_bulk_string(response_out, member);
    }
}

fn compute_sunion(
    processor: &RequestProcessor,
    keys: &[Vec<u8>],
) -> Result<BTreeSet<Vec<u8>>, RequestExecutionError> {
    let mut union = BTreeSet::new();
    for key in keys {
        if let Some(set) = processor.load_set_object(key)? {
            union.extend(set);
        }
    }
    Ok(union)
}

fn compute_sinter(
    processor: &RequestProcessor,
    keys: &[Vec<u8>],
) -> Result<BTreeSet<Vec<u8>>, RequestExecutionError> {
    let Some((first_key, remaining_keys)) = keys.split_first() else {
        return Ok(BTreeSet::new());
    };
    let mut inter = match processor.load_set_object(first_key)? {
        Some(set) => set,
        None => return Ok(BTreeSet::new()),
    };

    for key in remaining_keys {
        let Some(other_set) = processor.load_set_object(key)? else {
            inter.clear();
            break;
        };
        inter.retain(|member| other_set.contains(member));
        if inter.is_empty() {
            break;
        }
    }

    Ok(inter)
}

fn compute_sdiff(
    processor: &RequestProcessor,
    keys: &[Vec<u8>],
) -> Result<BTreeSet<Vec<u8>>, RequestExecutionError> {
    let Some((first_key, remaining_keys)) = keys.split_first() else {
        return Ok(BTreeSet::new());
    };
    let mut diff = match processor.load_set_object(first_key)? {
        Some(set) => set,
        None => return Ok(BTreeSet::new()),
    };

    for key in remaining_keys {
        let Some(other_set) = processor.load_set_object(key)? else {
            continue;
        };
        diff.retain(|member| !other_set.contains(member));
        if diff.is_empty() {
            break;
        }
    }

    Ok(diff)
}

fn try_build_contiguous_i64_range(members: &[&[u8]]) -> Option<(ContiguousI64RangeSet, usize)> {
    let mut unique_values = BTreeSet::new();
    for member in members {
        unique_values.insert(parse_canonical_i64_member(member)?);
    }
    let start = *unique_values.first()?;
    let end = *unique_values.last()?;
    let span = i128::from(end) - i128::from(start) + 1;
    let unique_len = unique_values.len();
    if usize::try_from(span).ok()? != unique_len {
        return None;
    }
    Some((ContiguousI64RangeSet::new(start, end)?, unique_len))
}

fn parse_canonical_i64_member(member: &[u8]) -> Option<i64> {
    let value = parse_i64_ascii(member)?;
    if value.to_string().as_bytes() != member {
        return None;
    }
    Some(value)
}

fn store_set_result(
    processor: &RequestProcessor,
    destination: &[u8],
    result_set: &BTreeSet<Vec<u8>>,
) -> Result<(), RequestExecutionError> {
    processor.expire_key_if_needed(destination)?;
    let destination_had_string = processor.key_exists(destination)?;
    let string_deleted = if destination_had_string {
        delete_string_value_for_object_overwrite(processor, destination)?
    } else {
        false
    };

    if result_set.is_empty() {
        let object_deleted = processor.object_delete(destination)?;
        if string_deleted && !object_deleted {
            processor.bump_watch_version(destination);
        }
        return Ok(());
    }

    processor.save_set_object(destination, result_set)
}

fn delete_string_value_for_object_overwrite(
    processor: &RequestProcessor,
    key: &[u8],
) -> Result<bool, RequestExecutionError> {
    let key_vec = key.to_vec();
    let mut store = processor.lock_string_store_for_key(key);
    let mut session = store.session(&processor.functions);
    let mut info = DeleteInfo::default();
    let status = session
        .delete(&key_vec, &mut info)
        .map_err(map_delete_error)?;
    match status {
        DeleteOperationStatus::TombstonedInPlace | DeleteOperationStatus::AppendedTombstone => {
            processor.remove_string_key_metadata(key);
            Ok(true)
        }
        DeleteOperationStatus::NotFound => {
            processor.remove_string_key_metadata(key);
            Ok(false)
        }
        DeleteOperationStatus::RetryLater => Err(RequestExecutionError::StorageBusy),
    }
}
