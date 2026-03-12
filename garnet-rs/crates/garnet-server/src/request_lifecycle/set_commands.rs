use super::*;

impl RequestProcessor {
    pub(super) fn handle_sadd(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 3, "SADD", "SADD key member [member ...]")?;

        let key = RedisKey::from(args[1]);
        let inserted = self.with_set_hot_entry(
            DbKeyRef::new(current_request_selected_db(), &key),
            |entry| {
                if entry.is_none() {
                    if let Some((range, inserted_count)) =
                        try_build_contiguous_i64_range(&args[2..])
                    {
                        let mut new_entry = SetObjectHotEntry::new(
                            DecodedSetObjectPayload::ContiguousI64Range(range),
                            false,
                        );
                        self.mark_set_hot_entry_dirty(
                            DbKeyRef::new(current_request_selected_db(), &key),
                            &mut new_entry,
                            false,
                        );
                        *entry = Some(new_entry);
                        return Ok(inserted_count as i64);
                    }
                    *entry = Some(SetObjectHotEntry::new(
                        DecodedSetObjectPayload::Members(BTreeSet::new()),
                        false,
                    ));
                }

                let entry = entry.as_mut().unwrap();
                let mut inserted = 0i64;
                for member in &args[2..] {
                    if entry.payload.insert_member(member) {
                        inserted += 1;
                    }
                }
                if inserted > 0 {
                    self.mark_set_hot_entry_dirty(
                        DbKeyRef::new(current_request_selected_db(), &key),
                        entry,
                        false,
                    );
                }
                Ok(inserted)
            },
        )?;

        if inserted > 0 {
            self.notify_keyspace_event(current_request_selected_db(), NOTIFY_SET, b"sadd", &key);
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
        let (removed, delete_key) = self.with_set_hot_entry(
            DbKeyRef::new(current_request_selected_db(), &key),
            |entry| {
                let Some(current_entry) = entry.as_mut() else {
                    return Ok((0i64, false));
                };

                let mut removed = 0i64;
                for member in &args[2..] {
                    if current_entry.payload.remove_member(member) {
                        removed += 1;
                    }
                }
                if removed == 0 {
                    return Ok((0i64, false));
                }

                if current_entry.payload.is_empty() {
                    *entry = None;
                    return Ok((removed, true));
                }

                self.mark_set_hot_entry_dirty(
                    DbKeyRef::new(current_request_selected_db(), &key),
                    current_entry,
                    false,
                );
                Ok((removed, false))
            },
        )?;

        if removed > 0 {
            self.notify_keyspace_event(current_request_selected_db(), NOTIFY_SET, b"srem", &key);
        }
        if delete_key {
            let _ = self.object_delete(DbKeyRef::new(current_request_selected_db(), &key))?;
            if removed > 0 {
                self.notify_keyspace_event(
                    current_request_selected_db(),
                    NOTIFY_GENERIC,
                    b"del",
                    &key,
                );
            }
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
        let set = match self.load_set_object(DbKeyRef::new(current_request_selected_db(), &key))? {
            Some(set) => set,
            None => {
                if resp3 {
                    append_set_length(response_out, 0);
                } else {
                    append_array_length(response_out, 0);
                }
                return Ok(());
            }
        };
        self.record_set_debug_ht_activity(
            DbKeyRef::new(current_request_selected_db(), &key),
            set.len(),
        );

        if resp3 {
            append_set_length(response_out, set.len());
        } else {
            append_array_length(response_out, set.len());
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
        let set = match self.load_set_object(DbKeyRef::new(current_request_selected_db(), &key))? {
            Some(set) => set,
            None => {
                append_integer(response_out, 0);
                return Ok(());
            }
        };
        self.record_set_debug_ht_activity(
            DbKeyRef::new(current_request_selected_db(), &key),
            set.len(),
        );
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
        let len = self.with_set_hot_entry(
            DbKeyRef::new(current_request_selected_db(), &key),
            |entry| {
                Ok(entry
                    .as_ref()
                    .map_or(0, |entry| entry.payload.member_count() as i64))
            },
        )?;
        if len > 0 {
            self.record_set_debug_ht_activity(
                DbKeyRef::new(current_request_selected_db(), &key),
                len as usize,
            );
        }
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

        if scan_options.pattern.is_none() {
            let (member_count, next_cursor, page) = self.with_set_hot_entry(
                DbKeyRef::new(current_request_selected_db(), &key),
                |entry| {
                    let Some(entry) = entry.as_mut() else {
                        return Ok((0usize, 0u64, Vec::new()));
                    };
                    let values = entry.ordered_members();
                    let raw_start = usize::try_from(cursor).unwrap_or(usize::MAX);
                    let start = if raw_start >= values.len() {
                        0
                    } else {
                        raw_start
                    };
                    let end = start.saturating_add(scan_options.count).min(values.len());
                    let next_cursor = if end >= values.len() { 0 } else { end as u64 };
                    Ok((values.len(), next_cursor, values[start..end].to_vec()))
                },
            )?;
            if member_count > 0 {
                self.record_set_debug_ht_activity(
                    DbKeyRef::new(current_request_selected_db(), &key),
                    member_count,
                );
            }

            append_array_length(response_out, 2);
            append_bulk_string(response_out, next_cursor.to_string().as_bytes());
            if resp3 {
                append_set_length(response_out, page.len());
            } else {
                append_array_length(response_out, page.len());
            }
            for member in &page {
                append_bulk_string(response_out, member);
            }
            return Ok(());
        }

        let Some(set) = self.load_set_object(DbKeyRef::new(current_request_selected_db(), &key))?
        else {
            append_set_scan_response(response_out, cursor, scan_options.count, &[], resp3);
            return Ok(());
        };
        self.record_set_debug_ht_activity(
            DbKeyRef::new(current_request_selected_db(), &key),
            set.len(),
        );

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
        let set = self.load_set_object(DbKeyRef::new(current_request_selected_db(), &key))?;
        if let Some(set) = &set {
            self.record_set_debug_ht_activity(
                DbKeyRef::new(current_request_selected_db(), &key),
                set.len(),
            );
        }
        let members = &args[2..];
        append_array_length(response_out, members.len());
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
        if args.len() == 2 {
            let sampled = self.with_set_hot_entry(
                DbKeyRef::new(current_request_selected_db(), &key),
                |entry| {
                    let Some(entry) = entry.as_mut() else {
                        return Ok(None);
                    };
                    let members = entry.ordered_members();
                    if members.is_empty() {
                        return Ok(None);
                    }
                    let random_index = (self.next_random_u64() as usize) % members.len();
                    Ok(Some((members.len(), members[random_index].clone())))
                },
            )?;
            let Some((member_count, member)) = sampled else {
                if resp3 {
                    append_null(response_out);
                } else {
                    append_null_bulk_string(response_out);
                }
                return Ok(());
            };
            self.record_set_debug_ht_activity(
                DbKeyRef::new(current_request_selected_db(), &key),
                member_count,
            );
            append_bulk_string(response_out, &member);
            return Ok(());
        }

        let count = parse_i64_ascii(args[2]).ok_or(RequestExecutionError::ValueNotInteger)?;
        // Positive count: distinct sampling → set type in RESP3.
        // Negative count: with-replacement → array type (may have duplicates).
        let use_set_type = resp3 && count > 0;
        let sampled = self.with_set_hot_entry(
            DbKeyRef::new(current_request_selected_db(), &key),
            |entry| {
                let Some(entry) = entry.as_mut() else {
                    return Ok((0usize, Vec::new()));
                };
                let members = entry.ordered_members();
                if members.is_empty() || count == 0 {
                    return Ok((members.len(), Vec::new()));
                }
                if count > 0 {
                    let requested = usize::try_from(count)
                        .map_err(|_| RequestExecutionError::ValueOutOfRange)?;
                    let sampled = if requested == 1 {
                        let random_index = (self.next_random_u64() as usize) % members.len();
                        vec![members[random_index].clone()]
                    } else {
                        select_random_members_distinct_from_ordered(self, members, requested)
                    };
                    return Ok((members.len(), sampled));
                }

                let requested = count
                    .checked_abs()
                    .ok_or(RequestExecutionError::ValueOutOfRange)
                    .and_then(|value| {
                        usize::try_from(value).map_err(|_| RequestExecutionError::ValueOutOfRange)
                    })?;
                Ok((
                    members.len(),
                    sample_random_members_with_replacement_from_ordered(self, members, requested),
                ))
            },
        )?;
        let (member_count, sampled) = sampled;
        if member_count == 0 {
            if use_set_type {
                append_set_length(response_out, 0);
            } else {
                append_array_length(response_out, 0);
            }
            return Ok(());
        }
        self.record_set_debug_ht_activity(
            DbKeyRef::new(current_request_selected_db(), &key),
            member_count,
        );
        if count < 0 {
            append_array_length(response_out, sampled.len());
            for member in sampled {
                append_bulk_string(response_out, &member);
            }
            return Ok(());
        }
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
        let mut set = self.load_set_object(DbKeyRef::new(current_request_selected_db(), &key))?;
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
                let _ = self.object_delete(DbKeyRef::new(current_request_selected_db(), &key))?;
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
            self.notify_keyspace_event(current_request_selected_db(), NOTIFY_SET, b"spop", &key);
            if set.is_empty() {
                let _ = self.object_delete(DbKeyRef::new(current_request_selected_db(), &key))?;
                self.notify_keyspace_event(
                    current_request_selected_db(),
                    NOTIFY_GENERIC,
                    b"del",
                    &key,
                );
            } else {
                self.save_set_object(DbKeyRef::new(current_request_selected_db(), &key), &set)?;
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
            self.notify_keyspace_event(current_request_selected_db(), NOTIFY_SET, b"spop", &key);
        }
        if set.is_empty() {
            let _ = self.object_delete(DbKeyRef::new(current_request_selected_db(), &key))?;
            if !selected.is_empty() {
                self.notify_keyspace_event(
                    current_request_selected_db(),
                    NOTIFY_GENERIC,
                    b"del",
                    &key,
                );
            }
        } else {
            self.save_set_object(DbKeyRef::new(current_request_selected_db(), &key), &set)?;
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

        let mut source_set =
            match self.load_set_object(DbKeyRef::new(current_request_selected_db(), &source))? {
                Some(set) => set,
                None => {
                    append_integer(response_out, 0);
                    return Ok(());
                }
            };
        if source == destination {
            if !source_set.contains(&member) {
                append_integer(response_out, 0);
                return Ok(());
            }
            append_integer(response_out, 1);
            return Ok(());
        }

        let mut destination_set = self
            .load_set_object(DbKeyRef::new(current_request_selected_db(), &destination))?
            .unwrap_or_default();
        if !source_set.contains(&member) {
            append_integer(response_out, 0);
            return Ok(());
        }

        source_set.remove(&member);
        let destination_changed = destination_set.insert(member);

        if source_set.is_empty() {
            let _ = self.object_delete(DbKeyRef::new(current_request_selected_db(), &source))?;
            self.notify_keyspace_event(current_request_selected_db(), NOTIFY_SET, b"srem", &source);
            self.notify_keyspace_event(
                current_request_selected_db(),
                NOTIFY_GENERIC,
                b"del",
                &source,
            );
        } else {
            self.save_set_object(
                DbKeyRef::new(current_request_selected_db(), &source),
                &source_set,
            )?;
            self.notify_keyspace_event(current_request_selected_db(), NOTIFY_SET, b"srem", &source);
        }
        if destination_changed {
            self.save_set_object(
                DbKeyRef::new(current_request_selected_db(), &destination),
                &destination_set,
            )?;
            self.notify_keyspace_event(
                current_request_selected_db(),
                NOTIFY_SET,
                b"sadd",
                &destination,
            );
        }
        append_integer(response_out, 1);
        Ok(())
    }

    pub(super) fn handle_sunion(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 2, "SUNION", "SUNION key [key ...]")?;
        let selected_db = current_request_selected_db();
        let keys = collect_set_keys(args, 1);
        let union = compute_sunion(self, selected_db, &keys)?;
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
        let selected_db = current_request_selected_db();
        let keys = collect_set_keys(args, 1);
        let inter = compute_sinter(self, selected_db, &keys)?;
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
        let num_keys =
            parse_i64_ascii(args[1]).ok_or(RequestExecutionError::NumkeysMustBeGreaterThanZero)?;
        if num_keys <= 0 {
            return Err(RequestExecutionError::NumkeysMustBeGreaterThanZero);
        }
        let num_keys = usize::try_from(num_keys)
            .map_err(|_| RequestExecutionError::NumkeysMustBeGreaterThanZero)?;
        let key_start = 2usize;
        let key_end = key_start + num_keys;
        if args.len() < key_end {
            return Err(RequestExecutionError::NumberOfKeysCantBeGreaterThanArgs);
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
            let parsed_limit = parse_i64_ascii(args[key_end + 1])
                .ok_or(RequestExecutionError::LimitCantBeNegative)?;
            if parsed_limit < 0 {
                return Err(RequestExecutionError::LimitCantBeNegative);
            }
            limit = usize::try_from(parsed_limit)
                .map_err(|_| RequestExecutionError::ValueOutOfRange)?;
        }

        let keys: Vec<Vec<u8>> = args[key_start..key_end]
            .iter()
            .map(|key| key.to_vec())
            .collect();
        let inter = compute_sinter(self, current_request_selected_db(), &keys)?;
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
        let selected_db = current_request_selected_db();
        let keys = collect_set_keys(args, 1);
        let diff = compute_sdiff(self, selected_db, &keys)?;
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
        let selected_db = current_request_selected_db();
        let destination = RedisKey::from(args[1]);
        let keys = collect_set_keys(args, 2);
        let union = compute_sunion(self, selected_db, &keys)?;
        store_set_result(self, DbKeyRef::new(selected_db, &destination), &union)?;
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
        let selected_db = current_request_selected_db();
        let destination = RedisKey::from(args[1]);
        let keys = collect_set_keys(args, 2);
        let inter = compute_sinter(self, selected_db, &keys)?;
        store_set_result(self, DbKeyRef::new(selected_db, &destination), &inter)?;
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
        let selected_db = current_request_selected_db();
        let destination = RedisKey::from(args[1]);
        let keys = collect_set_keys(args, 2);
        let diff = compute_sdiff(self, selected_db, &keys)?;
        store_set_result(self, DbKeyRef::new(selected_db, &destination), &diff)?;
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

fn select_random_members_distinct_from_ordered(
    processor: &RequestProcessor,
    members: &[Vec<u8>],
    count: usize,
) -> Vec<Vec<u8>> {
    let mut members = members.to_vec();
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

fn sample_random_members_with_replacement_from_ordered(
    processor: &RequestProcessor,
    members: &[Vec<u8>],
    count: usize,
) -> Vec<Vec<u8>> {
    if members.is_empty() || count == 0 {
        return Vec::new();
    }
    let mut sampled = Vec::with_capacity(count);
    for _ in 0..count {
        let index = (processor.next_random_u64() as usize) % members.len();
        sampled.push(members[index].clone());
    }
    sampled
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

    append_array_length(response_out, 2);
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
        append_array_length(response_out, set.len());
    }
    for member in set {
        append_bulk_string(response_out, member);
    }
}

fn compute_sunion(
    processor: &RequestProcessor,
    selected_db: DbName,
    keys: &[Vec<u8>],
) -> Result<BTreeSet<Vec<u8>>, RequestExecutionError> {
    let mut union = BTreeSet::new();
    for key in keys {
        if let Some(set) = processor.load_set_object(DbKeyRef::new(selected_db, key))? {
            union.extend(set);
        }
    }
    Ok(union)
}

fn compute_sinter(
    processor: &RequestProcessor,
    selected_db: DbName,
    keys: &[Vec<u8>],
) -> Result<BTreeSet<Vec<u8>>, RequestExecutionError> {
    let Some((first_key, remaining_keys)) = keys.split_first() else {
        return Ok(BTreeSet::new());
    };
    let mut inter = processor.load_set_object(DbKeyRef::new(selected_db, first_key))?;
    for key in remaining_keys {
        let other_set = processor.load_set_object(DbKeyRef::new(selected_db, key))?;
        if let Some(inter_set) = inter.as_mut() {
            let Some(other_set) = other_set else {
                inter = None;
                continue;
            };
            inter_set.retain(|member| other_set.contains(member));
            if inter_set.is_empty() {
                inter = None;
            }
        }
    }

    Ok(inter.unwrap_or_default())
}

fn compute_sdiff(
    processor: &RequestProcessor,
    selected_db: DbName,
    keys: &[Vec<u8>],
) -> Result<BTreeSet<Vec<u8>>, RequestExecutionError> {
    let Some((first_key, remaining_keys)) = keys.split_first() else {
        return Ok(BTreeSet::new());
    };
    let mut diff = processor.load_set_object(DbKeyRef::new(selected_db, first_key))?;

    for key in remaining_keys {
        let other_set = processor.load_set_object(DbKeyRef::new(selected_db, key))?;
        if let Some(diff_set) = diff.as_mut() {
            let Some(other_set) = other_set else {
                continue;
            };
            diff_set.retain(|member| !other_set.contains(member));
            if diff_set.is_empty() {
                diff = None;
            }
        }
    }

    Ok(diff.unwrap_or_default())
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
    destination: DbKeyRef<'_>,
    result_set: &BTreeSet<Vec<u8>>,
) -> Result<(), RequestExecutionError> {
    processor.expire_key_if_needed(destination)?;
    let (destination_had_string, destination_object_type) =
        processor.key_type_snapshot_for_setkey_overwrite(destination)?;
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

    processor.save_set_object_replacing_existing(destination, result_set)?;
    processor.notify_setkey_overwrite_events(
        destination.db(),
        destination.key(),
        destination_had_string,
        destination_object_type,
        Some(ObjectTypeTag::Set),
    );
    Ok(())
}

fn delete_string_value_for_object_overwrite(
    processor: &RequestProcessor,
    key: DbKeyRef<'_>,
) -> Result<bool, RequestExecutionError> {
    let key_vec = key.key().to_vec();
    let mut store = processor.lock_string_store_for_key(key.key());
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
