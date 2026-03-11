// Related blocking-wakeup isolation model:
// `formal/tla/specs/BlockingWaitClassIsolation.tla`
// Related stream consumer-group ownership model:
// `formal/tla/specs/StreamPelOwnership.tla`
use super::*;

impl RequestProcessor {
    fn parse_xgroup_id_arg(
        &self,
        id_arg: &[u8],
        stream: &StreamObject,
    ) -> Result<StreamId, RequestExecutionError> {
        if id_arg == b"-" {
            return Ok(StreamId::zero());
        }
        if id_arg == b"$" {
            return Ok(stream.last_generated_id);
        }
        StreamId::parse(id_arg).ok_or(RequestExecutionError::InvalidStreamId)
    }

    fn register_stream_consumer(
        &self,
        stream: &mut StreamObject,
        key: &RedisKey,
        group: &[u8],
        consumer: &[u8],
    ) -> bool {
        let now = current_unix_time_millis().unwrap_or(0);
        let Some(group_state) = stream.groups.get_mut(group) else {
            return false;
        };
        let inserted = ensure_stream_consumer(group_state, consumer, now);
        if inserted {
            self.notify_keyspace_event(NOTIFY_STREAM, b"xgroup-createconsumer", key);
        }
        inserted
    }

    pub(super) fn handle_xadd(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        let xadd_args = parse_xadd_arguments(args)?;
        let key = RedisKey::from(args[1]);
        let loaded_stream = self.load_stream_object(&key)?;
        if loaded_stream.is_none() && xadd_args.nomkstream {
            if self.resp_protocol_version().is_resp3() {
                append_null(response_out);
            } else {
                append_null_bulk_string(response_out);
            }
            return Ok(());
        }

        let mut stream = loaded_stream.unwrap_or_else(|| {
            StreamObject::with_idmp_config(
                self.stream_idmp_duration_seconds(),
                self.stream_idmp_maxsize(),
            )
        });
        stream.ensure_node_sizes(self.stream_node_max_entries());

        let mut fields = Vec::with_capacity((args.len() - xadd_args.field_start_index) / 2);
        let mut index = xadd_args.field_start_index;
        while index < args.len() {
            let field = args[index].to_vec();
            let value = args[index + 1].to_vec();
            fields.push((field, value));
            index += 2;
        }

        let now_millis = current_unix_time_millis().unwrap_or_else(|| {
            stream
                .last_generated_id
                .timestamp_millis()
                .saturating_add(1)
                .max(1)
        });

        let resolved_idmp = xadd_args
            .idmp_spec
            .as_ref()
            .map(|spec| ResolvedXaddIdmpSpec {
                producer_id: spec.producer_id.clone(),
                iid: spec.resolve_iid(&fields),
            });
        if let Some(idmp_spec) = resolved_idmp.as_ref()
            && let Some(existing_id) =
                stream.idmp_lookup(&idmp_spec.producer_id, &idmp_spec.iid, now_millis)
        {
            stream.iids_duplicates = stream.iids_duplicates.saturating_add(1);
            self.save_stream_object(&key, &stream)?;
            append_bulk_string(response_out, &existing_id.encode());
            return Ok(());
        }

        let id = resolve_xadd_id(&stream, xadd_args.id_spec)?;

        stream.entries.insert(id, fields);
        stream.note_appended_entry(self.stream_node_max_entries());
        stream.last_generated_id = id;
        stream.entries_added = stream.entries_added.saturating_add(1);
        if let Some(idmp_spec) = resolved_idmp {
            stream.remember_idmp_entry(idmp_spec.producer_id, idmp_spec.iid, id);
        }
        if let Some(trim_spec) = &xadd_args.trim_spec {
            let _ =
                apply_xtrim_spec_to_stream(&mut stream, trim_spec, self.stream_node_max_entries());
        }
        self.save_stream_object(&key, &stream)?;
        self.notify_keyspace_event(NOTIFY_STREAM, b"xadd", &key);
        append_bulk_string(response_out, &id.encode());
        Ok(())
    }

    pub(super) fn handle_xdel(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 3, "XDEL", "XDEL key id [id ...]")?;

        let key = RedisKey::from(args[1]);
        let mut stream = match self.load_stream_object(&key)? {
            Some(stream) => stream,
            None => {
                append_integer(response_out, 0);
                return Ok(());
            }
        };

        let mut removed = 0i64;
        for id in &args[2..] {
            let Some(id) = StreamId::parse(id) else {
                continue;
            };
            if remove_stream_entry(&mut stream, id) {
                removed += 1;
            }
        }

        self.save_stream_object(&key, &stream)?;
        if removed > 0 {
            self.notify_keyspace_event(NOTIFY_STREAM, b"xdel", &key);
        }
        append_integer(response_out, removed);
        Ok(())
    }

    pub(super) fn handle_xdelex(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            5,
            "XDELEX",
            "XDELEX key [KEEPREF|DELREF|ACKED] IDS numids id [id ...]",
        )?;
        let parsed = parse_stream_ack_delete_arguments(args, 2)?;
        let key = RedisKey::from(args[1]);
        let Some(mut stream) = self.load_stream_object(&key)? else {
            append_array_length(response_out, parsed.entry_ids.len());
            for _ in &parsed.entry_ids {
                append_integer(response_out, -1);
            }
            return Ok(());
        };

        let mut statuses = Vec::with_capacity(parsed.entry_ids.len());
        let mut changed = false;
        for entry_id in parsed.entry_ids {
            let mut can_delete = true;
            match parsed.delete_strategy {
                StreamDeleteStrategy::KeepRef => {}
                StreamDeleteStrategy::DelRef => {
                    changed |= cleanup_stream_entry_group_refs(&mut stream, entry_id);
                }
                StreamDeleteStrategy::Acked => {
                    can_delete = !stream_entry_is_referenced(&stream, entry_id);
                }
            }

            if !can_delete {
                statuses.push(2);
                continue;
            }

            if remove_stream_entry(&mut stream, entry_id) {
                changed = true;
                statuses.push(1);
            } else {
                statuses.push(-1);
            }
        }

        append_array_length(response_out, statuses.len());
        for status in statuses {
            append_integer(response_out, status);
        }
        if changed {
            if stream.entries.is_empty() && stream.groups.is_empty() {
                let _ = self.object_delete(&key)?;
            } else {
                self.save_stream_object(&key, &stream)?;
            }
        }
        Ok(())
    }

    pub(super) fn handle_xgroup(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            2,
            "XGROUP",
            "XGROUP <CREATE|SETID|DESTROY> key group [id]",
        )?;
        let subcommand = args[1];

        if ascii_eq_ignore_case(subcommand, b"HELP") {
            require_exact_arity(args, 2, "XGROUP|HELP", "XGROUP HELP")?;
            append_bulk_array(response_out, &XGROUP_HELP_LINES);
            return Ok(());
        }

        if ascii_eq_ignore_case(subcommand, b"CREATE") {
            ensure_min_arity(
                args,
                5,
                "XGROUP",
                "XGROUP CREATE key group id [MKSTREAM] [ENTRIESREAD entries-read]",
            )?;
            let key = RedisKey::from(args[2]);
            let group = args[3].to_vec();
            let mut mkstream = false;
            let mut entries_read = None;
            let mut index = 5;
            while index < args.len() {
                if ascii_eq_ignore_case(args[index], b"MKSTREAM") {
                    mkstream = true;
                    index += 1;
                } else if ascii_eq_ignore_case(args[index], b"ENTRIESREAD") {
                    if index + 1 >= args.len() {
                        return Err(RequestExecutionError::SyntaxError);
                    }
                    entries_read = Some(parse_stream_entries_read(args[index + 1])?);
                    index += 2;
                } else {
                    return Err(RequestExecutionError::SyntaxError);
                }
            }

            let loaded_stream = self.load_stream_object(&key)?;
            if loaded_stream.is_none() && !mkstream {
                return Err(RequestExecutionError::XGroupKeyMustExist);
            }
            let mut stream = loaded_stream.unwrap_or_default();
            let id = self.parse_xgroup_id_arg(args[4], &stream)?;
            if stream.groups.contains_key(&group) {
                return Err(RequestExecutionError::BusyGroup);
            }
            let group_state = StreamGroupState {
                last_delivered_id: id,
                entries_read: entries_read
                    .flatten()
                    .map(|value| value.min(stream.entries_added)),
                consumers: BTreeMap::new(),
                pending: BTreeMap::new(),
            };
            stream.groups.insert(group, group_state);
            self.save_stream_object(&key, &stream)?;
            self.notify_keyspace_event(NOTIFY_STREAM, b"xgroup-create", &key);
            append_simple_string(response_out, b"OK");
            return Ok(());
        }

        if ascii_eq_ignore_case(subcommand, b"SETID") {
            ensure_one_of_arities(
                args,
                &[5, 7],
                "XGROUP",
                "XGROUP SETID key group id [ENTRIESREAD entries-read]",
            )?;
            let key = RedisKey::from(args[2]);
            let group = args[3];
            let mut entries_read = None;
            if args.len() == 7 {
                if !ascii_eq_ignore_case(args[5], b"ENTRIESREAD") {
                    return Err(RequestExecutionError::SyntaxError);
                }
                entries_read = Some(parse_stream_entries_read(args[6])?);
            }
            let Some(mut stream) = self.load_stream_object(&key)? else {
                return Err(RequestExecutionError::XGroupKeyMustExist);
            };
            let id = self.parse_xgroup_id_arg(args[4], &stream)?;
            let stream_entries_added = stream.entries_added;
            let Some(group_state) = stream.groups.get_mut(group) else {
                return Err(RequestExecutionError::NoGroup);
            };
            group_state.last_delivered_id = id;
            if let Some(parsed) = entries_read {
                group_state.entries_read = parsed.map(|value| value.min(stream_entries_added));
            }
            self.save_stream_object(&key, &stream)?;
            self.notify_keyspace_event(NOTIFY_STREAM, b"xgroup-setid", &key);
            append_simple_string(response_out, b"OK");
            return Ok(());
        }

        if ascii_eq_ignore_case(subcommand, b"DESTROY") {
            require_exact_arity(args, 4, "XGROUP", "XGROUP DESTROY key group")?;
            let key = RedisKey::from(args[2]);
            let Some(mut stream) = self.load_stream_object(&key)? else {
                append_integer(response_out, 0);
                return Ok(());
            };
            let removed = if stream.groups.remove(args[3]).is_some() {
                1
            } else {
                0
            };
            if stream.entries.is_empty() && stream.groups.is_empty() {
                let _ = self.object_delete(&key)?;
            } else if removed == 1 {
                self.save_stream_object(&key, &stream)?;
            }
            if removed == 1 {
                self.notify_keyspace_event(NOTIFY_STREAM, b"xgroup-destroy", &key);
            }
            append_integer(response_out, removed);
            return Ok(());
        }

        if ascii_eq_ignore_case(subcommand, b"CREATECONSUMER") {
            require_exact_arity(
                args,
                5,
                "XGROUP",
                "XGROUP CREATECONSUMER key group consumer",
            )?;
            let key = RedisKey::from(args[2]);
            let Some(mut stream) = self.load_stream_object(&key)? else {
                return Err(RequestExecutionError::NoSuchKey);
            };
            if !stream.groups.contains_key(args[3]) {
                return Err(RequestExecutionError::NoGroup);
            }
            let created = self.register_stream_consumer(&mut stream, &key, args[3], args[4]);
            self.save_stream_object(&key, &stream)?;
            append_integer(response_out, if created { 1 } else { 0 });
            return Ok(());
        }

        if ascii_eq_ignore_case(subcommand, b"DELCONSUMER") {
            require_exact_arity(args, 5, "XGROUP", "XGROUP DELCONSUMER key group consumer")?;
            let key = RedisKey::from(args[2]);
            let Some(mut stream) = self.load_stream_object(&key)? else {
                return Err(RequestExecutionError::NoSuchKey);
            };
            if !stream.groups.contains_key(args[3]) {
                return Err(RequestExecutionError::NoGroup);
            }
            let consumer_removed = stream
                .groups
                .get(args[3])
                .is_some_and(|group_state| group_state.consumers.contains_key(args[4]));
            let removed_pending = stream
                .groups
                .get_mut(args[3])
                .map(|group_state| delete_stream_consumer(group_state, args[4]))
                .unwrap_or(0);
            if consumer_removed {
                self.notify_keyspace_event(NOTIFY_STREAM, b"xgroup-delconsumer", &key);
                self.save_stream_object(&key, &stream)?;
            }
            append_integer(response_out, removed_pending);
            return Ok(());
        }

        Err(RequestExecutionError::UnknownSubcommand)
    }

    pub(super) fn handle_xreadgroup(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        let parsed = parse_xreadgroup_arguments(args)?;
        let blocking_client_id = parsed
            .block_millis
            .and_then(|_| current_request_client_id());
        let now_millis = current_unix_time_millis().unwrap_or(0);
        let mut stream_results = Vec::new();
        let mut should_record_rdb_change = false;

        for (key, start_id) in parsed
            .stream_keys
            .iter()
            .cloned()
            .zip(parsed.stream_start_ids.iter().copied())
        {
            let mut stream = match self.load_stream_object(&key)? {
                Some(stream) => stream,
                None => return Err(RequestExecutionError::NoGroup),
            };
            if !stream.groups.contains_key(parsed.group) {
                return Err(RequestExecutionError::NoGroup);
            }

            let per_stream_limit = parsed.count;
            let special_greater_than_id = start_id == b">";
            let mut stream_changed = false;
            let mut stream_should_record_rdb_change = false;
            let consumer_created =
                self.register_stream_consumer(&mut stream, &key, parsed.group, parsed.consumer);
            if consumer_created {
                stream_changed = true;
                stream_should_record_rdb_change = true;
            }

            let selected = if special_greater_than_id {
                let selected = if let Some(min_idle_millis) = parsed.claim_min_idle_millis {
                    let mut claimed_entries = claim_xreadgroup_pending_entries_for_stream(
                        &mut stream,
                        parsed.group,
                        parsed.consumer,
                        min_idle_millis,
                        now_millis,
                        per_stream_limit,
                        parsed.noack,
                        &mut stream_changed,
                    )?;
                    let remaining_for_new = if per_stream_limit == usize::MAX {
                        usize::MAX
                    } else {
                        per_stream_limit.saturating_sub(claimed_entries.len())
                    };
                    let mut new_entries = deliver_xreadgroup_new_entries_for_stream(
                        &mut stream,
                        parsed.group,
                        parsed.consumer,
                        now_millis,
                        remaining_for_new,
                        parsed.noack,
                        true,
                        &mut stream_changed,
                    )?;
                    claimed_entries.append(&mut new_entries);
                    claimed_entries
                } else {
                    deliver_xreadgroup_new_entries_for_stream(
                        &mut stream,
                        parsed.group,
                        parsed.consumer,
                        now_millis,
                        per_stream_limit,
                        parsed.noack,
                        false,
                        &mut stream_changed,
                    )?
                };

                if selected.is_empty() {
                    if !consumer_created
                        && parsed.block_millis.is_none()
                        && let Some(group_state) = stream.groups.get_mut(parsed.group)
                        && let Some(consumer_state) = group_state.consumers.get_mut(parsed.consumer)
                    {
                        consumer_state.seen_time_millis = now_millis;
                        stream_changed = true;
                    }
                } else {
                    stream_should_record_rdb_change = true;
                }
                selected
            } else {
                let pivot =
                    StreamId::parse(start_id).ok_or(RequestExecutionError::InvalidStreamId)?;
                collect_stream_consumer_pending_response_entries(
                    &stream,
                    stream
                        .groups
                        .get(parsed.group)
                        .ok_or(RequestExecutionError::NoGroup)?,
                    parsed.consumer,
                    pivot,
                    per_stream_limit,
                )
            };

            if stream_changed {
                self.save_stream_object(&key, &stream)?;
            }
            if stream_should_record_rdb_change {
                should_record_rdb_change = true;
            }

            if selected.is_empty() {
                if !special_greater_than_id {
                    stream_results.push((key, Vec::new()));
                }
                continue;
            }
            stream_results.push((key, selected));
        }

        if should_record_rdb_change {
            self.record_rdb_change(1);
        }

        if stream_results.is_empty() {
            if parsed.block_millis.is_some()
                && parsed
                    .stream_start_ids
                    .iter()
                    .all(|start_id| *start_id == b">")
            {
                if let Some(client_id) = blocking_client_id {
                    self.set_blocked_stream_wait_state(
                        client_id,
                        BlockingStreamWaitState::Xreadgroup(BlockingXreadgroupWait {
                            stream_keys: parsed.stream_keys.clone(),
                            group: parsed.group.to_vec(),
                            claim_min_idle_millis: parsed.claim_min_idle_millis,
                        }),
                    );
                }
                if self.resp_protocol_version().is_resp3() {
                    append_null(response_out);
                } else {
                    append_null_array(response_out);
                }
            } else {
                append_array_length(response_out, 0);
            }
            return Ok(());
        }

        append_xreadgroup_response(
            response_out,
            &stream_results,
            self.resp_protocol_version().is_resp3(),
        );
        Ok(())
    }

    pub(super) fn handle_xread(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            4,
            "XREAD",
            "XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] id [id ...]",
        )?;

        let mut count = usize::MAX;
        let mut block_requested = false;
        let mut index = 1usize;
        let streams_index = loop {
            if index >= args.len() {
                return Err(RequestExecutionError::SyntaxError);
            }
            let token = args[index];
            if ascii_eq_ignore_case(token, b"COUNT") {
                if index + 1 >= args.len() {
                    return Err(RequestExecutionError::SyntaxError);
                }
                let parsed = parse_u64_ascii(args[index + 1])
                    .ok_or(RequestExecutionError::ValueNotInteger)?;
                count =
                    usize::try_from(parsed).map_err(|_| RequestExecutionError::ValueOutOfRange)?;
                index += 2;
                continue;
            }
            if ascii_eq_ignore_case(token, b"BLOCK") {
                if index + 1 >= args.len() {
                    return Err(RequestExecutionError::SyntaxError);
                }
                parse_stream_block_millis(args[index + 1])?;
                block_requested = true;
                index += 2;
                continue;
            }
            if ascii_eq_ignore_case(token, b"CLAIM") {
                return Err(RequestExecutionError::ClaimOptionOnlySupportedByXreadgroup);
            }
            if ascii_eq_ignore_case(token, b"STREAMS") {
                break index + 1;
            }
            return Err(RequestExecutionError::SyntaxError);
        };

        if streams_index >= args.len() {
            return Err(RequestExecutionError::SyntaxError);
        }
        let trailing = args.len() - streams_index;
        if trailing == 0 {
            return Err(RequestExecutionError::SyntaxError);
        }
        if !trailing.is_multiple_of(2) {
            return Err(RequestExecutionError::UnbalancedXreadStreamList);
        }
        let stream_count = trailing / 2;
        let blocking_client_id = if block_requested {
            current_request_client_id()
        } else {
            None
        };

        let mut stream_results = Vec::new();
        let mut wait_streams = Vec::with_capacity(stream_count);
        for stream_index in 0..stream_count {
            let key = RedisKey::from(args[streams_index + stream_index]);
            let raw_id = args[streams_index + stream_count + stream_index];
            let stream = self.load_stream_object(&key)?;
            let selection = if raw_id == b"$" {
                let current_tail = stream
                    .as_ref()
                    .and_then(|stream| stream.entries.keys().next_back().copied())
                    .unwrap_or_else(StreamId::zero);
                let pivot = if let Some(client_id) = blocking_client_id {
                    self.blocked_xread_tail_id(client_id, stream_index, stream_count, current_tail)
                } else {
                    current_tail
                };
                BlockingXreadWaitSelection::After(pivot)
            } else if raw_id == b"+" {
                BlockingXreadWaitSelection::LastEntry
            } else {
                let pivot =
                    StreamId::parse(raw_id).ok_or(RequestExecutionError::InvalidStreamId)?;
                BlockingXreadWaitSelection::After(pivot)
            };
            wait_streams.push(BlockingXreadStreamWait {
                key: key.clone(),
                selection,
            });
            let Some(stream) = stream else {
                continue;
            };

            let selected = collect_stream_entries_for_xread(&stream, selection, count);
            if selected.is_empty() {
                continue;
            }
            stream_results.push((key, selected));
        }

        let resp3 = self.resp_protocol_version().is_resp3();
        if stream_results.is_empty() {
            if let Some(client_id) = blocking_client_id {
                self.set_blocked_stream_wait_state(
                    client_id,
                    BlockingStreamWaitState::Xread(wait_streams),
                );
            }
            if resp3 {
                append_null(response_out);
            } else {
                append_null_array(response_out);
            }
            return Ok(());
        }
        if resp3 {
            append_map_length(response_out, stream_results.len());
        } else {
            append_array_length(response_out, stream_results.len());
        }
        for (key, entries) in stream_results {
            if !resp3 {
                append_array_length(response_out, 2);
            }
            append_bulk_string(response_out, key.as_slice());
            append_stream_entry_array(response_out, &entries, resp3);
        }
        Ok(())
    }

    pub(super) fn handle_xack(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 4, "XACK", "XACK key group id [id ...]")?;
        let mut parsed_ids = Vec::with_capacity(args.len().saturating_sub(3));
        for id_arg in &args[3..] {
            parsed_ids.push(StreamId::parse(id_arg).ok_or(RequestExecutionError::InvalidStreamId)?);
        }
        let key = RedisKey::from(args[1]);
        let group = args[2];
        let Some(mut stream) = self.load_stream_object(&key)? else {
            append_integer(response_out, 0);
            return Ok(());
        };
        let Some(group_state) = stream.groups.get_mut(group) else {
            append_integer(response_out, 0);
            return Ok(());
        };
        let mut acked = 0i64;
        for pending_id in parsed_ids {
            if ack_stream_pending_entry(group_state, pending_id) {
                acked += 1;
            }
        }
        if acked > 0 {
            self.save_stream_object(&key, &stream)?;
        }
        append_integer(response_out, acked);
        Ok(())
    }

    pub(super) fn handle_xackdel(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            6,
            "XACKDEL",
            "XACKDEL key group [KEEPREF|DELREF|ACKED] IDS numids id [id ...]",
        )?;
        let parsed = parse_stream_ack_delete_arguments(args, 3)?;
        let key = RedisKey::from(args[1]);
        let group = args[2];
        let Some(mut stream) = self.load_stream_object(&key)? else {
            append_array_length(response_out, parsed.entry_ids.len());
            for _ in &parsed.entry_ids {
                append_integer(response_out, -1);
            }
            return Ok(());
        };
        if !stream.groups.contains_key(group) {
            append_array_length(response_out, parsed.entry_ids.len());
            for _ in &parsed.entry_ids {
                append_integer(response_out, -1);
            }
            return Ok(());
        }

        let mut statuses = Vec::with_capacity(parsed.entry_ids.len());
        let mut changed = false;
        for entry_id in parsed.entry_ids {
            let was_pending = stream
                .groups
                .get(group)
                .is_some_and(|group_state| group_state.pending.contains_key(&entry_id));
            if !was_pending {
                statuses.push(-1);
                continue;
            }

            let Some(group_state) = stream.groups.get_mut(group) else {
                return Err(RequestExecutionError::NoGroup);
            };
            let acked = ack_stream_pending_entry(group_state, entry_id);
            debug_assert!(acked);
            changed = true;

            let mut can_delete = true;
            match parsed.delete_strategy {
                StreamDeleteStrategy::KeepRef => {}
                StreamDeleteStrategy::DelRef => {
                    changed |= cleanup_stream_entry_group_refs(&mut stream, entry_id);
                }
                StreamDeleteStrategy::Acked => {
                    can_delete = !stream_entry_is_referenced(&stream, entry_id);
                }
            }

            if !can_delete {
                statuses.push(2);
                continue;
            }

            let _ = remove_stream_entry(&mut stream, entry_id);
            statuses.push(1);
        }

        append_array_length(response_out, statuses.len());
        for status in statuses {
            append_integer(response_out, status);
        }
        if changed {
            if stream.entries.is_empty() && stream.groups.is_empty() {
                let _ = self.object_delete(&key)?;
            } else {
                self.save_stream_object(&key, &stream)?;
            }
        }
        Ok(())
    }

    pub(super) fn handle_xpending(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            3,
            "XPENDING",
            "XPENDING key group [IDLE min-idle-time] start end count [consumer]",
        )?;
        let key = RedisKey::from(args[1]);
        let group = args[2];
        let Some(stream) = self.load_stream_object(&key)? else {
            return Err(RequestExecutionError::NoGroup);
        };
        let Some(group_state) = stream.groups.get(group) else {
            return Err(RequestExecutionError::NoGroup);
        };

        if args.len() == 3 {
            let resp3 = self.resp_protocol_version().is_resp3();
            append_array_length(response_out, 4);
            append_integer(response_out, group_state.pending.len() as i64);
            if let Some(first_id) = group_state.pending.keys().next() {
                append_bulk_string(response_out, &first_id.encode());
            } else if resp3 {
                append_null(response_out);
            } else {
                append_null_bulk_string(response_out);
            }
            if let Some(last_id) = group_state.pending.keys().next_back() {
                append_bulk_string(response_out, &last_id.encode());
            } else if resp3 {
                append_null(response_out);
            } else {
                append_null_bulk_string(response_out);
            }
            let consumer_summaries: Vec<_> = group_state
                .consumers
                .iter()
                .filter_map(|(consumer_name, consumer_state)| {
                    let pending_count = consumer_state.pending.len();
                    if pending_count == 0 {
                        return None;
                    }
                    Some((consumer_name, pending_count))
                })
                .collect();
            append_array_length(response_out, consumer_summaries.len());
            for (consumer_name, pending_count) in consumer_summaries {
                append_array_length(response_out, 2);
                append_bulk_string(response_out, consumer_name);
                append_integer(response_out, pending_count as i64);
            }
            return Ok(());
        }

        let has_idle = args.len() >= 4 && ascii_eq_ignore_case(args[3], b"IDLE");
        let (min_idle_millis, base_index) = if has_idle {
            if args.len() < 8 {
                return Err(RequestExecutionError::SyntaxError);
            }
            let parsed = parse_i64_ascii(args[4]).ok_or(RequestExecutionError::ValueNotInteger)?;
            if parsed < 0 {
                return Err(RequestExecutionError::ValueOutOfRange);
            }
            (
                Some(u64::try_from(parsed).map_err(|_| RequestExecutionError::ValueOutOfRange)?),
                5usize,
            )
        } else {
            if args.len() < 6 {
                return Err(RequestExecutionError::SyntaxError);
            }
            (None, 3usize)
        };
        let parsed_count =
            parse_i64_ascii(args[base_index + 2]).ok_or(RequestExecutionError::ValueNotInteger)?;
        if parsed_count < 0 {
            return Err(RequestExecutionError::ValueOutOfRange);
        }
        let max_args = base_index + 4;
        if args.len() > max_args {
            return Err(RequestExecutionError::SyntaxError);
        }
        let start_bound = parse_xpending_bound(args[base_index], true)?;
        let end_bound = parse_xpending_bound(args[base_index + 1], false)?;
        let count =
            usize::try_from(parsed_count).map_err(|_| RequestExecutionError::ValueOutOfRange)?;
        let consumer_filter = args.get(base_index + 3).copied();
        let now_millis = current_unix_time_millis().unwrap_or(0);

        let mut matching = Vec::new();
        for (pending_id, pending_entry) in &group_state.pending {
            if !start_bound.matches_start(*pending_id) || !end_bound.matches_end(*pending_id) {
                continue;
            }
            if let Some(consumer_filter) = consumer_filter
                && pending_entry.consumer.as_slice() != consumer_filter
            {
                continue;
            }
            if let Some(min_idle_millis) = min_idle_millis
                && stream_pending_idle_millis(pending_entry, now_millis) < min_idle_millis
            {
                continue;
            }
            matching.push((pending_id, pending_entry));
            if matching.len() >= count {
                break;
            }
        }

        append_array_length(response_out, matching.len());
        for (pending_id, pending_entry) in matching {
            append_array_length(response_out, 4);
            append_bulk_string(response_out, &pending_id.encode());
            append_bulk_string(response_out, &pending_entry.consumer);
            append_integer(
                response_out,
                stream_pending_idle_millis(pending_entry, now_millis).min(i64::MAX as u64) as i64,
            );
            append_integer(
                response_out,
                pending_entry.delivery_count.min(i64::MAX as u64) as i64,
            );
        }
        Ok(())
    }

    pub(super) fn handle_xclaim(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            6,
            "XCLAIM",
            "XCLAIM key group consumer min-idle-time id [id ...] [options]",
        )?;
        let key = RedisKey::from(args[1]);
        let group = args[2];
        let consumer = args[3];
        let Some(mut stream) = self.load_stream_object(&key)? else {
            return Err(RequestExecutionError::NoGroup);
        };
        if !stream.groups.contains_key(group) {
            return Err(RequestExecutionError::NoGroup);
        }

        let min_idle_millis = parse_nonnegative_millis_clamped(args[4])?;
        let now_millis = current_unix_time_millis().unwrap_or(0);

        let mut ids = Vec::new();
        let mut index = 5usize;
        while index < args.len() {
            let token = args[index];
            if is_xclaim_option_token(token) {
                break;
            }
            ids.push(StreamId::parse(token).ok_or(RequestExecutionError::InvalidStreamId)?);
            index += 1;
        }
        if ids.is_empty() {
            return Err(RequestExecutionError::SyntaxError);
        }

        let mut delivery_time_millis = now_millis;
        let mut retry_count = None::<u64>;
        let mut force = false;
        let mut justid = false;
        let mut last_id = None::<StreamId>;
        while index < args.len() {
            let token = args[index];
            if ascii_eq_ignore_case(token, b"IDLE") {
                if index + 1 >= args.len() {
                    return Err(RequestExecutionError::SyntaxError);
                }
                delivery_time_millis =
                    now_millis.saturating_sub(parse_nonnegative_millis_clamped(args[index + 1])?);
                index += 2;
                continue;
            }
            if ascii_eq_ignore_case(token, b"TIME") {
                if index + 1 >= args.len() {
                    return Err(RequestExecutionError::SyntaxError);
                }
                delivery_time_millis =
                    parse_nonnegative_millis_clamped(args[index + 1])?.min(now_millis);
                index += 2;
                continue;
            }
            if ascii_eq_ignore_case(token, b"RETRYCOUNT") {
                if index + 1 >= args.len() {
                    return Err(RequestExecutionError::SyntaxError);
                }
                retry_count = Some(parse_nonnegative_millis_clamped(args[index + 1])?);
                index += 2;
                continue;
            }
            if ascii_eq_ignore_case(token, b"LASTID") {
                if index + 1 >= args.len() {
                    return Err(RequestExecutionError::SyntaxError);
                }
                last_id = Some(
                    StreamId::parse(args[index + 1])
                        .ok_or(RequestExecutionError::InvalidStreamId)?,
                );
                index += 2;
                continue;
            }
            if ascii_eq_ignore_case(token, b"FORCE") {
                force = true;
                index += 1;
                continue;
            }
            if ascii_eq_ignore_case(token, b"JUSTID") {
                justid = true;
                index += 1;
                continue;
            }
            return Err(RequestExecutionError::SyntaxError);
        }

        let mut changed = false;
        if self.register_stream_consumer(&mut stream, &key, group, consumer) {
            changed = true;
        }
        let mut claimed_entries = Vec::new();
        let mut claimed_ids = Vec::new();

        if let Some(group_state) = stream.groups.get_mut(group) {
            note_stream_consumer_seen(group_state, consumer, now_millis);
            changed = true;
            if let Some(last_id) = last_id
                && last_id > group_state.last_delivered_id
            {
                group_state.last_delivered_id = last_id;
            }
        }

        for entry_id in ids {
            let entry_fields = stream.entries.get(&entry_id).cloned();
            let Some(group_state) = stream.groups.get_mut(group) else {
                return Err(RequestExecutionError::NoGroup);
            };

            if entry_fields.is_none() {
                if ack_stream_pending_entry(group_state, entry_id) {
                    changed = true;
                }
                continue;
            }

            let Some(existing_pending) = group_state.pending.get(&entry_id) else {
                if !force {
                    continue;
                }
                let claimed = claim_stream_pending_entry(
                    group_state,
                    consumer,
                    entry_id,
                    delivery_time_millis,
                    now_millis,
                    retry_count,
                    justid,
                    true,
                );
                if claimed {
                    changed = true;
                    if justid {
                        claimed_ids.push(entry_id);
                    } else if let Some(fields) = entry_fields {
                        claimed_entries.push((entry_id, fields));
                    }
                }
                continue;
            };

            if existing_pending.consumer.as_slice() != consumer
                && stream_pending_idle_millis(existing_pending, now_millis) < min_idle_millis
            {
                continue;
            }

            let claimed = claim_stream_pending_entry(
                group_state,
                consumer,
                entry_id,
                delivery_time_millis,
                now_millis,
                retry_count,
                justid,
                false,
            );
            if claimed {
                changed = true;
                if justid {
                    claimed_ids.push(entry_id);
                } else if let Some(fields) = entry_fields {
                    claimed_entries.push((entry_id, fields));
                }
            }
        }

        if changed {
            self.save_stream_object(&key, &stream)?;
        }
        if justid {
            append_stream_id_array(response_out, &claimed_ids);
        } else {
            append_stream_entry_array(
                response_out,
                &claimed_entries,
                self.resp_protocol_version().is_resp3(),
            );
        }
        Ok(())
    }

    pub(super) fn handle_xautoclaim(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            6,
            "XAUTOCLAIM",
            "XAUTOCLAIM key group consumer min-idle-time start [COUNT count] [JUSTID]",
        )?;
        let key = RedisKey::from(args[1]);
        let group = args[2];
        let consumer = args[3];
        let min_idle_millis = parse_nonnegative_millis_clamped(args[4])?;
        let start_id = parse_xautoclaim_start_id(args[5])?;

        let mut count = 100usize;
        let mut justid = false;
        let mut index = 6usize;
        while index < args.len() {
            let token = args[index];
            if ascii_eq_ignore_case(token, b"COUNT") {
                if index + 1 >= args.len() {
                    return Err(RequestExecutionError::SyntaxError);
                }
                let parsed = parse_u64_ascii(args[index + 1])
                    .ok_or(RequestExecutionError::ValueNotInteger)?;
                if parsed == 0 || parsed > (usize::MAX / 10) as u64 {
                    return Err(RequestExecutionError::XautoclaimCountMustBeGreaterThanZero);
                }
                count = usize::try_from(parsed)
                    .map_err(|_| RequestExecutionError::XautoclaimCountMustBeGreaterThanZero)?;
                index += 2;
                continue;
            }
            if ascii_eq_ignore_case(token, b"JUSTID") {
                justid = true;
                index += 1;
                continue;
            }
            return Err(RequestExecutionError::SyntaxError);
        }

        let Some(mut stream) = self.load_stream_object(&key)? else {
            return Err(RequestExecutionError::NoGroup);
        };
        if !stream.groups.contains_key(group) {
            return Err(RequestExecutionError::NoGroup);
        }
        let now_millis = current_unix_time_millis().unwrap_or(0);

        let mut changed = false;
        if self.register_stream_consumer(&mut stream, &key, group, consumer) {
            changed = true;
        }

        let mut claimed_entries = Vec::new();
        let mut claimed_ids = Vec::new();
        let mut deleted_ids = Vec::new();
        let mut next_cursor = StreamId::zero();

        let candidate_ids = {
            let group_state = stream
                .groups
                .get(group)
                .ok_or(RequestExecutionError::NoGroup)?;
            group_state
                .pending
                .range(start_id..)
                .map(|(entry_id, _)| *entry_id)
                .collect::<Vec<_>>()
        };

        if let Some(group_state) = stream.groups.get_mut(group) {
            note_stream_consumer_seen(group_state, consumer, now_millis);
            changed = true;
        }

        let mut remaining = count;
        let mut attempts_remaining = count.saturating_mul(10);
        let mut last_scanned = None::<StreamId>;
        for entry_id in candidate_ids {
            if remaining == 0 || attempts_remaining == 0 {
                break;
            }
            attempts_remaining = attempts_remaining.saturating_sub(1);
            last_scanned = Some(entry_id);

            let entry_fields = stream.entries.get(&entry_id).cloned();
            let Some(group_state) = stream.groups.get_mut(group) else {
                return Err(RequestExecutionError::NoGroup);
            };

            if entry_fields.is_none() {
                if ack_stream_pending_entry(group_state, entry_id) {
                    changed = true;
                    deleted_ids.push(entry_id);
                    remaining = remaining.saturating_sub(1);
                }
                continue;
            }

            let Some(existing_pending) = group_state.pending.get(&entry_id) else {
                continue;
            };
            if stream_pending_idle_millis(existing_pending, now_millis) < min_idle_millis {
                continue;
            }

            let claimed = claim_stream_pending_entry(
                group_state,
                consumer,
                entry_id,
                now_millis,
                now_millis,
                None,
                justid,
                false,
            );
            if !claimed {
                continue;
            }

            changed = true;
            remaining = remaining.saturating_sub(1);
            if justid {
                claimed_ids.push(entry_id);
            } else if let Some(fields) = entry_fields {
                claimed_entries.push((entry_id, fields));
            }
        }

        if let Some(group_state) = stream.groups.get(group) {
            if let Some(last_scanned) = last_scanned {
                next_cursor = group_state
                    .pending
                    .range((
                        std::ops::Bound::Excluded(last_scanned),
                        std::ops::Bound::Unbounded,
                    ))
                    .next()
                    .map(|(entry_id, _)| *entry_id)
                    .unwrap_or_else(StreamId::zero);
            }
        }

        if changed {
            self.save_stream_object(&key, &stream)?;
        }
        append_array_length(response_out, 3);
        append_bulk_string(response_out, &next_cursor.encode());
        if justid {
            append_stream_id_array(response_out, &claimed_ids);
        } else {
            append_stream_entry_array(
                response_out,
                &claimed_entries,
                self.resp_protocol_version().is_resp3(),
            );
        }
        append_stream_id_array(response_out, &deleted_ids);
        Ok(())
    }

    pub(super) fn handle_xsetid(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            3,
            "XSETID",
            "XSETID key last-id [ENTRIESADDED entries-added] [MAXDELETEDID max-id]",
        )?;
        let key = RedisKey::from(args[1]);
        let last_id = StreamId::parse(args[2]).ok_or(RequestExecutionError::InvalidStreamId)?;
        let options = parse_xsetid_options(args, 3, last_id)?;
        let Some(stream) = self.load_stream_object(&key)? else {
            return Err(RequestExecutionError::NoSuchKey);
        };
        let mut stream = stream;

        if last_id < stream.max_deleted_entry_id {
            return Err(RequestExecutionError::XsetidSmallerThanCurrentMaxDeletedId);
        }

        if let Some(top_entry_id) = stream.entries.keys().next_back().copied()
            && last_id < top_entry_id
        {
            return Err(RequestExecutionError::XsetidSmallerThanTopItem);
        }

        if let Some(entries_added) = options.entries_added
            && entries_added < stream.entries.len() as u64
        {
            return Err(RequestExecutionError::XsetidEntriesAddedSmallerThanLength);
        }

        stream.last_generated_id = last_id;
        if let Some(entries_added) = options.entries_added {
            stream.entries_added = entries_added;
        }
        if let Some(max_deleted_entry_id) = options.max_deleted_entry_id
            && max_deleted_entry_id != StreamId::zero()
        {
            stream.max_deleted_entry_id = max_deleted_entry_id;
        }
        self.save_stream_object(&key, &stream)?;
        self.notify_keyspace_event(NOTIFY_STREAM, b"xsetid", &key);
        append_simple_string(response_out, b"OK");
        Ok(())
    }

    pub(super) fn handle_xinfo(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            2,
            "XINFO",
            "XINFO <subcommand> [<arg> [value] [opt] ...]",
        )?;
        let subcommand = args[1];

        if ascii_eq_ignore_case(subcommand, b"HELP") {
            require_exact_arity(args, 2, "XINFO|HELP", "XINFO HELP")?;
            append_bulk_array(response_out, &XINFO_HELP_LINES);
            return Ok(());
        }

        // All non-HELP subcommands require at least 3 args.
        ensure_min_arity(
            args,
            3,
            "XINFO",
            "XINFO <subcommand> [<arg> [value] [opt] ...]",
        )?;

        if ascii_eq_ignore_case(subcommand, b"GROUPS") {
            require_exact_arity(args, 3, "XINFO", "XINFO GROUPS key")?;
            return self.handle_xinfo_groups(args, response_out);
        }
        if ascii_eq_ignore_case(subcommand, b"CONSUMERS") {
            require_exact_arity(args, 4, "XINFO", "XINFO CONSUMERS key group")?;
            return self.handle_xinfo_consumers(args, response_out);
        }

        if !ascii_eq_ignore_case(subcommand, b"STREAM") {
            return Err(RequestExecutionError::UnknownSubcommand);
        }

        let key = RedisKey::from(args[2]);
        let mut stream = match self.load_stream_object(&key)? {
            Some(stream) => stream,
            None => return Err(RequestExecutionError::NoSuchKey),
        };
        let previous_idmp_tracked = stream.idmp_tracked_count();
        let previous_producer_count = stream.idmp_producers.len();
        stream.expire_idmp_entries(current_unix_time_millis().unwrap_or(0));
        if previous_idmp_tracked != stream.idmp_tracked_count()
            || previous_producer_count != stream.idmp_producers.len()
        {
            self.save_stream_object(&key, &stream)?;
        }

        let full = args.len() >= 4 && ascii_eq_ignore_case(args[3], b"FULL");
        if args.len() > 3 && !full {
            return Err(RequestExecutionError::SyntaxError);
        }

        let mut entry_count_limit = 10usize;
        if full && args.len() >= 6 {
            if args.len() != 6 || !ascii_eq_ignore_case(args[4], b"COUNT") {
                return Err(RequestExecutionError::SyntaxError);
            }
            let parsed = parse_i64_ascii(args[5]).ok_or(RequestExecutionError::ValueNotInteger)?;
            if parsed < 0 {
                entry_count_limit = 10;
            } else if parsed == 0 {
                entry_count_limit = usize::MAX;
            } else {
                entry_count_limit = usize::try_from(parsed).unwrap_or(usize::MAX);
            }
        }
        if full && args.len() > 6 {
            return Err(RequestExecutionError::SyntaxError);
        }

        let resp3 = self.resp_protocol_version().is_resp3();
        let last_generated_id = stream.last_generated_id;
        let first_entry_id = stream
            .entries
            .keys()
            .next()
            .copied()
            .unwrap_or_else(StreamId::zero);
        let entry_count = stream.entries.len();

        if full {
            append_xinfo_stream_full(
                response_out,
                &stream,
                entry_count,
                last_generated_id,
                first_entry_id,
                entry_count_limit,
                resp3,
            );
        } else {
            append_xinfo_stream_summary(
                response_out,
                &stream,
                entry_count,
                last_generated_id,
                first_entry_id,
                resp3,
            );
        }
        Ok(())
    }

    fn handle_xinfo_groups(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        let key = RedisKey::from(args[2]);
        let stream = match self.load_stream_object(&key)? {
            Some(stream) => stream,
            None => return Err(RequestExecutionError::NoSuchKey),
        };

        let resp3 = self.resp_protocol_version().is_resp3();
        append_array_length(response_out, stream.groups.len());
        for (group_name, group_state) in &stream.groups {
            // Valkey emits 6-field map per group.
            if resp3 {
                append_map_length(response_out, 6);
            } else {
                append_array_length(response_out, 12);
            }
            append_bulk_string(response_out, b"name");
            append_bulk_string(response_out, group_name);
            append_bulk_string(response_out, b"consumers");
            append_integer(response_out, group_state.consumers.len() as i64);
            append_bulk_string(response_out, b"pending");
            append_integer(response_out, group_state.pending.len() as i64);
            append_bulk_string(response_out, b"last-delivered-id");
            append_bulk_string(response_out, &group_state.last_delivered_id.encode());
            append_bulk_string(response_out, b"entries-read");
            append_stream_optional_counter(response_out, group_state.entries_read, resp3);
            append_bulk_string(response_out, b"lag");
            append_stream_optional_counter(
                response_out,
                stream_group_lag(&stream, group_state),
                resp3,
            );
        }
        Ok(())
    }

    fn handle_xinfo_consumers(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        let key = RedisKey::from(args[2]);
        let group = args[3];
        let stream = match self.load_stream_object(&key)? {
            Some(stream) => stream,
            None => return Err(RequestExecutionError::NoSuchKey),
        };
        let Some(group_state) = stream.groups.get(group) else {
            return Err(RequestExecutionError::NoGroup);
        };
        let now_millis = current_unix_time_millis().unwrap_or(0);
        append_array_length(response_out, group_state.consumers.len());
        for (consumer_name, consumer_state) in &group_state.consumers {
            append_array_length(response_out, 8);
            append_bulk_string(response_out, b"name");
            append_bulk_string(response_out, consumer_name);
            append_bulk_string(response_out, b"pending");
            append_integer(response_out, consumer_state.pending.len() as i64);
            append_bulk_string(response_out, b"idle");
            append_integer(
                response_out,
                now_millis.saturating_sub(consumer_state.seen_time_millis) as i64,
            );
            append_bulk_string(response_out, b"inactive");
            if let Some(active_time_millis) = consumer_state.active_time_millis {
                append_integer(
                    response_out,
                    now_millis.saturating_sub(active_time_millis) as i64,
                );
            } else {
                append_integer(response_out, -1);
            }
        }
        Ok(())
    }

    pub(super) fn handle_xlen(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 2, "XLEN", "XLEN key")?;
        let key = RedisKey::from(args[1]);
        let count = self
            .load_stream_object(&key)?
            .map_or(0usize, |stream| stream.entries.len());
        append_integer(response_out, count as i64);
        Ok(())
    }

    pub(super) fn handle_xrange(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_one_of_arities(
            args,
            &[4, 6],
            "XRANGE",
            "XRANGE key start end [COUNT count]",
        )?;
        let key = RedisKey::from(args[1]);
        let start = args[2];
        let end = args[3];
        let count = parse_stream_count_option(args)?;
        if count == 0 {
            append_array_length(response_out, 0);
            return Ok(());
        }

        let stream = match self.load_stream_object(&key)? {
            Some(stream) => stream,
            None => {
                append_array_length(response_out, 0);
                return Ok(());
            }
        };

        let lower_bound = parse_stream_range_lower_bound(start)?;
        let upper_bound = parse_stream_range_upper_bound(end)?;
        let entries = collect_stream_range_entries(&stream, lower_bound, upper_bound, false, count);
        let resp3 = self.resp_protocol_version().is_resp3();
        append_stream_entry_array(response_out, &entries, resp3);
        Ok(())
    }

    pub(super) fn handle_xrevrange(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_one_of_arities(
            args,
            &[4, 6],
            "XREVRANGE",
            "XREVRANGE key end start [COUNT count]",
        )?;
        let key = RedisKey::from(args[1]);
        let end = args[2];
        let start = args[3];
        let count = parse_stream_count_option(args)?;
        if count == 0 {
            append_array_length(response_out, 0);
            return Ok(());
        }

        let stream = match self.load_stream_object(&key)? {
            Some(stream) => stream,
            None => {
                append_array_length(response_out, 0);
                return Ok(());
            }
        };

        let lower_bound = parse_stream_range_lower_bound(start)?;
        let upper_bound = parse_stream_range_upper_bound(end)?;
        let entries = collect_stream_range_entries(&stream, lower_bound, upper_bound, true, count);
        let resp3 = self.resp_protocol_version().is_resp3();
        append_stream_entry_array(response_out, &entries, resp3);
        Ok(())
    }

    pub(super) fn handle_xtrim(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            4,
            "XTRIM",
            "XTRIM key MAXLEN|MINID [=|~] threshold [LIMIT count]",
        )?;
        let key = RedisKey::from(args[1]);
        let spec = parse_xtrim_spec(args)?;
        let mut stream = match self.load_stream_object(&key)? {
            Some(stream) => stream,
            None => {
                append_integer(response_out, 0);
                return Ok(());
            }
        };
        let removed =
            apply_xtrim_spec_to_stream(&mut stream, &spec, self.stream_node_max_entries());

        if removed > 0 {
            if stream.entries.is_empty() && stream.groups.is_empty() {
                let _ = self.object_delete(&key)?;
            } else {
                self.save_stream_object(&key, &stream)?;
            }
            self.notify_keyspace_event(NOTIFY_STREAM, b"xtrim", &key);
        }
        append_integer(response_out, removed as i64);
        Ok(())
    }

    pub(super) fn handle_xcfgset(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            2,
            "XCFGSET",
            "XCFGSET key [IDMP-DURATION <duration>] [IDMP-MAXSIZE <maxsize>]",
        )?;

        let parsed = parse_xcfgset_arguments(args)?;
        let key = RedisKey::from(args[1]);
        let Some(mut stream) = self.load_stream_object(&key)? else {
            return Err(RequestExecutionError::NoSuchKey);
        };

        let mut history_cleared = false;
        if let Some(idmp_duration_seconds) = parsed.idmp_duration_seconds {
            if stream.idmp_duration_seconds != idmp_duration_seconds {
                stream.idmp_duration_seconds = idmp_duration_seconds;
                history_cleared = true;
            }
        }
        if let Some(idmp_maxsize) = parsed.idmp_maxsize {
            if stream.idmp_maxsize != idmp_maxsize {
                stream.idmp_maxsize = idmp_maxsize;
                history_cleared = true;
            }
        }
        if history_cleared {
            stream.clear_idmp_history();
        }

        self.save_stream_object(&key, &stream)?;
        append_simple_string(response_out, b"OK");
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum XaddIdSpec {
    Auto,
    AutoSequenceForTimestamp(u64),
    Explicit(StreamId),
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct XaddArguments {
    trim_spec: Option<XtrimSpec>,
    nomkstream: bool,
    idmp_spec: Option<XaddIdmpSpec>,
    id_spec: XaddIdSpec,
    field_start_index: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct XaddIdmpSpec {
    producer_id: Vec<u8>,
    iid: XaddIdmpIidSpec,
}

impl XaddIdmpSpec {
    fn resolve_iid(&self, fields: &[(Vec<u8>, Vec<u8>)]) -> Vec<u8> {
        match &self.iid {
            XaddIdmpIidSpec::Explicit(iid) => iid.clone(),
            XaddIdmpIidSpec::Auto => canonicalize_xadd_idmpauto_iid(fields),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum XaddIdmpIidSpec {
    Explicit(Vec<u8>),
    Auto,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ResolvedXaddIdmpSpec {
    producer_id: Vec<u8>,
    iid: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct XcfgsetArguments {
    idmp_duration_seconds: Option<u64>,
    idmp_maxsize: Option<u64>,
}

fn parse_xadd_arguments(args: &[&[u8]]) -> Result<XaddArguments, RequestExecutionError> {
    ensure_min_arity(args, 5, "XADD", "XADD key id field value [field value ...]")?;

    let mut trim_strategy = None::<XtrimStrategy>;
    let mut trim_approx = false;
    let mut trim_limit = None::<usize>;
    let mut delete_strategy = None::<StreamDeleteStrategy>;
    let mut nomkstream = false;
    let mut idmp_spec = None::<XaddIdmpSpec>;
    let mut index = 2usize;
    while index < args.len() {
        let token = args[index];
        if ascii_eq_ignore_case(token, b"NOMKSTREAM") {
            nomkstream = true;
            index += 1;
            continue;
        }
        if ascii_eq_ignore_case(token, b"IDMPAUTO") {
            if idmp_spec.is_some() {
                return Err(RequestExecutionError::IdmpSpecifiedMultipleTimes);
            }
            let Some(producer_id) = args.get(index + 1) else {
                return Err(RequestExecutionError::SyntaxError);
            };
            if producer_id.is_empty() {
                return Err(RequestExecutionError::IdmpAutoRequiresNonEmptyProducerId);
            }
            idmp_spec = Some(XaddIdmpSpec {
                producer_id: producer_id.to_vec(),
                iid: XaddIdmpIidSpec::Auto,
            });
            index += 2;
            continue;
        }
        if ascii_eq_ignore_case(token, b"IDMP") {
            if idmp_spec.is_some() {
                return Err(RequestExecutionError::IdmpSpecifiedMultipleTimes);
            }
            let Some(producer_id) = args.get(index + 1) else {
                return Err(RequestExecutionError::SyntaxError);
            };
            let Some(iid) = args.get(index + 2) else {
                return Err(RequestExecutionError::SyntaxError);
            };
            if producer_id.is_empty() {
                return Err(RequestExecutionError::IdmpRequiresNonEmptyProducerId);
            }
            if iid.is_empty() {
                return Err(RequestExecutionError::IdmpRequiresNonEmptyIid);
            }
            idmp_spec = Some(XaddIdmpSpec {
                producer_id: producer_id.to_vec(),
                iid: XaddIdmpIidSpec::Explicit(iid.to_vec()),
            });
            index += 3;
            continue;
        }
        if ascii_eq_ignore_case(token, b"KEEPREF")
            || ascii_eq_ignore_case(token, b"DELREF")
            || ascii_eq_ignore_case(token, b"ACKED")
        {
            if delete_strategy.is_some() {
                return Err(RequestExecutionError::SyntaxError);
            }
            delete_strategy = Some(parse_stream_delete_strategy(token)?);
            index += 1;
            continue;
        }
        if ascii_eq_ignore_case(token, b"MAXLEN") || ascii_eq_ignore_case(token, b"MINID") {
            if trim_strategy.is_some() {
                return Err(RequestExecutionError::SyntaxError);
            }
            let (parsed_strategy, parsed_approx, next_index) =
                parse_xadd_trim_strategy(args, index)?;
            trim_strategy = Some(parsed_strategy);
            trim_approx = parsed_approx;
            index = next_index;
            continue;
        }
        if ascii_eq_ignore_case(token, b"LIMIT") {
            if trim_limit.is_some() || index + 1 >= args.len() {
                return Err(RequestExecutionError::SyntaxError);
            }
            let parsed =
                parse_i64_ascii(args[index + 1]).ok_or(RequestExecutionError::ValueNotInteger)?;
            if parsed < 0 {
                return Err(RequestExecutionError::ValueOutOfRange);
            }
            trim_limit =
                Some(usize::try_from(parsed).map_err(|_| RequestExecutionError::ValueOutOfRange)?);
            index += 2;
            continue;
        }
        break;
    }

    if trim_limit.is_some() && trim_strategy.is_none() {
        return Err(RequestExecutionError::SyntaxError);
    }
    if trim_limit.is_some() && !trim_approx {
        return Err(RequestExecutionError::SyntaxError);
    }

    if index >= args.len() {
        return Err(RequestExecutionError::WrongArity {
            command: "XADD",
            expected: "XADD key id field value [field value ...]",
        });
    }

    let id_spec = parse_xadd_id_spec(args[index])?;
    if idmp_spec.is_some() && !matches!(id_spec, XaddIdSpec::Auto) {
        return Err(RequestExecutionError::IdmpRequiresAutoGeneratedId);
    }
    let field_start_index = index + 1;
    let field_arg_count = args.len().saturating_sub(field_start_index);
    if field_arg_count == 0 || !field_arg_count.is_multiple_of(2) {
        return Err(RequestExecutionError::WrongArity {
            command: "XADD",
            expected: "XADD key id field value [field value ...]",
        });
    }

    let trim_spec = trim_strategy.map(|strategy| XtrimSpec {
        strategy,
        approx: trim_approx,
        limit: trim_limit,
        delete_strategy: delete_strategy.unwrap_or(StreamDeleteStrategy::KeepRef),
    });

    Ok(XaddArguments {
        trim_spec,
        nomkstream,
        idmp_spec,
        id_spec,
        field_start_index,
    })
}

fn parse_xadd_trim_strategy(
    args: &[&[u8]],
    start_index: usize,
) -> Result<(XtrimStrategy, bool, usize), RequestExecutionError> {
    let strategy_token = args[start_index];
    let mut index = start_index + 1;
    let mut approx = false;
    if index >= args.len() {
        return Err(RequestExecutionError::SyntaxError);
    }
    if args[index] == b"=" || args[index] == b"~" {
        approx = args[index] == b"~";
        index += 1;
    }
    if index >= args.len() {
        return Err(RequestExecutionError::SyntaxError);
    }
    let threshold = args[index];
    index += 1;

    let strategy = if ascii_eq_ignore_case(strategy_token, b"MAXLEN") {
        let parsed = parse_i64_ascii(threshold).ok_or(RequestExecutionError::ValueNotInteger)?;
        if parsed < 0 {
            return Err(RequestExecutionError::ValueOutOfRange);
        }
        XtrimStrategy::Maxlen(
            usize::try_from(parsed).map_err(|_| RequestExecutionError::ValueOutOfRange)?,
        )
    } else if ascii_eq_ignore_case(strategy_token, b"MINID") {
        XtrimStrategy::Minid(
            StreamId::parse_with_missing_sequence(threshold, 0)
                .ok_or(RequestExecutionError::InvalidStreamId)?,
        )
    } else {
        return Err(RequestExecutionError::SyntaxError);
    };

    Ok((strategy, approx, index))
}

fn parse_xadd_id_spec(raw_id: &[u8]) -> Result<XaddIdSpec, RequestExecutionError> {
    if raw_id == b"*" {
        return Ok(XaddIdSpec::Auto);
    }
    if let Some(timestamp_text) = raw_id.strip_suffix(b"-*") {
        let timestamp_millis =
            parse_u64_ascii(timestamp_text).ok_or(RequestExecutionError::InvalidStreamId)?;
        return Ok(XaddIdSpec::AutoSequenceForTimestamp(timestamp_millis));
    }
    let explicit_id = StreamId::parse(raw_id).ok_or(RequestExecutionError::InvalidStreamId)?;
    Ok(XaddIdSpec::Explicit(explicit_id))
}

fn resolve_xadd_id(
    stream: &StreamObject,
    id_spec: XaddIdSpec,
) -> Result<StreamId, RequestExecutionError> {
    match id_spec {
        XaddIdSpec::Auto => next_auto_stream_id(stream),
        XaddIdSpec::AutoSequenceForTimestamp(timestamp_millis) => {
            resolve_xadd_auto_sequence_id(stream, timestamp_millis)
        }
        XaddIdSpec::Explicit(id) => {
            if id == StreamId::zero() {
                return Err(RequestExecutionError::XaddIdEqualOrSmallerThanTopItem);
            }
            if id <= stream.last_generated_id {
                return Err(RequestExecutionError::XaddIdEqualOrSmallerThanTopItem);
            }
            Ok(id)
        }
    }
}

fn resolve_xadd_auto_sequence_id(
    stream: &StreamObject,
    timestamp_millis: u64,
) -> Result<StreamId, RequestExecutionError> {
    if timestamp_millis < stream.last_generated_id.timestamp_millis() {
        return Err(RequestExecutionError::XaddIdEqualOrSmallerThanTopItem);
    }
    if timestamp_millis == stream.last_generated_id.timestamp_millis() {
        let Some(sequence) = stream.last_generated_id.sequence().checked_add(1) else {
            return Err(RequestExecutionError::XaddIdEqualOrSmallerThanTopItem);
        };
        return Ok(StreamId::new(timestamp_millis, sequence));
    }
    Ok(StreamId::new(timestamp_millis, 0))
}

fn canonicalize_xadd_idmpauto_iid(fields: &[(Vec<u8>, Vec<u8>)]) -> Vec<u8> {
    let mut sorted_fields = fields.to_vec();
    sorted_fields.sort_unstable();

    let mut encoded = Vec::new();
    for (field, value) in sorted_fields {
        encoded.extend_from_slice(&(field.len() as u32).to_le_bytes());
        encoded.extend_from_slice(&field);
        encoded.extend_from_slice(&(value.len() as u32).to_le_bytes());
        encoded.extend_from_slice(&value);
    }
    encoded
}

fn parse_xcfgset_arguments(args: &[&[u8]]) -> Result<XcfgsetArguments, RequestExecutionError> {
    if args.len() == 2 {
        return Err(RequestExecutionError::XcfgsetRequiresAtLeastOneParameter);
    }

    let mut parsed = XcfgsetArguments::default();
    let mut index = 2usize;
    while index < args.len() {
        let Some(value) = args.get(index + 1) else {
            return Err(RequestExecutionError::SyntaxError);
        };
        if ascii_eq_ignore_case(args[index], b"IDMP-DURATION") {
            if parsed.idmp_duration_seconds.is_some() {
                return Err(RequestExecutionError::XcfgsetDurationSpecifiedMultipleTimes);
            }
            let duration = parse_xcfgset_positive_integer(value)?;
            if !(1..=86_400).contains(&duration) {
                return Err(RequestExecutionError::IdmpDurationMustBeBetween);
            }
            parsed.idmp_duration_seconds = Some(duration);
            index += 2;
            continue;
        }
        if ascii_eq_ignore_case(args[index], b"IDMP-MAXSIZE") {
            if parsed.idmp_maxsize.is_some() {
                return Err(RequestExecutionError::XcfgsetMaxsizeSpecifiedMultipleTimes);
            }
            let maxsize = parse_xcfgset_positive_integer(value)?;
            if !(1..=10_000).contains(&maxsize) {
                return Err(RequestExecutionError::IdmpMaxsizeMustBeBetween);
            }
            parsed.idmp_maxsize = Some(maxsize);
            index += 2;
            continue;
        }
        return Err(RequestExecutionError::SyntaxError);
    }

    if parsed.idmp_duration_seconds.is_none() && parsed.idmp_maxsize.is_none() {
        return Err(RequestExecutionError::XcfgsetRequiresAtLeastOneParameter);
    }
    Ok(parsed)
}

fn parse_xcfgset_positive_integer(raw_value: &[u8]) -> Result<u64, RequestExecutionError> {
    if raw_value.is_empty() || raw_value[0] == b'+' {
        return Err(RequestExecutionError::ValueNotInteger);
    }
    if raw_value[0] == b'-' {
        let parsed = parse_i64_ascii(raw_value).ok_or(RequestExecutionError::ValueNotInteger)?;
        if parsed < 0 {
            return Ok(0);
        }
    }
    if raw_value.len() > 1 && raw_value[0] == b'0' {
        return Err(RequestExecutionError::ValueNotInteger);
    }
    parse_u64_ascii(raw_value).ok_or(RequestExecutionError::ValueNotInteger)
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct XsetidOptions {
    entries_added: Option<u64>,
    max_deleted_entry_id: Option<StreamId>,
}

fn parse_xsetid_options(
    args: &[&[u8]],
    start_index: usize,
    last_id: StreamId,
) -> Result<XsetidOptions, RequestExecutionError> {
    let mut options = XsetidOptions::default();
    let mut index = start_index;
    while index < args.len() {
        let token = args[index];
        if ascii_eq_ignore_case(token, b"ENTRIESADDED") {
            if index + 1 >= args.len() {
                return Err(RequestExecutionError::SyntaxError);
            }
            let entries_added =
                parse_i64_ascii(args[index + 1]).ok_or(RequestExecutionError::ValueNotInteger)?;
            if entries_added < 0 {
                return Err(RequestExecutionError::XsetidEntriesAddedMustBePositive);
            }
            options.entries_added = Some(entries_added as u64);
            index += 2;
            continue;
        }
        if ascii_eq_ignore_case(token, b"MAXDELETEDID") {
            if index + 1 >= args.len() {
                return Err(RequestExecutionError::SyntaxError);
            }
            let max_deleted_entry_id =
                StreamId::parse(args[index + 1]).ok_or(RequestExecutionError::InvalidStreamId)?;
            if last_id < max_deleted_entry_id {
                return Err(RequestExecutionError::XsetidSmallerThanProvidedMaxDeletedId);
            }
            options.max_deleted_entry_id = Some(max_deleted_entry_id);
            index += 2;
            continue;
        }
        return Err(RequestExecutionError::SyntaxError);
    }
    // ENTRIESADDED and MAXDELETEDID must be specified together or not at all.
    if options.entries_added.is_some() != options.max_deleted_entry_id.is_some() {
        return Err(RequestExecutionError::SyntaxError);
    }
    Ok(options)
}

fn parse_stream_entries_read(
    raw_entries_read: &[u8],
) -> Result<Option<u64>, RequestExecutionError> {
    let parsed = parse_i64_ascii(raw_entries_read).ok_or(RequestExecutionError::ValueNotInteger)?;
    if parsed == -1 {
        return Ok(None);
    }
    if parsed < 0 {
        return Err(RequestExecutionError::EntriesReadMustBePositiveOrMinusOne);
    }
    Ok(Some(parsed as u64))
}

fn ensure_stream_consumer(
    group_state: &mut StreamGroupState,
    consumer: &[u8],
    now_millis: u64,
) -> bool {
    // TLA+ : EnsureConsumerState
    let consumer_state = group_state.consumers.entry(consumer.to_vec());
    match consumer_state {
        std::collections::btree_map::Entry::Occupied(_) => false,
        std::collections::btree_map::Entry::Vacant(slot) => {
            slot.insert(StreamConsumerState {
                pending: BTreeSet::new(),
                seen_time_millis: now_millis,
                active_time_millis: None,
            });
            true
        }
    }
}

fn delete_stream_consumer(group_state: &mut StreamGroupState, consumer: &[u8]) -> i64 {
    let Some(consumer_state) = group_state.consumers.remove(consumer) else {
        return 0;
    };

    let removed_pending = consumer_state.pending.len() as i64;
    for entry_id in consumer_state.pending {
        let _ = group_state.pending.remove(&entry_id);
    }
    removed_pending
}

fn note_stream_consumer_seen(
    group_state: &mut StreamGroupState,
    consumer: &[u8],
    now_millis: u64,
) -> bool {
    let inserted = ensure_stream_consumer(group_state, consumer, now_millis);
    if let Some(consumer_state) = group_state.consumers.get_mut(consumer) {
        consumer_state.seen_time_millis = now_millis;
    }
    inserted || group_state.consumers.contains_key(consumer)
}

fn note_stream_consumer_active(
    group_state: &mut StreamGroupState,
    consumer: &[u8],
    now_millis: u64,
) -> bool {
    let inserted = note_stream_consumer_seen(group_state, consumer, now_millis);
    if let Some(consumer_state) = group_state.consumers.get_mut(consumer) {
        consumer_state.active_time_millis = Some(now_millis);
    }
    inserted
}

fn claim_stream_pending_entry(
    group_state: &mut StreamGroupState,
    consumer: &[u8],
    entry_id: StreamId,
    delivery_time_millis: u64,
    activity_time_millis: u64,
    retry_count: Option<u64>,
    justid: bool,
    force_if_missing: bool,
) -> bool {
    let consumer_key = consumer.to_vec();
    let (mut pending_entry, previous_consumer, existed) =
        if let Some(existing_pending) = group_state.pending.remove(&entry_id) {
            let previous_consumer = existing_pending.consumer.clone();
            (existing_pending, Some(previous_consumer), true)
        } else {
            if !force_if_missing {
                return false;
            }
            (
                StreamPendingEntry {
                    consumer: consumer_key.clone(),
                    last_delivery_time_millis: delivery_time_millis,
                    delivery_count: retry_count.unwrap_or(if justid { 1 } else { 2 }),
                },
                None,
                false,
            )
        };

    // TLA+ : DeliverPendingOwnership
    if let Some(previous_consumer) = previous_consumer.as_ref()
        && previous_consumer != &consumer_key
        && let Some(previous_consumer_state) = group_state.consumers.get_mut(previous_consumer)
    {
        previous_consumer_state.pending.remove(&entry_id);
    }

    pending_entry.consumer = consumer_key.clone();
    pending_entry.last_delivery_time_millis = delivery_time_millis;
    if let Some(retry_count) = retry_count {
        pending_entry.delivery_count = retry_count;
    } else if existed && !justid {
        pending_entry.delivery_count = pending_entry.delivery_count.saturating_add(1);
    }
    group_state.pending.insert(entry_id, pending_entry);

    let consumer_state = group_state
        .consumers
        .entry(consumer_key)
        .or_insert_with(StreamConsumerState::default);
    consumer_state.pending.insert(entry_id);
    consumer_state.seen_time_millis = activity_time_millis;
    consumer_state.active_time_millis = Some(activity_time_millis);
    true
}

fn record_stream_pending_delivery(
    group_state: &mut StreamGroupState,
    consumer: &[u8],
    entry_id: StreamId,
    now_millis: u64,
) {
    // TLA+ : DeliverPendingOwnership
    let consumer_key = consumer.to_vec();
    let previous_consumer = match group_state.pending.get_mut(&entry_id) {
        Some(pending_entry) => {
            let previous_consumer = pending_entry.consumer.clone();
            pending_entry.consumer = consumer_key.clone();
            pending_entry.last_delivery_time_millis = now_millis;
            pending_entry.delivery_count = 1;
            Some(previous_consumer)
        }
        None => {
            group_state.pending.insert(
                entry_id,
                StreamPendingEntry {
                    consumer: consumer_key.clone(),
                    last_delivery_time_millis: now_millis,
                    delivery_count: 1,
                },
            );
            None
        }
    };

    if let Some(previous_consumer) = previous_consumer
        && previous_consumer != consumer_key
        && let Some(previous_consumer_state) = group_state.consumers.get_mut(&previous_consumer)
    {
        previous_consumer_state.pending.remove(&entry_id);
    }

    let consumer_state = group_state
        .consumers
        .entry(consumer_key)
        .or_insert_with(StreamConsumerState::default);
    consumer_state.pending.insert(entry_id);
    consumer_state.seen_time_millis = now_millis;
    consumer_state.active_time_millis = Some(now_millis);
}

fn ack_stream_pending_entry(group_state: &mut StreamGroupState, entry_id: StreamId) -> bool {
    // TLA+ : AckPendingEntry
    let Some(pending_entry) = group_state.pending.remove(&entry_id) else {
        return false;
    };
    if let Some(consumer_state) = group_state.consumers.get_mut(&pending_entry.consumer) {
        consumer_state.pending.remove(&entry_id);
    }
    true
}

fn stream_pending_idle_millis(pending_entry: &StreamPendingEntry, now_millis: u64) -> u64 {
    now_millis.saturating_sub(pending_entry.last_delivery_time_millis)
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ParsedXreadgroupArguments<'a> {
    group: &'a [u8],
    consumer: &'a [u8],
    count: usize,
    block_millis: Option<u64>,
    noack: bool,
    claim_min_idle_millis: Option<u64>,
    stream_keys: Vec<RedisKey>,
    stream_start_ids: Vec<&'a [u8]>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct XreadgroupResponseEntry {
    id: StreamId,
    fields: Vec<(Vec<u8>, Vec<u8>)>,
    idle_millis: Option<u64>,
    delivery_count: Option<u64>,
}

fn parse_xreadgroup_arguments<'a>(
    args: &[&'a [u8]],
) -> Result<ParsedXreadgroupArguments<'a>, RequestExecutionError> {
    ensure_min_arity(
        args,
        7,
        "XREADGROUP",
        "XREADGROUP GROUP group consumer [NOACK] [COUNT count] STREAMS key >",
    )?;

    let mut group = None::<&[u8]>;
    let mut consumer = None::<&[u8]>;
    let mut count = usize::MAX;
    let mut block_millis = None::<u64>;
    let mut noack = false;
    let mut claim_min_idle_millis = None::<u64>;
    let mut index = 1usize;
    while index < args.len() {
        let token = args[index];
        if ascii_eq_ignore_case(token, b"GROUP") {
            if index + 2 >= args.len() {
                return Err(RequestExecutionError::SyntaxError);
            }
            if is_xreadgroup_option_token(args[index + 2]) {
                return Err(RequestExecutionError::SyntaxError);
            }
            group = Some(args[index + 1]);
            consumer = Some(args[index + 2]);
            index += 3;
            continue;
        }
        if ascii_eq_ignore_case(token, b"COUNT") {
            if index + 1 >= args.len() {
                return Err(RequestExecutionError::SyntaxError);
            }
            let parsed =
                parse_u64_ascii(args[index + 1]).ok_or(RequestExecutionError::ValueNotInteger)?;
            count = usize::try_from(parsed).map_err(|_| RequestExecutionError::ValueOutOfRange)?;
            index += 2;
            continue;
        }
        if ascii_eq_ignore_case(token, b"BLOCK") {
            if index + 1 >= args.len() {
                return Err(RequestExecutionError::SyntaxError);
            }
            block_millis = Some(parse_stream_block_millis(args[index + 1])?);
            index += 2;
            continue;
        }
        if ascii_eq_ignore_case(token, b"NOACK") {
            noack = true;
            index += 1;
            continue;
        }
        if ascii_eq_ignore_case(token, b"CLAIM") {
            if index + 1 >= args.len() {
                return Err(RequestExecutionError::SyntaxError);
            }
            claim_min_idle_millis = Some(parse_xreadgroup_claim_min_idle(args[index + 1])?);
            index += 2;
            continue;
        }
        if ascii_eq_ignore_case(token, b"STREAMS") {
            let trailing = args.len().saturating_sub(index + 1);
            if trailing == 0 {
                return Err(RequestExecutionError::SyntaxError);
            }
            if !trailing.is_multiple_of(2) {
                return Err(RequestExecutionError::UnbalancedXreadgroupStreamList);
            }
            let stream_count = trailing / 2;
            let mut stream_keys = Vec::with_capacity(stream_count);
            let mut stream_start_ids = Vec::with_capacity(stream_count);
            for stream_index in 0..stream_count {
                stream_keys.push(RedisKey::from(args[index + 1 + stream_index]));
                stream_start_ids.push(args[index + 1 + stream_count + stream_index]);
            }
            return Ok(ParsedXreadgroupArguments {
                group: group.ok_or(RequestExecutionError::SyntaxError)?,
                consumer: consumer.ok_or(RequestExecutionError::SyntaxError)?,
                count,
                block_millis,
                noack,
                claim_min_idle_millis,
                stream_keys,
                stream_start_ids,
            });
        }
        return Err(RequestExecutionError::SyntaxError);
    }

    Err(RequestExecutionError::SyntaxError)
}

fn is_xreadgroup_option_token(token: &[u8]) -> bool {
    ascii_eq_ignore_case(token, b"GROUP")
        || ascii_eq_ignore_case(token, b"COUNT")
        || ascii_eq_ignore_case(token, b"BLOCK")
        || ascii_eq_ignore_case(token, b"NOACK")
        || ascii_eq_ignore_case(token, b"CLAIM")
        || ascii_eq_ignore_case(token, b"STREAMS")
}

fn parse_xreadgroup_claim_min_idle(raw_value: &[u8]) -> Result<u64, RequestExecutionError> {
    if raw_value.is_empty() || raw_value.starts_with(b"+") {
        return Err(RequestExecutionError::MinIdleTimeNotInteger);
    }
    if let Some(negative_value) = raw_value.strip_prefix(b"-") {
        if negative_value.is_empty() || !negative_value.iter().all(u8::is_ascii_digit) {
            return Err(RequestExecutionError::MinIdleTimeNotInteger);
        }
        if negative_value.iter().all(|digit| *digit == b'0') {
            return Err(RequestExecutionError::MinIdleTimeNotInteger);
        }
        return Err(RequestExecutionError::MinIdleTimeMustBePositiveInteger);
    }
    parse_u64_ascii(raw_value).ok_or(RequestExecutionError::MinIdleTimeNotInteger)
}

fn collect_stream_consumer_pending_response_entries(
    stream: &StreamObject,
    group_state: &StreamGroupState,
    consumer: &[u8],
    pivot: StreamId,
    count: usize,
) -> Vec<XreadgroupResponseEntry> {
    if count == 0 {
        return Vec::new();
    }

    let Some(consumer_state) = group_state.consumers.get(consumer) else {
        return Vec::new();
    };

    let mut entries = Vec::new();
    for pending_id in &consumer_state.pending {
        if *pending_id <= pivot {
            continue;
        }
        let fields = stream.entries.get(pending_id).cloned().unwrap_or_default();
        entries.push(XreadgroupResponseEntry {
            id: *pending_id,
            fields,
            idle_millis: None,
            delivery_count: None,
        });
        if entries.len() >= count {
            break;
        }
    }
    entries
}

enum XpendingBound {
    UnboundedLow,
    UnboundedHigh,
    Inclusive(StreamId),
    Exclusive(StreamId),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StreamDeleteStrategy {
    KeepRef,
    DelRef,
    Acked,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ParsedStreamAckDeleteArguments {
    delete_strategy: StreamDeleteStrategy,
    entry_ids: Vec<StreamId>,
}

fn parse_stream_ack_delete_arguments(
    args: &[&[u8]],
    start_index: usize,
) -> Result<ParsedStreamAckDeleteArguments, RequestExecutionError> {
    let mut delete_strategy = None::<StreamDeleteStrategy>;
    let mut id_args = None::<&[&[u8]]>;
    let mut index = start_index;
    while index < args.len() {
        let token = args[index];
        if ascii_eq_ignore_case(token, b"KEEPREF") && delete_strategy.is_none() {
            delete_strategy = Some(StreamDeleteStrategy::KeepRef);
            index += 1;
            continue;
        }
        if ascii_eq_ignore_case(token, b"DELREF") && delete_strategy.is_none() {
            delete_strategy = Some(StreamDeleteStrategy::DelRef);
            index += 1;
            continue;
        }
        if ascii_eq_ignore_case(token, b"ACKED") && delete_strategy.is_none() {
            delete_strategy = Some(StreamDeleteStrategy::Acked);
            index += 1;
            continue;
        }
        if ascii_eq_ignore_case(token, b"IDS") {
            if index + 1 >= args.len() {
                return Err(RequestExecutionError::SyntaxError);
            }
            let requested_count = parse_stream_positive_numids(args[index + 1])
                .map_err(|_| RequestExecutionError::NumberOfIdsMustBePositiveInteger)?;
            let ids_start = index + 2;
            let available = args.len().saturating_sub(ids_start);
            if requested_count > available {
                return Err(RequestExecutionError::NumidsParameterMustMatchArguments);
            }
            if requested_count < available {
                return Err(RequestExecutionError::SyntaxError);
            }
            id_args = Some(&args[ids_start..]);
            break;
        }
        return Err(RequestExecutionError::SyntaxError);
    }

    let id_args = id_args.ok_or(RequestExecutionError::SyntaxError)?;
    let mut entry_ids = Vec::with_capacity(id_args.len());
    for id_arg in id_args {
        entry_ids.push(StreamId::parse(id_arg).ok_or(RequestExecutionError::InvalidStreamId)?);
    }
    Ok(ParsedStreamAckDeleteArguments {
        delete_strategy: delete_strategy.unwrap_or(StreamDeleteStrategy::KeepRef),
        entry_ids,
    })
}

fn parse_stream_positive_numids(raw_count: &[u8]) -> Result<usize, ()> {
    let parsed = parse_i64_ascii(raw_count).ok_or(())?;
    if parsed <= 0 {
        return Err(());
    }
    usize::try_from(parsed).map_err(|_| ())
}

fn parse_stream_trim_strategy_token(
    args: &[&[u8]],
    start_index: usize,
) -> Result<(XtrimStrategy, bool, usize), RequestExecutionError> {
    let strategy_token = args[start_index];
    let mut index = start_index + 1;
    let mut approx = false;
    if index >= args.len() {
        return Err(RequestExecutionError::SyntaxError);
    }
    if args[index] == b"=" || args[index] == b"~" {
        approx = args[index] == b"~";
        index += 1;
    }
    if index >= args.len() {
        return Err(RequestExecutionError::SyntaxError);
    }
    let threshold = args[index];
    index += 1;

    let strategy = if ascii_eq_ignore_case(strategy_token, b"MAXLEN") {
        let parsed = parse_i64_ascii(threshold).ok_or(RequestExecutionError::ValueNotInteger)?;
        if parsed < 0 {
            return Err(RequestExecutionError::ValueOutOfRange);
        }
        XtrimStrategy::Maxlen(
            usize::try_from(parsed).map_err(|_| RequestExecutionError::ValueOutOfRange)?,
        )
    } else if ascii_eq_ignore_case(strategy_token, b"MINID") {
        XtrimStrategy::Minid(
            StreamId::parse_with_missing_sequence(threshold, 0)
                .ok_or(RequestExecutionError::InvalidStreamId)?,
        )
    } else {
        return Err(RequestExecutionError::SyntaxError);
    };

    Ok((strategy, approx, index))
}

fn parse_stream_delete_strategy(
    token: &[u8],
) -> Result<StreamDeleteStrategy, RequestExecutionError> {
    if ascii_eq_ignore_case(token, b"KEEPREF") {
        return Ok(StreamDeleteStrategy::KeepRef);
    }
    if ascii_eq_ignore_case(token, b"DELREF") {
        return Ok(StreamDeleteStrategy::DelRef);
    }
    if ascii_eq_ignore_case(token, b"ACKED") {
        return Ok(StreamDeleteStrategy::Acked);
    }
    Err(RequestExecutionError::SyntaxError)
}

impl XpendingBound {
    fn matches_start(&self, entry_id: StreamId) -> bool {
        match self {
            Self::UnboundedLow | Self::UnboundedHigh => true,
            Self::Inclusive(boundary) => entry_id >= *boundary,
            Self::Exclusive(boundary) => entry_id > *boundary,
        }
    }

    fn matches_end(&self, entry_id: StreamId) -> bool {
        match self {
            Self::UnboundedLow | Self::UnboundedHigh => true,
            Self::Inclusive(boundary) => entry_id <= *boundary,
            Self::Exclusive(boundary) => entry_id < *boundary,
        }
    }
}

fn parse_xpending_bound(
    raw_bound: &[u8],
    is_start: bool,
) -> Result<XpendingBound, RequestExecutionError> {
    if raw_bound == b"-" {
        return Ok(XpendingBound::UnboundedLow);
    }
    if raw_bound == b"+" {
        return Ok(XpendingBound::UnboundedHigh);
    }
    if let Some(exclusive_bound) = raw_bound.strip_prefix(b"(") {
        let parsed =
            StreamId::parse(exclusive_bound).ok_or(RequestExecutionError::InvalidStreamId)?;
        return Ok(XpendingBound::Exclusive(parsed));
    }
    let parsed = StreamId::parse(raw_bound).ok_or(RequestExecutionError::InvalidStreamId)?;
    let _ = is_start;
    Ok(XpendingBound::Inclusive(parsed))
}

fn parse_nonnegative_millis_clamped(raw_value: &[u8]) -> Result<u64, RequestExecutionError> {
    let parsed = parse_i64_ascii(raw_value).ok_or(RequestExecutionError::ValueNotInteger)?;
    if parsed <= 0 {
        return Ok(0);
    }
    u64::try_from(parsed).map_err(|_| RequestExecutionError::ValueOutOfRange)
}

fn parse_xautoclaim_start_id(raw_start: &[u8]) -> Result<StreamId, RequestExecutionError> {
    if raw_start == b"-" {
        return Ok(StreamId::zero());
    }
    if let Some(exclusive_start) = raw_start.strip_prefix(b"(") {
        let start =
            StreamId::parse(exclusive_start).ok_or(RequestExecutionError::InvalidStreamId)?;
        return increment_stream_id(start).ok_or(RequestExecutionError::InvalidStreamId);
    }
    StreamId::parse(raw_start).ok_or(RequestExecutionError::InvalidStreamId)
}

fn increment_stream_id(stream_id: StreamId) -> Option<StreamId> {
    if stream_id.sequence() < u64::MAX {
        return Some(StreamId::new(
            stream_id.timestamp_millis(),
            stream_id.sequence().saturating_add(1),
        ));
    }
    let next_millis = stream_id.timestamp_millis().checked_add(1)?;
    Some(StreamId::new(next_millis, 0))
}

fn decrement_stream_id(stream_id: StreamId) -> Option<StreamId> {
    if stream_id.sequence() > 0 {
        return Some(StreamId::new(
            stream_id.timestamp_millis(),
            stream_id.sequence() - 1,
        ));
    }
    let previous_millis = stream_id.timestamp_millis().checked_sub(1)?;
    Some(StreamId::new(previous_millis, u64::MAX))
}

fn remove_stream_entry(stream: &mut StreamObject, entry_id: StreamId) -> bool {
    let Some(entry_rank) = stream.entries.keys().position(|id| *id == entry_id) else {
        return false;
    };
    if stream.entries.remove(&entry_id).is_none() {
        return false;
    }
    stream.remove_entry_at_rank(entry_rank);
    if entry_id > stream.max_deleted_entry_id {
        stream.max_deleted_entry_id = entry_id;
    }
    true
}

fn trim_remove_stream_entry(stream: &mut StreamObject, entry_id: StreamId) -> bool {
    let Some(entry_rank) = stream.entries.keys().position(|id| *id == entry_id) else {
        return false;
    };
    if stream.entries.remove(&entry_id).is_none() {
        return false;
    }
    stream.remove_entry_at_rank(entry_rank);
    true
}

fn cleanup_stream_entry_group_refs(stream: &mut StreamObject, entry_id: StreamId) -> bool {
    let mut changed = false;
    for group_state in stream.groups.values_mut() {
        if let Some(pending_entry) = group_state.pending.remove(&entry_id) {
            if let Some(consumer_state) = group_state.consumers.get_mut(&pending_entry.consumer) {
                consumer_state.pending.remove(&entry_id);
            }
            changed = true;
        }
    }
    changed
}

fn stream_entry_is_referenced(stream: &StreamObject, entry_id: StreamId) -> bool {
    for group_state in stream.groups.values() {
        if group_state.last_delivered_id < entry_id {
            return true;
        }
        if group_state.pending.contains_key(&entry_id) {
            return true;
        }
    }
    false
}

fn delete_stream_entry_for_trim(
    stream: &mut StreamObject,
    entry_id: StreamId,
    delete_strategy: StreamDeleteStrategy,
) -> bool {
    match delete_strategy {
        StreamDeleteStrategy::KeepRef => trim_remove_stream_entry(stream, entry_id),
        StreamDeleteStrategy::DelRef => {
            let _ = cleanup_stream_entry_group_refs(stream, entry_id);
            trim_remove_stream_entry(stream, entry_id)
        }
        StreamDeleteStrategy::Acked => {
            if stream_entry_is_referenced(stream, entry_id) {
                return false;
            }
            trim_remove_stream_entry(stream, entry_id)
        }
    }
}

fn effective_stream_trim_limit(spec: &XtrimSpec, stream_node_max_entries: usize) -> Option<usize> {
    if !spec.approx {
        return None;
    }
    match spec.limit {
        Some(0) => None,
        Some(limit) => Some(limit),
        None => {
            let mut limit = stream_node_max_entries.saturating_mul(100);
            if limit == 0 {
                limit = 10_000;
            }
            Some(limit.min(1_000_000))
        }
    }
}

fn apply_xtrim_spec_to_stream(
    stream: &mut StreamObject,
    spec: &XtrimSpec,
    stream_node_max_entries: usize,
) -> usize {
    stream.ensure_node_sizes(stream_node_max_entries);
    let entry_ids: Vec<StreamId> = stream.entries.keys().copied().collect();
    if entry_ids.is_empty() {
        return 0;
    }

    if !spec.approx {
        let mut removed = 0usize;
        for entry_id in entry_ids {
            let should_stop = match spec.strategy {
                XtrimStrategy::Maxlen(maxlen) => stream.entries.len() <= maxlen,
                XtrimStrategy::Minid(minid) => entry_id >= minid,
            };
            if should_stop {
                break;
            }
            if delete_stream_entry_for_trim(stream, entry_id, spec.delete_strategy) {
                removed += 1;
            }
        }
        return removed;
    }

    let mut removed = 0usize;
    let node_sizes = stream.node_sizes.iter().copied().collect::<Vec<_>>();
    let effective_limit = effective_stream_trim_limit(spec, stream_node_max_entries);
    let mut node_start = 0usize;
    for node_size in node_sizes {
        let node_end = node_start.saturating_add(node_size).min(entry_ids.len());
        let node_entry_ids = &entry_ids[node_start..node_end];
        node_start = node_end;
        if node_entry_ids.is_empty() {
            continue;
        }
        if let Some(limit) = effective_limit
            && removed.saturating_add(node_entry_ids.len()) > limit
        {
            break;
        }

        let node_eligible = match spec.strategy {
            XtrimStrategy::Maxlen(maxlen) => {
                stream.entries.len().saturating_sub(node_entry_ids.len()) >= maxlen
            }
            XtrimStrategy::Minid(minid) => *node_entry_ids.last().unwrap() < minid,
        };
        if !node_eligible {
            break;
        }

        if spec.delete_strategy == StreamDeleteStrategy::KeepRef {
            for entry_id in node_entry_ids {
                if trim_remove_stream_entry(stream, *entry_id) {
                    removed += 1;
                }
            }
            continue;
        }

        for entry_id in node_entry_ids {
            if delete_stream_entry_for_trim(stream, *entry_id, spec.delete_strategy) {
                removed += 1;
            }
        }
    }
    removed
}

fn stream_group_lag(stream: &StreamObject, group_state: &StreamGroupState) -> Option<u64> {
    stream_group_lag_from_snapshot(&StreamLagSnapshot::new(stream), group_state)
}

fn append_stream_optional_counter(response_out: &mut Vec<u8>, value: Option<u64>, resp3: bool) {
    if let Some(value) = value {
        append_integer(response_out, value.min(i64::MAX as u64) as i64);
    } else if resp3 {
        append_null(response_out);
    } else {
        append_null_bulk_string(response_out);
    }
}

fn next_auto_stream_id(stream: &StreamObject) -> Result<StreamId, RequestExecutionError> {
    let now = current_unix_time_millis().unwrap_or(0);
    let last_id = stream.last_generated_id;
    if last_id == StreamId::zero() {
        return Ok(StreamId::new(now, 0));
    }
    if last_id.timestamp_millis() > now {
        if last_id.sequence() == u64::MAX {
            let next_millis = last_id
                .timestamp_millis()
                .checked_add(1)
                .ok_or(RequestExecutionError::ValueOutOfRange)?;
            return Ok(StreamId::new(next_millis, 0));
        }
        return Ok(StreamId::new(
            last_id.timestamp_millis(),
            last_id.sequence() + 1,
        ));
    }
    if last_id.timestamp_millis() == now {
        if last_id.sequence() == u64::MAX {
            let next_millis = now
                .checked_add(1)
                .ok_or(RequestExecutionError::ValueOutOfRange)?;
            return Ok(StreamId::new(next_millis, 0));
        }
        return Ok(StreamId::new(now, last_id.sequence() + 1));
    }
    Ok(StreamId::new(now, 0))
}

#[allow(clippy::type_complexity)]
fn collect_stream_entries_for_xread(
    stream: &StreamObject,
    selection: BlockingXreadWaitSelection,
    count: usize,
) -> Vec<(StreamId, Vec<(Vec<u8>, Vec<u8>)>)> {
    match selection {
        BlockingXreadWaitSelection::After(pivot) => {
            collect_stream_entries_after_id(stream, pivot, count)
        }
        BlockingXreadWaitSelection::LastEntry => collect_last_stream_entry(stream, count),
    }
}

#[allow(clippy::type_complexity)]
fn collect_stream_entries_after_id(
    stream: &StreamObject,
    pivot: StreamId,
    count: usize,
) -> Vec<(StreamId, Vec<(Vec<u8>, Vec<u8>)>)> {
    if count == 0 {
        return Vec::new();
    }

    let mut selected = Vec::new();
    for (id, fields) in &stream.entries {
        if *id <= pivot {
            continue;
        }
        selected.push((*id, fields.clone()));
        if selected.len() >= count {
            break;
        }
    }
    selected
}

#[allow(clippy::type_complexity)]
fn collect_last_stream_entry(
    stream: &StreamObject,
    count: usize,
) -> Vec<(StreamId, Vec<(Vec<u8>, Vec<u8>)>)> {
    if count == 0 {
        return Vec::new();
    }
    let Some((id, fields)) = stream.entries.iter().next_back() else {
        return Vec::new();
    };
    vec![(*id, fields.clone())]
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct StreamLagSnapshot {
    first_entry_id: StreamId,
    last_generated_id: StreamId,
    max_deleted_entry_id: StreamId,
    entries_added: u64,
    length: usize,
}

impl StreamLagSnapshot {
    fn new(stream: &StreamObject) -> Self {
        Self {
            first_entry_id: stream
                .entries
                .keys()
                .next()
                .copied()
                .unwrap_or_else(StreamId::zero),
            last_generated_id: stream.last_generated_id,
            max_deleted_entry_id: stream.max_deleted_entry_id,
            entries_added: stream.entries_added,
            length: stream.entries.len(),
        }
    }
}

fn stream_range_has_tombstones(
    snapshot: &StreamLagSnapshot,
    start: Option<StreamId>,
    end: Option<StreamId>,
) -> bool {
    if snapshot.length == 0 || snapshot.max_deleted_entry_id == StreamId::zero() {
        return false;
    }
    let start_id = start.unwrap_or_else(StreamId::zero);
    let end_id = end.unwrap_or_else(|| StreamId::new(u64::MAX, u64::MAX));
    start_id <= snapshot.max_deleted_entry_id && snapshot.max_deleted_entry_id <= end_id
}

fn stream_estimate_entries_read(snapshot: &StreamLagSnapshot, entry_id: StreamId) -> Option<u64> {
    if snapshot.entries_added == 0 {
        return Some(0);
    }

    if snapshot.length == 0 && entry_id <= snapshot.last_generated_id {
        return Some(snapshot.entries_added);
    }

    if entry_id != StreamId::zero() && entry_id < snapshot.max_deleted_entry_id {
        return None;
    }

    if entry_id == snapshot.last_generated_id {
        return Some(snapshot.entries_added);
    }
    if entry_id > snapshot.last_generated_id {
        return None;
    }

    let tombstones_before_first = snapshot.max_deleted_entry_id == StreamId::zero()
        || snapshot.max_deleted_entry_id < snapshot.first_entry_id;
    if tombstones_before_first {
        if entry_id < snapshot.first_entry_id {
            return Some(
                snapshot
                    .entries_added
                    .saturating_sub(snapshot.length as u64),
            );
        }
        if entry_id == snapshot.first_entry_id {
            return Some(
                snapshot
                    .entries_added
                    .saturating_sub(snapshot.length as u64)
                    .saturating_add(1),
            );
        }
    }

    None
}

fn stream_group_lag_from_snapshot(
    snapshot: &StreamLagSnapshot,
    group_state: &StreamGroupState,
) -> Option<u64> {
    if snapshot.entries_added == 0 || snapshot.length == 0 {
        return Some(0);
    }

    if group_state.last_delivered_id < snapshot.first_entry_id
        && snapshot.max_deleted_entry_id < snapshot.first_entry_id
    {
        return Some(snapshot.length as u64);
    }

    if let Some(entries_read) = group_state.entries_read
        && !stream_range_has_tombstones(snapshot, Some(group_state.last_delivered_id), None)
    {
        return Some(snapshot.entries_added.saturating_sub(entries_read));
    }

    let estimated_entries_read =
        stream_estimate_entries_read(snapshot, group_state.last_delivered_id)?;
    Some(
        snapshot
            .entries_added
            .saturating_sub(estimated_entries_read),
    )
}

fn update_stream_group_progress_after_delivery(
    snapshot: &StreamLagSnapshot,
    group_state: &mut StreamGroupState,
    entry_id: StreamId,
) {
    if entry_id <= group_state.last_delivered_id {
        return;
    }

    if let Some(entries_read) = group_state.entries_read
        && group_state.last_delivered_id >= snapshot.first_entry_id
        && !stream_range_has_tombstones(snapshot, Some(group_state.last_delivered_id), None)
    {
        group_state.entries_read = Some(entries_read.saturating_add(1));
    } else if snapshot.entries_added > 0 {
        group_state.entries_read = stream_estimate_entries_read(snapshot, entry_id);
    }

    group_state.last_delivered_id = entry_id;
}

fn claim_xreadgroup_pending_entries_for_stream(
    stream: &mut StreamObject,
    group: &[u8],
    consumer: &[u8],
    min_idle_millis: u64,
    now_millis: u64,
    count: usize,
    noack: bool,
    stream_changed: &mut bool,
) -> Result<Vec<XreadgroupResponseEntry>, RequestExecutionError> {
    let candidate_ids = {
        let group_state = stream
            .groups
            .get(group)
            .ok_or(RequestExecutionError::NoGroup)?;
        group_state.pending.keys().copied().collect::<Vec<_>>()
    };

    let mut response_entries = Vec::new();
    for entry_id in candidate_ids {
        if response_entries.len() >= count {
            break;
        }

        let entry_fields = stream.entries.get(&entry_id).cloned();
        let Some(group_state) = stream.groups.get_mut(group) else {
            return Err(RequestExecutionError::NoGroup);
        };
        let Some(existing_pending) = group_state.pending.get(&entry_id).cloned() else {
            continue;
        };

        if stream_pending_idle_millis(&existing_pending, now_millis) < min_idle_millis {
            continue;
        }

        if entry_fields.is_none() {
            if ack_stream_pending_entry(group_state, entry_id) {
                *stream_changed = true;
            }
            continue;
        }

        let idle_millis = stream_pending_idle_millis(&existing_pending, now_millis);
        if noack {
            let _ = note_stream_consumer_active(group_state, consumer, now_millis);
            *stream_changed = true;
        } else if claim_stream_pending_entry(
            group_state,
            consumer,
            entry_id,
            now_millis,
            now_millis,
            None,
            false,
            false,
        ) {
            *stream_changed = true;
        }

        response_entries.push(XreadgroupResponseEntry {
            id: entry_id,
            fields: entry_fields.unwrap_or_default(),
            idle_millis: Some(idle_millis),
            delivery_count: Some(existing_pending.delivery_count),
        });
    }

    Ok(response_entries)
}

fn deliver_xreadgroup_new_entries_for_stream(
    stream: &mut StreamObject,
    group: &[u8],
    consumer: &[u8],
    now_millis: u64,
    count: usize,
    noack: bool,
    include_claim_metadata: bool,
    stream_changed: &mut bool,
) -> Result<Vec<XreadgroupResponseEntry>, RequestExecutionError> {
    let selected = {
        let group_state = stream
            .groups
            .get(group)
            .ok_or(RequestExecutionError::NoGroup)?;
        collect_stream_entries_after_id(stream, group_state.last_delivered_id, count)
    };

    if selected.is_empty() {
        return Ok(Vec::new());
    }

    let lag_snapshot = StreamLagSnapshot::new(stream);
    let Some(group_state) = stream.groups.get_mut(group) else {
        return Err(RequestExecutionError::NoGroup);
    };
    for (entry_id, _) in &selected {
        // TLA+ : AdvanceLastDeliveredId
        update_stream_group_progress_after_delivery(&lag_snapshot, group_state, *entry_id);
    }
    if noack {
        let _ = note_stream_consumer_active(group_state, consumer, now_millis);
    } else {
        for (entry_id, _) in &selected {
            record_stream_pending_delivery(group_state, consumer, *entry_id, now_millis);
        }
    }
    *stream_changed = true;

    Ok(selected
        .into_iter()
        .map(|(entry_id, fields)| XreadgroupResponseEntry {
            id: entry_id,
            fields,
            idle_millis: include_claim_metadata.then_some(0),
            delivery_count: include_claim_metadata.then_some(0),
        })
        .collect())
}

fn append_xreadgroup_response(
    response_out: &mut Vec<u8>,
    stream_results: &[(RedisKey, Vec<XreadgroupResponseEntry>)],
    resp3: bool,
) {
    if resp3 {
        append_map_length(response_out, stream_results.len());
    } else {
        append_array_length(response_out, stream_results.len());
    }
    for (key, entries) in stream_results {
        if !resp3 {
            append_array_length(response_out, 2);
        }
        append_bulk_string(response_out, key.as_slice());
        append_xreadgroup_entry_array(response_out, entries, resp3);
    }
}

fn append_xreadgroup_entry_array(
    response_out: &mut Vec<u8>,
    entries: &[XreadgroupResponseEntry],
    resp3: bool,
) {
    append_array_length(response_out, entries.len());
    for entry in entries {
        if let (Some(idle_millis), Some(delivery_count)) = (entry.idle_millis, entry.delivery_count)
        {
            append_array_length(response_out, 4);
            append_bulk_string(response_out, &entry.id.encode());
            append_stream_entry_fields(response_out, &entry.fields, resp3);
            append_integer(response_out, idle_millis.min(i64::MAX as u64) as i64);
            append_integer(response_out, delivery_count.min(i64::MAX as u64) as i64);
        } else {
            append_array_length(response_out, 2);
            append_bulk_string(response_out, &entry.id.encode());
            append_stream_entry_fields(response_out, &entry.fields, resp3);
        }
    }
}

fn parse_stream_block_millis(raw_timeout: &[u8]) -> Result<u64, RequestExecutionError> {
    let Some(timeout) = parse_i64_ascii(raw_timeout) else {
        return Err(RequestExecutionError::TimeoutNotIntegerOrOutOfRange);
    };
    if timeout < 0 {
        return Err(RequestExecutionError::TimeoutIsNegative);
    }
    u64::try_from(timeout).map_err(|_| RequestExecutionError::TimeoutNotIntegerOrOutOfRange)
}

fn is_xclaim_option_token(token: &[u8]) -> bool {
    ascii_eq_ignore_case(token, b"IDLE")
        || ascii_eq_ignore_case(token, b"TIME")
        || ascii_eq_ignore_case(token, b"RETRYCOUNT")
        || ascii_eq_ignore_case(token, b"FORCE")
        || ascii_eq_ignore_case(token, b"JUSTID")
        || ascii_eq_ignore_case(token, b"LASTID")
}

fn parse_stream_count_option(args: &[&[u8]]) -> Result<usize, RequestExecutionError> {
    if args.len() == 4 {
        return Ok(usize::MAX);
    }
    let option = args[4];
    if !ascii_eq_ignore_case(option, b"COUNT") {
        return Err(RequestExecutionError::SyntaxError);
    }
    let parsed = parse_u64_ascii(args[5]).ok_or(RequestExecutionError::ValueNotInteger)?;
    if parsed > usize::MAX as u64 {
        return Err(RequestExecutionError::ValueOutOfRange);
    }
    Ok(parsed as usize)
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum XtrimStrategy {
    Maxlen(usize),
    Minid(StreamId),
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct XtrimSpec {
    strategy: XtrimStrategy,
    approx: bool,
    limit: Option<usize>,
    delete_strategy: StreamDeleteStrategy,
}

fn parse_xtrim_spec(args: &[&[u8]]) -> Result<XtrimSpec, RequestExecutionError> {
    let mut strategy = None::<XtrimStrategy>;
    let mut approx = false;
    let mut limit = None::<usize>;
    let mut delete_strategy = None::<StreamDeleteStrategy>;
    let mut index = 2usize;
    while index < args.len() {
        let token = args[index];
        if ascii_eq_ignore_case(token, b"MAXLEN") || ascii_eq_ignore_case(token, b"MINID") {
            if strategy.is_some() {
                return Err(RequestExecutionError::SyntaxError);
            }
            let (parsed_strategy, parsed_approx, next_index) =
                parse_stream_trim_strategy_token(args, index)?;
            strategy = Some(parsed_strategy);
            approx = parsed_approx;
            index = next_index;
            continue;
        }
        if ascii_eq_ignore_case(token, b"LIMIT") {
            if limit.is_some() || index + 1 >= args.len() {
                return Err(RequestExecutionError::SyntaxError);
            }
            let parsed =
                parse_i64_ascii(args[index + 1]).ok_or(RequestExecutionError::ValueNotInteger)?;
            if parsed < 0 {
                return Err(RequestExecutionError::ValueOutOfRange);
            }
            limit =
                Some(usize::try_from(parsed).map_err(|_| RequestExecutionError::ValueOutOfRange)?);
            index += 2;
            continue;
        }
        if ascii_eq_ignore_case(token, b"KEEPREF")
            || ascii_eq_ignore_case(token, b"DELREF")
            || ascii_eq_ignore_case(token, b"ACKED")
        {
            if delete_strategy.is_some() {
                return Err(RequestExecutionError::SyntaxError);
            }
            delete_strategy = Some(parse_stream_delete_strategy(token)?);
            index += 1;
            continue;
        }
        return Err(RequestExecutionError::SyntaxError);
    }

    let Some(strategy) = strategy else {
        return Err(RequestExecutionError::SyntaxError);
    };
    if limit.is_some() && !approx {
        return Err(RequestExecutionError::SyntaxError);
    }

    Ok(XtrimSpec {
        strategy,
        approx,
        limit,
        delete_strategy: delete_strategy.unwrap_or(StreamDeleteStrategy::KeepRef),
    })
}

fn parse_stream_range_lower_bound(raw_bound: &[u8]) -> Result<StreamId, RequestExecutionError> {
    if raw_bound == b"-" {
        return Ok(StreamId::zero());
    }
    if raw_bound == b"+" {
        return Ok(StreamId::new(u64::MAX, u64::MAX));
    }
    if let Some(exclusive_bound) = raw_bound.strip_prefix(b"(") {
        let bound = StreamId::parse_with_missing_sequence(exclusive_bound, 0)
            .ok_or(RequestExecutionError::InvalidStreamId)?;
        return increment_stream_id(bound).ok_or(RequestExecutionError::InvalidStreamId);
    }
    StreamId::parse_with_missing_sequence(raw_bound, 0)
        .ok_or(RequestExecutionError::InvalidStreamId)
}

fn parse_stream_range_upper_bound(raw_bound: &[u8]) -> Result<StreamId, RequestExecutionError> {
    if raw_bound == b"+" {
        return Ok(StreamId::new(u64::MAX, u64::MAX));
    }
    if raw_bound == b"-" {
        return Ok(StreamId::zero());
    }
    if let Some(exclusive_bound) = raw_bound.strip_prefix(b"(") {
        let bound = StreamId::parse_with_missing_sequence(exclusive_bound, u64::MAX)
            .ok_or(RequestExecutionError::InvalidStreamId)?;
        return decrement_stream_id(bound).ok_or(RequestExecutionError::InvalidStreamId);
    }
    StreamId::parse_with_missing_sequence(raw_bound, u64::MAX)
        .ok_or(RequestExecutionError::InvalidStreamId)
}

#[allow(clippy::type_complexity)]
fn collect_stream_range_entries(
    stream: &StreamObject,
    lower_bound: StreamId,
    upper_bound: StreamId,
    reverse: bool,
    count: usize,
) -> Vec<(StreamId, Vec<(Vec<u8>, Vec<u8>)>)> {
    if lower_bound > upper_bound {
        return Vec::new();
    }
    let mut selected = Vec::new();
    if reverse {
        for (id, fields) in stream.entries.iter().rev() {
            if *id < lower_bound {
                continue;
            }
            if *id > upper_bound {
                continue;
            }
            selected.push((*id, fields.clone()));
            if selected.len() >= count {
                break;
            }
        }
    } else {
        for (id, fields) in &stream.entries {
            if *id < lower_bound {
                continue;
            }
            if *id > upper_bound {
                continue;
            }
            selected.push((*id, fields.clone()));
            if selected.len() >= count {
                break;
            }
        }
    }
    selected
}

#[allow(clippy::type_complexity)]
fn append_stream_entry_array(
    response_out: &mut Vec<u8>,
    entries: &[(StreamId, Vec<(Vec<u8>, Vec<u8>)>)],
    resp3: bool,
) {
    append_array_length(response_out, entries.len());
    for (id, fields) in entries {
        append_array_length(response_out, 2);
        append_bulk_string(response_out, &id.encode());
        if resp3 {
            append_map_length(response_out, fields.len());
        } else {
            append_array_length(response_out, fields.len() * 2);
        }
        for (field, value) in fields {
            append_bulk_string(response_out, field);
            append_bulk_string(response_out, value);
        }
    }
}

fn append_stream_id_array(response_out: &mut Vec<u8>, entry_ids: &[StreamId]) {
    append_array_length(response_out, entry_ids.len());
    for entry_id in entry_ids {
        append_bulk_string(response_out, &entry_id.encode());
    }
}

/// Emit a single stream entry's field-value pairs.
fn append_stream_entry_fields(
    response_out: &mut Vec<u8>,
    fields: &[(Vec<u8>, Vec<u8>)],
    resp3: bool,
) {
    if resp3 {
        append_map_length(response_out, fields.len());
    } else {
        append_array_length(response_out, fields.len() * 2);
    }
    for (field, value) in fields {
        append_bulk_string(response_out, field);
        append_bulk_string(response_out, value);
    }
}

/// Emit `XINFO STREAM key` (non-FULL) response: 10-field map.
fn append_xinfo_stream_summary(
    response_out: &mut Vec<u8>,
    stream: &StreamObject,
    entry_count: usize,
    last_generated_id: StreamId,
    first_entry_id: StreamId,
    resp3: bool,
) {
    let field_count = 16;
    if resp3 {
        append_map_length(response_out, field_count);
    } else {
        append_array_length(response_out, field_count * 2);
    }

    // 1. length
    append_bulk_string(response_out, b"length");
    append_integer(response_out, entry_count as i64);

    // 2. radix-tree-keys (approximation: entry count)
    append_bulk_string(response_out, b"radix-tree-keys");
    append_integer(response_out, entry_count as i64);

    // 3. radix-tree-nodes (approximation: entry count)
    append_bulk_string(response_out, b"radix-tree-nodes");
    append_integer(response_out, entry_count as i64);

    // 4. last-generated-id
    append_bulk_string(response_out, b"last-generated-id");
    append_bulk_string(response_out, &last_generated_id.encode());

    // 5. max-deleted-entry-id
    append_bulk_string(response_out, b"max-deleted-entry-id");
    append_bulk_string(response_out, &stream.max_deleted_entry_id.encode());

    // 6. entries-added
    append_bulk_string(response_out, b"entries-added");
    append_integer(
        response_out,
        stream.entries_added.min(i64::MAX as u64) as i64,
    );

    // 7. recorded-first-entry-id
    append_bulk_string(response_out, b"recorded-first-entry-id");
    append_bulk_string(response_out, &first_entry_id.encode());

    // 8. idmp-duration
    append_bulk_string(response_out, b"idmp-duration");
    append_integer(
        response_out,
        stream.idmp_duration_seconds.min(i64::MAX as u64) as i64,
    );

    // 9. idmp-maxsize
    append_bulk_string(response_out, b"idmp-maxsize");
    append_integer(
        response_out,
        stream.idmp_maxsize.min(i64::MAX as u64) as i64,
    );

    // 10. pids-tracked
    append_bulk_string(response_out, b"pids-tracked");
    append_integer(
        response_out,
        stream.idmp_producers.len().min(i64::MAX as usize) as i64,
    );

    // 11. iids-tracked
    append_bulk_string(response_out, b"iids-tracked");
    append_integer(
        response_out,
        stream.idmp_tracked_count().min(i64::MAX as usize) as i64,
    );

    // 12. iids-added
    append_bulk_string(response_out, b"iids-added");
    append_integer(response_out, stream.iids_added.min(i64::MAX as u64) as i64);

    // 13. iids-duplicates
    append_bulk_string(response_out, b"iids-duplicates");
    append_integer(
        response_out,
        stream.iids_duplicates.min(i64::MAX as u64) as i64,
    );

    // 14. groups (count only in non-FULL mode)
    append_bulk_string(response_out, b"groups");
    append_integer(response_out, stream.groups.len() as i64);

    // 15. first-entry
    append_bulk_string(response_out, b"first-entry");
    if let Some((id, fields)) = stream.entries.first_key_value() {
        append_array_length(response_out, 2);
        append_bulk_string(response_out, &id.encode());
        append_stream_entry_fields(response_out, fields, resp3);
    } else if resp3 {
        append_null(response_out);
    } else {
        append_null_bulk_string(response_out);
    }

    // 16. last-entry
    append_bulk_string(response_out, b"last-entry");
    if let Some((id, fields)) = stream.entries.last_key_value() {
        append_array_length(response_out, 2);
        append_bulk_string(response_out, &id.encode());
        append_stream_entry_fields(response_out, fields, resp3);
    } else if resp3 {
        append_null(response_out);
    } else {
        append_null_bulk_string(response_out);
    }
}

/// Emit `XINFO STREAM key FULL [COUNT count]` response: 9-field map.
fn append_xinfo_stream_full(
    response_out: &mut Vec<u8>,
    stream: &StreamObject,
    entry_count: usize,
    last_generated_id: StreamId,
    first_entry_id: StreamId,
    entry_count_limit: usize,
    resp3: bool,
) {
    let field_count = 15;
    if resp3 {
        append_map_length(response_out, field_count);
    } else {
        append_array_length(response_out, field_count * 2);
    }

    // 1. length
    append_bulk_string(response_out, b"length");
    append_integer(response_out, entry_count as i64);

    // 2. radix-tree-keys
    append_bulk_string(response_out, b"radix-tree-keys");
    append_integer(response_out, entry_count as i64);

    // 3. radix-tree-nodes
    append_bulk_string(response_out, b"radix-tree-nodes");
    append_integer(response_out, entry_count as i64);

    // 4. last-generated-id
    append_bulk_string(response_out, b"last-generated-id");
    append_bulk_string(response_out, &last_generated_id.encode());

    // 5. max-deleted-entry-id
    append_bulk_string(response_out, b"max-deleted-entry-id");
    append_bulk_string(response_out, &stream.max_deleted_entry_id.encode());

    // 6. entries-added
    append_bulk_string(response_out, b"entries-added");
    append_integer(
        response_out,
        stream.entries_added.min(i64::MAX as u64) as i64,
    );

    // 7. recorded-first-entry-id
    append_bulk_string(response_out, b"recorded-first-entry-id");
    append_bulk_string(response_out, &first_entry_id.encode());

    // 8. idmp-duration
    append_bulk_string(response_out, b"idmp-duration");
    append_integer(
        response_out,
        stream.idmp_duration_seconds.min(i64::MAX as u64) as i64,
    );

    // 9. idmp-maxsize
    append_bulk_string(response_out, b"idmp-maxsize");
    append_integer(
        response_out,
        stream.idmp_maxsize.min(i64::MAX as u64) as i64,
    );

    // 10. pids-tracked
    append_bulk_string(response_out, b"pids-tracked");
    append_integer(
        response_out,
        stream.idmp_producers.len().min(i64::MAX as usize) as i64,
    );

    // 11. iids-tracked
    append_bulk_string(response_out, b"iids-tracked");
    append_integer(
        response_out,
        stream.idmp_tracked_count().min(i64::MAX as usize) as i64,
    );

    // 12. iids-added
    append_bulk_string(response_out, b"iids-added");
    append_integer(response_out, stream.iids_added.min(i64::MAX as u64) as i64);

    // 13. iids-duplicates
    append_bulk_string(response_out, b"iids-duplicates");
    append_integer(
        response_out,
        stream.iids_duplicates.min(i64::MAX as u64) as i64,
    );

    // 14. entries (limited by count)
    append_bulk_string(response_out, b"entries");
    let visible_entries: Vec<_> = stream.entries.iter().take(entry_count_limit).collect();
    append_array_length(response_out, visible_entries.len());
    for (id, fields) in &visible_entries {
        append_array_length(response_out, 2);
        append_bulk_string(response_out, &id.encode());
        append_stream_entry_fields(response_out, fields, resp3);
    }

    // 15. groups (detailed array in FULL mode)
    append_bulk_string(response_out, b"groups");
    append_array_length(response_out, stream.groups.len());
    for (group_name, group_state) in &stream.groups {
        let group_field_count = 7;
        if resp3 {
            append_map_length(response_out, group_field_count);
        } else {
            append_array_length(response_out, group_field_count * 2);
        }

        append_bulk_string(response_out, b"name");
        append_bulk_string(response_out, group_name);

        append_bulk_string(response_out, b"last-delivered-id");
        append_bulk_string(response_out, &group_state.last_delivered_id.encode());

        append_bulk_string(response_out, b"entries-read");
        append_stream_optional_counter(response_out, group_state.entries_read, resp3);

        append_bulk_string(response_out, b"lag");
        append_stream_optional_counter(response_out, stream_group_lag(stream, group_state), resp3);

        append_bulk_string(response_out, b"pel-count");
        append_integer(response_out, group_state.pending.len() as i64);

        append_bulk_string(response_out, b"pending");
        let visible_pending: Vec<_> = group_state.pending.iter().take(entry_count_limit).collect();
        append_array_length(response_out, visible_pending.len());
        for (pending_id, pending_entry) in visible_pending {
            append_array_length(response_out, 4);
            append_bulk_string(response_out, &pending_id.encode());
            append_bulk_string(response_out, &pending_entry.consumer);
            append_integer(
                response_out,
                pending_entry.last_delivery_time_millis.min(i64::MAX as u64) as i64,
            );
            append_integer(
                response_out,
                pending_entry.delivery_count.min(i64::MAX as u64) as i64,
            );
        }

        append_bulk_string(response_out, b"consumers");
        append_array_length(response_out, group_state.consumers.len());
        for (consumer_name, consumer_state) in &group_state.consumers {
            if resp3 {
                append_map_length(response_out, 5);
            } else {
                append_array_length(response_out, 10);
            }
            append_bulk_string(response_out, b"name");
            append_bulk_string(response_out, consumer_name);
            append_bulk_string(response_out, b"seen-time");
            append_integer(
                response_out,
                consumer_state.seen_time_millis.min(i64::MAX as u64) as i64,
            );
            append_bulk_string(response_out, b"active-time");
            if let Some(active_time_millis) = consumer_state.active_time_millis {
                append_integer(response_out, active_time_millis.min(i64::MAX as u64) as i64);
            } else {
                append_integer(response_out, -1);
            }
            append_bulk_string(response_out, b"pel-count");
            append_integer(response_out, consumer_state.pending.len() as i64);
            append_bulk_string(response_out, b"pending");
            let visible_consumer_pending: Vec<_> = consumer_state
                .pending
                .iter()
                .take(entry_count_limit)
                .collect();
            append_array_length(response_out, visible_consumer_pending.len());
            for pending_id in visible_consumer_pending {
                let Some(pending_entry) = group_state.pending.get(pending_id) else {
                    continue;
                };
                append_array_length(response_out, 3);
                append_bulk_string(response_out, &pending_id.encode());
                append_integer(
                    response_out,
                    pending_entry.last_delivery_time_millis.min(i64::MAX as u64) as i64,
                );
                append_integer(
                    response_out,
                    pending_entry.delivery_count.min(i64::MAX as u64) as i64,
                );
            }
        }
    }
}

const XGROUP_HELP_LINES: [&[u8]; 17] = [
    b"XGROUP <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
    b"CREATE <key> <groupname> <id|$> [option]",
    b"    Create a new consumer group. Options are:",
    b"    * MKSTREAM",
    b"      Create the empty stream if it does not exist.",
    b"    * ENTRIESREAD entries_read",
    b"      Set the group's entries_read counter (internal use).",
    b"CREATECONSUMER <key> <groupname> <consumer>",
    b"    Create a new consumer in the specified group.",
    b"DELCONSUMER <key> <groupname> <consumer>",
    b"    Remove the specified consumer.",
    b"DESTROY <key> <groupname>",
    b"    Remove the specified group.",
    b"SETID <key> <groupname> <id|$> [ENTRIESREAD entries_read]",
    b"    Set the current group ID and entries_read counter.",
    b"HELP",
    b"    Print this help.",
];

const XINFO_HELP_LINES: [&[u8]; 9] = [
    b"XINFO <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
    b"CONSUMERS <key> <groupname>",
    b"    Show consumers of <groupname>.",
    b"GROUPS <key>",
    b"    Show the stream consumer groups.",
    b"STREAM <key> [FULL [COUNT <count>]",
    b"    Show information about the stream.",
    b"HELP",
    b"    Print this help.",
];
