use super::*;

impl RequestProcessor {
    fn parse_xgroup_id_arg(
        &self,
        id_arg: &[u8],
        stream: &StreamObject,
    ) -> Result<StreamId, RequestExecutionError> {
        if id_arg == b"$" {
            return Ok(stream
                .entries
                .last_key_value()
                .map(|(id, _)| *id)
                .unwrap_or_else(StreamId::zero));
        }
        StreamId::parse(id_arg).ok_or(RequestExecutionError::SyntaxError)
    }

    pub(super) fn handle_xadd(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_paired_arity_after(
            args,
            5,
            3,
            "XADD",
            "XADD key id field value [field value ...]",
        )?;

        let key = RedisKey::from(args[1]);
        let requested_id = args[2];

        let mut stream = self.load_stream_object(&key)?.unwrap_or_default();
        let id = if requested_id == b"*" {
            next_auto_stream_id(&stream)
        } else {
            StreamId::parse(requested_id).ok_or(RequestExecutionError::SyntaxError)?
        };

        let mut fields = Vec::with_capacity((args.len() - 3) / 2);
        let mut index = 3usize;
        while index < args.len() {
            let field = args[index].to_vec();
            let value = args[index + 1].to_vec();
            fields.push((field, value));
            index += 2;
        }

        stream.entries.insert(id, fields);
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
            if stream.entries.remove(&id).is_some() {
                removed += 1;
            }
        }

        if stream.entries.is_empty() && stream.groups.is_empty() {
            let _ = self.object_delete(&key)?;
        } else {
            self.save_stream_object(&key, &stream)?;
        }
        append_integer(response_out, removed);
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
            // Parse optional trailing tokens: MKSTREAM and/or ENTRIESREAD n.
            let mut index = 5;
            while index < args.len() {
                if ascii_eq_ignore_case(args[index], b"MKSTREAM") {
                    index += 1;
                } else if ascii_eq_ignore_case(args[index], b"ENTRIESREAD") {
                    if index + 1 >= args.len() {
                        return Err(RequestExecutionError::SyntaxError);
                    }
                    // Validate the entries-read value is an integer; we accept
                    // but ignore it since we don't track entries-read state.
                    parse_i64_ascii(args[index + 1])
                        .ok_or(RequestExecutionError::ValueNotInteger)?;
                    index += 2;
                } else {
                    return Err(RequestExecutionError::SyntaxError);
                }
            }
            let key = RedisKey::from(args[2]);
            let group = args[3].to_vec();
            let mut stream = self.load_stream_object(&key)?.unwrap_or_default();
            let id = self.parse_xgroup_id_arg(args[4], &stream)?;
            stream.groups.insert(group, id);
            self.save_stream_object(&key, &stream)?;
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
            if args.len() == 7 {
                if !ascii_eq_ignore_case(args[5], b"ENTRIESREAD") {
                    return Err(RequestExecutionError::SyntaxError);
                }
                parse_i64_ascii(args[6]).ok_or(RequestExecutionError::ValueNotInteger)?;
            }
            let key = RedisKey::from(args[2]);
            let group = args[3].to_vec();
            let mut stream = self.load_stream_object(&key)?.unwrap_or_default();
            let id = self.parse_xgroup_id_arg(args[4], &stream)?;
            stream.groups.insert(group, id);
            self.save_stream_object(&key, &stream)?;
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
            // Consumer creation is implicit in our model; return 1 (created)
            // if the stream and group exist. Redis returns 0 if consumer
            // already exists, but we have no consumer tracking.
            let key = RedisKey::from(args[2]);
            let stream = self.load_stream_object(&key)?;
            if stream.is_none() {
                return Err(RequestExecutionError::NoSuchKey);
            }
            append_integer(response_out, 1);
            return Ok(());
        }

        if ascii_eq_ignore_case(subcommand, b"DELCONSUMER") {
            require_exact_arity(
                args,
                5,
                "XGROUP",
                "XGROUP DELCONSUMER key group consumer",
            )?;
            let key = RedisKey::from(args[2]);
            let stream = self.load_stream_object(&key)?;
            if stream.is_none() {
                return Err(RequestExecutionError::NoSuchKey);
            }
            // Consumer deletion: return 0 pending entries (no consumer tracking).
            append_integer(response_out, 0);
            return Ok(());
        }

        Err(RequestExecutionError::UnknownSubcommand)
    }

    pub(super) fn handle_xreadgroup(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            7,
            "XREADGROUP",
            "XREADGROUP GROUP group consumer [NOACK] [COUNT count] STREAMS key >",
        )?;
        if !ascii_eq_ignore_case(args[1], b"GROUP") {
            return Err(RequestExecutionError::SyntaxError);
        }

        let group = args[2].to_vec();
        let mut count = 1usize;
        let mut key = None::<RedisKey>;
        let mut start_id = None::<Vec<u8>>;
        let mut index = 4usize;
        while index < args.len() {
            let token = args[index];
            if ascii_eq_ignore_case(token, b"NOACK") {
                index += 1;
                continue;
            }
            if ascii_eq_ignore_case(token, b"COUNT") {
                if index + 1 >= args.len() {
                    return Err(RequestExecutionError::SyntaxError);
                }
                let parsed = parse_u64_ascii(args[index + 1])
                    .ok_or(RequestExecutionError::ValueNotInteger)?;
                count = usize::try_from(parsed).unwrap_or(usize::MAX);
                index += 2;
                continue;
            }
            if ascii_eq_ignore_case(token, b"BLOCK") {
                if index + 1 >= args.len() {
                    return Err(RequestExecutionError::SyntaxError);
                }
                parse_u64_ascii(args[index + 1]).ok_or(RequestExecutionError::ValueNotInteger)?;
                index += 2;
                continue;
            }
            if ascii_eq_ignore_case(token, b"STREAMS") {
                if index + 2 >= args.len() {
                    return Err(RequestExecutionError::SyntaxError);
                }
                key = Some(RedisKey::from(args[index + 1]));
                start_id = Some(args[index + 2].to_vec());
                index += 3;
                continue;
            }
            return Err(RequestExecutionError::SyntaxError);
        }

        let Some(key) = key else {
            return Err(RequestExecutionError::SyntaxError);
        };
        let Some(start_id) = start_id else {
            return Err(RequestExecutionError::SyntaxError);
        };

        let mut stream = match self.load_stream_object(&key)? {
            Some(stream) => stream,
            None => {
                append_array_length(response_out, 0);
                return Ok(());
            }
        };

        let last_id = stream
            .groups
            .get(&group)
            .copied()
            .unwrap_or_else(StreamId::zero);
        let pivot = if start_id == b">" {
            last_id
        } else {
            StreamId::parse(&start_id).ok_or(RequestExecutionError::SyntaxError)?
        };

        #[allow(clippy::type_complexity)]
        let mut selected: Vec<(StreamId, Vec<(Vec<u8>, Vec<u8>)>)> = Vec::new();
        for (id, fields) in &stream.entries {
            if *id > pivot {
                selected.push((*id, fields.clone()));
                if selected.len() >= count {
                    break;
                }
            }
        }

        if selected.is_empty() {
            append_array_length(response_out, 0);
            return Ok(());
        }

        let last_seen = selected.last().map(|(id, _)| *id).unwrap_or(pivot);
        stream.groups.insert(group, last_seen);
        self.save_stream_object(&key, &stream)?;

        let resp3 = self.resp_protocol_version().is_resp3();
        if resp3 {
            append_map_length(response_out, 1);
        } else {
            append_array_length(response_out, 1);
            append_array_length(response_out, 2);
        }
        append_bulk_string(response_out, key.as_slice());
        append_array_length(response_out, selected.len());
        for (id, fields) in selected {
            append_array_length(response_out, 2);
            append_bulk_string(response_out, &id.encode());
            append_stream_entry_fields(response_out, &fields, resp3);
        }
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
                count = usize::try_from(parsed).unwrap_or(usize::MAX);
                index += 2;
                continue;
            }
            if ascii_eq_ignore_case(token, b"BLOCK") {
                if index + 1 >= args.len() {
                    return Err(RequestExecutionError::SyntaxError);
                }
                parse_u64_ascii(args[index + 1]).ok_or(RequestExecutionError::ValueNotInteger)?;
                index += 2;
                continue;
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
        if trailing < 2 || !trailing.is_multiple_of(2) {
            return Err(RequestExecutionError::SyntaxError);
        }
        let stream_count = trailing / 2;

        let mut remaining = count;
        let mut stream_results = Vec::new();
        for stream_index in 0..stream_count {
            let key = RedisKey::from(args[streams_index + stream_index]);
            let raw_id = args[streams_index + stream_count + stream_index];
            let Some(stream) = self.load_stream_object(&key)? else {
                continue;
            };

            let pivot = if raw_id == b"$" {
                stream
                    .entries
                    .keys()
                    .next_back()
                    .copied()
                    .unwrap_or_else(StreamId::zero)
            } else {
                StreamId::parse(raw_id).ok_or(RequestExecutionError::SyntaxError)?
            };

            let per_stream_limit = if remaining == usize::MAX {
                usize::MAX
            } else {
                remaining
            };
            let selected = collect_stream_entries_after_id(&stream, pivot, per_stream_limit);
            if selected.is_empty() {
                continue;
            }
            if remaining != usize::MAX {
                remaining = remaining.saturating_sub(selected.len());
            }
            stream_results.push((key, selected));
            if remaining == 0 {
                break;
            }
        }

        let resp3 = self.resp_protocol_version().is_resp3();
        if stream_results.is_empty() {
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
        let key = RedisKey::from(args[1]);
        let group = args[2];
        let Some(stream) = self.load_stream_object(&key)? else {
            append_integer(response_out, 0);
            return Ok(());
        };
        if !stream.groups.contains_key(group) {
            append_integer(response_out, 0);
            return Ok(());
        }
        append_integer(response_out, 0);
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
        if !stream.groups.contains_key(group) {
            return Err(RequestExecutionError::NoGroup);
        }

        if args.len() == 3 {
            let resp3 = self.resp_protocol_version().is_resp3();
            append_array_length(response_out, 4);
            append_integer(response_out, 0);
            if resp3 {
                append_null(response_out);
                append_null(response_out);
            } else {
                append_null_bulk_string(response_out);
                append_null_bulk_string(response_out);
            }
            append_array_length(response_out, 0);
            return Ok(());
        }

        // Extended form: XPENDING key group [IDLE min-idle-time] start end count [consumer]
        // With IDLE: 8 or 9 args. Without IDLE: 6 or 7 args.
        let has_idle = args.len() >= 4 && ascii_eq_ignore_case(args[3], b"IDLE");
        let offset = if has_idle {
            if args.len() < 8 {
                return Err(RequestExecutionError::SyntaxError);
            }
            parse_i64_ascii(args[4]).ok_or(RequestExecutionError::ValueNotInteger)?;
            2
        } else {
            if args.len() < 6 {
                return Err(RequestExecutionError::SyntaxError);
            }
            0
        };
        let count_index = 5 + offset;
        if count_index >= args.len() {
            return Err(RequestExecutionError::SyntaxError);
        }
        let parsed_count =
            parse_i64_ascii(args[count_index]).ok_or(RequestExecutionError::ValueNotInteger)?;
        if parsed_count < 0 {
            return Err(RequestExecutionError::ValueOutOfRange);
        }
        // Validate total arity: base (6+offset) or with consumer (7+offset)
        let max_args = 7 + offset;
        if args.len() > max_args {
            return Err(RequestExecutionError::SyntaxError);
        }
        append_array_length(response_out, 0);
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
        let Some(stream) = self.load_stream_object(&key)? else {
            return Err(RequestExecutionError::NoGroup);
        };
        if !stream.groups.contains_key(group) {
            return Err(RequestExecutionError::NoGroup);
        }
        parse_u64_ascii(args[4]).ok_or(RequestExecutionError::ValueNotInteger)?;

        let mut option_start = args.len();
        for (i, token) in args.iter().enumerate().skip(6) {
            let token = *token;
            if is_xclaim_option_token(token) {
                option_start = i;
                break;
            }
        }
        if option_start <= 5 {
            return Err(RequestExecutionError::SyntaxError);
        }
        for id_arg in &args[5..option_start] {
            if StreamId::parse(id_arg).is_none() {
                return Err(RequestExecutionError::SyntaxError);
            }
        }

        let mut index = option_start;
        while index < args.len() {
            let token = args[index];
            if ascii_eq_ignore_case(token, b"IDLE")
                || ascii_eq_ignore_case(token, b"TIME")
                || ascii_eq_ignore_case(token, b"RETRYCOUNT")
            {
                if index + 1 >= args.len() {
                    return Err(RequestExecutionError::SyntaxError);
                }
                parse_u64_ascii(args[index + 1]).ok_or(RequestExecutionError::ValueNotInteger)?;
                index += 2;
                continue;
            }
            if ascii_eq_ignore_case(token, b"LASTID") {
                if index + 1 >= args.len() {
                    return Err(RequestExecutionError::SyntaxError);
                }
                if StreamId::parse(args[index + 1]).is_none() {
                    return Err(RequestExecutionError::SyntaxError);
                }
                index += 2;
                continue;
            }
            if ascii_eq_ignore_case(token, b"FORCE") || ascii_eq_ignore_case(token, b"JUSTID") {
                index += 1;
                continue;
            }
            return Err(RequestExecutionError::SyntaxError);
        }

        append_array_length(response_out, 0);
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
        let Some(stream) = self.load_stream_object(&key)? else {
            return Err(RequestExecutionError::NoGroup);
        };
        if !stream.groups.contains_key(group) {
            return Err(RequestExecutionError::NoGroup);
        }
        parse_u64_ascii(args[4]).ok_or(RequestExecutionError::ValueNotInteger)?;
        let start_id = args[5];
        if StreamId::parse(start_id).is_none() {
            return Err(RequestExecutionError::SyntaxError);
        }

        let mut index = 6usize;
        while index < args.len() {
            let token = args[index];
            if ascii_eq_ignore_case(token, b"COUNT") {
                if index + 1 >= args.len() {
                    return Err(RequestExecutionError::SyntaxError);
                }
                parse_u64_ascii(args[index + 1]).ok_or(RequestExecutionError::ValueNotInteger)?;
                index += 2;
                continue;
            }
            if ascii_eq_ignore_case(token, b"JUSTID") {
                index += 1;
                continue;
            }
            return Err(RequestExecutionError::SyntaxError);
        }

        append_array_length(response_out, 3);
        append_bulk_string(response_out, start_id);
        append_array_length(response_out, 0);
        append_array_length(response_out, 0);
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
            "XSETID key last-id [ENTRIESADDED entries-added] [MAXDELETEDID max-id] [KEEPREF|DELREF|ACKED]",
        )?;
        let key = RedisKey::from(args[1]);
        let last_id = args[2];
        if StreamId::parse(last_id).is_none() {
            return Err(RequestExecutionError::SyntaxError);
        }
        let Some(stream) = self.load_stream_object(&key)? else {
            return Err(RequestExecutionError::NoSuchKey);
        };

        let mut seen_ref_mode = false;
        let mut index = 3usize;
        while index < args.len() {
            let token = args[index];
            if ascii_eq_ignore_case(token, b"ENTRIESADDED") {
                if index + 1 >= args.len() {
                    return Err(RequestExecutionError::SyntaxError);
                }
                parse_u64_ascii(args[index + 1]).ok_or(RequestExecutionError::ValueNotInteger)?;
                index += 2;
                continue;
            }
            if ascii_eq_ignore_case(token, b"MAXDELETEDID") {
                if index + 1 >= args.len() {
                    return Err(RequestExecutionError::SyntaxError);
                }
                if StreamId::parse(args[index + 1]).is_none() {
                    return Err(RequestExecutionError::SyntaxError);
                }
                index += 2;
                continue;
            }
            if ascii_eq_ignore_case(token, b"KEEPREF")
                || ascii_eq_ignore_case(token, b"DELREF")
                || ascii_eq_ignore_case(token, b"ACKED")
            {
                if seen_ref_mode {
                    return Err(RequestExecutionError::SyntaxError);
                }
                seen_ref_mode = true;
                index += 1;
                continue;
            }
            return Err(RequestExecutionError::SyntaxError);
        }

        self.save_stream_object(&key, &stream)?;
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
            return Err(RequestExecutionError::UnknownCommand);
        }

        let key = RedisKey::from(args[2]);
        let stream = match self.load_stream_object(&key)? {
            Some(stream) => stream,
            None => return Err(RequestExecutionError::NoSuchKey),
        };

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
            } else {
                entry_count_limit = usize::try_from(parsed).unwrap_or(usize::MAX);
            }
        }
        if full && args.len() > 6 {
            return Err(RequestExecutionError::SyntaxError);
        }

        let resp3 = self.resp_protocol_version().is_resp3();
        let last_generated_id = stream
            .entries
            .keys()
            .next_back()
            .copied()
            .unwrap_or_else(StreamId::zero);
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
        for (group_name, last_id) in &stream.groups {
            // Valkey emits 6-field map per group.
            if resp3 {
                append_map_length(response_out, 6);
            } else {
                append_array_length(response_out, 12);
            }
            append_bulk_string(response_out, b"name");
            append_bulk_string(response_out, group_name);
            append_bulk_string(response_out, b"consumers");
            append_integer(response_out, 0);
            append_bulk_string(response_out, b"pending");
            append_integer(response_out, 0);
            append_bulk_string(response_out, b"last-delivered-id");
            append_bulk_string(response_out, &last_id.encode());
            append_bulk_string(response_out, b"entries-read");
            if resp3 {
                append_null(response_out);
            } else {
                append_null_bulk_string(response_out);
            }
            append_bulk_string(response_out, b"lag");
            append_integer(response_out, 0);
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
        if !stream.groups.contains_key(group) {
            return Err(RequestExecutionError::NoGroup);
        }
        // No consumer tracking — return empty array.
        append_array_length(response_out, 0);
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

        let entries = collect_stream_range_entries(&stream, start, end, false, count);
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

        let entries = collect_stream_range_entries(&stream, start, end, true, count);
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

        let entry_ids: Vec<StreamId> = stream.entries.keys().copied().collect();

        let mut removed = 0usize;
        match spec.strategy {
            XtrimStrategy::Maxlen(maxlen) => {
                let mut to_remove = stream.entries.len().saturating_sub(maxlen);
                if let Some(limit) = spec.limit {
                    to_remove = to_remove.min(limit);
                }
                for entry_id in entry_ids.into_iter().take(to_remove) {
                    if stream.entries.remove(&entry_id).is_some() {
                        removed += 1;
                    }
                }
            }
            XtrimStrategy::Minid(minid) => {
                for entry_id in entry_ids {
                    if entry_id >= minid {
                        break;
                    }
                    if let Some(limit) = spec.limit
                        && removed >= limit
                    {
                        break;
                    }
                    if stream.entries.remove(&entry_id).is_some() {
                        removed += 1;
                    }
                }
            }
        }

        if removed > 0 {
            if stream.entries.is_empty() && stream.groups.is_empty() {
                let _ = self.object_delete(&key)?;
            } else {
                self.save_stream_object(&key, &stream)?;
            }
        }
        append_integer(response_out, removed as i64);
        Ok(())
    }
}

fn next_auto_stream_id(stream: &StreamObject) -> StreamId {
    let now = current_unix_time_millis().unwrap_or(0);
    let Some(last_id) = stream.entries.keys().next_back().copied() else {
        return StreamId::new(now, 0);
    };
    if last_id.timestamp_millis() > now {
        return StreamId::new(
            last_id.timestamp_millis(),
            last_id.sequence().saturating_add(1),
        );
    }
    if last_id.timestamp_millis() == now {
        return StreamId::new(now, last_id.sequence().saturating_add(1));
    }
    StreamId::new(now, 0)
}

#[allow(clippy::type_complexity)]
fn collect_stream_entries_after_id(
    stream: &StreamObject,
    pivot: StreamId,
    count: usize,
) -> Vec<(StreamId, Vec<(Vec<u8>, Vec<u8>)>)> {
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
    limit: Option<usize>,
}

fn parse_xtrim_spec(args: &[&[u8]]) -> Result<XtrimSpec, RequestExecutionError> {
    let strategy_token = args[2];
    let mut index = 3usize;
    if index < args.len() {
        let marker = args[index];
        if marker == b"=" || marker == b"~" {
            index += 1;
        }
    }
    if index >= args.len() {
        return Err(RequestExecutionError::SyntaxError);
    }
    let threshold = args[index];
    index += 1;

    let limit = if index == args.len() {
        None
    } else {
        if index + 2 != args.len() {
            return Err(RequestExecutionError::SyntaxError);
        }
        if !ascii_eq_ignore_case(args[index], b"LIMIT") {
            return Err(RequestExecutionError::SyntaxError);
        }
        let parsed =
            parse_i64_ascii(args[index + 1]).ok_or(RequestExecutionError::ValueNotInteger)?;
        if parsed < 0 {
            return Err(RequestExecutionError::ValueOutOfRange);
        }
        Some(usize::try_from(parsed).map_err(|_| RequestExecutionError::ValueOutOfRange)?)
    };

    let strategy = if ascii_eq_ignore_case(strategy_token, b"MAXLEN") {
        let parsed = parse_i64_ascii(threshold).ok_or(RequestExecutionError::ValueNotInteger)?;
        if parsed < 0 {
            return Err(RequestExecutionError::ValueOutOfRange);
        }
        XtrimStrategy::Maxlen(
            usize::try_from(parsed).map_err(|_| RequestExecutionError::ValueOutOfRange)?,
        )
    } else if ascii_eq_ignore_case(strategy_token, b"MINID") {
        XtrimStrategy::Minid(StreamId::parse(threshold).ok_or(RequestExecutionError::SyntaxError)?)
    } else {
        return Err(RequestExecutionError::SyntaxError);
    };

    Ok(XtrimSpec { strategy, limit })
}

#[allow(clippy::type_complexity)]
fn collect_stream_range_entries(
    stream: &StreamObject,
    lower_bound: &[u8],
    upper_bound: &[u8],
    reverse: bool,
    count: usize,
) -> Vec<(StreamId, Vec<(Vec<u8>, Vec<u8>)>)> {
    let mut selected = Vec::new();
    if reverse {
        for (id, fields) in stream.entries.iter().rev() {
            let lower_ok = if lower_bound == b"-" {
                true
            } else {
                !id.compare_bytes(lower_bound).is_lt()
            };
            if !lower_ok {
                continue;
            }
            let upper_ok = if upper_bound == b"+" {
                true
            } else {
                !id.compare_bytes(upper_bound).is_gt()
            };
            if !upper_ok {
                continue;
            }
            selected.push((*id, fields.clone()));
            if selected.len() >= count {
                break;
            }
        }
    } else {
        for (id, fields) in &stream.entries {
            let lower_ok = if lower_bound == b"-" {
                true
            } else {
                !id.compare_bytes(lower_bound).is_lt()
            };
            if !lower_ok {
                continue;
            }
            let upper_ok = if upper_bound == b"+" {
                true
            } else {
                !id.compare_bytes(upper_bound).is_gt()
            };
            if !upper_ok {
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
    let field_count = 10;
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

    // 5. max-deleted-entry-id (not tracked)
    append_bulk_string(response_out, b"max-deleted-entry-id");
    append_bulk_string(response_out, b"0-0");

    // 6. entries-added (approximation: entry count)
    append_bulk_string(response_out, b"entries-added");
    append_integer(response_out, entry_count as i64);

    // 7. recorded-first-entry-id
    append_bulk_string(response_out, b"recorded-first-entry-id");
    append_bulk_string(response_out, &first_entry_id.encode());

    // 8. groups (count only in non-FULL mode)
    append_bulk_string(response_out, b"groups");
    append_integer(response_out, stream.groups.len() as i64);

    // 9. first-entry
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

    // 10. last-entry
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
    let field_count = 9;
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
    append_bulk_string(response_out, b"0-0");

    // 6. entries-added
    append_bulk_string(response_out, b"entries-added");
    append_integer(response_out, entry_count as i64);

    // 7. recorded-first-entry-id
    append_bulk_string(response_out, b"recorded-first-entry-id");
    append_bulk_string(response_out, &first_entry_id.encode());

    // 8. entries (limited by count)
    append_bulk_string(response_out, b"entries");
    let visible_entries: Vec<_> = stream.entries.iter().take(entry_count_limit).collect();
    append_array_length(response_out, visible_entries.len());
    for (id, fields) in &visible_entries {
        append_array_length(response_out, 2);
        append_bulk_string(response_out, &id.encode());
        append_stream_entry_fields(response_out, fields, resp3);
    }

    // 9. groups (detailed array in FULL mode)
    append_bulk_string(response_out, b"groups");
    append_array_length(response_out, stream.groups.len());
    for (group_name, last_id) in &stream.groups {
        let group_field_count = 7;
        if resp3 {
            append_map_length(response_out, group_field_count);
        } else {
            append_array_length(response_out, group_field_count * 2);
        }

        append_bulk_string(response_out, b"name");
        append_bulk_string(response_out, group_name);

        append_bulk_string(response_out, b"last-delivered-id");
        append_bulk_string(response_out, &last_id.encode());

        append_bulk_string(response_out, b"entries-read");
        if resp3 {
            append_null(response_out);
        } else {
            append_null_bulk_string(response_out);
        }

        append_bulk_string(response_out, b"lag");
        append_integer(response_out, 0);

        append_bulk_string(response_out, b"pel-count");
        append_integer(response_out, 0);

        append_bulk_string(response_out, b"pending");
        append_array_length(response_out, 0);

        append_bulk_string(response_out, b"consumers");
        append_array_length(response_out, 0);
    }
}

const XGROUP_HELP_LINES: [&[u8]; 12] = [
    b"CREATE <key> <groupname> <id|$> [MKSTREAM]",
    b"    Create a new consumer group.",
    b"CREATECONSUMER <key> <groupname> <consumer>",
    b"    Create a consumer in a consumer group.",
    b"DELCONSUMER <key> <groupname> <consumer>",
    b"    Delete a consumer from a consumer group.",
    b"SETID <key> <groupname> <id|$>",
    b"    Set the current group ID.",
    b"DESTROY <key> <groupname>",
    b"    Remove the specified consumer group.",
    b"HELP",
    b"    Print this help.",
];

const XINFO_HELP_LINES: [&[u8]; 6] = [
    b"CONSUMERS <key> <groupname>",
    b"    Show consumers of <groupname>.",
    b"GROUPS <key>",
    b"    Show the stream consumer groups.",
    b"STREAM <key> [FULL [COUNT <count>]]",
    b"    Show information about the stream.",
];
