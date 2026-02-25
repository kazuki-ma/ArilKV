use super::*;

impl RequestProcessor {
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

        let key = args[1].to_vec();
        let requested_id = args[2];

        let mut stream = self.load_stream_object(&key)?.unwrap_or_default();
        let id = if requested_id == b"*" {
            next_auto_stream_id(&stream)
        } else {
            requested_id.to_vec()
        };

        let mut fields = Vec::with_capacity((args.len() - 3) / 2);
        let mut index = 3usize;
        while index < args.len() {
            let field = args[index].to_vec();
            let value = args[index + 1].to_vec();
            fields.push((field, value));
            index += 2;
        }

        stream.entries.insert(id.clone(), fields);
        self.save_stream_object(&key, &stream)?;
        append_bulk_string(response_out, &id);
        Ok(())
    }

    pub(super) fn handle_xdel(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 3, "XDEL", "XDEL key id [id ...]")?;

        let key = args[1].to_vec();
        let mut stream = match self.load_stream_object(&key)? {
            Some(stream) => stream,
            None => {
                append_integer(response_out, 0);
                return Ok(());
            }
        };

        let mut removed = 0i64;
        for id in &args[2..] {
            if stream.entries.remove(*id).is_some() {
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
        ensure_min_arity(args, 2, "XGROUP", "XGROUP <CREATE|SETID> key group id")?;
        let subcommand = args[1];

        if ascii_eq_ignore_case(subcommand, b"CREATE") {
            ensure_one_of_arities(
                args,
                &[5, 6],
                "XGROUP",
                "XGROUP CREATE key group id [MKSTREAM]",
            )?;
            if args.len() == 6 && !ascii_eq_ignore_case(args[5], b"MKSTREAM") {
                return Err(RequestExecutionError::SyntaxError);
            }
            let key = args[2].to_vec();
            let group = args[3].to_vec();
            let id = args[4].to_vec();
            let mut stream = self.load_stream_object(&key)?.unwrap_or_default();
            stream.groups.insert(group, id);
            self.save_stream_object(&key, &stream)?;
            append_simple_string(response_out, b"OK");
            return Ok(());
        }

        if ascii_eq_ignore_case(subcommand, b"SETID") {
            require_exact_arity(args, 5, "XGROUP", "XGROUP SETID key group id")?;
            let key = args[2].to_vec();
            let group = args[3].to_vec();
            let id = args[4].to_vec();
            let mut stream = self.load_stream_object(&key)?.unwrap_or_default();
            stream.groups.insert(group, id);
            self.save_stream_object(&key, &stream)?;
            append_simple_string(response_out, b"OK");
            return Ok(());
        }

        Err(RequestExecutionError::UnknownCommand)
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
        let mut key = None::<Vec<u8>>;
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
                key = Some(args[index + 1].to_vec());
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
                response_out.extend_from_slice(b"*0\r\n");
                return Ok(());
            }
        };

        let last_id = stream
            .groups
            .get(&group)
            .cloned()
            .unwrap_or_else(|| b"0".to_vec());
        let pivot = if start_id == b">" { last_id } else { start_id };

        let mut selected: Vec<(Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>)> = Vec::new();
        for (id, fields) in &stream.entries {
            if stream_id_greater(id, &pivot) {
                selected.push((id.clone(), fields.clone()));
                if selected.len() >= count {
                    break;
                }
            }
        }

        if selected.is_empty() {
            response_out.extend_from_slice(b"*0\r\n");
            return Ok(());
        }

        let last_seen = selected
            .last()
            .map(|(id, _)| id.clone())
            .unwrap_or_else(|| pivot.clone());
        stream.groups.insert(group, last_seen);
        self.save_stream_object(&key, &stream)?;

        response_out.extend_from_slice(b"*1\r\n");
        response_out.extend_from_slice(b"*2\r\n");
        append_bulk_string(response_out, &key);
        response_out.push(b'*');
        response_out.extend_from_slice(selected.len().to_string().as_bytes());
        response_out.extend_from_slice(b"\r\n");
        for (id, fields) in selected {
            response_out.extend_from_slice(b"*2\r\n");
            append_bulk_string(response_out, &id);
            response_out.push(b'*');
            response_out.extend_from_slice((fields.len() * 2).to_string().as_bytes());
            response_out.extend_from_slice(b"\r\n");
            for (field, value) in fields {
                append_bulk_string(response_out, &field);
                append_bulk_string(response_out, &value);
            }
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
        if trailing < 2 || trailing % 2 != 0 {
            return Err(RequestExecutionError::SyntaxError);
        }
        let stream_count = trailing / 2;

        let mut remaining = count;
        let mut stream_results = Vec::new();
        for stream_index in 0..stream_count {
            let key = args[streams_index + stream_index].to_vec();
            let raw_id = args[streams_index + stream_count + stream_index];
            let Some(stream) = self.load_stream_object(&key)? else {
                continue;
            };

            let pivot = if raw_id == b"$" {
                stream
                    .entries
                    .keys()
                    .max_by(|lhs, rhs| compare_stream_ids(lhs, rhs))
                    .cloned()
                    .unwrap_or_else(|| b"0-0".to_vec())
            } else {
                raw_id.to_vec()
            };

            let per_stream_limit = if remaining == usize::MAX {
                usize::MAX
            } else {
                remaining
            };
            let selected = collect_stream_entries_after_id(&stream, &pivot, per_stream_limit);
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

        if stream_results.is_empty() {
            append_null_array(response_out);
            return Ok(());
        }

        response_out.push(b'*');
        response_out.extend_from_slice(stream_results.len().to_string().as_bytes());
        response_out.extend_from_slice(b"\r\n");
        for (key, entries) in stream_results {
            response_out.extend_from_slice(b"*2\r\n");
            append_bulk_string(response_out, &key);
            append_stream_entry_array(response_out, &entries);
        }
        Ok(())
    }

    pub(super) fn handle_xack(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 4, "XACK", "XACK key group id [id ...]")?;
        let key = args[1].to_vec();
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
            "XPENDING key group [start end count [consumer]]",
        )?;
        let key = args[1].to_vec();
        let group = args[2];
        let Some(stream) = self.load_stream_object(&key)? else {
            return Err(RequestExecutionError::NoGroup);
        };
        if !stream.groups.contains_key(group) {
            return Err(RequestExecutionError::NoGroup);
        }

        if args.len() == 3 {
            response_out.extend_from_slice(b"*4\r\n");
            append_integer(response_out, 0);
            append_null_bulk_string(response_out);
            append_null_bulk_string(response_out);
            response_out.extend_from_slice(b"*0\r\n");
            return Ok(());
        }
        ensure_ranged_arity(
            args,
            6,
            7,
            "XPENDING",
            "XPENDING key group [start end count [consumer]]",
        )?;
        let parsed_count =
            parse_i64_ascii(args[5]).ok_or(RequestExecutionError::ValueNotInteger)?;
        if parsed_count < 0 {
            return Err(RequestExecutionError::ValueOutOfRange);
        }
        response_out.extend_from_slice(b"*0\r\n");
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
        let key = args[1].to_vec();
        let group = args[2];
        let Some(stream) = self.load_stream_object(&key)? else {
            return Err(RequestExecutionError::NoGroup);
        };
        if !stream.groups.contains_key(group) {
            return Err(RequestExecutionError::NoGroup);
        }
        parse_u64_ascii(args[4]).ok_or(RequestExecutionError::ValueNotInteger)?;

        let mut option_start = args.len();
        for i in 6..args.len() {
            let token = args[i];
            if is_xclaim_option_token(token) {
                option_start = i;
                break;
            }
        }
        if option_start <= 5 {
            return Err(RequestExecutionError::SyntaxError);
        }
        for id_arg in &args[5..option_start] {
            if parse_stream_id(id_arg).is_none() {
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
                if parse_stream_id(args[index + 1]).is_none() {
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

        response_out.extend_from_slice(b"*0\r\n");
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
        let key = args[1].to_vec();
        let group = args[2];
        let Some(stream) = self.load_stream_object(&key)? else {
            return Err(RequestExecutionError::NoGroup);
        };
        if !stream.groups.contains_key(group) {
            return Err(RequestExecutionError::NoGroup);
        }
        parse_u64_ascii(args[4]).ok_or(RequestExecutionError::ValueNotInteger)?;
        let start_id = args[5];
        if parse_stream_id(start_id).is_none() {
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

        response_out.extend_from_slice(b"*3\r\n");
        append_bulk_string(response_out, start_id);
        response_out.extend_from_slice(b"*0\r\n");
        response_out.extend_from_slice(b"*0\r\n");
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
        let key = args[1].to_vec();
        let last_id = args[2];
        if parse_stream_id(last_id).is_none() {
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
                if parse_stream_id(args[index + 1]).is_none() {
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
        require_exact_arity(args, 4, "XINFO", "XINFO STREAM key FULL")?;
        let subcommand = args[1];
        if !ascii_eq_ignore_case(subcommand, b"STREAM") {
            return Err(RequestExecutionError::UnknownCommand);
        }
        let full = args[3];
        if !ascii_eq_ignore_case(full, b"FULL") {
            return Err(RequestExecutionError::UnknownCommand);
        }
        let key = args[2].to_vec();
        let stream = match self.load_stream_object(&key)? {
            Some(stream) => stream,
            None => {
                append_null_bulk_string(response_out);
                return Ok(());
            }
        };
        let digest = fnv_hex_digest(3, &serialize_stream_object_payload(&stream));
        append_bulk_string(response_out, &digest);
        Ok(())
    }

    pub(super) fn handle_xlen(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 2, "XLEN", "XLEN key")?;
        let key = args[1].to_vec();
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
        let key = args[1].to_vec();
        let start = args[2];
        let end = args[3];
        let count = parse_stream_count_option(args)?;
        if count == 0 {
            response_out.extend_from_slice(b"*0\r\n");
            return Ok(());
        }

        let stream = match self.load_stream_object(&key)? {
            Some(stream) => stream,
            None => {
                response_out.extend_from_slice(b"*0\r\n");
                return Ok(());
            }
        };

        let entries = collect_stream_range_entries(&stream, start, end, false, count);
        append_stream_entry_array(response_out, &entries);
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
        let key = args[1].to_vec();
        let end = args[2];
        let start = args[3];
        let count = parse_stream_count_option(args)?;
        if count == 0 {
            response_out.extend_from_slice(b"*0\r\n");
            return Ok(());
        }

        let stream = match self.load_stream_object(&key)? {
            Some(stream) => stream,
            None => {
                response_out.extend_from_slice(b"*0\r\n");
                return Ok(());
            }
        };

        let entries = collect_stream_range_entries(&stream, start, end, true, count);
        append_stream_entry_array(response_out, &entries);
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
        let key = args[1].to_vec();
        let spec = parse_xtrim_spec(args)?;
        let mut stream = match self.load_stream_object(&key)? {
            Some(stream) => stream,
            None => {
                append_integer(response_out, 0);
                return Ok(());
            }
        };

        let mut entry_ids: Vec<Vec<u8>> = stream.entries.keys().cloned().collect();
        entry_ids.sort_by(|lhs, rhs| compare_stream_ids(lhs, rhs));

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
                    if !compare_stream_ids(&entry_id, &minid).is_lt() {
                        break;
                    }
                    if let Some(limit) = spec.limit {
                        if removed >= limit {
                            break;
                        }
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

fn next_auto_stream_id(stream: &StreamObject) -> Vec<u8> {
    let now = current_unix_time_millis().unwrap_or(0);
    let sequence = stream.entries.len() as u64;
    format!("{now}-{sequence}").into_bytes()
}

fn stream_id_greater(lhs: &[u8], rhs: &[u8]) -> bool {
    compare_stream_ids(lhs, rhs).is_gt()
}

fn parse_stream_id(id: &[u8]) -> Option<(u64, u64)> {
    let text = core::str::from_utf8(id).ok()?;
    if let Some((ms, seq)) = text.split_once('-') {
        return Some((ms.parse::<u64>().ok()?, seq.parse::<u64>().ok()?));
    }
    Some((text.parse::<u64>().ok()?, 0))
}

fn compare_stream_ids(lhs: &[u8], rhs: &[u8]) -> core::cmp::Ordering {
    match (parse_stream_id(lhs), parse_stream_id(rhs)) {
        (Some(lhs_id), Some(rhs_id)) => lhs_id.cmp(&rhs_id),
        _ => lhs.cmp(rhs),
    }
}

fn collect_stream_entries_after_id(
    stream: &StreamObject,
    pivot: &[u8],
    count: usize,
) -> Vec<(Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>)> {
    let mut entries: Vec<(Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>)> = stream
        .entries
        .iter()
        .map(|(id, fields)| (id.clone(), fields.clone()))
        .collect();
    entries.sort_by(|(lhs, _), (rhs, _)| compare_stream_ids(lhs, rhs));

    let mut selected = Vec::new();
    for (id, fields) in entries {
        if !stream_id_greater(&id, pivot) {
            continue;
        }
        selected.push((id, fields));
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

fn append_null_array(response_out: &mut Vec<u8>) {
    response_out.extend_from_slice(b"*-1\r\n");
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
    Minid(Vec<u8>),
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
        XtrimStrategy::Minid(threshold.to_vec())
    } else {
        return Err(RequestExecutionError::SyntaxError);
    };

    Ok(XtrimSpec { strategy, limit })
}

fn collect_stream_range_entries(
    stream: &StreamObject,
    lower_bound: &[u8],
    upper_bound: &[u8],
    reverse: bool,
    count: usize,
) -> Vec<(Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>)> {
    let mut entries: Vec<(Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>)> = stream
        .entries
        .iter()
        .map(|(id, fields)| (id.clone(), fields.clone()))
        .collect();
    entries.sort_by(|(lhs, _), (rhs, _)| compare_stream_ids(lhs, rhs));
    if reverse {
        entries.reverse();
    }

    let mut selected = Vec::new();
    for (id, fields) in entries {
        let lower_ok = if lower_bound == b"-" {
            true
        } else {
            !compare_stream_ids(&id, lower_bound).is_lt()
        };
        if !lower_ok {
            continue;
        }
        let upper_ok = if upper_bound == b"+" {
            true
        } else {
            !compare_stream_ids(&id, upper_bound).is_gt()
        };
        if !upper_ok {
            continue;
        }
        selected.push((id, fields));
        if selected.len() >= count {
            break;
        }
    }
    selected
}

fn append_stream_entry_array(
    response_out: &mut Vec<u8>,
    entries: &[(Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>)],
) {
    response_out.push(b'*');
    response_out.extend_from_slice(entries.len().to_string().as_bytes());
    response_out.extend_from_slice(b"\r\n");
    for (id, fields) in entries {
        response_out.extend_from_slice(b"*2\r\n");
        append_bulk_string(response_out, id);
        response_out.push(b'*');
        response_out.extend_from_slice((fields.len() * 2).to_string().as_bytes());
        response_out.extend_from_slice(b"\r\n");
        for (field, value) in fields {
            append_bulk_string(response_out, field);
            append_bulk_string(response_out, value);
        }
    }
}

fn fnv_hex_digest(tag: u8, payload: &[u8]) -> Vec<u8> {
    let mut hash = 0xcbf29ce484222325u64;
    hash ^= u64::from(tag);
    hash = hash.wrapping_mul(0x100000001b3);
    for byte in payload {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}").into_bytes()
}
