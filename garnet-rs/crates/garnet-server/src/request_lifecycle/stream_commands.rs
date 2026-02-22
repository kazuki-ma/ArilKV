use super::*;

impl RequestProcessor {
    pub(super) fn handle_xadd(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() < 5 || ((args.len() - 3) % 2 != 0) {
            return Err(RequestExecutionError::WrongArity {
                command: "XADD",
                expected: "XADD key id field value [field value ...]",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        // SAFETY: caller guarantees argument backing memory validity.
        let requested_id = unsafe { args[2].as_slice() };

        let mut stream = self.load_stream_object(&key)?.unwrap_or_default();
        let id = if requested_id == b"*" {
            next_auto_stream_id(&stream)
        } else {
            requested_id.to_vec()
        };

        let mut fields = Vec::with_capacity((args.len() - 3) / 2);
        let mut index = 3usize;
        while index < args.len() {
            // SAFETY: caller guarantees argument backing memory validity.
            let field = unsafe { args[index].as_slice() }.to_vec();
            // SAFETY: caller guarantees argument backing memory validity.
            let value = unsafe { args[index + 1].as_slice() }.to_vec();
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
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() < 3 {
            return Err(RequestExecutionError::WrongArity {
                command: "XDEL",
                expected: "XDEL key id [id ...]",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        let mut stream = match self.load_stream_object(&key)? {
            Some(stream) => stream,
            None => {
                append_integer(response_out, 0);
                return Ok(());
            }
        };

        let mut removed = 0i64;
        for id in &args[2..] {
            // SAFETY: caller guarantees argument backing memory validity.
            if stream.entries.remove(unsafe { id.as_slice() }).is_some() {
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
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() < 2 {
            return Err(RequestExecutionError::WrongArity {
                command: "XGROUP",
                expected: "XGROUP <CREATE|SETID> key group id",
            });
        }
        // SAFETY: caller guarantees argument backing memory validity.
        let subcommand = unsafe { args[1].as_slice() };

        if ascii_eq_ignore_case(subcommand, b"CREATE") {
            if args.len() != 5 {
                return Err(RequestExecutionError::WrongArity {
                    command: "XGROUP",
                    expected: "XGROUP CREATE key group id",
                });
            }
            // SAFETY: caller guarantees argument backing memory validity.
            let key = unsafe { args[2].as_slice() }.to_vec();
            // SAFETY: caller guarantees argument backing memory validity.
            let group = unsafe { args[3].as_slice() }.to_vec();
            // SAFETY: caller guarantees argument backing memory validity.
            let id = unsafe { args[4].as_slice() }.to_vec();
            let mut stream = self.load_stream_object(&key)?.unwrap_or_default();
            stream.groups.insert(group, id);
            self.save_stream_object(&key, &stream)?;
            append_simple_string(response_out, b"OK");
            return Ok(());
        }

        if ascii_eq_ignore_case(subcommand, b"SETID") {
            if args.len() != 5 {
                return Err(RequestExecutionError::WrongArity {
                    command: "XGROUP",
                    expected: "XGROUP SETID key group id",
                });
            }
            // SAFETY: caller guarantees argument backing memory validity.
            let key = unsafe { args[2].as_slice() }.to_vec();
            // SAFETY: caller guarantees argument backing memory validity.
            let group = unsafe { args[3].as_slice() }.to_vec();
            // SAFETY: caller guarantees argument backing memory validity.
            let id = unsafe { args[4].as_slice() }.to_vec();
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
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() < 8 {
            return Err(RequestExecutionError::WrongArity {
                command: "XREADGROUP",
                expected: "XREADGROUP GROUP group consumer [NOACK] [COUNT count] STREAMS key >",
            });
        }
        // SAFETY: caller guarantees argument backing memory validity.
        if !ascii_eq_ignore_case(unsafe { args[1].as_slice() }, b"GROUP") {
            return Err(RequestExecutionError::SyntaxError);
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let group = unsafe { args[2].as_slice() }.to_vec();
        let mut count = 1usize;
        let mut key = None::<Vec<u8>>;
        let mut start_id = None::<Vec<u8>>;
        let mut index = 4usize;
        while index < args.len() {
            // SAFETY: caller guarantees argument backing memory validity.
            let token = unsafe { args[index].as_slice() };
            if ascii_eq_ignore_case(token, b"NOACK") {
                index += 1;
                continue;
            }
            if ascii_eq_ignore_case(token, b"COUNT") {
                if index + 1 >= args.len() {
                    return Err(RequestExecutionError::SyntaxError);
                }
                // SAFETY: caller guarantees argument backing memory validity.
                let parsed = parse_u64_ascii(unsafe { args[index + 1].as_slice() })
                    .ok_or(RequestExecutionError::ValueNotInteger)?;
                count = usize::try_from(parsed).unwrap_or(usize::MAX);
                index += 2;
                continue;
            }
            if ascii_eq_ignore_case(token, b"STREAMS") {
                if index + 2 >= args.len() {
                    return Err(RequestExecutionError::SyntaxError);
                }
                // SAFETY: caller guarantees argument backing memory validity.
                key = Some(unsafe { args[index + 1].as_slice() }.to_vec());
                // SAFETY: caller guarantees argument backing memory validity.
                start_id = Some(unsafe { args[index + 2].as_slice() }.to_vec());
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

    pub(super) fn handle_xinfo(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 4 {
            return Err(RequestExecutionError::WrongArity {
                command: "XINFO",
                expected: "XINFO STREAM key FULL",
            });
        }
        // SAFETY: caller guarantees argument backing memory validity.
        let subcommand = unsafe { args[1].as_slice() };
        if !ascii_eq_ignore_case(subcommand, b"STREAM") {
            return Err(RequestExecutionError::UnknownCommand);
        }
        // SAFETY: caller guarantees argument backing memory validity.
        let full = unsafe { args[3].as_slice() };
        if !ascii_eq_ignore_case(full, b"FULL") {
            return Err(RequestExecutionError::UnknownCommand);
        }
        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[2].as_slice() }.to_vec();
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
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 2 {
            return Err(RequestExecutionError::WrongArity {
                command: "XLEN",
                expected: "XLEN key",
            });
        }
        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        let count = self
            .load_stream_object(&key)?
            .map_or(0usize, |stream| stream.entries.len());
        append_integer(response_out, count as i64);
        Ok(())
    }

    pub(super) fn handle_xrange(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 4 && args.len() != 6 {
            return Err(RequestExecutionError::WrongArity {
                command: "XRANGE",
                expected: "XRANGE key start end [COUNT count]",
            });
        }
        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        // SAFETY: caller guarantees argument backing memory validity.
        let start = unsafe { args[2].as_slice() };
        // SAFETY: caller guarantees argument backing memory validity.
        let end = unsafe { args[3].as_slice() };
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
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 4 && args.len() != 6 {
            return Err(RequestExecutionError::WrongArity {
                command: "XREVRANGE",
                expected: "XREVRANGE key end start [COUNT count]",
            });
        }
        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        // SAFETY: caller guarantees argument backing memory validity.
        let end = unsafe { args[2].as_slice() };
        // SAFETY: caller guarantees argument backing memory validity.
        let start = unsafe { args[3].as_slice() };
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
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() < 4 {
            return Err(RequestExecutionError::WrongArity {
                command: "XTRIM",
                expected: "XTRIM key MAXLEN|MINID [=|~] threshold [LIMIT count]",
            });
        }
        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
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

fn parse_stream_count_option(args: &[ArgSlice]) -> Result<usize, RequestExecutionError> {
    if args.len() == 4 {
        return Ok(usize::MAX);
    }
    // SAFETY: caller guarantees argument backing memory validity.
    let option = unsafe { args[4].as_slice() };
    if !ascii_eq_ignore_case(option, b"COUNT") {
        return Err(RequestExecutionError::SyntaxError);
    }
    // SAFETY: caller guarantees argument backing memory validity.
    let parsed = parse_u64_ascii(unsafe { args[5].as_slice() })
        .ok_or(RequestExecutionError::ValueNotInteger)?;
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

fn parse_xtrim_spec(args: &[ArgSlice]) -> Result<XtrimSpec, RequestExecutionError> {
    // SAFETY: caller guarantees argument backing memory validity.
    let strategy_token = unsafe { args[2].as_slice() };
    let mut index = 3usize;
    if index < args.len() {
        // SAFETY: caller guarantees argument backing memory validity.
        let marker = unsafe { args[index].as_slice() };
        if marker == b"=" || marker == b"~" {
            index += 1;
        }
    }
    if index >= args.len() {
        return Err(RequestExecutionError::SyntaxError);
    }
    // SAFETY: caller guarantees argument backing memory validity.
    let threshold = unsafe { args[index].as_slice() };
    index += 1;

    let limit = if index == args.len() {
        None
    } else {
        if index + 2 != args.len() {
            return Err(RequestExecutionError::SyntaxError);
        }
        // SAFETY: caller guarantees argument backing memory validity.
        if !ascii_eq_ignore_case(unsafe { args[index].as_slice() }, b"LIMIT") {
            return Err(RequestExecutionError::SyntaxError);
        }
        // SAFETY: caller guarantees argument backing memory validity.
        let parsed = parse_i64_ascii(unsafe { args[index + 1].as_slice() })
            .ok_or(RequestExecutionError::ValueNotInteger)?;
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
