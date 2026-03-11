use super::object_store::list_listpack_compatible;
use super::object_store::listpack_growth_would_force_quicklist;
use super::object_store::listpack_shrink_should_keep_quicklist;
use super::*;

impl RequestProcessor {
    pub(super) fn handle_lpush(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 3, "LPUSH", "LPUSH key value [value ...]")?;

        let key = RedisKey::from(args[1]);
        let mut list = self.load_list_object(&key)?.unwrap_or_default();
        for value in &args[2..] {
            list.insert(0, value.to_vec());
        }
        self.save_list_object(&key, &list)?;
        self.notify_keyspace_event(current_request_selected_db(), NOTIFY_LIST, b"lpush", &key);
        append_integer(response_out, list.len() as i64);
        Ok(())
    }

    pub(super) fn handle_rpush(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 3, "RPUSH", "RPUSH key value [value ...]")?;

        let key = RedisKey::from(args[1]);
        let mut list = self.load_list_object(&key)?.unwrap_or_default();
        for value in &args[2..] {
            list.push(value.to_vec());
        }
        self.save_list_object(&key, &list)?;
        self.notify_keyspace_event(current_request_selected_db(), NOTIFY_LIST, b"rpush", &key);
        append_integer(response_out, list.len() as i64);
        Ok(())
    }

    pub(super) fn handle_lpop(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_pop_like(args, response_out, ListSide::Left, "LPOP")
    }

    pub(super) fn handle_rpop(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_pop_like(args, response_out, ListSide::Right, "RPOP")
    }

    fn handle_pop_like(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
        side: ListSide,
        command: &'static str,
    ) -> Result<(), RequestExecutionError> {
        let expected = if matches!(side, ListSide::Left) {
            "LPOP key [count]"
        } else {
            "RPOP key [count]"
        };
        ensure_one_of_arities(args, &[2, 3], command, expected)?;

        let key = RedisKey::from(args[1]);
        let count = if args.len() == 3 {
            let parsed = parse_i64_ascii(args[2]).ok_or(RequestExecutionError::ValueNotInteger)?;
            if parsed < 0 {
                return Err(RequestExecutionError::ValueOutOfRangePositive);
            }
            Some(usize::try_from(parsed).unwrap_or(usize::MAX))
        } else {
            None
        };
        let resp3 = self.resp_protocol_version().is_resp3();
        let configured_size = self.list_max_listpack_size.load(Ordering::Acquire);

        let mut list = match self.load_list_object(&key)? {
            Some(list) => list,
            None => {
                if resp3 {
                    append_null(response_out);
                } else if count.is_some() {
                    append_null_array(response_out);
                } else {
                    append_null_bulk_string(response_out);
                }
                return Ok(());
            }
        };
        let previous_was_quicklist = self
            .list_quicklist_encoding_is_forced(DbKeyRef::new(current_request_selected_db(), &key))
            || !list_listpack_compatible(&list, configured_size);
        if list.is_empty() {
            let _ = self.object_delete(DbKeyRef::new(current_request_selected_db(), &key))?;
            if resp3 {
                append_null(response_out);
            } else if count.is_some() {
                append_null_array(response_out);
            } else {
                append_null_bulk_string(response_out);
            }
            return Ok(());
        }

        let event_name: &[u8] = if matches!(side, ListSide::Left) {
            b"lpop"
        } else {
            b"rpop"
        };

        if let Some(count) = count {
            let mut popped: Vec<Vec<u8>> = Vec::new();
            for _ in 0..count {
                let Some(value) = pop_list_side(&mut list, side) else {
                    break;
                };
                popped.push(value);
            }
            if !popped.is_empty() {
                self.notify_keyspace_event(
                    current_request_selected_db(),
                    NOTIFY_LIST,
                    event_name,
                    &key,
                );
            }
            if list.is_empty() {
                let _ = self.object_delete(DbKeyRef::new(current_request_selected_db(), &key))?;
                if !popped.is_empty() {
                    self.notify_keyspace_event(
                        current_request_selected_db(),
                        NOTIFY_GENERIC,
                        b"del",
                        &key,
                    );
                }
            } else {
                self.save_list_object(&key, &list)?;
                self.maybe_preserve_quicklist_after_shrink(
                    &key,
                    previous_was_quicklist,
                    configured_size,
                    &list,
                );
            }
            append_array_length(response_out, popped.len());
            for value in &popped {
                append_bulk_string(response_out, value);
            }
            return Ok(());
        }

        let Some(value) = pop_list_side(&mut list, side) else {
            let _ = self.object_delete(DbKeyRef::new(current_request_selected_db(), &key))?;
            if resp3 {
                append_null(response_out);
            } else {
                append_null_bulk_string(response_out);
            }
            return Ok(());
        };
        self.notify_keyspace_event(current_request_selected_db(), NOTIFY_LIST, event_name, &key);
        if list.is_empty() {
            let _ = self.object_delete(DbKeyRef::new(current_request_selected_db(), &key))?;
            self.notify_keyspace_event(current_request_selected_db(), NOTIFY_GENERIC, b"del", &key);
        } else {
            self.save_list_object(&key, &list)?;
            self.maybe_preserve_quicklist_after_shrink(
                &key,
                previous_was_quicklist,
                configured_size,
                &list,
            );
        }
        append_bulk_string(response_out, &value);
        Ok(())
    }

    pub(super) fn handle_lrange(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 4, "LRANGE", "LRANGE key start stop")?;

        let key = RedisKey::from(args[1]);
        let start = parse_i64_ascii(args[2]).ok_or(RequestExecutionError::ValueNotInteger)?;
        let stop = parse_i64_ascii(args[3]).ok_or(RequestExecutionError::ValueNotInteger)?;
        let list = match self.load_list_object(&key)? {
            Some(list) => list,
            None => {
                append_array_length(response_out, 0);
                return Ok(());
            }
        };

        let len = list.len() as i64;
        if len == 0 {
            append_array_length(response_out, 0);
            return Ok(());
        }

        let mut normalized_start = if start < 0 { len + start } else { start };
        let mut normalized_stop = if stop < 0 { len + stop } else { stop };
        if normalized_start < 0 {
            normalized_start = 0;
        }
        if normalized_stop < 0 || normalized_start >= len {
            append_array_length(response_out, 0);
            return Ok(());
        }
        if normalized_stop >= len {
            normalized_stop = len - 1;
        }
        if normalized_start > normalized_stop {
            append_array_length(response_out, 0);
            return Ok(());
        }

        let count = (normalized_stop - normalized_start + 1) as usize;
        append_array_length(response_out, count);
        for index in normalized_start..=normalized_stop {
            append_bulk_string(response_out, &list[index as usize]);
        }
        Ok(())
    }

    pub(super) fn handle_llen(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 2, "LLEN", "LLEN key")?;

        let key = RedisKey::from(args[1]);
        let length = self
            .load_list_object(&key)?
            .map_or(0, |list| list.len() as i64);
        append_integer(response_out, length);
        Ok(())
    }

    pub(super) fn handle_lindex(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 3, "LINDEX", "LINDEX key index")?;
        let key = RedisKey::from(args[1]);
        let index = parse_i64_ascii(args[2]).ok_or(RequestExecutionError::ValueNotInteger)?;
        let resp3 = self.resp_protocol_version().is_resp3();
        let list = match self.load_list_object(&key)? {
            Some(list) => list,
            None => {
                if resp3 {
                    append_null(response_out);
                } else {
                    append_null_bulk_string(response_out);
                }
                return Ok(());
            }
        };
        let Some(index) = normalize_list_index(list.len(), index) else {
            if resp3 {
                append_null(response_out);
            } else {
                append_null_bulk_string(response_out);
            }
            return Ok(());
        };
        append_bulk_string(response_out, &list[index]);
        Ok(())
    }

    pub(super) fn handle_lpos(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            3,
            "LPOS",
            "LPOS key element [RANK rank] [COUNT num-matches] [MAXLEN len]",
        )?;
        let key = RedisKey::from(args[1]);
        let element = args[2];
        let options = parse_lpos_options(args, 3)?;
        let resp3 = self.resp_protocol_version().is_resp3();
        let Some(list) = self.load_list_object(&key)? else {
            if options.count.is_some() {
                append_array_length(response_out, 0);
            } else if resp3 {
                append_null(response_out);
            } else {
                append_null_bulk_string(response_out);
            }
            return Ok(());
        };

        let rank = options.rank;
        let mut skip_matches = rank.unsigned_abs() - 1;
        let count_limit = options
            .count
            .map(|value| usize::try_from(value).unwrap_or(usize::MAX))
            .unwrap_or(1);
        let unlimited_count = options.count == Some(0);
        let max_scan = options.maxlen.unwrap_or(usize::MAX);

        let mut positions = Vec::new();
        if rank > 0 {
            for (scanned, (index, value)) in list.iter().enumerate().enumerate() {
                if scanned >= max_scan {
                    break;
                }
                if value.as_slice() != element {
                    continue;
                }
                if skip_matches > 0 {
                    skip_matches -= 1;
                    continue;
                }
                positions.push(index as i64);
                if options.count.is_none() || (!unlimited_count && positions.len() >= count_limit) {
                    break;
                }
            }
        } else {
            for (scanned, (index, value)) in list.iter().enumerate().rev().enumerate() {
                if scanned >= max_scan {
                    break;
                }
                if value.as_slice() != element {
                    continue;
                }
                if skip_matches > 0 {
                    skip_matches -= 1;
                    continue;
                }
                positions.push(index as i64);
                if options.count.is_none() || (!unlimited_count && positions.len() >= count_limit) {
                    break;
                }
            }
        }

        if options.count.is_some() {
            append_array_length(response_out, positions.len());
            for position in positions {
                append_integer(response_out, position);
            }
        } else if let Some(position) = positions.first() {
            append_integer(response_out, *position);
        } else if resp3 {
            append_null(response_out);
        } else {
            append_null_bulk_string(response_out);
        }
        Ok(())
    }

    pub(super) fn handle_lset(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 4, "LSET", "LSET key index element")?;
        let key = RedisKey::from(args[1]);
        let index = parse_i64_ascii(args[2]).ok_or(RequestExecutionError::ValueNotInteger)?;
        let value = args[3].to_vec();
        let mut list = self
            .load_list_object(&key)?
            .ok_or(RequestExecutionError::NoSuchKey)?;
        let Some(index) = normalize_list_index(list.len(), index) else {
            return Err(RequestExecutionError::IndexOutOfRange);
        };
        let configured_size = self.list_max_listpack_size.load(Ordering::Acquire);
        let previous_was_quicklist = self
            .list_quicklist_encoding_is_forced(DbKeyRef::new(current_request_selected_db(), &key))
            || !list_listpack_compatible(&list, configured_size);
        let force_quicklist_after_replace = if previous_was_quicklist {
            false
        } else {
            listpack_growth_would_force_quicklist(&list, configured_size, &[value.as_slice()])
        };
        list[index] = value;
        self.save_list_object(&key, &list)?;
        self.notify_keyspace_event(current_request_selected_db(), NOTIFY_LIST, b"lset", &key);
        if force_quicklist_after_replace {
            self.force_list_quicklist_encoding(DbKeyRef::new(current_request_selected_db(), &key));
        } else if previous_was_quicklist {
            self.maybe_preserve_quicklist_after_shrink(
                &key,
                previous_was_quicklist,
                configured_size,
                &list,
            );
        }
        append_simple_string(response_out, b"OK");
        Ok(())
    }

    pub(super) fn handle_ltrim(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 4, "LTRIM", "LTRIM key start stop")?;
        let key = RedisKey::from(args[1]);
        let start = parse_i64_ascii(args[2]).ok_or(RequestExecutionError::ValueNotInteger)?;
        let stop = parse_i64_ascii(args[3]).ok_or(RequestExecutionError::ValueNotInteger)?;
        let list = match self.load_list_object(&key)? {
            Some(list) => list,
            None => {
                append_simple_string(response_out, b"OK");
                return Ok(());
            }
        };

        let len = list.len() as i64;
        if len == 0 {
            let _ = self.object_delete(DbKeyRef::new(current_request_selected_db(), &key))?;
            append_simple_string(response_out, b"OK");
            return Ok(());
        }
        let mut normalized_start = if start < 0 { len + start } else { start };
        let mut normalized_stop = if stop < 0 { len + stop } else { stop };
        if normalized_start < 0 {
            normalized_start = 0;
        }
        if normalized_stop < 0 || normalized_start >= len || normalized_start > normalized_stop {
            let _ = self.object_delete(DbKeyRef::new(current_request_selected_db(), &key))?;
            append_simple_string(response_out, b"OK");
            return Ok(());
        }
        if normalized_stop >= len {
            normalized_stop = len - 1;
        }

        let trimmed = list[normalized_start as usize..=normalized_stop as usize].to_vec();
        let configured_size = self.list_max_listpack_size.load(Ordering::Acquire);
        let previous_was_quicklist = self
            .list_quicklist_encoding_is_forced(DbKeyRef::new(current_request_selected_db(), &key))
            || !list_listpack_compatible(&list, configured_size);
        self.notify_keyspace_event(current_request_selected_db(), NOTIFY_LIST, b"ltrim", &key);
        if trimmed.is_empty() {
            let _ = self.object_delete(DbKeyRef::new(current_request_selected_db(), &key))?;
            self.notify_keyspace_event(current_request_selected_db(), NOTIFY_GENERIC, b"del", &key);
        } else {
            self.save_list_object(&key, &trimmed)?;
            self.maybe_preserve_quicklist_after_shrink(
                &key,
                previous_was_quicklist,
                configured_size,
                &trimmed,
            );
        }
        append_simple_string(response_out, b"OK");
        Ok(())
    }

    pub(super) fn handle_lpushx(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 3, "LPUSHX", "LPUSHX key value [value ...]")?;
        let key = RedisKey::from(args[1]);
        let Some(mut list) = self.load_list_object(&key)? else {
            append_integer(response_out, 0);
            return Ok(());
        };
        for value in &args[2..] {
            list.insert(0, value.to_vec());
        }
        self.save_list_object(&key, &list)?;
        self.notify_keyspace_event(current_request_selected_db(), NOTIFY_LIST, b"lpush", &key);
        append_integer(response_out, list.len() as i64);
        Ok(())
    }

    pub(super) fn handle_rpushx(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 3, "RPUSHX", "RPUSHX key value [value ...]")?;
        let key = RedisKey::from(args[1]);
        let Some(mut list) = self.load_list_object(&key)? else {
            append_integer(response_out, 0);
            return Ok(());
        };
        for value in &args[2..] {
            list.push(value.to_vec());
        }
        self.save_list_object(&key, &list)?;
        self.notify_keyspace_event(current_request_selected_db(), NOTIFY_LIST, b"rpush", &key);
        append_integer(response_out, list.len() as i64);
        Ok(())
    }

    pub(super) fn handle_lrem(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 4, "LREM", "LREM key count element")?;
        let key = RedisKey::from(args[1]);
        let count = parse_i64_ascii(args[2]).ok_or(RequestExecutionError::ValueNotInteger)?;
        let target = args[3].to_vec();
        let Some(mut list) = self.load_list_object(&key)? else {
            append_integer(response_out, 0);
            return Ok(());
        };
        let configured_size = self.list_max_listpack_size.load(Ordering::Acquire);
        let previous_was_quicklist = self
            .list_quicklist_encoding_is_forced(DbKeyRef::new(current_request_selected_db(), &key))
            || !list_listpack_compatible(&list, configured_size);

        let mut removed = 0i64;
        let limit = if count == 0 {
            usize::MAX
        } else {
            usize::try_from(count.unsigned_abs()).unwrap_or(usize::MAX)
        };

        if count >= 0 {
            let mut index = 0usize;
            while index < list.len() && (removed as usize) < limit {
                if list[index] == target {
                    list.remove(index);
                    removed += 1;
                } else {
                    index += 1;
                }
            }
        } else {
            let mut index = list.len();
            while index > 0 && (removed as usize) < limit {
                index -= 1;
                if list[index] == target {
                    list.remove(index);
                    removed += 1;
                }
            }
        }

        if removed > 0 {
            self.notify_keyspace_event(current_request_selected_db(), NOTIFY_LIST, b"lrem", &key);
            if list.is_empty() {
                let _ = self.object_delete(DbKeyRef::new(current_request_selected_db(), &key))?;
                self.notify_keyspace_event(
                    current_request_selected_db(),
                    NOTIFY_GENERIC,
                    b"del",
                    &key,
                );
            } else {
                self.save_list_object(&key, &list)?;
                self.maybe_preserve_quicklist_after_shrink(
                    &key,
                    previous_was_quicklist,
                    configured_size,
                    &list,
                );
            }
        }
        append_integer(response_out, removed);
        Ok(())
    }

    pub(super) fn handle_linsert(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 5, "LINSERT", "LINSERT key BEFORE|AFTER pivot element")?;

        let key = RedisKey::from(args[1]);
        let position = args[2];
        let pivot = args[3];
        let element = args[4].to_vec();

        let Some(mut list) = self.load_list_object(&key)? else {
            append_integer(response_out, 0);
            return Ok(());
        };

        let Some(pivot_index) = list.iter().position(|value| value.as_slice() == pivot) else {
            append_integer(response_out, -1);
            return Ok(());
        };

        if ascii_eq_ignore_case(position, b"BEFORE") {
            list.insert(pivot_index, element);
        } else if ascii_eq_ignore_case(position, b"AFTER") {
            list.insert(pivot_index + 1, element);
        } else {
            return Err(RequestExecutionError::SyntaxError);
        }
        self.save_list_object(&key, &list)?;
        self.notify_keyspace_event(current_request_selected_db(), NOTIFY_LIST, b"linsert", &key);
        append_integer(response_out, list.len() as i64);
        Ok(())
    }

    pub(super) fn handle_lmove(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(
            args,
            5,
            "LMOVE",
            "LMOVE source destination LEFT|RIGHT LEFT|RIGHT",
        )?;
        let source = RedisKey::from(args[1]);
        let destination = RedisKey::from(args[2]);
        let source_side = parse_list_side(args[3]).ok_or(RequestExecutionError::SyntaxError)?;
        let destination_side =
            parse_list_side(args[4]).ok_or(RequestExecutionError::SyntaxError)?;

        self.handle_lmove_like(
            &source,
            &destination,
            source_side,
            destination_side,
            response_out,
        )
    }

    pub(super) fn handle_rpoplpush(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 3, "RPOPLPUSH", "RPOPLPUSH source destination")?;

        let source = RedisKey::from(args[1]);
        let destination = RedisKey::from(args[2]);
        self.handle_lmove_like(
            &source,
            &destination,
            ListSide::Right,
            ListSide::Left,
            response_out,
        )
    }

    pub(super) fn handle_lmpop(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            4,
            "LMPOP",
            "LMPOP numkeys key [key ...] LEFT|RIGHT [COUNT count]",
        )?;
        let parsed_keys = parse_list_numkeys_and_keys(args, 1)?;
        if parsed_keys.option_start >= args.len() {
            return Err(RequestExecutionError::SyntaxError);
        }
        let side = parse_list_side(args[parsed_keys.option_start])
            .ok_or(RequestExecutionError::SyntaxError)?;
        let count = parse_list_count_option(args, parsed_keys.option_start + 1)?;
        self.handle_lmpop_like(&parsed_keys.keys, side, count, response_out)
    }

    pub(super) fn handle_blmpop(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            5,
            "BLMPOP",
            "BLMPOP timeout numkeys key [key ...] LEFT|RIGHT [COUNT count]",
        )?;
        parse_blocking_timeout_seconds(args, 1)?;
        let parsed_keys = parse_list_numkeys_and_keys(args, 2)?;
        if parsed_keys.option_start >= args.len() {
            return Err(RequestExecutionError::SyntaxError);
        }
        let side = parse_list_side(args[parsed_keys.option_start])
            .ok_or(RequestExecutionError::SyntaxError)?;
        let count = parse_list_count_option(args, parsed_keys.option_start + 1)?;
        self.handle_lmpop_like(&parsed_keys.keys, side, count, response_out)
    }

    pub(super) fn handle_blpop(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 3, "BLPOP", "BLPOP key [key ...] timeout")?;
        let timeout_index = args.len() - 1;
        parse_blocking_timeout_seconds(args, timeout_index)?;
        let keys = args[1..timeout_index]
            .iter()
            .map(|key| key.to_vec())
            .collect::<Vec<_>>();
        self.handle_blocking_pop_like(&keys, ListSide::Left, response_out)
    }

    pub(super) fn handle_brpop(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 3, "BRPOP", "BRPOP key [key ...] timeout")?;
        let timeout_index = args.len() - 1;
        parse_blocking_timeout_seconds(args, timeout_index)?;
        let keys = args[1..timeout_index]
            .iter()
            .map(|key| key.to_vec())
            .collect::<Vec<_>>();
        self.handle_blocking_pop_like(&keys, ListSide::Right, response_out)
    }

    pub(super) fn handle_blmove(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(
            args,
            6,
            "BLMOVE",
            "BLMOVE source destination LEFT|RIGHT LEFT|RIGHT timeout",
        )?;
        let source = RedisKey::from(args[1]);
        let destination = RedisKey::from(args[2]);
        let source_side = parse_list_side(args[3]).ok_or(RequestExecutionError::SyntaxError)?;
        let destination_side =
            parse_list_side(args[4]).ok_or(RequestExecutionError::SyntaxError)?;
        parse_blocking_timeout_seconds(args, 5)?;
        self.handle_lmove_like(
            &source,
            &destination,
            source_side,
            destination_side,
            response_out,
        )
    }

    pub(super) fn handle_brpoplpush(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(
            args,
            4,
            "BRPOPLPUSH",
            "BRPOPLPUSH source destination timeout",
        )?;
        let source = RedisKey::from(args[1]);
        let destination = RedisKey::from(args[2]);
        parse_blocking_timeout_seconds(args, 3)?;
        self.handle_lmove_like(
            &source,
            &destination,
            ListSide::Right,
            ListSide::Left,
            response_out,
        )
    }

    fn handle_lmove_like(
        &self,
        source: &[u8],
        destination: &[u8],
        source_side: ListSide,
        destination_side: ListSide,
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        let resp3 = self.resp_protocol_version().is_resp3();
        let Some(mut source_list) = self.load_list_object(source)? else {
            if resp3 {
                append_null(response_out);
            } else {
                append_null_bulk_string(response_out);
            }
            return Ok(());
        };
        let configured_size = self.list_max_listpack_size.load(Ordering::Acquire);
        let source_was_quicklist = self.list_quicklist_encoding_is_forced(DbKeyRef::new(
            current_request_selected_db(),
            source,
        )) || !list_listpack_compatible(&source_list, configured_size);
        if source_list.is_empty() {
            let _ = self.object_delete(DbKeyRef::new(current_request_selected_db(), source))?;
            if resp3 {
                append_null(response_out);
            } else {
                append_null_bulk_string(response_out);
            }
            return Ok(());
        }

        if source == destination {
            let value = pop_list_side(&mut source_list, source_side)
                .expect("source_list was checked as non-empty");
            push_list_side(&mut source_list, destination_side, value.clone());
            self.save_list_object(source, &source_list)?;
            append_bulk_string(response_out, &value);
            return Ok(());
        }

        let mut destination_list = self.load_list_object(destination)?.unwrap_or_default();
        let value = pop_list_side(&mut source_list, source_side)
            .expect("source_list was checked as non-empty");
        push_list_side(&mut destination_list, destination_side, value.clone());

        if source_list.is_empty() {
            let _ = self.object_delete(DbKeyRef::new(current_request_selected_db(), source))?;
        } else {
            self.save_list_object(source, &source_list)?;
            self.maybe_preserve_quicklist_after_shrink(
                source,
                source_was_quicklist,
                configured_size,
                &source_list,
            );
        }
        self.save_list_object(destination, &destination_list)?;
        append_bulk_string(response_out, &value);
        Ok(())
    }

    fn handle_blocking_pop_like(
        &self,
        keys: &[Vec<u8>],
        side: ListSide,
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        for key in keys {
            let Some(popped_values) = self.pop_list_values(key, side, 1)? else {
                continue;
            };
            append_blocking_pop_response(response_out, key, &popped_values[0]);
            return Ok(());
        }
        if self.resp_protocol_version().is_resp3() {
            append_null(response_out);
        } else {
            append_null_array(response_out);
        }
        Ok(())
    }

    fn handle_lmpop_like(
        &self,
        keys: &[Vec<u8>],
        side: ListSide,
        count: usize,
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        for key in keys {
            let Some(popped_values) = self.pop_list_values(key, side, count)? else {
                continue;
            };
            append_lmpop_response(response_out, key, &popped_values);
            return Ok(());
        }
        if self.resp_protocol_version().is_resp3() {
            append_null(response_out);
        } else {
            append_null_array(response_out);
        }
        Ok(())
    }

    fn pop_list_values(
        &self,
        key: &[u8],
        side: ListSide,
        count: usize,
    ) -> Result<Option<Vec<Vec<u8>>>, RequestExecutionError> {
        let Some(mut list) = self.load_list_object(key)? else {
            return Ok(None);
        };
        let configured_size = self.list_max_listpack_size.load(Ordering::Acquire);
        let previous_was_quicklist = self
            .list_quicklist_encoding_is_forced(DbKeyRef::new(current_request_selected_db(), key))
            || !list_listpack_compatible(&list, configured_size);
        if list.is_empty() {
            let _ = self.object_delete(DbKeyRef::new(current_request_selected_db(), key))?;
            return Ok(None);
        }

        let pop_count = count.min(list.len());
        let mut popped = Vec::with_capacity(pop_count);
        match side {
            ListSide::Left => {
                popped.extend(list.drain(0..pop_count));
            }
            ListSide::Right => {
                for _ in 0..pop_count {
                    popped.push(list.pop().expect("pop_count is bounded by list length"));
                }
            }
        }

        if list.is_empty() {
            let _ = self.object_delete(DbKeyRef::new(current_request_selected_db(), key))?;
        } else {
            self.save_list_object(key, &list)?;
            self.maybe_preserve_quicklist_after_shrink(
                key,
                previous_was_quicklist,
                configured_size,
                &list,
            );
        }
        Ok(Some(popped))
    }

    fn maybe_preserve_quicklist_after_shrink(
        &self,
        key: &[u8],
        previous_was_quicklist: bool,
        configured_size: i64,
        list: &[Vec<u8>],
    ) {
        if !previous_was_quicklist {
            return;
        }
        if !list_listpack_compatible(list, configured_size) {
            return;
        }
        if listpack_shrink_should_keep_quicklist(list, configured_size) {
            self.force_list_quicklist_encoding(DbKeyRef::new(current_request_selected_db(), key));
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum ListSide {
    Left,
    Right,
}

#[derive(Clone, Copy, Debug)]
struct LposOptions {
    rank: i64,
    count: Option<u64>,
    maxlen: Option<usize>,
}

fn parse_list_side(input: &[u8]) -> Option<ListSide> {
    if ascii_eq_ignore_case(input, b"LEFT") {
        return Some(ListSide::Left);
    }
    if ascii_eq_ignore_case(input, b"RIGHT") {
        return Some(ListSide::Right);
    }
    None
}

fn parse_lpos_options(
    args: &[&[u8]],
    start_index: usize,
) -> Result<LposOptions, RequestExecutionError> {
    let mut options = LposOptions {
        rank: 1,
        count: None,
        maxlen: None,
    };
    let mut index = start_index;
    while index < args.len() {
        if index + 1 >= args.len() {
            return Err(RequestExecutionError::SyntaxError);
        }
        let option = args[index];
        let value = args[index + 1];
        if ascii_eq_ignore_case(option, b"RANK") {
            let parsed = parse_i64_ascii(value).ok_or(RequestExecutionError::ValueNotInteger)?;
            if parsed == i64::MIN {
                return Err(RequestExecutionError::ValueOutOfRange);
            }
            if parsed == 0 {
                return Err(RequestExecutionError::LposRankZero);
            }
            options.rank = parsed;
        } else if ascii_eq_ignore_case(option, b"COUNT") {
            let parsed = parse_i64_ascii(value).ok_or(RequestExecutionError::ValueNotInteger)?;
            if parsed < 0 {
                return Err(RequestExecutionError::ValueOutOfRange);
            }
            options.count = Some(parsed as u64);
        } else if ascii_eq_ignore_case(option, b"MAXLEN") {
            let parsed = parse_i64_ascii(value).ok_or(RequestExecutionError::ValueNotInteger)?;
            if parsed < 0 {
                return Err(RequestExecutionError::ValueOutOfRange);
            }
            if parsed == 0 {
                options.maxlen = None;
            } else {
                options.maxlen = Some(
                    usize::try_from(parsed).map_err(|_| RequestExecutionError::ValueOutOfRange)?,
                );
            }
        } else {
            return Err(RequestExecutionError::SyntaxError);
        }
        index += 2;
    }
    Ok(options)
}

fn pop_list_side(list: &mut Vec<Vec<u8>>, side: ListSide) -> Option<Vec<u8>> {
    match side {
        ListSide::Left => {
            if list.is_empty() {
                None
            } else {
                Some(list.remove(0))
            }
        }
        ListSide::Right => list.pop(),
    }
}

fn push_list_side(list: &mut Vec<Vec<u8>>, side: ListSide, value: Vec<u8>) {
    match side {
        ListSide::Left => list.insert(0, value),
        ListSide::Right => list.push(value),
    }
}

struct ParsedListKeys {
    keys: Vec<Vec<u8>>,
    option_start: usize,
}

fn parse_list_numkeys_and_keys(
    args: &[&[u8]],
    numkeys_index: usize,
) -> Result<ParsedListKeys, RequestExecutionError> {
    if numkeys_index >= args.len() {
        return Err(RequestExecutionError::SyntaxError);
    }
    let raw_numkeys = parse_i64_ascii(args[numkeys_index])
        .ok_or(RequestExecutionError::NumkeysMustBeGreaterThanZero)?;
    if raw_numkeys <= 0 {
        return Err(RequestExecutionError::NumkeysMustBeGreaterThanZero);
    }
    let numkeys =
        usize::try_from(raw_numkeys).map_err(|_| RequestExecutionError::ValueOutOfRange)?;
    let key_start = numkeys_index + 1;
    let key_end = key_start
        .checked_add(numkeys)
        .ok_or(RequestExecutionError::ValueOutOfRange)?;
    if key_end > args.len() {
        return Err(RequestExecutionError::SyntaxError);
    }
    let keys = args[key_start..key_end]
        .iter()
        .map(|key| key.to_vec())
        .collect();
    Ok(ParsedListKeys {
        keys,
        option_start: key_end,
    })
}

fn parse_list_count_option(
    args: &[&[u8]],
    start_index: usize,
) -> Result<usize, RequestExecutionError> {
    if start_index == args.len() {
        return Ok(1);
    }
    if start_index + 2 != args.len() {
        return Err(RequestExecutionError::SyntaxError);
    }
    let count_token = args[start_index];
    if !ascii_eq_ignore_case(count_token, b"COUNT") {
        return Err(RequestExecutionError::SyntaxError);
    }
    let count = parse_i64_ascii(args[start_index + 1])
        .ok_or(RequestExecutionError::CountMustBeGreaterThanZero)?;
    if count <= 0 {
        return Err(RequestExecutionError::CountMustBeGreaterThanZero);
    }
    Ok(usize::try_from(count).unwrap_or(usize::MAX))
}

fn parse_blocking_timeout_seconds(
    args: &[&[u8]],
    index: usize,
) -> Result<f64, RequestExecutionError> {
    let timeout_token = args[index];
    let timeout_text =
        std::str::from_utf8(timeout_token).map_err(|_| RequestExecutionError::ValueNotFloat)?;
    let timeout = match timeout_text.parse::<f64>() {
        Ok(parsed) => parsed,
        Err(_) => {
            if looks_numeric_timeout_token(timeout_token) {
                return Err(RequestExecutionError::ValueOutOfRange);
            }
            return Err(RequestExecutionError::ValueNotFloat);
        }
    };
    if !timeout.is_finite() {
        return Err(RequestExecutionError::ValueOutOfRange);
    }
    if timeout < 0.0 {
        return Err(RequestExecutionError::TimeoutIsNegative);
    }
    Ok(timeout)
}

fn looks_numeric_timeout_token(token: &[u8]) -> bool {
    if token.is_empty() {
        return false;
    }
    let (sign_len, body) = match token.first().copied() {
        Some(b'+') | Some(b'-') if token.len() > 1 => (1usize, &token[1..]),
        _ => (0usize, token),
    };
    if body.len() > 2
        && body[0] == b'0'
        && (body[1] == b'x' || body[1] == b'X')
        && body[2..].iter().all(|byte| byte.is_ascii_hexdigit())
    {
        return true;
    }
    let mut seen_digit = false;
    for (index, byte) in token.iter().copied().enumerate() {
        let is_sign = byte == b'+' || byte == b'-';
        if is_sign && index == 0 && sign_len == 1 {
            continue;
        }
        if byte.is_ascii_digit() {
            seen_digit = true;
            continue;
        }
        if matches!(byte, b'.' | b'e' | b'E') {
            continue;
        }
        return false;
    }
    seen_digit
}

fn append_blocking_pop_response(response_out: &mut Vec<u8>, key: &[u8], value: &[u8]) {
    append_array_length(response_out, 2);
    append_bulk_string(response_out, key);
    append_bulk_string(response_out, value);
}

fn append_lmpop_response(response_out: &mut Vec<u8>, key: &[u8], values: &[Vec<u8>]) {
    append_array_length(response_out, 2);
    append_bulk_string(response_out, key);
    append_array_length(response_out, values.len());
    for value in values {
        append_bulk_string(response_out, value);
    }
}

fn normalize_list_index(len: usize, index: i64) -> Option<usize> {
    if len == 0 {
        return None;
    }
    let len_i = len as i128;
    let mut index_i = i128::from(index);
    if index_i < 0 {
        index_i += len_i;
    }
    if index_i < 0 || index_i >= len_i {
        return None;
    }
    Some(index_i as usize)
}
