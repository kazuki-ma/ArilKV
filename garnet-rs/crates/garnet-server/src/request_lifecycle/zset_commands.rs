use super::*;

impl RequestProcessor {
    pub(super) fn handle_zadd(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_paired_arity_after(
            args,
            4,
            2,
            "ZADD",
            "ZADD key score member [score member ...]",
        )?;

        let key = RedisKey::from(args[1]);
        let mut zset = self.load_zset_object(&key)?.unwrap_or_default();
        let mut inserted = 0i64;

        let mut index = 2usize;
        while index < args.len() {
            let score =
                parse_zset_score_value(args[index]).ok_or(RequestExecutionError::ValueNotFloat)?;
            let member = args[index + 1].to_vec();
            if zset.insert(member, score).is_none() {
                inserted += 1;
            }
            index += 2;
        }

        self.save_zset_object(&key, &zset)?;
        self.notify_keyspace_event(NOTIFY_ZSET, b"zadd", &key);
        append_integer(response_out, inserted);
        Ok(())
    }

    pub(super) fn handle_zrem(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 3, "ZREM", "ZREM key member [member ...]")?;

        let key = RedisKey::from(args[1]);
        let mut zset = match self.load_zset_object(&key)? {
            Some(zset) => zset,
            None => {
                append_integer(response_out, 0);
                return Ok(());
            }
        };

        let mut removed = 0i64;
        for member in &args[2..] {
            if zset.remove(*member).is_some() {
                removed += 1;
            }
        }

        if removed > 0 {
            self.notify_keyspace_event(NOTIFY_ZSET, b"zrem", &key);
        }
        if zset.is_empty() {
            let _ = self.object_delete(&key)?;
            if removed > 0 {
                self.notify_keyspace_event(NOTIFY_GENERIC, b"del", &key);
            }
        } else {
            self.save_zset_object(&key, &zset)?;
        }
        append_integer(response_out, removed);
        Ok(())
    }

    pub(super) fn handle_zrange(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_ranged_arity(args, 4, 5, "ZRANGE", "ZRANGE key start stop [WITHSCORES]")?;
        let with_scores = if args.len() == 5 {
            if !ascii_eq_ignore_case(args[4], b"WITHSCORES") {
                return Err(RequestExecutionError::SyntaxError);
            }
            true
        } else {
            false
        };

        let key = RedisKey::from(args[1]);
        let start = parse_i64_ascii(args[2]).ok_or(RequestExecutionError::ValueNotInteger)?;
        let stop = parse_i64_ascii(args[3]).ok_or(RequestExecutionError::ValueNotInteger)?;
        let zset = match self.load_zset_object(&key)? {
            Some(zset) => zset,
            None => {
                response_out.extend_from_slice(b"*0\r\n");
                return Ok(());
            }
        };

        let entries = sorted_zset_entries_by_score(&zset);
        let Some(range) = normalize_zset_index_range(entries.len(), start, stop) else {
            response_out.extend_from_slice(b"*0\r\n");
            return Ok(());
        };

        let selected = &entries[range.start..=range.end_inclusive];
        append_zrange_score_entries(
            response_out,
            selected,
            with_scores,
            self.emit_resp3_zset_pairs(),
        );
        Ok(())
    }

    pub(super) fn handle_zrevrange(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_ranged_arity(
            args,
            4,
            5,
            "ZREVRANGE",
            "ZREVRANGE key start stop [WITHSCORES]",
        )?;
        let with_scores = if args.len() == 5 {
            let option = args[4];
            if !ascii_eq_ignore_case(option, b"WITHSCORES") {
                return Err(RequestExecutionError::SyntaxError);
            }
            true
        } else {
            false
        };

        let key = RedisKey::from(args[1]);
        let start = parse_i64_ascii(args[2]).ok_or(RequestExecutionError::ValueNotInteger)?;
        let stop = parse_i64_ascii(args[3]).ok_or(RequestExecutionError::ValueNotInteger)?;
        let zset = match self.load_zset_object(&key)? {
            Some(zset) => zset,
            None => {
                response_out.extend_from_slice(b"*0\r\n");
                return Ok(());
            }
        };

        let mut entries = sorted_zset_entries_by_score(&zset);
        entries.reverse();
        let Some(range) = normalize_zset_index_range(entries.len(), start, stop) else {
            response_out.extend_from_slice(b"*0\r\n");
            return Ok(());
        };

        let selected = &entries[range.start..=range.end_inclusive];
        append_zrange_score_entries(
            response_out,
            selected,
            with_scores,
            self.emit_resp3_zset_pairs(),
        );
        Ok(())
    }

    pub(super) fn handle_zrangebyscore(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_zrangebyscore_like(args, response_out, false)
    }

    pub(super) fn handle_zrevrangebyscore(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_zrangebyscore_like(args, response_out, true)
    }

    pub(super) fn handle_zscore(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 3, "ZSCORE", "ZSCORE key member")?;

        let key = RedisKey::from(args[1]);
        let member = args[2];
        let zset = match self.load_zset_object(&key)? {
            Some(zset) => zset,
            None => {
                append_null_bulk_string(response_out);
                return Ok(());
            }
        };

        match zset.get(member) {
            Some(score) => append_bulk_string(response_out, score.to_string().as_bytes()),
            None => append_null_bulk_string(response_out),
        }
        Ok(())
    }

    pub(super) fn handle_zcard(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 2, "ZCARD", "ZCARD key")?;

        let key = RedisKey::from(args[1]);
        let count = self
            .load_zset_object(&key)?
            .map_or(0usize, |zset| zset.len());
        append_integer(response_out, count as i64);
        Ok(())
    }

    pub(super) fn handle_zcount(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 4, "ZCOUNT", "ZCOUNT key min max")?;

        let key = RedisKey::from(args[1]);
        let min = parse_zscore_bound(args[2]).ok_or(RequestExecutionError::ValueNotFloat)?;
        let max = parse_zscore_bound(args[3]).ok_or(RequestExecutionError::ValueNotFloat)?;
        let zset = match self.load_zset_object(&key)? {
            Some(zset) => zset,
            None => {
                append_integer(response_out, 0);
                return Ok(());
            }
        };

        let count = zset
            .values()
            .filter(|score| score_in_bound(**score, min, max))
            .count();
        append_integer(response_out, count as i64);
        Ok(())
    }

    pub(super) fn handle_zlexcount(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 4, "ZLEXCOUNT", "ZLEXCOUNT key min max")?;
        let key = RedisKey::from(args[1]);
        let min = parse_zlex_bound(args[2]).ok_or(RequestExecutionError::SyntaxError)?;
        let max = parse_zlex_bound(args[3]).ok_or(RequestExecutionError::SyntaxError)?;

        let count = match self.load_zset_object(&key)? {
            Some(zset) => zset
                .keys()
                .filter(|member| zlex_member_in_bounds(member.as_slice(), min, max))
                .count(),
            None => 0usize,
        };
        append_integer(response_out, count as i64);
        Ok(())
    }

    pub(super) fn handle_zrangebylex(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_zrangebylex_like(args, response_out, false)
    }

    pub(super) fn handle_zrevrangebylex(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_zrangebylex_like(args, response_out, true)
    }

    pub(super) fn handle_zremrangebylex(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 4, "ZREMRANGEBYLEX", "ZREMRANGEBYLEX key min max")?;
        let key = RedisKey::from(args[1]);
        let min = parse_zlex_bound(args[2]).ok_or(RequestExecutionError::SyntaxError)?;
        let max = parse_zlex_bound(args[3]).ok_or(RequestExecutionError::SyntaxError)?;

        let mut zset = match self.load_zset_object(&key)? {
            Some(zset) => zset,
            None => {
                append_integer(response_out, 0);
                return Ok(());
            }
        };

        let to_remove: Vec<Vec<u8>> = zset
            .keys()
            .filter(|member| zlex_member_in_bounds(member.as_slice(), min, max))
            .cloned()
            .collect();
        let removed = to_remove
            .into_iter()
            .filter(|member| zset.remove(member).is_some())
            .count() as i64;
        if removed == 0 {
            append_integer(response_out, 0);
            return Ok(());
        }
        if zset.is_empty() {
            let _ = self.object_delete(&key)?;
        } else {
            self.save_zset_object(&key, &zset)?;
        }
        append_integer(response_out, removed);
        Ok(())
    }

    pub(super) fn handle_zintercard(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            3,
            "ZINTERCARD",
            "ZINTERCARD numkeys key [key ...] [LIMIT limit]",
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
        let mut cardinality = compute_zinter_cardinality(self, &keys)?;
        if limit > 0 {
            cardinality = cardinality.min(limit);
        }
        append_integer(response_out, cardinality as i64);
        Ok(())
    }

    pub(super) fn handle_zdiff(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 3, "ZDIFF", "ZDIFF numkeys key [key ...] [WITHSCORES]")?;
        let parsed_keys = parse_zset_numkeys_and_keys(args, 1)?;
        let with_scores = parse_zdiff_withscores(args, parsed_keys.option_start)?;
        let diff = compute_zdiff(self, &parsed_keys.keys)?;
        append_zset_combine_response(
            response_out,
            &diff,
            with_scores,
            self.emit_resp3_zset_pairs(),
        );
        Ok(())
    }

    pub(super) fn handle_zdiffstore(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            4,
            "ZDIFFSTORE",
            "ZDIFFSTORE destination numkeys key [key ...]",
        )?;
        let destination = RedisKey::from(args[1]);
        let parsed_keys = parse_zset_numkeys_and_keys(args, 2)?;
        if parsed_keys.option_start != args.len() {
            return Err(RequestExecutionError::SyntaxError);
        }
        let diff = compute_zdiff(self, &parsed_keys.keys)?;
        store_zset_result(self, &destination, &diff)?;
        append_integer(response_out, diff.len() as i64);
        Ok(())
    }

    pub(super) fn handle_zinter(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            3,
            "ZINTER",
            "ZINTER numkeys key [key ...] [WEIGHTS w [w ...]] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]",
        )?;
        let parsed_keys = parse_zset_numkeys_and_keys(args, 1)?;
        let combine_options = parse_zset_combine_options(
            args,
            parsed_keys.option_start,
            parsed_keys.keys.len(),
            /*allow_with_scores=*/ true,
        )?;
        let inter = compute_zinter(
            self,
            &parsed_keys.keys,
            &combine_options.weights,
            combine_options.aggregate,
        )?;
        append_zset_combine_response(
            response_out,
            &inter,
            combine_options.with_scores,
            self.emit_resp3_zset_pairs(),
        );
        Ok(())
    }

    pub(super) fn handle_zinterstore(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            4,
            "ZINTERSTORE",
            "ZINTERSTORE destination numkeys key [key ...] [WEIGHTS w [w ...]] [AGGREGATE SUM|MIN|MAX]",
        )?;
        let destination = RedisKey::from(args[1]);
        let parsed_keys = parse_zset_numkeys_and_keys(args, 2)?;
        let combine_options = parse_zset_combine_options(
            args,
            parsed_keys.option_start,
            parsed_keys.keys.len(),
            /*allow_with_scores=*/ false,
        )?;
        let inter = compute_zinter(
            self,
            &parsed_keys.keys,
            &combine_options.weights,
            combine_options.aggregate,
        )?;
        store_zset_result(self, &destination, &inter)?;
        append_integer(response_out, inter.len() as i64);
        Ok(())
    }

    pub(super) fn handle_zunion(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            3,
            "ZUNION",
            "ZUNION numkeys key [key ...] [WEIGHTS w [w ...]] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]",
        )?;
        let parsed_keys = parse_zset_numkeys_and_keys(args, 1)?;
        let combine_options = parse_zset_combine_options(
            args,
            parsed_keys.option_start,
            parsed_keys.keys.len(),
            /*allow_with_scores=*/ true,
        )?;
        let union = compute_zunion(
            self,
            &parsed_keys.keys,
            &combine_options.weights,
            combine_options.aggregate,
        )?;
        append_zset_combine_response(
            response_out,
            &union,
            combine_options.with_scores,
            self.emit_resp3_zset_pairs(),
        );
        Ok(())
    }

    pub(super) fn handle_zunionstore(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            4,
            "ZUNIONSTORE",
            "ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS w [w ...]] [AGGREGATE SUM|MIN|MAX]",
        )?;
        let destination = RedisKey::from(args[1]);
        let parsed_keys = parse_zset_numkeys_and_keys(args, 2)?;
        let combine_options = parse_zset_combine_options(
            args,
            parsed_keys.option_start,
            parsed_keys.keys.len(),
            /*allow_with_scores=*/ false,
        )?;
        let union = compute_zunion(
            self,
            &parsed_keys.keys,
            &combine_options.weights,
            combine_options.aggregate,
        )?;
        store_zset_result(self, &destination, &union)?;
        append_integer(response_out, union.len() as i64);
        Ok(())
    }

    pub(super) fn handle_zmpop(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            4,
            "ZMPOP",
            "ZMPOP numkeys key [key ...] <MIN|MAX> [COUNT count]",
        )?;
        let parsed_keys = parse_zset_numkeys_and_keys(args, 1)?;
        let pop_options = parse_zmpop_direction_and_count(args, parsed_keys.option_start)?;
        self.handle_zmpop_like(
            &parsed_keys.keys,
            pop_options.pop_max,
            pop_options.count,
            response_out,
        )
    }

    pub(super) fn handle_bzmpop(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            5,
            "BZMPOP",
            "BZMPOP timeout numkeys key [key ...] <MIN|MAX> [COUNT count]",
        )?;
        parse_blocking_timeout_seconds(args, 1)?;
        let parsed_keys = parse_zset_numkeys_and_keys(args, 2)?;
        let pop_options = parse_zmpop_direction_and_count(args, parsed_keys.option_start)?;
        self.handle_zmpop_like(
            &parsed_keys.keys,
            pop_options.pop_max,
            pop_options.count,
            response_out,
        )
    }

    pub(super) fn handle_zrangestore(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            5,
            "ZRANGESTORE",
            "ZRANGESTORE dst src min max [BYSCORE|BYLEX] [REV] [LIMIT offset count]",
        )?;
        let destination = RedisKey::from(args[1]);
        let source = RedisKey::from(args[2]);
        let left = args[3];
        let right = args[4];
        let options = parse_zrangestore_options(args, 5)?;

        let selected = match self.load_zset_object(&source)? {
            Some(zset) => collect_zrangestore_entries(&zset, left, right, options)?,
            None => Vec::new(),
        };
        let result = zset_map_from_entries(selected);
        store_zset_result(self, &destination, &result)?;
        append_integer(response_out, result.len() as i64);
        Ok(())
    }

    pub(super) fn handle_zscan(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            3,
            "ZSCAN",
            "ZSCAN key cursor [MATCH pattern] [COUNT count]",
        )?;

        let key = RedisKey::from(args[1]);
        let cursor = parse_u64_ascii(args[2]).ok_or(RequestExecutionError::ValueNotInteger)?;
        let scan_options = parse_scan_match_count_options(args, 3)?;

        let resp3 = self.resp_protocol_version().is_resp3();
        let Some(zset) = self.load_zset_object(&key)? else {
            append_zset_scan_response(response_out, cursor, scan_options.count, &[], resp3);
            return Ok(());
        };

        let mut matched = Vec::new();
        for (member, score) in &zset {
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
            matched.push((member.as_slice(), *score));
        }
        append_zset_scan_response(response_out, cursor, scan_options.count, &matched, resp3);
        Ok(())
    }

    pub(super) fn handle_zrank(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_zrank_like(args, response_out, false)
    }

    pub(super) fn handle_zrevrank(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_zrank_like(args, response_out, true)
    }

    pub(super) fn handle_zincrby(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 4, "ZINCRBY", "ZINCRBY key increment member")?;

        let key = RedisKey::from(args[1]);
        let increment = parse_f64_ascii(args[2]).ok_or(RequestExecutionError::ValueNotFloat)?;
        let member = args[3].to_vec();

        let mut zset = self.load_zset_object(&key)?.unwrap_or_default();
        let current = zset.get(&member).copied().unwrap_or(0.0);
        let updated = current + increment;
        if !updated.is_finite() {
            return Err(RequestExecutionError::ValueNotFloat);
        }
        zset.insert(member, updated);
        self.save_zset_object(&key, &zset)?;
        self.notify_keyspace_event(NOTIFY_ZSET, b"zincr", &key);
        append_bulk_string(response_out, updated.to_string().as_bytes());
        Ok(())
    }

    pub(super) fn handle_zremrangebyrank(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 4, "ZREMRANGEBYRANK", "ZREMRANGEBYRANK key start stop")?;
        let key = RedisKey::from(args[1]);
        let start = parse_i64_ascii(args[2]).ok_or(RequestExecutionError::ValueNotInteger)?;
        let stop = parse_i64_ascii(args[3]).ok_or(RequestExecutionError::ValueNotInteger)?;

        let mut zset = match self.load_zset_object(&key)? {
            Some(zset) => zset,
            None => {
                append_integer(response_out, 0);
                return Ok(());
            }
        };

        let entries = sorted_zset_entries_by_score(&zset);
        let Some(range) = normalize_zset_index_range(entries.len(), start, stop) else {
            append_integer(response_out, 0);
            return Ok(());
        };
        let to_remove: Vec<Vec<u8>> = entries[range.start..=range.end_inclusive]
            .iter()
            .map(|(member, _score)| (*member).clone())
            .collect();
        let removed = to_remove
            .into_iter()
            .filter(|member| zset.remove(member).is_some())
            .count() as i64;

        if zset.is_empty() {
            let _ = self.object_delete(&key)?;
        } else {
            self.save_zset_object(&key, &zset)?;
        }
        append_integer(response_out, removed);
        Ok(())
    }

    pub(super) fn handle_zremrangebyscore(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 4, "ZREMRANGEBYSCORE", "ZREMRANGEBYSCORE key min max")?;
        let key = RedisKey::from(args[1]);
        let min = parse_zscore_bound(args[2]).ok_or(RequestExecutionError::ValueNotFloat)?;
        let max = parse_zscore_bound(args[3]).ok_or(RequestExecutionError::ValueNotFloat)?;

        let mut zset = match self.load_zset_object(&key)? {
            Some(zset) => zset,
            None => {
                append_integer(response_out, 0);
                return Ok(());
            }
        };
        let to_remove: Vec<Vec<u8>> = zset
            .iter()
            .filter(|(_member, score)| score_in_bound(**score, min, max))
            .map(|(member, _score)| member.clone())
            .collect();
        let removed = to_remove
            .into_iter()
            .filter(|member| zset.remove(member).is_some())
            .count() as i64;

        if removed == 0 {
            append_integer(response_out, 0);
            return Ok(());
        }
        if zset.is_empty() {
            let _ = self.object_delete(&key)?;
        } else {
            self.save_zset_object(&key, &zset)?;
        }
        append_integer(response_out, removed);
        Ok(())
    }

    fn handle_zrangebyscore_like(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
        reverse: bool,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            4,
            if reverse {
                "ZREVRANGEBYSCORE"
            } else {
                "ZRANGEBYSCORE"
            },
            if reverse {
                "ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]"
            } else {
                "ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]"
            },
        )?;

        let key = RedisKey::from(args[1]);
        let left_bound = parse_zscore_bound(args[2]).ok_or(RequestExecutionError::ValueNotFloat)?;
        let right_bound =
            parse_zscore_bound(args[3]).ok_or(RequestExecutionError::ValueNotFloat)?;
        let options = parse_zrange_by_score_options(args, 4)?;

        let zset = match self.load_zset_object(&key)? {
            Some(zset) => zset,
            None => {
                response_out.extend_from_slice(b"*0\r\n");
                return Ok(());
            }
        };

        let (min_bound, max_bound) = if reverse {
            (right_bound, left_bound)
        } else {
            (left_bound, right_bound)
        };
        let mut matched: Vec<(&Vec<u8>, f64)> = sorted_zset_entries_by_score(&zset)
            .into_iter()
            .filter(|(_member, score)| score_in_bound(*score, min_bound, max_bound))
            .collect();
        if reverse {
            matched.reverse();
        }

        let selected: Vec<(&Vec<u8>, f64)> = if let Some(limit) = options.limit {
            matched
                .into_iter()
                .skip(limit.offset)
                .take(limit.count)
                .collect()
        } else {
            matched
        };
        append_zrange_score_entries(
            response_out,
            &selected,
            options.with_scores,
            self.emit_resp3_zset_pairs(),
        );
        Ok(())
    }

    fn handle_zrangebylex_like(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
        reverse: bool,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            4,
            if reverse {
                "ZREVRANGEBYLEX"
            } else {
                "ZRANGEBYLEX"
            },
            if reverse {
                "ZREVRANGEBYLEX key max min [LIMIT offset count]"
            } else {
                "ZRANGEBYLEX key min max [LIMIT offset count]"
            },
        )?;

        let key = RedisKey::from(args[1]);
        let left_bound = parse_zlex_bound(args[2]).ok_or(RequestExecutionError::SyntaxError)?;
        let right_bound = parse_zlex_bound(args[3]).ok_or(RequestExecutionError::SyntaxError)?;
        let limit = parse_zrangebylex_limit(args, 4)?;
        let (min_bound, max_bound) = if reverse {
            (right_bound, left_bound)
        } else {
            (left_bound, right_bound)
        };

        let zset = match self.load_zset_object(&key)? {
            Some(zset) => zset,
            None => {
                response_out.extend_from_slice(b"*0\r\n");
                return Ok(());
            }
        };

        let mut matched = Vec::new();
        if reverse {
            for member in zset.keys().rev() {
                if zlex_member_in_bounds(member.as_slice(), min_bound, max_bound) {
                    matched.push(member);
                }
            }
        } else {
            for member in zset.keys() {
                if zlex_member_in_bounds(member.as_slice(), min_bound, max_bound) {
                    matched.push(member);
                }
            }
        }

        let selected: Vec<&Vec<u8>> = if let Some(limit) = limit {
            matched
                .into_iter()
                .skip(limit.offset)
                .take(limit.count)
                .collect()
        } else {
            matched
        };
        response_out.push(b'*');
        response_out.extend_from_slice(selected.len().to_string().as_bytes());
        response_out.extend_from_slice(b"\r\n");
        for member in selected {
            append_bulk_string(response_out, member);
        }
        Ok(())
    }

    pub(super) fn handle_zmscore(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 3, "ZMSCORE", "ZMSCORE key member [member ...]")?;
        let key = RedisKey::from(args[1]);
        let zset = self.load_zset_object(&key)?;
        let members = &args[2..];

        response_out.push(b'*');
        response_out.extend_from_slice(members.len().to_string().as_bytes());
        response_out.extend_from_slice(b"\r\n");
        for member in members {
            let score = zset.as_ref().and_then(|zset| zset.get(*member));
            match score {
                Some(score) => append_bulk_string(response_out, score.to_string().as_bytes()),
                None => append_null_bulk_string(response_out),
            }
        }
        Ok(())
    }

    pub(super) fn handle_zrandmember(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_ranged_arity(
            args,
            2,
            4,
            "ZRANDMEMBER",
            "ZRANDMEMBER key [count [WITHSCORES]]",
        )?;
        let key = RedisKey::from(args[1]);
        let zset = self.load_zset_object(&key)?;
        if args.len() == 2 {
            let Some(zset) = zset else {
                append_null_bulk_string(response_out);
                return Ok(());
            };
            if zset.is_empty() {
                append_null_bulk_string(response_out);
                return Ok(());
            }
            let sampled = select_random_zset_entries_distinct(self, &zset, 1);
            append_bulk_string(response_out, sampled[0].0.as_slice());
            return Ok(());
        }

        let count = parse_i64_ascii(args[2]).ok_or(RequestExecutionError::ValueNotInteger)?;
        let with_scores = if args.len() == 4 {
            let option = args[3];
            if !ascii_eq_ignore_case(option, b"WITHSCORES") {
                return Err(RequestExecutionError::SyntaxError);
            }
            true
        } else {
            false
        };

        let Some(zset) = zset else {
            response_out.extend_from_slice(b"*0\r\n");
            return Ok(());
        };
        if zset.is_empty() || count == 0 {
            response_out.extend_from_slice(b"*0\r\n");
            return Ok(());
        }

        let sampled = if count > 0 {
            let requested = usize::try_from(count).unwrap_or(usize::MAX);
            select_random_zset_entries_distinct(self, &zset, requested)
        } else {
            let requested = usize::try_from(count.unsigned_abs())
                .map_err(|_| RequestExecutionError::ValueOutOfRange)?;
            select_random_zset_entries_with_replacement(self, &zset, requested)
        };

        let emit_pair_array = with_scores && self.emit_resp3_zset_pairs();
        let response_items = if emit_pair_array {
            sampled.len()
        } else if with_scores {
            sampled.len() * 2
        } else {
            sampled.len()
        };
        response_out.push(b'*');
        response_out.extend_from_slice(response_items.to_string().as_bytes());
        response_out.extend_from_slice(b"\r\n");
        for (member, score) in sampled {
            if emit_pair_array {
                response_out.extend_from_slice(b"*2\r\n");
                append_bulk_string(response_out, member.as_slice());
                append_bulk_string(response_out, score.to_string().as_bytes());
                continue;
            }

            append_bulk_string(response_out, member.as_slice());
            if with_scores {
                append_bulk_string(response_out, score.to_string().as_bytes());
            }
        }
        Ok(())
    }

    pub(super) fn handle_zpopmin(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_zpop_like(args, response_out, false)
    }

    pub(super) fn handle_zpopmax(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_zpop_like(args, response_out, true)
    }

    pub(super) fn handle_bzpopmin(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_bzpop_like(args, response_out, false)
    }

    pub(super) fn handle_bzpopmax(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_bzpop_like(args, response_out, true)
    }

    fn handle_zrank_like(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
        reverse: bool,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(
            args,
            3,
            if reverse { "ZREVRANK" } else { "ZRANK" },
            if reverse {
                "ZREVRANK key member"
            } else {
                "ZRANK key member"
            },
        )?;

        let key = RedisKey::from(args[1]);
        let member = args[2];
        let zset = match self.load_zset_object(&key)? {
            Some(zset) => zset,
            None => {
                append_null_bulk_string(response_out);
                return Ok(());
            }
        };
        let entries = sorted_zset_entries_by_score(&zset);
        let Some(index) = entries
            .iter()
            .position(|(candidate_member, _score)| candidate_member.as_slice() == member)
        else {
            append_null_bulk_string(response_out);
            return Ok(());
        };
        let rank = if reverse {
            entries.len() - 1 - index
        } else {
            index
        };
        append_integer(response_out, rank as i64);
        Ok(())
    }

    fn handle_zpop_like(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
        pop_max: bool,
    ) -> Result<(), RequestExecutionError> {
        ensure_ranged_arity(
            args,
            2,
            3,
            if pop_max { "ZPOPMAX" } else { "ZPOPMIN" },
            if pop_max {
                "ZPOPMAX key [count]"
            } else {
                "ZPOPMIN key [count]"
            },
        )?;
        let key = RedisKey::from(args[1]);
        let count = if args.len() == 3 {
            parse_u64_ascii(args[2]).ok_or(RequestExecutionError::ValueNotInteger)?
        } else {
            1
        };
        if count == 0 {
            response_out.extend_from_slice(b"*0\r\n");
            return Ok(());
        }
        let requested = usize::try_from(count).unwrap_or(usize::MAX);
        let Some(selected) = self.pop_zset_entries_from_key(&key, pop_max, requested)? else {
            response_out.extend_from_slice(b"*0\r\n");
            return Ok(());
        };

        let emit_pair_array = args.len() == 3 && self.emit_resp3_zset_pairs();
        response_out.push(b'*');
        response_out.extend_from_slice(
            if emit_pair_array {
                selected.len()
            } else {
                selected.len() * 2
            }
            .to_string()
            .as_bytes(),
        );
        response_out.extend_from_slice(b"\r\n");
        for (member, score) in selected {
            if emit_pair_array {
                response_out.extend_from_slice(b"*2\r\n");
                append_bulk_string(response_out, member.as_slice());
                append_bulk_string(response_out, score.to_string().as_bytes());
                continue;
            }
            append_bulk_string(response_out, member.as_slice());
            append_bulk_string(response_out, score.to_string().as_bytes());
        }
        Ok(())
    }

    fn handle_bzpop_like(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
        pop_max: bool,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            3,
            if pop_max { "BZPOPMAX" } else { "BZPOPMIN" },
            if pop_max {
                "BZPOPMAX key [key ...] timeout"
            } else {
                "BZPOPMIN key [key ...] timeout"
            },
        )?;
        let timeout_index = args.len() - 1;
        parse_blocking_timeout_seconds(args, timeout_index)?;

        for key in &args[1..timeout_index] {
            let key = key.to_vec();
            let Some(mut selected) = self.pop_zset_entries_from_key(&key, pop_max, 1)? else {
                continue;
            };
            let entry = selected
                .pop()
                .expect("pop_zset_entries_from_key returns one element when requested=1");
            append_bzpop_response(response_out, &key, &entry);
            return Ok(());
        }

        response_out.extend_from_slice(b"*-1\r\n");
        Ok(())
    }

    fn handle_zmpop_like(
        &self,
        keys: &[Vec<u8>],
        pop_max: bool,
        count: usize,
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        for key in keys {
            let Some(selected) = self.pop_zset_entries_from_key(key, pop_max, count)? else {
                continue;
            };
            append_zmpop_response(response_out, key, &selected);
            return Ok(());
        }
        response_out.extend_from_slice(b"*-1\r\n");
        Ok(())
    }

    #[allow(clippy::type_complexity)]
    fn pop_zset_entries_from_key(
        &self,
        key: &[u8],
        pop_max: bool,
        count: usize,
    ) -> Result<Option<Vec<(Vec<u8>, f64)>>, RequestExecutionError> {
        let mut zset = match self.load_zset_object(key)? {
            Some(zset) => zset,
            None => return Ok(None),
        };
        if zset.is_empty() {
            let _ = self.object_delete(key)?;
            return Ok(None);
        }

        let mut entries = sorted_zset_entries_by_score(&zset);
        if pop_max {
            entries.reverse();
        }
        let selected_count = count.min(entries.len());
        if selected_count == 0 {
            return Ok(None);
        }
        let selected: Vec<(Vec<u8>, f64)> = entries
            .into_iter()
            .take(selected_count)
            .map(|(member, score)| (member.clone(), score))
            .collect();
        for (member, _score) in &selected {
            zset.remove(member);
        }
        if zset.is_empty() {
            let _ = self.object_delete(key)?;
        } else {
            self.save_zset_object(key, &zset)?;
        }
        Ok(Some(selected))
    }
}

#[derive(Debug, Clone, Copy)]
enum ZscoreBound {
    NegInf,
    PosInf,
    Inclusive(f64),
    Exclusive(f64),
}

#[derive(Debug, Clone, Copy)]
enum ZlexBound<'a> {
    NegInf,
    PosInf,
    Inclusive(&'a [u8]),
    Exclusive(&'a [u8]),
}

#[derive(Debug, Clone, Copy)]
enum ZsetAggregateMode {
    Sum,
    Min,
    Max,
}

#[derive(Debug)]
struct ZsetCombineOptions {
    weights: Vec<f64>,
    aggregate: ZsetAggregateMode,
    with_scores: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ZrangeStoreMode {
    Rank,
    Score,
    Lex,
}

#[derive(Debug, Clone, Copy)]
struct ZsetLimit {
    offset: usize,
    count: usize,
}

#[derive(Debug, Clone, Copy)]
struct ZrangeStoreOptions {
    mode: ZrangeStoreMode,
    reverse: bool,
    limit: Option<ZsetLimit>,
}

fn sorted_zset_entries_by_score(zset: &BTreeMap<Vec<u8>, f64>) -> Vec<(&Vec<u8>, f64)> {
    let mut entries: Vec<(&Vec<u8>, f64)> = zset
        .iter()
        .map(|(member, score)| (member, *score))
        .collect();
    entries.sort_by(|(lhs_member, lhs_score), (rhs_member, rhs_score)| {
        lhs_score
            .partial_cmp(rhs_score)
            .expect("zset scores are finite")
            .then_with(|| lhs_member.cmp(rhs_member))
    });
    entries
}

fn append_zset_scan_response(
    response_out: &mut Vec<u8>,
    cursor: u64,
    count: usize,
    entries: &[(&[u8], f64)],
    resp3: bool,
) {
    let raw_start = usize::try_from(cursor).unwrap_or(usize::MAX);
    let start = if raw_start >= entries.len() {
        0
    } else {
        raw_start
    };
    let end = start.saturating_add(count).min(entries.len());
    let next_cursor = if end >= entries.len() { 0 } else { end };
    let page = &entries[start..end];

    response_out.extend_from_slice(b"*2\r\n");
    let next_cursor_bytes = next_cursor.to_string();
    append_bulk_string(response_out, next_cursor_bytes.as_bytes());

    // In RESP3, emit member-score pairs as a map.
    if resp3 {
        append_map_length(response_out, page.len());
    } else {
        append_array_length(response_out, page.len() * 2);
    }

    for (member, score) in page {
        append_bulk_string(response_out, member);
        append_bulk_string(response_out, score.to_string().as_bytes());
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct ZrangeByScoreOptions {
    with_scores: bool,
    limit: Option<ZsetLimit>,
}

fn parse_zrange_by_score_options(
    args: &[&[u8]],
    start_index: usize,
) -> Result<ZrangeByScoreOptions, RequestExecutionError> {
    let mut options = ZrangeByScoreOptions::default();
    let mut index = start_index;
    while index < args.len() {
        let token = args[index];
        if ascii_eq_ignore_case(token, b"WITHSCORES") {
            if options.with_scores {
                return Err(RequestExecutionError::SyntaxError);
            }
            options.with_scores = true;
            index += 1;
            continue;
        }
        if ascii_eq_ignore_case(token, b"LIMIT") {
            if options.limit.is_some() || index + 2 >= args.len() {
                return Err(RequestExecutionError::SyntaxError);
            }
            let offset =
                parse_u64_ascii(args[index + 1]).ok_or(RequestExecutionError::ValueNotInteger)?;
            let count =
                parse_u64_ascii(args[index + 2]).ok_or(RequestExecutionError::ValueNotInteger)?;
            options.limit = Some(ZsetLimit {
                offset: usize::try_from(offset).unwrap_or(usize::MAX),
                count: usize::try_from(count).unwrap_or(usize::MAX),
            });
            index += 3;
            continue;
        }
        return Err(RequestExecutionError::SyntaxError);
    }
    Ok(options)
}

fn append_zrange_score_entries(
    response_out: &mut Vec<u8>,
    entries: &[(&Vec<u8>, f64)],
    with_scores: bool,
    resp3: bool,
) {
    if with_scores && resp3 {
        response_out.push(b'*');
        response_out.extend_from_slice(entries.len().to_string().as_bytes());
        response_out.extend_from_slice(b"\r\n");
        for (member, score) in entries {
            response_out.extend_from_slice(b"*2\r\n");
            append_bulk_string(response_out, member);
            append_bulk_string(response_out, score.to_string().as_bytes());
        }
        return;
    }

    let item_count = if with_scores {
        entries.len() * 2
    } else {
        entries.len()
    };
    response_out.push(b'*');
    response_out.extend_from_slice(item_count.to_string().as_bytes());
    response_out.extend_from_slice(b"\r\n");
    for (member, score) in entries {
        append_bulk_string(response_out, member);
        if with_scores {
            append_bulk_string(response_out, score.to_string().as_bytes());
        }
    }
}

fn normalize_zset_index_range(len: usize, start: i64, stop: i64) -> Option<NormalizedRange> {
    if len == 0 {
        return None;
    }
    let len_i = i64::try_from(len).ok()?;
    let mut normalized_start = if start < 0 { len_i + start } else { start };
    let mut normalized_stop = if stop < 0 { len_i + stop } else { stop };
    if normalized_start < 0 {
        normalized_start = 0;
    }
    if normalized_stop < 0 || normalized_start >= len_i {
        return None;
    }
    if normalized_stop >= len_i {
        normalized_stop = len_i - 1;
    }
    if normalized_start > normalized_stop {
        return None;
    }
    Some(NormalizedRange::new(
        normalized_start as usize,
        normalized_stop as usize,
    ))
}

fn parse_zscore_bound(input: &[u8]) -> Option<ZscoreBound> {
    if ascii_eq_ignore_case(input, b"-INF") {
        return Some(ZscoreBound::NegInf);
    }
    if ascii_eq_ignore_case(input, b"+INF") || ascii_eq_ignore_case(input, b"INF") {
        return Some(ZscoreBound::PosInf);
    }
    if let Some(exclusive) = input.strip_prefix(b"(") {
        return parse_f64_ascii(exclusive).map(ZscoreBound::Exclusive);
    }
    parse_f64_ascii(input).map(ZscoreBound::Inclusive)
}

fn parse_zset_score_value(input: &[u8]) -> Option<f64> {
    if ascii_eq_ignore_case(input, b"-INF") {
        return Some(f64::NEG_INFINITY);
    }
    if ascii_eq_ignore_case(input, b"+INF") || ascii_eq_ignore_case(input, b"INF") {
        return Some(f64::INFINITY);
    }
    parse_f64_ascii(input)
}

fn parse_zlex_bound(input: &[u8]) -> Option<ZlexBound<'_>> {
    if input == b"-" {
        return Some(ZlexBound::NegInf);
    }
    if input == b"+" {
        return Some(ZlexBound::PosInf);
    }
    if let Some(value) = input.strip_prefix(b"[") {
        return Some(ZlexBound::Inclusive(value));
    }
    if let Some(value) = input.strip_prefix(b"(") {
        return Some(ZlexBound::Exclusive(value));
    }
    None
}

fn zlex_member_in_bounds(member: &[u8], min: ZlexBound<'_>, max: ZlexBound<'_>) -> bool {
    let lower_ok = match min {
        ZlexBound::NegInf => true,
        ZlexBound::PosInf => false,
        ZlexBound::Inclusive(value) => member >= value,
        ZlexBound::Exclusive(value) => member > value,
    };
    if !lower_ok {
        return false;
    }
    match max {
        ZlexBound::NegInf => false,
        ZlexBound::PosInf => true,
        ZlexBound::Inclusive(value) => member <= value,
        ZlexBound::Exclusive(value) => member < value,
    }
}

fn parse_zrangebylex_limit(
    args: &[&[u8]],
    start_index: usize,
) -> Result<Option<ZsetLimit>, RequestExecutionError> {
    if args.len() == start_index {
        return Ok(None);
    }
    if args.len() != start_index + 3 {
        return Err(RequestExecutionError::SyntaxError);
    }
    let option = args[start_index];
    if !ascii_eq_ignore_case(option, b"LIMIT") {
        return Err(RequestExecutionError::SyntaxError);
    }
    let offset =
        parse_u64_ascii(args[start_index + 1]).ok_or(RequestExecutionError::ValueNotInteger)?;
    let count =
        parse_u64_ascii(args[start_index + 2]).ok_or(RequestExecutionError::ValueNotInteger)?;
    Ok(Some(ZsetLimit {
        offset: usize::try_from(offset).unwrap_or(usize::MAX),
        count: usize::try_from(count).unwrap_or(usize::MAX),
    }))
}

fn parse_zset_numkeys_and_keys(
    args: &[&[u8]],
    numkeys_index: usize,
) -> Result<ParsedZsetKeys, RequestExecutionError> {
    if numkeys_index >= args.len() {
        return Err(RequestExecutionError::SyntaxError);
    }
    let numkeys =
        parse_u64_ascii(args[numkeys_index]).ok_or(RequestExecutionError::ValueNotInteger)?;
    if numkeys == 0 {
        return Err(RequestExecutionError::SyntaxError);
    }
    let numkeys = usize::try_from(numkeys).map_err(|_| RequestExecutionError::ValueOutOfRange)?;
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
    Ok(ParsedZsetKeys {
        keys,
        option_start: key_end,
    })
}

struct ParsedZsetKeys {
    keys: Vec<Vec<u8>>,
    option_start: usize,
}

fn parse_zdiff_withscores(
    args: &[&[u8]],
    start_index: usize,
) -> Result<bool, RequestExecutionError> {
    if start_index == args.len() {
        return Ok(false);
    }
    if start_index + 1 != args.len() {
        return Err(RequestExecutionError::SyntaxError);
    }
    let token = args[start_index];
    if !ascii_eq_ignore_case(token, b"WITHSCORES") {
        return Err(RequestExecutionError::SyntaxError);
    }
    Ok(true)
}

fn parse_zset_combine_options(
    args: &[&[u8]],
    start_index: usize,
    num_keys: usize,
    allow_with_scores: bool,
) -> Result<ZsetCombineOptions, RequestExecutionError> {
    let mut weights = vec![1.0f64; num_keys];
    let mut aggregate = ZsetAggregateMode::Sum;
    let mut with_scores = false;
    let mut seen_weights = false;
    let mut seen_aggregate = false;
    let mut index = start_index;
    while index < args.len() {
        let token = args[index];
        if ascii_eq_ignore_case(token, b"WEIGHTS") {
            if seen_weights {
                return Err(RequestExecutionError::SyntaxError);
            }
            if args.len().saturating_sub(index + 1) < num_keys {
                return Err(RequestExecutionError::SyntaxError);
            }
            for (weight_index, weight_arg) in
                args[index + 1..index + 1 + num_keys].iter().enumerate()
            {
                let parsed =
                    parse_f64_ascii(weight_arg).ok_or(RequestExecutionError::ValueNotFloat)?;
                weights[weight_index] = parsed;
            }
            seen_weights = true;
            index += 1 + num_keys;
            continue;
        }
        if ascii_eq_ignore_case(token, b"AGGREGATE") {
            if seen_aggregate || index + 1 >= args.len() {
                return Err(RequestExecutionError::SyntaxError);
            }
            let aggregate_token = args[index + 1];
            aggregate = if ascii_eq_ignore_case(aggregate_token, b"SUM") {
                ZsetAggregateMode::Sum
            } else if ascii_eq_ignore_case(aggregate_token, b"MIN") {
                ZsetAggregateMode::Min
            } else if ascii_eq_ignore_case(aggregate_token, b"MAX") {
                ZsetAggregateMode::Max
            } else {
                return Err(RequestExecutionError::SyntaxError);
            };
            seen_aggregate = true;
            index += 2;
            continue;
        }
        if allow_with_scores && ascii_eq_ignore_case(token, b"WITHSCORES") {
            if with_scores {
                return Err(RequestExecutionError::SyntaxError);
            }
            with_scores = true;
            index += 1;
            continue;
        }
        return Err(RequestExecutionError::SyntaxError);
    }
    Ok(ZsetCombineOptions {
        weights,
        aggregate,
        with_scores,
    })
}

fn aggregate_zset_scores(current: f64, incoming: f64, mode: ZsetAggregateMode) -> f64 {
    match mode {
        ZsetAggregateMode::Sum => current + incoming,
        ZsetAggregateMode::Min => current.min(incoming),
        ZsetAggregateMode::Max => current.max(incoming),
    }
}

fn compute_zdiff(
    processor: &RequestProcessor,
    keys: &[Vec<u8>],
) -> Result<BTreeMap<Vec<u8>, f64>, RequestExecutionError> {
    let Some((first_key, remaining_keys)) = keys.split_first() else {
        return Ok(BTreeMap::new());
    };
    let mut diff = match processor.load_zset_object(first_key)? {
        Some(zset) => zset,
        None => return Ok(BTreeMap::new()),
    };
    for key in remaining_keys {
        let Some(other) = processor.load_zset_object(key)? else {
            continue;
        };
        diff.retain(|member, _score| !other.contains_key(member));
        if diff.is_empty() {
            break;
        }
    }
    Ok(diff)
}

fn compute_zinter(
    processor: &RequestProcessor,
    keys: &[Vec<u8>],
    weights: &[f64],
    aggregate_mode: ZsetAggregateMode,
) -> Result<BTreeMap<Vec<u8>, f64>, RequestExecutionError> {
    let Some((first_key, remaining_keys)) = keys.split_first() else {
        return Ok(BTreeMap::new());
    };
    let Some(first_set) = processor.load_zset_object(first_key)? else {
        return Ok(BTreeMap::new());
    };
    let mut inter: BTreeMap<Vec<u8>, f64> = first_set
        .into_iter()
        .map(|(member, score)| (member, score * weights[0]))
        .collect();

    for (index, key) in remaining_keys.iter().enumerate() {
        let Some(other) = processor.load_zset_object(key)? else {
            return Ok(BTreeMap::new());
        };
        let weight = weights[index + 1];
        inter.retain(|member, score| {
            let Some(other_score) = other.get(member) else {
                return false;
            };
            let weighted_other = *other_score * weight;
            *score = aggregate_zset_scores(*score, weighted_other, aggregate_mode);
            true
        });
        if inter.is_empty() {
            break;
        }
    }
    Ok(inter)
}

fn compute_zunion(
    processor: &RequestProcessor,
    keys: &[Vec<u8>],
    weights: &[f64],
    aggregate_mode: ZsetAggregateMode,
) -> Result<BTreeMap<Vec<u8>, f64>, RequestExecutionError> {
    let mut union = BTreeMap::new();
    for (index, key) in keys.iter().enumerate() {
        let Some(zset) = processor.load_zset_object(key)? else {
            continue;
        };
        let weight = weights[index];
        for (member, score) in zset {
            let weighted = score * weight;
            match union.get_mut(&member) {
                Some(existing) => {
                    *existing = aggregate_zset_scores(*existing, weighted, aggregate_mode);
                }
                None => {
                    union.insert(member, weighted);
                }
            }
        }
    }
    Ok(union)
}

fn append_zset_combine_response(
    response_out: &mut Vec<u8>,
    zset: &BTreeMap<Vec<u8>, f64>,
    with_scores: bool,
    resp3: bool,
) {
    let entries = sorted_zset_entries_by_score(zset);
    append_zrange_score_entries(response_out, &entries, with_scores, resp3);
}

fn append_zmpop_response(response_out: &mut Vec<u8>, key: &[u8], entries: &[(Vec<u8>, f64)]) {
    response_out.extend_from_slice(b"*2\r\n");
    append_bulk_string(response_out, key);
    response_out.push(b'*');
    response_out.extend_from_slice(entries.len().to_string().as_bytes());
    response_out.extend_from_slice(b"\r\n");
    for (member, score) in entries {
        response_out.extend_from_slice(b"*2\r\n");
        append_bulk_string(response_out, member);
        append_bulk_string(response_out, score.to_string().as_bytes());
    }
}

fn append_bzpop_response(response_out: &mut Vec<u8>, key: &[u8], entry: &(Vec<u8>, f64)) {
    response_out.extend_from_slice(b"*3\r\n");
    append_bulk_string(response_out, key);
    append_bulk_string(response_out, entry.0.as_slice());
    append_bulk_string(response_out, entry.1.to_string().as_bytes());
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
        return Err(RequestExecutionError::ValueOutOfRange);
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

fn parse_zmpop_direction_and_count(
    args: &[&[u8]],
    option_start: usize,
) -> Result<ParsedZmpopOptions, RequestExecutionError> {
    if option_start >= args.len() {
        return Err(RequestExecutionError::SyntaxError);
    }
    let direction = args[option_start];
    let pop_max = if ascii_eq_ignore_case(direction, b"MAX") {
        true
    } else if ascii_eq_ignore_case(direction, b"MIN") {
        false
    } else {
        return Err(RequestExecutionError::SyntaxError);
    };

    let count = if option_start + 1 == args.len() {
        1usize
    } else {
        if option_start + 3 != args.len() {
            return Err(RequestExecutionError::SyntaxError);
        }
        let count_token = args[option_start + 1];
        if !ascii_eq_ignore_case(count_token, b"COUNT") {
            return Err(RequestExecutionError::SyntaxError);
        }
        let parsed = parse_u64_ascii(args[option_start + 2])
            .ok_or(RequestExecutionError::ValueNotInteger)?;
        if parsed == 0 {
            return Err(RequestExecutionError::ValueOutOfRange);
        }
        usize::try_from(parsed).unwrap_or(usize::MAX)
    };
    Ok(ParsedZmpopOptions { pop_max, count })
}

struct ParsedZmpopOptions {
    pop_max: bool,
    count: usize,
}

fn parse_zrangestore_options(
    args: &[&[u8]],
    start_index: usize,
) -> Result<ZrangeStoreOptions, RequestExecutionError> {
    let mut mode = ZrangeStoreMode::Rank;
    let mut reverse = false;
    let mut limit = None;
    let mut index = start_index;
    while index < args.len() {
        let token = args[index];
        if ascii_eq_ignore_case(token, b"BYSCORE") {
            if mode != ZrangeStoreMode::Rank {
                return Err(RequestExecutionError::SyntaxError);
            }
            mode = ZrangeStoreMode::Score;
            index += 1;
            continue;
        }
        if ascii_eq_ignore_case(token, b"BYLEX") {
            if mode != ZrangeStoreMode::Rank {
                return Err(RequestExecutionError::SyntaxError);
            }
            mode = ZrangeStoreMode::Lex;
            index += 1;
            continue;
        }
        if ascii_eq_ignore_case(token, b"REV") {
            if reverse {
                return Err(RequestExecutionError::SyntaxError);
            }
            reverse = true;
            index += 1;
            continue;
        }
        if ascii_eq_ignore_case(token, b"LIMIT") {
            if limit.is_some() || index + 2 >= args.len() {
                return Err(RequestExecutionError::SyntaxError);
            }
            let offset =
                parse_u64_ascii(args[index + 1]).ok_or(RequestExecutionError::ValueNotInteger)?;
            let count =
                parse_u64_ascii(args[index + 2]).ok_or(RequestExecutionError::ValueNotInteger)?;
            limit = Some(ZsetLimit {
                offset: usize::try_from(offset).unwrap_or(usize::MAX),
                count: usize::try_from(count).unwrap_or(usize::MAX),
            });
            index += 3;
            continue;
        }
        return Err(RequestExecutionError::SyntaxError);
    }
    Ok(ZrangeStoreOptions {
        mode,
        reverse,
        limit,
    })
}

fn apply_optional_limit(
    entries: Vec<(Vec<u8>, f64)>,
    limit: Option<ZsetLimit>,
) -> Vec<(Vec<u8>, f64)> {
    match limit {
        Some(limit) => entries
            .into_iter()
            .skip(limit.offset)
            .take(limit.count)
            .collect(),
        None => entries,
    }
}

fn collect_zrangestore_entries(
    zset: &BTreeMap<Vec<u8>, f64>,
    left: &[u8],
    right: &[u8],
    options: ZrangeStoreOptions,
) -> Result<Vec<(Vec<u8>, f64)>, RequestExecutionError> {
    let selected = match options.mode {
        ZrangeStoreMode::Rank => {
            let start = parse_i64_ascii(left).ok_or(RequestExecutionError::ValueNotInteger)?;
            let stop = parse_i64_ascii(right).ok_or(RequestExecutionError::ValueNotInteger)?;
            let mut entries = sorted_zset_entries_by_score(zset);
            if options.reverse {
                entries.reverse();
            }
            let Some(range) = normalize_zset_index_range(entries.len(), start, stop) else {
                return Ok(Vec::new());
            };
            entries[range.start..=range.end_inclusive]
                .iter()
                .map(|(member, score)| ((*member).clone(), *score))
                .collect::<Vec<_>>()
        }
        ZrangeStoreMode::Score => {
            let left_bound =
                parse_zscore_bound(left).ok_or(RequestExecutionError::ValueNotFloat)?;
            let right_bound =
                parse_zscore_bound(right).ok_or(RequestExecutionError::ValueNotFloat)?;
            let (min_bound, max_bound) = if options.reverse {
                (right_bound, left_bound)
            } else {
                (left_bound, right_bound)
            };
            let mut entries: Vec<(Vec<u8>, f64)> = sorted_zset_entries_by_score(zset)
                .into_iter()
                .filter(|(_member, score)| score_in_bound(*score, min_bound, max_bound))
                .map(|(member, score)| (member.clone(), score))
                .collect();
            if options.reverse {
                entries.reverse();
            }
            entries
        }
        ZrangeStoreMode::Lex => {
            let left_bound = parse_zlex_bound(left).ok_or(RequestExecutionError::SyntaxError)?;
            let right_bound = parse_zlex_bound(right).ok_or(RequestExecutionError::SyntaxError)?;
            let (min_bound, max_bound) = if options.reverse {
                (right_bound, left_bound)
            } else {
                (left_bound, right_bound)
            };
            let mut entries = Vec::new();
            if options.reverse {
                for (member, score) in zset.iter().rev() {
                    if zlex_member_in_bounds(member.as_slice(), min_bound, max_bound) {
                        entries.push((member.clone(), *score));
                    }
                }
            } else {
                for (member, score) in zset {
                    if zlex_member_in_bounds(member.as_slice(), min_bound, max_bound) {
                        entries.push((member.clone(), *score));
                    }
                }
            }
            entries
        }
    };
    Ok(apply_optional_limit(selected, options.limit))
}

fn zset_map_from_entries(entries: Vec<(Vec<u8>, f64)>) -> BTreeMap<Vec<u8>, f64> {
    entries.into_iter().collect()
}

fn score_in_bound(score: f64, min: ZscoreBound, max: ZscoreBound) -> bool {
    let lower_ok = match min {
        ZscoreBound::NegInf => true,
        ZscoreBound::PosInf => false,
        ZscoreBound::Inclusive(value) => score >= value,
        ZscoreBound::Exclusive(value) => score > value,
    };
    if !lower_ok {
        return false;
    }
    match max {
        ZscoreBound::NegInf => false,
        ZscoreBound::PosInf => true,
        ZscoreBound::Inclusive(value) => score <= value,
        ZscoreBound::Exclusive(value) => score < value,
    }
}

fn compute_zinter_cardinality(
    processor: &RequestProcessor,
    keys: &[Vec<u8>],
) -> Result<usize, RequestExecutionError> {
    let Some((first_key, remaining_keys)) = keys.split_first() else {
        return Ok(0);
    };
    let mut intersection: BTreeSet<Vec<u8>> = match processor.load_zset_object(first_key)? {
        Some(zset) => zset.keys().cloned().collect(),
        None => return Ok(0),
    };

    for key in remaining_keys {
        let Some(other_zset) = processor.load_zset_object(key)? else {
            return Ok(0);
        };
        intersection.retain(|member| other_zset.contains_key(member));
        if intersection.is_empty() {
            break;
        }
    }
    Ok(intersection.len())
}

fn store_zset_result(
    processor: &RequestProcessor,
    destination: &[u8],
    result_zset: &BTreeMap<Vec<u8>, f64>,
) -> Result<(), RequestExecutionError> {
    processor.expire_key_if_needed(destination)?;
    let destination_had_string = processor.key_exists(destination)?;
    let string_deleted = if destination_had_string {
        delete_string_value_for_object_overwrite(processor, destination)?
    } else {
        false
    };

    if result_zset.is_empty() {
        let object_deleted = processor.object_delete(destination)?;
        if string_deleted && !object_deleted {
            processor.bump_watch_version(destination);
        }
        return Ok(());
    }

    processor.save_zset_object(destination, result_zset)
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

fn zset_entries_snapshot(zset: &BTreeMap<Vec<u8>, f64>) -> Vec<(Vec<u8>, f64)> {
    zset.iter()
        .map(|(member, score)| (member.clone(), *score))
        .collect()
}

fn select_random_zset_entries_distinct(
    processor: &RequestProcessor,
    zset: &BTreeMap<Vec<u8>, f64>,
    count: usize,
) -> Vec<(Vec<u8>, f64)> {
    let mut entries = zset_entries_snapshot(zset);
    if entries.len() <= 1 {
        entries.truncate(count.min(entries.len()));
        return entries;
    }
    for index in (1..entries.len()).rev() {
        let random_index = (processor.next_random_u64() as usize) % (index + 1);
        entries.swap(index, random_index);
    }
    entries.truncate(count.min(entries.len()));
    entries
}

fn select_random_zset_entries_with_replacement(
    processor: &RequestProcessor,
    zset: &BTreeMap<Vec<u8>, f64>,
    count: usize,
) -> Vec<(Vec<u8>, f64)> {
    let entries = zset_entries_snapshot(zset);
    if entries.is_empty() || count == 0 {
        return Vec::new();
    }
    let mut sampled = Vec::with_capacity(count);
    for _ in 0..count {
        let index = (processor.next_random_u64() as usize) % entries.len();
        sampled.push(entries[index].clone());
    }
    sampled
}
