use super::resp::append_bulk_double;
use super::*;

impl RequestProcessor {
    pub(super) fn handle_zadd(
        &self,
        _ctx: CommandContext,
        selected_db: DbName,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            4,
            "ZADD",
            "ZADD key [NX|XX] [GT|LT] [CH] [INCR] score member [score member ...]",
        )?;
        let options = parse_zadd_options(args)?;

        let key = RedisKey::from(args[1]);
        let db_key = DbKeyRef::new(selected_db, &key);
        let mut score_member_pairs =
            Vec::with_capacity((args.len() - options.score_start_index) / 2);
        let mut index = options.score_start_index;
        while index < args.len() {
            let score =
                parse_zset_score_value(args[index]).ok_or(RequestExecutionError::ValueNotFloat)?;
            score_member_pairs.push((score, args[index + 1].to_vec()));
            index += 2;
        }

        let mut zset = self.load_zset_object(db_key)?.unwrap_or_default();
        let mut added = 0i64;
        let mut updated = 0i64;
        let mut processed = 0i64;
        let mut increment_result = None;

        for (score_input, member) in score_member_pairs {
            let existing_score = zset.get(&member).copied();
            if options.nx && existing_score.is_some() {
                continue;
            }
            if options.xx && existing_score.is_none() {
                continue;
            }

            let current_score = existing_score.unwrap_or(0.0);
            let next_score = if options.incr {
                current_score + score_input
            } else {
                score_input
            };
            if next_score.is_nan() {
                return Err(RequestExecutionError::ResultingScoreNotANumber);
            }

            if let Some(current_score) = existing_score {
                if options.gt && next_score <= current_score {
                    continue;
                }
                if options.lt && next_score >= current_score {
                    continue;
                }
                processed += 1;
                increment_result = Some(next_score);
                if next_score != current_score {
                    zset.insert(member, next_score);
                    updated += 1;
                }
                continue;
            }

            processed += 1;
            increment_result = Some(next_score);
            zset.insert(member, next_score);
            added += 1;
        }

        let changed = added + updated;
        if changed > 0 {
            self.save_zset_object(db_key, &zset)?;
            self.notify_keyspace_event(
                selected_db,
                NOTIFY_ZSET,
                if options.incr { b"zincr" } else { b"zadd" },
                &key,
            );
        }

        if options.incr {
            if processed == 0 {
                if self.resp_protocol_version().is_resp3() {
                    append_null(response_out);
                } else {
                    append_null_bulk_string(response_out);
                }
                return Ok(());
            }
            let Some(score) = increment_result else {
                return Err(RequestExecutionError::StorageFailure);
            };
            if self.resp_protocol_version().is_resp3() {
                append_double(response_out, score);
            } else {
                append_bulk_double(response_out, score);
            }
            return Ok(());
        }

        append_integer(response_out, if options.ch { changed } else { added });
        Ok(())
    }

    pub(super) fn handle_zrem(
        &self,
        _ctx: CommandContext,
        selected_db: DbName,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 3, "ZREM", "ZREM key member [member ...]")?;

        let key = RedisKey::from(args[1]);
        let db_key = DbKeyRef::new(selected_db, &key);
        let mut zset = match self.load_zset_object(db_key)? {
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
            self.notify_keyspace_event(selected_db, NOTIFY_ZSET, b"zrem", &key);
        }
        if zset.is_empty() {
            let _ = self.object_delete(db_key)?;
            if removed > 0 {
                self.notify_keyspace_event(selected_db, NOTIFY_GENERIC, b"del", &key);
            }
        } else {
            self.save_zset_object(db_key, &zset)?;
        }
        append_integer(response_out, removed);
        Ok(())
    }

    pub(super) fn handle_zrange(
        &self,
        _ctx: CommandContext,
        selected_db: DbName,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            4,
            "ZRANGE",
            "ZRANGE key min max [BYSCORE|BYLEX] [REV] [WITHSCORES] [LIMIT offset count]",
        )?;
        let key = RedisKey::from(args[1]);
        let db_key = DbKeyRef::new(selected_db, &key);
        let left = args[2];
        let right = args[3];
        let options = parse_zrange_options(args, 4, true)?;
        let zset = match self.load_zset_object(db_key)? {
            Some(zset) => zset,
            None => {
                append_array_length(response_out, 0);
                return Ok(());
            }
        };

        let selected = collect_zrange_entries(&zset, left, right, options)?;
        append_owned_zrange_score_entries(
            response_out,
            &selected,
            options.with_scores,
            self.emit_resp3_zset_pairs(),
            self.resp_protocol_version().is_resp3(),
        );
        Ok(())
    }

    pub(super) fn handle_zrevrange(
        &self,
        _ctx: CommandContext,
        selected_db: DbName,
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
        let db_key = DbKeyRef::new(selected_db, &key);
        let start = parse_i64_ascii(args[2]).ok_or(RequestExecutionError::ValueNotInteger)?;
        let stop = parse_i64_ascii(args[3]).ok_or(RequestExecutionError::ValueNotInteger)?;
        let zset = match self.load_zset_object(db_key)? {
            Some(zset) => zset,
            None => {
                append_array_length(response_out, 0);
                return Ok(());
            }
        };

        let mut entries = sorted_zset_entries_by_score(&zset);
        entries.reverse();
        let Some(range) = normalize_zset_index_range(entries.len(), start, stop) else {
            append_array_length(response_out, 0);
            return Ok(());
        };

        let selected = &entries[range.start..=range.end_inclusive];
        append_zrange_score_entries(
            response_out,
            selected,
            with_scores,
            self.emit_resp3_zset_pairs(),
            self.resp_protocol_version().is_resp3(),
        );
        Ok(())
    }

    pub(super) fn handle_zrangebyscore(
        &self,
        ctx: CommandContext,
        selected_db: DbName,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_zrangebyscore_like(ctx, selected_db, args, response_out, false)
    }

    pub(super) fn handle_zrevrangebyscore(
        &self,
        ctx: CommandContext,
        selected_db: DbName,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_zrangebyscore_like(ctx, selected_db, args, response_out, true)
    }

    pub(super) fn handle_zscore(
        &self,
        _ctx: CommandContext,
        selected_db: DbName,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 3, "ZSCORE", "ZSCORE key member")?;

        let key = RedisKey::from(args[1]);
        let db_key = DbKeyRef::new(selected_db, &key);
        let member = args[2];
        let resp3 = self.resp_protocol_version().is_resp3();
        let zset = match self.load_zset_object(db_key)? {
            Some(zset) => zset,
            None => {
                if resp3 {
                    append_null(response_out);
                } else {
                    append_null_bulk_string(response_out);
                }
                return Ok(());
            }
        };

        match zset.get(member) {
            Some(score) => {
                if resp3 {
                    append_double(response_out, *score);
                } else {
                    append_bulk_double(response_out, *score);
                }
            }
            None => {
                if resp3 {
                    append_null(response_out);
                } else {
                    append_null_bulk_string(response_out);
                }
            }
        }
        Ok(())
    }

    pub(super) fn handle_zcard(
        &self,
        _ctx: CommandContext,
        selected_db: DbName,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 2, "ZCARD", "ZCARD key")?;

        let key = RedisKey::from(args[1]);
        let db_key = DbKeyRef::new(selected_db, &key);
        let count = self
            .load_zset_object(db_key)?
            .map_or(0usize, |zset| zset.len());
        append_integer(response_out, count as i64);
        Ok(())
    }

    pub(super) fn handle_zcount(
        &self,
        _ctx: CommandContext,
        selected_db: DbName,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 4, "ZCOUNT", "ZCOUNT key min max")?;

        let key = RedisKey::from(args[1]);
        let db_key = DbKeyRef::new(selected_db, &key);
        let min = parse_zscore_bound(args[2]).ok_or(RequestExecutionError::ValueNotFloat)?;
        let max = parse_zscore_bound(args[3]).ok_or(RequestExecutionError::ValueNotFloat)?;
        let zset = match self.load_zset_object(db_key)? {
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
        _ctx: CommandContext,
        selected_db: DbName,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 4, "ZLEXCOUNT", "ZLEXCOUNT key min max")?;
        let key = RedisKey::from(args[1]);
        let db_key = DbKeyRef::new(selected_db, &key);
        let min = parse_zlex_bound(args[2]).ok_or(RequestExecutionError::SyntaxError)?;
        let max = parse_zlex_bound(args[3]).ok_or(RequestExecutionError::SyntaxError)?;

        let count = match self.load_zset_object(db_key)? {
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
        ctx: CommandContext,
        selected_db: DbName,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_zrangebylex_like(ctx, selected_db, args, response_out, false)
    }

    pub(super) fn handle_zrevrangebylex(
        &self,
        ctx: CommandContext,
        selected_db: DbName,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_zrangebylex_like(ctx, selected_db, args, response_out, true)
    }

    pub(super) fn handle_zremrangebylex(
        &self,
        _ctx: CommandContext,
        selected_db: DbName,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 4, "ZREMRANGEBYLEX", "ZREMRANGEBYLEX key min max")?;
        let key = RedisKey::from(args[1]);
        let db_key = DbKeyRef::new(selected_db, &key);
        let min = parse_zlex_bound(args[2]).ok_or(RequestExecutionError::SyntaxError)?;
        let max = parse_zlex_bound(args[3]).ok_or(RequestExecutionError::SyntaxError)?;

        let mut zset = match self.load_zset_object(db_key)? {
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
            let _ = self.object_delete(db_key)?;
        } else {
            self.save_zset_object(db_key, &zset)?;
        }
        append_integer(response_out, removed);
        Ok(())
    }

    pub(super) fn handle_zintercard(
        &self,
        _ctx: CommandContext,
        selected_db: DbName,
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
            return Err(RequestExecutionError::AtLeastOneInputKey {
                command: "zintercard",
            });
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
        let mut cardinality = compute_zinter_cardinality(self, selected_db, &keys)?;
        if limit > 0 {
            cardinality = cardinality.min(limit);
        }
        append_integer(response_out, cardinality as i64);
        Ok(())
    }

    pub(super) fn handle_zdiff(
        &self,
        _ctx: CommandContext,
        selected_db: DbName,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 3, "ZDIFF", "ZDIFF numkeys key [key ...] [WITHSCORES]")?;
        let parsed_keys = parse_zset_numkeys_and_keys(args, 1, "zdiff")?;
        let with_scores = parse_zdiff_withscores(args, parsed_keys.option_start)?;
        let diff = compute_zdiff(self, selected_db, &parsed_keys.keys)?;
        append_zset_combine_response(
            response_out,
            &diff,
            with_scores,
            self.emit_resp3_zset_pairs(),
            self.resp_protocol_version().is_resp3(),
        );
        Ok(())
    }

    pub(super) fn handle_zdiffstore(
        &self,
        _ctx: CommandContext,
        selected_db: DbName,
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
        let parsed_keys = parse_zset_numkeys_and_keys(args, 2, "zdiffstore")?;
        if parsed_keys.option_start != args.len() {
            return Err(RequestExecutionError::SyntaxError);
        }
        let diff = compute_zdiff(self, selected_db, &parsed_keys.keys)?;
        store_zset_result(self, DbKeyRef::new(selected_db, &destination), &diff)?;
        append_integer(response_out, diff.len() as i64);
        Ok(())
    }

    pub(super) fn handle_zinter(
        &self,
        _ctx: CommandContext,
        selected_db: DbName,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            3,
            "ZINTER",
            "ZINTER numkeys key [key ...] [WEIGHTS w [w ...]] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]",
        )?;
        let parsed_keys = parse_zset_numkeys_and_keys(args, 1, "zinter")?;
        let combine_options = parse_zset_combine_options(
            args,
            parsed_keys.option_start,
            parsed_keys.keys.len(),
            /*allow_with_scores=*/ true,
        )?;
        let inter = compute_zinter(
            self,
            selected_db,
            &parsed_keys.keys,
            &combine_options.weights,
            combine_options.aggregate,
        )?;
        append_zset_combine_response(
            response_out,
            &inter,
            combine_options.with_scores,
            self.emit_resp3_zset_pairs(),
            self.resp_protocol_version().is_resp3(),
        );
        Ok(())
    }

    pub(super) fn handle_zinterstore(
        &self,
        _ctx: CommandContext,
        selected_db: DbName,
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
        let parsed_keys = parse_zset_numkeys_and_keys(args, 2, "zinterstore")?;
        let combine_options = parse_zset_combine_options(
            args,
            parsed_keys.option_start,
            parsed_keys.keys.len(),
            /*allow_with_scores=*/ false,
        )?;
        let inter = compute_zinter(
            self,
            selected_db,
            &parsed_keys.keys,
            &combine_options.weights,
            combine_options.aggregate,
        )?;
        store_zset_result(self, DbKeyRef::new(selected_db, &destination), &inter)?;
        append_integer(response_out, inter.len() as i64);
        Ok(())
    }

    pub(super) fn handle_zunion(
        &self,
        _ctx: CommandContext,
        selected_db: DbName,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            3,
            "ZUNION",
            "ZUNION numkeys key [key ...] [WEIGHTS w [w ...]] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]",
        )?;
        let parsed_keys = parse_zset_numkeys_and_keys(args, 1, "zunion")?;
        let combine_options = parse_zset_combine_options(
            args,
            parsed_keys.option_start,
            parsed_keys.keys.len(),
            /*allow_with_scores=*/ true,
        )?;
        let union = compute_zunion(
            self,
            selected_db,
            &parsed_keys.keys,
            &combine_options.weights,
            combine_options.aggregate,
        )?;
        append_zset_combine_response(
            response_out,
            &union,
            combine_options.with_scores,
            self.emit_resp3_zset_pairs(),
            self.resp_protocol_version().is_resp3(),
        );
        Ok(())
    }

    pub(super) fn handle_zunionstore(
        &self,
        _ctx: CommandContext,
        selected_db: DbName,
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
        let parsed_keys = parse_zset_numkeys_and_keys(args, 2, "zunionstore")?;
        let combine_options = parse_zset_combine_options(
            args,
            parsed_keys.option_start,
            parsed_keys.keys.len(),
            /*allow_with_scores=*/ false,
        )?;
        let union = compute_zunion(
            self,
            selected_db,
            &parsed_keys.keys,
            &combine_options.weights,
            combine_options.aggregate,
        )?;
        store_zset_result(self, DbKeyRef::new(selected_db, &destination), &union)?;
        append_integer(response_out, union.len() as i64);
        Ok(())
    }

    pub(super) fn handle_zmpop(
        &self,
        ctx: CommandContext,
        selected_db: DbName,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            4,
            "ZMPOP",
            "ZMPOP numkeys key [key ...] <MIN|MAX> [COUNT count]",
        )?;
        let parsed_keys = parse_zmpop_numkeys_and_keys(args, 1)?;
        let pop_options = parse_zmpop_direction_and_count(args, parsed_keys.option_start)?;
        self.handle_zmpop_like(
            ctx,
            selected_db,
            &parsed_keys.keys,
            pop_options.pop_max,
            pop_options.count,
            response_out,
        )
    }

    pub(super) fn handle_bzmpop(
        &self,
        ctx: CommandContext,
        selected_db: DbName,
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
        let parsed_keys = parse_zmpop_numkeys_and_keys(args, 2)?;
        let pop_options = parse_zmpop_direction_and_count(args, parsed_keys.option_start)?;
        self.handle_zmpop_like(
            ctx,
            selected_db,
            &parsed_keys.keys,
            pop_options.pop_max,
            pop_options.count,
            response_out,
        )
    }

    pub(super) fn handle_zrangestore(
        &self,
        _ctx: CommandContext,
        selected_db: DbName,
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
        let source_db_key = DbKeyRef::new(selected_db, &source);
        let destination_db_key = DbKeyRef::new(selected_db, &destination);
        let left = args[3];
        let right = args[4];
        let options = parse_zrange_options(args, 5, false)?;

        let selected = match self.load_zset_object(source_db_key)? {
            Some(zset) => collect_zrange_entries(&zset, left, right, options)?,
            None => Vec::new(),
        };
        let result = zset_map_from_entries(selected);
        store_zset_result(self, destination_db_key, &result)?;
        append_integer(response_out, result.len() as i64);
        Ok(())
    }

    pub(super) fn handle_zscan(
        &self,
        _ctx: CommandContext,
        selected_db: DbName,
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
        let db_key = DbKeyRef::new(selected_db, &key);
        let cursor = parse_u64_ascii(args[2]).ok_or(RequestExecutionError::ValueNotInteger)?;
        let scan_options = parse_scan_match_count_options(args, 3)?;

        let resp3 = self.resp_protocol_version().is_resp3();
        let Some(zset) = self.load_zset_object(db_key)? else {
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
        ctx: CommandContext,
        selected_db: DbName,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_zrank_like(ctx, selected_db, args, response_out, false)
    }

    pub(super) fn handle_zrevrank(
        &self,
        ctx: CommandContext,
        selected_db: DbName,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_zrank_like(ctx, selected_db, args, response_out, true)
    }

    pub(super) fn handle_zincrby(
        &self,
        _ctx: CommandContext,
        selected_db: DbName,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 4, "ZINCRBY", "ZINCRBY key increment member")?;

        let key = RedisKey::from(args[1]);
        let db_key = DbKeyRef::new(selected_db, &key);
        let increment =
            parse_zset_score_value(args[2]).ok_or(RequestExecutionError::ValueNotFloat)?;
        let member = args[3].to_vec();

        let mut zset = self.load_zset_object(db_key)?.unwrap_or_default();
        let current = zset.get(&member).copied().unwrap_or(0.0);
        let updated = current + increment;
        if updated.is_nan() {
            return Err(RequestExecutionError::ResultingScoreNotANumber);
        }
        zset.insert(member, updated);
        self.save_zset_object(db_key, &zset)?;
        self.notify_keyspace_event(selected_db, NOTIFY_ZSET, b"zincr", &key);
        if self.resp_protocol_version().is_resp3() {
            append_double(response_out, updated);
        } else {
            append_bulk_double(response_out, updated);
        }
        Ok(())
    }

    pub(super) fn handle_zremrangebyrank(
        &self,
        _ctx: CommandContext,
        selected_db: DbName,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 4, "ZREMRANGEBYRANK", "ZREMRANGEBYRANK key start stop")?;
        let key = RedisKey::from(args[1]);
        let db_key = DbKeyRef::new(selected_db, &key);
        let start = parse_i64_ascii(args[2]).ok_or(RequestExecutionError::ValueNotInteger)?;
        let stop = parse_i64_ascii(args[3]).ok_or(RequestExecutionError::ValueNotInteger)?;

        let mut zset = match self.load_zset_object(db_key)? {
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
            let _ = self.object_delete(db_key)?;
        } else {
            self.save_zset_object(db_key, &zset)?;
        }
        append_integer(response_out, removed);
        Ok(())
    }

    pub(super) fn handle_zremrangebyscore(
        &self,
        _ctx: CommandContext,
        selected_db: DbName,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 4, "ZREMRANGEBYSCORE", "ZREMRANGEBYSCORE key min max")?;
        let key = RedisKey::from(args[1]);
        let db_key = DbKeyRef::new(selected_db, &key);
        let min = parse_zscore_bound(args[2]).ok_or(RequestExecutionError::ValueNotFloat)?;
        let max = parse_zscore_bound(args[3]).ok_or(RequestExecutionError::ValueNotFloat)?;

        let mut zset = match self.load_zset_object(db_key)? {
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
            let _ = self.object_delete(db_key)?;
        } else {
            self.save_zset_object(db_key, &zset)?;
        }
        append_integer(response_out, removed);
        Ok(())
    }

    fn handle_zrangebyscore_like(
        &self,
        _ctx: CommandContext,
        selected_db: DbName,
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
        let db_key = DbKeyRef::new(selected_db, &key);
        let left_bound =
            parse_zscore_bound(args[2]).ok_or(RequestExecutionError::MinOrMaxNotFloat)?;
        let right_bound =
            parse_zscore_bound(args[3]).ok_or(RequestExecutionError::MinOrMaxNotFloat)?;
        let options = parse_zrange_by_score_options(args, 4)?;

        let zset = match self.load_zset_object(db_key)? {
            Some(zset) => zset,
            None => {
                append_array_length(response_out, 0);
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
            self.resp_protocol_version().is_resp3(),
        );
        Ok(())
    }

    fn handle_zrangebylex_like(
        &self,
        _ctx: CommandContext,
        selected_db: DbName,
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
        let db_key = DbKeyRef::new(selected_db, &key);
        let left_bound = parse_zlex_bound(args[2])
            .ok_or(RequestExecutionError::MinOrMaxNotValidStringRangeItem)?;
        let right_bound = parse_zlex_bound(args[3])
            .ok_or(RequestExecutionError::MinOrMaxNotValidStringRangeItem)?;
        let limit = parse_zrangebylex_limit(args, 4)?;
        let (min_bound, max_bound) = if reverse {
            (right_bound, left_bound)
        } else {
            (left_bound, right_bound)
        };

        let zset = match self.load_zset_object(db_key)? {
            Some(zset) => zset,
            None => {
                append_array_length(response_out, 0);
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
        append_array_length(response_out, selected.len());
        for member in selected {
            append_bulk_string(response_out, member);
        }
        Ok(())
    }

    pub(super) fn handle_zmscore(
        &self,
        _ctx: CommandContext,
        selected_db: DbName,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 3, "ZMSCORE", "ZMSCORE key member [member ...]")?;
        let key = RedisKey::from(args[1]);
        let db_key = DbKeyRef::new(selected_db, &key);
        let zset = self.load_zset_object(db_key)?;
        let members = &args[2..];

        let resp3 = self.resp_protocol_version().is_resp3();
        append_array_length(response_out, members.len());
        for member in members {
            let score = zset.as_ref().and_then(|zset| zset.get(*member));
            match score {
                Some(score) => {
                    if resp3 {
                        append_double(response_out, *score);
                    } else {
                        append_bulk_double(response_out, *score);
                    }
                }
                None => {
                    if resp3 {
                        append_null(response_out);
                    } else {
                        append_null_bulk_string(response_out);
                    }
                }
            }
        }
        Ok(())
    }

    pub(super) fn handle_zrandmember(
        &self,
        _ctx: CommandContext,
        selected_db: DbName,
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
        let db_key = DbKeyRef::new(selected_db, &key);
        let resp3 = self.resp_protocol_version().is_resp3();
        let zset = self.load_zset_object(db_key)?;
        if args.len() == 2 {
            let Some(zset) = zset else {
                if resp3 {
                    append_null(response_out);
                } else {
                    append_null_bulk_string(response_out);
                }
                return Ok(());
            };
            if zset.is_empty() {
                if resp3 {
                    append_null(response_out);
                } else {
                    append_null_bulk_string(response_out);
                }
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

        if count == i64::MIN {
            return Err(RequestExecutionError::ValueOutOfRange);
        }
        if with_scores && count.unsigned_abs() > (i64::MAX as u64 / 2) {
            return Err(RequestExecutionError::ValueOutOfRange);
        }

        let Some(zset) = zset else {
            append_array_length(response_out, 0);
            return Ok(());
        };
        if zset.is_empty() || count == 0 {
            append_array_length(response_out, 0);
            return Ok(());
        }

        let sampled = if count > 0 {
            let requested = usize::try_from(count).unwrap_or(usize::MAX);
            select_random_zset_entries_distinct(self, &zset, requested)
        } else {
            let requested = usize::try_from(count.unsigned_abs())
                .map_err(|_| RequestExecutionError::ValueOutOfRange)?;
            select_random_zset_entries_with_replacement(self, &zset, requested)?
        };

        let resp3 = self.resp_protocol_version().is_resp3();
        let emit_pair_array = with_scores && self.emit_resp3_zset_pairs();
        let response_items = if emit_pair_array {
            sampled.len()
        } else if with_scores {
            sampled.len() * 2
        } else {
            sampled.len()
        };
        append_array_length(response_out, response_items);
        for (member, score) in sampled {
            if emit_pair_array {
                append_array_length(response_out, 2);
                append_bulk_string(response_out, member.as_slice());
                if resp3 {
                    append_double(response_out, score);
                } else {
                    append_bulk_double(response_out, score);
                }
                continue;
            }

            append_bulk_string(response_out, member.as_slice());
            if with_scores {
                if resp3 {
                    append_double(response_out, score);
                } else {
                    append_bulk_double(response_out, score);
                }
            }
        }
        Ok(())
    }

    pub(super) fn handle_zpopmin(
        &self,
        ctx: CommandContext,
        selected_db: DbName,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_zpop_like(ctx, selected_db, args, response_out, false)
    }

    pub(super) fn handle_zpopmax(
        &self,
        ctx: CommandContext,
        selected_db: DbName,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_zpop_like(ctx, selected_db, args, response_out, true)
    }

    pub(super) fn handle_bzpopmin(
        &self,
        ctx: CommandContext,
        selected_db: DbName,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_bzpop_like(ctx, selected_db, args, response_out, false)
    }

    pub(super) fn handle_bzpopmax(
        &self,
        ctx: CommandContext,
        selected_db: DbName,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_bzpop_like(ctx, selected_db, args, response_out, true)
    }

    fn handle_zrank_like(
        &self,
        _ctx: CommandContext,
        selected_db: DbName,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
        reverse: bool,
    ) -> Result<(), RequestExecutionError> {
        ensure_ranged_arity(
            args,
            3,
            4,
            if reverse { "ZREVRANK" } else { "ZRANK" },
            if reverse {
                "ZREVRANK key member [WITHSCORE]"
            } else {
                "ZRANK key member [WITHSCORE]"
            },
        )?;
        let with_score = if args.len() == 4 {
            if !ascii_eq_ignore_case(args[3], b"WITHSCORE") {
                return Err(RequestExecutionError::SyntaxError);
            }
            true
        } else {
            false
        };

        let key = RedisKey::from(args[1]);
        let db_key = DbKeyRef::new(selected_db, &key);
        let member = args[2];
        let resp3 = self.resp_protocol_version().is_resp3();
        let zset = match self.load_zset_object(db_key)? {
            Some(zset) => zset,
            None => {
                if resp3 {
                    append_null(response_out);
                } else if with_score {
                    append_null_array(response_out);
                } else {
                    append_null_bulk_string(response_out);
                }
                return Ok(());
            }
        };
        let entries = sorted_zset_entries_by_score(&zset);
        let Some(index) = entries
            .iter()
            .position(|(candidate_member, _score)| candidate_member.as_slice() == member)
        else {
            if resp3 {
                append_null(response_out);
            } else if with_score {
                append_null_array(response_out);
            } else {
                append_null_bulk_string(response_out);
            }
            return Ok(());
        };
        let rank = if reverse {
            entries.len() - 1 - index
        } else {
            index
        };
        if with_score {
            append_array_length(response_out, 2);
            append_integer(response_out, rank as i64);
            let score = entries[index].1;
            if resp3 {
                append_double(response_out, score);
            } else {
                append_bulk_double(response_out, score);
            }
            return Ok(());
        }
        append_integer(response_out, rank as i64);
        Ok(())
    }

    fn handle_zpop_like(
        &self,
        _ctx: CommandContext,
        selected_db: DbName,
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
        let count: i64 = if args.len() == 3 {
            parse_i64_ascii(args[2]).ok_or(RequestExecutionError::ValueNotInteger)?
        } else {
            1
        };
        if count < 0 {
            return Err(RequestExecutionError::ValueOutOfRangePositive);
        }
        // Type-check the key even when count is 0 (Redis returns WRONGTYPE
        // for wrong-typed keys regardless of count).
        if count == 0 {
            let _ = self.load_zset_object(DbKeyRef::new(selected_db, &key))?;
            append_array_length(response_out, 0);
            return Ok(());
        }
        let requested = usize::try_from(count).unwrap_or(usize::MAX);
        let Some(selected) =
            self.pop_zset_entries_from_key(selected_db, &key, pop_max, requested)?
        else {
            append_array_length(response_out, 0);
            return Ok(());
        };

        let resp3 = self.resp_protocol_version().is_resp3();
        let emit_pair_array = args.len() == 3 && self.emit_resp3_zset_pairs();
        let array_len = if emit_pair_array {
            selected.len()
        } else {
            selected.len() * 2
        };
        append_array_length(response_out, array_len);
        for (member, score) in selected {
            if emit_pair_array {
                append_array_length(response_out, 2);
                append_bulk_string(response_out, member.as_slice());
                if resp3 {
                    append_double(response_out, score);
                } else {
                    append_bulk_double(response_out, score);
                }
                continue;
            }
            append_bulk_string(response_out, member.as_slice());
            if resp3 {
                append_double(response_out, score);
            } else {
                append_bulk_double(response_out, score);
            }
        }
        Ok(())
    }

    fn handle_bzpop_like(
        &self,
        _ctx: CommandContext,
        selected_db: DbName,
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
            let Some(mut selected) =
                self.pop_zset_entries_from_key(selected_db, &key, pop_max, 1)?
            else {
                continue;
            };
            let entry = selected
                .pop()
                .expect("pop_zset_entries_from_key returns one element when requested=1");
            let resp3 = self.resp_protocol_version().is_resp3();
            append_bzpop_response(response_out, &key, &entry, resp3);
            return Ok(());
        }

        if self.resp_protocol_version().is_resp3() {
            append_null(response_out);
        } else {
            append_null_array(response_out);
        }
        Ok(())
    }

    fn handle_zmpop_like(
        &self,
        _ctx: CommandContext,
        selected_db: DbName,
        keys: &[Vec<u8>],
        pop_max: bool,
        count: usize,
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        for key in keys {
            let Some(selected) =
                self.pop_zset_entries_from_key(selected_db, key, pop_max, count)?
            else {
                continue;
            };
            let resp3 = self.resp_protocol_version().is_resp3();
            append_zmpop_response(response_out, key, &selected, resp3);
            return Ok(());
        }
        if self.resp_protocol_version().is_resp3() {
            append_null(response_out);
        } else {
            append_null_array(response_out);
        }
        Ok(())
    }

    #[allow(clippy::type_complexity)]
    fn pop_zset_entries_from_key(
        &self,
        selected_db: DbName,
        key: &[u8],
        pop_max: bool,
        count: usize,
    ) -> Result<Option<Vec<(Vec<u8>, f64)>>, RequestExecutionError> {
        let db_key = DbKeyRef::new(selected_db, key);
        let mut zset = match self.load_zset_object(db_key)? {
            Some(zset) => zset,
            None => return Ok(None),
        };
        if zset.is_empty() {
            let _ = self.object_delete(db_key)?;
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
            let _ = self.object_delete(db_key)?;
        } else {
            self.save_zset_object(db_key, &zset)?;
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
enum ZrangeMode {
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
struct ZrangeOptions {
    mode: ZrangeMode,
    reverse: bool,
    limit: Option<ZsetLimit>,
    with_scores: bool,
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

    append_array_length(response_out, 2);
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
        if resp3 {
            append_double(response_out, *score);
        } else {
            append_bulk_double(response_out, *score);
        }
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
            let raw_offset =
                parse_i64_ascii(args[index + 1]).ok_or(RequestExecutionError::ValueNotInteger)?;
            let count =
                parse_u64_ascii(args[index + 2]).ok_or(RequestExecutionError::ValueNotInteger)?;
            options.limit = Some(ZsetLimit {
                offset: if raw_offset < 0 {
                    usize::MAX
                } else {
                    usize::try_from(raw_offset).unwrap_or(usize::MAX)
                },
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
    emit_pair_array: bool,
    resp3_double: bool,
) {
    if with_scores && emit_pair_array {
        append_array_length(response_out, entries.len());
        for (member, score) in entries {
            append_array_length(response_out, 2);
            append_bulk_string(response_out, member);
            if resp3_double {
                append_double(response_out, *score);
            } else {
                append_bulk_double(response_out, *score);
            }
        }
        return;
    }

    let item_count = if with_scores {
        entries.len() * 2
    } else {
        entries.len()
    };
    append_array_length(response_out, item_count);
    for (member, score) in entries {
        append_bulk_string(response_out, member);
        if with_scores {
            if resp3_double {
                append_double(response_out, *score);
            } else {
                append_bulk_double(response_out, *score);
            }
        }
    }
}

fn append_owned_zrange_score_entries(
    response_out: &mut Vec<u8>,
    entries: &[(Vec<u8>, f64)],
    with_scores: bool,
    emit_pair_array: bool,
    resp3_double: bool,
) {
    if with_scores && emit_pair_array {
        append_array_length(response_out, entries.len());
        for (member, score) in entries {
            append_array_length(response_out, 2);
            append_bulk_string(response_out, member);
            if resp3_double {
                append_double(response_out, *score);
            } else {
                append_bulk_double(response_out, *score);
            }
        }
        return;
    }

    let item_count = if with_scores {
        entries.len() * 2
    } else {
        entries.len()
    };
    append_array_length(response_out, item_count);
    for (member, score) in entries {
        append_bulk_string(response_out, member);
        if with_scores {
            if resp3_double {
                append_double(response_out, *score);
            } else {
                append_bulk_double(response_out, *score);
            }
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
        if ascii_eq_ignore_case(exclusive, b"-INF") {
            return Some(ZscoreBound::Exclusive(f64::NEG_INFINITY));
        }
        if ascii_eq_ignore_case(exclusive, b"+INF") || ascii_eq_ignore_case(exclusive, b"INF") {
            return Some(ZscoreBound::Exclusive(f64::INFINITY));
        }
        return parse_f64_ascii(exclusive).map(ZscoreBound::Exclusive);
    }
    parse_f64_ascii(input).map(ZscoreBound::Inclusive)
}

#[derive(Debug, Clone, Copy, Default)]
struct ZaddOptions {
    nx: bool,
    xx: bool,
    gt: bool,
    lt: bool,
    ch: bool,
    incr: bool,
    score_start_index: usize,
}

fn parse_zadd_options(args: &[&[u8]]) -> Result<ZaddOptions, RequestExecutionError> {
    let mut options = ZaddOptions::default();
    let mut index = 2usize;
    while index < args.len() {
        let token = args[index];
        if ascii_eq_ignore_case(token, b"NX") {
            options.nx = true;
        } else if ascii_eq_ignore_case(token, b"XX") {
            options.xx = true;
        } else if ascii_eq_ignore_case(token, b"GT") {
            options.gt = true;
        } else if ascii_eq_ignore_case(token, b"LT") {
            options.lt = true;
        } else if ascii_eq_ignore_case(token, b"CH") {
            options.ch = true;
        } else if ascii_eq_ignore_case(token, b"INCR") {
            options.incr = true;
        } else {
            break;
        }
        index += 1;
    }

    let score_arguments = args.len() - index;
    if score_arguments == 0 || score_arguments % 2 != 0 {
        return Err(RequestExecutionError::SyntaxError);
    }
    if options.nx && options.xx {
        return Err(RequestExecutionError::SyntaxError);
    }
    if (options.gt && options.nx) || (options.lt && options.nx) || (options.gt && options.lt) {
        return Err(RequestExecutionError::SyntaxError);
    }
    if options.incr && score_arguments != 2 {
        return Err(RequestExecutionError::SyntaxError);
    }

    options.score_start_index = index;
    Ok(options)
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
    let raw_offset =
        parse_i64_ascii(args[start_index + 1]).ok_or(RequestExecutionError::ValueNotInteger)?;
    let count =
        parse_u64_ascii(args[start_index + 2]).ok_or(RequestExecutionError::ValueNotInteger)?;
    Ok(Some(ZsetLimit {
        offset: if raw_offset < 0 {
            usize::MAX
        } else {
            usize::try_from(raw_offset).unwrap_or(usize::MAX)
        },
        count: usize::try_from(count).unwrap_or(usize::MAX),
    }))
}

fn parse_zset_numkeys_and_keys(
    args: &[&[u8]],
    numkeys_index: usize,
    command: &'static str,
) -> Result<ParsedZsetKeys, RequestExecutionError> {
    if numkeys_index >= args.len() {
        return Err(RequestExecutionError::SyntaxError);
    }
    let numkeys =
        parse_u64_ascii(args[numkeys_index]).ok_or(RequestExecutionError::ValueNotInteger)?;
    if numkeys == 0 {
        return Err(RequestExecutionError::AtLeastOneInputKey { command });
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

fn parse_zmpop_numkeys_and_keys(
    args: &[&[u8]],
    numkeys_index: usize,
) -> Result<ParsedZsetKeys, RequestExecutionError> {
    if numkeys_index >= args.len() {
        return Err(RequestExecutionError::SyntaxError);
    }
    let parsed = parse_i64_ascii(args[numkeys_index])
        .ok_or(RequestExecutionError::NumkeysMustBeGreaterThanZero)?;
    if parsed <= 0 {
        return Err(RequestExecutionError::NumkeysMustBeGreaterThanZero);
    }
    let numkeys = usize::try_from(parsed).map_err(|_| RequestExecutionError::ValueOutOfRange)?;
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
                let parsed = parse_zset_score_value(weight_arg)
                    .ok_or(RequestExecutionError::WeightValueNotFloat)?;
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

/// Aggregate two zset scores.  Redis treats NaN results (e.g. inf + -inf)
/// as 0.0, so we sanitize NaN to 0.0 here to match that behavior.
fn aggregate_zset_scores(current: f64, incoming: f64, mode: ZsetAggregateMode) -> f64 {
    let result = match mode {
        ZsetAggregateMode::Sum => current + incoming,
        ZsetAggregateMode::Min => current.min(incoming),
        ZsetAggregateMode::Max => current.max(incoming),
    };
    if result.is_nan() { 0.0 } else { result }
}

/// Apply a weight to a score, sanitizing NaN (e.g. 0.0 * inf) to 0.0 to
/// match Redis behavior.
fn apply_weight(score: f64, weight: f64) -> f64 {
    let result = score * weight;
    if result.is_nan() { 0.0 } else { result }
}

fn compute_zdiff(
    processor: &RequestProcessor,
    selected_db: DbName,
    keys: &[Vec<u8>],
) -> Result<BTreeMap<Vec<u8>, f64>, RequestExecutionError> {
    let Some((first_key, remaining_keys)) = keys.split_first() else {
        return Ok(BTreeMap::new());
    };
    let mut diff = match load_zset_like_object(processor, selected_db, first_key)? {
        Some(zset) => zset,
        None => return Ok(BTreeMap::new()),
    };
    for key in remaining_keys {
        let Some(other) = load_zset_like_object(processor, selected_db, key)? else {
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
    selected_db: DbName,
    keys: &[Vec<u8>],
    weights: &[f64],
    aggregate_mode: ZsetAggregateMode,
) -> Result<BTreeMap<Vec<u8>, f64>, RequestExecutionError> {
    let Some((first_key, remaining_keys)) = keys.split_first() else {
        return Ok(BTreeMap::new());
    };
    let Some(first_set) = load_zset_like_object(processor, selected_db, first_key)? else {
        return Ok(BTreeMap::new());
    };
    let mut inter: BTreeMap<Vec<u8>, f64> = first_set
        .into_iter()
        .map(|(member, score)| (member, apply_weight(score, weights[0])))
        .collect();

    for (index, key) in remaining_keys.iter().enumerate() {
        let Some(other) = load_zset_like_object(processor, selected_db, key)? else {
            return Ok(BTreeMap::new());
        };
        let weight = weights[index + 1];
        inter.retain(|member, score| {
            let Some(other_score) = other.get(member) else {
                return false;
            };
            let weighted_other = apply_weight(*other_score, weight);
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
    selected_db: DbName,
    keys: &[Vec<u8>],
    weights: &[f64],
    aggregate_mode: ZsetAggregateMode,
) -> Result<BTreeMap<Vec<u8>, f64>, RequestExecutionError> {
    let mut union = BTreeMap::new();
    for (index, key) in keys.iter().enumerate() {
        let Some(zset) = load_zset_like_object(processor, selected_db, key)? else {
            continue;
        };
        let weight = weights[index];
        for (member, score) in zset {
            let weighted = apply_weight(score, weight);
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
    emit_pair_array: bool,
    resp3_double: bool,
) {
    let entries = sorted_zset_entries_by_score(zset);
    append_zrange_score_entries(
        response_out,
        &entries,
        with_scores,
        emit_pair_array,
        resp3_double,
    );
}

fn append_zmpop_response(
    response_out: &mut Vec<u8>,
    key: &[u8],
    entries: &[(Vec<u8>, f64)],
    resp3: bool,
) {
    append_array_length(response_out, 2);
    append_bulk_string(response_out, key);
    append_array_length(response_out, entries.len());
    for (member, score) in entries {
        append_array_length(response_out, 2);
        append_bulk_string(response_out, member);
        if resp3 {
            append_double(response_out, *score);
        } else {
            append_bulk_double(response_out, *score);
        }
    }
}

fn append_bzpop_response(
    response_out: &mut Vec<u8>,
    key: &[u8],
    entry: &(Vec<u8>, f64),
    resp3: bool,
) {
    append_array_length(response_out, 3);
    append_bulk_string(response_out, key);
    append_bulk_string(response_out, entry.0.as_slice());
    if resp3 {
        append_double(response_out, entry.1);
    } else {
        append_bulk_double(response_out, entry.1);
    }
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
        let parsed = parse_i64_ascii(args[option_start + 2])
            .ok_or(RequestExecutionError::CountMustBeGreaterThanZero)?;
        if parsed <= 0 {
            return Err(RequestExecutionError::CountMustBeGreaterThanZero);
        }
        usize::try_from(parsed).map_err(|_| RequestExecutionError::ValueOutOfRange)?
    };
    Ok(ParsedZmpopOptions { pop_max, count })
}

struct ParsedZmpopOptions {
    pop_max: bool,
    count: usize,
}

fn parse_zrange_options(
    args: &[&[u8]],
    start_index: usize,
    allow_with_scores: bool,
) -> Result<ZrangeOptions, RequestExecutionError> {
    let mut mode = ZrangeMode::Rank;
    let mut reverse = false;
    let mut limit = None;
    let mut with_scores = false;
    let mut index = start_index;
    while index < args.len() {
        let token = args[index];
        if ascii_eq_ignore_case(token, b"BYSCORE") {
            if mode != ZrangeMode::Rank {
                return Err(RequestExecutionError::SyntaxError);
            }
            mode = ZrangeMode::Score;
            index += 1;
            continue;
        }
        if ascii_eq_ignore_case(token, b"BYLEX") {
            if mode != ZrangeMode::Rank {
                return Err(RequestExecutionError::SyntaxError);
            }
            mode = ZrangeMode::Lex;
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
            let raw_offset =
                parse_i64_ascii(args[index + 1]).ok_or(RequestExecutionError::ValueNotInteger)?;
            let count =
                parse_u64_ascii(args[index + 2]).ok_or(RequestExecutionError::ValueNotInteger)?;
            limit = Some(ZsetLimit {
                offset: if raw_offset < 0 {
                    usize::MAX
                } else {
                    usize::try_from(raw_offset).unwrap_or(usize::MAX)
                },
                count: usize::try_from(count).unwrap_or(usize::MAX),
            });
            index += 3;
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
    if limit.is_some() && mode == ZrangeMode::Rank {
        return Err(RequestExecutionError::SyntaxError);
    }
    if with_scores && mode == ZrangeMode::Lex {
        return Err(RequestExecutionError::SyntaxError);
    }
    Ok(ZrangeOptions {
        mode,
        reverse,
        limit,
        with_scores,
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

fn collect_zrange_entries(
    zset: &BTreeMap<Vec<u8>, f64>,
    left: &[u8],
    right: &[u8],
    options: ZrangeOptions,
) -> Result<Vec<(Vec<u8>, f64)>, RequestExecutionError> {
    let selected = match options.mode {
        ZrangeMode::Rank => {
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
        ZrangeMode::Score => {
            let left_bound =
                parse_zscore_bound(left).ok_or(RequestExecutionError::MinOrMaxNotFloat)?;
            let right_bound =
                parse_zscore_bound(right).ok_or(RequestExecutionError::MinOrMaxNotFloat)?;
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
        ZrangeMode::Lex => {
            let left_bound = parse_zlex_bound(left)
                .ok_or(RequestExecutionError::MinOrMaxNotValidStringRangeItem)?;
            let right_bound = parse_zlex_bound(right)
                .ok_or(RequestExecutionError::MinOrMaxNotValidStringRangeItem)?;
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
    selected_db: DbName,
    keys: &[Vec<u8>],
) -> Result<usize, RequestExecutionError> {
    let Some((first_key, remaining_keys)) = keys.split_first() else {
        return Ok(0);
    };
    let mut intersection: BTreeSet<Vec<u8>> =
        match load_zset_like_object(processor, selected_db, first_key)? {
            Some(zset) => zset.keys().cloned().collect(),
            None => return Ok(0),
        };

    for key in remaining_keys {
        let Some(other_zset) = load_zset_like_object(processor, selected_db, key)? else {
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
    destination: DbKeyRef<'_>,
    result_zset: &BTreeMap<Vec<u8>, f64>,
) -> Result<(), RequestExecutionError> {
    processor.expire_key_if_needed(destination)?;
    let (destination_had_string, destination_object_type) =
        processor.key_type_snapshot_for_setkey_overwrite(destination)?;
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

    processor.save_zset_object(destination, result_zset)?;
    processor.notify_setkey_overwrite_events(
        destination.db(),
        destination.key(),
        destination_had_string,
        destination_object_type,
        Some(ObjectTypeTag::Zset),
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
) -> Result<Vec<(Vec<u8>, f64)>, RequestExecutionError> {
    let entries = zset_entries_snapshot(zset);
    if entries.is_empty() || count == 0 {
        return Ok(Vec::new());
    }
    let mut sampled = Vec::new();
    sampled
        .try_reserve(count)
        .map_err(|_| RequestExecutionError::ValueOutOfRange)?;
    for _ in 0..count {
        let index = (processor.next_random_u64() as usize) % entries.len();
        sampled.push(entries[index].clone());
    }
    Ok(sampled)
}

fn load_zset_like_object(
    processor: &RequestProcessor,
    selected_db: DbName,
    key: &[u8],
) -> Result<Option<BTreeMap<Vec<u8>, f64>>, RequestExecutionError> {
    match processor.load_zset_object(DbKeyRef::new(selected_db, key)) {
        Ok(zset) => Ok(zset),
        Err(RequestExecutionError::WrongType) => {
            let Some(set) = processor.load_set_object(DbKeyRef::new(selected_db, key))? else {
                return Ok(None);
            };
            let zset = set.into_iter().map(|member| (member, 1.0)).collect();
            Ok(Some(zset))
        }
        Err(error) => Err(error),
    }
}
