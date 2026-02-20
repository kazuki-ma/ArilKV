use super::*;

impl RequestProcessor {
    pub(super) fn handle_get(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        crate::debug_sync_point!("request_processor.handle_get.enter");
        if args.len() != 2 {
            return Err(RequestExecutionError::WrongArity {
                command: "GET",
                expected: "GET key",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        let shard_index = self.string_store_shard_index_for_key(&key);
        self.expire_key_if_needed_in_shard(&key, shard_index)?;
        crate::debug_sync_point!("request_processor.handle_get.before_store_lock");

        let mut store = self.lock_string_store_for_shard(shard_index);
        crate::debug_sync_point!("request_processor.handle_get.after_store_lock");
        let mut session = store.session(&self.functions);
        let mut output = Vec::new();
        let status = session
            .read(&key, &Vec::new(), &mut output, &ReadInfo::default())
            .map_err(map_read_error)?;

        match status {
            ReadOperationStatus::FoundInMemory | ReadOperationStatus::FoundOnDisk => {
                append_bulk_string(response_out, &output);
                Ok(())
            }
            ReadOperationStatus::NotFound => {
                append_null_bulk_string(response_out);
                Ok(())
            }
            ReadOperationStatus::RetryLater => Err(RequestExecutionError::StorageBusy),
        }
    }

    pub(super) fn handle_set(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        crate::debug_sync_point!("request_processor.handle_set.enter");
        if args.len() < 3 {
            return Err(RequestExecutionError::WrongArity {
                command: "SET",
                expected: "SET key value",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        // SAFETY: caller guarantees argument backing memory validity.
        let value = unsafe { args[2].as_slice() }.to_vec();
        let shard_index = self.string_store_shard_index_for_key(&key);
        let options = parse_set_options(args)?;
        self.expire_key_if_needed_in_shard(&key, shard_index)?;
        crate::debug_sync_point!("request_processor.handle_set.before_store_lock");

        if options.only_if_absent || options.only_if_present {
            let exists = self.key_exists(&key)?;
            if options.only_if_absent && exists {
                append_null_bulk_string(response_out);
                return Ok(());
            }
            if options.only_if_present && !exists {
                append_null_bulk_string(response_out);
                return Ok(());
            }
        }

        let mut store = self.lock_string_store_for_shard(shard_index);
        crate::debug_sync_point!("request_processor.handle_set.after_store_lock");
        let mut session = store.session(&self.functions);
        let mut output = Vec::new();
        let mut info = UpsertInfo::default();
        let stored_value = encode_stored_value(
            &value,
            options.expiration.map(|expiration| expiration.unix_millis),
        );
        if options.expiration.is_some() {
            info.user_data |= UPSERT_USER_DATA_HAS_EXPIRATION;
        }
        session
            .upsert(&key, &stored_value, &mut output, &mut info)
            .map_err(map_upsert_error)?;
        drop(session);
        drop(store);
        crate::debug_sync_point!("request_processor.handle_set.before_metadata_locks");

        self.set_string_expiration_deadline_in_shard(
            &key,
            shard_index,
            options.expiration.map(|e| e.deadline),
        );
        self.track_string_key_in_shard(&key, shard_index);
        self.bump_watch_version(&key);

        append_simple_string(response_out, b"OK");
        Ok(())
    }

    pub(super) fn handle_del(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() < 2 {
            return Err(RequestExecutionError::WrongArity {
                command: "DEL",
                expected: "DEL key [key ...]",
            });
        }

        let keys: Vec<Vec<u8>> = args[1..]
            .iter()
            .map(|arg| {
                // SAFETY: caller guarantees argument backing memory validity.
                unsafe { arg.as_slice() }.to_vec()
            })
            .collect();

        for key in &keys {
            self.expire_key_if_needed(key)?;
        }

        let mut deleted = 0i64;
        for key in keys {
            let mut store = self.lock_string_store_for_key(&key);
            let mut session = store.session(&self.functions);
            let mut info = DeleteInfo::default();
            let status = session.delete(&key, &mut info).map_err(map_delete_error)?;
            match status {
                DeleteOperationStatus::TombstonedInPlace
                | DeleteOperationStatus::AppendedTombstone => {
                    deleted += 1;
                    self.remove_string_key_metadata(&key);
                    self.bump_watch_version(&key);
                }
                DeleteOperationStatus::NotFound => {}
                DeleteOperationStatus::RetryLater => {
                    return Err(RequestExecutionError::StorageBusy);
                }
            }
        }

        append_integer(response_out, deleted);
        Ok(())
    }

    pub(super) fn handle_incr_decr(
        &self,
        args: &[ArgSlice],
        delta: i64,
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 2 {
            return Err(RequestExecutionError::WrongArity {
                command: if delta > 0 { "INCR" } else { "DECR" },
                expected: if delta > 0 { "INCR key" } else { "DECR key" },
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        self.expire_key_if_needed(&key)?;
        let input = delta.to_string().into_bytes();
        let mut output = Vec::new();
        let mut info = RmwInfo::default();
        let mut store = self.lock_string_store_for_key(&key);
        let mut session = store.session(&self.functions);
        let status = session.rmw(&key, &input, &mut output, &mut info);

        match status {
            Ok(RmwOperationStatus::InPlaceUpdated)
            | Ok(RmwOperationStatus::CopiedToTail)
            | Ok(RmwOperationStatus::Inserted) => {
                let parsed =
                    parse_i64_ascii(&output).ok_or(RequestExecutionError::ValueNotInteger)?;
                self.track_string_key(&key);
                self.bump_watch_version(&key);
                append_integer(response_out, parsed);
                Ok(())
            }
            Ok(RmwOperationStatus::RetryLater) => Err(RequestExecutionError::StorageBusy),
            Ok(RmwOperationStatus::NotFound) => {
                let mut upsert_info = UpsertInfo::default();
                let mut upsert_output = Vec::new();
                let stored_value = encode_stored_value(&input, None);
                session
                    .upsert(&key, &stored_value, &mut upsert_output, &mut upsert_info)
                    .map_err(map_upsert_error)?;
                self.track_string_key(&key);
                self.bump_watch_version(&key);
                append_integer(response_out, delta);
                Ok(())
            }
            Err(RmwOperationError::OperationCancelled) => {
                Err(RequestExecutionError::ValueNotInteger)
            }
            Err(error) => Err(map_rmw_error(error)),
        }
    }

    pub(super) fn handle_expire(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_expire_like(args, response_out, false)
    }

    pub(super) fn handle_pexpire(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_expire_like(args, response_out, true)
    }

    fn handle_expire_like(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
        milliseconds: bool,
    ) -> Result<(), RequestExecutionError> {
        let (command, expected) = if milliseconds {
            ("PEXPIRE", "PEXPIRE key milliseconds")
        } else {
            ("EXPIRE", "EXPIRE key seconds")
        };
        if args.len() != 3 {
            return Err(RequestExecutionError::WrongArity { command, expected });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let amount = parse_i64_ascii(unsafe { args[2].as_slice() })
            .ok_or(RequestExecutionError::ValueNotInteger)?;
        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();

        self.expire_key_if_needed(&key)?;
        if !self.key_exists(&key)? {
            append_integer(response_out, 0);
            return Ok(());
        }

        if amount <= 0 {
            let mut store = self.lock_string_store_for_key(&key);
            let mut session = store.session(&self.functions);
            let mut info = DeleteInfo::default();
            let status = session.delete(&key, &mut info).map_err(map_delete_error)?;
            match status {
                DeleteOperationStatus::TombstonedInPlace
                | DeleteOperationStatus::AppendedTombstone => {
                    self.remove_string_key_metadata(&key);
                    self.bump_watch_version(&key);
                    append_integer(response_out, 1);
                    Ok(())
                }
                DeleteOperationStatus::NotFound => {
                    self.remove_string_key_metadata(&key);
                    append_integer(response_out, 0);
                    Ok(())
                }
                DeleteOperationStatus::RetryLater => Err(RequestExecutionError::StorageBusy),
            }?;
            return Ok(());
        }

        let duration = if milliseconds {
            Duration::from_millis(amount as u64)
        } else {
            Duration::from_secs(amount as u64)
        };
        let expiration = expiration_metadata_from_duration(duration)
            .ok_or(RequestExecutionError::ValueNotInteger)?;
        self.set_string_expiration_deadline(&key, Some(expiration.deadline));
        if !self.rewrite_existing_value_expiration(&key, Some(expiration.unix_millis))? {
            self.set_string_expiration_deadline(&key, None);
            append_integer(response_out, 0);
            return Ok(());
        }
        self.bump_watch_version(&key);
        append_integer(response_out, 1);
        Ok(())
    }

    pub(super) fn handle_ttl(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_ttl_like(args, response_out, false)
    }

    pub(super) fn handle_pttl(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_ttl_like(args, response_out, true)
    }

    fn handle_ttl_like(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
        milliseconds: bool,
    ) -> Result<(), RequestExecutionError> {
        let (command, expected) = if milliseconds {
            ("PTTL", "PTTL key")
        } else {
            ("TTL", "TTL key")
        };
        if args.len() != 2 {
            return Err(RequestExecutionError::WrongArity { command, expected });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        self.expire_key_if_needed(&key)?;

        if !self.key_exists(&key)? {
            append_integer(response_out, -2);
            return Ok(());
        }

        let deadline = self.string_expiration_deadline(&key);
        match deadline {
            None => append_integer(response_out, -1),
            Some(deadline) => {
                let now = Instant::now();
                if deadline <= now {
                    self.expire_key_if_needed(&key)?;
                    append_integer(response_out, -2);
                } else {
                    let remaining = deadline.duration_since(now);
                    let ttl = if milliseconds {
                        remaining.as_millis().min(i64::MAX as u128) as i64
                    } else {
                        remaining.as_secs().min(i64::MAX as u64) as i64
                    };
                    append_integer(response_out, ttl);
                }
            }
        }
        Ok(())
    }

    pub(super) fn handle_persist(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 2 {
            return Err(RequestExecutionError::WrongArity {
                command: "PERSIST",
                expected: "PERSIST key",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        self.expire_key_if_needed(&key)?;
        if !self.key_exists(&key)? {
            append_integer(response_out, 0);
            return Ok(());
        }

        let shard_index = self.string_store_shard_index_for_key(&key);
        let removed_deadline = {
            let mut expirations = self.lock_string_expirations_for_shard(shard_index);
            let removed = expirations.remove(&key).is_some();
            if removed {
                self.decrement_string_expiration_count(shard_index);
            }
            removed
        };
        if !removed_deadline {
            append_integer(response_out, 0);
            return Ok(());
        }

        if !self.rewrite_existing_value_expiration(&key, None)? {
            append_integer(response_out, 0);
            return Ok(());
        }

        self.bump_watch_version(&key);
        append_integer(response_out, 1);
        Ok(())
    }
}
