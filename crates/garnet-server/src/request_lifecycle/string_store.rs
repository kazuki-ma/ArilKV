use super::*;

impl RequestProcessor {
    pub(super) fn auxiliary_value_snapshot(
        &self,
        db: DbName,
        key: &[u8],
    ) -> Option<AuxiliaryDbValue> {
        let auxiliary_storage = self.auxiliary_storage_for_logical_db(db).ok().flatten()?;
        self.db_catalog
            .auxiliary_databases
            .snapshot_value(auxiliary_storage, key)
            .ok()
            .flatten()
    }

    pub(super) fn auxiliary_keys_snapshot(
        &self,
        db: DbName,
        predicate: impl Fn(&MigrationValue) -> bool,
    ) -> Vec<RedisKey> {
        let Ok(Some(auxiliary_storage)) = self.auxiliary_storage_for_logical_db(db) else {
            return Vec::new();
        };
        self.db_catalog
            .auxiliary_databases
            .keys_snapshot_matching(auxiliary_storage, predicate)
            .unwrap_or_default()
    }

    pub(super) fn auxiliary_db_keys_snapshot(&self) -> BTreeMap<DbName, Vec<RedisKey>> {
        let configured_databases = self.configured_databases();
        let mut keys_by_db = BTreeMap::new();
        for logical_index in 0..configured_databases {
            let logical_db = DbName::new(logical_index);
            let Ok(Some(auxiliary_storage)) = self.auxiliary_storage_for_logical_db(logical_db)
            else {
                continue;
            };
            let Ok(keys) = self
                .db_catalog
                .auxiliary_databases
                .visible_keys_snapshot(auxiliary_storage)
            else {
                continue;
            };
            if !keys.is_empty() {
                keys_by_db.insert(logical_db, keys);
            }
        }
        keys_by_db
    }

    pub(super) fn with_auxiliary_db_state<T>(
        &self,
        logical_db: DbName,
        create_if_missing: bool,
        f: impl FnOnce(&mut AuxiliaryDbState) -> T,
    ) -> Result<Option<T>, RequestExecutionError> {
        let Some(auxiliary_storage) = self.auxiliary_storage_for_logical_db(logical_db)? else {
            return Ok(None);
        };
        self.db_catalog
            .auxiliary_databases
            .with_state(auxiliary_storage, create_if_missing, f)
    }

    pub(super) fn with_auxiliary_db_state_read<T>(
        &self,
        logical_db: DbName,
        f: impl FnOnce(&AuxiliaryDbState) -> T,
    ) -> Result<Option<T>, RequestExecutionError> {
        let Some(auxiliary_storage) = self.auxiliary_storage_for_logical_db(logical_db)? else {
            return Ok(None);
        };
        self.db_catalog
            .auxiliary_databases
            .with_state_read(auxiliary_storage, f)
    }

    pub fn expire_stale_keys(&self, max_keys: usize) -> Result<usize, RequestExecutionError> {
        if max_keys == 0 {
            return Ok(0);
        }

        let mut removed = 0usize;
        for shard_index in self.db_catalog.main_db_runtime.string_stores.indices() {
            if removed >= max_keys {
                break;
            }
            removed += self.expire_stale_keys_in_shard(shard_index, max_keys - removed)?;
        }
        if removed < max_keys {
            removed += self.expire_stale_auxiliary_keys(max_keys - removed)?;
        }
        Ok(removed)
    }

    pub fn expire_stale_keys_in_shard(
        &self,
        shard_index: ShardIndex,
        max_keys: usize,
    ) -> Result<usize, RequestExecutionError> {
        if max_keys == 0 {
            return Ok(0);
        }
        if !self
            .db_catalog
            .main_db_runtime
            .string_stores
            .contains_shard(shard_index)
        {
            return Ok(0);
        }
        if self.string_expiration_count_for_shard(shard_index) == 0 {
            return Ok(0);
        }

        let now = current_instant();
        let expired_keys: Vec<RedisKey> = self
            .lock_string_expirations_for_shard(shard_index)
            .iter()
            .filter_map(|(key, metadata)| {
                if metadata.deadline <= now {
                    Some(key.clone())
                } else {
                    None
                }
            })
            .take(max_keys)
            .collect();

        let mut removed = 0usize;
        for key in expired_keys {
            let logical_db = self.logical_db_for_main_runtime()?;
            let status = {
                let mut store = self.lock_string_store_for_shard(shard_index);
                let mut session = store.session(&self.functions);
                let mut info = DeleteInfo::default();
                session.delete(&key, &mut info).map_err(map_delete_error)?
            };

            self.remove_string_key_metadata_in_shard(
                DbKeyRef::new(logical_db, key.as_slice()),
                shard_index,
            );
            let object_deleted = self.object_delete(DbKeyRef::new(logical_db, key.as_slice()))?;

            match status {
                DeleteOperationStatus::TombstonedInPlace
                | DeleteOperationStatus::AppendedTombstone => {
                    removed += 1;
                    self.notify_keyspace_event(
                        logical_db,
                        NOTIFY_EXPIRED,
                        b"expired",
                        key.as_slice(),
                    );
                    if !object_deleted {
                        self.bump_watch_version_in_db(logical_db, key.as_slice());
                    }
                }
                DeleteOperationStatus::NotFound => {
                    if object_deleted {
                        removed += 1;
                        self.notify_keyspace_event(
                            logical_db,
                            NOTIFY_EXPIRED,
                            b"expired",
                            key.as_slice(),
                        );
                    }
                }
                DeleteOperationStatus::RetryLater => {
                    return Err(RequestExecutionError::StorageBusy);
                }
            }
        }
        self.record_active_expired_keys(removed as u64);
        Ok(removed)
    }

    pub fn expire_stale_auxiliary_keys(
        &self,
        max_keys: usize,
    ) -> Result<usize, RequestExecutionError> {
        if max_keys == 0 {
            return Ok(0);
        }

        let mut removed = 0usize;
        for shard_index in self.db_catalog.main_db_runtime.string_stores.indices() {
            if removed >= max_keys {
                break;
            }
            removed +=
                self.expire_stale_auxiliary_keys_in_shard(shard_index, max_keys - removed)?;
        }
        Ok(removed)
    }

    pub fn expire_stale_auxiliary_keys_in_shard(
        &self,
        shard_index: ShardIndex,
        max_keys: usize,
    ) -> Result<usize, RequestExecutionError> {
        if max_keys == 0
            || !self
                .db_catalog
                .main_db_runtime
                .string_stores
                .contains_shard(shard_index)
        {
            return Ok(0);
        }

        let now = current_instant();
        let expired = self
            .db_catalog
            .auxiliary_databases
            .expire_stale_keys_in_shard(shard_index, max_keys, now, |key| {
                self.string_store_shard_index_for_key(key)
            })?;

        for (auxiliary_storage, key) in &expired {
            let Some(logical_db) = self.logical_db_for_auxiliary_storage(*auxiliary_storage)?
            else {
                continue;
            };
            self.clear_removed_auxiliary_key_side_state(DbKeyRef::new(logical_db, key.as_slice()));
            self.notify_keyspace_event(logical_db, NOTIFY_EXPIRED, b"expired", key.as_slice());
            self.enqueue_lazy_expired_key_for_replication(DbKeyRef::new(
                logical_db,
                key.as_slice(),
            ));
            self.bump_watch_version_server_origin_in_db(logical_db, key.as_slice());
        }
        self.record_active_expired_keys(expired.len() as u64);
        Ok(expired.len())
    }

    pub(super) fn read_string_value(
        &self,
        key: DbKeyRef<'_>,
    ) -> Result<Option<Vec<u8>>, RequestExecutionError> {
        let db = key.db();
        let key_bytes = key.key();
        if !self.logical_db_uses_main_runtime(db)? {
            if !allow_expired_data_access() {
                self.expire_key_if_needed(DbKeyRef::new(db, key_bytes))?;
            }
            let entry = self.auxiliary_value_snapshot(db, key_bytes);
            self.track_read_key_for_current_client(key_bytes);
            let Some(entry) = entry else {
                return Ok(None);
            };
            let Some(MigrationValue::String(value)) = entry.value else {
                return Ok(None);
            };
            return Ok(Some(value.into_vec()));
        }

        let mut store = self.lock_string_store_for_key(key_bytes);
        let mut session = store.session(&self.functions);
        let mut output = Vec::new();
        let status = session
            .read(
                &key_bytes.to_vec(),
                &Vec::new(),
                &mut output,
                &ReadInfo::default(),
            )
            .map_err(map_read_error)?;
        match status {
            ReadOperationStatus::FoundInMemory | ReadOperationStatus::FoundOnDisk => {
                self.track_read_key_for_current_client(key_bytes);
                Ok(Some(output))
            }
            ReadOperationStatus::NotFound => {
                self.track_read_key_for_current_client(key_bytes);
                Ok(None)
            }
            ReadOperationStatus::RetryLater => Err(RequestExecutionError::StorageBusy),
        }
    }

    pub(super) fn read_string_value_without_expiring(
        &self,
        key: DbKeyRef<'_>,
    ) -> Result<Option<Vec<u8>>, RequestExecutionError> {
        let db = key.db();
        let key_bytes = key.key();
        if !self.logical_db_uses_main_runtime(db)? {
            let entry = self.auxiliary_value_snapshot(db, key_bytes);
            self.track_read_key_for_current_client(key_bytes);
            let Some(entry) = entry else {
                return Ok(None);
            };
            let Some(MigrationValue::String(value)) = entry.value else {
                return Ok(None);
            };
            return Ok(Some(value.into_vec()));
        }

        self.read_string_value(key)
    }

    pub(super) fn key_exists(&self, key: DbKeyRef<'_>) -> Result<bool, RequestExecutionError> {
        let db = key.db();
        let key_bytes = key.key();
        if !self.logical_db_uses_main_runtime(db)? {
            if !allow_expired_data_access() {
                self.expire_key_if_needed(DbKeyRef::new(db, key_bytes))?;
            }
            let entry = self.auxiliary_value_snapshot(db, key_bytes);
            self.track_read_key_for_current_client(key_bytes);
            return Ok(matches!(
                entry.and_then(|entry| entry.value),
                Some(MigrationValue::String(_))
            ));
        }

        let mut store = self.lock_string_store_for_key(key_bytes);
        let mut session = store.session(&self.functions);
        let mut output = Vec::new();
        let key_vec = key_bytes.to_vec();
        let status = session
            .read(&key_vec, &Vec::new(), &mut output, &ReadInfo::default())
            .map_err(map_read_error)?;

        match status {
            ReadOperationStatus::FoundInMemory | ReadOperationStatus::FoundOnDisk => {
                self.track_read_key_for_current_client(key_bytes);
                Ok(true)
            }
            ReadOperationStatus::NotFound => {
                self.track_read_key_for_current_client(key_bytes);
                Ok(false)
            }
            ReadOperationStatus::RetryLater => Err(RequestExecutionError::StorageBusy),
        }
    }

    pub(super) fn object_key_exists(
        &self,
        key: DbKeyRef<'_>,
    ) -> Result<bool, RequestExecutionError> {
        let db = key.db();
        let key_bytes = key.key();
        if !self.logical_db_uses_main_runtime(db)? {
            if !allow_expired_data_access() {
                self.expire_key_if_needed(DbKeyRef::new(db, key_bytes))?;
            }
            if self.has_set_hot_entry(DbKeyRef::new(db, key_bytes)) {
                self.track_read_key_for_current_client(key_bytes);
                return Ok(true);
            }
            let entry = self.auxiliary_value_snapshot(db, key_bytes);
            self.track_read_key_for_current_client(key_bytes);
            return Ok(matches!(
                entry.and_then(|entry| entry.value),
                Some(MigrationValue::Object { .. })
            ));
        }

        if self.has_set_hot_entry(DbKeyRef::new(db, key_bytes)) {
            self.track_read_key_for_current_client(key_bytes);
            return Ok(true);
        }
        let mut store = self.lock_object_store_for_key(key_bytes);
        let mut session = store.session(&self.object_functions);
        let mut output = Vec::new();
        let status = session
            .read(
                &key_bytes.to_vec(),
                &Vec::new(),
                &mut output,
                &ReadInfo::default(),
            )
            .map_err(map_read_error)?;

        match status {
            ReadOperationStatus::FoundInMemory | ReadOperationStatus::FoundOnDisk => {
                self.track_read_key_for_current_client(key_bytes);
                Ok(true)
            }
            ReadOperationStatus::NotFound => {
                self.track_read_key_for_current_client(key_bytes);
                Ok(false)
            }
            ReadOperationStatus::RetryLater => Err(RequestExecutionError::StorageBusy),
        }
    }

    pub(crate) fn key_exists_any(&self, key: DbKeyRef<'_>) -> Result<bool, RequestExecutionError> {
        if self.key_exists(key)? {
            return Ok(true);
        }
        self.object_key_exists(key)
    }

    pub(crate) fn key_exists_any_without_expiring(
        &self,
        key: DbKeyRef<'_>,
    ) -> Result<bool, RequestExecutionError> {
        let db = key.db();
        let key_bytes = key.key();
        if !self.logical_db_uses_main_runtime(db)? {
            if self.has_set_hot_entry(DbKeyRef::new(db, key_bytes)) {
                self.track_read_key_for_current_client(key_bytes);
                return Ok(true);
            }
            let entry = self.auxiliary_value_snapshot(db, key_bytes);
            self.track_read_key_for_current_client(key_bytes);
            return Ok(entry.and_then(|entry| entry.value).is_some());
        }
        self.key_exists_any(key)
    }

    pub(super) fn delete_string_value(
        &self,
        key: DbKeyRef<'_>,
    ) -> Result<bool, RequestExecutionError> {
        let db = key.db();
        let key_bytes = key.key();
        if !self.logical_db_uses_main_runtime(db)? {
            let deleted = self
                .with_auxiliary_db_state(db, false, |state| {
                    let Some(entry) = state.entries.get_mut(key_bytes) else {
                        return false;
                    };
                    if !matches!(entry.value, Some(MigrationValue::String(_))) {
                        return false;
                    }
                    entry.value = None;
                    entry.expiration = None;
                    entry.hash_field_expirations.clear();
                    let _ = state.entries.remove(key_bytes);
                    true
                })?
                .unwrap_or(false);
            if deleted {
                self.clear_removed_auxiliary_key_side_state(key);
            }
            return Ok(deleted);
        }

        let mut store = self.lock_string_store_for_key(key_bytes);
        let mut session = store.session(&self.functions);
        let mut info = DeleteInfo::default();
        let status = session
            .delete(&key_bytes.to_vec(), &mut info)
            .map_err(map_delete_error)?;

        match status {
            DeleteOperationStatus::TombstonedInPlace | DeleteOperationStatus::AppendedTombstone => {
                self.remove_string_key_metadata(key);
                Ok(true)
            }
            DeleteOperationStatus::NotFound => {
                self.remove_string_key_metadata(key);
                Ok(false)
            }
            DeleteOperationStatus::RetryLater => Err(RequestExecutionError::StorageBusy),
        }
    }

    pub(super) fn upsert_string_value_for_migration(
        &self,
        key: DbKeyRef<'_>,
        user_value: &[u8],
        expiration_unix_millis: Option<u64>,
    ) -> Result<(), RequestExecutionError> {
        let db = key.db();
        let key_bytes = key.key();
        if !self.logical_db_uses_main_runtime(db)? {
            let expiration = expiration_unix_millis.and_then(|unix_millis| {
                let deadline = instant_from_unix_millis(unix_millis)?;
                Some(ExpirationMetadata {
                    deadline,
                    unix_millis: TimestampMillis::new(unix_millis),
                })
            });
            let _ = self.with_auxiliary_db_state(db, true, |state| {
                let entry = state.entries.entry(RedisKey::from(key_bytes)).or_default();
                entry.value = Some(MigrationValue::String(StringValue::from(user_value)));
                entry.expiration = expiration;
                entry.hash_field_expirations.clear();
            })?;
            self.bump_watch_version(key);
            return Ok(());
        }

        let mut store = self.lock_string_store_for_key(key_bytes);
        let mut session = store.session(&self.functions);
        let mut output = Vec::new();
        let mut upsert_info = UpsertInfo::default();
        if expiration_unix_millis.is_some() {
            upsert_info
                .user_data
                .insert(UPSERT_USER_DATA_HAS_EXPIRATION);
        }
        let stored_value = encode_stored_value(user_value, expiration_unix_millis);
        session
            .upsert(
                &key_bytes.to_vec(),
                &stored_value,
                &mut output,
                &mut upsert_info,
            )
            .map_err(map_upsert_error)?;
        // Explicit drops to end borrows before subsequent metadata locks.
        #[allow(clippy::drop_non_drop)]
        {
            drop(session);
            drop(store);
        }

        let expiration = expiration_unix_millis.and_then(|unix_millis| {
            let deadline = instant_from_unix_millis(unix_millis)?;
            Some(ExpirationMetadata {
                deadline,
                unix_millis: TimestampMillis::new(unix_millis),
            })
        });
        let shard_index = self.string_store_shard_index_for_key(key_bytes);
        self.set_string_expiration_metadata_in_shard(key, shard_index, expiration);
        self.track_string_key(key_bytes);
        self.bump_watch_version(key);
        Ok(())
    }

    pub(super) fn write_string_value(
        &self,
        key: DbKeyRef<'_>,
        user_value: &[u8],
        expiration_unix_millis: Option<u64>,
    ) -> Result<(), RequestExecutionError> {
        let db = key.db();
        let key_bytes = key.key();
        if !self.logical_db_uses_main_runtime(db)? {
            let expiration = expiration_unix_millis.and_then(|unix_millis| {
                let deadline = instant_from_unix_millis(unix_millis)?;
                Some(ExpirationMetadata {
                    deadline,
                    unix_millis: TimestampMillis::new(unix_millis),
                })
            });
            let _ = self.with_auxiliary_db_state(db, true, |state| {
                let entry = state.entries.entry(RedisKey::from(key_bytes)).or_default();
                entry.value = Some(MigrationValue::String(StringValue::from(user_value)));
                entry.expiration = expiration;
                entry.hash_field_expirations.clear();
            })?;
            return Ok(());
        }

        let mut store = self.lock_string_store_for_key(key_bytes);
        let mut session = store.session(&self.functions);
        let mut output = Vec::new();
        let mut upsert_info = UpsertInfo::default();
        if expiration_unix_millis.is_some() {
            upsert_info
                .user_data
                .insert(UPSERT_USER_DATA_HAS_EXPIRATION);
        }
        let stored_value = encode_stored_value(user_value, expiration_unix_millis);
        session
            .upsert(
                &key_bytes.to_vec(),
                &stored_value,
                &mut output,
                &mut upsert_info,
            )
            .map_err(map_upsert_error)?;
        Ok(())
    }

    pub(super) fn delete_string_key_for_migration(
        &self,
        key: DbKeyRef<'_>,
    ) -> Result<(), RequestExecutionError> {
        if self.delete_string_value(key)? {
            self.bump_watch_version(key);
        }
        Ok(())
    }

    pub(crate) fn expiration_unix_millis(&self, key: DbKeyRef<'_>) -> Option<u64> {
        let db = key.db();
        let key_bytes = key.key();
        if let Ok(false) = self.logical_db_uses_main_runtime(db) {
            return self
                .auxiliary_value_snapshot(db, key_bytes)
                .and_then(|entry| entry.expiration)
                .map(|metadata| metadata.unix_millis.as_u64());
        }

        let shard_index = self.string_store_shard_index_for_key(key_bytes);
        if self.string_expiration_count_for_shard(shard_index) == 0 {
            return None;
        }
        self.lock_string_expirations_for_shard(shard_index)
            .get(key_bytes)
            .map(|metadata| metadata.unix_millis.as_u64())
    }

    pub(super) fn rewrite_existing_value_expiration(
        &self,
        key: DbKeyRef<'_>,
        expiration_unix_millis: Option<u64>,
    ) -> Result<bool, RequestExecutionError> {
        let db = key.db();
        let key_bytes = key.key();
        if !self.logical_db_uses_main_runtime(db)? {
            let expiration = expiration_unix_millis.and_then(|unix_millis| {
                let deadline = instant_from_unix_millis(unix_millis)?;
                Some(ExpirationMetadata {
                    deadline,
                    unix_millis: TimestampMillis::new(unix_millis),
                })
            });
            return Ok(self
                .with_auxiliary_db_state(db, false, |state| {
                    let Some(entry) = state.entries.get_mut(key_bytes) else {
                        return false;
                    };
                    if !matches!(entry.value, Some(MigrationValue::String(_))) {
                        return false;
                    }
                    entry.expiration = expiration;
                    true
                })?
                .unwrap_or(false));
        }

        let key_vec = key_bytes.to_vec();
        let mut store = self.lock_string_store_for_key(key_bytes);
        let mut session = store.session(&self.functions);
        let mut current = Vec::new();
        let status = session
            .read(&key_vec, &Vec::new(), &mut current, &ReadInfo::default())
            .map_err(map_read_error)?;
        match status {
            ReadOperationStatus::FoundInMemory | ReadOperationStatus::FoundOnDisk => {
                let mut output = Vec::new();
                let mut upsert_info = UpsertInfo::default();
                if expiration_unix_millis.is_some() {
                    upsert_info
                        .user_data
                        .insert(UPSERT_USER_DATA_HAS_EXPIRATION);
                }
                let stored_value = encode_stored_value(&current, expiration_unix_millis);
                session
                    .upsert(&key_vec, &stored_value, &mut output, &mut upsert_info)
                    .map_err(map_upsert_error)?;
                Ok(true)
            }
            ReadOperationStatus::NotFound => Ok(false),
            ReadOperationStatus::RetryLater => Err(RequestExecutionError::StorageBusy),
        }
    }

    /// Check if a key has expired and, unless expiration is suppressed by
    /// CLIENT PAUSE, physically delete it.  Returns `true` when the key is
    /// logically expired (whether or not it was actually deleted).
    pub(super) fn expire_key_if_needed(
        &self,
        key: DbKeyRef<'_>,
    ) -> Result<bool, RequestExecutionError> {
        let shard_index = self.string_store_shard_index_for_key(key.key());
        self.expire_key_if_needed_in_shard(key.db(), key.key(), shard_index)
    }

    pub(super) fn expire_key_if_needed_in_shard(
        &self,
        db: DbName,
        key: &[u8],
        shard_index: ShardIndex,
    ) -> Result<bool, RequestExecutionError> {
        if !self.logical_db_uses_main_runtime(db)? {
            if self.allow_access_expired() {
                return Ok(false);
            }
            let now = current_instant();
            if self.is_expire_action_paused() {
                let logically_expired = self.with_auxiliary_db_state(db, false, |state| {
                    let Some(entry) = state.entries.get(key) else {
                        return false;
                    };
                    let Some(expiration) = entry.expiration else {
                        return false;
                    };
                    expiration.deadline <= now
                })?;
                return Ok(logically_expired.unwrap_or(false));
            }
            let Some(should_expire) = self.with_auxiliary_db_state(db, false, |state| {
                let Some(entry) = state.entries.get(key) else {
                    return None;
                };
                let Some(expiration) = entry.expiration else {
                    return None;
                };
                if expiration.deadline > now {
                    return Some(false);
                }
                let _ = state.entries.remove(key);
                Some(true)
            })?
            else {
                return Ok(false);
            };
            let Some(should_expire) = should_expire else {
                return Ok(false);
            };
            if should_expire {
                self.clear_removed_auxiliary_key_side_state(DbKeyRef::new(db, key));
                self.notify_keyspace_event(db, NOTIFY_EXPIRED, b"expired", key);
                self.record_lazy_expired_keys(1);
                self.enqueue_lazy_expired_key_for_replication(DbKeyRef::new(db, key));
                self.bump_watch_version_server_origin_in_db(db, key);
            }
            return Ok(should_expire);
        }

        if self.allow_access_expired() {
            return Ok(false);
        }
        if self.string_expiration_count_for_shard(shard_index) == 0 {
            return Ok(false);
        }
        crate::debug_sync_point!("request_processor.expire_key_if_needed.enter");

        // When expiration is suppressed by CLIENT PAUSE, check the deadline
        // without removing metadata or deleting the key.  The caller sees the
        // key as logically expired (returns true) but the physical key remains
        // so that expired_keys stats and replication are deferred until after
        // the pause ends.
        if self.is_expire_action_paused() {
            let now = current_instant();
            let logically_expired = {
                let expirations = self.lock_string_expirations_for_shard(shard_index);
                matches!(expirations.get(key), Some(metadata) if metadata.deadline <= now)
            };
            crate::debug_sync_point!(
                "request_processor.expire_key_if_needed.after_expiration_lookup"
            );
            return Ok(logically_expired);
        }

        let should_expire = {
            let mut expirations = self.lock_string_expirations_for_shard(shard_index);
            match expirations.get(key) {
                Some(metadata) if metadata.deadline <= current_instant() => {
                    if expirations.remove(key).is_some() {
                        self.decrement_string_expiration_count(shard_index);
                    }
                    true
                }
                _ => false,
            }
        };
        crate::debug_sync_point!("request_processor.expire_key_if_needed.after_expiration_lookup");

        if !should_expire {
            return Ok(false);
        }

        crate::debug_sync_point!("request_processor.expire_key_if_needed.before_store_lock");
        let mut store = self.lock_string_store_for_shard(shard_index);
        let mut session = store.session(&self.functions);
        let mut info = DeleteInfo::default();
        let status = session
            .delete(&key.to_vec(), &mut info)
            .map_err(map_delete_error)?;
        let logical_key = DbKeyRef::new(db, key);
        let _ = self.object_delete(logical_key)?;
        match status {
            DeleteOperationStatus::TombstonedInPlace | DeleteOperationStatus::AppendedTombstone => {
                self.bump_watch_version_server_origin_in_db(db, key);
            }
            DeleteOperationStatus::NotFound => {}
            DeleteOperationStatus::RetryLater => return Err(RequestExecutionError::StorageBusy),
        }
        self.untrack_string_key_in_shard(logical_key, shard_index);
        // Replicate/emit lazy-expire deletion whenever expiration metadata says the key expired,
        // even if the underlying delete path returns NotFound.
        self.notify_keyspace_event(db, NOTIFY_EXPIRED, b"expired", key);
        self.record_lazy_expired_keys(1);
        self.enqueue_lazy_expired_key_for_replication(logical_key);
        Ok(true)
    }

    #[inline]
    pub(super) fn string_store_shard_index_for_key(&self, key: &[u8]) -> ShardIndex {
        let shard_count = self.db_catalog.main_db_runtime.string_stores.len();
        debug_assert!(shard_count > 0);
        if shard_count == 1 {
            return ShardIndex::new(0);
        }
        ShardIndex::new((fnv1a_hash64(key) as usize) % shard_count)
    }

    #[inline]
    pub(super) fn object_store_shard_index_for_key(&self, key: &[u8]) -> ShardIndex {
        self.string_store_shard_index_for_key(key)
    }

    #[inline]
    pub(super) fn string_expiration_count_for_shard(&self, shard_index: ShardIndex) -> usize {
        debug_assert!(
            self.db_catalog
                .main_db_runtime
                .string_expiration_counts
                .contains_shard(shard_index)
        );
        self.db_catalog.main_db_runtime.string_expiration_counts[shard_index]
            .load(Ordering::Acquire)
    }

    #[inline]
    pub(super) fn increment_string_expiration_count(&self, shard_index: ShardIndex) {
        debug_assert!(
            self.db_catalog
                .main_db_runtime
                .string_expiration_counts
                .contains_shard(shard_index)
        );
        self.db_catalog.main_db_runtime.string_expiration_counts[shard_index]
            .fetch_add(1, Ordering::Release);
    }

    #[inline]
    pub(super) fn decrement_string_expiration_count(&self, shard_index: ShardIndex) {
        debug_assert!(
            self.db_catalog
                .main_db_runtime
                .string_expiration_counts
                .contains_shard(shard_index)
        );
        let previous = self.db_catalog.main_db_runtime.string_expiration_counts[shard_index]
            .fetch_sub(1, Ordering::Release);
        debug_assert!(previous > 0, "expiration count underflow");
    }

    #[inline]
    pub(super) fn lock_string_store_for_shard(
        &self,
        shard_index: ShardIndex,
    ) -> OrderedMutexGuard<'_, TsavoriteKV<Vec<u8>, Vec<u8>>> {
        debug_assert!(
            self.db_catalog
                .main_db_runtime
                .string_stores
                .contains_shard(shard_index)
        );
        self.db_catalog.main_db_runtime.string_stores[shard_index]
            .lock()
            .expect("store mutex poisoned")
    }

    #[inline]
    pub(super) fn lock_string_store_for_key(
        &self,
        key: &[u8],
    ) -> OrderedMutexGuard<'_, TsavoriteKV<Vec<u8>, Vec<u8>>> {
        let shard_index = self.string_store_shard_index_for_key(key);
        self.lock_string_store_for_shard(shard_index)
    }

    #[inline]
    pub(super) fn lock_object_store_for_shard(
        &self,
        shard_index: ShardIndex,
    ) -> OrderedMutexGuard<'_, TsavoriteKV<Vec<u8>, Vec<u8>>> {
        debug_assert!(
            self.db_catalog
                .main_db_runtime
                .object_stores
                .contains_shard(shard_index)
        );
        self.db_catalog.main_db_runtime.object_stores[shard_index]
            .lock()
            .expect("object store mutex poisoned")
    }

    #[inline]
    pub(super) fn lock_object_store_for_key(
        &self,
        key: &[u8],
    ) -> OrderedMutexGuard<'_, TsavoriteKV<Vec<u8>, Vec<u8>>> {
        let shard_index = self.object_store_shard_index_for_key(key);
        self.lock_object_store_for_shard(shard_index)
    }

    #[inline]
    pub(super) fn lock_string_expirations_for_shard(
        &self,
        shard_index: ShardIndex,
    ) -> OrderedMutexGuard<'_, HashMap<RedisKey, ExpirationMetadata>> {
        debug_assert!(
            self.db_catalog
                .main_db_runtime
                .string_expirations
                .contains_shard(shard_index)
        );
        self.db_catalog.main_db_runtime.string_expirations[shard_index]
            .lock()
            .expect("expiration mutex poisoned")
    }

    #[inline]
    pub(super) fn lock_hash_field_expirations_for_shard(
        &self,
        shard_index: ShardIndex,
    ) -> OrderedMutexGuard<'_, HashMap<RedisKey, HashMap<HashField, ExpirationMetadata>>> {
        debug_assert!(
            self.db_catalog
                .main_db_runtime
                .hash_field_expirations
                .contains_shard(shard_index)
        );
        self.db_catalog.main_db_runtime.hash_field_expirations[shard_index]
            .lock()
            .expect("hash field expiration mutex poisoned")
    }

    #[inline]
    pub(super) fn lock_string_key_registry_for_shard(
        &self,
        shard_index: ShardIndex,
    ) -> OrderedMutexGuard<'_, HashSet<RedisKey>> {
        debug_assert!(
            self.db_catalog
                .main_db_runtime
                .string_key_registries
                .contains_shard(shard_index)
        );
        self.db_catalog.main_db_runtime.string_key_registries[shard_index]
            .lock()
            .expect("key registry mutex poisoned")
    }

    #[inline]
    pub(super) fn lock_object_key_registry_for_shard(
        &self,
        shard_index: ShardIndex,
    ) -> OrderedMutexGuard<'_, HashSet<RedisKey>> {
        debug_assert!(
            self.db_catalog
                .main_db_runtime
                .object_key_registries
                .contains_shard(shard_index)
        );
        self.db_catalog.main_db_runtime.object_key_registries[shard_index]
            .lock()
            .expect("object key registry mutex poisoned")
    }

    pub(super) fn track_string_key_in_shard(&self, key: &[u8], shard_index: ShardIndex) {
        let mut registry = self.lock_string_key_registry_for_shard(shard_index);
        if registry.contains(key) {
            return;
        }
        registry.insert(RedisKey::from(key));
    }

    pub(super) fn tracked_string_key_exists_in_shard(
        &self,
        key: &[u8],
        shard_index: ShardIndex,
    ) -> bool {
        self.lock_string_key_registry_for_shard(shard_index)
            .contains(key)
    }

    pub(super) fn track_string_key(&self, key: &[u8]) {
        let shard_index = self.string_store_shard_index_for_key(key);
        self.track_string_key_in_shard(key, shard_index);
    }

    pub(super) fn untrack_string_key_in_shard(&self, key: DbKeyRef<'_>, shard_index: ShardIndex) {
        self.lock_string_key_registry_for_shard(shard_index)
            .remove(key.key());
        self.clear_key_access(key);
    }

    pub(super) fn untrack_string_key(&self, key: DbKeyRef<'_>) {
        let shard_index = self.string_store_shard_index_for_key(key.key());
        self.untrack_string_key_in_shard(key, shard_index);
    }

    pub(super) fn track_object_key_in_shard(&self, key: &[u8], shard_index: ShardIndex) {
        self.lock_object_key_registry_for_shard(shard_index)
            .insert(RedisKey::from(key));
    }

    pub(super) fn tracked_object_key_exists_in_shard(
        &self,
        key: &[u8],
        shard_index: ShardIndex,
    ) -> bool {
        self.lock_object_key_registry_for_shard(shard_index)
            .contains(key)
    }

    pub(super) fn untrack_object_key_in_shard(&self, key: DbKeyRef<'_>, shard_index: ShardIndex) {
        self.lock_object_key_registry_for_shard(shard_index)
            .remove(key.key());
        self.clear_key_access(key);
    }

    pub(super) fn untrack_object_key(&self, key: DbKeyRef<'_>) {
        let shard_index = self.object_store_shard_index_for_key(key.key());
        self.untrack_object_key_in_shard(key, shard_index);
    }

    pub(super) fn set_string_expiration_metadata_in_shard(
        &self,
        key: DbKeyRef<'_>,
        shard_index: ShardIndex,
        expiration: Option<ExpirationMetadata>,
    ) {
        if let Ok(false) = self.logical_db_uses_main_runtime(key.db()) {
            let _ = self.with_auxiliary_db_state(key.db(), false, |state| {
                let Some(entry) = state.entries.get_mut(key.key()) else {
                    return;
                };
                entry.expiration = expiration;
            });
            return;
        }

        let mut expirations = self.lock_string_expirations_for_shard(shard_index);
        match expiration {
            Some(expiration) => {
                let previous = expirations.insert(RedisKey::from(key.key()), expiration);
                if previous.is_none() {
                    self.increment_string_expiration_count(shard_index);
                }
            }
            None => {
                if expirations.remove(key.key()).is_some() {
                    self.decrement_string_expiration_count(shard_index);
                }
            }
        }
    }

    pub(super) fn set_string_expiration_deadline_in_shard(
        &self,
        key: DbKeyRef<'_>,
        shard_index: ShardIndex,
        deadline: Option<Instant>,
    ) {
        let expiration = deadline.and_then(|deadline| {
            let now = current_instant();
            let now_unix_millis = current_unix_time_millis()?;
            let unix_millis = if deadline <= now {
                now_unix_millis
            } else {
                let remaining_millis =
                    u64::try_from(deadline.duration_since(now).as_millis()).ok()?;
                now_unix_millis.checked_add(remaining_millis)?
            };
            Some(ExpirationMetadata {
                deadline,
                unix_millis: TimestampMillis::new(unix_millis),
            })
        });
        self.set_string_expiration_metadata_in_shard(key, shard_index, expiration);
    }

    pub(super) fn set_string_expiration_deadline(
        &self,
        key: DbKeyRef<'_>,
        deadline: Option<Instant>,
    ) {
        let shard_index = self.string_store_shard_index_for_key(key.key());
        self.set_string_expiration_deadline_in_shard(key, shard_index, deadline);
    }

    pub(super) fn remove_string_key_metadata_in_shard(
        &self,
        key: DbKeyRef<'_>,
        shard_index: ShardIndex,
    ) {
        self.set_string_expiration_deadline_in_shard(key, shard_index, None);
        self.untrack_string_key_in_shard(key, shard_index);
        self.clear_forced_raw_string_encoding(key);
    }

    pub(super) fn remove_string_key_metadata(&self, key: DbKeyRef<'_>) {
        let shard_index = self.string_store_shard_index_for_key(key.key());
        self.remove_string_key_metadata_in_shard(key, shard_index);
    }

    pub(super) fn set_hash_field_expiration_unix_millis_in_shard(
        &self,
        key: DbKeyRef<'_>,
        shard_index: ShardIndex,
        field: &[u8],
        expiration_unix_millis: Option<u64>,
    ) {
        if let Ok(false) = self.logical_db_uses_main_runtime(key.db()) {
            let create_if_missing = expiration_unix_millis.is_some();
            let _ = self.with_auxiliary_db_state(key.db(), create_if_missing, |state| {
                match expiration_unix_millis {
                    Some(unix_millis) => {
                        let Some(deadline) = instant_from_unix_millis(unix_millis) else {
                            return;
                        };
                        let entry = state.entries.entry(RedisKey::from(key.key())).or_default();
                        entry.hash_field_expirations.insert(
                            HashField::from(field),
                            ExpirationMetadata {
                                deadline,
                                unix_millis: TimestampMillis::new(unix_millis),
                            },
                        );
                    }
                    None => {
                        let Some(entry) = state.entries.get_mut(key.key()) else {
                            return;
                        };
                        entry.hash_field_expirations.remove(field);
                    }
                }
            });
            return;
        }

        let mut expirations = self.lock_hash_field_expirations_for_shard(shard_index);
        match expiration_unix_millis {
            Some(unix_millis) => {
                let Some(deadline) = instant_from_unix_millis(unix_millis) else {
                    return;
                };
                let per_key = expirations.entry(RedisKey::from(key.key())).or_default();
                per_key.insert(
                    HashField::from(field),
                    ExpirationMetadata {
                        deadline,
                        unix_millis: TimestampMillis::new(unix_millis),
                    },
                );
            }
            None => {
                if let Some(per_key) = expirations.get_mut(key.key()) {
                    per_key.remove(field);
                    if per_key.is_empty() {
                        expirations.remove(key.key());
                    }
                }
            }
        }
    }

    pub(super) fn set_hash_field_expiration_unix_millis(
        &self,
        key: DbKeyRef<'_>,
        field: &[u8],
        expiration_unix_millis: Option<u64>,
    ) {
        let shard_index = self.object_store_shard_index_for_key(key.key());
        self.set_hash_field_expiration_unix_millis_in_shard(
            key,
            shard_index,
            field,
            expiration_unix_millis,
        );
    }

    pub(super) fn hash_field_expiration_unix_millis(
        &self,
        key: DbKeyRef<'_>,
        field: &[u8],
    ) -> Option<u64> {
        if let Ok(false) = self.logical_db_uses_main_runtime(key.db()) {
            return self
                .auxiliary_value_snapshot(key.db(), key.key())
                .and_then(|entry| entry.hash_field_expirations.get(field).cloned())
                .map(|metadata| metadata.unix_millis.as_u64());
        }

        let shard_index = self.object_store_shard_index_for_key(key.key());
        self.lock_hash_field_expirations_for_shard(shard_index)
            .get(key.key())
            .and_then(|fields| fields.get(field))
            .map(|metadata| metadata.unix_millis.as_u64())
    }

    pub(super) fn has_hash_field_expirations_for_key(&self, key: DbKeyRef<'_>) -> bool {
        if let Ok(false) = self.logical_db_uses_main_runtime(key.db()) {
            return self
                .auxiliary_value_snapshot(key.db(), key.key())
                .is_some_and(|entry| !entry.hash_field_expirations.is_empty());
        }

        let shard_index = self.object_store_shard_index_for_key(key.key());
        self.lock_hash_field_expirations_for_shard(shard_index)
            .contains_key(key.key())
    }

    pub(super) fn snapshot_hash_field_expirations_for_key(
        &self,
        key: DbKeyRef<'_>,
    ) -> Vec<(HashField, u64)> {
        let now = current_instant();
        let mut expirations = if let Ok(false) = self.logical_db_uses_main_runtime(key.db()) {
            self.auxiliary_value_snapshot(key.db(), key.key())
                .map(|entry| {
                    entry
                        .hash_field_expirations
                        .into_iter()
                        .filter_map(|(field, metadata)| {
                            if metadata.deadline <= now {
                                return None;
                            }
                            Some((field, metadata.unix_millis.as_u64()))
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default()
        } else {
            let shard_index = self.object_store_shard_index_for_key(key.key());
            self.lock_hash_field_expirations_for_shard(shard_index)
                .get(key.key())
                .map(|per_key| {
                    per_key
                        .iter()
                        .filter_map(|(field, metadata)| {
                            if metadata.deadline <= now {
                                return None;
                            }
                            Some((field.clone(), metadata.unix_millis.as_u64()))
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default()
        };
        expirations.sort_by(|left, right| left.0.as_ref().cmp(right.0.as_ref()));
        expirations
    }

    pub(super) fn restore_hash_field_expirations_for_key(
        &self,
        key: DbKeyRef<'_>,
        expirations: &[(HashField, u64)],
    ) {
        let shard_index = self.object_store_shard_index_for_key(key.key());
        self.clear_hash_field_expirations_for_key_in_shard(key, shard_index);
        for (field, expiration_unix_millis) in expirations {
            self.set_hash_field_expiration_unix_millis_in_shard(
                key,
                shard_index,
                field.as_ref(),
                Some(*expiration_unix_millis),
            );
        }
    }

    pub(super) fn clear_hash_field_expirations_for_key_in_shard(
        &self,
        key: DbKeyRef<'_>,
        shard_index: ShardIndex,
    ) {
        if let Ok(false) = self.logical_db_uses_main_runtime(key.db()) {
            let _ = self.with_auxiliary_db_state(key.db(), false, |state| {
                if let Some(entry) = state.entries.get_mut(key.key()) {
                    entry.hash_field_expirations.clear();
                }
            });
            return;
        }

        self.lock_hash_field_expirations_for_shard(shard_index)
            .remove(key.key());
    }

    pub(super) fn remove_expired_hash_fields_for_access(
        &self,
        key: DbKeyRef<'_>,
        fields: &[&[u8]],
    ) -> Vec<HashField> {
        if fields.is_empty() {
            return Vec::new();
        }
        if let Ok(false) = self.logical_db_uses_main_runtime(key.db()) {
            return self
                .with_auxiliary_db_state(key.db(), false, |state| {
                    let Some(entry) = state.entries.get_mut(key.key()) else {
                        return Vec::new();
                    };
                    let now = current_instant();
                    let mut expired_fields = Vec::new();
                    for field in fields {
                        if let Some(metadata) = entry.hash_field_expirations.get(*field)
                            && metadata.deadline <= now
                        {
                            expired_fields.push(HashField::from(*field));
                        }
                    }
                    for field in &expired_fields {
                        entry.hash_field_expirations.remove(field.as_ref());
                    }
                    expired_fields
                })
                .ok()
                .flatten()
                .unwrap_or_default();
        }

        let shard_index = self.object_store_shard_index_for_key(key.key());
        let now = current_instant();
        let mut expirations = self.lock_hash_field_expirations_for_shard(shard_index);
        let Some(per_key) = expirations.get_mut(key.key()) else {
            return Vec::new();
        };

        let mut expired_fields = Vec::new();
        for field in fields {
            if let Some(metadata) = per_key.get(*field)
                && metadata.deadline <= now
            {
                expired_fields.push(HashField::from(*field));
            }
        }
        for field in &expired_fields {
            per_key.remove(field.as_ref());
        }
        if per_key.is_empty() {
            expirations.remove(key.key());
        }
        expired_fields
    }

    pub(super) fn remove_all_expired_hash_fields_for_key(
        &self,
        key: DbKeyRef<'_>,
    ) -> Vec<HashField> {
        if let Ok(false) = self.logical_db_uses_main_runtime(key.db()) {
            return self
                .with_auxiliary_db_state(key.db(), false, |state| {
                    let Some(entry) = state.entries.get_mut(key.key()) else {
                        return Vec::new();
                    };
                    let now = current_instant();
                    let mut expired_fields = entry
                        .hash_field_expirations
                        .iter()
                        .filter_map(|(field, metadata)| {
                            if metadata.deadline <= now {
                                return Some(field.clone());
                            }
                            None
                        })
                        .collect::<Vec<_>>();
                    for field in &expired_fields {
                        entry.hash_field_expirations.remove(field);
                    }
                    expired_fields.shrink_to_fit();
                    expired_fields
                })
                .ok()
                .flatten()
                .unwrap_or_default();
        }

        let shard_index = self.object_store_shard_index_for_key(key.key());
        let now = current_instant();
        let mut expirations = self.lock_hash_field_expirations_for_shard(shard_index);
        let Some(per_key) = expirations.get_mut(key.key()) else {
            return Vec::new();
        };

        let mut expired_fields = per_key
            .iter()
            .filter_map(|(field, metadata)| {
                if metadata.deadline <= now {
                    Some(field.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        if expired_fields.is_empty() {
            return Vec::new();
        }
        for field in &expired_fields {
            per_key.remove(field);
        }
        if per_key.is_empty() {
            expirations.remove(key.key());
        }
        expired_fields.shrink_to_fit();
        expired_fields
    }

    pub(super) fn string_expiration_deadline_in_shard(
        &self,
        key: DbKeyRef<'_>,
        shard_index: ShardIndex,
    ) -> Option<Instant> {
        if let Ok(false) = self.logical_db_uses_main_runtime(key.db()) {
            return self
                .auxiliary_value_snapshot(key.db(), key.key())
                .and_then(|entry| entry.expiration)
                .map(|metadata| metadata.deadline);
        }

        if self.string_expiration_count_for_shard(shard_index) == 0 {
            return None;
        }
        self.lock_string_expirations_for_shard(shard_index)
            .get(key.key())
            .map(|metadata| metadata.deadline)
    }

    pub(super) fn string_expiration_deadline(&self, key: DbKeyRef<'_>) -> Option<Instant> {
        let shard_index = self.string_store_shard_index_for_key(key.key());
        self.string_expiration_deadline_in_shard(key, shard_index)
    }

    pub(super) fn string_keys_snapshot(&self, db: DbName) -> Vec<RedisKey> {
        if let Ok(false) = self.logical_db_uses_main_runtime(db) {
            return self
                .auxiliary_keys_snapshot(db, |value| matches!(value, MigrationValue::String(_)));
        }
        self.db_catalog
            .main_db_runtime
            .string_key_registries
            .iter()
            .flat_map(|registry| {
                registry
                    .lock()
                    .expect("key registry mutex poisoned")
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    pub(super) fn object_keys_snapshot(&self, db: DbName) -> Vec<RedisKey> {
        if let Ok(false) = self.logical_db_uses_main_runtime(db) {
            let mut keys = self.auxiliary_keys_snapshot(db, |value| {
                matches!(value, MigrationValue::Object { .. })
            });
            keys.extend(self.set_hot_keys_snapshot_for_db(db));
            keys.sort();
            keys.dedup();
            return keys;
        }
        let mut keys = self
            .db_catalog
            .main_db_runtime
            .object_key_registries
            .iter()
            .flat_map(|registry| {
                registry
                    .lock()
                    .expect("object key registry mutex poisoned")
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        keys.extend(self.set_hot_keys_snapshot_for_db(db));
        keys.sort();
        keys.dedup();
        keys
    }

    pub(super) fn bump_watch_version_in_db(&self, db: DbName, key: &[u8]) {
        let watching_clients = self.has_watching_clients();
        let tracking_clients = self.has_tracking_clients();
        if !watching_clients && !tracking_clients {
            return;
        }
        if watching_clients {
            let slot = watch_version_slot(db, key);
            self.watch_versions[slot].fetch_add(1, Ordering::SeqCst);
        }
        if tracking_clients {
            self.enqueue_tracking_invalidation_for_key(key);
        }
    }

    pub(super) fn bump_watch_version(&self, key: DbKeyRef<'_>) {
        self.bump_watch_version_in_db(key.db(), key.key());
    }

    pub(super) fn bump_watch_version_server_origin_in_db(&self, db: DbName, key: &[u8]) {
        let watching_clients = self.has_watching_clients();
        let tracking_clients = self.has_tracking_clients();
        if !watching_clients && !tracking_clients {
            return;
        }
        if watching_clients {
            let slot = watch_version_slot(db, key);
            self.watch_versions[slot].fetch_add(1, Ordering::SeqCst);
        }
        if tracking_clients {
            self.enqueue_tracking_invalidation_for_key_as_server(key);
        }
    }

    pub(super) fn bump_watch_version_server_origin(&self, key: DbKeyRef<'_>) {
        self.bump_watch_version_server_origin_in_db(key.db(), key.key());
    }
}
