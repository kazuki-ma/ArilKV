use super::*;

impl RequestProcessor {
    pub fn expire_stale_keys(&self, max_keys: usize) -> Result<usize, RequestExecutionError> {
        if max_keys == 0 {
            return Ok(0);
        }

        let mut removed = 0usize;
        for shard_index in self.string_stores.indices() {
            if removed >= max_keys {
                break;
            }
            removed += self.expire_stale_keys_in_shard(shard_index, max_keys - removed)?;
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
        if !self.string_stores.contains_shard(shard_index) {
            return Ok(0);
        }
        if self.string_expiration_count_for_shard(shard_index) == 0 {
            return Ok(0);
        }

        let now = Instant::now();
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
            let status = {
                let mut store = self.lock_string_store_for_shard(shard_index);
                let mut session = store.session(&self.functions);
                let mut info = DeleteInfo::default();
                session.delete(&key, &mut info).map_err(map_delete_error)?
            };

            self.remove_string_key_metadata_in_shard(key.as_slice(), shard_index);
            let object_deleted = self.object_delete(key.as_slice())?;

            match status {
                DeleteOperationStatus::TombstonedInPlace
                | DeleteOperationStatus::AppendedTombstone => {
                    removed += 1;
                    self.notify_keyspace_event(NOTIFY_EXPIRED, b"expired", key.as_slice());
                    if !object_deleted {
                        self.bump_watch_version(key.as_slice());
                    }
                }
                DeleteOperationStatus::NotFound => {
                    if object_deleted {
                        removed += 1;
                        self.notify_keyspace_event(NOTIFY_EXPIRED, b"expired", key.as_slice());
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

    pub(super) fn read_string_value(
        &self,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, RequestExecutionError> {
        let mut store = self.lock_string_store_for_key(key);
        let mut session = store.session(&self.functions);
        let mut output = Vec::new();
        let status = session
            .read(
                &key.to_vec(),
                &Vec::new(),
                &mut output,
                &ReadInfo::default(),
            )
            .map_err(map_read_error)?;
        match status {
            ReadOperationStatus::FoundInMemory | ReadOperationStatus::FoundOnDisk => {
                Ok(Some(output))
            }
            ReadOperationStatus::NotFound => Ok(None),
            ReadOperationStatus::RetryLater => Err(RequestExecutionError::StorageBusy),
        }
    }

    pub(super) fn key_exists(&self, key: &[u8]) -> Result<bool, RequestExecutionError> {
        let mut store = self.lock_string_store_for_key(key);
        let mut session = store.session(&self.functions);
        let mut output = Vec::new();
        let key_vec = key.to_vec();
        let status = session
            .read(&key_vec, &Vec::new(), &mut output, &ReadInfo::default())
            .map_err(map_read_error)?;

        match status {
            ReadOperationStatus::FoundInMemory | ReadOperationStatus::FoundOnDisk => Ok(true),
            ReadOperationStatus::NotFound => Ok(false),
            ReadOperationStatus::RetryLater => Err(RequestExecutionError::StorageBusy),
        }
    }

    pub(super) fn object_key_exists(&self, key: &[u8]) -> Result<bool, RequestExecutionError> {
        let mut store = self.lock_object_store_for_key(key);
        let mut session = store.session(&self.object_functions);
        let mut output = Vec::new();
        let status = session
            .read(
                &key.to_vec(),
                &Vec::new(),
                &mut output,
                &ReadInfo::default(),
            )
            .map_err(map_read_error)?;

        match status {
            ReadOperationStatus::FoundInMemory | ReadOperationStatus::FoundOnDisk => Ok(true),
            ReadOperationStatus::NotFound => Ok(false),
            ReadOperationStatus::RetryLater => Err(RequestExecutionError::StorageBusy),
        }
    }

    pub(crate) fn key_exists_any(&self, key: &[u8]) -> Result<bool, RequestExecutionError> {
        if self.key_exists(key)? {
            return Ok(true);
        }
        self.object_key_exists(key)
    }

    pub(super) fn upsert_string_value_for_migration(
        &self,
        key: &[u8],
        user_value: &[u8],
        expiration_unix_millis: Option<u64>,
    ) -> Result<(), RequestExecutionError> {
        let mut store = self.lock_string_store_for_key(key);
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
            .upsert(&key.to_vec(), &stored_value, &mut output, &mut upsert_info)
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
        let shard_index = self.string_store_shard_index_for_key(key);
        self.set_string_expiration_metadata_in_shard(key, shard_index, expiration);
        self.track_string_key(key);
        self.bump_watch_version(key);
        Ok(())
    }

    pub(super) fn delete_string_key_for_migration(
        &self,
        key: &[u8],
    ) -> Result<(), RequestExecutionError> {
        let mut store = self.lock_string_store_for_key(key);
        let mut session = store.session(&self.functions);
        let mut info = DeleteInfo::default();
        let status = session
            .delete(&key.to_vec(), &mut info)
            .map_err(map_delete_error)?;

        match status {
            DeleteOperationStatus::TombstonedInPlace
            | DeleteOperationStatus::AppendedTombstone
            | DeleteOperationStatus::NotFound => {
                self.remove_string_key_metadata(key);
                if !matches!(status, DeleteOperationStatus::NotFound) {
                    self.bump_watch_version(key);
                }
                Ok(())
            }
            DeleteOperationStatus::RetryLater => Err(RequestExecutionError::StorageBusy),
        }
    }

    pub(crate) fn expiration_unix_millis_for_key(&self, key: &[u8]) -> Option<u64> {
        let shard_index = self.string_store_shard_index_for_key(key);
        if self.string_expiration_count_for_shard(shard_index) == 0 {
            return None;
        }
        self.lock_string_expirations_for_shard(shard_index)
            .get(key)
            .map(|metadata| metadata.unix_millis.as_u64())
    }

    pub(super) fn rewrite_existing_value_expiration(
        &self,
        key: &[u8],
        expiration_unix_millis: Option<u64>,
    ) -> Result<bool, RequestExecutionError> {
        let key_vec = key.to_vec();
        let mut store = self.lock_string_store_for_key(key);
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
    pub(super) fn expire_key_if_needed(&self, key: &[u8]) -> Result<bool, RequestExecutionError> {
        let shard_index = self.string_store_shard_index_for_key(key);
        self.expire_key_if_needed_in_shard(key, shard_index)
    }

    pub(super) fn expire_key_if_needed_in_shard(
        &self,
        key: &[u8],
        shard_index: ShardIndex,
    ) -> Result<bool, RequestExecutionError> {
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
            let logically_expired = {
                let expirations = self.lock_string_expirations_for_shard(shard_index);
                matches!(expirations.get(key), Some(metadata) if metadata.deadline <= Instant::now())
            };
            crate::debug_sync_point!(
                "request_processor.expire_key_if_needed.after_expiration_lookup"
            );
            return Ok(logically_expired);
        }

        let should_expire = {
            let mut expirations = self.lock_string_expirations_for_shard(shard_index);
            match expirations.get(key) {
                Some(metadata) if metadata.deadline <= Instant::now() => {
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
        let object_deleted = self.object_delete(key)?;
        let key_removed = match status {
            DeleteOperationStatus::TombstonedInPlace | DeleteOperationStatus::AppendedTombstone => {
                self.bump_watch_version(key);
                true
            }
            DeleteOperationStatus::NotFound => object_deleted,
            DeleteOperationStatus::RetryLater => return Err(RequestExecutionError::StorageBusy),
        };
        self.untrack_string_key_in_shard(key, shard_index);
        if key_removed {
            self.notify_keyspace_event(NOTIFY_EXPIRED, b"expired", key);
            self.record_lazy_expired_keys(1);
            self.enqueue_lazy_expired_key_for_replication(key);
        }
        Ok(true)
    }

    #[inline]
    pub(super) fn string_store_shard_index_for_key(&self, key: &[u8]) -> ShardIndex {
        let shard_count = self.string_stores.len();
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
        debug_assert!(self.string_expiration_counts.contains_shard(shard_index));
        self.string_expiration_counts[shard_index].load(Ordering::Acquire)
    }

    #[inline]
    pub(super) fn increment_string_expiration_count(&self, shard_index: ShardIndex) {
        debug_assert!(self.string_expiration_counts.contains_shard(shard_index));
        self.string_expiration_counts[shard_index].fetch_add(1, Ordering::Release);
    }

    #[inline]
    pub(super) fn decrement_string_expiration_count(&self, shard_index: ShardIndex) {
        debug_assert!(self.string_expiration_counts.contains_shard(shard_index));
        let previous = self.string_expiration_counts[shard_index].fetch_sub(1, Ordering::Release);
        debug_assert!(previous > 0, "expiration count underflow");
    }

    #[inline]
    pub(super) fn lock_string_store_for_shard(
        &self,
        shard_index: ShardIndex,
    ) -> OrderedMutexGuard<'_, TsavoriteKV<Vec<u8>, Vec<u8>>> {
        debug_assert!(self.string_stores.contains_shard(shard_index));
        self.string_stores[shard_index]
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
        debug_assert!(self.object_stores.contains_shard(shard_index));
        self.object_stores[shard_index]
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
        debug_assert!(self.string_expirations.contains_shard(shard_index));
        self.string_expirations[shard_index]
            .lock()
            .expect("expiration mutex poisoned")
    }

    #[inline]
    pub(super) fn lock_hash_field_expirations_for_shard(
        &self,
        shard_index: ShardIndex,
    ) -> OrderedMutexGuard<'_, HashMap<RedisKey, HashMap<HashField, ExpirationMetadata>>> {
        debug_assert!(self.hash_field_expirations.contains_shard(shard_index));
        self.hash_field_expirations[shard_index]
            .lock()
            .expect("hash field expiration mutex poisoned")
    }

    #[inline]
    pub(super) fn lock_string_key_registry_for_shard(
        &self,
        shard_index: ShardIndex,
    ) -> OrderedMutexGuard<'_, HashSet<RedisKey>> {
        debug_assert!(self.string_key_registries.contains_shard(shard_index));
        self.string_key_registries[shard_index]
            .lock()
            .expect("key registry mutex poisoned")
    }

    #[inline]
    pub(super) fn lock_object_key_registry_for_shard(
        &self,
        shard_index: ShardIndex,
    ) -> OrderedMutexGuard<'_, HashSet<RedisKey>> {
        debug_assert!(self.object_key_registries.contains_shard(shard_index));
        self.object_key_registries[shard_index]
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

    pub(super) fn track_string_key(&self, key: &[u8]) {
        let shard_index = self.string_store_shard_index_for_key(key);
        self.track_string_key_in_shard(key, shard_index);
    }

    pub(super) fn untrack_string_key_in_shard(&self, key: &[u8], shard_index: ShardIndex) {
        self.lock_string_key_registry_for_shard(shard_index)
            .remove(key);
        self.clear_key_access(key);
    }

    pub(super) fn untrack_string_key(&self, key: &[u8]) {
        let shard_index = self.string_store_shard_index_for_key(key);
        self.untrack_string_key_in_shard(key, shard_index);
    }

    pub(super) fn track_object_key_in_shard(&self, key: &[u8], shard_index: ShardIndex) {
        self.lock_object_key_registry_for_shard(shard_index)
            .insert(RedisKey::from(key));
    }

    pub(super) fn untrack_object_key_in_shard(&self, key: &[u8], shard_index: ShardIndex) {
        self.lock_object_key_registry_for_shard(shard_index)
            .remove(key);
        self.clear_key_access(key);
    }

    pub(super) fn untrack_object_key(&self, key: &[u8]) {
        let shard_index = self.object_store_shard_index_for_key(key);
        self.untrack_object_key_in_shard(key, shard_index);
    }

    pub(super) fn set_string_expiration_metadata_in_shard(
        &self,
        key: &[u8],
        shard_index: ShardIndex,
        expiration: Option<ExpirationMetadata>,
    ) {
        let mut expirations = self.lock_string_expirations_for_shard(shard_index);
        match expiration {
            Some(expiration) => {
                let previous = expirations.insert(RedisKey::from(key), expiration);
                if previous.is_none() {
                    self.increment_string_expiration_count(shard_index);
                }
            }
            None => {
                if expirations.remove(key).is_some() {
                    self.decrement_string_expiration_count(shard_index);
                }
            }
        }
    }

    pub(super) fn set_string_expiration_deadline_in_shard(
        &self,
        key: &[u8],
        shard_index: ShardIndex,
        deadline: Option<Instant>,
    ) {
        let expiration = deadline.and_then(|deadline| {
            let now = Instant::now();
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

    pub(super) fn set_string_expiration_deadline(&self, key: &[u8], deadline: Option<Instant>) {
        let shard_index = self.string_store_shard_index_for_key(key);
        self.set_string_expiration_deadline_in_shard(key, shard_index, deadline);
    }

    pub(super) fn remove_string_key_metadata_in_shard(&self, key: &[u8], shard_index: ShardIndex) {
        self.set_string_expiration_deadline_in_shard(key, shard_index, None);
        self.untrack_string_key_in_shard(key, shard_index);
    }

    pub(super) fn remove_string_key_metadata(&self, key: &[u8]) {
        let shard_index = self.string_store_shard_index_for_key(key);
        self.remove_string_key_metadata_in_shard(key, shard_index);
    }

    pub(super) fn set_hash_field_expiration_unix_millis_in_shard(
        &self,
        key: &[u8],
        shard_index: ShardIndex,
        field: &[u8],
        expiration_unix_millis: Option<u64>,
    ) {
        let mut expirations = self.lock_hash_field_expirations_for_shard(shard_index);
        match expiration_unix_millis {
            Some(unix_millis) => {
                let Some(deadline) = instant_from_unix_millis(unix_millis) else {
                    return;
                };
                let per_key = expirations.entry(RedisKey::from(key)).or_default();
                per_key.insert(
                    HashField::from(field),
                    ExpirationMetadata {
                        deadline,
                        unix_millis: TimestampMillis::new(unix_millis),
                    },
                );
            }
            None => {
                if let Some(per_key) = expirations.get_mut(key) {
                    per_key.remove(field);
                    if per_key.is_empty() {
                        expirations.remove(key);
                    }
                }
            }
        }
    }

    pub(super) fn set_hash_field_expiration_unix_millis(
        &self,
        key: &[u8],
        field: &[u8],
        expiration_unix_millis: Option<u64>,
    ) {
        let shard_index = self.object_store_shard_index_for_key(key);
        self.set_hash_field_expiration_unix_millis_in_shard(
            key,
            shard_index,
            field,
            expiration_unix_millis,
        );
    }

    pub(super) fn hash_field_expiration_unix_millis(
        &self,
        key: &[u8],
        field: &[u8],
    ) -> Option<u64> {
        let shard_index = self.object_store_shard_index_for_key(key);
        self.lock_hash_field_expirations_for_shard(shard_index)
            .get(key)
            .and_then(|fields| fields.get(field))
            .map(|metadata| metadata.unix_millis.as_u64())
    }

    pub(super) fn clear_hash_field_expirations_for_key_in_shard(
        &self,
        key: &[u8],
        shard_index: ShardIndex,
    ) {
        self.lock_hash_field_expirations_for_shard(shard_index)
            .remove(key);
    }

    pub(super) fn remove_expired_hash_fields_for_access(
        &self,
        key: &[u8],
        fields: &[&[u8]],
    ) -> Vec<HashField> {
        if fields.is_empty() {
            return Vec::new();
        }
        let shard_index = self.object_store_shard_index_for_key(key);
        let now = Instant::now();
        let mut expirations = self.lock_hash_field_expirations_for_shard(shard_index);
        let Some(per_key) = expirations.get_mut(key) else {
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
            expirations.remove(key);
        }
        expired_fields
    }

    pub(super) fn remove_all_expired_hash_fields_for_key(&self, key: &[u8]) -> Vec<HashField> {
        let shard_index = self.object_store_shard_index_for_key(key);
        let now = Instant::now();
        let mut expirations = self.lock_hash_field_expirations_for_shard(shard_index);
        let Some(per_key) = expirations.get_mut(key) else {
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
            expirations.remove(key);
        }
        expired_fields.shrink_to_fit();
        expired_fields
    }

    pub(super) fn string_expiration_deadline_in_shard(
        &self,
        key: &[u8],
        shard_index: ShardIndex,
    ) -> Option<Instant> {
        if self.string_expiration_count_for_shard(shard_index) == 0 {
            return None;
        }
        self.lock_string_expirations_for_shard(shard_index)
            .get(key)
            .map(|metadata| metadata.deadline)
    }

    pub(super) fn string_expiration_deadline(&self, key: &[u8]) -> Option<Instant> {
        let shard_index = self.string_store_shard_index_for_key(key);
        self.string_expiration_deadline_in_shard(key, shard_index)
    }

    pub(super) fn string_keys_snapshot(&self) -> Vec<RedisKey> {
        self.string_key_registries
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

    pub(super) fn object_keys_snapshot(&self) -> Vec<RedisKey> {
        self.object_key_registries
            .iter()
            .flat_map(|registry| {
                registry
                    .lock()
                    .expect("object key registry mutex poisoned")
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    pub(super) fn bump_watch_version(&self, key: &[u8]) {
        let slot = watch_version_slot(key);
        self.watch_versions[slot].fetch_add(1, Ordering::SeqCst);
    }
}
