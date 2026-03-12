use super::*;

impl RequestProcessor {
    fn object_upsert_raw(
        &self,
        key: DbKeyRef<'_>,
        object_type: ObjectTypeTag,
        payload: &[u8],
    ) -> Result<(), RequestExecutionError> {
        let db = key.db();
        let key_bytes = key.key();
        if !self.logical_db_uses_main_runtime(db)? {
            let _ = self.with_auxiliary_db_state(db, true, |state| {
                let entry = state.entries.entry(RedisKey::from(key_bytes)).or_default();
                entry.value = Some(MigrationValue::Object {
                    object_type,
                    payload: payload.to_vec(),
                });
            })?;
            self.bump_watch_version(key);
            return Ok(());
        }

        let shard_index = self.object_store_shard_index_for_key(key_bytes);
        let key = key_bytes.to_vec();
        let value = encode_object_value(object_type, payload);
        let mut store = self.lock_object_store_for_shard(shard_index);
        let mut session = store.session(&self.object_functions);
        let mut output = Vec::new();
        let mut info = UpsertInfo::default();
        session
            .upsert(&key, value.as_vec(), &mut output, &mut info)
            .map_err(map_upsert_error)?;
        self.track_object_key_in_shard(&key, shard_index);
        self.bump_watch_version(DbKeyRef::new(db, &key));
        Ok(())
    }

    fn object_read_raw(
        &self,
        key: DbKeyRef<'_>,
    ) -> Result<Option<DecodedObjectValue>, RequestExecutionError> {
        let db = key.db();
        let key_bytes = key.key();
        if !self.logical_db_uses_main_runtime(db)? {
            self.track_read_key_for_current_client(key_bytes);
            let Some(entry) = self.auxiliary_value_snapshot(db, key_bytes) else {
                return Ok(None);
            };
            let Some(MigrationValue::Object {
                object_type,
                payload,
            }) = entry.value
            else {
                return Ok(None);
            };
            return Ok(Some(DecodedObjectValue {
                object_type,
                payload,
            }));
        }

        let key_vec = key_bytes.to_vec();
        let mut store = self.lock_object_store_for_key(&key_vec);
        let mut session = store.session(&self.object_functions);
        let mut output = Vec::new();
        let status = session
            .read(&key_vec, &Vec::new(), &mut output, &ReadInfo::default())
            .map_err(map_read_error)?;
        match status {
            ReadOperationStatus::FoundInMemory | ReadOperationStatus::FoundOnDisk => {
                self.track_read_key_for_current_client(key_bytes);
                decode_object_value(&output).map(Some).ok_or_else(|| {
                    storage_failure("object_read", "failed to decode object value payload")
                })
            }
            ReadOperationStatus::NotFound => {
                self.track_read_key_for_current_client(key_bytes);
                Ok(None)
            }
            ReadOperationStatus::RetryLater => Err(RequestExecutionError::StorageBusy),
        }
    }

    fn object_delete_raw(&self, key: DbKeyRef<'_>) -> Result<bool, RequestExecutionError> {
        let db = key.db();
        let key_bytes = key.key();
        if !self.logical_db_uses_main_runtime(db)? {
            let deleted = self.with_auxiliary_db_state(db, false, |state| {
                let Some(entry) = state.entries.get_mut(key_bytes) else {
                    return false;
                };
                if !matches!(entry.value, Some(MigrationValue::Object { .. })) {
                    return false;
                }
                entry.value = None;
                entry.expiration = None;
                entry.hash_field_expirations.clear();
                let _ = state.entries.remove(key_bytes);
                true
            })?;
            if deleted.unwrap_or(false) {
                self.clear_forced_list_quicklist_encoding(key);
                self.clear_forced_set_encoding_floor(key);
                self.clear_set_debug_ht_state(key);
                self.bump_watch_version(key);
                return Ok(true);
            }
            return Ok(false);
        }

        let shard_index = self.object_store_shard_index_for_key(key_bytes);
        let key = key_bytes.to_vec();
        let mut store = self.lock_object_store_for_shard(shard_index);
        let mut session = store.session(&self.object_functions);
        let mut info = DeleteInfo::default();
        let status = session.delete(&key, &mut info).map_err(map_delete_error)?;
        match status {
            DeleteOperationStatus::TombstonedInPlace | DeleteOperationStatus::AppendedTombstone => {
                let logical_key = DbKeyRef::new(db, &key);
                self.set_string_expiration_deadline(logical_key, None);
                self.clear_hash_field_expirations_for_key_in_shard(logical_key, shard_index);
                self.untrack_object_key_in_shard(logical_key, shard_index);
                self.clear_forced_list_quicklist_encoding(logical_key);
                self.clear_forced_set_encoding_floor(logical_key);
                self.clear_set_debug_ht_state(logical_key);
                self.bump_watch_version(logical_key);
                Ok(true)
            }
            DeleteOperationStatus::NotFound => {
                let logical_key = DbKeyRef::new(db, &key);
                self.clear_hash_field_expirations_for_key_in_shard(logical_key, shard_index);
                self.untrack_object_key_in_shard(logical_key, shard_index);
                self.clear_forced_list_quicklist_encoding(logical_key);
                self.clear_forced_set_encoding_floor(logical_key);
                self.clear_set_debug_ht_state(logical_key);
                Ok(false)
            }
            DeleteOperationStatus::RetryLater => Err(RequestExecutionError::StorageBusy),
        }
    }

    fn serialize_set_hot_payload(payload: &DecodedSetObjectPayload) -> Vec<u8> {
        match payload {
            DecodedSetObjectPayload::Members(set) => serialize_set_object_payload(set),
            DecodedSetObjectPayload::ContiguousI64Range(range) => {
                serialize_contiguous_i64_range_set_payload(*range)
            }
        }
    }

    fn take_set_hot_entry(&self, key: DbKeyRef<'_>) -> Option<SetObjectHotEntry> {
        let Ok(mut hot_state) = self.db_catalog.side_state.set_object_hot_state.lock() else {
            return None;
        };
        hot_state.remove(key)
    }

    fn upsert_set_hot_entry(
        &self,
        key: DbKeyRef<'_>,
        entry: SetObjectHotEntry,
    ) -> Result<(), RequestExecutionError> {
        let evicted = {
            let Ok(mut hot_state) = self.db_catalog.side_state.set_object_hot_state.lock() else {
                return Err(RequestExecutionError::StorageBusy);
            };
            hot_state.insert(key, entry)
        };
        if let Some((oldest_key, oldest_entry)) = evicted
            && oldest_entry.dirty
        {
            let payload = Self::serialize_set_hot_payload(&oldest_entry.payload);
            self.object_upsert_raw(
                DbKeyRef::new(oldest_key.db, oldest_key.key.as_slice()),
                SET_OBJECT_TYPE_TAG,
                &payload,
            )?;
        }
        Ok(())
    }

    pub(super) fn has_set_hot_entry(&self, key: DbKeyRef<'_>) -> bool {
        self.db_catalog
            .side_state
            .set_object_hot_state
            .lock()
            .map(|hot_state| hot_state.contains(key))
            .unwrap_or(false)
    }

    pub(super) fn set_hot_keys_snapshot_for_db(&self, db: DbName) -> Vec<RedisKey> {
        let Ok(hot_state) = self.db_catalog.side_state.set_object_hot_state.lock() else {
            return Vec::new();
        };
        hot_state.keys_for_db(db)
    }

    pub(super) fn materialize_set_hot_entries_for_db(
        &self,
        db: DbName,
    ) -> Result<(), RequestExecutionError> {
        let mut dirty_entries = Vec::new();
        {
            let Ok(mut hot_state) = self.db_catalog.side_state.set_object_hot_state.lock() else {
                return Err(RequestExecutionError::StorageBusy);
            };
            let Some(db_state) = hot_state.by_db.get_mut(&db) else {
                return Ok(());
            };
            for (key, entry) in db_state.entries.iter_mut() {
                if !entry.dirty {
                    continue;
                }
                entry.dirty = false;
                dirty_entries.push((
                    DbScopedKey {
                        db,
                        key: key.clone(),
                    },
                    Self::serialize_set_hot_payload(&entry.payload),
                ));
            }
        }

        for (key, payload) in dirty_entries {
            self.object_upsert_raw(
                DbKeyRef::new(key.db, key.key.as_slice()),
                SET_OBJECT_TYPE_TAG,
                &payload,
            )?;
        }
        Ok(())
    }

    pub(super) fn clear_set_hot_entries_for_db(&self, db: DbName) {
        let Ok(mut hot_state) = self.db_catalog.side_state.set_object_hot_state.lock() else {
            return;
        };
        hot_state.clear_db(db);
    }

    fn set_hot_payload_for_object_read(&self, key: DbKeyRef<'_>) -> Option<Vec<u8>> {
        let Ok(mut hot_state) = self.db_catalog.side_state.set_object_hot_state.lock() else {
            return None;
        };
        let payload = {
            let db_state = hot_state.by_db.get(&key.db())?;
            let entry = db_state.entries.get(key.key())?;
            Self::serialize_set_hot_payload(&entry.payload)
        };
        hot_state.touch(key);
        Some(payload)
    }

    pub(super) fn with_set_hot_entry<R>(
        &self,
        key: DbKeyRef<'_>,
        operation: impl FnOnce(&mut Option<SetObjectHotEntry>) -> Result<R, RequestExecutionError>,
    ) -> Result<R, RequestExecutionError> {
        self.expire_key_if_needed_in_db(key.db(), key.key())?;

        let mut entry = self.take_set_hot_entry(key);
        if entry.is_none() {
            let object = match self.object_read_raw(key)? {
                Some(object) => object,
                None => {
                    if self.key_exists(key)? {
                        return Err(RequestExecutionError::WrongType);
                    }
                    DecodedObjectValue {
                        object_type: SET_OBJECT_TYPE_TAG,
                        payload: Vec::new(),
                    }
                }
            };
            if !object.payload.is_empty() {
                if object.object_type != SET_OBJECT_TYPE_TAG {
                    return Err(RequestExecutionError::WrongType);
                }
                let payload = decode_set_object_payload(&object.payload).ok_or_else(|| {
                    storage_failure(
                        "with_set_hot_entry",
                        "failed to deserialize cached set payload",
                    )
                })?;
                entry = Some(SetObjectHotEntry::new(payload, false));
            }
        }

        let result = operation(&mut entry);
        if let Some(entry) = entry {
            self.upsert_set_hot_entry(key, entry)?;
        }
        result
    }

    pub(super) fn mark_set_hot_entry_dirty(
        &self,
        key: DbKeyRef<'_>,
        entry: &mut SetObjectHotEntry,
        replace_existing: bool,
    ) {
        entry.invalidate_ordered_members();
        entry.dirty = true;
        self.record_set_debug_ht_activity(key, entry.payload.member_count());
        if let DecodedSetObjectPayload::Members(set) = &entry.payload {
            self.update_set_encoding_floor_for_members(key, set, replace_existing);
        }
        let shard_index = self.object_store_shard_index_for_key(key.key());
        self.track_object_key_in_shard(key.key(), shard_index);
        self.bump_watch_version(key);
    }

    pub(super) fn object_upsert(
        &self,
        key: DbKeyRef<'_>,
        object_type: ObjectTypeTag,
        payload: &[u8],
    ) -> Result<(), RequestExecutionError> {
        if object_type == SET_OBJECT_TYPE_TAG {
            let _ = self.take_set_hot_entry(key);
            if let Some(decoded) = decode_set_object_payload(payload) {
                self.upsert_set_hot_entry(key, SetObjectHotEntry::new(decoded, false))?;
            }
        }
        self.object_upsert_raw(key, object_type, payload)
    }

    pub(super) fn object_read(
        &self,
        key: DbKeyRef<'_>,
    ) -> Result<Option<DecodedObjectValue>, RequestExecutionError> {
        if let Some(payload) = self.set_hot_payload_for_object_read(key) {
            self.track_read_key_for_current_client(key.key());
            return Ok(Some(DecodedObjectValue {
                object_type: SET_OBJECT_TYPE_TAG,
                payload,
            }));
        }
        self.object_read_raw(key)
    }

    pub(super) fn object_delete(&self, key: DbKeyRef<'_>) -> Result<bool, RequestExecutionError> {
        let had_hot_entry = self.take_set_hot_entry(key).is_some();
        let deleted = self.object_delete_raw(key)?;
        if had_hot_entry && !deleted {
            self.bump_watch_version(key);
            return Ok(true);
        }
        Ok(deleted)
    }

    #[allow(clippy::type_complexity)]
    pub(super) fn load_hash_object(
        &self,
        key: DbKeyRef<'_>,
    ) -> Result<Option<BTreeMap<Vec<u8>, Vec<u8>>>, RequestExecutionError> {
        self.expire_key_if_needed_in_db(key.db(), key.key())?;
        let object = match self.object_read(key)? {
            Some(object) => object,
            None => {
                if self.key_exists(key)? {
                    return Err(RequestExecutionError::WrongType);
                }
                return Ok(None);
            }
        };
        if object.object_type != HASH_OBJECT_TYPE_TAG {
            return Err(RequestExecutionError::WrongType);
        }
        deserialize_hash_object_payload(&object.payload)
            .map(Some)
            .ok_or_else(|| {
                storage_failure("load_hash_object", "failed to deserialize hash payload")
            })
    }

    pub(super) fn save_hash_object(
        &self,
        key: DbKeyRef<'_>,
        hash: &BTreeMap<Vec<u8>, Vec<u8>>,
    ) -> Result<(), RequestExecutionError> {
        let payload = serialize_hash_object_payload(hash);
        self.object_upsert(key, HASH_OBJECT_TYPE_TAG, &payload)
    }

    pub(super) fn load_list_object(
        &self,
        key: DbKeyRef<'_>,
    ) -> Result<Option<Vec<Vec<u8>>>, RequestExecutionError> {
        self.expire_key_if_needed_in_db(key.db(), key.key())?;
        let object = match self.object_read(key)? {
            Some(object) => object,
            None => {
                if self.key_exists(key)? {
                    return Err(RequestExecutionError::WrongType);
                }
                return Ok(None);
            }
        };
        if object.object_type != LIST_OBJECT_TYPE_TAG {
            return Err(RequestExecutionError::WrongType);
        }
        deserialize_list_object_payload(&object.payload)
            .map(Some)
            .ok_or_else(|| {
                storage_failure("load_list_object", "failed to deserialize list payload")
            })
    }

    pub(super) fn save_list_object(
        &self,
        key: DbKeyRef<'_>,
        list: &[Vec<u8>],
    ) -> Result<(), RequestExecutionError> {
        let configured_size = self.list_max_listpack_size.load(Ordering::Acquire);
        if !list_listpack_compatible(list, configured_size) {
            self.force_list_quicklist_encoding(key);
        } else {
            self.clear_forced_list_quicklist_encoding(key);
        }
        let payload = serialize_list_object_payload(list);
        self.object_upsert(key, LIST_OBJECT_TYPE_TAG, &payload)
    }

    pub(super) fn load_set_object(
        &self,
        key: &[u8],
    ) -> Result<Option<BTreeSet<Vec<u8>>>, RequestExecutionError> {
        let payload = match self.load_set_object_payload(key)? {
            Some(payload) => payload,
            None => return Ok(None),
        };
        match payload {
            DecodedSetObjectPayload::Members(set) => Ok(Some(set)),
            DecodedSetObjectPayload::ContiguousI64Range(range) => {
                Ok(Some(materialize_contiguous_i64_range_set(range)))
            }
        }
    }

    pub(super) fn load_set_object_payload(
        &self,
        key: &[u8],
    ) -> Result<Option<DecodedSetObjectPayload>, RequestExecutionError> {
        self.expire_key_if_needed(key)?;
        let db_key = DbKeyRef::new(current_request_selected_db(), key);
        let object = match self.object_read(db_key)? {
            Some(object) => object,
            None => {
                if self.key_exists(db_key)? {
                    return Err(RequestExecutionError::WrongType);
                }
                return Ok(None);
            }
        };
        if object.object_type != SET_OBJECT_TYPE_TAG {
            return Err(RequestExecutionError::WrongType);
        }
        decode_set_object_payload(&object.payload)
            .map(Some)
            .ok_or_else(|| {
                storage_failure(
                    "load_set_object_payload",
                    "failed to deserialize set payload",
                )
            })
    }

    pub(super) fn save_set_object(
        &self,
        key: &[u8],
        set: &BTreeSet<Vec<u8>>,
    ) -> Result<(), RequestExecutionError> {
        let db_key = DbKeyRef::new(current_request_selected_db(), key);
        self.update_set_encoding_floor_for_members(db_key, set, false);
        self.record_set_debug_ht_activity(db_key, set.len());
        let payload = serialize_set_object_payload(set);
        self.object_upsert(db_key, SET_OBJECT_TYPE_TAG, &payload)
    }

    pub(super) fn save_set_object_replacing_existing(
        &self,
        key: &[u8],
        set: &BTreeSet<Vec<u8>>,
    ) -> Result<(), RequestExecutionError> {
        let db_key = DbKeyRef::new(current_request_selected_db(), key);
        self.update_set_encoding_floor_for_members(db_key, set, true);
        self.record_set_debug_ht_activity(db_key, set.len());
        let payload = serialize_set_object_payload(set);
        self.object_upsert(db_key, SET_OBJECT_TYPE_TAG, &payload)
    }

    pub(super) fn save_contiguous_i64_range_set_object(
        &self,
        key: &[u8],
        range: ContiguousI64RangeSet,
    ) -> Result<(), RequestExecutionError> {
        let member_count = usize::try_from(i128::from(range.end()) - i128::from(range.start()) + 1)
            .unwrap_or(usize::MAX);
        self.record_set_debug_ht_activity(
            DbKeyRef::new(current_request_selected_db(), key),
            member_count,
        );
        let payload = serialize_contiguous_i64_range_set_payload(range);
        self.object_upsert(
            DbKeyRef::new(current_request_selected_db(), key),
            SET_OBJECT_TYPE_TAG,
            &payload,
        )
    }

    pub(super) fn load_zset_object(
        &self,
        key: &[u8],
    ) -> Result<Option<BTreeMap<Vec<u8>, f64>>, RequestExecutionError> {
        self.expire_key_if_needed(key)?;
        let db_key = DbKeyRef::new(current_request_selected_db(), key);
        let object = match self.object_read(db_key)? {
            Some(object) => object,
            None => {
                if self.key_exists(db_key)? {
                    return Err(RequestExecutionError::WrongType);
                }
                return Ok(None);
            }
        };
        if object.object_type != ZSET_OBJECT_TYPE_TAG {
            return Err(RequestExecutionError::WrongType);
        }
        deserialize_zset_object_payload(&object.payload)
            .map(Some)
            .ok_or_else(|| {
                storage_failure("load_zset_object", "failed to deserialize zset payload")
            })
    }

    pub(super) fn save_zset_object(
        &self,
        key: &[u8],
        zset: &BTreeMap<Vec<u8>, f64>,
    ) -> Result<(), RequestExecutionError> {
        let payload = serialize_zset_object_payload(zset);
        self.object_upsert(
            DbKeyRef::new(current_request_selected_db(), key),
            ZSET_OBJECT_TYPE_TAG,
            &payload,
        )
    }

    pub(super) fn load_stream_object(
        &self,
        key: &[u8],
    ) -> Result<Option<StreamObject>, RequestExecutionError> {
        self.expire_key_if_needed(key)?;
        let db_key = DbKeyRef::new(current_request_selected_db(), key);
        let object = match self.object_read(db_key)? {
            Some(object) => object,
            None => {
                if self.key_exists(db_key)? {
                    return Err(RequestExecutionError::WrongType);
                }
                return Ok(None);
            }
        };
        if object.object_type != STREAM_OBJECT_TYPE_TAG {
            return Err(RequestExecutionError::WrongType);
        }
        let mut stream = deserialize_stream_object_payload(&object.payload).ok_or_else(|| {
            storage_failure("load_stream_object", "failed to deserialize stream payload")
        })?;
        stream.ensure_node_sizes(self.stream_node_max_entries());
        Ok(Some(stream))
    }

    pub(super) fn save_stream_object(
        &self,
        key: &[u8],
        stream: &StreamObject,
    ) -> Result<(), RequestExecutionError> {
        let mut stream_to_save = stream.clone();
        stream_to_save.ensure_node_sizes(self.stream_node_max_entries());
        let payload = serialize_stream_object_payload(&stream_to_save);
        self.object_upsert(
            DbKeyRef::new(current_request_selected_db(), key),
            STREAM_OBJECT_TYPE_TAG,
            &payload,
        )
    }
}

/// Check whether a list is eligible for listpack (compact) encoding based
/// solely on `list-max-listpack-size`.
///
/// The `DEBUG QUICKLIST-PACKED-THRESHOLD` setting does NOT affect OBJECT
/// ENCODING; it is an internal quicklist node-packing detail.  Keys with
/// oversized elements are tracked separately via the forced-quicklist flag.
pub(super) fn list_listpack_compatible(list: &[Vec<u8>], configured_size: i64) -> bool {
    const LISTPACK_POSITIVE_MIN: i64 = 1;
    const LISTPACK_NEGATIVE_MIN: i64 = -5;
    const LISTPACK_NEGATIVE_MAX: i64 = -1;

    let normalized = match configured_size {
        0 => LISTPACK_POSITIVE_MIN,
        value if value < LISTPACK_NEGATIVE_MIN => LISTPACK_NEGATIVE_MIN,
        value if value > 0 => value,
        value if (LISTPACK_NEGATIVE_MIN..=LISTPACK_NEGATIVE_MAX).contains(&value) => value,
        _ => LISTPACK_POSITIVE_MIN,
    };

    if normalized > 0 {
        return list.len() <= normalized as usize;
    }

    let max_bytes = match normalized {
        -1 => 4 * 1024usize,
        -2 => 8 * 1024usize,
        -3 => 16 * 1024usize,
        -4 => 32 * 1024usize,
        _ => 64 * 1024usize,
    };

    // In byte-limit mode, also reject if any single element exceeds the per-node
    // byte budget (matching Redis quicklist plain-node promotion logic).
    if list.iter().any(|v| v.len().saturating_add(2) > max_bytes) {
        return false;
    }

    let estimated_bytes = list
        .iter()
        .map(|value| value.len().saturating_add(2))
        .sum::<usize>();
    estimated_bytes <= max_bytes
}

pub(super) fn listpack_growth_would_force_quicklist(
    list: &[Vec<u8>],
    configured_size: i64,
    added_values: &[&[u8]],
) -> bool {
    if added_values.is_empty() {
        return false;
    }

    const LISTPACK_POSITIVE_MIN: i64 = 1;
    const LISTPACK_NEGATIVE_MIN: i64 = -5;
    const LISTPACK_NEGATIVE_MAX: i64 = -1;
    const LISTPACK_SIZE_SAFETY_LIMIT: usize = 8 * 1024usize;

    let normalized = match configured_size {
        0 => LISTPACK_POSITIVE_MIN,
        value if value < LISTPACK_NEGATIVE_MIN => LISTPACK_NEGATIVE_MIN,
        value if value > 0 => value,
        value if (LISTPACK_NEGATIVE_MIN..=LISTPACK_NEGATIVE_MAX).contains(&value) => value,
        _ => LISTPACK_POSITIVE_MIN,
    };

    let current_estimated_bytes = list
        .iter()
        .map(|value| value.len().saturating_add(2))
        .sum::<usize>();
    let added_estimated_bytes = added_values
        .iter()
        .map(|value| value.len().saturating_add(2))
        .sum::<usize>();
    let new_estimated_bytes = current_estimated_bytes.saturating_add(added_estimated_bytes);

    if normalized > 0 {
        let count_limit = normalized as usize;
        if new_estimated_bytes > LISTPACK_SIZE_SAFETY_LIMIT {
            return true;
        }
        return list.len().saturating_add(added_values.len()) > count_limit;
    }

    let max_bytes = match normalized {
        -1 => 4 * 1024usize,
        -2 => 8 * 1024usize,
        -3 => 16 * 1024usize,
        -4 => 32 * 1024usize,
        _ => 64 * 1024usize,
    };

    new_estimated_bytes > max_bytes
}

pub(super) fn listpack_shrink_should_keep_quicklist(
    list: &[Vec<u8>],
    configured_size: i64,
) -> bool {
    if list.is_empty() {
        return false;
    }

    const LISTPACK_POSITIVE_MIN: i64 = 1;
    const LISTPACK_NEGATIVE_MIN: i64 = -5;
    const LISTPACK_NEGATIVE_MAX: i64 = -1;

    let normalized = match configured_size {
        0 => LISTPACK_POSITIVE_MIN,
        value if value < LISTPACK_NEGATIVE_MIN => LISTPACK_NEGATIVE_MIN,
        value if value > 0 => value,
        value if (LISTPACK_NEGATIVE_MIN..=LISTPACK_NEGATIVE_MAX).contains(&value) => value,
        _ => LISTPACK_POSITIVE_MIN,
    };

    if normalized > 0 {
        let count_limit = normalized as usize;
        return list.len() > (count_limit / 2);
    }

    let max_bytes = match normalized {
        -1 => 4 * 1024usize,
        -2 => 8 * 1024usize,
        -3 => 16 * 1024usize,
        -4 => 32 * 1024usize,
        _ => 64 * 1024usize,
    };
    let estimated_bytes = list
        .iter()
        .map(|value| value.len().saturating_add(2))
        .sum::<usize>();
    estimated_bytes > (max_bytes / 2)
}
