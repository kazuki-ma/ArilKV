use super::*;

impl RequestProcessor {
    pub fn object_upsert(
        &self,
        key: &[u8],
        object_type: u8,
        payload: &[u8],
    ) -> Result<(), RequestExecutionError> {
        let shard_index = self.object_store_shard_index_for_key(key);
        let key = key.to_vec();
        let value = encode_object_value(object_type, payload);
        let mut store = self.lock_object_store_for_shard(shard_index);
        let mut session = store.session(&self.object_functions);
        let mut output = Vec::new();
        let mut info = UpsertInfo::default();
        session
            .upsert(&key, &value, &mut output, &mut info)
            .map_err(map_upsert_error)?;
        self.track_object_key_in_shard(&key, shard_index);
        self.bump_watch_version(&key);
        Ok(())
    }

    pub fn object_read(&self, key: &[u8]) -> Result<Option<(u8, Vec<u8>)>, RequestExecutionError> {
        let key = key.to_vec();
        let mut store = self.lock_object_store_for_key(&key);
        let mut session = store.session(&self.object_functions);
        let mut output = Vec::new();
        let status = session
            .read(&key, &Vec::new(), &mut output, &ReadInfo::default())
            .map_err(map_read_error)?;
        match status {
            ReadOperationStatus::FoundInMemory | ReadOperationStatus::FoundOnDisk => {
                decode_object_value(&output).map(Some).ok_or_else(|| {
                    storage_failure("object_read", "failed to decode object value payload")
                })
            }
            ReadOperationStatus::NotFound => Ok(None),
            ReadOperationStatus::RetryLater => Err(RequestExecutionError::StorageBusy),
        }
    }

    pub fn object_delete(&self, key: &[u8]) -> Result<bool, RequestExecutionError> {
        let shard_index = self.object_store_shard_index_for_key(key);
        let key = key.to_vec();
        let mut store = self.lock_object_store_for_shard(shard_index);
        let mut session = store.session(&self.object_functions);
        let mut info = DeleteInfo::default();
        let status = session.delete(&key, &mut info).map_err(map_delete_error)?;
        match status {
            DeleteOperationStatus::TombstonedInPlace | DeleteOperationStatus::AppendedTombstone => {
                self.set_string_expiration_deadline(&key, None);
                self.untrack_object_key_in_shard(&key, shard_index);
                self.bump_watch_version(&key);
                Ok(true)
            }
            DeleteOperationStatus::NotFound => {
                self.untrack_object_key_in_shard(&key, shard_index);
                Ok(false)
            }
            DeleteOperationStatus::RetryLater => Err(RequestExecutionError::StorageBusy),
        }
    }

    pub(super) fn load_hash_object(
        &self,
        key: &[u8],
    ) -> Result<Option<BTreeMap<Vec<u8>, Vec<u8>>>, RequestExecutionError> {
        self.expire_key_if_needed(key)?;
        let object = match self.object_read(key)? {
            Some(object) => object,
            None => {
                if self.key_exists(key)? {
                    return Err(RequestExecutionError::WrongType);
                }
                return Ok(None);
            }
        };
        if object.0 != HASH_OBJECT_TYPE_TAG {
            return Err(RequestExecutionError::WrongType);
        }
        deserialize_hash_object_payload(&object.1)
            .map(Some)
            .ok_or_else(|| {
                storage_failure("load_hash_object", "failed to deserialize hash payload")
            })
    }

    pub(super) fn save_hash_object(
        &self,
        key: &[u8],
        hash: &BTreeMap<Vec<u8>, Vec<u8>>,
    ) -> Result<(), RequestExecutionError> {
        let payload = serialize_hash_object_payload(hash);
        self.object_upsert(key, HASH_OBJECT_TYPE_TAG, &payload)
    }

    pub(super) fn load_list_object(
        &self,
        key: &[u8],
    ) -> Result<Option<Vec<Vec<u8>>>, RequestExecutionError> {
        self.expire_key_if_needed(key)?;
        let object = match self.object_read(key)? {
            Some(object) => object,
            None => {
                if self.key_exists(key)? {
                    return Err(RequestExecutionError::WrongType);
                }
                return Ok(None);
            }
        };
        if object.0 != LIST_OBJECT_TYPE_TAG {
            return Err(RequestExecutionError::WrongType);
        }
        deserialize_list_object_payload(&object.1)
            .map(Some)
            .ok_or_else(|| {
                storage_failure("load_list_object", "failed to deserialize list payload")
            })
    }

    pub(super) fn save_list_object(
        &self,
        key: &[u8],
        list: &[Vec<u8>],
    ) -> Result<(), RequestExecutionError> {
        let payload = serialize_list_object_payload(list);
        self.object_upsert(key, LIST_OBJECT_TYPE_TAG, &payload)
    }

    pub(super) fn load_set_object(
        &self,
        key: &[u8],
    ) -> Result<Option<BTreeSet<Vec<u8>>>, RequestExecutionError> {
        self.expire_key_if_needed(key)?;
        let object = match self.object_read(key)? {
            Some(object) => object,
            None => {
                if self.key_exists(key)? {
                    return Err(RequestExecutionError::WrongType);
                }
                return Ok(None);
            }
        };
        if object.0 != SET_OBJECT_TYPE_TAG {
            return Err(RequestExecutionError::WrongType);
        }
        deserialize_set_object_payload(&object.1)
            .map(Some)
            .ok_or_else(|| storage_failure("load_set_object", "failed to deserialize set payload"))
    }

    pub(super) fn save_set_object(
        &self,
        key: &[u8],
        set: &BTreeSet<Vec<u8>>,
    ) -> Result<(), RequestExecutionError> {
        let payload = serialize_set_object_payload(set);
        self.object_upsert(key, SET_OBJECT_TYPE_TAG, &payload)
    }

    pub(super) fn load_zset_object(
        &self,
        key: &[u8],
    ) -> Result<Option<BTreeMap<Vec<u8>, f64>>, RequestExecutionError> {
        self.expire_key_if_needed(key)?;
        let object = match self.object_read(key)? {
            Some(object) => object,
            None => {
                if self.key_exists(key)? {
                    return Err(RequestExecutionError::WrongType);
                }
                return Ok(None);
            }
        };
        if object.0 != ZSET_OBJECT_TYPE_TAG {
            return Err(RequestExecutionError::WrongType);
        }
        deserialize_zset_object_payload(&object.1)
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
        self.object_upsert(key, ZSET_OBJECT_TYPE_TAG, &payload)
    }

    pub(super) fn load_stream_object(
        &self,
        key: &[u8],
    ) -> Result<Option<StreamObject>, RequestExecutionError> {
        self.expire_key_if_needed(key)?;
        let object = match self.object_read(key)? {
            Some(object) => object,
            None => {
                if self.key_exists(key)? {
                    return Err(RequestExecutionError::WrongType);
                }
                return Ok(None);
            }
        };
        if object.0 != STREAM_OBJECT_TYPE_TAG {
            return Err(RequestExecutionError::WrongType);
        }
        deserialize_stream_object_payload(&object.1)
            .map(Some)
            .ok_or_else(|| {
                storage_failure("load_stream_object", "failed to deserialize stream payload")
            })
    }

    pub(super) fn save_stream_object(
        &self,
        key: &[u8],
        stream: &StreamObject,
    ) -> Result<(), RequestExecutionError> {
        let payload = serialize_stream_object_payload(stream);
        self.object_upsert(key, STREAM_OBJECT_TYPE_TAG, &payload)
    }
}
