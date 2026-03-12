use super::*;

#[derive(Default)]
pub(super) struct AuxiliaryDbStorageMap {
    databases: Mutex<HashMap<AuxiliaryStorageName, AuxiliaryDbState>>,
}

impl AuxiliaryDbStorageMap {
    pub(super) fn snapshot_value(
        &self,
        storage: AuxiliaryStorageName,
        key: &[u8],
    ) -> Result<Option<AuxiliaryDbValue>, RequestExecutionError> {
        let Ok(databases) = self.databases.lock() else {
            return Err(RequestExecutionError::StorageBusy);
        };
        Ok(databases
            .get(&storage)
            .and_then(|state| state.entries.get(key).cloned()))
    }

    pub(super) fn visible_keys_snapshot(
        &self,
        storage: AuxiliaryStorageName,
    ) -> Result<Vec<RedisKey>, RequestExecutionError> {
        let Ok(databases) = self.databases.lock() else {
            return Err(RequestExecutionError::StorageBusy);
        };
        let Some(state) = databases.get(&storage) else {
            return Ok(Vec::new());
        };
        Ok(state
            .entries
            .iter()
            .filter_map(|(key, value)| value.value.as_ref().map(|_| key.clone()))
            .collect())
    }

    pub(super) fn keys_snapshot_matching(
        &self,
        storage: AuxiliaryStorageName,
        predicate: impl Fn(&MigrationValue) -> bool,
    ) -> Result<Vec<RedisKey>, RequestExecutionError> {
        let Ok(databases) = self.databases.lock() else {
            return Err(RequestExecutionError::StorageBusy);
        };
        let Some(state) = databases.get(&storage) else {
            return Ok(Vec::new());
        };
        Ok(state
            .entries
            .iter()
            .filter_map(|(key, value)| {
                let stored = value.value.as_ref()?;
                if predicate(stored) {
                    return Some(key.clone());
                }
                None
            })
            .collect())
    }

    pub(super) fn with_state<T>(
        &self,
        storage: AuxiliaryStorageName,
        create_if_missing: bool,
        f: impl FnOnce(&mut AuxiliaryDbState) -> T,
    ) -> Result<Option<T>, RequestExecutionError> {
        let Ok(mut databases) = self.databases.lock() else {
            return Err(RequestExecutionError::StorageBusy);
        };
        if create_if_missing {
            let state = databases.entry(storage).or_default();
            return Ok(Some(f(state)));
        }
        Ok(databases.get_mut(&storage).map(f))
    }

    pub(super) fn with_state_read<T>(
        &self,
        storage: AuxiliaryStorageName,
        f: impl FnOnce(&AuxiliaryDbState) -> T,
    ) -> Result<Option<T>, RequestExecutionError> {
        let Ok(databases) = self.databases.lock() else {
            return Err(RequestExecutionError::StorageBusy);
        };
        Ok(databases.get(&storage).map(f))
    }

    pub(super) fn remove_if_empty(
        &self,
        storage: AuxiliaryStorageName,
    ) -> Result<(), RequestExecutionError> {
        let Ok(mut databases) = self.databases.lock() else {
            return Err(RequestExecutionError::StorageBusy);
        };
        if databases
            .get(&storage)
            .is_some_and(|state| state.entries.is_empty())
        {
            let _ = databases.remove(&storage);
        }
        Ok(())
    }

    pub(super) fn expire_stale_keys_in_shard(
        &self,
        shard_index: ShardIndex,
        max_keys: usize,
        now: Instant,
        string_store_shard_index_for_key: impl Fn(&KeyBytes) -> ShardIndex,
    ) -> Result<Vec<(AuxiliaryStorageName, RedisKey)>, RequestExecutionError> {
        let Ok(mut databases) = self.databases.lock() else {
            return Err(RequestExecutionError::StorageBusy);
        };

        let mut expired = Vec::new();
        let mut empty_dbs = Vec::new();

        for (storage, state) in databases.iter_mut() {
            if expired.len() >= max_keys {
                break;
            }

            let expired_keys = state
                .entries
                .iter()
                .filter_map(|(key, entry)| {
                    let expiration = entry.expiration?;
                    if expiration.deadline > now {
                        return None;
                    }
                    if string_store_shard_index_for_key(key.as_slice()) != shard_index {
                        return None;
                    }
                    Some(key.clone())
                })
                .take(max_keys - expired.len())
                .collect::<Vec<_>>();

            for key in expired_keys {
                let _ = state.entries.remove(key.as_slice());
                expired.push((*storage, key));
            }

            if state.entries.is_empty() {
                empty_dbs.push(*storage);
            }
        }

        for storage in empty_dbs {
            let _ = databases.remove(&storage);
        }

        Ok(expired)
    }
}
