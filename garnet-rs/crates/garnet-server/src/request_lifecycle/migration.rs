use super::*;

impl RequestProcessor {
    pub fn export_migration_entry(
        &self,
        key: &[u8],
    ) -> Result<Option<MigrationEntry>, RequestExecutionError> {
        self.expire_key_if_needed(key)?;

        if let Some(value) =
            self.read_string_value(DbKeyRef::new(current_request_selected_db(), key))?
        {
            return Ok(Some(MigrationEntry {
                key: ItemKey::from(key),
                value: MigrationValue::String(value.into()),
                expiration_unix_millis: self
                    .expiration_unix_millis(DbKeyRef::new(current_request_selected_db(), key)),
            }));
        }

        if let Some(object) = self.object_read(DbKeyRef::new(current_request_selected_db(), key))? {
            return Ok(Some(MigrationEntry {
                key: ItemKey::from(key),
                value: MigrationValue::Object {
                    object_type: object.object_type,
                    payload: object.payload,
                },
                expiration_unix_millis: self
                    .expiration_unix_millis(DbKeyRef::new(current_request_selected_db(), key)),
            }));
        }

        Ok(None)
    }

    pub fn import_migration_entry(
        &self,
        entry: &MigrationEntry,
    ) -> Result<(), RequestExecutionError> {
        match &entry.value {
            MigrationValue::String(value) => {
                self.upsert_string_value_for_migration(
                    DbKeyRef::new(current_request_selected_db(), entry.key.as_slice()),
                    value.as_slice(),
                    entry.expiration_unix_millis,
                )?;
                let _ = self.object_delete(DbKeyRef::new(
                    current_request_selected_db(),
                    entry.key.as_slice(),
                ))?;
            }
            MigrationValue::Object {
                object_type,
                payload,
            } => {
                self.delete_string_key_for_migration(DbKeyRef::new(
                    current_request_selected_db(),
                    entry.key.as_slice(),
                ))?;
                self.object_upsert(
                    DbKeyRef::new(current_request_selected_db(), entry.key.as_slice()),
                    *object_type,
                    payload,
                )?;
                self.set_string_expiration_deadline(
                    entry.key.as_slice(),
                    entry
                        .expiration_unix_millis
                        .and_then(instant_from_unix_millis),
                );
            }
        }
        Ok(())
    }

    pub fn migrate_keys_to(
        &self,
        target: &RequestProcessor,
        keys: &[Vec<u8>],
        delete_source: bool,
    ) -> Result<usize, RequestExecutionError> {
        let mut moved = 0usize;
        for key in keys {
            let Some(entry) = self.export_migration_entry(key)? else {
                continue;
            };
            target.import_migration_entry(&entry)?;
            if delete_source {
                self.delete_string_key_for_migration(DbKeyRef::new(
                    current_request_selected_db(),
                    key,
                ))?;
                let _ = self.object_delete(DbKeyRef::new(current_request_selected_db(), key))?;
            }
            moved += 1;
        }
        Ok(moved)
    }

    pub(super) fn snapshot_current_db_entries(
        &self,
    ) -> Result<Vec<MigrationEntry>, RequestExecutionError> {
        let mut keys = BTreeSet::<RedisKey>::new();
        keys.extend(self.string_keys_snapshot());
        keys.extend(self.object_keys_snapshot());

        let mut entries = Vec::with_capacity(keys.len());
        for key in keys {
            let expiration_unix_millis = self.expiration_unix_millis(DbKeyRef::new(
                current_request_selected_db(),
                key.as_slice(),
            ));
            let entry = if let Some(selected_db) = self.current_auxiliary_db_name() {
                let Some(entry) = self.auxiliary_value_snapshot(selected_db, key.as_slice()) else {
                    continue;
                };
                match entry.value {
                    Some(MigrationValue::String(value)) => Some(MigrationEntry {
                        key: ItemKey::from(key.as_slice()),
                        value: MigrationValue::String(value),
                        expiration_unix_millis,
                    }),
                    Some(MigrationValue::Object {
                        object_type,
                        payload,
                    }) => Some(MigrationEntry {
                        key: ItemKey::from(key.as_slice()),
                        value: MigrationValue::Object {
                            object_type,
                            payload,
                        },
                        expiration_unix_millis,
                    }),
                    None => None,
                }
            } else if let Some(value) = self
                .read_string_value(DbKeyRef::new(current_request_selected_db(), key.as_slice()))?
            {
                Some(MigrationEntry {
                    key: ItemKey::from(key.as_slice()),
                    value: MigrationValue::String(value.into()),
                    expiration_unix_millis,
                })
            } else {
                self.object_read(DbKeyRef::new(current_request_selected_db(), key.as_slice()))?
                    .map(|object| MigrationEntry {
                        key: ItemKey::from(key.as_slice()),
                        value: MigrationValue::Object {
                            object_type: object.object_type,
                            payload: object.payload,
                        },
                        expiration_unix_millis,
                    })
            };
            let Some(entry) = entry else {
                continue;
            };
            entries.push(entry);
        }
        Ok(entries)
    }

    pub fn migration_keys_for_slot(
        &self,
        slot: garnet_cluster::SlotNumber,
        max_keys: usize,
    ) -> Vec<Vec<u8>> {
        if max_keys == 0 {
            return Vec::new();
        }

        let mut slot_keys = BTreeSet::<RedisKey>::new();

        let string_keys = self.string_keys_snapshot();
        for key in string_keys {
            if redis_hash_slot(key.as_slice()) == slot {
                slot_keys.insert(key);
                if slot_keys.len() >= max_keys {
                    return slot_keys.into_iter().map(RedisKey::into_vec).collect();
                }
            }
        }

        let object_keys = self.object_keys_snapshot();
        for key in object_keys {
            if redis_hash_slot(key.as_slice()) == slot {
                slot_keys.insert(key);
                if slot_keys.len() >= max_keys {
                    return slot_keys.into_iter().map(RedisKey::into_vec).collect();
                }
            }
        }

        slot_keys.into_iter().map(RedisKey::into_vec).collect()
    }

    pub fn migrate_slot_to(
        &self,
        target: &RequestProcessor,
        slot: garnet_cluster::SlotNumber,
        max_keys: usize,
        delete_source: bool,
    ) -> Result<usize, RequestExecutionError> {
        let keys = self.migration_keys_for_slot(slot, max_keys);
        self.migrate_keys_to(target, &keys, delete_source)
    }
}
