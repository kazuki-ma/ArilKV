use super::*;

impl RequestProcessor {
    pub fn export_migration_entry(
        &self,
        key: &[u8],
    ) -> Result<Option<MigrationEntry>, RequestExecutionError> {
        self.expire_key_if_needed(key)?;

        if let Some(value) = self.read_string_value(key)? {
            return Ok(Some(MigrationEntry {
                key: key.to_vec(),
                value: MigrationValue::String(value),
                expiration_unix_millis: self.expiration_unix_millis_for_key(key),
            }));
        }

        if let Some((object_type, payload)) = self.object_read(key)? {
            return Ok(Some(MigrationEntry {
                key: key.to_vec(),
                value: MigrationValue::Object {
                    object_type,
                    payload,
                },
                expiration_unix_millis: None,
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
                    &entry.key,
                    value,
                    entry.expiration_unix_millis,
                )?;
                let _ = self.object_delete(&entry.key)?;
            }
            MigrationValue::Object {
                object_type,
                payload,
            } => {
                self.delete_string_key_for_migration(&entry.key)?;
                self.object_upsert(&entry.key, *object_type, payload)?;
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
                self.delete_string_key_for_migration(key)?;
                let _ = self.object_delete(key)?;
            }
            moved += 1;
        }
        Ok(moved)
    }

    pub fn migration_keys_for_slot(&self, slot: u16, max_keys: usize) -> Vec<Vec<u8>> {
        if max_keys == 0 {
            return Vec::new();
        }

        let mut slot_keys = BTreeSet::new();

        let string_keys = self.string_keys_snapshot();
        for key in string_keys {
            if redis_hash_slot(&key) == slot {
                slot_keys.insert(key);
                if slot_keys.len() >= max_keys {
                    return slot_keys.into_iter().collect();
                }
            }
        }

        let object_keys = self.object_keys_snapshot();
        for key in object_keys {
            if redis_hash_slot(&key) == slot {
                slot_keys.insert(key);
                if slot_keys.len() >= max_keys {
                    return slot_keys.into_iter().collect();
                }
            }
        }

        slot_keys.into_iter().collect()
    }

    pub fn migrate_slot_to(
        &self,
        target: &RequestProcessor,
        slot: u16,
        max_keys: usize,
        delete_source: bool,
    ) -> Result<usize, RequestExecutionError> {
        let keys = self.migration_keys_for_slot(slot, max_keys);
        self.migrate_keys_to(target, &keys, delete_source)
    }
}
