use super::*;
use std::io::Read;
use std::io::Write;
use std::net::TcpStream;
use std::net::ToSocketAddrs;
use std::time::Duration;

pub(crate) const DUMP_BLOB_MAGIC: &[u8] = b"GRN1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RemoteMigrateAuth {
    pub(crate) username: Option<Vec<u8>>,
    pub(crate) password: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RemoteMigrateItem {
    pub(crate) key: ItemKey,
    pub(crate) ttl_millis: u64,
    pub(crate) payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RemoteMigratePlan {
    pub(crate) host: Vec<u8>,
    pub(crate) port: u16,
    pub(crate) target_db: i64,
    pub(crate) timeout_millis: u64,
    pub(crate) replace: bool,
    pub(crate) auth: Option<RemoteMigrateAuth>,
    pub(crate) items: Vec<RemoteMigrateItem>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RemoteMigrateResponse {
    Ok,
    TargetError(Vec<u8>),
    IoErr { writing: bool },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RemoteMigrateExecution {
    pub(crate) response: RemoteMigrateResponse,
    pub(crate) acknowledged_keys: Vec<ItemKey>,
}

pub(crate) fn encode_migration_dump_blob(value: &MigrationValue) -> Vec<u8> {
    let mut encoded = Vec::new();
    encoded.extend_from_slice(DUMP_BLOB_MAGIC);
    match value {
        MigrationValue::String(raw) => {
            encoded.push(0);
            let len = u32::try_from(raw.as_slice().len()).unwrap_or(u32::MAX);
            encoded.extend_from_slice(&len.to_le_bytes());
            encoded.extend_from_slice(raw.as_slice());
        }
        MigrationValue::Object {
            object_type,
            payload,
        } => {
            encoded.push(1);
            object_type.write_to(&mut encoded);
            let len = u32::try_from(payload.len()).unwrap_or(u32::MAX);
            encoded.extend_from_slice(&len.to_le_bytes());
            encoded.extend_from_slice(payload);
        }
    }
    encoded
}

fn append_resp_command_frame(target: &mut Vec<u8>, parts: &[&[u8]]) {
    append_array_length(target, parts.len());
    for part in parts {
        append_bulk_string(target, part);
    }
}

fn connect_remote_migrate_target(
    host: &[u8],
    port: u16,
    timeout: Duration,
) -> std::io::Result<TcpStream> {
    let host_text = String::from_utf8_lossy(host);
    let mut last_error = None;
    for addr in (host_text.as_ref(), port).to_socket_addrs()? {
        match TcpStream::connect_timeout(&addr, timeout) {
            Ok(stream) => {
                stream.set_nodelay(true).ok();
                stream.set_read_timeout(Some(timeout)).ok();
                stream.set_write_timeout(Some(timeout)).ok();
                return Ok(stream);
            }
            Err(error) => {
                last_error = Some(error);
            }
        }
    }

    Err(last_error.unwrap_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::AddrNotAvailable,
            "failed to resolve MIGRATE target address",
        )
    }))
}

fn read_remote_resp_line(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    let mut line = Vec::new();
    loop {
        let mut byte = [0u8; 1];
        stream.read_exact(&mut byte)?;
        line.push(byte[0]);
        if line.ends_with(b"\r\n") {
            line.truncate(line.len() - 2);
            return Ok(line);
        }
    }
}

fn format_target_error_line(line: &[u8]) -> Vec<u8> {
    line.strip_prefix(b"-").unwrap_or(line).to_vec()
}

impl RequestProcessor {
    pub(crate) fn execute_remote_migrate_plan(
        &self,
        plan: &RemoteMigratePlan,
    ) -> RemoteMigrateExecution {
        let timeout = Duration::from_millis(plan.timeout_millis);
        let mut stream =
            match connect_remote_migrate_target(plan.host.as_slice(), plan.port, timeout) {
                Ok(stream) => stream,
                Err(_) => {
                    return RemoteMigrateExecution {
                        response: RemoteMigrateResponse::IoErr { writing: false },
                        acknowledged_keys: Vec::new(),
                    };
                }
            };

        let mut request = Vec::new();
        if let Some(auth) = &plan.auth {
            if let Some(username) = auth.username.as_ref() {
                append_resp_command_frame(
                    &mut request,
                    &[b"AUTH", username.as_slice(), auth.password.as_slice()],
                );
            } else {
                append_resp_command_frame(&mut request, &[b"AUTH", auth.password.as_slice()]);
            }
        }

        let target_db_text = plan.target_db.to_string().into_bytes();
        append_resp_command_frame(&mut request, &[b"SELECT", target_db_text.as_slice()]);

        let mut ttl_texts = Vec::with_capacity(plan.items.len());
        for item in &plan.items {
            ttl_texts.push(item.ttl_millis.to_string().into_bytes());
        }
        for (index, item) in plan.items.iter().enumerate() {
            let ttl = ttl_texts[index].as_slice();
            if plan.replace {
                append_resp_command_frame(
                    &mut request,
                    &[
                        b"RESTORE",
                        item.key.as_slice(),
                        ttl,
                        item.payload.as_slice(),
                        b"REPLACE",
                    ],
                );
            } else {
                append_resp_command_frame(
                    &mut request,
                    &[
                        b"RESTORE",
                        item.key.as_slice(),
                        ttl,
                        item.payload.as_slice(),
                    ],
                );
            }
        }

        if stream.write_all(request.as_slice()).is_err() {
            return RemoteMigrateExecution {
                response: RemoteMigrateResponse::IoErr { writing: true },
                acknowledged_keys: Vec::new(),
            };
        }

        if plan.auth.is_some() {
            let auth_reply = match read_remote_resp_line(&mut stream) {
                Ok(line) => line,
                Err(_) => {
                    return RemoteMigrateExecution {
                        response: RemoteMigrateResponse::IoErr { writing: false },
                        acknowledged_keys: Vec::new(),
                    };
                }
            };
            if auth_reply.first() == Some(&b'-') {
                return RemoteMigrateExecution {
                    response: RemoteMigrateResponse::TargetError(format_target_error_line(
                        auth_reply.as_slice(),
                    )),
                    acknowledged_keys: Vec::new(),
                };
            }
        }

        let select_reply = match read_remote_resp_line(&mut stream) {
            Ok(line) => line,
            Err(_) => {
                return RemoteMigrateExecution {
                    response: RemoteMigrateResponse::IoErr { writing: false },
                    acknowledged_keys: Vec::new(),
                };
            }
        };
        if select_reply.first() == Some(&b'-') {
            return RemoteMigrateExecution {
                response: RemoteMigrateResponse::TargetError(format_target_error_line(
                    select_reply.as_slice(),
                )),
                acknowledged_keys: Vec::new(),
            };
        }

        let mut acknowledged_keys = Vec::new();
        let mut first_target_error = None;
        for item in &plan.items {
            let reply = match read_remote_resp_line(&mut stream) {
                Ok(line) => line,
                Err(_) => {
                    return RemoteMigrateExecution {
                        response: RemoteMigrateResponse::IoErr { writing: false },
                        acknowledged_keys,
                    };
                }
            };
            if reply.first() == Some(&b'-') {
                if first_target_error.is_none() {
                    first_target_error = Some(format_target_error_line(reply.as_slice()));
                }
                continue;
            }
            acknowledged_keys.push(item.key.clone());
        }

        RemoteMigrateExecution {
            response: match first_target_error {
                Some(error) => RemoteMigrateResponse::TargetError(error),
                None => RemoteMigrateResponse::Ok,
            },
            acknowledged_keys,
        }
    }

    pub(crate) fn export_migration_entry(
        &self,
        db: DbName,
        key: &[u8],
    ) -> Result<Option<MigrationEntry>, RequestExecutionError> {
        self.expire_key_if_needed(DbKeyRef::new(db, key))?;

        if let Some(value) = self.read_string_value(DbKeyRef::new(db, key))? {
            return Ok(Some(MigrationEntry {
                key: ItemKey::from(key),
                value: MigrationValue::String(value.into()),
                expiration_unix_millis: self.expiration_unix_millis(DbKeyRef::new(db, key)),
            }));
        }

        if let Some(object) = self.object_read(DbKeyRef::new(db, key))? {
            return Ok(Some(MigrationEntry {
                key: ItemKey::from(key),
                value: MigrationValue::Object {
                    object_type: object.object_type,
                    payload: object.payload,
                },
                expiration_unix_millis: self.expiration_unix_millis(DbKeyRef::new(db, key)),
            }));
        }

        Ok(None)
    }

    pub(crate) fn import_migration_entry(
        &self,
        db: DbName,
        entry: &MigrationEntry,
    ) -> Result<(), RequestExecutionError> {
        match &entry.value {
            MigrationValue::String(value) => {
                self.upsert_string_value_for_migration(
                    DbKeyRef::new(db, entry.key.as_slice()),
                    value.as_slice(),
                    entry.expiration_unix_millis,
                )?;
                let _ = self.object_delete(DbKeyRef::new(db, entry.key.as_slice()))?;
            }
            MigrationValue::Object {
                object_type,
                payload,
            } => {
                self.delete_string_key_for_migration(DbKeyRef::new(db, entry.key.as_slice()))?;
                self.object_upsert(
                    DbKeyRef::new(db, entry.key.as_slice()),
                    *object_type,
                    payload,
                )?;
                self.set_string_expiration_deadline(
                    DbKeyRef::new(db, entry.key.as_slice()),
                    entry
                        .expiration_unix_millis
                        .and_then(instant_from_unix_millis),
                );
            }
        }
        Ok(())
    }

    pub(crate) fn migrate_keys_to(
        &self,
        target: &RequestProcessor,
        source_db: DbName,
        target_db: DbName,
        keys: &[Vec<u8>],
        delete_source: bool,
    ) -> Result<usize, RequestExecutionError> {
        let mut moved = 0usize;
        for key in keys {
            let Some(entry) = self.export_migration_entry(source_db, key)? else {
                continue;
            };
            target.import_migration_entry(target_db, &entry)?;
            if delete_source {
                self.delete_string_key_for_migration(DbKeyRef::new(source_db, key))?;
                let _ = self.object_delete(DbKeyRef::new(source_db, key))?;
            }
            moved += 1;
        }
        Ok(moved)
    }

    pub(crate) fn migration_keys_for_slot(
        &self,
        db: DbName,
        slot: garnet_cluster::SlotNumber,
        max_keys: usize,
    ) -> Vec<Vec<u8>> {
        if max_keys == 0 {
            return Vec::new();
        }

        let mut slot_keys = BTreeSet::<RedisKey>::new();

        let string_keys = self.string_keys_snapshot(db);
        for key in string_keys {
            if redis_hash_slot(key.as_slice()) == slot {
                slot_keys.insert(key);
                if slot_keys.len() >= max_keys {
                    return slot_keys.into_iter().map(RedisKey::into_vec).collect();
                }
            }
        }

        let object_keys = self.object_keys_snapshot(db);
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

    pub(crate) fn migrate_slot_to(
        &self,
        target: &RequestProcessor,
        selected_db: DbName,
        slot: garnet_cluster::SlotNumber,
        max_keys: usize,
        delete_source: bool,
    ) -> Result<usize, RequestExecutionError> {
        let keys = self.migration_keys_for_slot(selected_db, slot, max_keys);
        self.migrate_keys_to(target, selected_db, selected_db, &keys, delete_source)
    }
}
