//! Request lifecycle: parse result -> dispatch -> storage op -> RESP response.

use crate::{dispatch_from_arg_slices, CommandId};
use garnet_common::ArgSlice;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::mem::size_of;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tsavorite::{
    DeleteInfo, DeleteOperationStatus, HybridLogDeleteAdapter, HybridLogReadAdapter,
    HybridLogRmwAdapter, HybridLogUpsertAdapter, ISessionFunctions, ReadInfo, ReadOperationStatus,
    RecordInfo, RmwInfo, RmwOperationError, RmwOperationStatus, TsavoriteKV, TsavoriteKvConfig,
    TsavoriteKvInitError, UpsertInfo, WriteReason,
};

const UPSERT_USER_DATA_HAS_EXPIRATION: u8 = 0x1;
const VALUE_EXPIRATION_PREFIX_LEN: usize = size_of::<u64>();
const HASH_OBJECT_TYPE_TAG: u8 = 3;
const LIST_OBJECT_TYPE_TAG: u8 = 2;
const SET_OBJECT_TYPE_TAG: u8 = 4;
const ZSET_OBJECT_TYPE_TAG: u8 = 5;
const WATCH_VERSION_MAP_SIZE: usize = 1024;
const WATCH_VERSION_MAP_MASK: usize = WATCH_VERSION_MAP_SIZE - 1;

#[derive(Debug, Clone, Copy)]
struct ExpirationMetadata {
    deadline: Instant,
    unix_millis: u64,
}

#[derive(Debug)]
pub enum RequestProcessorInitError {
    Tsavorite(TsavoriteKvInitError),
}

impl core::fmt::Display for RequestProcessorInitError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Tsavorite(inner) => inner.fmt(f),
        }
    }
}

impl std::error::Error for RequestProcessorInitError {}

impl From<TsavoriteKvInitError> for RequestProcessorInitError {
    fn from(value: TsavoriteKvInitError) -> Self {
        Self::Tsavorite(value)
    }
}

pub struct RequestProcessor {
    store: Mutex<TsavoriteKV<Vec<u8>, Vec<u8>>>,
    object_store: Mutex<TsavoriteKV<Vec<u8>, Vec<u8>>>,
    expirations: Mutex<HashMap<Vec<u8>, Instant>>,
    key_registry: Mutex<HashSet<Vec<u8>>>,
    watch_versions: Vec<AtomicU64>,
    functions: KvSessionFunctions,
    object_functions: ObjectSessionFunctions,
}

impl RequestProcessor {
    pub fn new() -> Result<Self, RequestProcessorInitError> {
        Ok(Self {
            store: Mutex::new(TsavoriteKV::new(TsavoriteKvConfig::default())?),
            object_store: Mutex::new(TsavoriteKV::new(TsavoriteKvConfig::default())?),
            expirations: Mutex::new(HashMap::new()),
            key_registry: Mutex::new(HashSet::new()),
            watch_versions: (0..WATCH_VERSION_MAP_SIZE)
                .map(|_| AtomicU64::new(0))
                .collect(),
            functions: KvSessionFunctions,
            object_functions: ObjectSessionFunctions,
        })
    }

    pub fn watch_key_version(&self, key: &[u8]) -> u64 {
        let slot = watch_version_slot(key);
        self.watch_versions[slot].load(Ordering::SeqCst)
    }

    pub fn watch_versions_match(&self, watched_keys: &[(Vec<u8>, u64)]) -> bool {
        watched_keys
            .iter()
            .all(|(key, expected)| self.watch_key_version(key) == *expected)
    }

    pub fn execute(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.is_empty() {
            return Err(RequestExecutionError::UnknownCommand);
        }

        // SAFETY: caller ensures ArgSlice points into a live request buffer.
        let command = unsafe { dispatch_from_arg_slices(args) };
        match command {
            CommandId::Get => self.handle_get(args, response_out),
            CommandId::Set => self.handle_set(args, response_out),
            CommandId::Del => self.handle_del(args, response_out),
            CommandId::Incr => self.handle_incr_decr(args, 1, response_out),
            CommandId::Decr => self.handle_incr_decr(args, -1, response_out),
            CommandId::Expire => self.handle_expire(args, response_out),
            CommandId::Ttl => self.handle_ttl(args, response_out),
            CommandId::Pexpire => self.handle_pexpire(args, response_out),
            CommandId::Pttl => self.handle_pttl(args, response_out),
            CommandId::Persist => self.handle_persist(args, response_out),
            CommandId::Hset => self.handle_hset(args, response_out),
            CommandId::Hget => self.handle_hget(args, response_out),
            CommandId::Hdel => self.handle_hdel(args, response_out),
            CommandId::Hgetall => self.handle_hgetall(args, response_out),
            CommandId::Lpush => self.handle_lpush(args, response_out),
            CommandId::Rpush => self.handle_rpush(args, response_out),
            CommandId::Lpop => self.handle_lpop(args, response_out),
            CommandId::Rpop => self.handle_rpop(args, response_out),
            CommandId::Lrange => self.handle_lrange(args, response_out),
            CommandId::Sadd => self.handle_sadd(args, response_out),
            CommandId::Srem => self.handle_srem(args, response_out),
            CommandId::Smembers => self.handle_smembers(args, response_out),
            CommandId::Sismember => self.handle_sismember(args, response_out),
            CommandId::Zadd => self.handle_zadd(args, response_out),
            CommandId::Zrem => self.handle_zrem(args, response_out),
            CommandId::Zrange => self.handle_zrange(args, response_out),
            CommandId::Zscore => self.handle_zscore(args, response_out),
            CommandId::Ping => self.handle_ping(args, response_out),
            CommandId::Echo => self.handle_echo(args, response_out),
            CommandId::Info => self.handle_info(args, response_out),
            CommandId::Dbsize => self.handle_dbsize(args, response_out),
            CommandId::Command => self.handle_command(args, response_out),
            _ => Err(RequestExecutionError::UnknownCommand),
        }
    }

    pub fn object_upsert(
        &self,
        key: &[u8],
        object_type: u8,
        payload: &[u8],
    ) -> Result<(), RequestExecutionError> {
        let key = key.to_vec();
        let value = encode_object_value(object_type, payload);
        let mut store = self
            .object_store
            .lock()
            .expect("object store mutex poisoned");
        let mut session = store.session(&self.object_functions);
        let mut output = Vec::new();
        let mut info = UpsertInfo::default();
        session
            .upsert(&key, &value, &mut output, &mut info)
            .map_err(|_| RequestExecutionError::StorageFailure)?;
        self.bump_watch_version(&key);
        Ok(())
    }

    pub fn object_read(&self, key: &[u8]) -> Result<Option<(u8, Vec<u8>)>, RequestExecutionError> {
        let key = key.to_vec();
        let mut store = self
            .object_store
            .lock()
            .expect("object store mutex poisoned");
        let mut session = store.session(&self.object_functions);
        let mut output = Vec::new();
        let status = session
            .read(&key, &Vec::new(), &mut output, &ReadInfo::default())
            .map_err(|_| RequestExecutionError::StorageFailure)?;
        match status {
            ReadOperationStatus::FoundInMemory | ReadOperationStatus::FoundOnDisk => {
                decode_object_value(&output)
                    .map(Some)
                    .ok_or(RequestExecutionError::StorageFailure)
            }
            ReadOperationStatus::NotFound => Ok(None),
            ReadOperationStatus::RetryLater => Err(RequestExecutionError::StorageBusy),
        }
    }

    pub fn object_delete(&self, key: &[u8]) -> Result<bool, RequestExecutionError> {
        let key = key.to_vec();
        let mut store = self
            .object_store
            .lock()
            .expect("object store mutex poisoned");
        let mut session = store.session(&self.object_functions);
        let mut info = DeleteInfo::default();
        let status = session
            .delete(&key, &mut info)
            .map_err(|_| RequestExecutionError::StorageFailure)?;
        match status {
            DeleteOperationStatus::TombstonedInPlace | DeleteOperationStatus::AppendedTombstone => {
                self.bump_watch_version(&key);
                Ok(true)
            }
            DeleteOperationStatus::NotFound => Ok(false),
            DeleteOperationStatus::RetryLater => Err(RequestExecutionError::StorageBusy),
        }
    }

    fn load_hash_object(
        &self,
        key: &[u8],
    ) -> Result<Option<BTreeMap<Vec<u8>, Vec<u8>>>, RequestExecutionError> {
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
            .ok_or(RequestExecutionError::StorageFailure)
    }

    fn save_hash_object(
        &self,
        key: &[u8],
        hash: &BTreeMap<Vec<u8>, Vec<u8>>,
    ) -> Result<(), RequestExecutionError> {
        let payload = serialize_hash_object_payload(hash);
        self.object_upsert(key, HASH_OBJECT_TYPE_TAG, &payload)
    }

    fn load_list_object(&self, key: &[u8]) -> Result<Option<Vec<Vec<u8>>>, RequestExecutionError> {
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
            .ok_or(RequestExecutionError::StorageFailure)
    }

    fn save_list_object(&self, key: &[u8], list: &[Vec<u8>]) -> Result<(), RequestExecutionError> {
        let payload = serialize_list_object_payload(list);
        self.object_upsert(key, LIST_OBJECT_TYPE_TAG, &payload)
    }

    fn load_set_object(
        &self,
        key: &[u8],
    ) -> Result<Option<BTreeSet<Vec<u8>>>, RequestExecutionError> {
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
            .ok_or(RequestExecutionError::StorageFailure)
    }

    fn save_set_object(
        &self,
        key: &[u8],
        set: &BTreeSet<Vec<u8>>,
    ) -> Result<(), RequestExecutionError> {
        let payload = serialize_set_object_payload(set);
        self.object_upsert(key, SET_OBJECT_TYPE_TAG, &payload)
    }

    fn load_zset_object(
        &self,
        key: &[u8],
    ) -> Result<Option<BTreeMap<Vec<u8>, f64>>, RequestExecutionError> {
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
            .ok_or(RequestExecutionError::StorageFailure)
    }

    fn save_zset_object(
        &self,
        key: &[u8],
        zset: &BTreeMap<Vec<u8>, f64>,
    ) -> Result<(), RequestExecutionError> {
        let payload = serialize_zset_object_payload(zset);
        self.object_upsert(key, ZSET_OBJECT_TYPE_TAG, &payload)
    }

    pub fn expire_stale_keys(&self, max_keys: usize) -> Result<usize, RequestExecutionError> {
        if max_keys == 0 {
            return Ok(0);
        }

        let now = Instant::now();
        let expired_keys: Vec<Vec<u8>> = self
            .expirations
            .lock()
            .expect("expiration mutex poisoned")
            .iter()
            .filter_map(|(key, deadline)| {
                if *deadline <= now {
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
                let mut store = self.store.lock().expect("store mutex poisoned");
                let mut session = store.session(&self.functions);
                let mut info = DeleteInfo::default();
                session
                    .delete(&key, &mut info)
                    .map_err(|_| RequestExecutionError::StorageFailure)?
            };

            self.expirations
                .lock()
                .expect("expiration mutex poisoned")
                .remove(&key);
            self.key_registry
                .lock()
                .expect("key registry mutex poisoned")
                .remove(&key);

            match status {
                DeleteOperationStatus::TombstonedInPlace
                | DeleteOperationStatus::AppendedTombstone => {
                    removed += 1;
                    self.bump_watch_version(&key);
                }
                DeleteOperationStatus::NotFound => {}
                DeleteOperationStatus::RetryLater => {
                    return Err(RequestExecutionError::StorageBusy);
                }
            }
        }

        Ok(removed)
    }

    fn handle_get(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 2 {
            return Err(RequestExecutionError::WrongArity {
                command: "GET",
                expected: "GET key",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        self.expire_key_if_needed(&key)?;

        let mut store = self.store.lock().expect("store mutex poisoned");
        let mut session = store.session(&self.functions);
        let mut output = Vec::new();
        let status = session
            .read(&key, &Vec::new(), &mut output, &ReadInfo::default())
            .map_err(|_| RequestExecutionError::StorageFailure)?;

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

    fn handle_set(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
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
        let options = parse_set_options(args)?;
        self.expire_key_if_needed(&key)?;

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

        let mut store = self.store.lock().expect("store mutex poisoned");
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
            .map_err(|_| RequestExecutionError::StorageFailure)?;
        drop(session);
        drop(store);

        let mut expirations = self.expirations.lock().expect("expiration mutex poisoned");
        match options.expiration {
            Some(expiration) => {
                expirations.insert(key.clone(), expiration.deadline);
            }
            None => {
                expirations.remove(&key);
            }
        }
        self.key_registry
            .lock()
            .expect("key registry mutex poisoned")
            .insert(key.clone());
        self.bump_watch_version(&key);

        append_simple_string(response_out, b"OK");
        Ok(())
    }

    fn handle_del(
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
        let mut store = self.store.lock().expect("store mutex poisoned");
        let mut session = store.session(&self.functions);
        for key in keys {
            let mut info = DeleteInfo::default();
            let status = session
                .delete(&key, &mut info)
                .map_err(|_| RequestExecutionError::StorageFailure)?;
            match status {
                DeleteOperationStatus::TombstonedInPlace
                | DeleteOperationStatus::AppendedTombstone => {
                    deleted += 1;
                    self.expirations
                        .lock()
                        .expect("expiration mutex poisoned")
                        .remove(&key);
                    self.key_registry
                        .lock()
                        .expect("key registry mutex poisoned")
                        .remove(&key);
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

    fn handle_incr_decr(
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
        let mut store = self.store.lock().expect("store mutex poisoned");
        let mut session = store.session(&self.functions);
        let status = session.rmw(&key, &input, &mut output, &mut info);

        match status {
            Ok(RmwOperationStatus::InPlaceUpdated)
            | Ok(RmwOperationStatus::CopiedToTail)
            | Ok(RmwOperationStatus::Inserted) => {
                let parsed =
                    parse_i64_ascii(&output).ok_or(RequestExecutionError::ValueNotInteger)?;
                self.key_registry
                    .lock()
                    .expect("key registry mutex poisoned")
                    .insert(key.clone());
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
                    .map_err(|_| RequestExecutionError::StorageFailure)?;
                self.key_registry
                    .lock()
                    .expect("key registry mutex poisoned")
                    .insert(key.clone());
                self.bump_watch_version(&key);
                append_integer(response_out, delta);
                Ok(())
            }
            Err(RmwOperationError::OperationCancelled) => {
                Err(RequestExecutionError::ValueNotInteger)
            }
            Err(_) => Err(RequestExecutionError::StorageFailure),
        }
    }

    fn handle_expire(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_expire_like(args, response_out, false)
    }

    fn handle_pexpire(
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
            let mut store = self.store.lock().expect("store mutex poisoned");
            let mut session = store.session(&self.functions);
            let mut info = DeleteInfo::default();
            let status = session
                .delete(&key, &mut info)
                .map_err(|_| RequestExecutionError::StorageFailure)?;
            match status {
                DeleteOperationStatus::TombstonedInPlace
                | DeleteOperationStatus::AppendedTombstone => {
                    self.expirations
                        .lock()
                        .expect("expiration mutex poisoned")
                        .remove(&key);
                    self.key_registry
                        .lock()
                        .expect("key registry mutex poisoned")
                        .remove(&key);
                    self.bump_watch_version(&key);
                    append_integer(response_out, 1);
                    Ok(())
                }
                DeleteOperationStatus::NotFound => {
                    self.expirations
                        .lock()
                        .expect("expiration mutex poisoned")
                        .remove(&key);
                    self.key_registry
                        .lock()
                        .expect("key registry mutex poisoned")
                        .remove(&key);
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
        self.expirations
            .lock()
            .expect("expiration mutex poisoned")
            .insert(key.clone(), expiration.deadline);
        if !self.rewrite_existing_value_expiration(&key, Some(expiration.unix_millis))? {
            self.expirations
                .lock()
                .expect("expiration mutex poisoned")
                .remove(&key);
            append_integer(response_out, 0);
            return Ok(());
        }
        self.bump_watch_version(&key);
        append_integer(response_out, 1);
        Ok(())
    }

    fn handle_ttl(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_ttl_like(args, response_out, false)
    }

    fn handle_pttl(
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

        let deadline = self
            .expirations
            .lock()
            .expect("expiration mutex poisoned")
            .get(&key)
            .copied();
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

    fn handle_persist(
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

        let removed_deadline = self
            .expirations
            .lock()
            .expect("expiration mutex poisoned")
            .remove(&key)
            .is_some();
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

    fn handle_hset(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() < 4 || ((args.len() - 2) % 2 != 0) {
            return Err(RequestExecutionError::WrongArity {
                command: "HSET",
                expected: "HSET key field value [field value ...]",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        let mut hash = self.load_hash_object(&key)?.unwrap_or_default();
        let mut inserted = 0i64;

        let mut index = 2usize;
        while index < args.len() {
            // SAFETY: caller guarantees argument backing memory validity.
            let field = unsafe { args[index].as_slice() }.to_vec();
            // SAFETY: caller guarantees argument backing memory validity.
            let value = unsafe { args[index + 1].as_slice() }.to_vec();
            if hash.insert(field, value).is_none() {
                inserted += 1;
            }
            index += 2;
        }

        self.save_hash_object(&key, &hash)?;
        append_integer(response_out, inserted);
        Ok(())
    }

    fn handle_hget(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 3 {
            return Err(RequestExecutionError::WrongArity {
                command: "HGET",
                expected: "HGET key field",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        // SAFETY: caller guarantees argument backing memory validity.
        let field = unsafe { args[2].as_slice() };
        let hash = match self.load_hash_object(&key)? {
            Some(hash) => hash,
            None => {
                append_null_bulk_string(response_out);
                return Ok(());
            }
        };

        match hash.get(field) {
            Some(value) => append_bulk_string(response_out, value),
            None => append_null_bulk_string(response_out),
        }
        Ok(())
    }

    fn handle_hdel(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() < 3 {
            return Err(RequestExecutionError::WrongArity {
                command: "HDEL",
                expected: "HDEL key field [field ...]",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        let mut hash = match self.load_hash_object(&key)? {
            Some(hash) => hash,
            None => {
                append_integer(response_out, 0);
                return Ok(());
            }
        };

        let mut removed = 0i64;
        for field in &args[2..] {
            // SAFETY: caller guarantees argument backing memory validity.
            if hash.remove(unsafe { field.as_slice() }).is_some() {
                removed += 1;
            }
        }

        if hash.is_empty() {
            let _ = self.object_delete(&key)?;
        } else {
            self.save_hash_object(&key, &hash)?;
        }
        append_integer(response_out, removed);
        Ok(())
    }

    fn handle_hgetall(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 2 {
            return Err(RequestExecutionError::WrongArity {
                command: "HGETALL",
                expected: "HGETALL key",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        let hash = match self.load_hash_object(&key)? {
            Some(hash) => hash,
            None => {
                response_out.extend_from_slice(b"*0\r\n");
                return Ok(());
            }
        };

        let pair_count = hash.len().saturating_mul(2);
        response_out.push(b'*');
        response_out.extend_from_slice(pair_count.to_string().as_bytes());
        response_out.extend_from_slice(b"\r\n");
        for (field, value) in &hash {
            append_bulk_string(response_out, field);
            append_bulk_string(response_out, value);
        }
        Ok(())
    }

    fn handle_lpush(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() < 3 {
            return Err(RequestExecutionError::WrongArity {
                command: "LPUSH",
                expected: "LPUSH key value [value ...]",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        let mut list = self.load_list_object(&key)?.unwrap_or_default();
        for value in &args[2..] {
            // SAFETY: caller guarantees argument backing memory validity.
            list.insert(0, unsafe { value.as_slice() }.to_vec());
        }
        self.save_list_object(&key, &list)?;
        append_integer(response_out, list.len() as i64);
        Ok(())
    }

    fn handle_rpush(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() < 3 {
            return Err(RequestExecutionError::WrongArity {
                command: "RPUSH",
                expected: "RPUSH key value [value ...]",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        let mut list = self.load_list_object(&key)?.unwrap_or_default();
        for value in &args[2..] {
            // SAFETY: caller guarantees argument backing memory validity.
            list.push(unsafe { value.as_slice() }.to_vec());
        }
        self.save_list_object(&key, &list)?;
        append_integer(response_out, list.len() as i64);
        Ok(())
    }

    fn handle_lpop(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 2 {
            return Err(RequestExecutionError::WrongArity {
                command: "LPOP",
                expected: "LPOP key",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        let mut list = match self.load_list_object(&key)? {
            Some(list) => list,
            None => {
                append_null_bulk_string(response_out);
                return Ok(());
            }
        };
        if list.is_empty() {
            let _ = self.object_delete(&key)?;
            append_null_bulk_string(response_out);
            return Ok(());
        }

        let value = list.remove(0);
        if list.is_empty() {
            let _ = self.object_delete(&key)?;
        } else {
            self.save_list_object(&key, &list)?;
        }
        append_bulk_string(response_out, &value);
        Ok(())
    }

    fn handle_rpop(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 2 {
            return Err(RequestExecutionError::WrongArity {
                command: "RPOP",
                expected: "RPOP key",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        let mut list = match self.load_list_object(&key)? {
            Some(list) => list,
            None => {
                append_null_bulk_string(response_out);
                return Ok(());
            }
        };
        let value = match list.pop() {
            Some(value) => value,
            None => {
                let _ = self.object_delete(&key)?;
                append_null_bulk_string(response_out);
                return Ok(());
            }
        };

        if list.is_empty() {
            let _ = self.object_delete(&key)?;
        } else {
            self.save_list_object(&key, &list)?;
        }
        append_bulk_string(response_out, &value);
        Ok(())
    }

    fn handle_lrange(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 4 {
            return Err(RequestExecutionError::WrongArity {
                command: "LRANGE",
                expected: "LRANGE key start stop",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        // SAFETY: caller guarantees argument backing memory validity.
        let start = parse_i64_ascii(unsafe { args[2].as_slice() })
            .ok_or(RequestExecutionError::ValueNotInteger)?;
        // SAFETY: caller guarantees argument backing memory validity.
        let stop = parse_i64_ascii(unsafe { args[3].as_slice() })
            .ok_or(RequestExecutionError::ValueNotInteger)?;
        let list = match self.load_list_object(&key)? {
            Some(list) => list,
            None => {
                response_out.extend_from_slice(b"*0\r\n");
                return Ok(());
            }
        };

        let len = list.len() as i64;
        if len == 0 {
            response_out.extend_from_slice(b"*0\r\n");
            return Ok(());
        }

        let mut normalized_start = if start < 0 { len + start } else { start };
        let mut normalized_stop = if stop < 0 { len + stop } else { stop };
        if normalized_start < 0 {
            normalized_start = 0;
        }
        if normalized_stop < 0 || normalized_start >= len {
            response_out.extend_from_slice(b"*0\r\n");
            return Ok(());
        }
        if normalized_stop >= len {
            normalized_stop = len - 1;
        }
        if normalized_start > normalized_stop {
            response_out.extend_from_slice(b"*0\r\n");
            return Ok(());
        }

        let count = (normalized_stop - normalized_start + 1) as usize;
        response_out.push(b'*');
        response_out.extend_from_slice(count.to_string().as_bytes());
        response_out.extend_from_slice(b"\r\n");
        for index in normalized_start..=normalized_stop {
            append_bulk_string(response_out, &list[index as usize]);
        }
        Ok(())
    }

    fn handle_sadd(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() < 3 {
            return Err(RequestExecutionError::WrongArity {
                command: "SADD",
                expected: "SADD key member [member ...]",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        let mut set = self.load_set_object(&key)?.unwrap_or_default();
        let mut inserted = 0i64;
        for member in &args[2..] {
            // SAFETY: caller guarantees argument backing memory validity.
            if set.insert(unsafe { member.as_slice() }.to_vec()) {
                inserted += 1;
            }
        }
        self.save_set_object(&key, &set)?;
        append_integer(response_out, inserted);
        Ok(())
    }

    fn handle_srem(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() < 3 {
            return Err(RequestExecutionError::WrongArity {
                command: "SREM",
                expected: "SREM key member [member ...]",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        let mut set = match self.load_set_object(&key)? {
            Some(set) => set,
            None => {
                append_integer(response_out, 0);
                return Ok(());
            }
        };
        let mut removed = 0i64;
        for member in &args[2..] {
            // SAFETY: caller guarantees argument backing memory validity.
            if set.remove(unsafe { member.as_slice() }) {
                removed += 1;
            }
        }

        if set.is_empty() {
            let _ = self.object_delete(&key)?;
        } else {
            self.save_set_object(&key, &set)?;
        }
        append_integer(response_out, removed);
        Ok(())
    }

    fn handle_smembers(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 2 {
            return Err(RequestExecutionError::WrongArity {
                command: "SMEMBERS",
                expected: "SMEMBERS key",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        let set = match self.load_set_object(&key)? {
            Some(set) => set,
            None => {
                response_out.extend_from_slice(b"*0\r\n");
                return Ok(());
            }
        };

        response_out.push(b'*');
        response_out.extend_from_slice(set.len().to_string().as_bytes());
        response_out.extend_from_slice(b"\r\n");
        for member in &set {
            append_bulk_string(response_out, member);
        }
        Ok(())
    }

    fn handle_sismember(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 3 {
            return Err(RequestExecutionError::WrongArity {
                command: "SISMEMBER",
                expected: "SISMEMBER key member",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        // SAFETY: caller guarantees argument backing memory validity.
        let member = unsafe { args[2].as_slice() };
        let set = match self.load_set_object(&key)? {
            Some(set) => set,
            None => {
                append_integer(response_out, 0);
                return Ok(());
            }
        };
        append_integer(response_out, if set.contains(member) { 1 } else { 0 });
        Ok(())
    }

    fn handle_zadd(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() < 4 || ((args.len() - 2) % 2 != 0) {
            return Err(RequestExecutionError::WrongArity {
                command: "ZADD",
                expected: "ZADD key score member [score member ...]",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        let mut zset = self.load_zset_object(&key)?.unwrap_or_default();
        let mut inserted = 0i64;

        let mut index = 2usize;
        while index < args.len() {
            // SAFETY: caller guarantees argument backing memory validity.
            let score = parse_f64_ascii(unsafe { args[index].as_slice() })
                .ok_or(RequestExecutionError::ValueNotFloat)?;
            // SAFETY: caller guarantees argument backing memory validity.
            let member = unsafe { args[index + 1].as_slice() }.to_vec();
            if zset.insert(member, score).is_none() {
                inserted += 1;
            }
            index += 2;
        }

        self.save_zset_object(&key, &zset)?;
        append_integer(response_out, inserted);
        Ok(())
    }

    fn handle_zrem(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() < 3 {
            return Err(RequestExecutionError::WrongArity {
                command: "ZREM",
                expected: "ZREM key member [member ...]",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        let mut zset = match self.load_zset_object(&key)? {
            Some(zset) => zset,
            None => {
                append_integer(response_out, 0);
                return Ok(());
            }
        };

        let mut removed = 0i64;
        for member in &args[2..] {
            // SAFETY: caller guarantees argument backing memory validity.
            if zset.remove(unsafe { member.as_slice() }).is_some() {
                removed += 1;
            }
        }

        if zset.is_empty() {
            let _ = self.object_delete(&key)?;
        } else {
            self.save_zset_object(&key, &zset)?;
        }
        append_integer(response_out, removed);
        Ok(())
    }

    fn handle_zrange(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 4 {
            return Err(RequestExecutionError::WrongArity {
                command: "ZRANGE",
                expected: "ZRANGE key start stop",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        // SAFETY: caller guarantees argument backing memory validity.
        let start = parse_i64_ascii(unsafe { args[2].as_slice() })
            .ok_or(RequestExecutionError::ValueNotInteger)?;
        // SAFETY: caller guarantees argument backing memory validity.
        let stop = parse_i64_ascii(unsafe { args[3].as_slice() })
            .ok_or(RequestExecutionError::ValueNotInteger)?;
        let zset = match self.load_zset_object(&key)? {
            Some(zset) => zset,
            None => {
                response_out.extend_from_slice(b"*0\r\n");
                return Ok(());
            }
        };

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

        let len = entries.len() as i64;
        if len == 0 {
            response_out.extend_from_slice(b"*0\r\n");
            return Ok(());
        }

        let mut normalized_start = if start < 0 { len + start } else { start };
        let mut normalized_stop = if stop < 0 { len + stop } else { stop };
        if normalized_start < 0 {
            normalized_start = 0;
        }
        if normalized_stop < 0 || normalized_start >= len {
            response_out.extend_from_slice(b"*0\r\n");
            return Ok(());
        }
        if normalized_stop >= len {
            normalized_stop = len - 1;
        }
        if normalized_start > normalized_stop {
            response_out.extend_from_slice(b"*0\r\n");
            return Ok(());
        }

        let count = (normalized_stop - normalized_start + 1) as usize;
        response_out.push(b'*');
        response_out.extend_from_slice(count.to_string().as_bytes());
        response_out.extend_from_slice(b"\r\n");
        for index in normalized_start..=normalized_stop {
            append_bulk_string(response_out, entries[index as usize].0);
        }
        Ok(())
    }

    fn handle_zscore(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 3 {
            return Err(RequestExecutionError::WrongArity {
                command: "ZSCORE",
                expected: "ZSCORE key member",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        // SAFETY: caller guarantees argument backing memory validity.
        let member = unsafe { args[2].as_slice() };
        let zset = match self.load_zset_object(&key)? {
            Some(zset) => zset,
            None => {
                append_null_bulk_string(response_out);
                return Ok(());
            }
        };

        match zset.get(member) {
            Some(score) => append_bulk_string(response_out, score.to_string().as_bytes()),
            None => append_null_bulk_string(response_out),
        }
        Ok(())
    }

    fn handle_ping(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        match args.len() {
            1 => {
                append_simple_string(response_out, b"PONG");
                Ok(())
            }
            2 => {
                // SAFETY: caller guarantees argument backing memory validity.
                append_bulk_string(response_out, unsafe { args[1].as_slice() });
                Ok(())
            }
            _ => Err(RequestExecutionError::WrongArity {
                command: "PING",
                expected: "PING [message]",
            }),
        }
    }

    fn handle_echo(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 2 {
            return Err(RequestExecutionError::WrongArity {
                command: "ECHO",
                expected: "ECHO message",
            });
        }
        // SAFETY: caller guarantees argument backing memory validity.
        append_bulk_string(response_out, unsafe { args[1].as_slice() });
        Ok(())
    }

    fn handle_info(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 1 {
            return Err(RequestExecutionError::WrongArity {
                command: "INFO",
                expected: "INFO",
            });
        }

        let dbsize = self.active_key_count()?;
        let payload = format!(
            "# Server\r\nredis_version:garnet-rs\r\n# Stats\r\ndbsize:{}\r\n",
            dbsize
        );
        append_bulk_string(response_out, payload.as_bytes());
        Ok(())
    }

    fn handle_dbsize(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 1 {
            return Err(RequestExecutionError::WrongArity {
                command: "DBSIZE",
                expected: "DBSIZE",
            });
        }
        append_integer(response_out, self.active_key_count()?);
        Ok(())
    }

    fn handle_command(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 1 {
            return Err(RequestExecutionError::WrongArity {
                command: "COMMAND",
                expected: "COMMAND",
            });
        }
        append_bulk_array(
            response_out,
            &[
                b"GET",
                b"SET",
                b"DEL",
                b"INCR",
                b"DECR",
                b"EXPIRE",
                b"TTL",
                b"PEXPIRE",
                b"PTTL",
                b"PERSIST",
                b"HSET",
                b"HGET",
                b"HDEL",
                b"HGETALL",
                b"LPUSH",
                b"RPUSH",
                b"LPOP",
                b"RPOP",
                b"LRANGE",
                b"SADD",
                b"SREM",
                b"SMEMBERS",
                b"SISMEMBER",
                b"ZADD",
                b"ZREM",
                b"ZRANGE",
                b"ZSCORE",
                b"MULTI",
                b"EXEC",
                b"DISCARD",
                b"WATCH",
                b"UNWATCH",
                b"PING",
                b"ECHO",
                b"INFO",
                b"DBSIZE",
                b"COMMAND",
            ],
        );
        Ok(())
    }

    fn active_key_count(&self) -> Result<i64, RequestExecutionError> {
        let keys: Vec<Vec<u8>> = self
            .key_registry
            .lock()
            .expect("key registry mutex poisoned")
            .iter()
            .cloned()
            .collect();

        let mut count = 0i64;
        for key in keys {
            self.expire_key_if_needed(&key)?;
            if self.key_exists(&key)? {
                count += 1;
            } else {
                self.key_registry
                    .lock()
                    .expect("key registry mutex poisoned")
                    .remove(&key);
            }
        }
        Ok(count)
    }

    fn key_exists(&self, key: &[u8]) -> Result<bool, RequestExecutionError> {
        let mut store = self.store.lock().expect("store mutex poisoned");
        let mut session = store.session(&self.functions);
        let mut output = Vec::new();
        let key_vec = key.to_vec();
        let status = session
            .read(&key_vec, &Vec::new(), &mut output, &ReadInfo::default())
            .map_err(|_| RequestExecutionError::StorageFailure)?;

        match status {
            ReadOperationStatus::FoundInMemory | ReadOperationStatus::FoundOnDisk => Ok(true),
            ReadOperationStatus::NotFound => Ok(false),
            ReadOperationStatus::RetryLater => Err(RequestExecutionError::StorageBusy),
        }
    }

    fn rewrite_existing_value_expiration(
        &self,
        key: &[u8],
        expiration_unix_millis: Option<u64>,
    ) -> Result<bool, RequestExecutionError> {
        let key_vec = key.to_vec();
        let mut store = self.store.lock().expect("store mutex poisoned");
        let mut session = store.session(&self.functions);
        let mut current = Vec::new();
        let status = session
            .read(&key_vec, &Vec::new(), &mut current, &ReadInfo::default())
            .map_err(|_| RequestExecutionError::StorageFailure)?;
        match status {
            ReadOperationStatus::FoundInMemory | ReadOperationStatus::FoundOnDisk => {
                let mut output = Vec::new();
                let mut upsert_info = UpsertInfo::default();
                if expiration_unix_millis.is_some() {
                    upsert_info.user_data |= UPSERT_USER_DATA_HAS_EXPIRATION;
                }
                let stored_value = encode_stored_value(&current, expiration_unix_millis);
                session
                    .upsert(&key_vec, &stored_value, &mut output, &mut upsert_info)
                    .map_err(|_| RequestExecutionError::StorageFailure)?;
                Ok(true)
            }
            ReadOperationStatus::NotFound => Ok(false),
            ReadOperationStatus::RetryLater => Err(RequestExecutionError::StorageBusy),
        }
    }

    fn expire_key_if_needed(&self, key: &[u8]) -> Result<(), RequestExecutionError> {
        let should_expire = {
            let mut expirations = self.expirations.lock().expect("expiration mutex poisoned");
            match expirations.get(key) {
                Some(deadline) if *deadline <= Instant::now() => {
                    expirations.remove(key);
                    true
                }
                _ => false,
            }
        };

        if !should_expire {
            return Ok(());
        }

        let mut store = self.store.lock().expect("store mutex poisoned");
        let mut session = store.session(&self.functions);
        let mut info = DeleteInfo::default();
        let status = session
            .delete(&key.to_vec(), &mut info)
            .map_err(|_| RequestExecutionError::StorageFailure)?;
        if matches!(
            status,
            DeleteOperationStatus::TombstonedInPlace | DeleteOperationStatus::AppendedTombstone
        ) {
            self.bump_watch_version(key);
        }
        self.key_registry
            .lock()
            .expect("key registry mutex poisoned")
            .remove(key);
        Ok(())
    }

    fn bump_watch_version(&self, key: &[u8]) {
        let slot = watch_version_slot(key);
        self.watch_versions[slot].fetch_add(1, Ordering::SeqCst);
    }
}

fn watch_version_slot(key: &[u8]) -> usize {
    // FNV-1a 64-bit hash; compact and deterministic for slot mapping.
    let mut hash = 0xcbf29ce484222325u64;
    for byte in key {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    (hash as usize) & WATCH_VERSION_MAP_MASK
}

#[derive(Debug, Clone, Copy, Default)]
struct SetOptions {
    only_if_absent: bool,
    only_if_present: bool,
    expiration: Option<ExpirationMetadata>,
}

fn parse_set_options(args: &[ArgSlice]) -> Result<SetOptions, RequestExecutionError> {
    let mut options = SetOptions::default();
    let mut index = 3usize;

    while index < args.len() {
        // SAFETY: caller guarantees argument backing memory validity.
        let option = unsafe { args[index].as_slice() };

        if ascii_eq_ignore_case(option, b"NX") {
            if options.only_if_absent || options.only_if_present {
                return Err(RequestExecutionError::SyntaxError);
            }
            options.only_if_absent = true;
            index += 1;
            continue;
        }

        if ascii_eq_ignore_case(option, b"XX") {
            if options.only_if_absent || options.only_if_present {
                return Err(RequestExecutionError::SyntaxError);
            }
            options.only_if_present = true;
            index += 1;
            continue;
        }

        if ascii_eq_ignore_case(option, b"EX") || ascii_eq_ignore_case(option, b"PX") {
            if options.expiration.is_some() {
                return Err(RequestExecutionError::SyntaxError);
            }
            if index + 1 >= args.len() {
                return Err(RequestExecutionError::SyntaxError);
            }

            // SAFETY: caller guarantees argument backing memory validity.
            let value = unsafe { args[index + 1].as_slice() };
            let amount = parse_u64_ascii(value).ok_or(RequestExecutionError::InvalidExpireTime)?;
            if amount == 0 {
                return Err(RequestExecutionError::InvalidExpireTime);
            }

            let duration = if ascii_eq_ignore_case(option, b"EX") {
                Duration::from_secs(amount)
            } else {
                Duration::from_millis(amount)
            };
            options.expiration = Some(
                expiration_metadata_from_duration(duration)
                    .ok_or(RequestExecutionError::InvalidExpireTime)?,
            );
            index += 2;
            continue;
        }

        return Err(RequestExecutionError::SyntaxError);
    }

    Ok(options)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestExecutionError {
    WrongArity {
        command: &'static str,
        expected: &'static str,
    },
    UnknownCommand,
    SyntaxError,
    InvalidExpireTime,
    WrongType,
    StorageBusy,
    StorageFailure,
    ValueNotInteger,
    ValueNotFloat,
}

impl RequestExecutionError {
    pub fn append_resp_error(self, response_out: &mut Vec<u8>) {
        match self {
            Self::WrongArity { command, .. } => append_error(
                response_out,
                &format!("ERR wrong number of arguments for '{}' command", command),
            ),
            Self::UnknownCommand => append_error(response_out, "ERR unknown command"),
            Self::SyntaxError => append_error(response_out, "ERR syntax error"),
            Self::InvalidExpireTime => {
                append_error(response_out, "ERR invalid expire time in 'set' command")
            }
            Self::WrongType => append_error(
                response_out,
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            ),
            Self::StorageBusy => append_error(response_out, "ERR storage busy, retry later"),
            Self::StorageFailure => append_error(response_out, "ERR internal storage failure"),
            Self::ValueNotInteger => {
                append_error(response_out, "ERR value is not an integer or out of range")
            }
            Self::ValueNotFloat => append_error(response_out, "ERR value is not a valid float"),
        }
    }
}

fn parse_i64_ascii(input: &[u8]) -> Option<i64> {
    if input.is_empty() {
        return None;
    }
    let text = core::str::from_utf8(input).ok()?;
    text.parse::<i64>().ok()
}

fn parse_u64_ascii(input: &[u8]) -> Option<u64> {
    if input.is_empty() {
        return None;
    }
    let text = core::str::from_utf8(input).ok()?;
    text.parse::<u64>().ok()
}

fn parse_f64_ascii(input: &[u8]) -> Option<f64> {
    if input.is_empty() {
        return None;
    }
    let text = core::str::from_utf8(input).ok()?;
    let parsed = text.parse::<f64>().ok()?;
    if !parsed.is_finite() {
        return None;
    }
    Some(parsed)
}

#[derive(Debug, Clone, Copy)]
struct DecodedStoredValue<'a> {
    expiration_unix_millis: Option<u64>,
    user_value: &'a [u8],
}

fn current_unix_time_millis() -> Option<u64> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?;
    u64::try_from(now.as_millis()).ok()
}

fn expiration_metadata_from_duration(duration: Duration) -> Option<ExpirationMetadata> {
    let deadline = Instant::now().checked_add(duration)?;
    let now_millis = u128::from(current_unix_time_millis()?);
    let expiration_millis = now_millis.checked_add(duration.as_millis())?;
    let unix_millis = u64::try_from(expiration_millis).ok()?;
    Some(ExpirationMetadata {
        deadline,
        unix_millis,
    })
}

fn decode_stored_value(stored: &[u8]) -> DecodedStoredValue<'_> {
    if stored.len() < VALUE_EXPIRATION_PREFIX_LEN {
        return DecodedStoredValue {
            expiration_unix_millis: None,
            user_value: stored,
        };
    }

    let mut metadata = [0u8; VALUE_EXPIRATION_PREFIX_LEN];
    metadata.copy_from_slice(&stored[..VALUE_EXPIRATION_PREFIX_LEN]);
    let expiration_millis = u64::from_le_bytes(metadata);
    DecodedStoredValue {
        expiration_unix_millis: if expiration_millis == 0 {
            None
        } else {
            Some(expiration_millis)
        },
        user_value: &stored[VALUE_EXPIRATION_PREFIX_LEN..],
    }
}

fn encode_stored_value(user_value: &[u8], expiration_unix_millis: Option<u64>) -> Vec<u8> {
    let mut stored = Vec::with_capacity(VALUE_EXPIRATION_PREFIX_LEN + user_value.len());
    stored.extend_from_slice(&expiration_unix_millis.unwrap_or(0).to_le_bytes());
    stored.extend_from_slice(user_value);
    stored
}

fn encode_object_value(object_type: u8, payload: &[u8]) -> Vec<u8> {
    let mut value = Vec::with_capacity(1 + payload.len());
    value.push(object_type);
    value.extend_from_slice(payload);
    value
}

fn decode_object_value(encoded: &[u8]) -> Option<(u8, Vec<u8>)> {
    let (&object_type, payload) = encoded.split_first()?;
    Some((object_type, payload.to_vec()))
}

fn serialize_hash_object_payload(hash: &BTreeMap<Vec<u8>, Vec<u8>>) -> Vec<u8> {
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&(hash.len() as u32).to_le_bytes());
    for (field, value) in hash {
        encoded.extend_from_slice(&(field.len() as u32).to_le_bytes());
        encoded.extend_from_slice(field);
        encoded.extend_from_slice(&(value.len() as u32).to_le_bytes());
        encoded.extend_from_slice(value);
    }
    encoded
}

fn deserialize_hash_object_payload(encoded: &[u8]) -> Option<BTreeMap<Vec<u8>, Vec<u8>>> {
    let mut cursor = 0usize;

    fn take_u32(encoded: &[u8], cursor: &mut usize) -> Option<u32> {
        let end = (*cursor).checked_add(size_of::<u32>())?;
        let bytes = encoded.get(*cursor..end)?;
        let mut raw = [0u8; size_of::<u32>()];
        raw.copy_from_slice(bytes);
        *cursor = end;
        Some(u32::from_le_bytes(raw))
    }

    let count = take_u32(encoded, &mut cursor)? as usize;
    let mut hash = BTreeMap::new();
    for _ in 0..count {
        let field_len = usize::try_from(take_u32(encoded, &mut cursor)?).ok()?;
        let field_end = cursor.checked_add(field_len)?;
        let field = encoded.get(cursor..field_end)?.to_vec();
        cursor = field_end;

        let value_len = usize::try_from(take_u32(encoded, &mut cursor)?).ok()?;
        let value_end = cursor.checked_add(value_len)?;
        let value = encoded.get(cursor..value_end)?.to_vec();
        cursor = value_end;

        hash.insert(field, value);
    }

    if cursor != encoded.len() {
        return None;
    }
    Some(hash)
}

fn serialize_list_object_payload(list: &[Vec<u8>]) -> Vec<u8> {
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&(list.len() as u32).to_le_bytes());
    for value in list {
        encoded.extend_from_slice(&(value.len() as u32).to_le_bytes());
        encoded.extend_from_slice(value);
    }
    encoded
}

fn deserialize_list_object_payload(encoded: &[u8]) -> Option<Vec<Vec<u8>>> {
    let mut cursor = 0usize;

    fn take_u32(encoded: &[u8], cursor: &mut usize) -> Option<u32> {
        let end = (*cursor).checked_add(size_of::<u32>())?;
        let bytes = encoded.get(*cursor..end)?;
        let mut raw = [0u8; size_of::<u32>()];
        raw.copy_from_slice(bytes);
        *cursor = end;
        Some(u32::from_le_bytes(raw))
    }

    let count = take_u32(encoded, &mut cursor)? as usize;
    let mut list = Vec::with_capacity(count);
    for _ in 0..count {
        let value_len = usize::try_from(take_u32(encoded, &mut cursor)?).ok()?;
        let value_end = cursor.checked_add(value_len)?;
        let value = encoded.get(cursor..value_end)?.to_vec();
        cursor = value_end;
        list.push(value);
    }

    if cursor != encoded.len() {
        return None;
    }
    Some(list)
}

fn serialize_set_object_payload(set: &BTreeSet<Vec<u8>>) -> Vec<u8> {
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&(set.len() as u32).to_le_bytes());
    for member in set {
        encoded.extend_from_slice(&(member.len() as u32).to_le_bytes());
        encoded.extend_from_slice(member);
    }
    encoded
}

fn deserialize_set_object_payload(encoded: &[u8]) -> Option<BTreeSet<Vec<u8>>> {
    let mut cursor = 0usize;

    fn take_u32(encoded: &[u8], cursor: &mut usize) -> Option<u32> {
        let end = (*cursor).checked_add(size_of::<u32>())?;
        let bytes = encoded.get(*cursor..end)?;
        let mut raw = [0u8; size_of::<u32>()];
        raw.copy_from_slice(bytes);
        *cursor = end;
        Some(u32::from_le_bytes(raw))
    }

    let count = take_u32(encoded, &mut cursor)? as usize;
    let mut set = BTreeSet::new();
    for _ in 0..count {
        let member_len = usize::try_from(take_u32(encoded, &mut cursor)?).ok()?;
        let member_end = cursor.checked_add(member_len)?;
        let member = encoded.get(cursor..member_end)?.to_vec();
        cursor = member_end;
        set.insert(member);
    }

    if cursor != encoded.len() {
        return None;
    }
    Some(set)
}

fn serialize_zset_object_payload(zset: &BTreeMap<Vec<u8>, f64>) -> Vec<u8> {
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&(zset.len() as u32).to_le_bytes());
    for (member, score) in zset {
        encoded.extend_from_slice(&(member.len() as u32).to_le_bytes());
        encoded.extend_from_slice(member);
        encoded.extend_from_slice(&score.to_le_bytes());
    }
    encoded
}

fn deserialize_zset_object_payload(encoded: &[u8]) -> Option<BTreeMap<Vec<u8>, f64>> {
    let mut cursor = 0usize;

    fn take_u32(encoded: &[u8], cursor: &mut usize) -> Option<u32> {
        let end = (*cursor).checked_add(size_of::<u32>())?;
        let bytes = encoded.get(*cursor..end)?;
        let mut raw = [0u8; size_of::<u32>()];
        raw.copy_from_slice(bytes);
        *cursor = end;
        Some(u32::from_le_bytes(raw))
    }

    fn take_f64(encoded: &[u8], cursor: &mut usize) -> Option<f64> {
        let end = (*cursor).checked_add(size_of::<f64>())?;
        let bytes = encoded.get(*cursor..end)?;
        let mut raw = [0u8; size_of::<f64>()];
        raw.copy_from_slice(bytes);
        let value = f64::from_le_bytes(raw);
        if !value.is_finite() {
            return None;
        }
        *cursor = end;
        Some(value)
    }

    let count = take_u32(encoded, &mut cursor)? as usize;
    let mut zset = BTreeMap::new();
    for _ in 0..count {
        let member_len = usize::try_from(take_u32(encoded, &mut cursor)?).ok()?;
        let member_end = cursor.checked_add(member_len)?;
        let member = encoded.get(cursor..member_end)?.to_vec();
        cursor = member_end;
        let score = take_f64(encoded, &mut cursor)?;
        zset.insert(member, score);
    }

    if cursor != encoded.len() {
        return None;
    }
    Some(zset)
}

fn ascii_eq_ignore_case(input: &[u8], expected_upper: &[u8]) -> bool {
    if input.len() != expected_upper.len() {
        return false;
    }
    input
        .iter()
        .zip(expected_upper.iter())
        .all(|(lhs, rhs)| lhs.to_ascii_uppercase() == *rhs)
}

fn append_simple_string(response_out: &mut Vec<u8>, value: &[u8]) {
    response_out.push(b'+');
    response_out.extend_from_slice(value);
    response_out.extend_from_slice(b"\r\n");
}

fn append_error(response_out: &mut Vec<u8>, message: &str) {
    response_out.push(b'-');
    response_out.extend_from_slice(message.as_bytes());
    response_out.extend_from_slice(b"\r\n");
}

fn append_bulk_string(response_out: &mut Vec<u8>, value: &[u8]) {
    response_out.extend_from_slice(b"$");
    response_out.extend_from_slice(value.len().to_string().as_bytes());
    response_out.extend_from_slice(b"\r\n");
    response_out.extend_from_slice(value);
    response_out.extend_from_slice(b"\r\n");
}

fn append_bulk_array(response_out: &mut Vec<u8>, items: &[&[u8]]) {
    response_out.push(b'*');
    response_out.extend_from_slice(items.len().to_string().as_bytes());
    response_out.extend_from_slice(b"\r\n");
    for item in items {
        append_bulk_string(response_out, item);
    }
}

fn append_null_bulk_string(response_out: &mut Vec<u8>) {
    response_out.extend_from_slice(b"$-1\r\n");
}

fn append_integer(response_out: &mut Vec<u8>, value: i64) {
    response_out.push(b':');
    response_out.extend_from_slice(value.to_string().as_bytes());
    response_out.extend_from_slice(b"\r\n");
}

struct KvSessionFunctions;

impl ISessionFunctions for KvSessionFunctions {
    type Key = Vec<u8>;
    type Value = Vec<u8>;
    type Input = Vec<u8>;
    type Output = Vec<u8>;
    type Context = ();
    type Reader = ();
    type Writer = ();
    type Comparer = ();

    fn single_reader(
        &self,
        _key: &Self::Key,
        _input: &Self::Input,
        value: &Self::Value,
        output: &mut Self::Output,
        _read_info: &ReadInfo,
    ) -> bool {
        let decoded = decode_stored_value(value);
        if let Some(expiration) = decoded.expiration_unix_millis {
            if let Some(now) = current_unix_time_millis() {
                if now >= expiration {
                    return false;
                }
            }
        }
        *output = decoded.user_value.to_vec();
        true
    }

    fn concurrent_reader(
        &self,
        _key: &Self::Key,
        _input: &Self::Input,
        value: &Self::Value,
        output: &mut Self::Output,
        _read_info: &ReadInfo,
        _record_info: &RecordInfo,
    ) -> bool {
        let decoded = decode_stored_value(value);
        if let Some(expiration) = decoded.expiration_unix_millis {
            if let Some(now) = current_unix_time_millis() {
                if now >= expiration {
                    return false;
                }
            }
        }
        *output = decoded.user_value.to_vec();
        true
    }

    fn single_writer(
        &self,
        _key: &Self::Key,
        input: &Self::Input,
        _src: &Self::Value,
        dst: &mut Self::Value,
        output: &mut Self::Output,
        upsert_info: &mut UpsertInfo,
        _reason: WriteReason,
        record_info: &mut RecordInfo,
    ) -> bool {
        *dst = input.clone();
        *output = dst.clone();
        if (upsert_info.user_data & UPSERT_USER_DATA_HAS_EXPIRATION) != 0 {
            record_info.set_has_expiration();
        } else {
            record_info.clear_has_expiration();
        }
        true
    }

    fn concurrent_writer(
        &self,
        _key: &Self::Key,
        input: &Self::Input,
        _src: &Self::Value,
        dst: &mut Self::Value,
        output: &mut Self::Output,
        upsert_info: &mut UpsertInfo,
        record_info: &mut RecordInfo,
    ) -> bool {
        *dst = input.clone();
        *output = dst.clone();
        if (upsert_info.user_data & UPSERT_USER_DATA_HAS_EXPIRATION) != 0 {
            record_info.set_has_expiration();
        } else {
            record_info.clear_has_expiration();
        }
        true
    }

    fn in_place_updater(
        &self,
        _key: &Self::Key,
        input: &Self::Input,
        value: &mut Self::Value,
        output: &mut Self::Output,
        _rmw_info: &mut RmwInfo,
        _record_info: &mut RecordInfo,
    ) -> bool {
        let decoded = decode_stored_value(value);
        let current = parse_i64_ascii(decoded.user_value).unwrap_or(0);
        let delta = match parse_i64_ascii(input) {
            Some(v) => v,
            None => return false,
        };
        let next = match current.checked_add(delta) {
            Some(v) => v,
            None => return false,
        };
        let next_bytes = next.to_string().into_bytes();
        *value = encode_stored_value(&next_bytes, decoded.expiration_unix_millis);
        *output = next_bytes;
        true
    }

    fn copy_updater(
        &self,
        _key: &Self::Key,
        input: &Self::Input,
        old_value: &Self::Value,
        new_value: &mut Self::Value,
        output: &mut Self::Output,
        _rmw_info: &mut RmwInfo,
        _record_info: &mut RecordInfo,
    ) -> bool {
        let decoded = decode_stored_value(old_value);
        let current = if decoded.user_value.is_empty() {
            0
        } else {
            match parse_i64_ascii(decoded.user_value) {
                Some(v) => v,
                None => return false,
            }
        };
        let delta = match parse_i64_ascii(input) {
            Some(v) => v,
            None => return false,
        };
        let next = match current.checked_add(delta) {
            Some(v) => v,
            None => return false,
        };
        let next_bytes = next.to_string().into_bytes();
        *new_value = encode_stored_value(&next_bytes, decoded.expiration_unix_millis);
        *output = next_bytes;
        true
    }

    fn single_deleter(
        &self,
        _key: &Self::Key,
        value: &mut Self::Value,
        _delete_info: &mut DeleteInfo,
        record_info: &mut RecordInfo,
    ) -> bool {
        value.clear();
        record_info.clear_has_expiration();
        record_info.set_tombstone();
        true
    }

    fn concurrent_deleter(
        &self,
        _key: &Self::Key,
        value: &mut Self::Value,
        _delete_info: &mut DeleteInfo,
        record_info: &mut RecordInfo,
    ) -> bool {
        value.clear();
        record_info.clear_has_expiration();
        record_info.set_tombstone();
        true
    }
}

impl HybridLogReadAdapter for KvSessionFunctions {
    fn record_key_equals(&self, requested_key: &Self::Key, record_key: &[u8]) -> bool {
        requested_key.as_slice() == record_key
    }

    fn value_from_record(&self, record_value: &[u8]) -> Self::Value {
        record_value.to_vec()
    }
}

impl HybridLogUpsertAdapter for KvSessionFunctions {
    fn record_key_equals(&self, requested_key: &Self::Key, record_key: &[u8]) -> bool {
        requested_key.as_slice() == record_key
    }

    fn key_to_record_bytes(&self, key: &Self::Key) -> Vec<u8> {
        key.clone()
    }

    fn value_from_record(&self, record_value: &[u8]) -> Self::Value {
        record_value.to_vec()
    }

    fn value_to_record_bytes(&self, value: &Self::Value) -> Vec<u8> {
        value.clone()
    }
}

impl HybridLogRmwAdapter for KvSessionFunctions {
    fn record_key_equals(&self, requested_key: &Self::Key, record_key: &[u8]) -> bool {
        requested_key.as_slice() == record_key
    }

    fn key_to_record_bytes(&self, key: &Self::Key) -> Vec<u8> {
        key.clone()
    }

    fn value_from_record(&self, record_value: &[u8]) -> Self::Value {
        record_value.to_vec()
    }

    fn value_to_record_bytes(&self, value: &Self::Value) -> Vec<u8> {
        value.clone()
    }
}

impl HybridLogDeleteAdapter for KvSessionFunctions {
    fn record_key_equals(&self, requested_key: &Self::Key, record_key: &[u8]) -> bool {
        requested_key.as_slice() == record_key
    }

    fn key_to_record_bytes(&self, key: &Self::Key) -> Vec<u8> {
        key.clone()
    }

    fn value_from_record(&self, record_value: &[u8]) -> Self::Value {
        record_value.to_vec()
    }

    fn value_to_record_bytes(&self, value: &Self::Value) -> Vec<u8> {
        value.clone()
    }
}

struct ObjectSessionFunctions;

impl ISessionFunctions for ObjectSessionFunctions {
    type Key = Vec<u8>;
    type Value = Vec<u8>;
    type Input = Vec<u8>;
    type Output = Vec<u8>;
    type Context = ();
    type Reader = ();
    type Writer = ();
    type Comparer = ();

    fn single_reader(
        &self,
        _key: &Self::Key,
        _input: &Self::Input,
        value: &Self::Value,
        output: &mut Self::Output,
        _read_info: &ReadInfo,
    ) -> bool {
        *output = value.clone();
        true
    }

    fn concurrent_reader(
        &self,
        _key: &Self::Key,
        _input: &Self::Input,
        value: &Self::Value,
        output: &mut Self::Output,
        _read_info: &ReadInfo,
        _record_info: &RecordInfo,
    ) -> bool {
        *output = value.clone();
        true
    }

    fn single_writer(
        &self,
        _key: &Self::Key,
        input: &Self::Input,
        _src: &Self::Value,
        dst: &mut Self::Value,
        output: &mut Self::Output,
        _upsert_info: &mut UpsertInfo,
        _reason: WriteReason,
        _record_info: &mut RecordInfo,
    ) -> bool {
        *dst = input.clone();
        *output = dst.clone();
        true
    }

    fn concurrent_writer(
        &self,
        _key: &Self::Key,
        input: &Self::Input,
        _src: &Self::Value,
        dst: &mut Self::Value,
        output: &mut Self::Output,
        _upsert_info: &mut UpsertInfo,
        _record_info: &mut RecordInfo,
    ) -> bool {
        *dst = input.clone();
        *output = dst.clone();
        true
    }

    fn in_place_updater(
        &self,
        _key: &Self::Key,
        input: &Self::Input,
        value: &mut Self::Value,
        output: &mut Self::Output,
        _rmw_info: &mut RmwInfo,
        _record_info: &mut RecordInfo,
    ) -> bool {
        *value = input.clone();
        *output = value.clone();
        true
    }

    fn copy_updater(
        &self,
        _key: &Self::Key,
        input: &Self::Input,
        _old_value: &Self::Value,
        new_value: &mut Self::Value,
        output: &mut Self::Output,
        _rmw_info: &mut RmwInfo,
        _record_info: &mut RecordInfo,
    ) -> bool {
        *new_value = input.clone();
        *output = new_value.clone();
        true
    }

    fn single_deleter(
        &self,
        _key: &Self::Key,
        value: &mut Self::Value,
        _delete_info: &mut DeleteInfo,
        record_info: &mut RecordInfo,
    ) -> bool {
        value.clear();
        record_info.set_tombstone();
        true
    }

    fn concurrent_deleter(
        &self,
        _key: &Self::Key,
        value: &mut Self::Value,
        _delete_info: &mut DeleteInfo,
        record_info: &mut RecordInfo,
    ) -> bool {
        value.clear();
        record_info.set_tombstone();
        true
    }
}

impl HybridLogReadAdapter for ObjectSessionFunctions {
    fn record_key_equals(&self, requested_key: &Self::Key, record_key: &[u8]) -> bool {
        requested_key.as_slice() == record_key
    }

    fn value_from_record(&self, record_value: &[u8]) -> Self::Value {
        record_value.to_vec()
    }
}

impl HybridLogUpsertAdapter for ObjectSessionFunctions {
    fn record_key_equals(&self, requested_key: &Self::Key, record_key: &[u8]) -> bool {
        requested_key.as_slice() == record_key
    }

    fn key_to_record_bytes(&self, key: &Self::Key) -> Vec<u8> {
        key.clone()
    }

    fn value_from_record(&self, record_value: &[u8]) -> Self::Value {
        record_value.to_vec()
    }

    fn value_to_record_bytes(&self, value: &Self::Value) -> Vec<u8> {
        value.clone()
    }
}

impl HybridLogRmwAdapter for ObjectSessionFunctions {
    fn record_key_equals(&self, requested_key: &Self::Key, record_key: &[u8]) -> bool {
        requested_key.as_slice() == record_key
    }

    fn key_to_record_bytes(&self, key: &Self::Key) -> Vec<u8> {
        key.clone()
    }

    fn value_from_record(&self, record_value: &[u8]) -> Self::Value {
        record_value.to_vec()
    }

    fn value_to_record_bytes(&self, value: &Self::Value) -> Vec<u8> {
        value.clone()
    }
}

impl HybridLogDeleteAdapter for ObjectSessionFunctions {
    fn record_key_equals(&self, requested_key: &Self::Key, record_key: &[u8]) -> bool {
        requested_key.as_slice() == record_key
    }

    fn key_to_record_bytes(&self, key: &Self::Key) -> Vec<u8> {
        key.clone()
    }

    fn value_from_record(&self, record_value: &[u8]) -> Self::Value {
        record_value.to_vec()
    }

    fn value_to_record_bytes(&self, value: &Self::Value) -> Vec<u8> {
        value.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use garnet_common::parse_resp_command_arg_slices;
    use std::thread;
    use std::time::Duration;

    fn parse_integer_response(response: &[u8]) -> i64 {
        assert!(response.len() >= 4);
        assert_eq!(response[0], b':');
        assert!(response.ends_with(b"\r\n"));
        core::str::from_utf8(&response[1..response.len() - 2])
            .unwrap()
            .parse::<i64>()
            .unwrap()
    }

    #[test]
    fn writers_toggle_record_has_expiration_from_upsert_user_data() {
        let functions = KvSessionFunctions;
        let key = b"key".to_vec();
        let input = b"value".to_vec();
        let src = Vec::new();
        let mut dst = Vec::new();
        let mut output = Vec::new();
        let mut info = UpsertInfo::default();
        let mut record_info = RecordInfo::default();

        info.user_data = UPSERT_USER_DATA_HAS_EXPIRATION;
        assert!(functions.single_writer(
            &key,
            &input,
            &src,
            &mut dst,
            &mut output,
            &mut info,
            WriteReason::Insert,
            &mut record_info
        ));
        assert!(record_info.has_expiration());

        info.user_data = 0;
        assert!(functions.concurrent_writer(
            &key,
            &input,
            &src,
            &mut dst,
            &mut output,
            &mut info,
            &mut record_info
        ));
        assert!(!record_info.has_expiration());
    }

    #[test]
    fn executes_set_then_get_roundtrip() {
        let processor = RequestProcessor::new().unwrap();
        let mut args = [ArgSlice::EMPTY; 3];
        let frame_set = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        let meta = parse_resp_command_arg_slices(frame_set, &mut args).unwrap();
        let mut response = Vec::new();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"+OK\r\n");

        let frame_get = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
        let meta = parse_resp_command_arg_slices(frame_get, &mut args).unwrap();
        response.clear();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"$5\r\nvalue\r\n");
    }

    #[test]
    fn object_store_roundtrip_is_isolated_from_main_store() {
        let processor = RequestProcessor::new().unwrap();

        processor.object_upsert(b"obj", 3, b"payload").unwrap();
        let (object_type, payload) = processor.object_read(b"obj").unwrap().unwrap();
        assert_eq!(object_type, 3);
        assert_eq!(payload, b"payload");

        let mut args = [ArgSlice::EMPTY; 3];
        let mut response = Vec::new();
        let get = b"*2\r\n$3\r\nGET\r\n$3\r\nobj\r\n";
        let meta = parse_resp_command_arg_slices(get, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"$-1\r\n");

        response.clear();
        let set = b"*3\r\n$3\r\nSET\r\n$3\r\nobj\r\n$3\r\nstr\r\n";
        let meta = parse_resp_command_arg_slices(set, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"+OK\r\n");

        let (object_type, payload) = processor.object_read(b"obj").unwrap().unwrap();
        assert_eq!(object_type, 3);
        assert_eq!(payload, b"payload");

        assert!(processor.object_delete(b"obj").unwrap());
        assert!(processor.object_read(b"obj").unwrap().is_none());

        response.clear();
        let meta = parse_resp_command_arg_slices(get, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"$3\r\nstr\r\n");
    }

    #[test]
    fn hash_commands_roundtrip_over_object_store() {
        let processor = RequestProcessor::new().unwrap();
        let mut args = [ArgSlice::EMPTY; 16];
        let mut response = Vec::new();

        let hset1 = b"*4\r\n$4\r\nHSET\r\n$3\r\nkey\r\n$6\r\nfield1\r\n$2\r\nv1\r\n";
        let meta = parse_resp_command_arg_slices(hset1, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":1\r\n");

        response.clear();
        let hset2 = b"*4\r\n$4\r\nHSET\r\n$3\r\nkey\r\n$6\r\nfield1\r\n$2\r\nv2\r\n";
        let meta = parse_resp_command_arg_slices(hset2, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":0\r\n");

        response.clear();
        let hget = b"*3\r\n$4\r\nHGET\r\n$3\r\nkey\r\n$6\r\nfield1\r\n";
        let meta = parse_resp_command_arg_slices(hget, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"$2\r\nv2\r\n");

        response.clear();
        let hset3 = b"*4\r\n$4\r\nHSET\r\n$3\r\nkey\r\n$6\r\nfield2\r\n$2\r\nv3\r\n";
        let meta = parse_resp_command_arg_slices(hset3, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":1\r\n");

        response.clear();
        let hgetall = b"*2\r\n$7\r\nHGETALL\r\n$3\r\nkey\r\n";
        let meta = parse_resp_command_arg_slices(hgetall, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(
            response,
            b"*4\r\n$6\r\nfield1\r\n$2\r\nv2\r\n$6\r\nfield2\r\n$2\r\nv3\r\n"
        );

        response.clear();
        let hdel = b"*4\r\n$4\r\nHDEL\r\n$3\r\nkey\r\n$6\r\nfield1\r\n$6\r\nfield9\r\n";
        let meta = parse_resp_command_arg_slices(hdel, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":1\r\n");

        response.clear();
        let hdel_last = b"*3\r\n$4\r\nHDEL\r\n$3\r\nkey\r\n$6\r\nfield2\r\n";
        let meta = parse_resp_command_arg_slices(hdel_last, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":1\r\n");

        response.clear();
        let meta = parse_resp_command_arg_slices(hgetall, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"*0\r\n");
    }

    #[test]
    fn hash_commands_return_wrongtype_for_string_keys() {
        let processor = RequestProcessor::new().unwrap();
        let mut args = [ArgSlice::EMPTY; 8];
        let mut response = Vec::new();

        let set = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        let meta = parse_resp_command_arg_slices(set, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"+OK\r\n");

        response.clear();
        let hget = b"*3\r\n$4\r\nHGET\r\n$3\r\nkey\r\n$5\r\nfield\r\n";
        let meta = parse_resp_command_arg_slices(hget, &mut args).unwrap();
        let err = processor
            .execute(&args[..meta.argument_count], &mut response)
            .err()
            .unwrap();
        err.append_resp_error(&mut response);
        assert_eq!(
            response,
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        );
    }

    #[test]
    fn list_commands_roundtrip_over_object_store() {
        let processor = RequestProcessor::new().unwrap();
        let mut args = [ArgSlice::EMPTY; 16];
        let mut response = Vec::new();

        let lpush = b"*4\r\n$5\r\nLPUSH\r\n$3\r\nkey\r\n$1\r\na\r\n$1\r\nb\r\n";
        let meta = parse_resp_command_arg_slices(lpush, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":2\r\n");

        response.clear();
        let rpush = b"*3\r\n$5\r\nRPUSH\r\n$3\r\nkey\r\n$1\r\nc\r\n";
        let meta = parse_resp_command_arg_slices(rpush, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":3\r\n");

        response.clear();
        let lrange_all = b"*4\r\n$6\r\nLRANGE\r\n$3\r\nkey\r\n$1\r\n0\r\n$2\r\n-1\r\n";
        let meta = parse_resp_command_arg_slices(lrange_all, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"*3\r\n$1\r\nb\r\n$1\r\na\r\n$1\r\nc\r\n");

        response.clear();
        let lpop = b"*2\r\n$4\r\nLPOP\r\n$3\r\nkey\r\n";
        let meta = parse_resp_command_arg_slices(lpop, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"$1\r\nb\r\n");

        response.clear();
        let rpop = b"*2\r\n$4\r\nRPOP\r\n$3\r\nkey\r\n";
        let meta = parse_resp_command_arg_slices(rpop, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"$1\r\nc\r\n");

        response.clear();
        let meta = parse_resp_command_arg_slices(lpop, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"$1\r\na\r\n");

        response.clear();
        let meta = parse_resp_command_arg_slices(lpop, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"$-1\r\n");
    }

    #[test]
    fn lrange_supports_negative_indexes() {
        let processor = RequestProcessor::new().unwrap();
        let mut args = [ArgSlice::EMPTY; 16];
        let mut response = Vec::new();

        let rpush =
            b"*6\r\n$5\r\nRPUSH\r\n$3\r\nkey\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n";
        let meta = parse_resp_command_arg_slices(rpush, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":4\r\n");

        response.clear();
        let lrange = b"*4\r\n$6\r\nLRANGE\r\n$3\r\nkey\r\n$2\r\n-3\r\n$2\r\n-2\r\n";
        let meta = parse_resp_command_arg_slices(lrange, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"*2\r\n$1\r\nb\r\n$1\r\nc\r\n");
    }

    #[test]
    fn list_commands_return_wrongtype_for_string_keys() {
        let processor = RequestProcessor::new().unwrap();
        let mut args = [ArgSlice::EMPTY; 8];
        let mut response = Vec::new();

        let set = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        let meta = parse_resp_command_arg_slices(set, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"+OK\r\n");

        response.clear();
        let lpush = b"*3\r\n$5\r\nLPUSH\r\n$3\r\nkey\r\n$1\r\nv\r\n";
        let meta = parse_resp_command_arg_slices(lpush, &mut args).unwrap();
        let err = processor
            .execute(&args[..meta.argument_count], &mut response)
            .err()
            .unwrap();
        err.append_resp_error(&mut response);
        assert_eq!(
            response,
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        );
    }

    #[test]
    fn set_commands_roundtrip_over_object_store() {
        let processor = RequestProcessor::new().unwrap();
        let mut args = [ArgSlice::EMPTY; 16];
        let mut response = Vec::new();

        let sadd = b"*5\r\n$4\r\nSADD\r\n$3\r\nkey\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nb\r\n";
        let meta = parse_resp_command_arg_slices(sadd, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":2\r\n");

        response.clear();
        let sismember_yes = b"*3\r\n$9\r\nSISMEMBER\r\n$3\r\nkey\r\n$1\r\nb\r\n";
        let meta = parse_resp_command_arg_slices(sismember_yes, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":1\r\n");

        response.clear();
        let sismember_no = b"*3\r\n$9\r\nSISMEMBER\r\n$3\r\nkey\r\n$1\r\nz\r\n";
        let meta = parse_resp_command_arg_slices(sismember_no, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":0\r\n");

        response.clear();
        let smembers = b"*2\r\n$8\r\nSMEMBERS\r\n$3\r\nkey\r\n";
        let meta = parse_resp_command_arg_slices(smembers, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"*2\r\n$1\r\na\r\n$1\r\nb\r\n");

        response.clear();
        let srem = b"*4\r\n$4\r\nSREM\r\n$3\r\nkey\r\n$1\r\na\r\n$1\r\nx\r\n";
        let meta = parse_resp_command_arg_slices(srem, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":1\r\n");

        response.clear();
        let meta = parse_resp_command_arg_slices(smembers, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"*1\r\n$1\r\nb\r\n");

        response.clear();
        let srem_last = b"*3\r\n$4\r\nSREM\r\n$3\r\nkey\r\n$1\r\nb\r\n";
        let meta = parse_resp_command_arg_slices(srem_last, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":1\r\n");

        response.clear();
        let meta = parse_resp_command_arg_slices(smembers, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"*0\r\n");
    }

    #[test]
    fn set_commands_return_wrongtype_for_string_keys() {
        let processor = RequestProcessor::new().unwrap();
        let mut args = [ArgSlice::EMPTY; 8];
        let mut response = Vec::new();

        let set = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        let meta = parse_resp_command_arg_slices(set, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"+OK\r\n");

        response.clear();
        let sadd = b"*3\r\n$4\r\nSADD\r\n$3\r\nkey\r\n$1\r\nv\r\n";
        let meta = parse_resp_command_arg_slices(sadd, &mut args).unwrap();
        let err = processor
            .execute(&args[..meta.argument_count], &mut response)
            .err()
            .unwrap();
        err.append_resp_error(&mut response);
        assert_eq!(
            response,
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        );
    }

    #[test]
    fn zset_commands_roundtrip_over_object_store() {
        let processor = RequestProcessor::new().unwrap();
        let mut args = [ArgSlice::EMPTY; 20];
        let mut response = Vec::new();

        let zadd =
            b"*6\r\n$4\r\nZADD\r\n$3\r\nkey\r\n$1\r\n2\r\n$3\r\ntwo\r\n$1\r\n1\r\n$3\r\none\r\n";
        let meta = parse_resp_command_arg_slices(zadd, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":2\r\n");

        response.clear();
        let zscore = b"*3\r\n$6\r\nZSCORE\r\n$3\r\nkey\r\n$3\r\none\r\n";
        let meta = parse_resp_command_arg_slices(zscore, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"$1\r\n1\r\n");

        response.clear();
        let zrange = b"*4\r\n$6\r\nZRANGE\r\n$3\r\nkey\r\n$1\r\n0\r\n$2\r\n-1\r\n";
        let meta = parse_resp_command_arg_slices(zrange, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"*2\r\n$3\r\none\r\n$3\r\ntwo\r\n");

        response.clear();
        let zadd_update = b"*4\r\n$4\r\nZADD\r\n$3\r\nkey\r\n$1\r\n3\r\n$3\r\none\r\n";
        let meta = parse_resp_command_arg_slices(zadd_update, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":0\r\n");

        response.clear();
        let zrange_after_update = b"*4\r\n$6\r\nZRANGE\r\n$3\r\nkey\r\n$1\r\n0\r\n$1\r\n1\r\n";
        let meta = parse_resp_command_arg_slices(zrange_after_update, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"*2\r\n$3\r\ntwo\r\n$3\r\none\r\n");

        response.clear();
        let zrem = b"*4\r\n$4\r\nZREM\r\n$3\r\nkey\r\n$3\r\none\r\n$4\r\nnone\r\n";
        let meta = parse_resp_command_arg_slices(zrem, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":1\r\n");

        response.clear();
        let zrem_last = b"*3\r\n$4\r\nZREM\r\n$3\r\nkey\r\n$3\r\ntwo\r\n";
        let meta = parse_resp_command_arg_slices(zrem_last, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":1\r\n");

        response.clear();
        let meta = parse_resp_command_arg_slices(zrange, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"*0\r\n");
    }

    #[test]
    fn zset_commands_return_wrongtype_for_string_keys() {
        let processor = RequestProcessor::new().unwrap();
        let mut args = [ArgSlice::EMPTY; 8];
        let mut response = Vec::new();

        let set = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        let meta = parse_resp_command_arg_slices(set, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"+OK\r\n");

        response.clear();
        let zadd = b"*4\r\n$4\r\nZADD\r\n$3\r\nkey\r\n$1\r\n1\r\n$1\r\nv\r\n";
        let meta = parse_resp_command_arg_slices(zadd, &mut args).unwrap();
        let err = processor
            .execute(&args[..meta.argument_count], &mut response)
            .err()
            .unwrap();
        err.append_resp_error(&mut response);
        assert_eq!(
            response,
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        );
    }

    #[test]
    fn executes_incr_command() {
        let processor = RequestProcessor::new().unwrap();
        let mut args = [ArgSlice::EMPTY; 2];
        let frame = b"*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n";
        let meta = parse_resp_command_arg_slices(frame, &mut args).unwrap();

        let mut response = Vec::new();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":1\r\n");

        response.clear();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":2\r\n");
    }

    #[test]
    fn executes_del_command() {
        let processor = RequestProcessor::new().unwrap();
        let mut args = [ArgSlice::EMPTY; 3];
        let set_frame = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        let set_meta = parse_resp_command_arg_slices(set_frame, &mut args).unwrap();
        let mut response = Vec::new();
        processor
            .execute(&args[..set_meta.argument_count], &mut response)
            .unwrap();

        let del_frame = b"*2\r\n$3\r\nDEL\r\n$3\r\nkey\r\n";
        let del_meta = parse_resp_command_arg_slices(del_frame, &mut args).unwrap();
        response.clear();
        processor
            .execute(&args[..del_meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":1\r\n");
    }

    #[test]
    fn key_can_be_recreated_after_delete() {
        let processor = RequestProcessor::new().unwrap();
        let mut args = [ArgSlice::EMPTY; 3];
        let mut response = Vec::new();

        let set_first = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        let meta = parse_resp_command_arg_slices(set_first, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"+OK\r\n");

        response.clear();
        let del = b"*2\r\n$3\r\nDEL\r\n$3\r\nkey\r\n";
        let meta = parse_resp_command_arg_slices(del, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":1\r\n");

        response.clear();
        let set_second = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$7\r\nupdated\r\n";
        let meta = parse_resp_command_arg_slices(set_second, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"+OK\r\n");

        response.clear();
        let get = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
        let meta = parse_resp_command_arg_slices(get, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"$7\r\nupdated\r\n");
    }

    #[test]
    fn set_supports_nx_and_xx_conditions() {
        let processor = RequestProcessor::new().unwrap();
        let mut args = [ArgSlice::EMPTY; 6];
        let mut response = Vec::new();

        let set_nx = b"*4\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\nNX\r\n";
        let meta = parse_resp_command_arg_slices(set_nx, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"+OK\r\n");

        response.clear();
        let set_nx_again = b"*4\r\n$3\r\nSET\r\n$3\r\nkey\r\n$6\r\nvalue2\r\n$2\r\nNX\r\n";
        let meta = parse_resp_command_arg_slices(set_nx_again, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"$-1\r\n");

        response.clear();
        let set_xx = b"*4\r\n$3\r\nSET\r\n$3\r\nkey\r\n$7\r\nupdated\r\n$2\r\nXX\r\n";
        let meta = parse_resp_command_arg_slices(set_xx, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"+OK\r\n");
    }

    #[test]
    fn set_with_px_expires_key() {
        let processor = RequestProcessor::new().unwrap();
        let mut args = [ArgSlice::EMPTY; 8];
        let mut response = Vec::new();

        let set_px = b"*5\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\nPX\r\n$2\r\n10\r\n";
        let meta = parse_resp_command_arg_slices(set_px, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"+OK\r\n");

        thread::sleep(Duration::from_millis(20));

        response.clear();
        let get = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
        let meta = parse_resp_command_arg_slices(get, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"$-1\r\n");
    }

    #[test]
    fn expiration_scan_removes_expired_keys_in_background_style() {
        let processor = RequestProcessor::new().unwrap();
        let mut args = [ArgSlice::EMPTY; 8];
        let mut response = Vec::new();

        let set_px = b"*5\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\nPX\r\n$2\r\n10\r\n";
        let meta = parse_resp_command_arg_slices(set_px, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"+OK\r\n");

        thread::sleep(Duration::from_millis(20));
        let removed = processor.expire_stale_keys(16).unwrap();
        assert_eq!(removed, 1);

        response.clear();
        let get = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
        let meta = parse_resp_command_arg_slices(get, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"$-1\r\n");
    }

    #[test]
    fn set_returns_error_for_invalid_expire_time() {
        let processor = RequestProcessor::new().unwrap();
        let mut args = [ArgSlice::EMPTY; 8];
        let mut response = Vec::new();

        let invalid = b"*5\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\nPX\r\n$1\r\n0\r\n";
        let meta = parse_resp_command_arg_slices(invalid, &mut args).unwrap();
        let err = processor
            .execute(&args[..meta.argument_count], &mut response)
            .err()
            .unwrap();
        err.append_resp_error(&mut response);
        assert_eq!(response, b"-ERR invalid expire time in 'set' command\r\n");
    }

    #[test]
    fn expire_ttl_and_pexpire_commands_work() {
        let processor = RequestProcessor::new().unwrap();
        let mut args = [ArgSlice::EMPTY; 8];
        let mut response = Vec::new();

        let set = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        let meta = parse_resp_command_arg_slices(set, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"+OK\r\n");

        response.clear();
        let ttl = b"*2\r\n$3\r\nTTL\r\n$3\r\nkey\r\n";
        let meta = parse_resp_command_arg_slices(ttl, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":-1\r\n");

        response.clear();
        let expire = b"*3\r\n$6\r\nEXPIRE\r\n$3\r\nkey\r\n$1\r\n1\r\n";
        let meta = parse_resp_command_arg_slices(expire, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":1\r\n");

        response.clear();
        let pttl = b"*2\r\n$4\r\nPTTL\r\n$3\r\nkey\r\n";
        let meta = parse_resp_command_arg_slices(pttl, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        let remaining = parse_integer_response(&response);
        assert!((0..=1000).contains(&remaining));

        response.clear();
        let pexpire_now = b"*3\r\n$7\r\nPEXPIRE\r\n$3\r\nkey\r\n$1\r\n0\r\n";
        let meta = parse_resp_command_arg_slices(pexpire_now, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":1\r\n");

        response.clear();
        let get = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
        let meta = parse_resp_command_arg_slices(get, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"$-1\r\n");

        response.clear();
        let ttl_after_delete = b"*2\r\n$3\r\nTTL\r\n$3\r\nkey\r\n";
        let meta = parse_resp_command_arg_slices(ttl_after_delete, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":-2\r\n");
    }

    #[test]
    fn expire_and_ttl_on_missing_key_follow_redis_codes() {
        let processor = RequestProcessor::new().unwrap();
        let mut args = [ArgSlice::EMPTY; 8];
        let mut response = Vec::new();

        let expire = b"*3\r\n$6\r\nEXPIRE\r\n$7\r\nmissing\r\n$2\r\n10\r\n";
        let meta = parse_resp_command_arg_slices(expire, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":0\r\n");

        response.clear();
        let ttl = b"*2\r\n$3\r\nTTL\r\n$7\r\nmissing\r\n";
        let meta = parse_resp_command_arg_slices(ttl, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":-2\r\n");

        response.clear();
        let pexpire = b"*3\r\n$7\r\nPEXPIRE\r\n$7\r\nmissing\r\n$2\r\n10\r\n";
        let meta = parse_resp_command_arg_slices(pexpire, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":0\r\n");

        response.clear();
        let pttl = b"*2\r\n$4\r\nPTTL\r\n$7\r\nmissing\r\n";
        let meta = parse_resp_command_arg_slices(pttl, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":-2\r\n");
    }

    #[test]
    fn expire_with_invalid_timeout_returns_integer_error() {
        let processor = RequestProcessor::new().unwrap();
        let mut args = [ArgSlice::EMPTY; 8];
        let mut response = Vec::new();

        let invalid = b"*3\r\n$6\r\nEXPIRE\r\n$7\r\nmissing\r\n$3\r\nbad\r\n";
        let meta = parse_resp_command_arg_slices(invalid, &mut args).unwrap();
        let err = processor
            .execute(&args[..meta.argument_count], &mut response)
            .err()
            .unwrap();
        err.append_resp_error(&mut response);
        assert_eq!(
            response,
            b"-ERR value is not an integer or out of range\r\n"
        );
    }

    #[test]
    fn persist_removes_existing_expiration() {
        let processor = RequestProcessor::new().unwrap();
        let mut args = [ArgSlice::EMPTY; 8];
        let mut response = Vec::new();

        let set_px = b"*5\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\nPX\r\n$4\r\n1000\r\n";
        let meta = parse_resp_command_arg_slices(set_px, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"+OK\r\n");

        response.clear();
        let persist = b"*2\r\n$7\r\nPERSIST\r\n$3\r\nkey\r\n";
        let meta = parse_resp_command_arg_slices(persist, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":1\r\n");

        response.clear();
        let ttl = b"*2\r\n$3\r\nTTL\r\n$3\r\nkey\r\n";
        let meta = parse_resp_command_arg_slices(ttl, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":-1\r\n");

        response.clear();
        let meta = parse_resp_command_arg_slices(persist, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":0\r\n");

        response.clear();
        let persist_missing = b"*2\r\n$7\r\nPERSIST\r\n$7\r\nmissing\r\n";
        let meta = parse_resp_command_arg_slices(persist_missing, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":0\r\n");
    }

    #[test]
    fn info_dbsize_and_command_responses_are_generated() {
        let processor = RequestProcessor::new().unwrap();
        let mut args = [ArgSlice::EMPTY; 8];
        let mut response = Vec::new();

        let set = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        let meta = parse_resp_command_arg_slices(set, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();

        response.clear();
        let dbsize = b"*1\r\n$6\r\nDBSIZE\r\n";
        let meta = parse_resp_command_arg_slices(dbsize, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b":1\r\n");

        response.clear();
        let info = b"*1\r\n$4\r\nINFO\r\n";
        let meta = parse_resp_command_arg_slices(info, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert!(response.starts_with(b"$"));
        assert!(response.windows("dbsize:1".len()).any(|w| w == b"dbsize:1"));

        response.clear();
        let command = b"*1\r\n$7\r\nCOMMAND\r\n";
        let meta = parse_resp_command_arg_slices(command, &mut args).unwrap();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        assert!(response.starts_with(b"*"));
        assert!(response.windows(7).any(|w| w == b"$3\r\nGET"));
        assert!(response.windows(10).any(|w| w == b"$6\r\nEXPIRE"));
    }
}
