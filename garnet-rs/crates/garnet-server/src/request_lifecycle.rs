//! Request lifecycle: parse result -> dispatch -> storage op -> RESP response.

use crate::{dispatch_from_arg_slices, CommandId};
use garnet_common::ArgSlice;
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tsavorite::{
    DeleteInfo, DeleteOperationStatus, HybridLogDeleteAdapter, HybridLogReadAdapter,
    HybridLogRmwAdapter, HybridLogUpsertAdapter, ISessionFunctions, ReadInfo, ReadOperationStatus,
    RecordInfo, RmwInfo, RmwOperationError, RmwOperationStatus, TsavoriteKV, TsavoriteKvConfig,
    TsavoriteKvInitError, UpsertInfo, WriteReason,
};

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
    expirations: Mutex<HashMap<Vec<u8>, Instant>>,
    key_registry: Mutex<HashSet<Vec<u8>>>,
    functions: KvSessionFunctions,
}

impl RequestProcessor {
    pub fn new() -> Result<Self, RequestProcessorInitError> {
        Ok(Self {
            store: Mutex::new(TsavoriteKV::new(TsavoriteKvConfig::default())?),
            expirations: Mutex::new(HashMap::new()),
            key_registry: Mutex::new(HashSet::new()),
            functions: KvSessionFunctions,
        })
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
            CommandId::Ping => self.handle_ping(args, response_out),
            CommandId::Echo => self.handle_echo(args, response_out),
            CommandId::Info => self.handle_info(args, response_out),
            CommandId::Dbsize => self.handle_dbsize(args, response_out),
            CommandId::Command => self.handle_command(args, response_out),
            _ => Err(RequestExecutionError::UnknownCommand),
        }
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
        session
            .upsert(&key, &value, &mut output, &mut info)
            .map_err(|_| RequestExecutionError::StorageFailure)?;
        drop(session);
        drop(store);

        let mut expirations = self.expirations.lock().expect("expiration mutex poisoned");
        match options.expiration {
            Some(deadline) => {
                expirations.insert(key.clone(), deadline);
            }
            None => {
                expirations.remove(&key);
            }
        }
        self.key_registry
            .lock()
            .expect("key registry mutex poisoned")
            .insert(key);

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
                append_integer(response_out, parsed);
                Ok(())
            }
            Ok(RmwOperationStatus::RetryLater) => Err(RequestExecutionError::StorageBusy),
            Ok(RmwOperationStatus::NotFound) => {
                let mut upsert_info = UpsertInfo::default();
                let mut upsert_output = Vec::new();
                session
                    .upsert(&key, &input, &mut upsert_output, &mut upsert_info)
                    .map_err(|_| RequestExecutionError::StorageFailure)?;
                self.key_registry
                    .lock()
                    .expect("key registry mutex poisoned")
                    .insert(key.clone());
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
        let deadline = Instant::now()
            .checked_add(duration)
            .ok_or(RequestExecutionError::ValueNotInteger)?;
        self.expirations
            .lock()
            .expect("expiration mutex poisoned")
            .insert(key, deadline);
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

        let removed = self
            .expirations
            .lock()
            .expect("expiration mutex poisoned")
            .remove(&key)
            .is_some();
        append_integer(response_out, if removed { 1 } else { 0 });
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
                b"GET", b"SET", b"DEL", b"INCR", b"DECR", b"EXPIRE", b"TTL", b"PEXPIRE", b"PTTL",
                b"PERSIST", b"PING", b"ECHO", b"INFO", b"DBSIZE", b"COMMAND",
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
        session
            .delete(&key.to_vec(), &mut info)
            .map_err(|_| RequestExecutionError::StorageFailure)?;
        self.key_registry
            .lock()
            .expect("key registry mutex poisoned")
            .remove(key);
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct SetOptions {
    only_if_absent: bool,
    only_if_present: bool,
    expiration: Option<Instant>,
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
                Instant::now()
                    .checked_add(duration)
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
    StorageBusy,
    StorageFailure,
    ValueNotInteger,
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
            Self::StorageBusy => append_error(response_out, "ERR storage busy, retry later"),
            Self::StorageFailure => append_error(response_out, "ERR internal storage failure"),
            Self::ValueNotInteger => {
                append_error(response_out, "ERR value is not an integer or out of range")
            }
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
        let current = parse_i64_ascii(value).unwrap_or(0);
        let delta = match parse_i64_ascii(input) {
            Some(v) => v,
            None => return false,
        };
        let next = match current.checked_add(delta) {
            Some(v) => v,
            None => return false,
        };
        *value = next.to_string().into_bytes();
        *output = value.clone();
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
        let current = if old_value.is_empty() {
            0
        } else {
            match parse_i64_ascii(old_value) {
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
        *new_value = next.to_string().into_bytes();
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
