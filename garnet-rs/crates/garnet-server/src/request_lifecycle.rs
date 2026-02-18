//! Request lifecycle: parse result -> dispatch -> storage op -> RESP response.

use crate::{dispatch_from_arg_slices, CommandId};
use garnet_common::ArgSlice;
use std::sync::Mutex;
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
    functions: KvSessionFunctions,
}

impl RequestProcessor {
    pub fn new() -> Result<Self, RequestProcessorInitError> {
        Ok(Self {
            store: Mutex::new(TsavoriteKV::new(TsavoriteKvConfig::default())?),
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
            CommandId::Ping => self.handle_ping(args, response_out),
            CommandId::Echo => self.handle_echo(args, response_out),
            _ => Err(RequestExecutionError::UnknownCommand),
        }
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

        let mut store = self.store.lock().expect("store mutex poisoned");
        let mut session = store.session(&self.functions);
        let mut output = Vec::new();
        let mut info = UpsertInfo::default();
        session
            .upsert(&key, &value, &mut output, &mut info)
            .map_err(|_| RequestExecutionError::StorageFailure)?;

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

        let mut deleted = 0i64;
        let mut store = self.store.lock().expect("store mutex poisoned");
        let mut session = store.session(&self.functions);
        for arg in &args[1..] {
            // SAFETY: caller guarantees argument backing memory validity.
            let key = unsafe { arg.as_slice() }.to_vec();
            let mut info = DeleteInfo::default();
            let status = session
                .delete(&key, &mut info)
                .map_err(|_| RequestExecutionError::StorageFailure)?;
            match status {
                DeleteOperationStatus::TombstonedInPlace
                | DeleteOperationStatus::AppendedTombstone => deleted += 1,
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
                append_integer(response_out, delta);
                Ok(())
            }
            Err(RmwOperationError::OperationCancelled) => {
                Err(RequestExecutionError::ValueNotInteger)
            }
            Err(_) => Err(RequestExecutionError::StorageFailure),
        }
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestExecutionError {
    WrongArity {
        command: &'static str,
        expected: &'static str,
    },
    UnknownCommand,
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
}
