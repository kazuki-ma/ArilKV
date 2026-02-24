use super::*;
use crate::command_spec::command_is_mutating;
use crate::{dispatch_command_name, CommandId};
use mlua::{Error as LuaError, Lua, MultiValue, Table as LuaTable, Value as LuaValue};
use sha1::{Digest, Sha1};
use std::fmt::Write;

const SCRIPT_EVAL_USAGE: &str = "EVAL script numkeys [key ...] [arg ...]";
const SCRIPT_EVAL_RO_USAGE: &str = "EVAL_RO script numkeys [key ...] [arg ...]";
const SCRIPT_EVALSHA_USAGE: &str = "EVALSHA sha1 numkeys [key ...] [arg ...]";
const SCRIPT_EVALSHA_RO_USAGE: &str = "EVALSHA_RO sha1 numkeys [key ...] [arg ...]";
const SCRIPT_FCALL_USAGE: &str = "FCALL function numkeys [key ...] [arg ...]";
const SCRIPT_FCALL_RO_USAGE: &str = "FCALL_RO function numkeys [key ...] [arg ...]";
const SCRIPT_FUNCTION_USAGE: &str = "FUNCTION FLUSH";
const SCRIPT_FLUSH_USAGE: &str = "SCRIPT FLUSH [ASYNC|SYNC]";
const SCRIPT_LOAD_USAGE: &str = "SCRIPT LOAD script";
const SCRIPT_EXISTS_USAGE: &str = "SCRIPT EXISTS sha1 [sha1 ...]";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScriptMutability {
    ReadWrite,
    ReadOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LuaCallMode {
    Call,
    PCall,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RespFrame {
    SimpleString(Vec<u8>),
    Error(String),
    Integer(i64),
    Bulk(Option<Vec<u8>>),
    Array(Option<Vec<RespFrame>>),
    Null,
}

impl RequestProcessor {
    pub(super) fn handle_function(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 2, "FUNCTION", SCRIPT_FUNCTION_USAGE)?;
        let subcommand = args[1];
        if !ascii_eq_ignore_case(subcommand, b"FLUSH") {
            return Err(RequestExecutionError::UnknownCommand);
        }
        append_simple_string(response_out, b"OK");
        Ok(())
    }

    pub(super) fn handle_script(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 2, "SCRIPT", "SCRIPT <subcommand> [arg ...]")?;
        let subcommand = args[1];

        if ascii_eq_ignore_case(subcommand, b"FLUSH") {
            ensure_ranged_arity(args, 2, 3, "SCRIPT", SCRIPT_FLUSH_USAGE)?;
            if args.len() == 3 {
                let flush_mode = args[2];
                if !ascii_eq_ignore_case(flush_mode, b"ASYNC")
                    && !ascii_eq_ignore_case(flush_mode, b"SYNC")
                {
                    return Err(RequestExecutionError::UnknownCommand);
                }
            }
            self.clear_script_cache();
            append_simple_string(response_out, b"OK");
            return Ok(());
        }

        if !self.scripting_enabled() {
            return Err(RequestExecutionError::ScriptingDisabled);
        }

        if ascii_eq_ignore_case(subcommand, b"LOAD") {
            require_exact_arity(args, 3, "SCRIPT", SCRIPT_LOAD_USAGE)?;
            let sha1 = self.cache_script(args[2]);
            append_bulk_string(response_out, sha1.as_bytes());
            return Ok(());
        }

        if ascii_eq_ignore_case(subcommand, b"EXISTS") {
            ensure_min_arity(args, 3, "SCRIPT", SCRIPT_EXISTS_USAGE)?;
            response_out.push(b'*');
            response_out.extend_from_slice((args.len() - 2).to_string().as_bytes());
            response_out.extend_from_slice(b"\r\n");
            for sha1 in &args[2..] {
                let exists = self.get_cached_script(sha1).is_some();
                append_integer(response_out, if exists { 1 } else { 0 });
            }
            return Ok(());
        }

        Err(RequestExecutionError::UnknownCommand)
    }

    pub(super) fn handle_eval(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if !self.scripting_enabled() {
            validate_scripting_numkeys(args, "EVAL", SCRIPT_EVAL_USAGE)?;
            return Err(RequestExecutionError::ScriptingDisabled);
        }
        let key_count = validate_scripting_numkeys(args, "EVAL", SCRIPT_EVAL_USAGE)?;
        let script = args[1];
        let key_start = 3;
        let key_end = key_start + key_count;
        let _ = self.cache_script(script);
        self.execute_lua_script(
            script,
            &args[key_start..key_end],
            &args[key_end..],
            ScriptMutability::ReadWrite,
            response_out,
        );
        Ok(())
    }

    pub(super) fn handle_eval_ro(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if !self.scripting_enabled() {
            validate_scripting_numkeys(args, "EVAL_RO", SCRIPT_EVAL_RO_USAGE)?;
            return Err(RequestExecutionError::ScriptingDisabled);
        }
        let key_count = validate_scripting_numkeys(args, "EVAL_RO", SCRIPT_EVAL_RO_USAGE)?;
        let script = args[1];
        let key_start = 3;
        let key_end = key_start + key_count;
        let _ = self.cache_script(script);
        self.execute_lua_script(
            script,
            &args[key_start..key_end],
            &args[key_end..],
            ScriptMutability::ReadOnly,
            response_out,
        );
        Ok(())
    }

    pub(super) fn handle_evalsha(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if !self.scripting_enabled() {
            validate_scripting_numkeys(args, "EVALSHA", SCRIPT_EVALSHA_USAGE)?;
            return Err(RequestExecutionError::ScriptingDisabled);
        }
        let key_count = validate_scripting_numkeys(args, "EVALSHA", SCRIPT_EVALSHA_USAGE)?;
        let script = self
            .get_cached_script(args[1])
            .ok_or(RequestExecutionError::NoScript)?;
        let key_start = 3;
        let key_end = key_start + key_count;
        self.execute_lua_script(
            &script,
            &args[key_start..key_end],
            &args[key_end..],
            ScriptMutability::ReadWrite,
            response_out,
        );
        Ok(())
    }

    pub(super) fn handle_evalsha_ro(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if !self.scripting_enabled() {
            validate_scripting_numkeys(args, "EVALSHA_RO", SCRIPT_EVALSHA_RO_USAGE)?;
            return Err(RequestExecutionError::ScriptingDisabled);
        }
        let key_count = validate_scripting_numkeys(args, "EVALSHA_RO", SCRIPT_EVALSHA_RO_USAGE)?;
        let script = self
            .get_cached_script(args[1])
            .ok_or(RequestExecutionError::NoScript)?;
        let key_start = 3;
        let key_end = key_start + key_count;
        self.execute_lua_script(
            &script,
            &args[key_start..key_end],
            &args[key_end..],
            ScriptMutability::ReadOnly,
            response_out,
        );
        Ok(())
    }

    pub(super) fn handle_fcall(
        &self,
        args: &[&[u8]],
        _response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        validate_scripting_numkeys(args, "FCALL", SCRIPT_FCALL_USAGE)?;
        if !self.scripting_enabled() {
            return Err(RequestExecutionError::ScriptingDisabled);
        }
        Err(RequestExecutionError::CommandDisabled { command: "FCALL" })
    }

    pub(super) fn handle_fcall_ro(
        &self,
        args: &[&[u8]],
        _response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        validate_scripting_numkeys(args, "FCALL_RO", SCRIPT_FCALL_RO_USAGE)?;
        if !self.scripting_enabled() {
            return Err(RequestExecutionError::ScriptingDisabled);
        }
        Err(RequestExecutionError::CommandDisabled {
            command: "FCALL_RO",
        })
    }

    fn cache_script(&self, script: &[u8]) -> String {
        let sha1 = script_sha1_hex(script);
        if let Ok(mut cache) = self.script_cache.lock() {
            cache.entry(sha1.clone()).or_insert_with(|| script.to_vec());
        }
        sha1
    }

    fn get_cached_script(&self, sha1: &[u8]) -> Option<Vec<u8>> {
        let normalized = normalize_sha1(sha1);
        let Ok(cache) = self.script_cache.lock() else {
            return None;
        };
        cache.get(&normalized).cloned()
    }

    fn clear_script_cache(&self) {
        if let Ok(mut cache) = self.script_cache.lock() {
            cache.clear();
        }
    }

    fn execute_lua_script(
        &self,
        script: &[u8],
        keys: &[&[u8]],
        argv: &[&[u8]],
        mutability: ScriptMutability,
        response_out: &mut Vec<u8>,
    ) {
        let lua = Lua::new();
        let run_result = lua.scope(|scope| {
            let globals = lua.globals();
            let keys_table = lua.create_table_with_capacity(keys.len(), 0)?;
            for (index, key) in keys.iter().enumerate() {
                keys_table.set(index + 1, lua.create_string(key)?)?;
            }
            globals.set("KEYS", keys_table)?;

            let argv_table = lua.create_table_with_capacity(argv.len(), 0)?;
            for (index, arg) in argv.iter().enumerate() {
                argv_table.set(index + 1, lua.create_string(arg)?)?;
            }
            globals.set("ARGV", argv_table)?;

            let redis_table = lua.create_table()?;
            let call_fn = scope.create_function(move |lua, values: MultiValue| {
                self.execute_lua_redis_call(lua, values, mutability, LuaCallMode::Call)
            })?;
            let pcall_fn = scope.create_function(move |lua, values: MultiValue| {
                self.execute_lua_redis_call(lua, values, mutability, LuaCallMode::PCall)
            })?;
            let sha1hex_fn = scope.create_function(|_, value: mlua::String| {
                Ok(script_sha1_hex(value.as_bytes().as_ref()))
            })?;
            let status_reply_fn = scope.create_function(|lua, value: mlua::String| {
                let table = lua.create_table()?;
                table.set("ok", value)?;
                Ok(table)
            })?;
            let error_reply_fn = scope.create_function(|lua, value: mlua::String| {
                let table = lua.create_table()?;
                table.set("err", value)?;
                Ok(table)
            })?;
            let log_fn =
                scope.create_function(|_, (_level, _message): (LuaValue, LuaValue)| Ok(()))?;

            redis_table.set("call", call_fn)?;
            redis_table.set("pcall", pcall_fn)?;
            redis_table.set("sha1hex", sha1hex_fn)?;
            redis_table.set("status_reply", status_reply_fn)?;
            redis_table.set("error_reply", error_reply_fn)?;
            redis_table.set("log", log_fn)?;
            redis_table.set("LOG_DEBUG", 0)?;
            redis_table.set("LOG_VERBOSE", 1)?;
            redis_table.set("LOG_NOTICE", 2)?;
            redis_table.set("LOG_WARNING", 3)?;
            globals.set("redis", redis_table)?;

            let value: LuaValue = lua.load(script).eval()?;
            Ok::<LuaValue, LuaError>(value)
        });

        match run_result {
            Ok(value) => {
                if let Err(error_message) = append_lua_value_as_resp(value, response_out) {
                    let message = format!(
                        "ERR Error running script: {}",
                        sanitize_error_text(&error_message)
                    );
                    append_error(response_out, message.as_bytes());
                }
            }
            Err(error) => {
                let message = format!(
                    "ERR Error running script: {}",
                    sanitize_error_text(&error.to_string())
                );
                append_error(response_out, message.as_bytes());
            }
        }
    }

    fn execute_lua_redis_call<'lua>(
        &self,
        lua: &'lua Lua,
        values: MultiValue,
        mutability: ScriptMutability,
        call_mode: LuaCallMode,
    ) -> mlua::Result<LuaValue> {
        if values.is_empty() {
            return Err(LuaError::RuntimeError(
                "ERR redis.call/pcall requires a command name".to_string(),
            ));
        }

        let mut encoded_args = Vec::with_capacity(values.len());
        for (index, value) in values.into_iter().enumerate() {
            encoded_args.push(lua_argument_to_bytes(value, index)?);
        }
        let arg_refs = encoded_args
            .iter()
            .map(Vec::as_slice)
            .collect::<Vec<&[u8]>>();

        let command = dispatch_command_name(arg_refs[0]);
        if !command_allowed_from_script(command) {
            return lua_call_error_or_pcall_error(
                lua,
                call_mode,
                "ERR command is not allowed from script",
            );
        }
        if mutability == ScriptMutability::ReadOnly && command_is_mutating(command) {
            return lua_call_error_or_pcall_error(
                lua,
                call_mode,
                "ERR Write commands are not allowed from read-only scripts",
            );
        }

        let mut response = Vec::new();
        match self.execute_bytes(&arg_refs, &mut response) {
            Ok(()) => {}
            Err(error) => {
                let message = request_error_message(error);
                return lua_call_error_or_pcall_error(lua, call_mode, &message);
            }
        }

        let frame = parse_resp_frame_complete(&response).map_err(|message| {
            LuaError::RuntimeError(format!(
                "ERR internal scripting response parse error: {}",
                sanitize_error_text(&message)
            ))
        })?;
        match frame {
            RespFrame::Error(message) => lua_call_error_or_pcall_error(lua, call_mode, &message),
            other => resp_frame_to_lua_value(lua, other),
        }
    }
}

fn lua_call_error_or_pcall_error(
    lua: &Lua,
    call_mode: LuaCallMode,
    message: &str,
) -> mlua::Result<LuaValue> {
    match call_mode {
        LuaCallMode::Call => Err(LuaError::RuntimeError(message.to_string())),
        LuaCallMode::PCall => {
            let table = lua.create_table()?;
            table.set("err", message)?;
            Ok(LuaValue::Table(table))
        }
    }
}

fn lua_argument_to_bytes(value: LuaValue, index: usize) -> mlua::Result<Vec<u8>> {
    match value {
        LuaValue::String(value) => Ok(value.as_bytes().to_vec()),
        LuaValue::Integer(value) => Ok(value.to_string().into_bytes()),
        LuaValue::Number(value) => {
            if !value.is_finite() {
                return Err(LuaError::RuntimeError(
                    "ERR redis.call argument must be a finite number".to_string(),
                ));
            }
            Ok(value.to_string().into_bytes())
        }
        LuaValue::Boolean(value) => Ok(if value { b"1".to_vec() } else { b"0".to_vec() }),
        LuaValue::Nil => Err(LuaError::RuntimeError(format!(
            "ERR redis.call argument at position {} is nil",
            index + 1
        ))),
        _ => Err(LuaError::RuntimeError(format!(
            "ERR redis.call argument at position {} has unsupported type",
            index + 1
        ))),
    }
}

fn request_error_message(error: RequestExecutionError) -> String {
    let mut encoded = Vec::new();
    error.append_resp_error(&mut encoded);
    if encoded.starts_with(b"-") && encoded.ends_with(b"\r\n") && encoded.len() >= 3 {
        return String::from_utf8_lossy(&encoded[1..encoded.len() - 2]).into_owned();
    }
    "ERR internal script command failure".to_string()
}

fn append_lua_value_as_resp(value: LuaValue, response_out: &mut Vec<u8>) -> Result<(), String> {
    match value {
        LuaValue::Nil => {
            append_null_bulk_string(response_out);
            Ok(())
        }
        LuaValue::Boolean(value) => {
            if value {
                append_integer(response_out, 1);
            } else {
                append_null_bulk_string(response_out);
            }
            Ok(())
        }
        LuaValue::Integer(value) => {
            append_integer(response_out, value);
            Ok(())
        }
        LuaValue::Number(value) => {
            if !value.is_finite() {
                return Err("Lua number result is not finite".to_string());
            }
            if value < i64::MIN as f64 || value > i64::MAX as f64 {
                return Err("Lua number result is out of i64 range".to_string());
            }
            append_integer(response_out, value as i64);
            Ok(())
        }
        LuaValue::String(value) => {
            append_bulk_string(response_out, value.as_bytes().as_ref());
            Ok(())
        }
        LuaValue::Table(table) => append_lua_table_as_resp(table, response_out),
        _ => Err("Lua script returned unsupported value type".to_string()),
    }
}

fn append_lua_table_as_resp(table: LuaTable, response_out: &mut Vec<u8>) -> Result<(), String> {
    if let Some(error_payload) = lua_table_named_string_field(&table, "err")? {
        append_error(response_out, &error_payload);
        return Ok(());
    }
    if let Some(status_payload) = lua_table_named_string_field(&table, "ok")? {
        append_simple_string(response_out, &status_payload);
        return Ok(());
    }

    let mut values = Vec::new();
    for entry in table.sequence_values::<LuaValue>() {
        let value = entry.map_err(|error| sanitize_error_text(&error.to_string()))?;
        values.push(value);
    }
    response_out.push(b'*');
    response_out.extend_from_slice(values.len().to_string().as_bytes());
    response_out.extend_from_slice(b"\r\n");
    for value in values {
        append_lua_value_as_resp(value, response_out)?;
    }
    Ok(())
}

fn lua_table_named_string_field(table: &LuaTable, field: &str) -> Result<Option<Vec<u8>>, String> {
    let value = table
        .get::<LuaValue>(field)
        .map_err(|error| sanitize_error_text(&error.to_string()))?;
    match value {
        LuaValue::Nil => Ok(None),
        LuaValue::String(payload) => Ok(Some(payload.as_bytes().to_vec())),
        _ => Err(format!("Lua table field '{}' must be a string", field)),
    }
}

fn resp_frame_to_lua_value(lua: &Lua, frame: RespFrame) -> mlua::Result<LuaValue> {
    match frame {
        RespFrame::SimpleString(value) => {
            let table = lua.create_table()?;
            table.set("ok", lua.create_string(&value)?)?;
            Ok(LuaValue::Table(table))
        }
        RespFrame::Error(message) => {
            let table = lua.create_table()?;
            table.set("err", message)?;
            Ok(LuaValue::Table(table))
        }
        RespFrame::Integer(value) => Ok(LuaValue::Integer(value)),
        RespFrame::Bulk(Some(value)) => Ok(LuaValue::String(lua.create_string(&value)?)),
        RespFrame::Bulk(None) | RespFrame::Array(None) | RespFrame::Null => {
            Ok(LuaValue::Boolean(false))
        }
        RespFrame::Array(Some(items)) => {
            let table = lua.create_table_with_capacity(items.len(), 0)?;
            for (index, item) in items.into_iter().enumerate() {
                table.set(index + 1, resp_frame_to_lua_value(lua, item)?)?;
            }
            Ok(LuaValue::Table(table))
        }
    }
}

fn parse_resp_frame_complete(input: &[u8]) -> Result<RespFrame, String> {
    let mut index = 0usize;
    let frame = parse_resp_frame(input, &mut index)?;
    if index != input.len() {
        return Err("unexpected trailing bytes in RESP frame".to_string());
    }
    Ok(frame)
}

fn parse_resp_frame(input: &[u8], index: &mut usize) -> Result<RespFrame, String> {
    let prefix = *input
        .get(*index)
        .ok_or_else(|| "unexpected end of input while parsing RESP prefix".to_string())?;
    *index += 1;
    match prefix {
        b'+' => {
            let line = parse_resp_line(input, index)?;
            Ok(RespFrame::SimpleString(line.to_vec()))
        }
        b'-' => {
            let line = parse_resp_line(input, index)?;
            Ok(RespFrame::Error(String::from_utf8_lossy(line).into_owned()))
        }
        b':' => {
            let line = parse_resp_line(input, index)?;
            let value = parse_resp_i64(line)?;
            Ok(RespFrame::Integer(value))
        }
        b'$' => {
            let line = parse_resp_line(input, index)?;
            let length = parse_resp_i64(line)?;
            if length == -1 {
                return Ok(RespFrame::Bulk(None));
            }
            if length < 0 {
                return Err("invalid negative bulk length".to_string());
            }
            let length = length as usize;
            if *index + length + 2 > input.len() {
                return Err("bulk payload exceeds available input".to_string());
            }
            let payload = input[*index..*index + length].to_vec();
            *index += length;
            if input.get(*index) != Some(&b'\r') || input.get(*index + 1) != Some(&b'\n') {
                return Err("bulk payload missing trailing CRLF".to_string());
            }
            *index += 2;
            Ok(RespFrame::Bulk(Some(payload)))
        }
        b'*' => {
            let line = parse_resp_line(input, index)?;
            let length = parse_resp_i64(line)?;
            if length == -1 {
                return Ok(RespFrame::Array(None));
            }
            if length < 0 {
                return Err("invalid negative array length".to_string());
            }
            let mut entries = Vec::with_capacity(length as usize);
            for _ in 0..length {
                entries.push(parse_resp_frame(input, index)?);
            }
            Ok(RespFrame::Array(Some(entries)))
        }
        b'_' => {
            expect_resp_crlf(input, index)?;
            Ok(RespFrame::Null)
        }
        _ => Err("unsupported RESP type in scripting bridge".to_string()),
    }
}

fn parse_resp_line<'a>(input: &'a [u8], index: &mut usize) -> Result<&'a [u8], String> {
    let start = *index;
    while *index + 1 < input.len() {
        if input[*index] == b'\r' && input[*index + 1] == b'\n' {
            let line = &input[start..*index];
            *index += 2;
            return Ok(line);
        }
        *index += 1;
    }
    Err("unterminated RESP line".to_string())
}

fn expect_resp_crlf(input: &[u8], index: &mut usize) -> Result<(), String> {
    if input.get(*index) == Some(&b'\r') && input.get(*index + 1) == Some(&b'\n') {
        *index += 2;
        return Ok(());
    }
    Err("expected CRLF".to_string())
}

fn parse_resp_i64(bytes: &[u8]) -> Result<i64, String> {
    let text =
        std::str::from_utf8(bytes).map_err(|_| "RESP integer is not valid UTF-8".to_string())?;
    text.parse::<i64>()
        .map_err(|_| "RESP integer parse error".to_string())
}

fn command_allowed_from_script(command: CommandId) -> bool {
    !matches!(
        command,
        CommandId::Eval
            | CommandId::EvalRo
            | CommandId::Evalsha
            | CommandId::EvalshaRo
            | CommandId::Fcall
            | CommandId::FcallRo
            | CommandId::Function
            | CommandId::Script
            | CommandId::Multi
            | CommandId::Exec
            | CommandId::Discard
            | CommandId::Watch
            | CommandId::Unwatch
            | CommandId::Unknown
    )
}

fn validate_scripting_numkeys(
    args: &[&[u8]],
    command: &'static str,
    expected: &'static str,
) -> Result<usize, RequestExecutionError> {
    ensure_min_arity(args, 3, command, expected)?;
    let numkeys_raw = args[2];
    let numkeys = parse_i64_ascii(numkeys_raw).ok_or(RequestExecutionError::ValueNotInteger)?;
    if numkeys < 0 {
        return Err(RequestExecutionError::ValueOutOfRange);
    }
    let key_count = usize::try_from(numkeys).map_err(|_| RequestExecutionError::ValueOutOfRange)?;
    if key_count > args.len().saturating_sub(3) {
        return Err(RequestExecutionError::SyntaxError);
    }
    Ok(key_count)
}

fn normalize_sha1(raw: &[u8]) -> String {
    String::from_utf8_lossy(raw).to_ascii_lowercase()
}

fn script_sha1_hex(script: &[u8]) -> String {
    let mut hasher = Sha1::new();
    hasher.update(script);
    let digest = hasher.finalize();
    let mut output = String::with_capacity(digest.len() * 2);
    for byte in digest {
        let _ = write!(&mut output, "{byte:02x}");
    }
    output
}

fn sanitize_error_text(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    for ch in input.chars() {
        if ch == '\r' || ch == '\n' {
            output.push(' ');
        } else {
            output.push(ch);
        }
    }
    output
}
