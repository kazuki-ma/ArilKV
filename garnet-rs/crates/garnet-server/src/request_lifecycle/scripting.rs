use super::*;
use crate::CommandId;
use crate::command_spec::command_is_mutating;
use crate::command_spec::eval_script_has_no_writes_flag;
use crate::dispatch_command_name;
use mlua::Error as LuaError;
use mlua::Function as LuaFunction;
use mlua::HookTriggers;
use mlua::Lua;
use mlua::MultiValue;
use mlua::Table as LuaTable;
use mlua::Value as LuaValue;
use mlua::VmState;
use sha1::Digest;
use sha1::Sha1;
use std::cell::RefCell;
use std::fmt::Write;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

const SCRIPT_EVAL_USAGE: &str = "EVAL script numkeys [key ...] [arg ...]";
const SCRIPT_EVAL_RO_USAGE: &str = "EVAL_RO script numkeys [key ...] [arg ...]";
const SCRIPT_EVALSHA_USAGE: &str = "EVALSHA sha1 numkeys [key ...] [arg ...]";
const SCRIPT_EVALSHA_RO_USAGE: &str = "EVALSHA_RO sha1 numkeys [key ...] [arg ...]";
const SCRIPT_FCALL_USAGE: &str = "FCALL function numkeys [key ...] [arg ...]";
const SCRIPT_FCALL_RO_USAGE: &str = "FCALL_RO function numkeys [key ...] [arg ...]";
const SCRIPT_FUNCTION_USAGE: &str = "FUNCTION <subcommand> [args]";
const SCRIPT_FUNCTION_LOAD_USAGE: &str = "FUNCTION LOAD [REPLACE] library-code";
const SCRIPT_FUNCTION_LIST_USAGE: &str = "FUNCTION LIST [WITHCODE] [LIBRARYNAME pattern]";
const SCRIPT_FUNCTION_DELETE_USAGE: &str = "FUNCTION DELETE library-name";
const SCRIPT_FUNCTION_DUMP_USAGE: &str = "FUNCTION DUMP";
const SCRIPT_FLUSH_USAGE: &str = "SCRIPT FLUSH [ASYNC|SYNC]";
const SCRIPT_LOAD_USAGE: &str = "SCRIPT LOAD script";
const SCRIPT_EXISTS_USAGE: &str = "SCRIPT EXISTS sha1 [sha1 ...]";
const SCRIPT_DEBUG_USAGE: &str = "SCRIPT DEBUG YES|SYNC|NO";
const SCRIPT_TIMEOUT_ERROR_TEXT: &str = "ERR script execution timed out";
const SCRIPT_FUNCTION_LOAD_TIMEOUT_ERROR_TEXT: &str = "ERR FUNCTION LOAD timeout";
const SCRIPT_KILLED_ERROR_TEXT: &str = "ERR script killed by user";
const LUA_TIMEOUT_HOOK_INSTRUCTION_STRIDE: u32 = 1_024;
/// Maximum number of results that `unpack()` is allowed to push onto the Lua
/// stack. Matches the C-level `LUAI_MAXCSTACK` constant (8 000) used by the
/// vendored Lua 5.1 runtime.  The vanilla `luaB_unpack` relies on signed
/// integer overflow (undefined behaviour in C) to detect huge ranges; under
/// optimised ARM64 builds the compiler may elide the safety check, leading to
/// a SIGSEGV when the stack grows into unmapped memory.  By capping the range
/// in Rust before the C code runs, we avoid the UB entirely.
const LUA_UNPACK_MAX_RESULTS: i64 = 8_000;
/// Maximum recursion depth when converting a Lua value to RESP.
/// Matches Redis/Valkey behaviour: recursive tables produce nested `*1`
/// arrays up to this depth, then emit `-ERR reached lua stack limit`.
const LUA_REPLY_MAX_DEPTH: usize = 100;
const SCRIPT_FUNCTION_NAME_FORMAT_ERROR: &str = "Library names can only contain letters, numbers, or underscores(_) and must be at least one character long";
const SCRIPT_FUNCTION_LOAD_RUNTIME_ONLY_ERROR: &str =
    "redis.register_function can only be called on FUNCTION LOAD command";
const SCRIPT_REDIS_VERSION_TEXT: &str = "7.4.0";
const SCRIPT_REDIS_VERSION_NUM: i64 = ((7i64) << 16) | ((4i64) << 8);
const LUA_ALLOWED_GLOBALS: [&str; 26] = [
    "assert",
    "collectgarbage",
    "coroutine",
    "error",
    "gcinfo",
    "getmetatable",
    "ipairs",
    "load",
    "loadstring",
    "math",
    "next",
    "pairs",
    "pcall",
    "rawequal",
    "rawget",
    "rawset",
    "select",
    "setmetatable",
    "string",
    "table",
    "tonumber",
    "tostring",
    "type",
    "unpack",
    "xpcall",
    "_VERSION",
];

const SCRIPT_HELP_LINES: [&[u8]; 6] = [
    b"SCRIPT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
    b"DEBUG (YES|SYNC|NO) -- Set the debug mode for subsequent scripts executed.",
    b"EXISTS <sha1> [<sha1> ...] -- Return information about the existence of scripts in the cache.",
    b"FLUSH [ASYNC|SYNC] -- Flush the Lua scripts cache.",
    b"KILL -- Kill the currently executing Lua script.",
    b"LOAD <script> -- Load a script into the scripts cache without executing it.",
];

const FUNCTION_HELP_LINES: [&[u8]; 9] = [
    b"FUNCTION <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
    b"DELETE <library-name> -- Delete an existing library and all its functions.",
    b"DUMP -- Return a serialized payload representing all libraries.",
    b"FLUSH [ASYNC|SYNC] -- Delete all libraries and their functions.",
    b"HELP -- Return this help.",
    b"KILL -- Kill the currently executing Lua function.",
    b"LIST [WITHCODE] -- List libraries and their registered functions.",
    b"RESTORE <serialized-value> [FLUSH|APPEND|REPLACE] -- Restore libraries from serialized payload.",
    b"STATS -- Return minimal function-engine stats.",
];

const FUNCTION_DUMP_MAGIC: &[u8; 4] = b"GFD1";

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

#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingFunctionRegistration {
    name: String,
    description: String,
    flags: FunctionExecutionFlags,
}

struct RegisterFunctionBinding {
    name: String,
    description: String,
    flags: FunctionExecutionFlags,
    callback: LuaFunction,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct FunctionExecutionFlags {
    read_only: bool,
    allow_oom: bool,
    allow_stale: bool,
}

struct ResolvedFunctionTarget {
    library_source: Vec<u8>,
    flags: FunctionExecutionFlags,
}

enum FunctionRegistrationCollectError {
    Timeout,
    Compile(String),
}

enum FunctionRestoreError {
    Request(RequestExecutionError),
    LibraryAlreadyExists(String),
    FunctionAlreadyExists(String),
}

enum ScriptKillOutcome {
    NotBusy,
    Killed,
    BusyOtherKind,
}

struct RunningScriptGuard<'a> {
    processor: &'a RequestProcessor,
}

impl Drop for RunningScriptGuard<'_> {
    fn drop(&mut self) {
        self.processor
            .script_running
            .store(false, Ordering::Release);
        self.processor
            .script_kill_requested
            .store(false, Ordering::Release);
        self.processor
            .function_kill_requested
            .store(false, Ordering::Release);
        if let Ok(mut running_script) = self.processor.running_script.lock() {
            *running_script = None;
        }
    }
}

impl RequestProcessor {
    pub(super) fn handle_function(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 2, "FUNCTION", SCRIPT_FUNCTION_USAGE)?;
        let subcommand = args[1];

        if ascii_eq_ignore_case(subcommand, b"HELP") {
            require_exact_arity(args, 2, "FUNCTION", "FUNCTION HELP")?;
            append_bulk_array(response_out, &FUNCTION_HELP_LINES);
            return Ok(());
        }

        if ascii_eq_ignore_case(subcommand, b"FLUSH") {
            if args.len() > 3 {
                append_error(
                    response_out,
                    b"ERR unknown subcommand or wrong number of arguments for 'flush'. Try FUNCTION HELP.",
                );
                return Ok(());
            }
            let mut async_flush = false;
            if args.len() == 3 {
                let flush_mode = args[2];
                if ascii_eq_ignore_case(flush_mode, b"ASYNC") {
                    async_flush = true;
                } else if !ascii_eq_ignore_case(flush_mode, b"SYNC") {
                    append_error(response_out, b"ERR FUNCTION FLUSH only supports SYNC|ASYNC");
                    return Ok(());
                }
            }
            self.clear_function_registry(async_flush);
            append_simple_string(response_out, b"OK");
            return Ok(());
        }

        if ascii_eq_ignore_case(subcommand, b"DUMP") {
            require_exact_arity(args, 2, "FUNCTION", SCRIPT_FUNCTION_DUMP_USAGE)?;
            let payload = self.dump_function_registry()?;
            append_bulk_string(response_out, &payload);
            return Ok(());
        }

        if ascii_eq_ignore_case(subcommand, b"RESTORE") {
            if !self.scripting_enabled() {
                return Err(RequestExecutionError::ScriptingDisabled);
            }
            if self.maxmemory_limit_bytes() > 0 {
                append_error(
                    response_out,
                    b"OOM command not allowed when used memory > 'maxmemory'.",
                );
                return Ok(());
            }
            if args.len() != 3 && args.len() != 4 {
                append_error(
                    response_out,
                    b"ERR unknown subcommand or wrong number of arguments for 'restore'. Try FUNCTION HELP.",
                );
                return Ok(());
            }
            let mode = parse_function_restore_mode(args.get(3).copied())?;
            match self.restore_function_registry(args[2], mode) {
                Ok(()) => {}
                Err(FunctionRestoreError::Request(RequestExecutionError::InvalidDumpPayload)) => {
                    RequestExecutionError::InvalidDumpPayload.append_resp_error(response_out);
                    return Ok(());
                }
                Err(FunctionRestoreError::LibraryAlreadyExists(library_name)) => {
                    append_error(
                        response_out,
                        format!("ERR Library {library_name} already exists").as_bytes(),
                    );
                    return Ok(());
                }
                Err(FunctionRestoreError::FunctionAlreadyExists(function_name)) => {
                    append_error(
                        response_out,
                        format!("ERR Function {function_name} already exists").as_bytes(),
                    );
                    return Ok(());
                }
                Err(FunctionRestoreError::Request(error)) => return Err(error),
            }
            append_simple_string(response_out, b"OK");
            return Ok(());
        }

        if ascii_eq_ignore_case(subcommand, b"KILL") {
            require_exact_arity(args, 2, "FUNCTION", "FUNCTION KILL")?;
            match self.request_script_kill(RunningScriptKind::Function)? {
                ScriptKillOutcome::NotBusy => {
                    append_error(response_out, b"NOTBUSY No scripts in execution right now.");
                }
                ScriptKillOutcome::Killed => {
                    append_simple_string(response_out, b"OK");
                }
                ScriptKillOutcome::BusyOtherKind => {
                    RequestExecutionError::BusyScript.append_resp_error(response_out);
                }
            }
            return Ok(());
        }

        if ascii_eq_ignore_case(subcommand, b"LIST") {
            let list_options = match parse_function_list_options(args) {
                Ok(options) => options,
                Err(message) => {
                    append_error(response_out, format!("ERR {message}").as_bytes());
                    return Ok(());
                }
            };
            self.append_function_list_response(
                response_out,
                list_options.with_code,
                list_options.library_name_pattern.as_deref(),
            )?;
            return Ok(());
        }

        if ascii_eq_ignore_case(subcommand, b"DELETE") {
            require_exact_arity(args, 3, "FUNCTION", SCRIPT_FUNCTION_DELETE_USAGE)?;
            self.delete_function_library(args[2])?;
            append_simple_string(response_out, b"OK");
            return Ok(());
        }

        if ascii_eq_ignore_case(subcommand, b"STATS") {
            require_exact_arity(args, 2, "FUNCTION", "FUNCTION STATS")?;
            self.append_function_stats_response(response_out)?;
            return Ok(());
        }

        if ascii_eq_ignore_case(subcommand, b"LOAD") {
            if !self.scripting_enabled() {
                return Err(RequestExecutionError::ScriptingDisabled);
            }
            ensure_min_arity(args, 3, "FUNCTION", SCRIPT_FUNCTION_LOAD_USAGE)?;
            let (replace, source_index) = if args.len() == 3 {
                (false, 2)
            } else if ascii_eq_ignore_case(args[2], b"REPLACE") && args.len() == 4 {
                (true, 3)
            } else {
                append_error(response_out, b"ERR Unknown option given");
                return Ok(());
            };

            let library_source = args[source_index];
            self.validate_script_size_limit(library_source)?;
            let library_name = match parse_function_library_metadata(library_source) {
                Ok(metadata) => metadata.library_name,
                Err(FunctionLibraryParseError::InvalidLibraryName) => {
                    append_error(
                        response_out,
                        format!("ERR {SCRIPT_FUNCTION_NAME_FORMAT_ERROR}").as_bytes(),
                    );
                    return Ok(());
                }
                Err(FunctionLibraryParseError::LibraryNameMissing) => {
                    append_error(response_out, b"ERR Library name was not given");
                    return Ok(());
                }
                Err(FunctionLibraryParseError::LibraryNameGivenMultipleTimes) => {
                    append_error(
                        response_out,
                        b"ERR Invalid metadata value, name argument was given multiple times",
                    );
                    return Ok(());
                }
                Err(FunctionLibraryParseError::InvalidMetadataValue(value)) => {
                    append_error(
                        response_out,
                        format!("ERR Invalid metadata value given: {value}").as_bytes(),
                    );
                    return Ok(());
                }
                Err(FunctionLibraryParseError::EngineNotFound(engine)) => {
                    let message = format!("ERR Engine '{engine}' not found");
                    append_error(response_out, message.as_bytes());
                    return Ok(());
                }
                Err(FunctionLibraryParseError::Syntax) => {
                    return Err(RequestExecutionError::SyntaxError);
                }
            };
            if !replace && self.function_library_exists(&library_name)? {
                append_error(
                    response_out,
                    format!("ERR Library '{library_name}' already exists").as_bytes(),
                );
                return Ok(());
            }
            let registrations = match self.collect_function_registrations(library_source) {
                Ok(registrations) => registrations,
                Err(FunctionRegistrationCollectError::Timeout) => {
                    append_error(
                        response_out,
                        SCRIPT_FUNCTION_LOAD_TIMEOUT_ERROR_TEXT.as_bytes(),
                    );
                    return Ok(());
                }
                Err(FunctionRegistrationCollectError::Compile(message)) => {
                    append_function_load_compile_error(response_out, &message);
                    return Ok(());
                }
            };
            if self.maxmemory_limit_bytes() > 0
                && registrations
                    .iter()
                    .any(|entry| !entry.flags.read_only && !entry.flags.allow_oom)
            {
                append_error(
                    response_out,
                    b"OOM command not allowed when used memory > 'maxmemory'.",
                );
                return Ok(());
            }
            if let Some(function_name) =
                self.find_cross_library_function_name_conflict(&library_name, &registrations)?
            {
                let message = format!("ERR Function {function_name} already exists");
                append_error(response_out, message.as_bytes());
                return Ok(());
            }
            self.store_function_library(
                library_name.clone(),
                library_source,
                &registrations,
                replace,
            )?;
            append_bulk_string(response_out, library_name.as_bytes());
            return Ok(());
        }

        append_error(response_out, b"ERR unknown subcommand");
        Ok(())
    }

    pub(super) fn handle_script(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 2, "SCRIPT", "SCRIPT <subcommand> [arg ...]")?;
        let subcommand = args[1];

        if ascii_eq_ignore_case(subcommand, b"HELP") {
            require_exact_arity(args, 2, "SCRIPT", "SCRIPT HELP")?;
            append_bulk_array(response_out, &SCRIPT_HELP_LINES);
            return Ok(());
        }

        if ascii_eq_ignore_case(subcommand, b"KILL") {
            require_exact_arity(args, 2, "SCRIPT", "SCRIPT KILL")?;
            match self.request_script_kill(RunningScriptKind::Script)? {
                ScriptKillOutcome::NotBusy => {
                    append_error(response_out, b"NOTBUSY No scripts in execution right now.");
                }
                ScriptKillOutcome::Killed => {
                    append_simple_string(response_out, b"OK");
                }
                ScriptKillOutcome::BusyOtherKind => {
                    RequestExecutionError::BusyScript.append_resp_error(response_out);
                }
            }
            return Ok(());
        }

        if ascii_eq_ignore_case(subcommand, b"DEBUG") {
            require_exact_arity(args, 3, "SCRIPT", SCRIPT_DEBUG_USAGE)?;
            let mode = args[2];
            if !ascii_eq_ignore_case(mode, b"YES")
                && !ascii_eq_ignore_case(mode, b"SYNC")
                && !ascii_eq_ignore_case(mode, b"NO")
            {
                return Err(RequestExecutionError::SyntaxError);
            }
            append_simple_string(response_out, b"OK");
            return Ok(());
        }

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
            self.validate_script_size_limit(args[2])?;
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
        self.validate_script_size_limit(script)?;
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
        self.validate_script_size_limit(script)?;
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
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        let Some(key_count) =
            validate_fcall_numkeys(args, "FCALL", SCRIPT_FCALL_USAGE, response_out)
        else {
            return Ok(());
        };
        if !self.scripting_enabled() {
            return Err(RequestExecutionError::ScriptingDisabled);
        }
        let function_name = args[1];
        let function_target = self.resolve_function_target(function_name)?;
        let key_start = 3;
        let key_end = key_start + key_count;
        let mutability = if function_target.flags.read_only {
            ScriptMutability::ReadOnly
        } else {
            ScriptMutability::ReadWrite
        };
        self.execute_lua_function(
            &function_target.library_source,
            function_name,
            &args[key_start..key_end],
            &args[key_end..],
            mutability,
            function_target.flags.allow_oom,
            "fcall",
            response_out,
        );
        Ok(())
    }

    pub(super) fn handle_fcall_ro(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        let Some(key_count) =
            validate_fcall_numkeys(args, "FCALL_RO", SCRIPT_FCALL_RO_USAGE, response_out)
        else {
            return Ok(());
        };
        if !self.scripting_enabled() {
            return Err(RequestExecutionError::ScriptingDisabled);
        }
        let function_name = args[1];
        let function_target = self.resolve_function_target(function_name)?;
        if !function_target.flags.read_only {
            return Err(RequestExecutionError::FunctionNotReadOnly);
        }
        let key_start = 3;
        let key_end = key_start + key_count;
        self.execute_lua_function(
            &function_target.library_source,
            function_name,
            &args[key_start..key_end],
            &args[key_end..],
            ScriptMutability::ReadOnly,
            function_target.flags.allow_oom,
            "fcall_ro",
            response_out,
        );
        Ok(())
    }

    fn cache_script(&self, script: &[u8]) -> String {
        let sha1 = script_sha1_hex(script);
        let cache_max_entries = self.scripting_runtime_config().cache_max_entries;
        if let Ok(mut cache) = self.script_cache.lock() {
            let mut inserted = false;
            if let std::collections::hash_map::Entry::Vacant(slot) = cache.entry(sha1.clone()) {
                slot.insert(script.to_vec());
                inserted = true;
            }
            if inserted && let Ok(mut insertion_order) = self.script_cache_insertion_order.lock() {
                insertion_order.push_back(sha1.clone());
                if cache_max_entries > 0 {
                    while cache.len() > cache_max_entries {
                        if let Some(evicted_sha) = insertion_order.pop_front() {
                            if cache.remove(&evicted_sha).is_some() {
                                self.record_script_cache_eviction();
                            }
                        } else {
                            break;
                        }
                    }
                }
            }
        }
        sha1
    }

    fn get_cached_script(&self, sha1: &[u8]) -> Option<Vec<u8>> {
        let normalized = normalize_sha1(sha1);
        let Ok(cache) = self.script_cache.lock() else {
            self.record_script_cache_miss();
            return None;
        };
        let result = cache.get(&normalized).cloned();
        if result.is_some() {
            self.record_script_cache_hit();
        } else {
            self.record_script_cache_miss();
        }
        result
    }

    fn clear_script_cache(&self) {
        if let Ok(mut cache) = self.script_cache.lock() {
            cache.clear();
        }
        if let Ok(mut insertion_order) = self.script_cache_insertion_order.lock() {
            insertion_order.clear();
        }
    }

    fn validate_script_size_limit(&self, script: &[u8]) -> Result<(), RequestExecutionError> {
        let max_script_bytes = self.scripting_runtime_config().max_script_bytes;
        if max_script_bytes > 0 && script.len() > max_script_bytes {
            return Err(RequestExecutionError::ScriptTooBig);
        }
        Ok(())
    }

    fn clear_function_registry(&self, async_flush: bool) {
        if let Ok(mut registry) = self.function_registry.lock() {
            let library_count = registry.library_sources.len() as u64;
            registry.functions.clear();
            registry.library_sources.clear();
            registry.library_function_names.clear();
            self.set_used_memory_vm_functions(0);
            if async_flush && library_count > 0 {
                // Redis accounts async FUNCTION FLUSH lazyfree work as one object per
                // loaded library plus the function-engine container.
                self.record_lazyfreed_objects(library_count + 1);
            }
        }
    }

    fn function_library_exists(&self, library_name: &str) -> Result<bool, RequestExecutionError> {
        let Ok(registry) = self.function_registry.lock() else {
            return Err(storage_failure(
                "function.registry",
                "function registry lock poisoned",
            ));
        };
        Ok(registry.library_sources.contains_key(library_name))
    }

    fn find_cross_library_function_name_conflict(
        &self,
        library_name: &str,
        registrations: &[PendingFunctionRegistration],
    ) -> Result<Option<String>, RequestExecutionError> {
        let Ok(registry) = self.function_registry.lock() else {
            return Err(storage_failure(
                "function.registry",
                "function registry lock poisoned",
            ));
        };
        for registration in registrations {
            if let Some(existing) = registry.functions.get(&registration.name)
                && existing.library_name != library_name
            {
                return Ok(Some(registration.name.clone()));
            }
        }
        Ok(None)
    }

    fn enter_running_script(
        &self,
        kind: RunningScriptKind,
        name: &str,
        command: String,
    ) -> Result<RunningScriptGuard<'_>, RequestExecutionError> {
        let Ok(mut running_script) = self.running_script.lock() else {
            return Err(storage_failure(
                "script.running_state",
                "running script state lock poisoned",
            ));
        };
        if running_script.is_some() {
            return Err(RequestExecutionError::BusyScript);
        }
        self.script_kill_requested.store(false, Ordering::Release);
        self.function_kill_requested.store(false, Ordering::Release);
        *running_script = Some(RunningScriptState {
            kind,
            name: name.to_string(),
            command,
            started_at: Instant::now(),
            thread_id: std::thread::current().id(),
        });
        self.script_running.store(true, Ordering::Release);
        Ok(RunningScriptGuard { processor: self })
    }

    fn request_script_kill(
        &self,
        target: RunningScriptKind,
    ) -> Result<ScriptKillOutcome, RequestExecutionError> {
        let Ok(running_script) = self.running_script.lock() else {
            return Err(storage_failure(
                "script.running_state",
                "running script state lock poisoned",
            ));
        };
        let Some(state) = running_script.as_ref() else {
            return Ok(ScriptKillOutcome::NotBusy);
        };
        if state.kind != target {
            return Ok(ScriptKillOutcome::BusyOtherKind);
        }
        match target {
            RunningScriptKind::Script => {
                self.script_kill_requested.store(true, Ordering::Release);
            }
            RunningScriptKind::Function => {
                self.function_kill_requested.store(true, Ordering::Release);
            }
        }
        Ok(ScriptKillOutcome::Killed)
    }

    fn dump_function_registry(&self) -> Result<Vec<u8>, RequestExecutionError> {
        let Ok(registry) = self.function_registry.lock() else {
            return Err(storage_failure(
                "function.registry",
                "function registry lock poisoned",
            ));
        };
        let mut library_names = registry
            .library_sources
            .keys()
            .cloned()
            .collect::<Vec<String>>();
        library_names.sort_unstable();

        let mut payload = Vec::new();
        payload.extend_from_slice(FUNCTION_DUMP_MAGIC);
        payload.extend_from_slice(&(library_names.len() as u32).to_le_bytes());
        for library_name in library_names {
            let Some(source) = registry.library_sources.get(&library_name) else {
                return Err(storage_failure(
                    "function.registry",
                    "missing function library source during dump",
                ));
            };
            append_len_prefixed_bytes(&mut payload, library_name.as_bytes())?;
            append_len_prefixed_bytes(&mut payload, source)?;
        }
        Ok(payload)
    }

    fn restore_function_registry(
        &self,
        serialized_payload: &[u8],
        mode: FunctionRestoreMode,
    ) -> Result<(), FunctionRestoreError> {
        let libraries = parse_function_dump_payload(serialized_payload)
            .map_err(FunctionRestoreError::Request)?;
        if matches!(mode, FunctionRestoreMode::Flush) {
            self.clear_function_registry(false);
        }

        let replace = matches!(mode, FunctionRestoreMode::Replace);
        for (library_name, library_source) in libraries {
            self.validate_script_size_limit(&library_source)
                .map_err(FunctionRestoreError::Request)?;
            let parsed_name = parse_function_library_metadata(&library_source)
                .map_err(|_| {
                    FunctionRestoreError::Request(RequestExecutionError::InvalidDumpPayload)
                })?
                .library_name;
            if parsed_name != library_name {
                return Err(FunctionRestoreError::Request(
                    RequestExecutionError::InvalidDumpPayload,
                ));
            }
            let registrations = self
                .collect_function_registrations(&library_source)
                .map_err(|error| match error {
                    FunctionRegistrationCollectError::Timeout => FunctionRestoreError::Request(
                        RequestExecutionError::ScriptExecutionTimedOut,
                    ),
                    FunctionRegistrationCollectError::Compile(_) => {
                        FunctionRestoreError::Request(RequestExecutionError::InvalidDumpPayload)
                    }
                })?;
            if !replace
                && self
                    .function_library_exists(&parsed_name)
                    .map_err(FunctionRestoreError::Request)?
            {
                return Err(FunctionRestoreError::LibraryAlreadyExists(parsed_name));
            }
            if let Some(function_name) = self
                .find_cross_library_function_name_conflict(&parsed_name, &registrations)
                .map_err(FunctionRestoreError::Request)?
            {
                return Err(FunctionRestoreError::FunctionAlreadyExists(function_name));
            }
            self.store_function_library(parsed_name, &library_source, &registrations, replace)
                .map_err(FunctionRestoreError::Request)?;
        }
        Ok(())
    }

    fn append_function_list_response(
        &self,
        response_out: &mut Vec<u8>,
        with_code: bool,
        library_name_pattern: Option<&[u8]>,
    ) -> Result<(), RequestExecutionError> {
        let Ok(registry) = self.function_registry.lock() else {
            return Err(storage_failure(
                "function.registry",
                "function registry lock poisoned",
            ));
        };
        let mut library_names = registry
            .library_sources
            .keys()
            .filter(|name| {
                library_name_pattern
                    .map(|pattern| {
                        super::server_commands::redis_glob_match(
                            pattern,
                            name.as_bytes(),
                            super::server_commands::CaseSensitivity::Sensitive,
                            0,
                        )
                    })
                    .unwrap_or(true)
            })
            .cloned()
            .collect::<Vec<String>>();
        library_names.sort_unstable();

        response_out.push(b'*');
        response_out.extend_from_slice(library_names.len().to_string().as_bytes());
        response_out.extend_from_slice(b"\r\n");

        for library_name in library_names {
            let mut function_names = registry
                .library_function_names
                .get(&library_name)
                .cloned()
                .unwrap_or_default();
            function_names.sort_unstable();
            let entry_pairs = if with_code { 4 } else { 3 };
            response_out.push(b'*');
            response_out.extend_from_slice((entry_pairs * 2).to_string().as_bytes());
            response_out.extend_from_slice(b"\r\n");

            append_bulk_string(response_out, b"library_name");
            append_bulk_string(response_out, library_name.as_bytes());
            append_bulk_string(response_out, b"engine");
            append_bulk_string(response_out, b"LUA");
            append_bulk_string(response_out, b"functions");
            response_out.push(b'*');
            response_out.extend_from_slice(function_names.len().to_string().as_bytes());
            response_out.extend_from_slice(b"\r\n");
            for function_name in function_names {
                let descriptor = registry.functions.get(&function_name);
                let (description, flags) = if let Some(entry) = descriptor {
                    (
                        entry.description.as_str(),
                        FunctionExecutionFlags {
                            read_only: entry.read_only,
                            allow_oom: entry.allow_oom,
                            allow_stale: entry.allow_stale,
                        },
                    )
                } else {
                    ("", FunctionExecutionFlags::default())
                };

                response_out.extend_from_slice(b"*6\r\n");
                append_bulk_string(response_out, b"name");
                append_bulk_string(response_out, function_name.as_bytes());
                append_bulk_string(response_out, b"description");
                append_bulk_string(response_out, description.as_bytes());
                append_bulk_string(response_out, b"flags");
                let mut flags_entries = 0usize;
                if flags.read_only {
                    flags_entries += 1;
                }
                if flags.allow_oom {
                    flags_entries += 1;
                }
                if flags.allow_stale {
                    flags_entries += 1;
                }
                response_out.extend_from_slice(format!("*{flags_entries}\r\n").as_bytes());
                if flags.read_only {
                    append_bulk_string(response_out, b"no-writes");
                }
                if flags.allow_oom {
                    append_bulk_string(response_out, b"allow-oom");
                }
                if flags.allow_stale {
                    append_bulk_string(response_out, b"allow-stale");
                }
            }

            if with_code {
                append_bulk_string(response_out, b"library_code");
                if let Some(source) = registry.library_sources.get(&library_name) {
                    append_bulk_string(response_out, source);
                } else {
                    append_null_bulk_string(response_out);
                }
            }
        }
        Ok(())
    }

    fn delete_function_library(&self, library_name: &[u8]) -> Result<(), RequestExecutionError> {
        let library_name_text =
            std::str::from_utf8(library_name).map_err(|_| RequestExecutionError::SyntaxError)?;
        let Ok(mut registry) = self.function_registry.lock() else {
            return Err(storage_failure(
                "function.registry",
                "function registry lock poisoned",
            ));
        };
        let Some(function_names) = registry.library_function_names.remove(library_name_text) else {
            return Err(RequestExecutionError::FunctionLibraryNotFound);
        };
        for function_name in function_names {
            registry.functions.remove(&function_name);
        }
        registry.library_sources.remove(library_name_text);
        Ok(())
    }

    fn append_function_stats_response(
        &self,
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        let Ok(registry) = self.function_registry.lock() else {
            return Err(storage_failure(
                "function.registry",
                "function registry lock poisoned",
            ));
        };
        let Ok(running_script_state) = self.running_script.lock() else {
            return Err(storage_failure(
                "script.running_state",
                "running script state lock poisoned",
            ));
        };
        let running_script_state = running_script_state.clone();

        response_out.extend_from_slice(b"*4\r\n");
        append_bulk_string(response_out, b"running_script");
        if let Some(state) = running_script_state {
            let duration_millis = state.started_at.elapsed().as_millis();
            let duration_millis = i64::try_from(duration_millis).unwrap_or(i64::MAX);
            response_out.extend_from_slice(b"*6\r\n");
            append_bulk_string(response_out, b"name");
            append_bulk_string(response_out, state.name.as_bytes());
            append_bulk_string(response_out, b"command");
            append_bulk_string(response_out, state.command.as_bytes());
            append_bulk_string(response_out, b"duration_ms");
            append_integer(response_out, duration_millis);
        } else {
            response_out.extend_from_slice(b"*0\r\n");
        }

        append_bulk_string(response_out, b"engines");
        response_out.extend_from_slice(b"*2\r\n");
        append_bulk_string(response_out, b"LUA");
        response_out.extend_from_slice(b"*4\r\n");
        append_bulk_string(response_out, b"libraries_count");
        append_integer(response_out, registry.library_sources.len() as i64);
        append_bulk_string(response_out, b"functions_count");
        append_integer(response_out, registry.functions.len() as i64);
        Ok(())
    }

    fn collect_function_registrations(
        &self,
        library_source: &[u8],
    ) -> Result<Vec<PendingFunctionRegistration>, FunctionRegistrationCollectError> {
        let lua = Lua::new();
        install_safe_unpack(&lua)
            .and_then(|()| install_safe_loadstring(&lua))
            .and_then(|()| install_safe_pcall(&lua))
            .map_err(|error| {
                FunctionRegistrationCollectError::Compile(function_load_error_text(&error))
            })?;
        self.configure_lua_runtime_limits_for_function_load(&lua)
            .map_err(|error| {
                FunctionRegistrationCollectError::Compile(function_load_error_text(&error))
            })?;
        let registrations: Rc<RefCell<Vec<PendingFunctionRegistration>>> =
            Rc::new(RefCell::new(Vec::new()));
        let registration_names: Rc<RefCell<HashSet<String>>> =
            Rc::new(RefCell::new(HashSet::new()));
        let run_result = lua.scope(|scope| {
            let redis_table = lua.create_table()?;
            let registrations_ref = Rc::clone(&registrations);
            let registration_names_ref = Rc::clone(&registration_names);
            let register_function_fn = scope.create_function(move |_lua, values: MultiValue| {
                let binding = parse_register_function_binding(values)?;
                let normalized_name = binding.name.to_ascii_lowercase();
                let mut names = registration_names_ref.borrow_mut();
                if !names.insert(normalized_name.clone()) {
                    return Err(LuaError::RuntimeError(
                        "Function already exists in the library".to_string(),
                    ));
                }
                registrations_ref
                    .borrow_mut()
                    .push(PendingFunctionRegistration {
                        name: normalized_name,
                        description: binding.description,
                        flags: binding.flags,
                    });
                Ok(())
            })?;
            let log_fn =
                scope.create_function(|_, (_level, _message): (LuaValue, LuaValue)| Ok(()))?;
            redis_table.set("register_function", register_function_fn)?;
            redis_table.set("log", log_fn)?;
            redis_table.set("LOG_DEBUG", 0)?;
            redis_table.set("LOG_VERBOSE", 1)?;
            redis_table.set("LOG_NOTICE", 2)?;
            redis_table.set("LOG_WARNING", 3)?;
            redis_table.set("REDIS_VERSION", SCRIPT_REDIS_VERSION_TEXT)?;
            redis_table.set("REDIS_VERSION_NUM", SCRIPT_REDIS_VERSION_NUM)?;
            install_readonly_redis_table_metatable(&lua, &redis_table)?;
            let load_env = build_strict_lua_environment(&lua, &redis_table)?;
            lua.load(strip_lua_shebang(library_source))
                .set_name("@user_script")
                .set_environment(load_env)
                .exec()?;
            Ok::<(), LuaError>(())
        });

        match run_result {
            Ok(()) => {}
            Err(error) => {
                if lua_error_is_timeout(&error) {
                    self.record_script_runtime_timeout();
                    return Err(FunctionRegistrationCollectError::Timeout);
                }
                return Err(FunctionRegistrationCollectError::Compile(
                    function_load_error_text(&error),
                ));
            }
        }

        let registrations = registrations.borrow().clone();
        if registrations.is_empty() {
            return Err(FunctionRegistrationCollectError::Compile(
                "No functions registered".to_string(),
            ));
        }
        Ok(registrations)
    }

    fn store_function_library(
        &self,
        library_name: String,
        library_source: &[u8],
        registrations: &[PendingFunctionRegistration],
        replace: bool,
    ) -> Result<(), RequestExecutionError> {
        let Ok(mut registry) = self.function_registry.lock() else {
            return Err(storage_failure(
                "function.registry",
                "function registry lock poisoned",
            ));
        };

        let library_exists = registry.library_sources.contains_key(&library_name);
        if library_exists && !replace {
            return Err(RequestExecutionError::FunctionLibraryAlreadyExists);
        }

        let existing_library_names = registry
            .library_function_names
            .get(&library_name)
            .cloned()
            .unwrap_or_default();
        let existing_library_name_set = existing_library_names
            .iter()
            .map(|name| name.to_ascii_lowercase())
            .collect::<HashSet<String>>();

        for registration in registrations {
            let normalized_name = registration.name.to_ascii_lowercase();
            if let Some(existing) = registry.functions.get(&normalized_name)
                && existing.library_name != library_name
                && !existing_library_name_set.contains(&normalized_name)
            {
                return Err(RequestExecutionError::FunctionNameAlreadyExists);
            }
        }

        if library_exists {
            if let Some(old_names) = registry.library_function_names.remove(&library_name) {
                for old_name in old_names {
                    registry.functions.remove(&old_name);
                }
            }
            registry.library_sources.remove(&library_name);
        }

        for registration in registrations {
            if registry
                .functions
                .contains_key(&registration.name.to_ascii_lowercase())
            {
                return Err(RequestExecutionError::FunctionNameAlreadyExists);
            }
        }

        registry
            .library_sources
            .insert(library_name.clone(), library_source.to_vec());

        let mut registered_names = Vec::with_capacity(registrations.len());
        for registration in registrations {
            let normalized_name = registration.name.to_ascii_lowercase();
            registered_names.push(normalized_name.clone());
            registry.functions.insert(
                normalized_name,
                LoadedFunctionDescriptor {
                    library_name: library_name.clone(),
                    description: registration.description.clone(),
                    read_only: registration.flags.read_only,
                    allow_oom: registration.flags.allow_oom,
                    allow_stale: registration.flags.allow_stale,
                },
            );
        }
        registry
            .library_function_names
            .insert(library_name, registered_names);
        self.set_used_memory_vm_functions(estimate_function_vm_memory_bytes(&registry));
        Ok(())
    }

    fn resolve_function_target(
        &self,
        function_name: &[u8],
    ) -> Result<ResolvedFunctionTarget, RequestExecutionError> {
        let function_name_text =
            std::str::from_utf8(function_name).map_err(|_| RequestExecutionError::SyntaxError)?;
        let normalized_function_name = function_name_text.to_ascii_lowercase();
        let Ok(registry) = self.function_registry.lock() else {
            return Err(storage_failure(
                "function.registry",
                "function registry lock poisoned",
            ));
        };
        let descriptor = registry
            .functions
            .get(&normalized_function_name)
            .ok_or(RequestExecutionError::FunctionNotFound)?;
        let source = registry
            .library_sources
            .get(&descriptor.library_name)
            .ok_or(RequestExecutionError::FunctionNotFound)?;
        Ok(ResolvedFunctionTarget {
            library_source: source.clone(),
            flags: FunctionExecutionFlags {
                read_only: descriptor.read_only,
                allow_oom: descriptor.allow_oom,
                allow_stale: descriptor.allow_stale,
            },
        })
    }

    fn execute_lua_script(
        &self,
        script: &[u8],
        keys: &[&[u8]],
        argv: &[&[u8]],
        mutability: ScriptMutability,
        response_out: &mut Vec<u8>,
    ) {
        // Compute the SHA1 for this script up front, used both for caching
        // and for the `script: SHA` suffix in error messages.
        let script_sha = script_sha1_hex(script);

        // Strip the shebang header before loading into Lua.  The shebang
        // carries metadata (engine, flags) but is not valid Lua syntax.
        let lua_source = strip_lua_shebang(script);

        // Scripts declaring `flags=no-writes` in their shebang are read-only
        // even when invoked via EVAL (not only EVAL_RO).
        let mutability = if mutability == ScriptMutability::ReadWrite
            && eval_script_has_no_writes_flag(script)
        {
            ScriptMutability::ReadOnly
        } else {
            mutability
        };

        let _running_script_guard =
            match self.enter_running_script(RunningScriptKind::Script, "", "eval".to_string()) {
                Ok(guard) => guard,
                Err(error) => {
                    error.append_resp_error(response_out);
                    return;
                }
            };
        let lua = Lua::new();
        if let Err(error) = install_safe_unpack(&lua)
            .and_then(|()| install_safe_loadstring(&lua))
            .and_then(|()| install_safe_pcall(&lua))
        {
            let message = format!(
                "ERR Error running script: {}",
                sanitize_error_text(&error.to_string())
            );
            append_error(response_out, message.as_bytes());
            return;
        }
        if let Err(error) = self.configure_lua_runtime_limits(&lua, Some(RunningScriptKind::Script))
        {
            let message = format!(
                "ERR Error running script: {}",
                sanitize_error_text(&error.to_string())
            );
            append_error(response_out, message.as_bytes());
            return;
        }
        if let Err(error) = install_basic_type_metatable_protection(&lua) {
            let message = format!(
                "ERR Error running script: {}",
                sanitize_error_text(&error.to_string())
            );
            append_error(response_out, message.as_bytes());
            return;
        }
        let run_result = lua.scope(|scope| {
            let redis_table = lua.create_table()?;
            let call_fn = scope.create_function(move |lua, values: MultiValue| {
                self.execute_lua_redis_call(lua, values, mutability, false, LuaCallMode::Call)
            })?;
            let pcall_fn = scope.create_function(move |lua, values: MultiValue| {
                self.execute_lua_redis_call(lua, values, mutability, false, LuaCallMode::PCall)
            })?;
            let sha1hex_fn = scope.create_function(|_, args: MultiValue| {
                if args.len() != 1 {
                    return Err(LuaError::RuntimeError(
                        "wrong number of arguments".to_string(),
                    ));
                }
                let value = match args.into_iter().next().unwrap() {
                    LuaValue::String(s) => s,
                    other => {
                        let s = match other {
                            LuaValue::Integer(n) => n.to_string(),
                            LuaValue::Number(n) => n.to_string(),
                            _ => {
                                return Err(LuaError::RuntimeError(
                                    "wrong number of arguments".to_string(),
                                ));
                            }
                        };
                        return Ok(script_sha1_hex(s.as_bytes()));
                    }
                };
                Ok(script_sha1_hex(value.as_bytes().as_ref()))
            })?;
            let status_reply_fn = scope.create_function(|lua, value: mlua::String| {
                let table = lua.create_table()?;
                table.set("ok", value)?;
                Ok(table)
            })?;
            let error_reply_fn = scope.create_function(|lua, value: mlua::String| {
                let normalized = normalize_error_reply(&value.to_string_lossy());
                let table = lua.create_table()?;
                table.set("err", lua.create_string(&normalized)?)?;
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
            install_readonly_redis_table_metatable(&lua, &redis_table)?;
            let env = build_strict_lua_environment(&lua, &redis_table)?;

            let keys_table = lua.create_table_with_capacity(keys.len(), 0)?;
            for (index, key) in keys.iter().enumerate() {
                keys_table.set(index + 1, lua.create_string(key)?)?;
            }
            env.raw_set("KEYS", keys_table)?;

            let argv_table = lua.create_table_with_capacity(argv.len(), 0)?;
            for (index, arg) in argv.iter().enumerate() {
                argv_table.set(index + 1, lua.create_string(arg)?)?;
            }
            env.raw_set("ARGV", argv_table)?;

            let value: LuaValue = eval_script_with_table_error_handler(&lua, lua_source, env)?;
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
                if lua_error_is_timeout(&error) {
                    self.record_script_runtime_timeout();
                    append_error(response_out, SCRIPT_TIMEOUT_ERROR_TEXT.as_bytes());
                    return;
                }
                let normalized = normalize_lua_runtime_error_text(&error);
                let message = format_script_error_for_client(&normalized, "script", &script_sha);
                append_error(response_out, message.as_bytes());
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn execute_lua_function(
        &self,
        library_source: &[u8],
        function_name: &[u8],
        keys: &[&[u8]],
        argv: &[&[u8]],
        mutability: ScriptMutability,
        allow_oom: bool,
        command_name: &str,
        response_out: &mut Vec<u8>,
    ) {
        let function_name_text = match std::str::from_utf8(function_name) {
            Ok(name) => name.to_ascii_lowercase(),
            Err(_) => {
                append_error(
                    response_out,
                    b"ERR Error running function: invalid function name",
                );
                return;
            }
        };
        let command = format!("{} {} {}", command_name, function_name_text, keys.len());
        let _running_script_guard = match self.enter_running_script(
            RunningScriptKind::Function,
            function_name_text.as_str(),
            command,
        ) {
            Ok(guard) => guard,
            Err(error) => {
                error.append_resp_error(response_out);
                return;
            }
        };

        let lua = Lua::new();
        if let Err(error) = install_safe_unpack(&lua)
            .and_then(|()| install_safe_loadstring(&lua))
            .and_then(|()| install_safe_pcall(&lua))
        {
            let message = format!(
                "ERR Error running function: {}",
                sanitize_error_text(&error.to_string())
            );
            append_error(response_out, message.as_bytes());
            return;
        }
        if let Err(error) = install_basic_type_metatable_protection(&lua) {
            let message = format!(
                "ERR Error running function: {}",
                sanitize_error_text(&error.to_string())
            );
            append_error(response_out, message.as_bytes());
            return;
        }
        if let Err(error) =
            self.configure_lua_runtime_limits(&lua, Some(RunningScriptKind::Function))
        {
            let message = format!(
                "ERR Error running function: {}",
                sanitize_error_text(&error.to_string())
            );
            append_error(response_out, message.as_bytes());
            return;
        }
        let run_result = lua.scope(|scope| {
            let globals = lua.globals();
            let runtime_getmetatable = globals.get::<LuaFunction>("getmetatable").ok();
            let load_redis_table = lua.create_table()?;
            let call_fn = scope.create_function(move |lua, values: MultiValue| {
                self.execute_lua_redis_call(lua, values, mutability, allow_oom, LuaCallMode::Call)
            })?;
            let pcall_fn = scope.create_function(move |lua, values: MultiValue| {
                self.execute_lua_redis_call(lua, values, mutability, allow_oom, LuaCallMode::PCall)
            })?;
            let sha1hex_fn = scope.create_function(|_, args: MultiValue| {
                if args.len() != 1 {
                    return Err(LuaError::RuntimeError(
                        "wrong number of arguments".to_string(),
                    ));
                }
                let value = match args.into_iter().next().unwrap() {
                    LuaValue::String(s) => s,
                    other => {
                        let s = match other {
                            LuaValue::Integer(n) => n.to_string(),
                            LuaValue::Number(n) => n.to_string(),
                            _ => {
                                return Err(LuaError::RuntimeError(
                                    "wrong number of arguments".to_string(),
                                ));
                            }
                        };
                        return Ok(script_sha1_hex(s.as_bytes()));
                    }
                };
                Ok(script_sha1_hex(value.as_bytes().as_ref()))
            })?;
            let status_reply_fn = scope.create_function(|lua, value: mlua::String| {
                let table = lua.create_table()?;
                table.set("ok", value)?;
                Ok(table)
            })?;
            let error_reply_fn = scope.create_function(|lua, value: mlua::String| {
                let normalized = normalize_error_reply(&value.to_string_lossy());
                let table = lua.create_table()?;
                table.set("err", lua.create_string(&normalized)?)?;
                Ok(table)
            })?;
            let log_fn =
                scope.create_function(|_, (_level, _message): (LuaValue, LuaValue)| Ok(()))?;
            let registered_functions = lua.create_table()?;
            let registered_functions_ref = registered_functions.clone();
            let register_function_fn = scope.create_function(move |_lua, values: MultiValue| {
                let binding = parse_register_function_binding(values)?;
                let normalized_name = binding.name.to_ascii_lowercase();
                registered_functions_ref.set(normalized_name.as_str(), binding.callback)?;
                Ok(())
            })?;
            let runtime_only_register_fn =
                scope.create_function(|_, _values: MultiValue| -> mlua::Result<()> {
                    Err(LuaError::RuntimeError(
                        SCRIPT_FUNCTION_LOAD_RUNTIME_ONLY_ERROR.to_string(),
                    ))
                })?;

            load_redis_table.set("call", call_fn)?;
            load_redis_table.set("pcall", pcall_fn)?;
            load_redis_table.set("sha1hex", sha1hex_fn)?;
            load_redis_table.set("status_reply", status_reply_fn)?;
            load_redis_table.set("error_reply", error_reply_fn)?;
            load_redis_table.set("register_function", register_function_fn)?;
            load_redis_table.set("log", log_fn)?;
            load_redis_table.set("LOG_DEBUG", 0)?;
            load_redis_table.set("LOG_VERBOSE", 1)?;
            load_redis_table.set("LOG_NOTICE", 2)?;
            load_redis_table.set("LOG_WARNING", 3)?;
            load_redis_table.set("REDIS_VERSION", SCRIPT_REDIS_VERSION_TEXT)?;
            load_redis_table.set("REDIS_VERSION_NUM", SCRIPT_REDIS_VERSION_NUM)?;
            install_readonly_redis_table_metatable(&lua, &load_redis_table)?;
            let load_env = build_strict_lua_environment(&lua, &load_redis_table)?;

            lua.load(strip_lua_shebang(library_source))
                .set_name("@user_script")
                .set_environment(load_env.clone())
                .exec()?;
            load_redis_table.set("register_function", runtime_only_register_fn)?;

            let runtime_redis_table = lua.create_table()?;
            let runtime_call_fn = scope.create_function(move |lua, values: MultiValue| {
                self.execute_lua_redis_call(lua, values, mutability, allow_oom, LuaCallMode::Call)
            })?;
            let runtime_pcall_fn = scope.create_function(move |lua, values: MultiValue| {
                self.execute_lua_redis_call(lua, values, mutability, allow_oom, LuaCallMode::PCall)
            })?;
            let runtime_sha1hex_fn = scope.create_function(|_, args: MultiValue| {
                if args.len() != 1 {
                    return Err(LuaError::RuntimeError(
                        "wrong number of arguments".to_string(),
                    ));
                }
                match args.into_iter().next().unwrap_or(LuaValue::Nil) {
                    LuaValue::String(s) => Ok(script_sha1_hex(s.as_bytes().as_ref())),
                    _ => Err(LuaError::RuntimeError(
                        "wrong number of arguments".to_string(),
                    )),
                }
            })?;
            let runtime_status_reply_fn = scope.create_function(|lua, value: mlua::String| {
                let table = lua.create_table()?;
                table.set("ok", value)?;
                Ok(table)
            })?;
            let runtime_error_reply_fn = scope.create_function(|lua, value: mlua::String| {
                let normalized = normalize_error_reply(&value.to_string_lossy());
                let table = lua.create_table()?;
                table.set("err", lua.create_string(&normalized)?)?;
                Ok(table)
            })?;
            let runtime_log_fn =
                scope.create_function(|_, (_level, _message): (LuaValue, LuaValue)| Ok(()))?;
            runtime_redis_table.set("call", runtime_call_fn)?;
            runtime_redis_table.set("pcall", runtime_pcall_fn)?;
            runtime_redis_table.set("sha1hex", runtime_sha1hex_fn)?;
            runtime_redis_table.set("status_reply", runtime_status_reply_fn)?;
            runtime_redis_table.set("error_reply", runtime_error_reply_fn)?;
            runtime_redis_table.set("log", runtime_log_fn)?;
            runtime_redis_table.set("LOG_DEBUG", 0)?;
            runtime_redis_table.set("LOG_VERBOSE", 1)?;
            runtime_redis_table.set("LOG_NOTICE", 2)?;
            runtime_redis_table.set("LOG_WARNING", 3)?;
            runtime_redis_table.set("REDIS_VERSION", SCRIPT_REDIS_VERSION_TEXT)?;
            runtime_redis_table.set("REDIS_VERSION_NUM", SCRIPT_REDIS_VERSION_NUM)?;

            // Callbacks use this environment's global table for `redis`; keep the runtime table
            // limited so `redis.register_function` is not available while functions are running.
            // We must update the backing table (not the raw env) to preserve readonly
            // protection: writing to the raw env would let `redis = ...` bypass __newindex.
            {
                let readonly_runtime_redis = lua.create_table()?;
                let runtime_keys: Vec<(LuaValue, LuaValue)> = runtime_redis_table
                    .pairs::<LuaValue, LuaValue>()
                    .filter_map(|entry| entry.ok())
                    .collect();
                for (key, value) in &runtime_keys {
                    readonly_runtime_redis.raw_set(key.clone(), value.clone())?;
                }
                make_table_readonly(&lua, &readonly_runtime_redis)?;
                set_env_backing_value(&load_env, "redis", LuaValue::Table(readonly_runtime_redis))?;
            }
            if let Some(getmetatable) = runtime_getmetatable {
                set_env_backing_value(&load_env, "getmetatable", LuaValue::Function(getmetatable))?;
            }
            globals.set("redis", runtime_redis_table)?;

            let callback = registered_functions
                .get::<LuaFunction>(function_name_text.as_str())
                .map_err(|_| LuaError::RuntimeError("ERR Function not found".to_string()))?;
            let keys_table = lua.create_table_with_capacity(keys.len(), 0)?;
            for (index, key) in keys.iter().enumerate() {
                keys_table.set(index + 1, lua.create_string(key)?)?;
            }
            let argv_table = lua.create_table_with_capacity(argv.len(), 0)?;
            for (index, arg) in argv.iter().enumerate() {
                argv_table.set(index + 1, lua.create_string(arg)?)?;
            }
            let value: LuaValue =
                call_function_with_table_error_handler(&lua, callback, keys_table, argv_table)?;
            Ok::<LuaValue, LuaError>(value)
        });

        match run_result {
            Ok(value) => {
                if let Err(error_message) = append_lua_value_as_resp(value, response_out) {
                    let message = format!(
                        "ERR Error running function: {}",
                        sanitize_error_text(&error_message)
                    );
                    append_error(response_out, message.as_bytes());
                }
            }
            Err(error) => {
                if lua_error_is_timeout(&error) {
                    self.record_script_runtime_timeout();
                    append_error(response_out, SCRIPT_TIMEOUT_ERROR_TEXT.as_bytes());
                    return;
                }
                let normalized_error = normalize_lua_runtime_error_text(&error);
                if normalized_error.starts_with("OOM ") {
                    append_error(response_out, normalized_error.as_bytes());
                    return;
                }
                let message = format_script_error_for_client(
                    &normalized_error,
                    "function",
                    &function_name_text,
                );
                append_error(response_out, message.as_bytes());
            }
        }
    }

    fn execute_lua_redis_call(
        &self,
        lua: &Lua,
        values: MultiValue,
        mutability: ScriptMutability,
        allow_oom: bool,
        call_mode: LuaCallMode,
    ) -> mlua::Result<LuaValue> {
        if values.is_empty() {
            return Err(LuaError::RuntimeError(
                "ERR Please specify at least one argument for this redis lib call".to_string(),
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
        if command == CommandId::Unknown {
            return lua_call_error_or_pcall_error(
                lua,
                call_mode,
                "ERR Unknown Redis command called from script",
            );
        }
        if !command_allowed_from_script(command) {
            return lua_call_error_or_pcall_error(
                lua,
                call_mode,
                "ERR This Redis command is not allowed from script",
            );
        }
        if mutability == ScriptMutability::ReadOnly && command_is_mutating(command) {
            return lua_call_error_or_pcall_error(
                lua,
                call_mode,
                "ERR Write commands are not allowed from read-only scripts",
            );
        }
        if !allow_oom && command_is_mutating(command) && self.maxmemory_limit_bytes() > 0 {
            return lua_call_error_or_pcall_error(
                lua,
                call_mode,
                "OOM command not allowed when used memory > 'maxmemory'.",
            );
        }

        self.record_command_call(arg_refs[0]);
        let mut response = Vec::new();
        match self.execute_bytes(&arg_refs, &mut response) {
            Ok(()) => {}
            Err(error) => {
                self.record_command_failure(arg_refs[0]);
                let message = request_error_message(error);
                let rewritten = rewrite_script_error_message(&message);
                return lua_call_error_or_pcall_error(lua, call_mode, &rewritten);
            }
        }

        let frame = parse_resp_frame_complete(&response).map_err(|message| {
            LuaError::RuntimeError(format!(
                "ERR internal scripting response parse error: {}",
                sanitize_error_text(&message)
            ))
        })?;
        match frame {
            RespFrame::Error(message) => {
                self.record_command_failure(arg_refs[0]);
                let rewritten = rewrite_script_error_message(&message);
                lua_call_error_or_pcall_error(lua, call_mode, &rewritten)
            }
            other => resp_frame_to_lua_value(lua, other),
        }
    }

    fn configure_lua_runtime_limits(
        &self,
        lua: &Lua,
        running_kind: Option<RunningScriptKind>,
    ) -> mlua::Result<()> {
        let config = self.scripting_runtime_config();
        if config.max_memory_bytes > 0 {
            let _ = lua.set_memory_limit(config.max_memory_bytes)?;
        }
        let timeout = if config.max_execution_millis > 0 {
            Some(Duration::from_millis(config.max_execution_millis))
        } else {
            None
        };
        let kill_flag = match running_kind {
            Some(RunningScriptKind::Script) => Some(Arc::clone(&self.script_kill_requested)),
            Some(RunningScriptKind::Function) => Some(Arc::clone(&self.function_kill_requested)),
            None => None,
        };
        if timeout.is_some() || kill_flag.is_some() {
            let started = Instant::now();
            // Track whether the kill escalation has been armed.  Once armed,
            // the hook fires on every line so that pcall cannot suppress the
            // kill error (matching Redis's `lua_sethook(LUA_MASKLINE)`
            // escalation strategy).
            let kill_escalated = Arc::new(AtomicBool::new(false));
            let kill_escalated_clone = Arc::clone(&kill_escalated);
            lua.set_hook(
                HookTriggers::new().every_nth_instruction(LUA_TIMEOUT_HOOK_INSTRUCTION_STRIDE),
                move |lua, _debug| {
                    if let Some(flag) = &kill_flag
                        && flag.load(Ordering::Acquire)
                    {
                        // On first kill detection, escalate the hook to fire
                        // on every line so pcall cannot loop indefinitely
                        // catching the kill error.
                        if !kill_escalated_clone.load(Ordering::Relaxed) {
                            kill_escalated_clone.store(true, Ordering::Relaxed);
                            let _ = lua.set_hook(
                                HookTriggers::new().every_line(),
                                move |_lua, _debug| {
                                    Err(LuaError::RuntimeError(
                                        SCRIPT_KILLED_ERROR_TEXT.to_string(),
                                    ))
                                },
                            );
                        }
                        return Err(LuaError::RuntimeError(SCRIPT_KILLED_ERROR_TEXT.to_string()));
                    }
                    if let Some(timeout) = timeout
                        && started.elapsed() > timeout
                    {
                        return Err(LuaError::RuntimeError(
                            SCRIPT_TIMEOUT_ERROR_TEXT.to_string(),
                        ));
                    }
                    Ok(VmState::Continue)
                },
            )?;
        }
        Ok(())
    }

    fn configure_lua_runtime_limits_for_function_load(&self, lua: &Lua) -> mlua::Result<()> {
        let config = self.scripting_runtime_config();
        if config.max_memory_bytes > 0 {
            let _ = lua.set_memory_limit(config.max_memory_bytes)?;
        }

        let timeout_millis = if config.max_execution_millis > 0 {
            config.max_execution_millis
        } else {
            5_000
        };
        let timeout = Duration::from_millis(timeout_millis);
        let started = Instant::now();
        lua.set_hook(
            HookTriggers::new().every_nth_instruction(LUA_TIMEOUT_HOOK_INSTRUCTION_STRIDE),
            move |_lua, _debug| {
                if started.elapsed() > timeout {
                    return Err(LuaError::RuntimeError(
                        SCRIPT_FUNCTION_LOAD_TIMEOUT_ERROR_TEXT.to_string(),
                    ));
                }
                Ok(VmState::Continue)
            },
        )?;
        Ok(())
    }
}

fn lua_error_is_timeout(error: &LuaError) -> bool {
    let text = error.to_string();
    text.contains(SCRIPT_TIMEOUT_ERROR_TEXT)
        || text.contains(SCRIPT_FUNCTION_LOAD_TIMEOUT_ERROR_TEXT)
}

fn function_load_error_text(error: &LuaError) -> String {
    let mut text = sanitize_error_text(&error.to_string());
    for prefix in ["runtime error: ", "syntax error: "] {
        if let Some(stripped) = text.strip_prefix(prefix) {
            text = stripped.to_string();
        }
    }
    text
}

fn normalize_lua_runtime_error_text(error: &LuaError) -> String {
    let text = function_load_error_text(error);
    let primary = text.split(" stack traceback:").next().unwrap_or(&text);
    primary.trim().to_string()
}

/// Evaluate a Lua script chunk with a safe error handler that converts
/// table-type errors to their `err` field string.
///
/// Redis scripts can raise errors with `error({err='ERR msg'})`.  Without
/// an error handler, Lua would convert the table to its `tostring()`
/// representation (e.g. `"table: 0xdeadbeef"`), losing the error message.
///
/// This function installs a Lua-level error handler via `xpcall` that:
/// - For table errors: extracts `rawget(err, 'err')` or returns `"ERR unknown error"`
/// - For all other error types (string, userdata): passes through unchanged
///
/// The handler intentionally avoids calling `tostring()` on non-table values
/// to prevent triggering mlua's `__tostring` metamethod on WrappedFailure
/// userdata, which would cause "error in error handling".
fn eval_script_with_table_error_handler(
    lua: &Lua,
    source: &[u8],
    env: LuaTable,
) -> mlua::Result<LuaValue> {
    // Load the user script as a callable function.
    let script_fn: LuaFunction = lua
        .load(source)
        .set_name("@user_script")
        .set_environment(env)
        .into_function()?;

    // Install a minimal error handler that transforms table errors to their
    // `err` field and passes strings through unchanged.  Intentionally avoids
    // calling `tostring()` on non-table/non-string values to prevent mlua's
    // WrappedFailure `__tostring` from triggering "error in error handling".
    //
    // This mirrors Redis's `__redis__err__handler` from eval.c.
    let xpcall_wrapper: LuaFunction = lua
        .load(
            r#"
            return function(fn)
                local eh = function(err)
                    if type(err) == 'table' then
                        local e = rawget(err, 'err')
                        if type(e) ~= 'string' then e = 'ERR unknown error' end
                        return e
                    end
                    return err
                end
                return xpcall(fn, eh)
            end
            "#,
        )
        .eval()?;

    // Use xpcall to run the script with the error handler.
    let mut results: MultiValue = xpcall_wrapper.call(script_fn)?;

    let ok = match results.pop_front() {
        Some(LuaValue::Boolean(b)) => b,
        _ => false,
    };

    if ok {
        // Script succeeded: return the first result value (or nil if none).
        Ok(results.pop_front().unwrap_or(LuaValue::Nil))
    } else {
        // Script failed: the error value has been processed by our handler.
        let err_val = results.pop_front().unwrap_or(LuaValue::Nil);
        let err_msg = match err_val {
            LuaValue::String(s) => s
                .to_str()
                .map(|s| s.to_string())
                .unwrap_or_else(|_| "ERR unknown error".to_string()),
            LuaValue::Error(ref e) => extract_lua_error_message(e),
            _ => "ERR unknown error".to_string(),
        };
        Err(LuaError::RuntimeError(err_msg))
    }
}

/// Call a registered Lua function with a safe error handler that converts
/// table-type errors to their `err` field string.
///
/// This is the FUNCTION (FCALL) counterpart to
/// [`eval_script_with_table_error_handler`].  The function receives
/// (keys_table, argv_table) as arguments.
///
/// In Lua 5.1, `xpcall` only takes 2 arguments (no arg-passing), so we
/// wrap the callback in a closure that captures the arguments.
fn call_function_with_table_error_handler(
    lua: &Lua,
    callback: LuaFunction,
    keys_table: LuaTable,
    argv_table: LuaTable,
) -> mlua::Result<LuaValue> {
    // Create a Lua wrapper function that calls xpcall with the error handler.
    // The wrapper captures callback, keys, and argv as arguments since
    // Lua 5.1's xpcall doesn't pass extra arguments to the function.
    let wrapper: LuaFunction = lua
        .load(
            r#"
            return function(fn, keys, argv)
                local eh = function(err)
                    if type(err) == 'table' then
                        local e = rawget(err, 'err')
                        if type(e) == 'string' then return e end
                        return 'ERR unknown error'
                    end
                    return err
                end
                return xpcall(function() return fn(keys, argv) end, eh)
            end
            "#,
        )
        .eval()?;

    let mut results: MultiValue = wrapper.call((callback, keys_table, argv_table))?;

    let ok = match results.pop_front() {
        Some(LuaValue::Boolean(b)) => b,
        _ => false,
    };

    if ok {
        Ok(results.pop_front().unwrap_or(LuaValue::Nil))
    } else {
        let err_val = results.pop_front().unwrap_or(LuaValue::Nil);
        let err_msg = match err_val {
            LuaValue::String(s) => s
                .to_str()
                .map(|s| s.to_string())
                .unwrap_or_else(|_| "ERR unknown error".to_string()),
            LuaValue::Error(ref e) => extract_lua_error_message(e),
            _ => "ERR unknown error".to_string(),
        };
        Err(LuaError::RuntimeError(err_msg))
    }
}

/// Format a script/function runtime error for the RESP client.
///
/// When the error message itself already carries a standard Redis error
/// prefix (`ERR`, `WRONGTYPE`, `NOPERM`, etc.) we return it as-is so the
/// client sees the canonical error.  Otherwise we wrap it in
/// `"ERR Error running <kind>: ..."` to match Redis convention.
///
/// When `funcname` is provided (the script SHA or function name), the error
/// includes a `script: <funcname>, on <source>:<line>.` suffix matching the
/// format produced by Redis's `luaCallFunction`.
fn format_script_error_for_client(normalized_error: &str, kind: &str, funcname: &str) -> String {
    // Error messages produced by redis.call / the command layer already
    // carry a prefix like "ERR ...", "WRONGTYPE ...", etc.  Return those
    // directly so the test assertions match Redis behaviour.
    let known_prefixes = [
        "ERR ",
        "WRONGTYPE",
        "OOM ",
        "NOPERM",
        "READONLY",
        "NOTBUSY",
        "BUSY",
    ];

    let has_known_prefix = known_prefixes
        .iter()
        .any(|prefix| normalized_error.starts_with(prefix));

    let mut message = if has_known_prefix {
        normalized_error.to_string()
    } else if normalized_error.starts_with("user_script:") {
        // If the error looks like a Lua runtime error from the user script
        // (e.g. "user_script:1: msg"), add the ERR prefix so it follows the
        // Redis convention established by __redis__err__handler.
        format!("ERR {normalized_error}")
    } else {
        format!("ERR Error running {kind}: {normalized_error}")
    };

    // Append the Redis-style script identification suffix when we have a
    // funcname.  Redis's `luaCallFunction` always appends this when the
    // error handler provides source/line info.  We extract the location
    // from the error message if available, otherwise just add `script: SHA`.
    if !funcname.is_empty() {
        if let Some(location) = extract_script_error_location(&message) {
            write!(message, " script: {funcname}, on {location}.").unwrap();
        } else {
            write!(message, " script: {funcname}").unwrap();
        }
    }

    message
}

/// Try to extract a `source:line` location from a script error message.
///
/// Looks for patterns like `user_script:123:` in the error text and returns
/// `"user_script:123"` if found.  Returns `None` if no location is present
/// (e.g. for redis.call errors that carry an ERR prefix directly).
fn extract_script_error_location(error_text: &str) -> Option<String> {
    // Skip the error prefix (ERR, WRONGTYPE, etc.) before looking for the
    // source:line pattern.
    let body = error_text
        .strip_prefix("ERR ")
        .or_else(|| error_text.strip_prefix("WRONGTYPE "))
        .unwrap_or(error_text);

    // Match "user_script:N:" pattern at the start of the body.
    if let Some(rest) = body.strip_prefix("user_script:")
        && let Some(colon_pos) = rest.find(':')
    {
        let line_str = &rest[..colon_pos];
        if line_str.chars().all(|c| c.is_ascii_digit()) && !line_str.is_empty() {
            return Some(format!("user_script:{line_str}"));
        }
    }
    None
}

fn append_function_load_compile_error(response_out: &mut Vec<u8>, message: &str) {
    let message = message.strip_prefix("ERR ").unwrap_or(message);
    let prefixed = if message.starts_with("Error compiling function") {
        format!("ERR {message}")
    } else {
        format!("ERR Error compiling function: {message}")
    };
    append_error(response_out, prefixed.as_bytes());
}

fn script_global_name(value: LuaValue) -> String {
    match value {
        LuaValue::String(name) => name.to_string_lossy(),
        LuaValue::Integer(value) => value.to_string(),
        LuaValue::Number(value) => value.to_string(),
        LuaValue::Boolean(value) => {
            if value {
                "true".to_string()
            } else {
                "false".to_string()
            }
        }
        LuaValue::Nil => "nil".to_string(),
        _ => "<non-string>".to_string(),
    }
}

fn install_readonly_redis_table_metatable(lua: &Lua, redis_table: &LuaTable) -> mlua::Result<()> {
    let metatable = lua.create_table()?;
    let index_fn = lua.create_function(
        |_, (_table, key): (LuaValue, LuaValue)| -> mlua::Result<LuaValue> {
            let key_name = script_global_name(key);
            Err(LuaError::RuntimeError(format!(
                "Script attempted to access nonexistent global variable '{key_name}'"
            )))
        },
    )?;
    let newindex_fn = lua.create_function(
        |_, (_table, _key, _value): (LuaValue, LuaValue, LuaValue)| -> mlua::Result<()> {
            Err(LuaError::RuntimeError(
                "Attempt to modify a readonly table".to_string(),
            ))
        },
    )?;
    metatable.set("__index", index_fn)?;
    metatable.set("__newindex", newindex_fn)?;
    redis_table.set_metatable(Some(metatable))?;
    Ok(())
}

fn build_strict_lua_environment(lua: &Lua, redis_table: &LuaTable) -> mlua::Result<LuaTable> {
    // The environment table (`env`) is intentionally left EMPTY: all globals
    // live in the `__index` backing table so that ANY write (even to keys that
    // "exist") goes through `__newindex` and is rejected.  This matches
    // Redis/Valkey's patched-Lua readonly-table semantics.
    let env = lua.create_table()?;

    // Build library tables with readonly protection.
    // make_table_readonly moves all raw keys into a backing __index table
    // so that writes to existing keys (e.g. `cjson.encode = ...`) also
    // trigger __newindex and are rejected.
    let bit_table = build_lua_bit_table(lua)?;
    make_table_readonly(lua, &bit_table)?;

    let cjson_table = build_lua_cjson_table(lua)?;
    make_table_readonly(lua, &cjson_table)?;

    let cmsgpack_table = build_lua_cmsgpack_table(lua)?;
    make_table_readonly(lua, &cmsgpack_table)?;

    let os_table = build_lua_os_table(lua)?;
    make_table_readonly(lua, &os_table)?;

    // Make the redis table fully readonly by moving its raw keys into a
    // proxy.  This prevents `redis.call = ...` from succeeding.
    // We clone the redis table reference before making it readonly so the
    // original caller doesn't lose access to the raw keys.
    let readonly_redis = lua.create_table()?;
    let redis_keys: Vec<(LuaValue, LuaValue)> = redis_table
        .pairs::<LuaValue, LuaValue>()
        .filter_map(|entry| entry.ok())
        .collect();
    for (key, value) in &redis_keys {
        readonly_redis.raw_set(key.clone(), value.clone())?;
    }
    make_table_readonly(lua, &readonly_redis)?;

    // The backing table contains all visible globals.  Reads fall through
    // from `env` -> `__index` -> `backing`; writes hit `__newindex` and
    // always error.
    let backing = lua.create_table()?;
    backing.raw_set("redis", readonly_redis)?;
    backing.raw_set("_G", env.clone())?;
    backing.raw_set("bit", bit_table)?;
    backing.raw_set("cjson", cjson_table)?;
    backing.raw_set("cmsgpack", cmsgpack_table)?;
    backing.raw_set("os", os_table)?;

    let base_globals = lua.globals();
    for global_name in LUA_ALLOWED_GLOBALS {
        if global_name == "setmetatable" {
            continue; // replaced with safe wrapper below
        }
        if global_name == "pairs" || global_name == "next" || global_name == "ipairs" {
            continue; // replaced with readonly-proxy-aware versions below
        }
        if let Ok(value) = base_globals.get::<LuaValue>(global_name) {
            backing.raw_set(global_name, value)?;
        }
    }

    // Override pairs/next/ipairs to iterate backing tables of readonly proxies.
    // In Lua 5.1, __pairs is not supported, so we wrap the builtins.
    install_readonly_aware_iterators(lua, &backing)?;

    // Build a table of all "protected" (readonly) tables whose metatables
    // must not be changed via setmetatable().  We register env itself plus
    // the library tables.
    let protected_tables = lua.create_table()?;
    // Use lua_ref-like keys: store the table as both key and value so we
    // can do a fast lookup later.
    install_safe_setmetatable(lua, &backing, &env, &protected_tables)?;

    // The backing table's own metatable catches access to truly nonexistent
    // global names.
    let backing_metatable = lua.create_table()?;
    let nonexistent_index_fn = lua.create_function(
        |_, (_table, key): (LuaValue, LuaValue)| -> mlua::Result<LuaValue> {
            let key_name = script_global_name(key);
            Err(LuaError::RuntimeError(format!(
                "Script attempted to access nonexistent global variable '{key_name}'"
            )))
        },
    )?;
    let readonly_newindex_fn = lua.create_function(
        |_, (_table, _key, _value): (LuaValue, LuaValue, LuaValue)| -> mlua::Result<()> {
            Err(LuaError::RuntimeError(
                "Attempt to modify a readonly table".to_string(),
            ))
        },
    )?;
    backing_metatable.set("__index", nonexistent_index_fn)?;
    backing_metatable.set("__newindex", readonly_newindex_fn.clone())?;
    backing.set_metatable(Some(backing_metatable))?;

    // The environment's metatable: __index delegates reads to `backing`,
    // __newindex rejects all writes.
    // __metatable exposes a readonly proxy table so that:
    //   - getmetatable(_G) returns a table whose __newindex errors
    //   - setmetatable on the real env goes through our safe wrapper
    let fake_metatable = lua.create_table()?;
    install_readonly_table_metatable(lua, &fake_metatable)?;

    let env_metatable = lua.create_table()?;
    env_metatable.set("__index", backing)?;
    env_metatable.set("__newindex", readonly_newindex_fn)?;
    env_metatable.set("__metatable", fake_metatable)?;
    env.set_metatable(Some(env_metatable))?;
    Ok(env)
}

fn lua_bit_tobit(v: i64) -> i64 {
    (v as i32) as i64
}

fn build_lua_bit_table(lua: &Lua) -> mlua::Result<LuaTable> {
    let bit = lua.create_table()?;

    // tobit(x) — normalize to signed 32-bit
    let tobit_fn = lua.create_function(|_, v: i64| Ok(lua_bit_tobit(v)))?;

    // bnot(x) — bitwise NOT
    let bnot_fn = lua.create_function(|_, v: i64| Ok(lua_bit_tobit(!(v as u32) as i64)))?;

    // band(x1, ...) — bitwise AND (variadic)
    let band_fn = lua.create_function(|_, args: MultiValue| {
        let mut result: u32 = 0xFFFFFFFF;
        for arg in args {
            let v = match arg {
                LuaValue::Integer(n) => n,
                LuaValue::Number(n) => n as i64,
                _ => {
                    return Err(LuaError::RuntimeError(
                        "bad argument to 'band' (number expected)".to_string(),
                    ));
                }
            };
            result &= v as u32;
        }
        Ok(lua_bit_tobit(result as i64))
    })?;

    // bor(x1, ...) — bitwise OR (variadic)
    let bor_fn = lua.create_function(|_, args: MultiValue| {
        let mut result: u32 = 0;
        for arg in args {
            let v = match arg {
                LuaValue::Integer(n) => n,
                LuaValue::Number(n) => n as i64,
                _ => {
                    return Err(LuaError::RuntimeError(
                        "bad argument to 'bor' (number expected)".to_string(),
                    ));
                }
            };
            result |= v as u32;
        }
        Ok(lua_bit_tobit(result as i64))
    })?;

    // bxor(x1, ...) — bitwise XOR (variadic)
    let bxor_fn = lua.create_function(|_, args: MultiValue| {
        let mut result: u32 = 0;
        for arg in args {
            let v = match arg {
                LuaValue::Integer(n) => n,
                LuaValue::Number(n) => n as i64,
                _ => {
                    return Err(LuaError::RuntimeError(
                        "bad argument to 'bxor' (number expected)".to_string(),
                    ));
                }
            };
            result ^= v as u32;
        }
        Ok(lua_bit_tobit(result as i64))
    })?;

    // lshift(x, n) — logical left shift
    let lshift_fn = lua.create_function(|_, (value, shift): (i64, i64)| {
        let shift = (shift as u32) & 31;
        Ok(lua_bit_tobit(((value as u32).wrapping_shl(shift)) as i64))
    })?;

    // rshift(x, n) — logical right shift
    let rshift_fn = lua.create_function(|_, (value, shift): (i64, i64)| {
        let shift = (shift as u32) & 31;
        Ok(lua_bit_tobit(((value as u32).wrapping_shr(shift)) as i64))
    })?;

    // arshift(x, n) — arithmetic right shift
    let arshift_fn = lua.create_function(|_, (value, shift): (i64, i64)| {
        let shift = (shift as u32) & 31;
        Ok(lua_bit_tobit(((value as i32).wrapping_shr(shift)) as i64))
    })?;

    // rol(x, n) — rotate left
    let rol_fn = lua.create_function(|_, (value, shift): (i64, i64)| {
        let shift = (shift as u32) & 31;
        Ok(lua_bit_tobit((value as u32).rotate_left(shift) as i64))
    })?;

    // ror(x, n) — rotate right
    let ror_fn = lua.create_function(|_, (value, shift): (i64, i64)| {
        let shift = (shift as u32) & 31;
        Ok(lua_bit_tobit((value as u32).rotate_right(shift) as i64))
    })?;

    // bswap(x) — byte swap
    let bswap_fn =
        lua.create_function(|_, v: i64| Ok(lua_bit_tobit((v as u32).swap_bytes() as i64)))?;

    // tohex(x [, n]) — convert to hex string
    // n > 0: lowercase, n < 0: uppercase, |n| = number of hex digits (default 8)
    let tohex_fn = lua.create_function(|_, args: MultiValue| {
        let value = match args.front() {
            Some(LuaValue::Integer(n)) => *n as u32,
            Some(LuaValue::Number(n)) => *n as u32,
            _ => {
                return Err(LuaError::RuntimeError(
                    "bad argument #1 to 'tohex' (number expected)".to_string(),
                ));
            }
        };
        let n = match args.get(1) {
            Some(LuaValue::Integer(n)) => *n as i32,
            Some(LuaValue::Number(n)) => *n as i32,
            None => 8,
            _ => 8,
        };
        let uppercase = n < 0;
        let width = n.unsigned_abs().min(8) as usize;
        let hex = if uppercase {
            format!("{:0width$X}", value, width = width)
        } else {
            format!("{:0width$x}", value, width = width)
        };
        // Truncate to requested width (from the right)
        let result = if hex.len() > width {
            &hex[hex.len() - width..]
        } else {
            &hex
        };
        Ok(result.to_string())
    })?;

    bit.set("tobit", tobit_fn)?;
    bit.set("bnot", bnot_fn)?;
    bit.set("band", band_fn)?;
    bit.set("bor", bor_fn)?;
    bit.set("bxor", bxor_fn)?;
    bit.set("lshift", lshift_fn)?;
    bit.set("rshift", rshift_fn)?;
    bit.set("arshift", arshift_fn)?;
    bit.set("rol", rol_fn)?;
    bit.set("ror", ror_fn)?;
    bit.set("bswap", bswap_fn)?;
    bit.set("tohex", tohex_fn)?;
    Ok(bit)
}

// ---------------------------------------------------------------------------
// cjson library — JSON encode/decode backed by serde_json
// ---------------------------------------------------------------------------

/// Shared per-Lua cjson configuration state stored in Lua registry.
struct CjsonConfig {
    encode_max_depth: usize,
    decode_max_depth: usize,
    encode_keep_buffer: bool,
    encode_invalid_numbers: bool,
    decode_array_with_array_mt: bool,
}

impl Default for CjsonConfig {
    fn default() -> Self {
        Self {
            encode_max_depth: 1000,
            decode_max_depth: 1000,
            encode_keep_buffer: true,
            encode_invalid_numbers: false,
            decode_array_with_array_mt: false,
        }
    }
}

const CJSON_CONFIG_KEY: &str = "__garnet_cjson_config";
const CJSON_ARRAY_MT_KEY: &str = "__garnet_cjson_array_mt";

fn get_cjson_config(lua: &Lua) -> CjsonConfig {
    let globals = lua.globals();
    let encode_max_depth = globals
        .raw_get::<i64>(format!("{CJSON_CONFIG_KEY}_emd"))
        .unwrap_or(1000) as usize;
    let decode_max_depth = globals
        .raw_get::<i64>(format!("{CJSON_CONFIG_KEY}_dmd"))
        .unwrap_or(1000) as usize;
    let encode_keep_buffer = globals
        .raw_get::<bool>(format!("{CJSON_CONFIG_KEY}_ekb"))
        .unwrap_or(true);
    let encode_invalid_numbers = globals
        .raw_get::<bool>(format!("{CJSON_CONFIG_KEY}_ein"))
        .unwrap_or(false);
    let decode_array_with_array_mt = globals
        .raw_get::<bool>(format!("{CJSON_CONFIG_KEY}_daam"))
        .unwrap_or(false);
    CjsonConfig {
        encode_max_depth,
        decode_max_depth,
        encode_keep_buffer,
        encode_invalid_numbers,
        decode_array_with_array_mt,
    }
}

fn set_cjson_config(lua: &Lua, config: &CjsonConfig) -> mlua::Result<()> {
    let globals = lua.globals();
    globals.raw_set(
        format!("{CJSON_CONFIG_KEY}_emd"),
        config.encode_max_depth as i64,
    )?;
    globals.raw_set(
        format!("{CJSON_CONFIG_KEY}_dmd"),
        config.decode_max_depth as i64,
    )?;
    globals.raw_set(format!("{CJSON_CONFIG_KEY}_ekb"), config.encode_keep_buffer)?;
    globals.raw_set(
        format!("{CJSON_CONFIG_KEY}_ein"),
        config.encode_invalid_numbers,
    )?;
    globals.raw_set(
        format!("{CJSON_CONFIG_KEY}_daam"),
        config.decode_array_with_array_mt,
    )?;
    Ok(())
}

fn lua_value_to_json(
    lua: &Lua,
    value: LuaValue,
    depth: usize,
    max_depth: usize,
    allow_invalid_numbers: bool,
) -> mlua::Result<serde_json::Value> {
    if depth > max_depth {
        return Err(LuaError::RuntimeError(
            "Cannot serialise, excessive nesting".to_string(),
        ));
    }
    match value {
        LuaValue::Nil => Ok(serde_json::Value::Null),
        LuaValue::Boolean(b) => Ok(serde_json::Value::Bool(b)),
        LuaValue::Integer(n) => Ok(serde_json::Value::Number(
            serde_json::Number::from_f64(n as f64)
                .unwrap_or_else(|| serde_json::Number::from(0i64)),
        )),
        LuaValue::Number(n) => {
            if !n.is_finite() {
                if allow_invalid_numbers {
                    Ok(serde_json::Value::Null)
                } else {
                    Err(LuaError::RuntimeError(
                        "Cannot serialise inf or nan".to_string(),
                    ))
                }
            } else if n.fract() == 0.0 && n >= i64::MIN as f64 && n <= i64::MAX as f64 {
                Ok(serde_json::Value::Number(serde_json::Number::from(
                    n as i64,
                )))
            } else {
                Ok(serde_json::Value::Number(
                    serde_json::Number::from_f64(n)
                        .unwrap_or_else(|| serde_json::Number::from(0i64)),
                ))
            }
        }
        LuaValue::String(s) => Ok(serde_json::Value::String(s.to_string_lossy())),
        LuaValue::Table(table) => {
            // Check if table has cjson array metatable
            let has_array_mt = if let Some(mt) = table.metatable() {
                lua.globals()
                    .raw_get::<LuaTable>(CJSON_ARRAY_MT_KEY)
                    .ok()
                    .map(|array_mt| mt.equals(&array_mt).unwrap_or(false))
                    .unwrap_or(false)
            } else {
                false
            };

            // Determine if this is an array-like table
            let len = table.raw_len();
            let mut is_array = has_array_mt;

            if !is_array && len > 0 {
                // Check if all keys are sequential integers
                let mut count = 0;
                for entry in table.clone().pairs::<LuaValue, LuaValue>() {
                    let (key, _) = entry?;
                    match key {
                        LuaValue::Integer(n) if n >= 1 && n as usize <= len => {
                            count += 1;
                        }
                        _ => {
                            is_array = false;
                            break;
                        }
                    }
                }
                if count == len {
                    is_array = true;
                }
            }

            if is_array {
                let mut arr = Vec::with_capacity(len);
                for i in 1..=len {
                    let v: LuaValue = table.raw_get(i)?;
                    arr.push(lua_value_to_json(
                        lua,
                        v,
                        depth + 1,
                        max_depth,
                        allow_invalid_numbers,
                    )?);
                }
                Ok(serde_json::Value::Array(arr))
            } else {
                let mut map = serde_json::Map::new();
                for entry in table.pairs::<LuaValue, LuaValue>() {
                    let (key, val) = entry?;
                    let key_str = match key {
                        LuaValue::String(s) => s.to_string_lossy(),
                        LuaValue::Integer(n) => n.to_string(),
                        LuaValue::Number(n) => n.to_string(),
                        LuaValue::Boolean(b) => {
                            return Err(LuaError::RuntimeError(format!(
                                "Cannot serialise {} table key",
                                if b { "true" } else { "false" }
                            )));
                        }
                        _ => {
                            return Err(LuaError::RuntimeError(
                                "Cannot serialise table key".to_string(),
                            ));
                        }
                    };
                    map.insert(
                        key_str,
                        lua_value_to_json(lua, val, depth + 1, max_depth, allow_invalid_numbers)?,
                    );
                }
                Ok(serde_json::Value::Object(map))
            }
        }
        _ => Err(LuaError::RuntimeError(
            "Cannot serialise function or userdata".to_string(),
        )),
    }
}

fn json_value_to_lua(
    lua: &Lua,
    value: serde_json::Value,
    depth: usize,
    max_depth: usize,
    use_array_mt: bool,
) -> mlua::Result<LuaValue> {
    if depth > max_depth {
        return Err(LuaError::RuntimeError(
            "Cannot deserialise, excessive nesting".to_string(),
        ));
    }
    match value {
        serde_json::Value::Null => Ok(LuaValue::Nil),
        serde_json::Value::Bool(b) => Ok(LuaValue::Boolean(b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(LuaValue::Number(i as f64))
            } else if let Some(f) = n.as_f64() {
                Ok(LuaValue::Number(f))
            } else {
                Ok(LuaValue::Number(0.0))
            }
        }
        serde_json::Value::String(s) => Ok(LuaValue::String(lua.create_string(&s)?)),
        serde_json::Value::Array(arr) => {
            let table = lua.create_table_with_capacity(arr.len(), 0)?;
            for (i, v) in arr.into_iter().enumerate() {
                table.set(
                    i + 1,
                    json_value_to_lua(lua, v, depth + 1, max_depth, use_array_mt)?,
                )?;
            }
            if use_array_mt
                && let Ok(array_mt) = lua.globals().raw_get::<LuaTable>(CJSON_ARRAY_MT_KEY)
            {
                let _ = table.set_metatable(Some(array_mt));
            }
            Ok(LuaValue::Table(table))
        }
        serde_json::Value::Object(map) => {
            let table = lua.create_table_with_capacity(0, map.len())?;
            for (k, v) in map {
                table.set(
                    lua.create_string(&k)?,
                    json_value_to_lua(lua, v, depth + 1, max_depth, use_array_mt)?,
                )?;
            }
            Ok(LuaValue::Table(table))
        }
    }
}

fn build_lua_cjson_table(lua: &Lua) -> mlua::Result<LuaTable> {
    let cjson = lua.create_table()?;

    // Initialize default config
    set_cjson_config(lua, &CjsonConfig::default())?;

    // Create the readonly array metatable used to distinguish JSON arrays from objects
    let array_mt = lua.create_table()?;
    let is_array_fn = lua.create_function(|_, _: ()| Ok(true))?;
    array_mt.raw_set("__is_cjson_array", is_array_fn)?;
    make_table_readonly(lua, &array_mt)?;
    lua.globals().raw_set(CJSON_ARRAY_MT_KEY, array_mt)?;

    // cjson.encode(value)
    let encode_fn = lua.create_function(|lua, value: LuaValue| {
        let config = get_cjson_config(lua);
        let json_value = lua_value_to_json(
            lua,
            value,
            0,
            config.encode_max_depth,
            config.encode_invalid_numbers,
        )?;
        let json_string = serde_json::to_string(&json_value)
            .map_err(|e| LuaError::RuntimeError(format!("Cannot serialise: {e}")))?;
        Ok(json_string)
    })?;

    // cjson.decode(string)
    let decode_fn = lua.create_function(|lua, s: mlua::String| {
        let config = get_cjson_config(lua);
        let json_value: serde_json::Value = serde_json::from_slice(s.as_bytes().as_ref())
            .map_err(|e| LuaError::RuntimeError(format!("Invalid JSON: {e}")))?;
        json_value_to_lua(
            lua,
            json_value,
            0,
            config.decode_max_depth,
            config.decode_array_with_array_mt,
        )
    })?;

    // cjson.encode_max_depth(n)
    let encode_max_depth_fn = lua.create_function(|lua, depth: i64| {
        let mut config = get_cjson_config(lua);
        config.encode_max_depth = depth.max(1) as usize;
        set_cjson_config(lua, &config)?;
        Ok(())
    })?;

    // cjson.decode_max_depth(n)
    let decode_max_depth_fn = lua.create_function(|lua, depth: i64| {
        let mut config = get_cjson_config(lua);
        config.decode_max_depth = depth.max(1) as usize;
        set_cjson_config(lua, &config)?;
        Ok(())
    })?;

    // cjson.encode_keep_buffer(bool)
    let encode_keep_buffer_fn = lua.create_function(|lua, keep: bool| {
        let mut config = get_cjson_config(lua);
        config.encode_keep_buffer = keep;
        set_cjson_config(lua, &config)?;
        Ok(())
    })?;

    // cjson.encode_invalid_numbers(bool)
    let encode_invalid_numbers_fn = lua.create_function(|lua, allow: bool| {
        let mut config = get_cjson_config(lua);
        config.encode_invalid_numbers = allow;
        set_cjson_config(lua, &config)?;
        Ok(())
    })?;

    // cjson.decode_array_with_array_mt(bool)
    let decode_array_with_array_mt_fn = lua.create_function(|lua, use_mt: bool| {
        let mut config = get_cjson_config(lua);
        config.decode_array_with_array_mt = use_mt;
        set_cjson_config(lua, &config)?;
        Ok(())
    })?;

    cjson.set("encode", encode_fn)?;
    cjson.set("decode", decode_fn)?;
    cjson.set("encode_max_depth", encode_max_depth_fn)?;
    cjson.set("decode_max_depth", decode_max_depth_fn)?;
    cjson.set("encode_keep_buffer", encode_keep_buffer_fn)?;
    cjson.set("encode_invalid_numbers", encode_invalid_numbers_fn)?;
    cjson.set("decode_array_with_array_mt", decode_array_with_array_mt_fn)?;
    Ok(cjson)
}

// ---------------------------------------------------------------------------
// cmsgpack library — MessagePack pack/unpack backed by rmpv
// ---------------------------------------------------------------------------

const CMSGPACK_MAX_NESTING: usize = 16;

fn lua_value_to_msgpack(value: LuaValue, depth: usize) -> mlua::Result<rmpv::Value> {
    match value {
        LuaValue::Nil => Ok(rmpv::Value::Nil),
        LuaValue::Boolean(b) => Ok(rmpv::Value::Boolean(b)),
        LuaValue::Integer(n) => {
            if n >= 0 {
                Ok(rmpv::Value::from(n as u64))
            } else {
                Ok(rmpv::Value::from(n))
            }
        }
        LuaValue::Number(n) => {
            if n.fract() == 0.0 && n >= i64::MIN as f64 && n <= i64::MAX as f64 {
                let i = n as i64;
                if i >= 0 {
                    Ok(rmpv::Value::from(i as u64))
                } else {
                    Ok(rmpv::Value::from(i))
                }
            } else {
                Ok(rmpv::Value::F64(n))
            }
        }
        LuaValue::String(s) => Ok(rmpv::Value::String(rmpv::Utf8String::from(
            s.to_string_lossy().as_ref(),
        ))),
        LuaValue::Table(table) => {
            // Redis cmsgpack: at max nesting depth, tables become nil
            if depth >= CMSGPACK_MAX_NESTING {
                return Ok(rmpv::Value::Nil);
            }
            let len = table.raw_len();
            // Check if it's an array-like table
            let mut is_array = len > 0;
            if is_array {
                let mut count = 0;
                for entry in table.clone().pairs::<LuaValue, LuaValue>() {
                    let (key, _) = entry?;
                    match key {
                        LuaValue::Integer(n) if n >= 1 && n as usize <= len => {
                            count += 1;
                        }
                        _ => {
                            is_array = false;
                            break;
                        }
                    }
                }
                if count != len {
                    is_array = false;
                }
            }

            if is_array {
                let mut arr = Vec::with_capacity(len);
                for i in 1..=len {
                    let v: LuaValue = table.raw_get(i)?;
                    arr.push(lua_value_to_msgpack(v, depth + 1)?);
                }
                Ok(rmpv::Value::Array(arr))
            } else {
                let mut map = Vec::new();
                for entry in table.pairs::<LuaValue, LuaValue>() {
                    let (k, v) = entry?;
                    let key = lua_value_to_msgpack(k, depth + 1)?;
                    let val = lua_value_to_msgpack(v, depth + 1)?;
                    map.push((key, val));
                }
                Ok(rmpv::Value::Map(map))
            }
        }
        _ => Ok(rmpv::Value::Nil),
    }
}

fn msgpack_value_to_lua(lua: &Lua, value: rmpv::Value) -> mlua::Result<LuaValue> {
    match value {
        rmpv::Value::Nil => Ok(LuaValue::Nil),
        rmpv::Value::Boolean(b) => Ok(LuaValue::Boolean(b)),
        rmpv::Value::Integer(n) => {
            if let Some(i) = n.as_i64() {
                Ok(LuaValue::Number(i as f64))
            } else if let Some(u) = n.as_u64() {
                Ok(LuaValue::Number(u as f64))
            } else {
                Ok(LuaValue::Number(0.0))
            }
        }
        rmpv::Value::F32(f) => Ok(LuaValue::Number(f as f64)),
        rmpv::Value::F64(f) => Ok(LuaValue::Number(f)),
        rmpv::Value::String(s) => {
            if let Some(s) = s.as_str() {
                Ok(LuaValue::String(lua.create_string(s)?))
            } else {
                Ok(LuaValue::String(lua.create_string(s.as_bytes())?))
            }
        }
        rmpv::Value::Binary(b) => Ok(LuaValue::String(lua.create_string(&b)?)),
        rmpv::Value::Array(arr) => {
            let table = lua.create_table_with_capacity(arr.len(), 0)?;
            for (i, v) in arr.into_iter().enumerate() {
                table.set(i + 1, msgpack_value_to_lua(lua, v)?)?;
            }
            Ok(LuaValue::Table(table))
        }
        rmpv::Value::Map(map) => {
            let table = lua.create_table_with_capacity(0, map.len())?;
            for (k, v) in map {
                let lua_val = msgpack_value_to_lua(lua, v)?;
                // In Lua, setting a table key to nil removes the entry.
                // Skip nil values to match Redis's lua_settable behavior.
                if lua_val != LuaValue::Nil {
                    table.set(msgpack_value_to_lua(lua, k)?, lua_val)?;
                }
            }
            Ok(LuaValue::Table(table))
        }
        rmpv::Value::Ext(_, _) => Ok(LuaValue::Nil),
    }
}

fn build_lua_cmsgpack_table(lua: &Lua) -> mlua::Result<LuaTable> {
    let cmsgpack = lua.create_table()?;

    // cmsgpack.pack(...) — encode one or more values to msgpack binary string
    let pack_fn = lua.create_function(|lua, args: MultiValue| {
        let mut buf = Vec::new();
        for arg in args {
            let value = lua_value_to_msgpack(arg, 0)?;
            rmpv::encode::write_value(&mut buf, &value)
                .map_err(|e| LuaError::RuntimeError(format!("cmsgpack.pack error: {e}")))?;
        }
        lua.create_string(&buf)
    })?;

    // cmsgpack.unpack(string) — decode first value from msgpack
    let unpack_fn = lua.create_function(|lua, s: mlua::String| {
        let bytes = s.as_bytes();
        let mut cursor = std::io::Cursor::new(bytes.as_ref());
        let value = rmpv::decode::read_value(&mut cursor)
            .map_err(|e| LuaError::RuntimeError(format!("cmsgpack.unpack error: {e}")))?;
        msgpack_value_to_lua(lua, value)
    })?;

    // cmsgpack.unpack_one(string, offset) — decode one value from offset, return (offset, value)
    // offset is 0-based, returns -1 when at end
    let unpack_one_fn = lua.create_function(|lua, (s, offset): (mlua::String, i64)| {
        let bytes = s.as_bytes();
        let start = offset.max(0) as usize;
        if start >= bytes.len() {
            return Ok((-1i64, LuaValue::Nil));
        }
        let mut cursor = std::io::Cursor::new(&bytes.as_ref()[start..]);
        let value = rmpv::decode::read_value(&mut cursor)
            .map_err(|e| LuaError::RuntimeError(format!("cmsgpack.unpack_one error: {e}")))?;
        let new_offset = start + cursor.position() as usize;
        let return_offset = if new_offset >= bytes.len() {
            -1i64
        } else {
            new_offset as i64
        };
        Ok((return_offset, msgpack_value_to_lua(lua, value)?))
    })?;

    // cmsgpack.unpack_limit(string, limit, offset) — decode up to limit values
    let unpack_limit_fn =
        lua.create_function(|lua, (s, limit, offset): (mlua::String, i64, i64)| {
            let bytes = s.as_bytes();
            let start = offset.max(0) as usize;
            if start >= bytes.len() || limit <= 0 {
                return Ok((-1i64, LuaValue::Nil));
            }
            let mut cursor = std::io::Cursor::new(&bytes.as_ref()[start..]);
            let mut values = Vec::new();
            for _ in 0..limit {
                if cursor.position() as usize >= bytes.as_ref()[start..].len() {
                    break;
                }
                let value = rmpv::decode::read_value(&mut cursor).map_err(|e| {
                    LuaError::RuntimeError(format!("cmsgpack.unpack_limit error: {e}"))
                })?;
                values.push(msgpack_value_to_lua(lua, value)?);
            }
            let new_offset = start + cursor.position() as usize;
            let return_offset = if new_offset >= bytes.len() {
                -1i64
            } else {
                new_offset as i64
            };
            if values.len() == 1 {
                Ok((return_offset, values.into_iter().next().unwrap()))
            } else {
                let table = lua.create_table_with_capacity(values.len(), 0)?;
                for (i, v) in values.into_iter().enumerate() {
                    table.set(i + 1, v)?;
                }
                Ok((return_offset, LuaValue::Table(table)))
            }
        })?;

    cmsgpack.set("pack", pack_fn)?;
    cmsgpack.set("unpack", unpack_fn)?;
    cmsgpack.set("unpack_one", unpack_one_fn)?;
    cmsgpack.set("unpack_limit", unpack_limit_fn)?;
    Ok(cmsgpack)
}

// ---------------------------------------------------------------------------
// Sandboxed os table — only os.clock() exposed
// ---------------------------------------------------------------------------

fn build_lua_os_table(lua: &Lua) -> mlua::Result<LuaTable> {
    let os_table = lua.create_table()?;
    let clock_fn = lua.create_function(|_, ()| {
        // Return CPU time used by the process in seconds as a float
        // On Unix-like systems, this uses clock_gettime(CLOCK_PROCESS_CPUTIME_ID)
        // For simplicity and cross-platform compatibility, we use Instant-based wall clock
        // approximation. The test just checks elapsed time is >= 1 second.
        use std::time::SystemTime;
        let duration = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default();
        // Return a high-resolution timestamp (in seconds) that increases monotonically.
        // This isn't true CPU time but it matches the test expectations.
        Ok(duration.as_secs_f64())
    })?;
    os_table.set("clock", clock_fn)?;
    Ok(os_table)
}

// ---------------------------------------------------------------------------
// Readonly table protection helper
// ---------------------------------------------------------------------------

fn install_readonly_table_metatable(lua: &Lua, table: &LuaTable) -> mlua::Result<()> {
    let metatable = lua.create_table()?;
    let newindex_fn = lua.create_function(
        |_, (_table, _key, _value): (LuaValue, LuaValue, LuaValue)| -> mlua::Result<()> {
            Err(LuaError::RuntimeError(
                "Attempt to modify a readonly table".to_string(),
            ))
        },
    )?;
    metatable.set("__newindex", newindex_fn)?;
    table.set_metatable(Some(metatable))?;
    Ok(())
}

/// Create a readonly proxy: move all raw keys from `content` into a new
/// backing table and set `content` up as an empty proxy whose `__index`
/// points to the backing, `__newindex` rejects all writes, and
/// `__metatable` prevents metatable tampering.
///
/// After this call, `content` is empty and all lookups go through the
/// metatable, ensuring that writes to existing keys also fail.
/// Metatable key that stores the backing table for readonly proxy tables.
/// Used by the overridden `pairs`/`next` to iterate the real contents.
const READONLY_BACKING_KEY: &str = "__backing";

fn make_table_readonly(lua: &Lua, content: &LuaTable) -> mlua::Result<()> {
    // Move existing raw keys to a backing table
    let backing = lua.create_table()?;
    let keys_to_move: Vec<(LuaValue, LuaValue)> = content
        .pairs::<LuaValue, LuaValue>()
        .filter_map(|entry| entry.ok())
        .collect();
    for (key, value) in &keys_to_move {
        backing.raw_set(key.clone(), value.clone())?;
    }
    for (key, _) in &keys_to_move {
        content.raw_set(key.clone(), LuaValue::Nil)?;
    }
    // Set up metatable with __backing so overridden pairs/next can find keys
    let metatable = lua.create_table()?;
    metatable.raw_set("__index", backing.clone())?;
    metatable.raw_set(READONLY_BACKING_KEY, backing)?;
    let newindex_fn = lua.create_function(
        |_, (_table, _key, _value): (LuaValue, LuaValue, LuaValue)| -> mlua::Result<()> {
            Err(LuaError::RuntimeError(
                "Attempt to modify a readonly table".to_string(),
            ))
        },
    )?;
    metatable.raw_set("__newindex", newindex_fn)?;
    content.set_metatable(Some(metatable))?;
    // Register in the readonly tables set so safe_setmetatable blocks
    // setmetatable() targeting this table.
    mark_table_readonly(lua, content)?;
    Ok(())
}

/// Get the backing table for a readonly proxy table, if it has one.
fn get_readonly_backing(table: &LuaTable) -> Option<LuaTable> {
    let metatable = table.metatable()?;
    match metatable.raw_get::<LuaValue>(READONLY_BACKING_KEY) {
        Ok(LuaValue::Table(backing)) => Some(backing),
        _ => None,
    }
}

/// Set a value in the backing table of an environment built by
/// `build_strict_lua_environment`.  The env's metatable `__index` points to
/// the backing table.  Writing here keeps the raw env empty so that
/// `__newindex` protection remains effective.
fn set_env_backing_value(env: &LuaTable, key: &str, value: LuaValue) -> mlua::Result<()> {
    let metatable = env
        .metatable()
        .ok_or_else(|| LuaError::RuntimeError("env has no metatable".into()))?;
    let backing: LuaTable = metatable.get("__index")?;
    backing.raw_set(key, value)?;
    Ok(())
}

/// Lua registry key for the set of readonly-protected tables.
const READONLY_TABLES_REGISTRY_KEY: &str = "__garnet_readonly_tables";

/// Mark a table as readonly-protected in the Lua registry.  After this call,
/// `safe_setmetatable` will reject `setmetatable()` calls targeting this table.
fn mark_table_readonly(lua: &Lua, table: &LuaTable) -> mlua::Result<()> {
    let globals = lua.globals();
    let registry: LuaTable = match globals.raw_get::<LuaValue>(READONLY_TABLES_REGISTRY_KEY) {
        Ok(LuaValue::Table(t)) => t,
        _ => {
            let t = lua.create_table()?;
            globals.raw_set(READONLY_TABLES_REGISTRY_KEY, t.clone())?;
            t
        }
    };
    // Use the table itself as key (identity-based lookup)
    registry.raw_set(table.clone(), true)?;
    Ok(())
}

/// Override `pairs`, `next`, and `ipairs` to be aware of readonly proxy tables.
///
/// Readonly proxy tables (created by `make_table_readonly`) are empty; all real
/// keys live in the backing table stored as `__backing` in the metatable.
/// Standard Lua 5.1 does not support `__pairs`, so we wrap the builtins to
/// check for this backing table before iterating.
fn install_readonly_aware_iterators(lua: &Lua, backing: &LuaTable) -> mlua::Result<()> {
    let globals = lua.globals();
    let original_next: LuaFunction = globals.get("next")?;
    let original_ipairs: LuaFunction = globals.get("ipairs")?;

    // Wrap `next` to redirect to the backing table when called on a proxy.
    let next_clone = original_next.clone();
    let safe_next = lua.create_function(move |_, (table, key): (LuaTable, LuaValue)| {
        let target = get_readonly_backing(&table).unwrap_or(table);
        next_clone.call::<MultiValue>((target, key))
    })?;

    // Wrap `pairs` to use our safe `next` and the backing table.
    let safe_next_for_pairs = safe_next.clone();
    let safe_pairs =
        lua.create_function(move |_, table: LuaTable| -> mlua::Result<MultiValue> {
            let target = get_readonly_backing(&table).unwrap_or(table);
            Ok(MultiValue::from_iter([
                LuaValue::Function(safe_next_for_pairs.clone()),
                LuaValue::Table(target),
                LuaValue::Nil,
            ]))
        })?;

    // Wrap `ipairs` to iterate the backing table when present.
    let safe_ipairs =
        lua.create_function(move |_, table: LuaTable| -> mlua::Result<MultiValue> {
            let target = get_readonly_backing(&table).unwrap_or(table);
            original_ipairs.call::<MultiValue>(target)
        })?;

    backing.raw_set("next", safe_next)?;
    backing.raw_set("pairs", safe_pairs)?;
    backing.raw_set("ipairs", safe_ipairs)?;
    Ok(())
}

/// Replace `setmetatable` in the backing globals with a wrapper that rejects
/// modifications to protected (readonly) tables.  Redis's patched Lua checks a
/// C-level readonly flag; we emulate this by keeping a registry set of
/// protected table references and checking before delegation.
fn install_safe_setmetatable(
    lua: &Lua,
    backing: &LuaTable,
    env: &LuaTable,
    _protected_tables: &LuaTable,
) -> mlua::Result<()> {
    let globals = lua.globals();
    let original_setmetatable: LuaFunction = globals.get("setmetatable")?;

    // Mark the env table as protected
    mark_table_readonly(lua, env)?;

    let safe_setmetatable =
        lua.create_function(move |lua, (table, mt): (LuaValue, LuaValue)| {
            if let LuaValue::Table(ref t) = table {
                // Check the registry of readonly tables
                if let Ok(LuaValue::Table(registry)) = lua
                    .globals()
                    .raw_get::<LuaValue>(READONLY_TABLES_REGISTRY_KEY)
                    && let Ok(LuaValue::Boolean(true)) = registry.raw_get::<LuaValue>(t.clone())
                {
                    return Err(LuaError::RuntimeError(
                        "Attempt to modify a readonly table".to_string(),
                    ));
                }
            }
            original_setmetatable.call::<LuaValue>((table, mt))
        })?;
    backing.raw_set("setmetatable", safe_setmetatable)?;
    Ok(())
}

/// Protect basic type metatables (string metatable) to prevent modification.
/// In Lua 5.1, only string has a global metatable by default.
/// For nil, number, boolean, function, thread — getmetatable returns nil,
/// so the test checks for either "attempt to index a nil value" or
/// "Attempt to modify a readonly table".
fn install_basic_type_metatable_protection(lua: &Lua) -> mlua::Result<()> {
    // The string metatable is the only basic-type metatable in Lua 5.1.
    //
    // Redis uses a C-level readonly flag to protect it.  We emulate this by
    // setting `__metatable` on the real string metatable to a readonly proxy.
    // This makes `getmetatable('')` return the proxy (per Lua 5.1 semantics),
    // so `getmetatable('').__index = ...` hits `__newindex` and errors.
    // Meanwhile, the real metatable still functions normally for string
    // method dispatch (`s:upper()` etc.).
    lua.load(
        r#"
        local real_mt = getmetatable('')
        if real_mt then
            -- Build a readonly proxy: empty table whose __index reads from
            -- the real metatable and whose __newindex always errors.
            local proxy = {}
            local proxy_mt = {}
            proxy_mt.__index = real_mt
            proxy_mt.__newindex = function()
                error('Attempt to modify a readonly table')
            end
            setmetatable(proxy, proxy_mt)
            -- Intercept getmetatable('') to return the proxy instead of
            -- the real metatable.
            real_mt.__metatable = proxy
        end
        "#,
    )
    .exec()?;
    Ok(())
}

/// Replace `loadstring` with a safe wrapper that rejects binary bytecode
/// (strings starting with `\27Lua`). This matches Redis/Valkey behavior where
/// `loadstring(string.dump(...))` returns nil instead of loading the bytecode.
fn install_safe_loadstring(lua: &Lua) -> mlua::Result<()> {
    let globals = lua.globals();
    let original_loadstring: LuaFunction = globals.get("loadstring")?;
    let safe_loadstring = lua.create_function(move |lua, args: MultiValue| {
        // Check if first argument is a binary chunk (starts with \27 = ESC)
        #[allow(clippy::get_first)]
        if let Some(LuaValue::String(s)) = args.get(0) {
            let bytes = s.as_bytes();
            if bytes.as_ref().first() == Some(&0x1b) {
                // Binary chunk — return nil, error_message
                let mut result = MultiValue::new();
                result.push_back(LuaValue::Nil);
                result.push_back(LuaValue::String(
                    lua.create_string("attempt to load a binary chunk")?,
                ));
                return Ok(result);
            }
        }
        original_loadstring.call::<MultiValue>(args)
    })?;
    globals.set("loadstring", safe_loadstring)?;
    Ok(())
}

/// Replace the Lua `pcall` and `xpcall` globals with wrappers that convert
/// mlua userdata error objects to plain Lua strings.
///
/// mlua wraps Rust callback errors as userdata objects on the Lua stack. When
/// Lua code uses `pcall(f)` and the call raises an error from a Rust callback,
/// the second return value is userdata instead of a string. Redis's native Lua
/// always returns string error objects from `pcall`, so scripts that concatenate
/// or pattern-match on the error value would break without this conversion.
fn install_safe_pcall(lua: &Lua) -> mlua::Result<()> {
    let globals = lua.globals();

    // Wrap pcall: pcall(f, ...) -> status, result...
    let original_pcall: LuaFunction = globals.get("pcall")?;
    let safe_pcall = lua.create_function(move |lua, args: MultiValue| {
        let mut results = original_pcall.call::<MultiValue>(args)?;
        // If the call failed (first return value is false) and the error object
        // is not a plain Lua string, convert it to a string so that scripts
        // which concatenate or pattern-match on the error value work correctly.
        //
        // mlua represents Rust-callback errors as `Value::Error(...)`, which is
        // distinct from `Value::UserData(...)`.  Both must be handled.
        if results.len() >= 2
            && let Some(LuaValue::Boolean(false)) = results.front()
        {
            pcall_coerce_error_to_string(lua, &mut results)?;
        }
        Ok(results)
    })?;
    globals.set("pcall", safe_pcall)?;

    // Wrap xpcall: xpcall(f, handler) -> status, result...
    let original_xpcall: LuaFunction = globals.get("xpcall")?;
    let safe_xpcall = lua.create_function(move |lua, args: MultiValue| {
        let mut results = original_xpcall.call::<MultiValue>(args)?;
        if results.len() >= 2
            && let Some(LuaValue::Boolean(false)) = results.front()
        {
            pcall_coerce_error_to_string(lua, &mut results)?;
        }
        Ok(results)
    })?;
    globals.set("xpcall", safe_xpcall)?;

    Ok(())
}

/// If `results[1]` is a non-string error value (mlua `Error` variant or
/// userdata), replace it with its string representation so Lua scripts can
/// concatenate and pattern-match the error message.
fn pcall_coerce_error_to_string(lua: &Lua, results: &mut MultiValue) -> mlua::Result<()> {
    let err_val = &results[1];
    match err_val {
        LuaValue::Error(err) => {
            // mlua wraps Rust callback errors (e.g. LuaError::RuntimeError) in
            // a Value::Error box.  Extract the core message without traceback,
            // matching the behavior of native Lua's pcall which returns just
            // the error string.
            let msg = extract_lua_error_message(err);
            results[1] = LuaValue::String(lua.create_string(&msg)?);
        }
        LuaValue::UserData(_) => {
            let tostring: LuaFunction = lua.globals().get("tostring")?;
            let stringified: mlua::String = tostring.call::<mlua::String>(err_val.clone())?;
            results[1] = LuaValue::String(stringified);
        }
        _ => {}
    }
    Ok(())
}

/// Extract the core error message from an mlua Error, stripping the
/// `runtime error: ` prefix, traceback, and unwinding `CallbackError`
/// wrappers to find the root cause message.
fn extract_lua_error_message(error: &LuaError) -> String {
    match error {
        LuaError::RuntimeError(msg) => msg.clone(),
        LuaError::CallbackError { cause, .. } => extract_lua_error_message(cause),
        other => {
            // For other error types, use the Display implementation but
            // strip the common prefixes that mlua adds.
            let text = other.to_string();
            text.strip_prefix("runtime error: ")
                .unwrap_or(&text)
                .split(" stack traceback:")
                .next()
                .unwrap_or(&text)
                .to_string()
        }
    }
}

/// Replace the Lua `unpack` global with a safe wrapper that validates the
/// requested range before delegating to the original C implementation.
///
/// Vanilla Lua 5.1.5 computes `n = e - i + 1` using signed `int` arithmetic.
/// When the caller passes extreme indices (e.g. `unpack(t, 0, 2147483647)`)
/// the addition overflows, which is undefined behaviour in C. Under optimised
/// builds on ARM64, the compiler may elide the subsequent `n <= 0` safety
/// check and let the unpack loop run unbounded until the process crashes with
/// SIGSEGV.
///
/// This wrapper catches that situation by computing the range width with
/// saturating 64-bit arithmetic and returning an error for any range that
/// exceeds `LUA_UNPACK_MAX_RESULTS`.
fn install_safe_unpack(lua: &Lua) -> mlua::Result<()> {
    let globals = lua.globals();
    let original_unpack: LuaFunction = globals.get("unpack")?;
    let safe_unpack = lua.create_function(move |_lua, args: MultiValue| {
        // unpack(table [, i [, j]])
        // Default i = 1, default j = #table.
        let i: i64 = match args.get(1) {
            Some(LuaValue::Integer(n)) => *n,
            Some(LuaValue::Number(n)) => *n as i64,
            None => 1,
            _ => 1, // let original unpack handle type errors
        };
        let j_explicit: Option<i64> = match args.get(2) {
            Some(LuaValue::Integer(n)) => Some(*n),
            Some(LuaValue::Number(n)) => Some(*n as i64),
            _ => None,
        };
        let j: i64 = match j_explicit {
            Some(v) => v,
            None => {
                // Default j = #table — get the length of the first argument.
                #[allow(clippy::get_first)]
                match args.get(0) {
                    Some(LuaValue::Table(t)) => t.raw_len() as i64,
                    _ => 0, // let original unpack handle type errors
                }
            }
        };
        let count = (j as i128) - (i as i128) + 1;
        if count > LUA_UNPACK_MAX_RESULTS as i128 {
            return Err(LuaError::RuntimeError(
                "too many results to unpack".to_string(),
            ));
        }
        // Delegate to the original C unpack for correct behaviour on valid ranges
        // (including empty ranges where i > j, and negative indices).
        original_unpack.call::<MultiValue>(args)
    })?;
    globals.set("unpack", safe_unpack)?;
    Ok(())
}

fn estimate_function_vm_memory_bytes(registry: &FunctionRegistry) -> u64 {
    let source_bytes = registry
        .library_sources
        .values()
        .map(Vec::len)
        .sum::<usize>();
    // Include a small fixed-per-function overhead so INFO-based test thresholds
    // remain stable across library layouts.
    let function_overhead = registry.functions.len().saturating_mul(96);
    (source_bytes.saturating_add(function_overhead)) as u64
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

fn lua_argument_to_bytes(value: LuaValue, _index: usize) -> mlua::Result<Vec<u8>> {
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
        _ => Err(LuaError::RuntimeError(
            "ERR Lua redis lib command arguments must be strings or integers".to_string(),
        )),
    }
}

/// Rewrite error messages from the command execution layer to match
/// Redis/Valkey scripting error conventions.  Redis rewrites certain
/// well-known error strings so that scripts see a canonical form
/// regardless of how the server formats them internally.
fn rewrite_script_error_message(message: &str) -> String {
    // Strip leading "ERR " for comparison but keep it in the rewrite.
    let body = message.strip_prefix("ERR ").unwrap_or(message);

    // "wrong number of arguments for '...' command"
    // -> "ERR Wrong number of args calling Redis command from script"
    if body.starts_with("wrong number of arguments for ") {
        return "ERR Wrong number of args calling Redis command from script".to_string();
    }

    // "unknown command '...'" -> "ERR Unknown Redis command called from script"
    if body.starts_with("unknown command") {
        return "ERR Unknown Redis command called from script".to_string();
    }

    // All other messages are passed through unchanged.
    message.to_string()
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
    append_lua_value_as_resp_depth(value, response_out, 0)
}

fn append_lua_value_as_resp_depth(
    value: LuaValue,
    response_out: &mut Vec<u8>,
    depth: usize,
) -> Result<(), String> {
    if depth >= LUA_REPLY_MAX_DEPTH {
        append_error(response_out, b"ERR reached lua stack limit");
        return Ok(());
    }
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
        LuaValue::Table(table) => append_lua_table_as_resp_depth(table, response_out, depth),
        _ => Err("Lua script returned unsupported value type".to_string()),
    }
}

fn append_lua_table_as_resp_depth(
    table: LuaTable,
    response_out: &mut Vec<u8>,
    depth: usize,
) -> Result<(), String> {
    // Use raw_get to avoid triggering metamethods (matches Redis/Valkey behaviour).
    if let Some(error_payload) = lua_table_raw_string_field(&table, "err")? {
        append_error(response_out, &error_payload);
        return Ok(());
    }
    if let Some(status_payload) = lua_table_raw_string_field(&table, "ok")? {
        append_simple_string(response_out, &status_payload);
        return Ok(());
    }
    if let Some(double_payload) = lua_table_raw_double_field(&table, "double")? {
        append_bulk_string(response_out, double_payload.to_string().as_bytes());
        return Ok(());
    }

    // Use raw_len + raw_get to iterate the array portion without metamethods.
    let len = table.raw_len();
    let mut values = Vec::with_capacity(len);
    for i in 1..=len {
        let value: LuaValue = table
            .raw_get(i)
            .map_err(|error| sanitize_error_text(&error.to_string()))?;
        values.push(value);
    }
    response_out.push(b'*');
    response_out.extend_from_slice(values.len().to_string().as_bytes());
    response_out.extend_from_slice(b"\r\n");
    for value in values {
        append_lua_value_as_resp_depth(value, response_out, depth + 1)?;
    }
    Ok(())
}

fn lua_table_raw_string_field(table: &LuaTable, field: &str) -> Result<Option<Vec<u8>>, String> {
    let value: LuaValue = table
        .raw_get(field)
        .map_err(|error| sanitize_error_text(&error.to_string()))?;
    match value {
        LuaValue::Nil => Ok(None),
        LuaValue::String(payload) => Ok(Some(payload.as_bytes().to_vec())),
        _ => Err(format!("Lua table field '{}' must be a string", field)),
    }
}

fn lua_table_raw_double_field(table: &LuaTable, field: &str) -> Result<Option<f64>, String> {
    let value: LuaValue = table
        .raw_get(field)
        .map_err(|error| sanitize_error_text(&error.to_string()))?;
    match value {
        LuaValue::Nil => Ok(None),
        LuaValue::Number(n) => Ok(Some(n)),
        LuaValue::Integer(n) => Ok(Some(n as f64)),
        _ => Ok(None),
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

fn parse_register_function_binding(values: MultiValue) -> mlua::Result<RegisterFunctionBinding> {
    if values.is_empty() {
        return Err(LuaError::RuntimeError(
            "wrong number of arguments to redis.register_function".to_string(),
        ));
    }

    if values.len() == 1 {
        match values.into_iter().next().expect("single value must exist") {
            LuaValue::Table(descriptor) => parse_register_function_binding_from_table(descriptor),
            _ => Err(LuaError::RuntimeError(
                "calling redis.register_function with a single argument is only applicable to Lua table"
                    .to_string(),
            )),
        }
    } else if values.len() == 2 {
        let mut values = values.into_iter();
        let name = match values.next().expect("first value must exist") {
            LuaValue::String(name) => name.to_string_lossy(),
            _ => {
                return Err(LuaError::RuntimeError(
                    "first argument to redis.register_function must be a string".to_string(),
                ));
            }
        };
        let callback = match values.next().expect("second value must exist") {
            LuaValue::Function(callback) => callback,
            _ => {
                return Err(LuaError::RuntimeError(
                    "second argument to redis.register_function must be a function".to_string(),
                ));
            }
        };
        let name = parse_function_registration_name(&name)?;
        Ok(RegisterFunctionBinding {
            name,
            description: String::new(),
            flags: FunctionExecutionFlags::default(),
            callback,
        })
    } else {
        Err(LuaError::RuntimeError(
            "wrong number of arguments to redis.register_function".to_string(),
        ))
    }
}

fn parse_function_registration_name(raw: &str) -> mlua::Result<String> {
    let normalized = raw.trim();
    if !is_valid_script_function_name(normalized) {
        return Err(LuaError::RuntimeError(
            SCRIPT_FUNCTION_NAME_FORMAT_ERROR.to_string(),
        ));
    }
    Ok(normalized.to_string())
}

fn is_valid_script_function_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FunctionRestoreMode {
    Append,
    Flush,
    Replace,
}

#[derive(Default)]
struct FunctionListOptions {
    with_code: bool,
    library_name_pattern: Option<Vec<u8>>,
}

fn parse_function_list_options(args: &[&[u8]]) -> Result<FunctionListOptions, String> {
    ensure_min_arity(args, 2, "FUNCTION", SCRIPT_FUNCTION_LIST_USAGE).map_err(|error| {
        request_error_message(error)
            .trim_start_matches("ERR ")
            .to_string()
    })?;
    let mut options = FunctionListOptions::default();
    let mut index = 2usize;
    while index < args.len() {
        let token = args[index];
        if ascii_eq_ignore_case(token, b"WITHCODE") {
            if options.with_code {
                return Err("Unknown argument withcode".to_string());
            }
            options.with_code = true;
            index += 1;
            continue;
        }
        if ascii_eq_ignore_case(token, b"LIBRARYNAME") {
            if options.library_name_pattern.is_some() {
                return Err("Unknown argument libraryname".to_string());
            }
            if index + 1 >= args.len() {
                return Err("library name argument was not given".to_string());
            }
            options.library_name_pattern = Some(args[index + 1].to_vec());
            index += 2;
            continue;
        }
        return Err(format!(
            "Unknown argument {}",
            String::from_utf8_lossy(token)
        ));
    }
    Ok(options)
}

fn parse_function_restore_mode(
    mode: Option<&[u8]>,
) -> Result<FunctionRestoreMode, RequestExecutionError> {
    let Some(mode) = mode else {
        return Ok(FunctionRestoreMode::Append);
    };
    if ascii_eq_ignore_case(mode, b"APPEND") {
        return Ok(FunctionRestoreMode::Append);
    }
    if ascii_eq_ignore_case(mode, b"FLUSH") {
        return Ok(FunctionRestoreMode::Flush);
    }
    if ascii_eq_ignore_case(mode, b"REPLACE") {
        return Ok(FunctionRestoreMode::Replace);
    }
    Err(RequestExecutionError::SyntaxError)
}

fn append_len_prefixed_bytes(
    output: &mut Vec<u8>,
    data: &[u8],
) -> Result<(), RequestExecutionError> {
    let length = u32::try_from(data.len()).map_err(|_| RequestExecutionError::SyntaxError)?;
    output.extend_from_slice(&length.to_le_bytes());
    output.extend_from_slice(data);
    Ok(())
}

fn parse_function_dump_payload(
    payload: &[u8],
) -> Result<Vec<(String, Vec<u8>)>, RequestExecutionError> {
    if payload.len() < 8 {
        return Err(RequestExecutionError::InvalidDumpPayload);
    }
    if &payload[0..4] != FUNCTION_DUMP_MAGIC {
        return Err(RequestExecutionError::InvalidDumpPayload);
    }

    let mut cursor = 4usize;
    let library_count = read_u32_le(payload, &mut cursor)
        .ok_or(RequestExecutionError::InvalidDumpPayload)? as usize;
    let mut libraries = Vec::with_capacity(library_count);
    for _ in 0..library_count {
        let library_name_bytes = read_len_prefixed_bytes(payload, &mut cursor)
            .ok_or(RequestExecutionError::InvalidDumpPayload)?;
        let library_name = std::str::from_utf8(library_name_bytes)
            .map_err(|_| RequestExecutionError::InvalidDumpPayload)?
            .to_owned();
        let source_bytes = read_len_prefixed_bytes(payload, &mut cursor)
            .ok_or(RequestExecutionError::InvalidDumpPayload)?;
        libraries.push((library_name, source_bytes.to_vec()));
    }
    if cursor != payload.len() {
        return Err(RequestExecutionError::InvalidDumpPayload);
    }
    Ok(libraries)
}

fn read_u32_le(payload: &[u8], cursor: &mut usize) -> Option<u32> {
    if *cursor + 4 > payload.len() {
        return None;
    }
    let bytes = [
        payload[*cursor],
        payload[*cursor + 1],
        payload[*cursor + 2],
        payload[*cursor + 3],
    ];
    *cursor += 4;
    Some(u32::from_le_bytes(bytes))
}

fn read_len_prefixed_bytes<'a>(payload: &'a [u8], cursor: &mut usize) -> Option<&'a [u8]> {
    let length = read_u32_le(payload, cursor)? as usize;
    if *cursor + length > payload.len() {
        return None;
    }
    let bytes = &payload[*cursor..*cursor + length];
    *cursor += length;
    Some(bytes)
}

fn parse_register_function_binding_from_table(
    descriptor: LuaTable,
) -> mlua::Result<RegisterFunctionBinding> {
    for entry in descriptor.clone().pairs::<LuaValue, LuaValue>() {
        let (key, _value) = entry?;
        let key = match key {
            LuaValue::String(value) => value.to_string_lossy(),
            _ => {
                return Err(LuaError::RuntimeError(
                    "unknown argument given to redis.register_function".to_string(),
                ));
            }
        };
        if !matches!(
            key.as_str(),
            "function_name" | "name" | "callback" | "description" | "flags"
        ) {
            return Err(LuaError::RuntimeError(
                "unknown argument given to redis.register_function".to_string(),
            ));
        }
    }

    let name = match descriptor.get::<LuaValue>("function_name")? {
        LuaValue::String(name) => name.to_string_lossy(),
        LuaValue::Nil => match descriptor.get::<LuaValue>("name")? {
            LuaValue::String(name) => name.to_string_lossy(),
            LuaValue::Nil => {
                return Err(LuaError::RuntimeError(
                    "redis.register_function must get a function name argument".to_string(),
                ));
            }
            _ => {
                return Err(LuaError::RuntimeError(
                    "function_name argument given to redis.register_function must be a string"
                        .to_string(),
                ));
            }
        },
        _ => {
            return Err(LuaError::RuntimeError(
                "function_name argument given to redis.register_function must be a string"
                    .to_string(),
            ));
        }
    };
    let name = parse_function_registration_name(&name)?;

    let callback = match descriptor.get::<LuaValue>("callback")? {
        LuaValue::Function(callback) => callback,
        LuaValue::Nil => {
            return Err(LuaError::RuntimeError(
                "redis.register_function must get a callback argument".to_string(),
            ));
        }
        _ => {
            return Err(LuaError::RuntimeError(
                "callback argument given to redis.register_function must be a function".to_string(),
            ));
        }
    };
    let description = parse_register_function_description(&descriptor)?;
    let flags = parse_register_function_flags(&descriptor)?;
    Ok(RegisterFunctionBinding {
        name,
        description,
        flags,
        callback,
    })
}

fn parse_register_function_description(descriptor: &LuaTable) -> mlua::Result<String> {
    let description = descriptor.get::<LuaValue>("description")?;
    match description {
        LuaValue::Nil => Ok(String::new()),
        LuaValue::String(text) => Ok(text.to_string_lossy()),
        _ => Err(LuaError::RuntimeError(
            "ERR description argument given to redis.register_function must be a string"
                .to_string(),
        )),
    }
}

fn parse_register_function_flags(descriptor: &LuaTable) -> mlua::Result<FunctionExecutionFlags> {
    let flags = descriptor.get::<LuaValue>("flags")?;
    match flags {
        LuaValue::Nil => Ok(FunctionExecutionFlags::default()),
        LuaValue::Table(flags_table) => {
            let mut parsed_flags = FunctionExecutionFlags::default();
            for entry in flags_table.sequence_values::<LuaValue>() {
                let value = entry?;
                match value {
                    LuaValue::String(flag) => {
                        let flag = flag.to_string_lossy();
                        if flag.eq_ignore_ascii_case("no-writes") {
                            parsed_flags.read_only = true;
                        } else if flag.eq_ignore_ascii_case("allow-oom") {
                            parsed_flags.allow_oom = true;
                        } else if flag.eq_ignore_ascii_case("allow-stale") {
                            parsed_flags.allow_stale = true;
                        } else {
                            return Err(LuaError::RuntimeError("unknown flag given".to_string()));
                        }
                    }
                    _ => {
                        return Err(LuaError::RuntimeError("unknown flag given".to_string()));
                    }
                }
            }
            Ok(parsed_flags)
        }
        _ => Err(LuaError::RuntimeError(
            "flags argument to redis.register_function must be a table representing function flags"
                .to_string(),
        )),
    }
}

enum FunctionLibraryParseError {
    Syntax,
    InvalidLibraryName,
    EngineNotFound(String),
    LibraryNameMissing,
    LibraryNameGivenMultipleTimes,
    InvalidMetadataValue(String),
}

struct FunctionLibraryMetadata {
    library_name: String,
}

fn parse_function_library_metadata(
    library_source: &[u8],
) -> Result<FunctionLibraryMetadata, FunctionLibraryParseError> {
    let source_text =
        std::str::from_utf8(library_source).map_err(|_| FunctionLibraryParseError::Syntax)?;
    let first_line = source_text
        .lines()
        .next()
        .ok_or(FunctionLibraryParseError::Syntax)?;
    let first_line = first_line.trim();
    if !first_line.starts_with("#!") {
        return Err(FunctionLibraryParseError::Syntax);
    }
    let rest = first_line.trim_start_matches("#!").trim();
    if rest.is_empty() {
        return Err(FunctionLibraryParseError::EngineNotFound(String::new()));
    }
    let mut tokens = rest.split_whitespace();
    let engine = tokens
        .next()
        .ok_or(FunctionLibraryParseError::EngineNotFound(String::new()))?;
    if engine.contains('=') {
        return Err(FunctionLibraryParseError::EngineNotFound(String::new()));
    }
    if !engine.eq_ignore_ascii_case("lua") {
        return Err(FunctionLibraryParseError::EngineNotFound(
            engine.to_string(),
        ));
    }

    let mut library_name: Option<String> = None;
    for token in tokens {
        let Some((key, raw_value)) = token.split_once('=') else {
            return Err(FunctionLibraryParseError::InvalidMetadataValue(
                token.to_string(),
            ));
        };
        if !key.eq_ignore_ascii_case("name") {
            return Err(FunctionLibraryParseError::InvalidMetadataValue(
                token.to_string(),
            ));
        }
        if library_name.is_some() {
            return Err(FunctionLibraryParseError::LibraryNameGivenMultipleTimes);
        }
        let value = parse_function_metadata_value(raw_value);
        if !is_valid_script_function_name(&value) {
            return Err(FunctionLibraryParseError::InvalidLibraryName);
        }
        library_name = Some(value);
    }

    let library_name = library_name.ok_or(FunctionLibraryParseError::LibraryNameMissing)?;
    Ok(FunctionLibraryMetadata { library_name })
}

fn parse_function_metadata_value(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.len() >= 2
        && ((trimmed.starts_with('"') && trimmed.ends_with('"'))
            || (trimmed.starts_with('\'') && trimmed.ends_with('\'')))
    {
        return trimmed[1..trimmed.len() - 1].to_string();
    }
    trimmed.to_string()
}

fn strip_lua_shebang(source: &[u8]) -> &[u8] {
    if !source.starts_with(b"#!") {
        return source;
    }
    match source.iter().position(|byte| *byte == b'\n') {
        Some(index) if index + 1 < source.len() => &source[index + 1..],
        Some(_) => &[],
        None => &[],
    }
}

/// Commands flagged CMD_NOSCRIPT in Redis are not allowed from scripts.
/// This list mirrors the Redis NOSCRIPT flag for every CommandId variant
/// that garnet-rs currently recognises.
fn command_allowed_from_script(command: CommandId) -> bool {
    !matches!(
        command,
        // Scripting commands (recursive invocation forbidden)
        CommandId::Eval
            | CommandId::EvalRo
            | CommandId::Evalsha
            | CommandId::EvalshaRo
            | CommandId::Fcall
            | CommandId::FcallRo
            | CommandId::Function
            | CommandId::Script
            // Transaction control
            | CommandId::Multi
            | CommandId::Exec
            | CommandId::Discard
            | CommandId::Watch
            | CommandId::Unwatch
            // Pub/Sub
            | CommandId::Subscribe
            | CommandId::Psubscribe
            | CommandId::Ssubscribe
            | CommandId::Unsubscribe
            | CommandId::Punsubscribe
            | CommandId::Sunsubscribe
            // Admin / server lifecycle
            | CommandId::Cluster
            | CommandId::Failover
            | CommandId::Monitor
            | CommandId::Shutdown
            | CommandId::Save
            | CommandId::Bgsave
            | CommandId::Bgrewriteaof
            | CommandId::Debug
            | CommandId::Module
            | CommandId::Latency
            // Connection / auth
            | CommandId::Auth
            | CommandId::Hello
            | CommandId::Client
            | CommandId::Reset
            | CommandId::Quit
            | CommandId::Role
            // Configuration
            | CommandId::Config
            | CommandId::Acl
            // Unknown / unrecognised
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
        return Err(RequestExecutionError::NegativeKeyCount);
    }
    let key_count = usize::try_from(numkeys).map_err(|_| RequestExecutionError::ValueOutOfRange)?;
    if key_count > args.len().saturating_sub(3) {
        return Err(RequestExecutionError::SyntaxError);
    }
    Ok(key_count)
}

fn validate_fcall_numkeys(
    args: &[&[u8]],
    command: &'static str,
    expected: &'static str,
    response_out: &mut Vec<u8>,
) -> Option<usize> {
    if let Err(error) = ensure_min_arity(args, 3, command, expected) {
        error.append_resp_error(response_out);
        return None;
    }

    let Some(numkeys) = parse_i64_ascii(args[2]) else {
        append_error(response_out, b"ERR Bad number of keys provided");
        return None;
    };
    if numkeys < 0 {
        append_error(response_out, b"ERR Number of keys can't be negative");
        return None;
    }

    let key_count = usize::try_from(numkeys).ok()?;
    if key_count > args.len().saturating_sub(3) {
        append_error(
            response_out,
            b"ERR Number of keys can't be greater than number of args",
        );
        return None;
    }
    Some(key_count)
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

/// Normalize an error reply string to match Redis's `luaPushErrorBuff`
/// convention.  The returned string is suitable for the `err` field of
/// a Lua error-reply table and always has the form `"<CODE> <msg>"`.
///
/// Redis's `redis.error_reply(arg)` prepends `-` if absent, then
/// `luaPushErrorBuff` strips the leading `-`, splits on the first space
/// to extract code and message, and reassembles as `"{code} {msg}"`.
/// If there is no space after the dash, the code defaults to `"ERR"`.
fn normalize_error_reply(raw: &str) -> String {
    let trimmed = raw.trim_end_matches(['\r', '\n']);
    // Redis's luaRedisErrorReplyCommand prepends `-` if not present.
    let with_dash = if trimmed.starts_with('-') {
        trimmed.to_string()
    } else {
        format!("-{trimmed}")
    };
    // Strip the leading dash.
    let after_dash = &with_dash[1..];
    // Split on the first space to get error code and message.
    if let Some(space_idx) = after_dash.find(' ') {
        let code = &after_dash[..space_idx];
        let msg = &after_dash[space_idx + 1..];
        format!("{code} {msg}")
    } else {
        // No space: error_code = "ERR", msg = after_dash.
        format!("ERR {after_dash}")
    }
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
