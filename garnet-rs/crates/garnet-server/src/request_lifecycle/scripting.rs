use super::*;
use crate::CommandId;
use crate::command_spec::command_is_mutating;
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
const SCRIPT_FUNCTION_LIST_USAGE: &str = "FUNCTION LIST [WITHCODE]";
const SCRIPT_FUNCTION_DELETE_USAGE: &str = "FUNCTION DELETE library-name";
const SCRIPT_FUNCTION_DUMP_USAGE: &str = "FUNCTION DUMP";
const SCRIPT_FUNCTION_FLUSH_USAGE: &str = "FUNCTION FLUSH [ASYNC|SYNC]";
const SCRIPT_FLUSH_USAGE: &str = "SCRIPT FLUSH [ASYNC|SYNC]";
const SCRIPT_LOAD_USAGE: &str = "SCRIPT LOAD script";
const SCRIPT_EXISTS_USAGE: &str = "SCRIPT EXISTS sha1 [sha1 ...]";
const SCRIPT_DEBUG_USAGE: &str = "SCRIPT DEBUG YES|SYNC|NO";
const SCRIPT_TIMEOUT_ERROR_TEXT: &str = "ERR script execution timed out";
const SCRIPT_KILLED_ERROR_TEXT: &str = "ERR script killed by user";
const LUA_TIMEOUT_HOOK_INSTRUCTION_STRIDE: u32 = 1_024;

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
    read_only: bool,
}

struct RegisterFunctionBinding {
    name: String,
    description: String,
    read_only: bool,
    callback: LuaFunction,
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
            ensure_ranged_arity(args, 2, 3, "FUNCTION", SCRIPT_FUNCTION_FLUSH_USAGE)?;
            if args.len() == 3 {
                let flush_mode = args[2];
                if !ascii_eq_ignore_case(flush_mode, b"ASYNC")
                    && !ascii_eq_ignore_case(flush_mode, b"SYNC")
                {
                    return Err(RequestExecutionError::SyntaxError);
                }
            }
            self.clear_function_registry();
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
                Err(RequestExecutionError::InvalidDumpPayload) => {
                    RequestExecutionError::InvalidDumpPayload.append_resp_error(response_out);
                    return Ok(());
                }
                Err(error) => return Err(error),
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
            ensure_ranged_arity(args, 2, 3, "FUNCTION", SCRIPT_FUNCTION_LIST_USAGE)?;
            let with_code = if args.len() == 3 {
                if !ascii_eq_ignore_case(args[2], b"WITHCODE") {
                    return Err(RequestExecutionError::SyntaxError);
                }
                true
            } else {
                false
            };
            self.append_function_list_response(response_out, with_code)?;
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
            let library_name = match parse_function_library_name(library_source) {
                Ok(name) => name,
                Err(FunctionLibraryParseError::InvalidLibraryName) => {
                    append_error(
                        response_out,
                        b"ERR Library names can only contain letters, numbers, or underscores(_)",
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
            let registrations = match self.collect_function_registrations(library_source) {
                Ok(registrations) => registrations,
                Err(RequestExecutionError::SyntaxError) => {
                    append_error(response_out, b"ERR Error compiling function");
                    return Ok(());
                }
                Err(error) => return Err(error),
            };
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
        let (library_source, _read_only) = self.resolve_function_target(function_name)?;
        let key_start = 3;
        let key_end = key_start + key_count;
        self.execute_lua_function(
            &library_source,
            function_name,
            &args[key_start..key_end],
            &args[key_end..],
            ScriptMutability::ReadWrite,
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
        let (library_source, read_only) = self.resolve_function_target(function_name)?;
        if !read_only {
            return Err(RequestExecutionError::FunctionNotReadOnly);
        }
        let key_start = 3;
        let key_end = key_start + key_count;
        self.execute_lua_function(
            &library_source,
            function_name,
            &args[key_start..key_end],
            &args[key_end..],
            ScriptMutability::ReadOnly,
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
            if inserted {
                if let Ok(mut insertion_order) = self.script_cache_insertion_order.lock() {
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

    fn clear_function_registry(&self) {
        if let Ok(mut registry) = self.function_registry.lock() {
            registry.functions.clear();
            registry.library_sources.clear();
            registry.library_function_names.clear();
        }
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
    ) -> Result<(), RequestExecutionError> {
        let libraries = parse_function_dump_payload(serialized_payload)?;
        if matches!(mode, FunctionRestoreMode::Flush) {
            self.clear_function_registry();
        }

        let replace = matches!(mode, FunctionRestoreMode::Replace);
        for (library_name, library_source) in libraries {
            self.validate_script_size_limit(&library_source)?;
            let parsed_name = parse_function_library_name(&library_source)
                .map_err(|_| RequestExecutionError::InvalidDumpPayload)?;
            if parsed_name != library_name {
                return Err(RequestExecutionError::InvalidDumpPayload);
            }
            let registrations = self.collect_function_registrations(&library_source)?;
            self.store_function_library(parsed_name, &library_source, &registrations, replace)?;
        }
        Ok(())
    }

    fn append_function_list_response(
        &self,
        response_out: &mut Vec<u8>,
        with_code: bool,
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
                let (description, read_only) = descriptor
                    .map(|entry| (entry.description.as_str(), entry.read_only))
                    .unwrap_or(("", false));

                response_out.extend_from_slice(b"*6\r\n");
                append_bulk_string(response_out, b"name");
                append_bulk_string(response_out, function_name.as_bytes());
                append_bulk_string(response_out, b"description");
                append_bulk_string(response_out, description.as_bytes());
                append_bulk_string(response_out, b"flags");
                if read_only {
                    response_out.extend_from_slice(b"*1\r\n");
                    append_bulk_string(response_out, b"no-writes");
                } else {
                    response_out.extend_from_slice(b"*0\r\n");
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
        response_out.extend_from_slice(b"*1\r\n");
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
    ) -> Result<Vec<PendingFunctionRegistration>, RequestExecutionError> {
        let lua = Lua::new();
        self.configure_lua_runtime_limits(&lua, None)
            .map_err(|_| RequestExecutionError::SyntaxError)?;
        let registrations: Rc<RefCell<Vec<PendingFunctionRegistration>>> =
            Rc::new(RefCell::new(Vec::new()));
        let registration_names: Rc<RefCell<HashSet<String>>> =
            Rc::new(RefCell::new(HashSet::new()));
        let run_result = lua.scope(|scope| {
            let globals = lua.globals();
            let redis_table = lua.create_table()?;
            let registrations_ref = Rc::clone(&registrations);
            let registration_names_ref = Rc::clone(&registration_names);
            let register_function_fn = scope.create_function(move |_lua, values: MultiValue| {
                let binding = parse_register_function_binding(values)?;
                let normalized_name = binding.name.to_ascii_lowercase();
                let mut names = registration_names_ref.borrow_mut();
                if !names.insert(normalized_name.clone()) {
                    return Err(LuaError::RuntimeError(
                        "ERR Function already registered".to_string(),
                    ));
                }
                registrations_ref
                    .borrow_mut()
                    .push(PendingFunctionRegistration {
                        name: normalized_name,
                        description: binding.description,
                        read_only: binding.read_only,
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
            globals.set("redis", redis_table)?;
            lua.load(strip_lua_shebang(library_source)).exec()?;
            Ok::<(), LuaError>(())
        });

        match run_result {
            Ok(()) => {}
            Err(error) => {
                if lua_error_is_timeout(&error) {
                    self.record_script_runtime_timeout();
                    return Err(RequestExecutionError::ScriptExecutionTimedOut);
                }
                return Err(RequestExecutionError::SyntaxError);
            }
        }

        let registrations = registrations.borrow().clone();
        if registrations.is_empty() {
            return Err(RequestExecutionError::SyntaxError);
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
            if let Some(existing) = registry.functions.get(&normalized_name) {
                if existing.library_name != library_name
                    && !existing_library_name_set.contains(&normalized_name)
                {
                    return Err(RequestExecutionError::FunctionNameAlreadyExists);
                }
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
                    read_only: registration.read_only,
                },
            );
        }
        registry
            .library_function_names
            .insert(library_name, registered_names);
        Ok(())
    }

    fn resolve_function_target(
        &self,
        function_name: &[u8],
    ) -> Result<(Vec<u8>, bool), RequestExecutionError> {
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
        Ok((source.clone(), descriptor.read_only))
    }

    fn execute_lua_script(
        &self,
        script: &[u8],
        keys: &[&[u8]],
        argv: &[&[u8]],
        mutability: ScriptMutability,
        response_out: &mut Vec<u8>,
    ) {
        let _running_script_guard =
            match self.enter_running_script(RunningScriptKind::Script, "", "eval".to_string()) {
                Ok(guard) => guard,
                Err(error) => {
                    error.append_resp_error(response_out);
                    return;
                }
            };
        let lua = Lua::new();
        if let Err(error) = self.configure_lua_runtime_limits(&lua, Some(RunningScriptKind::Script))
        {
            let message = format!(
                "ERR Error running script: {}",
                sanitize_error_text(&error.to_string())
            );
            append_error(response_out, message.as_bytes());
            return;
        }
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
                if lua_error_is_timeout(&error) {
                    self.record_script_runtime_timeout();
                    append_error(response_out, SCRIPT_TIMEOUT_ERROR_TEXT.as_bytes());
                    return;
                }
                let message = format!(
                    "ERR Error running script: {}",
                    sanitize_error_text(&error.to_string())
                );
                append_error(response_out, message.as_bytes());
            }
        }
    }

    fn execute_lua_function(
        &self,
        library_source: &[u8],
        function_name: &[u8],
        keys: &[&[u8]],
        argv: &[&[u8]],
        mutability: ScriptMutability,
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
            let registered_functions = lua.create_table()?;
            let registered_functions_ref = registered_functions.clone();
            let register_function_fn = scope.create_function(move |_lua, values: MultiValue| {
                let binding = parse_register_function_binding(values)?;
                let normalized_name = binding.name.to_ascii_lowercase();
                registered_functions_ref.set(normalized_name.as_str(), binding.callback)?;
                Ok(())
            })?;

            redis_table.set("call", call_fn)?;
            redis_table.set("pcall", pcall_fn)?;
            redis_table.set("sha1hex", sha1hex_fn)?;
            redis_table.set("status_reply", status_reply_fn)?;
            redis_table.set("error_reply", error_reply_fn)?;
            redis_table.set("register_function", register_function_fn)?;
            redis_table.set("log", log_fn)?;
            redis_table.set("LOG_DEBUG", 0)?;
            redis_table.set("LOG_VERBOSE", 1)?;
            redis_table.set("LOG_NOTICE", 2)?;
            redis_table.set("LOG_WARNING", 3)?;
            globals.set("redis", redis_table)?;

            lua.load(strip_lua_shebang(library_source)).exec()?;

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
            let value: LuaValue = callback.call((keys_table, argv_table))?;
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
                let message = format!(
                    "ERR Error running function: {}",
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
            lua.set_hook(
                HookTriggers::new().every_nth_instruction(LUA_TIMEOUT_HOOK_INSTRUCTION_STRIDE),
                move |_lua, _debug| {
                    if let Some(flag) = &kill_flag {
                        if flag.load(Ordering::Acquire) {
                            return Err(LuaError::RuntimeError(
                                SCRIPT_KILLED_ERROR_TEXT.to_string(),
                            ));
                        }
                    }
                    if let Some(timeout) = timeout {
                        if started.elapsed() > timeout {
                            return Err(LuaError::RuntimeError(
                                SCRIPT_TIMEOUT_ERROR_TEXT.to_string(),
                            ));
                        }
                    }
                    Ok(VmState::Continue)
                },
            )?;
        }
        Ok(())
    }
}

fn lua_error_is_timeout(error: &LuaError) -> bool {
    error.to_string().contains(SCRIPT_TIMEOUT_ERROR_TEXT)
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

fn parse_register_function_binding(values: MultiValue) -> mlua::Result<RegisterFunctionBinding> {
    if values.is_empty() {
        return Err(LuaError::RuntimeError(
            "ERR redis.register_function requires arguments".to_string(),
        ));
    }

    if values.len() == 1 {
        match values.into_iter().next().expect("single value must exist") {
            LuaValue::Table(descriptor) => parse_register_function_binding_from_table(descriptor),
            _ => Err(LuaError::RuntimeError(
                "ERR redis.register_function expects (name, callback) or descriptor table"
                    .to_string(),
            )),
        }
    } else if values.len() == 2 {
        let mut values = values.into_iter();
        let name = match values.next().expect("first value must exist") {
            LuaValue::String(name) => name.to_string_lossy(),
            _ => {
                return Err(LuaError::RuntimeError(
                    "ERR redis.register_function name must be a string".to_string(),
                ));
            }
        };
        let callback = match values.next().expect("second value must exist") {
            LuaValue::Function(callback) => callback,
            _ => {
                return Err(LuaError::RuntimeError(
                    "ERR redis.register_function callback must be a function".to_string(),
                ));
            }
        };
        let name = name.trim();
        if name.is_empty() {
            return Err(LuaError::RuntimeError(
                "ERR redis.register_function name must not be empty".to_string(),
            ));
        }
        Ok(RegisterFunctionBinding {
            name: name.to_string(),
            description: String::new(),
            read_only: false,
            callback,
        })
    } else {
        Err(LuaError::RuntimeError(
            "ERR redis.register_function received too many arguments".to_string(),
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FunctionRestoreMode {
    Append,
    Flush,
    Replace,
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
    let name: Option<String> = descriptor.get("function_name")?;
    let name = match name {
        Some(name) => Some(name),
        None => descriptor.get("name")?,
    }
    .ok_or_else(|| {
        LuaError::RuntimeError(
            "ERR redis.register_function descriptor requires function_name".to_string(),
        )
    })?;
    let name = name.trim();
    if name.is_empty() {
        return Err(LuaError::RuntimeError(
            "ERR redis.register_function name must not be empty".to_string(),
        ));
    }

    let callback: LuaFunction = descriptor.get("callback").map_err(|_| {
        LuaError::RuntimeError(
            "ERR redis.register_function descriptor requires callback function".to_string(),
        )
    })?;
    let description = parse_register_function_description(&descriptor)?;
    let read_only = parse_register_function_flags(&descriptor)?;
    Ok(RegisterFunctionBinding {
        name: name.to_string(),
        description,
        read_only,
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

fn parse_register_function_flags(descriptor: &LuaTable) -> mlua::Result<bool> {
    let flags = descriptor.get::<LuaValue>("flags")?;
    match flags {
        LuaValue::Nil => Ok(false),
        LuaValue::String(flag) => Ok(flag.to_string_lossy().eq_ignore_ascii_case("no-writes")),
        LuaValue::Table(flags_table) => {
            for entry in flags_table.sequence_values::<LuaValue>() {
                let value = entry?;
                match value {
                    LuaValue::String(flag) => {
                        if flag.to_string_lossy().eq_ignore_ascii_case("no-writes") {
                            return Ok(true);
                        }
                    }
                    _ => {
                        return Err(LuaError::RuntimeError(
                            "ERR redis.register_function flags entries must be strings".to_string(),
                        ));
                    }
                }
            }
            Ok(false)
        }
        _ => Err(LuaError::RuntimeError(
            "ERR redis.register_function flags must be a string or array".to_string(),
        )),
    }
}

enum FunctionLibraryParseError {
    Syntax,
    InvalidLibraryName,
    EngineNotFound(String),
}

fn parse_function_library_name(library_source: &[u8]) -> Result<String, FunctionLibraryParseError> {
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
    let mut tokens = rest.split_whitespace();
    let engine = tokens.next().ok_or(FunctionLibraryParseError::Syntax)?;
    if !engine.eq_ignore_ascii_case("lua") {
        return Err(FunctionLibraryParseError::EngineNotFound(
            engine.to_string(),
        ));
    }
    for token in tokens {
        if let Some(name) = token.strip_prefix("name=") {
            if !name.is_empty()
                && name
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
            {
                return Ok(name.to_string());
            }
            return Err(FunctionLibraryParseError::InvalidLibraryName);
        }
    }
    Err(FunctionLibraryParseError::Syntax)
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
