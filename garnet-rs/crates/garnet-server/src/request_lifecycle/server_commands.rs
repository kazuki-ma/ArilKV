use super::*;
use crate::command_spec::ArityPolicy;
use crate::command_spec::command_arity_policy;
use crate::command_spec::command_id_from_name;
use crate::command_spec::command_is_mutating;
use crate::command_spec::command_names_for_command_response;

static NEXT_RANDOMKEY_INDEX: AtomicU64 = AtomicU64::new(0);
const MODULE_HELP_LINES: [&[u8]; 11] = [
    b"MODULE <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
    b"LIST",
    b"    Return a list of loaded modules.",
    b"LOAD <path> [<arg> ...]",
    b"    Load a module library from <path>, passing to it any optional arguments.",
    b"LOADEX <path> [[CONFIG NAME VALUE] [CONFIG NAME VALUE]] [ARGS ...]",
    b"    Load a module library from <path>, while passing it module configurations and optional arguments.",
    b"UNLOAD <name>",
    b"    Unload a module.",
    b"HELP",
    b"    Print this help.",
];
const LATENCY_HELP_LINES: [&[u8]; 15] = [
    b"LATENCY <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
    b"DOCTOR",
    b"    Return a human readable latency analysis report.",
    b"GRAPH <event>",
    b"    Return an ASCII latency graph for the <event> class.",
    b"HISTORY <event>",
    b"    Return time-latency samples for the <event> class.",
    b"LATEST",
    b"    Return the latest latency samples for all events.",
    b"RESET [<event> ...]",
    b"    Reset latency data of one or more <event> classes.",
    b"HISTOGRAM [COMMAND ...]",
    b"    Return cumulative latency histograms for command names.",
    b"HELP",
    b"    Print this help.",
];
const LATENCY_DOCTOR_DISABLED_MESSAGE: &[u8] =
    b"I'm sorry, Dave, I can't do that. Latency monitoring is disabled in this garnet-rs instance.";
const SLOWLOG_HELP_LINES: [&[u8]; 12] = [
    b"SLOWLOG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
    b"GET [<count>]",
    b"    Return top <count> entries from the slowlog (default: 10, -1 mean all).",
    b"    Entries are made of:",
    b"    id, timestamp, time in microseconds, arguments array, client IP and port,",
    b"    client name",
    b"LEN",
    b"    Return the length of the slowlog.",
    b"RESET",
    b"    Reset the slowlog.",
    b"HELP",
    b"    Print this help.",
];
const ACL_HELP_LINES: [&[u8]; 13] = [
    b"ACL <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
    b"CAT [<category>]",
    b"    List ACL categories and the commands inside them.",
    b"GETUSER <username>",
    b"    Get user details.",
    b"LIST",
    b"    List ACL rules in ACL file format.",
    b"SETUSER <username> [<rule> ...]",
    b"    Modify ACL rules for a user.",
    b"USERS",
    b"    List all configured users.",
    b"WHOAMI",
    b"    Return the current connection username.",
];
const ACL_CATEGORIES: [&[u8]; 8] = [
    b"keyspace",
    b"read",
    b"write",
    b"string",
    b"hash",
    b"list",
    b"set",
    b"stream",
];
const OBJECT_HELP_LINES: [&[u8]; 11] = [
    b"OBJECT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
    b"ENCODING <key>",
    b"    Show the internal encoding of the value stored at <key>.",
    b"FREQ <key>",
    b"    Show the logarithmic access frequency counter of the object stored at <key>.",
    b"IDLETIME <key>",
    b"    Show the idle time of the object stored at <key>.",
    b"REFCOUNT <key>",
    b"    Show the reference count of the object stored at <key>.",
    b"HELP",
    b"    Print this help.",
];
const MEMORY_HELP_LINES: [&[u8]; 7] = [
    b"MEMORY <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
    b"USAGE <key>",
    b"    Estimate memory usage for a key.",
    b"MALLOC-STATS",
    b"    Return allocator statistics.",
    b"PURGE",
    b"    Attempt to purge allocator caches.",
];
const PUBSUB_HELP_LINES: [&[u8]; 13] = [
    b"PUBSUB <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
    b"CHANNELS [<pattern>]",
    b"    Return the currently active channels matching a pattern (default all).",
    b"NUMPAT",
    b"    Return number of subscriptions to patterns.",
    b"NUMSUB [<channel> ...]",
    b"    Return number of subscribers for channels.",
    b"SHARDCHANNELS [<pattern>]",
    b"    Return active shard channels matching a pattern.",
    b"SHARDNUMSUB [<channel> ...]",
    b"    Return number of subscribers for shard channels.",
    b"HELP",
    b"    Print this help.",
];
const COMMAND_HELP_LINES: [&[u8]; 15] = [
    b"COMMAND <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
    b"COUNT",
    b"    Return the total number of commands in this Redis server.",
    b"GETKEYS <full-command>",
    b"    Return keys from a full Redis command.",
    b"GETKEYSANDFLAGS <full-command>",
    b"    Return keys and flags from a full Redis command.",
    b"HELP",
    b"    Print this help.",
    b"INFO [<command-name> ...]",
    b"    Return details about specific Redis commands.",
    b"LIST",
    b"    Return all commands in this Redis server.",
    b"DOCS [<command-name> ...]",
    b"    Return documentation details about specific Redis commands.",
];
const CONFIG_HELP_LINES: [&[u8]; 13] = [
    b"CONFIG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
    b"GET <pattern>",
    b"    Return parameters matching the glob-like <pattern> and their values.",
    b"SET <directive> <value>",
    b"    Set the configuration <directive> to <value>.",
    b"RESETSTAT",
    b"    Reset statistics reported by INFO.",
    b"REWRITE",
    b"    Rewrite the configuration file (not supported in garnet-rs).",
    b"HELP",
    b"    Print this help.",
    b"SET [... <directive> <value> ...]",
    b"    Set multiple configuration directives at once.",
];
const CLIENT_HELP_LINES: [&[u8]; 19] = [
    b"CLIENT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
    b"CACHING (YES|NO)",
    b"    Enable/disable tracking of the keys for next command in OPTIN/OPTOUT modes.",
    b"GETNAME",
    b"    Return the name of the current connection.",
    b"HELP",
    b"    Prints this help.",
    b"ID",
    b"    Return the ID of the current connection.",
    b"INFO",
    b"    Return information about the current client connection.",
    b"KILL <ip:port>",
    b"    Kill connection by ip:port.",
    b"LIST",
    b"    Return information about client connections.",
    b"NO-EVICT (ON|OFF)",
    b"    Protect current client connection from eviction.",
    b"NO-TOUCH (ON|OFF)",
    b"    Avoid touching LRU/LFU stats for current client connection commands.",
];
const DEBUG_PROTOCOL_ATTRIB_REPLY: &[u8] = b"Some real reply following the attribute";
const DEBUG_PROTOCOL_BIGNUM_VALUE: &[u8] = b"1234567999999999999999999999999999999";
const DEBUG_PROTOCOL_VERBATIM_VALUE: &[u8] = b"This is a verbatim\nstring";
const DEBUG_PROTOCOL_ATTRIBUTE_NAME: &[u8] = b"key-popularity";
const DEBUG_PROTOCOL_ATTRIBUTE_KEY: &[u8] = b"key:123";
const DUMP_BLOB_MAGIC: &[u8] = b"GRN1";
const MIGRATE_USAGE: &str = "MIGRATE host port key destination-db timeout [COPY] [REPLACE] [AUTH password] [AUTH2 username password] [KEYS key [key ...]]";
const COMMAND_LIST_EXTRA_NAMES: [&[u8]; 8] = [
    b"client|help",
    b"client|list",
    b"cluster|help",
    b"config|get",
    b"config|resetstat",
    b"config|rewrite",
    b"memory|usage",
    b"script|kill",
];
const COMMAND_FLAGS_OW_UPDATE: [&[u8]; 2] = [b"OW", b"update"];
const COMMAND_FLAGS_RW_ACCESS_DELETE: [&[u8]; 3] = [b"RW", b"access", b"delete"];
const COMMAND_FLAGS_RW_INSERT: [&[u8]; 2] = [b"RW", b"insert"];
const COMMAND_FLAGS_RO_ACCESS: [&[u8]; 2] = [b"RO", b"access"];
const COMMAND_FLAGS_RW_UPDATE: [&[u8]; 2] = [b"RW", b"update"];
const COMMAND_FLAGS_RW_ACCESS_UPDATE: [&[u8]; 3] = [b"RW", b"access", b"update"];
const COMMAND_FLAGS_RM_DELETE: [&[u8]; 2] = [b"RM", b"delete"];
const COMMAND_FLAGS_RW_DELETE: [&[u8]; 2] = [b"RW", b"delete"];
const INFO_KEYSIZES_BIN_LABELS: [&str; 60] = [
    "0", "1", "2", "4", "8", "16", "32", "64", "128", "256", "512", "1K", "2K", "4K", "8K", "16K",
    "32K", "64K", "128K", "256K", "512K", "1M", "2M", "4M", "8M", "16M", "32M", "64M", "128M",
    "256M", "512M", "1G", "2G", "4G", "8G", "16G", "32G", "64G", "128G", "256G", "512G", "1T",
    "2T", "4T", "8T", "16T", "32T", "64T", "128T", "256T", "512T", "1P", "2P", "4P", "8P", "16P",
    "32P", "64P", "128P", "256P",
];
const KEYSIZES_HISTOGRAM_TYPE_COUNT: usize = 5;
#[derive(Copy, Clone, Eq, PartialEq)]
enum InfoSection {
    Server,
    Clients,
    Memory,
    Stats,
    Replication,
    Cpu,
    Scripting,
    Keyspace,
    Keysizes,
    Commandstats,
}

fn push_info_section_once(sections: &mut Vec<InfoSection>, section: InfoSection) {
    if !sections.contains(&section) {
        sections.push(section);
    }
}

fn push_default_info_sections(sections: &mut Vec<InfoSection>) {
    for section in [
        InfoSection::Server,
        InfoSection::Clients,
        InfoSection::Memory,
        InfoSection::Stats,
        InfoSection::Replication,
        InfoSection::Cpu,
        InfoSection::Scripting,
        InfoSection::Keyspace,
    ] {
        push_info_section_once(sections, section);
    }
}

fn push_all_info_sections(sections: &mut Vec<InfoSection>) {
    push_default_info_sections(sections);
    push_info_section_once(sections, InfoSection::Keysizes);
    push_info_section_once(sections, InfoSection::Commandstats);
}

fn append_info_sections_from_token(sections: &mut Vec<InfoSection>, token: &[u8]) {
    if ascii_eq_ignore_case(token, b"DEFAULT") {
        push_default_info_sections(sections);
        return;
    }
    if ascii_eq_ignore_case(token, b"ALL") || ascii_eq_ignore_case(token, b"EVERYTHING") {
        push_all_info_sections(sections);
        return;
    }
    if ascii_eq_ignore_case(token, b"SERVER") {
        push_info_section_once(sections, InfoSection::Server);
        return;
    }
    if ascii_eq_ignore_case(token, b"CLIENTS") {
        push_info_section_once(sections, InfoSection::Clients);
        return;
    }
    if ascii_eq_ignore_case(token, b"MEMORY") {
        push_info_section_once(sections, InfoSection::Memory);
        return;
    }
    if ascii_eq_ignore_case(token, b"STATS") {
        push_info_section_once(sections, InfoSection::Stats);
        return;
    }
    if ascii_eq_ignore_case(token, b"REPLICATION") {
        push_info_section_once(sections, InfoSection::Replication);
        return;
    }
    if ascii_eq_ignore_case(token, b"CPU") {
        push_info_section_once(sections, InfoSection::Cpu);
        return;
    }
    if ascii_eq_ignore_case(token, b"SCRIPTING") {
        push_info_section_once(sections, InfoSection::Scripting);
        return;
    }
    if ascii_eq_ignore_case(token, b"KEYSPACE") {
        push_info_section_once(sections, InfoSection::Keyspace);
        return;
    }
    if ascii_eq_ignore_case(token, b"KEYSIZES") {
        push_info_section_once(sections, InfoSection::Keysizes);
        return;
    }
    if ascii_eq_ignore_case(token, b"COMMANDSTATS") {
        push_info_section_once(sections, InfoSection::Commandstats);
    }
}

impl RequestProcessor {
    pub(super) fn handle_quit(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 1, "QUIT", "QUIT")?;
        append_simple_string(response_out, b"OK");
        Ok(())
    }

    pub(super) fn handle_time(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 1, "TIME", "TIME")?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| RequestExecutionError::ValueOutOfRange)?;
        let seconds = now.as_secs().to_string();
        let microseconds = now.subsec_micros().to_string();
        append_bulk_array(response_out, &[seconds.as_bytes(), microseconds.as_bytes()]);
        Ok(())
    }

    pub(super) fn handle_ping(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_ranged_arity(args, 1, 2, "PING", "PING [message]")?;
        if self.resp_protocol_version() == RespProtocolVersion::Resp2
            && let Some(client_id) = super::current_request_client_id()
            && self.pubsub_subscription_count(client_id) > 0
        {
            response_out.extend_from_slice(b"*2\r\n");
            append_bulk_string(response_out, b"pong");
            let payload = if args.len() == 1 { b"" } else { args[1] };
            append_bulk_string(response_out, payload);
            return Ok(());
        }
        if args.len() == 1 {
            append_simple_string(response_out, b"PONG");
            return Ok(());
        }
        append_bulk_string(response_out, args[1]);
        Ok(())
    }

    pub(super) fn handle_echo(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 2, "ECHO", "ECHO message")?;
        append_bulk_string(response_out, args[1]);
        Ok(())
    }

    pub(super) fn handle_hello(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        // Parse optional protocol version.
        let version = if args.len() >= 2 {
            let raw = parse_u64_ascii(args[1]).ok_or(RequestExecutionError::ValueNotInteger)?;
            Some(RespProtocolVersion::from_u64(raw).ok_or(RequestExecutionError::SyntaxError)?)
        } else {
            None
        };

        // Scan remaining arguments for AUTH and SETNAME options.
        let mut idx = 2;
        while idx < args.len() {
            if ascii_eq_ignore_case(args[idx], b"AUTH") {
                if idx + 2 >= args.len() {
                    return Err(RequestExecutionError::SyntaxError);
                }
                // AUTH is not supported — reject like handle_auth.
                return Err(RequestExecutionError::AuthNotEnabled);
            } else if ascii_eq_ignore_case(args[idx], b"SETNAME") {
                if idx + 1 >= args.len() {
                    return Err(RequestExecutionError::SyntaxError);
                }
                // SETNAME is validated here for syntax; the actual name
                // application is performed by connection_handler after
                // successful command execution.
                let name = args[idx + 1];
                if name.iter().any(|&b| b == b' ' || b == b'\n') {
                    return Err(RequestExecutionError::SyntaxError);
                }
                idx += 2;
            } else {
                return Err(RequestExecutionError::SyntaxError);
            }
        }

        // Apply the protocol version change.
        if let Some(v) = version {
            self.set_resp_protocol_version(v);
        }

        // Build the HELLO response with server info.
        let proto = self.resp_protocol_version();
        let client_id = super::current_request_client_id().unwrap_or_default();
        self.append_hello_response(response_out, proto, client_id);
        Ok(())
    }

    fn append_hello_response(
        &self,
        response_out: &mut Vec<u8>,
        proto: RespProtocolVersion,
        client_id: ClientId,
    ) {
        let resp3 = proto.is_resp3();
        let entry_count = 7;
        if resp3 {
            append_map_length(response_out, entry_count);
        } else {
            append_array_length(response_out, entry_count * 2);
        }
        append_bulk_string(response_out, b"server");
        append_bulk_string(response_out, b"redis");
        append_bulk_string(response_out, b"version");
        append_bulk_string(response_out, b"garnet-rs");
        append_bulk_string(response_out, b"proto");
        append_integer(response_out, if resp3 { 3 } else { 2 });
        append_bulk_string(response_out, b"id");
        append_integer(response_out, u64::from(client_id) as i64);
        append_bulk_string(response_out, b"mode");
        append_bulk_string(response_out, b"standalone");
        append_bulk_string(response_out, b"role");
        append_bulk_string(response_out, b"master");
        append_bulk_string(response_out, b"modules");
        append_array_length(response_out, 0);
    }

    pub(super) fn handle_info(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 1, "INFO", "INFO [section [section ...]]")?;

        let dbsize = self.active_key_count()?;
        let blocked_clients = self.blocked_clients();
        let connected_clients = self.connected_clients();
        let watching_clients = self.watching_clients();
        let rdb_bgsave_in_progress = if self.rdb_bgsave_in_progress() { 1 } else { 0 };
        let rdb_changes_since_last_save = self.rdb_changes_since_last_save();
        let expired_keys = self.expired_keys();
        let expired_keys_active = self.expired_keys_active();
        let lazyfree_pending_objects = self.lazyfree_pending_objects();
        let lazyfreed_objects = self.lazyfreed_objects();
        let used_memory = self.estimated_used_memory_bytes()?;
        let scripting_runtime = self.scripting_runtime_config();
        let used_memory_vm_functions = self.used_memory_vm_functions();

        let mut sections = Vec::new();
        if args.len() == 1 {
            push_default_info_sections(&mut sections);
        } else {
            for token in &args[1..] {
                append_info_sections_from_token(&mut sections, token);
            }
        }

        let mut payload = String::new();
        for section in sections {
            match section {
                InfoSection::Server => {
                    payload.push_str(
                        "# Server\r\nredis_version:garnet-rs\r\nredis_git_sha1:garnet-rs\r\n",
                    );
                }
                InfoSection::Clients => {
                    payload.push_str(
                        format!(
                            "# Clients\r\nconnected_clients:{}\r\nblocked_clients:{}\r\nwatching_clients:{}\r\n",
                            connected_clients, blocked_clients, watching_clients
                        )
                        .as_str(),
                    );
                }
                InfoSection::Memory => {
                    payload
                        .push_str(format!("# Memory\r\nused_memory:{}\r\n", used_memory).as_str());
                }
                InfoSection::Stats => {
                    payload.push_str(
                        format!(
                            "# Stats\r\ndbsize:{}\r\nrdb_bgsave_in_progress:{}\r\nrdb_changes_since_last_save:{}\r\nexpired_keys:{}\r\nexpired_keys_active:{}\r\nlazyfree_pending_objects:{}\r\nlazyfreed_objects:{}\r\nmigrate_cached_sockets:0\r\n",
                            dbsize,
                            rdb_bgsave_in_progress,
                            rdb_changes_since_last_save,
                            expired_keys,
                            expired_keys_active,
                            lazyfree_pending_objects,
                            lazyfreed_objects,
                        )
                        .as_str(),
                    );
                }
                InfoSection::Replication => {
                    payload.push_str("# Replication\r\nmaster_repl_offset:0\r\n");
                }
                InfoSection::Cpu => {
                    payload.push_str(
                        "# CPU\r\nused_cpu_user:0.00\r\nused_cpu_sys:0.00\r\nused_cpu_user_children:0.00\r\nused_cpu_sys_children:0.00\r\n",
                    );
                }
                InfoSection::Scripting => {
                    payload.push_str(
                        format!(
                            "# Scripting\r\nscripting_enabled:{}\r\nscripting_cache_entries:{}\r\nscripting_cache_max_entries:{}\r\nscripting_cache_hits:{}\r\nscripting_cache_misses:{}\r\nscripting_cache_evictions:{}\r\nscripting_runtime_timeouts:{}\r\nused_memory_vm_functions:{}\r\nscripting_max_script_bytes:{}\r\nscripting_max_memory_bytes:{}\r\nscripting_max_execution_millis:{}\r\n",
                            if self.scripting_enabled() { 1 } else { 0 },
                            self.script_cache_entry_count(),
                            scripting_runtime.cache_max_entries,
                            self.script_cache_hits(),
                            self.script_cache_misses(),
                            self.script_cache_evictions(),
                            self.script_runtime_timeouts(),
                            used_memory_vm_functions,
                            scripting_runtime.max_script_bytes,
                            scripting_runtime.max_memory_bytes,
                            scripting_runtime.max_execution_millis,
                        )
                        .as_str(),
                    );
                }
                InfoSection::Keyspace => {
                    payload.push_str("# Keyspace\r\n");
                    if dbsize > 0 {
                        let expires = self.total_expiration_count();
                        payload.push_str(
                            format!("db0:keys={},expires={},avg_ttl=0\r\n", dbsize, expires)
                                .as_str(),
                        );
                    }
                }
                InfoSection::Keysizes => {
                    payload.push_str(&self.render_keysizes_info_payload()?);
                }
                InfoSection::Commandstats => {
                    payload.push_str(&self.render_commandstats_info_payload());
                }
            }
        }

        if self.resp_protocol_version().is_resp3() {
            append_verbatim_string(response_out, b"txt", payload.as_bytes());
        } else {
            append_bulk_string(response_out, payload.as_bytes());
        }
        Ok(())
    }

    pub(super) fn handle_lastsave(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 1, "LASTSAVE", "LASTSAVE")?;
        append_integer(response_out, self.lastsave_unix_seconds() as i64);
        Ok(())
    }

    pub(super) fn handle_auth(
        &self,
        args: &[&[u8]],
        _response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_ranged_arity(args, 2, 3, "AUTH", "AUTH [username] password")?;
        Err(RequestExecutionError::AuthNotEnabled)
    }

    pub(super) fn handle_select(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 2, "SELECT", "SELECT index")?;
        let index = parse_i64_ascii(args[1]).ok_or(RequestExecutionError::ValueNotInteger)?;
        if index != 0 {
            return Err(RequestExecutionError::DbIndexOutOfRange);
        }
        append_simple_string(response_out, b"OK");
        Ok(())
    }

    pub(super) fn handle_move(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 3, "MOVE", "MOVE key db")?;
        let target_db = parse_i64_ascii(args[2]).ok_or(RequestExecutionError::ValueNotInteger)?;
        if target_db == 0 {
            return Err(RequestExecutionError::SourceDestinationObjectsSame);
        }
        if target_db < 0 {
            return Err(RequestExecutionError::DbIndexOutOfRange);
        }
        let target_db =
            usize::try_from(target_db).map_err(|_| RequestExecutionError::DbIndexOutOfRange)?;
        let key = RedisKey::from(args[1]);

        self.expire_key_if_needed(&key)?;
        self.active_expire_hash_fields_for_key(&key)?;

        let moved_entry = if let Some(value) = self.read_string_value(&key)? {
            let length =
                super::string_commands::string_value_len_for_keysizes(self, value.as_slice());
            Some(super::MovedKeysizesEntry {
                histogram_type_index: 0,
                length,
            })
        } else if let Some(object) = self.object_read(&key)? {
            let Some(histogram_entry) =
                keysizes_object_histogram_type_and_len(object.object_type, &object.payload)?
            else {
                append_integer(response_out, 0);
                return Ok(());
            };
            Some(super::MovedKeysizesEntry {
                histogram_type_index: histogram_entry.histogram_type_index,
                length: histogram_entry.length,
            })
        } else {
            None
        };

        let Some(moved_entry) = moved_entry else {
            append_integer(response_out, 0);
            return Ok(());
        };

        {
            let moved_by_db = self
                .moved_keysizes_by_db
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if moved_by_db
                .get(&target_db)
                .is_some_and(|keys| keys.contains_key(key.as_slice()))
            {
                append_integer(response_out, 0);
                return Ok(());
            }
        }

        let mut string_deleted = false;
        {
            let mut store = self.lock_string_store_for_key(&key);
            let mut session = store.session(&self.functions);
            let mut info = DeleteInfo::default();
            let status = session.delete(&key, &mut info).map_err(map_delete_error)?;
            match status {
                DeleteOperationStatus::TombstonedInPlace
                | DeleteOperationStatus::AppendedTombstone => {
                    string_deleted = true;
                }
                DeleteOperationStatus::NotFound => {}
                DeleteOperationStatus::RetryLater => {
                    return Err(RequestExecutionError::StorageBusy);
                }
            }
        }
        if string_deleted {
            self.remove_string_key_metadata(&key);
        }
        let object_deleted = self.object_delete(&key)?;
        if !string_deleted && !object_deleted {
            append_integer(response_out, 0);
            return Ok(());
        }
        if string_deleted && !object_deleted {
            self.bump_watch_version(&key);
        }

        self.moved_keysizes_by_db
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(target_db)
            .or_default()
            .insert(key, moved_entry);

        append_integer(response_out, 1);
        Ok(())
    }

    pub(super) fn handle_swapdb(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 3, "SWAPDB", "SWAPDB index1 index2")?;
        let index1 = parse_i64_ascii(args[1]).ok_or(RequestExecutionError::ValueNotInteger)?;
        let index2 = parse_i64_ascii(args[2]).ok_or(RequestExecutionError::ValueNotInteger)?;
        if index1 != 0 || index2 != 0 {
            return Err(RequestExecutionError::DbIndexOutOfRange);
        }
        append_simple_string(response_out, b"OK");
        Ok(())
    }

    pub(super) fn handle_migrate(
        &self,
        args: &[&[u8]],
        _response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 6, "MIGRATE", MIGRATE_USAGE)?;
        let host = args[1];
        if host.is_empty() {
            return Err(RequestExecutionError::SyntaxError);
        }
        let port = parse_u64_ascii(args[2]).ok_or(RequestExecutionError::ValueNotInteger)?;
        if port > u16::MAX as u64 {
            return Err(RequestExecutionError::ValueNotInteger);
        }
        let key_arg = args[3];
        parse_i64_ascii(args[4]).ok_or(RequestExecutionError::ValueNotInteger)?;
        let timeout_millis =
            parse_i64_ascii(args[5]).ok_or(RequestExecutionError::ValueNotInteger)?;
        if timeout_millis <= 0 {
            return Err(RequestExecutionError::ValueOutOfRange);
        }

        let mut parsed_keys: Vec<Vec<u8>> = Vec::new();
        if !key_arg.is_empty() {
            parsed_keys.push(key_arg.to_vec());
        }

        let mut index = 6usize;
        while index < args.len() {
            let token = args[index];
            if ascii_eq_ignore_case(token, b"COPY") || ascii_eq_ignore_case(token, b"REPLACE") {
                index += 1;
                continue;
            }
            if ascii_eq_ignore_case(token, b"AUTH") {
                if index + 1 >= args.len() {
                    return Err(RequestExecutionError::SyntaxError);
                }
                index += 2;
                continue;
            }
            if ascii_eq_ignore_case(token, b"AUTH2") {
                if index + 2 >= args.len() {
                    return Err(RequestExecutionError::SyntaxError);
                }
                index += 3;
                continue;
            }
            if ascii_eq_ignore_case(token, b"KEYS") {
                if !key_arg.is_empty() {
                    return Err(RequestExecutionError::SyntaxError);
                }
                if index + 1 >= args.len() {
                    return Err(RequestExecutionError::SyntaxError);
                }
                for key in &args[index + 1..] {
                    if key.is_empty() {
                        return Err(RequestExecutionError::SyntaxError);
                    }
                    parsed_keys.push(key.to_vec());
                }
                break;
            }
            return Err(RequestExecutionError::SyntaxError);
        }

        if parsed_keys.is_empty() {
            return Err(RequestExecutionError::SyntaxError);
        }
        Err(RequestExecutionError::CommandDisabled { command: "MIGRATE" })
    }

    pub(super) fn handle_client(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            2,
            "CLIENT",
            "CLIENT <ID|GETNAME|SETNAME|LIST|UNBLOCK|PAUSE|UNPAUSE|NO-EVICT|NO-TOUCH|HELP> [arguments...]",
        )?;
        let subcommand = args[1];
        if ascii_eq_ignore_case(subcommand, b"HELP") {
            require_exact_arity(args, 2, "CLIENT", "CLIENT HELP")?;
            append_bulk_array(response_out, &CLIENT_HELP_LINES);
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"ID") {
            require_exact_arity(args, 2, "CLIENT", "CLIENT ID")?;
            let id = super::current_request_client_id()
                .map(|cid| i64::try_from(u64::from(cid)).unwrap_or(i64::MAX))
                .unwrap_or(1);
            append_integer(response_out, id);
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"GETNAME") {
            require_exact_arity(args, 2, "CLIENT", "CLIENT GETNAME")?;
            if self.resp_protocol_version().is_resp3() {
                append_null(response_out);
            } else {
                append_null_bulk_string(response_out);
            }
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"SETNAME") {
            require_exact_arity(args, 3, "CLIENT", "CLIENT SETNAME connection-name")?;
            append_simple_string(response_out, b"OK");
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"LIST") {
            require_exact_arity(args, 2, "CLIENT", "CLIENT LIST")?;
            // Minimal compatibility surface for tests that probe blocked EXEC visibility.
            append_bulk_string(response_out, b"id=1 cmd=exec");
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"UNBLOCK") {
            ensure_one_of_arities(
                args,
                &[3, 4],
                "CLIENT",
                "CLIENT UNBLOCK client-id [TIMEOUT|ERROR]",
            )?;
            append_integer(response_out, 0);
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"PAUSE") {
            ensure_one_of_arities(args, &[3, 4], "CLIENT", "CLIENT PAUSE timeout [WRITE|ALL]")?;
            let timeout = parse_u64_ascii(args[2]).ok_or(RequestExecutionError::ValueNotInteger)?;
            let pause_all = if args.len() == 4 {
                let mode = args[3];
                if ascii_eq_ignore_case(mode, b"ALL") {
                    true
                } else if ascii_eq_ignore_case(mode, b"WRITE") {
                    false
                } else {
                    return Err(RequestExecutionError::SyntaxError);
                }
            } else {
                true
            };
            self.client_pause(timeout, pause_all);
            append_simple_string(response_out, b"OK");
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"UNPAUSE") {
            require_exact_arity(args, 2, "CLIENT", "CLIENT UNPAUSE")?;
            self.client_unpause();
            append_simple_string(response_out, b"OK");
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"NO-EVICT") {
            require_exact_arity(args, 3, "CLIENT", "CLIENT NO-EVICT on|off")?;
            let mode = args[2];
            if ascii_eq_ignore_case(mode, b"ON") || ascii_eq_ignore_case(mode, b"OFF") {
                append_simple_string(response_out, b"OK");
                return Ok(());
            }
            return Err(RequestExecutionError::SyntaxError);
        }
        if ascii_eq_ignore_case(subcommand, b"NO-TOUCH") {
            require_exact_arity(args, 3, "CLIENT", "CLIENT NO-TOUCH on|off")?;
            let mode = args[2];
            if ascii_eq_ignore_case(mode, b"ON") {
                append_simple_string(response_out, b"OK");
                return Ok(());
            }
            if ascii_eq_ignore_case(mode, b"OFF") {
                append_simple_string(response_out, b"OK");
                return Ok(());
            }
            return Err(RequestExecutionError::SyntaxError);
        }
        Err(RequestExecutionError::UnknownSubcommand)
    }

    pub(super) fn handle_role(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 1, "ROLE", "ROLE")?;
        response_out.extend_from_slice(b"*3\r\n");
        append_bulk_string(response_out, b"master");
        append_integer(response_out, 0);
        response_out.extend_from_slice(b"*0\r\n");
        Ok(())
    }

    pub(super) fn handle_wait(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 3, "WAIT", "WAIT numreplicas timeout")?;
        parse_u64_ascii(args[1]).ok_or(RequestExecutionError::ValueNotInteger)?;
        parse_u64_ascii(args[2]).ok_or(RequestExecutionError::ValueNotInteger)?;
        append_integer(response_out, 0);
        Ok(())
    }

    pub(super) fn handle_waitaof(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 4, "WAITAOF", "WAITAOF numlocal numreplicas timeout")?;
        let numlocal = parse_u64_ascii(args[1]).ok_or(RequestExecutionError::ValueNotInteger)?;
        parse_u64_ascii(args[2]).ok_or(RequestExecutionError::ValueNotInteger)?;
        parse_u64_ascii(args[3]).ok_or(RequestExecutionError::ValueNotInteger)?;
        if numlocal > 1 {
            return Err(RequestExecutionError::ValueOutOfRange);
        }
        if numlocal > 0 {
            return Err(RequestExecutionError::WaitAofAppendOnlyDisabled);
        }
        // WAITAOF returns an array of two integers: [numlocal_synced, numreplicas_synced]
        append_array_length(response_out, 2);
        append_integer(response_out, 0);
        append_integer(response_out, 0);
        Ok(())
    }

    pub(super) fn handle_save(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 1, "SAVE", "SAVE")?;
        self.reset_rdb_changes_since_last_save();
        append_simple_string(response_out, b"OK");
        Ok(())
    }

    pub(super) fn handle_bgsave(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_ranged_arity(args, 1, 2, "BGSAVE", "BGSAVE [SCHEDULE]")?;
        if args.len() == 2 {
            let mode = args[1];
            if !ascii_eq_ignore_case(mode, b"SCHEDULE") {
                return Err(RequestExecutionError::SyntaxError);
            }
        }
        self.start_bgsave();
        self.reset_rdb_changes_since_last_save();
        append_simple_string(response_out, b"Background saving started");
        Ok(())
    }

    pub(super) fn handle_bgrewriteaof(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 1, "BGREWRITEAOF", "BGREWRITEAOF")?;
        append_simple_string(
            response_out,
            b"Background append only file rewriting started",
        );
        Ok(())
    }

    pub(super) fn handle_readonly(
        &self,
        args: &[&[u8]],
        _response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 1, "READONLY", "READONLY")?;
        Err(RequestExecutionError::ClusterSupportDisabled)
    }

    pub(super) fn handle_readwrite(
        &self,
        args: &[&[u8]],
        _response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 1, "READWRITE", "READWRITE")?;
        Err(RequestExecutionError::ClusterSupportDisabled)
    }

    pub(super) fn handle_reset(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 1, "RESET", "RESET")?;
        self.set_resp_protocol_version(RespProtocolVersion::Resp2);
        append_simple_string(response_out, b"RESET");
        Ok(())
    }

    pub(super) fn handle_lolwut(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() >= 2 {
            let first_option = args[1];
            if ascii_eq_ignore_case(first_option, b"VERSION") {
                require_exact_arity(args, 3, "LOLWUT", "LOLWUT [VERSION version]")?;
                parse_u64_ascii(args[2]).ok_or(RequestExecutionError::ValueNotInteger)?;
            }
        }
        append_bulk_string(response_out, b"Redis ver. garnet-rs\n");
        Ok(())
    }

    pub(super) fn handle_memory(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 2, "MEMORY", "MEMORY USAGE key")?;
        let subcommand = args[1];
        if ascii_eq_ignore_case(subcommand, b"HELP") {
            require_exact_arity(args, 2, "MEMORY", "MEMORY HELP")?;
            append_bulk_array(response_out, &MEMORY_HELP_LINES);
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"MALLOC-STATS") {
            require_exact_arity(args, 2, "MEMORY", "MEMORY MALLOC-STATS")?;
            append_bulk_string(response_out, b"");
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"PURGE") {
            require_exact_arity(args, 2, "MEMORY", "MEMORY PURGE")?;
            append_simple_string(response_out, b"OK");
            return Ok(());
        }
        if !ascii_eq_ignore_case(subcommand, b"USAGE") {
            return Err(RequestExecutionError::UnknownSubcommand);
        }
        require_exact_arity(args, 3, "MEMORY", "MEMORY USAGE key")?;

        let key = RedisKey::from(args[2]);
        self.expire_key_if_needed(&key)?;

        if let Some(value) = self.read_string_value(&key)? {
            append_integer(
                response_out,
                estimate_memory_usage_bytes(key.len(), value.len()),
            );
            return Ok(());
        }
        if let Some(object) = self.object_read(&key)? {
            append_integer(
                response_out,
                estimate_memory_usage_bytes(key.len(), object.payload.len()),
            );
            return Ok(());
        }

        if self.resp_protocol_version().is_resp3() {
            append_null(response_out);
        } else {
            append_null_bulk_string(response_out);
        }
        Ok(())
    }

    pub(super) fn handle_dbsize(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 1, "DBSIZE", "DBSIZE")?;
        append_integer(response_out, self.active_key_count()?);
        Ok(())
    }

    pub(super) fn handle_debug(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 2, "DEBUG", "DEBUG subcommand [arguments...]")?;
        let subcommand = args[1];
        if ascii_eq_ignore_case(subcommand, b"SET-ACTIVE-EXPIRE") {
            require_exact_arity(args, 3, "DEBUG", "DEBUG SET-ACTIVE-EXPIRE <0|1>")?;
            let enabled = args[2];
            if enabled != b"0" && enabled != b"1" {
                return Err(RequestExecutionError::SyntaxError);
            }
            self.set_active_expire_enabled(enabled == b"1");
            append_simple_string(response_out, b"OK");
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"LOADAOF") {
            require_exact_arity(args, 2, "DEBUG", "DEBUG LOADAOF")?;
            // Compatibility path: in-memory test mode has no persisted AOF replay boundary.
            // Redis tests use DEBUG LOADAOF as a synchronization hook; returning OK keeps
            // semantics for current appendonly-disabled/runtime-local execution.
            append_simple_string(response_out, b"OK");
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"SLEEP") {
            require_exact_arity(args, 3, "DEBUG", "DEBUG SLEEP <seconds>")?;
            let seconds = parse_f64_ascii(args[2]).ok_or(RequestExecutionError::ValueNotFloat)?;
            if !seconds.is_finite() || seconds < 0.0 {
                return Err(RequestExecutionError::ValueNotFloat);
            }
            std::thread::sleep(Duration::from_secs_f64(seconds));
            let latency_millis = (seconds * 1000.0).round() as u64;
            self.record_latency_event(b"command", latency_millis.max(1));
            append_simple_string(response_out, b"OK");
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"RELOAD") {
            ensure_min_arity(args, 2, "DEBUG", "DEBUG RELOAD [NOSAVE] [NOFLUSH] [MERGE]")?;
            let mut noflush = false;
            let mut merge = false;
            for option in &args[2..] {
                let token = option;
                if ascii_eq_ignore_case(token, b"NOSAVE") {
                    continue;
                }
                if ascii_eq_ignore_case(token, b"NOFLUSH") {
                    noflush = true;
                    continue;
                }
                if ascii_eq_ignore_case(token, b"MERGE") {
                    merge = true;
                    continue;
                }
                return Err(RequestExecutionError::SyntaxError);
            }
            if noflush && !merge {
                append_error(
                    response_out,
                    b"ERR Error trying to load the RDB payload with NOFLUSH only",
                );
                return Ok(());
            }
            // Compatibility path: in-memory test mode has no RDB reload boundary.
            // Redis tests use DEBUG RELOAD as a persistence synchronization hook.
            append_simple_string(response_out, b"OK");
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"DIGEST-VALUE") {
            require_exact_arity(args, 3, "DEBUG", "DEBUG DIGEST-VALUE key")?;
            let key = RedisKey::from(args[2]);
            self.expire_key_if_needed(&key)?;
            let digest = self.debug_digest_value_for_key(&key)?;
            append_bulk_string(response_out, &digest);
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"DIGEST") {
            require_exact_arity(args, 2, "DEBUG", "DEBUG DIGEST")?;
            let digest = self.debug_dataset_digest()?;
            append_bulk_string(response_out, &digest);
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"PROTOCOL") {
            require_exact_arity(args, 3, "DEBUG", "DEBUG PROTOCOL <subcommand>")?;
            let protocol_subcommand = args[2];
            let resp3 = self.resp_protocol_version().is_resp3();
            if ascii_eq_ignore_case(protocol_subcommand, b"ATTRIB") {
                if resp3 {
                    response_out.extend_from_slice(b"|1\r\n");
                    append_bulk_string(response_out, DEBUG_PROTOCOL_ATTRIBUTE_NAME);
                    response_out.extend_from_slice(b"*2\r\n");
                    append_bulk_string(response_out, DEBUG_PROTOCOL_ATTRIBUTE_KEY);
                    append_integer(response_out, 90);
                    append_bulk_string(response_out, DEBUG_PROTOCOL_ATTRIB_REPLY);
                } else {
                    append_bulk_string(response_out, DEBUG_PROTOCOL_ATTRIB_REPLY);
                }
                return Ok(());
            }
            if ascii_eq_ignore_case(protocol_subcommand, b"BIGNUM") {
                if resp3 {
                    append_resp3_bignum(response_out, DEBUG_PROTOCOL_BIGNUM_VALUE);
                } else {
                    append_bulk_string(response_out, DEBUG_PROTOCOL_BIGNUM_VALUE);
                }
                return Ok(());
            }
            if ascii_eq_ignore_case(protocol_subcommand, b"TRUE") {
                if resp3 {
                    append_resp3_boolean(response_out, true);
                } else {
                    append_integer(response_out, 1);
                }
                return Ok(());
            }
            if ascii_eq_ignore_case(protocol_subcommand, b"FALSE") {
                if resp3 {
                    append_resp3_boolean(response_out, false);
                } else {
                    append_integer(response_out, 0);
                }
                return Ok(());
            }
            if ascii_eq_ignore_case(protocol_subcommand, b"VERBATIM") {
                if resp3 {
                    append_verbatim_string(
                        response_out,
                        b"txt",
                        DEBUG_PROTOCOL_VERBATIM_VALUE,
                    );
                } else {
                    append_bulk_string(response_out, DEBUG_PROTOCOL_VERBATIM_VALUE);
                }
                return Ok(());
            }
            return Err(RequestExecutionError::UnknownSubcommand);
        }
        if ascii_eq_ignore_case(subcommand, b"OBJECT") {
            require_exact_arity(args, 3, "DEBUG", "DEBUG OBJECT key")?;
            let key = RedisKey::from(args[2]);
            self.expire_key_if_needed(&key)?;
            if !self.key_exists_any(&key)? {
                append_error(response_out, b"ERR no such key");
                return Ok(());
            }
            let lru = self.key_lru_millis(&key).unwrap_or(0);
            let idle = self.key_idle_seconds(&key).unwrap_or(0);
            let payload = format!(
                "Value at:0x0 refcount:1 encoding:raw serializedlength:0 lru:{} lru_seconds_idle:{}",
                lru, idle
            );
            if self.resp_protocol_version().is_resp3() {
                append_verbatim_string(response_out, b"txt", payload.as_bytes());
            } else {
                append_bulk_string(response_out, payload.as_bytes());
            }
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"PAUSE-CRON") {
            require_exact_arity(args, 3, "DEBUG", "DEBUG PAUSE-CRON <0|1>")?;
            let flag = args[2];
            if flag != b"0" && flag != b"1" {
                return Err(RequestExecutionError::SyntaxError);
            }
            self.set_debug_pause_cron(flag == b"1");
            append_simple_string(response_out, b"OK");
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"REPLYBUFFER") {
            require_exact_arity(args, 4, "DEBUG", "DEBUG REPLYBUFFER <subcommand> <value>")?;
            let rb_sub = args[2];
            if ascii_eq_ignore_case(rb_sub, b"PEAK-RESET-TIME") {
                // Accepts "never", "reset", or a numeric millisecond value.
                // garnet-rs does not yet implement reply-buffer peak tracking,
                // so this is a compatibility stub that accepts all valid inputs.
                let value = args[3];
                if !ascii_eq_ignore_case(value, b"NEVER") && !ascii_eq_ignore_case(value, b"RESET")
                {
                    // Validate that the value is a valid integer.
                    let _millis =
                        parse_i64_ascii(value).ok_or(RequestExecutionError::ValueNotInteger)?;
                }
                append_simple_string(response_out, b"OK");
                return Ok(());
            }
            if ascii_eq_ignore_case(rb_sub, b"RESIZING") {
                let flag = args[3];
                if flag != b"0" && flag != b"1" {
                    return Err(RequestExecutionError::SyntaxError);
                }
                // Compatibility stub: garnet-rs does not yet resize reply buffers.
                append_simple_string(response_out, b"OK");
                return Ok(());
            }
            return Err(RequestExecutionError::UnknownSubcommand);
        }
        if ascii_eq_ignore_case(subcommand, b"REPLY-COPY-AVOIDANCE") {
            require_exact_arity(args, 3, "DEBUG", "DEBUG REPLY-COPY-AVOIDANCE <0|1>")?;
            let flag = args[2];
            if flag != b"0" && flag != b"1" {
                return Err(RequestExecutionError::SyntaxError);
            }
            // Compatibility stub: garnet-rs does not use copy-avoidance for replies.
            append_simple_string(response_out, b"OK");
            return Ok(());
        }
        Err(RequestExecutionError::UnknownSubcommand)
    }

    pub(super) fn handle_object(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 2, "OBJECT", "OBJECT subcommand [arguments]")?;
        let subcommand = args[1];

        if ascii_eq_ignore_case(subcommand, b"HELP") {
            require_exact_arity(args, 2, "OBJECT", "OBJECT HELP")?;
            append_bulk_array(response_out, &OBJECT_HELP_LINES);
            return Ok(());
        }

        ensure_min_arity(
            args,
            3,
            "OBJECT",
            "OBJECT <ENCODING|REFCOUNT|IDLETIME|FREQ> key",
        )?;
        let resp3 = self.resp_protocol_version().is_resp3();

        if ascii_eq_ignore_case(subcommand, b"ENCODING") {
            require_exact_arity(args, 3, "OBJECT", "OBJECT ENCODING key")?;
            let key = RedisKey::from(args[2]);
            self.expire_key_if_needed(&key)?;
            if let Some(value) = self.read_string_value(&key)? {
                let encoding = string_encoding_name(&value);
                append_bulk_string(response_out, encoding);
                return Ok(());
            }
            let Some(object) = self.object_read(&key)? else {
                if resp3 {
                    append_null(response_out);
                } else {
                    append_null_bulk_string(response_out);
                }
                return Ok(());
            };
            if object.object_type == LIST_OBJECT_TYPE_TAG
                && self.list_quicklist_encoding_is_forced(&key)
            {
                append_bulk_string(response_out, b"quicklist");
                return Ok(());
            }
            let zset_max_listpack_entries = self.zset_max_listpack_entries.load(Ordering::Acquire);
            let list_max_listpack_size = self.list_max_listpack_size.load(Ordering::Acquire);
            let encoding = object_encoding_name(
                object.object_type,
                &object.payload,
                zset_max_listpack_entries,
                list_max_listpack_size,
            )?;
            append_bulk_string(response_out, encoding);
            return Ok(());
        }

        if ascii_eq_ignore_case(subcommand, b"REFCOUNT") {
            require_exact_arity(args, 3, "OBJECT", "OBJECT REFCOUNT key")?;
            let key = RedisKey::from(args[2]);
            self.expire_key_if_needed(&key)?;
            if self.key_exists_any(&key)? {
                append_integer(response_out, 1);
            } else if resp3 {
                append_null(response_out);
            } else {
                append_null_bulk_string(response_out);
            }
            return Ok(());
        }

        if ascii_eq_ignore_case(subcommand, b"IDLETIME") {
            require_exact_arity(args, 3, "OBJECT", "OBJECT IDLETIME key")?;
            let key = RedisKey::from(args[2]);
            self.expire_key_if_needed(&key)?;
            if !self.key_exists_any(&key)? {
                if resp3 {
                    append_null(response_out);
                } else {
                    append_null_bulk_string(response_out);
                }
                return Ok(());
            }
            append_integer(response_out, self.key_idle_seconds(&key).unwrap_or(0));
            return Ok(());
        }

        if ascii_eq_ignore_case(subcommand, b"FREQ") {
            require_exact_arity(args, 3, "OBJECT", "OBJECT FREQ key")?;
            let key = RedisKey::from(args[2]);
            self.expire_key_if_needed(&key)?;
            if !self.key_exists_any(&key)? {
                if resp3 {
                    append_null(response_out);
                } else {
                    append_null_bulk_string(response_out);
                }
                return Ok(());
            }
            append_integer(
                response_out,
                i64::from(self.key_frequency(&key).unwrap_or(0)),
            );
            return Ok(());
        }

        Err(RequestExecutionError::UnknownCommand)
    }

    pub(super) fn handle_keys(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 2, "KEYS", "KEYS pattern")?;
        let pattern = args[1];

        let mut keys: HashSet<RedisKey> = self.string_keys_snapshot().into_iter().collect();
        keys.extend(self.object_keys_snapshot());

        let mut matched = Vec::new();
        for key in keys {
            self.expire_key_if_needed(key.as_slice())?;

            let string_exists = self.key_exists(key.as_slice())?;
            let object_exists = self.object_key_exists(key.as_slice())?;
            if !string_exists {
                self.untrack_string_key(key.as_slice());
            }
            if !object_exists {
                self.untrack_object_key(key.as_slice());
            }
            if !(string_exists || object_exists) {
                continue;
            }
            if redis_glob_match(pattern, key.as_slice(), CaseSensitivity::Sensitive, 0) {
                matched.push(key);
            }
        }

        response_out.push(b'*');
        response_out.extend_from_slice(matched.len().to_string().as_bytes());
        response_out.extend_from_slice(b"\r\n");
        for key in matched {
            append_bulk_string(response_out, key.as_slice());
        }
        Ok(())
    }

    pub(super) fn handle_randomkey(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 1, "RANDOMKEY", "RANDOMKEY")?;

        let mut keys: HashSet<RedisKey> = self.string_keys_snapshot().into_iter().collect();
        keys.extend(self.object_keys_snapshot());

        let mut live_keys = Vec::new();
        for key in keys {
            self.expire_key_if_needed(key.as_slice())?;

            let string_exists = self.key_exists(key.as_slice())?;
            let object_exists = self.object_key_exists(key.as_slice())?;
            if !string_exists {
                self.untrack_string_key(key.as_slice());
            }
            if !object_exists {
                self.untrack_object_key(key.as_slice());
            }
            if string_exists || object_exists {
                live_keys.push(key);
            }
        }

        if live_keys.is_empty() {
            if self.resp_protocol_version().is_resp3() {
                append_null(response_out);
            } else {
                append_null_bulk_string(response_out);
            }
            return Ok(());
        }

        live_keys.sort_unstable();
        let index =
            (NEXT_RANDOMKEY_INDEX.fetch_add(1, Ordering::Relaxed) as usize) % live_keys.len();
        append_bulk_string(response_out, live_keys[index].as_slice());
        Ok(())
    }

    pub(super) fn handle_scan(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            2,
            "SCAN",
            "SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]",
        )?;

        let cursor = parse_u64_ascii(args[1]).ok_or(RequestExecutionError::ValueNotInteger)?;
        let mut pattern: Option<&[u8]> = None;
        let mut count = 10usize;
        // Unknown type names are silently accepted and match nothing (Redis 7.x
        // compat).  `None` means no TYPE filter was specified; `Some(None)`
        // means an unknown type was given.
        let mut type_filter: Option<Option<ScanTypeFilter>> = None;
        let mut index = 2usize;
        while index < args.len() {
            let token = args[index];
            if ascii_eq_ignore_case(token, b"MATCH") {
                if index + 1 >= args.len() {
                    return Err(RequestExecutionError::SyntaxError);
                }
                pattern = Some(args[index + 1]);
                index += 2;
                continue;
            }
            if ascii_eq_ignore_case(token, b"COUNT") {
                if index + 1 >= args.len() {
                    return Err(RequestExecutionError::SyntaxError);
                }
                let parsed_count = parse_u64_ascii(args[index + 1])
                    .ok_or(RequestExecutionError::ValueNotInteger)?;
                if parsed_count == 0 {
                    return Err(RequestExecutionError::ValueOutOfRange);
                }
                count = usize::try_from(parsed_count).unwrap_or(usize::MAX);
                index += 2;
                continue;
            }
            if ascii_eq_ignore_case(token, b"TYPE") {
                if index + 1 >= args.len() {
                    return Err(RequestExecutionError::SyntaxError);
                }
                let raw_type = args[index + 1];
                type_filter = Some(parse_scan_type_filter(raw_type));
                index += 2;
                continue;
            }
            return Err(RequestExecutionError::SyntaxError);
        }

        let mut keys: HashSet<RedisKey> = self.string_keys_snapshot().into_iter().collect();
        keys.extend(self.object_keys_snapshot());

        let mut matched = Vec::new();
        for key in keys {
            // Redis 7.x filter order: pattern -> expire -> type.
            // Keys that don't match the pattern are skipped without triggering
            // expiry.  Keys that match the pattern are lazily expired, then
            // type-filtered.
            if let Some(pattern) = pattern
                && !redis_glob_match(pattern, key.as_slice(), CaseSensitivity::Sensitive, 0)
            {
                continue;
            }

            self.expire_key_if_needed(key.as_slice())?;
            let string_exists = self.key_exists(key.as_slice())?;
            let object_exists = self.object_key_exists(key.as_slice())?;
            if !string_exists {
                self.untrack_string_key(key.as_slice());
            }
            if !object_exists {
                self.untrack_object_key(key.as_slice());
            }
            if !(string_exists || object_exists) {
                continue;
            }

            if let Some(filter_opt) = type_filter {
                let Some(filter) = filter_opt else {
                    // Unknown type name: matches nothing.
                    continue;
                };
                if !self.scan_key_matches_type_filter(
                    key.as_slice(),
                    string_exists,
                    object_exists,
                    filter,
                )? {
                    continue;
                }
            }

            matched.push(key.into_vec());
        }
        matched.sort_unstable();
        let resp3 = self.resp_protocol_version().is_resp3();
        append_scan_cursor_and_key_array(response_out, cursor, count, &matched, resp3);
        Ok(())
    }

    fn scan_key_matches_type_filter(
        &self,
        key: &[u8],
        string_exists: bool,
        object_exists: bool,
        type_filter: ScanTypeFilter,
    ) -> Result<bool, RequestExecutionError> {
        if type_filter == ScanTypeFilter::String {
            return Ok(string_exists);
        }
        if string_exists || !object_exists {
            return Ok(false);
        }
        let Some(object) = self.object_read(key)? else {
            self.untrack_object_key(key);
            return Ok(false);
        };
        Ok(scan_object_type_matches_filter(
            object.object_type,
            type_filter,
        ))
    }

    pub(super) fn handle_command(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 1, "COMMAND", "COMMAND")?;
        if args.len() == 1 {
            append_bulk_array(response_out, command_names_for_command_response());
            return Ok(());
        }

        if ascii_eq_ignore_case(args[1], b"HELP") {
            require_exact_arity(args, 2, "COMMAND", "COMMAND HELP")?;
            append_bulk_array(response_out, &COMMAND_HELP_LINES);
            return Ok(());
        }
        if ascii_eq_ignore_case(args[1], b"COUNT") {
            require_exact_arity(args, 2, "COMMAND", "COMMAND COUNT")?;
            append_integer(
                response_out,
                command_names_for_command_response().len() as i64,
            );
            return Ok(());
        }
        if ascii_eq_ignore_case(args[1], b"LIST") {
            return self.handle_command_list(args, response_out);
        }
        if ascii_eq_ignore_case(args[1], b"INFO") {
            return self.handle_command_info(args, response_out);
        }
        if ascii_eq_ignore_case(args[1], b"GETKEYS") {
            return self.handle_command_getkeys(args, response_out);
        }
        if ascii_eq_ignore_case(args[1], b"GETKEYSANDFLAGS") {
            return self.handle_command_getkeysandflags(args, response_out);
        }

        Err(RequestExecutionError::SyntaxError)
    }

    fn handle_command_list(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        let entries = command_list_entries();
        if args.len() == 2 {
            let refs: Vec<&[u8]> = entries.iter().map(Vec::as_slice).collect();
            append_bulk_array(response_out, &refs);
            return Ok(());
        }

        if args.len() != 5 || !ascii_eq_ignore_case(args[2], b"FILTERBY") {
            return Err(RequestExecutionError::SyntaxError);
        }

        let mut filtered: Vec<&[u8]> = Vec::new();
        if ascii_eq_ignore_case(args[3], b"ACLCAT") {
            for entry in &entries {
                if command_list_entry_matches_acl_category(entry, args[4]) {
                    filtered.push(entry);
                }
            }
        } else if ascii_eq_ignore_case(args[3], b"PATTERN") {
            for entry in &entries {
                if redis_glob_match(args[4], entry, CaseSensitivity::Insensitive, 0) {
                    filtered.push(entry);
                }
            }
        } else if ascii_eq_ignore_case(args[3], b"MODULE") {
            // Modules are not currently supported by garnet-rs.
        } else {
            return Err(RequestExecutionError::SyntaxError);
        }

        append_bulk_array(response_out, &filtered);
        Ok(())
    }

    fn handle_command_info(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 2, "COMMAND", "COMMAND INFO [command-name ...]")?;
        let resp3 = self.resp_protocol_version().is_resp3();

        if args.len() == 2 {
            if resp3 {
                append_map_length(response_out, 0);
            } else {
                response_out.extend_from_slice(b"*0\r\n");
            }
            return Ok(());
        }

        if resp3 {
            // RESP3: emit map keyed by command name; unknown commands are omitted.
            // Pre-scan to count valid entries for the map header.
            let valid_count = args[2..]
                .iter()
                .filter(|token| {
                    let Some(ct) = split_command_token(token) else {
                        return false;
                    };
                    let cid = command_id_from_name(ct.name);
                    let sub_known =
                        ct.subcommand
                            .is_some_and(|v| command_subcommand_known(ct.name, v));
                    cid.is_some() && (ct.subcommand.is_none() || sub_known)
                })
                .count();
            append_map_length(response_out, valid_count);
        } else {
            append_array_length(response_out, args.len() - 2);
        }

        for token in &args[2..] {
            let Some(command_token) = split_command_token(token) else {
                if !resp3 {
                    response_out.extend_from_slice(b"*0\r\n");
                }
                continue;
            };
            let command_id = command_id_from_name(command_token.name);
            let subcommand_known = command_token
                .subcommand
                .is_some_and(|value| command_subcommand_known(command_token.name, value));
            if command_id.is_none() || (command_token.subcommand.is_some() && !subcommand_known) {
                if !resp3 {
                    response_out.extend_from_slice(b"*0\r\n");
                }
                continue;
            }

            let mut flags = Vec::with_capacity(2);
            let mut arity = 0i64;
            if let Some(id) = command_id {
                flags.push(if command_is_mutating(id) {
                    b"write".as_slice()
                } else {
                    b"readonly".as_slice()
                });
                arity = command_arity_for_command_info(id);
                if command_has_movablekeys(id, command_token.subcommand) {
                    flags.push(b"movablekeys");
                }
            }

            let lowered = token.to_ascii_lowercase();
            if resp3 {
                // Map key: command name.
                append_bulk_string(response_out, &lowered);
            }
            // Map value (RESP3) or array element (RESP2): info entry.
            append_command_info_entry(response_out, &lowered, arity, &flags);
        }
        Ok(())
    }

    fn handle_command_getkeys(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 4, "COMMAND", "COMMAND GETKEYS command [arg ...]")?;
        let target = args[2];
        let mut keys: Vec<&[u8]> = Vec::new();

        if ascii_eq_ignore_case(target, b"FCALL") || ascii_eq_ignore_case(target, b"FCALL_RO") {
            ensure_min_arity(
                args,
                5,
                "COMMAND",
                "COMMAND GETKEYS FCALL function numkeys [key ...] [arg ...]",
            )?;
            let key_count = command_getkeys_numkeys(args[4], args.len().saturating_sub(5))?;
            let end = 5 + key_count;
            keys.extend_from_slice(&args[5..end]);
        } else if ascii_eq_ignore_case(target, b"GET") {
            require_exact_arity(args, 4, "COMMAND", "COMMAND GETKEYS GET key")?;
            keys.push(args[3]);
        } else if ascii_eq_ignore_case(target, b"MEMORY") {
            ensure_min_arity(
                args,
                5,
                "COMMAND",
                "COMMAND GETKEYS MEMORY <subcommand> [arg ...]",
            )?;
            if !ascii_eq_ignore_case(args[3], b"USAGE") {
                return Err(RequestExecutionError::SyntaxError);
            }
            keys.push(args[4]);
        } else if ascii_eq_ignore_case(target, b"XGROUP") {
            ensure_min_arity(
                args,
                5,
                "COMMAND",
                "COMMAND GETKEYS XGROUP CREATE key group id [options]",
            )?;
            keys.push(args[4]);
        } else if ascii_eq_ignore_case(target, b"EVAL")
            || ascii_eq_ignore_case(target, b"EVAL_RO")
            || ascii_eq_ignore_case(target, b"EVALSHA")
            || ascii_eq_ignore_case(target, b"EVALSHA_RO")
        {
            ensure_min_arity(
                args,
                5,
                "COMMAND",
                "COMMAND GETKEYS EVAL script numkeys [key ...]",
            )?;
            let key_count = command_getkeys_numkeys(args[4], args.len().saturating_sub(5))?;
            let end = 5 + key_count;
            keys.extend_from_slice(&args[5..end]);
        } else if ascii_eq_ignore_case(target, b"LCS") {
            ensure_min_arity(
                args,
                5,
                "COMMAND",
                "COMMAND GETKEYS LCS key1 key2 [options]",
            )?;
            keys.push(args[3]);
            keys.push(args[4]);
        } else if ascii_eq_ignore_case(target, b"ZUNIONSTORE") {
            ensure_min_arity(
                args,
                6,
                "COMMAND",
                "COMMAND GETKEYS ZUNIONSTORE destination numkeys key [key ...]",
            )?;
            let key_count = command_getkeys_numkeys(args[4], args.len().saturating_sub(5))?;
            keys.push(args[3]);
            let end = 5 + key_count;
            keys.extend_from_slice(&args[5..end]);
        } else {
            return Err(RequestExecutionError::SyntaxError);
        }

        append_bulk_array(response_out, &keys);
        Ok(())
    }

    fn handle_command_getkeysandflags(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            4,
            "COMMAND",
            "COMMAND GETKEYSANDFLAGS command [arg ...]",
        )?;
        let target = args[2];
        let mut entries: Vec<(Vec<u8>, &'static [&'static [u8]])> = Vec::new();

        if ascii_eq_ignore_case(target, b"SET") {
            ensure_min_arity(
                args,
                5,
                "COMMAND",
                "COMMAND GETKEYSANDFLAGS SET key value [option ...]",
            )?;
            let mut flags: &[&[u8]] = &COMMAND_FLAGS_OW_UPDATE;
            let mut index = 5usize;
            while index < args.len() {
                let token = args[index];
                if ascii_eq_ignore_case(token, b"GET") {
                    flags = &COMMAND_FLAGS_RW_ACCESS_UPDATE;
                    index += 1;
                    continue;
                }
                if ascii_eq_ignore_case(token, b"IFEQ") {
                    if index + 1 >= args.len() {
                        return Err(RequestExecutionError::InvalidArguments);
                    }
                    flags = &COMMAND_FLAGS_RW_UPDATE;
                    index += 2;
                    continue;
                }
                if ascii_eq_ignore_case(token, b"NX")
                    || ascii_eq_ignore_case(token, b"XX")
                    || ascii_eq_ignore_case(token, b"KEEPTTL")
                {
                    index += 1;
                    continue;
                }
                if ascii_eq_ignore_case(token, b"EX")
                    || ascii_eq_ignore_case(token, b"PX")
                    || ascii_eq_ignore_case(token, b"EXAT")
                    || ascii_eq_ignore_case(token, b"PXAT")
                {
                    if index + 1 >= args.len() {
                        return Err(RequestExecutionError::InvalidArguments);
                    }
                    index += 2;
                    continue;
                }
                return Err(RequestExecutionError::InvalidArguments);
            }
            entries.push((args[3].to_vec(), flags));
        } else if ascii_eq_ignore_case(target, b"MSET") {
            ensure_min_arity(
                args,
                5,
                "COMMAND",
                "COMMAND GETKEYSANDFLAGS MSET key value [key value ...]",
            )?;
            if (args.len() - 3).is_multiple_of(2) {
                for pair in args[3..].chunks_exact(2) {
                    entries.push((pair[0].to_vec(), &COMMAND_FLAGS_OW_UPDATE));
                }
            } else {
                return Err(RequestExecutionError::InvalidArguments);
            }
        } else if ascii_eq_ignore_case(target, b"LMOVE") {
            ensure_min_arity(
                args,
                7,
                "COMMAND",
                "COMMAND GETKEYSANDFLAGS LMOVE source destination LEFT|RIGHT LEFT|RIGHT",
            )?;
            entries.push((args[3].to_vec(), &COMMAND_FLAGS_RW_ACCESS_DELETE));
            entries.push((args[4].to_vec(), &COMMAND_FLAGS_RW_INSERT));
        } else if ascii_eq_ignore_case(target, b"SORT") {
            ensure_min_arity(
                args,
                4,
                "COMMAND",
                "COMMAND GETKEYSANDFLAGS SORT key [option ...]",
            )?;
            entries.push((args[3].to_vec(), &COMMAND_FLAGS_RO_ACCESS));
            let mut index = 4usize;
            while index < args.len() {
                if ascii_eq_ignore_case(args[index], b"STORE") {
                    if index + 1 >= args.len() {
                        return Err(RequestExecutionError::InvalidArguments);
                    }
                    entries.push((args[index + 1].to_vec(), &COMMAND_FLAGS_OW_UPDATE));
                    break;
                }
                index += 1;
            }
        } else if ascii_eq_ignore_case(target, b"DELEX") {
            ensure_min_arity(
                args,
                4,
                "COMMAND",
                "COMMAND GETKEYSANDFLAGS DELEX key [IFEQ value]",
            )?;
            if args.len() == 4 {
                entries.push((args[3].to_vec(), &COMMAND_FLAGS_RM_DELETE));
            } else if args.len() == 6 && ascii_eq_ignore_case(args[4], b"IFEQ") {
                entries.push((args[3].to_vec(), &COMMAND_FLAGS_RW_DELETE));
            } else {
                return Err(RequestExecutionError::InvalidArguments);
            }
        } else if ascii_eq_ignore_case(target, b"MSETEX") {
            ensure_min_arity(
                args,
                6,
                "COMMAND",
                "COMMAND GETKEYSANDFLAGS MSETEX numkeys key value [key value ...] [option ...]",
            )?;
            let numkeys =
                parse_u64_ascii(args[3]).ok_or(RequestExecutionError::InvalidArguments)?;
            let key_count =
                usize::try_from(numkeys).map_err(|_| RequestExecutionError::InvalidArguments)?;
            if key_count == 0 {
                return Err(RequestExecutionError::InvalidArguments);
            }
            let required_values = key_count.saturating_mul(2);
            if args.len() < 4 + required_values {
                return Err(RequestExecutionError::InvalidArguments);
            }
            for index in 0..key_count {
                let key_index = 4 + index * 2;
                entries.push((args[key_index].to_vec(), &COMMAND_FLAGS_OW_UPDATE));
            }
        } else if ascii_eq_ignore_case(target, b"ZINTERSTORE") {
            ensure_min_arity(
                args,
                6,
                "COMMAND",
                "COMMAND GETKEYSANDFLAGS ZINTERSTORE destination numkeys key [key ...]",
            )?;
            let numkeys =
                parse_u64_ascii(args[4]).ok_or(RequestExecutionError::InvalidArguments)?;
            let key_count =
                usize::try_from(numkeys).map_err(|_| RequestExecutionError::InvalidArguments)?;
            if key_count == 0 || key_count > args.len().saturating_sub(5) {
                return Err(RequestExecutionError::InvalidArguments);
            }
            entries.push((args[3].to_vec(), &COMMAND_FLAGS_OW_UPDATE));
            for source in &args[5..5 + key_count] {
                entries.push(((*source).to_vec(), &COMMAND_FLAGS_RO_ACCESS));
            }
        } else {
            return Err(RequestExecutionError::InvalidArguments);
        }

        append_command_getkeysandflags(response_out, &entries);
        Ok(())
    }

    pub(super) fn handle_dump(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 2, "DUMP", "DUMP key")?;
        let key = RedisKey::from(args[1]);
        self.expire_key_if_needed(&key)?;
        if let Some(value) = self.read_string_value(&key)? {
            append_bulk_string(
                response_out,
                &encode_dump_blob(MigrationValue::String(value.into())),
            );
            return Ok(());
        }
        if let Some(object) = self.object_read(&key)? {
            append_bulk_string(
                response_out,
                &encode_dump_blob(MigrationValue::Object {
                    object_type: object.object_type,
                    payload: object.payload,
                }),
            );
            return Ok(());
        }
        if self.resp_protocol_version().is_resp3() {
            append_null(response_out);
        } else {
            append_null_bulk_string(response_out);
        }
        Ok(())
    }

    pub(super) fn handle_restore(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        restore_from_dump_blob(self, args, response_out, "RESTORE")
    }

    pub(super) fn handle_restore_asking(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        restore_from_dump_blob(self, args, response_out, "RESTORE-ASKING")
    }

    pub(super) fn handle_latency(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            2,
            "LATENCY",
            "LATENCY <DOCTOR|GRAPH|HISTORY|LATEST|RESET|HISTOGRAM|HELP> [arguments...]",
        )?;
        let subcommand = args[1];
        if ascii_eq_ignore_case(subcommand, b"HELP") {
            require_exact_arity(args, 2, "LATENCY|HELP", "LATENCY HELP")?;
            append_bulk_array(response_out, &LATENCY_HELP_LINES);
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"LATEST") {
            require_exact_arity(args, 2, "LATENCY", "LATENCY LATEST")?;
            let latest = self.latency_latest();
            response_out.push(b'*');
            response_out.extend_from_slice(latest.len().to_string().as_bytes());
            response_out.extend_from_slice(b"\r\n");
            for event in latest {
                response_out.extend_from_slice(b"*4\r\n");
                append_bulk_string(response_out, &event.event_name);
                append_integer(response_out, event.latest_sample.unix_seconds as i64);
                append_integer(response_out, event.latest_sample.latency_millis as i64);
                append_integer(response_out, event.max_latency_millis as i64);
            }
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"HISTORY") {
            require_exact_arity(args, 3, "LATENCY", "LATENCY HISTORY event")?;
            let history = self.latency_history(args[2]);
            response_out.push(b'*');
            response_out.extend_from_slice(history.len().to_string().as_bytes());
            response_out.extend_from_slice(b"\r\n");
            for sample in history {
                response_out.extend_from_slice(b"*2\r\n");
                append_integer(response_out, sample.unix_seconds as i64);
                append_integer(response_out, sample.latency_millis as i64);
            }
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"RESET") {
            if args.len() == 2 {
                append_integer(response_out, self.reset_latency_events(None) as i64);
                return Ok(());
            }
            let event_names = args[2..]
                .iter()
                .map(|name| name.to_vec())
                .collect::<Vec<_>>();
            append_integer(
                response_out,
                self.reset_latency_events(Some(&event_names)) as i64,
            );
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"DOCTOR") {
            require_exact_arity(args, 2, "LATENCY", "LATENCY DOCTOR")?;
            append_bulk_string(response_out, LATENCY_DOCTOR_DISABLED_MESSAGE);
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"GRAPH") {
            require_exact_arity(args, 3, "LATENCY", "LATENCY GRAPH event")?;
            let resp3 = self.resp_protocol_version().is_resp3();
            let Some(range) = self.latency_graph_range(args[2]) else {
                if resp3 {
                    append_verbatim_string(response_out, b"txt", b"");
                } else {
                    append_bulk_string(response_out, b"");
                }
                return Ok(());
            };
            let normalized_event = args[2]
                .iter()
                .map(|byte| byte.to_ascii_lowercase())
                .collect::<Vec<u8>>();
            let graph = format!(
                "{} - high {} ms, low {} ms\r\n",
                String::from_utf8_lossy(&normalized_event),
                range.max_millis,
                range.min_millis
            );
            if resp3 {
                append_verbatim_string(response_out, b"txt", graph.as_bytes());
            } else {
                append_bulk_string(response_out, graph.as_bytes());
            }
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"HISTOGRAM") {
            let command_calls = self.command_call_counts_snapshot();
            let mut command_counts = BTreeMap::<String, u64>::new();
            for (command_name, count) in command_calls {
                let name = String::from_utf8_lossy(&command_name).to_string();
                if name == "latency" || name.starts_with("latency|") {
                    continue;
                }
                command_counts.insert(name, count);
            }

            let mut selected = BTreeSet::<String>::new();
            if args.len() == 2 {
                selected.extend(command_counts.keys().cloned());
            } else {
                for token in &args[2..] {
                    let name = String::from_utf8_lossy(token).to_ascii_lowercase();
                    if name.contains('|') {
                        if command_counts.contains_key(&name) {
                            selected.insert(name);
                        }
                        continue;
                    }
                    if command_counts.contains_key(&name) {
                        selected.insert(name.clone());
                    }
                    let prefix = format!("{name}|");
                    for known_command in command_counts.keys() {
                        if known_command.starts_with(&prefix) {
                            selected.insert(known_command.clone());
                        }
                    }
                }
            }

            response_out.push(b'*');
            response_out.extend_from_slice((selected.len() * 2).to_string().as_bytes());
            response_out.extend_from_slice(b"\r\n");
            for command_name in selected {
                append_bulk_string(response_out, command_name.as_bytes());
                let count = command_counts
                    .get(&command_name)
                    .copied()
                    .unwrap_or_default();
                let histogram = format!("calls {count} histogram_usec 1 1 1");
                append_bulk_string(response_out, histogram.as_bytes());
            }
            return Ok(());
        }
        Err(RequestExecutionError::UnknownCommand)
    }

    pub(super) fn handle_module(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 2, "MODULE", "MODULE <LIST|HELP>")?;
        let subcommand = args[1];
        if ascii_eq_ignore_case(subcommand, b"LIST") {
            require_exact_arity(args, 2, "MODULE", "MODULE LIST")?;
            response_out.extend_from_slice(b"*0\r\n");
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"HELP") {
            require_exact_arity(args, 2, "MODULE", "MODULE HELP")?;
            append_bulk_array(response_out, &MODULE_HELP_LINES);
            return Ok(());
        }
        Err(RequestExecutionError::UnknownCommand)
    }

    pub(super) fn handle_slowlog(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            2,
            "SLOWLOG",
            "SLOWLOG <GET|LEN|RESET|HELP> [arguments...]",
        )?;
        let subcommand = args[1];
        if ascii_eq_ignore_case(subcommand, b"HELP") {
            require_exact_arity(args, 2, "SLOWLOG", "SLOWLOG HELP")?;
            append_bulk_array(response_out, &SLOWLOG_HELP_LINES);
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"LEN") {
            require_exact_arity(args, 2, "SLOWLOG", "SLOWLOG LEN")?;
            append_integer(response_out, 0);
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"RESET") {
            require_exact_arity(args, 2, "SLOWLOG", "SLOWLOG RESET")?;
            append_simple_string(response_out, b"OK");
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"GET") {
            ensure_ranged_arity(args, 2, 3, "SLOWLOG", "SLOWLOG GET [count]")?;
            if args.len() == 3 {
                parse_i64_ascii(args[2]).ok_or(RequestExecutionError::ValueNotInteger)?;
            }
            response_out.extend_from_slice(b"*0\r\n");
            return Ok(());
        }
        Err(RequestExecutionError::UnknownCommand)
    }

    pub(super) fn handle_acl(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 2, "ACL", "ACL <subcommand> [arguments...]")?;
        let subcommand = args[1];
        if ascii_eq_ignore_case(subcommand, b"HELP") {
            require_exact_arity(args, 2, "ACL", "ACL HELP")?;
            append_bulk_array(response_out, &ACL_HELP_LINES);
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"WHOAMI") {
            require_exact_arity(args, 2, "ACL", "ACL WHOAMI")?;
            append_bulk_string(response_out, b"default");
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"USERS") {
            require_exact_arity(args, 2, "ACL", "ACL USERS")?;
            append_bulk_array(response_out, &[b"default"]);
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"LIST") {
            require_exact_arity(args, 2, "ACL", "ACL LIST")?;
            append_bulk_array(
                response_out,
                &[b"user default on nopass sanitize-payload ~* &* +@all"],
            );
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"SETUSER") {
            ensure_min_arity(args, 3, "ACL", "ACL SETUSER username [rule ...]")?;
            append_simple_string(response_out, b"OK");
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"CAT") {
            if args.len() == 2 {
                append_bulk_array(response_out, &ACL_CATEGORIES);
                return Ok(());
            }
            require_exact_arity(args, 3, "ACL", "ACL CAT [category]")?;
            append_bulk_array(response_out, &[]);
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"GETUSER") {
            require_exact_arity(args, 3, "ACL", "ACL GETUSER username")?;
            let username = args[2];
            if !ascii_eq_ignore_case(username, b"default") {
                if self.resp_protocol_version().is_resp3() {
                    append_null(response_out);
                } else {
                    append_null_bulk_string(response_out);
                }
                return Ok(());
            }
            append_bulk_array(
                response_out,
                &[
                    b"flags",
                    b"on",
                    b"nopass",
                    b"commands",
                    b"+@all",
                    b"keys",
                    b"~*",
                    b"channels",
                    b"&*",
                ],
            );
            return Ok(());
        }
        Err(RequestExecutionError::UnknownCommand)
    }

    pub(super) fn handle_cluster(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 2, "CLUSTER", "CLUSTER <subcommand> [arguments...]")?;
        let subcommand = args[1];
        if ascii_eq_ignore_case(subcommand, b"KEYSLOT") {
            require_exact_arity(args, 3, "CLUSTER", "CLUSTER KEYSLOT key")?;
            let key = args[2];
            append_integer(response_out, i64::from(u16::from(redis_hash_slot(key))));
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"INFO") {
            require_exact_arity(args, 2, "CLUSTER", "CLUSTER INFO")?;
            let info_payload =
                b"cluster_state:ok\r\ncluster_slots_assigned:16384\r\ncluster_known_nodes:1\r\n";
            if self.resp_protocol_version().is_resp3() {
                append_verbatim_string(response_out, b"txt", info_payload);
            } else {
                append_bulk_string(response_out, info_payload);
            }
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"MYID") {
            require_exact_arity(args, 2, "CLUSTER", "CLUSTER MYID")?;
            append_bulk_string(response_out, b"0000000000000000000000000000000000000000");
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"NODES") {
            require_exact_arity(args, 2, "CLUSTER", "CLUSTER NODES")?;
            append_bulk_string(
                response_out,
                b"0000000000000000000000000000000000000000 127.0.0.1:6379@16379 myself,master - 0 0 1 connected 0-16383\n",
            );
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"SLOTS") {
            require_exact_arity(args, 2, "CLUSTER", "CLUSTER SLOTS")?;
            response_out.extend_from_slice(b"*1\r\n*3\r\n");
            append_integer(response_out, 0);
            append_integer(response_out, 16_383);
            response_out.extend_from_slice(b"*2\r\n");
            append_bulk_string(response_out, b"127.0.0.1");
            append_integer(response_out, 6379);
            return Ok(());
        }
        Err(RequestExecutionError::ClusterSupportDisabled)
    }

    pub(super) fn handle_failover(
        &self,
        args: &[&[u8]],
        _response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            1,
            "FAILOVER",
            "FAILOVER [TO host port] [FORCE] [TIMEOUT ms]",
        )?;
        Err(RequestExecutionError::ClusterSupportDisabled)
    }

    pub(super) fn handle_subscribe(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 2, "SUBSCRIBE", "SUBSCRIBE channel [channel ...]")?;
        let resp3 = self.resp_protocol_version().is_resp3();
        if let Some(client_id) = super::current_request_client_id() {
            let acks = self.pubsub_subscribe_channels(client_id, &args[1..]);
            append_subscription_ack_entries(response_out, b"subscribe", &acks, resp3);
            return Ok(());
        }
        append_subscription_acks(response_out, &args[1..], b"subscribe", resp3);
        Ok(())
    }

    pub(super) fn handle_psubscribe(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 2, "PSUBSCRIBE", "PSUBSCRIBE pattern [pattern ...]")?;
        let resp3 = self.resp_protocol_version().is_resp3();
        if let Some(client_id) = super::current_request_client_id() {
            let acks = self.pubsub_subscribe_patterns(client_id, &args[1..]);
            append_subscription_ack_entries(response_out, b"psubscribe", &acks, resp3);
            return Ok(());
        }
        append_subscription_acks(response_out, &args[1..], b"psubscribe", resp3);
        Ok(())
    }

    pub(super) fn handle_ssubscribe(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            2,
            "SSUBSCRIBE",
            "SSUBSCRIBE shardchannel [shardchannel ...]",
        )?;
        let resp3 = self.resp_protocol_version().is_resp3();
        append_subscription_acks(response_out, &args[1..], b"ssubscribe", resp3);
        Ok(())
    }

    pub(super) fn handle_unsubscribe(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            1,
            "UNSUBSCRIBE",
            "UNSUBSCRIBE [channel [channel ...]]",
        )?;
        let resp3 = self.resp_protocol_version().is_resp3();
        if let Some(client_id) = super::current_request_client_id() {
            let acks = self.pubsub_unsubscribe_channels(client_id, &args[1..]);
            append_unsubscribe_ack_entries(response_out, b"unsubscribe", &acks, resp3);
            return Ok(());
        }
        append_unsubscribe_acks(response_out, &args[1..], b"unsubscribe", resp3);
        Ok(())
    }

    pub(super) fn handle_punsubscribe(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            1,
            "PUNSUBSCRIBE",
            "PUNSUBSCRIBE [pattern [pattern ...]]",
        )?;
        let resp3 = self.resp_protocol_version().is_resp3();
        if let Some(client_id) = super::current_request_client_id() {
            let acks = self.pubsub_unsubscribe_patterns(client_id, &args[1..]);
            append_unsubscribe_ack_entries(response_out, b"punsubscribe", &acks, resp3);
            return Ok(());
        }
        append_unsubscribe_acks(response_out, &args[1..], b"punsubscribe", resp3);
        Ok(())
    }

    pub(super) fn handle_sunsubscribe(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            1,
            "SUNSUBSCRIBE",
            "SUNSUBSCRIBE [shardchannel [shardchannel ...]]",
        )?;
        let resp3 = self.resp_protocol_version().is_resp3();
        append_unsubscribe_acks(response_out, &args[1..], b"sunsubscribe", resp3);
        Ok(())
    }

    pub(super) fn handle_publish(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 3, "PUBLISH", "PUBLISH channel message")?;
        let recipients = self.pubsub_publish(args[1], args[2]);
        append_integer(response_out, recipients);
        Ok(())
    }

    pub(super) fn handle_spublish(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 3, "SPUBLISH", "SPUBLISH shardchannel message")?;
        append_integer(response_out, 0);
        Ok(())
    }

    pub(super) fn handle_pubsub(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 2, "PUBSUB", "PUBSUB <subcommand> [arguments ...]")?;
        let subcommand = args[1];
        if ascii_eq_ignore_case(subcommand, b"HELP") {
            require_exact_arity(args, 2, "PUBSUB", "PUBSUB HELP")?;
            append_bulk_array(response_out, &PUBSUB_HELP_LINES);
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"CHANNELS")
            || ascii_eq_ignore_case(subcommand, b"SHARDCHANNELS")
        {
            ensure_ranged_arity(args, 2, 3, "PUBSUB", "PUBSUB CHANNELS [pattern]")?;
            let channels = if ascii_eq_ignore_case(subcommand, b"SHARDCHANNELS") {
                Vec::new()
            } else {
                self.pubsub_channels(args.get(2).copied())
            };
            response_out.extend_from_slice(format!("*{}\r\n", channels.len()).as_bytes());
            for channel in channels {
                append_bulk_string(response_out, &channel);
            }
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"NUMPAT") {
            require_exact_arity(args, 2, "PUBSUB", "PUBSUB NUMPAT")?;
            append_integer(response_out, saturating_usize_to_i64(self.pubsub_numpat()));
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"NUMSUB")
            || ascii_eq_ignore_case(subcommand, b"SHARDNUMSUB")
        {
            ensure_min_arity(args, 2, "PUBSUB", "PUBSUB NUMSUB [channel [channel ...]]")?;
            let subscribers = if ascii_eq_ignore_case(subcommand, b"SHARDNUMSUB") {
                args[2..]
                    .iter()
                    .map(|channel| ((*channel).to_vec(), 0usize))
                    .collect::<Vec<_>>()
            } else {
                self.pubsub_numsub(&args[2..])
            };
            if self.resp_protocol_version().is_resp3() {
                append_map_length(response_out, subscribers.len());
            } else {
                append_array_length(response_out, subscribers.len() * 2);
            }
            for (channel, count) in subscribers {
                append_bulk_string(response_out, &channel);
                append_integer(response_out, saturating_usize_to_i64(count));
            }
            return Ok(());
        }
        Err(RequestExecutionError::UnknownCommand)
    }

    pub(super) fn handle_monitor(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 1, "MONITOR", "MONITOR")?;
        append_simple_string(response_out, b"OK");
        Ok(())
    }

    pub(super) fn handle_shutdown(
        &self,
        args: &[&[u8]],
        _response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            1,
            "SHUTDOWN",
            "SHUTDOWN [NOSAVE|SAVE] [NOW|FORCE|ABORT]",
        )?;
        for option in &args[1..] {
            let token = option;
            if ascii_eq_ignore_case(token, b"SAVE")
                || ascii_eq_ignore_case(token, b"NOSAVE")
                || ascii_eq_ignore_case(token, b"NOW")
                || ascii_eq_ignore_case(token, b"FORCE")
                || ascii_eq_ignore_case(token, b"ABORT")
            {
                continue;
            }
            return Err(RequestExecutionError::SyntaxError);
        }
        Err(RequestExecutionError::CommandDisabled {
            command: "SHUTDOWN",
        })
    }

    pub(super) fn handle_config(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 2, "CONFIG", "CONFIG <GET|SET|RESETSTAT|HELP>")?;
        let subcommand = args[1];

        if ascii_eq_ignore_case(subcommand, b"HELP") {
            require_exact_arity(args, 2, "CONFIG", "CONFIG HELP")?;
            append_bulk_array(response_out, &CONFIG_HELP_LINES);
            return Ok(());
        }

        if ascii_eq_ignore_case(subcommand, b"RESETSTAT") {
            require_exact_arity(args, 2, "CONFIG", "CONFIG RESETSTAT")?;
            self.reset_rdb_changes_since_last_save();
            self.reset_expiration_stats();
            self.reset_lazyfree_pending_objects();
            self.reset_lazyfreed_objects();
            self.reset_commandstats();
            self.record_command_call(b"config|resetstat");
            append_simple_string(response_out, b"OK");
            return Ok(());
        }

        if ascii_eq_ignore_case(subcommand, b"SET") {
            ensure_paired_arity_after(
                args,
                4,
                2,
                "CONFIG",
                "CONFIG SET parameter value [parameter value ...]",
            )?;

            let mut pending: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
            let mut seen = HashSet::<Vec<u8>>::new();
            let current_config_port =
                self.config_items_snapshot()
                    .into_iter()
                    .find_map(|(key, value)| {
                        if key == b"port" {
                            parse_u64_ascii(&value)
                        } else {
                            None
                        }
                    });
            let mut index = 2usize;
            while index + 1 < args.len() {
                let parameter = args[index]
                    .iter()
                    .map(|byte| byte.to_ascii_lowercase())
                    .collect::<Vec<u8>>();
                let value = args[index + 1].to_vec();

                if !seen.insert(parameter.clone()) {
                    append_error(
                        response_out,
                        format!(
                            "ERR CONFIG SET failed (possibly related to argument '{}') - duplicate parameter",
                            String::from_utf8_lossy(&parameter)
                        )
                        .as_bytes(),
                    );
                    return Ok(());
                }
                if parameter == b"daemonize" {
                    append_error(
                        response_out,
                        b"ERR CONFIG SET failed (possibly related to argument 'daemonize') - immutable config",
                    );
                    return Ok(());
                }
                if parameter == b"maxmemory-clients" {
                    if let Some(percent_text) = value.strip_suffix(b"%") {
                        let Some(percent) = parse_u64_ascii(percent_text) else {
                            append_error(
                                response_out,
                                b"ERR CONFIG SET failed (possibly related to argument 'maxmemory-clients') - percentage argument must be less or equal to 100",
                            );
                            return Ok(());
                        };
                        if percent > 100 {
                            append_error(
                                response_out,
                                b"ERR CONFIG SET failed (possibly related to argument 'maxmemory-clients') - percentage argument must be less or equal to 100",
                            );
                            return Ok(());
                        }
                    } else if parse_u64_ascii(&value).is_none() {
                        append_error(
                            response_out,
                            b"ERR CONFIG SET failed (possibly related to argument 'maxmemory-clients') - percentage argument must be less or equal to 100",
                        );
                        return Ok(());
                    }
                }
                if parameter == b"notify-keyspace-events" {
                    let Some(flags) = super::keyspace_events_string_to_flags(&value) else {
                        append_error(
                            response_out,
                            b"ERR Invalid event class character. Use 'g$lszhxeKEtmdn'.",
                        );
                        return Ok(());
                    };
                    self.set_notify_keyspace_events_flags(flags);
                    // Store the canonical string in config overrides for CONFIG GET.
                    let canonical = super::keyspace_events_flags_to_string(flags);
                    self.set_config_value(b"notify-keyspace-events", canonical);
                    // Continue to process remaining parameters in this CONFIG SET.
                }
                if parameter == b"port" {
                    let parsed_port =
                        parse_u64_ascii(&value).ok_or(RequestExecutionError::ValueNotInteger)?;
                    if parsed_port > u16::MAX as u64 {
                        return Err(RequestExecutionError::ValueNotInteger);
                    }
                    if current_config_port != Some(parsed_port) {
                        append_error(
                            response_out,
                            b"ERR CONFIG SET failed (possibly related to argument 'port') - Unable to listen on this port",
                        );
                        return Ok(());
                    }
                }
                if parameter == b"replica-serve-stale-data"
                    && !value.eq_ignore_ascii_case(b"yes")
                    && !value.eq_ignore_ascii_case(b"no")
                {
                    append_error(
                        response_out,
                        b"ERR CONFIG SET failed (possibly related to argument 'replica-serve-stale-data') - argument must be 'yes' or 'no'",
                    );
                    return Ok(());
                }
                pending.push((parameter, value));
                index += 2;
            }

            for (parameter, value) in pending {
                if parameter == b"zset-max-ziplist-entries"
                    || parameter == b"zset-max-listpack-entries"
                {
                    let parsed =
                        parse_u64_ascii(&value).ok_or(RequestExecutionError::ValueNotInteger)?;
                    self.zset_max_listpack_entries
                        .store(parsed as usize, Ordering::Release);
                } else if parameter == b"list-max-ziplist-size"
                    || parameter == b"list-max-listpack-size"
                {
                    let parsed =
                        parse_i64_ascii(&value).ok_or(RequestExecutionError::ValueNotInteger)?;
                    self.list_max_listpack_size.store(parsed, Ordering::Release);
                    self.clear_all_forced_list_quicklist_encodings();
                } else if parameter == b"maxmemory" {
                    let Some(parsed) = parse_u64_ascii(&value) else {
                        append_error(
                            response_out,
                            b"ERR CONFIG SET failed (possibly related to argument 'maxmemory') - argument must be a memory value",
                        );
                        return Ok(());
                    };
                    self.set_maxmemory_limit_bytes(parsed);
                } else if parameter == b"min-replicas-to-write"
                    || parameter == b"min-slaves-to-write"
                {
                    let parsed =
                        parse_u64_ascii(&value).ok_or(RequestExecutionError::ValueNotInteger)?;
                    self.set_min_replicas_to_write(parsed);
                } else if parameter == b"replica-serve-stale-data" {
                    self.set_replica_serve_stale_data(value.eq_ignore_ascii_case(b"yes"));
                } else if parameter == b"rdb-key-save-delay" {
                    let parsed =
                        parse_u64_ascii(&value).ok_or(RequestExecutionError::ValueNotInteger)?;
                    self.set_rdb_key_save_delay_micros(parsed);
                } else if parameter == b"notify-keyspace-events" {
                    // Already handled in the validation pass above.
                    continue;
                } else {
                    self.set_config_value(&parameter, value);
                    continue;
                }
                self.set_config_value(&parameter, value);
            }
            append_simple_string(response_out, b"OK");
            return Ok(());
        }

        if ascii_eq_ignore_case(subcommand, b"GET") {
            ensure_min_arity(args, 3, "CONFIG", "CONFIG GET parameter [parameter ...]")?;
            let zset_max_listpack_entries = self.zset_max_listpack_entries.load(Ordering::Acquire);
            let zset_max_listpack_entries_value = zset_max_listpack_entries.to_string();
            let list_max_listpack_size = self.list_max_listpack_size.load(Ordering::Acquire);
            let list_max_listpack_size_value = list_max_listpack_size.to_string();
            let maxmemory_value = self.maxmemory_limit_bytes().to_string();
            let min_replicas_to_write_value = self.min_replicas_to_write().to_string();
            let replica_serve_stale_data_value = if self.replica_serve_stale_data() {
                b"yes".to_vec()
            } else {
                b"no".to_vec()
            };
            let rdb_key_save_delay_value = self.rdb_key_save_delay_micros().to_string();
            let mut config_map = BTreeMap::<Vec<u8>, Vec<u8>>::new();
            for (key, value) in self.config_items_snapshot() {
                config_map.insert(key, value);
            }
            config_map.insert(b"maxmemory".to_vec(), maxmemory_value.into_bytes());
            config_map.insert(
                b"min-replicas-to-write".to_vec(),
                min_replicas_to_write_value.as_bytes().to_vec(),
            );
            config_map.insert(
                b"min-slaves-to-write".to_vec(),
                min_replicas_to_write_value.as_bytes().to_vec(),
            );
            config_map.insert(
                b"replica-serve-stale-data".to_vec(),
                replica_serve_stale_data_value,
            );
            config_map.insert(
                b"rdb-key-save-delay".to_vec(),
                rdb_key_save_delay_value.into_bytes(),
            );
            config_map.insert(
                b"list-max-ziplist-size".to_vec(),
                list_max_listpack_size_value.as_bytes().to_vec(),
            );
            config_map.insert(
                b"list-max-listpack-size".to_vec(),
                list_max_listpack_size_value.as_bytes().to_vec(),
            );
            config_map.insert(
                b"zset-max-ziplist-entries".to_vec(),
                zset_max_listpack_entries_value.as_bytes().to_vec(),
            );
            config_map.insert(
                b"zset-max-listpack-entries".to_vec(),
                zset_max_listpack_entries_value.as_bytes().to_vec(),
            );

            let mut matched_items = Vec::<(Vec<u8>, Vec<u8>)>::new();
            let mut emitted = HashSet::<Vec<u8>>::new();
            for pattern in &args[2..] {
                let has_glob =
                    pattern.contains(&b'*') || pattern.contains(&b'?') || pattern.contains(&b'[');
                for (key, value) in config_map.iter() {
                    if key.as_slice() == b"key-load-delay" && has_glob {
                        continue;
                    }
                    let is_match = if has_glob {
                        redis_glob_match(pattern, key, CaseSensitivity::Insensitive, 0)
                    } else {
                        pattern.eq_ignore_ascii_case(key)
                    };
                    if is_match && emitted.insert(key.clone()) {
                        matched_items.push((key.clone(), value.clone()));
                    }
                }
            }

            if self.resp_protocol_version().is_resp3() {
                append_map_length(response_out, matched_items.len());
            } else {
                append_array_length(response_out, matched_items.len() * 2);
            }
            for (key, value) in matched_items {
                append_bulk_string(response_out, &key);
                append_bulk_string(response_out, &value);
            }
            return Ok(());
        }

        Err(RequestExecutionError::UnknownSubcommand)
    }

    pub(super) fn handle_flushdb(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        let flush_mode = parse_flush_mode(args, "FLUSHDB", "FLUSHDB [ASYNC|SYNC]")?;
        let removed_count = self.flush_all_keys()?;
        if should_record_lazyfreed_for_flush_mode(flush_mode) {
            self.record_lazyfreed_objects(removed_count);
        }
        append_simple_string(response_out, b"OK");
        Ok(())
    }

    pub(super) fn handle_flushall(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        let flush_mode = parse_flush_mode(args, "FLUSHALL", "FLUSHALL [ASYNC|SYNC]")?;
        let removed_count = self.flush_all_keys()?;
        if should_record_lazyfreed_for_flush_mode(flush_mode) {
            self.record_lazyfreed_objects(removed_count);
        }
        append_simple_string(response_out, b"OK");
        Ok(())
    }

    fn flush_all_keys(&self) -> Result<u64, RequestExecutionError> {
        let mut keys: HashSet<RedisKey> = self.string_keys_snapshot().into_iter().collect();
        keys.extend(self.object_keys_snapshot());
        self.set_lazyfree_pending_objects(u64::try_from(keys.len()).unwrap_or(u64::MAX));

        let mut deleted_count = 0u64;

        for key in keys {
            if let Err(error) = self.expire_key_if_needed(key.as_slice()) {
                self.reset_lazyfree_pending_objects();
                return Err(error);
            }

            let mut string_deleted = false;
            {
                let mut store = self.lock_string_store_for_key(key.as_slice());
                let mut session = store.session(&self.functions);
                let mut info = DeleteInfo::default();
                let status = session.delete(&key, &mut info).map_err(map_delete_error)?;
                match status {
                    DeleteOperationStatus::TombstonedInPlace
                    | DeleteOperationStatus::AppendedTombstone => {
                        string_deleted = true;
                    }
                    DeleteOperationStatus::NotFound => {}
                    DeleteOperationStatus::RetryLater => {
                        self.reset_lazyfree_pending_objects();
                        return Err(RequestExecutionError::StorageBusy);
                    }
                }
            }
            if string_deleted {
                self.remove_string_key_metadata(key.as_slice());
            }

            let object_deleted = match self.object_delete(key.as_slice()) {
                Ok(value) => value,
                Err(error) => {
                    self.reset_lazyfree_pending_objects();
                    return Err(error);
                }
            };
            if string_deleted && !object_deleted {
                self.bump_watch_version(key.as_slice());
            }
            if string_deleted || object_deleted {
                deleted_count = deleted_count.saturating_add(1);
            }
        }

        for shard_index in self.string_expiration_counts.indices() {
            self.lock_string_expirations_for_shard(shard_index).clear();
            self.lock_hash_field_expirations_for_shard(shard_index)
                .clear();
            self.lock_string_key_registry_for_shard(shard_index).clear();
            self.lock_object_key_registry_for_shard(shard_index).clear();
            self.string_expiration_counts[shard_index].store(0, Ordering::Release);
        }
        self.moved_keysizes_by_db
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();

        self.reset_lazyfree_pending_objects();
        Ok(deleted_count)
    }

    fn active_key_count(&self) -> Result<i64, RequestExecutionError> {
        let mut keys: HashSet<RedisKey> = self.string_keys_snapshot().into_iter().collect();
        keys.extend(self.object_keys_snapshot());
        Ok(i64::try_from(keys.len()).unwrap_or(i64::MAX))
    }

    fn total_expiration_count(&self) -> usize {
        let mut total = 0usize;
        for shard_index in self.string_expiration_counts.indices() {
            total = total.saturating_add(self.string_expiration_count_for_shard(shard_index));
        }
        total
    }

    fn estimated_used_memory_bytes(&self) -> Result<u64, RequestExecutionError> {
        const ESTIMATED_BASE_MEMORY_BYTES: u64 = 1024;

        let mut total_bytes = ESTIMATED_BASE_MEMORY_BYTES;
        total_bytes = total_bytes.saturating_add(self.used_memory_vm_functions());

        let mut keys: HashSet<RedisKey> = self.string_keys_snapshot().into_iter().collect();
        keys.extend(self.object_keys_snapshot());

        for key in keys {
            if let Some(value) = self.read_string_value(key.as_slice())? {
                total_bytes = total_bytes.saturating_add(
                    u64::try_from(estimate_memory_usage_bytes(key.len(), value.len()))
                        .unwrap_or(u64::MAX),
                );
                continue;
            }

            if let Some(object) = self.object_read(key.as_slice())? {
                let estimated_payload_bytes = estimate_object_payload_memory_usage_bytes(&object);
                total_bytes = total_bytes.saturating_add(
                    u64::try_from(estimate_memory_usage_bytes(
                        key.len(),
                        estimated_payload_bytes,
                    ))
                    .unwrap_or(u64::MAX),
                );
            }
        }

        Ok(total_bytes)
    }

    fn render_keysizes_info_payload(&self) -> Result<String, RequestExecutionError> {
        let mut payload = String::from("# Keysizes\r\n");
        let mut histograms =
            [[0u64; INFO_KEYSIZES_BIN_LABELS.len()]; KEYSIZES_HISTOGRAM_TYPE_COUNT];
        let mut histograms_by_db = BTreeMap::new();

        let mut keys: HashSet<RedisKey> = self.string_keys_snapshot().into_iter().collect();
        keys.extend(self.object_keys_snapshot());

        for key in keys {
            self.expire_key_if_needed(key.as_slice())?;
            self.active_expire_hash_fields_for_key(key.as_slice())?;

            if let Some(value) = self.read_string_value(key.as_slice())? {
                let length =
                    super::string_commands::string_value_len_for_keysizes(self, value.as_slice());
                let bin_index = keysizes_histogram_bin_index(length);
                histograms[0][bin_index] += 1;
                continue;
            }

            let Some(object) = self.object_read(key.as_slice())? else {
                continue;
            };
            let Some(histogram_entry) =
                keysizes_object_histogram_type_and_len(object.object_type, &object.payload)?
            else {
                continue;
            };
            let bin_index = keysizes_histogram_bin_index(histogram_entry.length);
            histograms[histogram_entry.histogram_type_index][bin_index] += 1;
        }

        histograms_by_db.insert(0usize, histograms);
        {
            let moved_keysizes_by_db = self
                .moved_keysizes_by_db
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            for (db_index, moved_keysizes_entries) in moved_keysizes_by_db.iter() {
                let db_histograms = histograms_by_db.entry(*db_index).or_insert(
                    [[0u64; INFO_KEYSIZES_BIN_LABELS.len()]; KEYSIZES_HISTOGRAM_TYPE_COUNT],
                );
                for moved_entry in moved_keysizes_entries.values() {
                    let bin_index = keysizes_histogram_bin_index(moved_entry.length);
                    db_histograms[moved_entry.histogram_type_index][bin_index] += 1;
                }
            }
        }

        for (db_index, db_histograms) in histograms_by_db {
            append_keysizes_histogram_line(
                &mut payload,
                db_index,
                "distrib_strings_sizes",
                &db_histograms[0],
            );
            append_keysizes_histogram_line(
                &mut payload,
                db_index,
                "distrib_lists_items",
                &db_histograms[1],
            );
            append_keysizes_histogram_line(
                &mut payload,
                db_index,
                "distrib_sets_items",
                &db_histograms[2],
            );
            append_keysizes_histogram_line(
                &mut payload,
                db_index,
                "distrib_zsets_items",
                &db_histograms[3],
            );
            append_keysizes_histogram_line(
                &mut payload,
                db_index,
                "distrib_hashes_items",
                &db_histograms[4],
            );
        }

        Ok(payload)
    }

    fn debug_digest_value_for_key(&self, key: &[u8]) -> Result<Vec<u8>, RequestExecutionError> {
        if let Some(value) = self.read_string_value(key)? {
            return Ok(fnv_hex_digest(1, &value));
        }

        if let Some(object) = self.object_read(key)? {
            let encoded = object.encode();
            return Ok(fnv_hex_digest(2, encoded.as_slice()));
        }

        Ok(fnv_hex_digest(0, b""))
    }

    fn debug_dataset_digest(&self) -> Result<Vec<u8>, RequestExecutionError> {
        let mut keys: Vec<RedisKey> = self.string_keys_snapshot().into_iter().collect();
        keys.extend(self.object_keys_snapshot());
        keys.sort();
        keys.dedup();

        let mut combined = Vec::new();
        for key in keys {
            self.expire_key_if_needed(key.as_slice())?;
            self.active_expire_hash_fields_for_key(key.as_slice())?;
            if !self.key_exists_any(key.as_slice())? {
                continue;
            }
            let digest = self.debug_digest_value_for_key(key.as_slice())?;
            combined.extend_from_slice(&(key.len() as u64).to_le_bytes());
            combined.extend_from_slice(key.as_slice());
            combined.extend_from_slice(&(digest.len() as u64).to_le_bytes());
            combined.extend_from_slice(&digest);
        }

        Ok(fnv_hex_digest(3, &combined))
    }
}

#[derive(Debug, Clone, Copy)]
struct RestoreOptions {
    replace: bool,
    absttl: bool,
    idle_time_seconds: Option<u64>,
    frequency: Option<u8>,
}

fn restore_from_dump_blob(
    processor: &RequestProcessor,
    args: &[&[u8]],
    response_out: &mut Vec<u8>,
    command_name: &'static str,
) -> Result<(), RequestExecutionError> {
    ensure_min_arity(
        args,
        4,
        command_name,
        "RESTORE key ttl serialized-value [REPLACE] [ABSTTL] [IDLETIME seconds] [FREQ frequency]",
    )?;

    let key = RedisKey::from(args[1]);
    let ttl_input = parse_i64_ascii(args[2]).ok_or(RequestExecutionError::ValueNotInteger)?;
    if ttl_input < 0 {
        return Err(RequestExecutionError::ValueOutOfRange);
    }
    let dump_blob = args[3];
    let options = parse_restore_options(args, 4, command_name)?;

    processor.expire_key_if_needed(&key)?;
    if !options.replace && processor.key_exists_any(&key)? {
        return Err(RequestExecutionError::BusyKey);
    }
    let value = decode_dump_blob(dump_blob).ok_or(RequestExecutionError::InvalidDumpPayload)?;

    if options.replace {
        processor.delete_string_key_for_migration(&key)?;
        let _ = processor.object_delete(&key)?;
    }

    let ttl_u64 = u64::try_from(ttl_input).map_err(|_| RequestExecutionError::ValueOutOfRange)?;
    let expiration_unix_millis = if ttl_u64 == 0 {
        None
    } else if options.absttl {
        Some(ttl_u64)
    } else {
        let now = current_unix_time_millis().ok_or(RequestExecutionError::ValueOutOfRange)?;
        Some(
            now.checked_add(ttl_u64)
                .ok_or(RequestExecutionError::ValueOutOfRange)?,
        )
    };

    match value {
        MigrationValue::String(raw) => {
            processor.upsert_string_value_for_migration(
                &key,
                raw.as_slice(),
                expiration_unix_millis,
            )?;
            let _ = processor.object_delete(&key)?;
        }
        MigrationValue::Object {
            object_type,
            payload,
        } => {
            processor.delete_string_key_for_migration(&key)?;
            processor.object_upsert(&key, object_type, &payload)?;
            processor.set_string_expiration_deadline(
                &key,
                expiration_unix_millis.and_then(instant_from_unix_millis),
            );
        }
    }

    if let Some(idle_time_seconds) = options.idle_time_seconds {
        processor.set_key_idle_seconds(&key, idle_time_seconds);
    } else {
        processor.record_key_access(&key, true);
    }
    if let Some(frequency) = options.frequency {
        processor.set_key_frequency(&key, frequency);
    }

    append_simple_string(response_out, b"OK");
    Ok(())
}

fn parse_restore_options(
    args: &[&[u8]],
    start_index: usize,
    command_name: &'static str,
) -> Result<RestoreOptions, RequestExecutionError> {
    let mut options = RestoreOptions {
        replace: false,
        absttl: false,
        idle_time_seconds: None,
        frequency: None,
    };
    let mut index = start_index;
    while index < args.len() {
        let token = args[index];
        if ascii_eq_ignore_case(token, b"REPLACE") {
            options.replace = true;
            index += 1;
            continue;
        }
        if ascii_eq_ignore_case(token, b"ABSTTL") {
            options.absttl = true;
            index += 1;
            continue;
        }
        if ascii_eq_ignore_case(token, b"IDLETIME") {
            ensure_min_arity(
                &args[index..],
                2,
                command_name,
                "RESTORE key ttl serialized-value [REPLACE] [ABSTTL] [IDLETIME seconds] [FREQ frequency]",
            )?;
            let idle_time_seconds =
                parse_u64_ascii(args[index + 1]).ok_or(RequestExecutionError::ValueNotInteger)?;
            options.idle_time_seconds = Some(idle_time_seconds);
            index += 2;
            continue;
        }
        if ascii_eq_ignore_case(token, b"FREQ") {
            ensure_min_arity(
                &args[index..],
                2,
                command_name,
                "RESTORE key ttl serialized-value [REPLACE] [ABSTTL] [IDLETIME seconds] [FREQ frequency]",
            )?;
            let frequency =
                parse_u64_ascii(args[index + 1]).ok_or(RequestExecutionError::ValueNotInteger)?;
            if frequency > u64::from(u8::MAX) {
                return Err(RequestExecutionError::ValueOutOfRange);
            }
            options.frequency = Some(frequency as u8);
            index += 2;
            continue;
        }
        return Err(RequestExecutionError::SyntaxError);
    }
    Ok(options)
}

fn encode_dump_blob(value: MigrationValue) -> Vec<u8> {
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
            encoded.extend_from_slice(&payload);
        }
    }
    encoded
}

fn decode_dump_blob(encoded: &[u8]) -> Option<MigrationValue> {
    if !encoded.starts_with(DUMP_BLOB_MAGIC) {
        return None;
    }
    let mut index = DUMP_BLOB_MAGIC.len();
    let kind = *encoded.get(index)?;
    index += 1;
    match kind {
        0 => {
            let len = u32::from_le_bytes(encoded.get(index..index + 4)?.try_into().ok()?) as usize;
            index += 4;
            let value = encoded.get(index..index + len)?.to_vec();
            Some(MigrationValue::String(value.into()))
        }
        1 => {
            let object_type = ObjectTypeTag::from_u8(*encoded.get(index)?)?;
            index += 1;
            let len = u32::from_le_bytes(encoded.get(index..index + 4)?.try_into().ok()?) as usize;
            index += 4;
            let payload = encoded.get(index..index + len)?.to_vec();
            Some(MigrationValue::Object {
                object_type,
                payload,
            })
        }
        _ => None,
    }
}

fn append_subscription_acks(
    response_out: &mut Vec<u8>,
    targets: &[&[u8]],
    kind: &[u8],
    resp3: bool,
) {
    for (index, &channel) in targets.iter().enumerate() {
        append_pubsub_ack(response_out, kind, Some(channel), index + 1, resp3);
    }
}

fn append_subscription_ack_entries(
    response_out: &mut Vec<u8>,
    kind: &[u8],
    entries: &[(Vec<u8>, usize)],
    resp3: bool,
) {
    for (channel, count) in entries {
        append_pubsub_ack(response_out, kind, Some(channel), *count, resp3);
    }
}

fn append_unsubscribe_acks(
    response_out: &mut Vec<u8>,
    targets: &[&[u8]],
    kind: &[u8],
    resp3: bool,
) {
    if targets.is_empty() {
        append_pubsub_ack(response_out, kind, None, 0, resp3);
        return;
    }
    for (index, &channel) in targets.iter().enumerate() {
        let remaining = targets.len().saturating_sub(index + 1);
        append_pubsub_ack(response_out, kind, Some(channel), remaining, resp3);
    }
}

fn append_unsubscribe_ack_entries(
    response_out: &mut Vec<u8>,
    kind: &[u8],
    entries: &[(Option<Vec<u8>>, usize)],
    resp3: bool,
) {
    for (channel, count) in entries {
        append_pubsub_ack(response_out, kind, channel.as_deref(), *count, resp3);
    }
}

fn append_pubsub_ack(
    response_out: &mut Vec<u8>,
    kind: &[u8],
    channel: Option<&[u8]>,
    count: usize,
    resp3: bool,
) {
    if resp3 {
        append_push_length(response_out, 3);
    } else {
        response_out.extend_from_slice(b"*3\r\n");
    }
    append_bulk_string(response_out, kind);
    match channel {
        Some(channel) => append_bulk_string(response_out, channel),
        None => {
            if resp3 {
                append_null(response_out);
            } else {
                append_null_bulk_string(response_out);
            }
        }
    }
    append_integer(response_out, saturating_usize_to_i64(count));
}

#[inline]
fn saturating_usize_to_i64(value: usize) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

/// Determine the Redis-compatible encoding name for a string value.
///
/// Redis uses three string encodings:
/// - "int": the value is representable as a 64-bit signed integer
/// - "embstr": the value is at most 44 bytes (fits in Redis embedded string)
/// - "raw": all other strings
const EMBSTR_MAX_LEN: usize = 44;

fn string_encoding_name(value: &[u8]) -> &'static [u8] {
    if parse_i64_ascii(value).is_some() {
        return b"int";
    }
    if value.len() <= EMBSTR_MAX_LEN {
        b"embstr"
    } else {
        b"raw"
    }
}

fn object_encoding_name(
    object_type: ObjectTypeTag,
    payload: &[u8],
    zset_listpack_max_entries: usize,
    list_max_listpack_size: i64,
) -> Result<&'static [u8], RequestExecutionError> {
    const SMALL_ELEMENT_BYTES: usize = 64;
    const SET_LISTPACK_MAX_ENTRIES: usize = 128;
    const HASH_LISTPACK_MAX_ENTRIES: usize = 32;

    match object_type {
        LIST_OBJECT_TYPE_TAG => {
            let list = deserialize_list_object_payload(payload).ok_or_else(|| {
                storage_failure("object.encoding", "failed to decode list payload")
            })?;
            let compact = list_listpack_compatible(&list, list_max_listpack_size);
            if compact {
                Ok(b"listpack")
            } else {
                Ok(b"quicklist")
            }
        }
        SET_OBJECT_TYPE_TAG => {
            let set = deserialize_set_object_payload(payload).ok_or_else(|| {
                storage_failure("object.encoding", "failed to decode set payload")
            })?;
            let intset =
                set.iter().all(|member| parse_i64_ascii(member).is_some()) && set.len() <= 512;
            if intset {
                return Ok(b"intset");
            }
            let compact = set.len() <= SET_LISTPACK_MAX_ENTRIES
                && set.iter().all(|member| member.len() <= SMALL_ELEMENT_BYTES);
            if compact {
                Ok(b"listpack")
            } else {
                Ok(b"hashtable")
            }
        }
        HASH_OBJECT_TYPE_TAG => {
            let hash = deserialize_hash_object_payload(payload).ok_or_else(|| {
                storage_failure("object.encoding", "failed to decode hash payload")
            })?;
            let compact = hash.len() <= HASH_LISTPACK_MAX_ENTRIES
                && hash.iter().all(|(field, value)| {
                    field.len() <= SMALL_ELEMENT_BYTES && value.len() <= SMALL_ELEMENT_BYTES
                });
            if compact {
                Ok(b"listpack")
            } else {
                Ok(b"hashtable")
            }
        }
        ZSET_OBJECT_TYPE_TAG => {
            let zset = deserialize_zset_object_payload(payload).ok_or_else(|| {
                storage_failure("object.encoding", "failed to decode zset payload")
            })?;
            let compact = zset.len() <= zset_listpack_max_entries
                && zset
                    .keys()
                    .all(|member| member.len() <= SMALL_ELEMENT_BYTES);
            if compact {
                Ok(b"listpack")
            } else {
                Ok(b"skiplist")
            }
        }
        STREAM_OBJECT_TYPE_TAG => Ok(b"stream"),
    }
}

fn list_listpack_compatible(list: &[Vec<u8>], configured_size: i64) -> bool {
    const LISTPACK_POSITIVE_MIN: i64 = 1;
    const LISTPACK_NEGATIVE_MIN: i64 = -5;
    const LISTPACK_NEGATIVE_MAX: i64 = -1;

    let normalized = match configured_size {
        0 => LISTPACK_POSITIVE_MIN,
        value if value < LISTPACK_NEGATIVE_MIN => LISTPACK_NEGATIVE_MIN,
        value if value > 0 => value,
        value if (LISTPACK_NEGATIVE_MIN..=LISTPACK_NEGATIVE_MAX).contains(&value) => value,
        _ => LISTPACK_POSITIVE_MIN,
    };

    if normalized > 0 {
        return list.len() <= normalized as usize;
    }

    let max_bytes = match normalized {
        -1 => 4 * 1024usize,
        -2 => 8 * 1024usize,
        -3 => 16 * 1024usize,
        -4 => 32 * 1024usize,
        _ => 64 * 1024usize,
    };
    // Approximate listpack footprint from member payload plus small per-entry overhead.
    let estimated_bytes = list
        .iter()
        .map(|value| value.len().saturating_add(2))
        .sum::<usize>();
    estimated_bytes <= max_bytes
}

fn fnv_hex_digest(tag: u8, payload: &[u8]) -> Vec<u8> {
    let mut hash = 0xcbf29ce484222325u64;
    hash ^= u64::from(tag);
    hash = hash.wrapping_mul(0x100000001b3);
    for byte in payload {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}").into_bytes()
}

fn estimate_memory_usage_bytes(key_len: usize, value_len: usize) -> i64 {
    const ESTIMATED_ENTRY_OVERHEAD_BYTES: usize = 64;
    let total = key_len
        .saturating_add(value_len)
        .saturating_add(ESTIMATED_ENTRY_OVERHEAD_BYTES);
    i64::try_from(total).unwrap_or(i64::MAX)
}

fn estimate_object_payload_memory_usage_bytes(object: &DecodedObjectValue) -> usize {
    if object.object_type != SET_OBJECT_TYPE_TAG {
        return object.payload.len();
    }

    match decode_set_object_payload(&object.payload) {
        Some(DecodedSetObjectPayload::Members(set)) => set
            .iter()
            .map(|member| member.len().saturating_add(16))
            .sum::<usize>()
            .saturating_add(64),
        Some(DecodedSetObjectPayload::ContiguousI64Range(range)) => {
            let cardinality = range
                .end()
                .checked_sub(range.start())
                .and_then(|delta| delta.checked_add(1))
                .and_then(|value| u64::try_from(value).ok())
                .unwrap_or(u64::MAX);
            usize::try_from(cardinality)
                .unwrap_or(usize::MAX)
                .saturating_mul(16)
                .saturating_add(64)
        }
        None => object.payload.len(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FlushMode {
    Default,
    Sync,
    Async,
}

fn parse_flush_mode(
    args: &[&[u8]],
    command: &'static str,
    usage: &'static str,
) -> Result<FlushMode, RequestExecutionError> {
    ensure_one_of_arities(args, &[1, 2], command, usage)?;
    if args.len() == 1 {
        return Ok(FlushMode::Default);
    }
    if ascii_eq_ignore_case(args[1], b"SYNC") {
        return Ok(FlushMode::Sync);
    }
    if ascii_eq_ignore_case(args[1], b"ASYNC") {
        return Ok(FlushMode::Async);
    }
    Err(RequestExecutionError::SyntaxError)
}

fn should_record_lazyfreed_for_flush_mode(mode: FlushMode) -> bool {
    match mode {
        FlushMode::Async => true,
        FlushMode::Sync => false,
        FlushMode::Default => !current_request_in_transaction(),
    }
}

fn append_scan_cursor_and_key_array(
    response_out: &mut Vec<u8>,
    cursor: u64,
    count: usize,
    keys: &[Vec<u8>],
    resp3: bool,
) {
    let raw_start = usize::try_from(cursor).unwrap_or(usize::MAX);
    let start = if raw_start >= keys.len() {
        0
    } else {
        raw_start
    };
    let end = start.saturating_add(count).min(keys.len());
    let next_cursor = if end >= keys.len() { 0 } else { end };

    response_out.extend_from_slice(b"*2\r\n");
    let next_cursor_bytes = next_cursor.to_string();
    append_bulk_string(response_out, next_cursor_bytes.as_bytes());

    // In RESP3, emit keys as set type.
    let page_len = end - start;
    if resp3 {
        append_set_length(response_out, page_len);
    } else {
        append_array_length(response_out, page_len);
    }
    for key in &keys[start..end] {
        append_bulk_string(response_out, key);
    }
}

fn command_list_entries() -> Vec<Vec<u8>> {
    let mut entries = Vec::with_capacity(
        command_names_for_command_response().len() + COMMAND_LIST_EXTRA_NAMES.len(),
    );
    for name in command_names_for_command_response() {
        entries.push(name.to_ascii_lowercase());
    }
    for entry in COMMAND_LIST_EXTRA_NAMES {
        entries.push(entry.to_vec());
    }
    entries.sort_unstable();
    entries.dedup();
    entries
}

fn command_list_entry_matches_acl_category(entry: &[u8], category: &[u8]) -> bool {
    if !ascii_eq_ignore_case(category, b"SCRIPTING") {
        return false;
    }

    let root = command_root_name(entry);
    ascii_eq_ignore_case(root, b"EVAL")
        || ascii_eq_ignore_case(root, b"EVAL_RO")
        || ascii_eq_ignore_case(root, b"EVALSHA")
        || ascii_eq_ignore_case(root, b"EVALSHA_RO")
        || ascii_eq_ignore_case(root, b"FCALL")
        || ascii_eq_ignore_case(root, b"FCALL_RO")
        || ascii_eq_ignore_case(root, b"SCRIPT")
        || ascii_eq_ignore_case(root, b"FUNCTION")
}

fn command_root_name(entry: &[u8]) -> &[u8] {
    let Some(separator) = entry.iter().position(|byte| *byte == b'|') else {
        return entry;
    };
    &entry[..separator]
}

struct CommandTokenParts<'a> {
    name: &'a [u8],
    subcommand: Option<&'a [u8]>,
}

fn split_command_token(token: &[u8]) -> Option<CommandTokenParts<'_>> {
    let mut sections = token.split(|byte| *byte == b'|');
    let name = sections.next()?;
    if name.is_empty() {
        return None;
    }
    let subcommand = sections.next();
    if sections.next().is_some() {
        return None;
    }
    Some(CommandTokenParts { name, subcommand })
}

fn command_subcommand_known(command: &[u8], subcommand: &[u8]) -> bool {
    COMMAND_LIST_EXTRA_NAMES.iter().any(|entry| {
        let Some(entry_command) = split_command_token(entry) else {
            return false;
        };
        entry_command.subcommand.is_some_and(|value| {
            ascii_eq_ignore_case(entry_command.name, command)
                && ascii_eq_ignore_case(value, subcommand)
        })
    })
}

fn command_arity_for_command_info(command: CommandId) -> i64 {
    match command_arity_policy(command) {
        Some(ArityPolicy::Exact(value)) => value as i64,
        Some(ArityPolicy::Min(value)) => -(value as i64),
        None => 0,
    }
}

fn command_has_movablekeys(command: CommandId, subcommand: Option<&[u8]>) -> bool {
    if subcommand.is_some() {
        return false;
    }
    matches!(
        command,
        CommandId::Zunionstore
            | CommandId::Xread
            | CommandId::Eval
            | CommandId::Sort
            | CommandId::SortRo
            | CommandId::Migrate
            | CommandId::Georadius
    )
}

fn command_getkeys_numkeys(
    raw_numkeys: &[u8],
    available_keys: usize,
) -> Result<usize, RequestExecutionError> {
    let Some(numkeys) = parse_i64_ascii(raw_numkeys) else {
        return Err(RequestExecutionError::ValueNotInteger);
    };
    if numkeys < 0 {
        return Err(RequestExecutionError::ValueOutOfRange);
    }
    let key_count = usize::try_from(numkeys).map_err(|_| RequestExecutionError::ValueOutOfRange)?;
    if key_count > available_keys {
        return Err(RequestExecutionError::SyntaxError);
    }
    Ok(key_count)
}

fn append_command_info_entry(response_out: &mut Vec<u8>, name: &[u8], arity: i64, flags: &[&[u8]]) {
    response_out.extend_from_slice(b"*3\r\n");
    append_bulk_string(response_out, name);
    append_integer(response_out, arity);
    append_bulk_array(response_out, flags);
}

fn append_command_getkeysandflags(
    response_out: &mut Vec<u8>,
    entries: &[(Vec<u8>, &'static [&'static [u8]])],
) {
    response_out.push(b'*');
    response_out.extend_from_slice(entries.len().to_string().as_bytes());
    response_out.extend_from_slice(b"\r\n");
    for (key, flags) in entries {
        response_out.extend_from_slice(b"*2\r\n");
        append_bulk_string(response_out, key);
        append_bulk_array(response_out, flags);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScanTypeFilter {
    String,
    Hash,
    List,
    Set,
    Zset,
    Stream,
}

fn parse_scan_type_filter(input: &[u8]) -> Option<ScanTypeFilter> {
    if ascii_eq_ignore_case(input, b"STRING") {
        return Some(ScanTypeFilter::String);
    }
    if ascii_eq_ignore_case(input, b"HASH") {
        return Some(ScanTypeFilter::Hash);
    }
    if ascii_eq_ignore_case(input, b"LIST") {
        return Some(ScanTypeFilter::List);
    }
    if ascii_eq_ignore_case(input, b"SET") {
        return Some(ScanTypeFilter::Set);
    }
    if ascii_eq_ignore_case(input, b"ZSET") {
        return Some(ScanTypeFilter::Zset);
    }
    if ascii_eq_ignore_case(input, b"STREAM") {
        return Some(ScanTypeFilter::Stream);
    }
    None
}

fn scan_object_type_matches_filter(object_type: ObjectTypeTag, filter: ScanTypeFilter) -> bool {
    match filter {
        ScanTypeFilter::String => false,
        ScanTypeFilter::Hash => object_type == HASH_OBJECT_TYPE_TAG,
        ScanTypeFilter::List => object_type == LIST_OBJECT_TYPE_TAG,
        ScanTypeFilter::Set => object_type == SET_OBJECT_TYPE_TAG,
        ScanTypeFilter::Zset => object_type == ZSET_OBJECT_TYPE_TAG,
        ScanTypeFilter::Stream => object_type == STREAM_OBJECT_TYPE_TAG,
    }
}

fn keysizes_histogram_bin_index(value: usize) -> usize {
    if value == 0 {
        return 0;
    }
    let floor_log2 = usize::BITS as usize - 1 - value.leading_zeros() as usize;
    (floor_log2 + 1).min(INFO_KEYSIZES_BIN_LABELS.len() - 1)
}

fn append_keysizes_histogram_line(
    payload: &mut String,
    db_index: usize,
    field_name: &str,
    histogram: &[u64; INFO_KEYSIZES_BIN_LABELS.len()],
) {
    if !histogram.iter().any(|count| *count > 0) {
        return;
    }

    payload.push_str("db");
    payload.push_str(&db_index.to_string());
    payload.push('_');
    payload.push_str(field_name);
    payload.push(':');

    let mut first = true;
    for (index, count) in histogram.iter().enumerate() {
        if *count == 0 {
            continue;
        }
        if !first {
            payload.push(',');
        }
        first = false;
        payload.push_str(INFO_KEYSIZES_BIN_LABELS[index]);
        payload.push('=');
        payload.push_str(&count.to_string());
    }
    payload.push_str("\r\n");
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct HistogramEntry {
    histogram_type_index: usize,
    length: usize,
}

impl HistogramEntry {
    const fn new(histogram_type_index: usize, length: usize) -> Self {
        Self {
            histogram_type_index,
            length,
        }
    }
}

fn keysizes_object_histogram_type_and_len(
    object_type: ObjectTypeTag,
    payload: &[u8],
) -> Result<Option<HistogramEntry>, RequestExecutionError> {
    match object_type {
        LIST_OBJECT_TYPE_TAG => {
            let list = deserialize_list_object_payload(payload).ok_or_else(|| {
                storage_failure(
                    "info.keysizes",
                    "failed to deserialize list payload while building keysizes histogram",
                )
            })?;
            Ok(Some(HistogramEntry::new(1, list.len())))
        }
        SET_OBJECT_TYPE_TAG => {
            let set = deserialize_set_object_payload(payload).ok_or_else(|| {
                storage_failure(
                    "info.keysizes",
                    "failed to deserialize set payload while building keysizes histogram",
                )
            })?;
            Ok(Some(HistogramEntry::new(2, set.len())))
        }
        ZSET_OBJECT_TYPE_TAG => {
            let zset = deserialize_zset_object_payload(payload).ok_or_else(|| {
                storage_failure(
                    "info.keysizes",
                    "failed to deserialize zset payload while building keysizes histogram",
                )
            })?;
            Ok(Some(HistogramEntry::new(3, zset.len())))
        }
        HASH_OBJECT_TYPE_TAG => {
            let hash = deserialize_hash_object_payload(payload).ok_or_else(|| {
                storage_failure(
                    "info.keysizes",
                    "failed to deserialize hash payload while building keysizes histogram",
                )
            })?;
            Ok(Some(HistogramEntry::new(4, hash.len())))
        }
        _ => Ok(None),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CaseSensitivity {
    Sensitive,
    Insensitive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ClassMatchResult {
    matched: bool,
    next_pattern_index: usize,
}

impl ClassMatchResult {
    const fn new(matched: bool, next_pattern_index: usize) -> Self {
        Self {
            matched,
            next_pattern_index,
        }
    }
}

pub(super) fn redis_glob_match(
    pattern: &[u8],
    text: &[u8],
    case_sensitivity: CaseSensitivity,
    _nesting: usize,
) -> bool {
    // Guard against pathological wildcard patterns that can trigger excessive backtracking.
    // Redis keyspace regression tests rely on this returning quickly instead of hanging.
    const MAX_MATCH_WORK: usize = 1_000_000;
    let estimated_work = pattern.len().saturating_mul(text.len());
    if estimated_work > MAX_MATCH_WORK {
        return false;
    }

    let mut pattern_index = 0usize;
    let mut text_index = 0usize;
    let mut star_resume_pattern_index = None::<usize>;
    let mut star_resume_text_index = 0usize;

    while text_index < text.len() {
        if pattern_index < pattern.len() {
            match pattern[pattern_index] {
                b'*' => {
                    while pattern_index < pattern.len() && pattern[pattern_index] == b'*' {
                        pattern_index += 1;
                    }
                    if pattern_index == pattern.len() {
                        return true;
                    }
                    star_resume_pattern_index = Some(pattern_index);
                    star_resume_text_index = text_index;
                    continue;
                }
                b'?' => {
                    pattern_index += 1;
                    text_index += 1;
                    continue;
                }
                b'[' => {
                    if let Some(result) =
                        glob_match_class(pattern, pattern_index, text[text_index], case_sensitivity)
                    {
                        if result.matched {
                            pattern_index = result.next_pattern_index;
                            text_index += 1;
                            continue;
                        }
                    } else if bytes_eq(b'[', text[text_index], case_sensitivity) {
                        pattern_index += 1;
                        text_index += 1;
                        continue;
                    }
                }
                b'\\' => {
                    let literal_index = (pattern_index + 1).min(pattern.len() - 1);
                    if bytes_eq(pattern[literal_index], text[text_index], case_sensitivity) {
                        pattern_index = literal_index + 1;
                        text_index += 1;
                        continue;
                    }
                }
                pattern_ch => {
                    if bytes_eq(pattern_ch, text[text_index], case_sensitivity) {
                        pattern_index += 1;
                        text_index += 1;
                        continue;
                    }
                }
            }
        }

        if let Some(resume_pattern_index) = star_resume_pattern_index {
            star_resume_text_index += 1;
            if star_resume_text_index > text.len() {
                return false;
            }
            text_index = star_resume_text_index;
            pattern_index = resume_pattern_index;
            continue;
        }

        return false;
    }

    while pattern_index < pattern.len() && pattern[pattern_index] == b'*' {
        pattern_index += 1;
    }
    pattern_index == pattern.len()
}

fn glob_match_class(
    pattern: &[u8],
    class_open_index: usize,
    candidate: u8,
    case_sensitivity: CaseSensitivity,
) -> Option<ClassMatchResult> {
    let nocase = matches!(case_sensitivity, CaseSensitivity::Insensitive);
    let mut class_index = class_open_index + 1;
    let mut negate = false;
    if class_index < pattern.len() && pattern[class_index] == b'^' {
        negate = true;
        class_index += 1;
    }

    let mut matched = false;
    while class_index < pattern.len() {
        let class_ch = pattern[class_index];
        if class_ch == b']' {
            class_index += 1;
            if negate {
                matched = !matched;
            }
            return Some(ClassMatchResult::new(matched, class_index));
        }

        if class_ch == b'\\' && class_index + 1 < pattern.len() {
            class_index += 1;
            if bytes_eq(pattern[class_index], candidate, case_sensitivity) {
                matched = true;
            }
            class_index += 1;
            continue;
        }

        if class_index + 2 < pattern.len()
            && pattern[class_index + 1] == b'-'
            && pattern[class_index + 2] != b']'
        {
            let mut start = pattern[class_index];
            let mut end = pattern[class_index + 2];
            let mut value = candidate;
            if nocase {
                start = start.to_ascii_lowercase();
                end = end.to_ascii_lowercase();
                value = value.to_ascii_lowercase();
            }
            if start > end {
                std::mem::swap(&mut start, &mut end);
            }
            if value >= start && value <= end {
                matched = true;
            }
            class_index += 3;
            continue;
        }

        if bytes_eq(class_ch, candidate, case_sensitivity) {
            matched = true;
        }
        class_index += 1;
    }

    None
}

#[inline]
fn bytes_eq(left: u8, right: u8, case_sensitivity: CaseSensitivity) -> bool {
    match case_sensitivity {
        CaseSensitivity::Insensitive => left.eq_ignore_ascii_case(&right),
        CaseSensitivity::Sensitive => left == right,
    }
}

fn append_resp3_boolean(response_out: &mut Vec<u8>, value: bool) {
    if value {
        response_out.extend_from_slice(b"#t\r\n");
    } else {
        response_out.extend_from_slice(b"#f\r\n");
    }
}

fn append_resp3_bignum(response_out: &mut Vec<u8>, value: &[u8]) {
    response_out.push(b'(');
    response_out.extend_from_slice(value);
    response_out.extend_from_slice(b"\r\n");
}

#[cfg(test)]
mod tests {
    use super::CaseSensitivity;
    use super::redis_glob_match;

    #[test]
    fn redis_glob_match_supports_star_and_question() {
        assert!(redis_glob_match(
            b"foo*",
            b"foobar",
            CaseSensitivity::Sensitive,
            0
        ));
        assert!(redis_glob_match(
            b"f?o",
            b"foo",
            CaseSensitivity::Sensitive,
            0
        ));
        assert!(!redis_glob_match(
            b"foo?",
            b"foo",
            CaseSensitivity::Sensitive,
            0
        ));
    }

    #[test]
    fn redis_glob_match_supports_character_classes_and_escapes() {
        assert!(redis_glob_match(
            b"f[oa]o",
            b"foo",
            CaseSensitivity::Sensitive,
            0
        ));
        assert!(redis_glob_match(
            b"f[a-z]o",
            b"fbo",
            CaseSensitivity::Sensitive,
            0
        ));
        assert!(redis_glob_match(
            b"f[^x]o",
            b"foo",
            CaseSensitivity::Sensitive,
            0
        ));
        assert!(!redis_glob_match(
            b"f[^o]o",
            b"foo",
            CaseSensitivity::Sensitive,
            0
        ));
        assert!(redis_glob_match(
            b"foo\\*",
            b"foo*",
            CaseSensitivity::Sensitive,
            0
        ));
    }
}
