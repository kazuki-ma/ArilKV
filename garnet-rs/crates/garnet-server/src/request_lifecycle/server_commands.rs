// TLA+ model linkage:
// - formal/tla/specs/SwapDbWatchVisibility.tla

use super::object_store::list_listpack_compatible;
use super::*;
use crate::command_spec::ArityPolicy;
use crate::command_spec::KeyAccessPattern;
use crate::command_spec::command_arity_policy;
use crate::command_spec::command_id_from_name;
use crate::command_spec::command_is_mutating;
use crate::command_spec::command_key_access_pattern;
use crate::command_spec::command_names_for_command_response;

static NEXT_RANDOMKEY_INDEX: AtomicU64 = AtomicU64::new(0);
const COMPATIBLE_REDIS_VERSION: &str = "8.4.0";
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
const ACL_HELP_LINES: [&[u8]; 27] = [
    b"ACL <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
    b"CAT [<category>]",
    b"    List ACL categories and the commands inside them.",
    b"DELUSER <username> [<username> ...]",
    b"    Delete ACL users and associated rules.",
    b"DRYRUN <username> <command> [<arg> ...]",
    b"    Test whether a user can run a command without executing it.",
    b"GENPASS [<bits>]",
    b"    Generate a secure password for ACL users.",
    b"GETUSER <username>",
    b"    Get user details.",
    b"HELP",
    b"    Print this help.",
    b"LIST",
    b"    List ACL rules in ACL file format.",
    b"LOAD",
    b"    Reload users from the ACL file.",
    b"LOG [<count> | RESET]",
    b"    Show ACL log entries.",
    b"SAVE",
    b"    Save the current ACL rules to the ACL file.",
    b"SETUSER <username> [<rule> ...]",
    b"    Modify ACL rules for a user.",
    b"USERS",
    b"    List all configured users.",
    b"WHOAMI",
    b"    Return the current connection username.",
];
const ACL_CATEGORIES: [&[u8]; 21] = [
    b"keyspace",
    b"read",
    b"write",
    b"set",
    b"sortedset",
    b"list",
    b"hash",
    b"string",
    b"bitmap",
    b"hyperloglog",
    b"geo",
    b"stream",
    b"pubsub",
    b"admin",
    b"fast",
    b"slow",
    b"blocking",
    b"dangerous",
    b"connection",
    b"transaction",
    b"scripting",
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
const MEMORY_HELP_LINES: [&[u8]; 13] = [
    b"MEMORY <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
    b"DOCTOR",
    b"    Return memory problems reports.",
    b"HELP",
    b"    Print this help.",
    b"MALLOC-STATS",
    b"    Return allocator statistics.",
    b"PURGE",
    b"    Attempt to purge allocator caches.",
    b"STATS",
    b"    Return information about memory usage.",
    b"USAGE <key> [SAMPLES <count>]",
    b"    Estimate memory usage for a key.",
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

struct TrackingReadsDisabledScope {
    previous_enabled: bool,
}

impl TrackingReadsDisabledScope {
    fn enter() -> Self {
        let previous_enabled = REQUEST_EXECUTION_CONTEXT.with(|state| {
            let mut context = state.get();
            let previous = context.tracking_reads_enabled;
            context.tracking_reads_enabled = false;
            state.set(context);
            previous
        });
        Self { previous_enabled }
    }
}

impl Drop for TrackingReadsDisabledScope {
    fn drop(&mut self) {
        REQUEST_EXECUTION_CONTEXT.with(|state| {
            let mut context = state.get();
            context.tracking_reads_enabled = self.previous_enabled;
            state.set(context);
        });
    }
}
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
const CLIENT_HELP_LINES: [&[u8]; 35] = [
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
    b"KILL <ip:port | <option> [value] ...>",
    b"    Kill connections matching the given filter.",
    b"LIST [TYPE (NORMAL|MASTER|REPLICA|PUBSUB)]",
    b"    Return information about client connections.",
    b"NO-EVICT (ON|OFF)",
    b"    Protect current client connection from eviction.",
    b"NO-TOUCH (ON|OFF)",
    b"    Avoid touching LRU/LFU stats for current client connection commands.",
    b"PAUSE <timeout> [WRITE|ALL]",
    b"    Suspend processing of all clients for <timeout> milliseconds.",
    b"REPLY (ON|OFF|SKIP)",
    b"    Control the replies sent to the current connection.",
    b"SETINFO <option> <value>",
    b"    Set client library info (LIB-NAME or LIB-VER).",
    b"SETNAME <connection-name>",
    b"    Set the current connection name.",
    b"TRACKING (ON|OFF) [REDIRECT <id>] [BCAST] [PREFIX <prefix> [...]] [OPTIN] [OPTOUT]",
    b"    Enable/disable server-assisted client-side caching.",
    b"TRACKINGINFO",
    b"    Return the tracking status and redirect client id for the current connection.",
    b"GETREDIR",
    b"    Return the redirect client id for the current connection.",
    b"UNBLOCK <clientid> [TIMEOUT|ERROR]",
    b"    Unblock a client blocked by a blocking command.",
];
const DEBUG_HELP_LINES: [&[u8]; 30] = [
    b"DEBUG <subcommand> [<arg> [value] [opt] ...]",
    b"Available subcommands:",
    b"DIGEST",
    b"    Compute a dataset digest.",
    b"DIGEST-VALUE <key>",
    b"    Compute a digest for the value stored at <key>.",
    b"HTSTATS-KEY <key> [full]",
    b"    Return synthetic hash table stats for hash-table-backed values.",
    b"LOADAOF",
    b"    Flush and reload the dataset from the AOF file.",
    b"OBJECT <key>",
    b"    Show low-level info about the given key.",
    b"PAUSE-CRON <0|1>",
    b"    Pause or resume active expiration cron sweep.",
    b"POPULATE <count> [prefix] [size]",
    b"    Create string keys named prefix:index without overwriting existing keys.",
    b"PROTOCOL <type>",
    b"    Return a sample of the specified RESP protocol type (ATTRIB|BIGNUM|TRUE|FALSE|VERBATIM).",
    b"RELOAD [NOSAVE|NOFLUSH|MERGE]",
    b"    Trigger a no-op reload.",
    b"REPLYBUFFER (PEAK-RESET-TIME|RESIZING) <value>",
    b"    Control reply buffer settings.",
    b"SET-ACTIVE-EXPIRE <0|1>",
    b"    Enable or disable active expiration sweeps.",
    b"SET-ALLOW-ACCESS-EXPIRED <0|1>",
    b"    Allow commands to access expired keys without deleting them.",
    b"SET-DISABLE-DENY-SCRIPTS <0|1>",
    b"    Temporarily allow/disallow commands that are usually blocked from scripts.",
    b"SLEEP <seconds>",
    b"    Sleep for the specified number of seconds.",
];
const CLUSTER_HELP_LINES: [&[u8]; 24] = [
    b"CLUSTER <subcommand> [<arg> [value] [opt] ...]",
    b"Available subcommands:",
    b"COUNTKEYSINSLOT <slot>",
    b"    Return the number of keys in the specified hash slot.",
    b"GETKEYSINSLOT <slot> <count>",
    b"    Return keys in the specified hash slot.",
    b"INFO",
    b"    Return information about the cluster.",
    b"KEYSLOT <key>",
    b"    Return the hash slot for <key>.",
    b"MYID",
    b"    Return the node ID.",
    b"NODES",
    b"    Return cluster configuration of nodes.",
    b"RESET [HARD|SOFT]",
    b"    Reset a node.",
    b"SAVECONFIG",
    b"    Force save the cluster configuration on disk.",
    b"SHARDS",
    b"    Return information about slot range mappings.",
    b"SLOTS",
    b"    Return information about slots range mappings.",
    b"HELP",
    b"    Print this help.",
];
const DEBUG_PROTOCOL_ATTRIB_REPLY: &[u8] = b"Some real reply following the attribute";
const DEBUG_PROTOCOL_BIGNUM_VALUE: &[u8] = b"1234567999999999999999999999999999999";
const DEBUG_PROTOCOL_VERBATIM_VALUE: &[u8] = b"This is a verbatim\nstring";
const DEBUG_PROTOCOL_ATTRIBUTE_NAME: &[u8] = b"key-popularity";
const DEBUG_PROTOCOL_ATTRIBUTE_KEY: &[u8] = b"key:123";
const DUMP_BLOB_MAGIC: &[u8] = b"GRN1";
const DEBUG_RELOAD_SNAPSHOT_MAGIC_V1: &[u8] = b"GRSNAP1";
const DEBUG_RELOAD_SNAPSHOT_MAGIC_V2: &[u8] = b"GRSNAP2";
const DEBUG_RELOAD_SNAPSHOT_MAGIC_V3: &[u8] = b"GRSNAP3";
const DEBUG_RELOAD_SNAPSHOT_NO_EXPIRATION: u64 = u64::MAX;
const DEBUG_RELOAD_SNAPSHOT_ENCODING_RAW: u8 = 0;
const DEBUG_RELOAD_SNAPSHOT_ENCODING_ZERO_RUN: u8 = 1;
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
    Errorstats,
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
    push_info_section_once(sections, InfoSection::Errorstats);
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
        return;
    }
    if ascii_eq_ignore_case(token, b"ERRORSTATS") {
        push_info_section_once(sections, InfoSection::Errorstats);
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
        let now_micros =
            current_unix_time_micros().ok_or(RequestExecutionError::ValueOutOfRange)?;
        let seconds = (now_micros / 1_000_000).to_string();
        let microseconds = (now_micros % 1_000_000).to_string();
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
            append_array_length(response_out, 2);
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
                let name = args[idx + 1];
                if name.iter().any(|&b| b == b' ' || b == b'\n') {
                    return Err(RequestExecutionError::SyntaxError);
                }
                *self.client_name.lock().unwrap_or_else(|e| e.into_inner()) = name.to_vec();
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
        append_bulk_string(response_out, COMPATIBLE_REDIS_VERSION.as_bytes());
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
        // INFO performs internal full-key scans (for memory/dbsize estimates).
        // These reads must not be added to CLIENT TRACKING tables.
        let _tracking_reads_disabled = TrackingReadsDisabledScope::enter();

        let dbsize = self.active_key_count()?;
        let blocked_clients = self.blocked_clients();
        let (total_blocking_keys, total_blocking_keys_on_nokey) = self.blocking_key_counts();
        let connected_clients = self.connected_clients();
        let watching_clients = self.watching_clients();
        let rdb_bgsave_in_progress = if self.rdb_bgsave_in_progress() { 1 } else { 0 };
        let rdb_changes_since_last_save = self.rdb_changes_since_last_save();
        let (expired_keys, expired_keys_active) = self.expiration_stats_snapshot();
        let total_error_replies = self.total_error_replies();
        let lazyfree_pending_objects = self.lazyfree_pending_objects();
        let lazyfreed_objects = self.lazyfreed_objects();
        let used_memory = self.estimated_used_memory_bytes()?;
        let cached_script_count = self.script_cache_entry_count();
        let scripting_runtime = self.scripting_runtime_config();
        let used_memory_vm_functions = self.used_memory_vm_functions();
        let process_id = std::process::id();
        let (tracking_total_items, tracking_total_keys, tracking_total_prefixes, tracking_clients) =
            self.tracking_info_snapshot();

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
                        format!(
                            "# Server\r\nredis_version:{redis_version}\r\nredis_git_sha1:garnet-rs\r\nredis_mode:standalone\r\nos:Linux\r\nprocess_id:{process_id}\r\ntcp_port:6379\r\nuptime_in_seconds:0\r\nuptime_in_days:0\r\nhz:10\r\nconfigured_hz:10\r\n",
                            redis_version = COMPATIBLE_REDIS_VERSION,
                        )
                        .as_str(),
                    );
                }
                InfoSection::Clients => {
                    payload.push_str(
                        format!(
                            "# Clients\r\nconnected_clients:{connected_clients}\r\nblocked_clients:{blocked_clients}\r\ntracking_clients:{tracking_clients}\r\ntracking_total_items:{tracking_total_items}\r\ntracking_total_keys:{tracking_total_keys}\r\ntracking_total_prefixes:{tracking_total_prefixes}\r\nclients_in_timeout_table:0\r\ntotal_blocking_clients:{blocked_clients}\r\ntotal_blocking_keys:{total_blocking_keys}\r\ntotal_blocking_keys_on_nokey:{total_blocking_keys_on_nokey}\r\nwatching_clients:{watching_clients}\r\n",
                        )
                        .as_str(),
                    );
                }
                InfoSection::Memory => {
                    payload.push_str(
                        format!(
                            "# Memory\r\nused_memory:{used_memory}\r\nused_memory_human:{used_memory_human}\r\nused_memory_rss:{used_memory}\r\nused_memory_peak:{used_memory}\r\nused_memory_peak_human:{used_memory_human}\r\nallocator_allocated:{used_memory}\r\nallocator_active:{used_memory}\r\nallocator_resident:{used_memory}\r\nnumber_of_cached_scripts:{cached_script_count}\r\nmaxmemory:{maxmemory}\r\nmaxmemory_human:{maxmemory_human}\r\nmaxmemory_policy:noeviction\r\nmem_not_counted_for_evict:0\r\nmem_fragmentation_ratio:1.00\r\n",
                            used_memory = used_memory,
                            used_memory_human = format_human_bytes(used_memory),
                            cached_script_count = cached_script_count,
                            maxmemory = self.maxmemory_limit_bytes.load(Ordering::Acquire),
                            maxmemory_human = format_human_bytes(self.maxmemory_limit_bytes.load(Ordering::Acquire)),
                        )
                        .as_str(),
                    );
                }
                InfoSection::Stats => {
                    payload.push_str(
                        format!(
                            "# Stats\r\ntotal_connections_received:0\r\ntotal_commands_processed:0\r\ninstantaneous_ops_per_sec:0\r\ntotal_net_input_bytes:0\r\ntotal_net_output_bytes:0\r\ninstantaneous_input_kbps:0.00\r\ninstantaneous_output_kbps:0.00\r\nrejected_connections:0\r\nkeyspace_hits:0\r\nkeyspace_misses:0\r\ntotal_error_replies:{}\r\ndbsize:{}\r\nrdb_bgsave_in_progress:{}\r\nrdb_changes_since_last_save:{}\r\nexpired_keys:{}\r\nexpired_keys_active:{}\r\nlazyfree_pending_objects:{}\r\nlazyfreed_objects:{}\r\nmigrate_cached_sockets:0\r\n",
                            total_error_replies,
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
                    let replica_addrs = self
                        .server_metrics
                        .get()
                        .map(|metrics| metrics.replica_client_addrs())
                        .unwrap_or_default();
                    payload.push_str("# Replication\r\nrole:master\r\nconnected_slaves:");
                    payload.push_str(&replica_addrs.len().to_string());
                    payload.push_str("\r\n");
                    for (index, addr) in replica_addrs.iter().enumerate() {
                        let (ip, port) = split_client_addr_for_replication(addr);
                        payload.push_str("slave");
                        payload.push_str(&index.to_string());
                        payload.push_str(":ip=");
                        payload.push_str(&ip);
                        payload.push_str(",port=");
                        payload.push_str(&port);
                        payload.push_str(",state=online,offset=0,lag=0\r\n");
                    }
                    payload.push_str(
                        "master_replid:0000000000000000000000000000000000000000\r\nmaster_replid2:0000000000000000000000000000000000000000\r\nmaster_repl_offset:0\r\nsecond_repl_offset:-1\r\nrepl_backlog_active:0\r\nrepl_backlog_size:1048576\r\nrepl_backlog_first_byte_offset:0\r\nrepl_backlog_histlen:0\r\n",
                    );
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
                    payload.push_str(&self.render_keyspace_info_payload()?);
                }
                InfoSection::Keysizes => {
                    payload.push_str(&self.render_keysizes_info_payload()?);
                }
                InfoSection::Commandstats => {
                    payload.push_str(&self.render_commandstats_info_payload());
                }
                InfoSection::Errorstats => {
                    payload.push_str(&self.render_errorstats_info_payload());
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
        parse_configured_db_name_arg(args[1], self.configured_databases())?;
        append_simple_string(response_out, b"OK");
        Ok(())
    }

    pub(super) fn handle_move(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 3, "MOVE", "MOVE key db")?;
        let target_db = parse_configured_db_name_arg(args[2], self.configured_databases())?;
        if target_db == super::current_request_selected_db() {
            return Err(RequestExecutionError::SourceDestinationObjectsSame);
        }
        let key = RedisKey::from(args[1]);

        self.expire_key_if_needed(&key)?;
        self.active_expire_hash_fields_for_key(&key)?;

        let Some(mut entry) = self.export_migration_entry(current_request_selected_db(), &key)?
        else {
            append_integer(response_out, 0);
            return Ok(());
        };

        let target_exists = self.key_exists_any(DbKeyRef::new(target_db, &key))?;
        if target_exists {
            append_integer(response_out, 0);
            return Ok(());
        }

        entry.key = key.clone().into();
        self.with_selected_db(target_db, || self.import_migration_entry(target_db, &entry))?;
        self.delete_string_key_for_migration(DbKeyRef::new(current_request_selected_db(), &key))?;
        let _ = self.object_delete(DbKeyRef::new(current_request_selected_db(), &key))?;

        append_integer(response_out, 1);
        Ok(())
    }

    pub(super) fn handle_swapdb(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 3, "SWAPDB", "SWAPDB index1 index2")?;
        let configured_databases = self.configured_databases();
        let index1 = parse_swapdb_db_name_arg(args[1], RequestExecutionError::InvalidFirstDbIndex)?;
        let index2 =
            parse_swapdb_db_name_arg(args[2], RequestExecutionError::InvalidSecondDbIndex)?;
        if usize::from(index1) >= configured_databases
            || usize::from(index2) >= configured_databases
        {
            return Err(RequestExecutionError::DbIndexOutOfRange);
        }
        if index1 == index2 {
            append_simple_string(response_out, b"OK");
            return Ok(());
        }

        // TLA+ : SwapLogicalDbBindings
        self.swap_logical_databases(index1, index2)?;

        append_simple_string(response_out, b"OK");
        Ok(())
    }

    fn swap_logical_databases(
        &self,
        db1: DbName,
        db2: DbName,
    ) -> Result<(), RequestExecutionError> {
        debug_assert_ne!(db1, db2);

        // TLA+ : InvalidateSwapTrackedKeys
        self.invalidate_all_tracking_entries();
        self.swap_logical_db_storage_bindings(db1, db2)?;
        self.swap_set_hot_state_for_logical_db_pair(db1, db2);
        if let Ok(mut forced) = self.db_catalog.side_state.forced_list_quicklist_keys.lock() {
            forced.swap_databases(db1, db2);
        }
        if let Ok(mut forced) = self.db_catalog.side_state.forced_raw_string_keys.lock() {
            forced.swap_databases(db1, db2);
        }
        if let Ok(mut floors) = self.db_catalog.side_state.forced_set_encoding_floors.lock() {
            floors.swap_databases(db1, db2);
        }
        if let Ok(mut states) = self.db_catalog.side_state.set_debug_ht_state.lock() {
            states.swap_databases(db1, db2);
        }
        if let Ok(mut lru) = self.db_catalog.side_state.key_lru_access_millis.lock() {
            lru.swap_databases(db1, db2);
        }
        if let Ok(mut lfu) = self.db_catalog.side_state.key_lfu_frequency.lock() {
            lfu.swap_databases(db1, db2);
        }

        Ok(())
    }

    fn swap_set_hot_state_for_logical_db_pair(&self, db1: DbName, db2: DbName) {
        let mut hot_state = match self.db_catalog.side_state.set_object_hot_state.lock() {
            Ok(state) => state,
            Err(_) => return,
        };
        let mut moved = HashMap::new();
        moved.reserve(hot_state.entries.len());

        for (key, value) in std::mem::take(&mut hot_state.entries) {
            let remapped_key = Self::remap_scoped_key(key, db1, db2);
            let _ = moved.insert(remapped_key, value);
        }
        hot_state.entries = moved;

        for key in &mut hot_state.lru {
            *key = Self::remap_scoped_key(key.clone(), db1, db2);
        }
    }

    fn remap_scoped_key(key: DbScopedKey, db1: DbName, db2: DbName) -> DbScopedKey {
        if key.db == db1 {
            return DbScopedKey::new(db2, &key.key);
        }
        if key.db == db2 {
            return DbScopedKey::new(db1, &key.key);
        }
        key
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
            "CLIENT <ID|GETNAME|SETNAME|LIST|INFO|KILL|UNBLOCK|PAUSE|UNPAUSE|NO-EVICT|NO-TOUCH|CACHING|REPLY|HELP> [arguments...]",
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
            let name = self.client_name.lock().unwrap_or_else(|e| e.into_inner());
            if name.is_empty() {
                if self.resp_protocol_version().is_resp3() {
                    append_null(response_out);
                } else {
                    append_null_bulk_string(response_out);
                }
            } else {
                append_bulk_string(response_out, &name);
            }
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"SETNAME") {
            require_exact_arity(args, 3, "CLIENT", "CLIENT SETNAME connection-name")?;
            let name = args[2];
            if name.iter().any(|&b| b == b' ' || b == b'\n') {
                append_error(
                    response_out,
                    b"ERR Client names cannot contain spaces, newlines or special characters.",
                );
                return Ok(());
            }
            *self.client_name.lock().unwrap_or_else(|e| e.into_inner()) = name.to_vec();
            append_simple_string(response_out, b"OK");
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"SETINFO") {
            require_exact_arity(args, 4, "CLIENT", "CLIENT SETINFO <option> <value>")?;
            let option = args[2];
            let value = args[3];
            if ascii_eq_ignore_case(option, b"LIB-NAME") {
                if value.iter().any(|&b| b == b' ' || b == b'\n') {
                    append_error(
                        response_out,
                        b"ERR lib-name can only contain characters that are allowed in CLIENT SETNAME",
                    );
                    return Ok(());
                }
                *self
                    .client_lib_name
                    .lock()
                    .unwrap_or_else(|e| e.into_inner()) = value.to_vec();
                append_simple_string(response_out, b"OK");
                return Ok(());
            }
            if ascii_eq_ignore_case(option, b"LIB-VER") {
                *self
                    .client_lib_ver
                    .lock()
                    .unwrap_or_else(|e| e.into_inner()) = value.to_vec();
                append_simple_string(response_out, b"OK");
                return Ok(());
            }
            append_error(
                response_out,
                b"ERR Unrecognized option 'lib-name' or 'lib-ver' expected",
            );
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"LIST") {
            // Accept optional TYPE or ID filter arguments for compatibility.
            if args.len() > 2 {
                let filter_option = args[2];
                if ascii_eq_ignore_case(filter_option, b"TYPE") {
                    if args.len() != 4 {
                        return Err(RequestExecutionError::SyntaxError);
                    }
                    let type_value = args[3];
                    if !ascii_eq_ignore_case(type_value, b"NORMAL")
                        && !ascii_eq_ignore_case(type_value, b"MASTER")
                        && !ascii_eq_ignore_case(type_value, b"REPLICA")
                        && !ascii_eq_ignore_case(type_value, b"PUBSUB")
                    {
                        return Err(RequestExecutionError::SyntaxError);
                    }
                } else if ascii_eq_ignore_case(filter_option, b"ID") {
                    if args.len() < 4 {
                        return Err(RequestExecutionError::SyntaxError);
                    }
                    for id_arg in &args[3..] {
                        parse_u64_ascii(id_arg).ok_or(RequestExecutionError::ValueNotInteger)?;
                    }
                } else {
                    return Err(RequestExecutionError::SyntaxError);
                }
            }
            let client_id = super::current_request_client_id()
                .map(u64::from)
                .unwrap_or(1);
            let selected_db = usize::from(super::current_request_selected_db());
            let client_name = self.client_name.lock().unwrap_or_else(|e| e.into_inner());
            let name_str = String::from_utf8_lossy(&client_name);
            let lib_name = self
                .client_lib_name
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let lib_name_str = String::from_utf8_lossy(&lib_name);
            let lib_ver = self
                .client_lib_ver
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let lib_ver_str = String::from_utf8_lossy(&lib_ver);
            let client_info = format!(
                "id={client_id} addr=127.0.0.1:0 fd=0 name={name_str} db={selected_db} sub=0 psub=0 ssub=0 multi=-1 watch=0 qbuf=0 qbuf-free=0 argv-mem=0 multi-mem=0 tot-mem=0 net-i=0 net-o=0 age=0 idle=0 flags=N events=r cmd=client|list user=default lib-name={lib_name_str} lib-ver={lib_ver_str}\n"
            );
            if self.resp_protocol_version().is_resp3() {
                append_verbatim_string(response_out, b"txt", client_info.as_bytes());
            } else {
                append_bulk_string(response_out, client_info.as_bytes());
            }
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
        if ascii_eq_ignore_case(subcommand, b"INFO") {
            require_exact_arity(args, 2, "CLIENT", "CLIENT INFO")?;
            let client_id = super::current_request_client_id()
                .map(u64::from)
                .unwrap_or(1);
            let selected_db = usize::from(super::current_request_selected_db());
            let client_name = self.client_name.lock().unwrap_or_else(|e| e.into_inner());
            let name_str = String::from_utf8_lossy(&client_name);
            let lib_name = self
                .client_lib_name
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let lib_name_str = String::from_utf8_lossy(&lib_name);
            let lib_ver = self
                .client_lib_ver
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let lib_ver_str = String::from_utf8_lossy(&lib_ver);
            let info_line = format!(
                "id={client_id} addr=127.0.0.1:0 fd=0 name={name_str} db={selected_db} sub=0 psub=0 ssub=0 multi=-1 watch=0 qbuf=0 qbuf-free=0 argv-mem=0 multi-mem=0 tot-mem=0 net-i=0 net-o=0 age=0 idle=0 flags=N events=r cmd=client|info user=default lib-name={lib_name_str} lib-ver={lib_ver_str}\n"
            );
            if self.resp_protocol_version().is_resp3() {
                append_verbatim_string(response_out, b"txt", info_line.as_bytes());
            } else {
                append_bulk_string(response_out, info_line.as_bytes());
            }
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"KILL") {
            ensure_min_arity(
                args,
                3,
                "CLIENT",
                "CLIENT KILL <ip:port | <option> [value] ...>",
            )?;
            append_integer(response_out, 0);
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"CACHING") {
            require_exact_arity(args, 3, "CLIENT", "CLIENT CACHING YES|NO")?;
            let mode = args[2];
            if ascii_eq_ignore_case(mode, b"YES") || ascii_eq_ignore_case(mode, b"NO") {
                append_simple_string(response_out, b"OK");
                return Ok(());
            }
            return Err(RequestExecutionError::SyntaxError);
        }
        if ascii_eq_ignore_case(subcommand, b"REPLY") {
            require_exact_arity(args, 3, "CLIENT", "CLIENT REPLY ON|OFF|SKIP")?;
            let mode = args[2];
            if ascii_eq_ignore_case(mode, b"ON") {
                self.client_reply_mode
                    .store(CLIENT_REPLY_ON, Ordering::Release);
                append_simple_string(response_out, b"OK");
                return Ok(());
            }
            if ascii_eq_ignore_case(mode, b"OFF") {
                self.client_reply_mode
                    .store(CLIENT_REPLY_OFF, Ordering::Release);
                append_simple_string(response_out, b"OK");
                return Ok(());
            }
            if ascii_eq_ignore_case(mode, b"SKIP") {
                self.client_reply_mode
                    .store(CLIENT_REPLY_SKIP, Ordering::Release);
                append_simple_string(response_out, b"OK");
                return Ok(());
            }
            return Err(RequestExecutionError::SyntaxError);
        }
        if ascii_eq_ignore_case(subcommand, b"TRACKING") {
            ensure_min_arity(
                args,
                3,
                "CLIENT",
                "CLIENT TRACKING (ON|OFF) [REDIRECT <id>] [BCAST] [PREFIX <prefix> [...]] [OPTIN] [OPTOUT]",
            )?;
            let mode = args[2];
            if ascii_eq_ignore_case(mode, b"ON") || ascii_eq_ignore_case(mode, b"OFF") {
                append_simple_string(response_out, b"OK");
                return Ok(());
            }
            return Err(RequestExecutionError::SyntaxError);
        }
        if ascii_eq_ignore_case(subcommand, b"TRACKINGINFO") {
            require_exact_arity(args, 2, "CLIENT", "CLIENT TRACKINGINFO")?;
            let resp3 = self.resp_protocol_version().is_resp3();
            if resp3 {
                append_map_length(response_out, 3);
            } else {
                append_array_length(response_out, 6);
            }
            append_bulk_string(response_out, b"flags");
            append_array_length(response_out, 1);
            append_bulk_string(response_out, b"off");
            append_bulk_string(response_out, b"redirect");
            append_integer(response_out, -1);
            append_bulk_string(response_out, b"prefixes");
            append_array_length(response_out, 0);
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"GETREDIR") {
            require_exact_arity(args, 2, "CLIENT", "CLIENT GETREDIR")?;
            append_integer(response_out, -1);
            return Ok(());
        }
        Err(RequestExecutionError::UnknownSubcommand)
    }

    pub(super) fn handle_role(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 1, "ROLE", "ROLE")?;
        append_array_length(response_out, 3);
        append_bulk_string(response_out, b"master");
        append_integer(response_out, 0);
        append_array_length(response_out, 0);
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
        let snapshot = self.build_debug_reload_snapshot(false)?;
        self.set_saved_rdb_snapshot(snapshot);
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
        let snapshot = self.build_debug_reload_snapshot(false)?;
        self.set_saved_rdb_snapshot(snapshot);
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
        let version_banner = format!("Redis ver. {COMPATIBLE_REDIS_VERSION}\n");
        append_bulk_string(response_out, version_banner.as_bytes());
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
        if ascii_eq_ignore_case(subcommand, b"DOCTOR") {
            require_exact_arity(args, 2, "MEMORY", "MEMORY DOCTOR")?;
            if self.resp_protocol_version().is_resp3() {
                append_verbatim_string(response_out, b"txt", b"Sam, I have no memory problems");
            } else {
                append_bulk_string(response_out, b"Sam, I have no memory problems");
            }
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"STATS") {
            require_exact_arity(args, 2, "MEMORY", "MEMORY STATS")?;
            let resp3 = self.resp_protocol_version().is_resp3();
            if resp3 {
                append_map_length(response_out, 4);
            } else {
                append_array_length(response_out, 8);
            }
            append_bulk_string(response_out, b"peak.allocated");
            append_integer(response_out, 0);
            append_bulk_string(response_out, b"total.allocated");
            append_integer(response_out, 0);
            append_bulk_string(response_out, b"startup.allocated");
            append_integer(response_out, 0);
            append_bulk_string(response_out, b"dataset.bytes");
            append_integer(response_out, 0);
            return Ok(());
        }
        if !ascii_eq_ignore_case(subcommand, b"USAGE") {
            return Err(RequestExecutionError::UnknownSubcommand);
        }
        ensure_one_of_arities(args, &[3, 5], "MEMORY", "MEMORY USAGE key [SAMPLES count]")?;
        if args.len() == 5 {
            if !ascii_eq_ignore_case(args[3], b"SAMPLES") {
                return Err(RequestExecutionError::SyntaxError);
            }
            // Validate count is an integer; we ignore the value since our
            // estimation doesn't sample.
            parse_i64_ascii(args[4]).ok_or(RequestExecutionError::ValueNotInteger)?;
        }

        let key = RedisKey::from(args[2]);
        self.expire_key_if_needed(&key)?;

        if let Some(value) =
            self.read_string_value(DbKeyRef::new(current_request_selected_db(), &key))?
        {
            append_integer(
                response_out,
                estimate_string_memory_usage_bytes(key.len(), value.len()),
            );
            return Ok(());
        }
        if let Some(object) =
            self.object_read(DbKeyRef::new(current_request_selected_db(), &key))?
        {
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
        if ascii_eq_ignore_case(subcommand, b"HELP") {
            require_exact_arity(args, 2, "DEBUG", "DEBUG HELP")?;
            append_bulk_array(response_out, &DEBUG_HELP_LINES);
            return Ok(());
        }
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
        if ascii_eq_ignore_case(subcommand, b"SET-ALLOW-ACCESS-EXPIRED") {
            require_exact_arity(args, 3, "DEBUG", "DEBUG SET-ALLOW-ACCESS-EXPIRED <0|1>")?;
            let enabled = args[2];
            if enabled != b"0" && enabled != b"1" {
                return Err(RequestExecutionError::SyntaxError);
            }
            self.set_allow_access_expired(enabled == b"1");
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
        if ascii_eq_ignore_case(subcommand, b"POPULATE") {
            ensure_ranged_arity(
                args,
                3,
                5,
                "DEBUG",
                "DEBUG POPULATE <count> [prefix] [size]",
            )?;

            let key_count = parse_debug_populate_number(args[2])?;
            let key_prefix = if args.len() >= 4 { args[3] } else { b"key" };
            let value_size = if args.len() == 5 {
                let parsed = parse_debug_populate_number(args[4])?;
                usize::try_from(parsed)
                    .map_err(|_| RequestExecutionError::ValueOutOfRangePositive)?
            } else {
                0
            };

            for index in 0..key_count {
                let key_name = encode_debug_populate_key_name(key_prefix, index);
                let key = RedisKey::from(key_name.as_slice());
                self.expire_key_if_needed(&key)?;
                if self.key_exists_any(DbKeyRef::new(current_request_selected_db(), &key))? {
                    continue;
                }

                let value = encode_debug_populate_value(index, value_size);
                self.upsert_string_value_for_migration(
                    DbKeyRef::new(current_request_selected_db(), &key),
                    value.as_slice(),
                    None,
                )?;
            }

            append_simple_string(response_out, b"OK");
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"RELOAD") {
            ensure_min_arity(args, 2, "DEBUG", "DEBUG RELOAD [NOSAVE] [NOFLUSH] [MERGE]")?;
            let mut nosave = false;
            let mut noflush = false;
            let mut merge = false;
            for option in &args[2..] {
                let token = option;
                if ascii_eq_ignore_case(token, b"NOSAVE") {
                    nosave = true;
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
            if !noflush && !merge {
                if !nosave {
                    let snapshot = self.build_debug_reload_snapshot(false)?;
                    self.reload_debug_snapshot_bytes(snapshot)?;
                    append_simple_string(response_out, b"OK");
                    return Ok(());
                }
                if self.reload_debug_snapshot_source()? {
                    append_simple_string(response_out, b"OK");
                    return Ok(());
                }
            }
            // Compatibility path fallback: when no snapshot source exists, keep the
            // historical in-memory reload boundary behavior used by existing tests.
            self.clear_all_forced_list_quicklist_encodings();
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
        if ascii_eq_ignore_case(subcommand, b"HTSTATS-KEY") {
            ensure_ranged_arity(args, 3, 4, "DEBUG", "DEBUG HTSTATS-KEY <key> [FULL]")?;
            let full = if args.len() == 4 {
                if !ascii_eq_ignore_case(args[3], b"FULL") {
                    return Err(RequestExecutionError::SyntaxError);
                }
                true
            } else {
                false
            };

            let key = RedisKey::from(args[2]);
            self.expire_key_if_needed(&key)?;

            let object = match self
                .object_read(DbKeyRef::new(current_request_selected_db(), &key))?
            {
                Some(object) => object,
                None => {
                    if self.key_exists_any(DbKeyRef::new(current_request_selected_db(), &key))? {
                        append_error(
                            response_out,
                            b"ERR The value stored at the specified key is not represented using an hash table",
                        );
                        return Ok(());
                    }
                    return Err(RequestExecutionError::NoSuchKey);
                }
            };

            let zset_max = self.zset_max_listpack_entries.load(Ordering::Acquire);
            let list_max = self.list_max_listpack_size.load(Ordering::Acquire);
            let hash_max = self.hash_max_listpack_entries.load(Ordering::Acquire);
            let hash_max_value = self.hash_max_listpack_value.load(Ordering::Acquire);
            let set_max_lp = self.set_max_listpack_entries.load(Ordering::Acquire);
            let set_max_lp_value = self.set_max_listpack_value.load(Ordering::Acquire);
            let set_max_is = self.set_max_intset_entries.load(Ordering::Acquire);
            let set_encoding_floor = if object.object_type == SET_OBJECT_TYPE_TAG {
                self.set_encoding_floor(DbKeyRef::new(
                    current_request_selected_db(),
                    key.as_slice(),
                ))
            } else {
                None
            };
            let hash_has_field_expirations = object.object_type == HASH_OBJECT_TYPE_TAG
                && self.has_hash_field_expirations_for_key(DbKeyRef::new(
                    current_request_selected_db(),
                    key.as_slice(),
                ));

            let encoding = object_encoding_name(
                object.object_type,
                &object.payload,
                zset_max,
                list_max,
                hash_max,
                hash_max_value,
                set_max_lp,
                set_max_lp_value,
                set_max_is,
                set_encoding_floor,
                hash_has_field_expirations,
            )?;
            if object.object_type != SET_OBJECT_TYPE_TAG || encoding != b"hashtable" {
                append_error(
                    response_out,
                    b"ERR The value stored at the specified key is not represented using an hash table",
                );
                return Ok(());
            }

            let payload = decode_set_object_payload(&object.payload).ok_or_else(|| {
                storage_failure("debug.htstats-key", "failed to decode set payload")
            })?;
            let member_count = payload.member_count();
            let snapshot = self.set_debug_ht_stats_snapshot(
                DbKeyRef::new(current_request_selected_db(), &key),
                member_count,
            );
            let stats = format_set_debug_ht_stats_payload(snapshot, member_count, full);
            if self.resp_protocol_version().is_resp3() {
                append_verbatim_string(response_out, b"txt", &stats);
            } else {
                append_bulk_string(response_out, &stats);
            }
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"PROTOCOL") {
            require_exact_arity(args, 3, "DEBUG", "DEBUG PROTOCOL <subcommand>")?;
            let protocol_subcommand = args[2];
            let resp3 = self.resp_protocol_version().is_resp3();
            if ascii_eq_ignore_case(protocol_subcommand, b"DOUBLE") {
                if resp3 {
                    append_double(response_out, 3.141);
                } else {
                    append_bulk_string(response_out, b"3.141");
                }
                return Ok(());
            }
            if ascii_eq_ignore_case(protocol_subcommand, b"NULL") {
                if resp3 {
                    append_null(response_out);
                } else {
                    append_null_bulk_string(response_out);
                }
                return Ok(());
            }
            if ascii_eq_ignore_case(protocol_subcommand, b"SET") {
                if resp3 {
                    append_set_length(response_out, 3);
                } else {
                    append_array_length(response_out, 3);
                }
                for member in 0..3 {
                    append_integer(response_out, member);
                }
                return Ok(());
            }
            if ascii_eq_ignore_case(protocol_subcommand, b"MAP") {
                if resp3 {
                    append_map_length(response_out, 3);
                } else {
                    append_array_length(response_out, 6);
                }
                for member in 0..3 {
                    append_integer(response_out, member);
                    if resp3 {
                        append_resp3_boolean(response_out, member == 1);
                    } else {
                        append_integer(response_out, if member == 1 { 1 } else { 0 });
                    }
                }
                return Ok(());
            }
            if ascii_eq_ignore_case(protocol_subcommand, b"ATTRIB") {
                if resp3 {
                    response_out.extend_from_slice(b"|1\r\n");
                    append_bulk_string(response_out, DEBUG_PROTOCOL_ATTRIBUTE_NAME);
                    append_array_length(response_out, 2);
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
                    append_verbatim_string(response_out, b"txt", DEBUG_PROTOCOL_VERBATIM_VALUE);
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
            let allow_expired_debug_access =
                !self.active_expire_enabled() || self.allow_access_expired();
            if !allow_expired_debug_access {
                self.expire_key_if_needed(&key)?;
            }
            let list_max = self.list_max_listpack_size.load(Ordering::Acquire);
            let list_compress_depth = self.list_compress_depth();
            let quicklist_debug_suffix = |encoding: &str| -> String {
                if encoding != "quicklist" {
                    return String::new();
                }
                let ql_compressed = if list_compress_depth > 0 { 1 } else { 0 };
                format!(" ql_listpack_max:{list_max} ql_compressed:{ql_compressed}")
            };
            let string_value = with_expired_data_access(allow_expired_debug_access, || {
                self.read_string_value(DbKeyRef::new(current_request_selected_db(), &key))
            })?;
            let object_value = if string_value.is_some() {
                None
            } else {
                self.object_read(DbKeyRef::new(current_request_selected_db(), &key))?
            };
            let (encoding, serialized_len) = if let Some(value) = string_value {
                let enc = String::from_utf8_lossy(string_encoding_name(
                    &value,
                    self.string_encoding_is_forced_raw(DbKeyRef::new(
                        current_request_selected_db(),
                        key.as_slice(),
                    )),
                ))
                .into_owned();
                let len = value.len();
                (enc, len)
            } else if let Some(object) = object_value {
                // Check forced-quicklist flag so DEBUG OBJECT is consistent
                // with OBJECT ENCODING for lists with oversized elements.
                if object.object_type == LIST_OBJECT_TYPE_TAG
                    && self.list_quicklist_encoding_is_forced(DbKeyRef::new(
                        current_request_selected_db(),
                        &key,
                    ))
                {
                    let len = object.payload.len();
                    let db_key = DbKeyRef::new(current_request_selected_db(), &key);
                    let lru = self.key_lru_millis(db_key).unwrap_or(0);
                    let idle = self.key_idle_seconds(db_key).unwrap_or(0);
                    let encoding = "quicklist";
                    let payload = format!(
                        "Value at:0x0 refcount:1 encoding:{encoding} serializedlength:{len} lru:{lru} lru_seconds_idle:{idle}{}",
                        quicklist_debug_suffix(encoding)
                    );
                    if self.resp_protocol_version().is_resp3() {
                        append_verbatim_string(response_out, b"txt", payload.as_bytes());
                    } else {
                        append_bulk_string(response_out, payload.as_bytes());
                    }
                    return Ok(());
                }
                let zset_max = self.zset_max_listpack_entries.load(Ordering::Acquire);
                let list_max = self.list_max_listpack_size.load(Ordering::Acquire);
                let hash_max = self.hash_max_listpack_entries.load(Ordering::Acquire);
                let hash_max_value = self.hash_max_listpack_value.load(Ordering::Acquire);
                let set_max_lp = self.set_max_listpack_entries.load(Ordering::Acquire);
                let set_max_lp_value = self.set_max_listpack_value.load(Ordering::Acquire);
                let set_max_is = self.set_max_intset_entries.load(Ordering::Acquire);
                let set_encoding_floor = if object.object_type == SET_OBJECT_TYPE_TAG {
                    self.set_encoding_floor(DbKeyRef::new(
                        current_request_selected_db(),
                        key.as_slice(),
                    ))
                } else {
                    None
                };
                let hash_has_field_expirations = object.object_type == HASH_OBJECT_TYPE_TAG
                    && self.has_hash_field_expirations_for_key(DbKeyRef::new(
                        current_request_selected_db(),
                        key.as_slice(),
                    ));
                let enc = match object_encoding_name(
                    object.object_type,
                    &object.payload,
                    zset_max,
                    list_max,
                    hash_max,
                    hash_max_value,
                    set_max_lp,
                    set_max_lp_value,
                    set_max_is,
                    set_encoding_floor,
                    hash_has_field_expirations,
                ) {
                    Ok(name) => String::from_utf8_lossy(name).into_owned(),
                    Err(_) => "raw".to_owned(),
                };
                let len = object.payload.len();
                (enc, len)
            } else {
                return Err(RequestExecutionError::NoSuchKey);
            };
            let db_key = DbKeyRef::new(current_request_selected_db(), &key);
            let lru = self.key_lru_millis(db_key).unwrap_or(0);
            let idle = self.key_idle_seconds(db_key).unwrap_or(0);
            let quicklist_metadata = quicklist_debug_suffix(&encoding);
            let payload = format!(
                "Value at:0x0 refcount:1 encoding:{encoding} serializedlength:{serialized_len} lru:{lru} lru_seconds_idle:{idle}{quicklist_metadata}"
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
        if ascii_eq_ignore_case(subcommand, b"SET-DISABLE-DENY-SCRIPTS") {
            require_exact_arity(args, 3, "DEBUG", "DEBUG SET-DISABLE-DENY-SCRIPTS <0|1>")?;
            let flag = args[2];
            if flag != b"0" && flag != b"1" {
                return Err(RequestExecutionError::SyntaxError);
            }
            self.set_debug_disable_deny_scripts(flag == b"1");
            append_simple_string(response_out, b"OK");
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"REPLYBUFFER") {
            require_exact_arity(args, 4, "DEBUG", "DEBUG REPLYBUFFER <subcommand> <value>")?;
            let rb_sub = args[2];
            if ascii_eq_ignore_case(rb_sub, b"PEAK-RESET-TIME") {
                let value = args[3];
                if ascii_eq_ignore_case(value, b"NEVER") {
                    self.set_debug_reply_buffer_peak_reset_time_millis(None);
                    append_simple_string(response_out, b"OK");
                    return Ok(());
                }
                if ascii_eq_ignore_case(value, b"RESET") {
                    self.reset_debug_reply_buffer_peak_reset_time();
                    append_simple_string(response_out, b"OK");
                    return Ok(());
                }
                let millis =
                    parse_i64_ascii(value).ok_or(RequestExecutionError::ValueNotInteger)?;
                let millis = u64::try_from(millis).ok();
                self.set_debug_reply_buffer_peak_reset_time_millis(millis);
                append_simple_string(response_out, b"OK");
                return Ok(());
            }
            if ascii_eq_ignore_case(rb_sub, b"RESIZING") {
                let flag = args[3];
                if flag != b"0" && flag != b"1" {
                    return Err(RequestExecutionError::SyntaxError);
                }
                self.set_debug_reply_buffer_resizing_enabled(flag == b"1");
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
            self.set_debug_reply_copy_avoidance_enabled(flag == b"1");
            append_simple_string(response_out, b"OK");
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"QUICKLIST-PACKED-THRESHOLD") {
            require_exact_arity(args, 3, "DEBUG", "DEBUG QUICKLIST-PACKED-THRESHOLD <bytes>")?;
            // Redis accepts an optional trailing 'b'/'B' suffix (meaning bytes).
            let raw = args[2];
            let num_bytes = if raw.last().map_or(false, |c| *c == b'b' || *c == b'B') {
                &raw[..raw.len() - 1]
            } else {
                raw
            };
            let threshold =
                parse_i64_ascii(num_bytes).ok_or(RequestExecutionError::ValueNotInteger)?;
            if threshold < 0 {
                return Err(RequestExecutionError::ValueNotInteger);
            }
            self.quicklist_packed_threshold
                .store(threshold as usize, Ordering::Release);
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
            if let Some(value) =
                self.read_string_value(DbKeyRef::new(current_request_selected_db(), &key))?
            {
                let encoding = string_encoding_name(
                    &value,
                    self.string_encoding_is_forced_raw(DbKeyRef::new(
                        current_request_selected_db(),
                        key.as_slice(),
                    )),
                );
                append_bulk_string(response_out, encoding);
                return Ok(());
            }
            let Some(object) =
                self.object_read(DbKeyRef::new(current_request_selected_db(), &key))?
            else {
                if resp3 {
                    append_null(response_out);
                } else {
                    append_null_bulk_string(response_out);
                }
                return Ok(());
            };
            if object.object_type == LIST_OBJECT_TYPE_TAG
                && self.list_quicklist_encoding_is_forced(DbKeyRef::new(
                    current_request_selected_db(),
                    &key,
                ))
            {
                append_bulk_string(response_out, b"quicklist");
                return Ok(());
            }
            let zset_max_listpack_entries = self.zset_max_listpack_entries.load(Ordering::Acquire);
            let list_max_listpack_size = self.list_max_listpack_size.load(Ordering::Acquire);
            let hash_max_listpack_entries = self.hash_max_listpack_entries.load(Ordering::Acquire);
            let hash_max_listpack_value = self.hash_max_listpack_value.load(Ordering::Acquire);
            let set_max_listpack_entries = self.set_max_listpack_entries.load(Ordering::Acquire);
            let set_max_listpack_value = self.set_max_listpack_value.load(Ordering::Acquire);
            let set_max_intset_entries = self.set_max_intset_entries.load(Ordering::Acquire);
            let set_encoding_floor = if object.object_type == SET_OBJECT_TYPE_TAG {
                self.set_encoding_floor(DbKeyRef::new(
                    current_request_selected_db(),
                    key.as_slice(),
                ))
            } else {
                None
            };
            let hash_has_field_expirations = object.object_type == HASH_OBJECT_TYPE_TAG
                && self.has_hash_field_expirations_for_key(DbKeyRef::new(
                    current_request_selected_db(),
                    key.as_slice(),
                ));
            let encoding = object_encoding_name(
                object.object_type,
                &object.payload,
                zset_max_listpack_entries,
                list_max_listpack_size,
                hash_max_listpack_entries,
                hash_max_listpack_value,
                set_max_listpack_entries,
                set_max_listpack_value,
                set_max_intset_entries,
                set_encoding_floor,
                hash_has_field_expirations,
            )?;
            append_bulk_string(response_out, encoding);
            return Ok(());
        }

        if ascii_eq_ignore_case(subcommand, b"REFCOUNT") {
            require_exact_arity(args, 3, "OBJECT", "OBJECT REFCOUNT key")?;
            let key = RedisKey::from(args[2]);
            self.expire_key_if_needed(&key)?;
            if self.key_exists_any(DbKeyRef::new(current_request_selected_db(), &key))? {
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
            if !self.key_exists_any(DbKeyRef::new(current_request_selected_db(), &key))? {
                if resp3 {
                    append_null(response_out);
                } else {
                    append_null_bulk_string(response_out);
                }
                return Ok(());
            }
            append_integer(
                response_out,
                self.key_idle_seconds(DbKeyRef::new(current_request_selected_db(), &key))
                    .unwrap_or(0),
            );
            return Ok(());
        }

        if ascii_eq_ignore_case(subcommand, b"FREQ") {
            require_exact_arity(args, 3, "OBJECT", "OBJECT FREQ key")?;
            let key = RedisKey::from(args[2]);
            self.expire_key_if_needed(&key)?;
            if !self.key_exists_any(DbKeyRef::new(current_request_selected_db(), &key))? {
                if resp3 {
                    append_null(response_out);
                } else {
                    append_null_bulk_string(response_out);
                }
                return Ok(());
            }
            append_integer(
                response_out,
                i64::from(
                    self.key_frequency(DbKeyRef::new(current_request_selected_db(), &key))
                        .unwrap_or(0),
                ),
            );
            return Ok(());
        }

        Err(RequestExecutionError::UnknownSubcommand)
    }

    pub(super) fn handle_keys(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 2, "KEYS", "KEYS pattern")?;
        let pattern = args[1];
        let allow_access_expired = self.allow_access_expired();

        let mut keys: HashSet<RedisKey> = self
            .string_keys_snapshot(current_request_selected_db())
            .into_iter()
            .collect();
        keys.extend(self.object_keys_snapshot(current_request_selected_db()));

        let mut matched = Vec::new();
        for key in keys {
            self.expire_key_if_needed(key.as_slice())?;

            if allow_access_expired {
                if redis_glob_match(pattern, key.as_slice(), CaseSensitivity::Sensitive, 0) {
                    matched.push(key);
                }
                continue;
            }

            let string_exists =
                self.key_exists(DbKeyRef::new(current_request_selected_db(), key.as_slice()))?;
            let object_exists = self
                .object_key_exists(DbKeyRef::new(current_request_selected_db(), key.as_slice()))?;
            if !string_exists {
                self.untrack_string_key(DbKeyRef::new(
                    current_request_selected_db(),
                    key.as_slice(),
                ));
            }
            if !object_exists {
                self.untrack_object_key(DbKeyRef::new(
                    current_request_selected_db(),
                    key.as_slice(),
                ));
            }
            if !(string_exists || object_exists) {
                continue;
            }
            if redis_glob_match(pattern, key.as_slice(), CaseSensitivity::Sensitive, 0) {
                matched.push(key);
            }
        }

        append_array_length(response_out, matched.len());
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

        let mut keys: HashSet<RedisKey> = self
            .string_keys_snapshot(current_request_selected_db())
            .into_iter()
            .collect();
        keys.extend(self.object_keys_snapshot(current_request_selected_db()));
        let expire_paused = self.is_expire_action_paused();

        let mut live_keys = Vec::new();
        for key in keys {
            if expire_paused {
                // During CLIENT PAUSE, keep keys discoverable for RANDOMKEY even
                // when they are logically expired. This matches Redis behavior
                // and avoids spinning when only paused-expired keys exist.
                live_keys.push(key);
                continue;
            }
            self.expire_key_if_needed(key.as_slice())?;

            let string_exists =
                self.key_exists(DbKeyRef::new(current_request_selected_db(), key.as_slice()))?;
            let object_exists = self
                .object_key_exists(DbKeyRef::new(current_request_selected_db(), key.as_slice()))?;
            if !string_exists {
                self.untrack_string_key(DbKeyRef::new(
                    current_request_selected_db(),
                    key.as_slice(),
                ));
            }
            if !object_exists {
                self.untrack_object_key(DbKeyRef::new(
                    current_request_selected_db(),
                    key.as_slice(),
                ));
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

        let mut keys: HashSet<RedisKey> = self
            .string_keys_snapshot(current_request_selected_db())
            .into_iter()
            .collect();
        keys.extend(self.object_keys_snapshot(current_request_selected_db()));

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
            let string_exists =
                self.key_exists(DbKeyRef::new(current_request_selected_db(), key.as_slice()))?;
            let object_exists = self
                .object_key_exists(DbKeyRef::new(current_request_selected_db(), key.as_slice()))?;
            if !string_exists {
                self.untrack_string_key(DbKeyRef::new(
                    current_request_selected_db(),
                    key.as_slice(),
                ));
            }
            if !object_exists {
                self.untrack_object_key(DbKeyRef::new(
                    current_request_selected_db(),
                    key.as_slice(),
                ));
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
        let Some(object) = self.object_read(DbKeyRef::new(current_request_selected_db(), key))?
        else {
            self.untrack_object_key(DbKeyRef::new(current_request_selected_db(), key));
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
        if ascii_eq_ignore_case(args[1], b"DOCS") {
            // An empty COMMAND DOCS table is treated as authoritative by redis-cli
            // and breaks command-line hinting. Until real docs are emitted, return
            // an error so clients can fall back to their bundled command metadata.
            return Err(RequestExecutionError::UnknownSubcommand);
        }
        if ascii_eq_ignore_case(args[1], b"GETKEYS") {
            return self.handle_command_getkeys(args, response_out);
        }
        if ascii_eq_ignore_case(args[1], b"GETKEYSANDFLAGS") {
            return self.handle_command_getkeysandflags(args, response_out);
        }

        Err(RequestExecutionError::UnknownSubcommand)
    }

    fn handle_command_list(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        let resp3 = self.resp_protocol_version().is_resp3();
        let entries = command_list_entries();
        if args.len() == 2 {
            let refs: Vec<&[u8]> = entries.iter().map(Vec::as_slice).collect();
            if resp3 {
                append_set_length(response_out, refs.len());
                for item in &refs {
                    append_bulk_string(response_out, item);
                }
            } else {
                append_bulk_array(response_out, &refs);
            }
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

        if resp3 {
            append_set_length(response_out, filtered.len());
            for item in &filtered {
                append_bulk_string(response_out, item);
            }
        } else {
            append_bulk_array(response_out, &filtered);
        }
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
                append_array_length(response_out, 0);
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
                    let sub_known = ct
                        .subcommand
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
                    append_null_array(response_out);
                }
                continue;
            };
            let command_id = command_id_from_name(command_token.name);
            let subcommand_known = command_token
                .subcommand
                .is_some_and(|value| command_subcommand_known(command_token.name, value));
            if command_id.is_none() || (command_token.subcommand.is_some() && !subcommand_known) {
                if !resp3 {
                    append_null_array(response_out);
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
        } else if ascii_eq_ignore_case(target, b"SORT") || ascii_eq_ignore_case(target, b"SORT_RO")
        {
            ensure_min_arity(args, 4, "COMMAND", "COMMAND GETKEYS SORT key [option ...]")?;
            keys.push(args[3]);
            // Scan for the last STORE destination key.
            let mut last_store: Option<&[u8]> = None;
            let mut index = 4usize;
            while index < args.len() {
                if ascii_eq_ignore_case(args[index], b"STORE") && index + 1 < args.len() {
                    last_store = Some(args[index + 1]);
                    index += 2;
                } else {
                    index += 1;
                }
            }
            if let Some(store_key) = last_store {
                keys.push(store_key);
            }
        } else {
            let target_command = command_id_from_name(target);
            match target_command {
                Some(id) if command_key_access_pattern(id) == KeyAccessPattern::FirstKey => {
                    ensure_min_arity(
                        args,
                        4,
                        "COMMAND",
                        "COMMAND GETKEYS <command> key [arg ...]",
                    )?;
                    keys.push(args[3]);
                }
                _ => return Err(RequestExecutionError::SyntaxError),
            }
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
                if ascii_eq_ignore_case(token, b"IFEQ")
                    || ascii_eq_ignore_case(token, b"IFNE")
                    || ascii_eq_ignore_case(token, b"IFDEQ")
                    || ascii_eq_ignore_case(token, b"IFDNE")
                {
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
        } else if ascii_eq_ignore_case(target, b"SORT") || ascii_eq_ignore_case(target, b"SORT_RO")
        {
            ensure_min_arity(
                args,
                4,
                "COMMAND",
                "COMMAND GETKEYSANDFLAGS SORT key [option ...]",
            )?;
            entries.push((args[3].to_vec(), &COMMAND_FLAGS_RO_ACCESS));
            // Track the last STORE destination (Redis uses the last one when
            // multiple STORE tokens appear).
            let mut last_store: Option<&[u8]> = None;
            let mut index = 4usize;
            while index < args.len() {
                if ascii_eq_ignore_case(args[index], b"STORE") {
                    if index + 1 >= args.len() {
                        return Err(RequestExecutionError::InvalidArguments);
                    }
                    last_store = Some(args[index + 1]);
                    index += 2;
                } else {
                    index += 1;
                }
            }
            if let Some(store_key) = last_store {
                entries.push((store_key.to_vec(), &COMMAND_FLAGS_OW_UPDATE));
            }
        } else if ascii_eq_ignore_case(target, b"DELEX") {
            ensure_min_arity(
                args,
                4,
                "COMMAND",
                "COMMAND GETKEYSANDFLAGS DELEX key [IFEQ value|IFNE value|IFDEQ digest|IFDNE digest]",
            )?;
            if args.len() == 4 {
                entries.push((args[3].to_vec(), &COMMAND_FLAGS_RM_DELETE));
            } else if args.len() == 6
                && (ascii_eq_ignore_case(args[4], b"IFEQ")
                    || ascii_eq_ignore_case(args[4], b"IFNE")
                    || ascii_eq_ignore_case(args[4], b"IFDEQ")
                    || ascii_eq_ignore_case(args[4], b"IFDNE"))
            {
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
            let target_command = command_id_from_name(target);
            match target_command {
                Some(id) if command_key_access_pattern(id) == KeyAccessPattern::FirstKey => {
                    ensure_min_arity(
                        args,
                        4,
                        "COMMAND",
                        "COMMAND GETKEYSANDFLAGS <command> key [arg ...]",
                    )?;
                    let flags = if command_is_mutating(id) {
                        &COMMAND_FLAGS_OW_UPDATE
                    } else {
                        &COMMAND_FLAGS_RO_ACCESS
                    };
                    entries.push((args[3].to_vec(), flags));
                }
                _ => return Err(RequestExecutionError::InvalidArguments),
            }
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
        if let Some(value) =
            self.read_string_value(DbKeyRef::new(current_request_selected_db(), &key))?
        {
            append_bulk_string(
                response_out,
                &encode_dump_blob(MigrationValue::String(value.into())),
            );
            return Ok(());
        }
        if let Some(object) =
            self.object_read(DbKeyRef::new(current_request_selected_db(), &key))?
        {
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
            append_array_length(response_out, latest.len());
            for event in latest {
                append_array_length(response_out, 4);
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
            append_array_length(response_out, history.len());
            for sample in history {
                append_array_length(response_out, 2);
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
            if self.resp_protocol_version().is_resp3() {
                append_verbatim_string(response_out, b"txt", LATENCY_DOCTOR_DISABLED_MESSAGE);
            } else {
                append_bulk_string(response_out, LATENCY_DOCTOR_DISABLED_MESSAGE);
            }
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

            let resp3 = self.resp_protocol_version().is_resp3();
            if resp3 {
                append_map_length(response_out, selected.len());
            } else {
                append_array_length(response_out, selected.len() * 2);
            }
            for command_name in selected {
                append_bulk_string(response_out, command_name.as_bytes());
                let count = command_counts
                    .get(&command_name)
                    .copied()
                    .unwrap_or_default();
                if resp3 {
                    // RESP3: each command value is a map with "calls" and "histogram_usec" keys.
                    append_map_length(response_out, 2);
                    append_bulk_string(response_out, b"calls");
                    append_integer(response_out, count as i64);
                    append_bulk_string(response_out, b"histogram_usec");
                    append_map_length(response_out, 1);
                    append_integer(response_out, 1);
                    append_integer(response_out, count as i64);
                } else {
                    let histogram = format!("calls {count} histogram_usec 1 1 1");
                    append_bulk_string(response_out, histogram.as_bytes());
                }
            }
            return Ok(());
        }
        Err(RequestExecutionError::UnknownSubcommand)
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
            append_array_length(response_out, 0);
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"LOAD") {
            ensure_min_arity(args, 3, "MODULE", "MODULE LOAD <path> [<arg> ...]")?;
            append_error(response_out, b"ERR Module loading is not supported");
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"LOADEX") {
            ensure_min_arity(args, 3, "MODULE", "MODULE LOADEX <path> [args ...]")?;
            append_error(response_out, b"ERR Module loading is not supported");
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"UNLOAD") {
            require_exact_arity(args, 3, "MODULE", "MODULE UNLOAD <name>")?;
            append_error(
                response_out,
                b"ERR Error unloading module: no such module with that name",
            );
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"HELP") {
            require_exact_arity(args, 2, "MODULE", "MODULE HELP")?;
            append_bulk_array(response_out, &MODULE_HELP_LINES);
            return Ok(());
        }
        Err(RequestExecutionError::UnknownSubcommand)
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
            append_integer(response_out, self.slowlog_len() as i64);
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"RESET") {
            require_exact_arity(args, 2, "SLOWLOG", "SLOWLOG RESET")?;
            self.reset_slowlog();
            append_simple_string(response_out, b"OK");
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"GET") {
            ensure_ranged_arity(args, 2, 3, "SLOWLOG", "SLOWLOG GET [count]")?;
            let mut count = 10usize;
            if args.len() == 3 {
                let requested =
                    parse_i64_ascii(args[2]).ok_or(RequestExecutionError::ValueNotInteger)?;
                if requested < -1 {
                    return Err(
                        RequestExecutionError::SlowlogCountMustBeGreaterThanOrEqualToMinusOne,
                    );
                }
                if requested == -1 {
                    count = self.slowlog_len();
                } else {
                    count = usize::try_from(requested).unwrap_or(usize::MAX);
                }
            }
            let entries = self.slowlog_entries(Some(count));
            append_array_length(response_out, entries.len());
            for entry in entries {
                append_array_length(response_out, 6);
                append_integer(response_out, entry.id as i64);
                append_integer(response_out, entry.unix_time_seconds);
                append_integer(response_out, entry.duration_micros);
                append_array_length(response_out, entry.args.len());
                for argument in entry.args {
                    append_bulk_string(response_out, &argument);
                }
                append_bulk_string(response_out, &entry.peer_id);
                append_bulk_string(response_out, &entry.client_name);
            }
            return Ok(());
        }
        Err(RequestExecutionError::UnknownSubcommand)
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
        if ascii_eq_ignore_case(subcommand, b"DELUSER") {
            ensure_min_arity(args, 3, "ACL", "ACL DELUSER username [username ...]")?;
            // Only "default" user exists; it cannot be deleted.
            for &username in &args[2..] {
                if ascii_eq_ignore_case(username, b"DEFAULT") {
                    append_error(response_out, b"ERR The 'default' user cannot be removed");
                    return Ok(());
                }
            }
            append_integer(response_out, 0);
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"GENPASS") {
            ensure_ranged_arity(args, 2, 3, "ACL", "ACL GENPASS [bits]")?;
            let hex_chars = if args.len() == 3 {
                let bits =
                    parse_u64_ascii(args[2]).ok_or(RequestExecutionError::ValueNotInteger)?;
                if bits == 0 || bits > 4096 {
                    append_error(
                        response_out,
                        b"ERR ACL GENPASS argument must be the number of bits for the output password, a positive number up to 4096",
                    );
                    return Ok(());
                }
                // Each hex char encodes 4 bits; round up.
                ((bits + 3) / 4) as usize
            } else {
                64 // Default: 256 bits = 64 hex chars
            };
            let mut hex_output = Vec::with_capacity(hex_chars);
            let mut remaining = hex_chars;
            while remaining > 0 {
                let rand_val = self.next_random_u64();
                let hex_chunk = format!("{rand_val:016x}");
                let take = remaining.min(16);
                hex_output.extend_from_slice(&hex_chunk.as_bytes()[..take]);
                remaining -= take;
            }
            append_bulk_string(response_out, &hex_output);
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"LOG") {
            ensure_ranged_arity(args, 2, 3, "ACL", "ACL LOG [count | RESET]")?;
            if args.len() == 3 {
                if ascii_eq_ignore_case(args[2], b"RESET") {
                    append_simple_string(response_out, b"OK");
                    return Ok(());
                }
                parse_i64_ascii(args[2]).ok_or(RequestExecutionError::ValueNotInteger)?;
            }
            append_array_length(response_out, 0);
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"SAVE") {
            require_exact_arity(args, 2, "ACL", "ACL SAVE")?;
            append_simple_string(response_out, b"OK");
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"LOAD") {
            require_exact_arity(args, 2, "ACL", "ACL LOAD")?;
            append_simple_string(response_out, b"OK");
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"DRYRUN") {
            ensure_min_arity(args, 4, "ACL", "ACL DRYRUN username command [arg ...]")?;
            // Single-user stub: the "default" user can run everything.
            append_simple_string(response_out, b"OK");
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"GETUSER") {
            require_exact_arity(args, 3, "ACL", "ACL GETUSER username")?;
            let username = args[2];
            if !ascii_eq_ignore_case(username, b"DEFAULT") {
                if self.resp_protocol_version().is_resp3() {
                    append_null(response_out);
                } else {
                    append_null_bulk_string(response_out);
                }
                return Ok(());
            }
            let resp3 = self.resp_protocol_version().is_resp3();
            // Valkey emits a 6-field map: flags, passwords, commands, keys,
            // channels, selectors.
            if resp3 {
                append_map_length(response_out, 6);
            } else {
                append_array_length(response_out, 12);
            }

            append_bulk_string(response_out, b"flags");
            if resp3 {
                append_set_length(response_out, 2);
            } else {
                append_array_length(response_out, 2);
            }
            append_bulk_string(response_out, b"on");
            append_bulk_string(response_out, b"nopass");

            append_bulk_string(response_out, b"passwords");
            append_array_length(response_out, 0);

            append_bulk_string(response_out, b"commands");
            append_bulk_string(response_out, b"+@all");

            append_bulk_string(response_out, b"keys");
            append_bulk_string(response_out, b"~*");

            append_bulk_string(response_out, b"channels");
            append_bulk_string(response_out, b"&*");

            append_bulk_string(response_out, b"selectors");
            append_array_length(response_out, 0);

            return Ok(());
        }
        Err(RequestExecutionError::UnknownSubcommand)
    }

    pub(super) fn handle_cluster(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 2, "CLUSTER", "CLUSTER <subcommand> [arguments...]")?;
        let subcommand = args[1];
        if ascii_eq_ignore_case(subcommand, b"HELP") {
            append_bulk_array(response_out, &CLUSTER_HELP_LINES);
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"KEYSLOT") {
            require_exact_arity(args, 3, "CLUSTER", "CLUSTER KEYSLOT key")?;
            let key = args[2];
            append_integer(response_out, i64::from(u16::from(redis_hash_slot(key))));
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"INFO") {
            require_exact_arity(args, 2, "CLUSTER", "CLUSTER INFO")?;
            let info_payload =
                b"cluster_state:ok\r\ncluster_slots_assigned:16384\r\ncluster_slots_ok:16384\r\ncluster_slots_pfail:0\r\ncluster_slots_fail:0\r\ncluster_known_nodes:1\r\ncluster_size:1\r\ncluster_current_epoch:1\r\ncluster_my_epoch:1\r\ncluster_stats_messages_sent:0\r\ncluster_stats_messages_received:0\r\ntotal_cluster_links_buffer_limit_exceeded:0\r\n";
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
            append_array_length(response_out, 1);
            append_array_length(response_out, 3);
            append_integer(response_out, 0);
            append_integer(response_out, 16_383);
            append_array_length(response_out, 2);
            append_bulk_string(response_out, b"127.0.0.1");
            append_integer(response_out, 6379);
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"SAVECONFIG") {
            require_exact_arity(args, 2, "CLUSTER", "CLUSTER SAVECONFIG")?;
            append_simple_string(response_out, b"OK");
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"COUNTKEYSINSLOT") {
            require_exact_arity(args, 3, "CLUSTER", "CLUSTER COUNTKEYSINSLOT slot")?;
            let slot = parse_u64_ascii(args[2]).ok_or(RequestExecutionError::ValueNotInteger)?;
            if slot > 16383 {
                append_error(response_out, b"ERR Invalid or out of range slot");
                return Ok(());
            }
            let slot = garnet_cluster::SlotNumber::new(slot as u16);
            let keys =
                self.migration_keys_for_slot(current_request_selected_db(), slot, usize::MAX);
            append_integer(response_out, i64::try_from(keys.len()).unwrap_or(i64::MAX));
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"GETKEYSINSLOT") {
            require_exact_arity(args, 4, "CLUSTER", "CLUSTER GETKEYSINSLOT slot count")?;
            let slot = parse_u64_ascii(args[2]).ok_or(RequestExecutionError::ValueNotInteger)?;
            if slot > 16383 {
                append_error(response_out, b"ERR Invalid or out of range slot");
                return Ok(());
            }
            let count = parse_u64_ascii(args[3]).ok_or(RequestExecutionError::ValueNotInteger)?;
            let max_keys = usize::try_from(count).unwrap_or(usize::MAX);
            let slot = garnet_cluster::SlotNumber::new(slot as u16);
            let keys = self.migration_keys_for_slot(current_request_selected_db(), slot, max_keys);
            append_array_length(response_out, keys.len());
            for key in keys {
                append_bulk_string(response_out, &key);
            }
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"RESET") {
            ensure_ranged_arity(args, 2, 3, "CLUSTER", "CLUSTER RESET [HARD|SOFT]")?;
            if args.len() == 3 {
                let mode = args[2];
                if !ascii_eq_ignore_case(mode, b"HARD") && !ascii_eq_ignore_case(mode, b"SOFT") {
                    return Err(RequestExecutionError::SyntaxError);
                }
            }
            append_simple_string(response_out, b"OK");
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"SHARDS") {
            require_exact_arity(args, 2, "CLUSTER", "CLUSTER SHARDS")?;
            // Return a single shard covering all slots with this node.
            append_array_length(response_out, 1);
            append_array_length(response_out, 4);
            append_bulk_string(response_out, b"slots");
            append_array_length(response_out, 2);
            append_integer(response_out, 0);
            append_integer(response_out, 16_383);
            append_bulk_string(response_out, b"nodes");
            append_array_length(response_out, 1);
            append_array_length(response_out, 8);
            append_bulk_string(response_out, b"id");
            append_bulk_string(response_out, b"0000000000000000000000000000000000000000");
            append_bulk_string(response_out, b"port");
            append_integer(response_out, 6379);
            append_bulk_string(response_out, b"ip");
            append_bulk_string(response_out, b"127.0.0.1");
            append_bulk_string(response_out, b"role");
            append_bulk_string(response_out, b"master");
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
            append_array_length(response_out, channels.len());
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
        Err(RequestExecutionError::UnknownSubcommand)
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
            self.reset_error_reply_stats();
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
                            b"ERR Invalid event class character. Use 'g$lszhxeKEtmdnoc'.",
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
                if parameter == b"maxmemory-policy" {
                    let valid = [
                        &b"noeviction"[..],
                        b"allkeys-lru",
                        b"volatile-lru",
                        b"allkeys-lfu",
                        b"volatile-lfu",
                        b"allkeys-random",
                        b"volatile-random",
                        b"volatile-ttl",
                    ];
                    if !valid.iter().any(|v| value.eq_ignore_ascii_case(v)) {
                        append_error(
                            response_out,
                            b"ERR CONFIG SET failed (possibly related to argument 'maxmemory-policy') - Invalid maxmemory policy",
                        );
                        return Ok(());
                    }
                }
                if parameter == b"loglevel" {
                    let valid = [&b"debug"[..], b"verbose", b"notice", b"warning"];
                    if !valid.iter().any(|v| value.eq_ignore_ascii_case(v)) {
                        append_error(
                            response_out,
                            b"ERR CONFIG SET failed (possibly related to argument 'loglevel') - Invalid argument 'loglevel' - must be one of debug verbose notice warning",
                        );
                        return Ok(());
                    }
                }
                if parameter == b"hz" {
                    let Some(hz_val) = parse_u64_ascii(&value) else {
                        append_error(
                            response_out,
                            b"ERR CONFIG SET failed (possibly related to argument 'hz') - argument must be between 1 and 500",
                        );
                        return Ok(());
                    };
                    if hz_val < 1 || hz_val > 500 {
                        append_error(
                            response_out,
                            b"ERR CONFIG SET failed (possibly related to argument 'hz') - argument must be between 1 and 500",
                        );
                        return Ok(());
                    }
                }
                if (parameter == b"appendonly"
                    || parameter == b"dynamic-hz"
                    || parameter == b"lazyfree-lazy-expire"
                    || parameter == b"lazyfree-lazy-eviction"
                    || parameter == b"lazyfree-lazy-user-del"
                    || parameter == b"lazyfree-lazy-server-del"
                    || parameter == b"lazyfree-lazy-user-flush"
                    || parameter == b"rdbcompression"
                    || parameter == b"rdbchecksum"
                    || parameter == b"activedefrag"
                    || parameter == b"activerehashing"
                    || parameter == b"aof-use-rdb-preamble"
                    || parameter == b"no-appendfsync-on-rewrite"
                    || parameter == b"stop-writes-on-bgsave-error"
                    || parameter == b"set-proc-title"
                    || parameter == b"jemalloc-bg-thread"
                    || parameter == b"latency-tracking"
                    || parameter == b"cluster-enabled"
                    || parameter == b"replica-read-only"
                    || parameter == b"repl-diskless-sync")
                    && !value.eq_ignore_ascii_case(b"yes")
                    && !value.eq_ignore_ascii_case(b"no")
                {
                    append_error(
                        response_out,
                        format!(
                            "ERR CONFIG SET failed (possibly related to argument '{}') - argument must be 'yes' or 'no'",
                            String::from_utf8_lossy(&parameter)
                        )
                        .as_bytes(),
                    );
                    return Ok(());
                }
                if parameter == b"stream-idmp-duration" || parameter == b"stream-idmp-maxsize" {
                    let parsed = match parse_i64_ascii(&value) {
                        Some(parsed) => parsed,
                        None => {
                            append_error(
                                response_out,
                                format!(
                                    "ERR CONFIG SET failed (possibly related to argument '{}') - argument must be a valid integer",
                                    String::from_utf8_lossy(&parameter)
                                )
                                .as_bytes(),
                            );
                            return Ok(());
                        }
                    };
                    let (minimum, maximum) = if parameter == b"stream-idmp-duration" {
                        (1i64, 86_400i64)
                    } else {
                        (1i64, 10_000i64)
                    };
                    if parsed < minimum || parsed > maximum {
                        append_error(
                            response_out,
                            format!(
                                "ERR CONFIG SET failed (possibly related to argument '{}') - argument must be between {} and {} inclusive",
                                String::from_utf8_lossy(&parameter),
                                minimum,
                                maximum
                            )
                            .as_bytes(),
                        );
                        return Ok(());
                    }
                }
                if parameter == b"slowlog-log-slower-than" {
                    let parsed =
                        parse_i64_ascii(&value).ok_or(RequestExecutionError::ValueNotInteger);
                    let Ok(parsed) = parsed else {
                        append_error(
                            response_out,
                            format!(
                                "ERR CONFIG SET failed (possibly related to argument '{}') - argument couldn't be parsed into an integer",
                                String::from_utf8_lossy(&parameter)
                            )
                            .as_bytes(),
                        );
                        return Ok(());
                    };
                    if parsed < -1 {
                        append_error(
                            response_out,
                            format!(
                                "ERR CONFIG SET failed (possibly related to argument '{}') - argument must be greater than or equal to -1",
                                String::from_utf8_lossy(&parameter)
                            )
                            .as_bytes(),
                        );
                        return Ok(());
                    }
                }
                if (parameter == b"timeout"
                    || parameter == b"tcp-keepalive"
                    || parameter == b"databases"
                    || parameter == b"lua-time-limit"
                    || parameter == b"slowlog-max-len"
                    || parameter == b"repl-backlog-size"
                    || parameter == b"repl-diskless-sync-delay"
                    || parameter == b"hash-max-ziplist-entries"
                    || parameter == b"hash-max-ziplist-value"
                    || parameter == b"hash-max-listpack-entries"
                    || parameter == b"hash-max-listpack-value"
                    || parameter == b"set-max-intset-entries"
                    || parameter == b"set-max-listpack-entries"
                    || parameter == b"set-max-listpack-value"
                    || parameter == b"proto-max-bulk-len"
                    || parameter == b"client-query-buffer-limit"
                    || parameter == b"lfu-log-factor"
                    || parameter == b"lfu-decay-time"
                    || parameter == b"list-compress-depth"
                    || parameter == b"stream-node-max-bytes"
                    || parameter == b"stream-node-max-entries"
                    || parameter == b"maxclients"
                    || parameter == b"repl-backlog-ttl"
                    || parameter == b"repl-min-slaves-to-write"
                    || parameter == b"cluster-node-timeout"
                    || parameter == b"tracking-table-max-keys"
                    || parameter == b"min-replicas-max-lag"
                    || parameter == b"min-slaves-max-lag"
                    || parameter == b"repl-min-slaves-max-lag")
                    && parse_u64_ascii(&value).is_none()
                {
                    append_error(
                        response_out,
                        format!(
                            "ERR CONFIG SET failed (possibly related to argument '{}') - argument must be a valid integer",
                            String::from_utf8_lossy(&parameter)
                        )
                        .as_bytes(),
                    );
                    return Ok(());
                }
                if parameter == b"appendfsync" {
                    let valid = [&b"always"[..], b"everysec", b"no"];
                    if !valid.iter().any(|v| value.eq_ignore_ascii_case(v)) {
                        append_error(
                            response_out,
                            b"ERR CONFIG SET failed (possibly related to argument 'appendfsync') - argument must be 'always', 'everysec' or 'no'",
                        );
                        return Ok(());
                    }
                }
                if parameter == b"dir" {
                    pending.push((parameter, normalize_config_dir_value(&value)));
                    index += 2;
                    continue;
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
                } else if parameter == b"hash-max-ziplist-entries"
                    || parameter == b"hash-max-listpack-entries"
                {
                    let parsed =
                        parse_u64_ascii(&value).ok_or(RequestExecutionError::ValueNotInteger)?;
                    self.hash_max_listpack_entries
                        .store(parsed as usize, Ordering::Release);
                } else if parameter == b"hash-max-ziplist-value"
                    || parameter == b"hash-max-listpack-value"
                {
                    let parsed =
                        parse_u64_ascii(&value).ok_or(RequestExecutionError::ValueNotInteger)?;
                    self.hash_max_listpack_value
                        .store(parsed as usize, Ordering::Release);
                } else if parameter == b"set-max-listpack-entries" {
                    let parsed =
                        parse_u64_ascii(&value).ok_or(RequestExecutionError::ValueNotInteger)?;
                    self.set_max_listpack_entries
                        .store(parsed as usize, Ordering::Release);
                } else if parameter == b"set-max-listpack-value" {
                    let parsed =
                        parse_u64_ascii(&value).ok_or(RequestExecutionError::ValueNotInteger)?;
                    self.set_max_listpack_value
                        .store(parsed as usize, Ordering::Release);
                } else if parameter == b"set-max-intset-entries" {
                    let parsed =
                        parse_u64_ascii(&value).ok_or(RequestExecutionError::ValueNotInteger)?;
                    self.set_max_intset_entries
                        .store(parsed as usize, Ordering::Release);
                } else if parameter == b"maxmemory" {
                    let Some(parsed) = parse_u64_ascii(&value) else {
                        append_error(
                            response_out,
                            b"ERR CONFIG SET failed (possibly related to argument 'maxmemory') - argument must be a memory value",
                        );
                        return Ok(());
                    };
                    self.set_maxmemory_limit_bytes(parsed);
                    self.evict_keys_to_fit_maxmemory(parsed)?;
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
                } else if parameter == b"tracking-table-max-keys" {
                    let parsed =
                        parse_u64_ascii(&value).ok_or(RequestExecutionError::ValueNotInteger)?;
                    self.set_tracking_table_max_keys(parsed);
                    self.enforce_tracking_table_max_keys();
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
            let hash_max_listpack_entries = self.hash_max_listpack_entries.load(Ordering::Acquire);
            let hash_max_listpack_entries_value = hash_max_listpack_entries.to_string();
            let hash_max_listpack_value = self.hash_max_listpack_value.load(Ordering::Acquire);
            let hash_max_listpack_value_value = hash_max_listpack_value.to_string();
            let set_max_listpack_entries = self.set_max_listpack_entries.load(Ordering::Acquire);
            let set_max_listpack_entries_value = set_max_listpack_entries.to_string();
            let set_max_listpack_value = self.set_max_listpack_value.load(Ordering::Acquire);
            let set_max_listpack_value_value = set_max_listpack_value.to_string();
            let set_max_intset_entries = self.set_max_intset_entries.load(Ordering::Acquire);
            let set_max_intset_entries_value = set_max_intset_entries.to_string();
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
            config_map.insert(
                b"hash-max-ziplist-entries".to_vec(),
                hash_max_listpack_entries_value.as_bytes().to_vec(),
            );
            config_map.insert(
                b"hash-max-listpack-entries".to_vec(),
                hash_max_listpack_entries_value.as_bytes().to_vec(),
            );
            config_map.insert(
                b"hash-max-ziplist-value".to_vec(),
                hash_max_listpack_value_value.as_bytes().to_vec(),
            );
            config_map.insert(
                b"hash-max-listpack-value".to_vec(),
                hash_max_listpack_value_value.as_bytes().to_vec(),
            );
            config_map.insert(
                b"set-max-listpack-entries".to_vec(),
                set_max_listpack_entries_value.as_bytes().to_vec(),
            );
            config_map.insert(
                b"set-max-listpack-value".to_vec(),
                set_max_listpack_value_value.as_bytes().to_vec(),
            );
            config_map.insert(
                b"set-max-intset-entries".to_vec(),
                set_max_intset_entries_value.as_bytes().to_vec(),
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
        if ascii_eq_ignore_case(subcommand, b"REWRITE") {
            require_exact_arity(args, 2, "CONFIG", "CONFIG REWRITE")?;
            append_error(
                response_out,
                b"ERR The server is running without a config file",
            );
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
        let removed_count = self.flush_current_db_keys()?;
        clear_tracking_invalidation_collection();
        self.invalidate_all_tracking_entries();
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
        let removed_count = self.flush_all_logical_databases()?;
        if should_record_lazyfreed_for_flush_mode(flush_mode) {
            self.record_lazyfreed_objects(removed_count);
        }
        append_simple_string(response_out, b"OK");
        Ok(())
    }

    fn flush_current_db_keys(&self) -> Result<u64, RequestExecutionError> {
        let selected_db = current_request_selected_db();
        if !self.logical_db_uses_main_runtime(selected_db)? {
            self.materialize_set_hot_entries_for_db(selected_db)?;
            let Some(keys) = self.with_auxiliary_db_state_read(selected_db, |state| {
                state
                    .entries
                    .iter()
                    .filter_map(|(key, entry)| entry.value.as_ref().map(|_| key.clone()))
                    .collect::<Vec<_>>()
            })?
            else {
                self.clear_set_hot_entries_for_db(selected_db);
                return Ok(0);
            };
            self.set_lazyfree_pending_objects(u64::try_from(keys.len()).unwrap_or(u64::MAX));

            let mut deleted_count = 0u64;
            for key in keys {
                self.expire_key_if_needed(key.as_slice())?;
                let string_deleted = self.delete_string_value(DbKeyRef::new(
                    current_request_selected_db(),
                    key.as_slice(),
                ))?;
                let object_deleted = self
                    .object_delete(DbKeyRef::new(current_request_selected_db(), key.as_slice()))?;
                if string_deleted && !object_deleted {
                    self.bump_watch_version(DbKeyRef::new(
                        current_request_selected_db(),
                        key.as_slice(),
                    ));
                }
                if string_deleted || object_deleted {
                    deleted_count = deleted_count.saturating_add(1);
                }
            }

            let Some(auxiliary_storage) = self.auxiliary_storage_for_logical_db(selected_db)?
            else {
                self.clear_set_hot_entries_for_db(selected_db);
                self.reset_lazyfree_pending_objects();
                return Ok(deleted_count);
            };
            self.db_catalog
                .auxiliary_databases
                .remove_if_empty(auxiliary_storage)?;
            self.clear_set_hot_entries_for_db(selected_db);
            self.reset_lazyfree_pending_objects();
            return Ok(deleted_count);
        }

        self.materialize_set_hot_entries_for_db(selected_db)?;
        let mut keys: HashSet<RedisKey> = self
            .string_keys_snapshot(current_request_selected_db())
            .into_iter()
            .collect();
        keys.extend(self.object_keys_snapshot(current_request_selected_db()));
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
                self.remove_string_key_metadata(DbKeyRef::new(
                    current_request_selected_db(),
                    key.as_slice(),
                ));
            }

            let object_deleted = match self
                .object_delete(DbKeyRef::new(current_request_selected_db(), key.as_slice()))
            {
                Ok(value) => value,
                Err(error) => {
                    self.reset_lazyfree_pending_objects();
                    return Err(error);
                }
            };
            if string_deleted && !object_deleted {
                self.bump_watch_version(DbKeyRef::new(
                    current_request_selected_db(),
                    key.as_slice(),
                ));
            }
            if string_deleted || object_deleted {
                deleted_count = deleted_count.saturating_add(1);
            }
        }

        for shard_index in self
            .db_catalog
            .main_db_runtime
            .string_expiration_counts
            .indices()
        {
            self.lock_string_expirations_for_shard(shard_index).clear();
            self.lock_hash_field_expirations_for_shard(shard_index)
                .clear();
            self.lock_string_key_registry_for_shard(shard_index).clear();
            self.lock_object_key_registry_for_shard(shard_index).clear();
            self.db_catalog.main_db_runtime.string_expiration_counts[shard_index]
                .store(0, Ordering::Release);
        }
        self.clear_set_hot_entries_for_db(selected_db);
        self.reset_lazyfree_pending_objects();
        Ok(deleted_count)
    }

    fn flush_all_logical_databases(&self) -> Result<u64, RequestExecutionError> {
        let mut removed = 0u64;
        for db_index in 0..self.configured_databases() {
            removed = removed.saturating_add(
                self.with_selected_db(DbName::new(db_index), || self.flush_current_db_keys())?,
            );
        }
        clear_tracking_invalidation_collection();
        self.invalidate_all_tracking_entries();
        Ok(removed)
    }

    fn active_key_count(&self) -> Result<i64, RequestExecutionError> {
        let mut keys: HashSet<RedisKey> = self
            .string_keys_snapshot(current_request_selected_db())
            .into_iter()
            .collect();
        keys.extend(self.object_keys_snapshot(current_request_selected_db()));
        Ok(i64::try_from(keys.len()).unwrap_or(i64::MAX))
    }

    fn expiration_count_for_selected_db(&self) -> usize {
        let selected_db = current_request_selected_db();
        let mut keys: HashSet<RedisKey> =
            self.string_keys_snapshot(selected_db).into_iter().collect();
        keys.extend(self.object_keys_snapshot(selected_db));
        keys.into_iter()
            .filter(|key| {
                self.expiration_unix_millis(DbKeyRef::new(selected_db, key.as_slice()))
                    .is_some()
            })
            .count()
    }

    fn info_key_count_for_selected_db(&self) -> usize {
        let selected_db = current_request_selected_db();
        let mut keys: HashSet<RedisKey> =
            self.string_keys_snapshot(selected_db).into_iter().collect();
        keys.extend(self.object_keys_snapshot(selected_db));
        keys.into_iter()
            .filter(|key| {
                let db_key = DbKeyRef::new(selected_db, key.as_slice());
                self.key_exists_any_without_expiring(db_key)
                    .unwrap_or(false)
                    || self.expiration_unix_millis(db_key).is_some()
            })
            .count()
    }

    fn render_keyspace_info_payload(&self) -> Result<String, RequestExecutionError> {
        let mut payload = String::from("# Keyspace\r\n");
        let mut dbs = Vec::new();
        for db_index in 0..self.configured_databases() {
            let db = DbName::new(db_index);
            let key_count = self.with_selected_db(db, || self.info_key_count_for_selected_db());
            if key_count == 0 {
                continue;
            }
            let expires = self.with_selected_db(db, || self.expiration_count_for_selected_db());
            dbs.push((db, key_count, expires));
        }

        for (db, key_count, expires) in dbs {
            payload.push_str(
                format!(
                    "db{}:keys={},expires={},avg_ttl=0\r\n",
                    usize::from(db),
                    key_count,
                    expires
                )
                .as_str(),
            );
        }

        Ok(payload)
    }

    fn estimated_used_memory_bytes(&self) -> Result<u64, RequestExecutionError> {
        const ESTIMATED_BASE_MEMORY_BYTES: u64 = 1024;

        let mut total_bytes = ESTIMATED_BASE_MEMORY_BYTES;
        total_bytes = total_bytes.saturating_add(self.used_memory_vm_functions());

        let mut keys: HashSet<RedisKey> = self
            .string_keys_snapshot(current_request_selected_db())
            .into_iter()
            .collect();
        keys.extend(self.object_keys_snapshot(current_request_selected_db()));

        for key in keys {
            if let Some(value) = self
                .read_string_value(DbKeyRef::new(current_request_selected_db(), key.as_slice()))?
            {
                total_bytes = total_bytes.saturating_add(
                    u64::try_from(estimate_string_memory_usage_bytes(key.len(), value.len()))
                        .unwrap_or(u64::MAX),
                );
                continue;
            }

            if let Some(object) =
                self.object_read(DbKeyRef::new(current_request_selected_db(), key.as_slice()))?
            {
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

    fn maxmemory_policy_value(&self) -> Vec<u8> {
        self.config_items_snapshot()
            .into_iter()
            .find_map(|(parameter, value)| {
                if parameter == b"maxmemory-policy" {
                    Some(value)
                } else {
                    None
                }
            })
            .unwrap_or_else(|| b"noeviction".to_vec())
    }

    pub(crate) fn should_reject_mutating_command_for_maxmemory(
        &self,
        command_mutating: bool,
    ) -> Result<bool, RequestExecutionError> {
        if !command_mutating {
            return Ok(false);
        }
        let maxmemory_limit_bytes = self.maxmemory_limit_bytes();
        if maxmemory_limit_bytes == 0 {
            return Ok(false);
        }
        let maxmemory_policy = self.maxmemory_policy_value();
        if !maxmemory_policy.eq_ignore_ascii_case(b"noeviction") {
            return Ok(false);
        }
        let used_memory_bytes = self.estimated_used_memory_bytes()?;
        Ok(used_memory_bytes > maxmemory_limit_bytes)
    }

    pub(crate) fn evict_keys_to_fit_maxmemory(
        &self,
        maxmemory_limit_bytes: u64,
    ) -> Result<bool, RequestExecutionError> {
        if maxmemory_limit_bytes == 0 {
            return Ok(false);
        }
        let maxmemory_policy = self.maxmemory_policy_value();
        if maxmemory_policy.eq_ignore_ascii_case(b"noeviction") {
            return Ok(false);
        }

        let mut remaining_steps = 1024usize;
        let mut evicted_any = false;
        while remaining_steps > 0 {
            let used_memory_bytes = self.estimated_used_memory_bytes()?;
            if used_memory_bytes <= maxmemory_limit_bytes {
                break;
            }
            if !self.evict_single_key_for_maxmemory(&maxmemory_policy)? {
                break;
            }
            evicted_any = true;
            remaining_steps -= 1;
        }
        Ok(evicted_any)
    }

    pub(crate) fn evict_single_key_for_maxmemory(
        &self,
        maxmemory_policy: &[u8],
    ) -> Result<bool, RequestExecutionError> {
        let mut keys: Vec<RedisKey> = self.string_keys_snapshot(current_request_selected_db());
        keys.extend(self.object_keys_snapshot(current_request_selected_db()));
        if keys.is_empty() {
            return Ok(false);
        }

        let policy_is_volatile = maxmemory_policy.eq_ignore_ascii_case(b"volatile-lru")
            || maxmemory_policy.eq_ignore_ascii_case(b"volatile-lfu")
            || maxmemory_policy.eq_ignore_ascii_case(b"volatile-random")
            || maxmemory_policy.eq_ignore_ascii_case(b"volatile-ttl");
        if policy_is_volatile {
            let now = current_instant();
            keys.retain(|key| {
                self.string_expiration_deadline(DbKeyRef::new(
                    current_request_selected_db(),
                    key.as_slice(),
                ))
                .is_some_and(|deadline| deadline > now)
            });
            if keys.is_empty() {
                return Ok(false);
            }
        }

        if maxmemory_policy.eq_ignore_ascii_case(b"allkeys-lru")
            || maxmemory_policy.eq_ignore_ascii_case(b"volatile-lru")
            || maxmemory_policy.eq_ignore_ascii_case(b"allkeys-lfu")
            || maxmemory_policy.eq_ignore_ascii_case(b"volatile-lfu")
        {
            keys.sort_by_key(|key| {
                self.key_lru_millis(DbKeyRef::new(current_request_selected_db(), key.as_slice()))
                    .unwrap_or(0)
            });
        } else if maxmemory_policy.eq_ignore_ascii_case(b"volatile-ttl") {
            let now = current_instant();
            keys.sort_by_key(|key| {
                self.string_expiration_deadline(DbKeyRef::new(
                    current_request_selected_db(),
                    key.as_slice(),
                ))
                .map_or(u128::MAX, |deadline| {
                    deadline.saturating_duration_since(now).as_millis()
                })
            });
        }

        for key in keys {
            let shard_index = self.string_store_shard_index_for_key(key.as_slice());
            let mut string_deleted = false;
            {
                let mut store = self.lock_string_store_for_shard(shard_index);
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
                self.remove_string_key_metadata_in_shard(
                    DbKeyRef::new(current_request_selected_db(), key.as_slice()),
                    shard_index,
                );
            }
            let object_deleted =
                self.object_delete(DbKeyRef::new(current_request_selected_db(), key.as_slice()))?;
            if !string_deleted && !object_deleted {
                continue;
            }
            if string_deleted || object_deleted {
                self.bump_watch_version_server_origin(DbKeyRef::new(
                    current_request_selected_db(),
                    key.as_slice(),
                ));
            }
            self.notify_keyspace_event(
                current_request_selected_db(),
                NOTIFY_EVICTED,
                b"evicted",
                key.as_slice(),
            );
            return Ok(true);
        }
        Ok(false)
    }

    fn render_keysizes_info_payload(&self) -> Result<String, RequestExecutionError> {
        let mut payload = String::from("# Keysizes\r\n");
        let mut histograms =
            [[0u64; INFO_KEYSIZES_BIN_LABELS.len()]; KEYSIZES_HISTOGRAM_TYPE_COUNT];
        let mut histograms_by_db = BTreeMap::new();
        let main_runtime_db = self.logical_db_for_main_runtime()?;

        self.with_selected_db(main_runtime_db, || {
            let mut keys: HashSet<RedisKey> = self
                .string_keys_snapshot(current_request_selected_db())
                .into_iter()
                .collect();
            keys.extend(self.object_keys_snapshot(current_request_selected_db()));

            for key in keys {
                self.expire_key_if_needed(key.as_slice())?;
                self.active_expire_hash_fields_for_key(key.as_slice())?;

                if let Some(value) = self.read_string_value(DbKeyRef::new(
                    current_request_selected_db(),
                    key.as_slice(),
                ))? {
                    let length = super::string_commands::string_value_len_for_keysizes(
                        self,
                        value.as_slice(),
                    );
                    let bin_index = keysizes_histogram_bin_index(length);
                    histograms[0][bin_index] += 1;
                    continue;
                }

                let Some(object) =
                    self.object_read(DbKeyRef::new(current_request_selected_db(), key.as_slice()))?
                else {
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

            Ok::<(), RequestExecutionError>(())
        })?;

        histograms_by_db.insert(main_runtime_db, histograms);
        for (db_index, keys) in self.auxiliary_db_keys_snapshot() {
            let db_histograms = histograms_by_db
                .entry(db_index)
                .or_insert([[0u64; INFO_KEYSIZES_BIN_LABELS.len()]; KEYSIZES_HISTOGRAM_TYPE_COUNT]);
            for key in keys {
                self.with_selected_db(db_index, || self.expire_key_if_needed(key.as_slice()))?;
                self.with_selected_db(db_index, || {
                    self.active_expire_hash_fields_for_key(key.as_slice())
                })?;

                if let Some(value) =
                    self.read_string_value(DbKeyRef::new(db_index, key.as_slice()))?
                {
                    let length = super::string_commands::string_value_len_for_keysizes(
                        self,
                        value.as_slice(),
                    );
                    let bin_index = keysizes_histogram_bin_index(length);
                    db_histograms[0][bin_index] += 1;
                    continue;
                }

                let Some(object) = self.object_read(DbKeyRef::new(db_index, key.as_slice()))?
                else {
                    continue;
                };
                let Some(histogram_entry) =
                    keysizes_object_histogram_type_and_len(object.object_type, &object.payload)?
                else {
                    continue;
                };
                let bin_index = keysizes_histogram_bin_index(histogram_entry.length);
                db_histograms[histogram_entry.histogram_type_index][bin_index] += 1;
            }
        }

        for (db_index, db_histograms) in histograms_by_db {
            append_keysizes_histogram_line(
                &mut payload,
                usize::from(db_index),
                "distrib_strings_sizes",
                &db_histograms[0],
            );
            append_keysizes_histogram_line(
                &mut payload,
                usize::from(db_index),
                "distrib_lists_items",
                &db_histograms[1],
            );
            append_keysizes_histogram_line(
                &mut payload,
                usize::from(db_index),
                "distrib_sets_items",
                &db_histograms[2],
            );
            append_keysizes_histogram_line(
                &mut payload,
                usize::from(db_index),
                "distrib_zsets_items",
                &db_histograms[3],
            );
            append_keysizes_histogram_line(
                &mut payload,
                usize::from(db_index),
                "distrib_hashes_items",
                &db_histograms[4],
            );
        }

        Ok(payload)
    }

    fn debug_digest_value_for_key(&self, key: &[u8]) -> Result<Vec<u8>, RequestExecutionError> {
        if let Some(value) =
            self.read_string_value(DbKeyRef::new(current_request_selected_db(), key))?
        {
            return Ok(fnv_hex_digest(1, &value));
        }

        if let Some(object) = self.object_read(DbKeyRef::new(current_request_selected_db(), key))? {
            let encoded = object.encode();
            return Ok(fnv_hex_digest(2, encoded.as_slice()));
        }

        Ok(fnv_hex_digest(0, b""))
    }

    fn debug_dataset_digest(&self) -> Result<Vec<u8>, RequestExecutionError> {
        let mut keys: Vec<RedisKey> = self
            .string_keys_snapshot(current_request_selected_db())
            .into_iter()
            .collect();
        keys.extend(self.object_keys_snapshot(current_request_selected_db()));
        keys.sort();
        keys.dedup();

        let mut combined = Vec::new();
        for key in keys {
            self.expire_key_if_needed(key.as_slice())?;
            self.active_expire_hash_fields_for_key(key.as_slice())?;
            if !self.key_exists_any(DbKeyRef::new(current_request_selected_db(), key.as_slice()))? {
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
    if !options.replace
        && processor.key_exists_any(DbKeyRef::new(current_request_selected_db(), &key))?
    {
        return Err(RequestExecutionError::BusyKey);
    }
    let value = decode_dump_blob(dump_blob).ok_or(RequestExecutionError::InvalidDumpPayload)?;
    let restored_type = match &value {
        MigrationValue::String(_) => None,
        MigrationValue::Object { object_type, .. } => Some(*object_type),
    };
    let previous_string_exists =
        processor.key_exists(DbKeyRef::new(current_request_selected_db(), &key))?;
    let previous_type = if previous_string_exists {
        None
    } else {
        processor
            .object_read(DbKeyRef::new(current_request_selected_db(), &key))?
            .map(|object| object.object_type)
    };
    let had_previous_value = previous_string_exists || previous_type.is_some();

    if options.replace {
        processor
            .delete_string_key_for_migration(DbKeyRef::new(current_request_selected_db(), &key))?;
        let _ = processor.object_delete(DbKeyRef::new(current_request_selected_db(), &key))?;
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

    let restored = restore_migration_value(processor, &key, value, expiration_unix_millis)?;

    if restored {
        if let Some(idle_time_seconds) = options.idle_time_seconds {
            processor.set_key_idle_seconds(
                DbKeyRef::new(current_request_selected_db(), &key),
                idle_time_seconds,
            );
        } else {
            processor.record_key_access(DbKeyRef::new(current_request_selected_db(), &key), true);
        }
        if let Some(frequency) = options.frequency {
            processor.set_key_frequency(
                DbKeyRef::new(current_request_selected_db(), &key),
                frequency,
            );
        }
        processor.notify_keyspace_event(
            current_request_selected_db(),
            NOTIFY_GENERIC,
            b"restore",
            &key,
        );
        if had_previous_value {
            processor.notify_keyspace_event(
                current_request_selected_db(),
                NOTIFY_OVERWRITTEN,
                b"overwritten",
                &key,
            );
            if previous_type != restored_type {
                processor.notify_keyspace_event(
                    current_request_selected_db(),
                    NOTIFY_TYPE_CHANGED,
                    b"type_changed",
                    &key,
                );
            }
        }
    }

    append_simple_string(response_out, b"OK");
    Ok(())
}

fn restore_migration_value(
    processor: &RequestProcessor,
    key: &[u8],
    value: MigrationValue,
    expiration_unix_millis: Option<u64>,
) -> Result<bool, RequestExecutionError> {
    if should_materialize_restored_value(expiration_unix_millis)? == false {
        return Ok(false);
    }

    match value {
        MigrationValue::String(raw) => {
            processor.upsert_string_value_for_migration(
                DbKeyRef::new(current_request_selected_db(), key),
                raw.as_slice(),
                expiration_unix_millis,
            )?;
            let _ = processor.object_delete(DbKeyRef::new(current_request_selected_db(), key))?;
        }
        MigrationValue::Object {
            object_type,
            payload,
        } => {
            processor.delete_string_key_for_migration(DbKeyRef::new(
                current_request_selected_db(),
                key,
            ))?;
            processor.object_upsert(
                DbKeyRef::new(current_request_selected_db(), key),
                object_type,
                &payload,
            )?;
            processor.set_string_expiration_deadline(
                DbKeyRef::new(current_request_selected_db(), key),
                expiration_unix_millis.and_then(instant_from_unix_millis),
            );
        }
    }
    Ok(true)
}

fn should_materialize_restored_value(
    expiration_unix_millis: Option<u64>,
) -> Result<bool, RequestExecutionError> {
    let Some(expiration_unix_millis) = expiration_unix_millis else {
        return Ok(true);
    };
    let now_unix_millis =
        current_unix_time_millis().ok_or(RequestExecutionError::ValueOutOfRange)?;
    Ok(expiration_unix_millis > now_unix_millis)
}

#[derive(Debug, Clone)]
struct DebugReloadSnapshotEntry {
    key: Vec<u8>,
    expiration_unix_millis: Option<u64>,
    dump_blob: Vec<u8>,
    hash_field_expirations: Vec<DebugReloadHashFieldExpiration>,
}

#[derive(Debug, Clone)]
struct DebugReloadHashFieldExpiration {
    field: Vec<u8>,
    expiration_unix_millis: u64,
}

impl RequestProcessor {
    pub(crate) fn build_debug_reload_snapshot(
        &self,
        functions_only: bool,
    ) -> Result<Vec<u8>, RequestExecutionError> {
        let entries = if functions_only {
            Vec::new()
        } else {
            self.collect_debug_reload_snapshot_entries()?
        };
        let function_payload = self.dump_function_registry()?;
        Ok(encode_debug_reload_snapshot(&entries, &function_payload))
    }

    pub(crate) fn reload_debug_snapshot_source(&self) -> Result<bool, RequestExecutionError> {
        let snapshot_bytes = match self.read_debug_reload_snapshot_source()? {
            Some(snapshot_bytes) => snapshot_bytes,
            None => return Ok(false),
        };
        self.reload_debug_snapshot_bytes(snapshot_bytes)
    }

    pub(crate) fn reload_debug_snapshot_bytes(
        &self,
        snapshot_bytes: Vec<u8>,
    ) -> Result<bool, RequestExecutionError> {
        let Some(decoded) = decode_debug_reload_snapshot(&snapshot_bytes) else {
            return Ok(false);
        };

        let _ = self.flush_all_logical_databases()?;
        self.clear_function_registry(false);
        self.clear_all_forced_list_quicklist_encodings();

        for entry in decoded.entries {
            let value = decode_dump_blob(&entry.dump_blob)
                .ok_or(RequestExecutionError::InvalidDumpPayload)?;
            let restored = restore_migration_value(
                self,
                entry.key.as_slice(),
                value,
                entry.expiration_unix_millis,
            )?;
            if restored {
                let hash_field_expirations = entry
                    .hash_field_expirations
                    .iter()
                    .map(|expiration| {
                        (
                            HashField::from(expiration.field.as_slice()),
                            expiration.expiration_unix_millis,
                        )
                    })
                    .collect::<Vec<_>>();
                self.restore_hash_field_expirations_for_key(
                    DbKeyRef::new(current_request_selected_db(), entry.key.as_slice()),
                    &hash_field_expirations,
                );
            }
        }
        self.restore_function_registry_from_snapshot(&decoded.function_payload)?;
        self.set_saved_rdb_snapshot(snapshot_bytes);
        Ok(true)
    }

    fn collect_debug_reload_snapshot_entries(
        &self,
    ) -> Result<Vec<DebugReloadSnapshotEntry>, RequestExecutionError> {
        self.materialize_set_hot_entries_for_db(current_request_selected_db())?;
        let mut keys = std::collections::BTreeSet::new();
        keys.extend(self.string_keys_snapshot(current_request_selected_db()));
        keys.extend(self.object_keys_snapshot(current_request_selected_db()));

        let mut entries = Vec::with_capacity(keys.len());
        for key in keys {
            self.expire_key_if_needed(key.as_slice())?;

            if let Some(stored_value) = self
                .read_string_value(DbKeyRef::new(current_request_selected_db(), key.as_slice()))?
            {
                let expiration_unix_millis = self.expiration_unix_millis(DbKeyRef::new(
                    current_request_selected_db(),
                    key.as_slice(),
                ));
                let dump_blob = encode_dump_blob(MigrationValue::String(stored_value.into()));
                entries.push(DebugReloadSnapshotEntry {
                    key: key.to_vec(),
                    expiration_unix_millis,
                    dump_blob,
                    hash_field_expirations: Vec::new(),
                });
                continue;
            }

            if let Some(object) =
                self.object_read(DbKeyRef::new(current_request_selected_db(), key.as_slice()))?
            {
                let expiration_unix_millis = self.expiration_unix_millis(DbKeyRef::new(
                    current_request_selected_db(),
                    key.as_slice(),
                ));
                let hash_field_expirations = if object.object_type == HASH_OBJECT_TYPE_TAG {
                    self.snapshot_hash_field_expirations_for_key(DbKeyRef::new(
                        current_request_selected_db(),
                        key.as_slice(),
                    ))
                    .into_iter()
                    .map(
                        |(field, expiration_unix_millis)| DebugReloadHashFieldExpiration {
                            field: field.as_ref().to_vec(),
                            expiration_unix_millis,
                        },
                    )
                    .collect()
                } else {
                    Vec::new()
                };
                let dump_blob = encode_dump_blob(MigrationValue::Object {
                    object_type: object.object_type,
                    payload: object.payload,
                });
                entries.push(DebugReloadSnapshotEntry {
                    key: key.to_vec(),
                    expiration_unix_millis,
                    dump_blob,
                    hash_field_expirations,
                });
            }
        }
        Ok(entries)
    }

    fn read_debug_reload_snapshot_source(&self) -> Result<Option<Vec<u8>>, RequestExecutionError> {
        let snapshot_path = configured_dump_snapshot_path(&self.config_items_snapshot());
        match std::fs::read(&snapshot_path) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Ok(self.saved_rdb_snapshot())
            }
            Err(_) => Err(storage_failure(
                "debug.reload.snapshot",
                "failed to read configured dump snapshot",
            )),
        }
    }
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

#[derive(Debug)]
struct DecodedDebugReloadSnapshot {
    entries: Vec<DebugReloadSnapshotEntry>,
    function_payload: Vec<u8>,
}

fn encode_debug_reload_snapshot(
    entries: &[DebugReloadSnapshotEntry],
    function_payload: &[u8],
) -> Vec<u8> {
    let raw = encode_debug_reload_snapshot_body(entries, function_payload);
    let raw_len = u32::try_from(raw.len()).unwrap_or(u32::MAX);
    let compressed = encode_debug_reload_snapshot_zero_run(raw.as_slice());
    let (encoding, payload) = if compressed.len() < raw.len() {
        (DEBUG_RELOAD_SNAPSHOT_ENCODING_ZERO_RUN, compressed)
    } else {
        (DEBUG_RELOAD_SNAPSHOT_ENCODING_RAW, raw)
    };

    let mut encoded =
        Vec::with_capacity(DEBUG_RELOAD_SNAPSHOT_MAGIC_V3.len() + 1 + 4 + 4 + payload.len());
    encoded.extend_from_slice(DEBUG_RELOAD_SNAPSHOT_MAGIC_V3);
    encoded.push(encoding);
    encoded.extend_from_slice(&raw_len.to_le_bytes());
    append_snapshot_len_prefixed_bytes(&mut encoded, payload.as_slice());
    encoded
}

fn raw_len_for_snapshot_payload(
    entries: &[DebugReloadSnapshotEntry],
    function_payload: &[u8],
) -> usize {
    let mut total = 4usize;
    for entry in entries {
        let hash_field_expirations =
            encode_debug_reload_hash_field_expirations(&entry.hash_field_expirations);
        total = total
            .saturating_add(4)
            .saturating_add(entry.key.len())
            .saturating_add(8)
            .saturating_add(4)
            .saturating_add(entry.dump_blob.len())
            .saturating_add(4)
            .saturating_add(hash_field_expirations.len());
    }
    total
        .saturating_add(4)
        .saturating_add(function_payload.len())
}

fn encode_debug_reload_snapshot_body(
    entries: &[DebugReloadSnapshotEntry],
    function_payload: &[u8],
) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(raw_len_for_snapshot_payload(entries, function_payload));
    encoded.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for entry in entries {
        append_snapshot_len_prefixed_bytes(&mut encoded, entry.key.as_slice());
        encoded.extend_from_slice(
            &entry
                .expiration_unix_millis
                .unwrap_or(DEBUG_RELOAD_SNAPSHOT_NO_EXPIRATION)
                .to_le_bytes(),
        );
        append_snapshot_len_prefixed_bytes(&mut encoded, entry.dump_blob.as_slice());
        let hash_field_expirations =
            encode_debug_reload_hash_field_expirations(&entry.hash_field_expirations);
        append_snapshot_len_prefixed_bytes(&mut encoded, hash_field_expirations.as_slice());
    }
    append_snapshot_len_prefixed_bytes(&mut encoded, function_payload);
    encoded
}

fn decode_debug_reload_snapshot(encoded: &[u8]) -> Option<DecodedDebugReloadSnapshot> {
    if encoded.starts_with(DEBUG_RELOAD_SNAPSHOT_MAGIC_V3) {
        return decode_debug_reload_snapshot_v3(encoded);
    }
    if encoded.starts_with(DEBUG_RELOAD_SNAPSHOT_MAGIC_V2) {
        return decode_debug_reload_snapshot_v2(encoded);
    }
    if encoded.starts_with(DEBUG_RELOAD_SNAPSHOT_MAGIC_V1) {
        return decode_debug_reload_snapshot_v1(encoded);
    }
    None
}

fn decode_debug_reload_snapshot_v1(encoded: &[u8]) -> Option<DecodedDebugReloadSnapshot> {
    let body = encoded.get(DEBUG_RELOAD_SNAPSHOT_MAGIC_V1.len()..)?;
    decode_debug_reload_snapshot_body(body, false)
}

fn decode_debug_reload_snapshot_v2(encoded: &[u8]) -> Option<DecodedDebugReloadSnapshot> {
    let mut index = DEBUG_RELOAD_SNAPSHOT_MAGIC_V2.len();
    decode_debug_reload_snapshot_compressed_body(encoded, &mut index, false)
}

fn decode_debug_reload_snapshot_v3(encoded: &[u8]) -> Option<DecodedDebugReloadSnapshot> {
    let mut index = DEBUG_RELOAD_SNAPSHOT_MAGIC_V3.len();
    decode_debug_reload_snapshot_compressed_body(encoded, &mut index, true)
}

fn decode_debug_reload_snapshot_compressed_body(
    encoded: &[u8],
    index: &mut usize,
    includes_hash_field_expirations: bool,
) -> Option<DecodedDebugReloadSnapshot> {
    let encoding = *encoded.get(*index)?;
    *index += 1;
    let expected_raw_len =
        u32::from_le_bytes(encoded.get(*index..*index + 4)?.try_into().ok()?) as usize;
    *index += 4;
    let payload = read_snapshot_len_prefixed_bytes(encoded, index)?;
    if *index != encoded.len() {
        return None;
    }

    let raw = match encoding {
        DEBUG_RELOAD_SNAPSHOT_ENCODING_RAW => payload.to_vec(),
        DEBUG_RELOAD_SNAPSHOT_ENCODING_ZERO_RUN => {
            decode_debug_reload_snapshot_zero_run(payload, expected_raw_len)?
        }
        _ => return None,
    };
    if raw.len() != expected_raw_len {
        return None;
    }
    decode_debug_reload_snapshot_body(raw.as_slice(), includes_hash_field_expirations)
}

fn decode_debug_reload_snapshot_body(
    encoded: &[u8],
    includes_hash_field_expirations: bool,
) -> Option<DecodedDebugReloadSnapshot> {
    let mut index = 0usize;
    let entry_count = u32::from_le_bytes(encoded.get(index..index + 4)?.try_into().ok()?) as usize;
    index += 4;

    let mut entries = Vec::with_capacity(entry_count);
    for _ in 0..entry_count {
        let key = read_snapshot_len_prefixed_bytes(encoded, &mut index)?.to_vec();
        let expiration_raw = u64::from_le_bytes(encoded.get(index..index + 8)?.try_into().ok()?);
        index += 8;
        let dump_blob = read_snapshot_len_prefixed_bytes(encoded, &mut index)?.to_vec();
        let hash_field_expirations = if includes_hash_field_expirations {
            let encoded_hash_field_expirations =
                read_snapshot_len_prefixed_bytes(encoded, &mut index)?;
            decode_debug_reload_hash_field_expirations(encoded_hash_field_expirations)?
        } else {
            Vec::new()
        };
        let expiration_unix_millis = if expiration_raw == DEBUG_RELOAD_SNAPSHOT_NO_EXPIRATION {
            None
        } else {
            Some(expiration_raw)
        };
        entries.push(DebugReloadSnapshotEntry {
            key,
            expiration_unix_millis,
            dump_blob,
            hash_field_expirations,
        });
    }

    let function_payload = read_snapshot_len_prefixed_bytes(encoded, &mut index)?.to_vec();
    if index != encoded.len() {
        return None;
    }

    Some(DecodedDebugReloadSnapshot {
        entries,
        function_payload,
    })
}

fn encode_debug_reload_hash_field_expirations(
    hash_field_expirations: &[DebugReloadHashFieldExpiration],
) -> Vec<u8> {
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&(hash_field_expirations.len() as u32).to_le_bytes());
    for expiration in hash_field_expirations {
        append_snapshot_len_prefixed_bytes(&mut encoded, expiration.field.as_slice());
        encoded.extend_from_slice(&expiration.expiration_unix_millis.to_le_bytes());
    }
    encoded
}

fn decode_debug_reload_hash_field_expirations(
    encoded: &[u8],
) -> Option<Vec<DebugReloadHashFieldExpiration>> {
    let mut index = 0usize;
    let count = u32::from_le_bytes(encoded.get(index..index + 4)?.try_into().ok()?) as usize;
    index += 4;
    let mut hash_field_expirations = Vec::with_capacity(count);
    for _ in 0..count {
        let field = read_snapshot_len_prefixed_bytes(encoded, &mut index)?.to_vec();
        let expiration_unix_millis =
            u64::from_le_bytes(encoded.get(index..index + 8)?.try_into().ok()?);
        index += 8;
        hash_field_expirations.push(DebugReloadHashFieldExpiration {
            field,
            expiration_unix_millis,
        });
    }
    if index != encoded.len() {
        return None;
    }
    Some(hash_field_expirations)
}

fn append_snapshot_len_prefixed_bytes(target: &mut Vec<u8>, bytes: &[u8]) {
    let len = u32::try_from(bytes.len()).unwrap_or(u32::MAX);
    target.extend_from_slice(&len.to_le_bytes());
    target.extend_from_slice(bytes);
}

fn read_snapshot_len_prefixed_bytes<'a>(encoded: &'a [u8], index: &mut usize) -> Option<&'a [u8]> {
    let len = u32::from_le_bytes(encoded.get(*index..*index + 4)?.try_into().ok()?) as usize;
    *index += 4;
    let bytes = encoded.get(*index..*index + len)?;
    *index += len;
    Some(bytes)
}

fn encode_debug_reload_snapshot_zero_run(raw: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(raw.len());
    let mut index = 0usize;
    while index < raw.len() {
        let zero_run = count_snapshot_zero_run(raw, index);
        if zero_run >= 4 {
            let mut remaining = zero_run;
            while remaining > 0 {
                let chunk_len = remaining.min(127);
                encoded.push(0x80 | (chunk_len as u8));
                remaining -= chunk_len;
            }
            index += zero_run;
            continue;
        }

        let literal_start = index;
        let mut literal_len = 0usize;
        while index < raw.len() && literal_len < 127 {
            let next_zero_run = count_snapshot_zero_run(raw, index);
            if next_zero_run >= 4 {
                break;
            }
            let advance = next_zero_run.max(1);
            if literal_len + advance > 127 {
                break;
            }
            index += advance;
            literal_len += advance;
        }
        encoded.push(literal_len as u8);
        encoded.extend_from_slice(&raw[literal_start..literal_start + literal_len]);
    }
    encoded
}

fn decode_debug_reload_snapshot_zero_run(
    encoded: &[u8],
    expected_raw_len: usize,
) -> Option<Vec<u8>> {
    let mut decoded = Vec::with_capacity(expected_raw_len);
    let mut index = 0usize;
    while index < encoded.len() {
        let control = *encoded.get(index)?;
        index += 1;
        let chunk_len = usize::from(control & 0x7f);
        if chunk_len == 0 {
            return None;
        }
        if (control & 0x80) != 0 {
            decoded.resize(decoded.len().checked_add(chunk_len)?, 0);
            continue;
        }
        let literal = encoded.get(index..index + chunk_len)?;
        decoded.extend_from_slice(literal);
        index += chunk_len;
    }
    Some(decoded)
}

fn count_snapshot_zero_run(raw: &[u8], start: usize) -> usize {
    raw.get(start..)
        .unwrap_or_default()
        .iter()
        .take_while(|byte| **byte == 0)
        .count()
}

fn configured_dump_snapshot_path(config_items: &[(Vec<u8>, Vec<u8>)]) -> std::path::PathBuf {
    let mut dir = b".".to_vec();
    let mut dbfilename = b"dump.rdb".to_vec();
    for (key, value) in config_items {
        if key == b"dir" {
            dir = value.clone();
        } else if key == b"dbfilename" {
            dbfilename = value.clone();
        }
    }

    let mut path = std::path::PathBuf::from(String::from_utf8_lossy(&dir).into_owned());
    path.push(String::from_utf8_lossy(&dbfilename).into_owned());
    path
}

fn normalize_config_dir_value(value: &[u8]) -> Vec<u8> {
    let path = std::path::PathBuf::from(String::from_utf8_lossy(value).into_owned());
    let absolute = if path.is_absolute() {
        path
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| std::path::PathBuf::from("."))
            .join(path)
    };
    absolute.to_string_lossy().into_owned().into_bytes()
}

fn decode_dump_blob(encoded: &[u8]) -> Option<MigrationValue> {
    if !encoded.starts_with(DUMP_BLOB_MAGIC) {
        return decode_redis_dump_blob(encoded);
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

const REDIS_DUMP_FOOTER_LEN: usize = 10;
const REDIS_DUMP_MAX_RDB_VERSION: u16 = 13;
const REDIS_RDB_TYPE_STRING: u8 = 0;
const REDIS_RDB_TYPE_STREAM_LISTPACKS: u8 = 15;
const REDIS_RDB_TYPE_STREAM_LISTPACKS_2: u8 = 19;
const REDIS_RDB_TYPE_STREAM_LISTPACKS_3: u8 = 21;
const REDIS_RDB_ENC_INT8: u64 = 0;
const REDIS_RDB_ENC_INT16: u64 = 1;
const REDIS_RDB_ENC_INT32: u64 = 2;
const REDIS_RDB_ENC_LZF: u64 = 3;
const STREAM_ITEM_FLAG_DELETED: i64 = 1 << 0;
const STREAM_ITEM_FLAG_SAMEFIELDS: i64 = 1 << 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RedisRdbLen {
    Normal(u64),
    Encoded(u64),
}

struct RedisDumpReader<'a> {
    bytes: &'a [u8],
    cursor: usize,
}

impl<'a> RedisDumpReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, cursor: 0 }
    }

    fn is_exhausted(&self) -> bool {
        self.cursor == self.bytes.len()
    }

    fn read_u8(&mut self) -> Option<u8> {
        let byte = *self.bytes.get(self.cursor)?;
        self.cursor += 1;
        Some(byte)
    }

    fn read_exact(&mut self, len: usize) -> Option<&'a [u8]> {
        let end = self.cursor.checked_add(len)?;
        let bytes = self.bytes.get(self.cursor..end)?;
        self.cursor = end;
        Some(bytes)
    }

    fn read_len(&mut self) -> Option<RedisRdbLen> {
        let first = self.read_u8()?;
        match first >> 6 {
            0 => Some(RedisRdbLen::Normal(u64::from(first & 0x3f))),
            1 => {
                let second = self.read_u8()?;
                let len = (u64::from(first & 0x3f) << 8) | u64::from(second);
                Some(RedisRdbLen::Normal(len))
            }
            2 if first == 0x80 => {
                let mut raw = [0u8; 4];
                raw.copy_from_slice(self.read_exact(4)?);
                Some(RedisRdbLen::Normal(u64::from(u32::from_be_bytes(raw))))
            }
            2 if first == 0x81 => {
                let mut raw = [0u8; 8];
                raw.copy_from_slice(self.read_exact(8)?);
                Some(RedisRdbLen::Normal(u64::from_be_bytes(raw)))
            }
            3 => Some(RedisRdbLen::Encoded(u64::from(first & 0x3f))),
            _ => None,
        }
    }

    fn read_normal_len(&mut self) -> Option<u64> {
        match self.read_len()? {
            RedisRdbLen::Normal(len) => Some(len),
            RedisRdbLen::Encoded(_) => None,
        }
    }

    fn read_string(&mut self) -> Option<Vec<u8>> {
        match self.read_len()? {
            RedisRdbLen::Normal(len) => Some(self.read_exact(usize::try_from(len).ok()?)?.to_vec()),
            RedisRdbLen::Encoded(REDIS_RDB_ENC_INT8) => {
                let raw = self.read_u8()? as i8;
                Some(raw.to_string().into_bytes())
            }
            RedisRdbLen::Encoded(REDIS_RDB_ENC_INT16) => {
                let mut raw = [0u8; 2];
                raw.copy_from_slice(self.read_exact(2)?);
                Some(i16::from_le_bytes(raw).to_string().into_bytes())
            }
            RedisRdbLen::Encoded(REDIS_RDB_ENC_INT32) => {
                let mut raw = [0u8; 4];
                raw.copy_from_slice(self.read_exact(4)?);
                Some(i32::from_le_bytes(raw).to_string().into_bytes())
            }
            RedisRdbLen::Encoded(REDIS_RDB_ENC_LZF) => {
                let compressed_len = usize::try_from(self.read_normal_len()?).ok()?;
                let original_len = usize::try_from(self.read_normal_len()?).ok()?;
                let compressed = self.read_exact(compressed_len)?;
                redis_lzf_decompress(compressed, original_len)
            }
            RedisRdbLen::Encoded(_) => None,
        }
    }

    fn read_millis(&mut self) -> Option<u64> {
        let mut raw = [0u8; 8];
        raw.copy_from_slice(self.read_exact(8)?);
        Some(u64::from_le_bytes(raw))
    }

    fn read_stream_id_raw(&mut self) -> Option<StreamId> {
        let raw = self.read_exact(16)?;
        decode_redis_stream_raw_id(raw)
    }
}

fn decode_redis_dump_blob(encoded: &[u8]) -> Option<MigrationValue> {
    let payload = verify_and_strip_redis_dump_footer(encoded)?;
    let mut reader = RedisDumpReader::new(payload);
    let rdb_type = reader.read_u8()?;
    match rdb_type {
        REDIS_RDB_TYPE_STRING => {
            let value = reader.read_string()?;
            if !reader.is_exhausted() {
                return None;
            }
            Some(MigrationValue::String(value.into()))
        }
        REDIS_RDB_TYPE_STREAM_LISTPACKS
        | REDIS_RDB_TYPE_STREAM_LISTPACKS_2
        | REDIS_RDB_TYPE_STREAM_LISTPACKS_3 => {
            let mut stream = decode_redis_stream_dump_payload(&mut reader, rdb_type)?;
            stream.ensure_node_sizes(100);
            if !reader.is_exhausted() {
                return None;
            }
            Some(MigrationValue::Object {
                object_type: STREAM_OBJECT_TYPE_TAG,
                payload: serialize_stream_object_payload(&stream),
            })
        }
        _ => None,
    }
}

fn verify_and_strip_redis_dump_footer(encoded: &[u8]) -> Option<&[u8]> {
    if encoded.len() < REDIS_DUMP_FOOTER_LEN {
        return None;
    }
    let footer_start = encoded.len() - REDIS_DUMP_FOOTER_LEN;
    let version = u16::from_le_bytes([encoded[footer_start], encoded[footer_start + 1]]);
    if version > REDIS_DUMP_MAX_RDB_VERSION {
        return None;
    }

    let mut checksum_bytes = [0u8; 8];
    checksum_bytes.copy_from_slice(&encoded[footer_start + 2..]);
    let expected_checksum = u64::from_le_bytes(checksum_bytes);
    if expected_checksum != 0 {
        let actual_checksum = redis_crc64(&encoded[..encoded.len() - 8]);
        if actual_checksum != expected_checksum {
            return None;
        }
    }

    Some(&encoded[..footer_start])
}

fn redis_crc64(payload: &[u8]) -> u64 {
    const POLY: u64 = 0xad93_d235_94c9_35a9;

    let mut crc = 0u64;
    for &byte in payload {
        let mut mask = 1u8;
        while mask != 0 {
            let mut bit = (crc & (1u64 << 63)) != 0;
            if byte & mask != 0 {
                bit = !bit;
            }
            crc <<= 1;
            if bit {
                crc ^= POLY;
            }
            mask = mask.wrapping_shl(1);
        }
    }

    crc.reverse_bits()
}

fn redis_lzf_decompress(compressed: &[u8], expected_len: usize) -> Option<Vec<u8>> {
    let mut input_index = 0usize;
    let mut output = Vec::with_capacity(expected_len);

    while input_index < compressed.len() {
        let ctrl = *compressed.get(input_index)?;
        input_index += 1;

        if ctrl < (1 << 5) {
            let literal_len = usize::from(ctrl) + 1;
            let literal_end = input_index.checked_add(literal_len)?;
            output.extend_from_slice(compressed.get(input_index..literal_end)?);
            input_index = literal_end;
            continue;
        }

        let mut len = usize::from(ctrl >> 5);
        let ref_offset_high = usize::from(ctrl & 0x1f) << 8;
        if len == 7 {
            len = len.checked_add(usize::from(*compressed.get(input_index)?))?;
            input_index += 1;
        }
        let ref_offset_low = usize::from(*compressed.get(input_index)?);
        input_index += 1;

        let distance = ref_offset_high
            .checked_add(ref_offset_low)?
            .checked_add(1)?;
        let copy_len = len.checked_add(2)?;
        if distance > output.len() {
            return None;
        }
        let mut ref_index = output.len() - distance;
        for _ in 0..copy_len {
            let value = *output.get(ref_index)?;
            output.push(value);
            ref_index += 1;
        }
    }

    if output.len() != expected_len {
        return None;
    }
    Some(output)
}

fn decode_redis_stream_dump_payload(
    reader: &mut RedisDumpReader<'_>,
    rdb_type: u8,
) -> Option<StreamObject> {
    let listpack_count = usize::try_from(reader.read_normal_len()?).ok()?;
    let mut entries = BTreeMap::new();
    for _ in 0..listpack_count {
        let master_id = decode_redis_stream_raw_id(&reader.read_string()?)?;
        let listpack = reader.read_string()?;
        decode_redis_stream_listpack_entries(&listpack, master_id, &mut entries)?;
    }

    let stream_length = reader.read_normal_len()?;
    let last_generated_id = StreamId::new(reader.read_normal_len()?, reader.read_normal_len()?);
    let (max_deleted_entry_id, entries_added) = if rdb_type >= REDIS_RDB_TYPE_STREAM_LISTPACKS_2 {
        let _first_entry_id = StreamId::new(reader.read_normal_len()?, reader.read_normal_len()?);
        let max_deleted_entry_id =
            StreamId::new(reader.read_normal_len()?, reader.read_normal_len()?);
        (max_deleted_entry_id, reader.read_normal_len()?)
    } else {
        (StreamId::zero(), stream_length)
    };

    if usize::try_from(stream_length).ok()? != entries.len() {
        return None;
    }

    let group_count = usize::try_from(reader.read_normal_len()?).ok()?;
    let mut groups = BTreeMap::new();
    for _ in 0..group_count {
        let group_name = reader.read_string()?;
        let last_delivered_id = StreamId::new(reader.read_normal_len()?, reader.read_normal_len()?);
        let entries_read = if rdb_type >= REDIS_RDB_TYPE_STREAM_LISTPACKS_2 {
            Some(reader.read_normal_len()?.min(entries_added))
        } else {
            estimate_legacy_stream_entries_read(
                &entries,
                last_generated_id,
                max_deleted_entry_id,
                entries_added,
                last_delivered_id,
            )
        };

        let group_pending_count = usize::try_from(reader.read_normal_len()?).ok()?;
        let mut pending = BTreeMap::new();
        for _ in 0..group_pending_count {
            let pending_id = reader.read_stream_id_raw()?;
            let last_delivery_time_millis = reader.read_millis()?;
            let delivery_count = reader.read_normal_len()?;
            pending.insert(
                pending_id,
                StreamPendingEntry {
                    consumer: Vec::new(),
                    last_delivery_time_millis,
                    delivery_count,
                },
            );
        }

        let consumer_count = usize::try_from(reader.read_normal_len()?).ok()?;
        let mut consumers = BTreeMap::new();
        for _ in 0..consumer_count {
            let consumer_name = reader.read_string()?;
            let seen_time_millis = reader.read_millis()?;
            let active_time_millis = if rdb_type >= REDIS_RDB_TYPE_STREAM_LISTPACKS_3 {
                Some(reader.read_millis()?)
            } else {
                Some(seen_time_millis)
            };

            let consumer_pending_count = usize::try_from(reader.read_normal_len()?).ok()?;
            let mut consumer_state = StreamConsumerState {
                pending: BTreeSet::new(),
                seen_time_millis,
                active_time_millis,
            };
            for _ in 0..consumer_pending_count {
                let pending_id = reader.read_stream_id_raw()?;
                let pending_entry = pending.get_mut(&pending_id)?;
                pending_entry.consumer = consumer_name.clone();
                consumer_state.pending.insert(pending_id);
            }

            if consumers.insert(consumer_name, consumer_state).is_some() {
                return None;
            }
        }

        if pending.values().any(|entry| entry.consumer.is_empty()) {
            return None;
        }

        groups.insert(
            group_name,
            StreamGroupState {
                last_delivered_id,
                entries_read,
                consumers,
                pending,
            },
        );
    }

    Some(StreamObject {
        entries,
        node_sizes: VecDeque::new(),
        groups,
        last_generated_id,
        max_deleted_entry_id,
        entries_added,
        idmp_duration_seconds: super::DEFAULT_STREAM_IDMP_DURATION_SECONDS,
        idmp_maxsize: super::DEFAULT_STREAM_IDMP_MAXSIZE,
        idmp_producers: BTreeMap::new(),
        iids_added: 0,
        iids_duplicates: 0,
    })
}

fn estimate_legacy_stream_entries_read(
    entries: &BTreeMap<StreamId, Vec<(Vec<u8>, Vec<u8>)>>,
    last_generated_id: StreamId,
    max_deleted_entry_id: StreamId,
    entries_added: u64,
    last_delivered_id: StreamId,
) -> Option<u64> {
    if entries_added == 0 {
        return Some(0);
    }

    let length = u64::try_from(entries.len()).ok()?;
    if length == 0 && last_delivered_id <= last_generated_id {
        return Some(entries_added);
    }

    if last_delivered_id != StreamId::zero() && last_delivered_id < max_deleted_entry_id {
        return None;
    }

    match last_delivered_id.cmp(&last_generated_id) {
        core::cmp::Ordering::Equal => return Some(entries_added),
        core::cmp::Ordering::Greater => return None,
        core::cmp::Ordering::Less => {}
    }

    let first_entry_id = *entries.keys().next()?;
    if max_deleted_entry_id == StreamId::zero() || max_deleted_entry_id < first_entry_id {
        if last_delivered_id < first_entry_id {
            return Some(entries_added.saturating_sub(length));
        }
        if last_delivered_id == first_entry_id {
            return Some(entries_added.saturating_sub(length).saturating_add(1));
        }
    }

    None
}

fn decode_redis_stream_raw_id(raw: &[u8]) -> Option<StreamId> {
    if raw.len() != 16 {
        return None;
    }
    let mut millis = [0u8; 8];
    millis.copy_from_slice(&raw[..8]);
    let mut sequence = [0u8; 8];
    sequence.copy_from_slice(&raw[8..]);
    Some(StreamId::new(
        u64::from_be_bytes(millis),
        u64::from_be_bytes(sequence),
    ))
}

fn decode_redis_stream_listpack_entries(
    listpack: &[u8],
    master_id: StreamId,
    entries_out: &mut BTreeMap<StreamId, Vec<(Vec<u8>, Vec<u8>)>>,
) -> Option<()> {
    if listpack.len() < 7 {
        return None;
    }
    let total_bytes = u32::from_le_bytes(listpack.get(0..4)?.try_into().ok()?) as usize;
    if total_bytes != listpack.len() || *listpack.last()? != 0xff {
        return None;
    }

    let mut offset = 6usize;
    let entry_count = usize::try_from(redis_listpack_parse_i64(listpack, &mut offset)?).ok()?;
    let deleted_count = usize::try_from(redis_listpack_parse_i64(listpack, &mut offset)?).ok()?;
    let master_field_count =
        usize::try_from(redis_listpack_parse_i64(listpack, &mut offset)?).ok()?;
    let mut master_fields = Vec::with_capacity(master_field_count);
    for _ in 0..master_field_count {
        master_fields.push(redis_listpack_parse_bytes(listpack, &mut offset)?);
    }
    if redis_listpack_parse_i64(listpack, &mut offset)? != 0 {
        return None;
    }

    let total_entries = entry_count.checked_add(deleted_count)?;
    for _ in 0..total_entries {
        let flags = redis_listpack_parse_i64(listpack, &mut offset)?;
        let ms_delta = u64::try_from(redis_listpack_parse_i64(listpack, &mut offset)?).ok()?;
        let seq_delta = u64::try_from(redis_listpack_parse_i64(listpack, &mut offset)?).ok()?;
        let entry_id = StreamId::new(
            master_id.timestamp_millis().checked_add(ms_delta)?,
            master_id.sequence().checked_add(seq_delta)?,
        );

        let same_fields = (flags & STREAM_ITEM_FLAG_SAMEFIELDS) != 0;
        let field_names = if same_fields {
            master_fields.clone()
        } else {
            let field_count =
                usize::try_from(redis_listpack_parse_i64(listpack, &mut offset)?).ok()?;
            let mut field_names = Vec::with_capacity(field_count);
            for _ in 0..field_count {
                field_names.push(redis_listpack_parse_bytes(listpack, &mut offset)?);
            }
            field_names
        };

        let mut field_pairs = Vec::with_capacity(field_names.len());
        for field_name in field_names {
            let value = redis_listpack_parse_bytes(listpack, &mut offset)?;
            field_pairs.push((field_name, value));
        }

        let expected_piece_count = if same_fields {
            field_pairs.len() + 3
        } else {
            field_pairs.len() * 2 + 4
        };
        if redis_listpack_parse_i64(listpack, &mut offset)? != expected_piece_count as i64 {
            return None;
        }

        if (flags & STREAM_ITEM_FLAG_DELETED) != 0 {
            continue;
        }
        if entries_out.insert(entry_id, field_pairs).is_some() {
            return None;
        }
    }

    if offset != listpack.len() - 1 {
        return None;
    }
    Some(())
}

fn redis_listpack_parse_i64(listpack: &[u8], offset: &mut usize) -> Option<i64> {
    parse_i64_ascii(&redis_listpack_parse_bytes(listpack, offset)?)
}

fn redis_listpack_parse_bytes(listpack: &[u8], offset: &mut usize) -> Option<Vec<u8>> {
    let start = *offset;
    let entry = listpack.get(start..)?;
    let first = *entry.first()?;

    let (payload, encoded_size) = if first & 0x80 == 0 {
        (i64::from(first & 0x7f).to_string().into_bytes(), 1usize)
    } else if first & 0xc0 == 0x80 {
        let len = usize::from(first & 0x3f);
        let payload_start = start.checked_add(1)?;
        let payload_end = payload_start.checked_add(len)?;
        (listpack.get(payload_start..payload_end)?.to_vec(), 1 + len)
    } else if first & 0xe0 == 0xc0 {
        let second = *listpack.get(start + 1)?;
        let mut value = i64::from((i16::from(first & 0x1f) << 8) | i16::from(second));
        if value >= (1 << 12) {
            value -= 1 << 13;
        }
        (value.to_string().into_bytes(), 2)
    } else if first == 0xf1 {
        let mut raw = [0u8; 2];
        raw.copy_from_slice(listpack.get(start + 1..start + 3)?);
        (i16::from_le_bytes(raw).to_string().into_bytes(), 3)
    } else if first == 0xf2 {
        let bytes = listpack.get(start + 1..start + 4)?;
        let mut value =
            i64::from(bytes[0]) | (i64::from(bytes[1]) << 8) | (i64::from(bytes[2]) << 16);
        if value >= (1 << 23) {
            value -= 1 << 24;
        }
        (value.to_string().into_bytes(), 4)
    } else if first == 0xf3 {
        let mut raw = [0u8; 4];
        raw.copy_from_slice(listpack.get(start + 1..start + 5)?);
        (i32::from_le_bytes(raw).to_string().into_bytes(), 5)
    } else if first == 0xf4 {
        let mut raw = [0u8; 8];
        raw.copy_from_slice(listpack.get(start + 1..start + 9)?);
        (i64::from_le_bytes(raw).to_string().into_bytes(), 9)
    } else if first & 0xf0 == 0xe0 {
        let second = *listpack.get(start + 1)?;
        let len = usize::from((u16::from(first & 0x0f) << 8) | u16::from(second));
        let payload_start = start.checked_add(2)?;
        let payload_end = payload_start.checked_add(len)?;
        (listpack.get(payload_start..payload_end)?.to_vec(), 2 + len)
    } else if first == 0xf0 {
        let mut raw = [0u8; 4];
        raw.copy_from_slice(listpack.get(start + 1..start + 5)?);
        let len = u32::from_le_bytes(raw) as usize;
        let payload_start = start.checked_add(5)?;
        let payload_end = payload_start.checked_add(len)?;
        (listpack.get(payload_start..payload_end)?.to_vec(), 5 + len)
    } else {
        return None;
    };

    let expected_backlen = redis_listpack_encode_backlen(encoded_size);
    let backlen_start = start.checked_add(encoded_size)?;
    let backlen_end = backlen_start.checked_add(expected_backlen.len())?;
    if listpack.get(backlen_start..backlen_end)? != expected_backlen.as_slice() {
        return None;
    }
    *offset = backlen_end;
    Some(payload)
}

fn redis_listpack_encode_backlen(entry_len: usize) -> Vec<u8> {
    let mut value = u64::try_from(entry_len).unwrap_or(u64::MAX);
    if value <= 127 {
        return vec![value as u8];
    }

    let mut bytes = Vec::new();
    while value > 0 {
        let chunk = (value & 0x7f) as u8;
        bytes.push(chunk | 0x80);
        value >>= 7;
    }
    if let Some(last) = bytes.last_mut() {
        *last &= 0x7f;
    }
    bytes.reverse();
    bytes
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
        append_array_length(response_out, 3);
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

fn string_encoding_name(value: &[u8], force_raw: bool) -> &'static [u8] {
    if force_raw {
        return b"raw";
    }
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
    hash_listpack_max_entries: usize,
    hash_listpack_max_value: usize,
    set_listpack_max_entries: usize,
    set_listpack_max_value: usize,
    set_intset_max_entries: usize,
    set_encoding_floor: Option<SetEncodingFloor>,
    hash_has_field_expirations: bool,
) -> Result<&'static [u8], RequestExecutionError> {
    const SMALL_ELEMENT_BYTES: usize = 64;

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
            let intset = set.iter().all(|member| parse_i64_ascii(member).is_some())
                && set.len() <= set_intset_max_entries;
            let compact = set.len() <= set_listpack_max_entries
                && set
                    .iter()
                    .all(|member| member.len() <= set_listpack_max_value);
            let inferred: &'static [u8] = if intset {
                b"intset"
            } else if compact {
                b"listpack"
            } else {
                b"hashtable"
            };
            if inferred == b"intset" && set_encoding_floor == Some(SetEncodingFloor::Listpack) {
                return Ok(b"listpack");
            }
            if (inferred == b"intset" || inferred == b"listpack")
                && set_encoding_floor == Some(SetEncodingFloor::Hashtable)
            {
                return Ok(b"hashtable");
            }
            Ok(inferred)
        }
        HASH_OBJECT_TYPE_TAG => {
            let hash = deserialize_hash_object_payload(payload).ok_or_else(|| {
                storage_failure("object.encoding", "failed to decode hash payload")
            })?;
            let compact = hash.len() <= hash_listpack_max_entries
                && hash.iter().all(|(field, value)| {
                    field.len() <= hash_listpack_max_value && value.len() <= hash_listpack_max_value
                });
            if compact {
                if hash_has_field_expirations {
                    Ok(b"listpackex")
                } else {
                    Ok(b"listpack")
                }
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

fn format_set_debug_ht_stats_payload(
    snapshot: SetDebugHtStatsSnapshot,
    member_count: usize,
    full: bool,
) -> Vec<u8> {
    let mut payload = String::new();
    append_set_debug_ht_stats_section(
        &mut payload,
        0,
        "main hash table",
        snapshot.main_table_size,
        member_count,
        full,
    );
    if let Some(target_size) = snapshot.rehash_target_size {
        append_set_debug_ht_stats_section(
            &mut payload,
            1,
            "rehashing target",
            target_size,
            member_count,
            full,
        );
    }
    payload.into_bytes()
}

fn append_set_debug_ht_stats_section(
    payload: &mut String,
    table_index: usize,
    table_name: &str,
    table_size: usize,
    member_count: usize,
    full: bool,
) {
    if member_count == 0 {
        payload.push_str(&format!(
            "Hash table {table_index} stats ({table_name}):\nNo stats available for empty dictionaries\n"
        ));
        return;
    }

    payload.push_str(&format!(
        "Hash table {table_index} stats ({table_name}):\n table size: {table_size}\n number of elements: {member_count}\n"
    ));

    if !full {
        return;
    }

    let different_slots = 1usize;
    let max_chain_length = member_count;
    let average_chain = member_count as f64;
    let empty_slots = table_size.saturating_sub(different_slots);

    payload.push_str(&format!(
        " different slots: {different_slots}\n max chain length: {max_chain_length}\n avg chain length (counted): {average_chain:.02}\n avg chain length (computed): {average_chain:.02}\n Chain length distribution:\n"
    ));
    if empty_slots > 0 {
        payload.push_str(&format!(
            "   0: {empty_slots} ({:.02}%)\n",
            (empty_slots as f64 * 100.0) / table_size as f64
        ));
    }
    payload.push_str(&format!(
        "   {max_chain_length}: 1 ({:.02}%)\n",
        100.0 / table_size as f64
    ));
}

// list_listpack_compatible lives in object_store.rs as the single source of truth.

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

fn estimate_string_memory_usage_bytes(key_len: usize, value_len: usize) -> i64 {
    let string_header_bytes = if cfg!(target_pointer_width = "32") {
        12usize
    } else {
        16usize
    };
    let total = key_len
        .saturating_add(value_len)
        .saturating_add(string_header_bytes);
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

    append_array_length(response_out, 2);
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
    append_array_length(response_out, 3);
    append_bulk_string(response_out, name);
    append_integer(response_out, arity);
    append_bulk_array(response_out, flags);
}

fn append_command_getkeysandflags(
    response_out: &mut Vec<u8>,
    entries: &[(Vec<u8>, &'static [&'static [u8]])],
) {
    append_array_length(response_out, entries.len());
    for (key, flags) in entries {
        append_array_length(response_out, 2);
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

fn parse_db_name_arg(value: &[u8]) -> Result<DbName, RequestExecutionError> {
    let index = parse_i64_ascii(value).ok_or(RequestExecutionError::ValueNotInteger)?;
    if index < 0 {
        return Err(RequestExecutionError::DbIndexOutOfRange);
    }
    let index = usize::try_from(index).map_err(|_| RequestExecutionError::DbIndexOutOfRange)?;
    Ok(DbName::new(index))
}

fn parse_debug_populate_number(value: &[u8]) -> Result<u64, RequestExecutionError> {
    let parsed = parse_i64_ascii(value).ok_or(RequestExecutionError::ValueNotInteger)?;
    if parsed < 0 {
        return Err(RequestExecutionError::ValueOutOfRangePositive);
    }
    u64::try_from(parsed).map_err(|_| RequestExecutionError::ValueOutOfRangePositive)
}

fn encode_debug_populate_key_name(prefix: &[u8], index: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(prefix.len() + 24);
    key.extend_from_slice(prefix);
    key.push(b':');
    key.extend_from_slice(index.to_string().as_bytes());
    key
}

fn encode_debug_populate_value(index: u64, value_size: usize) -> Vec<u8> {
    let default_value = format!("value:{index}").into_bytes();
    if value_size == 0 {
        return default_value;
    }

    let mut value = vec![0u8; value_size];
    let copy_len = default_value.len().min(value.len());
    value[..copy_len].copy_from_slice(&default_value[..copy_len]);
    value
}

fn split_client_addr_for_replication(addr: &[u8]) -> (String, String) {
    let text = String::from_utf8_lossy(addr);
    if let Some((ip, port)) = text.rsplit_once(':') {
        return (ip.to_string(), port.to_string());
    }
    (text.into_owned(), "0".to_string())
}

fn parse_configured_db_name_arg(
    value: &[u8],
    configured_databases: usize,
) -> Result<DbName, RequestExecutionError> {
    let db_name = parse_db_name_arg(value)?;
    if usize::from(db_name) >= configured_databases {
        return Err(RequestExecutionError::DbIndexOutOfRange);
    }
    Ok(db_name)
}

fn parse_swapdb_db_name_arg(
    value: &[u8],
    invalid_error: RequestExecutionError,
) -> Result<DbName, RequestExecutionError> {
    let Some(index) = parse_i64_ascii(value) else {
        return Err(invalid_error);
    };
    if index < 0 {
        return Err(invalid_error);
    }
    let index = usize::try_from(index).map_err(|_| invalid_error)?;
    Ok(DbName::new(index))
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

fn format_human_bytes(bytes: u64) -> String {
    if bytes >= 1024 * 1024 * 1024 {
        format!("{:.2}G", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    } else if bytes >= 1024 * 1024 {
        format!("{:.2}M", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.2}K", bytes as f64 / 1024.0)
    } else {
        format!("{bytes}B")
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
