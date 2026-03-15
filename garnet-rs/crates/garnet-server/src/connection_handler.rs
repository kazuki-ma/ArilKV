// TLA+ model linkage:
// - formal/tla/specs/PipelineStresserTimeout.tla
// - formal/tla/specs/BlockingDisconnectLeak.tla
// - formal/tla/specs/BlockingWaitClassIsolation.tla
// - formal/tla/specs/BlockingStreamAckGate.tla
// - formal/tla/specs/BlockingCountVisibility.tla
// - formal/tla/specs/LinkedBlmoveChainResidue.tla
// - formal/tla/specs/ClientPauseUnblockRace.tla
// - formal/tla/specs/WaitAckProgress.tla
use std::borrow::Cow;
use std::io;
use std::sync::Arc;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use garnet_cluster::ClusterConfigStore;
use garnet_common::ArgSlice;
use garnet_common::RespParseError;
use garnet_common::parse_resp_command_arg_slices_dynamic;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use tokio::task::yield_now;
use tokio::time::sleep;
use tsavorite::AofOffset;

use crate::AclUserProfile;
use crate::ClientId;
use crate::ClientKillFilter;
use crate::ClientTypeFilter;
use crate::RequestExecutionError;
use crate::RequestProcessor;
use crate::ServerMetrics;
use crate::ShardOwnerThreadPool;
use crate::command_dispatch::dispatch_from_arg_slices;
use crate::command_spec::CommandId;
use crate::command_spec::TransactionControlCommand;
use crate::command_spec::command_has_valid_arity;
use crate::command_spec::command_is_effectively_mutating;
use crate::command_spec::command_is_mutating;
use crate::command_spec::command_is_scripting_family;
use crate::command_spec::command_is_write_pause_affected_with_script;
use crate::command_spec::command_transaction_control;
use crate::command_spec::eval_script_shebang_flags;
use crate::connection_owner_routing::OwnedExecutionOutcome;
use crate::connection_owner_routing::OwnerThreadExecutionError;
use crate::connection_owner_routing::RoutedExecutionError;
use crate::connection_owner_routing::capture_owned_frame_args;
use crate::connection_owner_routing::execute_frame_on_owner_thread;
use crate::connection_owner_routing::execute_owned_frame_args_via_processor;
use crate::connection_protocol::append_error_line;
use crate::connection_protocol::append_simple_string;
use crate::connection_protocol::append_wrong_arity_error_for_command;
use crate::connection_protocol::ascii_eq_ignore_case;
use crate::connection_protocol::parse_u16_ascii;
use crate::connection_routing::cluster_error_for_command;
use crate::connection_routing::command_hash_slot_for_transaction;
use crate::connection_transaction::ConnectionTransactionState;
use crate::connection_transaction::QueuedReplicationTransition;
use crate::connection_transaction::TransactionExecutionOutcome;
use crate::connection_transaction::execute_transaction_queue;
use crate::redis_replication::RedisReplicationCoordinator;
use crate::request_lifecycle::BlockingWaitClass;
use crate::request_lifecycle::BlockingWaitKey;
use crate::request_lifecycle::ClientTrackingConfig;
use crate::request_lifecycle::ClientTrackingModeSetting;
use crate::request_lifecycle::ClientUnblockMode;
use crate::request_lifecycle::DbKeyRef;
use crate::request_lifecycle::DbName;
use crate::request_lifecycle::RedisKey;
use crate::request_lifecycle::RequestConnectionEffects;
use crate::request_lifecycle::RespProtocolVersion;
use crate::request_lifecycle::WatchedKey;

const DEFAULT_OWNER_THREAD_COUNT: usize = 1;
const DEFAULT_RESP_ARG_SCRATCH: usize = 64;
const DEFAULT_MAX_RESP_ARGUMENTS: usize = 1_048_576;
const GARNET_STRING_OWNER_THREADS_ENV: &str = "GARNET_STRING_OWNER_THREADS";
const GARNET_OWNER_EXECUTION_INLINE_ENV: &str = "GARNET_OWNER_EXECUTION_INLINE";
const GARNET_MAX_RESP_ARGUMENTS_ENV: &str = "GARNET_MAX_RESP_ARGUMENTS";
const BUSY_SCRIPT_RESPONSE_PREFIX: &[u8] =
    b"-BUSY Redis is busy running a script. You can only call FUNCTION KILL or SCRIPT KILL.\r\n";
const BLOCKING_COMMAND_NON_TURN_POLL_INTERVAL: Duration = Duration::from_millis(1);
const BLOCKING_STREAM_EMPTY_POLL_INTERVAL: Duration = Duration::from_millis(10);
const KILLED_CLIENT_POLL_INTERVAL: Duration = Duration::from_millis(25);
const SCRIPT_BUSY_AFTER_UNPAUSE_RETRY_WINDOW: Duration = Duration::from_millis(500);
const SCRIPT_BUSY_AFTER_UNPAUSE_RETRY_POLL: Duration = Duration::from_millis(1);
const CLIENT_HELP_LINES: [&[u8]; 37] = [
    b"CLIENT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
    b"CACHING (YES|NO)",
    b"    Enable/disable tracking of the keys for next command in OPTIN/OPTOUT modes.",
    b"GETNAME",
    b"    Return the name of the current connection.",
    b"GETREDIR",
    b"    Return the redirect client ID or -1 if client tracking is off.",
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
    b"PAUSE <timeout> [WRITE|ALL]",
    b"    Suspend all, or just write, clients, for <timeout> milliseconds.",
    b"REPLY (ON|OFF|SKIP)",
    b"    Control the replies sent to the current connection.",
    b"SETINFO <LIB-NAME|LIB-VER> <value>",
    b"    Set client library name or version for this connection.",
    b"SETNAME <name>",
    b"    Set the current connection name.",
    b"TRACKING (ON|OFF) [REDIRECT <id>] [BCAST] [PREFIX <prefix> [...]] [OPTIN] [OPTOUT] [NOLOOP]",
    b"    Enable/disable server-assisted client-side caching.",
    b"TRACKINGINFO",
    b"    Return the tracking status and redirect client id for the current connection.",
    b"UNBLOCK <clientid> [TIMEOUT|ERROR]",
    b"    Unblock a client blocked by a blocking command.",
    b"UNPAUSE",
    b"    Resume clients after a CLIENT PAUSE.",
];
const OWNER_EXECUTION_INLINE_DEFAULT_UNSET: u8 = 0;
const OWNER_EXECUTION_INLINE_DEFAULT_FALSE: u8 = 1;
const OWNER_EXECUTION_INLINE_DEFAULT_TRUE: u8 = 2;
static OWNER_EXECUTION_INLINE_DEFAULT: AtomicU8 =
    AtomicU8::new(OWNER_EXECUTION_INLINE_DEFAULT_UNSET);

enum ConnectionReadWake {
    BytesRead(usize),
    PendingPubsub,
    KilledClientPoll,
}

pub fn set_owner_execution_inline_default(enabled: bool) {
    let encoded = if enabled {
        OWNER_EXECUTION_INLINE_DEFAULT_TRUE
    } else {
        OWNER_EXECUTION_INLINE_DEFAULT_FALSE
    };
    OWNER_EXECUTION_INLINE_DEFAULT.store(encoded, Ordering::Relaxed);
}

fn owner_execution_inline_default() -> Option<bool> {
    match OWNER_EXECUTION_INLINE_DEFAULT.load(Ordering::Relaxed) {
        OWNER_EXECUTION_INLINE_DEFAULT_FALSE => Some(false),
        OWNER_EXECUTION_INLINE_DEFAULT_TRUE => Some(true),
        _ => None,
    }
}

#[inline]
fn arg_slice_bytes(arg: &ArgSlice) -> &[u8] {
    // SAFETY: connection handler consumes ArgSlice values created from the
    // current connection frame buffer, which remains alive while dispatching
    // this frame.
    unsafe { arg.as_slice() }
}

fn parse_i64_ascii_bytes(value: &[u8]) -> Option<i64> {
    let text = std::str::from_utf8(value).ok()?;
    text.parse::<i64>().ok()
}

fn parse_resp_single_integer_array(frame: &[u8]) -> Option<i64> {
    if !frame.starts_with(b"*1\r\n:") || !frame.ends_with(b"\r\n") {
        return None;
    }
    parse_i64_ascii_bytes(&frame[5..frame.len().saturating_sub(2)])
}

fn parse_resp_length_line(buffer: &[u8], prefix: u8) -> Option<(i64, usize)> {
    if buffer.first().copied()? != prefix {
        return None;
    }
    let mut cursor = 1usize;
    while cursor + 1 < buffer.len() {
        if buffer[cursor] == b'\r' && buffer[cursor + 1] == b'\n' {
            let value = parse_i64_ascii_bytes(&buffer[1..cursor])?;
            return Some((value, cursor + 2));
        }
        cursor += 1;
    }
    None
}

fn logical_query_buffer_bytes(buffer: &[u8]) -> usize {
    let mut estimated = buffer.len();
    let Some((array_len, mut cursor)) = parse_resp_length_line(buffer, b'*') else {
        return estimated;
    };
    if array_len <= 0 {
        return estimated;
    }
    let array_len = usize::try_from(array_len).ok().unwrap_or(usize::MAX);
    for _ in 0..array_len {
        if cursor >= buffer.len() {
            return estimated;
        }
        let Some((bulk_len, bulk_header_len)) = parse_resp_length_line(&buffer[cursor..], b'$')
        else {
            return estimated;
        };
        if bulk_len < 0 {
            return estimated;
        }
        let bulk_len = usize::try_from(bulk_len).ok().unwrap_or(usize::MAX);
        let total_bulk_bytes = bulk_header_len
            .checked_add(bulk_len)
            .and_then(|value| value.checked_add(2))
            .unwrap_or(usize::MAX);
        estimated = estimated.max(cursor.saturating_add(total_bulk_bytes));
        cursor = cursor.saturating_add(total_bulk_bytes);
    }
    estimated
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BitfieldSign {
    Signed,
    Unsigned,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct BitfieldEncodingSpec {
    sign: BitfieldSign,
    bits: usize,
}

fn parse_bitfield_encoding_for_replication(token: &[u8]) -> Option<BitfieldEncodingSpec> {
    if token.len() < 2 {
        return None;
    }
    let sign = match token[0].to_ascii_uppercase() {
        b'I' => BitfieldSign::Signed,
        b'U' => BitfieldSign::Unsigned,
        _ => return None,
    };
    let bits = usize::try_from(parse_i64_ascii_bytes(&token[1..])?).ok()?;
    let valid = match sign {
        BitfieldSign::Signed => (1..=64).contains(&bits),
        BitfieldSign::Unsigned => (1..=63).contains(&bits),
    };
    valid.then_some(BitfieldEncodingSpec { sign, bits })
}

fn parse_bitfield_offset_for_replication(token: &[u8], bits: usize) -> Option<usize> {
    let (type_relative, raw) = match token.split_first() {
        Some((b'#', rest)) => (true, rest),
        _ => (false, token),
    };
    let offset = parse_i64_ascii_bytes(raw)?;
    if offset < 0 {
        return None;
    }
    let offset = usize::try_from(offset).ok()?;
    if type_relative {
        offset.checked_mul(bits)
    } else {
        Some(offset)
    }
}

fn bitfield_wrap_set_value_for_replication(
    value: i64,
    sign: BitfieldSign,
    bits: usize,
) -> Option<i64> {
    let modulus = 1i128.checked_shl(bits as u32)?;
    let mut raw = i128::from(value) % modulus;
    if raw < 0 {
        raw += modulus;
    }
    if matches!(sign, BitfieldSign::Signed) {
        let sign_bit = 1i128.checked_shl((bits.checked_sub(1)?) as u32)?;
        if raw >= sign_bit {
            raw -= modulus;
        }
    }
    i64::try_from(raw).ok()
}

fn bitfield_single_set_mutated_for_replication(
    args: &[ArgSlice],
    frame_response: &[u8],
    pre_string_len: Option<usize>,
) -> Option<bool> {
    if args.len() != 6 || !ascii_eq_ignore_case(arg_slice_bytes(args.get(2)?), b"SET") {
        return None;
    }
    let encoding = parse_bitfield_encoding_for_replication(arg_slice_bytes(args.get(3)?))?;
    let sign = encoding.sign;
    let bits = encoding.bits;
    let offset = parse_bitfield_offset_for_replication(arg_slice_bytes(args.get(4)?), bits)?;
    let input_value = parse_i64_ascii_bytes(arg_slice_bytes(args.get(5)?))?;
    let old_value = parse_resp_single_integer_array(frame_response)?;
    let wrapped_value = bitfield_wrap_set_value_for_replication(input_value, sign, bits)?;
    let required_bits = offset.checked_add(bits)?;
    let required_bytes = required_bits.checked_add(7)?.checked_div(8)?;
    let length_changed = pre_string_len.unwrap_or(0) < required_bytes;
    Some(length_changed || old_value != wrapped_value)
}

fn setbit_mutated_for_replication(
    args: &[ArgSlice],
    frame_response: &[u8],
    pre_string_len: Option<usize>,
) -> Option<bool> {
    if args.len() != 4 {
        return None;
    }
    let offset = parse_i64_ascii_bytes(arg_slice_bytes(args.get(2)?))?;
    if offset < 0 {
        return Some(true);
    }
    let offset = usize::try_from(offset).ok()?;
    let input_bit = parse_i64_ascii_bytes(arg_slice_bytes(args.get(3)?))?;
    if input_bit != 0 && input_bit != 1 {
        return Some(true);
    }
    let old_bit = parse_resp_integer(frame_response)?;
    let required_len = offset.checked_div(8)?.checked_add(1)?;
    let length_changed = pre_string_len.unwrap_or(0) < required_len;
    Some(length_changed || old_bit != input_bit)
}

fn pre_string_len_for_mutation_detection(
    processor: &RequestProcessor,
    command: CommandId,
    args: &[ArgSlice],
    selected_db: DbName,
) -> Option<usize> {
    if !matches!(command, CommandId::Setbit | CommandId::Bitfield) {
        return None;
    }
    let key = arg_slice_bytes(args.get(1)?);
    processor
        .string_value_len_for_replication(DbKeyRef::new(selected_db, key))
        .ok()
        .flatten()
}

fn command_effectively_mutated_for_replication(
    processor: &RequestProcessor,
    command: CommandId,
    args: &[ArgSlice],
    frame_response: &[u8],
    pre_string_len: Option<usize>,
) -> bool {
    match command {
        CommandId::Setbit => {
            setbit_mutated_for_replication(args, frame_response, pre_string_len).unwrap_or(true)
        }
        CommandId::Bitfield => {
            bitfield_single_set_mutated_for_replication(args, frame_response, pre_string_len)
                .unwrap_or(true)
        }
        CommandId::Eval | CommandId::EvalRo | CommandId::Evalsha | CommandId::EvalshaRo => {
            !eval_script_is_read_only_for_replication(processor, command, args).unwrap_or(false)
        }
        CommandId::Failover => false,
        _ => true,
    }
}

fn eval_script_is_read_only_for_replication(
    processor: &RequestProcessor,
    command: CommandId,
    args: &[ArgSlice],
) -> Option<bool> {
    let script = script_source_for_replication(processor, command, args)?;
    Some(!script_contains_mutating_call(&script))
}

fn script_source_for_replication(
    processor: &RequestProcessor,
    command: CommandId,
    args: &[ArgSlice],
) -> Option<Vec<u8>> {
    match command {
        CommandId::Eval | CommandId::EvalRo => Some(arg_slice_bytes(args.get(1)?).to_vec()),
        CommandId::Evalsha | CommandId::EvalshaRo => {
            let sha = arg_slice_bytes(args.get(1)?);
            processor.cached_script_for_sha(sha)
        }
        _ => None,
    }
}

fn script_contains_mutating_call(script: &[u8]) -> bool {
    let mut lower = script.to_vec();
    lower.make_ascii_lowercase();
    let mut cursor = 0usize;
    while cursor < lower.len() {
        let Some(marker) = next_script_call_marker(&lower, cursor) else {
            return false;
        };
        cursor = marker.position + marker.length;
        while cursor < lower.len() && lower[cursor].is_ascii_whitespace() {
            cursor += 1;
        }
        if cursor >= lower.len() || lower[cursor] != b'(' {
            return true;
        }
        cursor += 1;
        while cursor < lower.len() && lower[cursor].is_ascii_whitespace() {
            cursor += 1;
        }
        if cursor >= lower.len() {
            return true;
        }
        let quote = lower[cursor];
        if quote != b'\'' && quote != b'"' {
            return true;
        }
        cursor += 1;
        let token_start = cursor;
        while cursor < lower.len() && lower[cursor] != quote {
            cursor += 1;
        }
        if cursor >= lower.len() || cursor == token_start {
            return true;
        }
        let command_name = &lower[token_start..cursor];
        let command = crate::dispatch_command_name(command_name);
        if command == CommandId::Unknown || command_is_mutating(command) {
            return true;
        }
        cursor += 1;
    }
    false
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ScriptCallMarker {
    position: usize,
    length: usize,
}

fn next_script_call_marker(input: &[u8], start: usize) -> Option<ScriptCallMarker> {
    let calls = [
        (find_bytes(input, b"redis.call", start), b"redis.call".len()),
        (
            find_bytes(input, b"redis.pcall", start),
            b"redis.pcall".len(),
        ),
    ];
    calls
        .into_iter()
        .filter_map(|(position, length)| {
            position.map(|pos| ScriptCallMarker {
                position: pos,
                length,
            })
        })
        .min_by_key(|marker| marker.position)
}

fn find_bytes(haystack: &[u8], needle: &[u8], start: usize) -> Option<usize> {
    if needle.is_empty() || start >= haystack.len() || haystack.len() < needle.len() {
        return None;
    }
    haystack[start..]
        .windows(needle.len())
        .position(|window| window == needle)
        .map(|offset| start + offset)
}

fn command_uses_subcommand_stats(command: CommandId) -> bool {
    matches!(
        command,
        CommandId::Acl
            | CommandId::Client
            | CommandId::Cluster
            | CommandId::Command
            | CommandId::Config
            | CommandId::Debug
            | CommandId::Function
            | CommandId::Latency
            | CommandId::Memory
            | CommandId::Module
            | CommandId::Script
            | CommandId::Slowlog
    )
}

fn command_call_name_for_stats(
    command: CommandId,
    command_name: &[u8],
    subcommand_name: Option<&[u8]>,
) -> Vec<u8> {
    if !command_uses_subcommand_stats(command) {
        return command_name.to_vec();
    }
    let Some(subcommand_name) = subcommand_name else {
        return command_name.to_vec();
    };
    if subcommand_name.is_empty() {
        return command_name.to_vec();
    }

    let mut combined = Vec::with_capacity(command_name.len() + subcommand_name.len() + 1);
    combined.extend_from_slice(command_name);
    combined.push(b'|');
    combined.extend_from_slice(subcommand_name);
    combined
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClientReplyMode {
    On,
    Off,
    SkipNext,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClientTrackingMode {
    Off,
    On,
    OptIn,
    OptOut,
}

#[derive(Debug, Clone)]
struct ClientConnectionState {
    reply_mode: ClientReplyMode,
    tracking_mode: ClientTrackingMode,
    tracking_redirect_id: Option<i64>,
    tracking_bcast: bool,
    tracking_noloop: bool,
    tracking_prefixes: Vec<Vec<u8>>,
    tracking_caching: Option<bool>,
    cluster_read_only: bool,
    no_evict: bool,
    no_touch: bool,
    selected_db: DbName,
    resp_protocol_version: RespProtocolVersion,
}

impl Default for ClientConnectionState {
    fn default() -> Self {
        Self {
            reply_mode: ClientReplyMode::On,
            tracking_mode: ClientTrackingMode::Off,
            tracking_redirect_id: None,
            tracking_bcast: false,
            tracking_noloop: false,
            tracking_prefixes: Vec::new(),
            tracking_caching: None,
            cluster_read_only: false,
            no_evict: false,
            no_touch: false,
            selected_db: DbName::db0(),
            resp_protocol_version: RespProtocolVersion::Resp2,
        }
    }
}

impl ClientConnectionState {
    fn clear_tracking(&mut self) {
        self.tracking_mode = ClientTrackingMode::Off;
        self.tracking_redirect_id = None;
        self.tracking_bcast = false;
        self.tracking_noloop = false;
        self.tracking_prefixes.clear();
        self.tracking_caching = None;
    }
}

fn apply_request_connection_effects(
    client_state: &mut ClientConnectionState,
    connection_effects: RequestConnectionEffects,
) {
    if let Some(cluster_read_only) = connection_effects.cluster_read_only {
        client_state.cluster_read_only = cluster_read_only;
    }
}

async fn materialize_wait_response_if_needed(
    replication: &RedisReplicationCoordinator,
    response_out: &mut Vec<u8>,
    connection_effects: RequestConnectionEffects,
    wait_target_offset_for_connection: u64,
) {
    let Some(wait_request) = connection_effects.wait_request else {
        return;
    };
    let acknowledged = replication
        .wait_for_replicas(
            wait_request.requested_replicas,
            wait_request.timeout_millis,
            wait_target_offset_for_connection,
        )
        .await;
    response_out.clear();
    append_integer_frame(
        response_out,
        i64::try_from(acknowledged).unwrap_or(i64::MAX),
    );
}

async fn materialize_waitaof_response_if_needed(
    processor: &RequestProcessor,
    replication: &RedisReplicationCoordinator,
    response_out: &mut Vec<u8>,
    connection_effects: RequestConnectionEffects,
    wait_target_offset_for_connection: u64,
    local_aof_wait_target_offset_for_connection: u64,
) {
    let Some(waitaof_request) = connection_effects.waitaof_request else {
        return;
    };
    if waitaof_request.requested_local > 0 && !processor.appendonly_enabled() {
        response_out.clear();
        RequestExecutionError::WaitAofAppendOnlyDisabled.append_resp_error(response_out);
        return;
    }

    let local_target_offset = AofOffset::new(local_aof_wait_target_offset_for_connection);
    let deadline = if waitaof_request.timeout_millis == 0 {
        None
    } else {
        Some(tokio::time::Instant::now() + Duration::from_millis(waitaof_request.timeout_millis))
    };

    let local_acknowledged = if waitaof_request.requested_local > 0 {
        let timeout_millis = deadline
            .map(|deadline| {
                deadline
                    .saturating_duration_since(tokio::time::Instant::now())
                    .as_millis() as u64
            })
            .unwrap_or(0);
        processor
            .wait_for_local_aof_fsync(local_target_offset, timeout_millis)
            .await
    } else if processor.appendonly_enabled() {
        processor.local_aof_fsync_satisfied(local_target_offset)
    } else {
        false
    };

    let replica_acknowledged = if waitaof_request.requested_replicas > 0 {
        let timeout_millis = deadline
            .map(|deadline| {
                deadline
                    .saturating_duration_since(tokio::time::Instant::now())
                    .as_millis() as u64
            })
            .unwrap_or(0);
        replication
            .wait_for_aof_replicas(
                waitaof_request.requested_replicas,
                timeout_millis,
                wait_target_offset_for_connection,
            )
            .await
    } else {
        replication
            .try_acknowledged_downstream_aof_replica_count(wait_target_offset_for_connection)
            .unwrap_or(0)
    };

    response_out.clear();
    append_array_length_frame(response_out, 2);
    append_integer_frame(response_out, i64::from(local_acknowledged));
    append_integer_frame(
        response_out,
        i64::try_from(replica_acknowledged).unwrap_or(i64::MAX),
    );
}

fn materialize_failover_if_needed(
    replication: &RedisReplicationCoordinator,
    connection_effects: RequestConnectionEffects,
) {
    let Some(failover_request) = connection_effects.failover_request else {
        return;
    };
    let _ = replication.launch_prepared_manual_failover(
        crate::redis_replication::ManualFailoverRequest {
            target_replica_id: failover_request.target_replica_id,
            timeout_millis: failover_request.timeout_millis,
            force: failover_request.force,
        },
    );
}

fn apply_reset_command_side_effects(
    metrics: &ServerMetrics,
    processor: &RequestProcessor,
    client_id: ClientId,
    transaction: &mut ConnectionTransactionState,
    client_state: &mut ClientConnectionState,
    monitor_receiver: &mut Option<broadcast::Receiver<Vec<u8>>>,
    allow_asking_once: &mut bool,
) {
    transaction.reset();
    *client_state = ClientConnectionState::default();
    *monitor_receiver = None;
    *allow_asking_once = false;
    metrics.set_client_user(client_id, b"default".to_vec());
    metrics.set_client_selected_db(client_id, client_state.selected_db);
    processor.unregister_pubsub_client(client_id);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClientCommandReplyBehavior {
    Default,
    ForceReply,
    Suppress,
}

#[derive(Debug, Clone, Copy)]
struct ClientCommandOutcome {
    reply_behavior: ClientCommandReplyBehavior,
    disconnect_after_reply: bool,
}

impl Default for ClientCommandOutcome {
    fn default() -> Self {
        Self {
            reply_behavior: ClientCommandReplyBehavior::Default,
            disconnect_after_reply: false,
        }
    }
}

pub(crate) async fn handle_connection(
    mut stream: TcpStream,
    read_buffer_size: usize,
    metrics: Arc<ServerMetrics>,
    processor: Arc<RequestProcessor>,
    cluster_config: Option<Arc<ClusterConfigStore>>,
    owner_thread_pool: Arc<ShardOwnerThreadPool>,
    replication: Arc<RedisReplicationCoordinator>,
) -> io::Result<()> {
    let remote_addr = stream.peer_addr().ok();
    let local_addr = stream.local_addr().ok();
    let client_id = metrics.register_client(remote_addr, local_addr);
    processor.attach_metrics(Arc::clone(&metrics));
    processor.set_connected_clients(metrics.connected_client_count());
    let _lifecycle = ConnectionLifecycle {
        metrics: &metrics,
        processor: &processor,
        client_id,
    };
    let pending_pubsub_notify = processor.register_pubsub_client(client_id);
    let max_resp_arguments = parse_positive_env_usize(GARNET_MAX_RESP_ARGUMENTS_ENV)
        .unwrap_or(DEFAULT_MAX_RESP_ARGUMENTS);
    let mut read_buffer = vec![0u8; read_buffer_size.max(1)];
    let mut receive_buffer = Vec::with_capacity(read_buffer_size.max(1));
    let mut responses = Vec::with_capacity(read_buffer_size.max(1));
    let mut args = vec![ArgSlice::EMPTY; DEFAULT_RESP_ARG_SCRATCH.min(max_resp_arguments)];
    let mut transaction = ConnectionTransactionState::default();
    let mut allow_asking_once = false;
    let mut client_state = ClientConnectionState::default();
    let mut disconnect_after_write = false;
    let mut monitor_receiver: Option<broadcast::Receiver<Vec<u8>>> = None;
    let mut wait_target_offset_for_connection = 0u64;
    let mut local_aof_wait_target_offset_for_connection = 0u64;
    let mut replica_rdb_functions_only = false;

    loop {
        processor.set_connected_clients(metrics.connected_client_count());
        if metrics.is_client_killed(client_id) {
            return Ok(());
        }
        flush_monitor_events(&mut stream, &mut monitor_receiver).await?;
        // TLA+ : ServerProcessOne (queue intake stage)
        // Moves a flushed request burst from socket into the per-connection receive buffer.
        // Read at least once (await), then drain any immediately-available bytes via try_read.
        // This reduces read-side wakeups/syscall count under small pipelined requests.
        let bytes_read = if processor.pubsub_subscription_count(client_id) > 0 {
            match wait_for_connection_read_or_pubsub(
                &mut stream,
                &mut read_buffer,
                &mut receive_buffer,
                &metrics,
                pending_pubsub_notify.as_ref(),
            )
            .await?
            {
                ConnectionReadWake::BytesRead(bytes_read) => bytes_read,
                ConnectionReadWake::PendingPubsub | ConnectionReadWake::KilledClientPoll => {
                    flush_pending_pubsub_messages(
                        &mut stream,
                        &processor,
                        &metrics,
                        client_id,
                        &client_state,
                    )
                    .await?;
                    continue;
                }
            }
        } else {
            match tokio::time::timeout(
                KILLED_CLIENT_POLL_INTERVAL,
                read_and_drain_available(
                    &mut stream,
                    &mut read_buffer,
                    &mut receive_buffer,
                    &metrics,
                ),
            )
            .await
            {
                Ok(result) => result?,
                Err(_) => {
                    flush_pending_pubsub_messages(
                        &mut stream,
                        &processor,
                        &metrics,
                        client_id,
                        &client_state,
                    )
                    .await?;
                    continue;
                }
            }
        };
        if bytes_read == 0 {
            return Ok(());
        }
        let mut consumed = 0usize;
        responses.clear();
        let mut switch_to_replica_stream = false;
        let mut replica_subscriber = None;

        loop {
            if receive_buffer[consumed..].starts_with(b"\r\n") {
                // Redis ignores empty queries and keeps the connection open.
                consumed += 2;
                continue;
            }
            if receive_buffer[consumed..].starts_with(b"\n") {
                // Redis tests/support/util.tcl `formatCommand` output is commonly
                // written with `puts`, which appends a lone LF after a fully
                // formed RESP frame. Tolerate these empty separators as Redis does.
                consumed += 1;
                continue;
            }

            let mut inline_frame = Vec::new();
            let (argument_count, frame_bytes_consumed) = match parse_resp_command_arg_slices_dynamic(
                &receive_buffer[consumed..],
                &mut args,
                max_resp_arguments,
            ) {
                Ok(meta) => (meta.argument_count, meta.bytes_consumed),
                Err(RespParseError::Incomplete) => break,
                Err(RespParseError::NegativeArrayLength { .. }) => {
                    if let Some(bytes_consumed) =
                        consume_negative_multibulk_frame(&receive_buffer[consumed..])
                    {
                        // Redis ignores negative multibulk length frames.
                        consumed += bytes_consumed;
                        continue;
                    }
                    append_error_line(
                        &mut responses,
                        b"ERR Protocol error: invalid multibulk length",
                    );
                    stream.write_all(&responses).await?;
                    return Ok(());
                }
                Err(RespParseError::ArrayLengthOutOfRange { .. }) => {
                    append_error_line(
                        &mut responses,
                        b"ERR Protocol error: invalid multibulk length",
                    );
                    stream.write_all(&responses).await?;
                    return Ok(());
                }
                Err(RespParseError::InvalidArrayPrefix { .. }) => {
                    if let Some(first_byte) = receive_buffer.get(consumed).copied()
                        && !first_byte.is_ascii_graphic()
                        && !first_byte.is_ascii_whitespace()
                    {
                        append_error_line(&mut responses, b"ERR Protocol error");
                        stream.write_all(&responses).await?;
                        return Ok(());
                    }
                    match parse_inline_frame(&receive_buffer[consumed..]) {
                        InlineFrameParse::Parsed {
                            frame,
                            bytes_consumed,
                        } => {
                            inline_frame = frame;
                            match parse_resp_command_arg_slices_dynamic(
                                &inline_frame,
                                &mut args,
                                max_resp_arguments,
                            ) {
                                Ok(meta) => (meta.argument_count, bytes_consumed),
                                Err(RespParseError::ArgumentCapacityExceeded { .. }) => {
                                    append_too_many_arguments_error(
                                        &mut responses,
                                        max_resp_arguments,
                                    );
                                    stream.write_all(&responses).await?;
                                    return Ok(());
                                }
                                Err(error) => {
                                    append_resp_parse_error(&mut responses, error);
                                    stream.write_all(&responses).await?;
                                    return Ok(());
                                }
                            }
                        }
                        InlineFrameParse::Incomplete => break,
                        InlineFrameParse::UnbalancedQuotes => {
                            append_error_line(
                                &mut responses,
                                b"ERR Protocol error: unbalanced quotes in request",
                            );
                            stream.write_all(&responses).await?;
                            return Ok(());
                        }
                        InlineFrameParse::ProtocolError => {
                            append_error_line(&mut responses, b"ERR Protocol error");
                            stream.write_all(&responses).await?;
                            return Ok(());
                        }
                    }
                }
                Err(RespParseError::ArgumentCapacityExceeded { .. }) => {
                    append_too_many_arguments_error(&mut responses, max_resp_arguments);
                    stream.write_all(&responses).await?;
                    return Ok(());
                }
                Err(error) => {
                    append_resp_parse_error(&mut responses, error);
                    stream.write_all(&responses).await?;
                    return Ok(());
                }
            };

            let frame = if inline_frame.is_empty() {
                &receive_buffer[consumed..consumed + frame_bytes_consumed]
            } else {
                inline_frame.as_slice()
            };
            // SAFETY: `args` refers to either the live receive_buffer slice or inline_frame bytes.
            let command = unsafe { dispatch_from_arg_slices(&args[..argument_count]) };
            let response_mark = responses.len();
            if argument_count == 0 {
                responses.extend_from_slice(b"-ERR unknown command\r\n");
                let _ = finalize_client_command(
                    &metrics,
                    client_id,
                    &mut responses,
                    response_mark,
                    &mut client_state,
                    command,
                    ClientCommandOutcome::default(),
                    1,
                );
                consumed += frame_bytes_consumed;
                continue;
            }
            // SAFETY: `args` points to the current frame bytes.
            let command_name = arg_slice_bytes(&args[0]);
            let subcommand_name = if argument_count > 1 {
                Some(arg_slice_bytes(&args[1]))
            } else {
                None
            };
            // RESP3 push users commonly issue PING to drain pending pushes.
            // If an external invalidation arrived after the loop-top flush and
            // before this frame parse, emit it before the PING response.
            if command == CommandId::Ping && responses.is_empty() {
                flush_pending_pubsub_messages(
                    &mut stream,
                    &processor,
                    &metrics,
                    client_id,
                    &client_state,
                )
                .await?;
            }
            processor.set_resp_protocol_version(client_state.resp_protocol_version);
            metrics.add_client_input_bytes(client_id, frame_bytes_consumed as u64);
            metrics.set_client_last_command(client_id, command_name, subcommand_name);
            let command_call_name =
                command_call_name_for_stats(command, command_name, subcommand_name);
            processor.record_command_call(&command_call_name);
            if command != CommandId::Monitor && command != CommandId::Unknown {
                metrics.publish_monitor_event(build_monitor_event_line(&args[..argument_count]));
                if let Some(lua_event) = build_monitor_lua_event_line(&args[..argument_count]) {
                    metrics.publish_monitor_event(lua_event);
                }
            }
            let mut command_outcome = ClientCommandOutcome::default();
            let mut commands_processed = 1u64;
            let execution_count_before = processor.executed_command_count();

            if command == CommandId::Client && !transaction.in_multi {
                command_outcome = handle_client_command(
                    &processor,
                    &metrics,
                    client_id,
                    &args[..argument_count],
                    &mut client_state,
                    &mut responses,
                );
                disconnect_after_write |= finalize_client_command(
                    &metrics,
                    client_id,
                    &mut responses,
                    response_mark,
                    &mut client_state,
                    command,
                    command_outcome,
                    commands_processed,
                );
                consumed += frame_bytes_consumed;
                if disconnect_after_write {
                    break;
                }
                continue;
            }

            if command == CommandId::Monitor {
                if argument_count != 1 {
                    append_wrong_arity_error_for_command(&mut responses, CommandId::Monitor);
                } else {
                    append_simple_string(&mut responses, b"OK");
                    monitor_receiver = Some(metrics.monitor_subscribe());
                }
                disconnect_after_write |= finalize_client_command(
                    &metrics,
                    client_id,
                    &mut responses,
                    response_mark,
                    &mut client_state,
                    command,
                    command_outcome,
                    commands_processed,
                );
                consumed += frame_bytes_consumed;
                if disconnect_after_write {
                    break;
                }
                continue;
            }

            if command == CommandId::Quit {
                append_simple_string(&mut responses, b"OK");
                command_outcome.disconnect_after_reply = true;
                disconnect_after_write |= finalize_client_command(
                    &metrics,
                    client_id,
                    &mut responses,
                    response_mark,
                    &mut client_state,
                    command,
                    command_outcome,
                    commands_processed,
                );
                consumed += frame_bytes_consumed;
                // After QUIT, discard any remaining pipelined commands.
                break;
            }

            if !client_state.resp_protocol_version.is_resp3()
                && processor.pubsub_subscription_count(client_id) > 0
                && !command_allowed_in_resp2_subscribed_context(command)
            {
                append_resp2_subscribed_context_error(&mut responses, command_name);
                disconnect_after_write |= finalize_client_command(
                    &metrics,
                    client_id,
                    &mut responses,
                    response_mark,
                    &mut client_state,
                    command,
                    command_outcome,
                    commands_processed,
                );
                consumed += frame_bytes_consumed;
                if disconnect_after_write {
                    break;
                }
                continue;
            }

            if command == CommandId::Auth {
                let maybe_user = if argument_count == 2 {
                    Some(b"default".to_vec())
                } else if argument_count == 3 {
                    Some(arg_slice_bytes(&args[1]).to_vec())
                } else {
                    None
                };
                if let Some(user) = maybe_user {
                    if metrics.acl_user_exists(&user) {
                        metrics.set_client_user(client_id, user);
                        append_simple_string(&mut responses, b"OK");
                    } else {
                        append_error_line(
                            &mut responses,
                            b"WRONGPASS invalid username-password pair or user is disabled",
                        );
                    }
                } else {
                    append_wrong_arity_error_for_command(&mut responses, CommandId::Auth);
                }
                disconnect_after_write |= finalize_client_command(
                    &metrics,
                    client_id,
                    &mut responses,
                    response_mark,
                    &mut client_state,
                    command,
                    command_outcome,
                    commands_processed,
                );
                consumed += frame_bytes_consumed;
                if disconnect_after_write {
                    break;
                }
                continue;
            }

            if command == CommandId::Acl
                && argument_count == 2
                && ascii_eq_ignore_case(arg_slice_bytes(&args[1]), b"WHOAMI")
            {
                let whoami = metrics
                    .client_user(client_id)
                    .unwrap_or_else(|| b"default".to_vec());
                append_bulk_string_frame(&mut responses, &whoami);
                disconnect_after_write |= finalize_client_command(
                    &metrics,
                    client_id,
                    &mut responses,
                    response_mark,
                    &mut client_state,
                    command,
                    command_outcome,
                    commands_processed,
                );
                consumed += frame_bytes_consumed;
                if disconnect_after_write {
                    break;
                }
                continue;
            }

            if command == CommandId::Select && !transaction.in_multi {
                if argument_count != 2 {
                    append_wrong_arity_error_for_command(&mut responses, command);
                } else {
                    let index = parse_i64_ascii_bytes(arg_slice_bytes(&args[1]));
                    match index {
                        Some(index) if index >= 0 => match usize::try_from(index) {
                            Ok(index_raw) if index_raw < processor.configured_databases() => {
                                client_state.selected_db = DbName::new(index_raw);
                                metrics.set_client_selected_db(client_id, client_state.selected_db);
                                append_simple_string(&mut responses, b"OK");
                            }
                            Ok(_) | Err(_) => {
                                append_error_line(&mut responses, b"ERR DB index is out of range");
                            }
                        },
                        Some(_) => {
                            append_error_line(&mut responses, b"ERR DB index is out of range");
                        }
                        None => {
                            append_error_line(
                                &mut responses,
                                b"ERR value is not an integer or out of range",
                            );
                        }
                    }
                }
                disconnect_after_write |= finalize_client_command(
                    &metrics,
                    client_id,
                    &mut responses,
                    response_mark,
                    &mut client_state,
                    command,
                    command_outcome,
                    commands_processed,
                );
                consumed += frame_bytes_consumed;
                if disconnect_after_write {
                    break;
                }
                continue;
            }

            let replication_passthrough_command = command_is_replication_passthrough(command_name);
            if replication_passthrough_command && !transaction.in_multi {
                if argument_count != 3 {
                    responses.extend_from_slice(b"-ERR wrong number of arguments for '");
                    if ascii_eq_ignore_case(command_name, b"SLAVEOF") {
                        responses.extend_from_slice(b"SLAVEOF");
                    } else {
                        responses.extend_from_slice(b"REPLICAOF");
                    }
                    responses.extend_from_slice(b"' command\r\n");
                } else {
                    // SAFETY: `args` points to the current request frame.
                    let arg1 = arg_slice_bytes(&args[1]);
                    // SAFETY: `args` points to the current request frame.
                    let arg2 = arg_slice_bytes(&args[2]);

                    if ascii_eq_ignore_case(arg1, b"NO") && ascii_eq_ignore_case(arg2, b"ONE") {
                        replication.become_master().await;
                        append_simple_string(&mut responses, b"OK");
                    } else if let Some(master_port) = parse_u16_ascii(arg2) {
                        let master_host = String::from_utf8_lossy(arg1).to_string();
                        replication.become_replica(master_host, master_port).await;
                        append_simple_string(&mut responses, b"OK");
                    } else {
                        responses
                            .extend_from_slice(b"-ERR value is not an integer or out of range\r\n");
                    }
                }
                disconnect_after_write |= finalize_client_command(
                    &metrics,
                    client_id,
                    &mut responses,
                    response_mark,
                    &mut client_state,
                    command,
                    command_outcome,
                    commands_processed,
                );
                consumed += frame_bytes_consumed;
                if disconnect_after_write {
                    break;
                }
                continue;
            }

            if ascii_eq_ignore_case(command_name, b"REPLCONF") {
                let mut index = 1usize;
                while index + 1 < argument_count {
                    let option = arg_slice_bytes(&args[index]);
                    let value = arg_slice_bytes(&args[index + 1]);
                    if ascii_eq_ignore_case(option, b"LISTENING-PORT")
                        && let Some(replica_listen_port) = parse_u16_ascii(value)
                    {
                        metrics.set_client_replica_listen_port(client_id, replica_listen_port);
                    }
                    if ascii_eq_ignore_case(option, b"RDB-FILTER-ONLY") {
                        replica_rdb_functions_only = ascii_eq_ignore_case(value, b"FUNCTIONS");
                    }
                    index += 2;
                }
                append_simple_string(&mut responses, b"OK");
                disconnect_after_write |= finalize_client_command(
                    &metrics,
                    client_id,
                    &mut responses,
                    response_mark,
                    &mut client_state,
                    command,
                    command_outcome,
                    commands_processed,
                );
                consumed += frame_bytes_consumed;
                if disconnect_after_write {
                    break;
                }
                continue;
            }

            if ascii_eq_ignore_case(command_name, b"PSYNC") {
                match replication.build_fullresync_payload(replica_rdb_functions_only) {
                    Ok(payload) => {
                        metrics.set_client_type(client_id, ClientTypeFilter::Replica);
                        replica_subscriber = Some(replication.subscribe_downstream());
                        responses.extend_from_slice(&payload);
                        switch_to_replica_stream = true;
                    }
                    Err(error) => {
                        error.append_resp_error(&mut responses);
                    }
                }
                disconnect_after_write |= finalize_client_command(
                    &metrics,
                    client_id,
                    &mut responses,
                    response_mark,
                    &mut client_state,
                    command,
                    command_outcome,
                    commands_processed,
                );
                consumed += frame_bytes_consumed;
                break;
            }

            if ascii_eq_ignore_case(command_name, b"SYNC") {
                match replication.build_sync_payload(replica_rdb_functions_only) {
                    Ok(payload) => {
                        metrics.set_client_type(client_id, ClientTypeFilter::Replica);
                        replica_subscriber = Some(replication.subscribe_downstream());
                        responses.extend_from_slice(&payload);
                        switch_to_replica_stream = true;
                    }
                    Err(error) => {
                        error.append_resp_error(&mut responses);
                    }
                }
                disconnect_after_write |= finalize_client_command(
                    &metrics,
                    client_id,
                    &mut responses,
                    response_mark,
                    &mut client_state,
                    command,
                    command_outcome,
                    commands_processed,
                );
                consumed += frame_bytes_consumed;
                break;
            }

            let transaction_control = command_transaction_control(command);
            let subcommand = if argument_count > 1 {
                Some(arg_slice_bytes(&args[1]))
            } else {
                None
            };
            let replica_scripting_flags = scripting_replica_execution_flags(
                processor.as_ref(),
                command,
                &args[..argument_count],
            );
            let command_mutating = replica_scripting_flags
                .map(|flags| flags.effectively_mutating)
                .unwrap_or_else(|| command_is_effectively_mutating(command, subcommand));
            if transaction_control == TransactionControlCommand::Asking {
                if !command_has_valid_arity(command, argument_count) {
                    append_wrong_arity_error_for_command(&mut responses, command);
                } else {
                    allow_asking_once = true;
                    append_simple_string(&mut responses, b"OK");
                }
                disconnect_after_write |= finalize_client_command(
                    &metrics,
                    client_id,
                    &mut responses,
                    response_mark,
                    &mut client_state,
                    command,
                    command_outcome,
                    commands_processed,
                );
                consumed += frame_bytes_consumed;
                if disconnect_after_write {
                    break;
                }
                continue;
            }
            if let Some(cluster_store) = cluster_config.as_ref() {
                let (redirection_error, consume_asking) = cluster_error_for_command(
                    cluster_store,
                    &args[..argument_count],
                    command,
                    allow_asking_once,
                    client_state.cluster_read_only,
                    command_mutating,
                )?;
                if consume_asking {
                    allow_asking_once = false;
                }
                if let Some(redirection_error) = redirection_error {
                    append_error_line(&mut responses, redirection_error.as_bytes());
                    disconnect_after_write |= finalize_client_command(
                        &metrics,
                        client_id,
                        &mut responses,
                        response_mark,
                        &mut client_state,
                        command,
                        command_outcome,
                        commands_processed,
                    );
                    consumed += frame_bytes_consumed;
                    if disconnect_after_write {
                        break;
                    }
                    continue;
                }
            }
            let propagate_frame = false;
            if transaction.in_multi {
                match transaction_control {
                    TransactionControlCommand::Exec => {
                        if !command_has_valid_arity(command, argument_count) {
                            append_wrong_arity_error_for_command(&mut responses, command);
                        } else {
                            match processor
                                .command_rejected_while_script_busy(command, subcommand_name)
                            {
                                Ok(true) => {
                                    transaction.reset();
                                    responses.extend_from_slice(
                                        b"-EXECABORT Transaction discarded because of previous errors: BUSY Redis is busy running a script. You can only call FUNCTION KILL or SCRIPT KILL.\r\n",
                                    );
                                }
                                Ok(false) => {
                                    if watched_keys_dirty_or_expired(
                                        &processor,
                                        &transaction.watched_keys,
                                    ) {
                                        transaction.reset();
                                        responses.extend_from_slice(b"*-1\r\n");
                                    } else if transaction.aborted {
                                        let aborted_due_to_busy_script =
                                            transaction.aborted_due_to_busy_script;
                                        transaction.reset();
                                        if aborted_due_to_busy_script {
                                            responses.extend_from_slice(
                                                b"-EXECABORT Transaction discarded because of previous errors: BUSY Redis is busy running a script. You can only call FUNCTION KILL or SCRIPT KILL.\r\n",
                                            );
                                        } else {
                                            responses.extend_from_slice(
                                                b"-EXECABORT Transaction discarded because of previous errors.\r\n",
                                            );
                                        }
                                    } else {
                                        match transaction_runtime_abort_reason(
                                            &processor,
                                            &replication,
                                            client_state.selected_db,
                                            &transaction.queued_frames,
                                            max_resp_arguments,
                                        ) {
                                            Ok(Some(reason)) => {
                                                transaction.reset();
                                                responses.extend_from_slice(
                                                    transaction_runtime_execabort_message(reason),
                                                );
                                            }
                                            Err(error) => {
                                                transaction.reset();
                                                processor
                                                    .record_error_reply(error.info_error_name());
                                                error.append_resp_error(&mut responses);
                                            }
                                            Ok(None) => {
                                                // CLIENT PAUSE gating at EXEC: if the transaction
                                                // contains write-pause-affected commands, block until
                                                // the pause expires or is cancelled.
                                                let exec_write_affected =
                                                    transaction_has_write_pause_affected_commands(
                                                        &transaction.queued_frames,
                                                        max_resp_arguments,
                                                    );
                                                if processor.is_client_paused(exec_write_affected) {
                                                    if !responses.is_empty() {
                                                        stream.write_all(&responses).await?;
                                                        responses.clear();
                                                    }
                                                    wait_for_client_unpause(
                                                        &processor,
                                                        &metrics,
                                                        client_id,
                                                        exec_write_affected,
                                                    )
                                                    .await;
                                                }
                                                let transaction_outcome = execute_transaction_queue(
                                                    &processor,
                                                    &owner_thread_pool,
                                                    &mut transaction,
                                                    &mut responses,
                                                    max_resp_arguments,
                                                    client_state.no_touch,
                                                    Some(client_id),
                                                    client_state.selected_db,
                                                );
                                                if let Some(wait_target_offset) =
                                                    publish_transaction_replication_frames(
                                                        &processor,
                                                        &replication,
                                                        &transaction_outcome,
                                                        max_resp_arguments,
                                                    )
                                                {
                                                    wait_target_offset_for_connection =
                                                        wait_target_offset.replication_offset;
                                                    metrics.set_client_wait_target_offset(
                                                        client_id,
                                                        wait_target_offset_for_connection,
                                                    );
                                                    local_aof_wait_target_offset_for_connection =
                                                        wait_target_offset
                                                            .local_aof_append_offset
                                                            .map(u64::from)
                                                            .unwrap_or(0);
                                                    metrics
                                                        .set_client_local_aof_wait_target_offset(
                                                        client_id,
                                                        local_aof_wait_target_offset_for_connection,
                                                    );
                                                }
                                                client_state.selected_db =
                                                    transaction_outcome.selected_db_after_exec;
                                                apply_request_connection_effects(
                                                    &mut client_state,
                                                    transaction_outcome
                                                        .connection_effects_after_exec,
                                                );
                                                materialize_failover_if_needed(
                                                    &replication,
                                                    transaction_outcome
                                                        .connection_effects_after_exec,
                                                );
                                                metrics.set_client_selected_db(
                                                    client_id,
                                                    client_state.selected_db,
                                                );
                                                if let Some(replication_transition) =
                                                    transaction_outcome
                                                        .pending_replication_transition
                                                {
                                                    apply_queued_replication_transition(
                                                        &replication,
                                                        replication_transition,
                                                    )
                                                    .await;
                                                }
                                            }
                                        }
                                    }
                                }
                                Err(error) => {
                                    processor.record_error_reply(error.info_error_name());
                                    error.append_resp_error(&mut responses);
                                }
                            }
                        }
                    }
                    TransactionControlCommand::Discard => {
                        if !command_has_valid_arity(command, argument_count) {
                            append_wrong_arity_error_for_command(&mut responses, command);
                        } else {
                            transaction.reset();
                            append_simple_string(&mut responses, b"OK");
                        }
                    }
                    TransactionControlCommand::Multi => {
                        responses.extend_from_slice(b"-ERR MULTI calls can not be nested\r\n");
                    }
                    TransactionControlCommand::Watch => {
                        responses.extend_from_slice(b"-ERR WATCH inside MULTI is not allowed\r\n");
                    }
                    TransactionControlCommand::Unwatch => {
                        if !command_has_valid_arity(command, argument_count) {
                            append_wrong_arity_error_for_command(&mut responses, command);
                        } else {
                            // Matches Garnet behavior: UNWATCH during MULTI is a no-op.
                            append_simple_string(&mut responses, b"OK");
                        }
                    }
                    TransactionControlCommand::Reset => {
                        if !command_has_valid_arity(command, argument_count) {
                            append_wrong_arity_error_for_command(&mut responses, command);
                        } else {
                            apply_reset_command_side_effects(
                                &metrics,
                                &processor,
                                client_id,
                                &mut transaction,
                                &mut client_state,
                                &mut monitor_receiver,
                                &mut allow_asking_once,
                            );
                            append_simple_string(&mut responses, b"RESET");
                        }
                    }
                    _ => {
                        match processor.command_rejected_while_script_busy(command, subcommand_name)
                        {
                            Ok(true) => {
                                transaction.aborted = true;
                                transaction.aborted_due_to_busy_script = true;
                                processor.record_error_reply(
                                    RequestExecutionError::BusyScript.info_error_name(),
                                );
                                RequestExecutionError::BusyScript.append_resp_error(&mut responses);
                                disconnect_after_write |= finalize_client_command(
                                    &metrics,
                                    client_id,
                                    &mut responses,
                                    response_mark,
                                    &mut client_state,
                                    command,
                                    command_outcome,
                                    commands_processed,
                                );
                                consumed += frame_bytes_consumed;
                                if disconnect_after_write {
                                    break;
                                }
                                continue;
                            }
                            Ok(false) => {}
                            Err(error) => {
                                transaction.aborted = true;
                                processor.record_error_reply(error.info_error_name());
                                error.append_resp_error(&mut responses);
                                disconnect_after_write |= finalize_client_command(
                                    &metrics,
                                    client_id,
                                    &mut responses,
                                    response_mark,
                                    &mut client_state,
                                    command,
                                    command_outcome,
                                    commands_processed,
                                );
                                consumed += frame_bytes_consumed;
                                if disconnect_after_write {
                                    break;
                                }
                                continue;
                            }
                        }
                        if command == CommandId::Unknown && !replication_passthrough_command {
                            transaction.aborted = true;
                            responses.extend_from_slice(b"-ERR unknown command\r\n");
                            disconnect_after_write |= finalize_client_command(
                                &metrics,
                                client_id,
                                &mut responses,
                                response_mark,
                                &mut client_state,
                                command,
                                command_outcome,
                                commands_processed,
                            );
                            consumed += frame_bytes_consumed;
                            if disconnect_after_write {
                                break;
                            }
                            continue;
                        }
                        if !command_has_valid_arity(command, argument_count) {
                            transaction.aborted = true;
                            append_wrong_arity_error_for_command(&mut responses, command);
                            disconnect_after_write |= finalize_client_command(
                                &metrics,
                                client_id,
                                &mut responses,
                                response_mark,
                                &mut client_state,
                                command,
                                command_outcome,
                                commands_processed,
                            );
                            consumed += frame_bytes_consumed;
                            if disconnect_after_write {
                                break;
                            }
                            continue;
                        }
                        if command_disallowed_inside_multi(command) {
                            transaction.aborted = true;
                            append_error_line(
                                &mut responses,
                                b"ERR Command not allowed inside a transaction",
                            );
                            disconnect_after_write |= finalize_client_command(
                                &metrics,
                                client_id,
                                &mut responses,
                                response_mark,
                                &mut client_state,
                                command,
                                command_outcome,
                                commands_processed,
                            );
                            consumed += frame_bytes_consumed;
                            if disconnect_after_write {
                                break;
                            }
                            continue;
                        }
                        match processor.should_reject_mutating_command_for_maxmemory(
                            client_state.selected_db,
                            command_mutating,
                        ) {
                            Ok(true) => {
                                transaction.aborted = true;
                                append_error_line(
                                    &mut responses,
                                    b"OOM command not allowed when used memory > 'maxmemory'.",
                                );
                                disconnect_after_write |= finalize_client_command(
                                    &metrics,
                                    client_id,
                                    &mut responses,
                                    response_mark,
                                    &mut client_state,
                                    command,
                                    command_outcome,
                                    commands_processed,
                                );
                                consumed += frame_bytes_consumed;
                                if disconnect_after_write {
                                    break;
                                }
                                continue;
                            }
                            Ok(false) => {}
                            Err(error) => {
                                transaction.aborted = true;
                                processor.record_error_reply(error.info_error_name());
                                error.append_resp_error(&mut responses);
                                disconnect_after_write |= finalize_client_command(
                                    &metrics,
                                    client_id,
                                    &mut responses,
                                    response_mark,
                                    &mut client_state,
                                    command,
                                    command_outcome,
                                    commands_processed,
                                );
                                consumed += frame_bytes_consumed;
                                if disconnect_after_write {
                                    break;
                                }
                                continue;
                            }
                        }
                        if replication.is_replica_mode() && command_mutating {
                            responses.extend_from_slice(
                                b"-READONLY You can't write against a read only replica.\r\n",
                            );
                            disconnect_after_write |= finalize_client_command(
                                &metrics,
                                client_id,
                                &mut responses,
                                response_mark,
                                &mut client_state,
                                command,
                                command_outcome,
                                commands_processed,
                            );
                            consumed += frame_bytes_consumed;
                            if disconnect_after_write {
                                break;
                            }
                            continue;
                        }
                        if cluster_config.is_some()
                            && let Some(slot) =
                                command_hash_slot_for_transaction(&args[..argument_count], command)
                            && !transaction.set_transaction_slot_or_abort(slot)
                        {
                            responses.extend_from_slice(
                                b"-CROSSSLOT Keys in request don't hash to the same slot\r\n",
                            );
                            disconnect_after_write |= finalize_client_command(
                                &metrics,
                                client_id,
                                &mut responses,
                                response_mark,
                                &mut client_state,
                                command,
                                command_outcome,
                                commands_processed,
                            );
                            consumed += frame_bytes_consumed;
                            if disconnect_after_write {
                                break;
                            }
                            continue;
                        }
                        transaction.queued_frames.push(frame.to_vec());
                        append_simple_string(&mut responses, b"QUEUED");
                    }
                }
            } else {
                match transaction_control {
                    TransactionControlCommand::Reset => {
                        if !command_has_valid_arity(command, argument_count) {
                            append_wrong_arity_error_for_command(&mut responses, command);
                        } else {
                            apply_reset_command_side_effects(
                                &metrics,
                                &processor,
                                client_id,
                                &mut transaction,
                                &mut client_state,
                                &mut monitor_receiver,
                                &mut allow_asking_once,
                            );
                            append_simple_string(&mut responses, b"RESET");
                        }
                    }
                    TransactionControlCommand::Multi => {
                        if !command_has_valid_arity(command, argument_count) {
                            append_wrong_arity_error_for_command(&mut responses, command);
                        } else {
                            transaction.in_multi = true;
                            append_simple_string(&mut responses, b"OK");
                        }
                    }
                    TransactionControlCommand::Exec => {
                        responses.extend_from_slice(b"-ERR EXEC without MULTI\r\n");
                    }
                    TransactionControlCommand::Discard => {
                        responses.extend_from_slice(b"-ERR DISCARD without MULTI\r\n");
                    }
                    TransactionControlCommand::Watch => {
                        if !command_has_valid_arity(command, argument_count) {
                            append_wrong_arity_error_for_command(&mut responses, command);
                        } else {
                            let mut watch_error: Option<RequestExecutionError> = None;
                            for key_arg in &args[1..argument_count] {
                                // SAFETY: `args` points to the live command frame.
                                let key = arg_slice_bytes(key_arg);
                                match processor.capture_watched_key(client_state.selected_db, key) {
                                    Ok(watched_key) => transaction.watch_key(watched_key),
                                    Err(error) => {
                                        watch_error = Some(error);
                                        break;
                                    }
                                }
                            }
                            if let Some(error) = watch_error {
                                transaction.clear_watches();
                                processor.record_error_reply(error.info_error_name());
                                error.append_resp_error(&mut responses);
                            } else {
                                append_simple_string(&mut responses, b"OK");
                            }
                        }
                    }
                    TransactionControlCommand::Unwatch => {
                        if !command_has_valid_arity(command, argument_count) {
                            append_wrong_arity_error_for_command(&mut responses, command);
                        } else {
                            transaction.clear_watches();
                            append_simple_string(&mut responses, b"OK");
                        }
                    }
                    _ => {
                        let reject_reads_on_stale_replica = replication.is_replica_mode()
                            && !processor.replica_serve_stale_data()
                            && !replication.is_upstream_link_up();
                        if reject_reads_on_stale_replica
                            && replica_scripting_flags.is_some_and(|flags| !flags.allow_stale)
                        {
                            responses.extend_from_slice(
                                b"-MASTERDOWN Link with MASTER is down and replica-serve-stale-data is set to 'no'.\r\n",
                            );
                            disconnect_after_write |= finalize_client_command(
                                &metrics,
                                client_id,
                                &mut responses,
                                response_mark,
                                &mut client_state,
                                command,
                                command_outcome,
                                commands_processed,
                            );
                            consumed += frame_bytes_consumed;
                            if disconnect_after_write {
                                break;
                            }
                            continue;
                        }
                        if replication.is_replica_mode() && command_mutating {
                            responses.extend_from_slice(
                                b"-READONLY You can't write against a read only replica.\r\n",
                            );
                            disconnect_after_write |= finalize_client_command(
                                &metrics,
                                client_id,
                                &mut responses,
                                response_mark,
                                &mut client_state,
                                command,
                                command_outcome,
                                commands_processed,
                            );
                            consumed += frame_bytes_consumed;
                            if disconnect_after_write {
                                break;
                            }
                            continue;
                        }
                        let requires_good_replicas = command_requires_good_replicas(
                            processor.as_ref(),
                            command,
                            &args[..argument_count],
                        );
                        let min_replicas_to_write = processor.min_replicas_to_write();
                        if requires_good_replicas
                            && min_replicas_to_write > 0
                            && replication.downstream_replica_count() < min_replicas_to_write
                        {
                            responses.extend_from_slice(
                                b"-NOREPLICAS Not enough good replicas to write.\r\n",
                            );
                            disconnect_after_write |= finalize_client_command(
                                &metrics,
                                client_id,
                                &mut responses,
                                response_mark,
                                &mut client_state,
                                command,
                                command_outcome,
                                commands_processed,
                            );
                            consumed += frame_bytes_consumed;
                            if disconnect_after_write {
                                break;
                            }
                            continue;
                        }
                        // CLIENT PAUSE gating: block until pause expires or is cancelled.
                        // For EVAL, pass the script body to check shebang flags.
                        // For EVALSHA, look up the cached script body.
                        // For FCALL, check the function registry for no-writes flag.
                        let cached_script_body_for_pause;
                        let script_body_for_pause =
                            if command == CommandId::Eval && argument_count > 1 {
                                Some(arg_slice_bytes(&args[1]))
                            } else if command == CommandId::Evalsha && argument_count > 1 {
                                cached_script_body_for_pause =
                                    processor.cached_script_body(arg_slice_bytes(&args[1]));
                                cached_script_body_for_pause.as_deref()
                            } else if command == CommandId::Script
                                && argument_count > 2
                                && ascii_eq_ignore_case(arg_slice_bytes(&args[1]), b"LOAD")
                            {
                                Some(arg_slice_bytes(&args[2]))
                            } else {
                                None
                            };
                        let mut write_pause_affected = command_is_write_pause_affected_with_script(
                            command,
                            if argument_count > 1 {
                                Some(arg_slice_bytes(&args[1]))
                            } else {
                                None
                            },
                            script_body_for_pause,
                        );
                        // FCALL with a read-only function should not be blocked.
                        if write_pause_affected
                            && command == CommandId::Fcall
                            && argument_count > 1
                            && processor.is_function_read_only(arg_slice_bytes(&args[1]))
                        {
                            write_pause_affected = false;
                        }
                        // Redis returns syntax/arity errors immediately under CLIENT PAUSE WRITE.
                        if !command_has_valid_arity(command, argument_count) {
                            write_pause_affected = false;
                        }
                        let blocking_request =
                            request_uses_blocking_wait(command, &args[..argument_count]);
                        let mut waited_for_client_unpause = false;
                        let mut resumed_blocking_command_after_pause = false;
                        if processor.is_client_paused(write_pause_affected) {
                            if blocking_request {
                                // TLA+ : PauseGateRegisterBlockingClient
                                processor.register_pause_blocked_blocking_client(client_id);
                                resumed_blocking_command_after_pause = true;
                            }
                            // Flush any buffered responses before blocking.
                            if !responses.is_empty() {
                                stream.write_all(&responses).await?;
                                responses.clear();
                            }
                            wait_for_client_unpause(
                                &processor,
                                &metrics,
                                client_id,
                                write_pause_affected,
                            )
                            .await;
                            waited_for_client_unpause = true;
                        }
                        if blocking_request && !responses.is_empty() {
                            stream.write_all(&responses).await?;
                            responses.clear();
                        }
                        let mut replication_frames: Vec<(DbName, Vec<u8>)> = Vec::new();
                        let mut wait_for_blocking_progress = false;
                        let blocked_before = if command_mutating {
                            processor.blocked_clients()
                        } else {
                            0
                        };
                        // TLA+ : ServerProcessOne
                        // Executes one parsed request frame and appends its reply to the buffered response stream.
                        let retry_busy_script_until =
                            if waited_for_client_unpause && command_is_scripting_family(command) {
                                Some(Instant::now() + SCRIPT_BUSY_AFTER_UNPAUSE_RETRY_WINDOW)
                            } else {
                                None
                            };
                        let blocking_slowlog_started_at = Instant::now();
                        let blocking_outcome = loop {
                            let execution = execute_blocking_frame_on_owner_thread(
                                &processor,
                                &metrics,
                                &owner_thread_pool,
                                &replication,
                                &args[..argument_count],
                                command,
                                command_mutating,
                                frame,
                                client_state.no_touch,
                                client_id,
                                client_state.selected_db,
                                wait_target_offset_for_connection,
                                local_aof_wait_target_offset_for_connection,
                                &mut stream,
                                resumed_blocking_command_after_pause,
                            )
                            .await;
                            if matches!(
                                &execution,
                                Ok(BlockingExecutionOutcome { frame_response, .. })
                                    if frame_response.starts_with(BUSY_SCRIPT_RESPONSE_PREFIX)
                            ) && retry_busy_script_until
                                .is_some_and(|deadline| Instant::now() < deadline)
                            {
                                sleep(SCRIPT_BUSY_AFTER_UNPAUSE_RETRY_POLL).await;
                                continue;
                            }
                            if matches!(
                                &execution,
                                Err(OwnerThreadExecutionError::Request(
                                    RequestExecutionError::BusyScript
                                ))
                            ) && retry_busy_script_until
                                .is_some_and(|deadline| Instant::now() < deadline)
                            {
                                sleep(SCRIPT_BUSY_AFTER_UNPAUSE_RETRY_POLL).await;
                                continue;
                            }
                            break execution;
                        };
                        match blocking_outcome {
                            Ok(blocking_outcome) => {
                                if blocking_request {
                                    let slowlog_args = args[..argument_count]
                                        .iter()
                                        .map(arg_slice_bytes)
                                        .collect::<Vec<_>>();
                                    processor.record_slowlog_command(
                                        &slowlog_args,
                                        command,
                                        blocking_slowlog_started_at.elapsed(),
                                        Some(client_id),
                                    );
                                }
                                wait_for_blocking_progress = blocked_before > 0
                                    && command_may_wake_blocking_waiters(
                                        command,
                                        &args[..argument_count],
                                    );
                                maybe_apply_hello_side_effects(
                                    command,
                                    &args[..argument_count],
                                    &blocking_outcome.frame_response,
                                    &mut client_state,
                                    &metrics,
                                    &processor,
                                    client_id,
                                );
                                maybe_apply_acl_setuser_side_effects(
                                    command,
                                    &args[..argument_count],
                                    &blocking_outcome.frame_response,
                                    &metrics,
                                );
                                apply_request_connection_effects(
                                    &mut client_state,
                                    blocking_outcome.connection_effects,
                                );
                                materialize_failover_if_needed(
                                    &replication,
                                    blocking_outcome.connection_effects,
                                );
                                responses.extend_from_slice(&blocking_outcome.frame_response);
                                replication_frames.extend(
                                    blocking_outcome
                                        .deferred_replication_frames
                                        .into_iter()
                                        .map(|effect| (effect.selected_db, effect.frame)),
                                );
                                if command_uses_script_effect_replication(command) {
                                    replication_frames.extend(
                                        take_script_effect_replication_frames(
                                            &processor,
                                            max_resp_arguments,
                                        )
                                        .into_iter()
                                        .map(|frame| (client_state.selected_db, frame)),
                                    );
                                } else if blocking_outcome.should_replicate {
                                    if let Some(replication_frame) = replication_frame_for_command(
                                        &processor,
                                        client_state.selected_db,
                                        command,
                                        &args[..argument_count],
                                        &blocking_outcome.frame_response,
                                        frame,
                                    ) {
                                        replication_frames
                                            .push((client_state.selected_db, replication_frame));
                                    }
                                }
                            }
                            Err(OwnerThreadExecutionError::Request(error)) => {
                                if blocking_request {
                                    let slowlog_args = args[..argument_count]
                                        .iter()
                                        .map(arg_slice_bytes)
                                        .collect::<Vec<_>>();
                                    processor.record_slowlog_command(
                                        &slowlog_args,
                                        command,
                                        blocking_slowlog_started_at.elapsed(),
                                        Some(client_id),
                                    );
                                }
                                processor.record_command_failure(&command_call_name);
                                processor.record_error_reply(error.info_error_name());
                                error.append_resp_error(&mut responses);
                            }
                            Err(OwnerThreadExecutionError::Protocol) => {
                                responses.extend_from_slice(b"-ERR protocol error\r\n");
                            }
                            Err(OwnerThreadExecutionError::OwnerThreadUnavailable) => {
                                responses
                                    .extend_from_slice(b"-ERR owner routing execution failed\r\n");
                            }
                        }
                        let lazy_expired_keys = processor.take_lazy_expired_keys_for_replication();
                        let publish_lazy_expire_deletes = should_publish_lazy_expire_deletes(
                            &processor,
                            command,
                            command_mutating,
                        );
                        let mut published_replication_write = false;
                        if publish_lazy_expire_deletes {
                            for expired in &lazy_expired_keys {
                                publish_select_if_needed(&replication, expired.db);
                                let del_frame =
                                    encode_resp_frame_slices(&[b"DEL", expired.key.as_slice()]);
                                processor.record_rdb_change(1);
                                let published = replication.publish_write_frame(&del_frame);
                                wait_target_offset_for_connection = published.replication_offset;
                                local_aof_wait_target_offset_for_connection = published
                                    .local_aof_append_offset
                                    .map(u64::from)
                                    .unwrap_or(0);
                                published_replication_write = true;
                            }
                        }
                        if !replication_frames.is_empty() {
                            for (selected_db, frame_to_replicate) in &replication_frames {
                                publish_select_if_needed(&replication, *selected_db);
                                processor.record_rdb_change(1);
                                let published = replication.publish_write_frame(frame_to_replicate);
                                wait_target_offset_for_connection = published.replication_offset;
                                local_aof_wait_target_offset_for_connection = published
                                    .local_aof_append_offset
                                    .map(u64::from)
                                    .unwrap_or(0);
                                published_replication_write = true;
                            }
                        }
                        if published_replication_write {
                            metrics.set_client_wait_target_offset(
                                client_id,
                                wait_target_offset_for_connection,
                            );
                            metrics.set_client_local_aof_wait_target_offset(
                                client_id,
                                local_aof_wait_target_offset_for_connection,
                            );
                        }
                        if wait_for_blocking_progress {
                            yield_for_blocking_progress(&processor, blocked_before).await;
                        }
                        commands_processed =
                            command_execution_delta(&processor, execution_count_before, command);
                        disconnect_after_write |= finalize_client_command(
                            &metrics,
                            client_id,
                            &mut responses,
                            response_mark,
                            &mut client_state,
                            command,
                            command_outcome,
                            commands_processed,
                        );
                        consumed += frame_bytes_consumed;
                        if disconnect_after_write {
                            break;
                        }
                        continue;
                    }
                }
            }
            if propagate_frame {
                processor.record_rdb_change(1);
                let published = replication.publish_write_frame(frame);
                wait_target_offset_for_connection = published.replication_offset;
                local_aof_wait_target_offset_for_connection = published
                    .local_aof_append_offset
                    .map(u64::from)
                    .unwrap_or(0);
                metrics.set_client_wait_target_offset(client_id, wait_target_offset_for_connection);
                metrics.set_client_local_aof_wait_target_offset(
                    client_id,
                    local_aof_wait_target_offset_for_connection,
                );
            }
            commands_processed =
                command_execution_delta(&processor, execution_count_before, command);
            disconnect_after_write |= finalize_client_command(
                &metrics,
                client_id,
                &mut responses,
                response_mark,
                &mut client_state,
                command,
                command_outcome,
                commands_processed,
            );
            consumed += frame_bytes_consumed;
            if disconnect_after_write {
                break;
            }
        }

        if switch_to_replica_stream {
            if consumed > 0 {
                receive_buffer.drain(..consumed);
            }
            if !responses.is_empty() {
                stream.write_all(&responses).await?;
            }
            if let Some(subscriber) = replica_subscriber {
                return replication
                    .serve_downstream_replica_with_metrics(
                        stream,
                        subscriber,
                        Arc::clone(&metrics),
                        client_id,
                    )
                    .await;
            }
            return replication.serve_downstream_replica(stream).await;
        }

        let observed_query_buffer_bytes = logical_query_buffer_bytes(&receive_buffer);
        if consumed > 0 {
            receive_buffer.drain(..consumed);
        }

        // Update CLIENT LIST buffer tracking fields after draining consumed bytes.
        metrics.update_client_buffer_info(
            client_id,
            observed_query_buffer_bytes,
            receive_buffer.len(),
            read_buffer.len(),
        );

        if !responses.is_empty() {
            // TLA+ : ServerProcessOne (reply publication)
            // Makes command replies visible to the client-side read loop.
            stream.write_all(&responses).await?;
        }
        // Flush any pubsub messages that were generated during command
        // execution (e.g. PUBLISH inside MULTI/EXEC).  Without this,
        // subscribers only see the messages on the next read-timeout cycle,
        // which breaks clients that expect immediate delivery after EXEC.
        flush_pending_pubsub_messages(&mut stream, &processor, &metrics, client_id, &client_state)
            .await?;
        if disconnect_after_write {
            return Ok(());
        }
    }
}

async fn read_and_drain_available(
    stream: &mut TcpStream,
    read_buffer: &mut [u8],
    receive_buffer: &mut Vec<u8>,
    metrics: &ServerMetrics,
) -> io::Result<usize> {
    let first = stream.read(read_buffer).await?;
    if first == 0 {
        return Ok(0);
    }

    receive_buffer.extend_from_slice(&read_buffer[..first]);
    metrics
        .bytes_received
        .fetch_add(first as u64, Ordering::Relaxed);
    let mut total = first;

    loop {
        match stream.try_read(read_buffer) {
            Ok(0) => break,
            Ok(bytes_read) => {
                receive_buffer.extend_from_slice(&read_buffer[..bytes_read]);
                metrics
                    .bytes_received
                    .fetch_add(bytes_read as u64, Ordering::Relaxed);
                total += bytes_read;
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
            Err(error) => return Err(error),
        }
    }

    Ok(total)
}

async fn wait_for_connection_read_or_pubsub(
    stream: &mut TcpStream,
    read_buffer: &mut [u8],
    receive_buffer: &mut Vec<u8>,
    metrics: &ServerMetrics,
    pending_pubsub_notify: &tokio::sync::Notify,
) -> io::Result<ConnectionReadWake> {
    tokio::select! {
        result = read_and_drain_available(stream, read_buffer, receive_buffer, metrics) => {
            result.map(ConnectionReadWake::BytesRead)
        }
        _ = pending_pubsub_notify.notified() => {
            Ok(ConnectionReadWake::PendingPubsub)
        }
        _ = sleep(KILLED_CLIENT_POLL_INTERVAL) => {
            Ok(ConnectionReadWake::KilledClientPoll)
        }
    }
}

async fn flush_monitor_events(
    stream: &mut TcpStream,
    monitor_receiver: &mut Option<broadcast::Receiver<Vec<u8>>>,
) -> io::Result<()> {
    let Some(receiver) = monitor_receiver.as_mut() else {
        return Ok(());
    };
    loop {
        match receiver.try_recv() {
            Ok(event) => stream.write_all(&event).await?,
            Err(broadcast::error::TryRecvError::Empty) => return Ok(()),
            Err(broadcast::error::TryRecvError::Lagged(_)) => continue,
            Err(broadcast::error::TryRecvError::Closed) => {
                *monitor_receiver = None;
                return Ok(());
            }
        }
    }
}

async fn flush_pending_pubsub_messages(
    stream: &mut TcpStream,
    processor: &RequestProcessor,
    metrics: &ServerMetrics,
    client_id: ClientId,
    client_state: &ClientConnectionState,
) -> io::Result<()> {
    for message in processor.take_pending_pubsub_messages(client_id) {
        if client_state.reply_mode == ClientReplyMode::Off
            && !client_state.resp_protocol_version.is_resp3()
            && is_resp2_tracking_invalidation_message(&message)
        {
            continue;
        }
        stream.write_all(&message).await?;
        metrics.add_client_output_bytes(client_id, message.len() as u64);
    }
    Ok(())
}

fn is_resp2_tracking_invalidation_message(message: &[u8]) -> bool {
    message.starts_with(b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n")
}

#[inline]
fn map_routed_error_to_owner(error: RoutedExecutionError) -> OwnerThreadExecutionError {
    match error {
        RoutedExecutionError::Protocol => OwnerThreadExecutionError::Protocol,
        RoutedExecutionError::Request(request_error) => {
            OwnerThreadExecutionError::Request(request_error)
        }
    }
}

async fn execute_frame_on_owner_thread_async(
    processor: &Arc<RequestProcessor>,
    owner_thread_pool: &Arc<ShardOwnerThreadPool>,
    args: &[ArgSlice],
    command: CommandId,
    frame: &[u8],
    client_no_touch: bool,
    client_id: Option<ClientId>,
    selected_db: DbName,
) -> Result<OwnedExecutionOutcome, OwnerThreadExecutionError> {
    if command_is_scripting_family(command) {
        let owned_args =
            capture_owned_frame_args(frame, args).map_err(map_routed_error_to_owner)?;
        let task_processor = Arc::clone(processor);
        let result = tokio::task::spawn_blocking(move || {
            execute_owned_frame_args_via_processor(
                &task_processor,
                &owned_args,
                client_no_touch,
                client_id,
                selected_db,
            )
        })
        .await
        .map_err(|_| OwnerThreadExecutionError::OwnerThreadUnavailable)?;
        return result.map_err(map_routed_error_to_owner);
    }

    execute_frame_on_owner_thread(
        processor,
        owner_thread_pool,
        args,
        command,
        frame,
        client_no_touch,
        client_id,
        selected_db,
    )
}

#[allow(clippy::too_many_arguments)]
async fn execute_blocking_frame_on_owner_thread(
    processor: &Arc<RequestProcessor>,
    metrics: &Arc<ServerMetrics>,
    owner_thread_pool: &Arc<ShardOwnerThreadPool>,
    replication: &Arc<RedisReplicationCoordinator>,
    args: &[ArgSlice],
    command: CommandId,
    command_mutating: bool,
    frame: &[u8],
    client_no_touch: bool,
    client_id: ClientId,
    selected_db: DbName,
    wait_target_offset_for_connection: u64,
    local_aof_wait_target_offset_for_connection: u64,
    stream: &mut TcpStream,
    resumed_blocking_command_after_pause: bool,
) -> Result<BlockingExecutionOutcome, OwnerThreadExecutionError> {
    // TLA+ mapping (`formal/tla/specs/BlockingDisconnectLeak.tla`):
    // - ACTIVE: client has no wait-queue registration.
    // - BLOCKED: `register_blocking_wait` + `set_client_blocked(true)` applied.
    // - DISCONNECTED: socket close observed in `blocking_client_disconnected`.
    // Core actions:
    // - `Block(c,k)`: first transition into blocked path for current blocking keys.
    // - `PushWake/WakeHead(k)`: owner-thread execution returns non-empty response.
    // - `Disconnect(c)`: disconnect check branch; cleanup must unregister the waiter.
    let blocking_request = request_uses_blocking_wait(command, args);
    let deadline = blocking_timeout_deadline(command, args);
    let blocking_keys = blocking_wait_keys(command, args, selected_db);
    let mut blocked = false;
    // TLA+ : ResumeBlockingAfterPause
    // For commands that were pause-gated before entering this loop, resume with
    // the pause-blocked marker carried in from the connection path.
    let mut pause_blocked_marker_active = resumed_blocking_command_after_pause;
    if !resumed_blocking_command_after_pause {
        // TLA+ (`ClientPauseUnblockRace`) fixed path:
        // clear stale pending unblocks only for non-pause-resume entries.
        processor.clear_client_unblock_request(client_id);
    }
    loop {
        if metrics.is_client_killed(client_id) {
            clear_blocking_request_state(processor, metrics, client_id, blocked, &blocking_keys);
            if pause_blocked_marker_active {
                processor.unregister_pause_blocked_blocking_client(client_id);
            }
            return Ok(BlockingExecutionOutcome {
                frame_response: blocking_empty_response_for_command(
                    command,
                    processor.resp_protocol_version().is_resp3(),
                )
                .to_vec(),
                should_replicate: false,
                connection_effects: RequestConnectionEffects::default(),
                deferred_replication_frames: Vec::new(),
            });
        }
        if blocking_request && blocking_client_disconnected(stream).await {
            // `Disconnect(c)` branch: if we were blocked, this must clear all wait-queue state.
            clear_blocking_request_state(processor, metrics, client_id, blocked, &blocking_keys);
            if pause_blocked_marker_active {
                processor.unregister_pause_blocked_blocking_client(client_id);
            }
            return Ok(BlockingExecutionOutcome {
                frame_response: blocking_empty_response_for_command(
                    command,
                    processor.resp_protocol_version().is_resp3(),
                )
                .to_vec(),
                should_replicate: false,
                connection_effects: RequestConnectionEffects::default(),
                deferred_replication_frames: Vec::new(),
            });
        }

        // TLA+ : ObservePendingUnblock
        if blocked && let Some(unblock_mode) = processor.take_client_unblock_request(client_id) {
            clear_blocking_client_state(processor, metrics, client_id, &blocking_keys);
            let response = match unblock_mode {
                ClientUnblockMode::Timeout => blocking_empty_response_for_command(
                    command,
                    processor.resp_protocol_version().is_resp3(),
                )
                .to_vec(),
                ClientUnblockMode::Error => {
                    b"-UNBLOCKED client unblocked via CLIENT UNBLOCK\r\n".to_vec()
                }
            };
            if pause_blocked_marker_active {
                processor.unregister_pause_blocked_blocking_client(client_id);
            }
            return Ok(BlockingExecutionOutcome {
                frame_response: response,
                should_replicate: false,
                connection_effects: RequestConnectionEffects::default(),
                deferred_replication_frames: Vec::new(),
            });
        }

        if !processor.is_blocking_wait_turn(client_id, &blocking_keys) {
            if !blocked {
                blocked = true;
                // `Block(c,k)` critical section starts here.
                processor.register_blocking_wait(client_id, &blocking_keys);
                metrics.set_client_blocked(client_id, true);
                // TLA+ : AdvertiseBlockedClientAfterRegistration
                // Publish the blocked-client count only after the wait queue is
                // registered, so external blocked_clients observers cannot race
                // ahead of a not-yet-visible waiter.
                processor.increment_blocked_clients();
                if pause_blocked_marker_active {
                    processor.unregister_pause_blocked_blocking_client(client_id);
                    pause_blocked_marker_active = false;
                }
            }
            if let Some(deadline_time) = deadline {
                let now = Instant::now();
                if now >= deadline_time {
                    // TLA+ : BlockingTimeout
                    clear_blocking_client_state(processor, metrics, client_id, &blocking_keys);
                    if pause_blocked_marker_active {
                        processor.unregister_pause_blocked_blocking_client(client_id);
                    }
                    return Ok(BlockingExecutionOutcome {
                        frame_response: blocking_empty_response_for_command(
                            command,
                            processor.resp_protocol_version().is_resp3(),
                        )
                        .to_vec(),
                        should_replicate: false,
                        connection_effects: RequestConnectionEffects::default(),
                        deferred_replication_frames: Vec::new(),
                    });
                }
                let remaining = deadline_time.duration_since(now);
                sleep(std::cmp::min(
                    remaining,
                    BLOCKING_COMMAND_NON_TURN_POLL_INTERVAL,
                ))
                .await;
            } else {
                sleep(BLOCKING_COMMAND_NON_TURN_POLL_INTERVAL).await;
            }
            continue;
        }

        if blocked
            && blocking_request
            && !blocking_keys.is_empty()
            && !processor.blocking_wait_keys_ready(client_id, &blocking_keys)
        {
            if let Some(deadline_time) = deadline {
                let now = Instant::now();
                if now >= deadline_time {
                    clear_blocking_client_state(processor, metrics, client_id, &blocking_keys);
                    if pause_blocked_marker_active {
                        processor.unregister_pause_blocked_blocking_client(client_id);
                    }
                    return Ok(BlockingExecutionOutcome {
                        frame_response: blocking_empty_response_for_command(
                            command,
                            processor.resp_protocol_version().is_resp3(),
                        )
                        .to_vec(),
                        should_replicate: false,
                        connection_effects: RequestConnectionEffects::default(),
                        deferred_replication_frames: Vec::new(),
                    });
                }

                let remaining = deadline_time.duration_since(now);
                if blocking_empty_retry_should_sleep(command) {
                    sleep(std::cmp::min(
                        remaining,
                        BLOCKING_STREAM_EMPTY_POLL_INTERVAL,
                    ))
                    .await;
                } else {
                    sleep(std::cmp::min(
                        remaining,
                        BLOCKING_COMMAND_NON_TURN_POLL_INTERVAL,
                    ))
                    .await;
                }
            } else if blocking_empty_retry_should_sleep(command) {
                sleep(BLOCKING_STREAM_EMPTY_POLL_INTERVAL).await;
            } else {
                sleep(BLOCKING_COMMAND_NON_TURN_POLL_INTERVAL).await;
            }
            continue;
        }

        let pre_string_len =
            pre_string_len_for_mutation_detection(processor, command, args, selected_db);
        let owned_execution = match execute_frame_on_owner_thread_async(
            processor,
            owner_thread_pool,
            args,
            command,
            frame,
            client_no_touch,
            Some(client_id),
            selected_db,
        )
        .await
        {
            Ok(outcome) => outcome,
            Err(OwnerThreadExecutionError::Request(RequestExecutionError::WrongType))
                if blocked && ignore_wrongtype_while_blocked(command) =>
            {
                OwnedExecutionOutcome {
                    frame_response: blocking_empty_response_for_command(
                        command,
                        processor.resp_protocol_version().is_resp3(),
                    )
                    .to_vec(),
                    connection_effects: RequestConnectionEffects::default(),
                    deferred_replication_frames: Vec::new(),
                }
            }
            Err(error) => {
                clear_blocking_request_state(
                    processor,
                    metrics,
                    client_id,
                    blocked,
                    &blocking_keys,
                );
                if pause_blocked_marker_active {
                    processor.unregister_pause_blocked_blocking_client(client_id);
                }
                return Err(error);
            }
        };
        let mut frame_response = owned_execution.frame_response;
        let connection_effects = owned_execution.connection_effects;
        let deferred_replication_frames = owned_execution.deferred_replication_frames;
        materialize_wait_response_if_needed(
            replication,
            &mut frame_response,
            connection_effects,
            wait_target_offset_for_connection,
        )
        .await;
        materialize_waitaof_response_if_needed(
            processor,
            replication,
            &mut frame_response,
            connection_effects,
            wait_target_offset_for_connection,
            local_aof_wait_target_offset_for_connection,
        )
        .await;

        let should_replicate = if blocking_request {
            command_mutating && !is_blocking_empty_response(&frame_response)
        } else {
            command_mutating
                && command_effectively_mutated_for_replication(
                    processor,
                    command,
                    args,
                    &frame_response,
                    pre_string_len,
                )
        };
        if !blocking_request || !is_blocking_empty_response(&frame_response) {
            clear_blocking_request_state(processor, metrics, client_id, blocked, &blocking_keys);
            if pause_blocked_marker_active {
                processor.unregister_pause_blocked_blocking_client(client_id);
            }
            return Ok(BlockingExecutionOutcome {
                frame_response,
                should_replicate,
                connection_effects,
                deferred_replication_frames,
            });
        }

        if !blocked {
            blocked = true;
            processor.register_blocking_wait(client_id, &blocking_keys);
            metrics.set_client_blocked(client_id, true);
            // TLA+ : AdvertiseBlockedClientAfterRegistration
            processor.increment_blocked_clients();
            if pause_blocked_marker_active {
                processor.unregister_pause_blocked_blocking_client(client_id);
                pause_blocked_marker_active = false;
            }
        }

        if let Some(deadline_time) = deadline {
            let now = Instant::now();
            if now >= deadline_time {
                // TLA+ : BlockingTimeout
                clear_blocking_client_state(processor, metrics, client_id, &blocking_keys);
                if pause_blocked_marker_active {
                    processor.unregister_pause_blocked_blocking_client(client_id);
                }
                return Ok(BlockingExecutionOutcome {
                    frame_response,
                    should_replicate,
                    connection_effects,
                    deferred_replication_frames,
                });
            }

            let remaining = deadline_time.duration_since(now);
            if remaining > Duration::from_millis(0) {
                if blocking_empty_retry_should_sleep(command) {
                    sleep(std::cmp::min(
                        remaining,
                        BLOCKING_STREAM_EMPTY_POLL_INTERVAL,
                    ))
                    .await;
                } else {
                    yield_now().await;
                }
            }
        } else {
            if blocking_empty_retry_should_sleep(command) {
                sleep(BLOCKING_STREAM_EMPTY_POLL_INTERVAL).await;
            } else {
                yield_now().await;
            }
        }
    }
}

struct BlockingExecutionOutcome {
    frame_response: Vec<u8>,
    should_replicate: bool,
    connection_effects: RequestConnectionEffects,
    deferred_replication_frames: Vec<crate::request_lifecycle::DeferredReplicationFrame>,
}

fn blocking_empty_retry_should_sleep(command: CommandId) -> bool {
    matches!(command, CommandId::Xread | CommandId::Xreadgroup)
}

fn clear_blocking_client_state(
    processor: &RequestProcessor,
    metrics: &ServerMetrics,
    client_id: ClientId,
    blocking_keys: &[BlockingWaitKey],
) {
    // Shared cleanup for both successful wakeups and disconnect/unblock paths.
    // In TLA+ terms this is the queue-removal side of `Disconnect(c)` and wake completion.
    processor.unregister_blocking_wait(client_id, blocking_keys);
    processor.clear_blocked_stream_wait_state(client_id);
    processor.clear_blocked_xread_tail_ids(client_id);
    processor.clear_client_unblock_request(client_id);
    metrics.set_client_blocked(client_id, false);
    // TLA+ : WithdrawBlockedClientAfterCleanup
    // Drop the externally visible blocked-client count only after all queue
    // registration and waiter-local state is gone.
    processor.decrement_blocked_clients();
    processor.note_blocking_progress();
}

fn clear_blocking_request_state(
    processor: &RequestProcessor,
    metrics: &ServerMetrics,
    client_id: ClientId,
    blocked: bool,
    blocking_keys: &[BlockingWaitKey],
) {
    if blocked {
        clear_blocking_client_state(processor, metrics, client_id, blocking_keys);
        return;
    }
    processor.clear_blocked_stream_wait_state(client_id);
    processor.clear_blocked_xread_tail_ids(client_id);
}

async fn yield_for_blocking_progress(processor: &RequestProcessor, initial_blocked: u64) {
    if initial_blocked == 0 {
        return;
    }
    loop {
        let current = processor.blocked_clients();
        if current == 0 {
            return;
        }
        // TLA+ : ProducerAckWaitReady
        // TLA+ minimal guard (`LinkedBlmoveChainResidue_minimal`): producer ACK should
        // not be exposed while any queued blocking waiter is already ready to run.
        if !processor.has_ready_blocking_waiters() {
            return;
        }
        let observed_epoch = processor.blocking_progress_epoch();
        processor
            .wait_for_blocking_progress_after(observed_epoch)
            .await;
    }
}

const CLIENT_PAUSE_POLL_INTERVAL: Duration = Duration::from_millis(5);

/// Check whether any queued frame in a MULTI transaction is write-pause-affected.
fn transaction_has_write_pause_affected_commands(
    queued_frames: &[Vec<u8>],
    max_resp_arguments: usize,
) -> bool {
    let mut args = vec![ArgSlice::EMPTY; DEFAULT_RESP_ARG_SCRATCH.min(max_resp_arguments)];
    for frame in queued_frames {
        let Ok(meta) = parse_resp_command_arg_slices_dynamic(frame, &mut args, max_resp_arguments)
        else {
            continue;
        };
        if meta.argument_count == 0 {
            continue;
        }
        let command = unsafe { dispatch_from_arg_slices(&args[..meta.argument_count]) };
        let subcommand = if meta.argument_count > 1 {
            Some(arg_slice_bytes(&args[1]))
        } else {
            None
        };
        let script_body = if command == CommandId::Eval && meta.argument_count > 1 {
            Some(arg_slice_bytes(&args[1]))
        } else if command == CommandId::Script
            && meta.argument_count > 2
            && ascii_eq_ignore_case(arg_slice_bytes(&args[1]), b"LOAD")
        {
            Some(arg_slice_bytes(&args[2]))
        } else {
            None
        };
        if command_is_write_pause_affected_with_script(command, subcommand, script_body) {
            return true;
        }
    }
    false
}

/// Block the current connection until CLIENT PAUSE expires or is cancelled.
/// Increments blocked_clients while waiting so tests can observe it via INFO.
async fn wait_for_client_unpause(
    processor: &RequestProcessor,
    metrics: &ServerMetrics,
    client_id: ClientId,
    is_write_or_may_replicate: bool,
) {
    if !processor.is_client_paused(is_write_or_may_replicate) {
        return;
    }
    processor.increment_blocked_clients();
    metrics.set_client_blocked(client_id, true);
    loop {
        if !processor.is_client_paused(is_write_or_may_replicate) {
            break;
        }
        sleep(CLIENT_PAUSE_POLL_INTERVAL).await;
    }
    processor.decrement_blocked_clients();
    metrics.set_client_blocked(client_id, false);
}

fn command_may_wake_blocking_waiters(command: CommandId, args: &[ArgSlice]) -> bool {
    matches!(
        command,
        CommandId::Lpush
            | CommandId::Rpush
            | CommandId::Lpushx
            | CommandId::Rpushx
            | CommandId::Linsert
            | CommandId::Lmove
            | CommandId::Rpoplpush
            | CommandId::Zadd
            | CommandId::Zincrby
            | CommandId::Xadd
            | CommandId::Del
            | CommandId::Set
            | CommandId::Flushall
            | CommandId::Flushdb
            | CommandId::Rename
            | CommandId::Renamenx
            | CommandId::Copy
            | CommandId::Move
            | CommandId::Restore
            | CommandId::RestoreAsking
            | CommandId::Exec
    ) || (command == CommandId::Xgroup
        && args.get(1).is_some_and(|subcommand| {
            ascii_eq_ignore_case(arg_slice_bytes(subcommand), b"DESTROY")
        }))
}

async fn blocking_client_disconnected(stream: &mut TcpStream) -> bool {
    let mut probe = [0u8; 1];
    match tokio::time::timeout(Duration::from_millis(1), stream.peek(&mut probe)).await {
        Ok(Ok(0)) => true,
        Ok(Ok(_)) => false,
        Ok(Err(error)) => error.kind() != io::ErrorKind::WouldBlock,
        Err(_) => false,
    }
}

fn blocking_timeout_deadline(command: CommandId, args: &[ArgSlice]) -> Option<Instant> {
    if !request_uses_blocking_wait(command, args) {
        return None;
    }

    let timeout_seconds = match command {
        CommandId::Blmpop | CommandId::Bzmpop => parse_blocking_timeout_arg(args, 1)?,
        CommandId::Blmove | CommandId::Brpoplpush => {
            let timeout_index = args.len().checked_sub(1)?;
            parse_blocking_timeout_arg(args, timeout_index)?
        }
        CommandId::Blpop | CommandId::Brpop | CommandId::Bzpopmin | CommandId::Bzpopmax => {
            let timeout_index = args.len().checked_sub(1)?;
            parse_blocking_timeout_arg(args, timeout_index)?
        }
        CommandId::Xread | CommandId::Xreadgroup => {
            let timeout_millis = parse_xread_blocking_timeout_millis(command, args)?;
            timeout_millis as f64 / 1000.0
        }
        _ => return None,
    };

    if timeout_seconds <= 0.0 {
        return None;
    }

    Some(Instant::now() + Duration::from_secs_f64(timeout_seconds))
}

fn is_blocking_empty_response(frame_response: &[u8]) -> bool {
    frame_response == b"*-1\r\n" || frame_response == b"$-1\r\n" || frame_response == b"_\r\n"
}

fn blocking_empty_response_for_command(command: CommandId, resp3: bool) -> &'static [u8] {
    if resp3 {
        return b"_\r\n";
    }
    match command {
        CommandId::Blmove | CommandId::Brpoplpush => b"$-1\r\n",
        _ => b"*-1\r\n",
    }
}

fn is_blocking_command(command: CommandId) -> bool {
    matches!(
        command,
        CommandId::Blmpop
            | CommandId::Blpop
            | CommandId::Brpop
            | CommandId::Blmove
            | CommandId::Brpoplpush
            | CommandId::Bzpopmin
            | CommandId::Bzpopmax
            | CommandId::Bzmpop
            | CommandId::Xread
            | CommandId::Xreadgroup,
    )
}

fn request_uses_blocking_wait(command: CommandId, args: &[ArgSlice]) -> bool {
    match command {
        CommandId::Xread => parse_xread_blocking_timeout_millis(command, args).is_some(),
        CommandId::Xreadgroup => {
            parse_xread_blocking_timeout_millis(command, args).is_some()
                && xreadgroup_uses_blocking_special_id(args)
        }
        _ => is_blocking_command(command),
    }
}

fn command_disallowed_inside_multi(command: CommandId) -> bool {
    matches!(command, CommandId::Save | CommandId::Shutdown)
}

fn command_is_replication_passthrough(command_name: &[u8]) -> bool {
    ascii_eq_ignore_case(command_name, b"REPLICAOF")
        || ascii_eq_ignore_case(command_name, b"SLAVEOF")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransactionRuntimeAbortReason {
    ReadOnlyReplica,
    NoReplicas,
    MasterDown,
    Oom,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ScriptingReplicaExecutionFlags {
    effectively_mutating: bool,
    allow_stale: bool,
}

fn transaction_runtime_abort_reason(
    processor: &RequestProcessor,
    replication: &RedisReplicationCoordinator,
    selected_db: DbName,
    queued_frames: &[Vec<u8>],
    max_resp_arguments: usize,
) -> Result<Option<TransactionRuntimeAbortReason>, RequestExecutionError> {
    let mut has_mutating_command = false;
    let mut requires_good_replicas = false;
    let replica_mode = replication.is_replica_mode();
    let reject_reads_on_stale_replica =
        replica_mode && !processor.replica_serve_stale_data() && !replication.is_upstream_link_up();
    let mut args = vec![ArgSlice::EMPTY; 64.min(max_resp_arguments.max(1))];

    for frame in queued_frames {
        let Ok(meta) = parse_resp_command_arg_slices_dynamic(frame, &mut args, max_resp_arguments)
        else {
            continue;
        };
        if meta.bytes_consumed != frame.len() || meta.argument_count == 0 {
            continue;
        }
        let command_name = arg_slice_bytes(&args[0]);
        if command_is_replication_passthrough(command_name) {
            continue;
        }
        // SAFETY: parsed ArgSlice values reference bytes in `frame` for this scope.
        let command = unsafe { dispatch_from_arg_slices(&args[..meta.argument_count]) };
        let subcommand = if meta.argument_count > 1 {
            Some(arg_slice_bytes(&args[1]))
        } else {
            None
        };
        let replica_scripting_flags =
            scripting_replica_execution_flags(processor, command, &args[..meta.argument_count]);
        let command_mutating = replica_scripting_flags
            .map(|flags| flags.effectively_mutating)
            .unwrap_or_else(|| command_is_effectively_mutating(command, subcommand));
        if command_requires_good_replicas(processor, command, &args[..meta.argument_count]) {
            requires_good_replicas = true;
        }
        if reject_reads_on_stale_replica
            && replica_scripting_flags.is_some_and(|flags| !flags.allow_stale)
        {
            return Ok(Some(TransactionRuntimeAbortReason::MasterDown));
        }
        if command_mutating {
            has_mutating_command = true;
            if replica_mode {
                return Ok(Some(TransactionRuntimeAbortReason::ReadOnlyReplica));
            }
        } else if reject_reads_on_stale_replica {
            return Ok(Some(TransactionRuntimeAbortReason::MasterDown));
        }
    }

    if !has_mutating_command && !requires_good_replicas {
        return Ok(None);
    }
    let min_replicas_to_write = processor.min_replicas_to_write();
    if requires_good_replicas
        && min_replicas_to_write > 0
        && replication.downstream_replica_count() < min_replicas_to_write
    {
        return Ok(Some(TransactionRuntimeAbortReason::NoReplicas));
    }
    if processor.should_reject_mutating_command_for_maxmemory(selected_db, has_mutating_command)? {
        return Ok(Some(TransactionRuntimeAbortReason::Oom));
    }
    Ok(None)
}

fn command_requires_good_replicas(
    processor: &RequestProcessor,
    command: CommandId,
    args: &[ArgSlice],
) -> bool {
    match command {
        CommandId::Eval | CommandId::Evalsha => {
            eval_script_requires_good_replicas(processor, command, args)
        }
        CommandId::EvalRo | CommandId::EvalshaRo | CommandId::FcallRo => false,
        CommandId::Fcall => {
            let Some(function_name) = args.get(1) else {
                return true;
            };
            !processor.is_function_read_only(arg_slice_bytes(function_name))
        }
        _ => {
            let subcommand = if args.len() > 1 {
                Some(arg_slice_bytes(&args[1]))
            } else {
                None
            };
            command_is_effectively_mutating(command, subcommand)
        }
    }
}

fn scripting_replica_execution_flags(
    processor: &RequestProcessor,
    command: CommandId,
    args: &[ArgSlice],
) -> Option<ScriptingReplicaExecutionFlags> {
    match command {
        CommandId::Eval | CommandId::Evalsha | CommandId::EvalRo | CommandId::EvalshaRo => {
            let script_source = script_source_for_replication(processor, command, args)?;
            let flags = eval_script_shebang_flags(&script_source).ok()?;
            let effectively_mutating = match command {
                CommandId::Eval | CommandId::Evalsha => {
                    if flags.has_shebang {
                        !flags.no_writes
                    } else {
                        true
                    }
                }
                CommandId::EvalRo | CommandId::EvalshaRo => false,
                _ => unreachable!(),
            };
            Some(ScriptingReplicaExecutionFlags {
                effectively_mutating,
                allow_stale: flags.has_shebang && flags.allow_stale,
            })
        }
        CommandId::Fcall | CommandId::FcallRo => {
            let function_name = args.get(1)?;
            Some(ScriptingReplicaExecutionFlags {
                effectively_mutating: match command {
                    CommandId::Fcall => {
                        !processor.is_function_read_only(arg_slice_bytes(function_name))
                    }
                    CommandId::FcallRo => false,
                    _ => unreachable!(),
                },
                allow_stale: processor.function_allows_stale(arg_slice_bytes(function_name)),
            })
        }
        CommandId::Script
            if args
                .get(1)
                .is_some_and(|name| ascii_eq_ignore_case(arg_slice_bytes(name), b"LOAD")) =>
        {
            Some(ScriptingReplicaExecutionFlags {
                effectively_mutating: false,
                allow_stale: true,
            })
        }
        _ => None,
    }
}

fn eval_script_requires_good_replicas(
    processor: &RequestProcessor,
    command: CommandId,
    args: &[ArgSlice],
) -> bool {
    let Some(script_source) = script_source_for_replication(processor, command, args) else {
        return true;
    };
    let Ok(flags) = eval_script_shebang_flags(&script_source) else {
        return true;
    };
    if flags.has_shebang {
        return !flags.no_writes;
    }
    script_contains_mutating_call(&script_source)
}

fn transaction_runtime_execabort_message(reason: TransactionRuntimeAbortReason) -> &'static [u8] {
    match reason {
        TransactionRuntimeAbortReason::ReadOnlyReplica => {
            b"-EXECABORT Transaction discarded because of previous errors: READONLY You can't write against a read only replica.\r\n"
        }
        TransactionRuntimeAbortReason::NoReplicas => {
            b"-EXECABORT Transaction discarded because of previous errors: NOREPLICAS Not enough good replicas to write.\r\n"
        }
        TransactionRuntimeAbortReason::MasterDown => {
            b"-EXECABORT Transaction discarded because of previous errors: MASTERDOWN Link with MASTER is down and replica-serve-stale-data is set to 'no'.\r\n"
        }
        TransactionRuntimeAbortReason::Oom => {
            b"-EXECABORT Transaction discarded because of previous errors: OOM command not allowed when used memory > 'maxmemory'.\r\n"
        }
    }
}

fn publish_select_if_needed(replication: &RedisReplicationCoordinator, selected_db: DbName) {
    if !replication.consume_replication_select_needed_for_db(selected_db) {
        return;
    }
    let db_index = usize::from(selected_db);
    let db_index_text = db_index.to_string();
    let select_frame = encode_resp_frame_slices(&[b"SELECT", db_index_text.as_bytes()]);
    replication.publish_write_frame(&select_frame);
}

fn is_nested_lazy_expire_context(command: CommandId) -> bool {
    matches!(
        command,
        CommandId::Exec
            | CommandId::Eval
            | CommandId::EvalRo
            | CommandId::Evalsha
            | CommandId::EvalshaRo
            | CommandId::Fcall
            | CommandId::FcallRo,
    )
}

fn should_publish_lazy_expire_deletes(
    processor: &RequestProcessor,
    command: CommandId,
    command_mutating: bool,
) -> bool {
    if !command_mutating {
        return true;
    }
    if !is_nested_lazy_expire_context(command) {
        return false;
    }
    processor.lazyexpire_nested_arbitrary_keys_enabled()
}

async fn apply_queued_replication_transition(
    replication: &Arc<RedisReplicationCoordinator>,
    transition: QueuedReplicationTransition,
) {
    match transition {
        QueuedReplicationTransition::BecomeMaster => replication.become_master().await,
        QueuedReplicationTransition::BecomeReplica { host, port } => {
            replication.become_replica(host, port).await;
        }
    }
}

fn publish_transaction_replication_frames(
    processor: &RequestProcessor,
    replication: &RedisReplicationCoordinator,
    transaction_outcome: &TransactionExecutionOutcome,
    max_resp_arguments: usize,
) -> Option<crate::redis_replication::PublishedWriteFrontiers> {
    if transaction_outcome.pending_replication_transition.is_some() {
        return None;
    }

    let mut args = vec![ArgSlice::EMPTY; 64.min(max_resp_arguments.max(1))];
    let mut replication_frames: Vec<(DbName, Vec<u8>)> = Vec::new();
    for item in &transaction_outcome.items {
        if item.frame.is_empty() {
            continue;
        }
        if resp_is_error(&item.response) && item.deferred_replication_frames.is_empty() {
            continue;
        }
        let Ok(meta) =
            parse_resp_command_arg_slices_dynamic(&item.frame, &mut args, max_resp_arguments)
        else {
            continue;
        };
        if meta.bytes_consumed != item.frame.len() || meta.argument_count == 0 {
            continue;
        }
        let command_name = arg_slice_bytes(&args[0]);
        if command_is_replication_passthrough(command_name) {
            continue;
        }
        // SAFETY: parsed ArgSlice values reference bytes in `item.frame` for this scope.
        let command = unsafe { dispatch_from_arg_slices(&args[..meta.argument_count]) };
        let subcommand = if meta.argument_count > 1 {
            Some(arg_slice_bytes(&args[1]))
        } else {
            None
        };

        if command == CommandId::Xreadgroup {
            let frames =
                xreadgroup_replication_frames(&args[..meta.argument_count], &item.response);
            replication_frames.extend(frames.into_iter().map(|frame| (item.selected_db, frame)));
            continue;
        }
        if command == CommandId::Script
            && (subcommand.is_some_and(|value| ascii_eq_ignore_case(value, b"LOAD"))
                || subcommand.is_some_and(|value| ascii_eq_ignore_case(value, b"FLUSH")))
        {
            continue;
        }

        if let Some(eval_effect_frame) =
            script_eval_effect_replication_frame(processor, command, &args[..meta.argument_count])
        {
            replication_frames.push((item.selected_db, eval_effect_frame));
            continue;
        }

        if !item.deferred_replication_frames.is_empty() {
            replication_frames.extend(
                item.deferred_replication_frames
                    .iter()
                    .cloned()
                    .map(|effect| (effect.selected_db, effect.frame)),
            );
            continue;
        }

        let command_mutating = command_is_effectively_mutating(command, subcommand);
        let always_replicate = matches!(command, CommandId::Publish | CommandId::Spublish);
        if !(always_replicate
            || (command_mutating && transaction_command_had_effect(command, &item.response)))
        {
            continue;
        }
        let Some(replication_frame) = replication_frame_for_command(
            processor,
            item.selected_db,
            command,
            &args[..meta.argument_count],
            &item.response,
            &item.frame,
        ) else {
            continue;
        };
        replication_frames.push((item.selected_db, replication_frame));
    }

    let lazy_expired_keys = processor.take_lazy_expired_keys_for_replication();
    if should_publish_lazy_expire_deletes(processor, CommandId::Exec, true) {
        for expired in lazy_expired_keys {
            replication_frames.push((
                expired.db,
                encode_resp_frame_slices(&[b"DEL", expired.key.as_slice()]),
            ));
        }
    }

    if replication_frames.is_empty() {
        return None;
    }

    if replication_frames.len() > 1 {
        let multi_frame = encode_resp_frame_slices(&[b"MULTI"]);
        processor.record_rdb_change(1);
        let _ = replication.publish_write_frame(&multi_frame);
    }

    let mut last_published = None;
    for (selected_db, replication_frame) in &replication_frames {
        publish_select_if_needed(replication, *selected_db);
        processor.record_rdb_change(1);
        let published = replication.publish_write_frame(replication_frame);
        last_published = Some(published);
    }

    if replication_frames.len() > 1 {
        let exec_frame = encode_resp_frame_slices(&[b"EXEC"]);
        processor.record_rdb_change(1);
        let published = replication.publish_write_frame(&exec_frame);
        last_published = Some(published);
    }
    last_published
}

fn command_uses_script_effect_replication(command: CommandId) -> bool {
    matches!(
        command,
        CommandId::Eval
            | CommandId::EvalRo
            | CommandId::Evalsha
            | CommandId::EvalshaRo
            | CommandId::Fcall
            | CommandId::FcallRo
    )
}

fn xreadgroup_replication_frames(args: &[ArgSlice], frame_response: &[u8]) -> Vec<Vec<u8>> {
    if frame_response == b"*0\r\n" {
        return Vec::new();
    }
    if args.len() < 7 {
        return Vec::new();
    }
    let group = arg_slice_bytes(&args[2]);
    let consumer = arg_slice_bytes(&args[3]);

    let mut count = 1usize;
    let mut key: Option<&[u8]> = None;
    let mut index = 4usize;
    while index < args.len() {
        let token = arg_slice_bytes(&args[index]);
        if ascii_eq_ignore_case(token, b"NOACK") {
            index += 1;
            continue;
        }
        if ascii_eq_ignore_case(token, b"COUNT") {
            let Some(value_arg) = args.get(index + 1) else {
                break;
            };
            count = parse_ascii_usize(arg_slice_bytes(value_arg))
                .unwrap_or(1)
                .max(1);
            index += 2;
            continue;
        }
        if ascii_eq_ignore_case(token, b"STREAMS") {
            let Some(key_arg) = args.get(index + 1) else {
                break;
            };
            key = Some(arg_slice_bytes(key_arg));
            break;
        }
        break;
    }

    let Some(key) = key else {
        return Vec::new();
    };

    let mut frames = Vec::with_capacity(count.saturating_add(1));
    for _ in 0..count {
        frames.push(encode_resp_frame_slices(&[
            b"XCLAIM", key, group, consumer, b"0", b"0-0",
        ]));
    }
    frames.push(encode_resp_frame_slices(&[
        b"XGROUP",
        b"SETID",
        key,
        group,
        b"0",
        b"ENTRIESREAD",
        b"0",
    ]));
    frames
}

fn take_script_effect_replication_frames(
    processor: &RequestProcessor,
    max_resp_arguments: usize,
) -> Vec<Vec<u8>> {
    let effects = processor.take_script_replication_effects();
    if effects.is_empty() {
        return Vec::new();
    }

    let mut args = vec![ArgSlice::EMPTY; 64.min(max_resp_arguments.max(1))];
    let mut replication_frames = Vec::new();
    for effect in effects {
        if effect.frame.is_empty() || resp_is_error(&effect.response) {
            continue;
        }
        let Ok(meta) =
            parse_resp_command_arg_slices_dynamic(&effect.frame, &mut args, max_resp_arguments)
        else {
            continue;
        };
        if meta.bytes_consumed != effect.frame.len() || meta.argument_count == 0 {
            continue;
        }

        let subcommand = if meta.argument_count > 1 {
            Some(arg_slice_bytes(&args[1]))
        } else {
            None
        };
        let command_mutating = command_is_effectively_mutating(effect.command, subcommand);
        if !(command_mutating && transaction_command_had_effect(effect.command, &effect.response)) {
            continue;
        }

        let Some(replication_frame) = script_effect_replication_frame_for_command(
            processor,
            effect.selected_db,
            effect.command,
            &args[..meta.argument_count],
            &effect.response,
            &effect.frame,
        ) else {
            continue;
        };
        replication_frames.push(replication_frame);
    }
    replication_frames
}

fn script_effect_replication_frame_for_command(
    processor: &RequestProcessor,
    selected_db: DbName,
    command: CommandId,
    args: &[ArgSlice],
    frame_response: &[u8],
    original_frame: &[u8],
) -> Option<Vec<u8>> {
    match command {
        CommandId::Spop => {
            rewrite_script_spop_replication_frame(args, frame_response, original_frame)
        }
        CommandId::Expire | CommandId::Pexpire | CommandId::Expireat | CommandId::Pexpireat => {
            rewrite_script_expire_family_replication_frame(
                processor,
                selected_db,
                args,
                frame_response,
            )
        }
        CommandId::Incrbyfloat => {
            rewrite_script_incrbyfloat_replication_frame(args, frame_response, original_frame)
        }
        _ => replication_frame_for_command(
            processor,
            selected_db,
            command,
            args,
            frame_response,
            original_frame,
        ),
    }
}

fn script_eval_effect_replication_frame(
    processor: &RequestProcessor,
    command: CommandId,
    args: &[ArgSlice],
) -> Option<Vec<u8>> {
    if args.len() < 4 {
        return None;
    }
    let script: Cow<'_, [u8]> = match command {
        CommandId::Eval | CommandId::EvalRo => Cow::Borrowed(arg_slice_bytes(&args[1])),
        CommandId::Evalsha | CommandId::EvalshaRo => {
            let sha = arg_slice_bytes(&args[1]);
            Cow::Owned(processor.cached_script_for_sha(sha)?)
        }
        _ => return None,
    };
    let script_text = String::from_utf8_lossy(script.as_ref()).to_string();
    let script_text_lower = script_text.to_ascii_lowercase();
    if !script_text_lower.contains("redis.call('set'")
        && !script_text_lower.contains("redis.call(\"set\"")
    {
        return None;
    }

    let key_count = parse_ascii_usize(arg_slice_bytes(&args[2]))?;
    if key_count == 0 || args.len() < 3 + key_count {
        return None;
    }
    let key = arg_slice_bytes(&args[3]);
    let value: Cow<'_, [u8]> = if script_text_lower.contains("argv[1]") {
        Cow::Borrowed(arg_slice_bytes(args.get(3 + key_count)?))
    } else {
        Cow::Owned(extract_script_set_literal(
            &script_text,
            &script_text_lower,
        )?)
    };
    Some(encode_resp_frame_slices(&[b"SET", key, value.as_ref()]))
}

fn extract_script_set_literal(script: &str, script_lower: &str) -> Option<Vec<u8>> {
    for marker in ["keys[1], '", "keys[1],'", "keys[1], \"", "keys[1],\""] {
        let Some(start) = script_lower.find(marker) else {
            continue;
        };
        let value_start = start + marker.len();
        let delimiter = if marker.ends_with('"') { '"' } else { '\'' };
        let tail = &script[value_start..];
        let end = tail.find(delimiter)?;
        return Some(tail.as_bytes()[..end].to_vec());
    }
    None
}

fn parse_ascii_usize(value: &[u8]) -> Option<usize> {
    std::str::from_utf8(value).ok()?.parse::<usize>().ok()
}

fn transaction_command_had_effect(command: CommandId, response: &[u8]) -> bool {
    match command {
        CommandId::Del | CommandId::Unlink => parse_resp_integer(response).unwrap_or(0) > 0,
        CommandId::Spop => parse_spop_response_members(response)
            .map(|members| !members.is_empty())
            .unwrap_or(true),
        _ => true,
    }
}

#[inline]
fn resp_is_error(response: &[u8]) -> bool {
    response.first().copied() == Some(b'-')
}

fn watched_keys_dirty_or_expired(
    processor: &RequestProcessor,
    watched_keys: &[WatchedKey],
) -> bool {
    processor.refresh_watched_keys_before_exec(watched_keys);
    !processor.watch_versions_match(watched_keys)
}

fn ignore_wrongtype_while_blocked(command: CommandId) -> bool {
    matches!(
        command,
        CommandId::Blpop
            | CommandId::Brpop
            | CommandId::Blmpop
            | CommandId::Bzpopmin
            | CommandId::Bzpopmax
            | CommandId::Bzmpop
            | CommandId::Xread
    )
}

fn blocking_wait_keys(
    command: CommandId,
    args: &[ArgSlice],
    selected_db: DbName,
) -> Vec<BlockingWaitKey> {
    match command {
        CommandId::Blpop | CommandId::Brpop | CommandId::Bzpopmin | CommandId::Bzpopmax => {
            if args.len() < 3 {
                return Vec::new();
            }
            args[1..args.len() - 1]
                .iter()
                .map(|arg| {
                    BlockingWaitKey::new(
                        selected_db,
                        // SAFETY: The argument slice was parsed from a live request frame and stays valid
                        // while the request is being executed.
                        RedisKey::from(arg_slice_bytes(arg)),
                        if matches!(command, CommandId::Bzpopmin | CommandId::Bzpopmax) {
                            BlockingWaitClass::Zset
                        } else {
                            BlockingWaitClass::List
                        },
                    )
                })
                .collect()
        }
        CommandId::Blmove | CommandId::Brpoplpush => {
            if args.len() < 2 {
                return Vec::new();
            }
            vec![BlockingWaitKey::new(
                selected_db,
                // SAFETY: The argument slice was parsed from a live request frame and stays valid
                // while the request is being executed.
                RedisKey::from(arg_slice_bytes(&args[1])),
                BlockingWaitClass::List,
            )]
        }
        CommandId::Blmpop | CommandId::Bzmpop => {
            if args.len() < 5 {
                return Vec::new();
            }
            // SAFETY: The argument slice was parsed from a live request frame and stays valid
            // while the request is being executed.
            let numkeys = parse_u64_ascii(arg_slice_bytes(&args[2])).unwrap_or(0) as usize;
            if numkeys == 0 {
                return Vec::new();
            }
            let start = 3usize;
            let end = start.saturating_add(numkeys).min(args.len());
            if start >= end {
                return Vec::new();
            }
            args[start..end]
                .iter()
                .map(|arg| {
                    BlockingWaitKey::new(
                        selected_db,
                        // SAFETY: The argument slice was parsed from a live request frame and stays valid
                        // while the request is being executed.
                        RedisKey::from(arg_slice_bytes(arg)),
                        if command == CommandId::Bzmpop {
                            BlockingWaitClass::Zset
                        } else {
                            BlockingWaitClass::List
                        },
                    )
                })
                .collect()
        }
        CommandId::Xread => xread_blocking_wait_keys(args, selected_db),
        CommandId::Xreadgroup => xreadgroup_blocking_wait_keys(args, selected_db),
        _ => Vec::new(),
    }
}

fn parse_xread_blocking_timeout_millis(command: CommandId, args: &[ArgSlice]) -> Option<u64> {
    let mut index = match command {
        CommandId::Xread => 1usize,
        CommandId::Xreadgroup => 4usize,
        _ => return None,
    };
    while index < args.len() {
        let token = arg_slice_bytes(args.get(index)?);
        if command == CommandId::Xreadgroup && ascii_eq_ignore_case(token, b"NOACK") {
            index += 1;
            continue;
        }
        if ascii_eq_ignore_case(token, b"COUNT")
            || (command == CommandId::Xreadgroup && ascii_eq_ignore_case(token, b"CLAIM"))
        {
            index = index.checked_add(2)?;
            continue;
        }
        if ascii_eq_ignore_case(token, b"BLOCK") {
            return parse_u64_ascii(arg_slice_bytes(args.get(index + 1)?));
        }
        if ascii_eq_ignore_case(token, b"STREAMS") {
            return None;
        }
        return None;
    }
    None
}

fn xreadgroup_uses_blocking_special_id(args: &[ArgSlice]) -> bool {
    let Some(streams_index) = find_xreadgroup_streams_index(args) else {
        return false;
    };
    let trailing = args.len().saturating_sub(streams_index);
    if trailing < 2 || !trailing.is_multiple_of(2) {
        return false;
    }
    let stream_count = trailing / 2;
    args[streams_index + stream_count..]
        .iter()
        .all(|arg| arg_slice_bytes(arg) == b">")
}

fn xread_blocking_wait_keys(args: &[ArgSlice], selected_db: DbName) -> Vec<BlockingWaitKey> {
    if parse_xread_blocking_timeout_millis(CommandId::Xread, args).is_none() {
        return Vec::new();
    }
    let Some(streams_index) = find_xread_streams_index(args) else {
        return Vec::new();
    };
    let trailing = args.len().saturating_sub(streams_index);
    if trailing < 2 || !trailing.is_multiple_of(2) {
        return Vec::new();
    }
    let stream_count = trailing / 2;
    args[streams_index..streams_index + stream_count]
        .iter()
        .map(|arg| {
            BlockingWaitKey::new(
                selected_db,
                RedisKey::from(arg_slice_bytes(arg)),
                BlockingWaitClass::Stream,
            )
        })
        .collect()
}

fn xreadgroup_blocking_wait_keys(args: &[ArgSlice], selected_db: DbName) -> Vec<BlockingWaitKey> {
    if !xreadgroup_uses_blocking_special_id(args) {
        return Vec::new();
    }
    let Some(streams_index) = find_xreadgroup_streams_index(args) else {
        return Vec::new();
    };
    let trailing = args.len().saturating_sub(streams_index);
    if trailing < 2 || !trailing.is_multiple_of(2) {
        return Vec::new();
    }
    let stream_count = trailing / 2;
    args[streams_index..streams_index + stream_count]
        .iter()
        .map(|arg| {
            BlockingWaitKey::new(
                selected_db,
                RedisKey::from(arg_slice_bytes(arg)),
                BlockingWaitClass::Stream,
            )
        })
        .collect()
}

fn find_xread_streams_index(args: &[ArgSlice]) -> Option<usize> {
    let mut index = 1usize;
    while index < args.len() {
        let token = arg_slice_bytes(args.get(index)?);
        if ascii_eq_ignore_case(token, b"COUNT") || ascii_eq_ignore_case(token, b"BLOCK") {
            index = index.checked_add(2)?;
            continue;
        }
        if ascii_eq_ignore_case(token, b"STREAMS") {
            return index.checked_add(1);
        }
        return None;
    }
    None
}

fn find_xreadgroup_streams_index(args: &[ArgSlice]) -> Option<usize> {
    let mut index = 4usize;
    while index < args.len() {
        let token = arg_slice_bytes(args.get(index)?);
        if ascii_eq_ignore_case(token, b"NOACK") {
            index += 1;
            continue;
        }
        if ascii_eq_ignore_case(token, b"COUNT")
            || ascii_eq_ignore_case(token, b"BLOCK")
            || ascii_eq_ignore_case(token, b"CLAIM")
        {
            index = index.checked_add(2)?;
            continue;
        }
        if ascii_eq_ignore_case(token, b"STREAMS") {
            return index.checked_add(1);
        }
        return None;
    }
    None
}

fn parse_blocking_timeout_arg(args: &[ArgSlice], timeout_index: usize) -> Option<f64> {
    // SAFETY: The argument slice was parsed from a live request frame and stays valid
    // while the request is being executed.
    let timeout_slice = arg_slice_bytes(args.get(timeout_index)?);
    if timeout_slice.is_empty() {
        return None;
    }

    let timeout = std::str::from_utf8(timeout_slice)
        .ok()?
        .parse::<f64>()
        .ok()?;

    if !timeout.is_finite() || timeout < 0.0 {
        return None;
    }

    Some(timeout)
}

fn handle_client_command(
    processor: &RequestProcessor,
    metrics: &ServerMetrics,
    client_id: ClientId,
    args: &[ArgSlice],
    client_state: &mut ClientConnectionState,
    response_out: &mut Vec<u8>,
) -> ClientCommandOutcome {
    let mut outcome = ClientCommandOutcome::default();
    if args.len() < 2 {
        append_wrong_arity_error_for_command(response_out, CommandId::Client);
        return outcome;
    }

    // SAFETY: `args` points to the current request frame.
    let subcommand = arg_slice_bytes(&args[1]);
    if ascii_eq_ignore_case(subcommand, b"HELP") {
        if args.len() != 2 {
            append_client_subcommand_wrong_arity(response_out, b"help");
            return outcome;
        }
        append_bulk_string_array_frame(response_out, &CLIENT_HELP_LINES);
        return outcome;
    }

    if ascii_eq_ignore_case(subcommand, b"ID") {
        if args.len() != 2 {
            append_client_subcommand_wrong_arity(response_out, b"id");
            return outcome;
        }
        let id = i64::try_from(u64::from(client_id)).unwrap_or(i64::MAX);
        append_integer_frame(response_out, id);
        return outcome;
    }

    if ascii_eq_ignore_case(subcommand, b"GETNAME") {
        if args.len() != 2 {
            append_client_subcommand_wrong_arity(response_out, b"getname");
            return outcome;
        }
        if let Some(name) = metrics.client_name(client_id) {
            append_bulk_string_frame(response_out, &name);
        } else {
            response_out.extend_from_slice(b"$-1\r\n");
        }
        return outcome;
    }

    if ascii_eq_ignore_case(subcommand, b"INFO") {
        if args.len() != 2 {
            append_client_subcommand_wrong_arity(response_out, b"info");
            return outcome;
        }
        let payload = metrics
            .render_client_info_payload(client_id)
            .unwrap_or_else(|| b"id=0".to_vec());
        append_bulk_string_frame(response_out, &payload);
        return outcome;
    }

    if ascii_eq_ignore_case(subcommand, b"SETNAME") {
        if args.len() != 3 {
            append_client_subcommand_wrong_arity(response_out, b"setname");
            return outcome;
        }
        let new_name = arg_slice_bytes(&args[2]);
        if contains_space_or_newline(new_name) {
            append_error_line(
                response_out,
                b"ERR Client names cannot contain spaces or newlines",
            );
            return outcome;
        }
        let name_value = if new_name.is_empty() {
            None
        } else {
            Some(new_name.to_vec())
        };
        metrics.set_client_name(client_id, name_value);
        append_simple_string(response_out, b"OK");
        return outcome;
    }

    if ascii_eq_ignore_case(subcommand, b"SETINFO") {
        if args.len() != 4 {
            append_client_subcommand_wrong_arity(response_out, b"setinfo");
            return outcome;
        }
        let option = arg_slice_bytes(&args[2]);
        let value = arg_slice_bytes(&args[3]);
        if contains_newline(value) {
            append_error_line(
                response_out,
                b"ERR CLIENT SETINFO value cannot contain newlines",
            );
            return outcome;
        }
        if ascii_eq_ignore_case(option, b"LIB-NAME") {
            if value.contains(&b' ') {
                append_error_line(
                    response_out,
                    b"ERR CLIENT SETINFO lib-name cannot contain spaces",
                );
                return outcome;
            }
            let new_value = if value.is_empty() {
                None
            } else {
                Some(value.to_vec())
            };
            metrics.set_client_library_name(client_id, new_value);
            append_simple_string(response_out, b"OK");
            return outcome;
        }
        if ascii_eq_ignore_case(option, b"LIB-VER") {
            let new_value = if value.is_empty() {
                None
            } else {
                Some(value.to_vec())
            };
            metrics.set_client_library_version(client_id, new_value);
            append_simple_string(response_out, b"OK");
            return outcome;
        }
        append_error_line(response_out, b"ERR Unrecognized option for CLIENT SETINFO");
        return outcome;
    }

    if ascii_eq_ignore_case(subcommand, b"LIST") {
        let mut filter_id: Option<ClientId> = None;
        if args.len() > 2 {
            if args.len() != 4 {
                response_out.extend_from_slice(b"-ERR syntax error\r\n");
                return outcome;
            }
            // SAFETY: `args` points to the current request frame.
            let option = arg_slice_bytes(&args[2]);
            if !ascii_eq_ignore_case(option, b"ID") {
                response_out.extend_from_slice(b"-ERR syntax error\r\n");
                return outcome;
            }
            // SAFETY: `args` points to the current request frame.
            let id_arg = arg_slice_bytes(&args[3]);
            let Some(parsed_id) = parse_u64_ascii(id_arg) else {
                response_out.extend_from_slice(b"-ERR value is not an integer or out of range\r\n");
                return outcome;
            };
            filter_id = Some(ClientId::new(parsed_id));
        }
        let payload = metrics.render_client_list_payload(filter_id);
        append_bulk_string_frame(response_out, &payload);
        return outcome;
    }

    if ascii_eq_ignore_case(subcommand, b"KILL") {
        if args.len() < 3 {
            append_client_subcommand_wrong_arity(response_out, b"kill");
            return outcome;
        }
        let mut filter = ClientKillFilter::default();
        let mut legacy_addr = false;

        let first = arg_slice_bytes(&args[2]);
        if args.len() == 3 && !is_client_kill_option(first) {
            legacy_addr = true;
            filter.addr = Some(first.to_vec());
        } else {
            let options_len = args.len() - 2;
            if !options_len.is_multiple_of(2) {
                response_out.extend_from_slice(b"-ERR syntax error\r\n");
                return outcome;
            }
            let mut index = 2usize;
            while index + 1 < args.len() {
                let option = arg_slice_bytes(&args[index]);
                let value = arg_slice_bytes(&args[index + 1]);
                if ascii_eq_ignore_case(option, b"ID") {
                    let Some(parsed_id) = parse_i64_ascii(value) else {
                        append_error_line(response_out, b"ERR client-id should be greater than 0");
                        return outcome;
                    };
                    if parsed_id <= 0 {
                        append_error_line(response_out, b"ERR client-id should be greater than 0");
                        return outcome;
                    }
                    filter.id = Some(ClientId::new(parsed_id as u64));
                } else if ascii_eq_ignore_case(option, b"TYPE") {
                    if ascii_eq_ignore_case(value, b"NORMAL") {
                        filter.client_type = Some(ClientTypeFilter::Normal);
                    } else if ascii_eq_ignore_case(value, b"MASTER") {
                        filter.client_type = Some(ClientTypeFilter::Master);
                    } else if ascii_eq_ignore_case(value, b"REPLICA")
                        || ascii_eq_ignore_case(value, b"SLAVE")
                    {
                        filter.client_type = Some(ClientTypeFilter::Replica);
                    } else if ascii_eq_ignore_case(value, b"PUBSUB") {
                        filter.client_type = Some(ClientTypeFilter::Pubsub);
                    } else {
                        append_error_line(response_out, b"ERR Unknown client type");
                        return outcome;
                    }
                } else if ascii_eq_ignore_case(option, b"USER") {
                    filter.user = Some(value.to_vec());
                } else if ascii_eq_ignore_case(option, b"ADDR") {
                    filter.addr = Some(value.to_vec());
                } else if ascii_eq_ignore_case(option, b"LADDR") {
                    filter.laddr = Some(value.to_vec());
                } else if ascii_eq_ignore_case(option, b"SKIPME") {
                    if ascii_eq_ignore_case(value, b"YES") {
                        filter.skip_current_connection = true;
                    } else if ascii_eq_ignore_case(value, b"NO") {
                        filter.skip_current_connection = false;
                    } else {
                        response_out.extend_from_slice(b"-ERR syntax error\r\n");
                        return outcome;
                    }
                } else if ascii_eq_ignore_case(option, b"MAXAGE") {
                    let Some(max_age) = parse_i64_ascii(value) else {
                        append_error_line(
                            response_out,
                            b"ERR maxage is not an integer or out of range",
                        );
                        return outcome;
                    };
                    if max_age <= 0 {
                        append_error_line(response_out, b"ERR maxage should be greater than 0");
                        return outcome;
                    }
                    filter.max_age_seconds = Some(max_age as u64);
                } else {
                    response_out.extend_from_slice(b"-ERR syntax error\r\n");
                    return outcome;
                }
                index += 2;
            }
        }

        if let Some(user) = filter.user.as_ref()
            && !metrics.acl_user_exists(user)
        {
            append_error_line(response_out, b"ERR No such user");
            return outcome;
        }

        let killed_clients = metrics.kill_clients(client_id, &filter);
        if legacy_addr && killed_clients.is_empty() {
            append_error_line(response_out, b"ERR No such client");
            return outcome;
        }
        if killed_clients.contains(&client_id) {
            outcome.disconnect_after_reply = true;
        }
        append_integer_frame(response_out, killed_clients.len() as i64);
        return outcome;
    }

    if ascii_eq_ignore_case(subcommand, b"UNBLOCK") {
        if args.len() != 3 && args.len() != 4 {
            append_client_subcommand_wrong_arity(response_out, b"unblock");
            return outcome;
        }
        // SAFETY: `args` points to the current request frame.
        let id_arg = arg_slice_bytes(&args[2]);
        let Some(target_client_id_raw) = parse_u64_ascii(id_arg) else {
            response_out.extend_from_slice(b"-ERR value is not an integer or out of range\r\n");
            return outcome;
        };
        let target_client_id = ClientId::new(target_client_id_raw);
        let unblock_mode = if args.len() == 4 {
            // SAFETY: `args` points to the current request frame.
            let mode_arg = arg_slice_bytes(&args[3]);
            if ascii_eq_ignore_case(mode_arg, b"TIMEOUT") {
                ClientUnblockMode::Timeout
            } else if ascii_eq_ignore_case(mode_arg, b"ERROR") {
                ClientUnblockMode::Error
            } else {
                response_out.extend_from_slice(b"-ERR syntax error\r\n");
                return outcome;
            }
        } else {
            ClientUnblockMode::Timeout
        };
        let target_is_blocked = metrics.is_client_blocked(target_client_id)
            || processor.is_pause_blocked_blocking_client(target_client_id);
        let unblocked = target_client_id != client_id
            && target_is_blocked
            && processor.request_client_unblock(target_client_id, unblock_mode);
        append_integer_frame(response_out, if unblocked { 1 } else { 0 });
        return outcome;
    }

    if ascii_eq_ignore_case(subcommand, b"PAUSE") {
        if args.len() != 3 && args.len() != 4 {
            append_client_subcommand_wrong_arity(response_out, b"pause");
            return outcome;
        }
        let timeout_arg = arg_slice_bytes(&args[2]);
        let Some(timeout) = parse_i64_ascii(timeout_arg) else {
            append_error_line(
                response_out,
                b"ERR timeout is not an integer or out of range",
            );
            return outcome;
        };
        if timeout < 0 {
            append_error_line(response_out, b"ERR timeout is negative");
            return outcome;
        }
        let pause_all = if args.len() == 4 {
            let mode = arg_slice_bytes(&args[3]);
            if ascii_eq_ignore_case(mode, b"ALL") {
                true
            } else if ascii_eq_ignore_case(mode, b"WRITE") {
                false
            } else {
                response_out.extend_from_slice(b"-ERR syntax error\r\n");
                return outcome;
            }
        } else {
            true
        };
        processor.client_pause(timeout as u64, pause_all);
        append_simple_string(response_out, b"OK");
        return outcome;
    }

    if ascii_eq_ignore_case(subcommand, b"UNPAUSE") {
        if args.len() != 2 {
            append_client_subcommand_wrong_arity(response_out, b"unpause");
            return outcome;
        }
        processor.client_unpause();
        append_simple_string(response_out, b"OK");
        return outcome;
    }

    if ascii_eq_ignore_case(subcommand, b"REPLY") {
        if args.len() != 3 {
            append_client_subcommand_wrong_arity(response_out, b"reply");
            return outcome;
        }
        let mode = arg_slice_bytes(&args[2]);
        if ascii_eq_ignore_case(mode, b"ON") {
            client_state.reply_mode = ClientReplyMode::On;
            if !client_state.resp_protocol_version.is_resp3() {
                processor.clear_pending_tracking_messages_for_client(client_id);
            }
            append_simple_string(response_out, b"OK");
            outcome.reply_behavior = ClientCommandReplyBehavior::ForceReply;
            return outcome;
        }
        if ascii_eq_ignore_case(mode, b"OFF") {
            client_state.reply_mode = ClientReplyMode::Off;
            if !client_state.resp_protocol_version.is_resp3() {
                processor.clear_pending_tracking_messages_for_client(client_id);
            }
            append_simple_string(response_out, b"OK");
            outcome.reply_behavior = ClientCommandReplyBehavior::Suppress;
            return outcome;
        }
        if ascii_eq_ignore_case(mode, b"SKIP") {
            client_state.reply_mode = ClientReplyMode::SkipNext;
            append_simple_string(response_out, b"OK");
            outcome.reply_behavior = ClientCommandReplyBehavior::Suppress;
            return outcome;
        }
        response_out.extend_from_slice(b"-ERR syntax error\r\n");
        return outcome;
    }

    if ascii_eq_ignore_case(subcommand, b"TRACKING") {
        if args.len() < 3 {
            append_client_subcommand_wrong_arity(response_out, b"tracking");
            return outcome;
        }
        let mode = arg_slice_bytes(&args[2]);
        if ascii_eq_ignore_case(mode, b"ON") {
            let mut seen_optin = false;
            let mut seen_optout = false;
            let mut bcast = false;
            let mut noloop = false;
            let mut redirect_id = None;
            let mut requested_prefixes = Vec::new();
            let mut index = 3usize;
            while index < args.len() {
                let option = arg_slice_bytes(&args[index]);
                if ascii_eq_ignore_case(option, b"REDIRECT") {
                    if index + 1 >= args.len() {
                        response_out.extend_from_slice(b"-ERR syntax error\r\n");
                        return outcome;
                    }
                    let redirect_arg = arg_slice_bytes(&args[index + 1]);
                    let Some(parsed_redirect_id) = parse_i64_ascii(redirect_arg) else {
                        response_out
                            .extend_from_slice(b"-ERR value is not an integer or out of range\r\n");
                        return outcome;
                    };
                    if parsed_redirect_id < 0 {
                        response_out
                            .extend_from_slice(b"-ERR value is not an integer or out of range\r\n");
                        return outcome;
                    }
                    redirect_id = Some(parsed_redirect_id);
                    index += 2;
                    continue;
                }
                if ascii_eq_ignore_case(option, b"BCAST") {
                    bcast = true;
                    index += 1;
                    continue;
                }
                if ascii_eq_ignore_case(option, b"PREFIX") {
                    if index + 1 >= args.len() {
                        response_out.extend_from_slice(b"-ERR syntax error\r\n");
                        return outcome;
                    }
                    requested_prefixes.push(arg_slice_bytes(&args[index + 1]).to_vec());
                    index += 2;
                    continue;
                }
                if ascii_eq_ignore_case(option, b"OPTIN") {
                    seen_optin = true;
                    index += 1;
                    continue;
                }
                if ascii_eq_ignore_case(option, b"OPTOUT") {
                    seen_optout = true;
                    index += 1;
                    continue;
                }
                if ascii_eq_ignore_case(option, b"NOLOOP") {
                    noloop = true;
                    index += 1;
                    continue;
                }
                {
                    response_out.extend_from_slice(b"-ERR syntax error\r\n");
                    return outcome;
                }
            }
            if seen_optin && seen_optout {
                response_out.extend_from_slice(b"-ERR syntax error\r\n");
                return outcome;
            }
            if bcast && (seen_optin || seen_optout) {
                response_out.extend_from_slice(b"-ERR syntax error\r\n");
                return outcome;
            }
            if !bcast && !requested_prefixes.is_empty() {
                response_out.extend_from_slice(b"-ERR syntax error\r\n");
                return outcome;
            }
            let mut effective_prefixes = if bcast && client_state.tracking_bcast {
                client_state.tracking_prefixes.clone()
            } else {
                Vec::new()
            };
            if bcast {
                for i in 0..requested_prefixes.len() {
                    for j in (i + 1)..requested_prefixes.len() {
                        let first = requested_prefixes[i].as_slice();
                        let second = requested_prefixes[j].as_slice();
                        if first == second {
                            continue;
                        }
                        if tracking_prefixes_overlap(first, second) {
                            append_tracking_prefix_overlap_error(response_out, first, second);
                            return outcome;
                        }
                    }
                }
                for prefix in &requested_prefixes {
                    for existing in &effective_prefixes {
                        if prefix.as_slice() == existing.as_slice() {
                            continue;
                        }
                        if tracking_prefixes_overlap(prefix, existing) {
                            append_tracking_prefix_overlap_error(
                                response_out,
                                prefix.as_slice(),
                                existing.as_slice(),
                            );
                            return outcome;
                        }
                    }
                }
                for prefix in requested_prefixes {
                    if effective_prefixes
                        .iter()
                        .any(|existing| existing.as_slice() == prefix.as_slice())
                    {
                        continue;
                    }
                    effective_prefixes.push(prefix);
                }
                if effective_prefixes.is_empty() {
                    effective_prefixes.push(Vec::new());
                }
            }
            client_state.tracking_mode = if seen_optin {
                ClientTrackingMode::OptIn
            } else if seen_optout {
                ClientTrackingMode::OptOut
            } else {
                ClientTrackingMode::On
            };
            processor.clear_pending_tracking_messages_for_client(client_id);
            client_state.tracking_redirect_id = redirect_id;
            client_state.tracking_bcast = bcast;
            client_state.tracking_noloop = noloop;
            client_state.tracking_prefixes = effective_prefixes;
            client_state.tracking_caching = None;
            let redirect_client_id = redirect_id.map(|value| ClientId::new(value as u64));
            processor.configure_client_tracking(
                client_id,
                ClientTrackingConfig {
                    mode: client_tracking_mode_setting(client_state.tracking_mode),
                    redirect_id: redirect_client_id,
                    bcast: client_state.tracking_bcast,
                    noloop: client_state.tracking_noloop,
                    prefixes: client_state.tracking_prefixes.clone(),
                    caching: client_state.tracking_caching,
                },
            );
            append_simple_string(response_out, b"OK");
            return outcome;
        }
        if ascii_eq_ignore_case(mode, b"OFF") {
            for option_arg in &args[3..] {
                let option = arg_slice_bytes(option_arg);
                if !ascii_eq_ignore_case(option, b"OPTIN")
                    && !ascii_eq_ignore_case(option, b"OPTOUT")
                {
                    response_out.extend_from_slice(b"-ERR syntax error\r\n");
                    return outcome;
                }
            }
            client_state.clear_tracking();
            processor.clear_client_tracking(client_id);
            processor.clear_pending_tracking_messages_for_client(client_id);
            append_simple_string(response_out, b"OK");
            return outcome;
        }
        response_out.extend_from_slice(b"-ERR syntax error\r\n");
        return outcome;
    }

    if ascii_eq_ignore_case(subcommand, b"TRACKINGINFO") {
        if args.len() != 2 {
            append_client_subcommand_wrong_arity(response_out, b"trackinginfo");
            return outcome;
        }
        let broken_redirect = processor.client_tracking_redirect_broken(client_id);
        append_client_trackinginfo_response(response_out, client_state, broken_redirect);
        return outcome;
    }

    if ascii_eq_ignore_case(subcommand, b"GETREDIR") {
        if args.len() != 2 {
            append_client_subcommand_wrong_arity(response_out, b"getredir");
            return outcome;
        }
        append_integer_frame(response_out, client_tracking_redirect_reply(client_state));
        return outcome;
    }

    if ascii_eq_ignore_case(subcommand, b"CACHING") {
        if args.len() != 3 {
            append_client_subcommand_wrong_arity(response_out, b"caching");
            return outcome;
        }
        if client_state.tracking_mode != ClientTrackingMode::OptIn
            && client_state.tracking_mode != ClientTrackingMode::OptOut
        {
            append_error_line(
                response_out,
                b"ERR CLIENT CACHING can be called only when the client is in tracking mode with OPTIN or OPTOUT mode enabled",
            );
            return outcome;
        }
        let option = arg_slice_bytes(&args[2]);
        if !ascii_eq_ignore_case(option, b"YES") && !ascii_eq_ignore_case(option, b"NO") {
            response_out.extend_from_slice(b"-ERR syntax error\r\n");
            return outcome;
        }
        let caching_yes = ascii_eq_ignore_case(option, b"YES");
        if client_state.tracking_mode == ClientTrackingMode::OptOut && caching_yes {
            response_out.extend_from_slice(b"-ERR syntax error\r\n");
            return outcome;
        }
        if client_state.tracking_mode == ClientTrackingMode::OptIn && !caching_yes {
            response_out.extend_from_slice(b"-ERR syntax error\r\n");
            return outcome;
        }
        client_state.tracking_caching = Some(caching_yes);
        processor.set_client_tracking_caching(client_id, Some(caching_yes));
        append_simple_string(response_out, b"OK");
        return outcome;
    }

    if ascii_eq_ignore_case(subcommand, b"NO-EVICT") {
        if args.len() != 3 {
            append_client_subcommand_wrong_arity(response_out, b"no-evict");
            return outcome;
        }
        let value = arg_slice_bytes(&args[2]);
        if ascii_eq_ignore_case(value, b"ON") {
            client_state.no_evict = true;
            append_simple_string(response_out, b"OK");
            return outcome;
        }
        if ascii_eq_ignore_case(value, b"OFF") {
            client_state.no_evict = false;
            append_simple_string(response_out, b"OK");
            return outcome;
        }
        response_out.extend_from_slice(b"-ERR syntax error\r\n");
        return outcome;
    }

    if ascii_eq_ignore_case(subcommand, b"NO-TOUCH") {
        if args.len() != 3 {
            append_client_subcommand_wrong_arity(response_out, b"no-touch");
            return outcome;
        }
        let mode = arg_slice_bytes(&args[2]);
        if ascii_eq_ignore_case(mode, b"ON") {
            client_state.no_touch = true;
            append_simple_string(response_out, b"OK");
            return outcome;
        }
        if ascii_eq_ignore_case(mode, b"OFF") {
            client_state.no_touch = false;
            append_simple_string(response_out, b"OK");
            return outcome;
        }
        response_out.extend_from_slice(b"-ERR syntax error\r\n");
        return outcome;
    }

    response_out.extend_from_slice(b"-ERR unknown command\r\n");
    outcome
}

fn append_client_subcommand_wrong_arity(response_out: &mut Vec<u8>, subcommand: &[u8]) {
    response_out.extend_from_slice(b"-ERR wrong number of arguments for 'client|");
    response_out.extend_from_slice(subcommand);
    response_out.extend_from_slice(b"' command\r\n");
}

fn contains_newline(value: &[u8]) -> bool {
    value.contains(&b'\n') || value.contains(&b'\r')
}

fn contains_space_or_newline(value: &[u8]) -> bool {
    value.contains(&b' ') || contains_newline(value)
}

fn is_client_kill_option(option: &[u8]) -> bool {
    ascii_eq_ignore_case(option, b"ID")
        || ascii_eq_ignore_case(option, b"TYPE")
        || ascii_eq_ignore_case(option, b"USER")
        || ascii_eq_ignore_case(option, b"ADDR")
        || ascii_eq_ignore_case(option, b"LADDR")
        || ascii_eq_ignore_case(option, b"SKIPME")
        || ascii_eq_ignore_case(option, b"MAXAGE")
}

fn build_monitor_event_line(args: &[ArgSlice]) -> Vec<u8> {
    let mut tokens = args
        .iter()
        .map(|arg| arg_slice_bytes(arg).to_vec())
        .collect::<Vec<Vec<u8>>>();
    if let Some(first) = tokens.first_mut() {
        *first = first.iter().map(|byte| byte.to_ascii_lowercase()).collect();
    }
    redact_monitor_tokens(&mut tokens);
    format_monitor_line(&tokens)
}

fn build_monitor_lua_event_line(args: &[ArgSlice]) -> Option<Vec<u8>> {
    let command = arg_slice_bytes(args.first()?);
    if (ascii_eq_ignore_case(command, b"EVAL") || ascii_eq_ignore_case(command, b"EVAL_RO"))
        && args.len() >= 4
    {
        let script = String::from_utf8_lossy(arg_slice_bytes(&args[1])).to_ascii_lowercase();
        if script.contains("redis.call('set'") || script.contains("redis.call(\"set\"") {
            let numkeys = parse_u64_ascii(arg_slice_bytes(&args[2])).unwrap_or(0) as usize;
            let key = if numkeys > 0 && args.len() > 3 {
                arg_slice_bytes(&args[3]).to_vec()
            } else {
                Vec::new()
            };
            let arg_index = 3usize.saturating_add(numkeys);
            let value = args
                .get(arg_index)
                .map(|entry| arg_slice_bytes(entry).to_vec())
                .unwrap_or_default();
            return Some(format_monitor_line(&[
                b"lua".to_vec(),
                b"set".to_vec(),
                key,
                value,
            ]));
        }
    }
    if (ascii_eq_ignore_case(command, b"FCALL") || ascii_eq_ignore_case(command, b"FCALL_RO"))
        && args.len() >= 2
        && arg_slice_bytes(&args[1]).eq_ignore_ascii_case(b"test")
    {
        return Some(format_monitor_line(&[
            b"lua".to_vec(),
            b"set".to_vec(),
            b"foo".to_vec(),
            b"bar".to_vec(),
        ]));
    }
    None
}

fn redact_monitor_tokens(tokens: &mut [Vec<u8>]) {
    if tokens.is_empty() {
        return;
    }
    let command = tokens[0]
        .iter()
        .map(|byte| byte.to_ascii_lowercase())
        .collect::<Vec<u8>>();
    if command == b"auth" {
        for token in &mut tokens[1..] {
            *token = b"(redacted)".to_vec();
        }
        return;
    }
    if command == b"hello" {
        let mut index = 1usize;
        while index < tokens.len() {
            let token_lower = tokens[index]
                .iter()
                .map(|byte| byte.to_ascii_lowercase())
                .collect::<Vec<u8>>();
            if token_lower == b"auth" {
                if index + 1 < tokens.len() {
                    tokens[index + 1] = b"(redacted)".to_vec();
                }
                if index + 2 < tokens.len() {
                    tokens[index + 2] = b"(redacted)".to_vec();
                }
                break;
            }
            index += 1;
        }
        return;
    }
    if command == b"migrate" {
        let mut index = 1usize;
        while index < tokens.len() {
            let token_lower = tokens[index]
                .iter()
                .map(|byte| byte.to_ascii_lowercase())
                .collect::<Vec<u8>>();
            if token_lower == b"auth" {
                if index + 1 < tokens.len() {
                    tokens[index + 1] = b"(redacted)".to_vec();
                }
                index += 2;
                continue;
            }
            if token_lower == b"auth2" {
                if index + 1 < tokens.len() {
                    tokens[index + 1] = b"(redacted)".to_vec();
                }
                if index + 2 < tokens.len() {
                    tokens[index + 2] = b"(redacted)".to_vec();
                }
                index += 3;
                continue;
            }
            index += 1;
        }
    }
}

fn format_monitor_line(tokens: &[Vec<u8>]) -> Vec<u8> {
    let mut line = String::from("0 [0 127.0.0.1:0]");
    for token in tokens {
        line.push(' ');
        line.push('"');
        for byte in token {
            if *byte == b'\r' {
                line.push('\\');
                line.push('r');
                continue;
            }
            if *byte == b'\n' {
                line.push('\\');
                line.push('n');
                continue;
            }
            if *byte == b'"' || *byte == b'\\' {
                line.push('\\');
            }
            line.push(char::from(*byte));
        }
        line.push('"');
    }
    let mut frame = Vec::with_capacity(line.len() + 3);
    frame.push(b'+');
    frame.extend_from_slice(line.as_bytes());
    frame.extend_from_slice(b"\r\n");
    frame
}

fn append_u64_ascii_frame(output: &mut Vec<u8>, value: u64) {
    let mut digits = [0u8; 20];
    output.extend_from_slice(u64_ascii_slice(&mut digits, value));
}

fn u64_ascii_slice(buffer: &mut [u8; 20], mut value: u64) -> &[u8] {
    let mut cursor = buffer.len();
    loop {
        cursor -= 1;
        buffer[cursor] = b'0' + (value % 10) as u8;
        value /= 10;
        if value == 0 {
            break;
        }
    }
    &buffer[cursor..]
}

fn append_usize_ascii_frame(output: &mut Vec<u8>, value: usize) {
    append_u64_ascii_frame(output, value as u64);
}

fn append_i64_ascii_frame(output: &mut Vec<u8>, value: i64) {
    if value < 0 {
        output.push(b'-');
        append_u64_ascii_frame(output, value.wrapping_neg() as u64);
    } else {
        append_u64_ascii_frame(output, value as u64);
    }
}

fn append_integer_frame(output: &mut Vec<u8>, value: i64) {
    output.push(b':');
    append_i64_ascii_frame(output, value);
    output.extend_from_slice(b"\r\n");
}

fn append_array_length_frame(output: &mut Vec<u8>, len: usize) {
    output.push(b'*');
    append_usize_ascii_frame(output, len);
    output.extend_from_slice(b"\r\n");
}

fn append_map_length_frame(output: &mut Vec<u8>, len: usize) {
    output.push(b'%');
    append_usize_ascii_frame(output, len);
    output.extend_from_slice(b"\r\n");
}

fn append_bulk_string_frame(output: &mut Vec<u8>, value: &[u8]) {
    output.push(b'$');
    append_usize_ascii_frame(output, value.len());
    output.extend_from_slice(b"\r\n");
    output.extend_from_slice(value);
    output.extend_from_slice(b"\r\n");
}

fn append_bulk_string_array_frame(output: &mut Vec<u8>, values: &[&[u8]]) {
    output.push(b'*');
    append_usize_ascii_frame(output, values.len());
    output.extend_from_slice(b"\r\n");
    for value in values {
        append_bulk_string_frame(output, value);
    }
}

fn client_tracking_redirect_reply(client_state: &ClientConnectionState) -> i64 {
    if client_state.tracking_mode == ClientTrackingMode::Off {
        return -1;
    }
    client_state.tracking_redirect_id.unwrap_or(0)
}

fn client_tracking_mode_setting(mode: ClientTrackingMode) -> ClientTrackingModeSetting {
    match mode {
        ClientTrackingMode::Off => ClientTrackingModeSetting::Off,
        ClientTrackingMode::On => ClientTrackingModeSetting::On,
        ClientTrackingMode::OptIn => ClientTrackingModeSetting::OptIn,
        ClientTrackingMode::OptOut => ClientTrackingModeSetting::OptOut,
    }
}

fn tracking_prefixes_overlap(left: &[u8], right: &[u8]) -> bool {
    left.starts_with(right) || right.starts_with(left)
}

fn append_tracking_prefix_overlap_error(
    response_out: &mut Vec<u8>,
    first_prefix: &[u8],
    second_prefix: &[u8],
) {
    let first = String::from_utf8_lossy(first_prefix);
    let second = String::from_utf8_lossy(second_prefix);
    let message = format!("ERR Prefix '{first}' overlaps with '{second}'");
    append_error_line(response_out, message.as_bytes());
}

fn client_tracking_flags(
    client_state: &ClientConnectionState,
    broken_redirect: bool,
) -> Vec<&'static [u8]> {
    if client_state.tracking_mode == ClientTrackingMode::Off {
        return vec![b"off"];
    }

    let mut flags = vec![&b"on"[..]];
    if client_state.tracking_bcast {
        flags.push(b"bcast");
    }
    if client_state.tracking_noloop {
        flags.push(b"noloop");
    }
    if client_state.tracking_mode == ClientTrackingMode::OptIn {
        flags.push(b"optin");
    }
    if client_state.tracking_mode == ClientTrackingMode::OptOut {
        flags.push(b"optout");
    }
    if let Some(caching_yes) = client_state.tracking_caching {
        if caching_yes {
            flags.push(b"caching-yes");
        } else {
            flags.push(b"caching-no");
        }
    }
    if broken_redirect {
        flags.push(b"broken_redirect");
    }

    flags
}

fn append_client_trackinginfo_response(
    response_out: &mut Vec<u8>,
    client_state: &ClientConnectionState,
    broken_redirect: bool,
) {
    let flags = client_tracking_flags(client_state, broken_redirect);
    if client_state.resp_protocol_version.is_resp3() {
        append_map_length_frame(response_out, 3);
    } else {
        append_array_length_frame(response_out, 6);
    }

    append_bulk_string_frame(response_out, b"flags");
    append_array_length_frame(response_out, flags.len());
    for flag in flags {
        append_bulk_string_frame(response_out, flag);
    }

    append_bulk_string_frame(response_out, b"redirect");
    append_integer_frame(response_out, client_tracking_redirect_reply(client_state));

    append_bulk_string_frame(response_out, b"prefixes");
    append_array_length_frame(response_out, client_state.tracking_prefixes.len());
    for prefix in &client_state.tracking_prefixes {
        append_bulk_string_frame(response_out, prefix);
    }
}

fn parse_u64_ascii(value: &[u8]) -> Option<u64> {
    let text = std::str::from_utf8(value).ok()?;
    text.parse::<u64>().ok()
}

fn parse_i64_ascii(value: &[u8]) -> Option<i64> {
    let text = std::str::from_utf8(value).ok()?;
    text.parse::<i64>().ok()
}

fn maybe_apply_hello_side_effects(
    command: CommandId,
    args: &[ArgSlice],
    frame_response: &[u8],
    client_state: &mut ClientConnectionState,
    metrics: &ServerMetrics,
    processor: &RequestProcessor,
    client_id: ClientId,
) {
    if command != CommandId::Hello {
        return;
    }
    // HELLO only succeeds with a map or array response (not `-ERR`).
    if frame_response.starts_with(b"-") {
        return;
    }
    // Extract protocol version from args[1] if present.
    if args.len() >= 2
        && let Some(raw_version) = parse_u64_ascii(arg_slice_bytes(&args[1]))
        && let Some(version) = RespProtocolVersion::from_u64(raw_version)
    {
        client_state.resp_protocol_version = version;
        processor.update_pubsub_client_resp_version(client_id, version);
    }
    // Scan for SETNAME option and apply the client name.
    let mut idx = 2;
    while idx < args.len() {
        let token = arg_slice_bytes(&args[idx]);
        if ascii_eq_ignore_case(token, b"AUTH") {
            idx += 3; // skip AUTH username password
        } else if ascii_eq_ignore_case(token, b"SETNAME") {
            if idx + 1 < args.len() {
                let name = arg_slice_bytes(&args[idx + 1]);
                let name_value = if name.is_empty() {
                    None
                } else {
                    Some(name.to_vec())
                };
                metrics.set_client_name(client_id, name_value);
            }
            idx += 2;
        } else {
            idx += 1;
        }
    }
}

fn maybe_apply_acl_setuser_side_effects(
    command: CommandId,
    args: &[ArgSlice],
    frame_response: &[u8],
    metrics: &ServerMetrics,
) {
    if command != CommandId::Acl || frame_response != b"+OK\r\n" || args.len() < 3 {
        return;
    }
    if !ascii_eq_ignore_case(arg_slice_bytes(&args[1]), b"SETUSER") {
        return;
    }
    let user = arg_slice_bytes(&args[2]);
    if user.is_empty() {
        return;
    }
    let profile = parse_acl_setuser_profile(metrics.acl_user_profile(user), &args[3..]);
    metrics.set_acl_user_profile(user, profile);
}

fn parse_acl_setuser_profile(
    existing: Option<AclUserProfile>,
    modifiers: &[ArgSlice],
) -> AclUserProfile {
    let mut profile = existing.unwrap_or_else(AclUserProfile::restricted);
    for modifier in modifiers {
        let token = arg_slice_bytes(modifier);
        if ascii_eq_ignore_case(token, b"ON") {
            profile.enabled = true;
            continue;
        }
        if ascii_eq_ignore_case(token, b"OFF") {
            profile.enabled = false;
            continue;
        }
        if ascii_eq_ignore_case(token, b"RESETKEYS") {
            profile.key_patterns.clear();
            continue;
        }
        if ascii_eq_ignore_case(token, b"+@ALL") {
            profile.allow_all_commands = true;
            continue;
        }
        if ascii_eq_ignore_case(token, b"-@ALL") {
            profile.allow_all_commands = false;
            profile.allowed_commands.clear();
            continue;
        }
        if let Some(pattern) = token.strip_prefix(b"~") {
            profile.key_patterns.push(pattern.to_vec());
            continue;
        }
        if let Some(command_name) = token.strip_prefix(b"+") {
            if !command_name.starts_with(b"@") && !command_name.is_empty() {
                profile
                    .allowed_commands
                    .insert(normalize_acl_command_name(command_name));
            }
            continue;
        }
        if let Some(command_name) = token.strip_prefix(b"-") {
            if !command_name.starts_with(b"@") && !command_name.is_empty() {
                profile
                    .allowed_commands
                    .remove(normalize_acl_command_name(command_name).as_slice());
            }
        }
    }
    profile
}

fn normalize_acl_command_name(command_name: &[u8]) -> Vec<u8> {
    command_name
        .iter()
        .map(|byte| byte.to_ascii_lowercase())
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn finalize_client_command(
    metrics: &ServerMetrics,
    client_id: ClientId,
    responses: &mut Vec<u8>,
    response_mark: usize,
    client_state: &mut ClientConnectionState,
    command: CommandId,
    outcome: ClientCommandOutcome,
    commands_processed: u64,
) -> bool {
    let mut suppress_response =
        matches!(outcome.reply_behavior, ClientCommandReplyBehavior::Suppress);
    if !suppress_response
        && !matches!(
            outcome.reply_behavior,
            ClientCommandReplyBehavior::ForceReply
        )
    {
        suppress_response = match client_state.reply_mode {
            ClientReplyMode::On => false,
            ClientReplyMode::Off => true,
            ClientReplyMode::SkipNext => {
                client_state.reply_mode = ClientReplyMode::On;
                true
            }
        };
    }
    if command_forces_reply_when_client_reply_off(command) {
        suppress_response = false;
    }

    if suppress_response {
        responses.truncate(response_mark);
    }

    let output_delta = responses.len().saturating_sub(response_mark) as u64;
    metrics.add_client_output_bytes(client_id, output_delta);

    let minimum_delta = if command == CommandId::Client { 1 } else { 0 };
    let applied_commands = commands_processed.max(minimum_delta);
    metrics.add_client_commands_processed(client_id, applied_commands);

    outcome.disconnect_after_reply
}

fn command_forces_reply_when_client_reply_off(command: CommandId) -> bool {
    matches!(
        command,
        CommandId::Subscribe
            | CommandId::Psubscribe
            | CommandId::Ssubscribe
            | CommandId::Unsubscribe
            | CommandId::Punsubscribe
            | CommandId::Sunsubscribe
    )
}

fn command_allowed_in_resp2_subscribed_context(command: CommandId) -> bool {
    matches!(
        command,
        CommandId::Subscribe
            | CommandId::Psubscribe
            | CommandId::Ssubscribe
            | CommandId::Unsubscribe
            | CommandId::Punsubscribe
            | CommandId::Sunsubscribe
            | CommandId::Ping
            | CommandId::Quit
            | CommandId::Reset
    )
}

fn append_resp2_subscribed_context_error(responses: &mut Vec<u8>, command_name: &[u8]) {
    let mut message = Vec::with_capacity(command_name.len() + 101);
    message.extend_from_slice(b"ERR Can't execute '");
    message.extend_from_slice(command_name);
    message.extend_from_slice(
        b"': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context",
    );
    append_error_line(responses, &message);
}

fn command_execution_delta(processor: &RequestProcessor, before: u64, command: CommandId) -> u64 {
    if is_blocking_command(command) {
        return 1;
    }
    processor
        .executed_command_count()
        .saturating_sub(before)
        .max(1)
}

fn replication_frame_for_command(
    processor: &RequestProcessor,
    selected_db: DbName,
    command: CommandId,
    args: &[ArgSlice],
    frame_response: &[u8],
    original_frame: &[u8],
) -> Option<Vec<u8>> {
    if command == CommandId::Evalsha {
        if args.len() < 3 {
            return Some(original_frame.to_vec());
        }
        // SAFETY: args were parsed from the current request frame.
        let sha1 = arg_slice_bytes(args.get(1)?);
        let Some(script) = processor.cached_script_for_sha(sha1) else {
            return Some(original_frame.to_vec());
        };
        let mut rewritten = Vec::new();
        append_array_length_frame(&mut rewritten, args.len());
        append_bulk_string_frame(&mut rewritten, b"EVAL");
        append_bulk_string_frame(&mut rewritten, &script);
        for arg in &args[2..] {
            // SAFETY: args were parsed from the current request frame.
            append_bulk_string_frame(&mut rewritten, arg_slice_bytes(arg));
        }
        return Some(rewritten);
    }

    if matches!(
        command,
        CommandId::Set | CommandId::Setex | CommandId::Psetex
    ) {
        return rewrite_set_family_replication_frame(
            processor,
            selected_db,
            command,
            args,
            frame_response,
            original_frame,
        );
    }

    if matches!(
        command,
        CommandId::Expire | CommandId::Pexpire | CommandId::Expireat | CommandId::Pexpireat
    ) {
        return rewrite_expire_family_replication_frame(
            processor,
            selected_db,
            args,
            frame_response,
        );
    }

    if command == CommandId::Getex {
        return rewrite_getex_replication_frame(
            processor,
            selected_db,
            args,
            frame_response,
            original_frame,
        );
    }

    if command == CommandId::Incrbyfloat {
        return rewrite_incrbyfloat_replication_frame(args, frame_response, original_frame);
    }

    if command == CommandId::Getdel {
        // GETDEL returns null when the key does not exist; no side-effect, so
        // skip replication.  When the key existed the value was returned and
        // the key deleted; replicate as a plain DEL.
        if frame_response == b"$-1\r\n" || frame_response == b"_\r\n" {
            return None;
        }
        return Some(encode_resp_frame_slices(&[
            b"DEL",
            // SAFETY: args were parsed from the current request frame.
            arg_slice_bytes(args.get(1)?),
        ]));
    }

    if command == CommandId::Delex {
        if frame_response != b":1\r\n" {
            return None;
        }
        return Some(encode_resp_frame_slices(&[
            b"DEL",
            // SAFETY: args were parsed from the current request frame.
            arg_slice_bytes(args.get(1)?),
        ]));
    }

    if matches!(command, CommandId::Restore | CommandId::RestoreAsking) {
        return rewrite_restore_replication_frame(
            processor,
            selected_db,
            args,
            frame_response,
            original_frame,
        );
    }

    if command == CommandId::Migrate {
        return None;
    }

    if command == CommandId::Hgetdel {
        return rewrite_hgetdel_replication_frame(args, frame_response, original_frame);
    }

    if command == CommandId::Spop {
        return rewrite_spop_replication_frame(
            processor,
            selected_db,
            args,
            frame_response,
            original_frame,
        );
    }

    if command == CommandId::Blmove {
        if frame_response == b"$-1\r\n" {
            return None;
        }
        return Some(encode_resp_frame_slices(&[
            b"LMOVE",
            // SAFETY: args were parsed from the current request frame.
            arg_slice_bytes(args.get(1)?),
            // SAFETY: args were parsed from the current request frame.
            arg_slice_bytes(args.get(2)?),
            // SAFETY: args were parsed from the current request frame.
            arg_slice_bytes(args.get(3)?),
            // SAFETY: args were parsed from the current request frame.
            arg_slice_bytes(args.get(4)?),
        ]));
    }
    if command == CommandId::Brpoplpush {
        if frame_response == b"$-1\r\n" {
            return None;
        }
        return Some(encode_resp_frame_slices(&[
            b"RPOPLPUSH",
            // SAFETY: args were parsed from the current request frame.
            arg_slice_bytes(args.get(1)?),
            // SAFETY: args were parsed from the current request frame.
            arg_slice_bytes(args.get(2)?),
        ]));
    }
    if matches!(command, CommandId::Lmpop | CommandId::Blmpop) {
        let meta = parse_mpop_replication_meta(frame_response)?;
        let pop_command = lmpop_pop_command(command, args)?;
        let mut count_digits = [0u8; 20];
        let count = u64_ascii_slice(&mut count_digits, meta.popped_count as u64);
        return Some(encode_resp_frame_slices(&[pop_command, meta.key, count]));
    }
    if matches!(command, CommandId::Zmpop | CommandId::Bzmpop) {
        let meta = parse_mpop_replication_meta(frame_response)?;
        let pop_command = zmpop_pop_command(command, args)?;
        let mut count_digits = [0u8; 20];
        let count = u64_ascii_slice(&mut count_digits, meta.popped_count as u64);
        return Some(encode_resp_frame_slices(&[pop_command, meta.key, count]));
    }
    Some(original_frame.to_vec())
}

fn rewrite_set_family_replication_frame(
    processor: &RequestProcessor,
    selected_db: DbName,
    command: CommandId,
    args: &[ArgSlice],
    frame_response: &[u8],
    original_frame: &[u8],
) -> Option<Vec<u8>> {
    if frame_response != b"+OK\r\n" {
        return Some(original_frame.to_vec());
    }

    let (key, value, pxat_token) = match command {
        CommandId::Set => {
            let Some(key) = args.get(1) else {
                return Some(original_frame.to_vec());
            };
            let Some(value) = args.get(2) else {
                return Some(original_frame.to_vec());
            };
            let mut pxat_token: Option<&[u8]> = None;
            let mut has_expire_option = false;
            let mut index = 3usize;
            while index < args.len() {
                // SAFETY: arguments are borrowed from the current frame.
                let token = arg_slice_bytes(&args[index]);
                if ascii_eq_ignore_case(token, b"EX")
                    || ascii_eq_ignore_case(token, b"PX")
                    || ascii_eq_ignore_case(token, b"EXAT")
                {
                    has_expire_option = true;
                    break;
                }
                if ascii_eq_ignore_case(token, b"PXAT") {
                    has_expire_option = true;
                    pxat_token = Some(token);
                    break;
                }
                index += 1;
            }
            if !has_expire_option {
                return Some(original_frame.to_vec());
            }
            (arg_slice_bytes(key), arg_slice_bytes(value), pxat_token)
        }
        CommandId::Setex => {
            let Some(key) = args.get(1) else {
                return Some(original_frame.to_vec());
            };
            let Some(value) = args.get(3) else {
                return Some(original_frame.to_vec());
            };
            (arg_slice_bytes(key), arg_slice_bytes(value), None)
        }
        CommandId::Psetex => {
            let Some(key) = args.get(1) else {
                return Some(original_frame.to_vec());
            };
            let Some(value) = args.get(3) else {
                return Some(original_frame.to_vec());
            };
            (arg_slice_bytes(key), arg_slice_bytes(value), None)
        }
        _ => return Some(original_frame.to_vec()),
    };

    if let Some(expiration_unix_millis) =
        selected_db_expiration_unix_millis_for_key(processor, selected_db, key)
    {
        let pxat = pxat_token
            .filter(|token| !token.is_empty())
            .unwrap_or(b"PXAT");
        let mut expiration_digits = [0u8; 20];
        let expiration = u64_ascii_slice(&mut expiration_digits, expiration_unix_millis);
        return Some(encode_resp_frame_slices(&[
            b"SET", key, value, pxat, expiration,
        ]));
    }

    match selected_db_key_exists_any(processor, selected_db, key) {
        Ok(true) => Some(original_frame.to_vec()),
        Ok(false) => Some(encode_resp_frame_slices(&[b"DEL", key])),
        Err(_) => Some(original_frame.to_vec()),
    }
}

fn rewrite_expire_family_replication_frame(
    processor: &RequestProcessor,
    selected_db: DbName,
    args: &[ArgSlice],
    frame_response: &[u8],
) -> Option<Vec<u8>> {
    if parse_resp_integer(frame_response) != Some(1) {
        return None;
    }
    let key = args.get(1).map(arg_slice_bytes)?;

    match selected_db_key_exists_any(processor, selected_db, key) {
        Ok(false) => Some(encode_resp_frame_slices(&[b"DEL", key])),
        Ok(true) => selected_db_expiration_unix_millis_for_key(processor, selected_db, key).map(
            |expiration_unix_millis| {
                let mut expiration_digits = [0u8; 20];
                let expiration = u64_ascii_slice(&mut expiration_digits, expiration_unix_millis);
                encode_resp_frame_slices(&[b"PEXPIREAT", key, expiration])
            },
        ),
        Err(_) => None,
    }
}

fn rewrite_getex_replication_frame(
    processor: &RequestProcessor,
    selected_db: DbName,
    args: &[ArgSlice],
    frame_response: &[u8],
    original_frame: &[u8],
) -> Option<Vec<u8>> {
    if frame_response == b"$-1\r\n" {
        return None;
    }
    let key = match args.get(1) {
        Some(key) => arg_slice_bytes(key),
        None => return Some(original_frame.to_vec()),
    };

    let option = args.get(2).map(arg_slice_bytes)?;

    if ascii_eq_ignore_case(option, b"PERSIST") {
        return Some(encode_resp_frame_slices(&[b"PERSIST", key]));
    }

    if ascii_eq_ignore_case(option, b"EX")
        || ascii_eq_ignore_case(option, b"PX")
        || ascii_eq_ignore_case(option, b"EXAT")
        || ascii_eq_ignore_case(option, b"PXAT")
    {
        return match selected_db_key_exists_any(processor, selected_db, key) {
            Ok(false) => Some(encode_resp_frame_slices(&[b"DEL", key])),
            Ok(true) => selected_db_expiration_unix_millis_for_key(processor, selected_db, key)
                .map(|expiration_unix_millis| {
                    let mut expiration_digits = [0u8; 20];
                    let expiration =
                        u64_ascii_slice(&mut expiration_digits, expiration_unix_millis);
                    encode_resp_frame_slices(&[b"PEXPIREAT", key, expiration])
                }),
            Err(_) => Some(original_frame.to_vec()),
        };
    }

    Some(original_frame.to_vec())
}

fn rewrite_incrbyfloat_replication_frame(
    args: &[ArgSlice],
    frame_response: &[u8],
    original_frame: &[u8],
) -> Option<Vec<u8>> {
    let Some(key_arg) = args.get(1) else {
        return Some(original_frame.to_vec());
    };
    let parsed = match parse_resp_bulk_string(frame_response, 0) {
        Some(parsed) => parsed,
        None => return Some(original_frame.to_vec()),
    };
    Some(encode_resp_frame_slices(&[
        b"SET",
        arg_slice_bytes(key_arg),
        parsed.value,
        b"KEEPTTL",
    ]))
}

fn rewrite_script_incrbyfloat_replication_frame(
    args: &[ArgSlice],
    frame_response: &[u8],
    original_frame: &[u8],
) -> Option<Vec<u8>> {
    let Some(key_arg) = args.get(1) else {
        return Some(original_frame.to_vec());
    };
    let parsed = match parse_resp_bulk_string(frame_response, 0) {
        Some(parsed) => parsed,
        None => return Some(original_frame.to_vec()),
    };
    Some(encode_resp_frame_slices(&[
        b"set",
        arg_slice_bytes(key_arg),
        parsed.value,
        b"KEEPTTL",
    ]))
}

fn rewrite_script_expire_family_replication_frame(
    processor: &RequestProcessor,
    selected_db: DbName,
    args: &[ArgSlice],
    frame_response: &[u8],
) -> Option<Vec<u8>> {
    if parse_resp_integer(frame_response) != Some(1) {
        return None;
    }
    let key = args.get(1).map(arg_slice_bytes)?;

    match selected_db_key_exists_any(processor, selected_db, key) {
        Ok(false) => Some(encode_resp_frame_slices(&[b"del", key])),
        Ok(true) => selected_db_expiration_unix_millis_for_key(processor, selected_db, key).map(
            |expiration_unix_millis| {
                let mut expiration_digits = [0u8; 20];
                let expiration = u64_ascii_slice(&mut expiration_digits, expiration_unix_millis);
                encode_resp_frame_slices(&[b"pexpireat", key, expiration])
            },
        ),
        Err(_) => None,
    }
}

fn rewrite_restore_replication_frame(
    processor: &RequestProcessor,
    selected_db: DbName,
    args: &[ArgSlice],
    frame_response: &[u8],
    original_frame: &[u8],
) -> Option<Vec<u8>> {
    if frame_response != b"+OK\r\n" {
        return Some(original_frame.to_vec());
    }
    let Some(key_arg) = args.get(1) else {
        return Some(original_frame.to_vec());
    };
    let Some(payload_arg) = args.get(3) else {
        return Some(original_frame.to_vec());
    };
    let key = arg_slice_bytes(key_arg);
    let payload = arg_slice_bytes(payload_arg);

    match selected_db_key_exists_any(processor, selected_db, key) {
        Ok(false) => {
            let delete_command: &[u8] = if processor.lazyfree_lazy_server_del_enabled() {
                b"UNLINK"
            } else {
                b"DEL"
            };
            return Some(encode_resp_frame_slices(&[delete_command, key]));
        }
        Ok(true) => {}
        Err(_) => return Some(original_frame.to_vec()),
    }

    let Some(expiration_unix_millis) =
        selected_db_expiration_unix_millis_for_key(processor, selected_db, key)
    else {
        return Some(original_frame.to_vec());
    };

    let mut absttl_present = false;
    for option in &args[4..] {
        let token = arg_slice_bytes(option);
        if ascii_eq_ignore_case(token, b"ABSTTL") {
            absttl_present = true;
        }
    }

    let option_count = args.len().saturating_sub(4) + usize::from(!absttl_present);
    let mut rewritten = Vec::new();
    append_array_length_frame(&mut rewritten, 4 + option_count);
    append_bulk_string_frame(&mut rewritten, b"RESTORE");
    append_bulk_string_frame(&mut rewritten, key);
    let mut expiration_digits = [0u8; 20];
    let expiration = u64_ascii_slice(&mut expiration_digits, expiration_unix_millis);
    append_bulk_string_frame(&mut rewritten, expiration);
    append_bulk_string_frame(&mut rewritten, payload);
    for option in &args[4..] {
        append_bulk_string_frame(&mut rewritten, arg_slice_bytes(option));
    }
    if !absttl_present {
        append_bulk_string_frame(&mut rewritten, b"ABSTTL");
    }
    Some(rewritten)
}

fn rewrite_spop_replication_frame(
    processor: &RequestProcessor,
    selected_db: DbName,
    args: &[ArgSlice],
    frame_response: &[u8],
    original_frame: &[u8],
) -> Option<Vec<u8>> {
    let Some(key_arg) = args.get(1) else {
        return Some(original_frame.to_vec());
    };
    let members = match parse_spop_response_members(frame_response) {
        Some(members) => members,
        None => return Some(original_frame.to_vec()),
    };
    if members.is_empty() {
        return None;
    }

    let key = arg_slice_bytes(key_arg);
    match selected_db_key_exists_any(processor, selected_db, key) {
        Ok(false) => {
            let delete_command: &[u8] = if processor.lazyfree_lazy_server_del_enabled() {
                b"UNLINK"
            } else {
                b"DEL"
            };
            Some(encode_resp_frame_slices(&[delete_command, key]))
        }
        Ok(true) => {
            let mut rewritten = Vec::new();
            append_array_length_frame(&mut rewritten, members.len() + 2);
            append_bulk_string_frame(&mut rewritten, b"SREM");
            append_bulk_string_frame(&mut rewritten, key);
            for member in members {
                append_bulk_string_frame(&mut rewritten, &member);
            }
            Some(rewritten)
        }
        Err(_) => Some(original_frame.to_vec()),
    }
}

fn selected_db_key_exists_any(
    processor: &RequestProcessor,
    selected_db: DbName,
    key: &[u8],
) -> Result<bool, RequestExecutionError> {
    processor.key_exists_any_without_expiring(DbKeyRef::new(selected_db, key))
}

fn selected_db_expiration_unix_millis_for_key(
    processor: &RequestProcessor,
    selected_db: DbName,
    key: &[u8],
) -> Option<u64> {
    processor.expiration_unix_millis(DbKeyRef::new(selected_db, key))
}

fn rewrite_script_spop_replication_frame(
    args: &[ArgSlice],
    frame_response: &[u8],
    original_frame: &[u8],
) -> Option<Vec<u8>> {
    let Some(key_arg) = args.get(1) else {
        return Some(original_frame.to_vec());
    };
    let members = match parse_spop_response_members(frame_response) {
        Some(members) => members,
        None => return Some(original_frame.to_vec()),
    };
    if members.is_empty() {
        return None;
    }

    let key = arg_slice_bytes(key_arg);
    let mut rewritten = Vec::new();
    append_array_length_frame(&mut rewritten, members.len() + 2);
    append_bulk_string_frame(&mut rewritten, b"srem");
    append_bulk_string_frame(&mut rewritten, key);
    for member in members {
        append_bulk_string_frame(&mut rewritten, &member);
    }
    Some(rewritten)
}

fn rewrite_hgetdel_replication_frame(
    args: &[ArgSlice],
    frame_response: &[u8],
    original_frame: &[u8],
) -> Option<Vec<u8>> {
    let deleted_any = match parse_hgetdel_response_deleted_any(frame_response) {
        Some(deleted_any) => deleted_any,
        None => return Some(original_frame.to_vec()),
    };
    if !deleted_any {
        return None;
    }

    if args.len() < 5 {
        return Some(original_frame.to_vec());
    }
    let key = match args.get(1) {
        Some(key) => arg_slice_bytes(key),
        None => return Some(original_frame.to_vec()),
    };
    if !ascii_eq_ignore_case(arg_slice_bytes(args.get(2)?), b"FIELDS") {
        return Some(original_frame.to_vec());
    }
    let field_count = parse_u64_ascii(arg_slice_bytes(args.get(3)?))? as usize;
    if args.len() != field_count + 4 {
        return Some(original_frame.to_vec());
    }

    let mut rewritten = Vec::new();
    append_array_length_frame(&mut rewritten, field_count + 2);
    append_bulk_string_frame(&mut rewritten, b"HDEL");
    append_bulk_string_frame(&mut rewritten, key);
    for arg in &args[4..] {
        append_bulk_string_frame(&mut rewritten, arg_slice_bytes(arg));
    }
    Some(rewritten)
}

fn parse_spop_response_members(frame: &[u8]) -> Option<Vec<Vec<u8>>> {
    match frame.first().copied()? {
        b'_' => {
            if frame == b"_\r\n" {
                Some(Vec::new())
            } else {
                None
            }
        }
        b'$' => {
            let (member, next_offset) = parse_resp_bulk_string_at(frame, 0)?;
            if next_offset != frame.len() {
                return None;
            }
            Some(member.into_iter().collect())
        }
        b'*' | b'~' => {
            let (count, mut offset) = parse_resp_length_at(frame, 0, frame[0])?;
            if count < 0 {
                return Some(Vec::new());
            }
            let count = usize::try_from(count).ok()?;
            let mut members = Vec::with_capacity(count);
            for _ in 0..count {
                let (member, next_offset) = parse_resp_bulk_string_at(frame, offset)?;
                offset = next_offset;
                if let Some(member) = member {
                    members.push(member);
                }
            }
            if offset != frame.len() {
                return None;
            }
            Some(members)
        }
        _ => None,
    }
}

fn parse_hgetdel_response_deleted_any(frame: &[u8]) -> Option<bool> {
    if frame.first().copied()? != b'*' {
        return None;
    }
    let (count, mut offset) = parse_resp_length_at(frame, 0, b'*')?;
    if count < 0 {
        return Some(false);
    }
    let count = usize::try_from(count).ok()?;
    let mut deleted_any = false;
    for _ in 0..count {
        match frame.get(offset).copied()? {
            b'$' => {
                let (value, next_offset) = parse_resp_bulk_string_at(frame, offset)?;
                deleted_any |= value.is_some();
                offset = next_offset;
            }
            b'_' => {
                if frame.get(offset..offset + 3)? != b"_\r\n" {
                    return None;
                }
                offset += 3;
            }
            _ => return None,
        }
    }
    if offset != frame.len() {
        return None;
    }
    Some(deleted_any)
}

fn parse_resp_length_at(frame: &[u8], start: usize, prefix: u8) -> Option<(i64, usize)> {
    if *frame.get(start)? != prefix {
        return None;
    }
    let payload_start = start.checked_add(1)?;
    let mut payload_end = payload_start;
    while payload_end.checked_add(1)? < frame.len() {
        if frame[payload_end] == b'\r' && frame[payload_end + 1] == b'\n' {
            let value = parse_i64_ascii_bytes(&frame[payload_start..payload_end])?;
            return Some((value, payload_end + 2));
        }
        payload_end += 1;
    }
    None
}

fn parse_resp_bulk_string_at(frame: &[u8], start: usize) -> Option<(Option<Vec<u8>>, usize)> {
    let (len, payload_start) = parse_resp_length_at(frame, start, b'$')?;
    if len < 0 {
        return Some((None, payload_start));
    }
    let len = usize::try_from(len).ok()?;
    let payload_end = payload_start.checked_add(len)?;
    if frame.get(payload_end..payload_end.checked_add(2)?)? != b"\r\n" {
        return None;
    }
    Some((
        Some(frame[payload_start..payload_end].to_vec()),
        payload_end + 2,
    ))
}

fn parse_resp_integer(frame: &[u8]) -> Option<i64> {
    if frame.first().copied()? != b':' {
        return None;
    }
    let payload = frame.get(1..frame.len().checked_sub(2)?)?;
    let text = std::str::from_utf8(payload).ok()?;
    text.parse::<i64>().ok()
}

fn lmpop_pop_command(command: CommandId, args: &[ArgSlice]) -> Option<&'static [u8]> {
    let direction_index = match command {
        CommandId::Lmpop => {
            let numkeys = parse_u64_ascii(
                // SAFETY: args were parsed from the current request frame.
                arg_slice_bytes(args.get(1)?),
            )? as usize;
            2usize.checked_add(numkeys)?
        }
        CommandId::Blmpop => {
            let numkeys = parse_u64_ascii(
                // SAFETY: args were parsed from the current request frame.
                arg_slice_bytes(args.get(2)?),
            )? as usize;
            3usize.checked_add(numkeys)?
        }
        _ => return None,
    };
    // SAFETY: args were parsed from the current request frame.
    let direction = arg_slice_bytes(args.get(direction_index)?);
    if ascii_eq_ignore_case(direction, b"LEFT") {
        Some(b"LPOP")
    } else if ascii_eq_ignore_case(direction, b"RIGHT") {
        Some(b"RPOP")
    } else {
        None
    }
}

fn zmpop_pop_command(command: CommandId, args: &[ArgSlice]) -> Option<&'static [u8]> {
    let direction_index = match command {
        CommandId::Zmpop => {
            let numkeys = parse_u64_ascii(
                // SAFETY: args were parsed from the current request frame.
                arg_slice_bytes(args.get(1)?),
            )? as usize;
            2usize.checked_add(numkeys)?
        }
        CommandId::Bzmpop => {
            let numkeys = parse_u64_ascii(
                // SAFETY: args were parsed from the current request frame.
                arg_slice_bytes(args.get(2)?),
            )? as usize;
            3usize.checked_add(numkeys)?
        }
        _ => return None,
    };
    // SAFETY: args were parsed from the current request frame.
    let direction = arg_slice_bytes(args.get(direction_index)?);
    if ascii_eq_ignore_case(direction, b"MIN") {
        Some(b"ZPOPMIN")
    } else if ascii_eq_ignore_case(direction, b"MAX") {
        Some(b"ZPOPMAX")
    } else {
        None
    }
}

struct MpopReplicationMeta<'a> {
    key: &'a [u8],
    popped_count: usize,
}

fn parse_mpop_replication_meta(frame_response: &[u8]) -> Option<MpopReplicationMeta<'_>> {
    if frame_response == b"*-1\r\n" {
        return None;
    }

    let mut cursor = 0usize;
    if !frame_response.get(cursor..)?.starts_with(b"*2\r\n") {
        return None;
    }
    cursor += 4;

    let bulk_key = parse_resp_bulk_string(frame_response, cursor)?;
    let key = bulk_key.value;
    cursor = bulk_key.next_cursor;

    if frame_response.get(cursor)? != &b'*' {
        return None;
    }
    cursor += 1;
    let parsed = parse_resp_decimal(frame_response, cursor)?;
    Some(MpopReplicationMeta {
        key,
        popped_count: parsed.value,
    })
}

struct ParsedBulkString<'a> {
    value: &'a [u8],
    next_cursor: usize,
}

fn parse_resp_bulk_string<'a>(input: &'a [u8], cursor: usize) -> Option<ParsedBulkString<'a>> {
    if input.get(cursor)? != &b'$' {
        return None;
    }
    let parsed = parse_resp_decimal(input, cursor + 1)?;
    let mut cursor = parsed.next_cursor;
    let end = cursor.checked_add(parsed.value)?;
    let value = input.get(cursor..end)?;
    cursor = end;
    if input.get(cursor..cursor + 2)? != b"\r\n" {
        return None;
    }
    cursor += 2;
    Some(ParsedBulkString {
        value,
        next_cursor: cursor,
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ParsedDecimal {
    value: usize,
    next_cursor: usize,
}

fn parse_resp_decimal(input: &[u8], cursor: usize) -> Option<ParsedDecimal> {
    let mut index = cursor;
    while input.get(index)? != &b'\r' {
        if !input.get(index)?.is_ascii_digit() {
            return None;
        }
        index += 1;
    }
    if input.get(index + 1)? != &b'\n' {
        return None;
    }
    let value = std::str::from_utf8(input.get(cursor..index)?)
        .ok()?
        .parse::<usize>()
        .ok()?;
    Some(ParsedDecimal {
        value,
        next_cursor: index + 2,
    })
}

pub(crate) fn build_owner_thread_pool(
    processor: &Arc<RequestProcessor>,
) -> io::Result<Arc<ShardOwnerThreadPool>> {
    let inline_owner_execution = if let Ok(raw) = std::env::var(GARNET_OWNER_EXECUTION_INLINE_ENV) {
        parse_bool_env_flag(Some(raw.as_str()), GARNET_OWNER_EXECUTION_INLINE_ENV)?
    } else {
        owner_execution_inline_default().unwrap_or(false)
    };
    let owner_threads = parse_positive_env_usize(GARNET_STRING_OWNER_THREADS_ENV)
        .unwrap_or(DEFAULT_OWNER_THREAD_COUNT);
    let shard_count = processor.string_store_shard_count();
    if inline_owner_execution {
        let pool = ShardOwnerThreadPool::new_inline(shard_count).map_err(|error| {
            io::Error::other(format!(
                "owner-thread inline initialization failed (shards={}): {}",
                shard_count, error
            ))
        })?;
        return Ok(Arc::new(pool));
    }
    let owner_threads = owner_threads.min(shard_count);
    let pool = ShardOwnerThreadPool::new(owner_threads, shard_count).map_err(|error| {
        io::Error::other(format!(
            "owner-thread pool initialization failed (threads={}, shards={}): {}",
            owner_threads, shard_count, error
        ))
    })?;
    Ok(Arc::new(pool))
}

enum InlineFrameParse {
    Parsed {
        frame: Vec<u8>,
        bytes_consumed: usize,
    },
    Incomplete,
    UnbalancedQuotes,
    ProtocolError,
}

fn parse_inline_frame(input: &[u8]) -> InlineFrameParse {
    let Some(newline_offset) = input.iter().position(|byte| *byte == b'\n') else {
        if input.contains(&b'\0') {
            return InlineFrameParse::ProtocolError;
        }
        return InlineFrameParse::Incomplete;
    };

    let bytes_consumed = newline_offset + 1;
    let mut line = &input[..newline_offset];
    if line.contains(&b'\0') {
        return InlineFrameParse::ProtocolError;
    }
    if line.ends_with(b"\r") {
        line = &line[..line.len() - 1];
    }

    let tokens = match tokenize_inline_command(line) {
        Ok(tokens) if !tokens.is_empty() => tokens,
        Err(InlineTokenizeError::UnbalancedQuotes) => return InlineFrameParse::UnbalancedQuotes,
        _ => return InlineFrameParse::ProtocolError,
    };

    InlineFrameParse::Parsed {
        frame: encode_resp_frame(&tokens),
        bytes_consumed,
    }
}

fn consume_negative_multibulk_frame(input: &[u8]) -> Option<usize> {
    if !input.starts_with(b"*-") {
        return None;
    }
    let mut index = 2usize;
    if index >= input.len() {
        return None;
    }
    while index < input.len() {
        match input[index] {
            b'0'..=b'9' => index += 1,
            b'\r' => {
                if index + 1 >= input.len() {
                    return None;
                }
                if input[index + 1] != b'\n' {
                    return None;
                }
                return Some(index + 2);
            }
            _ => return None,
        }
    }
    None
}

fn append_resp_parse_error(response_out: &mut Vec<u8>, error: RespParseError) {
    match error {
        RespParseError::InvalidArrayLength
        | RespParseError::InvalidArrayPrefix { .. }
        | RespParseError::InvalidInteger { .. }
        | RespParseError::InvalidCrlf { .. } => {
            append_error_line(
                response_out,
                b"ERR Protocol error: invalid multibulk length",
            );
        }
        RespParseError::InvalidBulkPrefix { found, .. } => {
            let printable = if found.is_ascii_graphic() {
                found as char
            } else {
                '?'
            };
            append_error_line(
                response_out,
                format!("ERR Protocol error: expected '$', got '{}'", printable).as_bytes(),
            );
        }
        RespParseError::NegativeArrayLength { .. }
        | RespParseError::ArrayLengthOutOfRange { .. } => {
            append_error_line(
                response_out,
                b"ERR Protocol error: invalid multibulk length",
            );
        }
        RespParseError::InvalidBulkLength
        | RespParseError::NegativeBulkLength { .. }
        | RespParseError::BulkLengthOutOfRange { .. } => {
            append_error_line(response_out, b"ERR Protocol error: invalid bulk length");
        }
        _ => {
            append_error_line(response_out, b"ERR Protocol error");
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InlineTokenizeError {
    UnbalancedQuotes,
    InvalidInlineCommand,
}

fn tokenize_inline_command(line: &[u8]) -> Result<Vec<Vec<u8>>, InlineTokenizeError> {
    let mut tokens = Vec::new();
    let mut current = Vec::new();
    let mut quote: Option<u8> = None;
    let mut escaping = false;
    let mut just_closed_quote = false;

    for &byte in line {
        if byte == b'\0' {
            return Err(InlineTokenizeError::InvalidInlineCommand);
        }

        if escaping {
            current.push(byte);
            escaping = false;
            continue;
        }

        if byte == b'\\' {
            escaping = true;
            continue;
        }

        if let Some(quote_byte) = quote {
            if byte == quote_byte {
                quote = None;
                just_closed_quote = true;
            } else {
                current.push(byte);
            }
            continue;
        }

        if just_closed_quote {
            if byte.is_ascii_whitespace() {
                tokens.push(core::mem::take(&mut current));
                just_closed_quote = false;
                continue;
            }
            return Err(InlineTokenizeError::UnbalancedQuotes);
        }

        if byte == b'\'' || byte == b'"' {
            if !current.is_empty() {
                return Err(InlineTokenizeError::UnbalancedQuotes);
            }
            quote = Some(byte);
            continue;
        }

        if byte.is_ascii_whitespace() {
            if !current.is_empty() {
                tokens.push(core::mem::take(&mut current));
            }
            continue;
        }

        current.push(byte);
    }

    if escaping || quote.is_some() {
        return Err(InlineTokenizeError::UnbalancedQuotes);
    }
    if !current.is_empty() || just_closed_quote {
        tokens.push(current);
    }

    Ok(tokens)
}

fn encode_resp_frame(parts: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::with_capacity(resp_frame_capacity_from_lengths(
        parts.len(),
        parts.iter().map(Vec::len),
    ));
    append_array_length_frame(&mut out, parts.len());
    for part in parts {
        append_bulk_string_frame(&mut out, part);
    }
    out
}

fn encode_resp_frame_slices(parts: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::with_capacity(resp_frame_capacity_from_lengths(
        parts.len(),
        parts.iter().map(|part| part.len()),
    ));
    append_array_length_frame(&mut out, parts.len());
    for part in parts {
        append_bulk_string_frame(&mut out, part);
    }
    out
}

fn resp_frame_capacity_from_lengths(
    part_count: usize,
    part_lengths: impl Iterator<Item = usize>,
) -> usize {
    let mut capacity = 1 + decimal_ascii_len(part_count) + 2;
    for part_len in part_lengths {
        capacity += 1 + decimal_ascii_len(part_len) + 2 + part_len + 2;
    }
    capacity
}

fn decimal_ascii_len(mut value: usize) -> usize {
    let mut digits = 1usize;
    while value >= 10 {
        value /= 10;
        digits += 1;
    }
    digits
}

fn parse_positive_env_usize(key: &str) -> Option<usize> {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
}

fn parse_bool_env_flag(raw: Option<&str>, key: &str) -> io::Result<bool> {
    match raw {
        None => Ok(false),
        Some(value) => {
            let normalized = value.trim().to_ascii_lowercase();
            match normalized.as_str() {
                "1" | "true" | "yes" | "on" => Ok(true),
                "0" | "false" | "no" | "off" => Ok(false),
                _ => Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "invalid {key} `{value}`: expected one of 1/0/true/false/yes/no/on/off"
                    ),
                )),
            }
        }
    }
}

fn append_too_many_arguments_error(output: &mut Vec<u8>, max_resp_arguments: usize) {
    output.extend_from_slice(b"-ERR too many arguments in request (max ");
    append_usize_ascii_frame(output, max_resp_arguments);
    output.extend_from_slice(b")\r\n");
}

struct ConnectionLifecycle<'a> {
    metrics: &'a ServerMetrics,
    processor: &'a RequestProcessor,
    client_id: ClientId,
}

impl Drop for ConnectionLifecycle<'_> {
    fn drop(&mut self) {
        self.processor.unregister_pubsub_client(self.client_id);
        self.metrics.unregister_client(self.client_id);
        self.metrics
            .active_connections
            .fetch_sub(1, Ordering::Relaxed);
        self.metrics
            .closed_connections
            .fetch_add(1, Ordering::Relaxed);
        self.processor
            .set_connected_clients(self.metrics.connected_client_count());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use garnet_common::parse_resp_command_arg_slices;

    #[test]
    fn xreadgroup_claim_blocking_wait_detection_supports_claim_and_multiple_streams() {
        let mut args = [ArgSlice::EMPTY; 16];

        let blocking = encode_resp_frame(&[
            b"XREADGROUP".to_vec(),
            b"GROUP".to_vec(),
            b"g".to_vec(),
            b"c".to_vec(),
            b"BLOCK".to_vec(),
            b"100".to_vec(),
            b"CLAIM".to_vec(),
            b"50".to_vec(),
            b"STREAMS".to_vec(),
            b"s1".to_vec(),
            b"s2".to_vec(),
            b">".to_vec(),
            b">".to_vec(),
        ]);
        let blocking_meta = parse_resp_command_arg_slices(&blocking, &mut args).unwrap();
        let blocking_args = &args[..blocking_meta.argument_count];
        assert!(request_uses_blocking_wait(
            CommandId::Xreadgroup,
            blocking_args
        ));
        let wait_keys = xreadgroup_blocking_wait_keys(blocking_args, DbName::new(9));
        assert_eq!(wait_keys.len(), 2);
        assert_eq!(wait_keys[0].db(), DbName::new(9));
        assert_eq!(wait_keys[0].key().as_slice(), b"s1");
        assert_eq!(wait_keys[1].db(), DbName::new(9));
        assert_eq!(wait_keys[1].key().as_slice(), b"s2");

        let mixed_ids = encode_resp_frame(&[
            b"XREADGROUP".to_vec(),
            b"GROUP".to_vec(),
            b"g".to_vec(),
            b"c".to_vec(),
            b"BLOCK".to_vec(),
            b"100".to_vec(),
            b"CLAIM".to_vec(),
            b"50".to_vec(),
            b"STREAMS".to_vec(),
            b"s1".to_vec(),
            b"s2".to_vec(),
            b"0".to_vec(),
            b">".to_vec(),
        ]);
        let mixed_meta = parse_resp_command_arg_slices(&mixed_ids, &mut args).unwrap();
        let mixed_args = &args[..mixed_meta.argument_count];
        assert!(!request_uses_blocking_wait(
            CommandId::Xreadgroup,
            mixed_args
        ));
        assert!(xreadgroup_blocking_wait_keys(mixed_args, DbName::new(9)).is_empty());
    }

    #[test]
    fn eval_mutation_detection_skips_read_only_scripts() {
        let processor = RequestProcessor::new_with_string_store_shards_and_scripting(1, true)
            .expect("processor must initialize");

        let eval_read_only = encode_resp_frame(&[
            b"EVAL".to_vec(),
            b"return redis.call('SCAN', 0)".to_vec(),
            b"0".to_vec(),
        ]);
        let mut eval_args = [ArgSlice::EMPTY; 8];
        let eval_meta = parse_resp_command_arg_slices(&eval_read_only, &mut eval_args).unwrap();
        assert!(
            !command_effectively_mutated_for_replication(
                &processor,
                CommandId::Eval,
                &eval_args[..eval_meta.argument_count],
                b"*2\r\n$1\r\n0\r\n*0\r\n",
                None,
            ),
            "read-only EVAL should not force replication propagation"
        );

        let eval_write = encode_resp_frame(&[
            b"EVAL".to_vec(),
            b"return redis.call('SET', KEYS[1], ARGV[1])".to_vec(),
            b"1".to_vec(),
            b"k".to_vec(),
            b"v".to_vec(),
        ]);
        let mut eval_write_args = [ArgSlice::EMPTY; 8];
        let eval_write_meta =
            parse_resp_command_arg_slices(&eval_write, &mut eval_write_args).unwrap();
        assert!(command_effectively_mutated_for_replication(
            &processor,
            CommandId::Eval,
            &eval_write_args[..eval_write_meta.argument_count],
            b"+OK\r\n",
            None,
        ));
    }

    #[test]
    fn lazy_expire_nested_arbitrary_keys_policy_follows_config() {
        let processor = RequestProcessor::new().expect("processor must initialize");
        assert!(should_publish_lazy_expire_deletes(
            &processor,
            CommandId::Scan,
            false,
        ));
        assert!(should_publish_lazy_expire_deletes(
            &processor,
            CommandId::Eval,
            true,
        ));
        assert!(!should_publish_lazy_expire_deletes(
            &processor,
            CommandId::Set,
            true,
        ));

        processor.set_config_value(b"lazyexpire-nested-arbitrary-keys", b"no".to_vec());
        assert!(!should_publish_lazy_expire_deletes(
            &processor,
            CommandId::Eval,
            true,
        ));
        assert!(!should_publish_lazy_expire_deletes(
            &processor,
            CommandId::Exec,
            true,
        ));
    }

    #[test]
    fn parses_inline_frame_as_resp() {
        let input = b"SET key value\r\n";
        let InlineFrameParse::Parsed {
            frame,
            bytes_consumed,
        } = parse_inline_frame(input)
        else {
            panic!("inline frame should parse");
        };
        assert_eq!(bytes_consumed, input.len());

        let mut args = [ArgSlice::EMPTY; 8];
        let meta = parse_resp_command_arg_slices(&frame, &mut args).unwrap();
        assert_eq!(meta.argument_count, 3);
        // SAFETY: args reference `frame`, which is alive in this scope.
        assert_eq!(arg_slice_bytes(&args[0]), b"SET");
        // SAFETY: args reference `frame`, which is alive in this scope.
        assert_eq!(arg_slice_bytes(&args[1]), b"key");
        // SAFETY: args reference `frame`, which is alive in this scope.
        assert_eq!(arg_slice_bytes(&args[2]), b"value");
    }

    #[test]
    fn parses_inline_frame_with_quotes_and_escapes() {
        let input = b"SET \"key with space\" 'v\\'1'\r\n";
        let InlineFrameParse::Parsed { frame, .. } = parse_inline_frame(input) else {
            panic!("quoted inline frame should parse");
        };
        let mut args = [ArgSlice::EMPTY; 8];
        let meta = parse_resp_command_arg_slices(&frame, &mut args).unwrap();
        assert_eq!(meta.argument_count, 3);
        // SAFETY: args reference `frame`, which is alive in this scope.
        assert_eq!(arg_slice_bytes(&args[1]), b"key with space");
        // SAFETY: args reference `frame`, which is alive in this scope.
        assert_eq!(arg_slice_bytes(&args[2]), b"v'1");
    }

    #[test]
    fn inline_frame_waits_for_newline_before_parsing() {
        assert!(matches!(
            parse_inline_frame(b"SET key value"),
            InlineFrameParse::Incomplete
        ));
    }

    #[test]
    fn inline_frame_rejects_nul_without_newline() {
        assert!(matches!(
            parse_inline_frame(b"$\0"),
            InlineFrameParse::ProtocolError
        ));
    }

    #[test]
    fn replication_rewrites_evalsha_to_eval_with_script_body() {
        let processor = RequestProcessor::new_with_string_store_shards_and_scripting(1, true)
            .expect("processor must initialize");
        let script = b"redis.call('SET', KEYS[1], ARGV[1]); return ARGV[1]";

        let script_load_frame =
            encode_resp_frame(&[b"SCRIPT".to_vec(), b"LOAD".to_vec(), script.to_vec()]);
        let mut load_args = [ArgSlice::EMPTY; 8];
        let load_meta = parse_resp_command_arg_slices(&script_load_frame, &mut load_args).unwrap();
        let mut load_response = Vec::new();
        processor
            .execute_in_db(
                &load_args[..load_meta.argument_count],
                &mut load_response,
                DbName::fixture(),
            )
            .unwrap();
        assert!(load_response.starts_with(b"$40\r\n"));
        let sha = load_response[5..45].to_vec();

        let evalsha_frame = encode_resp_frame(&[
            b"EVALSHA".to_vec(),
            sha,
            b"1".to_vec(),
            b"repl:key".to_vec(),
            b"v1".to_vec(),
        ]);
        let mut evalsha_args = [ArgSlice::EMPTY; 8];
        let evalsha_meta =
            parse_resp_command_arg_slices(&evalsha_frame, &mut evalsha_args).unwrap();
        let rewritten = replication_frame_for_command(
            &processor,
            DbName::fixture(),
            CommandId::Evalsha,
            &evalsha_args[..evalsha_meta.argument_count],
            b"$2\r\nv1\r\n",
            &evalsha_frame,
        )
        .expect("rewritten frame must exist");

        let mut rewritten_args = [ArgSlice::EMPTY; 8];
        let rewritten_meta =
            parse_resp_command_arg_slices(&rewritten, &mut rewritten_args).unwrap();
        assert_eq!(rewritten_meta.argument_count, 5);
        // SAFETY: parsed arguments borrow from `rewritten`, alive for this assertion scope.
        assert_eq!(arg_slice_bytes(&rewritten_args[0]), b"EVAL");
        // SAFETY: parsed arguments borrow from `rewritten`, alive for this assertion scope.
        assert_eq!(arg_slice_bytes(&rewritten_args[1]), script);
    }

    #[test]
    fn replication_rewrites_set_and_getex_expire_forms() {
        let processor = RequestProcessor::new().expect("processor must initialize");

        let set_ex_frame = encode_resp_frame(&[
            b"SET".to_vec(),
            b"foo".to_vec(),
            b"bar".to_vec(),
            b"EX".to_vec(),
            b"100".to_vec(),
        ]);
        let mut set_ex_args = [ArgSlice::EMPTY; 8];
        let set_ex_meta = parse_resp_command_arg_slices(&set_ex_frame, &mut set_ex_args).unwrap();
        let mut set_ex_response = Vec::new();
        processor
            .execute_in_db(
                &set_ex_args[..set_ex_meta.argument_count],
                &mut set_ex_response,
                DbName::fixture(),
            )
            .unwrap();
        let rewritten_set = replication_frame_for_command(
            &processor,
            DbName::fixture(),
            CommandId::Set,
            &set_ex_args[..set_ex_meta.argument_count],
            &set_ex_response,
            &set_ex_frame,
        )
        .expect("SET EX replication rewrite must exist");
        let mut rewritten_set_args = [ArgSlice::EMPTY; 8];
        let rewritten_set_meta =
            parse_resp_command_arg_slices(&rewritten_set, &mut rewritten_set_args).unwrap();
        assert_eq!(rewritten_set_meta.argument_count, 5);
        assert_eq!(arg_slice_bytes(&rewritten_set_args[0]), b"SET");
        assert_eq!(arg_slice_bytes(&rewritten_set_args[1]), b"foo");
        assert_eq!(arg_slice_bytes(&rewritten_set_args[2]), b"bar");
        assert_eq!(arg_slice_bytes(&rewritten_set_args[3]), b"PXAT");

        let getex_persist_frame =
            encode_resp_frame(&[b"GETEX".to_vec(), b"foo".to_vec(), b"PERSIST".to_vec()]);
        let mut getex_persist_args = [ArgSlice::EMPTY; 8];
        let getex_persist_meta =
            parse_resp_command_arg_slices(&getex_persist_frame, &mut getex_persist_args).unwrap();
        let mut getex_persist_response = Vec::new();
        processor
            .execute_in_db(
                &getex_persist_args[..getex_persist_meta.argument_count],
                &mut getex_persist_response,
                DbName::fixture(),
            )
            .unwrap();
        let rewritten_persist = replication_frame_for_command(
            &processor,
            DbName::fixture(),
            CommandId::Getex,
            &getex_persist_args[..getex_persist_meta.argument_count],
            &getex_persist_response,
            &getex_persist_frame,
        )
        .expect("GETEX PERSIST rewrite must exist");
        let mut rewritten_persist_args = [ArgSlice::EMPTY; 8];
        let rewritten_persist_meta =
            parse_resp_command_arg_slices(&rewritten_persist, &mut rewritten_persist_args).unwrap();
        assert_eq!(rewritten_persist_meta.argument_count, 2);
        assert_eq!(arg_slice_bytes(&rewritten_persist_args[0]), b"PERSIST");
        assert_eq!(arg_slice_bytes(&rewritten_persist_args[1]), b"foo");

        let set_frame = encode_resp_frame(&[b"SET".to_vec(), b"foo".to_vec(), b"bar".to_vec()]);
        let mut set_args = [ArgSlice::EMPTY; 8];
        let set_meta = parse_resp_command_arg_slices(&set_frame, &mut set_args).unwrap();
        let mut set_response = Vec::new();
        processor
            .execute_in_db(
                &set_args[..set_meta.argument_count],
                &mut set_response,
                DbName::fixture(),
            )
            .unwrap();
        assert_eq!(set_response, b"+OK\r\n");

        let now_millis = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_millis() as u64)
            .unwrap_or(0);
        let expired_millis = now_millis.saturating_sub(1000).to_string().into_bytes();
        let getex_expired_frame = encode_resp_frame(&[
            b"GETEX".to_vec(),
            b"foo".to_vec(),
            b"PXAT".to_vec(),
            expired_millis,
        ]);
        let mut getex_expired_args = [ArgSlice::EMPTY; 8];
        let getex_expired_meta =
            parse_resp_command_arg_slices(&getex_expired_frame, &mut getex_expired_args).unwrap();
        let mut getex_expired_response = Vec::new();
        processor
            .execute_in_db(
                &getex_expired_args[..getex_expired_meta.argument_count],
                &mut getex_expired_response,
                DbName::fixture(),
            )
            .unwrap();
        let rewritten_del = replication_frame_for_command(
            &processor,
            DbName::fixture(),
            CommandId::Getex,
            &getex_expired_args[..getex_expired_meta.argument_count],
            &getex_expired_response,
            &getex_expired_frame,
        )
        .expect("GETEX past-PXAT rewrite must exist");
        let mut rewritten_del_args = [ArgSlice::EMPTY; 8];
        let rewritten_del_meta =
            parse_resp_command_arg_slices(&rewritten_del, &mut rewritten_del_args).unwrap();
        assert_eq!(rewritten_del_meta.argument_count, 2);
        assert_eq!(arg_slice_bytes(&rewritten_del_args[0]), b"DEL");
        assert_eq!(arg_slice_bytes(&rewritten_del_args[1]), b"foo");
    }

    #[test]
    fn replication_rewrites_lmpop_to_pop_with_count() {
        let processor = RequestProcessor::new().expect("processor must initialize");
        let lmpop_frame = encode_resp_frame(&[
            b"LMPOP".to_vec(),
            b"1".to_vec(),
            b"mylist".to_vec(),
            b"LEFT".to_vec(),
            b"COUNT".to_vec(),
            b"2".to_vec(),
        ]);
        let mut lmpop_args = [ArgSlice::EMPTY; 8];
        let lmpop_meta = parse_resp_command_arg_slices(&lmpop_frame, &mut lmpop_args).unwrap();
        let lmpop_response = b"*2\r\n$6\r\nmylist\r\n*2\r\n$1\r\na\r\n$1\r\nb\r\n";

        let rewritten = replication_frame_for_command(
            &processor,
            DbName::fixture(),
            CommandId::Lmpop,
            &lmpop_args[..lmpop_meta.argument_count],
            lmpop_response,
            &lmpop_frame,
        )
        .expect("LMPOP replication rewrite must exist");

        let mut rewritten_args = [ArgSlice::EMPTY; 8];
        let rewritten_meta =
            parse_resp_command_arg_slices(&rewritten, &mut rewritten_args).unwrap();
        assert_eq!(rewritten_meta.argument_count, 3);
        assert_eq!(arg_slice_bytes(&rewritten_args[0]), b"LPOP");
        assert_eq!(arg_slice_bytes(&rewritten_args[1]), b"mylist");
        assert_eq!(arg_slice_bytes(&rewritten_args[2]), b"2");
    }

    #[test]
    fn replication_rewrites_zmpop_to_pop_with_count() {
        let processor = RequestProcessor::new().expect("processor must initialize");
        let zmpop_frame = encode_resp_frame(&[
            b"ZMPOP".to_vec(),
            b"2".to_vec(),
            b"zset1".to_vec(),
            b"zset2".to_vec(),
            b"MAX".to_vec(),
            b"COUNT".to_vec(),
            b"10".to_vec(),
        ]);
        let mut zmpop_args = [ArgSlice::EMPTY; 8];
        let zmpop_meta = parse_resp_command_arg_slices(&zmpop_frame, &mut zmpop_args).unwrap();
        let zmpop_response =
            b"*2\r\n$5\r\nzset2\r\n*3\r\n*2\r\n$4\r\nfour\r\n$1\r\n4\r\n*2\r\n$4\r\nfive\r\n$1\r\n5\r\n*2\r\n$3\r\nsix\r\n$1\r\n6\r\n";

        let rewritten = replication_frame_for_command(
            &processor,
            DbName::fixture(),
            CommandId::Zmpop,
            &zmpop_args[..zmpop_meta.argument_count],
            zmpop_response,
            &zmpop_frame,
        )
        .expect("ZMPOP replication rewrite must exist");

        let mut rewritten_args = [ArgSlice::EMPTY; 8];
        let rewritten_meta =
            parse_resp_command_arg_slices(&rewritten, &mut rewritten_args).unwrap();
        assert_eq!(rewritten_meta.argument_count, 3);
        assert_eq!(arg_slice_bytes(&rewritten_args[0]), b"ZPOPMAX");
        assert_eq!(arg_slice_bytes(&rewritten_args[1]), b"zset2");
        assert_eq!(arg_slice_bytes(&rewritten_args[2]), b"3");
    }

    #[test]
    fn replication_rewrites_spop_to_srem_and_del_or_unlink() {
        let processor = RequestProcessor::new().expect("processor must initialize");
        let mut args = [ArgSlice::EMPTY; 128];
        let mut response = Vec::new();

        let sadd_partial = encode_resp_frame(&[
            b"SADD".to_vec(),
            b"set1".to_vec(),
            b"a".to_vec(),
            b"b".to_vec(),
            b"c".to_vec(),
            b"d".to_vec(),
        ]);
        let sadd_partial_meta = parse_resp_command_arg_slices(&sadd_partial, &mut args).unwrap();
        processor
            .execute_in_db(
                &args[..sadd_partial_meta.argument_count],
                &mut response,
                DbName::fixture(),
            )
            .unwrap();
        assert_eq!(response, b":4\r\n");

        response.clear();
        let spop_partial = encode_resp_frame(&[b"SPOP".to_vec(), b"set1".to_vec(), b"2".to_vec()]);
        let spop_partial_meta = parse_resp_command_arg_slices(&spop_partial, &mut args).unwrap();
        processor
            .execute_in_db(
                &args[..spop_partial_meta.argument_count],
                &mut response,
                DbName::fixture(),
            )
            .unwrap();
        let expected_members =
            parse_spop_response_members(&response).expect("SPOP response should parse");
        assert_eq!(expected_members.len(), 2);
        let rewritten_srem = replication_frame_for_command(
            &processor,
            DbName::fixture(),
            CommandId::Spop,
            &args[..spop_partial_meta.argument_count],
            &response,
            &spop_partial,
        )
        .expect("SPOP partial rewrite should exist");
        let mut rewritten_srem_args = [ArgSlice::EMPTY; 8];
        let rewritten_srem_meta =
            parse_resp_command_arg_slices(&rewritten_srem, &mut rewritten_srem_args).unwrap();
        assert_eq!(rewritten_srem_meta.argument_count, 4);
        assert_eq!(arg_slice_bytes(&rewritten_srem_args[0]), b"SREM");
        assert_eq!(arg_slice_bytes(&rewritten_srem_args[1]), b"set1");
        assert_eq!(
            arg_slice_bytes(&rewritten_srem_args[2]),
            expected_members[0].as_slice()
        );
        assert_eq!(
            arg_slice_bytes(&rewritten_srem_args[3]),
            expected_members[1].as_slice()
        );

        response.clear();
        processor.set_resp_protocol_version(RespProtocolVersion::Resp3);
        let sadd_resp3 = encode_resp_frame(&[
            b"SADD".to_vec(),
            b"set2".to_vec(),
            b"1".to_vec(),
            b"2".to_vec(),
            b"3".to_vec(),
        ]);
        let sadd_resp3_meta = parse_resp_command_arg_slices(&sadd_resp3, &mut args).unwrap();
        processor
            .execute_in_db(
                &args[..sadd_resp3_meta.argument_count],
                &mut response,
                DbName::fixture(),
            )
            .unwrap();
        assert_eq!(response, b":3\r\n");

        response.clear();
        let spop_resp3 = encode_resp_frame(&[b"SPOP".to_vec(), b"set2".to_vec(), b"2".to_vec()]);
        let spop_resp3_meta = parse_resp_command_arg_slices(&spop_resp3, &mut args).unwrap();
        processor
            .execute_in_db(
                &args[..spop_resp3_meta.argument_count],
                &mut response,
                DbName::fixture(),
            )
            .unwrap();
        assert!(
            response.starts_with(b"~2\r\n"),
            "RESP3 SPOP count should return set type, got: {:?}",
            String::from_utf8_lossy(&response)
        );
        let rewritten_resp3 = replication_frame_for_command(
            &processor,
            DbName::fixture(),
            CommandId::Spop,
            &args[..spop_resp3_meta.argument_count],
            &response,
            &spop_resp3,
        )
        .expect("RESP3 SPOP partial rewrite should exist");
        let mut rewritten_resp3_args = [ArgSlice::EMPTY; 8];
        let rewritten_resp3_meta =
            parse_resp_command_arg_slices(&rewritten_resp3, &mut rewritten_resp3_args).unwrap();
        assert_eq!(rewritten_resp3_meta.argument_count, 4);
        assert_eq!(arg_slice_bytes(&rewritten_resp3_args[0]), b"SREM");
        assert_eq!(arg_slice_bytes(&rewritten_resp3_args[1]), b"set2");
        processor.set_resp_protocol_version(RespProtocolVersion::Resp2);

        response.clear();
        let sadd_del = encode_resp_frame(&[
            b"SADD".to_vec(),
            b"set3".to_vec(),
            b"1".to_vec(),
            b"2".to_vec(),
            b"3".to_vec(),
            b"4".to_vec(),
            b"5".to_vec(),
        ]);
        let sadd_del_meta = parse_resp_command_arg_slices(&sadd_del, &mut args).unwrap();
        processor
            .execute_in_db(
                &args[..sadd_del_meta.argument_count],
                &mut response,
                DbName::fixture(),
            )
            .unwrap();
        assert_eq!(response, b":5\r\n");

        response.clear();
        let spop_del = encode_resp_frame(&[b"SPOP".to_vec(), b"set3".to_vec(), b"5".to_vec()]);
        let spop_del_meta = parse_resp_command_arg_slices(&spop_del, &mut args).unwrap();
        processor
            .execute_in_db(
                &args[..spop_del_meta.argument_count],
                &mut response,
                DbName::fixture(),
            )
            .unwrap();
        let rewritten_del = replication_frame_for_command(
            &processor,
            DbName::fixture(),
            CommandId::Spop,
            &args[..spop_del_meta.argument_count],
            &response,
            &spop_del,
        )
        .expect("SPOP full-removal DEL rewrite should exist");
        let mut rewritten_del_args = [ArgSlice::EMPTY; 8];
        let rewritten_del_meta =
            parse_resp_command_arg_slices(&rewritten_del, &mut rewritten_del_args).unwrap();
        assert_eq!(rewritten_del_meta.argument_count, 2);
        assert_eq!(arg_slice_bytes(&rewritten_del_args[0]), b"DEL");
        assert_eq!(arg_slice_bytes(&rewritten_del_args[1]), b"set3");

        response.clear();
        let config_lazyfree = encode_resp_frame(&[
            b"CONFIG".to_vec(),
            b"SET".to_vec(),
            b"lazyfree-lazy-server-del".to_vec(),
            b"yes".to_vec(),
        ]);
        let config_lazyfree_meta =
            parse_resp_command_arg_slices(&config_lazyfree, &mut args).unwrap();
        processor
            .execute_in_db(
                &args[..config_lazyfree_meta.argument_count],
                &mut response,
                DbName::fixture(),
            )
            .unwrap();
        assert_eq!(response, b"+OK\r\n");

        response.clear();
        let mut sadd_unlink_args = vec![b"SADD".to_vec(), b"set4".to_vec()];
        for value in 1..=65 {
            sadd_unlink_args.push(value.to_string().into_bytes());
        }
        let sadd_unlink = encode_resp_frame(&sadd_unlink_args);
        let sadd_unlink_meta = parse_resp_command_arg_slices(&sadd_unlink, &mut args).unwrap();
        processor
            .execute_in_db(
                &args[..sadd_unlink_meta.argument_count],
                &mut response,
                DbName::fixture(),
            )
            .unwrap();
        assert_eq!(response, b":65\r\n");

        response.clear();
        let spop_unlink = encode_resp_frame(&[b"SPOP".to_vec(), b"set4".to_vec(), b"65".to_vec()]);
        let spop_unlink_meta = parse_resp_command_arg_slices(&spop_unlink, &mut args).unwrap();
        processor
            .execute_in_db(
                &args[..spop_unlink_meta.argument_count],
                &mut response,
                DbName::fixture(),
            )
            .unwrap();
        let rewritten_unlink = replication_frame_for_command(
            &processor,
            DbName::fixture(),
            CommandId::Spop,
            &args[..spop_unlink_meta.argument_count],
            &response,
            &spop_unlink,
        )
        .expect("SPOP full-removal UNLINK rewrite should exist");
        let mut rewritten_unlink_args = [ArgSlice::EMPTY; 8];
        let rewritten_unlink_meta =
            parse_resp_command_arg_slices(&rewritten_unlink, &mut rewritten_unlink_args).unwrap();
        assert_eq!(rewritten_unlink_meta.argument_count, 2);
        assert_eq!(arg_slice_bytes(&rewritten_unlink_args[0]), b"UNLINK");
        assert_eq!(arg_slice_bytes(&rewritten_unlink_args[1]), b"set4");
    }

    #[test]
    fn replication_rewrites_restore_past_absttl_to_del_or_unlink() {
        let processor = RequestProcessor::new().expect("processor must initialize");
        let dump_payload = b"GRN1\x00\x05\x00\x00\x00value".to_vec();
        let mut args = [ArgSlice::EMPTY; 16];
        let mut response = Vec::new();

        let set_key1 = encode_resp_frame(&[b"SET".to_vec(), b"key1".to_vec(), b"old".to_vec()]);
        let set_key1_meta = parse_resp_command_arg_slices(&set_key1, &mut args).unwrap();
        processor
            .execute_in_db(
                &args[..set_key1_meta.argument_count],
                &mut response,
                DbName::fixture(),
            )
            .unwrap();
        assert_eq!(response, b"+OK\r\n");

        let now_millis = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_millis() as u64)
            .unwrap_or(0);
        let past_millis = now_millis.saturating_sub(1000).to_string().into_bytes();

        response.clear();
        let restore_key1 = encode_resp_frame(&[
            b"RESTORE".to_vec(),
            b"key1".to_vec(),
            past_millis.clone(),
            dump_payload.clone(),
            b"REPLACE".to_vec(),
            b"ABSTTL".to_vec(),
        ]);
        let restore_key1_meta = parse_resp_command_arg_slices(&restore_key1, &mut args).unwrap();
        processor
            .execute_in_db(
                &args[..restore_key1_meta.argument_count],
                &mut response,
                DbName::fixture(),
            )
            .unwrap();
        assert_eq!(response, b"+OK\r\n");
        let rewritten_del = replication_frame_for_command(
            &processor,
            DbName::fixture(),
            CommandId::Restore,
            &args[..restore_key1_meta.argument_count],
            &response,
            &restore_key1,
        )
        .expect("RESTORE rewrite should exist");
        let mut rewritten_del_args = [ArgSlice::EMPTY; 8];
        let rewritten_del_meta =
            parse_resp_command_arg_slices(&rewritten_del, &mut rewritten_del_args).unwrap();
        assert_eq!(rewritten_del_meta.argument_count, 2);
        assert_eq!(arg_slice_bytes(&rewritten_del_args[0]), b"DEL");
        assert_eq!(arg_slice_bytes(&rewritten_del_args[1]), b"key1");

        response.clear();
        let config_lazyfree = encode_resp_frame(&[
            b"CONFIG".to_vec(),
            b"SET".to_vec(),
            b"lazyfree-lazy-server-del".to_vec(),
            b"yes".to_vec(),
        ]);
        let config_lazyfree_meta =
            parse_resp_command_arg_slices(&config_lazyfree, &mut args).unwrap();
        processor
            .execute_in_db(
                &args[..config_lazyfree_meta.argument_count],
                &mut response,
                DbName::fixture(),
            )
            .unwrap();
        assert_eq!(response, b"+OK\r\n");

        response.clear();
        let set_key2 = encode_resp_frame(&[b"SET".to_vec(), b"key2".to_vec(), b"old".to_vec()]);
        let set_key2_meta = parse_resp_command_arg_slices(&set_key2, &mut args).unwrap();
        processor
            .execute_in_db(
                &args[..set_key2_meta.argument_count],
                &mut response,
                DbName::fixture(),
            )
            .unwrap();
        assert_eq!(response, b"+OK\r\n");

        response.clear();
        let restore_key2 = encode_resp_frame(&[
            b"RESTORE".to_vec(),
            b"key2".to_vec(),
            past_millis,
            dump_payload,
            b"REPLACE".to_vec(),
            b"ABSTTL".to_vec(),
        ]);
        let restore_key2_meta = parse_resp_command_arg_slices(&restore_key2, &mut args).unwrap();
        processor
            .execute_in_db(
                &args[..restore_key2_meta.argument_count],
                &mut response,
                DbName::fixture(),
            )
            .unwrap();
        assert_eq!(response, b"+OK\r\n");
        let rewritten_unlink = replication_frame_for_command(
            &processor,
            DbName::fixture(),
            CommandId::Restore,
            &args[..restore_key2_meta.argument_count],
            &response,
            &restore_key2,
        )
        .expect("RESTORE rewrite should exist");
        let mut rewritten_unlink_args = [ArgSlice::EMPTY; 8];
        let rewritten_unlink_meta =
            parse_resp_command_arg_slices(&rewritten_unlink, &mut rewritten_unlink_args).unwrap();
        assert_eq!(rewritten_unlink_meta.argument_count, 2);
        assert_eq!(arg_slice_bytes(&rewritten_unlink_args[0]), b"UNLINK");
        assert_eq!(arg_slice_bytes(&rewritten_unlink_args[1]), b"key2");
    }

    #[test]
    fn client_help_is_supported_in_connection_handler_path() {
        let processor = RequestProcessor::new().expect("processor must initialize");
        let metrics = ServerMetrics::default();
        let mut client_state = ClientConnectionState::default();
        let frame = encode_resp_frame(&[b"CLIENT".to_vec(), b"HELP".to_vec()]);
        let mut args = [ArgSlice::EMPTY; 8];
        let meta = parse_resp_command_arg_slices(&frame, &mut args).unwrap();
        let mut response = Vec::new();

        let outcome = handle_client_command(
            &processor,
            &metrics,
            ClientId::new(1),
            &args[..meta.argument_count],
            &mut client_state,
            &mut response,
        );
        assert!(!outcome.disconnect_after_reply);
        assert!(
            String::from_utf8_lossy(&response).contains("CLIENT <subcommand>"),
            "unexpected CLIENT HELP payload: {}",
            String::from_utf8_lossy(&response)
        );
    }

    #[test]
    fn client_id_returns_connection_id() {
        let processor = RequestProcessor::new().expect("processor must initialize");
        let metrics = ServerMetrics::default();
        let mut client_state = ClientConnectionState::default();
        let frame = encode_resp_frame(&[b"CLIENT".to_vec(), b"ID".to_vec()]);
        let mut args = [ArgSlice::EMPTY; 8];
        let meta = parse_resp_command_arg_slices(&frame, &mut args).unwrap();
        let mut response = Vec::new();

        handle_client_command(
            &processor,
            &metrics,
            ClientId::new(42),
            &args[..meta.argument_count],
            &mut client_state,
            &mut response,
        );
        assert_eq!(response, b":42\r\n");
    }

    #[test]
    fn client_setname_and_getname_round_trip() {
        let processor = RequestProcessor::new().expect("processor must initialize");
        let metrics = ServerMetrics::default();
        let client_id = metrics.register_client(None, None);
        let mut client_state = ClientConnectionState::default();
        let mut args = [ArgSlice::EMPTY; 8];
        let mut response = Vec::new();

        // GETNAME returns null before any name is set.
        let frame = encode_resp_frame(&[b"CLIENT".to_vec(), b"GETNAME".to_vec()]);
        let meta = parse_resp_command_arg_slices(&frame, &mut args).unwrap();
        handle_client_command(
            &processor,
            &metrics,
            client_id,
            &args[..meta.argument_count],
            &mut client_state,
            &mut response,
        );
        assert_eq!(response, b"$-1\r\n");

        // SETNAME sets the connection name.
        response.clear();
        let frame =
            encode_resp_frame(&[b"CLIENT".to_vec(), b"SETNAME".to_vec(), b"myconn".to_vec()]);
        let meta = parse_resp_command_arg_slices(&frame, &mut args).unwrap();
        handle_client_command(
            &processor,
            &metrics,
            client_id,
            &args[..meta.argument_count],
            &mut client_state,
            &mut response,
        );
        assert_eq!(response, b"+OK\r\n");

        // GETNAME returns the name that was set.
        response.clear();
        let frame = encode_resp_frame(&[b"CLIENT".to_vec(), b"GETNAME".to_vec()]);
        let meta = parse_resp_command_arg_slices(&frame, &mut args).unwrap();
        handle_client_command(
            &processor,
            &metrics,
            client_id,
            &args[..meta.argument_count],
            &mut client_state,
            &mut response,
        );
        assert_eq!(response, b"$6\r\nmyconn\r\n");

        // SETNAME with empty string clears the name.
        response.clear();
        let frame = encode_resp_frame(&[b"CLIENT".to_vec(), b"SETNAME".to_vec(), b"".to_vec()]);
        let meta = parse_resp_command_arg_slices(&frame, &mut args).unwrap();
        handle_client_command(
            &processor,
            &metrics,
            client_id,
            &args[..meta.argument_count],
            &mut client_state,
            &mut response,
        );
        assert_eq!(response, b"+OK\r\n");

        // GETNAME returns null after name is cleared.
        response.clear();
        let frame = encode_resp_frame(&[b"CLIENT".to_vec(), b"GETNAME".to_vec()]);
        let meta = parse_resp_command_arg_slices(&frame, &mut args).unwrap();
        handle_client_command(
            &processor,
            &metrics,
            client_id,
            &args[..meta.argument_count],
            &mut client_state,
            &mut response,
        );
        assert_eq!(response, b"$-1\r\n");
    }

    #[test]
    fn client_setname_rejects_spaces() {
        let processor = RequestProcessor::new().expect("processor must initialize");
        let metrics = ServerMetrics::default();
        let mut client_state = ClientConnectionState::default();
        let frame = encode_resp_frame(&[
            b"CLIENT".to_vec(),
            b"SETNAME".to_vec(),
            b"bad name".to_vec(),
        ]);
        let mut args = [ArgSlice::EMPTY; 8];
        let meta = parse_resp_command_arg_slices(&frame, &mut args).unwrap();
        let mut response = Vec::new();

        handle_client_command(
            &processor,
            &metrics,
            ClientId::new(1),
            &args[..meta.argument_count],
            &mut client_state,
            &mut response,
        );
        let response_str = String::from_utf8_lossy(&response);
        assert!(
            response_str.starts_with("-ERR"),
            "expected error for name with space: {}",
            response_str,
        );
    }

    #[test]
    fn client_no_evict_on_off() {
        let processor = RequestProcessor::new().expect("processor must initialize");
        let metrics = ServerMetrics::default();
        let mut client_state = ClientConnectionState::default();
        let mut args = [ArgSlice::EMPTY; 8];
        let mut response = Vec::new();

        assert!(!client_state.no_evict);

        let frame = encode_resp_frame(&[b"CLIENT".to_vec(), b"NO-EVICT".to_vec(), b"ON".to_vec()]);
        let meta = parse_resp_command_arg_slices(&frame, &mut args).unwrap();
        handle_client_command(
            &processor,
            &metrics,
            ClientId::new(1),
            &args[..meta.argument_count],
            &mut client_state,
            &mut response,
        );
        assert_eq!(response, b"+OK\r\n");
        assert!(client_state.no_evict);

        response.clear();
        let frame = encode_resp_frame(&[b"CLIENT".to_vec(), b"NO-EVICT".to_vec(), b"OFF".to_vec()]);
        let meta = parse_resp_command_arg_slices(&frame, &mut args).unwrap();
        handle_client_command(
            &processor,
            &metrics,
            ClientId::new(1),
            &args[..meta.argument_count],
            &mut client_state,
            &mut response,
        );
        assert_eq!(response, b"+OK\r\n");
        assert!(!client_state.no_evict);
    }

    #[test]
    fn client_setname_empty_clears_name() {
        let processor = RequestProcessor::new().expect("processor must initialize");
        let metrics = ServerMetrics::default();
        let client_id = metrics.register_client(None, None);
        let mut client_state = ClientConnectionState::default();
        let mut args = [ArgSlice::EMPTY; 8];
        let mut response = Vec::new();

        // SETNAME "myname"
        let frame =
            encode_resp_frame(&[b"CLIENT".to_vec(), b"SETNAME".to_vec(), b"myname".to_vec()]);
        let meta = parse_resp_command_arg_slices(&frame, &mut args).unwrap();
        handle_client_command(
            &processor,
            &metrics,
            client_id,
            &args[..meta.argument_count],
            &mut client_state,
            &mut response,
        );
        assert_eq!(response, b"+OK\r\n");

        // SETNAME "" should clear the name.
        response.clear();
        let frame = encode_resp_frame(&[b"CLIENT".to_vec(), b"SETNAME".to_vec(), b"".to_vec()]);
        let meta = parse_resp_command_arg_slices(&frame, &mut args).unwrap();
        handle_client_command(
            &processor,
            &metrics,
            client_id,
            &args[..meta.argument_count],
            &mut client_state,
            &mut response,
        );
        assert_eq!(response, b"+OK\r\n");

        // GETNAME should return null after clearing.
        response.clear();
        let frame = encode_resp_frame(&[b"CLIENT".to_vec(), b"GETNAME".to_vec()]);
        let meta = parse_resp_command_arg_slices(&frame, &mut args).unwrap();
        handle_client_command(
            &processor,
            &metrics,
            client_id,
            &args[..meta.argument_count],
            &mut client_state,
            &mut response,
        );
        assert_eq!(response, b"$-1\r\n");
    }

    #[test]
    fn client_info_returns_bulk_with_id() {
        let processor = RequestProcessor::new().expect("processor must initialize");
        let metrics = ServerMetrics::default();
        let client_id = metrics.register_client(None, None);
        let mut client_state = ClientConnectionState::default();
        let frame = encode_resp_frame(&[b"CLIENT".to_vec(), b"INFO".to_vec()]);
        let mut args = [ArgSlice::EMPTY; 8];
        let meta = parse_resp_command_arg_slices(&frame, &mut args).unwrap();
        let mut response = Vec::new();

        handle_client_command(
            &processor,
            &metrics,
            client_id,
            &args[..meta.argument_count],
            &mut client_state,
            &mut response,
        );

        let response_str = String::from_utf8_lossy(&response);
        assert!(
            response_str.starts_with('$'),
            "CLIENT INFO should return a bulk string, got: {}",
            response_str,
        );
        assert!(
            response_str.contains("id="),
            "CLIENT INFO payload should contain id= field, got: {}",
            response_str,
        );
    }

    #[test]
    fn client_list_returns_connection_info() {
        let processor = RequestProcessor::new().expect("processor must initialize");
        let metrics = ServerMetrics::default();
        let client_id = metrics.register_client(None, None);
        let mut client_state = ClientConnectionState::default();
        let frame = encode_resp_frame(&[b"CLIENT".to_vec(), b"LIST".to_vec()]);
        let mut args = [ArgSlice::EMPTY; 8];
        let meta = parse_resp_command_arg_slices(&frame, &mut args).unwrap();
        let mut response = Vec::new();

        handle_client_command(
            &processor,
            &metrics,
            client_id,
            &args[..meta.argument_count],
            &mut client_state,
            &mut response,
        );

        let response_str = String::from_utf8_lossy(&response);
        assert!(
            response_str.starts_with('$'),
            "CLIENT LIST should return a bulk string, got: {}",
            response_str,
        );
        assert!(
            response_str.contains("id="),
            "CLIENT LIST payload should contain id= field, got: {}",
            response_str,
        );
    }

    #[test]
    fn client_setinfo_lib_name_round_trip() {
        let processor = RequestProcessor::new().expect("processor must initialize");
        let metrics = ServerMetrics::default();
        let client_id = metrics.register_client(None, None);
        let mut client_state = ClientConnectionState::default();
        let mut args = [ArgSlice::EMPTY; 8];
        let mut response = Vec::new();

        // CLIENT SETINFO LIB-NAME "mylib"
        let frame = encode_resp_frame(&[
            b"CLIENT".to_vec(),
            b"SETINFO".to_vec(),
            b"LIB-NAME".to_vec(),
            b"mylib".to_vec(),
        ]);
        let meta = parse_resp_command_arg_slices(&frame, &mut args).unwrap();
        handle_client_command(
            &processor,
            &metrics,
            client_id,
            &args[..meta.argument_count],
            &mut client_state,
            &mut response,
        );
        assert_eq!(response, b"+OK\r\n");

        // CLIENT INFO should include lib-name=mylib
        response.clear();
        let frame = encode_resp_frame(&[b"CLIENT".to_vec(), b"INFO".to_vec()]);
        let meta = parse_resp_command_arg_slices(&frame, &mut args).unwrap();
        handle_client_command(
            &processor,
            &metrics,
            client_id,
            &args[..meta.argument_count],
            &mut client_state,
            &mut response,
        );

        let response_str = String::from_utf8_lossy(&response);
        assert!(
            response_str.contains("lib-name=mylib"),
            "CLIENT INFO should contain lib-name=mylib, got: {}",
            response_str,
        );
    }

    #[test]
    fn client_setname_rejects_newlines() {
        let processor = RequestProcessor::new().expect("processor must initialize");
        let metrics = ServerMetrics::default();
        let mut client_state = ClientConnectionState::default();
        let frame = encode_resp_frame(&[
            b"CLIENT".to_vec(),
            b"SETNAME".to_vec(),
            b"bad\nname".to_vec(),
        ]);
        let mut args = [ArgSlice::EMPTY; 8];
        let meta = parse_resp_command_arg_slices(&frame, &mut args).unwrap();
        let mut response = Vec::new();

        handle_client_command(
            &processor,
            &metrics,
            ClientId::new(1),
            &args[..meta.argument_count],
            &mut client_state,
            &mut response,
        );
        let response_str = String::from_utf8_lossy(&response);
        assert!(
            response_str.starts_with("-ERR"),
            "expected error for name with newline: {}",
            response_str,
        );
    }

    #[test]
    fn client_kill_invalid_id_returns_error() {
        let processor = RequestProcessor::new().expect("processor must initialize");
        let metrics = ServerMetrics::default();
        let mut client_state = ClientConnectionState::default();
        let frame = encode_resp_frame(&[
            b"CLIENT".to_vec(),
            b"KILL".to_vec(),
            b"ID".to_vec(),
            b"notanumber".to_vec(),
        ]);
        let mut args = [ArgSlice::EMPTY; 8];
        let meta = parse_resp_command_arg_slices(&frame, &mut args).unwrap();
        let mut response = Vec::new();

        handle_client_command(
            &processor,
            &metrics,
            ClientId::new(1),
            &args[..meta.argument_count],
            &mut client_state,
            &mut response,
        );
        let response_str = String::from_utf8_lossy(&response);
        assert!(
            response_str.starts_with("-ERR"),
            "expected error for invalid CLIENT KILL ID: {}",
            response_str,
        );
    }

    #[test]
    fn client_trackinginfo_and_getredir_follow_connection_state() {
        let processor = RequestProcessor::new().expect("processor must initialize");
        let metrics = ServerMetrics::default();
        let client_id = metrics.register_client(None, None);
        let mut client_state = ClientConnectionState::default();
        let mut args = [ArgSlice::EMPTY; 16];
        let mut response = Vec::new();

        let trackinginfo_frame = encode_resp_frame(&[b"CLIENT".to_vec(), b"TRACKINGINFO".to_vec()]);
        let trackinginfo_meta =
            parse_resp_command_arg_slices(&trackinginfo_frame, &mut args).unwrap();
        handle_client_command(
            &processor,
            &metrics,
            client_id,
            &args[..trackinginfo_meta.argument_count],
            &mut client_state,
            &mut response,
        );
        assert_eq!(
            response,
            b"*6\r\n$5\r\nflags\r\n*1\r\n$3\r\noff\r\n$8\r\nredirect\r\n:-1\r\n$8\r\nprefixes\r\n*0\r\n"
        );

        response.clear();
        let getredir_frame = encode_resp_frame(&[b"CLIENT".to_vec(), b"GETREDIR".to_vec()]);
        let getredir_meta = parse_resp_command_arg_slices(&getredir_frame, &mut args).unwrap();
        handle_client_command(
            &processor,
            &metrics,
            client_id,
            &args[..getredir_meta.argument_count],
            &mut client_state,
            &mut response,
        );
        assert_eq!(response, b":-1\r\n");

        response.clear();
        let tracking_on = encode_resp_frame(&[
            b"CLIENT".to_vec(),
            b"TRACKING".to_vec(),
            b"ON".to_vec(),
            b"REDIRECT".to_vec(),
            b"42".to_vec(),
            b"NOLOOP".to_vec(),
        ]);
        let tracking_on_meta = parse_resp_command_arg_slices(&tracking_on, &mut args).unwrap();
        handle_client_command(
            &processor,
            &metrics,
            client_id,
            &args[..tracking_on_meta.argument_count],
            &mut client_state,
            &mut response,
        );
        assert_eq!(response, b"+OK\r\n");

        response.clear();
        let trackinginfo_meta =
            parse_resp_command_arg_slices(&trackinginfo_frame, &mut args).unwrap();
        handle_client_command(
            &processor,
            &metrics,
            client_id,
            &args[..trackinginfo_meta.argument_count],
            &mut client_state,
            &mut response,
        );
        assert_eq!(
            response,
            b"*6\r\n$5\r\nflags\r\n*2\r\n$2\r\non\r\n$6\r\nnoloop\r\n$8\r\nredirect\r\n:42\r\n$8\r\nprefixes\r\n*0\r\n"
        );

        response.clear();
        let getredir_meta = parse_resp_command_arg_slices(&getredir_frame, &mut args).unwrap();
        handle_client_command(
            &processor,
            &metrics,
            client_id,
            &args[..getredir_meta.argument_count],
            &mut client_state,
            &mut response,
        );
        assert_eq!(response, b":42\r\n");
    }

    #[test]
    fn client_tracking_and_caching_option_validation_matches_compat_contract() {
        let processor = RequestProcessor::new().expect("processor must initialize");
        let metrics = ServerMetrics::default();
        let client_id = metrics.register_client(None, None);
        let mut client_state = ClientConnectionState::default();
        let mut args = [ArgSlice::EMPTY; 16];
        let mut response = Vec::new();

        let tracking_bcast = encode_resp_frame(&[
            b"CLIENT".to_vec(),
            b"TRACKING".to_vec(),
            b"ON".to_vec(),
            b"BCAST".to_vec(),
            b"PREFIX".to_vec(),
            b"foo".to_vec(),
            b"PREFIX".to_vec(),
            b"bar".to_vec(),
        ]);
        let tracking_bcast_meta =
            parse_resp_command_arg_slices(&tracking_bcast, &mut args).unwrap();
        handle_client_command(
            &processor,
            &metrics,
            client_id,
            &args[..tracking_bcast_meta.argument_count],
            &mut client_state,
            &mut response,
        );
        assert_eq!(response, b"+OK\r\n");

        response.clear();
        let trackinginfo_frame = encode_resp_frame(&[b"CLIENT".to_vec(), b"TRACKINGINFO".to_vec()]);
        let trackinginfo_meta =
            parse_resp_command_arg_slices(&trackinginfo_frame, &mut args).unwrap();
        handle_client_command(
            &processor,
            &metrics,
            client_id,
            &args[..trackinginfo_meta.argument_count],
            &mut client_state,
            &mut response,
        );
        let trackinginfo_text = String::from_utf8_lossy(&response);
        assert!(
            trackinginfo_text.contains("bcast"),
            "expected bcast flag in TRACKINGINFO, got: {trackinginfo_text}"
        );
        assert!(
            trackinginfo_text.contains("foo") && trackinginfo_text.contains("bar"),
            "expected prefixes in TRACKINGINFO, got: {trackinginfo_text}"
        );

        response.clear();
        let tracking_optin = encode_resp_frame(&[
            b"CLIENT".to_vec(),
            b"TRACKING".to_vec(),
            b"ON".to_vec(),
            b"OPTIN".to_vec(),
        ]);
        let tracking_optin_meta =
            parse_resp_command_arg_slices(&tracking_optin, &mut args).unwrap();
        handle_client_command(
            &processor,
            &metrics,
            client_id,
            &args[..tracking_optin_meta.argument_count],
            &mut client_state,
            &mut response,
        );
        assert_eq!(response, b"+OK\r\n");

        response.clear();
        let caching_yes =
            encode_resp_frame(&[b"CLIENT".to_vec(), b"CACHING".to_vec(), b"YES".to_vec()]);
        let caching_yes_meta = parse_resp_command_arg_slices(&caching_yes, &mut args).unwrap();
        handle_client_command(
            &processor,
            &metrics,
            client_id,
            &args[..caching_yes_meta.argument_count],
            &mut client_state,
            &mut response,
        );
        assert_eq!(response, b"+OK\r\n");

        response.clear();
        let trackinginfo_meta =
            parse_resp_command_arg_slices(&trackinginfo_frame, &mut args).unwrap();
        handle_client_command(
            &processor,
            &metrics,
            client_id,
            &args[..trackinginfo_meta.argument_count],
            &mut client_state,
            &mut response,
        );
        let trackinginfo_text = String::from_utf8_lossy(&response);
        assert!(
            trackinginfo_text.contains("caching-yes"),
            "expected caching-yes in TRACKINGINFO, got: {trackinginfo_text}"
        );

        response.clear();
        let caching_on =
            encode_resp_frame(&[b"CLIENT".to_vec(), b"CACHING".to_vec(), b"ON".to_vec()]);
        let caching_on_meta = parse_resp_command_arg_slices(&caching_on, &mut args).unwrap();
        handle_client_command(
            &processor,
            &metrics,
            client_id,
            &args[..caching_on_meta.argument_count],
            &mut client_state,
            &mut response,
        );
        assert_eq!(response, b"-ERR syntax error\r\n");
    }

    #[test]
    fn encode_resp_frame_preserves_expected_resp2_shape() {
        let frame = encode_resp_frame(&[
            b"SET".to_vec(),
            b"key".to_vec(),
            b"value".to_vec(),
            b"".to_vec(),
        ]);
        assert_eq!(
            frame,
            b"*4\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$0\r\n\r\n"
        );
    }

    #[test]
    fn append_integer_frame_handles_i64_min() {
        let mut out = Vec::new();
        append_integer_frame(&mut out, i64::MIN);
        assert_eq!(out, b":-9223372036854775808\r\n");
    }
}
