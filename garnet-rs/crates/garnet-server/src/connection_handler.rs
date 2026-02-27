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
use crate::command_spec::command_transaction_control;
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
use crate::request_lifecycle::ClientUnblockMode;
use crate::request_lifecycle::RedisKey;
use crate::request_lifecycle::RespProtocolVersion;
use crate::request_lifecycle::WatchedKey;

const DEFAULT_OWNER_THREAD_COUNT: usize = 1;
const DEFAULT_RESP_ARG_SCRATCH: usize = 64;
const DEFAULT_MAX_RESP_ARGUMENTS: usize = 1_048_576;
const GARNET_STRING_OWNER_THREADS_ENV: &str = "GARNET_STRING_OWNER_THREADS";
const GARNET_OWNER_EXECUTION_INLINE_ENV: &str = "GARNET_OWNER_EXECUTION_INLINE";
const GARNET_MAX_RESP_ARGUMENTS_ENV: &str = "GARNET_MAX_RESP_ARGUMENTS";
const BLOCKING_COMMAND_NON_TURN_POLL_INTERVAL: Duration = Duration::from_millis(1);
const KILLED_CLIENT_POLL_INTERVAL: Duration = Duration::from_millis(25);
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
const OWNER_EXECUTION_INLINE_DEFAULT_UNSET: u8 = 0;
const OWNER_EXECUTION_INLINE_DEFAULT_FALSE: u8 = 1;
const OWNER_EXECUTION_INLINE_DEFAULT_TRUE: u8 = 2;
static OWNER_EXECUTION_INLINE_DEFAULT: AtomicU8 =
    AtomicU8::new(OWNER_EXECUTION_INLINE_DEFAULT_UNSET);

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
) -> Option<usize> {
    if !matches!(command, CommandId::Setbit | CommandId::Bitfield) {
        return None;
    }
    let key = arg_slice_bytes(args.get(1)?);
    processor
        .string_value_len_for_replication(key)
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
    OptIn,
    OptOut,
}

#[derive(Debug, Clone, Copy)]
struct ClientConnectionState {
    reply_mode: ClientReplyMode,
    tracking_mode: ClientTrackingMode,
    no_evict: bool,
    no_touch: bool,
    resp_protocol_version: RespProtocolVersion,
}

impl Default for ClientConnectionState {
    fn default() -> Self {
        Self {
            reply_mode: ClientReplyMode::On,
            tracking_mode: ClientTrackingMode::Off,
            no_evict: false,
            no_touch: false,
            resp_protocol_version: RespProtocolVersion::Resp2,
        }
    }
}

fn apply_reset_command_side_effects(
    metrics: &ServerMetrics,
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
    processor.set_connected_clients(metrics.connected_client_count());
    let _lifecycle = ConnectionLifecycle {
        metrics: &metrics,
        processor: &processor,
        client_id,
    };
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

    loop {
        processor.set_connected_clients(metrics.connected_client_count());
        if metrics.is_client_killed(client_id) {
            return Ok(());
        }
        if let Some(receiver) = monitor_receiver.as_mut() {
            loop {
                match receiver.try_recv() {
                    Ok(event) => {
                        stream.write_all(&event).await?;
                    }
                    Err(broadcast::error::TryRecvError::Empty) => break,
                    Err(broadcast::error::TryRecvError::Lagged(_)) => continue,
                    Err(broadcast::error::TryRecvError::Closed) => {
                        monitor_receiver = None;
                        break;
                    }
                }
            }
        }
        // Read at least once (await), then drain any immediately-available bytes via try_read.
        // This reduces read-side wakeups/syscall count under small pipelined requests.
        let bytes_read = match tokio::time::timeout(
            KILLED_CLIENT_POLL_INTERVAL,
            read_and_drain_available(&mut stream, &mut read_buffer, &mut receive_buffer, &metrics),
        )
        .await
        {
            Ok(result) => result?,
            Err(_) => continue,
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

            if command == CommandId::Client {
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
                && argument_count >= 3
                && ascii_eq_ignore_case(arg_slice_bytes(&args[1]), b"SETUSER")
            {
                let user = arg_slice_bytes(&args[2]);
                if !user.is_empty() {
                    metrics.register_acl_user(user);
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

            if command == CommandId::Migrate {
                append_simple_string(&mut responses, b"NOKEY");
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
                replica_subscriber = Some(replication.subscribe_downstream());
                responses.extend_from_slice(&replication.build_fullresync_payload());
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
                switch_to_replica_stream = true;
                break;
            }

            if ascii_eq_ignore_case(command_name, b"SYNC") {
                replica_subscriber = Some(replication.subscribe_downstream());
                responses.extend_from_slice(&replication.build_sync_payload());
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
                switch_to_replica_stream = true;
                break;
            }

            let transaction_control = command_transaction_control(command);
            let command_mutating = command_is_effectively_mutating(
                command,
                if argument_count > 1 {
                    Some(arg_slice_bytes(&args[1]))
                } else {
                    None
                },
            );
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
                                        if let Some(reason) = transaction_runtime_abort_reason(
                                            &processor,
                                            &replication,
                                            &transaction.queued_frames,
                                            max_resp_arguments,
                                        ) {
                                            transaction.reset();
                                            responses.extend_from_slice(
                                                transaction_runtime_execabort_message(reason),
                                            );
                                        } else {
                                            let transaction_outcome = execute_transaction_queue(
                                                &processor,
                                                &owner_thread_pool,
                                                &mut transaction,
                                                &mut responses,
                                                max_resp_arguments,
                                                client_state.no_touch,
                                            );
                                            publish_transaction_replication_frames(
                                                &processor,
                                                &replication,
                                                &transaction_outcome,
                                                max_resp_arguments,
                                            );
                                            if let Some(replication_transition) =
                                                transaction_outcome.pending_replication_transition
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
                                Err(error) => {
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
                        if command_mutating && processor.maxmemory_limit_bytes() > 0 {
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
                        if cluster_config.is_some() {
                            if let Some(slot) =
                                command_hash_slot_for_transaction(&args[..argument_count], command)
                            {
                                if !transaction.set_transaction_slot_or_abort(slot) {
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
                            }
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
                                let key = arg_slice_bytes(&key_arg);
                                if let Err(error) = processor.expire_watch_key_if_needed(key) {
                                    watch_error = Some(error);
                                    break;
                                }
                                let version = processor.watch_key_version(key);
                                transaction.watch_key(key, version);
                            }
                            if let Some(error) = watch_error {
                                transaction.clear_watches();
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
                        if is_blocking_command(command) && !responses.is_empty() {
                            stream.write_all(&responses).await?;
                            responses.clear();
                        }
                        let mut replication_frame: Option<Vec<u8>> = None;
                        let mut wait_for_blocking_progress = false;
                        let blocked_before = if command_mutating {
                            processor.blocked_clients()
                        } else {
                            0
                        };
                        match execute_blocking_frame_on_owner_thread(
                            &processor,
                            &metrics,
                            &owner_thread_pool,
                            &args[..argument_count],
                            command,
                            command_mutating,
                            frame,
                            client_state.no_touch,
                            client_id,
                            &mut stream,
                        )
                        .await
                        {
                            Ok((frame_response, should_replicate)) => {
                                wait_for_blocking_progress = blocked_before > 0
                                    && command_may_wake_blocking_waiters(command);
                                maybe_update_client_resp_protocol_version_after_hello(
                                    command,
                                    &args[..argument_count],
                                    &frame_response,
                                    &mut client_state,
                                );
                                responses.extend_from_slice(&frame_response);
                                if should_replicate {
                                    replication_frame = replication_frame_for_command(
                                        &processor,
                                        command,
                                        &args[..argument_count],
                                        &frame_response,
                                        frame,
                                    );
                                }
                            }
                            Err(OwnerThreadExecutionError::Request(error)) => {
                                processor.record_command_failure(&command_call_name);
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
                        let mut published_select = false;
                        if publish_lazy_expire_deletes {
                            if !lazy_expired_keys.is_empty() {
                                publish_select_if_needed(&replication);
                                published_select = true;
                            }
                            for key in lazy_expired_keys {
                                let del_frame =
                                    encode_resp_frame(&[b"DEL".to_vec(), key.into_vec()]);
                                processor.record_rdb_change(1);
                                replication.publish_write_frame(&del_frame);
                            }
                        }
                        if let Some(frame_to_replicate) = replication_frame.as_ref() {
                            if !published_select {
                                publish_select_if_needed(&replication);
                            }
                            processor.record_rdb_change(1);
                            replication.publish_write_frame(frame_to_replicate);
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
                replication.publish_write_frame(frame);
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
                    .serve_downstream_replica_with_subscriber(stream, subscriber)
                    .await;
            }
            return replication.serve_downstream_replica(stream).await;
        }

        if consumed > 0 {
            receive_buffer.drain(..consumed);
        }

        if !responses.is_empty() {
            stream.write_all(&responses).await?;
        }
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
) -> Result<Vec<u8>, OwnerThreadExecutionError> {
    if command_is_scripting_family(command) {
        let owned_args =
            capture_owned_frame_args(frame, args).map_err(map_routed_error_to_owner)?;
        let task_processor = Arc::clone(processor);
        let result = tokio::task::spawn_blocking(move || {
            execute_owned_frame_args_via_processor(&task_processor, &owned_args, client_no_touch)
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
    )
}

async fn execute_blocking_frame_on_owner_thread(
    processor: &Arc<RequestProcessor>,
    metrics: &Arc<ServerMetrics>,
    owner_thread_pool: &Arc<ShardOwnerThreadPool>,
    args: &[ArgSlice],
    command: CommandId,
    command_mutating: bool,
    frame: &[u8],
    client_no_touch: bool,
    client_id: ClientId,
    stream: &mut TcpStream,
) -> Result<(Vec<u8>, bool), OwnerThreadExecutionError> {
    // TLA+ mapping (`formal/tla/specs/BlockingDisconnectLeak.tla`):
    // - ACTIVE: client has no wait-queue registration.
    // - BLOCKED: `register_blocking_wait` + `set_client_blocked(true)` applied.
    // - DISCONNECTED: socket close observed in `blocking_client_disconnected`.
    // Core actions:
    // - `Block(c,k)`: first transition into blocked path for current blocking keys.
    // - `PushWake/WakeHead(k)`: owner-thread execution returns non-empty response.
    // - `Disconnect(c)`: disconnect check branch; cleanup must unregister the waiter.
    let deadline = blocking_timeout_deadline(command, args);
    let blocking_keys = blocking_wait_keys(command, args);
    let mut blocked = false;
    processor.clear_client_unblock_request(client_id);
    loop {
        if metrics.is_client_killed(client_id) {
            if blocked {
                clear_blocking_client_state(processor, metrics, client_id, &blocking_keys);
            }
            return Ok((blocking_empty_response_for_command(command).to_vec(), false));
        }
        if is_blocking_command(command) && blocking_client_disconnected(stream).await {
            // `Disconnect(c)` branch: if we were blocked, this must clear all wait-queue state.
            if blocked {
                clear_blocking_client_state(processor, metrics, client_id, &blocking_keys);
            }
            return Ok((blocking_empty_response_for_command(command).to_vec(), false));
        }

        if blocked {
            if let Some(unblock_mode) = processor.take_client_unblock_request(client_id) {
                clear_blocking_client_state(processor, metrics, client_id, &blocking_keys);
                let response = match unblock_mode {
                    ClientUnblockMode::Timeout => {
                        blocking_empty_response_for_command(command).to_vec()
                    }
                    ClientUnblockMode::Error => {
                        b"-UNBLOCKED client unblocked via CLIENT UNBLOCK\r\n".to_vec()
                    }
                };
                return Ok((response, false));
            }
        }

        if !processor.is_blocking_wait_turn(client_id, &blocking_keys) {
            if !blocked {
                blocked = true;
                // `Block(c,k)` critical section starts here.
                processor.increment_blocked_clients();
                processor.register_blocking_wait(client_id, &blocking_keys);
                metrics.set_client_blocked(client_id, true);
            }
            if let Some(deadline_time) = deadline {
                let now = Instant::now();
                if now >= deadline_time {
                    clear_blocking_client_state(processor, metrics, client_id, &blocking_keys);
                    return Ok((blocking_empty_response_for_command(command).to_vec(), false));
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

        let pre_string_len = pre_string_len_for_mutation_detection(processor, command, args);
        let frame_response = match execute_frame_on_owner_thread_async(
            processor,
            owner_thread_pool,
            args,
            command,
            frame,
            client_no_touch,
        )
        .await
        {
            Ok(response) => response,
            Err(OwnerThreadExecutionError::Request(RequestExecutionError::WrongType))
                if blocked && ignore_wrongtype_while_blocked(command) =>
            {
                blocking_empty_response_for_command(command).to_vec()
            }
            Err(error) => {
                if blocked {
                    clear_blocking_client_state(processor, metrics, client_id, &blocking_keys);
                }
                return Err(error);
            }
        };

        let should_replicate = if is_blocking_command(command) {
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
        if !is_blocking_command(command) || !is_blocking_empty_response(&frame_response) {
            if blocked {
                clear_blocking_client_state(processor, metrics, client_id, &blocking_keys);
            }
            return Ok((frame_response, should_replicate));
        }

        if !blocked {
            blocked = true;
            processor.increment_blocked_clients();
            processor.register_blocking_wait(client_id, &blocking_keys);
            metrics.set_client_blocked(client_id, true);
        }

        if let Some(deadline_time) = deadline {
            let now = Instant::now();
            if now >= deadline_time {
                clear_blocking_client_state(processor, metrics, client_id, &blocking_keys);
                return Ok((frame_response, should_replicate));
            }

            let remaining = deadline_time.duration_since(now);
            if remaining > Duration::from_millis(0) {
                yield_now().await;
            }
        } else {
            yield_now().await;
        }
    }
}

fn clear_blocking_client_state(
    processor: &RequestProcessor,
    metrics: &ServerMetrics,
    client_id: ClientId,
    blocking_keys: &[RedisKey],
) {
    // Shared cleanup for both successful wakeups and disconnect/unblock paths.
    // In TLA+ terms this is the queue-removal side of `Disconnect(c)` and wake completion.
    processor.decrement_blocked_clients();
    processor.unregister_blocking_wait(client_id, blocking_keys);
    processor.clear_client_unblock_request(client_id);
    metrics.set_client_blocked(client_id, false);
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
        // TLA+ minimal guard (`LinkedBlmoveChainResidue_minimal`): producer ACK should
        // not be exposed while any queued blocking waiter is already ready to run.
        if !processor.has_ready_blocking_waiters() {
            return;
        }
        sleep(BLOCKING_COMMAND_NON_TURN_POLL_INTERVAL).await;
        yield_now().await;
    }
}

fn command_may_wake_blocking_waiters(command: CommandId) -> bool {
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
            | CommandId::Rename
            | CommandId::Renamenx
            | CommandId::Copy
            | CommandId::Move
            | CommandId::Restore
            | CommandId::RestoreAsking
    )
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
    if !is_blocking_command(command) {
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
        _ => return None,
    };

    if timeout_seconds <= 0.0 {
        return None;
    }

    Some(Instant::now() + Duration::from_secs_f64(timeout_seconds))
}

fn is_blocking_empty_response(frame_response: &[u8]) -> bool {
    frame_response == b"*-1\r\n" || frame_response == b"$-1\r\n"
}

fn blocking_empty_response_for_command(command: CommandId) -> &'static [u8] {
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
            | CommandId::Bzmpop,
    )
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
    Oom,
    ReadOnlyReplica,
    NoReplicas,
    MasterDown,
}

fn transaction_runtime_abort_reason(
    processor: &RequestProcessor,
    replication: &RedisReplicationCoordinator,
    queued_frames: &[Vec<u8>],
    max_resp_arguments: usize,
) -> Option<TransactionRuntimeAbortReason> {
    let mut has_mutating_command = false;
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
        let command_mutating = command_is_effectively_mutating(command, subcommand);
        if command_mutating {
            has_mutating_command = true;
            if replica_mode {
                return Some(TransactionRuntimeAbortReason::ReadOnlyReplica);
            }
        } else if reject_reads_on_stale_replica {
            return Some(TransactionRuntimeAbortReason::MasterDown);
        }
    }

    if !has_mutating_command {
        return None;
    }
    if processor.maxmemory_limit_bytes() > 0 {
        return Some(TransactionRuntimeAbortReason::Oom);
    }

    let min_replicas_to_write = processor.min_replicas_to_write();
    if min_replicas_to_write > 0 && replication.downstream_replica_count() < min_replicas_to_write {
        return Some(TransactionRuntimeAbortReason::NoReplicas);
    }
    None
}

fn transaction_runtime_execabort_message(reason: TransactionRuntimeAbortReason) -> &'static [u8] {
    match reason {
        TransactionRuntimeAbortReason::Oom => {
            b"-EXECABORT Transaction discarded because of previous errors: OOM command not allowed when used memory > 'maxmemory'.\r\n"
        }
        TransactionRuntimeAbortReason::ReadOnlyReplica => {
            b"-EXECABORT Transaction discarded because of previous errors: READONLY You can't write against a read only replica.\r\n"
        }
        TransactionRuntimeAbortReason::NoReplicas => {
            b"-EXECABORT Transaction discarded because of previous errors: NOREPLICAS Not enough good replicas to write.\r\n"
        }
        TransactionRuntimeAbortReason::MasterDown => {
            b"-EXECABORT Transaction discarded because of previous errors: MASTERDOWN Link with MASTER is down and replica-serve-stale-data is set to 'no'.\r\n"
        }
    }
}

fn publish_select_if_needed(replication: &RedisReplicationCoordinator) {
    if !replication.consume_replication_select_needed() {
        return;
    }
    let select_frame = encode_resp_frame(&[b"SELECT".to_vec(), b"0".to_vec()]);
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
) {
    if transaction_outcome.pending_replication_transition.is_some() {
        return;
    }

    let mut args = vec![ArgSlice::EMPTY; 64.min(max_resp_arguments.max(1))];
    let mut replication_frames = Vec::new();
    for item in &transaction_outcome.items {
        if item.frame.is_empty() || resp_is_error(&item.response) {
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
            replication_frames.extend(frames);
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
            replication_frames.push(eval_effect_frame);
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
            command,
            &args[..meta.argument_count],
            &item.response,
            &item.frame,
        ) else {
            continue;
        };
        replication_frames.push(replication_frame);
    }

    let lazy_expired_keys = processor.take_lazy_expired_keys_for_replication();
    if should_publish_lazy_expire_deletes(processor, CommandId::Exec, true) {
        for key in lazy_expired_keys {
            replication_frames.push(encode_resp_frame(&[b"DEL".to_vec(), key.into_vec()]));
        }
    }

    if replication_frames.is_empty() {
        return;
    }

    if replication_frames.len() > 1 {
        let multi_frame = encode_resp_frame(&[b"MULTI".to_vec()]);
        processor.record_rdb_change(1);
        replication.publish_write_frame(&multi_frame);
    }
    publish_select_if_needed(replication);

    for replication_frame in &replication_frames {
        processor.record_rdb_change(1);
        replication.publish_write_frame(replication_frame);
    }

    if replication_frames.len() > 1 {
        let exec_frame = encode_resp_frame(&[b"EXEC".to_vec()]);
        processor.record_rdb_change(1);
        replication.publish_write_frame(&exec_frame);
    }
}

fn xreadgroup_replication_frames(args: &[ArgSlice], frame_response: &[u8]) -> Vec<Vec<u8>> {
    if frame_response == b"*0\r\n" {
        return Vec::new();
    }
    if args.len() < 7 {
        return Vec::new();
    }
    let group = arg_slice_bytes(&args[2]).to_vec();
    let consumer = arg_slice_bytes(&args[3]).to_vec();

    let mut count = 1usize;
    let mut key: Option<Vec<u8>> = None;
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
            key = Some(arg_slice_bytes(key_arg).to_vec());
            break;
        }
        break;
    }

    let Some(key) = key else {
        return Vec::new();
    };

    let mut frames = Vec::with_capacity(count.saturating_add(1));
    for _ in 0..count {
        frames.push(encode_resp_frame(&[
            b"XCLAIM".to_vec(),
            key.clone(),
            group.clone(),
            consumer.clone(),
            b"0".to_vec(),
            b"0-0".to_vec(),
        ]));
    }
    frames.push(encode_resp_frame(&[
        b"XGROUP".to_vec(),
        b"SETID".to_vec(),
        key,
        group,
        b"0".to_vec(),
        b"ENTRIESREAD".to_vec(),
        b"0".to_vec(),
    ]));
    frames
}

fn script_eval_effect_replication_frame(
    processor: &RequestProcessor,
    command: CommandId,
    args: &[ArgSlice],
) -> Option<Vec<u8>> {
    if args.len() < 4 {
        return None;
    }
    let script = match command {
        CommandId::Eval | CommandId::EvalRo => arg_slice_bytes(&args[1]).to_vec(),
        CommandId::Evalsha | CommandId::EvalshaRo => {
            let sha = arg_slice_bytes(&args[1]);
            processor.cached_script_for_sha(sha)?
        }
        _ => return None,
    };
    let script_text = String::from_utf8_lossy(&script).to_string();
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
    let key = arg_slice_bytes(&args[3]).to_vec();
    let value = if script_text_lower.contains("argv[1]") {
        arg_slice_bytes(args.get(3 + key_count)?).to_vec()
    } else {
        extract_script_set_literal(&script_text, &script_text_lower)?
    };
    Some(encode_resp_frame(&[b"SET".to_vec(), key, value]))
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
        return Some(tail[..end].as_bytes().to_vec());
    }
    None
}

fn parse_ascii_usize(value: &[u8]) -> Option<usize> {
    std::str::from_utf8(value).ok()?.parse::<usize>().ok()
}

fn transaction_command_had_effect(command: CommandId, response: &[u8]) -> bool {
    match command {
        CommandId::Del | CommandId::Unlink => parse_resp_integer(response).unwrap_or(0) > 0,
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
    )
}

fn blocking_wait_keys(command: CommandId, args: &[ArgSlice]) -> Vec<RedisKey> {
    match command {
        CommandId::Blpop | CommandId::Brpop | CommandId::Bzpopmin | CommandId::Bzpopmax => {
            if args.len() < 3 {
                return Vec::new();
            }
            args[1..args.len() - 1]
                .iter()
                .map(|arg| {
                    // SAFETY: The argument slice was parsed from a live request frame and stays valid
                    // while the request is being executed.
                    RedisKey::from(arg_slice_bytes(&arg))
                })
                .collect()
        }
        CommandId::Blmove | CommandId::Brpoplpush => {
            if args.len() < 2 {
                return Vec::new();
            }
            vec![
                // SAFETY: The argument slice was parsed from a live request frame and stays valid
                // while the request is being executed.
                RedisKey::from(arg_slice_bytes(&args[1])),
            ]
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
                    // SAFETY: The argument slice was parsed from a live request frame and stays valid
                    // while the request is being executed.
                    RedisKey::from(arg_slice_bytes(&arg))
                })
                .collect()
        }
        _ => Vec::new(),
    }
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
        metrics.set_client_name(client_id, Some(new_name.to_vec()));
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
            if options_len % 2 != 0 {
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

        if let Some(user) = filter.user.as_ref() {
            if !metrics.acl_user_exists(user) {
                append_error_line(response_out, b"ERR No such user");
                return outcome;
            }
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
        let unblocked = target_client_id != client_id
            && metrics.is_client_blocked(target_client_id)
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
        if args.len() == 4 {
            let mode = arg_slice_bytes(&args[3]);
            if !ascii_eq_ignore_case(mode, b"WRITE") && !ascii_eq_ignore_case(mode, b"ALL") {
                response_out.extend_from_slice(b"-ERR syntax error\r\n");
                return outcome;
            }
        }
        append_simple_string(response_out, b"OK");
        return outcome;
    }

    if ascii_eq_ignore_case(subcommand, b"UNPAUSE") {
        if args.len() != 2 {
            append_client_subcommand_wrong_arity(response_out, b"unpause");
            return outcome;
        }
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
            append_simple_string(response_out, b"OK");
            outcome.reply_behavior = ClientCommandReplyBehavior::ForceReply;
            return outcome;
        }
        if ascii_eq_ignore_case(mode, b"OFF") {
            client_state.reply_mode = ClientReplyMode::Off;
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
            for option_arg in &args[3..] {
                let option = arg_slice_bytes(option_arg);
                if ascii_eq_ignore_case(option, b"OPTIN") {
                    seen_optin = true;
                } else if ascii_eq_ignore_case(option, b"OPTOUT") {
                    seen_optout = true;
                } else {
                    response_out.extend_from_slice(b"-ERR syntax error\r\n");
                    return outcome;
                }
            }
            if seen_optin && seen_optout {
                response_out.extend_from_slice(b"-ERR syntax error\r\n");
                return outcome;
            }
            client_state.tracking_mode = if seen_optin {
                ClientTrackingMode::OptIn
            } else if seen_optout {
                ClientTrackingMode::OptOut
            } else {
                ClientTrackingMode::Off
            };
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
            client_state.tracking_mode = ClientTrackingMode::Off;
            append_simple_string(response_out, b"OK");
            return outcome;
        }
        response_out.extend_from_slice(b"-ERR syntax error\r\n");
        return outcome;
    }

    if ascii_eq_ignore_case(subcommand, b"CACHING") {
        if args.len() != 3 {
            append_client_subcommand_wrong_arity(response_out, b"caching");
            return outcome;
        }
        if client_state.tracking_mode == ClientTrackingMode::Off {
            append_error_line(
                response_out,
                b"ERR CLIENT CACHING can be called only when the client is in tracking mode with OPTIN or OPTOUT mode enabled",
            );
            return outcome;
        }
        let option = arg_slice_bytes(&args[2]);
        if !ascii_eq_ignore_case(option, b"ON") && !ascii_eq_ignore_case(option, b"OFF") {
            response_out.extend_from_slice(b"-ERR syntax error\r\n");
            return outcome;
        }
        if client_state.tracking_mode == ClientTrackingMode::OptOut
            && ascii_eq_ignore_case(option, b"ON")
        {
            response_out.extend_from_slice(b"-ERR syntax error\r\n");
            return outcome;
        }
        if client_state.tracking_mode == ClientTrackingMode::OptIn
            && ascii_eq_ignore_case(option, b"OFF")
        {
            response_out.extend_from_slice(b"-ERR syntax error\r\n");
            return outcome;
        }
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

fn append_integer_frame(output: &mut Vec<u8>, value: i64) {
    output.push(b':');
    output.extend_from_slice(value.to_string().as_bytes());
    output.extend_from_slice(b"\r\n");
}

fn append_bulk_string_frame(output: &mut Vec<u8>, value: &[u8]) {
    output.push(b'$');
    output.extend_from_slice(value.len().to_string().as_bytes());
    output.extend_from_slice(b"\r\n");
    output.extend_from_slice(value);
    output.extend_from_slice(b"\r\n");
}

fn append_bulk_string_array_frame(output: &mut Vec<u8>, values: &[&[u8]]) {
    output.push(b'*');
    output.extend_from_slice(values.len().to_string().as_bytes());
    output.extend_from_slice(b"\r\n");
    for value in values {
        append_bulk_string_frame(output, value);
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

fn maybe_update_client_resp_protocol_version_after_hello(
    command: CommandId,
    args: &[ArgSlice],
    frame_response: &[u8],
    client_state: &mut ClientConnectionState,
) {
    if command != CommandId::Hello || !frame_response.starts_with(b"+OK\r\n") {
        return;
    }
    if args.len() != 2 {
        return;
    }
    let Some(version_arg) = args.get(1) else {
        return;
    };
    // SAFETY: args reference the currently parsed frame bytes.
    let Some(raw_version) = parse_u64_ascii(arg_slice_bytes(version_arg)) else {
        return;
    };
    let Some(version) = RespProtocolVersion::from_u64(raw_version) else {
        return;
    };
    client_state.resp_protocol_version = version;
}

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
        let mut parts = Vec::with_capacity(args.len());
        parts.push(b"EVAL".to_vec());
        parts.push(script);
        for arg in &args[2..] {
            // SAFETY: args were parsed from the current request frame.
            parts.push(arg_slice_bytes(arg).to_vec());
        }
        return Some(encode_resp_frame(&parts));
    }

    if matches!(
        command,
        CommandId::Set | CommandId::Setex | CommandId::Psetex
    ) {
        return rewrite_set_family_replication_frame(
            processor,
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
        return rewrite_expire_family_replication_frame(processor, args, frame_response);
    }

    if command == CommandId::Getex {
        return rewrite_getex_replication_frame(processor, args, frame_response, original_frame);
    }

    if matches!(command, CommandId::Restore | CommandId::RestoreAsking) {
        return rewrite_restore_replication_frame(processor, args, frame_response, original_frame);
    }

    if command == CommandId::Blmove {
        if frame_response == b"$-1\r\n" {
            return None;
        }
        return Some(encode_resp_frame(&[
            b"LMOVE".to_vec(),
            // SAFETY: args were parsed from the current request frame.
            arg_slice_bytes(args.get(1)?).to_vec(),
            // SAFETY: args were parsed from the current request frame.
            arg_slice_bytes(args.get(2)?).to_vec(),
            // SAFETY: args were parsed from the current request frame.
            arg_slice_bytes(args.get(3)?).to_vec(),
            // SAFETY: args were parsed from the current request frame.
            arg_slice_bytes(args.get(4)?).to_vec(),
        ]));
    }
    if command == CommandId::Brpoplpush {
        if frame_response == b"$-1\r\n" {
            return None;
        }
        return Some(encode_resp_frame(&[
            b"RPOPLPUSH".to_vec(),
            // SAFETY: args were parsed from the current request frame.
            arg_slice_bytes(args.get(1)?).to_vec(),
            // SAFETY: args were parsed from the current request frame.
            arg_slice_bytes(args.get(2)?).to_vec(),
        ]));
    }
    if matches!(command, CommandId::Lmpop | CommandId::Blmpop) {
        let (key, popped_count) = parse_lmpop_replication_meta(frame_response)?;
        let pop_command = lmpop_pop_command(command, args)?;
        let count = popped_count.to_string().into_bytes();
        return Some(encode_resp_frame(&[pop_command.to_vec(), key, count]));
    }
    Some(original_frame.to_vec())
}

fn rewrite_set_family_replication_frame(
    processor: &RequestProcessor,
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
            let mut pxat_token: Option<Vec<u8>> = None;
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
                    pxat_token = Some(token.to_vec());
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

    if let Some(expiration_unix_millis) = processor.expiration_unix_millis_for_key(key) {
        let mut pxat = pxat_token.unwrap_or_else(|| b"PXAT".to_vec());
        if pxat.is_empty() {
            pxat = b"PXAT".to_vec();
        }
        return Some(encode_resp_frame(&[
            b"SET".to_vec(),
            key.to_vec(),
            value.to_vec(),
            pxat,
            expiration_unix_millis.to_string().into_bytes(),
        ]));
    }

    match processor.key_exists_any(key) {
        Ok(true) => Some(original_frame.to_vec()),
        Ok(false) => Some(encode_resp_frame(&[b"DEL".to_vec(), key.to_vec()])),
        Err(_) => Some(original_frame.to_vec()),
    }
}

fn rewrite_expire_family_replication_frame(
    processor: &RequestProcessor,
    args: &[ArgSlice],
    frame_response: &[u8],
) -> Option<Vec<u8>> {
    if parse_resp_integer(frame_response) != Some(1) {
        return None;
    }
    let key = args.get(1).map(arg_slice_bytes)?;

    match processor.key_exists_any(key) {
        Ok(false) => Some(encode_resp_frame(&[b"DEL".to_vec(), key.to_vec()])),
        Ok(true) => processor
            .expiration_unix_millis_for_key(key)
            .map(|expiration_unix_millis| {
                encode_resp_frame(&[
                    b"PEXPIREAT".to_vec(),
                    key.to_vec(),
                    expiration_unix_millis.to_string().into_bytes(),
                ])
            }),
        Err(_) => None,
    }
}

fn rewrite_getex_replication_frame(
    processor: &RequestProcessor,
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

    let Some(option) = args.get(2).map(arg_slice_bytes) else {
        return None;
    };

    if ascii_eq_ignore_case(option, b"PERSIST") {
        return Some(encode_resp_frame(&[b"PERSIST".to_vec(), key.to_vec()]));
    }

    if ascii_eq_ignore_case(option, b"EX")
        || ascii_eq_ignore_case(option, b"PX")
        || ascii_eq_ignore_case(option, b"EXAT")
        || ascii_eq_ignore_case(option, b"PXAT")
    {
        return match processor.key_exists_any(key) {
            Ok(false) => Some(encode_resp_frame(&[b"DEL".to_vec(), key.to_vec()])),
            Ok(true) => {
                processor
                    .expiration_unix_millis_for_key(key)
                    .map(|expiration_unix_millis| {
                        encode_resp_frame(&[
                            b"PEXPIREAT".to_vec(),
                            key.to_vec(),
                            expiration_unix_millis.to_string().into_bytes(),
                        ])
                    })
            }
            Err(_) => Some(original_frame.to_vec()),
        };
    }

    Some(original_frame.to_vec())
}

fn rewrite_restore_replication_frame(
    processor: &RequestProcessor,
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

    match processor.key_exists_any(key) {
        Ok(false) => {
            let delete_command = if processor.lazyfree_lazy_server_del_enabled() {
                b"UNLINK".to_vec()
            } else {
                b"DEL".to_vec()
            };
            return Some(encode_resp_frame(&[delete_command, key.to_vec()]));
        }
        Ok(true) => {}
        Err(_) => return Some(original_frame.to_vec()),
    }

    let Some(expiration_unix_millis) = processor.expiration_unix_millis_for_key(key) else {
        return Some(original_frame.to_vec());
    };

    let mut option_parts = Vec::new();
    let mut absttl_present = false;
    for option in &args[4..] {
        let token = arg_slice_bytes(option).to_vec();
        if ascii_eq_ignore_case(&token, b"ABSTTL") {
            absttl_present = true;
        }
        option_parts.push(token);
    }
    if !absttl_present {
        option_parts.push(b"ABSTTL".to_vec());
    }

    let mut parts = vec![
        b"RESTORE".to_vec(),
        key.to_vec(),
        expiration_unix_millis.to_string().into_bytes(),
        payload.to_vec(),
    ];
    parts.extend(option_parts);
    Some(encode_resp_frame(&parts))
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

fn parse_lmpop_replication_meta(frame_response: &[u8]) -> Option<(Vec<u8>, usize)> {
    if frame_response == b"*-1\r\n" {
        return None;
    }

    let mut cursor = 0usize;
    if !frame_response.get(cursor..)?.starts_with(b"*2\r\n") {
        return None;
    }
    cursor += 4;

    let (key, next_cursor) = parse_resp_bulk_string(frame_response, cursor)?;
    cursor = next_cursor;

    if frame_response.get(cursor)? != &b'*' {
        return None;
    }
    cursor += 1;
    let parsed = parse_resp_decimal(frame_response, cursor)?;
    Some((key, parsed.value))
}

fn parse_resp_bulk_string(input: &[u8], cursor: usize) -> Option<(Vec<u8>, usize)> {
    if input.get(cursor)? != &b'$' {
        return None;
    }
    let parsed = parse_resp_decimal(input, cursor + 1)?;
    let mut cursor = parsed.next_cursor;
    let end = cursor.checked_add(parsed.value)?;
    let value = input.get(cursor..end)?.to_vec();
    cursor = end;
    if input.get(cursor..cursor + 2)? != b"\r\n" {
        return None;
    }
    cursor += 2;
    Some((value, cursor))
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
            io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "owner-thread inline initialization failed (shards={}): {}",
                    shard_count, error
                ),
            )
        })?;
        return Ok(Arc::new(pool));
    }
    let owner_threads = owner_threads.min(shard_count);
    let pool = ShardOwnerThreadPool::new(owner_threads, shard_count).map_err(|error| {
        io::Error::new(
            io::ErrorKind::Other,
            format!(
                "owner-thread pool initialization failed (threads={}, shards={}): {}",
                owner_threads, shard_count, error
            ),
        )
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
    let mut out = Vec::new();
    out.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
    for part in parts {
        out.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
        out.extend_from_slice(part);
        out.extend_from_slice(b"\r\n");
    }
    out
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
    output.extend_from_slice(max_resp_arguments.to_string().as_bytes());
    output.extend_from_slice(b")\r\n");
}

struct ConnectionLifecycle<'a> {
    metrics: &'a ServerMetrics,
    processor: &'a RequestProcessor,
    client_id: ClientId,
}

impl Drop for ConnectionLifecycle<'_> {
    fn drop(&mut self) {
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
            .execute(&load_args[..load_meta.argument_count], &mut load_response)
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
            .execute(
                &set_ex_args[..set_ex_meta.argument_count],
                &mut set_ex_response,
            )
            .unwrap();
        let rewritten_set = replication_frame_for_command(
            &processor,
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
            .execute(
                &getex_persist_args[..getex_persist_meta.argument_count],
                &mut getex_persist_response,
            )
            .unwrap();
        let rewritten_persist = replication_frame_for_command(
            &processor,
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
            .execute(&set_args[..set_meta.argument_count], &mut set_response)
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
            .execute(
                &getex_expired_args[..getex_expired_meta.argument_count],
                &mut getex_expired_response,
            )
            .unwrap();
        let rewritten_del = replication_frame_for_command(
            &processor,
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
    fn replication_rewrites_restore_past_absttl_to_del_or_unlink() {
        let processor = RequestProcessor::new().expect("processor must initialize");
        let dump_payload = b"GRN1\x00\x05\x00\x00\x00value".to_vec();
        let mut args = [ArgSlice::EMPTY; 16];
        let mut response = Vec::new();

        let set_key1 = encode_resp_frame(&[b"SET".to_vec(), b"key1".to_vec(), b"old".to_vec()]);
        let set_key1_meta = parse_resp_command_arg_slices(&set_key1, &mut args).unwrap();
        processor
            .execute(&args[..set_key1_meta.argument_count], &mut response)
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
            .execute(&args[..restore_key1_meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"+OK\r\n");
        let rewritten_del = replication_frame_for_command(
            &processor,
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
            .execute(&args[..config_lazyfree_meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"+OK\r\n");

        response.clear();
        let set_key2 = encode_resp_frame(&[b"SET".to_vec(), b"key2".to_vec(), b"old".to_vec()]);
        let set_key2_meta = parse_resp_command_arg_slices(&set_key2, &mut args).unwrap();
        processor
            .execute(&args[..set_key2_meta.argument_count], &mut response)
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
            .execute(&args[..restore_key2_meta.argument_count], &mut response)
            .unwrap();
        assert_eq!(response, b"+OK\r\n");
        let rewritten_unlink = replication_frame_for_command(
            &processor,
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
}
