use std::io;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use garnet_cluster::ClusterConfigStore;
use garnet_common::{parse_resp_command_arg_slices_dynamic, ArgSlice, RespParseError};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::task::yield_now;
use tokio::time::sleep;

use crate::command_dispatch::dispatch_from_arg_slices;
use crate::command_spec::{
    command_has_valid_arity, command_is_mutating, command_transaction_control, CommandId,
    TransactionControlCommand,
};
use crate::connection_owner_routing::{execute_frame_on_owner_thread, OwnerThreadExecutionError};
use crate::connection_protocol::{
    append_error_line, append_simple_string, append_wrong_arity_error_for_command,
    ascii_eq_ignore_case, parse_u16_ascii,
};
use crate::connection_routing::{cluster_error_for_command, command_hash_slot_for_transaction};
use crate::connection_transaction::{execute_transaction_queue, ConnectionTransactionState};
use crate::redis_replication::RedisReplicationCoordinator;
use crate::request_lifecycle::ClientUnblockMode;
use crate::{
    RequestExecutionError, RequestProcessor, ServerMetrics, ShardOwnerThreadPool,
};

const DEFAULT_OWNER_THREAD_COUNT: usize = 1;
const DEFAULT_RESP_ARG_SCRATCH: usize = 64;
const DEFAULT_MAX_RESP_ARGUMENTS: usize = 1_048_576;
const GARNET_STRING_OWNER_THREADS_ENV: &str = "GARNET_STRING_OWNER_THREADS";
const GARNET_MAX_RESP_ARGUMENTS_ENV: &str = "GARNET_MAX_RESP_ARGUMENTS";
const BLOCKING_COMMAND_NON_TURN_POLL_INTERVAL: Duration = Duration::from_millis(1);

pub(crate) async fn handle_connection(
    mut stream: TcpStream,
    read_buffer_size: usize,
    metrics: Arc<ServerMetrics>,
    processor: Arc<RequestProcessor>,
    cluster_config: Option<Arc<ClusterConfigStore>>,
    owner_thread_pool: Arc<ShardOwnerThreadPool>,
    replication: Arc<RedisReplicationCoordinator>,
) -> io::Result<()> {
    let client_id = metrics.register_client();
    let _lifecycle = ConnectionLifecycle {
        metrics: &metrics,
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

    loop {
        let bytes_read = stream.read(&mut read_buffer).await?;
        if bytes_read == 0 {
            return Ok(());
        }
        metrics
            .bytes_received
            .fetch_add(bytes_read as u64, Ordering::Relaxed);

        receive_buffer.extend_from_slice(&read_buffer[..bytes_read]);
        let mut consumed = 0usize;
        responses.clear();
        let mut switch_to_replica_stream = false;

        loop {
            let mut inline_frame = Vec::new();
            let (argument_count, frame_bytes_consumed) = match parse_resp_command_arg_slices_dynamic(
                &receive_buffer[consumed..],
                &mut args,
                max_resp_arguments,
            ) {
                Ok(meta) => (meta.argument_count, meta.bytes_consumed),
                Err(RespParseError::Incomplete) => break,
                Err(RespParseError::InvalidArrayPrefix { .. }) => {
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
                                Err(_) => {
                                    responses.extend_from_slice(b"-ERR protocol error\r\n");
                                    stream.write_all(&responses).await?;
                                    return Ok(());
                                }
                            }
                        }
                        InlineFrameParse::Incomplete => break,
                        InlineFrameParse::ProtocolError => {
                            responses.extend_from_slice(b"-ERR protocol error\r\n");
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
                Err(_) => {
                    responses.extend_from_slice(b"-ERR protocol error\r\n");
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
            if argument_count == 0 {
                responses.extend_from_slice(b"-ERR unknown command\r\n");
                consumed += frame_bytes_consumed;
                continue;
            }
            // SAFETY: `args` points to the current frame bytes.
            let command_name = unsafe { args[0].as_slice() };
            metrics.set_client_last_command(client_id, command_name);

            if command == CommandId::Client {
                handle_client_command(
                    &processor,
                    &metrics,
                    client_id,
                    &args[..argument_count],
                    &mut responses,
                );
                consumed += frame_bytes_consumed;
                continue;
            }

            if ascii_eq_ignore_case(command_name, b"REPLICAOF")
                || ascii_eq_ignore_case(command_name, b"SLAVEOF")
            {
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
                    let arg1 = unsafe { args[1].as_slice() };
                    // SAFETY: `args` points to the current request frame.
                    let arg2 = unsafe { args[2].as_slice() };

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
                consumed += frame_bytes_consumed;
                continue;
            }

            if ascii_eq_ignore_case(command_name, b"REPLCONF") {
                append_simple_string(&mut responses, b"OK");
                consumed += frame_bytes_consumed;
                continue;
            }

            if ascii_eq_ignore_case(command_name, b"PSYNC") {
                responses.extend_from_slice(&replication.build_fullresync_payload());
                consumed += frame_bytes_consumed;
                switch_to_replica_stream = true;
                break;
            }

            if ascii_eq_ignore_case(command_name, b"SYNC") {
                responses.extend_from_slice(&replication.build_sync_payload());
                consumed += frame_bytes_consumed;
                switch_to_replica_stream = true;
                break;
            }

            let transaction_control = command_transaction_control(command);
            if transaction_control == TransactionControlCommand::Asking {
                if !command_has_valid_arity(command, argument_count) {
                    append_wrong_arity_error_for_command(&mut responses, command);
                } else {
                    allow_asking_once = true;
                    append_simple_string(&mut responses, b"OK");
                }
                consumed += frame_bytes_consumed;
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
                    consumed += frame_bytes_consumed;
                    continue;
                }
            }
            let propagate_frame = false;
            if transaction.in_multi {
                match transaction_control {
                    TransactionControlCommand::Exec => {
                        if !command_has_valid_arity(command, argument_count) {
                            append_wrong_arity_error_for_command(&mut responses, command);
                        } else if !processor.watch_versions_match(&transaction.watched_keys) {
                            transaction.reset();
                            responses.extend_from_slice(b"*-1\r\n");
                        } else if transaction.aborted {
                            transaction.reset();
                            responses.extend_from_slice(
                                b"-EXECABORT Transaction discarded because of previous errors.\r\n",
                            );
                        } else {
                            execute_transaction_queue(
                                &processor,
                                &owner_thread_pool,
                                &mut transaction,
                                &mut responses,
                                max_resp_arguments,
                            );
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
                    _ => {
                        if replication.is_replica_mode() && command_is_mutating(command) {
                            responses.extend_from_slice(
                                b"-READONLY You can't write against a read only replica.\r\n",
                            );
                            consumed += frame_bytes_consumed;
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
                                    consumed += frame_bytes_consumed;
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
                            for key_arg in &args[1..argument_count] {
                                // SAFETY: `args` points to the live command frame.
                                let key = unsafe { key_arg.as_slice() };
                                let version = processor.watch_key_version(key);
                                transaction.watch_key(key, version);
                            }
                            append_simple_string(&mut responses, b"OK");
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
                        if replication.is_replica_mode() && command_is_mutating(command) {
                            responses.extend_from_slice(
                                b"-READONLY You can't write against a read only replica.\r\n",
                            );
                            consumed += frame_bytes_consumed;
                            continue;
                        }
                        if is_blocking_command(command) && !responses.is_empty() {
                            stream.write_all(&responses).await?;
                            responses.clear();
                        }
                        let mut replication_frame: Option<Vec<u8>> = None;
                        match execute_blocking_frame_on_owner_thread(
                            &processor,
                            &metrics,
                            &owner_thread_pool,
                            &args[..argument_count],
                            command,
                            frame,
                            client_id,
                            &mut stream,
                        )
                        .await
                        {
                            Ok((frame_response, should_replicate)) => {
                                responses.extend_from_slice(&frame_response);
                                if should_replicate {
                                    replication_frame = replication_frame_for_command(
                                        command,
                                        &args[..argument_count],
                                        &frame_response,
                                        frame,
                                    );
                                }
                            }
                            Err(OwnerThreadExecutionError::Request(error)) => {
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
                        if let Some(frame_to_replicate) = replication_frame.as_ref() {
                            processor.record_rdb_change(1);
                            replication.publish_write_frame(frame_to_replicate);
                        }
                        consumed += frame_bytes_consumed;
                        continue;
                    }
                }
            }
            if propagate_frame {
                processor.record_rdb_change(1);
                replication.publish_write_frame(frame);
            }
            consumed += frame_bytes_consumed;
        }

        if switch_to_replica_stream {
            if consumed > 0 {
                receive_buffer.drain(..consumed);
            }
            if !responses.is_empty() {
                stream.write_all(&responses).await?;
            }
            return replication.serve_downstream_replica(stream).await;
        }

        if consumed > 0 {
            receive_buffer.drain(..consumed);
        }

        if !responses.is_empty() {
            stream.write_all(&responses).await?;
        }
    }
}

async fn execute_blocking_frame_on_owner_thread(
    processor: &Arc<RequestProcessor>,
    metrics: &Arc<ServerMetrics>,
    owner_thread_pool: &Arc<ShardOwnerThreadPool>,
    args: &[ArgSlice],
    command: CommandId,
    frame: &[u8],
    client_id: u64,
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
                    ClientUnblockMode::Timeout => blocking_empty_response_for_command(command).to_vec(),
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
            sleep(BLOCKING_COMMAND_NON_TURN_POLL_INTERVAL).await;
            continue;
        }

        let frame_response =
            match execute_frame_on_owner_thread(processor, owner_thread_pool, args, command, frame)
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

        let mutating_command = command_is_mutating(command);
        let should_replicate = if is_blocking_command(command) {
            mutating_command && !is_blocking_empty_response(&frame_response)
        } else {
            mutating_command
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
    client_id: u64,
    blocking_keys: &[Vec<u8>],
) {
    // Shared cleanup for both successful wakeups and disconnect/unblock paths.
    // In TLA+ terms this is the queue-removal side of `Disconnect(c)` and wake completion.
    processor.decrement_blocked_clients();
    processor.unregister_blocking_wait(client_id, blocking_keys);
    processor.clear_client_unblock_request(client_id);
    metrics.set_client_blocked(client_id, false);
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

fn blocking_wait_keys(command: CommandId, args: &[ArgSlice]) -> Vec<Vec<u8>> {
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
                    unsafe { arg.as_slice() }.to_vec()
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
                unsafe { args[1].as_slice() }.to_vec(),
            ]
        }
        CommandId::Blmpop | CommandId::Bzmpop => {
            if args.len() < 5 {
                return Vec::new();
            }
            // SAFETY: The argument slice was parsed from a live request frame and stays valid
            // while the request is being executed.
            let numkeys = parse_u64_ascii(unsafe { args[2].as_slice() }).unwrap_or(0) as usize;
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
                    unsafe { arg.as_slice() }.to_vec()
                })
                .collect()
        }
        _ => Vec::new(),
    }
}

fn parse_blocking_timeout_arg(args: &[ArgSlice], timeout_index: usize) -> Option<f64> {
    // SAFETY: The argument slice was parsed from a live request frame and stays valid
    // while the request is being executed.
    let timeout_slice = unsafe { args.get(timeout_index)?.as_slice() };
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
    client_id: u64,
    args: &[ArgSlice],
    response_out: &mut Vec<u8>,
) {
    if args.len() < 2 {
        append_wrong_arity_error_for_command(response_out, CommandId::Client);
        return;
    }

    // SAFETY: `args` points to the current request frame.
    let subcommand = unsafe { args[1].as_slice() };
    if ascii_eq_ignore_case(subcommand, b"ID") {
        if args.len() != 2 {
            append_wrong_arity_error_for_command(response_out, CommandId::Client);
            return;
        }
        append_integer_frame(response_out, client_id as i64);
        return;
    }

    if ascii_eq_ignore_case(subcommand, b"GETNAME") {
        if args.len() != 2 {
            append_wrong_arity_error_for_command(response_out, CommandId::Client);
            return;
        }
        if let Some(name) = metrics.client_name(client_id) {
            append_bulk_string_frame(response_out, &name);
        } else {
            response_out.extend_from_slice(b"$-1\r\n");
        }
        return;
    }

    if ascii_eq_ignore_case(subcommand, b"SETNAME") {
        if args.len() != 3 {
            append_wrong_arity_error_for_command(response_out, CommandId::Client);
            return;
        }
        // SAFETY: `args` points to the current request frame.
        let new_name = unsafe { args[2].as_slice() }.to_vec();
        metrics.set_client_name(client_id, Some(new_name));
        append_simple_string(response_out, b"OK");
        return;
    }

    if ascii_eq_ignore_case(subcommand, b"LIST") {
        let mut filter_id = None;
        if args.len() > 2 {
            if args.len() != 4 {
                response_out.extend_from_slice(b"-ERR syntax error\r\n");
                return;
            }
            // SAFETY: `args` points to the current request frame.
            let option = unsafe { args[2].as_slice() };
            if !ascii_eq_ignore_case(option, b"ID") {
                response_out.extend_from_slice(b"-ERR syntax error\r\n");
                return;
            }
            // SAFETY: `args` points to the current request frame.
            let id_arg = unsafe { args[3].as_slice() };
            let Some(parsed_id) = parse_u64_ascii(id_arg) else {
                response_out.extend_from_slice(b"-ERR value is not an integer or out of range\r\n");
                return;
            };
            filter_id = Some(parsed_id);
        }
        let payload = metrics.render_client_list_payload(filter_id);
        append_bulk_string_frame(response_out, &payload);
        return;
    }

    if ascii_eq_ignore_case(subcommand, b"UNBLOCK") {
        if args.len() != 3 && args.len() != 4 {
            append_wrong_arity_error_for_command(response_out, CommandId::Client);
            return;
        }
        // SAFETY: `args` points to the current request frame.
        let id_arg = unsafe { args[2].as_slice() };
        let Some(target_client_id) = parse_u64_ascii(id_arg) else {
            response_out.extend_from_slice(b"-ERR value is not an integer or out of range\r\n");
            return;
        };
        let unblock_mode = if args.len() == 4 {
            // SAFETY: `args` points to the current request frame.
            let mode_arg = unsafe { args[3].as_slice() };
            if ascii_eq_ignore_case(mode_arg, b"TIMEOUT") {
                ClientUnblockMode::Timeout
            } else if ascii_eq_ignore_case(mode_arg, b"ERROR") {
                ClientUnblockMode::Error
            } else {
                response_out.extend_from_slice(b"-ERR syntax error\r\n");
                return;
            }
        } else {
            ClientUnblockMode::Timeout
        };
        let unblocked = target_client_id != client_id
            && metrics.is_client_blocked(target_client_id)
            && processor.request_client_unblock(target_client_id, unblock_mode);
        append_integer_frame(response_out, if unblocked { 1 } else { 0 });
        return;
    }

    if ascii_eq_ignore_case(subcommand, b"PAUSE") {
        if args.len() != 3 && args.len() != 4 {
            append_wrong_arity_error_for_command(response_out, CommandId::Client);
            return;
        }
        // SAFETY: `args` points to the current request frame.
        let timeout_arg = unsafe { args[2].as_slice() };
        if parse_u64_ascii(timeout_arg).is_none() {
            response_out.extend_from_slice(b"-ERR value is not an integer or out of range\r\n");
            return;
        }
        if args.len() == 4 {
            // SAFETY: `args` points to the current request frame.
            let mode = unsafe { args[3].as_slice() };
            if !ascii_eq_ignore_case(mode, b"WRITE") && !ascii_eq_ignore_case(mode, b"ALL") {
                response_out.extend_from_slice(b"-ERR syntax error\r\n");
                return;
            }
        }
        append_simple_string(response_out, b"OK");
        return;
    }

    if ascii_eq_ignore_case(subcommand, b"UNPAUSE") {
        if args.len() != 2 {
            append_wrong_arity_error_for_command(response_out, CommandId::Client);
            return;
        }
        append_simple_string(response_out, b"OK");
        return;
    }

    if ascii_eq_ignore_case(subcommand, b"NO-TOUCH") {
        if args.len() != 3 {
            append_wrong_arity_error_for_command(response_out, CommandId::Client);
            return;
        }
        // SAFETY: `args` points to the current request frame.
        let mode = unsafe { args[2].as_slice() };
        if !ascii_eq_ignore_case(mode, b"ON") && !ascii_eq_ignore_case(mode, b"OFF") {
            response_out.extend_from_slice(b"-ERR syntax error\r\n");
            return;
        }
        append_simple_string(response_out, b"OK");
        return;
    }

    response_out.extend_from_slice(b"-ERR unknown command\r\n");
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

fn parse_u64_ascii(value: &[u8]) -> Option<u64> {
    let text = std::str::from_utf8(value).ok()?;
    text.parse::<u64>().ok()
}

fn replication_frame_for_command(
    command: CommandId,
    args: &[ArgSlice],
    frame_response: &[u8],
    original_frame: &[u8],
) -> Option<Vec<u8>> {
    if matches!(command, CommandId::Lmpop | CommandId::Blmpop) {
        let (key, popped_count) = parse_lmpop_replication_meta(frame_response)?;
        let pop_command = lmpop_pop_command(command, args)?;
        let count = popped_count.to_string().into_bytes();
        return Some(encode_resp_frame(&[pop_command.to_vec(), key, count]));
    }
    Some(original_frame.to_vec())
}

fn lmpop_pop_command(command: CommandId, args: &[ArgSlice]) -> Option<&'static [u8]> {
    let direction_index = match command {
        CommandId::Lmpop => {
            let numkeys = parse_u64_ascii(
                // SAFETY: args were parsed from the current request frame.
                unsafe { args.get(1)?.as_slice() },
            )? as usize;
            2usize.checked_add(numkeys)?
        }
        CommandId::Blmpop => {
            let numkeys = parse_u64_ascii(
                // SAFETY: args were parsed from the current request frame.
                unsafe { args.get(2)?.as_slice() },
            )? as usize;
            3usize.checked_add(numkeys)?
        }
        _ => return None,
    };
    // SAFETY: args were parsed from the current request frame.
    let direction = unsafe { args.get(direction_index)?.as_slice() };
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
    let (count, _) = parse_resp_decimal(frame_response, cursor)?;
    Some((key, count))
}

fn parse_resp_bulk_string(input: &[u8], cursor: usize) -> Option<(Vec<u8>, usize)> {
    if input.get(cursor)? != &b'$' {
        return None;
    }
    let (len, mut cursor) = parse_resp_decimal(input, cursor + 1)?;
    let end = cursor.checked_add(len)?;
    let value = input.get(cursor..end)?.to_vec();
    cursor = end;
    if input.get(cursor..cursor + 2)? != b"\r\n" {
        return None;
    }
    cursor += 2;
    Some((value, cursor))
}

fn parse_resp_decimal(input: &[u8], cursor: usize) -> Option<(usize, usize)> {
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
    Some((value, index + 2))
}

pub(crate) fn build_owner_thread_pool(
    processor: &Arc<RequestProcessor>,
) -> io::Result<Arc<ShardOwnerThreadPool>> {
    let owner_threads = parse_positive_env_usize(GARNET_STRING_OWNER_THREADS_ENV)
        .unwrap_or(DEFAULT_OWNER_THREAD_COUNT);
    let shard_count = processor.string_store_shard_count();
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
    ProtocolError,
}

fn parse_inline_frame(input: &[u8]) -> InlineFrameParse {
    let Some(newline_offset) = input.iter().position(|byte| *byte == b'\n') else {
        return InlineFrameParse::Incomplete;
    };

    let bytes_consumed = newline_offset + 1;
    let mut line = &input[..newline_offset];
    if line.ends_with(b"\r") {
        line = &line[..line.len() - 1];
    }

    let tokens = match tokenize_inline_command(line) {
        Ok(tokens) if !tokens.is_empty() => tokens,
        _ => return InlineFrameParse::ProtocolError,
    };

    InlineFrameParse::Parsed {
        frame: encode_resp_frame(&tokens),
        bytes_consumed,
    }
}

fn tokenize_inline_command(line: &[u8]) -> Result<Vec<Vec<u8>>, ()> {
    let mut tokens = Vec::new();
    let mut current = Vec::new();
    let mut quote: Option<u8> = None;
    let mut escaping = false;

    for &byte in line {
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
            } else {
                current.push(byte);
            }
            continue;
        }

        if byte == b'\'' || byte == b'"' {
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
        return Err(());
    }
    if !current.is_empty() {
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

fn append_too_many_arguments_error(output: &mut Vec<u8>, max_resp_arguments: usize) {
    output.extend_from_slice(b"-ERR too many arguments in request (max ");
    output.extend_from_slice(max_resp_arguments.to_string().as_bytes());
    output.extend_from_slice(b")\r\n");
}

struct ConnectionLifecycle<'a> {
    metrics: &'a ServerMetrics,
    client_id: u64,
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use garnet_common::parse_resp_command_arg_slices;

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
        assert_eq!(unsafe { args[0].as_slice() }, b"SET");
        // SAFETY: args reference `frame`, which is alive in this scope.
        assert_eq!(unsafe { args[1].as_slice() }, b"key");
        // SAFETY: args reference `frame`, which is alive in this scope.
        assert_eq!(unsafe { args[2].as_slice() }, b"value");
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
        assert_eq!(unsafe { args[1].as_slice() }, b"key with space");
        // SAFETY: args reference `frame`, which is alive in this scope.
        assert_eq!(unsafe { args[2].as_slice() }, b"v'1");
    }

    #[test]
    fn inline_frame_waits_for_newline_before_parsing() {
        assert!(matches!(
            parse_inline_frame(b"SET key value"),
            InlineFrameParse::Incomplete
        ));
    }
}
