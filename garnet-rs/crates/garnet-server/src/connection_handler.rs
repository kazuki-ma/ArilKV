use std::io;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use garnet_cluster::ClusterConfigStore;
use garnet_common::{parse_resp_command_arg_slices, ArgSlice, RespParseError};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::command_dispatch::dispatch_from_arg_slices;
use crate::command_spec::{
    command_has_valid_arity, command_is_mutating, command_transaction_control,
    TransactionControlCommand,
};
use crate::connection_owner_routing::{
    capture_owned_frame_args, execute_owned_frame_args_via_processor, RoutedExecutionError,
};
use crate::connection_protocol::{
    append_error_line, append_simple_string, append_wrong_arity_error_for_command,
    ascii_eq_ignore_case, parse_u16_ascii,
};
use crate::connection_routing::{
    cluster_error_for_command, command_hash_slot_for_transaction, owner_shard_for_command,
};
use crate::connection_transaction::{execute_transaction_queue, ConnectionTransactionState};
use crate::redis_replication::RedisReplicationCoordinator;
use crate::{RequestProcessor, ServerMetrics, ShardOwnerThreadPool};

const DEFAULT_OWNER_THREAD_COUNT: usize = 1;
const GARNET_STRING_OWNER_THREADS_ENV: &str = "GARNET_STRING_OWNER_THREADS";

pub(crate) async fn handle_connection(
    mut stream: TcpStream,
    read_buffer_size: usize,
    metrics: Arc<ServerMetrics>,
    processor: Arc<RequestProcessor>,
    cluster_config: Option<Arc<ClusterConfigStore>>,
    owner_thread_pool: Arc<ShardOwnerThreadPool>,
    replication: Arc<RedisReplicationCoordinator>,
) -> io::Result<()> {
    let _lifecycle = ConnectionLifecycle { metrics: &metrics };
    let mut read_buffer = vec![0u8; read_buffer_size.max(1)];
    let mut receive_buffer = Vec::with_capacity(read_buffer_size.max(1));
    let mut responses = Vec::with_capacity(read_buffer_size.max(1));
    let mut args = [ArgSlice::EMPTY; 64];
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
            match parse_resp_command_arg_slices(&receive_buffer[consumed..], &mut args) {
                Ok(meta) => {
                    let frame_start = consumed;
                    let frame_end = consumed + meta.bytes_consumed;
                    // SAFETY: `args` refers to the still-live `receive_buffer` backing bytes.
                    let command = unsafe { dispatch_from_arg_slices(&args[..meta.argument_count]) };
                    if meta.argument_count == 0 {
                        responses.extend_from_slice(b"-ERR unknown command\r\n");
                        consumed += meta.bytes_consumed;
                        continue;
                    }
                    // SAFETY: `args` points to the current request frame in `receive_buffer`.
                    let command_name = unsafe { args[0].as_slice() };

                    if ascii_eq_ignore_case(command_name, b"REPLICAOF") {
                        if meta.argument_count != 3 {
                            responses.extend_from_slice(
                                b"-ERR wrong number of arguments for 'REPLICAOF' command\r\n",
                            );
                        } else {
                            // SAFETY: `args` points to the current request frame in `receive_buffer`.
                            let arg1 = unsafe { args[1].as_slice() };
                            // SAFETY: `args` points to the current request frame in `receive_buffer`.
                            let arg2 = unsafe { args[2].as_slice() };

                            if ascii_eq_ignore_case(arg1, b"NO")
                                && ascii_eq_ignore_case(arg2, b"ONE")
                            {
                                replication.become_master().await;
                                append_simple_string(&mut responses, b"OK");
                            } else if let Some(master_port) = parse_u16_ascii(arg2) {
                                let master_host = String::from_utf8_lossy(arg1).to_string();
                                replication.become_replica(master_host, master_port).await;
                                append_simple_string(&mut responses, b"OK");
                            } else {
                                responses.extend_from_slice(
                                    b"-ERR value is not an integer or out of range\r\n",
                                );
                            }
                        }
                        consumed += meta.bytes_consumed;
                        continue;
                    }

                    if ascii_eq_ignore_case(command_name, b"REPLCONF") {
                        append_simple_string(&mut responses, b"OK");
                        consumed += meta.bytes_consumed;
                        continue;
                    }

                    if ascii_eq_ignore_case(command_name, b"PSYNC")
                        || ascii_eq_ignore_case(command_name, b"SYNC")
                    {
                        responses.extend_from_slice(&replication.build_fullresync_payload());
                        consumed += meta.bytes_consumed;
                        switch_to_replica_stream = true;
                        break;
                    }

                    let transaction_control = command_transaction_control(command);
                    if transaction_control == TransactionControlCommand::Asking {
                        if !command_has_valid_arity(command, meta.argument_count) {
                            append_wrong_arity_error_for_command(&mut responses, command);
                        } else {
                            allow_asking_once = true;
                            append_simple_string(&mut responses, b"OK");
                        }
                        consumed += meta.bytes_consumed;
                        continue;
                    }
                    if let Some(cluster_store) = cluster_config.as_ref() {
                        let (redirection_error, consume_asking) = cluster_error_for_command(
                            cluster_store,
                            &args[..meta.argument_count],
                            command,
                            allow_asking_once,
                        )?;
                        if consume_asking {
                            allow_asking_once = false;
                        }
                        if let Some(redirection_error) = redirection_error {
                            append_error_line(&mut responses, redirection_error.as_bytes());
                            consumed += meta.bytes_consumed;
                            continue;
                        }
                    }
                    let mut propagate_frame = false;
                    if transaction.in_multi {
                        match transaction_control {
                            TransactionControlCommand::Exec => {
                                if !command_has_valid_arity(command, meta.argument_count) {
                                    append_wrong_arity_error_for_command(&mut responses, command);
                                } else if !processor.watch_versions_match(&transaction.watched_keys)
                                {
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
                                    );
                                }
                            }
                            TransactionControlCommand::Discard => {
                                if !command_has_valid_arity(command, meta.argument_count) {
                                    append_wrong_arity_error_for_command(&mut responses, command);
                                } else {
                                    transaction.reset();
                                    append_simple_string(&mut responses, b"OK");
                                }
                            }
                            TransactionControlCommand::Multi => {
                                responses
                                    .extend_from_slice(b"-ERR MULTI calls can not be nested\r\n");
                            }
                            TransactionControlCommand::Watch => {
                                responses.extend_from_slice(
                                    b"-ERR WATCH inside MULTI is not allowed\r\n",
                                );
                            }
                            TransactionControlCommand::Unwatch => {
                                if !command_has_valid_arity(command, meta.argument_count) {
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
                                    consumed += meta.bytes_consumed;
                                    continue;
                                }
                                if cluster_config.is_some() {
                                    if let Some(slot) = command_hash_slot_for_transaction(
                                        &args[..meta.argument_count],
                                        command,
                                    ) {
                                        if !transaction.set_transaction_slot_or_abort(slot) {
                                            responses.extend_from_slice(
                                                b"-CROSSSLOT Keys in request don't hash to the same slot\r\n",
                                            );
                                            consumed += meta.bytes_consumed;
                                            continue;
                                        }
                                    }
                                }
                                transaction
                                    .queued_frames
                                    .push(receive_buffer[frame_start..frame_end].to_vec());
                                append_simple_string(&mut responses, b"QUEUED");
                            }
                        }
                    } else {
                        match transaction_control {
                            TransactionControlCommand::Multi => {
                                if !command_has_valid_arity(command, meta.argument_count) {
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
                                if !command_has_valid_arity(command, meta.argument_count) {
                                    append_wrong_arity_error_for_command(&mut responses, command);
                                } else {
                                    for key_arg in &args[1..meta.argument_count] {
                                        // SAFETY: `args` points to the live receive buffer.
                                        let key = unsafe { key_arg.as_slice() };
                                        let version = processor.watch_key_version(key);
                                        transaction.watch_key(key, version);
                                    }
                                    append_simple_string(&mut responses, b"OK");
                                }
                            }
                            TransactionControlCommand::Unwatch => {
                                if !command_has_valid_arity(command, meta.argument_count) {
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
                                    consumed += meta.bytes_consumed;
                                    continue;
                                }
                                let shard_index = owner_shard_for_command(
                                    &processor,
                                    &args[..meta.argument_count],
                                    command,
                                );
                                let owned_args = match capture_owned_frame_args(
                                    &receive_buffer[frame_start..frame_end],
                                    &args[..meta.argument_count],
                                ) {
                                    Ok(owned_args) => owned_args,
                                    Err(_) => {
                                        responses.extend_from_slice(b"-ERR protocol error\r\n");
                                        consumed += meta.bytes_consumed;
                                        continue;
                                    }
                                };
                                let routed_processor = Arc::clone(&processor);
                                match owner_thread_pool.execute_sync(shard_index, move || {
                                    execute_owned_frame_args_via_processor(
                                        &routed_processor,
                                        &owned_args,
                                    )
                                }) {
                                    Ok(Ok(frame_response)) => {
                                        responses.extend_from_slice(&frame_response);
                                        if command_is_mutating(command) {
                                            propagate_frame = true;
                                        }
                                    }
                                    Ok(Err(RoutedExecutionError::Request(error))) => {
                                        error.append_resp_error(&mut responses);
                                    }
                                    Ok(Err(RoutedExecutionError::Protocol)) => {
                                        responses.extend_from_slice(b"-ERR protocol error\r\n");
                                    }
                                    Err(_) => {
                                        responses.extend_from_slice(
                                            b"-ERR owner routing execution failed\r\n",
                                        );
                                    }
                                }
                                if propagate_frame {
                                    replication.publish_write_frame(
                                        &receive_buffer[frame_start..frame_end],
                                    );
                                }
                                consumed += meta.bytes_consumed;
                                continue;
                            }
                        }
                    }
                    if propagate_frame {
                        replication.publish_write_frame(&receive_buffer[frame_start..frame_end]);
                    }
                    consumed += meta.bytes_consumed;
                }
                Err(RespParseError::Incomplete) => break,
                Err(_) => {
                    responses.extend_from_slice(b"-ERR protocol error\r\n");
                    stream.write_all(&responses).await?;
                    return Ok(());
                }
            }
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

fn parse_positive_env_usize(key: &str) -> Option<usize> {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
}

struct ConnectionLifecycle<'a> {
    metrics: &'a ServerMetrics,
}

impl Drop for ConnectionLifecycle<'_> {
    fn drop(&mut self) {
        self.metrics
            .active_connections
            .fetch_sub(1, Ordering::Relaxed);
        self.metrics
            .closed_connections
            .fetch_add(1, Ordering::Relaxed);
    }
}
