//! TCP server accept loop and connection handler primitives.

pub mod aof_replay;
pub mod command_dispatch;
pub mod limited_fixed_buffer_pool;
pub mod request_lifecycle;

pub use aof_replay::{replay_aof_file, replay_aof_operations};
pub use command_dispatch::{
    dispatch_command_name, dispatch_from_arg_slices, dispatch_from_resp_args, CommandId,
};
pub use limited_fixed_buffer_pool::{
    LimitedFixedBufferPool, LimitedFixedBufferPoolConfig, LimitedFixedBufferPoolError, PoolEntry,
    ReturnStatus,
};
pub use request_lifecycle::{
    MigrationEntry, MigrationValue, RequestExecutionError, RequestProcessor,
    RequestProcessorInitError,
};

use garnet_cluster::{redis_hash_slot, ClusterConfigStore, SlotRouteDecision};
use garnet_common::{parse_resp_command_arg_slices, ArgSlice, RespParseError};
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServerConfig {
    pub bind_addr: SocketAddr,
    pub read_buffer_size: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:6379"
                .parse()
                .expect("default bind address must parse"),
            read_buffer_size: 8 * 1024,
        }
    }
}

#[derive(Debug, Default)]
pub struct ServerMetrics {
    accepted_connections: AtomicU64,
    active_connections: AtomicU64,
    closed_connections: AtomicU64,
    bytes_received: AtomicU64,
}

impl ServerMetrics {
    #[inline]
    pub fn accepted_connections(&self) -> u64 {
        self.accepted_connections.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn active_connections(&self) -> u64 {
        self.active_connections.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn closed_connections(&self) -> u64 {
        self.closed_connections.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn bytes_received(&self) -> u64 {
        self.bytes_received.load(Ordering::Relaxed)
    }
}

pub async fn run(config: ServerConfig, metrics: Arc<ServerMetrics>) -> io::Result<()> {
    run_with_shutdown(config, metrics, std::future::pending::<()>()).await
}

pub async fn run_with_cluster(
    config: ServerConfig,
    metrics: Arc<ServerMetrics>,
    cluster_config: Arc<ClusterConfigStore>,
) -> io::Result<()> {
    run_with_shutdown_and_cluster_config(
        config,
        metrics,
        std::future::pending::<()>(),
        Some(cluster_config),
    )
    .await
}

pub async fn run_with_shutdown<F>(
    config: ServerConfig,
    metrics: Arc<ServerMetrics>,
    shutdown: F,
) -> io::Result<()>
where
    F: Future<Output = ()> + Send,
{
    run_with_shutdown_and_cluster_config(config, metrics, shutdown, None).await
}

pub async fn run_with_shutdown_and_cluster_config<F>(
    config: ServerConfig,
    metrics: Arc<ServerMetrics>,
    shutdown: F,
    cluster_config: Option<Arc<ClusterConfigStore>>,
) -> io::Result<()>
where
    F: Future<Output = ()> + Send,
{
    let listener = TcpListener::bind(config.bind_addr).await?;
    run_listener_with_shutdown_and_cluster(
        listener,
        config.read_buffer_size,
        metrics,
        shutdown,
        cluster_config,
    )
    .await
}

pub async fn run_listener_with_shutdown<F>(
    listener: TcpListener,
    read_buffer_size: usize,
    metrics: Arc<ServerMetrics>,
    shutdown: F,
) -> io::Result<()>
where
    F: Future<Output = ()> + Send,
{
    run_listener_with_shutdown_and_cluster(listener, read_buffer_size, metrics, shutdown, None)
        .await
}

pub async fn run_listener_with_shutdown_and_cluster<F>(
    listener: TcpListener,
    read_buffer_size: usize,
    metrics: Arc<ServerMetrics>,
    shutdown: F,
    cluster_config: Option<Arc<ClusterConfigStore>>,
) -> io::Result<()>
where
    F: Future<Output = ()> + Send,
{
    let processor = Arc::new(RequestProcessor::new().map_err(|err| {
        io::Error::new(
            io::ErrorKind::Other,
            format!("request processor initialization failed: {err}"),
        )
    })?);
    run_listener_with_shutdown_and_cluster_with_processor(
        listener,
        read_buffer_size,
        metrics,
        shutdown,
        cluster_config,
        processor,
    )
    .await
}

pub async fn run_listener_with_shutdown_and_cluster_with_processor<F>(
    listener: TcpListener,
    read_buffer_size: usize,
    metrics: Arc<ServerMetrics>,
    shutdown: F,
    cluster_config: Option<Arc<ClusterConfigStore>>,
    processor: Arc<RequestProcessor>,
) -> io::Result<()>
where
    F: Future<Output = ()> + Send,
{
    let mut tasks = JoinSet::new();
    let expiration_processor = Arc::clone(&processor);
    let expiration_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(50));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let _ = expiration_processor.expire_stale_keys(128);
        }
    });
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            _ = &mut shutdown => {
                break;
            }
            accept_result = listener.accept() => {
                let (stream, _) = accept_result?;
                metrics.accepted_connections.fetch_add(1, Ordering::Relaxed);
                metrics.active_connections.fetch_add(1, Ordering::Relaxed);

                let task_metrics = Arc::clone(&metrics);
                let task_processor = Arc::clone(&processor);
                let task_cluster = cluster_config.clone();
                tasks.spawn(async move {
                    let _ =
                        handle_connection(
                            stream,
                            read_buffer_size,
                            task_metrics,
                            task_processor,
                            task_cluster,
                        )
                        .await;
                });
            }
        }
    }

    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    expiration_task.abort();
    let _ = expiration_task.await;
    Ok(())
}

async fn handle_connection(
    mut stream: TcpStream,
    read_buffer_size: usize,
    metrics: Arc<ServerMetrics>,
    processor: Arc<RequestProcessor>,
    cluster_config: Option<Arc<ClusterConfigStore>>,
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

        loop {
            match parse_resp_command_arg_slices(&receive_buffer[consumed..], &mut args) {
                Ok(meta) => {
                    let frame_start = consumed;
                    let frame_end = consumed + meta.bytes_consumed;
                    // SAFETY: `args` refers to the still-live `receive_buffer` backing bytes.
                    let command = unsafe { dispatch_from_arg_slices(&args[..meta.argument_count]) };
                    if command == CommandId::Asking {
                        if meta.argument_count != 1 {
                            responses.extend_from_slice(
                                b"-ERR wrong number of arguments for 'ASKING' command\r\n",
                            );
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
                    if transaction.in_multi {
                        match command {
                            CommandId::Exec => {
                                if meta.argument_count != 1 {
                                    responses.extend_from_slice(
                                        b"-ERR wrong number of arguments for 'EXEC' command\r\n",
                                    );
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
                                        &mut transaction,
                                        &mut responses,
                                    );
                                }
                            }
                            CommandId::Discard => {
                                if meta.argument_count != 1 {
                                    responses.extend_from_slice(
                                        b"-ERR wrong number of arguments for 'DISCARD' command\r\n",
                                    );
                                } else {
                                    transaction.reset();
                                    append_simple_string(&mut responses, b"OK");
                                }
                            }
                            CommandId::Multi => {
                                responses
                                    .extend_from_slice(b"-ERR MULTI calls can not be nested\r\n");
                            }
                            CommandId::Watch => {
                                responses.extend_from_slice(
                                    b"-ERR WATCH inside MULTI is not allowed\r\n",
                                );
                            }
                            CommandId::Unwatch => {
                                if meta.argument_count != 1 {
                                    responses.extend_from_slice(
                                        b"-ERR wrong number of arguments for 'UNWATCH' command\r\n",
                                    );
                                } else {
                                    // Matches Garnet behavior: UNWATCH during MULTI is a no-op.
                                    append_simple_string(&mut responses, b"OK");
                                }
                            }
                            _ => {
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
                        match command {
                            CommandId::Multi => {
                                if meta.argument_count != 1 {
                                    responses.extend_from_slice(
                                        b"-ERR wrong number of arguments for 'MULTI' command\r\n",
                                    );
                                } else {
                                    transaction.in_multi = true;
                                    append_simple_string(&mut responses, b"OK");
                                }
                            }
                            CommandId::Exec => {
                                responses.extend_from_slice(b"-ERR EXEC without MULTI\r\n");
                            }
                            CommandId::Discard => {
                                responses.extend_from_slice(b"-ERR DISCARD without MULTI\r\n");
                            }
                            CommandId::Watch => {
                                if meta.argument_count < 2 {
                                    responses.extend_from_slice(
                                        b"-ERR wrong number of arguments for 'WATCH' command\r\n",
                                    );
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
                            CommandId::Unwatch => {
                                if meta.argument_count != 1 {
                                    responses.extend_from_slice(
                                        b"-ERR wrong number of arguments for 'UNWATCH' command\r\n",
                                    );
                                } else {
                                    transaction.clear_watches();
                                    append_simple_string(&mut responses, b"OK");
                                }
                            }
                            _ => {
                                if let Err(error) =
                                    processor.execute(&args[..meta.argument_count], &mut responses)
                                {
                                    error.append_resp_error(&mut responses);
                                }
                            }
                        }
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

        if consumed > 0 {
            receive_buffer.drain(..consumed);
        }

        if !responses.is_empty() {
            stream.write_all(&responses).await?;
        }
    }
}

#[derive(Default)]
struct ConnectionTransactionState {
    in_multi: bool,
    queued_frames: Vec<Vec<u8>>,
    watched_keys: Vec<(Vec<u8>, u64)>,
    transaction_slot: Option<u16>,
    aborted: bool,
}

impl ConnectionTransactionState {
    fn reset(&mut self) {
        self.in_multi = false;
        self.queued_frames.clear();
        self.watched_keys.clear();
        self.transaction_slot = None;
        self.aborted = false;
    }

    fn clear_watches(&mut self) {
        self.watched_keys.clear();
    }

    fn watch_key(&mut self, key: &[u8], version: u64) {
        if let Some((_, watched_version)) = self
            .watched_keys
            .iter_mut()
            .find(|(watched_key, _)| watched_key.as_slice() == key)
        {
            *watched_version = version;
            return;
        }
        self.watched_keys.push((key.to_vec(), version));
    }

    fn set_transaction_slot_or_abort(&mut self, slot: u16) -> bool {
        match self.transaction_slot {
            None => {
                self.transaction_slot = Some(slot);
                true
            }
            Some(existing) if existing == slot => true,
            Some(_) => {
                self.aborted = true;
                false
            }
        }
    }
}

fn execute_transaction_queue(
    processor: &RequestProcessor,
    transaction: &mut ConnectionTransactionState,
    responses: &mut Vec<u8>,
) {
    let queued = std::mem::take(&mut transaction.queued_frames);
    transaction.in_multi = false;
    transaction.watched_keys.clear();
    transaction.transaction_slot = None;
    transaction.aborted = false;

    responses.push(b'*');
    responses.extend_from_slice(queued.len().to_string().as_bytes());
    responses.extend_from_slice(b"\r\n");

    let mut args = [ArgSlice::EMPTY; 64];
    for frame in queued {
        let mut item_response = Vec::new();
        match parse_resp_command_arg_slices(&frame, &mut args) {
            Ok(meta) if meta.bytes_consumed == frame.len() => {
                if let Err(error) =
                    processor.execute(&args[..meta.argument_count], &mut item_response)
                {
                    error.append_resp_error(&mut item_response);
                }
            }
            _ => item_response.extend_from_slice(b"-ERR protocol error\r\n"),
        }
        responses.extend_from_slice(&item_response);
    }
}

fn append_simple_string(output: &mut Vec<u8>, value: &[u8]) {
    output.push(b'+');
    output.extend_from_slice(value);
    output.extend_from_slice(b"\r\n");
}

fn append_error_line(output: &mut Vec<u8>, value: &[u8]) {
    output.push(b'-');
    output.extend_from_slice(value);
    output.extend_from_slice(b"\r\n");
}

fn cluster_error_for_command(
    cluster_store: &ClusterConfigStore,
    args: &[ArgSlice],
    command: CommandId,
    asking_allowed: bool,
) -> io::Result<(Option<String>, bool)> {
    if args.len() < 2 {
        return Ok((None, asking_allowed));
    }

    match command {
        CommandId::Del | CommandId::Watch => {
            let config = cluster_store.load();
            let mut first_slot = None;
            for arg in &args[1..] {
                // SAFETY: argument slices reference the current request frame.
                let key = unsafe { arg.as_slice() };
                let slot = redis_hash_slot(key);

                if let Some(existing) = first_slot {
                    if slot != existing {
                        return Ok((
                            Some(
                                "CROSSSLOT Keys in request don't hash to the same slot".to_string(),
                            ),
                            true,
                        ));
                    }
                } else {
                    first_slot = Some(slot);
                }

                if let Some(redirection_error) =
                    cluster_redirection_for_slot(&config, slot, asking_allowed).map_err(
                        |error| {
                            io::Error::new(io::ErrorKind::Other, format!("cluster error: {error}"))
                        },
                    )?
                {
                    return Ok((Some(redirection_error), true));
                }
            }
            Ok((None, true))
        }
        _ => {
            let uses_first_key = matches!(
                command,
                CommandId::Get
                    | CommandId::Set
                    | CommandId::Incr
                    | CommandId::Decr
                    | CommandId::Expire
                    | CommandId::Ttl
                    | CommandId::Pexpire
                    | CommandId::Pttl
                    | CommandId::Persist
                    | CommandId::Hset
                    | CommandId::Hget
                    | CommandId::Hdel
                    | CommandId::Hgetall
                    | CommandId::Lpush
                    | CommandId::Rpush
                    | CommandId::Lpop
                    | CommandId::Rpop
                    | CommandId::Lrange
                    | CommandId::Sadd
                    | CommandId::Srem
                    | CommandId::Smembers
                    | CommandId::Sismember
                    | CommandId::Zadd
                    | CommandId::Zrem
                    | CommandId::Zrange
                    | CommandId::Zscore
            );
            if !uses_first_key {
                return Ok((None, asking_allowed));
            }

            // SAFETY: argument slices reference the current request frame.
            let key = unsafe { args[1].as_slice() };
            let slot = redis_hash_slot(key);
            let error = cluster_redirection_for_slot(&cluster_store.load(), slot, asking_allowed)
                .map_err(|cluster_error| {
                io::Error::new(
                    io::ErrorKind::Other,
                    format!("cluster error: {cluster_error}"),
                )
            })?;
            Ok((error, asking_allowed))
        }
    }
}

fn cluster_redirection_for_slot(
    config: &garnet_cluster::ClusterConfig,
    slot: u16,
    asking_allowed: bool,
) -> Result<Option<String>, garnet_cluster::ClusterConfigError> {
    match config.route_for_slot(slot)? {
        SlotRouteDecision::Local => Ok(None),
        SlotRouteDecision::Ask { .. } if asking_allowed => Ok(None),
        _ => config.redirection_error_for_slot(slot),
    }
}

fn command_hash_slot_for_transaction(args: &[ArgSlice], command: CommandId) -> Option<u16> {
    if args.len() < 2 {
        return None;
    }
    match command {
        CommandId::Del => {
            // SAFETY: argument slices reference the current request frame.
            let first_key = unsafe { args[1].as_slice() };
            Some(redis_hash_slot(first_key))
        }
        CommandId::Get
        | CommandId::Set
        | CommandId::Incr
        | CommandId::Decr
        | CommandId::Expire
        | CommandId::Ttl
        | CommandId::Pexpire
        | CommandId::Pttl
        | CommandId::Persist
        | CommandId::Hset
        | CommandId::Hget
        | CommandId::Hdel
        | CommandId::Hgetall
        | CommandId::Lpush
        | CommandId::Rpush
        | CommandId::Lpop
        | CommandId::Rpop
        | CommandId::Lrange
        | CommandId::Sadd
        | CommandId::Srem
        | CommandId::Smembers
        | CommandId::Sismember
        | CommandId::Zadd
        | CommandId::Zrem
        | CommandId::Zrange
        | CommandId::Zscore => {
            // SAFETY: argument slices reference the current request frame.
            let key = unsafe { args[1].as_slice() };
            Some(redis_hash_slot(key))
        }
        _ => None,
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use garnet_cluster::{
        redis_hash_slot, AsyncGossipEngine, ChannelReplicationTransport, ClusterConfig,
        ClusterConfigStore, ClusterFailoverController, ClusterManager, FailoverCoordinator,
        FailureDetector, GossipCoordinator, GossipNode, InMemoryGossipTransport, ReplicationEvent,
        ReplicationManager, SlotState, Worker, WorkerRole, LOCAL_WORKER_ID,
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::oneshot;
    use tokio::time::{sleep, Duration, Instant};

    #[tokio::test]
    async fn accept_loop_spawns_connection_handlers() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let metrics = Arc::new(ServerMetrics::default());
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        let server_metrics = Arc::clone(&metrics);
        let server = tokio::spawn(async move {
            run_listener_with_shutdown(listener, 1024, server_metrics, async move {
                let _ = shutdown_rx.await;
            })
            .await
            .unwrap();
        });

        let mut client1 = TcpStream::connect(addr).await.unwrap();
        client1.write_all(b"PING").await.unwrap();
        drop(client1);

        let mut client2 = TcpStream::connect(addr).await.unwrap();
        client2.write_all(b"PONG").await.unwrap();
        drop(client2);

        wait_until(
            || metrics.closed_connections() >= 2 && metrics.bytes_received() >= 8,
            Duration::from_secs(1),
        )
        .await;

        let _ = shutdown_tx.send(());
        server.await.unwrap();

        assert_eq!(metrics.accepted_connections(), 2);
        assert_eq!(metrics.active_connections(), 0);
        assert_eq!(metrics.closed_connections(), 2);
        assert!(metrics.bytes_received() >= 8);
    }

    #[tokio::test]
    async fn shutdown_signal_stops_accept_loop() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let metrics = Arc::new(ServerMetrics::default());
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        let server_metrics = Arc::clone(&metrics);
        let server = tokio::spawn(async move {
            run_listener_with_shutdown(listener, 512, server_metrics, async move {
                let _ = shutdown_rx.await;
            })
            .await
            .unwrap();
        });

        let _ = shutdown_tx.send(());
        let joined = tokio::time::timeout(Duration::from_secs(1), server).await;
        assert!(joined.is_ok());
        assert_eq!(metrics.accepted_connections(), 0);
    }

    #[tokio::test]
    async fn tcp_pipeline_executes_basic_crud_commands() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let metrics = Arc::new(ServerMetrics::default());
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        let server_metrics = Arc::clone(&metrics);
        let server = tokio::spawn(async move {
            run_listener_with_shutdown(listener, 1024, server_metrics, async move {
                let _ = shutdown_rx.await;
            })
            .await
            .unwrap();
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        send_and_expect(
            &mut client,
            b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n",
            b"+OK\r\n",
        )
        .await;
        send_and_expect(
            &mut client,
            b"*4\r\n$3\r\nSET\r\n$3\r\nkey\r\n$6\r\nvalue2\r\n$2\r\nNX\r\n",
            b"$-1\r\n",
        )
        .await;
        send_and_expect(
            &mut client,
            b"*4\r\n$3\r\nSET\r\n$3\r\nkey\r\n$7\r\nupdated\r\n$2\r\nXX\r\n",
            b"+OK\r\n",
        )
        .await;
        send_and_expect(
            &mut client,
            b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n",
            b"$7\r\nupdated\r\n",
        )
        .await;
        send_and_expect(
            &mut client,
            b"*5\r\n$3\r\nSET\r\n$3\r\nttl\r\n$5\r\nvalue\r\n$2\r\nPX\r\n$2\r\n10\r\n",
            b"+OK\r\n",
        )
        .await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        send_and_expect(&mut client, b"*2\r\n$3\r\nGET\r\n$3\r\nttl\r\n", b"$-1\r\n").await;
        send_and_expect(
            &mut client,
            b"*3\r\n$3\r\nSET\r\n$6\r\nexpkey\r\n$5\r\nvalue\r\n",
            b"+OK\r\n",
        )
        .await;
        send_and_expect(
            &mut client,
            b"*3\r\n$6\r\nEXPIRE\r\n$6\r\nexpkey\r\n$1\r\n0\r\n",
            b":1\r\n",
        )
        .await;
        send_and_expect(
            &mut client,
            b"*2\r\n$4\r\nPTTL\r\n$6\r\nexpkey\r\n",
            b":-2\r\n",
        )
        .await;
        send_and_expect(
            &mut client,
            b"*3\r\n$3\r\nSET\r\n$6\r\npexkey\r\n$5\r\nvalue\r\n",
            b"+OK\r\n",
        )
        .await;
        send_and_expect(
            &mut client,
            b"*3\r\n$7\r\nPEXPIRE\r\n$6\r\npexkey\r\n$1\r\n0\r\n",
            b":1\r\n",
        )
        .await;
        send_and_expect(
            &mut client,
            b"*2\r\n$3\r\nTTL\r\n$6\r\npexkey\r\n",
            b":-2\r\n",
        )
        .await;
        send_and_expect(
            &mut client,
            b"*5\r\n$3\r\nSET\r\n$11\r\npersist-key\r\n$5\r\nvalue\r\n$2\r\nPX\r\n$4\r\n1000\r\n",
            b"+OK\r\n",
        )
        .await;
        send_and_expect(
            &mut client,
            b"*2\r\n$7\r\nPERSIST\r\n$11\r\npersist-key\r\n",
            b":1\r\n",
        )
        .await;
        send_and_expect(
            &mut client,
            b"*2\r\n$3\r\nTTL\r\n$11\r\npersist-key\r\n",
            b":-1\r\n",
        )
        .await;
        send_and_expect(
            &mut client,
            b"*2\r\n$3\r\nDEL\r\n$11\r\npersist-key\r\n",
            b":1\r\n",
        )
        .await;
        send_and_expect(
            &mut client,
            b"*4\r\n$4\r\nHSET\r\n$4\r\nhkey\r\n$6\r\nfield1\r\n$2\r\nv1\r\n",
            b":1\r\n",
        )
        .await;
        send_and_expect(
            &mut client,
            b"*3\r\n$4\r\nHGET\r\n$4\r\nhkey\r\n$6\r\nfield1\r\n",
            b"$2\r\nv1\r\n",
        )
        .await;
        send_and_expect(
            &mut client,
            b"*2\r\n$7\r\nHGETALL\r\n$4\r\nhkey\r\n",
            b"*2\r\n$6\r\nfield1\r\n$2\r\nv1\r\n",
        )
        .await;
        send_and_expect(
            &mut client,
            b"*3\r\n$4\r\nHDEL\r\n$4\r\nhkey\r\n$6\r\nfield1\r\n",
            b":1\r\n",
        )
        .await;
        send_and_expect(
            &mut client,
            b"*2\r\n$7\r\nHGETALL\r\n$4\r\nhkey\r\n",
            b"*0\r\n",
        )
        .await;
        send_and_expect(
            &mut client,
            b"*4\r\n$5\r\nLPUSH\r\n$4\r\nlkey\r\n$1\r\na\r\n$1\r\nb\r\n",
            b":2\r\n",
        )
        .await;
        send_and_expect(
            &mut client,
            b"*4\r\n$6\r\nLRANGE\r\n$4\r\nlkey\r\n$1\r\n0\r\n$2\r\n-1\r\n",
            b"*2\r\n$1\r\nb\r\n$1\r\na\r\n",
        )
        .await;
        send_and_expect(
            &mut client,
            b"*2\r\n$4\r\nRPOP\r\n$4\r\nlkey\r\n",
            b"$1\r\na\r\n",
        )
        .await;
        send_and_expect(
            &mut client,
            b"*2\r\n$4\r\nLPOP\r\n$4\r\nlkey\r\n",
            b"$1\r\nb\r\n",
        )
        .await;
        send_and_expect(
            &mut client,
            b"*2\r\n$4\r\nLPOP\r\n$4\r\nlkey\r\n",
            b"$-1\r\n",
        )
        .await;
        send_and_expect(
            &mut client,
            b"*4\r\n$4\r\nSADD\r\n$4\r\nskey\r\n$1\r\na\r\n$1\r\nb\r\n",
            b":2\r\n",
        )
        .await;
        send_and_expect(
            &mut client,
            b"*3\r\n$9\r\nSISMEMBER\r\n$4\r\nskey\r\n$1\r\nb\r\n",
            b":1\r\n",
        )
        .await;
        send_and_expect(
            &mut client,
            b"*2\r\n$8\r\nSMEMBERS\r\n$4\r\nskey\r\n",
            b"*2\r\n$1\r\na\r\n$1\r\nb\r\n",
        )
        .await;
        send_and_expect(
            &mut client,
            b"*4\r\n$4\r\nSREM\r\n$4\r\nskey\r\n$1\r\na\r\n$1\r\nb\r\n",
            b":2\r\n",
        )
        .await;
        send_and_expect(
            &mut client,
            b"*2\r\n$8\r\nSMEMBERS\r\n$4\r\nskey\r\n",
            b"*0\r\n",
        )
        .await;
        send_and_expect(
            &mut client,
            b"*6\r\n$4\r\nZADD\r\n$4\r\nzkey\r\n$1\r\n2\r\n$3\r\ntwo\r\n$1\r\n1\r\n$3\r\none\r\n",
            b":2\r\n",
        )
        .await;
        send_and_expect(
            &mut client,
            b"*3\r\n$6\r\nZSCORE\r\n$4\r\nzkey\r\n$3\r\none\r\n",
            b"$1\r\n1\r\n",
        )
        .await;
        send_and_expect(
            &mut client,
            b"*4\r\n$6\r\nZRANGE\r\n$4\r\nzkey\r\n$1\r\n0\r\n$2\r\n-1\r\n",
            b"*2\r\n$3\r\none\r\n$3\r\ntwo\r\n",
        )
        .await;
        send_and_expect(
            &mut client,
            b"*4\r\n$4\r\nZREM\r\n$4\r\nzkey\r\n$3\r\none\r\n$3\r\ntwo\r\n",
            b":2\r\n",
        )
        .await;
        send_and_expect(
            &mut client,
            b"*4\r\n$6\r\nZRANGE\r\n$4\r\nzkey\r\n$1\r\n0\r\n$2\r\n-1\r\n",
            b"*0\r\n",
        )
        .await;
        send_and_expect(
            &mut client,
            b"*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n",
            b":1\r\n",
        )
        .await;
        send_and_expect(
            &mut client,
            b"*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n",
            b":2\r\n",
        )
        .await;
        send_and_expect(
            &mut client,
            b"*1\r\n$4\r\nEXEC\r\n",
            b"-ERR EXEC without MULTI\r\n",
        )
        .await;
        send_and_expect(
            &mut client,
            b"*1\r\n$7\r\nDISCARD\r\n",
            b"-ERR DISCARD without MULTI\r\n",
        )
        .await;
        send_and_expect(
            &mut client,
            b"*1\r\n$5\r\nWATCH\r\n",
            b"-ERR wrong number of arguments for 'WATCH' command\r\n",
        )
        .await;
        send_and_expect(
            &mut client,
            b"*2\r\n$7\r\nUNWATCH\r\n$1\r\nx\r\n",
            b"-ERR wrong number of arguments for 'UNWATCH' command\r\n",
        )
        .await;
        send_and_expect(
            &mut client,
            b"*2\r\n$6\r\nASKING\r\n$1\r\nx\r\n",
            b"-ERR wrong number of arguments for 'ASKING' command\r\n",
        )
        .await;
        send_and_expect(&mut client, b"*1\r\n$6\r\nASKING\r\n", b"+OK\r\n").await;
        send_and_expect(&mut client, b"*1\r\n$5\r\nMULTI\r\n", b"+OK\r\n").await;
        send_and_expect(
            &mut client,
            b"*3\r\n$3\r\nSET\r\n$5\r\ntxkey\r\n$1\r\n1\r\n",
            b"+QUEUED\r\n",
        )
        .await;
        send_and_expect(
            &mut client,
            b"*2\r\n$4\r\nINCR\r\n$5\r\ntxkey\r\n",
            b"+QUEUED\r\n",
        )
        .await;
        send_and_expect(&mut client, b"*1\r\n$4\r\nEXEC\r\n", b"*2\r\n+OK\r\n:2\r\n").await;
        send_and_expect(
            &mut client,
            b"*2\r\n$3\r\nGET\r\n$5\r\ntxkey\r\n",
            b"$1\r\n2\r\n",
        )
        .await;
        send_and_expect(&mut client, b"*1\r\n$5\r\nMULTI\r\n", b"+OK\r\n").await;
        send_and_expect(
            &mut client,
            b"*3\r\n$3\r\nSET\r\n$8\r\ndiscardk\r\n$1\r\nx\r\n",
            b"+QUEUED\r\n",
        )
        .await;
        send_and_expect(&mut client, b"*1\r\n$7\r\nDISCARD\r\n", b"+OK\r\n").await;
        send_and_expect(
            &mut client,
            b"*2\r\n$3\r\nGET\r\n$8\r\ndiscardk\r\n",
            b"$-1\r\n",
        )
        .await;
        let mut peer = TcpStream::connect(addr).await.unwrap();
        send_and_expect(
            &mut client,
            b"*2\r\n$5\r\nWATCH\r\n$7\r\ntxwatch\r\n",
            b"+OK\r\n",
        )
        .await;
        send_and_expect(&mut client, b"*1\r\n$5\r\nMULTI\r\n", b"+OK\r\n").await;
        send_and_expect(
            &mut client,
            b"*3\r\n$3\r\nSET\r\n$7\r\ntxwatch\r\n$5\r\ninner\r\n",
            b"+QUEUED\r\n",
        )
        .await;
        send_and_expect(
            &mut peer,
            b"*3\r\n$3\r\nSET\r\n$7\r\ntxwatch\r\n$5\r\nouter\r\n",
            b"+OK\r\n",
        )
        .await;
        send_and_expect(&mut client, b"*1\r\n$4\r\nEXEC\r\n", b"*-1\r\n").await;
        send_and_expect(
            &mut client,
            b"*2\r\n$3\r\nGET\r\n$7\r\ntxwatch\r\n",
            b"$5\r\nouter\r\n",
        )
        .await;
        send_and_expect(
            &mut client,
            b"*2\r\n$5\r\nWATCH\r\n$7\r\ntxwatch\r\n",
            b"+OK\r\n",
        )
        .await;
        send_and_expect(&mut client, b"*1\r\n$7\r\nUNWATCH\r\n", b"+OK\r\n").await;
        send_and_expect(&mut client, b"*1\r\n$5\r\nMULTI\r\n", b"+OK\r\n").await;
        send_and_expect(
            &mut client,
            b"*3\r\n$3\r\nSET\r\n$7\r\ntxwatch\r\n$5\r\ninner\r\n",
            b"+QUEUED\r\n",
        )
        .await;
        send_and_expect(
            &mut peer,
            b"*3\r\n$3\r\nSET\r\n$7\r\ntxwatch\r\n$6\r\nouter2\r\n",
            b"+OK\r\n",
        )
        .await;
        send_and_expect(&mut client, b"*1\r\n$4\r\nEXEC\r\n", b"*1\r\n+OK\r\n").await;
        send_and_expect(
            &mut client,
            b"*2\r\n$3\r\nGET\r\n$7\r\ntxwatch\r\n",
            b"$5\r\ninner\r\n",
        )
        .await;
        send_and_expect(&mut client, b"*1\r\n$5\r\nMULTI\r\n", b"+OK\r\n").await;
        send_and_expect(
            &mut client,
            b"*2\r\n$5\r\nWATCH\r\n$7\r\ntxwatch\r\n",
            b"-ERR WATCH inside MULTI is not allowed\r\n",
        )
        .await;
        send_and_expect(&mut client, b"*1\r\n$7\r\nDISCARD\r\n", b"+OK\r\n").await;
        send_and_expect(
            &mut client,
            b"*2\r\n$3\r\nDEL\r\n$5\r\ntxkey\r\n",
            b":1\r\n",
        )
        .await;
        send_and_expect(
            &mut client,
            b"*2\r\n$3\r\nDEL\r\n$7\r\ntxwatch\r\n",
            b":1\r\n",
        )
        .await;
        send_and_expect(&mut client, b"*2\r\n$3\r\nDEL\r\n$3\r\nkey\r\n", b":1\r\n").await;
        send_and_expect(&mut client, b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n", b"$-1\r\n").await;
        send_and_expect(&mut client, b"*1\r\n$6\r\nDBSIZE\r\n", b":1\r\n").await;
        send_and_expect(
            &mut client,
            b"*1\r\n$7\r\nCOMMAND\r\n",
            b"*38\r\n$3\r\nGET\r\n$3\r\nSET\r\n$3\r\nDEL\r\n$4\r\nINCR\r\n$4\r\nDECR\r\n$6\r\nEXPIRE\r\n$3\r\nTTL\r\n$7\r\nPEXPIRE\r\n$4\r\nPTTL\r\n$7\r\nPERSIST\r\n$4\r\nHSET\r\n$4\r\nHGET\r\n$4\r\nHDEL\r\n$7\r\nHGETALL\r\n$5\r\nLPUSH\r\n$5\r\nRPUSH\r\n$4\r\nLPOP\r\n$4\r\nRPOP\r\n$6\r\nLRANGE\r\n$4\r\nSADD\r\n$4\r\nSREM\r\n$8\r\nSMEMBERS\r\n$9\r\nSISMEMBER\r\n$4\r\nZADD\r\n$4\r\nZREM\r\n$6\r\nZRANGE\r\n$6\r\nZSCORE\r\n$5\r\nMULTI\r\n$4\r\nEXEC\r\n$7\r\nDISCARD\r\n$5\r\nWATCH\r\n$7\r\nUNWATCH\r\n$6\r\nASKING\r\n$4\r\nPING\r\n$4\r\nECHO\r\n$4\r\nINFO\r\n$6\r\nDBSIZE\r\n$7\r\nCOMMAND\r\n",
        )
        .await;

        let _ = shutdown_tx.send(());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn cluster_routing_returns_moved_for_remote_slots() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let metrics = Arc::new(ServerMetrics::default());
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        let mut cluster_config = ClusterConfig::new_local("local", "127.0.0.1", 6379);
        let remote_worker = Worker::new("remote", "10.0.0.2", 6380, WorkerRole::Primary);
        let (with_remote, remote_id) = cluster_config.add_worker(remote_worker).unwrap();
        cluster_config = with_remote;

        let local_key = b"local-k";
        let local_slot = redis_hash_slot(local_key);
        cluster_config = cluster_config
            .set_slot_state(local_slot, LOCAL_WORKER_ID, SlotState::Stable)
            .unwrap();

        let remote_key = b"remote-k";
        let remote_slot = redis_hash_slot(remote_key);
        cluster_config = cluster_config
            .set_slot_state(remote_slot, remote_id, SlotState::Stable)
            .unwrap();

        let ask_key = b"ask-k";
        let ask_slot = redis_hash_slot(ask_key);
        cluster_config = cluster_config
            .set_slot_state(ask_slot, remote_id, SlotState::Importing)
            .unwrap();

        let tx_key_a = b"tx-slot-a";
        let tx_slot_a = redis_hash_slot(tx_key_a);
        let mut tx_key_b = b"tx-slot-b".to_vec();
        let mut tx_slot_b = redis_hash_slot(&tx_key_b);
        while tx_slot_b == tx_slot_a {
            tx_key_b.push(b'x');
            tx_slot_b = redis_hash_slot(&tx_key_b);
        }
        cluster_config = cluster_config
            .set_slot_state(tx_slot_a, LOCAL_WORKER_ID, SlotState::Stable)
            .unwrap();
        cluster_config = cluster_config
            .set_slot_state(tx_slot_b, LOCAL_WORKER_ID, SlotState::Stable)
            .unwrap();

        let mut unbound_key = b"unbound-k".to_vec();
        let mut unbound_slot = redis_hash_slot(&unbound_key);
        while [local_slot, remote_slot, ask_slot, tx_slot_a, tx_slot_b].contains(&unbound_slot) {
            unbound_key.push(b'x');
            unbound_slot = redis_hash_slot(&unbound_key);
        }
        let cluster_store = Arc::new(ClusterConfigStore::new(cluster_config));

        let server_metrics = Arc::clone(&metrics);
        let server_cluster = Arc::clone(&cluster_store);
        let server = tokio::spawn(async move {
            run_listener_with_shutdown_and_cluster(
                listener,
                1024,
                server_metrics,
                async move {
                    let _ = shutdown_rx.await;
                },
                Some(server_cluster),
            )
            .await
            .unwrap();
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        send_and_expect(
            &mut client,
            b"*3\r\n$3\r\nSET\r\n$7\r\nlocal-k\r\n$2\r\nok\r\n",
            b"+OK\r\n",
        )
        .await;
        send_and_expect(
            &mut client,
            b"*2\r\n$3\r\nGET\r\n$7\r\nlocal-k\r\n",
            b"$2\r\nok\r\n",
        )
        .await;

        let expected_moved = format!("-MOVED {} 10.0.0.2:6380\r\n", remote_slot);
        send_and_expect(
            &mut client,
            b"*2\r\n$3\r\nGET\r\n$8\r\nremote-k\r\n",
            expected_moved.as_bytes(),
        )
        .await;
        send_and_expect(
            &mut client,
            b"*3\r\n$3\r\nDEL\r\n$7\r\nlocal-k\r\n$8\r\nremote-k\r\n",
            b"-CROSSSLOT Keys in request don't hash to the same slot\r\n",
        )
        .await;
        let expected_ask = format!("-ASK {} 10.0.0.2:6380\r\n", ask_slot);
        send_and_expect(
            &mut client,
            b"*2\r\n$3\r\nGET\r\n$5\r\nask-k\r\n",
            expected_ask.as_bytes(),
        )
        .await;
        send_and_expect(
            &mut client,
            b"*3\r\n$5\r\nWATCH\r\n$7\r\nlocal-k\r\n$8\r\nremote-k\r\n",
            b"-CROSSSLOT Keys in request don't hash to the same slot\r\n",
        )
        .await;
        send_and_expect(&mut client, b"*1\r\n$6\r\nASKING\r\n", b"+OK\r\n").await;
        send_and_expect(&mut client, b"*1\r\n$4\r\nPING\r\n", b"+PONG\r\n").await;
        send_and_expect(
            &mut client,
            b"*2\r\n$3\r\nGET\r\n$5\r\nask-k\r\n",
            expected_ask.as_bytes(),
        )
        .await;
        send_and_expect(&mut client, b"*1\r\n$6\r\nASKING\r\n", b"+OK\r\n").await;
        send_and_expect(
            &mut client,
            b"*2\r\n$3\r\nGET\r\n$5\r\nask-k\r\n",
            b"$-1\r\n",
        )
        .await;
        send_and_expect(
            &mut client,
            b"*2\r\n$3\r\nGET\r\n$5\r\nask-k\r\n",
            expected_ask.as_bytes(),
        )
        .await;
        send_and_expect(&mut client, b"*1\r\n$5\r\nMULTI\r\n", b"+OK\r\n").await;
        send_and_expect(
            &mut client,
            b"*3\r\n$3\r\nSET\r\n$9\r\ntx-slot-a\r\n$1\r\n1\r\n",
            b"+QUEUED\r\n",
        )
        .await;
        let tx_b_len = tx_key_b.len();
        let tx_b_req = format!(
            "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n$1\r\n2\r\n",
            tx_b_len,
            String::from_utf8(tx_key_b.clone()).unwrap()
        );
        send_and_expect(
            &mut client,
            tx_b_req.as_bytes(),
            b"-CROSSSLOT Keys in request don't hash to the same slot\r\n",
        )
        .await;
        send_and_expect(
            &mut client,
            b"*1\r\n$4\r\nEXEC\r\n",
            b"-EXECABORT Transaction discarded because of previous errors.\r\n",
        )
        .await;
        send_and_expect(
            &mut client,
            b"*2\r\n$3\r\nGET\r\n$9\r\ntx-slot-a\r\n",
            b"$-1\r\n",
        )
        .await;
        let unbound_req = format!(
            "*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n",
            unbound_key.len(),
            String::from_utf8(unbound_key.clone()).unwrap()
        );
        let expected_clusterdown =
            format!("-CLUSTERDOWN Hash slot {} is unbound\r\n", unbound_slot);
        send_and_expect(
            &mut client,
            unbound_req.as_bytes(),
            expected_clusterdown.as_bytes(),
        )
        .await;

        let _ = shutdown_tx.send(());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn cluster_multi_node_slot_routing_and_failover_updates_redirections() {
        let listener1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr1 = listener1.local_addr().unwrap();
        let listener2 = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr2 = listener2.local_addr().unwrap();
        let listener3 = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr3 = listener3.local_addr().unwrap();

        let key1 = b"node1-key".to_vec();
        let slot1 = redis_hash_slot(&key1);
        let mut key2 = b"node2-key".to_vec();
        let mut slot2 = redis_hash_slot(&key2);
        while slot2 == slot1 {
            key2.push(b'x');
            slot2 = redis_hash_slot(&key2);
        }
        let mut key3 = b"node3-key".to_vec();
        let mut slot3 = redis_hash_slot(&key3);
        while slot3 == slot1 || slot3 == slot2 {
            key3.push(b'x');
            slot3 = redis_hash_slot(&key3);
        }

        let mut config1 = ClusterConfig::new_local("node-1", "127.0.0.1", addr1.port());
        let (next1, node2_id_in_1) = config1
            .add_worker(Worker::new(
                "node-2",
                "127.0.0.1",
                addr2.port(),
                WorkerRole::Primary,
            ))
            .unwrap();
        config1 = next1;
        let (next1, node3_id_in_1) = config1
            .add_worker(Worker::new(
                "node-3",
                "127.0.0.1",
                addr3.port(),
                WorkerRole::Primary,
            ))
            .unwrap();
        config1 = next1;
        config1 = config1
            .set_slot_state(slot1, LOCAL_WORKER_ID, SlotState::Stable)
            .unwrap();
        config1 = config1
            .set_slot_state(slot2, node2_id_in_1, SlotState::Stable)
            .unwrap();
        config1 = config1
            .set_slot_state(slot3, node3_id_in_1, SlotState::Stable)
            .unwrap();

        let mut config2 = ClusterConfig::new_local("node-2", "127.0.0.1", addr2.port());
        let (next2, node1_id_in_2) = config2
            .add_worker(Worker::new(
                "node-1",
                "127.0.0.1",
                addr1.port(),
                WorkerRole::Primary,
            ))
            .unwrap();
        config2 = next2;
        let (next2, node3_id_in_2) = config2
            .add_worker(Worker::new(
                "node-3",
                "127.0.0.1",
                addr3.port(),
                WorkerRole::Primary,
            ))
            .unwrap();
        config2 = next2;
        config2 = config2
            .set_slot_state(slot1, node1_id_in_2, SlotState::Stable)
            .unwrap();
        config2 = config2
            .set_slot_state(slot2, LOCAL_WORKER_ID, SlotState::Stable)
            .unwrap();
        config2 = config2
            .set_slot_state(slot3, node3_id_in_2, SlotState::Stable)
            .unwrap();

        let mut config3 = ClusterConfig::new_local("node-3", "127.0.0.1", addr3.port());
        let (next3, node1_id_in_3) = config3
            .add_worker(Worker::new(
                "node-1",
                "127.0.0.1",
                addr1.port(),
                WorkerRole::Primary,
            ))
            .unwrap();
        config3 = next3;
        let (next3, node2_id_in_3) = config3
            .add_worker(Worker::new(
                "node-2",
                "127.0.0.1",
                addr2.port(),
                WorkerRole::Primary,
            ))
            .unwrap();
        config3 = next3;
        config3 = config3
            .set_slot_state(slot1, node1_id_in_3, SlotState::Stable)
            .unwrap();
        config3 = config3
            .set_slot_state(slot2, node2_id_in_3, SlotState::Stable)
            .unwrap();
        config3 = config3
            .set_slot_state(slot3, LOCAL_WORKER_ID, SlotState::Stable)
            .unwrap();

        let store1 = Arc::new(ClusterConfigStore::new(config1));
        let store2 = Arc::new(ClusterConfigStore::new(config2));
        let store3 = Arc::new(ClusterConfigStore::new(config3));

        let (shutdown1_tx, shutdown1_rx) = oneshot::channel::<()>();
        let (shutdown2_tx, shutdown2_rx) = oneshot::channel::<()>();
        let (shutdown3_tx, shutdown3_rx) = oneshot::channel::<()>();

        let server1_metrics = Arc::new(ServerMetrics::default());
        let server1_cluster = Arc::clone(&store1);
        let server1 = tokio::spawn(async move {
            run_listener_with_shutdown_and_cluster(
                listener1,
                1024,
                server1_metrics,
                async move {
                    let _ = shutdown1_rx.await;
                },
                Some(server1_cluster),
            )
            .await
            .unwrap();
        });

        let server2_metrics = Arc::new(ServerMetrics::default());
        let server2_cluster = Arc::clone(&store2);
        let server2 = tokio::spawn(async move {
            run_listener_with_shutdown_and_cluster(
                listener2,
                1024,
                server2_metrics,
                async move {
                    let _ = shutdown2_rx.await;
                },
                Some(server2_cluster),
            )
            .await
            .unwrap();
        });

        let server3_metrics = Arc::new(ServerMetrics::default());
        let server3_cluster = Arc::clone(&store3);
        let server3 = tokio::spawn(async move {
            run_listener_with_shutdown_and_cluster(
                listener3,
                1024,
                server3_metrics,
                async move {
                    let _ = shutdown3_rx.await;
                },
                Some(server3_cluster),
            )
            .await
            .unwrap();
        });

        let mut node1 = TcpStream::connect(addr1).await.unwrap();
        let mut node2 = TcpStream::connect(addr2).await.unwrap();
        let mut node3 = TcpStream::connect(addr3).await.unwrap();

        let set_key1 = encode_resp_command(&[b"SET", &key1, b"v1"]);
        let get_key1 = encode_resp_command(&[b"GET", &key1]);
        let set_key2 = encode_resp_command(&[b"SET", &key2, b"v2"]);
        let get_key2 = encode_resp_command(&[b"GET", &key2]);
        let set_key3 = encode_resp_command(&[b"SET", &key3, b"v3"]);
        let get_key3 = encode_resp_command(&[b"GET", &key3]);

        send_and_expect(&mut node1, &set_key1, b"+OK\r\n").await;
        send_and_expect(&mut node1, &get_key1, b"$2\r\nv1\r\n").await;
        send_and_expect(&mut node2, &set_key2, b"+OK\r\n").await;
        send_and_expect(&mut node2, &get_key2, b"$2\r\nv2\r\n").await;
        send_and_expect(&mut node3, &set_key3, b"+OK\r\n").await;
        send_and_expect(&mut node3, &get_key3, b"$2\r\nv3\r\n").await;

        let moved_to_node1 = format!("-MOVED {} 127.0.0.1:{}\r\n", slot1, addr1.port());
        let moved_to_node2 = format!("-MOVED {} 127.0.0.1:{}\r\n", slot2, addr2.port());
        let moved_to_node3 = format!("-MOVED {} 127.0.0.1:{}\r\n", slot3, addr3.port());

        send_and_expect(&mut node1, &get_key2, moved_to_node2.as_bytes()).await;
        send_and_expect(&mut node1, &get_key3, moved_to_node3.as_bytes()).await;
        send_and_expect(&mut node2, &get_key1, moved_to_node1.as_bytes()).await;
        send_and_expect(&mut node3, &get_key1, moved_to_node1.as_bytes()).await;

        let mut replication1 = ReplicationManager::new(Some(7), 1_000, 2_000).unwrap();
        let mut replication2 = ReplicationManager::new(Some(7), 1_000, 2_000).unwrap();
        let mut replication3 = ReplicationManager::new(Some(7), 1_000, 2_000).unwrap();
        replication1.record_replica_offset(node3_id_in_1, 1_950);
        replication2.record_replica_offset(node3_id_in_2, 1_950);
        replication3.record_replica_offset(LOCAL_WORKER_ID, 1_950);
        let mut coordinator1 = FailoverCoordinator::new();
        let mut coordinator2 = FailoverCoordinator::new();
        let mut coordinator3 = FailoverCoordinator::new();

        let failover_input1 = store1
            .load()
            .as_ref()
            .clone()
            .set_worker_replica_of(node3_id_in_1, "node-2")
            .unwrap();
        store1.publish(failover_input1);
        let plan1 = coordinator1
            .execute_for_failed_primary(&store1, &replication1, "node-2")
            .unwrap()
            .expect("node-1 config should elect node-3");
        assert_eq!(plan1.promoted_worker_id, node3_id_in_1);

        let failover_input2 = store2
            .load()
            .as_ref()
            .clone()
            .set_worker_replica_of(node3_id_in_2, "node-2")
            .unwrap();
        store2.publish(failover_input2);
        let plan2 = coordinator2
            .execute_for_failed_primary(&store2, &replication2, "node-2")
            .unwrap()
            .expect("node-2 config should elect node-3");
        assert_eq!(plan2.promoted_worker_id, node3_id_in_2);

        let failover_input3 = store3
            .load()
            .as_ref()
            .clone()
            .set_worker_replica_of(LOCAL_WORKER_ID, "node-2")
            .unwrap();
        store3.publish(failover_input3);
        let plan3 = coordinator3
            .execute_for_failed_primary(&store3, &replication3, "node-2")
            .unwrap()
            .expect("node-3 config should elect local node");
        assert_eq!(plan3.failed_primary_worker_id, node2_id_in_3);
        assert_eq!(plan3.promoted_worker_id, LOCAL_WORKER_ID);

        let moved_to_node3_after_failover =
            format!("-MOVED {} 127.0.0.1:{}\r\n", slot2, addr3.port());
        send_and_expect(
            &mut node1,
            &get_key2,
            moved_to_node3_after_failover.as_bytes(),
        )
        .await;
        send_and_expect(
            &mut node2,
            &get_key2,
            moved_to_node3_after_failover.as_bytes(),
        )
        .await;
        send_and_expect(&mut node3, &get_key2, b"$-1\r\n").await;
        let set_key2_failover = encode_resp_command(&[b"SET", &key2, b"v2f"]);
        send_and_expect(&mut node3, &set_key2_failover, b"+OK\r\n").await;
        send_and_expect(&mut node3, &get_key2, b"$3\r\nv2f\r\n").await;

        let _ = shutdown1_tx.send(());
        let _ = shutdown2_tx.send(());
        let _ = shutdown3_tx.send(());
        server1.await.unwrap();
        server2.await.unwrap();
        server3.await.unwrap();
    }

    #[tokio::test]
    async fn cluster_manager_failover_loop_updates_server_redirections() {
        let listener1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr1 = listener1.local_addr().unwrap();
        let listener3 = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr3 = listener3.local_addr().unwrap();
        let listener2_for_port = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr2 = listener2_for_port.local_addr().unwrap();

        let key2 = b"node2-key".to_vec();
        let slot2 = redis_hash_slot(&key2);

        let mut config1 = ClusterConfig::new_local("node-1", "127.0.0.1", addr1.port());
        let (next1, node2_id_in_1) = config1
            .add_worker(Worker::new(
                "node-2",
                "127.0.0.1",
                addr2.port(),
                WorkerRole::Primary,
            ))
            .unwrap();
        config1 = next1;
        let (next1, node3_id_in_1) = config1
            .add_worker(Worker::new(
                "node-3",
                "127.0.0.1",
                addr3.port(),
                WorkerRole::Replica,
            ))
            .unwrap();
        config1 = next1;
        config1 = config1
            .set_worker_replica_of(node3_id_in_1, "node-2")
            .unwrap()
            .set_slot_state(slot2, node2_id_in_1, SlotState::Stable)
            .unwrap();

        let mut config3 = ClusterConfig::new_local("node-3", "127.0.0.1", addr3.port())
            .set_local_worker_role(WorkerRole::Replica)
            .unwrap()
            .set_worker_replica_of(LOCAL_WORKER_ID, "node-2")
            .unwrap();
        let (next3, node1_id_in_3) = config3
            .add_worker(Worker::new(
                "node-1",
                "127.0.0.1",
                addr1.port(),
                WorkerRole::Primary,
            ))
            .unwrap();
        config3 = next3;
        let (next3, node2_id_in_3) = config3
            .add_worker(Worker::new(
                "node-2",
                "127.0.0.1",
                addr2.port(),
                WorkerRole::Primary,
            ))
            .unwrap();
        config3 = next3;
        config3 = config3
            .set_slot_state(slot2, node2_id_in_3, SlotState::Stable)
            .unwrap()
            .set_slot_state(
                redis_hash_slot(b"node1-anchor"),
                node1_id_in_3,
                SlotState::Stable,
            )
            .unwrap();

        let store1 = Arc::new(ClusterConfigStore::new(config1));
        let store3 = Arc::new(ClusterConfigStore::new(config3));

        let (shutdown1_tx, shutdown1_rx) = oneshot::channel::<()>();
        let (shutdown3_tx, shutdown3_rx) = oneshot::channel::<()>();

        let server1_metrics = Arc::new(ServerMetrics::default());
        let server1_cluster = Arc::clone(&store1);
        let server1 = tokio::spawn(async move {
            run_listener_with_shutdown_and_cluster(
                listener1,
                1024,
                server1_metrics,
                async move {
                    let _ = shutdown1_rx.await;
                },
                Some(server1_cluster),
            )
            .await
            .unwrap();
        });

        let server3_metrics = Arc::new(ServerMetrics::default());
        let server3_cluster = Arc::clone(&store3);
        let server3 = tokio::spawn(async move {
            run_listener_with_shutdown_and_cluster(
                listener3,
                1024,
                server3_metrics,
                async move {
                    let _ = shutdown3_rx.await;
                },
                Some(server3_cluster),
            )
            .await
            .unwrap();
        });

        let mut node1 = TcpStream::connect(addr1).await.unwrap();
        let mut node3 = TcpStream::connect(addr3).await.unwrap();
        let get_key2 = encode_resp_command(&[b"GET", &key2]);
        let moved_to_node2 = format!("-MOVED {} 127.0.0.1:{}\r\n", slot2, addr2.port());
        send_and_expect(&mut node1, &get_key2, moved_to_node2.as_bytes()).await;
        send_and_expect(&mut node3, &get_key2, moved_to_node2.as_bytes()).await;

        let gossip_nodes1 = vec![GossipNode::new(node2_id_in_1, 0)];
        let gossip_nodes3 = vec![GossipNode::new(node2_id_in_3, 0)];
        let gossip_engine1 = AsyncGossipEngine::new(
            GossipCoordinator::new(gossip_nodes1, 1),
            InMemoryGossipTransport::new(Arc::clone(&store1)),
            100,
            0,
        );
        let gossip_engine3 = AsyncGossipEngine::new(
            GossipCoordinator::new(gossip_nodes3, 1),
            InMemoryGossipTransport::new(Arc::clone(&store3)),
            100,
            0,
        );
        let mut manager1 = ClusterManager::new(gossip_engine1, Duration::from_millis(5));
        let mut manager3 = ClusterManager::new(gossip_engine3, Duration::from_millis(5));

        let mut detector1 = FailureDetector::new(1);
        let mut detector3 = FailureDetector::new(1);
        let mut controller1 = ClusterFailoverController::new();
        let mut controller3 = ClusterFailoverController::new();
        let mut replication1 = ReplicationManager::new(Some(7), 2_000, 2_200).unwrap();
        let mut replication3 = ReplicationManager::new(Some(7), 2_000, 2_200).unwrap();
        replication1.record_replica_offset(node3_id_in_1, 1_950);
        replication3.record_replica_offset(LOCAL_WORKER_ID, 1_950);

        let (repl1_tx, mut repl1_rx) = tokio::sync::mpsc::unbounded_channel::<ReplicationEvent>();
        let (repl3_tx, mut repl3_rx) = tokio::sync::mpsc::unbounded_channel::<ReplicationEvent>();
        let mut repl_transport1 = ChannelReplicationTransport::new(repl1_tx, 1_980);
        let mut repl_transport3 = ChannelReplicationTransport::new(repl3_tx, 1_980);
        let (_updates1_tx, updates1_rx) = tokio::sync::mpsc::unbounded_channel::<ClusterConfig>();
        let (_updates3_tx, updates3_rx) = tokio::sync::mpsc::unbounded_channel::<ClusterConfig>();

        let reports1 = manager1
            .run_with_config_updates_and_failover(
                &store1,
                updates1_rx,
                &mut detector1,
                &mut controller1,
                &mut replication1,
                &mut repl_transport1,
                tokio::time::sleep(Duration::from_millis(20)),
            )
            .await
            .unwrap();
        let reports3 = manager3
            .run_with_config_updates_and_failover(
                &store3,
                updates3_rx,
                &mut detector3,
                &mut controller3,
                &mut replication3,
                &mut repl_transport3,
                tokio::time::sleep(Duration::from_millis(20)),
            )
            .await
            .unwrap();
        assert!(reports1
            .iter()
            .any(|report| report.failed_worker_ids.contains(&node2_id_in_1)));
        assert!(reports3
            .iter()
            .any(|report| report.failed_worker_ids.contains(&node2_id_in_3)));
        assert_eq!(
            repl1_rx.recv().await,
            Some(ReplicationEvent::Checkpoint {
                worker_id: node3_id_in_1,
                checkpoint_id: 7,
            })
        );
        assert_eq!(
            repl3_rx.recv().await,
            Some(ReplicationEvent::Checkpoint {
                worker_id: LOCAL_WORKER_ID,
                checkpoint_id: 7,
            })
        );

        let moved_to_node3 = format!("-MOVED {} 127.0.0.1:{}\r\n", slot2, addr3.port());
        send_and_expect(&mut node1, &get_key2, moved_to_node3.as_bytes()).await;
        send_and_expect(&mut node3, &get_key2, b"$-1\r\n").await;
        let set_key2 = encode_resp_command(&[b"SET", &key2, b"v2-after-failover"]);
        send_and_expect(&mut node3, &set_key2, b"+OK\r\n").await;
        send_and_expect(&mut node3, &get_key2, b"$17\r\nv2-after-failover\r\n").await;

        let _ = shutdown1_tx.send(());
        let _ = shutdown3_tx.send(());
        server1.await.unwrap();
        server3.await.unwrap();
    }

    #[tokio::test]
    async fn cluster_live_slot_migration_transfers_data_and_updates_redirections() {
        let listener1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr1 = listener1.local_addr().unwrap();
        let listener2 = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr2 = listener2.local_addr().unwrap();

        let key = b"live-migrate-key".to_vec();
        let slot = redis_hash_slot(&key);

        let mut config1 = ClusterConfig::new_local("node-1", "127.0.0.1", addr1.port());
        let (next1, node2_id_in_1) = config1
            .add_worker(Worker::new(
                "node-2",
                "127.0.0.1",
                addr2.port(),
                WorkerRole::Primary,
            ))
            .unwrap();
        config1 = next1
            .set_slot_state(slot, LOCAL_WORKER_ID, SlotState::Stable)
            .unwrap();

        let mut config2 = ClusterConfig::new_local("node-2", "127.0.0.1", addr2.port());
        let (next2, node1_id_in_2) = config2
            .add_worker(Worker::new(
                "node-1",
                "127.0.0.1",
                addr1.port(),
                WorkerRole::Primary,
            ))
            .unwrap();
        config2 = next2
            .set_slot_state(slot, node1_id_in_2, SlotState::Stable)
            .unwrap();

        let store1 = Arc::new(ClusterConfigStore::new(config1));
        let store2 = Arc::new(ClusterConfigStore::new(config2));
        let source_processor = Arc::new(RequestProcessor::new().unwrap());
        let target_processor = Arc::new(RequestProcessor::new().unwrap());

        let (shutdown1_tx, shutdown1_rx) = oneshot::channel::<()>();
        let (shutdown2_tx, shutdown2_rx) = oneshot::channel::<()>();

        let server1_metrics = Arc::new(ServerMetrics::default());
        let server1_cluster = Arc::clone(&store1);
        let server1_processor = Arc::clone(&source_processor);
        let server1 = tokio::spawn(async move {
            run_listener_with_shutdown_and_cluster_with_processor(
                listener1,
                1024,
                server1_metrics,
                async move {
                    let _ = shutdown1_rx.await;
                },
                Some(server1_cluster),
                server1_processor,
            )
            .await
            .unwrap();
        });

        let server2_metrics = Arc::new(ServerMetrics::default());
        let server2_cluster = Arc::clone(&store2);
        let server2_processor = Arc::clone(&target_processor);
        let server2 = tokio::spawn(async move {
            run_listener_with_shutdown_and_cluster_with_processor(
                listener2,
                1024,
                server2_metrics,
                async move {
                    let _ = shutdown2_rx.await;
                },
                Some(server2_cluster),
                server2_processor,
            )
            .await
            .unwrap();
        });

        let mut node1 = TcpStream::connect(addr1).await.unwrap();
        let mut node2 = TcpStream::connect(addr2).await.unwrap();
        let set_key = encode_resp_command(&[b"SET", &key, b"value"]);
        let get_key = encode_resp_command(&[b"GET", &key]);

        send_and_expect(&mut node1, &set_key, b"+OK\r\n").await;
        send_and_expect(&mut node1, &get_key, b"$5\r\nvalue\r\n").await;

        let moved_to_node1 = format!("-MOVED {} 127.0.0.1:{}\r\n", slot, addr1.port());
        send_and_expect(&mut node2, &get_key, moved_to_node1.as_bytes()).await;

        let migration_source = store1
            .load()
            .as_ref()
            .clone()
            .set_slot_state(slot, node2_id_in_1, SlotState::Migrating)
            .unwrap();
        store1.publish(migration_source);
        let migration_target = store2
            .load()
            .as_ref()
            .clone()
            .set_slot_state(slot, node1_id_in_2, SlotState::Importing)
            .unwrap();
        store2.publish(migration_target);

        let ask_to_node2 = format!("-ASK {} 127.0.0.1:{}\r\n", slot, addr2.port());
        send_and_expect(&mut node1, &get_key, ask_to_node2.as_bytes()).await;
        let ask_to_node1 = format!("-ASK {} 127.0.0.1:{}\r\n", slot, addr1.port());
        send_and_expect(&mut node2, &get_key, ask_to_node1.as_bytes()).await;

        send_and_expect(&mut node2, b"*1\r\n$6\r\nASKING\r\n", b"+OK\r\n").await;
        send_and_expect(&mut node2, &get_key, b"$-1\r\n").await;

        let moved = source_processor
            .migrate_slot_to(&target_processor, slot, 16, true)
            .unwrap();
        assert_eq!(moved, 1);

        send_and_expect(&mut node2, b"*1\r\n$6\r\nASKING\r\n", b"+OK\r\n").await;
        send_and_expect(&mut node2, &get_key, b"$5\r\nvalue\r\n").await;

        let finalized_source = store1
            .load()
            .as_ref()
            .clone()
            .set_slot_state(slot, node2_id_in_1, SlotState::Stable)
            .unwrap();
        store1.publish(finalized_source);
        let finalized_target = store2
            .load()
            .as_ref()
            .clone()
            .set_slot_state(slot, LOCAL_WORKER_ID, SlotState::Stable)
            .unwrap();
        store2.publish(finalized_target);

        let moved_to_node2 = format!("-MOVED {} 127.0.0.1:{}\r\n", slot, addr2.port());
        send_and_expect(&mut node1, &get_key, moved_to_node2.as_bytes()).await;
        send_and_expect(&mut node2, &get_key, b"$5\r\nvalue\r\n").await;

        let _ = shutdown1_tx.send(());
        let _ = shutdown2_tx.send(());
        server1.await.unwrap();
        server2.await.unwrap();
    }

    async fn wait_until<P>(mut predicate: P, timeout: Duration)
    where
        P: FnMut() -> bool,
    {
        let deadline = Instant::now() + timeout;
        loop {
            if predicate() || Instant::now() >= deadline {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    }

    async fn send_and_expect(client: &mut TcpStream, request: &[u8], expected_response: &[u8]) {
        client.write_all(request).await.unwrap();

        let mut actual = vec![0u8; expected_response.len()];
        tokio::time::timeout(Duration::from_secs(1), client.read_exact(&mut actual))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(actual, expected_response);
    }

    fn encode_resp_command(parts: &[&[u8]]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
        for part in parts {
            out.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
            out.extend_from_slice(part);
            out.extend_from_slice(b"\r\n");
        }
        out
    }
}
