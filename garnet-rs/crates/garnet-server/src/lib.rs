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
pub use request_lifecycle::{RequestExecutionError, RequestProcessor, RequestProcessorInitError};

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

pub async fn run_with_shutdown<F>(
    config: ServerConfig,
    metrics: Arc<ServerMetrics>,
    shutdown: F,
) -> io::Result<()>
where
    F: Future<Output = ()> + Send,
{
    let listener = TcpListener::bind(config.bind_addr).await?;
    run_listener_with_shutdown(listener, config.read_buffer_size, metrics, shutdown).await
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
    let processor = Arc::new(RequestProcessor::new().map_err(|err| {
        io::Error::new(
            io::ErrorKind::Other,
            format!("request processor initialization failed: {err}"),
        )
    })?);
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
                tasks.spawn(async move {
                    let _ =
                        handle_connection(stream, read_buffer_size, task_metrics, task_processor)
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
) -> io::Result<()> {
    let _lifecycle = ConnectionLifecycle { metrics: &metrics };
    let mut read_buffer = vec![0u8; read_buffer_size.max(1)];
    let mut receive_buffer = Vec::with_capacity(read_buffer_size.max(1));
    let mut responses = Vec::with_capacity(read_buffer_size.max(1));
    let mut args = [ArgSlice::EMPTY; 64];
    let mut transaction = ConnectionTransactionState::default();

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
}

impl ConnectionTransactionState {
    fn reset(&mut self) {
        self.in_multi = false;
        self.queued_frames.clear();
        self.watched_keys.clear();
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
}

fn execute_transaction_queue(
    processor: &RequestProcessor,
    transaction: &mut ConnectionTransactionState,
    responses: &mut Vec<u8>,
) {
    let queued = std::mem::take(&mut transaction.queued_frames);
    transaction.in_multi = false;
    transaction.watched_keys.clear();

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
            b"*37\r\n$3\r\nGET\r\n$3\r\nSET\r\n$3\r\nDEL\r\n$4\r\nINCR\r\n$4\r\nDECR\r\n$6\r\nEXPIRE\r\n$3\r\nTTL\r\n$7\r\nPEXPIRE\r\n$4\r\nPTTL\r\n$7\r\nPERSIST\r\n$4\r\nHSET\r\n$4\r\nHGET\r\n$4\r\nHDEL\r\n$7\r\nHGETALL\r\n$5\r\nLPUSH\r\n$5\r\nRPUSH\r\n$4\r\nLPOP\r\n$4\r\nRPOP\r\n$6\r\nLRANGE\r\n$4\r\nSADD\r\n$4\r\nSREM\r\n$8\r\nSMEMBERS\r\n$9\r\nSISMEMBER\r\n$4\r\nZADD\r\n$4\r\nZREM\r\n$6\r\nZRANGE\r\n$6\r\nZSCORE\r\n$5\r\nMULTI\r\n$4\r\nEXEC\r\n$7\r\nDISCARD\r\n$5\r\nWATCH\r\n$7\r\nUNWATCH\r\n$4\r\nPING\r\n$4\r\nECHO\r\n$4\r\nINFO\r\n$6\r\nDBSIZE\r\n$7\r\nCOMMAND\r\n",
        )
        .await;

        let _ = shutdown_tx.send(());
        server.await.unwrap();
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
}
