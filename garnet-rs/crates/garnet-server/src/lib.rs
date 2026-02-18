//! TCP server accept loop and connection handler primitives.

pub mod command_dispatch;
pub mod limited_fixed_buffer_pool;
pub mod request_lifecycle;

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
                    if let Err(error) =
                        processor.execute(&args[..meta.argument_count], &mut responses)
                    {
                        error.append_resp_error(&mut responses);
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
        send_and_expect(&mut client, b"*2\r\n$3\r\nDEL\r\n$3\r\nkey\r\n", b":1\r\n").await;
        send_and_expect(&mut client, b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n", b"$-1\r\n").await;
        send_and_expect(&mut client, b"*1\r\n$6\r\nDBSIZE\r\n", b":1\r\n").await;
        send_and_expect(
            &mut client,
            b"*1\r\n$7\r\nCOMMAND\r\n",
            b"*14\r\n$3\r\nGET\r\n$3\r\nSET\r\n$3\r\nDEL\r\n$4\r\nINCR\r\n$4\r\nDECR\r\n$6\r\nEXPIRE\r\n$3\r\nTTL\r\n$7\r\nPEXPIRE\r\n$4\r\nPTTL\r\n$4\r\nPING\r\n$4\r\nECHO\r\n$4\r\nINFO\r\n$6\r\nDBSIZE\r\n$7\r\nCOMMAND\r\n",
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
