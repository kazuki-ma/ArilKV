use super::*;
use crate::RequestTimeSnapshot;
use crate::redis_replication::RedisReplicationCoordinator;
use crate::request_lifecycle::RespProtocolVersion;
use crate::server_runtime::apply_startup_config_overrides_to_processor;
use crate::server_runtime::isolate_default_persistence_dir_for_tests;
use garnet_cluster::AsyncGossipEngine;
use garnet_cluster::ChannelReplicationTransport;
use garnet_cluster::CheckpointId;
use garnet_cluster::ClusterConfig;
use garnet_cluster::ClusterConfigStore;
use garnet_cluster::ClusterFailoverController;
use garnet_cluster::ClusterManager;
use garnet_cluster::FailoverCoordinator;
use garnet_cluster::FailureDetector;
use garnet_cluster::GossipCoordinator;
use garnet_cluster::GossipNode;
use garnet_cluster::InMemoryGossipTransport;
use garnet_cluster::LOCAL_WORKER_ID;
use garnet_cluster::ReplicationEvent;
use garnet_cluster::ReplicationManager;
use garnet_cluster::ReplicationOffset;
use garnet_cluster::SlotNumber;
use garnet_cluster::SlotState;
use garnet_cluster::Worker;
use garnet_cluster::WorkerRole;
use garnet_cluster::redis_hash_slot;
use std::path::PathBuf;
use std::process::Command;
use std::process::Output;
use std::process::Stdio;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::process::Command as TokioCommand;
use tokio::sync::oneshot;
use tokio::time::Duration;
use tokio::time::Instant;
use tokio::time::sleep;

fn scripting_test_mutex() -> &'static tokio::sync::Mutex<()> {
    static LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

async fn lock_scripting_test_serial() -> tokio::sync::MutexGuard<'static, ()> {
    scripting_test_mutex().lock().await
}

fn redis_repo_root() -> PathBuf {
    std::env::var_os("REDIS_REPO_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/Users/kazuki-matsuda/dev/src/github.com/redis/redis"))
}

fn runnable_repo_redis_cli() -> Option<PathBuf> {
    let cli = redis_repo_root().join("src/redis-cli");
    if !cli.is_file() {
        return None;
    }
    let output = Command::new(&cli).arg("--version").output().ok()?;
    if !output.status.success() {
        return None;
    }
    Some(cli)
}

fn docker_available() -> bool {
    Command::new("docker")
        .arg("version")
        .output()
        .is_ok_and(|output| output.status.success())
}

fn system_redis_server_available() -> bool {
    Command::new("redis-server")
        .arg("--version")
        .output()
        .is_ok_and(|output| output.status.success())
}

fn system_redis_server_supports_hash_field_expiration() -> bool {
    let output = match Command::new("redis-server").arg("--version").output() {
        Ok(output) if output.status.success() => output,
        _ => return false,
    };
    let version_text = String::from_utf8_lossy(&output.stdout);
    version_text
        .split_whitespace()
        .find_map(|token| token.strip_prefix("v="))
        .and_then(|version| version.split('.').next())
        .and_then(|major| major.parse::<u64>().ok())
        .is_some_and(|major| major >= 8)
}

fn current_unix_time_millis_for_test() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

struct TestServerHandle {
    addr: std::net::SocketAddr,
    shutdown_tx: Option<oneshot::Sender<()>>,
    server: Option<tokio::task::JoinHandle<()>>,
}

impl TestServerHandle {
    fn reserve_local_test_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    fn spawn_with_config(config: ServerConfig) -> Self {
        let addr = config.bind_addr;
        let metrics = Arc::new(ServerMetrics::default());
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            run_with_shutdown(config, metrics, async move {
                let _ = shutdown_rx.await;
            })
            .await
            .unwrap();
        });
        Self {
            addr,
            shutdown_tx: Some(shutdown_tx),
            server: Some(server),
        }
    }

    async fn start_with_processor(processor: Arc<RequestProcessor>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let metrics = Arc::new(ServerMetrics::default());
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let server_processor = Arc::clone(&processor);
        let isolated_dir = isolate_default_persistence_dir_for_tests(&processor);

        let server_metrics = Arc::clone(&metrics);
        let server = tokio::spawn(async move {
            run_listener_with_shutdown_and_cluster_with_processor(
                listener,
                1024,
                server_metrics,
                async move {
                    let _ = shutdown_rx.await;
                },
                None,
                server_processor,
            )
            .await
            .unwrap();
            if let Some(dir) = isolated_dir {
                let _ = std::fs::remove_dir_all(dir);
            }
        });

        Self {
            addr,
            shutdown_tx: Some(shutdown_tx),
            server: Some(server),
        }
    }

    fn into_parts(
        mut self,
    ) -> (
        std::net::SocketAddr,
        oneshot::Sender<()>,
        tokio::task::JoinHandle<()>,
    ) {
        (
            self.addr,
            self.shutdown_tx
                .take()
                .expect("test server handle missing shutdown sender"),
            self.server
                .take()
                .expect("test server handle missing join handle"),
        )
    }

    async fn wait_ready(&self) {
        wait_for_server_ping(self.addr).await;
    }

    async fn connect(&self) -> TcpStream {
        self.wait_ready().await;
        TcpStream::connect(self.addr).await.unwrap()
    }

    async fn shutdown(mut self) {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }
        if let Some(server) = self.server.take() {
            server.await.unwrap();
        }
    }
}

struct DockerContainerGuard {
    name: String,
}

impl DockerContainerGuard {
    fn new(name: String) -> Self {
        Self { name }
    }
}

impl Drop for DockerContainerGuard {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "-f", self.name.as_str()])
            .output();
    }
}

struct LocalRedisServerGuard {
    child: Option<std::process::Child>,
}

impl LocalRedisServerGuard {
    fn spawn(port: u16, log_prefix: &str) -> Self {
        let log_dir = unique_test_temp_dir(log_prefix);
        let dir_arg = log_dir.to_string_lossy().into_owned();
        let log_path = log_dir.join("redis-server.log");
        let log_file = std::fs::File::create(&log_path).unwrap();
        let log_file_err = log_file.try_clone().unwrap();
        let child = Command::new("redis-server")
            .args([
                "--port",
                &port.to_string(),
                "--dir",
                dir_arg.as_str(),
                "--save",
                "",
                "--appendonly",
                "no",
            ])
            .stdout(Stdio::from(log_file))
            .stderr(Stdio::from(log_file_err))
            .spawn()
            .unwrap_or_else(|error| {
                panic!(
                    "failed to start local redis-server on port {port} (log: {})\n{error}",
                    log_path.display()
                )
            });
        Self { child: Some(child) }
    }
}

impl Drop for LocalRedisServerGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn redis_cli_hint_suite_path() -> Option<PathBuf> {
    let hint_suite = redis_repo_root().join("tests/assets/test_cli_hint_suite.txt");
    if hint_suite.is_file() {
        Some(hint_suite)
    } else {
        None
    }
}

async fn run_redis_cli_hint_suite(
    redis_cli: &std::path::Path,
    hint_suite: &std::path::Path,
    args: &[&str],
) -> Output {
    TokioCommand::new(redis_cli)
        .args(args)
        .arg("--test_hint_file")
        .arg(hint_suite)
        .output()
        .await
        .unwrap()
}

fn assert_redis_cli_hint_suite_success(output: &Output, context: &str) {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "{context} exited unsuccessfully\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stdout.contains("SUCCESS: 69/69 passed"),
        "{context} did not report full hint-suite success\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
}

async fn collect_process_output<R>(mut reader: R, sink: Arc<tokio::sync::Mutex<Vec<u8>>>)
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut chunk = [0u8; 1024];
    loop {
        match reader.read(&mut chunk).await {
            Ok(0) => return,
            Ok(bytes_read) => sink.lock().await.extend_from_slice(&chunk[..bytes_read]),
            Err(_) => return,
        }
    }
}

async fn wait_for_replica_info_line(
    client: &mut TcpStream,
    expected_connected_replicas: u64,
    timeout: Duration,
) -> Vec<u8> {
    let deadline = Instant::now() + timeout;
    loop {
        let payload = send_and_read_bulk_payload(
            client,
            &encode_resp_command(&[b"INFO", b"REPLICATION"]),
            Duration::from_secs(1),
        )
        .await;
        let text = String::from_utf8_lossy(&payload);
        if read_info_u64(&payload, "connected_slaves") == Some(expected_connected_replicas)
            && text.contains("slave0:")
            && text.contains("state=online")
        {
            return payload;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for connected_slaves={expected_connected_replicas}; last payload: {text}"
        );
        sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_stale_replica_mode(processor: &RequestProcessor, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        if processor.stale_replica_reads_denied() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for stale replica mode"
        );
        sleep(Duration::from_millis(25)).await;
    }
}

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
async fn config_port_reflects_listener_port_and_accepts_noop_set() {
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
    let port_text = addr.port().to_string();
    let get_expected = format!(
        "*2\r\n$4\r\nport\r\n${}\r\n{}\r\n",
        port_text.len(),
        port_text
    );
    send_and_expect(
        &mut client,
        b"*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$4\r\nport\r\n",
        get_expected.as_bytes(),
    )
    .await;

    let set_current = format!(
        "*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$4\r\nport\r\n${}\r\n{}\r\n",
        port_text.len(),
        port_text
    );
    send_and_expect(&mut client, set_current.as_bytes(), b"+OK\r\n").await;

    let other_port = if addr.port() == u16::MAX {
        u16::MAX - 1
    } else {
        addr.port() + 1
    };
    let other_port_text = other_port.to_string();
    let set_other = format!(
        "*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$4\r\nport\r\n${}\r\n{}\r\n",
        other_port_text.len(),
        other_port_text
    );
    send_and_expect(
        &mut client,
        set_other.as_bytes(),
        b"-ERR CONFIG SET failed (possibly related to argument 'port') - Unable to listen on this port\r\n",
    )
    .await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn protocol_ignores_empty_and_negative_multibulk_queries() {
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
    client.write_all(b"\r\n").await.unwrap();
    send_and_expect(&mut client, b"*1\r\n$4\r\nPING\r\n", b"+PONG\r\n").await;

    client.write_all(b"*-10\r\n").await.unwrap();
    send_and_expect(&mut client, b"*1\r\n$4\r\nPING\r\n", b"+PONG\r\n").await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn protocol_accepts_tcl_puts_extra_lf_between_resp_commands() {
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
    let mut pipeline = encode_resp_command(&[b"DEL", b"test-counter"]);
    for _ in 0..4 {
        pipeline.push(b'\n');
        pipeline.extend_from_slice(&encode_resp_command(&[b"INCR", b"test-counter"]));
    }
    pipeline.push(b'\n');
    pipeline.extend_from_slice(&encode_resp_command(&[b"GET", b"test-counter"]));

    client.write_all(&pipeline).await.unwrap();

    let del_reply = read_resp_line_with_timeout(&mut client, Duration::from_secs(1)).await;
    assert!(
        del_reply.starts_with(b":"),
        "expected integer DEL reply, got: {:?}",
        String::from_utf8_lossy(&del_reply)
    );
    for expected in 1..=4 {
        let incr_reply = read_resp_line_with_timeout(&mut client, Duration::from_secs(1)).await;
        assert_eq!(incr_reply, format!(":{expected}").into_bytes());
    }
    let counter = read_bulk_payload_with_timeout(&mut client, Duration::from_secs(1)).await;
    assert_eq!(counter, b"4");

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn protocol_returns_redis_style_resp_parse_errors() {
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

    {
        let mut client = TcpStream::connect(addr).await.unwrap();
        client.write_all(b"*3000000000\r\n").await.unwrap();
        let error = read_resp_line_with_timeout(&mut client, Duration::from_secs(1)).await;
        assert!(String::from_utf8_lossy(&error).contains("invalid multibulk length"));
    }

    {
        let mut client = TcpStream::connect(addr).await.unwrap();
        client
            .write_all(b"*3\r\n$3\r\nSET\r\n$1\r\nx\r\nfooz\r\n")
            .await
            .unwrap();
        let error = read_resp_line_with_timeout(&mut client, Duration::from_secs(1)).await;
        assert!(String::from_utf8_lossy(&error).contains("expected '$', got 'f'"));
    }

    {
        let mut client = TcpStream::connect(addr).await.unwrap();
        client
            .write_all(b"*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$-10\r\n")
            .await
            .unwrap();
        let error = read_resp_line_with_timeout(&mut client, Duration::from_secs(1)).await;
        assert!(String::from_utf8_lossy(&error).contains("invalid bulk length"));
    }

    {
        let mut client = TcpStream::connect(addr).await.unwrap();
        client
            .write_all(b"*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$2000000000\r\n")
            .await
            .unwrap();
        let error = read_resp_line_with_timeout(&mut client, Duration::from_secs(1)).await;
        assert!(String::from_utf8_lossy(&error).contains("invalid bulk length"));
    }

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn hello_protocol_version_is_connection_scoped() {
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

    let mut client_a = TcpStream::connect(addr).await.unwrap();
    let mut client_b = TcpStream::connect(addr).await.unwrap();
    let debug_protocol_true = b"*3\r\n$5\r\nDEBUG\r\n$8\r\nPROTOCOL\r\n$4\r\nTRUE\r\n";

    send_hello_and_drain(&mut client_a, b"3").await;
    send_and_expect(&mut client_a, debug_protocol_true, b"#t\r\n").await;

    send_and_expect(&mut client_b, debug_protocol_true, b":1\r\n").await;

    send_hello_and_drain(&mut client_a, b"2").await;
    send_and_expect(&mut client_a, debug_protocol_true, b":1\r\n").await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
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
    let expected_command_response =
        encode_resp_command(crate::command_spec::command_names_for_command_response());
    send_and_expect(
        &mut client,
        b"*1\r\n$7\r\nCOMMAND\r\n",
        &expected_command_response,
    )
    .await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn multi_queue_errors_abort_exec_and_reset_connection_state() {
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
    let mut admin = TcpStream::connect(addr).await.unwrap();

    send_and_expect(&mut client, b"*1\r\n$5\r\nMULTI\r\n", b"+OK\r\n").await;
    send_and_expect(
        &mut client,
        b"*1\r\n$3\r\nFOO\r\n",
        b"-ERR unknown command\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*3\r\n$3\r\nSET\r\n$11\r\nunknown:key\r\n$1\r\n1\r\n",
        b"+QUEUED\r\n",
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
        b"*2\r\n$3\r\nGET\r\n$11\r\nunknown:key\r\n",
        b"$-1\r\n",
    )
    .await;
    send_and_expect(&mut client, b"*1\r\n$4\r\nPING\r\n", b"+PONG\r\n").await;

    send_and_expect(&mut client, b"*1\r\n$5\r\nMULTI\r\n", b"+OK\r\n").await;
    send_and_expect(
        &mut client,
        b"*2\r\n$4\r\nSAVE\r\n$1\r\nx\r\n",
        b"-ERR wrong number of arguments for 'SAVE' command\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*3\r\n$3\r\nSET\r\n$9\r\narity:key\r\n$1\r\n1\r\n",
        b"+QUEUED\r\n",
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
        b"*2\r\n$3\r\nGET\r\n$9\r\narity:key\r\n",
        b"$-1\r\n",
    )
    .await;
    send_and_expect(&mut client, b"*1\r\n$4\r\nPING\r\n", b"+PONG\r\n").await;

    send_and_expect(&mut client, b"*1\r\n$5\r\nMULTI\r\n", b"+OK\r\n").await;
    send_and_expect(
        &mut client,
        b"*3\r\n$3\r\nSET\r\n$8\r\nsave:key\r\n$1\r\n1\r\n",
        b"+QUEUED\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*1\r\n$4\r\nSAVE\r\n",
        b"-ERR Command not allowed inside a transaction\r\n",
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
        b"*2\r\n$3\r\nGET\r\n$8\r\nsave:key\r\n",
        b"$-1\r\n",
    )
    .await;

    send_and_expect(&mut client, b"*1\r\n$5\r\nMULTI\r\n", b"+OK\r\n").await;
    send_and_expect(
        &mut client,
        b"*3\r\n$3\r\nSET\r\n$8\r\nshut:key\r\n$1\r\n1\r\n",
        b"+QUEUED\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*1\r\n$8\r\nSHUTDOWN\r\n",
        b"-ERR Command not allowed inside a transaction\r\n",
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
        b"*2\r\n$3\r\nGET\r\n$8\r\nshut:key\r\n",
        b"$-1\r\n",
    )
    .await;
    send_and_expect(&mut client, b"*1\r\n$4\r\nPING\r\n", b"+PONG\r\n").await;

    send_and_expect(
        &mut admin,
        b"*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$9\r\nmaxmemory\r\n$1\r\n1\r\n",
        b"+OK\r\n",
    )
    .await;
    send_and_expect(&mut client, b"*1\r\n$5\r\nMULTI\r\n", b"+OK\r\n").await;
    send_and_expect(
        &mut client,
        b"*3\r\n$3\r\nSET\r\n$7\r\noom:key\r\n$1\r\n1\r\n",
        b"-OOM command not allowed when used memory > 'maxmemory'.\r\n",
    )
    .await;
    send_and_expect(
        &mut admin,
        b"*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$9\r\nmaxmemory\r\n$1\r\n0\r\n",
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*3\r\n$3\r\nSET\r\n$8\r\noom2:key\r\n$1\r\n2\r\n",
        b"+QUEUED\r\n",
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
        b"*2\r\n$3\r\nGET\r\n$8\r\noom2:key\r\n",
        b"$-1\r\n",
    )
    .await;
    send_and_expect(&mut client, b"*1\r\n$4\r\nPING\r\n", b"+PONG\r\n").await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn multi_queues_replicaof_until_exec_then_applies_readonly_mode() {
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
        b"*3\r\n$3\r\nSET\r\n$2\r\nk1\r\n$2\r\nv1\r\n",
        b"+OK\r\n",
    )
    .await;
    send_and_expect(&mut client, b"*1\r\n$5\r\nMULTI\r\n", b"+OK\r\n").await;
    send_and_expect(
        &mut client,
        b"*3\r\n$3\r\nSET\r\n$2\r\nk2\r\n$2\r\nv2\r\n",
        b"+QUEUED\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*3\r\n$9\r\nREPLICAOF\r\n$9\r\nlocalhost\r\n$4\r\n9999\r\n",
        b"+QUEUED\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*3\r\n$3\r\nSET\r\n$2\r\nk3\r\n$2\r\nv3\r\n",
        b"+QUEUED\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*1\r\n$4\r\nEXEC\r\n",
        b"*3\r\n+OK\r\n+OK\r\n+OK\r\n",
    )
    .await;

    send_and_expect(
        &mut client,
        b"*3\r\n$3\r\nSET\r\n$2\r\nk4\r\n$2\r\nv4\r\n",
        b"-READONLY You can't write against a read only replica.\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*3\r\n$9\r\nREPLICAOF\r\n$2\r\nNO\r\n$3\r\nONE\r\n",
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*3\r\n$3\r\nSET\r\n$2\r\nk4\r\n$2\r\nv4\r\n",
        b"+OK\r\n",
    )
    .await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn reset_clears_multi_and_authenticated_state() {
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
        b"*7\r\n$3\r\nACL\r\n$7\r\nSETUSER\r\n$5\r\nuser1\r\n$2\r\non\r\n$7\r\n>secret\r\n$5\r\n+@all\r\n$7\r\nallkeys\r\n",
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*3\r\n$4\r\nAUTH\r\n$5\r\nuser1\r\n$6\r\nsecret\r\n",
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*2\r\n$3\r\nACL\r\n$6\r\nWHOAMI\r\n",
        b"$5\r\nuser1\r\n",
    )
    .await;

    send_and_expect(&mut client, b"*1\r\n$5\r\nMULTI\r\n", b"+OK\r\n").await;
    send_and_expect(
        &mut client,
        b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
        b"+QUEUED\r\n",
    )
    .await;
    send_and_expect(&mut client, b"*1\r\n$5\r\nRESET\r\n", b"+RESET\r\n").await;
    send_and_expect(
        &mut client,
        b"*1\r\n$4\r\nEXEC\r\n",
        b"-ERR EXEC without MULTI\r\n",
    )
    .await;
    send_and_expect(&mut client, b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n", b"$-1\r\n").await;
    send_and_expect(
        &mut client,
        b"*2\r\n$3\r\nACL\r\n$6\r\nWHOAMI\r\n",
        b"$7\r\ndefault\r\n",
    )
    .await;
    send_and_expect(&mut client, b"*1\r\n$4\r\nPING\r\n", b"+PONG\r\n").await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn acl_auth_password_rotation_and_hello_auth_match_external_scenarios() {
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
        &encode_resp_command(&[b"ACL", b"WHOAMI"]),
        b"$7\r\ndefault\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"ACL", b"SETUSER", b"newuser"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"ACL", b"USERS"]),
        b"*2\r\n$7\r\ndefault\r\n$7\r\nnewuser\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"ACL", b"SETUSER", b"newuser", b">passwd1"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"AUTH", b"newuser", b"passwd1"]),
        b"-WRONGPASS invalid username-password pair or user is disabled.\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"ACL", b"SETUSER", b"newuser", b"on", b"+acl"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"AUTH", b"newuser", b"passwd1"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"ACL", b"WHOAMI"]),
        b"$7\r\nnewuser\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"ACL", b"SETUSER", b"newuser", b">passwd2"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"AUTH", b"newuser", b"passwd1"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"AUTH", b"newuser", b"passwd2"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"AUTH", b"newuser", b"passwd3"]),
        b"-WRONGPASS invalid username-password pair or user is disabled.\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"ACL", b"SETUSER", b"newuser", b"<passwd1"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"AUTH", b"newuser", b"passwd1"]),
        b"-WRONGPASS invalid username-password pair or user is disabled.\r\n",
    )
    .await;

    let passwd4_hash = crate::acl_password_hash_hex(b"passwd4");
    let mut add_hash = vec![b'#'];
    add_hash.extend_from_slice(&passwd4_hash);
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"ACL", b"SETUSER", b"newuser", add_hash.as_slice()]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"AUTH", b"newuser", b"passwd4"]),
        b"+OK\r\n",
    )
    .await;

    let getuser = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"ACL", b"GETUSER", b"newuser"]),
        Duration::from_secs(1),
    )
    .await;
    assert!(
        resp_socket_contains_bulk(&getuser, passwd4_hash.as_slice()),
        "ACL GETUSER should expose the password hash"
    );
    assert!(
        !resp_socket_contains_bulk(&getuser, b"passwd4"),
        "ACL GETUSER must not expose the plaintext password"
    );

    let mut remove_hash = vec![b'!'];
    remove_hash.extend_from_slice(&passwd4_hash);
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"ACL", b"SETUSER", b"newuser", remove_hash.as_slice()]),
        b"+OK\r\n",
    )
    .await;
    let getuser_after_remove = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"ACL", b"GETUSER", b"newuser"]),
        Duration::from_secs(1),
    )
    .await;
    assert!(
        !resp_socket_contains_bulk(&getuser_after_remove, passwd4_hash.as_slice()),
        "ACL GETUSER should stop exposing a removed hash"
    );

    let hello = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"HELLO", b"3", b"AUTH", b"newuser", b"passwd2"]),
        Duration::from_secs(1),
    )
    .await;
    assert!(
        matches!(hello, RespSocketValue::Map(_) | RespSocketValue::Array(_)),
        "HELLO 3 AUTH should return server info, got {hello:?}"
    );
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"ACL", b"WHOAMI"]),
        b"$7\r\nnewuser\r\n",
    )
    .await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn acl_current_connection_and_hello_chain_match_external_scenarios() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let timeout = Duration::from_secs(5);
    let mut admin = TcpStream::connect(addr).await.unwrap();

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"ACL", b"SETUSER", b"default", b"off"]),
        b"+OK\r\n",
    )
    .await;

    let mut fresh = TcpStream::connect(addr).await.unwrap();
    let noauth =
        send_and_read_error_line(&mut fresh, &encode_resp_command(&[b"PING"]), timeout).await;
    assert!(
        noauth.contains("NOAUTH"),
        "new connections should require AUTH when the default user is off, got {noauth:?}"
    );

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"ACL", b"SETUSER", b"default", b"on"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut admin,
        &encode_resp_command(&[
            b"ACL",
            b"SETUSER",
            b"secure-user",
            b">supass",
            b"on",
            b"+@all",
        ]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut admin,
        &encode_resp_command(&[
            b"ACL",
            b"SETUSER",
            b"secure-user1",
            b">supass",
            b"on",
            b"+@all",
        ]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut admin,
        &encode_resp_command(&[
            b"ACL",
            b"SETUSER",
            b"secure-user2",
            b">supass",
            b"on",
            b"+@all",
        ]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"ACL", b"SETUSER", b"default", b"-@all"]),
        b"+OK\r\n",
    )
    .await;

    let hello_auth = send_and_read_resp_value(
        &mut admin,
        &encode_resp_command(&[b"HELLO", b"2", b"AUTH", b"secure-user", b"supass"]),
        timeout,
    )
    .await;
    assert!(
        matches!(
            hello_auth,
            RespSocketValue::Map(_) | RespSocketValue::Array(_)
        ),
        "HELLO AUTH should succeed even when the current default user has no command permissions, got {hello_auth:?}"
    );
    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"ACL", b"WHOAMI"]),
        b"$11\r\nsecure-user\r\n",
    )
    .await;

    let chained_hello = send_and_read_resp_value(
        &mut admin,
        &encode_resp_command(&[
            b"HELLO",
            b"2",
            b"AUTH",
            b"secure-user",
            b"pass",
            b"AUTH",
            b"secure-user2",
            b"supass",
            b"AUTH",
            b"secure-user1",
            b"supass",
        ]),
        timeout,
    )
    .await;
    assert!(
        matches!(
            chained_hello,
            RespSocketValue::Map(_) | RespSocketValue::Array(_)
        ),
        "HELLO should accept the last AUTH option, got {chained_hello:?}"
    );
    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"ACL", b"WHOAMI"]),
        b"$12\r\nsecure-user1\r\n",
    )
    .await;

    let wrongpass = send_and_read_error_line(
        &mut admin,
        &encode_resp_command(&[
            b"HELLO",
            b"2",
            b"AUTH",
            b"secure-user",
            b"supass",
            b"AUTH",
            b"secure-user2",
            b"supass",
            b"AUTH",
            b"secure-user",
            b"pass",
        ]),
        timeout,
    )
    .await;
    assert!(
        wrongpass.contains("WRONGPASS"),
        "last HELLO AUTH should decide the result, got {wrongpass:?}"
    );
    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"ACL", b"WHOAMI"]),
        b"$12\r\nsecure-user1\r\n",
    )
    .await;

    let setname_hello = send_and_read_resp_value(
        &mut admin,
        &encode_resp_command(&[
            b"HELLO", b"2", b"SETNAME", b"client1", b"SETNAME", b"client2", b"SETNAME", b"client3",
            b"SETNAME", b"client4",
        ]),
        timeout,
    )
    .await;
    assert!(
        matches!(
            setname_hello,
            RespSocketValue::Map(_) | RespSocketValue::Array(_)
        ),
        "HELLO should accept the last SETNAME option, got {setname_hello:?}"
    );
    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"CLIENT", b"GETNAME"]),
        b"$7\r\nclient4\r\n",
    )
    .await;

    let invalid_name = send_and_read_error_line(
        &mut admin,
        &encode_resp_command(&[
            b"HELLO",
            b"2",
            b"SETNAME",
            b"client5",
            b"SETNAME",
            b"client6",
            b"SETNAME",
            b"client name",
        ]),
        timeout,
    )
    .await;
    assert!(
        invalid_name.contains("Client names cannot contain spaces"),
        "HELLO should validate only the final SETNAME token, got {invalid_name:?}"
    );
    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"CLIENT", b"GETNAME"]),
        b"$7\r\nclient4\r\n",
    )
    .await;

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"CLIENT", b"SETNAME", b"client0"]),
        b"+OK\r\n",
    )
    .await;
    let failed_hello = send_and_read_error_line(
        &mut admin,
        &encode_resp_command(&[
            b"HELLO", b"2", b"AUTH", b"user", b"pass", b"SETNAME", b"client1",
        ]),
        timeout,
    )
    .await;
    assert!(
        failed_hello.contains("WRONGPASS"),
        "failed HELLO AUTH should not authenticate the client, got {failed_hello:?}"
    );
    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"CLIENT", b"GETNAME"]),
        b"$7\r\nclient0\r\n",
    )
    .await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn acl_runtime_aclfile_reload_and_metrics_match_external_scenarios() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let timeout = Duration::from_secs(5);
    let temp_dir = unique_test_temp_dir("acl-runtime-file");
    let aclfile_path = temp_dir.join("user.acl");
    std::fs::write(
        &aclfile_path,
        concat!(
            "user alice on allcommands allkeys &* >alice\n",
            "user bob on -@all +@set +acl ~set* &* >bob\n",
            "user doug on resetchannels &test* +@all ~* >doug\n",
            "user default on nopass ~* &* +@all\n",
        ),
    )
    .unwrap();
    let dir_bytes = temp_dir.to_string_lossy().into_owned().into_bytes();

    let mut admin = TcpStream::connect(addr).await.unwrap();
    let mut inspector = TcpStream::connect(addr).await.unwrap();

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"CONFIG", b"SET", b"dir", dir_bytes.as_slice()]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"CONFIG", b"SET", b"acl-pubsub-default", b"allchannels"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"CONFIG", b"SET", b"aclfile", b"user.acl"]),
        b"+OK\r\n",
    )
    .await;

    let mut alice = TcpStream::connect(addr).await.unwrap();
    send_and_expect(
        &mut alice,
        &encode_resp_command(&[b"AUTH", b"alice", b"alice"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut alice,
        &encode_resp_command(&[b"ACL", b"WHOAMI"]),
        b"$5\r\nalice\r\n",
    )
    .await;

    let mut bob = TcpStream::connect(addr).await.unwrap();
    send_and_expect(
        &mut bob,
        &encode_resp_command(&[b"AUTH", b"bob", b"bob"]),
        b"+OK\r\n",
    )
    .await;
    assert_eq!(
        send_and_read_integer(
            &mut bob,
            &encode_resp_command(&[b"SADD", b"set", b"1", b"2", b"3"]),
            timeout,
        )
        .await,
        3
    );
    let bob_denied = send_and_read_error_line(
        &mut bob,
        &encode_resp_command(&[b"SET", b"key", b"value"]),
        timeout,
    )
    .await;
    assert!(bob_denied.contains("NOPERM"));
    assert!(bob_denied.contains("set"));

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[
            b"ACL", b"SETUSER", b"default", b"reset", b"on", b"nopass", b"~*", b"+@all",
        ]),
        b"+OK\r\n",
    )
    .await;
    let mut allchannels_client = TcpStream::connect(addr).await.unwrap();
    let subscribe_ok = send_and_read_resp_value(
        &mut allchannels_client,
        &encode_resp_command(&[b"SUBSCRIBE", b"foo"]),
        timeout,
    )
    .await;
    let subscribe_ok_items = resp_socket_array(&subscribe_ok);
    assert_eq!(resp_socket_bulk(&subscribe_ok_items[0]), b"subscribe");
    assert_eq!(resp_socket_bulk(&subscribe_ok_items[1]), b"foo");
    drop(allchannels_client);

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"CONFIG", b"SET", b"acl-pubsub-default", b"resetchannels"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut admin,
        &encode_resp_command(&[
            b"ACL", b"SETUSER", b"default", b"reset", b"on", b"nopass", b"~*", b"+@all",
        ]),
        b"+OK\r\n",
    )
    .await;
    let mut resetchannels_client = TcpStream::connect(addr).await.unwrap();
    let denied_channel = send_and_read_error_line(
        &mut resetchannels_client,
        &encode_resp_command(&[b"SUBSCRIBE", b"foo"]),
        timeout,
    )
    .await;
    assert!(denied_channel.contains("NOPERM"));
    assert!(denied_channel.contains("channel"));

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"CONFIG", b"SET", b"user", b"default on nopass ~* +@all"]),
        b"+OK\r\n",
    )
    .await;
    let mut config_user_denied = TcpStream::connect(addr).await.unwrap();
    let config_user_denied_error = send_and_read_error_line(
        &mut config_user_denied,
        &encode_resp_command(&[b"SUBSCRIBE", b"foo"]),
        timeout,
    )
    .await;
    assert!(config_user_denied_error.contains("NOPERM"));
    assert!(config_user_denied_error.contains("channel"));

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"CONFIG", b"SET", b"user", b"default on nopass ~* &* +@all"]),
        b"+OK\r\n",
    )
    .await;
    let mut config_user_allowed = TcpStream::connect(addr).await.unwrap();
    let config_user_allowed_subscribe = send_and_read_resp_value(
        &mut config_user_allowed,
        &encode_resp_command(&[b"SUBSCRIBE", b"foo"]),
        timeout,
    )
    .await;
    assert_eq!(
        resp_socket_bulk(&resp_socket_array(&config_user_allowed_subscribe)[1]),
        b"foo"
    );
    let config_user_allowed_psubscribe = send_and_read_resp_value(
        &mut config_user_allowed,
        &encode_resp_command(&[b"PSUBSCRIBE", b"bar*"]),
        timeout,
    )
    .await;
    let config_user_allowed_psubscribe_items = resp_socket_array(&config_user_allowed_psubscribe);
    assert_eq!(
        resp_socket_bulk(&config_user_allowed_psubscribe_items[0]),
        b"psubscribe"
    );
    assert_eq!(
        resp_socket_bulk(&config_user_allowed_psubscribe_items[1]),
        b"bar*"
    );
    drop(config_user_allowed);

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"CONFIG", b"RESETSTAT"]),
        b"+OK\r\n",
    )
    .await;
    let auth_failed = send_and_read_error_line(
        &mut admin,
        &encode_resp_command(&[b"HELLO", b"2", b"AUTH", b"notrealuser", b"1233456"]),
        timeout,
    )
    .await;
    assert_eq!(
        auth_failed,
        "WRONGPASS invalid username-password pair or user is disabled."
    );
    let auth_failed_resp3 = send_and_read_error_line(
        &mut admin,
        &encode_resp_command(&[b"HELLO", b"3", b"AUTH", b"notrealuser", b"1233456"]),
        timeout,
    )
    .await;
    assert_eq!(
        auth_failed_resp3,
        "WRONGPASS invalid username-password pair or user is disabled."
    );
    let auth_failed_plain = send_and_read_error_line(
        &mut admin,
        &encode_resp_command(&[b"AUTH", b"notrealuser", b"1233456"]),
        timeout,
    )
    .await;
    assert_eq!(
        auth_failed_plain,
        "WRONGPASS invalid username-password pair or user is disabled."
    );

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[
            b"ACL",
            b"SETUSER",
            b"invalidcmduser",
            b"on",
            b">passwd",
            b"nocommands",
        ]),
        b"+OK\r\n",
    )
    .await;
    let mut invalid_cmd = TcpStream::connect(addr).await.unwrap();
    send_and_expect(
        &mut invalid_cmd,
        &encode_resp_command(&[b"AUTH", b"invalidcmduser", b"passwd"]),
        b"+OK\r\n",
    )
    .await;
    let invalid_cmd_error = send_and_read_error_line(
        &mut invalid_cmd,
        &encode_resp_command(&[b"ACL", b"LIST"]),
        timeout,
    )
    .await;
    assert!(invalid_cmd_error.contains("no permissions to run"));

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[
            b"ACL",
            b"SETUSER",
            b"invalidkeyuser",
            b"on",
            b">passwd",
            b"resetkeys",
            b"allcommands",
        ]),
        b"+OK\r\n",
    )
    .await;
    let mut invalid_key = TcpStream::connect(addr).await.unwrap();
    send_and_expect(
        &mut invalid_key,
        &encode_resp_command(&[b"AUTH", b"invalidkeyuser", b"passwd"]),
        b"+OK\r\n",
    )
    .await;
    let invalid_key_error = send_and_read_error_line(
        &mut invalid_key,
        &encode_resp_command(&[b"GET", b"x"]),
        timeout,
    )
    .await;
    assert!(invalid_key_error.contains("NOPERM"));
    assert!(invalid_key_error.contains("key"));

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[
            b"ACL",
            b"SETUSER",
            b"invalidchanneluser",
            b"on",
            b">passwd",
            b"resetchannels",
            b"allcommands",
        ]),
        b"+OK\r\n",
    )
    .await;
    let mut invalid_channel = TcpStream::connect(addr).await.unwrap();
    send_and_expect(
        &mut invalid_channel,
        &encode_resp_command(&[b"AUTH", b"invalidchanneluser", b"passwd"]),
        b"+OK\r\n",
    )
    .await;
    let invalid_channel_error = send_and_read_error_line(
        &mut invalid_channel,
        &encode_resp_command(&[b"SUBSCRIBE", b"x"]),
        timeout,
    )
    .await;
    assert!(invalid_channel_error.contains("NOPERM"));
    assert!(invalid_channel_error.contains("channel"));

    let stats = send_and_read_bulk_payload(
        &mut admin,
        &encode_resp_command(&[b"INFO", b"STATS"]),
        timeout,
    )
    .await;
    assert_eq!(read_info_u64(&stats, "acl_access_denied_auth"), Some(3));
    assert_eq!(read_info_u64(&stats, "acl_access_denied_cmd"), Some(1));
    assert_eq!(read_info_u64(&stats, "acl_access_denied_key"), Some(1));
    assert_eq!(read_info_u64(&stats, "acl_access_denied_channel"), Some(1));

    let mut alice_subscriber = TcpStream::connect(addr).await.unwrap();
    send_and_expect(
        &mut alice_subscriber,
        &encode_resp_command(&[b"AUTH", b"alice", b"alice"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut alice_subscriber,
        &encode_resp_command(&[b"CLIENT", b"SETNAME", b"alice-subscriber"]),
        b"+OK\r\n",
    )
    .await;
    let alice_subscribe = send_and_read_resp_value(
        &mut alice_subscriber,
        &encode_resp_command(&[b"SUBSCRIBE", b"test1"]),
        timeout,
    )
    .await;
    assert_eq!(
        resp_socket_bulk(&resp_socket_array(&alice_subscribe)[1]),
        b"test1"
    );

    let mut doug_subscriber = TcpStream::connect(addr).await.unwrap();
    send_and_expect(
        &mut doug_subscriber,
        &encode_resp_command(&[b"AUTH", b"doug", b"doug"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut doug_subscriber,
        &encode_resp_command(&[b"CLIENT", b"SETNAME", b"doug-subscriber"]),
        b"+OK\r\n",
    )
    .await;
    let doug_subscribe = send_and_read_resp_value(
        &mut doug_subscriber,
        &encode_resp_command(&[b"SUBSCRIBE", b"test1"]),
        timeout,
    )
    .await;
    assert_eq!(
        resp_socket_bulk(&resp_socket_array(&doug_subscribe)[1]),
        b"test1"
    );

    std::fs::write(
        &aclfile_path,
        concat!(
            "user alice on allcommands allkeys &* >alice\n",
            "user bob on -@all +@set +acl ~set* &* >bob\n",
            "user doug on resetchannels &test +@all ~* >doug\n",
            "user default on nopass ~* &* +@all\n",
        ),
    )
    .unwrap();
    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"ACL", b"LOAD"]),
        b"+OK\r\n",
    )
    .await;
    let client_list_after_load = send_and_read_bulk_payload(
        &mut inspector,
        &encode_resp_command(&[b"CLIENT", b"LIST"]),
        timeout,
    )
    .await;
    let client_list_text = std::str::from_utf8(&client_list_after_load).unwrap();
    assert!(client_list_line_with_name(client_list_text, "alice-subscriber").is_some());
    assert!(client_list_line_with_name(client_list_text, "doug-subscriber").is_none());

    let publish = send_and_read_resp_value(
        &mut admin,
        &encode_resp_command(&[b"PUBLISH", b"test1", b"test-message"]),
        timeout,
    )
    .await;
    assert!(
        resp_socket_integer(&publish) >= 1,
        "PUBLISH should still reach the unaffected subscriber, got {publish:?}"
    );
    let delivered = read_resp_value_with_timeout(&mut alice_subscriber, timeout).await;
    let delivered_items = resp_socket_array(&delivered);
    assert_eq!(resp_socket_bulk(&delivered_items[0]), b"message");
    assert_eq!(resp_socket_bulk(&delivered_items[1]), b"test1");
    assert_eq!(resp_socket_bulk(&delivered_items[2]), b"test-message");
    let mut killed_subscriber_byte = [0u8; 1];
    let killed_subscriber_read = tokio::time::timeout(
        Duration::from_secs(1),
        doug_subscriber.read_exact(&mut killed_subscriber_byte),
    )
    .await;
    assert!(
        matches!(killed_subscriber_read, Ok(Err(_))),
        "ACL LOAD should disconnect the affected subscriber socket: {killed_subscriber_read:?}"
    );

    std::fs::write(
        &aclfile_path,
        concat!(
            "user alice on allcommands allkeys &* >alice\n",
            "user bob on -@all +@set +acl ~set* &* >bob\n",
            "user doug on resetchannels &test* +@all ~* >doug\n",
            "user default on nopass ~* &* +@all\n",
        ),
    )
    .unwrap();
    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"ACL", b"LOAD"]),
        b"+OK\r\n",
    )
    .await;

    let mut self_loader = TcpStream::connect(addr).await.unwrap();
    let self_loader_hello = send_and_read_resp_value(
        &mut self_loader,
        &encode_resp_command(&[b"HELLO", b"3", b"AUTH", b"doug", b"doug"]),
        timeout,
    )
    .await;
    assert!(
        matches!(self_loader_hello, RespSocketValue::Map(_)),
        "HELLO 3 AUTH should return a RESP3 map, got {self_loader_hello:?}"
    );
    send_and_expect(
        &mut self_loader,
        &encode_resp_command(&[b"CLIENT", b"SETNAME", b"self-loader"]),
        b"+OK\r\n",
    )
    .await;
    let self_subscribe = send_and_read_resp_value(
        &mut self_loader,
        &encode_resp_command(&[b"SUBSCRIBE", b"test1"]),
        timeout,
    )
    .await;
    assert_eq!(
        resp_socket_bulk(&resp_socket_array(&self_subscribe)[1]),
        b"test1"
    );
    std::fs::write(
        &aclfile_path,
        concat!(
            "user alice on allcommands allkeys &* >alice\n",
            "user bob on -@all +@set +acl ~set* &* >bob\n",
            "user doug on resetchannels &test +@all ~* >doug\n",
            "user default on nopass ~* &* +@all\n",
        ),
    )
    .unwrap();
    let self_load = send_and_read_resp_value(
        &mut self_loader,
        &encode_resp_command(&[b"ACL", b"LOAD"]),
        timeout,
    )
    .await;
    match self_load {
        RespSocketValue::Simple(message) => assert_eq!(message, b"OK"),
        other => panic!("expected ACL LOAD to return +OK, got {other:?}"),
    }
    let client_list_after_self_load = send_and_read_bulk_payload(
        &mut inspector,
        &encode_resp_command(&[b"CLIENT", b"LIST"]),
        timeout,
    )
    .await;
    let client_list_after_self_load_text =
        std::str::from_utf8(&client_list_after_self_load).unwrap();
    assert!(client_list_line_with_name(client_list_after_self_load_text, "self-loader").is_none());

    let _ = shutdown_tx.send(());
    server.await.unwrap();
    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn acl_deluser_disconnects_current_client_after_reply_like_external_scenario() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let timeout = Duration::from_secs(5);
    let mut admin = TcpStream::connect(addr).await.unwrap();
    let mut user = TcpStream::connect(addr).await.unwrap();

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"ACL", b"SETUSER", b"using", b"on", b"+acl", b">passwd"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut user,
        &encode_resp_command(&[b"AUTH", b"using", b"passwd"]),
        b"+OK\r\n",
    )
    .await;

    let deleted = send_and_read_integer(
        &mut user,
        &encode_resp_command(&[b"ACL", b"DELUSER", b"using"]),
        timeout,
    )
    .await;
    assert_eq!(deleted, 1);

    user.write_all(&encode_resp_command(&[b"PING"]))
        .await
        .unwrap();
    let mut eof_probe = [0u8; 1];
    let read_result = tokio::time::timeout(timeout, user.read(&mut eof_probe)).await;
    match read_result {
        Ok(Ok(0)) => {}
        Ok(Err(error))
            if matches!(
                error.kind(),
                std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
            ) => {}
        other => {
            panic!("deleted-user client should disconnect after DELUSER reply: {other:?}");
        }
    }

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn acl_load_is_allowed_while_server_is_replica() {
    let (addr, shutdown_tx, server) = start_test_server_with_scripting_enabled().await;
    let timeout = Duration::from_secs(5);
    let mut client = TcpStream::connect(addr).await.unwrap();

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SLAVEOF", b"127.0.0.1", b"1"]),
        b"+OK\r\n",
    )
    .await;
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let set_response = send_and_read_resp_value(
            &mut client,
            &encode_resp_command(&[b"SET", b"replica-probe", b"1"]),
            timeout,
        )
        .await;
        if matches!(&set_response, RespSocketValue::Error(message) if String::from_utf8_lossy(message).contains("READONLY"))
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for read-only replica mode; last response: {set_response:?}"
        );
        sleep(Duration::from_millis(50)).await;
    }

    let load_error = send_and_read_error_line(
        &mut client,
        &encode_resp_command(&[b"ACL", b"LOAD"]),
        timeout,
    )
    .await;
    assert_eq!(
        load_error,
        "ERR This Redis instance is not configured to use an ACL file"
    );

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn acl_command_key_channel_and_dryrun_permissions_match_external_scenarios() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let timeout = Duration::from_secs(5);
    let mut client = TcpStream::connect(addr).await.unwrap();

    send_and_expect(
        &mut client,
        &encode_resp_command(&[
            b"ACL", b"SETUSER", b"newuser", b"reset", b"on", b"nopass", b"+acl",
        ]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"AUTH", b"newuser", b"pass"]),
        b"+OK\r\n",
    )
    .await;

    let denied_command = send_and_read_error_line(
        &mut client,
        &encode_resp_command(&[b"SET", b"foo", b"bar"]),
        timeout,
    )
    .await;
    assert!(denied_command.contains("NOPERM"));
    assert!(denied_command.contains("set"));

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"ACL", b"SETUSER", b"newuser", b"+set"]),
        b"+OK\r\n",
    )
    .await;
    let denied_key = send_and_read_error_line(
        &mut client,
        &encode_resp_command(&[b"SET", b"foo", b"bar"]),
        timeout,
    )
    .await;
    assert!(denied_key.contains("NOPERM"));
    assert!(denied_key.contains("key"));

    send_and_expect(
        &mut client,
        &encode_resp_command(&[
            b"ACL",
            b"SETUSER",
            b"newuser",
            b"allcommands",
            b"~foo:*",
            b"~bar:*",
        ]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"foo:1", b"a"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"bar:2", b"b"]),
        b"+OK\r\n",
    )
    .await;
    let denied_pattern = send_and_read_error_line(
        &mut client,
        &encode_resp_command(&[b"SET", b"zap:3", b"c"]),
        timeout,
    )
    .await;
    assert!(denied_pattern.contains("NOPERM"));
    assert!(denied_pattern.contains("key"));

    send_and_expect(
        &mut client,
        &encode_resp_command(&[
            b"ACL", b"SETUSER", b"newuser", b"-@all", b"+@set", b"allkeys", b"+acl",
        ]),
        b"+OK\r\n",
    )
    .await;
    assert_eq!(
        send_and_read_integer(
            &mut client,
            &encode_resp_command(&[b"SADD", b"myset", b"a", b"b", b"c"]),
            timeout,
        )
        .await,
        3
    );
    let denied_string = send_and_read_error_line(
        &mut client,
        &encode_resp_command(&[b"SET", b"x", b"1"]),
        timeout,
    )
    .await;
    assert!(denied_string.contains("NOPERM"));
    assert!(denied_string.contains("set"));

    send_and_expect(
        &mut client,
        &encode_resp_command(&[
            b"ACL",
            b"SETUSER",
            b"newuser",
            b"+@all",
            b"-client",
            b"+client|id",
            b"+client|setname",
        ]),
        b"+OK\r\n",
    )
    .await;
    assert!(
        send_and_read_integer(
            &mut client,
            &encode_resp_command(&[b"CLIENT", b"ID"]),
            timeout
        )
        .await
            > 0
    );
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"CLIENT", b"SETNAME", b"foo"]),
        b"+OK\r\n",
    )
    .await;
    let denied_subcommand = send_and_read_error_line(
        &mut client,
        &encode_resp_command(&[b"CLIENT", b"KILL", b"TYPE", b"master"]),
        timeout,
    )
    .await;
    assert!(denied_subcommand.contains("NOPERM"));
    assert!(denied_subcommand.contains("client|kill"));

    send_and_expect(
        &mut client,
        &encode_resp_command(&[
            b"ACL",
            b"SETUSER",
            b"newuser",
            b"-@all",
            b"+acl",
            b"+config",
            b"-config|set",
        ]),
        b"+OK\r\n",
    )
    .await;
    let config_get = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"CONFIG", b"GET", b"loglevel"]),
        timeout,
    )
    .await;
    assert!(
        matches!(
            config_get,
            RespSocketValue::Array(_) | RespSocketValue::Map(_)
        ),
        "CONFIG GET should be allowed, got {config_get:?}"
    );
    let denied_config = send_and_read_error_line(
        &mut client,
        &encode_resp_command(&[b"CONFIG", b"SET", b"loglevel", b"notice"]),
        timeout,
    )
    .await;
    assert!(denied_config.contains("NOPERM"));
    assert!(denied_config.contains("config|set"));

    send_and_expect(
        &mut client,
        &encode_resp_command(&[
            b"ACL",
            b"SETUSER",
            b"newuser",
            b"-@all",
            b"+acl",
            b"+select|0",
        ]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SELECT", b"0"]),
        b"+OK\r\n",
    )
    .await;
    let denied_select = send_and_read_error_line(
        &mut client,
        &encode_resp_command(&[b"SELECT", b"1"]),
        timeout,
    )
    .await;
    assert!(denied_select.contains("NOPERM"));
    assert!(denied_select.contains("select|1"));

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"ACL", b"SETUSER", b"newuser", b"+@all", b"-config|get"]),
        b"+OK\r\n",
    )
    .await;
    let nested_rule_error = send_and_read_error_line(
        &mut client,
        &encode_resp_command(&[b"ACL", b"SETUSER", b"newuser", b"+config|get|appendonly"]),
        timeout,
    )
    .await;
    assert!(nested_rule_error.contains("Allowing first-arg of a subcommand is not supported"));

    let unknown_rule_error = send_and_read_error_line(
        &mut client,
        &encode_resp_command(&[b"ACL", b"SETUSER", b"newuser", b"+config|asdf"]),
        timeout,
    )
    .await;
    assert!(unknown_rule_error.contains("Unknown command or category name in ACL"));

    let cat_unknown = send_and_read_error_line(
        &mut client,
        &encode_resp_command(&[b"ACL", b"CAT", b"NON_EXISTS"]),
        timeout,
    )
    .await;
    assert!(cat_unknown.contains("Unknown category 'NON_EXISTS'"));

    let scripting_category = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"ACL", b"CAT", b"scripting"]),
        timeout,
    )
    .await;
    let scripting_members = resp_socket_array(&scripting_category)
        .iter()
        .map(resp_socket_bulk)
        .collect::<Vec<_>>();
    assert!(scripting_members.contains(&b"function|list".as_slice()));
    assert!(!scripting_members.contains(&b"set".as_slice()));

    send_and_expect(
        &mut client,
        &encode_resp_command(&[
            b"ACL",
            b"SETUSER",
            b"scripter",
            b"on",
            b"nopass",
            b"+readonly",
        ]),
        b"+OK\r\n",
    )
    .await;
    let dryrun_error = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"ACL", b"DRYRUN", b"scripter", b"EVAL_RO", b"", b"0"]),
        timeout,
    )
    .await;
    let dryrun_error_text = match dryrun_error {
        RespSocketValue::Bulk(message) => String::from_utf8(message).unwrap(),
        other => panic!("expected ACL DRYRUN denial as bulk string, got {other:?}"),
    };
    assert!(dryrun_error_text.contains("NOPERM"));
    assert!(dryrun_error_text.contains("eval_ro"));

    send_and_expect(
        &mut client,
        &encode_resp_command(&[
            b"ACL",
            b"SETUSER",
            b"psuser",
            b"reset",
            b"on",
            b">pspass",
            b"+acl",
            b"+client",
            b"+multi",
            b"+discard",
            b"+@pubsub",
        ]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"AUTH", b"psuser", b"pspass"]),
        b"+OK\r\n",
    )
    .await;
    let denied_publish = send_and_read_error_line(
        &mut client,
        &encode_resp_command(&[b"PUBLISH", b"foo", b"bar"]),
        timeout,
    )
    .await;
    assert!(denied_publish.contains("NOPERM"));
    assert!(denied_publish.contains("channel"));

    send_and_expect(
        &mut client,
        &encode_resp_command(&[
            b"ACL",
            b"SETUSER",
            b"psuser",
            b"resetchannels",
            b"&foo:1",
            b"&bar:*",
        ]),
        b"+OK\r\n",
    )
    .await;
    assert_eq!(
        send_and_read_integer(
            &mut client,
            &encode_resp_command(&[b"PUBLISH", b"foo:1", b"somemessage"]),
            timeout,
        )
        .await,
        0
    );
    assert_eq!(
        send_and_read_integer(
            &mut client,
            &encode_resp_command(&[b"PUBLISH", b"bar:2", b"anothermessage"]),
            timeout,
        )
        .await,
        0
    );
    let denied_channel = send_and_read_error_line(
        &mut client,
        &encode_resp_command(&[b"PUBLISH", b"zap:3", b"nosuchmessage"]),
        timeout,
    )
    .await;
    assert!(denied_channel.contains("NOPERM"));
    assert!(denied_channel.contains("channel"));

    send_and_expect(&mut client, &encode_resp_command(&[b"MULTI"]), b"+OK\r\n").await;
    let queued_publish = send_and_read_error_line(
        &mut client,
        &encode_resp_command(&[b"PUBLISH", b"notexists", b"helloworld"]),
        timeout,
    )
    .await;
    assert!(queued_publish.contains("NOPERM"));
    assert!(queued_publish.contains("channel"));
    send_and_expect(&mut client, &encode_resp_command(&[b"DISCARD"]), b"+OK\r\n").await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn oidc_auth_bearer_token_and_hello_auth_match_configured_jwks_scenarios() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let timeout = Duration::from_secs(5);
    let oidc_auth_user = crate::request_lifecycle::OIDC_AUTH_SENTINEL_USERNAME;
    let temp_dir = unique_test_temp_dir("oidc-auth-jwks");
    let jwks_path = temp_dir.join("oidc-jwks.json");
    std::fs::write(&jwks_path, TEST_OIDC_RSA_JWKS).unwrap();
    let dir_bytes = temp_dir.to_string_lossy().into_owned().into_bytes();
    let issuer = "https://issuer.example.test";
    let audience = "garnet-auth-tests";

    let mut admin = TcpStream::connect(addr).await.unwrap();
    let mut controller = TcpStream::connect(addr).await.unwrap();
    let mut hello_client = TcpStream::connect(addr).await.unwrap();

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"ACL", b"SETUSER", b"workload", b"on", b"+@all", b"allkeys"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"ACL", b"SETUSER", b"app-oid", b"on", b"+@all", b"allkeys"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"CONFIG", b"SET", b"dir", dir_bytes.as_slice()]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"CONFIG", b"SET", b"auth-oidc-issuer", issuer.as_bytes()]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut admin,
        &encode_resp_command(&[
            b"CONFIG",
            b"SET",
            b"auth-oidc-audience",
            audience.as_bytes(),
        ]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"CONFIG", b"SET", b"auth-oidc-jwks-file", b"oidc-jwks.json"]),
        b"+OK\r\n",
    )
    .await;

    let config = send_and_read_resp_value(
        &mut admin,
        &encode_resp_command(&[b"CONFIG", b"GET", b"auth-oidc-*"]),
        timeout,
    )
    .await;
    let config_map = resp_socket_map_or_flat_map(&config);
    assert_eq!(
        resp_socket_bulk(config_map[&b"auth-oidc-issuer".to_vec()]),
        issuer.as_bytes()
    );
    assert_eq!(
        resp_socket_bulk(config_map[&b"auth-oidc-audience".to_vec()]),
        audience.as_bytes()
    );
    assert_eq!(
        resp_socket_bulk(config_map[&b"auth-oidc-jwks-file".to_vec()]),
        b"oidc-jwks.json"
    );
    assert_eq!(
        resp_socket_bulk(config_map[&b"auth-oidc-principal-claim".to_vec()]),
        b"sub"
    );
    assert_eq!(
        resp_socket_bulk(config_map[&b"auth-oidc-algorithms".to_vec()]),
        b"RS256"
    );

    let workload_token = issue_test_oidc_token(Some("workload"), None, issuer, audience);
    let implicit_oidc = send_and_read_error_line(
        &mut admin,
        &encode_resp_command(&[b"AUTH", b"workload", workload_token.as_bytes()]),
        timeout,
    )
    .await;
    assert!(implicit_oidc.contains("WRONGPASS"));
    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"AUTH", oidc_auth_user, workload_token.as_bytes()]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"ACL", b"WHOAMI"]),
        b"$8\r\nworkload\r\n",
    )
    .await;

    let hello = send_and_read_resp_value(
        &mut hello_client,
        &encode_resp_command(&[
            b"HELLO",
            b"3",
            b"AUTH",
            oidc_auth_user,
            workload_token.as_bytes(),
        ]),
        timeout,
    )
    .await;
    assert!(
        matches!(hello, RespSocketValue::Map(_) | RespSocketValue::Array(_)),
        "HELLO AUTH with OIDC token should return server info, got {hello:?}"
    );
    send_and_expect(
        &mut hello_client,
        &encode_resp_command(&[b"ACL", b"WHOAMI"]),
        b"$8\r\nworkload\r\n",
    )
    .await;

    let wrong_audience_token =
        issue_test_oidc_token(Some("workload"), None, issuer, "wrong-audience");
    let wrong_audience = send_and_read_error_line(
        &mut admin,
        &encode_resp_command(&[b"AUTH", oidc_auth_user, wrong_audience_token.as_bytes()]),
        timeout,
    )
    .await;
    assert!(wrong_audience.contains("WRONGPASS"));
    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"ACL", b"WHOAMI"]),
        b"$8\r\nworkload\r\n",
    )
    .await;

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"CONFIG", b"SET", b"auth-oidc-principal-claim", b"oid"]),
        b"+OK\r\n",
    )
    .await;
    let oid_token = issue_test_oidc_token(None, Some("app-oid"), issuer, audience);
    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"AUTH", oidc_auth_user, oid_token.as_bytes()]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"ACL", b"WHOAMI"]),
        b"$7\r\napp-oid\r\n",
    )
    .await;

    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"ACL", b"SETUSER", b"app-oid", b"off"]),
        b"+OK\r\n",
    )
    .await;
    let disabled_principal = send_and_read_error_line(
        &mut admin,
        &encode_resp_command(&[b"AUTH", oidc_auth_user, oid_token.as_bytes()]),
        timeout,
    )
    .await;
    assert!(disabled_principal.contains("WRONGPASS"));
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"ACL", b"SETUSER", b"app-oid", b"on", b"+@all", b"allkeys"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"ACL", b"WHOAMI"]),
        b"$7\r\napp-oid\r\n",
    )
    .await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn oidc_auth_config_validation_rejects_incomplete_or_unsafe_settings() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let timeout = Duration::from_secs(5);
    let temp_dir = unique_test_temp_dir("oidc-auth-config");
    std::fs::write(temp_dir.join("oidc-jwks.json"), TEST_OIDC_RSA_JWKS).unwrap();
    std::fs::write(temp_dir.join("invalid-jwks.json"), b"{not-json").unwrap();
    let dir_bytes = temp_dir.to_string_lossy().into_owned().into_bytes();

    let mut client = TcpStream::connect(addr).await.unwrap();
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"CONFIG", b"SET", b"dir", dir_bytes.as_slice()]),
        b"+OK\r\n",
    )
    .await;

    let incomplete = send_and_read_error_line(
        &mut client,
        &encode_resp_command(&[b"CONFIG", b"SET", b"auth-oidc-jwks-file", b"oidc-jwks.json"]),
        timeout,
    )
    .await;
    assert!(incomplete.contains("auth-oidc-issuer and auth-oidc-audience"));

    let invalid_algorithms = send_and_read_error_line(
        &mut client,
        &encode_resp_command(&[b"CONFIG", b"SET", b"auth-oidc-algorithms", b"HS256"]),
        timeout,
    )
    .await;
    assert!(invalid_algorithms.contains("symmetric JWT algorithms are not allowed"));

    let empty_claim = send_and_read_error_line(
        &mut client,
        &encode_resp_command(&[b"CONFIG", b"SET", b"auth-oidc-principal-claim", b""]),
        timeout,
    )
    .await;
    assert!(empty_claim.contains("non-empty UTF-8 string"));

    let reserved_username = send_and_read_error_line(
        &mut client,
        &encode_resp_command(&[b"ACL", b"SETUSER", b"__OIDC__", b"on"]),
        timeout,
    )
    .await;
    assert!(reserved_username.contains("'__OIDC__' is reserved"));

    send_and_expect(
        &mut client,
        &encode_resp_command(&[
            b"CONFIG",
            b"SET",
            b"auth-oidc-issuer",
            b"https://issuer.example.test",
        ]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[
            b"CONFIG",
            b"SET",
            b"auth-oidc-audience",
            b"garnet-auth-tests",
        ]),
        b"+OK\r\n",
    )
    .await;

    let missing_file = send_and_read_error_line(
        &mut client,
        &encode_resp_command(&[
            b"CONFIG",
            b"SET",
            b"auth-oidc-jwks-file",
            b"missing-jwks.json",
        ]),
        timeout,
    )
    .await;
    assert!(missing_file.contains("unable to read JWKS file"));

    let invalid_file = send_and_read_error_line(
        &mut client,
        &encode_resp_command(&[
            b"CONFIG",
            b"SET",
            b"auth-oidc-jwks-file",
            b"invalid-jwks.json",
        ]),
        timeout,
    )
    .await;
    assert!(invalid_file.contains("invalid JWKS JSON"));

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn acl_pubsub_revocation_and_blocking_recheck_match_external_scenarios() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let timeout = Duration::from_secs(5);
    let mut admin = TcpStream::connect(addr).await.unwrap();
    let mut inspector = TcpStream::connect(addr).await.unwrap();

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[
            b"ACL",
            b"SETUSER",
            b"psuser",
            b"reset",
            b"on",
            b">pspass",
            b"+acl",
            b"+client",
            b"+@pubsub",
            b"resetchannels",
            b"&foo:1",
        ]),
        b"+OK\r\n",
    )
    .await;

    let mut subscriber = TcpStream::connect(addr).await.unwrap();
    send_and_expect(
        &mut subscriber,
        &encode_resp_command(&[b"AUTH", b"psuser", b"pspass"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut subscriber,
        &encode_resp_command(&[b"CLIENT", b"SETNAME", b"deathrow"]),
        b"+OK\r\n",
    )
    .await;
    let subscribe = send_and_read_resp_value(
        &mut subscriber,
        &encode_resp_command(&[b"SUBSCRIBE", b"foo:1"]),
        timeout,
    )
    .await;
    let subscribe_items = resp_socket_array(&subscribe);
    assert_eq!(resp_socket_bulk(&subscribe_items[0]), b"subscribe");
    assert_eq!(resp_socket_bulk(&subscribe_items[1]), b"foo:1");
    assert_eq!(resp_socket_integer(&subscribe_items[2]), 1);

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"ACL", b"SETUSER", b"psuser", b"resetchannels"]),
        b"+OK\r\n",
    )
    .await;
    let client_list_after_revoke = send_and_read_bulk_payload(
        &mut inspector,
        &encode_resp_command(&[b"CLIENT", b"LIST"]),
        timeout,
    )
    .await;
    let client_list_text = std::str::from_utf8(&client_list_after_revoke).unwrap();
    assert!(client_list_line_with_name(client_list_text, "deathrow").is_none());

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[
            b"ACL",
            b"SETUSER",
            b"psuser",
            b"resetchannels",
            b"&foo:1",
            b"&bar:*",
            b"&orders",
        ]),
        b"+OK\r\n",
    )
    .await;

    let mut pardoned = TcpStream::connect(addr).await.unwrap();
    send_and_expect(
        &mut pardoned,
        &encode_resp_command(&[b"AUTH", b"psuser", b"pspass"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut pardoned,
        &encode_resp_command(&[b"CLIENT", b"SETNAME", b"pardoned"]),
        b"+OK\r\n",
    )
    .await;
    let subscribe_one = send_and_read_resp_value(
        &mut pardoned,
        &encode_resp_command(&[b"SUBSCRIBE", b"foo:1"]),
        timeout,
    )
    .await;
    assert_eq!(
        resp_socket_bulk(&resp_socket_array(&subscribe_one)[1]),
        b"foo:1"
    );
    let subscribe_two = send_and_read_resp_value(
        &mut pardoned,
        &encode_resp_command(&[b"SSUBSCRIBE", b"orders"]),
        timeout,
    )
    .await;
    assert_eq!(
        resp_socket_bulk(&resp_socket_array(&subscribe_two)[1]),
        b"orders"
    );
    let subscribe_three = send_and_read_resp_value(
        &mut pardoned,
        &encode_resp_command(&[b"PSUBSCRIBE", b"bar:*"]),
        timeout,
    )
    .await;
    assert_eq!(
        resp_socket_bulk(&resp_socket_array(&subscribe_three)[1]),
        b"bar:*"
    );

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[
            b"ACL",
            b"SETUSER",
            b"psuser",
            b"resetchannels",
            b"&foo:1",
            b"&bar:*",
            b"&orders",
            b"&baz:qaz",
            b"&zoo:*",
        ]),
        b"+OK\r\n",
    )
    .await;
    let client_list_pardoned = send_and_read_bulk_payload(
        &mut inspector,
        &encode_resp_command(&[b"CLIENT", b"LIST"]),
        timeout,
    )
    .await;
    let client_list_pardoned_text = std::str::from_utf8(&client_list_pardoned).unwrap();
    assert!(client_list_line_with_name(client_list_pardoned_text, "pardoned").is_some());

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"ACL", b"SETUSER", b"psuser", b"allchannels"]),
        b"+OK\r\n",
    )
    .await;
    let client_list_allchannels = send_and_read_bulk_payload(
        &mut inspector,
        &encode_resp_command(&[b"CLIENT", b"LIST"]),
        timeout,
    )
    .await;
    let client_list_allchannels_text = std::str::from_utf8(&client_list_allchannels).unwrap();
    assert!(client_list_line_with_name(client_list_allchannels_text, "pardoned").is_some());

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"AUTH", b"default", b"pass"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"CONFIG", b"RESETSTAT"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut admin,
        &encode_resp_command(&[
            b"ACL", b"SETUSER", b"psuser", b"reset", b"on", b"nopass", b"+@all", b"allkeys",
        ]),
        b"+OK\r\n",
    )
    .await;

    let mut blocked = TcpStream::connect(addr).await.unwrap();
    send_and_expect(
        &mut blocked,
        &encode_resp_command(&[b"AUTH", b"psuser", b"pspass"]),
        b"+OK\r\n",
    )
    .await;
    blocked
        .write_all(&encode_resp_command(&[b"BLPOP", b"list1", b"0"]))
        .await
        .unwrap();
    wait_for_blocked_clients(&mut inspector, 1, Duration::from_secs(1)).await;

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"ACL", b"SETUSER", b"psuser", b"resetkeys"]),
        b"+OK\r\n",
    )
    .await;
    assert_eq!(
        send_and_read_integer(
            &mut admin,
            &encode_resp_command(&[b"LPUSH", b"list1", b"foo"]),
            timeout,
        )
        .await,
        1
    );
    let blocked_error = read_resp_value_with_timeout(&mut blocked, timeout).await;
    match blocked_error {
        RespSocketValue::Error(message) => {
            let text = String::from_utf8(message).unwrap();
            assert!(text.contains("NOPERM"));
            assert!(text.contains("No permissions to access a key"));
        }
        other => panic!("expected BLPOP NOPERM after ACL change, got {other:?}"),
    }
    send_and_expect(&mut blocked, &encode_resp_command(&[b"PING"]), b"+PONG\r\n").await;

    let commandstats = send_and_read_bulk_payload(
        &mut admin,
        &encode_resp_command(&[b"INFO", b"commandstats"]),
        timeout,
    )
    .await;
    let commandstats_text = String::from_utf8_lossy(&commandstats);
    assert!(
        commandstats_text.contains("cmdstat_blpop:"),
        "BLPOP stats should be present after ACL recheck rejection: {commandstats_text}"
    );
    assert!(
        commandstats_text.contains("rejected_calls=1"),
        "BLPOP rejection should be tracked after ACL recheck: {commandstats_text}"
    );
    assert!(
        commandstats_text.contains("failed_calls=0"),
        "BLPOP ACL rejection should not count as failed_calls: {commandstats_text}"
    );

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn acl_channel_matching_and_shard_subscriptions_match_external_scenarios() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let timeout = Duration::from_secs(5);
    let mut admin = TcpStream::connect(addr).await.unwrap();

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[
            b"ACL",
            b"SETUSER",
            b"hpuser",
            b"on",
            b"nopass",
            b"resetchannels",
            b"&foo",
            b"+@all",
        ]),
        b"+OK\r\n",
    )
    .await;
    let acl_list = send_and_read_resp_value(
        &mut admin,
        &encode_resp_command(&[b"ACL", b"LIST"]),
        timeout,
    )
    .await;
    let acl_list_lines = resp_socket_array(&acl_list)
        .iter()
        .map(resp_socket_bulk)
        .collect::<Vec<_>>();
    assert!(
        acl_list_lines.iter().any(|line| {
            *line == b"user hpuser on nopass sanitize-payload resetchannels &foo +@all"
        }),
        "ACL LIST should omit alldbs for root all-database access: {acl_list_lines:?}"
    );

    let mut hpuser = TcpStream::connect(addr).await.unwrap();
    send_and_expect(
        &mut hpuser,
        &encode_resp_command(&[b"AUTH", b"hpuser", b"pass"]),
        b"+OK\r\n",
    )
    .await;
    assert_eq!(
        send_and_read_integer(
            &mut hpuser,
            &encode_resp_command(&[b"PUBLISH", b"foo", b"bar"]),
            timeout,
        )
        .await,
        0
    );
    let denied_publish = send_and_read_error_line(
        &mut hpuser,
        &encode_resp_command(&[b"PUBLISH", b"bar", b"game"]),
        timeout,
    )
    .await;
    assert!(denied_publish.contains("NOPERM"));
    assert!(denied_publish.contains("channel"));

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[
            b"ACL",
            b"SETUSER",
            b"psuser",
            b"reset",
            b"on",
            b">pspass",
            b"+acl",
            b"+client",
            b"+@pubsub",
            b"resetchannels",
            b"&foo:1",
            b"&bar:*",
        ]),
        b"+OK\r\n",
    )
    .await;

    let mut shard_subscriber = TcpStream::connect(addr).await.unwrap();
    send_and_expect(
        &mut shard_subscriber,
        &encode_resp_command(&[b"AUTH", b"psuser", b"pspass"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut shard_subscriber,
        &encode_resp_command(&[b"CLIENT", b"SETNAME", b"deathrow"]),
        b"+OK\r\n",
    )
    .await;
    let shard_ack_one = send_and_read_resp_value(
        &mut shard_subscriber,
        &encode_resp_command(&[b"SSUBSCRIBE", b"foo:1"]),
        timeout,
    )
    .await;
    let shard_ack_one_items = resp_socket_array(&shard_ack_one);
    assert_eq!(resp_socket_bulk(&shard_ack_one_items[0]), b"ssubscribe");
    assert_eq!(resp_socket_bulk(&shard_ack_one_items[1]), b"foo:1");
    assert_eq!(resp_socket_integer(&shard_ack_one_items[2]), 1);
    let shard_ack_two = send_and_read_resp_value(
        &mut shard_subscriber,
        &encode_resp_command(&[b"SSUBSCRIBE", b"bar:2"]),
        timeout,
    )
    .await;
    let shard_ack_two_items = resp_socket_array(&shard_ack_two);
    assert_eq!(resp_socket_bulk(&shard_ack_two_items[1]), b"bar:2");
    assert_eq!(resp_socket_integer(&shard_ack_two_items[2]), 2);

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"ACL", b"SETUSER", b"psuser", b"resetchannels"]),
        b"+OK\r\n",
    )
    .await;
    let client_list_after_shard_revoke = send_and_read_bulk_payload(
        &mut admin,
        &encode_resp_command(&[b"CLIENT", b"LIST"]),
        timeout,
    )
    .await;
    let client_list_after_shard_revoke =
        std::str::from_utf8(&client_list_after_shard_revoke).unwrap();
    assert!(client_list_line_with_name(client_list_after_shard_revoke, "deathrow").is_none());

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[
            b"ACL",
            b"SETUSER",
            b"psuser",
            b"reset",
            b"on",
            b">pspass",
            b"+acl",
            b"+client",
            b"+@pubsub",
            b"resetchannels",
            b"&foo:1",
            b"&bar:*",
        ]),
        b"+OK\r\n",
    )
    .await;

    let mut pattern_subscriber = TcpStream::connect(addr).await.unwrap();
    send_and_expect(
        &mut pattern_subscriber,
        &encode_resp_command(&[b"AUTH", b"psuser", b"pspass"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut pattern_subscriber,
        &encode_resp_command(&[b"CLIENT", b"SETNAME", b"patternrow"]),
        b"+OK\r\n",
    )
    .await;
    let pattern_ack_one = send_and_read_resp_value(
        &mut pattern_subscriber,
        &encode_resp_command(&[b"PSUBSCRIBE", b"foo:1"]),
        timeout,
    )
    .await;
    let pattern_ack_one_items = resp_socket_array(&pattern_ack_one);
    assert_eq!(resp_socket_bulk(&pattern_ack_one_items[0]), b"psubscribe");
    assert_eq!(resp_socket_bulk(&pattern_ack_one_items[1]), b"foo:1");
    assert_eq!(resp_socket_integer(&pattern_ack_one_items[2]), 1);
    let pattern_ack_two = send_and_read_resp_value(
        &mut pattern_subscriber,
        &encode_resp_command(&[b"PSUBSCRIBE", b"bar:*"]),
        timeout,
    )
    .await;
    let pattern_ack_two_items = resp_socket_array(&pattern_ack_two);
    assert_eq!(resp_socket_bulk(&pattern_ack_two_items[1]), b"bar:*");
    assert_eq!(resp_socket_integer(&pattern_ack_two_items[2]), 2);
    let denied_pattern = send_and_read_error_line(
        &mut pattern_subscriber,
        &encode_resp_command(&[b"PSUBSCRIBE", b"bar:baz"]),
        timeout,
    )
    .await;
    assert!(denied_pattern.contains("NOPERM"));
    assert!(denied_pattern.contains("channel"));

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"ACL", b"SETUSER", b"psuser", b"resetchannels"]),
        b"+OK\r\n",
    )
    .await;
    let client_list_after_pattern_revoke = send_and_read_bulk_payload(
        &mut admin,
        &encode_resp_command(&[b"CLIENT", b"LIST"]),
        timeout,
    )
    .await;
    let client_list_after_pattern_revoke =
        std::str::from_utf8(&client_list_after_pattern_revoke).unwrap();
    assert!(client_list_line_with_name(client_list_after_pattern_revoke, "patternrow").is_none());

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn acl_log_tracks_auth_and_acl_denials_like_external_scenarios() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let timeout = Duration::from_secs(5);
    let mut admin = TcpStream::connect(addr).await.unwrap();
    let mut user = TcpStream::connect(addr).await.unwrap();

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[
            b"ACL",
            b"SETUSER",
            b"antirez",
            b"reset",
            b"on",
            b">foo",
            b"+set",
            b"+publish",
            b"+multi",
            b"+exec",
            b"~object:1234",
            b"resetchannels",
        ]),
        b"+OK\r\n",
    )
    .await;

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"ACL", b"LOG", b"RESET"]),
        b"+OK\r\n",
    )
    .await;
    let wrongpass = send_and_read_error_line(
        &mut user,
        &encode_resp_command(&[b"AUTH", b"antirez", b"doo"]),
        timeout,
    )
    .await;
    assert!(wrongpass.contains("WRONGPASS"));

    let auth_log = send_and_read_resp_value(
        &mut admin,
        &encode_resp_command(&[b"ACL", b"LOG", b"1"]),
        timeout,
    )
    .await;
    let auth_entries = resp_socket_array(&auth_log);
    assert_eq!(auth_entries.len(), 1);
    let auth_entry = resp_socket_map_or_flat_map(&auth_entries[0]);
    assert_eq!(resp_socket_bulk(auth_entry[&b"reason".to_vec()]), b"auth");
    assert_eq!(
        resp_socket_bulk(auth_entry[&b"context".to_vec()]),
        b"toplevel"
    );
    assert_eq!(resp_socket_bulk(auth_entry[&b"object".to_vec()]), b"AUTH");
    assert_eq!(
        resp_socket_bulk(auth_entry[&b"username".to_vec()]),
        b"antirez"
    );
    assert!(
        resp_socket_integer(auth_entry[&b"timestamp-created".to_vec()]) > 0,
        "auth log should expose creation time"
    );

    send_and_expect(
        &mut user,
        &encode_resp_command(&[b"AUTH", b"antirez", b"foo"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"ACL", b"LOG", b"RESET"]),
        b"+OK\r\n",
    )
    .await;

    for _ in 0..3 {
        let denied_command =
            send_and_read_error_line(&mut user, &encode_resp_command(&[b"GET", b"foo"]), timeout)
                .await;
        assert!(denied_command.contains("NOPERM"));
        assert!(denied_command.contains("get"));
    }

    let command_log = send_and_read_resp_value(
        &mut admin,
        &encode_resp_command(&[b"ACL", b"LOG", b"1"]),
        timeout,
    )
    .await;
    let command_entry = resp_socket_map_or_flat_map(&resp_socket_array(&command_log)[0]);
    assert_eq!(
        resp_socket_bulk(command_entry[&b"reason".to_vec()]),
        b"command"
    );
    assert_eq!(
        resp_socket_bulk(command_entry[&b"context".to_vec()]),
        b"toplevel"
    );
    assert_eq!(resp_socket_bulk(command_entry[&b"object".to_vec()]), b"get");
    assert_eq!(
        resp_socket_bulk(command_entry[&b"username".to_vec()]),
        b"antirez"
    );
    assert_eq!(resp_socket_integer(command_entry[&b"count".to_vec()]), 3);
    assert!(
        String::from_utf8_lossy(resp_socket_bulk(command_entry[&b"client-info".to_vec()]))
            .contains("cmd=get"),
        "client-info should include the denied command"
    );

    let denied_key = send_and_read_error_line(
        &mut user,
        &encode_resp_command(&[b"SET", b"somekeynotallowed", b"1234"]),
        timeout,
    )
    .await;
    assert!(denied_key.contains("NOPERM"));
    assert!(denied_key.contains("key"));

    let denied_channel = send_and_read_error_line(
        &mut user,
        &encode_resp_command(&[b"PUBLISH", b"somechannelnotallowed", b"nullmsg"]),
        timeout,
    )
    .await;
    assert!(denied_channel.contains("NOPERM"));
    assert!(denied_channel.contains("channel"));

    let hello =
        send_and_read_resp_value(&mut admin, &encode_resp_command(&[b"HELLO", b"3"]), timeout)
            .await;
    assert!(
        matches!(hello, RespSocketValue::Map(_) | RespSocketValue::Array(_)),
        "HELLO 3 should return server info, got {hello:?}"
    );

    let resp3_log = send_and_read_resp_value(
        &mut admin,
        &encode_resp_command(&[b"ACL", b"LOG", b"2"]),
        timeout,
    )
    .await;
    let resp3_entries = resp_socket_array(&resp3_log);
    assert_eq!(resp3_entries.len(), 2);

    let channel_entry = resp_socket_map_or_flat_map(&resp3_entries[0]);
    assert_eq!(
        resp_socket_bulk(channel_entry[&b"reason".to_vec()]),
        b"channel"
    );
    assert_eq!(
        resp_socket_bulk(channel_entry[&b"object".to_vec()]),
        b"somechannelnotallowed"
    );
    assert!(matches!(
        channel_entry[&b"age-seconds".to_vec()],
        RespSocketValue::Double(_)
    ));

    let key_entry = resp_socket_map_or_flat_map(&resp3_entries[1]);
    assert_eq!(resp_socket_bulk(key_entry[&b"reason".to_vec()]), b"key");
    assert_eq!(
        resp_socket_bulk(key_entry[&b"object".to_vec()]),
        b"somekeynotallowed"
    );

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"ACL", b"LOG", b"RESET"]),
        b"+OK\r\n",
    )
    .await;
    let empty_after_reset =
        send_and_read_resp_value(&mut admin, &encode_resp_command(&[b"ACL", b"LOG"]), timeout)
            .await;
    assert!(resp_socket_array(&empty_after_reset).is_empty());

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn acl_log_distinguishes_multi_context_like_external_scenario() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let timeout = Duration::from_secs(5);
    let mut admin = TcpStream::connect(addr).await.unwrap();
    let mut user = TcpStream::connect(addr).await.unwrap();

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[
            b"ACL", b"SETUSER", b"antirez", b"reset", b"on", b">foo", b"+multi", b"+exec",
            b"allkeys",
        ]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"ACL", b"LOG", b"RESET"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut user,
        &encode_resp_command(&[b"AUTH", b"antirez", b"foo"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(&mut user, &encode_resp_command(&[b"MULTI"]), b"+OK\r\n").await;
    let denied_incr =
        send_and_read_error_line(&mut user, &encode_resp_command(&[b"INCR", b"foo"]), timeout)
            .await;
    assert!(denied_incr.contains("NOPERM"));
    assert!(denied_incr.contains("incr"));
    let exec_error =
        send_and_read_error_line(&mut user, &encode_resp_command(&[b"EXEC"]), timeout).await;
    assert!(exec_error.contains("EXECABORT"));

    let multi_log = send_and_read_resp_value(
        &mut admin,
        &encode_resp_command(&[b"ACL", b"LOG", b"1"]),
        timeout,
    )
    .await;
    let multi_entry = resp_socket_map_or_flat_map(&resp_socket_array(&multi_log)[0]);
    assert_eq!(
        resp_socket_bulk(multi_entry[&b"reason".to_vec()]),
        b"command"
    );
    assert_eq!(
        resp_socket_bulk(multi_entry[&b"context".to_vec()]),
        b"multi"
    );
    assert_eq!(resp_socket_bulk(multi_entry[&b"object".to_vec()]), b"incr");

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"ACL", b"LOG", b"RESET"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut admin,
        &encode_resp_command(&[
            b"ACL", b"SETUSER", b"antirez", b"reset", b"on", b">foo", b"+multi", b"+exec",
            b"+incr", b"allkeys",
        ]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut user,
        &encode_resp_command(&[b"AUTH", b"antirez", b"foo"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(&mut user, &encode_resp_command(&[b"MULTI"]), b"+OK\r\n").await;
    send_and_expect(
        &mut user,
        &encode_resp_command(&[b"INCR", b"object:1234"]),
        b"+QUEUED\r\n",
    )
    .await;
    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"ACL", b"SETUSER", b"antirez", b"-incr"]),
        b"+OK\r\n",
    )
    .await;

    let exec = send_and_read_resp_value(&mut user, &encode_resp_command(&[b"EXEC"]), timeout).await;
    let exec_items = resp_socket_array(&exec);
    assert_eq!(exec_items.len(), 1);
    assert!(
        String::from_utf8_lossy(resp_socket_bulk(&exec_items[0])).contains("NOPERM"),
        "EXEC should re-check ACLs at execution time: {exec:?}"
    );

    let get_after_exec = send_and_read_resp_value(
        &mut admin,
        &encode_resp_command(&[b"GET", b"object:1234"]),
        timeout,
    )
    .await;
    assert!(
        matches!(get_after_exec, RespSocketValue::Null),
        "revoked INCR should not execute at EXEC time: {get_after_exec:?}"
    );

    let exec_log = send_and_read_resp_value(
        &mut admin,
        &encode_resp_command(&[b"ACL", b"LOG", b"1"]),
        timeout,
    )
    .await;
    let exec_entry = resp_socket_map_or_flat_map(&resp_socket_array(&exec_log)[0]);
    assert_eq!(resp_socket_bulk(exec_entry[&b"context".to_vec()]), b"multi");
    assert_eq!(resp_socket_bulk(exec_entry[&b"object".to_vec()]), b"incr");
    assert!(
        String::from_utf8_lossy(resp_socket_bulk(exec_entry[&b"client-info".to_vec()]))
            .contains("cmd=exec"),
        "EXEC-time ACL log should keep EXEC in client-info"
    );

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn acl_database_permissions_and_log_match_external_scenarios() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let timeout = Duration::from_secs(5);
    let mut admin = TcpStream::connect(addr).await.unwrap();
    let mut user = TcpStream::connect(addr).await.unwrap();

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[
            b"ACL", b"SETUSER", b"dbuser", b"reset", b"on", b"nopass", b"db=0", b"+@all", b"~*",
        ]),
        b"+OK\r\n",
    )
    .await;

    let getuser = send_and_read_resp_value(
        &mut admin,
        &encode_resp_command(&[b"ACL", b"GETUSER", b"dbuser"]),
        timeout,
    )
    .await;
    assert!(
        resp_socket_contains_bulk(&getuser, b"databases"),
        "ACL GETUSER should expose the databases field"
    );
    assert!(
        resp_socket_contains_bulk(&getuser, b"db=0"),
        "ACL GETUSER should expose db=0"
    );

    send_and_expect(
        &mut user,
        &encode_resp_command(&[b"AUTH", b"dbuser", b"password"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"ACL", b"LOG", b"RESET"]),
        b"+OK\r\n",
    )
    .await;

    let denied_select =
        send_and_read_error_line(&mut user, &encode_resp_command(&[b"SELECT", b"1"]), timeout)
            .await;
    assert!(denied_select.contains("NOPERM"));
    assert!(denied_select.contains("database"));

    let denied_swapdb = send_and_read_error_line(
        &mut user,
        &encode_resp_command(&[b"SWAPDB", b"0", b"2"]),
        timeout,
    )
    .await;
    assert!(denied_swapdb.contains("NOPERM"));
    assert!(denied_swapdb.contains("database"));

    let denied_flushall =
        send_and_read_error_line(&mut user, &encode_resp_command(&[b"FLUSHALL"]), timeout).await;
    assert!(denied_flushall.contains("NOPERM"));
    assert!(denied_flushall.contains("database"));

    let acl_log =
        send_and_read_resp_value(&mut admin, &encode_resp_command(&[b"ACL", b"LOG"]), timeout)
            .await;
    let entries = resp_socket_array(&acl_log);
    assert_eq!(entries.len(), 3);

    let flushall_entry = resp_socket_map_or_flat_map(&entries[0]);
    assert_eq!(
        resp_socket_bulk(flushall_entry[&b"reason".to_vec()]),
        b"database"
    );
    assert_eq!(
        resp_socket_bulk(flushall_entry[&b"object".to_vec()]),
        b"flushall"
    );

    let swapdb_entry = resp_socket_map_or_flat_map(&entries[1]);
    assert_eq!(
        resp_socket_bulk(swapdb_entry[&b"reason".to_vec()]),
        b"database"
    );
    assert_eq!(resp_socket_bulk(swapdb_entry[&b"object".to_vec()]), b"2");

    let select_entry = resp_socket_map_or_flat_map(&entries[2]);
    assert_eq!(
        resp_socket_bulk(select_entry[&b"reason".to_vec()]),
        b"database"
    );
    assert_eq!(resp_socket_bulk(select_entry[&b"object".to_vec()]), b"1");

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"ACL", b"SETUSER", b"dbuser", b"resetdbs", b"db=0,1"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut user,
        &encode_resp_command(&[b"SELECT", b"1"]),
        b"+OK\r\n",
    )
    .await;
    let denied_select_again =
        send_and_read_error_line(&mut user, &encode_resp_command(&[b"SELECT", b"2"]), timeout)
            .await;
    assert!(denied_select_again.contains("NOPERM"));
    assert!(denied_select_again.contains("database"));

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"ACL", b"SETUSER", b"dbuser", b"alldbs"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(&mut user, &encode_resp_command(&[b"FLUSHALL"]), b"+OK\r\n").await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn acl_log_max_len_config_matches_external_scenarios() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let timeout = Duration::from_secs(5);
    let mut admin = TcpStream::connect(addr).await.unwrap();
    let mut user = TcpStream::connect(addr).await.unwrap();

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[
            b"ACL", b"SETUSER", b"antirez", b"reset", b"on", b">foo", b"+set",
        ]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut user,
        &encode_resp_command(&[b"AUTH", b"antirez", b"foo"]),
        b"+OK\r\n",
    )
    .await;

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"ACL", b"LOG", b"RESET"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"CONFIG", b"SET", b"acllog-max-len", b"0"]),
        b"+OK\r\n",
    )
    .await;
    for index in 0..10 {
        let key = format!("obj:{index}");
        let denied = send_and_read_error_line(
            &mut user,
            &encode_resp_command(&[b"SET", key.as_bytes(), b"123"]),
            timeout,
        )
        .await;
        assert!(denied.contains("NOPERM"));
    }
    let zero_log =
        send_and_read_resp_value(&mut admin, &encode_resp_command(&[b"ACL", b"LOG"]), timeout)
            .await;
    assert_eq!(resp_socket_array(&zero_log).len(), 0);

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"ACL", b"LOG", b"RESET"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"CONFIG", b"SET", b"acllog-max-len", b"5"]),
        b"+OK\r\n",
    )
    .await;
    for index in 0..10 {
        let key = format!("obj:{index}");
        let denied = send_and_read_error_line(
            &mut user,
            &encode_resp_command(&[b"SET", key.as_bytes(), b"123"]),
            timeout,
        )
        .await;
        assert!(denied.contains("NOPERM"));
    }
    let limited_log =
        send_and_read_resp_value(&mut admin, &encode_resp_command(&[b"ACL", b"LOG"]), timeout)
            .await;
    assert_eq!(resp_socket_array(&limited_log).len(), 5);

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"CONFIG", b"SET", b"acllog-max-len", b"0"]),
        b"+OK\r\n",
    )
    .await;
    let preserved_log =
        send_and_read_resp_value(&mut admin, &encode_resp_command(&[b"ACL", b"LOG"]), timeout)
            .await;
    assert_eq!(resp_socket_array(&preserved_log).len(), 5);

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn acl_selectors_match_valkey_semantics_for_or_permissions_and_rendering() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let timeout = Duration::from_secs(5);
    let mut admin = TcpStream::connect(addr).await.unwrap();
    let mut user = TcpStream::connect(addr).await.unwrap();

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"SELECT", b"1"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"SET", b"foo:1", b"db1-value"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"SELECT", b"0"]),
        b"+OK\r\n",
    )
    .await;

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[
            b"ACL",
            b"SETUSER",
            b"selector",
            b"reset",
            b"on",
            b">pw",
            b"+set",
            b"+select|0",
            b"~root:*",
            b"db=0",
            b"(+get",
            b"+select|1",
            b"~foo:*",
            b"db=1)",
            b"(+publish",
            b"resetchannels",
            b"&alerts:*",
            b"alldbs)",
        ]),
        b"+OK\r\n",
    )
    .await;

    let getuser = send_and_read_resp_value(
        &mut admin,
        &encode_resp_command(&[b"ACL", b"GETUSER", b"selector"]),
        timeout,
    )
    .await;
    let getuser_map = resp_socket_map_or_flat_map(&getuser);
    let selectors = resp_socket_array(getuser_map[&b"selectors".to_vec()]);
    assert_eq!(selectors.len(), 2);

    let selector_one = resp_socket_map_or_flat_map(&selectors[0]);
    assert_eq!(
        resp_socket_bulk(selector_one[&b"commands".to_vec()]),
        b"-@all +get +select|1"
    );
    assert_eq!(resp_socket_bulk(selector_one[&b"keys".to_vec()]), b"~foo:*");
    assert_eq!(resp_socket_bulk(selector_one[&b"channels".to_vec()]), b"");
    assert_eq!(
        resp_socket_bulk(selector_one[&b"databases".to_vec()]),
        b"db=1"
    );

    let selector_two = resp_socket_map_or_flat_map(&selectors[1]);
    assert_eq!(
        resp_socket_bulk(selector_two[&b"commands".to_vec()]),
        b"-@all +publish"
    );
    assert_eq!(resp_socket_bulk(selector_two[&b"keys".to_vec()]), b"");
    assert_eq!(
        resp_socket_bulk(selector_two[&b"channels".to_vec()]),
        b"&alerts:*"
    );
    assert_eq!(
        resp_socket_bulk(selector_two[&b"databases".to_vec()]),
        b"alldbs"
    );

    let acl_list = send_and_read_resp_value(
        &mut admin,
        &encode_resp_command(&[b"ACL", b"LIST"]),
        timeout,
    )
    .await;
    let acl_list_lines = resp_socket_array(&acl_list)
        .iter()
        .map(resp_socket_bulk)
        .collect::<Vec<_>>();
    let acl_list_text = acl_list_lines
        .iter()
        .map(|line| String::from_utf8_lossy(line))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(acl_list_text.contains("(~foo:* resetchannels db=1 -@all +get +select|1)"));
    assert!(acl_list_text.contains("(resetchannels &alerts:* alldbs -@all +publish)"));

    send_and_expect(
        &mut user,
        &encode_resp_command(&[b"AUTH", b"selector", b"pw"]),
        b"+OK\r\n",
    )
    .await;

    send_and_expect(
        &mut user,
        &encode_resp_command(&[b"SET", b"root:1", b"root-value"]),
        b"+OK\r\n",
    )
    .await;

    let denied_get_root = send_and_read_error_line(
        &mut user,
        &encode_resp_command(&[b"GET", b"root:1"]),
        timeout,
    )
    .await;
    assert!(denied_get_root.contains("NOPERM"));
    assert!(denied_get_root.contains("get"));

    send_and_expect(
        &mut user,
        &encode_resp_command(&[b"SELECT", b"1"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut user,
        &encode_resp_command(&[b"GET", b"foo:1"]),
        b"$9\r\ndb1-value\r\n",
    )
    .await;

    let denied_set_db1 = send_and_read_error_line(
        &mut user,
        &encode_resp_command(&[b"SET", b"foo:1", b"nope"]),
        timeout,
    )
    .await;
    assert!(denied_set_db1.contains("NOPERM"));
    assert!(denied_set_db1.contains("set"));

    assert_eq!(
        send_and_read_integer(
            &mut user,
            &encode_resp_command(&[b"PUBLISH", b"alerts:1", b"hello"]),
            timeout,
        )
        .await,
        0
    );

    let denied_publish = send_and_read_error_line(
        &mut user,
        &encode_resp_command(&[b"PUBLISH", b"news:1", b"hello"]),
        timeout,
    )
    .await;
    assert!(denied_publish.contains("NOPERM"));
    assert!(denied_publish.contains("channel"));

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"ACL", b"SETUSER", b"selector", b"clearselectors"]),
        b"+OK\r\n",
    )
    .await;
    let getuser_after_clear = send_and_read_resp_value(
        &mut admin,
        &encode_resp_command(&[b"ACL", b"GETUSER", b"selector"]),
        timeout,
    )
    .await;
    let getuser_after_clear_map = resp_socket_map_or_flat_map(&getuser_after_clear);
    assert!(resp_socket_array(getuser_after_clear_map[&b"selectors".to_vec()]).is_empty());

    send_and_expect(
        &mut user,
        &encode_resp_command(&[b"SELECT", b"0"]),
        b"+OK\r\n",
    )
    .await;
    let denied_select_after_clear =
        send_and_read_error_line(&mut user, &encode_resp_command(&[b"SELECT", b"1"]), timeout)
            .await;
    assert!(denied_select_after_clear.contains("NOPERM"));
    assert!(denied_select_after_clear.contains("select|1"));

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn acl_v2_key_permissions_and_rendering_match_external_scenarios() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let timeout = Duration::from_secs(5);
    let mut admin = TcpStream::connect(addr).await.unwrap();
    let mut user = TcpStream::connect(addr).await.unwrap();

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[
            b"ACL",
            b"SETUSER",
            b"command-test",
            b"on",
            b"nopass",
            b"+@all",
            b"%R~read*",
            b"%W~write*",
            b"%RW~rw*",
        ]),
        b"+OK\r\n",
    )
    .await;

    let getuser = send_and_read_resp_value(
        &mut admin,
        &encode_resp_command(&[b"ACL", b"GETUSER", b"command-test"]),
        timeout,
    )
    .await;
    let getuser_map = resp_socket_map_or_flat_map(&getuser);
    assert_eq!(
        resp_socket_bulk(getuser_map[&b"keys".to_vec()]),
        b"%R~read* %W~write* ~rw*"
    );

    let acl_list = send_and_read_resp_value(
        &mut admin,
        &encode_resp_command(&[b"ACL", b"LIST"]),
        timeout,
    )
    .await;
    let acl_list_text = resp_socket_array(&acl_list)
        .iter()
        .map(resp_socket_bulk)
        .map(String::from_utf8_lossy)
        .collect::<Vec<_>>()
        .join("\n");
    assert!(acl_list_text.contains("%R~read* %W~write* ~rw*"));

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"SET", b"readstr", b"bar"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"SET", b"read", b"copy-source"]),
        b"+OK\r\n",
    )
    .await;

    send_and_expect(
        &mut user,
        &encode_resp_command(&[b"AUTH", b"command-test", b"password"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(&mut user, &encode_resp_command(&[b"PING"]), b"+PONG\r\n").await;
    send_and_expect(
        &mut user,
        &encode_resp_command(&[b"GET", b"readstr"]),
        b"$3\r\nbar\r\n",
    )
    .await;

    let denied_set = send_and_read_error_line(
        &mut user,
        &encode_resp_command(&[b"SET", b"readstr", b"bar"]),
        timeout,
    )
    .await;
    assert!(denied_set.contains("NOPERM"));
    assert!(denied_set.contains("key"));

    assert_eq!(
        send_and_read_integer(
            &mut user,
            &encode_resp_command(&[b"LPUSH", b"writelist", b"10"]),
            timeout,
        )
        .await,
        1
    );

    let denied_get = send_and_read_error_line(
        &mut user,
        &encode_resp_command(&[b"GET", b"writestr"]),
        timeout,
    )
    .await;
    assert!(denied_get.contains("NOPERM"));
    assert!(denied_get.contains("key"));

    assert_eq!(
        send_and_read_integer(
            &mut user,
            &encode_resp_command(&[b"COPY", b"read", b"write"]),
            timeout,
        )
        .await,
        1
    );

    let denied_copy = send_and_read_error_line(
        &mut user,
        &encode_resp_command(&[b"COPY", b"write", b"read"]),
        timeout,
    )
    .await;
    assert!(denied_copy.contains("NOPERM"));
    assert!(denied_copy.contains("key"));

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn acl_v2_dryrun_key_permissions_match_external_scenarios() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let timeout = Duration::from_secs(5);
    let mut admin = TcpStream::connect(addr).await.unwrap();

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[
            b"ACL",
            b"SETUSER",
            b"command-test",
            b"on",
            b"nopass",
            b"+@all",
            b"%R~read*",
            b"%W~write*",
            b"%RW~rw*",
        ]),
        b"+OK\r\n",
    )
    .await;

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"ACL", b"DRYRUN", b"command-test", b"GET", b"read"]),
        b"+OK\r\n",
    )
    .await;

    let missing_user = send_and_read_error_line(
        &mut admin,
        &encode_resp_command(&[b"ACL", b"DRYRUN", b"not-a-user", b"GET", b"read"]),
        timeout,
    )
    .await;
    assert_eq!(missing_user, "ERR User 'not-a-user' not found");

    let missing_command = send_and_read_error_line(
        &mut admin,
        &encode_resp_command(&[
            b"ACL",
            b"DRYRUN",
            b"command-test",
            b"not-a-command",
            b"read",
        ]),
        timeout,
    )
    .await;
    assert_eq!(missing_command, "ERR Command 'not-a-command' not found");

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[
            b"ACL",
            b"SETUSER",
            b"test-dry-run",
            b"on",
            b"nopass",
            b"+@all",
            b"~v*",
        ]),
        b"+OK\r\n",
    )
    .await;
    let wrong_arity = send_and_read_error_line(
        &mut admin,
        &encode_resp_command(&[b"ACL", b"DRYRUN", b"test-dry-run", b"SET", b"v"]),
        timeout,
    )
    .await;
    assert_eq!(
        wrong_arity,
        "ERR wrong number of arguments for 'set' command"
    );

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[
            b"ACL",
            b"DRYRUN",
            b"command-test",
            b"MIGRATE",
            b"whatever",
            b"whatever",
            b"rw",
            b"0",
            b"5000",
        ]),
        b"+OK\r\n",
    )
    .await;
    let migrate_read_denied = send_and_read_resp_value(
        &mut admin,
        &encode_resp_command(&[
            b"ACL",
            b"DRYRUN",
            b"command-test",
            b"MIGRATE",
            b"whatever",
            b"whatever",
            b"read",
            b"0",
            b"5000",
        ]),
        timeout,
    )
    .await;
    assert!(
        String::from_utf8_lossy(resp_socket_bulk(&migrate_read_denied))
            .contains("access the 'read' key")
    );

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[
            b"ACL",
            b"DRYRUN",
            b"command-test",
            b"EVAL_RO",
            b"",
            b"1",
            b"read",
        ]),
        b"+OK\r\n",
    )
    .await;
    let eval_denied = send_and_read_resp_value(
        &mut admin,
        &encode_resp_command(&[
            b"ACL",
            b"DRYRUN",
            b"command-test",
            b"EVAL",
            b"",
            b"1",
            b"read",
        ]),
        timeout,
    )
    .await;
    assert!(
        String::from_utf8_lossy(resp_socket_bulk(&eval_denied)).contains("access the 'read' key")
    );

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[
            b"ACL",
            b"DRYRUN",
            b"command-test",
            b"SORT",
            b"read",
            b"STORE",
            b"write",
        ]),
        b"+OK\r\n",
    )
    .await;
    let sort_denied = send_and_read_resp_value(
        &mut admin,
        &encode_resp_command(&[
            b"ACL",
            b"DRYRUN",
            b"command-test",
            b"SORT",
            b"read",
            b"STORE",
            b"read",
        ]),
        timeout,
    )
    .await;
    assert!(
        String::from_utf8_lossy(resp_socket_bulk(&sort_denied)).contains("access the 'read' key")
    );

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[
            b"ACL",
            b"SETUSER",
            b"set-key-permission-W",
            b"on",
            b"nopass",
            b"%W~write*",
            b"+@all",
        ]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut admin,
        &encode_resp_command(&[
            b"ACL",
            b"DRYRUN",
            b"set-key-permission-W",
            b"SET",
            b"writestr",
            b"bar",
        ]),
        b"+OK\r\n",
    )
    .await;
    let set_get_denied = send_and_read_resp_value(
        &mut admin,
        &encode_resp_command(&[
            b"ACL",
            b"DRYRUN",
            b"set-key-permission-W",
            b"SET",
            b"writestr",
            b"bar",
            b"GET",
        ]),
        timeout,
    )
    .await;
    assert!(
        String::from_utf8_lossy(resp_socket_bulk(&set_get_denied))
            .contains("access the 'writestr' key")
    );
    let set_invalid_ex_denied = send_and_read_resp_value(
        &mut admin,
        &encode_resp_command(&[
            b"ACL",
            b"DRYRUN",
            b"set-key-permission-W",
            b"SET",
            b"writestr",
            b"bar",
            b"EX",
            b"get",
        ]),
        timeout,
    )
    .await;
    assert!(
        String::from_utf8_lossy(resp_socket_bulk(&set_invalid_ex_denied))
            .contains("access the 'writestr' key")
    );

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn acl_v2_sort_by_and_get_require_unrestricted_read_access_like_external_scenario() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let timeout = Duration::from_secs(5);
    let mut admin = TcpStream::connect(addr).await.unwrap();
    let mut user = TcpStream::connect(addr).await.unwrap();

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"SET", b"v1", b"1"]),
        b"+OK\r\n",
    )
    .await;
    assert_eq!(
        send_and_read_integer(
            &mut admin,
            &encode_resp_command(&[b"LPUSH", b"mylist", b"1"]),
            timeout,
        )
        .await,
        1
    );

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[
            b"ACL",
            b"SETUSER",
            b"test-sort-acl",
            b"on",
            b"nopass",
            b"(+sort",
            b"~mylist)",
        ]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut user,
        &encode_resp_command(&[b"AUTH", b"test-sort-acl", b"nopass"]),
        b"+OK\r\n",
    )
    .await;

    let by_denied = send_and_read_error_line(
        &mut user,
        &encode_resp_command(&[b"SORT", b"mylist", b"BY", b"v*"]),
        timeout,
    )
    .await;
    assert_eq!(
        by_denied,
        "ERR BY option of SORT denied due to insufficient ACL permissions."
    );
    let get_denied = send_and_read_error_line(
        &mut user,
        &encode_resp_command(&[b"SORT", b"mylist", b"GET", b"v*"]),
        timeout,
    )
    .await;
    assert_eq!(
        get_denied,
        "ERR GET option of SORT denied due to insufficient ACL permissions."
    );

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[
            b"ACL",
            b"SETUSER",
            b"test-sort-acl",
            b"(+sort",
            b"~mylist",
            b"~v*)",
        ]),
        b"+OK\r\n",
    )
    .await;
    let by_still_denied = send_and_read_error_line(
        &mut user,
        &encode_resp_command(&[b"SORT", b"mylist", b"BY", b"v*"]),
        timeout,
    )
    .await;
    assert_eq!(
        by_still_denied,
        "ERR BY option of SORT denied due to insufficient ACL permissions."
    );

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[
            b"ACL",
            b"SETUSER",
            b"test-sort-acl",
            b"(+sort",
            b"~mylist",
            b"%W~*)",
        ]),
        b"+OK\r\n",
    )
    .await;
    let get_write_only_denied = send_and_read_error_line(
        &mut user,
        &encode_resp_command(&[b"SORT", b"mylist", b"GET", b"v*"]),
        timeout,
    )
    .await;
    assert_eq!(
        get_write_only_denied,
        "ERR GET option of SORT denied due to insufficient ACL permissions."
    );

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[
            b"ACL",
            b"SETUSER",
            b"test-sort-acl",
            b"(+sort",
            b"~mylist",
            b"%R~*)",
        ]),
        b"+OK\r\n",
    )
    .await;
    let sorted = send_and_read_resp_value(
        &mut user,
        &encode_resp_command(&[b"SORT", b"mylist", b"BY", b"v*"]),
        timeout,
    )
    .await;
    let sorted_items = resp_socket_array(&sorted);
    assert_eq!(sorted_items.len(), 1);
    assert_eq!(resp_socket_bulk(&sorted_items[0]), b"1");

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn acl_v2_aclfile_selector_key_permissions_load_match_external_scenario() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let timeout = Duration::from_secs(5);
    let temp_dir = unique_test_temp_dir("acl-v2-file");
    let aclfile_path = temp_dir.join("userwithselectors.acl");
    std::fs::write(
        &aclfile_path,
        "user alice on (+get ~rw*)\nuser bob on (+set %W~w*) (+get %R~r*)\n",
    )
    .unwrap();

    let mut admin = TcpStream::connect(addr).await.unwrap();
    let dir_bytes = temp_dir.to_string_lossy().into_owned().into_bytes();

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"CONFIG", b"SET", b"dir", dir_bytes.as_slice()]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"CONFIG", b"SET", b"aclfile", b"userwithselectors.acl"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"ACL", b"LOAD"]),
        b"+OK\r\n",
    )
    .await;

    let alice = send_and_read_resp_value(
        &mut admin,
        &encode_resp_command(&[b"ACL", b"GETUSER", b"alice"]),
        timeout,
    )
    .await;
    let alice_map = resp_socket_map_or_flat_map(&alice);
    let alice_selectors = resp_socket_array(alice_map[&b"selectors".to_vec()]);
    assert_eq!(alice_selectors.len(), 1);
    let alice_selector = resp_socket_map_or_flat_map(&alice_selectors[0]);
    assert_eq!(
        resp_socket_bulk(alice_selector[&b"commands".to_vec()]),
        b"-@all +get"
    );
    assert_eq!(resp_socket_bulk(alice_selector[&b"keys".to_vec()]), b"~rw*");

    let bob = send_and_read_resp_value(
        &mut admin,
        &encode_resp_command(&[b"ACL", b"GETUSER", b"bob"]),
        timeout,
    )
    .await;
    let bob_map = resp_socket_map_or_flat_map(&bob);
    let bob_selectors = resp_socket_array(bob_map[&b"selectors".to_vec()]);
    assert_eq!(bob_selectors.len(), 2);
    let bob_writer = resp_socket_map_or_flat_map(&bob_selectors[0]);
    assert_eq!(
        resp_socket_bulk(bob_writer[&b"commands".to_vec()]),
        b"-@all +set"
    );
    assert_eq!(resp_socket_bulk(bob_writer[&b"keys".to_vec()]), b"%W~w*");
    let bob_reader = resp_socket_map_or_flat_map(&bob_selectors[1]);
    assert_eq!(
        resp_socket_bulk(bob_reader[&b"commands".to_vec()]),
        b"-@all +get"
    );
    assert_eq!(resp_socket_bulk(bob_reader[&b"keys".to_vec()]), b"%R~r*");

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn client_info_and_list_follow_selected_db_and_reset_to_zero() {
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
        b"*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$9\r\ndatabases\r\n$2\r\n16\r\n",
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*2\r\n$6\r\nSELECT\r\n$1\r\n1\r\n",
        b"+OK\r\n",
    )
    .await;

    let info_payload = send_and_read_bulk_payload(
        &mut client,
        b"*2\r\n$6\r\nCLIENT\r\n$4\r\nINFO\r\n",
        Duration::from_secs(1),
    )
    .await;
    assert!(
        info_payload.windows("db=1".len()).any(|w| w == b"db=1"),
        "CLIENT INFO should contain db=1 after SELECT 1: {}",
        String::from_utf8_lossy(&info_payload),
    );

    let client_id = send_and_read_integer(
        &mut client,
        b"*2\r\n$6\r\nCLIENT\r\n$2\r\nID\r\n",
        Duration::from_secs(1),
    )
    .await;
    let client_id_text = client_id.to_string();
    let client_list_frame =
        encode_resp_command(&[b"CLIENT", b"LIST", b"ID", client_id_text.as_bytes()]);
    let list_payload =
        send_and_read_bulk_payload(&mut client, &client_list_frame, Duration::from_secs(1)).await;
    assert!(
        list_payload.windows("db=1".len()).any(|w| w == b"db=1"),
        "CLIENT LIST should contain db=1 after SELECT 1: {}",
        String::from_utf8_lossy(&list_payload),
    );

    send_and_expect(&mut client, b"*1\r\n$5\r\nRESET\r\n", b"+RESET\r\n").await;
    let reset_info_payload = send_and_read_bulk_payload(
        &mut client,
        b"*2\r\n$6\r\nCLIENT\r\n$4\r\nINFO\r\n",
        Duration::from_secs(1),
    )
    .await;
    assert!(
        reset_info_payload
            .windows("db=0".len())
            .any(|w| w == b"db=0"),
        "CLIENT INFO should contain db=0 after RESET: {}",
        String::from_utf8_lossy(&reset_info_payload),
    );

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[test]
fn client_last_activity_only_tracks_input_and_output_not_internal_bookkeeping() {
    let metrics = ServerMetrics::default();
    let client_id = metrics.register_client(None, None);

    let initial_activity = metrics
        .clients
        .lock()
        .unwrap()
        .get(&client_id)
        .unwrap()
        .last_activity;

    std::thread::sleep(Duration::from_millis(2));
    metrics.add_client_input_bytes(client_id, 5);
    let after_input = metrics
        .clients
        .lock()
        .unwrap()
        .get(&client_id)
        .unwrap()
        .last_activity;
    assert!(after_input > initial_activity);

    std::thread::sleep(Duration::from_millis(2));
    metrics.set_client_last_command(client_id, b"GET", None);
    metrics.add_client_commands_processed(client_id, 1);
    metrics.set_client_wait_target_offset(client_id, 11);
    metrics.set_client_local_aof_wait_target_offset(client_id, 22);
    let after_bookkeeping = metrics
        .clients
        .lock()
        .unwrap()
        .get(&client_id)
        .unwrap()
        .last_activity;
    assert_eq!(after_bookkeeping, after_input);

    std::thread::sleep(Duration::from_millis(2));
    metrics.add_client_output_bytes(client_id, 7);
    let after_output = metrics
        .clients
        .lock()
        .unwrap()
        .get(&client_id)
        .unwrap()
        .last_activity;
    assert!(after_output > after_bookkeeping);
}

#[test]
fn add_client_output_bytes_updates_chunk_capacity_reply_buffer_state() {
    let metrics = ServerMetrics::default();
    let client_id = metrics.register_client(None, None);

    let (initial_activity, initial_peak_reset) = {
        let clients = metrics.clients.lock().unwrap();
        let client = clients.get(&client_id).unwrap();
        assert_eq!(client.reply_buffer_size, REPLY_BUFFER_CHUNK_BYTES);
        (client.last_activity, client.reply_buffer_peak_last_reset)
    };

    std::thread::sleep(Duration::from_millis(2));
    metrics.add_client_output_bytes(client_id, 257);

    let clients = metrics.clients.lock().unwrap();
    let client = clients.get(&client_id).unwrap();
    assert_eq!(client.total_output_bytes, 257);
    assert_eq!(client.reply_buffer_size, REPLY_BUFFER_CHUNK_BYTES);
    assert_eq!(client.reply_buffer_peak, 257);
    assert!(client.last_activity > initial_activity);
    assert!(client.reply_buffer_peak_last_reset > initial_peak_reset);
}

#[test]
fn combined_input_and_last_command_updates_counters_activity_and_lowercased_command() {
    let metrics = ServerMetrics::default();
    let client_id = metrics.register_client(None, None);
    let initial_activity = metrics
        .clients
        .lock()
        .unwrap()
        .get(&client_id)
        .unwrap()
        .last_activity;

    std::thread::sleep(Duration::from_millis(2));
    metrics.add_client_input_bytes_and_last_command(client_id, 13, b"CLIENT", Some(b"LIST"));

    let clients = metrics.clients.lock().unwrap();
    let client = clients.get(&client_id).unwrap();
    assert_eq!(client.total_input_bytes, 13);
    assert_eq!(client.last_command, b"client|list");
    assert!(client.last_activity > initial_activity);
}

#[test]
fn client_last_command_stores_lowercased_command_and_subcommand() {
    let metrics = ServerMetrics::default();
    let client_id = metrics.register_client(None, None);

    metrics.set_client_last_command(client_id, b"GET", None);
    let after_get = metrics
        .clients
        .lock()
        .unwrap()
        .get(&client_id)
        .unwrap()
        .last_command
        .clone();
    assert_eq!(after_get, b"get");

    metrics.set_client_last_command(client_id, b"CLIENT", Some(b"LIST"));
    let after_client_list = metrics
        .clients
        .lock()
        .unwrap()
        .get(&client_id)
        .unwrap()
        .last_command
        .clone();
    assert_eq!(after_client_list, b"client|list");

    metrics.set_client_last_command(client_id, b"PING", Some(b""));
    let after_ping = metrics
        .clients
        .lock()
        .unwrap()
        .get(&client_id)
        .unwrap()
        .last_command
        .clone();
    assert_eq!(after_ping, b"ping");
}

#[tokio::test]
async fn multidb_select_copy_move_and_flushdb_match_external_scenarios_over_tcp() {
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
        &encode_resp_command(&[b"CONFIG", b"SET", b"databases", b"4"]),
        b"+OK\r\n",
    )
    .await;

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"shared", b"zero"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SELECT", b"1"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"GET", b"shared"]),
        b"$-1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"shared", b"one"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"GET", b"shared"]),
        b"$3\r\none\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SELECT", b"0"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"GET", b"shared"]),
        b"$4\r\nzero\r\n",
    )
    .await;

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"src", b"hello", b"PX", b"60000"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"COPY", b"src", b"copied", b"DB", b"1"]),
        b":1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SELECT", b"1"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"GET", b"copied"]),
        b"$5\r\nhello\r\n",
    )
    .await;
    let copied_ttl = send_and_read_integer(
        &mut client,
        &encode_resp_command(&[b"PTTL", b"copied"]),
        Duration::from_secs(1),
    )
    .await;
    assert!(copied_ttl > 0);

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"MOVE", b"copied", b"3"]),
        b":1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"GET", b"copied"]),
        b"$-1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SELECT", b"3"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"GET", b"copied"]),
        b"$5\r\nhello\r\n",
    )
    .await;
    let moved_ttl = send_and_read_integer(
        &mut client,
        &encode_resp_command(&[b"PTTL", b"copied"]),
        Duration::from_secs(1),
    )
    .await;
    assert!(moved_ttl > 0);

    send_and_expect(&mut client, &encode_resp_command(&[b"FLUSHDB"]), b"+OK\r\n").await;
    send_and_expect(&mut client, &encode_resp_command(&[b"DBSIZE"]), b":0\r\n").await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SELECT", b"0"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"GET", b"src"]),
        b"$5\r\nhello\r\n",
    )
    .await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn multidb_keyspace_sequences_match_external_scenarios_over_tcp() {
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
        &encode_resp_command(&[b"CONFIG", b"SET", b"databases", b"16"]),
        b"+OK\r\n",
    )
    .await;

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"x", b"foo"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"GET", b"x"]),
        b"$3\r\nfoo\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEL", b"x"]),
        b":1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"GET", b"x"]),
        b"$-1\r\n",
    )
    .await;

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"MSET", b"foo1", b"a", b"foo2", b"b", b"foo3", b"c"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"MGET", b"foo1", b"foo2", b"foo3", b"foo4"]),
        b"*4\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n$-1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEL", b"foo1", b"foo2", b"foo3", b"foo4"]),
        b":3\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"MGET", b"foo1", b"foo2", b"foo3"]),
        b"*3\r\n$-1\r\n$-1\r\n$-1\r\n",
    )
    .await;

    for key in [b"key_x", b"key_y", b"key_z", b"foo_a", b"foo_b", b"foo_c"] {
        send_and_expect(
            &mut client,
            &encode_resp_command(&[b"SET", key, b"hello"]),
            b"+OK\r\n",
        )
        .await;
    }
    send_and_expect(&mut client, &encode_resp_command(&[b"DBSIZE"]), b":6\r\n").await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[
            b"DEL", b"key_x", b"key_y", b"key_z", b"foo_a", b"foo_b", b"foo_c",
        ]),
        b":6\r\n",
    )
    .await;
    send_and_expect(&mut client, &encode_resp_command(&[b"DBSIZE"]), b":0\r\n").await;

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"newkey", b"test"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"EXISTS", b"newkey"]),
        b":1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEL", b"newkey"]),
        b":1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"EXISTS", b"newkey"]),
        b":0\r\n",
    )
    .await;

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"emptykey", b""]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"GET", b"emptykey"]),
        b"$0\r\n\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"EXISTS", b"emptykey"]),
        b":1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEL", b"emptykey"]),
        b":1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"EXISTS", b"emptykey"]),
        b":0\r\n",
    )
    .await;

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEL", b"mykey"]),
        b":0\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"RENAME", b"mykey", b"mykey"]),
        b"-ERR no such key\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SELECT", b"3"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"MSET", b"foo1", b"a", b"foo2", b"b", b"foo3", b"c"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"MGET", b"foo1", b"foo2", b"foo3", b"foo4"]),
        b"*4\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n$-1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEL", b"foo1", b"foo2", b"foo3", b"foo4"]),
        b":3\r\n",
    )
    .await;

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SELECT", b"9"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"mykey", b"foobar"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"COPY", b"mykey", b"mynewkey", b"DB", b"10"]),
        b":1\r\n",
    )
    .await;
    send_and_expect(&mut client, &encode_resp_command(&[b"DBSIZE"]), b":1\r\n").await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SELECT", b"10"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"GET", b"mynewkey"]),
        b"$6\r\nfoobar\r\n",
    )
    .await;
    send_and_expect(&mut client, &encode_resp_command(&[b"DBSIZE"]), b":1\r\n").await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SELECT", b"9"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEL", b"set1", b"newset1"]),
        b":0\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SADD", b"set1", b"1", b"2", b"3", b"a"]),
        b":4\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"COPY", b"set1", b"newset1"]),
        b":1\r\n",
    )
    .await;
    let set1_refcount = send_and_read_integer(
        &mut client,
        &encode_resp_command(&[b"OBJECT", b"REFCOUNT", b"set1"]),
        Duration::from_secs(1),
    )
    .await;
    let newset1_refcount = send_and_read_integer(
        &mut client,
        &encode_resp_command(&[b"OBJECT", b"REFCOUNT", b"newset1"]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(set1_refcount, 1);
    assert_eq!(newset1_refcount, 1);

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SELECT", b"0"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"db0_key", b"zero"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SELECT", b"1"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"db1_key", b"one"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SELECT", b"0"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SWAPDB", b"0", b"1"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"GET", b"db0_key"]),
        b"$-1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"GET", b"db1_key"]),
        b"$3\r\none\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SELECT", b"1"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"GET", b"db0_key"]),
        b"$4\r\nzero\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"GET", b"db1_key"]),
        b"$-1\r\n",
    )
    .await;
    let keyspace_info = send_and_read_bulk_payload(
        &mut client,
        &encode_resp_command(&[b"INFO", b"keyspace"]),
        Duration::from_secs(1),
    )
    .await;
    let keyspace_info = String::from_utf8(keyspace_info).unwrap();
    assert!(keyspace_info.contains("# Keyspace\r\n"));
    assert!(keyspace_info.contains("db0:keys=1,expires=0,avg_ttl=0\r\n"));
    assert!(keyspace_info.contains("db1:keys=1,expires=0,avg_ttl=0\r\n"));
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SWAPDB", b"0", b"bad"]),
        b"-ERR invalid second DB index\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SWAPDB", b"bad", b"1"]),
        b"-ERR invalid first DB index\r\n",
    )
    .await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn multidb_string_mutations_and_persist_match_external_scenarios_over_tcp() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let mut client = TcpStream::connect(addr).await.unwrap();
    let timeout = Duration::from_secs(5);

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"CONFIG", b"SET", b"databases", b"16"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SELECT", b"9"]),
        b"+OK\r\n",
    )
    .await;

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEL", b"bits"]),
        b":0\r\n",
    )
    .await;
    let bitfield_signed_first = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"BITFIELD", b"bits", b"SET", b"i8", b"0", b"-100"]),
        timeout,
    )
    .await;
    assert_eq!(resp_socket_integer_array(&bitfield_signed_first), vec![0]);
    let bitfield_signed_second = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"BITFIELD", b"bits", b"SET", b"i8", b"0", b"101"]),
        timeout,
    )
    .await;
    assert_eq!(
        resp_socket_integer_array(&bitfield_signed_second),
        vec![-100]
    );
    let bitfield_signed_get = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"BITFIELD", b"bits", b"GET", b"i8", b"0"]),
        timeout,
    )
    .await;
    assert_eq!(resp_socket_integer_array(&bitfield_signed_get), vec![101]);

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEL", b"bits"]),
        b":1\r\n",
    )
    .await;
    let bitfield_unsigned_first = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"BITFIELD", b"bits", b"SET", b"u8", b"0", b"255"]),
        timeout,
    )
    .await;
    assert_eq!(resp_socket_integer_array(&bitfield_unsigned_first), vec![0]);
    let bitfield_unsigned_second = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"BITFIELD", b"bits", b"SET", b"u8", b"0", b"100"]),
        timeout,
    )
    .await;
    assert_eq!(
        resp_socket_integer_array(&bitfield_unsigned_second),
        vec![255]
    );
    let bitfield_unsigned_get = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"BITFIELD", b"bits", b"GET", b"u8", b"0"]),
        timeout,
    )
    .await;
    assert_eq!(resp_socket_integer_array(&bitfield_unsigned_get), vec![100]);

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEL", b"bits"]),
        b":1\r\n",
    )
    .await;
    send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"BITFIELD", b"bits", b"SET", b"u8", b"#0", b"65"]),
        timeout,
    )
    .await;
    send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"BITFIELD", b"bits", b"SET", b"u8", b"#1", b"66"]),
        timeout,
    )
    .await;
    send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"BITFIELD", b"bits", b"SET", b"u8", b"#2", b"67"]),
        timeout,
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"GET", b"bits"]),
        b"$3\r\nABC\r\n",
    )
    .await;

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEL", b"bits"]),
        b":1\r\n",
    )
    .await;
    send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"BITFIELD", b"bits", b"SET", b"u8", b"#0", b"10"]),
        timeout,
    )
    .await;
    let bitfield_incr_first = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"BITFIELD", b"bits", b"INCRBY", b"u8", b"#0", b"100"]),
        timeout,
    )
    .await;
    assert_eq!(resp_socket_integer_array(&bitfield_incr_first), vec![110]);
    let bitfield_incr_second = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"BITFIELD", b"bits", b"INCRBY", b"u8", b"#0", b"100"]),
        timeout,
    )
    .await;
    assert_eq!(resp_socket_integer_array(&bitfield_incr_second), vec![210]);

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEL", b"foo"]),
        b":0\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SETBIT", b"foo", b"0", b"1"]),
        b":0\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"BITCOUNT", b"foo", b"0", b"4294967296"]),
        b":1\r\n",
    )
    .await;

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"x", b"foo"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"EXPIRE", b"x", b"50"]),
        b":1\r\n",
    )
    .await;
    let ttl_before_persist =
        send_and_read_integer(&mut client, &encode_resp_command(&[b"TTL", b"x"]), timeout).await;
    assert!(
        (1..=50).contains(&ttl_before_persist),
        "TTL before PERSIST should be between 1 and 50 seconds, got {ttl_before_persist}"
    );
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"PERSIST", b"x"]),
        b":1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"TTL", b"x"]),
        b":-1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"GET", b"x"]),
        b"$3\r\nfoo\r\n",
    )
    .await;

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEL", b"foo"]),
        b":1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"APPEND", b"foo", b"bar"]),
        b":3\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"GET", b"foo"]),
        b"$3\r\nbar\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"APPEND", b"foo", b"100"]),
        b":6\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"GET", b"foo"]),
        b"$6\r\nbar100\r\n",
    )
    .await;

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEL", b"foo"]),
        b":1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"APPEND", b"foo", b"1"]),
        b":1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"APPEND", b"foo", b"2"]),
        b":2\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"GET", b"foo"]),
        b"$2\r\n12\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"foo", b"1"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"APPEND", b"foo", b"2"]),
        b":2\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"GET", b"foo"]),
        b"$2\r\n12\r\n",
    )
    .await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn multidb_setbit_bitfield_noop_do_not_increase_dirty_counter_like_external_scenario() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let mut client = TcpStream::connect(addr).await.unwrap();
    let timeout = Duration::from_secs(5);

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"CONFIG", b"SET", b"databases", b"16"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SELECT", b"9"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(&mut client, &encode_resp_command(&[b"FLUSHDB"]), b"+OK\r\n").await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"CONFIG", b"RESETSTAT"]),
        b"+OK\r\n",
    )
    .await;

    let info_before = send_and_read_bulk_payload(
        &mut client,
        &encode_resp_command(&[b"INFO", b"stats"]),
        timeout,
    )
    .await;
    let dirty_before = read_info_u64(&info_before, "rdb_changes_since_last_save").unwrap_or(0);

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SETBIT", b"foo{t}", b"0", b"0"]),
        b":0\r\n",
    )
    .await;
    let bitfield_create = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"BITFIELD", b"foo2{t}", b"SET", b"i5", b"0", b"0"]),
        timeout,
    )
    .await;
    assert_eq!(resp_socket_integer_array(&bitfield_create), vec![0]);

    let info_after_create = send_and_read_bulk_payload(
        &mut client,
        &encode_resp_command(&[b"INFO", b"stats"]),
        timeout,
    )
    .await;
    let dirty_after_create =
        read_info_u64(&info_after_create, "rdb_changes_since_last_save").unwrap_or(0);
    assert_eq!(dirty_after_create.saturating_sub(dirty_before), 2);

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SETBIT", b"foo{t}", b"0", b"0"]),
        b":0\r\n",
    )
    .await;
    let bitfield_noop = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"BITFIELD", b"foo2{t}", b"SET", b"i5", b"0", b"0"]),
        timeout,
    )
    .await;
    assert_eq!(resp_socket_integer_array(&bitfield_noop), vec![0]);

    let info_after_noop = send_and_read_bulk_payload(
        &mut client,
        &encode_resp_command(&[b"INFO", b"stats"]),
        timeout,
    )
    .await;
    let dirty_after_noop =
        read_info_u64(&info_after_noop, "rdb_changes_since_last_save").unwrap_or(0);
    assert_eq!(dirty_after_noop, dirty_after_create);

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multidb_active_expire_increments_expired_keys_active_like_external_scenario() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let mut writer_client = TcpStream::connect(addr).await.unwrap();
    let mut stats_client = TcpStream::connect(addr).await.unwrap();
    let timeout = Duration::from_secs(5);

    send_and_expect(
        &mut writer_client,
        &encode_resp_command(&[b"CONFIG", b"SET", b"databases", b"16"]),
        b"+OK\r\n",
    )
    .await;
    for client in [&mut writer_client, &mut stats_client] {
        send_and_expect(client, &encode_resp_command(&[b"SELECT", b"9"]), b"+OK\r\n").await;
    }
    send_and_expect(
        &mut writer_client,
        &encode_resp_command(&[b"FLUSHDB"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut writer_client,
        &encode_resp_command(&[b"CONFIG", b"RESETSTAT"]),
        b"+OK\r\n",
    )
    .await;

    for key in [b"foo1", b"foo2", b"foo3"] {
        send_and_expect(
            &mut writer_client,
            &encode_resp_command(&[b"SET", key, b"bar", b"PX", b"1"]),
            b"+OK\r\n",
        )
        .await;
    }

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let info = send_and_read_bulk_payload(
            &mut stats_client,
            &encode_resp_command(&[b"INFO", b"stats"]),
            timeout,
        )
        .await;
        let expired_keys = read_info_u64(&info, "expired_keys").unwrap_or(0);
        let expired_keys_active = read_info_u64(&info, "expired_keys_active").unwrap_or(0);
        if expired_keys_active == 3 {
            assert_eq!(
                expired_keys, 3,
                "active expire reached expired_keys_active=3 but expired_keys={expired_keys} on db9"
            );
            break;
        }

        if expired_keys == 3 && expired_keys_active < 3 {
            send_and_expect(
                &mut writer_client,
                &encode_resp_command(&[b"DEBUG", b"SET-ACTIVE-EXPIRE", b"1"]),
                b"+OK\r\n",
            )
            .await;
        }

        assert!(
            Instant::now() < deadline,
            "active expire did not reach expired_keys_active=3 on db9: expired_keys={expired_keys}, expired_keys_active={expired_keys_active}"
        );
        sleep(Duration::from_millis(100)).await;
    }

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn latency_of_expire_events_are_collected_like_external_scenario() {
    let (addr, shutdown_tx, server) = start_test_server_with_scripting_enabled().await;
    let mut client = TcpStream::connect(addr).await.unwrap();
    let timeout = Duration::from_secs(30);

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SELECT", b"9"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"CONFIG", b"SET", b"latency-monitor-threshold", b"20"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(&mut client, &encode_resp_command(&[b"FLUSHDB"]), b"+OK\r\n").await;

    let populate = encode_resp_command(&[
        b"EVAL",
        b"local i = 0; while (i < tonumber(ARGV[1])) do redis.call('sadd', KEYS[1], i); i = i + 1; end",
        b"1",
        b"mybigkey",
        b"1000000",
    ]);
    client.write_all(&populate).await.unwrap();
    let populate_response = read_resp_value_with_timeout(&mut client, timeout).await;
    assert!(
        !matches!(populate_response, RespSocketValue::Error(_)),
        "EVAL populate should succeed, got {populate_response:?}",
    );
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"PEXPIRE", b"mybigkey", b"50"]),
        b":1\r\n",
    )
    .await;

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let dbsize = send_and_read_integer(
            &mut client,
            &encode_resp_command(&[b"DBSIZE"]),
            Duration::from_secs(1),
        )
        .await;
        if dbsize == 0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "dbsize did not reach zero after active expire for DB9 large set",
        );
        sleep(Duration::from_millis(100)).await;
    }

    let latest = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"LATENCY", b"LATEST"]),
        timeout,
    )
    .await;
    assert!(
        resp_socket_contains_bulk(&latest, b"expire-cycle"),
        "LATENCY LATEST should include expire-cycle after active expire, got {latest:?}",
    );

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"CONFIG", b"SET", b"latency-monitor-threshold", b"200"]),
        b"+OK\r\n",
    )
    .await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn multidb_debug_reload_preserves_mixed_dataset_with_hash_field_expirations_like_external_other_scenario()
 {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let mut client = TcpStream::connect(addr).await.unwrap();
    let now_unix_seconds = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let expire_a = now_unix_seconds + 3600;
    let expire_b = now_unix_seconds + 3601;

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"CONFIG", b"SET", b"databases", b"16"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SELECT", b"9"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(&mut client, &encode_resp_command(&[b"FLUSHDB"]), b"+OK\r\n").await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"plain{t}", b"value"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"RPUSH", b"list{t}", b"a", b"b"]),
        b":2\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SADD", b"set{t}", b"a", b"b"]),
        b":2\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"ZADD", b"zset{t}", b"1", b"one", b"2", b"two"]),
        b":2\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"HSET", b"hash{t}", b"a", b"1", b"b", b"2", b"c", b"3"]),
        b":3\r\n",
    )
    .await;

    let expire_a_text = expire_a.to_string();
    let expire_b_text = expire_b.to_string();
    send_and_expect(
        &mut client,
        &encode_resp_command(&[
            b"HEXPIREAT",
            b"hash{t}",
            expire_a_text.as_bytes(),
            b"FIELDS",
            b"1",
            b"a",
        ]),
        b"*1\r\n:1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[
            b"HEXPIREAT",
            b"hash{t}",
            expire_b_text.as_bytes(),
            b"FIELDS",
            b"1",
            b"b",
        ]),
        b"*1\r\n:1\r\n",
    )
    .await;

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEBUG", b"RELOAD"]),
        b"+OK\r\n",
    )
    .await;

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"GET", b"plain{t}"]),
        b"$5\r\nvalue\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"LRANGE", b"list{t}", b"0", b"-1"]),
        b"*2\r\n$1\r\na\r\n$1\r\nb\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SCARD", b"set{t}"]),
        b":2\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SISMEMBER", b"set{t}", b"a"]),
        b":1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SISMEMBER", b"set{t}", b"b"]),
        b":1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"ZRANGE", b"zset{t}", b"0", b"-1", b"WITHSCORES"]),
        b"*4\r\n$3\r\none\r\n$1\r\n1\r\n$3\r\ntwo\r\n$1\r\n2\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"HGETALL", b"hash{t}"]),
        b"*6\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n$1\r\nc\r\n$1\r\n3\r\n",
    )
    .await;

    let expected_hexpiretime = format!("*3\r\n:{}\r\n:{}\r\n:-1\r\n", expire_a, expire_b);
    send_and_expect(
        &mut client,
        &encode_resp_command(&[
            b"HEXPIRETIME",
            b"hash{t}",
            b"FIELDS",
            b"3",
            b"a",
            b"b",
            b"c",
        ]),
        expected_hexpiretime.as_bytes(),
    )
    .await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn multidb_set_hot_entries_are_visible_to_keys_and_digest_across_debug_reload() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let mut client = TcpStream::connect(addr).await.unwrap();
    let timeout = Duration::from_secs(5);

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"CONFIG", b"SET", b"databases", b"16"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SELECT", b"9"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(&mut client, &encode_resp_command(&[b"FLUSHDB"]), b"+OK\r\n").await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SADD", b"hot{t}", b"a"]),
        b":1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"KEYS", b"*"]),
        b"*1\r\n$6\r\nhot{t}\r\n",
    )
    .await;

    let digest_before = send_and_read_bulk_payload(
        &mut client,
        &encode_resp_command(&[b"DEBUG", b"DIGEST"]),
        timeout,
    )
    .await;

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEBUG", b"RELOAD"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"KEYS", b"*"]),
        b"*1\r\n$6\r\nhot{t}\r\n",
    )
    .await;

    let digest_after = send_and_read_bulk_payload(
        &mut client,
        &encode_resp_command(&[b"DEBUG", b"DIGEST"]),
        timeout,
    )
    .await;
    assert_eq!(
        digest_before, digest_after,
        "debug digest changed across reload"
    );

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn multidb_watch_flush_swapdb_expire_and_discard_match_external_multi_scenarios() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let mut client = TcpStream::connect(addr).await.unwrap();

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"CONFIG", b"SET", b"databases", b"16"]),
        b"+OK\r\n",
    )
    .await;

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SELECT", b"5"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"x", b"30"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"WATCH", b"x"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SELECT", b"1"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"x", b"10"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SELECT", b"5"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(&mut client, &encode_resp_command(&[b"MULTI"]), b"+OK\r\n").await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"PING"]),
        b"+QUEUED\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"EXEC"]),
        b"*1\r\n+PONG\r\n",
    )
    .await;

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SELECT", b"9"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"x", b"30"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"WATCH", b"x"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"FLUSHALL"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(&mut client, &encode_resp_command(&[b"MULTI"]), b"+OK\r\n").await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"PING"]),
        b"+QUEUED\r\n",
    )
    .await;
    send_and_expect(&mut client, &encode_resp_command(&[b"EXEC"]), b"*-1\r\n").await;

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"x", b"30"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"WATCH", b"x"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(&mut client, &encode_resp_command(&[b"FLUSHDB"]), b"+OK\r\n").await;
    send_and_expect(&mut client, &encode_resp_command(&[b"MULTI"]), b"+OK\r\n").await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"PING"]),
        b"+QUEUED\r\n",
    )
    .await;
    send_and_expect(&mut client, &encode_resp_command(&[b"EXEC"]), b"*-1\r\n").await;

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEBUG", b"SET-ACTIVE-EXPIRE", b"0"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"FLUSHALL"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SELECT", b"1"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"x", b"foo", b"PX", b"1"]),
        b"+OK\r\n",
    )
    .await;
    sleep(Duration::from_millis(5)).await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"WATCH", b"x"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SWAPDB", b"0", b"1"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(&mut client, &encode_resp_command(&[b"MULTI"]), b"+OK\r\n").await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"PING"]),
        b"+QUEUED\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"EXEC"]),
        b"*1\r\n+PONG\r\n",
    )
    .await;

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"FLUSHALL"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SELECT", b"1"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"x", b"foo", b"PX", b"1"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SELECT", b"0"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"x", b"bar", b"PX", b"1"]),
        b"+OK\r\n",
    )
    .await;
    sleep(Duration::from_millis(5)).await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SELECT", b"1"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"WATCH", b"x"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SWAPDB", b"0", b"1"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(&mut client, &encode_resp_command(&[b"MULTI"]), b"+OK\r\n").await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"PING"]),
        b"+QUEUED\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"EXEC"]),
        b"*1\r\n+PONG\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEBUG", b"SET-ACTIVE-EXPIRE", b"1"]),
        b"+OK\r\n",
    )
    .await;

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SELECT", b"9"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEBUG", b"SET-ACTIVE-EXPIRE", b"0"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"key", b"1", b"PX", b"1"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"WATCH", b"key"]),
        b"+OK\r\n",
    )
    .await;
    sleep(Duration::from_millis(5)).await;
    send_and_expect(&mut client, &encode_resp_command(&[b"MULTI"]), b"+OK\r\n").await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"INCR", b"key"]),
        b"+QUEUED\r\n",
    )
    .await;
    send_and_expect(&mut client, &encode_resp_command(&[b"EXEC"]), b"*-1\r\n").await;

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEL", b"x"]),
        b":0\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"x", b"foo", b"PX", b"1"]),
        b"+OK\r\n",
    )
    .await;
    sleep(Duration::from_millis(5)).await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"WATCH", b"x"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"EXISTS", b"x"]),
        b":0\r\n",
    )
    .await;
    send_and_expect(&mut client, &encode_resp_command(&[b"MULTI"]), b"+OK\r\n").await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"PING"]),
        b"+QUEUED\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"EXEC"]),
        b"*1\r\n+PONG\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEBUG", b"SET-ACTIVE-EXPIRE", b"1"]),
        b"+OK\r\n",
    )
    .await;

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"FLUSHALL"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEL", b"x"]),
        b":0\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"x", b"foo"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"EXPIRE", b"x", b"1"]),
        b":1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"WATCH", b"x"]),
        b"+OK\r\n",
    )
    .await;
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let dbsize = send_and_read_integer(
            &mut client,
            &encode_resp_command(&[b"DBSIZE"]),
            Duration::from_secs(1),
        )
        .await;
        if dbsize == 0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "dbsize did not reach zero after active expire"
        );
        sleep(Duration::from_millis(20)).await;
    }
    send_and_expect(&mut client, &encode_resp_command(&[b"MULTI"]), b"+OK\r\n").await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"PING"]),
        b"+QUEUED\r\n",
    )
    .await;
    send_and_expect(&mut client, &encode_resp_command(&[b"EXEC"]), b"*-1\r\n").await;

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"WATCH", b"x"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"x", b"10"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(&mut client, &encode_resp_command(&[b"MULTI"]), b"+OK\r\n").await;
    send_and_expect(&mut client, &encode_resp_command(&[b"DISCARD"]), b"+OK\r\n").await;
    send_and_expect(&mut client, &encode_resp_command(&[b"MULTI"]), b"+OK\r\n").await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"INCR", b"x"]),
        b"+QUEUED\r\n",
    )
    .await;
    let discard_exec = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"EXEC"]),
        Duration::from_secs(1),
    )
    .await;
    let discard_exec_items = resp_socket_array(&discard_exec);
    assert_eq!(discard_exec_items.len(), 1);
    assert_eq!(resp_socket_integer(&discard_exec_items[0]), 11);

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"WATCH", b"x"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"x", b"10"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(&mut client, &encode_resp_command(&[b"MULTI"]), b"+OK\r\n").await;
    send_and_expect(&mut client, &encode_resp_command(&[b"DISCARD"]), b"+OK\r\n").await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"x", b"10"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(&mut client, &encode_resp_command(&[b"MULTI"]), b"+OK\r\n").await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"INCR", b"x"]),
        b"+QUEUED\r\n",
    )
    .await;
    let unwatch_exec = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"EXEC"]),
        Duration::from_secs(1),
    )
    .await;
    let unwatch_exec_items = resp_socket_array(&unwatch_exec);
    assert_eq!(unwatch_exec_items.len(), 1);
    assert_eq!(resp_socket_integer(&unwatch_exec_items[0]), 11);

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn multidb_info_keysizes_swapdb_and_debug_reload_match_external_scenarios_over_tcp() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let mut client = TcpStream::connect(addr).await.unwrap();

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"CONFIG", b"SET", b"databases", b"16"]),
        b"+OK\r\n",
    )
    .await;

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"FLUSHALL"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"RPUSH", b"l1", b"1", b"2", b"3", b"4"]),
        b":4\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SELECT", b"1"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"ZADD", b"z1", b"1", b"A"]),
        b":1\r\n",
    )
    .await;
    let info_before_swap = send_and_read_bulk_payload(
        &mut client,
        &encode_resp_command(&[b"INFO", b"KEYSIZES"]),
        Duration::from_secs(1),
    )
    .await;
    let info_before_swap = String::from_utf8(info_before_swap).unwrap();
    assert!(info_before_swap.contains("db0_distrib_lists_items:4=1\r\n"));
    assert!(info_before_swap.contains("db1_distrib_zsets_items:1=1\r\n"));
    assert!(!info_before_swap.contains("db0_distrib_zsets_items:1=1\r\n"));

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SWAPDB", b"0", b"1"]),
        b"+OK\r\n",
    )
    .await;
    let info_after_swap = send_and_read_bulk_payload(
        &mut client,
        &encode_resp_command(&[b"INFO", b"KEYSIZES"]),
        Duration::from_secs(1),
    )
    .await;
    let info_after_swap = String::from_utf8(info_after_swap).unwrap();
    assert!(info_after_swap.contains("db0_distrib_zsets_items:1=1\r\n"));
    assert!(info_after_swap.contains("db1_distrib_lists_items:4=1\r\n"));
    assert!(!info_after_swap.contains("db0_distrib_lists_items:4=1\r\n"));

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SELECT", b"0"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"FLUSHALL"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"RPUSH", b"l10", b"1", b"2", b"3", b"4"]),
        b":4\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"s2", b"1234567890"]),
        b"+OK\r\n",
    )
    .await;
    let info_before_reload = send_and_read_bulk_payload(
        &mut client,
        &encode_resp_command(&[b"INFO", b"KEYSIZES"]),
        Duration::from_secs(1),
    )
    .await;
    let info_before_reload = String::from_utf8(info_before_reload).unwrap();
    assert!(info_before_reload.contains("db0_distrib_strings_sizes:8=1\r\n"));
    assert!(info_before_reload.contains("db0_distrib_lists_items:4=1\r\n"));

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEBUG", b"RELOAD"]),
        b"+OK\r\n",
    )
    .await;
    let info_after_reload = send_and_read_bulk_payload(
        &mut client,
        &encode_resp_command(&[b"INFO", b"KEYSIZES"]),
        Duration::from_secs(1),
    )
    .await;
    let info_after_reload = String::from_utf8(info_after_reload).unwrap();
    assert!(info_after_reload.contains("db0_distrib_strings_sizes:8=1\r\n"));
    assert!(info_after_reload.contains("db0_distrib_lists_items:4=1\r\n"));

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEL", b"l10"]),
        b":1\r\n",
    )
    .await;
    let info_after_delete = send_and_read_bulk_payload(
        &mut client,
        &encode_resp_command(&[b"INFO", b"KEYSIZES"]),
        Duration::from_secs(1),
    )
    .await;
    let info_after_delete = String::from_utf8(info_after_delete).unwrap();
    assert!(info_after_delete.contains("db0_distrib_strings_sizes:8=1\r\n"));
    assert!(!info_after_delete.contains("db0_distrib_lists_items:4=1\r\n"));

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEBUG", b"RELOAD"]),
        b"+OK\r\n",
    )
    .await;
    let info_after_second_reload = send_and_read_bulk_payload(
        &mut client,
        &encode_resp_command(&[b"INFO", b"KEYSIZES"]),
        Duration::from_secs(1),
    )
    .await;
    let info_after_second_reload = String::from_utf8(info_after_second_reload).unwrap();
    assert!(info_after_second_reload.contains("db0_distrib_strings_sizes:8=1\r\n"));
    assert!(!info_after_second_reload.contains("db0_distrib_lists_items:4=1\r\n"));

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn info_clients_updates_watch_and_tracking_counts_on_disable_and_disconnect() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let timeout = Duration::from_secs(1);
    let mut observer = TcpStream::connect(addr).await.unwrap();
    let mut watcher = TcpStream::connect(addr).await.unwrap();
    let mut tracker = TcpStream::connect(addr).await.unwrap();

    send_and_expect(
        &mut watcher,
        &encode_resp_command(&[b"WATCH", b"watched"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut tracker,
        &encode_resp_command(&[b"CLIENT", b"TRACKING", b"ON", b"BCAST", b"PREFIX", b"wat"]),
        b"+OK\r\n",
    )
    .await;

    let info_with_both = send_and_read_bulk_payload(
        &mut observer,
        &encode_resp_command(&[b"INFO", b"clients"]),
        timeout,
    )
    .await;
    let info_with_both = String::from_utf8(info_with_both).unwrap();
    assert!(
        info_with_both.contains("watching_clients:1\r\n"),
        "INFO clients should report one watcher, got: {info_with_both}"
    );
    assert!(
        info_with_both.contains("tracking_clients:1\r\n"),
        "INFO clients should report one tracking client, got: {info_with_both}"
    );

    send_and_expect(
        &mut tracker,
        &encode_resp_command(&[b"CLIENT", b"TRACKING", b"OFF"]),
        b"+OK\r\n",
    )
    .await;

    let info_after_tracking_off = send_and_read_bulk_payload(
        &mut observer,
        &encode_resp_command(&[b"INFO", b"clients"]),
        timeout,
    )
    .await;
    let info_after_tracking_off = String::from_utf8(info_after_tracking_off).unwrap();
    assert!(
        info_after_tracking_off.contains("watching_clients:1\r\n"),
        "INFO clients should keep one watcher after TRACKING OFF, got: {info_after_tracking_off}"
    );
    assert!(
        info_after_tracking_off.contains("tracking_clients:0\r\n"),
        "INFO clients should drop tracking count after TRACKING OFF, got: {info_after_tracking_off}"
    );

    drop(watcher);
    let mut final_info = String::new();
    for _ in 0..20 {
        let payload = send_and_read_bulk_payload(
            &mut observer,
            &encode_resp_command(&[b"INFO", b"clients"]),
            timeout,
        )
        .await;
        final_info = String::from_utf8(payload).unwrap();
        if final_info.contains("watching_clients:0\r\n")
            && final_info.contains("tracking_clients:0\r\n")
        {
            break;
        }
        sleep(Duration::from_millis(10)).await;
    }
    assert!(
        final_info.contains("watching_clients:0\r\n"),
        "INFO clients should drop watcher count after disconnect, got: {final_info}"
    );
    assert!(
        final_info.contains("tracking_clients:0\r\n"),
        "INFO clients should keep tracking count at zero after disconnect, got: {final_info}"
    );

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn watch_stale_key_then_lazy_delete_does_not_abort_exec() {
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
        b"*3\r\n$5\r\nDEBUG\r\n$17\r\nSET-ACTIVE-EXPIRE\r\n$1\r\n0\r\n",
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*5\r\n$3\r\nSET\r\n$1\r\nx\r\n$3\r\nfoo\r\n$2\r\nPX\r\n$1\r\n1\r\n",
        b"+OK\r\n",
    )
    .await;
    sleep(Duration::from_millis(5)).await;
    send_and_expect(&mut client, b"*2\r\n$5\r\nWATCH\r\n$1\r\nx\r\n", b"+OK\r\n").await;
    send_and_expect(&mut client, b"*2\r\n$6\r\nEXISTS\r\n$1\r\nx\r\n", b":0\r\n").await;
    send_and_expect(&mut client, b"*1\r\n$5\r\nMULTI\r\n", b"+OK\r\n").await;
    send_and_expect(&mut client, b"*1\r\n$4\r\nPING\r\n", b"+QUEUED\r\n").await;
    send_and_expect(&mut client, b"*1\r\n$4\r\nEXEC\r\n", b"*1\r\n+PONG\r\n").await;
    send_and_expect(
        &mut client,
        b"*3\r\n$5\r\nDEBUG\r\n$17\r\nSET-ACTIVE-EXPIRE\r\n$1\r\n1\r\n",
        b"+OK\r\n",
    )
    .await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn reply_buffer_limits_match_external_scenario() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let mut inspector = TcpStream::connect(addr).await.unwrap();
    let mut test_client = TcpStream::connect(addr).await.unwrap();

    send_and_expect(
        &mut inspector,
        &encode_resp_command(&[b"DEBUG", b"REPLYBUFFER", b"PEAK-RESET-TIME", b"100"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut inspector,
        &encode_resp_command(&[b"DEBUG", b"REPLY-COPY-AVOIDANCE", b"0"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut test_client,
        &encode_resp_command(&[b"CLIENT", b"SETNAME", b"test_client"]),
        b"+OK\r\n",
    )
    .await;

    let mut idle_reply_buffer_size = 0usize;
    let idle_deadline = Instant::now() + Duration::from_secs(1);
    let mut idle_ok = false;
    while Instant::now() < idle_deadline {
        idle_reply_buffer_size =
            reply_buffer_size_for_named_client(&mut inspector, "test_client").await;
        if (1024..2046).contains(&idle_reply_buffer_size) {
            idle_ok = true;
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }
    assert!(
        idle_ok,
        "reply buffer of idle client is {idle_reply_buffer_size} after 1 seconds"
    );

    let big_value = vec![b'x'; 32_768];
    send_and_expect(
        &mut inspector,
        &encode_resp_command(&[b"SET", b"bigval", big_value.as_slice()]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut inspector,
        &encode_resp_command(&[b"DEBUG", b"REPLYBUFFER", b"PEAK-RESET-TIME", b"never"]),
        b"+OK\r\n",
    )
    .await;

    let get_big_value = encode_resp_command(&[b"GET", b"bigval"]);
    let mut busy_reply_buffer_size = 0usize;
    let busy_deadline = Instant::now() + Duration::from_secs(1);
    let mut busy_ok = false;
    while Instant::now() < busy_deadline {
        let payload =
            send_and_read_bulk_payload(&mut test_client, &get_big_value, Duration::from_secs(2))
                .await;
        assert_eq!(payload.len(), big_value.len());
        busy_reply_buffer_size =
            reply_buffer_size_for_named_client(&mut inspector, "test_client").await;
        if (16_384..32_768).contains(&busy_reply_buffer_size) {
            busy_ok = true;
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }
    assert!(
        busy_ok,
        "reply buffer of busy client is {busy_reply_buffer_size} after 1 seconds"
    );

    send_and_expect(
        &mut inspector,
        &encode_resp_command(&[b"DEBUG", b"REPLYBUFFER", b"PEAK-RESET-TIME", b"reset"]),
        b"+OK\r\n",
    )
    .await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn query_buffer_resizing_matches_external_scenarios() {
    let timeout = Duration::from_secs(1);
    let (addr, shutdown_tx, server) = start_test_server().await;
    let mut inspector = TcpStream::connect(addr).await.unwrap();

    send_and_expect(
        &mut inspector,
        &encode_resp_command(&[b"CONFIG", b"SET", b"hz", b"100"]),
        b"+OK\r\n",
    )
    .await;

    let partial_name = "querybuf_partial";
    let mut partial_client = TcpStream::connect(addr).await.unwrap();
    send_and_expect(
        &mut partial_client,
        &encode_resp_command(&[b"CLIENT", b"SETNAME", partial_name.as_bytes()]),
        b"+OK\r\n",
    )
    .await;
    assert_eq!(
        query_buffer_total_for_named_client(&mut inspector, partial_name).await,
        0
    );

    send_and_expect(
        &mut inspector,
        &encode_resp_command(&[b"DEBUG", b"PAUSE-CRON", b"1"]),
        b"+OK\r\n",
    )
    .await;

    partial_client
        .write_all(b"*3\r\n$3\r\nset\r\n$2\r\na")
        .await
        .unwrap();
    let partial_deadline = Instant::now() + Duration::from_secs(1);
    let mut partial_query_buffer = 0usize;
    while Instant::now() < partial_deadline {
        partial_query_buffer =
            query_buffer_total_for_named_client(&mut inspector, partial_name).await;
        if partial_query_buffer > 0 {
            break;
        }
        sleep(Duration::from_millis(10)).await;
    }
    assert!(
        partial_query_buffer > 0,
        "client should start using a private query buffer"
    );

    partial_client.write_all(b"a\r\n$1\r\nb\r\n").await.unwrap();
    let partial_set_reply = read_resp_line_with_timeout(&mut partial_client, timeout).await;
    assert_eq!(partial_set_reply, b"+OK".to_vec());

    let original_partial_query_buffer =
        query_buffer_total_for_named_client(&mut inspector, partial_name).await;
    assert!(
        (16_384..=32_770).contains(&original_partial_query_buffer),
        "unexpected private query buffer size after partial command: {original_partial_query_buffer}"
    );

    send_and_expect(
        &mut inspector,
        &encode_resp_command(&[b"DEBUG", b"PAUSE-CRON", b"0"]),
        b"+OK\r\n",
    )
    .await;

    let partial_shrink_deadline = Instant::now() + Duration::from_secs(5);
    let mut partial_shrunk = false;
    while Instant::now() < partial_shrink_deadline {
        let idle_seconds = client_idle_seconds_for_named_client(&mut inspector, partial_name).await;
        let query_buffer = query_buffer_total_for_named_client(&mut inspector, partial_name).await;
        if idle_seconds >= 3 && query_buffer < original_partial_query_buffer {
            partial_shrunk = true;
            break;
        }
        sleep(Duration::from_millis(10)).await;
    }
    assert!(partial_shrunk, "query buffer was not resized");
    drop(partial_client);

    let busy_name = "querybuf_busy";
    let mut busy_client = TcpStream::connect(addr).await.unwrap();
    send_and_expect(
        &mut busy_client,
        &encode_resp_command(&[b"CLIENT", b"SETNAME", busy_name.as_bytes()]),
        b"+OK\r\n",
    )
    .await;

    send_and_expect(
        &mut inspector,
        &encode_resp_command(&[b"DEBUG", b"PAUSE-CRON", b"1"]),
        b"+OK\r\n",
    )
    .await;

    let large_value = vec![b'A'; 400_000];
    send_and_expect(
        &mut busy_client,
        &encode_resp_command(&[b"SET", b"x", large_value.as_slice()]),
        b"+OK\r\n",
    )
    .await;

    let original_busy_query_buffer =
        query_buffer_total_for_named_client(&mut inspector, busy_name).await;
    assert!(
        original_busy_query_buffer > 32_768,
        "large query buffer should exceed resize threshold, got {original_busy_query_buffer}"
    );

    send_and_expect(
        &mut inspector,
        &encode_resp_command(&[b"DEBUG", b"PAUSE-CRON", b"0"]),
        b"+OK\r\n",
    )
    .await;

    let small_value = vec![b'A'; 100];
    let busy_shrink_deadline = Instant::now() + Duration::from_secs(1);
    while Instant::now() < busy_shrink_deadline {
        send_and_expect(
            &mut busy_client,
            &encode_resp_command(&[b"SET", b"x", small_value.as_slice()]),
            b"+OK\r\n",
        )
        .await;
        let current_query_buffer =
            query_buffer_total_for_named_client(&mut inspector, busy_name).await;
        if current_query_buffer < original_busy_query_buffer {
            break;
        }
        sleep(Duration::from_millis(10)).await;
    }
    let final_busy_query_buffer =
        query_buffer_total_for_named_client(&mut inspector, busy_name).await;
    assert!(
        final_busy_query_buffer > 0 && final_busy_query_buffer < original_busy_query_buffer,
        "query buffer should shrink but remain private after recent small writes: {final_busy_query_buffer}"
    );
    drop(busy_client);

    let fat_name = "querybuf_fat";
    let mut fat_client = TcpStream::connect(addr).await.unwrap();
    send_and_expect(
        &mut fat_client,
        &encode_resp_command(&[b"CLIENT", b"SETNAME", fat_name.as_bytes()]),
        b"+OK\r\n",
    )
    .await;

    send_and_expect(
        &mut inspector,
        &encode_resp_command(&[b"DEBUG", b"PAUSE-CRON", b"1"]),
        b"+OK\r\n",
    )
    .await;

    fat_client
        .write_all(b"*3\r\n$3\r\nset\r\n$1\r\na\r\n$1000000\r\n")
        .await
        .unwrap();
    let fat_deadline = Instant::now() + Duration::from_secs(1);
    let mut fat_query_buffer = 0usize;
    while Instant::now() < fat_deadline {
        fat_query_buffer = query_buffer_total_for_named_client(&mut inspector, fat_name).await;
        if fat_query_buffer > 1_000_000 {
            break;
        }
        sleep(Duration::from_millis(10)).await;
    }
    assert!(
        fat_query_buffer > 1_000_000,
        "client should start using a large private query buffer"
    );

    fat_client.write_all(b"a").await.unwrap();
    send_and_expect(
        &mut inspector,
        &encode_resp_command(&[b"DEBUG", b"PAUSE-CRON", b"0"]),
        b"+OK\r\n",
    )
    .await;

    sleep(Duration::from_millis(120)).await;
    assert!(
        query_buffer_total_for_named_client(&mut inspector, fat_name).await > 1_000_000,
        "query buffer should not be resized when client idle time is smaller than 2 seconds"
    );

    let fat_shrink_deadline = Instant::now() + Duration::from_secs(5);
    let mut fat_shrunk = false;
    while Instant::now() < fat_shrink_deadline {
        let idle_seconds = client_idle_seconds_for_named_client(&mut inspector, fat_name).await;
        let query_buffer = query_buffer_total_for_named_client(&mut inspector, fat_name).await;
        if idle_seconds >= 3 && query_buffer < 1_000_000 {
            fat_shrunk = true;
            break;
        }
        sleep(Duration::from_millis(10)).await;
    }
    assert!(
        fat_shrunk,
        "query buffer should be resized when client idle time is bigger than 2 seconds"
    );
    drop(fat_client);

    let reusable_name = "querybuf_reusable";
    let mut reusable_client = TcpStream::connect(addr).await.unwrap();
    send_and_expect(
        &mut reusable_client,
        &encode_resp_command(&[b"CLIENT", b"SETNAME", reusable_name.as_bytes()]),
        b"+OK\r\n",
    )
    .await;

    let reusable_payload = send_and_read_bulk_payload(
        &mut inspector,
        &encode_resp_command(&[b"CLIENT", b"LIST"]),
        timeout,
    )
    .await;
    let reusable_payload_text = String::from_utf8(reusable_payload).unwrap();
    let reusable_client_line = client_list_line_with_name(&reusable_payload_text, reusable_name)
        .unwrap_or_else(|| {
            panic!("client named `{reusable_name}` not found in CLIENT LIST payload")
        });
    assert!(
        reusable_client_line.contains(" qbuf=0 qbuf-free=0 ")
            && reusable_client_line.contains(" cmd=client|setname "),
        "reusable query buffer line mismatch: {reusable_client_line}"
    );

    let io_threads = send_and_read_bulk_array_payloads(
        &mut inspector,
        &encode_resp_command(&[b"CONFIG", b"GET", b"io-threads"]),
        timeout,
    )
    .await;
    let client_list_line = client_list_line_with_command(&reusable_payload_text, "client|list")
        .unwrap_or_else(|| panic!("CLIENT LIST response should include the executing client line"));
    if io_threads
        .get(1)
        .is_some_and(|value| value.as_slice() == b"1")
    {
        assert!(
            client_list_line.contains(" qbuf=26 qbuf-free="),
            "CLIENT LIST should expose reusable query buffer bytes while executing: {client_list_line}"
        );
    } else {
        assert!(
            client_list_line.contains(" qbuf=0 qbuf-free="),
            "CLIENT LIST should expose zero qbuf for reusable-buffer execution: {client_list_line}"
        );
    }

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn sscan_shrink_regression_issue_4906_matches_external_scenario() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let mut client = TcpStream::connect(addr).await.unwrap();

    for iteration_seed in 0..100usize {
        let _ = send_and_read_integer(
            &mut client,
            &encode_resp_command(&[b"DEL", b"set"]),
            Duration::from_secs(5),
        )
        .await;
        assert_eq!(
            send_and_read_integer(
                &mut client,
                &encode_resp_command(&[b"SADD", b"set", b"x"]),
                Duration::from_secs(5),
            )
            .await,
            1
        );

        let numele = 101 + ((iteration_seed * 37) % 1000);
        let mut to_remove = Vec::new();
        for value in 0..numele {
            let member = value.to_string();
            assert_eq!(
                send_and_read_integer(
                    &mut client,
                    &encode_resp_command(&[b"SADD", b"set", member.as_bytes()]),
                    Duration::from_secs(5),
                )
                .await,
                1
            );
            if value >= 100 {
                to_remove.push(member.into_bytes());
            }
        }

        let mut found = std::collections::BTreeSet::<Vec<u8>>::new();
        let mut cursor = 0u64;
        let mut scan_iteration = 0usize;
        let delete_iteration = iteration_seed % 10;

        loop {
            let cursor_text = cursor.to_string();
            let (next_cursor, items) = send_and_read_scan_cursor_and_members(
                &mut client,
                &encode_resp_command(&[b"SSCAN", b"set", cursor_text.as_bytes()]),
                Duration::from_secs(5),
            )
            .await;
            for item in items {
                found.insert(item);
            }

            scan_iteration += 1;
            if scan_iteration == delete_iteration && !to_remove.is_empty() {
                let mut parts = Vec::with_capacity(to_remove.len() + 2);
                parts.push(b"SREM".as_slice());
                parts.push(b"set".as_slice());
                for member in &to_remove {
                    parts.push(member.as_slice());
                }
                let removed = send_and_read_integer(
                    &mut client,
                    &encode_resp_command(&parts),
                    Duration::from_secs(5),
                )
                .await;
                assert_eq!(removed, to_remove.len() as i64);
            }

            if next_cursor == 0 {
                break;
            }
            cursor = next_cursor;
        }

        for expected in 0..100usize {
            let member = expected.to_string().into_bytes();
            assert!(
                found.contains(&member),
                "SSCAN element missing {} for seed {} (numele={}, delete_iteration={})",
                expected,
                iteration_seed,
                numele,
                delete_iteration
            );
        }
    }

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn watch_key_expiring_after_watch_aborts_exec() {
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
        b"*3\r\n$5\r\nDEBUG\r\n$17\r\nSET-ACTIVE-EXPIRE\r\n$1\r\n0\r\n",
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*5\r\n$3\r\nSET\r\n$1\r\nx\r\n$3\r\nfoo\r\n$2\r\nPX\r\n$2\r\n20\r\n",
        b"+OK\r\n",
    )
    .await;
    send_and_expect(&mut client, b"*2\r\n$5\r\nWATCH\r\n$1\r\nx\r\n", b"+OK\r\n").await;
    sleep(Duration::from_millis(30)).await;
    send_and_expect(&mut client, b"*1\r\n$5\r\nMULTI\r\n", b"+OK\r\n").await;
    send_and_expect(&mut client, b"*1\r\n$4\r\nPING\r\n", b"+QUEUED\r\n").await;
    send_and_expect(&mut client, b"*1\r\n$4\r\nEXEC\r\n", b"*-1\r\n").await;
    send_and_expect(&mut client, b"*1\r\n$4\r\nPING\r\n", b"+PONG\r\n").await;
    send_and_expect(
        &mut client,
        b"*3\r\n$5\r\nDEBUG\r\n$17\r\nSET-ACTIVE-EXPIRE\r\n$1\r\n1\r\n",
        b"+OK\r\n",
    )
    .await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn smove_existing_destination_member_does_not_abort_watched_exec() {
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
    let mut client2 = TcpStream::connect(addr).await.unwrap();

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEL", b"srcset{t}", b"dstset{t}"]),
        b":0\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SADD", b"srcset{t}", b"a", b"b"]),
        b":2\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SADD", b"dstset{t}", b"a"]),
        b":1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"WATCH", b"dstset{t}"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(&mut client, &encode_resp_command(&[b"MULTI"]), b"+OK\r\n").await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SADD", b"dstset{t}", b"c"]),
        b"+QUEUED\r\n",
    )
    .await;

    send_and_expect(
        &mut client2,
        &encode_resp_command(&[b"SMOVE", b"srcset{t}", b"dstset{t}", b"a"]),
        b":1\r\n",
    )
    .await;

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"EXEC"]),
        b"*1\r\n:1\r\n",
    )
    .await;
    assert_eq!(
        send_and_read_integer(
            &mut client,
            &encode_resp_command(&[b"SCARD", b"dstset{t}"]),
            Duration::from_secs(5),
        )
        .await,
        2
    );

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn tcp_inline_pipeline_executes_basic_crud_commands() {
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
    client
        .write_all(b"SET k1 xyzk\r\nGET k1\r\nPING\r\n")
        .await
        .unwrap();
    let expected = b"+OK\r\n$4\r\nxyzk\r\n+PONG\r\n";
    let mut actual = vec![0u8; expected.len()];
    tokio::time::timeout(Duration::from_secs(1), client.read_exact(&mut actual))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(actual, expected);

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn inline_pipelining_stresser_external_scenario_round_trips_all_pairs() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    run_inline_pipelining_stresser_external_scenario(addr, None).await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn inline_pipelining_stresser_external_scenario_round_trips_all_pairs_in_nonzero_db() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    run_inline_pipelining_stresser_external_scenario(addr, Some(9)).await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

async fn run_inline_pipelining_stresser_external_scenario(
    addr: SocketAddr,
    selected_db: Option<u16>,
) {
    let mut client = TcpStream::connect(addr).await.unwrap();

    if let Some(selected_db) = selected_db {
        let select = format!("SELECT {selected_db}\r\n");
        client.write_all(select.as_bytes()).await.unwrap();
        let select_reply = read_resp_line_with_timeout(&mut client, Duration::from_secs(5)).await;
        assert_eq!(select_reply, b"+OK");
    }

    // Redis tests/unit/other.tcl:
    // "PIPELINING stresser (also a regression for the old epoll bug)"
    const PIPELINE_PAIRS: usize = 100_000;
    let mut pipeline = Vec::with_capacity(4 * 1024 * 1024);
    for i in 0..PIPELINE_PAIRS {
        let value = format!("0000{i}0000");
        pipeline.extend_from_slice(format!("SET key:{i} {value}\r\n").as_bytes());
        pipeline.extend_from_slice(format!("GET key:{i}\r\n").as_bytes());
    }

    client.write_all(&pipeline).await.unwrap();

    tokio::time::timeout(Duration::from_secs(60), async {
        for i in 0..PIPELINE_PAIRS {
            let set_line = read_resp_line_with_timeout(&mut client, Duration::from_secs(5)).await;
            assert_eq!(
                set_line,
                b"+OK",
                "expected +OK for pipelined SET at index {i}, got: {:?}",
                String::from_utf8_lossy(&set_line)
            );

            let payload = read_bulk_payload_with_timeout(&mut client, Duration::from_secs(5)).await;
            let expected = format!("0000{i}0000").into_bytes();
            assert_eq!(payload, expected, "unexpected GET payload at index {i}");
        }
    })
    .await
    .expect("inline pipelining stresser timed out");
}

#[tokio::test]
async fn redis_cli_pipe_raw_protocol_external_scenario_round_trips_all_replies() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let mut client = TcpStream::connect(addr).await.unwrap();

    // Redis tests/integration/redis-cli.tcl: "Piping raw protocol"
    const INCR_SET_PAIRS: usize = 1_000;
    const VERY_LARGE_SET_COUNT: usize = 100;
    let delete_counter = encode_resp_command(&[b"DEL", b"test-counter"]);
    let incr_counter = encode_resp_command(&[b"INCR", b"test-counter"]);
    let large_value = vec![b'x'; 20_000];
    let set_large = encode_resp_command(&[b"SET", b"large-key", large_value.as_slice()]);
    let very_large_value = vec![b'x'; 512_000];
    let set_very_large =
        encode_resp_command(&[b"SET", b"very-large-key", very_large_value.as_slice()]);
    let echo_tag = b"pipe-sentinel-tag";
    let echo_sentinel = encode_resp_command(&[b"ECHO", echo_tag]);

    client.write_all(&delete_counter).await.unwrap();
    for _ in 0..INCR_SET_PAIRS {
        client.write_all(&incr_counter).await.unwrap();
        client.write_all(&set_large).await.unwrap();
    }
    for _ in 0..VERY_LARGE_SET_COUNT {
        client.write_all(&set_very_large).await.unwrap();
    }
    client.write_all(&echo_sentinel).await.unwrap();

    tokio::time::timeout(Duration::from_secs(120), async {
        let del_reply = read_resp_line_with_timeout(&mut client, Duration::from_secs(5)).await;
        assert!(
            del_reply.starts_with(b":"),
            "expected integer DEL reply, got: {:?}",
            String::from_utf8_lossy(&del_reply)
        );

        for expected_counter in 1..=INCR_SET_PAIRS {
            let incr_reply = read_resp_line_with_timeout(&mut client, Duration::from_secs(5)).await;
            let expected = expected_counter.to_string().into_bytes();
            assert_eq!(
                incr_reply,
                [&b":"[..], expected.as_slice()].concat(),
                "unexpected INCR reply at index {expected_counter}"
            );

            let set_reply = read_resp_line_with_timeout(&mut client, Duration::from_secs(5)).await;
            assert_eq!(set_reply, b"+OK", "expected +OK for large SET");
        }

        for index in 0..VERY_LARGE_SET_COUNT {
            let set_reply = read_resp_line_with_timeout(&mut client, Duration::from_secs(5)).await;
            assert_eq!(
                set_reply, b"+OK",
                "expected +OK for very-large SET at index {index}"
            );
        }

        let sentinel_reply =
            read_bulk_payload_with_timeout(&mut client, Duration::from_secs(5)).await;
        assert_eq!(sentinel_reply, echo_tag);
    })
    .await
    .expect("redis-cli raw pipe scenario timed out");

    let counter = send_and_read_bulk_payload(
        &mut client,
        &encode_resp_command(&[b"GET", b"test-counter"]),
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(counter, b"1000");

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn redis_cli_pipe_mode_matches_external_scenario_when_repo_cli_is_available() {
    let Some(redis_cli) = runnable_repo_redis_cli() else {
        return;
    };

    let (addr, shutdown_tx, server) = start_test_server().await;
    wait_for_server_ping(addr).await;

    let temp_dir = unique_test_temp_dir("redis-cli-pipe");
    let cmds_path = temp_dir.join("cli_cmds");
    {
        let file = std::fs::File::create(&cmds_path).unwrap();
        let mut writer = std::io::BufWriter::new(file);
        let delete_counter = encode_resp_command(&[b"DEL", b"test-counter"]);
        let incr_counter = encode_resp_command(&[b"INCR", b"test-counter"]);
        let large_value = vec![b'x'; 20_000];
        let set_large = encode_resp_command(&[b"SET", b"large-key", large_value.as_slice()]);
        let very_large_value = vec![b'x'; 512_000];
        let set_very_large =
            encode_resp_command(&[b"SET", b"very-large-key", very_large_value.as_slice()]);

        std::io::Write::write_all(&mut writer, &delete_counter).unwrap();
        for _ in 0..1_000 {
            std::io::Write::write_all(&mut writer, &incr_counter).unwrap();
            std::io::Write::write_all(&mut writer, &set_large).unwrap();
        }
        for _ in 0..100 {
            std::io::Write::write_all(&mut writer, &set_very_large).unwrap();
        }
        std::io::Write::flush(&mut writer).unwrap();
    }

    let host = addr.ip().to_string();
    let port = addr.port().to_string();
    let stdin_file = std::fs::File::open(&cmds_path).unwrap();
    let output = tokio::time::timeout(
        Duration::from_secs(120),
        TokioCommand::new(&redis_cli)
            .args(["-h", &host, "-p", &port, "--pipe"])
            .stdin(Stdio::from(stdin_file))
            .output(),
    )
    .await
    .expect("redis-cli --pipe timed out")
    .unwrap();

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "redis-cli --pipe failed\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stdout.contains("All data transferred") && stdout.contains("errors: 0"),
        "redis-cli --pipe summary missing success markers\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stdout.contains("replies: 2101"),
        "redis-cli --pipe summary missing reply count\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );

    let mut verifier = TcpStream::connect(addr).await.unwrap();
    let counter = send_and_read_bulk_payload(
        &mut verifier,
        &encode_resp_command(&[b"GET", b"test-counter"]),
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(counter, b"1000");

    let _ = std::fs::remove_file(&cmds_path);
    let _ = std::fs::remove_dir_all(&temp_dir);
    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn blocking_blpop_wakes_by_polling_from_another_connection() {
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

    let mut waiter = TcpStream::connect(addr).await.unwrap();
    let mut producer = TcpStream::connect(addr).await.unwrap();
    waiter
        .write_all(b"*3\r\n$5\r\nBLPOP\r\n$1\r\nk\r\n$1\r\n0\r\n")
        .await
        .unwrap();

    let mut probe = [0u8; 1];
    assert!(
        tokio::time::timeout(Duration::from_millis(20), waiter.read(&mut probe))
            .await
            .is_err(),
        "blocking BLPOP should not return before producer pushes",
    );

    producer
        .write_all(b"*3\r\n$5\r\nRPUSH\r\n$1\r\nk\r\n$1\r\nv\r\n")
        .await
        .unwrap();

    let response = read_exact_with_timeout(&mut waiter, 18, Duration::from_secs(1)).await;
    assert_eq!(response, b"*2\r\n$1\r\nk\r\n$1\r\nv\r\n");

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn blocking_blpop_respects_timeout_without_updates() {
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
    client
        .write_all(b"*3\r\n$5\r\nBLPOP\r\n$1\r\nk\r\n$4\r\n0.05\r\n")
        .await
        .unwrap();

    let response = read_exact_with_timeout(&mut client, 5, Duration::from_secs(1)).await;
    assert_eq!(response, b"*-1\r\n");

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn blocking_clients_are_visible_in_info_and_client_list() {
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

    let mut waiter = TcpStream::connect(addr).await.unwrap();
    let mut inspector = TcpStream::connect(addr).await.unwrap();
    let mut producer = TcpStream::connect(addr).await.unwrap();

    send_and_expect(
        &mut waiter,
        b"*3\r\n$6\r\nCLIENT\r\n$7\r\nSETNAME\r\n$6\r\nwaiter\r\n",
        b"+OK\r\n",
    )
    .await;
    let waiter_id = send_and_read_integer(
        &mut waiter,
        b"*2\r\n$6\r\nCLIENT\r\n$2\r\nID\r\n",
        Duration::from_secs(1),
    )
    .await;

    waiter
        .write_all(b"*3\r\n$5\r\nBLPOP\r\n$1\r\nk\r\n$1\r\n1\r\n")
        .await
        .unwrap();

    let waiter_id_text = waiter_id.to_string();
    let client_list_frame =
        encode_resp_command(&[b"CLIENT", b"LIST", b"ID", waiter_id_text.as_bytes()]);
    let mut blocked_visible = false;
    let deadline = Instant::now() + Duration::from_secs(1);
    while Instant::now() < deadline {
        let info = send_and_read_bulk_payload(
            &mut inspector,
            b"*1\r\n$4\r\nINFO\r\n",
            Duration::from_secs(1),
        )
        .await;
        if !info
            .windows("blocked_clients:1".len())
            .any(|w| w == b"blocked_clients:1")
        {
            sleep(Duration::from_millis(10)).await;
            continue;
        }

        let list_payload =
            send_and_read_bulk_payload(&mut inspector, &client_list_frame, Duration::from_secs(1))
                .await;
        if list_payload
            .windows("flags=b".len())
            .any(|w| w == b"flags=b")
            && list_payload
                .windows("cmd=blpop".len())
                .any(|w| w == b"cmd=blpop")
            && list_payload
                .windows("name=waiter".len())
                .any(|w| w == b"name=waiter")
        {
            blocked_visible = true;
            break;
        }
        sleep(Duration::from_millis(10)).await;
    }
    assert!(
        blocked_visible,
        "blocked client should be visible via INFO/CLIENT LIST while waiting"
    );

    producer
        .write_all(b"*3\r\n$5\r\nRPUSH\r\n$1\r\nk\r\n$1\r\nv\r\n")
        .await
        .unwrap();
    let popped = read_exact_with_timeout(&mut waiter, 18, Duration::from_secs(1)).await;
    assert_eq!(popped, b"*2\r\n$1\r\nk\r\n$1\r\nv\r\n");

    let info_after = send_and_read_bulk_payload(
        &mut inspector,
        b"*1\r\n$4\r\nINFO\r\n",
        Duration::from_secs(1),
    )
    .await;
    assert!(
        info_after
            .windows("blocked_clients:0".len())
            .any(|w| w == b"blocked_clients:0"),
        "INFO should report blocked_clients:0 after wakeup: {}",
        String::from_utf8_lossy(&info_after),
    );

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn client_pause_inside_multi_is_queued_and_applies_after_exec() {
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

    let mut tx_client = TcpStream::connect(addr).await.unwrap();
    let mut writer = TcpStream::connect(addr).await.unwrap();
    let mut inspector = TcpStream::connect(addr).await.unwrap();

    send_and_expect(&mut tx_client, b"*1\r\n$5\r\nMULTI\r\n", b"+OK\r\n").await;
    send_and_expect(
        &mut tx_client,
        b"*4\r\n$6\r\nCLIENT\r\n$5\r\nPAUSE\r\n$5\r\n60000\r\n$5\r\nWRITE\r\n",
        b"+QUEUED\r\n",
    )
    .await;
    send_and_expect(
        &mut tx_client,
        b"*3\r\n$3\r\nSET\r\n$7\r\nmulti:k\r\n$1\r\n1\r\n",
        b"+QUEUED\r\n",
    )
    .await;
    send_and_expect(
        &mut tx_client,
        b"*1\r\n$4\r\nEXEC\r\n",
        b"*2\r\n+OK\r\n+OK\r\n",
    )
    .await;

    writer
        .write_all(b"*3\r\n$3\r\nSET\r\n$8\r\npaused:k\r\n$1\r\n2\r\n")
        .await
        .unwrap();
    wait_for_blocked_clients(&mut inspector, 1, Duration::from_secs(1)).await;

    send_and_expect(
        &mut tx_client,
        b"*2\r\n$6\r\nCLIENT\r\n$7\r\nUNPAUSE\r\n",
        b"+OK\r\n",
    )
    .await;
    let resumed = read_exact_with_timeout(&mut writer, 5, Duration::from_secs(1)).await;
    assert_eq!(resumed, b"+OK\r\n");
    wait_for_blocked_clients(&mut inspector, 0, Duration::from_secs(1)).await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn client_pause_write_keeps_randomkey_visible_until_unpause() {
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
    send_and_expect(&mut client, b"*1\r\n$5\r\nMULTI\r\n", b"+OK\r\n").await;
    send_and_expect(
        &mut client,
        b"*5\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\nPX\r\n$1\r\n3\r\n",
        b"+QUEUED\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*4\r\n$6\r\nCLIENT\r\n$5\r\nPAUSE\r\n$5\r\n10000\r\n$5\r\nWRITE\r\n",
        b"+QUEUED\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*1\r\n$4\r\nEXEC\r\n",
        b"*2\r\n+OK\r\n+OK\r\n",
    )
    .await;

    sleep(Duration::from_millis(5)).await;

    let mut saw_key_during_pause = false;
    for _ in 0..50 {
        let random = send_and_read_optional_bulk(
            &mut client,
            b"*1\r\n$9\r\nRANDOMKEY\r\n",
            Duration::from_millis(100),
        )
        .await;
        if random.as_deref() == Some(b"key") {
            saw_key_during_pause = true;
            break;
        }
        sleep(Duration::from_millis(10)).await;
    }
    assert!(
        saw_key_during_pause,
        "RANDOMKEY should keep returning expired key while CLIENT PAUSE WRITE is active",
    );

    send_and_expect(
        &mut client,
        b"*2\r\n$6\r\nCLIENT\r\n$7\r\nUNPAUSE\r\n",
        b"+OK\r\n",
    )
    .await;

    let mut became_empty_after_unpause = false;
    for _ in 0..50 {
        let random = send_and_read_optional_bulk(
            &mut client,
            b"*1\r\n$9\r\nRANDOMKEY\r\n",
            Duration::from_millis(100),
        )
        .await;
        if random.is_none() {
            became_empty_after_unpause = true;
            break;
        }
        sleep(Duration::from_millis(10)).await;
    }
    assert!(
        became_empty_after_unpause,
        "RANDOMKEY should become empty after unpause when only key is expired"
    );

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn client_pause_write_defers_expired_keys_stats_until_unpause_like_external_scenario() {
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
    let mut inspector = TcpStream::connect(addr).await.unwrap();
    let timeout = Duration::from_secs(1);

    send_and_expect(
        &mut client,
        b"*2\r\n$6\r\nSELECT\r\n$1\r\n9\r\n",
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut inspector,
        b"*2\r\n$6\r\nSELECT\r\n$1\r\n9\r\n",
        b"+OK\r\n",
    )
    .await;

    let baseline_info = send_and_read_bulk_payload(
        &mut inspector,
        b"*2\r\n$4\r\nINFO\r\n$5\r\nstats\r\n",
        timeout,
    )
    .await;
    let baseline_expired_keys = read_info_u64(&baseline_info, "expired_keys").unwrap_or(0);

    send_and_expect(&mut client, b"*1\r\n$5\r\nMULTI\r\n", b"+OK\r\n").await;
    send_and_expect(
        &mut client,
        b"*5\r\n$3\r\nSET\r\n$6\r\nfoo{t}\r\n$6\r\nbar{t}\r\n$2\r\nPX\r\n$2\r\n10\r\n",
        b"+QUEUED\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*5\r\n$3\r\nSET\r\n$6\r\nbar{t}\r\n$6\r\nfoo{t}\r\n$2\r\nPX\r\n$2\r\n10\r\n",
        b"+QUEUED\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*4\r\n$6\r\nCLIENT\r\n$5\r\nPAUSE\r\n$5\r\n50000\r\n$5\r\nWRITE\r\n",
        b"+QUEUED\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*1\r\n$4\r\nEXEC\r\n",
        b"*3\r\n+OK\r\n+OK\r\n+OK\r\n",
    )
    .await;

    let mut logically_expired = false;
    for _ in 0..10 {
        let foo = send_and_read_optional_bulk(
            &mut client,
            b"*2\r\n$3\r\nGET\r\n$6\r\nfoo{t}\r\n",
            timeout,
        )
        .await;
        let bar = send_and_read_optional_bulk(
            &mut client,
            b"*2\r\n$3\r\nGET\r\n$6\r\nbar{t}\r\n",
            timeout,
        )
        .await;
        if foo.is_none() && bar.is_none() {
            logically_expired = true;
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }
    assert!(
        logically_expired,
        "keys never became logically expired during CLIENT PAUSE WRITE",
    );

    let paused_info = send_and_read_bulk_payload(
        &mut inspector,
        b"*2\r\n$4\r\nINFO\r\n$5\r\nstats\r\n",
        timeout,
    )
    .await;
    assert_eq!(
        read_info_u64(&paused_info, "expired_keys").unwrap_or(0),
        baseline_expired_keys,
        "expired_keys should stay unchanged while expire actions are paused",
    );

    send_and_expect(
        &mut client,
        b"*2\r\n$6\r\nCLIENT\r\n$7\r\nUNPAUSE\r\n",
        b"+OK\r\n",
    )
    .await;
    let _ =
        send_and_read_optional_bulk(&mut client, b"*2\r\n$3\r\nGET\r\n$6\r\nfoo{t}\r\n", timeout)
            .await;
    let _ =
        send_and_read_optional_bulk(&mut client, b"*2\r\n$3\r\nGET\r\n$6\r\nbar{t}\r\n", timeout)
            .await;

    let resumed_info = send_and_read_bulk_payload(
        &mut inspector,
        b"*2\r\n$4\r\nINFO\r\n$5\r\nstats\r\n",
        timeout,
    )
    .await;
    assert_eq!(
        read_info_u64(&resumed_info, "expired_keys").unwrap_or(0),
        baseline_expired_keys + 2,
        "expired_keys should advance after forced lazy expiration post-unpause",
    );

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn client_unblock_cannot_interrupt_pause_block_but_works_after_unpause() {
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

    let mut controller = TcpStream::connect(addr).await.unwrap();
    let mut inspector = TcpStream::connect(addr).await.unwrap();
    let mut client1 = TcpStream::connect(addr).await.unwrap();
    let mut client2 = TcpStream::connect(addr).await.unwrap();

    let client1_id = send_and_read_integer(
        &mut client1,
        b"*2\r\n$6\r\nCLIENT\r\n$2\r\nID\r\n",
        Duration::from_secs(1),
    )
    .await;
    let client2_id = send_and_read_integer(
        &mut client2,
        b"*2\r\n$6\r\nCLIENT\r\n$2\r\nID\r\n",
        Duration::from_secs(1),
    )
    .await;
    let client1_id_text = client1_id.to_string();
    let client2_id_text = client2_id.to_string();

    send_and_expect(
        &mut controller,
        b"*2\r\n$3\r\nDEL\r\n$6\r\nmylist\r\n",
        b":0\r\n",
    )
    .await;
    send_and_expect(
        &mut controller,
        b"*4\r\n$6\r\nCLIENT\r\n$5\r\nPAUSE\r\n$6\r\n100000\r\n$5\r\nWRITE\r\n",
        b"+OK\r\n",
    )
    .await;

    client1
        .write_all(b"*3\r\n$5\r\nBLPOP\r\n$6\r\nmylist\r\n$1\r\n0\r\n")
        .await
        .unwrap();
    client2
        .write_all(b"*3\r\n$5\r\nBLPOP\r\n$6\r\nmylist\r\n$1\r\n0\r\n")
        .await
        .unwrap();
    wait_for_blocked_clients(&mut inspector, 2, Duration::from_secs(1)).await;

    let unblock1_during_pause = encode_resp_command(&[
        b"CLIENT",
        b"UNBLOCK",
        client1_id_text.as_bytes(),
        b"TIMEOUT",
    ]);
    let unblock2_during_pause =
        encode_resp_command(&[b"CLIENT", b"UNBLOCK", client2_id_text.as_bytes(), b"ERROR"]);
    let unblock1_during_pause_result = send_and_read_integer(
        &mut controller,
        &unblock1_during_pause,
        Duration::from_secs(1),
    )
    .await;
    let unblock2_during_pause_result = send_and_read_integer(
        &mut controller,
        &unblock2_during_pause,
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(unblock1_during_pause_result, 0);
    assert_eq!(unblock2_during_pause_result, 0);

    send_and_expect(
        &mut controller,
        b"*2\r\n$6\r\nCLIENT\r\n$7\r\nUNPAUSE\r\n",
        b"+OK\r\n",
    )
    .await;

    let unblock1_after_unpause = send_and_read_integer(
        &mut controller,
        &unblock1_during_pause,
        Duration::from_secs(1),
    )
    .await;
    let unblock2_after_unpause = send_and_read_integer(
        &mut controller,
        &unblock2_during_pause,
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(unblock1_after_unpause, 1);
    assert_eq!(unblock2_after_unpause, 1);

    let timeout_response = read_exact_with_timeout(&mut client1, 5, Duration::from_secs(1)).await;
    assert_eq!(timeout_response, b"*-1\r\n");
    let error_response = read_resp_line_with_timeout(&mut client2, Duration::from_secs(1)).await;
    assert_eq!(
        error_response,
        b"-UNBLOCKED client unblocked via CLIENT UNBLOCK"
    );
    wait_for_blocked_clients(&mut inspector, 0, Duration::from_secs(1)).await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn client_pause_write_unpause_releases_script_commands_without_busy_error() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let metrics = Arc::new(ServerMetrics::default());
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let processor =
        Arc::new(RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap());
    let server_processor = Arc::clone(&processor);

    let server_metrics = Arc::clone(&metrics);
    let server = tokio::spawn(async move {
        run_listener_with_shutdown_and_cluster_with_processor(
            listener,
            1024,
            server_metrics,
            async move {
                let _ = shutdown_rx.await;
            },
            None,
            server_processor,
        )
        .await
        .unwrap()
    });

    let mut controller = TcpStream::connect(addr).await.unwrap();
    let mut inspector = TcpStream::connect(addr).await.unwrap();
    let mut script_a = TcpStream::connect(addr).await.unwrap();
    let mut script_b = TcpStream::connect(addr).await.unwrap();

    send_and_expect(
        &mut controller,
        b"*4\r\n$6\r\nCLIENT\r\n$5\r\nPAUSE\r\n$5\r\n60000\r\n$5\r\nWRITE\r\n",
        b"+OK\r\n",
    )
    .await;

    script_a
        .write_all(b"*3\r\n$4\r\nEVAL\r\n$8\r\nreturn 1\r\n$1\r\n0\r\n")
        .await
        .unwrap();
    script_b
        .write_all(b"*3\r\n$4\r\nEVAL\r\n$14\r\n#!lua\nreturn 1\r\n$1\r\n0\r\n")
        .await
        .unwrap();

    wait_for_blocked_clients(&mut inspector, 2, Duration::from_secs(1)).await;
    send_and_expect(
        &mut controller,
        b"*2\r\n$6\r\nCLIENT\r\n$7\r\nUNPAUSE\r\n",
        b"+OK\r\n",
    )
    .await;

    let script_a_response =
        read_resp_line_with_timeout(&mut script_a, Duration::from_secs(1)).await;
    let script_b_response =
        read_resp_line_with_timeout(&mut script_b, Duration::from_secs(1)).await;
    assert_eq!(
        script_a_response,
        b":1",
        "first script returned unexpected response: {}",
        String::from_utf8_lossy(&script_a_response)
    );
    assert_eq!(
        script_b_response,
        b":1",
        "second script returned unexpected response: {}",
        String::from_utf8_lossy(&script_b_response)
    );
    wait_for_blocked_clients(&mut inspector, 0, Duration::from_secs(1)).await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn scripting_resp3_map_external_scenario_runs_as_tcp_integration_test() {
    let _serial = lock_scripting_test_serial().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let metrics = Arc::new(ServerMetrics::default());
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let processor =
        Arc::new(RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap());

    let server_metrics = Arc::clone(&metrics);
    let server = tokio::spawn(async move {
        run_listener_with_shutdown_and_cluster_with_processor(
            listener,
            1024,
            server_metrics,
            async move {
                let _ = shutdown_rx.await;
            },
            None,
            processor,
        )
        .await
        .unwrap()
    });

    let timeout = Duration::from_secs(1);
    let mut client = TcpStream::connect(addr).await.unwrap();
    send_hello_and_drain(&mut client, b"3").await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"HSET", b"hash", b"field", b"value"]),
        b":1\r\n",
    )
    .await;

    let hgetall_resp = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"HGETALL", b"hash"]),
        timeout,
    )
    .await;
    assert_eq!(
        hgetall_resp,
        RespSocketValue::Map(vec![(
            RespSocketValue::Bulk(b"field".to_vec()),
            RespSocketValue::Bulk(b"value".to_vec()),
        )])
    );

    let script_resp3 = b"redis.setresp(3); return redis.call('hgetall', KEYS[1])".as_slice();
    let resp3_eval = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"EVAL", script_resp3, b"1", b"hash"]),
        timeout,
    )
    .await;
    assert_eq!(
        resp3_eval,
        RespSocketValue::Map(vec![(
            RespSocketValue::Bulk(b"field".to_vec()),
            RespSocketValue::Bulk(b"value".to_vec()),
        )])
    );

    let script_resp2 = b"redis.setresp(2); return redis.call('hgetall', KEYS[1])".as_slice();
    let resp2_eval = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"EVAL", script_resp2, b"1", b"hash"]),
        timeout,
    )
    .await;
    assert_eq!(
        resp2_eval,
        RespSocketValue::Array(vec![
            RespSocketValue::Bulk(b"field".to_vec()),
            RespSocketValue::Bulk(b"value".to_vec()),
        ])
    );

    send_hello_and_drain(&mut client, b"2").await;

    let resp3_eval_resp2_client = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"EVAL", script_resp3, b"1", b"hash"]),
        timeout,
    )
    .await;
    assert_eq!(
        resp3_eval_resp2_client,
        RespSocketValue::Array(vec![
            RespSocketValue::Bulk(b"field".to_vec()),
            RespSocketValue::Bulk(b"value".to_vec()),
        ])
    );

    let resp2_eval_resp2_client = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"EVAL", script_resp2, b"1", b"hash"]),
        timeout,
    )
    .await;
    assert_eq!(
        resp2_eval_resp2_client,
        RespSocketValue::Array(vec![
            RespSocketValue::Bulk(b"field".to_vec()),
            RespSocketValue::Bulk(b"value".to_vec()),
        ])
    );

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn scripting_resp_protocol_parsing_matrix_matches_external_scenarios() {
    let _serial = lock_scripting_test_serial().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let metrics = Arc::new(ServerMetrics::default());
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let processor =
        Arc::new(RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap());

    let server_metrics = Arc::clone(&metrics);
    let server = tokio::spawn(async move {
        run_listener_with_shutdown_and_cluster_with_processor(
            listener,
            1024,
            server_metrics,
            async move {
                let _ = shutdown_rx.await;
            },
            None,
            processor,
        )
        .await
        .unwrap()
    });

    let timeout = Duration::from_secs(1);
    let mut controller = TcpStream::connect(addr).await.unwrap();
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"DEBUG", b"SET-DISABLE-DENY-SCRIPTS", b"1"]),
        b"+OK\r\n",
    )
    .await;

    let bignum_value = b"1234567999999999999999999999999999999".to_vec();
    let malformed_bignum = b"123  123".to_vec();

    for client_proto in [RespProtocolVersion::Resp2, RespProtocolVersion::Resp3] {
        let mut client = TcpStream::connect(addr).await.unwrap();
        send_hello_and_drain(
            &mut client,
            if client_proto.is_resp3() { b"3" } else { b"2" },
        )
        .await;

        for script_proto in [RespProtocolVersion::Resp2, RespProtocolVersion::Resp3] {
            let bignum_script = format!(
                "redis.setresp({}); return redis.call('debug', 'protocol', 'bignum')",
                script_proto.as_u8()
            );
            let bignum_response = send_and_read_resp_value(
                &mut client,
                &encode_resp_command(&[b"EVAL", bignum_script.as_bytes(), b"0"]),
                timeout,
            )
            .await;
            if client_proto.is_resp3() && script_proto.is_resp3() {
                assert_eq!(
                    bignum_response,
                    RespSocketValue::BigNumber(bignum_value.clone())
                );
            } else {
                assert_eq!(bignum_response, RespSocketValue::Bulk(bignum_value.clone()));
            }

            let malformed_bignum_response = send_and_read_resp_value(
                &mut client,
                &encode_resp_command(&[b"EVAL", b"return {big_number='123\\r\\n123'}", b"0"]),
                timeout,
            )
            .await;
            if client_proto.is_resp3() {
                assert_eq!(
                    malformed_bignum_response,
                    RespSocketValue::BigNumber(malformed_bignum.clone())
                );
            } else {
                assert_eq!(
                    malformed_bignum_response,
                    RespSocketValue::Bulk(malformed_bignum.clone())
                );
            }

            let map_script = format!(
                "redis.setresp({}); return redis.call('debug', 'protocol', 'map')",
                script_proto.as_u8()
            );
            let map_response = send_and_read_resp_value(
                &mut client,
                &encode_resp_command(&[b"EVAL", map_script.as_bytes(), b"0"]),
                timeout,
            )
            .await;
            if client_proto.is_resp3() && script_proto.is_resp3() {
                match map_response {
                    RespSocketValue::Map(entries) => assert_eq!(entries.len(), 3),
                    other => panic!("expected RESP3 map, got {other:?}"),
                }
            } else {
                match map_response {
                    RespSocketValue::Array(items) => assert_eq!(items.len(), 6),
                    other => panic!("expected RESP2 flattened map, got {other:?}"),
                }
            }

            let set_script = format!(
                "redis.setresp({}); return redis.call('debug', 'protocol', 'set')",
                script_proto.as_u8()
            );
            let set_response = send_and_read_resp_value(
                &mut client,
                &encode_resp_command(&[b"EVAL", set_script.as_bytes(), b"0"]),
                timeout,
            )
            .await;
            if client_proto.is_resp3() && script_proto.is_resp3() {
                match set_response {
                    RespSocketValue::Set(entries) => assert_eq!(entries.len(), 3),
                    other => panic!("expected RESP3 set, got {other:?}"),
                }
            } else {
                match set_response {
                    RespSocketValue::Array(items) => assert_eq!(items.len(), 3),
                    other => panic!("expected RESP2 flattened set, got {other:?}"),
                }
            }

            let double_script = format!(
                "redis.setresp({}); return redis.call('debug', 'protocol', 'double')",
                script_proto.as_u8()
            );
            let double_response = send_and_read_resp_value(
                &mut client,
                &encode_resp_command(&[b"EVAL", double_script.as_bytes(), b"0"]),
                timeout,
            )
            .await;
            if client_proto.is_resp3() && script_proto.is_resp3() {
                assert_eq!(double_response, RespSocketValue::Double(b"3.141".to_vec()));
            } else {
                assert_eq!(double_response, RespSocketValue::Bulk(b"3.141".to_vec()));
            }

            let null_script = format!(
                "redis.setresp({}); return redis.call('debug', 'protocol', 'null')",
                script_proto.as_u8()
            );
            client
                .write_all(&encode_resp_command(&[
                    b"EVAL",
                    null_script.as_bytes(),
                    b"0",
                ]))
                .await
                .unwrap();
            let null_header = read_resp_line_with_timeout(&mut client, timeout).await;
            if client_proto.is_resp3() {
                assert_eq!(null_header, b"_");
            } else {
                assert_eq!(null_header, b"$-1");
            }

            let verbatim_script = format!(
                "redis.setresp({}); return redis.call('debug', 'protocol', 'verbatim')",
                script_proto.as_u8()
            );
            let verbatim_response = send_and_read_resp_value(
                &mut client,
                &encode_resp_command(&[b"EVAL", verbatim_script.as_bytes(), b"0"]),
                timeout,
            )
            .await;
            if client_proto.is_resp3() && script_proto.is_resp3() {
                assert_eq!(
                    verbatim_response,
                    RespSocketValue::Verbatim {
                        format: b"txt".to_vec(),
                        value: b"This is a verbatim\nstring".to_vec(),
                    }
                );
            } else {
                assert_eq!(
                    verbatim_response,
                    RespSocketValue::Bulk(b"This is a verbatim\nstring".to_vec())
                );
            }

            let true_script = format!(
                "redis.setresp({}); return redis.call('debug', 'protocol', 'true')",
                script_proto.as_u8()
            );
            let true_response = send_and_read_resp_value(
                &mut client,
                &encode_resp_command(&[b"EVAL", true_script.as_bytes(), b"0"]),
                timeout,
            )
            .await;
            if client_proto.is_resp3() && script_proto.is_resp3() {
                assert_eq!(true_response, RespSocketValue::Boolean(true));
            } else {
                assert_eq!(true_response, RespSocketValue::Integer(1));
            }

            let false_script = format!(
                "redis.setresp({}); return redis.call('debug', 'protocol', 'false')",
                script_proto.as_u8()
            );
            let false_response = send_and_read_resp_value(
                &mut client,
                &encode_resp_command(&[b"EVAL", false_script.as_bytes(), b"0"]),
                timeout,
            )
            .await;
            if client_proto.is_resp3() && script_proto.is_resp3() {
                assert_eq!(false_response, RespSocketValue::Boolean(false));
            } else {
                assert_eq!(false_response, RespSocketValue::Integer(0));
            }
        }

        if client_proto.is_resp3() {
            let attribute_response = send_and_read_resp_value(
                &mut client,
                &encode_resp_command(&[
                    b"EVAL",
                    b"redis.setresp(3); return redis.call('debug', 'protocol', 'attrib')",
                    b"0",
                ]),
                timeout,
            )
            .await;
            assert_eq!(
                attribute_response,
                RespSocketValue::Bulk(b"Some real reply following the attribute".to_vec())
            );
        }
    }

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn client_pause_write_does_not_block_wrong_arity_errors() {
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

    let mut controller = TcpStream::connect(addr).await.unwrap();
    let mut inspector = TcpStream::connect(addr).await.unwrap();
    let mut paused_client = TcpStream::connect(addr).await.unwrap();

    send_and_expect(
        &mut controller,
        b"*4\r\n$6\r\nCLIENT\r\n$5\r\nPAUSE\r\n$5\r\n60000\r\n$5\r\nWRITE\r\n",
        b"+OK\r\n",
    )
    .await;

    paused_client
        .write_all(b"*2\r\n$3\r\nSET\r\n$3\r\nFOO\r\n")
        .await
        .unwrap();
    let response = read_resp_line_with_timeout(&mut paused_client, Duration::from_secs(1)).await;
    assert_eq!(
        response,
        b"-ERR wrong number of arguments for 'set' command"
    );
    wait_for_blocked_clients(&mut inspector, 0, Duration::from_secs(1)).await;

    send_and_expect(
        &mut controller,
        b"*2\r\n$6\r\nCLIENT\r\n$7\r\nUNPAUSE\r\n",
        b"+OK\r\n",
    )
    .await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn client_unblock_unblocks_blocking_pop_with_timeout_and_error_modes() {
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

    let mut waiter = TcpStream::connect(addr).await.unwrap();
    let mut controller = TcpStream::connect(addr).await.unwrap();
    let mut inspector = TcpStream::connect(addr).await.unwrap();

    let waiter_id = send_and_read_integer(
        &mut waiter,
        b"*2\r\n$6\r\nCLIENT\r\n$2\r\nID\r\n",
        Duration::from_secs(1),
    )
    .await;
    let waiter_id_text = waiter_id.to_string();

    waiter
        .write_all(b"*3\r\n$5\r\nBLPOP\r\n$1\r\nk\r\n$1\r\n0\r\n")
        .await
        .unwrap();
    wait_for_blocked_clients(&mut inspector, 1, Duration::from_secs(1)).await;

    let unblock_timeout = encode_resp_command(&[b"CLIENT", b"UNBLOCK", waiter_id_text.as_bytes()]);
    let timeout_unblocked =
        send_and_read_integer(&mut controller, &unblock_timeout, Duration::from_secs(1)).await;
    assert_eq!(timeout_unblocked, 1);
    let timeout_response = read_exact_with_timeout(&mut waiter, 5, Duration::from_secs(1)).await;
    assert_eq!(timeout_response, b"*-1\r\n");

    waiter
        .write_all(b"*3\r\n$5\r\nBLPOP\r\n$1\r\nk\r\n$1\r\n0\r\n")
        .await
        .unwrap();
    wait_for_blocked_clients(&mut inspector, 1, Duration::from_secs(1)).await;

    let unblock_error =
        encode_resp_command(&[b"CLIENT", b"UNBLOCK", waiter_id_text.as_bytes(), b"ERROR"]);
    let error_unblocked =
        send_and_read_integer(&mut controller, &unblock_error, Duration::from_secs(1)).await;
    assert_eq!(error_unblocked, 1);
    let error_response = read_resp_line_with_timeout(&mut waiter, Duration::from_secs(1)).await;
    assert_eq!(
        error_response,
        b"-UNBLOCKED client unblocked via CLIENT UNBLOCK"
    );

    let controller_id = send_and_read_integer(
        &mut controller,
        b"*2\r\n$6\r\nCLIENT\r\n$2\r\nID\r\n",
        Duration::from_secs(1),
    )
    .await;
    let controller_id_text = controller_id.to_string();
    let unblock_non_blocked =
        encode_resp_command(&[b"CLIENT", b"UNBLOCK", controller_id_text.as_bytes()]);
    let non_blocked_result = send_and_read_integer(
        &mut controller,
        &unblock_non_blocked,
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(non_blocked_result, 0);

    controller
        .write_all(&encode_resp_command(&[b"CLIENT", b"UNBLOCK", b"asd"]))
        .await
        .unwrap();
    let invalid_id_response =
        read_resp_line_with_timeout(&mut controller, Duration::from_secs(1)).await;
    assert_eq!(
        invalid_id_response,
        b"-ERR value is not an integer or out of range"
    );

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn blocking_pipeline_preserves_waiter_fairness() {
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

    let mut waiter = TcpStream::connect(addr).await.unwrap();
    let mut pipelined = TcpStream::connect(addr).await.unwrap();
    let mut inspector = TcpStream::connect(addr).await.unwrap();

    send_and_expect(
        &mut inspector,
        b"*2\r\n$3\r\nDEL\r\n$6\r\nmylist\r\n",
        b":0\r\n",
    )
    .await;

    waiter
        .write_all(b"*3\r\n$5\r\nBLPOP\r\n$6\r\nmylist\r\n$1\r\n0\r\n")
        .await
        .unwrap();
    wait_for_blocked_clients(&mut inspector, 1, Duration::from_secs(1)).await;

    pipelined
        .write_all(b"LPUSH mylist 1\r\nBLPOP mylist 0\r\n")
        .await
        .unwrap();
    let waiter_value = read_exact_with_timeout(&mut waiter, 23, Duration::from_secs(1)).await;
    assert_eq!(waiter_value, b"*2\r\n$6\r\nmylist\r\n$1\r\n1\r\n");
    let pipelined_lpush = read_exact_with_timeout(&mut pipelined, 4, Duration::from_secs(1)).await;
    assert_eq!(pipelined_lpush, b":1\r\n");

    wait_for_blocked_clients(&mut inspector, 1, Duration::from_secs(1)).await;

    send_and_expect(
        &mut inspector,
        b"*3\r\n$5\r\nLPUSH\r\n$6\r\nmylist\r\n$1\r\n2\r\n",
        b":1\r\n",
    )
    .await;
    wait_for_blocked_clients(&mut inspector, 0, Duration::from_secs(1)).await;

    let pipelined_blpop = read_exact_with_timeout(&mut pipelined, 23, Duration::from_secs(1)).await;
    assert_eq!(pipelined_blpop, b"*2\r\n$6\r\nmylist\r\n$1\r\n2\r\n");

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn blocking_list_wakeups_increase_rdb_changes_since_last_save() {
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

    let mut waiter = TcpStream::connect(addr).await.unwrap();
    let mut producer = TcpStream::connect(addr).await.unwrap();
    let mut inspector = TcpStream::connect(addr).await.unwrap();

    let info_before = send_and_read_bulk_payload(
        &mut inspector,
        b"*1\r\n$4\r\nINFO\r\n",
        Duration::from_secs(1),
    )
    .await;
    let dirty_before = read_info_u64(&info_before, "rdb_changes_since_last_save").unwrap_or(0);

    waiter
        .write_all(b"*3\r\n$5\r\nBLPOP\r\n$6\r\nlst{t}\r\n$1\r\n0\r\n")
        .await
        .unwrap();
    wait_for_blocked_clients(&mut inspector, 1, Duration::from_secs(1)).await;

    producer
        .write_all(b"*3\r\n$5\r\nLPUSH\r\n$6\r\nlst{t}\r\n$1\r\na\r\n")
        .await
        .unwrap();
    let popped = read_exact_with_timeout(&mut waiter, 23, Duration::from_secs(1)).await;
    assert_eq!(popped, b"*2\r\n$6\r\nlst{t}\r\n$1\r\na\r\n");

    let info_after_blpop = send_and_read_bulk_payload(
        &mut inspector,
        b"*1\r\n$4\r\nINFO\r\n",
        Duration::from_secs(1),
    )
    .await;
    let dirty_after_blpop =
        read_info_u64(&info_after_blpop, "rdb_changes_since_last_save").unwrap_or(0);
    assert_eq!(
        dirty_after_blpop.saturating_sub(dirty_before),
        2,
        "BLPOP wakeup path should increase dirty counter by two (LPUSH + unblocked pop)"
    );

    let info_before_blmove = info_after_blpop;
    let dirty_before_blmove =
        read_info_u64(&info_before_blmove, "rdb_changes_since_last_save").unwrap_or(0);

    waiter
        .write_all(
            b"*6\r\n$6\r\nBLMOVE\r\n$6\r\nlst{t}\r\n$7\r\nlst1{t}\r\n$4\r\nLEFT\r\n$4\r\nLEFT\r\n$1\r\n0\r\n",
        )
        .await
        .unwrap();
    wait_for_blocked_clients(&mut inspector, 1, Duration::from_secs(1)).await;

    producer
        .write_all(b"*3\r\n$5\r\nLPUSH\r\n$6\r\nlst{t}\r\n$1\r\nb\r\n")
        .await
        .unwrap();
    let moved = read_exact_with_timeout(&mut waiter, 7, Duration::from_secs(1)).await;
    assert_eq!(moved, b"$1\r\nb\r\n");

    let info_after_blmove = send_and_read_bulk_payload(
        &mut inspector,
        b"*1\r\n$4\r\nINFO\r\n",
        Duration::from_secs(1),
    )
    .await;
    let dirty_after_blmove =
        read_info_u64(&info_after_blmove, "rdb_changes_since_last_save").unwrap_or(0);
    assert_eq!(
        dirty_after_blmove.saturating_sub(dirty_before_blmove),
        2,
        "BLMOVE wakeup path should increase dirty counter by two (LPUSH + unblocked move)"
    );

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn linked_blmove_chain_is_observable_without_intermediate_residue() {
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

    let mut waiter1 = TcpStream::connect(addr).await.unwrap();
    let mut waiter2 = TcpStream::connect(addr).await.unwrap();
    let mut producer = TcpStream::connect(addr).await.unwrap();
    let mut inspector = TcpStream::connect(addr).await.unwrap();

    for client in [&mut waiter1, &mut waiter2, &mut producer, &mut inspector] {
        send_and_expect(client, b"*2\r\n$6\r\nSELECT\r\n$1\r\n9\r\n", b"+OK\r\n").await;
    }

    send_and_expect(
        &mut inspector,
        b"*4\r\n$3\r\nDEL\r\n$8\r\nlist1{t}\r\n$8\r\nlist2{t}\r\n$8\r\nlist3{t}\r\n",
        b":0\r\n",
    )
    .await;

    waiter1
        .write_all(
            b"*6\r\n$6\r\nBLMOVE\r\n$8\r\nlist1{t}\r\n$8\r\nlist2{t}\r\n$5\r\nRIGHT\r\n$4\r\nLEFT\r\n$1\r\n0\r\n",
        )
        .await
        .unwrap();
    wait_for_blocked_clients(&mut inspector, 1, Duration::from_secs(1)).await;
    waiter2
        .write_all(
            b"*6\r\n$6\r\nBLMOVE\r\n$8\r\nlist2{t}\r\n$8\r\nlist3{t}\r\n$4\r\nLEFT\r\n$5\r\nRIGHT\r\n$1\r\n0\r\n",
        )
        .await
        .unwrap();
    wait_for_blocked_clients(&mut inspector, 2, Duration::from_secs(1)).await;

    send_and_expect(
        &mut producer,
        b"*3\r\n$5\r\nRPUSH\r\n$8\r\nlist1{t}\r\n$3\r\nfoo\r\n",
        b":1\r\n",
    )
    .await;

    send_and_expect(
        &mut inspector,
        b"*4\r\n$6\r\nLRANGE\r\n$8\r\nlist1{t}\r\n$1\r\n0\r\n$2\r\n-1\r\n",
        b"*0\r\n",
    )
    .await;
    send_and_expect(
        &mut inspector,
        b"*4\r\n$6\r\nLRANGE\r\n$8\r\nlist2{t}\r\n$1\r\n0\r\n$2\r\n-1\r\n",
        b"*0\r\n",
    )
    .await;
    send_and_expect(
        &mut inspector,
        b"*4\r\n$6\r\nLRANGE\r\n$8\r\nlist3{t}\r\n$1\r\n0\r\n$2\r\n-1\r\n",
        b"*1\r\n$3\r\nfoo\r\n",
    )
    .await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn brpoplpush_wakeup_invalidates_watch_before_exec() {
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

    let mut blocked_client = TcpStream::connect(addr).await.unwrap();
    let mut watching_client = TcpStream::connect(addr).await.unwrap();
    let mut producer = TcpStream::connect(addr).await.unwrap();
    let mut inspector = TcpStream::connect(addr).await.unwrap();

    send_and_expect(
        &mut producer,
        b"*4\r\n$3\r\nDEL\r\n$10\r\nsrclist{t}\r\n$10\r\ndstlist{t}\r\n$10\r\nsomekey{t}\r\n",
        b":0\r\n",
    )
    .await;
    send_and_expect(
        &mut producer,
        b"*3\r\n$3\r\nSET\r\n$10\r\nsomekey{t}\r\n$9\r\nsomevalue\r\n",
        b"+OK\r\n",
    )
    .await;

    blocked_client
        .write_all(
            b"*4\r\n$10\r\nBRPOPLPUSH\r\n$10\r\nsrclist{t}\r\n$10\r\ndstlist{t}\r\n$1\r\n0\r\n",
        )
        .await
        .unwrap();
    wait_for_blocked_clients(&mut inspector, 1, Duration::from_secs(1)).await;

    send_and_expect(
        &mut watching_client,
        b"*2\r\n$5\r\nWATCH\r\n$10\r\ndstlist{t}\r\n",
        b"+OK\r\n",
    )
    .await;
    send_and_expect(&mut watching_client, b"*1\r\n$5\r\nMULTI\r\n", b"+OK\r\n").await;
    send_and_expect(
        &mut watching_client,
        b"*2\r\n$3\r\nGET\r\n$10\r\nsomekey{t}\r\n",
        b"+QUEUED\r\n",
    )
    .await;

    send_and_expect(
        &mut producer,
        b"*3\r\n$5\r\nLPUSH\r\n$10\r\nsrclist{t}\r\n$7\r\nelement\r\n",
        b":1\r\n",
    )
    .await;
    send_and_expect(&mut watching_client, b"*1\r\n$4\r\nEXEC\r\n", b"*-1\r\n").await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn info_commandstats_counts_blocking_command_once() {
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

    let mut waiter = TcpStream::connect(addr).await.unwrap();
    let mut inspector = TcpStream::connect(addr).await.unwrap();
    let mut producer = TcpStream::connect(addr).await.unwrap();

    send_and_expect(
        &mut inspector,
        b"*2\r\n$6\r\nCONFIG\r\n$9\r\nRESETSTAT\r\n",
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut inspector,
        b"*2\r\n$3\r\nDEL\r\n$6\r\nmylist\r\n",
        b":0\r\n",
    )
    .await;

    waiter
        .write_all(b"*3\r\n$5\r\nBLPOP\r\n$6\r\nmylist\r\n$1\r\n0\r\n")
        .await
        .unwrap();
    wait_for_blocked_clients(&mut inspector, 1, Duration::from_secs(1)).await;

    send_and_expect(
        &mut producer,
        b"*3\r\n$5\r\nLPUSH\r\n$6\r\nmylist\r\n$1\r\n1\r\n",
        b":1\r\n",
    )
    .await;
    let _ = read_exact_with_timeout(&mut waiter, 23, Duration::from_secs(1)).await;

    let payload = send_and_read_bulk_payload(
        &mut inspector,
        b"*2\r\n$4\r\nINFO\r\n$12\r\nCOMMANDSTATS\r\n",
        Duration::from_secs(1),
    )
    .await;
    let payload_text = String::from_utf8_lossy(&payload);
    assert!(
        payload_text.contains("cmdstat_blpop:calls=1"),
        "unexpected INFO COMMANDSTATS payload: {payload_text}"
    );
    assert!(
        payload_text.contains("rejected_calls=0,failed_calls=0"),
        "unexpected INFO COMMANDSTATS payload: {payload_text}"
    );

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn blocking_xread_waiting_new_data_external_scenario_runs_as_tcp_integration_test() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let mut controller = TcpStream::connect(addr).await.unwrap();
    let mut inspector = TcpStream::connect(addr).await.unwrap();
    let mut waiter = TcpStream::connect(addr).await.unwrap();

    // Redis tests/unit/type/stream.tcl:
    // "Blocking XREAD waiting new data"
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"DEL", b"s1{t}", b"s2{t}", b"s3{t}"]),
        b":0\r\n",
    )
    .await;
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"XADD", b"s2{t}", b"90-0", b"old", b"abcd1234"]),
        b"$4\r\n90-0\r\n",
    )
    .await;

    waiter
        .write_all(&encode_resp_command(&[
            b"XREAD", b"BLOCK", b"20000", b"STREAMS", b"s1{t}", b"s2{t}", b"s3{t}", b"$", b"$",
            b"$",
        ]))
        .await
        .unwrap();
    wait_for_blocked_clients(&mut inspector, 1, Duration::from_secs(1)).await;

    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"XADD", b"s2{t}", b"100-0", b"new", b"abcd1234"]),
        b"$5\r\n100-0\r\n",
    )
    .await;

    let response = read_exact_with_timeout(
        &mut waiter,
        b"*1\r\n*2\r\n$5\r\ns2{t}\r\n*1\r\n*2\r\n$5\r\n100-0\r\n*2\r\n$3\r\nnew\r\n$8\r\nabcd1234\r\n"
            .len(),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(
        response,
        b"*1\r\n*2\r\n$5\r\ns2{t}\r\n*1\r\n*2\r\n$5\r\n100-0\r\n*2\r\n$3\r\nnew\r\n$8\r\nabcd1234\r\n"
    );

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn blocking_xread_last_element_plus_from_empty_stream_external_scenario_runs_as_tcp_integration_test()
 {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let mut controller = TcpStream::connect(addr).await.unwrap();
    let mut inspector = TcpStream::connect(addr).await.unwrap();
    let mut waiter = TcpStream::connect(addr).await.unwrap();

    // Redis tests/unit/type/stream.tcl:
    // "XREAD last element blocking from empty stream"
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"DEL", b"lestream"]),
        b":0\r\n",
    )
    .await;

    waiter
        .write_all(&encode_resp_command(&[
            b"XREAD",
            b"BLOCK",
            b"20000",
            b"STREAMS",
            b"lestream",
            b"+",
        ]))
        .await
        .unwrap();
    wait_for_blocked_clients(&mut inspector, 1, Duration::from_secs(1)).await;

    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"XADD", b"lestream", b"1-0", b"k1", b"v1"]),
        b"$3\r\n1-0\r\n",
    )
    .await;

    let expected =
        b"*1\r\n*2\r\n$8\r\nlestream\r\n*1\r\n*2\r\n$3\r\n1-0\r\n*2\r\n$2\r\nk1\r\n$2\r\nv1\r\n";
    let response =
        read_exact_with_timeout(&mut waiter, expected.len(), Duration::from_secs(1)).await;
    assert_eq!(response, expected);

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn blocking_xread_last_element_plus_from_non_empty_stream_external_scenario_runs_as_tcp_integration_test()
 {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let mut client = TcpStream::connect(addr).await.unwrap();

    // Redis tests/unit/type/stream.tcl:
    // "XREAD last element blocking from non-empty stream"
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEL", b"lestream"]),
        b":0\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"XADD", b"lestream", b"1-0", b"k1", b"v1"]),
        b"$3\r\n1-0\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"XADD", b"lestream", b"2-0", b"k2", b"v2"]),
        b"$3\r\n2-0\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"XADD", b"lestream", b"3-0", b"k3", b"v3"]),
        b"$3\r\n3-0\r\n",
    )
    .await;

    send_and_expect(
        &mut client,
        &encode_resp_command(&[
            b"XREAD",
            b"BLOCK",
            b"1000000",
            b"STREAMS",
            b"lestream",
            b"+",
        ]),
        b"*1\r\n*2\r\n$8\r\nlestream\r\n*1\r\n*2\r\n$3\r\n3-0\r\n*2\r\n$2\r\nk3\r\n$2\r\nv3\r\n",
    )
    .await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn blocking_xread_streamid_edge_external_scenario_runs_as_tcp_integration_test() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let mut controller = TcpStream::connect(addr).await.unwrap();
    let mut inspector = TcpStream::connect(addr).await.unwrap();
    let mut waiter = TcpStream::connect(addr).await.unwrap();

    // Redis tests/unit/type/stream.tcl:
    // "XREAD streamID edge (blocking)"
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"DEL", b"x"]),
        b":0\r\n",
    )
    .await;

    waiter
        .write_all(&encode_resp_command(&[
            b"XREAD",
            b"BLOCK",
            b"0",
            b"STREAMS",
            b"x",
            b"1-18446744073709551615",
        ]))
        .await
        .unwrap();
    wait_for_blocked_clients(&mut inspector, 1, Duration::from_secs(1)).await;

    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"XADD", b"x", b"1-1", b"f", b"v"]),
        b"$3\r\n1-1\r\n",
    )
    .await;
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"XADD", b"x", b"1-18446744073709551615", b"f", b"v"]),
        b"$22\r\n1-18446744073709551615\r\n",
    )
    .await;
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"XADD", b"x", b"2-1", b"f", b"v"]),
        b"$3\r\n2-1\r\n",
    )
    .await;

    let expected = b"*1\r\n*2\r\n$1\r\nx\r\n*1\r\n*2\r\n$3\r\n2-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n";
    let response =
        read_exact_with_timeout(&mut waiter, expected.len(), Duration::from_secs(1)).await;
    assert_eq!(response, expected);

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn blocking_xreadgroup_with_list_waiter_on_same_key_external_scenario_runs_as_tcp_integration_test()
 {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let mut controller = TcpStream::connect(addr).await.unwrap();
    let mut inspector = TcpStream::connect(addr).await.unwrap();
    let mut list_waiter = TcpStream::connect(addr).await.unwrap();
    let mut stream_waiter = TcpStream::connect(addr).await.unwrap();

    // Redis tests/unit/type/stream-cgroups.tcl:
    // "Blocking XREADGROUP for stream key that has clients blocked on list"
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"DEL", b"mystream"]),
        b":0\r\n",
    )
    .await;

    let list_waiter_id = send_and_read_integer(
        &mut list_waiter,
        &encode_resp_command(&[b"CLIENT", b"ID"]),
        Duration::from_secs(1),
    )
    .await;
    let list_waiter_id_text = list_waiter_id.to_string();

    list_waiter
        .write_all(&encode_resp_command(&[b"BLPOP", b"mystream", b"0"]))
        .await
        .unwrap();
    wait_for_blocked_clients(&mut inspector, 1, Duration::from_secs(1)).await;
    let info_after_list_wait = send_and_read_bulk_payload(
        &mut inspector,
        &encode_resp_command(&[b"INFO", b"clients"]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(
        read_info_u64(&info_after_list_wait, "total_blocking_keys"),
        Some(1)
    );
    assert_eq!(
        read_info_u64(&info_after_list_wait, "total_blocking_keys_on_nokey"),
        Some(0)
    );

    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"XADD", b"mystream", b"666-0", b"key", b"value"]),
        b"$5\r\n666-0\r\n",
    )
    .await;
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[
            b"XGROUP",
            b"CREATE",
            b"mystream",
            b"mygroup",
            b"$",
            b"MKSTREAM",
        ]),
        b"+OK\r\n",
    )
    .await;

    stream_waiter
        .write_all(&encode_resp_command(&[
            b"XREADGROUP",
            b"GROUP",
            b"mygroup",
            b"myconsumer",
            b"BLOCK",
            b"0",
            b"STREAMS",
            b"mystream",
            b">",
        ]))
        .await
        .unwrap();
    wait_for_blocked_clients(&mut inspector, 2, Duration::from_secs(1)).await;
    let info_after_stream_wait = send_and_read_bulk_payload(
        &mut inspector,
        &encode_resp_command(&[b"INFO", b"clients"]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(
        read_info_u64(&info_after_stream_wait, "total_blocking_keys"),
        Some(1)
    );
    assert_eq!(
        read_info_u64(&info_after_stream_wait, "total_blocking_keys_on_nokey"),
        Some(1)
    );

    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"DEL", b"mystream"]),
        b":1\r\n",
    )
    .await;
    let error = read_resp_line_with_timeout(&mut stream_waiter, Duration::from_secs(1)).await;
    assert!(
        error.starts_with(b"-NOGROUP "),
        "expected NOGROUP wakeup, got: {}",
        String::from_utf8_lossy(&error)
    );
    let info_after_stream_unblock = send_and_read_bulk_payload(
        &mut inspector,
        &encode_resp_command(&[b"INFO", b"clients"]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(
        read_info_u64(&info_after_stream_unblock, "total_blocking_keys"),
        Some(1)
    );
    assert_eq!(
        read_info_u64(&info_after_stream_unblock, "total_blocking_keys_on_nokey"),
        Some(0)
    );

    send_and_expect(
        &mut controller,
        &encode_resp_command(&[
            b"CLIENT",
            b"UNBLOCK",
            list_waiter_id_text.as_bytes(),
            b"TIMEOUT",
        ]),
        b":1\r\n",
    )
    .await;
    let list_response = read_exact_with_timeout(&mut list_waiter, 5, Duration::from_secs(1)).await;
    assert_eq!(list_response, b"*-1\r\n");
    wait_for_blocked_clients(&mut inspector, 0, Duration::from_secs(1)).await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn xgroup_destroy_unblocks_xreadgroup_with_nogroup_external_scenario_runs_as_tcp_integration_test()
 {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let mut controller = TcpStream::connect(addr).await.unwrap();
    let mut inspector = TcpStream::connect(addr).await.unwrap();
    let mut waiter = TcpStream::connect(addr).await.unwrap();

    // Redis tests/unit/type/stream-cgroups.tcl:
    // "XGROUP DESTROY should unblock XREADGROUP with -NOGROUP"
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"CONFIG", b"RESETSTAT"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"DEL", b"mystream"]),
        b":0\r\n",
    )
    .await;
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[
            b"XGROUP",
            b"CREATE",
            b"mystream",
            b"mygroup",
            b"$",
            b"MKSTREAM",
        ]),
        b"+OK\r\n",
    )
    .await;

    waiter
        .write_all(&encode_resp_command(&[
            b"XREADGROUP",
            b"GROUP",
            b"mygroup",
            b"Alice",
            b"BLOCK",
            b"0",
            b"STREAMS",
            b"mystream",
            b">",
        ]))
        .await
        .unwrap();
    wait_for_blocked_clients(&mut inspector, 1, Duration::from_secs(1)).await;

    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"XGROUP", b"DESTROY", b"mystream", b"mygroup"]),
        b":1\r\n",
    )
    .await;
    let error = read_resp_line_with_timeout(&mut waiter, Duration::from_secs(1)).await;
    assert!(
        error.starts_with(b"-NOGROUP "),
        "expected NOGROUP wakeup, got: {}",
        String::from_utf8_lossy(&error)
    );

    let errorstats = send_and_read_bulk_payload(
        &mut inspector,
        &encode_resp_command(&[b"INFO", b"errorstats"]),
        Duration::from_secs(1),
    )
    .await;
    let errorstats_text = String::from_utf8_lossy(&errorstats);
    assert!(
        errorstats_text.contains("errorstat_NOGROUP:count=1"),
        "unexpected INFO errorstats payload: {errorstats_text}"
    );

    let commandstats = send_and_read_bulk_payload(
        &mut inspector,
        &encode_resp_command(&[b"INFO", b"commandstats"]),
        Duration::from_secs(1),
    )
    .await;
    let commandstats_text = String::from_utf8_lossy(&commandstats);
    assert!(
        commandstats_text.contains("cmdstat_xreadgroup:calls=1"),
        "unexpected INFO commandstats payload: {commandstats_text}"
    );
    assert!(
        commandstats_text.contains("rejected_calls=0,failed_calls=1"),
        "unexpected INFO commandstats payload: {commandstats_text}"
    );

    let stats = send_and_read_bulk_payload(
        &mut inspector,
        &encode_resp_command(&[b"INFO", b"stats"]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(read_info_u64(&stats, "total_error_replies"), Some(1));

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn xgroup_destroy_removes_all_consumer_group_references_external_scenario_runs_as_tcp_integration_test()
 {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let mut client = TcpStream::connect(addr).await.unwrap();

    // Redis tests/unit/type/stream-cgroups.tcl:
    // "XGROUP DESTROY removes all consumer group references"
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEL", b"mystream"]),
        b":0\r\n",
    )
    .await;
    for j in 0..5u64 {
        let id = format!("{j}-1");
        let item = format!("{j}");
        let expected = format!("${}\r\n{}\r\n", id.len(), id);
        send_and_expect(
            &mut client,
            &encode_resp_command(&[
                b"XADD",
                b"mystream",
                id.as_bytes(),
                b"item",
                item.as_bytes(),
            ]),
            expected.as_bytes(),
        )
        .await;
    }
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"XGROUP", b"CREATE", b"mystream", b"mygroup", b"0"]),
        b"+OK\r\n",
    )
    .await;

    let delivered = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[
            b"XREADGROUP",
            b"GROUP",
            b"mygroup",
            b"consumer1",
            b"STREAMS",
            b"mystream",
            b">",
        ]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(resp_socket_array(&delivered).len(), 1);

    let pending = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"XPENDING", b"mystream", b"mygroup"]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(resp_socket_integer(&resp_socket_array(&pending)[0]), 5);

    let blocked_delete = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[
            b"XDELEX",
            b"mystream",
            b"ACKED",
            b"IDS",
            b"5",
            b"0-1",
            b"1-1",
            b"2-1",
            b"3-1",
            b"4-1",
        ]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(
        resp_socket_integer_array(&blocked_delete),
        vec![2, 2, 2, 2, 2]
    );

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"XGROUP", b"DESTROY", b"mystream", b"mygroup"]),
        b":1\r\n",
    )
    .await;

    let deleted = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[
            b"XDELEX",
            b"mystream",
            b"ACKED",
            b"IDS",
            b"5",
            b"0-1",
            b"1-1",
            b"2-1",
            b"3-1",
            b"4-1",
        ]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(resp_socket_integer_array(&deleted), vec![1, 1, 1, 1, 1]);
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"XLEN", b"mystream"]),
        b":0\r\n",
    )
    .await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn xgroup_destroy_updates_acknowledged_delete_references_external_scenario_runs_as_tcp_integration_test()
 {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let mut client = TcpStream::connect(addr).await.unwrap();

    // Redis tests/unit/type/stream-cgroups.tcl:
    // "XGROUP DESTROY correctly manage min_cgroup_last_id cache"
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEL", b"mystream"]),
        b":0\r\n",
    )
    .await;
    for (id, field, value) in [
        (b"1-0", b"f1", b"v1"),
        (b"2-0", b"f2", b"v2"),
        (b"3-0", b"f3", b"v3"),
        (b"4-0", b"f4", b"v4"),
        (b"5-0", b"f5", b"v5"),
    ] {
        let expected = format!("${}\r\n{}\r\n", id.len(), String::from_utf8_lossy(id));
        send_and_expect(
            &mut client,
            &encode_resp_command(&[b"XADD", b"mystream", id, field, value]),
            expected.as_bytes(),
        )
        .await;
    }
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"XGROUP", b"CREATE", b"mystream", b"group1", b"1-0"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"XGROUP", b"CREATE", b"mystream", b"group2", b"3-0"]),
        b"+OK\r\n",
    )
    .await;

    let first_delete = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"XDELEX", b"mystream", b"ACKED", b"IDS", b"1", b"1-0"]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(resp_socket_integer_array(&first_delete), vec![1]);

    let still_referenced = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"XDELEX", b"mystream", b"ACKED", b"IDS", b"1", b"2-0"]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(resp_socket_integer_array(&still_referenced), vec![2]);

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"XGROUP", b"DESTROY", b"mystream", b"group1"]),
        b":1\r\n",
    )
    .await;

    let after_destroy = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"XDELEX", b"mystream", b"ACKED", b"IDS", b"1", b"2-0"]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(resp_socket_integer_array(&after_destroy), vec![1]);

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn xackdel_surface_external_scenarios_run_as_tcp_integration_test() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let mut client = TcpStream::connect(addr).await.unwrap();

    // Redis tests/unit/type/stream-cgroups.tcl:
    // - "XACKDEL wrong number of args"
    // - "XACKDEL should return empty array when key doesn't exist or group doesn't exist"
    // - "XACKDEL IDS parameter validation"
    // - "XACKDEL KEEPREF/DELREF/ACKED parameter validation"
    for request in [
        vec![b"XACKDEL".as_slice()],
        vec![b"XACKDEL".as_slice(), b"s"],
        vec![b"XACKDEL".as_slice(), b"s", b"g"],
    ] {
        let error = send_and_read_error_line(
            &mut client,
            &encode_resp_command(&request),
            Duration::from_secs(1),
        )
        .await;
        assert_eq!(error, "ERR wrong number of arguments for 'xackdel' command");
    }

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEL", b"s"]),
        b":0\r\n",
    )
    .await;
    let missing_key = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"XACKDEL", b"s", b"g", b"IDS", b"2", b"1-1", b"2-2"]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(resp_socket_integer_array(&missing_key), vec![-1, -1]);

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"XADD", b"s", b"1-0", b"f", b"v"]),
        b"$3\r\n1-0\r\n",
    )
    .await;
    let missing_group = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"XACKDEL", b"s", b"g", b"IDS", b"2", b"1-1", b"2-2"]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(resp_socket_integer_array(&missing_group), vec![-1, -1]);

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"XGROUP", b"CREATE", b"s", b"g", b"0"]),
        b"+OK\r\n",
    )
    .await;

    for invalid_numids in [b"abc".as_slice(), b"0".as_slice(), b"-5".as_slice()] {
        let error = send_and_read_error_line(
            &mut client,
            &encode_resp_command(&[b"XACKDEL", b"s", b"g", b"IDS", invalid_numids, b"1-1"]),
            Duration::from_secs(1),
        )
        .await;
        assert_eq!(error, "ERR Number of IDs must be a positive integer");
    }

    let mismatch = send_and_read_error_line(
        &mut client,
        &encode_resp_command(&[b"XACKDEL", b"s", b"g", b"IDS", b"3", b"1-1", b"2-2"]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(
        mismatch,
        "ERR The `numids` parameter must match the number of arguments"
    );

    let syntax_error = send_and_read_error_line(
        &mut client,
        &encode_resp_command(&[b"XACKDEL", b"s", b"g", b"IDS", b"1", b"1-1", b"2-2"]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(syntax_error, "ERR syntax error");

    for request in [
        vec![
            b"XACKDEL".as_slice(),
            b"s",
            b"g",
            b"KEEPREF",
            b"DELREF",
            b"IDS",
            b"1",
            b"1-1",
        ],
        vec![
            b"XACKDEL".as_slice(),
            b"s",
            b"g",
            b"KEEPREF",
            b"ACKED",
            b"IDS",
            b"1",
            b"1-1",
        ],
        vec![
            b"XACKDEL".as_slice(),
            b"s",
            b"g",
            b"DELREF",
            b"ACKED",
            b"IDS",
            b"1",
            b"1-1",
        ],
    ] {
        let error = send_and_read_error_line(
            &mut client,
            &encode_resp_command(&request),
            Duration::from_secs(1),
        )
        .await;
        assert_eq!(error, "ERR syntax error");
    }

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn xackdel_reference_modes_external_scenarios_run_as_tcp_integration_test() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let mut client = TcpStream::connect(addr).await.unwrap();

    // Redis tests/unit/type/stream-cgroups.tcl:
    // - "XACKDEL with DELREF option acknowledges will remove entry from all PELs"
    // - "XACKDEL with ACKED option only deletes messages acknowledged by all groups"
    // - "XACKDEL with KEEPREF"
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEL", b"mystream"]),
        b":0\r\n",
    )
    .await;
    for id in [b"1-0".as_slice(), b"2-0".as_slice()] {
        let expected = format!("${}\r\n{}\r\n", id.len(), String::from_utf8_lossy(id));
        send_and_expect(
            &mut client,
            &encode_resp_command(&[b"XADD", b"mystream", id, b"f", b"v"]),
            expected.as_bytes(),
        )
        .await;
    }
    for group in [b"group1".as_slice(), b"group2".as_slice()] {
        send_and_expect(
            &mut client,
            &encode_resp_command(&[b"XGROUP", b"CREATE", b"mystream", group, b"0"]),
            b"+OK\r\n",
        )
        .await;
    }
    for (group, consumer) in [
        (b"group1".as_slice(), b"consumer1".as_slice()),
        (b"group2".as_slice(), b"consumer2".as_slice()),
    ] {
        let delivered = send_and_read_resp_value(
            &mut client,
            &encode_resp_command(&[
                b"XREADGROUP",
                b"GROUP",
                group,
                consumer,
                b"STREAMS",
                b"mystream",
                b">",
            ]),
            Duration::from_secs(1),
        )
        .await;
        assert_eq!(resp_socket_array(&delivered).len(), 1);
    }

    let delref = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[
            b"XACKDEL",
            b"mystream",
            b"group1",
            b"DELREF",
            b"IDS",
            b"2",
            b"1-0",
            b"2-0",
        ]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(resp_socket_integer_array(&delref), vec![1, 1]);
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"XLEN", b"mystream"]),
        b":0\r\n",
    )
    .await;

    let pending_group1 = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"XPENDING", b"mystream", b"group1"]),
        Duration::from_secs(1),
    )
    .await;
    let pending_group1_items = resp_socket_array(&pending_group1);
    assert_eq!(resp_socket_integer(&pending_group1_items[0]), 0);
    assert!(matches!(pending_group1_items[1], RespSocketValue::Null));
    assert!(matches!(pending_group1_items[2], RespSocketValue::Null));
    assert!(resp_socket_array(&pending_group1_items[3]).is_empty());

    let pending_group2 = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"XPENDING", b"mystream", b"group2"]),
        Duration::from_secs(1),
    )
    .await;
    let pending_group2_items = resp_socket_array(&pending_group2);
    assert_eq!(resp_socket_integer(&pending_group2_items[0]), 0);
    assert!(matches!(pending_group2_items[1], RespSocketValue::Null));
    assert!(matches!(pending_group2_items[2], RespSocketValue::Null));
    assert!(resp_socket_array(&pending_group2_items[3]).is_empty());

    let missing_after_delref = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[
            b"XACKDEL",
            b"mystream",
            b"group2",
            b"DELREF",
            b"IDS",
            b"2",
            b"1-0",
            b"2-0",
        ]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(
        resp_socket_integer_array(&missing_after_delref),
        vec![-1, -1]
    );

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEL", b"mystream"]),
        b":1\r\n",
    )
    .await;
    for id in [b"1-0".as_slice(), b"2-0".as_slice()] {
        let expected = format!("${}\r\n{}\r\n", id.len(), String::from_utf8_lossy(id));
        send_and_expect(
            &mut client,
            &encode_resp_command(&[b"XADD", b"mystream", id, b"f", b"v"]),
            expected.as_bytes(),
        )
        .await;
    }
    for group in [b"group1".as_slice(), b"group2".as_slice()] {
        send_and_expect(
            &mut client,
            &encode_resp_command(&[b"XGROUP", b"CREATE", b"mystream", group, b"0"]),
            b"+OK\r\n",
        )
        .await;
    }
    for (group, consumer) in [
        (b"group1".as_slice(), b"consumer1".as_slice()),
        (b"group2".as_slice(), b"consumer2".as_slice()),
    ] {
        let delivered = send_and_read_resp_value(
            &mut client,
            &encode_resp_command(&[
                b"XREADGROUP",
                b"GROUP",
                group,
                consumer,
                b"STREAMS",
                b"mystream",
                b">",
            ]),
            Duration::from_secs(1),
        )
        .await;
        assert_eq!(resp_socket_array(&delivered).len(), 1);
    }

    let acked_first_group = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[
            b"XACKDEL",
            b"mystream",
            b"group1",
            b"ACKED",
            b"IDS",
            b"2",
            b"1-0",
            b"2-0",
        ]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(resp_socket_integer_array(&acked_first_group), vec![2, 2]);
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"XLEN", b"mystream"]),
        b":2\r\n",
    )
    .await;
    let pending_after_first_ack = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"XPENDING", b"mystream", b"group1"]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(
        resp_socket_integer(&resp_socket_array(&pending_after_first_ack)[0]),
        0
    );
    let pending_after_first_ack_group2 = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"XPENDING", b"mystream", b"group2"]),
        Duration::from_secs(1),
    )
    .await;
    let group2_items = resp_socket_array(&pending_after_first_ack_group2);
    assert_eq!(resp_socket_integer(&group2_items[0]), 2);
    assert_eq!(resp_socket_bulk(&group2_items[1]), b"1-0");
    assert_eq!(resp_socket_bulk(&group2_items[2]), b"2-0");
    let group2_consumers = resp_socket_array(&group2_items[3]);
    assert_eq!(group2_consumers.len(), 1);
    let group2_consumer = resp_socket_array(&group2_consumers[0]);
    assert_eq!(resp_socket_bulk(&group2_consumer[0]), b"consumer2");
    assert_eq!(resp_socket_integer(&group2_consumer[1]), 2);

    let acked_second_group = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[
            b"XACKDEL",
            b"mystream",
            b"group2",
            b"ACKED",
            b"IDS",
            b"2",
            b"1-0",
            b"2-0",
        ]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(resp_socket_integer_array(&acked_second_group), vec![1, 1]);
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"XLEN", b"mystream"]),
        b":0\r\n",
    )
    .await;

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEL", b"mystream"]),
        b":1\r\n",
    )
    .await;
    for id in [b"1-0".as_slice(), b"2-0".as_slice()] {
        let expected = format!("${}\r\n{}\r\n", id.len(), String::from_utf8_lossy(id));
        send_and_expect(
            &mut client,
            &encode_resp_command(&[b"XADD", b"mystream", id, b"f", b"v"]),
            expected.as_bytes(),
        )
        .await;
    }
    for group in [b"group1".as_slice(), b"group2".as_slice()] {
        send_and_expect(
            &mut client,
            &encode_resp_command(&[b"XGROUP", b"CREATE", b"mystream", group, b"0"]),
            b"+OK\r\n",
        )
        .await;
    }
    for (group, consumer) in [
        (b"group1".as_slice(), b"consumer1".as_slice()),
        (b"group2".as_slice(), b"consumer2".as_slice()),
    ] {
        let delivered = send_and_read_resp_value(
            &mut client,
            &encode_resp_command(&[
                b"XREADGROUP",
                b"GROUP",
                group,
                consumer,
                b"STREAMS",
                b"mystream",
                b">",
            ]),
            Duration::from_secs(1),
        )
        .await;
        assert_eq!(resp_socket_array(&delivered).len(), 1);
    }

    let keepref_group1 = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[
            b"XACKDEL",
            b"mystream",
            b"group1",
            b"KEEPREF",
            b"IDS",
            b"2",
            b"1-0",
            b"2-0",
        ]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(resp_socket_integer_array(&keepref_group1), vec![1, 1]);
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"XLEN", b"mystream"]),
        b":0\r\n",
    )
    .await;
    let keepref_group2_pending = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"XPENDING", b"mystream", b"group2"]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(
        resp_socket_integer(&resp_socket_array(&keepref_group2_pending)[0]),
        2
    );

    let keepref_group2 = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[
            b"XACKDEL",
            b"mystream",
            b"group2",
            b"KEEPREF",
            b"IDS",
            b"2",
            b"1-0",
            b"2-0",
        ]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(resp_socket_integer_array(&keepref_group2), vec![1, 1]);
    let keepref_group1_pending = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"XPENDING", b"mystream", b"group1"]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(
        resp_socket_integer(&resp_socket_array(&keepref_group1_pending)[0]),
        0
    );
    let keepref_group2_pending_after = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"XPENDING", b"mystream", b"group2"]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(
        resp_socket_integer(&resp_socket_array(&keepref_group2_pending_after)[0]),
        0
    );

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn xackdel_many_ids_external_scenario_runs_as_tcp_integration_test() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let mut client = TcpStream::connect(addr).await.unwrap();

    // Redis tests/unit/type/stream-cgroups.tcl:
    // "XACKDEL with IDs exceeding STREAMID_STATIC_VECTOR_LEN for heap allocation"
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEL", b"mystream"]),
        b":0\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[
            b"XGROUP",
            b"CREATE",
            b"mystream",
            b"mygroup",
            b"$",
            b"MKSTREAM",
        ]),
        b"+OK\r\n",
    )
    .await;

    let mut command = vec![
        b"XACKDEL".as_slice(),
        b"mystream",
        b"mygroup",
        b"IDS",
        b"50",
    ];
    let ids: Vec<String> = (0..50).map(|index| format!("{index}-1")).collect();
    for id in &ids {
        command.push(id.as_bytes());
    }
    let reply = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&command),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(resp_socket_array(&reply).len(), 50);
    assert!(
        resp_socket_integer_array(&reply)
            .iter()
            .all(|value| *value == -1)
    );

    send_and_expect(&mut client, &encode_resp_command(&[b"PING"]), b"+PONG\r\n").await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn xreadgroup_dirty_semantics_match_stream_cgroups_external_scenarios() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let mut controller = TcpStream::connect(addr).await.unwrap();
    let mut inspector = TcpStream::connect(addr).await.unwrap();

    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"DEL", b"x"]),
        b":0\r\n",
    )
    .await;
    for (id, value) in [
        (b"1-0", b"a"),
        (b"2-0", b"b"),
        (b"3-0", b"c"),
        (b"4-0", b"d"),
    ] {
        let mut expected = Vec::new();
        expected.extend_from_slice(format!("${}\r\n", id.len()).as_bytes());
        expected.extend_from_slice(id);
        expected.extend_from_slice(b"\r\n");
        send_and_expect(
            &mut controller,
            &encode_resp_command(&[b"XADD", b"x", id, b"data", value]),
            &expected,
        )
        .await;
    }
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"XGROUP", b"CREATE", b"x", b"g1", b"0"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"XGROUP", b"CREATECONSUMER", b"x", b"g1", b"Alice"]),
        b":1\r\n",
    )
    .await;

    let info_before = send_and_read_bulk_payload(
        &mut inspector,
        &encode_resp_command(&[b"INFO"]),
        Duration::from_secs(1),
    )
    .await;
    let dirty_before = read_info_u64(&info_before, "rdb_changes_since_last_save").unwrap_or(0);
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[
            b"XREADGROUP",
            b"GROUP",
            b"g1",
            b"Alice",
            b"COUNT",
            b"2",
            b"STREAMS",
            b"x",
            b">",
        ]),
        b"*1\r\n*2\r\n$1\r\nx\r\n*2\r\n*2\r\n$3\r\n1-0\r\n*2\r\n$4\r\ndata\r\n$1\r\na\r\n*2\r\n$3\r\n2-0\r\n*2\r\n$4\r\ndata\r\n$1\r\nb\r\n",
    )
    .await;
    let info_after = send_and_read_bulk_payload(
        &mut inspector,
        &encode_resp_command(&[b"INFO"]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(
        read_info_u64(&info_after, "rdb_changes_since_last_save").unwrap_or(0) - dirty_before,
        1
    );

    let dirty_before = read_info_u64(&info_after, "rdb_changes_since_last_save").unwrap_or(0);
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[
            b"XREADGROUP",
            b"GROUP",
            b"g1",
            b"Alice",
            b"NOACK",
            b"COUNT",
            b"2",
            b"STREAMS",
            b"x",
            b">",
        ]),
        b"*1\r\n*2\r\n$1\r\nx\r\n*2\r\n*2\r\n$3\r\n3-0\r\n*2\r\n$4\r\ndata\r\n$1\r\nc\r\n*2\r\n$3\r\n4-0\r\n*2\r\n$4\r\ndata\r\n$1\r\nd\r\n",
    )
    .await;
    let info_after = send_and_read_bulk_payload(
        &mut inspector,
        &encode_resp_command(&[b"INFO"]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(
        read_info_u64(&info_after, "rdb_changes_since_last_save").unwrap_or(0) - dirty_before,
        1
    );

    let dirty_before = read_info_u64(&info_after, "rdb_changes_since_last_save").unwrap_or(0);
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[
            b"XREADGROUP",
            b"GROUP",
            b"g1",
            b"Alice",
            b"COUNT",
            b"2",
            b"STREAMS",
            b"x",
            b"0",
        ]),
        b"*1\r\n*2\r\n$1\r\nx\r\n*2\r\n*2\r\n$3\r\n1-0\r\n*2\r\n$4\r\ndata\r\n$1\r\na\r\n*2\r\n$3\r\n2-0\r\n*2\r\n$4\r\ndata\r\n$1\r\nb\r\n",
    )
    .await;
    let info_after = send_and_read_bulk_payload(
        &mut inspector,
        &encode_resp_command(&[b"INFO"]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(
        read_info_u64(&info_after, "rdb_changes_since_last_save").unwrap_or(0) - dirty_before,
        0
    );

    let dirty_before = read_info_u64(&info_after, "rdb_changes_since_last_save").unwrap_or(0);
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[
            b"XREADGROUP",
            b"GROUP",
            b"g1",
            b"Alice",
            b"COUNT",
            b"2",
            b"STREAMS",
            b"x",
            b"9000",
        ]),
        b"*1\r\n*2\r\n$1\r\nx\r\n*0\r\n",
    )
    .await;
    let info_after = send_and_read_bulk_payload(
        &mut inspector,
        &encode_resp_command(&[b"INFO"]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(
        read_info_u64(&info_after, "rdb_changes_since_last_save").unwrap_or(0) - dirty_before,
        0
    );

    let dirty_before = read_info_u64(&info_after, "rdb_changes_since_last_save").unwrap_or(0);
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[
            b"XREADGROUP",
            b"GROUP",
            b"g1",
            b"noconsumer",
            b"COUNT",
            b"2",
            b"STREAMS",
            b"x",
            b"0",
        ]),
        b"*1\r\n*2\r\n$1\r\nx\r\n*0\r\n",
    )
    .await;
    let info_after = send_and_read_bulk_payload(
        &mut inspector,
        &encode_resp_command(&[b"INFO"]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(
        read_info_u64(&info_after, "rdb_changes_since_last_save").unwrap_or(0) - dirty_before,
        1
    );

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn consumer_without_pel_is_present_after_aofrw_external_scenario_runs_as_tcp_integration_test()
 {
    let processor =
        Arc::new(RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap());
    let temp_dir = configure_processor_aof_for_test(processor.as_ref(), "consumer-aofrw");
    let (addr, shutdown_tx, server) = start_test_server_with_processor(processor).await;
    let mut controller = TcpStream::connect(addr).await.unwrap();
    let mut inspector = TcpStream::connect(addr).await.unwrap();
    let mut blocked = TcpStream::connect(addr).await.unwrap();

    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"DEL", b"mystream"]),
        b":0\r\n",
    )
    .await;
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[
            b"XGROUP",
            b"CREATE",
            b"mystream",
            b"mygroup",
            b"$",
            b"MKSTREAM",
        ]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"XADD", b"mystream", b"1-0", b"f", b"v"]),
        b"$3\r\n1-0\r\n",
    )
    .await;
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[
            b"XREADGROUP",
            b"GROUP",
            b"mygroup",
            b"Alice",
            b"NOACK",
            b"STREAMS",
            b"mystream",
            b">",
        ]),
        b"*1\r\n*2\r\n$8\r\nmystream\r\n*1\r\n*2\r\n$3\r\n1-0\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n",
    )
    .await;

    blocked
        .write_all(&encode_resp_command(&[
            b"XREADGROUP",
            b"GROUP",
            b"mygroup",
            b"Bob",
            b"BLOCK",
            b"0",
            b"NOACK",
            b"STREAMS",
            b"mystream",
            b">",
        ]))
        .await
        .unwrap();
    wait_for_blocked_clients(&mut inspector, 1, Duration::from_secs(1)).await;

    send_and_expect(
        &mut controller,
        &encode_resp_command(&[
            b"XGROUP",
            b"CREATECONSUMER",
            b"mystream",
            b"mygroup",
            b"Charlie",
        ]),
        b":1\r\n",
    )
    .await;

    let groups_before = send_and_read_resp_value(
        &mut controller,
        &encode_resp_command(&[b"XINFO", b"GROUPS", b"mystream"]),
        Duration::from_secs(1),
    )
    .await;
    let groups_before_array = resp_socket_array(&groups_before);
    assert_eq!(groups_before_array.len(), 1);
    let group_before = resp_socket_flat_map(&groups_before_array[0]);
    assert_eq!(resp_socket_integer(group_before[&b"consumers".to_vec()]), 3);

    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"BGREWRITEAOF"]),
        b"+Background append only file rewriting started\r\n",
    )
    .await;
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"DEBUG", b"LOADAOF"]),
        b"+OK\r\n",
    )
    .await;

    let groups_after = send_and_read_resp_value(
        &mut controller,
        &encode_resp_command(&[b"XINFO", b"GROUPS", b"mystream"]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(groups_after, groups_before);
    let groups_after_array = resp_socket_array(&groups_after);
    let group_after = resp_socket_flat_map(&groups_after_array[0]);
    assert_eq!(resp_socket_integer(group_after[&b"consumers".to_vec()]), 3);

    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"XADD", b"mystream", b"2-0", b"f", b"v2"]),
        b"$3\r\n2-0\r\n",
    )
    .await;
    let blocked_response = read_exact_with_timeout(
        &mut blocked,
        b"*1\r\n*2\r\n$8\r\nmystream\r\n*1\r\n*2\r\n$3\r\n2-0\r\n*2\r\n$1\r\nf\r\n$2\r\nv2\r\n"
            .len(),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(
        blocked_response,
        b"*1\r\n*2\r\n$8\r\nmystream\r\n*1\r\n*2\r\n$3\r\n2-0\r\n*2\r\n$1\r\nf\r\n$2\r\nv2\r\n"
    );

    let _ = shutdown_tx.send(());
    server.await.unwrap();
    let _ = std::fs::remove_dir_all(temp_dir);
}

#[tokio::test]
async fn stream_aof_rewrite_after_xdel_lastid_matches_external_scenario() {
    let processor =
        Arc::new(RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap());
    let temp_dir = configure_processor_aof_for_test(processor.as_ref(), "stream-aofrw");
    let (addr, shutdown_tx, server) = start_test_server_with_processor(processor).await;
    let mut client = TcpStream::connect(addr).await.unwrap();

    // Redis tests/unit/type/stream.tcl:
    // - "Empty stream can be rewrite into AOF correctly"
    // - "Stream can be rewrite into AOF correctly after XDEL lastid"
    let empty_create = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"XADD", b"mystream", b"MAXLEN", b"0", b"*", b"a", b"b"]),
        Duration::from_secs(1),
    )
    .await;
    assert!(
        matches!(empty_create, RespSocketValue::Bulk(_)),
        "expected XADD MAXLEN 0 to return a bulk stream ID, got {empty_create:?}"
    );

    let empty_info = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"XINFO", b"STREAM", b"mystream"]),
        Duration::from_secs(1),
    )
    .await;
    let empty_info_map = resp_socket_flat_map(&empty_info);
    assert_eq!(resp_socket_integer(empty_info_map[&b"length".to_vec()]), 0);

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"XSETID", b"mystream", b"0-0"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"XADD", b"mystream", b"1-1", b"a", b"b"]),
        b"$3\r\n1-1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"XADD", b"mystream", b"2-2", b"a", b"b"]),
        b"$3\r\n2-2\r\n",
    )
    .await;

    let before_delete = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"XINFO", b"STREAM", b"mystream"]),
        Duration::from_secs(1),
    )
    .await;
    let before_delete_map = resp_socket_flat_map(&before_delete);
    assert_eq!(
        resp_socket_integer(before_delete_map[&b"length".to_vec()]),
        2
    );

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"XDEL", b"mystream", b"2-2"]),
        b":1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"BGREWRITEAOF"]),
        b"+Background append only file rewriting started\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEBUG", b"LOADAOF"]),
        b"+OK\r\n",
    )
    .await;

    let after_load = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"XINFO", b"STREAM", b"mystream"]),
        Duration::from_secs(1),
    )
    .await;
    let after_load_map = resp_socket_flat_map(&after_load);
    assert_eq!(resp_socket_integer(after_load_map[&b"length".to_vec()]), 1);
    assert_eq!(
        resp_socket_bulk(after_load_map[&b"last-generated-id".to_vec()]),
        b"2-2"
    );

    let _ = shutdown_tx.send(());
    server.await.unwrap();
    let _ = std::fs::remove_dir_all(temp_dir);
}

#[tokio::test]
async fn zrandmember_skiplist_external_scenario_runs_as_tcp_integration_test() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let mut client = TcpStream::connect(addr).await.unwrap();

    // Redis tests/unit/type/zset.tcl:
    // - "ZRANDMEMBER count overflow"
    // - "ZRANDMEMBER count of 0 is handled correctly - emptyarray"
    // - "ZRANDMEMBER with <count> against non existing key - emptyarray"
    // - "ZRANDMEMBER with <count> - skiplist"
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEL", b"myzset", b"nonexisting_key"]),
        b":0\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"ZADD", b"myzset", b"0", b"a"]),
        b":1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[
            b"ZRANDMEMBER",
            b"myzset",
            b"-9223372036854770000",
            b"WITHSCORES",
        ]),
        b"-ERR value is out of range\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[
            b"ZRANDMEMBER",
            b"myzset",
            b"-9223372036854775808",
            b"WITHSCORES",
        ]),
        b"-ERR value is out of range\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"ZRANDMEMBER", b"myzset", b"-9223372036854775808"]),
        b"-ERR value is out of range\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"ZRANDMEMBER", b"myzset", b"0"]),
        b"*0\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"ZRANDMEMBER", b"nonexisting_key", b"100"]),
        b"*0\r\n",
    )
    .await;

    let original_max_value = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"CONFIG", b"GET", b"zset-max-ziplist-value"]),
        Duration::from_secs(1),
    )
    .await;
    let original_max_value_items = resp_socket_array(&original_max_value);
    assert_eq!(
        resp_socket_bulk(&original_max_value_items[0]),
        b"zset-max-ziplist-value"
    );

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"CONFIG", b"SET", b"zset-max-ziplist-value", b"10"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEL", b"myzset"]),
        b":1\r\n",
    )
    .await;

    let long_member =
        b"skiplist-member-abcdefghijklmnopqrstuvwxyz-abcdefghijklmnopqrstuvwxyz-0123456789";
    let create = vec![
        b"ZADD".as_slice(),
        b"myzset",
        b"1",
        b"a",
        b"2",
        b"b",
        b"3",
        b"c",
        b"4",
        b"d",
        b"5",
        b"e",
        b"6",
        b"f",
        b"7",
        b"g",
        b"7",
        b"h",
        b"9",
        b"i",
        b"10",
        long_member,
    ];
    send_and_expect(&mut client, &encode_resp_command(&create), b":10\r\n").await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"OBJECT", b"ENCODING", b"myzset"]),
        b"$8\r\nskiplist\r\n",
    )
    .await;

    let members_with_scores = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"ZRANGE", b"myzset", b"0", b"-1", b"WITHSCORES"]),
        Duration::from_secs(1),
    )
    .await;
    let member_score_items = resp_socket_array(&members_with_scores);
    assert_eq!(member_score_items.len(), 20);
    let mut allowed = std::collections::BTreeMap::new();
    for pair in member_score_items.chunks_exact(2) {
        allowed.insert(
            resp_socket_bulk(&pair[0]).to_vec(),
            resp_socket_bulk(&pair[1]).to_vec(),
        );
    }

    let negative_count = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"ZRANDMEMBER", b"myzset", b"-20"]),
        Duration::from_secs(1),
    )
    .await;
    let negative_items = resp_socket_array(&negative_count);
    assert_eq!(negative_items.len(), 20);
    for item in negative_items {
        assert!(allowed.contains_key(resp_socket_bulk(item)));
    }

    let negative_with_scores = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"ZRANDMEMBER", b"myzset", b"-20", b"WITHSCORES"]),
        Duration::from_secs(1),
    )
    .await;
    let negative_with_scores_items = resp_socket_array(&negative_with_scores);
    assert_eq!(negative_with_scores_items.len(), 40);
    for pair in negative_with_scores_items.chunks_exact(2) {
        let member = resp_socket_bulk(&pair[0]).to_vec();
        let score = resp_socket_bulk(&pair[1]);
        assert_eq!(allowed.get(&member).map(Vec::as_slice), Some(score));
    }

    send_and_expect(&mut client, &encode_resp_command(&[b"PING"]), b"+PONG\r\n").await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn zset_regular_set_algebra_external_scenario_runs_as_tcp_integration_test() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let mut client = TcpStream::connect(addr).await.unwrap();

    // Redis tests/unit/type/zset.tcl:
    // - "ZUNIONSTORE with weights - listpack"
    // - "ZUNIONSTORE with a regular set and weights - listpack"
    // - "ZINTERSTORE with weights - listpack"
    // - "ZINTERSTORE with a regular set and weights - listpack"
    // - "ZDIFFSTORE basics - listpack"
    // - "ZDIFFSTORE with a regular set - listpack"
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEL", b"zseta{t}", b"zsetb{t}", b"zsetc{t}", b"seta{t}"]),
        b":0\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"ZADD", b"zseta{t}", b"1", b"a", b"2", b"b", b"3", b"c"]),
        b":3\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"ZADD", b"zsetb{t}", b"1", b"b", b"2", b"c", b"3", b"d"]),
        b":3\r\n",
    )
    .await;

    send_and_expect(
        &mut client,
        &encode_resp_command(&[
            b"ZUNIONSTORE",
            b"zsetc{t}",
            b"2",
            b"zseta{t}",
            b"zsetb{t}",
            b"WEIGHTS",
            b"2",
            b"3",
        ]),
        b":4\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"ZRANGE", b"zsetc{t}", b"0", b"-1", b"WITHSCORES"]),
        b"*8\r\n$1\r\na\r\n$1\r\n2\r\n$1\r\nb\r\n$1\r\n7\r\n$1\r\nd\r\n$1\r\n9\r\n$1\r\nc\r\n$2\r\n12\r\n",
    )
    .await;

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEL", b"seta{t}"]),
        b":0\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SADD", b"seta{t}", b"a"]),
        b":1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SADD", b"seta{t}", b"b"]),
        b":1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SADD", b"seta{t}", b"c"]),
        b":1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[
            b"ZUNIONSTORE",
            b"zsetc{t}",
            b"2",
            b"seta{t}",
            b"zsetb{t}",
            b"WEIGHTS",
            b"2",
            b"3",
        ]),
        b":4\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"ZRANGE", b"zsetc{t}", b"0", b"-1", b"WITHSCORES"]),
        b"*8\r\n$1\r\na\r\n$1\r\n2\r\n$1\r\nb\r\n$1\r\n5\r\n$1\r\nc\r\n$1\r\n8\r\n$1\r\nd\r\n$1\r\n9\r\n",
    )
    .await;

    send_and_expect(
        &mut client,
        &encode_resp_command(&[
            b"ZINTERSTORE",
            b"zsetc{t}",
            b"2",
            b"zseta{t}",
            b"zsetb{t}",
            b"WEIGHTS",
            b"2",
            b"3",
        ]),
        b":2\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"ZRANGE", b"zsetc{t}", b"0", b"-1", b"WITHSCORES"]),
        b"*4\r\n$1\r\nb\r\n$1\r\n7\r\n$1\r\nc\r\n$2\r\n12\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[
            b"ZINTERSTORE",
            b"zsetc{t}",
            b"2",
            b"seta{t}",
            b"zsetb{t}",
            b"WEIGHTS",
            b"2",
            b"3",
        ]),
        b":2\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"ZRANGE", b"zsetc{t}", b"0", b"-1", b"WITHSCORES"]),
        b"*4\r\n$1\r\nb\r\n$1\r\n5\r\n$1\r\nc\r\n$1\r\n8\r\n",
    )
    .await;

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"ZDIFFSTORE", b"zsetc{t}", b"2", b"zseta{t}", b"zsetb{t}"]),
        b":1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"ZRANGE", b"zsetc{t}", b"0", b"-1", b"WITHSCORES"]),
        b"*2\r\n$1\r\na\r\n$1\r\n1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"ZDIFFSTORE", b"zsetc{t}", b"2", b"seta{t}", b"zsetb{t}"]),
        b":1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"ZRANGE", b"zsetc{t}", b"0", b"-1", b"WITHSCORES"]),
        b"*2\r\n$1\r\na\r\n$1\r\n1\r\n",
    )
    .await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn blocking_xread_key_deleted_external_scenario_runs_as_tcp_integration_test() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let mut controller = TcpStream::connect(addr).await.unwrap();
    let mut inspector = TcpStream::connect(addr).await.unwrap();
    let mut waiter = TcpStream::connect(addr).await.unwrap();

    // Redis tests/unit/type/stream-cgroups.tcl:
    // "Blocking XREAD: key deleted"
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"DEL", b"mystream"]),
        b":0\r\n",
    )
    .await;
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"XADD", b"mystream", b"666", b"f", b"v"]),
        b"$5\r\n666-0\r\n",
    )
    .await;

    waiter
        .write_all(&encode_resp_command(&[
            b"XREAD",
            b"BLOCK",
            b"0",
            b"STREAMS",
            b"mystream",
            b"$",
        ]))
        .await
        .unwrap();
    wait_for_blocked_clients(&mut inspector, 1, Duration::from_secs(1)).await;

    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"DEL", b"mystream"]),
        b":1\r\n",
    )
    .await;
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"XADD", b"mystream", b"667", b"f", b"v"]),
        b"$5\r\n667-0\r\n",
    )
    .await;

    let response = read_exact_with_timeout(
        &mut waiter,
        b"*1\r\n*2\r\n$8\r\nmystream\r\n*1\r\n*2\r\n$5\r\n667-0\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n"
            .len(),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(
        response,
        b"*1\r\n*2\r\n$8\r\nmystream\r\n*1\r\n*2\r\n$5\r\n667-0\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n"
    );
    wait_for_blocked_clients(&mut inspector, 0, Duration::from_secs(1)).await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn xgroup_create_surface_external_scenarios_run_as_tcp_integration_test() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let mut client = TcpStream::connect(addr).await.unwrap();

    // Redis tests/unit/type/stream-cgroups.tcl:
    // - "XGROUP CREATE: creation and duplicate group name detection"
    // - "XGROUP CREATE: with ENTRIESREAD parameter"
    // - "XGROUP CREATE: automatic stream creation fails without MKSTREAM"
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEL", b"mystream"]),
        b":0\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"XADD", b"mystream", b"1-1", b"a", b"1"]),
        b"$3\r\n1-1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"XADD", b"mystream", b"1-2", b"b", b"2"]),
        b"$3\r\n1-2\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"XADD", b"mystream", b"1-3", b"c", b"3"]),
        b"$3\r\n1-3\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"XADD", b"mystream", b"1-4", b"d", b"4"]),
        b"$3\r\n1-4\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"XGROUP", b"CREATE", b"mystream", b"mygroup", b"$"]),
        b"+OK\r\n",
    )
    .await;

    let duplicate = send_and_read_error_line(
        &mut client,
        &encode_resp_command(&[b"XGROUP", b"CREATE", b"mystream", b"mygroup", b"$"]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(duplicate, "BUSYGROUP Consumer Group name already exists");

    let invalid_entries_read = send_and_read_error_line(
        &mut client,
        &encode_resp_command(&[
            b"XGROUP",
            b"CREATE",
            b"mystream",
            b"badgroup",
            b"$",
            b"ENTRIESREAD",
            b"-3",
        ]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(
        invalid_entries_read,
        "ERR value for ENTRIESREAD must be positive or -1"
    );

    let missing_mkstream = send_and_read_error_line(
        &mut client,
        &encode_resp_command(&[b"XGROUP", b"CREATE", b"missingstream", b"mygroup", b"$"]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(
        missing_mkstream,
        "ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically."
    );

    send_and_expect(
        &mut client,
        &encode_resp_command(&[
            b"XGROUP",
            b"CREATE",
            b"mkstream",
            b"mygroup",
            b"$",
            b"MKSTREAM",
        ]),
        b"+OK\r\n",
    )
    .await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn xread_and_xreadgroup_wrong_parameter_external_scenario_runs_as_tcp_integration_test() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let mut client = TcpStream::connect(addr).await.unwrap();

    // Redis tests/unit/type/stream-cgroups.tcl:
    // "XREAD and XREADGROUP against wrong parameter"
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"XADD", b"mystream", b"666", b"f", b"v"]),
        b"$5\r\n666-0\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"XGROUP", b"CREATE", b"mystream", b"mygroup", b"$"]),
        b"+OK\r\n",
    )
    .await;

    let xreadgroup_error = send_and_read_error_line(
        &mut client,
        &encode_resp_command(&[
            b"XREADGROUP",
            b"GROUP",
            b"mygroup",
            b"Alice",
            b"COUNT",
            b"1",
            b"STREAMS",
            b"mystream",
        ]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(
        xreadgroup_error,
        "ERR Unbalanced 'xreadgroup' list of streams: for each stream key an ID or '>' must be specified."
    );

    let xread_error = send_and_read_error_line(
        &mut client,
        &encode_resp_command(&[b"XREAD", b"COUNT", b"1", b"STREAMS", b"mystream"]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(
        xread_error,
        "ERR Unbalanced 'xread' list of streams: for each stream key an ID, '+', or '$' must be specified."
    );

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn blocking_xreadgroup_stream_ran_dry_external_scenario_runs_as_tcp_integration_test() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let mut controller = TcpStream::connect(addr).await.unwrap();
    let mut inspector = TcpStream::connect(addr).await.unwrap();
    let mut waiter = TcpStream::connect(addr).await.unwrap();

    // Redis tests/unit/type/stream-cgroups.tcl:
    // "Blocking XREADGROUP for stream that ran dry (issue #5299)"
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"DEL", b"mystream"]),
        b":0\r\n",
    )
    .await;
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[
            b"XGROUP",
            b"CREATE",
            b"mystream",
            b"mygroup",
            b"$",
            b"MKSTREAM",
        ]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"XADD", b"mystream", b"666", b"key", b"value"]),
        b"$5\r\n666-0\r\n",
    )
    .await;
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"XDEL", b"mystream", b"666"]),
        b":1\r\n",
    )
    .await;

    send_and_expect(
        &mut waiter,
        &encode_resp_command(&[
            b"XREADGROUP",
            b"GROUP",
            b"mygroup",
            b"myconsumer",
            b"BLOCK",
            b"10",
            b"STREAMS",
            b"mystream",
            b">",
        ]),
        b"*-1\r\n",
    )
    .await;

    let smaller_id_error = send_and_read_error_line(
        &mut controller,
        &encode_resp_command(&[b"XADD", b"mystream", b"665", b"key", b"value"]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(
        smaller_id_error,
        "ERR The ID specified in XADD is equal or smaller than the target stream top item"
    );

    let equal_id_error = send_and_read_error_line(
        &mut controller,
        &encode_resp_command(&[b"XADD", b"mystream", b"666", b"key", b"value"]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(
        equal_id_error,
        "ERR The ID specified in XADD is equal or smaller than the target stream top item"
    );

    waiter
        .write_all(&encode_resp_command(&[
            b"XREADGROUP",
            b"GROUP",
            b"mygroup",
            b"myconsumer",
            b"BLOCK",
            b"0",
            b"STREAMS",
            b"mystream",
            b">",
        ]))
        .await
        .unwrap();
    wait_for_blocked_clients(&mut inspector, 1, Duration::from_secs(1)).await;

    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"XADD", b"mystream", b"667", b"key", b"value"]),
        b"$5\r\n667-0\r\n",
    )
    .await;
    let response = read_exact_with_timeout(
        &mut waiter,
        b"*1\r\n*2\r\n$8\r\nmystream\r\n*1\r\n*2\r\n$5\r\n667-0\r\n*2\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
            .len(),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(
        response,
        b"*1\r\n*2\r\n$8\r\nmystream\r\n*1\r\n*2\r\n$5\r\n667-0\r\n*2\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
    );
    wait_for_blocked_clients(&mut inspector, 0, Duration::from_secs(1)).await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn xreadgroup_claim_with_two_blocked_clients_external_scenario_runs_as_tcp_integration_test()
{
    let (addr, shutdown_tx, server) = start_test_server().await;
    let mut controller = TcpStream::connect(addr).await.unwrap();
    let mut inspector = TcpStream::connect(addr).await.unwrap();
    let mut waiter1 = TcpStream::connect(addr).await.unwrap();
    let mut waiter2 = TcpStream::connect(addr).await.unwrap();

    // Redis tests/unit/type/stream-cgroups.tcl:
    // "XREADGROUP CLAIM with two blocked clients"
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"DEL", b"mystream"]),
        b":0\r\n",
    )
    .await;
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"XADD", b"mystream", b"1-0", b"f", b"v1"]),
        b"$3\r\n1-0\r\n",
    )
    .await;
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"XDEL", b"mystream", b"1-0"]),
        b":1\r\n",
    )
    .await;
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[
            b"XGROUP",
            b"CREATE",
            b"mystream",
            b"group1",
            b"0",
            b"MKSTREAM",
        ]),
        b"+OK\r\n",
    )
    .await;

    waiter1
        .write_all(&encode_resp_command(&[
            b"XREADGROUP",
            b"GROUP",
            b"group1",
            b"consumer1",
            b"BLOCK",
            b"0",
            b"CLAIM",
            b"100",
            b"STREAMS",
            b"mystream",
            b">",
        ]))
        .await
        .unwrap();
    waiter2
        .write_all(&encode_resp_command(&[
            b"XREADGROUP",
            b"GROUP",
            b"group1",
            b"consumer2",
            b"BLOCK",
            b"0",
            b"CLAIM",
            b"100",
            b"STREAMS",
            b"mystream",
            b">",
        ]))
        .await
        .unwrap();

    wait_for_blocked_clients(&mut inspector, 2, Duration::from_secs(1)).await;

    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"XADD", b"mystream", b"2-0", b"f", b"v2"]),
        b"$3\r\n2-0\r\n",
    )
    .await;

    let reply1 = read_resp_value_with_timeout(&mut waiter1, Duration::from_secs(2)).await;
    let reply2 = read_resp_value_with_timeout(&mut waiter2, Duration::from_secs(2)).await;
    assert_eq!(resp_socket_array(&reply1).len(), 1);
    assert_eq!(resp_socket_array(&reply2).len(), 1);
    assert!(resp_socket_contains_bulk(&reply1, b"mystream"));
    assert!(resp_socket_contains_bulk(&reply2, b"mystream"));

    wait_for_blocked_clients(&mut inspector, 0, Duration::from_secs(1)).await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn bzmpop_multiple_blocked_clients_external_scenario_runs_as_tcp_integration_test() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let mut controller = TcpStream::connect(addr).await.unwrap();
    let mut inspector = TcpStream::connect(addr).await.unwrap();
    let mut waiter1 = TcpStream::connect(addr).await.unwrap();
    let mut waiter2 = TcpStream::connect(addr).await.unwrap();
    let mut waiter3 = TcpStream::connect(addr).await.unwrap();
    let mut waiter4 = TcpStream::connect(addr).await.unwrap();

    // Redis tests/unit/type/zset.tcl:
    // "BZMPOP with multiple blocked clients"
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"DEL", b"myzset{t}", b"myzset2{t}"]),
        b":0\r\n",
    )
    .await;

    waiter1
        .write_all(&encode_resp_command(&[
            b"BZMPOP",
            b"0",
            b"2",
            b"myzset{t}",
            b"myzset2{t}",
            b"MIN",
            b"COUNT",
            b"1",
        ]))
        .await
        .unwrap();
    wait_for_blocked_clients(&mut inspector, 1, Duration::from_secs(1)).await;

    waiter2
        .write_all(&encode_resp_command(&[
            b"BZMPOP",
            b"0",
            b"2",
            b"myzset{t}",
            b"myzset2{t}",
            b"MAX",
            b"COUNT",
            b"10",
        ]))
        .await
        .unwrap();
    wait_for_blocked_clients(&mut inspector, 2, Duration::from_secs(1)).await;

    waiter3
        .write_all(&encode_resp_command(&[
            b"BZMPOP",
            b"0",
            b"2",
            b"myzset{t}",
            b"myzset2{t}",
            b"MIN",
            b"COUNT",
            b"10",
        ]))
        .await
        .unwrap();
    wait_for_blocked_clients(&mut inspector, 3, Duration::from_secs(1)).await;

    waiter4
        .write_all(&encode_resp_command(&[
            b"BZMPOP",
            b"0",
            b"2",
            b"myzset{t}",
            b"myzset2{t}",
            b"MAX",
            b"COUNT",
            b"1",
        ]))
        .await
        .unwrap();
    wait_for_blocked_clients(&mut inspector, 4, Duration::from_secs(1)).await;

    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"MULTI"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[
            b"ZADD",
            b"myzset{t}",
            b"1",
            b"a",
            b"2",
            b"b",
            b"3",
            b"c",
            b"4",
            b"d",
            b"5",
            b"e",
        ]),
        b"+QUEUED\r\n",
    )
    .await;
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[
            b"ZADD",
            b"myzset2{t}",
            b"1",
            b"a",
            b"2",
            b"b",
            b"3",
            b"c",
            b"4",
            b"d",
            b"5",
            b"e",
        ]),
        b"+QUEUED\r\n",
    )
    .await;
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"EXEC"]),
        b"*2\r\n:5\r\n:5\r\n",
    )
    .await;

    assert_eq!(
        read_zmpop_like_response(&mut waiter1, Duration::from_secs(1)).await,
        (b"myzset{t}".to_vec(), vec![(b"a".to_vec(), b"1".to_vec())])
    );
    assert_eq!(
        read_zmpop_like_response(&mut waiter2, Duration::from_secs(1)).await,
        (
            b"myzset{t}".to_vec(),
            vec![
                (b"e".to_vec(), b"5".to_vec()),
                (b"d".to_vec(), b"4".to_vec()),
                (b"c".to_vec(), b"3".to_vec()),
                (b"b".to_vec(), b"2".to_vec())
            ]
        )
    );
    assert_eq!(
        read_zmpop_like_response(&mut waiter3, Duration::from_secs(1)).await,
        (
            b"myzset2{t}".to_vec(),
            vec![
                (b"a".to_vec(), b"1".to_vec()),
                (b"b".to_vec(), b"2".to_vec()),
                (b"c".to_vec(), b"3".to_vec()),
                (b"d".to_vec(), b"4".to_vec()),
                (b"e".to_vec(), b"5".to_vec())
            ]
        )
    );

    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"ZADD", b"myzset2{t}", b"1", b"a", b"2", b"b", b"3", b"c"]),
        b":3\r\n",
    )
    .await;
    assert_eq!(
        read_zmpop_like_response(&mut waiter4, Duration::from_secs(1)).await,
        (b"myzset2{t}".to_vec(), vec![(b"c".to_vec(), b"3".to_vec())])
    );
    wait_for_blocked_clients(&mut inspector, 0, Duration::from_secs(1)).await;

    let _ = send_and_read_integer(
        &mut controller,
        &encode_resp_command(&[b"DEL", b"myzset{t}", b"myzset2{t}"]),
        Duration::from_secs(1),
    )
    .await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn blocking_zset_pop_timeout_returns_resp3_null_like_external_readraw_scenarios() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let mut client = TcpStream::connect(addr).await.unwrap();

    // Redis tests/unit/type/zset.tcl:
    // - "BZPOPMIN/BZPOPMAX readraw in RESP3"
    // - "BZMPOP readraw in RESP3"
    send_hello_and_drain(&mut client, b"3").await;

    client
        .write_all(&encode_resp_command(&[b"BZPOPMIN", b"missing-bz", b"0.01"]))
        .await
        .unwrap();
    assert_eq!(
        read_resp_line_with_timeout(&mut client, Duration::from_secs(1)).await,
        b"_"
    );

    client
        .write_all(&encode_resp_command(&[
            b"BZMPOP",
            b"0.01",
            b"1",
            b"missing-bzm",
            b"MIN",
        ]))
        .await
        .unwrap();
    assert_eq!(
        read_resp_line_with_timeout(&mut client, Duration::from_secs(1)).await,
        b"_"
    );

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn bzmpop_illegal_arguments_match_redis_external_scenario() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let mut client = TcpStream::connect(addr).await.unwrap();

    // Redis tests/unit/type/zset.tcl:
    // "BZMPOP with illegal argument"
    assert_eq!(
        send_and_read_error_line(
            &mut client,
            &encode_resp_command(&[b"BZMPOP"]),
            Duration::from_secs(1),
        )
        .await,
        "ERR wrong number of arguments for 'bzmpop' command"
    );
    assert_eq!(
        send_and_read_error_line(
            &mut client,
            &encode_resp_command(&[b"BZMPOP", b"0", b"1"]),
            Duration::from_secs(1),
        )
        .await,
        "ERR wrong number of arguments for 'bzmpop' command"
    );
    assert_eq!(
        send_and_read_error_line(
            &mut client,
            &encode_resp_command(&[b"BZMPOP", b"0", b"1", b"myzset{t}"]),
            Duration::from_secs(1),
        )
        .await,
        "ERR wrong number of arguments for 'bzmpop' command"
    );

    for request in [
        encode_resp_command(&[b"BZMPOP", b"1", b"0", b"myzset{t}", b"MIN"]),
        encode_resp_command(&[b"BZMPOP", b"1", b"a", b"myzset{t}", b"MIN"]),
        encode_resp_command(&[b"BZMPOP", b"1", b"-1", b"myzset{t}", b"MAX"]),
    ] {
        let error = send_and_read_error_line(&mut client, &request, Duration::from_secs(1)).await;
        assert!(
            error.starts_with("ERR numkeys"),
            "expected numkeys error, got: {error}"
        );
    }

    for request in [
        encode_resp_command(&[b"BZMPOP", b"1", b"1", b"myzset{t}", b"bad_where"]),
        encode_resp_command(&[b"BZMPOP", b"1", b"1", b"myzset{t}", b"MIN", b"bar_arg"]),
        encode_resp_command(&[b"BZMPOP", b"1", b"1", b"myzset{t}", b"MAX", b"MIN"]),
        encode_resp_command(&[b"BZMPOP", b"1", b"1", b"myzset{t}", b"COUNT"]),
        encode_resp_command(&[
            b"BZMPOP",
            b"1",
            b"1",
            b"myzset{t}",
            b"MIN",
            b"COUNT",
            b"1",
            b"COUNT",
            b"2",
        ]),
        encode_resp_command(&[
            b"BZMPOP",
            b"1",
            b"2",
            b"myzset{t}",
            b"myzset2{t}",
            b"bad_arg",
        ]),
    ] {
        let error = send_and_read_error_line(&mut client, &request, Duration::from_secs(1)).await;
        assert!(
            error.starts_with("ERR syntax error"),
            "expected syntax error, got: {error}"
        );
    }

    for request in [
        encode_resp_command(&[b"BZMPOP", b"1", b"1", b"myzset{t}", b"MIN", b"COUNT", b"0"]),
        encode_resp_command(&[b"BZMPOP", b"1", b"1", b"myzset{t}", b"MAX", b"COUNT", b"a"]),
        encode_resp_command(&[b"BZMPOP", b"1", b"1", b"myzset{t}", b"MIN", b"COUNT", b"-1"]),
        encode_resp_command(&[
            b"BZMPOP",
            b"1",
            b"2",
            b"myzset{t}",
            b"myzset2{t}",
            b"MAX",
            b"COUNT",
            b"-1",
        ]),
    ] {
        let error = send_and_read_error_line(&mut client, &request, Duration::from_secs(1)).await;
        assert!(
            error.starts_with("ERR count"),
            "expected count error, got: {error}"
        );
    }

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn replicaof_enables_replication_and_no_one_promotes_back_to_master() {
    let master_processor =
        Arc::new(RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap());
    let master_server = TestServerHandle::start_with_processor(master_processor).await;
    let replica_server =
        TestServerHandle::start_with_processor(Arc::new(RequestProcessor::new().unwrap())).await;

    let mut master_client = master_server.connect().await;
    let mut replica_client = replica_server.connect().await;

    let master_port = master_server.addr.port().to_string();
    let slaveof = encode_resp_command(&[b"SLAVEOF", b"127.0.0.1", master_port.as_bytes()]);
    send_and_expect(&mut replica_client, &slaveof, b"+OK\r\n").await;

    let mut replicated = false;
    for attempt in 0..50 {
        let value = format!("v{attempt}");
        let set_frame = encode_resp_command(&[b"SET", b"repl:test", value.as_bytes()]);
        send_and_expect(&mut master_client, &set_frame, b"+OK\r\n").await;

        let get_frame = encode_resp_command(&[b"GET", b"repl:test"]);
        replica_client.write_all(&get_frame).await.unwrap();
        let mut response = [0u8; 64];
        let bytes_read = tokio::time::timeout(
            Duration::from_millis(200),
            replica_client.read(&mut response),
        )
        .await
        .unwrap()
        .unwrap();
        let expected = format!("${}\r\n{}\r\n", value.len(), value);
        if &response[..bytes_read] == expected.as_bytes() {
            replicated = true;
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
    assert!(replicated, "replica did not receive replicated value");

    send_and_expect(
        &mut replica_client,
        b"*3\r\n$3\r\nSET\r\n$13\r\nrepl:readonly\r\n$1\r\nx\r\n",
        b"-READONLY You can't write against a read only replica.\r\n",
    )
    .await;

    send_and_expect(
        &mut replica_client,
        b"*3\r\n$9\r\nREPLICAOF\r\n$2\r\nNO\r\n$3\r\nONE\r\n",
        b"+OK\r\n",
    )
    .await;

    send_and_expect(
        &mut replica_client,
        b"*3\r\n$3\r\nSET\r\n$13\r\nrepl:readonly\r\n$1\r\ny\r\n",
        b"+OK\r\n",
    )
    .await;

    master_server.shutdown().await;
    replica_server.shutdown().await;
}

#[tokio::test]
async fn wait_returns_ack_count_after_replica_applies_write() {
    let master_processor =
        Arc::new(RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap());
    let master_server = TestServerHandle::start_with_processor(master_processor).await;
    let replica_server =
        TestServerHandle::start_with_processor(Arc::new(RequestProcessor::new().unwrap())).await;

    let mut master_client = master_server.connect().await;
    let mut replica_client = replica_server.connect().await;

    let master_port = master_server.addr.port().to_string();
    let slaveof = encode_resp_command(&[b"SLAVEOF", b"127.0.0.1", master_port.as_bytes()]);
    send_and_expect(&mut replica_client, &slaveof, b"+OK\r\n").await;

    let mut replicated = false;
    for attempt in 0..50 {
        let value = format!("v{attempt}");
        let set_frame = encode_resp_command(&[b"SET", b"wait:sync", value.as_bytes()]);
        send_and_expect(&mut master_client, &set_frame, b"+OK\r\n").await;

        let get_frame = encode_resp_command(&[b"GET", b"wait:sync"]);
        replica_client.write_all(&get_frame).await.unwrap();
        let mut response = [0u8; 64];
        let bytes_read = tokio::time::timeout(
            Duration::from_millis(200),
            replica_client.read(&mut response),
        )
        .await
        .unwrap()
        .unwrap();
        let expected = format!("${}\r\n{}\r\n", value.len(), value);
        if &response[..bytes_read] == expected.as_bytes() {
            replicated = true;
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
    assert!(replicated, "replica did not reach steady replication state");

    send_and_expect(
        &mut master_client,
        &encode_resp_command(&[b"SET", b"wait:key", b"value"]),
        b"+OK\r\n",
    )
    .await;
    let wait_frame = encode_resp_command(&[b"WAIT", b"1", b"2000"]);
    let acknowledged =
        send_and_read_integer(&mut master_client, &wait_frame, Duration::from_secs(3)).await;
    assert!(
        acknowledged >= 1,
        "expected WAIT to observe at least one acknowledged replica, got {acknowledged}"
    );

    master_server.shutdown().await;
    replica_server.shutdown().await;
}

#[tokio::test]
async fn failover_promotes_connected_replica_and_demotes_source() {
    let master_server =
        TestServerHandle::start_with_processor(Arc::new(RequestProcessor::new().unwrap())).await;
    let replica_server =
        TestServerHandle::start_with_processor(Arc::new(RequestProcessor::new().unwrap())).await;

    let mut master_client = master_server.connect().await;
    let mut replica_client = replica_server.connect().await;

    let master_port = master_server.addr.port().to_string();
    send_and_expect(
        &mut replica_client,
        &encode_resp_command(&[b"REPLICAOF", b"127.0.0.1", master_port.as_bytes()]),
        b"+OK\r\n",
    )
    .await;
    let replication_info =
        wait_for_replica_info_line(&mut master_client, 1, Duration::from_secs(5)).await;
    assert!(
        String::from_utf8_lossy(&replication_info)
            .contains(format!("port={}", replica_server.addr.port()).as_str()),
        "INFO REPLICATION did not expose replica listening port: {}",
        String::from_utf8_lossy(&replication_info)
    );

    send_and_expect(
        &mut master_client,
        &encode_resp_command(&[b"SET", b"failover:seed", b"seed"]),
        b"+OK\r\n",
    )
    .await;
    let mut replicated = false;
    for _ in 0..50 {
        let observed = send_and_read_bulk_payload(
            &mut replica_client,
            &encode_resp_command(&[b"GET", b"failover:seed"]),
            Duration::from_millis(250),
        )
        .await;
        if observed == b"seed" {
            replicated = true;
            break;
        }
        sleep(Duration::from_millis(20)).await;
    }
    assert!(replicated, "replica did not reach steady replication state");

    send_and_expect(
        &mut master_client,
        &encode_resp_command(&[b"SET", b"failover:wait", b"ready"]),
        b"+OK\r\n",
    )
    .await;
    let acknowledged = send_and_read_integer(
        &mut master_client,
        &encode_resp_command(&[b"WAIT", b"1", b"2000"]),
        Duration::from_secs(3),
    )
    .await;
    assert!(
        acknowledged >= 1,
        "expected WAIT to observe the downstream replica before FAILOVER, got {acknowledged}"
    );

    let failover_request = encode_resp_command(&[b"FAILOVER", b"TIMEOUT", b"2000"]);
    master_client.write_all(&failover_request).await.unwrap();
    let failover_reply =
        read_resp_line_with_timeout(&mut master_client, Duration::from_secs(1)).await;
    assert_eq!(
        failover_reply,
        b"+OK",
        "unexpected FAILOVER reply: {}",
        String::from_utf8_lossy(&failover_reply)
    );

    let mut promoted_value = None;
    for attempt in 0..120 {
        let value = format!("after-{attempt}");
        let request = encode_resp_command(&[b"SET", b"failover:post", value.as_bytes()]);
        replica_client.write_all(&request).await.unwrap();
        let mut response = [0u8; 128];
        let bytes_read = tokio::time::timeout(
            Duration::from_millis(250),
            replica_client.read(&mut response),
        )
        .await
        .unwrap()
        .unwrap();
        if &response[..bytes_read] == b"+OK\r\n" {
            promoted_value = Some(value);
            break;
        }
        assert_eq!(
            &response[..bytes_read],
            b"-READONLY You can't write against a read only replica.\r\n",
            "unexpected FAILOVER target write response before promotion"
        );
        sleep(Duration::from_millis(25)).await;
    }
    let promoted_value =
        promoted_value.expect("FAILOVER target was not promoted to writable master");

    let mut source_following = false;
    for _ in 0..120 {
        let observed = send_and_read_bulk_payload(
            &mut master_client,
            &encode_resp_command(&[b"GET", b"failover:post"]),
            Duration::from_millis(250),
        )
        .await;
        if observed == promoted_value.as_bytes() {
            source_following = true;
            break;
        }
        sleep(Duration::from_millis(25)).await;
    }
    assert!(
        source_following,
        "source did not follow promoted target after FAILOVER"
    );

    send_and_expect(
        &mut master_client,
        &encode_resp_command(&[b"SET", b"failover:readonly", b"x"]),
        b"-READONLY You can't write against a read only replica.\r\n",
    )
    .await;

    master_server.shutdown().await;
    replica_server.shutdown().await;
}

#[tokio::test]
async fn failover_rejects_unknown_target_even_with_connected_replica() {
    let master_server =
        TestServerHandle::start_with_processor(Arc::new(RequestProcessor::new().unwrap())).await;
    let replica_server =
        TestServerHandle::start_with_processor(Arc::new(RequestProcessor::new().unwrap())).await;

    let mut master_client = master_server.connect().await;
    let mut replica_client = replica_server.connect().await;

    let master_port = master_server.addr.port().to_string();
    send_and_expect(
        &mut replica_client,
        &encode_resp_command(&[b"REPLICAOF", b"127.0.0.1", master_port.as_bytes()]),
        b"+OK\r\n",
    )
    .await;
    let _ = wait_for_replica_info_line(&mut master_client, 1, Duration::from_secs(5)).await;

    send_and_expect(
        &mut master_client,
        &encode_resp_command(&[b"SET", b"failover:target-check", b"ready"]),
        b"+OK\r\n",
    )
    .await;
    let acknowledged = send_and_read_integer(
        &mut master_client,
        &encode_resp_command(&[b"WAIT", b"1", b"2000"]),
        Duration::from_secs(3),
    )
    .await;
    assert!(
        acknowledged >= 1,
        "expected WAIT to observe the downstream replica before target validation, got {acknowledged}"
    );

    let unknown_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let unknown_port = unknown_listener.local_addr().unwrap().port().to_string();
    drop(unknown_listener);

    send_and_expect(
        &mut master_client,
        &encode_resp_command(&[
            b"FAILOVER",
            b"TO",
            b"127.0.0.1",
            unknown_port.as_bytes(),
            b"TIMEOUT",
            b"100",
        ]),
        b"-ERR FAILOVER target HOST and PORT is not a replica.\r\n",
    )
    .await;

    master_server.shutdown().await;
    replica_server.shutdown().await;
}

#[tokio::test]
async fn cluster_failover_command_updates_source_and_target_redirections() {
    let master_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let master_addr = master_listener.local_addr().unwrap();
    let replica_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let replica_addr = replica_listener.local_addr().unwrap();

    let key = b"cluster-failover-key".to_vec();
    let slot = redis_hash_slot(&key);

    let mut master_config = ClusterConfig::new_local("node-1", "127.0.0.1", master_addr.port());
    let (next, replica_id_in_master) = master_config
        .add_worker(Worker::new(
            "node-3",
            "127.0.0.1",
            replica_addr.port(),
            WorkerRole::Replica,
        ))
        .unwrap();
    master_config = next
        .set_worker_replica_of(replica_id_in_master, "node-1")
        .unwrap()
        .set_slot_state(slot, LOCAL_WORKER_ID, SlotState::Stable)
        .unwrap();

    let mut replica_config = ClusterConfig::new_local("node-3", "127.0.0.1", replica_addr.port())
        .set_local_worker_role(WorkerRole::Replica)
        .unwrap()
        .set_worker_replica_of(LOCAL_WORKER_ID, "node-1")
        .unwrap();
    let (next, master_id_in_replica) = replica_config
        .add_worker(Worker::new(
            "node-1",
            "127.0.0.1",
            master_addr.port(),
            WorkerRole::Primary,
        ))
        .unwrap();
    replica_config = next
        .set_slot_state(slot, master_id_in_replica, SlotState::Stable)
        .unwrap();

    let master_store = Arc::new(ClusterConfigStore::new(master_config));
    let replica_store = Arc::new(ClusterConfigStore::new(replica_config));

    let master_metrics = Arc::new(ServerMetrics::default());
    let replica_metrics = Arc::new(ServerMetrics::default());
    let (master_shutdown_tx, master_shutdown_rx) = oneshot::channel::<()>();
    let (replica_shutdown_tx, replica_shutdown_rx) = oneshot::channel::<()>();

    let master_metrics_task = Arc::clone(&master_metrics);
    let master_cluster = Arc::clone(&master_store);
    let master_server = tokio::spawn(async move {
        run_listener_with_shutdown_and_cluster(
            master_listener,
            1024,
            master_metrics_task,
            async move {
                let _ = master_shutdown_rx.await;
            },
            Some(master_cluster),
        )
        .await
        .unwrap();
    });

    let replica_metrics_task = Arc::clone(&replica_metrics);
    let replica_cluster = Arc::clone(&replica_store);
    let replica_server = tokio::spawn(async move {
        run_listener_with_shutdown_and_cluster(
            replica_listener,
            1024,
            replica_metrics_task,
            async move {
                let _ = replica_shutdown_rx.await;
            },
            Some(replica_cluster),
        )
        .await
        .unwrap();
    });

    let mut master_client = TcpStream::connect(master_addr).await.unwrap();
    let mut replica_client = TcpStream::connect(replica_addr).await.unwrap();

    let master_port = master_addr.port().to_string();
    send_and_expect(
        &mut replica_client,
        &encode_resp_command(&[b"REPLICAOF", b"127.0.0.1", master_port.as_bytes()]),
        b"+OK\r\n",
    )
    .await;
    let replication_info =
        wait_for_replica_info_line(&mut master_client, 1, Duration::from_secs(5)).await;
    assert!(
        String::from_utf8_lossy(&replication_info)
            .contains(format!("port={}", replica_addr.port()).as_str()),
        "INFO REPLICATION did not expose replica listening port: {}",
        String::from_utf8_lossy(&replication_info)
    );

    send_and_expect(
        &mut replica_client,
        &encode_resp_command(&[b"READONLY"]),
        b"+OK\r\n",
    )
    .await;

    send_and_expect(
        &mut master_client,
        &encode_resp_command(&[b"SET", &key, b"seed"]),
        b"+OK\r\n",
    )
    .await;
    let mut replicated = false;
    for _ in 0..50 {
        let observed = send_and_read_bulk_payload(
            &mut replica_client,
            &encode_resp_command(&[b"GET", &key]),
            Duration::from_millis(250),
        )
        .await;
        if observed == b"seed" {
            replicated = true;
            break;
        }
        sleep(Duration::from_millis(20)).await;
    }
    assert!(
        replicated,
        "replica did not expose the slot value through READONLY before cluster FAILOVER"
    );

    let acknowledged = send_and_read_integer(
        &mut master_client,
        &encode_resp_command(&[b"WAIT", b"1", b"2000"]),
        Duration::from_secs(3),
    )
    .await;
    assert!(
        acknowledged >= 1,
        "expected WAIT to observe the downstream replica before cluster FAILOVER, got {acknowledged}"
    );

    let replica_port = replica_addr.port().to_string();
    send_and_expect(
        &mut master_client,
        &encode_resp_command(&[
            b"FAILOVER",
            b"TO",
            b"127.0.0.1",
            replica_port.as_bytes(),
            b"TIMEOUT",
            b"2000",
        ]),
        b"+OK\r\n",
    )
    .await;

    wait_until(
        || {
            let master_snapshot = master_store.load();
            let replica_snapshot = replica_store.load();
            master_snapshot.local_worker().unwrap().role == WorkerRole::Replica
                && master_snapshot.slot_assigned_owner(slot).unwrap() == replica_id_in_master
                && master_snapshot.worker(replica_id_in_master).unwrap().role == WorkerRole::Primary
                && master_snapshot
                    .local_worker()
                    .unwrap()
                    .replica_of_node_id
                    .as_deref()
                    == Some("node-3")
                && replica_snapshot.local_worker().unwrap().role == WorkerRole::Primary
                && replica_snapshot.slot_assigned_owner(slot).unwrap() == LOCAL_WORKER_ID
                && replica_snapshot.worker(master_id_in_replica).unwrap().role
                    == WorkerRole::Replica
                && replica_snapshot
                    .worker(master_id_in_replica)
                    .unwrap()
                    .replica_of_node_id
                    .as_deref()
                    == Some("node-3")
        },
        Duration::from_secs(5),
    )
    .await;

    let moved_to_replica = format!("-MOVED {} 127.0.0.1:{}\r\n", slot, replica_addr.port());
    send_and_expect(
        &mut master_client,
        &encode_resp_command(&[b"GET", &key]),
        moved_to_replica.as_bytes(),
    )
    .await;

    let moved_to_master = format!("-MOVED {} 127.0.0.1:{}\r\n", slot, master_addr.port());
    let mut promoted_value = None;
    for attempt in 0..120 {
        let value = format!("cluster-after-{attempt}");
        let request = encode_resp_command(&[b"SET", &key, value.as_bytes()]);
        replica_client.write_all(&request).await.unwrap();
        let mut response = [0u8; 128];
        let bytes_read = tokio::time::timeout(
            Duration::from_millis(250),
            replica_client.read(&mut response),
        )
        .await
        .unwrap()
        .unwrap();
        if &response[..bytes_read] == b"+OK\r\n" {
            promoted_value = Some(value);
            break;
        }
        assert_eq!(
            &response[..bytes_read],
            moved_to_master.as_bytes(),
            "unexpected cluster FAILOVER target response before promotion"
        );
        sleep(Duration::from_millis(25)).await;
    }
    let promoted_value =
        promoted_value.expect("cluster FAILOVER target was not promoted to writable master");

    let expected_promoted_get = format!("${}\r\n{}\r\n", promoted_value.len(), promoted_value);
    send_and_expect(
        &mut replica_client,
        &encode_resp_command(&[b"GET", &key]),
        expected_promoted_get.as_bytes(),
    )
    .await;

    let _ = master_shutdown_tx.send(());
    let _ = replica_shutdown_tx.send(());
    master_server.await.unwrap();
    replica_server.await.unwrap();
}

#[tokio::test]
async fn cluster_failover_command_converges_observer_redirections_via_gossip_manager() {
    let primary_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let primary_addr = primary_listener.local_addr().unwrap();
    let observer_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let observer_addr = observer_listener.local_addr().unwrap();
    let replica_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let replica_addr = replica_listener.local_addr().unwrap();

    let key = b"cluster-failover-observer-key".to_vec();
    let slot = redis_hash_slot(&key);

    let mut primary_config = ClusterConfig::new_local("node-1", "127.0.0.1", primary_addr.port());
    let (next, observer_id_in_primary) = primary_config
        .add_worker(Worker::new(
            "node-2",
            "127.0.0.1",
            observer_addr.port(),
            WorkerRole::Primary,
        ))
        .unwrap();
    primary_config = next;
    let (next, replica_id_in_primary) = primary_config
        .add_worker(Worker::new(
            "node-3",
            "127.0.0.1",
            replica_addr.port(),
            WorkerRole::Replica,
        ))
        .unwrap();
    primary_config = next
        .set_worker_replica_of(replica_id_in_primary, "node-1")
        .unwrap()
        .set_slot_state(slot, LOCAL_WORKER_ID, SlotState::Stable)
        .unwrap();

    let mut observer_config = ClusterConfig::new_local("node-2", "127.0.0.1", observer_addr.port());
    let (next, primary_id_in_observer) = observer_config
        .add_worker(Worker::new(
            "node-1",
            "127.0.0.1",
            primary_addr.port(),
            WorkerRole::Primary,
        ))
        .unwrap();
    observer_config = next;
    let (next, replica_id_in_observer) = observer_config
        .add_worker(Worker::new(
            "node-3",
            "127.0.0.1",
            replica_addr.port(),
            WorkerRole::Replica,
        ))
        .unwrap();
    observer_config = next
        .set_worker_replica_of(replica_id_in_observer, "node-1")
        .unwrap()
        .set_slot_state(slot, primary_id_in_observer, SlotState::Stable)
        .unwrap();

    let mut replica_config = ClusterConfig::new_local("node-3", "127.0.0.1", replica_addr.port())
        .set_local_worker_role(WorkerRole::Replica)
        .unwrap()
        .set_worker_replica_of(LOCAL_WORKER_ID, "node-1")
        .unwrap();
    let (next, primary_id_in_replica) = replica_config
        .add_worker(Worker::new(
            "node-1",
            "127.0.0.1",
            primary_addr.port(),
            WorkerRole::Primary,
        ))
        .unwrap();
    replica_config = next;
    let (next, observer_id_in_replica) = replica_config
        .add_worker(Worker::new(
            "node-2",
            "127.0.0.1",
            observer_addr.port(),
            WorkerRole::Primary,
        ))
        .unwrap();
    replica_config = next
        .set_slot_state(slot, primary_id_in_replica, SlotState::Stable)
        .unwrap();

    let primary_store = Arc::new(ClusterConfigStore::new(primary_config));
    let observer_store = Arc::new(ClusterConfigStore::new(observer_config));
    let replica_store = Arc::new(ClusterConfigStore::new(replica_config));

    let (primary_shutdown_tx, primary_shutdown_rx) = oneshot::channel::<()>();
    let (observer_shutdown_tx, observer_shutdown_rx) = oneshot::channel::<()>();
    let (replica_shutdown_tx, replica_shutdown_rx) = oneshot::channel::<()>();

    let primary_server = {
        let metrics = Arc::new(ServerMetrics::default());
        let cluster_store = Arc::clone(&primary_store);
        tokio::spawn(async move {
            run_listener_with_shutdown_and_cluster(
                primary_listener,
                1024,
                metrics,
                async move {
                    let _ = primary_shutdown_rx.await;
                },
                Some(cluster_store),
            )
            .await
            .unwrap();
        })
    };

    let observer_server = {
        let metrics = Arc::new(ServerMetrics::default());
        let cluster_store = Arc::clone(&observer_store);
        tokio::spawn(async move {
            run_listener_with_shutdown_and_cluster(
                observer_listener,
                1024,
                metrics,
                async move {
                    let _ = observer_shutdown_rx.await;
                },
                Some(cluster_store),
            )
            .await
            .unwrap();
        })
    };

    let replica_server = {
        let metrics = Arc::new(ServerMetrics::default());
        let cluster_store = Arc::clone(&replica_store);
        tokio::spawn(async move {
            run_listener_with_shutdown_and_cluster(
                replica_listener,
                1024,
                metrics,
                async move {
                    let _ = replica_shutdown_rx.await;
                },
                Some(cluster_store),
            )
            .await
            .unwrap();
        })
    };

    let mut primary_client = TcpStream::connect(primary_addr).await.unwrap();
    let mut observer_client = TcpStream::connect(observer_addr).await.unwrap();
    let mut replica_client = TcpStream::connect(replica_addr).await.unwrap();

    let primary_port = primary_addr.port().to_string();
    send_and_expect(
        &mut replica_client,
        &encode_resp_command(&[b"REPLICAOF", b"127.0.0.1", primary_port.as_bytes()]),
        b"+OK\r\n",
    )
    .await;
    let _ = wait_for_replica_info_line(&mut primary_client, 1, Duration::from_secs(5)).await;

    send_and_expect(
        &mut replica_client,
        &encode_resp_command(&[b"READONLY"]),
        b"+OK\r\n",
    )
    .await;

    send_and_expect(
        &mut primary_client,
        &encode_resp_command(&[b"SET", &key, b"seed"]),
        b"+OK\r\n",
    )
    .await;
    let acknowledged = send_and_read_integer(
        &mut primary_client,
        &encode_resp_command(&[b"WAIT", b"1", b"2000"]),
        Duration::from_secs(3),
    )
    .await;
    assert!(
        acknowledged >= 1,
        "expected WAIT to observe the downstream replica before cluster FAILOVER, got {acknowledged}"
    );

    let moved_to_primary = format!("-MOVED {} 127.0.0.1:{}\r\n", slot, primary_addr.port());
    send_and_expect(
        &mut observer_client,
        &encode_resp_command(&[b"GET", &key]),
        moved_to_primary.as_bytes(),
    )
    .await;

    let (observer_updates_tx, observer_updates_rx) =
        tokio::sync::mpsc::unbounded_channel::<ClusterConfig>();

    let mut primary_transport = InMemoryGossipTransport::new(Arc::clone(&primary_store));
    primary_transport.add_peer(observer_id_in_primary, observer_updates_tx.clone());
    let primary_gossip_engine = AsyncGossipEngine::new(
        GossipCoordinator::new(vec![GossipNode::new(observer_id_in_primary, 0)], 1),
        primary_transport,
        100,
        0,
    );
    let mut primary_manager = ClusterManager::new(primary_gossip_engine, Duration::from_millis(2));

    let mut replica_transport = InMemoryGossipTransport::new(Arc::clone(&replica_store));
    replica_transport.add_peer(observer_id_in_replica, observer_updates_tx.clone());
    let replica_gossip_engine = AsyncGossipEngine::new(
        GossipCoordinator::new(vec![GossipNode::new(observer_id_in_replica, 0)], 1),
        replica_transport,
        100,
        0,
    );
    let mut replica_manager = ClusterManager::new(replica_gossip_engine, Duration::from_millis(2));

    let observer_manager_store = Arc::clone(&observer_store);
    let (observer_manager_shutdown_tx, observer_manager_shutdown_rx) = oneshot::channel::<()>();
    let observer_manager = tokio::spawn(async move {
        let gossip_engine = AsyncGossipEngine::new(
            GossipCoordinator::new(Vec::new(), 1),
            InMemoryGossipTransport::new(Arc::clone(&observer_manager_store)),
            100,
            0,
        );
        let mut manager = ClusterManager::new(gossip_engine, Duration::from_millis(2));
        let mut failure_detector = FailureDetector::new(1_000);
        let mut failover_controller = ClusterFailoverController::new();
        let mut replication_manager = ReplicationManager::new(
            Some(CheckpointId::new(7)),
            ReplicationOffset::new(2_000),
            ReplicationOffset::new(2_200),
        )
        .unwrap();
        let (repl_tx, _repl_rx) = tokio::sync::mpsc::unbounded_channel::<ReplicationEvent>();
        let mut replication_transport =
            ChannelReplicationTransport::new(repl_tx, ReplicationOffset::new(1_980));

        manager
            .run_with_config_updates_and_failover_report(
                observer_manager_store.as_ref(),
                observer_updates_rx,
                &mut failure_detector,
                &mut failover_controller,
                &mut replication_manager,
                &mut replication_transport,
                async move {
                    let _ = observer_manager_shutdown_rx.await;
                },
            )
            .await
            .unwrap()
    });

    let replica_port = replica_addr.port().to_string();
    send_and_expect(
        &mut primary_client,
        &encode_resp_command(&[
            b"FAILOVER",
            b"TO",
            b"127.0.0.1",
            replica_port.as_bytes(),
            b"TIMEOUT",
            b"2000",
        ]),
        b"+OK\r\n",
    )
    .await;

    wait_until(
        || {
            let primary_snapshot = primary_store.load();
            let replica_snapshot = replica_store.load();
            primary_snapshot.local_worker().unwrap().role == WorkerRole::Replica
                && primary_snapshot.slot_assigned_owner(slot).unwrap() == replica_id_in_primary
                && primary_snapshot.worker(replica_id_in_primary).unwrap().role
                    == WorkerRole::Primary
                && primary_snapshot
                    .local_worker()
                    .unwrap()
                    .replica_of_node_id
                    .as_deref()
                    == Some("node-3")
                && replica_snapshot.local_worker().unwrap().role == WorkerRole::Primary
                && replica_snapshot.slot_assigned_owner(slot).unwrap() == LOCAL_WORKER_ID
                && replica_snapshot.worker(primary_id_in_replica).unwrap().role
                    == WorkerRole::Replica
                && replica_snapshot
                    .worker(primary_id_in_replica)
                    .unwrap()
                    .replica_of_node_id
                    .as_deref()
                    == Some("node-3")
        },
        Duration::from_secs(5),
    )
    .await;

    primary_manager.run_for_rounds(1).await;
    replica_manager.run_for_rounds(1).await;

    wait_until(
        || {
            let observer_snapshot = observer_store.load();
            observer_snapshot.slot_assigned_owner(slot).unwrap() == replica_id_in_observer
                && observer_snapshot
                    .worker(replica_id_in_observer)
                    .unwrap()
                    .role
                    == WorkerRole::Primary
                && observer_snapshot
                    .worker(primary_id_in_observer)
                    .unwrap()
                    .role
                    == WorkerRole::Replica
                && observer_snapshot
                    .worker(primary_id_in_observer)
                    .unwrap()
                    .replica_of_node_id
                    .as_deref()
                    == Some("node-3")
        },
        Duration::from_secs(5),
    )
    .await;

    let moved_to_replica = format!("-MOVED {} 127.0.0.1:{}\r\n", slot, replica_addr.port());
    send_and_expect(
        &mut observer_client,
        &encode_resp_command(&[b"GET", &key]),
        moved_to_replica.as_bytes(),
    )
    .await;

    let expected_get = b"$4\r\nseed\r\n";
    send_and_expect(
        &mut replica_client,
        &encode_resp_command(&[b"GET", &key]),
        expected_get,
    )
    .await;

    let _ = observer_manager_shutdown_tx.send(());
    observer_manager.await.unwrap();

    let _ = primary_shutdown_tx.send(());
    let _ = observer_shutdown_tx.send(());
    let _ = replica_shutdown_tx.send(());
    primary_server.await.unwrap();
    observer_server.await.unwrap();
    replica_server.await.unwrap();
}

#[tokio::test]
async fn publish_write_frame_advances_local_aof_frontiers_when_appendonly_enabled() {
    let temp_dir = unique_test_temp_dir("live-aof-integration");

    let processor =
        Arc::new(RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap());
    apply_test_startup_config_overrides(
        processor.as_ref(),
        aof_test_startup_overrides(&temp_dir, "integration.aof", "always"),
    );
    let runtime = processor
        .ensure_live_aof_durability_runtime()
        .unwrap()
        .unwrap();

    let owner_thread_pool = build_owner_thread_pool(&processor).unwrap();
    let replication = RedisReplicationCoordinator::new(Arc::clone(&processor), owner_thread_pool);
    let frame = encode_resp_command(&[b"SET", b"aof:key", b"value"]);
    let expected_offset = u64::try_from(4 + frame.len()).unwrap();

    let published = replication.publish_write_frame(&frame);
    assert_eq!(published.replication_offset, frame.len() as u64);
    assert_eq!(
        published.local_aof_append_offset,
        Some(tsavorite::AofOffset::new(expected_offset))
    );

    let reached = runtime
        .wait_for_frontiers_at_least(
            crate::aof_durability::LocalAofFrontiersSnapshot {
                append_offset: tsavorite::AofOffset::new(expected_offset),
                fsync_offset: tsavorite::AofOffset::new(expected_offset),
            },
            Duration::from_secs(1),
        )
        .await;
    assert!(reached, "timed out waiting for local AOF frontier");
    assert_eq!(replication.current_master_repl_offset(), frame.len() as u64);
    assert_eq!(
        processor.local_aof_frontiers_snapshot(),
        Some(crate::aof_durability::LocalAofFrontiersSnapshot {
            append_offset: tsavorite::AofOffset::new(expected_offset),
            fsync_offset: tsavorite::AofOffset::new(expected_offset),
        })
    );

    let aof_path = temp_dir.join("integration.aof");
    let mut reader = tsavorite::AofReader::open(&aof_path).unwrap();
    assert_eq!(reader.replay_all_tolerant().unwrap(), vec![frame]);

    let _ = std::fs::remove_file(aof_path);
    let _ = std::fs::remove_dir_all(temp_dir);
}

#[tokio::test]
async fn config_set_appendonly_yes_persists_live_writes_for_debug_loadaof() {
    let processor =
        Arc::new(RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap());
    let temp_dir = configure_processor_aof_for_test(processor.as_ref(), "config-appendonly");
    let (addr, shutdown_tx, server) =
        start_test_server_with_processor(Arc::clone(&processor)).await;
    let mut client = TcpStream::connect(addr).await.unwrap();

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"CONFIG", b"SET", b"appendfsync", b"always"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"CONFIG", b"SET", b"appendonly", b"yes"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"cfg:aof", b"value"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEBUG", b"LOADAOF"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"GET", b"cfg:aof"]),
        b"$5\r\nvalue\r\n",
    )
    .await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
    let _ = std::fs::remove_dir_all(temp_dir);
}

#[tokio::test]
async fn startup_with_appendonly_replays_configured_aof_before_serving_traffic() {
    let temp_dir = unique_test_temp_dir("startup-appendonly-replay");
    let appendfilename = "startup-replay.aof";
    let startup_overrides = aof_test_startup_overrides(&temp_dir, appendfilename, "always");

    let first_processor =
        Arc::new(RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap());
    apply_test_startup_config_overrides(first_processor.as_ref(), startup_overrides.clone());
    let first_server = TestServerHandle::start_with_processor(Arc::clone(&first_processor)).await;
    let mut first_client = first_server.connect().await;
    send_and_expect(
        &mut first_client,
        &encode_resp_command(&[b"SET", b"boot:key:db0", b"value:db0"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut first_client,
        &encode_resp_command(&[b"SELECT", b"1"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut first_client,
        &encode_resp_command(&[b"SET", b"boot:key:db1", b"value:db1"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut first_client,
        &encode_resp_command(&[b"WAITAOF", b"1", b"0", b"1000"]),
        b"*2\r\n:1\r\n:0\r\n",
    )
    .await;

    first_server.shutdown().await;
    first_processor.stop_live_aof_durability_runtime();
    drop(first_processor);

    let second_processor =
        Arc::new(RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap());
    apply_test_startup_config_overrides(second_processor.as_ref(), startup_overrides.clone());
    let second_server = TestServerHandle::start_with_processor(Arc::clone(&second_processor)).await;
    let mut second_client = second_server.connect().await;
    send_and_expect(
        &mut second_client,
        &encode_resp_command(&[b"GET", b"boot:key:db0"]),
        b"$9\r\nvalue:db0\r\n",
    )
    .await;
    send_and_expect(
        &mut second_client,
        &encode_resp_command(&[b"SELECT", b"1"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut second_client,
        &encode_resp_command(&[b"GET", b"boot:key:db1"]),
        b"$9\r\nvalue:db1\r\n",
    )
    .await;
    send_and_expect(
        &mut second_client,
        &encode_resp_command(&[b"SET", b"boot:key:db1:next", b"value:next"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut second_client,
        &encode_resp_command(&[b"WAITAOF", b"1", b"0", b"1000"]),
        b"*2\r\n:1\r\n:0\r\n",
    )
    .await;

    second_server.shutdown().await;
    second_processor.stop_live_aof_durability_runtime();
    drop(second_processor);

    let third_processor =
        Arc::new(RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap());
    apply_test_startup_config_overrides(third_processor.as_ref(), startup_overrides);
    let third_server = TestServerHandle::start_with_processor(Arc::clone(&third_processor)).await;
    let mut third_client = third_server.connect().await;
    send_and_expect(
        &mut third_client,
        &encode_resp_command(&[b"SELECT", b"1"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut third_client,
        &encode_resp_command(&[b"GET", b"boot:key:db1:next"]),
        b"$10\r\nvalue:next\r\n",
    )
    .await;

    third_server.shutdown().await;
    third_processor.stop_live_aof_durability_runtime();
    let _ = std::fs::remove_dir_all(temp_dir);
}

#[tokio::test]
async fn startup_config_overrides_apply_aof_and_aclfile_before_serving_traffic() {
    let temp_dir = unique_test_temp_dir("startup-config-overrides");
    let aclfile_path = temp_dir.join("users.acl");
    std::fs::write(
        &aclfile_path,
        concat!(
            "user alice on allcommands allkeys &* >alice\n",
            "user default on nopass ~* &* +@all\n",
        ),
    )
    .unwrap();

    let bind_addr =
        std::net::SocketAddr::from(([127, 0, 0, 1], TestServerHandle::reserve_local_test_port()));
    let config = ServerConfig {
        bind_addr,
        read_buffer_size: 1024,
        startup_config_overrides: StartupConfigOverrides {
            dir: Some(temp_dir.clone()),
            appendonly: Some(true),
            appendfsync: Some("always".to_string()),
            appendfilename: Some("startup-overrides.aof".to_string()),
            aclfile: Some(std::path::PathBuf::from("users.acl")),
            ..Default::default()
        },
    };

    let first_server = TestServerHandle::spawn_with_config(config.clone());
    let mut first_client = first_server.connect().await;
    send_and_expect(
        &mut first_client,
        &encode_resp_command(&[b"AUTH", b"alice", b"alice"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut first_client,
        &encode_resp_command(&[b"ACL", b"WHOAMI"]),
        b"$5\r\nalice\r\n",
    )
    .await;
    send_and_expect(
        &mut first_client,
        &encode_resp_command(&[b"SET", b"boot:override:key", b"boot:override:value"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut first_client,
        &encode_resp_command(&[b"WAITAOF", b"1", b"0", b"1000"]),
        b"*2\r\n:1\r\n:0\r\n",
    )
    .await;
    assert!(temp_dir.join("startup-overrides.aof").is_file());

    first_server.shutdown().await;

    let second_server = TestServerHandle::spawn_with_config(config);
    let mut second_client = second_server.connect().await;
    send_and_expect(
        &mut second_client,
        &encode_resp_command(&[b"AUTH", b"alice", b"alice"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut second_client,
        &encode_resp_command(&[b"GET", b"boot:override:key"]),
        b"$19\r\nboot:override:value\r\n",
    )
    .await;

    second_server.shutdown().await;
    let _ = std::fs::remove_dir_all(temp_dir);
}

#[tokio::test]
async fn startup_with_dump_snapshot_replays_before_serving_traffic() {
    let temp_dir = unique_test_temp_dir("startup-dump-snapshot");
    let dbfilename = "startup-replay.rdb";
    let startup_overrides = dump_snapshot_test_startup_overrides(&temp_dir, dbfilename);

    let first_processor =
        Arc::new(RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap());
    apply_test_startup_config_overrides(first_processor.as_ref(), startup_overrides.clone());
    let first_server = TestServerHandle::start_with_processor(Arc::clone(&first_processor)).await;
    let mut first_client = first_server.connect().await;
    send_and_expect(
        &mut first_client,
        &encode_resp_command(&[b"SET", b"boot:rdb:db0", b"value:db0"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut first_client,
        &encode_resp_command(&[b"SELECT", b"1"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut first_client,
        &encode_resp_command(&[b"SET", b"boot:rdb:db1", b"value:db1"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut first_client,
        &encode_resp_command(&[b"SAVE"]),
        b"+OK\r\n",
    )
    .await;

    first_server.shutdown().await;
    drop(first_processor);

    let second_processor =
        Arc::new(RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap());
    apply_test_startup_config_overrides(second_processor.as_ref(), startup_overrides);
    let second_server = TestServerHandle::start_with_processor(Arc::clone(&second_processor)).await;
    let mut second_client = second_server.connect().await;
    send_and_expect(
        &mut second_client,
        &encode_resp_command(&[b"GET", b"boot:rdb:db0"]),
        b"$9\r\nvalue:db0\r\n",
    )
    .await;
    send_and_expect(
        &mut second_client,
        &encode_resp_command(&[b"SELECT", b"1"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut second_client,
        &encode_resp_command(&[b"GET", b"boot:rdb:db1"]),
        b"$9\r\nvalue:db1\r\n",
    )
    .await;

    second_server.shutdown().await;
    let _ = std::fs::remove_dir_all(temp_dir);
}

#[tokio::test]
async fn startup_with_appendonly_enabled_and_missing_aof_falls_back_to_dump_snapshot() {
    let temp_dir = unique_test_temp_dir("startup-dump-fallback");
    let dbfilename = "startup-fallback.rdb";
    let appendfilename = "startup-fallback.aof";
    let aof_path = temp_dir.join("startup-fallback.aof");
    let dump_startup_overrides = dump_snapshot_test_startup_overrides(&temp_dir, dbfilename);
    let appendonly_startup_overrides =
        aof_test_startup_overrides(&temp_dir, appendfilename, "always");

    let first_processor =
        Arc::new(RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap());
    apply_test_startup_config_overrides(first_processor.as_ref(), dump_startup_overrides.clone());
    let first_server = TestServerHandle::start_with_processor(Arc::clone(&first_processor)).await;
    let mut first_client = first_server.connect().await;
    send_and_expect(
        &mut first_client,
        &encode_resp_command(&[b"SET", b"boot:fallback:key", b"value"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut first_client,
        &encode_resp_command(&[b"SAVE"]),
        b"+OK\r\n",
    )
    .await;

    first_server.shutdown().await;
    drop(first_processor);

    assert!(
        !aof_path.exists(),
        "appendonly file should not exist before appendonly boot fallback"
    );

    let second_processor =
        Arc::new(RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap());
    let mut second_startup_overrides = dump_startup_overrides;
    second_startup_overrides.appendonly = appendonly_startup_overrides.appendonly;
    second_startup_overrides.appendfsync = appendonly_startup_overrides.appendfsync.clone();
    second_startup_overrides.appendfilename = appendonly_startup_overrides.appendfilename.clone();
    apply_test_startup_config_overrides(second_processor.as_ref(), second_startup_overrides);
    let second_server = TestServerHandle::start_with_processor(Arc::clone(&second_processor)).await;
    let mut second_client = second_server.connect().await;
    send_and_expect(
        &mut second_client,
        &encode_resp_command(&[b"GET", b"boot:fallback:key"]),
        b"$5\r\nvalue\r\n",
    )
    .await;

    second_server.shutdown().await;
    second_processor.stop_live_aof_durability_runtime();
    let _ = std::fs::remove_dir_all(temp_dir);
}

#[tokio::test]
async fn live_bgrewriteaof_compacts_history_and_preserves_post_rewrite_writes_on_restart() {
    let temp_dir = unique_test_temp_dir("live-bgrewriteaof");
    let appendfilename = "live-bgrewriteaof.aof";
    let aof_path = temp_dir.join(appendfilename);
    let startup_overrides = aof_test_startup_overrides(&temp_dir, appendfilename, "always");

    let first_processor =
        Arc::new(RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap());
    apply_test_startup_config_overrides(first_processor.as_ref(), startup_overrides.clone());
    let first_server = TestServerHandle::start_with_processor(Arc::clone(&first_processor)).await;
    let mut first_client = first_server.connect().await;
    send_and_expect(
        &mut first_client,
        &encode_resp_command(&[b"SET", b"keep:key", b"keep-value"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut first_client,
        &encode_resp_command(&[b"SET", b"gone:key", b"gone-value"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut first_client,
        &encode_resp_command(&[b"DEL", b"gone:key"]),
        b":1\r\n",
    )
    .await;
    send_and_expect(
        &mut first_client,
        &encode_resp_command(&[b"SELECT", b"1"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut first_client,
        &encode_resp_command(&[b"SET", b"keep:key:db1", b"db1-value"]),
        b"+OK\r\n",
    )
    .await;
    let bgrewriteaof_reply = send_and_read_resp_value(
        &mut first_client,
        &encode_resp_command(&[b"BGREWRITEAOF"]),
        Duration::from_secs(20),
    )
    .await;
    assert_eq!(
        resp_socket_bulk(&bgrewriteaof_reply),
        b"Background append only file rewriting started"
    );
    send_and_expect(
        &mut first_client,
        &encode_resp_command(&[b"SELECT", b"0"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut first_client,
        &encode_resp_command(&[b"SET", b"post:key", b"post-value"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut first_client,
        &encode_resp_command(&[b"WAITAOF", b"1", b"0", b"1000"]),
        b"*2\r\n:1\r\n:0\r\n",
    )
    .await;

    first_server.shutdown().await;
    first_processor.stop_live_aof_durability_runtime();
    drop(first_processor);

    let mut reader = tsavorite::AofReader::open(&aof_path).unwrap();
    let operations = reader.replay_all_tolerant().unwrap();
    assert!(
        operations
            .iter()
            .any(|frame| frame.starts_with(b"*5\r\n$7\r\nRESTORE")),
        "rewritten live AOF should contain RESTORE snapshot frames"
    );
    assert!(
        operations.iter().any(|frame| frame
            .windows(b"post:key".len())
            .any(|window| window == b"post:key")),
        "rewritten live AOF should keep post-rewrite live writes"
    );
    assert!(
        !operations.iter().any(|frame| frame
            .windows(b"gone:key".len())
            .any(|window| window == b"gone:key")),
        "rewritten live AOF should compact deleted keys out of the rewritten log"
    );

    let second_processor =
        Arc::new(RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap());
    apply_test_startup_config_overrides(second_processor.as_ref(), startup_overrides);
    let second_server = TestServerHandle::start_with_processor(Arc::clone(&second_processor)).await;
    let mut second_client = second_server.connect().await;
    send_and_expect(
        &mut second_client,
        &encode_resp_command(&[b"GET", b"keep:key"]),
        b"$10\r\nkeep-value\r\n",
    )
    .await;
    send_and_expect(
        &mut second_client,
        &encode_resp_command(&[b"GET", b"post:key"]),
        b"$10\r\npost-value\r\n",
    )
    .await;
    send_and_expect(
        &mut second_client,
        &encode_resp_command(&[b"GET", b"gone:key"]),
        b"$-1\r\n",
    )
    .await;
    send_and_expect(
        &mut second_client,
        &encode_resp_command(&[b"SELECT", b"1"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut second_client,
        &encode_resp_command(&[b"GET", b"keep:key:db1"]),
        b"$9\r\ndb1-value\r\n",
    )
    .await;

    second_server.shutdown().await;
    second_processor.stop_live_aof_durability_runtime();
    let _ = std::fs::remove_dir_all(temp_dir);
}

#[tokio::test]
async fn startup_with_appendonly_replays_function_libraries_after_bgrewriteaof() {
    let temp_dir = unique_test_temp_dir("startup-aof-functions");
    let appendfilename = "startup-aof-functions.aof";
    let aof_path = temp_dir.join(appendfilename);
    let function_library = b"#!lua name=aoflib\nredis.register_function{function_name='setv', callback=function(keys, args) redis.call('set', keys[1], args[1]); return args[1] end}";
    let startup_overrides = aof_test_startup_overrides(&temp_dir, appendfilename, "always");

    let first_processor =
        Arc::new(RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap());
    apply_test_startup_config_overrides(first_processor.as_ref(), startup_overrides.clone());
    let first_server = TestServerHandle::start_with_processor(Arc::clone(&first_processor)).await;
    let mut first_client = first_server.connect().await;
    let loaded_library = send_and_read_resp_value(
        &mut first_client,
        &encode_resp_command(&[b"FUNCTION", b"LOAD", function_library]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(resp_socket_bulk(&loaded_library), b"aoflib");
    let initial_value = send_and_read_resp_value(
        &mut first_client,
        &encode_resp_command(&[b"FCALL", b"setv", b"1", b"boot:function:key", b"boot-value"]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(resp_socket_bulk(&initial_value), b"boot-value");
    send_and_expect(
        &mut first_client,
        &encode_resp_command(&[b"BGREWRITEAOF"]),
        b"+Background append only file rewriting started\r\n",
    )
    .await;
    let live_value = send_and_read_resp_value(
        &mut first_client,
        &encode_resp_command(&[
            b"FCALL",
            b"setv",
            b"1",
            b"boot:function:key:live",
            b"live-value",
        ]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(resp_socket_bulk(&live_value), b"live-value");
    send_and_expect(
        &mut first_client,
        &encode_resp_command(&[b"WAITAOF", b"1", b"0", b"1000"]),
        b"*2\r\n:1\r\n:0\r\n",
    )
    .await;

    first_server.shutdown().await;
    first_processor.stop_live_aof_durability_runtime();
    drop(first_processor);

    let mut reader = tsavorite::AofReader::open(&aof_path).unwrap();
    let operations = reader.replay_all_tolerant().unwrap();
    assert!(
        operations.iter().any(|frame| {
            frame.starts_with(b"*3\r\n$8\r\nFUNCTION\r\n$4\r\nLOAD\r\n")
                && frame
                    .windows(function_library.len())
                    .any(|window| window == function_library)
        }),
        "rewritten AOF should contain a FUNCTION LOAD snapshot frame"
    );
    assert!(
        operations.iter().any(|frame| {
            frame
                .windows(b"boot:function:key:live".len())
                .any(|window| window == b"boot:function:key:live")
        }),
        "rewritten AOF should keep live writes after the rewrite"
    );

    let second_processor =
        Arc::new(RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap());
    apply_test_startup_config_overrides(second_processor.as_ref(), startup_overrides);
    let second_server = TestServerHandle::start_with_processor(Arc::clone(&second_processor)).await;
    let mut second_client = second_server.connect().await;
    send_and_expect(
        &mut second_client,
        &encode_resp_command(&[b"GET", b"boot:function:key"]),
        b"$10\r\nboot-value\r\n",
    )
    .await;
    send_and_expect(
        &mut second_client,
        &encode_resp_command(&[b"GET", b"boot:function:key:live"]),
        b"$10\r\nlive-value\r\n",
    )
    .await;
    let restarted_value = send_and_read_resp_value(
        &mut second_client,
        &encode_resp_command(&[
            b"FCALL",
            b"setv",
            b"1",
            b"boot:function:key:restart",
            b"restart-value",
        ]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(resp_socket_bulk(&restarted_value), b"restart-value");

    second_server.shutdown().await;
    second_processor.stop_live_aof_durability_runtime();
    let _ = std::fs::remove_dir_all(temp_dir);
}

#[tokio::test]
async fn waitaof_local_times_out_before_everysec_fsync_like_external_wait_scenario() {
    let temp_dir = unique_test_temp_dir("waitaof-everysec");

    let processor =
        Arc::new(RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap());
    apply_test_startup_config_overrides(
        processor.as_ref(),
        aof_test_startup_overrides(&temp_dir, "waitaof-everysec.aof", "everysec"),
    );
    let server = TestServerHandle::start_with_processor(Arc::clone(&processor)).await;
    let mut client = server.connect().await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"waitaof:everysec", b"value"]),
        b"+OK\r\n",
    )
    .await;

    let reply = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"WAITAOF", b"1", b"0", b"50"]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(resp_socket_integer_array(&reply), vec![0, 0]);

    server.shutdown().await;
    let _ = std::fs::remove_file(temp_dir.join("waitaof-everysec.aof"));
    let _ = std::fs::remove_dir_all(temp_dir);
}

#[tokio::test]
async fn waitaof_local_succeeds_after_always_fsync_write_like_external_wait_scenario() {
    let temp_dir = unique_test_temp_dir("waitaof-always");

    let processor =
        Arc::new(RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap());
    apply_test_startup_config_overrides(
        processor.as_ref(),
        aof_test_startup_overrides(&temp_dir, "waitaof-always.aof", "always"),
    );
    let server = TestServerHandle::start_with_processor(Arc::clone(&processor)).await;
    let mut client = server.connect().await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"waitaof:always", b"value"]),
        b"+OK\r\n",
    )
    .await;

    let reply = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"WAITAOF", b"1", b"0", b"2000"]),
        Duration::from_secs(3),
    )
    .await;
    assert_eq!(resp_socket_integer_array(&reply), vec![1, 0]);

    server.shutdown().await;
    let _ = std::fs::remove_file(temp_dir.join("waitaof-always.aof"));
    let _ = std::fs::remove_dir_all(temp_dir);
}

#[tokio::test]
async fn config_set_appendfsync_reconfigures_live_aof_runtime_and_updates_waitaof_like_external_wait_scenario()
 {
    let temp_dir = unique_test_temp_dir("waitaof-reconfig");

    let processor =
        Arc::new(RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap());
    apply_test_startup_config_overrides(
        processor.as_ref(),
        aof_test_startup_overrides(&temp_dir, "waitaof-reconfig.aof", "everysec"),
    );
    let server = TestServerHandle::start_with_processor(Arc::clone(&processor)).await;
    let mut client = server.connect().await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"waitaof:reconfig:before", b"value"]),
        b"+OK\r\n",
    )
    .await;
    let before_reconfigure = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"WAITAOF", b"1", b"0", b"50"]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(resp_socket_integer_array(&before_reconfigure), vec![0, 0]);

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"CONFIG", b"SET", b"appendfsync", b"always"]),
        b"+OK\r\n",
    )
    .await;

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"waitaof:reconfig:after", b"value"]),
        b"+OK\r\n",
    )
    .await;
    let after_reconfigure = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"WAITAOF", b"1", b"0", b"2000"]),
        Duration::from_secs(3),
    )
    .await;
    assert_eq!(resp_socket_integer_array(&after_reconfigure), vec![1, 0]);

    server.shutdown().await;
    let _ = std::fs::remove_file(temp_dir.join("waitaof-reconfig.aof"));
    let _ = std::fs::remove_dir_all(temp_dir);
}

#[tokio::test]
async fn waitaof_replica_copy_before_fsync_like_external_wait_scenario() {
    let master_temp_dir = unique_test_temp_dir("waitaof-master-before-fsync");
    let replica_temp_dir = unique_test_temp_dir("waitaof-replica-before-fsync");

    let master_processor =
        Arc::new(RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap());
    apply_test_startup_config_overrides(
        master_processor.as_ref(),
        aof_test_startup_overrides(&master_temp_dir, "master-waitaof.aof", "always"),
    );

    let replica_processor =
        Arc::new(RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap());
    apply_test_startup_config_overrides(
        replica_processor.as_ref(),
        aof_test_startup_overrides(&replica_temp_dir, "replica-waitaof.aof", "everysec"),
    );
    let master_server = TestServerHandle::start_with_processor(master_processor).await;
    let replica_server = TestServerHandle::start_with_processor(replica_processor).await;

    let mut master_client = master_server.connect().await;
    let mut replica_client = replica_server.connect().await;

    let master_port = master_server.addr.port().to_string();
    send_and_expect(
        &mut replica_client,
        &encode_resp_command(&[b"REPLICAOF", b"127.0.0.1", master_port.as_bytes()]),
        b"+OK\r\n",
    )
    .await;
    let _ = wait_for_replica_info_line(&mut master_client, 1, Duration::from_secs(5)).await;

    send_and_expect(
        &mut master_client,
        &encode_resp_command(&[b"SET", b"waitaof:replica:seed", b"seed"]),
        b"+OK\r\n",
    )
    .await;
    let acknowledged = send_and_read_integer(
        &mut master_client,
        &encode_resp_command(&[b"WAIT", b"1", b"2000"]),
        Duration::from_secs(3),
    )
    .await;
    assert!(
        acknowledged >= 1,
        "expected WAIT to observe the downstream replica before WAITAOF, got {acknowledged}"
    );

    // Redis tests/unit/wait.tcl: "WAITAOF replica copy before fsync"
    send_and_expect(
        &mut master_client,
        &encode_resp_command(&[b"SET", b"waitaof:replica:before-fsync", b"value"]),
        b"+OK\r\n",
    )
    .await;
    let reply = send_and_read_resp_value(
        &mut master_client,
        &encode_resp_command(&[b"WAITAOF", b"0", b"1", b"50"]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(resp_socket_integer_array(&reply), vec![1, 0]);

    master_server.shutdown().await;
    replica_server.shutdown().await;
    let _ = std::fs::remove_file(master_temp_dir.join("master-waitaof.aof"));
    let _ = std::fs::remove_file(replica_temp_dir.join("replica-waitaof.aof"));
    let _ = std::fs::remove_dir_all(master_temp_dir);
    let _ = std::fs::remove_dir_all(replica_temp_dir);
}

#[tokio::test]
async fn waitaof_master_isnt_configured_to_do_aof_like_external_wait_scenario() {
    let replica_temp_dir = unique_test_temp_dir("waitaof-replica-master-aof-off");

    let master_processor =
        Arc::new(RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap());

    let replica_processor =
        Arc::new(RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap());
    apply_test_startup_config_overrides(
        replica_processor.as_ref(),
        aof_test_startup_overrides(&replica_temp_dir, "replica-master-aof-off.aof", "always"),
    );
    let master_server = TestServerHandle::start_with_processor(master_processor).await;
    let replica_server = TestServerHandle::start_with_processor(replica_processor).await;

    let mut master_client = master_server.connect().await;
    let mut replica_client = replica_server.connect().await;

    let master_port = master_server.addr.port().to_string();
    send_and_expect(
        &mut replica_client,
        &encode_resp_command(&[b"REPLICAOF", b"127.0.0.1", master_port.as_bytes()]),
        b"+OK\r\n",
    )
    .await;
    let _ = wait_for_replica_info_line(&mut master_client, 1, Duration::from_secs(5)).await;

    send_and_expect(
        &mut master_client,
        &encode_resp_command(&[b"SET", b"waitaof:replica:no-master-aof-seed", b"seed"]),
        b"+OK\r\n",
    )
    .await;
    let acknowledged = send_and_read_integer(
        &mut master_client,
        &encode_resp_command(&[b"WAIT", b"1", b"2000"]),
        Duration::from_secs(3),
    )
    .await;
    assert!(
        acknowledged >= 1,
        "expected WAIT to observe the downstream replica before WAITAOF, got {acknowledged}"
    );

    // Redis tests/unit/wait.tcl: "WAITAOF master isn't configured to do AOF"
    send_and_expect(
        &mut master_client,
        &encode_resp_command(&[b"SET", b"waitaof:replica:no-master-aof", b"value"]),
        b"+OK\r\n",
    )
    .await;
    let reply = send_and_read_resp_value(
        &mut master_client,
        &encode_resp_command(&[b"WAITAOF", b"0", b"1", b"2000"]),
        Duration::from_secs(3),
    )
    .await;
    assert_eq!(resp_socket_integer_array(&reply), vec![0, 1]);

    master_server.shutdown().await;
    replica_server.shutdown().await;
    let _ = std::fs::remove_file(replica_temp_dir.join("replica-master-aof-off.aof"));
    let _ = std::fs::remove_dir_all(replica_temp_dir);
}

#[tokio::test]
async fn wait_times_out_without_downstream_replicas() {
    let processor = Arc::new(RequestProcessor::new().unwrap());
    let server = TestServerHandle::start_with_processor(processor).await;
    let mut client = server.connect().await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"wait:no-replica", b"v"]),
        b"+OK\r\n",
    )
    .await;
    let wait_frame = encode_resp_command(&[b"WAIT", b"1", b"50"]);
    let acknowledged =
        send_and_read_integer(&mut client, &wait_frame, Duration::from_secs(1)).await;
    assert_eq!(
        acknowledged, 0,
        "expected WAIT timeout without replicas to return 0"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn wait_after_exec_uses_connection_replication_offset() {
    let master_server =
        TestServerHandle::start_with_processor(Arc::new(RequestProcessor::new().unwrap())).await;
    let replica_server =
        TestServerHandle::start_with_processor(Arc::new(RequestProcessor::new().unwrap())).await;

    let mut master_client = master_server.connect().await;
    let mut replica_client = replica_server.connect().await;

    let master_port = master_server.addr.port().to_string();
    let slaveof = encode_resp_command(&[b"SLAVEOF", b"127.0.0.1", master_port.as_bytes()]);
    send_and_expect(&mut replica_client, &slaveof, b"+OK\r\n").await;
    let _ = wait_for_replica_info_line(&mut master_client, 1, Duration::from_secs(5)).await;

    send_and_expect(
        &mut master_client,
        &encode_resp_command(&[b"MULTI"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut master_client,
        &encode_resp_command(&[b"SET", b"wait:exec", b"value"]),
        b"+QUEUED\r\n",
    )
    .await;
    send_and_expect(
        &mut master_client,
        &encode_resp_command(&[b"EXEC"]),
        b"*1\r\n+OK\r\n",
    )
    .await;

    let acknowledged = send_and_read_integer(
        &mut master_client,
        &encode_resp_command(&[b"WAIT", b"1", b"2000"]),
        Duration::from_secs(3),
    )
    .await;
    assert!(
        acknowledged >= 1,
        "expected WAIT after EXEC to observe replicated transaction write, got {acknowledged}"
    );

    master_server.shutdown().await;
    replica_server.shutdown().await;
}

#[tokio::test]
async fn wait_in_script_reports_acknowledged_replica_count() {
    let master_processor =
        Arc::new(RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap());
    let master_server = TestServerHandle::start_with_processor(master_processor).await;
    let replica_server =
        TestServerHandle::start_with_processor(Arc::new(RequestProcessor::new().unwrap())).await;

    let mut master_client = master_server.connect().await;
    let mut replica_client = replica_server.connect().await;

    let master_port = master_server.addr.port().to_string();
    let slaveof = encode_resp_command(&[b"SLAVEOF", b"127.0.0.1", master_port.as_bytes()]);
    send_and_expect(&mut replica_client, &slaveof, b"+OK\r\n").await;
    let _ = wait_for_replica_info_line(&mut master_client, 1, Duration::from_secs(5)).await;

    send_and_expect(
        &mut master_client,
        &encode_resp_command(&[b"SET", b"wait:script", b"value"]),
        b"+OK\r\n",
    )
    .await;

    let eval_wait = encode_resp_command(&[b"EVAL", b"return redis.call('WAIT', '1', '0')", b"0"]);
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut last_response = None;
    let acknowledged = loop {
        let response =
            send_and_read_resp_value(&mut master_client, &eval_wait, Duration::from_secs(1)).await;
        let acknowledged = match &response {
            RespSocketValue::Integer(value) => *value,
            _ => {
                last_response = Some(response);
                break -1;
            }
        };
        if acknowledged == 1 || Instant::now() >= deadline {
            break acknowledged;
        }
        sleep(Duration::from_millis(10)).await;
    };
    if let Some(response) = last_response {
        panic!("expected integer response for WAIT in script, got {response:?}");
    }
    assert_eq!(
        acknowledged, 1,
        "expected WAIT in script to report the acknowledged replica count"
    );

    master_server.shutdown().await;
    replica_server.shutdown().await;
}

#[tokio::test]
async fn scripting_min_replicas_gate_matches_external_scenario() {
    let _serial = lock_scripting_test_serial().await;
    let processor =
        Arc::new(RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap());
    let server = TestServerHandle::start_with_processor(processor).await;
    let mut client = server.connect().await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"x", b"some value"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"CONFIG", b"SET", b"min-replicas-to-write", b"1"]),
        b"+OK\r\n",
    )
    .await;

    send_and_expect(
        &mut client,
        &encode_resp_command(&[
            b"EVAL",
            b"#!lua flags=no-writes\nreturn redis.call('get','x')",
            b"1",
            b"x",
        ]),
        b"$10\r\nsome value\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"EVAL", b"return redis.call('get','x')", b"1", b"x"]),
        b"$10\r\nsome value\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"EVAL", b"#!lua\nreturn redis.call('get','x')", b"1", b"x"]),
        b"-NOREPLICAS Not enough good replicas to write.\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"EVAL", b"return redis.call('set','x',1)", b"1", b"x"]),
        b"-NOREPLICAS Not enough good replicas to write.\r\n",
    )
    .await;

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"CONFIG", b"SET", b"min-replicas-to-write", b"0"]),
        b"+OK\r\n",
    )
    .await;

    server.shutdown().await;
}

#[tokio::test]
async fn replicaof_replication_rewrites_evalsha_after_replica_cache_flush() {
    let master_processor =
        Arc::new(RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap());
    let replica_processor =
        Arc::new(RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap());
    let master_server = TestServerHandle::start_with_processor(master_processor).await;
    let replica_server = TestServerHandle::start_with_processor(replica_processor).await;

    let mut master_client = master_server.connect().await;
    let mut replica_client = replica_server.connect().await;

    let master_port = master_server.addr.port().to_string();
    let slaveof = encode_resp_command(&[b"SLAVEOF", b"127.0.0.1", master_port.as_bytes()]);
    send_and_expect(&mut replica_client, &slaveof, b"+OK\r\n").await;

    let script = b"redis.call('SET', KEYS[1], ARGV[1]); return ARGV[1]";
    let sha = send_and_read_bulk_payload(
        &mut master_client,
        &encode_resp_command(&[b"SCRIPT", b"LOAD", script]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(sha.len(), 40);

    let key = b"repl:lua:key";
    let mut replicated_v1 = false;
    for _ in 0..50 {
        let evalsha_v1 = encode_resp_command(&[b"EVALSHA", sha.as_slice(), b"1", key, b"v1"]);
        send_and_expect(&mut master_client, &evalsha_v1, b"$2\r\nv1\r\n").await;

        let get_frame = encode_resp_command(&[b"GET", key]);
        replica_client.write_all(&get_frame).await.unwrap();
        let mut response = [0u8; 64];
        let bytes_read = tokio::time::timeout(
            Duration::from_millis(200),
            replica_client.read(&mut response),
        )
        .await
        .unwrap()
        .unwrap();
        if &response[..bytes_read] == b"$2\r\nv1\r\n" {
            replicated_v1 = true;
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
    assert!(replicated_v1, "replica did not receive first evalsha write");

    send_and_expect(
        &mut replica_client,
        b"*3\r\n$9\r\nREPLICAOF\r\n$2\r\nNO\r\n$3\r\nONE\r\n",
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut replica_client,
        b"*2\r\n$6\r\nSCRIPT\r\n$5\r\nFLUSH\r\n",
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut replica_client,
        &encode_resp_command(&[b"EVALSHA", sha.as_slice(), b"1", key, b"probe"]),
        b"-NOSCRIPT No matching script. Please use EVAL.\r\n",
    )
    .await;

    send_and_expect(&mut replica_client, &slaveof, b"+OK\r\n").await;

    let mut replicated_v2 = false;
    for _ in 0..80 {
        let evalsha_v2 = encode_resp_command(&[b"EVALSHA", sha.as_slice(), b"1", key, b"v2"]);
        send_and_expect(&mut master_client, &evalsha_v2, b"$2\r\nv2\r\n").await;

        let get_frame = encode_resp_command(&[b"GET", key]);
        replica_client.write_all(&get_frame).await.unwrap();
        let mut response = [0u8; 64];
        let bytes_read = tokio::time::timeout(
            Duration::from_millis(200),
            replica_client.read(&mut response),
        )
        .await
        .unwrap()
        .unwrap();
        if &response[..bytes_read] == b"$2\r\nv2\r\n" {
            replicated_v2 = true;
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
    assert!(
        replicated_v2,
        "replica did not receive second evalsha write"
    );

    master_server.shutdown().await;
    replica_server.shutdown().await;
}

#[tokio::test]
async fn replicaof_replication_propagates_function_load_and_fcall() {
    let master_processor =
        Arc::new(RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap());
    let replica_processor =
        Arc::new(RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap());
    let master_server = TestServerHandle::start_with_processor(master_processor).await;
    let replica_server = TestServerHandle::start_with_processor(replica_processor).await;

    let mut master_client = master_server.connect().await;
    let mut replica_client = replica_server.connect().await;

    let master_port = master_server.addr.port().to_string();
    let slaveof = encode_resp_command(&[b"SLAVEOF", b"127.0.0.1", master_port.as_bytes()]);
    send_and_expect(&mut replica_client, &slaveof, b"+OK\r\n").await;
    send_and_expect(
        &mut replica_client,
        &encode_resp_command(&[b"SCRIPT", b"EXISTS", b"deadbeef"]),
        b"*1\r\n:0\r\n",
    )
    .await;
    send_and_expect(
        &mut replica_client,
        &encode_resp_command(&[b"FUNCTION", b"LIST"]),
        b"*0\r\n",
    )
    .await;

    let library_source = b"#!lua name=lib_repl\nredis.register_function{function_name='rw_set', callback=function(keys, args) return redis.call('SET', keys[1], args[1]) end}\nredis.register_function{function_name='ro_get', callback=function(keys, args) return redis.call('GET', keys[1]) end, flags={'no-writes'}}";
    let mut function_replicated = false;
    for _ in 0..80 {
        send_and_expect(
            &mut master_client,
            &encode_resp_command(&[b"FUNCTION", b"LOAD", b"REPLACE", library_source]),
            b"$8\r\nlib_repl\r\n",
        )
        .await;

        let probe = encode_resp_command(&[b"FCALL_RO", b"ro_get", b"1", b"repl:function:probe"]);
        replica_client.write_all(&probe).await.unwrap();
        let mut response = [0u8; 128];
        let bytes_read = tokio::time::timeout(
            Duration::from_millis(200),
            replica_client.read(&mut response),
        )
        .await
        .unwrap()
        .unwrap();
        if &response[..bytes_read] == b"$-1\r\n" {
            function_replicated = true;
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
    assert!(
        function_replicated,
        "replica did not receive replicated function library"
    );

    let key = b"repl:function:key";
    let mut replicated = false;
    for _ in 0..80 {
        let fcall_rw = encode_resp_command(&[b"FCALL", b"rw_set", b"1", key, b"v1"]);
        send_and_expect(&mut master_client, &fcall_rw, b"+OK\r\n").await;

        let fcall_ro = encode_resp_command(&[b"FCALL_RO", b"ro_get", b"1", key]);
        replica_client.write_all(&fcall_ro).await.unwrap();
        let mut response = [0u8; 128];
        let bytes_read = tokio::time::timeout(
            Duration::from_millis(200),
            replica_client.read(&mut response),
        )
        .await
        .unwrap()
        .unwrap();
        if &response[..bytes_read] == b"$2\r\nv1\r\n" {
            replicated = true;
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }

    assert!(
        replicated,
        "replica did not receive function library and fcall updates"
    );

    master_server.shutdown().await;
    replica_server.shutdown().await;
}

#[tokio::test]
async fn eval_allow_stale_matches_external_stale_replica_semantics() {
    let (addr, shutdown_tx, server, processor) =
        start_test_server_with_scripting_enabled_and_processor().await;
    let mut client = TcpStream::connect(addr).await.unwrap();

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"CONFIG", b"SET", b"replica-serve-stale-data", b"no"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SLAVEOF", b"127.0.0.1", b"1"]),
        b"+OK\r\n",
    )
    .await;
    wait_for_stale_replica_mode(processor.as_ref(), Duration::from_secs(5)).await;

    let plain_eval = send_and_read_error_line(
        &mut client,
        &encode_resp_command(&[b"EVAL", b"return redis.call('get','x')", b"1", b"x"]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(
        plain_eval,
        "MASTERDOWN Link with MASTER is down and replica-serve-stale-data is set to 'no'."
    );

    let no_writes_eval = send_and_read_error_line(
        &mut client,
        &encode_resp_command(&[b"EVAL", b"#!lua flags=no-writes\nreturn 1", b"0"]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(
        no_writes_eval,
        "MASTERDOWN Link with MASTER is down and replica-serve-stale-data is set to 'no'."
    );

    assert_eq!(
        send_and_read_integer(
            &mut client,
            &encode_resp_command(&[
                b"EVAL",
                b"#!lua flags=allow-stale,no-writes\nreturn 1",
                b"0",
            ]),
            Duration::from_secs(1),
        )
        .await,
        1
    );

    let stale_get_error = send_and_read_error_line(
        &mut client,
        &encode_resp_command(&[
            b"EVAL",
            b"#!lua flags=allow-stale,no-writes\nreturn redis.call('get','x')",
            b"1",
            b"x",
        ]),
        Duration::from_secs(1),
    )
    .await;
    assert!(
        stale_get_error.contains("Can not execute the command on a stale replica"),
        "unexpected stale replica script error: {stale_get_error}"
    );

    let echo_payload = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[
            b"EVAL",
            b"#!lua flags=allow-stale,no-writes\nreturn redis.call('echo','foobar')",
            b"0",
        ]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(resp_socket_bulk(&echo_payload), b"foobar");

    let sha = send_and_read_bulk_payload(
        &mut client,
        &encode_resp_command(&[
            b"SCRIPT",
            b"LOAD",
            b"#!lua flags=allow-stale,no-writes\nreturn redis.call('echo','foobar')",
        ]),
        Duration::from_secs(1),
    )
    .await;
    let evalsha_payload = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"EVALSHA", sha.as_slice(), b"0"]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(resp_socket_bulk(&evalsha_payload), b"foobar");

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn function_allow_stale_matches_external_stale_replica_semantics() {
    let (addr, shutdown_tx, server, processor) =
        start_test_server_with_scripting_enabled_and_processor().await;
    let mut client = TcpStream::connect(addr).await.unwrap();

    let library_source = b"#!lua name=test\nredis.register_function{function_name='f1', callback=function() return 'hello' end, flags={'no-writes'}}\nredis.register_function{function_name='f2', callback=function() return 'hello' end, flags={'allow-stale', 'no-writes'}}\nredis.register_function{function_name='f3', callback=function() return redis.call('get', 'x') end, flags={'allow-stale', 'no-writes'}}\nredis.register_function{function_name='f4', callback=function() return redis.call('info', 'server') end, flags={'allow-stale', 'no-writes'}}";
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"FUNCTION", b"LOAD", b"REPLACE", library_source]),
        b"$4\r\ntest\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"CONFIG", b"SET", b"replica-serve-stale-data", b"no"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SLAVEOF", b"127.0.0.1", b"1"]),
        b"+OK\r\n",
    )
    .await;
    wait_for_stale_replica_mode(processor.as_ref(), Duration::from_secs(5)).await;

    let f1_error = send_and_read_error_line(
        &mut client,
        &encode_resp_command(&[b"FCALL", b"f1", b"0"]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(
        f1_error,
        "MASTERDOWN Link with MASTER is down and replica-serve-stale-data is set to 'no'."
    );

    let f2_payload = send_and_read_bulk_payload(
        &mut client,
        &encode_resp_command(&[b"FCALL", b"f2", b"0"]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(f2_payload, b"hello");

    let f3_error = send_and_read_error_line(
        &mut client,
        &encode_resp_command(&[b"FCALL", b"f3", b"1", b"x"]),
        Duration::from_secs(1),
    )
    .await;
    assert!(
        f3_error.contains("Can not execute the command on a stale replica"),
        "unexpected stale replica function error: {f3_error}"
    );

    let f4_payload = send_and_read_bulk_payload(
        &mut client,
        &encode_resp_command(&[b"FCALL", b"f4", b"0"]),
        Duration::from_secs(1),
    )
    .await;
    assert!(
        String::from_utf8_lossy(&f4_payload).contains("redis_version"),
        "expected INFO server payload, got: {}",
        String::from_utf8_lossy(&f4_payload)
    );

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn sync_replication_stream_starts_with_select_db_zero() {
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

    let mut replica_stream = TcpStream::connect(addr).await.unwrap();
    replica_stream.write_all(b"SYNC\r\n").await.unwrap();

    let header = read_resp_line_with_timeout(&mut replica_stream, Duration::from_secs(1)).await;
    assert!(
        header.starts_with(b"$"),
        "SYNC response must start with bulk RDB length, got: {}",
        String::from_utf8_lossy(&header)
    );
    let payload_len = std::str::from_utf8(&header[1..])
        .unwrap()
        .parse::<usize>()
        .unwrap();
    let _payload =
        read_exact_with_timeout(&mut replica_stream, payload_len, Duration::from_secs(1)).await;

    let mut client = TcpStream::connect(addr).await.unwrap();
    send_and_expect(
        &mut client,
        b"*3\r\n$3\r\nSET\r\n$11\r\nsync:select\r\n$2\r\nv1\r\n",
        b"+OK\r\n",
    )
    .await;

    let mut expected = Vec::new();
    expected.extend_from_slice(&encode_resp_command(&[b"SELECT", b"0"]));
    expected.extend_from_slice(&encode_resp_command(&[b"SET", b"sync:select", b"v1"]));
    let replicated =
        read_exact_with_timeout(&mut replica_stream, expected.len(), Duration::from_secs(1)).await;
    assert_eq!(replicated, expected);

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn sync_replication_stream_rewrites_bzmpop_as_zpop_commands_like_redis_external_scenario() {
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

    let mut replica_stream = TcpStream::connect(addr).await.unwrap();
    replica_stream.write_all(b"SYNC\r\n").await.unwrap();
    let header = read_resp_line_with_timeout(&mut replica_stream, Duration::from_secs(1)).await;
    assert!(header.starts_with(b"$"));
    let payload_len = std::str::from_utf8(&header[1..])
        .unwrap()
        .parse::<usize>()
        .unwrap();
    let _payload =
        read_exact_with_timeout(&mut replica_stream, payload_len, Duration::from_secs(1)).await;

    let mut controller = TcpStream::connect(addr).await.unwrap();
    let mut inspector = TcpStream::connect(addr).await.unwrap();
    let mut waiter = TcpStream::connect(addr).await.unwrap();

    // Redis tests/unit/type/zset.tcl:
    // "BZMPOP propagate as pop with count command to replica"
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[
            b"ZADD",
            b"myzset{t}",
            b"1",
            b"one",
            b"2",
            b"two",
            b"3",
            b"three",
        ]),
        b":3\r\n",
    )
    .await;
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[
            b"ZADD",
            b"myzset2{t}",
            b"4",
            b"four",
            b"5",
            b"five",
            b"6",
            b"six",
        ]),
        b":3\r\n",
    )
    .await;

    controller
        .write_all(&encode_resp_command(&[
            b"BZMPOP",
            b"0",
            b"1",
            b"myzset{t}",
            b"MIN",
        ]))
        .await
        .unwrap();
    assert_eq!(
        read_zmpop_like_response(&mut controller, Duration::from_secs(1)).await,
        (
            b"myzset{t}".to_vec(),
            vec![(b"one".to_vec(), b"1".to_vec())]
        )
    );

    controller
        .write_all(&encode_resp_command(&[
            b"BZMPOP",
            b"0",
            b"2",
            b"myzset{t}",
            b"myzset2{t}",
            b"MAX",
            b"COUNT",
            b"10",
        ]))
        .await
        .unwrap();
    assert_eq!(
        read_zmpop_like_response(&mut controller, Duration::from_secs(1)).await,
        (
            b"myzset{t}".to_vec(),
            vec![
                (b"three".to_vec(), b"3".to_vec()),
                (b"two".to_vec(), b"2".to_vec())
            ]
        )
    );

    controller
        .write_all(&encode_resp_command(&[
            b"BZMPOP",
            b"0",
            b"2",
            b"myzset{t}",
            b"myzset2{t}",
            b"MAX",
            b"COUNT",
            b"10",
        ]))
        .await
        .unwrap();
    assert_eq!(
        read_zmpop_like_response(&mut controller, Duration::from_secs(1)).await,
        (
            b"myzset2{t}".to_vec(),
            vec![
                (b"six".to_vec(), b"6".to_vec()),
                (b"five".to_vec(), b"5".to_vec()),
                (b"four".to_vec(), b"4".to_vec())
            ]
        )
    );

    waiter
        .write_all(&encode_resp_command(&[
            b"BZMPOP",
            b"0",
            b"1",
            b"myzset{t}",
            b"MIN",
            b"COUNT",
            b"1",
        ]))
        .await
        .unwrap();
    wait_for_blocked_clients(&mut inspector, 1, Duration::from_secs(1)).await;
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"ZADD", b"myzset{t}", b"1", b"one"]),
        b":1\r\n",
    )
    .await;
    assert_eq!(
        read_zmpop_like_response(&mut waiter, Duration::from_secs(1)).await,
        (
            b"myzset{t}".to_vec(),
            vec![(b"one".to_vec(), b"1".to_vec())]
        )
    );

    waiter
        .write_all(&encode_resp_command(&[
            b"BZMPOP",
            b"0",
            b"2",
            b"myzset{t}",
            b"myzset2{t}",
            b"MIN",
            b"COUNT",
            b"5",
        ]))
        .await
        .unwrap();
    wait_for_blocked_clients(&mut inspector, 1, Duration::from_secs(1)).await;
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[
            b"ZADD",
            b"myzset{t}",
            b"1",
            b"one",
            b"2",
            b"two",
            b"3",
            b"three",
        ]),
        b":3\r\n",
    )
    .await;
    assert_eq!(
        read_zmpop_like_response(&mut waiter, Duration::from_secs(1)).await,
        (
            b"myzset{t}".to_vec(),
            vec![
                (b"one".to_vec(), b"1".to_vec()),
                (b"two".to_vec(), b"2".to_vec()),
                (b"three".to_vec(), b"3".to_vec())
            ]
        )
    );

    waiter
        .write_all(&encode_resp_command(&[
            b"BZMPOP",
            b"0",
            b"2",
            b"myzset{t}",
            b"myzset2{t}",
            b"MAX",
            b"COUNT",
            b"10",
        ]))
        .await
        .unwrap();
    wait_for_blocked_clients(&mut inspector, 1, Duration::from_secs(1)).await;
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[
            b"ZADD",
            b"myzset2{t}",
            b"4",
            b"four",
            b"5",
            b"five",
            b"6",
            b"six",
        ]),
        b":3\r\n",
    )
    .await;
    assert_eq!(
        read_zmpop_like_response(&mut waiter, Duration::from_secs(1)).await,
        (
            b"myzset2{t}".to_vec(),
            vec![
                (b"six".to_vec(), b"6".to_vec()),
                (b"five".to_vec(), b"5".to_vec()),
                (b"four".to_vec(), b"4".to_vec())
            ]
        )
    );

    send_and_expect(
        &mut controller,
        &encode_resp_command(&[
            b"BZMPOP",
            b"0.01",
            b"1",
            b"myzset{t}",
            b"MAX",
            b"COUNT",
            b"10",
        ]),
        b"*-1\r\n",
    )
    .await;
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"SET", b"foo{t}", b"bar"]),
        b"+OK\r\n",
    )
    .await;

    let mut expected = Vec::new();
    expected.extend_from_slice(&encode_resp_command(&[b"SELECT", b"0"]));
    expected.extend_from_slice(&encode_resp_command(&[
        b"ZADD",
        b"myzset{t}",
        b"1",
        b"one",
        b"2",
        b"two",
        b"3",
        b"three",
    ]));
    expected.extend_from_slice(&encode_resp_command(&[
        b"ZADD",
        b"myzset2{t}",
        b"4",
        b"four",
        b"5",
        b"five",
        b"6",
        b"six",
    ]));
    expected.extend_from_slice(&encode_resp_command(&[b"ZPOPMIN", b"myzset{t}", b"1"]));
    expected.extend_from_slice(&encode_resp_command(&[b"ZPOPMAX", b"myzset{t}", b"2"]));
    expected.extend_from_slice(&encode_resp_command(&[b"ZPOPMAX", b"myzset2{t}", b"3"]));
    expected.extend_from_slice(&encode_resp_command(&[b"ZADD", b"myzset{t}", b"1", b"one"]));
    expected.extend_from_slice(&encode_resp_command(&[b"ZPOPMIN", b"myzset{t}", b"1"]));
    expected.extend_from_slice(&encode_resp_command(&[
        b"ZADD",
        b"myzset{t}",
        b"1",
        b"one",
        b"2",
        b"two",
        b"3",
        b"three",
    ]));
    expected.extend_from_slice(&encode_resp_command(&[b"ZPOPMIN", b"myzset{t}", b"3"]));
    expected.extend_from_slice(&encode_resp_command(&[
        b"ZADD",
        b"myzset2{t}",
        b"4",
        b"four",
        b"5",
        b"five",
        b"6",
        b"six",
    ]));
    expected.extend_from_slice(&encode_resp_command(&[b"ZPOPMAX", b"myzset2{t}", b"3"]));
    expected.extend_from_slice(&encode_resp_command(&[b"SET", b"foo{t}", b"bar"]));

    let replicated =
        read_exact_with_timeout(&mut replica_stream, expected.len(), Duration::from_secs(1)).await;
    assert_eq!(replicated, expected);

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn sync_replication_stream_preserves_nested_blmove_unblock_order_like_external_scenario() {
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

    let mut controller = TcpStream::connect(addr).await.unwrap();
    let mut inspector = TcpStream::connect(addr).await.unwrap();
    let mut waiter1 = TcpStream::connect(addr).await.unwrap();
    let mut waiter2 = TcpStream::connect(addr).await.unwrap();
    let mut producer = TcpStream::connect(addr).await.unwrap();

    send_and_expect(
        &mut controller,
        &encode_resp_command(&[
            b"DEL", b"src{t}", b"dst{t}", b"key1{t}", b"key2{t}", b"key3{t}",
        ]),
        b":0\r\n",
    )
    .await;

    let mut replica_stream = TcpStream::connect(addr).await.unwrap();
    replica_stream.write_all(b"SYNC\r\n").await.unwrap();
    let header = read_resp_line_with_timeout(&mut replica_stream, Duration::from_secs(1)).await;
    assert!(header.starts_with(b"$"));
    let payload_len = std::str::from_utf8(&header[1..])
        .unwrap()
        .parse::<usize>()
        .unwrap();
    let _payload =
        read_exact_with_timeout(&mut replica_stream, payload_len, Duration::from_secs(1)).await;

    waiter1
        .write_all(&encode_resp_command(&[
            b"BLMOVE", b"src{t}", b"dst{t}", b"LEFT", b"RIGHT", b"0",
        ]))
        .await
        .unwrap();
    wait_for_blocked_clients(&mut inspector, 1, Duration::from_secs(1)).await;

    waiter2
        .write_all(&encode_resp_command(&[
            b"BLMOVE", b"dst{t}", b"src{t}", b"RIGHT", b"LEFT", b"0",
        ]))
        .await
        .unwrap();
    wait_for_blocked_clients(&mut inspector, 2, Duration::from_secs(1)).await;

    let mut pipeline = Vec::new();
    pipeline.extend_from_slice(&encode_resp_command(&[b"SET", b"key1{t}", b"value1"]));
    pipeline.extend_from_slice(&encode_resp_command(&[b"LPUSH", b"src{t}", b"dummy"]));
    pipeline.extend_from_slice(&encode_resp_command(&[b"SET", b"key2{t}", b"value2"]));
    producer.write_all(&pipeline).await.unwrap();

    wait_for_blocked_clients(&mut inspector, 0, Duration::from_secs(1)).await;

    let dummy_bulk = b"$5\r\ndummy\r\n";
    let waiter1_response =
        read_exact_with_timeout(&mut waiter1, dummy_bulk.len(), Duration::from_secs(1)).await;
    let waiter2_response =
        read_exact_with_timeout(&mut waiter2, dummy_bulk.len(), Duration::from_secs(1)).await;
    assert_eq!(waiter1_response, dummy_bulk);
    assert_eq!(waiter2_response, dummy_bulk);

    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"SET", b"key3{t}", b"value3"]),
        b"+OK\r\n",
    )
    .await;

    let mut expected_pipeline_responses = Vec::new();
    expected_pipeline_responses.extend_from_slice(b"+OK\r\n");
    expected_pipeline_responses.extend_from_slice(b":1\r\n");
    expected_pipeline_responses.extend_from_slice(b"+OK\r\n");
    let pipeline_responses = read_exact_with_timeout(
        &mut producer,
        expected_pipeline_responses.len(),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(pipeline_responses, expected_pipeline_responses);

    let mut expected = Vec::new();
    expected.extend_from_slice(&encode_resp_command(&[b"SELECT", b"0"]));
    expected.extend_from_slice(&encode_resp_command(&[b"SET", b"key1{t}", b"value1"]));
    expected.extend_from_slice(&encode_resp_command(&[b"LPUSH", b"src{t}", b"dummy"]));
    expected.extend_from_slice(&encode_resp_command(&[
        b"LMOVE", b"src{t}", b"dst{t}", b"LEFT", b"RIGHT",
    ]));
    expected.extend_from_slice(&encode_resp_command(&[
        b"LMOVE", b"dst{t}", b"src{t}", b"RIGHT", b"LEFT",
    ]));
    expected.extend_from_slice(&encode_resp_command(&[b"SET", b"key2{t}", b"value2"]));
    expected.extend_from_slice(&encode_resp_command(&[b"SET", b"key3{t}", b"value3"]));
    let replicated =
        read_exact_with_timeout(&mut replica_stream, expected.len(), Duration::from_secs(1)).await;
    assert_eq!(replicated, expected);

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn sync_replication_stream_rewrites_hgetdel_as_hdel_like_redis_external_scenario() {
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

    let mut replica_stream = TcpStream::connect(addr).await.unwrap();
    replica_stream.write_all(b"SYNC\r\n").await.unwrap();
    let header = read_resp_line_with_timeout(&mut replica_stream, Duration::from_secs(1)).await;
    assert!(header.starts_with(b"$"));
    let payload_len = std::str::from_utf8(&header[1..])
        .unwrap()
        .parse::<usize>()
        .unwrap();
    let _payload =
        read_exact_with_timeout(&mut replica_stream, payload_len, Duration::from_secs(1)).await;

    let mut client = TcpStream::connect(addr).await.unwrap();
    send_and_expect(
        &mut client,
        &encode_resp_command(&[
            b"HSET", b"key1", b"f1", b"v1", b"f2", b"v2", b"f3", b"v3", b"f4", b"v4", b"f5", b"v5",
        ]),
        b":5\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"HGETDEL", b"key1", b"FIELDS", b"1", b"f1"]),
        b"*1\r\n$2\r\nv1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"HGETDEL", b"key1", b"FIELDS", b"2", b"f2", b"f3"]),
        b"*2\r\n$2\r\nv2\r\n$2\r\nv3\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"HGETDEL", b"key1", b"FIELDS", b"2", b"f7", b"f8"]),
        b"*2\r\n$-1\r\n$-1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"HGETDEL", b"key1", b"FIELDS", b"3", b"f4", b"f5", b"f6"]),
        b"*3\r\n$2\r\nv4\r\n$2\r\nv5\r\n$-1\r\n",
    )
    .await;

    let mut expected = Vec::new();
    expected.extend_from_slice(&encode_resp_command(&[b"SELECT", b"0"]));
    expected.extend_from_slice(&encode_resp_command(&[
        b"HSET", b"key1", b"f1", b"v1", b"f2", b"v2", b"f3", b"v3", b"f4", b"v4", b"f5", b"v5",
    ]));
    expected.extend_from_slice(&encode_resp_command(&[b"HDEL", b"key1", b"f1"]));
    expected.extend_from_slice(&encode_resp_command(&[b"HDEL", b"key1", b"f2", b"f3"]));
    expected.extend_from_slice(&encode_resp_command(&[
        b"HDEL", b"key1", b"f4", b"f5", b"f6",
    ]));

    let replicated =
        read_exact_with_timeout(&mut replica_stream, expected.len(), Duration::from_secs(1)).await;
    assert_eq!(replicated, expected);

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn sync_replication_stream_rewrites_script_spop_commands_like_external_scenario() {
    let _serial = lock_scripting_test_serial().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let metrics = Arc::new(ServerMetrics::default());
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let processor =
        Arc::new(RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap());

    let server_metrics = Arc::clone(&metrics);
    let server = tokio::spawn(async move {
        run_listener_with_shutdown_and_cluster_with_processor(
            listener,
            1024,
            server_metrics,
            async move {
                let _ = shutdown_rx.await;
            },
            None,
            processor,
        )
        .await
        .unwrap();
    });

    let mut replica_stream = TcpStream::connect(addr).await.unwrap();
    replica_stream.write_all(b"SYNC\r\n").await.unwrap();
    let header = read_resp_line_with_timeout(&mut replica_stream, Duration::from_secs(1)).await;
    assert!(header.starts_with(b"$"));
    let payload_len = std::str::from_utf8(&header[1..])
        .unwrap()
        .parse::<usize>()
        .unwrap();
    let _payload =
        read_exact_with_timeout(&mut replica_stream, payload_len, Duration::from_secs(1)).await;

    let mut client = TcpStream::connect(addr).await.unwrap();
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SADD", b"myset", b"ppp"]),
        b":1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEL", b"myset"]),
        b":1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SADD", b"myset", b"a", b"b", b"c"]),
        b":3\r\n",
    )
    .await;

    let first_spop = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"EVAL", b"return redis.call('spop', 'myset')", b"0"]),
        Duration::from_secs(1),
    )
    .await;
    assert!(
        matches!(first_spop, RespSocketValue::Bulk(ref payload) if !payload.is_empty()),
        "expected non-empty bulk reply from scripted SPOP, got {first_spop:?}"
    );

    let second_spop = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"EVAL", b"return redis.call('spop', 'myset', 1)", b"0"]),
        Duration::from_secs(1),
    )
    .await;
    match second_spop {
        RespSocketValue::Array(items) => {
            assert_eq!(items.len(), 1);
            assert!(!resp_socket_bulk(&items[0]).is_empty());
        }
        other => panic!("expected single-item array reply from scripted SPOP count, got {other:?}"),
    }

    let third_spop = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[
            b"EVAL",
            b"return redis.call('spop', KEYS[1])",
            b"1",
            b"myset",
        ]),
        Duration::from_secs(1),
    )
    .await;
    assert!(
        matches!(third_spop, RespSocketValue::Bulk(ref payload) if !payload.is_empty()),
        "expected non-empty bulk reply from keyed scripted SPOP, got {third_spop:?}"
    );

    let empty_spop = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[
            b"EVAL",
            b"return redis.call('spop', KEYS[1])",
            b"1",
            b"myset",
        ]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(empty_spop, RespSocketValue::Null);

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"trailingkey", b"1"]),
        b"+OK\r\n",
    )
    .await;

    assert_eq!(
        read_replication_command_with_timeout(&mut replica_stream, Duration::from_secs(1)).await,
        vec![b"SELECT".to_vec(), b"0".to_vec()]
    );
    assert_eq!(
        read_replication_command_with_timeout(&mut replica_stream, Duration::from_secs(1)).await,
        vec![b"SADD".to_vec(), b"myset".to_vec(), b"ppp".to_vec()]
    );
    assert_eq!(
        read_replication_command_with_timeout(&mut replica_stream, Duration::from_secs(1)).await,
        vec![b"DEL".to_vec(), b"myset".to_vec()]
    );
    assert_eq!(
        read_replication_command_with_timeout(&mut replica_stream, Duration::from_secs(1)).await,
        vec![
            b"SADD".to_vec(),
            b"myset".to_vec(),
            b"a".to_vec(),
            b"b".to_vec(),
            b"c".to_vec(),
        ]
    );

    let mut popped_members = Vec::new();
    for _ in 0..3 {
        let command =
            read_replication_command_with_timeout(&mut replica_stream, Duration::from_secs(1))
                .await;
        assert_eq!(command.len(), 3);
        assert_eq!(command[0], b"srem");
        assert_eq!(command[1], b"myset");
        popped_members.push(command[2].clone());
    }
    popped_members.sort();
    assert_eq!(
        popped_members,
        vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]
    );

    assert_eq!(
        read_replication_command_with_timeout(&mut replica_stream, Duration::from_secs(1)).await,
        vec![b"SET".to_vec(), b"trailingkey".to_vec(), b"1".to_vec()]
    );

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn sync_replication_stream_rewrites_script_expire_and_argv_expansion_like_external_scenarios()
{
    let _serial = lock_scripting_test_serial().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let metrics = Arc::new(ServerMetrics::default());
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let processor =
        Arc::new(RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap());

    let server_metrics = Arc::clone(&metrics);
    let server = tokio::spawn(async move {
        run_listener_with_shutdown_and_cluster_with_processor(
            listener,
            1024,
            server_metrics,
            async move {
                let _ = shutdown_rx.await;
            },
            None,
            processor,
        )
        .await
        .unwrap();
    });

    let mut replica_stream = TcpStream::connect(addr).await.unwrap();
    replica_stream.write_all(b"SYNC\r\n").await.unwrap();
    let header = read_resp_line_with_timeout(&mut replica_stream, Duration::from_secs(1)).await;
    assert!(header.starts_with(b"$"));
    let payload_len = std::str::from_utf8(&header[1..])
        .unwrap()
        .parse::<usize>()
        .unwrap();
    let _payload =
        read_exact_with_timeout(&mut replica_stream, payload_len, Duration::from_secs(1)).await;

    let mut client = TcpStream::connect(addr).await.unwrap();
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"expirekey", b"1"]),
        b"+OK\r\n",
    )
    .await;

    let expire_result = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[
            b"EVAL",
            b"return redis.call('expire', KEYS[1], ARGV[1])",
            b"1",
            b"expirekey",
            b"3",
        ]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(resp_socket_integer(&expire_result), 1);

    let hmget_result = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[
            b"EVAL",
            b"return redis.call('hmget', KEYS[1], 1, 2, 3)",
            b"1",
            b"key",
        ]),
        Duration::from_secs(1),
    )
    .await;
    let hmget_values = resp_socket_array(&hmget_result);
    assert_eq!(hmget_values.len(), 3);
    assert!(
        hmget_values
            .iter()
            .all(|value| *value == RespSocketValue::Null)
    );

    let incrbyfloat_result = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[
            b"EVAL",
            b"return redis.call('incrbyfloat', KEYS[1], 1)",
            b"1",
            b"key",
        ]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(resp_socket_bulk(&incrbyfloat_result), b"1");

    let set_keep_ttl_result = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[
            b"EVAL",
            b"return redis.call('set', KEYS[1], '1', 'KEEPTTL')",
            b"1",
            b"key",
        ]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(resp_socket_bulk(&set_keep_ttl_result), b"OK");

    assert_eq!(
        read_replication_command_with_timeout(&mut replica_stream, Duration::from_secs(1)).await,
        vec![b"SELECT".to_vec(), b"0".to_vec()]
    );
    assert_eq!(
        read_replication_command_with_timeout(&mut replica_stream, Duration::from_secs(1)).await,
        vec![b"SET".to_vec(), b"expirekey".to_vec(), b"1".to_vec()]
    );

    let expire_command =
        read_replication_command_with_timeout(&mut replica_stream, Duration::from_secs(1)).await;
    assert_eq!(expire_command.len(), 3);
    assert_eq!(expire_command[0], b"pexpireat");
    assert_eq!(expire_command[1], b"expirekey");
    assert!(
        std::str::from_utf8(&expire_command[2])
            .unwrap()
            .parse::<u64>()
            .unwrap()
            > 0
    );

    assert_eq!(
        read_replication_command_with_timeout(&mut replica_stream, Duration::from_secs(1)).await,
        vec![
            b"set".to_vec(),
            b"key".to_vec(),
            b"1".to_vec(),
            b"KEEPTTL".to_vec(),
        ]
    );
    assert_eq!(
        read_replication_command_with_timeout(&mut replica_stream, Duration::from_secs(1)).await,
        vec![
            b"set".to_vec(),
            b"key".to_vec(),
            b"1".to_vec(),
            b"KEEPTTL".to_vec(),
        ]
    );

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

async fn start_scripting_test_server() -> (
    std::net::SocketAddr,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let metrics = Arc::new(ServerMetrics::default());
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let processor =
        Arc::new(RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap());

    let server_metrics = Arc::clone(&metrics);
    let server = tokio::spawn(async move {
        run_listener_with_shutdown_and_cluster_with_processor(
            listener,
            1024,
            server_metrics,
            async move {
                let _ = shutdown_rx.await;
            },
            None,
            processor,
        )
        .await
        .unwrap();
    });

    (addr, shutdown_tx, server)
}

async fn start_multishard_scripting_test_server() -> (
    std::net::SocketAddr,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let metrics = Arc::new(ServerMetrics::default());
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let processor =
        Arc::new(RequestProcessor::new_with_string_store_shards_and_scripting(2, true).unwrap());

    let server_metrics = Arc::clone(&metrics);
    let server = tokio::spawn(async move {
        run_listener_with_shutdown_and_cluster_with_processor(
            listener,
            1024,
            server_metrics,
            async move {
                let _ = shutdown_rx.await;
            },
            None,
            processor,
        )
        .await
        .unwrap();
    });

    (addr, shutdown_tx, server)
}

#[tokio::test]
async fn scripting_deletes_expired_key_on_access_like_external_scenario() {
    let _serial = lock_scripting_test_serial().await;
    let (addr, shutdown_tx, server) = start_scripting_test_server().await;
    let mut client = TcpStream::connect(addr).await.unwrap();

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEBUG", b"set-disable-deny-scripts", b"1"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEBUG", b"set-active-expire", b"0"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"key", b"value", b"PX", b"1"]),
        b"+OK\r\n",
    )
    .await;
    sleep(Duration::from_millis(5)).await;

    let debug_object = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"DEBUG", b"OBJECT", b"key"]),
        Duration::from_secs(1),
    )
    .await;
    assert!(!resp_socket_bulk(&debug_object).is_empty());

    let exists = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"EVAL", b"return redis.call('EXISTS', 'key')", b"1", b"key"]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(resp_socket_integer(&exists), 0);

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"EXISTS", b"key"]),
        b":0\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEBUG", b"set-active-expire", b"1"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEBUG", b"set-disable-deny-scripts", b"0"]),
        b"+OK\r\n",
    )
    .await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn scripting_time_command_uses_cached_time_like_external_scenario() {
    let _serial = lock_scripting_test_serial().await;
    let (addr, shutdown_tx, server) = start_scripting_test_server().await;
    let mut client = TcpStream::connect(addr).await.unwrap();

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEBUG", b"set-disable-deny-scripts", b"1"]),
        b"+OK\r\n",
    )
    .await;
    let response = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[
            b"EVAL",
            b"local result1 = {redis.call('TIME')}; redis.call('DEBUG', 'SLEEP', 0.01); local result2 = {redis.call('TIME')}; return {result1, result2}",
            b"0",
        ]),
        Duration::from_secs(1),
    )
    .await;
    let results = resp_socket_array(&response);
    assert_eq!(results.len(), 2);
    assert_eq!(results[0], results[1]);
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEBUG", b"set-disable-deny-scripts", b"0"]),
        b"+OK\r\n",
    )
    .await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn scripting_acl_check_cmd_matches_external_scenario_for_eval_and_function() {
    let _serial = lock_scripting_test_serial().await;
    let (addr, shutdown_tx, server) = start_scripting_test_server().await;
    let mut client = TcpStream::connect(addr).await.unwrap();

    send_and_expect(
        &mut client,
        &encode_resp_command(&[
            b"ACL",
            b"SETUSER",
            b"bob",
            b"on",
            b">123",
            b"+@scripting",
            b"+set",
            b"~x*",
        ]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"AUTH", b"bob", b"123"]),
        b"+OK\r\n",
    )
    .await;

    send_and_expect(
        &mut client,
        &encode_resp_command(&[
            b"EVAL",
            b"return redis.acl_check_cmd('set','xx',1)",
            b"1",
            b"xx",
        ]),
        b":1\r\n",
    )
    .await;
    let denied_command = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[
            b"EVAL",
            b"return redis.acl_check_cmd('hset','xx','f',1)",
            b"1",
            b"xx",
        ]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(denied_command, RespSocketValue::Null);
    let denied_key = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"EVAL", b"return redis.acl_check_cmd('set','yy',1)", b"0"]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(denied_key, RespSocketValue::Null);
    let eval_invalid = send_and_read_error_line(
        &mut client,
        &encode_resp_command(&[
            b"EVAL",
            b"return redis.acl_check_cmd('invalid-cmd','arg')",
            b"0",
        ]),
        Duration::from_secs(1),
    )
    .await;
    assert!(eval_invalid.contains("Invalid command passed to redis.acl_check_cmd()"));

    send_and_expect(
        &mut client,
        &encode_resp_command(&[
            b"FUNCTION",
            b"LOAD",
            b"REPLACE",
            b"#!lua name=aclcheck\nredis.register_function('aclcheck', function(KEYS, ARGV)\n return redis.acl_check_cmd(unpack(ARGV))\nend)",
        ]),
        b"$8\r\naclcheck\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"FCALL", b"aclcheck", b"1", b"xx", b"set", b"xx", b"1"]),
        b":1\r\n",
    )
    .await;
    let function_denied_command = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[
            b"FCALL",
            b"aclcheck",
            b"1",
            b"xx",
            b"hset",
            b"xx",
            b"f",
            b"1",
        ]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(function_denied_command, RespSocketValue::Null);
    let function_denied_key = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"FCALL", b"aclcheck", b"0", b"set", b"yy", b"1"]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(function_denied_key, RespSocketValue::Null);
    let function_invalid = send_and_read_error_line(
        &mut client,
        &encode_resp_command(&[b"FCALL", b"aclcheck", b"0", b"invalid-cmd", b"arg"]),
        Duration::from_secs(1),
    )
    .await;
    assert!(function_invalid.contains("Invalid command passed to redis.acl_check_cmd()"));

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn scripting_acl_enforcement_and_acl_log_context_match_external_scenarios() {
    let _serial = lock_scripting_test_serial().await;
    let (addr, shutdown_tx, server) = start_scripting_test_server().await;
    let timeout = Duration::from_secs(5);
    let mut admin = TcpStream::connect(addr).await.unwrap();
    let mut user = TcpStream::connect(addr).await.unwrap();

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[
            b"ACL", b"SETUSER", b"bob", b"reset", b"on", b">123", b"+eval", b"+fcall", b"allkeys",
        ]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"ACL", b"LOG", b"RESET"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut user,
        &encode_resp_command(&[b"AUTH", b"bob", b"123"]),
        b"+OK\r\n",
    )
    .await;

    let eval_denied = send_and_read_error_line(
        &mut user,
        &encode_resp_command(&[b"EVAL", b"return redis.call('incr','foo')", b"0"]),
        timeout,
    )
    .await;
    assert!(eval_denied.contains("NOPERM"));
    assert!(eval_denied.contains("incr"));

    let eval_log = send_and_read_resp_value(
        &mut admin,
        &encode_resp_command(&[b"ACL", b"LOG", b"1"]),
        timeout,
    )
    .await;
    let eval_entry = resp_socket_map_or_flat_map(&resp_socket_array(&eval_log)[0]);
    assert_eq!(
        resp_socket_bulk(eval_entry[&b"reason".to_vec()]),
        b"command"
    );
    assert_eq!(resp_socket_bulk(eval_entry[&b"context".to_vec()]), b"lua");
    assert_eq!(resp_socket_bulk(eval_entry[&b"object".to_vec()]), b"incr");
    assert!(
        String::from_utf8_lossy(resp_socket_bulk(eval_entry[&b"client-info".to_vec()]))
            .contains("cmd=eval"),
        "lua ACL log should keep the top-level EVAL command in client-info"
    );

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"FUNCTION", b"FLUSH"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut admin,
        &encode_resp_command(&[
            b"FUNCTION",
            b"LOAD",
            b"REPLACE",
            b"#!lua name=aclctx\nredis.register_function('deny_incr', function(KEYS, ARGV)\n return redis.call('incr', 'foo')\nend)",
        ]),
        b"$6\r\naclctx\r\n",
    )
    .await;
    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"ACL", b"LOG", b"RESET"]),
        b"+OK\r\n",
    )
    .await;

    let fcall_denied = send_and_read_error_line(
        &mut user,
        &encode_resp_command(&[b"FCALL", b"deny_incr", b"0"]),
        timeout,
    )
    .await;
    assert!(fcall_denied.contains("NOPERM"));
    assert!(fcall_denied.contains("incr"));

    let function_log = send_and_read_resp_value(
        &mut admin,
        &encode_resp_command(&[b"ACL", b"LOG", b"1"]),
        timeout,
    )
    .await;
    let function_entry = resp_socket_map_or_flat_map(&resp_socket_array(&function_log)[0]);
    assert_eq!(
        resp_socket_bulk(function_entry[&b"reason".to_vec()]),
        b"command"
    );
    assert_eq!(
        resp_socket_bulk(function_entry[&b"context".to_vec()]),
        b"script"
    );
    assert_eq!(
        resp_socket_bulk(function_entry[&b"object".to_vec()]),
        b"incr"
    );
    assert!(
        String::from_utf8_lossy(resp_socket_bulk(function_entry[&b"client-info".to_vec()]))
            .contains("cmd=fcall"),
        "function ACL log should keep the top-level FCALL command in client-info"
    );

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn scripting_function_freezes_key_expiration_during_execution_like_external_scenario() {
    let _serial = lock_scripting_test_serial().await;
    let (addr, shutdown_tx, server) = start_scripting_test_server().await;
    let mut client = TcpStream::connect(addr).await.unwrap();

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEBUG", b"set-disable-deny-scripts", b"1"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[
            b"FUNCTION",
            b"LOAD",
            b"REPLACE",
            b"#!lua name=test\nredis.register_function('test', function(KEYS, ARGV)\n redis.call('SET', 'key', 'value', 'PX', '1'); redis.call('DEBUG', 'SLEEP', 0.01); return redis.call('EXISTS', 'key')\nend)",
        ]),
        b"$4\r\ntest\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"FCALL", b"test", b"1", b"key"]),
        b":1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"EXISTS", b"key"]),
        b":0\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEBUG", b"set-disable-deny-scripts", b"0"]),
        b"+OK\r\n",
    )
    .await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn scripting_function_restore_expired_keys_with_expiration_time_like_external_scenario() {
    let _serial = lock_scripting_test_serial().await;
    let (addr, shutdown_tx, server) = start_scripting_test_server().await;
    let mut client = TcpStream::connect(addr).await.unwrap();

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEBUG", b"set-disable-deny-scripts", b"1"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[
            b"FUNCTION",
            b"LOAD",
            b"REPLACE",
            b"#!lua name=test\nredis.register_function('test', function(KEYS, ARGV)\n redis.call('SET', 'key1{t}', 'value'); local encoded = redis.call('DUMP', 'key1{t}'); redis.call('RESTORE', 'key2{t}', 1, encoded, 'REPLACE'); redis.call('DEBUG', 'SLEEP', 0.01); redis.call('RESTORE', 'key3{t}', 1, encoded, 'REPLACE'); return {redis.call('PEXPIRETIME', 'key2{t}'), redis.call('PEXPIRETIME', 'key3{t}')}\nend)",
        ]),
        b"$4\r\ntest\r\n",
    )
    .await;
    let response = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"FCALL", b"test", b"3", b"key1{t}", b"key2{t}", b"key3{t}"]),
        Duration::from_secs(1),
    )
    .await;
    let values = resp_socket_array(&response);
    assert_eq!(values.len(), 2);
    let first = resp_socket_integer(&values[0]);
    let second = resp_socket_integer(&values[1]);
    assert!(
        first > 0,
        "expected positive PEXPIRETIME values, got {values:?}"
    );
    assert_eq!(first, second);
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEBUG", b"set-disable-deny-scripts", b"0"]),
        b"+OK\r\n",
    )
    .await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scripting_default_server_function_freezes_key_expiration_during_execution_like_external_scenario()
 {
    let _serial = lock_scripting_test_serial().await;
    let (addr, shutdown_tx, server) = start_multishard_scripting_test_server().await;
    let mut client = TcpStream::connect(addr).await.unwrap();

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEBUG", b"set-disable-deny-scripts", b"1"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[
            b"FUNCTION",
            b"LOAD",
            b"REPLACE",
            b"#!lua name=test\nredis.register_function('test', function(KEYS, ARGV)\n redis.call('SET', 'key', 'value', 'PX', '1'); redis.call('DEBUG', 'SLEEP', 0.01); return redis.call('EXISTS', 'key')\nend)",
        ]),
        b"$4\r\ntest\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"FCALL", b"test", b"1", b"key"]),
        b":1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"EXISTS", b"key"]),
        b":0\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEBUG", b"set-disable-deny-scripts", b"0"]),
        b"+OK\r\n",
    )
    .await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scripting_default_server_function_restore_expired_keys_with_expiration_time_like_external_scenario()
 {
    let _serial = lock_scripting_test_serial().await;
    let (addr, shutdown_tx, server) = start_multishard_scripting_test_server().await;
    let mut client = TcpStream::connect(addr).await.unwrap();

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEBUG", b"set-disable-deny-scripts", b"1"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[
            b"FUNCTION",
            b"LOAD",
            b"REPLACE",
            b"#!lua name=test\nredis.register_function('test', function(KEYS, ARGV)\n redis.call('SET', 'key1{t}', 'value'); local encoded = redis.call('DUMP', 'key1{t}'); redis.call('RESTORE', 'key2{t}', 1, encoded, 'REPLACE'); redis.call('DEBUG', 'SLEEP', 0.01); redis.call('RESTORE', 'key3{t}', 1, encoded, 'REPLACE'); return {redis.call('PEXPIRETIME', 'key2{t}'), redis.call('PEXPIRETIME', 'key3{t}')}\nend)",
        ]),
        b"$4\r\ntest\r\n",
    )
    .await;
    let response = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"FCALL", b"test", b"3", b"key1{t}", b"key2{t}", b"key3{t}"]),
        Duration::from_secs(1),
    )
    .await;
    let values = resp_socket_array(&response);
    assert_eq!(values.len(), 2);
    let first = resp_socket_integer(&values[0]);
    let second = resp_socket_integer(&values[1]);
    assert!(
        first > 0,
        "expected positive PEXPIRETIME values, got {values:?}"
    );
    assert_eq!(first, second);
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEBUG", b"set-disable-deny-scripts", b"0"]),
        b"+OK\r\n",
    )
    .await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scripting_function_freezes_expiration_while_active_expire_runs() {
    let _serial = lock_scripting_test_serial().await;
    let (addr, shutdown_tx, server) = start_multishard_scripting_test_server().await;
    let mut client = TcpStream::connect(addr).await.unwrap();

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEBUG", b"set-disable-deny-scripts", b"1"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[
            b"FUNCTION",
            b"LOAD",
            b"REPLACE",
            b"#!lua name=test\nredis.register_function('test', function(KEYS, ARGV)\n redis.call('SET', 'key', 'value', 'PX', '1'); redis.call('DEBUG', 'SLEEP', 0.08); return redis.call('EXISTS', 'key')\nend)",
        ]),
        b"$4\r\ntest\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"FCALL", b"test", b"1", b"key"]),
        b":1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"EXISTS", b"key"]),
        b":0\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEBUG", b"set-disable-deny-scripts", b"0"]),
        b"+OK\r\n",
    )
    .await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scripting_function_restore_keeps_frozen_ttl_under_active_expire() {
    let _serial = lock_scripting_test_serial().await;
    let (addr, shutdown_tx, server) = start_multishard_scripting_test_server().await;
    let mut client = TcpStream::connect(addr).await.unwrap();

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEBUG", b"set-disable-deny-scripts", b"1"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[
            b"FUNCTION",
            b"LOAD",
            b"REPLACE",
            b"#!lua name=test\nredis.register_function('test', function(KEYS, ARGV)\n redis.call('SET', 'key1{t}', 'value'); local encoded = redis.call('DUMP', 'key1{t}'); redis.call('RESTORE', 'key2{t}', 1, encoded, 'REPLACE'); local p2a = redis.call('PEXPIRETIME', 'key2{t}'); redis.call('DEBUG', 'SLEEP', 0.08); local p2b = redis.call('PEXPIRETIME', 'key2{t}'); redis.call('RESTORE', 'key3{t}', 1, encoded, 'REPLACE'); local p3 = redis.call('PEXPIRETIME', 'key3{t}'); return {p2a, p2b, p3}\nend)",
        ]),
        b"$4\r\ntest\r\n",
    )
    .await;
    let response = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"FCALL", b"test", b"3", b"key1{t}", b"key2{t}", b"key3{t}"]),
        Duration::from_secs(1),
    )
    .await;
    let values = resp_socket_array(&response);
    assert_eq!(values.len(), 3);
    let parsed = values.iter().map(resp_socket_integer).collect::<Vec<_>>();
    assert!(
        parsed[0] > 0,
        "expected positive PEXPIRETIME values, got {parsed:?}"
    );
    assert_eq!(
        parsed[0], parsed[1],
        "expected key2 to survive active expire while function runs: {parsed:?}"
    );
    assert_eq!(
        parsed[1], parsed[2],
        "expected restored keys to share the same frozen deadline: {parsed:?}"
    );
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEBUG", b"set-disable-deny-scripts", b"0"]),
        b"+OK\r\n",
    )
    .await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn scripting_freezes_time_for_expiration_related_commands_like_external_scenario() {
    let _serial = lock_scripting_test_serial().await;
    let (addr, shutdown_tx, server) = start_scripting_test_server().await;
    let mut client = TcpStream::connect(addr).await.unwrap();

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEBUG", b"set-disable-deny-scripts", b"1"]),
        b"+OK\r\n",
    )
    .await;
    let response = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[
            b"EVAL",
            b"redis.call('SET', 'key1{t}', 'value', 'EX', 1); redis.call('DEBUG', 'SLEEP', 0.01); redis.call('SET', 'key2{t}', 'value', 'PX', 1000); redis.call('DEBUG', 'SLEEP', 0.01); redis.call('SET', 'key3{t}', 'value'); redis.call('EXPIRE', 'key3{t}', 1); redis.call('DEBUG', 'SLEEP', 0.01); redis.call('SET', 'key4{t}', 'value'); redis.call('PEXPIRE', 'key4{t}', 1000); redis.call('DEBUG', 'SLEEP', 0.01); redis.call('SETEX', 'key5{t}', 1, 'value'); redis.call('DEBUG', 'SLEEP', 0.01); redis.call('PSETEX', 'key6{t}', 1000, 'value'); redis.call('DEBUG', 'SLEEP', 0.01); redis.call('SET', 'key7{t}', 'value'); redis.call('GETEX', 'key7{t}', 'EX', 1); redis.call('DEBUG', 'SLEEP', 0.01); redis.call('SET', 'key8{t}', 'value'); redis.call('GETEX', 'key8{t}', 'PX', 1000); redis.call('DEBUG', 'SLEEP', 0.01); local ttl_results = {redis.call('TTL', 'key1{t}'), redis.call('TTL', 'key2{t}'), redis.call('TTL', 'key3{t}'), redis.call('TTL', 'key4{t}'), redis.call('TTL', 'key5{t}'), redis.call('TTL', 'key6{t}'), redis.call('TTL', 'key7{t}'), redis.call('TTL', 'key8{t}')}; local pttl_results = {redis.call('PTTL', 'key1{t}'), redis.call('PTTL', 'key2{t}'), redis.call('PTTL', 'key3{t}'), redis.call('PTTL', 'key4{t}'), redis.call('PTTL', 'key5{t}'), redis.call('PTTL', 'key6{t}'), redis.call('PTTL', 'key7{t}'), redis.call('PTTL', 'key8{t}')}; local expiretime_results = {redis.call('EXPIRETIME', 'key1{t}'), redis.call('EXPIRETIME', 'key2{t}'), redis.call('EXPIRETIME', 'key3{t}'), redis.call('EXPIRETIME', 'key4{t}'), redis.call('EXPIRETIME', 'key5{t}'), redis.call('EXPIRETIME', 'key6{t}'), redis.call('EXPIRETIME', 'key7{t}'), redis.call('EXPIRETIME', 'key8{t}')}; local pexpiretime_results = {redis.call('PEXPIRETIME', 'key1{t}'), redis.call('PEXPIRETIME', 'key2{t}'), redis.call('PEXPIRETIME', 'key3{t}'), redis.call('PEXPIRETIME', 'key4{t}'), redis.call('PEXPIRETIME', 'key5{t}'), redis.call('PEXPIRETIME', 'key6{t}'), redis.call('PEXPIRETIME', 'key7{t}'), redis.call('PEXPIRETIME', 'key8{t}')}; return {ttl_results, pttl_results, expiretime_results, pexpiretime_results}",
            b"8",
            b"key1{t}",
            b"key2{t}",
            b"key3{t}",
            b"key4{t}",
            b"key5{t}",
            b"key6{t}",
            b"key7{t}",
            b"key8{t}",
        ]),
        Duration::from_secs(2),
    )
    .await;

    let groups = resp_socket_array(&response);
    assert_eq!(groups.len(), 4);
    for group in groups {
        let values: Vec<i64> = resp_socket_array(group)
            .iter()
            .map(resp_socket_integer)
            .collect();
        assert_eq!(values.len(), 8);
        let first = values[0];
        assert!(
            first > 0,
            "expected positive expiration-derived value, got {values:?}"
        );
        assert!(
            values.iter().all(|value| *value == first),
            "expected equal expiration-derived values, got {values:?}"
        );
    }
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEBUG", b"set-disable-deny-scripts", b"0"]),
        b"+OK\r\n",
    )
    .await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn sync_replication_stream_rewrites_delex_as_del_like_redis_external_scenario() {
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

    let mut replica_stream = TcpStream::connect(addr).await.unwrap();
    replica_stream.write_all(b"SYNC\r\n").await.unwrap();
    let header = read_resp_line_with_timeout(&mut replica_stream, Duration::from_secs(1)).await;
    assert!(header.starts_with(b"$"));
    let payload_len = std::str::from_utf8(&header[1..])
        .unwrap()
        .parse::<usize>()
        .unwrap();
    let _payload =
        read_exact_with_timeout(&mut replica_stream, payload_len, Duration::from_secs(1)).await;

    let mut client = TcpStream::connect(addr).await.unwrap();
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"foo", b"bar"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DELEX", b"foo", b"IFEQ", b"bar"]),
        b":1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"foo", b"bar2"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DELEX", b"foo", b"IFEQ", b"baz"]),
        b":0\r\n",
    )
    .await;

    let mut expected = Vec::new();
    expected.extend_from_slice(&encode_resp_command(&[b"SELECT", b"0"]));
    expected.extend_from_slice(&encode_resp_command(&[b"SET", b"foo", b"bar"]));
    expected.extend_from_slice(&encode_resp_command(&[b"DEL", b"foo"]));
    expected.extend_from_slice(&encode_resp_command(&[b"SET", b"foo", b"bar2"]));

    let replicated =
        read_exact_with_timeout(&mut replica_stream, expected.len(), Duration::from_secs(1)).await;
    assert_eq!(replicated, expected);

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn sync_replication_stream_emits_select_on_db_switch() {
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

    let mut admin_client = TcpStream::connect(addr).await.unwrap();
    send_and_expect(
        &mut admin_client,
        &encode_resp_command(&[b"CONFIG", b"SET", b"databases", b"2"]),
        b"+OK\r\n",
    )
    .await;

    let mut replica_stream = TcpStream::connect(addr).await.unwrap();
    replica_stream.write_all(b"SYNC\r\n").await.unwrap();

    let header = read_resp_line_with_timeout(&mut replica_stream, Duration::from_secs(1)).await;
    assert!(
        header.starts_with(b"$"),
        "SYNC response must start with bulk RDB length, got: {}",
        String::from_utf8_lossy(&header)
    );
    let payload_len = std::str::from_utf8(&header[1..])
        .unwrap()
        .parse::<usize>()
        .unwrap();
    let _payload =
        read_exact_with_timeout(&mut replica_stream, payload_len, Duration::from_secs(1)).await;

    let mut client = TcpStream::connect(addr).await.unwrap();
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SELECT", b"1"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"db1:key", b"v1"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SELECT", b"0"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"db0:key", b"v0"]),
        b"+OK\r\n",
    )
    .await;

    let mut expected = Vec::new();
    expected.extend_from_slice(&encode_resp_command(&[b"SELECT", b"1"]));
    expected.extend_from_slice(&encode_resp_command(&[b"SET", b"db1:key", b"v1"]));
    expected.extend_from_slice(&encode_resp_command(&[b"SELECT", b"0"]));
    expected.extend_from_slice(&encode_resp_command(&[b"SET", b"db0:key", b"v0"]));
    let replicated =
        read_exact_with_timeout(&mut replica_stream, expected.len(), Duration::from_secs(1)).await;
    assert_eq!(replicated, expected);

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn sync_replication_stream_swapdb_preserves_expire_then_del_for_expired_blocked_list_key() {
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

    let mut controller = TcpStream::connect(addr).await.unwrap();
    let mut blocked = TcpStream::connect(addr).await.unwrap();
    let mut inspector = TcpStream::connect(addr).await.unwrap();

    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"CONFIG", b"SET", b"databases", b"16"]),
        b"+OK\r\n",
    )
    .await;

    let mut replica_stream = TcpStream::connect(addr).await.unwrap();
    replica_stream.write_all(b"SYNC\r\n").await.unwrap();
    let header = read_resp_line_with_timeout(&mut replica_stream, Duration::from_secs(1)).await;
    let payload_len = std::str::from_utf8(&header[1..])
        .unwrap()
        .parse::<usize>()
        .unwrap();
    let _payload =
        read_exact_with_timeout(&mut replica_stream, payload_len, Duration::from_secs(1)).await;

    // Redis tests/unit/type/list.tcl:
    // "SWAPDB wants to wake blocked client, but the key already expired"
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"FLUSHALL"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"DEBUG", b"SET-ACTIVE-EXPIRE", b"0"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"SELECT", b"1"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"RPUSH", b"k", b"hello"]),
        b":1\r\n",
    )
    .await;
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"PEXPIRE", b"k", b"100"]),
        b":1\r\n",
    )
    .await;

    send_and_expect(
        &mut blocked,
        &encode_resp_command(&[b"SELECT", b"9"]),
        b"+OK\r\n",
    )
    .await;
    let blocked_id = send_and_read_integer(
        &mut blocked,
        &encode_resp_command(&[b"CLIENT", b"ID"]),
        Duration::from_secs(1),
    )
    .await;
    blocked
        .write_all(&encode_resp_command(&[b"BRPOP", b"k", b"1"]))
        .await
        .unwrap();
    wait_for_blocked_clients(&mut inspector, 1, Duration::from_secs(1)).await;

    sleep(Duration::from_millis(101)).await;
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"SWAPDB", b"1", b"9"]),
        b"+OK\r\n",
    )
    .await;

    let blocked_list = send_and_read_bulk_payload(
        &mut inspector,
        &encode_resp_command(&[b"CLIENT", b"LIST", b"ID", blocked_id.to_string().as_bytes()]),
        Duration::from_secs(1),
    )
    .await;
    assert!(
        String::from_utf8_lossy(&blocked_list).contains("flags=b"),
        "blocked client should remain blocked after SWAPDB when the key is already expired: {}",
        String::from_utf8_lossy(&blocked_list)
    );

    let unblock_result = send_and_read_integer(
        &mut controller,
        &encode_resp_command(&[b"CLIENT", b"UNBLOCK", blocked_id.to_string().as_bytes()]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(unblock_result, 1);
    let blocked_timeout = read_exact_with_timeout(&mut blocked, 5, Duration::from_secs(1)).await;
    assert_eq!(blocked_timeout, b"*-1\r\n");

    send_and_expect(
        &mut blocked,
        &encode_resp_command(&[b"SET", b"somekey1", b"someval1"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut blocked,
        &encode_resp_command(&[b"EXISTS", b"k"]),
        b":0\r\n",
    )
    .await;

    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"SET", b"somekey2", b"someval2"]),
        b"+OK\r\n",
    )
    .await;

    assert_eq!(
        read_replication_command_with_timeout(&mut replica_stream, Duration::from_secs(1)).await,
        vec![b"SELECT".to_vec(), b"0".to_vec()]
    );
    assert_eq!(
        read_replication_command_with_timeout(&mut replica_stream, Duration::from_secs(1)).await,
        vec![b"FLUSHALL".to_vec()]
    );
    assert_eq!(
        read_replication_command_with_timeout(&mut replica_stream, Duration::from_secs(1)).await,
        vec![b"SELECT".to_vec(), b"1".to_vec()]
    );
    assert_eq!(
        read_replication_command_with_timeout(&mut replica_stream, Duration::from_secs(1)).await,
        vec![b"RPUSH".to_vec(), b"k".to_vec(), b"hello".to_vec()]
    );
    let expire_command =
        read_replication_command_with_timeout(&mut replica_stream, Duration::from_secs(1)).await;
    assert_eq!(expire_command.len(), 3);
    assert_eq!(expire_command[0], b"PEXPIREAT");
    assert_eq!(expire_command[1], b"k");
    assert!(
        std::str::from_utf8(&expire_command[2])
            .unwrap()
            .parse::<u64>()
            .unwrap()
            > 0
    );
    assert_eq!(
        read_replication_command_with_timeout(&mut replica_stream, Duration::from_secs(1)).await,
        vec![b"SWAPDB".to_vec(), b"1".to_vec(), b"9".to_vec()]
    );
    assert_eq!(
        read_replication_command_with_timeout(&mut replica_stream, Duration::from_secs(1)).await,
        vec![b"SELECT".to_vec(), b"9".to_vec()]
    );
    assert_eq!(
        read_replication_command_with_timeout(&mut replica_stream, Duration::from_secs(1)).await,
        vec![b"SET".to_vec(), b"somekey1".to_vec(), b"someval1".to_vec()]
    );
    assert_eq!(
        read_replication_command_with_timeout(&mut replica_stream, Duration::from_secs(1)).await,
        vec![b"DEL".to_vec(), b"k".to_vec()]
    );
    assert_eq!(
        read_replication_command_with_timeout(&mut replica_stream, Duration::from_secs(1)).await,
        vec![b"SELECT".to_vec(), b"1".to_vec()]
    );
    assert_eq!(
        read_replication_command_with_timeout(&mut replica_stream, Duration::from_secs(1)).await,
        vec![b"SET".to_vec(), b"somekey2".to_vec(), b"someval2".to_vec()]
    );

    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"DEBUG", b"SET-ACTIVE-EXPIRE", b"1"]),
        b"+OK\r\n",
    )
    .await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn sync_replication_stream_exec_preserves_expire_then_del_for_expired_blocked_list_key() {
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

    let mut controller = TcpStream::connect(addr).await.unwrap();
    let mut blocked = TcpStream::connect(addr).await.unwrap();
    let mut inspector = TcpStream::connect(addr).await.unwrap();

    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"CONFIG", b"SET", b"databases", b"16"]),
        b"+OK\r\n",
    )
    .await;

    let mut replica_stream = TcpStream::connect(addr).await.unwrap();
    replica_stream.write_all(b"SYNC\r\n").await.unwrap();
    let header = read_resp_line_with_timeout(&mut replica_stream, Duration::from_secs(1)).await;
    let payload_len = std::str::from_utf8(&header[1..])
        .unwrap()
        .parse::<usize>()
        .unwrap();
    let _payload =
        read_exact_with_timeout(&mut replica_stream, payload_len, Duration::from_secs(1)).await;

    // Redis tests/unit/type/list.tcl:
    // "MULTI + LPUSH + EXPIRE + DEBUG SLEEP on blocked client, key already expired"
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"FLUSHALL"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"DEBUG", b"SET-ACTIVE-EXPIRE", b"0"]),
        b"+OK\r\n",
    )
    .await;

    let blocked_id = send_and_read_integer(
        &mut blocked,
        &encode_resp_command(&[b"CLIENT", b"ID"]),
        Duration::from_secs(1),
    )
    .await;
    blocked
        .write_all(&encode_resp_command(&[b"BRPOP", b"k", b"0"]))
        .await
        .unwrap();
    wait_for_blocked_clients(&mut inspector, 1, Duration::from_secs(1)).await;

    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"MULTI"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"RPUSH", b"k", b"hello"]),
        b"+QUEUED\r\n",
    )
    .await;
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"PEXPIRE", b"k", b"100"]),
        b"+QUEUED\r\n",
    )
    .await;
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"DEBUG", b"SLEEP", b"0.2"]),
        b"+QUEUED\r\n",
    )
    .await;
    let exec_response = send_and_read_resp_value(
        &mut controller,
        &encode_resp_command(&[b"EXEC"]),
        Duration::from_secs(1),
    )
    .await;
    let exec_items = resp_socket_array(&exec_response);
    assert_eq!(exec_items.len(), 3);
    assert_eq!(resp_socket_integer(&exec_items[0]), 1);
    assert_eq!(resp_socket_integer(&exec_items[1]), 1);
    assert_eq!(resp_socket_bulk(&exec_items[2]), b"OK");

    let blocked_list = send_and_read_bulk_payload(
        &mut inspector,
        &encode_resp_command(&[b"CLIENT", b"LIST", b"ID", blocked_id.to_string().as_bytes()]),
        Duration::from_secs(1),
    )
    .await;
    assert!(
        String::from_utf8_lossy(&blocked_list).contains("flags=b"),
        "blocked client should remain blocked after EXEC when the key is already expired: {}",
        String::from_utf8_lossy(&blocked_list)
    );

    let unblock_result = send_and_read_integer(
        &mut controller,
        &encode_resp_command(&[b"CLIENT", b"UNBLOCK", blocked_id.to_string().as_bytes()]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(unblock_result, 1);
    let blocked_timeout = read_exact_with_timeout(&mut blocked, 5, Duration::from_secs(1)).await;
    assert_eq!(blocked_timeout, b"*-1\r\n");
    send_and_expect(
        &mut blocked,
        &encode_resp_command(&[b"EXISTS", b"k"]),
        b":0\r\n",
    )
    .await;

    assert_eq!(
        read_replication_command_with_timeout(&mut replica_stream, Duration::from_secs(1)).await,
        vec![b"SELECT".to_vec(), b"0".to_vec()]
    );
    assert_eq!(
        read_replication_command_with_timeout(&mut replica_stream, Duration::from_secs(1)).await,
        vec![b"FLUSHALL".to_vec()]
    );
    assert_eq!(
        read_replication_command_with_timeout(&mut replica_stream, Duration::from_secs(1)).await,
        vec![b"MULTI".to_vec()]
    );
    assert_eq!(
        read_replication_command_with_timeout(&mut replica_stream, Duration::from_secs(1)).await,
        vec![b"RPUSH".to_vec(), b"k".to_vec(), b"hello".to_vec()]
    );
    let expire_command =
        read_replication_command_with_timeout(&mut replica_stream, Duration::from_secs(1)).await;
    assert_eq!(expire_command.len(), 3);
    assert_eq!(expire_command[0], b"PEXPIREAT");
    assert_eq!(expire_command[1], b"k");
    assert!(
        std::str::from_utf8(&expire_command[2])
            .unwrap()
            .parse::<u64>()
            .unwrap()
            > 0
    );
    assert_eq!(
        read_replication_command_with_timeout(&mut replica_stream, Duration::from_secs(1)).await,
        vec![b"EXEC".to_vec()]
    );
    assert_eq!(
        read_replication_command_with_timeout(&mut replica_stream, Duration::from_secs(1)).await,
        vec![b"DEL".to_vec(), b"k".to_vec()]
    );

    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"DEBUG", b"SET-ACTIVE-EXPIRE", b"1"]),
        b"+OK\r\n",
    )
    .await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn sync_replication_stream_propagates_lazy_expire_del_from_get_without_replicating_get() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let metrics = Arc::new(ServerMetrics::default());
    let processor = Arc::new(RequestProcessor::new().unwrap());
    processor.set_active_expire_enabled(false);
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let server_metrics = Arc::clone(&metrics);
    let server_processor = Arc::clone(&processor);
    let server = tokio::spawn(async move {
        run_listener_with_shutdown_and_cluster_with_processor(
            listener,
            1024,
            server_metrics,
            async move {
                let _ = shutdown_rx.await;
            },
            None,
            server_processor,
        )
        .await
        .unwrap()
    });

    let mut client = TcpStream::connect(addr).await.unwrap();
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"FLUSHALL"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"foo", b"bar", b"PX", b"200"]),
        b"+OK\r\n",
    )
    .await;

    let mut replica_stream = TcpStream::connect(addr).await.unwrap();
    replica_stream.write_all(b"SYNC\r\n").await.unwrap();
    let header = read_resp_line_with_timeout(&mut replica_stream, Duration::from_secs(1)).await;
    assert!(
        header.starts_with(b"$"),
        "SYNC response must start with bulk RDB length, got: {}",
        String::from_utf8_lossy(&header)
    );
    let payload_len = std::str::from_utf8(&header[1..])
        .unwrap()
        .parse::<usize>()
        .unwrap();
    let _payload =
        read_exact_with_timeout(&mut replica_stream, payload_len, Duration::from_secs(1)).await;

    let get_foo = encode_resp_command(&[b"GET", b"foo"]);
    send_and_expect(&mut client, &get_foo, b"$3\r\nbar\r\n").await;

    let mut foo_expired = false;
    let mut nil_observations = 0u8;
    for _ in 0..80 {
        sleep(Duration::from_millis(5)).await;
        client.write_all(&get_foo).await.unwrap();
        let mut response = [0u8; 64];
        let bytes_read =
            tokio::time::timeout(Duration::from_millis(200), client.read(&mut response))
                .await
                .unwrap()
                .unwrap();
        let frame = &response[..bytes_read];
        if frame == b"$-1\r\n" {
            nil_observations = nil_observations.saturating_add(1);
            if nil_observations >= 2 {
                foo_expired = true;
                break;
            }
            continue;
        }
        nil_observations = 0;
        assert_eq!(frame, b"$3\r\nbar\r\n");
    }
    assert!(
        foo_expired,
        "GET foo did not observe expiration within retry budget"
    );

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"x", b"1"]),
        b"+OK\r\n",
    )
    .await;

    let mut expected = Vec::new();
    expected.extend_from_slice(&encode_resp_command(&[b"SELECT", b"0"]));
    expected.extend_from_slice(&encode_resp_command(&[b"DEL", b"foo"]));
    expected.extend_from_slice(&encode_resp_command(&[b"SET", b"x", b"1"]));
    let replicated =
        read_exact_with_timeout(&mut replica_stream, expected.len(), Duration::from_secs(1)).await;
    assert_eq!(replicated, expected);

    let mut trailing = [0u8; 1];
    let trailing_read = tokio::time::timeout(
        Duration::from_millis(200),
        replica_stream.read_exact(&mut trailing),
    )
    .await;
    assert!(
        trailing_read.is_err(),
        "unexpected extra replication frames after lazy-expire GET path: {:?}",
        trailing_read
    );

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn sync_replication_stream_propagates_lazy_expire_del_from_short_ttl_get() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let metrics = Arc::new(ServerMetrics::default());
    let processor = Arc::new(RequestProcessor::new().unwrap());
    processor.set_active_expire_enabled(false);
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let server_metrics = Arc::clone(&metrics);
    let server_processor = Arc::clone(&processor);
    let server = tokio::spawn(async move {
        run_listener_with_shutdown_and_cluster_with_processor(
            listener,
            1024,
            server_metrics,
            async move {
                let _ = shutdown_rx.await;
            },
            None,
            server_processor,
        )
        .await
        .unwrap()
    });

    let mut client = TcpStream::connect(addr).await.unwrap();
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"FLUSHALL"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"foo", b"bar", b"PX", b"1"]),
        b"+OK\r\n",
    )
    .await;

    let mut replica_stream = TcpStream::connect(addr).await.unwrap();
    replica_stream.write_all(b"SYNC\r\n").await.unwrap();
    let header = read_resp_line_with_timeout(&mut replica_stream, Duration::from_secs(1)).await;
    assert!(
        header.starts_with(b"$"),
        "SYNC response must start with bulk RDB length, got: {}",
        String::from_utf8_lossy(&header)
    );
    let payload_len = std::str::from_utf8(&header[1..])
        .unwrap()
        .parse::<usize>()
        .unwrap();
    let _payload =
        read_exact_with_timeout(&mut replica_stream, payload_len, Duration::from_secs(1)).await;

    let get_foo = encode_resp_command(&[b"GET", b"foo"]);
    let mut foo_expired = false;
    for _ in 0..80 {
        sleep(Duration::from_millis(5)).await;
        client.write_all(&get_foo).await.unwrap();
        let mut response = [0u8; 64];
        let bytes_read =
            tokio::time::timeout(Duration::from_millis(200), client.read(&mut response))
                .await
                .unwrap()
                .unwrap();
        let frame = &response[..bytes_read];
        if frame == b"$-1\r\n" {
            foo_expired = true;
            break;
        }
        assert_eq!(frame, b"$3\r\nbar\r\n");
    }
    assert!(
        foo_expired,
        "GET foo did not observe short-TTL expiration within retry budget"
    );

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"x", b"1"]),
        b"+OK\r\n",
    )
    .await;

    let mut expected = Vec::new();
    expected.extend_from_slice(&encode_resp_command(&[b"SELECT", b"0"]));
    expected.extend_from_slice(&encode_resp_command(&[b"DEL", b"foo"]));
    expected.extend_from_slice(&encode_resp_command(&[b"SET", b"x", b"1"]));
    let replicated =
        read_exact_with_timeout(&mut replica_stream, expected.len(), Duration::from_secs(1)).await;
    assert_eq!(replicated, expected);

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn sync_replication_stream_propagates_lazy_expire_del_when_active_expire_is_debug_disabled() {
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
        &encode_resp_command(&[b"DEBUG", b"SET-ACTIVE-EXPIRE", b"0"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"FLUSHALL"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"foo", b"bar", b"PX", b"1"]),
        b"+OK\r\n",
    )
    .await;

    let mut replica_stream = TcpStream::connect(addr).await.unwrap();
    replica_stream.write_all(b"SYNC\r\n").await.unwrap();
    let header = read_resp_line_with_timeout(&mut replica_stream, Duration::from_secs(1)).await;
    assert!(
        header.starts_with(b"$"),
        "SYNC response must start with bulk RDB length, got: {}",
        String::from_utf8_lossy(&header)
    );
    let payload_len = std::str::from_utf8(&header[1..])
        .unwrap()
        .parse::<usize>()
        .unwrap();
    let _payload =
        read_exact_with_timeout(&mut replica_stream, payload_len, Duration::from_secs(1)).await;

    let get_foo = encode_resp_command(&[b"GET", b"foo"]);
    let mut foo_expired = false;
    for _ in 0..50 {
        sleep(Duration::from_millis(20)).await;
        client.write_all(&get_foo).await.unwrap();
        let mut response = [0u8; 64];
        let bytes_read =
            tokio::time::timeout(Duration::from_millis(500), client.read(&mut response))
                .await
                .unwrap()
                .unwrap();
        let frame = &response[..bytes_read];
        if frame == b"$-1\r\n" {
            foo_expired = true;
            break;
        }
        assert_eq!(frame, b"$3\r\nbar\r\n");
    }
    assert!(
        foo_expired,
        "GET foo did not observe expiration with DEBUG SET-ACTIVE-EXPIRE 0"
    );

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"x", b"1"]),
        b"+OK\r\n",
    )
    .await;

    let mut expected = Vec::new();
    expected.extend_from_slice(&encode_resp_command(&[b"SELECT", b"0"]));
    expected.extend_from_slice(&encode_resp_command(&[b"DEL", b"foo"]));
    expected.extend_from_slice(&encode_resp_command(&[b"SET", b"x", b"1"]));
    let replicated =
        read_exact_with_timeout(&mut replica_stream, expected.len(), Duration::from_secs(1)).await;
    assert_eq!(replicated, expected);

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEBUG", b"SET-ACTIVE-EXPIRE", b"1"]),
        b"+OK\r\n",
    )
    .await;
    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn sync_replication_stream_propagates_single_write_multi_exec_without_wrappers() {
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

    let mut replica_stream = TcpStream::connect(addr).await.unwrap();
    replica_stream.write_all(b"SYNC\r\n").await.unwrap();

    let header = read_resp_line_with_timeout(&mut replica_stream, Duration::from_secs(1)).await;
    assert!(
        header.starts_with(b"$"),
        "SYNC response must start with bulk RDB length, got: {}",
        String::from_utf8_lossy(&header)
    );
    let payload_len = std::str::from_utf8(&header[1..])
        .unwrap()
        .parse::<usize>()
        .unwrap();
    let _payload =
        read_exact_with_timeout(&mut replica_stream, payload_len, Duration::from_secs(1)).await;

    let mut client = TcpStream::connect(addr).await.unwrap();
    send_and_expect(&mut client, b"*1\r\n$5\r\nMULTI\r\n", b"+OK\r\n").await;
    send_and_expect(
        &mut client,
        b"*3\r\n$3\r\nSET\r\n$9\r\ntx:single\r\n$2\r\nv1\r\n",
        b"+QUEUED\r\n",
    )
    .await;
    send_and_expect(&mut client, b"*1\r\n$4\r\nEXEC\r\n", b"*1\r\n+OK\r\n").await;

    let mut expected_set = Vec::new();
    expected_set.extend_from_slice(&encode_resp_command(&[b"SELECT", b"0"]));
    expected_set.extend_from_slice(&encode_resp_command(&[b"SET", b"tx:single", b"v1"]));
    let replicated = read_exact_with_timeout(
        &mut replica_stream,
        expected_set.len(),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(replicated, expected_set);

    let mut trailing = [0u8; 1];
    let trailing_read = tokio::time::timeout(
        Duration::from_millis(200),
        replica_stream.read_exact(&mut trailing),
    )
    .await;
    assert!(
        trailing_read.is_err(),
        "unexpected extra replication frames after single-write EXEC: {:?}",
        trailing_read
    );

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn sync_replication_stream_wraps_multi_exec_with_multiple_writes() {
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

    let mut replica_stream = TcpStream::connect(addr).await.unwrap();
    replica_stream.write_all(b"SYNC\r\n").await.unwrap();

    let header = read_resp_line_with_timeout(&mut replica_stream, Duration::from_secs(1)).await;
    assert!(
        header.starts_with(b"$"),
        "SYNC response must start with bulk RDB length, got: {}",
        String::from_utf8_lossy(&header)
    );
    let payload_len = std::str::from_utf8(&header[1..])
        .unwrap()
        .parse::<usize>()
        .unwrap();
    let _payload =
        read_exact_with_timeout(&mut replica_stream, payload_len, Duration::from_secs(1)).await;

    let mut client = TcpStream::connect(addr).await.unwrap();
    send_and_expect(&mut client, b"*1\r\n$5\r\nMULTI\r\n", b"+OK\r\n").await;
    send_and_expect(
        &mut client,
        b"*3\r\n$3\r\nSET\r\n$8\r\ntx:multi\r\n$2\r\nv1\r\n",
        b"+QUEUED\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*2\r\n$3\r\nGET\r\n$8\r\ntx:multi\r\n",
        b"+QUEUED\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*3\r\n$3\r\nSET\r\n$9\r\ntx:multi2\r\n$2\r\nv2\r\n",
        b"+QUEUED\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        b"*1\r\n$4\r\nEXEC\r\n",
        b"*3\r\n+OK\r\n$2\r\nv1\r\n+OK\r\n",
    )
    .await;

    let mut expected = Vec::new();
    expected.extend_from_slice(&encode_resp_command(&[b"MULTI"]));
    expected.extend_from_slice(&encode_resp_command(&[b"SELECT", b"0"]));
    expected.extend_from_slice(&encode_resp_command(&[b"SET", b"tx:multi", b"v1"]));
    expected.extend_from_slice(&encode_resp_command(&[b"SET", b"tx:multi2", b"v2"]));
    expected.extend_from_slice(&encode_resp_command(&[b"EXEC"]));

    let replicated =
        read_exact_with_timeout(&mut replica_stream, expected.len(), Duration::from_secs(1)).await;
    assert_eq!(replicated, expected);

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn sync_replication_stream_multi_exec_with_selects_matches_external_multi_scenario() {
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

    let mut admin = TcpStream::connect(addr).await.unwrap();
    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"CONFIG", b"SET", b"databases", b"16"]),
        b"+OK\r\n",
    )
    .await;

    let mut replica_stream = TcpStream::connect(addr).await.unwrap();
    replica_stream.write_all(b"SYNC\r\n").await.unwrap();

    let header = read_resp_line_with_timeout(&mut replica_stream, Duration::from_secs(1)).await;
    assert!(
        header.starts_with(b"$"),
        "SYNC response must start with bulk RDB length, got: {}",
        String::from_utf8_lossy(&header)
    );
    let payload_len = std::str::from_utf8(&header[1..])
        .unwrap()
        .parse::<usize>()
        .unwrap();
    let _payload =
        read_exact_with_timeout(&mut replica_stream, payload_len, Duration::from_secs(1)).await;

    let mut client = TcpStream::connect(addr).await.unwrap();
    send_and_expect(&mut client, &encode_resp_command(&[b"MULTI"]), b"+OK\r\n").await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SELECT", b"1"]),
        b"+QUEUED\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"foo{t}", b"bar"]),
        b"+QUEUED\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"GET", b"foo{t}"]),
        b"+QUEUED\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SELECT", b"2"]),
        b"+QUEUED\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"foo2{t}", b"bar2"]),
        b"+QUEUED\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"GET", b"foo2{t}"]),
        b"+QUEUED\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SELECT", b"3"]),
        b"+QUEUED\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"foo3{t}", b"bar3"]),
        b"+QUEUED\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"GET", b"foo3{t}"]),
        b"+QUEUED\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"EXEC"]),
        b"*9\r\n+OK\r\n+OK\r\n$3\r\nbar\r\n+OK\r\n+OK\r\n$4\r\nbar2\r\n+OK\r\n+OK\r\n$4\r\nbar3\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"GET", b"foo3{t}"]),
        b"$4\r\nbar3\r\n",
    )
    .await;

    let mut expected = Vec::new();
    expected.extend_from_slice(&encode_resp_command(&[b"MULTI"]));
    expected.extend_from_slice(&encode_resp_command(&[b"SELECT", b"1"]));
    expected.extend_from_slice(&encode_resp_command(&[b"SET", b"foo{t}", b"bar"]));
    expected.extend_from_slice(&encode_resp_command(&[b"SELECT", b"2"]));
    expected.extend_from_slice(&encode_resp_command(&[b"SET", b"foo2{t}", b"bar2"]));
    expected.extend_from_slice(&encode_resp_command(&[b"SELECT", b"3"]));
    expected.extend_from_slice(&encode_resp_command(&[b"SET", b"foo3{t}", b"bar3"]));
    expected.extend_from_slice(&encode_resp_command(&[b"EXEC"]));

    let replicated =
        read_exact_with_timeout(&mut replica_stream, expected.len(), Duration::from_secs(1)).await;
    assert_eq!(replicated, expected);

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
    let expected_clusterdown = format!("-CLUSTERDOWN Hash slot {} is unbound\r\n", unbound_slot);
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
async fn cluster_mode_readonly_and_readwrite_return_ok() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let metrics = Arc::new(ServerMetrics::default());
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let key = b"replica-read-key";
    let slot = redis_hash_slot(key);
    let mut cluster_config = ClusterConfig::new_local("node-1", "127.0.0.1", addr.port())
        .set_local_worker_role(WorkerRole::Replica)
        .unwrap()
        .set_worker_replica_of(LOCAL_WORKER_ID, "node-2")
        .unwrap();
    let (next, remote_id) = cluster_config
        .add_worker(Worker::new("node-2", "10.0.0.2", 6380, WorkerRole::Primary))
        .unwrap();
    cluster_config = next
        .set_slot_state(slot, remote_id, SlotState::Stable)
        .unwrap();
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
    let expected_moved = format!("-MOVED {} 10.0.0.2:6380\r\n", slot);
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"GET", key]),
        expected_moved.as_bytes(),
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"READONLY"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"GET", key]),
        b"$-1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", key, b"value"]),
        expected_moved.as_bytes(),
    )
    .await;
    send_and_expect(&mut client, &encode_resp_command(&[b"MULTI"]), b"+OK\r\n").await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"READWRITE"]),
        b"+QUEUED\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"EXEC"]),
        b"*1\r\n+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"GET", key]),
        expected_moved.as_bytes(),
    )
    .await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn cluster_mode_cluster_snapshot_commands_return_live_topology() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let metrics = Arc::new(ServerMetrics::default());
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let mut cluster_config = ClusterConfig::new_local("node-1", "127.0.0.1", addr.port());
    let (next, node2_id) = cluster_config
        .add_worker(Worker::new("node-2", "10.0.0.2", 6380, WorkerRole::Primary))
        .unwrap();
    cluster_config = next;
    cluster_config = cluster_config
        .set_slot_state(SlotNumber::new(0), LOCAL_WORKER_ID, SlotState::Stable)
        .unwrap();
    cluster_config = cluster_config
        .set_slot_state(SlotNumber::new(1), node2_id, SlotState::Stable)
        .unwrap();
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
    let cluster_info = send_and_read_bulk_payload(
        &mut client,
        &encode_resp_command(&[b"CLUSTER", b"INFO"]),
        Duration::from_secs(1),
    )
    .await;
    let cluster_info_text = String::from_utf8_lossy(&cluster_info);
    assert!(cluster_info_text.contains("cluster_state:fail"));
    assert!(cluster_info_text.contains("cluster_slots_assigned:2"));
    assert!(cluster_info_text.contains("cluster_known_nodes:2"));

    let myid = send_and_read_bulk_payload(
        &mut client,
        &encode_resp_command(&[b"CLUSTER", b"MYID"]),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(myid, b"node-1");

    let cluster_nodes = send_and_read_bulk_payload(
        &mut client,
        &encode_resp_command(&[b"CLUSTER", b"NODES"]),
        Duration::from_secs(1),
    )
    .await;
    let cluster_nodes_text = String::from_utf8_lossy(&cluster_nodes);
    assert!(cluster_nodes_text.contains("node-1 127.0.0.1:"));
    assert!(cluster_nodes_text.contains("myself,master"));
    assert!(cluster_nodes_text.contains("node-2 10.0.0.2:6380@16380"));
    assert!(cluster_nodes_text.contains(" connected 0"));
    assert!(cluster_nodes_text.contains(" connected 1"));

    let mut slots_client = TcpStream::connect(addr).await.unwrap();
    slots_client
        .write_all(&encode_resp_command(&[b"CLUSTER", b"SLOTS"]))
        .await
        .unwrap();
    let slots_header = read_resp_line_with_timeout(&mut slots_client, Duration::from_secs(1)).await;
    assert_eq!(slots_header, b"*2");

    let mut shards_client = TcpStream::connect(addr).await.unwrap();
    shards_client
        .write_all(&encode_resp_command(&[b"CLUSTER", b"SHARDS"]))
        .await
        .unwrap();
    let shards_header =
        read_resp_line_with_timeout(&mut shards_client, Duration::from_secs(1)).await;
    assert_eq!(shards_header, b"*2");

    let mut resp3_client = TcpStream::connect(addr).await.unwrap();
    send_hello_and_drain(&mut resp3_client, b"3").await;
    resp3_client
        .write_all(&encode_resp_command(&[b"CLUSTER", b"INFO"]))
        .await
        .unwrap();
    let resp3_header = read_resp_line_with_timeout(&mut resp3_client, Duration::from_secs(1)).await;
    assert!(resp3_header.starts_with(b"="));
    let resp3_len = std::str::from_utf8(&resp3_header[1..])
        .unwrap()
        .parse::<usize>()
        .unwrap();
    let resp3_payload =
        read_exact_with_timeout(&mut resp3_client, resp3_len + 2, Duration::from_secs(1)).await;
    let resp3_payload_text = String::from_utf8_lossy(&resp3_payload);
    assert!(resp3_payload_text.starts_with("txt:cluster_state:fail"));

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

    let mut replication1 = ReplicationManager::new(
        Some(CheckpointId::new(7)),
        ReplicationOffset::new(1_000),
        ReplicationOffset::new(2_000),
    )
    .unwrap();
    let mut replication2 = ReplicationManager::new(
        Some(CheckpointId::new(7)),
        ReplicationOffset::new(1_000),
        ReplicationOffset::new(2_000),
    )
    .unwrap();
    let mut replication3 = ReplicationManager::new(
        Some(CheckpointId::new(7)),
        ReplicationOffset::new(1_000),
        ReplicationOffset::new(2_000),
    )
    .unwrap();
    replication1.record_replica_offset(node3_id_in_1, ReplicationOffset::new(1_950));
    replication2.record_replica_offset(node3_id_in_2, ReplicationOffset::new(1_950));
    replication3.record_replica_offset(LOCAL_WORKER_ID, ReplicationOffset::new(1_950));
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

    let moved_to_node3_after_failover = format!("-MOVED {} 127.0.0.1:{}\r\n", slot2, addr3.port());
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
    let mut replication1 = ReplicationManager::new(
        Some(CheckpointId::new(7)),
        ReplicationOffset::new(2_000),
        ReplicationOffset::new(2_200),
    )
    .unwrap();
    let mut replication3 = ReplicationManager::new(
        Some(CheckpointId::new(7)),
        ReplicationOffset::new(2_000),
        ReplicationOffset::new(2_200),
    )
    .unwrap();
    replication1.record_replica_offset(node3_id_in_1, ReplicationOffset::new(1_950));
    replication3.record_replica_offset(LOCAL_WORKER_ID, ReplicationOffset::new(1_950));

    let (repl1_tx, mut repl1_rx) = tokio::sync::mpsc::unbounded_channel::<ReplicationEvent>();
    let (repl3_tx, mut repl3_rx) = tokio::sync::mpsc::unbounded_channel::<ReplicationEvent>();
    let mut repl_transport1 =
        ChannelReplicationTransport::new(repl1_tx, ReplicationOffset::new(1_980));
    let mut repl_transport3 =
        ChannelReplicationTransport::new(repl3_tx, ReplicationOffset::new(1_980));
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
    assert!(
        reports1
            .iter()
            .any(|report| report.failed_worker_ids.contains(&node2_id_in_1))
    );
    assert!(
        reports3
            .iter()
            .any(|report| report.failed_worker_ids.contains(&node2_id_in_3))
    );
    assert_eq!(
        repl1_rx.recv().await,
        Some(ReplicationEvent::Checkpoint {
            worker_id: node3_id_in_1,
            checkpoint_id: CheckpointId::new(7),
        })
    );
    assert_eq!(
        repl3_rx.recv().await,
        Some(ReplicationEvent::Checkpoint {
            worker_id: LOCAL_WORKER_ID,
            checkpoint_id: CheckpointId::new(7),
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
async fn run_listener_with_cluster_control_plane_runs_server_and_detected_migration() {
    let listener1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr1 = listener1.local_addr().unwrap();
    let listener2 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr2 = listener2.local_addr().unwrap();

    let key = b"{listener-cp}k".to_vec();
    let slot = redis_hash_slot(&key);
    let get_key = encode_resp_command(&[b"GET", &key]);

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
    let target_processor = Arc::new(RequestProcessor::new().unwrap());
    let (shutdown2_tx, shutdown2_rx) = oneshot::channel::<()>();
    let server2_metrics = Arc::new(ServerMetrics::default());
    let server2_store = Arc::clone(&store2);
    let server2_processor = Arc::clone(&target_processor);
    let server2 = tokio::spawn(async move {
        run_listener_with_shutdown_and_cluster_with_processor(
            listener2,
            1024,
            server2_metrics,
            async move {
                let _ = shutdown2_rx.await;
            },
            Some(server2_store),
            server2_processor,
        )
        .await
        .unwrap();
    });

    let (shutdown1_tx, shutdown1_rx) = oneshot::channel::<()>();
    let server1_metrics = Arc::new(ServerMetrics::default());
    let server1_store = Arc::clone(&store1);
    let server1_target_store = Arc::clone(&store2);
    let server1_target_processor = Arc::clone(&target_processor);
    let server1 = tokio::spawn(async move {
        let gossip_engine = AsyncGossipEngine::new(
            GossipCoordinator::new(Vec::new(), 1),
            InMemoryGossipTransport::new(Arc::clone(&server1_store)),
            100,
            0,
        );
        let mut manager = ClusterManager::new(gossip_engine, Duration::from_millis(2));
        let mut failure_detector = FailureDetector::new(1_000);
        let mut failover_controller = ClusterFailoverController::new();
        let mut replication_manager = ReplicationManager::new(
            Some(CheckpointId::new(7)),
            ReplicationOffset::new(2_000),
            ReplicationOffset::new(2_200),
        )
        .unwrap();
        let (repl_tx, mut repl_rx) = tokio::sync::mpsc::unbounded_channel::<ReplicationEvent>();
        let mut replication_transport =
            ChannelReplicationTransport::new(repl_tx, ReplicationOffset::new(1_980));
        let (_updates_tx, updates_rx) = tokio::sync::mpsc::unbounded_channel::<ClusterConfig>();
        let report = run_listener_with_cluster_control_plane(
            listener1,
            1024,
            server1_metrics,
            Arc::clone(&server1_store),
            &mut manager,
            server1_target_store.as_ref(),
            server1_target_processor.as_ref(),
            updates_rx,
            &mut failure_detector,
            &mut failover_controller,
            &mut replication_manager,
            &mut replication_transport,
            1,
            Duration::from_millis(1),
            Duration::from_millis(1),
            async move {
                let _ = shutdown1_rx.await;
            },
        )
        .await
        .unwrap();
        assert!(report.failover_report.failover_records.is_empty());
        assert!(!report.failover_report.gossip_reports.is_empty());
        assert!(!report.migration_reports.is_empty());
        assert!(repl_rx.try_recv().is_err());
        report
    });

    let mut node1 = TcpStream::connect(addr1).await.unwrap();
    let mut node2 = TcpStream::connect(addr2).await.unwrap();
    let set_key = encode_resp_command(&[b"SET", &key, b"value"]);
    send_and_expect(&mut node1, &set_key, b"+OK\r\n").await;
    send_and_expect(&mut node1, &get_key, b"$5\r\nvalue\r\n").await;

    let moved_to_node1 = format!("-MOVED {} 127.0.0.1:{}\r\n", slot, addr1.port());
    send_and_expect(&mut node2, &get_key, moved_to_node1.as_bytes()).await;

    let source_migrating = store1
        .load()
        .as_ref()
        .clone()
        .begin_slot_migration_to(slot, node2_id_in_1)
        .unwrap();
    store1.publish(source_migrating);
    let target_importing = store2
        .load()
        .as_ref()
        .clone()
        .begin_slot_import_from(slot, node1_id_in_2)
        .unwrap();
    store2.publish(target_importing);

    wait_until(
        || {
            store1.load().slot_state(slot).unwrap() == SlotState::Stable
                && store1.load().slot_assigned_owner(slot).unwrap() == node2_id_in_1
                && execute_processor_frame(target_processor.as_ref(), &get_key)
                    == b"$5\r\nvalue\r\n"
        },
        Duration::from_secs(1),
    )
    .await;

    let moved_to_node2 = format!("-MOVED {} 127.0.0.1:{}\r\n", slot, addr2.port());
    send_and_expect(&mut node1, &get_key, moved_to_node2.as_bytes()).await;
    send_and_expect(&mut node2, &get_key, b"$5\r\nvalue\r\n").await;

    let _ = shutdown1_tx.send(());
    let _ = shutdown2_tx.send(());
    let report = server1.await.unwrap();
    assert_eq!(report.migration_reports.len(), 1);
    server2.await.unwrap();
}

#[tokio::test]
async fn run_listener_with_cluster_control_plane_propagates_control_plane_errors() {
    let listener1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr1 = listener1.local_addr().unwrap();

    let source_config = ClusterConfig::new_local("node-1", "127.0.0.1", addr1.port());
    let target_config = ClusterConfig::new_local("node-2", "127.0.0.1", 8752);
    let source_store = Arc::new(ClusterConfigStore::new(source_config));
    let target_store = ClusterConfigStore::new(target_config);
    let target_processor = RequestProcessor::new().unwrap();

    let gossip_engine = AsyncGossipEngine::new(
        GossipCoordinator::new(Vec::new(), 1),
        InMemoryGossipTransport::new(Arc::clone(&source_store)),
        100,
        0,
    );
    let mut manager = ClusterManager::new(gossip_engine, Duration::from_millis(2));
    let mut failure_detector = FailureDetector::new(1_000);
    let mut failover_controller = ClusterFailoverController::new();
    let mut replication_manager = ReplicationManager::new(
        Some(CheckpointId::new(7)),
        ReplicationOffset::new(2_000),
        ReplicationOffset::new(2_200),
    )
    .unwrap();
    let (repl_tx, _repl_rx) = tokio::sync::mpsc::unbounded_channel::<ReplicationEvent>();
    let mut replication_transport =
        ChannelReplicationTransport::new(repl_tx, ReplicationOffset::new(1_980));
    let (_updates_tx, updates_rx) = tokio::sync::mpsc::unbounded_channel::<ClusterConfig>();

    let result = run_listener_with_cluster_control_plane(
        listener1,
        1024,
        Arc::new(ServerMetrics::default()),
        Arc::clone(&source_store),
        &mut manager,
        &target_store,
        &target_processor,
        updates_rx,
        &mut failure_detector,
        &mut failover_controller,
        &mut replication_manager,
        &mut replication_transport,
        1,
        Duration::from_millis(1),
        Duration::from_millis(1),
        tokio::time::sleep(Duration::from_secs(1)),
    )
    .await;
    assert!(matches!(
        result,
        Err(ClusteredServerRunError::ControlPlane(
            ClusterManagerFailoverMigrationError::Migration(
                LiveSlotMigrationError::MissingSourcePeerNode(node_id)
            )
        )) if node_id == "node-2"
    ));
}

#[tokio::test]
async fn run_with_cluster_control_plane_binds_and_serves_requests() {
    let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr1 = probe.local_addr().unwrap();
    drop(probe);

    let mut config1 = ClusterConfig::new_local("node-1", "127.0.0.1", addr1.port());
    let (config1_next, _node2_id_in_1) = config1
        .add_worker(Worker::new(
            "node-2",
            "127.0.0.1",
            8702,
            WorkerRole::Primary,
        ))
        .unwrap();
    config1 = config1_next;

    let mut config2 = ClusterConfig::new_local("node-2", "127.0.0.1", 8702);
    let (config2_next, _node1_id_in_2) = config2
        .add_worker(Worker::new(
            "node-1",
            "127.0.0.1",
            addr1.port(),
            WorkerRole::Primary,
        ))
        .unwrap();
    config2 = config2_next;

    let store1 = Arc::new(ClusterConfigStore::new(config1));
    let store2 = Arc::new(ClusterConfigStore::new(config2));
    let target_processor = RequestProcessor::new().unwrap();
    let metrics = Arc::new(ServerMetrics::default());
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let client_addr = addr1;
    let client = tokio::spawn(async move {
        for _ in 0..50 {
            if let Ok(mut stream) = TcpStream::connect(client_addr).await {
                send_and_expect(&mut stream, b"*1\r\n$4\r\nPING\r\n", b"+PONG\r\n").await;
                let _ = shutdown_tx.send(());
                return;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        panic!("failed to connect to clustered server test listener");
    });

    let gossip_engine = AsyncGossipEngine::new(
        GossipCoordinator::new(Vec::new(), 1),
        InMemoryGossipTransport::new(Arc::clone(&store1)),
        100,
        0,
    );
    let mut manager = ClusterManager::new(gossip_engine, Duration::from_millis(2));
    let mut failure_detector = FailureDetector::new(1_000);
    let mut failover_controller = ClusterFailoverController::new();
    let mut replication_manager = ReplicationManager::new(
        Some(CheckpointId::new(7)),
        ReplicationOffset::new(2_000),
        ReplicationOffset::new(2_200),
    )
    .unwrap();
    let (repl_tx, mut repl_rx) = tokio::sync::mpsc::unbounded_channel::<ReplicationEvent>();
    let mut replication_transport =
        ChannelReplicationTransport::new(repl_tx, ReplicationOffset::new(1_980));
    let (_updates_tx, updates_rx) = tokio::sync::mpsc::unbounded_channel::<ClusterConfig>();

    let report = run_with_cluster_control_plane(
        ServerConfig {
            bind_addr: addr1,
            read_buffer_size: 1024,
            startup_config_overrides: Default::default(),
        },
        metrics,
        Arc::clone(&store1),
        &mut manager,
        store2.as_ref(),
        &target_processor,
        updates_rx,
        &mut failure_detector,
        &mut failover_controller,
        &mut replication_manager,
        &mut replication_transport,
        1,
        Duration::from_millis(1),
        Duration::from_millis(1),
        async move {
            let _ = shutdown_rx.await;
        },
    )
    .await
    .unwrap();
    client.await.unwrap();

    assert!(!report.failover_report.gossip_reports.is_empty());
    assert!(report.failover_report.failover_records.is_empty());
    assert!(report.migration_reports.is_empty());
    assert!(repl_rx.try_recv().is_err());
}

#[tokio::test]
async fn run_with_cluster_control_plane_propagates_bind_errors() {
    let occupied = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = occupied.local_addr().unwrap();

    let source_store = Arc::new(ClusterConfigStore::new(ClusterConfig::new_local(
        "node-1",
        "127.0.0.1",
        addr.port(),
    )));
    let target_store =
        ClusterConfigStore::new(ClusterConfig::new_local("node-2", "127.0.0.1", 8802));
    let target_processor = RequestProcessor::new().unwrap();
    let gossip_engine = AsyncGossipEngine::new(
        GossipCoordinator::new(Vec::new(), 1),
        InMemoryGossipTransport::new(Arc::clone(&source_store)),
        100,
        0,
    );
    let mut manager = ClusterManager::new(gossip_engine, Duration::from_millis(2));
    let mut failure_detector = FailureDetector::new(1_000);
    let mut failover_controller = ClusterFailoverController::new();
    let mut replication_manager = ReplicationManager::new(
        Some(CheckpointId::new(7)),
        ReplicationOffset::new(2_000),
        ReplicationOffset::new(2_200),
    )
    .unwrap();
    let (repl_tx, _repl_rx) = tokio::sync::mpsc::unbounded_channel::<ReplicationEvent>();
    let mut replication_transport =
        ChannelReplicationTransport::new(repl_tx, ReplicationOffset::new(1_980));
    let (_updates_tx, updates_rx) = tokio::sync::mpsc::unbounded_channel::<ClusterConfig>();

    let result = run_with_cluster_control_plane(
        ServerConfig {
            bind_addr: addr,
            read_buffer_size: 1024,
            startup_config_overrides: Default::default(),
        },
        Arc::new(ServerMetrics::default()),
        source_store,
        &mut manager,
        &target_store,
        &target_processor,
        updates_rx,
        &mut failure_detector,
        &mut failover_controller,
        &mut replication_manager,
        &mut replication_transport,
        1,
        Duration::from_millis(1),
        Duration::from_millis(1),
        tokio::time::sleep(Duration::from_millis(10)),
    )
    .await;
    assert!(matches!(result, Err(ClusteredServerRunError::Io(_))));
}

#[tokio::test]
async fn cluster_manager_failover_and_detected_migration_runner_executes_migration_loop() {
    let key = b"{manager-detected}k".to_vec();
    let slot = redis_hash_slot(&key);

    let mut config1 = ClusterConfig::new_local("node-1", "127.0.0.1", 8401);
    let (next1, node2_id_in_1) = config1
        .add_worker(Worker::new(
            "node-2",
            "127.0.0.1",
            8402,
            WorkerRole::Primary,
        ))
        .unwrap();
    config1 = next1
        .set_slot_state(slot, LOCAL_WORKER_ID, SlotState::Stable)
        .unwrap();

    let mut config2 = ClusterConfig::new_local("node-2", "127.0.0.1", 8402);
    let (next2, node1_id_in_2) = config2
        .add_worker(Worker::new(
            "node-1",
            "127.0.0.1",
            8401,
            WorkerRole::Primary,
        ))
        .unwrap();
    config2 = next2
        .set_slot_state(slot, node1_id_in_2, SlotState::Stable)
        .unwrap();

    let store1 = Arc::new(ClusterConfigStore::new(config1));
    let store2 = Arc::new(ClusterConfigStore::new(config2));
    let source_processor = RequestProcessor::new().unwrap();
    let target_processor = RequestProcessor::new().unwrap();

    assert_eq!(
        execute_processor_frame(
            &source_processor,
            &encode_resp_command(&[b"SET", &key, b"value"])
        ),
        b"+OK\r\n"
    );
    let source_migrating = store1
        .load()
        .as_ref()
        .clone()
        .begin_slot_migration_to(slot, node2_id_in_1)
        .unwrap();
    store1.publish(source_migrating);
    let target_importing = store2
        .load()
        .as_ref()
        .clone()
        .begin_slot_import_from(slot, node1_id_in_2)
        .unwrap();
    store2.publish(target_importing);

    let gossip_engine = AsyncGossipEngine::new(
        GossipCoordinator::new(Vec::new(), 1),
        InMemoryGossipTransport::new(Arc::clone(&store1)),
        100,
        0,
    );
    let mut manager = ClusterManager::new(gossip_engine, Duration::from_millis(2));
    let mut failure_detector = FailureDetector::new(1_000);
    let mut failover_controller = ClusterFailoverController::new();
    let mut replication_manager = ReplicationManager::new(
        Some(CheckpointId::new(7)),
        ReplicationOffset::new(2_000),
        ReplicationOffset::new(2_200),
    )
    .unwrap();
    let (repl_tx, mut repl_rx) = tokio::sync::mpsc::unbounded_channel::<ReplicationEvent>();
    let mut replication_transport =
        ChannelReplicationTransport::new(repl_tx, ReplicationOffset::new(1_980));
    let (_updates_tx, updates_rx) = tokio::sync::mpsc::unbounded_channel::<ClusterConfig>();

    let report = run_cluster_manager_with_config_updates_failover_and_detected_migrations(
        &mut manager,
        &store1,
        &store2,
        &source_processor,
        &target_processor,
        updates_rx,
        &mut failure_detector,
        &mut failover_controller,
        &mut replication_manager,
        &mut replication_transport,
        1,
        Duration::from_millis(1),
        Duration::from_millis(1),
        tokio::time::sleep(Duration::from_millis(20)),
    )
    .await
    .unwrap();
    assert!(!report.failover_report.gossip_reports.is_empty());
    assert!(report.failover_report.failover_records.is_empty());
    assert!(repl_rx.try_recv().is_err());
    assert_eq!(
        report.migration_reports,
        vec![LiveSlotMigrationsRunReport {
            slots: vec![LiveSlotMigrationSlotReport {
                slot,
                batches: 1,
                moved_keys: 1,
                finalized: true,
            }],
            interrupted: false,
        }]
    );

    let get_key = encode_resp_command(&[b"GET", &key]);
    assert_eq!(
        execute_processor_frame(&source_processor, &get_key),
        b"$-1\r\n"
    );
    assert_eq!(
        execute_processor_frame(&target_processor, &get_key),
        b"$5\r\nvalue\r\n"
    );
    assert_eq!(
        store1.load().slot_assigned_owner(slot).unwrap(),
        node2_id_in_1
    );
    assert_eq!(
        store2.load().slot_assigned_owner(slot).unwrap(),
        LOCAL_WORKER_ID
    );
}

#[tokio::test]
async fn cluster_manager_failover_and_detected_migration_runner_propagates_migration_errors() {
    let source_store = Arc::new(ClusterConfigStore::new(ClusterConfig::new_local(
        "node-1",
        "127.0.0.1",
        8501,
    )));
    let target_config = ClusterConfig::new_local("node-2", "127.0.0.1", 8502);
    let (target_config, _node1_id_in_2) = target_config
        .add_worker(Worker::new(
            "node-1",
            "127.0.0.1",
            8501,
            WorkerRole::Primary,
        ))
        .unwrap();
    let target_store = Arc::new(ClusterConfigStore::new(target_config));
    let source_processor = RequestProcessor::new().unwrap();
    let target_processor = RequestProcessor::new().unwrap();

    let gossip_engine = AsyncGossipEngine::new(
        GossipCoordinator::new(Vec::new(), 1),
        InMemoryGossipTransport::new(Arc::clone(&source_store)),
        100,
        0,
    );
    let mut manager = ClusterManager::new(gossip_engine, Duration::from_millis(2));
    let mut failure_detector = FailureDetector::new(1_000);
    let mut failover_controller = ClusterFailoverController::new();
    let mut replication_manager = ReplicationManager::new(
        Some(CheckpointId::new(7)),
        ReplicationOffset::new(2_000),
        ReplicationOffset::new(2_200),
    )
    .unwrap();
    let (repl_tx, _repl_rx) = tokio::sync::mpsc::unbounded_channel::<ReplicationEvent>();
    let mut replication_transport =
        ChannelReplicationTransport::new(repl_tx, ReplicationOffset::new(1_980));
    let (_updates_tx, updates_rx) = tokio::sync::mpsc::unbounded_channel::<ClusterConfig>();

    let result = run_cluster_manager_with_config_updates_failover_and_detected_migrations(
        &mut manager,
        &source_store,
        &target_store,
        &source_processor,
        &target_processor,
        updates_rx,
        &mut failure_detector,
        &mut failover_controller,
        &mut replication_manager,
        &mut replication_transport,
        1,
        Duration::from_millis(1),
        Duration::from_millis(1),
        tokio::time::sleep(Duration::from_millis(50)),
    )
    .await;
    assert!(matches!(
        result,
        Err(ClusterManagerFailoverMigrationError::Migration(
            LiveSlotMigrationError::MissingSourcePeerNode(node_id)
        )) if node_id == "node-2"
    ));
}

#[tokio::test]
async fn cluster_manager_failover_and_detected_migration_runner_propagates_failover_errors() {
    let slot = SlotNumber::new(860);
    let mut source_config = ClusterConfig::new_local("node-1", "127.0.0.1", 8601)
        .set_local_worker_role(WorkerRole::Replica)
        .unwrap()
        .set_worker_replica_of(LOCAL_WORKER_ID, "node-2")
        .unwrap();
    let (source_config_next, node2_id_in_1) = source_config
        .add_worker(Worker::new(
            "node-2",
            "127.0.0.1",
            8602,
            WorkerRole::Primary,
        ))
        .unwrap();
    source_config = source_config_next
        .set_slot_state(slot, node2_id_in_1, SlotState::Stable)
        .unwrap();

    let mut target_config = ClusterConfig::new_local("node-2", "127.0.0.1", 8602);
    let (target_config_next, node1_id_in_2) = target_config
        .add_worker(Worker::new(
            "node-1",
            "127.0.0.1",
            8601,
            WorkerRole::Replica,
        ))
        .unwrap();
    target_config = target_config_next
        .set_slot_state(slot, node1_id_in_2, SlotState::Stable)
        .unwrap();

    let source_store = Arc::new(ClusterConfigStore::new(source_config));
    let target_store = Arc::new(ClusterConfigStore::new(target_config));
    let source_processor = RequestProcessor::new().unwrap();
    let target_processor = RequestProcessor::new().unwrap();

    let gossip_engine = AsyncGossipEngine::new(
        GossipCoordinator::new(vec![GossipNode::new(node2_id_in_1, 0)], 1),
        InMemoryGossipTransport::new(Arc::clone(&source_store)),
        100,
        0,
    );
    let mut manager = ClusterManager::new(gossip_engine, Duration::from_millis(2));
    let mut failure_detector = FailureDetector::new(1);
    let mut failover_controller = ClusterFailoverController::new();
    let mut replication_manager = ReplicationManager::new(
        Some(CheckpointId::new(7)),
        ReplicationOffset::new(2_000),
        ReplicationOffset::new(2_200),
    )
    .unwrap();
    let (repl_tx, repl_rx) = tokio::sync::mpsc::unbounded_channel::<ReplicationEvent>();
    drop(repl_rx);
    let mut replication_transport =
        ChannelReplicationTransport::new(repl_tx, ReplicationOffset::new(1_980));
    let (_updates_tx, updates_rx) = tokio::sync::mpsc::unbounded_channel::<ClusterConfig>();

    let result = run_cluster_manager_with_config_updates_failover_and_detected_migrations(
        &mut manager,
        &source_store,
        &target_store,
        &source_processor,
        &target_processor,
        updates_rx,
        &mut failure_detector,
        &mut failover_controller,
        &mut replication_manager,
        &mut replication_transport,
        1,
        Duration::from_millis(1),
        Duration::from_millis(1),
        tokio::time::sleep(Duration::from_millis(50)),
    )
    .await;
    assert!(matches!(
        result,
        Err(ClusterManagerFailoverMigrationError::Failover(
            garnet_cluster::FailoverControllerError::Replication(
                garnet_cluster::ReplicationSyncError::Transport(
                    garnet_cluster::ChannelReplicationTransportError::ChannelClosed
                )
            )
        ))
    ));
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
        .begin_slot_migration_to(slot, node2_id_in_1)
        .unwrap();
    store1.publish(migration_source);
    let migration_target = store2
        .load()
        .as_ref()
        .clone()
        .begin_slot_import_from(slot, node1_id_in_2)
        .unwrap();
    store2.publish(migration_target);

    let ask_to_node2 = format!("-ASK {} 127.0.0.1:{}\r\n", slot, addr2.port());
    send_and_expect(&mut node1, &get_key, ask_to_node2.as_bytes()).await;
    let ask_to_node1 = format!("-ASK {} 127.0.0.1:{}\r\n", slot, addr1.port());
    send_and_expect(&mut node2, &get_key, ask_to_node1.as_bytes()).await;

    send_and_expect(&mut node2, b"*1\r\n$6\r\nASKING\r\n", b"+OK\r\n").await;
    send_and_expect(&mut node2, &get_key, b"$-1\r\n").await;

    let moved = source_processor
        .migrate_slot_to(&target_processor, DbName::fixture(), slot, 16, true)
        .unwrap();
    assert_eq!(moved, 1);

    send_and_expect(&mut node2, b"*1\r\n$6\r\nASKING\r\n", b"+OK\r\n").await;
    send_and_expect(&mut node2, &get_key, b"$5\r\nvalue\r\n").await;

    let finalized_source = store1
        .load()
        .as_ref()
        .clone()
        .finalize_slot_migration(slot, node2_id_in_1)
        .unwrap();
    store1.publish(finalized_source);
    let finalized_target = store2
        .load()
        .as_ref()
        .clone()
        .finalize_slot_migration(slot, LOCAL_WORKER_ID)
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

#[tokio::test]
async fn detected_live_slot_migration_runner_updates_server_redirections() {
    let listener1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr1 = listener1.local_addr().unwrap();
    let listener2 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr2 = listener2.local_addr().unwrap();

    let key = b"detected-live-migrate-key".to_vec();
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
        .begin_slot_migration_to(slot, node2_id_in_1)
        .unwrap();
    store1.publish(migration_source);
    let migration_target = store2
        .load()
        .as_ref()
        .clone()
        .begin_slot_import_from(slot, node1_id_in_2)
        .unwrap();
    store2.publish(migration_target);

    let ask_to_node2 = format!("-ASK {} 127.0.0.1:{}\r\n", slot, addr2.port());
    send_and_expect(&mut node1, &get_key, ask_to_node2.as_bytes()).await;

    let report = run_detected_live_slot_migrations_until_complete(
        &store1,
        &store2,
        &source_processor,
        &target_processor,
        1,
        Duration::from_millis(1),
        std::future::pending::<()>(),
    )
    .await
    .unwrap();
    assert!(!report.interrupted);
    assert_eq!(
        report.slots,
        vec![LiveSlotMigrationSlotReport {
            slot,
            batches: 1,
            moved_keys: 1,
            finalized: true,
        }]
    );

    let moved_to_node2 = format!("-MOVED {} 127.0.0.1:{}\r\n", slot, addr2.port());
    send_and_expect(&mut node1, &get_key, moved_to_node2.as_bytes()).await;
    send_and_expect(&mut node2, &get_key, b"$5\r\nvalue\r\n").await;

    let _ = shutdown1_tx.send(());
    let _ = shutdown2_tx.send(());
    server1.await.unwrap();
    server2.await.unwrap();
}

#[tokio::test]
async fn execute_live_slot_migration_orchestrates_transfer_and_finalization() {
    let listener1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr1 = listener1.local_addr().unwrap();
    let listener2 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr2 = listener2.local_addr().unwrap();

    let key = b"orchestrated-live-migrate-key".to_vec();
    let slot = redis_hash_slot(&key);

    let mut config1 = ClusterConfig::new_local("node-1", "127.0.0.1", addr1.port());
    let (next1, _node2_id_in_1) = config1
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
    let set_key = encode_resp_command(&[b"SET", &key, b"v-orchestrated"]);
    let get_key = encode_resp_command(&[b"GET", &key]);

    send_and_expect(&mut node1, &set_key, b"+OK\r\n").await;
    send_and_expect(&mut node1, &get_key, b"$14\r\nv-orchestrated\r\n").await;

    let moved_to_node1 = format!("-MOVED {} 127.0.0.1:{}\r\n", slot, addr1.port());
    send_and_expect(&mut node2, &get_key, moved_to_node1.as_bytes()).await;

    let moved = execute_live_slot_migration(
        &store1,
        &store2,
        &source_processor,
        &target_processor,
        slot,
        16,
    )
    .unwrap();
    assert_eq!(moved, 1);

    let moved_to_node2 = format!("-MOVED {} 127.0.0.1:{}\r\n", slot, addr2.port());
    send_and_expect(&mut node1, &get_key, moved_to_node2.as_bytes()).await;
    send_and_expect(&mut node2, &get_key, b"$14\r\nv-orchestrated\r\n").await;

    let _ = shutdown1_tx.send(());
    let _ = shutdown2_tx.send(());
    server1.await.unwrap();
    server2.await.unwrap();
}

#[test]
fn execute_live_slot_migration_batches_until_slot_exhausted() {
    let key1 = b"{batch-slot}one".to_vec();
    let key2 = b"{batch-slot}two".to_vec();
    let key3 = b"{batch-slot}three".to_vec();
    let slot = redis_hash_slot(&key1);
    assert_eq!(slot, redis_hash_slot(&key2));
    assert_eq!(slot, redis_hash_slot(&key3));

    let mut config1 = ClusterConfig::new_local("node-1", "127.0.0.1", 7001);
    let (next1, node2_id_in_1) = config1
        .add_worker(Worker::new(
            "node-2",
            "127.0.0.1",
            7002,
            WorkerRole::Primary,
        ))
        .unwrap();
    config1 = next1
        .set_slot_state(slot, LOCAL_WORKER_ID, SlotState::Stable)
        .unwrap();

    let mut config2 = ClusterConfig::new_local("node-2", "127.0.0.1", 7002);
    let (next2, node1_id_in_2) = config2
        .add_worker(Worker::new(
            "node-1",
            "127.0.0.1",
            7001,
            WorkerRole::Primary,
        ))
        .unwrap();
    config2 = next2
        .set_slot_state(slot, node1_id_in_2, SlotState::Stable)
        .unwrap();

    let store1 = ClusterConfigStore::new(config1);
    let store2 = ClusterConfigStore::new(config2);
    let source = RequestProcessor::new().unwrap();
    let target = RequestProcessor::new().unwrap();

    let set1 = encode_resp_command(&[b"SET", &key1, b"v1"]);
    assert_eq!(execute_processor_frame(&source, &set1), b"+OK\r\n");
    let set2 = encode_resp_command(&[b"SET", &key2, b"v2"]);
    assert_eq!(execute_processor_frame(&source, &set2), b"+OK\r\n");
    let set3 = encode_resp_command(&[b"SET", &key3, b"v3"]);
    assert_eq!(execute_processor_frame(&source, &set3), b"+OK\r\n");

    let moved = execute_live_slot_migration(&store1, &store2, &source, &target, slot, 1)
        .expect("batched migration should complete");
    assert_eq!(moved, 3);

    let get1 = encode_resp_command(&[b"GET", &key1]);
    let get2 = encode_resp_command(&[b"GET", &key2]);
    let get3 = encode_resp_command(&[b"GET", &key3]);
    assert_eq!(execute_processor_frame(&source, &get1), b"$-1\r\n");
    assert_eq!(execute_processor_frame(&source, &get2), b"$-1\r\n");
    assert_eq!(execute_processor_frame(&source, &get3), b"$-1\r\n");
    assert_eq!(execute_processor_frame(&target, &get1), b"$2\r\nv1\r\n");
    assert_eq!(execute_processor_frame(&target, &get2), b"$2\r\nv2\r\n");
    assert_eq!(execute_processor_frame(&target, &get3), b"$2\r\nv3\r\n");

    assert_eq!(
        store1.load().slot_assigned_owner(slot).unwrap(),
        node2_id_in_1
    );
    assert_eq!(
        store2.load().slot_assigned_owner(slot).unwrap(),
        LOCAL_WORKER_ID
    );
}

#[test]
fn execute_live_slot_migration_step_progresses_then_finalizes() {
    let key1 = b"{step-slot}one".to_vec();
    let key2 = b"{step-slot}two".to_vec();
    let slot = redis_hash_slot(&key1);
    assert_eq!(slot, redis_hash_slot(&key2));

    let mut config1 = ClusterConfig::new_local("node-1", "127.0.0.1", 7101);
    let (next1, node2_id_in_1) = config1
        .add_worker(Worker::new(
            "node-2",
            "127.0.0.1",
            7102,
            WorkerRole::Primary,
        ))
        .unwrap();
    config1 = next1
        .set_slot_state(slot, LOCAL_WORKER_ID, SlotState::Stable)
        .unwrap();

    let mut config2 = ClusterConfig::new_local("node-2", "127.0.0.1", 7102);
    let (next2, node1_id_in_2) = config2
        .add_worker(Worker::new(
            "node-1",
            "127.0.0.1",
            7101,
            WorkerRole::Primary,
        ))
        .unwrap();
    config2 = next2
        .set_slot_state(slot, node1_id_in_2, SlotState::Stable)
        .unwrap();

    let store1 = ClusterConfigStore::new(config1);
    let store2 = ClusterConfigStore::new(config2);
    let source = RequestProcessor::new().unwrap();
    let target = RequestProcessor::new().unwrap();

    let set1 = encode_resp_command(&[b"SET", &key1, b"v1"]);
    assert_eq!(execute_processor_frame(&source, &set1), b"+OK\r\n");
    let set2 = encode_resp_command(&[b"SET", &key2, b"v2"]);
    assert_eq!(execute_processor_frame(&source, &set2), b"+OK\r\n");

    let step1 =
        execute_live_slot_migration_step(&store1, &store2, &source, &target, slot, 1).unwrap();
    assert_eq!(
        step1,
        LiveSlotMigrationStepOutcome {
            moved_keys: 1,
            finalized: false,
        }
    );
    assert_eq!(
        store1.load().slot_state(slot).unwrap(),
        SlotState::Migrating
    );
    assert_eq!(
        store1.load().slot_assigned_owner(slot).unwrap(),
        node2_id_in_1
    );
    assert_eq!(
        store2.load().slot_state(slot).unwrap(),
        SlotState::Importing
    );
    assert_eq!(
        store2.load().slot_assigned_owner(slot).unwrap(),
        node1_id_in_2
    );

    let step2 =
        execute_live_slot_migration_step(&store1, &store2, &source, &target, slot, 1).unwrap();
    assert_eq!(
        step2,
        LiveSlotMigrationStepOutcome {
            moved_keys: 1,
            finalized: true,
        }
    );
    assert_eq!(store1.load().slot_state(slot).unwrap(), SlotState::Stable);
    assert_eq!(
        store1.load().slot_assigned_owner(slot).unwrap(),
        node2_id_in_1
    );
    assert_eq!(store2.load().slot_state(slot).unwrap(), SlotState::Stable);
    assert_eq!(
        store2.load().slot_assigned_owner(slot).unwrap(),
        LOCAL_WORKER_ID
    );

    let get1 = encode_resp_command(&[b"GET", &key1]);
    let get2 = encode_resp_command(&[b"GET", &key2]);
    assert_eq!(execute_processor_frame(&source, &get1), b"$-1\r\n");
    assert_eq!(execute_processor_frame(&source, &get2), b"$-1\r\n");
    assert_eq!(execute_processor_frame(&target, &get1), b"$2\r\nv1\r\n");
    assert_eq!(execute_processor_frame(&target, &get2), b"$2\r\nv2\r\n");
}

#[test]
fn execute_live_slot_migration_step_with_zero_batch_is_noop() {
    let key = b"{zero-step}k".to_vec();
    let slot = redis_hash_slot(&key);

    let mut config1 = ClusterConfig::new_local("node-1", "127.0.0.1", 7201);
    let (next1, _node2_id_in_1) = config1
        .add_worker(Worker::new(
            "node-2",
            "127.0.0.1",
            7202,
            WorkerRole::Primary,
        ))
        .unwrap();
    config1 = next1
        .set_slot_state(slot, LOCAL_WORKER_ID, SlotState::Stable)
        .unwrap();

    let mut config2 = ClusterConfig::new_local("node-2", "127.0.0.1", 7202);
    let (next2, node1_id_in_2) = config2
        .add_worker(Worker::new(
            "node-1",
            "127.0.0.1",
            7201,
            WorkerRole::Primary,
        ))
        .unwrap();
    config2 = next2
        .set_slot_state(slot, node1_id_in_2, SlotState::Stable)
        .unwrap();

    let store1 = ClusterConfigStore::new(config1);
    let store2 = ClusterConfigStore::new(config2);
    let source = RequestProcessor::new().unwrap();
    let target = RequestProcessor::new().unwrap();

    let set = encode_resp_command(&[b"SET", &key, b"value"]);
    assert_eq!(execute_processor_frame(&source, &set), b"+OK\r\n");

    let step =
        execute_live_slot_migration_step(&store1, &store2, &source, &target, slot, 0).unwrap();
    assert_eq!(
        step,
        LiveSlotMigrationStepOutcome {
            moved_keys: 0,
            finalized: false,
        }
    );
    assert_eq!(store1.load().slot_state(slot).unwrap(), SlotState::Stable);
    assert_eq!(
        store1.load().slot_assigned_owner(slot).unwrap(),
        LOCAL_WORKER_ID
    );
    assert_eq!(store2.load().slot_state(slot).unwrap(), SlotState::Stable);
    assert_eq!(
        store2.load().slot_assigned_owner(slot).unwrap(),
        node1_id_in_2
    );
    let get = encode_resp_command(&[b"GET", &key]);
    assert_eq!(execute_processor_frame(&source, &get), b"$5\r\nvalue\r\n");
    assert_eq!(execute_processor_frame(&target, &get), b"$-1\r\n");
}

#[tokio::test]
async fn run_live_slot_migration_until_complete_reports_batches() {
    let key1 = b"{run-slot}one".to_vec();
    let key2 = b"{run-slot}two".to_vec();
    let key3 = b"{run-slot}three".to_vec();
    let slot = redis_hash_slot(&key1);
    assert_eq!(slot, redis_hash_slot(&key2));
    assert_eq!(slot, redis_hash_slot(&key3));

    let mut config1 = ClusterConfig::new_local("node-1", "127.0.0.1", 7301);
    let (next1, node2_id_in_1) = config1
        .add_worker(Worker::new(
            "node-2",
            "127.0.0.1",
            7302,
            WorkerRole::Primary,
        ))
        .unwrap();
    config1 = next1
        .set_slot_state(slot, LOCAL_WORKER_ID, SlotState::Stable)
        .unwrap();

    let mut config2 = ClusterConfig::new_local("node-2", "127.0.0.1", 7302);
    let (next2, node1_id_in_2) = config2
        .add_worker(Worker::new(
            "node-1",
            "127.0.0.1",
            7301,
            WorkerRole::Primary,
        ))
        .unwrap();
    config2 = next2
        .set_slot_state(slot, node1_id_in_2, SlotState::Stable)
        .unwrap();

    let store1 = ClusterConfigStore::new(config1);
    let store2 = ClusterConfigStore::new(config2);
    let source = RequestProcessor::new().unwrap();
    let target = RequestProcessor::new().unwrap();

    let set1 = encode_resp_command(&[b"SET", &key1, b"v1"]);
    let set2 = encode_resp_command(&[b"SET", &key2, b"v2"]);
    let set3 = encode_resp_command(&[b"SET", &key3, b"v3"]);
    assert_eq!(execute_processor_frame(&source, &set1), b"+OK\r\n");
    assert_eq!(execute_processor_frame(&source, &set2), b"+OK\r\n");
    assert_eq!(execute_processor_frame(&source, &set3), b"+OK\r\n");

    let report = run_live_slot_migration_until_complete(
        &store1,
        &store2,
        &source,
        &target,
        slot,
        1,
        Duration::from_millis(1),
        std::future::pending::<()>(),
    )
    .await
    .unwrap();
    assert_eq!(
        report,
        LiveSlotMigrationRunReport {
            batches: 3,
            moved_keys: 3,
            finalized: true,
        }
    );

    let get1 = encode_resp_command(&[b"GET", &key1]);
    let get2 = encode_resp_command(&[b"GET", &key2]);
    let get3 = encode_resp_command(&[b"GET", &key3]);
    assert_eq!(execute_processor_frame(&source, &get1), b"$-1\r\n");
    assert_eq!(execute_processor_frame(&source, &get2), b"$-1\r\n");
    assert_eq!(execute_processor_frame(&source, &get3), b"$-1\r\n");
    assert_eq!(execute_processor_frame(&target, &get1), b"$2\r\nv1\r\n");
    assert_eq!(execute_processor_frame(&target, &get2), b"$2\r\nv2\r\n");
    assert_eq!(execute_processor_frame(&target, &get3), b"$2\r\nv3\r\n");
    assert_eq!(
        store1.load().slot_assigned_owner(slot).unwrap(),
        node2_id_in_1
    );
    assert_eq!(
        store2.load().slot_assigned_owner(slot).unwrap(),
        LOCAL_WORKER_ID
    );
}

#[tokio::test]
async fn run_live_slot_migration_until_complete_honors_shutdown() {
    let key = b"{run-shutdown}k".to_vec();
    let slot = redis_hash_slot(&key);

    let mut config1 = ClusterConfig::new_local("node-1", "127.0.0.1", 7401);
    let (next1, _node2_id_in_1) = config1
        .add_worker(Worker::new(
            "node-2",
            "127.0.0.1",
            7402,
            WorkerRole::Primary,
        ))
        .unwrap();
    config1 = next1
        .set_slot_state(slot, LOCAL_WORKER_ID, SlotState::Stable)
        .unwrap();

    let mut config2 = ClusterConfig::new_local("node-2", "127.0.0.1", 7402);
    let (next2, node1_id_in_2) = config2
        .add_worker(Worker::new(
            "node-1",
            "127.0.0.1",
            7401,
            WorkerRole::Primary,
        ))
        .unwrap();
    config2 = next2
        .set_slot_state(slot, node1_id_in_2, SlotState::Stable)
        .unwrap();

    let store1 = ClusterConfigStore::new(config1);
    let store2 = ClusterConfigStore::new(config2);
    let source = RequestProcessor::new().unwrap();
    let target = RequestProcessor::new().unwrap();

    let set = encode_resp_command(&[b"SET", &key, b"value"]);
    assert_eq!(execute_processor_frame(&source, &set), b"+OK\r\n");

    let report = run_live_slot_migration_until_complete(
        &store1,
        &store2,
        &source,
        &target,
        slot,
        1,
        Duration::from_millis(1),
        std::future::ready(()),
    )
    .await
    .unwrap();
    assert_eq!(
        report,
        LiveSlotMigrationRunReport {
            batches: 0,
            moved_keys: 0,
            finalized: false,
        }
    );
    let get = encode_resp_command(&[b"GET", &key]);
    assert_eq!(execute_processor_frame(&source, &get), b"$5\r\nvalue\r\n");
    assert_eq!(execute_processor_frame(&target, &get), b"$-1\r\n");
    assert_eq!(store1.load().slot_state(slot).unwrap(), SlotState::Stable);
    assert_eq!(
        store1.load().slot_assigned_owner(slot).unwrap(),
        LOCAL_WORKER_ID
    );
    assert_eq!(store2.load().slot_state(slot).unwrap(), SlotState::Stable);
    assert_eq!(
        store2.load().slot_assigned_owner(slot).unwrap(),
        node1_id_in_2
    );
}

#[tokio::test]
async fn run_live_slot_migrations_until_complete_round_robins_slots() {
    let key_a1 = b"{multi-a}one".to_vec();
    let key_a2 = b"{multi-a}two".to_vec();
    let slot_a = redis_hash_slot(&key_a1);
    let mut key_b1 = b"{multi-b}one".to_vec();
    while redis_hash_slot(&key_b1) == slot_a {
        key_b1.push(b'x');
    }
    let key_b2 = [key_b1.as_slice(), b"-two".as_slice()].concat();
    let slot_b = redis_hash_slot(&key_b1);
    assert_ne!(slot_a, slot_b);
    assert_eq!(slot_a, redis_hash_slot(&key_a2));
    assert_eq!(slot_b, redis_hash_slot(&key_b2));

    let mut config1 = ClusterConfig::new_local("node-1", "127.0.0.1", 7501);
    let (next1, node2_id_in_1) = config1
        .add_worker(Worker::new(
            "node-2",
            "127.0.0.1",
            7502,
            WorkerRole::Primary,
        ))
        .unwrap();
    config1 = next1
        .set_slot_state(slot_a, LOCAL_WORKER_ID, SlotState::Stable)
        .unwrap()
        .set_slot_state(slot_b, LOCAL_WORKER_ID, SlotState::Stable)
        .unwrap();

    let mut config2 = ClusterConfig::new_local("node-2", "127.0.0.1", 7502);
    let (next2, node1_id_in_2) = config2
        .add_worker(Worker::new(
            "node-1",
            "127.0.0.1",
            7501,
            WorkerRole::Primary,
        ))
        .unwrap();
    config2 = next2
        .set_slot_state(slot_a, node1_id_in_2, SlotState::Stable)
        .unwrap()
        .set_slot_state(slot_b, node1_id_in_2, SlotState::Stable)
        .unwrap();

    let store1 = ClusterConfigStore::new(config1);
    let store2 = ClusterConfigStore::new(config2);
    let source = RequestProcessor::new().unwrap();
    let target = RequestProcessor::new().unwrap();

    let set_a1 = encode_resp_command(&[b"SET", &key_a1, b"a1"]);
    let set_a2 = encode_resp_command(&[b"SET", &key_a2, b"a2"]);
    let set_b1 = encode_resp_command(&[b"SET", &key_b1, b"b1"]);
    let set_b2 = encode_resp_command(&[b"SET", &key_b2, b"b2"]);
    assert_eq!(execute_processor_frame(&source, &set_a1), b"+OK\r\n");
    assert_eq!(execute_processor_frame(&source, &set_a2), b"+OK\r\n");
    assert_eq!(execute_processor_frame(&source, &set_b1), b"+OK\r\n");
    assert_eq!(execute_processor_frame(&source, &set_b2), b"+OK\r\n");

    let report = run_live_slot_migrations_until_complete(
        &store1,
        &store2,
        &source,
        &target,
        &[slot_a, slot_b],
        1,
        Duration::from_millis(1),
        std::future::pending::<()>(),
    )
    .await
    .unwrap();

    assert!(!report.interrupted);
    assert_eq!(report.slots.len(), 2);
    let report_a = report
        .slots
        .iter()
        .find(|slot_report| slot_report.slot == slot_a)
        .unwrap();
    let report_b = report
        .slots
        .iter()
        .find(|slot_report| slot_report.slot == slot_b)
        .unwrap();
    assert_eq!(
        *report_a,
        LiveSlotMigrationSlotReport {
            slot: slot_a,
            batches: 2,
            moved_keys: 2,
            finalized: true,
        }
    );
    assert_eq!(
        *report_b,
        LiveSlotMigrationSlotReport {
            slot: slot_b,
            batches: 2,
            moved_keys: 2,
            finalized: true,
        }
    );

    let get_a1 = encode_resp_command(&[b"GET", &key_a1]);
    let get_a2 = encode_resp_command(&[b"GET", &key_a2]);
    let get_b1 = encode_resp_command(&[b"GET", &key_b1]);
    let get_b2 = encode_resp_command(&[b"GET", &key_b2]);
    assert_eq!(execute_processor_frame(&source, &get_a1), b"$-1\r\n");
    assert_eq!(execute_processor_frame(&source, &get_a2), b"$-1\r\n");
    assert_eq!(execute_processor_frame(&source, &get_b1), b"$-1\r\n");
    assert_eq!(execute_processor_frame(&source, &get_b2), b"$-1\r\n");
    assert_eq!(execute_processor_frame(&target, &get_a1), b"$2\r\na1\r\n");
    assert_eq!(execute_processor_frame(&target, &get_a2), b"$2\r\na2\r\n");
    assert_eq!(execute_processor_frame(&target, &get_b1), b"$2\r\nb1\r\n");
    assert_eq!(execute_processor_frame(&target, &get_b2), b"$2\r\nb2\r\n");
    assert_eq!(
        store1.load().slot_assigned_owner(slot_a).unwrap(),
        node2_id_in_1
    );
    assert_eq!(
        store1.load().slot_assigned_owner(slot_b).unwrap(),
        node2_id_in_1
    );
    assert_eq!(
        store2.load().slot_assigned_owner(slot_a).unwrap(),
        LOCAL_WORKER_ID
    );
    assert_eq!(
        store2.load().slot_assigned_owner(slot_b).unwrap(),
        LOCAL_WORKER_ID
    );
}

#[tokio::test]
async fn run_live_slot_migrations_until_complete_honors_shutdown() {
    let key = b"{multi-shutdown}k".to_vec();
    let slot = redis_hash_slot(&key);

    let mut config1 = ClusterConfig::new_local("node-1", "127.0.0.1", 7601);
    let (next1, _node2_id_in_1) = config1
        .add_worker(Worker::new(
            "node-2",
            "127.0.0.1",
            7602,
            WorkerRole::Primary,
        ))
        .unwrap();
    config1 = next1
        .set_slot_state(slot, LOCAL_WORKER_ID, SlotState::Stable)
        .unwrap();

    let mut config2 = ClusterConfig::new_local("node-2", "127.0.0.1", 7602);
    let (next2, node1_id_in_2) = config2
        .add_worker(Worker::new(
            "node-1",
            "127.0.0.1",
            7601,
            WorkerRole::Primary,
        ))
        .unwrap();
    config2 = next2
        .set_slot_state(slot, node1_id_in_2, SlotState::Stable)
        .unwrap();

    let store1 = ClusterConfigStore::new(config1);
    let store2 = ClusterConfigStore::new(config2);
    let source = RequestProcessor::new().unwrap();
    let target = RequestProcessor::new().unwrap();

    let set = encode_resp_command(&[b"SET", &key, b"value"]);
    assert_eq!(execute_processor_frame(&source, &set), b"+OK\r\n");

    let report = run_live_slot_migrations_until_complete(
        &store1,
        &store2,
        &source,
        &target,
        &[slot],
        1,
        Duration::from_millis(1),
        std::future::ready(()),
    )
    .await
    .unwrap();
    assert!(report.interrupted);
    assert_eq!(
        report.slots,
        vec![LiveSlotMigrationSlotReport {
            slot,
            batches: 0,
            moved_keys: 0,
            finalized: false,
        }]
    );
    let get = encode_resp_command(&[b"GET", &key]);
    assert_eq!(execute_processor_frame(&source, &get), b"$5\r\nvalue\r\n");
    assert_eq!(execute_processor_frame(&target, &get), b"$-1\r\n");
    assert_eq!(store1.load().slot_state(slot).unwrap(), SlotState::Stable);
    assert_eq!(
        store1.load().slot_assigned_owner(slot).unwrap(),
        LOCAL_WORKER_ID
    );
    assert_eq!(store2.load().slot_state(slot).unwrap(), SlotState::Stable);
    assert_eq!(
        store2.load().slot_assigned_owner(slot).unwrap(),
        node1_id_in_2
    );
}

#[tokio::test]
async fn run_live_slot_migrations_until_complete_deduplicates_slots() {
    let key = b"{multi-dedupe}k".to_vec();
    let slot = redis_hash_slot(&key);

    let mut config1 = ClusterConfig::new_local("node-1", "127.0.0.1", 7701);
    let (next1, node2_id_in_1) = config1
        .add_worker(Worker::new(
            "node-2",
            "127.0.0.1",
            7702,
            WorkerRole::Primary,
        ))
        .unwrap();
    config1 = next1
        .set_slot_state(slot, LOCAL_WORKER_ID, SlotState::Stable)
        .unwrap();

    let mut config2 = ClusterConfig::new_local("node-2", "127.0.0.1", 7702);
    let (next2, node1_id_in_2) = config2
        .add_worker(Worker::new(
            "node-1",
            "127.0.0.1",
            7701,
            WorkerRole::Primary,
        ))
        .unwrap();
    config2 = next2
        .set_slot_state(slot, node1_id_in_2, SlotState::Stable)
        .unwrap();

    let store1 = ClusterConfigStore::new(config1);
    let store2 = ClusterConfigStore::new(config2);
    let source = RequestProcessor::new().unwrap();
    let target = RequestProcessor::new().unwrap();

    let set = encode_resp_command(&[b"SET", &key, b"value"]);
    assert_eq!(execute_processor_frame(&source, &set), b"+OK\r\n");

    let report = run_live_slot_migrations_until_complete(
        &store1,
        &store2,
        &source,
        &target,
        &[slot, slot],
        1,
        Duration::from_millis(1),
        std::future::pending::<()>(),
    )
    .await
    .unwrap();
    assert!(!report.interrupted);
    assert_eq!(
        report.slots,
        vec![LiveSlotMigrationSlotReport {
            slot,
            batches: 1,
            moved_keys: 1,
            finalized: true,
        }]
    );
    let get = encode_resp_command(&[b"GET", &key]);
    assert_eq!(execute_processor_frame(&source, &get), b"$-1\r\n");
    assert_eq!(execute_processor_frame(&target, &get), b"$5\r\nvalue\r\n");
    assert_eq!(
        store1.load().slot_assigned_owner(slot).unwrap(),
        node2_id_in_1
    );
    assert_eq!(
        store2.load().slot_assigned_owner(slot).unwrap(),
        LOCAL_WORKER_ID
    );
}

#[tokio::test]
async fn run_live_slot_migrations_until_complete_zero_batch_is_interrupted_noop() {
    let key = b"{multi-zero}k".to_vec();
    let slot = redis_hash_slot(&key);

    let mut config1 = ClusterConfig::new_local("node-1", "127.0.0.1", 7801);
    let (next1, _node2_id_in_1) = config1
        .add_worker(Worker::new(
            "node-2",
            "127.0.0.1",
            7802,
            WorkerRole::Primary,
        ))
        .unwrap();
    config1 = next1
        .set_slot_state(slot, LOCAL_WORKER_ID, SlotState::Stable)
        .unwrap();

    let mut config2 = ClusterConfig::new_local("node-2", "127.0.0.1", 7802);
    let (next2, node1_id_in_2) = config2
        .add_worker(Worker::new(
            "node-1",
            "127.0.0.1",
            7801,
            WorkerRole::Primary,
        ))
        .unwrap();
    config2 = next2
        .set_slot_state(slot, node1_id_in_2, SlotState::Stable)
        .unwrap();

    let store1 = ClusterConfigStore::new(config1);
    let store2 = ClusterConfigStore::new(config2);
    let source = RequestProcessor::new().unwrap();
    let target = RequestProcessor::new().unwrap();

    let set = encode_resp_command(&[b"SET", &key, b"value"]);
    assert_eq!(execute_processor_frame(&source, &set), b"+OK\r\n");

    let report = run_live_slot_migrations_until_complete(
        &store1,
        &store2,
        &source,
        &target,
        &[slot],
        0,
        Duration::from_millis(1),
        std::future::pending::<()>(),
    )
    .await
    .unwrap();
    assert!(report.interrupted);
    assert_eq!(
        report.slots,
        vec![LiveSlotMigrationSlotReport {
            slot,
            batches: 0,
            moved_keys: 0,
            finalized: false,
        }]
    );
    let get = encode_resp_command(&[b"GET", &key]);
    assert_eq!(execute_processor_frame(&source, &get), b"$5\r\nvalue\r\n");
    assert_eq!(execute_processor_frame(&target, &get), b"$-1\r\n");
}

#[test]
fn detect_live_slot_migration_slots_intersects_migrating_and_importing() {
    let slot_a = SlotNumber::new(510);
    let slot_b = SlotNumber::new(511);
    let slot_c = SlotNumber::new(512);
    let slot_d = SlotNumber::new(513);

    let mut config1 = ClusterConfig::new_local("node-1", "127.0.0.1", 7901);
    let (next1, node2_id_in_1) = config1
        .add_worker(Worker::new(
            "node-2",
            "127.0.0.1",
            7902,
            WorkerRole::Primary,
        ))
        .unwrap();
    config1 = next1
        .begin_slot_migration_to(slot_a, node2_id_in_1)
        .unwrap()
        .begin_slot_migration_to(slot_b, node2_id_in_1)
        .unwrap()
        .set_slot_state(slot_c, LOCAL_WORKER_ID, SlotState::Stable)
        .unwrap();

    let mut config2 = ClusterConfig::new_local("node-2", "127.0.0.1", 7902);
    let (next2, node1_id_in_2) = config2
        .add_worker(Worker::new(
            "node-1",
            "127.0.0.1",
            7901,
            WorkerRole::Primary,
        ))
        .unwrap();
    config2 = next2
        .begin_slot_import_from(slot_a, node1_id_in_2)
        .unwrap()
        .set_slot_state(slot_b, node1_id_in_2, SlotState::Stable)
        .unwrap()
        .begin_slot_import_from(slot_d, node1_id_in_2)
        .unwrap();

    let store1 = ClusterConfigStore::new(config1);
    let store2 = ClusterConfigStore::new(config2);
    let slots = detect_live_slot_migration_slots(&store1, &store2).unwrap();
    assert_eq!(slots, vec![slot_a]);
}

#[tokio::test]
async fn run_detected_live_slot_migrations_until_complete_executes_detected_slots() {
    let key_a = b"{detected-a}k".to_vec();
    let key_b = b"{detected-b}k".to_vec();
    let key_c = b"{detected-c}k".to_vec();
    let slot_a = redis_hash_slot(&key_a);
    let slot_b = redis_hash_slot(&key_b);
    let slot_c = redis_hash_slot(&key_c);
    assert_ne!(slot_a, slot_b);
    assert_ne!(slot_a, slot_c);
    assert_ne!(slot_b, slot_c);

    let mut config1 = ClusterConfig::new_local("node-1", "127.0.0.1", 8001);
    let (next1, node2_id_in_1) = config1
        .add_worker(Worker::new(
            "node-2",
            "127.0.0.1",
            8002,
            WorkerRole::Primary,
        ))
        .unwrap();
    config1 = next1
        .begin_slot_migration_to(slot_a, node2_id_in_1)
        .unwrap()
        .begin_slot_migration_to(slot_b, node2_id_in_1)
        .unwrap()
        .set_slot_state(slot_c, LOCAL_WORKER_ID, SlotState::Stable)
        .unwrap();

    let mut config2 = ClusterConfig::new_local("node-2", "127.0.0.1", 8002);
    let (next2, node1_id_in_2) = config2
        .add_worker(Worker::new(
            "node-1",
            "127.0.0.1",
            8001,
            WorkerRole::Primary,
        ))
        .unwrap();
    config2 = next2
        .begin_slot_import_from(slot_a, node1_id_in_2)
        .unwrap()
        .begin_slot_import_from(slot_b, node1_id_in_2)
        .unwrap()
        .set_slot_state(slot_c, node1_id_in_2, SlotState::Stable)
        .unwrap();

    let store1 = ClusterConfigStore::new(config1);
    let store2 = ClusterConfigStore::new(config2);
    let source = RequestProcessor::new().unwrap();
    let target = RequestProcessor::new().unwrap();

    assert_eq!(
        execute_processor_frame(&source, &encode_resp_command(&[b"SET", &key_a, b"va"])),
        b"+OK\r\n"
    );
    assert_eq!(
        execute_processor_frame(&source, &encode_resp_command(&[b"SET", &key_b, b"vb"])),
        b"+OK\r\n"
    );
    assert_eq!(
        execute_processor_frame(&source, &encode_resp_command(&[b"SET", &key_c, b"vc"])),
        b"+OK\r\n"
    );

    let report = run_detected_live_slot_migrations_until_complete(
        &store1,
        &store2,
        &source,
        &target,
        1,
        Duration::from_millis(1),
        std::future::pending::<()>(),
    )
    .await
    .unwrap();
    assert!(!report.interrupted);
    assert_eq!(report.slots.len(), 2);
    let migrated_slots: HashSet<SlotNumber> = report.slots.iter().map(|slot| slot.slot).collect();
    assert_eq!(migrated_slots, HashSet::from([slot_a, slot_b]));
    for slot_report in report.slots {
        assert_eq!(slot_report.batches, 1);
        assert_eq!(slot_report.moved_keys, 1);
        assert!(slot_report.finalized);
    }

    assert_eq!(
        execute_processor_frame(&source, &encode_resp_command(&[b"GET", &key_a])),
        b"$-1\r\n"
    );
    assert_eq!(
        execute_processor_frame(&source, &encode_resp_command(&[b"GET", &key_b])),
        b"$-1\r\n"
    );
    assert_eq!(
        execute_processor_frame(&target, &encode_resp_command(&[b"GET", &key_a])),
        b"$2\r\nva\r\n"
    );
    assert_eq!(
        execute_processor_frame(&target, &encode_resp_command(&[b"GET", &key_b])),
        b"$2\r\nvb\r\n"
    );
    assert_eq!(
        execute_processor_frame(&source, &encode_resp_command(&[b"GET", &key_c])),
        b"$2\r\nvc\r\n"
    );
    assert_eq!(
        execute_processor_frame(&target, &encode_resp_command(&[b"GET", &key_c])),
        b"$-1\r\n"
    );

    for slot in [slot_a, slot_b] {
        assert_eq!(store1.load().slot_state(slot).unwrap(), SlotState::Stable);
        assert_eq!(store2.load().slot_state(slot).unwrap(), SlotState::Stable);
        assert_eq!(
            store1.load().slot_assigned_owner(slot).unwrap(),
            node2_id_in_1
        );
        assert_eq!(
            store2.load().slot_assigned_owner(slot).unwrap(),
            LOCAL_WORKER_ID
        );
    }
    assert_eq!(
        store1.load().slot_assigned_owner(slot_c).unwrap(),
        LOCAL_WORKER_ID
    );
    assert_eq!(
        store2.load().slot_assigned_owner(slot_c).unwrap(),
        node1_id_in_2
    );
}

#[tokio::test]
async fn run_detected_live_slot_migrations_until_complete_noop_when_no_detected_slots() {
    let mut config1 = ClusterConfig::new_local("node-1", "127.0.0.1", 8101);
    let (next1, _node2_id_in_1) = config1
        .add_worker(Worker::new(
            "node-2",
            "127.0.0.1",
            8102,
            WorkerRole::Primary,
        ))
        .unwrap();
    let slot = SlotNumber::new(901);
    config1 = next1
        .set_slot_state(slot, LOCAL_WORKER_ID, SlotState::Stable)
        .unwrap();

    let mut config2 = ClusterConfig::new_local("node-2", "127.0.0.1", 8102);
    let (next2, node1_id_in_2) = config2
        .add_worker(Worker::new(
            "node-1",
            "127.0.0.1",
            8101,
            WorkerRole::Primary,
        ))
        .unwrap();
    config2 = next2
        .set_slot_state(slot, node1_id_in_2, SlotState::Stable)
        .unwrap();

    let store1 = ClusterConfigStore::new(config1);
    let store2 = ClusterConfigStore::new(config2);
    let source = RequestProcessor::new().unwrap();
    let target = RequestProcessor::new().unwrap();

    let report = run_detected_live_slot_migrations_until_complete(
        &store1,
        &store2,
        &source,
        &target,
        1,
        Duration::from_millis(1),
        std::future::pending::<()>(),
    )
    .await
    .unwrap();
    assert!(!report.interrupted);
    assert!(report.slots.is_empty());
    assert_eq!(
        store1.load().slot_assigned_owner(slot).unwrap(),
        LOCAL_WORKER_ID
    );
    assert_eq!(
        store2.load().slot_assigned_owner(slot).unwrap(),
        node1_id_in_2
    );
}

#[tokio::test]
async fn run_detected_live_slot_migrations_until_shutdown_runs_multiple_detection_cycles() {
    let key_a = b"{detected-cycle-a}k".to_vec();
    let key_b = b"{detected-cycle-b}k".to_vec();
    let slot_a = redis_hash_slot(&key_a);
    let slot_b = redis_hash_slot(&key_b);
    assert_ne!(slot_a, slot_b);

    let mut config1 = ClusterConfig::new_local("node-1", "127.0.0.1", 8201);
    let (next1, node2_id_in_1) = config1
        .add_worker(Worker::new(
            "node-2",
            "127.0.0.1",
            8202,
            WorkerRole::Primary,
        ))
        .unwrap();
    config1 = next1
        .set_slot_state(slot_a, LOCAL_WORKER_ID, SlotState::Stable)
        .unwrap()
        .set_slot_state(slot_b, LOCAL_WORKER_ID, SlotState::Stable)
        .unwrap();

    let mut config2 = ClusterConfig::new_local("node-2", "127.0.0.1", 8202);
    let (next2, node1_id_in_2) = config2
        .add_worker(Worker::new(
            "node-1",
            "127.0.0.1",
            8201,
            WorkerRole::Primary,
        ))
        .unwrap();
    config2 = next2
        .set_slot_state(slot_a, node1_id_in_2, SlotState::Stable)
        .unwrap()
        .set_slot_state(slot_b, node1_id_in_2, SlotState::Stable)
        .unwrap();

    let store1 = Arc::new(ClusterConfigStore::new(config1));
    let store2 = Arc::new(ClusterConfigStore::new(config2));
    let source = Arc::new(RequestProcessor::new().unwrap());
    let target = Arc::new(RequestProcessor::new().unwrap());

    assert_eq!(
        execute_processor_frame(&source, &encode_resp_command(&[b"SET", &key_a, b"va"])),
        b"+OK\r\n"
    );

    let first_source = store1
        .load()
        .as_ref()
        .clone()
        .begin_slot_migration_to(slot_a, node2_id_in_1)
        .unwrap();
    store1.publish(first_source);
    let first_target = store2
        .load()
        .as_ref()
        .clone()
        .begin_slot_import_from(slot_a, node1_id_in_2)
        .unwrap();
    store2.publish(first_target);

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let orchestrator_store1 = Arc::clone(&store1);
    let orchestrator_store2 = Arc::clone(&store2);
    let orchestrator_source = Arc::clone(&source);
    let orchestrator_target = Arc::clone(&target);
    let orchestrator = tokio::spawn(async move {
        let get_a = encode_resp_command(&[b"GET", &key_a]);
        wait_until(
            || {
                execute_processor_frame(&orchestrator_source, &get_a) == b"$-1\r\n"
                    && execute_processor_frame(&orchestrator_target, &get_a) == b"$2\r\nva\r\n"
            },
            Duration::from_secs(1),
        )
        .await;
        assert_eq!(
            execute_processor_frame(&orchestrator_source, &get_a),
            b"$-1\r\n"
        );
        assert_eq!(
            execute_processor_frame(&orchestrator_target, &get_a),
            b"$2\r\nva\r\n"
        );

        assert_eq!(
            execute_processor_frame(
                &orchestrator_source,
                &encode_resp_command(&[b"SET", &key_b, b"vb"])
            ),
            b"+OK\r\n"
        );
        let second_source = orchestrator_store1
            .load()
            .as_ref()
            .clone()
            .begin_slot_migration_to(slot_b, node2_id_in_1)
            .unwrap();
        orchestrator_store1.publish(second_source);
        let second_target = orchestrator_store2
            .load()
            .as_ref()
            .clone()
            .begin_slot_import_from(slot_b, node1_id_in_2)
            .unwrap();
        orchestrator_store2.publish(second_target);

        let get_b = encode_resp_command(&[b"GET", &key_b]);
        wait_until(
            || {
                execute_processor_frame(&orchestrator_source, &get_b) == b"$-1\r\n"
                    && execute_processor_frame(&orchestrator_target, &get_b) == b"$2\r\nvb\r\n"
            },
            Duration::from_secs(1),
        )
        .await;
        assert_eq!(
            execute_processor_frame(&orchestrator_source, &get_b),
            b"$-1\r\n"
        );
        assert_eq!(
            execute_processor_frame(&orchestrator_target, &get_b),
            b"$2\r\nvb\r\n"
        );
        let _ = shutdown_tx.send(());
    });

    let reports = run_detected_live_slot_migrations_until_shutdown(
        &store1,
        &store2,
        &source,
        &target,
        1,
        Duration::from_millis(1),
        Duration::from_millis(1),
        async move {
            let _ = shutdown_rx.await;
        },
    )
    .await
    .unwrap();
    orchestrator.await.unwrap();

    assert_eq!(reports.len(), 2);
    assert!(reports.iter().all(|report| !report.interrupted));
    let migrated_slots: HashSet<SlotNumber> = reports
        .iter()
        .flat_map(|report| report.slots.iter().map(|slot| slot.slot))
        .collect();
    assert_eq!(migrated_slots, HashSet::from([slot_a, slot_b]));
}

#[tokio::test]
async fn run_detected_live_slot_migrations_until_shutdown_honors_shutdown_without_work() {
    let mut config1 = ClusterConfig::new_local("node-1", "127.0.0.1", 8301);
    let (next1, _node2_id_in_1) = config1
        .add_worker(Worker::new(
            "node-2",
            "127.0.0.1",
            8302,
            WorkerRole::Primary,
        ))
        .unwrap();
    let slot = SlotNumber::new(1001);
    config1 = next1
        .set_slot_state(slot, LOCAL_WORKER_ID, SlotState::Stable)
        .unwrap();

    let mut config2 = ClusterConfig::new_local("node-2", "127.0.0.1", 8302);
    let (next2, node1_id_in_2) = config2
        .add_worker(Worker::new(
            "node-1",
            "127.0.0.1",
            8301,
            WorkerRole::Primary,
        ))
        .unwrap();
    config2 = next2
        .set_slot_state(slot, node1_id_in_2, SlotState::Stable)
        .unwrap();

    let store1 = ClusterConfigStore::new(config1);
    let store2 = ClusterConfigStore::new(config2);
    let source = RequestProcessor::new().unwrap();
    let target = RequestProcessor::new().unwrap();

    let reports = run_detected_live_slot_migrations_until_shutdown(
        &store1,
        &store2,
        &source,
        &target,
        1,
        Duration::from_millis(1),
        Duration::from_millis(10),
        tokio::time::sleep(Duration::from_millis(5)),
    )
    .await
    .unwrap();
    assert!(reports.is_empty());
}

#[test]
fn owner_routed_shard_selection_handles_single_and_multi_key_commands() {
    let processor = RequestProcessor::new().unwrap();
    let mut args = [ArgSlice::EMPTY; 64];

    let get_frame = encode_resp_command(&[b"GET", b"my-key"]);
    let meta = parse_resp_command_arg_slices(&get_frame, &mut args).unwrap();
    let command = unsafe { dispatch_from_arg_slices(&args[..meta.argument_count]) };
    let routed_shard =
        owner_routed_shard_for_command(&processor, &args[..meta.argument_count], command);
    assert_eq!(
        routed_shard,
        Some(processor.string_store_shard_index(b"my-key"))
    );

    let del_multi_frame = encode_resp_command(&[b"DEL", b"k1", b"k2"]);
    let del_meta = parse_resp_command_arg_slices(&del_multi_frame, &mut args).unwrap();
    let del_command = unsafe { dispatch_from_arg_slices(&args[..del_meta.argument_count]) };
    assert_eq!(
        owner_routed_shard_for_command(&processor, &args[..del_meta.argument_count], del_command),
        None
    );
}

#[test]
fn execute_frame_via_processor_matches_direct_execution() {
    let processor = RequestProcessor::new().unwrap();

    let set_frame = encode_resp_command(&[b"SET", b"k", b"v"]);
    let routed_set = execute_frame_via_processor(&processor, &set_frame).unwrap();
    let direct_set = execute_processor_frame(&processor, &set_frame);
    assert_eq!(routed_set, direct_set);

    let get_frame = encode_resp_command(&[b"GET", b"k"]);
    let routed_get = execute_frame_via_processor(&processor, &get_frame).unwrap();
    let direct_get = execute_processor_frame(&processor, &get_frame);
    assert_eq!(routed_get, direct_get);

    assert!(matches!(
        execute_frame_via_processor(&processor, b"*1\r\n$4\r\nPING"),
        Err(RoutedExecutionError::Protocol)
    ));
}

#[test]
fn execute_owned_args_via_processor_matches_direct_execution() {
    let processor = RequestProcessor::new().unwrap();

    let set_frame = encode_resp_command(&[b"SET", b"k", b"v"]);
    let set_owned_args = owned_args_from_frame(&set_frame);
    let routed_set = execute_owned_args_via_processor(&processor, &set_owned_args).unwrap();
    let direct_set = execute_processor_frame(&processor, &set_frame);
    assert_eq!(routed_set, direct_set);

    let get_frame = encode_resp_command(&[b"GET", b"k"]);
    let get_owned_args = owned_args_from_frame(&get_frame);
    let routed_get = execute_owned_args_via_processor(&processor, &get_owned_args).unwrap();
    let direct_get = execute_processor_frame(&processor, &get_frame);
    assert_eq!(routed_get, direct_get);

    assert!(matches!(
        execute_owned_args_via_processor(&processor, &[]),
        Err(RoutedExecutionError::Protocol)
    ));
}

#[test]
fn execute_owned_frame_args_via_processor_matches_direct_execution() {
    let processor = RequestProcessor::new().unwrap();

    let set_frame = encode_resp_command(&[b"SET", b"k", b"v"]);
    let set_owned_args = owned_frame_args_from_frame(&set_frame);
    let routed_set = execute_owned_frame_args_via_processor(
        &processor,
        &set_owned_args,
        RequestTimeSnapshot::capture(),
        false,
        false,
        None,
        crate::request_lifecycle::DbName::fixture(),
    )
    .unwrap();
    let direct_set = execute_processor_frame(&processor, &set_frame);
    assert_eq!(routed_set.frame_response, direct_set);

    let get_frame = encode_resp_command(&[b"GET", b"k"]);
    let get_owned_args = owned_frame_args_from_frame(&get_frame);
    let routed_get = execute_owned_frame_args_via_processor(
        &processor,
        &get_owned_args,
        RequestTimeSnapshot::capture(),
        false,
        false,
        None,
        crate::request_lifecycle::DbName::fixture(),
    )
    .unwrap();
    let direct_get = execute_processor_frame(&processor, &get_frame);
    assert_eq!(routed_get.frame_response, direct_get);
}

#[test]
fn capture_owned_frame_args_rejects_invalid_argument_views() {
    let frame = encode_resp_command(&[b"GET", b"k"]);
    assert!(matches!(
        capture_owned_frame_args(&frame, &[]),
        Err(RoutedExecutionError::Protocol)
    ));

    let foreign_arg = ArgSlice::from_slice(b"foreign").unwrap();
    assert!(matches!(
        capture_owned_frame_args(&frame, &[foreign_arg]),
        Err(RoutedExecutionError::Protocol)
    ));
}

#[tokio::test]
async fn srandmember_long_chain_external_scenario_runs_as_tcp_integration_test() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let metrics = Arc::new(ServerMetrics::default());
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let processor =
        Arc::new(RequestProcessor::new_with_string_store_shards_and_scripting(1, false).unwrap());

    let server_metrics = Arc::clone(&metrics);
    let server_processor = Arc::clone(&processor);
    let server = tokio::spawn(async move {
        run_listener_with_shutdown_and_cluster_with_processor(
            listener,
            1024,
            server_metrics,
            async move {
                let _ = shutdown_rx.await;
            },
            None,
            server_processor,
        )
        .await
        .unwrap();
    });

    let mut client = TcpStream::connect(addr).await.unwrap();
    let info_server = send_and_read_bulk_payload(
        &mut client,
        &encode_resp_command(&[b"INFO", b"SERVER"]),
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(
        read_info_u64(&info_server, "process_id"),
        Some(std::process::id() as u64)
    );

    // Redis tests/unit/type/set.tcl:
    // "SRANDMEMBER with a dict containing long chain"
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"CONFIG", b"SET", b"save", b""]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"CONFIG", b"SET", b"set-max-listpack-entries", b"0"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"CONFIG", b"SET", b"rdb-key-save-delay", b"2147483647"]),
        b"+OK\r\n",
    )
    .await;

    create_set_like_redis_external_test(&mut client, b"myset", 100_000).await;

    wait_for_set_rehashing_to_complete_like_redis_external_test(
        &mut client,
        b"myset",
        b"100",
        Duration::from_secs(5),
    )
    .await;

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"BGSAVE"]),
        b"+Background saving started\r\n",
    )
    .await;
    rem_hash_set_top_n_like_redis_external_test(&mut client, b"myset", 100_000 - 500).await;
    assert_eq!(
        send_and_read_integer(
            &mut client,
            &encode_resp_command(&[b"SCARD", b"myset"]),
            Duration::from_secs(5),
        )
        .await,
        500
    );

    processor.force_finish_bgsave_for_tests();
    wait_for_bgsave_to_finish(&mut client, Duration::from_secs(5)).await;

    let popped = send_and_read_bulk_array_payloads(
        &mut client,
        &encode_resp_command(&[b"SPOP", b"myset", b"1"]),
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(popped.len(), 1);
    assert!(
        debug_htstats_key_is_rehashing(&mut client, b"myset", Duration::from_secs(5)).await,
        "DEBUG HTSTATS-KEY should report rehashing after extreme shrink trigger"
    );

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"BGSAVE"]),
        b"+Background saving started\r\n",
    )
    .await;

    wait_for_set_rehashing_to_complete_like_redis_external_test(
        &mut client,
        b"myset",
        b"1",
        Duration::from_secs(5),
    )
    .await;

    processor.force_finish_bgsave_for_tests();
    wait_for_bgsave_to_finish(&mut client, Duration::from_secs(5)).await;

    let expected_members = send_and_read_bulk_array_payloads(
        &mut client,
        &encode_resp_command(&[b"SMEMBERS", b"myset"]),
        Duration::from_secs(5),
    )
    .await
    .into_iter()
    .collect::<std::collections::BTreeSet<_>>();
    let mut observed_members = std::collections::BTreeSet::new();

    let mut iterations = 1000usize;
    while iterations != 0 {
        iterations -= 1;
        let sample = send_and_read_bulk_array_payloads(
            &mut client,
            &encode_resp_command(&[b"SRANDMEMBER", b"myset", b"-10"]),
            Duration::from_secs(5),
        )
        .await;
        observed_members.extend(sample);
        if observed_members == expected_members {
            break;
        }
    }
    assert_ne!(iterations, 0);

    rem_hash_set_top_n_like_redis_external_test(&mut client, b"myset", 499 - 30).await;
    assert_eq!(
        send_and_read_integer(
            &mut client,
            &encode_resp_command(&[b"SCARD", b"myset"]),
            Duration::from_secs(5),
        )
        .await,
        30
    );
    let htstats = send_and_read_debug_htstats_key_payload(
        &mut client,
        b"myset",
        false,
        Duration::from_secs(5),
    )
    .await;
    let htstats_text = String::from_utf8(htstats).unwrap();
    assert!(
        !htstats_text.contains("rehashing target"),
        "rehashing should be complete: {htstats_text}"
    );
    assert!(
        htstats_text.contains("table size: 64"),
        "expected shrunk table size: {htstats_text}"
    );
    assert!(
        htstats_text.contains("number of elements: 30"),
        "expected final member count: {htstats_text}"
    );

    let htstats_full = send_and_read_debug_htstats_key_payload(
        &mut client,
        b"myset",
        true,
        Duration::from_secs(5),
    )
    .await;
    let htstats_full_text = String::from_utf8(htstats_full).unwrap();
    assert!(
        htstats_full_text.contains("different slots: 1"),
        "expected single-slot synthetic distribution: {htstats_full_text}"
    );
    assert!(
        htstats_full_text.contains("max chain length: 30"),
        "expected long-chain synthetic distribution: {htstats_full_text}"
    );

    let members = send_and_read_bulk_array_payloads(
        &mut client,
        &encode_resp_command(&[b"SMEMBERS", b"myset"]),
        Duration::from_secs(5),
    )
    .await
    .into_iter()
    .collect::<std::collections::BTreeSet<_>>();
    let mut histogram_samples = Vec::with_capacity(10_000);
    for _ in 0..1000 {
        let sample = send_and_read_bulk_array_payloads(
            &mut client,
            &encode_resp_command(&[b"SRANDMEMBER", b"myset", b"10"]),
            Duration::from_secs(5),
        )
        .await;
        assert_eq!(sample.len(), 10);
        histogram_samples.extend(sample);
    }
    assert!(chi_square_value(&histogram_samples, &members) < 73.0);

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn string_external_setbit_with_out_of_range_bit_offset_returns_range_error() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let mut client = TcpStream::connect(addr).await.unwrap();

    // Redis tests/unit/type/string.tcl:
    // "SETBIT with out of range bit offset"
    let huge_offset = (4u64 * 1024 * 1024 * 1024).to_string();
    let error = send_and_read_error_line(
        &mut client,
        &encode_resp_command(&[b"SETBIT", b"mykey", huge_offset.as_bytes(), b"1"]),
        Duration::from_secs(5),
    )
    .await;
    assert!(
        error.contains("out of range"),
        "expected out-of-range error, got: {error}"
    );

    let negative_error = send_and_read_error_line(
        &mut client,
        &encode_resp_command(&[b"SETBIT", b"mykey", b"-1", b"1"]),
        Duration::from_secs(5),
    )
    .await;
    assert!(
        negative_error.contains("out of range"),
        "expected out-of-range error, got: {negative_error}"
    );

    send_and_expect(&mut client, &encode_resp_command(&[b"PING"]), b"+PONG\r\n").await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn string_external_setrange_offset_limits_match_redis() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let mut client = TcpStream::connect(addr).await.unwrap();

    // Redis tests/unit/type/string.tcl:
    // "SETRANGE with out of range offset"
    let max_offset = (512usize * 1024 * 1024 - 4).to_string();
    let max_size_error = send_and_read_error_line(
        &mut client,
        &encode_resp_command(&[b"SETRANGE", b"mykey", max_offset.as_bytes(), b"world"]),
        Duration::from_secs(5),
    )
    .await;
    assert!(
        max_size_error.contains("maximum allowed size"),
        "expected maximum allowed size error, got: {max_size_error}"
    );

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"mykey", b"hello"]),
        b"+OK\r\n",
    )
    .await;

    let negative_error = send_and_read_error_line(
        &mut client,
        &encode_resp_command(&[b"SETRANGE", b"mykey", b"-1", b"world"]),
        Duration::from_secs(5),
    )
    .await;
    assert!(
        negative_error.contains("out of range"),
        "expected out-of-range error, got: {negative_error}"
    );

    let repeated_max_size_error = send_and_read_error_line(
        &mut client,
        &encode_resp_command(&[b"SETRANGE", b"mykey", max_offset.as_bytes(), b"world"]),
        Duration::from_secs(5),
    )
    .await;
    assert!(
        repeated_max_size_error.contains("maximum allowed size"),
        "expected maximum allowed size error, got: {repeated_max_size_error}"
    );

    // Redis tests/unit/type/string.tcl:
    // "SETRANGE with huge offset"
    for offset in [b"9223372036854775807".as_slice(), b"2147483647".as_slice()] {
        let error = send_and_read_error_line(
            &mut client,
            &encode_resp_command(&[b"SETRANGE", b"K", offset, b"A"]),
            Duration::from_secs(5),
        )
        .await;
        assert!(
            error.contains("string exceeds maximum allowed size") || error.contains("out of range"),
            "expected huge-offset error, got: {error}"
        );
    }

    send_and_expect(&mut client, &encode_resp_command(&[b"PING"]), b"+PONG\r\n").await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn string_external_mutations_switch_integer_encoding_to_raw() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let mut client = TcpStream::connect(addr).await.unwrap();

    // Redis tests/unit/type/string.tcl:
    // "SETRANGE against integer-encoded key"
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"mykey", b"1234"]),
        b"+OK\r\n",
    )
    .await;
    assert_eq!(
        send_and_read_bulk_payload(
            &mut client,
            &encode_resp_command(&[b"OBJECT", b"ENCODING", b"mykey"]),
            Duration::from_secs(5),
        )
        .await,
        b"int"
    );
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SETRANGE", b"mykey", b"0", b"2"]),
        b":4\r\n",
    )
    .await;
    assert_eq!(
        send_and_read_bulk_payload(
            &mut client,
            &encode_resp_command(&[b"OBJECT", b"ENCODING", b"mykey"]),
            Duration::from_secs(5),
        )
        .await,
        b"raw"
    );
    assert_eq!(
        send_and_read_bulk_payload(
            &mut client,
            &encode_resp_command(&[b"GET", b"mykey"]),
            Duration::from_secs(5),
        )
        .await,
        b"2234"
    );

    // Redis tests/unit/type/string.tcl:
    // "APPEND modifies the encoding from int to raw"
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEL", b"foo"]),
        b":0\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"foo", b"1"]),
        b"+OK\r\n",
    )
    .await;
    assert_eq!(
        send_and_read_bulk_payload(
            &mut client,
            &encode_resp_command(&[b"OBJECT", b"ENCODING", b"foo"]),
            Duration::from_secs(5),
        )
        .await,
        b"int"
    );
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"APPEND", b"foo", b"2"]),
        b":2\r\n",
    )
    .await;
    assert_eq!(
        send_and_read_bulk_payload(
            &mut client,
            &encode_resp_command(&[b"GET", b"foo"]),
            Duration::from_secs(5),
        )
        .await,
        b"12"
    );
    assert_eq!(
        send_and_read_bulk_payload(
            &mut client,
            &encode_resp_command(&[b"OBJECT", b"ENCODING", b"foo"]),
            Duration::from_secs(5),
        )
        .await,
        b"raw"
    );

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn publish_to_self_inside_multi_external_pubsub_scenario() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let mut client = TcpStream::connect(addr).await.unwrap();
    let timeout = Duration::from_secs(5);

    send_hello_and_drain(&mut client, b"3").await;

    let subscribe = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"SUBSCRIBE", b"foo"]),
        timeout,
    )
    .await;
    let subscribe_items = resp_socket_array(&subscribe);
    assert_eq!(resp_socket_bulk(&subscribe_items[0]), b"subscribe");
    assert_eq!(resp_socket_bulk(&subscribe_items[1]), b"foo");
    assert_eq!(resp_socket_integer(&subscribe_items[2]), 1);

    send_and_expect(&mut client, &encode_resp_command(&[b"MULTI"]), b"+OK\r\n").await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"PING", b"abc"]),
        b"+QUEUED\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"PUBLISH", b"foo", b"bar"]),
        b"+QUEUED\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"PUBLISH", b"foo", b"vaz"]),
        b"+QUEUED\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"PING", b"def"]),
        b"+QUEUED\r\n",
    )
    .await;

    let exec =
        send_and_read_resp_value(&mut client, &encode_resp_command(&[b"EXEC"]), timeout).await;
    let exec_items = resp_socket_array(&exec);
    assert_eq!(exec_items.len(), 4);
    assert_eq!(resp_socket_bulk(&exec_items[0]), b"abc");
    assert_eq!(resp_socket_integer(&exec_items[1]), 1);
    assert_eq!(resp_socket_integer(&exec_items[2]), 1);
    assert_eq!(resp_socket_bulk(&exec_items[3]), b"def");

    let first_message = read_resp_value_with_timeout(&mut client, timeout).await;
    let first_items = resp_socket_array(&first_message);
    assert_eq!(first_items.len(), 3);
    assert_eq!(resp_socket_bulk(&first_items[0]), b"message");
    assert_eq!(resp_socket_bulk(&first_items[1]), b"foo");
    assert_eq!(resp_socket_bulk(&first_items[2]), b"bar");

    let second_message = read_resp_value_with_timeout(&mut client, timeout).await;
    let second_items = resp_socket_array(&second_message);
    assert_eq!(second_items.len(), 3);
    assert_eq!(resp_socket_bulk(&second_items[0]), b"message");
    assert_eq!(resp_socket_bulk(&second_items[1]), b"foo");
    assert_eq!(resp_socket_bulk(&second_items[2]), b"vaz");

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn unsubscribe_inside_multi_then_publish_to_self_external_pubsub_scenario() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let mut client = TcpStream::connect(addr).await.unwrap();
    let timeout = Duration::from_secs(5);

    send_hello_and_drain(&mut client, b"3").await;

    let first_subscribe = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"SUBSCRIBE", b"foo", b"bar", b"baz"]),
        timeout,
    )
    .await;
    let first_subscribe_items = resp_socket_array(&first_subscribe);
    assert_eq!(resp_socket_bulk(&first_subscribe_items[0]), b"subscribe");
    assert_eq!(resp_socket_bulk(&first_subscribe_items[1]), b"foo");
    assert_eq!(resp_socket_integer(&first_subscribe_items[2]), 1);

    let second_subscribe = read_resp_value_with_timeout(&mut client, timeout).await;
    let second_subscribe_items = resp_socket_array(&second_subscribe);
    assert_eq!(resp_socket_bulk(&second_subscribe_items[0]), b"subscribe");
    assert_eq!(resp_socket_bulk(&second_subscribe_items[1]), b"bar");
    assert_eq!(resp_socket_integer(&second_subscribe_items[2]), 2);

    let third_subscribe = read_resp_value_with_timeout(&mut client, timeout).await;
    let third_subscribe_items = resp_socket_array(&third_subscribe);
    assert_eq!(resp_socket_bulk(&third_subscribe_items[0]), b"subscribe");
    assert_eq!(resp_socket_bulk(&third_subscribe_items[1]), b"baz");
    assert_eq!(resp_socket_integer(&third_subscribe_items[2]), 3);

    send_and_expect(&mut client, &encode_resp_command(&[b"MULTI"]), b"+OK\r\n").await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"PING", b"abc"]),
        b"+QUEUED\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"UNSUBSCRIBE", b"bar"]),
        b"+QUEUED\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"UNSUBSCRIBE", b"baz"]),
        b"+QUEUED\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"PING", b"def"]),
        b"+QUEUED\r\n",
    )
    .await;

    let exec =
        send_and_read_resp_value(&mut client, &encode_resp_command(&[b"EXEC"]), timeout).await;
    let exec_items = resp_socket_array(&exec);
    assert_eq!(exec_items.len(), 4);
    assert_eq!(resp_socket_bulk(&exec_items[0]), b"abc");

    let first_unsubscribe = resp_socket_array(&exec_items[1]);
    assert_eq!(resp_socket_bulk(&first_unsubscribe[0]), b"unsubscribe");
    assert_eq!(resp_socket_bulk(&first_unsubscribe[1]), b"bar");
    assert_eq!(resp_socket_integer(&first_unsubscribe[2]), 2);

    let second_unsubscribe = resp_socket_array(&exec_items[2]);
    assert_eq!(resp_socket_bulk(&second_unsubscribe[0]), b"unsubscribe");
    assert_eq!(resp_socket_bulk(&second_unsubscribe[1]), b"baz");
    assert_eq!(resp_socket_integer(&second_unsubscribe[2]), 1);

    assert_eq!(resp_socket_bulk(&exec_items[3]), b"def");

    assert_eq!(
        send_and_read_integer(
            &mut client,
            &encode_resp_command(&[b"PUBLISH", b"foo", b"vaz"]),
            timeout,
        )
        .await,
        1
    );

    let message = read_resp_value_with_timeout(&mut client, timeout).await;
    let message_items = resp_socket_array(&message);
    assert_eq!(message_items.len(), 3);
    assert_eq!(resp_socket_bulk(&message_items[0]), b"message");
    assert_eq!(resp_socket_bulk(&message_items[1]), b"foo");
    assert_eq!(resp_socket_bulk(&message_items[2]), b"vaz");

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn keyspace_notifications_evicted_events_external_pubsub_scenario() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let mut writer = TcpStream::connect(addr).await.unwrap();
    let mut subscriber = TcpStream::connect(addr).await.unwrap();
    let timeout = Duration::from_secs(5);

    send_and_expect(
        &mut writer,
        &encode_resp_command(&[b"CONFIG", b"SET", b"notify-keyspace-events", b"Ee"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut writer,
        &encode_resp_command(&[b"CONFIG", b"SET", b"maxmemory-policy", b"allkeys-lru"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(&mut writer, &encode_resp_command(&[b"FLUSHDB"]), b"+OK\r\n").await;

    let subscribe = send_and_read_resp_value(
        &mut subscriber,
        &encode_resp_command(&[b"PSUBSCRIBE", b"*"]),
        timeout,
    )
    .await;
    let subscribe_items = resp_socket_array(&subscribe);
    assert_eq!(resp_socket_bulk(&subscribe_items[0]), b"psubscribe");
    assert_eq!(resp_socket_bulk(&subscribe_items[1]), b"*");
    assert_eq!(resp_socket_integer(&subscribe_items[2]), 1);

    send_and_expect(
        &mut writer,
        &encode_resp_command(&[b"SET", b"foo", b"bar"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut writer,
        &encode_resp_command(&[b"CONFIG", b"SET", b"maxmemory", b"1"]),
        b"+OK\r\n",
    )
    .await;

    let evicted = read_resp_value_with_timeout(&mut subscriber, timeout).await;
    let evicted_items = resp_socket_array(&evicted);
    assert_eq!(resp_socket_bulk(&evicted_items[0]), b"pmessage");
    assert_eq!(resp_socket_bulk(&evicted_items[1]), b"*");
    assert_eq!(
        resp_socket_bulk(&evicted_items[2]),
        b"__keyevent@0__:evicted"
    );
    assert_eq!(resp_socket_bulk(&evicted_items[3]), b"foo");

    send_and_expect(
        &mut writer,
        &encode_resp_command(&[b"CONFIG", b"SET", b"maxmemory", b"0"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut writer,
        &encode_resp_command(&[b"CONFIG", b"SET", b"maxmemory-policy", b"noeviction"]),
        b"+OK\r\n",
    )
    .await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn subscribed_mode_resp2_after_hello2_rejects_regular_commands_like_external_redis_cli_scenario()
 {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let mut client = TcpStream::connect(addr).await.unwrap();
    let timeout = Duration::from_secs(5);

    send_hello_and_drain(&mut client, b"3").await;

    let first_subscribe = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"SUBSCRIBE", b"ch1", b"ch2", b"ch3"]),
        timeout,
    )
    .await;
    let first_subscribe_items = resp_socket_array(&first_subscribe);
    assert_eq!(resp_socket_bulk(&first_subscribe_items[0]), b"subscribe");
    assert_eq!(resp_socket_bulk(&first_subscribe_items[1]), b"ch1");
    assert_eq!(resp_socket_integer(&first_subscribe_items[2]), 1);

    let second_subscribe = read_resp_value_with_timeout(&mut client, timeout).await;
    let second_subscribe_items = resp_socket_array(&second_subscribe);
    assert_eq!(resp_socket_bulk(&second_subscribe_items[0]), b"subscribe");
    assert_eq!(resp_socket_bulk(&second_subscribe_items[1]), b"ch2");
    assert_eq!(resp_socket_integer(&second_subscribe_items[2]), 2);

    let third_subscribe = read_resp_value_with_timeout(&mut client, timeout).await;
    let third_subscribe_items = resp_socket_array(&third_subscribe);
    assert_eq!(resp_socket_bulk(&third_subscribe_items[0]), b"subscribe");
    assert_eq!(resp_socket_bulk(&third_subscribe_items[1]), b"ch3");
    assert_eq!(resp_socket_integer(&third_subscribe_items[2]), 3);

    let publish_count = send_and_read_integer(
        &mut client,
        &encode_resp_command(&[b"PUBLISH", b"ch2", b"hello"]),
        timeout,
    )
    .await;
    assert_eq!(publish_count, 1);

    let published_message = read_resp_value_with_timeout(&mut client, timeout).await;
    let published_message_items = resp_socket_array(&published_message);
    assert_eq!(resp_socket_bulk(&published_message_items[0]), b"message");
    assert_eq!(resp_socket_bulk(&published_message_items[1]), b"ch2");
    assert_eq!(resp_socket_bulk(&published_message_items[2]), b"hello");

    let first_unsubscribe = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"UNSUBSCRIBE", b"ch1", b"ch2"]),
        timeout,
    )
    .await;
    let first_unsubscribe_items = resp_socket_array(&first_unsubscribe);
    assert_eq!(
        resp_socket_bulk(&first_unsubscribe_items[0]),
        b"unsubscribe"
    );
    assert_eq!(resp_socket_bulk(&first_unsubscribe_items[1]), b"ch1");
    assert_eq!(resp_socket_integer(&first_unsubscribe_items[2]), 2);

    let second_unsubscribe = read_resp_value_with_timeout(&mut client, timeout).await;
    let second_unsubscribe_items = resp_socket_array(&second_unsubscribe);
    assert_eq!(
        resp_socket_bulk(&second_unsubscribe_items[0]),
        b"unsubscribe"
    );
    assert_eq!(resp_socket_bulk(&second_unsubscribe_items[1]), b"ch2");
    assert_eq!(resp_socket_integer(&second_unsubscribe_items[2]), 1);

    send_hello_and_drain(&mut client, b"2").await;
    client
        .write_all(&encode_resp_command(&[b"get", b"k"]))
        .await
        .unwrap();
    let get_error = read_resp_line_with_timeout(&mut client, timeout).await;
    assert_eq!(
        get_error,
        b"-ERR Can't execute 'get': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context",
    );

    let ping_in_subscribed_resp2 =
        send_and_read_resp_value(&mut client, &encode_resp_command(&[b"PING"]), timeout).await;
    let ping_in_subscribed_resp2_items = resp_socket_array(&ping_in_subscribed_resp2);
    assert_eq!(ping_in_subscribed_resp2_items.len(), 2);
    assert_eq!(
        resp_socket_bulk(&ping_in_subscribed_resp2_items[0]),
        b"pong"
    );
    assert_eq!(resp_socket_bulk(&ping_in_subscribed_resp2_items[1]), b"");

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"RESET"]),
        b"+RESET\r\n",
    )
    .await;
    send_and_expect(&mut client, &encode_resp_command(&[b"PING"]), b"+PONG\r\n").await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn redis_cli_hint_suite_matches_external_scenarios_when_repo_cli_is_available() {
    let Some(redis_cli) = runnable_repo_redis_cli() else {
        return;
    };
    let Some(hint_suite) = redis_cli_hint_suite_path() else {
        return;
    };

    let (addr, shutdown_tx, server) = start_test_server().await;
    wait_for_server_ping(addr).await;
    let host = "127.0.0.1".to_string();
    let port = addr.port().to_string();

    let latest_server =
        run_redis_cli_hint_suite(&redis_cli, &hint_suite, &["-h", &host, "-p", &port]).await;
    assert_redis_cli_hint_suite_success(&latest_server, "latest server hint suite");

    let no_server = run_redis_cli_hint_suite(&redis_cli, &hint_suite, &["-p", "123"]).await;
    assert_redis_cli_hint_suite_success(&no_server, "no server hint suite");

    let mut admin = TcpStream::connect(addr).await.unwrap();
    send_and_expect(
        &mut admin,
        &encode_resp_command(&[
            b"ACL",
            b"SETUSER",
            b"clitest",
            b"on",
            b"nopass",
            b"+@all",
            b"-command|docs",
        ]),
        b"+OK\r\n",
    )
    .await;

    let old_server = run_redis_cli_hint_suite(
        &redis_cli,
        &hint_suite,
        &[
            "-h",
            &host,
            "-p",
            &port,
            "--user",
            "clitest",
            "-a",
            "nopass",
            "--no-auth-warning",
        ],
    )
    .await;
    assert_redis_cli_hint_suite_success(&old_server, "old server hint suite");

    let deleted_users = send_and_read_integer(
        &mut admin,
        &encode_resp_command(&[b"ACL", b"DELUSER", b"clitest"]),
        Duration::from_secs(5),
    )
    .await;
    assert!(
        deleted_users == 0 || deleted_users == 1,
        "ACL DELUSER cleanup should return 0 or 1, got {deleted_users}"
    );

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn sync_client_appears_in_info_replication_and_client_kill_type_slave_disconnects_it() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    wait_for_server_ping(addr).await;

    let mut admin = TcpStream::connect(addr).await.unwrap();
    let mut replica_stream = TcpStream::connect(addr).await.unwrap();
    replica_stream.write_all(b"SYNC\r\n").await.unwrap();

    let header = read_resp_line_with_timeout(&mut replica_stream, Duration::from_secs(1)).await;
    assert!(
        header.starts_with(b"$"),
        "SYNC response must start with bulk RDB length, got: {}",
        String::from_utf8_lossy(&header)
    );
    let payload_len = std::str::from_utf8(&header[1..])
        .unwrap()
        .parse::<usize>()
        .unwrap();
    let _ = read_exact_with_timeout(&mut replica_stream, payload_len, Duration::from_secs(1)).await;

    let info_payload = wait_for_replica_info_line(&mut admin, 1, Duration::from_secs(5)).await;
    let info_text = String::from_utf8_lossy(&info_payload);
    assert!(
        info_text.contains("slave0:ip=127.0.0.1"),
        "replication info should expose replica address, got: {info_text}"
    );

    let killed = send_and_read_integer(
        &mut admin,
        &encode_resp_command(&[b"CLIENT", b"KILL", b"TYPE", b"SLAVE"]),
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(killed, 1);

    let mut eof_probe = [0u8; 1];
    let read_result =
        tokio::time::timeout(Duration::from_secs(5), replica_stream.read(&mut eof_probe)).await;
    match read_result {
        Ok(Ok(0)) => {}
        Ok(Err(error))
            if matches!(
                error.kind(),
                std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
            ) => {}
        other => {
            panic!("replica stream did not disconnect after CLIENT KILL TYPE SLAVE: {other:?}")
        }
    }

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn min_replicas_to_write_tracks_live_replica_clients_after_client_kill_type_slave() {
    let (addr, shutdown_tx, server) = start_test_server_with_scripting_enabled().await;
    wait_for_server_ping(addr).await;

    let mut admin = TcpStream::connect(addr).await.unwrap();
    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"SET", b"x", b"some value"]),
        b"+OK\r\n",
    )
    .await;
    let mut replica_stream = TcpStream::connect(addr).await.unwrap();
    replica_stream.write_all(b"SYNC\r\n").await.unwrap();

    let header = read_resp_line_with_timeout(&mut replica_stream, Duration::from_secs(1)).await;
    assert!(
        header.starts_with(b"$"),
        "SYNC response must start with bulk RDB length, got: {}",
        String::from_utf8_lossy(&header)
    );
    let payload_len = std::str::from_utf8(&header[1..])
        .unwrap()
        .parse::<usize>()
        .unwrap();
    let _ = read_exact_with_timeout(&mut replica_stream, payload_len, Duration::from_secs(1)).await;

    let _ = wait_for_replica_info_line(&mut admin, 1, Duration::from_secs(5)).await;

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"CONFIG", b"SET", b"min-replicas-to-write", b"1"]),
        b"+OK\r\n",
    )
    .await;

    let killed = send_and_read_integer(
        &mut admin,
        &encode_resp_command(&[b"CLIENT", b"KILL", b"TYPE", b"SLAVE"]),
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(killed, 1);

    let mut eof_probe = [0u8; 1];
    let read_result =
        tokio::time::timeout(Duration::from_secs(5), replica_stream.read(&mut eof_probe)).await;
    match read_result {
        Ok(Ok(0)) => {}
        Ok(Err(error))
            if matches!(
                error.kind(),
                std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
            ) => {}
        other => {
            panic!("replica stream did not disconnect after CLIENT KILL TYPE SLAVE: {other:?}")
        }
    }

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let payload = send_and_read_bulk_payload(
            &mut admin,
            &encode_resp_command(&[b"INFO", b"REPLICATION"]),
            Duration::from_secs(1),
        )
        .await;
        if read_info_u64(&payload, "connected_slaves") == Some(0) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for connected_slaves=0; last payload: {}",
            String::from_utf8_lossy(&payload)
        );
        sleep(Duration::from_millis(10)).await;
    }

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[
            b"EVAL",
            b"#!lua flags=no-writes\nreturn redis.call('get','x')",
            b"1",
            b"x",
        ]),
        b"$10\r\nsome value\r\n",
    )
    .await;
    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"EVAL", b"return redis.call('get','x')", b"1", b"x"]),
        b"$10\r\nsome value\r\n",
    )
    .await;
    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"EVAL", b"#!lua\nreturn redis.call('get','x')", b"1", b"x"]),
        b"-NOREPLICAS Not enough good replicas to write.\r\n",
    )
    .await;
    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"EVAL", b"return redis.call('set','x',1)", b"1", b"x"]),
        b"-NOREPLICAS Not enough good replicas to write.\r\n",
    )
    .await;

    send_and_expect(
        &mut admin,
        &encode_resp_command(&[b"CONFIG", b"SET", b"min-replicas-to-write", b"0"]),
        b"+OK\r\n",
    )
    .await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn redis_cli_replica_mode_matches_external_scenario_when_repo_cli_is_available() {
    let Some(redis_cli) = runnable_repo_redis_cli() else {
        return;
    };

    let (addr, shutdown_tx, server) = start_test_server().await;
    wait_for_server_ping(addr).await;
    let host = "127.0.0.1".to_string();
    let port = addr.port().to_string();

    let mut replica_cli = TokioCommand::new(redis_cli);
    replica_cli
        .arg("-h")
        .arg(&host)
        .arg("-p")
        .arg(&port)
        .arg("--replica")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut replica_cli = replica_cli.spawn().unwrap();

    let stdout_buffer = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let stderr_buffer = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let stdout_task = tokio::spawn(collect_process_output(
        replica_cli.stdout.take().unwrap(),
        Arc::clone(&stdout_buffer),
    ));
    let stderr_task = tokio::spawn(collect_process_output(
        replica_cli.stderr.take().unwrap(),
        Arc::clone(&stderr_buffer),
    ));

    let mut admin = TcpStream::connect(addr).await.unwrap();
    let _ = wait_for_replica_info_line(&mut admin, 1, Duration::from_secs(5)).await;

    for index in 0..100 {
        let value = format!("test-value-{index}");
        send_and_expect(
            &mut admin,
            &encode_resp_command(&[b"SET", b"test-key", value.as_bytes()]),
            b"+OK\r\n",
        )
        .await;
    }

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let stdout = stdout_buffer.lock().await.clone();
        let stderr = stderr_buffer.lock().await.clone();
        let combined = [stdout.as_slice(), stderr.as_slice()].concat();
        if combined
            .windows(b"test-value-99".len())
            .any(|window| window == b"test-value-99")
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "redis-cli --replica did not print the replicated command stream\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&stdout),
            String::from_utf8_lossy(&stderr)
        );
        sleep(Duration::from_millis(10)).await;
    }

    let killed = send_and_read_integer(
        &mut admin,
        &encode_resp_command(&[b"CLIENT", b"KILL", b"TYPE", b"SLAVE"]),
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(killed, 1);

    let status = tokio::time::timeout(Duration::from_secs(5), replica_cli.wait())
        .await
        .unwrap()
        .unwrap();
    stdout_task.await.unwrap();
    stderr_task.await.unwrap();
    let stdout = stdout_buffer.lock().await.clone();
    let stderr = stderr_buffer.lock().await.clone();
    let combined = [stdout.as_slice(), stderr.as_slice()].concat();
    let combined_text = String::from_utf8_lossy(&combined);
    assert!(
        combined_text.contains("test-value-99"),
        "redis-cli --replica output should contain the latest replicated value\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&stdout),
        String::from_utf8_lossy(&stderr)
    );
    assert!(
        combined_text.contains("Server closed the connection"),
        "redis-cli --replica should report server-side disconnect after CLIENT KILL TYPE slave\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&stdout),
        String::from_utf8_lossy(&stderr)
    );
    assert!(
        !status.success(),
        "redis-cli --replica is expected to exit non-zero after server-side disconnect"
    );

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn debug_populate_matches_external_redis_cli_rdb_dump_precondition() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    wait_for_server_ping(addr).await;

    let mut client = TcpStream::connect(addr).await.unwrap();
    let timeout = Duration::from_secs(5);

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"key:1", b"keep"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEBUG", b"POPULATE", b"3", b"key", b"4"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"GET", b"key:0"]),
        b"$4\r\nvalu\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"GET", b"key:1"]),
        b"$4\r\nkeep\r\n",
    )
    .await;
    let dbsize =
        send_and_read_integer(&mut client, &encode_resp_command(&[b"DBSIZE"]), timeout).await;
    assert_eq!(dbsize, 3);

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn config_get_dir_returns_absolute_path_for_external_redis_cli_rdb_dump_scenario() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    wait_for_server_ping(addr).await;

    let mut client = TcpStream::connect(addr).await.unwrap();
    let response = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"CONFIG", b"GET", b"dir"]),
        Duration::from_secs(5),
    )
    .await;
    let RespSocketValue::Array(items) = response else {
        panic!("expected CONFIG GET dir array response");
    };
    assert_eq!(items.len(), 2, "expected key/value pair for CONFIG GET dir");
    assert_eq!(resp_socket_bulk(&items[0]), b"dir");
    let dir_value = resp_socket_bulk(&items[1]);
    let dir_text = String::from_utf8_lossy(dir_value).into_owned();
    assert!(
        std::path::Path::new(dir_text.as_str()).is_absolute(),
        "CONFIG GET dir must return an absolute path, got {:?}",
        dir_text
    );

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn sync_without_rdb_only_returns_redis_rdb_payload_for_real_replicas() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    wait_for_server_ping(addr).await;

    let mut replica_stream = TcpStream::connect(addr).await.unwrap();
    replica_stream.write_all(b"SYNC\r\n").await.unwrap();
    let snapshot_payload =
        read_sync_snapshot_payload_with_timeout(&mut replica_stream, Duration::from_secs(5)).await;
    assert!(
        snapshot_payload.starts_with(b"REDIS0011"),
        "expected Redis RDB header for non-rdb-only sync, got: {:?}",
        &snapshot_payload[..snapshot_payload.len().min(16)]
    );
    assert!(
        !snapshot_payload.starts_with(b"GRSNAP"),
        "non-rdb-only sync must not return Garnet debug snapshot payload"
    );

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn sync_snapshot_includes_preexisting_string_dataset_for_real_replicas() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    wait_for_server_ping(addr).await;

    let mut client = TcpStream::connect(addr).await.unwrap();
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"seed:key", b"seed:value"]),
        b"+OK\r\n",
    )
    .await;

    let mut replica_stream = TcpStream::connect(addr).await.unwrap();
    replica_stream.write_all(b"SYNC\r\n").await.unwrap();
    let snapshot_payload =
        read_sync_snapshot_payload_with_timeout(&mut replica_stream, Duration::from_secs(5)).await;

    assert!(
        snapshot_payload.starts_with(b"REDIS0011"),
        "expected Redis RDB header for non-rdb-only sync, got: {:?}",
        &snapshot_payload[..snapshot_payload.len().min(16)]
    );
    assert!(
        snapshot_payload
            .windows(b"seed:key".len())
            .any(|window| window == b"seed:key"),
        "preexisting string key must be present in full-sync RDB payload"
    );
    assert!(
        snapshot_payload
            .windows(b"seed:value".len())
            .any(|window| window == b"seed:value"),
        "preexisting string value must be present in full-sync RDB payload"
    );

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"GET", b"seed:key"]),
        b"$10\r\nseed:value\r\n",
    )
    .await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn sync_snapshot_rdb_payload_passes_redis_check_rdb_when_available() {
    let probe = Command::new("sh")
        .args(["-c", "command -v redis-check-rdb"])
        .output()
        .unwrap();
    if !probe.status.success() {
        return;
    }
    let redis_check_rdb = String::from_utf8_lossy(&probe.stdout).trim().to_owned();

    let (addr, shutdown_tx, server) = start_test_server().await;
    wait_for_server_ping(addr).await;

    let mut client = TcpStream::connect(addr).await.unwrap();
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"seed:key", b"seed:value"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[
            b"HSET",
            b"seed:hash",
            b"field1",
            b"value1",
            b"field2",
            b"value2",
        ]),
        b":2\r\n",
    )
    .await;
    let future_millis = current_unix_time_millis_for_test() + 60_000;
    let future_millis_text = future_millis.to_string();
    send_and_expect(
        &mut client,
        &encode_resp_command(&[
            b"HPEXPIREAT",
            b"seed:hash",
            future_millis_text.as_bytes(),
            b"FIELDS",
            b"1",
            b"field1",
        ]),
        b"*1\r\n:1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SADD", b"seed:set", b"alpha", b"beta"]),
        b":2\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"ZADD", b"seed:zset", b"1", b"one", b"2", b"two"]),
        b":2\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"RPUSH", b"seed:list", b"alpha", b"beta"]),
        b":2\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"XADD", b"seed:stream", b"1-0", b"f", b"v1"]),
        b"$3\r\n1-0\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"XADD", b"seed:stream", b"2-0", b"f", b"v2"]),
        b"$3\r\n2-0\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"XGROUP", b"CREATE", b"seed:stream", b"group1", b"0"]),
        b"+OK\r\n",
    )
    .await;
    let _ = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[
            b"XREADGROUP",
            b"GROUP",
            b"group1",
            b"consumer1",
            b"COUNT",
            b"1",
            b"STREAMS",
            b"seed:stream",
            b">",
        ]),
        Duration::from_secs(2),
    )
    .await;

    let mut replica_stream = TcpStream::connect(addr).await.unwrap();
    replica_stream.write_all(b"SYNC\r\n").await.unwrap();
    let snapshot_payload =
        read_sync_snapshot_payload_with_timeout(&mut replica_stream, Duration::from_secs(5)).await;

    let temp_dir = unique_test_temp_dir("sync-snapshot-rdb-check");
    std::fs::create_dir_all(&temp_dir).unwrap();
    let snapshot_path = temp_dir.join("dump.rdb");
    std::fs::write(&snapshot_path, snapshot_payload).unwrap();

    let output = Command::new(redis_check_rdb)
        .arg(&snapshot_path)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "redis-check-rdb rejected generated full-sync snapshot\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

async fn run_sync_rdb_dump_and_debug_reload_scenario(
    populate_count: u64,
    value_size: usize,
    selected_db: u16,
) {
    let _serial = lock_scripting_test_serial().await;
    let (addr, shutdown_tx, server) = start_test_server_with_scripting_enabled().await;
    wait_for_server_ping(addr).await;

    let timeout = Duration::from_secs(120);
    let temp_dir = unique_test_temp_dir("redis-cli-rdb-dump-full");
    let dump_path = temp_dir.join("dump.rdb");
    let cli_dump_path = temp_dir.join("cli.rdb");

    let mut client = TcpStream::connect(addr).await.unwrap();
    let mut sync_client = TcpStream::connect(addr).await.unwrap();

    assert_eq!(
        resp_socket_bulk(
            &send_and_read_resp_value(
                &mut client,
                &encode_resp_command(&[b"CONFIG", b"SET", b"databases", b"16"]),
                timeout,
            )
            .await
        ),
        b"OK"
    );
    if selected_db != 0 {
        let selected_db_text = selected_db.to_string();
        assert_eq!(
            resp_socket_bulk(
                &send_and_read_resp_value(
                    &mut client,
                    &encode_resp_command(&[b"SELECT", selected_db_text.as_bytes()]),
                    timeout,
                )
                .await
            ),
            b"OK"
        );
    }
    assert_eq!(
        resp_socket_bulk(
            &send_and_read_resp_value(
                &mut client,
                &encode_resp_command(&[
                    b"CONFIG",
                    b"SET",
                    b"dir",
                    temp_dir.to_string_lossy().as_bytes()
                ]),
                timeout,
            )
            .await
        ),
        b"OK"
    );
    assert_eq!(
        resp_socket_bulk(
            &send_and_read_resp_value(&mut client, &encode_resp_command(&[b"FLUSHDB"]), timeout,)
                .await
        ),
        b"OK"
    );
    assert_eq!(
        resp_socket_bulk(
            &send_and_read_resp_value(
                &mut client,
                &encode_resp_command(&[b"FUNCTION", b"FLUSH"]),
                timeout,
            )
            .await
        ),
        b"OK"
    );
    assert_eq!(
        resp_socket_bulk(
            &send_and_read_resp_value(
                &mut client,
                &encode_resp_command(&[
                    b"DEBUG",
                    b"POPULATE",
                    populate_count.to_string().as_bytes(),
                    b"key",
                    value_size.to_string().as_bytes(),
                ]),
                timeout,
            )
            .await
        ),
        b"OK"
    );
    assert_eq!(
        resp_socket_bulk(
            &send_and_read_resp_value(
                &mut client,
                &encode_resp_command(&[
                    b"FUNCTION",
                    b"LOAD",
                    b"#!lua name=lib1\nredis.register_function('func1', function() return 123 end)",
                ]),
                timeout,
            )
            .await
        ),
        b"lib1"
    );

    send_and_expect(
        &mut sync_client,
        &encode_resp_command(&[b"REPLCONF", b"rdb-only", b"1"]),
        b"+OK\r\n",
    )
    .await;
    sync_client.write_all(b"SYNC\r\n").await.unwrap();
    let snapshot_payload = read_sync_snapshot_payload_with_timeout(&mut sync_client, timeout).await;
    std::fs::write(&cli_dump_path, &snapshot_payload).unwrap();
    std::fs::rename(&cli_dump_path, &dump_path).unwrap();

    assert_eq!(
        resp_socket_bulk(
            &send_and_read_resp_value(
                &mut client,
                &encode_resp_command(&[b"SET", b"should-not-exist", b"1"]),
                timeout,
            )
            .await
        ),
        b"OK"
    );
    assert_eq!(
        resp_socket_bulk(
            &send_and_read_resp_value(
                &mut client,
                &encode_resp_command(&[
                    b"FUNCTION",
                    b"LOAD",
                    b"#!lua name=should_not_exist_func\nredis.register_function('should_not_exist_func', function() return 456 end)",
                ]),
                timeout,
            )
            .await
        ),
        b"should_not_exist_func"
    );
    assert_eq!(
        resp_socket_bulk(
            &send_and_read_resp_value(
                &mut client,
                &encode_resp_command(&[b"DEBUG", b"RELOAD", b"NOSAVE"]),
                timeout,
            )
            .await
        ),
        b"OK"
    );

    assert!(matches!(
        send_and_read_resp_value(
            &mut client,
            &encode_resp_command(&[b"GET", b"should-not-exist"]),
            timeout,
        )
        .await,
        RespSocketValue::Null
    ));
    let function_list = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"FUNCTION", b"LIST"]),
        timeout,
    )
    .await;
    assert!(resp_socket_contains_bulk(&function_list, b"lib1"));
    assert!(!resp_socket_contains_bulk(
        &function_list,
        b"should_not_exist_func"
    ));
    assert_eq!(
        resp_socket_integer(
            &send_and_read_resp_value(&mut client, &encode_resp_command(&[b"DBSIZE"]), timeout,)
                .await
        ),
        i64::try_from(populate_count).unwrap()
    );
    if selected_db != 0 {
        assert_eq!(
            resp_socket_bulk(
                &send_and_read_resp_value(
                    &mut client,
                    &encode_resp_command(&[b"SELECT", b"0"]),
                    timeout,
                )
                .await
            ),
            b"OK"
        );
        assert_eq!(
            resp_socket_integer(
                &send_and_read_resp_value(
                    &mut client,
                    &encode_resp_command(&[b"DBSIZE"]),
                    timeout,
                )
                .await
            ),
            0
        );
    }

    let _ = std::fs::remove_file(&dump_path);
    let _ = std::fs::remove_dir_all(&temp_dir);
    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn sync_rdb_dump_and_debug_reload_round_trip_smoke() {
    run_sync_rdb_dump_and_debug_reload_scenario(1_000, 128, 0).await;
}

#[tokio::test]
async fn sync_rdb_dump_and_debug_reload_round_trip_smoke_in_nonzero_db() {
    run_sync_rdb_dump_and_debug_reload_scenario(1_000, 128, 9).await;
}

#[tokio::test]
#[ignore = "exact external redis-cli dump scenario; run targeted"]
async fn sync_rdb_dump_and_debug_reload_match_external_redis_cli_scenario() {
    run_sync_rdb_dump_and_debug_reload_scenario(100_000, 1_000, 0).await;
}

#[tokio::test]
#[ignore = "exact external redis-cli dump scenario in non-zero db; run targeted"]
async fn sync_rdb_dump_and_debug_reload_match_external_redis_cli_scenario_in_nonzero_db() {
    run_sync_rdb_dump_and_debug_reload_scenario(100_000, 1_000, 9).await;
}

async fn run_sync_functions_rdb_dump_and_debug_reload_scenario(
    populate_count: u64,
    value_size: usize,
    selected_db: u16,
) {
    let _serial = lock_scripting_test_serial().await;
    let (addr, shutdown_tx, server) = start_test_server_with_scripting_enabled().await;
    wait_for_server_ping(addr).await;

    let timeout = Duration::from_secs(120);
    let temp_dir = unique_test_temp_dir("redis-cli-rdb-dump-functions");
    let dump_path = temp_dir.join("dump.rdb");
    let cli_dump_path = temp_dir.join("cli.rdb");

    let mut client = TcpStream::connect(addr).await.unwrap();
    let mut sync_client = TcpStream::connect(addr).await.unwrap();

    assert_eq!(
        resp_socket_bulk(
            &send_and_read_resp_value(
                &mut client,
                &encode_resp_command(&[b"CONFIG", b"SET", b"databases", b"16"]),
                timeout,
            )
            .await
        ),
        b"OK"
    );
    if selected_db != 0 {
        let selected_db_text = selected_db.to_string();
        assert_eq!(
            resp_socket_bulk(
                &send_and_read_resp_value(
                    &mut client,
                    &encode_resp_command(&[b"SELECT", selected_db_text.as_bytes()]),
                    timeout,
                )
                .await
            ),
            b"OK"
        );
    }
    assert_eq!(
        resp_socket_bulk(
            &send_and_read_resp_value(
                &mut client,
                &encode_resp_command(&[
                    b"CONFIG",
                    b"SET",
                    b"dir",
                    temp_dir.to_string_lossy().as_bytes()
                ]),
                timeout,
            )
            .await
        ),
        b"OK"
    );
    assert_eq!(
        resp_socket_bulk(
            &send_and_read_resp_value(&mut client, &encode_resp_command(&[b"FLUSHDB"]), timeout,)
                .await
        ),
        b"OK"
    );
    assert_eq!(
        resp_socket_bulk(
            &send_and_read_resp_value(
                &mut client,
                &encode_resp_command(&[b"FUNCTION", b"FLUSH"]),
                timeout,
            )
            .await
        ),
        b"OK"
    );
    assert_eq!(
        resp_socket_bulk(
            &send_and_read_resp_value(
                &mut client,
                &encode_resp_command(&[
                    b"DEBUG",
                    b"POPULATE",
                    populate_count.to_string().as_bytes(),
                    b"key",
                    value_size.to_string().as_bytes(),
                ]),
                timeout,
            )
            .await
        ),
        b"OK"
    );
    assert_eq!(
        resp_socket_bulk(
            &send_and_read_resp_value(
                &mut client,
                &encode_resp_command(&[
                    b"FUNCTION",
                    b"LOAD",
                    b"#!lua name=lib1\nredis.register_function('func1', function() return 123 end)",
                ]),
                timeout,
            )
            .await
        ),
        b"lib1"
    );

    send_and_expect(
        &mut sync_client,
        &encode_resp_command(&[b"REPLCONF", b"rdb-only", b"1"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut sync_client,
        &encode_resp_command(&[b"REPLCONF", b"rdb-filter-only", b"functions"]),
        b"+OK\r\n",
    )
    .await;
    sync_client.write_all(b"SYNC\r\n").await.unwrap();
    let snapshot_payload = read_sync_snapshot_payload_with_timeout(&mut sync_client, timeout).await;
    std::fs::write(&cli_dump_path, &snapshot_payload).unwrap();
    std::fs::rename(&cli_dump_path, &dump_path).unwrap();

    assert_eq!(
        resp_socket_bulk(
            &send_and_read_resp_value(
                &mut client,
                &encode_resp_command(&[b"SET", b"should-not-exist", b"1"]),
                timeout,
            )
            .await
        ),
        b"OK"
    );
    assert_eq!(
        resp_socket_bulk(
            &send_and_read_resp_value(
                &mut client,
                &encode_resp_command(&[
                    b"FUNCTION",
                    b"LOAD",
                    b"#!lua name=should_not_exist_func\nredis.register_function('should_not_exist_func', function() return 456 end)",
                ]),
                timeout,
            )
            .await
        ),
        b"should_not_exist_func"
    );
    assert_eq!(
        resp_socket_bulk(
            &send_and_read_resp_value(
                &mut client,
                &encode_resp_command(&[b"DEBUG", b"RELOAD", b"NOSAVE"]),
                timeout,
            )
            .await
        ),
        b"OK"
    );

    assert!(matches!(
        send_and_read_resp_value(
            &mut client,
            &encode_resp_command(&[b"GET", b"should-not-exist"]),
            timeout,
        )
        .await,
        RespSocketValue::Null
    ));
    let function_list = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"FUNCTION", b"LIST"]),
        timeout,
    )
    .await;
    assert!(resp_socket_contains_bulk(&function_list, b"lib1"));
    assert!(!resp_socket_contains_bulk(
        &function_list,
        b"should_not_exist_func"
    ));
    assert_eq!(
        resp_socket_integer(
            &send_and_read_resp_value(&mut client, &encode_resp_command(&[b"DBSIZE"]), timeout,)
                .await
        ),
        0
    );

    let _ = std::fs::remove_file(&dump_path);
    let _ = std::fs::remove_dir_all(&temp_dir);
    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn sync_functions_rdb_dump_and_debug_reload_round_trip_smoke() {
    run_sync_functions_rdb_dump_and_debug_reload_scenario(1_000, 128, 0).await;
}

#[tokio::test]
async fn sync_functions_rdb_dump_and_debug_reload_round_trip_smoke_in_nonzero_db() {
    run_sync_functions_rdb_dump_and_debug_reload_scenario(1_000, 128, 9).await;
}

#[tokio::test]
#[ignore = "exact external redis-cli dump scenario; run targeted"]
async fn sync_functions_rdb_dump_and_debug_reload_match_external_redis_cli_scenario() {
    run_sync_functions_rdb_dump_and_debug_reload_scenario(100_000, 1_000, 0).await;
}

#[tokio::test]
#[ignore = "exact external redis-cli dump scenario in non-zero db; run targeted"]
async fn sync_functions_rdb_dump_and_debug_reload_match_external_redis_cli_scenario_in_nonzero_db()
{
    run_sync_functions_rdb_dump_and_debug_reload_scenario(100_000, 1_000, 9).await;
}

#[tokio::test]
#[ignore = "exact external Redis replica interop scenario; run targeted"]
async fn garnet_primary_to_docker_redis_replica_reaches_link_up_and_replicates_post_sync_writes() {
    if !docker_available() {
        return;
    }

    let (addr, shutdown_tx, server) = start_test_server().await;
    wait_for_server_ping(addr).await;

    let redis_port = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let container_name = format!("garnet-repl-redis-{}", redis_port);
    let _container = DockerContainerGuard::new(container_name.clone());

    let run_output = Command::new("docker")
        .args([
            "run",
            "-d",
            "--rm",
            "--name",
            container_name.as_str(),
            "-p",
            &format!("{redis_port}:6379"),
            "--add-host=host.docker.internal:host-gateway",
            "redis:7.2-alpine",
            "redis-server",
            "--port",
            "6379",
            "--save",
            "",
            "--appendonly",
            "no",
        ])
        .output()
        .unwrap();
    assert!(
        run_output.status.success(),
        "failed to start docker redis replica\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&run_output.stdout),
        String::from_utf8_lossy(&run_output.stderr)
    );

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match TcpStream::connect(("127.0.0.1", redis_port)).await {
            Ok(mut redis_admin) => {
                redis_admin
                    .write_all(&encode_resp_command(&[b"PING"]))
                    .await
                    .unwrap();
                let reply =
                    read_resp_line_with_timeout(&mut redis_admin, Duration::from_secs(1)).await;
                if reply == b"+PONG" {
                    break;
                }
            }
            Err(_) => {}
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for docker redis replica to accept PING on port {redis_port}"
        );
        sleep(Duration::from_millis(100)).await;
    }

    let mut redis_admin = TcpStream::connect(("127.0.0.1", redis_port)).await.unwrap();
    let garnet_port = addr.port().to_string();
    send_and_expect(
        &mut redis_admin,
        &encode_resp_command(&[
            b"REPLICAOF",
            b"host.docker.internal",
            garnet_port.as_bytes(),
        ]),
        b"+OK\r\n",
    )
    .await;

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let info = send_and_read_bulk_payload(
            &mut redis_admin,
            &encode_resp_command(&[b"INFO", b"REPLICATION"]),
            Duration::from_secs(2),
        )
        .await;
        let info_text = String::from_utf8_lossy(&info);
        if info_text.contains("master_link_status:up\r\n") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "docker redis replica did not reach master_link_status=up\n{info_text}"
        );
        sleep(Duration::from_millis(250)).await;
    }

    let mut garnet_client = TcpStream::connect(addr).await.unwrap();
    send_and_expect(
        &mut garnet_client,
        &encode_resp_command(&[b"SET", b"repl:probe", b"from-garnet"]),
        b"+OK\r\n",
    )
    .await;

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let value = send_and_read_resp_value(
            &mut redis_admin,
            &encode_resp_command(&[b"GET", b"repl:probe"]),
            Duration::from_secs(2),
        )
        .await;
        if matches!(&value, RespSocketValue::Bulk(bytes) if bytes == b"from-garnet") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "docker redis replica did not receive post-sync write; last response: {value:?}"
        );
        sleep(Duration::from_millis(100)).await;
    }

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
#[ignore = "exact external Redis replica interop scenario; run targeted"]
async fn garnet_primary_to_docker_redis_replica_receives_preexisting_string_dataset_on_full_sync() {
    if !docker_available() {
        return;
    }

    let (addr, shutdown_tx, server) = start_test_server().await;
    wait_for_server_ping(addr).await;

    let mut garnet_client = TcpStream::connect(addr).await.unwrap();
    send_and_expect(
        &mut garnet_client,
        &encode_resp_command(&[b"SET", b"seed:key", b"seed:value"]),
        b"+OK\r\n",
    )
    .await;

    let redis_port = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let container_name = format!("garnet-repl-redis-seed-{}", redis_port);
    let _container = DockerContainerGuard::new(container_name.clone());

    let run_output = Command::new("docker")
        .args([
            "run",
            "-d",
            "--rm",
            "--name",
            container_name.as_str(),
            "-p",
            &format!("{redis_port}:6379"),
            "--add-host=host.docker.internal:host-gateway",
            "redis:7.2-alpine",
            "redis-server",
            "--port",
            "6379",
            "--save",
            "",
            "--appendonly",
            "no",
        ])
        .output()
        .unwrap();
    assert!(
        run_output.status.success(),
        "failed to start docker redis replica\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&run_output.stdout),
        String::from_utf8_lossy(&run_output.stderr)
    );

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match TcpStream::connect(("127.0.0.1", redis_port)).await {
            Ok(mut redis_admin) => {
                redis_admin
                    .write_all(&encode_resp_command(&[b"PING"]))
                    .await
                    .unwrap();
                let reply =
                    read_resp_line_with_timeout(&mut redis_admin, Duration::from_secs(1)).await;
                if reply == b"+PONG" {
                    break;
                }
            }
            Err(_) => {}
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for docker redis replica to accept PING on port {redis_port}"
        );
        sleep(Duration::from_millis(100)).await;
    }

    let mut redis_admin = TcpStream::connect(("127.0.0.1", redis_port)).await.unwrap();
    let garnet_port = addr.port().to_string();
    send_and_expect(
        &mut redis_admin,
        &encode_resp_command(&[
            b"REPLICAOF",
            b"host.docker.internal",
            garnet_port.as_bytes(),
        ]),
        b"+OK\r\n",
    )
    .await;

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let info = send_and_read_bulk_payload(
            &mut redis_admin,
            &encode_resp_command(&[b"INFO", b"REPLICATION"]),
            Duration::from_secs(2),
        )
        .await;
        let info_text = String::from_utf8_lossy(&info);
        if info_text.contains("master_link_status:up\r\n") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "docker redis replica did not reach master_link_status=up\n{info_text}"
        );
        sleep(Duration::from_millis(250)).await;
    }

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let value = send_and_read_resp_value(
            &mut redis_admin,
            &encode_resp_command(&[b"GET", b"seed:key"]),
            Duration::from_secs(2),
        )
        .await;
        if matches!(&value, RespSocketValue::Bulk(bytes) if bytes == b"seed:value") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "docker redis replica did not receive preexisting seed dataset; last response: {value:?}"
        );
        sleep(Duration::from_millis(100)).await;
    }

    send_and_expect(
        &mut garnet_client,
        &encode_resp_command(&[b"SET", b"repl:probe", b"from-garnet"]),
        b"+OK\r\n",
    )
    .await;

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let value = send_and_read_resp_value(
            &mut redis_admin,
            &encode_resp_command(&[b"GET", b"repl:probe"]),
            Duration::from_secs(2),
        )
        .await;
        if matches!(&value, RespSocketValue::Bulk(bytes) if bytes == b"from-garnet") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "docker redis replica did not receive post-sync write after seeded full sync; last response: {value:?}"
        );
        sleep(Duration::from_millis(100)).await;
    }

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
#[ignore = "exact external Redis replica interop scenario; run targeted"]
async fn garnet_primary_to_docker_redis_replica_receives_preexisting_object_dataset_on_full_sync() {
    if !docker_available() {
        return;
    }

    let (addr, shutdown_tx, server) = start_test_server().await;
    wait_for_server_ping(addr).await;
    let large_list_value = vec![b'x'; 9000];

    let mut garnet_client = TcpStream::connect(addr).await.unwrap();
    send_and_expect(
        &mut garnet_client,
        &encode_resp_command(&[
            b"HSET",
            b"seed:hash",
            b"field1",
            b"value1",
            b"field2",
            b"value2",
        ]),
        b":2\r\n",
    )
    .await;
    send_and_expect(
        &mut garnet_client,
        &encode_resp_command(&[b"PEXPIRE", b"seed:hash", b"60000"]),
        b":1\r\n",
    )
    .await;
    send_and_expect(
        &mut garnet_client,
        &encode_resp_command(&[b"SADD", b"seed:set", b"alpha", b"beta", b"gamma"]),
        b":3\r\n",
    )
    .await;
    send_and_expect(
        &mut garnet_client,
        &encode_resp_command(&[b"ZADD", b"seed:zset", b"1", b"one", b"2", b"two"]),
        b":2\r\n",
    )
    .await;
    send_and_expect(
        &mut garnet_client,
        &encode_resp_command(&[
            b"RPUSH",
            b"seed:list",
            b"alpha",
            large_list_value.as_slice(),
            b"omega",
        ]),
        b":3\r\n",
    )
    .await;

    let redis_port = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let container_name = format!("garnet-repl-redis-objects-{}", redis_port);
    let _container = DockerContainerGuard::new(container_name.clone());

    let run_output = Command::new("docker")
        .args([
            "run",
            "-d",
            "--rm",
            "--name",
            container_name.as_str(),
            "-p",
            &format!("{redis_port}:6379"),
            "--add-host=host.docker.internal:host-gateway",
            "redis:7.2-alpine",
            "redis-server",
            "--port",
            "6379",
            "--save",
            "",
            "--appendonly",
            "no",
        ])
        .output()
        .unwrap();
    assert!(
        run_output.status.success(),
        "failed to start docker redis replica\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&run_output.stdout),
        String::from_utf8_lossy(&run_output.stderr)
    );

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match TcpStream::connect(("127.0.0.1", redis_port)).await {
            Ok(mut redis_admin) => {
                redis_admin
                    .write_all(&encode_resp_command(&[b"PING"]))
                    .await
                    .unwrap();
                let reply =
                    read_resp_line_with_timeout(&mut redis_admin, Duration::from_secs(1)).await;
                if reply == b"+PONG" {
                    break;
                }
            }
            Err(_) => {}
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for docker redis replica to accept PING on port {redis_port}"
        );
        sleep(Duration::from_millis(100)).await;
    }

    let mut redis_admin = TcpStream::connect(("127.0.0.1", redis_port)).await.unwrap();
    let garnet_port = addr.port().to_string();
    send_and_expect(
        &mut redis_admin,
        &encode_resp_command(&[
            b"REPLICAOF",
            b"host.docker.internal",
            garnet_port.as_bytes(),
        ]),
        b"+OK\r\n",
    )
    .await;

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let info = send_and_read_bulk_payload(
            &mut redis_admin,
            &encode_resp_command(&[b"INFO", b"REPLICATION"]),
            Duration::from_secs(2),
        )
        .await;
        let info_text = String::from_utf8_lossy(&info);
        if info_text.contains("master_link_status:up\r\n") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "docker redis replica did not reach master_link_status=up\n{info_text}"
        );
        sleep(Duration::from_millis(250)).await;
    }

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let hash_value = send_and_read_resp_value(
            &mut redis_admin,
            &encode_resp_command(&[b"HGET", b"seed:hash", b"field1"]),
            Duration::from_secs(2),
        )
        .await;
        if matches!(&hash_value, RespSocketValue::Bulk(bytes) if bytes == b"value1") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "docker redis replica did not receive seeded hash dataset; last response: {hash_value:?}"
        );
        sleep(Duration::from_millis(100)).await;
    }

    assert_eq!(
        send_and_read_integer(
            &mut redis_admin,
            &encode_resp_command(&[b"SISMEMBER", b"seed:set", b"alpha"]),
            Duration::from_secs(2),
        )
        .await,
        1
    );
    assert_eq!(
        send_and_read_integer(
            &mut redis_admin,
            &encode_resp_command(&[b"SISMEMBER", b"seed:set", b"gamma"]),
            Duration::from_secs(2),
        )
        .await,
        1
    );
    let zrange = send_and_read_resp_value(
        &mut redis_admin,
        &encode_resp_command(&[b"ZRANGE", b"seed:zset", b"0", b"-1", b"WITHSCORES"]),
        Duration::from_secs(2),
    )
    .await;
    let zrange_items = resp_socket_array(&zrange);
    assert_eq!(resp_socket_bulk(&zrange_items[0]), b"one");
    assert_eq!(resp_socket_bulk(&zrange_items[1]), b"1");
    assert_eq!(resp_socket_bulk(&zrange_items[2]), b"two");
    assert_eq!(resp_socket_bulk(&zrange_items[3]), b"2");
    assert_eq!(
        send_and_read_integer(
            &mut redis_admin,
            &encode_resp_command(&[b"LLEN", b"seed:list"]),
            Duration::from_secs(2),
        )
        .await,
        3
    );
    let list_head = send_and_read_resp_value(
        &mut redis_admin,
        &encode_resp_command(&[b"LINDEX", b"seed:list", b"0"]),
        Duration::from_secs(2),
    )
    .await;
    assert!(matches!(&list_head, RespSocketValue::Bulk(bytes) if bytes == b"alpha"));
    let list_middle = send_and_read_resp_value(
        &mut redis_admin,
        &encode_resp_command(&[b"LINDEX", b"seed:list", b"1"]),
        Duration::from_secs(2),
    )
    .await;
    assert!(matches!(
        &list_middle,
        RespSocketValue::Bulk(bytes) if bytes == large_list_value.as_slice()
    ));
    let list_tail = send_and_read_resp_value(
        &mut redis_admin,
        &encode_resp_command(&[b"LINDEX", b"seed:list", b"2"]),
        Duration::from_secs(2),
    )
    .await;
    assert!(matches!(&list_tail, RespSocketValue::Bulk(bytes) if bytes == b"omega"));
    let hash_ttl = send_and_read_integer(
        &mut redis_admin,
        &encode_resp_command(&[b"PTTL", b"seed:hash"]),
        Duration::from_secs(2),
    )
    .await;
    assert!(hash_ttl > 0, "expected positive PTTL for replicated hash");

    send_and_expect(
        &mut garnet_client,
        &encode_resp_command(&[b"HSET", b"repl:hash", b"field", b"from-garnet"]),
        b":1\r\n",
    )
    .await;

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let value = send_and_read_resp_value(
            &mut redis_admin,
            &encode_resp_command(&[b"HGET", b"repl:hash", b"field"]),
            Duration::from_secs(2),
        )
        .await;
        if matches!(&value, RespSocketValue::Bulk(bytes) if bytes == b"from-garnet") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "docker redis replica did not receive post-sync object write; last response: {value:?}"
        );
        sleep(Duration::from_millis(100)).await;
    }

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
#[ignore = "exact external Redis primary interop scenario; run targeted"]
async fn garnet_primary_to_docker_redis_replica_receives_preexisting_stream_dataset_on_full_sync() {
    if !docker_available() {
        return;
    }

    let (addr, shutdown_tx, server) = start_test_server().await;
    wait_for_server_ping(addr).await;

    let mut garnet_client = TcpStream::connect(addr).await.unwrap();
    send_and_expect(
        &mut garnet_client,
        &encode_resp_command(&[b"XADD", b"seed:stream", b"1-0", b"f", b"v1"]),
        b"$3\r\n1-0\r\n",
    )
    .await;
    send_and_expect(
        &mut garnet_client,
        &encode_resp_command(&[b"XADD", b"seed:stream", b"2-0", b"f", b"v2"]),
        b"$3\r\n2-0\r\n",
    )
    .await;
    send_and_expect(
        &mut garnet_client,
        &encode_resp_command(&[b"XADD", b"seed:stream", b"3-0", b"f", b"v3"]),
        b"$3\r\n3-0\r\n",
    )
    .await;
    send_and_expect(
        &mut garnet_client,
        &encode_resp_command(&[b"PEXPIRE", b"seed:stream", b"60000"]),
        b":1\r\n",
    )
    .await;
    send_and_expect(
        &mut garnet_client,
        &encode_resp_command(&[b"XGROUP", b"CREATE", b"seed:stream", b"group1", b"0"]),
        b"+OK\r\n",
    )
    .await;
    let _ = send_and_read_resp_value(
        &mut garnet_client,
        &encode_resp_command(&[
            b"XREADGROUP",
            b"GROUP",
            b"group1",
            b"consumer1",
            b"COUNT",
            b"1",
            b"STREAMS",
            b"seed:stream",
            b">",
        ]),
        Duration::from_secs(2),
    )
    .await;
    let _ = send_and_read_resp_value(
        &mut garnet_client,
        &encode_resp_command(&[
            b"XREADGROUP",
            b"GROUP",
            b"group1",
            b"consumer2",
            b"COUNT",
            b"1",
            b"STREAMS",
            b"seed:stream",
            b">",
        ]),
        Duration::from_secs(2),
    )
    .await;

    let redis_port = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let container_name = format!("garnet-repl-redis-streams-{}", redis_port);
    let _container = DockerContainerGuard::new(container_name.clone());

    let run_output = Command::new("docker")
        .args([
            "run",
            "-d",
            "--rm",
            "--name",
            container_name.as_str(),
            "-p",
            &format!("{redis_port}:6379"),
            "--add-host=host.docker.internal:host-gateway",
            "redis:7.2-alpine",
            "redis-server",
            "--port",
            "6379",
            "--save",
            "",
            "--appendonly",
            "no",
        ])
        .output()
        .unwrap();
    assert!(
        run_output.status.success(),
        "failed to start docker redis replica\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&run_output.stdout),
        String::from_utf8_lossy(&run_output.stderr)
    );

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match TcpStream::connect(("127.0.0.1", redis_port)).await {
            Ok(mut redis_admin) => {
                redis_admin
                    .write_all(&encode_resp_command(&[b"PING"]))
                    .await
                    .unwrap();
                let reply =
                    read_resp_line_with_timeout(&mut redis_admin, Duration::from_secs(1)).await;
                if reply == b"+PONG" {
                    break;
                }
            }
            Err(_) => {}
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for docker redis replica to accept PING on port {redis_port}"
        );
        sleep(Duration::from_millis(100)).await;
    }

    let mut redis_admin = TcpStream::connect(("127.0.0.1", redis_port)).await.unwrap();
    let garnet_port = addr.port().to_string();
    send_and_expect(
        &mut redis_admin,
        &encode_resp_command(&[
            b"REPLICAOF",
            b"host.docker.internal",
            garnet_port.as_bytes(),
        ]),
        b"+OK\r\n",
    )
    .await;

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let info = send_and_read_bulk_payload(
            &mut redis_admin,
            &encode_resp_command(&[b"INFO", b"REPLICATION"]),
            Duration::from_secs(2),
        )
        .await;
        let info_text = String::from_utf8_lossy(&info);
        if info_text.contains("master_link_status:up\r\n") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "docker redis replica did not reach master_link_status=up\n{info_text}"
        );
        sleep(Duration::from_millis(250)).await;
    }

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let xlen = send_and_read_resp_value(
            &mut redis_admin,
            &encode_resp_command(&[b"XLEN", b"seed:stream"]),
            Duration::from_secs(2),
        )
        .await;
        if matches!(&xlen, RespSocketValue::Integer(3)) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "docker redis replica did not receive seeded stream dataset; last response: {xlen:?}"
        );
        sleep(Duration::from_millis(100)).await;
    }

    let groups = send_and_read_resp_value(
        &mut redis_admin,
        &encode_resp_command(&[b"XINFO", b"GROUPS", b"seed:stream"]),
        Duration::from_secs(2),
    )
    .await;
    let groups_array = resp_socket_array(&groups);
    assert_eq!(groups_array.len(), 1);
    let group = resp_socket_map_or_flat_map(&groups_array[0]);
    assert_eq!(resp_socket_bulk(group[&b"name".to_vec()]), b"group1");
    assert_eq!(
        resp_socket_bulk(group[&b"last-delivered-id".to_vec()]),
        b"2-0"
    );
    assert_eq!(
        resp_socket_integer_or_bulk(group[&b"consumers".to_vec()]),
        2
    );
    assert_eq!(resp_socket_integer_or_bulk(group[&b"pending".to_vec()]), 2);

    let pending = send_and_read_resp_value(
        &mut redis_admin,
        &encode_resp_command(&[b"XPENDING", b"seed:stream", b"group1"]),
        Duration::from_secs(2),
    )
    .await;
    let pending_items = resp_socket_array(&pending);
    assert_eq!(resp_socket_integer_or_bulk(&pending_items[0]), 2);
    assert_eq!(resp_socket_bulk(&pending_items[1]), b"1-0");
    assert_eq!(resp_socket_bulk(&pending_items[2]), b"2-0");
    let pending_consumers = resp_socket_array(&pending_items[3]);
    assert_eq!(pending_consumers.len(), 2);
    let consumer_one = resp_socket_array(&pending_consumers[0]);
    assert_eq!(resp_socket_bulk(&consumer_one[0]), b"consumer1");
    assert_eq!(resp_socket_integer_or_bulk(&consumer_one[1]), 1);
    let consumer_two = resp_socket_array(&pending_consumers[1]);
    assert_eq!(resp_socket_bulk(&consumer_two[0]), b"consumer2");
    assert_eq!(resp_socket_integer_or_bulk(&consumer_two[1]), 1);

    let stream_ttl = send_and_read_integer(
        &mut redis_admin,
        &encode_resp_command(&[b"PTTL", b"seed:stream"]),
        Duration::from_secs(2),
    )
    .await;
    assert!(
        stream_ttl > 0,
        "expected positive PTTL for replicated stream"
    );

    send_and_expect(
        &mut garnet_client,
        &encode_resp_command(&[b"XADD", b"repl:stream", b"10-0", b"f", b"from-garnet"]),
        b"$4\r\n10-0\r\n",
    )
    .await;

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let replayed = send_and_read_resp_value(
            &mut redis_admin,
            &encode_resp_command(&[b"XRANGE", b"repl:stream", b"-", b"+"]),
            Duration::from_secs(2),
        )
        .await;
        if let RespSocketValue::Array(entries) = &replayed
            && entries.len() == 1
            && resp_socket_bulk(&resp_socket_array(&entries[0])[0]) == b"10-0"
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "docker redis replica did not receive post-sync stream write; last response: {replayed:?}"
        );
        sleep(Duration::from_millis(100)).await;
    }

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
#[ignore = "exact external Redis primary interop scenario; run targeted"]
async fn redis_primary_to_garnet_replica_receives_preexisting_string_dataset_on_full_sync() {
    if !docker_available() {
        return;
    }

    let (addr, shutdown_tx, server) = start_test_server().await;
    wait_for_server_ping(addr).await;

    let redis_port = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let container_name = format!("redis-primary-garnet-repl-{}", redis_port);
    let _container = DockerContainerGuard::new(container_name.clone());

    let run_output = Command::new("docker")
        .args([
            "run",
            "-d",
            "--rm",
            "--name",
            container_name.as_str(),
            "-p",
            &format!("{redis_port}:6379"),
            "redis:7.2-alpine",
            "redis-server",
            "--port",
            "6379",
            "--save",
            "",
            "--appendonly",
            "no",
        ])
        .output()
        .unwrap();
    assert!(
        run_output.status.success(),
        "failed to start docker redis primary\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&run_output.stdout),
        String::from_utf8_lossy(&run_output.stderr)
    );

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match TcpStream::connect(("127.0.0.1", redis_port)).await {
            Ok(mut redis_admin) => {
                redis_admin
                    .write_all(&encode_resp_command(&[b"PING"]))
                    .await
                    .unwrap();
                let reply =
                    read_resp_line_with_timeout(&mut redis_admin, Duration::from_secs(1)).await;
                if reply == b"+PONG" {
                    break;
                }
            }
            Err(_) => {}
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for docker redis primary to accept PING on port {redis_port}"
        );
        sleep(Duration::from_millis(100)).await;
    }

    let mut redis_admin = TcpStream::connect(("127.0.0.1", redis_port)).await.unwrap();
    send_and_expect(
        &mut redis_admin,
        &encode_resp_command(&[b"SET", b"seed:key", b"seed:value"]),
        b"+OK\r\n",
    )
    .await;

    let mut garnet_admin = TcpStream::connect(addr).await.unwrap();
    let redis_port_text = redis_port.to_string();
    send_and_expect(
        &mut garnet_admin,
        &encode_resp_command(&[b"REPLICAOF", b"127.0.0.1", redis_port_text.as_bytes()]),
        b"+OK\r\n",
    )
    .await;

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let value = send_and_read_resp_value(
            &mut garnet_admin,
            &encode_resp_command(&[b"GET", b"seed:key"]),
            Duration::from_secs(2),
        )
        .await;
        if matches!(&value, RespSocketValue::Bulk(bytes) if bytes == b"seed:value") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "garnet replica did not receive preexisting seed dataset; last response: {value:?}"
        );
        sleep(Duration::from_millis(100)).await;
    }

    send_and_expect(
        &mut redis_admin,
        &encode_resp_command(&[b"SET", b"repl:probe", b"from-redis"]),
        b"+OK\r\n",
    )
    .await;

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let value = send_and_read_resp_value(
            &mut garnet_admin,
            &encode_resp_command(&[b"GET", b"repl:probe"]),
            Duration::from_secs(2),
        )
        .await;
        if matches!(&value, RespSocketValue::Bulk(bytes) if bytes == b"from-redis") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "garnet replica did not receive post-sync write after seeded full sync; last response: {value:?}"
        );
        sleep(Duration::from_millis(100)).await;
    }

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
#[ignore = "exact external Redis primary interop scenario; run targeted"]
async fn redis_primary_to_garnet_replica_receives_preexisting_object_dataset_on_full_sync() {
    if !docker_available() {
        return;
    }

    let (addr, shutdown_tx, server) = start_test_server().await;
    wait_for_server_ping(addr).await;
    let large_list_value = vec![b'x'; 9000];

    let redis_port = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let container_name = format!("redis-primary-garnet-objects-{}", redis_port);
    let _container = DockerContainerGuard::new(container_name.clone());

    let run_output = Command::new("docker")
        .args([
            "run",
            "-d",
            "--rm",
            "--name",
            container_name.as_str(),
            "-p",
            &format!("{redis_port}:6379"),
            "redis:7.2-alpine",
            "redis-server",
            "--port",
            "6379",
            "--save",
            "",
            "--appendonly",
            "no",
        ])
        .output()
        .unwrap();
    assert!(
        run_output.status.success(),
        "failed to start docker redis primary\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&run_output.stdout),
        String::from_utf8_lossy(&run_output.stderr)
    );

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match TcpStream::connect(("127.0.0.1", redis_port)).await {
            Ok(mut redis_admin) => {
                redis_admin
                    .write_all(&encode_resp_command(&[b"PING"]))
                    .await
                    .unwrap();
                let reply =
                    read_resp_line_with_timeout(&mut redis_admin, Duration::from_secs(1)).await;
                if reply == b"+PONG" {
                    break;
                }
            }
            Err(_) => {}
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for docker redis primary to accept PING on port {redis_port}"
        );
        sleep(Duration::from_millis(100)).await;
    }

    let mut redis_admin = TcpStream::connect(("127.0.0.1", redis_port)).await.unwrap();
    send_and_expect(
        &mut redis_admin,
        &encode_resp_command(&[
            b"HSET",
            b"seed:hash",
            b"field1",
            b"value1",
            b"field2",
            b"value2",
        ]),
        b":2\r\n",
    )
    .await;
    send_and_expect(
        &mut redis_admin,
        &encode_resp_command(&[b"PEXPIRE", b"seed:hash", b"60000"]),
        b":1\r\n",
    )
    .await;
    send_and_expect(
        &mut redis_admin,
        &encode_resp_command(&[b"SADD", b"seed:set", b"alpha", b"beta", b"gamma"]),
        b":3\r\n",
    )
    .await;
    send_and_expect(
        &mut redis_admin,
        &encode_resp_command(&[b"ZADD", b"seed:zset", b"1", b"one", b"2", b"two"]),
        b":2\r\n",
    )
    .await;
    send_and_expect(
        &mut redis_admin,
        &encode_resp_command(&[
            b"RPUSH",
            b"seed:list",
            b"alpha",
            large_list_value.as_slice(),
            b"omega",
        ]),
        b":3\r\n",
    )
    .await;

    let mut garnet_admin = TcpStream::connect(addr).await.unwrap();
    let redis_port_text = redis_port.to_string();
    send_and_expect(
        &mut garnet_admin,
        &encode_resp_command(&[b"REPLICAOF", b"127.0.0.1", redis_port_text.as_bytes()]),
        b"+OK\r\n",
    )
    .await;

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let value = send_and_read_resp_value(
            &mut garnet_admin,
            &encode_resp_command(&[b"HGET", b"seed:hash", b"field1"]),
            Duration::from_secs(2),
        )
        .await;
        if matches!(&value, RespSocketValue::Bulk(bytes) if bytes == b"value1") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "garnet replica did not receive preexisting object dataset; last response: {value:?}"
        );
        sleep(Duration::from_millis(100)).await;
    }

    assert_eq!(
        send_and_read_integer(
            &mut garnet_admin,
            &encode_resp_command(&[b"SISMEMBER", b"seed:set", b"beta"]),
            Duration::from_secs(2),
        )
        .await,
        1
    );
    let zrange = send_and_read_resp_value(
        &mut garnet_admin,
        &encode_resp_command(&[b"ZRANGE", b"seed:zset", b"0", b"-1", b"WITHSCORES"]),
        Duration::from_secs(2),
    )
    .await;
    let zrange_items = resp_socket_array(&zrange);
    assert_eq!(resp_socket_bulk(&zrange_items[0]), b"one");
    assert_eq!(resp_socket_bulk(&zrange_items[1]), b"1");
    assert_eq!(resp_socket_bulk(&zrange_items[2]), b"two");
    assert_eq!(resp_socket_bulk(&zrange_items[3]), b"2");
    send_and_expect(
        &mut garnet_admin,
        &encode_resp_command(&[b"OBJECT", b"ENCODING", b"seed:list"]),
        b"$9\r\nquicklist\r\n",
    )
    .await;
    assert_eq!(
        send_and_read_integer(
            &mut garnet_admin,
            &encode_resp_command(&[b"LLEN", b"seed:list"]),
            Duration::from_secs(2),
        )
        .await,
        3
    );
    let list_head = send_and_read_resp_value(
        &mut garnet_admin,
        &encode_resp_command(&[b"LINDEX", b"seed:list", b"0"]),
        Duration::from_secs(2),
    )
    .await;
    assert!(matches!(&list_head, RespSocketValue::Bulk(bytes) if bytes == b"alpha"));
    let list_middle = send_and_read_resp_value(
        &mut garnet_admin,
        &encode_resp_command(&[b"LINDEX", b"seed:list", b"1"]),
        Duration::from_secs(2),
    )
    .await;
    assert!(matches!(
        &list_middle,
        RespSocketValue::Bulk(bytes) if bytes == large_list_value.as_slice()
    ));
    let list_tail = send_and_read_resp_value(
        &mut garnet_admin,
        &encode_resp_command(&[b"LINDEX", b"seed:list", b"2"]),
        Duration::from_secs(2),
    )
    .await;
    assert!(matches!(&list_tail, RespSocketValue::Bulk(bytes) if bytes == b"omega"));
    let hash_ttl = send_and_read_integer(
        &mut garnet_admin,
        &encode_resp_command(&[b"PTTL", b"seed:hash"]),
        Duration::from_secs(2),
    )
    .await;
    assert!(hash_ttl > 0, "expected positive PTTL for imported hash");

    send_and_expect(
        &mut redis_admin,
        &encode_resp_command(&[b"HSET", b"repl:hash", b"field", b"from-redis"]),
        b":1\r\n",
    )
    .await;

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let value = send_and_read_resp_value(
            &mut garnet_admin,
            &encode_resp_command(&[b"HGET", b"repl:hash", b"field"]),
            Duration::from_secs(2),
        )
        .await;
        if matches!(&value, RespSocketValue::Bulk(bytes) if bytes == b"from-redis") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "garnet replica did not receive post-sync object write; last response: {value:?}"
        );
        sleep(Duration::from_millis(100)).await;
    }

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
#[ignore = "exact external Redis 6.2 primary interop scenario; run targeted"]
async fn redis_6_2_primary_to_garnet_replica_receives_legacy_object_dataset_on_full_sync() {
    if !docker_available() {
        return;
    }

    let (addr, shutdown_tx, server) = start_test_server().await;
    wait_for_server_ping(addr).await;
    let large_list_value = vec![b'x'; 9000];

    let redis_port = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let container_name = format!("redis62-primary-garnet-objects-{}", redis_port);
    let _container = DockerContainerGuard::new(container_name.clone());

    let run_output = Command::new("docker")
        .args([
            "run",
            "-d",
            "--rm",
            "--name",
            container_name.as_str(),
            "-p",
            &format!("{redis_port}:6379"),
            "redis:6.2-alpine",
            "redis-server",
            "--port",
            "6379",
            "--save",
            "",
            "--appendonly",
            "no",
        ])
        .output()
        .unwrap();
    assert!(
        run_output.status.success(),
        "failed to start docker redis 6.2 primary\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&run_output.stdout),
        String::from_utf8_lossy(&run_output.stderr)
    );

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match TcpStream::connect(("127.0.0.1", redis_port)).await {
            Ok(mut redis_admin) => {
                redis_admin
                    .write_all(&encode_resp_command(&[b"PING"]))
                    .await
                    .unwrap();
                let reply =
                    read_resp_line_with_timeout(&mut redis_admin, Duration::from_secs(1)).await;
                if reply == b"+PONG" {
                    break;
                }
            }
            Err(_) => {}
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for docker redis 6.2 primary to accept PING on port {redis_port}"
        );
        sleep(Duration::from_millis(100)).await;
    }

    let mut redis_admin = TcpStream::connect(("127.0.0.1", redis_port)).await.unwrap();
    send_and_expect(
        &mut redis_admin,
        &encode_resp_command(&[
            b"HSET",
            b"seed:hash",
            b"field1",
            b"value1",
            b"field2",
            b"value2",
        ]),
        b":2\r\n",
    )
    .await;
    send_and_expect(
        &mut redis_admin,
        &encode_resp_command(&[b"PEXPIRE", b"seed:hash", b"60000"]),
        b":1\r\n",
    )
    .await;
    send_and_expect(
        &mut redis_admin,
        &encode_resp_command(&[b"ZADD", b"seed:zset", b"1", b"one", b"2", b"two"]),
        b":2\r\n",
    )
    .await;
    send_and_expect(
        &mut redis_admin,
        &encode_resp_command(&[
            b"RPUSH",
            b"seed:list",
            b"alpha",
            large_list_value.as_slice(),
            b"omega",
        ]),
        b":3\r\n",
    )
    .await;

    let mut garnet_admin = TcpStream::connect(addr).await.unwrap();
    let redis_port_text = redis_port.to_string();
    send_and_expect(
        &mut garnet_admin,
        &encode_resp_command(&[b"REPLICAOF", b"127.0.0.1", redis_port_text.as_bytes()]),
        b"+OK\r\n",
    )
    .await;

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let value = send_and_read_resp_value(
            &mut garnet_admin,
            &encode_resp_command(&[b"HGET", b"seed:hash", b"field1"]),
            Duration::from_secs(2),
        )
        .await;
        if matches!(&value, RespSocketValue::Bulk(bytes) if bytes == b"value1") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "garnet replica did not receive redis 6.2 seeded object dataset; last response: {value:?}"
        );
        sleep(Duration::from_millis(100)).await;
    }

    let zrange = send_and_read_resp_value(
        &mut garnet_admin,
        &encode_resp_command(&[b"ZRANGE", b"seed:zset", b"0", b"-1", b"WITHSCORES"]),
        Duration::from_secs(2),
    )
    .await;
    let zrange_items = resp_socket_array(&zrange);
    assert_eq!(resp_socket_bulk(&zrange_items[0]), b"one");
    assert_eq!(resp_socket_bulk(&zrange_items[1]), b"1");
    assert_eq!(resp_socket_bulk(&zrange_items[2]), b"two");
    assert_eq!(resp_socket_bulk(&zrange_items[3]), b"2");
    send_and_expect(
        &mut garnet_admin,
        &encode_resp_command(&[b"OBJECT", b"ENCODING", b"seed:list"]),
        b"$9\r\nquicklist\r\n",
    )
    .await;
    assert_eq!(
        send_and_read_integer(
            &mut garnet_admin,
            &encode_resp_command(&[b"LLEN", b"seed:list"]),
            Duration::from_secs(2),
        )
        .await,
        3
    );
    let list_middle = send_and_read_resp_value(
        &mut garnet_admin,
        &encode_resp_command(&[b"LINDEX", b"seed:list", b"1"]),
        Duration::from_secs(2),
    )
    .await;
    assert!(matches!(
        &list_middle,
        RespSocketValue::Bulk(bytes) if bytes == large_list_value.as_slice()
    ));
    let hash_ttl = send_and_read_integer(
        &mut garnet_admin,
        &encode_resp_command(&[b"PTTL", b"seed:hash"]),
        Duration::from_secs(2),
    )
    .await;
    assert!(
        hash_ttl > 0,
        "expected positive PTTL for imported legacy hash"
    );

    send_and_expect(
        &mut redis_admin,
        &encode_resp_command(&[b"HSET", b"repl:hash", b"field", b"from-redis62"]),
        b":1\r\n",
    )
    .await;

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let value = send_and_read_resp_value(
            &mut garnet_admin,
            &encode_resp_command(&[b"HGET", b"repl:hash", b"field"]),
            Duration::from_secs(2),
        )
        .await;
        if matches!(&value, RespSocketValue::Bulk(bytes) if bytes == b"from-redis62") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "garnet replica did not receive post-sync redis 6.2 object write; last response: {value:?}"
        );
        sleep(Duration::from_millis(100)).await;
    }

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
#[ignore = "exact external local redis-server HFE interop scenario; run targeted"]
async fn garnet_primary_to_local_redis_replica_receives_hash_field_expiration_dataset_on_full_sync()
{
    if !system_redis_server_supports_hash_field_expiration() {
        return;
    }

    let (addr, shutdown_tx, server) = start_test_server().await;
    wait_for_server_ping(addr).await;

    let mut garnet_client = TcpStream::connect(addr).await.unwrap();
    send_and_expect(
        &mut garnet_client,
        &encode_resp_command(&[
            b"HSET",
            b"seed:hfe",
            b"field1",
            b"value1",
            b"field2",
            b"value2",
        ]),
        b":2\r\n",
    )
    .await;
    let future_millis = current_unix_time_millis_for_test() + 60_000;
    let future_millis_text = future_millis.to_string();
    send_and_expect(
        &mut garnet_client,
        &encode_resp_command(&[
            b"HPEXPIREAT",
            b"seed:hfe",
            future_millis_text.as_bytes(),
            b"FIELDS",
            b"1",
            b"field1",
        ]),
        b"*1\r\n:1\r\n",
    )
    .await;

    let redis_port = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let _redis = LocalRedisServerGuard::spawn(redis_port, "redis-hfe-replica");
    wait_for_server_ping(std::net::SocketAddr::from(([127, 0, 0, 1], redis_port))).await;

    let mut redis_admin = TcpStream::connect(("127.0.0.1", redis_port)).await.unwrap();
    let garnet_port = addr.port().to_string();
    send_and_expect(
        &mut redis_admin,
        &encode_resp_command(&[b"REPLICAOF", b"127.0.0.1", garnet_port.as_bytes()]),
        b"+OK\r\n",
    )
    .await;

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let info = send_and_read_bulk_payload(
            &mut redis_admin,
            &encode_resp_command(&[b"INFO", b"REPLICATION"]),
            Duration::from_secs(2),
        )
        .await;
        let info_text = String::from_utf8_lossy(&info);
        if info_text.contains("master_link_status:up\r\n") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "local redis replica did not reach master_link_status=up\n{info_text}"
        );
        sleep(Duration::from_millis(250)).await;
    }

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let value = send_and_read_resp_value(
            &mut redis_admin,
            &encode_resp_command(&[b"HGET", b"seed:hfe", b"field1"]),
            Duration::from_secs(2),
        )
        .await;
        if matches!(&value, RespSocketValue::Bulk(bytes) if bytes == b"value1") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "local redis replica did not receive seeded HFE dataset; last response: {value:?}"
        );
        sleep(Duration::from_millis(100)).await;
    }

    send_and_expect(
        &mut redis_admin,
        &encode_resp_command(&[b"OBJECT", b"ENCODING", b"seed:hfe"]),
        b"$10\r\nlistpackex\r\n",
    )
    .await;
    let expiretimes = send_and_read_resp_value(
        &mut redis_admin,
        &encode_resp_command(&[
            b"HPEXPIRETIME",
            b"seed:hfe",
            b"FIELDS",
            b"2",
            b"field1",
            b"field2",
        ]),
        Duration::from_secs(2),
    )
    .await;
    let expiretime_items = resp_socket_array(&expiretimes);
    assert_eq!(resp_socket_integer_or_bulk(&expiretime_items[1]), -1);
    assert!(resp_socket_integer_or_bulk(&expiretime_items[0]) > 0);

    send_and_expect(
        &mut garnet_client,
        &encode_resp_command(&[b"HSET", b"repl:hfe", b"field", b"from-garnet"]),
        b":1\r\n",
    )
    .await;
    let repl_future_millis = current_unix_time_millis_for_test() + 60_000;
    let repl_future_millis_text = repl_future_millis.to_string();
    send_and_expect(
        &mut garnet_client,
        &encode_resp_command(&[
            b"HPEXPIREAT",
            b"repl:hfe",
            repl_future_millis_text.as_bytes(),
            b"FIELDS",
            b"1",
            b"field",
        ]),
        b"*1\r\n:1\r\n",
    )
    .await;

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let expiretimes = send_and_read_resp_value(
            &mut redis_admin,
            &encode_resp_command(&[b"HPEXPIRETIME", b"repl:hfe", b"FIELDS", b"1", b"field"]),
            Duration::from_secs(2),
        )
        .await;
        let expiretime_items = resp_socket_array(&expiretimes);
        if resp_socket_integer_or_bulk(&expiretime_items[0]) > 0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "local redis replica did not receive post-sync HFE write; last response: {expiretimes:?}"
        );
        sleep(Duration::from_millis(100)).await;
    }

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
#[ignore = "exact external local redis-server HFE interop scenario; run targeted"]
async fn local_redis_primary_to_garnet_replica_receives_hash_field_expiration_dataset_on_full_sync()
{
    if !system_redis_server_supports_hash_field_expiration() {
        return;
    }

    let (addr, shutdown_tx, server) = start_test_server().await;
    wait_for_server_ping(addr).await;

    let redis_port = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let _redis = LocalRedisServerGuard::spawn(redis_port, "redis-hfe-primary");
    wait_for_server_ping(std::net::SocketAddr::from(([127, 0, 0, 1], redis_port))).await;

    let mut redis_admin = TcpStream::connect(("127.0.0.1", redis_port)).await.unwrap();
    send_and_expect(
        &mut redis_admin,
        &encode_resp_command(&[
            b"HSET",
            b"seed:hfe",
            b"field1",
            b"value1",
            b"field2",
            b"value2",
        ]),
        b":2\r\n",
    )
    .await;
    let future_millis = current_unix_time_millis_for_test() + 60_000;
    let future_millis_text = future_millis.to_string();
    send_and_expect(
        &mut redis_admin,
        &encode_resp_command(&[
            b"HPEXPIREAT",
            b"seed:hfe",
            future_millis_text.as_bytes(),
            b"FIELDS",
            b"1",
            b"field1",
        ]),
        b"*1\r\n:1\r\n",
    )
    .await;

    let mut garnet_admin = TcpStream::connect(addr).await.unwrap();
    let redis_port_text = redis_port.to_string();
    send_and_expect(
        &mut garnet_admin,
        &encode_resp_command(&[b"REPLICAOF", b"127.0.0.1", redis_port_text.as_bytes()]),
        b"+OK\r\n",
    )
    .await;

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let value = send_and_read_resp_value(
            &mut garnet_admin,
            &encode_resp_command(&[b"HGET", b"seed:hfe", b"field1"]),
            Duration::from_secs(2),
        )
        .await;
        if matches!(&value, RespSocketValue::Bulk(bytes) if bytes == b"value1") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "garnet replica did not receive seeded HFE dataset; last response: {value:?}"
        );
        sleep(Duration::from_millis(100)).await;
    }

    send_and_expect(
        &mut garnet_admin,
        &encode_resp_command(&[b"OBJECT", b"ENCODING", b"seed:hfe"]),
        b"$10\r\nlistpackex\r\n",
    )
    .await;
    let expiretimes = send_and_read_resp_value(
        &mut garnet_admin,
        &encode_resp_command(&[
            b"HPEXPIRETIME",
            b"seed:hfe",
            b"FIELDS",
            b"2",
            b"field1",
            b"field2",
        ]),
        Duration::from_secs(2),
    )
    .await;
    let expiretime_items = resp_socket_array(&expiretimes);
    assert_eq!(resp_socket_integer_or_bulk(&expiretime_items[1]), -1);
    assert!(resp_socket_integer_or_bulk(&expiretime_items[0]) > 0);

    send_and_expect(
        &mut redis_admin,
        &encode_resp_command(&[b"HSET", b"repl:hfe", b"field", b"from-redis"]),
        b":1\r\n",
    )
    .await;
    let repl_future_millis = current_unix_time_millis_for_test() + 60_000;
    let repl_future_millis_text = repl_future_millis.to_string();
    send_and_expect(
        &mut redis_admin,
        &encode_resp_command(&[
            b"HPEXPIREAT",
            b"repl:hfe",
            repl_future_millis_text.as_bytes(),
            b"FIELDS",
            b"1",
            b"field",
        ]),
        b"*1\r\n:1\r\n",
    )
    .await;

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let expiretimes = send_and_read_resp_value(
            &mut garnet_admin,
            &encode_resp_command(&[b"HPEXPIRETIME", b"repl:hfe", b"FIELDS", b"1", b"field"]),
            Duration::from_secs(2),
        )
        .await;
        let expiretime_items = resp_socket_array(&expiretimes);
        if resp_socket_integer_or_bulk(&expiretime_items[0]) > 0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "garnet replica did not receive post-sync HFE write; last response: {expiretimes:?}"
        );
        sleep(Duration::from_millis(100)).await;
    }

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
#[ignore = "exact external Redis primary interop scenario; run targeted"]
async fn redis_primary_to_garnet_replica_receives_preexisting_stream_dataset_on_full_sync() {
    if !docker_available() {
        return;
    }

    let (addr, shutdown_tx, server) = start_test_server().await;
    wait_for_server_ping(addr).await;

    let redis_port = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let container_name = format!("redis-primary-garnet-streams-{}", redis_port);
    let _container = DockerContainerGuard::new(container_name.clone());

    let run_output = Command::new("docker")
        .args([
            "run",
            "-d",
            "--rm",
            "--name",
            container_name.as_str(),
            "-p",
            &format!("{redis_port}:6379"),
            "redis:7.2-alpine",
            "redis-server",
            "--port",
            "6379",
            "--save",
            "",
            "--appendonly",
            "no",
        ])
        .output()
        .unwrap();
    assert!(
        run_output.status.success(),
        "failed to start docker redis primary\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&run_output.stdout),
        String::from_utf8_lossy(&run_output.stderr)
    );

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match TcpStream::connect(("127.0.0.1", redis_port)).await {
            Ok(mut redis_admin) => {
                redis_admin
                    .write_all(&encode_resp_command(&[b"PING"]))
                    .await
                    .unwrap();
                let reply =
                    read_resp_line_with_timeout(&mut redis_admin, Duration::from_secs(1)).await;
                if reply == b"+PONG" {
                    break;
                }
            }
            Err(_) => {}
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for docker redis primary to accept PING on port {redis_port}"
        );
        sleep(Duration::from_millis(100)).await;
    }

    let mut redis_admin = TcpStream::connect(("127.0.0.1", redis_port)).await.unwrap();
    send_and_expect(
        &mut redis_admin,
        &encode_resp_command(&[b"XADD", b"seed:stream", b"1-0", b"f", b"v1"]),
        b"$3\r\n1-0\r\n",
    )
    .await;
    send_and_expect(
        &mut redis_admin,
        &encode_resp_command(&[b"XADD", b"seed:stream", b"2-0", b"f", b"v2"]),
        b"$3\r\n2-0\r\n",
    )
    .await;
    send_and_expect(
        &mut redis_admin,
        &encode_resp_command(&[b"XADD", b"seed:stream", b"3-0", b"f", b"v3"]),
        b"$3\r\n3-0\r\n",
    )
    .await;
    send_and_expect(
        &mut redis_admin,
        &encode_resp_command(&[b"PEXPIRE", b"seed:stream", b"60000"]),
        b":1\r\n",
    )
    .await;
    send_and_expect(
        &mut redis_admin,
        &encode_resp_command(&[b"XGROUP", b"CREATE", b"seed:stream", b"group1", b"0"]),
        b"+OK\r\n",
    )
    .await;
    let _ = send_and_read_resp_value(
        &mut redis_admin,
        &encode_resp_command(&[
            b"XREADGROUP",
            b"GROUP",
            b"group1",
            b"consumer1",
            b"COUNT",
            b"1",
            b"STREAMS",
            b"seed:stream",
            b">",
        ]),
        Duration::from_secs(2),
    )
    .await;
    let _ = send_and_read_resp_value(
        &mut redis_admin,
        &encode_resp_command(&[
            b"XREADGROUP",
            b"GROUP",
            b"group1",
            b"consumer2",
            b"COUNT",
            b"1",
            b"STREAMS",
            b"seed:stream",
            b">",
        ]),
        Duration::from_secs(2),
    )
    .await;

    let mut garnet_admin = TcpStream::connect(addr).await.unwrap();
    let redis_port_text = redis_port.to_string();
    send_and_expect(
        &mut garnet_admin,
        &encode_resp_command(&[b"REPLICAOF", b"127.0.0.1", redis_port_text.as_bytes()]),
        b"+OK\r\n",
    )
    .await;

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let xlen = send_and_read_resp_value(
            &mut garnet_admin,
            &encode_resp_command(&[b"XLEN", b"seed:stream"]),
            Duration::from_secs(2),
        )
        .await;
        if matches!(&xlen, RespSocketValue::Integer(3)) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "garnet replica did not receive preexisting stream dataset; last response: {xlen:?}"
        );
        sleep(Duration::from_millis(100)).await;
    }

    let groups = send_and_read_resp_value(
        &mut garnet_admin,
        &encode_resp_command(&[b"XINFO", b"GROUPS", b"seed:stream"]),
        Duration::from_secs(2),
    )
    .await;
    let groups_array = resp_socket_array(&groups);
    assert_eq!(groups_array.len(), 1);
    let group = resp_socket_map_or_flat_map(&groups_array[0]);
    assert_eq!(resp_socket_bulk(group[&b"name".to_vec()]), b"group1");
    assert_eq!(
        resp_socket_bulk(group[&b"last-delivered-id".to_vec()]),
        b"2-0"
    );
    assert_eq!(
        resp_socket_integer_or_bulk(group[&b"consumers".to_vec()]),
        2
    );
    assert_eq!(resp_socket_integer_or_bulk(group[&b"pending".to_vec()]), 2);

    let pending = send_and_read_resp_value(
        &mut garnet_admin,
        &encode_resp_command(&[b"XPENDING", b"seed:stream", b"group1"]),
        Duration::from_secs(2),
    )
    .await;
    let pending_items = resp_socket_array(&pending);
    assert_eq!(resp_socket_integer_or_bulk(&pending_items[0]), 2);
    assert_eq!(resp_socket_bulk(&pending_items[1]), b"1-0");
    assert_eq!(resp_socket_bulk(&pending_items[2]), b"2-0");
    let pending_consumers = resp_socket_array(&pending_items[3]);
    assert_eq!(pending_consumers.len(), 2);
    let consumer_one = resp_socket_array(&pending_consumers[0]);
    assert_eq!(resp_socket_bulk(&consumer_one[0]), b"consumer1");
    assert_eq!(resp_socket_integer_or_bulk(&consumer_one[1]), 1);
    let consumer_two = resp_socket_array(&pending_consumers[1]);
    assert_eq!(resp_socket_bulk(&consumer_two[0]), b"consumer2");
    assert_eq!(resp_socket_integer_or_bulk(&consumer_two[1]), 1);

    let stream_ttl = send_and_read_integer(
        &mut garnet_admin,
        &encode_resp_command(&[b"PTTL", b"seed:stream"]),
        Duration::from_secs(2),
    )
    .await;
    assert!(stream_ttl > 0, "expected positive PTTL for imported stream");

    send_and_expect(
        &mut redis_admin,
        &encode_resp_command(&[b"XADD", b"repl:stream", b"10-0", b"f", b"from-redis"]),
        b"$4\r\n10-0\r\n",
    )
    .await;

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let replayed = send_and_read_resp_value(
            &mut garnet_admin,
            &encode_resp_command(&[b"XRANGE", b"repl:stream", b"-", b"+"]),
            Duration::from_secs(2),
        )
        .await;
        if let RespSocketValue::Array(entries) = &replayed
            && entries.len() == 1
            && resp_socket_bulk(&resp_socket_array(&entries[0])[0]) == b"10-0"
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "garnet replica did not receive post-sync stream write; last response: {replayed:?}"
        );
        sleep(Duration::from_millis(100)).await;
    }

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn slowlog_threshold_and_entry_shape_match_external_scenarios() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let mut client = TcpStream::connect(addr).await.unwrap();
    let timeout = Duration::from_secs(5);

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"config", b"set", b"slowlog-log-slower-than", b"100000"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(&mut client, &encode_resp_command(&[b"ping"]), b"+PONG\r\n").await;
    assert_eq!(
        send_and_read_integer(
            &mut client,
            &encode_resp_command(&[b"slowlog", b"len"]),
            timeout,
        )
        .await,
        0
    );

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"debug", b"sleep", b"0.2"]),
        b"+OK\r\n",
    )
    .await;
    assert_eq!(
        send_and_read_integer(
            &mut client,
            &encode_resp_command(&[b"slowlog", b"len"]),
            timeout,
        )
        .await,
        1
    );

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"client", b"setname", b"foobar"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"debug", b"sleep", b"0.2"]),
        b"+OK\r\n",
    )
    .await;

    let entries = send_and_read_slowlog_entries(&mut client, b"-1", timeout).await;
    assert_eq!(
        slowlog_entry_argument_words(&entries[0]),
        vec!["debug", "sleep", "0.2"]
    );
    assert_eq!(resp_socket_array(&entries[0]).len(), 6);
    assert!(resp_socket_integer(&resp_socket_array(&entries[0])[2]) > 100_000);
    assert_eq!(
        resp_socket_bulk(&resp_socket_array(&entries[0])[5]),
        b"foobar"
    );

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn slowlog_sensitive_redaction_matches_external_scenarios() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let mut client = TcpStream::connect(addr).await.unwrap();
    let timeout = Duration::from_secs(5);
    let port_text = addr.port().to_string();

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"config", b"set", b"slowlog-max-len", b"100"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"config", b"set", b"slowlog-log-slower-than", b"0"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"slowlog", b"reset"]),
        b"+OK\r\n",
    )
    .await;

    client
        .write_all(&encode_resp_command(&[
            b"acl",
            b"setuser",
            b"slowlog test user",
            b"+get",
            b"+set",
        ]))
        .await
        .unwrap();
    let _ = read_resp_line_with_timeout(&mut client, timeout).await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"config", b"set", b"masteruser", b""]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"config", b"set", b"masterauth", b""]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"config", b"set", b"requirepass", b""]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"config", b"set", b"tls-key-file-pass", b""]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"config", b"set", b"tls-client-key-file-pass", b""]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"acl", b"setuser", b"slowlog-test-user", b"+get", b"+set"]),
        b"+OK\r\n",
    )
    .await;
    let _ = send_and_read_resp_value(
        &mut client,
        &encode_resp_command(&[b"acl", b"getuser", b"slowlog-test-user"]),
        timeout,
    )
    .await;
    let _ = send_and_read_integer(
        &mut client,
        &encode_resp_command(&[
            b"acl",
            b"deluser",
            b"slowlog-test-user",
            b"non-existing-user",
        ]),
        timeout,
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"config", b"set", b"slowlog-log-slower-than", b"0"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"config", b"set", b"slowlog-log-slower-than", b"-1"]),
        b"+OK\r\n",
    )
    .await;

    let sensitive_entries = send_and_read_slowlog_entries(&mut client, b"-1", timeout).await;
    assert_eq!(
        slowlog_entry_texts(&sensitive_entries),
        vec![
            "config set slowlog-log-slower-than 0",
            "acl deluser (redacted) (redacted)",
            "acl getuser (redacted)",
            "acl setuser (redacted) (redacted) (redacted)",
            "config set tls-client-key-file-pass (redacted)",
            "config set tls-key-file-pass (redacted)",
            "config set requirepass (redacted)",
            "config set masterauth (redacted)",
            "config set masteruser (redacted)",
            "acl setuser (redacted) (redacted) (redacted)",
            "slowlog reset",
        ]
    );

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"config", b"set", b"slowlog-log-slower-than", b"0"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"slowlog", b"reset"]),
        b"+OK\r\n",
    )
    .await;

    let migrate_base = [
        b"migrate".as_slice(),
        b"127.0.0.1".as_slice(),
        port_text.as_bytes(),
        b"key".as_slice(),
        b"9".as_slice(),
        b"5000".as_slice(),
    ];
    client
        .write_all(&encode_resp_command(&migrate_base))
        .await
        .unwrap();
    let _ = read_resp_line_with_timeout(&mut client, timeout).await;
    client
        .write_all(&encode_resp_command(&[
            migrate_base[0],
            migrate_base[1],
            migrate_base[2],
            migrate_base[3],
            migrate_base[4],
            migrate_base[5],
            b"AUTH",
            b"user",
        ]))
        .await
        .unwrap();
    let _ = read_resp_line_with_timeout(&mut client, timeout).await;
    client
        .write_all(&encode_resp_command(&[
            migrate_base[0],
            migrate_base[1],
            migrate_base[2],
            migrate_base[3],
            migrate_base[4],
            migrate_base[5],
            b"AUTH2",
            b"user",
            b"password",
        ]))
        .await
        .unwrap();
    let _ = read_resp_line_with_timeout(&mut client, timeout).await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"config", b"set", b"slowlog-log-slower-than", b"-1"]),
        b"+OK\r\n",
    )
    .await;

    let migrate_entries = send_and_read_slowlog_entries(&mut client, b"-1", timeout).await;
    assert_eq!(
        slowlog_entry_texts(&migrate_entries),
        vec![
            format!(
                "migrate 127.0.0.1 {} key 9 5000 AUTH2 (redacted) (redacted)",
                port_text
            ),
            format!("migrate 127.0.0.1 {} key 9 5000 AUTH (redacted)", port_text),
            format!("migrate 127.0.0.1 {} key 9 5000", port_text),
            "slowlog reset".to_string(),
        ]
    );

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn migrate_moves_key_between_two_live_servers_like_external_dump_scenario() {
    let (source_addr, source_shutdown_tx, source_server) = start_test_server_on_dedicated_thread();
    let (target_addr, target_shutdown_tx, target_server) = start_test_server_on_dedicated_thread();
    wait_for_server_ping(source_addr).await;
    wait_for_server_ping(target_addr).await;

    let mut source = TcpStream::connect(source_addr).await.unwrap();
    let mut target = TcpStream::connect(target_addr).await.unwrap();
    let target_port = target_addr.port().to_string();

    send_and_expect(
        &mut source,
        &encode_resp_command(&[b"SET", b"key", b"Some Value"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut source,
        &encode_resp_command(&[
            b"MIGRATE",
            b"127.0.0.1",
            target_port.as_bytes(),
            b"key",
            b"9",
            b"5000",
        ]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut source,
        &encode_resp_command(&[b"EXISTS", b"key"]),
        b":0\r\n",
    )
    .await;

    send_and_expect(
        &mut target,
        &encode_resp_command(&[b"SELECT", b"9"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut target,
        &encode_resp_command(&[b"EXISTS", b"key"]),
        b":1\r\n",
    )
    .await;
    send_and_expect(
        &mut target,
        &encode_resp_command(&[b"GET", b"key"]),
        b"$10\r\nSome Value\r\n",
    )
    .await;
    send_and_expect(
        &mut target,
        &encode_resp_command(&[b"TTL", b"key"]),
        b":-1\r\n",
    )
    .await;

    let _ = source_shutdown_tx.send(());
    let _ = target_shutdown_tx.send(());
    source_server.join().unwrap();
    target_server.join().unwrap();
}

#[tokio::test]
async fn migrate_copy_replace_and_keys_follow_external_dump_scenarios() {
    let (source_addr, source_shutdown_tx, source_server) = start_test_server_on_dedicated_thread();
    let (target_addr, target_shutdown_tx, target_server) = start_test_server_on_dedicated_thread();
    wait_for_server_ping(source_addr).await;
    wait_for_server_ping(target_addr).await;

    let mut source = TcpStream::connect(source_addr).await.unwrap();
    let mut target = TcpStream::connect(target_addr).await.unwrap();
    let timeout = Duration::from_secs(5);
    let target_port = target_addr.port().to_string();

    send_and_expect(
        &mut source,
        &encode_resp_command(&[b"LPUSH", b"list", b"a", b"b", b"c", b"d"]),
        b":4\r\n",
    )
    .await;
    send_and_expect(
        &mut source,
        &encode_resp_command(&[
            b"MIGRATE",
            b"127.0.0.1",
            target_port.as_bytes(),
            b"list",
            b"9",
            b"5000",
            b"COPY",
        ]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut source,
        &encode_resp_command(&[b"EXISTS", b"list"]),
        b":1\r\n",
    )
    .await;

    send_and_expect(
        &mut target,
        &encode_resp_command(&[b"SELECT", b"9"]),
        b"+OK\r\n",
    )
    .await;
    let copied = send_and_read_resp_value(
        &mut target,
        &encode_resp_command(&[b"LRANGE", b"list", b"0", b"-1"]),
        timeout,
    )
    .await;
    assert_eq!(
        resp_socket_array(&copied)
            .iter()
            .map(|item| resp_socket_bulk(item).to_vec())
            .collect::<Vec<_>>(),
        vec![b"d".to_vec(), b"c".to_vec(), b"b".to_vec(), b"a".to_vec()]
    );

    send_and_expect(
        &mut source,
        &encode_resp_command(&[b"LPUSH", b"replace-key", b"a", b"b", b"c", b"d"]),
        b":4\r\n",
    )
    .await;
    send_and_expect(
        &mut target,
        &encode_resp_command(&[b"SET", b"replace-key", b"busy"]),
        b"+OK\r\n",
    )
    .await;
    let busy = send_and_read_resp_value(
        &mut source,
        &encode_resp_command(&[
            b"MIGRATE",
            b"127.0.0.1",
            target_port.as_bytes(),
            b"replace-key",
            b"9",
            b"5000",
            b"COPY",
        ]),
        timeout,
    )
    .await;
    assert_eq!(
        resp_socket_bulk(&busy),
        b"ERR Target instance replied with error: BUSYKEY Target key name already exists."
    );
    send_and_expect(
        &mut source,
        &encode_resp_command(&[b"EXISTS", b"replace-key"]),
        b":1\r\n",
    )
    .await;

    send_and_expect(
        &mut source,
        &encode_resp_command(&[
            b"MIGRATE",
            b"127.0.0.1",
            target_port.as_bytes(),
            b"replace-key",
            b"9",
            b"5000",
            b"COPY",
            b"REPLACE",
        ]),
        b"+OK\r\n",
    )
    .await;
    let replaced = send_and_read_resp_value(
        &mut target,
        &encode_resp_command(&[b"LRANGE", b"replace-key", b"0", b"-1"]),
        timeout,
    )
    .await;
    assert_eq!(
        resp_socket_array(&replaced)
            .iter()
            .map(|item| resp_socket_bulk(item).to_vec())
            .collect::<Vec<_>>(),
        vec![b"d".to_vec(), b"c".to_vec(), b"b".to_vec(), b"a".to_vec()]
    );

    send_and_expect(
        &mut source,
        &encode_resp_command(&[b"SET", b"key1", b"v1"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut source,
        &encode_resp_command(&[b"SET", b"key2", b"v2"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut source,
        &encode_resp_command(&[
            b"MIGRATE",
            b"127.0.0.1",
            target_port.as_bytes(),
            b"",
            b"9",
            b"5000",
            b"KEYS",
            b"nokey-1",
            b"nokey-2",
        ]),
        b"+NOKEY\r\n",
    )
    .await;
    send_and_expect(
        &mut source,
        &encode_resp_command(&[
            b"MIGRATE",
            b"127.0.0.1",
            target_port.as_bytes(),
            b"",
            b"9",
            b"5000",
            b"KEYS",
            b"key1",
            b"key2",
        ]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut source,
        &encode_resp_command(&[b"EXISTS", b"key1", b"key2"]),
        b":0\r\n",
    )
    .await;
    send_and_expect(
        &mut target,
        &encode_resp_command(&[b"GET", b"key1"]),
        b"$2\r\nv1\r\n",
    )
    .await;
    send_and_expect(
        &mut target,
        &encode_resp_command(&[b"GET", b"key2"]),
        b"$2\r\nv2\r\n",
    )
    .await;

    let _ = source_shutdown_tx.send(());
    let _ = target_shutdown_tx.send(());
    source_server.join().unwrap();
    target_server.join().unwrap();
}

#[tokio::test]
async fn migrate_rewrites_source_delete_for_downstream_replica_and_target_state() {
    let (source_addr, source_shutdown_tx, source_server) = start_test_server_on_dedicated_thread();
    let (target_addr, target_shutdown_tx, target_server) = start_test_server_on_dedicated_thread();
    let (replica_addr, replica_shutdown_tx, replica_server) =
        start_test_server_on_dedicated_thread();
    wait_for_server_ping(source_addr).await;
    wait_for_server_ping(target_addr).await;
    wait_for_server_ping(replica_addr).await;

    let mut source = TcpStream::connect(source_addr).await.unwrap();
    let mut target = TcpStream::connect(target_addr).await.unwrap();
    let mut replica = TcpStream::connect(replica_addr).await.unwrap();
    let source_port = source_addr.port().to_string();
    let target_port = target_addr.port().to_string();

    send_and_expect(
        &mut replica,
        &encode_resp_command(&[b"REPLICAOF", b"127.0.0.1", source_port.as_bytes()]),
        b"+OK\r\n",
    )
    .await;
    let _ = wait_for_replica_info_line(&mut source, 1, Duration::from_secs(5)).await;

    send_and_expect(
        &mut source,
        &encode_resp_command(&[b"SET", b"migrate-replica-key", b"value"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut source,
        &encode_resp_command(&[b"WAIT", b"1", b"5000"]),
        b":1\r\n",
    )
    .await;
    send_and_expect(
        &mut replica,
        &encode_resp_command(&[b"GET", b"migrate-replica-key"]),
        b"$5\r\nvalue\r\n",
    )
    .await;

    send_and_expect(
        &mut source,
        &encode_resp_command(&[
            b"MIGRATE",
            b"127.0.0.1",
            target_port.as_bytes(),
            b"migrate-replica-key",
            b"9",
            b"5000",
        ]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut source,
        &encode_resp_command(&[b"WAIT", b"1", b"5000"]),
        b":1\r\n",
    )
    .await;

    send_and_expect(
        &mut source,
        &encode_resp_command(&[b"EXISTS", b"migrate-replica-key"]),
        b":0\r\n",
    )
    .await;
    send_and_expect(
        &mut replica,
        &encode_resp_command(&[b"EXISTS", b"migrate-replica-key"]),
        b":0\r\n",
    )
    .await;
    send_and_expect(
        &mut target,
        &encode_resp_command(&[b"SELECT", b"9"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut target,
        &encode_resp_command(&[b"GET", b"migrate-replica-key"]),
        b"$5\r\nvalue\r\n",
    )
    .await;

    let _ = source_shutdown_tx.send(());
    let _ = target_shutdown_tx.send(());
    let _ = replica_shutdown_tx.send(());
    source_server.join().unwrap();
    target_server.join().unwrap();
    replica_server.join().unwrap();
}

#[tokio::test]
#[ignore = "exact external local redis-server MIGRATE scenario; run targeted"]
async fn migrate_to_local_redis_target_preserves_string_and_ttl_like_external_dump_scenario() {
    if !system_redis_server_available() {
        return;
    }

    let (source_addr, source_shutdown_tx, source_server) = start_test_server_on_dedicated_thread();
    wait_for_server_ping(source_addr).await;

    let redis_port = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let _redis = LocalRedisServerGuard::spawn(redis_port, "redis-migrate-target-string");
    wait_for_server_ping(std::net::SocketAddr::from(([127, 0, 0, 1], redis_port))).await;

    let mut source = TcpStream::connect(source_addr).await.unwrap();
    let mut target = TcpStream::connect(("127.0.0.1", redis_port)).await.unwrap();
    let target_port = redis_port.to_string();

    send_and_expect(
        &mut source,
        &encode_resp_command(&[b"SET", b"migrate:string", b"Some Value"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut source,
        &encode_resp_command(&[b"PEXPIRE", b"migrate:string", b"60000"]),
        b":1\r\n",
    )
    .await;
    send_and_expect(
        &mut source,
        &encode_resp_command(&[
            b"MIGRATE",
            b"127.0.0.1",
            target_port.as_bytes(),
            b"migrate:string",
            b"9",
            b"5000",
        ]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut source,
        &encode_resp_command(&[b"EXISTS", b"migrate:string"]),
        b":0\r\n",
    )
    .await;

    send_and_expect(
        &mut target,
        &encode_resp_command(&[b"SELECT", b"9"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut target,
        &encode_resp_command(&[b"GET", b"migrate:string"]),
        b"$10\r\nSome Value\r\n",
    )
    .await;
    let ttl = send_and_read_integer(
        &mut target,
        &encode_resp_command(&[b"PTTL", b"migrate:string"]),
        Duration::from_secs(2),
    )
    .await;
    assert!(ttl > 0);
    assert!(ttl <= 60_000);

    let _ = source_shutdown_tx.send(());
    source_server.join().unwrap();
}

#[tokio::test]
#[ignore = "exact external local redis-server MIGRATE scenario; run targeted"]
async fn migrate_to_local_redis_target_preserves_object_dataset_like_external_dump_scenario() {
    if !system_redis_server_available() {
        return;
    }

    let (source_addr, source_shutdown_tx, source_server) = start_test_server_on_dedicated_thread();
    wait_for_server_ping(source_addr).await;

    let redis_port = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let _redis = LocalRedisServerGuard::spawn(redis_port, "redis-migrate-target-objects");
    wait_for_server_ping(std::net::SocketAddr::from(([127, 0, 0, 1], redis_port))).await;

    let mut source = TcpStream::connect(source_addr).await.unwrap();
    let mut target = TcpStream::connect(("127.0.0.1", redis_port)).await.unwrap();
    let target_port = redis_port.to_string();
    let timeout = Duration::from_secs(2);

    send_and_expect(
        &mut source,
        &encode_resp_command(&[b"LPUSH", b"migrate:list", b"a", b"b", b"c"]),
        b":3\r\n",
    )
    .await;
    send_and_expect(
        &mut source,
        &encode_resp_command(&[
            b"HSET",
            b"migrate:hash",
            b"field1",
            b"value1",
            b"field2",
            b"value2",
        ]),
        b":2\r\n",
    )
    .await;
    send_and_expect(
        &mut source,
        &encode_resp_command(&[b"PEXPIRE", b"migrate:hash", b"60000"]),
        b":1\r\n",
    )
    .await;
    send_and_expect(
        &mut source,
        &encode_resp_command(&[b"SADD", b"migrate:set", b"alpha", b"beta", b"gamma"]),
        b":3\r\n",
    )
    .await;
    send_and_expect(
        &mut source,
        &encode_resp_command(&[b"ZADD", b"migrate:zset", b"1", b"one", b"2", b"two"]),
        b":2\r\n",
    )
    .await;
    let stream_id = send_and_read_resp_value(
        &mut source,
        &encode_resp_command(&[
            b"XADD",
            b"migrate:stream",
            b"*",
            b"field1",
            b"value1",
            b"field2",
            b"value2",
        ]),
        timeout,
    )
    .await;
    assert!(!resp_socket_bulk(&stream_id).is_empty());

    for key in [
        b"migrate:list".as_slice(),
        b"migrate:hash".as_slice(),
        b"migrate:set".as_slice(),
        b"migrate:zset".as_slice(),
        b"migrate:stream".as_slice(),
    ] {
        send_and_expect(
            &mut source,
            &encode_resp_command(&[
                b"MIGRATE",
                b"127.0.0.1",
                target_port.as_bytes(),
                key,
                b"9",
                b"5000",
            ]),
            b"+OK\r\n",
        )
        .await;
    }

    send_and_expect(
        &mut source,
        &encode_resp_command(&[
            b"EXISTS",
            b"migrate:list",
            b"migrate:hash",
            b"migrate:set",
            b"migrate:zset",
            b"migrate:stream",
        ]),
        b":0\r\n",
    )
    .await;

    send_and_expect(
        &mut target,
        &encode_resp_command(&[b"SELECT", b"9"]),
        b"+OK\r\n",
    )
    .await;

    let list = send_and_read_resp_value(
        &mut target,
        &encode_resp_command(&[b"LRANGE", b"migrate:list", b"0", b"-1"]),
        timeout,
    )
    .await;
    assert_eq!(
        resp_socket_array(&list)
            .iter()
            .map(|item| resp_socket_bulk(item).to_vec())
            .collect::<Vec<_>>(),
        vec![b"c".to_vec(), b"b".to_vec(), b"a".to_vec()]
    );
    send_and_expect(
        &mut target,
        &encode_resp_command(&[b"HGET", b"migrate:hash", b"field1"]),
        b"$6\r\nvalue1\r\n",
    )
    .await;
    let hash_ttl = send_and_read_integer(
        &mut target,
        &encode_resp_command(&[b"PTTL", b"migrate:hash"]),
        timeout,
    )
    .await;
    assert!(hash_ttl > 0);
    assert!(hash_ttl <= 60_000);
    assert_eq!(
        send_and_read_integer(
            &mut target,
            &encode_resp_command(&[b"SISMEMBER", b"migrate:set", b"alpha"]),
            timeout,
        )
        .await,
        1
    );
    let zrange = send_and_read_resp_value(
        &mut target,
        &encode_resp_command(&[b"ZRANGE", b"migrate:zset", b"0", b"-1", b"WITHSCORES"]),
        timeout,
    )
    .await;
    let zrange_items = resp_socket_array(&zrange);
    assert_eq!(resp_socket_bulk(&zrange_items[0]), b"one");
    assert_eq!(resp_socket_bulk(&zrange_items[1]), b"1");
    assert_eq!(resp_socket_bulk(&zrange_items[2]), b"two");
    assert_eq!(resp_socket_bulk(&zrange_items[3]), b"2");
    let xlen = send_and_read_integer(
        &mut target,
        &encode_resp_command(&[b"XLEN", b"migrate:stream"]),
        timeout,
    )
    .await;
    assert_eq!(xlen, 1);
    let xrange = send_and_read_resp_value(
        &mut target,
        &encode_resp_command(&[b"XRANGE", b"migrate:stream", b"-", b"+"]),
        timeout,
    )
    .await;
    let stream_entries = resp_socket_array(&xrange);
    assert_eq!(stream_entries.len(), 1);
    let stream_entry = resp_socket_array(&stream_entries[0]);
    let stream_fields = resp_socket_array(&stream_entry[1]);
    assert_eq!(resp_socket_bulk(&stream_fields[0]), b"field1");
    assert_eq!(resp_socket_bulk(&stream_fields[1]), b"value1");
    assert_eq!(resp_socket_bulk(&stream_fields[2]), b"field2");
    assert_eq!(resp_socket_bulk(&stream_fields[3]), b"value2");

    let _ = source_shutdown_tx.send(());
    source_server.join().unwrap();
}

#[tokio::test]
async fn slowlog_argument_trimming_matches_external_scenarios() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let mut client = TcpStream::connect(addr).await.unwrap();
    let timeout = Duration::from_secs(5);

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"config", b"set", b"slowlog-log-slower-than", b"0"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"slowlog", b"reset"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[
            b"sadd", b"set", b"3", b"4", b"5", b"6", b"7", b"8", b"9", b"10", b"11", b"12", b"13",
            b"14", b"15", b"16", b"17", b"18", b"19", b"20", b"21", b"22", b"23", b"24", b"25",
            b"26", b"27", b"28", b"29", b"30", b"31", b"32", b"33",
        ]),
        b":31\r\n",
    )
    .await;
    let entries = send_and_read_slowlog_entries(&mut client, b"-1", timeout).await;
    assert_eq!(
        slowlog_entry_argument_words(&entries[0]),
        vec![
            "sadd",
            "set",
            "3",
            "4",
            "5",
            "6",
            "7",
            "8",
            "9",
            "10",
            "11",
            "12",
            "13",
            "14",
            "15",
            "16",
            "17",
            "18",
            "19",
            "20",
            "21",
            "22",
            "23",
            "24",
            "25",
            "26",
            "27",
            "28",
            "29",
            "30",
            "31",
            "... (2 more arguments)",
        ]
    );
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"slowlog", b"reset"]),
        b"+OK\r\n",
    )
    .await;
    let long_argument = vec![b'A'; 129];
    client
        .write_all(&encode_resp_command(&[
            b"sadd",
            b"set",
            b"foo",
            long_argument.as_slice(),
        ]))
        .await
        .unwrap();
    let _ = read_resp_line_with_timeout(&mut client, timeout).await;
    let entries = send_and_read_slowlog_entries(&mut client, b"-1", timeout).await;
    assert_eq!(
        slowlog_entry_argument_words(&entries[0]),
        vec![
            "sadd",
            "set",
            "foo",
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA... (1 more bytes)",
        ]
    );
    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn slowlog_rewritten_and_blocking_commands_match_external_scenarios() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    let mut controller = TcpStream::connect(addr).await.unwrap();
    let mut waiter = TcpStream::connect(addr).await.unwrap();
    let mut inspector = TcpStream::connect(addr).await.unwrap();
    let mut blocked = TcpStream::connect(addr).await.unwrap();
    let timeout = Duration::from_secs(5);

    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"config", b"set", b"slowlog-log-slower-than", b"0"]),
        b"+OK\r\n",
    )
    .await;

    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"sadd", b"set", b"a", b"b", b"c", b"d", b"e"]),
        b":5\r\n",
    )
    .await;
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"slowlog", b"reset"]),
        b"+OK\r\n",
    )
    .await;
    let spop = send_and_read_resp_value(
        &mut controller,
        &encode_resp_command(&[b"spop", b"set", b"10"]),
        timeout,
    )
    .await;
    assert_eq!(resp_socket_array(&spop).len(), 5);
    let entries = send_and_read_slowlog_entries(&mut controller, b"-1", timeout).await;
    assert_eq!(slowlog_entry_text(&entries[0]), "spop set 10");

    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"slowlog", b"reset"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[
            b"geoadd",
            b"cool-cities",
            b"-122.33207",
            b"47.60621",
            b"Seattle",
        ]),
        b":1\r\n",
    )
    .await;
    let entries = send_and_read_slowlog_entries(&mut controller, b"-1", timeout).await;
    assert_eq!(
        slowlog_entry_text(&entries[0]),
        "geoadd cool-cities -122.33207 47.60621 Seattle"
    );

    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"set", b"A", b"5"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"slowlog", b"reset"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"getset", b"a", b"5"]),
        b"$-1\r\n",
    )
    .await;
    let entries = send_and_read_slowlog_entries(&mut controller, b"-1", timeout).await;
    assert_eq!(slowlog_entry_text(&entries[0]), "getset a 5");

    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"set", b"A", b"0"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"slowlog", b"reset"]),
        b"+OK\r\n",
    )
    .await;
    let _ = send_and_read_resp_value(
        &mut controller,
        &encode_resp_command(&[b"INCRBYFLOAT", b"A", b"1.0"]),
        timeout,
    )
    .await;
    let entries = send_and_read_slowlog_entries(&mut controller, b"-1", timeout).await;
    assert_eq!(slowlog_entry_text(&entries[0]), "INCRBYFLOAT A 1.0");

    waiter
        .write_all(&encode_resp_command(&[b"blpop", b"l", b"0"]))
        .await
        .unwrap();
    wait_for_blocked_clients(&mut inspector, 1, Duration::from_secs(1)).await;
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"multi"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"lpush", b"l", b"foo"]),
        b"+QUEUED\r\n",
    )
    .await;
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"slowlog", b"reset"]),
        b"+QUEUED\r\n",
    )
    .await;
    let exec =
        send_and_read_resp_value(&mut controller, &encode_resp_command(&[b"exec"]), timeout).await;
    assert_eq!(resp_socket_array(&exec).len(), 2);
    let waiter_reply = read_resp_value_with_timeout(&mut waiter, timeout).await;
    assert_eq!(resp_socket_array(&waiter_reply).len(), 2);
    let entries = send_and_read_slowlog_entries(&mut controller, b"-1", timeout).await;
    assert_eq!(slowlog_entry_text(&entries[0]), "blpop l 0");

    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"del", b"mylist"]),
        b":0\r\n",
    )
    .await;
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"slowlog", b"reset"]),
        b"+OK\r\n",
    )
    .await;
    blocked
        .write_all(&encode_resp_command(&[b"BLPOP", b"mylist", b"0"]))
        .await
        .unwrap();
    wait_for_blocked_clients(&mut inspector, 1, Duration::from_secs(1)).await;
    assert!(
        !slowlog_entry_texts(&send_and_read_slowlog_entries(&mut controller, b"-1", timeout).await)
            .iter()
            .any(|entry| entry == "BLPOP mylist 0")
    );
    send_and_expect(
        &mut controller,
        &encode_resp_command(&[b"lpush", b"mylist", b"1"]),
        b":1\r\n",
    )
    .await;
    wait_for_blocked_clients(&mut inspector, 0, Duration::from_secs(1)).await;
    let blocked_reply = read_resp_value_with_timeout(&mut blocked, timeout).await;
    assert_eq!(resp_socket_array(&blocked_reply).len(), 2);
    assert!(
        slowlog_entry_texts(&send_and_read_slowlog_entries(&mut controller, b"-1", timeout).await)
            .iter()
            .filter(|entry| *entry == "BLPOP mylist 0")
            .count()
            == 1
    );

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn scan_with_expired_keys_type_filter_and_pattern_filter_matches_external_scenario() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    wait_for_server_ping(addr).await;

    let mut client = TcpStream::connect(addr).await.unwrap();
    let timeout = Duration::from_secs(1);

    send_and_expect(&mut client, &encode_resp_command(&[b"FLUSHDB"]), b"+OK\r\n").await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEBUG", b"SET-ACTIVE-EXPIRE", b"0"]),
        b"+OK\r\n",
    )
    .await;

    for index in 0..1000 {
        let key = format!("key:{index}");
        send_and_expect(
            &mut client,
            &encode_resp_command(&[b"SET", key.as_bytes(), b"value"]),
            b"+OK\r\n",
        )
        .await;
    }

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"key:foo", b"bar"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"PEXPIRE", b"key:foo", b"1"]),
        b":1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"HSET", b"key:hash", b"f", b"v"]),
        b":1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"PEXPIRE", b"key:hash", b"1"]),
        b":1\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"boo", b"far"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"PEXPIRE", b"boo", b"1"]),
        b":1\r\n",
    )
    .await;

    sleep(Duration::from_millis(10)).await;

    let mut cursor = b"0".to_vec();
    let mut seen = 0usize;
    loop {
        let response = send_and_read_resp_value(
            &mut client,
            &encode_resp_command(&[
                b"SCAN",
                cursor.as_slice(),
                b"TYPE",
                b"string",
                b"MATCH",
                b"key*",
                b"COUNT",
                b"10",
            ]),
            timeout,
        )
        .await;
        let items = resp_socket_array(&response);
        assert_eq!(
            items.len(),
            2,
            "SCAN reply should have cursor and key array"
        );
        cursor = resp_socket_bulk(&items[0]).to_vec();
        seen += resp_socket_array(&items[1]).len();
        if cursor == b"0" {
            break;
        }
    }
    assert_eq!(
        seen, 1000,
        "SCAN should only return the non-expired key* strings"
    );

    let keyspace = send_and_read_bulk_payload(
        &mut client,
        &encode_resp_command(&[b"INFO", b"keyspace"]),
        timeout,
    )
    .await;
    let keyspace_text = String::from_utf8(keyspace).unwrap();
    assert!(
        keyspace_text.contains("db0:keys=1001,"),
        "INFO keyspace should keep the non-matching expired key visible until accessed: {keyspace_text}"
    );

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEBUG", b"SET-ACTIVE-EXPIRE", b"1"]),
        b"+OK\r\n",
    )
    .await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn monitor_receives_set_events_when_subscribed_like_external_scenario() {
    let server =
        TestServerHandle::start_with_processor(Arc::new(RequestProcessor::new().unwrap())).await;
    let mut monitor = server.connect().await;
    let mut writer = server.connect().await;

    send_and_expect(
        &mut monitor,
        &encode_resp_command(&[b"MONITOR"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut writer,
        &encode_resp_command(&[b"SET", b"monitor:key", b"value"]),
        b"+OK\r\n",
    )
    .await;

    let event = read_resp_line_with_timeout(&mut monitor, Duration::from_secs(1)).await;
    let event_text = String::from_utf8_lossy(&event).to_ascii_lowercase();
    assert!(
        event_text.contains("\"set\""),
        "unexpected monitor event: {event_text}"
    );
    assert!(
        event_text.contains("\"monitor:key\""),
        "unexpected monitor event: {event_text}"
    );
    assert!(
        event_text.contains("\"value\""),
        "unexpected monitor event: {event_text}"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn tracking_gets_notification_of_expired_keys_like_external_scenario() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    wait_for_server_ping(addr).await;

    let timeout = Duration::from_secs(1);
    let mut writer = TcpStream::connect(addr).await.unwrap();
    let mut redirect = TcpStream::connect(addr).await.unwrap();

    let redirect_id = send_and_read_integer(
        &mut redirect,
        &encode_resp_command(&[b"CLIENT", b"ID"]),
        timeout,
    )
    .await;
    let redirect_id_text = redirect_id.to_string();

    send_and_expect(
        &mut writer,
        &encode_resp_command(&[b"CLIENT", b"TRACKING", b"OFF"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut writer,
        &encode_resp_command(&[
            b"CLIENT",
            b"TRACKING",
            b"ON",
            b"BCAST",
            b"REDIRECT",
            redirect_id_text.as_bytes(),
            b"NOLOOP",
        ]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut writer,
        &encode_resp_command(&[b"SET", b"mykey", b"myval", b"PX", b"1"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut writer,
        &encode_resp_command(&[b"SET", b"mykeyotherkey", b"myval"]),
        b"+OK\r\n",
    )
    .await;

    sleep(Duration::from_millis(1000)).await;

    let message = read_resp_value_with_timeout(&mut redirect, Duration::from_secs(3)).await;
    let items = resp_socket_array(&message);
    assert_eq!(
        items.len(),
        3,
        "tracking invalidation should use RESP2 pubsub message"
    );
    assert_eq!(resp_socket_bulk(&items[0]), b"message");
    assert_eq!(resp_socket_bulk(&items[1]), b"__redis__:invalidate");
    assert_eq!(resp_socket_bulk(&items[2]), b"mykey");

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn client_tracking_optin_only_tracks_reads_after_caching_yes() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    wait_for_server_ping(addr).await;

    let timeout = Duration::from_secs(1);
    let mut writer = TcpStream::connect(addr).await.unwrap();
    let mut redirect = TcpStream::connect(addr).await.unwrap();

    let redirect_id = send_and_read_integer(
        &mut redirect,
        &encode_resp_command(&[b"CLIENT", b"ID"]),
        timeout,
    )
    .await;
    let redirect_id_text = redirect_id.to_string();

    send_and_expect(
        &mut writer,
        &encode_resp_command(&[
            b"CLIENT",
            b"TRACKING",
            b"ON",
            b"OPTIN",
            b"REDIRECT",
            redirect_id_text.as_bytes(),
        ]),
        b"+OK\r\n",
    )
    .await;

    send_and_expect(
        &mut writer,
        &encode_resp_command(&[b"SET", b"tracked", b"v1"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut writer,
        &encode_resp_command(&[b"GET", b"tracked"]),
        b"$2\r\nv1\r\n",
    )
    .await;
    send_and_expect(
        &mut writer,
        &encode_resp_command(&[b"SET", b"tracked", b"v2"]),
        b"+OK\r\n",
    )
    .await;

    let mut peek_probe = [0u8; 1];
    assert!(
        tokio::time::timeout(Duration::from_millis(200), redirect.peek(&mut peek_probe))
            .await
            .is_err(),
        "OPTIN without CLIENT CACHING YES must not register read tracking"
    );

    send_and_expect(
        &mut writer,
        &encode_resp_command(&[b"CLIENT", b"CACHING", b"YES"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut writer,
        &encode_resp_command(&[b"GET", b"tracked"]),
        b"$2\r\nv2\r\n",
    )
    .await;
    send_and_expect(
        &mut writer,
        &encode_resp_command(&[b"SET", b"tracked", b"v3"]),
        b"+OK\r\n",
    )
    .await;

    let message = read_resp_value_with_timeout(&mut redirect, Duration::from_secs(1)).await;
    let items = resp_socket_array(&message);
    assert_eq!(items.len(), 3);
    assert_eq!(resp_socket_bulk(&items[0]), b"message");
    assert_eq!(resp_socket_bulk(&items[1]), b"__redis__:invalidate");
    assert_eq!(resp_socket_bulk(&items[2]), b"tracked");

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
    match tokio::time::timeout(Duration::from_secs(1), client.read_exact(&mut actual)).await {
        Ok(Ok(_)) => {}
        Ok(Err(error)) => {
            panic!(
                "read_exact failed for request {:?}: {}",
                String::from_utf8_lossy(request),
                error
            );
        }
        Err(_) => {
            panic!(
                "timed out waiting for response to request {:?}; expected {:?}",
                String::from_utf8_lossy(request),
                String::from_utf8_lossy(expected_response)
            );
        }
    }
    assert_eq!(
        actual,
        expected_response,
        "unexpected response for request {:?}",
        String::from_utf8_lossy(request)
    );
}

async fn send_and_read_integer(client: &mut TcpStream, request: &[u8], timeout: Duration) -> i64 {
    client.write_all(request).await.unwrap();
    let line = read_resp_line_with_timeout(client, timeout).await;
    assert!(line.starts_with(b":"));
    std::str::from_utf8(&line[1..])
        .unwrap()
        .parse::<i64>()
        .unwrap()
}

async fn send_and_read_error_line(
    client: &mut TcpStream,
    request: &[u8],
    timeout: Duration,
) -> String {
    client.write_all(request).await.unwrap();
    let line = read_resp_line_with_timeout(client, timeout).await;
    assert!(
        line.starts_with(b"-"),
        "expected error response, got: {:?}",
        String::from_utf8_lossy(&line)
    );
    String::from_utf8(line[1..].to_vec()).unwrap()
}

async fn read_bulk_payload_with_timeout(client: &mut TcpStream, timeout: Duration) -> Vec<u8> {
    let header = read_resp_line_with_timeout(client, timeout).await;
    assert!(header.starts_with(b"$"));
    let payload_len = std::str::from_utf8(&header[1..])
        .unwrap()
        .parse::<usize>()
        .unwrap();
    let mut payload = vec![0u8; payload_len + 2];
    tokio::time::timeout(timeout, client.read_exact(&mut payload))
        .await
        .unwrap()
        .unwrap();
    payload.truncate(payload_len);
    payload
}

async fn read_sync_snapshot_payload_with_timeout(
    client: &mut TcpStream,
    timeout: Duration,
) -> Vec<u8> {
    let header = read_resp_line_with_timeout(client, timeout).await;
    assert!(header.starts_with(b"$"));
    let payload_len = std::str::from_utf8(&header[1..])
        .unwrap()
        .parse::<usize>()
        .unwrap();
    read_exact_with_timeout(client, payload_len, timeout).await
}

async fn send_and_read_bulk_payload(
    client: &mut TcpStream,
    request: &[u8],
    timeout: Duration,
) -> Vec<u8> {
    client.write_all(request).await.unwrap();
    read_bulk_payload_with_timeout(client, timeout).await
}

async fn reply_buffer_size_for_named_client(client: &mut TcpStream, name: &str) -> usize {
    let payload = send_and_read_bulk_payload(
        client,
        &encode_resp_command(&[b"CLIENT", b"LIST"]),
        Duration::from_secs(1),
    )
    .await;
    let text = std::str::from_utf8(&payload).unwrap();
    for line in text.split("\r\n") {
        let Some(line_name) = client_list_field_value(line, "name") else {
            continue;
        };
        if line_name != name {
            continue;
        }
        let reply_buffer_size = client_list_field_value(line, "rbs")
            .unwrap_or_else(|| panic!("rbs field not found in {line}"));
        return reply_buffer_size.parse::<usize>().unwrap();
    }
    panic!(
        "client named `{name}` not found in CLIENT LIST payload: {}",
        String::from_utf8_lossy(&payload)
    );
}

async fn query_buffer_total_for_named_client(client: &mut TcpStream, name: &str) -> usize {
    let payload = send_and_read_bulk_payload(
        client,
        &encode_resp_command(&[b"CLIENT", b"LIST"]),
        Duration::from_secs(1),
    )
    .await;
    let text = std::str::from_utf8(&payload).unwrap();
    let line = client_list_line_with_name(text, name).unwrap_or_else(|| {
        panic!(
            "client named `{name}` not found in CLIENT LIST payload: {}",
            String::from_utf8_lossy(&payload)
        )
    });
    let qbuf = client_list_field_value(line, "qbuf")
        .unwrap_or_else(|| panic!("qbuf field not found in {line}"))
        .parse::<usize>()
        .unwrap();
    let qbuf_free = client_list_field_value(line, "qbuf-free")
        .unwrap_or_else(|| panic!("qbuf-free field not found in {line}"))
        .parse::<usize>()
        .unwrap();
    qbuf + qbuf_free
}

async fn client_idle_seconds_for_named_client(client: &mut TcpStream, name: &str) -> u64 {
    let payload = send_and_read_bulk_payload(
        client,
        &encode_resp_command(&[b"CLIENT", b"LIST"]),
        Duration::from_secs(1),
    )
    .await;
    let text = std::str::from_utf8(&payload).unwrap();
    let line = client_list_line_with_name(text, name).unwrap_or_else(|| {
        panic!(
            "client named `{name}` not found in CLIENT LIST payload: {}",
            String::from_utf8_lossy(&payload)
        )
    });
    client_list_field_value(line, "idle")
        .unwrap_or_else(|| panic!("idle field not found in {line}"))
        .parse::<u64>()
        .unwrap()
}

fn client_list_line_with_name<'a>(payload: &'a str, name: &str) -> Option<&'a str> {
    payload
        .split("\r\n")
        .find(|line| client_list_field_value(line, "name") == Some(name))
}

fn client_list_line_with_command<'a>(payload: &'a str, command: &str) -> Option<&'a str> {
    payload
        .split("\r\n")
        .find(|line| client_list_field_value(line, "cmd") == Some(command))
}

fn client_list_field_value<'a>(line: &'a str, field: &str) -> Option<&'a str> {
    for part in line.split(' ') {
        if let Some(value) = part.strip_prefix(field) {
            return value.strip_prefix('=');
        }
    }
    None
}

async fn read_bulk_array_payloads_with_timeout(
    client: &mut TcpStream,
    timeout: Duration,
) -> Vec<Vec<u8>> {
    let header = read_resp_line_with_timeout(client, timeout).await;
    assert!(header.starts_with(b"*"));
    let len = std::str::from_utf8(&header[1..])
        .unwrap()
        .parse::<usize>()
        .unwrap();
    let mut values = Vec::with_capacity(len);
    for _ in 0..len {
        values.push(read_bulk_payload_with_timeout(client, timeout).await);
    }
    values
}

async fn send_and_read_bulk_array_payloads(
    client: &mut TcpStream,
    request: &[u8],
    timeout: Duration,
) -> Vec<Vec<u8>> {
    client.write_all(request).await.unwrap();
    read_bulk_array_payloads_with_timeout(client, timeout).await
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RespSocketValue {
    Integer(i64),
    Bulk(Vec<u8>),
    Simple(Vec<u8>),
    Error(Vec<u8>),
    Array(Vec<RespSocketValue>),
    Map(Vec<(RespSocketValue, RespSocketValue)>),
    Set(Vec<RespSocketValue>),
    Boolean(bool),
    Double(Vec<u8>),
    BigNumber(Vec<u8>),
    Verbatim { format: Vec<u8>, value: Vec<u8> },
    Null,
}

fn read_resp_value_with_timeout<'a>(
    client: &'a mut TcpStream,
    timeout: Duration,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = RespSocketValue> + 'a>> {
    Box::pin(async move {
        let header = read_resp_line_with_timeout(client, timeout).await;
        match header.first().copied().unwrap() {
            b':' => RespSocketValue::Integer(
                std::str::from_utf8(&header[1..])
                    .unwrap()
                    .parse::<i64>()
                    .unwrap(),
            ),
            b'+' => RespSocketValue::Simple(header[1..].to_vec()),
            b'-' => RespSocketValue::Error(header[1..].to_vec()),
            b'$' => {
                if header == b"$-1" {
                    return RespSocketValue::Null;
                }
                let payload_len = std::str::from_utf8(&header[1..])
                    .unwrap()
                    .parse::<usize>()
                    .unwrap();
                let mut payload = vec![0u8; payload_len + 2];
                tokio::time::timeout(timeout, client.read_exact(&mut payload))
                    .await
                    .unwrap()
                    .unwrap();
                payload.truncate(payload_len);
                RespSocketValue::Bulk(payload)
            }
            b'*' => {
                let len = std::str::from_utf8(&header[1..])
                    .unwrap()
                    .parse::<usize>()
                    .unwrap();
                let mut items = Vec::with_capacity(len);
                for _ in 0..len {
                    items.push(read_resp_value_with_timeout(client, timeout).await);
                }
                RespSocketValue::Array(items)
            }
            b'%' => {
                let len = std::str::from_utf8(&header[1..])
                    .unwrap()
                    .parse::<usize>()
                    .unwrap();
                let mut items = Vec::with_capacity(len);
                for _ in 0..len {
                    let key = read_resp_value_with_timeout(client, timeout).await;
                    let value = read_resp_value_with_timeout(client, timeout).await;
                    items.push((key, value));
                }
                RespSocketValue::Map(items)
            }
            b'~' => {
                let len = std::str::from_utf8(&header[1..])
                    .unwrap()
                    .parse::<usize>()
                    .unwrap();
                let mut items = Vec::with_capacity(len);
                for _ in 0..len {
                    items.push(read_resp_value_with_timeout(client, timeout).await);
                }
                RespSocketValue::Set(items)
            }
            b'>' => {
                let len = std::str::from_utf8(&header[1..])
                    .unwrap()
                    .parse::<usize>()
                    .unwrap();
                let mut items = Vec::with_capacity(len);
                for _ in 0..len {
                    items.push(read_resp_value_with_timeout(client, timeout).await);
                }
                RespSocketValue::Array(items)
            }
            b'#' => RespSocketValue::Boolean(&header[1..] == b"t"),
            b',' => RespSocketValue::Double(header[1..].to_vec()),
            b'(' => RespSocketValue::BigNumber(header[1..].to_vec()),
            b'=' => {
                let payload_len = std::str::from_utf8(&header[1..])
                    .unwrap()
                    .parse::<usize>()
                    .unwrap();
                let mut payload = vec![0u8; payload_len + 2];
                tokio::time::timeout(timeout, client.read_exact(&mut payload))
                    .await
                    .unwrap()
                    .unwrap();
                payload.truncate(payload_len);
                let separator = payload.iter().position(|byte| *byte == b':').unwrap();
                RespSocketValue::Verbatim {
                    format: payload[..separator].to_vec(),
                    value: payload[separator + 1..].to_vec(),
                }
            }
            b'_' => RespSocketValue::Null,
            other => panic!("unsupported RESP token from socket: {}", other as char),
        }
    })
}

async fn send_and_read_resp_value(
    client: &mut TcpStream,
    request: &[u8],
    timeout: Duration,
) -> RespSocketValue {
    client.write_all(request).await.unwrap();
    read_resp_value_with_timeout(client, timeout).await
}

fn resp_socket_array(value: &RespSocketValue) -> &[RespSocketValue] {
    match value {
        RespSocketValue::Array(items) => items,
        other => panic!("expected RESP array, got {other:?}"),
    }
}

fn resp_socket_bulk(value: &RespSocketValue) -> &[u8] {
    match value {
        RespSocketValue::Bulk(payload) => payload,
        RespSocketValue::Simple(payload) => payload,
        RespSocketValue::Error(payload) => payload,
        other => panic!("expected RESP bulk/simple string, got {other:?}"),
    }
}

fn resp_socket_contains_bulk(value: &RespSocketValue, needle: &[u8]) -> bool {
    match value {
        RespSocketValue::Bulk(payload)
        | RespSocketValue::Simple(payload)
        | RespSocketValue::Error(payload)
        | RespSocketValue::Double(payload)
        | RespSocketValue::BigNumber(payload) => payload == needle,
        RespSocketValue::Verbatim { format, value } => format == needle || value == needle,
        RespSocketValue::Array(items) | RespSocketValue::Set(items) => items
            .iter()
            .any(|item| resp_socket_contains_bulk(item, needle)),
        RespSocketValue::Map(items) => items.iter().any(|(key, value)| {
            resp_socket_contains_bulk(key, needle) || resp_socket_contains_bulk(value, needle)
        }),
        RespSocketValue::Integer(_) | RespSocketValue::Boolean(_) | RespSocketValue::Null => false,
    }
}

fn resp_socket_integer(value: &RespSocketValue) -> i64 {
    match value {
        RespSocketValue::Integer(number) => *number,
        other => panic!("expected RESP integer, got {other:?}"),
    }
}

fn resp_socket_integer_or_bulk(value: &RespSocketValue) -> i64 {
    match value {
        RespSocketValue::Integer(number) => *number,
        RespSocketValue::Bulk(bytes) => std::str::from_utf8(bytes).unwrap().parse::<i64>().unwrap(),
        other => panic!("expected RESP integer/bulk integer, got {other:?}"),
    }
}

fn resp_socket_integer_array(value: &RespSocketValue) -> Vec<i64> {
    resp_socket_array(value)
        .iter()
        .map(resp_socket_integer)
        .collect()
}

async fn read_replication_command_with_timeout(
    client: &mut TcpStream,
    timeout: Duration,
) -> Vec<Vec<u8>> {
    let value = read_resp_value_with_timeout(client, timeout).await;
    resp_socket_array(&value)
        .iter()
        .map(|item| resp_socket_bulk(item).to_vec())
        .collect()
}

async fn send_and_read_slowlog_entries(
    client: &mut TcpStream,
    count: &[u8],
    timeout: Duration,
) -> Vec<RespSocketValue> {
    let response = send_and_read_resp_value(
        client,
        &encode_resp_command(&[b"SLOWLOG", b"GET", count]),
        timeout,
    )
    .await;
    resp_socket_array(&response).to_vec()
}

fn slowlog_entry_argument_words(entry: &RespSocketValue) -> Vec<String> {
    let items = resp_socket_array(entry);
    resp_socket_array(&items[3])
        .iter()
        .map(|value| String::from_utf8_lossy(resp_socket_bulk(value)).into_owned())
        .collect()
}

fn slowlog_entry_text(entry: &RespSocketValue) -> String {
    slowlog_entry_argument_words(entry).join(" ")
}

fn slowlog_entry_texts(entries: &[RespSocketValue]) -> Vec<String> {
    entries.iter().map(slowlog_entry_text).collect()
}

fn resp_socket_flat_map<'a>(
    value: &'a RespSocketValue,
) -> std::collections::BTreeMap<Vec<u8>, &'a RespSocketValue> {
    let items = resp_socket_array(value);
    assert_eq!(items.len() % 2, 0);
    let mut map = std::collections::BTreeMap::new();
    let mut index = 0usize;
    while index < items.len() {
        let key = resp_socket_bulk(&items[index]).to_vec();
        map.insert(key, &items[index + 1]);
        index += 2;
    }
    map
}

fn resp_socket_map_or_flat_map<'a>(
    value: &'a RespSocketValue,
) -> std::collections::BTreeMap<Vec<u8>, &'a RespSocketValue> {
    match value {
        RespSocketValue::Map(items) => {
            let mut map = std::collections::BTreeMap::new();
            for (key, value) in items {
                map.insert(resp_socket_bulk(key).to_vec(), value);
            }
            map
        }
        RespSocketValue::Array(_) => resp_socket_flat_map(value),
        other => panic!("expected RESP map/flat map, got {other:?}"),
    }
}

async fn read_zmpop_like_response(
    client: &mut TcpStream,
    timeout: Duration,
) -> (Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>) {
    let header = read_resp_line_with_timeout(client, timeout).await;
    assert_eq!(header, b"*2");
    let key = read_bulk_payload_with_timeout(client, timeout).await;
    let members_header = read_resp_line_with_timeout(client, timeout).await;
    assert!(members_header.starts_with(b"*"));
    let member_count = std::str::from_utf8(&members_header[1..])
        .unwrap()
        .parse::<usize>()
        .unwrap();
    let mut members = Vec::with_capacity(member_count);
    for _ in 0..member_count {
        let pair_header = read_resp_line_with_timeout(client, timeout).await;
        assert_eq!(pair_header, b"*2");
        let member = read_bulk_payload_with_timeout(client, timeout).await;
        let score = read_bulk_payload_with_timeout(client, timeout).await;
        members.push((member, score));
    }
    (key, members)
}

async fn send_and_read_debug_htstats_key_payload(
    client: &mut TcpStream,
    key: &[u8],
    full: bool,
    timeout: Duration,
) -> Vec<u8> {
    if full {
        send_and_read_bulk_payload(
            client,
            &encode_resp_command(&[b"DEBUG", b"HTSTATS-KEY", key, b"full"]),
            timeout,
        )
        .await
    } else {
        send_and_read_bulk_payload(
            client,
            &encode_resp_command(&[b"DEBUG", b"HTSTATS-KEY", key]),
            timeout,
        )
        .await
    }
}

async fn debug_htstats_key_is_rehashing(
    client: &mut TcpStream,
    key: &[u8],
    timeout: Duration,
) -> bool {
    let payload = send_and_read_debug_htstats_key_payload(client, key, false, timeout).await;
    String::from_utf8(payload)
        .unwrap()
        .contains("rehashing target")
}

async fn wait_for_set_rehashing_to_complete_like_redis_external_test(
    client: &mut TcpStream,
    key: &[u8],
    sample_count: &[u8],
    timeout: Duration,
) {
    let mut remaining_iterations = 256usize;
    while debug_htstats_key_is_rehashing(client, key, timeout).await {
        if remaining_iterations == 0 {
            let payload = send_and_read_debug_htstats_key_payload(client, key, true, timeout).await;
            panic!(
                "DEBUG HTSTATS-KEY kept reporting rehashing after 256 iterations: {}",
                String::from_utf8(payload).unwrap()
            );
        }
        remaining_iterations -= 1;
        let request = encode_resp_command(&[b"SRANDMEMBER", key, sample_count]);
        assert!(
            !send_and_read_bulk_array_payloads(client, &request, timeout)
                .await
                .is_empty()
        );
    }
}

async fn send_and_read_scan_cursor_and_members(
    client: &mut TcpStream,
    request: &[u8],
    timeout: Duration,
) -> (u64, Vec<Vec<u8>>) {
    client.write_all(request).await.unwrap();
    let header = read_resp_line_with_timeout(client, timeout).await;
    assert_eq!(header, b"*2");
    let cursor = read_bulk_payload_with_timeout(client, timeout).await;
    let cursor = std::str::from_utf8(&cursor)
        .unwrap()
        .parse::<u64>()
        .unwrap();
    let members = read_bulk_array_payloads_with_timeout(client, timeout).await;
    (cursor, members)
}

async fn send_and_read_optional_bulk(
    client: &mut TcpStream,
    request: &[u8],
    timeout: Duration,
) -> Option<Vec<u8>> {
    client.write_all(request).await.unwrap();
    let header = read_resp_line_with_timeout(client, timeout).await;
    if header == b"$-1" {
        return None;
    }
    assert!(header.starts_with(b"$"));
    let payload_len = std::str::from_utf8(&header[1..])
        .unwrap()
        .parse::<usize>()
        .unwrap();
    let mut payload = vec![0u8; payload_len + 2];
    tokio::time::timeout(timeout, client.read_exact(&mut payload))
        .await
        .unwrap()
        .unwrap();
    payload.truncate(payload_len);
    Some(payload)
}

#[tokio::test]
async fn bgsave_then_debug_reload_preserves_current_value_like_external_other_scenario() {
    let (addr, shutdown_tx, server) = start_test_server().await;
    wait_for_server_ping(addr).await;

    let mut client = TcpStream::connect(addr).await.unwrap();

    // Redis tests/unit/other.tcl:
    // - "BGSAVE"
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"FLUSHALL"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(&mut client, &encode_resp_command(&[b"SAVE"]), b"+OK\r\n").await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"x", b"10"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"BGSAVE"]),
        b"+Background saving started\r\n",
    )
    .await;
    wait_for_bgsave_to_finish(&mut client, Duration::from_secs(5)).await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEBUG", b"RELOAD"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"GET", b"x"]),
        b"$2\r\n10\r\n",
    )
    .await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn save_and_bgsave_persist_dump_file_and_debug_reload_nosave_uses_it() {
    let temp_dir = unique_test_temp_dir("save-bgsave-dump-file");
    let dump_path = temp_dir.join("dump.rdb");
    let (addr, shutdown_tx, server) = start_test_server().await;
    wait_for_server_ping(addr).await;

    let mut client = TcpStream::connect(addr).await.unwrap();
    send_and_expect(
        &mut client,
        &encode_resp_command(&[
            b"CONFIG",
            b"SET",
            b"dir",
            temp_dir.to_string_lossy().as_bytes(),
        ]),
        b"+OK\r\n",
    )
    .await;

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"persist:key", b"alpha"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(&mut client, &encode_resp_command(&[b"SAVE"]), b"+OK\r\n").await;
    let save_snapshot = std::fs::read(&dump_path).unwrap();
    assert!(
        save_snapshot.starts_with(b"REDIS0011") || save_snapshot.starts_with(b"REDIS0012"),
        "SAVE should persist a real RDB snapshot, got {:?}",
        &save_snapshot[..save_snapshot.len().min(16)]
    );

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"FLUSHALL"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEBUG", b"RELOAD", b"NOSAVE"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"GET", b"persist:key"]),
        b"$5\r\nalpha\r\n",
    )
    .await;

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"SET", b"persist:key", b"beta"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"BGSAVE"]),
        b"+Background saving started\r\n",
    )
    .await;
    wait_for_bgsave_to_finish(&mut client, Duration::from_secs(5)).await;
    let bgsave_snapshot = std::fs::read(&dump_path).unwrap();
    assert!(
        bgsave_snapshot.starts_with(b"REDIS0011") || bgsave_snapshot.starts_with(b"REDIS0012"),
        "BGSAVE should persist a real RDB snapshot, got {:?}",
        &bgsave_snapshot[..bgsave_snapshot.len().min(16)]
    );

    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"FLUSHALL"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"DEBUG", b"RELOAD", b"NOSAVE"]),
        b"+OK\r\n",
    )
    .await;
    send_and_expect(
        &mut client,
        &encode_resp_command(&[b"GET", b"persist:key"]),
        b"$4\r\nbeta\r\n",
    )
    .await;

    let _ = shutdown_tx.send(());
    server.await.unwrap();
    let _ = std::fs::remove_dir_all(temp_dir);
}

async fn wait_for_bgsave_to_finish(client: &mut TcpStream, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let payload = send_and_read_bulk_payload(
            client,
            &encode_resp_command(&[b"INFO"]),
            Duration::from_secs(1),
        )
        .await;
        if read_info_u64(&payload, "rdb_bgsave_in_progress") == Some(0) {
            return;
        }
        sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for BGSAVE to finish");
}

async fn create_set_like_redis_external_test(
    client: &mut TcpStream,
    key: &[u8],
    member_count: usize,
) {
    let _ = send_and_read_integer(
        client,
        &encode_resp_command(&[b"DEL", key]),
        Duration::from_secs(5),
    )
    .await;
    for index in 0..member_count {
        let member = format!("m:{index}");
        assert_eq!(
            send_and_read_integer(
                client,
                &encode_resp_command(&[b"SADD", key, member.as_bytes()]),
                Duration::from_secs(5),
            )
            .await,
            1
        );
    }
}

async fn rem_hash_set_top_n_like_redis_external_test(
    client: &mut TcpStream,
    key: &[u8],
    remove_count: usize,
) {
    let mut cursor = 0u64;
    let mut members_to_remove = Vec::with_capacity(remove_count);
    loop {
        let cursor_text = cursor.to_string();
        let (next_cursor, members) = send_and_read_scan_cursor_and_members(
            client,
            &encode_resp_command(&[b"SSCAN", key, cursor_text.as_bytes()]),
            Duration::from_secs(5),
        )
        .await;
        for member in members {
            members_to_remove.push(member);
            if members_to_remove.len() >= remove_count {
                break;
            }
        }
        if members_to_remove.len() >= remove_count || next_cursor == 0 {
            break;
        }
        cursor = next_cursor;
    }

    assert_eq!(members_to_remove.len(), remove_count);

    for member in &members_to_remove {
        client
            .write_all(&encode_resp_command(&[b"SREM", key, member.as_slice()]))
            .await
            .unwrap();
    }
    for _ in &members_to_remove {
        assert_eq!(
            read_resp_line_with_timeout(client, Duration::from_secs(5)).await,
            b":1"
        );
    }
}

fn chi_square_value(samples: &[Vec<u8>], population: &std::collections::BTreeSet<Vec<u8>>) -> f64 {
    if samples.is_empty() || population.is_empty() {
        return 0.0;
    }

    let expected = samples.len() as f64 / population.len() as f64;
    let mut observed = std::collections::BTreeMap::<&[u8], usize>::new();
    for sample in samples {
        *observed.entry(sample.as_slice()).or_default() += 1;
    }

    population
        .iter()
        .map(|member| {
            let observed_count = *observed.get(member.as_slice()).unwrap_or(&0) as f64;
            let delta = observed_count - expected;
            (delta * delta) / expected
        })
        .sum()
}

async fn read_resp_line_with_timeout(stream: &mut TcpStream, timeout: Duration) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let mut byte = [0u8; 1];
        tokio::time::timeout(timeout, stream.read_exact(&mut byte))
            .await
            .unwrap()
            .unwrap();
        out.push(byte[0]);
        if out.ends_with(b"\r\n") {
            out.truncate(out.len() - 2);
            return out;
        }
    }
}

async fn read_exact_with_timeout(
    stream: &mut TcpStream,
    expected_len: usize,
    timeout: Duration,
) -> Vec<u8> {
    let mut response = vec![0u8; expected_len];
    tokio::time::timeout(timeout, stream.read_exact(&mut response))
        .await
        .unwrap()
        .unwrap();
    response
}

/// Send a HELLO command and read/drain the full server-info map response.
async fn send_hello_and_drain(client: &mut TcpStream, version: &[u8]) {
    let frame = format!(
        "*2\r\n$5\r\nHELLO\r\n${}\r\n{}\r\n",
        version.len(),
        std::str::from_utf8(version).unwrap()
    );
    client.write_all(frame.as_bytes()).await.unwrap();
    // Read the header line (%7 or *14).
    let header = read_resp_line_with_timeout(client, Duration::from_secs(1)).await;
    let is_map = header.starts_with(b"%");
    let is_array = header.starts_with(b"*");
    assert!(
        is_map || is_array,
        "expected map or array header from HELLO, got: {:?}",
        String::from_utf8_lossy(&header)
    );
    // Drain remaining lines: 7 key-value pairs = 14 RESP items.
    // Each item is a RESP line (simple/integer) or bulk string (header + payload).
    let item_count = 7 * 2; // 7 key-value pairs, both map and array have 14 items
    for _ in 0..item_count {
        let line = read_resp_line_with_timeout(client, Duration::from_secs(1)).await;
        if line.starts_with(b"$") {
            // Bulk string: read payload + CRLF.
            let len = std::str::from_utf8(&line[1..])
                .unwrap()
                .parse::<usize>()
                .unwrap();
            let mut payload = vec![0u8; len + 2];
            tokio::time::timeout(Duration::from_secs(1), client.read_exact(&mut payload))
                .await
                .unwrap()
                .unwrap();
        } else if line.starts_with(b"*") {
            // Empty modules array (*0) — nothing more to read.
        }
        // Integer lines (:N) are already fully consumed by read_resp_line.
    }
}

async fn wait_for_blocked_clients(stream: &mut TcpStream, expected: u64, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let payload =
            send_and_read_bulk_payload(stream, b"*1\r\n$4\r\nINFO\r\n", Duration::from_secs(1))
                .await;
        if read_info_u64(&payload, "blocked_clients") == Some(expected) {
            return;
        }
        sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for blocked_clients={expected}");
}

fn read_info_u64(payload: &[u8], field: &str) -> Option<u64> {
    let text = std::str::from_utf8(payload).ok()?;
    let prefix = format!("{field}:");
    text.split("\r\n").find_map(|line| {
        line.strip_prefix(&prefix)
            .and_then(|value| value.parse::<u64>().ok())
    })
}

const TEST_OIDC_RSA_PRIVATE_KEY_PEM: &str = concat!(
    "-----BEGIN PRIVATE KEY-----\n",
    "MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDJETqse41HRBsc\n",
    "7cfcq3ak4oZWFCoZlcic525A3FfO4qW9BMtRO/iXiyCCHn8JhiL9y8j5JdVP2Q9Z\n",
    "IpfElcFd3/guS9w+5RqQGgCR+H56IVUyHZWtTJbKPcwWXQdNUX0rBFcsBzCRESJL\n",
    "eelOEdHIjG7LRkx5l/FUvlqsyHDVJEQsHwegZ8b8C0fz0EgT2MMEdn10t6Ur1rXz\n",
    "jMB/wvCg8vG8lvciXmedyo9xJ8oMOh0wUEgxziVDMMovmC+aJctcHUAYubwoGN8T\n",
    "yzcvnGqL7JSh36Pwy28iPzXZ2RLhAyJFU39vLaHdljwthUaupldlNyCfa6Ofy4qN\n",
    "ctlUPlN1AgMBAAECggEAdESTQjQ70O8QIp1ZSkCYXeZjuhj081CK7jhhp/4ChK7J\n",
    "GlFQZMwiBze7d6K84TwAtfQGZhQ7km25E1kOm+3hIDCoKdVSKch/oL54f/BK6sKl\n",
    "qlIzQEAenho4DuKCm3I4yAw9gEc0DV70DuMTR0LEpYyXcNJY3KNBOTjN5EYQAR9s\n",
    "2MeurpgK2MdJlIuZaIbzSGd+diiz2E6vkmcufJLtmYUT/k/ddWvEtz+1DnO6bRHh\n",
    "xuuDMeJA/lGB/EYloSLtdyCF6sII6C6slJJtgfb0bPy7l8VtL5iDyz46IKyzdyzW\n",
    "tKAn394dm7MYR1RlUBEfqFUyNK7C+pVMVoTwCC2V4QKBgQD64syfiQ2oeUlLYDm4\n",
    "CcKSP3RnES02bcTyEDFSuGyyS1jldI4A8GXHJ/lG5EYgiYa1RUivge4lJrlNfjyf\n",
    "dV230xgKms7+JiXqag1FI+3mqjAgg4mYiNjaao8N8O3/PD59wMPeWYImsWXNyeHS\n",
    "55rUKiHERtCcvdzKl4u35ZtTqQKBgQDNKnX2bVqOJ4WSqCgHRhOm386ugPHfy+8j\n",
    "m6cicmUR46ND6ggBB03bCnEG9OtGisxTo/TuYVRu3WP4KjoJs2LD5fwdwJqpgtHl\n",
    "yVsk45Y1Hfo+7M6lAuR8rzCi6kHHNb0HyBmZjysHWZsn79ZM+sQnLpgaYgQGRbKV\n",
    "DZWlbw7g7QKBgQCl1u+98UGXAP1jFutwbPsx40IVszP4y5ypCe0gqgon3UiY/G+1\n",
    "zTLp79GGe/SjI2VpQ7AlW7TI2A0bXXvDSDi3/5Dfya9ULnFXv9yfvH1QwWToySpW\n",
    "Kvd1gYSoiX84/WCtjZOr0e0HmLIb0vw0hqZA4szJSqoxQgvF22EfIWaIaQKBgQCf\n",
    "34+OmMYw8fEvSCPxDxVvOwW2i7pvV14hFEDYIeZKW2W1HWBhVMzBfFB5SE8yaCQy\n",
    "pRfOzj9aKOCm2FjjiErVNpkQoi6jGtLvScnhZAt/lr2TXTrl8OwVkPrIaN0bG/AS\n",
    "aUYxmBPCpXu3UjhfQiWqFq/mFyzlqlgvuCc9g95HPQKBgAscKP8mLxdKwOgX8yFW\n",
    "GcZ0izY/30012ajdHY+/QK5lsMoxTnn0skdS+spLxaS5ZEO4qvPVb8RAoCkWMMal\n",
    "2pOhmquJQVDPDLuZHdrIiKiDM20dy9sMfHygWcZjQ4WSxf/J7T9canLZIXFhHAZT\n",
    "3wc9h4G8BBCtWN2TN/LsGZdB\n",
    "-----END PRIVATE KEY-----\n",
);

const TEST_OIDC_RSA_JWKS: &str = concat!(
    "{\"keys\":[{",
    "\"kty\":\"RSA\",",
    "\"n\":\"yRE6rHuNR0QbHO3H3Kt2pOKGVhQqGZXInOduQNxXzuKlvQTLUTv4l4sggh5_CYYi_cvI-SXVT9kPWSKXxJXBXd_4LkvcPuUakBoAkfh-eiFVMh2VrUyWyj3MFl0HTVF9KwRXLAcwkREiS3npThHRyIxuy0ZMeZfxVL5arMhw1SRELB8HoGfG_AtH89BIE9jDBHZ9dLelK9a184zAf8LwoPLxvJb3Il5nncqPcSfKDDodMFBIMc4lQzDKL5gvmiXLXB1AGLm8KBjfE8s3L5xqi-yUod-j8MtvIj812dkS4QMiRVN_by2h3ZY8LYVGrqZXZTcgn2ujn8uKjXLZVD5TdQ\",",
    "\"e\":\"AQAB\",",
    "\"kid\":\"rsa01\",",
    "\"alg\":\"RS256\",",
    "\"use\":\"sig\"",
    "}]}",
);

#[derive(serde::Serialize)]
struct TestOidcClaims<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    sub: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    oid: Option<&'a str>,
    iss: &'a str,
    aud: &'a str,
    exp: usize,
}

fn issue_test_oidc_token(
    subject: Option<&str>,
    object_id: Option<&str>,
    issuer: &str,
    audience: &str,
) -> String {
    let exp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as usize
        + 3600;
    let claims = TestOidcClaims {
        sub: subject,
        oid: object_id,
        iss: issuer,
        aud: audience,
        exp,
    };
    let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256);
    header.kid = Some("rsa01".to_string());
    jsonwebtoken::encode(
        &header,
        &claims,
        &jsonwebtoken::EncodingKey::from_rsa_pem(TEST_OIDC_RSA_PRIVATE_KEY_PEM.as_bytes()).unwrap(),
    )
    .unwrap()
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

fn execute_processor_frame(processor: &RequestProcessor, frame: &[u8]) -> Vec<u8> {
    let mut args = [ArgSlice::EMPTY; 64];
    let meta = parse_resp_command_arg_slices(frame, &mut args).unwrap();
    let mut response = Vec::new();
    processor
        .execute_in_db(
            &args[..meta.argument_count],
            &mut response,
            DbName::fixture(),
        )
        .unwrap();
    response
}

async fn start_test_server() -> (
    std::net::SocketAddr,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    let processor = Arc::new(RequestProcessor::new().unwrap());
    start_test_server_with_processor(processor).await
}

fn start_test_server_on_dedicated_thread() -> (
    std::net::SocketAddr,
    oneshot::Sender<()>,
    std::thread::JoinHandle<()>,
) {
    let std_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    std_listener.set_nonblocking(true).unwrap();
    let addr = std_listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let processor = Arc::new(RequestProcessor::new().unwrap());
    let isolated_dir = isolate_default_persistence_dir_for_tests(&processor);
    let server_processor = Arc::clone(&processor);

    let server = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async move {
            let listener = TcpListener::from_std(std_listener).unwrap();
            let metrics = Arc::new(ServerMetrics::default());
            run_listener_with_shutdown_and_cluster_with_processor(
                listener,
                1024,
                metrics,
                async move {
                    let _ = shutdown_rx.await;
                },
                None,
                server_processor,
            )
            .await
            .unwrap();
            if let Some(dir) = isolated_dir {
                let _ = std::fs::remove_dir_all(dir);
            }
        });
    });

    (addr, shutdown_tx, server)
}

async fn start_test_server_with_scripting_enabled() -> (
    std::net::SocketAddr,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    let (addr, shutdown_tx, server, _) =
        start_test_server_with_scripting_enabled_and_processor().await;
    (addr, shutdown_tx, server)
}

async fn start_test_server_with_processor(
    processor: Arc<RequestProcessor>,
) -> (
    std::net::SocketAddr,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    TestServerHandle::start_with_processor(processor)
        .await
        .into_parts()
}

async fn start_test_server_with_scripting_enabled_and_processor() -> (
    std::net::SocketAddr,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
    Arc<RequestProcessor>,
) {
    let processor =
        Arc::new(RequestProcessor::new_with_string_store_shards_and_scripting(1, true).unwrap());
    let (addr, shutdown_tx, server) =
        start_test_server_with_processor(Arc::clone(&processor)).await;

    (addr, shutdown_tx, server, processor)
}

fn unique_test_temp_dir(prefix: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("garnet-{prefix}-{nanos}"));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn apply_test_startup_config_overrides(
    processor: &RequestProcessor,
    overrides: StartupConfigOverrides,
) {
    apply_startup_config_overrides_to_processor(processor, &overrides).unwrap();
}

fn aof_test_startup_overrides(
    dir: &std::path::Path,
    appendfilename: &str,
    appendfsync: &str,
) -> StartupConfigOverrides {
    StartupConfigOverrides {
        dir: Some(dir.to_path_buf()),
        appendonly: Some(true),
        appendfsync: Some(appendfsync.to_string()),
        appendfilename: Some(appendfilename.to_string()),
        ..Default::default()
    }
}

fn dump_snapshot_test_startup_overrides(
    dir: &std::path::Path,
    dbfilename: &str,
) -> StartupConfigOverrides {
    StartupConfigOverrides {
        dir: Some(dir.to_path_buf()),
        dbfilename: Some(dbfilename.to_string()),
        ..Default::default()
    }
}

fn configure_processor_aof_for_test(processor: &RequestProcessor, prefix: &str) -> PathBuf {
    let dir = unique_test_temp_dir(prefix);
    apply_test_startup_config_overrides(
        processor,
        StartupConfigOverrides {
            dir: Some(dir.clone()),
            appendfilename: Some(format!("{prefix}.aof")),
            ..Default::default()
        },
    );
    dir
}

async fn wait_for_server_ping(addr: std::net::SocketAddr) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(mut probe) = TcpStream::connect(addr).await {
            probe
                .write_all(&encode_resp_command(&[b"PING"]))
                .await
                .unwrap();
            let mut response = [0u8; 7];
            if let Ok(Ok(_)) =
                tokio::time::timeout(Duration::from_millis(200), probe.read_exact(&mut response))
                    .await
            {
                if response == *b"+PONG\r\n" {
                    return;
                }
            }
        }
        assert!(Instant::now() < deadline, "server did not become ready");
        sleep(Duration::from_millis(10)).await;
    }
}

fn owned_args_from_frame(frame: &[u8]) -> Vec<Vec<u8>> {
    let mut args = [ArgSlice::EMPTY; 64];
    let meta = parse_resp_command_arg_slices(frame, &mut args).unwrap();
    let mut owned = Vec::with_capacity(meta.argument_count);
    for arg in &args[..meta.argument_count] {
        owned.push(arg_slice_bytes(arg).to_vec());
    }
    owned
}

fn owned_frame_args_from_frame(frame: &[u8]) -> crate::connection_owner_routing::OwnedFrameArgs {
    let mut args = [ArgSlice::EMPTY; 64];
    let meta = parse_resp_command_arg_slices(frame, &mut args).unwrap();
    capture_owned_frame_args(frame, &args[..meta.argument_count]).unwrap()
}

#[inline]
fn arg_slice_bytes(arg: &ArgSlice) -> &[u8] {
    // SAFETY: test helpers use ArgSlice values derived from `frame` slices
    // that remain alive for the conversion scope.
    unsafe { arg.as_slice() }
}
