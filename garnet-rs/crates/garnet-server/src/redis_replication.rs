//! Minimal Redis replication compatibility for REPLICAOF/PSYNC/REPLCONF flows.

use crate::{dispatch_from_arg_slices, CommandId, RequestProcessor};
use garnet_common::{parse_resp_command_arg_slices, ArgSlice, RespParseError};
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{broadcast, Mutex, RwLock};
use tokio::task::JoinHandle;

// Empty Redis 7.x RDB payload (binary-safe) for FULLRESYNC responses.
const EMPTY_RDB_HEX: &str =
    "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
const DOWNSTREAM_BROADCAST_CAPACITY: usize = 4096;

#[derive(Clone, Debug, PartialEq, Eq)]
struct MasterEndpoint {
    host: String,
    port: u16,
}

struct ReplicationInner {
    processor: Arc<RequestProcessor>,
    downstream_tx: broadcast::Sender<Vec<u8>>,
    upstream_task: Mutex<Option<JoinHandle<()>>>,
    upstream_endpoint: RwLock<Option<MasterEndpoint>>,
    is_replica_mode: AtomicBool,
    upstream_link_up: AtomicBool,
    master_repl_offset: AtomicU64,
    repl_id: String,
    empty_rdb_payload: Vec<u8>,
}

#[derive(Clone)]
pub(crate) struct RedisReplicationCoordinator {
    inner: Arc<ReplicationInner>,
}

impl RedisReplicationCoordinator {
    pub(crate) fn new(processor: Arc<RequestProcessor>) -> Self {
        let (downstream_tx, _) = broadcast::channel(DOWNSTREAM_BROADCAST_CAPACITY);
        let inner = ReplicationInner {
            processor,
            downstream_tx,
            upstream_task: Mutex::new(None),
            upstream_endpoint: RwLock::new(None),
            is_replica_mode: AtomicBool::new(false),
            upstream_link_up: AtomicBool::new(false),
            master_repl_offset: AtomicU64::new(0),
            repl_id: generate_repl_id(),
            empty_rdb_payload: decode_hex_bytes(EMPTY_RDB_HEX),
        };
        Self {
            inner: Arc::new(inner),
        }
    }

    pub(crate) fn is_replica_mode(&self) -> bool {
        self.inner.is_replica_mode.load(Ordering::Relaxed)
    }

    pub(crate) async fn become_master(&self) {
        self.inner.is_replica_mode.store(false, Ordering::Relaxed);
        self.inner.upstream_link_up.store(false, Ordering::Relaxed);
        {
            let mut endpoint_guard = self.inner.upstream_endpoint.write().await;
            *endpoint_guard = None;
        }
        let mut task_guard = self.inner.upstream_task.lock().await;
        if let Some(task) = task_guard.take() {
            task.abort();
        }
    }

    pub(crate) async fn become_replica(&self, host: String, port: u16) {
        self.inner.is_replica_mode.store(true, Ordering::Relaxed);
        self.inner.upstream_link_up.store(false, Ordering::Relaxed);
        {
            let mut endpoint_guard = self.inner.upstream_endpoint.write().await;
            *endpoint_guard = Some(MasterEndpoint {
                host: host.clone(),
                port,
            });
        }

        let mut task_guard = self.inner.upstream_task.lock().await;
        if let Some(task) = task_guard.take() {
            task.abort();
        }

        let endpoint = MasterEndpoint { host, port };
        let inner = Arc::clone(&self.inner);
        *task_guard = Some(tokio::spawn(async move {
            run_upstream_replica_loop(inner, endpoint).await;
        }));
    }

    pub(crate) fn publish_write_frame(&self, frame: &[u8]) {
        self.inner
            .master_repl_offset
            .fetch_add(frame.len() as u64, Ordering::Relaxed);
        let _ = self.inner.downstream_tx.send(frame.to_vec());
    }

    pub(crate) fn build_fullresync_payload(&self) -> Vec<u8> {
        let repl_offset = self.inner.master_repl_offset.load(Ordering::Relaxed);
        let mut response = Vec::with_capacity(256 + self.inner.empty_rdb_payload.len());
        response.extend_from_slice(
            format!("+FULLRESYNC {} {}\r\n", self.inner.repl_id, repl_offset).as_bytes(),
        );
        response.extend_from_slice(
            format!("${}\r\n", self.inner.empty_rdb_payload.len()).as_bytes(),
        );
        response.extend_from_slice(&self.inner.empty_rdb_payload);
        response.extend_from_slice(b"\r\n");
        response
    }

    pub(crate) async fn serve_downstream_replica(&self, mut stream: TcpStream) -> io::Result<()> {
        let mut subscriber = self.inner.downstream_tx.subscribe();
        let mut inbound_buf = [0u8; 1024];

        loop {
            tokio::select! {
                result = subscriber.recv() => {
                    match result {
                        Ok(frame) => {
                            stream.write_all(&frame).await?;
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            continue;
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            return Ok(());
                        }
                    }
                }
                read_result = stream.read(&mut inbound_buf) => {
                    match read_result {
                        Ok(0) => return Ok(()),
                        Ok(_) => {
                            // Ignore REPLCONF ACK chatter from downstream replicas.
                        }
                        Err(error) => return Err(error),
                    }
                }
            }
        }
    }
}

async fn run_upstream_replica_loop(inner: Arc<ReplicationInner>, endpoint: MasterEndpoint) {
    loop {
        if !inner.is_replica_mode.load(Ordering::Relaxed) {
            return;
        }

        if !upstream_endpoint_matches(&inner, &endpoint).await {
            return;
        }

        let result = sync_once_from_upstream(&inner, &endpoint).await;
        if let Err(error) = result {
            eprintln!(
                "replication upstream sync failed ({}:{}): {}",
                endpoint.host, endpoint.port, error
            );
        }
        inner.upstream_link_up.store(false, Ordering::Relaxed);

        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn upstream_endpoint_matches(inner: &ReplicationInner, endpoint: &MasterEndpoint) -> bool {
    let current = inner.upstream_endpoint.read().await;
    matches!(current.as_ref(), Some(value) if value == endpoint)
}

async fn sync_once_from_upstream(
    inner: &ReplicationInner,
    endpoint: &MasterEndpoint,
) -> io::Result<()> {
    let mut stream = TcpStream::connect((endpoint.host.as_str(), endpoint.port)).await?;
    let _ = stream.set_nodelay(true);

    let mut receive_buffer = Vec::with_capacity(8 * 1024);
    let mut read_scratch = [0u8; 8 * 1024];

    write_resp_command(&mut stream, &[b"PING"]).await?;
    let ping_reply = read_line(&mut stream, &mut receive_buffer, &mut read_scratch).await?;
    if !starts_with_ascii_no_case(&ping_reply, b"+PONG") {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!(
                "upstream did not reply to PING with +PONG (reply={})",
                String::from_utf8_lossy(&ping_reply)
            ),
        ));
    }

    write_resp_command(&mut stream, &[b"REPLCONF", b"listening-port", b"0"]).await?;
    let replconf_reply = read_line(&mut stream, &mut receive_buffer, &mut read_scratch).await?;
    if !starts_with_ascii_no_case(&replconf_reply, b"+OK") {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!(
                "upstream did not accept REPLCONF listening-port (reply={})",
                String::from_utf8_lossy(&replconf_reply)
            ),
        ));
    }

    write_resp_command(&mut stream, &[b"REPLCONF", b"capa", b"psync2"]).await?;
    let replconf_reply = read_line(&mut stream, &mut receive_buffer, &mut read_scratch).await?;
    if !starts_with_ascii_no_case(&replconf_reply, b"+OK") {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!(
                "upstream did not accept REPLCONF capa psync2 (reply={})",
                String::from_utf8_lossy(&replconf_reply)
            ),
        ));
    }

    write_resp_command(&mut stream, &[b"PSYNC", b"?", b"-1"]).await?;
    let psync_reply = read_line(&mut stream, &mut receive_buffer, &mut read_scratch).await?;
    if starts_with_ascii_no_case(&psync_reply, b"+FULLRESYNC") {
        let rdb_header = read_line(&mut stream, &mut receive_buffer, &mut read_scratch).await?;
        if !rdb_header.starts_with(b"$") {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "invalid FULLRESYNC payload header: {}",
                    String::from_utf8_lossy(&rdb_header)
                ),
            ));
        }

        let rdb_len = parse_bulk_length(&rdb_header[1..])?;
        discard_bulk_payload(&mut stream, &mut receive_buffer, &mut read_scratch, rdb_len).await?;
    } else if !starts_with_ascii_no_case(&psync_reply, b"+CONTINUE") {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!(
                "upstream PSYNC reply is not FULLRESYNC/CONTINUE (reply={})",
                String::from_utf8_lossy(&psync_reply)
            ),
        ));
    }

    inner.upstream_link_up.store(true, Ordering::Relaxed);

    let mut args = [ArgSlice::EMPTY; 64];
    let mut applied_offset = 0u64;

    loop {
        let mut consumed = 0usize;

        loop {
            match parse_resp_command_arg_slices(&receive_buffer[consumed..], &mut args) {
                Ok(meta) => {
                    process_upstream_frame(
                        &mut stream,
                        inner,
                        &args[..meta.argument_count],
                        meta.bytes_consumed,
                        &mut applied_offset,
                    )
                    .await?;
                    consumed += meta.bytes_consumed;
                }
                Err(RespParseError::Incomplete) => break,
                Err(_) => {
                    let preview_len = receive_buffer.len().saturating_sub(consumed).min(96);
                    let preview = &receive_buffer[consumed..consumed + preview_len];
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!(
                            "invalid RESP frame in upstream replication stream (preview={:?})",
                            String::from_utf8_lossy(preview)
                        ),
                    ));
                }
            }
        }

        if consumed > 0 {
            receive_buffer.drain(..consumed);
        }

        let bytes_read = stream.read(&mut read_scratch).await?;
        if bytes_read == 0 {
            return Ok(());
        }
        receive_buffer.extend_from_slice(&read_scratch[..bytes_read]);
    }
}

async fn process_upstream_frame(
    stream: &mut TcpStream,
    inner: &ReplicationInner,
    args: &[ArgSlice],
    frame_len: usize,
    applied_offset: &mut u64,
) -> io::Result<()> {
    if args.is_empty() {
        return Ok(());
    }

    *applied_offset += frame_len as u64;

    // SAFETY: arg slices reference data owned by the live upstream receive buffer.
    let command_name = unsafe { args[0].as_slice() };

    if starts_with_ascii_no_case(command_name, b"REPLCONF") {
        if args.len() >= 2 {
            // SAFETY: same lifetime guarantee as above.
            let sub = unsafe { args[1].as_slice() };
            if starts_with_ascii_no_case(sub, b"GETACK") {
                write_resp_command(
                    stream,
                    &[
                        b"REPLCONF",
                        b"ACK",
                        applied_offset.to_string().as_bytes(),
                    ],
                )
                .await?;
            }
        }
        return Ok(());
    }

    if starts_with_ascii_no_case(command_name, b"PING")
        || starts_with_ascii_no_case(command_name, b"SELECT")
    {
        return Ok(());
    }

    // SAFETY: arg slices reference data owned by the live upstream receive buffer.
    let command_id = unsafe { dispatch_from_arg_slices(args) };
    if !is_replicated_mutating_command(command_id) {
        return Ok(());
    }

    let mut ignored_response = Vec::new();
    let _ = inner.processor.execute(args, &mut ignored_response);
    Ok(())
}

fn is_replicated_mutating_command(command: CommandId) -> bool {
    matches!(
        command,
        CommandId::Set
            | CommandId::Del
            | CommandId::Incr
            | CommandId::Decr
            | CommandId::Expire
            | CommandId::Pexpire
            | CommandId::Persist
            | CommandId::Hset
            | CommandId::Hdel
            | CommandId::Lpush
            | CommandId::Rpush
            | CommandId::Lpop
            | CommandId::Rpop
            | CommandId::Sadd
            | CommandId::Srem
            | CommandId::Zadd
            | CommandId::Zrem
    )
}

async fn write_resp_command(stream: &mut TcpStream, args: &[&[u8]]) -> io::Result<()> {
    let mut frame = Vec::with_capacity(256);
    frame.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
    for arg in args {
        frame.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
        frame.extend_from_slice(arg);
        frame.extend_from_slice(b"\r\n");
    }
    stream.write_all(&frame).await
}

async fn read_line(
    stream: &mut TcpStream,
    receive_buffer: &mut Vec<u8>,
    scratch: &mut [u8],
) -> io::Result<Vec<u8>> {
    loop {
        if let Some(position) = find_crlf(receive_buffer) {
            let mut line = receive_buffer.drain(..position + 2).collect::<Vec<u8>>();
            line.truncate(line.len().saturating_sub(2));
            return Ok(line);
        }

        let bytes_read = stream.read(scratch).await?;
        if bytes_read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "upstream closed stream while reading line",
            ));
        }
        receive_buffer.extend_from_slice(&scratch[..bytes_read]);
    }
}

async fn discard_bulk_payload(
    stream: &mut TcpStream,
    receive_buffer: &mut Vec<u8>,
    scratch: &mut [u8],
    payload_len: usize,
) -> io::Result<()> {
    while receive_buffer.len() < payload_len {
        let bytes_read = stream.read(scratch).await?;
        if bytes_read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "upstream closed stream while sending RDB payload",
            ));
        }
        receive_buffer.extend_from_slice(&scratch[..bytes_read]);
    }
    receive_buffer.drain(..payload_len);

    // Redis FULLRESYNC bulk transfer framing is observed both with and without a
    // trailing CRLF before command streaming; tolerate either shape.
    if receive_buffer.starts_with(b"\r\n") {
        receive_buffer.drain(..2);
    }

    Ok(())
}

fn parse_bulk_length(raw: &[u8]) -> io::Result<usize> {
    let value = std::str::from_utf8(raw)
        .ok()
        .and_then(|text| text.parse::<i64>().ok())
        .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "invalid bulk length in replication"))?;
    if value < 0 {
        return Ok(0);
    }
    Ok(value as usize)
}

fn find_crlf(buffer: &[u8]) -> Option<usize> {
    buffer.windows(2).position(|window| window == b"\r\n")
}

fn decode_hex_bytes(value: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(value.len() / 2);
    let bytes = value.as_bytes();
    let mut index = 0usize;
    while index + 1 < bytes.len() {
        let hi = decode_hex_nibble(bytes[index]);
        let lo = decode_hex_nibble(bytes[index + 1]);
        out.push((hi << 4) | lo);
        index += 2;
    }
    out
}

fn decode_hex_nibble(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        b'A'..=b'F' => byte - b'A' + 10,
        _ => 0,
    }
}

fn starts_with_ascii_no_case(input: &[u8], expected_upper_prefix: &[u8]) -> bool {
    if input.len() < expected_upper_prefix.len() {
        return false;
    }

    input
        .iter()
        .take(expected_upper_prefix.len())
        .zip(expected_upper_prefix.iter())
        .all(|(lhs, rhs)| ascii_upper(*lhs) == *rhs)
}

fn ascii_upper(byte: u8) -> u8 {
    if byte.is_ascii_lowercase() {
        byte - 32
    } else {
        byte
    }
}

fn generate_repl_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{:040x}", nanos)
}
