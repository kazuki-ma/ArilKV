//! Minimal Redis replication compatibility for REPLICAOF/PSYNC/REPLCONF flows.
// TLA+ model linkage:
// - formal/tla/specs/WaitAckProgress.tla

mod protocol;

use crate::RequestExecutionError;
use crate::RequestProcessor;
use crate::ServerMetrics;
use crate::ShardOwnerThreadPool;
use crate::command_spec::command_is_effectively_mutating;
use crate::connection_owner_routing::OwnerThreadExecutionError;
use crate::connection_owner_routing::execute_frame_on_owner_thread;
use crate::dispatch_from_arg_slices;
use crate::redis_replication::protocol::discard_bulk_payload;
use crate::redis_replication::protocol::generate_repl_id;
use crate::redis_replication::protocol::parse_bulk_length;
use crate::redis_replication::protocol::read_line;
use crate::redis_replication::protocol::starts_with_ascii_no_case;
use crate::redis_replication::protocol::write_resp_command;
use crate::request_lifecycle::DbName;
use garnet_common::ArgSlice;
use garnet_common::RespParseError;
use garnet_common::parse_resp_command_arg_slices_dynamic;
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::sync::Notify;
use tokio::sync::RwLock;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tsavorite::AofOffset;

const DOWNSTREAM_BROADCAST_CAPACITY: usize = 4096;
const DEFAULT_RESP_ARG_SCRATCH: usize = 64;
const DEFAULT_MAX_RESP_ARGUMENTS: usize = 1_048_576;
const GARNET_MAX_RESP_ARGUMENTS_ENV: &str = "GARNET_MAX_RESP_ARGUMENTS";
const REPLCONF_GETACK_FRAME: &[u8] = b"*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";

#[inline]
fn arg_slice_bytes(arg: &ArgSlice) -> &[u8] {
    // SAFETY: replication command processing inspects ArgSlice entries only
    // while their backing receive buffer is still alive.
    unsafe { arg.as_slice() }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct MasterEndpoint {
    host: String,
    port: u16,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct DownstreamReplicaEndpoint {
    host: String,
    port: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ManualFailoverRequest {
    pub(crate) target_replica_id: Option<u64>,
    pub(crate) timeout_millis: Option<u64>,
    pub(crate) force: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ManualFailoverPrepareError {
    AlreadyInProgress,
    IsReplica,
    RequiresConnectedReplicas,
    TargetNotReplica,
}

#[derive(Debug)]
enum ManualFailoverState {
    Idle,
    Prepared(ManualFailoverRequest),
    Running { abort_requested: Arc<AtomicBool> },
}

impl Default for ManualFailoverState {
    fn default() -> Self {
        Self::Idle
    }
}

struct ReplicationInner {
    processor: Arc<RequestProcessor>,
    owner_thread_pool: Arc<ShardOwnerThreadPool>,
    downstream_tx: broadcast::Sender<Arc<[u8]>>,
    upstream_task: Mutex<Option<JoinHandle<()>>>,
    upstream_endpoint: RwLock<Option<MasterEndpoint>>,
    is_replica_mode: AtomicBool,
    upstream_link_up: AtomicBool,
    downstream_replica_count: AtomicUsize,
    local_listen_port: AtomicUsize,
    replication_select_needed: AtomicBool,
    replication_selected_db: AtomicUsize,
    master_repl_offset: AtomicU64,
    next_downstream_replica_id: AtomicU64,
    downstream_ack_offsets: Mutex<HashMap<u64, u64>>,
    downstream_replica_endpoints: std::sync::Mutex<HashMap<u64, DownstreamReplicaEndpoint>>,
    downstream_ack_notify: Notify,
    manual_failover_state: std::sync::Mutex<ManualFailoverState>,
    repl_id: String,
}

#[derive(Clone)]
pub(crate) struct RedisReplicationCoordinator {
    inner: Arc<ReplicationInner>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PublishedWriteFrontiers {
    pub(crate) replication_offset: u64,
    pub(crate) local_aof_append_offset: Option<AofOffset>,
}

impl RedisReplicationCoordinator {
    pub(crate) fn new(
        processor: Arc<RequestProcessor>,
        owner_thread_pool: Arc<ShardOwnerThreadPool>,
    ) -> Self {
        let (downstream_tx, _) = broadcast::channel(DOWNSTREAM_BROADCAST_CAPACITY);
        let inner = ReplicationInner {
            processor,
            owner_thread_pool,
            downstream_tx,
            upstream_task: Mutex::new(None),
            upstream_endpoint: RwLock::new(None),
            is_replica_mode: AtomicBool::new(false),
            upstream_link_up: AtomicBool::new(false),
            downstream_replica_count: AtomicUsize::new(0),
            local_listen_port: AtomicUsize::new(0),
            replication_select_needed: AtomicBool::new(true),
            replication_selected_db: AtomicUsize::new(0),
            master_repl_offset: AtomicU64::new(0),
            next_downstream_replica_id: AtomicU64::new(1),
            downstream_ack_offsets: Mutex::new(HashMap::new()),
            downstream_replica_endpoints: std::sync::Mutex::new(HashMap::new()),
            downstream_ack_notify: Notify::new(),
            manual_failover_state: std::sync::Mutex::new(ManualFailoverState::Idle),
            repl_id: generate_repl_id(),
        };
        Self {
            inner: Arc::new(inner),
        }
    }

    pub(crate) fn is_replica_mode(&self) -> bool {
        self.inner.is_replica_mode.load(Ordering::Relaxed)
    }

    pub(crate) fn is_upstream_link_up(&self) -> bool {
        self.inner.upstream_link_up.load(Ordering::Relaxed)
    }

    pub(crate) fn downstream_replica_count(&self) -> u64 {
        self.inner.downstream_replica_count.load(Ordering::Relaxed) as u64
    }

    pub(crate) fn set_listen_port(&self, port: u16) {
        self.inner
            .local_listen_port
            .store(usize::from(port), Ordering::Release);
    }

    pub(crate) fn consume_replication_select_needed_for_db(&self, selected_db: DbName) -> bool {
        let selected_db = usize::from(selected_db);
        let select_needed = self
            .inner
            .replication_select_needed
            .swap(false, Ordering::AcqRel);
        let current_selected_db = self.inner.replication_selected_db.load(Ordering::Acquire);
        if !select_needed && current_selected_db == selected_db {
            return false;
        }
        self.inner
            .replication_selected_db
            .store(selected_db, Ordering::Release);
        true
    }

    pub(crate) async fn become_master(&self) {
        self.inner.is_replica_mode.store(false, Ordering::Relaxed);
        self.inner.upstream_link_up.store(false, Ordering::Relaxed);
        self.inner
            .replication_select_needed
            .store(true, Ordering::Release);
        self.inner
            .replication_selected_db
            .store(0, Ordering::Release);
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
        self.inner
            .replication_select_needed
            .store(true, Ordering::Release);
        self.inner
            .replication_selected_db
            .store(0, Ordering::Release);
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

    pub(crate) fn publish_write_frame(&self, frame: &[u8]) -> PublishedWriteFrontiers {
        let replication_offset = self
            .inner
            .master_repl_offset
            .fetch_add(frame.len() as u64, Ordering::Relaxed)
            .saturating_add(frame.len() as u64);
        let local_aof_append_offset = self.inner.processor.publish_local_aof_frame(frame);
        let _ = self.inner.downstream_tx.send(Arc::from(frame.to_vec()));
        PublishedWriteFrontiers {
            replication_offset,
            local_aof_append_offset,
        }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn current_master_repl_offset(&self) -> u64 {
        self.inner.master_repl_offset.load(Ordering::Relaxed)
    }

    pub(crate) fn prepare_manual_failover(
        &self,
        request: ManualFailoverRequest,
    ) -> Result<(), ManualFailoverPrepareError> {
        if self.is_replica_mode() {
            return Err(ManualFailoverPrepareError::IsReplica);
        }
        if self.downstream_replica_count() == 0 {
            return Err(ManualFailoverPrepareError::RequiresConnectedReplicas);
        }
        if let Some(target_replica_id) = request.target_replica_id
            && !self.downstream_replica_exists(target_replica_id)
        {
            return Err(ManualFailoverPrepareError::TargetNotReplica);
        }

        let mut state = self.inner.manual_failover_state.lock().unwrap();
        if !matches!(*state, ManualFailoverState::Idle) {
            return Err(ManualFailoverPrepareError::AlreadyInProgress);
        }
        *state = ManualFailoverState::Prepared(request);
        Ok(())
    }

    pub(crate) fn abort_manual_failover(&self) -> bool {
        let mut state = self.inner.manual_failover_state.lock().unwrap();
        match &*state {
            ManualFailoverState::Idle => false,
            ManualFailoverState::Prepared(_) => {
                *state = ManualFailoverState::Idle;
                true
            }
            ManualFailoverState::Running { abort_requested } => {
                abort_requested.store(true, Ordering::Release);
                true
            }
        }
    }

    pub(crate) fn launch_prepared_manual_failover(&self, request: ManualFailoverRequest) -> bool {
        let abort_requested = {
            let mut state = self.inner.manual_failover_state.lock().unwrap();
            match *state {
                ManualFailoverState::Prepared(prepared_request) if prepared_request == request => {
                    let abort_requested = Arc::new(AtomicBool::new(false));
                    *state = ManualFailoverState::Running {
                        abort_requested: Arc::clone(&abort_requested),
                    };
                    abort_requested
                }
                _ => return false,
            }
        };

        let inner = Arc::clone(&self.inner);
        tokio::spawn(async move {
            run_manual_failover(inner, request, abort_requested).await;
        });
        true
    }

    pub(crate) fn find_downstream_replica_id_by_host_port(
        &self,
        host: &[u8],
        port: u16,
    ) -> Option<u64> {
        let host_text = std::str::from_utf8(host).ok()?;
        let endpoints = self.inner.downstream_replica_endpoints.lock().unwrap();
        endpoints.iter().find_map(|(&replica_id, endpoint)| {
            if endpoint.port != port {
                return None;
            }
            if endpoint_matches_host(endpoint, host_text) {
                return Some(replica_id);
            }
            None
        })
    }

    pub(crate) async fn wait_for_replicas(
        &self,
        requested_replicas: u64,
        timeout_millis: u64,
        target_offset: u64,
    ) -> u64 {
        // TLA+ : WaitRequestAckRefresh
        self.request_downstream_ack_refresh();

        if timeout_millis == 0 {
            loop {
                let notified = self.inner.downstream_ack_notify.notified();
                let acknowledged = self
                    .acknowledged_downstream_replica_count(target_offset)
                    .await;
                if acknowledged >= requested_replicas {
                    // TLA+ : WaitObserveAckQuorum
                    return acknowledged;
                }
                notified.await;
            }
        }

        let deadline = Instant::now() + Duration::from_millis(timeout_millis);
        loop {
            let notified = self.inner.downstream_ack_notify.notified();
            let acknowledged = self
                .acknowledged_downstream_replica_count(target_offset)
                .await;
            if acknowledged >= requested_replicas {
                // TLA+ : WaitObserveAckQuorum
                return acknowledged;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                // TLA+ : WaitObserveTimeout
                return acknowledged;
            }
            if tokio::time::timeout(remaining, notified).await.is_err() {
                // TLA+ : WaitObserveTimeout
                return self
                    .acknowledged_downstream_replica_count(target_offset)
                    .await;
            }
        }
    }

    pub(crate) fn try_acknowledged_downstream_replica_count(
        &self,
        target_offset: u64,
    ) -> Option<u64> {
        let ack_offsets = self.inner.downstream_ack_offsets.try_lock().ok()?;
        Some(
            ack_offsets
                .values()
                .filter(|&&value| value >= target_offset)
                .count() as u64,
        )
    }

    async fn acknowledged_downstream_replica_count(&self, target_offset: u64) -> u64 {
        let ack_offsets = self.inner.downstream_ack_offsets.lock().await;
        ack_offsets
            .values()
            .filter(|&&value| value >= target_offset)
            .count() as u64
    }

    pub(crate) fn request_downstream_ack_refresh(&self) {
        if self.inner.downstream_replica_count.load(Ordering::Relaxed) == 0 {
            return;
        }
        let _ = self
            .inner
            .downstream_tx
            .send(Arc::from(REPLCONF_GETACK_FRAME));
    }

    async fn register_downstream_replica(
        &self,
        peer_addr: Option<SocketAddr>,
        announced_listen_port: Option<u16>,
    ) -> u64 {
        self.inner
            .downstream_replica_count
            .fetch_add(1, Ordering::Relaxed);
        let replica_id = self
            .inner
            .next_downstream_replica_id
            .fetch_add(1, Ordering::Relaxed);
        let mut ack_offsets = self.inner.downstream_ack_offsets.lock().await;
        ack_offsets.insert(replica_id, 0);
        drop(ack_offsets);
        if let Some(endpoint) =
            downstream_replica_endpoint_from_addr(peer_addr, announced_listen_port)
        {
            let mut endpoints = self.inner.downstream_replica_endpoints.lock().unwrap();
            endpoints.insert(replica_id, endpoint);
        }
        self.inner.downstream_ack_notify.notify_waiters();
        replica_id
    }

    async fn unregister_downstream_replica(&self, replica_id: u64) {
        self.inner
            .downstream_replica_count
            .fetch_sub(1, Ordering::Relaxed);
        let mut ack_offsets = self.inner.downstream_ack_offsets.lock().await;
        ack_offsets.remove(&replica_id);
        drop(ack_offsets);
        let mut endpoints = self.inner.downstream_replica_endpoints.lock().unwrap();
        endpoints.remove(&replica_id);
        drop(endpoints);
        self.inner.downstream_ack_notify.notify_waiters();
    }

    pub(crate) fn subscribe_downstream(&self) -> broadcast::Receiver<Arc<[u8]>> {
        // Re-arm SELECT before subscribing so post-SYNC frames observed by this subscriber
        // include an initial SELECT even if writes arrive before stream handoff.
        self.inner
            .replication_select_needed
            .store(true, Ordering::Release);
        self.inner.downstream_tx.subscribe()
    }

    pub(crate) fn build_fullresync_payload(
        &self,
        functions_only: bool,
    ) -> Result<Vec<u8>, RequestExecutionError> {
        let repl_offset = self.inner.master_repl_offset.load(Ordering::Relaxed);
        let sync_payload = self.build_sync_payload(functions_only)?;
        let mut response = Vec::with_capacity(256 + sync_payload.len());
        response.extend_from_slice(
            format!("+FULLRESYNC {} {}\r\n", self.inner.repl_id, repl_offset).as_bytes(),
        );
        response.extend_from_slice(sync_payload.as_slice());
        Ok(response)
    }

    pub(crate) fn build_sync_payload(
        &self,
        functions_only: bool,
    ) -> Result<Vec<u8>, RequestExecutionError> {
        let snapshot = self
            .inner
            .processor
            .build_debug_reload_snapshot(DbName::db0(), functions_only)?;
        let mut response = Vec::with_capacity(64 + snapshot.len());
        response.extend_from_slice(format!("${}\r\n", snapshot.len()).as_bytes());
        response.extend_from_slice(&snapshot);
        Ok(response)
    }

    pub(crate) async fn serve_downstream_replica(&self, stream: TcpStream) -> io::Result<()> {
        let subscriber = self.subscribe_downstream();
        self.serve_downstream_replica_with_subscriber(stream, subscriber)
            .await
    }

    pub(crate) async fn serve_downstream_replica_with_metrics(
        &self,
        stream: TcpStream,
        subscriber: broadcast::Receiver<Arc<[u8]>>,
        metrics: Arc<ServerMetrics>,
        client_id: crate::ClientId,
    ) -> io::Result<()> {
        self.serve_downstream_replica_inner(stream, subscriber, Some((metrics, client_id)))
            .await
    }

    pub(crate) async fn serve_downstream_replica_with_subscriber(
        &self,
        stream: TcpStream,
        subscriber: broadcast::Receiver<Arc<[u8]>>,
    ) -> io::Result<()> {
        self.serve_downstream_replica_inner(stream, subscriber, None)
            .await
    }

    async fn serve_downstream_replica_inner(
        &self,
        mut stream: TcpStream,
        mut subscriber: broadcast::Receiver<Arc<[u8]>>,
        kill_watch: Option<(Arc<ServerMetrics>, crate::ClientId)>,
    ) -> io::Result<()> {
        let announced_listen_port = kill_watch
            .as_ref()
            .and_then(|(metrics, client_id)| metrics.client_replica_listen_port(*client_id));
        let replica_id = self
            .register_downstream_replica(stream.peer_addr().ok(), announced_listen_port)
            .await;
        let mut inbound_buf = [0u8; 1024];
        let max_resp_arguments = parse_positive_env_usize(GARNET_MAX_RESP_ARGUMENTS_ENV)
            .unwrap_or(DEFAULT_MAX_RESP_ARGUMENTS);
        let mut inbound_receive_buffer = Vec::with_capacity(1024);
        let mut inbound_args =
            vec![ArgSlice::EMPTY; DEFAULT_RESP_ARG_SCRATCH.min(max_resp_arguments)];

        loop {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(25)), if kill_watch.is_some() => {
                    let Some((metrics, client_id)) = kill_watch.as_ref() else {
                        continue;
                    };
                    if metrics.is_client_killed(*client_id) {
                        self.unregister_downstream_replica(replica_id).await;
                        return Ok(());
                    }
                }
                result = subscriber.recv() => {
                    match result {
                        Ok(frame) => {
                            if let Err(error) = stream.write_all(&frame).await {
                                self.unregister_downstream_replica(replica_id).await;
                                return Err(error);
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            continue;
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            self.unregister_downstream_replica(replica_id).await;
                            return Ok(());
                        }
                    }
                }
                read_result = stream.read(&mut inbound_buf) => {
                    match read_result {
                        Ok(0) => {
                            self.unregister_downstream_replica(replica_id).await;
                            return Ok(());
                        }
                        Ok(bytes_read) => {
                            inbound_receive_buffer.extend_from_slice(&inbound_buf[..bytes_read]);
                            process_downstream_control_frames(
                                &self.inner,
                                replica_id,
                                &mut inbound_receive_buffer,
                                &mut inbound_args,
                                max_resp_arguments,
                            )
                            .await;
                        }
                        Err(error) => {
                            self.unregister_downstream_replica(replica_id).await;
                            return Err(error);
                        }
                    }
                }
            }
        }
    }
}

impl RedisReplicationCoordinator {
    fn downstream_replica_exists(&self, replica_id: u64) -> bool {
        let endpoints = self.inner.downstream_replica_endpoints.lock().unwrap();
        endpoints.contains_key(&replica_id)
    }
}

fn downstream_replica_endpoint_from_addr(
    peer_addr: Option<SocketAddr>,
    announced_listen_port: Option<u16>,
) -> Option<DownstreamReplicaEndpoint> {
    let peer_addr = peer_addr?;
    Some(DownstreamReplicaEndpoint {
        host: peer_addr.ip().to_string(),
        port: announced_listen_port.unwrap_or(peer_addr.port()),
    })
}

fn endpoint_matches_host(endpoint: &DownstreamReplicaEndpoint, requested_host: &str) -> bool {
    if endpoint.host.eq_ignore_ascii_case(requested_host) {
        return true;
    }
    if requested_host.eq_ignore_ascii_case("localhost") {
        return endpoint.host == "127.0.0.1" || endpoint.host == "::1";
    }
    false
}

fn clear_manual_failover_state(inner: &ReplicationInner, abort_requested: &Arc<AtomicBool>) {
    let mut state = inner.manual_failover_state.lock().unwrap();
    let should_clear = matches!(
        &*state,
        ManualFailoverState::Running {
            abort_requested: current_abort_requested,
        } if Arc::ptr_eq(current_abort_requested, abort_requested)
    );
    if should_clear {
        *state = ManualFailoverState::Idle;
    }
}

async fn choose_manual_failover_target(
    inner: &ReplicationInner,
    request: ManualFailoverRequest,
) -> Option<(u64, DownstreamReplicaEndpoint)> {
    if let Some(target_replica_id) = request.target_replica_id {
        let endpoint = {
            let endpoints = inner.downstream_replica_endpoints.lock().unwrap();
            endpoints.get(&target_replica_id).cloned()
        }?;
        return Some((target_replica_id, endpoint));
    }

    let ack_offsets = inner.downstream_ack_offsets.lock().await;
    let endpoints = inner.downstream_replica_endpoints.lock().unwrap();
    ack_offsets
        .iter()
        .filter_map(|(&replica_id, &ack_offset)| {
            let endpoint = endpoints.get(&replica_id)?.clone();
            Some((replica_id, ack_offset, endpoint))
        })
        .max_by(|lhs, rhs| lhs.1.cmp(&rhs.1).then_with(|| rhs.0.cmp(&lhs.0)))
        .map(|(replica_id, _, endpoint)| (replica_id, endpoint))
}

async fn current_downstream_ack_offset(inner: &ReplicationInner, replica_id: u64) -> Option<u64> {
    let ack_offsets = inner.downstream_ack_offsets.lock().await;
    ack_offsets.get(&replica_id).copied()
}

async fn wait_for_manual_failover_target_sync(
    inner: &ReplicationInner,
    replica_id: u64,
    target_offset: u64,
    timeout_millis: Option<u64>,
    force: bool,
    abort_requested: &Arc<AtomicBool>,
) -> bool {
    let deadline = timeout_millis.map(|millis| Instant::now() + Duration::from_millis(millis));
    loop {
        if abort_requested.load(Ordering::Acquire) {
            return false;
        }

        if current_downstream_ack_offset(inner, replica_id)
            .await
            .is_some_and(|ack_offset| ack_offset >= target_offset)
        {
            return true;
        }

        let _ = inner.downstream_tx.send(Arc::from(REPLCONF_GETACK_FRAME));

        if let Some(deadline) = deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return force;
            }
            let wait_budget = remaining.min(Duration::from_millis(50));
            let notified = inner.downstream_ack_notify.notified();
            let _ = tokio::time::timeout(wait_budget, notified).await;
        } else {
            inner.downstream_ack_notify.notified().await;
        }
    }
}

async fn promote_manual_failover_target(endpoint: &DownstreamReplicaEndpoint) -> io::Result<()> {
    let mut stream = TcpStream::connect((endpoint.host.as_str(), endpoint.port)).await?;
    stream.set_nodelay(true)?;
    write_resp_command(&mut stream, &[b"REPLICAOF", b"NO", b"ONE"]).await?;
    let mut receive_buffer = Vec::new();
    let mut scratch = [0u8; 1024];
    let line = read_line(&mut stream, &mut receive_buffer, &mut scratch).await?;
    if !starts_with_ascii_no_case(&line, b"+OK") {
        let text = String::from_utf8_lossy(&line);
        return Err(io::Error::other(format!(
            "failover target promotion failed: {text}"
        )));
    }
    Ok(())
}

async fn run_manual_failover(
    inner: Arc<ReplicationInner>,
    request: ManualFailoverRequest,
    abort_requested: Arc<AtomicBool>,
) {
    let Some((replica_id, endpoint)) = choose_manual_failover_target(&inner, request).await else {
        clear_manual_failover_state(&inner, &abort_requested);
        return;
    };

    let target_offset = inner.master_repl_offset.load(Ordering::Acquire);
    let target_synced = wait_for_manual_failover_target_sync(
        &inner,
        replica_id,
        target_offset,
        request.timeout_millis,
        request.force,
        &abort_requested,
    )
    .await;
    if !target_synced || abort_requested.load(Ordering::Acquire) {
        clear_manual_failover_state(&inner, &abort_requested);
        return;
    }

    if promote_manual_failover_target(&endpoint).await.is_ok()
        && !abort_requested.load(Ordering::Acquire)
    {
        let coordinator = RedisReplicationCoordinator {
            inner: Arc::clone(&inner),
        };
        coordinator
            .become_replica(endpoint.host, endpoint.port)
            .await;
    }

    clear_manual_failover_state(&inner, &abort_requested);
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
        return Err(io::Error::other(format!(
            "upstream did not reply to PING with +PONG (reply={})",
            String::from_utf8_lossy(&ping_reply)
        )));
    }

    let listen_port = inner.local_listen_port.load(Ordering::Acquire);
    let listen_port_text = listen_port.to_string();
    write_resp_command(
        &mut stream,
        &[b"REPLCONF", b"listening-port", listen_port_text.as_bytes()],
    )
    .await?;
    let replconf_reply = read_line(&mut stream, &mut receive_buffer, &mut read_scratch).await?;
    if !starts_with_ascii_no_case(&replconf_reply, b"+OK") {
        return Err(io::Error::other(format!(
            "upstream did not accept REPLCONF listening-port (reply={})",
            String::from_utf8_lossy(&replconf_reply)
        )));
    }

    write_resp_command(&mut stream, &[b"REPLCONF", b"capa", b"psync2"]).await?;
    let replconf_reply = read_line(&mut stream, &mut receive_buffer, &mut read_scratch).await?;
    if !starts_with_ascii_no_case(&replconf_reply, b"+OK") {
        return Err(io::Error::other(format!(
            "upstream did not accept REPLCONF capa psync2 (reply={})",
            String::from_utf8_lossy(&replconf_reply)
        )));
    }

    write_resp_command(&mut stream, &[b"PSYNC", b"?", b"-1"]).await?;
    let psync_reply = read_line(&mut stream, &mut receive_buffer, &mut read_scratch).await?;
    let mut applied_offset_base = 0u64;
    if starts_with_ascii_no_case(&psync_reply, b"+FULLRESYNC") {
        applied_offset_base = parse_fullresync_offset(&psync_reply).ok_or_else(|| {
            io::Error::other(format!(
                "upstream FULLRESYNC reply does not include a valid offset (reply={})",
                String::from_utf8_lossy(&psync_reply)
            ))
        })?;
        let rdb_header = read_line(&mut stream, &mut receive_buffer, &mut read_scratch).await?;
        if !rdb_header.starts_with(b"$") {
            return Err(io::Error::other(format!(
                "invalid FULLRESYNC payload header: {}",
                String::from_utf8_lossy(&rdb_header)
            )));
        }

        let rdb_len = parse_bulk_length(&rdb_header[1..])?;
        discard_bulk_payload(&mut stream, &mut receive_buffer, &mut read_scratch, rdb_len).await?;
    } else if !starts_with_ascii_no_case(&psync_reply, b"+CONTINUE") {
        return Err(io::Error::other(format!(
            "upstream PSYNC reply is not FULLRESYNC/CONTINUE (reply={})",
            String::from_utf8_lossy(&psync_reply)
        )));
    }

    inner.upstream_link_up.store(true, Ordering::Relaxed);

    if listen_port > 0 {
        write_resp_command(
            &mut stream,
            &[b"REPLCONF", b"listening-port", listen_port_text.as_bytes()],
        )
        .await?;
    }

    let max_resp_arguments = parse_positive_env_usize(GARNET_MAX_RESP_ARGUMENTS_ENV)
        .unwrap_or(DEFAULT_MAX_RESP_ARGUMENTS);
    let mut args = vec![ArgSlice::EMPTY; DEFAULT_RESP_ARG_SCRATCH.min(max_resp_arguments)];
    let mut applied_offset = applied_offset_base;
    let mut applied_selected_db = DbName::db0();

    loop {
        let mut consumed = 0usize;

        loop {
            match parse_resp_command_arg_slices_dynamic(
                &receive_buffer[consumed..],
                &mut args,
                max_resp_arguments,
            ) {
                Ok(meta) => {
                    process_upstream_frame(
                        &mut stream,
                        inner,
                        &receive_buffer[consumed..consumed + meta.bytes_consumed],
                        &args[..meta.argument_count],
                        meta.bytes_consumed,
                        &mut applied_offset,
                        &mut applied_selected_db,
                    )
                    .await?;
                    consumed += meta.bytes_consumed;
                }
                Err(RespParseError::Incomplete) => break,
                Err(RespParseError::ArgumentCapacityExceeded { required, capacity }) => {
                    return Err(io::Error::other(format!(
                        "upstream frame has too many arguments (required={}, max={})",
                        required, capacity
                    )));
                }
                Err(_) => {
                    let preview_len = receive_buffer.len().saturating_sub(consumed).min(96);
                    let preview = &receive_buffer[consumed..consumed + preview_len];
                    return Err(io::Error::other(format!(
                        "invalid RESP frame in upstream replication stream (preview={:?})",
                        String::from_utf8_lossy(preview)
                    )));
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
    frame: &[u8],
    args: &[ArgSlice],
    frame_len: usize,
    applied_offset: &mut u64,
    applied_selected_db: &mut DbName,
) -> io::Result<()> {
    if args.is_empty() {
        return Ok(());
    }

    *applied_offset += frame_len as u64;

    let command_name = arg_slice_bytes(&args[0]);

    if starts_with_ascii_no_case(command_name, b"REPLCONF") {
        if args.len() >= 2 {
            let sub = arg_slice_bytes(&args[1]);
            if starts_with_ascii_no_case(sub, b"GETACK") {
                write_resp_command(
                    stream,
                    &[b"REPLCONF", b"ACK", applied_offset.to_string().as_bytes()],
                )
                .await?;
            }
        }
        return Ok(());
    }

    if starts_with_ascii_no_case(command_name, b"PING") {
        return Ok(());
    }

    if starts_with_ascii_no_case(command_name, b"SELECT") {
        if args.len() >= 2
            && let Some(parsed) = parse_u64_ascii_bytes(arg_slice_bytes(&args[1]))
            && let Ok(db_index) = usize::try_from(parsed)
        {
            *applied_selected_db = DbName::new(db_index);
        }
        return Ok(());
    }

    // SAFETY: arg slices reference data owned by the live upstream receive buffer.
    let command_id = unsafe { dispatch_from_arg_slices(args) };
    let command_mutating = command_is_effectively_mutating(
        command_id,
        if args.len() > 1 {
            Some(arg_slice_bytes(&args[1]))
        } else {
            None
        },
    );
    if !command_mutating {
        return Ok(());
    }
    match execute_frame_on_owner_thread(
        &inner.processor,
        &inner.owner_thread_pool,
        args,
        command_id,
        frame,
        false,
        None,
        *applied_selected_db,
    ) {
        Ok(_) => {}
        Err(OwnerThreadExecutionError::Request(error)) => {
            eprintln!("replication apply command failed: {error:?}");
        }
        Err(OwnerThreadExecutionError::Protocol) => {
            return Err(io::Error::other("replication apply failed: protocol error"));
        }
        Err(OwnerThreadExecutionError::OwnerThreadUnavailable) => {
            return Err(io::Error::other(
                "replication apply failed: owner routing execution failed",
            ));
        }
    }
    Ok(())
}

async fn process_downstream_control_frames(
    inner: &ReplicationInner,
    replica_id: u64,
    receive_buffer: &mut Vec<u8>,
    args: &mut Vec<ArgSlice>,
    max_resp_arguments: usize,
) {
    let mut consumed = 0usize;
    loop {
        match parse_resp_command_arg_slices_dynamic(
            &receive_buffer[consumed..],
            args,
            max_resp_arguments,
        ) {
            Ok(meta) => {
                maybe_record_downstream_ack(inner, replica_id, &args[..meta.argument_count]).await;
                consumed += meta.bytes_consumed;
            }
            Err(RespParseError::Incomplete) => break,
            Err(RespParseError::ArgumentCapacityExceeded { .. }) => {
                consumed = receive_buffer.len();
                break;
            }
            Err(_) => {
                consumed = receive_buffer.len();
                break;
            }
        }
    }
    if consumed > 0 {
        receive_buffer.drain(..consumed);
    }
}

async fn maybe_record_downstream_ack(inner: &ReplicationInner, replica_id: u64, args: &[ArgSlice]) {
    if args.len() < 3 {
        return;
    }
    let command = arg_slice_bytes(&args[0]);
    if !starts_with_ascii_no_case(command, b"REPLCONF") {
        return;
    }
    let subcommand = arg_slice_bytes(&args[1]);
    if starts_with_ascii_no_case(subcommand, b"LISTENING-PORT") {
        let Some(listen_port) = parse_u64_ascii_bytes(arg_slice_bytes(&args[2]))
            .and_then(|value| u16::try_from(value).ok())
        else {
            return;
        };
        let mut endpoints = inner.downstream_replica_endpoints.lock().unwrap();
        if let Some(endpoint) = endpoints.get_mut(&replica_id) {
            endpoint.port = listen_port;
        }
        return;
    }
    if !starts_with_ascii_no_case(subcommand, b"ACK") {
        return;
    }
    let Some(ack_offset) = parse_u64_ascii_bytes(arg_slice_bytes(&args[2])) else {
        return;
    };

    let mut ack_offsets = inner.downstream_ack_offsets.lock().await;
    let Some(current_offset) = ack_offsets.get_mut(&replica_id) else {
        return;
    };
    if ack_offset <= *current_offset {
        return;
    }
    // TLA+ : ReplicaAckAdvance
    *current_offset = ack_offset;
    drop(ack_offsets);
    inner.downstream_ack_notify.notify_waiters();
}

fn parse_u64_ascii_bytes(value: &[u8]) -> Option<u64> {
    let text = std::str::from_utf8(value).ok()?;
    text.parse::<u64>().ok()
}

fn parse_fullresync_offset(line: &[u8]) -> Option<u64> {
    let text = std::str::from_utf8(line).ok()?;
    let mut parts = text.split_ascii_whitespace();
    let prefix = parts.next()?;
    if !prefix.eq_ignore_ascii_case("+FULLRESYNC") {
        return None;
    }
    let _replid = parts.next()?;
    let offset = parts.next()?;
    offset.parse::<u64>().ok()
}

fn parse_positive_env_usize(key: &str) -> Option<usize> {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
}
