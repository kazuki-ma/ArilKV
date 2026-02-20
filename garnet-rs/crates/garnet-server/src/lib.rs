//! TCP server accept loop and connection handler primitives.

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL_ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

pub mod aof_replay;
pub mod command_dispatch;
pub mod command_spec;
mod connection_protocol;
mod connection_routing;
mod connection_transaction;
pub mod debug_concurrency;
pub mod limited_fixed_buffer_pool;
pub mod redis_replication;
pub mod request_lifecycle;
mod server_runtime;
pub mod shard_owner_threads;
#[cfg(test)]
pub(crate) mod testkit;

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
pub use server_runtime::{
    run, run_listener_with_shutdown, run_listener_with_shutdown_and_cluster,
    run_listener_with_shutdown_and_cluster_with_processor, run_with_cluster, run_with_shutdown,
    run_with_shutdown_and_cluster_config,
};
pub use shard_owner_threads::{ShardOwnerThreadPool, ShardOwnerThreadPoolError};

use garnet_cluster::{ClusterConfigError, ClusterConfigStore, SlotState, LOCAL_WORKER_ID};
use garnet_common::{parse_resp_command_arg_slices, ArgSlice, RespParseError};
use std::collections::{HashSet, VecDeque};
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;

use crate::command_spec::{
    command_has_valid_arity, command_is_mutating, command_transaction_control,
    TransactionControlCommand,
};
use crate::connection_protocol::{
    append_error_line, append_simple_string, append_wrong_arity_error_for_command,
    ascii_eq_ignore_case, parse_u16_ascii,
};
use crate::connection_routing::{
    cluster_error_for_command, command_hash_slot_for_transaction, owner_routed_shard_for_command,
};
use crate::connection_transaction::{execute_transaction_queue, ConnectionTransactionState};
use crate::redis_replication::RedisReplicationCoordinator;

const GARNET_STRING_OWNER_THREADS_ENV: &str = "GARNET_STRING_OWNER_THREADS";

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

#[derive(Debug)]
pub enum LiveSlotMigrationError {
    ClusterConfig(ClusterConfigError),
    MissingSourcePeerNode(String),
    MissingTargetPeerNode(String),
    DataPlane(RequestExecutionError),
}

impl core::fmt::Display for LiveSlotMigrationError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::ClusterConfig(error) => error.fmt(f),
            Self::MissingSourcePeerNode(node_id) => {
                write!(f, "source config missing peer node: {node_id}")
            }
            Self::MissingTargetPeerNode(node_id) => {
                write!(f, "target config missing peer node: {node_id}")
            }
            Self::DataPlane(error) => write!(f, "{error:?}"),
        }
    }
}

impl std::error::Error for LiveSlotMigrationError {}

impl From<ClusterConfigError> for LiveSlotMigrationError {
    fn from(value: ClusterConfigError) -> Self {
        Self::ClusterConfig(value)
    }
}

impl From<RequestExecutionError> for LiveSlotMigrationError {
    fn from(value: RequestExecutionError) -> Self {
        Self::DataPlane(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LiveSlotMigrationStepOutcome {
    pub moved_keys: usize,
    pub finalized: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LiveSlotMigrationRunReport {
    pub batches: usize,
    pub moved_keys: usize,
    pub finalized: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LiveSlotMigrationSlotReport {
    pub slot: u16,
    pub batches: usize,
    pub moved_keys: usize,
    pub finalized: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveSlotMigrationsRunReport {
    pub slots: Vec<LiveSlotMigrationSlotReport>,
    pub interrupted: bool,
}

#[derive(Debug, Clone)]
pub struct ClusterManagerFailoverMigrationRunReport {
    pub failover_report: garnet_cluster::ClusterManagerFailoverRunReport,
    pub migration_reports: Vec<LiveSlotMigrationsRunReport>,
}

#[derive(Debug)]
pub enum ClusterManagerFailoverMigrationError<E> {
    Failover(garnet_cluster::FailoverControllerError<E>),
    Migration(LiveSlotMigrationError),
}

impl<E: core::fmt::Display> core::fmt::Display for ClusterManagerFailoverMigrationError<E> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Failover(error) => write!(f, "{error}"),
            Self::Migration(error) => write!(f, "{error}"),
        }
    }
}

impl<E: core::fmt::Display + core::fmt::Debug> std::error::Error
    for ClusterManagerFailoverMigrationError<E>
{
}

impl<E> From<garnet_cluster::FailoverControllerError<E>>
    for ClusterManagerFailoverMigrationError<E>
{
    fn from(value: garnet_cluster::FailoverControllerError<E>) -> Self {
        Self::Failover(value)
    }
}

impl<E> From<LiveSlotMigrationError> for ClusterManagerFailoverMigrationError<E> {
    fn from(value: LiveSlotMigrationError) -> Self {
        Self::Migration(value)
    }
}

#[derive(Debug)]
pub enum ClusteredServerRunError<E> {
    Io(io::Error),
    ControlPlane(ClusterManagerFailoverMigrationError<E>),
}

impl<E: core::fmt::Display> core::fmt::Display for ClusteredServerRunError<E> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Io(error) => write!(f, "{error}"),
            Self::ControlPlane(error) => write!(f, "{error}"),
        }
    }
}

impl<E: core::fmt::Display + core::fmt::Debug> std::error::Error for ClusteredServerRunError<E> {}

impl<E> From<io::Error> for ClusteredServerRunError<E> {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl<E> From<ClusterManagerFailoverMigrationError<E>> for ClusteredServerRunError<E> {
    fn from(value: ClusterManagerFailoverMigrationError<E>) -> Self {
        Self::ControlPlane(value)
    }
}

fn resolve_migration_peer_ids(
    source_store: &ClusterConfigStore,
    target_store: &ClusterConfigStore,
) -> Result<(u16, u16), LiveSlotMigrationError> {
    let source_snapshot = source_store.load();
    let target_snapshot = target_store.load();
    let source_local_node_id = source_snapshot.local_worker()?.node_id.clone();
    let target_local_node_id = target_snapshot.local_worker()?.node_id.clone();

    let target_worker_id_in_source = source_snapshot
        .workers()
        .iter()
        .find(|worker| worker.node_id == target_local_node_id)
        .map(|worker| worker.id)
        .ok_or_else(|| LiveSlotMigrationError::MissingSourcePeerNode(target_local_node_id))?;
    let source_worker_id_in_target = target_snapshot
        .workers()
        .iter()
        .find(|worker| worker.node_id == source_local_node_id)
        .map(|worker| worker.id)
        .ok_or_else(|| LiveSlotMigrationError::MissingTargetPeerNode(source_local_node_id))?;
    Ok((target_worker_id_in_source, source_worker_id_in_target))
}

pub fn detect_live_slot_migration_slots(
    source_store: &ClusterConfigStore,
    target_store: &ClusterConfigStore,
) -> Result<Vec<u16>, LiveSlotMigrationError> {
    let (target_worker_id_in_source, source_worker_id_in_target) =
        resolve_migration_peer_ids(source_store, target_store)?;
    let source_snapshot = source_store.load();
    let target_snapshot = target_store.load();
    let source_migrating = source_snapshot
        .slots_assigned_to_worker_in_state(target_worker_id_in_source, SlotState::Migrating)?;
    let target_importing: HashSet<u16> = target_snapshot
        .slots_assigned_to_worker_in_state(source_worker_id_in_target, SlotState::Importing)?
        .into_iter()
        .collect();
    Ok(source_migrating
        .into_iter()
        .filter(|slot| target_importing.contains(slot))
        .collect())
}

pub fn execute_live_slot_migration_step(
    source_store: &ClusterConfigStore,
    target_store: &ClusterConfigStore,
    source_processor: &RequestProcessor,
    target_processor: &RequestProcessor,
    slot: u16,
    max_keys: usize,
) -> Result<LiveSlotMigrationStepOutcome, LiveSlotMigrationError> {
    if max_keys == 0 {
        return Ok(LiveSlotMigrationStepOutcome {
            moved_keys: 0,
            finalized: false,
        });
    }

    let (target_worker_id_in_source, source_worker_id_in_target) =
        resolve_migration_peer_ids(source_store, target_store)?;

    let source_migrating = source_store
        .load()
        .as_ref()
        .clone()
        .begin_slot_migration_to(slot, target_worker_id_in_source)?;
    source_store.publish(source_migrating);
    let target_importing = target_store
        .load()
        .as_ref()
        .clone()
        .begin_slot_import_from(slot, source_worker_id_in_target)?;
    target_store.publish(target_importing);

    let moved = source_processor.migrate_slot_to(target_processor, slot, max_keys, true)?;
    let finalized = source_processor.migration_keys_for_slot(slot, 1).is_empty();

    if finalized {
        let source_finalized = source_store
            .load()
            .as_ref()
            .clone()
            .finalize_slot_migration(slot, target_worker_id_in_source)?;
        source_store.publish(source_finalized);
        let target_finalized = target_store
            .load()
            .as_ref()
            .clone()
            .finalize_slot_migration(slot, LOCAL_WORKER_ID)?;
        target_store.publish(target_finalized);
    }

    Ok(LiveSlotMigrationStepOutcome {
        moved_keys: moved,
        finalized,
    })
}

pub fn execute_live_slot_migration(
    source_store: &ClusterConfigStore,
    target_store: &ClusterConfigStore,
    source_processor: &RequestProcessor,
    target_processor: &RequestProcessor,
    slot: u16,
    max_keys: usize,
) -> Result<usize, LiveSlotMigrationError> {
    if max_keys == 0 {
        return Ok(0);
    }

    let mut total_moved = 0usize;
    loop {
        let step = execute_live_slot_migration_step(
            source_store,
            target_store,
            source_processor,
            target_processor,
            slot,
            max_keys,
        )?;
        total_moved += step.moved_keys;
        if step.finalized {
            break;
        }
    }

    Ok(total_moved)
}

pub async fn run_live_slot_migration_until_complete<F>(
    source_store: &ClusterConfigStore,
    target_store: &ClusterConfigStore,
    source_processor: &RequestProcessor,
    target_processor: &RequestProcessor,
    slot: u16,
    max_keys_per_step: usize,
    step_interval: std::time::Duration,
    shutdown: F,
) -> Result<LiveSlotMigrationRunReport, LiveSlotMigrationError>
where
    F: Future<Output = ()>,
{
    if max_keys_per_step == 0 {
        return Ok(LiveSlotMigrationRunReport {
            batches: 0,
            moved_keys: 0,
            finalized: false,
        });
    }

    let mut report = LiveSlotMigrationRunReport {
        batches: 0,
        moved_keys: 0,
        finalized: false,
    };
    let mut interval = tokio::time::interval(step_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => {
                return Ok(report);
            }
            _ = interval.tick() => {
                let step = execute_live_slot_migration_step(
                    source_store,
                    target_store,
                    source_processor,
                    target_processor,
                    slot,
                    max_keys_per_step,
                )?;
                report.batches += 1;
                report.moved_keys += step.moved_keys;
                if step.finalized {
                    report.finalized = true;
                    return Ok(report);
                }
            }
        }
    }
}

pub async fn run_live_slot_migrations_until_complete<F>(
    source_store: &ClusterConfigStore,
    target_store: &ClusterConfigStore,
    source_processor: &RequestProcessor,
    target_processor: &RequestProcessor,
    slots: &[u16],
    max_keys_per_step: usize,
    step_interval: std::time::Duration,
    shutdown: F,
) -> Result<LiveSlotMigrationsRunReport, LiveSlotMigrationError>
where
    F: Future<Output = ()>,
{
    let mut unique_slots = Vec::new();
    for slot in slots {
        if !unique_slots.contains(slot) {
            unique_slots.push(*slot);
        }
    }

    let mut reports: Vec<LiveSlotMigrationSlotReport> = unique_slots
        .iter()
        .map(|slot| LiveSlotMigrationSlotReport {
            slot: *slot,
            batches: 0,
            moved_keys: 0,
            finalized: false,
        })
        .collect();
    if reports.is_empty() {
        return Ok(LiveSlotMigrationsRunReport {
            slots: reports,
            interrupted: false,
        });
    }
    if max_keys_per_step == 0 {
        return Ok(LiveSlotMigrationsRunReport {
            slots: reports,
            interrupted: true,
        });
    }

    let mut pending_indices: VecDeque<usize> = (0..reports.len()).collect();
    let mut interval = tokio::time::interval(step_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tokio::pin!(shutdown);

    while !pending_indices.is_empty() {
        tokio::select! {
            biased;
            _ = &mut shutdown => {
                return Ok(LiveSlotMigrationsRunReport {
                    slots: reports,
                    interrupted: true,
                });
            }
            _ = interval.tick() => {
                let report_index = pending_indices
                    .pop_front()
                    .expect("pending queue must be non-empty while loop is active");
                let slot = reports[report_index].slot;
                let step = execute_live_slot_migration_step(
                    source_store,
                    target_store,
                    source_processor,
                    target_processor,
                    slot,
                    max_keys_per_step,
                )?;
                let report = &mut reports[report_index];
                report.batches += 1;
                report.moved_keys += step.moved_keys;
                report.finalized = step.finalized;
                if !step.finalized {
                    pending_indices.push_back(report_index);
                }
            }
        }
    }

    Ok(LiveSlotMigrationsRunReport {
        slots: reports,
        interrupted: false,
    })
}

pub async fn run_detected_live_slot_migrations_until_complete<F>(
    source_store: &ClusterConfigStore,
    target_store: &ClusterConfigStore,
    source_processor: &RequestProcessor,
    target_processor: &RequestProcessor,
    max_keys_per_step: usize,
    step_interval: std::time::Duration,
    shutdown: F,
) -> Result<LiveSlotMigrationsRunReport, LiveSlotMigrationError>
where
    F: Future<Output = ()>,
{
    let slots = detect_live_slot_migration_slots(source_store, target_store)?;
    run_live_slot_migrations_until_complete(
        source_store,
        target_store,
        source_processor,
        target_processor,
        &slots,
        max_keys_per_step,
        step_interval,
        shutdown,
    )
    .await
}

pub async fn run_detected_live_slot_migrations_until_shutdown<F>(
    source_store: &ClusterConfigStore,
    target_store: &ClusterConfigStore,
    source_processor: &RequestProcessor,
    target_processor: &RequestProcessor,
    max_keys_per_step: usize,
    step_interval: std::time::Duration,
    detection_interval: std::time::Duration,
    shutdown: F,
) -> Result<Vec<LiveSlotMigrationsRunReport>, LiveSlotMigrationError>
where
    F: Future<Output = ()>,
{
    let mut interval = tokio::time::interval(detection_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tokio::pin!(shutdown);
    let mut reports = Vec::new();

    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => return Ok(reports),
            _ = interval.tick() => {
                let slots = detect_live_slot_migration_slots(source_store, target_store)?;
                if slots.is_empty() {
                    continue;
                }

                let mut run = Box::pin(run_live_slot_migrations_until_complete(
                    source_store,
                    target_store,
                    source_processor,
                    target_processor,
                    &slots,
                    max_keys_per_step,
                    step_interval,
                    std::future::pending::<()>(),
                ));
                tokio::select! {
                    biased;
                    _ = &mut shutdown => return Ok(reports),
                    report = &mut run => reports.push(report?),
                }
            }
        }
    }
}

async fn wait_for_watch_shutdown(mut shutdown: tokio::sync::watch::Receiver<bool>) {
    if *shutdown.borrow() {
        return;
    }
    loop {
        if shutdown.changed().await.is_err() || *shutdown.borrow() {
            return;
        }
    }
}

pub async fn run_cluster_manager_with_config_updates_failover_and_detected_migrations<F, T, R>(
    manager: &mut garnet_cluster::ClusterManager<T>,
    source_store: &ClusterConfigStore,
    target_store: &ClusterConfigStore,
    source_processor: &RequestProcessor,
    target_processor: &RequestProcessor,
    updates: tokio::sync::mpsc::UnboundedReceiver<garnet_cluster::ClusterConfig>,
    failure_detector: &mut garnet_cluster::FailureDetector,
    failover_controller: &mut garnet_cluster::ClusterFailoverController,
    replication_manager: &mut garnet_cluster::ReplicationManager,
    replication_transport: &mut R,
    migration_max_keys_per_step: usize,
    migration_step_interval: std::time::Duration,
    migration_detection_interval: std::time::Duration,
    shutdown: F,
) -> Result<ClusterManagerFailoverMigrationRunReport, ClusterManagerFailoverMigrationError<R::Error>>
where
    F: Future<Output = ()> + Send + 'static,
    T: garnet_cluster::AsyncGossipTransport,
    R: garnet_cluster::AsyncReplicationTransport,
{
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let shutdown_task = tokio::spawn(async move {
        shutdown.await;
        let _ = shutdown_tx.send(true);
    });
    let failover_shutdown = shutdown_rx.clone();
    let migration_shutdown = shutdown_rx.clone();

    let failover_future = async {
        manager
            .run_with_config_updates_and_failover_report(
                source_store,
                updates,
                failure_detector,
                failover_controller,
                replication_manager,
                replication_transport,
                wait_for_watch_shutdown(failover_shutdown),
            )
            .await
            .map_err(ClusterManagerFailoverMigrationError::Failover)
    };
    let migration_future = async {
        run_detected_live_slot_migrations_until_shutdown(
            source_store,
            target_store,
            source_processor,
            target_processor,
            migration_max_keys_per_step,
            migration_step_interval,
            migration_detection_interval,
            wait_for_watch_shutdown(migration_shutdown),
        )
        .await
        .map_err(ClusterManagerFailoverMigrationError::Migration)
    };

    let run_result = tokio::try_join!(failover_future, migration_future);
    shutdown_task.abort();
    let (failover_report, migration_reports) = run_result?;
    Ok(ClusterManagerFailoverMigrationRunReport {
        failover_report,
        migration_reports,
    })
}

pub async fn run_listener_with_cluster_control_plane<F, T, R>(
    listener: TcpListener,
    read_buffer_size: usize,
    metrics: Arc<ServerMetrics>,
    source_store: Arc<ClusterConfigStore>,
    manager: &mut garnet_cluster::ClusterManager<T>,
    target_store: &ClusterConfigStore,
    target_processor: &RequestProcessor,
    updates: tokio::sync::mpsc::UnboundedReceiver<garnet_cluster::ClusterConfig>,
    failure_detector: &mut garnet_cluster::FailureDetector,
    failover_controller: &mut garnet_cluster::ClusterFailoverController,
    replication_manager: &mut garnet_cluster::ReplicationManager,
    replication_transport: &mut R,
    migration_max_keys_per_step: usize,
    migration_step_interval: std::time::Duration,
    migration_detection_interval: std::time::Duration,
    shutdown: F,
) -> Result<ClusterManagerFailoverMigrationRunReport, ClusteredServerRunError<R::Error>>
where
    F: Future<Output = ()> + Send + 'static,
    T: garnet_cluster::AsyncGossipTransport,
    R: garnet_cluster::AsyncReplicationTransport,
{
    let processor = Arc::new(RequestProcessor::new().map_err(|err| {
        io::Error::new(
            io::ErrorKind::Other,
            format!("request processor initialization failed: {err}"),
        )
    })?);

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let shutdown_signal_tx = shutdown_tx.clone();
    let shutdown_task = tokio::spawn(async move {
        shutdown.await;
        let _ = shutdown_signal_tx.send(true);
    });

    let server_shutdown = shutdown_rx.clone();
    let control_plane_shutdown = shutdown_rx.clone();
    let server_processor = Arc::clone(&processor);
    let server_store = Arc::clone(&source_store);
    let server_task = tokio::spawn(async move {
        run_listener_with_shutdown_and_cluster_with_processor(
            listener,
            read_buffer_size,
            metrics,
            wait_for_watch_shutdown(server_shutdown),
            Some(server_store),
            server_processor,
        )
        .await
    });

    let control_plane_result =
        run_cluster_manager_with_config_updates_failover_and_detected_migrations(
            manager,
            source_store.as_ref(),
            target_store,
            processor.as_ref(),
            target_processor,
            updates,
            failure_detector,
            failover_controller,
            replication_manager,
            replication_transport,
            migration_max_keys_per_step,
            migration_step_interval,
            migration_detection_interval,
            wait_for_watch_shutdown(control_plane_shutdown),
        )
        .await;

    let _ = shutdown_tx.send(true);
    shutdown_task.abort();
    let server_result = server_task.await.map_err(|join_error| {
        io::Error::new(
            io::ErrorKind::Other,
            format!("server task join failed: {join_error}"),
        )
    })?;

    let control_plane_report = control_plane_result?;
    server_result?;
    Ok(control_plane_report)
}

pub async fn run_with_cluster_control_plane<F, T, R>(
    config: ServerConfig,
    metrics: Arc<ServerMetrics>,
    source_store: Arc<ClusterConfigStore>,
    manager: &mut garnet_cluster::ClusterManager<T>,
    target_store: &ClusterConfigStore,
    target_processor: &RequestProcessor,
    updates: tokio::sync::mpsc::UnboundedReceiver<garnet_cluster::ClusterConfig>,
    failure_detector: &mut garnet_cluster::FailureDetector,
    failover_controller: &mut garnet_cluster::ClusterFailoverController,
    replication_manager: &mut garnet_cluster::ReplicationManager,
    replication_transport: &mut R,
    migration_max_keys_per_step: usize,
    migration_step_interval: std::time::Duration,
    migration_detection_interval: std::time::Duration,
    shutdown: F,
) -> Result<ClusterManagerFailoverMigrationRunReport, ClusteredServerRunError<R::Error>>
where
    F: Future<Output = ()> + Send + 'static,
    T: garnet_cluster::AsyncGossipTransport,
    R: garnet_cluster::AsyncReplicationTransport,
{
    let listener = TcpListener::bind(config.bind_addr).await?;
    run_listener_with_cluster_control_plane(
        listener,
        config.read_buffer_size,
        metrics,
        source_store,
        manager,
        target_store,
        target_processor,
        updates,
        failure_detector,
        failover_controller,
        replication_manager,
        replication_transport,
        migration_max_keys_per_step,
        migration_step_interval,
        migration_detection_interval,
        shutdown,
    )
    .await
}

async fn handle_connection(
    mut stream: TcpStream,
    read_buffer_size: usize,
    metrics: Arc<ServerMetrics>,
    processor: Arc<RequestProcessor>,
    cluster_config: Option<Arc<ClusterConfigStore>>,
    owner_thread_pool: Option<Arc<ShardOwnerThreadPool>>,
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

                                if let Some(owner_pool) = owner_thread_pool.as_ref() {
                                    if let Some(shard_index) = owner_routed_shard_for_command(
                                        &processor,
                                        &args[..meta.argument_count],
                                        command,
                                    ) {
                                        let mut owned_args =
                                            Vec::with_capacity(meta.argument_count);
                                        for arg in &args[..meta.argument_count] {
                                            // SAFETY: `args` points to the live receive buffer.
                                            owned_args.push(unsafe { arg.as_slice() }.to_vec());
                                        }
                                        let routed_processor = Arc::clone(&processor);
                                        match owner_pool.execute_sync(shard_index, move || {
                                            execute_owned_args_via_processor(
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
                                                responses
                                                    .extend_from_slice(b"-ERR protocol error\r\n");
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

                                if let Err(error) =
                                    processor.execute(&args[..meta.argument_count], &mut responses)
                                {
                                    error.append_resp_error(&mut responses);
                                } else if command_is_mutating(command) {
                                    propagate_frame = true;
                                }
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

#[derive(Debug)]
enum RoutedExecutionError {
    Protocol,
    Request(RequestExecutionError),
}

fn execute_owned_args_via_processor(
    processor: &RequestProcessor,
    owned_args: &[Vec<u8>],
) -> Result<Vec<u8>, RoutedExecutionError> {
    if owned_args.is_empty() || owned_args.len() > 64 {
        return Err(RoutedExecutionError::Protocol);
    }

    let mut args = [ArgSlice::EMPTY; 64];
    for (index, arg) in owned_args.iter().enumerate() {
        args[index] = ArgSlice::from_slice(arg).map_err(|_| RoutedExecutionError::Protocol)?;
    }

    let mut response = Vec::new();
    processor
        .execute(&args[..owned_args.len()], &mut response)
        .map_err(RoutedExecutionError::Request)?;
    Ok(response)
}

#[cfg(test)]
fn execute_frame_via_processor(
    processor: &RequestProcessor,
    frame: &[u8],
) -> Result<Vec<u8>, RoutedExecutionError> {
    let mut args = [ArgSlice::EMPTY; 64];
    let meta = parse_resp_command_arg_slices(frame, &mut args)
        .map_err(|_| RoutedExecutionError::Protocol)?;
    if meta.bytes_consumed != frame.len() {
        return Err(RoutedExecutionError::Protocol);
    }
    let mut response = Vec::new();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .map_err(RoutedExecutionError::Request)?;
    Ok(response)
}

fn build_owner_thread_pool(
    processor: &Arc<RequestProcessor>,
) -> io::Result<Option<Arc<ShardOwnerThreadPool>>> {
    let owner_threads = match parse_positive_env_usize(GARNET_STRING_OWNER_THREADS_ENV) {
        Some(value) => value,
        None => return Ok(None),
    };
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
    Ok(Some(Arc::new(pool)))
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

#[cfg(test)]
mod tests;
