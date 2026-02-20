//! TCP server accept loop and connection handler primitives.

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL_ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

pub mod aof_replay;
pub mod command_dispatch;
pub mod debug_concurrency;
pub mod limited_fixed_buffer_pool;
pub mod request_lifecycle;
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
pub use shard_owner_threads::{ShardOwnerThreadPool, ShardOwnerThreadPoolError};

use garnet_cluster::{
    redis_hash_slot, ClusterConfigError, ClusterConfigStore, SlotRouteDecision, SlotState,
    LOCAL_WORKER_ID,
};
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
    let owner_thread_pool = build_owner_thread_pool(&processor)?;
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
                let _ = stream.set_nodelay(true);
                metrics.accepted_connections.fetch_add(1, Ordering::Relaxed);
                metrics.active_connections.fetch_add(1, Ordering::Relaxed);

                let task_metrics = Arc::clone(&metrics);
                let task_processor = Arc::clone(&processor);
                let task_cluster = cluster_config.clone();
                let task_owner_threads = owner_thread_pool.clone();
                tasks.spawn(async move {
                    let _ =
                        handle_connection(
                            stream,
                            read_buffer_size,
                            task_metrics,
                            task_processor,
                            task_cluster,
                            task_owner_threads,
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
    owner_thread_pool: Option<Arc<ShardOwnerThreadPool>>,
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
                                        consumed += meta.bytes_consumed;
                                        continue;
                                    }
                                }

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

fn owner_routed_shard_for_command(
    processor: &RequestProcessor,
    args: &[ArgSlice],
    command: CommandId,
) -> Option<usize> {
    if args.len() < 2 {
        return None;
    }

    let is_routed = match command {
        CommandId::Get
        | CommandId::Set
        | CommandId::Incr
        | CommandId::Decr
        | CommandId::Expire
        | CommandId::Pexpire
        | CommandId::Ttl
        | CommandId::Pttl
        | CommandId::Persist => true,
        CommandId::Del => args.len() == 2,
        _ => false,
    };
    if !is_routed {
        return None;
    }

    // SAFETY: ArgSlice memory is owned by the live receive buffer in the caller.
    let key = unsafe { args[1].as_slice() };
    Some(processor.string_store_shard_index(key))
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
            let mut replication_manager = ReplicationManager::new(Some(7), 2_000, 2_200).unwrap();
            let (repl_tx, mut repl_rx) = tokio::sync::mpsc::unbounded_channel::<ReplicationEvent>();
            let mut replication_transport = ChannelReplicationTransport::new(repl_tx, 1_980);
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
            assert!(report.failover_report.gossip_reports.len() > 0);
            assert!(report.migration_reports.len() > 0);
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
        let mut replication_manager = ReplicationManager::new(Some(7), 2_000, 2_200).unwrap();
        let (repl_tx, _repl_rx) = tokio::sync::mpsc::unbounded_channel::<ReplicationEvent>();
        let mut replication_transport = ChannelReplicationTransport::new(repl_tx, 1_980);
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
        let client_addr = addr1;
        let client = tokio::spawn(async move {
            for _ in 0..50 {
                if let Ok(mut stream) = TcpStream::connect(client_addr).await {
                    send_and_expect(&mut stream, b"*1\r\n$4\r\nPING\r\n", b"+PONG\r\n").await;
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
        let mut replication_manager = ReplicationManager::new(Some(7), 2_000, 2_200).unwrap();
        let (repl_tx, mut repl_rx) = tokio::sync::mpsc::unbounded_channel::<ReplicationEvent>();
        let mut replication_transport = ChannelReplicationTransport::new(repl_tx, 1_980);
        let (_updates_tx, updates_rx) = tokio::sync::mpsc::unbounded_channel::<ClusterConfig>();

        let report = run_with_cluster_control_plane(
            ServerConfig {
                bind_addr: addr1,
                read_buffer_size: 1024,
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
            tokio::time::sleep(Duration::from_millis(30)),
        )
        .await
        .unwrap();
        client.await.unwrap();

        assert!(report.failover_report.gossip_reports.len() > 0);
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
        let mut replication_manager = ReplicationManager::new(Some(7), 2_000, 2_200).unwrap();
        let (repl_tx, _repl_rx) = tokio::sync::mpsc::unbounded_channel::<ReplicationEvent>();
        let mut replication_transport = ChannelReplicationTransport::new(repl_tx, 1_980);
        let (_updates_tx, updates_rx) = tokio::sync::mpsc::unbounded_channel::<ClusterConfig>();

        let result = run_with_cluster_control_plane(
            ServerConfig {
                bind_addr: addr,
                read_buffer_size: 1024,
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
        let mut replication_manager = ReplicationManager::new(Some(7), 2_000, 2_200).unwrap();
        let (repl_tx, mut repl_rx) = tokio::sync::mpsc::unbounded_channel::<ReplicationEvent>();
        let mut replication_transport = ChannelReplicationTransport::new(repl_tx, 1_980);
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
        let mut replication_manager = ReplicationManager::new(Some(7), 2_000, 2_200).unwrap();
        let (repl_tx, _repl_rx) = tokio::sync::mpsc::unbounded_channel::<ReplicationEvent>();
        let mut replication_transport = ChannelReplicationTransport::new(repl_tx, 1_980);
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
        let slot = 860u16;
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
        let mut replication_manager = ReplicationManager::new(Some(7), 2_000, 2_200).unwrap();
        let (repl_tx, repl_rx) = tokio::sync::mpsc::unbounded_channel::<ReplicationEvent>();
        drop(repl_rx);
        let mut replication_transport = ChannelReplicationTransport::new(repl_tx, 1_980);
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
            .migrate_slot_to(&target_processor, slot, 16, true)
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
        let slot_a = 510u16;
        let slot_b = 511u16;
        let slot_c = 512u16;
        let slot_d = 513u16;

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
        let migrated_slots: HashSet<u16> = report.slots.iter().map(|slot| slot.slot).collect();
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
        config1 = next1
            .set_slot_state(901, LOCAL_WORKER_ID, SlotState::Stable)
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
            .set_slot_state(901, node1_id_in_2, SlotState::Stable)
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
            store1.load().slot_assigned_owner(901).unwrap(),
            LOCAL_WORKER_ID
        );
        assert_eq!(
            store2.load().slot_assigned_owner(901).unwrap(),
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
        let migrated_slots: HashSet<u16> = reports
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
        config1 = next1
            .set_slot_state(1001, LOCAL_WORKER_ID, SlotState::Stable)
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
            .set_slot_state(1001, node1_id_in_2, SlotState::Stable)
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
            owner_routed_shard_for_command(
                &processor,
                &args[..del_meta.argument_count],
                del_command
            ),
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

    fn execute_processor_frame(processor: &RequestProcessor, frame: &[u8]) -> Vec<u8> {
        let mut args = [ArgSlice::EMPTY; 64];
        let meta = parse_resp_command_arg_slices(frame, &mut args).unwrap();
        let mut response = Vec::new();
        processor
            .execute(&args[..meta.argument_count], &mut response)
            .unwrap();
        response
    }

    fn owned_args_from_frame(frame: &[u8]) -> Vec<Vec<u8>> {
        let mut args = [ArgSlice::EMPTY; 64];
        let meta = parse_resp_command_arg_slices(frame, &mut args).unwrap();
        let mut owned = Vec::with_capacity(meta.argument_count);
        for arg in &args[..meta.argument_count] {
            // SAFETY: ArgSlice references `frame`, which is alive for this conversion.
            owned.push(unsafe { arg.as_slice() }.to_vec());
        }
        owned
    }
}
