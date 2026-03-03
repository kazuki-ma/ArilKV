use garnet_cluster::ClusterConfigError;
use garnet_cluster::ClusterConfigStore;
use garnet_cluster::LOCAL_WORKER_ID;
use garnet_cluster::SlotNumber;
use garnet_cluster::SlotState;
use garnet_cluster::WorkerId;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::future::Future;
use std::io;
use std::sync::Arc;
use tokio::net::TcpListener;

use crate::RequestExecutionError;
use crate::RequestProcessor;
use crate::ServerConfig;
use crate::ServerMetrics;
use crate::run_listener_with_shutdown_and_cluster_with_processor;

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
    pub slot: SlotNumber,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct MigrationPeerIds {
    target_worker_id_in_source: WorkerId,
    source_worker_id_in_target: WorkerId,
}

impl MigrationPeerIds {
    const fn new(
        target_worker_id_in_source: WorkerId,
        source_worker_id_in_target: WorkerId,
    ) -> Self {
        Self {
            target_worker_id_in_source,
            source_worker_id_in_target,
        }
    }
}

fn resolve_migration_peer_ids(
    source_store: &ClusterConfigStore,
    target_store: &ClusterConfigStore,
) -> Result<MigrationPeerIds, LiveSlotMigrationError> {
    let source_snapshot = source_store.load();
    let target_snapshot = target_store.load();
    let source_local_node_id = source_snapshot.local_worker()?.node_id.clone();
    let target_local_node_id = target_snapshot.local_worker()?.node_id.clone();

    let target_worker_id_in_source = source_snapshot
        .workers()
        .iter()
        .find(|worker| worker.node_id == target_local_node_id)
        .map(|worker| worker.id)
        .ok_or(LiveSlotMigrationError::MissingSourcePeerNode(
            target_local_node_id,
        ))?;
    let source_worker_id_in_target = target_snapshot
        .workers()
        .iter()
        .find(|worker| worker.node_id == source_local_node_id)
        .map(|worker| worker.id)
        .ok_or(LiveSlotMigrationError::MissingTargetPeerNode(
            source_local_node_id,
        ))?;
    Ok(MigrationPeerIds::new(
        target_worker_id_in_source,
        source_worker_id_in_target,
    ))
}

pub fn detect_live_slot_migration_slots(
    source_store: &ClusterConfigStore,
    target_store: &ClusterConfigStore,
) -> Result<Vec<SlotNumber>, LiveSlotMigrationError> {
    let peer_ids = resolve_migration_peer_ids(source_store, target_store)?;
    let source_snapshot = source_store.load();
    let target_snapshot = target_store.load();
    let source_migrating = source_snapshot.slots_assigned_to_worker_in_state(
        peer_ids.target_worker_id_in_source,
        SlotState::Migrating,
    )?;
    let target_importing: HashSet<SlotNumber> = target_snapshot
        .slots_assigned_to_worker_in_state(
            peer_ids.source_worker_id_in_target,
            SlotState::Importing,
        )?
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
    slot: SlotNumber,
    max_keys: usize,
) -> Result<LiveSlotMigrationStepOutcome, LiveSlotMigrationError> {
    if max_keys == 0 {
        return Ok(LiveSlotMigrationStepOutcome {
            moved_keys: 0,
            finalized: false,
        });
    }

    let peer_ids = resolve_migration_peer_ids(source_store, target_store)?;
    let source_migrating = source_store
        .load()
        .as_ref()
        .clone()
        .begin_slot_migration_to(slot, peer_ids.target_worker_id_in_source)?;
    source_store.publish(source_migrating);
    let target_importing = target_store
        .load()
        .as_ref()
        .clone()
        .begin_slot_import_from(slot, peer_ids.source_worker_id_in_target)?;
    target_store.publish(target_importing);

    let moved = source_processor.migrate_slot_to(target_processor, slot, max_keys, true)?;
    let finalized = source_processor.migration_keys_for_slot(slot, 1).is_empty();

    if finalized {
        let source_finalized = source_store
            .load()
            .as_ref()
            .clone()
            .finalize_slot_migration(slot, peer_ids.target_worker_id_in_source)?;
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
    slot: SlotNumber,
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

#[allow(clippy::too_many_arguments)]
pub async fn run_live_slot_migration_until_complete<F>(
    source_store: &ClusterConfigStore,
    target_store: &ClusterConfigStore,
    source_processor: &RequestProcessor,
    target_processor: &RequestProcessor,
    slot: SlotNumber,
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

#[allow(clippy::too_many_arguments)]
pub async fn run_live_slot_migrations_until_complete<F>(
    source_store: &ClusterConfigStore,
    target_store: &ClusterConfigStore,
    source_processor: &RequestProcessor,
    target_processor: &RequestProcessor,
    slots: &[SlotNumber],
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

#[allow(clippy::too_many_arguments)]
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

#[allow(clippy::too_many_arguments)]
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

#[allow(clippy::too_many_arguments)]
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
        io::Error::other(format!("request processor initialization failed: {err}"))
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
    let server_result = server_task
        .await
        .map_err(|join_error| io::Error::other(format!("server task join failed: {join_error}")))?;

    let control_plane_report = control_plane_result?;
    server_result?;
    Ok(control_plane_report)
}

#[allow(clippy::too_many_arguments)]
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
