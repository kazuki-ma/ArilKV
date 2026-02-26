use garnet_cluster::ClusterConfig;
use garnet_cluster::ClusterConfigStore;
use garnet_cluster::HASH_SLOT_COUNT;
use garnet_cluster::SlotNumber;
use garnet_cluster::SlotState;
use garnet_cluster::Worker;
use garnet_cluster::WorkerId;
use garnet_cluster::WorkerRole;
use garnet_server::ServerConfig;
use garnet_server::ServerMetrics;
use garnet_server::run_with_shutdown_and_cluster_config;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::mpsc;

use crate::server_launch_config::ServerLaunchConfig;
use crate::server_launch_config::SlotOwnershipPolicy;
use crate::server_launch_config::ThreadPinningConfig;

struct ListenerThread {
    bind_addr: SocketAddr,
    shutdown_tx: tokio::sync::oneshot::Sender<()>,
    join_handle: std::thread::JoinHandle<()>,
}

struct ListenerThreadResult {
    bind_addr: SocketAddr,
    result: std::io::Result<()>,
}

fn cluster_config_error_to_io(context: &str, error: impl core::fmt::Display) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::Other, format!("{context}: {error}"))
}

fn build_cluster_store_for_local_index(
    bind_addrs: &[SocketAddr],
    local_index: usize,
    policy: SlotOwnershipPolicy,
) -> std::io::Result<Arc<ClusterConfigStore>> {
    let local_addr = bind_addrs[local_index];
    let mut config = ClusterConfig::new_local(
        format!("node-{local_index}"),
        local_addr.ip().to_string(),
        local_addr.port(),
    );

    let mut worker_ids_by_index = vec![WorkerId::new(0); bind_addrs.len()];
    worker_ids_by_index[local_index] = config
        .local_worker()
        .map_err(|error| cluster_config_error_to_io("failed to read local worker", error))?
        .id;

    for (index, bind_addr) in bind_addrs.iter().enumerate() {
        if index == local_index {
            continue;
        }
        let (next, worker_id) = config
            .add_worker(Worker::new(
                format!("node-{index}"),
                bind_addr.ip().to_string(),
                bind_addr.port(),
                WorkerRole::Primary,
            ))
            .map_err(|error| cluster_config_error_to_io("failed to add cluster worker", error))?;
        config = next;
        worker_ids_by_index[index] = worker_id;
    }

    for slot in 0..HASH_SLOT_COUNT {
        let owner_index = slot_owner_index(slot, bind_addrs.len(), policy);
        let owner_worker_id = worker_ids_by_index[owner_index];
        let slot_number = SlotNumber::new(
            u16::try_from(slot).expect("slot index must fit into u16 hash-slot range"),
        );
        config = config
            .set_slot_state(slot_number, owner_worker_id, SlotState::Stable)
            .map_err(|error| {
                cluster_config_error_to_io("failed to set cluster slot owner", error)
            })?;
    }

    Ok(Arc::new(ClusterConfigStore::new(config)))
}

pub(super) fn build_multi_port_cluster_stores(
    bind_addrs: &[SocketAddr],
    policy: SlotOwnershipPolicy,
) -> std::io::Result<Vec<Arc<ClusterConfigStore>>> {
    let mut stores = Vec::with_capacity(bind_addrs.len());
    for local_index in 0..bind_addrs.len() {
        stores.push(build_cluster_store_for_local_index(
            bind_addrs,
            local_index,
            policy,
        )?);
    }
    Ok(stores)
}

pub(super) fn slot_owner_index(
    slot: usize,
    node_count: usize,
    policy: SlotOwnershipPolicy,
) -> usize {
    if node_count <= 1 {
        return 0;
    }
    match policy {
        SlotOwnershipPolicy::Modulo => slot % node_count,
        SlotOwnershipPolicy::Contiguous => {
            let base = HASH_SLOT_COUNT / node_count;
            let extra = HASH_SLOT_COUNT % node_count;
            let threshold = (base + 1) * extra;
            if slot < threshold {
                slot / (base + 1)
            } else {
                extra + ((slot - threshold) / base)
            }
        }
    }
}

pub(super) fn resolve_core_assignments_from_available(
    available_core_ids: &[usize],
    listener_count: usize,
    pinning: &ThreadPinningConfig,
) -> std::io::Result<Vec<Option<usize>>> {
    if !pinning.enabled {
        return Ok(vec![None; listener_count]);
    }
    if available_core_ids.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "owner-thread pinning requested, but no CPU cores were reported",
        ));
    }

    let pinned_pool: Vec<usize> = if pinning.cpu_set.is_empty() {
        available_core_ids.to_vec()
    } else {
        let mut selected = Vec::with_capacity(pinning.cpu_set.len());
        for requested in &pinning.cpu_set {
            if !available_core_ids.contains(requested) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "invalid GARNET_OWNER_THREAD_CPU_SET: CPU `{requested}` is not available"
                    ),
                ));
            }
            selected.push(*requested);
        }
        selected
    };

    let mut assignments = Vec::with_capacity(listener_count);
    for index in 0..listener_count {
        assignments.push(Some(pinned_pool[index % pinned_pool.len()]));
    }
    Ok(assignments)
}

fn resolve_core_assignments(
    listener_count: usize,
    pinning: &ThreadPinningConfig,
) -> std::io::Result<Vec<Option<usize>>> {
    if !pinning.enabled {
        return Ok(vec![None; listener_count]);
    }
    let available = core_affinity::get_core_ids().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::Other,
            "owner-thread pinning requested, but CPU enumeration failed",
        )
    })?;
    let available_ids: Vec<usize> = available.iter().map(|core| core.id).collect();
    resolve_core_assignments_from_available(&available_ids, listener_count, pinning)
}

fn pin_current_thread_to_cpu(cpu_id: usize) -> std::io::Result<()> {
    let available = core_affinity::get_core_ids().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::Other,
            "failed to enumerate CPU cores for owner-thread pinning",
        )
    })?;
    let core = available
        .into_iter()
        .find(|core| core.id == cpu_id)
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("requested CPU `{cpu_id}` is not available"),
            )
        })?;
    if core_affinity::set_for_current(core) {
        return Ok(());
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::Other,
        format!("failed to pin current thread to CPU `{cpu_id}`"),
    ))
}

fn spawn_listener_thread(
    bind_addr: SocketAddr,
    read_buffer_size: usize,
    pinned_cpu: Option<usize>,
    cluster_config: Option<Arc<ClusterConfigStore>>,
    result_tx: mpsc::Sender<ListenerThreadResult>,
) -> std::io::Result<ListenerThread> {
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let join_handle = std::thread::Builder::new()
        .name(format!("garnet-node-{bind_addr}"))
        .spawn(move || {
            if let Some(cpu_id) = pinned_cpu {
                if let Err(error) = pin_current_thread_to_cpu(cpu_id) {
                    eprintln!(
                        "warning: listener {bind_addr} could not pin to CPU `{cpu_id}` ({error}); continuing without affinity"
                    );
                }
            }
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build();
            let result = match runtime {
                Ok(runtime) => runtime.block_on(async move {
                    let config = ServerConfig {
                        bind_addr,
                        read_buffer_size,
                    };
                    let metrics = Arc::new(ServerMetrics::default());
                    run_with_shutdown_and_cluster_config(
                        config,
                        metrics,
                        async move {
                            let _ = shutdown_rx.await;
                        },
                        cluster_config,
                    )
                    .await
                }),
                Err(error) => Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("failed to build runtime for listener {bind_addr}: {error}"),
                )),
            };
            let _ = result_tx.send(ListenerThreadResult { bind_addr, result });
        })
        .map_err(|error| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("failed to spawn listener thread for {bind_addr}: {error}"),
            )
        })?;
    Ok(ListenerThread {
        bind_addr,
        shutdown_tx,
        join_handle,
    })
}

fn shutdown_and_join_listener_threads(listeners: Vec<ListenerThread>) -> std::io::Result<()> {
    let mut panicked = Vec::new();
    for listener in listeners {
        let _ = listener.shutdown_tx.send(());
        if listener.join_handle.join().is_err() {
            panicked.push(listener.bind_addr);
        }
    }

    if panicked.is_empty() {
        return Ok(());
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::Other,
        format!(
            "listener thread panicked for {}",
            panicked
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        ),
    ))
}

pub(super) fn run_multi_bind_addrs(launch: ServerLaunchConfig) -> std::io::Result<()> {
    let (result_tx, result_rx) = mpsc::channel::<ListenerThreadResult>();
    let mut listeners = Vec::with_capacity(launch.bind_addrs.len());
    let pinned_cpus =
        resolve_core_assignments(launch.bind_addrs.len(), &launch.owner_thread_pinning)?;

    let cluster_stores = if launch.multi_port_cluster_mode {
        Some(build_multi_port_cluster_stores(
            &launch.bind_addrs,
            launch.multi_port_slot_policy,
        )?)
    } else {
        None
    };

    for (index, bind_addr) in launch.bind_addrs.into_iter().enumerate() {
        let cluster_store = cluster_stores
            .as_ref()
            .and_then(|stores| stores.get(index).cloned());
        match spawn_listener_thread(
            bind_addr,
            launch.read_buffer_size,
            pinned_cpus[index],
            cluster_store,
            result_tx.clone(),
        ) {
            Ok(listener) => listeners.push(listener),
            Err(error) => {
                let _ = shutdown_and_join_listener_threads(listeners);
                return Err(error);
            }
        }
    }

    drop(result_tx);

    let first = result_rx.recv().map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::Other,
            "all listener threads exited before reporting a result",
        )
    })?;

    shutdown_and_join_listener_threads(listeners)?;

    match first.result {
        Ok(()) => Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("listener {} exited unexpectedly", first.bind_addr),
        )),
        Err(error) => Err(std::io::Error::new(
            error.kind(),
            format!("listener {} failed: {error}", first.bind_addr),
        )),
    }
}
