use garnet_cluster::{
    ClusterConfig, ClusterConfigStore, SlotState, Worker, WorkerRole, HASH_SLOT_COUNT,
};
use garnet_server::{
    run, run_with_cluster, run_with_shutdown_and_cluster_config, ServerConfig, ServerMetrics,
};
use std::net::SocketAddr;
use std::sync::mpsc;
use std::sync::Arc;

#[cfg(test)]
fn parse_server_config_from_values(
    bind_addr: Option<&str>,
    read_buffer_size: Option<&str>,
) -> std::io::Result<ServerConfig> {
    let mut config = ServerConfig::default();
    if let Some(bind_addr_raw) = bind_addr {
        config.bind_addr = bind_addr_raw.parse::<SocketAddr>().map_err(|error| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("invalid GARNET_BIND_ADDR `{bind_addr_raw}`: {error}"),
            )
        })?;
    }
    if let Some(read_buffer_size_raw) = read_buffer_size {
        config.read_buffer_size = read_buffer_size_raw.parse::<usize>().map_err(|error| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("invalid GARNET_READ_BUFFER_SIZE `{read_buffer_size_raw}`: {error}"),
            )
        })?;
    }
    Ok(config)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ServerLaunchConfig {
    bind_addrs: Vec<SocketAddr>,
    multi_port_cluster_mode: bool,
    multi_port_slot_policy: SlotOwnershipPolicy,
    owner_thread_pinning: ThreadPinningConfig,
    read_buffer_size: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SlotOwnershipPolicy {
    Modulo,
    Contiguous,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ThreadPinningConfig {
    enabled: bool,
    cpu_set: Vec<usize>,
}

fn parse_bind_addr(raw: &str, key: &str) -> std::io::Result<SocketAddr> {
    raw.parse::<SocketAddr>().map_err(|error| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("invalid {key} `{raw}`: {error}"),
        )
    })
}

fn parse_read_buffer_size(read_buffer_size: Option<&str>) -> std::io::Result<usize> {
    match read_buffer_size {
        Some(raw) => raw.parse::<usize>().map_err(|error| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("invalid GARNET_READ_BUFFER_SIZE `{raw}`: {error}"),
            )
        }),
        None => Ok(ServerConfig::default().read_buffer_size),
    }
}

fn parse_bool_env_flag(raw: Option<&str>, key: &str) -> std::io::Result<bool> {
    match raw {
        None => Ok(false),
        Some(value) => {
            let normalized = value.trim().to_ascii_lowercase();
            match normalized.as_str() {
                "1" | "true" | "yes" | "on" => Ok(true),
                "0" | "false" | "no" | "off" => Ok(false),
                _ => Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "invalid {key} `{value}`: expected one of 1/0/true/false/yes/no/on/off"
                    ),
                )),
            }
        }
    }
}

fn parse_owner_node_count(owner_node_count: Option<&str>) -> std::io::Result<usize> {
    match owner_node_count {
        Some(raw) => {
            let parsed = raw.parse::<usize>().map_err(|error| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("invalid GARNET_OWNER_NODE_COUNT `{raw}`: {error}"),
                )
            })?;
            if parsed == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "invalid GARNET_OWNER_NODE_COUNT `0`: must be >= 1",
                ));
            }
            Ok(parsed)
        }
        None => Ok(1),
    }
}

fn parse_slot_ownership_policy(raw: Option<&str>) -> std::io::Result<SlotOwnershipPolicy> {
    match raw {
        None => Ok(SlotOwnershipPolicy::Modulo),
        Some(value) => match value.trim().to_ascii_lowercase().as_str() {
            "modulo" => Ok(SlotOwnershipPolicy::Modulo),
            "contiguous" => Ok(SlotOwnershipPolicy::Contiguous),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "invalid GARNET_MULTI_PORT_SLOT_POLICY `{value}`: expected `modulo` or `contiguous`"
                ),
            )),
        },
    }
}

fn parse_cpu_set(raw: Option<&str>) -> std::io::Result<Vec<usize>> {
    let Some(raw) = raw else {
        return Ok(Vec::new());
    };
    if raw.trim().is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "invalid GARNET_OWNER_THREAD_CPU_SET ``: expected at least one CPU index",
        ));
    }

    let mut cpus = Vec::new();
    for candidate in raw.split(',') {
        let trimmed = candidate.trim();
        if trimmed.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "invalid GARNET_OWNER_THREAD_CPU_SET `{raw}`: contains an empty CPU segment"
                ),
            ));
        }
        let cpu = trimmed.parse::<usize>().map_err(|error| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("invalid GARNET_OWNER_THREAD_CPU_SET `{raw}`: {error}"),
            )
        })?;
        if cpus.contains(&cpu) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("invalid GARNET_OWNER_THREAD_CPU_SET `{raw}`: duplicate CPU `{cpu}`"),
            ));
        }
        cpus.push(cpu);
    }
    Ok(cpus)
}

fn parse_thread_pinning_config(
    pinning_flag: Option<&str>,
    cpu_set_raw: Option<&str>,
) -> std::io::Result<ThreadPinningConfig> {
    let cpu_set = parse_cpu_set(cpu_set_raw)?;
    let enabled_from_flag = parse_bool_env_flag(pinning_flag, "GARNET_OWNER_THREAD_PINNING")?;
    let enabled = enabled_from_flag || !cpu_set.is_empty();
    Ok(ThreadPinningConfig { enabled, cpu_set })
}

fn expand_bind_addrs_from_base(
    base: SocketAddr,
    owner_node_count: usize,
) -> std::io::Result<Vec<SocketAddr>> {
    if owner_node_count == 1 {
        return Ok(vec![base]);
    }

    let last_port = base.port() as u32 + owner_node_count as u32 - 1;
    if last_port > u16::MAX as u32 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "invalid GARNET_OWNER_NODE_COUNT `{owner_node_count}` with base bind `{base}`: port range exceeds 65535"
            ),
        ));
    }

    let mut addrs = Vec::with_capacity(owner_node_count);
    for offset in 0..owner_node_count {
        let mut addr = base;
        addr.set_port(base.port() + offset as u16);
        addrs.push(addr);
    }
    Ok(addrs)
}

fn parse_bind_addrs_from_values(
    bind_addrs: Option<&str>,
    bind_addr: Option<&str>,
    owner_node_count: Option<&str>,
) -> std::io::Result<Vec<SocketAddr>> {
    if let Some(raw) = bind_addrs {
        if raw.trim().is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "invalid GARNET_BIND_ADDRS ``: expected at least one address",
            ));
        }

        let mut addrs = Vec::new();
        for candidate in raw.split(',') {
            let candidate = candidate.trim();
            if candidate.is_empty() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("invalid GARNET_BIND_ADDRS `{raw}`: contains an empty address segment"),
                ));
            }
            let addr = parse_bind_addr(candidate, "GARNET_BIND_ADDRS")?;
            if addrs.contains(&addr) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("invalid GARNET_BIND_ADDRS `{raw}`: duplicate address `{candidate}`"),
                ));
            }
            addrs.push(addr);
        }
        return Ok(addrs);
    }

    let owner_node_count = parse_owner_node_count(owner_node_count)?;
    let base = if let Some(raw) = bind_addr {
        parse_bind_addr(raw, "GARNET_BIND_ADDR")?
    } else {
        ServerConfig::default().bind_addr
    };

    expand_bind_addrs_from_base(base, owner_node_count)
}

fn parse_server_launch_config_from_values(
    bind_addr: Option<&str>,
    bind_addrs: Option<&str>,
    owner_node_count: Option<&str>,
    multi_port_cluster_mode: Option<&str>,
    multi_port_slot_policy: Option<&str>,
    owner_thread_pinning: Option<&str>,
    owner_thread_cpu_set: Option<&str>,
    read_buffer_size: Option<&str>,
) -> std::io::Result<ServerLaunchConfig> {
    let bind_addrs = parse_bind_addrs_from_values(bind_addrs, bind_addr, owner_node_count)?;
    let multi_port_cluster_mode =
        parse_bool_env_flag(multi_port_cluster_mode, "GARNET_MULTI_PORT_CLUSTER_MODE")?;
    let multi_port_slot_policy = parse_slot_ownership_policy(multi_port_slot_policy)?;
    let owner_thread_pinning =
        parse_thread_pinning_config(owner_thread_pinning, owner_thread_cpu_set)?;
    let read_buffer_size = parse_read_buffer_size(read_buffer_size)?;
    Ok(ServerLaunchConfig {
        bind_addrs,
        multi_port_cluster_mode,
        multi_port_slot_policy,
        owner_thread_pinning,
        read_buffer_size,
    })
}

fn parse_server_launch_config_from_env() -> std::io::Result<ServerLaunchConfig> {
    parse_server_launch_config_from_values(
        std::env::var("GARNET_BIND_ADDR").ok().as_deref(),
        std::env::var("GARNET_BIND_ADDRS").ok().as_deref(),
        std::env::var("GARNET_OWNER_NODE_COUNT").ok().as_deref(),
        std::env::var("GARNET_MULTI_PORT_CLUSTER_MODE")
            .ok()
            .as_deref(),
        std::env::var("GARNET_MULTI_PORT_SLOT_POLICY")
            .ok()
            .as_deref(),
        std::env::var("GARNET_OWNER_THREAD_PINNING").ok().as_deref(),
        std::env::var("GARNET_OWNER_THREAD_CPU_SET").ok().as_deref(),
        std::env::var("GARNET_READ_BUFFER_SIZE").ok().as_deref(),
    )
}

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

    let mut worker_ids_by_index = vec![0u16; bind_addrs.len()];
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
        config = config
            .set_slot_state(slot as u16, owner_worker_id, SlotState::Stable)
            .map_err(|error| {
                cluster_config_error_to_io("failed to set cluster slot owner", error)
            })?;
    }

    Ok(Arc::new(ClusterConfigStore::new(config)))
}

fn build_multi_port_cluster_stores(
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

fn slot_owner_index(slot: usize, node_count: usize, policy: SlotOwnershipPolicy) -> usize {
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

fn resolve_core_assignments_from_available(
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

fn run_multi_bind_addrs(launch: ServerLaunchConfig) -> std::io::Result<()> {
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

#[tokio::main(flavor = "multi_thread")]
async fn main() -> std::io::Result<()> {
    let launch = parse_server_launch_config_from_env()?;
    if launch.bind_addrs.len() == 1 && !launch.owner_thread_pinning.enabled {
        let config = ServerConfig {
            bind_addr: launch.bind_addrs[0],
            read_buffer_size: launch.read_buffer_size,
        };
        let metrics = Arc::new(ServerMetrics::default());
        if launch.multi_port_cluster_mode {
            let cluster_store =
                build_multi_port_cluster_stores(&launch.bind_addrs, launch.multi_port_slot_policy)?
                    .into_iter()
                    .next()
                    .ok_or_else(|| {
                        std::io::Error::new(
                            std::io::ErrorKind::Other,
                            "failed to build cluster store for single-port mode",
                        )
                    })?;
            return run_with_cluster(config, metrics, cluster_store).await;
        }
        return run(config, metrics).await;
    }
    run_multi_bind_addrs(launch)
}

#[cfg(test)]
mod main_tests;
