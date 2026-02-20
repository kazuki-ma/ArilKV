use garnet_cluster::{
    ClusterConfig, ClusterConfigStore, SlotState, Worker, WorkerRole, HASH_SLOT_COUNT,
};
use garnet_server::{run, run_with_shutdown_and_cluster_config, ServerConfig, ServerMetrics};
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
    read_buffer_size: usize,
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
    read_buffer_size: Option<&str>,
) -> std::io::Result<ServerLaunchConfig> {
    let bind_addrs = parse_bind_addrs_from_values(bind_addrs, bind_addr, owner_node_count)?;
    let multi_port_cluster_mode =
        parse_bool_env_flag(multi_port_cluster_mode, "GARNET_MULTI_PORT_CLUSTER_MODE")?;
    let read_buffer_size = parse_read_buffer_size(read_buffer_size)?;
    Ok(ServerLaunchConfig {
        bind_addrs,
        multi_port_cluster_mode,
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
        let owner_index = slot % bind_addrs.len();
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
) -> std::io::Result<Vec<Arc<ClusterConfigStore>>> {
    let mut stores = Vec::with_capacity(bind_addrs.len());
    for local_index in 0..bind_addrs.len() {
        stores.push(build_cluster_store_for_local_index(
            bind_addrs,
            local_index,
        )?);
    }
    Ok(stores)
}

fn spawn_listener_thread(
    bind_addr: SocketAddr,
    read_buffer_size: usize,
    cluster_config: Option<Arc<ClusterConfigStore>>,
    result_tx: mpsc::Sender<ListenerThreadResult>,
) -> std::io::Result<ListenerThread> {
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let join_handle = std::thread::Builder::new()
        .name(format!("garnet-node-{bind_addr}"))
        .spawn(move || {
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

    let cluster_stores = if launch.multi_port_cluster_mode {
        Some(build_multi_port_cluster_stores(&launch.bind_addrs)?)
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
    if launch.bind_addrs.len() == 1 {
        let config = ServerConfig {
            bind_addr: launch.bind_addrs[0],
            read_buffer_size: launch.read_buffer_size,
        };
        let metrics = Arc::new(ServerMetrics::default());
        return run(config, metrics).await;
    }
    run_multi_bind_addrs(launch)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_server_config_from_values_applies_valid_overrides() {
        let config = parse_server_config_from_values(Some("127.0.0.1:7001"), Some("4096")).unwrap();
        assert_eq!(
            config.bind_addr,
            "127.0.0.1:7001".parse::<SocketAddr>().unwrap()
        );
        assert_eq!(config.read_buffer_size, 4096);
    }

    #[test]
    fn parse_server_config_from_values_rejects_invalid_bind_addr() {
        let error = parse_server_config_from_values(Some("not-an-addr"), None).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains("GARNET_BIND_ADDR"));
    }

    #[test]
    fn parse_server_config_from_values_rejects_invalid_buffer_size() {
        let error = parse_server_config_from_values(None, Some("not-a-number")).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains("GARNET_READ_BUFFER_SIZE"));
    }

    #[test]
    fn parse_server_launch_config_defaults_to_single_default_bind_addr() {
        let config = parse_server_launch_config_from_values(None, None, None, None, None).unwrap();
        assert_eq!(config.bind_addrs.len(), 1);
        assert_eq!(config.bind_addrs[0], ServerConfig::default().bind_addr);
        assert!(!config.multi_port_cluster_mode);
        assert_eq!(
            config.read_buffer_size,
            ServerConfig::default().read_buffer_size
        );
    }

    #[test]
    fn parse_server_launch_config_parses_multiple_bind_addrs() {
        let config = parse_server_launch_config_from_values(
            Some("127.0.0.1:7001"),
            Some("127.0.0.1:7101,127.0.0.1:7102"),
            Some("4"),
            Some("true"),
            Some("2048"),
        )
        .unwrap();
        assert_eq!(config.bind_addrs.len(), 2);
        assert_eq!(
            config.bind_addrs,
            vec![
                "127.0.0.1:7101".parse::<SocketAddr>().unwrap(),
                "127.0.0.1:7102".parse::<SocketAddr>().unwrap()
            ]
        );
        assert!(config.multi_port_cluster_mode);
        assert_eq!(config.read_buffer_size, 2048);
    }

    #[test]
    fn parse_server_launch_config_rejects_duplicate_bind_addrs() {
        let error = parse_server_launch_config_from_values(
            None,
            Some("127.0.0.1:7101,127.0.0.1:7101"),
            None,
            None,
            None,
        )
        .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains("duplicate address"));
    }

    #[test]
    fn parse_server_launch_config_rejects_empty_bind_addrs_segment() {
        let error = parse_server_launch_config_from_values(
            None,
            Some("127.0.0.1:7101, ,127.0.0.1:7102"),
            None,
            None,
            None,
        )
        .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains("empty address segment"));
    }

    #[test]
    fn parse_server_launch_config_expands_owner_node_count_from_bind_addr() {
        let config = parse_server_launch_config_from_values(
            Some("127.0.0.1:7300"),
            None,
            Some("4"),
            None,
            None,
        )
        .unwrap();
        assert_eq!(
            config.bind_addrs,
            vec![
                "127.0.0.1:7300".parse::<SocketAddr>().unwrap(),
                "127.0.0.1:7301".parse::<SocketAddr>().unwrap(),
                "127.0.0.1:7302".parse::<SocketAddr>().unwrap(),
                "127.0.0.1:7303".parse::<SocketAddr>().unwrap(),
            ]
        );
    }

    #[test]
    fn parse_server_launch_config_rejects_invalid_owner_node_count() {
        let error =
            parse_server_launch_config_from_values(None, None, Some("not-a-number"), None, None)
                .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains("GARNET_OWNER_NODE_COUNT"));
    }

    #[test]
    fn parse_server_launch_config_rejects_zero_owner_node_count() {
        let error =
            parse_server_launch_config_from_values(None, None, Some("0"), None, None).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains("must be >= 1"));
    }

    #[test]
    fn parse_server_launch_config_rejects_owner_node_count_port_overflow() {
        let error = parse_server_launch_config_from_values(
            Some("127.0.0.1:65535"),
            None,
            Some("2"),
            None,
            None,
        )
        .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains("exceeds 65535"));
    }

    #[test]
    fn parse_server_launch_config_bind_addrs_takes_priority_over_owner_node_count() {
        let config = parse_server_launch_config_from_values(
            Some("127.0.0.1:7400"),
            Some("127.0.0.1:7501,127.0.0.1:7502"),
            Some("8"),
            Some("false"),
            None,
        )
        .unwrap();
        assert_eq!(
            config.bind_addrs,
            vec![
                "127.0.0.1:7501".parse::<SocketAddr>().unwrap(),
                "127.0.0.1:7502".parse::<SocketAddr>().unwrap(),
            ]
        );
    }

    #[test]
    fn parse_server_launch_config_rejects_invalid_cluster_mode_flag() {
        let error = parse_server_launch_config_from_values(None, None, None, Some("maybe"), None)
            .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains("GARNET_MULTI_PORT_CLUSTER_MODE"));
    }

    #[test]
    fn build_multi_port_cluster_stores_assigns_slot_owners_by_modulo() {
        let bind_addrs = vec![
            "127.0.0.1:8101".parse::<SocketAddr>().unwrap(),
            "127.0.0.1:8102".parse::<SocketAddr>().unwrap(),
            "127.0.0.1:8103".parse::<SocketAddr>().unwrap(),
        ];
        let stores = build_multi_port_cluster_stores(&bind_addrs).unwrap();
        assert_eq!(stores.len(), 3);

        let local0 = stores[0].load();
        let local1 = stores[1].load();

        assert_eq!(local0.local_worker().unwrap().endpoint(), "127.0.0.1:8101");
        assert_eq!(local1.local_worker().unwrap().endpoint(), "127.0.0.1:8102");

        let moved_from_0_for_slot_1 = local0.redirection_error_for_slot(1).unwrap();
        assert_eq!(
            moved_from_0_for_slot_1.as_deref(),
            Some("MOVED 1 127.0.0.1:8102")
        );

        let moved_from_1_for_slot_2 = local1.redirection_error_for_slot(2).unwrap();
        assert_eq!(
            moved_from_1_for_slot_2.as_deref(),
            Some("MOVED 2 127.0.0.1:8103")
        );
    }
}
