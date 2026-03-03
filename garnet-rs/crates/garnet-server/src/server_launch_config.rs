use garnet_server::ServerConfig;
use std::net::SocketAddr;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ServerLaunchConfig {
    pub(super) bind_addrs: Vec<SocketAddr>,
    pub(super) multi_port_cluster_mode: bool,
    pub(super) multi_port_slot_policy: SlotOwnershipPolicy,
    pub(super) owner_thread_pinning: ThreadPinningConfig,
    pub(super) read_buffer_size: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SlotOwnershipPolicy {
    Modulo,
    Contiguous,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ThreadPinningConfig {
    pub(super) enabled: bool,
    pub(super) cpu_set: Vec<usize>,
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

#[allow(clippy::too_many_arguments)]
pub(super) fn parse_server_launch_config_from_values(
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

pub(super) fn parse_server_launch_config_from_env() -> std::io::Result<ServerLaunchConfig> {
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
