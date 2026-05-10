use garnet_server::ServerConfig;
use garnet_server::ServerMetrics;
use garnet_server::run;
use garnet_server::run_with_cluster;
use garnet_server::set_owner_execution_inline_default;
#[cfg(test)]
use std::net::SocketAddr;
use std::sync::Arc;

const TOKIO_WORKER_THREADS_ENV: &str = "TOKIO_WORKER_THREADS";
const DEFAULT_MAX_TOKIO_WORKER_THREADS: usize = 4;

mod multi_port_runtime;
mod server_launch_config;

use crate::multi_port_runtime::build_multi_port_cluster_stores;
#[cfg(test)]
use crate::multi_port_runtime::resolve_core_assignments_from_available;
use crate::multi_port_runtime::run_multi_bind_addrs;
#[cfg(test)]
use crate::multi_port_runtime::slot_owner_index;
#[cfg(test)]
use crate::server_launch_config::SlotOwnershipPolicy;
#[cfg(test)]
use crate::server_launch_config::ThreadPinningConfig;
use crate::server_launch_config::parse_server_launch_config_from_env;
#[cfg(test)]
use crate::server_launch_config::parse_server_launch_config_from_values;
#[cfg(test)]
use crate::server_launch_config::parse_startup_config_overrides_from_values;

fn validate_server_launch_config(
    launch: &server_launch_config::ServerLaunchConfig,
) -> std::io::Result<()> {
    if launch.bind_addrs.len() > 1 && !launch.startup_config_overrides.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "startup persistence/ACL config overrides are not supported with multi-port listener mode",
        ));
    }
    Ok(())
}

fn parse_tokio_worker_threads(raw: Option<&str>) -> std::io::Result<Option<usize>> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let trimmed = raw.trim();
    let parsed = trimmed.parse::<usize>().map_err(|error| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("invalid {TOKIO_WORKER_THREADS_ENV} `{raw}`: {error}"),
        )
    })?;
    if parsed == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("invalid {TOKIO_WORKER_THREADS_ENV} `{raw}`: must be >= 1"),
        ));
    }
    Ok(Some(parsed))
}

fn resolve_tokio_worker_threads(
    raw: Option<&str>,
    available_parallelism: usize,
) -> std::io::Result<usize> {
    if let Some(explicit) = parse_tokio_worker_threads(raw)? {
        return Ok(explicit);
    }
    Ok(available_parallelism
        .max(1)
        .min(DEFAULT_MAX_TOKIO_WORKER_THREADS))
}

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

fn main() -> std::io::Result<()> {
    let available_parallelism = std::thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(DEFAULT_MAX_TOKIO_WORKER_THREADS);
    let worker_threads = resolve_tokio_worker_threads(
        std::env::var(TOKIO_WORKER_THREADS_ENV).ok().as_deref(),
        available_parallelism,
    )?;
    let mut runtime = tokio::runtime::Builder::new_multi_thread();
    runtime.enable_all();
    runtime.worker_threads(worker_threads);
    runtime.build()?.block_on(async_main())
}

async fn async_main() -> std::io::Result<()> {
    let launch = parse_server_launch_config_from_env()?;
    validate_server_launch_config(&launch)?;
    if launch.bind_addrs.len() == 1 && !launch.owner_thread_pinning.enabled {
        let config = ServerConfig {
            bind_addr: launch.bind_addrs[0],
            read_buffer_size: launch.read_buffer_size,
            startup_config_overrides: launch.startup_config_overrides.clone(),
        };
        let metrics = Arc::new(ServerMetrics::default());
        if launch.multi_port_cluster_mode {
            let cluster_store =
                build_multi_port_cluster_stores(&launch.bind_addrs, launch.multi_port_slot_policy)?
                    .into_iter()
                    .next()
                    .ok_or_else(|| {
                        std::io::Error::other("failed to build cluster store for single-port mode")
                    })?;
            return run_with_cluster(config, metrics, cluster_store).await;
        }
        return run(config, metrics).await;
    }
    set_owner_execution_inline_default(true);
    run_multi_bind_addrs(launch)
}

#[cfg(test)]
mod main_tests;
