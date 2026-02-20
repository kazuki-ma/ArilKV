//! TCP server accept loop and connection handler primitives.

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL_ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

pub mod aof_replay;
mod cluster_control_plane;
pub mod command_dispatch;
pub mod command_spec;
mod connection_handler;
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
pub use cluster_control_plane::{
    detect_live_slot_migration_slots, execute_live_slot_migration,
    execute_live_slot_migration_step,
    run_cluster_manager_with_config_updates_failover_and_detected_migrations,
    run_detected_live_slot_migrations_until_complete,
    run_detected_live_slot_migrations_until_shutdown, run_listener_with_cluster_control_plane,
    run_live_slot_migration_until_complete, run_live_slot_migrations_until_complete,
    run_with_cluster_control_plane, ClusterManagerFailoverMigrationError,
    ClusterManagerFailoverMigrationRunReport, ClusteredServerRunError, LiveSlotMigrationError,
    LiveSlotMigrationRunReport, LiveSlotMigrationSlotReport, LiveSlotMigrationStepOutcome,
    LiveSlotMigrationsRunReport,
};
pub use command_dispatch::{
    dispatch_command_name, dispatch_from_arg_slices, dispatch_from_resp_args, CommandId,
};
pub(crate) use connection_handler::{build_owner_thread_pool, handle_connection};
#[cfg(test)]
pub(crate) use connection_handler::{
    execute_frame_via_processor, execute_owned_args_via_processor, RoutedExecutionError,
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

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(test)]
use garnet_common::{parse_resp_command_arg_slices, ArgSlice};
#[cfg(test)]
use std::collections::HashSet;
#[cfg(test)]
use std::sync::Arc;
#[cfg(test)]
use tokio::net::{TcpListener, TcpStream};

#[cfg(test)]
use crate::connection_routing::owner_routed_shard_for_command;

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

#[cfg(test)]
mod tests;
