//! TCP server accept loop and connection handler primitives.

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL_ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

pub mod aof_replay;
mod cluster_control_plane;
pub mod command_dispatch;
pub mod command_spec;
mod connection_handler;
mod connection_owner_routing;
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

pub use aof_replay::replay_aof_file;
pub use aof_replay::replay_aof_operations;
pub use cluster_control_plane::ClusterManagerFailoverMigrationError;
pub use cluster_control_plane::ClusterManagerFailoverMigrationRunReport;
pub use cluster_control_plane::ClusteredServerRunError;
pub use cluster_control_plane::LiveSlotMigrationError;
pub use cluster_control_plane::LiveSlotMigrationRunReport;
pub use cluster_control_plane::LiveSlotMigrationSlotReport;
pub use cluster_control_plane::LiveSlotMigrationStepOutcome;
pub use cluster_control_plane::LiveSlotMigrationsRunReport;
pub use cluster_control_plane::detect_live_slot_migration_slots;
pub use cluster_control_plane::execute_live_slot_migration;
pub use cluster_control_plane::execute_live_slot_migration_step;
pub use cluster_control_plane::run_cluster_manager_with_config_updates_failover_and_detected_migrations;
pub use cluster_control_plane::run_detected_live_slot_migrations_until_complete;
pub use cluster_control_plane::run_detected_live_slot_migrations_until_shutdown;
pub use cluster_control_plane::run_listener_with_cluster_control_plane;
pub use cluster_control_plane::run_live_slot_migration_until_complete;
pub use cluster_control_plane::run_live_slot_migrations_until_complete;
pub use cluster_control_plane::run_with_cluster_control_plane;
pub use command_dispatch::dispatch_command_name;
pub use command_dispatch::dispatch_from_arg_slices;
pub use command_dispatch::dispatch_from_resp_args;
pub use command_spec::CommandId;
pub(crate) use connection_handler::build_owner_thread_pool;
pub(crate) use connection_handler::handle_connection;
pub use connection_handler::set_owner_execution_inline_default;
#[cfg(test)]
pub(crate) use connection_owner_routing::RoutedExecutionError;
#[cfg(test)]
pub(crate) use connection_owner_routing::capture_owned_frame_args;
#[cfg(test)]
pub(crate) use connection_owner_routing::execute_frame_via_processor;
#[cfg(test)]
pub(crate) use connection_owner_routing::execute_owned_args_via_processor;
#[cfg(test)]
pub(crate) use connection_owner_routing::execute_owned_frame_args_via_processor;
pub use limited_fixed_buffer_pool::LimitedFixedBufferPool;
pub use limited_fixed_buffer_pool::LimitedFixedBufferPoolConfig;
pub use limited_fixed_buffer_pool::LimitedFixedBufferPoolError;
pub use limited_fixed_buffer_pool::PoolEntry;
pub use limited_fixed_buffer_pool::ReturnStatus;
pub use request_lifecycle::MigrationEntry;
pub use request_lifecycle::MigrationValue;
pub use request_lifecycle::RequestExecutionError;
pub use request_lifecycle::RequestProcessor;
pub use request_lifecycle::RequestProcessorInitError;
pub use server_runtime::run;
pub use server_runtime::run_listener_with_shutdown;
pub use server_runtime::run_listener_with_shutdown_and_cluster;
pub use server_runtime::run_listener_with_shutdown_and_cluster_with_processor;
pub use server_runtime::run_with_cluster;
pub use server_runtime::run_with_shutdown;
pub use server_runtime::run_with_shutdown_and_cluster_config;
pub use shard_owner_threads::ShardOwnerThreadPool;
pub use shard_owner_threads::ShardOwnerThreadPoolError;

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

#[cfg(test)]
use garnet_common::ArgSlice;
#[cfg(test)]
use garnet_common::parse_resp_command_arg_slices;
#[cfg(test)]
use std::collections::HashSet;
#[cfg(test)]
use std::sync::Arc;
#[cfg(test)]
use tokio::net::TcpListener;
#[cfg(test)]
use tokio::net::TcpStream;

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
    next_client_id: AtomicU64,
    clients: Mutex<BTreeMap<u64, ClientRuntimeInfo>>,
}

#[derive(Debug, Clone)]
struct ClientRuntimeInfo {
    name: Option<Vec<u8>>,
    last_command: Vec<u8>,
    blocked: bool,
}

impl Default for ClientRuntimeInfo {
    fn default() -> Self {
        Self {
            name: None,
            last_command: b"unknown".to_vec(),
            blocked: false,
        }
    }
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

    pub fn register_client(&self) -> u64 {
        let id = self.next_client_id.fetch_add(1, Ordering::Relaxed) + 1;
        if let Ok(mut clients) = self.clients.lock() {
            clients.insert(id, ClientRuntimeInfo::default());
        }
        id
    }

    pub fn unregister_client(&self, client_id: u64) {
        if let Ok(mut clients) = self.clients.lock() {
            let _ = clients.remove(&client_id);
        }
    }

    pub fn set_client_name(&self, client_id: u64, name: Option<Vec<u8>>) {
        if let Ok(mut clients) = self.clients.lock() {
            if let Some(client) = clients.get_mut(&client_id) {
                client.name = name;
            }
        }
    }

    pub fn client_name(&self, client_id: u64) -> Option<Vec<u8>> {
        self.clients.lock().ok().and_then(|clients| {
            clients
                .get(&client_id)
                .and_then(|client| client.name.clone())
        })
    }

    pub fn set_client_last_command(&self, client_id: u64, command_name: &[u8]) {
        if let Ok(mut clients) = self.clients.lock() {
            if let Some(client) = clients.get_mut(&client_id) {
                client.last_command = command_name
                    .iter()
                    .map(|byte| byte.to_ascii_lowercase())
                    .collect();
            }
        }
    }

    pub fn set_client_blocked(&self, client_id: u64, blocked: bool) {
        if let Ok(mut clients) = self.clients.lock() {
            if let Some(client) = clients.get_mut(&client_id) {
                client.blocked = blocked;
            }
        }
    }

    pub fn is_client_blocked(&self, client_id: u64) -> bool {
        self.clients
            .lock()
            .ok()
            .and_then(|clients| clients.get(&client_id).map(|client| client.blocked))
            .unwrap_or(false)
    }

    pub fn render_client_list_payload(&self, filter_id: Option<u64>) -> Vec<u8> {
        let Ok(clients) = self.clients.lock() else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for (id, client) in clients.iter() {
            if let Some(expected_id) = filter_id {
                if *id != expected_id {
                    continue;
                }
            }
            if !out.is_empty() {
                out.push(b'\n');
            }
            out.extend_from_slice(b"id=");
            out.extend_from_slice(id.to_string().as_bytes());
            out.extend_from_slice(b" name=");
            if let Some(name) = client.name.as_ref() {
                out.extend_from_slice(name);
            }
            out.extend_from_slice(b" flags=");
            if client.blocked {
                out.extend_from_slice(b"b");
            } else {
                out.extend_from_slice(b"N");
            }
            out.extend_from_slice(b" cmd=");
            out.extend_from_slice(&client.last_command);
        }
        out
    }
}

#[cfg(test)]
mod tests;
