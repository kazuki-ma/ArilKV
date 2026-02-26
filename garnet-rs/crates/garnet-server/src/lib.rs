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
pub use request_lifecycle::ItemKey;
pub use request_lifecycle::MigrationEntry;
pub use request_lifecycle::MigrationValue;
pub use request_lifecycle::RequestExecutionError;
pub use request_lifecycle::RequestProcessor;
pub use request_lifecycle::RequestProcessorInitError;
pub use request_lifecycle::StringValue;
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
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Instant;
use tokio::sync::broadcast;

#[cfg(test)]
use garnet_common::ArgSlice;
#[cfg(test)]
use garnet_common::parse_resp_command_arg_slices;
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

#[derive(Debug)]
pub struct ServerMetrics {
    accepted_connections: AtomicU64,
    active_connections: AtomicU64,
    closed_connections: AtomicU64,
    bytes_received: AtomicU64,
    next_client_id: AtomicU64,
    clients: Mutex<BTreeMap<u64, ClientRuntimeInfo>>,
    acl_users: Mutex<HashSet<Vec<u8>>>,
    monitor_broadcast: broadcast::Sender<Vec<u8>>,
}

#[derive(Debug, Clone)]
struct ClientRuntimeInfo {
    name: Option<Vec<u8>>,
    user: Vec<u8>,
    library_name: Option<Vec<u8>>,
    library_version: Option<Vec<u8>>,
    last_command: Vec<u8>,
    blocked: bool,
    killed: bool,
    connect_time: Instant,
    last_activity: Instant,
    addr: Vec<u8>,
    laddr: Vec<u8>,
    total_input_bytes: u64,
    total_output_bytes: u64,
    total_commands: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClientTypeFilter {
    Normal,
    Master,
    Replica,
    Pubsub,
}

#[derive(Debug, Clone)]
pub(crate) struct ClientKillFilter {
    pub id: Option<u64>,
    pub user: Option<Vec<u8>>,
    pub addr: Option<Vec<u8>>,
    pub laddr: Option<Vec<u8>>,
    pub max_age_seconds: Option<u64>,
    pub skip_current_connection: bool,
    pub client_type: Option<ClientTypeFilter>,
}

impl Default for ClientKillFilter {
    fn default() -> Self {
        Self {
            id: None,
            user: None,
            addr: None,
            laddr: None,
            max_age_seconds: None,
            skip_current_connection: true,
            client_type: None,
        }
    }
}

impl ClientRuntimeInfo {
    fn new(remote_addr: Option<SocketAddr>, local_addr: Option<SocketAddr>) -> Self {
        let now = Instant::now();
        let addr = remote_addr
            .map(|entry| entry.to_string().into_bytes())
            .unwrap_or_else(|| b"127.0.0.1:0".to_vec());
        let laddr = local_addr
            .map(|entry| entry.to_string().into_bytes())
            .unwrap_or_else(|| b"127.0.0.1:0".to_vec());
        Self {
            name: None,
            user: b"default".to_vec(),
            library_name: None,
            library_version: None,
            last_command: b"unknown".to_vec(),
            blocked: false,
            killed: false,
            connect_time: now,
            last_activity: now,
            addr,
            laddr,
            total_input_bytes: 0,
            total_output_bytes: 0,
            total_commands: 0,
        }
    }
}

impl Default for ServerMetrics {
    fn default() -> Self {
        let mut acl_users = HashSet::new();
        acl_users.insert(b"default".to_vec());
        Self {
            accepted_connections: AtomicU64::new(0),
            active_connections: AtomicU64::new(0),
            closed_connections: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            next_client_id: AtomicU64::new(0),
            clients: Mutex::new(BTreeMap::new()),
            acl_users: Mutex::new(acl_users),
            monitor_broadcast: broadcast::channel(4096).0,
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

    pub fn register_client(
        &self,
        remote_addr: Option<SocketAddr>,
        local_addr: Option<SocketAddr>,
    ) -> u64 {
        let id = self.next_client_id.fetch_add(1, Ordering::Relaxed) + 1;
        if let Ok(mut clients) = self.clients.lock() {
            clients.insert(id, ClientRuntimeInfo::new(remote_addr, local_addr));
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
                client.last_activity = Instant::now();
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

    pub fn set_client_last_command(
        &self,
        client_id: u64,
        command_name: &[u8],
        subcommand_name: Option<&[u8]>,
    ) {
        if let Ok(mut clients) = self.clients.lock() {
            if let Some(client) = clients.get_mut(&client_id) {
                client.last_command = command_name
                    .iter()
                    .map(|byte| byte.to_ascii_lowercase())
                    .collect();
                if let Some(subcommand) = subcommand_name {
                    if !subcommand.is_empty() {
                        client.last_command.push(b'|');
                        client.last_command.extend(
                            subcommand
                                .iter()
                                .map(|byte| byte.to_ascii_lowercase())
                                .collect::<Vec<u8>>(),
                        );
                    }
                }
                client.last_activity = Instant::now();
            }
        }
    }

    pub fn set_client_blocked(&self, client_id: u64, blocked: bool) {
        if let Ok(mut clients) = self.clients.lock() {
            if let Some(client) = clients.get_mut(&client_id) {
                client.blocked = blocked;
                client.last_activity = Instant::now();
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

    pub fn set_client_user(&self, client_id: u64, user: Vec<u8>) {
        if let Ok(mut clients) = self.clients.lock() {
            if let Some(client) = clients.get_mut(&client_id) {
                client.user = user;
                client.last_activity = Instant::now();
            }
        }
    }

    pub fn client_user(&self, client_id: u64) -> Option<Vec<u8>> {
        self.clients.lock().ok().and_then(|clients| {
            clients
                .get(&client_id)
                .and_then(|client| Some(client.user.clone()))
        })
    }

    pub fn set_client_library_name(&self, client_id: u64, value: Option<Vec<u8>>) {
        if let Ok(mut clients) = self.clients.lock() {
            if let Some(client) = clients.get_mut(&client_id) {
                client.library_name = value;
                client.last_activity = Instant::now();
            }
        }
    }

    pub fn set_client_library_version(&self, client_id: u64, value: Option<Vec<u8>>) {
        if let Ok(mut clients) = self.clients.lock() {
            if let Some(client) = clients.get_mut(&client_id) {
                client.library_version = value;
                client.last_activity = Instant::now();
            }
        }
    }

    pub fn add_client_input_bytes(&self, client_id: u64, bytes: u64) {
        if let Ok(mut clients) = self.clients.lock() {
            if let Some(client) = clients.get_mut(&client_id) {
                client.total_input_bytes = client.total_input_bytes.saturating_add(bytes);
                client.last_activity = Instant::now();
            }
        }
    }

    pub fn add_client_output_bytes(&self, client_id: u64, bytes: u64) {
        if let Ok(mut clients) = self.clients.lock() {
            if let Some(client) = clients.get_mut(&client_id) {
                client.total_output_bytes = client.total_output_bytes.saturating_add(bytes);
                client.last_activity = Instant::now();
            }
        }
    }

    pub fn add_client_commands_processed(&self, client_id: u64, delta: u64) {
        if delta == 0 {
            return;
        }
        if let Ok(mut clients) = self.clients.lock() {
            if let Some(client) = clients.get_mut(&client_id) {
                client.total_commands = client.total_commands.saturating_add(delta);
                client.last_activity = Instant::now();
            }
        }
    }

    pub fn connected_client_count(&self) -> u64 {
        let Ok(clients) = self.clients.lock() else {
            return 0;
        };
        clients.values().filter(|client| !client.killed).count() as u64
    }

    pub fn is_client_killed(&self, client_id: u64) -> bool {
        self.clients
            .lock()
            .ok()
            .and_then(|clients| clients.get(&client_id).map(|client| client.killed))
            .unwrap_or(false)
    }

    pub(crate) fn kill_clients(
        &self,
        current_client_id: u64,
        filter: &ClientKillFilter,
    ) -> Vec<u64> {
        let Ok(mut clients) = self.clients.lock() else {
            return Vec::new();
        };
        let now = Instant::now();
        let mut killed = Vec::new();
        for (client_id, client) in clients.iter_mut() {
            if client.killed {
                continue;
            }
            if filter.skip_current_connection && *client_id == current_client_id {
                continue;
            }
            if let Some(expected_type) = filter.client_type {
                if expected_type != ClientTypeFilter::Normal {
                    continue;
                }
            }
            if let Some(expected_id) = filter.id {
                if *client_id != expected_id {
                    continue;
                }
            }
            if let Some(expected_user) = filter.user.as_ref() {
                if client.user.as_slice() != expected_user.as_slice() {
                    continue;
                }
            }
            if let Some(expected_addr) = filter.addr.as_ref() {
                if client.addr.as_slice() != expected_addr.as_slice() {
                    continue;
                }
            }
            if let Some(expected_laddr) = filter.laddr.as_ref() {
                if client.laddr.as_slice() != expected_laddr.as_slice() {
                    continue;
                }
            }
            if let Some(max_age_seconds) = filter.max_age_seconds {
                let age_seconds = now.duration_since(client.connect_time).as_secs();
                if age_seconds < max_age_seconds {
                    continue;
                }
            }
            client.killed = true;
            killed.push(*client_id);
        }
        killed
    }

    pub fn acl_user_exists(&self, user: &[u8]) -> bool {
        self.acl_users
            .lock()
            .map(|users| users.contains(user))
            .unwrap_or(false)
    }

    pub fn register_acl_user(&self, user: &[u8]) {
        if user.is_empty() {
            return;
        }
        if let Ok(mut users) = self.acl_users.lock() {
            users.insert(user.to_vec());
        }
    }

    pub fn monitor_subscribe(&self) -> broadcast::Receiver<Vec<u8>> {
        self.monitor_broadcast.subscribe()
    }

    pub fn publish_monitor_event(&self, payload: Vec<u8>) {
        let _ = self.monitor_broadcast.send(payload);
    }

    pub fn render_client_info_payload(&self, client_id: u64) -> Option<Vec<u8>> {
        let Ok(clients) = self.clients.lock() else {
            return None;
        };
        let now = Instant::now();
        let (_, client) = clients.get_key_value(&client_id)?;
        if client.killed {
            return None;
        }
        Some(render_client_line(client_id, client, now))
    }

    pub fn render_client_list_payload(&self, filter_id: Option<u64>) -> Vec<u8> {
        let Ok(clients) = self.clients.lock() else {
            return Vec::new();
        };
        let mut out = Vec::new();
        let now = Instant::now();
        for (id, client) in clients.iter() {
            if client.killed {
                continue;
            }
            if let Some(expected_id) = filter_id {
                if *id != expected_id {
                    continue;
                }
            }
            if !out.is_empty() {
                out.extend_from_slice(b"\r\n");
            }
            out.extend_from_slice(&render_client_line(*id, client, now));
        }
        out
    }
}

fn render_client_line(client_id: u64, client: &ClientRuntimeInfo, now: Instant) -> Vec<u8> {
    let age_seconds = now.duration_since(client.connect_time).as_secs();
    let idle_seconds = now.duration_since(client.last_activity).as_secs();
    let flags = if client.blocked { "b" } else { "N" };
    let name = client
        .name
        .as_ref()
        .map(|value| String::from_utf8_lossy(value).to_string())
        .unwrap_or_default();
    let user = String::from_utf8_lossy(&client.user);
    let command = String::from_utf8_lossy(&client.last_command);
    let library_name = client
        .library_name
        .as_ref()
        .map(|value| String::from_utf8_lossy(value).to_string())
        .unwrap_or_default();
    let library_version = client
        .library_version
        .as_ref()
        .map(|value| String::from_utf8_lossy(value).to_string())
        .unwrap_or_default();
    format!(
        "id={} addr={} laddr={} fd=8 name={} age={} idle={} flags={} db=0 sub=0 psub=0 ssub=0 multi=-1 watch=0 qbuf=0 qbuf-free=20474 argv-mem=0 multi-mem=0 rbs=16384 rbp=0 obl=0 oll=0 omem=0 tot-mem=20480 events=r cmd={} user={} redir=-1 resp=3 lib-name={} lib-ver={} io-thread=0 tot-net-in={} tot-net-out={} tot-cmds={}",
        client_id,
        String::from_utf8_lossy(&client.addr),
        String::from_utf8_lossy(&client.laddr),
        name,
        age_seconds,
        idle_seconds,
        flags,
        command,
        user,
        library_name,
        library_version,
        client.total_input_bytes,
        client.total_output_bytes,
        client.total_commands
    )
    .into_bytes()
}

#[cfg(test)]
mod tests;
