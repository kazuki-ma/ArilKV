//! TCP server accept loop and connection handler primitives.

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL_ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod aof_durability;
pub mod aof_replay;
mod cluster_control_plane;
mod cluster_live_view;
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
pub use request_lifecycle::ShardIndex;
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

use sha2::Digest;
use sha2::Sha256;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashSet;
use std::fmt;
use std::net::SocketAddr;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::broadcast;

use crate::request_lifecycle::DbName;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct ClientId(u64);

impl ClientId {
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }
}

impl fmt::Display for ClientId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for ClientId {
    fn from(value: u64) -> Self {
        Self::new(value)
    }
}

impl From<ClientId> for u64 {
    fn from(value: ClientId) -> Self {
        value.0
    }
}

#[derive(Debug)]
pub struct ServerMetrics {
    accepted_connections: AtomicU64,
    active_connections: AtomicU64,
    closed_connections: AtomicU64,
    bytes_received: AtomicU64,
    next_client_id: AtomicU64,
    clients: Mutex<BTreeMap<ClientId, ClientRuntimeInfo>>,
    reply_buffer_settings: Mutex<ReplyBufferSettings>,
    monitor_broadcast: broadcast::Sender<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub(crate) struct AclUserProfile {
    pub(crate) enabled: bool,
    pub(crate) nopass: bool,
    pub(crate) allow_all_commands: bool,
    pub(crate) allowed_categories: HashSet<Vec<u8>>,
    pub(crate) denied_categories: HashSet<Vec<u8>>,
    pub(crate) allowed_commands: HashSet<Vec<u8>>,
    pub(crate) denied_commands: HashSet<Vec<u8>>,
    pub(crate) key_patterns: Vec<Vec<u8>>,
    pub(crate) allow_all_channels: bool,
    pub(crate) channel_patterns: Vec<Vec<u8>>,
    pub(crate) allow_all_databases: bool,
    pub(crate) allowed_databases: BTreeSet<request_lifecycle::DbName>,
    pub(crate) password_hashes: HashSet<Vec<u8>>,
}

impl AclUserProfile {
    pub(crate) fn default_superuser() -> Self {
        Self {
            enabled: true,
            nopass: true,
            allow_all_commands: true,
            allowed_categories: HashSet::new(),
            denied_categories: HashSet::new(),
            allowed_commands: HashSet::new(),
            denied_commands: HashSet::new(),
            key_patterns: vec![b"*".to_vec()],
            allow_all_channels: true,
            channel_patterns: Vec::new(),
            allow_all_databases: true,
            allowed_databases: BTreeSet::new(),
            password_hashes: HashSet::new(),
        }
    }

    pub(crate) fn restricted() -> Self {
        Self {
            enabled: false,
            nopass: false,
            allow_all_commands: false,
            allowed_categories: HashSet::new(),
            denied_categories: HashSet::new(),
            allowed_commands: HashSet::new(),
            denied_commands: HashSet::new(),
            key_patterns: Vec::new(),
            allow_all_channels: false,
            channel_patterns: Vec::new(),
            allow_all_databases: true,
            allowed_databases: BTreeSet::new(),
            password_hashes: HashSet::new(),
        }
    }

    pub(crate) fn password_matches(&self, password: &[u8]) -> bool {
        let password_hash = acl_password_hash_hex(password);
        self.password_hashes.contains(password_hash.as_slice())
    }
}

pub(crate) fn acl_password_hash_hex(password: &[u8]) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(password);
    let digest = hasher.finalize();
    let mut out = Vec::with_capacity(64);
    for byte in digest {
        out.extend_from_slice(format!("{byte:02x}").as_bytes());
    }
    out
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
    /// Current bytes used in the client's query (receive) buffer.
    query_buffer_used: usize,
    /// Free bytes in the client's query (receive) buffer (capacity - len).
    query_buffer_free: usize,
    /// Logical allocation of the private query buffer surfaced via CLIENT LIST.
    query_buffer_capacity: usize,
    /// Allocated size of the client's read buffer.
    read_buffer_size: usize,
    /// Logical reply buffer size surfaced through CLIENT LIST rbs=.
    reply_buffer_size: usize,
    /// Peak reply bytes seen since the last reset window.
    reply_buffer_peak: usize,
    /// Last time the reply buffer peak window was reset.
    reply_buffer_peak_last_reset: Instant,
    wait_target_offset: u64,
    local_aof_wait_target_offset: u64,
    replica_listen_port: Option<u16>,
    selected_db: DbName,
    client_type: ClientTypeFilter,
}

const REPLY_BUFFER_MIN_BYTES: usize = 1024;
const REPLY_BUFFER_CHUNK_BYTES: usize = 16 * 1024;
const REPLY_BUFFER_DEFAULT_PEAK_RESET_TIME_MILLIS: u64 = 5_000;
const QUERY_BUFFER_REUSABLE_BYTES: usize = 16 * 1024;
const QUERY_BUFFER_IDLE_SHRINK_AFTER: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, Copy)]
struct ReplyBufferSettings {
    peak_reset_time: Option<Duration>,
    resizing_enabled: bool,
    copy_avoidance_enabled: bool,
}

impl Default for ReplyBufferSettings {
    fn default() -> Self {
        Self {
            peak_reset_time: Some(Duration::from_millis(
                REPLY_BUFFER_DEFAULT_PEAK_RESET_TIME_MILLIS,
            )),
            resizing_enabled: true,
            copy_avoidance_enabled: true,
        }
    }
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
    pub id: Option<ClientId>,
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
            query_buffer_used: 0,
            query_buffer_free: 0,
            query_buffer_capacity: 0,
            read_buffer_size: 0,
            reply_buffer_size: REPLY_BUFFER_CHUNK_BYTES,
            reply_buffer_peak: 0,
            reply_buffer_peak_last_reset: now,
            wait_target_offset: 0,
            local_aof_wait_target_offset: 0,
            replica_listen_port: None,
            selected_db: DbName::db0(),
            client_type: ClientTypeFilter::Normal,
        }
    }

    fn observe_query_buffer_activity(
        &mut self,
        observed_bytes_before_drain: usize,
        buffered_bytes_after_drain: usize,
        read_buffer_size: usize,
    ) {
        self.read_buffer_size = read_buffer_size;

        let observed_peak = observed_bytes_before_drain.max(buffered_bytes_after_drain);
        let should_use_private_buffer = buffered_bytes_after_drain > 0
            || observed_bytes_before_drain > QUERY_BUFFER_REUSABLE_BYTES;
        if should_use_private_buffer {
            let target_capacity = private_query_buffer_capacity_for(observed_peak);
            if target_capacity > self.query_buffer_capacity {
                self.query_buffer_capacity = target_capacity;
            }
        } else if self.query_buffer_capacity > 0 && observed_bytes_before_drain > 0 {
            let target_capacity = private_query_buffer_capacity_for(observed_peak);
            if target_capacity < self.query_buffer_capacity {
                self.query_buffer_capacity = target_capacity;
            }
        }

        if self.query_buffer_capacity == 0 {
            self.query_buffer_used = 0;
            self.query_buffer_free = 0;
            return;
        }

        self.query_buffer_used = buffered_bytes_after_drain;
        self.query_buffer_free = self
            .query_buffer_capacity
            .saturating_sub(buffered_bytes_after_drain);
    }

    fn apply_query_buffer_housekeeping(&mut self, now: Instant) {
        if self.query_buffer_capacity == 0 {
            self.query_buffer_used = 0;
            self.query_buffer_free = 0;
            return;
        }

        if now.duration_since(self.last_activity) >= QUERY_BUFFER_IDLE_SHRINK_AFTER {
            if self.query_buffer_used == 0 {
                self.query_buffer_capacity = 0;
                self.query_buffer_used = 0;
                self.query_buffer_free = 0;
                return;
            }

            self.query_buffer_capacity = private_query_buffer_capacity_for(self.query_buffer_used);
        }

        self.query_buffer_free = self
            .query_buffer_capacity
            .saturating_sub(self.query_buffer_used);
    }

    fn observe_reply_bytes(&mut self, bytes: usize, settings: ReplyBufferSettings, now: Instant) {
        if bytes == 0 {
            return;
        }

        let observed_bytes = bytes.max(1);
        self.reply_buffer_peak = self.reply_buffer_peak.max(observed_bytes);
        if !settings.resizing_enabled {
            return;
        }

        while self.reply_buffer_size < REPLY_BUFFER_CHUNK_BYTES
            && self.reply_buffer_peak >= self.reply_buffer_size
        {
            let next_size = self
                .reply_buffer_size
                .saturating_mul(2)
                .min(REPLY_BUFFER_CHUNK_BYTES);
            if next_size == self.reply_buffer_size {
                break;
            }
            self.reply_buffer_size = next_size;
        }

        self.reply_buffer_peak_last_reset = now;
    }

    fn apply_reply_buffer_housekeeping(
        &mut self,
        settings: ReplyBufferSettings,
        now: Instant,
    ) -> usize {
        if !settings.copy_avoidance_enabled {
            // Garnet always accounts copied replies; the flag only exists for
            // compatibility and does not change the logical rbs model.
        }

        if let Some(reset_interval) = settings.peak_reset_time
            && now.duration_since(self.reply_buffer_peak_last_reset) >= reset_interval
        {
            self.reply_buffer_peak = 0;
            self.reply_buffer_peak_last_reset = now;
        }

        if settings.resizing_enabled {
            let target_shrink_size = self.reply_buffer_size / 2;
            if target_shrink_size >= REPLY_BUFFER_MIN_BYTES
                && self.reply_buffer_peak < target_shrink_size
            {
                self.reply_buffer_size =
                    REPLY_BUFFER_MIN_BYTES.max(self.reply_buffer_peak.saturating_add(1));
            }
        }

        self.reply_buffer_size.max(REPLY_BUFFER_MIN_BYTES)
    }
}

impl Default for ServerMetrics {
    fn default() -> Self {
        Self {
            accepted_connections: AtomicU64::new(0),
            active_connections: AtomicU64::new(0),
            closed_connections: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            next_client_id: AtomicU64::new(0),
            clients: Mutex::new(BTreeMap::new()),
            reply_buffer_settings: Mutex::new(ReplyBufferSettings::default()),
            monitor_broadcast: broadcast::channel(4096).0,
        }
    }
}

fn private_query_buffer_capacity_for(observed_bytes: usize) -> usize {
    let required = observed_bytes.max(QUERY_BUFFER_REUSABLE_BYTES);
    required.checked_next_power_of_two().unwrap_or(required)
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
    ) -> ClientId {
        let id = ClientId::new(self.next_client_id.fetch_add(1, Ordering::Relaxed) + 1);
        if let Ok(mut clients) = self.clients.lock() {
            clients.insert(id, ClientRuntimeInfo::new(remote_addr, local_addr));
        }
        id
    }

    pub fn unregister_client(&self, client_id: ClientId) {
        if let Ok(mut clients) = self.clients.lock() {
            let _ = clients.remove(&client_id);
        }
    }

    pub fn set_client_name(&self, client_id: ClientId, name: Option<Vec<u8>>) {
        if let Ok(mut clients) = self.clients.lock()
            && let Some(client) = clients.get_mut(&client_id)
        {
            client.name = name;
            client.last_activity = Instant::now();
        }
    }

    pub fn client_name(&self, client_id: ClientId) -> Option<Vec<u8>> {
        self.clients.lock().ok().and_then(|clients| {
            clients
                .get(&client_id)
                .and_then(|client| client.name.clone())
        })
    }

    pub fn client_peer_id(&self, client_id: ClientId) -> Option<Vec<u8>> {
        self.clients
            .lock()
            .ok()
            .and_then(|clients| clients.get(&client_id).map(|client| client.addr.clone()))
    }

    pub fn set_client_last_command(
        &self,
        client_id: ClientId,
        command_name: &[u8],
        subcommand_name: Option<&[u8]>,
    ) {
        if let Ok(mut clients) = self.clients.lock()
            && let Some(client) = clients.get_mut(&client_id)
        {
            client.last_command = command_name
                .iter()
                .map(|byte| byte.to_ascii_lowercase())
                .collect();
            if let Some(subcommand) = subcommand_name
                && !subcommand.is_empty()
            {
                client.last_command.push(b'|');
                client.last_command.extend(
                    subcommand
                        .iter()
                        .map(|byte| byte.to_ascii_lowercase())
                        .collect::<Vec<u8>>(),
                );
            }
            client.last_activity = Instant::now();
        }
    }

    pub fn set_client_blocked(&self, client_id: ClientId, blocked: bool) {
        if let Ok(mut clients) = self.clients.lock()
            && let Some(client) = clients.get_mut(&client_id)
        {
            client.blocked = blocked;
            client.last_activity = Instant::now();
        }
    }

    pub fn is_client_blocked(&self, client_id: ClientId) -> bool {
        self.clients
            .lock()
            .ok()
            .and_then(|clients| clients.get(&client_id).map(|client| client.blocked))
            .unwrap_or(false)
    }

    pub fn set_client_user(&self, client_id: ClientId, user: Vec<u8>) {
        if let Ok(mut clients) = self.clients.lock()
            && let Some(client) = clients.get_mut(&client_id)
        {
            client.user = user;
            client.last_activity = Instant::now();
        }
    }

    pub fn client_user(&self, client_id: ClientId) -> Option<Vec<u8>> {
        self.clients
            .lock()
            .ok()
            .and_then(|clients| clients.get(&client_id).map(|client| client.user.clone()))
    }

    pub(crate) fn client_ids_for_user(&self, user: &[u8]) -> Vec<ClientId> {
        let Ok(clients) = self.clients.lock() else {
            return Vec::new();
        };
        clients
            .iter()
            .filter(|(_, client)| !client.killed && client.user.as_slice() == user)
            .map(|(client_id, _)| *client_id)
            .collect()
    }

    pub(crate) fn mark_clients_killed(&self, client_ids: &[ClientId]) {
        let Ok(mut clients) = self.clients.lock() else {
            return;
        };
        for client_id in client_ids {
            if let Some(client) = clients.get_mut(client_id) {
                client.killed = true;
            }
        }
    }

    pub fn set_client_library_name(&self, client_id: ClientId, value: Option<Vec<u8>>) {
        if let Ok(mut clients) = self.clients.lock()
            && let Some(client) = clients.get_mut(&client_id)
        {
            client.library_name = value;
            client.last_activity = Instant::now();
        }
    }

    pub fn set_client_library_version(&self, client_id: ClientId, value: Option<Vec<u8>>) {
        if let Ok(mut clients) = self.clients.lock()
            && let Some(client) = clients.get_mut(&client_id)
        {
            client.library_version = value;
            client.last_activity = Instant::now();
        }
    }

    pub fn add_client_input_bytes(&self, client_id: ClientId, bytes: u64) {
        if let Ok(mut clients) = self.clients.lock()
            && let Some(client) = clients.get_mut(&client_id)
        {
            client.total_input_bytes = client.total_input_bytes.saturating_add(bytes);
            client.last_activity = Instant::now();
        }
    }

    pub fn add_client_output_bytes(&self, client_id: ClientId, bytes: u64) {
        let now = Instant::now();
        let settings = self.reply_buffer_settings_snapshot();
        if let Ok(mut clients) = self.clients.lock()
            && let Some(client) = clients.get_mut(&client_id)
        {
            client.total_output_bytes = client.total_output_bytes.saturating_add(bytes);
            client.last_activity = now;
            client.observe_reply_bytes(bytes as usize, settings, now);
        }
    }

    pub fn update_client_buffer_info(
        &self,
        client_id: ClientId,
        observed_bytes_before_drain: usize,
        buffered_bytes_after_drain: usize,
        read_buffer_size: usize,
    ) {
        if let Ok(mut clients) = self.clients.lock()
            && let Some(client) = clients.get_mut(&client_id)
        {
            client.observe_query_buffer_activity(
                observed_bytes_before_drain,
                buffered_bytes_after_drain,
                read_buffer_size,
            );
        }
    }

    pub fn add_client_commands_processed(&self, client_id: ClientId, delta: u64) {
        if delta == 0 {
            return;
        }
        if let Ok(mut clients) = self.clients.lock()
            && let Some(client) = clients.get_mut(&client_id)
        {
            client.total_commands = client.total_commands.saturating_add(delta);
            client.last_activity = Instant::now();
        }
    }

    pub(crate) fn set_client_selected_db(&self, client_id: ClientId, selected_db: DbName) {
        if let Ok(mut clients) = self.clients.lock()
            && let Some(client) = clients.get_mut(&client_id)
        {
            client.selected_db = selected_db;
            client.last_activity = Instant::now();
        }
    }

    pub(crate) fn set_client_wait_target_offset(&self, client_id: ClientId, wait_target: u64) {
        if let Ok(mut clients) = self.clients.lock()
            && let Some(client) = clients.get_mut(&client_id)
        {
            client.wait_target_offset = wait_target;
            client.last_activity = Instant::now();
        }
    }

    pub(crate) fn client_wait_target_offset(&self, client_id: ClientId) -> Option<u64> {
        self.clients.lock().ok().and_then(|clients| {
            clients
                .get(&client_id)
                .map(|client| client.wait_target_offset)
        })
    }

    pub(crate) fn set_client_local_aof_wait_target_offset(
        &self,
        client_id: ClientId,
        wait_target: u64,
    ) {
        if let Ok(mut clients) = self.clients.lock()
            && let Some(client) = clients.get_mut(&client_id)
        {
            client.local_aof_wait_target_offset = wait_target;
            client.last_activity = Instant::now();
        }
    }

    pub(crate) fn client_local_aof_wait_target_offset(&self, client_id: ClientId) -> Option<u64> {
        self.clients.lock().ok().and_then(|clients| {
            clients
                .get(&client_id)
                .map(|client| client.local_aof_wait_target_offset)
        })
    }

    pub(crate) fn set_client_replica_listen_port(&self, client_id: ClientId, port: u16) {
        if let Ok(mut clients) = self.clients.lock()
            && let Some(client) = clients.get_mut(&client_id)
        {
            client.replica_listen_port = Some(port);
            client.last_activity = Instant::now();
        }
    }

    pub(crate) fn client_replica_listen_port(&self, client_id: ClientId) -> Option<u16> {
        self.clients.lock().ok().and_then(|clients| {
            clients
                .get(&client_id)
                .and_then(|client| client.replica_listen_port)
        })
    }

    pub(crate) fn set_client_type(&self, client_id: ClientId, client_type: ClientTypeFilter) {
        if let Ok(mut clients) = self.clients.lock()
            && let Some(client) = clients.get_mut(&client_id)
        {
            client.client_type = client_type;
            client.last_activity = Instant::now();
        }
    }

    pub(crate) fn replica_client_addrs(&self) -> Vec<Vec<u8>> {
        let Ok(clients) = self.clients.lock() else {
            return Vec::new();
        };
        clients
            .values()
            .filter(|client| !client.killed && client.client_type == ClientTypeFilter::Replica)
            .map(|client| {
                let Some(replica_listen_port) = client.replica_listen_port else {
                    return client.addr.clone();
                };
                let Ok(addr_text) = std::str::from_utf8(&client.addr) else {
                    return client.addr.clone();
                };
                let Ok(mut socket_addr) = addr_text.parse::<SocketAddr>() else {
                    return client.addr.clone();
                };
                socket_addr.set_port(replica_listen_port);
                socket_addr.to_string().into_bytes()
            })
            .collect()
    }

    pub fn connected_client_count(&self) -> u64 {
        let Ok(clients) = self.clients.lock() else {
            return 0;
        };
        clients.values().filter(|client| !client.killed).count() as u64
    }

    pub fn is_client_killed(&self, client_id: ClientId) -> bool {
        self.clients
            .lock()
            .ok()
            .and_then(|clients| clients.get(&client_id).map(|client| client.killed))
            .unwrap_or(false)
    }

    pub(crate) fn kill_clients(
        &self,
        current_client_id: ClientId,
        filter: &ClientKillFilter,
    ) -> Vec<ClientId> {
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
            if let Some(expected_type) = filter.client_type
                && client.client_type != expected_type
            {
                continue;
            }
            if let Some(expected_id) = filter.id
                && *client_id != expected_id
            {
                continue;
            }
            if let Some(expected_user) = filter.user.as_ref()
                && client.user.as_slice() != expected_user.as_slice()
            {
                continue;
            }
            if let Some(expected_addr) = filter.addr.as_ref()
                && client.addr.as_slice() != expected_addr.as_slice()
            {
                continue;
            }
            if let Some(expected_laddr) = filter.laddr.as_ref()
                && client.laddr.as_slice() != expected_laddr.as_slice()
            {
                continue;
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

    pub fn monitor_subscribe(&self) -> broadcast::Receiver<Vec<u8>> {
        self.monitor_broadcast.subscribe()
    }

    pub fn publish_monitor_event(&self, payload: Vec<u8>) {
        let _ = self.monitor_broadcast.send(payload);
    }

    pub fn render_client_info_payload(&self, client_id: ClientId) -> Option<Vec<u8>> {
        let Ok(mut clients) = self.clients.lock() else {
            return None;
        };
        let now = Instant::now();
        let settings = self.reply_buffer_settings_snapshot();
        let client = clients.get_mut(&client_id)?;
        if client.killed {
            return None;
        }
        client.apply_query_buffer_housekeeping(now);
        let reply_buffer_size = client.apply_reply_buffer_housekeeping(settings, now);
        Some(render_client_line(
            client_id,
            client,
            now,
            client.selected_db,
            reply_buffer_size,
        ))
    }

    pub fn render_client_list_payload(&self, filter_id: Option<ClientId>) -> Vec<u8> {
        let Ok(mut clients) = self.clients.lock() else {
            return Vec::new();
        };
        let mut out = Vec::new();
        let now = Instant::now();
        let settings = self.reply_buffer_settings_snapshot();
        for (id, client) in clients.iter_mut() {
            if client.killed {
                continue;
            }
            if let Some(expected_id) = filter_id
                && *id != expected_id
            {
                continue;
            }
            if !out.is_empty() {
                out.extend_from_slice(b"\r\n");
            }
            client.apply_query_buffer_housekeeping(now);
            let reply_buffer_size = client.apply_reply_buffer_housekeeping(settings, now);
            out.extend_from_slice(&render_client_line(
                *id,
                client,
                now,
                client.selected_db,
                reply_buffer_size,
            ));
        }
        out
    }

    pub fn set_reply_buffer_peak_reset_time_millis(&self, millis: Option<u64>) {
        if let Ok(mut settings) = self.reply_buffer_settings.lock() {
            settings.peak_reset_time = millis.map(Duration::from_millis);
        }
    }

    pub fn reset_reply_buffer_peak_reset_time(&self) {
        self.set_reply_buffer_peak_reset_time_millis(Some(
            REPLY_BUFFER_DEFAULT_PEAK_RESET_TIME_MILLIS,
        ));
    }

    pub fn set_reply_buffer_resizing_enabled(&self, enabled: bool) {
        if let Ok(mut settings) = self.reply_buffer_settings.lock() {
            settings.resizing_enabled = enabled;
        }
    }

    pub fn set_reply_copy_avoidance_enabled(&self, enabled: bool) {
        if let Ok(mut settings) = self.reply_buffer_settings.lock() {
            settings.copy_avoidance_enabled = enabled;
        }
    }

    fn reply_buffer_settings_snapshot(&self) -> ReplyBufferSettings {
        self.reply_buffer_settings
            .lock()
            .map(|settings| *settings)
            .unwrap_or_default()
    }
}

fn render_client_line(
    client_id: ClientId,
    client: &ClientRuntimeInfo,
    now: Instant,
    selected_db: DbName,
    reply_buffer_size: usize,
) -> Vec<u8> {
    let age_seconds = now.duration_since(client.connect_time).as_secs();
    let idle_seconds = now.duration_since(client.last_activity).as_secs();
    let mut flags = String::new();
    if client.blocked {
        flags.push('b');
    }
    match client.client_type {
        ClientTypeFilter::Normal => {}
        ClientTypeFilter::Master => flags.push('M'),
        ClientTypeFilter::Replica => flags.push('S'),
        ClientTypeFilter::Pubsub => flags.push('P'),
    }
    if flags.is_empty() {
        flags.push('N');
    }
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
    let qbuf = client.query_buffer_used;
    let qbuf_free = client.query_buffer_free;
    // Approximate total client memory: query buffer allocation + read buffer + base overhead.
    let query_buffer_alloc = qbuf + qbuf_free;
    let tot_mem = query_buffer_alloc + reply_buffer_size + 20480;
    format!(
        "id={} addr={} laddr={} fd=8 name={} age={} idle={} flags={} db={} sub=0 psub=0 ssub=0 multi=-1 watch=0 qbuf={} qbuf-free={} argv-mem=0 multi-mem=0 rbs={} rbp=0 obl=0 oll=0 omem=0 tot-mem={} events=r cmd={} user={} redir=-1 resp=3 lib-name={} lib-ver={} io-thread=0 tot-net-in={} tot-net-out={} tot-cmds={}",
        u64::from(client_id),
        String::from_utf8_lossy(&client.addr),
        String::from_utf8_lossy(&client.laddr),
        name,
        age_seconds,
        idle_seconds,
        flags,
        usize::from(selected_db),
        qbuf,
        qbuf_free,
        reply_buffer_size,
        tot_mem,
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
