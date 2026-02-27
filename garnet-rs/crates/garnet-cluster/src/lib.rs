//! Cluster configuration primitives for garnet-rs.
//!
//! This module provides immutable-style copy-on-write cluster configuration
//! structures with Redis-compatible 16,384 hash slots.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;
use std::future::Future;
use std::io::Write;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;

pub const HASH_SLOT_COUNT: usize = 16_384;
pub const RESERVED_WORKER_ID: WorkerId = WorkerId::new(0);
pub const LOCAL_WORKER_ID: WorkerId = WorkerId::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct WorkerId(u16);

impl WorkerId {
    pub const fn new(raw: u16) -> Self {
        Self(raw)
    }

    pub const fn as_u16(self) -> u16 {
        self.0
    }

    pub const fn as_usize(self) -> usize {
        self.0 as usize
    }

    pub fn from_usize(value: usize) -> Option<Self> {
        u16::try_from(value).ok().map(Self)
    }
}

impl fmt::Display for WorkerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u16> for WorkerId {
    fn from(value: u16) -> Self {
        Self::new(value)
    }
}

impl From<WorkerId> for u16 {
    fn from(value: WorkerId) -> Self {
        value.as_u16()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct SlotNumber(u16);

impl SlotNumber {
    pub const fn new(raw: u16) -> Self {
        Self(raw)
    }

    pub fn from_usize(value: usize) -> Option<Self> {
        u16::try_from(value).ok().map(Self)
    }
}

impl fmt::Display for SlotNumber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u16> for SlotNumber {
    fn from(value: u16) -> Self {
        Self::new(value)
    }
}

impl From<SlotNumber> for u16 {
    fn from(value: SlotNumber) -> Self {
        value.0
    }
}

impl From<SlotNumber> for usize {
    fn from(value: SlotNumber) -> Self {
        usize::from(value.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct ConfigEpoch(u64);

impl ConfigEpoch {
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }
}

impl fmt::Display for ConfigEpoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for ConfigEpoch {
    fn from(value: u64) -> Self {
        Self::new(value)
    }
}

impl From<ConfigEpoch> for u64 {
    fn from(value: ConfigEpoch) -> Self {
        value.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct ReplicationOffset(u64);

impl ReplicationOffset {
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }
}

impl fmt::Display for ReplicationOffset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for ReplicationOffset {
    fn from(value: u64) -> Self {
        Self::new(value)
    }
}

impl From<ReplicationOffset> for u64 {
    fn from(value: ReplicationOffset) -> Self {
        value.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct CheckpointId(u64);

impl CheckpointId {
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }
}

impl fmt::Display for CheckpointId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for CheckpointId {
    fn from(value: u64) -> Self {
        Self::new(value)
    }
}

impl From<CheckpointId> for u64 {
    fn from(value: CheckpointId) -> Self {
        value.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct GossipTick(u64);

impl GossipTick {
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    pub fn increment(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

impl fmt::Display for GossipTick {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for GossipTick {
    fn from(value: u64) -> Self {
        Self::new(value)
    }
}

impl From<GossipTick> for u64 {
    fn from(value: GossipTick) -> Self {
        value.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SlotState {
    Offline = 0,
    Stable = 1,
    Migrating = 2,
    Importing = 3,
    Fail = 4,
    Node = 5,
    Invalid = 6,
}

impl SlotState {
    fn from_u8(raw: u8) -> Self {
        match raw {
            0 => Self::Offline,
            1 => Self::Stable,
            2 => Self::Migrating,
            3 => Self::Importing,
            4 => Self::Fail,
            5 => Self::Node,
            _ => Self::Invalid,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HashSlot {
    bytes: [u8; 3],
}

impl HashSlot {
    pub fn new(worker_id: WorkerId, state: SlotState) -> Self {
        let [lo, hi] = worker_id.as_u16().to_le_bytes();
        Self {
            bytes: [lo, hi, state as u8],
        }
    }

    pub fn assigned_worker_id(self) -> WorkerId {
        WorkerId::new(u16::from_le_bytes([self.bytes[0], self.bytes[1]]))
    }

    pub fn worker_id(self) -> WorkerId {
        if self.state() == SlotState::Migrating {
            LOCAL_WORKER_ID
        } else {
            self.assigned_worker_id()
        }
    }

    pub fn state(self) -> SlotState {
        SlotState::from_u8(self.bytes[2])
    }

    pub fn is_local(self) -> bool {
        self.worker_id() == LOCAL_WORKER_ID
    }
}

impl Default for HashSlot {
    fn default() -> Self {
        Self::new(RESERVED_WORKER_ID, SlotState::Offline)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WorkerRole {
    #[default]
    Unassigned,
    Primary,
    Replica,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Worker {
    pub id: WorkerId,
    pub node_id: String,
    pub host: String,
    pub port: u16,
    pub config_epoch: ConfigEpoch,
    pub role: WorkerRole,
    pub replica_of_node_id: Option<String>,
    pub replication_offset: ReplicationOffset,
}

impl Worker {
    pub fn new(
        node_id: impl Into<String>,
        host: impl Into<String>,
        port: u16,
        role: WorkerRole,
    ) -> Self {
        Self {
            id: RESERVED_WORKER_ID,
            node_id: node_id.into(),
            host: host.into(),
            port,
            config_epoch: ConfigEpoch::new(0),
            role,
            replica_of_node_id: None,
            replication_offset: ReplicationOffset::new(0),
        }
    }

    fn reserved() -> Self {
        Self {
            id: RESERVED_WORKER_ID,
            node_id: String::new(),
            host: String::new(),
            port: 0,
            config_epoch: ConfigEpoch::new(0),
            role: WorkerRole::Unassigned,
            replica_of_node_id: None,
            replication_offset: ReplicationOffset::new(0),
        }
    }

    pub fn endpoint(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterConfigError {
    InvalidSlot(SlotNumber),
    WorkerNotFound(WorkerId),
    WorkerCapacityExceeded,
    MissingLocalWorker,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotRouteDecision {
    Local,
    Moved {
        slot: SlotNumber,
        worker_id: WorkerId,
    },
    Ask {
        slot: SlotNumber,
        worker_id: WorkerId,
    },
    Unbound {
        slot: SlotNumber,
    },
}

impl fmt::Display for ClusterConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSlot(slot) => write!(f, "invalid hash slot: {slot}"),
            Self::WorkerNotFound(worker_id) => write!(f, "worker not found: {worker_id}"),
            Self::WorkerCapacityExceeded => write!(f, "worker capacity exceeded"),
            Self::MissingLocalWorker => write!(f, "local worker is missing"),
        }
    }
}

impl std::error::Error for ClusterConfigError {}

#[derive(Debug, Clone)]
pub struct ClusterConfig {
    slot_map: Box<[HashSlot; HASH_SLOT_COUNT]>,
    workers: Vec<Worker>,
}

impl ClusterConfig {
    pub fn new(local_worker: Worker) -> Self {
        let mut workers = Vec::with_capacity(2);
        workers.push(Worker::reserved());
        let mut local = local_worker;
        local.id = LOCAL_WORKER_ID;
        workers.push(local);
        Self {
            slot_map: Box::new([HashSlot::default(); HASH_SLOT_COUNT]),
            workers,
        }
    }

    pub fn new_local(node_id: impl Into<String>, host: impl Into<String>, port: u16) -> Self {
        Self::new(Worker::new(node_id, host, port, WorkerRole::Primary))
    }

    pub fn workers(&self) -> &[Worker] {
        &self.workers
    }

    pub fn worker(&self, worker_id: WorkerId) -> Option<&Worker> {
        self.workers.get(worker_id.as_usize())
    }

    pub fn local_worker(&self) -> Result<&Worker, ClusterConfigError> {
        self.worker(LOCAL_WORKER_ID)
            .ok_or(ClusterConfigError::MissingLocalWorker)
    }

    pub fn slot(&self, slot: SlotNumber) -> Result<HashSlot, ClusterConfigError> {
        Ok(self.slot_map[Self::slot_index(slot)?])
    }

    pub fn slot_state(&self, slot: SlotNumber) -> Result<SlotState, ClusterConfigError> {
        Ok(self.slot(slot)?.state())
    }

    pub fn slot_owner(&self, slot: SlotNumber) -> Result<WorkerId, ClusterConfigError> {
        Ok(self.slot(slot)?.worker_id())
    }

    pub fn slot_assigned_owner(&self, slot: SlotNumber) -> Result<WorkerId, ClusterConfigError> {
        Ok(self.slot(slot)?.assigned_worker_id())
    }

    pub fn slots_in_state(&self, state: SlotState) -> Vec<SlotNumber> {
        self.slot_map
            .iter()
            .enumerate()
            .filter_map(|(slot, entry)| {
                (entry.state() == state)
                    .then_some(slot)
                    .and_then(SlotNumber::from_usize)
            })
            .collect()
    }

    pub fn slots_assigned_to_worker_in_state(
        &self,
        worker_id: WorkerId,
        state: SlotState,
    ) -> Result<Vec<SlotNumber>, ClusterConfigError> {
        if self.worker(worker_id).is_none() {
            return Err(ClusterConfigError::WorkerNotFound(worker_id));
        }
        Ok(self
            .slot_map
            .iter()
            .enumerate()
            .filter_map(|(slot, entry)| {
                (entry.assigned_worker_id() == worker_id && entry.state() == state)
                    .then_some(slot)
                    .and_then(SlotNumber::from_usize)
            })
            .collect())
    }

    pub fn is_local_slot(&self, slot: SlotNumber) -> Result<bool, ClusterConfigError> {
        Ok(self.slot(slot)?.is_local())
    }

    pub fn route_for_slot(
        &self,
        slot: SlotNumber,
    ) -> Result<SlotRouteDecision, ClusterConfigError> {
        let entry = self.slot(slot)?;
        let assigned_owner = entry.assigned_worker_id();
        let decision = match entry.state() {
            SlotState::Stable => {
                if entry.is_local() {
                    SlotRouteDecision::Local
                } else {
                    SlotRouteDecision::Moved {
                        slot,
                        worker_id: assigned_owner,
                    }
                }
            }
            SlotState::Migrating | SlotState::Importing => {
                if assigned_owner == LOCAL_WORKER_ID {
                    SlotRouteDecision::Local
                } else {
                    SlotRouteDecision::Ask {
                        slot,
                        worker_id: assigned_owner,
                    }
                }
            }
            SlotState::Offline | SlotState::Fail | SlotState::Invalid => {
                SlotRouteDecision::Unbound { slot }
            }
            SlotState::Node => {
                if assigned_owner == LOCAL_WORKER_ID {
                    SlotRouteDecision::Local
                } else {
                    SlotRouteDecision::Moved {
                        slot,
                        worker_id: assigned_owner,
                    }
                }
            }
        };
        Ok(decision)
    }

    pub fn route_for_key(&self, key: &[u8]) -> Result<SlotRouteDecision, ClusterConfigError> {
        let slot = redis_hash_slot(key);
        self.route_for_slot(slot)
    }

    pub fn redirection_error_for_slot(
        &self,
        slot: SlotNumber,
    ) -> Result<Option<String>, ClusterConfigError> {
        match self.route_for_slot(slot)? {
            SlotRouteDecision::Local => Ok(None),
            SlotRouteDecision::Moved { slot, worker_id } => {
                let endpoint = self.worker_endpoint(worker_id)?;
                Ok(Some(format!("MOVED {slot} {endpoint}")))
            }
            SlotRouteDecision::Ask { slot, worker_id } => {
                let endpoint = self.worker_endpoint(worker_id)?;
                Ok(Some(format!("ASK {slot} {endpoint}")))
            }
            SlotRouteDecision::Unbound { slot } => {
                Ok(Some(format!("CLUSTERDOWN Hash slot {slot} is unbound")))
            }
        }
    }

    pub fn redirection_error_for_key(
        &self,
        key: &[u8],
    ) -> Result<Option<String>, ClusterConfigError> {
        let slot = redis_hash_slot(key);
        self.redirection_error_for_slot(slot)
    }

    pub fn set_slot_state(
        &self,
        slot: SlotNumber,
        worker_id: WorkerId,
        state: SlotState,
    ) -> Result<Self, ClusterConfigError> {
        let slot_index = Self::slot_index(slot)?;
        if self.worker(worker_id).is_none() {
            return Err(ClusterConfigError::WorkerNotFound(worker_id));
        }
        let mut next = self.clone();
        next.slot_map[slot_index] = HashSlot::new(worker_id, state);
        Ok(next)
    }

    pub fn begin_slot_migration_to(
        &self,
        slot: SlotNumber,
        target_worker_id: WorkerId,
    ) -> Result<Self, ClusterConfigError> {
        self.set_slot_state(slot, target_worker_id, SlotState::Migrating)
    }

    pub fn begin_slot_import_from(
        &self,
        slot: SlotNumber,
        source_worker_id: WorkerId,
    ) -> Result<Self, ClusterConfigError> {
        self.set_slot_state(slot, source_worker_id, SlotState::Importing)
    }

    pub fn finalize_slot_migration(
        &self,
        slot: SlotNumber,
        new_owner_worker_id: WorkerId,
    ) -> Result<Self, ClusterConfigError> {
        self.set_slot_state(slot, new_owner_worker_id, SlotState::Stable)
    }

    pub fn set_local_worker_role(&self, role: WorkerRole) -> Result<Self, ClusterConfigError> {
        let mut next = self.clone();
        let local = next
            .workers
            .get_mut(LOCAL_WORKER_ID.as_usize())
            .ok_or(ClusterConfigError::MissingLocalWorker)?;
        local.role = role;
        if role != WorkerRole::Replica {
            local.replica_of_node_id = None;
        }
        Ok(next)
    }

    pub fn set_worker_config_epoch(
        &self,
        worker_id: WorkerId,
        config_epoch: ConfigEpoch,
    ) -> Result<Self, ClusterConfigError> {
        let mut next = self.clone();
        let worker = next
            .workers
            .get_mut(worker_id.as_usize())
            .ok_or(ClusterConfigError::WorkerNotFound(worker_id))?;
        worker.config_epoch = config_epoch;
        Ok(next)
    }

    pub fn set_worker_replica_of(
        &self,
        worker_id: WorkerId,
        primary_node_id: impl Into<String>,
    ) -> Result<Self, ClusterConfigError> {
        let mut next = self.clone();
        let worker = next
            .workers
            .get_mut(worker_id.as_usize())
            .ok_or(ClusterConfigError::WorkerNotFound(worker_id))?;
        worker.role = WorkerRole::Replica;
        worker.replica_of_node_id = Some(primary_node_id.into());
        Ok(next)
    }

    pub fn set_worker_replication_offset(
        &self,
        worker_id: WorkerId,
        replication_offset: ReplicationOffset,
    ) -> Result<Self, ClusterConfigError> {
        let mut next = self.clone();
        let worker = next
            .workers
            .get_mut(worker_id.as_usize())
            .ok_or(ClusterConfigError::WorkerNotFound(worker_id))?;
        worker.replication_offset = replication_offset;
        Ok(next)
    }

    pub fn add_worker(&self, mut worker: Worker) -> Result<(Self, WorkerId), ClusterConfigError> {
        let worker_id = WorkerId::from_usize(self.workers.len())
            .ok_or(ClusterConfigError::WorkerCapacityExceeded)?;
        let mut next = self.clone();
        worker.id = worker_id;
        next.workers.push(worker);
        Ok((next, worker_id))
    }

    pub fn take_over_slots_from_primary(
        &self,
        failed_primary_worker_id: WorkerId,
    ) -> Result<Self, ClusterConfigError> {
        if self.worker(failed_primary_worker_id).is_none() {
            return Err(ClusterConfigError::WorkerNotFound(failed_primary_worker_id));
        }

        let mut next = self.clone();
        for slot in next.slot_map.iter_mut() {
            if slot.assigned_worker_id() == failed_primary_worker_id {
                *slot = HashSlot::new(LOCAL_WORKER_ID, SlotState::Stable);
            }
        }

        let local = next
            .workers
            .get_mut(LOCAL_WORKER_ID.as_usize())
            .ok_or(ClusterConfigError::MissingLocalWorker)?;
        local.role = WorkerRole::Primary;
        local.replica_of_node_id = None;
        Ok(next)
    }

    pub fn apply_failover_plan(&self, plan: &FailoverPlan) -> Result<Self, ClusterConfigError> {
        if self.worker(plan.failed_primary_worker_id).is_none() {
            return Err(ClusterConfigError::WorkerNotFound(
                plan.failed_primary_worker_id,
            ));
        }
        if self.worker(plan.promoted_worker_id).is_none() {
            return Err(ClusterConfigError::WorkerNotFound(plan.promoted_worker_id));
        }

        let mut next = self.clone();
        let failed_primary_node_id = next.workers[plan.failed_primary_worker_id.as_usize()]
            .node_id
            .clone();
        let promoted_node_id = next.workers[plan.promoted_worker_id.as_usize()]
            .node_id
            .clone();

        for slot in next.slot_map.iter_mut() {
            if slot.assigned_worker_id() == plan.failed_primary_worker_id {
                *slot = HashSlot::new(plan.promoted_worker_id, SlotState::Stable);
            }
        }

        let next_epoch = next
            .workers
            .iter()
            .map(|worker| u64::from(worker.config_epoch))
            .max()
            .unwrap_or(0)
            .saturating_add(1);

        for worker in next.workers.iter_mut() {
            if worker.id == plan.promoted_worker_id {
                worker.role = WorkerRole::Primary;
                worker.replica_of_node_id = None;
                worker.replication_offset = plan.promoted_replication_offset;
                worker.config_epoch = ConfigEpoch::new(next_epoch);
                continue;
            }

            if worker.id == plan.failed_primary_worker_id {
                worker.role = WorkerRole::Replica;
                worker.replica_of_node_id = Some(promoted_node_id.clone());
                continue;
            }

            if worker.role == WorkerRole::Replica
                && worker.replica_of_node_id.as_deref() == Some(failed_primary_node_id.as_str())
            {
                worker.replica_of_node_id = Some(promoted_node_id.clone());
            }
        }

        Ok(next)
    }

    pub fn merge_from(&self, incoming: &Self) -> Self {
        let mut next = self.clone();

        for incoming_worker in &incoming.workers {
            let worker_index = incoming_worker.id.as_usize();
            if worker_index >= next.workers.len() {
                next.workers.push(incoming_worker.clone());
                continue;
            }

            let existing = &next.workers[worker_index];
            let should_replace = incoming_worker.config_epoch > existing.config_epoch
                || (incoming_worker.config_epoch == existing.config_epoch
                    && !incoming_worker.node_id.is_empty()
                    && (existing.node_id.is_empty() || incoming_worker.node_id < existing.node_id));
            if should_replace {
                next.workers[worker_index] = incoming_worker.clone();
            }
        }

        for (index, slot) in next.slot_map.iter_mut().enumerate() {
            let local_slot = self.slot_map[index];
            let incoming_slot = incoming.slot_map[index];
            if should_prefer_incoming_slot(self, local_slot, incoming, incoming_slot) {
                *slot = incoming_slot;
            }
        }

        next
    }

    fn slot_index(slot: SlotNumber) -> Result<usize, ClusterConfigError> {
        let slot_index = usize::from(slot);
        if slot_index >= HASH_SLOT_COUNT {
            return Err(ClusterConfigError::InvalidSlot(slot));
        }
        Ok(slot_index)
    }

    fn worker_endpoint(&self, worker_id: WorkerId) -> Result<String, ClusterConfigError> {
        let worker = self
            .worker(worker_id)
            .ok_or(ClusterConfigError::WorkerNotFound(worker_id))?;
        Ok(worker.endpoint())
    }
}

fn should_prefer_incoming_slot(
    local_config: &ClusterConfig,
    local_slot: HashSlot,
    incoming_config: &ClusterConfig,
    incoming_slot: HashSlot,
) -> bool {
    let local_claim = slot_claim_key(local_config, local_slot);
    let incoming_claim = slot_claim_key(incoming_config, incoming_slot);
    incoming_claim.0 > local_claim.0
        || (incoming_claim.0 == local_claim.0 && incoming_claim.1 < local_claim.1)
}

fn slot_claim_key(config: &ClusterConfig, slot: HashSlot) -> (ConfigEpoch, &str) {
    config
        .worker(slot.assigned_worker_id())
        .map(|worker| (worker.config_epoch, worker.node_id.as_str()))
        .unwrap_or((ConfigEpoch::new(0), ""))
}

const CLUSTER_CONFIG_SNAPSHOT_MAGIC: [u8; 4] = *b"GCFG";
const CLUSTER_CONFIG_SNAPSHOT_VERSION: u8 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterConfigCodecError {
    WorkerCountTooLarge(usize),
    StringTooLong(usize),
    UnexpectedEof,
    InvalidMagic,
    InvalidVersion(u8),
    InvalidUtf8,
    TrailingBytes(usize),
}

impl fmt::Display for ClusterConfigCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WorkerCountTooLarge(count) => {
                write!(f, "worker count does not fit into u16: {count}")
            }
            Self::StringTooLong(len) => write!(f, "string length does not fit into u16: {len}"),
            Self::UnexpectedEof => write!(f, "unexpected end of cluster-config snapshot bytes"),
            Self::InvalidMagic => write!(f, "invalid cluster-config snapshot magic"),
            Self::InvalidVersion(version) => {
                write!(f, "unsupported cluster-config snapshot version: {version}")
            }
            Self::InvalidUtf8 => write!(f, "cluster-config snapshot contains invalid utf8"),
            Self::TrailingBytes(extra) => write!(
                f,
                "cluster-config snapshot has trailing bytes after decode: {extra}"
            ),
        }
    }
}

impl std::error::Error for ClusterConfigCodecError {}

pub fn encode_cluster_config_snapshot(
    config: &ClusterConfig,
) -> Result<Vec<u8>, ClusterConfigCodecError> {
    let worker_count = config.workers.len();
    let worker_count_u16 = u16::try_from(worker_count)
        .map_err(|_| ClusterConfigCodecError::WorkerCountTooLarge(worker_count))?;
    let mut out = Vec::with_capacity(64 + worker_count * 96 + HASH_SLOT_COUNT * 3);
    out.extend_from_slice(&CLUSTER_CONFIG_SNAPSHOT_MAGIC);
    out.push(CLUSTER_CONFIG_SNAPSHOT_VERSION);
    out.extend_from_slice(&worker_count_u16.to_le_bytes());

    for worker in &config.workers {
        out.extend_from_slice(&worker.id.as_u16().to_le_bytes());
        out.extend_from_slice(&u64::from(worker.config_epoch).to_le_bytes());
        out.extend_from_slice(&worker.port.to_le_bytes());
        out.push(worker.role as u8);
        out.extend_from_slice(&u64::from(worker.replication_offset).to_le_bytes());
        push_len_prefixed_string(&mut out, &worker.node_id)?;
        push_len_prefixed_string(&mut out, &worker.host)?;
        match worker.replica_of_node_id.as_deref() {
            Some(primary_node_id) => {
                out.push(1);
                push_len_prefixed_string(&mut out, primary_node_id)?;
            }
            None => out.push(0),
        }
    }

    for slot in config.slot_map.iter() {
        out.extend_from_slice(&slot.assigned_worker_id().as_u16().to_le_bytes());
        out.push(slot.state() as u8);
    }

    Ok(out)
}

pub fn decode_cluster_config_snapshot(
    bytes: &[u8],
) -> Result<ClusterConfig, ClusterConfigCodecError> {
    let mut cursor = 0usize;
    let magic = read_exact_bytes(bytes, &mut cursor, CLUSTER_CONFIG_SNAPSHOT_MAGIC.len())?;
    if magic != CLUSTER_CONFIG_SNAPSHOT_MAGIC {
        return Err(ClusterConfigCodecError::InvalidMagic);
    }

    let version = read_u8(bytes, &mut cursor)?;
    if version != CLUSTER_CONFIG_SNAPSHOT_VERSION {
        return Err(ClusterConfigCodecError::InvalidVersion(version));
    }

    let worker_count = read_u16(bytes, &mut cursor)? as usize;
    let mut decoded_workers = Vec::with_capacity(worker_count);
    let mut max_worker_id = RESERVED_WORKER_ID;
    for _ in 0..worker_count {
        let id = WorkerId::new(read_u16(bytes, &mut cursor)?);
        let config_epoch = ConfigEpoch::new(read_u64(bytes, &mut cursor)?);
        let port = read_u16(bytes, &mut cursor)?;
        let role = match read_u8(bytes, &mut cursor)? {
            0 => WorkerRole::Unassigned,
            1 => WorkerRole::Primary,
            2 => WorkerRole::Replica,
            _ => WorkerRole::Unassigned,
        };
        let replication_offset = ReplicationOffset::new(read_u64(bytes, &mut cursor)?);
        let node_id = read_len_prefixed_string(bytes, &mut cursor)?;
        let host = read_len_prefixed_string(bytes, &mut cursor)?;
        let replica_of_node_id = match read_u8(bytes, &mut cursor)? {
            0 => None,
            1 => Some(read_len_prefixed_string(bytes, &mut cursor)?),
            _ => None,
        };

        max_worker_id = max_worker_id.max(id);
        decoded_workers.push(Worker {
            id,
            node_id,
            host,
            port,
            config_epoch,
            role,
            replica_of_node_id,
            replication_offset,
        });
    }

    let mut workers = if worker_count == 0 {
        vec![Worker::reserved(); LOCAL_WORKER_ID.as_usize() + 1]
    } else {
        vec![Worker::reserved(); max_worker_id.as_usize() + 1]
    };
    if workers.len() <= LOCAL_WORKER_ID.as_usize() {
        workers.resize(LOCAL_WORKER_ID.as_usize() + 1, Worker::reserved());
    }
    for worker in decoded_workers {
        let worker_id = worker.id.as_usize();
        if worker_id >= workers.len() {
            workers.resize(worker_id + 1, Worker::reserved());
        }
        workers[worker_id] = worker;
    }

    let mut slot_map = [HashSlot::default(); HASH_SLOT_COUNT];
    for slot in &mut slot_map {
        let worker_id = WorkerId::new(read_u16(bytes, &mut cursor)?);
        let state = SlotState::from_u8(read_u8(bytes, &mut cursor)?);
        *slot = HashSlot::new(worker_id, state);
    }

    if cursor != bytes.len() {
        return Err(ClusterConfigCodecError::TrailingBytes(bytes.len() - cursor));
    }

    Ok(ClusterConfig {
        slot_map: Box::new(slot_map),
        workers,
    })
}

fn push_len_prefixed_string(out: &mut Vec<u8>, value: &str) -> Result<(), ClusterConfigCodecError> {
    let len = value.len();
    let len_u16 = u16::try_from(len).map_err(|_| ClusterConfigCodecError::StringTooLong(len))?;
    out.extend_from_slice(&len_u16.to_le_bytes());
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn read_exact_bytes<'a>(
    bytes: &'a [u8],
    cursor: &mut usize,
    len: usize,
) -> Result<&'a [u8], ClusterConfigCodecError> {
    let end = cursor.saturating_add(len);
    if end > bytes.len() {
        return Err(ClusterConfigCodecError::UnexpectedEof);
    }
    let out = &bytes[*cursor..end];
    *cursor = end;
    Ok(out)
}

fn read_u8(bytes: &[u8], cursor: &mut usize) -> Result<u8, ClusterConfigCodecError> {
    Ok(read_exact_bytes(bytes, cursor, 1)?[0])
}

fn read_u16(bytes: &[u8], cursor: &mut usize) -> Result<u16, ClusterConfigCodecError> {
    let raw = read_exact_bytes(bytes, cursor, 2)?;
    Ok(u16::from_le_bytes([raw[0], raw[1]]))
}

fn read_u64(bytes: &[u8], cursor: &mut usize) -> Result<u64, ClusterConfigCodecError> {
    let raw = read_exact_bytes(bytes, cursor, 8)?;
    Ok(u64::from_le_bytes([
        raw[0], raw[1], raw[2], raw[3], raw[4], raw[5], raw[6], raw[7],
    ]))
}

fn read_len_prefixed_string(
    bytes: &[u8],
    cursor: &mut usize,
) -> Result<String, ClusterConfigCodecError> {
    let len = read_u16(bytes, cursor)? as usize;
    let raw = read_exact_bytes(bytes, cursor, len)?;
    let as_str = std::str::from_utf8(raw).map_err(|_| ClusterConfigCodecError::InvalidUtf8)?;
    Ok(as_str.to_owned())
}

pub fn redis_hash_slot(key: &[u8]) -> SlotNumber {
    let hash_key = extract_hash_tag(key).unwrap_or(key);
    SlotNumber::new(crc16_xmodem(hash_key) % (HASH_SLOT_COUNT as u16))
}

fn extract_hash_tag(key: &[u8]) -> Option<&[u8]> {
    let start = key.iter().position(|byte| *byte == b'{')?;
    let tail = &key[start + 1..];
    let end_offset = tail.iter().position(|byte| *byte == b'}')?;
    if end_offset == 0 {
        return None;
    }
    Some(&tail[..end_offset])
}

fn crc16_xmodem(input: &[u8]) -> u16 {
    let mut crc = 0u16;
    for byte in input {
        crc ^= u16::from(*byte) << 8;
        for _ in 0..8 {
            if (crc & 0x8000) != 0 {
                crc = (crc << 1) ^ 0x1021;
            } else {
                crc <<= 1;
            }
        }
    }
    crc
}

#[derive(Debug)]
pub struct ClusterConfigStore {
    current: RwLock<Arc<ClusterConfig>>,
}

impl ClusterConfigStore {
    pub fn new(initial: ClusterConfig) -> Self {
        Self {
            current: RwLock::new(Arc::new(initial)),
        }
    }

    pub fn load(&self) -> Arc<ClusterConfig> {
        self.current
            .read()
            .expect("cluster config store read lock poisoned")
            .clone()
    }

    pub fn publish(&self, next: ClusterConfig) -> Arc<ClusterConfig> {
        let mut write_guard = self
            .current
            .write()
            .expect("cluster config store write lock poisoned");
        let previous = write_guard.clone();
        *write_guard = Arc::new(next);
        previous
    }

    pub fn merge_publish(&self, incoming: &ClusterConfig) -> Arc<ClusterConfig> {
        let mut write_guard = self
            .current
            .write()
            .expect("cluster config store write lock poisoned");
        let merged = write_guard.merge_from(incoming);
        *write_guard = Arc::new(merged);
        write_guard.clone()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicationError {
    InvalidAofWindow {
        replay_start_offset: ReplicationOffset,
        tail_offset: ReplicationOffset,
    },
    MissingCheckpointForFullSync,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicationSyncError<E> {
    Replication(ReplicationError),
    Transport(E),
}

impl fmt::Display for ReplicationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidAofWindow {
                replay_start_offset,
                tail_offset,
            } => write!(
                f,
                "invalid AOF replay window: start offset {replay_start_offset} is greater than tail offset {tail_offset}"
            ),
            Self::MissingCheckpointForFullSync => {
                write!(f, "full sync requested but no checkpoint is available")
            }
        }
    }
}

impl std::error::Error for ReplicationError {}

impl<E: fmt::Display> fmt::Display for ReplicationSyncError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Replication(error) => write!(f, "{error}"),
            Self::Transport(error) => write!(f, "{error}"),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for ReplicationSyncError<E> {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationSyncMode {
    Full,
    Incremental,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationSyncPlan {
    pub mode: ReplicationSyncMode,
    pub checkpoint_id: Option<CheckpointId>,
    pub aof_start_offset: ReplicationOffset,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaProgress {
    pub worker_id: WorkerId,
    pub acknowledged_offset: ReplicationOffset,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailoverPlan {
    pub failed_primary_worker_id: WorkerId,
    pub promoted_worker_id: WorkerId,
    pub promoted_replication_offset: ReplicationOffset,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationSyncOutcome {
    pub plan: ReplicationSyncPlan,
    pub streamed_until_offset: ReplicationOffset,
}

pub trait ReplicationTransport {
    type Error;

    fn send_checkpoint(
        &mut self,
        worker_id: WorkerId,
        checkpoint_id: CheckpointId,
    ) -> Result<(), Self::Error>;
    fn stream_aof_from_offset(
        &mut self,
        worker_id: WorkerId,
        start_offset: ReplicationOffset,
    ) -> Result<ReplicationOffset, Self::Error>;
}

pub trait AsyncReplicationTransport {
    type Error;
    type CheckpointFut<'a>: Future<Output = Result<(), Self::Error>> + 'a
    where
        Self: 'a;
    type StreamFut<'a>: Future<Output = Result<ReplicationOffset, Self::Error>> + 'a
    where
        Self: 'a;

    fn send_checkpoint<'a>(
        &'a mut self,
        worker_id: WorkerId,
        checkpoint_id: CheckpointId,
    ) -> Self::CheckpointFut<'a>;
    fn stream_aof_from_offset<'a>(
        &'a mut self,
        worker_id: WorkerId,
        start_offset: ReplicationOffset,
    ) -> Self::StreamFut<'a>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicationEvent {
    Checkpoint {
        worker_id: WorkerId,
        checkpoint_id: CheckpointId,
    },
    StreamAof {
        worker_id: WorkerId,
        start_offset: ReplicationOffset,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChannelReplicationTransportError {
    ChannelClosed,
}

impl fmt::Display for ChannelReplicationTransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ChannelClosed => write!(f, "replication event channel is closed"),
        }
    }
}

impl std::error::Error for ChannelReplicationTransportError {}

#[derive(Debug)]
pub struct ChannelReplicationTransport {
    sender: tokio::sync::mpsc::UnboundedSender<ReplicationEvent>,
    stream_result: ReplicationOffset,
}

impl ChannelReplicationTransport {
    pub fn new(
        sender: tokio::sync::mpsc::UnboundedSender<ReplicationEvent>,
        stream_result: ReplicationOffset,
    ) -> Self {
        Self {
            sender,
            stream_result,
        }
    }

    pub fn stream_result(&self) -> ReplicationOffset {
        self.stream_result
    }

    pub fn set_stream_result(&mut self, stream_result: ReplicationOffset) {
        self.stream_result = stream_result;
    }
}

impl AsyncReplicationTransport for ChannelReplicationTransport {
    type Error = ChannelReplicationTransportError;
    type CheckpointFut<'a>
        = std::future::Ready<Result<(), Self::Error>>
    where
        Self: 'a;
    type StreamFut<'a>
        = std::future::Ready<Result<ReplicationOffset, Self::Error>>
    where
        Self: 'a;

    fn send_checkpoint<'a>(
        &'a mut self,
        worker_id: WorkerId,
        checkpoint_id: CheckpointId,
    ) -> Self::CheckpointFut<'a> {
        std::future::ready(
            self.sender
                .send(ReplicationEvent::Checkpoint {
                    worker_id,
                    checkpoint_id,
                })
                .map_err(|_| ChannelReplicationTransportError::ChannelClosed),
        )
    }

    fn stream_aof_from_offset<'a>(
        &'a mut self,
        worker_id: WorkerId,
        start_offset: ReplicationOffset,
    ) -> Self::StreamFut<'a> {
        std::future::ready(
            self.sender
                .send(ReplicationEvent::StreamAof {
                    worker_id,
                    start_offset,
                })
                .map(|_| self.stream_result)
                .map_err(|_| ChannelReplicationTransportError::ChannelClosed),
        )
    }
}

#[derive(Debug)]
pub struct FileReplicationTransport {
    root: PathBuf,
    stream_result: ReplicationOffset,
}

impl FileReplicationTransport {
    pub fn new(root: impl Into<PathBuf>, stream_result: ReplicationOffset) -> Self {
        Self {
            root: root.into(),
            stream_result,
        }
    }

    pub fn stream_result(&self) -> ReplicationOffset {
        self.stream_result
    }

    pub fn set_stream_result(&mut self, stream_result: ReplicationOffset) {
        self.stream_result = stream_result;
    }

    fn checkpoint_path(&self, worker_id: WorkerId) -> PathBuf {
        self.root.join(format!("worker-{worker_id}.checkpoint"))
    }

    fn aof_path(&self, worker_id: WorkerId) -> PathBuf {
        self.root.join(format!("worker-{worker_id}.aof"))
    }

    fn ensure_root_dir(&self) -> Result<(), FileReplicationTransportError> {
        std::fs::create_dir_all(&self.root).map_err(FileReplicationTransportError::Io)
    }

    fn write_checkpoint(
        &self,
        worker_id: WorkerId,
        checkpoint_id: CheckpointId,
    ) -> Result<(), FileReplicationTransportError> {
        self.ensure_root_dir()?;
        std::fs::write(
            self.checkpoint_path(worker_id),
            format!("{checkpoint_id}\n"),
        )
        .map_err(FileReplicationTransportError::Io)
    }

    fn append_aof_start_offset(
        &self,
        worker_id: WorkerId,
        start_offset: ReplicationOffset,
    ) -> Result<(), FileReplicationTransportError> {
        self.ensure_root_dir()?;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.aof_path(worker_id))
            .map_err(FileReplicationTransportError::Io)?;
        writeln!(file, "{start_offset}").map_err(FileReplicationTransportError::Io)
    }
}

#[derive(Debug)]
pub enum FileReplicationTransportError {
    Io(std::io::Error),
}

impl fmt::Display for FileReplicationTransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for FileReplicationTransportError {}

impl ReplicationTransport for FileReplicationTransport {
    type Error = FileReplicationTransportError;

    fn send_checkpoint(
        &mut self,
        worker_id: WorkerId,
        checkpoint_id: CheckpointId,
    ) -> Result<(), Self::Error> {
        self.write_checkpoint(worker_id, checkpoint_id)
    }

    fn stream_aof_from_offset(
        &mut self,
        worker_id: WorkerId,
        start_offset: ReplicationOffset,
    ) -> Result<ReplicationOffset, Self::Error> {
        self.append_aof_start_offset(worker_id, start_offset)?;
        Ok(self.stream_result)
    }
}

impl AsyncReplicationTransport for FileReplicationTransport {
    type Error = FileReplicationTransportError;
    type CheckpointFut<'a>
        = std::future::Ready<Result<(), Self::Error>>
    where
        Self: 'a;
    type StreamFut<'a>
        = std::future::Ready<Result<ReplicationOffset, Self::Error>>
    where
        Self: 'a;

    fn send_checkpoint<'a>(
        &'a mut self,
        worker_id: WorkerId,
        checkpoint_id: CheckpointId,
    ) -> Self::CheckpointFut<'a> {
        std::future::ready(self.write_checkpoint(worker_id, checkpoint_id))
    }

    fn stream_aof_from_offset<'a>(
        &'a mut self,
        worker_id: WorkerId,
        start_offset: ReplicationOffset,
    ) -> Self::StreamFut<'a> {
        std::future::ready(
            self.append_aof_start_offset(worker_id, start_offset)
                .map(|_| self.stream_result),
        )
    }
}

#[derive(Debug)]
pub struct TcpReplicationTransport {
    peers: BTreeMap<WorkerId, SocketAddr>,
    stream_result: ReplicationOffset,
}

impl TcpReplicationTransport {
    pub fn new(stream_result: ReplicationOffset) -> Self {
        Self {
            peers: BTreeMap::new(),
            stream_result,
        }
    }

    pub fn add_peer(&mut self, worker_id: WorkerId, endpoint: SocketAddr) {
        self.peers.insert(worker_id, endpoint);
    }

    pub fn stream_result(&self) -> ReplicationOffset {
        self.stream_result
    }

    pub fn set_stream_result(&mut self, stream_result: ReplicationOffset) {
        self.stream_result = stream_result;
    }

    fn peer_endpoint(
        &self,
        worker_id: WorkerId,
    ) -> Result<SocketAddr, TcpReplicationTransportError> {
        self.peers
            .get(&worker_id)
            .copied()
            .ok_or(TcpReplicationTransportError::UnknownPeer(worker_id))
    }
}

#[derive(Debug)]
pub enum TcpReplicationTransportError {
    UnknownPeer(WorkerId),
    Io(std::io::Error),
}

impl fmt::Display for TcpReplicationTransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownPeer(worker_id) => {
                write!(f, "unknown replication peer worker id: {worker_id}")
            }
            Self::Io(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for TcpReplicationTransportError {}

impl ReplicationTransport for TcpReplicationTransport {
    type Error = TcpReplicationTransportError;

    fn send_checkpoint(
        &mut self,
        worker_id: WorkerId,
        checkpoint_id: CheckpointId,
    ) -> Result<(), Self::Error> {
        let endpoint = self.peer_endpoint(worker_id)?;
        let mut stream =
            std::net::TcpStream::connect(endpoint).map_err(TcpReplicationTransportError::Io)?;
        stream
            .write_all(format!("CHECKPOINT {checkpoint_id}\n").as_bytes())
            .map_err(TcpReplicationTransportError::Io)
    }

    fn stream_aof_from_offset(
        &mut self,
        worker_id: WorkerId,
        start_offset: ReplicationOffset,
    ) -> Result<ReplicationOffset, Self::Error> {
        let endpoint = self.peer_endpoint(worker_id)?;
        let mut stream =
            std::net::TcpStream::connect(endpoint).map_err(TcpReplicationTransportError::Io)?;
        stream
            .write_all(format!("AOF {start_offset}\n").as_bytes())
            .map_err(TcpReplicationTransportError::Io)?;
        Ok(self.stream_result)
    }
}

impl AsyncReplicationTransport for TcpReplicationTransport {
    type Error = TcpReplicationTransportError;
    type CheckpointFut<'a>
        = Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>>
    where
        Self: 'a;
    type StreamFut<'a>
        = Pin<Box<dyn Future<Output = Result<ReplicationOffset, Self::Error>> + Send + 'a>>
    where
        Self: 'a;

    fn send_checkpoint<'a>(
        &'a mut self,
        worker_id: WorkerId,
        checkpoint_id: CheckpointId,
    ) -> Self::CheckpointFut<'a> {
        let endpoint = self.peers.get(&worker_id).copied();
        Box::pin(async move {
            let endpoint = endpoint.ok_or(TcpReplicationTransportError::UnknownPeer(worker_id))?;
            let mut stream = tokio::net::TcpStream::connect(endpoint)
                .await
                .map_err(TcpReplicationTransportError::Io)?;
            stream
                .write_all(format!("CHECKPOINT {checkpoint_id}\n").as_bytes())
                .await
                .map_err(TcpReplicationTransportError::Io)
        })
    }

    fn stream_aof_from_offset<'a>(
        &'a mut self,
        worker_id: WorkerId,
        start_offset: ReplicationOffset,
    ) -> Self::StreamFut<'a> {
        let endpoint = self.peers.get(&worker_id).copied();
        let stream_result = self.stream_result;
        Box::pin(async move {
            let endpoint = endpoint.ok_or(TcpReplicationTransportError::UnknownPeer(worker_id))?;
            let mut stream = tokio::net::TcpStream::connect(endpoint)
                .await
                .map_err(TcpReplicationTransportError::Io)?;
            stream
                .write_all(format!("AOF {start_offset}\n").as_bytes())
                .await
                .map_err(TcpReplicationTransportError::Io)?;
            Ok(stream_result)
        })
    }
}

#[derive(Debug, Clone)]
pub struct ReplicationManager {
    checkpoint_id: Option<CheckpointId>,
    aof_replay_start_offset: ReplicationOffset,
    aof_tail_offset: ReplicationOffset,
    replica_offsets: BTreeMap<WorkerId, ReplicationOffset>,
}

impl ReplicationManager {
    pub fn new(
        checkpoint_id: Option<CheckpointId>,
        aof_replay_start_offset: ReplicationOffset,
        aof_tail_offset: ReplicationOffset,
    ) -> Result<Self, ReplicationError> {
        if aof_replay_start_offset > aof_tail_offset {
            return Err(ReplicationError::InvalidAofWindow {
                replay_start_offset: aof_replay_start_offset,
                tail_offset: aof_tail_offset,
            });
        }
        Ok(Self {
            checkpoint_id,
            aof_replay_start_offset,
            aof_tail_offset,
            replica_offsets: BTreeMap::new(),
        })
    }

    pub fn checkpoint_id(&self) -> Option<CheckpointId> {
        self.checkpoint_id
    }

    pub fn aof_replay_start_offset(&self) -> ReplicationOffset {
        self.aof_replay_start_offset
    }

    pub fn aof_tail_offset(&self) -> ReplicationOffset {
        self.aof_tail_offset
    }

    pub fn update_recovery_window(
        &mut self,
        checkpoint_id: Option<CheckpointId>,
        aof_replay_start_offset: ReplicationOffset,
        aof_tail_offset: ReplicationOffset,
    ) -> Result<(), ReplicationError> {
        if aof_replay_start_offset > aof_tail_offset {
            return Err(ReplicationError::InvalidAofWindow {
                replay_start_offset: aof_replay_start_offset,
                tail_offset: aof_tail_offset,
            });
        }
        self.checkpoint_id = checkpoint_id;
        self.aof_replay_start_offset = aof_replay_start_offset;
        self.aof_tail_offset = aof_tail_offset;
        Ok(())
    }

    pub fn set_aof_tail_offset(
        &mut self,
        aof_tail_offset: ReplicationOffset,
    ) -> Result<(), ReplicationError> {
        if self.aof_replay_start_offset > aof_tail_offset {
            return Err(ReplicationError::InvalidAofWindow {
                replay_start_offset: self.aof_replay_start_offset,
                tail_offset: aof_tail_offset,
            });
        }
        self.aof_tail_offset = aof_tail_offset;
        Ok(())
    }

    pub fn record_replica_offset(
        &mut self,
        worker_id: WorkerId,
        acknowledged_offset: ReplicationOffset,
    ) {
        self.replica_offsets.insert(worker_id, acknowledged_offset);
    }

    pub fn replica_offset(&self, worker_id: WorkerId) -> Option<ReplicationOffset> {
        self.replica_offsets.get(&worker_id).copied()
    }

    pub fn best_replica_candidate(&self) -> Option<ReplicaProgress> {
        self.replica_offsets
            .iter()
            .max_by_key(|(worker_id, offset)| (*offset, std::cmp::Reverse(**worker_id)))
            .map(|(worker_id, offset)| ReplicaProgress {
                worker_id: *worker_id,
                acknowledged_offset: *offset,
            })
    }

    pub fn plan_sync(&self, replica_offset: Option<ReplicationOffset>) -> ReplicationSyncPlan {
        if let Some(offset) = replica_offset {
            if offset >= self.aof_replay_start_offset && offset <= self.aof_tail_offset {
                return ReplicationSyncPlan {
                    mode: ReplicationSyncMode::Incremental,
                    checkpoint_id: None,
                    aof_start_offset: offset,
                };
            }
        }

        ReplicationSyncPlan {
            mode: ReplicationSyncMode::Full,
            checkpoint_id: self.checkpoint_id,
            aof_start_offset: self.aof_replay_start_offset,
        }
    }

    pub fn execute_sync<T: ReplicationTransport>(
        &mut self,
        worker_id: WorkerId,
        replica_offset: Option<ReplicationOffset>,
        transport: &mut T,
    ) -> Result<ReplicationSyncOutcome, ReplicationSyncError<T::Error>> {
        let plan = self.plan_sync(replica_offset);
        if matches!(plan.mode, ReplicationSyncMode::Full) {
            let checkpoint_id = plan.checkpoint_id.ok_or(ReplicationSyncError::Replication(
                ReplicationError::MissingCheckpointForFullSync,
            ))?;
            transport
                .send_checkpoint(worker_id, checkpoint_id)
                .map_err(ReplicationSyncError::Transport)?;
        }

        let streamed_until_offset = transport
            .stream_aof_from_offset(worker_id, plan.aof_start_offset)
            .map_err(ReplicationSyncError::Transport)?;
        self.record_replica_offset(worker_id, streamed_until_offset);

        Ok(ReplicationSyncOutcome {
            plan,
            streamed_until_offset,
        })
    }

    pub fn execute_sync_for_worker<T: ReplicationTransport>(
        &mut self,
        worker_id: WorkerId,
        transport: &mut T,
    ) -> Result<ReplicationSyncOutcome, ReplicationSyncError<T::Error>> {
        self.execute_sync(worker_id, self.replica_offset(worker_id), transport)
    }

    pub fn sync_replicas_of_primary<T: ReplicationTransport>(
        &mut self,
        config: &ClusterConfig,
        primary_node_id: &str,
        transport: &mut T,
    ) -> Result<Vec<(WorkerId, ReplicationSyncOutcome)>, ReplicationSyncError<T::Error>> {
        let mut outcomes = Vec::new();
        for worker_id in self.replica_worker_ids_for_primary(config, primary_node_id) {
            let outcome = self.execute_sync_for_worker(worker_id, transport)?;
            outcomes.push((worker_id, outcome));
        }
        Ok(outcomes)
    }

    pub async fn execute_sync_async<T: AsyncReplicationTransport>(
        &mut self,
        worker_id: WorkerId,
        replica_offset: Option<ReplicationOffset>,
        transport: &mut T,
    ) -> Result<ReplicationSyncOutcome, ReplicationSyncError<T::Error>> {
        let plan = self.plan_sync(replica_offset);
        if matches!(plan.mode, ReplicationSyncMode::Full) {
            let checkpoint_id = plan.checkpoint_id.ok_or(ReplicationSyncError::Replication(
                ReplicationError::MissingCheckpointForFullSync,
            ))?;
            transport
                .send_checkpoint(worker_id, checkpoint_id)
                .await
                .map_err(ReplicationSyncError::Transport)?;
        }

        let streamed_until_offset = transport
            .stream_aof_from_offset(worker_id, plan.aof_start_offset)
            .await
            .map_err(ReplicationSyncError::Transport)?;
        self.record_replica_offset(worker_id, streamed_until_offset);

        Ok(ReplicationSyncOutcome {
            plan,
            streamed_until_offset,
        })
    }

    pub async fn execute_sync_for_worker_async<T: AsyncReplicationTransport>(
        &mut self,
        worker_id: WorkerId,
        transport: &mut T,
    ) -> Result<ReplicationSyncOutcome, ReplicationSyncError<T::Error>> {
        self.execute_sync_async(worker_id, self.replica_offset(worker_id), transport)
            .await
    }

    pub async fn sync_replicas_of_primary_async<T: AsyncReplicationTransport>(
        &mut self,
        config: &ClusterConfig,
        primary_node_id: &str,
        transport: &mut T,
    ) -> Result<Vec<(WorkerId, ReplicationSyncOutcome)>, ReplicationSyncError<T::Error>> {
        let mut outcomes = Vec::new();
        for worker_id in self.replica_worker_ids_for_primary(config, primary_node_id) {
            let outcome = self
                .execute_sync_for_worker_async(worker_id, transport)
                .await?;
            outcomes.push((worker_id, outcome));
        }
        Ok(outcomes)
    }

    pub fn replica_worker_ids_for_primary(
        &self,
        config: &ClusterConfig,
        primary_node_id: &str,
    ) -> Vec<WorkerId> {
        let mut worker_ids = config
            .workers()
            .iter()
            .filter(|worker| worker.role == WorkerRole::Replica)
            .filter(|worker| worker.replica_of_node_id.as_deref() == Some(primary_node_id))
            .map(|worker| worker.id)
            .collect::<Vec<_>>();
        worker_ids.sort_unstable();
        worker_ids
    }

    pub fn plan_failover(
        &self,
        config: &ClusterConfig,
        failed_primary_node_id: &str,
    ) -> Option<FailoverPlan> {
        let failed_primary = config.workers().iter().find(|worker| {
            worker.role == WorkerRole::Primary && worker.node_id == failed_primary_node_id
        })?;

        let mut best: Option<(ReplicationOffset, WorkerId)> = None;
        for worker in config.workers().iter() {
            if worker.role != WorkerRole::Replica {
                continue;
            }
            if worker.replica_of_node_id.as_deref() != Some(failed_primary_node_id) {
                continue;
            }

            let offset = self
                .replica_offset(worker.id)
                .unwrap_or(worker.replication_offset);
            match best {
                None => best = Some((offset, worker.id)),
                Some((best_offset, best_worker_id)) => {
                    if offset > best_offset || (offset == best_offset && worker.id < best_worker_id)
                    {
                        best = Some((offset, worker.id));
                    }
                }
            }
        }

        let (promoted_replication_offset, promoted_worker_id) = best?;
        Some(FailoverPlan {
            failed_primary_worker_id: failed_primary.id,
            promoted_worker_id,
            promoted_replication_offset,
        })
    }

    pub fn execute_failover(
        &self,
        config: &ClusterConfig,
        failed_primary_node_id: &str,
    ) -> Result<Option<(FailoverPlan, ClusterConfig)>, ClusterConfigError> {
        let Some(plan) = self.plan_failover(config, failed_primary_node_id) else {
            return Ok(None);
        };
        let updated = config.apply_failover_plan(&plan)?;
        Ok(Some((plan, updated)))
    }
}

#[derive(Debug, Default)]
pub struct FailoverCoordinator {
    handled_failed_primaries: BTreeSet<String>,
}

impl FailoverCoordinator {
    pub fn new() -> Self {
        Self {
            handled_failed_primaries: BTreeSet::new(),
        }
    }

    pub fn handled_failed_primaries(&self) -> &BTreeSet<String> {
        &self.handled_failed_primaries
    }

    pub fn has_handled_failed_primary(&self, failed_primary_node_id: &str) -> bool {
        self.handled_failed_primaries
            .contains(failed_primary_node_id)
    }

    pub fn execute_for_failed_primary(
        &mut self,
        config_store: &ClusterConfigStore,
        replication_manager: &ReplicationManager,
        failed_primary_node_id: &str,
    ) -> Result<Option<FailoverPlan>, ClusterConfigError> {
        if self
            .handled_failed_primaries
            .contains(failed_primary_node_id)
        {
            return Ok(None);
        }

        let current = config_store.load();
        let Some((plan, updated)) =
            replication_manager.execute_failover(current.as_ref(), failed_primary_node_id)?
        else {
            return Ok(None);
        };

        config_store.publish(updated);
        self.handled_failed_primaries
            .insert(failed_primary_node_id.to_owned());
        Ok(Some(plan))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FailoverControllerError<E> {
    Replication(ReplicationSyncError<E>),
    Config(ClusterConfigError),
}

impl<E: fmt::Display> fmt::Display for FailoverControllerError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Replication(error) => write!(f, "{error}"),
            Self::Config(error) => write!(f, "{error}"),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for FailoverControllerError<E> {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailoverControllerOutcome {
    pub synchronized_replicas: Vec<(WorkerId, ReplicationSyncOutcome)>,
    pub failover_plan: Option<FailoverPlan>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailoverExecutionRecord {
    pub failed_worker_id: WorkerId,
    pub synchronized_replicas: Vec<(WorkerId, ReplicationSyncOutcome)>,
    pub plan: FailoverPlan,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterManagerFailoverRunReport {
    pub gossip_reports: Vec<GossipRoundReport>,
    pub failover_records: Vec<FailoverExecutionRecord>,
}

#[derive(Debug, Default)]
pub struct ClusterFailoverController {
    coordinator: FailoverCoordinator,
}

impl ClusterFailoverController {
    pub fn new() -> Self {
        Self {
            coordinator: FailoverCoordinator::new(),
        }
    }

    pub fn coordinator(&self) -> &FailoverCoordinator {
        &self.coordinator
    }

    pub fn handle_failed_primary<T: ReplicationTransport>(
        &mut self,
        config_store: &ClusterConfigStore,
        replication_manager: &mut ReplicationManager,
        failed_primary_node_id: &str,
        transport: &mut T,
    ) -> Result<FailoverControllerOutcome, FailoverControllerError<T::Error>> {
        if self
            .coordinator
            .has_handled_failed_primary(failed_primary_node_id)
        {
            return Ok(FailoverControllerOutcome {
                synchronized_replicas: Vec::new(),
                failover_plan: None,
            });
        }

        let current = config_store.load();
        let synchronized_replicas = replication_manager
            .sync_replicas_of_primary(current.as_ref(), failed_primary_node_id, transport)
            .map_err(FailoverControllerError::Replication)?;
        let failover_plan = self
            .coordinator
            .execute_for_failed_primary(config_store, replication_manager, failed_primary_node_id)
            .map_err(FailoverControllerError::Config)?;

        Ok(FailoverControllerOutcome {
            synchronized_replicas,
            failover_plan,
        })
    }

    pub async fn handle_failed_primary_async<T: AsyncReplicationTransport>(
        &mut self,
        config_store: &ClusterConfigStore,
        replication_manager: &mut ReplicationManager,
        failed_primary_node_id: &str,
        transport: &mut T,
    ) -> Result<FailoverControllerOutcome, FailoverControllerError<T::Error>> {
        if self
            .coordinator
            .has_handled_failed_primary(failed_primary_node_id)
        {
            return Ok(FailoverControllerOutcome {
                synchronized_replicas: Vec::new(),
                failover_plan: None,
            });
        }

        let current = config_store.load();
        let synchronized_replicas = replication_manager
            .sync_replicas_of_primary_async(current.as_ref(), failed_primary_node_id, transport)
            .await
            .map_err(FailoverControllerError::Replication)?;
        let failover_plan = self
            .coordinator
            .execute_for_failed_primary(config_store, replication_manager, failed_primary_node_id)
            .map_err(FailoverControllerError::Config)?;

        Ok(FailoverControllerOutcome {
            synchronized_replicas,
            failover_plan,
        })
    }

    pub fn handle_failed_workers<T: ReplicationTransport>(
        &mut self,
        config_store: &ClusterConfigStore,
        replication_manager: &mut ReplicationManager,
        failed_worker_ids: &[WorkerId],
        transport: &mut T,
    ) -> Result<Vec<(WorkerId, FailoverControllerOutcome)>, FailoverControllerError<T::Error>> {
        let mut failed_primaries = Vec::new();
        {
            let snapshot = config_store.load();
            for worker_id in failed_worker_ids.iter().copied() {
                let Some(worker) = snapshot.worker(worker_id) else {
                    continue;
                };
                if worker.role != WorkerRole::Primary {
                    continue;
                }
                failed_primaries.push((worker_id, worker.node_id.clone()));
            }
        }

        let mut outcomes = Vec::new();
        for (worker_id, node_id) in failed_primaries {
            let outcome =
                self.handle_failed_primary(config_store, replication_manager, &node_id, transport)?;
            if outcome.failover_plan.is_some() || !outcome.synchronized_replicas.is_empty() {
                outcomes.push((worker_id, outcome));
            }
        }
        Ok(outcomes)
    }

    pub async fn handle_failed_workers_async<T: AsyncReplicationTransport>(
        &mut self,
        config_store: &ClusterConfigStore,
        replication_manager: &mut ReplicationManager,
        failed_worker_ids: &[WorkerId],
        transport: &mut T,
    ) -> Result<Vec<(WorkerId, FailoverControllerOutcome)>, FailoverControllerError<T::Error>> {
        let mut failed_primaries = Vec::new();
        {
            let snapshot = config_store.load();
            for worker_id in failed_worker_ids.iter().copied() {
                let Some(worker) = snapshot.worker(worker_id) else {
                    continue;
                };
                if worker.role != WorkerRole::Primary {
                    continue;
                }
                failed_primaries.push((worker_id, worker.node_id.clone()));
            }
        }

        let mut outcomes = Vec::new();
        for (worker_id, node_id) in failed_primaries {
            let outcome = self
                .handle_failed_primary_async(config_store, replication_manager, &node_id, transport)
                .await?;
            if outcome.failover_plan.is_some() || !outcome.synchronized_replicas.is_empty() {
                outcomes.push((worker_id, outcome));
            }
        }
        Ok(outcomes)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GossipNode {
    pub worker_id: WorkerId,
    pub last_gossip_tick: GossipTick,
}

impl GossipNode {
    pub fn new(worker_id: WorkerId, last_gossip_tick: impl Into<GossipTick>) -> Self {
        Self {
            worker_id,
            last_gossip_tick: last_gossip_tick.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GossipSendResult {
    Success,
    Failure,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GossipRoundReport {
    pub attempted_worker_ids: Vec<WorkerId>,
    pub successful_worker_ids: Vec<WorkerId>,
    pub failed_worker_ids: Vec<WorkerId>,
    pub success_count: usize,
    pub failure_count: usize,
    pub remaining_failure_budget: usize,
}

pub fn calculate_gossip_failure_budget(node_count: usize, sample_percent: usize) -> usize {
    if node_count == 0 {
        return 0;
    }
    let bounded_percent = sample_percent.min(100);
    let sampled = (node_count * bounded_percent).div_ceil(100);
    sampled.max(1)
}

#[derive(Debug, Clone)]
pub struct FailureDetector {
    failure_threshold: usize,
    consecutive_failures: BTreeMap<WorkerId, usize>,
    failed_workers: BTreeSet<WorkerId>,
}

impl FailureDetector {
    pub fn new(failure_threshold: usize) -> Self {
        Self {
            failure_threshold: failure_threshold.max(1),
            consecutive_failures: BTreeMap::new(),
            failed_workers: BTreeSet::new(),
        }
    }

    pub fn failure_threshold(&self) -> usize {
        self.failure_threshold
    }

    pub fn failed_workers(&self) -> &BTreeSet<WorkerId> {
        &self.failed_workers
    }

    pub fn consecutive_failures(&self, worker_id: WorkerId) -> usize {
        self.consecutive_failures
            .get(&worker_id)
            .copied()
            .unwrap_or(0)
    }

    pub fn is_failed(&self, worker_id: WorkerId) -> bool {
        self.failed_workers.contains(&worker_id)
    }

    pub fn clear_worker(&mut self, worker_id: WorkerId) {
        self.consecutive_failures.remove(&worker_id);
        self.failed_workers.remove(&worker_id);
    }

    pub fn record_report(&mut self, report: &GossipRoundReport) -> Vec<WorkerId> {
        let failed_in_round = report
            .failed_worker_ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let mut newly_failed = Vec::new();

        for worker_id in report.attempted_worker_ids.iter().copied() {
            if failed_in_round.contains(&worker_id) {
                let entry = self.consecutive_failures.entry(worker_id).or_insert(0);
                *entry = entry.saturating_add(1);
                if *entry >= self.failure_threshold && self.failed_workers.insert(worker_id) {
                    newly_failed.push(worker_id);
                }
            } else {
                self.consecutive_failures.insert(worker_id, 0);
                self.failed_workers.remove(&worker_id);
            }
        }

        newly_failed
    }
}

pub fn run_gossip_sample_round<Selector, Attempt>(
    nodes: &mut [GossipNode],
    sample_percent: usize,
    max_random_nodes_to_poll: usize,
    round_start_tick: impl Into<GossipTick>,
    mut selector: Selector,
    mut attempt: Attempt,
) -> GossipRoundReport
where
    Selector: FnMut(&[GossipNode], usize) -> Vec<usize>,
    Attempt: FnMut(&mut GossipNode) -> GossipSendResult,
{
    let round_start_tick = round_start_tick.into();
    let mut failure_budget = calculate_gossip_failure_budget(nodes.len(), sample_percent);
    let mut report = GossipRoundReport {
        attempted_worker_ids: Vec::new(),
        successful_worker_ids: Vec::new(),
        failed_worker_ids: Vec::new(),
        success_count: 0,
        failure_count: 0,
        remaining_failure_budget: failure_budget,
    };

    while failure_budget > 0 {
        let candidate_indices = selector(nodes, max_random_nodes_to_poll);
        let candidate = candidate_indices
            .into_iter()
            .take(max_random_nodes_to_poll)
            .filter(|index| *index < nodes.len())
            .filter(|index| nodes[*index].last_gossip_tick < round_start_tick)
            .min_by_key(|index| nodes[*index].last_gossip_tick);

        let Some(candidate) = candidate else {
            break;
        };

        let node = &mut nodes[candidate];
        report.attempted_worker_ids.push(node.worker_id);
        match attempt(node) {
            GossipSendResult::Success => {
                node.last_gossip_tick = round_start_tick;
                report.successful_worker_ids.push(node.worker_id);
                report.success_count += 1;
            }
            GossipSendResult::Failure => {
                // The original implementation removes failed connections from the
                // candidate pool for the remainder of the round. Mark this node
                // as non-stale so it is not retried in this round.
                node.last_gossip_tick = round_start_tick;
                failure_budget -= 1;
                report.failed_worker_ids.push(node.worker_id);
                report.failure_count += 1;
            }
        }
    }

    report.remaining_failure_budget = failure_budget;
    report
}

pub trait GossipTransport {
    type Error;

    fn try_gossip(&mut self, worker_id: WorkerId) -> Result<(), Self::Error>;
}

pub trait AsyncGossipTransport {
    type Error;
    type Fut<'a>: Future<Output = Result<(), Self::Error>> + 'a
    where
        Self: 'a;

    fn try_gossip<'a>(&'a mut self, worker_id: WorkerId) -> Self::Fut<'a>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InMemoryGossipTransportError {
    UnknownPeer(WorkerId),
    ChannelClosed(WorkerId),
}

impl fmt::Display for InMemoryGossipTransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownPeer(worker_id) => write!(f, "unknown gossip peer worker id: {worker_id}"),
            Self::ChannelClosed(worker_id) => {
                write!(f, "gossip peer channel is closed: {worker_id}")
            }
        }
    }
}

impl std::error::Error for InMemoryGossipTransportError {}

#[derive(Debug)]
pub struct InMemoryGossipTransport {
    local_config: Arc<ClusterConfigStore>,
    peers: BTreeMap<WorkerId, tokio::sync::mpsc::UnboundedSender<ClusterConfig>>,
}

impl InMemoryGossipTransport {
    pub fn new(local_config: Arc<ClusterConfigStore>) -> Self {
        Self {
            local_config,
            peers: BTreeMap::new(),
        }
    }

    pub fn add_peer(
        &mut self,
        worker_id: WorkerId,
        sender: tokio::sync::mpsc::UnboundedSender<ClusterConfig>,
    ) {
        self.peers.insert(worker_id, sender);
    }
}

impl AsyncGossipTransport for InMemoryGossipTransport {
    type Error = InMemoryGossipTransportError;
    type Fut<'a>
        = std::future::Ready<Result<(), Self::Error>>
    where
        Self: 'a;

    fn try_gossip<'a>(&'a mut self, worker_id: WorkerId) -> Self::Fut<'a> {
        let Some(peer) = self.peers.get(&worker_id) else {
            return std::future::ready(Err(InMemoryGossipTransportError::UnknownPeer(worker_id)));
        };

        let snapshot = self.local_config.load().as_ref().clone();
        std::future::ready(
            peer.send(snapshot)
                .map_err(|_| InMemoryGossipTransportError::ChannelClosed(worker_id)),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TcpGossipTransportError {
    UnknownPeer(WorkerId),
    MessageTooLarge(usize),
    Codec(ClusterConfigCodecError),
    Io(String),
}

impl fmt::Display for TcpGossipTransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownPeer(worker_id) => write!(f, "unknown gossip peer worker id: {worker_id}"),
            Self::MessageTooLarge(len) => write!(
                f,
                "gossip snapshot payload exceeds u32 frame length: {len} bytes"
            ),
            Self::Codec(error) => write!(f, "{error}"),
            Self::Io(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for TcpGossipTransportError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TcpGossipReadError {
    Io(String),
    MessageTooLarge(usize),
    Codec(ClusterConfigCodecError),
}

impl fmt::Display for TcpGossipReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "{error}"),
            Self::MessageTooLarge(len) => {
                write!(f, "gossip frame length exceeds read limit: {len} bytes")
            }
            Self::Codec(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for TcpGossipReadError {}

pub async fn read_gossip_snapshot(
    stream: &mut tokio::net::TcpStream,
    max_payload_len: usize,
) -> Result<ClusterConfig, TcpGossipReadError> {
    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .await
        .map_err(|error| TcpGossipReadError::Io(error.to_string()))?;
    let payload_len = u32::from_le_bytes(len_buf) as usize;
    if payload_len > max_payload_len {
        return Err(TcpGossipReadError::MessageTooLarge(payload_len));
    }

    let mut payload = vec![0u8; payload_len];
    stream
        .read_exact(&mut payload)
        .await
        .map_err(|error| TcpGossipReadError::Io(error.to_string()))?;
    decode_cluster_config_snapshot(&payload).map_err(TcpGossipReadError::Codec)
}

#[derive(Debug)]
pub struct TcpGossipTransport {
    local_config: Arc<ClusterConfigStore>,
    peers: BTreeMap<WorkerId, SocketAddr>,
}

impl TcpGossipTransport {
    pub fn new(local_config: Arc<ClusterConfigStore>) -> Self {
        Self {
            local_config,
            peers: BTreeMap::new(),
        }
    }

    pub fn add_peer(&mut self, worker_id: WorkerId, endpoint: SocketAddr) {
        self.peers.insert(worker_id, endpoint);
    }
}

impl AsyncGossipTransport for TcpGossipTransport {
    type Error = TcpGossipTransportError;
    type Fut<'a>
        = Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>>
    where
        Self: 'a;

    fn try_gossip<'a>(&'a mut self, worker_id: WorkerId) -> Self::Fut<'a> {
        let peer_endpoint = self.peers.get(&worker_id).copied();
        let snapshot = self.local_config.load();
        let payload = encode_cluster_config_snapshot(snapshot.as_ref());

        Box::pin(async move {
            let endpoint = peer_endpoint.ok_or(TcpGossipTransportError::UnknownPeer(worker_id))?;
            let payload = payload.map_err(TcpGossipTransportError::Codec)?;
            let payload_len = payload.len();
            let payload_len_u32 = u32::try_from(payload_len)
                .map_err(|_| TcpGossipTransportError::MessageTooLarge(payload_len))?;

            let mut stream = tokio::net::TcpStream::connect(endpoint)
                .await
                .map_err(|error| TcpGossipTransportError::Io(error.to_string()))?;
            stream
                .write_all(&payload_len_u32.to_le_bytes())
                .await
                .map_err(|error| TcpGossipTransportError::Io(error.to_string()))?;
            stream
                .write_all(&payload)
                .await
                .map_err(|error| TcpGossipTransportError::Io(error.to_string()))
        })
    }
}

#[derive(Debug)]
pub struct GossipCoordinator {
    nodes: Vec<GossipNode>,
    max_random_nodes_to_poll: usize,
    rng_state: u64,
}

impl GossipCoordinator {
    pub fn new(nodes: Vec<GossipNode>, max_random_nodes_to_poll: usize) -> Self {
        Self {
            nodes,
            max_random_nodes_to_poll,
            rng_state: 0x9E3779B97F4A7C15,
        }
    }

    pub fn nodes(&self) -> &[GossipNode] {
        &self.nodes
    }

    pub fn run_round<T: GossipTransport>(
        &mut self,
        sample_percent: usize,
        round_start_tick: impl Into<GossipTick>,
        transport: &mut T,
    ) -> GossipRoundReport {
        let round_start_tick = round_start_tick.into();
        let mut local_rng_state = self.rng_state;
        let max_poll = self.max_random_nodes_to_poll;
        let report = run_gossip_sample_round(
            &mut self.nodes,
            sample_percent,
            max_poll,
            round_start_tick,
            |nodes, requested| {
                sample_random_unique_indices(
                    nodes.len(),
                    requested.min(max_poll),
                    &mut local_rng_state,
                )
            },
            |node| match transport.try_gossip(node.worker_id) {
                Ok(()) => GossipSendResult::Success,
                Err(_) => GossipSendResult::Failure,
            },
        );
        self.rng_state = local_rng_state;
        report
    }

    pub async fn run_round_async<T: AsyncGossipTransport>(
        &mut self,
        sample_percent: usize,
        round_start_tick: impl Into<GossipTick>,
        transport: &mut T,
    ) -> GossipRoundReport {
        let round_start_tick = round_start_tick.into();
        let mut failure_budget = calculate_gossip_failure_budget(self.nodes.len(), sample_percent);
        let mut report = GossipRoundReport {
            attempted_worker_ids: Vec::new(),
            successful_worker_ids: Vec::new(),
            failed_worker_ids: Vec::new(),
            success_count: 0,
            failure_count: 0,
            remaining_failure_budget: failure_budget,
        };
        let mut local_rng_state = self.rng_state;

        while failure_budget > 0 {
            let candidate_indices = sample_random_unique_indices(
                self.nodes.len(),
                self.max_random_nodes_to_poll,
                &mut local_rng_state,
            );
            let candidate = candidate_indices
                .into_iter()
                .take(self.max_random_nodes_to_poll)
                .filter(|index| *index < self.nodes.len())
                .filter(|index| self.nodes[*index].last_gossip_tick < round_start_tick)
                .min_by_key(|index| self.nodes[*index].last_gossip_tick);

            let Some(candidate) = candidate else {
                break;
            };

            let node = &mut self.nodes[candidate];
            report.attempted_worker_ids.push(node.worker_id);
            match transport.try_gossip(node.worker_id).await {
                Ok(()) => {
                    node.last_gossip_tick = round_start_tick;
                    report.successful_worker_ids.push(node.worker_id);
                    report.success_count += 1;
                }
                Err(_) => {
                    node.last_gossip_tick = round_start_tick;
                    failure_budget -= 1;
                    report.failed_worker_ids.push(node.worker_id);
                    report.failure_count += 1;
                }
            }
        }

        report.remaining_failure_budget = failure_budget;
        self.rng_state = local_rng_state;
        report
    }
}

#[derive(Debug)]
pub struct GossipEngine<T: GossipTransport> {
    coordinator: GossipCoordinator,
    transport: T,
    sample_percent: usize,
    tick: GossipTick,
}

impl<T: GossipTransport> GossipEngine<T> {
    pub fn new(
        coordinator: GossipCoordinator,
        transport: T,
        sample_percent: usize,
        initial_tick: impl Into<GossipTick>,
    ) -> Self {
        Self {
            coordinator,
            transport,
            sample_percent,
            tick: initial_tick.into(),
        }
    }

    pub fn tick(&self) -> GossipTick {
        self.tick
    }

    pub fn coordinator(&self) -> &GossipCoordinator {
        &self.coordinator
    }

    pub fn run_once(&mut self) -> GossipRoundReport {
        self.tick = self.tick.increment();
        self.coordinator
            .run_round(self.sample_percent, self.tick, &mut self.transport)
    }

    pub fn run_for_rounds(&mut self, rounds: usize) -> Vec<GossipRoundReport> {
        (0..rounds).map(|_| self.run_once()).collect()
    }

    pub fn transport(&self) -> &T {
        &self.transport
    }
}

#[derive(Debug)]
pub struct AsyncGossipEngine<T: AsyncGossipTransport> {
    coordinator: GossipCoordinator,
    transport: T,
    sample_percent: usize,
    tick: GossipTick,
}

impl<T: AsyncGossipTransport> AsyncGossipEngine<T> {
    pub fn new(
        coordinator: GossipCoordinator,
        transport: T,
        sample_percent: usize,
        initial_tick: impl Into<GossipTick>,
    ) -> Self {
        Self {
            coordinator,
            transport,
            sample_percent,
            tick: initial_tick.into(),
        }
    }

    pub fn tick(&self) -> GossipTick {
        self.tick
    }

    pub fn coordinator(&self) -> &GossipCoordinator {
        &self.coordinator
    }

    pub fn transport(&self) -> &T {
        &self.transport
    }

    pub async fn run_once(&mut self) -> GossipRoundReport {
        self.tick = self.tick.increment();
        self.coordinator
            .run_round_async(self.sample_percent, self.tick, &mut self.transport)
            .await
    }

    pub async fn run_for_rounds(&mut self, rounds: usize) -> Vec<GossipRoundReport> {
        let mut reports = Vec::with_capacity(rounds);
        for _ in 0..rounds {
            reports.push(self.run_once().await);
        }
        reports
    }
}

#[derive(Debug)]
pub struct ClusterManager<T: AsyncGossipTransport> {
    engine: AsyncGossipEngine<T>,
    gossip_delay: Duration,
}

impl<T: AsyncGossipTransport> ClusterManager<T> {
    pub fn new(engine: AsyncGossipEngine<T>, gossip_delay: Duration) -> Self {
        Self {
            engine,
            gossip_delay,
        }
    }

    pub fn engine(&self) -> &AsyncGossipEngine<T> {
        &self.engine
    }

    pub fn engine_mut(&mut self) -> &mut AsyncGossipEngine<T> {
        &mut self.engine
    }

    pub async fn run_for_rounds(&mut self, rounds: usize) -> Vec<GossipRoundReport> {
        let mut interval = tokio::time::interval(self.gossip_delay);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        interval.tick().await;

        let mut reports = Vec::with_capacity(rounds);
        for _ in 0..rounds {
            interval.tick().await;
            reports.push(self.engine.run_once().await);
        }
        reports
    }

    pub async fn run_until_shutdown<F>(&mut self, shutdown: F) -> Vec<GossipRoundReport>
    where
        F: Future<Output = ()>,
    {
        let mut interval = tokio::time::interval(self.gossip_delay);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        interval.tick().await;

        let mut reports = Vec::new();
        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                _ = &mut shutdown => break,
                _ = interval.tick() => {
                    reports.push(self.engine.run_once().await);
                }
            }
        }
        reports
    }

    pub async fn run_with_config_updates<F>(
        &mut self,
        config_store: &ClusterConfigStore,
        mut updates: tokio::sync::mpsc::UnboundedReceiver<ClusterConfig>,
        shutdown: F,
    ) -> Vec<GossipRoundReport>
    where
        F: Future<Output = ()>,
    {
        let mut interval = tokio::time::interval(self.gossip_delay);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        interval.tick().await;

        let mut reports = Vec::new();
        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                _ = &mut shutdown => break,
                _ = interval.tick() => {
                    reports.push(self.engine.run_once().await);
                }
                update = updates.recv() => {
                    if let Some(incoming) = update {
                        let _ = config_store.merge_publish(&incoming);
                    }
                }
            }
        }
        reports
    }

    pub async fn run_with_config_updates_and_failover<F, R>(
        &mut self,
        config_store: &ClusterConfigStore,
        updates: tokio::sync::mpsc::UnboundedReceiver<ClusterConfig>,
        failure_detector: &mut FailureDetector,
        failover_controller: &mut ClusterFailoverController,
        replication_manager: &mut ReplicationManager,
        replication_transport: &mut R,
        shutdown: F,
    ) -> Result<Vec<GossipRoundReport>, FailoverControllerError<R::Error>>
    where
        F: Future<Output = ()>,
        R: AsyncReplicationTransport,
    {
        Ok(self
            .run_with_config_updates_and_failover_report(
                config_store,
                updates,
                failure_detector,
                failover_controller,
                replication_manager,
                replication_transport,
                shutdown,
            )
            .await?
            .gossip_reports)
    }

    pub async fn run_with_config_updates_and_failover_report<F, R>(
        &mut self,
        config_store: &ClusterConfigStore,
        mut updates: tokio::sync::mpsc::UnboundedReceiver<ClusterConfig>,
        failure_detector: &mut FailureDetector,
        failover_controller: &mut ClusterFailoverController,
        replication_manager: &mut ReplicationManager,
        replication_transport: &mut R,
        shutdown: F,
    ) -> Result<ClusterManagerFailoverRunReport, FailoverControllerError<R::Error>>
    where
        F: Future<Output = ()>,
        R: AsyncReplicationTransport,
    {
        let mut interval = tokio::time::interval(self.gossip_delay);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        interval.tick().await;

        let mut reports = Vec::new();
        let mut failover_records = Vec::new();
        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                _ = &mut shutdown => {
                    if reports.is_empty() {
                        let report = self
                            .run_failover_round(
                                config_store,
                                failure_detector,
                                failover_controller,
                                replication_manager,
                                replication_transport,
                                &mut failover_records,
                            )
                            .await?;
                        reports.push(report);
                    }
                    break;
                }
                _ = interval.tick() => {
                    let report = self
                        .run_failover_round(
                            config_store,
                            failure_detector,
                            failover_controller,
                            replication_manager,
                            replication_transport,
                            &mut failover_records,
                        )
                        .await?;
                    reports.push(report);
                }
                update = updates.recv() => {
                    if let Some(incoming) = update {
                        let _ = config_store.merge_publish(&incoming);
                    }
                }
            }
        }
        Ok(ClusterManagerFailoverRunReport {
            gossip_reports: reports,
            failover_records,
        })
    }

    async fn run_failover_round<R>(
        &mut self,
        config_store: &ClusterConfigStore,
        failure_detector: &mut FailureDetector,
        failover_controller: &mut ClusterFailoverController,
        replication_manager: &mut ReplicationManager,
        replication_transport: &mut R,
        failover_records: &mut Vec<FailoverExecutionRecord>,
    ) -> Result<GossipRoundReport, FailoverControllerError<R::Error>>
    where
        R: AsyncReplicationTransport,
    {
        let report = self.engine.run_once().await;
        let newly_failed_workers = failure_detector.record_report(&report);
        if !newly_failed_workers.is_empty() {
            let outcomes = failover_controller
                .handle_failed_workers_async(
                    config_store,
                    replication_manager,
                    &newly_failed_workers,
                    replication_transport,
                )
                .await?;
            for (failed_worker_id, outcome) in outcomes {
                if let Some(plan) = outcome.failover_plan {
                    failover_records.push(FailoverExecutionRecord {
                        failed_worker_id,
                        synchronized_replicas: outcome.synchronized_replicas,
                        plan,
                    });
                }
            }
        }
        Ok(report)
    }
}

fn sample_random_unique_indices(
    node_count: usize,
    max_count: usize,
    rng_state: &mut u64,
) -> Vec<usize> {
    if node_count == 0 || max_count == 0 {
        return Vec::new();
    }
    let target = node_count.min(max_count);
    let mut out = Vec::with_capacity(target);
    while out.len() < target {
        let index = (next_rng(rng_state) as usize) % node_count;
        if !out.contains(&index) {
            out.push(index);
        }
    }
    out
}

fn next_rng(state: &mut u64) -> u64 {
    // SplitMix64 step; fast and deterministic for sampling.
    *state = state.wrapping_add(0x9E3779B97F4A7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem::size_of;
    use std::time::SystemTime;
    use std::time::UNIX_EPOCH;
    use tokio::io::AsyncReadExt;

    fn base_config() -> ClusterConfig {
        ClusterConfig::new_local("local-node", "127.0.0.1", 6379)
    }

    fn unique_temp_dir(prefix: &str) -> std::path::PathBuf {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should be after epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "garnet-cluster-{prefix}-{}-{now}",
            std::process::id()
        ));
        std::fs::create_dir_all(&path).expect("temp directory should be creatable");
        path
    }

    #[test]
    fn hash_slot_layout_is_three_bytes() {
        assert_eq!(size_of::<HashSlot>(), 3);
    }

    #[test]
    fn initializes_reserved_and_local_workers() {
        let config = base_config();
        assert_eq!(config.workers().len(), 2);
        assert_eq!(
            config.workers()[RESERVED_WORKER_ID as usize].id,
            RESERVED_WORKER_ID
        );
        assert_eq!(
            config.workers()[LOCAL_WORKER_ID as usize].id,
            LOCAL_WORKER_ID
        );
        assert_eq!(config.local_worker().unwrap().role, WorkerRole::Primary);
    }

    #[test]
    fn set_slot_state_is_copy_on_write() {
        let config = base_config();
        let updated = config
            .set_slot_state(42, LOCAL_WORKER_ID, SlotState::Stable)
            .unwrap();
        assert_eq!(config.slot_state(42).unwrap(), SlotState::Offline);
        assert_eq!(updated.slot_state(42).unwrap(), SlotState::Stable);
        assert_eq!(updated.slot_owner(42).unwrap(), LOCAL_WORKER_ID);
    }

    #[test]
    fn slot_migration_helpers_set_expected_slot_states() {
        let config = base_config();
        let (config, remote_id) = config
            .add_worker(Worker::new("node-2", "10.0.0.2", 6380, WorkerRole::Primary))
            .unwrap();

        let migrating = config.begin_slot_migration_to(42, remote_id).unwrap();
        assert_eq!(migrating.slot_state(42).unwrap(), SlotState::Migrating);
        assert_eq!(migrating.slot_assigned_owner(42).unwrap(), remote_id);
        assert_eq!(migrating.slot_owner(42).unwrap(), LOCAL_WORKER_ID);

        let importing = config.begin_slot_import_from(42, remote_id).unwrap();
        assert_eq!(importing.slot_state(42).unwrap(), SlotState::Importing);
        assert_eq!(importing.slot_assigned_owner(42).unwrap(), remote_id);

        let finalized = migrating.finalize_slot_migration(42, remote_id).unwrap();
        assert_eq!(finalized.slot_state(42).unwrap(), SlotState::Stable);
        assert_eq!(finalized.slot_assigned_owner(42).unwrap(), remote_id);
        assert_eq!(finalized.slot_owner(42).unwrap(), remote_id);
    }

    #[test]
    fn slot_migration_helpers_reject_unknown_worker() {
        let config = base_config();
        assert!(matches!(
            config.begin_slot_migration_to(42, 99),
            Err(ClusterConfigError::WorkerNotFound(99))
        ));
        assert!(matches!(
            config.begin_slot_import_from(42, 99),
            Err(ClusterConfigError::WorkerNotFound(99))
        ));
        assert!(matches!(
            config.finalize_slot_migration(42, 99),
            Err(ClusterConfigError::WorkerNotFound(99))
        ));
    }

    #[test]
    fn slots_in_state_lists_matching_slots() {
        let config = base_config();
        let (config, remote_id) = config
            .add_worker(Worker::new("node-2", "10.0.0.2", 6380, WorkerRole::Primary))
            .unwrap();
        let config = config
            .set_slot_state(11, LOCAL_WORKER_ID, SlotState::Stable)
            .unwrap()
            .set_slot_state(12, remote_id, SlotState::Migrating)
            .unwrap()
            .set_slot_state(13, remote_id, SlotState::Importing)
            .unwrap();

        assert_eq!(config.slots_in_state(SlotState::Stable), vec![11]);
        assert_eq!(config.slots_in_state(SlotState::Migrating), vec![12]);
        assert_eq!(config.slots_in_state(SlotState::Importing), vec![13]);
    }

    #[test]
    fn slots_assigned_to_worker_in_state_filters_by_owner_and_state() {
        let config = base_config();
        let (config, remote_id) = config
            .add_worker(Worker::new("node-2", "10.0.0.2", 6380, WorkerRole::Primary))
            .unwrap();
        let config = config
            .set_slot_state(21, remote_id, SlotState::Migrating)
            .unwrap()
            .set_slot_state(22, remote_id, SlotState::Migrating)
            .unwrap()
            .set_slot_state(23, remote_id, SlotState::Stable)
            .unwrap()
            .set_slot_state(24, LOCAL_WORKER_ID, SlotState::Migrating)
            .unwrap();

        assert_eq!(
            config
                .slots_assigned_to_worker_in_state(remote_id, SlotState::Migrating)
                .unwrap(),
            vec![21, 22]
        );
    }

    #[test]
    fn slots_assigned_to_worker_in_state_rejects_unknown_worker() {
        let config = base_config();
        assert!(matches!(
            config.slots_assigned_to_worker_in_state(99, SlotState::Stable),
            Err(ClusterConfigError::WorkerNotFound(99))
        ));
    }

    #[test]
    fn migrating_slot_reports_local_owner() {
        let config = base_config();
        let remote = Worker::new("node-2", "10.0.0.2", 6380, WorkerRole::Primary);
        let (with_remote, remote_id) = config.add_worker(remote).unwrap();
        let updated = with_remote
            .set_slot_state(777, remote_id, SlotState::Migrating)
            .unwrap();
        assert_eq!(updated.slot_assigned_owner(777).unwrap(), remote_id);
        assert_eq!(updated.slot_owner(777).unwrap(), LOCAL_WORKER_ID);
        assert!(updated.is_local_slot(777).unwrap());
    }

    #[test]
    fn add_worker_does_not_mutate_original_config() {
        let config = base_config();
        let remote = Worker::new("node-2", "10.0.0.2", 6380, WorkerRole::Primary);
        let (updated, remote_id) = config.add_worker(remote).unwrap();
        assert_eq!(remote_id, 2);
        assert!(config.worker(remote_id).is_none());
        assert_eq!(updated.worker(remote_id).unwrap().node_id, "node-2");
    }

    #[test]
    fn takeover_slots_from_primary_reassigns_slots_to_local_primary() {
        let config = base_config()
            .set_local_worker_role(WorkerRole::Replica)
            .unwrap();
        let remote = Worker::new("node-2", "10.0.0.2", 6380, WorkerRole::Primary);
        let (with_remote, remote_id) = config.add_worker(remote).unwrap();
        let with_slots = with_remote
            .set_slot_state(100, remote_id, SlotState::Stable)
            .unwrap()
            .set_slot_state(101, remote_id, SlotState::Importing)
            .unwrap()
            .set_slot_state(102, LOCAL_WORKER_ID, SlotState::Stable)
            .unwrap();

        let updated = with_slots.take_over_slots_from_primary(remote_id).unwrap();
        assert_eq!(updated.slot_owner(100).unwrap(), LOCAL_WORKER_ID);
        assert_eq!(updated.slot_state(100).unwrap(), SlotState::Stable);
        assert_eq!(updated.slot_owner(101).unwrap(), LOCAL_WORKER_ID);
        assert_eq!(updated.slot_state(101).unwrap(), SlotState::Stable);
        assert_eq!(updated.slot_owner(102).unwrap(), LOCAL_WORKER_ID);
        assert_eq!(updated.local_worker().unwrap().role, WorkerRole::Primary);
    }

    #[test]
    fn takeover_slots_from_primary_rejects_unknown_worker() {
        let config = base_config();
        assert!(matches!(
            config.take_over_slots_from_primary(99),
            Err(ClusterConfigError::WorkerNotFound(99))
        ));
    }

    #[test]
    fn apply_failover_plan_reassigns_slots_and_updates_roles() {
        let config = base_config();
        let (config, failed_primary_id) = config
            .add_worker(Worker::new("node-2", "10.0.0.2", 6380, WorkerRole::Primary))
            .unwrap();
        let (config, promoted_replica_id) = config
            .add_worker(Worker::new(
                "replica-a",
                "10.0.0.3",
                6381,
                WorkerRole::Replica,
            ))
            .unwrap();
        let (config, follower_replica_id) = config
            .add_worker(Worker::new(
                "replica-b",
                "10.0.0.4",
                6382,
                WorkerRole::Replica,
            ))
            .unwrap();
        let config = config
            .set_worker_replica_of(promoted_replica_id, "node-2")
            .unwrap()
            .set_worker_replica_of(follower_replica_id, "node-2")
            .unwrap()
            .set_worker_config_epoch(failed_primary_id, ConfigEpoch::new(3))
            .unwrap()
            .set_worker_config_epoch(promoted_replica_id, ConfigEpoch::new(2))
            .unwrap()
            .set_worker_config_epoch(follower_replica_id, ConfigEpoch::new(1))
            .unwrap()
            .set_slot_state(300, failed_primary_id, SlotState::Stable)
            .unwrap()
            .set_slot_state(301, failed_primary_id, SlotState::Importing)
            .unwrap()
            .set_slot_state(302, LOCAL_WORKER_ID, SlotState::Stable)
            .unwrap();

        let updated = config
            .apply_failover_plan(&FailoverPlan {
                failed_primary_worker_id: failed_primary_id,
                promoted_worker_id: promoted_replica_id,
                promoted_replication_offset: ReplicationOffset::new(888),
            })
            .unwrap();

        assert_eq!(
            updated.slot_assigned_owner(300).unwrap(),
            promoted_replica_id
        );
        assert_eq!(
            updated.slot_assigned_owner(301).unwrap(),
            promoted_replica_id
        );
        assert_eq!(updated.slot_state(300).unwrap(), SlotState::Stable);
        assert_eq!(updated.slot_state(301).unwrap(), SlotState::Stable);
        assert_eq!(updated.slot_assigned_owner(302).unwrap(), LOCAL_WORKER_ID);

        let promoted = updated.worker(promoted_replica_id).unwrap();
        assert_eq!(promoted.role, WorkerRole::Primary);
        assert_eq!(promoted.replica_of_node_id, None);
        assert_eq!(promoted.replication_offset, 888);
        assert!(promoted.config_epoch > ConfigEpoch::new(3));

        let failed = updated.worker(failed_primary_id).unwrap();
        assert_eq!(failed.role, WorkerRole::Replica);
        assert_eq!(failed.replica_of_node_id.as_deref(), Some("replica-a"));

        let follower = updated.worker(follower_replica_id).unwrap();
        assert_eq!(follower.role, WorkerRole::Replica);
        assert_eq!(follower.replica_of_node_id.as_deref(), Some("replica-a"));
    }

    #[test]
    fn apply_failover_plan_rejects_unknown_worker_ids() {
        let config = base_config();
        let result = config.apply_failover_plan(&FailoverPlan {
            failed_primary_worker_id: 99,
            promoted_worker_id: LOCAL_WORKER_ID,
            promoted_replication_offset: ReplicationOffset::new(0),
        });
        assert!(matches!(
            result,
            Err(ClusterConfigError::WorkerNotFound(99))
        ));
    }

    #[test]
    fn config_store_publish_keeps_snapshot_isolation() {
        let config = base_config();
        let store = ClusterConfigStore::new(config.clone());
        let before = store.load();
        let next = config
            .set_slot_state(11, LOCAL_WORKER_ID, SlotState::Stable)
            .unwrap();

        let previous = store.publish(next);
        let after = store.load();

        assert_eq!(before.slot_state(11).unwrap(), SlotState::Offline);
        assert_eq!(previous.slot_state(11).unwrap(), SlotState::Offline);
        assert_eq!(after.slot_state(11).unwrap(), SlotState::Stable);
    }

    #[test]
    fn merge_from_prefers_higher_epoch_slot_claims() {
        let base = base_config();
        let worker2 = Worker::new("node-b", "10.0.0.2", 6380, WorkerRole::Primary);
        let (base, worker2_id) = base.add_worker(worker2).unwrap();
        let worker3 = Worker::new("node-c", "10.0.0.3", 6381, WorkerRole::Primary);
        let (base, worker3_id) = base.add_worker(worker3).unwrap();
        let base = base
            .set_worker_config_epoch(worker2_id, ConfigEpoch::new(1))
            .unwrap()
            .set_worker_config_epoch(worker3_id, ConfigEpoch::new(1))
            .unwrap()
            .set_slot_state(200, worker2_id, SlotState::Stable)
            .unwrap();

        let incoming = base
            .set_worker_config_epoch(worker3_id, ConfigEpoch::new(2))
            .unwrap()
            .set_slot_state(200, worker3_id, SlotState::Stable)
            .unwrap();
        let merged = base.merge_from(&incoming);

        assert_eq!(merged.slot_assigned_owner(200).unwrap(), worker3_id);
    }

    #[test]
    fn merge_from_breaks_epoch_ties_by_node_id() {
        let base = base_config();
        let worker2 = Worker::new("node-z", "10.0.0.2", 6380, WorkerRole::Primary);
        let (base, worker2_id) = base.add_worker(worker2).unwrap();
        let worker3 = Worker::new("node-a", "10.0.0.3", 6381, WorkerRole::Primary);
        let (base, worker3_id) = base.add_worker(worker3).unwrap();
        let base = base
            .set_worker_config_epoch(worker2_id, ConfigEpoch::new(7))
            .unwrap()
            .set_worker_config_epoch(worker3_id, ConfigEpoch::new(7))
            .unwrap()
            .set_slot_state(201, worker2_id, SlotState::Stable)
            .unwrap();

        let incoming = base
            .set_slot_state(201, worker3_id, SlotState::Stable)
            .unwrap();
        let merged = base.merge_from(&incoming);

        assert_eq!(merged.slot_assigned_owner(201).unwrap(), worker3_id);
    }

    #[test]
    fn merge_from_includes_workers_from_incoming_snapshot() {
        let base = base_config();
        let worker2 = Worker::new("node-b", "10.0.0.2", 6380, WorkerRole::Primary);
        let (base, _) = base.add_worker(worker2).unwrap();

        let worker3 = Worker::new("node-c", "10.0.0.3", 6381, WorkerRole::Primary);
        let (incoming, worker3_id) = base.add_worker(worker3).unwrap();
        let merged = base.merge_from(&incoming);

        assert_eq!(merged.worker(worker3_id).unwrap().node_id, "node-c");
    }

    #[test]
    fn config_store_merge_publish_applies_merged_slot_assignment() {
        let base = base_config();
        let worker2 = Worker::new("node-b", "10.0.0.2", 6380, WorkerRole::Primary);
        let (base, worker2_id) = base.add_worker(worker2).unwrap();
        let worker3 = Worker::new("node-c", "10.0.0.3", 6381, WorkerRole::Primary);
        let (base, worker3_id) = base.add_worker(worker3).unwrap();

        let current = base
            .set_worker_config_epoch(worker2_id, ConfigEpoch::new(1))
            .unwrap()
            .set_slot_state(202, worker2_id, SlotState::Stable)
            .unwrap();
        let incoming = base
            .set_worker_config_epoch(worker3_id, ConfigEpoch::new(2))
            .unwrap()
            .set_slot_state(202, worker3_id, SlotState::Stable)
            .unwrap();

        let store = ClusterConfigStore::new(current);
        let merged = store.merge_publish(&incoming);
        assert_eq!(merged.slot_assigned_owner(202).unwrap(), worker3_id);
    }

    #[test]
    fn cluster_config_snapshot_codec_roundtrip_preserves_workers_and_slots() {
        let base = base_config();
        let (base, worker2_id) = base
            .add_worker(Worker::new("node-b", "10.0.0.2", 6380, WorkerRole::Primary))
            .unwrap();
        let (base, worker3_id) = base
            .add_worker(Worker::new("node-c", "10.0.0.3", 6381, WorkerRole::Replica))
            .unwrap();
        let original = base
            .set_worker_config_epoch(worker2_id, ConfigEpoch::new(5))
            .unwrap()
            .set_worker_replica_of(worker3_id, "node-b")
            .unwrap()
            .set_worker_replication_offset(worker3_id, ReplicationOffset::new(1_234))
            .unwrap()
            .set_slot_state(101, worker2_id, SlotState::Stable)
            .unwrap()
            .set_slot_state(102, worker3_id, SlotState::Importing)
            .unwrap();

        let encoded = encode_cluster_config_snapshot(&original).unwrap();
        let decoded = decode_cluster_config_snapshot(&encoded).unwrap();

        assert_eq!(decoded.slot_assigned_owner(101).unwrap(), worker2_id);
        assert_eq!(decoded.slot_state(101).unwrap(), SlotState::Stable);
        assert_eq!(decoded.slot_assigned_owner(102).unwrap(), worker3_id);
        assert_eq!(decoded.slot_state(102).unwrap(), SlotState::Importing);
        assert_eq!(
            decoded.worker(worker2_id).unwrap().config_epoch,
            ConfigEpoch::new(5)
        );
        assert_eq!(
            decoded.worker(worker3_id).unwrap().role,
            WorkerRole::Replica
        );
        assert_eq!(
            decoded
                .worker(worker3_id)
                .unwrap()
                .replica_of_node_id
                .as_deref(),
            Some("node-b")
        );
        assert_eq!(
            decoded.worker(worker3_id).unwrap().replication_offset,
            1_234
        );
    }

    #[test]
    fn replication_manager_selects_incremental_when_replica_offset_is_in_window() {
        let manager = ReplicationManager::new(
            Some(99),
            ReplicationOffset::new(1_000),
            ReplicationOffset::new(2_000),
        )
        .unwrap();
        let plan = manager.plan_sync(Some(1_500));
        assert_eq!(plan.mode, ReplicationSyncMode::Incremental);
        assert_eq!(plan.checkpoint_id, None);
        assert_eq!(plan.aof_start_offset, 1_500);
    }

    #[test]
    fn replication_manager_selects_full_when_replica_offset_is_stale() {
        let manager = ReplicationManager::new(
            Some(55),
            ReplicationOffset::new(5_000),
            ReplicationOffset::new(8_000),
        )
        .unwrap();
        let plan = manager.plan_sync(Some(4_999));
        assert_eq!(plan.mode, ReplicationSyncMode::Full);
        assert_eq!(plan.checkpoint_id, Some(55));
        assert_eq!(plan.aof_start_offset, 5_000);
    }

    #[test]
    fn replication_manager_selects_full_when_replica_offset_is_unknown() {
        let manager = ReplicationManager::new(
            Some(12),
            ReplicationOffset::new(100),
            ReplicationOffset::new(200),
        )
        .unwrap();
        let plan = manager.plan_sync(None);
        assert_eq!(plan.mode, ReplicationSyncMode::Full);
        assert_eq!(plan.checkpoint_id, Some(12));
        assert_eq!(plan.aof_start_offset, 100);
    }

    #[test]
    fn replication_manager_rejects_invalid_recovery_window() {
        let result = ReplicationManager::new(
            Some(1),
            ReplicationOffset::new(10),
            ReplicationOffset::new(9),
        );
        assert!(matches!(
            result,
            Err(ReplicationError::InvalidAofWindow {
                replay_start_offset: ReplicationOffset::new(10),
                tail_offset: ReplicationOffset::new(9),
            })
        ));
    }

    #[test]
    fn replication_manager_tracks_best_replica_by_highest_offset_then_lowest_id() {
        let mut manager = ReplicationManager::new(
            None,
            ReplicationOffset::new(0),
            ReplicationOffset::new(10_000),
        )
        .unwrap();
        manager.record_replica_offset(7, ReplicationOffset::new(8_000));
        manager.record_replica_offset(2, ReplicationOffset::new(9_000));
        manager.record_replica_offset(5, ReplicationOffset::new(9_000));

        assert_eq!(
            manager.best_replica_candidate(),
            Some(ReplicaProgress {
                worker_id: 2,
                acknowledged_offset: 9_000,
            })
        );
    }

    #[test]
    fn replication_manager_updates_recovery_window_and_tail() {
        let mut manager = ReplicationManager::new(
            Some(1),
            ReplicationOffset::new(100),
            ReplicationOffset::new(200),
        )
        .unwrap();
        manager
            .update_recovery_window(Some(CheckpointId::new(2)), 150, 400)
            .expect("window update should succeed");
        manager
            .set_aof_tail_offset(ReplicationOffset::new(450))
            .expect("tail advance should succeed");

        assert_eq!(manager.checkpoint_id(), Some(CheckpointId::new(2)));
        assert_eq!(manager.aof_replay_start_offset(), 150);
        assert_eq!(manager.aof_tail_offset(), 450);
    }

    #[derive(Default)]
    struct MockReplicationTransport {
        checkpoints: Vec<(u16, u64)>,
        streams: Vec<(u16, u64)>,
        stream_result: u64,
        fail_checkpoint: bool,
        fail_stream: bool,
    }

    impl ReplicationTransport for MockReplicationTransport {
        type Error = &'static str;

        fn send_checkpoint(
            &mut self,
            worker_id: u16,
            checkpoint_id: u64,
        ) -> Result<(), Self::Error> {
            if self.fail_checkpoint {
                return Err("checkpoint failed");
            }
            self.checkpoints.push((worker_id, checkpoint_id));
            Ok(())
        }

        fn stream_aof_from_offset(
            &mut self,
            worker_id: u16,
            start_offset: u64,
        ) -> Result<u64, Self::Error> {
            if self.fail_stream {
                return Err("stream failed");
            }
            self.streams.push((worker_id, start_offset));
            Ok(self.stream_result)
        }
    }

    #[test]
    fn replication_manager_execute_sync_uses_incremental_plan_without_checkpoint() {
        let mut manager = ReplicationManager::new(
            Some(7),
            ReplicationOffset::new(1_000),
            ReplicationOffset::new(2_000),
        )
        .unwrap();
        let mut transport = MockReplicationTransport {
            stream_result: 1_750,
            ..Default::default()
        };

        let outcome = manager
            .execute_sync(3, Some(1_500), &mut transport)
            .expect("incremental sync should succeed");
        assert_eq!(outcome.plan.mode, ReplicationSyncMode::Incremental);
        assert!(transport.checkpoints.is_empty());
        assert_eq!(transport.streams, vec![(3, 1_500)]);
        assert_eq!(manager.replica_offset(3), Some(1_750));
    }

    #[test]
    fn replication_manager_execute_sync_sends_checkpoint_for_full_sync() {
        let mut manager = ReplicationManager::new(
            Some(9),
            ReplicationOffset::new(500),
            ReplicationOffset::new(900),
        )
        .unwrap();
        let mut transport = MockReplicationTransport {
            stream_result: 900,
            ..Default::default()
        };

        let outcome = manager
            .execute_sync(4, Some(400), &mut transport)
            .expect("full sync should succeed");
        assert_eq!(outcome.plan.mode, ReplicationSyncMode::Full);
        assert_eq!(transport.checkpoints, vec![(4, 9)]);
        assert_eq!(transport.streams, vec![(4, 500)]);
        assert_eq!(manager.replica_offset(4), Some(900));
    }

    #[test]
    fn replication_manager_execute_sync_errors_when_checkpoint_missing_for_full_sync() {
        let mut manager = ReplicationManager::new(
            None,
            ReplicationOffset::new(1_000),
            ReplicationOffset::new(2_000),
        )
        .unwrap();
        let mut transport = MockReplicationTransport {
            stream_result: 2_000,
            ..Default::default()
        };

        let result = manager.execute_sync(8, Some(999), &mut transport);
        assert!(matches!(
            result,
            Err(ReplicationSyncError::Replication(
                ReplicationError::MissingCheckpointForFullSync
            ))
        ));
        assert!(transport.streams.is_empty());
    }

    #[test]
    fn replication_manager_execute_sync_propagates_transport_errors() {
        let mut manager = ReplicationManager::new(
            Some(1),
            ReplicationOffset::new(10),
            ReplicationOffset::new(20),
        )
        .unwrap();
        let mut transport = MockReplicationTransport {
            fail_stream: true,
            ..Default::default()
        };

        let result = manager.execute_sync(2, Some(15), &mut transport);
        assert!(matches!(
            result,
            Err(ReplicationSyncError::Transport("stream failed"))
        ));
    }

    #[test]
    fn replication_manager_execute_sync_for_worker_reuses_tracked_offset() {
        let mut manager = ReplicationManager::new(
            Some(9),
            ReplicationOffset::new(500),
            ReplicationOffset::new(900),
        )
        .unwrap();
        let mut transport = MockReplicationTransport {
            stream_result: 900,
            ..Default::default()
        };

        let first = manager
            .execute_sync_for_worker(4, &mut transport)
            .expect("first sync should succeed");
        assert_eq!(first.plan.mode, ReplicationSyncMode::Full);
        assert_eq!(transport.checkpoints, vec![(4, 9)]);
        assert_eq!(transport.streams, vec![(4, 500)]);

        manager
            .set_aof_tail_offset(ReplicationOffset::new(940))
            .unwrap();
        transport.checkpoints.clear();
        transport.streams.clear();
        transport.stream_result = 940;

        let second = manager
            .execute_sync_for_worker(4, &mut transport)
            .expect("second sync should succeed");
        assert_eq!(second.plan.mode, ReplicationSyncMode::Incremental);
        assert!(transport.checkpoints.is_empty());
        assert_eq!(transport.streams, vec![(4, 900)]);
        assert_eq!(manager.replica_offset(4), Some(940));
    }

    #[test]
    fn replication_manager_sync_replicas_of_primary_targets_matching_replicas() {
        let config = base_config();
        let (config, replica_a) = config
            .add_worker(Worker::new(
                "replica-a",
                "10.0.0.2",
                6380,
                WorkerRole::Replica,
            ))
            .unwrap();
        let (config, replica_b) = config
            .add_worker(Worker::new(
                "replica-b",
                "10.0.0.3",
                6381,
                WorkerRole::Replica,
            ))
            .unwrap();
        let (config, replica_other) = config
            .add_worker(Worker::new(
                "replica-other",
                "10.0.0.4",
                6382,
                WorkerRole::Replica,
            ))
            .unwrap();
        let config = config
            .set_worker_replica_of(replica_a, "local-node")
            .unwrap()
            .set_worker_replica_of(replica_b, "local-node")
            .unwrap()
            .set_worker_replica_of(replica_other, "different-primary")
            .unwrap();

        let mut manager = ReplicationManager::new(
            Some(9),
            ReplicationOffset::new(500),
            ReplicationOffset::new(900),
        )
        .unwrap();
        let mut transport = MockReplicationTransport {
            stream_result: 900,
            ..Default::default()
        };
        let outcomes = manager
            .sync_replicas_of_primary(&config, "local-node", &mut transport)
            .expect("matching replicas should sync");

        assert_eq!(outcomes.len(), 2);
        assert_eq!(outcomes[0].0, replica_a);
        assert_eq!(outcomes[1].0, replica_b);
        assert_eq!(transport.checkpoints, vec![(replica_a, 9), (replica_b, 9)]);
        assert_eq!(transport.streams, vec![(replica_a, 500), (replica_b, 500)]);
        assert_eq!(manager.replica_offset(replica_a), Some(900));
        assert_eq!(manager.replica_offset(replica_b), Some(900));
        assert_eq!(manager.replica_offset(replica_other), None);
    }

    #[derive(Default)]
    struct AsyncMockReplicationTransport {
        checkpoints: Vec<(u16, u64)>,
        streams: Vec<(u16, u64)>,
        stream_result: u64,
        fail_checkpoint: bool,
        fail_stream: bool,
    }

    impl AsyncReplicationTransport for AsyncMockReplicationTransport {
        type Error = &'static str;
        type CheckpointFut<'a>
            = std::future::Ready<Result<(), Self::Error>>
        where
            Self: 'a;
        type StreamFut<'a>
            = std::future::Ready<Result<u64, Self::Error>>
        where
            Self: 'a;

        fn send_checkpoint<'a>(
            &'a mut self,
            worker_id: u16,
            checkpoint_id: u64,
        ) -> Self::CheckpointFut<'a> {
            std::future::ready(if self.fail_checkpoint {
                Err("checkpoint failed")
            } else {
                self.checkpoints.push((worker_id, checkpoint_id));
                Ok(())
            })
        }

        fn stream_aof_from_offset<'a>(
            &'a mut self,
            worker_id: u16,
            start_offset: u64,
        ) -> Self::StreamFut<'a> {
            std::future::ready(if self.fail_stream {
                Err("stream failed")
            } else {
                self.streams.push((worker_id, start_offset));
                Ok(self.stream_result)
            })
        }
    }

    #[tokio::test]
    async fn replication_manager_execute_sync_async_uses_incremental_plan_without_checkpoint() {
        let mut manager = ReplicationManager::new(
            Some(7),
            ReplicationOffset::new(1_000),
            ReplicationOffset::new(2_000),
        )
        .unwrap();
        let mut transport = AsyncMockReplicationTransport {
            stream_result: 1_750,
            ..Default::default()
        };

        let outcome = manager
            .execute_sync_async(3, Some(1_500), &mut transport)
            .await
            .expect("incremental sync should succeed");
        assert_eq!(outcome.plan.mode, ReplicationSyncMode::Incremental);
        assert!(transport.checkpoints.is_empty());
        assert_eq!(transport.streams, vec![(3, 1_500)]);
        assert_eq!(manager.replica_offset(3), Some(1_750));
    }

    #[tokio::test]
    async fn replication_manager_execute_sync_async_sends_checkpoint_for_full_sync() {
        let mut manager = ReplicationManager::new(
            Some(9),
            ReplicationOffset::new(500),
            ReplicationOffset::new(900),
        )
        .unwrap();
        let mut transport = AsyncMockReplicationTransport {
            stream_result: 900,
            ..Default::default()
        };

        let outcome = manager
            .execute_sync_async(4, Some(400), &mut transport)
            .await
            .expect("full sync should succeed");
        assert_eq!(outcome.plan.mode, ReplicationSyncMode::Full);
        assert_eq!(transport.checkpoints, vec![(4, 9)]);
        assert_eq!(transport.streams, vec![(4, 500)]);
        assert_eq!(manager.replica_offset(4), Some(900));
    }

    #[tokio::test]
    async fn replication_manager_execute_sync_async_errors_when_checkpoint_missing_for_full_sync() {
        let mut manager = ReplicationManager::new(
            None,
            ReplicationOffset::new(1_000),
            ReplicationOffset::new(2_000),
        )
        .unwrap();
        let mut transport = AsyncMockReplicationTransport {
            stream_result: 2_000,
            ..Default::default()
        };

        let result = manager
            .execute_sync_async(8, Some(999), &mut transport)
            .await;
        assert!(matches!(
            result,
            Err(ReplicationSyncError::Replication(
                ReplicationError::MissingCheckpointForFullSync
            ))
        ));
        assert!(transport.streams.is_empty());
    }

    #[tokio::test]
    async fn replication_manager_execute_sync_async_propagates_transport_errors() {
        let mut manager = ReplicationManager::new(
            Some(1),
            ReplicationOffset::new(10),
            ReplicationOffset::new(20),
        )
        .unwrap();
        let mut transport = AsyncMockReplicationTransport {
            fail_stream: true,
            ..Default::default()
        };

        let result = manager
            .execute_sync_async(2, Some(15), &mut transport)
            .await;
        assert!(matches!(
            result,
            Err(ReplicationSyncError::Transport("stream failed"))
        ));
    }

    #[tokio::test]
    async fn channel_replication_transport_emits_full_sync_events_in_order() {
        let mut manager = ReplicationManager::new(
            Some(42),
            ReplicationOffset::new(1_000),
            ReplicationOffset::new(2_000),
        )
        .unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let mut transport = ChannelReplicationTransport::new(tx, ReplicationOffset::new(1_750));

        let outcome = manager
            .execute_sync_async(9, Some(999), &mut transport)
            .await
            .expect("full sync should succeed");
        assert_eq!(outcome.plan.mode, ReplicationSyncMode::Full);
        assert_eq!(outcome.streamed_until_offset, 1_750);

        assert_eq!(
            rx.recv().await,
            Some(ReplicationEvent::Checkpoint {
                worker_id: 9,
                checkpoint_id: CheckpointId::new(42),
            })
        );
        assert_eq!(
            rx.recv().await,
            Some(ReplicationEvent::StreamAof {
                worker_id: 9,
                start_offset: 1_000,
            })
        );
        assert_eq!(manager.replica_offset(9), Some(1_750));
    }

    #[tokio::test]
    async fn channel_replication_transport_emits_incremental_stream_only() {
        let mut manager = ReplicationManager::new(
            Some(42),
            ReplicationOffset::new(1_000),
            ReplicationOffset::new(2_000),
        )
        .unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let mut transport = ChannelReplicationTransport::new(tx, ReplicationOffset::new(1_980));

        let outcome = manager
            .execute_sync_async(5, Some(1_500), &mut transport)
            .await
            .expect("incremental sync should succeed");
        assert_eq!(outcome.plan.mode, ReplicationSyncMode::Incremental);

        assert_eq!(
            rx.recv().await,
            Some(ReplicationEvent::StreamAof {
                worker_id: 5,
                start_offset: 1_500,
            })
        );
        assert_eq!(manager.replica_offset(5), Some(1_980));
    }

    #[tokio::test]
    async fn channel_replication_transport_propagates_closed_channel_error() {
        let mut manager = ReplicationManager::new(
            Some(42),
            ReplicationOffset::new(1_000),
            ReplicationOffset::new(2_000),
        )
        .unwrap();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<ReplicationEvent>();
        drop(rx);
        let mut transport = ChannelReplicationTransport::new(tx, ReplicationOffset::new(1_980));

        let result = manager
            .execute_sync_async(5, Some(1_500), &mut transport)
            .await;
        assert!(matches!(
            result,
            Err(ReplicationSyncError::Transport(
                ChannelReplicationTransportError::ChannelClosed
            ))
        ));
    }

    #[test]
    fn file_replication_transport_writes_checkpoint_and_stream_offsets() {
        let dir = unique_temp_dir("file-transport-sync");
        let mut manager = ReplicationManager::new(
            Some(9),
            ReplicationOffset::new(500),
            ReplicationOffset::new(900),
        )
        .unwrap();
        let mut transport = FileReplicationTransport::new(&dir, ReplicationOffset::new(900));

        let outcome = manager
            .execute_sync(4, Some(400), &mut transport)
            .expect("full sync should succeed");
        assert_eq!(outcome.plan.mode, ReplicationSyncMode::Full);
        assert_eq!(outcome.streamed_until_offset, 900);

        let checkpoint =
            std::fs::read_to_string(dir.join("worker-4.checkpoint")).expect("checkpoint exists");
        assert_eq!(checkpoint, "9\n");
        let aof_log = std::fs::read_to_string(dir.join("worker-4.aof")).expect("aof log exists");
        assert_eq!(aof_log, "500\n");

        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn file_replication_transport_supports_async_full_then_incremental_sync() {
        let dir = unique_temp_dir("file-transport-async");
        let mut manager = ReplicationManager::new(
            Some(11),
            ReplicationOffset::new(700),
            ReplicationOffset::new(900),
        )
        .unwrap();
        let mut transport = FileReplicationTransport::new(&dir, ReplicationOffset::new(900));

        let first = manager
            .execute_sync_for_worker_async(6, &mut transport)
            .await
            .expect("first sync should succeed");
        assert_eq!(first.plan.mode, ReplicationSyncMode::Full);
        assert_eq!(first.plan.aof_start_offset, 700);

        manager
            .set_aof_tail_offset(ReplicationOffset::new(960))
            .unwrap();
        transport.set_stream_result(ReplicationOffset::new(960));
        let second = manager
            .execute_sync_for_worker_async(6, &mut transport)
            .await
            .expect("second sync should succeed");
        assert_eq!(second.plan.mode, ReplicationSyncMode::Incremental);
        assert_eq!(second.plan.aof_start_offset, 900);

        let checkpoint =
            std::fs::read_to_string(dir.join("worker-6.checkpoint")).expect("checkpoint exists");
        assert_eq!(checkpoint, "11\n");
        let aof_log = std::fs::read_to_string(dir.join("worker-6.aof")).expect("aof log exists");
        assert_eq!(aof_log, "700\n900\n");

        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn tcp_replication_transport_emits_checkpoint_and_aof_messages() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener
            .local_addr()
            .expect("listener should expose address");
        let receiver = tokio::spawn(async move {
            let mut payloads = Vec::new();
            for _ in 0..2 {
                let (mut socket, _) = listener.accept().await.expect("accept should succeed");
                let mut buf = Vec::new();
                socket
                    .read_to_end(&mut buf)
                    .await
                    .expect("read should succeed");
                payloads.push(String::from_utf8(buf).expect("payload should be utf8"));
            }
            payloads
        });

        let mut manager = ReplicationManager::new(
            Some(9),
            ReplicationOffset::new(500),
            ReplicationOffset::new(900),
        )
        .unwrap();
        let mut transport = TcpReplicationTransport::new(ReplicationOffset::new(900));
        transport.add_peer(4, addr);

        let outcome = manager
            .execute_sync_async(4, Some(400), &mut transport)
            .await
            .expect("sync should succeed");
        assert_eq!(outcome.plan.mode, ReplicationSyncMode::Full);
        assert_eq!(outcome.streamed_until_offset, 900);

        let payloads = receiver.await.expect("receiver task should succeed");
        assert_eq!(
            payloads,
            vec!["CHECKPOINT 9\n".to_string(), "AOF 500\n".to_string()]
        );
    }

    #[tokio::test]
    async fn tcp_replication_transport_reports_unknown_peer() {
        let mut manager = ReplicationManager::new(
            Some(9),
            ReplicationOffset::new(500),
            ReplicationOffset::new(900),
        )
        .unwrap();
        let mut transport = TcpReplicationTransport::new(ReplicationOffset::new(900));

        let result = manager
            .execute_sync_async(4, Some(400), &mut transport)
            .await;
        assert!(matches!(
            result,
            Err(ReplicationSyncError::Transport(
                TcpReplicationTransportError::UnknownPeer(4)
            ))
        ));
    }

    #[tokio::test]
    async fn replication_manager_execute_sync_for_worker_async_reuses_tracked_offset() {
        let mut manager = ReplicationManager::new(
            Some(42),
            ReplicationOffset::new(1_000),
            ReplicationOffset::new(2_000),
        )
        .unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let mut transport = ChannelReplicationTransport::new(tx, ReplicationOffset::new(2_000));

        let first = manager
            .execute_sync_for_worker_async(9, &mut transport)
            .await
            .expect("first sync should succeed");
        assert_eq!(first.plan.mode, ReplicationSyncMode::Full);
        assert_eq!(
            rx.recv().await,
            Some(ReplicationEvent::Checkpoint {
                worker_id: 9,
                checkpoint_id: CheckpointId::new(42),
            })
        );
        assert_eq!(
            rx.recv().await,
            Some(ReplicationEvent::StreamAof {
                worker_id: 9,
                start_offset: 1_000,
            })
        );

        manager
            .set_aof_tail_offset(ReplicationOffset::new(2_100))
            .unwrap();
        transport.set_stream_result(ReplicationOffset::new(2_100));
        let second = manager
            .execute_sync_for_worker_async(9, &mut transport)
            .await
            .expect("second sync should succeed");
        assert_eq!(second.plan.mode, ReplicationSyncMode::Incremental);
        assert_eq!(
            rx.recv().await,
            Some(ReplicationEvent::StreamAof {
                worker_id: 9,
                start_offset: 2_000,
            })
        );
        assert_eq!(manager.replica_offset(9), Some(2_100));
    }

    #[tokio::test]
    async fn replication_manager_sync_replicas_of_primary_async_reuses_tracked_offsets() {
        let config = base_config();
        let (config, replica_a) = config
            .add_worker(Worker::new(
                "replica-a",
                "10.0.0.2",
                6380,
                WorkerRole::Replica,
            ))
            .unwrap();
        let (config, replica_b) = config
            .add_worker(Worker::new(
                "replica-b",
                "10.0.0.3",
                6381,
                WorkerRole::Replica,
            ))
            .unwrap();
        let config = config
            .set_worker_replica_of(replica_a, "local-node")
            .unwrap()
            .set_worker_replica_of(replica_b, "local-node")
            .unwrap();

        let mut manager = ReplicationManager::new(
            Some(11),
            ReplicationOffset::new(700),
            ReplicationOffset::new(900),
        )
        .unwrap();
        let mut transport = AsyncMockReplicationTransport {
            stream_result: 900,
            ..Default::default()
        };
        let first = manager
            .sync_replicas_of_primary_async(&config, "local-node", &mut transport)
            .await
            .expect("initial full sync should succeed");
        assert_eq!(first.len(), 2);
        assert_eq!(
            transport.checkpoints,
            vec![(replica_a, 11), (replica_b, 11)]
        );
        assert_eq!(transport.streams, vec![(replica_a, 700), (replica_b, 700)]);

        manager
            .set_aof_tail_offset(ReplicationOffset::new(960))
            .unwrap();
        transport.checkpoints.clear();
        transport.streams.clear();
        transport.stream_result = 960;
        let second = manager
            .sync_replicas_of_primary_async(&config, "local-node", &mut transport)
            .await
            .expect("follow-up incremental sync should succeed");
        assert_eq!(second.len(), 2);
        assert!(
            second
                .iter()
                .all(|(_, outcome)| outcome.plan.mode == ReplicationSyncMode::Incremental)
        );
        assert!(transport.checkpoints.is_empty());
        assert_eq!(transport.streams, vec![(replica_a, 900), (replica_b, 900)]);
        assert_eq!(manager.replica_offset(replica_a), Some(960));
        assert_eq!(manager.replica_offset(replica_b), Some(960));
    }

    #[test]
    fn failover_plan_selects_replica_with_highest_runtime_offset() {
        let config = base_config();
        let (config, replica_a) = config
            .add_worker(Worker::new(
                "replica-a",
                "10.0.0.2",
                6380,
                WorkerRole::Replica,
            ))
            .unwrap();
        let (config, replica_b) = config
            .add_worker(Worker::new(
                "replica-b",
                "10.0.0.3",
                6381,
                WorkerRole::Replica,
            ))
            .unwrap();
        let config = config
            .set_worker_replica_of(replica_a, "local-node")
            .unwrap()
            .set_worker_replica_of(replica_b, "local-node")
            .unwrap();

        let mut manager = ReplicationManager::new(
            Some(1),
            ReplicationOffset::new(100),
            ReplicationOffset::new(200),
        )
        .unwrap();
        manager.record_replica_offset(replica_a, ReplicationOffset::new(150));
        manager.record_replica_offset(replica_b, ReplicationOffset::new(180));

        let plan = manager.plan_failover(&config, "local-node").unwrap();
        assert_eq!(plan.failed_primary_worker_id, LOCAL_WORKER_ID);
        assert_eq!(plan.promoted_worker_id, replica_b);
        assert_eq!(plan.promoted_replication_offset, 180);
    }

    #[test]
    fn failover_plan_breaks_offset_ties_by_smallest_worker_id() {
        let config = base_config();
        let (config, replica_a) = config
            .add_worker(Worker::new(
                "replica-a",
                "10.0.0.2",
                6380,
                WorkerRole::Replica,
            ))
            .unwrap();
        let (config, replica_b) = config
            .add_worker(Worker::new(
                "replica-b",
                "10.0.0.3",
                6381,
                WorkerRole::Replica,
            ))
            .unwrap();
        let config = config
            .set_worker_replica_of(replica_a, "local-node")
            .unwrap()
            .set_worker_replica_of(replica_b, "local-node")
            .unwrap();

        let mut manager = ReplicationManager::new(
            Some(1),
            ReplicationOffset::new(100),
            ReplicationOffset::new(200),
        )
        .unwrap();
        manager.record_replica_offset(replica_a, ReplicationOffset::new(190));
        manager.record_replica_offset(replica_b, ReplicationOffset::new(190));

        let plan = manager.plan_failover(&config, "local-node").unwrap();
        assert_eq!(plan.promoted_worker_id, replica_a);
        assert_eq!(plan.promoted_replication_offset, 190);
    }

    #[test]
    fn failover_plan_uses_config_replication_offset_when_runtime_offset_missing() {
        let config = base_config();
        let (config, replica_a) = config
            .add_worker(Worker::new(
                "replica-a",
                "10.0.0.2",
                6380,
                WorkerRole::Replica,
            ))
            .unwrap();
        let config = config
            .set_worker_replica_of(replica_a, "local-node")
            .unwrap()
            .set_worker_replication_offset(replica_a, ReplicationOffset::new(777))
            .unwrap();

        let manager = ReplicationManager::new(
            Some(1),
            ReplicationOffset::new(100),
            ReplicationOffset::new(200),
        )
        .unwrap();
        let plan = manager.plan_failover(&config, "local-node").unwrap();
        assert_eq!(plan.promoted_worker_id, replica_a);
        assert_eq!(plan.promoted_replication_offset, 777);
    }

    #[test]
    fn failover_plan_returns_none_without_eligible_replicas() {
        let config = base_config();
        let (config, replica_a) = config
            .add_worker(Worker::new(
                "replica-a",
                "10.0.0.2",
                6380,
                WorkerRole::Replica,
            ))
            .unwrap();
        let config = config
            .set_worker_replica_of(replica_a, "some-other-primary")
            .unwrap();
        let manager = ReplicationManager::new(
            Some(1),
            ReplicationOffset::new(100),
            ReplicationOffset::new(200),
        )
        .unwrap();

        assert!(manager.plan_failover(&config, "local-node").is_none());
        assert!(manager.plan_failover(&config, "unknown-primary").is_none());
    }

    #[test]
    fn execute_failover_applies_selected_plan_to_config() {
        let config = base_config();
        let (config, replica_a) = config
            .add_worker(Worker::new(
                "replica-a",
                "10.0.0.2",
                6380,
                WorkerRole::Replica,
            ))
            .unwrap();
        let (config, replica_b) = config
            .add_worker(Worker::new(
                "replica-b",
                "10.0.0.3",
                6381,
                WorkerRole::Replica,
            ))
            .unwrap();
        let config = config
            .set_worker_replica_of(replica_a, "local-node")
            .unwrap()
            .set_worker_replica_of(replica_b, "local-node")
            .unwrap()
            .set_slot_state(450, LOCAL_WORKER_ID, SlotState::Stable)
            .unwrap();

        let mut manager = ReplicationManager::new(
            Some(1),
            ReplicationOffset::new(100),
            ReplicationOffset::new(200),
        )
        .unwrap();
        manager.record_replica_offset(replica_a, ReplicationOffset::new(170));
        manager.record_replica_offset(replica_b, ReplicationOffset::new(190));

        let (plan, updated) = manager
            .execute_failover(&config, "local-node")
            .unwrap()
            .expect("failover should be elected");

        assert_eq!(plan.promoted_worker_id, replica_b);
        assert_eq!(updated.slot_assigned_owner(450).unwrap(), replica_b);
        assert_eq!(updated.slot_state(450).unwrap(), SlotState::Stable);
        assert_eq!(updated.worker(replica_b).unwrap().role, WorkerRole::Primary);
        assert_eq!(updated.worker(replica_b).unwrap().replica_of_node_id, None);
        assert_eq!(updated.worker(replica_b).unwrap().replication_offset, 190);
        assert_eq!(
            updated.worker(LOCAL_WORKER_ID).unwrap().role,
            WorkerRole::Replica
        );
        assert_eq!(
            updated
                .worker(LOCAL_WORKER_ID)
                .unwrap()
                .replica_of_node_id
                .as_deref(),
            Some("replica-b")
        );
    }

    #[test]
    fn execute_failover_returns_none_when_no_candidate_exists() {
        let config = base_config();
        let manager = ReplicationManager::new(
            Some(1),
            ReplicationOffset::new(100),
            ReplicationOffset::new(200),
        )
        .unwrap();
        let outcome = manager.execute_failover(&config, "local-node").unwrap();
        assert!(outcome.is_none());
    }

    #[test]
    fn failover_coordinator_executes_and_publishes_failover_once() {
        let config = base_config();
        let (config, replica_a) = config
            .add_worker(Worker::new(
                "replica-a",
                "10.0.0.2",
                6380,
                WorkerRole::Replica,
            ))
            .unwrap();
        let (config, replica_b) = config
            .add_worker(Worker::new(
                "replica-b",
                "10.0.0.3",
                6381,
                WorkerRole::Replica,
            ))
            .unwrap();
        let config = config
            .set_worker_replica_of(replica_a, "local-node")
            .unwrap()
            .set_worker_replica_of(replica_b, "local-node")
            .unwrap()
            .set_slot_state(451, LOCAL_WORKER_ID, SlotState::Stable)
            .unwrap();
        let store = ClusterConfigStore::new(config);

        let mut manager = ReplicationManager::new(
            Some(1),
            ReplicationOffset::new(100),
            ReplicationOffset::new(200),
        )
        .unwrap();
        manager.record_replica_offset(replica_a, ReplicationOffset::new(150));
        manager.record_replica_offset(replica_b, ReplicationOffset::new(190));
        let mut coordinator = FailoverCoordinator::new();

        let plan = coordinator
            .execute_for_failed_primary(&store, &manager, "local-node")
            .unwrap()
            .expect("first execution should apply failover");
        assert_eq!(plan.promoted_worker_id, replica_b);
        assert_eq!(store.load().slot_assigned_owner(451).unwrap(), replica_b);
        assert!(
            coordinator
                .handled_failed_primaries()
                .contains("local-node")
        );

        let second = coordinator
            .execute_for_failed_primary(&store, &manager, "local-node")
            .unwrap();
        assert!(second.is_none());
    }

    #[test]
    fn failover_coordinator_keeps_state_clean_when_no_plan_exists() {
        let store = ClusterConfigStore::new(base_config());
        let manager = ReplicationManager::new(
            Some(1),
            ReplicationOffset::new(100),
            ReplicationOffset::new(200),
        )
        .unwrap();
        let mut coordinator = FailoverCoordinator::new();

        let outcome = coordinator
            .execute_for_failed_primary(&store, &manager, "local-node")
            .unwrap();
        assert!(outcome.is_none());
        assert!(coordinator.handled_failed_primaries().is_empty());
    }

    #[test]
    fn cluster_failover_controller_syncs_replicas_and_publishes_once() {
        let config = base_config();
        let (config, replica_a) = config
            .add_worker(Worker::new(
                "replica-a",
                "10.0.0.2",
                6380,
                WorkerRole::Replica,
            ))
            .unwrap();
        let (config, replica_b) = config
            .add_worker(Worker::new(
                "replica-b",
                "10.0.0.3",
                6381,
                WorkerRole::Replica,
            ))
            .unwrap();
        let config = config
            .set_worker_replica_of(replica_a, "local-node")
            .unwrap()
            .set_worker_replica_of(replica_b, "local-node")
            .unwrap()
            .set_slot_state(452, LOCAL_WORKER_ID, SlotState::Stable)
            .unwrap();
        let store = ClusterConfigStore::new(config);

        let mut manager = ReplicationManager::new(
            Some(9),
            ReplicationOffset::new(500),
            ReplicationOffset::new(900),
        )
        .unwrap();
        let mut transport = MockReplicationTransport {
            stream_result: 900,
            ..Default::default()
        };
        let mut controller = ClusterFailoverController::new();

        let first = controller
            .handle_failed_primary(&store, &mut manager, "local-node", &mut transport)
            .unwrap();
        assert_eq!(first.synchronized_replicas.len(), 2);
        assert_eq!(
            first
                .failover_plan
                .as_ref()
                .map(|plan| plan.promoted_worker_id),
            Some(replica_a)
        );
        assert_eq!(store.load().slot_assigned_owner(452).unwrap(), replica_a);
        assert_eq!(transport.checkpoints, vec![(replica_a, 9), (replica_b, 9)]);
        assert_eq!(transport.streams, vec![(replica_a, 500), (replica_b, 500)]);

        transport.checkpoints.clear();
        transport.streams.clear();
        let second = controller
            .handle_failed_primary(&store, &mut manager, "local-node", &mut transport)
            .unwrap();
        assert!(second.failover_plan.is_none());
        assert!(second.synchronized_replicas.is_empty());
        assert!(transport.checkpoints.is_empty());
        assert!(transport.streams.is_empty());
    }

    #[tokio::test]
    async fn cluster_failover_controller_async_syncs_and_publishes() {
        let config = base_config();
        let (config, replica_a) = config
            .add_worker(Worker::new(
                "replica-a",
                "10.0.0.2",
                6380,
                WorkerRole::Replica,
            ))
            .unwrap();
        let (config, replica_b) = config
            .add_worker(Worker::new(
                "replica-b",
                "10.0.0.3",
                6381,
                WorkerRole::Replica,
            ))
            .unwrap();
        let config = config
            .set_worker_replica_of(replica_a, "local-node")
            .unwrap()
            .set_worker_replica_of(replica_b, "local-node")
            .unwrap()
            .set_slot_state(453, LOCAL_WORKER_ID, SlotState::Stable)
            .unwrap();
        let store = ClusterConfigStore::new(config);

        let mut manager = ReplicationManager::new(
            Some(7),
            ReplicationOffset::new(700),
            ReplicationOffset::new(900),
        )
        .unwrap();
        let mut transport = AsyncMockReplicationTransport {
            stream_result: 900,
            ..Default::default()
        };
        let mut controller = ClusterFailoverController::new();

        let outcome = controller
            .handle_failed_primary_async(&store, &mut manager, "local-node", &mut transport)
            .await
            .unwrap();
        assert_eq!(outcome.synchronized_replicas.len(), 2);
        assert_eq!(
            outcome
                .failover_plan
                .as_ref()
                .map(|plan| plan.promoted_worker_id),
            Some(replica_a)
        );
        assert_eq!(store.load().slot_assigned_owner(453).unwrap(), replica_a);
        assert_eq!(transport.checkpoints, vec![(replica_a, 7), (replica_b, 7)]);
        assert_eq!(transport.streams, vec![(replica_a, 700), (replica_b, 700)]);
    }

    #[test]
    fn cluster_failover_controller_handles_failed_worker_ids_for_primaries_only() {
        let config = base_config()
            .set_local_worker_role(WorkerRole::Replica)
            .unwrap();
        let (config, failed_primary_id) = config
            .add_worker(Worker::new("node-2", "10.0.0.2", 6380, WorkerRole::Primary))
            .unwrap();
        let config = config
            .set_worker_replica_of(LOCAL_WORKER_ID, "node-2")
            .unwrap()
            .set_slot_state(454, failed_primary_id, SlotState::Stable)
            .unwrap();
        let store = ClusterConfigStore::new(config);

        let mut manager = ReplicationManager::new(
            Some(9),
            ReplicationOffset::new(500),
            ReplicationOffset::new(900),
        )
        .unwrap();
        let mut transport = MockReplicationTransport {
            stream_result: 900,
            ..Default::default()
        };
        let mut controller = ClusterFailoverController::new();

        let outcomes = controller
            .handle_failed_workers(
                &store,
                &mut manager,
                &[LOCAL_WORKER_ID, failed_primary_id],
                &mut transport,
            )
            .unwrap();
        assert_eq!(outcomes.len(), 1);
        assert_eq!(outcomes[0].0, failed_primary_id);
        assert_eq!(
            store.load().slot_assigned_owner(454).unwrap(),
            LOCAL_WORKER_ID
        );
        assert_eq!(
            outcomes[0]
                .1
                .failover_plan
                .as_ref()
                .map(|plan| plan.promoted_worker_id),
            Some(LOCAL_WORKER_ID)
        );
    }

    #[tokio::test]
    async fn cluster_failover_controller_handles_failed_worker_ids_async() {
        let config = base_config()
            .set_local_worker_role(WorkerRole::Replica)
            .unwrap();
        let (config, failed_primary_id) = config
            .add_worker(Worker::new("node-2", "10.0.0.2", 6380, WorkerRole::Primary))
            .unwrap();
        let config = config
            .set_worker_replica_of(LOCAL_WORKER_ID, "node-2")
            .unwrap()
            .set_slot_state(455, failed_primary_id, SlotState::Stable)
            .unwrap();
        let store = ClusterConfigStore::new(config);

        let mut manager = ReplicationManager::new(
            Some(5),
            ReplicationOffset::new(300),
            ReplicationOffset::new(900),
        )
        .unwrap();
        let mut transport = AsyncMockReplicationTransport {
            stream_result: 900,
            ..Default::default()
        };
        let mut controller = ClusterFailoverController::new();

        let outcomes = controller
            .handle_failed_workers_async(&store, &mut manager, &[failed_primary_id], &mut transport)
            .await
            .unwrap();
        assert_eq!(outcomes.len(), 1);
        assert_eq!(outcomes[0].0, failed_primary_id);
        assert_eq!(
            store.load().slot_assigned_owner(455).unwrap(),
            LOCAL_WORKER_ID
        );
        assert_eq!(transport.checkpoints, vec![(LOCAL_WORKER_ID, 5)]);
        assert_eq!(transport.streams, vec![(LOCAL_WORKER_ID, 300)]);
    }

    #[test]
    fn gossip_failure_budget_is_zero_without_nodes() {
        assert_eq!(calculate_gossip_failure_budget(0, 50), 0);
    }

    #[test]
    fn gossip_failure_budget_has_minimum_of_one_for_non_empty_cluster() {
        assert_eq!(calculate_gossip_failure_budget(3, 0), 1);
    }

    #[test]
    fn gossip_round_continues_across_successes_until_no_stale_nodes_exist() {
        let mut nodes = vec![
            GossipNode::new(1, 10),
            GossipNode::new(2, 20),
            GossipNode::new(3, 30),
        ];
        let report = run_gossip_sample_round(
            &mut nodes,
            10,
            3,
            100,
            |nodes, max| (0..nodes.len().min(max)).collect(),
            |_node| GossipSendResult::Success,
        );

        assert_eq!(report.success_count, 3);
        assert_eq!(report.failure_count, 0);
        assert_eq!(report.attempted_worker_ids, vec![1, 2, 3]);
        assert_eq!(report.remaining_failure_budget, 1);
    }

    #[test]
    fn gossip_round_decrements_failure_budget_only_on_failure() {
        let mut nodes = vec![GossipNode::new(1, 10), GossipNode::new(2, 20)];
        let mut calls = 0usize;
        let report = run_gossip_sample_round(
            &mut nodes,
            50,
            2,
            100,
            |nodes, max| (0..nodes.len().min(max)).collect(),
            |_node| {
                calls += 1;
                if calls == 1 {
                    GossipSendResult::Success
                } else {
                    GossipSendResult::Failure
                }
            },
        );

        assert_eq!(report.success_count, 1);
        assert_eq!(report.failure_count, 1);
        assert_eq!(report.remaining_failure_budget, 0);
    }

    #[test]
    fn gossip_round_chooses_stalest_candidate_from_sample() {
        let mut nodes = vec![
            GossipNode::new(1, 80),
            GossipNode::new(2, 10),
            GossipNode::new(3, 50),
        ];
        let report = run_gossip_sample_round(
            &mut nodes,
            100,
            3,
            100,
            |_nodes, _max| vec![0, 1, 2],
            |_node| GossipSendResult::Failure,
        );

        assert_eq!(report.attempted_worker_ids, vec![2, 3, 1]);
        assert_eq!(report.failure_count, 3);
        assert_eq!(report.success_count, 0);
    }

    #[test]
    fn failure_detector_marks_worker_failed_after_threshold() {
        let mut detector = FailureDetector::new(2);
        let report = GossipRoundReport {
            attempted_worker_ids: vec![7],
            successful_worker_ids: Vec::new(),
            failed_worker_ids: vec![7],
            success_count: 0,
            failure_count: 1,
            remaining_failure_budget: 0,
        };

        assert!(detector.record_report(&report).is_empty());
        assert_eq!(detector.consecutive_failures(7), 1);
        assert!(!detector.is_failed(7));

        let newly_failed = detector.record_report(&report);
        assert_eq!(newly_failed, vec![7]);
        assert_eq!(detector.consecutive_failures(7), 2);
        assert!(detector.is_failed(7));
    }

    #[test]
    fn failure_detector_clears_failure_state_on_successful_probe() {
        let mut detector = FailureDetector::new(2);
        let fail_report = GossipRoundReport {
            attempted_worker_ids: vec![8],
            successful_worker_ids: Vec::new(),
            failed_worker_ids: vec![8],
            success_count: 0,
            failure_count: 1,
            remaining_failure_budget: 0,
        };
        let success_report = GossipRoundReport {
            attempted_worker_ids: vec![8],
            successful_worker_ids: vec![8],
            failed_worker_ids: Vec::new(),
            success_count: 1,
            failure_count: 0,
            remaining_failure_budget: 1,
        };

        let _ = detector.record_report(&fail_report);
        let _ = detector.record_report(&fail_report);
        assert!(detector.is_failed(8));

        let newly_failed = detector.record_report(&success_report);
        assert!(newly_failed.is_empty());
        assert!(!detector.is_failed(8));
        assert_eq!(detector.consecutive_failures(8), 0);
    }

    #[test]
    fn failure_detector_tracks_multiple_workers_independently() {
        let mut detector = FailureDetector::new(3);
        let mixed_report = GossipRoundReport {
            attempted_worker_ids: vec![2, 3],
            successful_worker_ids: vec![3],
            failed_worker_ids: vec![2],
            success_count: 1,
            failure_count: 1,
            remaining_failure_budget: 1,
        };

        for _ in 0..2 {
            let _ = detector.record_report(&mixed_report);
        }
        assert_eq!(detector.consecutive_failures(2), 2);
        assert_eq!(detector.consecutive_failures(3), 0);
        assert!(!detector.is_failed(2));
        assert!(!detector.is_failed(3));

        let newly_failed = detector.record_report(&mixed_report);
        assert_eq!(newly_failed, vec![2]);
        assert!(detector.is_failed(2));
        assert!(!detector.is_failed(3));
    }

    #[derive(Default)]
    struct MockTransport {
        fail_on_worker: Option<u16>,
        calls: Vec<u16>,
    }

    impl GossipTransport for MockTransport {
        type Error = ();

        fn try_gossip(&mut self, worker_id: u16) -> Result<(), Self::Error> {
            self.calls.push(worker_id);
            if self.fail_on_worker == Some(worker_id) {
                Err(())
            } else {
                Ok(())
            }
        }
    }

    #[test]
    fn coordinator_samples_unique_indices() {
        let mut seed = 42u64;
        let sample = sample_random_unique_indices(10, 4, &mut seed);
        assert_eq!(sample.len(), 4);
        let mut dedup = sample.clone();
        dedup.sort_unstable();
        dedup.dedup();
        assert_eq!(dedup.len(), 4);
    }

    #[test]
    fn coordinator_round_uses_transport_result_for_success_failure() {
        let nodes = vec![
            GossipNode::new(10, 0),
            GossipNode::new(20, 0),
            GossipNode::new(30, 0),
        ];
        let mut coordinator = GossipCoordinator::new(nodes, 3);
        let mut transport = MockTransport {
            fail_on_worker: Some(20),
            calls: Vec::new(),
        };

        let report = coordinator.run_round(100, 100, &mut transport);
        assert!(report.attempted_worker_ids.contains(&20));
        assert!(report.failure_count >= 1);
        assert!(report.success_count >= 1);
    }

    #[test]
    fn gossip_engine_advances_tick_and_runs_rounds() {
        let nodes = vec![GossipNode::new(1, 0), GossipNode::new(2, 0)];
        let coordinator = GossipCoordinator::new(nodes, 2);
        let transport = MockTransport {
            fail_on_worker: None,
            calls: Vec::new(),
        };
        let mut engine = GossipEngine::new(coordinator, transport, 100, 10);

        let reports = engine.run_for_rounds(2);
        assert_eq!(engine.tick(), GossipTick::new(12));
        assert_eq!(reports.len(), 2);
        assert_eq!(reports[0].failure_count, 0);
        assert_eq!(reports[1].failure_count, 0);
    }

    #[test]
    fn gossip_engine_exposes_transport_state_after_round() {
        let nodes = vec![GossipNode::new(1, 0)];
        let coordinator = GossipCoordinator::new(nodes, 1);
        let transport = MockTransport {
            fail_on_worker: None,
            calls: Vec::new(),
        };
        let mut engine = GossipEngine::new(coordinator, transport, 100, 0);
        let _ = engine.run_once();

        assert_eq!(engine.transport().calls, vec![1]);
    }

    #[derive(Default)]
    struct AsyncMockTransport {
        fail_on_worker: Option<u16>,
        calls: Vec<u16>,
    }

    impl AsyncGossipTransport for AsyncMockTransport {
        type Error = ();
        type Fut<'a>
            = std::future::Ready<Result<(), Self::Error>>
        where
            Self: 'a;

        fn try_gossip<'a>(&'a mut self, worker_id: u16) -> Self::Fut<'a> {
            self.calls.push(worker_id);
            std::future::ready(if self.fail_on_worker == Some(worker_id) {
                Err(())
            } else {
                Ok(())
            })
        }
    }

    #[tokio::test]
    async fn coordinator_round_async_uses_transport_result_for_success_failure() {
        let nodes = vec![
            GossipNode::new(10, 0),
            GossipNode::new(20, 0),
            GossipNode::new(30, 0),
        ];
        let mut coordinator = GossipCoordinator::new(nodes, 3);
        let mut transport = AsyncMockTransport {
            fail_on_worker: Some(20),
            calls: Vec::new(),
        };

        let report = coordinator.run_round_async(100, 100, &mut transport).await;
        assert!(report.attempted_worker_ids.contains(&20));
        assert!(report.failure_count >= 1);
        assert!(report.success_count >= 1);
    }

    #[tokio::test]
    async fn in_memory_gossip_transport_sends_current_config_snapshot() {
        let config = base_config()
            .set_slot_state(12, LOCAL_WORKER_ID, SlotState::Stable)
            .unwrap();
        let store = std::sync::Arc::new(ClusterConfigStore::new(config));
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

        let mut transport = InMemoryGossipTransport::new(store);
        transport.add_peer(2, tx);
        transport.try_gossip(2).await.unwrap();

        let received = rx.recv().await.unwrap();
        assert_eq!(received.slot_state(12).unwrap(), SlotState::Stable);
        assert_eq!(received.slot_owner(12).unwrap(), LOCAL_WORKER_ID);
    }

    #[tokio::test]
    async fn in_memory_gossip_transport_returns_unknown_peer_error() {
        let store = std::sync::Arc::new(ClusterConfigStore::new(base_config()));
        let mut transport = InMemoryGossipTransport::new(store);
        let result = transport.try_gossip(7).await;

        assert!(matches!(
            result,
            Err(InMemoryGossipTransportError::UnknownPeer(7))
        ));
    }

    #[tokio::test]
    async fn in_memory_gossip_transport_returns_channel_closed_error() {
        let store = std::sync::Arc::new(ClusterConfigStore::new(base_config()));
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<ClusterConfig>();
        drop(rx);
        let mut transport = InMemoryGossipTransport::new(store);
        transport.add_peer(4, tx);

        let result = transport.try_gossip(4).await;
        assert!(matches!(
            result,
            Err(InMemoryGossipTransportError::ChannelClosed(4))
        ));
    }

    #[tokio::test]
    async fn tcp_gossip_transport_sends_snapshot_over_socket() {
        let config = base_config()
            .set_slot_state(12, LOCAL_WORKER_ID, SlotState::Stable)
            .unwrap();
        let store = std::sync::Arc::new(ClusterConfigStore::new(config));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener.local_addr().expect("listener should have addr");
        let receiver = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept should succeed");
            read_gossip_snapshot(&mut socket, 2 * 1024 * 1024).await
        });

        let mut transport = TcpGossipTransport::new(std::sync::Arc::clone(&store));
        transport.add_peer(2, addr);
        transport.try_gossip(2).await.unwrap();

        let received = receiver.await.unwrap().unwrap();
        assert_eq!(received.slot_state(12).unwrap(), SlotState::Stable);
        assert_eq!(received.slot_owner(12).unwrap(), LOCAL_WORKER_ID);
        assert_eq!(received.local_worker().unwrap().node_id, "local-node");
    }

    #[tokio::test]
    async fn tcp_gossip_transport_returns_unknown_peer_error() {
        let store = std::sync::Arc::new(ClusterConfigStore::new(base_config()));
        let mut transport = TcpGossipTransport::new(store);

        let result = transport.try_gossip(99).await;
        assert!(matches!(
            result,
            Err(TcpGossipTransportError::UnknownPeer(99))
        ));
    }

    #[tokio::test]
    async fn async_gossip_engine_advances_tick_and_runs_rounds() {
        let nodes = vec![GossipNode::new(1, 0), GossipNode::new(2, 0)];
        let coordinator = GossipCoordinator::new(nodes, 2);
        let transport = AsyncMockTransport {
            fail_on_worker: None,
            calls: Vec::new(),
        };
        let mut engine = AsyncGossipEngine::new(coordinator, transport, 100, 10);

        let reports = engine.run_for_rounds(2).await;
        assert_eq!(engine.tick(), GossipTick::new(12));
        assert_eq!(reports.len(), 2);
        assert_eq!(reports[0].failure_count, 0);
        assert_eq!(reports[1].failure_count, 0);
        assert_eq!(engine.transport().calls.len(), 4);
    }

    #[tokio::test]
    async fn cluster_manager_runs_configured_number_of_rounds() {
        let nodes = vec![GossipNode::new(1, 0)];
        let coordinator = GossipCoordinator::new(nodes, 1);
        let transport = AsyncMockTransport {
            fail_on_worker: None,
            calls: Vec::new(),
        };
        let engine = AsyncGossipEngine::new(coordinator, transport, 100, 0);
        let mut manager = ClusterManager::new(engine, std::time::Duration::from_millis(2));
        let reports = manager.run_for_rounds(3).await;
        assert_eq!(reports.len(), 3);
    }

    #[tokio::test]
    async fn cluster_manager_stops_on_shutdown_signal() {
        let nodes = vec![GossipNode::new(1, 0)];
        let coordinator = GossipCoordinator::new(nodes, 1);
        let transport = AsyncMockTransport {
            fail_on_worker: None,
            calls: Vec::new(),
        };
        let engine = AsyncGossipEngine::new(coordinator, transport, 100, 0);
        let mut manager = ClusterManager::new(engine, std::time::Duration::from_millis(5));
        let reports = manager
            .run_until_shutdown(tokio::time::sleep(std::time::Duration::from_millis(13)))
            .await;
        assert!(reports.len() >= 2);
        assert!(reports.len() <= 3);
    }

    #[tokio::test]
    async fn cluster_manager_merges_incoming_config_updates() {
        let base = base_config();
        let worker2 = Worker::new("node-b", "10.0.0.2", 6380, WorkerRole::Primary);
        let (base, worker2_id) = base.add_worker(worker2).unwrap();
        let worker3 = Worker::new("node-a", "10.0.0.3", 6381, WorkerRole::Primary);
        let (base, worker3_id) = base.add_worker(worker3).unwrap();

        let current = base
            .set_worker_config_epoch(worker2_id, ConfigEpoch::new(1))
            .unwrap()
            .set_slot_state(303, worker2_id, SlotState::Stable)
            .unwrap();
        let incoming = base
            .set_worker_config_epoch(worker3_id, ConfigEpoch::new(2))
            .unwrap()
            .set_slot_state(303, worker3_id, SlotState::Stable)
            .unwrap();
        let store = ClusterConfigStore::new(current);

        let nodes = vec![GossipNode::new(1, 0)];
        let coordinator = GossipCoordinator::new(nodes, 1);
        let transport = AsyncMockTransport {
            fail_on_worker: None,
            calls: Vec::new(),
        };
        let engine = AsyncGossipEngine::new(coordinator, transport, 100, 0);
        let mut manager = ClusterManager::new(engine, std::time::Duration::from_millis(5));

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        tx.send(incoming).unwrap();
        drop(tx);

        let reports = manager
            .run_with_config_updates(
                &store,
                rx,
                tokio::time::sleep(std::time::Duration::from_millis(12)),
            )
            .await;
        assert!(reports.len() >= 2);
        assert_eq!(store.load().slot_assigned_owner(303).unwrap(), worker3_id);
    }

    #[tokio::test]
    async fn cluster_manager_applies_in_memory_gossip_delivery_end_to_end() {
        let base = base_config();
        let worker2 = Worker::new("node-b", "10.0.0.2", 6380, WorkerRole::Primary);
        let (base, worker2_id) = base.add_worker(worker2).unwrap();
        let worker3 = Worker::new("node-a", "10.0.0.3", 6381, WorkerRole::Primary);
        let (base, worker3_id) = base.add_worker(worker3).unwrap();

        let newer = base
            .set_worker_config_epoch(worker2_id, ConfigEpoch::new(1))
            .unwrap()
            .set_worker_config_epoch(worker3_id, ConfigEpoch::new(2))
            .unwrap()
            .set_slot_state(304, worker3_id, SlotState::Stable)
            .unwrap();
        let older = base
            .set_worker_config_epoch(worker2_id, ConfigEpoch::new(1))
            .unwrap()
            .set_worker_config_epoch(worker3_id, ConfigEpoch::new(1))
            .unwrap()
            .set_slot_state(304, worker2_id, SlotState::Stable)
            .unwrap();

        let sender_store = std::sync::Arc::new(ClusterConfigStore::new(newer));
        let receiver_store = ClusterConfigStore::new(older);
        let (tx_updates, rx_updates) = tokio::sync::mpsc::unbounded_channel();

        let mut sender_transport =
            InMemoryGossipTransport::new(std::sync::Arc::clone(&sender_store));
        sender_transport.add_peer(worker2_id, tx_updates);

        let sender_coordinator = GossipCoordinator::new(vec![GossipNode::new(worker2_id, 0)], 1);
        let sender_engine = AsyncGossipEngine::new(sender_coordinator, sender_transport, 100, 0);
        let mut sender_manager =
            ClusterManager::new(sender_engine, std::time::Duration::from_millis(5));

        let receiver_coordinator = GossipCoordinator::new(Vec::new(), 1);
        let receiver_engine = AsyncGossipEngine::new(
            receiver_coordinator,
            AsyncMockTransport {
                fail_on_worker: None,
                calls: Vec::new(),
            },
            100,
            0,
        );
        let mut receiver_manager =
            ClusterManager::new(receiver_engine, std::time::Duration::from_millis(5));

        let receiver_task = tokio::spawn(async move {
            receiver_manager
                .run_with_config_updates(
                    &receiver_store,
                    rx_updates,
                    tokio::time::sleep(std::time::Duration::from_millis(20)),
                )
                .await;
            receiver_store
        });

        sender_manager.run_for_rounds(1).await;
        let receiver_store = receiver_task.await.unwrap();
        assert_eq!(
            receiver_store.load().slot_assigned_owner(304).unwrap(),
            worker3_id
        );
    }

    #[tokio::test]
    async fn cluster_manager_applies_tcp_gossip_delivery_end_to_end() {
        let base = base_config();
        let worker2 = Worker::new("node-b", "127.0.0.1", 6380, WorkerRole::Primary);
        let (base, worker2_id) = base.add_worker(worker2).unwrap();
        let worker3 = Worker::new("node-a", "127.0.0.1", 6381, WorkerRole::Primary);
        let (base, worker3_id) = base.add_worker(worker3).unwrap();

        let newer = base
            .set_worker_config_epoch(worker2_id, ConfigEpoch::new(1))
            .unwrap()
            .set_worker_config_epoch(worker3_id, ConfigEpoch::new(2))
            .unwrap()
            .set_slot_state(305, worker3_id, SlotState::Stable)
            .unwrap();
        let older = base
            .set_worker_config_epoch(worker2_id, ConfigEpoch::new(1))
            .unwrap()
            .set_worker_config_epoch(worker3_id, ConfigEpoch::new(1))
            .unwrap()
            .set_slot_state(305, worker2_id, SlotState::Stable)
            .unwrap();

        let sender_store = std::sync::Arc::new(ClusterConfigStore::new(newer));
        let receiver_store = ClusterConfigStore::new(older);
        let (tx_updates, rx_updates) = tokio::sync::mpsc::unbounded_channel();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let listener_addr = listener.local_addr().expect("listener should have addr");
        let listener_task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept should succeed");
            let snapshot = read_gossip_snapshot(&mut socket, 2 * 1024 * 1024)
                .await
                .expect("snapshot should decode");
            tx_updates
                .send(snapshot)
                .expect("update channel should receive snapshot");
        });

        let mut sender_transport = TcpGossipTransport::new(std::sync::Arc::clone(&sender_store));
        sender_transport.add_peer(worker2_id, listener_addr);

        let sender_coordinator = GossipCoordinator::new(vec![GossipNode::new(worker2_id, 0)], 1);
        let sender_engine = AsyncGossipEngine::new(sender_coordinator, sender_transport, 100, 0);
        let mut sender_manager =
            ClusterManager::new(sender_engine, std::time::Duration::from_millis(5));

        let receiver_coordinator = GossipCoordinator::new(Vec::new(), 1);
        let receiver_engine = AsyncGossipEngine::new(
            receiver_coordinator,
            AsyncMockTransport {
                fail_on_worker: None,
                calls: Vec::new(),
            },
            100,
            0,
        );
        let mut receiver_manager =
            ClusterManager::new(receiver_engine, std::time::Duration::from_millis(5));

        let receiver_task = tokio::spawn(async move {
            receiver_manager
                .run_with_config_updates(
                    &receiver_store,
                    rx_updates,
                    tokio::time::sleep(std::time::Duration::from_millis(20)),
                )
                .await;
            receiver_store
        });

        sender_manager.run_for_rounds(1).await;
        listener_task.await.unwrap();
        let receiver_store = receiver_task.await.unwrap();
        assert_eq!(
            receiver_store.load().slot_assigned_owner(305).unwrap(),
            worker3_id
        );
    }

    #[tokio::test]
    async fn cluster_manager_run_with_failover_triggers_controller_on_detected_failure() {
        let config = base_config()
            .set_local_worker_role(WorkerRole::Replica)
            .unwrap();
        let (config, failed_primary_id) = config
            .add_worker(Worker::new("node-2", "10.0.0.2", 6380, WorkerRole::Primary))
            .unwrap();
        let config = config
            .set_worker_replica_of(LOCAL_WORKER_ID, "node-2")
            .unwrap()
            .set_slot_state(460, failed_primary_id, SlotState::Stable)
            .unwrap();
        let store = ClusterConfigStore::new(config);

        let gossip_nodes = vec![GossipNode::new(failed_primary_id, 0)];
        let gossip_coordinator = GossipCoordinator::new(gossip_nodes, 1);
        let gossip_transport = AsyncMockTransport {
            fail_on_worker: Some(failed_primary_id),
            calls: Vec::new(),
        };
        let gossip_engine = AsyncGossipEngine::new(gossip_coordinator, gossip_transport, 100, 0);
        let mut manager = ClusterManager::new(gossip_engine, std::time::Duration::from_millis(5));

        let (_tx_updates, rx_updates) = tokio::sync::mpsc::unbounded_channel();
        let mut failure_detector = FailureDetector::new(1);
        let mut failover_controller = ClusterFailoverController::new();
        let mut replication_manager = ReplicationManager::new(
            Some(3),
            ReplicationOffset::new(100),
            ReplicationOffset::new(200),
        )
        .unwrap();
        let mut replication_transport = AsyncMockReplicationTransport {
            stream_result: 200,
            ..Default::default()
        };

        let reports = manager
            .run_with_config_updates_and_failover(
                &store,
                rx_updates,
                &mut failure_detector,
                &mut failover_controller,
                &mut replication_manager,
                &mut replication_transport,
                tokio::time::sleep(std::time::Duration::from_millis(20)),
            )
            .await
            .unwrap();

        assert!(!reports.is_empty());
        assert!(
            reports
                .iter()
                .any(|report| report.failed_worker_ids.contains(&failed_primary_id))
        );
        assert_eq!(
            store.load().slot_assigned_owner(460).unwrap(),
            LOCAL_WORKER_ID
        );
        assert_eq!(
            store.load().local_worker().unwrap().role,
            WorkerRole::Primary
        );
        assert_eq!(
            replication_transport.checkpoints,
            vec![(LOCAL_WORKER_ID, 3)]
        );
        assert_eq!(replication_transport.streams, vec![(LOCAL_WORKER_ID, 100)]);
    }

    #[tokio::test]
    async fn cluster_manager_failover_report_exposes_executed_plan_details() {
        let config = base_config()
            .set_local_worker_role(WorkerRole::Replica)
            .unwrap();
        let (config, failed_primary_id) = config
            .add_worker(Worker::new("node-2", "10.0.0.2", 6380, WorkerRole::Primary))
            .unwrap();
        let config = config
            .set_worker_replica_of(LOCAL_WORKER_ID, "node-2")
            .unwrap()
            .set_slot_state(461, failed_primary_id, SlotState::Stable)
            .unwrap();
        let store = ClusterConfigStore::new(config);

        let gossip_engine = AsyncGossipEngine::new(
            GossipCoordinator::new(vec![GossipNode::new(failed_primary_id, 0)], 1),
            AsyncMockTransport {
                fail_on_worker: Some(failed_primary_id),
                calls: Vec::new(),
            },
            100,
            0,
        );
        let mut manager = ClusterManager::new(gossip_engine, std::time::Duration::from_millis(5));

        let (_tx_updates, rx_updates) = tokio::sync::mpsc::unbounded_channel();
        let mut failure_detector = FailureDetector::new(1);
        let mut failover_controller = ClusterFailoverController::new();
        let mut replication_manager = ReplicationManager::new(
            Some(4),
            ReplicationOffset::new(200),
            ReplicationOffset::new(300),
        )
        .unwrap();
        let mut replication_transport = AsyncMockReplicationTransport {
            stream_result: 300,
            ..Default::default()
        };

        let report = manager
            .run_with_config_updates_and_failover_report(
                &store,
                rx_updates,
                &mut failure_detector,
                &mut failover_controller,
                &mut replication_manager,
                &mut replication_transport,
                tokio::time::sleep(std::time::Duration::from_millis(20)),
            )
            .await
            .unwrap();

        assert!(!report.gossip_reports.is_empty());
        assert_eq!(report.failover_records.len(), 1);
        let record = &report.failover_records[0];
        assert_eq!(record.failed_worker_id, failed_primary_id);
        assert_eq!(record.plan.failed_primary_worker_id, failed_primary_id);
        assert_eq!(record.plan.promoted_worker_id, LOCAL_WORKER_ID);
        assert_eq!(record.synchronized_replicas.len(), 1);
        assert_eq!(
            store.load().slot_assigned_owner(461).unwrap(),
            LOCAL_WORKER_ID
        );
    }

    #[tokio::test]
    async fn cluster_manager_failover_report_runs_round_on_empty_shutdown() {
        let store = ClusterConfigStore::new(base_config());
        let gossip_engine = AsyncGossipEngine::new(
            GossipCoordinator::new(Vec::new(), 1),
            AsyncMockTransport::default(),
            100,
            0,
        );
        let mut manager = ClusterManager::new(gossip_engine, std::time::Duration::from_millis(5));

        let (_tx_updates, rx_updates) = tokio::sync::mpsc::unbounded_channel();
        let mut failure_detector = FailureDetector::new(1);
        let mut failover_controller = ClusterFailoverController::new();
        let mut replication_manager = ReplicationManager::new(
            Some(4),
            ReplicationOffset::new(200),
            ReplicationOffset::new(300),
        )
        .unwrap();
        let mut replication_transport = AsyncMockReplicationTransport::default();

        let report = manager
            .run_with_config_updates_and_failover_report(
                &store,
                rx_updates,
                &mut failure_detector,
                &mut failover_controller,
                &mut replication_manager,
                &mut replication_transport,
                std::future::ready(()),
            )
            .await
            .unwrap();

        assert_eq!(report.gossip_reports.len(), 1);
        assert!(report.failover_records.is_empty());
    }

    #[test]
    fn redis_hash_slot_uses_crc16_xmodem() {
        assert_eq!(crc16_xmodem(b"123456789"), 0x31C3);
        let slot = redis_hash_slot(b"123456789");
        assert_eq!(slot, 12_739);
    }

    #[test]
    fn redis_hash_slot_honors_hash_tags() {
        let a = redis_hash_slot(b"{user1000}.following");
        let b = redis_hash_slot(b"{user1000}.followers");
        let c = redis_hash_slot(b"user1000.following");
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn stable_remote_slot_routes_to_moved() {
        let config = base_config();
        let remote = Worker::new("node-2", "10.0.0.2", 6380, WorkerRole::Primary);
        let (with_remote, remote_id) = config.add_worker(remote).unwrap();
        let updated = with_remote
            .set_slot_state(120, remote_id, SlotState::Stable)
            .unwrap();
        assert_eq!(
            updated.route_for_slot(120).unwrap(),
            SlotRouteDecision::Moved {
                slot: 120,
                worker_id: remote_id,
            }
        );
    }

    #[test]
    fn migrating_remote_slot_routes_to_ask() {
        let config = base_config();
        let remote = Worker::new("node-2", "10.0.0.2", 6380, WorkerRole::Primary);
        let (with_remote, remote_id) = config.add_worker(remote).unwrap();
        let updated = with_remote
            .set_slot_state(121, remote_id, SlotState::Migrating)
            .unwrap();
        assert_eq!(
            updated.route_for_slot(121).unwrap(),
            SlotRouteDecision::Ask {
                slot: 121,
                worker_id: remote_id,
            }
        );
    }

    #[test]
    fn local_stable_slot_routes_locally() {
        let config = base_config()
            .set_slot_state(122, LOCAL_WORKER_ID, SlotState::Stable)
            .unwrap();
        assert_eq!(
            config.route_for_slot(122).unwrap(),
            SlotRouteDecision::Local
        );
    }

    #[test]
    fn worker_endpoint_formats_host_and_port() {
        let worker = Worker::new("n1", "127.0.0.1", 6379, WorkerRole::Primary);
        assert_eq!(worker.endpoint(), "127.0.0.1:6379");
    }

    #[test]
    fn moved_redirection_error_is_generated_for_remote_stable_slot() {
        let config = base_config();
        let remote = Worker::new("node-2", "10.0.0.2", 6380, WorkerRole::Primary);
        let (with_remote, remote_id) = config.add_worker(remote).unwrap();
        let updated = with_remote
            .set_slot_state(220, remote_id, SlotState::Stable)
            .unwrap();
        assert_eq!(
            updated.redirection_error_for_slot(220).unwrap(),
            Some("MOVED 220 10.0.0.2:6380".to_string())
        );
    }

    #[test]
    fn ask_redirection_error_is_generated_for_remote_migrating_slot() {
        let config = base_config();
        let remote = Worker::new("node-2", "10.0.0.2", 6380, WorkerRole::Primary);
        let (with_remote, remote_id) = config.add_worker(remote).unwrap();
        let updated = with_remote
            .set_slot_state(221, remote_id, SlotState::Migrating)
            .unwrap();
        assert_eq!(
            updated.redirection_error_for_slot(221).unwrap(),
            Some("ASK 221 10.0.0.2:6380".to_string())
        );
    }

    #[test]
    fn local_slot_has_no_redirection_error() {
        let config = base_config()
            .set_slot_state(222, LOCAL_WORKER_ID, SlotState::Stable)
            .unwrap();
        assert_eq!(config.redirection_error_for_slot(222).unwrap(), None);
    }

    #[test]
    fn key_based_redirection_uses_hash_slot() {
        let key = b"{shared-tag}.k1";
        let slot = redis_hash_slot(key);

        let config = base_config();
        let remote = Worker::new("node-2", "10.0.0.2", 6380, WorkerRole::Primary);
        let (with_remote, remote_id) = config.add_worker(remote).unwrap();
        let updated = with_remote
            .set_slot_state(slot, remote_id, SlotState::Stable)
            .unwrap();

        assert_eq!(
            updated.redirection_error_for_key(key).unwrap(),
            Some(format!("MOVED {slot} 10.0.0.2:6380"))
        );
    }
}
