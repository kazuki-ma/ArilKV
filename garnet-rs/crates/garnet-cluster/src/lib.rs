//! Cluster configuration primitives for garnet-rs.
//!
//! This module provides immutable-style copy-on-write cluster configuration
//! structures with Redis-compatible 16,384 hash slots.

use std::fmt;
use std::sync::{Arc, RwLock};

pub const HASH_SLOT_COUNT: usize = 16_384;
pub const RESERVED_WORKER_ID: u16 = 0;
pub const LOCAL_WORKER_ID: u16 = 1;

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
    pub fn new(worker_id: u16, state: SlotState) -> Self {
        let [lo, hi] = worker_id.to_le_bytes();
        Self {
            bytes: [lo, hi, state as u8],
        }
    }

    pub fn assigned_worker_id(self) -> u16 {
        u16::from_le_bytes([self.bytes[0], self.bytes[1]])
    }

    pub fn worker_id(self) -> u16 {
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
    pub id: u16,
    pub node_id: String,
    pub host: String,
    pub port: u16,
    pub config_epoch: u64,
    pub role: WorkerRole,
    pub replica_of_node_id: Option<String>,
    pub replication_offset: u64,
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
            config_epoch: 0,
            role,
            replica_of_node_id: None,
            replication_offset: 0,
        }
    }

    fn reserved() -> Self {
        Self {
            id: RESERVED_WORKER_ID,
            node_id: String::new(),
            host: String::new(),
            port: 0,
            config_epoch: 0,
            role: WorkerRole::Unassigned,
            replica_of_node_id: None,
            replication_offset: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterConfigError {
    InvalidSlot(u16),
    WorkerNotFound(u16),
    WorkerCapacityExceeded,
    MissingLocalWorker,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotRouteDecision {
    Local,
    Moved { slot: u16, worker_id: u16 },
    Ask { slot: u16, worker_id: u16 },
    Unbound { slot: u16 },
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

    pub fn worker(&self, worker_id: u16) -> Option<&Worker> {
        self.workers.get(worker_id as usize)
    }

    pub fn local_worker(&self) -> Result<&Worker, ClusterConfigError> {
        self.worker(LOCAL_WORKER_ID)
            .ok_or(ClusterConfigError::MissingLocalWorker)
    }

    pub fn slot(&self, slot: u16) -> Result<HashSlot, ClusterConfigError> {
        Ok(self.slot_map[Self::slot_index(slot)?])
    }

    pub fn slot_state(&self, slot: u16) -> Result<SlotState, ClusterConfigError> {
        Ok(self.slot(slot)?.state())
    }

    pub fn slot_owner(&self, slot: u16) -> Result<u16, ClusterConfigError> {
        Ok(self.slot(slot)?.worker_id())
    }

    pub fn slot_assigned_owner(&self, slot: u16) -> Result<u16, ClusterConfigError> {
        Ok(self.slot(slot)?.assigned_worker_id())
    }

    pub fn is_local_slot(&self, slot: u16) -> Result<bool, ClusterConfigError> {
        Ok(self.slot(slot)?.is_local())
    }

    pub fn route_for_slot(&self, slot: u16) -> Result<SlotRouteDecision, ClusterConfigError> {
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

    pub fn set_slot_state(
        &self,
        slot: u16,
        worker_id: u16,
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

    pub fn set_local_worker_role(&self, role: WorkerRole) -> Result<Self, ClusterConfigError> {
        let mut next = self.clone();
        let local = next
            .workers
            .get_mut(LOCAL_WORKER_ID as usize)
            .ok_or(ClusterConfigError::MissingLocalWorker)?;
        local.role = role;
        if role != WorkerRole::Replica {
            local.replica_of_node_id = None;
        }
        Ok(next)
    }

    pub fn add_worker(&self, mut worker: Worker) -> Result<(Self, u16), ClusterConfigError> {
        let worker_id = u16::try_from(self.workers.len())
            .map_err(|_| ClusterConfigError::WorkerCapacityExceeded)?;
        let mut next = self.clone();
        worker.id = worker_id;
        next.workers.push(worker);
        Ok((next, worker_id))
    }

    fn slot_index(slot: u16) -> Result<usize, ClusterConfigError> {
        let slot_index = slot as usize;
        if slot_index >= HASH_SLOT_COUNT {
            return Err(ClusterConfigError::InvalidSlot(slot));
        }
        Ok(slot_index)
    }
}

pub fn redis_hash_slot(key: &[u8]) -> u16 {
    let hash_key = extract_hash_tag(key).unwrap_or(key);
    crc16_xmodem(hash_key) % (HASH_SLOT_COUNT as u16)
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GossipNode {
    pub worker_id: u16,
    pub last_gossip_tick: u64,
}

impl GossipNode {
    pub fn new(worker_id: u16, last_gossip_tick: u64) -> Self {
        Self {
            worker_id,
            last_gossip_tick,
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
    pub attempted_worker_ids: Vec<u16>,
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

pub fn run_gossip_sample_round<Selector, Attempt>(
    nodes: &mut [GossipNode],
    sample_percent: usize,
    max_random_nodes_to_poll: usize,
    round_start_tick: u64,
    mut selector: Selector,
    mut attempt: Attempt,
) -> GossipRoundReport
where
    Selector: FnMut(&[GossipNode], usize) -> Vec<usize>,
    Attempt: FnMut(&mut GossipNode) -> GossipSendResult,
{
    let mut failure_budget = calculate_gossip_failure_budget(nodes.len(), sample_percent);
    let mut report = GossipRoundReport {
        attempted_worker_ids: Vec::new(),
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
                report.success_count += 1;
            }
            GossipSendResult::Failure => {
                // The original implementation removes failed connections from the
                // candidate pool for the remainder of the round. Mark this node
                // as non-stale so it is not retried in this round.
                node.last_gossip_tick = round_start_tick;
                failure_budget -= 1;
                report.failure_count += 1;
            }
        }
    }

    report.remaining_failure_budget = failure_budget;
    report
}

pub trait GossipTransport {
    type Error;

    fn try_gossip(&mut self, worker_id: u16) -> Result<(), Self::Error>;
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
        round_start_tick: u64,
        transport: &mut T,
    ) -> GossipRoundReport {
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

    fn base_config() -> ClusterConfig {
        ClusterConfig::new_local("local-node", "127.0.0.1", 6379)
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

    #[derive(Default)]
    struct MockTransport {
        fail_on_worker: Option<u16>,
    }

    impl GossipTransport for MockTransport {
        type Error = ();

        fn try_gossip(&mut self, worker_id: u16) -> Result<(), Self::Error> {
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
        };

        let report = coordinator.run_round(100, 100, &mut transport);
        assert!(report.attempted_worker_ids.contains(&20));
        assert!(report.failure_count >= 1);
        assert!(report.success_count >= 1);
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
}
