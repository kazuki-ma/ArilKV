use std::collections::BTreeMap;

use garnet_cluster::ClusterConfig;
use garnet_cluster::HASH_SLOT_COUNT;
use garnet_cluster::LOCAL_WORKER_ID;
use garnet_cluster::RESERVED_WORKER_ID;
use garnet_cluster::SlotNumber;
use garnet_cluster::SlotState;
use garnet_cluster::Worker;
use garnet_cluster::WorkerId;
use garnet_cluster::WorkerRole;

use crate::request_lifecycle::RespProtocolVersion;

const ZERO_CLUSTER_NODE_ID: &str = "0000000000000000000000000000000000000000";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ClusterSlotRange {
    start: u16,
    end: u16,
    owner: WorkerId,
}

fn cluster_node_id_or_default(node_id: &str) -> &str {
    if node_id.is_empty() {
        ZERO_CLUSTER_NODE_ID
    } else {
        node_id
    }
}

fn cluster_bus_port(port: u16) -> u16 {
    port.saturating_add(10_000)
}

fn collect_assigned_cluster_slot_ranges(config: &ClusterConfig) -> Vec<ClusterSlotRange> {
    let mut ranges = Vec::new();
    let mut current: Option<ClusterSlotRange> = None;
    for slot_index in 0..HASH_SLOT_COUNT {
        let slot = SlotNumber::new(slot_index as u16);
        let state = config.slot_state(slot).unwrap_or(SlotState::Invalid);
        if matches!(state, SlotState::Offline | SlotState::Invalid) {
            if let Some(range) = current.take() {
                ranges.push(range);
            }
            continue;
        }
        let owner = config
            .slot_assigned_owner(slot)
            .unwrap_or(RESERVED_WORKER_ID);
        if owner == RESERVED_WORKER_ID {
            if let Some(range) = current.take() {
                ranges.push(range);
            }
            continue;
        }
        let slot_u16 = slot_index as u16;
        match current.as_mut() {
            Some(range)
                if range.owner == owner
                    && range
                        .end
                        .checked_add(1)
                        .is_some_and(|next| next == slot_u16) =>
            {
                range.end = slot_u16;
            }
            Some(_) => {
                ranges.push(current.take().expect("range exists"));
                current = Some(ClusterSlotRange {
                    start: slot_u16,
                    end: slot_u16,
                    owner,
                });
            }
            None => {
                current = Some(ClusterSlotRange {
                    start: slot_u16,
                    end: slot_u16,
                    owner,
                });
            }
        }
    }
    if let Some(range) = current.take() {
        ranges.push(range);
    }
    ranges
}

fn collect_cluster_slot_state_counts(config: &ClusterConfig) -> (usize, usize, usize, usize) {
    let mut slots_assigned = 0usize;
    let mut slots_ok = 0usize;
    let slots_pfail = 0usize;
    let mut slots_fail = 0usize;
    for slot_index in 0..HASH_SLOT_COUNT {
        let slot = SlotNumber::new(slot_index as u16);
        let state = config.slot_state(slot).unwrap_or(SlotState::Invalid);
        match state {
            SlotState::Stable => {
                slots_assigned += 1;
                slots_ok += 1;
            }
            SlotState::Migrating | SlotState::Importing | SlotState::Node => {
                slots_assigned += 1;
            }
            SlotState::Fail => {
                slots_assigned += 1;
                slots_fail += 1;
            }
            SlotState::Offline | SlotState::Invalid => {}
        }
    }
    (slots_assigned, slots_ok, slots_pfail, slots_fail)
}

fn worker_by_id_map(config: &ClusterConfig) -> BTreeMap<WorkerId, &Worker> {
    let mut workers = BTreeMap::new();
    for worker in config.workers() {
        if worker.id == RESERVED_WORKER_ID {
            continue;
        }
        workers.insert(worker.id, worker);
    }
    workers
}

fn slot_ranges_by_owner(ranges: &[ClusterSlotRange]) -> BTreeMap<WorkerId, Vec<ClusterSlotRange>> {
    let mut ranges_by_owner: BTreeMap<WorkerId, Vec<ClusterSlotRange>> = BTreeMap::new();
    for range in ranges {
        ranges_by_owner.entry(range.owner).or_default().push(*range);
    }
    ranges_by_owner
}

fn cluster_primary_owner_ids(
    ranges_by_owner: &BTreeMap<WorkerId, Vec<ClusterSlotRange>>,
    workers: &BTreeMap<WorkerId, &Worker>,
) -> Vec<WorkerId> {
    let mut owners = Vec::new();
    for owner in ranges_by_owner.keys() {
        if workers
            .get(owner)
            .is_some_and(|worker| worker.role != WorkerRole::Replica)
        {
            owners.push(*owner);
        }
    }
    if owners.is_empty() {
        owners.extend(ranges_by_owner.keys().copied());
    }
    owners
}

fn append_i64_ascii(output: &mut Vec<u8>, value: i64) {
    output.extend_from_slice(value.to_string().as_bytes());
}

fn append_usize_ascii(output: &mut Vec<u8>, value: usize) {
    output.extend_from_slice(value.to_string().as_bytes());
}

fn append_integer_frame(output: &mut Vec<u8>, value: i64) {
    output.push(b':');
    append_i64_ascii(output, value);
    output.extend_from_slice(b"\r\n");
}

fn append_array_length_frame(output: &mut Vec<u8>, len: usize) {
    output.push(b'*');
    append_usize_ascii(output, len);
    output.extend_from_slice(b"\r\n");
}

fn append_bulk_string_frame(output: &mut Vec<u8>, value: &[u8]) {
    output.push(b'$');
    append_usize_ascii(output, value.len());
    output.extend_from_slice(b"\r\n");
    output.extend_from_slice(value);
    output.extend_from_slice(b"\r\n");
}

fn append_verbatim_string_frame(output: &mut Vec<u8>, format: &[u8], value: &[u8]) {
    output.push(b'=');
    let payload_len = format.len().saturating_add(1).saturating_add(value.len());
    append_usize_ascii(output, payload_len);
    output.extend_from_slice(b"\r\n");
    output.extend_from_slice(format);
    output.push(b':');
    output.extend_from_slice(value);
    output.extend_from_slice(b"\r\n");
}

pub(crate) fn append_cluster_info_snapshot(
    response_out: &mut Vec<u8>,
    config: &ClusterConfig,
    resp_protocol_version: RespProtocolVersion,
) {
    let ranges = collect_assigned_cluster_slot_ranges(config);
    let ranges_by_owner = slot_ranges_by_owner(&ranges);
    let workers = worker_by_id_map(config);
    let primary_owner_ids = cluster_primary_owner_ids(&ranges_by_owner, &workers);
    let (slots_assigned, slots_ok, slots_pfail, slots_fail) =
        collect_cluster_slot_state_counts(config);
    let known_nodes = workers
        .values()
        .filter(|worker| worker.role != WorkerRole::Unassigned)
        .count();
    let current_epoch = workers
        .values()
        .map(|worker| u64::from(worker.config_epoch))
        .max()
        .unwrap_or(0);
    let my_epoch = config
        .local_worker()
        .map(|worker| u64::from(worker.config_epoch))
        .unwrap_or(0);
    let cluster_state = if slots_assigned == HASH_SLOT_COUNT && slots_fail == 0 {
        "ok"
    } else {
        "fail"
    };
    let mut payload = String::new();
    payload.push_str("cluster_state:");
    payload.push_str(cluster_state);
    payload.push_str("\r\ncluster_slots_assigned:");
    payload.push_str(&slots_assigned.to_string());
    payload.push_str("\r\ncluster_slots_ok:");
    payload.push_str(&slots_ok.to_string());
    payload.push_str("\r\ncluster_slots_pfail:");
    payload.push_str(&slots_pfail.to_string());
    payload.push_str("\r\ncluster_slots_fail:");
    payload.push_str(&slots_fail.to_string());
    payload.push_str("\r\ncluster_known_nodes:");
    payload.push_str(&known_nodes.to_string());
    payload.push_str("\r\ncluster_size:");
    payload.push_str(&primary_owner_ids.len().to_string());
    payload.push_str("\r\ncluster_current_epoch:");
    payload.push_str(&current_epoch.to_string());
    payload.push_str("\r\ncluster_my_epoch:");
    payload.push_str(&my_epoch.to_string());
    payload.push_str("\r\ncluster_stats_messages_sent:0");
    payload.push_str("\r\ncluster_stats_messages_received:0");
    payload.push_str("\r\ntotal_cluster_links_buffer_limit_exceeded:0\r\n");
    if resp_protocol_version.is_resp3() {
        append_verbatim_string_frame(response_out, b"txt", payload.as_bytes());
    } else {
        append_bulk_string_frame(response_out, payload.as_bytes());
    }
}

pub(crate) fn append_cluster_myid_snapshot(response_out: &mut Vec<u8>, config: &ClusterConfig) {
    let node_id = config
        .local_worker()
        .map(|worker| cluster_node_id_or_default(&worker.node_id))
        .unwrap_or(ZERO_CLUSTER_NODE_ID);
    append_bulk_string_frame(response_out, node_id.as_bytes());
}

pub(crate) fn append_cluster_nodes_snapshot(response_out: &mut Vec<u8>, config: &ClusterConfig) {
    let ranges = collect_assigned_cluster_slot_ranges(config);
    let ranges_by_owner = slot_ranges_by_owner(&ranges);
    let workers = worker_by_id_map(config);
    let mut payload = String::new();
    for worker in workers.values() {
        let node_id = cluster_node_id_or_default(&worker.node_id);
        let mut flags = String::new();
        if worker.id == LOCAL_WORKER_ID {
            flags.push_str("myself,");
        }
        if worker.role == WorkerRole::Replica {
            flags.push_str("slave");
        } else {
            flags.push_str("master");
        }
        let master_id = if worker.role == WorkerRole::Replica {
            cluster_node_id_or_default(worker.replica_of_node_id.as_deref().unwrap_or(""))
        } else {
            "-"
        };
        payload.push_str(node_id);
        payload.push(' ');
        payload.push_str(&worker.host);
        payload.push(':');
        payload.push_str(&worker.port.to_string());
        payload.push('@');
        payload.push_str(&cluster_bus_port(worker.port).to_string());
        payload.push(' ');
        payload.push_str(&flags);
        payload.push(' ');
        payload.push_str(master_id);
        payload.push_str(" 0 0 ");
        payload.push_str(&u64::from(worker.config_epoch).to_string());
        payload.push_str(" connected");
        if let Some(slot_ranges) = ranges_by_owner.get(&worker.id) {
            for range in slot_ranges {
                payload.push(' ');
                payload.push_str(&range.start.to_string());
                if range.start != range.end {
                    payload.push('-');
                    payload.push_str(&range.end.to_string());
                }
            }
        }
        payload.push('\n');
    }
    append_bulk_string_frame(response_out, payload.as_bytes());
}

pub(crate) fn append_cluster_slots_snapshot(response_out: &mut Vec<u8>, config: &ClusterConfig) {
    let ranges = collect_assigned_cluster_slot_ranges(config);
    let workers = worker_by_id_map(config);
    append_array_length_frame(response_out, ranges.len());
    for range in ranges {
        append_array_length_frame(response_out, 3);
        append_integer_frame(response_out, i64::from(range.start));
        append_integer_frame(response_out, i64::from(range.end));
        append_array_length_frame(response_out, 3);
        if let Some(worker) = workers.get(&range.owner) {
            append_bulk_string_frame(response_out, worker.host.as_bytes());
            append_integer_frame(response_out, i64::from(worker.port));
            append_bulk_string_frame(
                response_out,
                cluster_node_id_or_default(&worker.node_id).as_bytes(),
            );
        } else {
            append_bulk_string_frame(response_out, b"127.0.0.1");
            append_integer_frame(response_out, 6379);
            append_bulk_string_frame(response_out, ZERO_CLUSTER_NODE_ID.as_bytes());
        }
    }
}

pub(crate) fn append_cluster_shards_snapshot(response_out: &mut Vec<u8>, config: &ClusterConfig) {
    let ranges = collect_assigned_cluster_slot_ranges(config);
    let ranges_by_owner = slot_ranges_by_owner(&ranges);
    let workers = worker_by_id_map(config);
    let primary_owner_ids = cluster_primary_owner_ids(&ranges_by_owner, &workers);
    append_array_length_frame(response_out, primary_owner_ids.len());
    for primary_owner in primary_owner_ids {
        let Some(primary_worker) = workers.get(&primary_owner) else {
            continue;
        };
        let mut shard_nodes: Vec<&Worker> = vec![*primary_worker];
        for worker in workers.values() {
            if worker.id == primary_worker.id {
                continue;
            }
            if worker.role == WorkerRole::Replica
                && worker
                    .replica_of_node_id
                    .as_deref()
                    .is_some_and(|node_id| node_id == primary_worker.node_id)
            {
                shard_nodes.push(*worker);
            }
        }
        append_array_length_frame(response_out, 4);
        append_bulk_string_frame(response_out, b"slots");
        let primary_ranges = ranges_by_owner
            .get(&primary_owner)
            .cloned()
            .unwrap_or_default();
        append_array_length_frame(response_out, primary_ranges.len() * 2);
        for range in primary_ranges {
            append_integer_frame(response_out, i64::from(range.start));
            append_integer_frame(response_out, i64::from(range.end));
        }
        append_bulk_string_frame(response_out, b"nodes");
        append_array_length_frame(response_out, shard_nodes.len());
        for node in shard_nodes {
            append_array_length_frame(response_out, 8);
            append_bulk_string_frame(response_out, b"id");
            append_bulk_string_frame(
                response_out,
                cluster_node_id_or_default(&node.node_id).as_bytes(),
            );
            append_bulk_string_frame(response_out, b"port");
            append_integer_frame(response_out, i64::from(node.port));
            append_bulk_string_frame(response_out, b"ip");
            append_bulk_string_frame(response_out, node.host.as_bytes());
            append_bulk_string_frame(response_out, b"role");
            append_bulk_string_frame(
                response_out,
                if node.role == WorkerRole::Replica {
                    b"replica"
                } else {
                    b"master"
                },
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode_bulk_payload(frame: &[u8]) -> Vec<u8> {
        assert!(frame.starts_with(b"$"));
        let Some(header_end) = frame.windows(2).position(|window| window == b"\r\n") else {
            panic!("bulk string frame must contain CRLF header");
        };
        let payload_len = std::str::from_utf8(&frame[1..header_end])
            .unwrap()
            .parse::<usize>()
            .unwrap();
        let payload_start = header_end + 2;
        let payload_end = payload_start + payload_len;
        frame[payload_start..payload_end].to_vec()
    }

    #[test]
    fn cluster_snapshot_helpers_render_live_info_and_nodes() {
        let mut config = ClusterConfig::new_local("node-1", "127.0.0.1", 7001);
        let (next, remote_id) = config
            .add_worker(Worker::new("node-2", "10.0.0.2", 6380, WorkerRole::Primary))
            .unwrap();
        config = next;
        config = config
            .set_slot_state(SlotNumber::new(0), LOCAL_WORKER_ID, SlotState::Stable)
            .unwrap();
        config = config
            .set_slot_state(SlotNumber::new(1), remote_id, SlotState::Stable)
            .unwrap();

        let mut resp2_info = Vec::new();
        append_cluster_info_snapshot(&mut resp2_info, &config, RespProtocolVersion::Resp2);
        let info_payload = decode_bulk_payload(&resp2_info);
        let info_text = String::from_utf8_lossy(&info_payload);
        assert!(info_text.contains("cluster_slots_assigned:2"));
        assert!(info_text.contains("cluster_known_nodes:2"));
        assert!(info_text.contains("cluster_state:fail"));

        let mut resp3_info = Vec::new();
        append_cluster_info_snapshot(&mut resp3_info, &config, RespProtocolVersion::Resp3);
        assert!(resp3_info.starts_with(b"="));
        assert!(
            String::from_utf8_lossy(&resp3_info).contains("txt:cluster_state:fail"),
            "RESP3 cluster info must use verbatim string format"
        );

        let mut nodes = Vec::new();
        append_cluster_nodes_snapshot(&mut nodes, &config);
        let nodes_payload = decode_bulk_payload(&nodes);
        let nodes_text = String::from_utf8_lossy(&nodes_payload);
        assert!(nodes_text.contains("node-1 127.0.0.1:7001@17001 myself,master"));
        assert!(nodes_text.contains("node-2 10.0.0.2:6380@16380 master"));
        assert!(
            nodes_text.contains("connected 0") && nodes_text.contains("connected 1"),
            "cluster nodes should expose slot ownership"
        );
    }

    #[test]
    fn cluster_snapshot_helpers_render_live_slots_and_shards() {
        let mut config = ClusterConfig::new_local("node-1", "127.0.0.1", 7001);
        let (next, remote_id) = config
            .add_worker(Worker::new("node-2", "10.0.0.2", 6380, WorkerRole::Primary))
            .unwrap();
        config = next;
        config = config
            .set_slot_state(SlotNumber::new(0), LOCAL_WORKER_ID, SlotState::Stable)
            .unwrap();
        config = config
            .set_slot_state(SlotNumber::new(1), remote_id, SlotState::Stable)
            .unwrap();

        let mut myid = Vec::new();
        append_cluster_myid_snapshot(&mut myid, &config);
        assert_eq!(myid, b"$6\r\nnode-1\r\n");

        let mut slots = Vec::new();
        append_cluster_slots_snapshot(&mut slots, &config);
        assert!(
            slots.starts_with(b"*2\r\n"),
            "CLUSTER SLOTS should include one range per assigned owner in this fixture"
        );
        let slots_text = String::from_utf8_lossy(&slots);
        assert!(slots_text.contains("$9\r\n127.0.0.1\r\n"));
        assert!(slots_text.contains("$8\r\n10.0.0.2\r\n"));
        assert!(slots_text.contains("$6\r\nnode-1\r\n"));
        assert!(slots_text.contains("$6\r\nnode-2\r\n"));

        let mut shards = Vec::new();
        append_cluster_shards_snapshot(&mut shards, &config);
        assert!(
            shards.starts_with(b"*2\r\n"),
            "CLUSTER SHARDS should include both primaries in this fixture"
        );
        let shards_text = String::from_utf8_lossy(&shards);
        assert!(shards_text.contains("$5\r\nslots\r\n"));
        assert!(shards_text.contains("$5\r\nnodes\r\n"));
        assert!(shards_text.contains("$4\r\nrole\r\n$6\r\nmaster\r\n"));
    }
}
