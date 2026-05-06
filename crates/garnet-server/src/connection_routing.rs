use garnet_cluster::ClusterConfigError;
use garnet_cluster::ClusterConfigStore;
use garnet_cluster::SlotNumber;
use garnet_cluster::SlotRouteDecision;
use garnet_cluster::WorkerRole;
use garnet_cluster::redis_hash_slot;
use garnet_common::ArgSlice;
use std::io;

use crate::CommandId;
use crate::RequestProcessor;
use crate::command_spec::KeyAccessPattern;
#[cfg(test)]
use crate::command_spec::command_is_owner_routable;
use crate::command_spec::command_key_access_pattern;
use crate::request_lifecycle::ShardIndex;

#[inline]
fn arg_slice_bytes(arg: &ArgSlice) -> &[u8] {
    // SAFETY: routing helpers consume ArgSlice values while the request frame
    // backing buffer is still alive in the caller.
    unsafe { arg.as_slice() }
}

#[cfg(test)]
pub(crate) fn owner_routed_shard_for_command(
    processor: &RequestProcessor,
    args: &[ArgSlice],
    command: CommandId,
) -> Option<ShardIndex> {
    if !command_is_owner_routable(command, args.len()) {
        return None;
    }

    let key = arg_slice_bytes(&args[1]);
    Some(processor.string_store_shard_index(key))
}

pub(crate) fn owner_shard_for_command(
    processor: &RequestProcessor,
    args: &[ArgSlice],
    command: CommandId,
) -> ShardIndex {
    if args.len() < 2 {
        return ShardIndex::new(0);
    }

    match command_key_access_pattern(command) {
        KeyAccessPattern::FirstKey | KeyAccessPattern::AllKeysFromArg1 => {
            let key = arg_slice_bytes(&args[1]);
            processor.string_store_shard_index(key)
        }
        KeyAccessPattern::None => ShardIndex::new(0),
    }
}

pub(crate) fn cluster_error_for_command(
    cluster_store: &ClusterConfigStore,
    args: &[ArgSlice],
    command: CommandId,
    asking_allowed: bool,
    read_only_mode: bool,
    command_mutating: bool,
) -> io::Result<(Option<String>, bool)> {
    if args.len() < 2 {
        return Ok((None, asking_allowed));
    }

    match command_key_access_pattern(command) {
        KeyAccessPattern::AllKeysFromArg1 => {
            let config = cluster_store.load();
            let mut first_slot = None;
            for arg in &args[1..] {
                let key = arg_slice_bytes(arg);
                let slot = redis_hash_slot(key);

                if let Some(existing) = first_slot {
                    if slot != existing {
                        return Ok((
                            Some(
                                "CROSSSLOT Keys in request don't hash to the same slot".to_string(),
                            ),
                            true,
                        ));
                    }
                } else {
                    first_slot = Some(slot);
                }

                if let Some(redirection_error) = cluster_redirection_for_slot(
                    &config,
                    slot,
                    asking_allowed,
                    read_only_mode,
                    command_mutating,
                )
                .map_err(|error| io::Error::other(format!("cluster error: {error}")))?
                {
                    return Ok((Some(redirection_error), true));
                }
            }
            Ok((None, true))
        }
        KeyAccessPattern::FirstKey => {
            let key = arg_slice_bytes(&args[1]);
            let slot = redis_hash_slot(key);
            let error = cluster_redirection_for_slot(
                &cluster_store.load(),
                slot,
                asking_allowed,
                read_only_mode,
                command_mutating,
            )
            .map_err(|cluster_error| io::Error::other(format!("cluster error: {cluster_error}")))?;
            Ok((error, asking_allowed))
        }
        KeyAccessPattern::None => Ok((None, asking_allowed)),
    }
}

fn cluster_redirection_for_slot(
    config: &garnet_cluster::ClusterConfig,
    slot: SlotNumber,
    asking_allowed: bool,
    read_only_mode: bool,
    command_mutating: bool,
) -> Result<Option<String>, ClusterConfigError> {
    match config.route_for_slot(slot)? {
        SlotRouteDecision::Local => Ok(None),
        SlotRouteDecision::Ask { .. } if asking_allowed => Ok(None),
        SlotRouteDecision::Moved { worker_id, .. }
            if read_only_mode
                && !command_mutating
                && local_replica_can_serve_reads_for_worker(config, worker_id)? =>
        {
            Ok(None)
        }
        _ => config.redirection_error_for_slot(slot),
    }
}

fn local_replica_can_serve_reads_for_worker(
    config: &garnet_cluster::ClusterConfig,
    owner_worker_id: garnet_cluster::WorkerId,
) -> Result<bool, ClusterConfigError> {
    let local_worker = config.local_worker()?;
    if local_worker.role != WorkerRole::Replica {
        return Ok(false);
    }
    let Some(local_primary_node_id) = local_worker.replica_of_node_id.as_deref() else {
        return Ok(false);
    };
    let Some(owner_worker) = config.worker(owner_worker_id) else {
        return Err(ClusterConfigError::WorkerNotFound(owner_worker_id));
    };
    Ok(owner_worker.node_id == local_primary_node_id)
}

pub(crate) fn command_hash_slot_for_transaction(
    args: &[ArgSlice],
    command: CommandId,
) -> Option<SlotNumber> {
    if args.len() < 2 {
        return None;
    }
    match command_key_access_pattern(command) {
        KeyAccessPattern::FirstKey | KeyAccessPattern::AllKeysFromArg1 => {
            let key = arg_slice_bytes(&args[1]);
            Some(redis_hash_slot(key))
        }
        KeyAccessPattern::None => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use garnet_cluster::ClusterConfig;
    use garnet_cluster::LOCAL_WORKER_ID;
    use garnet_cluster::SlotState;
    use garnet_cluster::Worker;
    use garnet_cluster::WorkerRole;

    #[test]
    fn owner_shard_for_command_routes_multikey_del_by_first_key() {
        let processor = RequestProcessor::new().unwrap();
        let args = vec![
            ArgSlice::from_slice(b"DEL").unwrap(),
            ArgSlice::from_slice(b"k1").unwrap(),
            ArgSlice::from_slice(b"k2").unwrap(),
        ];
        let shard = owner_shard_for_command(&processor, &args, CommandId::Del);
        assert_eq!(shard, processor.string_store_shard_index(b"k1"));
    }

    #[test]
    fn owner_shard_for_command_routes_keyless_commands_to_shard_zero() {
        let processor = RequestProcessor::new().unwrap();
        let args = vec![ArgSlice::from_slice(b"PING").unwrap()];
        assert_eq!(
            owner_shard_for_command(&processor, &args, CommandId::Ping),
            ShardIndex::new(0)
        );
    }

    #[test]
    fn cluster_readonly_allows_local_replica_reads_but_not_writes() {
        let key = b"replica-read-key";
        let slot = redis_hash_slot(key);
        let mut config = ClusterConfig::new_local("node-1", "127.0.0.1", 7001)
            .set_local_worker_role(WorkerRole::Replica)
            .unwrap()
            .set_worker_replica_of(LOCAL_WORKER_ID, "node-2")
            .unwrap();
        let (next, remote_id) = config
            .add_worker(Worker::new("node-2", "10.0.0.2", 6380, WorkerRole::Primary))
            .unwrap();
        config = next
            .set_slot_state(slot, remote_id, SlotState::Stable)
            .unwrap();
        let cluster_store = ClusterConfigStore::new(config);
        let get_args = vec![
            ArgSlice::from_slice(b"GET").unwrap(),
            ArgSlice::from_slice(key).unwrap(),
        ];
        let set_args = vec![
            ArgSlice::from_slice(b"SET").unwrap(),
            ArgSlice::from_slice(key).unwrap(),
            ArgSlice::from_slice(b"value").unwrap(),
        ];

        let (get_redirection, _) = cluster_error_for_command(
            &cluster_store,
            &get_args,
            CommandId::Get,
            false,
            false,
            false,
        )
        .unwrap();
        let moved = get_redirection.expect("GET should redirect before READONLY");
        assert!(moved.starts_with(&format!("MOVED {slot} 10.0.0.2:6380")));

        let (readonly_get_redirection, _) = cluster_error_for_command(
            &cluster_store,
            &get_args,
            CommandId::Get,
            false,
            true,
            false,
        )
        .unwrap();
        assert_eq!(readonly_get_redirection, None);

        let (readonly_set_redirection, _) =
            cluster_error_for_command(&cluster_store, &set_args, CommandId::Set, false, true, true)
                .unwrap();
        let moved = readonly_set_redirection.expect("SET should still redirect in READONLY mode");
        assert!(moved.starts_with(&format!("MOVED {slot} 10.0.0.2:6380")));
    }
}
