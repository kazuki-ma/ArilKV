use garnet_cluster::ClusterConfigError;
use garnet_cluster::ClusterConfigStore;
use garnet_cluster::SlotRouteDecision;
use garnet_cluster::redis_hash_slot;
use garnet_common::ArgSlice;
use std::io;

use crate::CommandId;
use crate::RequestProcessor;
use crate::command_spec::KeyAccessPattern;
#[cfg(test)]
use crate::command_spec::command_is_owner_routable;
use crate::command_spec::command_key_access_pattern;

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
) -> Option<usize> {
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
) -> usize {
    if args.len() < 2 {
        return 0;
    }

    match command_key_access_pattern(command) {
        KeyAccessPattern::FirstKey | KeyAccessPattern::AllKeysFromArg1 => {
            let key = arg_slice_bytes(&args[1]);
            processor.string_store_shard_index(key)
        }
        KeyAccessPattern::None => 0,
    }
}

pub(crate) fn cluster_error_for_command(
    cluster_store: &ClusterConfigStore,
    args: &[ArgSlice],
    command: CommandId,
    asking_allowed: bool,
) -> io::Result<(Option<String>, bool)> {
    if args.len() < 2 {
        return Ok((None, asking_allowed));
    }

    match command_key_access_pattern(command) {
        KeyAccessPattern::AllKeysFromArg1 => {
            let config = cluster_store.load();
            let mut first_slot = None;
            for arg in &args[1..] {
                let key = arg_slice_bytes(&arg);
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

                if let Some(redirection_error) =
                    cluster_redirection_for_slot(&config, slot, asking_allowed).map_err(
                        |error| {
                            io::Error::new(io::ErrorKind::Other, format!("cluster error: {error}"))
                        },
                    )?
                {
                    return Ok((Some(redirection_error), true));
                }
            }
            Ok((None, true))
        }
        KeyAccessPattern::FirstKey => {
            let key = arg_slice_bytes(&args[1]);
            let slot = redis_hash_slot(key);
            let error = cluster_redirection_for_slot(&cluster_store.load(), slot, asking_allowed)
                .map_err(|cluster_error| {
                io::Error::new(
                    io::ErrorKind::Other,
                    format!("cluster error: {cluster_error}"),
                )
            })?;
            Ok((error, asking_allowed))
        }
        KeyAccessPattern::None => Ok((None, asking_allowed)),
    }
}

fn cluster_redirection_for_slot(
    config: &garnet_cluster::ClusterConfig,
    slot: u16,
    asking_allowed: bool,
) -> Result<Option<String>, ClusterConfigError> {
    match config.route_for_slot(slot)? {
        SlotRouteDecision::Local => Ok(None),
        SlotRouteDecision::Ask { .. } if asking_allowed => Ok(None),
        _ => config.redirection_error_for_slot(slot),
    }
}

pub(crate) fn command_hash_slot_for_transaction(
    args: &[ArgSlice],
    command: CommandId,
) -> Option<u16> {
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
            0
        );
    }
}
