//! Centralized command metadata used by dispatch, routing, replication, and
//! COMMAND output.

use std::sync::OnceLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CommandId {
    Get,
    Set,
    Del,
    Incr,
    Decr,
    Expire,
    Ttl,
    Pexpire,
    Pttl,
    Persist,
    Hset,
    Hget,
    Hdel,
    Hgetall,
    Lpush,
    Rpush,
    Lpop,
    Rpop,
    Lrange,
    Sadd,
    Srem,
    Smembers,
    Sismember,
    Zadd,
    Zrem,
    Zrange,
    Zscore,
    Multi,
    Exec,
    Discard,
    Watch,
    Unwatch,
    Asking,
    Ping,
    Echo,
    Info,
    Dbsize,
    Command,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyAccessPattern {
    None,
    FirstKey,
    AllKeysFromArg1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArityPolicy {
    Exact(usize),
    Min(usize),
}

impl ArityPolicy {
    pub fn matches(self, argument_count: usize) -> bool {
        match self {
            Self::Exact(value) => argument_count == value,
            Self::Min(value) => argument_count >= value,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionControlCommand {
    None,
    Asking,
    Multi,
    Exec,
    Discard,
    Watch,
    Unwatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OwnerRoutingPolicy {
    Never,
    FirstKey,
    SingleKeyOnly,
}

#[derive(Debug, Clone, Copy)]
struct CommandSpecEntry {
    id: CommandId,
    name_upper: &'static [u8],
    key_access_pattern: KeyAccessPattern,
    owner_routing_policy: OwnerRoutingPolicy,
    is_mutating: bool,
    transaction_control: TransactionControlCommand,
    arity_policy: Option<ArityPolicy>,
    include_in_command_response: bool,
}

const REPLICATION_PROTOCOL_COMMANDS: [&[u8]; 4] = [b"REPLICAOF", b"REPLCONF", b"PSYNC", b"SYNC"];
const COMMAND_ID_COUNT: usize = CommandId::Unknown as usize + 1;

const COMMAND_SPECS: [CommandSpecEntry; COMMAND_ID_COUNT] = [
    CommandSpecEntry {
        id: CommandId::Get,
        name_upper: b"GET",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::FirstKey,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: None,
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Set,
        name_upper: b"SET",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::FirstKey,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: None,
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Del,
        name_upper: b"DEL",
        key_access_pattern: KeyAccessPattern::AllKeysFromArg1,
        owner_routing_policy: OwnerRoutingPolicy::SingleKeyOnly,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: None,
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Incr,
        name_upper: b"INCR",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::FirstKey,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: None,
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Decr,
        name_upper: b"DECR",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::FirstKey,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: None,
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Expire,
        name_upper: b"EXPIRE",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::FirstKey,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: None,
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Ttl,
        name_upper: b"TTL",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::FirstKey,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: None,
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Pexpire,
        name_upper: b"PEXPIRE",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::FirstKey,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: None,
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Pttl,
        name_upper: b"PTTL",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::FirstKey,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: None,
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Persist,
        name_upper: b"PERSIST",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::FirstKey,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: None,
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Hset,
        name_upper: b"HSET",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: None,
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Hget,
        name_upper: b"HGET",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: None,
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Hdel,
        name_upper: b"HDEL",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: None,
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Hgetall,
        name_upper: b"HGETALL",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: None,
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Lpush,
        name_upper: b"LPUSH",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: None,
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Rpush,
        name_upper: b"RPUSH",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: None,
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Lpop,
        name_upper: b"LPOP",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: None,
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Rpop,
        name_upper: b"RPOP",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: None,
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Lrange,
        name_upper: b"LRANGE",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: None,
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Sadd,
        name_upper: b"SADD",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: None,
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Srem,
        name_upper: b"SREM",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: None,
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Smembers,
        name_upper: b"SMEMBERS",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: None,
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Sismember,
        name_upper: b"SISMEMBER",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: None,
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Zadd,
        name_upper: b"ZADD",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: None,
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Zrem,
        name_upper: b"ZREM",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: true,
        transaction_control: TransactionControlCommand::None,
        arity_policy: None,
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Zrange,
        name_upper: b"ZRANGE",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: None,
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Zscore,
        name_upper: b"ZSCORE",
        key_access_pattern: KeyAccessPattern::FirstKey,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: None,
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Multi,
        name_upper: b"MULTI",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::Multi,
        arity_policy: Some(ArityPolicy::Exact(1)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Exec,
        name_upper: b"EXEC",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::Exec,
        arity_policy: Some(ArityPolicy::Exact(1)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Discard,
        name_upper: b"DISCARD",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::Discard,
        arity_policy: Some(ArityPolicy::Exact(1)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Watch,
        name_upper: b"WATCH",
        key_access_pattern: KeyAccessPattern::AllKeysFromArg1,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::Watch,
        arity_policy: Some(ArityPolicy::Min(2)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Unwatch,
        name_upper: b"UNWATCH",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::Unwatch,
        arity_policy: Some(ArityPolicy::Exact(1)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Asking,
        name_upper: b"ASKING",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::Asking,
        arity_policy: Some(ArityPolicy::Exact(1)),
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Ping,
        name_upper: b"PING",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: None,
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Echo,
        name_upper: b"ECHO",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: None,
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Info,
        name_upper: b"INFO",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: None,
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Dbsize,
        name_upper: b"DBSIZE",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: None,
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Command,
        name_upper: b"COMMAND",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: None,
        include_in_command_response: true,
    },
    CommandSpecEntry {
        id: CommandId::Unknown,
        name_upper: b"UNKNOWN",
        key_access_pattern: KeyAccessPattern::None,
        owner_routing_policy: OwnerRoutingPolicy::Never,
        is_mutating: false,
        transaction_control: TransactionControlCommand::None,
        arity_policy: None,
        include_in_command_response: false,
    },
];

static COMMAND_RESPONSE_NAMES: OnceLock<Box<[&'static [u8]]>> = OnceLock::new();

#[inline]
fn spec_for(command: CommandId) -> &'static CommandSpecEntry {
    &COMMAND_SPECS[command as usize]
}

#[inline]
fn ascii_upper(byte: u8) -> u8 {
    if byte.is_ascii_lowercase() {
        byte - 32
    } else {
        byte
    }
}

fn ascii_eq_ignore_case(input: &[u8], expected_upper: &[u8]) -> bool {
    input.len() == expected_upper.len()
        && input
            .iter()
            .zip(expected_upper)
            .all(|(left, right)| ascii_upper(*left) == *right)
}

pub(crate) fn command_id_from_name(command_name: &[u8]) -> Option<CommandId> {
    COMMAND_SPECS[..CommandId::Unknown as usize]
        .iter()
        .find(|entry| ascii_eq_ignore_case(command_name, entry.name_upper))
        .map(|entry| entry.id)
}

pub fn command_transaction_control(command: CommandId) -> TransactionControlCommand {
    spec_for(command).transaction_control
}

pub fn command_arity_policy(command: CommandId) -> Option<ArityPolicy> {
    spec_for(command).arity_policy
}

pub fn command_has_valid_arity(command: CommandId, argument_count: usize) -> bool {
    match command_arity_policy(command) {
        Some(policy) => policy.matches(argument_count),
        None => true,
    }
}

pub fn command_name_upper(command: CommandId) -> &'static [u8] {
    spec_for(command).name_upper
}

pub fn command_key_access_pattern(command: CommandId) -> KeyAccessPattern {
    spec_for(command).key_access_pattern
}

pub fn command_is_owner_routable(command: CommandId, argument_count: usize) -> bool {
    match spec_for(command).owner_routing_policy {
        OwnerRoutingPolicy::Never => false,
        OwnerRoutingPolicy::FirstKey => argument_count >= 2,
        OwnerRoutingPolicy::SingleKeyOnly => argument_count == 2,
    }
}

pub fn command_is_mutating(command: CommandId) -> bool {
    spec_for(command).is_mutating
}

pub fn command_names_for_command_response() -> &'static [&'static [u8]] {
    COMMAND_RESPONSE_NAMES
        .get_or_init(|| {
            let mut names =
                Vec::with_capacity(COMMAND_ID_COUNT + REPLICATION_PROTOCOL_COMMANDS.len() - 1);
            for spec in COMMAND_SPECS {
                if spec.include_in_command_response {
                    names.push(spec.name_upper);
                }
            }
            names.extend_from_slice(&REPLICATION_PROTOCOL_COMMANDS);
            names.into_boxed_slice()
        })
        .as_ref()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{dispatch_command_name, CommandId};

    #[test]
    fn command_response_names_include_replication_protocol_commands() {
        let names = command_names_for_command_response();
        assert!(names.contains(&&b"REPLICAOF"[..]));
        assert!(names.contains(&&b"REPLCONF"[..]));
        assert!(names.contains(&&b"PSYNC"[..]));
        assert!(names.contains(&&b"SYNC"[..]));
    }

    #[test]
    fn owner_routing_restricts_del_to_single_key() {
        assert!(command_is_owner_routable(CommandId::Del, 2));
        assert!(!command_is_owner_routable(CommandId::Del, 3));
        assert!(command_is_owner_routable(CommandId::Set, 3));
    }

    #[test]
    fn key_access_patterns_match_expected_commands() {
        assert_eq!(
            command_key_access_pattern(CommandId::Del),
            KeyAccessPattern::AllKeysFromArg1
        );
        assert_eq!(
            command_key_access_pattern(CommandId::Watch),
            KeyAccessPattern::AllKeysFromArg1
        );
        assert_eq!(
            command_key_access_pattern(CommandId::Set),
            KeyAccessPattern::FirstKey
        );
        assert_eq!(
            command_key_access_pattern(CommandId::Ping),
            KeyAccessPattern::None
        );
    }

    #[test]
    fn transaction_control_classification_matches_expected_commands() {
        assert_eq!(
            command_transaction_control(CommandId::Asking),
            TransactionControlCommand::Asking
        );
        assert_eq!(
            command_transaction_control(CommandId::Multi),
            TransactionControlCommand::Multi
        );
        assert_eq!(
            command_transaction_control(CommandId::Exec),
            TransactionControlCommand::Exec
        );
        assert_eq!(
            command_transaction_control(CommandId::Discard),
            TransactionControlCommand::Discard
        );
        assert_eq!(
            command_transaction_control(CommandId::Watch),
            TransactionControlCommand::Watch
        );
        assert_eq!(
            command_transaction_control(CommandId::Unwatch),
            TransactionControlCommand::Unwatch
        );
        assert_eq!(
            command_transaction_control(CommandId::Set),
            TransactionControlCommand::None
        );
    }

    #[test]
    fn transaction_related_arity_policies_match_expected_rules() {
        assert!(command_has_valid_arity(CommandId::Asking, 1));
        assert!(!command_has_valid_arity(CommandId::Asking, 2));
        assert!(command_has_valid_arity(CommandId::Watch, 2));
        assert!(command_has_valid_arity(CommandId::Watch, 3));
        assert!(!command_has_valid_arity(CommandId::Watch, 1));
        assert!(command_has_valid_arity(CommandId::Set, 2));
    }

    #[test]
    fn command_name_upper_maps_known_and_unknown_commands() {
        assert_eq!(command_name_upper(CommandId::Set), b"SET");
        assert_eq!(command_name_upper(CommandId::Watch), b"WATCH");
        assert_eq!(command_name_upper(CommandId::Unknown), b"UNKNOWN");
    }

    #[test]
    fn command_response_names_match_dispatch_behavior() {
        for name in command_names_for_command_response() {
            let dispatch = dispatch_command_name(name);
            let protocol_passthrough = *name == b"REPLICAOF"
                || *name == b"REPLCONF"
                || *name == b"PSYNC"
                || *name == b"SYNC";
            if protocol_passthrough {
                assert_eq!(dispatch, CommandId::Unknown);
            } else {
                assert_ne!(dispatch, CommandId::Unknown);
            }
        }
    }
}
