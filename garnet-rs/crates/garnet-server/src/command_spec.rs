//! Centralized command metadata used by routing, replication, and COMMAND output.

use crate::command_dispatch::CommandId;

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

pub fn command_transaction_control(command: CommandId) -> TransactionControlCommand {
    match command {
        CommandId::Asking => TransactionControlCommand::Asking,
        CommandId::Multi => TransactionControlCommand::Multi,
        CommandId::Exec => TransactionControlCommand::Exec,
        CommandId::Discard => TransactionControlCommand::Discard,
        CommandId::Watch => TransactionControlCommand::Watch,
        CommandId::Unwatch => TransactionControlCommand::Unwatch,
        _ => TransactionControlCommand::None,
    }
}

pub fn command_arity_policy(command: CommandId) -> Option<ArityPolicy> {
    match command {
        CommandId::Asking
        | CommandId::Multi
        | CommandId::Exec
        | CommandId::Discard
        | CommandId::Unwatch => Some(ArityPolicy::Exact(1)),
        CommandId::Watch => Some(ArityPolicy::Min(2)),
        _ => None,
    }
}

pub fn command_has_valid_arity(command: CommandId, argument_count: usize) -> bool {
    match command_arity_policy(command) {
        Some(policy) => policy.matches(argument_count),
        None => true,
    }
}

pub fn command_key_access_pattern(command: CommandId) -> KeyAccessPattern {
    match command {
        CommandId::Del | CommandId::Watch => KeyAccessPattern::AllKeysFromArg1,
        CommandId::Get
        | CommandId::Set
        | CommandId::Incr
        | CommandId::Decr
        | CommandId::Expire
        | CommandId::Ttl
        | CommandId::Pexpire
        | CommandId::Pttl
        | CommandId::Persist
        | CommandId::Hset
        | CommandId::Hget
        | CommandId::Hdel
        | CommandId::Hgetall
        | CommandId::Lpush
        | CommandId::Rpush
        | CommandId::Lpop
        | CommandId::Rpop
        | CommandId::Lrange
        | CommandId::Sadd
        | CommandId::Srem
        | CommandId::Smembers
        | CommandId::Sismember
        | CommandId::Zadd
        | CommandId::Zrem
        | CommandId::Zrange
        | CommandId::Zscore => KeyAccessPattern::FirstKey,
        _ => KeyAccessPattern::None,
    }
}

pub fn command_is_owner_routable(command: CommandId, argument_count: usize) -> bool {
    let routing_policy = match command {
        CommandId::Get
        | CommandId::Set
        | CommandId::Incr
        | CommandId::Decr
        | CommandId::Expire
        | CommandId::Pexpire
        | CommandId::Ttl
        | CommandId::Pttl
        | CommandId::Persist => OwnerRoutingPolicy::FirstKey,
        CommandId::Del => OwnerRoutingPolicy::SingleKeyOnly,
        _ => OwnerRoutingPolicy::Never,
    };

    match routing_policy {
        OwnerRoutingPolicy::Never => false,
        OwnerRoutingPolicy::FirstKey => argument_count >= 2,
        OwnerRoutingPolicy::SingleKeyOnly => argument_count == 2,
    }
}

pub fn command_is_mutating(command: CommandId) -> bool {
    matches!(
        command,
        CommandId::Set
            | CommandId::Del
            | CommandId::Incr
            | CommandId::Decr
            | CommandId::Expire
            | CommandId::Pexpire
            | CommandId::Persist
            | CommandId::Hset
            | CommandId::Hdel
            | CommandId::Lpush
            | CommandId::Rpush
            | CommandId::Lpop
            | CommandId::Rpop
            | CommandId::Sadd
            | CommandId::Srem
            | CommandId::Zadd
            | CommandId::Zrem
    )
}

pub fn command_names_for_command_response() -> &'static [&'static [u8]] {
    &[
        b"GET",
        b"SET",
        b"DEL",
        b"INCR",
        b"DECR",
        b"EXPIRE",
        b"TTL",
        b"PEXPIRE",
        b"PTTL",
        b"PERSIST",
        b"HSET",
        b"HGET",
        b"HDEL",
        b"HGETALL",
        b"LPUSH",
        b"RPUSH",
        b"LPOP",
        b"RPOP",
        b"LRANGE",
        b"SADD",
        b"SREM",
        b"SMEMBERS",
        b"SISMEMBER",
        b"ZADD",
        b"ZREM",
        b"ZRANGE",
        b"ZSCORE",
        b"MULTI",
        b"EXEC",
        b"DISCARD",
        b"WATCH",
        b"UNWATCH",
        b"ASKING",
        b"PING",
        b"ECHO",
        b"INFO",
        b"DBSIZE",
        b"COMMAND",
        b"REPLICAOF",
        b"REPLCONF",
        b"PSYNC",
        b"SYNC",
    ]
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
